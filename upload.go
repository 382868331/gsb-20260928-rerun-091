package objectstore

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync/atomic"
	"time"
)

const (
	stateReceiving  = "receiving"
	stateAssembling = "assembling"
	stateAssembled  = "assembled"
	stateCommitted  = "committed"

	maxPartNumber = 10000
)

// PartInfo describes one uploaded part.
type PartInfo struct {
	Number uint32 `json:"number"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
}

// Version describes a published, immutable object version.
type Version struct {
	Key         string    `json:"key"`
	Version     string    `json:"version"`
	Size        int64     `json:"size"`
	PublishedAt time.Time `json:"published_at"`
}

type uploadMeta struct {
	ID              string              `json:"id"`
	Key             string              `json:"key"`
	ExpectedVersion string              `json:"expected_version,omitempty"`
	CreatedAt       time.Time           `json:"created_at"`
	UpdatedAt       time.Time           `json:"updated_at"`
	State           string              `json:"state"`
	Parts           map[uint32]PartInfo `json:"parts"`
	Version         string              `json:"version,omitempty"`
	Size            int64               `json:"size,omitempty"`
}

type pointer struct {
	Version     string    `json:"version"`
	Size        int64     `json:"size"`
	PublishedAt time.Time `json:"published_at"`
	UploadID    string    `json:"upload_id"`
}

// Upload is an in-progress multipart upload session.
type Upload struct {
	s    *Store
	meta uploadMeta

	finalized atomic.Bool
	liveLock  *fileLock
}

// UploadOption configures a multipart upload.
type UploadOption func(*uploadMeta)

// IfMatch applies optimistic concurrency: CompleteUpload wins only if the
// current version of the key equals expected. Use "" to require that the
// key does not yet exist.
func IfMatch(expected string) UploadOption {
	return func(m *uploadMeta) { m.ExpectedVersion = expected }
}

// CreateUpload starts a multipart upload session for key.
func (s *Store) CreateUpload(key string, opts ...UploadOption) (*Upload, error) {
	if key == "" {
		return nil, errors.New("objectstore: empty key")
	}
	id, err := newUploadID()
	if err != nil {
		return nil, err
	}
	now := nowUTC()
	m := uploadMeta{
		ID:        id,
		Key:       key,
		CreatedAt: now,
		UpdatedAt: now,
		State:     stateReceiving,
		Parts:     map[uint32]PartInfo{},
	}
	for _, opt := range opts {
		opt(&m)
	}
	dir := s.uploadDir(id)
	// Live-session lock: acquired before the session becomes visible, and
	// released automatically by the kernel if this process dies. Recovery
	// never quarantines a session whose lock is held.
	if err := os.MkdirAll(dir, dirPerm); err != nil {
		return nil, err
	}
	lk, err := acquireLock(filepath.Join(dir, ".live"))
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Join(dir, "parts"), dirPerm); err != nil {
		lk.release()
		return nil, err
	}
	if err := s.writeMeta(&m); err != nil {
		lk.release()
		return nil, err
	}
	if err := syncDir(filepath.Join(dir, "parts")); err != nil {
		return nil, err
	}
	u := &Upload{s: s, meta: m, liveLock: lk}
	return u, nil
}

func (u *Upload) markFinalized() {
	u.finalized.Store(true)
	if u.liveLock != nil {
		u.liveLock.release()
		u.liveLock = nil
	}
}

func (s *Store) uploadDir(id string) string {
	return s.path("uploads", id)
}

func (s *Store) writeMeta(m *uploadMeta) error {
	m.UpdatedAt = nowUTC()
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	return writeFileAtomic(filepath.Join(s.uploadDir(m.ID), "meta.json"), data, filePerm)
}

// ID returns the upload session identifier.
func (u *Upload) ID() string { return u.meta.ID }

// PutPart stores one part. Part numbers start at 1 and must be contiguous at
// completion time. Re-uploading the same number is idempotent only when the
// content is byte-identical.
func (u *Upload) PutPart(number uint32, r io.Reader) (PartInfo, error) {
	if u.finalized.Load() {
		return PartInfo{}, ErrAlreadyFinalized
	}
	if number == 0 || number > maxPartNumber {
		return PartInfo{}, ErrInvalidPart
	}
	if existing, ok := u.meta.Parts[number]; ok {
		return existing, nil
	}

	partPath := filepath.Join(u.s.uploadDir(u.meta.ID), "parts", fmt.Sprintf("%08d", number))
	tmpPath := partPath + ".tmp-" + randomSuffix()
	f, err := os.OpenFile(tmpPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, filePerm)
	if err != nil {
		return PartInfo{}, err
	}
	h := sha256.New()
	n, err := io.Copy(io.MultiWriter(f, h), r)
	if err != nil {
		f.Close()
		os.Remove(tmpPath)
		return PartInfo{}, err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return PartInfo{}, err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmpPath)
		return PartInfo{}, err
	}
	if err := os.Rename(tmpPath, partPath); err != nil {
		os.Remove(tmpPath)
		return PartInfo{}, err
	}
	if err := syncDir(filepath.Dir(partPath)); err != nil {
		return PartInfo{}, err
	}

	info := PartInfo{Number: number, Size: n, SHA256: hex.EncodeToString(h.Sum(nil))}
	u.meta.Parts[number] = info
	if err := u.s.writeMeta(&u.meta); err != nil {
		return PartInfo{}, err
	}
	return info, nil
}

func orderedParts(m *uploadMeta) ([]PartInfo, error) {
	if len(m.Parts) == 0 {
		return nil, ErrInvalidPart
	}
	numbers := make([]int, 0, len(m.Parts))
	for n := range m.Parts {
		numbers = append(numbers, int(n))
	}
	sort.Ints(numbers)
	parts := make([]PartInfo, 0, len(numbers))
	for i, n := range numbers {
		if uint32(n) != uint32(i+1) {
			return nil, ErrInvalidPart
		}
		parts = append(parts, m.Parts[uint32(n)])
	}
	return parts, nil
}

// Abort discards all fragments of this upload.
func (u *Upload) Abort() error {
	if u.finalized.Load() {
		return ErrAlreadyFinalized
	}
	u.markFinalized()
	if err := os.RemoveAll(u.s.uploadDir(u.meta.ID)); err != nil {
		return err
	}
	return syncDir(u.s.path("uploads"))
}

// CompleteUpload assembles all parts, verifies the result and atomically
// publishes it. The returned Version is stable forever: its ID is the SHA-256
// of the full object content.
func (u *Upload) CompleteUpload() (Version, error) {
	if u.finalized.Load() {
		return Version{}, ErrAlreadyFinalized
	}
	u.s.mu.Lock()
	defer u.s.mu.Unlock()

	if u.meta.State == stateCommitted || u.finalized.Load() {
		return Version{}, ErrAlreadyFinalized
	}
	parts, err := orderedParts(&u.meta)
	if err != nil {
		return Version{}, err
	}

	// Flip to assembling so recovery knows a crash happened mid-assembly.
	u.meta.State = stateAssembling
	if err := u.s.writeMeta(&u.meta); err != nil {
		return Version{}, err
	}

	version, size, err := u.s.assemble(u.meta.ID, parts)
	if err != nil {
		return Version{}, err
	}
	u.meta.Version = version
	u.meta.Size = size
	u.meta.State = stateAssembled
	if err := u.s.writeMeta(&u.meta); err != nil {
		return Version{}, err
	}
	if u.s.hook != nil {
		u.s.hook("after_assemble")
	}

	v, err := u.s.publish(&u.meta)
	if err != nil {
		return Version{}, err
	}
	u.markFinalized()
	return v, nil
}

func (s *Store) assemble(uploadID string, parts []PartInfo) (string, int64, error) {
	dir := s.uploadDir(uploadID)
	staged := filepath.Join(dir, "staged")
	tmp := staged + ".tmp-" + randomSuffix()
	out, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, filePerm)
	if err != nil {
		return "", 0, err
	}
	var total int64
	fail := func(err error) (string, int64, error) {
		out.Close()
		os.Remove(tmp)
		return "", 0, err
	}
	for _, p := range parts {
		pf, err := os.Open(filepath.Join(dir, "parts", fmt.Sprintf("%08d", p.Number)))
		if err != nil {
			return fail(err)
		}
		ph := sha256.New()
		n, err := io.Copy(io.MultiWriter(out, ph), pf)
		pf.Close()
		if err != nil {
			return fail(err)
		}
		if n != p.Size || hex.EncodeToString(ph.Sum(nil)) != p.SHA256 {
			return fail(ErrCorrupt)
		}
		total += n
	}
	if err := out.Sync(); err != nil {
		return fail(err)
	}
	if err := out.Close(); err != nil {
		os.Remove(tmp)
		return "", 0, err
	}
	if err := os.Rename(tmp, staged); err != nil {
		os.Remove(tmp)
		return "", 0, err
	}
	if err := syncDir(dir); err != nil {
		return "", 0, err
	}
	return hashFile(staged)
}
