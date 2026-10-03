package localobjstore

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
)

// Upload tracks one in-progress multipart write. Its staging directory holds
// parts with temp names until fsync+rename commits each part, so a crash can
// never leave a part that looks complete but is only half on disk.
type Upload struct {
	store *Store
	ID    string
	Key   string
	dir   string

	mu      chan struct{}
	parts   []PartInfo
	nextNum int32
	aborted bool
}

type uploadMeta struct {
	ID  string `json:"id"`
	Key string `json:"key"`
}

// BeginMultipart starts a segmented upload for key.
func (s *Store) BeginMultipart(key string) (*Upload, error) {
	if key == "" {
		return nil, errors.New("localobjstore: empty key")
	}
	id, err := newUploadID()
	if err != nil {
		return nil, err
	}
	dir := filepath.Join(s.root, "staging", "uploads", id)
	if err := os.MkdirAll(filepath.Join(dir, partsDirName), 0o755); err != nil {
		return nil, err
	}
	meta := uploadMeta{ID: id, Key: key}
	b, _ := json.Marshal(&meta)
	if err := writeFileAtomic(dir, "meta.json", b, 0o644); err != nil {
		_ = os.RemoveAll(dir)
		return nil, err
	}
	u := &Upload{store: s, ID: id, Key: key, dir: dir, mu: make(chan struct{}, 1), nextNum: 1}
	s.upMu.Lock()
	s.uploads[id] = u
	s.upMu.Unlock()
	return u, nil
}

// WritePart appends one segment. Parts must be written strictly in order
// starting at 1; the part is committed with an fsynced atomic rename and its
// returned PartInfo is the checksum receipt.
func (u *Upload) WritePart(r io.Reader) (PartInfo, error) {
	u.mu <- struct{}{}
	defer func() { <-u.mu }()
	if u.aborted {
		return PartInfo{}, ErrUploadNotFound
	}
	number := u.nextNum
	final := filepath.Join(u.dir, partsDirName, partFileName(number))
	tmp, err := os.CreateTemp(filepath.Join(u.dir, partsDirName), ".part-tmp-*")
	if err != nil {
		return PartInfo{}, err
	}
	tmpName := tmp.Name()
	hw := sha256.New()
	n, err := io.Copy(io.MultiWriter(tmp, hw), r)
	if err != nil {
		tmp.Close()
		_ = os.Remove(tmpName)
		return PartInfo{}, err
	}
	if err := tmp.Chmod(0o444); err != nil {
		tmp.Close()
		_ = os.Remove(tmpName)
		return PartInfo{}, err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		_ = os.Remove(tmpName)
		return PartInfo{}, err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpName)
		return PartInfo{}, err
	}
	// Atomic commit of the part.
	if err := durableRename(tmpName, final); err != nil {
		_ = os.Remove(tmpName)
		return PartInfo{}, err
	}
	info := PartInfo{Number: number, Size: n, Sha256: hex.EncodeToString(hw.Sum(nil))}
	u.parts = append(u.parts, info)
	u.nextNum++
	return info, nil
}

// Abort discards the upload. Its staging directory is removed immediately;
// an interrupted process is cleaned up by Open's recovery.
func (u *Upload) Abort() error {
	u.mu <- struct{}{}
	defer func() { <-u.mu }()
	if u.aborted {
		return ErrUploadNotFound
	}
	u.aborted = true
	u.store.forgetUpload(u.ID)
	if err := os.RemoveAll(u.dir); err != nil {
		return err
	}
	return fsyncDir(filepath.Join(u.store.root, "staging", "uploads"))
}

func (s *Store) forgetUpload(id string) {
	s.upMu.Lock()
	delete(s.uploads, id)
	s.upMu.Unlock()
}

// Complete validates receipts, assembles the immutable version, and performs
// the atomic publish. expectedVersion enables optimistic concurrency:
//   - expectedVersion == "" means "only publish if the key is new";
//   - any other value must equal the current version id.
//
// Omit expectedVersion entirely (nil) for plain last-writer-wins.
//
// With multiple processes, the loser is decided deterministically by version
// id (time-ordered): the highest version id becomes "current".
func (u *Upload) Complete(expectedVersion *string) (Manifest, error) {
	u.mu <- struct{}{}
	defer func() { <-u.mu }()
	if u.aborted {
		return Manifest{}, ErrUploadNotFound
	}
	if len(u.parts) == 0 {
		return Manifest{}, fmt.Errorf("%w: no parts", ErrInvalidParts)
	}
	for i, p := range u.parts {
		if p.Number != int32(i+1) {
			return Manifest{}, fmt.Errorf("%w: expected part %d, got %d", ErrInvalidParts, i+1, p.Number)
		}
	}

	vid, err := newVersionID()
	if err != nil {
		return Manifest{}, err
	}
	m, err := u.store.buildVersion(u.Key, vid, u.parts, u.dir)
	if err != nil {
		return Manifest{}, err
	}
	if err := u.store.publish(m, expectedVersion); err != nil {
		return Manifest{}, err
	}
	u.aborted = true
	u.store.forgetUpload(u.ID)
	_ = os.RemoveAll(u.dir)
	_ = fsyncDir(filepath.Join(u.store.root, "staging", "uploads"))
	return m, nil
}

// buildVersion concatenates and verifies every part into a staging version
// directory, writes the manifest, and fsyncs everything before the rename
// that makes the version visible.
func (s *Store) buildVersion(key, vid string, parts []PartInfo, uploadDir string) (Manifest, error) {
	pdir := filepath.Join(s.root, "staging", "publish", vid)
	if err := os.MkdirAll(filepath.Join(pdir, dataDirName), 0o755); err != nil {
		return Manifest{}, err
	}
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.RemoveAll(pdir)
		}
	}()

	totalHash := sha256.New()
	verified := make([]PartInfo, 0, len(parts))
	var size int64
	for _, p := range parts {
		src := filepath.Join(uploadDir, partsDirName, partFileName(p.Number))
		h, n, err := hashFile(src)
		if err != nil {
			return Manifest{}, err
		}
		if n != p.Size || hex.EncodeToString(h.Sum(nil)) != p.Sha256 {
			return Manifest{}, fmt.Errorf("%w: part %d checksum mismatch at complete", ErrCorrupted, p.Number)
		}
		dst := filepath.Join(pdir, dataDirName, partFileName(p.Number))
		if err := copyFile(src, dst, 0o444); err != nil {
			return Manifest{}, err
		}
		pf, err := os.Open(src)
		if err != nil {
			return Manifest{}, err
		}
		if _, err := io.Copy(totalHash, pf); err != nil {
			pf.Close()
			return Manifest{}, err
		}
		pf.Close()
		size += n
		verified = append(verified, p)
	}
	m := Manifest{
		Key:       key,
		VersionID: vid,
		CreatedAt: parseVersionTime(vid),
		Size:      size,
		Sha256:    hex.EncodeToString(totalHash.Sum(nil)),
		Parts:     verified,
	}
	b, err := json.MarshalIndent(&m, "", "  ")
	if err != nil {
		return Manifest{}, err
	}
	if err := writeFileAtomic(pdir, manifestName, append(b, '\n'), 0o444); err != nil {
		return Manifest{}, err
	}
	if err := fsyncDir(filepath.Join(pdir, dataDirName)); err != nil {
		return Manifest{}, err
	}
	if err := fsyncDir(pdir); err != nil {
		return Manifest{}, err
	}
	cleanup = false
	return m, nil
}

// publish atomically moves the staged version into the object's versions tree,
// fsyncs, and then updates "current" under a cross-process lock.
//
// The per-key lock is taken before the rename (not just before the pointer
// update) so a concurrent CollectGarbage pass can never observe a version
// directory that is not yet reachable from "current" and sweep it as
// unreferenced: GC holds the same lock while evaluating the key's versions.
func (s *Store) publish(m Manifest, expectedVersion *string) error {
	s.pubMu.Lock()
	defer s.pubMu.Unlock()
	return s.publishLocked(m, expectedVersion)
}

// publishLocked is publish with the process-wide publish mutex already held
// (used by RestoreGroup to commit a whole group inside one critical
// section).
func (s *Store) publishLocked(m Manifest, expectedVersion *string) error {
	lock, err := acquireFlock(filepath.Join(s.root, "locks", "publish-"+objectID(m.Key)+".lock"))
	if err != nil {
		return err
	}
	defer lock.release()

	src := filepath.Join(s.root, "staging", "publish", m.VersionID)
	dst := filepath.Join(s.versionsDir(m.Key), m.VersionID)
	if err := os.MkdirAll(s.versionsDir(m.Key), 0o755); err != nil {
		return err
	}
	if err := os.Rename(src, dst); err != nil {
		return err
	}
	if err := fsyncDir(s.versionsDir(m.Key)); err != nil {
		return err
	}
	// A successful publish lifts any delete tombstone for the key.
	if err := os.Remove(filepath.Join(s.objectDir(m.Key), deletedName)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	// A successful publish (including a restore) also resolves any recorded
	// corruption fault: the new current version is valid and supersedes it.
	if err := os.Remove(filepath.Join(s.objectDir(m.Key), faultName)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}

	valid, _, err := s.listValidVersions(m.Key)
	if err != nil {
		return err
	}
	highest := m.VersionID
	if len(valid) > 0 && valid[len(valid)-1] > highest {
		highest = valid[len(valid)-1]
	}
	cur, err := s.readCurrent(m.Key)
	if err != nil {
		return err
	}
	if expectedVersion != nil && cur != *expectedVersion {
		return ErrConflict
	}
	if cur == highest {
		return nil
	}
	return s.writeCurrent(m.Key, highest)
}

// PutObject is the small-object convenience path: it runs the same staged,
// checksummed, atomically-published pipeline as a single-part upload. It never
// writes payload bytes straight into the object directory.
func (s *Store) PutObject(key string, r io.Reader, expectedVersion *string) (Manifest, error) {
	u, err := s.BeginMultipart(key)
	if err != nil {
		return Manifest{}, err
	}
	if _, err := u.WritePart(r); err != nil {
		_ = u.Abort()
		return Manifest{}, err
	}
	m, err := u.Complete(expectedVersion)
	if err != nil {
		return Manifest{}, err
	}
	return m, nil
}

func hashFile(path string) (hash.Hash, int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, 0, err
	}
	defer f.Close()
	h := sha256.New()
	n, err := io.Copy(h, f)
	if err != nil {
		return nil, 0, err
	}
	return h, n, nil
}

func copyFile(src, dst string, perm os.FileMode) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_EXCL, perm)
	if err != nil {
		return err
	}
	_, werr := io.Copy(out, in)
	serr := out.Sync()
	cberr := out.Close()
	if werr != nil {
		return werr
	}
	if serr != nil {
		return serr
	}
	return cberr
}
