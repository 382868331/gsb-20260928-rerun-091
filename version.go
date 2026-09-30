package localobjstore

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

func parseVersionTime(vid string) time.Time {
	vid = strings.TrimPrefix(vid, "v")
	if i := strings.IndexByte(vid, '-'); i >= 0 {
		vid = vid[:i]
	}
	n, err := strconv.ParseInt(vid, 10, 64)
	if err != nil {
		return time.Time{}
	}
	return time.Unix(0, n).UTC()
}

// CurrentVersion returns the manifest of the winning (current) version.
func (s *Store) CurrentVersion(key string) (Manifest, error) {
	vid, err := s.readCurrent(key)
	if err != nil {
		return Manifest{}, err
	}
	if vid == "" {
		return Manifest{}, fmt.Errorf("%w: %s", ErrNotFound, key)
	}
	if err := s.validateVersionStructure(key, vid); err != nil {
		return Manifest{}, err
	}
	return s.loadManifest(key, vid)
}

// ListVersions returns all published version ids for key, oldest first.
func (s *Store) ListVersions(key string) ([]Manifest, error) {
	ids, _, err := s.listValidVersions(key)
	if err != nil {
		return nil, err
	}
	out := make([]Manifest, 0, len(ids))
	for _, vid := range ids {
		m, err := s.loadManifest(key, vid)
		if err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, nil
}

// GetObject opens the current version for reading. SHA-256 checksums are
// verified lazily as bytes are consumed; Verify() checks every byte up front.
func (s *Store) GetObject(key string) (ReadSeekCloser, error) {
	vid, err := s.readCurrent(key)
	if err != nil {
		return nil, err
	}
	if vid == "" {
		return nil, fmt.Errorf("%w: %s", ErrNotFound, key)
	}
	return s.GetVersion(key, vid)
}

// GetVersion opens a specific immutable version for reading.
func (s *Store) GetVersion(key, versionID string) (ReadSeekCloser, error) {
	if err := s.validateVersionStructure(key, versionID); err != nil {
		return nil, err
	}
	m, err := s.loadManifest(key, versionID)
	if err != nil {
		return nil, err
	}
	// Register the reader before returning so a concurrent GC pass cannot
	// reclaim the version out from under it; Close releases the reference.
	release := s.trackReader(key, versionID)
	r, err := newVerifyingReader(s.versionsDir(key), m)
	if err != nil {
		release()
		return nil, err
	}
	r.release = release
	return r, nil
}

func newVerifyingReader(versionsDir string, m Manifest) (*readerImpl, error) {
	return &readerImpl{base: filepath.Join(versionsDir, m.VersionID, dataDirName), m: m, checked: make([]bool, len(m.Parts))}, nil
}

// readerImpl provides checksum-verified sequential reads, random access and a
// full Verify pass.
type readerImpl struct {
	base    string
	m       Manifest
	pos     int64
	checked []bool
	release func()
}

func (r *readerImpl) Manifest() Manifest { return r.m }

func (r *readerImpl) Close() error {
	if r.release != nil {
		r.release()
		r.release = nil
	}
	return nil
}

func (r *readerImpl) Seek(offset int64, whence int) (int64, error) {
	var np int64
	switch whence {
	case io.SeekStart:
		np = offset
	case io.SeekCurrent:
		np = r.pos + offset
	case io.SeekEnd:
		np = r.m.Size + offset
	default:
		return 0, errors.New("localobjstore: invalid whence")
	}
	if np < 0 || np > r.m.Size {
		return 0, errors.New("localobjstore: seek out of range")
	}
	r.pos = np
	return np, nil
}

// ReadAt serves random reads from any part and verifies the touched part's
// checksum before returning its bytes.
func (r *readerImpl) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 || off > r.m.Size {
		return 0, errors.New("localobjstore: read offset out of range")
	}
	total := 0
	for total < len(p) && off < r.m.Size {
		n, err := r.readRange(p[total:], off)
		total += n
		off += int64(n)
		if err != nil && err != io.EOF {
			return total, err
		}
		if n == 0 {
			break
		}
	}
	if total == 0 && len(p) > 0 {
		return 0, io.EOF
	}
	return total, nil
}

// Read sequentially serves bytes across parts, verifying every touched part's
// checksum before any of its bytes leave the engine.
func (r *readerImpl) Read(p []byte) (int, error) {
	n, err := r.readRange(p, r.pos)
	r.pos += int64(n)
	return n, err
}

func (r *readerImpl) readRange(p []byte, off int64) (int, error) {
	var partStart int64
	total := 0
	for idx := range r.m.Parts {
		end := partStart + r.m.Parts[idx].Size
		if off < end && total < len(p) {
			if !r.checked[idx] {
				if err := r.verifyPart(idx); err != nil {
					return total, err
				}
				r.checked[idx] = true
			}
			data, err := os.ReadFile(filepath.Join(r.base, partFileName(r.m.Parts[idx].Number)))
			if err != nil {
				return total, fmt.Errorf("%w: %v", ErrCorrupted, err)
			}
			seg := data[off-partStart:]
			n := copy(p[total:], seg)
			total += n
			off += int64(n)
		}
		partStart = end
	}
	var err error
	if total < len(p) {
		err = io.EOF
	}
	return total, err
}

// verifyPart hashes one part file and compares it with its receipt.
func (r *readerImpl) verifyPart(index int) error {
	pi := r.m.Parts[index]
	path := filepath.Join(r.base, partFileName(pi.Number))
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrCorrupted, err)
	}
	defer f.Close()
	h := sha256.New()
	n, err := io.Copy(h, f)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrCorrupted, err)
	}
	if n != pi.Size || hex.EncodeToString(h.Sum(nil)) != pi.Sha256 {
		return fmt.Errorf("%w: part %d checksum mismatch", ErrCorrupted, pi.Number)
	}
	return nil
}

// Verify hashes every part and the concatenated whole-object digest.
func (r *readerImpl) Verify() error {
	total := sha256.New()
	var size int64
	for i := range r.m.Parts {
		if err := r.verifyPart(i); err != nil {
			return err
		}
		path := filepath.Join(r.base, partFileName(r.m.Parts[i].Number))
		data, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("%w: %v", ErrCorrupted, err)
		}
		total.Write(data)
		size += int64(len(data))
	}
	if size != r.m.Size || hex.EncodeToString(total.Sum(nil)) != r.m.Sha256 {
		return fmt.Errorf("%w: whole-object checksum mismatch", ErrCorrupted)
	}
	return nil
}
