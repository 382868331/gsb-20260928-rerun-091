package objectstore

import (
	"encoding/json"
	"os"
	"path/filepath"
)

// publish installs the assembled, already-verified blob into content storage
// and atomically advances the key pointer. Caller holds the per-key lock
// conceptually; publish acquires it (cross-process flock).
func (s *Store) publish(m *uploadMeta) (Version, error) {
	lockPath := s.path(keyDir(m.Key), "lock")
	lk, err := acquireLock(lockPath)
	if err != nil {
		return Version{}, err
	}
	defer lk.release()

	now := nowUTC()
	cur, err := s.currentPointer(m.Key)
	if err != nil {
		return Version{}, err
	}
	if err := checkExpected(m.ExpectedVersion, cur); err != nil {
		return Version{}, err
	}

	// Install immutable blob (content-addressed; same content = same version).
	blobPath, err := s.blobPath(m.Version)
	if err != nil {
		return Version{}, err
	}
	if _, statErr := os.Stat(blobPath); statErr != nil {
		if !os.IsNotExist(statErr) {
			return Version{}, statErr
		}
		if err := s.installBlob(m, blobPath); err != nil {
			return Version{}, err
		}
	}

	p := pointer{Version: m.Version, Size: m.Size, PublishedAt: now, UploadID: m.ID}
	if err := s.writePointer(m.Key, p); err != nil {
		return Version{}, err
	}

	m.State = stateCommitted
	if err := s.writeMeta(m); err != nil {
		return Version{}, err
	}

	// Upload session is fully committed; remove its fragments.
	if err := os.RemoveAll(s.uploadDir(m.ID)); err != nil {
		return Version{}, err
	}
	_ = syncDir(s.path("uploads"))

	return Version{Key: m.Key, Version: m.Version, Size: m.Size, PublishedAt: now}, nil
}

func checkExpected(expected string, cur *pointer) error {
	if expected == "" {
		if cur != nil {
			return ErrConflict
		}
		return nil
	}
	// Non-empty expectation: wildcard "*" means "object must already exist".
	if expected == "*" {
		if cur == nil {
			return ErrNotFound
		}
		return nil
	}
	if cur == nil || cur.Version != expected {
		return ErrConflict
	}
	return nil
}

func (s *Store) installBlob(m *uploadMeta, blobPath string) error {
	if err := os.MkdirAll(filepath.Dir(blobPath), dirPerm); err != nil {
		return err
	}
	src := filepath.Join(s.uploadDir(m.ID), "staged")
	// Link into content storage atomically, then verify the linked bytes.
	tmpLink := blobPath + ".tmp-" + randomSuffix()
	if err := os.Link(src, tmpLink); err != nil {
		return err
	}
	got, size, err := hashFile(tmpLink)
	if err != nil {
		os.Remove(tmpLink)
		return err
	}
	if got != m.Version || size != m.Size {
		os.Remove(tmpLink)
		return ErrCorrupt
	}
	if err := os.Chmod(tmpLink, blobPerm); err != nil {
		os.Remove(tmpLink)
		return err
	}
	if err := os.Rename(tmpLink, blobPath); err != nil {
		os.Remove(tmpLink)
		return err
	}
	if err := syncDir(filepath.Dir(blobPath)); err != nil {
		return err
	}
	return nil
}

func (s *Store) writePointer(key string, p pointer) error {
	data, err := json.MarshalIndent(p, "", "  ")
	if err != nil {
		return err
	}
	return writeFileAtomic(s.path(keyDir(key), "pointer"), data, filePerm)
}

func (s *Store) currentPointer(key string) (*pointer, error) {
	data, err := os.ReadFile(s.path(keyDir(key), "pointer"))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var p pointer
	if err := json.Unmarshal(data, &p); err != nil {
		return nil, err
	}
	if p.Version == "" {
		return nil, nil
	}
	return &p, nil
}
