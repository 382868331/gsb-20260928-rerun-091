package localobjstore

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"time"
)

const corruptionName = "corruption.json"

func (s *Store) corruptionPath(key string) string {
	return filepath.Join(s.objectDir(key), corruptionName)
}

func (s *Store) readCorruption(key string) (CorruptionRecord, error) {
	var rec CorruptionRecord
	b, err := os.ReadFile(s.corruptionPath(key))
	if errors.Is(err, os.ErrNotExist) {
		return rec, nil
	}
	if err != nil {
		return rec, err
	}
	if err := json.Unmarshal(b, &rec); err != nil {
		return rec, fmt.Errorf("%w: corruption record: %v", ErrCorrupted, err)
	}
	return rec, nil
}

func writeCorruption(dir string, rec CorruptionRecord) error {
	b, err := json.MarshalIndent(&rec, "", "  ")
	if err != nil {
		return err
	}
	return writeFileAtomic(dir, corruptionName, append(b, '\n'), 0o644)
}

func (s *Store) writeCorruptionRecord(rec CorruptionRecord) error {
	if err := os.MkdirAll(s.objectDir(rec.Key), 0o755); err != nil {
		return err
	}
	return writeCorruption(s.objectDir(rec.Key), rec)
}

// ListCorruptions returns durable current-version corruption records, oldest
// first. Resolved records are included so the original fault evidence remains
// auditable.
func (s *Store) ListCorruptions() ([]CorruptionRecord, error) {
	root := filepath.Join(s.root, "objects")
	entries, err := os.ReadDir(root)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var out []CorruptionRecord
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		b, err := os.ReadFile(filepath.Join(root, e.Name(), corruptionName))
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, err
		}
		var rec CorruptionRecord
		if err := json.Unmarshal(b, &rec); err != nil {
			return nil, err
		}
		out = append(out, rec)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].DetectedAt.Equal(out[j].DetectedAt) {
			return out[i].Key < out[j].Key
		}
		return out[i].DetectedAt.Before(out[j].DetectedAt)
	})
	return out, nil
}

// ListRecoveryCandidates lists readable historical versions for key and
// verifies every byte of each candidate. The damaged current version is not
// offered as a candidate.
func (s *Store) ListRecoveryCandidates(key string) ([]RecoveryCandidate, error) {
	rec, err := s.readCorruption(key)
	if err != nil {
		return nil, err
	}
	if !rec.Active {
		return nil, fmt.Errorf("%w: no active corruption for %s", ErrNotFound, key)
	}
	versions, err := s.ListVersions(key)
	if err != nil {
		return nil, err
	}
	out := make([]RecoveryCandidate, 0, len(versions))
	for _, m := range versions {
		c := RecoveryCandidate{Manifest: m}
		r, err := s.GetVersion(key, m.VersionID)
		if err != nil {
			c.Error = err.Error()
		} else {
			if err := r.Verify(); err != nil {
				c.Error = err.Error()
			} else {
				c.Verified = true
			}
			r.Close()
		}
		out = append(out, c)
	}
	return out, nil
}

// RestoreVersion creates a new current version from a verified historical
// version. The source version and quarantined damaged version are never
// modified.
func (s *Store) RestoreVersion(key, sourceVersionID string) (Manifest, error) {
	s.pubMu.Lock()
	defer s.pubMu.Unlock()
	lock, err := acquireFlock(filepath.Join(s.root, "locks", "publish-"+objectID(key)+".lock"))
	if err != nil {
		return Manifest{}, err
	}
	defer lock.release()

	rec, err := s.readCorruption(key)
	if err != nil {
		return Manifest{}, err
	}
	if !rec.Active {
		return Manifest{}, fmt.Errorf("%w: no active corruption for %s", ErrNotFound, key)
	}
	if sourceVersionID == rec.CorruptedVersionID {
		return Manifest{}, fmt.Errorf("%w: corrupted version cannot be restored", ErrCorrupted)
	}
	if err := s.validateVersionStructure(key, sourceVersionID); err != nil {
		return Manifest{}, err
	}
	src, err := s.loadManifest(key, sourceVersionID)
	if err != nil {
		return Manifest{}, err
	}
	if err := verifyDirBytes(filepath.Join(s.versionsDir(key), sourceVersionID), src); err != nil {
		return Manifest{}, err
	}

	newID, err := newVersionID()
	if err != nil {
		return Manifest{}, err
	}
	staged := filepath.Join(s.root, "staging", "publish", newID)
	if err := copyVersionTree(filepath.Join(s.versionsDir(key), sourceVersionID), staged); err != nil {
		_ = os.RemoveAll(staged)
		return Manifest{}, err
	}
	src.VersionID = newID
	src.CreatedAt = parseVersionTime(newID)
	b, err := json.MarshalIndent(&src, "", "  ")
	if err != nil {
		_ = os.RemoveAll(staged)
		return Manifest{}, err
	}
	if err := writeFileAtomic(staged, manifestName, append(b, '\n'), 0o444); err != nil {
		_ = os.RemoveAll(staged)
		return Manifest{}, err
	}
	if err := fsyncDir(staged); err != nil {
		_ = os.RemoveAll(staged)
		return Manifest{}, err
	}
	rec.RestorePendingID = newID
	if err := s.writeCorruptionRecord(rec); err != nil {
		_ = os.RemoveAll(staged)
		return Manifest{}, err
	}
	dst := filepath.Join(s.versionsDir(key), newID)
	if err := os.MkdirAll(s.versionsDir(key), 0o755); err != nil {
		_ = os.RemoveAll(staged)
		rec.RestorePendingID = ""
		_ = s.writeCorruptionRecord(rec)
		return Manifest{}, err
	}
	if err := os.Rename(staged, dst); err != nil {
		_ = os.RemoveAll(staged)
		rec.RestorePendingID = ""
		_ = s.writeCorruptionRecord(rec)
		return Manifest{}, err
	}
	if err := fsyncDir(s.versionsDir(key)); err != nil {
		return Manifest{}, err
	}
	if err := s.writeCurrent(key, newID); err != nil {
		return Manifest{}, err
	}
	if err := os.Remove(filepath.Join(s.objectDir(key), deletedName)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return Manifest{}, err
	}
	now := time.Now().UTC()
	rec.Active = false
	rec.SourceVersionID = sourceVersionID
	rec.RestoredVersionID = newID
	rec.ResolvedAt = &now
	if err := s.writeCorruptionRecord(rec); err != nil {
		return Manifest{}, err
	}
	return src, nil
}

func copyVersionTree(src, dst string) error {
	entries, err := os.ReadDir(filepath.Join(src, dataDirName))
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Join(dst, dataDirName), 0o755); err != nil {
		return err
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		in, err := os.Open(filepath.Join(src, dataDirName, e.Name()))
		if err != nil {
			return err
		}
		out, err := os.OpenFile(filepath.Join(dst, dataDirName, e.Name()), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o444)
		if err != nil {
			in.Close()
			return err
		}
		_, cpErr := io.Copy(out, in)
		syncErr := out.Sync()
		closeErr := out.Close()
		in.Close()
		if cpErr != nil {
			return cpErr
		}
		if syncErr != nil {
			return syncErr
		}
		if closeErr != nil {
			return closeErr
		}
	}
	return fsyncDir(filepath.Join(dst, dataDirName))
}
