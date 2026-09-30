package localobjstore

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// refKey is the composite address of one pinned version.
func refKey(key, versionID string) string {
	return objectID(key) + "/" + versionID
}

func (s *Store) refsDir(key string) string {
	return filepath.Join(s.root, "refs", objectID(key))
}

func (s *Store) refPath(key, refID string) string {
	return filepath.Join(s.refsDir(key), refID+".json")
}

func (s *Store) deletedPath(key string) string {
	return filepath.Join(s.root, "deleted", objectID(key)+".json")
}

// addPin registers a transient reference held by an open reader.
func (s *Store) addPin(key, versionID string) {
	s.pinsMu.Lock()
	s.pins[refKey(key, versionID)]++
	s.pinsMu.Unlock()
}

// releasePin drops one transient reader reference.
func (s *Store) releasePin(key, versionID string) {
	s.pinsMu.Lock()
	k := refKey(key, versionID)
	if s.pins[k] > 1 {
		s.pins[k]--
	} else {
		delete(s.pins, k)
	}
	s.pinsMu.Unlock()
}

// pinnedInMemory reports whether any open reader in this process pins vid.
func (s *Store) pinnedInMemory(key, versionID string) bool {
	s.pinsMu.Lock()
	defer s.pinsMu.Unlock()
	return s.pins[refKey(key, versionID)] > 0
}

// Snapshot pins a specific published version durably. The returned Ref keeps
// that exact version (including its bytes) readable even after the object's
// current pointer moves on or the object is deleted, until ReleaseSnapshot is
// called. The version must exist and be structurally valid.
func (s *Store) Snapshot(key, versionID string) (Ref, error) {
	// Fast-fail before taking any locks.
	if err := s.validateVersionStructure(key, versionID); err != nil {
		return Ref{}, err
	}
	// Serialize with GC for this object: GC holds both the per-key publish
	// lock and this lock while deciding a version's fate. We also exclude
	// in-process GC via gcMu so the version cannot be moved after validation.
	s.gcMu.RLock()
	defer s.gcMu.RUnlock()
	lock, err := acquireFlock(filepath.Join(s.root, "locks", "publish-"+objectID(key)+".lock"))
	if err != nil {
		return Ref{}, err
	}
	defer lock.release()
	refLock, err := acquireFlock(filepath.Join(s.root, "locks", "refs-"+objectID(key)+".lock"))
	if err != nil {
		return Ref{}, err
	}
	defer refLock.release()
	if err := s.validateVersionStructure(key, versionID); err != nil {
		return Ref{}, err
	}
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return Ref{}, err
	}
	r := Ref{
		ID:        hex.EncodeToString(b[:]),
		Key:       key,
		VersionID: versionID,
		CreatedAt: time.Now().UTC(),
	}
	data, err := json.MarshalIndent(&r, "", "  ")
	if err != nil {
		return Ref{}, err
	}
	if err := writeFileAtomic(s.refsDir(key), r.ID+".json", append(data, '\n'), 0o444); err != nil {
		return Ref{}, err
	}
	return r, nil
}

// ReleaseSnapshot removes a durable reference. After release the referenced
// version enters the reclaimable set (subject to retention) on the next GC.
func (s *Store) ReleaseSnapshot(key, refID string) error {
	err := os.Remove(s.refPath(key, refID))
	if os.IsNotExist(err) {
		return fmt.Errorf("%w: snapshot %s", ErrNotFound, refID)
	}
	if err != nil {
		return err
	}
	return fsyncDir(s.refsDir(key))
}

// ListSnapshots returns the durable snapshots currently held for key.
func (s *Store) ListSnapshots(key string) ([]Ref, error) {
	entries, err := os.ReadDir(s.refsDir(key))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var out []Ref
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		b, err := os.ReadFile(filepath.Join(s.refsDir(key), e.Name()))
		if err != nil {
			continue
		}
		var r Ref
		if json.Unmarshal(b, &r) == nil {
			out = append(out, r)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.Before(out[j].CreatedAt) })
	return out, nil
}

// durableRefs returns the set of version ids still pinned by snapshot files
// for key. Snapshot files that no longer parse or point at a non-existent
// version are returned as stale for recovery to discard.
func (s *Store) durableRefs(key string) (map[string]bool, []string, error) {
	live := make(map[string]bool)
	var stale []string
	entries, err := os.ReadDir(s.refsDir(key))
	if os.IsNotExist(err) {
		return live, nil, nil
	}
	if err != nil {
		return nil, nil, err
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		var r Ref
		b, err := os.ReadFile(filepath.Join(s.refsDir(key), e.Name()))
		if err != nil {
			stale = append(stale, e.Name())
			continue
		}
		if json.Unmarshal(b, &r) != nil || r.Key != key || r.VersionID == "" {
			stale = append(stale, e.Name())
			continue
		}
		if _, serr := os.Stat(filepath.Join(s.versionsDir(key), r.VersionID, manifestName)); serr != nil {
			stale = append(stale, e.Name())
			continue
		}
		live[r.VersionID] = true
	}
	return live, stale, nil
}

// readTombstone returns nil for objects that have not been deleted.
func (s *Store) readTombstone(key string) (*Tombstone, error) {
	b, err := os.ReadFile(s.deletedPath(key))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var t Tombstone
	if err := json.Unmarshal(b, &t); err != nil {
		return nil, err
	}
	return &t, nil
}

func (s *Store) writeTombstone(t *Tombstone) error {
	b, err := json.MarshalIndent(t, "", "  ")
	if err != nil {
		return err
	}
	return writeFileAtomic(filepath.Join(s.root, "deleted"), objectID(t.Key)+".json", append(b, '\n'), 0o444)
}

// isReferenced reports a durable snapshot or an open reader in this process.
func (s *Store) isReferenced(key, versionID string) (bool, error) {
	if s.pinnedInMemory(key, versionID) {
		return true, nil
	}
	durable, _, err := s.durableRefs(key)
	if err != nil {
		return false, err
	}
	return durable[versionID], nil
}

// listAllObjectIDs returns the object-id directories under objects/.
func (s *Store) listAllObjectIDs() ([]string, error) {
	entries, err := os.ReadDir(filepath.Join(s.root, "objects"))
	if err != nil {
		return nil, err
	}
	var ids []string
	for _, e := range entries {
		if e.IsDir() {
			ids = append(ids, e.Name())
		}
	}
	sort.Strings(ids)
	return ids, nil
}
