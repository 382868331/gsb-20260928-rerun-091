package localobjstore

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// Pin is a durable named reference ("snapshot") that retains one immutable
// version. A pinned version is never reclaimed, even after the object itself
// is deleted, until the pin is explicitly removed.
type Pin struct {
	Name      string    `json:"name"`
	Key       string    `json:"key"`
	VersionID string    `json:"version_id"`
	CreatedAt time.Time `json:"created_at"`
}

// GCItem reports one reclamation decision made by CollectGarbage.
type GCItem struct {
	Key       string
	VersionID string
	Action    string // "reclaimed" | "discarded" | "retained"
	Reason    string
}

// GCReport summarizes one CollectGarbage pass.
type GCReport struct {
	Items []GCItem
}

func (s *Store) refsDir() string  { return filepath.Join(s.root, "refs") }
func (s *Store) trashDir() string { return filepath.Join(s.root, "trash") }

func (s *Store) pinPath(name string) string {
	return filepath.Join(s.refsDir(), objectID(name)+".json")
}

// PinVersion creates a durable named reference to versionID of key. The
// version must exist and be structurally valid; while the pin exists the
// version is retained across deletes and garbage collection.
func (s *Store) PinVersion(name, key, versionID string) error {
	if name == "" {
		return errors.New("localobjstore: empty pin name")
	}
	if err := s.validateVersionStructure(key, versionID); err != nil {
		return err
	}
	p := Pin{Name: name, Key: key, VersionID: versionID, CreatedAt: time.Now().UTC()}
	b, err := json.Marshal(&p)
	if err != nil {
		return err
	}
	return writeFileAtomic(s.refsDir(), filepath.Base(s.pinPath(name)), b, 0o644)
}

// UnpinVersion releases a named reference. The referenced version becomes
// reclaimable once no other pin, reader or current pointer retains it.
func (s *Store) UnpinVersion(name string) error {
	path := s.pinPath(name)
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("%w: pin %q", ErrNotFound, name)
	}
	if err := os.Remove(path); err != nil {
		return err
	}
	return fsyncDir(s.refsDir())
}

// ListPins returns all durable references, sorted by name.
func (s *Store) ListPins() ([]Pin, error) {
	entries, err := os.ReadDir(s.refsDir())
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var pins []Pin
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		b, err := os.ReadFile(filepath.Join(s.refsDir(), e.Name()))
		if err != nil {
			return nil, err
		}
		var p Pin
		if json.Unmarshal(b, &p) != nil {
			continue
		}
		pins = append(pins, p)
	}
	sort.Slice(pins, func(i, j int) bool { return pins[i].Name < pins[j].Name })
	return pins, nil
}

// pinsForKey returns the set of version ids of key retained by pins.
func (s *Store) pinsForKey(key string) (map[string]bool, error) {
	pins, err := s.ListPins()
	if err != nil {
		return nil, err
	}
	set := make(map[string]bool)
	for _, p := range pins {
		if p.Key == key {
			set[p.VersionID] = true
		}
	}
	return set, nil
}

// DeleteObject removes the current pointer: the key disappears from
// GetObject/CurrentVersion, but its immutable versions are not erased. Every
// version stays readable via GetVersion and remains on disk until it is
// unreferenced (not current, not pinned, not held by an open reader) and a
// CollectGarbage pass reclaims it. Deleting a key with no current version
// returns ErrNotFound.
//
// A durable "deleted" tombstone is written so crash recovery never
// resurrects the object by rebuilding its current pointer; the tombstone is
// lifted by the next successful publish of the key.
func (s *Store) DeleteObject(key string) error {
	s.pubMu.Lock()
	defer s.pubMu.Unlock()
	lock, err := acquireFlock(filepath.Join(s.root, "locks", "publish-"+objectID(key)+".lock"))
	if err != nil {
		return err
	}
	defer lock.release()

	cur, err := s.readCurrent(key)
	if err != nil {
		return err
	}
	if cur == "" {
		// A faulted object has no current pointer but still exists: deleting
		// it resolves the fault with a tombstone so its remaining versions
		// become ordinary GC candidates.
		if rec, ok, ferr := s.Fault(key); ferr != nil {
			return ferr
		} else if ok {
			if werr := writeFileAtomic(s.objectDir(key), deletedName, []byte(rec.VersionID+"\n"), 0o644); werr != nil {
				return werr
			}
			if rerr := os.Remove(s.faultPath(key)); rerr != nil {
				return rerr
			}
			return fsyncDir(s.objectDir(key))
		}
		return fmt.Errorf("%w: %s", ErrNotFound, key)
	}
	if err := writeFileAtomic(s.objectDir(key), deletedName, []byte(cur+"\n"), 0o644); err != nil {
		return err
	}
	if err := os.Remove(filepath.Join(s.objectDir(key), currentName)); err != nil {
		return err
	}
	return fsyncDir(s.objectDir(key))
}

// trackReader registers an open reader so GC cannot reclaim the version it
// is reading. The returned function releases the reference exactly once.
func (s *Store) trackReader(key, versionID string) func() {
	ref := readerRef{key: key, vid: versionID}
	s.gcMu.Lock()
	s.readers[ref]++
	s.gcMu.Unlock()
	var released bool
	return func() {
		s.gcMu.Lock()
		defer s.gcMu.Unlock()
		if released {
			return
		}
		released = true
		if s.readers[ref] <= 1 {
			delete(s.readers, ref)
		} else {
			s.readers[ref]--
		}
	}
}

// CollectGarbage reclaims every version that is no longer referenced: not the
// current version of its key, not pinned, and not held by any open reader of
// this process. Reclamation is crash-safe: a version directory is first
// atomically moved into trash/ (the point of no return) and only then
// deleted, so an interrupted pass never leaves a half-deleted version inside
// the live tree. Leftover trash from an interrupted pass is discarded by the
// next Open.
//
// CollectGarbage never touches staging/: in-progress multipart parts and
// not-yet-published versions are out of scope, and the per-key publish lock
// (also held by publish and DeleteObject) serialises GC against concurrent
// publishers so a version that is becoming current is never swept.
func (s *Store) CollectGarbage() (GCReport, error) {
	s.gcMu.Lock()
	defer s.gcMu.Unlock()

	var rep GCReport
	// Discard residue of an interrupted previous pass first.
	rep.Items = append(rep.Items, s.discardTrash()...)

	objs, err := os.ReadDir(filepath.Join(s.root, "objects"))
	if errors.Is(err, os.ErrNotExist) {
		return rep, nil
	}
	if err != nil {
		return rep, err
	}
	for _, od := range objs {
		if !od.IsDir() {
			continue
		}
		if err := s.collectObject(od.Name(), &rep); err != nil {
			return rep, err
		}
	}
	return rep, nil
}

// discardTrash removes leftover trash entries from an interrupted GC pass.
// Their content was already logically reclaimed before the interruption.
func (s *Store) discardTrash() []GCItem {
	var items []GCItem
	entries, err := os.ReadDir(s.trashDir())
	if err != nil {
		return items
	}
	for _, e := range entries {
		path := filepath.Join(s.trashDir(), e.Name())
		if err := os.RemoveAll(path); err == nil {
			items = append(items, GCItem{Action: "discarded", Reason: "interrupted previous reclamation", VersionID: e.Name()})
		}
	}
	if len(items) > 0 {
		_ = fsyncDir(s.trashDir())
	}
	return items
}

// collectObject sweeps one object's version tree. objID is the hashed key
// directory name; the key itself is recovered from manifests.
func (s *Store) collectObject(objID string, rep *GCReport) error {
	objPath := filepath.Join(s.root, "objects", objID)
	lock, err := acquireFlock(filepath.Join(s.root, "locks", "publish-"+objID+".lock"))
	if err != nil {
		return err
	}
	defer lock.release()

	vdir := filepath.Join(objPath, "versions")
	entries, err := os.ReadDir(vdir)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	curData, _ := os.ReadFile(filepath.Join(objPath, currentName))
	current := strings.TrimSpace(string(curData))
	// An object with a recorded corruption fault keeps every remaining
	// version: they are the operator's restore candidates and must never be
	// reclaimed out from under a pending manual recovery, even when readers,
	// tombstones and GC passes coincide.
	_, faulted, err := s.readFaultByObjID(objID)
	if err != nil {
		return err
	}

	for _, ve := range entries {
		if !ve.IsDir() || !strings.HasPrefix(ve.Name(), "v") {
			continue
		}
		vid := ve.Name()
		m, err := readAnyManifest(filepath.Join(vdir, vid))
		if err != nil {
			continue // corrupt versions are recovery's job, never GC's
		}
		key := m.Key
		if vid == current {
			continue
		}
		if faulted {
			rep.Items = append(rep.Items, GCItem{Key: key, VersionID: vid, Action: "retained", Reason: "fault recovery candidate"})
			continue
		}
		pinned, err := s.pinsForKey(key)
		if err != nil {
			return err
		}
		if pinned[vid] {
			rep.Items = append(rep.Items, GCItem{Key: key, VersionID: vid, Action: "retained", Reason: "pinned"})
			continue
		}
		inPoint, err := s.restorePointsForKey(key)
		if err != nil {
			return err
		}
		if inPoint[vid] {
			rep.Items = append(rep.Items, GCItem{Key: key, VersionID: vid, Action: "retained", Reason: "restore point member"})
			continue
		}
		pendingRestore, err := s.pendingRestoreRefs(key)
		if err != nil {
			return err
		}
		if pendingRestore[vid] {
			rep.Items = append(rep.Items, GCItem{Key: key, VersionID: vid, Action: "retained", Reason: "pending group restore"})
			continue
		}
		if s.readers[readerRef{key: key, vid: vid}] > 0 {
			rep.Items = append(rep.Items, GCItem{Key: key, VersionID: vid, Action: "retained", Reason: "open reader"})
			continue
		}
		// Crash-safe reclamation: rename into trash, fsync, then delete.
		trash := filepath.Join(s.trashDir(), time.Now().UTC().Format("20060102T150405.000000000")+"-"+objID+"-"+vid)
		if err := os.Rename(filepath.Join(vdir, vid), trash); err != nil {
			return err
		}
		if err := fsyncDir(vdir); err != nil {
			return err
		}
		if err := os.RemoveAll(trash); err != nil {
			return err
		}
		rep.Items = append(rep.Items, GCItem{Key: key, VersionID: vid, Action: "reclaimed", Reason: "unreferenced"})
	}
	if err := fsyncDir(s.trashDir()); err != nil {
		return err
	}
	return fsyncDir(vdir)
}
