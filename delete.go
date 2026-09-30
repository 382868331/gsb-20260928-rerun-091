package localobjstore

import (
	"context"
	"os"
	"path/filepath"
	"time"
)

// defaultGCMinAge is the grace period applied when GC is called with nil
// options: a non-current version is only reclaimed a day after publication,
// and deleted object content a day after deletion.
const defaultGCMinAge time.Duration = 24 * time.Hour

// DeleteObject removes the current pointer of key. Payload bytes are not
// deleted: every version stays readable through existing snapshots and open
// readers, and remains on disk until references are released and the
// retention period has elapsed, at which point GC reclaims it.
//
// Deleting an already-deleted or never-published key returns ErrNotFound.
func (s *Store) DeleteObject(key string) error {
	s.gcMu.RLock()
	defer s.gcMu.RUnlock()
	s.pubMu.Lock()
	defer s.pubMu.Unlock()

	lock, err := acquireFlock(filepath.Join(s.root, "locks", "publish-"+objectID(key)+".lock"))
	if err != nil {
		return err
	}
	defer lock.release()

	vid, err := s.readCurrent(key)
	if err != nil {
		return err
	}
	t, err := s.readTombstone(key)
	if err != nil {
		return err
	}
	if vid == "" || t != nil {
		return errNotFound(key)
	}

	// The tombstone is the durable commit: write it first, then remove the
	// current pointer. A crash between the two is reconciled on next Open.
	tomb := &Tombstone{Key: key, DeletedAt: time.Now().UTC(), LastVersion: vid}
	if err := s.writeTombstone(tomb); err != nil {
		return err
	}
	if err := os.Remove(filepath.Join(s.objectDir(key), currentName)); err != nil {
		return err
	}
	return fsyncDir(s.objectDir(key))
}

// GC runs one safe reclamation pass over the whole store:
//   - versions of deleted objects are reclaimed once every durable snapshot
//     and open reader has released them and the MinAge grace period elapsed;
//   - non-current versions of live objects are reclaimed under the same
//     reference + MinAge rules (the current version is never touched);
//   - staging uploads/publishes are never scanned, so parts of an upload that
//     has not completed yet cannot be mistaken for garbage;
//   - versions that fail structural or (with VerifyContent) digest checks are
//     isolated in quarantine and never made readable again by a rebuilt
//     pointer.
//
// The pass is interruptible via ctx; on cancellation it returns the work
// already done alongside ctx.Err(). After an interruption, reopen and rerun
// GC: every candidate version and digest is re-validated from disk.
func (s *Store) GC(ctx context.Context, opts *GCOptions) (GCReport, error) {
	minAge := defaultGCMinAge
	verify := false
	if opts != nil {
		minAge = opts.MinAge
		verify = opts.VerifyContent
	}

	// Block in-process publishes/uploads completion for the whole pass.
	s.gcMu.Lock()
	defer s.gcMu.Unlock()

	var rep GCReport
	objIDs, err := s.listAllObjectIDs()
	if err != nil {
		return rep, err
	}
	now := time.Now().UTC()
	for _, objID := range objIDs {
		if err := ctx.Err(); err != nil {
			return rep, err
		}
		key := s.keyForObject(objID)
		if key == "" {
			continue
		}
		items, gerr := s.gcObject(ctx, objID, key, minAge, verify, now)
		rep.Items = append(rep.Items, items...)
		if gerr != nil {
			return rep, gerr
		}
	}
	return rep, nil
}

// gcObject reclaims one object directory. It holds the per-key cross-process
// publish lock, making version set and pointer checks race-free against
// concurrent publishers in other processes.
func (s *Store) gcObject(ctx context.Context, objID, key string, minAge time.Duration, verify bool, now time.Time) ([]GCItem, error) {
	lock, err := acquireFlock(filepath.Join(s.root, "locks", "publish-"+objID+".lock"))
	if err != nil {
		return nil, err
	}
	defer lock.release()
	// Snapshot files are created independently; take the refs lock too so a
	// concurrent Snapshot cannot pin a version after GC has decided to remove
	// it. Snapshot itself validates the version on disk and will fail if GC
	// wins the race.
	refLock, err := acquireFlock(filepath.Join(s.root, "locks", "refs-"+objID+".lock"))
	if err != nil {
		return nil, err
	}
	defer refLock.release()

	valid, bad, err := s.listValidVersions(key)
	if err != nil {
		return nil, err
	}
	current, err := s.readCurrent(key)
	if err != nil {
		return nil, err
	}
	durable, stale, err := s.durableRefs(key)
	if err != nil {
		return nil, err
	}
	// Discard snapshot files that point at versions which no longer exist; they
	// cannot protect anything.
	for _, name := range stale {
		if err := os.Remove(filepath.Join(s.refsDir(key), name)); err != nil && !os.IsNotExist(err) {
			return nil, err
		}
	}
	tomb, err := s.readTombstone(key)
	if err != nil {
		return nil, err
	}

	var items []GCItem
	vdir := filepath.Join(s.root, "objects", objID, "versions")

	// Isolate anything structurally invalid or content-corrupt. A quarantined
	// version is gone from the live tree; pointers are never rebuilt onto it.
	for _, vid := range valid {
		if !verify {
			continue
		}
		if err := ctx.Err(); err != nil {
			return items, err
		}
		m, err := s.loadManifest(key, vid)
		if err != nil {
			return items, err
		}
		if verr := verifyDirBytes(filepath.Join(vdir, vid), m); verr != nil {
			qd, qerr := s.quarantineGC(filepath.Join(vdir, vid))
			if qerr != nil {
				return items, qerr
			}
			items = append(items, GCItem{Key: key, VersionID: vid, Kind: "corrupt", Action: "quarantined", Reason: verr.Error(), QuarDir: qd})
			delete(durable, vid)
		}
	}
	for vid, verr := range bad {
		if err := ctx.Err(); err != nil {
			return items, err
		}
		qd, qerr := s.quarantineGC(filepath.Join(vdir, vid))
		if qerr != nil {
			return items, qerr
		}
		items = append(items, GCItem{Key: key, VersionID: vid, Kind: "corrupt", Action: "quarantined", Reason: verr.Error(), QuarDir: qd})
		delete(durable, vid)
	}

	// Recompute the surviving version set after isolation.
	survivors, err := os.ReadDir(vdir)
	if err != nil && !os.IsNotExist(err) {
		return items, err
	}
	var alive []string
	for _, e := range survivors {
		if e.IsDir() && len(e.Name()) > 0 && e.Name()[0] == 'v' {
			alive = append(alive, e.Name())
		}
	}

	for _, vid := range alive {
		if err := ctx.Err(); err != nil {
			return items, err
		}
		m, err := s.loadManifest(key, vid)
		if err != nil {
			qd, qerr := s.quarantineGC(filepath.Join(vdir, vid))
			if qerr != nil {
				return items, qerr
			}
			items = append(items, GCItem{Key: key, VersionID: vid, Kind: "corrupt", Action: "quarantined", Reason: err.Error(), QuarDir: qd})
			continue
		}
		referenced := durable[vid] || s.pinnedInMemory(key, vid)
		var eligible bool
		var kind, reason string
		switch {
		case tomb != nil:
			eligible = now.Sub(tomb.DeletedAt) >= minAge
			kind = "deleted"
			reason = "object deleted; retention elapsed"
		case vid != current:
			// MinAge <= 0 means no grace period; otherwise the version must
			// be older than the retention window.
			eligible = minAge <= 0 || now.Sub(m.CreatedAt) >= minAge
			kind = "expired"
			reason = "non-current version; retention elapsed"
		}
		if referenced {
			eligible = false
		}
		if !eligible {
			continue
		}
		qd, err := s.quarantineGC(filepath.Join(vdir, vid))
		if err != nil {
			return items, err
		}
		items = append(items, GCItem{Key: key, VersionID: vid, Kind: kind, Action: "reclaimed", Reason: reason, QuarDir: qd})
	}

	// Finalize a fully deleted object: no versions, no live snapshots, past
	// grace. Readers in this process that still pin a version keep it alive
	// until they close (their version would not have been reclaimed above).
	liveRefs, _, err := s.durableRefs(key)
	if err != nil {
		return items, err
	}
	remaining, _ := os.ReadDir(vdir)
	if tomb != nil && len(remaining) == 0 && len(liveRefs) == 0 && now.Sub(tomb.DeletedAt) >= minAge {
		if err := os.Remove(s.deletedPath(key)); err != nil && !os.IsNotExist(err) {
			return items, err
		}
		_ = fsyncDir(filepath.Join(s.root, "deleted"))
		if err := os.RemoveAll(s.objectDir(key)); err != nil {
			return items, err
		}
		_ = os.RemoveAll(s.refsDir(key))
	}
	return items, nil
}

// keyForObject resolves the caller key for an object-id directory. The mapping
// is one-way, but every surviving piece of metadata records the original key:
// the tombstone, a snapshot file, or a published manifest.
func (s *Store) keyForObject(objID string) string {
	b, err := os.ReadFile(filepath.Join(s.root, "deleted", objID+".json"))
	if err == nil {
		var t Tombstone
		if jsonUnmarshal(b, &t) == nil && t.Key != "" {
			return t.Key
		}
	}
	vdir := filepath.Join(s.root, "objects", objID, "versions")
	entries, _ := os.ReadDir(vdir)
	var highest string
	for _, e := range entries {
		if e.IsDir() && e.Name() > highest {
			highest = e.Name()
		}
	}
	if highest != "" {
		if m, err := readAnyManifest(filepath.Join(vdir, highest)); err == nil && m.Key != "" {
			return m.Key
		}
	}
	refsRoot := filepath.Join(s.root, "refs", objID)
	refs, _ := os.ReadDir(refsRoot)
	for _, e := range refs {
		if e.IsDir() {
			continue
		}
		b, err := os.ReadFile(filepath.Join(refsRoot, e.Name()))
		if err != nil {
			continue
		}
		var r Ref
		if jsonUnmarshal(b, &r) == nil && r.Key != "" {
			return r.Key
		}
	}
	return ""
}

// quarantineGC moves reclaimed or corrupt live versions into the GC safety
// area. Nothing is hard-deleted by GC; the moved directory is the recycling
// bin and can be audited before eventual physical removal.
func (s *Store) quarantineGC(src string) (string, error) {
	dst := filepath.Join(s.root, "quarantine", "gc",
		time.Now().UTC().Format("20060102T150405.000000000")+"-"+filepath.Base(filepath.Dir(src))+"-"+filepath.Base(src))
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return "", err
	}
	if err := os.Rename(src, dst); err != nil {
		return "", err
	}
	_ = fsyncDir(filepath.Dir(dst))
	_ = fsyncDir(filepath.Dir(src))
	return dst, nil
}
