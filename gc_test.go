package localobjstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// After deleting the current object, GC must not remove content still pinned by
// a durable snapshot; releasing the snapshot moves it into the reclaimable set.
func TestDeleteAndSnapshotRetention(t *testing.T) {
	s, dir := openTemp(t)
	m1, _ := s.PutObject("k", strings.NewReader("one"), nil)
	m2, _ := s.PutObject("k", strings.NewReader("two"), nil)

	snap, err := s.Snapshot("k", m1.VersionID)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteObject("k"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetObject("k"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("deleted key must have no current version, got %v", err)
	}
	// Deleting twice is an error.
	if err := s.DeleteObject("k"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("double delete = %v, want ErrNotFound", err)
	}

	// Immediate GC with zero retention: snapshot-pinned version survives.
	rep, err := s.GC(context.Background(), &GCOptions{MinAge: 0})
	if err != nil {
		t.Fatal(err)
	}
	r, err := s.GetVersion("k", m1.VersionID)
	if err != nil {
		t.Fatalf("snapshotted version must stay readable after delete: %v", err)
	}
	b, _ := io.ReadAll(r)
	r.Close()
	if string(b) != "one" {
		t.Fatalf("snapshot content changed: %q", b)
	}
	// m2 was never referenced and is reclaimed.
	reclaimedM2 := false
	for _, it := range rep.Items {
		if it.VersionID == m2.VersionID && it.Action == "reclaimed" {
			reclaimedM2 = true
		}
	}
	if !reclaimedM2 {
		t.Fatalf("unreferenced deleted version not reclaimed: %+v", rep.Items)
	}
	if _, err := s.GetVersion("k", m2.VersionID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("reclaimed version must be gone, got %v", err)
	}

	// Release the snapshot; a second GC reclaims the last version and the
	// whole object directory plus tombstone.
	if err := s.ReleaseSnapshot("k", snap.ID); err != nil {
		t.Fatal(err)
	}
	rep2, err := s.GC(context.Background(), &GCOptions{MinAge: 0})
	if err != nil {
		t.Fatal(err)
	}
	reclaimedM1 := false
	for _, it := range rep2.Items {
		if it.VersionID == m1.VersionID && it.Action == "reclaimed" {
			reclaimedM1 = true
		}
	}
	if !reclaimedM1 {
		t.Fatalf("released snapshot version not reclaimed: %+v", rep2.Items)
	}
	if _, err := os.Stat(filepath.Join(dir, "objects", objectID("k"))); !os.IsNotExist(err) {
		t.Fatalf("object directory should be finalized away, err=%v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "deleted", objectID("k")+".json")); !os.IsNotExist(err) {
		t.Fatal("tombstone should be removed after finalization")
	}
	// Content is not hard-erased: it lives in the recycling bin (quarantine).
	gc, _ := os.ReadDir(filepath.Join(dir, "quarantine", "gc"))
	if len(gc) < 2 {
		t.Fatalf("expected reclaimed versions in quarantine/gc, got %v", gc)
	}
}

// Non-current versions of a live object are protected by the retention window
// and by snapshots; the current version is never reclaimed.
func TestGCLiveObjectRetentionAndCurrentProtected(t *testing.T) {
	s, _ := openTemp(t)
	m1, _ := s.PutObject("k", strings.NewReader("one"), nil)
	m2, _ := s.PutObject("k", strings.NewReader("two"), nil)
	m3, _ := s.PutObject("k", strings.NewReader("three"), nil)

	// Default retention: nothing eligible immediately.
	rep, err := s.GC(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.Items) != 0 {
		t.Fatalf("default 24h retention must keep everything, got %+v", rep.Items)
	}
	for _, vid := range []string{m1.VersionID, m2.VersionID, m3.VersionID} {
		rr, err := s.GetVersion("k", vid)
		if err != nil {
			t.Fatalf("version %s lost under retention: %v", vid, err)
		}
		rr.Close()
	}

	// Zero retention: old versions reclaimed, current stays and is readable.
	rep, err = s.GC(context.Background(), &GCOptions{MinAge: 0})
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for _, it := range rep.Items {
		got[it.VersionID] = true
	}
	if !got[m1.VersionID] || !got[m2.VersionID] || got[m3.VersionID] {
		t.Fatalf("reclaim set wrong: %+v", rep.Items)
	}
	r, err := s.GetObject("k")
	if err != nil {
		t.Fatalf("current version must survive GC: %v", err)
	}
	b, _ := io.ReadAll(r)
	r.Close()
	if string(b) != "three" {
		t.Fatalf("current content changed: %q", b)
	}
}

// An open reader transiently pins its version: a concurrent GC cannot reclaim
// bytes the reader is still serving.
func TestGCRespectsOpenReader(t *testing.T) {
	s, _ := openTemp(t)
	m1, _ := s.PutObject("k", strings.NewReader("old"), nil)
	m2, _ := s.PutObject("k", strings.NewReader("new"), nil)
	r, err := s.GetVersion("k", m1.VersionID)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	rep, err := s.GC(context.Background(), &GCOptions{MinAge: 0})
	if err != nil {
		t.Fatal(err)
	}
	for _, it := range rep.Items {
		if it.VersionID == m1.VersionID {
			t.Fatalf("open reader's version reclaimed: %+v", rep.Items)
		}
	}
	// The other old version (none referenced) logic: m2 is current, so only
	// m1 existed besides it; nothing should be reclaimed at all here.
	if len(rep.Items) != 0 {
		t.Fatalf("expected no reclamations with reader open, got %+v", rep.Items)
	}
	buf := make([]byte, 3)
	if _, err := r.ReadAt(buf, 0); err != nil {
		t.Fatalf("pinned read failed: %v", err)
	}
	if string(buf) != "old" {
		t.Fatalf("pinned bytes wrong: %q", buf)
	}
	r.Close()
	rep, err = s.GC(context.Background(), &GCOptions{MinAge: 0})
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, it := range rep.Items {
		if it.VersionID == m1.VersionID {
			found = true
		}
	}
	if !found {
		t.Fatalf("version should be reclaimable after reader closed: %+v", rep.Items)
	}
	_ = m2
}

// GC never scans staging: a multipart upload in progress keeps its parts.
func TestGCDoesNotTouchUnpublishedParts(t *testing.T) {
	s, _ := openTemp(t)
	u, err := s.BeginMultipart("inflight")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = u.WritePart(strings.NewReader("part-one"))
	if _, err := s.GC(context.Background(), &GCOptions{MinAge: 0}); err != nil {
		t.Fatal(err)
	}
	if _, err := u.WritePart(strings.NewReader("part-two")); err != nil {
		t.Fatalf("active upload parts must survive GC: %v", err)
	}
	m, err := u.Complete(nil)
	if err != nil {
		t.Fatalf("completing the upload after GC: %v", err)
	}
	r, err := s.GetVersion("inflight", m.VersionID)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if err := r.Verify(); err != nil {
		t.Fatalf("published content wrong after GC ran mid-upload: %v", err)
	}
}

// A GC pass is interruptible; after restart the next pass re-validates from
// disk and completes safely.
func TestGCInterruptedThenReverified(t *testing.T) {
	s, dir := openTemp(t)
	var ms []struct{ vid string }
	for i := 0; i < 4; i++ {
		m, _ := s.PutObject("k", strings.NewReader(string(rune('a'+i))), nil)
		ms = append(ms, struct{ vid string }{m.VersionID})
	}
	// Cancel before the pass starts processing objects: it must return the
	// context error with a (possibly empty) report and leave everything live.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := s.GC(ctx, &GCOptions{MinAge: 0})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("interrupted GC must return ctx error, got %v", err)
	}
	s.Close()

	// Reopen (re-validates versions and pointers from disk), then run a full
	// pass: nothing was half-removed by the interrupted run.
	s2, rep, err := Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if len(rep.Items) != 0 {
		t.Fatalf("interrupted GC left residue for recovery: %+v", rep.Items)
	}
	gc, err := s2.GC(context.Background(), &GCOptions{MinAge: 0})
	if err != nil {
		t.Fatal(err)
	}
	// Three old versions reclaimed; the newest current version remains.
	if len(gc.Items) != 3 {
		t.Fatalf("expected 3 reclaimed old versions, got %+v", gc.Items)
	}
	cur, err := s2.CurrentVersion("k")
	if err != nil || cur.VersionID != ms[3].vid {
		t.Fatalf("current wrong after interrupted+resumed GC: %v %s", err, cur.VersionID)
	}
}

// Content-corrupted candidate versions are isolated on GC verification, never
// reclaimed as ordinary garbage, and the current pointer is not rebuilt onto
// older content.
func TestGCCorruptVersionIsolatedNotResurrected(t *testing.T) {
	s, dir := openTemp(t)
	m1, _ := s.PutObject("k", strings.NewReader("one"), nil)
	m2, _ := s.PutObject("k", strings.NewReader("two"), nil)
	s.Close()

	// Tamper with the current version's bytes (same length).
	p := filepath.Join(dir, "objects", objectID("k"), "versions", m2.VersionID, "data", "000001")
	_ = os.Chmod(p, 0o644)
	if err := os.WriteFile(p, []byte("TWo"), 0o644); err != nil {
		t.Fatal(err)
	}

	s2, rep, err := Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	// Structural scan passes; GC with VerifyContent isolates the corruption.
	gc, err := s2.GC(context.Background(), &GCOptions{MinAge: 0, VerifyContent: true})
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, it := range gc.Items {
		if it.VersionID == m2.VersionID && it.Kind == "corrupt" && it.Action == "quarantined" {
			found = true
		}
	}
	if !found {
		t.Fatalf("corrupt current version not isolated by GC: %+v", gc.Items)
	}
	// The object must not silently become readable again via the old version.
	if _, err := s2.GetObject("k"); err == nil {
		t.Fatal("corrupt object must not be resurrected by pointer rebuild")
	}
	_ = rep
	_ = m1
}

// Startup with VerifyOnOpen on a corrupt current version removes the pointer
// instead of rebuilding it onto an older version.
func TestRecoverCorruptCurrentPointerRemoved(t *testing.T) {
	s, dir := openTemp(t)
	m1, _ := s.PutObject("k", strings.NewReader("one"), nil)
	m2, _ := s.PutObject("k", strings.NewReader("two"), nil)
	s.Close()

	p := filepath.Join(dir, "objects", objectID("k"), "versions", m2.VersionID, "data", "000001")
	_ = os.Chmod(p, 0o644)
	_ = os.WriteFile(p, []byte("TWo"), 0o644)

	s2, rep, err := Open(dir, &Options{VerifyOnOpen: true})
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	var removed, rebuilt bool
	for _, it := range rep.Items {
		if it.Kind == "pointer" && it.Action == "pointer-removed" {
			removed = true
		}
		if it.Action == "pointer-rebuilt" {
			rebuilt = true
		}
	}
	if !removed || rebuilt {
		t.Fatalf("expected pointer-removed and no rebuild, got %+v", rep.Items)
	}
	if _, err := s2.GetObject("k"); err == nil {
		t.Fatal("object must stay unavailable after its only-good-current was corrupt")
	}
	// Old version remains explicitly addressable (not silently promoted).
	r, err := s2.GetVersion("k", m1.VersionID)
	if err != nil {
		t.Fatal(err)
	}
	r.Close()
}

// Concurrent publishes, GCs and readers must never lose the current version,
// corrupt a reader's bytes, or reclaim an active version directory.
func TestConcurrentPublishGCReaders(t *testing.T) {
	s, _ := openTemp(t)
	const key = "stress/obj"
	if _, err := s.PutObject(key, strings.NewReader("seed"), nil); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	stop := make(chan struct{})

	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 25; i++ {
				select {
				case <-stop:
					return
				default:
				}
				payload := fmt.Sprintf("writer-%d-%d", w, i)
				if _, err := s.PutObject(key, strings.NewReader(payload), nil); err != nil {
					t.Errorf("put: %v", err)
					return
				}
			}
		}(w)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 30; i++ {
			rep, err := s.GC(context.Background(), &GCOptions{MinAge: 0})
			if err != nil {
				t.Errorf("gc: %v", err)
				close(stop)
				return
			}
			for _, it := range rep.Items {
				if it.VersionID == "" {
					t.Errorf("gc reported bad item %+v", it)
				}
			}
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			select {
			case <-stop:
				return
			default:
			}
			r, err := s.GetObject(key)
			if err != nil {
				t.Errorf("get: %v", err)
				return
			}
			buf := make([]byte, 64)
			n, err := r.ReadAt(buf, 0)
			if err != nil && n == 0 {
				t.Errorf("read: %v", err)
				r.Close()
				return
			}
			r.Close()
		}
	}()
	wg.Wait()

	cur, err := s.CurrentVersion(key)
	if err != nil {
		t.Fatal(err)
	}
	r, err := s.GetVersion(key, cur.VersionID)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if err := r.Verify(); err != nil {
		t.Fatalf("current version corrupted by concurrent GC: %v", err)
	}
}

// Reopening a deleted object (tombstone + no pointer) must be stable: no
// pointer rebuild/remove churn on every restart.
func TestReopenDeletedObjectIsStable(t *testing.T) {
	s, dir := openTemp(t)
	m, _ := s.PutObject("k", strings.NewReader("data"), nil)
	snap, err := s.Snapshot("k", m.VersionID)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteObject("k"); err != nil {
		t.Fatal(err)
	}
	s.Close()

	for i := 0; i < 2; i++ {
		s2, rep, err := Open(dir, nil)
		if err != nil {
			t.Fatal(err)
		}
		for _, it := range rep.Items {
			if it.Kind == "pointer" {
				t.Fatalf("deleted object pointer churned on reopen #%d: %+v", i, it)
			}
		}
		// Snapshot still readable after delete + restart.
		r, err := s2.GetVersion("k", snap.VersionID)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(r)
		r.Close()
		if string(b) != "data" {
			t.Fatalf("snapshot content wrong: %q", b)
		}
		if _, err := s2.GetObject("k"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("deleted object has no current version, got %v", err)
		}
		s2.Close()
	}
}
