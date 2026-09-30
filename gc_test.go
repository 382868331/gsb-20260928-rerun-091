package localobjstore

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func gcActions(rep GCReport, action string) int {
	n := 0
	for _, it := range rep.Items {
		if it.Action == action {
			n++
		}
	}
	return n
}

// Delete hides the key but pinned versions survive GC until unpinned.
func TestDeleteRetainsPinnedVersionUntilUnpin(t *testing.T) {
	s, _ := openTemp(t)
	m1, _ := s.PutObject("doc", strings.NewReader("v1"), nil)
	m2, _ := s.PutObject("doc", strings.NewReader("v2"), nil)

	if err := s.PinVersion("snap1", "doc", m1.VersionID); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteObject("doc"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetObject("doc"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetObject after delete = %v, want ErrNotFound", err)
	}
	if err := s.DeleteObject("doc"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("second delete = %v, want ErrNotFound", err)
	}

	rep, err := s.CollectGarbage()
	if err != nil {
		t.Fatal(err)
	}
	// m2 (unreferenced) reclaimed, m1 (pinned) retained.
	if gcActions(rep, "reclaimed") != 1 || gcActions(rep, "retained") != 1 {
		t.Fatalf("unexpected GC report: %+v", rep.Items)
	}
	if _, err := s.GetVersion("doc", m2.VersionID); err == nil {
		t.Fatal("reclaimed version still readable")
	}
	r, err := s.GetVersion("doc", m1.VersionID)
	if err != nil {
		t.Fatalf("pinned version lost: %v", err)
	}
	b, _ := io.ReadAll(r)
	r.Close()
	if string(b) != "v1" {
		t.Fatalf("pinned content wrong: %q", b)
	}

	// Releasing the pin makes the version reclaimable.
	if err := s.UnpinVersion("snap1"); err != nil {
		t.Fatal(err)
	}
	rep, err = s.CollectGarbage()
	if err != nil {
		t.Fatal(err)
	}
	if gcActions(rep, "reclaimed") != 1 {
		t.Fatalf("expected final reclaim: %+v", rep.Items)
	}
	if _, err := s.GetVersion("doc", m1.VersionID); err == nil {
		t.Fatal("unpinned version survived GC")
	}
	if err := s.UnpinVersion("snap1"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("double unpin = %v, want ErrNotFound", err)
	}
}

// GC keeps the current version and reclaims superseded ones.
func TestGCKeepsCurrentReclaimsOld(t *testing.T) {
	s, _ := openTemp(t)
	m1, _ := s.PutObject("k", strings.NewReader("old"), nil)
	m2, _ := s.PutObject("k", strings.NewReader("new"), nil)

	rep, err := s.CollectGarbage()
	if err != nil {
		t.Fatal(err)
	}
	if gcActions(rep, "reclaimed") != 1 {
		t.Fatalf("expected one reclaim: %+v", rep.Items)
	}
	if _, err := s.GetVersion("k", m1.VersionID); err == nil {
		t.Fatal("old version survived GC")
	}
	r, err := s.GetObject("k")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if r.Manifest().VersionID != m2.VersionID {
		t.Fatalf("current changed: %s", r.Manifest().VersionID)
	}
	if err := r.Verify(); err != nil {
		t.Fatal(err)
	}
}

// An open reader pins its version for the duration of the read.
func TestGCSkipsOpenReader(t *testing.T) {
	s, _ := openTemp(t)
	m1, _ := s.PutObject("k", strings.NewReader("old"), nil)
	if _, err := s.PutObject("k", strings.NewReader("new"), nil); err != nil {
		t.Fatal(err)
	}
	r, err := s.GetVersion("k", m1.VersionID)
	if err != nil {
		t.Fatal(err)
	}
	rep, err := s.CollectGarbage()
	if err != nil {
		t.Fatal(err)
	}
	if gcActions(rep, "reclaimed") != 0 || gcActions(rep, "retained") != 1 {
		t.Fatalf("reader-held version reclaimed: %+v", rep.Items)
	}
	b, err := io.ReadAll(r)
	if err != nil || string(b) != "old" {
		t.Fatalf("read through GC failed: %v %q", err, b)
	}
	r.Close()

	rep, err = s.CollectGarbage()
	if err != nil {
		t.Fatal(err)
	}
	if gcActions(rep, "reclaimed") != 1 {
		t.Fatalf("closed reader still retains: %+v", rep.Items)
	}
}

// GC racing an in-flight multipart upload must not touch staged parts; the
// upload completes and publishes a fully readable version afterwards.
func TestGCDoesNotTouchInFlightUpload(t *testing.T) {
	s, _ := openTemp(t)
	if _, err := s.PutObject("k", strings.NewReader("gen1"), nil); err != nil {
		t.Fatal(err)
	}
	u, err := s.BeginMultipart("k")
	if err != nil {
		t.Fatal(err)
	}
	chunk := bytes.Repeat([]byte("x"), 100000)
	if _, err := u.WritePart(bytes.NewReader(chunk)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CollectGarbage(); err != nil {
		t.Fatal(err)
	}
	if _, err := u.WritePart(bytes.NewReader(chunk)); err != nil {
		t.Fatalf("part lost to GC: %v", err)
	}
	m, err := u.Complete(nil)
	if err != nil {
		t.Fatalf("complete after GC: %v", err)
	}
	r, err := s.GetVersion("k", m.VersionID)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if err := r.Verify(); err != nil {
		t.Fatal(err)
	}
	if r.Manifest().Size != int64(2*len(chunk)) {
		t.Fatalf("size = %d", r.Manifest().Size)
	}
}

// An aborted upload leaves nothing for GC to trip over, and GC never
// resurrects it.
func TestAbortUploadThenGC(t *testing.T) {
	s, dir := openTemp(t)
	u, _ := s.BeginMultipart("k")
	if _, err := u.WritePart(strings.NewReader("partial")); err != nil {
		t.Fatal(err)
	}
	if err := u.Abort(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CollectGarbage(); err != nil {
		t.Fatal(err)
	}
	entries, _ := os.ReadDir(filepath.Join(dir, "staging", "uploads"))
	if len(entries) != 0 {
		t.Fatalf("aborted upload residue: %v", entries)
	}
	if _, err := s.GetObject("k"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("aborted upload visible: %v", err)
	}
}

// A crash mid-GC leaves trash behind; recovery discards it and reports it,
// and the reclaimed version is not resurrected.
func TestRecoveryDiscardsInterruptedGC(t *testing.T) {
	s, dir := openTemp(t)
	m1, _ := s.PutObject("k", strings.NewReader("old"), nil)
	if _, err := s.PutObject("k", strings.NewReader("new"), nil); err != nil {
		t.Fatal(err)
	}
	s.Close()

	// Simulate a GC pass killed right after the rename into trash/.
	src := filepath.Join(dir, "objects", objectID("k"), "versions", m1.VersionID)
	if err := os.Rename(src, filepath.Join(dir, "trash", "fake-"+m1.VersionID)); err != nil {
		t.Fatal(err)
	}
	s2, rep, err := Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	found := false
	for _, it := range rep.Items {
		if it.Kind == "trash" && it.Action == "discarded" {
			found = true
		}
	}
	if !found {
		t.Fatalf("interrupted GC not reported: %+v", rep.Items)
	}
	entries, _ := os.ReadDir(filepath.Join(dir, "trash"))
	if len(entries) != 0 {
		t.Fatalf("trash not discarded: %v", entries)
	}
	if _, err := s2.GetVersion("k", m1.VersionID); err == nil {
		t.Fatal("reclaimed version resurrected")
	}
	r, err := s2.GetObject("k")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if err := r.Verify(); err != nil {
		t.Fatal(err)
	}
}

// A deleted object stays deleted across restart: recovery must not rebuild
// its current pointer over the tombstone.
func TestDeleteSurvivesRestart(t *testing.T) {
	s, dir := openTemp(t)
	if _, err := s.PutObject("k", strings.NewReader("data"), nil); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteObject("k"); err != nil {
		t.Fatal(err)
	}
	s.Close()

	s2, rep, err := Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	for _, it := range rep.Items {
		if it.Action == "pointer-rebuilt" {
			t.Fatalf("deleted object resurrected: %+v", it)
		}
	}
	if _, err := s2.GetObject("k"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("deleted object readable after restart: %v", err)
	}

	// A fresh publish lifts the tombstone.
	m, err := s2.PutObject("k", strings.NewReader("reborn"), nil)
	if err != nil {
		t.Fatal(err)
	}
	r, err := s2.GetObject("k")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if r.Manifest().VersionID != m.VersionID {
		t.Fatal("republish did not lift tombstone")
	}
}

// Publishers and GC running concurrently never lose the winning version.
func TestConcurrentPublishAndGC(t *testing.T) {
	s, _ := openTemp(t)
	var wg sync.WaitGroup
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 5; i++ {
				if _, err := s.PutObject("hot", strings.NewReader("gen"), nil); err != nil {
					t.Errorf("put: %v", err)
					return
				}
			}
		}(g)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 10; i++ {
			if _, err := s.CollectGarbage(); err != nil {
				t.Errorf("gc: %v", err)
				return
			}
		}
	}()
	wg.Wait()

	r, err := s.GetObject("hot")
	if err != nil {
		t.Fatalf("winning version lost: %v", err)
	}
	defer r.Close()
	if err := r.Verify(); err != nil {
		t.Fatal(err)
	}
}
