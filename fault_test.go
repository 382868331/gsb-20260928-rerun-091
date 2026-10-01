package localobjstore

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// corruptVersion tampers with one data part of a published version on disk.
func corruptVersion(t *testing.T, dir, key, vid string, part int32) {
	t.Helper()
	p := filepath.Join(dir, "objects", objectID(key), "versions", vid, "data", partFileName(part))
	if err := os.Chmod(p, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte("tampered-bytes"), 0o644); err != nil {
		t.Fatal(err)
	}
}

func faultItems(rep RecoveryReport) int {
	n := 0
	for _, it := range rep.Items {
		if it.Kind == "fault" && it.Action == "fault-recorded" {
			n++
		}
	}
	return n
}

// A corrupted current version is quarantined and recorded as a durable
// fault; the object must NOT silently roll back to an older version, and
// repeated restarts keep the same failure fact.
func TestCorruptCurrentNoSilentRollback(t *testing.T) {
	s, dir := openTemp(t)
	m1, _ := s.PutObject("k", strings.NewReader("old-good"), nil)
	m2, _ := s.PutObject("k", strings.NewReader("new-bad"), nil)
	s.Close()

	corruptVersion(t, dir, "k", m2.VersionID, 1)

	s2, rep, err := Open(dir, &Options{VerifyOnOpen: true})
	if err != nil {
		t.Fatal(err)
	}
	if faultItems(rep) != 1 {
		t.Fatalf("expected one recorded fault, got %+v", rep.Items)
	}
	// Current must not masquerade as the old version.
	if _, err := s2.GetObject("k"); !errors.Is(err, ErrFaulted) {
		t.Fatalf("GetObject = %v, want ErrFaulted", err)
	}
	if _, err := s2.CurrentVersion("k"); !errors.Is(err, ErrFaulted) {
		t.Fatalf("CurrentVersion = %v, want ErrFaulted", err)
	}
	// The old version stays explicitly readable as itself.
	r, err := s2.GetVersion("k", m1.VersionID)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(r)
	r.Close()
	if string(b) != "old-good" {
		t.Fatalf("old version unreadable: %q", b)
	}
	// The fault record is inspectable and names the corrupt version.
	rec, ok, err := s2.Fault("k")
	if err != nil || !ok || rec.VersionID != m2.VersionID {
		t.Fatalf("fault record = %+v ok=%v err=%v", rec, ok, err)
	}
	s2.Close()

	// Reopening repeatedly keeps the same fault fact; nothing new happens.
	s3, rep3, err := Open(dir, &Options{VerifyOnOpen: true})
	if err != nil {
		t.Fatal(err)
	}
	defer s3.Close()
	if len(rep3.Items) != 0 {
		t.Fatalf("second reopen changed the failure fact: %+v", rep3.Items)
	}
	if _, err := s3.GetObject("k"); !errors.Is(err, ErrFaulted) {
		t.Fatalf("still faulted expected, got %v", err)
	}
	rec3, ok, _ := s3.Fault("k")
	if !ok || rec3.VersionID != rec.VersionID || !rec3.DetectedAt.Equal(rec.DetectedAt) {
		t.Fatalf("fault fact mutated across restarts: %+v vs %+v", rec3, rec)
	}
}

// Manual restore: candidates are shown with verification evidence, a
// confirmed restore creates a NEW version record, and the quarantined
// corrupt evidence is preserved untouched.
func TestRestoreCreatesNewRecordKeepsEvidence(t *testing.T) {
	s, dir := openTemp(t)
	m1, _ := s.PutObject("k", strings.NewReader("old-good"), nil)
	m2, _ := s.PutObject("k", strings.NewReader("new-bad"), nil)
	s.Close()
	corruptVersion(t, dir, "k", m2.VersionID, 1)

	s2, _, err := Open(dir, &Options{VerifyOnOpen: true})
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()

	st, err := s2.InspectVersions("k")
	if err != nil {
		t.Fatal(err)
	}
	if len(st) != 1 || !st[0].Verified || st[0].Manifest.VersionID != m1.VersionID {
		t.Fatalf("candidates = %+v", st)
	}

	nm, err := s2.RestoreVersion("k", m1.VersionID)
	if err != nil {
		t.Fatal(err)
	}
	if nm.RestoredFrom != m1.VersionID || nm.VersionID == m1.VersionID {
		t.Fatalf("restore must form a new record: %+v", nm)
	}
	r, err := s2.GetObject("k")
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(r)
	r.Close()
	if string(b) != "old-good" {
		t.Fatalf("restored content = %q", b)
	}
	if _, ok, _ := s2.Fault("k"); ok {
		t.Fatal("fault not resolved by restore")
	}
	// The corrupt evidence stays in quarantine, unmodified.
	q, _ := os.ReadDir(filepath.Join(dir, "quarantine", "version"))
	if len(q) != 1 {
		t.Fatalf("corrupt evidence lost: %v", q)
	}
	if _, err := os.Stat(filepath.Join(dir, "quarantine", "version", q[0].Name(), manifestName)); err != nil {
		t.Fatalf("quarantined manifest missing: %v", err)
	}
	// Restore survives restart as an ordinary current version.
	s2.Close()
	s3, rep3, err := Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s3.Close()
	if len(rep3.Items) != 0 {
		t.Fatalf("unexpected recovery after restore: %+v", rep3.Items)
	}
	cur, err := s3.CurrentVersion("k")
	if err != nil || cur.VersionID != nm.VersionID {
		t.Fatalf("restored version not current after restart: %v %+v", err, cur)
	}
}

// A failed restore (candidate fails verification) must not change the
// current state: the fault remains and no new version appears.
func TestFailedRestoreLeavesStateUntouched(t *testing.T) {
	s, dir := openTemp(t)
	m1, _ := s.PutObject("k", strings.NewReader("old-bad-too"), nil)
	m2, _ := s.PutObject("k", strings.NewReader("new-bad"), nil)
	s.Close()
	corruptVersion(t, dir, "k", m2.VersionID, 1)
	// Same-size tampering: passes the structural scan, fails checksums, so
	// it stays in the tree as a (bad) restore candidate.
	p := filepath.Join(dir, "objects", objectID("k"), "versions", m1.VersionID, "data", partFileName(1))
	if err := os.Chmod(p, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte("TAMPERED!!!"), 0o644); err != nil {
		t.Fatal(err)
	}

	s2, _, err := Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()

	st, _ := s2.InspectVersions("k")
	if len(st) != 1 || st[0].Verified || st[0].Err == nil {
		t.Fatalf("corrupt candidate must be reported with its error: %+v", st)
	}
	if _, err := s2.RestoreVersion("k", m1.VersionID); !errors.Is(err, ErrCorrupted) {
		t.Fatalf("restore of corrupt candidate = %v, want ErrCorrupted", err)
	}
	if _, ok, _ := s2.Fault("k"); !ok {
		t.Fatal("failed restore cleared the fault")
	}
	if _, err := s2.GetObject("k"); !errors.Is(err, ErrFaulted) {
		t.Fatalf("failed restore changed read state: %v", err)
	}
	vs, _ := s2.ListVersions("k")
	if len(vs) != 1 {
		t.Fatalf("failed restore created a version: %+v", vs)
	}
	// Restoring without a fault is rejected.
	if _, err := s2.RestoreVersion("other", m1.VersionID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("restore without fault = %v, want ErrNotFound", err)
	}
}

// GC must never reclaim the remaining versions of a faulted object: they
// are the operator's restore candidates. Delete tombstones and open readers
// do not weaken that retention.
func TestGCRetainsFaultRecoveryCandidates(t *testing.T) {
	s, dir := openTemp(t)
	m1, _ := s.PutObject("k", strings.NewReader("old-good"), nil)
	m2, _ := s.PutObject("k", strings.NewReader("new-bad"), nil)
	s.Close()
	corruptVersion(t, dir, "k", m2.VersionID, 1)

	s2, _, err := Open(dir, &Options{VerifyOnOpen: true})
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()

	rep, err := s2.CollectGarbage()
	if err != nil {
		t.Fatal(err)
	}
	retained := false
	for _, it := range rep.Items {
		if it.VersionID == m1.VersionID {
			if it.Action != "retained" {
				t.Fatalf("restore candidate reclaimed: %+v", it)
			}
			retained = true
		}
	}
	if !retained {
		t.Fatalf("faulted object's versions not reported retained: %+v", rep.Items)
	}
	if _, err := s2.GetVersion("k", m1.VersionID); err != nil {
		t.Fatalf("restore candidate lost to GC: %v", err)
	}
	// Still restorable after the GC pass.
	if _, err := s2.RestoreVersion("k", m1.VersionID); err != nil {
		t.Fatalf("restore after GC: %v", err)
	}
}

// Deleting a faulted object resolves the fault into a tombstone: no
// resurrection on restart, and its versions become ordinary GC candidates.
func TestDeleteFaultedObject(t *testing.T) {
	s, dir := openTemp(t)
	m1, _ := s.PutObject("k", strings.NewReader("old-good"), nil)
	m2, _ := s.PutObject("k", strings.NewReader("new-bad"), nil)
	s.Close()
	corruptVersion(t, dir, "k", m2.VersionID, 1)

	s2, _, err := Open(dir, &Options{VerifyOnOpen: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := s2.DeleteObject("k"); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := s2.Fault("k"); ok {
		t.Fatal("delete did not resolve the fault")
	}
	if _, err := s2.GetObject("k"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("deleted faulted object readable: %v", err)
	}
	s2.Close()

	s3, rep, err := Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s3.Close()
	for _, it := range rep.Items {
		if it.Action == "pointer-rebuilt" {
			t.Fatalf("deleted faulted object resurrected: %+v", it)
		}
	}
	if _, err := s3.GetObject("k"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("tombstone not durable: %v", err)
	}
	grep, err := s3.CollectGarbage()
	if err != nil {
		t.Fatal(err)
	}
	reclaimed := false
	for _, it := range grep.Items {
		if it.VersionID == m1.VersionID && it.Action == "reclaimed" {
			reclaimed = true
		}
	}
	if !reclaimed {
		t.Fatalf("unreferenced version not reclaimed after delete: %+v", grep.Items)
	}
}

// Interrupted upload, corrupted current version and interrupted GC residue
// are all continued correctly by a single recovery pass, and the store
// keeps accepting writes afterwards (a fresh publish lifts the fault).
func TestCombinedRecoveryContinuation(t *testing.T) {
	s, dir := openTemp(t)
	m1, _ := s.PutObject("k", strings.NewReader("old-good"), nil)
	m2, _ := s.PutObject("k", strings.NewReader("new-bad"), nil)
	// Interrupted upload residue.
	u, _ := s.BeginMultipart("half/object")
	_, _ = u.WritePart(strings.NewReader("segment"))
	// Interrupted GC residue: an already-trashed version pending delete.
	trash := filepath.Join(dir, "trash", "20260101T000000.000000000-deadbeef-vold")
	if err := os.MkdirAll(trash, 0o755); err != nil {
		t.Fatal(err)
	}
	s.Close()
	corruptVersion(t, dir, "k", m2.VersionID, 1)

	s2, rep, err := Open(dir, &Options{VerifyOnOpen: true})
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	var uploadQuar, trashDiscarded bool
	for _, it := range rep.Items {
		switch {
		case it.Kind == "upload" && it.Action == "quarantined":
			uploadQuar = true
		case it.Kind == "trash" && it.Action == "discarded":
			trashDiscarded = true
		}
	}
	if !uploadQuar || !trashDiscarded || faultItems(rep) != 1 {
		t.Fatalf("incomplete recovery: upload=%v trash=%v faults=%d items=%+v",
			uploadQuar, trashDiscarded, faultItems(rep), rep.Items)
	}
	if left, _ := os.ReadDir(filepath.Join(dir, "trash")); len(left) != 0 {
		t.Fatalf("trash residue left: %v", left)
	}
	if _, err := s2.GetObject("k"); !errors.Is(err, ErrFaulted) {
		t.Fatalf("corrupt current not faulted: %v", err)
	}

	// The store continues: a fresh publish lifts the fault.
	m3, err := s2.PutObject("k", strings.NewReader("reborn"), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := s2.Fault("k"); ok {
		t.Fatal("fresh publish did not lift the fault")
	}
	r, err := s2.GetObject("k")
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(r)
	r.Close()
	if string(b) != "reborn" || r.Manifest().VersionID != m3.VersionID {
		t.Fatalf("publish after fault broken: %q", b)
	}
	// Old history is still explicitly addressable.
	if _, err := s2.GetVersion("k", m1.VersionID); err != nil {
		t.Fatalf("history lost: %v", err)
	}
}
