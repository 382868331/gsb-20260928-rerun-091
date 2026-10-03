package localobjstore

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestDeleteRestorePointReportsRetention(t *testing.T) {
	s, _ := openTemp(t)
	putStr(t, s, "cur", "v1")
	putStr(t, s, "shared", "v1")
	putStr(t, s, "free", "v1")
	putStr(t, s, "pinned", "v1")
	// Move every key past the captured version so "current" only protects
	// the key we leave untouched.
	if _, err := s.CreateRestorePoint("main", []RestoreMemberInput{{Key: "cur"}, {Key: "shared"}, {Key: "free"}, {Key: "pinned"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateRestorePoint("other", []RestoreMemberInput{{Key: "shared"}}); err != nil {
		t.Fatal(err)
	}
	if err := s.PinVersion("keep", "pinned", currentID(t, s, "pinned")); err != nil {
		t.Fatal(err)
	}
	putStr(t, s, "shared", "v2")
	putStr(t, s, "free", "v2")
	putStr(t, s, "pinned", "v2")

	rep, err := s.DeleteRestorePoint("main")
	if err != nil {
		t.Fatal(err)
	}
	retained := map[string][]string{}
	for _, m := range rep.Retained {
		retained[m.Key] = m.Reasons
	}
	if len(rep.Released) != 1 || rep.Released[0].Key != "free" {
		t.Fatalf("released=%+v", rep.Released)
	}
	joined := func(key string) string { return strings.Join(retained[key], ",") }
	if !strings.Contains(joined("cur"), "current version") {
		t.Fatalf("cur reasons: %v", retained["cur"])
	}
	if !strings.Contains(joined("shared"), "restore point") {
		t.Fatalf("shared reasons: %v", retained["shared"])
	}
	if !strings.Contains(joined("pinned"), "pin") {
		t.Fatalf("pinned reasons: %v", retained["pinned"])
	}
	if _, err := s.GetRestorePoint("main"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("point still exists: %v", err)
	}
}

func TestDeleteRestorePointReportsOpenReader(t *testing.T) {
	s, _ := openTemp(t)
	putStr(t, s, "k", "v1")
	if _, err := s.CreateRestorePoint("p", []RestoreMemberInput{{Key: "k"}}); err != nil {
		t.Fatal(err)
	}
	r, err := s.GetObject("k")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	putStr(t, s, "k", "v2")

	rep, err := s.DeleteRestorePoint("p")
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.Retained) != 1 {
		t.Fatalf("retained=%+v", rep.Retained)
	}
	joined := strings.Join(rep.Retained[0].Reasons, ",")
	if !strings.Contains(joined, "open reader") {
		t.Fatalf("reasons: %v", rep.Retained[0].Reasons)
	}
}

// GC never reclaims versions referenced by a restore point; deleting the
// point releases them.
func TestGCRetainsRestorePointMembers(t *testing.T) {
	s, _ := openTemp(t)
	v1 := putStr(t, s, "k", "v1")
	if _, err := s.CreateRestorePoint("p", []RestoreMemberInput{{Key: "k"}}); err != nil {
		t.Fatal(err)
	}
	putStr(t, s, "k", "v2")

	rep, err := s.CollectGarbage()
	if err != nil {
		t.Fatal(err)
	}
	retained := false
	for _, it := range rep.Items {
		if it.VersionID == v1.VersionID && it.Action == "retained" && strings.Contains(it.Reason, "restore point") {
			retained = true
		}
	}
	if !retained {
		t.Fatalf("v1 not retained by restore point: %+v", rep.Items)
	}
	rd, err := s.GetVersion("k", v1.VersionID)
	if err != nil {
		t.Fatalf("v1 unreadable: %v", err)
	}
	if err := rd.Close(); err != nil {
		t.Fatal(err)
	}

	drep, err := s.DeleteRestorePoint("p")
	if err != nil {
		t.Fatal(err)
	}
	if len(drep.Released) != 1 {
		t.Fatalf("released=%+v", drep.Released)
	}
	rep, err = s.CollectGarbage()
	if err != nil {
		t.Fatal(err)
	}
	reclaimed := false
	for _, it := range rep.Items {
		if it.VersionID == v1.VersionID && it.Action == "reclaimed" {
			reclaimed = true
		}
	}
	if !reclaimed {
		t.Fatalf("v1 not reclaimed after point deletion: %+v", rep.Items)
	}
}

// An interrupted group restore is recognisable after reopening: confirmed
// members keep their state, pending ones stay pending, and ResumeRestore
// finishes exactly the recorded plan.
func TestInterruptedRestoreRecoveredAndResumed(t *testing.T) {
	s, dir := openTemp(t)
	a1 := putStr(t, s, "a", "a1")
	b1 := putStr(t, s, "b", "b1")
	rp, err := s.CreateRestorePoint("p", []RestoreMemberInput{{Key: "a"}, {Key: "b"}})
	if err != nil {
		t.Fatal(err)
	}
	putStr(t, s, "a", "a2")
	putStr(t, s, "b", "b2")

	// Simulate a crash mid-restore: journal written, first member
	// confirmed, second still pending, process gone.
	rec := RestoreRecord{
		ID:        newRestoreID(),
		PointName: rp.Name,
		PointID:   rp.ID,
		State:     "in-progress",
		StartedAt: time.Now().UTC(),
		Members: []RestoreMemberResult{
			{Key: "a", VersionID: a1.VersionID, State: "pending"},
			{Key: "b", VersionID: b1.VersionID, State: "pending"},
		},
	}
	if err := s.writeRestoreRecord(&rec); err != nil {
		t.Fatal(err)
	}
	if err := s.applyRestoreMember("a", a1.VersionID); err != nil {
		t.Fatal(err)
	}
	rec.Members[0].State = "confirmed"
	rec.Members[0].ConfirmedAt = time.Now().UTC()
	if err := s.writeRestoreRecord(&rec); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, rep, err := Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	marked := false
	for _, it := range rep.Items {
		if it.Kind == "restore" && it.Action == "marked-interrupted" {
			marked = true
		}
	}
	if !marked {
		t.Fatalf("recovery did not report the interrupted restore: %+v", rep.Items)
	}
	st, err := s2.RestoreStatus(rec.ID)
	if err != nil {
		t.Fatal(err)
	}
	if st.State != "interrupted" || !st.Interrupted {
		t.Fatalf("state=%s interrupted=%v", st.State, st.Interrupted)
	}
	if st.Members[0].State != "confirmed" || st.Members[1].State != "pending" {
		t.Fatalf("member states: %+v", st.Members)
	}
	// Confirmed result is visible; the pending member was never switched.
	if got := readStr(t, s2, "a"); got != "a1" {
		t.Fatalf("a=%q", got)
	}
	if got := readStr(t, s2, "b"); got != "b2" {
		t.Fatalf("b=%q", got)
	}

	done, err := s2.ResumeRestore(rec.ID)
	if err != nil {
		t.Fatal(err)
	}
	if done.State != "completed" {
		t.Fatalf("state=%s", done.State)
	}
	if got := readStr(t, s2, "b"); got != "b1" {
		t.Fatalf("b=%q after resume", got)
	}
	if currentID(t, s2, "b") != b1.VersionID {
		t.Fatal("b not switched to the recorded member version")
	}
}

// Resuming an interrupted restore whose member version has vanished records
// a durable gap instead of failing silently.
func TestResumeRestoreWithLostMember(t *testing.T) {
	s, dir := openTemp(t)
	putStr(t, s, "a", "a1")
	b1 := putStr(t, s, "b", "b1")
	rp, err := s.CreateRestorePoint("p", []RestoreMemberInput{{Key: "a"}, {Key: "b"}})
	if err != nil {
		t.Fatal(err)
	}
	rec := RestoreRecord{
		ID:        newRestoreID(),
		PointName: rp.Name,
		PointID:   rp.ID,
		State:     "in-progress",
		StartedAt: time.Now().UTC(),
		Members: []RestoreMemberResult{
			{Key: "a", VersionID: currentID(t, s, "a"), State: "confirmed", ConfirmedAt: time.Now().UTC()},
			{Key: "b", VersionID: b1.VersionID, State: "pending"},
		},
	}
	if err := s.writeRestoreRecord(&rec); err != nil {
		t.Fatal(err)
	}
	// Destroy the pending member's version while the store is "down".
	vdir := filepath.Join(dir, "objects", objectID("b"), "versions", b1.VersionID)
	if err := os.Chmod(filepath.Join(vdir, "data", "000001"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(vdir); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, _, err := Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	done, err := s2.ResumeRestore(rec.ID)
	if err != nil {
		t.Fatal(err)
	}
	if done.State != "completed-with-gaps" {
		t.Fatalf("state=%s", done.State)
	}
	if done.Members[1].State != "gap" || done.Members[1].Reason == "" {
		t.Fatalf("member b: %+v", done.Members[1])
	}
}

// An uncommitted restore-point creation (temp file left by a crash) is
// discarded by recovery and never listed.
func TestUncommittedRestorePointDiscarded(t *testing.T) {
	s, dir := openTemp(t)
	putStr(t, s, "k", "v1")
	tmp := filepath.Join(dir, "restorepoints", ".tmp-"+objectID("half")+"-xyz")
	if err := os.WriteFile(tmp, []byte("{}"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2, rep, err := Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	discarded := false
	for _, it := range rep.Items {
		if it.Kind == "restore-meta" && it.Action == "discarded" {
			discarded = true
		}
	}
	if !discarded {
		t.Fatalf("temp file not reported: %+v", rep.Items)
	}
	points, err := s2.ListRestorePoints()
	if err != nil || len(points) != 0 {
		t.Fatalf("points=%v err=%v", points, err)
	}
	if _, err := os.Stat(tmp); !os.IsNotExist(err) {
		t.Fatalf("temp file still there: %v", err)
	}
}

// Corruption evidence recorded before a group restore survives it: the
// quarantined version and the fault history are never rewritten.
func TestRestorePreservesCorruptionEvidence(t *testing.T) {
	s, dir := openTemp(t)
	putStr(t, s, "k", "good")
	if _, err := s.CreateRestorePoint("p", []RestoreMemberInput{{Key: "k"}}); err != nil {
		t.Fatal(err)
	}
	bad := putStr(t, s, "k", "bad")
	corruptVersion(t, dir, "k", bad.VersionID, 1)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2, rep, err := Open(dir, &Options{VerifyOnOpen: true})
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if faultItems(rep) != 1 {
		t.Fatalf("expected fault, got %+v", rep.Items)
	}
	if _, ok, err := s2.Fault("k"); err != nil || !ok {
		t.Fatalf("fault not recorded: %v %v", ok, err)
	}
	if _, err := s2.GetObject("k"); !errors.Is(err, ErrFaulted) {
		t.Fatalf("expected ErrFaulted, got %v", err)
	}

	rec, err := s2.RestorePoint("p", nil)
	if err != nil {
		t.Fatal(err)
	}
	if rec.State != "completed" {
		t.Fatalf("state=%s", rec.State)
	}
	if got := readStr(t, s2, "k"); got != "good" {
		t.Fatalf("k=%q", got)
	}
	// The fault is resolved by the known-good restore, but the quarantined
	// corrupt evidence is still on disk.
	if _, ok, _ := s2.Fault("k"); ok {
		t.Fatal("fault should be lifted by the restore")
	}
	entries, err := os.ReadDir(filepath.Join(dir, "quarantine", "version"))
	if err != nil || len(entries) != 1 {
		t.Fatalf("quarantine evidence lost: %v %d", err, len(entries))
	}
}
