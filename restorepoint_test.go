package localobjstore

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func putStr(t *testing.T, s *Store, key, data string) Manifest {
	t.Helper()
	m, err := s.PutObject(key, bytes.NewReader([]byte(data)), nil)
	if err != nil {
		t.Fatalf("put %s: %v", key, err)
	}
	return m
}

func currentID(t *testing.T, s *Store, key string) string {
	t.Helper()
	m, err := s.CurrentVersion(key)
	if err != nil {
		t.Fatalf("current %s: %v", key, err)
	}
	return m.VersionID
}

func readStr(t *testing.T, s *Store, key string) string {
	t.Helper()
	r, err := s.GetObject(key)
	if err != nil {
		t.Fatalf("get %s: %v", key, err)
	}
	defer r.Close()
	b, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("read %s: %v", key, err)
	}
	return string(b)
}

func TestRestorePointCreateShowAndImmutability(t *testing.T) {
	s, _ := openTemp(t)
	m1 := putStr(t, s, "doc/a", "a1")
	m2 := putStr(t, s, "doc/b", "b1")

	rp, err := s.CreateRestorePoint("commit-1", []RestoreMemberInput{
		{Key: "doc/a", References: []string{"doc/b"}},
		{Key: "doc/b"},
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if len(rp.Members) != 2 {
		t.Fatalf("members=%d", len(rp.Members))
	}
	if rp.Members[0].VersionID != m1.VersionID || rp.Members[1].VersionID != m2.VersionID {
		t.Fatalf("captured versions wrong: %+v", rp.Members)
	}
	if len(rp.Members[0].References) != 1 || rp.Members[0].References[0] != "doc/b" {
		t.Fatalf("references not recorded: %+v", rp.Members[0])
	}

	// Duplicate name is rejected; the confirmed point is immutable.
	if _, err := s.CreateRestorePoint("commit-1", []RestoreMemberInput{{Key: "doc/a"}}); !errors.Is(err, ErrRestorePointExists) {
		t.Fatalf("expected ErrRestorePointExists, got %v", err)
	}

	// Later publishes never join the confirmed point.
	putStr(t, s, "doc/a", "a2")
	got, err := s.GetRestorePoint("commit-1")
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range got.Members {
		if m.Key == "doc/a" && m.VersionID != m1.VersionID {
			t.Fatalf("in-flight publish silently joined the point: %s", m.VersionID)
		}
	}
	points, err := s.ListRestorePoints()
	if err != nil || len(points) != 1 {
		t.Fatalf("list: %v %d", err, len(points))
	}
}

func TestRestorePointCreateValidation(t *testing.T) {
	s, _ := openTemp(t)
	putStr(t, s, "a", "1")
	if _, err := s.CreateRestorePoint("", []RestoreMemberInput{{Key: "a"}}); err == nil {
		t.Fatal("empty name accepted")
	}
	if _, err := s.CreateRestorePoint("p", nil); err == nil {
		t.Fatal("empty members accepted")
	}
	if _, err := s.CreateRestorePoint("p", []RestoreMemberInput{{Key: "a"}, {Key: "a"}}); err == nil {
		t.Fatal("duplicate key accepted")
	}
	if _, err := s.CreateRestorePoint("p", []RestoreMemberInput{{Key: "a", References: []string{"b"}}}); err == nil {
		t.Fatal("reference outside group accepted")
	}
	if _, err := s.CreateRestorePoint("p", []RestoreMemberInput{{Key: "missing"}}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

// A key whose new content is still being uploaded must not be silently
// captured at its pre-upload version.
func TestRestorePointRejectsInFlightUpload(t *testing.T) {
	s, _ := openTemp(t)
	putStr(t, s, "k", "old")
	u, err := s.BeginMultipart("k")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := u.WritePart(bytes.NewReader([]byte("new"))); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateRestorePoint("p", []RestoreMemberInput{{Key: "k"}}); !errors.Is(err, ErrRestoreInFlight) {
		t.Fatalf("expected ErrRestoreInFlight, got %v", err)
	}
	if _, err := u.Complete(nil); err != nil {
		t.Fatal(err)
	}
	rp, err := s.CreateRestorePoint("p", []RestoreMemberInput{{Key: "k"}})
	if err != nil {
		t.Fatal(err)
	}
	if got := readStr(t, s, "k"); got != "new" {
		t.Fatalf("got %q", got)
	}
	if rp.Members[0].VersionID != currentID(t, s, "k") {
		t.Fatal("point did not capture the completed version")
	}
}

func TestRestorePointDiff(t *testing.T) {
	s, _ := openTemp(t)
	putStr(t, s, "keep", "1")
	putStr(t, s, "gone", "1")
	putStr(t, s, "chg", "1")
	if _, err := s.CreateRestorePoint("p1", []RestoreMemberInput{{Key: "keep"}, {Key: "gone"}, {Key: "chg"}}); err != nil {
		t.Fatal(err)
	}
	putStr(t, s, "chg", "2")
	putStr(t, s, "new", "1")
	if _, err := s.CreateRestorePoint("p2", []RestoreMemberInput{{Key: "keep"}, {Key: "chg"}, {Key: "new"}}); err != nil {
		t.Fatal(err)
	}
	changes, err := s.DiffRestorePoints("p1", "p2")
	if err != nil {
		t.Fatal(err)
	}
	if len(changes) != 3 {
		t.Fatalf("changes=%v", changes)
	}
	byKey := map[string]MemberChange{}
	for _, c := range changes {
		byKey[c.Key] = c
	}
	if byKey["chg"].Change != "changed" || byKey["new"].Change != "added" || byKey["gone"].Change != "removed" {
		t.Fatalf("wrong diff: %+v", changes)
	}
	if byKey["chg"].FromVersion == "" || byKey["chg"].ToVersion == "" || byKey["chg"].FromVersion == byKey["chg"].ToVersion {
		t.Fatalf("changed versions not recorded: %+v", byKey["chg"])
	}
}

func TestInspectRestorePointHealth(t *testing.T) {
	s, dir := openTemp(t)
	putStr(t, s, "ok", "fine")
	bad := putStr(t, s, "bad", "broken")
	lost := putStr(t, s, "lost", "gone")
	if _, err := s.CreateRestorePoint("p", []RestoreMemberInput{{Key: "ok"}, {Key: "bad"}, {Key: "lost"}}); err != nil {
		t.Fatal(err)
	}
	corruptVersion(t, dir, "bad", bad.VersionID, 1)
	vdir := filepath.Join(dir, "objects", objectID("lost"), "versions", lost.VersionID)
	if err := os.Chmod(filepath.Join(vdir, "data", "000001"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(vdir); err != nil {
		t.Fatal(err)
	}

	plan, err := s.InspectRestorePoint("p")
	if err != nil {
		t.Fatal(err)
	}
	if plan.Complete {
		t.Fatal("plan should be incomplete")
	}
	status := map[string]string{}
	for _, h := range plan.Members {
		status[h.Key] = h.Status
	}
	if status["ok"] != "readable" || status["bad"] != "corrupted" || status["lost"] != "missing" {
		t.Fatalf("statuses: %v", status)
	}
}

// A full restore changes nothing unless every member is available.
func TestFullRestoreRefusedWhenMemberUnavailable(t *testing.T) {
	s, dir := openTemp(t)
	putStr(t, s, "a", "a1")
	b := putStr(t, s, "b", "b1")
	if _, err := s.CreateRestorePoint("p", []RestoreMemberInput{{Key: "a"}, {Key: "b"}}); err != nil {
		t.Fatal(err)
	}
	putStr(t, s, "a", "a2")
	putStr(t, s, "b", "b2")
	corruptVersion(t, dir, "b", b.VersionID, 1)

	if _, err := s.RestorePoint("p", nil); !errors.Is(err, ErrRestoreMembersUnavailable) {
		t.Fatalf("expected ErrRestoreMembersUnavailable, got %v", err)
	}
	// Nothing became visible: both keys keep their post-point versions.
	if got := readStr(t, s, "a"); got != "a2" {
		t.Fatalf("a changed to %q", got)
	}
	if got := readStr(t, s, "b"); got != "b2" {
		t.Fatalf("b changed to %q", got)
	}
	recs, err := s.ListRestores()
	if err != nil || len(recs) != 0 {
		t.Fatalf("no restore record should exist: %v %d", err, len(recs))
	}
}

func TestFullRestoreSwitchesWholeGroup(t *testing.T) {
	s, _ := openTemp(t)
	a1 := putStr(t, s, "a", "a1")
	b1 := putStr(t, s, "b", "b1")
	if _, err := s.CreateRestorePoint("p", []RestoreMemberInput{{Key: "a", References: []string{"b"}}, {Key: "b"}}); err != nil {
		t.Fatal(err)
	}
	putStr(t, s, "a", "a2")
	putStr(t, s, "b", "b2")
	if err := s.DeleteObject("b"); err != nil {
		t.Fatal(err)
	}

	rec, err := s.RestorePoint("p", nil)
	if err != nil {
		t.Fatal(err)
	}
	if rec.State != "completed" || len(rec.Members) != 2 {
		t.Fatalf("rec=%+v", rec)
	}
	for _, m := range rec.Members {
		if m.State != "confirmed" || m.ConfirmedAt.IsZero() {
			t.Fatalf("member not confirmed: %+v", m)
		}
	}
	if got := readStr(t, s, "a"); got != "a1" {
		t.Fatalf("a=%q", got)
	}
	if got := readStr(t, s, "b"); got != "b1" {
		t.Fatalf("b=%q (tombstone should be lifted)", got)
	}
	if currentID(t, s, "a") != a1.VersionID || currentID(t, s, "b") != b1.VersionID {
		t.Fatal("currents do not point at the restore point members")
	}
	// The restore is recorded durably.
	st, err := s.RestoreStatus(rec.ID)
	if err != nil || st.State != "completed" {
		t.Fatalf("status: %v %+v", err, st)
	}
}

// A partial restore restores readable members and marks the gaps durably,
// leaving the gapped keys exactly as they were.
func TestPartialRestoreWithGaps(t *testing.T) {
	s, dir := openTemp(t)
	putStr(t, s, "a", "a1")
	b1 := putStr(t, s, "b", "b1")
	if _, err := s.CreateRestorePoint("p", []RestoreMemberInput{{Key: "a"}, {Key: "b"}}); err != nil {
		t.Fatal(err)
	}
	putStr(t, s, "a", "a2")
	putStr(t, s, "b", "b2")
	corruptVersion(t, dir, "b", b1.VersionID, 1)

	rec, err := s.RestorePoint("p", &RestoreOptions{AllowPartial: true})
	if err != nil {
		t.Fatal(err)
	}
	if rec.State != "completed-with-gaps" {
		t.Fatalf("state=%s", rec.State)
	}
	states := map[string]RestoreMemberResult{}
	for _, m := range rec.Members {
		states[m.Key] = m
	}
	if states["a"].State != "confirmed" {
		t.Fatalf("a: %+v", states["a"])
	}
	if states["b"].State != "gap" || states["b"].Reason == "" {
		t.Fatalf("b gap not marked: %+v", states["b"])
	}
	if got := readStr(t, s, "a"); got != "a1" {
		t.Fatalf("a=%q", got)
	}
	if got := readStr(t, s, "b"); got != "b2" {
		t.Fatalf("gapped b must stay untouched, got %q", got)
	}
	// The gap marker survives reopening.
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2, _, err := Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	st, err := s2.RestoreStatus(rec.ID)
	if err != nil {
		t.Fatal(err)
	}
	if st.State != "completed-with-gaps" {
		t.Fatalf("after reopen state=%s", st.State)
	}
	foundGap := false
	for _, m := range st.Members {
		if m.Key == "b" && m.State == "gap" && m.Reason != "" {
			foundGap = true
		}
	}
	if !foundGap {
		t.Fatal("gap marker lost after reopen")
	}
}
