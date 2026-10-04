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
	diff, err := s.DiffRestorePoints("p1", "p2")
	if err != nil {
		t.Fatal(err)
	}
	if len(diff.MemberChanges) != 3 {
		t.Fatalf("changes=%v", diff.MemberChanges)
	}
	byKey := map[string]MemberChange{}
	for _, c := range diff.MemberChanges {
		byKey[c.Key] = c
	}
	if byKey["chg"].Change != "changed" || byKey["new"].Change != "added" || byKey["gone"].Change != "removed" {
		t.Fatalf("wrong diff: %+v", diff.MemberChanges)
	}
	if byKey["chg"].FromVersion == "" || byKey["chg"].ToVersion == "" || byKey["chg"].FromVersion == byKey["chg"].ToVersion {
		t.Fatalf("changed versions not recorded: %+v", byKey["chg"])
	}
	if len(diff.ReferenceChanges) != 0 {
		t.Fatalf("points without references must not report reference changes: %+v", diff.ReferenceChanges)
	}
	if diff.PointA.Name != "p1" || diff.PointB.Name != "p2" {
		t.Fatalf("diff must carry both points verbatim: %q %q", diff.PointA.Name, diff.PointB.Name)
	}
}

// Same member versions but a changed reference topology must be reported:
// added edges, removed edges and reversed edges are distinguished, and a
// mere reordering of reference declarations changes nothing.
func TestRestorePointDiffReferences(t *testing.T) {
	s, _ := openTemp(t)
	putStr(t, s, "a", "1")
	putStr(t, s, "b", "1")
	putStr(t, s, "c", "1")
	putStr(t, s, "d", "1")
	// p1: a -> b (reversed in p2), a -> c (dropped in p2), declared in
	// this order.
	if _, err := s.CreateRestorePoint("p1", []RestoreMemberInput{
		{Key: "a", References: []string{"b", "c"}},
		{Key: "b"},
		{Key: "c"},
		{Key: "d"},
	}); err != nil {
		t.Fatal(err)
	}
	// Same versions; p2 reverses a <-> b, drops a -> c and adds c -> d.
	if _, err := s.CreateRestorePoint("p2", []RestoreMemberInput{
		{Key: "a"},
		{Key: "b", References: []string{"a"}},
		{Key: "c", References: []string{"d"}},
		{Key: "d"},
	}); err != nil {
		t.Fatal(err)
	}

	diff, err := s.DiffRestorePoints("p1", "p2")
	if err != nil {
		t.Fatal(err)
	}
	if len(diff.MemberChanges) != 0 {
		t.Fatalf("same member versions must not report member changes: %+v", diff.MemberChanges)
	}
	got := map[string]ReferenceChange{}
	for _, rc := range diff.ReferenceChanges {
		got[rc.From+"->"+rc.To] = rc
	}
	if len(got) != 3 {
		t.Fatalf("want 3 reference changes, got %+v", diff.ReferenceChanges)
	}
	if got["a->b"].Change != "reversed" {
		t.Fatalf("a->b should be reversed: %+v", got["a->b"])
	}
	if got["a->c"].Change != "removed" {
		t.Fatalf("a->c should be removed: %+v", got["a->c"])
	}
	if got["c->d"].Change != "added" {
		t.Fatalf("c->d should be added: %+v", got["c->d"])
	}

	// A point declaring the same topology in a different reference order
	// must compare equal to p1.
	if _, err := s.CreateRestorePoint("p3", []RestoreMemberInput{
		{Key: "a", References: []string{"c", "b"}},
		{Key: "c"},
		{Key: "b"},
		{Key: "d"},
	}); err != nil {
		t.Fatal(err)
	}
	eq, err := s.DiffRestorePoints("p1", "p3")
	if err != nil {
		t.Fatal(err)
	}
	if len(eq.MemberChanges) != 0 || len(eq.ReferenceChanges) != 0 {
		t.Fatalf("declaration order must not create differences: members=%+v refs=%+v", eq.MemberChanges, eq.ReferenceChanges)
	}
}

// Diffing is purely read-only: neither restore point file changes, and the
// comparison stays identical after a confirmed restore. Missing or
// corrupted members still keep the references the points recorded.
func TestRestorePointDiffStableAndReadOnly(t *testing.T) {
	s, dir := openTemp(t)
	putStr(t, s, "a", "a1")
	lost := putStr(t, s, "lost", "l1")
	if _, err := s.CreateRestorePoint("p1", []RestoreMemberInput{
		{Key: "a", References: []string{"lost"}},
		{Key: "lost"},
	}); err != nil {
		t.Fatal(err)
	}
	putStr(t, s, "a", "a2")
	if _, err := s.CreateRestorePoint("p2", []RestoreMemberInput{{Key: "a"}, {Key: "lost"}}); err != nil {
		t.Fatal(err)
	}

	before, err := os.ReadFile(filepath.Join(dir, "restorepoints", objectID("p1")+".json"))
	if err != nil {
		t.Fatal(err)
	}
	diff1, err := s.DiffRestorePoints("p1", "p2")
	if err != nil {
		t.Fatal(err)
	}
	after, err := os.ReadFile(filepath.Join(dir, "restorepoints", objectID("p1")+".json"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("diff modified restore point p1 on disk")
	}
	if len(diff1.ReferenceChanges) != 1 || diff1.ReferenceChanges[0].Change != "removed" ||
		diff1.ReferenceChanges[0].From != "a" || diff1.ReferenceChanges[0].To != "lost" {
		t.Fatalf("unexpected ref diff: %+v", diff1.ReferenceChanges)
	}

	// Confirm a restore of p1, then delete one member version entirely.
	if _, err := s.RestorePoint("p1", nil); err != nil {
		t.Fatal(err)
	}
	lostDir := filepath.Join(dir, "objects", objectID("lost"), "versions", lost.VersionID)
	if err := os.RemoveAll(lostDir); err != nil {
		t.Fatal(err)
	}

	// The points and the comparison stay stable; the missing member does
	// not erase its recorded edges.
	rp1, err := s.GetRestorePoint("p1")
	if err != nil {
		t.Fatal(err)
	}
	if len(rp1.Members[0].References) != 1 || rp1.Members[0].References[0] != "lost" {
		t.Fatalf("recorded references lost: %+v", rp1.Members[0])
	}
	diff2, err := s.DiffRestorePoints("p1", "p2")
	if err != nil {
		t.Fatal(err)
	}
	if len(diff2.MemberChanges) != len(diff1.MemberChanges) || len(diff2.ReferenceChanges) != len(diff1.ReferenceChanges) {
		t.Fatalf("diff unstable after restore: %+v vs %+v", diff1, diff2)
	}
	if diff2.ReferenceChanges[0].From != "a" || diff2.ReferenceChanges[0].To != "lost" {
		t.Fatalf("missing member erased reference fact: %+v", diff2.ReferenceChanges)
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
