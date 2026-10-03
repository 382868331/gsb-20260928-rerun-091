package localobjstore

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func mustPut(t *testing.T, s *Store, key, data string) Manifest {
	t.Helper()
	m, err := s.PutObject(key, strings.NewReader(data), nil)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func getString(t *testing.T, s *Store, key string) string {
	t.Helper()
	r, err := s.GetObject(key)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	b, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// A recovery point captures the exact current versions plus the reference
// graph, and two points can be diffed.
func TestRecoveryPointCreateDescribeDiff(t *testing.T) {
	s, _ := openTemp(t)
	defer s.Close()
	a1 := mustPut(t, s, "doc/a", "a1")
	b1 := mustPut(t, s, "doc/b", "b1")
	mustPut(t, s, "doc/c", "c1")

	rp1, err := s.CreateRecoveryPoint("release-1", []GroupMemberSpec{
		{Key: "doc/a", Refs: []string{"doc/b"}},
		{Key: "doc/b", Refs: []string{"doc/a"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(rp1.Members) != 2 || rp1.Members[0].VersionID != a1.VersionID {
		t.Fatalf("unexpected members: %+v", rp1.Members)
	}
	if _, err := s.CreateRecoveryPoint("release-1", []GroupMemberSpec{{Key: "doc/a"}}); !errors.Is(err, ErrGroupExists) {
		t.Fatalf("expected ErrGroupExists, got %v", err)
	}
	if _, err := s.CreateRecoveryPoint("bad", []GroupMemberSpec{{Key: "doc/a", Refs: []string{"doc/c"}}}); err == nil {
		t.Fatal("reference to non-member must fail")
	}

	// New business commit: a changes, c joins the group.
	a2 := mustPut(t, s, "doc/a", "a2")
	if _, err := s.CreateRecoveryPoint("release-2", []GroupMemberSpec{
		{Key: "doc/a", Refs: []string{"doc/b", "doc/c"}},
		{Key: "doc/b"},
		{Key: "doc/c"},
	}); err != nil {
		t.Fatal(err)
	}

	desc, err := s.DescribeRecoveryPoint("release-1")
	if err != nil {
		t.Fatal(err)
	}
	if desc.Members[1].VersionID != b1.VersionID || len(desc.Members[0].Refs) != 1 {
		t.Fatalf("describe lost members/refs: %+v", desc)
	}

	diff, err := s.DiffRecoveryPoints("release-1", "release-2")
	if err != nil {
		t.Fatal(err)
	}
	if len(diff.Added) != 1 || diff.Added[0].Key != "doc/c" {
		t.Fatalf("expected doc/c added: %+v", diff.Added)
	}
	if len(diff.Changed) != 2 {
		t.Fatalf("expected doc/a and doc/b changed: %+v", diff.Changed)
	}
	var ca, cb *GroupDiffChange
	for i := range diff.Changed {
		switch diff.Changed[i].Key {
		case "doc/a":
			ca = &diff.Changed[i]
		case "doc/b":
			cb = &diff.Changed[i]
		}
	}
	if ca == nil || ca.FromVersion != a1.VersionID || ca.ToVersion != a2.VersionID ||
		len(ca.RefsAdded) != 1 || ca.RefsAdded[0] != "doc/c" {
		t.Fatalf("doc/a change: %+v", ca)
	}
	if cb == nil || cb.FromVersion != cb.ToVersion || len(cb.RefsRemoved) != 1 || cb.RefsRemoved[0] != "doc/a" {
		t.Fatalf("doc/b ref removal: %+v", cb)
	}
	ps, _ := s.ListRecoveryPoints()
	if len(ps) != 2 {
		t.Fatalf("expected 2 points, got %d", len(ps))
	}
}

// An upload still in flight must never leak into a confirmed point: the
// point records the version that was current when it was confirmed, and a
// later publish does not change the record.
func TestRecoveryPointExcludesInFlightWrites(t *testing.T) {
	s, _ := openTemp(t)
	defer s.Close()
	v1 := mustPut(t, s, "k", "committed")

	u, err := s.BeginMultipart("k")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := u.WritePart(strings.NewReader("in-flight")); err != nil {
		t.Fatal(err)
	}
	rp, err := s.CreateRecoveryPoint("p1", []GroupMemberSpec{{Key: "k"}})
	if err != nil {
		t.Fatal(err)
	}
	if rp.Members[0].VersionID != v1.VersionID {
		t.Fatalf("in-flight write leaked into recovery point: %+v", rp.Members[0])
	}
	if _, err := u.Complete(nil); err != nil {
		t.Fatal(err)
	}
	desc, _ := s.DescribeRecoveryPoint("p1")
	if desc.Members[0].VersionID != v1.VersionID {
		t.Fatal("confirmed recovery point changed after a later publish")
	}
}

// Versions referenced by a recovery point survive deletes and GC until the
// point is deleted; the delete report explains what is still occupied.
func TestRecoveryPointRetentionAndDeleteReport(t *testing.T) {
	s, _ := openTemp(t)
	defer s.Close()
	a1 := mustPut(t, s, "a", "a1")
	b1 := mustPut(t, s, "b", "b1")
	if _, err := s.CreateRecoveryPoint("keep", []GroupMemberSpec{{Key: "a"}, {Key: "b"}}); err != nil {
		t.Fatal(err)
	}
	mustPut(t, s, "a", "a2")
	if err := s.PinVersion("hold-b", "b", b1.VersionID); err != nil {
		t.Fatal(err)
	}
	// Open reader on the old a version keeps it occupied too.
	r, err := s.GetVersion("a", a1.VersionID)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	rep, err := s.CollectGarbage()
	if err != nil {
		t.Fatal(err)
	}
	if gcActions(rep, "reclaimed") != 0 {
		t.Fatalf("group-referenced versions must be retained: %+v", rep.Items)
	}

	drep, err := s.DeleteRecoveryPoint("keep")
	if err != nil {
		t.Fatal(err)
	}
	if len(drep.Released) != 0 {
		t.Fatalf("both versions still occupied: %+v", drep.Released)
	}
	held := make(map[string][]string)
	for _, rv := range drep.Retained {
		held[rv.Key] = rv.HeldBy
	}
	if !containsAll(held["a"], []string{"open-reader"}) {
		t.Fatalf("expected open-reader hold on a: %v", held["a"])
	}
	if !containsAll(held["b"], []string{"current", "pin:hold-b"}) {
		t.Fatalf("expected current+pin holds on b: %v", held["b"])
	}

	// Once the reader closes and the pin is removed, GC reclaims the old
	// versions; b1 is still current so it stays.
	r.Close()
	if err := s.UnpinVersion("hold-b"); err != nil {
		t.Fatal(err)
	}
	mustPut(t, s, "b", "b2")
	rep, _ = s.CollectGarbage()
	if gcActions(rep, "reclaimed") != 2 {
		t.Fatalf("expected 2 reclamations after release: %+v", rep.Items)
	}
}

// Full restore: members deleted after the capture come back exactly as
// recorded, only after every member is restorable, and the journal is kept.
func TestGroupRestoreFull(t *testing.T) {
	s, dir := openTemp(t)
	mustPut(t, s, "x", "x-data")
	mustPut(t, s, "y", "y-data")
	if _, err := s.CreateRecoveryPoint("snap", []GroupMemberSpec{{Key: "x", Refs: []string{"y"}}, {Key: "y"}}); err != nil {
		t.Fatal(err)
	}
	mustPut(t, s, "x", "x-newer")
	if err := s.DeleteObject("y"); err != nil {
		t.Fatal(err)
	}

	plan, err := s.PlanGroupRestore("snap")
	if err != nil {
		t.Fatal(err)
	}
	if !plan.Complete() {
		t.Fatalf("plan should be complete: %+v", plan.Members)
	}
	rep, err := s.RestoreGroup("snap", false)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Journal.Status != "completed" {
		t.Fatalf("journal: %+v", rep.Journal)
	}
	if got := getString(t, s, "x"); got != "x-data" {
		t.Fatalf("x = %q", got)
	}
	if got := getString(t, s, "y"); got != "y-data" {
		t.Fatalf("y = %q", got)
	}
	cur, _ := s.CurrentVersion("x")
	if cur.RestoredFrom == "" || cur.GroupRestore != rep.Journal.ID {
		t.Fatalf("restored version must name its source and journal: %+v", cur)
	}
	s.Close()

	// Reopen: the restore record is durable history, members stay committed.
	s2, rep2, err := Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	for _, it := range rep2.Items {
		if it.Kind == "group-restore" {
			t.Fatalf("completed restore must not be reported as interrupted: %+v", it)
		}
	}
	js, _ := s2.ListRestoreJournals()
	if len(js) != 1 || js[0].Status != "completed" {
		t.Fatalf("journals: %+v", js)
	}
	if got := getString(t, s2, "y"); got != "y-data" {
		t.Fatalf("y after reopen = %q", got)
	}
}

// A damaged member blocks a full restore; the caller must explicitly accept
// a partial restore, which records durable gap markers. Corrupt evidence is
// never touched.
func TestGroupRestorePartialWithGaps(t *testing.T) {
	s, dir := openTemp(t)
	good := mustPut(t, s, "good", "good-data")
	bad := mustPut(t, s, "bad", "bad-data")
	if _, err := s.CreateRecoveryPoint("mix", []GroupMemberSpec{{Key: "good"}, {Key: "bad"}}); err != nil {
		t.Fatal(err)
	}
	// Damage the recorded version of one member beyond readability.
	corruptVersion(t, dir, "bad", bad.VersionID, 1)
	mustPut(t, s, "good", "good-newer")

	// Before any restart the damage is detectable in place.
	pre, err := s.PlanGroupRestore("mix")
	if err != nil {
		t.Fatal(err)
	}
	if pre.Complete() {
		t.Fatal("plan must detect the corrupted member")
	}
	for _, mh := range pre.Members {
		if mh.Member.Key == "bad" && mh.Status != "corrupted" {
			t.Fatalf("expected corrupted, got %s (%v)", mh.Status, mh.Err)
		}
	}
	s.Close()

	s2, _, err := Open(dir, &Options{VerifyOnOpen: true})
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()

	plan, err := s2.PlanGroupRestore("mix")
	if err != nil {
		t.Fatal(err)
	}
	if plan.Complete() {
		t.Fatal("plan must be incomplete with a corrupted member")
	}
	status := map[string]string{}
	for _, mh := range plan.Members {
		status[mh.Member.Key] = mh.Status
	}
	// VerifyOnOpen quarantined the corrupt version, so it now reads missing.
	if status["good"] != "ready" || status["bad"] != "missing" {
		t.Fatalf("plan statuses: %v", status)
	}

	if _, err := s2.RestoreGroup("mix", false); !errors.Is(err, ErrGroupIncomplete) {
		t.Fatalf("expected ErrGroupIncomplete, got %v", err)
	}
	if got := getString(t, s2, "good"); got != "good-newer" {
		t.Fatalf("failed full restore must change nothing, good = %q", got)
	}

	rep, err := s2.RestoreGroup("mix", true)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Journal.Status != "completed-partial" {
		t.Fatalf("journal status: %+v", rep.Journal)
	}
	var gaps int
	for _, m := range rep.Journal.Members {
		if m.Status == "gap" {
			gaps++
			if m.Key != "bad" || m.Reason == "" {
				t.Fatalf("gap marker incomplete: %+v", m)
			}
		}
	}
	if gaps != 1 {
		t.Fatalf("expected exactly one gap: %+v", rep.Journal.Members)
	}
	if got := getString(t, s2, "good"); got != "good-data" {
		t.Fatalf("good = %q after partial restore", got)
	}
	// The corrupted bytes were quarantined by recovery, not deleted.
	entries, _ := os.ReadDir(filepath.Join(dir, "quarantine", "version"))
	if len(entries) == 0 {
		t.Fatal("corrupt evidence must be preserved in quarantine")
	}
	// The good member's restored record points at the captured version.
	if _, err := s2.GetVersion("good", good.VersionID); err != nil {
		t.Fatal(err)
	}
}

// A member whose recorded version is gone entirely shows up as "missing";
// another point's version is never substituted to fake completeness.
func TestGroupRestoreMissingMember(t *testing.T) {
	s, dir := openTemp(t)
	m1 := mustPut(t, s, "m", "m1")
	mustPut(t, s, "n", "n1")
	if _, err := s.CreateRecoveryPoint("p", []GroupMemberSpec{{Key: "m"}, {Key: "n"}}); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(filepath.Join(dir, "objects", objectID("m"), "versions", m1.VersionID)); err != nil {
		t.Fatal(err)
	}
	plan, err := s.PlanGroupRestore("p")
	if err != nil {
		t.Fatal(err)
	}
	if plan.Members[0].Status != "missing" {
		t.Fatalf("expected missing member: %+v", plan.Members[0])
	}
	if _, err := s.RestoreGroup("p", false); !errors.Is(err, ErrGroupIncomplete) {
		t.Fatalf("expected ErrGroupIncomplete, got %v", err)
	}
}

// An interrupted restore is reconciled on the next Open: confirmed members
// stay confirmed, unfinished ones are reported, and the journal keeps both.
func TestGroupRestoreInterruptedReconciledOnOpen(t *testing.T) {
	s, dir := openTemp(t)
	mustPut(t, s, "p", "p1")
	mustPut(t, s, "q", "q1")
	if _, err := s.CreateRecoveryPoint("g", []GroupMemberSpec{{Key: "p"}, {Key: "q"}}); err != nil {
		t.Fatal(err)
	}
	rep, err := s.RestoreGroup("g", false)
	if err != nil {
		t.Fatal(err)
	}
	// Simulate a crash mid-restore: rewind the durable journal to the state
	// where "p" was committed and "q" was still pending.
	j := rep.Journal
	j.Status = "in-progress"
	j.FinishedAt = time.Time{}
	j.Members[1].Status = "pending"
	j.Members[1].NewVersion = ""
	if err := s.writeJournal(j); err != nil {
		t.Fatal(err)
	}
	s.Close()

	s2, rep2, err := Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	var items int
	for _, it := range rep2.Items {
		if it.Kind == "group-restore" && it.Action == "restore-interrupted" {
			items++
			if !strings.Contains(it.Reason, "1 members confirmed, 1 incomplete") {
				t.Fatalf("unexpected reconcile summary: %s", it.Reason)
			}
		}
	}
	if items != 1 {
		t.Fatalf("expected one interrupted-restore report item, got %d", items)
	}
	js, err := s2.ListRestoreJournals()
	if err != nil {
		t.Fatal(err)
	}
	if len(js) != 1 || js[0].Status != "interrupted" {
		t.Fatalf("journal must be marked interrupted: %+v", js)
	}
	if js[0].Members[0].Status != "committed" {
		t.Fatalf("confirmed member must stay confirmed: %+v", js[0].Members[0])
	}
	if js[0].Members[1].Status != "incomplete" {
		t.Fatalf("unfinished member must be marked incomplete: %+v", js[0].Members[1])
	}
}

func containsAll(haystack, needles []string) bool {
	set := make(map[string]bool)
	for _, h := range haystack {
		set[h] = true
	}
	for _, n := range needles {
		if !set[n] {
			return false
		}
	}
	return true
}
