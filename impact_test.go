package localobjstore

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func objByKey(t *testing.T, an ImpactAnalysis, key string) ImpactObject {
	t.Helper()
	for _, o := range an.Objects {
		if o.Key == key {
			return o
		}
	}
	t.Fatalf("object %s not in scope of %s", key, an.ID)
	return ImpactObject{}
}

// The impact query starts from a business entry object and follows the
// directed references the restore point actually saved: the entry is
// reported as directly changed, everything reached only through reference
// chains as dependency-affected, with the full relationship path shown.
func TestImpactAnalysisDirectAndDependency(t *testing.T) {
	s, _ := openTemp(t)
	putStr(t, s, "app", "app1")
	putStr(t, s, "svc", "svc1")
	putStr(t, s, "db", "db1")
	putStr(t, s, "unrelated", "u1")
	if _, err := s.CreateRestorePoint("p1", []RestoreMemberInput{
		{Key: "app", References: []string{"svc"}},
		{Key: "svc", References: []string{"db"}},
		{Key: "db"},
		{Key: "unrelated"},
	}); err != nil {
		t.Fatalf("create: %v", err)
	}

	an, err := s.AnalyzeImpact(ImpactQuery{Point: "p1", Entries: []string{"app"}})
	if err != nil {
		t.Fatalf("analyze: %v", err)
	}
	if len(an.Objects) != 3 {
		t.Fatalf("scope=%d objects, want 3: %+v", len(an.Objects), an.Objects)
	}
	app := objByKey(t, an, "app")
	if !app.Direct {
		t.Fatal("entry object must be directly changed")
	}
	if len(app.Paths) != 1 || !reflect.DeepEqual(app.Paths[0], []string{"app"}) {
		t.Fatalf("entry paths: %v", app.Paths)
	}
	svc := objByKey(t, an, "svc")
	if svc.Direct {
		t.Fatal("svc is only dependency-affected")
	}
	if len(svc.Paths) != 1 || !reflect.DeepEqual(svc.Paths[0], []string{"app", "svc"}) {
		t.Fatalf("svc paths: %v", svc.Paths)
	}
	db := objByKey(t, an, "db")
	if db.Direct || len(db.Paths) != 1 || !reflect.DeepEqual(db.Paths[0], []string{"app", "svc", "db"}) {
		t.Fatalf("db paths: %v", db.Paths)
	}
	for _, o := range an.Objects {
		if o.Content != "readable" {
			t.Fatalf("%s content=%s", o.Key, o.Content)
		}
	}
	if len(an.Cycles) != 0 {
		t.Fatalf("unexpected cycles: %v", an.Cycles)
	}

	// An entry outside the restore point is rejected.
	if _, err := s.AnalyzeImpact(ImpactQuery{Point: "p1", Entries: []string{"ghost"}}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

// Cyclic references are preserved completely (the closing edge is shown)
// but traversal terminates and each object enters the scope exactly once.
func TestImpactAnalysisCyclesTerminate(t *testing.T) {
	s, _ := openTemp(t)
	putStr(t, s, "a", "a1")
	putStr(t, s, "b", "b1")
	putStr(t, s, "c", "c1")
	if _, err := s.CreateRestorePoint("cyc", []RestoreMemberInput{
		{Key: "a", References: []string{"b"}},
		{Key: "b", References: []string{"c"}},
		{Key: "c", References: []string{"a"}},
	}); err != nil {
		t.Fatalf("create: %v", err)
	}
	an, err := s.AnalyzeImpact(ImpactQuery{Point: "cyc", Entries: []string{"a"}})
	if err != nil {
		t.Fatalf("analyze: %v", err)
	}
	if len(an.Objects) != 3 {
		t.Fatalf("scope=%d, want 3 (no double counting): %+v", len(an.Objects), an.Objects)
	}
	seen := map[string]int{}
	for _, o := range an.Objects {
		seen[o.Key]++
	}
	for k, n := range seen {
		if n != 1 {
			t.Fatalf("%s counted %d times", k, n)
		}
	}
	if len(an.Cycles) != 1 || !reflect.DeepEqual(an.Cycles[0], []string{"a", "b", "c", "a"}) {
		t.Fatalf("cycles: %v", an.Cycles)
	}
	// The cycle edge is still a recorded in-scope edge.
	found := false
	for _, e := range an.Edges {
		if e.From == "c" && e.To == "a" {
			found = true
		}
	}
	if !found {
		t.Fatalf("cycle edge c->a missing from edges: %v", an.Edges)
	}
}

// Two restore points are compared with separate business entries each:
// the common recoverable scope and each side's missing dependencies are
// reported independently.
func TestImpactComparisonCommonAndMissing(t *testing.T) {
	s, _ := openTemp(t)
	putStr(t, s, "app", "app1")
	putStr(t, s, "svc", "svc1")
	putStr(t, s, "db", "db1")
	if _, err := s.CreateRestorePoint("pa", []RestoreMemberInput{
		{Key: "app", References: []string{"svc"}},
		{Key: "svc", References: []string{"db"}},
		{Key: "db"},
	}); err != nil {
		t.Fatalf("create pa: %v", err)
	}
	putStr(t, s, "app", "app2")
	putStr(t, s, "svc", "svc2")
	if _, err := s.CreateRestorePoint("pb", []RestoreMemberInput{
		{Key: "app", References: []string{"svc"}},
		{Key: "svc"},
	}); err != nil {
		t.Fatalf("create pb: %v", err)
	}

	cmp, err := s.CompareImpact(
		ImpactQuery{Point: "pa", Entries: []string{"app"}},
		ImpactQuery{Point: "pb", Entries: []string{"app"}},
	)
	if err != nil {
		t.Fatalf("compare: %v", err)
	}
	if len(cmp.Common) != 2 {
		t.Fatalf("common=%v", cmp.Common)
	}
	keys := []string{cmp.Common[0].Key, cmp.Common[1].Key}
	if !reflect.DeepEqual(keys, []string{"app", "svc"}) {
		t.Fatalf("common keys: %v", keys)
	}
	for _, c := range cmp.Common {
		if !c.Recoverable {
			t.Fatalf("%s should be recoverable on both sides", c.Key)
		}
	}
	if !reflect.DeepEqual(cmp.A.MissingDependencies, []string{"db"}) {
		t.Fatalf("A missing in B: %v", cmp.A.MissingDependencies)
	}
	if len(cmp.B.MissingDependencies) != 0 {
		t.Fatalf("B missing in A: %v", cmp.B.MissingDependencies)
	}
}

// When member content is unreadable, the structural impact (paths, edges,
// scope) is still reported in full and content availability is shown
// separately per object.
func TestImpactContentUnavailableShownSeparately(t *testing.T) {
	s, dir := openTemp(t)
	putStr(t, s, "app", "app1")
	msvc := putStr(t, s, "svc", "svc1")
	if _, err := s.CreateRestorePoint("p1", []RestoreMemberInput{
		{Key: "app", References: []string{"svc"}},
		{Key: "svc"},
	}); err != nil {
		t.Fatalf("create: %v", err)
	}
	corruptVersion(t, dir, "svc", msvc.VersionID, 1)

	an, err := s.AnalyzeImpact(ImpactQuery{Point: "p1", Entries: []string{"app"}})
	if err != nil {
		t.Fatalf("analyze: %v", err)
	}
	if len(an.Objects) != 2 {
		t.Fatalf("structural scope lost objects: %+v", an.Objects)
	}
	svc := objByKey(t, an, "svc")
	if svc.Content != "corrupted" || svc.ContentErr == "" {
		t.Fatalf("svc content=%s err=%q", svc.Content, svc.ContentErr)
	}
	if len(svc.Paths) != 1 || !reflect.DeepEqual(svc.Paths[0], []string{"app", "svc"}) {
		t.Fatalf("structural path lost for unreadable member: %v", svc.Paths)
	}
	if objByKey(t, an, "app").Content != "readable" {
		t.Fatal("app should stay readable")
	}
}

// Impact queries never modify restore points or restore objects: point
// files and current pointers are byte-identical afterwards.
func TestImpactQueryIsReadOnly(t *testing.T) {
	s, dir := openTemp(t)
	putStr(t, s, "app", "app1")
	putStr(t, s, "svc", "svc1")
	if _, err := s.CreateRestorePoint("p1", []RestoreMemberInput{
		{Key: "app", References: []string{"svc"}},
		{Key: "svc"},
	}); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := s.CreateRestorePoint("p2", []RestoreMemberInput{
		{Key: "app", References: []string{"svc"}},
		{Key: "svc"},
	}); err != nil {
		t.Fatalf("create: %v", err)
	}
	pointFile := filepath.Join(dir, "restorepoints", objectID("p1")+".json")
	before, err := os.ReadFile(pointFile)
	if err != nil {
		t.Fatal(err)
	}
	curBefore := currentID(t, s, "app")

	an, err := s.AnalyzeImpact(ImpactQuery{Point: "p1", Entries: []string{"app"}})
	if err != nil {
		t.Fatalf("analyze: %v", err)
	}
	if _, err := s.CompareImpact(
		ImpactQuery{Point: "p1", Entries: []string{"app"}},
		ImpactQuery{Point: "p2", Entries: []string{"svc"}},
	); err != nil {
		t.Fatalf("compare: %v", err)
	}
	if _, err := s.ExportImpactScope(an.ID, []string{"app"}); err != nil {
		t.Fatalf("export: %v", err)
	}

	after, err := os.ReadFile(pointFile)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, after) {
		t.Fatal("restore point file modified by impact query")
	}
	if cur := currentID(t, s, "app"); cur != curBefore {
		t.Fatalf("current pointer changed: %s -> %s", curBefore, cur)
	}
}

// A persisted analysis survives a reopen exactly as computed, and restore
// points created afterwards never change it.
func TestImpactAnalysisPersistsAcrossReopen(t *testing.T) {
	s, dir := openTemp(t)
	putStr(t, s, "app", "app1")
	putStr(t, s, "svc", "svc1")
	if _, err := s.CreateRestorePoint("p1", []RestoreMemberInput{
		{Key: "app", References: []string{"svc"}},
		{Key: "svc"},
	}); err != nil {
		t.Fatalf("create: %v", err)
	}
	an, err := s.AnalyzeImpact(ImpactQuery{Point: "p1", Entries: []string{"app"}})
	if err != nil {
		t.Fatalf("analyze: %v", err)
	}
	s.Close()

	s2, _, err := Open(dir, nil)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()
	got, err := s2.GetImpactAnalysis(an.ID)
	if err != nil {
		t.Fatalf("get after reopen: %v", err)
	}
	if !reflect.DeepEqual(an, got) {
		t.Fatalf("analysis changed across reopen:\n%+v\n%+v", an, got)
	}

	// A newer restore point does not alter the old analysis.
	putStr(t, s2, "app", "app2")
	if _, err := s2.CreateRestorePoint("p2", []RestoreMemberInput{{Key: "app"}, {Key: "svc"}}); err != nil {
		t.Fatalf("create p2: %v", err)
	}
	got2, err := s2.GetImpactAnalysis(an.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(an, got2) {
		t.Fatal("new restore point changed the old analysis")
	}
	list, err := s2.ListImpactAnalyses()
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 || list[0].ID != an.ID {
		t.Fatalf("list: %+v", list)
	}
}

// Exporting a selected scope explicitly names the recorded references that
// cross the selection boundary and are therefore not included.
func TestImpactExportCrossBoundaryReferences(t *testing.T) {
	s, _ := openTemp(t)
	putStr(t, s, "app", "app1")
	putStr(t, s, "svc", "svc1")
	putStr(t, s, "db", "db1")
	if _, err := s.CreateRestorePoint("p1", []RestoreMemberInput{
		{Key: "app", References: []string{"svc"}},
		{Key: "svc", References: []string{"db"}},
		{Key: "db"},
	}); err != nil {
		t.Fatalf("create: %v", err)
	}
	an, err := s.AnalyzeImpact(ImpactQuery{Point: "p1", Entries: []string{"app"}})
	if err != nil {
		t.Fatalf("analyze: %v", err)
	}

	exp, err := s.ExportImpactScope(an.ID, []string{"app", "svc"})
	if err != nil {
		t.Fatalf("export: %v", err)
	}
	if len(exp.Included) != 2 {
		t.Fatalf("included: %+v", exp.Included)
	}
	want := []ImpactCrossRef{{From: "svc", To: "db"}}
	if !reflect.DeepEqual(exp.ExcludedCrossReferences, want) {
		t.Fatalf("cross refs: %+v want %+v", exp.ExcludedCrossReferences, want)
	}

	// Full-scope export has no cross-boundary references.
	full, err := s.ExportImpactScope(an.ID, []string{"app", "svc", "db"})
	if err != nil {
		t.Fatal(err)
	}
	if len(full.ExcludedCrossReferences) != 0 {
		t.Fatalf("full export cross refs: %+v", full.ExcludedCrossReferences)
	}

	// Exporting an object outside the analysis scope is rejected.
	if _, err := s.ExportImpactScope(an.ID, []string{"ghost"}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}
