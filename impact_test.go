package localobjstore

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func scopeKeys(side ImpactSide) map[string]ImpactNode {
	m := make(map[string]ImpactNode, len(side.Scope))
	for _, n := range side.Scope {
		m[n.Key] = n
	}
	return m
}

// Object-entry impact query over one restore point: the recorded
// reference graph is traversed from the entry, cycles are preserved in
// the paths but every object enters the scope exactly once.
func TestImpactAnalysisSinglePointCycle(t *testing.T) {
	s, _ := openTemp(t)
	putStr(t, s, "app", "a")
	putStr(t, s, "svc", "b")
	putStr(t, s, "db", "c")
	if _, err := s.CreateRestorePoint("p1", []RestoreMemberInput{
		{Key: "app", References: []string{"svc"}},
		{Key: "svc", References: []string{"db", "app"}}, // cycle svc -> app
		{Key: "db", References: []string{"svc"}},        // cycle db -> svc
	}); err != nil {
		t.Fatal(err)
	}

	an, err := s.CreateImpactAnalysis("ia1", ImpactQuery{Point: "p1", Entries: []string{"app"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(an.Sides) != 1 {
		t.Fatalf("sides=%d", len(an.Sides))
	}
	scope := scopeKeys(an.Sides[0])
	if len(scope) != 3 {
		t.Fatalf("scope should count each object once, got %d: %v", len(scope), scope)
	}
	for _, k := range []string{"app", "svc", "db"} {
		if _, ok := scope[k]; !ok {
			t.Fatalf("%s missing from scope", k)
		}
	}
	if !scope["app"].Entry || !scope["app"].Direct {
		t.Fatalf("entry should be direct in a single-point query: %+v", scope["app"])
	}
	if scope["svc"].Direct || scope["db"].Direct {
		t.Fatal("non-entry objects are dependency-affected only")
	}
	// The cycle svc -> app is preserved as a closed path on app, and
	// db -> svc as a closed path on svc, without re-traversal.
	var closed [][]string
	for _, p := range scope["app"].Paths {
		if p[len(p)-1] == "app" && len(p) > 1 {
			closed = append(closed, p)
		}
	}
	if len(closed) != 1 || closed[0][0] != "app" || closed[0][1] != "svc" {
		t.Fatalf("cycle path not preserved: %v", scope["app"].Paths)
	}
	var svcCycle bool
	for _, p := range scope["svc"].Paths {
		if p[len(p)-1] == "svc" && len(p) > 1 {
			svcCycle = true
		}
	}
	if !svcCycle {
		t.Fatalf("db -> svc cycle not preserved: %v", scope["svc"].Paths)
	}
	for k, n := range scope {
		if n.Status != "readable" {
			t.Fatalf("%s status=%s", k, n.Status)
		}
	}
}

// Comparing two restore points with per-point business entries: direct
// changes vs dependency-only impact, common recoverable scope and each
// side's missing dependencies.
func TestImpactAnalysisCompareTwoPoints(t *testing.T) {
	s, _ := openTemp(t)
	putStr(t, s, "app", "a1")
	putStr(t, s, "svc", "s1")
	putStr(t, s, "db", "d1")
	putStr(t, s, "cache", "c1")
	if _, err := s.CreateRestorePoint("old", []RestoreMemberInput{
		{Key: "app", References: []string{"svc"}},
		{Key: "svc", References: []string{"db", "cache"}},
		{Key: "db"},
		{Key: "cache"},
	}); err != nil {
		t.Fatal(err)
	}
	putStr(t, s, "svc", "s2") // direct change
	if _, err := s.CreateRestorePoint("new", []RestoreMemberInput{
		{Key: "app", References: []string{"svc"}},
		{Key: "svc", References: []string{"db"}},
		{Key: "db"},
	}); err != nil {
		t.Fatal(err)
	}

	an, err := s.CreateImpactAnalysis("cmp",
		ImpactQuery{Point: "old", Entries: []string{"app"}},
		ImpactQuery{Point: "new", Entries: []string{"app"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(an.Sides) != 2 {
		t.Fatalf("sides=%d", len(an.Sides))
	}
	oldScope := scopeKeys(an.Sides[0])
	if !oldScope["svc"].Direct {
		t.Fatal("svc changed version between the points: must be direct")
	}
	if oldScope["app"].Direct || oldScope["db"].Direct {
		t.Fatal("app and db are only dependency-affected")
	}
	if len(an.Common) != 3 {
		t.Fatalf("common scope: %+v", an.Common)
	}
	for _, c := range an.Common {
		if !c.Recoverable {
			t.Fatalf("%s should be recoverable on both sides", c.Key)
		}
	}
	// cache is a member of old but not reachable from app's entries on
	// the new side: it is a dependency missing from the new scope.
	if len(an.MissingB) != 1 || an.MissingB[0] != "cache" {
		t.Fatalf("missing on new side: %v", an.MissingB)
	}
	if len(an.MissingA) != 0 {
		t.Fatalf("missing on old side: %v", an.MissingA)
	}
}

// Structural impact and content availability are reported separately:
// an unreadable member keeps its recorded place in the graph.
func TestImpactAnalysisAvailabilitySeparateFromStructure(t *testing.T) {
	s, dir := openTemp(t)
	putStr(t, s, "app", "a")
	bad := putStr(t, s, "svc", "s")
	if _, err := s.CreateRestorePoint("p", []RestoreMemberInput{
		{Key: "app", References: []string{"svc"}},
		{Key: "svc"},
	}); err != nil {
		t.Fatal(err)
	}
	corruptVersion(t, dir, "svc", bad.VersionID, 1)

	an, err := s.CreateImpactAnalysis("avail", ImpactQuery{Point: "p", Entries: []string{"app"}})
	if err != nil {
		t.Fatal(err)
	}
	scope := scopeKeys(an.Sides[0])
	if len(scope) != 2 {
		t.Fatalf("corrupted member dropped from structural scope: %v", scope)
	}
	if scope["svc"].Status != "corrupted" || scope["svc"].StatusErr == "" {
		t.Fatalf("svc availability: %+v", scope["svc"])
	}
	if scope["app"].Status != "readable" {
		t.Fatalf("app availability: %+v", scope["app"])
	}
	if len(scope["svc"].Paths) != 1 || scope["svc"].Paths[0][0] != "app" {
		t.Fatalf("relationship path kept despite unreadable content: %v", scope["svc"].Paths)
	}
}

// A saved analysis survives a reopen, is never recomputed, and restore
// points created afterwards do not change it. The query never modifies
// the restore points or the objects.
func TestImpactAnalysisPersistedAndReadOnly(t *testing.T) {
	s, dir := openTemp(t)
	putStr(t, s, "app", "a1")
	putStr(t, s, "svc", "s1")
	if _, err := s.CreateRestorePoint("p1", []RestoreMemberInput{
		{Key: "app", References: []string{"svc"}},
		{Key: "svc"},
	}); err != nil {
		t.Fatal(err)
	}
	pointFile := filepath.Join(dir, "restorepoints", objectID("p1")+".json")
	before, err := os.ReadFile(pointFile)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := s.CreateImpactAnalysis("snap", ImpactQuery{Point: "p1", Entries: []string{"app"}}); err != nil {
		t.Fatal(err)
	}
	// Duplicate names are rejected; saved analyses are immutable.
	if _, err := s.CreateImpactAnalysis("snap", ImpactQuery{Point: "p1", Entries: []string{"app"}}); !errors.Is(err, ErrImpactAnalysisExists) {
		t.Fatalf("expected ErrImpactAnalysisExists, got %v", err)
	}
	// The query did not rewrite the restore point.
	after, err := os.ReadFile(pointFile)
	if err != nil {
		t.Fatal(err)
	}
	if string(before) != string(after) {
		t.Fatal("impact query modified the restore point")
	}

	// A later restore point must not change the saved analysis.
	putStr(t, s, "app", "a2")
	if _, err := s.CreateRestorePoint("p2", []RestoreMemberInput{{Key: "app"}, {Key: "svc"}}); err != nil {
		t.Fatal(err)
	}
	s.Close()
	s2, _, err := Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	got, err := s2.GetImpactAnalysis("snap")
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Sides) != 1 || got.Sides[0].Query.Point != "p1" {
		t.Fatalf("reopened analysis: %+v", got.Sides)
	}
	for _, n := range got.Sides[0].Scope {
		if n.Key == "app" && n.VersionID == "" {
			t.Fatal("analysis lost the version captured at creation time")
		}
	}
	list, err := s2.ListImpactAnalyses()
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 || list[0].Name != "snap" {
		t.Fatalf("list: %+v", list)
	}
	if _, err := s2.GetImpactAnalysis("nope"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

// Exporting a selected scope explicitly lists the recorded references
// that cross the selection boundary and are therefore not included.
func TestImpactExportCrossBoundaryReferences(t *testing.T) {
	s, _ := openTemp(t)
	putStr(t, s, "app", "a")
	putStr(t, s, "svc", "s")
	putStr(t, s, "db", "d")
	if _, err := s.CreateRestorePoint("p", []RestoreMemberInput{
		{Key: "app", References: []string{"svc"}},
		{Key: "svc", References: []string{"db"}},
		{Key: "db"},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateImpactAnalysis("exp", ImpactQuery{Point: "p", Entries: []string{"app"}}); err != nil {
		t.Fatal(err)
	}

	exp, err := s.ExportImpactScope("exp", "p", []string{"app", "svc"})
	if err != nil {
		t.Fatal(err)
	}
	if len(exp.Members) != 2 {
		t.Fatalf("exported members: %+v", exp.Members)
	}
	if len(exp.CrossBoundaryReferences) != 1 ||
		exp.CrossBoundaryReferences[0].From != "svc" ||
		exp.CrossBoundaryReferences[0].To != "db" {
		t.Fatalf("cross-boundary references: %+v", exp.CrossBoundaryReferences)
	}

	// Keys outside the impacted scope cannot be exported.
	if _, err := s.ExportImpactScope("exp", "p", []string{"ghost"}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
	if _, err := s.ExportImpactScope("exp", "other-point", []string{"app"}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestImpactAnalysisValidation(t *testing.T) {
	s, _ := openTemp(t)
	putStr(t, s, "app", "a")
	putStr(t, s, "svc", "s")
	if _, err := s.CreateRestorePoint("p", []RestoreMemberInput{
		{Key: "app", References: []string{"svc"}},
		{Key: "svc"},
	}); err != nil {
		t.Fatal(err)
	}
	// Entry must be a member of the point.
	if _, err := s.CreateImpactAnalysis("bad1", ImpactQuery{Point: "p", Entries: []string{"ghost"}}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
	// Unknown restore point.
	if _, err := s.CreateImpactAnalysis("bad2", ImpactQuery{Point: "nope", Entries: []string{"app"}}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
	// No entries.
	if _, err := s.CreateImpactAnalysis("bad3", ImpactQuery{Point: "p"}); err == nil {
		t.Fatal("expected error for empty entries")
	}
	// A failed analysis must not leave a visible record behind.
	list, err := s.ListImpactAnalyses()
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 0 {
		t.Fatalf("failed analyses leaked: %+v", list)
	}
}
