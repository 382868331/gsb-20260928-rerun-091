package localobjstore

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// ImpactQuery selects the business entry objects whose reference
// relationships are traced inside one restore point.
type ImpactQuery struct {
	Point   string   `json:"point"`
	Entries []string `json:"entries"`
}

// ImpactObject is one object inside the affected scope of an impact
// analysis. Direct marks the entry objects themselves (the ones that
// directly changed); every other object is present only because a
// dependency chain of recorded references reaches it. Paths lists every
// simple directed path from an entry to this object, so the operator can
// see exactly which relationship chain makes it part of the scope. Content
// reports the availability of the member's bytes, kept separate from the
// structural impact: an unreadable member still takes part in the
// reference topology exactly as the point recorded it.
type ImpactObject struct {
	Key        string     `json:"key"`
	VersionID  string     `json:"version_id"`
	Direct     bool       `json:"direct"`
	Paths      [][]string `json:"paths"`
	Content    string     `json:"content"` // "readable" | "corrupted" | "missing"
	ContentErr string     `json:"content_err,omitempty"`
}

// ImpactEdge is one directed reference between two objects inside the
// affected scope, copied verbatim from the restore point.
type ImpactEdge struct {
	From string `json:"from"`
	To   string `json:"to"`
}

// ImpactAnalysis is the durable, immutable result of tracing the directed
// references a restore point actually saved, starting from the chosen
// business entry objects. It is computed once, persisted under impact/ and
// never rewritten: reopening the store shows exactly the same analysis,
// and restore points created later cannot change it (the source point is
// pinned by name and id, and every reported fact is stored verbatim).
//
// Cycles in the reference graph are preserved completely: every cyclic
// chain appears in Cycles as a path whose last element repeats an earlier
// one. Traversal never follows a cycle past its closing edge, so the walk
// always terminates and each object enters the scope exactly once.
type ImpactAnalysis struct {
	ID        string         `json:"id"`
	PointName string         `json:"point_name"`
	PointID   string         `json:"point_id"`
	Entries   []string       `json:"entries"`
	Objects   []ImpactObject `json:"objects"` // unique by key, sorted
	Edges     []ImpactEdge   `json:"edges"`   // recorded edges with both ends in scope
	Cycles    [][]string     `json:"cycles,omitempty"`
	CreatedAt time.Time      `json:"created_at"`
}

// ImpactSide is one half of a two-point impact comparison.
type ImpactSide struct {
	Query    ImpactQuery    `json:"query"`
	Analysis ImpactAnalysis `json:"analysis"`
	// MissingDependencies lists objects of this side's affected scope that
	// the other point does not contain as members at all: recovering the
	// other point cannot provide them.
	MissingDependencies []string `json:"missing_dependencies,omitempty"`
}

// ImpactCommonObject is one object present in both affected scopes, with
// the content availability each side offers for it.
type ImpactCommonObject struct {
	Key         string `json:"key"`
	VersionA    string `json:"version_a"`
	VersionB    string `json:"version_b"`
	ContentA    string `json:"content_a"`
	ContentB    string `json:"content_b"`
	Recoverable bool   `json:"recoverable"` // readable on both sides
}

// ImpactComparison contrasts the affected scopes of two restore points,
// each limited by its own business entries. Common is the shared
// recoverable scope; each side separately reports the dependencies the
// other point is missing.
type ImpactComparison struct {
	A      ImpactSide           `json:"a"`
	B      ImpactSide           `json:"b"`
	Common []ImpactCommonObject `json:"common"`
}

// ImpactCrossRef is a recorded reference from an exported object to an
// object that is not part of the exported selection.
type ImpactCrossRef struct {
	From string `json:"from"`
	To   string `json:"to"`
}

// ImpactExport is the read-only export manifest for a selected subset of
// an impact analysis. ExcludedCrossReferences explicitly names every
// recorded reference that leaves the selected scope, so a consumer of the
// export knows exactly which cross-boundary relationships were not
// included. Exporting never modifies the analysis, the restore point or
// any object.
type ImpactExport struct {
	AnalysisID              string           `json:"analysis_id"`
	PointName               string           `json:"point_name"`
	Included                []RestoreMember  `json:"included"`
	ExcludedCrossReferences []ImpactCrossRef `json:"excluded_cross_references,omitempty"`
}

func (s *Store) impactDir() string { return filepath.Join(s.root, "impact") }

func (s *Store) impactPath(id string) string {
	return filepath.Join(s.impactDir(), id+".json")
}

// computeImpact traces the directed references recorded in the named
// restore point, starting from the entry keys. It only reads the point
// file and verifies member content; nothing is modified.
func (s *Store) computeImpact(query ImpactQuery) (ImpactAnalysis, error) {
	if len(query.Entries) == 0 {
		return ImpactAnalysis{}, errors.New("localobjstore: impact query needs at least one entry object")
	}
	rp, err := s.GetRestorePoint(query.Point)
	if err != nil {
		return ImpactAnalysis{}, err
	}
	members := make(map[string]RestoreMember, len(rp.Members))
	out := make(map[string][]string, len(rp.Members))
	for _, m := range rp.Members {
		members[m.Key] = m
		out[m.Key] = append(out[m.Key], m.References...)
	}
	seenEntry := make(map[string]bool)
	entries := make([]string, 0, len(query.Entries))
	for _, e := range query.Entries {
		if e == "" {
			return ImpactAnalysis{}, errors.New("localobjstore: empty entry key")
		}
		if seenEntry[e] {
			continue
		}
		seenEntry[e] = true
		if _, ok := members[e]; !ok {
			return ImpactAnalysis{}, fmt.Errorf("%w: entry %q is not a member of restore point %q", ErrNotFound, e, query.Point)
		}
		entries = append(entries, e)
	}
	sort.Strings(entries)

	// Enumerate simple directed paths from every entry. A path never
	// repeats an object, so traversal terminates even on cyclic graphs;
	// an edge that would close a cycle is recorded in Cycles (with the
	// repeated object appended) instead of being followed.
	objects := make(map[string]*ImpactObject)
	pathSeen := make(map[string]map[string]bool)
	var cycles [][]string
	cycleSeen := make(map[string]bool)
	addPath := func(key string, path []string) {
		obj, ok := objects[key]
		if !ok {
			obj = &ImpactObject{Key: key, VersionID: members[key].VersionID}
			objects[key] = obj
			pathSeen[key] = make(map[string]bool)
		}
		sig := fmt.Sprintf("%q", path)
		if !pathSeen[key][sig] {
			pathSeen[key][sig] = true
			obj.Paths = append(obj.Paths, append([]string(nil), path...))
		}
	}
	var walk func(path []string)
	walk = func(path []string) {
		last := path[len(path)-1]
		addPath(last, path)
		inPath := make(map[string]bool, len(path))
		for _, k := range path {
			inPath[k] = true
		}
		for _, next := range out[last] {
			if inPath[next] {
				cyc := append(append([]string(nil), path...), next)
				sig := fmt.Sprintf("%q", cyc)
				if !cycleSeen[sig] {
					cycleSeen[sig] = true
					cycles = append(cycles, cyc)
				}
				continue
			}
			walk(append(append([]string(nil), path...), next))
		}
	}
	for _, e := range entries {
		walk([]string{e})
	}

	an := ImpactAnalysis{
		PointName: rp.Name,
		PointID:   rp.ID,
		Entries:   entries,
		CreatedAt: time.Now().UTC(),
	}
	keys := make([]string, 0, len(objects))
	for k := range objects {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		obj := objects[k]
		obj.Direct = seenEntry[k]
		sort.Slice(obj.Paths, func(i, j int) bool {
			a, b := obj.Paths[i], obj.Paths[j]
			if len(a) != len(b) {
				return len(a) < len(b)
			}
			return fmt.Sprintf("%q", a) < fmt.Sprintf("%q", b)
		})
		status, cerr := s.checkMemberContent(k, obj.VersionID)
		obj.Content = status
		if cerr != nil {
			obj.ContentErr = cerr.Error()
		}
		an.Objects = append(an.Objects, *obj)
	}
	inScope := make(map[string]bool, len(keys))
	for _, k := range keys {
		inScope[k] = true
	}
	for _, k := range keys {
		for _, ref := range out[k] {
			if inScope[ref] {
				an.Edges = append(an.Edges, ImpactEdge{From: k, To: ref})
			}
		}
	}
	sort.Slice(cycles, func(i, j int) bool {
		return fmt.Sprintf("%q", cycles[i]) < fmt.Sprintf("%q", cycles[j])
	})
	an.Cycles = cycles
	return an, nil
}

// AnalyzeImpact computes the impact analysis for a query and persists it
// as one immutable record under impact/. The analysis is committed with a
// single atomic rename; it never modifies the restore point or any object.
func (s *Store) AnalyzeImpact(query ImpactQuery) (ImpactAnalysis, error) {
	an, err := s.computeImpact(query)
	if err != nil {
		return ImpactAnalysis{}, err
	}
	id, err := newVersionID()
	if err != nil {
		return ImpactAnalysis{}, err
	}
	an.ID = "ia-" + id[1:]
	b, err := json.MarshalIndent(&an, "", "  ")
	if err != nil {
		return ImpactAnalysis{}, err
	}
	if err := writeFileAtomic(s.impactDir(), filepath.Base(s.impactPath(an.ID)), append(b, '\n'), 0o644); err != nil {
		return ImpactAnalysis{}, err
	}
	return an, nil
}

// GetImpactAnalysis loads a persisted impact analysis by id. The record is
// immutable, so the returned analysis is exactly the one that was computed,
// regardless of restore points created afterwards.
func (s *Store) GetImpactAnalysis(id string) (ImpactAnalysis, error) {
	var an ImpactAnalysis
	b, err := os.ReadFile(s.impactPath(id))
	if errors.Is(err, os.ErrNotExist) {
		return an, fmt.Errorf("%w: impact analysis %q", ErrNotFound, id)
	}
	if err != nil {
		return an, err
	}
	if err := json.Unmarshal(b, &an); err != nil {
		return an, fmt.Errorf("%w: impact analysis %q unreadable: %v", ErrCorrupted, id, err)
	}
	return an, nil
}

// ListImpactAnalyses returns every persisted impact analysis, sorted by id.
func (s *Store) ListImpactAnalyses() ([]ImpactAnalysis, error) {
	entries, err := os.ReadDir(s.impactDir())
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var out []ImpactAnalysis
	for _, e := range entries {
		if e.IsDir() || filepath.Ext(e.Name()) != ".json" {
			continue
		}
		b, err := os.ReadFile(filepath.Join(s.impactDir(), e.Name()))
		if err != nil {
			return nil, err
		}
		var an ImpactAnalysis
		if json.Unmarshal(b, &an) != nil || an.ID == "" {
			continue
		}
		out = append(out, an)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

// CompareImpact contrasts the affected scopes of two restore points, each
// limited by its own business entries. The comparison is derived on the
// fly from the two point files and member content; it writes nothing.
func (s *Store) CompareImpact(queryA, queryB ImpactQuery) (ImpactComparison, error) {
	a, err := s.computeImpact(queryA)
	if err != nil {
		return ImpactComparison{}, err
	}
	b, err := s.computeImpact(queryB)
	if err != nil {
		return ImpactComparison{}, err
	}
	cmp := ImpactComparison{
		A: ImpactSide{Query: queryA, Analysis: a},
		B: ImpactSide{Query: queryB, Analysis: b},
	}
	membersA := make(map[string]bool)
	for _, o := range a.Objects {
		membersA[o.Key] = true
	}
	membersB := make(map[string]bool)
	for _, o := range b.Objects {
		membersB[o.Key] = true
	}
	// Missing dependencies are judged against the other point's full
	// member list, not only the other scope: a dependency the other point
	// simply does not contain can never be recovered from it.
	rpA, err := s.GetRestorePoint(queryA.Point)
	if err != nil {
		return ImpactComparison{}, err
	}
	rpB, err := s.GetRestorePoint(queryB.Point)
	if err != nil {
		return ImpactComparison{}, err
	}
	pointMembersA := make(map[string]bool)
	for _, m := range rpA.Members {
		pointMembersA[m.Key] = true
	}
	pointMembersB := make(map[string]bool)
	for _, m := range rpB.Members {
		pointMembersB[m.Key] = true
	}
	byKeyA := make(map[string]ImpactObject)
	for _, o := range a.Objects {
		byKeyA[o.Key] = o
		if !pointMembersB[o.Key] {
			cmp.A.MissingDependencies = append(cmp.A.MissingDependencies, o.Key)
		}
	}
	byKeyB := make(map[string]ImpactObject)
	for _, o := range b.Objects {
		byKeyB[o.Key] = o
		if !pointMembersA[o.Key] {
			cmp.B.MissingDependencies = append(cmp.B.MissingDependencies, o.Key)
		}
	}
	for k := range membersA {
		if !membersB[k] {
			continue
		}
		oa, ob := byKeyA[k], byKeyB[k]
		cmp.Common = append(cmp.Common, ImpactCommonObject{
			Key:         k,
			VersionA:    oa.VersionID,
			VersionB:    ob.VersionID,
			ContentA:    oa.Content,
			ContentB:    ob.Content,
			Recoverable: oa.Content == "readable" && ob.Content == "readable",
		})
	}
	sort.Strings(cmp.A.MissingDependencies)
	sort.Strings(cmp.B.MissingDependencies)
	sort.Slice(cmp.Common, func(i, j int) bool { return cmp.Common[i].Key < cmp.Common[j].Key })
	return cmp, nil
}

// ExportImpactScope builds the export manifest for a selected subset of a
// persisted impact analysis. Every recorded reference from an included
// object to an object outside the selection is listed explicitly in
// ExcludedCrossReferences. The export is computed from the stored analysis
// alone; it modifies nothing.
func (s *Store) ExportImpactScope(analysisID string, keys []string) (ImpactExport, error) {
	if len(keys) == 0 {
		return ImpactExport{}, errors.New("localobjstore: export needs at least one object")
	}
	an, err := s.GetImpactAnalysis(analysisID)
	if err != nil {
		return ImpactExport{}, err
	}
	byKey := make(map[string]ImpactObject, len(an.Objects))
	for _, o := range an.Objects {
		byKey[o.Key] = o
	}
	included := make(map[string]bool)
	exp := ImpactExport{AnalysisID: an.ID, PointName: an.PointName}
	seen := make(map[string]bool)
	for _, k := range keys {
		if seen[k] {
			continue
		}
		seen[k] = true
		o, ok := byKey[k]
		if !ok {
			return ImpactExport{}, fmt.Errorf("%w: %q is not in the scope of impact analysis %q", ErrNotFound, k, analysisID)
		}
		included[k] = true
		exp.Included = append(exp.Included, RestoreMember{Key: o.Key, VersionID: o.VersionID})
	}
	sort.Slice(exp.Included, func(i, j int) bool { return exp.Included[i].Key < exp.Included[j].Key })
	for _, e := range an.Edges {
		if included[e.From] && !included[e.To] {
			exp.ExcludedCrossReferences = append(exp.ExcludedCrossReferences, ImpactCrossRef{From: e.From, To: e.To})
		}
	}
	return exp, nil
}
