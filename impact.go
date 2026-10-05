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

// ImpactQuery names one business entry point into a restore point: the
// reference graph the point recorded is traversed from every key in
// Entries. Every entry must be a member of the point.
type ImpactQuery struct {
	Point   string   `json:"point"`
	Entries []string `json:"entries"`
}

// ImpactNode is one object inside the impacted scope of one restore
// point. Direct marks objects that changed themselves (captured version
// added, removed or replaced between the two compared points); every
// other reached object is only affected through its dependencies. Paths
// lists every relationship path from an entry to this object exactly as
// the point recorded it; a path whose last key repeats an earlier key
// closes a reference cycle. Cycles are preserved in the paths but never
// traversed twice, so every object appears in the scope exactly once.
// Status reports content availability (readable, corrupted, missing)
// independently of the structural impact: scope and paths come from the
// recorded edges alone, so an unreadable member keeps its place in the
// graph.
type ImpactNode struct {
	Key       string     `json:"key"`
	VersionID string     `json:"version_id"`
	Entry     bool       `json:"entry,omitempty"`
	Direct    bool       `json:"direct"`
	Paths     [][]string `json:"paths"`
	Status    string     `json:"status"`
	StatusErr string     `json:"status_err,omitempty"`
}

// ImpactSide is the impacted scope of one restore point as seen from
// its business entries. Point is embedded verbatim (as persisted) so
// the analysis stays checkable against what the point actually saved
// and never changes when new restore points are created later.
type ImpactSide struct {
	Query ImpactQuery  `json:"query"`
	Point RestorePoint `json:"point"`
	Scope []ImpactNode `json:"scope"`
}

// CommonImpact is one object reachable from the business entries of
// both compared restore points. Recoverable is true only when the
// captured version is readable on both sides.
type CommonImpact struct {
	Key         string `json:"key"`
	VersionA    string `json:"version_a"`
	VersionB    string `json:"version_b"`
	StatusA     string `json:"status_a"`
	StatusB     string `json:"status_b"`
	Recoverable bool   `json:"recoverable"`
}

// ImpactAnalysis is a durable, immutable, read-only impact report over
// one or two restore points, each restricted to its own business
// entries. It is computed once, persisted atomically and never
// recomputed: reopening the store shows the same analysis, and restore
// points created afterwards cannot change it. Computing it reads only
// the point files and member content for verification; it never
// modifies a restore point and never restores an object.
type ImpactAnalysis struct {
	Name      string         `json:"name"`
	ID        string         `json:"id"`
	CreatedAt time.Time      `json:"created_at"`
	Sides     []ImpactSide   `json:"sides"`
	Common    []CommonImpact `json:"common,omitempty"`
	// MissingA lists dependencies reachable from side A's entries that
	// side B's scope does not contain at all (and vice versa): the
	// dependencies each side would lack on its own.
	MissingA []string `json:"missing_a,omitempty"`
	MissingB []string `json:"missing_b,omitempty"`
}

// CrossBoundaryReference is a recorded reference from an exported
// object to an object that is not part of the exported selection.
type CrossBoundaryReference struct {
	From string `json:"from"`
	To   string `json:"to"`
}

// ImpactExport is the result of exporting a selected scope out of a
// saved impact analysis: the selected members with their recorded
// versions, plus every recorded reference that crosses the selection
// boundary and is therefore explicitly not included.
type ImpactExport struct {
	Analysis                string                   `json:"analysis"`
	Point                   string                   `json:"point"`
	Members                 []RestoreMember          `json:"members"`
	CrossBoundaryReferences []CrossBoundaryReference `json:"cross_boundary_references,omitempty"`
}

func (s *Store) analysesDir() string { return filepath.Join(s.root, "analyses") }

func (s *Store) analysisPath(name string) string {
	return filepath.Join(s.analysesDir(), objectID(name)+".json")
}

func newAnalysisID() string {
	id, err := newVersionID()
	if err != nil {
		return fmt.Sprintf("ia-%d", time.Now().UTC().UnixNano())
	}
	return "ia-" + id[1:]
}

// CreateImpactAnalysis computes an object-entry impact analysis over
// one or two restore points and persists it under name. Each query
// restricts one point to its own business entries; with two queries the
// report also classifies every reached object as directly changed or
// only dependency-affected and lists the common recoverable scope plus
// the dependencies each side is missing. The traversal follows only the
// directed references the points actually saved; cycles are preserved
// in the recorded paths but never cause re-traversal or double
// counting. The analysis is committed with a single atomic rename and
// is immutable afterwards. Restore points and objects are only read,
// never modified.
func (s *Store) CreateImpactAnalysis(name string, queries ...ImpactQuery) (ImpactAnalysis, error) {
	if name == "" {
		return ImpactAnalysis{}, errors.New("localobjstore: empty impact analysis name")
	}
	if len(queries) == 0 || len(queries) > 2 {
		return ImpactAnalysis{}, errors.New("localobjstore: impact analysis needs one or two queries")
	}
	if _, err := os.Stat(s.analysisPath(name)); err == nil {
		return ImpactAnalysis{}, fmt.Errorf("%w: %s", ErrImpactAnalysisExists, name)
	} else if !errors.Is(err, os.ErrNotExist) {
		return ImpactAnalysis{}, err
	}

	an := ImpactAnalysis{Name: name, ID: newAnalysisID(), CreatedAt: time.Now().UTC()}
	points := make([]RestorePoint, 0, len(queries))
	for _, q := range queries {
		rp, err := s.GetRestorePoint(q.Point)
		if err != nil {
			return ImpactAnalysis{}, err
		}
		points = append(points, rp)
		side, err := s.impactSide(q, rp)
		if err != nil {
			return ImpactAnalysis{}, err
		}
		an.Sides = append(an.Sides, side)
	}

	if len(an.Sides) == 2 {
		changed := changedMemberKeys(points[0], points[1])
		for si := range an.Sides {
			for ni := range an.Sides[si].Scope {
				if changed[an.Sides[si].Scope[ni].Key] {
					an.Sides[si].Scope[ni].Direct = true
				}
			}
		}
		scopeA := nodeSet(an.Sides[0].Scope)
		scopeB := nodeSet(an.Sides[1].Scope)
		for key, na := range scopeA {
			nb, ok := scopeB[key]
			if !ok {
				an.MissingB = append(an.MissingB, key)
				continue
			}
			an.Common = append(an.Common, CommonImpact{
				Key: key, VersionA: na.VersionID, VersionB: nb.VersionID,
				StatusA: na.Status, StatusB: nb.Status,
				Recoverable: na.Status == "readable" && nb.Status == "readable",
			})
		}
		for key := range scopeB {
			if _, ok := scopeA[key]; !ok {
				an.MissingA = append(an.MissingA, key)
			}
		}
		sort.Slice(an.Common, func(i, j int) bool { return an.Common[i].Key < an.Common[j].Key })
		sort.Strings(an.MissingA)
		sort.Strings(an.MissingB)
	} else {
		// A single-point query has nothing to diff against: the entry
		// objects themselves are the directly considered objects.
		for ni := range an.Sides[0].Scope {
			if an.Sides[0].Scope[ni].Entry {
				an.Sides[0].Scope[ni].Direct = true
			}
		}
	}

	b, err := json.MarshalIndent(&an, "", "  ")
	if err != nil {
		return ImpactAnalysis{}, err
	}
	if err := writeFileAtomic(s.analysesDir(), filepath.Base(s.analysisPath(name)), append(b, '\n'), 0o644); err != nil {
		return ImpactAnalysis{}, err
	}
	return an, nil
}

// impactSide computes the impacted scope of one restore point from its
// business entries. The traversal is a simple-path enumeration over the
// recorded directed edges: a node already on the current path closes a
// cycle (recorded, not followed), so traversal always terminates and
// every object enters the scope exactly once. Content availability is
// verified separately and only fills Status; it never changes the
// structure derived from the recorded edges.
func (s *Store) impactSide(q ImpactQuery, rp RestorePoint) (ImpactSide, error) {
	if len(q.Entries) == 0 {
		return ImpactSide{}, fmt.Errorf("localobjstore: impact query for %q needs at least one entry", q.Point)
	}
	members := make(map[string]RestoreMember, len(rp.Members))
	adj := make(map[string][]string, len(rp.Members))
	for _, m := range rp.Members {
		members[m.Key] = m
		adj[m.Key] = append([]string(nil), m.References...)
		sort.Strings(adj[m.Key])
	}
	seenEntry := make(map[string]bool, len(q.Entries))
	for _, e := range q.Entries {
		if e == "" {
			return ImpactSide{}, errors.New("localobjstore: empty impact entry key")
		}
		if seenEntry[e] {
			return ImpactSide{}, fmt.Errorf("localobjstore: duplicate impact entry %q", e)
		}
		seenEntry[e] = true
		if _, ok := members[e]; !ok {
			return ImpactSide{}, fmt.Errorf("%w: impact entry %q is not a member of restore point %q", ErrNotFound, e, q.Point)
		}
	}

	paths := make(map[string][][]string)
	var walk func(path []string)
	walk = func(path []string) {
		cur := path[len(path)-1]
		recordPath(paths, cur, path)
		for _, next := range adj[cur] {
			if onPath(path, next) {
				// The edge closes a reference cycle: keep the full
				// cycle in the recorded paths but do not follow it.
				recordPath(paths, next, append(append([]string(nil), path...), next))
				continue
			}
			walk(append(path, next))
		}
	}
	for _, e := range q.Entries {
		walk([]string{e})
	}

	side := ImpactSide{Query: q, Point: rp}
	keys := make([]string, 0, len(paths))
	for k := range paths {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		m := members[k]
		node := ImpactNode{
			Key:       k,
			VersionID: m.VersionID,
			Entry:     seenEntry[k],
			Paths:     paths[k],
		}
		node.Status, node.StatusErr = s.memberAvailability(k, m.VersionID)
		side.Scope = append(side.Scope, node)
	}
	return side, nil
}

// memberAvailability classifies one recorded member version as readable,
// corrupted or missing without changing anything.
func (s *Store) memberAvailability(key, versionID string) (string, string) {
	vdir := filepath.Join(s.versionsDir(key), versionID)
	if _, err := os.Stat(vdir); errors.Is(err, os.ErrNotExist) {
		return "missing", fmt.Sprintf("%v: version %s of %s", ErrNotFound, versionID, key)
	}
	if err := s.validateVersionStructure(key, versionID); err != nil {
		return "corrupted", err.Error()
	}
	mf, err := s.loadManifest(key, versionID)
	if err != nil {
		return "corrupted", err.Error()
	}
	if err := verifyDirBytes(vdir, mf); err != nil {
		return "corrupted", err.Error()
	}
	return "readable", ""
}

// recordPath appends path to the paths recorded for key, ignoring exact
// duplicates.
func recordPath(paths map[string][][]string, key string, path []string) {
	cp := append([]string(nil), path...)
	for _, p := range paths[key] {
		if len(p) != len(cp) {
			continue
		}
		eq := true
		for i := range p {
			if p[i] != cp[i] {
				eq = false
				break
			}
		}
		if eq {
			return
		}
	}
	paths[key] = append(paths[key], cp)
}

func onPath(path []string, key string) bool {
	for _, k := range path {
		if k == key {
			return true
		}
	}
	return false
}

func nodeSet(scope []ImpactNode) map[string]ImpactNode {
	m := make(map[string]ImpactNode, len(scope))
	for _, n := range scope {
		m[n.Key] = n
	}
	return m
}

// changedMemberKeys returns the keys whose captured version was added,
// removed or replaced between restore points a and b.
func changedMemberKeys(a, b RestorePoint) map[string]bool {
	am := make(map[string]string, len(a.Members))
	for _, m := range a.Members {
		am[m.Key] = m.VersionID
	}
	bm := make(map[string]string, len(b.Members))
	for _, m := range b.Members {
		bm[m.Key] = m.VersionID
	}
	changed := make(map[string]bool)
	for k, av := range am {
		if bv, ok := bm[k]; !ok || bv != av {
			changed[k] = true
		}
	}
	for k := range bm {
		if _, ok := am[k]; !ok {
			changed[k] = true
		}
	}
	return changed
}

// GetImpactAnalysis loads a saved impact analysis exactly as it was
// persisted. The stored snapshot is returned verbatim: restore points
// created after the analysis never change it.
func (s *Store) GetImpactAnalysis(name string) (ImpactAnalysis, error) {
	var an ImpactAnalysis
	b, err := os.ReadFile(s.analysisPath(name))
	if errors.Is(err, os.ErrNotExist) {
		return an, fmt.Errorf("%w: impact analysis %q", ErrNotFound, name)
	}
	if err != nil {
		return an, err
	}
	if err := json.Unmarshal(b, &an); err != nil {
		return an, fmt.Errorf("%w: impact analysis %q unreadable: %v", ErrCorrupted, name, err)
	}
	return an, nil
}

// ListImpactAnalyses returns every saved impact analysis, sorted by name.
func (s *Store) ListImpactAnalyses() ([]ImpactAnalysis, error) {
	entries, err := os.ReadDir(s.analysesDir())
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
		b, err := os.ReadFile(filepath.Join(s.analysesDir(), e.Name()))
		if err != nil {
			return nil, err
		}
		var an ImpactAnalysis
		if json.Unmarshal(b, &an) != nil || an.Name == "" {
			continue
		}
		out = append(out, an)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// ExportImpactScope exports a selected scope out of a saved impact
// analysis: the recorded member (key, version, references) of every
// selected key on the side of the analysis for point, plus every
// recorded reference that leaves the selection. Cross-boundary
// references are reported explicitly because the export does not
// include their targets. Every selected key must belong to that side's
// impacted scope. The analysis, the restore points and the objects are
// only read, never modified.
func (s *Store) ExportImpactScope(analysis, point string, keys []string) (ImpactExport, error) {
	an, err := s.GetImpactAnalysis(analysis)
	if err != nil {
		return ImpactExport{}, err
	}
	if len(keys) == 0 {
		return ImpactExport{}, errors.New("localobjstore: export needs at least one key")
	}
	var side *ImpactSide
	for i := range an.Sides {
		if an.Sides[i].Query.Point == point {
			if side != nil {
				return ImpactExport{}, fmt.Errorf("localobjstore: impact analysis %q has two sides on point %q; selection is ambiguous", analysis, point)
			}
			side = &an.Sides[i]
		}
	}
	if side == nil {
		return ImpactExport{}, fmt.Errorf("%w: impact analysis %q has no side on restore point %q", ErrNotFound, analysis, point)
	}
	inScope := nodeSet(side.Scope)
	selected := make(map[string]bool, len(keys))
	for _, k := range keys {
		if k == "" {
			return ImpactExport{}, errors.New("localobjstore: empty export key")
		}
		if selected[k] {
			return ImpactExport{}, fmt.Errorf("localobjstore: duplicate export key %q", k)
		}
		if _, ok := inScope[k]; !ok {
			return ImpactExport{}, fmt.Errorf("%w: key %q is not in the impacted scope of %q in analysis %q", ErrNotFound, k, point, analysis)
		}
		selected[k] = true
	}
	exp := ImpactExport{Analysis: analysis, Point: point}
	for _, m := range side.Point.Members {
		if selected[m.Key] {
			exp.Members = append(exp.Members, m)
			continue
		}
	}
	for _, m := range side.Point.Members {
		if !selected[m.Key] {
			continue
		}
		for _, ref := range m.References {
			if !selected[ref] {
				exp.CrossBoundaryReferences = append(exp.CrossBoundaryReferences, CrossBoundaryReference{From: m.Key, To: ref})
			}
		}
	}
	sort.Slice(exp.CrossBoundaryReferences, func(i, j int) bool {
		x, y := exp.CrossBoundaryReferences[i], exp.CrossBoundaryReferences[j]
		if x.From != y.From {
			return x.From < y.From
		}
		return x.To < y.To
	})
	return exp, nil
}
