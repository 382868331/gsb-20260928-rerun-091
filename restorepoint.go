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

// RestoreMemberInput declares one member of a group restore point: the
// object key plus the keys of other members it references. References are
// recorded (not resolved) so the group captures the mutual-reference
// topology of one business commit.
type RestoreMemberInput struct {
	Key        string   `json:"key"`
	References []string `json:"references,omitempty"`
}

// RestoreMember is one confirmed member of a restore point: the exact
// immutable version captured for the key at creation time.
type RestoreMember struct {
	Key        string   `json:"key"`
	VersionID  string   `json:"version_id"`
	References []string `json:"references,omitempty"`
}

// RestorePoint is a durable, named, immutable snapshot of a group of
// mutually-referencing objects taken at one business commit. Once confirmed
// it never changes: later publishes create new versions that are not
// members, so in-flight or future writes can never silently join a
// confirmed restore point.
type RestorePoint struct {
	ID        string          `json:"id"`
	Name      string          `json:"name"`
	CreatedAt time.Time       `json:"created_at"`
	Members   []RestoreMember `json:"members"`
}

// MemberChange describes one difference between two restore points.
type MemberChange struct {
	Key         string
	Change      string // "added" | "removed" | "changed"
	FromVersion string // set for "changed" and "removed"
	ToVersion   string // set for "changed" and "added"
}

// ReferenceChange describes one difference in the recorded reference edges
// between two restore points. An edge is a declared, directed reference
// from one member key to another. Declarations are treated as a set, so a
// different declaration order for the same topology produces no changes.
//
// Change is one of:
//
//   - "added": the edge From -> To exists only in the second point;
//   - "removed": the edge From -> To exists only in the first point;
//   - "reversed": the first point records From -> To and the second
//     records the opposite direction, To -> From, instead.
type ReferenceChange struct {
	From   string
	To     string
	Change string // "added" | "removed" | "reversed"
}

// RestorePointDiff is the full, read-only comparison of two confirmed
// restore points. Both points are included verbatim (as persisted, member
// declaration order preserved) so every reported difference can be checked
// against what each point actually saved. MemberChanges covers member
// presence and captured versions; ReferenceChanges covers the directed
// reference topology. The comparison never touches member objects and never
// modifies either restore point, so missing or corrupted member versions
// do not erase the recorded reference facts.
type RestorePointDiff struct {
	PointA           RestorePoint
	PointB           RestorePoint
	MemberChanges    []MemberChange
	ReferenceChanges []ReferenceChange
}

// MemberHealth is the availability check of one restore point member.
type MemberHealth struct {
	Key       string
	VersionID string
	Status    string // "readable" | "corrupted" | "missing"
	Err       error
}

// RestorePlan previews a group restore without changing anything: every
// member is classified as readable, corrupted or missing so the operator
// can choose a full restore or explicitly accept a partial one with gaps.
type RestorePlan struct {
	Point    RestorePoint
	Members  []MemberHealth
	Complete bool // every member is readable
}

// RetainedMember explains why one historical version is still occupied
// after its restore point was deleted.
type RetainedMember struct {
	Key       string
	VersionID string
	Reasons   []string
}

// DeleteRestorePointReport lists, per member version, whether it is still
// retained (and by whom) or released for the next garbage-collection pass.
type DeleteRestorePointReport struct {
	Name     string
	Retained []RetainedMember
	Released []RestoreMember
}

func (s *Store) restorePointsDir() string { return filepath.Join(s.root, "restorepoints") }

func (s *Store) restorePointPath(name string) string {
	return filepath.Join(s.restorePointsDir(), objectID(name)+".json")
}

// checkMemberContent fully verifies one member version (structure +
// checksums) and classifies it as "readable", "corrupted" or "missing".
// It only reads; nothing is modified.
func (s *Store) checkMemberContent(key, versionID string) (string, error) {
	vdir := filepath.Join(s.versionsDir(key), versionID)
	if _, err := os.Stat(vdir); errors.Is(err, os.ErrNotExist) {
		return "missing", fmt.Errorf("%w: version %s of %s", ErrNotFound, versionID, key)
	}
	if err := s.validateVersionStructure(key, versionID); err != nil {
		return "corrupted", err
	}
	mf, err := s.loadManifest(key, versionID)
	if err != nil {
		return "corrupted", err
	}
	if err := verifyDirBytes(vdir, mf); err != nil {
		return "corrupted", err
	}
	return "readable", nil
}

// CreateRestorePoint captures the current version of every member key as
// one named, immutable group snapshot. The capture serialises with
// publishes (per-key locks, in deterministic order), so the group always
// reflects a consistent moment; a key with an in-progress upload in this
// process is rejected with ErrRestoreInFlight rather than silently
// snapshotting its pre-upload version. The point file is committed with a
// single atomic rename: a crash mid-create leaves only a temp file that
// recovery discards, never a half-written restore point.
func (s *Store) CreateRestorePoint(name string, members []RestoreMemberInput) (RestorePoint, error) {
	if name == "" {
		return RestorePoint{}, errors.New("localobjstore: empty restore point name")
	}
	if len(members) == 0 {
		return RestorePoint{}, errors.New("localobjstore: restore point needs at least one member")
	}
	if _, err := os.Stat(s.restorePointPath(name)); err == nil {
		return RestorePoint{}, fmt.Errorf("%w: %s", ErrRestorePointExists, name)
	} else if !errors.Is(err, os.ErrNotExist) {
		return RestorePoint{}, err
	}

	seen := make(map[string]bool, len(members))
	keys := make([]string, 0, len(members))
	for _, m := range members {
		if m.Key == "" {
			return RestorePoint{}, errors.New("localobjstore: empty member key")
		}
		if seen[m.Key] {
			return RestorePoint{}, fmt.Errorf("localobjstore: duplicate member key %q", m.Key)
		}
		seen[m.Key] = true
		keys = append(keys, m.Key)
	}
	for _, m := range members {
		for _, ref := range m.References {
			if !seen[ref] {
				return RestorePoint{}, fmt.Errorf("localobjstore: member %q references %q outside the group", m.Key, ref)
			}
		}
	}

	// Refuse to silently capture a key whose new content is still being
	// written: the caller must finish or abort the upload first.
	s.upMu.Lock()
	for _, u := range s.uploads {
		if seen[u.Key] {
			s.upMu.Unlock()
			return RestorePoint{}, fmt.Errorf("%w: upload %s still writing %s", ErrRestoreInFlight, u.ID, u.Key)
		}
	}
	s.upMu.Unlock()

	// Serialise against publishers (in-process and cross-process) so no
	// publish can interleave with the group capture.
	s.pubMu.Lock()
	defer s.pubMu.Unlock()
	sorted := append([]string(nil), keys...)
	sort.Strings(sorted)
	var locks []*flockFile
	for _, key := range sorted {
		lock, err := acquireFlock(filepath.Join(s.root, "locks", "publish-"+objectID(key)+".lock"))
		if err != nil {
			for _, l := range locks {
				l.release()
			}
			return RestorePoint{}, err
		}
		locks = append(locks, lock)
	}
	defer func() {
		for _, l := range locks {
			l.release()
		}
	}()

	rp := RestorePoint{
		ID:        newRestorePointID(),
		Name:      name,
		CreatedAt: time.Now().UTC(),
	}
	for _, m := range members {
		cur, err := s.readCurrent(m.Key)
		if err != nil {
			return RestorePoint{}, err
		}
		if cur == "" {
			if _, ok, ferr := s.Fault(m.Key); ferr != nil {
				return RestorePoint{}, ferr
			} else if ok {
				return RestorePoint{}, fmt.Errorf("%w: member %s is faulted", ErrFaulted, m.Key)
			}
			return RestorePoint{}, fmt.Errorf("%w: member %s has no current version", ErrNotFound, m.Key)
		}
		if err := s.validateVersionStructure(m.Key, cur); err != nil {
			return RestorePoint{}, fmt.Errorf("localobjstore: member %s: %w", m.Key, err)
		}
		rp.Members = append(rp.Members, RestoreMember{
			Key:        m.Key,
			VersionID:  cur,
			References: append([]string(nil), m.References...),
		})
	}
	b, err := json.MarshalIndent(&rp, "", "  ")
	if err != nil {
		return RestorePoint{}, err
	}
	if err := writeFileAtomic(s.restorePointsDir(), filepath.Base(s.restorePointPath(name)), append(b, '\n'), 0o644); err != nil {
		return RestorePoint{}, err
	}
	return rp, nil
}

func newRestorePointID() string {
	id, err := newVersionID()
	if err != nil {
		return fmt.Sprintf("rp-%d", time.Now().UTC().UnixNano())
	}
	return "rp-" + id[1:]
}

// GetRestorePoint loads a confirmed restore point by name.
func (s *Store) GetRestorePoint(name string) (RestorePoint, error) {
	var rp RestorePoint
	b, err := os.ReadFile(s.restorePointPath(name))
	if errors.Is(err, os.ErrNotExist) {
		return rp, fmt.Errorf("%w: restore point %q", ErrNotFound, name)
	}
	if err != nil {
		return rp, err
	}
	if err := json.Unmarshal(b, &rp); err != nil {
		return rp, fmt.Errorf("%w: restore point %q unreadable: %v", ErrCorrupted, name, err)
	}
	return rp, nil
}

// ListRestorePoints returns every confirmed restore point, sorted by name.
func (s *Store) ListRestorePoints() ([]RestorePoint, error) {
	entries, err := os.ReadDir(s.restorePointsDir())
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var out []RestorePoint
	for _, e := range entries {
		if e.IsDir() || filepath.Ext(e.Name()) != ".json" {
			continue
		}
		b, err := os.ReadFile(filepath.Join(s.restorePointsDir(), e.Name()))
		if err != nil {
			return nil, err
		}
		var rp RestorePoint
		if json.Unmarshal(b, &rp) != nil || rp.Name == "" {
			continue
		}
		out = append(out, rp)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// refEdge is one directed reference from one member key to another.
type refEdge struct{ from, to string }

// referenceEdgeSet returns the set of directed reference edges a restore
// point recorded. References are recorded facts copied straight from the
// point file: they are neither resolved nor probed against the object tree,
// so a member whose version is currently missing or corrupted still keeps
// every edge the point saved. Duplicate declarations and declaration order
// are irrelevant because the topology is a set.
func referenceEdgeSet(rp RestorePoint) map[refEdge]bool {
	edges := make(map[refEdge]bool)
	for _, m := range rp.Members {
		for _, ref := range m.References {
			edges[refEdge{from: m.Key, to: ref}] = true
		}
	}
	return edges
}

// DiffRestorePoints compares two restore points without modifying either.
// It reports member presence and captured-version changes together with
// the changes in directed reference topology: edges added in the second
// point, edges removed from it, and edges whose direction reversed
// (A -> B in the first point becomes B -> A in the second). Reference
// declarations are compared as sets, so the same topology declared in a
// different order produces no reference changes. Only the two point files
// are read; member objects are never opened or verified, which keeps the
// comparison stable after a restore and preserves recorded edges even when
// member versions are missing or corrupted.
func (s *Store) DiffRestorePoints(aName, bName string) (RestorePointDiff, error) {
	a, err := s.GetRestorePoint(aName)
	if err != nil {
		return RestorePointDiff{}, err
	}
	b, err := s.GetRestorePoint(bName)
	if err != nil {
		return RestorePointDiff{}, err
	}
	diff := RestorePointDiff{PointA: a, PointB: b}

	am := make(map[string]string, len(a.Members))
	for _, m := range a.Members {
		am[m.Key] = m.VersionID
	}
	bm := make(map[string]string, len(b.Members))
	for _, m := range b.Members {
		bm[m.Key] = m.VersionID
	}
	for key, av := range am {
		bv, ok := bm[key]
		switch {
		case !ok:
			diff.MemberChanges = append(diff.MemberChanges, MemberChange{Key: key, Change: "removed", FromVersion: av})
		case bv != av:
			diff.MemberChanges = append(diff.MemberChanges, MemberChange{Key: key, Change: "changed", FromVersion: av, ToVersion: bv})
		}
	}
	for key, bv := range bm {
		if _, ok := am[key]; !ok {
			diff.MemberChanges = append(diff.MemberChanges, MemberChange{Key: key, Change: "added", ToVersion: bv})
		}
	}
	sort.Slice(diff.MemberChanges, func(i, j int) bool { return diff.MemberChanges[i].Key < diff.MemberChanges[j].Key })

	aEdges := referenceEdgeSet(a)
	bEdges := referenceEdgeSet(b)
	var onlyA []refEdge
	unmatchedB := make(map[refEdge]bool)
	for e := range aEdges {
		if !bEdges[e] {
			onlyA = append(onlyA, e)
		}
	}
	for e := range bEdges {
		if !aEdges[e] {
			unmatchedB[e] = true
		}
	}
	// Pair up opposite-direction edges first so a reversed reference is
	// reported as one "reversed" change instead of one removal plus one
	// addition.
	var refChanges []ReferenceChange
	for _, ae := range onlyA {
		rev := refEdge{from: ae.to, to: ae.from}
		if unmatchedB[rev] {
			refChanges = append(refChanges, ReferenceChange{From: ae.from, To: ae.to, Change: "reversed"})
			delete(unmatchedB, rev)
			continue
		}
		refChanges = append(refChanges, ReferenceChange{From: ae.from, To: ae.to, Change: "removed"})
	}
	for be := range unmatchedB {
		refChanges = append(refChanges, ReferenceChange{From: be.from, To: be.to, Change: "added"})
	}
	sort.Slice(refChanges, func(i, j int) bool {
		x, y := refChanges[i], refChanges[j]
		if x.From != y.From {
			return x.From < y.From
		}
		if x.To != y.To {
			return x.To < y.To
		}
		return x.Change < y.Change
	})
	diff.ReferenceChanges = refChanges
	return diff, nil
}

// InspectRestorePoint previews a group restore: every member version is
// fully verified (structure + checksums) and classified as readable,
// corrupted or missing. Nothing is modified.
func (s *Store) InspectRestorePoint(name string) (RestorePlan, error) {
	rp, err := s.GetRestorePoint(name)
	if err != nil {
		return RestorePlan{}, err
	}
	plan := RestorePlan{Point: rp, Complete: true}
	for _, m := range rp.Members {
		h := MemberHealth{Key: m.Key, VersionID: m.VersionID, Status: "readable"}
		h.Status, h.Err = s.checkMemberContent(m.Key, m.VersionID)
		if h.Status != "readable" {
			plan.Complete = false
		}
		plan.Members = append(plan.Members, h)
	}
	return plan, nil
}

// DeleteRestorePoint removes a confirmed restore point. The report lists,
// for every member version, whether it is still occupied — by the current
// pointer, a durable pin, another restore point, an open reader or a
// pending group restore — or released and reclaimable by the next
// CollectGarbage pass. Member versions themselves are never deleted here.
func (s *Store) DeleteRestorePoint(name string) (DeleteRestorePointReport, error) {
	rp, err := s.GetRestorePoint(name)
	if err != nil {
		return DeleteRestorePointReport{}, err
	}
	rep := DeleteRestorePointReport{Name: name}
	for _, m := range rp.Members {
		reasons, err := s.retentionReasons(m.Key, m.VersionID, name)
		if err != nil {
			return DeleteRestorePointReport{}, err
		}
		if len(reasons) > 0 {
			rep.Retained = append(rep.Retained, RetainedMember{Key: m.Key, VersionID: m.VersionID, Reasons: reasons})
		} else {
			rep.Released = append(rep.Released, m)
		}
	}
	if err := os.Remove(s.restorePointPath(name)); err != nil {
		return DeleteRestorePointReport{}, err
	}
	return rep, fsyncDir(s.restorePointsDir())
}

// retentionReasons lists why versionID of key is still referenced, ignoring
// the restore point named exclude ("" excludes nothing).
func (s *Store) retentionReasons(key, versionID, exclude string) ([]string, error) {
	var reasons []string
	if cur, err := s.readCurrent(key); err != nil {
		return nil, err
	} else if cur == versionID {
		reasons = append(reasons, "current version")
	}
	pins, err := s.ListPins()
	if err != nil {
		return nil, err
	}
	for _, p := range pins {
		if p.Key == key && p.VersionID == versionID {
			reasons = append(reasons, fmt.Sprintf("pin %q", p.Name))
		}
	}
	points, err := s.ListRestorePoints()
	if err != nil {
		return nil, err
	}
	for _, rp := range points {
		if rp.Name == exclude {
			continue
		}
		for _, m := range rp.Members {
			if m.Key == key && m.VersionID == versionID {
				reasons = append(reasons, fmt.Sprintf("restore point %q", rp.Name))
			}
		}
	}
	s.gcMu.Lock()
	reading := s.readers[readerRef{key: key, vid: versionID}] > 0
	s.gcMu.Unlock()
	if reading {
		reasons = append(reasons, "open reader")
	}
	pending, err := s.pendingRestoreRefs(key)
	if err != nil {
		return nil, err
	}
	if pending[versionID] {
		reasons = append(reasons, "pending group restore")
	}
	return reasons, nil
}

// restorePointsForKey returns the set of version ids of key referenced by
// any confirmed restore point; GC retains those versions.
func (s *Store) restorePointsForKey(key string) (map[string]bool, error) {
	points, err := s.ListRestorePoints()
	if err != nil {
		return nil, err
	}
	set := make(map[string]bool)
	for _, rp := range points {
		for _, m := range rp.Members {
			if m.Key == key {
				set[m.VersionID] = true
			}
		}
	}
	return set, nil
}
