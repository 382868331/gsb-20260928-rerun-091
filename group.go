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

// GroupMemberSpec names one object that belongs to a recovery point and the
// keys of other members it references. References are recorded so operators
// can inspect the mutual-reference graph that made the group one business
// commit; every referenced key must itself be a member of the group.
type GroupMemberSpec struct {
	Key  string
	Refs []string
}

// GroupMember is the immutable record of one object inside a confirmed
// recovery point: the exact version (and its whole-object checksum) that was
// current when the point was confirmed.
type GroupMember struct {
	Key       string   `json:"key"`
	VersionID string   `json:"version_id"`
	Sha256    string   `json:"sha256"`
	Refs      []string `json:"refs,omitempty"`
}

// RecoveryPoint is a durable, named, group-wide snapshot: the complete state
// of a set of mutually referencing objects at one business commit. Once
// confirmed it never changes; member versions are retained across deletes
// and garbage collection until the point is explicitly deleted.
type RecoveryPoint struct {
	Name      string        `json:"name"`
	ID        string        `json:"id"`
	CreatedAt time.Time     `json:"created_at"`
	Members   []GroupMember `json:"members"`
}

// GroupDiff describes how two recovery points differ.
type GroupDiff struct {
	Added   []GroupMember     // members only in the second point
	Removed []GroupMember     // members only in the first point
	Changed []GroupDiffChange // members present in both with different state
}

// GroupDiffChange records one member whose captured version or reference set
// changed between two recovery points.
type GroupDiffChange struct {
	Key         string
	FromVersion string
	ToVersion   string
	RefsAdded   []string
	RefsRemoved []string
}

// MemberHealth is the pre-restore condition of one recorded member.
type MemberHealth struct {
	Member GroupMember
	Status string // "ready" | "missing" | "corrupted"
	Err    error  // set when Status != "ready"
}

// GroupRestorePlan shows, before anything is changed, which members of a
// recovery point can be restored and which are missing or damaged.
type GroupRestorePlan struct {
	Point   RecoveryPoint
	Members []MemberHealth
}

// Complete reports whether every recorded member is restorable.
func (p GroupRestorePlan) Complete() bool {
	for _, m := range p.Members {
		if m.Status != "ready" {
			return false
		}
	}
	return true
}

// RestoreMemberRecord is the durable per-member line of a restore journal.
type RestoreMemberRecord struct {
	Key           string `json:"key"`
	SourceVersion string `json:"source_version"`
	NewVersion    string `json:"new_version,omitempty"`
	Status        string `json:"status"` // "pending" | "staged" | "committed" | "gap" | "incomplete"
	Reason        string `json:"reason,omitempty"`
}

// RestoreJournal is the durable record of one group restore operation. It is
// written before the first change and updated after every member step, so
// reopening the store after an interruption can tell confirmed results from
// operations that never finished. Completed journals are kept forever as the
// restore history of the group.
type RestoreJournal struct {
	ID         string                `json:"id"`
	GroupName  string                `json:"group_name"`
	GroupID    string                `json:"group_id"`
	Partial    bool                  `json:"partial"`
	StartedAt  time.Time             `json:"started_at"`
	FinishedAt time.Time             `json:"finished_at,omitempty"`
	Status     string                `json:"status"` // "in-progress" | "completed" | "completed-partial" | "interrupted"
	Members    []RestoreMemberRecord `json:"members"`
}

// GroupRestoreReport summarizes a finished RestoreGroup call.
type GroupRestoreReport struct {
	Journal RestoreJournal
	Plan    GroupRestorePlan
}

// RetainedVersion explains why one historical version is still occupied
// after its recovery point was deleted.
type RetainedVersion struct {
	Key       string
	VersionID string
	HeldBy    []string // "current" | "pin:<name>" | "recovery-point:<name>" | "open-reader"
}

// GroupDeleteReport is returned by DeleteRecoveryPoint: which member
// versions are now unreferenced (reclaimable by GC) and which are still
// occupied by other recovery points, pins, readers or the current pointer.
type GroupDeleteReport struct {
	Name     string
	Released []GroupMember
	Retained []RetainedVersion
}

func (s *Store) groupsDir() string   { return filepath.Join(s.root, "groups") }
func (s *Store) journalsDir() string { return filepath.Join(s.groupsDir(), "journals") }

func (s *Store) groupPath(name string) string {
	return filepath.Join(s.groupsDir(), objectID(name)+".json")
}

func (s *Store) journalPath(id string) string {
	return filepath.Join(s.journalsDir(), id+".json")
}

// CreateRecoveryPoint confirms a named group-wide snapshot: for every
// requested key it records the version that is current right now, together
// with the declared reference edges. Capture serialises with publishing
// (same process-wide and per-key locks the publish path uses), so a write
// that is still in flight either commits fully before the capture reads the
// pointer — and is captured as a whole — or commits after, and never leaks
// half-visible state into an already confirmed recovery point.
//
// The point is confirmed only when every member's current version has been
// recorded and the group file is durably written; a key that is missing,
// deleted or faulted fails the whole capture and confirms nothing.
func (s *Store) CreateRecoveryPoint(name string, specs []GroupMemberSpec) (RecoveryPoint, error) {
	if name == "" {
		return RecoveryPoint{}, errors.New("localobjstore: empty recovery point name")
	}
	if len(specs) == 0 {
		return RecoveryPoint{}, errors.New("localobjstore: recovery point needs at least one member")
	}
	seen := make(map[string]bool)
	for _, sp := range specs {
		if sp.Key == "" {
			return RecoveryPoint{}, errors.New("localobjstore: empty member key")
		}
		if seen[sp.Key] {
			return RecoveryPoint{}, fmt.Errorf("localobjstore: duplicate member %q", sp.Key)
		}
		seen[sp.Key] = true
	}
	for _, sp := range specs {
		for _, ref := range sp.Refs {
			if !seen[ref] {
				return RecoveryPoint{}, fmt.Errorf("localobjstore: member %q references %q which is not in the group", sp.Key, ref)
			}
		}
	}
	if _, err := os.Stat(s.groupPath(name)); err == nil {
		return RecoveryPoint{}, fmt.Errorf("%w: recovery point %q", ErrGroupExists, name)
	}

	s.pubMu.Lock()
	defer s.pubMu.Unlock()
	// Take every member's per-key publish lock in key order (deadlock-free)
	// so no publish can commit a pointer change while the group is captured.
	keys := make([]string, 0, len(specs))
	for _, sp := range specs {
		keys = append(keys, sp.Key)
	}
	sort.Strings(keys)
	var locks []*flockFile
	for _, k := range keys {
		l, err := acquireFlock(filepath.Join(s.root, "locks", "publish-"+objectID(k)+".lock"))
		if err != nil {
			for _, pl := range locks {
				pl.release()
			}
			return RecoveryPoint{}, err
		}
		locks = append(locks, l)
	}
	defer func() {
		for _, l := range locks {
			l.release()
		}
	}()

	rp := RecoveryPoint{Name: name, CreatedAt: time.Now().UTC()}
	if id, err := newUploadID(); err != nil {
		return RecoveryPoint{}, err
	} else {
		rp.ID = id
	}
	for _, sp := range specs {
		cur, err := s.readCurrent(sp.Key)
		if err != nil {
			return RecoveryPoint{}, err
		}
		if cur == "" {
			if _, ok, ferr := s.Fault(sp.Key); ferr != nil {
				return RecoveryPoint{}, ferr
			} else if ok {
				return RecoveryPoint{}, fmt.Errorf("%w: %s", ErrFaulted, sp.Key)
			}
			return RecoveryPoint{}, fmt.Errorf("%w: %s", ErrNotFound, sp.Key)
		}
		m, err := s.loadManifest(sp.Key, cur)
		if err != nil {
			return RecoveryPoint{}, err
		}
		if err := s.validateVersionStructure(sp.Key, cur); err != nil {
			return RecoveryPoint{}, err
		}
		member := GroupMember{Key: sp.Key, VersionID: cur, Sha256: m.Sha256}
		if len(sp.Refs) > 0 {
			member.Refs = append([]string(nil), sp.Refs...)
			sort.Strings(member.Refs)
		}
		rp.Members = append(rp.Members, member)
	}
	sort.Slice(rp.Members, func(i, j int) bool { return rp.Members[i].Key < rp.Members[j].Key })

	b, err := json.MarshalIndent(&rp, "", "  ")
	if err != nil {
		return RecoveryPoint{}, err
	}
	if err := writeFileAtomic(s.groupsDir(), filepath.Base(s.groupPath(name)), append(b, '\n'), 0o644); err != nil {
		return RecoveryPoint{}, err
	}
	return rp, nil
}

// loadGroup reads a confirmed recovery point by name.
func (s *Store) loadGroup(name string) (RecoveryPoint, error) {
	var rp RecoveryPoint
	b, err := os.ReadFile(s.groupPath(name))
	if errors.Is(err, os.ErrNotExist) {
		return rp, fmt.Errorf("%w: recovery point %q", ErrNotFound, name)
	}
	if err != nil {
		return rp, err
	}
	if err := json.Unmarshal(b, &rp); err != nil {
		return rp, err
	}
	return rp, nil
}

// ListRecoveryPoints returns every confirmed recovery point, sorted by name.
func (s *Store) ListRecoveryPoints() ([]RecoveryPoint, error) {
	entries, err := os.ReadDir(s.groupsDir())
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var out []RecoveryPoint
	for _, e := range entries {
		if e.IsDir() || filepath.Ext(e.Name()) != ".json" {
			continue
		}
		b, err := os.ReadFile(filepath.Join(s.groupsDir(), e.Name()))
		if err != nil {
			return nil, err
		}
		var rp RecoveryPoint
		if json.Unmarshal(b, &rp) != nil || rp.Name == "" {
			continue
		}
		out = append(out, rp)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// DescribeRecoveryPoint returns the named point with every member's captured
// version and the recorded reference edges between members.
func (s *Store) DescribeRecoveryPoint(name string) (RecoveryPoint, error) {
	return s.loadGroup(name)
}

// DiffRecoveryPoints compares two confirmed points: members added or removed
// and members whose captured version or reference set changed.
func (s *Store) DiffRecoveryPoints(a, b string) (GroupDiff, error) {
	pa, err := s.loadGroup(a)
	if err != nil {
		return GroupDiff{}, err
	}
	pb, err := s.loadGroup(b)
	if err != nil {
		return GroupDiff{}, err
	}
	ma := make(map[string]GroupMember)
	for _, m := range pa.Members {
		ma[m.Key] = m
	}
	mb := make(map[string]GroupMember)
	for _, m := range pb.Members {
		mb[m.Key] = m
	}
	var diff GroupDiff
	for _, m := range pb.Members {
		if _, ok := ma[m.Key]; !ok {
			diff.Added = append(diff.Added, m)
		}
	}
	for _, m := range pa.Members {
		om, ok := mb[m.Key]
		if !ok {
			diff.Removed = append(diff.Removed, m)
			continue
		}
		ch := GroupDiffChange{Key: m.Key, FromVersion: m.VersionID, ToVersion: om.VersionID}
		newRefs := make(map[string]bool)
		for _, r := range om.Refs {
			newRefs[r] = true
		}
		oldRefs := make(map[string]bool)
		for _, r := range m.Refs {
			oldRefs[r] = true
			if !newRefs[r] {
				ch.RefsRemoved = append(ch.RefsRemoved, r)
			}
		}
		for _, r := range om.Refs {
			if !oldRefs[r] {
				ch.RefsAdded = append(ch.RefsAdded, r)
			}
		}
		if m.VersionID != om.VersionID || len(ch.RefsAdded) > 0 || len(ch.RefsRemoved) > 0 {
			diff.Changed = append(diff.Changed, ch)
		}
	}
	return diff, nil
}

// checkMember verifies one recorded member against the live tree: the exact
// recorded version must exist, be structurally valid and hash to the
// recorded whole-object checksum. Another version of the same key is never
// substituted — a group is restored from its own members only.
func (s *Store) checkMember(m GroupMember) MemberHealth {
	h := MemberHealth{Member: m, Status: "ready"}
	mf, err := s.loadManifest(m.Key, m.VersionID)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			h.Status = "missing"
		} else {
			h.Status = "corrupted"
		}
		h.Err = err
		return h
	}
	if mf.Sha256 != m.Sha256 {
		h.Status = "corrupted"
		h.Err = fmt.Errorf("%w: recorded checksum does not match manifest", ErrCorrupted)
		return h
	}
	if err := s.validateVersionStructure(m.Key, m.VersionID); err != nil {
		h.Status = "corrupted"
		h.Err = err
		return h
	}
	if err := verifyDirBytes(filepath.Join(s.versionsDir(m.Key), m.VersionID), mf); err != nil {
		h.Status = "corrupted"
		h.Err = err
		return h
	}
	return h
}

// PlanGroupRestore inspects every recorded member and reports which are
// restorable ("ready"), which are gone ("missing") and which fail
// verification ("corrupted"), without changing anything.
func (s *Store) PlanGroupRestore(name string) (GroupRestorePlan, error) {
	rp, err := s.loadGroup(name)
	if err != nil {
		return GroupRestorePlan{}, err
	}
	plan := GroupRestorePlan{Point: rp}
	for _, m := range rp.Members {
		plan.Members = append(plan.Members, s.checkMember(m))
	}
	return plan, nil
}

func (s *Store) writeJournal(j RestoreJournal) error {
	b, err := json.MarshalIndent(&j, "", "  ")
	if err != nil {
		return err
	}
	return writeFileAtomic(s.journalsDir(), filepath.Base(s.journalPath(j.ID)), append(b, '\n'), 0o644)
}

// RestoreGroup restores every member of a recovery point to its recorded
// version. The plan is evaluated first; if any member is missing or corrupt
// and allowPartial is false, ErrGroupIncomplete is returned together with
// the plan and nothing is changed. With allowPartial the caller explicitly
// accepts the gaps: restorable members are restored and every gap is
// recorded as a durable "gap" line in the restore journal.
//
// Visibility: all restorable members are first fully verified and staged as
// new immutable versions; only then does the commit phase flip any current
// pointer. Members always come from this one recovery point — versions from
// other points are never mixed in to fake a complete group. Every step is
// journaled, so an interrupted restore is reconciled and reported by the
// next Open, with confirmed members distinguishable from unfinished ones.
func (s *Store) RestoreGroup(name string, allowPartial bool) (GroupRestoreReport, error) {
	plan, err := s.PlanGroupRestore(name)
	if err != nil {
		return GroupRestoreReport{}, err
	}
	if !plan.Complete() && !allowPartial {
		return GroupRestoreReport{Plan: plan}, fmt.Errorf("%w: recovery point %q", ErrGroupIncomplete, name)
	}

	id, err := newUploadID()
	if err != nil {
		return GroupRestoreReport{}, err
	}
	j := RestoreJournal{
		ID:        id,
		GroupName: plan.Point.Name,
		GroupID:   plan.Point.ID,
		Partial:   allowPartial && !plan.Complete(),
		StartedAt: time.Now().UTC(),
		Status:    "in-progress",
	}
	ready := make(map[int]bool)
	for i, mh := range plan.Members {
		rec := RestoreMemberRecord{Key: mh.Member.Key, SourceVersion: mh.Member.VersionID, Status: "pending"}
		if mh.Status != "ready" {
			rec.Status = "gap"
			rec.Reason = fmt.Sprintf("%s: %v", mh.Status, mh.Err)
		} else {
			ready[i] = true
		}
		j.Members = append(j.Members, rec)
	}
	if err := s.writeJournal(j); err != nil {
		return GroupRestoreReport{}, err
	}

	// Phase 1: stage every restorable member as a new immutable version.
	staged := make(map[int]Manifest)
	for i, mh := range plan.Members {
		if !ready[i] {
			continue
		}
		src := mh.Member
		sm, err := s.loadManifest(src.Key, src.VersionID)
		if err != nil {
			return GroupRestoreReport{Plan: plan}, err
		}
		vid, err := newVersionID()
		if err != nil {
			return GroupRestoreReport{Plan: plan}, err
		}
		nm := Manifest{
			Key:          src.Key,
			VersionID:    vid,
			CreatedAt:    parseVersionTime(vid),
			Size:         sm.Size,
			Sha256:       sm.Sha256,
			Parts:        sm.Parts,
			RestoredFrom: src.VersionID,
			GroupRestore: id,
		}
		if err := s.stageVersionCopy(src.Key, src.VersionID, nm); err != nil {
			return GroupRestoreReport{Plan: plan}, err
		}
		staged[i] = nm
		j.Members[i].NewVersion = vid
		j.Members[i].Status = "staged"
		if err := s.writeJournal(j); err != nil {
			return GroupRestoreReport{Plan: plan}, err
		}
	}

	// Phase 2: commit. All required versions are assembled and verified, so
	// now — and only now — pointers move, one journal line at a time.
	s.pubMu.Lock()
	for i := range plan.Members {
		nm, ok := staged[i]
		if !ok {
			continue
		}
		if err := s.publishLocked(nm, nil); err != nil {
			s.pubMu.Unlock()
			return GroupRestoreReport{Plan: plan}, err
		}
		j.Members[i].Status = "committed"
		if err := s.writeJournal(j); err != nil {
			s.pubMu.Unlock()
			return GroupRestoreReport{Plan: plan}, err
		}
	}
	s.pubMu.Unlock()

	j.FinishedAt = time.Now().UTC()
	if j.Partial {
		j.Status = "completed-partial"
	} else {
		j.Status = "completed"
	}
	if err := s.writeJournal(j); err != nil {
		return GroupRestoreReport{}, err
	}
	return GroupRestoreReport{Journal: j, Plan: plan}, nil
}

// ListRestoreJournals returns the restore history of every group, completed
// and interrupted alike, oldest first.
func (s *Store) ListRestoreJournals() ([]RestoreJournal, error) {
	entries, err := os.ReadDir(s.journalsDir())
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var out []RestoreJournal
	for _, e := range entries {
		if e.IsDir() || filepath.Ext(e.Name()) != ".json" {
			continue
		}
		b, err := os.ReadFile(filepath.Join(s.journalsDir(), e.Name()))
		if err != nil {
			return nil, err
		}
		var j RestoreJournal
		if json.Unmarshal(b, &j) != nil || j.ID == "" {
			continue
		}
		out = append(out, j)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].StartedAt.Before(out[j].StartedAt) })
	return out, nil
}

// DeleteRecoveryPoint removes a confirmed point. The report explains, for
// every version the point used to retain, whether it is now unreferenced or
// still occupied — and by whom (current pointer, a pin, another recovery
// point or an open reader) — so nothing historical disappears silently.
func (s *Store) DeleteRecoveryPoint(name string) (GroupDeleteReport, error) {
	rp, err := s.loadGroup(name)
	if err != nil {
		return GroupDeleteReport{}, err
	}
	if err := os.Remove(s.groupPath(name)); err != nil {
		return GroupDeleteReport{}, err
	}
	if err := fsyncDir(s.groupsDir()); err != nil {
		return GroupDeleteReport{}, err
	}
	rep := GroupDeleteReport{Name: name}
	for _, m := range rp.Members {
		held, err := s.retentionOf(m.Key, m.VersionID)
		if err != nil {
			return rep, err
		}
		if len(held) == 0 {
			rep.Released = append(rep.Released, m)
		} else {
			rep.Retained = append(rep.Retained, RetainedVersion{Key: m.Key, VersionID: m.VersionID, HeldBy: held})
		}
	}
	return rep, nil
}

// retentionOf lists everything that still references one version.
func (s *Store) retentionOf(key, vid string) ([]string, error) {
	var held []string
	if cur, err := s.readCurrent(key); err != nil {
		return nil, err
	} else if cur == vid {
		held = append(held, "current")
	}
	pins, err := s.ListPins()
	if err != nil {
		return nil, err
	}
	for _, p := range pins {
		if p.Key == key && p.VersionID == vid {
			held = append(held, "pin:"+p.Name)
		}
	}
	groups, err := s.ListRecoveryPoints()
	if err != nil {
		return nil, err
	}
	for _, g := range groups {
		for _, m := range g.Members {
			if m.Key == key && m.VersionID == vid {
				held = append(held, "recovery-point:"+g.Name)
			}
		}
	}
	s.gcMu.Lock()
	if s.readers[readerRef{key: key, vid: vid}] > 0 {
		held = append(held, "open-reader")
	}
	s.gcMu.Unlock()
	return held, nil
}

// groupRefsForKey returns the set of version ids of key retained by
// confirmed recovery points.
func (s *Store) groupRefsForKey(key string) (map[string]bool, error) {
	groups, err := s.ListRecoveryPoints()
	if err != nil {
		return nil, err
	}
	set := make(map[string]bool)
	for _, g := range groups {
		for _, m := range g.Members {
			if m.Key == key {
				set[m.VersionID] = true
			}
		}
	}
	return set, nil
}

// reconcileJournals runs during Open: any journal still "in-progress" belongs
// to a restore that was interrupted. Each member line is checked against the
// live tree — committed pointers that are really in place stay confirmed,
// everything else is marked incomplete — and the journal is rewritten as
// "interrupted" so the evidence survives alongside the quarantined residue.
func (s *Store) reconcileJournals(rep *RecoveryReport) error {
	journals, err := s.ListRestoreJournals()
	if err != nil {
		return err
	}
	for _, j := range journals {
		if j.Status != "in-progress" {
			continue
		}
		var confirmed, pending int
		for i, m := range j.Members {
			switch m.Status {
			case "committed":
				cur, _ := s.readCurrent(m.Key)
				if cur == m.NewVersion && s.validateVersionStructure(m.Key, m.NewVersion) == nil {
					confirmed++
				} else {
					j.Members[i].Status = "incomplete"
					j.Members[i].Reason = "committed version not reachable after interruption"
					pending++
				}
			case "pending", "staged":
				j.Members[i].Status = "incomplete"
				j.Members[i].Reason = "restore interrupted before commit"
				pending++
			}
		}
		j.Status = "interrupted"
		if err := s.writeJournal(j); err != nil {
			return err
		}
		rep.Items = append(rep.Items, RecoveryItem{
			Kind:   "group-restore",
			Key:    j.GroupName,
			Path:   s.journalPath(j.ID),
			Action: "restore-interrupted",
			Reason: fmt.Sprintf("restore %s of recovery point %q: %d members confirmed, %d incomplete", j.ID, j.GroupName, confirmed, pending),
		})
	}
	return nil
}
