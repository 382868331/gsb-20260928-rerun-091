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

// RestoreOptions controls group restore behaviour.
type RestoreOptions struct {
	// AllowPartial permits the restore to proceed when some members are
	// missing or corrupted: those members are recorded as durable gaps and
	// their keys are left untouched. Without it, any unavailable member
	// fails the whole restore before anything becomes visible.
	AllowPartial bool
}

// RestoreMemberResult is the durable per-member outcome of a group restore.
type RestoreMemberResult struct {
	Key         string    `json:"key"`
	VersionID   string    `json:"version_id"` // restore point member version
	State       string    `json:"state"`      // "pending" | "confirmed" | "gap"
	Reason      string    `json:"reason,omitempty"`
	ConfirmedAt time.Time `json:"confirmed_at,omitempty"`
}

// RestoreRecord is the durable journal of one group restore. It is written
// before any member becomes visible and rewritten atomically as each member
// is confirmed, so reopening the store after an interruption can tell
// confirmed results apart from operations that never finished. Records are
// kept forever under restores/ as the restore history.
type RestoreRecord struct {
	ID          string                `json:"id"`
	PointName   string                `json:"point_name"`
	PointID     string                `json:"point_id"`
	Partial     bool                  `json:"partial"`
	State       string                `json:"state"`                 // "in-progress" | "interrupted" | "completed" | "completed-with-gaps"
	Interrupted bool                  `json:"interrupted,omitempty"` // recovery found it unfinished
	StartedAt   time.Time             `json:"started_at"`
	CompletedAt time.Time             `json:"completed_at,omitempty"`
	Members     []RestoreMemberResult `json:"members"`
}

func (s *Store) restoresDir() string { return filepath.Join(s.root, "restores") }

func (s *Store) restoreRecordPath(id string) string {
	return filepath.Join(s.restoresDir(), id+".json")
}

// writeRestoreRecord atomically rewrites the journal; the rename is the
// commit point for each state transition.
func (s *Store) writeRestoreRecord(rec *RestoreRecord) error {
	b, err := json.MarshalIndent(rec, "", "  ")
	if err != nil {
		return err
	}
	return writeFileAtomic(s.restoresDir(), rec.ID+".json", append(b, '\n'), 0o644)
}

// RestorePoint restores the group captured by the named restore point.
//
// The flow is preview-then-commit: InspectRestorePoint shows which members
// are readable, corrupted or missing; RestorePoint then either restores
// every member or, with RestoreOptions{AllowPartial: true}, restores the
// readable ones and records durable gap markers for the rest.
//
// A full restore changes nothing unless every required member is available
// and verified. The journal is written before the first pointer switch and
// every member is switched to exactly the version recorded in the restore
// point — never a version from another point — so readers can never observe
// a group stitched together from different restore points. An interrupted
// restore is recognised by the next Open (state "interrupted") and finished
// explicitly with ResumeRestore.
func (s *Store) RestorePoint(name string, opts *RestoreOptions) (RestoreRecord, error) {
	rp, err := s.GetRestorePoint(name)
	if err != nil {
		return RestoreRecord{}, err
	}
	plan, err := s.InspectRestorePoint(name)
	if err != nil {
		return RestoreRecord{}, err
	}
	partial := opts != nil && opts.AllowPartial
	if !plan.Complete && !partial {
		return RestoreRecord{}, fmt.Errorf("%w: restore point %q has unavailable members, inspect it first or allow a partial restore", ErrRestoreMembersUnavailable, name)
	}

	rec := RestoreRecord{
		ID:        newRestoreID(),
		PointName: rp.Name,
		PointID:   rp.ID,
		Partial:   partial,
		State:     "in-progress",
		StartedAt: time.Now().UTC(),
	}
	health := make(map[string]MemberHealth, len(plan.Members))
	for _, h := range plan.Members {
		health[h.Key] = h
	}
	for _, m := range rp.Members {
		mr := RestoreMemberResult{Key: m.Key, VersionID: m.VersionID, State: "pending"}
		if h := health[m.Key]; h.Status != "readable" {
			mr.State = "gap"
			mr.Reason = fmt.Sprintf("%s: %v", h.Status, h.Err)
		}
		rec.Members = append(rec.Members, mr)
	}
	// The durable journal exists before anything becomes visible.
	if err := s.writeRestoreRecord(&rec); err != nil {
		return RestoreRecord{}, err
	}
	if err := s.applyPendingMembers(&rec); err != nil {
		return rec, err
	}
	return rec, s.finishRestoreRecord(&rec)
}

func newRestoreID() string {
	id, err := newVersionID()
	if err != nil {
		return fmt.Sprintf("restore-%d", time.Now().UTC().UnixNano())
	}
	return "restore-" + id[1:]
}

// applyPendingMembers switches every pending member of the journal to its
// recorded version, confirming each in the journal before moving on.
func (s *Store) applyPendingMembers(rec *RestoreRecord) error {
	for i := range rec.Members {
		if rec.Members[i].State != "pending" {
			continue
		}
		if err := s.applyRestoreMember(rec.Members[i].Key, rec.Members[i].VersionID); err != nil {
			return fmt.Errorf("localobjstore: restore member %s: %w", rec.Members[i].Key, err)
		}
		rec.Members[i].State = "confirmed"
		rec.Members[i].ConfirmedAt = time.Now().UTC()
		if err := s.writeRestoreRecord(rec); err != nil {
			return err
		}
	}
	return nil
}

// applyRestoreMember makes the recorded member version current for its key.
// The version is fully re-verified first; the switch serialises with
// publishes and GC on the per-key lock, and lifts any tombstone or fault
// marker because the restored current version is known-good.
func (s *Store) applyRestoreMember(key, versionID string) error {
	if err := s.validateVersionStructure(key, versionID); err != nil {
		return err
	}
	m, err := s.loadManifest(key, versionID)
	if err != nil {
		return err
	}
	if err := verifyDirBytes(filepath.Join(s.versionsDir(key), versionID), m); err != nil {
		return err
	}

	s.pubMu.Lock()
	defer s.pubMu.Unlock()
	lock, err := acquireFlock(filepath.Join(s.root, "locks", "publish-"+objectID(key)+".lock"))
	if err != nil {
		return err
	}
	defer lock.release()

	if err := s.writeCurrent(key, versionID); err != nil {
		return err
	}
	if err := os.Remove(filepath.Join(s.objectDir(key), deletedName)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := os.Remove(filepath.Join(s.objectDir(key), faultName)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return fsyncDir(s.objectDir(key))
}

// finishRestoreRecord marks the journal completed (with or without gaps).
func (s *Store) finishRestoreRecord(rec *RestoreRecord) error {
	gaps := 0
	for _, m := range rec.Members {
		if m.State == "gap" {
			gaps++
		}
	}
	if gaps > 0 {
		rec.State = "completed-with-gaps"
	} else {
		rec.State = "completed"
	}
	rec.CompletedAt = time.Now().UTC()
	return s.writeRestoreRecord(rec)
}

// ResumeRestore finishes an interrupted group restore after a crash or
// shutdown: pending members are re-verified and switched, members whose
// versions have since disappeared become durable gaps, and the journal is
// completed. Already-confirmed members are never redone.
func (s *Store) ResumeRestore(id string) (RestoreRecord, error) {
	rec, err := s.RestoreStatus(id)
	if err != nil {
		return RestoreRecord{}, err
	}
	if rec.State != "interrupted" && rec.State != "in-progress" {
		return rec, fmt.Errorf("localobjstore: restore %s is %s, nothing to resume", id, rec.State)
	}
	rec.State = "in-progress"
	if err := s.writeRestoreRecord(&rec); err != nil {
		return rec, err
	}
	for i := range rec.Members {
		if rec.Members[i].State != "pending" {
			continue
		}
		m := &rec.Members[i]
		if err := s.applyRestoreMember(m.Key, m.VersionID); err != nil {
			m.State = "gap"
			m.Reason = fmt.Sprintf("unavailable at resume: %v", err)
		} else {
			m.State = "confirmed"
			m.ConfirmedAt = time.Now().UTC()
		}
		if err := s.writeRestoreRecord(&rec); err != nil {
			return rec, err
		}
	}
	return rec, s.finishRestoreRecord(&rec)
}

// RestoreStatus loads the durable journal of one group restore.
func (s *Store) RestoreStatus(id string) (RestoreRecord, error) {
	var rec RestoreRecord
	b, err := os.ReadFile(s.restoreRecordPath(id))
	if errors.Is(err, os.ErrNotExist) {
		return rec, fmt.Errorf("%w: restore %q", ErrNotFound, id)
	}
	if err != nil {
		return rec, err
	}
	if err := json.Unmarshal(b, &rec); err != nil {
		return rec, fmt.Errorf("%w: restore record %q unreadable: %v", ErrCorrupted, id, err)
	}
	return rec, nil
}

// ListRestores returns the full restore history, oldest first.
func (s *Store) ListRestores() ([]RestoreRecord, error) {
	entries, err := os.ReadDir(s.restoresDir())
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var out []RestoreRecord
	for _, e := range entries {
		if e.IsDir() || filepath.Ext(e.Name()) != ".json" {
			continue
		}
		b, err := os.ReadFile(filepath.Join(s.restoresDir(), e.Name()))
		if err != nil {
			return nil, err
		}
		var rec RestoreRecord
		if json.Unmarshal(b, &rec) != nil || rec.ID == "" {
			continue
		}
		out = append(out, rec)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].StartedAt.Before(out[j].StartedAt) })
	return out, nil
}

// pendingRestoreRefs returns the set of version ids of key that unfinished
// group restores still need; GC retains them so an interrupted restore can
// always be resumed.
func (s *Store) pendingRestoreRefs(key string) (map[string]bool, error) {
	recs, err := s.ListRestores()
	if err != nil {
		return nil, err
	}
	set := make(map[string]bool)
	for _, rec := range recs {
		if rec.State != "in-progress" && rec.State != "interrupted" {
			continue
		}
		for _, m := range rec.Members {
			if m.Key == key && m.State == "pending" {
				set[m.VersionID] = true
			}
		}
	}
	return set, nil
}
