package objectstore

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
)

// RecoveryReport summarizes a startup recovery pass.
type RecoveryReport struct {
	Published   int      // assembled-but-uncommitted objects re-published
	Quarantined []string // upload session directories isolated as fragments
}

// Recover scans the uploads directory and reconciles every session left over
// from a crash:
//
//   - receiving/assembling: incomplete or possibly half-assembled fragments
//     are moved to quarantine; readers never saw them.
//   - assembled: parts and staged bytes are verified; valid objects are
//     installed and atomically published (when the key has no pointer), then
//     the session is removed. Invalid material is quarantined.
//   - committed: publish already completed; the session directory is cleaned
//     up. The published pointer/blob are the durable truth.
func (s *Store) Recover(ctx context.Context) (RecoveryReport, error) {
	var report RecoveryReport

	entries, err := os.ReadDir(s.path("uploads"))
	if err != nil {
		if os.IsNotExist(err) {
			return report, nil
		}
		return report, err
	}

	var ready []pendingEntry
	for _, ent := range entries {
		if ctx.Err() != nil {
			return report, ctx.Err()
		}
		if !ent.IsDir() {
			continue
		}
		dir := filepath.Join(s.path("uploads"), ent.Name())
		if s.isLiveSession(dir) {
			// Owner process is alive (or holding its flock); never touch it.
			continue
		}
		m, ok := s.loadMeta(dir)
		if !ok {
			// Unreadable session: isolate as unknown fragment.
			if q := s.quarantine(dir, "unmetadata"); q != "" {
				report.Quarantined = append(report.Quarantined, q)
			}
			continue
		}
		switch m.State {
		case stateCommitted:
			// Publish is durable; only session cleanup may have been missed.
			_ = os.RemoveAll(dir)
		case stateAssembled:
			// Another process may be finishing this session right now. We can
			// verify and reclaim it cheaply, but must not quarantine a valid
			// one we did not publish: an assembled session is only garbage if
			// its bytes fail verification. Leave verified ones in place when
			// the CAS pointer is already taken by a different winner.
			ready = append(ready, pendingEntry{dir: dir, meta: m})
		default:
			// receiving/assembling with no live owner lock = crash residue.
			if q := s.quarantine(dir, m.State); q != "" {
				report.Quarantined = append(report.Quarantined, q)
			}
		}
	}

	// Deterministic, oldest-first order makes concurrent crash recoveries of the
	// same key resolve to one well-defined winner.
	sortPending(ready)
	for _, p := range ready {
		if ctx.Err() != nil {
			return report, ctx.Err()
		}
		m := p.meta
		parts, perr := orderedParts(&m)
		valid := perr == nil && s.verifySession(p.dir, &m, parts)
		if !valid {
			if q := s.quarantine(p.dir, "invalid-assembled"); q != "" {
				report.Quarantined = append(report.Quarantined, q)
			}
			continue
		}
		if _, ierr := s.installBlobIfMissing(&m); ierr != nil {
			if q := s.quarantine(p.dir, "install-failed"); q != "" {
				report.Quarantined = append(report.Quarantined, q)
			}
			continue
		}
		published, perr := s.recoverPublish(&m)
		if perr != nil {
			return report, perr
		}
		if published {
			report.Published++
		}
		m.State = stateCommitted
		_ = s.writeMeta(&m)
		_ = os.RemoveAll(p.dir)
	}
	_ = syncDir(s.path("uploads"))
	return report, nil
}

func (s *Store) loadMeta(dir string) (uploadMeta, bool) {
	var m uploadMeta
	data, err := os.ReadFile(filepath.Join(dir, "meta.json"))
	if err != nil {
		return m, false
	}
	if err := json.Unmarshal(data, &m); err != nil {
		return m, false
	}
	if m.ID == "" || m.Key == "" || m.Parts == nil {
		return m, false
	}
	return m, true
}

// isLiveSession reports whether an upload session is locked by a running
// process. flock is released automatically on process death (including
// SIGKILL/power loss), so a held lock means "active"; an unheld lock means
// "safe for recovery to reconcile".
func (s *Store) isLiveSession(dir string) bool {
	f, err := os.OpenFile(filepath.Join(dir, ".live"), os.O_RDWR|os.O_CREATE, filePerm)
	if err != nil {
		return false
	}
	defer f.Close()
	err = tryFlock(f)
	if err != nil {
		return true
	}
	_ = unlockFlock(f)
	return false
}

func (s *Store) verifySession(dir string, m *uploadMeta, parts []PartInfo) bool {
	staged := filepath.Join(dir, "staged")
	got, size, err := hashFile(staged)
	if err != nil {
		return false
	}
	if got != m.Version || size != m.Size {
		return false
	}
	for _, p := range parts {
		hash, psize, err := hashFile(filepath.Join(dir, "parts", partName(p.Number)))
		if err != nil || hash != p.SHA256 || psize != p.Size {
			return false
		}
	}
	return true
}

func (s *Store) installBlobIfMissing(m *uploadMeta) (bool, error) {
	blobPath, err := s.blobPath(m.Version)
	if err != nil {
		return false, err
	}
	if _, err := os.Stat(blobPath); err == nil {
		return false, nil
	}
	if err := s.installBlob(m, blobPath); err != nil {
		return false, err
	}
	return true, nil
}

func (s *Store) quarantine(dir, reason string) string {
	dest := filepath.Join(s.path("quarantine"),
		filepath.Base(dir)+"-"+sanitizeReason(reason)+"-"+randomSuffix())
	if err := os.Rename(dir, dest); err != nil {
		return ""
	}
	_ = syncDir(s.path("uploads"))
	_ = syncDir(s.path("quarantine"))
	return dest
}
