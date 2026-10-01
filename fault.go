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

const faultName = "fault"

// faultPath locates the durable fault marker of an object directory.
func (s *Store) faultPath(key string) string {
	return filepath.Join(s.objectDir(key), faultName)
}

// writeFault durably records that the current version was quarantined as
// corrupt. The marker is the stable failure fact: later restarts see the
// same record instead of silently rolling current back to an older version.
func writeFault(objPath string, rec FaultRecord) error {
	b, err := json.Marshal(&rec)
	if err != nil {
		return err
	}
	return writeFileAtomic(objPath, faultName, append(b, '\n'), 0o644)
}

// readFaultByObjID reads the fault marker of an object directory, if any.
func (s *Store) readFaultByObjID(objID string) (FaultRecord, bool, error) {
	var rec FaultRecord
	b, err := os.ReadFile(filepath.Join(s.root, "objects", objID, faultName))
	if errors.Is(err, os.ErrNotExist) {
		return rec, false, nil
	}
	if err != nil {
		return rec, false, err
	}
	if json.Unmarshal(b, &rec) != nil {
		return rec, false, nil
	}
	return rec, true, nil
}

// Fault returns the recorded corruption fault for key, if one is active.
func (s *Store) Fault(key string) (FaultRecord, bool, error) {
	return s.readFaultByObjID(objectID(key))
}

// InspectVersions lists every published version of key together with the
// result of a full structural + checksum verification, so an operator can
// pick a restore candidate on evidence rather than guessing.
func (s *Store) InspectVersions(key string) ([]VersionStatus, error) {
	entries, err := os.ReadDir(s.versionsDir(key))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var out []VersionStatus
	for _, e := range entries {
		if !e.IsDir() || len(e.Name()) == 0 || e.Name()[0] != 'v' {
			continue
		}
		vid := e.Name()
		st := VersionStatus{}
		m, merr := s.loadManifest(key, vid)
		if merr != nil {
			st.Manifest = Manifest{Key: key, VersionID: vid}
			st.Err = merr
		} else {
			st.Manifest = m
			if verr := s.validateVersionStructure(key, vid); verr != nil {
				st.Err = verr
			} else if verr := verifyDirBytes(filepath.Join(s.versionsDir(key), vid), m); verr != nil {
				st.Err = verr
			} else {
				st.Verified = true
			}
		}
		out = append(out, st)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Manifest.VersionID < out[j].Manifest.VersionID })
	return out, nil
}

// RestoreVersion manually recovers a faulted object from one of its
// historical versions. The source version is fully verified first; a failed
// verification returns an error and leaves the current state untouched.
// On success a brand-new immutable version record is created (with
// RestoredFrom pointing at the source) and becomes current; the quarantined
// corrupt evidence is never rewritten or deleted.
func (s *Store) RestoreVersion(key, versionID string) (Manifest, error) {
	if _, ok, err := s.Fault(key); err != nil {
		return Manifest{}, err
	} else if !ok {
		return Manifest{}, fmt.Errorf("%w: no fault recorded for %s", ErrNotFound, key)
	}
	// Verify the candidate completely before changing anything.
	m, err := s.loadManifest(key, versionID)
	if err != nil {
		return Manifest{}, err
	}
	if err := s.validateVersionStructure(key, versionID); err != nil {
		return Manifest{}, err
	}
	if err := verifyDirBytes(filepath.Join(s.versionsDir(key), versionID), m); err != nil {
		return Manifest{}, err
	}

	vid, err := newVersionID()
	if err != nil {
		return Manifest{}, err
	}
	nm := Manifest{
		Key:          key,
		VersionID:    vid,
		CreatedAt:    parseVersionTime(vid),
		Size:         m.Size,
		Sha256:       m.Sha256,
		Parts:        m.Parts,
		RestoredFrom: versionID,
	}
	if err := s.stageVersionCopy(key, versionID, nm); err != nil {
		return Manifest{}, err
	}
	if err := s.publish(nm, nil); err != nil {
		return Manifest{}, err
	}
	return nm, nil
}

// stageVersionCopy assembles a new version record under staging/publish by
// copying the verified parts of srcVersionID, then fsyncs everything before
// the atomic publish rename. Failure leaves only staging residue, which the
// next Open quarantines; the live tree is never half-modified.
func (s *Store) stageVersionCopy(key, srcVersionID string, nm Manifest) error {
	pdir := filepath.Join(s.root, "staging", "publish", nm.VersionID)
	if err := os.MkdirAll(filepath.Join(pdir, dataDirName), 0o755); err != nil {
		return err
	}
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.RemoveAll(pdir)
		}
	}()
	srcDir := filepath.Join(s.versionsDir(key), srcVersionID, dataDirName)
	for _, p := range nm.Parts {
		src := filepath.Join(srcDir, partFileName(p.Number))
		dst := filepath.Join(pdir, dataDirName, partFileName(p.Number))
		if err := copyFile(src, dst, 0o444); err != nil {
			return err
		}
	}
	b, err := json.MarshalIndent(&nm, "", "  ")
	if err != nil {
		return err
	}
	if err := writeFileAtomic(pdir, manifestName, append(b, '\n'), 0o444); err != nil {
		return err
	}
	if err := fsyncDir(filepath.Join(pdir, dataDirName)); err != nil {
		return err
	}
	if err := fsyncDir(pdir); err != nil {
		return err
	}
	cleanup = false
	return nil
}

// faultRecord builds the durable marker for a quarantined current version.
func faultRecord(key, versionID, reason string) FaultRecord {
	return FaultRecord{Key: key, VersionID: versionID, Reason: reason, DetectedAt: time.Now().UTC()}
}
