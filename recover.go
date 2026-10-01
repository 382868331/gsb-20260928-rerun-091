package localobjstore

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// recover runs after taking the store lock:
//   - leftover trash/ entries from an interrupted garbage-collection pass
//     are discarded (their content was already logically reclaimed);
//   - every unfinished multipart upload is quarantined (never silently kept
//     where readers could mistake it for a published object);
//   - every half-published version left in staging/publish is quarantined;
//   - structurally invalid version directories are quarantined;
//   - a missing/dangling "current" pointer is rebuilt to the highest valid
//     version (deterministic winner after a crash mid-publish).
func (s *Store) recover() (RecoveryReport, error) {
	var rep RecoveryReport

	trashEntries, _ := os.ReadDir(s.trashDir())
	for _, e := range trashEntries {
		path := filepath.Join(s.trashDir(), e.Name())
		if err := os.RemoveAll(path); err != nil {
			return rep, err
		}
		rep.Items = append(rep.Items, RecoveryItem{
			Kind: "trash", Path: e.Name(), Action: "discarded",
			Reason: "interrupted garbage collection",
		})
	}

	upRoot := filepath.Join(s.root, "staging", "uploads")
	entries, err := os.ReadDir(upRoot)
	if err != nil {
		return rep, err
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		key := readUploadKey(filepath.Join(upRoot, e.Name()))
		qd, err := s.quarantine("upload", filepath.Join(upRoot, e.Name()))
		rep.Items = append(rep.Items, RecoveryItem{
			Kind: "upload", Key: key, Path: e.Name(), Action: "quarantined",
			Reason: "interrupted multipart upload", QuarDir: qd,
		})
		if err != nil {
			return rep, err
		}
	}

	pubRoot := filepath.Join(s.root, "staging", "publish")
	pubs, err := os.ReadDir(pubRoot)
	if err != nil {
		return rep, err
	}
	for _, e := range pubs {
		if !e.IsDir() {
			continue
		}
		qd, err := s.quarantine("publish", filepath.Join(pubRoot, e.Name()))
		rep.Items = append(rep.Items, RecoveryItem{
			Kind: "publish", Path: e.Name(), Action: "quarantined",
			Reason: "version publish did not commit", QuarDir: qd,
		})
		if err != nil {
			return rep, err
		}
	}

	objs, err := os.ReadDir(filepath.Join(s.root, "objects"))
	if err != nil {
		return rep, err
	}
	for _, od := range objs {
		if !od.IsDir() {
			continue
		}
		objPath := filepath.Join(s.root, "objects", od.Name())
		curData, _ := os.ReadFile(filepath.Join(objPath, currentName))
		curID := string(bytesTrimSpace(curData))
		vdir := filepath.Join(s.root, "objects", od.Name(), "versions")
		ventries, err := os.ReadDir(vdir)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return rep, err
		}
		for _, ve := range ventries {
			if !ve.IsDir() || ve.Name()[0] != 'v' {
				continue
			}
			vp := filepath.Join(vdir, ve.Name())
			m, merr := readAnyManifest(vp)
			var verr error
			if merr == nil {
				verr = validateDir(vp, m)
			} else {
				verr = merr
			}
			if verr == nil && s.verifyOpen {
				verr = verifyDirBytes(vp, m)
			}
			if verr != nil {
				qd, qerr := s.quarantine("version", vp)
				rep.Items = append(rep.Items, RecoveryItem{
					Kind: "version", Key: m.Key, Path: vp, Action: "quarantined",
					Reason: verr.Error(), QuarDir: qd,
				})
				if qerr != nil {
					return rep, qerr
				}
				// Quarantining the version "current" points at must not
				// silently roll the object back to an older version: record
				// a durable fault and drop the pointer, so readers see an
				// explicit failure until a human restores a chosen version.
				if ve.Name() == curID {
					rec := faultRecord(m.Key, curID, verr.Error())
					if ferr := writeFault(objPath, rec); ferr != nil {
						return rep, ferr
					}
					if rerr := os.Remove(filepath.Join(objPath, currentName)); rerr != nil && !os.IsNotExist(rerr) {
						return rep, rerr
					}
					_ = fsyncDir(objPath)
					rep.Items = append(rep.Items, RecoveryItem{
						Kind: "fault", Key: m.Key, Path: objPath, Action: "fault-recorded",
						Reason: fmt.Sprintf("current version %s quarantined as corrupt", curID),
					})
				}
			}
		}
		if err := s.repairCurrent(od.Name(), &rep); err != nil {
			return rep, err
		}
	}
	return rep, nil
}

// repairCurrent ensures current points to an existing valid version.
func (s *Store) repairCurrent(objID string, rep *RecoveryReport) error {
	objPath := filepath.Join(s.root, "objects", objID)
	// A delete tombstone means the missing pointer is intentional: the
	// object must stay deleted and only garbage collection may touch its
	// remaining versions. Never rebuild a pointer over a tombstone.
	if _, err := os.Stat(filepath.Join(objPath, deletedName)); err == nil {
		return nil
	}
	// A recorded fault is a stable failure fact: never rebuild the pointer
	// over it, so repeated restarts keep reporting the same fault instead of
	// resurrecting an old version as current. A stale fault marker next to a
	// valid current pointer (e.g. crash mid-restore) is cleared.
	if _, ok, err := s.readFaultByObjID(objID); err != nil {
		return err
	} else if ok {
		cur, _ := os.ReadFile(filepath.Join(objPath, currentName))
		curID := string(bytesTrimSpace(cur))
		if curID != "" {
			if _, serr := os.Stat(filepath.Join(objPath, "versions", curID, manifestName)); serr == nil {
				if rerr := os.Remove(filepath.Join(objPath, faultName)); rerr != nil {
					return rerr
				}
				rep.Items = append(rep.Items, RecoveryItem{
					Kind: "fault", Path: objPath, Action: "fault-cleared",
					Reason: "current pointer valid again",
				})
			}
		}
		return nil
	}
	vdir := filepath.Join(objPath, "versions")
	entries, err := os.ReadDir(vdir)
	if err != nil {
		return err
	}
	var ids []string
	for _, e := range entries {
		if e.IsDir() && len(e.Name()) > 0 && e.Name()[0] == 'v' {
			if _, serr := os.Stat(filepath.Join(vdir, e.Name(), manifestName)); serr == nil {
				ids = append(ids, e.Name())
			}
		}
	}
	cur, _ := os.ReadFile(filepath.Join(objPath, currentName))
	curID := string(bytesTrimSpace(cur))
	_, curErr := os.Stat(filepath.Join(vdir, curID, manifestName))
	if curID != "" && curErr == nil {
		return nil
	}
	// Dangling or missing pointer: rebuild deterministically to highest id.
	if len(ids) == 0 {
		return nil
	}
	highest := ids[0]
	for _, id := range ids[1:] {
		if id > highest {
			highest = id
		}
	}
	if err := writeFileAtomic(objPath, currentName, []byte(highest+"\n"), 0o444); err != nil {
		return err
	}
	m, _ := readAnyManifest(filepath.Join(vdir, highest))
	rep.Items = append(rep.Items, RecoveryItem{
		Kind: "pointer", Key: m.Key, Path: objPath, Action: "pointer-rebuilt",
		Reason: fmt.Sprintf("current -> %s", highest),
	})
	return nil
}

func bytesTrimSpace(b []byte) []byte {
	for len(b) > 0 && (b[0] == ' ' || b[0] == '\n' || b[0] == '\t' || b[0] == '\r') {
		b = b[1:]
	}
	for len(b) > 0 && (b[len(b)-1] == ' ' || b[len(b)-1] == '\n' || b[len(b)-1] == '\t' || b[len(b)-1] == '\r') {
		b = b[:len(b)-1]
	}
	return b
}

func readUploadKey(dir string) string {
	b, err := os.ReadFile(filepath.Join(dir, "meta.json"))
	if err != nil {
		return ""
	}
	var m uploadMeta
	if json.Unmarshal(b, &m) != nil {
		return ""
	}
	return m.Key
}

func readAnyManifest(versionDir string) (Manifest, error) {
	var m Manifest
	b, err := os.ReadFile(filepath.Join(versionDir, manifestName))
	if err != nil {
		return m, err
	}
	if err := json.Unmarshal(b, &m); err != nil {
		return m, err
	}
	return m, nil
}

// quarantine atomically moves residue out of the live tree.
func (s *Store) quarantine(kind, src string) (string, error) {
	dst := filepath.Join(s.root, "quarantine", kind, time.Now().UTC().Format("20060102T150405.000000000")+"-"+filepath.Base(src))
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return "", err
	}
	if err := os.Rename(src, dst); err != nil {
		return "", err
	}
	_ = fsyncDir(filepath.Dir(dst))
	return dst, nil
}
