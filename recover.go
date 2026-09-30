package localobjstore

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// recover runs after taking the store lock:
//   - every unfinished multipart upload is quarantined (never silently kept
//     where readers could mistake it for a published object);
//   - every half-published version left in staging/publish is quarantined;
//   - structurally invalid (and, with VerifyOnOpen, content-corrupted) version
//     directories are quarantined rather than deleted;
//   - stale durable snapshot files (referenced version gone) are discarded;
//   - a missing/dangling "current" pointer is rebuilt to the highest valid
//     version, except when the current version itself was quarantined as
//     corrupt: that object stays explicitly unavailable and is never made
//     readable again by pointing at an older version.
func (s *Store) recover() (RecoveryReport, error) {
	var rep RecoveryReport

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
		vdir := filepath.Join(objPath, "versions")
		ventries, err := os.ReadDir(vdir)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return rep, err
		}
		curBytes, _ := os.ReadFile(filepath.Join(objPath, currentName))
		curID := string(bytesTrimSpace(curBytes))
		currentQuarantined := false
		var objKey string
		var tomb *Tombstone
		if tb, terr := os.ReadFile(filepath.Join(s.root, "deleted", od.Name()+".json")); terr == nil {
			tomb = &Tombstone{}
			_ = json.Unmarshal(tb, tomb)
		}
		for _, ve := range ventries {
			if !ve.IsDir() || len(ve.Name()) == 0 || ve.Name()[0] != 'v' {
				continue
			}
			vp := filepath.Join(vdir, ve.Name())
			m, merr := readAnyManifest(vp)
			var verr error
			if merr == nil {
				verr = validateDir(vp, m)
				if verr == nil && s.verifyOpen {
					verr = verifyDirBytes(vp, m)
				}
			} else {
				verr = merr
			}
			if verr != nil {
				qd, qerr := s.quarantine("version", vp)
				if ve.Name() == curID {
					currentQuarantined = true
				}
				if m.Key != "" {
					objKey = m.Key
				}
				rep.Items = append(rep.Items, RecoveryItem{
					Kind: "version", Key: m.Key, Path: vp, Action: "quarantined",
					Reason: verr.Error(), QuarDir: qd,
				})
				if qerr != nil {
					return rep, qerr
				}
			} else {
				objKey = m.Key
			}
		}

		// Discard durable snapshot files that reference a version which no
		// longer exists (isolated above or reclaimed elsewhere). A snapshot
		// cannot outlive its content; dropping it re-opens reclaim eligibility.
		refsDir := filepath.Join(s.root, "refs", od.Name())
		if rentries, rerr := os.ReadDir(refsDir); rerr == nil {
			for _, re := range rentries {
				if re.IsDir() {
					continue
				}
				rp := filepath.Join(refsDir, re.Name())
				b, berr := os.ReadFile(rp)
				var ref Ref
				if berr != nil || json.Unmarshal(b, &ref) != nil || ref.VersionID == "" {
					_ = os.Remove(rp)
					rep.Items = append(rep.Items, RecoveryItem{
						Kind: "ref", Key: objKey, Path: rp, Action: "discarded",
						Reason: "unreadable snapshot file",
					})
					continue
				}
				if _, serr := os.Stat(filepath.Join(vdir, ref.VersionID, manifestName)); serr != nil {
					_ = os.Remove(rp)
					rep.Items = append(rep.Items, RecoveryItem{
						Kind: "ref", Key: ref.Key, Path: rp, Action: "discarded",
						Reason: "referenced version missing",
					})
				}
			}
		}

		if currentQuarantined || (tomb != nil && tomb.Key != "" && curID != "") {
			// The current version was corrupt and is now isolated. Never
			// rebuild the pointer onto an older version: the object must stay
			// explicitly unavailable rather than silently serving stale bytes.
			// The same applies when a tombstone exists: a crash landed after
			// the delete commit but before the pointer was removed.
			if err := os.Remove(filepath.Join(objPath, currentName)); err != nil && !os.IsNotExist(err) {
				return rep, err
			}
			_ = fsyncDir(objPath)
			reason := "current version quarantined as corrupt"
			if !currentQuarantined {
				reason = "delete committed by tombstone after crash"
			}
			rep.Items = append(rep.Items, RecoveryItem{
				Kind: "pointer", Key: objKey, Path: objPath, Action: "pointer-removed",
				Reason: reason,
			})
			continue
		}
		if tomb != nil && tomb.Key != "" && curID == "" {
			// Object is deleted and its pointer is already gone: do not
			// rebuild the pointer onto retained-but-deleted content.
			continue
		}
		if err := s.repairCurrent(od.Name(), &rep); err != nil {
			return rep, err
		}
	}
	if err := s.reconcileTombstones(&rep); err != nil {
		return rep, err
	}
	return rep, nil
}

// reconcileTombstones finishes a delete that crashed between writing the
// tombstone and removing the current pointer, and drops tombstones whose
// object directory has already been fully removed.
func (s *Store) reconcileTombstones(rep *RecoveryReport) error {
	dir := filepath.Join(s.root, "deleted")
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if e.IsDir() || filepath.Ext(e.Name()) != ".json" {
			continue
		}
		var t Tombstone
		b, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			continue
		}
		if json.Unmarshal(b, &t) != nil || t.Key == "" {
			continue
		}
		objPath := s.objectDir(t.Key)
		if _, serr := os.Stat(objPath); serr != nil {
			// Object directory already gone (fully finalized content).
			if err := os.Remove(filepath.Join(dir, e.Name())); err != nil && !os.IsNotExist(err) {
				return err
			}
			_ = fsyncDir(dir)
			continue
		}
		curPath := filepath.Join(objPath, currentName)
		if cur, rerr := os.ReadFile(curPath); rerr == nil && len(bytesTrimSpace(cur)) > 0 {
			if err := os.Remove(curPath); err != nil && !os.IsNotExist(err) {
				return err
			}
			_ = fsyncDir(objPath)
			rep.Items = append(rep.Items, RecoveryItem{
				Kind: "pointer", Key: t.Key, Path: objPath, Action: "pointer-removed",
				Reason: "delete committed by tombstone after crash",
			})
		}
	}
	return nil
}

// repairCurrent ensures current points to an existing version directory. It
// rebuilds a missing/dangling pointer deterministically to the highest version
// id (the crash window between version rename and pointer commit), but it is
// never invoked for a pointer whose target was quarantined as corrupt.
func (s *Store) repairCurrent(objID string, rep *RecoveryReport) error {
	objPath := filepath.Join(s.root, "objects", objID)
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
