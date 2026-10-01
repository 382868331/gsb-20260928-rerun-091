package localobjstore

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func tamperVersion(t *testing.T, dir, key string, m Manifest, payload string) {
	t.Helper()
	p := filepath.Join(dir, "objects", objectID(key), "versions", m.VersionID, "data", "000001")
	if err := os.Chmod(p, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(payload), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestCorruptedCurrentStaysIsolatedAcrossReopens(t *testing.T) {
	s, dir := openTemp(t)
	old, _ := s.PutObject("k", strings.NewReader("old-data"), nil)
	bad, _ := s.PutObject("k", strings.NewReader("new-data"), nil)
	s.Close()
	tamperVersion(t, dir, "k", bad, "BAD-DATA!")

	s2, rep, err := Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.Items) != 1 {
		t.Fatalf("expected one recovery item, got %+v", rep.Items)
	}
	if _, err := s2.GetObject("k"); !errors.Is(err, ErrCorrupted) {
		t.Fatalf("GetObject = %v, want ErrCorrupted", err)
	}
	if _, err := s2.CurrentVersion("k"); !errors.Is(err, ErrCorrupted) {
		t.Fatalf("CurrentVersion = %v, want ErrCorrupted", err)
	}
	r, err := s2.GetVersion("k", old.VersionID)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(r)
	r.Close()
	if string(b) != "old-data" {
		t.Fatalf("historical version not explicitly readable: %q", b)
	}
	recs, err := s2.ListCorruptions()
	if err != nil || len(recs) != 1 || !recs[0].Active || recs[0].CorruptedVersionID != bad.VersionID {
		t.Fatalf("bad corruption records: %+v err=%v", recs, err)
	}
	first := recs[0]
	s2.Close()

	s3, rep3, err := Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s3.Close()
	if len(rep3.Items) != 1 || rep3.Items[0].Path != bad.VersionID || rep3.Items[0].QuarDir != first.QuarantinePath {
		t.Fatalf("fault fact changed on reopen: %+v", rep3.Items)
	}
	again, _ := s3.ListCorruptions()
	if len(again) != 1 || again[0] != first {
		t.Fatalf("corruption record changed: before=%+v after=%+v", first, again[0])
	}
}

func TestManualRestoreCreatesNewVersionAndPreservesEvidence(t *testing.T) {
	s, dir := openTemp(t)
	old, _ := s.PutObject("k", strings.NewReader("old-data"), nil)
	bad, _ := s.PutObject("k", strings.NewReader("new-data"), nil)
	s.Close()
	tamperVersion(t, dir, "k", bad, "BAD-DATA!")
	s2, _, err := Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()

	cands, err := s2.ListRecoveryCandidates("k")
	if err != nil || len(cands) != 1 || !cands[0].Verified || cands[0].Manifest.VersionID != old.VersionID {
		t.Fatalf("bad candidates: %+v err=%v", cands, err)
	}
	m, err := s2.RestoreVersion("k", old.VersionID)
	if err != nil {
		t.Fatal(err)
	}
	if m.VersionID == old.VersionID || m.VersionID == bad.VersionID {
		t.Fatal("restore reused an existing version id")
	}
	r, err := s2.GetObject("k")
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(r)
	r.Close()
	if string(b) != "old-data" {
		t.Fatalf("restored content = %q", b)
	}
	recs, _ := s2.ListCorruptions()
	if len(recs) != 1 || recs[0].Active || recs[0].RestoredVersionID != m.VersionID || recs[0].CorruptedVersionID != bad.VersionID {
		t.Fatalf("record not resolved correctly: %+v", recs)
	}
	if _, err := os.Stat(recs[0].QuarantinePath); err != nil {
		t.Fatalf("original damaged evidence removed: %v", err)
	}
}

func TestFailedRestoreLeavesCurrentFaultUnchanged(t *testing.T) {
	s, dir := openTemp(t)
	old, _ := s.PutObject("k", strings.NewReader("old-data"), nil)
	bad, _ := s.PutObject("k", strings.NewReader("new-data"), nil)
	s.Close()
	tamperVersion(t, dir, "k", bad, "BAD-DATA!")
	s2, _, err := Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()

	tamperVersion(t, dir, "k", old, "OLD-BAD!")
	cands, err := s2.ListRecoveryCandidates("k")
	if err != nil || len(cands) != 1 || cands[0].Verified || cands[0].Error == "" {
		t.Fatalf("expected invalid candidate checksum: %+v err=%v", cands, err)
	}
	if _, err := s2.RestoreVersion("k", old.VersionID); !errors.Is(err, ErrCorrupted) {
		t.Fatalf("restore from damaged history = %v, want ErrCorrupted", err)
	}
	recs, _ := s2.ListCorruptions()
	if len(recs) != 1 || !recs[0].Active || recs[0].CorruptedVersionID != bad.VersionID || recs[0].RestoredVersionID != "" {
		t.Fatalf("failed restore mutated fault state: %+v", recs)
	}
	if _, err := s2.GetObject("k"); !errors.Is(err, ErrCorrupted) {
		t.Fatalf("current became readable after failed restore: %v", err)
	}
}

func TestGCRetainsRecoveryCandidatesWithTombstoneReadersAndPins(t *testing.T) {
	s, dir := openTemp(t)
	old, _ := s.PutObject("k", strings.NewReader("old-data"), nil)
	bad, _ := s.PutObject("k", strings.NewReader("new-data"), nil)
	if err := s.PinVersion("snap", "k", old.VersionID); err != nil {
		t.Fatal(err)
	}
	r0, err := s.GetVersion("k", old.VersionID)
	if err != nil {
		t.Fatal(err)
	}
	defer r0.Close()
	s.Close()
	tamperVersion(t, dir, "k", bad, "BAD-DATA!")
	s2, _, err := Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if err := os.WriteFile(filepath.Join(dir, "objects", objectID("k"), deletedName), []byte(bad.VersionID+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	rep, err := s2.CollectGarbage()
	if err != nil {
		t.Fatal(err)
	}
	if gcActions(rep, "reclaimed") != 0 || gcActions(rep, "retained") != 1 {
		t.Fatalf("recovery candidate swept: %+v", rep.Items)
	}
	if _, err := s2.GetVersion("k", old.VersionID); err != nil {
		t.Fatalf("candidate lost under delete/read/pin protection: %v", err)
	}
	m, err := s2.RestoreVersion("k", old.VersionID)
	if err != nil {
		t.Fatal(err)
	}
	if m.VersionID == old.VersionID {
		t.Fatal("restore must create a new record")
	}
}

func TestReopenSequencesUploadCorruptionAndInterruptedGC(t *testing.T) {
	s, dir := openTemp(t)
	old, _ := s.PutObject("k", strings.NewReader("old-data"), nil)
	bad, _ := s.PutObject("k", strings.NewReader("new-data"), nil)
	u, err := s.BeginMultipart("k")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := u.WritePart(strings.NewReader("partial")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "trash", "leftover"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	s.Close()
	tamperVersion(t, dir, "k", bad, "BAD-DATA!")

	s2, rep, err := Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	kinds := map[string]int{}
	for _, it := range rep.Items {
		kinds[it.Kind]++
	}
	if kinds["trash"] != 1 || kinds["upload"] != 1 || kinds["version"] != 1 {
		t.Fatalf("unexpected combined recovery: %+v", rep.Items)
	}
	s2.Close()

	s3, rep3, err := Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s3.Close()
	if len(rep3.Items) != 1 || rep3.Items[0].Path != bad.VersionID {
		t.Fatalf("second open must retain only the same fault: %+v", rep3.Items)
	}
	m, err := s3.RestoreVersion("k", old.VersionID)
	if err != nil {
		t.Fatal(err)
	}
	r, err := s3.GetObject("k")
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(r)
	r.Close()
	if string(b) != "old-data" {
		t.Fatalf("restore after interrupted work = %q", b)
	}
	if m.VersionID <= bad.VersionID {
		t.Fatal("restored record must be newer than damaged version")
	}
}

func TestInterruptedRestoreCompletesOnReopen(t *testing.T) {
	s, dir := openTemp(t)
	old, _ := s.PutObject("k", strings.NewReader("old-data"), nil)
	bad, _ := s.PutObject("k", strings.NewReader("new-data"), nil)
	s.Close()
	tamperVersion(t, dir, "k", bad, "BAD-DATA!")
	s2, _, err := Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	s2.Close()

	newID, err := newVersionID()
	if err != nil {
		t.Fatal(err)
	}
	obj := filepath.Join(dir, "objects", objectID("k"))
	if err := copyVersionTree(filepath.Join(obj, "versions", old.VersionID), filepath.Join(obj, "versions", newID)); err != nil {
		t.Fatal(err)
	}
	m, err := loadManifestFile(t, filepath.Join(obj, "versions", old.VersionID, manifestName))
	if err != nil {
		t.Fatal(err)
	}
	m.VersionID = newID
	m.CreatedAt = parseVersionTime(newID)
	b, _ := json.MarshalIndent(&m, "", "  ")
	tmp := filepath.Join(obj, "versions", newID, ".manifest.tmp")
	if err := os.WriteFile(tmp, append(b, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(tmp, filepath.Join(obj, "versions", newID, manifestName)); err != nil {
		t.Fatal(err)
	}
	rec := CorruptionRecord{
		Key: "k", CorruptedVersionID: bad.VersionID, SourceVersionID: old.VersionID,
		RestorePendingID: newID, DetectedAt: bad.CreatedAt, Active: true,
		QuarantinePath: filepath.Join(dir, "quarantine", "version", "x"),
	}
	recBytes, _ := json.MarshalIndent(&rec, "", "  ")
	if err := os.WriteFile(filepath.Join(obj, corruptionName), append(recBytes, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}

	s3, rep, err := Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s3.Close()
	if len(rep.Items) != 2 {
		t.Fatalf("expected fault replay plus resumed restore, got %+v", rep.Items)
	}
	r, err := s3.GetObject("k")
	if err != nil {
		t.Fatal(err)
	}
	data, _ := io.ReadAll(r)
	r.Close()
	if string(data) != "old-data" {
		t.Fatalf("restored current unavailable: %q", data)
	}
	recs, _ := s3.ListCorruptions()
	if len(recs) != 1 || recs[0].Active || recs[0].RestoredVersionID != newID {
		t.Fatalf("pending restore not resolved: %+v", recs)
	}
}

func loadManifestFile(t *testing.T, path string) (Manifest, error) {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		return Manifest{}, err
	}
	var m Manifest
	return m, json.Unmarshal(b, &m)
}
