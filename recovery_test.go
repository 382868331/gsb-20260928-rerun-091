package localobjstore

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// A crash halfway through writing leaves only residue in staging; after reopen
// it is quarantined and never visible as a published object.
func TestRecoverInterruptedUploadQuarantined(t *testing.T) {
	s, dir := openTemp(t)
	u, err := s.BeginMultipart("half/object")
	if err != nil {
		t.Fatal(err)
	}
	_, err = u.WritePart(strings.NewReader("segment-one"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = u.WritePart(strings.NewReader("segment-two-without-complete"))
	if err != nil {
		t.Fatal(err)
	}
	// Simulate process death: drop all in-memory state without Complete/Abort.
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, rep, err := Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if len(rep.Items) != 1 || rep.Items[0].Kind != "upload" || rep.Items[0].Action != "quarantined" {
		t.Fatalf("expected one quarantined upload, got %+v", rep.Items)
	}
	if rep.Items[0].Key != "half/object" {
		t.Fatalf("quarantine lost object key association: %+v", rep.Items[0])
	}
	if _, err := s2.GetObject("half/object"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("half-written object must not be readable, got %v", err)
	}
	left, _ := os.ReadDir(filepath.Join(dir, "staging", "uploads"))
	if len(left) != 0 {
		t.Fatalf("staging uploads not cleaned: %v", left)
	}
	q, _ := os.ReadDir(filepath.Join(dir, "quarantine", "upload"))
	if len(q) != 1 {
		t.Fatalf("residue not moved to quarantine: %v", q)
	}

	// Second restart is stable: nothing new to recover.
	s2.Close()
	_, rep2, err := Open(dir, nil)
	if err != nil || len(rep2.Items) != 0 {
		t.Fatalf("second reopen should be clean, items=%+v err=%v", rep2.Items, err)
	}
}

// Corrupting published bytes must be detected by readers.
func TestCorruptionDetectedOnRead(t *testing.T) {
	s, dir := openTemp(t)
	m, err := s.PutObject("data/important", strings.NewReader("the quick brown fox"), nil)
	if err != nil {
		t.Fatal(err)
	}
	s.Close()

	// Tamper with the immutable data file out of band.
	dataFile := filepath.Join(dir, "objects", objectID("data/important"), "versions", m.VersionID, "data", "000001")
	if err := os.Chmod(dataFile, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dataFile, []byte("the quick BROWN fox"), 0o644); err != nil {
		t.Fatal(err)
	}

	s2, rep, err := Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	found := false
	for _, it := range rep.Items {
		if it.Kind == "version" && it.Action == "quarantined" {
			found = true
		}
	}
	if !found {
		t.Fatalf("corrupted current not quarantined on reopen: %+v", rep.Items)
	}
	if _, err := s2.GetObject("data/important"); !errors.Is(err, ErrCorrupted) {
		t.Fatalf("damaged current readable as fallback, got %v", err)
	}
}

// A dangling current pointer (crash between version rename and pointer update)
// is deterministically rebuilt to the highest valid version.
func TestRecoverDanglingPointerRebuilt(t *testing.T) {
	s, dir := openTemp(t)
	m1, _ := s.PutObject("k", strings.NewReader("first"), nil)
	m2, _ := s.PutObject("k", strings.NewReader("second"), nil)
	s.Close()

	// Delete the pointer to simulate a crash before it was committed.
	if err := os.Remove(filepath.Join(dir, "objects", objectID("k"), currentName)); err != nil {
		t.Fatal(err)
	}
	s2, rep, err := Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if len(rep.Items) != 1 || rep.Items[0].Action != "pointer-rebuilt" {
		t.Fatalf("expected pointer rebuild, got %+v", rep.Items)
	}
	cur, _ := s2.CurrentVersion("k")
	if cur.VersionID != m2.VersionID || cur.VersionID <= m1.VersionID {
		t.Fatalf("rebuilt pointer wrong: %s", cur.VersionID)
	}
}

// A half-committed publish (staged version never renamed) is quarantined.
func TestRecoverHalfPublishQuarantined(t *testing.T) {
	s, dir := openTemp(t)
	m, _ := s.PutObject("k", strings.NewReader("committed"), nil)
	s.Close()

	// Fabricate a publish staging directory as if the process died mid-publish.
	fake := filepath.Join(dir, "staging", "publish", "v99999999999999999999-aaaaaaaaaaaa")
	if err := os.MkdirAll(filepath.Join(fake, dataDirName), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(fake, manifestName), []byte("{}"), 0o644); err != nil {
		t.Fatal(err)
	}
	s2, rep, err := Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	found := false
	for _, it := range rep.Items {
		if it.Kind == "publish" && it.Action == "quarantined" {
			found = true
		}
	}
	if !found {
		t.Fatalf("half publish not quarantined: %+v", rep.Items)
	}
	// Committed object unaffected.
	r, err := s2.GetObject("k")
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(r)
	r.Close()
	if string(b) != "committed" {
		t.Fatalf("committed object altered: %q (version %s)", b, m.VersionID)
	}
}

// Exercise multipart corruption: tamper one part inside a multi-part object.
func TestCorruptMiddlePartMultipart(t *testing.T) {
	s, dir := openTemp(t)
	u, _ := s.BeginMultipart("multi")
	want := bytes.Repeat([]byte{'A'}, 1000)
	_, _ = u.WritePart(bytes.NewReader(want))
	mid := bytes.Repeat([]byte{'B'}, 1000)
	_, _ = u.WritePart(bytes.NewReader(mid))
	_, _ = u.WritePart(bytes.NewReader(bytes.Repeat([]byte{'C'}, 1000)))
	m, err := u.Complete(nil)
	if err != nil {
		t.Fatal(err)
	}
	s.Close()

	p := filepath.Join(dir, "objects", objectID("multi"), "versions", m.VersionID, "data", "000002")
	_ = os.Chmod(p, 0o644)
	_ = os.WriteFile(p, bytes.Repeat([]byte{'X'}, 1000), 0o644)

	s2, _, err := Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if _, err := s2.GetObject("multi"); !errors.Is(err, ErrCorrupted) {
		t.Fatalf("middle-part corruption not isolated as current: %v", err)
	}
}

// Active interruption test: a subprocess is killed while streaming a large
// upload; the data never appears as a readable published object, and reopen
// isolates the residue.
func TestSubprocessKilledMidUpload(t *testing.T) {
	if os.Getenv("OBJSTORE_CRASH_CHILD") == "1" {
		dir := os.Getenv("OBJSTORE_DIR")
		s, _, err := Open(dir, nil)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		u, err := s.BeginMultipart("interrupted/huge")
		if err != nil {
			os.Exit(2)
		}
		// Signal readiness, then block forever inside the (slow) part write.
		fmt.Println("started")
		os.Stdout.Sync()
		u.WritePart(blockingReader{}) // never returns: simulates mid-write death
		select {}
	}
	dir := t.TempDir()
	cmd := newTestHelperProcess(dir)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, len("started\n"))
	if _, err := cmd.stdout.Read(buf); err != nil {
		t.Fatal(err)
	}
	if err := cmd.cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = cmd.cmd.Wait()

	s, rep, err := Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if len(rep.Items) != 1 || rep.Items[0].Key != "interrupted/huge" {
		t.Fatalf("expected exactly the interrupted upload, got %+v", rep.Items)
	}
	if _, err := s.GetObject("interrupted/huge"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("interrupted upload visible to readers: %v", err)
	}
}

type blockingReader struct{}

func (blockingReader) Read(p []byte) (int, error) { select {} }
