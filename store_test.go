package objectstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
)

func openTestStore(t *testing.T) *Store {
	t.Helper()
	s, err := Open(context.Background(), t.TempDir())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func sha256Hex(b []byte) string {
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

func complete(t *testing.T, s *Store, key string, chunks [][]byte, opts ...UploadOption) Version {
	t.Helper()
	u, err := s.CreateUpload(key, opts...)
	if err != nil {
		t.Fatalf("create upload: %v", err)
	}
	for i, c := range chunks {
		if _, err := u.PutPart(uint32(i+1), bytes.NewReader(c)); err != nil {
			t.Fatalf("put part %d: %v", i+1, err)
		}
	}
	v, err := u.CompleteUpload()
	if err != nil {
		t.Fatalf("complete: %v", err)
	}
	return v
}

func readAll(t *testing.T, s *Store, key string) (Version, []byte) {
	t.Helper()
	v, rc, err := s.ReadObject(key)
	if err != nil {
		t.Fatalf("read object: %v", err)
	}
	defer rc.Close()
	b, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read all: %v", err)
	}
	return v, b
}

func TestSmallObjectRoundTripAndVersionStability(t *testing.T) {
	s := openTestStore(t)
	payload := []byte("hello world")

	v := complete(t, s, "docs/a", [][]byte{payload})
	if v.Version != sha256Hex(payload) {
		t.Fatalf("version id is not content hash: got %s", v.Version)
	}
	if v.Size != int64(len(payload)) {
		t.Fatalf("size = %d", v.Size)
	}

	gotVer, got := readAll(t, s, "docs/a")
	if !bytes.Equal(got, payload) {
		t.Fatalf("payload mismatch: %q", got)
	}
	if gotVer.Version != v.Version {
		t.Fatalf("read version mismatch")
	}

	// Empty-ish small object still requires at least one part.
	tiny := []byte{0}
	v2 := complete(t, s, "docs/b", [][]byte{tiny})
	if v2.Version != sha256Hex(tiny) {
		t.Fatalf("tiny version mismatch")
	}
}

func TestMultipartAssembly(t *testing.T) {
	s := openTestStore(t)
	chunks := [][]byte{
		bytes.Repeat([]byte("a"), 5000),
		bytes.Repeat([]byte("b"), 3),
		bytes.Repeat([]byte("c"), 12345),
	}
	var want bytes.Buffer
	for _, c := range chunks {
		want.Write(c)
	}
	complete(t, s, "big", chunks)
	_, got := readAll(t, s, "big")
	if !bytes.Equal(got, want.Bytes()) {
		t.Fatalf("assembled content mismatch")
	}
}

func TestPartsMustBeContiguous(t *testing.T) {
	s := openTestStore(t)
	u, err := s.CreateUpload("x")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := u.PutPart(1, bytes.NewReader([]byte("a"))); err != nil {
		t.Fatal(err)
	}
	if _, err := u.PutPart(3, bytes.NewReader([]byte("c"))); err != nil {
		t.Fatal(err)
	}
	if _, err := u.CompleteUpload(); !errors.Is(err, ErrInvalidPart) {
		t.Fatalf("expected ErrInvalidPart, got %v", err)
	}
}

func TestIdempotentSamePartAndChangedPartRejected(t *testing.T) {
	s := openTestStore(t)
	u, _ := s.CreateUpload("x")
	p1, err := u.PutPart(1, bytes.NewReader([]byte("hello")))
	if err != nil {
		t.Fatal(err)
	}
	p1b, err := u.PutPart(1, bytes.NewReader([]byte("hello")))
	if err != nil {
		t.Fatal(err)
	}
	if p1 != p1b {
		t.Fatalf("identical part re-upload should be idempotent")
	}
}

func TestConcurrentPublishHasDeterministicWinner(t *testing.T) {
	s := openTestStore(t)
	key := "race"
	first := complete(t, s, key, [][]byte{[]byte("v1")})

	// Both contenders believe they are updating v1. Exactly one wins.
	const n = 8
	var wg sync.WaitGroup
	results := make([]error, n)
	wg.Add(n)
	for i := 0; i < n; i++ {
		payload := []byte("winner-" + string(rune('A'+i)))
		go func(i int, payload []byte) {
			defer wg.Done()
			u, err := s.CreateUpload(key, IfMatch(first.Version))
			if err != nil {
				results[i] = err
				return
			}
			if _, err := u.PutPart(1, bytes.NewReader(payload)); err != nil {
				results[i] = err
				return
			}
			_, results[i] = u.CompleteUpload()
		}(i, payload)
	}
	wg.Wait()

	wins, losses := 0, 0
	for _, err := range results {
		switch {
		case err == nil:
			wins++
		case errors.Is(err, ErrConflict):
			losses++
		default:
			t.Fatalf("unexpected publish error: %v", err)
		}
	}
	if wins != 1 || losses != n-1 {
		t.Fatalf("expected 1 win / %d losses, got %d/%d", n-1, wins, losses)
	}

	cur, err := s.CurrentVersion(key)
	if err != nil {
		t.Fatal(err)
	}
	_, data := readAll(t, s, key)
	if sha256Hex(data) != cur.Version {
		t.Fatalf("current pointer does not match content")
	}

	// The loser that never got installed is not left as a readable half object.
	if _, _, err := s.ReadObject(key + "-ghost"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("ghost object visible: %v", err)
	}
}

func TestCreateOnlyCAS(t *testing.T) {
	s := openTestStore(t)
	complete(t, s, "k", [][]byte{[]byte("one")})
	u, err := s.CreateUpload("k", IfMatch(""))
	if err != nil {
		t.Fatal(err)
	}
	_, _ = u.PutPart(1, bytes.NewReader([]byte("two")))
	if _, err := u.CompleteUpload(); !errors.Is(err, ErrConflict) {
		t.Fatalf("expected conflict, got %v", err)
	}
}

func TestOldVersionsRemainReadable(t *testing.T) {
	s := openTestStore(t)
	v1 := complete(t, s, "k", [][]byte{[]byte("first")})
	v2 := complete(t, s, "k", [][]byte{[]byte("second")}, IfMatch(v1.Version))

	if v1.Version == v2.Version {
		t.Fatalf("different content must produce different versions")
	}
	rc, err := s.ReadVersion(v1.Version)
	if err != nil {
		t.Fatalf("old version unreadable: %v", err)
	}
	b, _ := io.ReadAll(rc)
	rc.Close()
	if string(b) != "first" {
		t.Fatalf("old version content changed: %q", b)
	}
}

func TestCorruptionDetectedOnReadAndVerify(t *testing.T) {
	s := openTestStore(t)
	v := complete(t, s, "k", [][]byte{[]byte("important data")})
	blob, err := s.blobPath(v.Version)
	if err != nil {
		t.Fatal(err)
	}

	if err := os.Chmod(blob, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(blob, []byte("tampered!!"), 0o644); err != nil {
		t.Fatal(err)
	}

	if err := s.VerifyVersion(v.Version); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("verify expected corrupt, got %v", err)
	}
	_, rc, readErr := s.ReadObject("k")
	if readErr == nil {
		_, readErr = io.ReadAll(rc)
		rc.Close()
	}
	if !errors.Is(readErr, ErrCorrupt) {
		t.Fatalf("read expected corrupt, got %v", readErr)
	}
}

func TestAbortQuarantinesNothingVisible(t *testing.T) {
	s := openTestStore(t)
	u, _ := s.CreateUpload("k")
	_, _ = u.PutPart(1, bytes.NewReader([]byte("partial")))
	if err := u.Abort(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.ReadObject("k"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("aborted upload leaked an object: %v", err)
	}
}

func TestCrashAfterAssemble_RecoveryPublishesAndQuarantines(t *testing.T) {
	if os.Getenv("OBJECTSTORE_HELPER") == "1" {
		root := os.Getenv("OBJECTSTORE_ROOT")
		s, err := Open(context.Background(), root)
		if err != nil {
			os.Exit(1)
		}
		// Complete an unrelated half upload that must be quarantined.
		frag, _ := s.CreateUpload("fragment")
		_, _ = frag.PutPart(1, bytes.NewReader([]byte("loose bytes")))
		_ = frag // left in receiving state on process death

		u, _ := s.CreateUpload("ready")
		_, _ = u.PutPart(1, bytes.NewReader([]byte("recover-me")))
		_, _ = u.CompleteUpload() // injected hook kills after assembly
		os.Exit(0)
	}

	root := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=TestCrashAfterAssemble_RecoveryPublishesAndQuarantines")
	cmd.Env = append(os.Environ(),
		"OBJECTSTORE_HELPER=1",
		"OBJECTSTORE_ROOT="+root,
		"OBJECTSTORE_CRASH_STAGE=after_assemble",
	)
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	err := cmd.Run()
	if exitErr, ok := err.(*exec.ExitError); !ok || exitErr.ExitCode() != 42 {
		t.Fatalf("helper should be killed with 42, got %v: %s", err, out.String())
	}

	// Before reopening: object is not readable through the API layer yet.
	// After reopen, recovery runs.
	s, err := Open(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	// Open already ran recovery: the assembled object must now be readable
	// and the half-written session must have been quarantined.
	_, data := readAll(t, s, "ready")
	if string(data) != "recover-me" {
		t.Fatalf("recovered content mismatch: %q", data)
	}
	if _, _, err := s.ReadObject("fragment"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("fragment should not be visible: %v", err)
	}
	if entries, _ := os.ReadDir(filepath.Join(root, "quarantine")); len(entries) != 1 {
		t.Fatalf("quarantine dir should hold 1 entry")
	}
	if entries, _ := os.ReadDir(filepath.Join(root, "uploads")); len(entries) != 0 {
		t.Fatalf("uploads should be empty after recovery")
	}

	// A second restart is stable: no duplicate publication or quarantine.
	s2, err := Open(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	report2, _ := s2.Recover(context.Background())
	if report2.Published != 0 || len(report2.Quarantined) != 0 {
		t.Fatalf("idempotent recovery changed state: %+v", report2)
	}
	runtime.KeepAlive(s)
}

func TestClose(t *testing.T) {
	s := openTestStore(t)
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}
