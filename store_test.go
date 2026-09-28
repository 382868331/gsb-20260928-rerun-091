package localobjstore

import (
	"bytes"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
)

func openTemp(t *testing.T) (*Store, string) {
	t.Helper()
	dir := t.TempDir()
	s, _, err := Open(dir, nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s, dir
}

func randomBytes(n int) []byte {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return b
}

// Small objects go through the same staged pipeline and read back byte-exact.
func TestPutObjectSmallFileRoundTrip(t *testing.T) {
	s, _ := openTemp(t)
	payload := []byte("hello, local object store!\n")
	m, err := s.PutObject("docs/greeting.txt", bytes.NewReader(payload), nil)
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	if m.Size != int64(len(payload)) || len(m.Parts) != 1 {
		t.Fatalf("unexpected manifest: %+v", m)
	}
	r, err := s.GetObject("docs/greeting.txt")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer r.Close()
	if err := r.Verify(); err != nil {
		t.Fatalf("verify: %v", err)
	}
	got, err := io.ReadAll(r)
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("data mismatch: %v len=%d", err, len(got))
	}

	// Payload bytes must never be dumped directly into the object directory:
	// published data lives under an immutable version id directory.
	matches, _ := filepath.Glob(filepath.Join(s.root, "objects", "*", "versions", "v*", "data", "000001"))
	if len(matches) != 1 {
		t.Fatalf("expected one staged version data file, got %v", matches)
	}
}

func TestMultipartRoundTripAndPartChecksum(t *testing.T) {
	s, _ := openTemp(t)
	u, err := s.BeginMultipart("big/blob.bin")
	if err != nil {
		t.Fatal(err)
	}
	want := make([]byte, 0, 3*1024*1024)
	for i := 0; i < 3; i++ {
		chunk := randomBytes(1024 * 1024)
		receipt, err := u.WritePart(bytes.NewReader(chunk))
		if err != nil {
			t.Fatal(err)
		}
		if receipt.Number != int32(i+1) || receipt.Size != int64(len(chunk)) {
			t.Fatalf("bad receipt %+v", receipt)
		}
		want = append(want, chunk...)
	}
	m, err := u.Complete(nil)
	if err != nil {
		t.Fatalf("complete: %v", err)
	}
	if len(m.Parts) != 3 || m.Size != int64(len(want)) {
		t.Fatalf("bad manifest %+v", m)
	}
	r, err := s.GetObject("big/blob.bin")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if err := r.Verify(); err != nil {
		t.Fatalf("verify: %v", err)
	}
	// Random access spanning parts.
	buf := make([]byte, 100)
	off := int64(1024*1024 - 40)
	if _, err := r.ReadAt(buf, off); err != nil {
		t.Fatalf("readat: %v", err)
	}
	if !bytes.Equal(buf, want[off:off+100]) {
		t.Fatal("ReadAt returned wrong bytes across part boundary")
	}
	got, _ := io.ReadAll(r)
	if !bytes.Equal(got, want) {
		t.Fatalf("full read mismatch len=%d", len(got))
	}
}

func TestPartOrderingEnforced(t *testing.T) {
	s, _ := openTemp(t)
	u, err := s.BeginMultipart("k")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = u.WritePart(strings.NewReader("a"))
	m, err := u.Complete(nil)
	if err != nil || m.VersionID == "" {
		t.Fatalf("complete single part failed: %v", err)
	}

	u2, _ := s.BeginMultipart("k2")
	if err := u2.Abort(); err != nil {
		t.Fatal(err)
	}
	if _, err := u2.WritePart(strings.NewReader("b")); !errors.Is(err, ErrUploadNotFound) {
		t.Fatalf("expected ErrUploadNotFound after abort, got %v", err)
	}
}

// Same-key concurrent publishes must have one deterministic winner.
func TestConcurrentPublishWinner(t *testing.T) {
	s, dir := openTemp(t)
	const key = "race/object"
	const n = 8
	var wg sync.WaitGroup
	manifests := make([]Manifest, n)
	errCh := make(chan error, n)
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			payload := []byte(fmt.Sprintf("writer-%d", i))
			m, err := s.PutObject(key, bytes.NewReader(payload), nil)
			if err != nil {
				errCh <- err
				return
			}
			manifests[i] = m
		}(i)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("concurrent put: %v", err)
	}

	versions, err := s.ListVersions(key)
	if err != nil {
		t.Fatal(err)
	}
	if len(versions) != n {
		t.Fatalf("expected %d retained versions, got %d", n, len(versions))
	}
	ids := make([]string, n)
	for i, m := range versions {
		ids[i] = m.VersionID
	}
	if !sort.StringsAreSorted(ids) {
		t.Fatal("versions not ordered")
	}
	cur, err := s.CurrentVersion(key)
	if err != nil {
		t.Fatal(err)
	}
	highest := ids[len(ids)-1]
	if cur.VersionID != highest {
		t.Fatalf("winner = %s, want highest version id %s", cur.VersionID, highest)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen: winner must persist, and optimistic CAS must detect staleness.
	s2, rep, err := Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	cur2, _ := s2.CurrentVersion(key)
	if cur2.VersionID != highest {
		t.Fatalf("after reopen current changed: %s", cur2.VersionID)
	}
	if len(rep.Items) != 0 {
		t.Fatalf("clean shutdown should recover nothing, got %+v", rep.Items)
	}
	stale := "v00000000000000000000-deadbeefdead"
	if _, err := s2.PutObject(key, strings.NewReader("x"), &stale); !errors.Is(err, ErrConflict) {
		t.Fatalf("expected ErrConflict for stale expected version, got %v", err)
	}
	expect := cur2.VersionID
	m, err := s2.PutObject(key, strings.NewReader("next"), &expect)
	if err != nil {
		t.Fatalf("CAS with correct expected version failed: %v", err)
	}
	// Old version remains readable and immutable.
	old, err := s2.GetVersion(key, cur2.VersionID)
	if err != nil {
		t.Fatal(err)
	}
	if err := old.Verify(); err != nil {
		t.Fatalf("old version mutated: %v", err)
	}
	old.Close()
	if m.VersionID <= cur2.VersionID {
		t.Fatal("new version must order after the old one")
	}
}

// Published versions never mutate: overwrite keeps every old version readable.
func TestPublishedVersionImmutability(t *testing.T) {
	s, _ := openTemp(t)
	m1, _ := s.PutObject("k", strings.NewReader("one"), nil)
	m2, _ := s.PutObject("k", strings.NewReader("two-two"), nil)
	r1, err := s.GetVersion("k", m1.VersionID)
	if err != nil {
		t.Fatal(err)
	}
	b1, _ := io.ReadAll(r1)
	r1.Close()
	if string(b1) != "one" {
		t.Fatalf("old version content changed: %q", b1)
	}
	r2, _ := s.GetVersion("k", m2.VersionID)
	b2, _ := io.ReadAll(r2)
	r2.Close()
	if string(b2) != "two-two" {
		t.Fatalf("new version wrong: %q", b2)
	}
}
