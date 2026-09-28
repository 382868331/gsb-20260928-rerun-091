// Package localobjstore is a dependency-free local object storage engine.
//
// Data model stability guarantees (fixed from day one):
//   - An object is addressed by its caller-supplied key; the on-disk layout
//     (a SHA-256 of the key) is an internal detail and never changes the key.
//   - Every successful publish creates an immutable, checksummed version with
//     a unique, time-ordered, lexicographically sortable version id.
//   - Readers always go through GetObject / GetVersion; later cleanup or
//     replication features must not mutate a published version directory.
package localobjstore

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	manifestName = "manifest.json"
	currentName  = "current"
	partsDirName = "parts"
	dataDirName  = "data"
)

// Options controls Open behaviour.
type Options struct {
	// VerifyOnOpen hashes every byte of every published version during recovery.
	// By default only structural integrity is checked; content checksums are
	// verified when a reader touches the data (or via Reader.Verify).
	VerifyOnOpen bool
}

// Store is the engine handle. It is safe for concurrent use.
type Store struct {
	root       string
	lock       *flockFile
	pubMu      sync.Mutex // serializes publish CAS within the process
	upMu       sync.Mutex
	uploads    map[string]*Upload
	verifyOpen bool
}

// Open (or creates) a store at root and runs crash recovery before returning.
func Open(root string, opts *Options) (*Store, RecoveryReport, error) {
	for _, d := range []string{"objects", filepath.Join("staging", "uploads"), filepath.Join("staging", "publish"), "quarantine", "locks"} {
		if err := os.MkdirAll(filepath.Join(root, d), 0o755); err != nil {
			return nil, RecoveryReport{}, err
		}
	}
	lock, err := acquireFlock(filepath.Join(root, "locks", "store.lock"))
	if err != nil {
		return nil, RecoveryReport{}, err
	}
	s := &Store{
		root:       root,
		lock:       lock,
		uploads:    make(map[string]*Upload),
		verifyOpen: opts != nil && opts.VerifyOnOpen,
	}
	report, err := s.recover()
	if err != nil {
		lock.release()
		return nil, RecoveryReport{}, err
	}
	return s, report, nil
}

// Close releases the process lock.
func (s *Store) Close() error {
	if s.lock != nil {
		s.lock.release()
		s.lock = nil
	}
	return nil
}

func objectID(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:])
}

func (s *Store) objectDir(key string) string {
	return filepath.Join(s.root, "objects", objectID(key))
}

func (s *Store) versionsDir(key string) string {
	return filepath.Join(s.objectDir(key), "versions")
}

// newVersionID is UTC nanosecond time (fixed-width, lex sortable) plus 12 hex
// random nibbles so concurrent publishes never collide and always have a
// total, time-ordered ordering.
func newVersionID() (string, error) {
	var b [6]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return fmt.Sprintf("v%020d-%x", time.Now().UTC().UnixNano(), b[:]), nil
}

func newUploadID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

// readCurrent returns "" (with no error) when no version is published yet.
func (s *Store) readCurrent(key string) (string, error) {
	data, err := os.ReadFile(filepath.Join(s.objectDir(key), currentName))
	if errors.Is(err, os.ErrNotExist) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(data)), nil
}

func (s *Store) writeCurrent(key, versionID string) error {
	return writeFileAtomic(s.objectDir(key), currentName, []byte(versionID+"\n"), 0o444)
}

func manifestPath(key, versionID string) string {
	return filepath.Join("objects", objectID(key), "versions", versionID, manifestName)
}

// loadManifest reads and structurally validates a published manifest.
func (s *Store) loadManifest(key, versionID string) (Manifest, error) {
	var m Manifest
	data, err := os.ReadFile(filepath.Join(s.root, manifestPath(key, versionID)))
	if err != nil {
		return m, err
	}
	if err := json.Unmarshal(data, &m); err != nil {
		return m, err
	}
	if m.VersionID != versionID || m.Key != key || len(m.Parts) == 0 {
		return m, fmt.Errorf("%w: manifest identity mismatch", ErrCorrupted)
	}
	var total int64
	for _, p := range m.Parts {
		total += p.Size
	}
	if total != m.Size {
		return m, fmt.Errorf("%w: manifest size mismatch", ErrCorrupted)
	}
	return m, nil
}

// validateVersionStructure checks manifest + part files exist with the right
// sizes. Content hashes are checked by readers (or VerifyOnOpen).
func (s *Store) validateVersionStructure(key, versionID string) error {
	m, err := s.loadManifest(key, versionID)
	if err != nil {
		return err
	}
	for i, p := range m.Parts {
		if p.Number != int32(i+1) {
			return fmt.Errorf("%w: part numbering gap", ErrCorrupted)
		}
		path := filepath.Join(s.versionsDir(key), versionID, dataDirName, partFileName(p.Number))
		st, err := os.Stat(path)
		if err != nil {
			return fmt.Errorf("%w: missing part %d: %v", ErrCorrupted, p.Number, err)
		}
		if st.Size() != p.Size {
			return fmt.Errorf("%w: part %d size mismatch", ErrCorrupted, p.Number)
		}
	}
	return nil
}

func partFileName(number int32) string {
	return fmt.Sprintf("%06d", number)
}

// listValidVersions returns structurally valid version ids sorted oldest→newest.
func (s *Store) listValidVersions(key string) ([]string, map[string]error, error) {
	entries, err := os.ReadDir(s.versionsDir(key))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, err
	}
	var ids []string
	bad := make(map[string]error)
	for _, e := range entries {
		if !e.IsDir() || !strings.HasPrefix(e.Name(), "v") {
			continue
		}
		vid := e.Name()
		if err := s.validateVersionStructure(key, vid); err != nil {
			bad[vid] = err
			continue
		}
		ids = append(ids, vid)
	}
	sort.Strings(ids)
	return ids, bad, nil
}
