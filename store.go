// Package objectstore implements a local, dependency-free object storage
// engine with multipart uploads, content-addressed immutable versions,
// atomic publishing and crash recovery.
//
// On-disk layout:
//
//	root/
//	  objects/ab/cd/<sha256>          immutable version blobs (0444)
//	  keys/<hex(key)>/pointer         current version pointer (atomically renamed)
//	  keys/<hex(key)>/lock            cross-process publish lock
//	  uploads/<uploadID>/
//	    meta.json                     upload/state metadata
//	    parts/00000001                immutable part files
//	    staged                        assembled object before publish
//	  quarantine/<ts>-<name>/         fragments isolated by recovery
package objectstore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	dirPerm  = 0o755
	filePerm = 0o644
	blobPerm = 0o444
)

// Store is a local object storage engine rooted at a single directory.
type Store struct {
	root string

	mu sync.Mutex // protects in-flight upload registry below

	// hook is used by crash tests to kill the process at a precise point.
	hook func(stage string)
}

// Open opens (creating if needed) a store at root and runs crash recovery:
// previously completed objects are reclaimed and leftover fragments are
// quarantined.
func Open(ctx context.Context, root string) (*Store, error) {
	s := &Store{root: root}
	// Test/crash-injection hook: when set, the process exits abruptly at the
	// named internal stage, simulating a power loss with no cleanup.
	if stage := os.Getenv("OBJECTSTORE_CRASH_STAGE"); stage != "" {
		s.hook = func(reached string) {
			if reached == stage {
				os.Exit(42)
			}
		}
	}
	for _, dir := range []string{"objects", "keys", "uploads", "quarantine"} {
		if err := os.MkdirAll(filepath.Join(root, dir), dirPerm); err != nil {
			return nil, err
		}
	}
	if err := syncDir(root); err != nil {
		return nil, err
	}
	// Cross-process gate: only one process performs startup reconciliation at
	// a time, and a process that opens while another is alive will never
	// mistake that process's active sessions for crash residue. The lock is
	// held for the process lifetime and released automatically on exit.
	// Startup gate: serialize recovery across processes so a process booting
	// up cannot mistake another process's in-flight sessions for crash
	// residue. It is released after reconciliation; per-key flock + heartbeat
	// grace periods govern ongoing concurrent publishes.
	lk, err := acquireLock(filepath.Join(root, "store.lock"))
	if err != nil {
		return nil, err
	}
	if _, err := s.Recover(ctx); err != nil {
		lk.release()
		return nil, err
	}
	lk.release()
	return s, nil
}

// Root returns the store root directory.
func (s *Store) Root() string { return s.root }

// Close releases store resources. The engine is stateless between requests
// apart from advisory locks released by the OS, so Close is currently a
// stable no-op hook for future background work.
func (s *Store) Close() error { return nil }

func (s *Store) path(parts ...string) string {
	return filepath.Join(append([]string{s.root}, parts...)...)
}

func blobRelPath(version string) (string, error) {
	if len(version) != 64 {
		return "", errors.New("objectstore: malformed version id")
	}
	if _, err := hex.DecodeString(version); err != nil {
		return "", errors.New("objectstore: malformed version id")
	}
	return filepath.Join("objects", version[0:2], version[2:4], version), nil
}

func (s *Store) blobPath(version string) (string, error) {
	rel, err := blobRelPath(version)
	if err != nil {
		return "", err
	}
	return s.path(rel), nil
}

func keyDir(key string) string {
	return filepath.Join("keys", hex.EncodeToString([]byte(key)))
}

func writeFileAtomic(path string, data []byte, perm os.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(path), dirPerm); err != nil {
		return err
	}
	tmp := path + ".tmp-" + randomSuffix()
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, filePerm)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Chmod(perm); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return err
	}
	return syncDir(filepath.Dir(path))
}

func syncDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	err = f.Sync()
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	return err
}

// hashReader copies r to discard while computing its SHA-256 and length.
func hashReader(r io.Reader) (string, int64, error) {
	h := sha256.New()
	n, err := io.Copy(h, r)
	if err != nil {
		return "", 0, err
	}
	return hex.EncodeToString(h.Sum(nil)), n, nil
}

func nowUTC() time.Time { return time.Now().UTC() }
