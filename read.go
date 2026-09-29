package objectstore

import (
	"crypto/sha256"
	"encoding/hex"
	"hash"
	"io"
	"os"
)

// CurrentVersion returns the current published version of key, or ErrNotFound.
func (s *Store) CurrentVersion(key string) (Version, error) {
	p, err := s.currentPointer(key)
	if err != nil {
		return Version{}, err
	}
	if p == nil {
		return Version{}, ErrNotFound
	}
	return Version{Key: key, Version: p.Version, Size: p.Size, PublishedAt: p.PublishedAt}, nil
}

// ReadObject reads the current version of key and verifies every byte
// against its content-derived checksum. It returns ErrCorrupt if stored
// bytes were damaged on disk.
func (s *Store) ReadObject(key string) (Version, io.ReadCloser, error) {
	p, err := s.currentPointer(key)
	if err != nil {
		return Version{}, nil, err
	}
	if p == nil {
		return Version{}, nil, ErrNotFound
	}
	v := Version{Key: key, Version: p.Version, Size: p.Size, PublishedAt: p.PublishedAt}
	rc, err := s.openVerifiedBlob(p.Version, p.Size)
	if err != nil {
		return Version{}, nil, err
	}
	return v, rc, nil
}

// ReadVersion reads any previously published (immutable) version blob by ID.
// Old versions stay readable after the key pointer advances: the version ID
// is the SHA-256 of the content and blobs are never rewritten.
func (s *Store) ReadVersion(version string) (io.ReadCloser, error) {
	return s.openVerifiedBlob(version, -1)
}

// VerifyVersion checks that a version blob is byte-intact.
func (s *Store) VerifyVersion(version string) error {
	path, err := s.blobPath(version)
	if err != nil {
		return err
	}
	got, _, err := hashFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return ErrNotFound
		}
		return err
	}
	if got != version {
		return ErrCorrupt
	}
	return nil
}

func (s *Store) openVerifiedBlob(version string, expectedSize int64) (io.ReadCloser, error) {
	path, err := s.blobPath(version)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	if expectedSize >= 0 && info.Size() != expectedSize {
		f.Close()
		return nil, ErrCorrupt
	}
	return &verifiedReader{file: f, version: version, size: info.Size(), hasher: sha256.New()}, nil
}

type verifiedReader struct {
	file    *os.File
	version string
	size    int64
	hasher  hash.Hash
	read    int64
}

func (r *verifiedReader) Read(p []byte) (int, error) {
	n, err := r.file.Read(p)
	if n > 0 {
		r.hasher.Write(p[:n])
		r.read += int64(n)
	}
	if err == io.EOF {
		if r.read != r.size || hex.EncodeToString(r.hasher.Sum(nil)) != r.version {
			return n, ErrCorrupt
		}
	}
	return n, err
}

func (r *verifiedReader) Close() error { return r.file.Close() }
