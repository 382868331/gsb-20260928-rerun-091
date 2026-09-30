package localobjstore

import (
	"encoding/json"
	"errors"
	"fmt"
)

var (
	// ErrNotFound indicates that the object or the requested version does not exist.
	ErrNotFound = errors.New("localobjstore: object not found")

	// ErrConflict indicates a concurrent publish won the race for the same key.
	ErrConflict = errors.New("localobjstore: concurrent publish conflict")

	// ErrCorrupted indicates that stored bytes no longer match their recorded checksums.
	ErrCorrupted = errors.New("localobjstore: stored object is corrupted")

	// ErrUploadNotFound indicates an unknown or aborted multipart upload id.
	ErrUploadNotFound = errors.New("localobjstore: upload not found")

	// ErrInvalidParts indicates that the completed part list is not 1..N contiguous.
	ErrInvalidParts = errors.New("localobjstore: invalid part list")
)

func errNotFound(key string) error { return fmt.Errorf("%w: %s", ErrNotFound, key) }

func jsonUnmarshal(b []byte, v any) error { return json.Unmarshal(b, v) }
