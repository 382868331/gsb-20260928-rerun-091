package objectstore

import "errors"

var (
	// ErrNotFound indicates that the object, version or upload does not exist.
	ErrNotFound = errors.New("objectstore: not found")

	// ErrConflict indicates a concurrent publish beat an ExpectedVersion compare.
	ErrConflict = errors.New("objectstore: concurrent publish conflict")

	// ErrCorrupt indicates that stored bytes do not match their checksum.
	ErrCorrupt = errors.New("objectstore: stored content is corrupt")

	// ErrInvalidPart indicates a bad part number, checksum or upload state.
	ErrInvalidPart = errors.New("objectstore: invalid part")

	// ErrAlreadyFinalized indicates that an upload was already completed.
	ErrAlreadyFinalized = errors.New("objectstore: upload already finalized")
)
