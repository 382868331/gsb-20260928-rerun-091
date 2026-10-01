package localobjstore

import "errors"

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

	// ErrFaulted indicates the current version was quarantined as corrupt and
	// the object awaits an explicit RestoreVersion; historical versions stay
	// readable via GetVersion but none masquerades as current.
	ErrFaulted = errors.New("localobjstore: current version quarantined as corrupt, explicit restore required")
)
