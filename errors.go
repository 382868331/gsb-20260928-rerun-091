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

	// ErrGroupExists indicates a recovery point with the same name is already
	// confirmed; recovery points are immutable once confirmed.
	ErrGroupExists = errors.New("localobjstore: recovery point already exists")

	// ErrGroupIncomplete indicates a group restore was requested while some
	// recorded members are missing or corrupt and partial restore was not
	// explicitly accepted. Nothing is changed in this case.
	ErrGroupIncomplete = errors.New("localobjstore: recovery point has missing or corrupt members")
)
