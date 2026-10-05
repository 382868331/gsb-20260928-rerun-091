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

	// ErrRestorePointExists indicates a restore point name is already taken;
	// confirmed restore points are immutable and cannot be overwritten.
	ErrRestorePointExists = errors.New("localobjstore: restore point already exists")

	// ErrRestoreInFlight indicates a member key has an unfinished upload in
	// this process; the restore point would silently capture pre-upload
	// content, so creation is refused instead.
	ErrRestoreInFlight = errors.New("localobjstore: member object is being written")

	// ErrRestoreMembersUnavailable indicates a full group restore was
	// requested while some members are missing or corrupted; nothing was
	// changed. Inspect the restore point and either fix the members or
	// explicitly request a partial restore with gap markers.
	ErrRestoreMembersUnavailable = errors.New("localobjstore: restore point members unavailable")

	// ErrImpactAnalysisExists indicates an impact analysis name is already
	// taken; saved analyses are immutable and cannot be overwritten.
	ErrImpactAnalysisExists = errors.New("localobjstore: impact analysis already exists")
)
