package localobjstore

import (
	"io"
	"time"
)

// PartInfo is the immutable record of one uploaded segment.
type PartInfo struct {
	Number int32  `json:"number"`
	Size   int64  `json:"size"`
	Sha256 string `json:"sha256"` // hex encoded SHA-256 of the segment bytes
}

// Manifest is the immutable, checksummed description of one published version.
// Once published a manifest never changes; future cleanup or replication work
// must treat it as read-only.
type Manifest struct {
	Key       string     `json:"key"`
	VersionID string     `json:"version_id"`
	CreatedAt time.Time  `json:"created_at"`
	Size      int64      `json:"size"`
	Sha256    string     `json:"sha256"` // hex SHA-256 over the concatenation of all parts
	Parts     []PartInfo `json:"parts"`
	// RestoredFrom records the source version when this manifest was created
	// by RestoreVersion; empty for ordinary publishes. The original corrupt
	// evidence in quarantine is never rewritten.
	RestoredFrom string `json:"restored_from,omitempty"`
	// GroupRestore records the restore-journal id when this manifest was
	// created by RestoreGroup, tying the version to one recovery point's
	// restore operation so members of different points are never mixed.
	GroupRestore string `json:"group_restore,omitempty"`
}

// FaultRecord is the durable fact that the current version of a key was
// quarantined as corrupt. It is written once and survives restarts until an
// explicit RestoreVersion, DeleteObject or a fresh publish resolves it, so
// reopening the store never changes the failure fact.
type FaultRecord struct {
	Key        string    `json:"key"`
	VersionID  string    `json:"version_id"` // quarantined corrupt version
	Reason     string    `json:"reason"`
	DetectedAt time.Time `json:"detected_at"`
}

// VersionStatus pairs a published version with its full verification result,
// so a human can choose a restore candidate with evidence.
type VersionStatus struct {
	Manifest Manifest
	Verified bool  // structural check and all checksums passed
	Err      error // nil when Verified
}

// ReadSeekCloser is the stable reading entry point exposed to readers.
type ReadSeekCloser interface {
	io.Reader
	io.ReaderAt
	io.Seeker
	io.Closer
	Manifest() Manifest
	// Verify reads every byte and returns ErrCorrupted if checksums do not match.
	Verify() error
}

// RecoveryItem reports one piece of residue handled during Open.
type RecoveryItem struct {
	Kind    string // "upload" | "publish" | "version" | "pointer"
	Key     string // object key when known
	Path    string // original location
	Action  string // "quarantined" | "pointer-rebuilt"
	Reason  string
	QuarDir string
}

// RecoveryReport summarizes what Open found after a crash or dirty shutdown.
type RecoveryReport struct {
	Items []RecoveryItem
}
