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

// CorruptionRecord is the durable fact that a current version was damaged.
type CorruptionRecord struct {
	Key                string     `json:"key"`
	CorruptedVersionID string     `json:"corrupted_version_id"`
	QuarantinePath     string     `json:"quarantine_path"`
	Reason             string     `json:"reason"`
	DetectedAt         time.Time  `json:"detected_at"`
	Active             bool       `json:"active"`
	SourceVersionID    string     `json:"source_version_id,omitempty"`
	RestoredVersionID  string     `json:"restored_version_id,omitempty"`
	RestorePendingID   string     `json:"restore_pending_id,omitempty"`
	ResolvedAt         *time.Time `json:"resolved_at,omitempty"`
}

// RecoveryCandidate describes a historical version that may be restored.
type RecoveryCandidate struct {
	Manifest Manifest
	Verified bool
	Error    string
}
