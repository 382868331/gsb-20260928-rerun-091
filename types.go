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
	Kind    string // "upload" | "publish" | "version" | "pointer" | "ref"
	Key     string // object key when known
	Path    string // original location
	Action  string // "quarantined" | "pointer-rebuilt" | "pointer-removed" | "discarded"
	Reason  string
	QuarDir string
}

// RecoveryReport summarizes what Open found after a crash or dirty shutdown.
type RecoveryReport struct {
	Items []RecoveryItem
}

// Ref is a durable (snapshot) or transient (open reader) reference that keeps a
// specific immutable version alive against garbage collection.
type Ref struct {
	ID        string    `json:"id"`
	Key       string    `json:"key"`
	VersionID string    `json:"version_id"`
	CreatedAt time.Time `json:"created_at"`
}

// Tombstone marks an object whose current pointer was deleted. The versions
// stay on disk until every reference is released and retention has elapsed;
// only then does GC reclaim them.
type Tombstone struct {
	Key         string    `json:"key"`
	DeletedAt   time.Time `json:"deleted_at"`
	LastVersion string    `json:"last_version"`
}

// GCOptions tunes a garbage collection pass.
type GCOptions struct {
	// MinAge is the retention period: live non-current versions (measured from
	// their publish time) and deleted versions (measured from the deletion
	// time) are only eligible once older than this duration. Zero enforces no
	// grace period; the Store-level default (24h) applies when nil options are
	// passed.
	MinAge time.Duration
	// VerifyContent re-hashes every candidate version's bytes before deciding
	// its fate. A version whose content digest no longer matches is isolated
	// instead of being reclaimed or relied upon.
	VerifyContent bool
}

// GCItem reports one version handled by a garbage collection pass.
type GCItem struct {
	Key       string // object key
	VersionID string
	Kind      string // "expired" | "deleted" | "corrupt"
	Action    string // "reclaimed" | "quarantined"
	Reason    string
	QuarDir   string
}

// GCReport summarizes a GC run. A run interrupted via context cancellation
// returns everything completed so far plus ctx.Err(); rerunning Open/GC after
// the interruption re-validates versions and digests from scratch.
type GCReport struct {
	Items []GCItem
}
