# localobjstore

A dependency-free local object storage engine in Go. This first milestone
implements segmented writes, content checksumming and crash-safe atomic
publishing. It deliberately establishes the stable identity, versioning and
read model up front, so later cleanup policies or replication cannot change a
version that has already been published.

## Stability guarantees

- **Object identity**: callers address objects by an arbitrary key (e.g.
  `photos/2026/a.jpg`). Keys are mapped to a directory name with SHA-256, so
  there is no path-traversal surface; the mapping is an internal detail and
  the key never changes.
- **Immutable versions**: every successful publish creates a new directory
  `versions/<version-id>/` containing a `manifest.json` and read-only data
  parts. Version ids are `v<utc-nanos>-<12 hex random>`: unique, total-ordered
  and lexicographically time-sortable. Nothing ever mutates a published
  version, so future GC/replication work can reference them safely.
- **Stable read entry points**: `GetObject(key)` reads the winning current
  version; `GetVersion(key, versionID)` reads any specific historical version;
  `CurrentVersion` / `ListVersions` inspect metadata. These signatures are
  fixed from day one.
- **Deterministic concurrent-write winner**: same-key publishes serialise on
  a per-key `flock` plus an in-process mutex. After all versions are visible,
  `current` always points to the highest (newest) version id, so concurrent or
  cross-process publishers have an unambiguous, reproducible winner. Old
  versions remain readable. An optional optimistic-CAS condition
  (`expectedVersion`) lets callers turn a lost race into `ErrConflict`.

## Write path (no raw dumps into the object tree)

Both small objects (`PutObject`) and segmented uploads use the same pipeline:

1. `BeginMultipart` creates `staging/uploads/<upload-id>/` with a `meta.json`
   recording the key and a `parts/` directory.
2. `WritePart` streams a segment to a temp file, SHA-256 hashes while copying,
   fsyncs, sets it read-only and **atomically renames** it to
   `parts/000001`, `parts/000002`, ... Parts must be written strictly in order
   starting at 1; the returned receipt is the part checksum.
3. `Complete` re-hashes every staged part (catching on-disk corruption),
   assembles an immutable version under `staging/publish/<version-id>/`,
   writes `manifest.json`, fsyncs data + directories, and only then renames
   the directory into the object's `versions/` tree (the visibility point).
4. Finally it atomically updates the `current` pointer. A crash between steps
   3 and 4 leaves the version intact; recovery deterministically rebuilds the
   pointer to the highest valid version.

## Crash recovery and quarantine

`Open` takes a store-wide `flock` (so two processes cannot recover the same
root concurrently) and then:

- moves every leftover `staging/uploads/*` (an upload that never completed) to
  `quarantine/upload/<timestamp>-<id>/`;
- moves every leftover `staging/publish/*` (a publish that did not commit its
  rename) to `quarantine/publish/...`;
- structurally validates every published version; invalid versions are moved
  to `quarantine/version/...` instead of being deleted;
- repairs a missing or dangling `current` by selecting the highest valid
  version id; every action is returned in the `RecoveryReport`.

Readers can therefore never observe a half-written object: unpublished bytes
live only in staging and are isolated on the next start. With
`Options{VerifyOnOpen: true}`, recovery also hashes all bytes and quarantines
content-corrupted versions.

## Read-time corruption detection

Each part file is SHA-256 checksummed and the manifest records a whole-object
SHA-256 over the concatenation of parts. Readers verify lazily: a part is
hashed before any of its bytes are returned (sequential `Read`, `Seek`, and
`ReadAt` across part boundaries), and `Reader.Verify()` hashes everything up
front. Mismatches return `ErrCorrupted`.

## Layout

```
<root>/
  objects/<sha256(key)>/current                 # winning version id
  objects/<sha256(key)>/versions/<vid>/manifest.json
  objects/<sha256(key)>/versions/<vid>/data/000001, 000002, ...
  staging/uploads/<upload-id>/{meta.json,parts/}  # in-progress writes
  staging/publish/<vid>/...                        # assembled, pre-commit
  quarantine/{upload,publish,version}/<ts>-<name>/ # isolated residue
  locks/                                           # flocks
```

## Usage

```go
s, report, err := localobjstore.Open("/var/lib/objects", nil)

// small object
m, _ := s.PutObject("k", bytes.NewReader(data), nil)

// segmented
u, _ := s.BeginMultipart("big.bin")
u.WritePart(chunk1)
u.WritePart(chunk2)
m, _ = u.Complete(nil) // or &expected for optimistic CAS

r, _ := s.GetObject("big.bin")
defer r.Close()
if err := r.Verify(); err != nil { /* ErrCorrupted */ }
io.Copy(dst, r)
```

CLI:

```
go run ./cmd/objstore put    <root> <key> <file>
go run ./cmd/objstore get    <root> <key> [file|-]
go run ./cmd/objstore info   <root> <key>
go run ./cmd/objstore verify <root> <key>
```

## Tests

`go test ./...` (also run with `-race`) covers: small-file round trips,
multi-part round trips with cross-boundary `ReadAt`, strict part ordering,
8-way concurrent same-key publishing with a deterministic winner and surviving
old versions, optimistic CAS (`ErrConflict`), version immutability across
overwrites, interrupted-upload quarantine after a simulated crash, dangling
pointer rebuild, half-publish quarantine, read-time corruption detection
(including a middle part), startup quarantine with `VerifyOnOpen`, and a
subprocess that is **actively killed (`SIGKILL`) mid-write** to prove readers
can never see the partial object and the residue is isolated on restart.

## Retention policy and safe reclamation

Deletion is a two-phase operation. `DeleteObject(key)` commits a durable
`tombstone` and removes the `current` pointer; it never deletes payload bytes.
Content stays protected while anything references it:

- **durable snapshots**: `Snapshot(key, versionID)` writes a read-only
  reference under `refs/<sha256(key)>/` that survives process restarts and
  keeps the exact version readable (`GetVersion`) after the object is deleted
  or overwritten; `ReleaseSnapshot` drops it and `ListSnapshots` inspects them;
- **open readers**: `GetObject` / `GetVersion` register a transient per-version
  pin for the lifetime of the returned reader (`Close` releases it); a
  concurrent collector cannot reclaim bytes a reader is still serving.

`GC(ctx, opts)` performs the actual reclamation:

- versions of *deleted* objects are eligible only after `MinAge` since the
  deletion; non-current versions of *live* objects after `MinAge` since
  publication (default 24h; `MinAge: 0` disables the grace period);
- the current version is never touched; a version pinned by a snapshot or an
  open reader is never touched;
- staging (`uploads/`, `publish/`) is never scanned, so parts of an upload or
  a new version that has not finished publishing cannot be mistaken for
  garbage, even when GC runs concurrently with `WritePart`/`Complete`;
- collection is interruptible via `ctx`: on cancellation it returns the work
  done plus `ctx.Err()`. Nothing is deleted in place, so reopening and running
  GC again simply re-validates every candidate version and digest from disk;
- reclaimed and corrupt versions are moved to `quarantine/gc/` (the recycling
  bin), never hard-erased; with `GCOptions{VerifyContent: true}` each
  candidate is fully re-hashed and a digest mismatch is isolated as `corrupt`.

GC and publishing coordinate with the store-wide RWMutex in-process and with
per-key `flock`s across processes; the publish lock is taken *before* the new
version directory becomes visible, so no version is ever reachable but not yet
current. A lost optimistic-CAS race after the rename rolls the version back
out of the live tree instead of leaving an orphan.

## Recovery vs. corruption (never resurrected)

Recovery re-verifies every version on restart (structure always, bytes with
`VerifyOnOpen`) and re-checks snapshot references. A durable snapshot whose
version is gone is discarded. When the current pointer's target is quarantined
as corrupt the pointer is **removed**, never rebuilt onto an older version:
the object fails closed (`GetObject` errors) rather than silently serving
stale content. A pointer is rebuilt to the highest valid version only when it
is genuinely missing/dangling (crash between publish rename and pointer
commit). Tombstones left by a crash mid-delete are reconciled the same way.

```
<root>/
  objects/<sha256(key)>/{current,versions/<vid>/...}
  refs/<sha256(key)>/<ref-id>.json       # durable snapshots
  deleted/<sha256(key)>.json             # tombstones
  staging/{uploads,publish}/...          # never scanned by GC
  quarantine/{upload,publish,version,gc}/ # isolated residue / recycling bin
  locks/
```

CLI additions:

```
go run ./cmd/objstore delete     <root> <key>
go run ./cmd/objstore gc         <root> -
go run ./cmd/objstore snapshot   <root> <key> [version-id|-]
go run ./cmd/objstore snapshots  <root> <key>
go run ./cmd/objstore release    <root> <key> <snapshot-id>
go run ./cmd/objstore getversion <root> <key> <version-id>
```
