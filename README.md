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

If the invalid version is the one `current` points at, recovery does **not**
silently roll the object back to an older version: it quarantines the corrupt
version, removes the pointer and writes a durable `fault` marker
(`FaultRecord`). From then on `GetObject`/`CurrentVersion` fail with
`ErrFaulted`, while historical versions stay explicitly readable via
`GetVersion`. The marker makes the failure fact stable: reopening the store
any number of times reports the same fault instead of resurrecting old
content as current.

## Manual restore

A faulted object is recovered explicitly, by a human choosing a candidate on
evidence:

- `Fault(key)` returns the recorded `FaultRecord` (quarantined version id,
  reason, detection time).
- `InspectVersions(key)` lists every remaining version together with a full
  structural and checksum verification result (`VersionStatus.Verified/Err`).
- `RestoreVersion(key, versionID)` fully verifies the chosen version first;
  a failed verification returns an error and changes nothing. On success it
  assembles a **new** immutable version record (`RestoredFrom` names the
  source) and makes it current; the quarantined corrupt evidence is never
  rewritten or deleted. A fresh publish likewise lifts the fault, and
  `DeleteObject` on a faulted object resolves it into a tombstone.

While a fault is recorded, `CollectGarbage` retains all of the object's
remaining versions as restore candidates (pins, open readers and tombstones
keep their usual protection on top), so a pending manual recovery can never
lose its sources to reclamation.

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

## Retention and safe reclamation

Published versions are reference-counted by policy, never deleted eagerly:

- **References**: a version is retained while it is (a) the `current` version
  of its key, (b) held by a durable named **pin** (`PinVersion` /
  `UnpinVersion`, persisted under `refs/` so snapshots survive restarts), or
  (c) recorded in a confirmed **recovery point** (see below), or (d) held by
  an **open reader** — `GetObject`/`GetVersion` register the
  reader and `Close` releases it.
- **Delete is a tombstone**: `DeleteObject(key)` removes the `current`
  pointer and writes a durable `deleted` marker. The key disappears from
  `GetObject`/`CurrentVersion`, but every version stays readable via
  `GetVersion` and on disk until unreferenced. Crash recovery never rebuilds
  a pointer over a tombstone, so a deleted object cannot be resurrected; the
  next successful publish lifts the tombstone.
- **Garbage collection**: `CollectGarbage()` sweeps versions that are not
  current, not pinned and not being read. Reclamation is crash-safe: a
  version directory is first atomically renamed into `trash/` (the point of
  no return) and only then deleted, so an interrupted pass never leaves a
  half-deleted version inside the live tree. Leftover `trash/` entries are
  discarded by the next `Open` and reported in the `RecoveryReport`.
- **No interference with in-flight writes**: GC never touches `staging/`, so
  parts of an in-progress multipart upload and assembled-but-unpublished
  versions are out of scope. GC, `publish` and `DeleteObject` all serialise
  on the same per-key lock, and `publish` holds it from the version rename
  through the pointer update, so a version that is becoming current can never
  be swept by a concurrent GC pass.

## Group recovery points

A **recovery point** (`CreateRecoveryPoint`) confirms the complete state of a
set of mutually referencing objects at one business commit: for every member
key it durably records the exact current version id and whole-object
checksum, plus the declared reference edges to other members (every
referenced key must itself be a member). Capture serialises with the publish
locks, so a write that is still in flight either commits fully before the
capture — and is captured as a whole — or after it, and never leaks into an
already confirmed point. Points are immutable: reusing a name fails with
`ErrGroupExists`.

- **Inspection**: `ListRecoveryPoints` / `DescribeRecoveryPoint` show every
  member's captured version and the reference graph;
  `DiffRecoveryPoints(a, b)` reports added/removed members and per-member
  version and reference changes.
- **Retention**: versions recorded in a recovery point are retained across
  deletes and GC, exactly like pins. `DeleteRecoveryPoint` removes the point
  and reports, per former member version, whether it is now reclaimable or
  still occupied — and by whom (`current`, `pin:<name>`,
  `recovery-point:<name>`, `open-reader`).
- **Planned restore**: `PlanGroupRestore` verifies every recorded member
  against the live tree and reports `ready` / `missing` / `corrupted`
  before anything changes. `RestoreGroup(name, false)` fails with
  `ErrGroupIncomplete` unless every member is restorable; with
  `allowPartial` the caller explicitly accepts the gaps and each one is
  recorded as a durable `gap` line in the restore journal. Members are
  restored only from this point's own recorded versions — versions from
  other points are never substituted to fake a complete group.
- **Atomic visibility**: restore first verifies and stages *all* restorable
  members as new immutable versions (`RestoredFrom` plus the journal id in
  `GroupRestore`), and only then commits any `current` pointer, inside one
  publish critical section.
- **Interruption-proof**: every step is written to a durable journal under
  `groups/journals/`. If the process dies mid-restore, the next `Open`
  reconciles the journal against the live tree — committed members stay
  confirmed, unfinished ones are marked `incomplete` — reports a
  `group-restore` item in the `RecoveryReport`, and rewrites the journal
  as `interrupted`. Completed journals are kept as restore history, and
  quarantined corruption evidence is never touched.

## Layout

```
<root>/
  objects/<sha256(key)>/current                 # winning version id
  objects/<sha256(key)>/deleted                 # tombstone (when deleted)
  objects/<sha256(key)>/fault                   # recorded corruption fault (when current was quarantined)
  objects/<sha256(key)>/versions/<vid>/manifest.json
  objects/<sha256(key)>/versions/<vid>/data/000001, 000002, ...
  staging/uploads/<upload-id>/{meta.json,parts/}  # in-progress writes
  staging/publish/<vid>/...                        # assembled, pre-commit
  quarantine/{upload,publish,version}/<ts>-<name>/ # isolated residue
  refs/<sha256(pin-name)>.json                     # durable pins (snapshots)
  groups/<sha256(point-name)>.json                 # confirmed recovery points
  groups/journals/<restore-id>.json                # durable restore journals
  trash/<ts>-<obj>-<vid>/                          # reclaimed, pending delete
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
go run ./cmd/objstore delete <root> <key>
go run ./cmd/objstore fault      <root> <key>
go run ./cmd/objstore candidates <root> <key>
go run ./cmd/objstore restore    <root> <key> <version-id>
go run ./cmd/objstore pin    <root> <key> <version-id> <name>
go run ./cmd/objstore unpin  <root> <name>
go run ./cmd/objstore pins   <root>
go run ./cmd/objstore gc     <root>
go run ./cmd/objstore group-create  <root> <name> <key[:ref1,ref2]>...
go run ./cmd/objstore groups        <root>
go run ./cmd/objstore group-show    <root> <name>
go run ./cmd/objstore group-diff    <root> <name-a> <name-b>
go run ./cmd/objstore group-plan    <root> <name>
go run ./cmd/objstore group-restore <root> <name> [--partial]
go run ./cmd/objstore group-delete  <root> <name>
go run ./cmd/objstore group-journals <root>
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
Retention/GC tests cover: delete-with-pin retention and release, current
version surviving GC while superseded versions are reclaimed, open-reader
protection, GC racing an in-flight multipart upload and concurrent
publishers, abort-then-GC, interrupted-GC trash discarded on recovery, and
delete tombstones surviving restart (no pointer resurrection).
