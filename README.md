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
  (c) held by an **open reader** — `GetObject`/`GetVersion` register the
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

## Group restore points

A **restore point** captures a group of mutually-referencing objects as
they were at one business commit: `CreateRestorePoint(name, members)`
records, for every member key, the exact immutable current version plus the
declared reference edges to other members. The capture serialises with
publishes (per-key locks in deterministic order), so the group is always
consistent; a key with an unfinished upload in this process is rejected
with `ErrRestoreInFlight` instead of being silently captured at its
pre-upload version. The point is committed with a single atomic rename, so
a crash mid-create leaves only a temp file that recovery discards — never a
half-written, half-visible restore point. Once confirmed a point is
immutable: later publishes create new versions that are not members, so
in-flight writes can never silently join a confirmed restore point.

- **Inspect**: `GetRestorePoint` / `ListRestorePoints` show the member
  versions and reference edges; `DiffRestorePoints(a, b)` returns a
  `RestorePointDiff` carrying both points verbatim plus member changes
  (added, removed and version-changed keys) and reference-edge changes:
  edges added in the second point, edges removed from it, and edges whose
  direction reversed (`a -> b` becoming `b -> a`). References are compared
  as sets, so the same topology declared in a different order reports no
  changes. The comparison reads only the two point files — it never opens
  member objects or writes anything — so it stays stable after a confirmed
  restore and keeps the recorded edges even when a member version is
  missing or corrupted, instead of mistaking unreadability for a missing
  relationship.
- **Preview before restore**: `InspectRestorePoint(name)` fully verifies
  every member version and classifies it as `readable`, `corrupted` or
  `missing`, without changing anything.
- **Full restore**: `RestorePoint(name, nil)` fails with
  `ErrRestoreMembersUnavailable` before touching anything unless every
  member is available and verified. Each member is switched to exactly the
  version recorded in the point — never a version from another point — so
  readers can never observe a group stitched together from different
  restore points. Tombstones and fault markers on restored keys are lifted
  because the restored current version is known-good.
- **Partial restore**: `RestorePoint(name, &RestoreOptions{AllowPartial:
  true})` restores the readable members and records a durable `gap` marker
  (with the reason) for every unavailable one; gapped keys are left exactly
  as they were. The record ends in state `completed-with-gaps`.
- **Durable journal**: every restore writes a journal under `restores/`
  before the first member becomes visible and rewrites it atomically as
  each member is confirmed. If the process stops mid-restore, the next
  `Open` marks the record `interrupted` (confirmed members keep their
  state and timestamps, pending ones stay pending) and reports it in the
  `RecoveryReport`; `ResumeRestore(id)` finishes exactly the recorded
  plan, turning members whose versions have since vanished into gaps.
  `ListRestores` / `RestoreStatus` expose the full history, and the
  quarantined corruption evidence is never rewritten.
- **Deletion**: `DeleteRestorePoint(name)` removes the point and reports,
  per member version, whether it is still occupied — by the current
  pointer, a pin, another restore point, an open reader or a pending group
  restore — or released for the next `CollectGarbage` pass. Versions
  referenced by any restore point (or needed by an unfinished restore) are
  retained by GC, so a restore source can never be reclaimed out from
  under a recovery.

## Object-entry impact analysis

An **impact analysis** answers "what does this business object pull into
the recoverable scope?" without touching anything:
`CreateImpactAnalysis(name, queries...)` takes one or two
`ImpactQuery{Point, Entries}` values — each restore point with its own
business entry objects — and traverses only the directed references the
points actually recorded.

- **Direct vs dependency impact**: with two queries, an object whose
  captured version was added, removed or replaced between the points is
  marked `Direct`; every other reached object is only affected through
  its dependencies. `Paths` shows every relationship path from an
  entry to each affected object.
- **Cycles preserved, scope counted once**: reference cycles appear in
  the recorded paths as a path whose last key repeats an earlier one,
  but traversal never follows a cycle twice, so every object enters the
  scope exactly once and the walk always terminates.
- **Common scope and missing dependencies**: with two queries the report
  lists the objects reachable from both sides' entries (with both
  versions and whether both are readable) plus the dependencies each
  side's scope is missing relative to the other.
- **Structure vs content availability**: the scope and paths come from
  the recorded edges alone; each node's `Status`
  (readable/corrupted/missing) is verified separately, so an unreadable
  member keeps its structural place in the graph.
- **Read-only and durable**: the query never modifies restore points or
  restores objects. The analysis is committed atomically under
  `analyses/` and is immutable: reopening the store shows the same
  analysis and restore points created later cannot change it.
  `GetImpactAnalysis` / `ListImpactAnalyses` read it back.
- **Export**: `ExportImpactScope(analysis, point, keys)` returns the
  recorded members of a selected scope and explicitly lists every
  recorded reference that crosses the selection boundary
  (`CrossBoundaryReferences`) because its target is not included.

CLI:

```
go run ./cmd/objstore impact-create <root> <name> <pointA>=<e1,e2> [<pointB>=<e3,...>]
go run ./cmd/objstore impact-list   <root>
go run ./cmd/objstore impact-show   <root> <name>
go run ./cmd/objstore impact-export <root> <name> <point> <k1,k2,...>
```

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
  trash/<ts>-<obj>-<vid>/                          # reclaimed, pending delete
  locks/                                           # flocks
  analyses/<sha256(name)>.json                     # saved impact analyses
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
