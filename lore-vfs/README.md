# lore-vfs

Experimental common filesystem code for Lore, with Linux FUSE adapters. The
explicit experimental `lore mount` command supports a pinned read-only revision
or an opt-in writable working tree with private recovery storage. Both preserve
repository links. Writable startup supports local/offline import and explicit
online file hydration. The revision/branch browser for CI is deferred.

## Components

- `core`: platform-independent lookup, inode/handle tables, bounded directory
  pagination, reads, shutdown and digest lookup.
- `revision`: a backend using an existing authorized `RepositoryContext`, pinned
  `State` and verified content reads. Nested links keep their own repository,
  revision and source-root context. A denied link remains visible as an opaque
  mountpoint; its contents are inaccessible.
- `linux` (Linux only): read-only FUSE mount, callbacks, xattr translation and
  explicit unmount. Uses `fuser` 0.16.0 without a libfuse link dependency.
- `transition`: bounded read-only generations, pinned open handles and coordinated
  cache invalidation, available to embedding callers.
- `digest`: bounded whole-file SHA-256 computation and generation-checked digest
  publication.
- `digest_index`: optional, versioned, removable SHA-256 records keyed by owning
  repository, full immutable Address, whole-file size and digest algorithm.
- `overlay`: bounded per-file sparse writes over a pinned backend, with an explicit
  access gate and private external storage. Writable admission revokes the file's
  digest. Writable mappings are unsupported.
- `storage_budget`: a shared append-byte limit for private stores. Wrap every
  file store, including recovery stores, with the same owner budget. Failed,
  short and cancelled writes retain their reservations; unlink and truncate do
  not reclaim them. Restore must account for the checkpoint's historical
  reservations before admitting operations. It counts requested bytes, not
  filesystem block overhead.
- `namespace`: bounded merged directories, stable local inode identities, file and
  directory creation, removal and rename, with retained open-file identities.
  Names follow Lore's Unicode lowercase matching without additional normalization.
- `recovery`: private ownership locks and bounded full checkpoints containing
  namespace metadata and dirty file extents. Successful publication syncs the
  checkpoint, atomically replaces it and syncs its directory. Ordinary mutations
  remain volatile until a checkpoint succeeds.
- `provider`: an injected Lore stage/commit provider over the mutable namespace.
  Exclusive operation leases reject active writers and fence cancelled work.
  Requested ingestion content is exported lazily into bounded private storage.
- `linux_working`: an experimental writable mount over that same namespace, with
  bounded handles, directory snapshots and private append stores. Direct I/O and
  zero cache TTLs keep writes visible; writable mappings are rejected. File and
  directory `fsync` publish coherent recovery checkpoints.

Open directory handles retain their captured names and read-authorization binding.
Each page rechecks permission against the captured directory, parent and child
identities, including after rename or removal. Retaining a handle does not retain
permission or freeze the namespace.

File handles retain the backend and effective permission binding selected at
open, including immutable base reads and metadata operations. Linux uses the
supplied handle for `getattr`; invalid or mismatched handles are rejected.
Reads, writes and handle metadata recheck the retained policy, so revocation
still takes effect. Cleanup remains possible after revocation.

The real Lore backend reports `user.sha256` only when a configured index contains a
verified record. It does not trust legacy path metadata or hash file contents while
answering an xattr request. Missing, incompatible or corrupt records are safe misses.
The attribute is exactly 32 raw SHA-256 bytes; mounted fixture tests exercise the
adapter's wire contract.

## Linux command

```sh
lore --repository /source mount --experimental --revision main@LATEST /empty/view
```

The revision is resolved once. The command stays in the foreground until interrupted
or externally unmounted. It requires an existing empty mount directory, rejects
repository metadata paths and existing mounts, and does not hold repository command
locks for the mount lifetime. `--json` and `--dry-run` are unsupported.

Add `--digest-cache /private/digests` to use existing verified SHA-256 records. The
cache must be owned by the invoking user and not writable by other users; a new
directory is created with mode 0700. This read-only mount does not backfill missing
digests.
Cache and mount directories cannot contain one another. Without this option,
digest caching is disabled. Other platforms reject this command explicitly.

### Writable local instance

On Linux, the service can own a separate writable instance imported from a normal
local checkout. The source checkout is read without converting its metadata or
changing its working files. The selected revision and linked pins are copied
into newly owned local stores, with a default 16 GiB import byte limit and
bounded scope, node and fragment counts. The fragment count is checked after the
existing collector builds its address list; these limits are not a strict peak
RAM cap. This offline import remains separate from the explicit online contents
mode below. Checkout conversion and enabled shared stores are unsupported. The
read-only mount path keeps its existing behavior.

```sh
mkdir -m 700 /tmp/lore-working-metadata
mkdir /tmp/lore-working-view
lore --offline mount --experimental --writable --source /source \
  --revision main@LATEST --metadata /tmp/lore-working-metadata \
  --backing /tmp/lore-working-backing /tmp/lore-working-view
```

For fresh creation, the metadata directory must already exist, be empty and
owned by the invoking user with mode 0700. The backing path must be fresh. Metadata, backing, source,
mountpoint and any digest cache must satisfy the command's overlap checks.
On a fresh writable import, `--digest-cache /tmp/lore-working-digests` explicitly
hashes verified immutable files into the optional digest index before publication.
This bounded setup does not materialize private overlay files or perform hashing
inside xattr lookups. Reuse reads existing records; it does not backfill them.
Local edits use normal checkout permissions; editing does not request an online
write grant. File fsync and clean service retirement persist private overlay
bytes without committing them to the source repository.

Use `lore service stop` to cleanly retire **all** service-owned mounts. Reopen
with the same metadata and an explicit current source read policy:

```sh
lore --offline mount --experimental --writable --reuse --source /source \
  --metadata /tmp/lore-working-metadata /tmp/lore-working-view
```

Reuse reads the recorded backing path and checkpoint selection; `--source`
does not redirect stored data or replace linked pins. It must have the same root
repository identity and currently authorize the linked reads. Nonlinked instances
may omit `--source`; link traversal is then denied. Exact old/new transition
recovery is reconciled against the actual strict anchors before admission;
missing, corrupt or unprovable recovery data fails closed rather than creating
replacement state.
For on-demand remote contents, use an online source checkout with an authenticated
remote and add `--remote-contents` without `--offline`:

```sh
mkdir -m 700 /tmp/lore-online-metadata
mkdir /tmp/lore-online-view
lore mount --experimental --writable --remote-contents --source /source \
  --revision main@LATEST --metadata /tmp/lore-online-metadata \
  --backing /tmp/lore-online-backing /tmp/lore-online-view
# Retires every mount owned by this service, including the online instance.
lore service stop
lore mount --experimental --writable --remote-contents --reuse --source /source \
  --metadata /tmp/lore-online-metadata /tmp/lore-online-view
```

Online creation fetches the selected immutable metadata while file contents are
fetched and verified on demand, then cached in the private local stores. It does
not upload private edits. Online reuse requires a current source capability with
the recorded repository, remote URL and identity; it does not change stored
pins. `--digest-cache` remains a cache-only lookup in this mode: cold
`user.sha256` lookups return no value and never fetch file contents or compute a
hash. `--trust-remote` is refused for this mode. An immutable hash, when already
cached, is returned as 32 raw bytes;
private edits revoke it. Directory listing and metadata lookups do not prefetch
file payloads.

Native Linux FUSE validation covers the online factory and in-process service
command with a remote protocol serving real immutable fragments: no payload
fetch during mount, listing or cold digest lookup; verified first read, cached
repeat read, private write and fsync, and persisted private bytes after reuse.
The command's callback and finite service work are released while the mount
remains usable, and source checkout locking remains available. Linked reads
retain exact repository partitions and pins, including after reuse, and policy
revocation denies cached child reads. These tests use the normal owned remote
session and VFS read path; they do not run a shell CLI against an external TCP
server.

Root hold/sync supports one unchanged root view. It rejects conflicting local
changes by default; `--forward-changes` preserves local edits and global `--force`
selects overwrite. Reset-local sync, selective `--root-file` sync, view changes,
layers and selected linked-scope transitions are unsupported. A no-op remains
valid, but a transformed view with unchanged normal anchors is refused. Linked
traversal, pinned reads and online hydration remain supported.

Native Linux FUSE validation exercises successive root transitions A → B → C → B
through the in-process command/service dispatch and a normal remote Protocol
serving immutable fragments. It covers known cached raw hashes, old read handles,
conflict refusal, preserved and overwritten local edits, private-byte recovery
after remount, and unchanged source files and metadata. This is not an external
TCP server or shell-CLI sync qualification. Separate native tests cover caller
cancellation after durable intent, exclusion through actual publisher thread join,
post-anchor failure quarantine followed by exact-candidate recovery on reuse, and
publisher-destructor panic withdrawal before retirement. Startup recovery tests
use actual pending stores and exact old/new anchors, reject mixed anchors and
unknown pending bytes, and simulate interrupted checkpoint publication and
cleanup. These targeted tests do not qualify physical power loss.

Native Linux CLI validation covers importing a normal committed repository,
compiling a C++ program through the mount, editing and fsyncing it, and running
status, stage with scanning, and an offline commit against the mounted root.
With the optional digest cache, the fresh clean import returns 32 raw
`user.sha256` bytes matching an independent SHA-256 calculation; an fsynced
private edit returns no immutable hash. After commit, the raw hash matches the
new content and status is clean. Stopping the service and reopening preserves
the edited bytes and builds the same program. The source files and normal
metadata remain unchanged. Reproduce this production CLI/service/FUSE path with
[the local working-driver smoke script](../contrib/linux-working-driver-smoke.sh).

Native Linux CLI validation also covers a **same-repository historical pinned
link**: fresh clean hashes, editing and fsyncing the linked file with dirty hash
absence, clean service stop, and private-byte recovery through reuse with a
current source policy. Source checksums remain unchanged. The
[linked working-driver smoke script](../contrib/linux-working-linked-smoke.sh)
includes fixture-generation and invocation instructions. A separate host test,
`imported_foreign_repository_pin_reads_from_independent_stores_and_rechecks_access`,
qualifies distinct-repository import into independent stores, exact child pins and
content contexts, real backend reads, and authorization revocation. That test does
not establish a full distinct-repository CLI mount workflow.

Commands routed through the owning service resolve the instance's external
metadata from its registration. Pending and retired registrations refuse normal
repository loading. Root hold/sync uses the transactional path below; link
changes and other unsupported replacement operations still refuse explicitly.

## Embedding

Construct `RevisionBackend` from an already authorized repository context and an
explicit revision hash while inside a Lore execution context. The backend captures
that execution context for callbacks from filesystem threads. It preserves the
context's child-repository access policy and uses the owning child's stores for
linked reads. Construct `Core` from an `Arc` of that backend and `backend.root()`.
On Linux, pass the core and a live multithread Tokio runtime handle to `linux::mount`.

Digest caching is opt-in: call `backend.with_digest_index(DigestIndex::new(path))`
before sharing the backend, using a private cache directory owned by the authorized
user. Explicitly call `backend.backfill_digest(&node.key, max_bytes).await` to verify
and hash one immutable file within a caller-supplied whole-file byte limit. Backfill
uses Lore's verified content stream with bounded buffering and publishes only after
successful completion with exactly the expected length. Link authorization is checked
for both backfill and cached lookup. Records use checksums and atomic replacement;
the directory is trusted storage, not an authentication boundary against another
writer. New Unix cache directories and records use modes 0700 and 0600. Removing the
cache directory only removes digest availability. The default backend constructor
leaves the index disabled.

External working-tree providers can opt into ingestion hashing with
`ExternalOperation::wants_content_sha256`. Lore hashes the same bytes it ingests,
then supplies `CommittedContentDigests` through `committed_digests` after the
enclosing operation and finalization succeed. The provider can pass that batch to
`DigestIndex::publish_committed`. Failed and dry-run operations do not publish
batches; cache callback failures are warnings. This is an opt-in provider API;
the read-only mount does not attach a working-tree commit provider.

Keep the runtime alive until explicit unmount or drop finishes. Mount/unmount use
blocking FUSE operations; call them from a blocking thread when embedding in an
async application, or use `unmount_async` for teardown. Drop must also happen
outside runtime worker threads. Shutdown cancels pending backend futures before
joining the FUSE callback thread, so stalled storage does not hold unmount open.
The mount uses kernel permission checks, read-only, nodev and
nosuid options, and does not enable `allow_other`. The configured uid/gid should
identify the mounting user.

Linux rechecks the owning backend's authorization before each new file or directory
open, including when names and file pages are cached. Backend reads and digest
lookups also check authorization. This does not revoke bytes already served to an
existing descriptor or mapping by the operating system.

The command mounts a fixed read-only snapshot. Embedding callers can obtain
`Mounted::transition_control` to switch read-only generations on kernels supporting
explicit invalidation. Existing file handles and directory enumeration retain their
original generation; fresh paths resolve against the new view. Relative lookup
through an old root directory descriptor follows the active view because FUSE lookup
does not carry a directory handle. A transition succeeds only after invalidation
requests are accepted; rollback failure blocks further switches until recovery.
Negative lookup caching is disabled so new names are immediately discoverable even
on kernels with incomplete negative-dentry invalidation support.

The experimental writable Linux library supports create, write, truncate,
executable-mode changes, rename and unlink with retained open handles. It requires
an explicit access gate and a private store factory; it is not selected by the
read-only CLI. Construct the model before sharing its namespace with a
`WorkingProvider`, and let the model perform all materialization and mutations.
Provider freezes and receipt adoption use that same namespace. Restore requires
fresh budgeted stores; the factory's owner manages their lifetime and cleanup.
Successful construction binds the namespace and exact revision backend to one
Model for their lifetimes. Closing that Model does not make those objects
reusable; construct fresh objects or restore. Cancelled or failed construction
releases its uncommitted reservation.
Close/flush do not promise durability. Shutdown cancels pending model operations,
drains handle cleanup and waits for any admitted checkpoint publication.
Completed cleanup jobs release their file and session ownership before reporting
completion; cancelling a release wait leaves its resources available for retry.

Linux inotify reports named events for kernel-originated create, write, close,
executable-mode change, rename and delete operations, including paired rename
cookies. Sync and notifications for out-of-band transitions remain separate work.
The experimental Swift–Rust FSKit bridge exposes
the read-only core; signed-extension runtime qualification is still required.
Inode metadata is retained for the mount lifetime up to
an explicit cap; kernel forget-based reclamation remains future work. Metadata
currently has synthetic timestamps. Writable timestamps encode persisted file
generations in whole seconds so tools that round timestamps still detect edits;
they are not wall-clock modification times. Arbitrary timestamp, ownership and
permission changes, and synchronous-open flags, are unsupported. Long names
unsupported by Linux fail explicitly.

`lore-revision` also exposes an external provider boundary for working-tree
integration. A provider implements `InstanceOperation` and `ExternalOperation`,
then returns `InstanceOperationImpl::external` from `begin_operation`. State,
storage, commit and finalize handoffs are explicit. `provider::WorkingProvider`
supplies frozen namespace reads for stage and commit, plus generation-specific
receipts checked against stored revisions. It preserves unselected edits and open
read handles. Optional `with_digest_index` publishes ingestion hashes for later
immutable reads. Root hold/sync uses a dedicated transactional path; view changes,
restricted dependency sets and reset-local sync remain unsupported. Other branch
replacement, restore, cherry-pick, revert and merge-state commands refuse before
changing instance anchors or merge carry data. Link add, remove and pin updates, file resets and resets of the held
branch also refuse before replacing the mounted view. Resetting an unrelated
branch remains available when the current branch is known. Normal file stage,
unstage and commit remain supported. Unstage checks all selected paths and layers
before modifying state, and refuses selections that would undo a link addition,
removal or pin replacement. Provider mutations
outside the implemented working-tree operations fail explicitly. Commands routed
through the owning service share this provider with the writable mount.

`lore::working_mount::Supervisor` supplies the instance ownership layer for
service integration. It accepts prepared repository metadata, private backing
storage and an explicit access policy. Linux publication waits for kernel
readiness; retirement withdraws the provider, stops kernel requests, publishes a
final checkpoint and drains the owned runtime and backing IO. Cancelled startup
and failed retirement remain discoverable as retained instances. Recovery refuses
a normal root anchor inconsistent with the checkpoint's logical root revision;
linked scopes retain their own pinned revisions independently of branch tips.
Mountpoint validation rejects repository metadata directories and pins the
underlying directory identity through startup. Replacing that directory or adding
contents before the kernel mount refuses publication.
Unix service commands use a fresh owner and provider registry for each run.
Status and stop have separate bounded admission; failed retirement closes
normal work admission while retaining status and retry access. A stale command
cannot stop a newer service run. Cancelling the service caller requests retirement
on an owned runtime; the run remains reserved until the actual thread exits.
Library shutdown waits for that retirement before tearing down shared resources;
a timeout returns failure and permits retry. Internal Linux mount startup requires
the exact service work scope, prepared resources and an explicit access gate.
The named internal local-edit entry point follows normal checkout semantics:
local OS access permits edits, while existing server checks govern remote pushes.
Its policy binds to the exact revision backend, credentials and service run.
Restore binds it to the final reconstructed backend, including linked exposures.
Stricter library callers can still inject their own policy. The explicit local
writable CLI selects this policy. Root hold/sync prepares and durably publishes
a replacement view through the exact running Model.
`prepare_instance_with_options` accepts an optional private `digest_cache` path.
The supervisor shares that verified index between commit publication and pinned
revision reads, rejects overlap with managed roots, and rechecks the path before
mounting. This library preparation path performs no implicit backfill; the fresh
writable CLI import explicitly prepares hashes when a cache is requested. A cache
miss or optional publication
failure does not change committed data or the durable overlay receipt.

Bind `with_checkpoint_owner(model.checkpoint_owner())` for durable commit
handoffs; an unbound provider is explicitly volatile. Bound providers require
repository stores to synchronize before adopting a committed revision into a
checkpoint. A failed or cancelled flush or handoff prevents successful operation
finalization. This ordering does not make subsequent normal anchor updates atomic
with checkpoint publication. `close_admission` rejects
new operations and metadata reads while admitted operations can finish.
`cancel_operations` also cancels their pending requests. `drain_operations`
waits for phase/request tickets; `drain_exports` separately waits for retained
content sources, handles, actual export IO and cleanup. Those leases retain the
bound checkpoint/storage owner through completion. Failed cleanup retains files
and makes the drain fail. Kernel request closure is separate from terminal
checkpoint closure, so already-admitted commit handoffs can finish durably.

Committed overlay files expose a raw SHA-256 only when the exact frozen file's
export hash matches ingestion evidence for its validated immutable identity.
Writable admission clears that receipt before mutation. Receipt lookup performs
no content reads, and missing evidence leaves the digest absent. Independently
committed child receipts survive a later parent failure.

Commit handoffs identify the exact exposed source subtree and mount route.
Logical held revisions are tracked separately for each exposure, including
deletion-only commits and independent parent link projections. Repeated links
to the same repository retain distinct baselines. Stale scope handoffs fail;
failed or cancelled critical handoffs prevent successful operation finalization.

Transition preparation can retain an internal metadata seal over its exact frozen
source. A shared epoch covers namespace cache changes, receipts and backend link
exposures. While sealed, cached reads remain usable but metadata enrichment and
new exposures fail with `Busy`; dropping the seal reopens admission. Cached read
authorization runs outside the namespace lock. Sealing grants neither permission
to install a revision nor authority to update anchors. Root hold/sync additionally
retains the exact command token and store claims, acknowledges its source
checkpoint, writes durable intent, publishes checked anchors and installs the
prepared view before completing its checkpoint. Restart selects only an exact
old or new anchor tuple; mixed or unknown outcomes remain fenced.

`lore-revision::fs::provider_registry` reserves the canonical mount root and a
separate private metadata directory for an explicitly configured
`ExperimentalWorking` instance. Service commands from nested mount directories
resolve to that root while preserving the caller's working directory and file
arguments. Registration identity, repository and instance IDs are checked again
before attaching the provider; stale, missing or unready owners fail without
OS-provider fallback or automatic identity recovery. Metadata is not exposed
inside the mount. Ordinary and Windows SWFS selection stay unchanged.
Cloning into a registered working mount, including nested paths and aliases,
is rejected before `--force` can remove metadata. Writable CLI selection and
command-driven repository bootstrap remain separate work.

Checkpoint capture locks the namespace and its materialized files in inode order.
It copies the namespace header and selected authorization routes, then releases
the namespace before reading checkpoint data. File guards retain a consistent
content snapshot; acquiring them may wait for existing file IO. Policy and store
callbacks must not reenter the same namespace or a locked file.
Publication is blocking I/O and belongs on a blocking thread. Restore requires the
same pinned root, current authorization, and fresh per-file stores; open handles
are not restored. Version 5 checkpoints retain the immutable original root,
per-exposure active underlays, commit receipts and separate logical baselines.
A missing underlay override keeps the original pin, including a staged origin;
it never implicitly selects the logical committed head. Backend provenance retains
up to 1,024 additional immutable views and validates original routes, unchanged
child bindings and monotonic exposure allocation before loading their states.
Restore validates owning revisions, source subtrees, parent projections, node
types/sizes/modes and full content addresses before reconstructing private data
or restoring digest availability. Absent target subtrees retain independent
authorization checks for subsequent local creates and writes.
Older or unknown formats are rejected and retained, without
automatic migration. Failed pending data is retained for inspection. Corrupt committed
data and stale snapshots are rejected; an uncertain publication requires reopening
the recovery owner. Physical power-loss ordering remains a qualification step.
Encoding checks the full wire size before allocating its output buffer, including
the checksum, and bounds that buffer's capacity. This limit does not include the
already captured snapshot or other simultaneously retained checkpoint images.

`working_storage` provides private, identity-bound session directories for backing
files and exports. Its exclusive ownership lease survives cancelled callers until
their actual queued filesystem jobs finish. IO concurrency, retained sessions and
file allocations are bounded; failed attempts and old sessions are retained and
charged. It never removes recovery data or implicitly adopts an existing checkout.
After kernel/provider work and checkpoint capture have stopped,
`close_and_drain_io` permanently fences backing-file IO and waits for its actual
queued jobs. Cancelling this wait leaves the fence closed; it can be retried.
Export and checkpoint publication owners must be drained separately.

## Validation

Common and real-backend tests run on macOS or Linux:

```sh
cargo test --locked -p lore-vfs --lib
cargo clippy --locked -p lore-vfs --all-targets -- -D warnings
cargo +nightly fmt -p lore-vfs -- --check
```

On Linux, install `fusermount3` (typically the `fuse3` package), provide an accessible
`/dev/fuse`, and run mounted contracts as the ordinary user:

```sh
timeout --kill-after=10s 180s cargo test --locked -p lore-vfs --lib \
  -- --ignored --nocapture --test-threads=1
timeout --kill-after=10s 180s cargo test --locked -p lore --test unit \
  working_mount::mounted_ -- --ignored --nocapture --test-threads=1
timeout --kill-after=10s 180s cargo test --locked -p lore --features test-util \
  --test unit working_mount_factory::remote::transition_tests \
  -- --ignored --nocapture --test-threads=1
```

The ignored tests cover trusted digests, real linked repositories, read-only
revision switching and unmount during a blocked storage read. Writable contracts
cover mutations, retained handles, linked write denial, cross-repository rename
rejection, provider freezes, normal commits and recovery after killing the mount
owner. Separate tests verify inotify delivery and child/mount cleanup on unwind.
Supervisor contracts cover readiness-gated publication, cancelled startup,
registered stage/commit, final retirement recovery and unexpected kernel teardown.
They must fail if mounting is unavailable rather than count an unexercised mount
as a pass. The dedicated
`vfs-mount` CI job supplies those prerequisites. Its configuration is not evidence
that a CI run has passed.

The Bazel workload gate additionally requires Bazel 7.5.0 (the CI-pinned version):

```sh
timeout --kill-after=10s 240s cargo test --locked -p lore-vfs --test bazel \
  -- --ignored --nocapture --test-threads=1
```

Set `LORE_TEST_BAZEL` to select the binary. The test compares ordinary and mounted
build output, confirms Bazel requests `user.sha256`, checks execution-log SHA-256,
and reuses a live Bazel server across same-size revision changes and missing-digest
fallback. Its output/cache directories stay outside the read-only mount. This
fixture qualifies the transport contract. A second workload keeps the same inode
and byte length across mutable edits, checks digest revocation before writing,
and verifies the build after checkpoint recovery into fresh stores. These tests
do not qualify real-repository workload scale or FSKit.

Native Linux build/test is the release gate. `fuser` 0.16.0's build script uses the
build-host OS when deciding whether a no-libfuse build is supported, so an
unmodified macOS-to-Linux cross-build fails before compiling the adapter. Do not
interpret a macOS build, or a cross-check with a temporary dependency patch, as
validation of native Linux mounting.

See [PLAN.md](PLAN.md) for remaining slices, dependencies and acceptance criteria,
and [the FSKit prototype](../contrib/fskit-prototype/README.md) for macOS runtime work.
