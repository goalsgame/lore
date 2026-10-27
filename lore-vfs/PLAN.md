# Common and Linux VFS implementation

## Target and delivery order

Deliver a normal Lore working-tree mount on Linux and macOS 27, including linked
repositories and raw SHA-256 xattrs. Windows SWFS remains supported. A revision or
branch browser for CI is deferred; revision identity remains explicit so it can
reuse the core later. No platform default changes during the experimental rollout.

The first integration milestone is deliberately read-only and pinned to one
revision. This establishes real repository reads, link authorization and FUSE
translation before adding mutable state. It is a stepping stone to the working-tree
mount, not a replacement for the full scope. The native FSKit prototype remains in
`contrib/fskit-prototype`. Its signed fixture passes native data-cache and writable
tests on macOS 27.0.1; a container-local offline repository passes pinned read-only
mount checks. Live namespace switching remains blocked by stale negative name
caches. Native Bazel 7.5 consumes the fixture's raw hash xattr and rebuilds for a
same-size change. External repository access, remote credentials and real
repository digest-cache publication remain qualification gates.
The URL-backed FSKit fixture also observes successful `fsync` without a
`synchronize` callback. Native writable durability requires separate qualification;
Linux checkpoint acknowledgment does not establish the macOS transport contract.

See [the architecture and macOS capability gates](../contrib/fskit-prototype/PLAN.md)
for the overall design. This document specifies implementation slices and their
acceptance criteria. A slice is complete only when its stated evidence exists.

## First milestone: shared reads and Linux transport

### Windows regression qualification

Native Windows validation is a final integration gate. Cross-compiling `lore-io`
type-checks IOCP changes but does not execute completion handling or SWFS.
On the Windows test machine, run:

```powershell
cargo test --locked -p lore-io --test unit
cargo test --locked -p lore-base --test unit runtime
cargo test --locked -p lore-base --test runtime_shutdown --test task_lifecycle_observer --test task_spawn_location
cargo test --locked -p lore-revision --test unit
cargo test --locked -p lore-revision --test external_provider
cargo test --locked -p lore --test unit
cargo test --locked -p lore --test calls_after_shutdown --test shutdown
cargo clippy --locked -p lore-io -p lore-base -p lore-revision -p lore --all-targets -- -D warnings
```

The IOCP named-pipe regression must exercise a genuinely pending operation,
cancel its awaiting future, retain ownership until kernel completion, and release
it afterward. Also qualify the installed SWFS driver through normal mount,
stage/commit, linked-repository operations and unmount. Unit tests alone do not
qualify driver behavior. Preserve ordinary and SWFS selection and ensure neither
requires experimental-provider registration or new bootstrap metadata.

### Unix service lifecycle validation

```sh
cargo check --locked -p lore --lib
cargo test --locked -p lore --test unit
cargo test --locked -p lore --test shutdown_preflight --test calls_after_shutdown --test shutdown
cargo test --locked -p lore --test unit working_mount::mounted_ -- --ignored --nocapture --test-threads=1
cargo test --locked -p lore --test unit service_owned_mount::mounted_ -- --ignored --nocapture --test-threads=1
cargo clippy --locked -p lore -p lore-revision -p lore-vfs --all-targets -- -D warnings
```

Run socket tests where Unix socket binding is permitted; mounted tests require
Linux FUSE. Run the production library check separately so test-helper features
cannot hide visibility errors. Isolated child processes test terminal library
shutdown and retained service failures without tearing down the parent test runtime. Qualification
covers caller-runtime cancellation, failed retirement with usable controls,
startup unwind, connection saturation, and actual setup/session thread completion.
The isolated service-owner and shutdown-preflight suites pass on native macOS
and Linux. These service lifecycle checks do not qualify FSKit mount behavior.

### Shared implementation tracks

Three implementation tracks own distinct modules:

| Track | Files | Deliverable |
| --- | --- | --- |
| Common core | `src/core.rs` | Backend contract, inode and handle lifecycle, bounded reads and enumeration, cache-only xattrs |
| Lore revision adapter | `src/revision.rs` | Pinned repository reads and nested link traversal through existing Lore contexts and stores |
| Linux adapter | `src/linux.rs`, Linux test harness | FUSE callbacks, errors, xattr wire behavior, mount/unmount lifecycle |

The coordinator owns crate manifests, exports, lockfile, CI wiring and integration
review. Shared interfaces are agreed before adapter implementation. No agent changes
another track's files without coordination. Each track reports test commands and
unverified behavior; compiling the macOS build does not validate Linux callbacks.

### Common core contract

- The backend is asynchronous so existing Lore storage reads can be reused. FUSE
  callbacks bridge through an explicitly supplied runtime, outside an async task.
- Each node carries its owning repository, revision, node identifier and mount
  exposure. Two links to one child may have different parents and source roots;
  they cannot accidentally alias namespace entries or authorization routes.
- The mount root inode is fixed. Assign unique nonroot inode numbers per generation.
  Retain identities needed by kernel references and open handles; reject unknown
  or released handles, and do not reuse stale identifiers.
- Directory continuation is bounded and explicit. Reject invalid cookies and
  non-directory traversal. Do not eagerly load an entire repository.
- Clamp reads to EOF, validate offsets and request limits, and bound allocation.
  Surface errors; never turn authorization or corruption failures into empty files.
- `user.sha256` is exactly 32 raw bytes when known and absent otherwise. Lookup
  must not hydrate content. Reject caller mutation of this computed attribute.
- Shutdown prevents new operations and drains or rejects outstanding work without
  returning successful reads from released state. Do not call OS callbacks under
  inode-table locks.

Acceptance: fake-backend tests cover link exposure identity, overlapping node IDs
across repositories/revisions, EOF and oversized requests, continuation bounds,
released handles, denied reads, absent/known digests and shutdown. Concurrent
operations must not hold shared locks across storage awaits.

### Real Lore backend contract

Accept an existing `RepositoryContext` and explicit revision; do not invent a new
credential store, remote connector or content cache. Deserialize the pinned state
and reuse verified immutable range reads. Keep context/state with the resolved
node, including for metadata and directory listing.

Follow links at the final path component as well as intermediate components. Use
the child's pinned revision and source node, and authorize its repository before
reading its tree or payload. Nested links and repeated mounts must preserve their
own exposure. Do not pass a child node number back to the parent's state. Surface
cycles/depth exhaustion with bounded failure rather than infinite recursion.

The backend returns no SHA-256 for existing content without a trusted digest
record. Legacy path metadata is not trusted merely because size matches. Filesystem
features not represented by Lore's current node format must fail explicitly rather
than fabricate symlink/hardlink semantics.

Acceptance: real in-memory Lore repository fixtures exercise ordinary reads, EOF,
empty files, nested and subtree links, two pins of a child, denied child reads,
missing/corrupt content and read errors. Tests must verify reads reach the child's
partition and the correct revision, not merely that names look right.

### Linux adapter contract

Use the pinned released `fuser` dependency with native Linux FUSE transport. Its
protocol features are negotiated with the kernel; document the supported runtime
baseline after mounted validation. Keep Linux-only dependencies behind target cfg.

Implement lookup/getattr, directory open/list/release, file open/read/release,
forget/reference handling, raw getxattr/listxattr, and consistent errors. Size-zero
xattr calls return the required length; insufficient buffers return ERANGE; missing
attributes return ENODATA. Mount read-only and reject write/truncate/create/rename/
unlink/setxattr/removexattr. Do not enable `allow_other` or grant the mount broader
access than the invoking user. Implement an explicit session teardown API and a
safe drop path. Report mount failures without leaving a success-shaped handle.

Acceptance: Linux compilation and adapter unit tests, then a real `/dev/fuse`
mount test checking listing, range reads, metadata, xattr sizes/values/errors,
read-only enforcement, open handles and unmount. An unavailable FUSE device must
be reported as a skipped runtime gate, never a passing mount test. Run the same
checks with the real Lore backend after its fixture harness is connected.

## Remaining implementation slices

The current library implements the R1 optional digest index and explicit verified
backfill. R2 supplies opt-in ingestion hashing and successful-operation digest
batches for external providers. R3 provides the injected operation boundary in
`lore-revision`, including required state, storage, commit and finalize hooks.
R5 has bounded per-file sparse overlays and a mutable namespace with stable local
identities, access checks and digest revocation. R6 has private ownership locking
and explicit bounded full checkpoints, including dirty extent data, with stale
snapshot rejection and process-exit/fault tests. A shared storage-budget wrapper
bounds append requests across private stores, including failed and cancelled
writes. The experimental `linux_working` library connects the shared namespace
to direct-I/O writable FUSE operations, bounded directory snapshots and full
checkpoint-backed fsync. Native contracts cover mutations, retained handles,
denied writes, resource limits and recovery after abrupt mount-owner death.
Writable mappings remain unsupported. R7 supplies a
stage/commit provider over a frozen namespace with private bounded ingestion
exports and exact-generation commit receipts. Matching export and ingestion SHA-256
evidence enables cache-only overlay digests. Version 5 checkpoints retain these
receipts, scoped logical revisions and independently selected underlays, and
revalidate their immutable identity
during restore. Providers bound to the mount's checkpoint owner acknowledge each
successful child commit only after full local checkpoint publication, so a later
parent failure cannot erase an acknowledged child handoff. Root hold/sync uses a
separate transactional command path rather than the generic commit handoff.

R4 supplies read-only generation transitions with immutable open file reads and
directory enumeration. Mounted Linux contracts cover cache invalidation, same-size
changes, misses, deletion/type changes and retained descriptors. Negative caching
is disabled for compatibility with incomplete kernel negative-dentry invalidation.
R8 supplies an explicit foreground experimental read-only command, pinned
revision selection, optional private digest caching, path validation and cancellation
through startup and teardown. The separate explicit writable command uses
service-owned bootstrap and private recovery storage.

Writable service/CLI selection is explicit. It supports local/offline import,
opt-in online file hydration and transactional root hold/sync. Native Linux read-only mounting is exercised through the
transport, real linked-repository and transition contracts. The Bazel fixture
checks raw digest use, execution-log hashes, ordinary-checkout output equivalence
and same-size changes with a persistent server. These read-only tests do not prove
writable transition semantics. The separate mutable Bazel fixture verifies
same-inode same-size edits with dirty digest absence and checkpoint recovery.
Physical power-loss qualification remains
separate from process-exit tests.

| Slice | Dependencies | Concrete change | Acceptance evidence |
| --- | --- | --- | --- |
| R1: Verified digest records | Real Lore reads | Versioned removable index keyed by full Address, size and algorithm; bounded backfill; cache lookup API | Empty/large/fragmented files, length mismatch, read failure and context isolation; xattr hit performs no content I/O |
| R2: Ingestion and commit hashing | R1 | Compute whole-file SHA-256 in the ingestion/commit stream; publish after immutable identity established | Digest equals independent whole-file SHA-256; no fragment-root substitution; failed/aborted commits cannot publish |
| R3: External provider boundary | Core contract stable | Extend `FilesystemProvider`/operation dispatch for an injected provider without a lore-revision → lore-vfs dependency; preserve OS and SWFS behavior | Existing revision/provider tests pass; fake external provider receives state/commit notifications; no silent no-op state handoff |
| R4: Revision transitions | R3, mounted Linux reads | Transition barrier, changed inode/name set, generation-safe directory cookies and kernel invalidation acknowledgements | Same-size content switch, negative lookup, deletion/type change, open descriptors/mappings and concurrent reads; failure never acknowledges a successful transition |
| R5: Writable overlay | R3 | External sparse overlay, inode aliases, writer/mapping tracking, mutations and digest revocation before writable access | Same-size write/truncate, concurrent writers, unlink-open, rename-over, executable mode, zero extension, disk-full/short writes; digest absent whenever mutable |
| R6: Durability and recovery | R5 | Versioned full checkpoints, data/metadata fsync ordering, validated restore and recovery ownership | Kill/restart at each publication boundary; acknowledged fsync survives; no lost edits or stale replay; corrupt checkpoints produce explicit recovery errors |
| R7: Stage/commit/sync | R2–R6 | Integrate overlay into normal Lore staging, commit, dirty tracking and sync; preserve/reject local conflicts explicitly | Commit/readback on parent and nested children, denied child writes, failed upload, concurrent commit/sync, restart after partial work |
| R8: Service and CLI | R3, R4, R6 | Explicit Linux backend selection, repository/mount registry, canonical paths, ownership, shutdown and stale-mount diagnostics | Multiple mounts, duplicate/misowned mount rejection, service death/restart, busy unmount, clean install and rollback; Windows selection unchanged |
| R9: Native FSKit bridge | macOS runtime gates, core contract | Swift–Rust versioned ABI, explicit buffers/handles/errors, cancellation, repository access under extension sandbox | Probe passes on macOS 27; real Lore linked reads; credential/storage permissions; no callbacks after unmount; ABI mismatch rejected |
| R10: Writable FSKit | R5–R7, R9 | Cache upgrade/downgrade, flush/invalidate sequencing and durable overlay operations | Dirty mmap and flush behavior, fsync crash tests, revision transition conflicts and failed invalidation without lost writes |
| R11: CI and qualification | Applicable mounted adapters | Linux/macOS contract jobs, Bazel workload and resource/performance regression suite | Same build outputs and execution-log SHA-256 as ordinary checkout; same-size changes rerun actions; no content reads for warm digest lookup; bounded memory/resources |

R1–R3 may proceed independently once the first interfaces are stable. R4 and R5
must agree on writable-handle and transition semantics before integration. R6 is a
prerequisite for calling any writable backend production-ready. R8 may add an
explicit experimental read-only command earlier, but must label unsupported
operations and cannot change defaults. R9 need not block Linux work.

## Review decisions before mutable integration

### Writable Linux adapter acceptance

The first writable adapter must use the common namespace, overlay and recovery
owner directly. Keep it separate from the read-only command until these contracts
are exercised through the kernel:

- Retain an inode-to-overlay mapping and global handle table. Unlinked or replaced
  files remain readable through existing handles; release works after access is
  revoked. Never reuse a stale handle or inode.
- Serialize operation admission with the provider's exact freeze lease. Writes,
  create/remove/rename and executable-mode changes fail while a command owns the
  frozen view. Cancellation cannot let a late completion adopt a subsequent view.
- Initially use direct file I/O and zero metadata/entry TTLs for mutable files.
  Reject unsupported writable mappings explicitly. Introduce kernel write caching
  only with tests for mapped writes, digest revocation and flush ordering.
- Map namespace conflicts and resource limits to useful errno values. Preserve
  repository/exposure boundaries during rename and independently authorize linked
  writes. Read permission alone is not an implementation of the write gate.
- Bound total append storage across files, including reservations consumed by failed
  or cancelled writes. A per-file limit is insufficient. Checkpoint size and inode
  limits must fail without discarding dirty content.
- Serialize checkpoint requests before capture. `fsync` and directory `fsync`
  acknowledge only after data-bearing checkpoint replacement and directory sync.
  Ordinary close/flush does not promise durability. Recover namespace and dirty
  bytes under the same exclusive owner after restart; never silently discard an
  uncertain or corrupt checkpoint.
- Test actual mounted create/write/truncate, rename-over, unlink-open, same-size
  edits, denied linked writes, raw xattr absence while dirty, concurrent command
  freezes, checkpoint failure and kill/restart. Repeat the Bazel workload through
  mutable writes; read-only generation-switch success is insufficient.

Command integration additionally needs an explicit connection between the mounted
instance and its provider owner. Separate CLI processes must reach that owner for
stage/commit/sync; observing the FUSE paths through an ordinary OS provider does not
establish a frozen VFS operation. Define ownership, bounded requests, cancellation
and recovery before exposing this as a normal working-tree mount. Reuse Lore's
existing local service and command routing (`lore/src/service` and
`lore/src/remote`) instead of introducing another credential store or command IPC
protocol. Inject a provider resolver at the repository boundary so `lore-revision`
does not depend on `lore-vfs`; preserve the existing Windows mount-manager path.

### Working-instance service integration

Unix service runs have a fresh state and registry, with a private supervisor
bound to that same registry. Accepted connections retain their exact owner;
trusted invocation scope checks the owner/ticket pair. Status, stop and quit
capture the run identity before awaits. Run exit fences old sockets even if they
retain the owner, and a stale command cannot stop or report a successor run.
The supervisor's startup surface remains private until control dispatch,
retirement and cancellation ownership are integrated. Windows retains its legacy
service state; the global SWFS callback buffer supplies diagnostics only on Unix.

After mounted writes pass, integrate an explicitly selected working instance in
these dependency-ordered pieces:

1. Add an injected provider-resolution boundary in `lore-revision`. Resolve a
   canonical instance path and instance ID to its already-running provider. A
   configured VFS instance without an owner must fail with a useful diagnostic;
   it must never fall back to the OS provider and ingest an empty mountpoint.
   Preserve the existing `None` and `Swfs` configuration meanings. Tests cover
   ordinary instances, stale owners, wrong instance IDs and Windows selection.
2. Let the existing Lore service own the mount, provider, recovery lock and
   private store factory together. Bound registrations and reserve a canonical
   target before asynchronous setup. Refuse duplicate, overlapping and aliased
   targets. Failed setup releases only its own reservation. Keep repository
   metadata, recovery, stores and exports outside the mounted tree.
3. Route ordinary stage/commit/sync commands through the existing service command
   protocol to that owner. Each operation uses the same namespace freeze seen by
   kernel writes. Retain command credentials and repository write-lock rules;
   mount lifetime must not hold a command's write token. Test two actual CLI
   processes against one mount, including a cancelled command and denied child.
4. Extend service status/stop to include experimental mounts. Stop admission before
   draining work and releasing the kernel mount. Explicit durability requests must
   complete or fail before acknowledgement. Forced teardown preserves recovery
   data and does not claim unsynced edits are durable. Test busy stop, service death,
   stale registrations and restart from a committed checkpoint.
5. Expose the backend only through an explicit experimental CLI selection after
   these paths pass. Unmount and disabling the experiment retain recovery data;
   neither converts a nonempty working mount into an ordinary checkout implicitly.

The supervisor must retain the mount, registration, provider and private storage
until both kernel requests and admitted provider operations have drained. Existing
IPC invocation tasks can outlive their connection, so connection counts alone do
not establish that an instance is idle. Registry entries retain providers; they
must not form an ownership cycle through a supervisor's registration lease.
Experimental mount status must remain distinct from the existing SWFS count.

Teardown must close provider and kernel admission before observing idle counts.
`ServiceStateImpl::invocation_count` includes detached command tasks, but is not
an admission fence. Its `close_invocation_admission` rejects new IPC commands,
including commands on existing sockets; the service owner can then await
`close_and_drain_invocations`. Never wait for that drain inside a command holding
its own invocation ticket. Cancel or finish command capture, drain admitted checkpoint
publishers, and then use `WorkingStorage::close_and_drain_io` to fence and drain
actual backing-file jobs. Keep the retiring mount record and its leases until
kernel unmount and every drain succeed; cancellation must allow a later retry.
Checkpoint publisher completion follows destruction of its captured snapshot,
freeze and storage ownership, including unwind and failed spawn. It does not
claim that the publisher's OS thread has been joined. Handle cleanup retains
retryable resources across release waits, then drops the file and handle
reservation before caching completion. Retained completed cleanup jobs therefore
cannot keep a storage session locked.
Provider export jobs and leased content handles retain their directory and bound
checkpoint/storage owner through actual OS completion. `drain_exports` waits for
those owners and actual cleanup; cleanup failure refuses successful retirement.
Dropping an operation alone does not drain its exported content sources.
Service shutdown must first fence new commands, request retirement of all mount
instances, and drain admitted command tasks before allowing final supervisor
resource release. Cancelling a provider phase alone does not stop its enclosing
command from finishing repository IO or updating normal anchors. Keep the
supervisor and its reservations alive until those command tickets settle.
On Unix, non-forced stop rejects busy work without closing work admission. It
checks other fallible shutdown preconditions, atomically fences idle work using
its control ticket, and requests supervisor retirement without an intervening
cancellation point. Forced stop fences new work and waits for admitted commands;
it does not abort them as a substitute for draining. Failed retirement preserves
the exact run, listener and control admission so status and retry remain available.
Only successful resource retirement permits terminal control drain and listener
shutdown. The legacy Windows path retains its existing invocation fence.

IPC dispatch retains its exact ticket and owner in trusted task-local scopes
through command completion and result enqueue. Calls outside those scopes receive
no owner or closure capability. These scopes are not propagated into background
mount tasks or retained execution contexts. Each Unix service run has a fresh
registry and admission state; stale owners cannot control a successor.

Service state also provides bounded control tickets for trusted status/stop
dispatch. `invocation_count` remains the total; `work_invocation_count` excludes
controls. `close_work_admission` and `close_and_drain_work_invocations` preserve
up to 32 control tasks, while the existing all-command close/drain remains
terminal. A control ticket's `try_close_work_admission_if_idle` serializes the
graceful fence with normal command admission. Dispatch must classify typed
commands itself; caller fields cannot request control privileges. Final run
cleanup closes both paths, and a work ticket must never enter its own work drain.
The Unix service runs on an owned thread and private runtime. Cancelling its
caller requests retirement without dropping the run reservation. Socket IO and
invocations retain connection leases; completion requires runtime destruction
and joining the exact actor thread. Connections are capped at 128. Startup
publication is serialized with library shutdown, so shutdown cannot miss a
partially admitted run. Uncertain actor failures retain ownership and diagnostics.

`try_shutdown` preflights that exact run before closing storage handles, dropping
connections, stopping core/network runtimes or finalizing the allocator. A timeout
or failed retirement leaves those resources available and returns an explicit
error; a later call can retry. Concurrent shutdown attempts return immediately.
The existing boolean/C shutdown entry points report unsuccessful preflight as
failure. Ordinary no-service teardown and the legacy Windows path remain intact.
The internal Linux `ServiceOwner::start_working` entry point requires the exact
trusted work-ticket scope, prepared resources and an explicit access gate. It
returns rejected inputs intact before reservation and delegates admitted setup
and publication to owned supervisor workers. Callers cannot supply the mount UID
or GID. This prepared-input entry remains internal; the writable CLI uses the
owned `bootstrap_working_local` path with exact recovery admission.

Linux retirement must record the mounted instance's mount ID, reject a detected
replacement or stacked mount before unmounting, and verify the owned ID is absent
before releasing resources. The unprivileged helper remains path-based:
[fusermount3 3.14](https://github.com/libfuse/libfuse/blob/fuse-3.14.0/util/fusermount.c)
resolves its argument before checking and unmounting it. Passing a `/proc/self/fd`
path therefore does not establish an atomic unmount by mount ID. Pre/post checks
detect changed ownership but do not eliminate a concurrent external mount between
the check and unmount. Qualification must state this limitation; do not silently
add a privileged helper or claim that session join alone proves unmount.

Bootstrap keeps the canonical root token and registration through actual backing
IO completion, including queued jobs after caller cancellation. Metadata jobs
carry the enclosing IO lease into prepared metadata. Store initialization retains
both that inherited lease and its caller's lease through actual job completion;
it releases stores, locks and tokens before those leases and reports completion
last. The returned mounted-owner context retains neither startup lease.
Prepared metadata
alone is unready: normal stores, explicit revision/branch anchors, authorization
and the provider must be prepared before publication. Reuse never generates IDs,
repairs missing anchors or silently upgrades stores. A recovered namespace uses
its checkpoint's original pin, independently of newer committed anchors. Filter
loading for experimental mounts must use bounded asynchronous provider reads;
it must not read mounted paths synchronously during repository setup.
Reuse reads anchors through the strict mutable-store reader and rejects a
noncanonical branch encoding. Missing backing payloads never authorize fresh
instance registration or an empty current revision.
The supervisor can reserve target, metadata and backing paths before a startup
input exists. These reservations use the same record map, capacity, identifiers
and cache-overlap checks as prepared inputs. Promotion checks the exact
registration and paths, then installs input once in that same record. Cancelled
jobs retain their startup ticket until actual work settles; retirement handles
records with no storage yet. This is path accounting, not recovery admission:
the compatibility path still accepts prebuilt inputs, and the production factory
must establish recovery ownership before mutable initialization.

Local editing follows ordinary checkout behavior: the mount's OS identity may
edit available, readable content; existing server checks authorize remote pushes.
No online permission check is required for each local edit. Stage and commit
remain local operations. The `can_read_link` hook governs configured linked-route
checks; `RevisionBackend::revalidate(key, true)` is not a remote write grant.

Writable construction still requires an explicit policy. The internal Linux
`start_working_local` entry point mints admission from its exact trusted work
scope and binds the local policy to the concrete revision backend and retained
execution context. Unknown keys and stale runs fail closed. Existing injected
policies remain available for stricter callers and permission-denial tests.
Restoration binds the immutable policy only after provenance replay selects the
final backend, before authorizing saved keys or reconstructing private extents.
Fresh and restored namespaces retain the exact original policy factory. Future
prepared-backend binding must use that factory; it cannot substitute a looser
policy or mutate the gates retained by existing open files.
Repeated linked exposures retain distinct identities. Target-revision access
requires a separate proven selection; no arbitrary-key fallback is permitted.
The explicit Linux `mount --experimental --writable --offline` workflow imports
a selected local immutable revision into separate private metadata/backing, mounts
it through the service, and supports local edits, fsync, normal status/stage/commit
and clean stop/reuse. Linked reuse requires a current source read policy; without
one, link traversal fails closed. Held-view replacement and durable sync remain
guarded and unsupported. Import fragment-count rejection currently follows the
existing collector's address-list allocation and sorting. Strict pre-allocation
fragment-memory admission remains required hardening; the current local import
limits do not establish a peak-RAM cap.

### Scoped revision state and sync acceptance

The working provider keeps `supports_revision_replacement` false. Legacy
revision-changing command paths reject it before normal-anchor, staged-state or
merge-carry writes, including metadata-only changes with no file delta. Derived
repository contexts preserve that capability even when command locks and the
actual provider are stripped. OS, SWFS and ordinary external providers retain the
legacy default. Normal stage/unstage/commit and unrelated branch publication stay
available. The dedicated root hold/sync driver uses a separate capability and leaves other
legacy replacement commands disabled.
It accepts one unchanged root view, rejects local conflicts by default, preserves
edits with `--forward-changes` and overwrites them with global `--force`. Reset,
selective root files, view changes, layers, linked-scope transitions and changed
views with unchanged normal anchors are refused. Linked traversal and lazy reads
remain supported.
Native Linux validation covers successive A → B → C → B transitions through
in-process command/service dispatch, a normal remote Protocol and actual FUSE:
known cached raw hashes, retained read handles, conflict/preserve/overwrite
behavior, remount recovery of private edits and unchanged source data. It does
not establish external TCP or shell-CLI sync qualification. Native lifecycle
tests cover caller cancellation after intent, exclusion through actual publisher
thread join, post-anchor failure quarantine and exact-candidate recovery on reuse,
and publisher-destructor panic withdrawal before retirement. These targeted
failure tests do not establish physical power-loss durability.

Link add, remove and working-tree pin updates and file resets require that
capability before changing staged state or registry mappings. Branch reset may
update an unrelated branch, but refuses to reset the held branch. Its preflight
uses strict instance anchors and read-only branch resolution; missing or malformed
current identity refuses without repairing metadata or updating name caches.
Unstage preflights all selected parent and layer paths before making changes.
Selections that would undo a staged link addition, deletion or pin replacement
refuse; ordinary file unstage, including files within links, remains available.
The preflight uses the same path casing, filters and single-node selection rules
as the operation and bounds its traversal.

The working tree's logical held revision must be recorded separately from the
immutable revisions supplying its existing bytes. File receipts alone cannot
represent deletion-only commits or the baseline for a later sync.

- Identify a scope by its full original repository, pin, exposure and selected
  subtree root. Two mounts of one repository must retain independent baselines.
- Carry commit scope from normal traversal, including the mount-relative path and
  independently verified source root. Reject ambiguous handoffs; a repository ID
  alone cannot identify which linked exposure was committed.
- Adopt receipts and the scoped logical baseline under the same exact freeze
  lease. Checkpoints must preserve both, including commits with no file receipts.
  Restore reauthorizes the original route and validates the committed scope.
- Keep a child's durable logical head separate from the revision projected by its
  parent link. A later parent failure must preserve the child's successful commit
  without claiming that the parent pin advanced.
- For root hold/sync, prepare a bounded candidate against the logical
  baseline. Reject conflicting local changes by default; preserve them only when
  requested, and replace them only with explicit overwrite. Active writers refuse
  the transition. Retained read and directory handles keep their prior view.
- Coordinate candidate publication with the command's instance anchor, staged
  anchor and view updates through durable recovery intent. A crash between those
  writes cannot be treated as a successful atomic transition. Reset and view
  changes need explicit semantics and tests before they are accepted.

Tests must include deletion-only commit/restart, repeated repositories at distinct
pins and subtrees, a selected child commit, child success followed by parent
failure, denied restore, stale checkpoint rejection, conflict handling and
cancellation at the metadata/publication boundaries.

`transition_prepare::prepare` produces an opaque candidate for one explicitly
selected owning scope. It retains the exact source freeze, reads bounded private
dirty bytes and immutable tree metadata, and leaves namespace, anchors and
checkpoints unchanged. It uses the namespace's exact retained revision backend
and injected policy, including a final target authorization check. Aggregate
node, spelling, path, depth, dirty-byte and comparison budgets apply across the
whole preparation. Rename classification uses a counted index so unchanged
siblings do not cause a quadratic scan for each conflict. Unknown dirty content
is always treated as changed; preparation never hydrates or hashes immutable
payloads. Reset, view, dependency-restricted and layered transitions remain
unsupported, as do unresolved ancestor/type merges and forced conflicted renames.

Every revision selection first invokes `AccessGate::authorize_selection` with
an opaque, backend-minted source proof and requested revision, before reading
that target revision. Policies without explicit support return `Unsupported`.
The proof includes the full owning scope, canonical source path, exact backend
identity and captured execution context; it grants no target-node access. Local
checkout policy validates that source and run independently of remote push rights.
Resolved target metadata uses a separate opaque `ProvenTargetMetadata` proof
with an exact selected subtree or explicit absence. `authorize_target_head` and
`authorize_target_node` require explicit policy support. The concrete validator
checks the originating backend, context, source route, requested revision and
bounded target ancestry. An absent selection grants no node access, including
ancestors; target-node writes return `ReadOnly`. Policy checks repeat after
metadata and payload IO. Read-only target proofs do not authorize installation
or mutable anchor writes.

`PreparedTransition::authorize_candidate_edits` separately checks source write
permissions and explicit candidate-edit decisions. It checks the selected head
even for an empty diff, and includes local edits carried into the candidate as
well as changes to the mounted namespace. Edge changes check the applicable
source and target directory authorities. Requests distinguish mounted source,
immutable target and candidate summaries; missing target paths require private
proof from the complete target image. Changed links remain unsupported. The
bounded, single-admission check releases on cancellation, rechecks the source
witness and policy, and publishes no candidate data or anchors. Success is not an
installation permit and does not prevent later permission revocation.

`PreparedTransition::prepare_target_view` retains the freeze and exposes an
isolated immutable reader for the selected target. Original keys continue to
read their original revisions. Reads use the captured execution context and
check the injected policy before and after IO. Metadata and digest lookup do
not hydrate payloads; range, directory-page and concurrent-request limits apply.
Only already-proven unchanged child routes are supported. The private reader
cannot allocate live link exposures or publish checkpoint state.

The private `PreparedTransition::prepare_complete_backend` path reconstructs a
complete independent backend from original provenance plus the selected view and
proven unchanged child bindings. It preserves original keys, exposure identities
and allocator floors. One aggregate budget covers retained plan data, metadata
images, provenance copies, encoding and strict replay. The result is
checkpointable while new link allocation remains disabled, and retains neither
the namespace nor its writer freeze. Source identity and epoch checks reject
stale or unrelated preparations.

Private policy binding consumes that opaque result and invokes the namespace's
exact retained factory once, checking source identity and epoch before and after
the callback. The resulting backend and policy can outlive preparation without
keeping the source frozen. These are read-only preparation steps; candidate-edit
authorization and publication remain separate. The optional `test-util` feature
exercises this same private path for cross-crate policy tests and is disabled by
default.

The private metadata layout validates the prepared backend and policy against the
exact source, preserves unchanged child regions, and assigns checked fresh inode
identities to replacement entries. It creates no backing stores and cannot
activate a transition. Owner-membership facts distinguish full revision-qualified
keys and repeated link exposures within one backend; raw links remain explicit
boundaries. These facts establish identity only and do not authorize access.
The private inventory reads each immutable base through its registered full key,
including lazy directories from retained versions. Local authorities and immutable
node reads receive separate policy checks; child regions require the exact
parent-version binding. Rename detection uses full immutable identities, while
local-only access anchors carry no immutable identity. Installed-underlay
preparation uses per-version rename origins, bounded layouts and exact routing
and publication claims. A retained base from another version cannot be
matched to the original tree by reusing its raw node ID.
After a normal commit, exact-current receipts can prove a local rename's logical
source through a full verified key, including a replacement destination. These
facts are temporary and do not change physical base identity. Conflicted
overwrite is refused when relocation lineage is unproven; unconflicted overwrite
remains available.

The private intent metadata codec binds complete checkpoint-byte hashes, the
original mount root, selected owning scope and exact old/new instance anchors.
It rejects malformed records and ambiguous anchor tuples. Its classifier only
distinguishes exact source, candidate and unresolved tuples; it neither validates
checkpoint images nor writes, installs or completes a durable transition.

Recovery can retain an opaque loaded checkpoint containing its exact checked
wire bytes, validated snapshot and complete-byte identity. Reads allocate from
the opened file's bounded size and detect truncation or growth without growing
the buffer. The identity survives valid noncanonical record ordering; it is not
computed from a reencoded snapshot. An internal budgeted loader uses the same
parser and charges encoded and decoded allocations to the caller's existing
ledger before allocating, then reserves structural-validation scratch. A bounded
successor wrapper reserves its comparison maps before applying the unchanged
successor rules. Ordinary loading and publication retain their existing behavior;
neither loader proves source currentness without the checkpoint owner's lock.
Internal retained publication accepts an already-accounted source snapshot,
loads and validates the actual current checkpoint under the caller's ownership,
and reserves successor, validation and encoding work in the same ledger. It
encodes once and computes the exact identity before using the ordinary checkpoint
write/file-sync/rename/directory-sync engine. Only successful directory sync
returns the retained snapshot and bytes. That result records a durable write;
it grants no runtime ownership, later currentness, intent or installation rights.
Owned source preparation consumes the captured candidate and its exact source
plan, preserving the original private buffers and full intent constraints. It
rejects dry-run preparation, foreign freeze acquisitions and stale source or
Model witnesses. A budgeted authority list retains current attached routes and
pinned retired routes; policy calls hold neither namespace nor name locks.
Preparation rechecks witnesses before sealing metadata and retains the checkpoint
serialization lease, seal and original request admission. It performs no private
file IO or publication and grants no durable acknowledgment.
The source-publication worker consumes that preparation, rechecks authorization
before admission and publishes through the exact checkpoint owner. A prestarted
joiner joins the publisher before handing its result to asynchronous source and
policy checks. Only that path constructs an opaque acknowledgment, retaining the
source bytes, full intent, candidate, serialization lease, seal and request
admission. Cancellation retains resources until publication finishes. Failed or
panicking handoffs poison the owner before detaching, and the publisher retains
its own resources through completion; those failures never produce an
acknowledgment. This worker does not record intent, change anchors or install a
candidate.

Ordinary Recovery admission and publication reject any object at the fixed
`transition` path, including a symlink or FIFO, without opening its contents.
Existing-directory admission checks before lock-file creation and rechecks after
acquiring ownership. Original-root lookup also refuses the marker before Model
restoration can allocate stores. Preopened owners retain read-only committed
fact loading; it grants no repair or view-selection permission. Publication
refusal poisons the mount owner. This gate creates no transition artifacts and
does not protect earlier cache, session or mutable-store bootstrap by itself.
Private recovery inspection can open an existing owner lock without creating or
repairing files. It pins the directory descriptor and reads fixed names relative
to it, rejects unsafe file types and permissions, and retains exclusive ownership
until inspection ends. Committed checkpoint loading shares the caller's capture
budget and preserves exact encoded bytes. Transition presence is an independent
fact, even when the committed checkpoint is missing or invalid. Inspection grants
no publication, cleanup, anchor-selection or reconciliation authority.
The artifact reader inventories only the fixed transition names using a bounded,
descriptor-relative scan. It reports each source image, candidate image, pending
intent and committed intent independently as missing, checked or invalid. Images
and exact intent bytes consume one aggregate capture budget; exhaustion aborts
the read. Unknown names and excess entries remain explicit inventory flags.
These facts do not establish that the images match the intent or that any anchor,
scope, policy or selected view is valid.
A separate pure relation check requires complete, unflagged facts with no pending
intent. It compares exact source and candidate identities with the intent, both
original-root keys with the intent's key, and the full original-root descriptors
with each other. Success proves only those immutable equalities; it grants no
ownership, freshness, authorization or reconciliation permission.

The private overlay-copy primitive borrows a frozen file image and preserves its
original base, sparse holes and generation in independent storage. It charges
every write attempt, bounds fragmentation from short writes, and validates and
reissues receipts for the replacement inode. It does not allocate stores, attach
files to a Model or install namespace state.

Private resource preparation uses the receiver Model's store factory, append
budget, file limits and checkpoint-data limit. It retains one request slot until
the resources are dropped, so shutdown waits for them. Copied files share one
validated effective gate that checks the source and selected underlay, with new
receipt cells and the namespace's existing write freeze. Known combined quotas
and write denials are checked before store creation; failed attempts retain their
debt. The caller's plan still owns the freeze. This prepares unpublished files
without changing live maps or checkpoints.

Model construction reserves the exact namespace and revision backend together,
commits their runtime owner identity after fallible validation, and rolls back
uncommitted reservations on cancellation. Committed claims remain exclusive for
those objects' lifetimes, including after shutdown. Independent replayed or
prepared backends receive distinct slots even when they share a metadata gate.
Freeze admission uses the same reservation lock; checkpoint owners retain a
read-only lineage identity. These claims do not authorize a backend handoff,
anchor changes or installation.

An owned candidate can reserve its exact prepared backend's runtime slot through
the actual Model owner. The issuer binds the source namespace/backend and the
validated complete target. An independently claimed target is refused; dropping
the reservation releases only its fresh nonce. Source ownership and routing stay
unchanged, and the original claim still rejects the target backend. This
provisional reservation neither selects an active successor nor permits intent,
anchor writes or installation.

Private resources can be consumed into an owned unpublished candidate. It keeps
the same request admission, exact source freeze and witness, independent
namespace maps, prepared file cells and paired backend/policy binding after the
caller drops the plan. Construction preserves original-root identity, retired
records and inode high-water marks; it clears retired directory names and rejects
duplicate attached keys, invalid topology and unproven parent projections.
Attached entries require ordinary membership in the complete backend, separately
from access checks. Fresh private files must match the prepared backend, gate,
mutation gate and freeze; retained source files keep their original routes.
An owned candidate can capture one validated checkpoint snapshot and encoded
image through its exact attached and retired read bindings. Capture bounds
aggregate owned buffer capacities and separate metadata/name counts, including
retained plan data, optional loaded-image evidence, private bytes, read scratch,
validator scratch and encoding. It rejects active writers and rechecks policy
and source identity around reads. The caller may retain the plan, so its measured
footprint stays charged pessimistically. This is per-candidate accounting, not a
global RSS limit or a bound on a source loader's earlier allocation peak.
Loaded-image evidence does not prove the current acknowledged source; that needs
the checkpoint owner's serialized verification before intent. Candidates also
preallocate their exact detached-inode set and checked next Model directory
generation under the name-operation lock. This waits for an admitted name
mutation's namespace and Model updates to finish together. The detached set is
charged before construction and retained in capture accounting; live handle
tables and quotas are not copied. Actual index replacement and durable handoff
remain separate work; none of these preparations permits installation.
Name mutations preflight the Model's next directory generation under its name
lock. Overflow or an unrepresentable generation fails before namespace changes,
store allocation or create-handle reservation; successful mutations publish the
prechecked value before releasing that lock.

Provider operations capture one backend and effective source gate under their
exact namespace freeze, before export admission. Direct metadata calls recheck
known source authorities around backend work; committed proof keys are validated
by the backend rather than passed to generic policy. The capture retains no
freeze lease, and finishing the operation preserves existing export ownership.

Namespace routing pairs an immutable backend with its effective gate and raw
policy. Attached records select the namespace's current binding; detached
records retain the binding from their first retirement. Lazy reads, metadata and
digest checks on detached records therefore keep their original authority.
Restore binds detached records to freshly authorized replayed state rather than
persisting credentials. Routing accessors capture one immutable backend/policy
pair through a private lock and fail closed if that lock is poisoned. Owner
checks confirm the same pair after checking publication claims, without holding
the routing lock across claim locks or policy calls. Prepared root installation
replaces routing only under its exact source, ownership and metadata-seal checks.

File opens capture the exact namespace record, overlay and effective binding,
then confirm that identity after allocating an owned cleanup lease. Each handle
retains its selected backend and gate for reads, writes and metadata; unchanged
files can share storage while old and new handles retain distinct policies.
Linux passes file and directory handles through `getattr` and rejects unknown or
mismatched handles. Cancellation or stale confirmation releases the unpublished
handle through owned cleanup. Handleless metadata, cache-only digest and frozen
export reads also select the attached or retired binding and confirm identity
after IO. Frozen writer scans release namespace state before policy/file waits.
Live source checkpoints capture the attached or retired route together with an
owned namespace header and ordered file guards. Acquiring those guards can wait
for existing file IO while holding namespace state; capture releases namespace
state before its own policy, provenance and storage IO. Ordinary checkpoints
allow open writers and later namespace changes while retaining a coherent
header/file point. Transition capture additionally rejects epoch or binding
drift. Callbacks must not reenter the same namespace or a locked file. Native root
transition validation uses the local-edit policy; injected policies retain their
own authorization requirements.

Root publication enforces these invariants:

1. Install retained immutable target views under an exact metadata seal. Old node
   keys must continue selecting their original view and authorization route.
   Publish routing and policy together with namespace state. The operation-scoped
   provider capture must then select that same installed view under its freeze.
   Bound candidate maps together with retained state and reject an incomplete
   child mapping. The isolated prepared reader and recovery format supply the
   read and restore paths; the root coordinator performs live installation.
2. Preallocate namespace state while preserving inode/exposure high-water marks.
   Keep inode 1 as the live filesystem root; allocate fresh nonroot inodes
   for replacement nodes and retain old files/directories for existing handles.
   Use the namespace's attached-node key index separately from retained inode
   records. A detached record may share its immutable key with a new attached
   record, but its private bytes and handles remain inode-specific. Restore
   indexes attached records only, and hydration rejects an index pointing at a
   detached record. Duplicate attached keys remain invalid.
   Preallocate the Model's replacement file map and detached-inode index, and
   update directory generations consistently with the namespace swap.
   Capture Model metadata under its name-operation lock, which covers both the
   namespace mutation and its detached-index/generation updates. Installation
   takes that lock before the file-map and namespace locks, then Model state
   before routing and ownership locks. Acquire all asynchronous guards before
   synchronous guards; perform no awaits or allocations during final replacement.
   Preserve live handle tables and drop displaced maps after releasing guards.
   Inventory after a switch must read each node through its retained view.
   Incompatible experiment checkpoints must be rejected and retained.
   Separate the bounded metadata layout from private-file materialization. Lazy
   immutable entries need no new backing file. Carried dirty data needs a fresh
   inode-specific overlay that preserves its original base and sparse extent
   semantics, with a new receipt identity. Materialize through the exact existing
   Model, checkpoint owner, store factory and shared storage budget. Include
   retained data and failed allocation debt in quotas; cancellation does not
   imply that private backing files were removed. An absent writable underlay
   remains unsupported until its typed head policy is implemented.
   Copying private extents requires fresh storage offsets and additive accounting,
   rather than recovery's historical reservation floor. Short writes may split an
   extent across noncontiguous stored ranges; bound that fragmentation and charge
   every requested retry before issuing IO. Validate receipts against the source
   inode before reissuing them for the replacement inode. The source file and its
   receipt cell remain independent of the prepared replacement.
3. Use an external-provider anchor handshake around the actual hold/sync
   command ordering. Prebuild the install state and source/candidate checkpoints,
   synchronize immutable dependencies, then seal the exact source metadata before
   durably recording a bounded checksummed
   pending intent with transaction ID, exact checkpoint identities and expected
   old/new normal and staged anchors before changing those anchors. The pending
   owner retains its freeze and storage through caller cancellation. Verify the
   durable expected anchors before an installation that performs no fallible IO
   or allocation, then durably complete the intent before acknowledging/thawing.
   Uncertain outcomes retain pending ownership and require explicit old/new
   reconciliation at startup; branch latest is never a recovery substitute.
   Checkpoint identities must hash the exact retained encoded bytes, including
   their checksum. Loading a snapshot and reencoding it does not establish the
   identity of the acknowledged source file. Bound source/candidate snapshots,
   encoded buffer capacities and scratch memory together; append-storage debt
   is not a transient-memory budget. Avoid concatenating another full bundle of
   both images or reencoding an already prepared candidate during publication.
   A loaded checkpoint may predate frozen local edits. Move the prepared source
   snapshot into owner-bound publication, validate it against the actual current
   checkpoint under the checkpoint owner's serialization lease, and durably
   publish its exact encoded bytes before recording intent. Budget that current
   load's encoded, decoded and validation allocations before allocating them.
   Consume the source plan and captured candidate together, checking their exact
   shared freeze acquisition before moving the source snapshot. Keep the resulting
   acknowledgment paired with that candidate; matching namespace epochs alone
   cannot bind a separately prepared candidate to an acknowledgment.
   Retain the serialization lease with the source acknowledgment through intent
   admission so an ordinary fsync cannot replace the acknowledged image. Keep
   transition artifacts separate from ordinary checkpoint `pending`; incomplete
   artifacts must block startup write admission until reconciled.
   Use a fixed private transition directory whose presence blocks ordinary
   publication without parsing its contents. Retain the mutation fence once
   artifact IO starts, including partial creation or sync failure before intent
   is committed; dropping preparation must not thaw that unresolved mount.
   Live quarantine must also withdraw the ready provider registration while the
   command token still excludes new writers and sharing siblings are quiescent.
   Poisoning the Model alone does not stop repository loading from opening stores
   through an otherwise ready provider before operation admission.

The recovery foundation uses v5 namespace checkpoints and v2 append-only backend
provenance. It distinguishes the immutable original root from active underlays
and logical held heads, retains revision-qualified views and unchanged proven
child bindings, and preserves monotonic exposure floors. It bounds additional
views at 1,024 and original exposures at 65,536. Graph ordering, duplicate records,
allocator overflow and size limits are checked before loading target states.
Scope indexes and incremental provenance sizing avoid quadratic scans. Fresh
restore verifies immutable node metadata before reconstructing private data and
keeps target authorization active even when the selected subtree is absent.
The root hold/sync path installs prepared namespace routing, publication ownership
and Model maps under ordered guards. Displaced files and state remain retained
by the external transition worker, preserving existing handles. Preparation
inventories active underlays and supports subsequent root transitions.

The internal metadata-seal primitive compares the exact freeze, namespace
generation, inode high-water mark and shared mutation epoch under ordered
namespace, backend-owner and metadata locks. Namespace cache changes, receipt
invalidation and backend exposure insertion all advance that epoch, including
changes through retained backend clones. Seal acquisition performs no snapshot
allocation or IO. Existing pinned reads and cached metadata remain usable; cache
misses and new exposures fail with `Busy`. Cached read paths release namespace
locks before policy or backing IO; cold hydration may delay seal acquisition.
Dropping the seal reopens metadata admission before releasing the freeze.

New Model file-cache entries advance that same mutation epoch under the cache
map lock. A metadata seal refuses missing entries before allocation or store IO;
existing shared cells remain usable and may finish populating. Owned candidates
check the epoch while cloning the map and validate the full source outside that
lock. Later map growth therefore invalidates a preparation instead of leaving
two wrappers with separate append locks for one unchanged inode. Cancellation
retains the announced empty cell and epoch rather than rolling them back.

The seal is neither an authorization grant nor an install/intent capability.
Publication must revalidate policy independently. No namespace or file mutex may
span anchor IO. Installation must swap preallocated state under short ordered
locks and release replaced state after unlocking.

The installed reader must own immutable routing and authorization data separately
from the preparation freeze. The isolated `PreparedTargetView` retains that
freeze and cannot become the live backend. Existing open files retain their
original backend and policy; new namespace entries need an explicitly bound
policy for their installed view. Candidate authorization alone does not grant
generic writes to immutable target nodes.

Backend allocation admission distinguishes prepared, active and retired objects.
Link resolution checks admission before IO and again under the final insertion
lock. Known routes remain readable, subject to normal authorization; refused
insertion leaves routes, provenance and allocator floors unchanged. Provenance
completeness is independent, so a complete prepared backend stays checkpointable
while new link allocation is disabled. Root installation transfers exact allocator
ownership to the prepared backend and retires new allocation on the old backend;
existing pinned routes remain readable subject to their captured policy.

Activation must retire allocation on the exact historical backend while enabling
the prepared backend. Independent restored backends may share a metadata
gate without belonging to the same publication lifetime. Runtime claims enforce
exclusive ownership of the namespace and its active backend. Root installation
uses a typed successor handoff under that same claim; reference counts and
read-only lineage pins cannot authorize the handoff.

Pinned directory handles retain their original effective authorization binding
and recheck captured keys on each page, including after ordinary unlink. Live
installation preserves that binding while lookup through inode 1 follows the
active root. New allocation on retired bindings is refused.

The root hold/sync callback uses the exact running Model and an externally owned
publisher. It writes source and candidate images before durable intent, checks and
publishes normal anchors, installs the prepared view, then publishes the selected
checkpoint before deleting intent artifacts. A cleanup marker remains durable
until checkpoint and artifact-directory changes are synchronized. Startup uses
the actual pending owner’s strict stores to select only exact old or new anchors;
mixed anchors, unknown pending bytes and incomplete unprovable bundles refuse
admission. Caller cancellation retains actual IO ownership through resource
completion and publisher join. This protocol orders separate stores; it does not
claim cross-store atomicity or physical power-loss qualification.

`instance::strict_anchors::read` supplies mutable-store-only current, branch and
staged facts for that handshake. Only an absent key or zero value denotes an
empty current/staged anchor; payload loss, authorization and other store failures
remain errors. Branch encoding must be canonical and nonzero. The reader performs
no legacy fallback, repair or flush; callers must retain the store claim and
serialize writers. Prepare a replacement staged signature with
`state::rebase_staged_state` before recording intent. The ordinary
`rebase_staged_anchor` helper mutates anchors while computing the replacement and
does not provide the required pre-intent boundary.

The pending worker must retain the command's actual client write token, repository
lock and store claims. Revision backends intentionally discard command locks;
their lifetime and the VFS owner claim cannot serialize normal anchor writers.
A linked scope uses its child repository partition under the same canonical
root's command lock and instance identity. An anchor-only owned context must
retain those locks without retaining the filesystem provider, which could form
a cycle through the checkpoint owner. Sharing a command token blocks subsequent
commands but does not quiesce sibling tasks already sharing that token; the
handoff must establish one anchor-writing phase after those tasks finish.
A private bootstrap gate distinguishes each explicit repository load from its
inherited contexts and counts finite work separately. Exclusive reservation is
nonwaiting: other load principals or any retained work return Busy. Dropping the
last pin on an exclusive reservation reopens admission only before terminal
retirement. Each registration retains a private gate and shared reservation
claim. Dropping the
publication owner withdraws the provider immediately; retention-only claim pins
keep the retired path reservation until their last drop, without retaining
publication rights. Registry entries hold only weak claim references.
Bound resolution verifies metadata and atomically pairs a ready provider with a
load principal and that exact claim. Finite work rechecks the same ready entry;
its IO lease retains the work and claim before releasing inherited accounting.
Listing producers started inside such a scope retain the lease through actual
spawned work, while buffered results require no further store admission. These
guards do not supply client tokens or store claims, and consumers must retain
their actual resources.
Private command-store adapters admit supported store methods and propagate their
IO leases to spawned work. Returned store guards retain store claims, not root
admission; callers must retain the load and actual job resources alongside them.
Arbitrary raw store-lock manipulation is outside this boundary.
Working repository loaders select the exact ready admission before opening or
upgrading stores. Writable loads acquire and validate the canonical root Client
token before counted admission. Opening IO retains that token, finite work and
inherited accounting through actual job completion. The context and both store
adapters retain one issuer, including contexts stripped of command locks and
escaped store handles. Listing producers retain actual claims and admission after
caller or receiver cancellation. Working command loads disable periodic
incremental maintenance, whose sleeping workers would retain opening admission;
finite explicit maintenance remains admitted through the adapters. Newly built
mounted-owner contexts carry no command admission. Exclusive root command handoff
quiesces sibling loads and actual store jobs while retaining the canonical Client,
store claims and anchor serialization through durable publication and live
installation.
Long-lived authentication refresh tasks release command IO leases while
preserving credential rotation; initial authentication remains counted. Their
execution contexts also need an ownership
boundary before loader adoption: command callback dispatchers and opaque caller
state can retain service resources after the IO lease is detached. The provider
resolver itself is weak. Initial working-store connections already use the
background-owner context. Explicit owner-aware remote stores also scope lazy
connection, session and admin setup to a detached owner while preserving caller
session correlation and error reporting. Explicit online working startup selects
this path using a live credential source; ordinary loaders retain their existing
connection behavior.
An owner reconnect must retain a live credential source from an established
connection rather than replay token strings saved in that context. The explicit
owner connection path uses that exact source outside the ordinary connection
cache; adopting a different credential authority requires an explicit handoff.
Cached gRPC streams retain IO ownership and bounded capacity per logical request
through queueing, coalescing, retries and response or terminal failure, including
after caller cancellation. Idle readers retain neither command context nor their
request sender. Session retirement drains accepted work and half-closes request
bodies; a stopped generation cannot reopen. Stream failure closes and disposes
queued and in-flight work outside admission and map locks.
Cooperating processes are excluded by repository/store locks. After process
exit, startup must reconcile pending intent before admitting repair or write
commands. Checked store flushes and strict anchor rereads belong to the owned
worker; background post-command flush is not a durable acknowledgment.
Production bootstrap needs an owned recovery-admission check before mutable
store initialization and cache preparation. A check only in Model restore or
the later recovery-owner constructor cannot protect those earlier operations.
Keep this coordination in Lore without introducing a revision-layer dependency
on the VFS crate.
The revision metadata initializer has a separate pending-entry path. It consumes
an unshared canonical-root Client token before acquiring exclusive admission and
retains both through the actual blocking job. Its opaque result exposes metadata
facts only; dropping the result, or failing initialization, terminally retires the
exact registration before releasing the token and inherited work accounting.
Provider publication checks the gate before its callback and again before
installation. The typed store initializer consumes that metadata and retains the
same pending owner through actual store work and its opaque result, alongside
any distinct caller accounting. Neither result grants provider publication or
qualifies VFS recovery.
The trusted owner-build handoff requires unique actual-job and Client ownership.
Its external publication permit retains resources through preflight, releases
the Client while exclusion remains closed, and installs the provider and reopens
admission in one exact-claim commit. The caller must retain deferred cleanup
until outer Record guards are released. Failure requires quarantine before failed-owner drain.
Startup integration must keep this permit outside Input, Record and Model;
the long-lived mount retains neither the permit nor the command token.
Working storage provides separate create-new and existing-only constructors.
Existing-only opens never create a missing root or owner lock; create-new refuses
even an existing empty directory. The private `.working-backing-v1` record binds
the repository and instance identities to the canonical backing directory using
bounded raw Unix path bytes and a checksum. Its revision-layer reader rejects
missing, corrupt, foreign or unsafe records without substituting a fresh directory.
The bound pending-store initializer rechecks the independently expected backing
root before opening stores and after setup. Creating stores permits only the
three ordinary metadata files and that validated record; the generic initializer
keeps its three-file whitelist. This path refuses shared stores and online remotes;
offline configurations may retain a remote URL. The startup factory must still
establish, retain and transfer actual recovery ownership before store setup.
Ordinary recovery also has an existing-only constructor: it acquires the existing
lock without creating, syncing or repairing files, and rejects transition markers
before and after locking. Storage and recovery owners can compare current paths
with their held directory and lock descriptors before setup proceeds. These
read-only checks detect replaced ownership paths; they do not make path access
atomic against hostile writers or authorize reconciliation.
Startup input owns an opaque, one-shot recovery admission. Setup consumes it
before cache or session creation, checks the exact storage owner, registration,
location and checkpoint limit, and transfers the same Recovery into the Model.
A failed transfer stays consumed and cannot reopen recovery on retry. The test
compatibility constructor acquires real recovery ownership after prepared stores;
it does not establish the required production ordering before store initialization.
Pending state must also avoid owning a candidate through its own checkpoint
owner: the candidate retains its Model, which already owns that checkpoint
owner. Use an external coordinator or an explicitly noncyclic resource owner.

Sync also updates shared branch latest/status/history and last-sync bookkeeping.
These are distinct from the instance's current-branch anchor and may be changed
by another instance. Define their ordering and partial-failure behavior separately;
recovery must neither roll back an unrelated branch advance nor use branch latest
to infer the mounted view.

### Cross-platform decisions

1. **Open-file semantics during sync:** read-only transitions retain immutable file
   handles and directory enumeration. Root-relative lookup follows the active view;
   retained nonroot directory inodes retain their generation. Writable transition
   conflicts and FSKit cache behavior still require platform evidence.
2. **Conflicting local edits:** define precise reject/preserve behavior for sync,
   link-pin updates and parent/child commits; no invalidation may discard dirty data.
3. **Overlay location and ownership:** choose repository instance layout, locking,
   retention and downgrade behavior before persistent writes ship.
4. **Namespace fidelity:** establish supported case/normalization behavior and node
   kinds from Lore's current tree format; test platform differences explicitly.
5. **Watchers:** cache invalidation is not watcher delivery. Specify and test what
   build tools and editors observe on ordinary edits and out-of-band sync.
6. **Authorization after caching:** retain user/partition scope for shared caches
   and define token expiry/revocation handling. Knowing a digest never grants read
   access to its content, including across links.

## Validation and stopping points

Run nightly rustfmt and the crate's tests/Clippy on the host. Compile and test the
Linux adapter on a native Linux FUSE host; the released no-libfuse dependency does
not support a macOS-hosted cross-build.
Existing CI has a Linux workspace unit job; target-gated tests should participate
there. Mounted integration tests require an explicit job with `/dev/fuse` and
mount-helper availability and must clean up in failure paths.

No broad workspace refactor, CLI default switch, production deployment, Apple
account action or cloud-host creation is necessary for the first milestone. End
that milestone with reviewable source, focused test results, a runnable Linux
harness and an exact list of remaining gates. Then take the next dependency-ready
slice rather than merging several durability and integration changes at once.
