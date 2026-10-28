# Native FSKit prototype

The `loreprobe` resource is a disposable in-memory filesystem for testing macOS 27
FSKit cache control. It implements lookup, enumeration, reads, symlinks, raw
`user.sha256`, writable `edit.txt`, and revision switching. Its data disappears on
unmount; `fsync` does not provide persistent storage.

A separate experimental `lorerevision` resource binds Swift to the shared Rust
read-only core and a real pinned Lore revision. Its content and authorization use
the normal Lore backend. Writable operations are rejected; the probe model is not
used as repository content.

The extension builds with Xcode 27 and targets macOS 27. The host app only provides
an extension container. Neither the build script nor the tests install software,
register an extension, or mount a filesystem.

## Build and local validation

From the repository root:

```sh
python3 contrib/fskit-prototype/build.py --identity -
python3 contrib/fskit-prototype/bridge_test.py
python3 contrib/fskit-prototype/Tests/BuildTests.py
python3 contrib/fskit-prototype/Tests/PreflightTests.py
xcrun swiftc -parse-as-library \
  -module-cache-path target/fskit-prototype/module-cache \
  contrib/fskit-prototype/Sources/Model.swift \
  contrib/fskit-prototype/Tests/ModelTests.swift \
  -o target/fskit-prototype/model-tests
target/fskit-prototype/model-tests
```

`build.py` builds the explicit `native-bridge` feature with the Rust `fskit`
unwind profile, compiles Swift with warnings treated as errors and verifies the
bundle signature. `--rust-profile dev` supports local iteration. Rust dependencies
must be available for the locked offline build. `bridge_test.py` links standalone
Swift contracts against a separate test-fixture static library; it loads no FSKit
module and accesses no real repository or account. Signing identity is always
explicit: `--identity -` selects ad-hoc signing for build verification. Runtime
loading needs a development identity and a matching macOS provisioning profile
from a team supporting the **FSKit Module** capability. Xcode rejects this
capability for Personal Teams; a signing certificate alone is insufficient.
The extension includes the FSKit module, app sandbox, team and application identifier
entitlements. Development builds require an explicit `--team`, checked against
the selected signing certificate. Bundle identifiers use the neutral
`org.example.lore.fskit-probe` namespace; adapt the matching host, extension and
status identifiers together for your own registered application.

```sh
python3 contrib/fskit-prototype/build.py --identity "$SIGNING_IDENTITY" --team "$TEAM_ID"
python3 contrib/fskit-prototype/provision.py --identity "$SIGNING_IDENTITY" --team "$TEAM_ID"
```

`provision.py` uses Xcode automatic signing. It can contact Apple, create development
provisioning assets and register this Mac with the selected team. Use it only with
the developer account owner's authorization. `--generate-only` creates a reviewable
Xcode project without building or contacting Apple. The project is written beneath
`target/fskit-prototype/Provision.xcodeproj`. Provisioning requires a suitable account
signed into Xcode, with current program agreements accepted by its Account Holder.
The explicitly supplied identity and team configure both Xcode targets; the same
identity signs the host and status tool. No account-specific identity is selected
by a script default. Run provisioning after `build.py`: rebuilding replaces the signed
extension and requires provisioning/registration again.

## Experimental pinned-revision resource

The real mode is read-only and uses the versioned
[shared Rust bridge](../../lore-fskit-bridge/README.md). Reads, metadata, directory
cookies, owning repository/link authorization and trusted raw SHA-256 lookup stay
in Rust. Swift grants read caching only after explicit backend revalidation at
open/cache upgrade. Revoking bytes already cached by the OS, including existing
file descriptors, is a separate native qualification gate.

A host preflight knows the actual target passed to `mount`. It requires an existing
empty, user-owned, non-symlink target outside `.lore`/`.urc` metadata and disjoint
from the source checkout, digest cache and bootstrap config. It resolves aliases,
rejects unsafe cache/config permissions and never overwrites an existing config.
The default invocation validates only:

```sh
python3 contrib/fskit-prototype/revision_mount.py \
  --repository /absolute/source/checkout --revision main@LATEST \
  --target /tmp/lore-revision-view \
  --config /private/approved-container/bootstrap.json --offline
```

Only an explicit `--mount` writes a fresh mode-0600 bootstrap file and invokes an
already-enabled module with `mount -r`. Optional `--identity` selects an existing normal Lore
identity; `--digest-cache` enables verified cache-only digest lookup without
backfill. Keep all metadata, credentials, stores, config and cache outside the
mounted namespace. The extension needs actual sandbox access to them; a generic
URL does not grant filesystem or keychain access. Network-client entitlement
permits the normal Rust network path but supplies no authentication or file grant.

Direct/manual `lorerevision://mount?config=…` mounting bypasses host preflight.
The selected SDK does not expose the OS target path to this volume adapter, so the
extension cannot independently verify the target/source geometry. Use the helper
and qualify this boundary under actual signed extension permissions before relying
on the deployment.

Real revision volumes advertise case-insensitive, case-preserving names and no
symlink representation. Lookup returns stored spelling using a bounded 16,384-entry
parent scan; larger scans fail explicitly. The original probe's static personality
is case-sensitive while per-volume capabilities differ; actual FSKit personality
selection and cache/name behavior remain runtime gates. The probe scheme/model
remain unchanged. Read buffers are filled in bounded 1 MiB Rust calls; native large
request behavior, malformed byte names, normalization and case collisions still
need qualification.

The signed generic-URL fixture mounts on macOS 27.0.1. Fixture, standalone
Swift and preflight checks do not prove that a signed extension can access normal
Lore credentials/stores or mount a real repository.

## macOS 27 runtime test

Open `target/fskit-prototype/LoreFSKitProbe.app`. In System Settings → General →
Login Items & Extensions, select **By Category → File System Extensions**, then
turn on LoreFSKitProbe. On macOS 27.0.1 the generic **By App → LoreFSKitProbe →
FSKit Modules** switch returns to off; use the category-specific pane.

For an explicit registration refresh:

```sh
pluginkit -a target/fskit-prototype/LoreFSKitProbe.app/Contents/Extensions/LoreFSKitProbe.appex
target/fskit-prototype/probe-status
```

The status tool queries FSKit itself, whose enablement state differs from PlugInKit's
user election. The tool must be development-signed to see this development module;
an unsigned/ad-hoc client may receive a filtered list. Re-enable after replacing a
registered bundle. Do not disable system security settings. Successful signature
verification and enablement do not prove that the extension can launch: a missing
profile produces an AMFI rejection and an ExtensionKit resource-loading error.

Use an empty, disposable mountpoint, never a Lore checkout:

```sh
mkdir -p /tmp/lore-fskit-view
mount -F -t loreprobe loreprobe://fixture /tmp/lore-fskit-view
python3 contrib/fskit-prototype/mount_test.py /tmp/lore-fskit-view
umount /tmp/lore-fskit-view
```

The script verifies the prototype marker before writing its disposable fixture,
requires a fresh mount, and runs the worker with a 90-second deadline. The deadline
bounds the test process, not an unresponsive kernel mount; inspect any remaining
mount separately after a timeout. Retain terminal output and OS build number.
For independent qualification, `--suite data-cache` and `--suite writable` each
require a fresh mount. Their output names the tested subset; neither substitutes
for the default complete namespace-switch suite.
`--suite sync` separately requires a synchronization callback before `fsync`
returns. The writable data test alone does not establish durability.

`python3 contrib/fskit-prototype/bazel_test.py /tmp/lore-fskit-view --bazel /absolute/path/to/bazel`
tests a fresh fixture with an existing Bazel 7.5 executable. It creates a temporary
workspace whose input links to the mounted file, checks initial and same-size
changed outputs and execution-log SHA-256 values, and verifies digest-xattr
requests using fixture counters. Build logs are retained at the printed path.
The script never mounts or unmounts; unmount the fixture afterward.

The suite exercises:

- SHA-256 as exactly 32 raw bytes, with no filesystem read callback for lookup.
- Same-size revision changes after warming read and name caches.
- Existing read descriptors and read mappings observing the switched version.
- Negative lookup invalidation, addition, deletion, file-to-directory changes,
  symlink targets and directory enumeration, including switching back.
- Digest revocation at writable open, same-size writes, truncate and writable mmap.
- Rejection of attempts to overwrite the computed digest.

`value.txt` is immutable to filesystem callers. A root control xattr changes the
backing model directly and invalidates observed items and the root directory using
FSKit. No synthetic rename or write through mounted paths performs invalidation.
`edit.txt` remains outside revision switching so `.invalidate` cannot discard its
dirty pages. A failed cache transition leaves the test volume in a failed state;
remount it rather than continuing.

This is a capability probe, not a concurrent transactional revision engine.
Visibility during a transition, watcher events, busy unmount, restart recovery,
large repositories, and Bazel running on the mount are separate acceptance gates
in [the implementation plan](PLAN.md). The fixture computes SHA-256 from its tiny
in-memory data; production uses precomputed content-bound digests.

## Isolated cache and synchronization diagnostics

`cache_probe.py` reports callback counters and exact native errors from a disposable
fixture. Its JSON output is diagnostic evidence, not a namespace or durability
acceptance result. Existing `mount_test.py` acceptance checks remain unchanged.

The diagnostic fixture accepts these explicit `loreprobe://fixture` query options:

- `ids=legacy` keeps the fixture's original identifiers; `ids=reserved` maps the
  kernel-facing root to `FSItem.Identifier.rootDirectory`, its parent to
  `parentOfRoot`, and all other identifiers above the reserved range. Internal
  fixture identities remain unchanged.
- `cache=read`, `none`, `through`, or `back` selects compatible cache grants.
- `sync-error=1` makes an actual volume synchronization callback return `EIO`.
- `revision=1` starts with the directory variant visible, without a live switch.

For example, after explicitly mounting a fresh diagnostic fixture:

```sh
python3 contrib/fskit-prototype/cache_probe.py /private/owned/mount \
  --switch --negative-only --open-fd --item root --mode none --coherency none --action invalidate
python3 contrib/fskit-prototype/cache_probe.py /private/owned/mount --kind sync
```

Use separate fresh mounts for independent cases. The cache diagnostic can vary the
control's origin (`--origin root|file`) and dispatch delay (`--delay-ms 0|100|250`)
to examine reentry timing. It records whether the negative lookup was cached before
switching; a delayed observation alone is not proof of invalidation. The explicit
`--negative-only` mode requires `--switch --item root`. It warms the absent path
without enumerating unrelated files, preserves strict transition failure checks,
and reports both lookup-counter samples and post-transition visibility. Cache controls
only target immutable fixture items. Dirty `edit.txt` is rejected, and `revoke`
requires a separate explicit `--allow-revoke` choice.

The synchronization diagnostic compares file `fsync`, `F_FULLFSYNC`, writable
mapping flush, and directory `fsync` against volume-level callback counts and
flags. These observations do not establish persistent storage for the RAM model.
Apple’s [synchronization contract](https://developer.apple.com/documentation/fskit/fsvolume/commonoperations/synchronize(flags:replyhandler:)) describes a volume/resource operation; a missing callback
for one syscall is evidence about delivery on that tested path.

`diagnostic_build.py` compiles Swift against a preserved bridge archive and its
recorded linker requirements. It requires explicit `--identity` and `--team`
arguments and checks the preserved extension entitlements against that team and
the example bundle identifier, without running Cargo or provisioning. Its default output is
`target/fskit-prototype/diagnostic-build/LoreFSKitProbe.app`; the explicit
`--replace-fixture` option also copies that bundle to the ordinary fixture output.
Registration and runtime mounting require separate authorization. Preserve the
validated bundle, archive checksum, sources, and logs before preparing diagnostics.
Both standalone extension build commands use the `_NSExtensionMain` linker entry
point required by the ExtensionKit host.

## Standalone persistent-marker engine

`PersistentMarker.swift` and its standalone fault tests implement bounded backing
record publication independently of FSKit. Run them without mounting, signing,
registering, or running Cargo:

```sh
python3 contrib/fskit-prototype/persistent_marker_test.py
```

The engine accepts an existing caller-owned private parent directory and creates
an exclusive per-run UUID directory. It retains directory and owner-lock
descriptors, rejects hardlinked or symlinked records, and verifies the named run
and lock still match its ownership claim before mutation. A serial worker admits
at most four actual-I/O jobs. Cancellation of a waiting task does not release an
accepted job; closing the worker drains actual work and releases its claim before
acknowledging completion.

The default RAM fixture is unchanged. The opt-in persistent fixture stores only
`edit.txt` inside the extension's sandbox container. It accepts `cache=none`,
`persistent=<UUID>`, and `create=1` for a fresh run or `create=0` to reopen it.
Writes and truncates complete their backing-store publication before their FSKit
callbacks return; unload drains admitted work. The filesystem's namespace is
fixed: create, unlink, rename, and other namespace mutations remain unsupported.

With the signed probe already enabled, run the mounted write, truncate, and
clean-remount check using:

```sh
python3 contrib/fskit-prototype/persistent_mount_test.py
```

The test creates a unique private run in the extension's container and leaves
its bounded records there. It cleans up its own mount. To test the write callback,
concurrent writes and injected publication failure without mounting, first build
the bridge with `build.py`, then run:

```sh
python3 contrib/fskit-prototype/persistent_volume_test.py
```

An isolated extension-process crash check records the acknowledged generation
and bytes outside the mount, terminates the verified probe process, and checks
that a fresh mount reads that exact generation through FSKit:

```sh
python3 contrib/fskit-prototype/persistent_crash_test.py
```

The crash check refuses to terminate the probe if another FSKit mount is present
or the process identity is ambiguous. Clean remount and extension-process crash
recovery do not establish OS-crash or physical-power-loss durability.
The fixture does not yet use a Lore repository or qualify namespace sync.

The store uses an exclusive ownership claim and bounded actual-I/O admission.
Immutable generation records contain a version, run identity, sequence, length,
checksum, and at most 1 MiB of file data. Retention is bounded to current,
previous, and one pending generation. A new generation must be created
exclusively, completely written, file-synchronized, and directory-synchronized
before a separately written and synchronized pointer names it as current and
retains the prior current as previous. Atomically replacing that pointer and
successfully synchronizing its parent directory precede the write or truncate
callback's success. File synchronization checks both `fsync` and `F_FULLFSYNC`.
The prior current generation's data file is never overwritten.

A fixed `mutation.pending` record is exclusively created and synchronized before
publication. It is removed only after pointer publication and reclamation complete,
followed by checked directory synchronization. A pending marker or unexpected
record inventory refuses ordinary recovery. Any mutation failure poisons that
engine instance. If the final marker-removal directory sync fails, a valid new
pointer may be visible without a pending marker; reopening exposes that image only
as a disk fact and never invents a pre-crash callback acknowledgment. Crash tests
must compare the external parent's exact acknowledgment record.

No generation is deleted before durable pointer completion. If pointer replacement
succeeds but directory synchronization fails, both the prior current and pending
generation remain available, along with the previous generation; further mutation
is refused pending explicit reconciliation. Reclamation of an older generation
occurs only after durable completion, with checked directory synchronization.
Failed or uncertain reclamation also blocks the next mutation rather than
exceeding the three-generation bound. Recovery must not guess that the highest
sequence was acknowledged.

Every result must be checked. Unsupported synchronization, including `EINVAL`,
is a failure rather than durable success. Cancellation cannot release ownership
or refund an admitted job while its actual I/O continues. Unload must drain those
jobs. The checksum verifies the record and does not become a digest xattr for
mutable content.

Qualification must establish these ordering and recovery facts:

- Pause a write callback before publication. Distinguish a still-pending POSIX
  write from one already acknowledged: a concurrent `fsync` need not include an
  unfinished write. Test synchronization ordered after an acknowledged write,
  and synchronous mapping flushes, against the corresponding durable sequence.
- Inject short writes and file-sync, replacement, and directory-sync failures.
  Failed publication must not acknowledge success or discard the last valid
  record. A missing volume-level synchronization callback alone does not decide
  whether durable write-through acknowledgment is possible.
- Record the exact acknowledged sequence and expected bytes in the external test
  parent before a crash. Terminate only a verified fixture PID, executable/start
  identity, and run token, after proving the shared extension process serves no
  unrelated volume. Otherwise, do not terminate it or restart a system agent.
- Restore on a fresh process and mount with caching disabled, and verify bytes,
  length, sequence, and an actual read callback. This establishes process-crash
  evidence only. OS-crash and physical power-loss guarantees require separate
  qualification, even when `F_FULLFSYNC` succeeds.

Synchronizing the backing record's directory name does not qualify directory
`fsync` or name mutations in the mounted filesystem. The RAM fixture and its
existing acceptance assertions remain unchanged until these gates are proven.

## Validation scope

The extension compiles against SDK 27.0 with Xcode 27.1 on macOS 27.0.1 (26A434).
Bundle verification and local model tests pass. PlugInKit registration, discovery
by a development-signed FSClient and the category-specific enablement switch work.
The development-provisioned generic-URL fixture mounts successfully. The native
`--suite data-cache` and `--suite writable` checks pass on separate fresh mounts:
raw SHA-256 lookup causes no file reads; same-size data changes reach existing
descriptors and read mappings; writable open removes the digest before the first
write; write, truncate and writable mmap preserve digest unavailability.

The default complete suite still fails live namespace switching: a warmed
negative lookup hides a newly added name after the transition. Root data-cache invalidation returns an invalid-argument error on this OS build,
including with an open descriptor retained for the root directory.
Live namespace switching is not qualified; partial suite passes do not satisfy
that gate.

With reserved root/parent identifiers, isolated root invalidation still returns
`kIOReturnBadArgument` with an open root descriptor, for both `none`/`noCache` and
`readWithCache`/`readCache`. An opened regular file succeeds with read caching under
both identifier layouts. A non-root directory present at initial mount also
accepts read-cache invalidation with a retained descriptor. These isolated calls do not establish invalidation of warmed negative lookups
or live type changes. An opened regular file also accepts `none`/`noCache`
invalidation.

The explicit negative-only diagnostic reaches a strict `ready:1` transition
without enumerating unopened files. On fresh reserved-ID mounts, the two absent
lookups produce unchanged lookup/miss counters (`8/8` followed by `8/8`), proving
the negative result was cached. With the root descriptor retained, both
`readWithCache`/`readCache` and `none`/`noCache` root invalidation return
`kIOReturnBadArgument`; `appeared.txt` remains invisible and the lookup counters do
not advance. This blocks this tested namespace-switch path on macOS 27.0.1. It
does not establish that every FSKit namespace strategy is impossible.

The native `--suite sync` gate also fails: `fsync` returns success without a call
to the fixture's `synchronize` handler. Writable data and mmap tests do not prove
durable flush delivery for this URL-backed resource. A production writable mount
requires a validated durability path before it can acknowledge durable writes.
The isolated diagnostic also observes successful file `fsync`, `F_FULLFSYNC`,
writable mapping flush and directory `fsync` without a volume synchronization
callback under each of `none`, `read`, `through` and `back` cache policies. An
injected callback error is therefore not delivered through those tested calls.
This does not rule out durable acknowledgment implemented in write callbacks.

A real offline repository inside the extension's sandbox container passes native
listing, file reads, case-insensitive lookup, read mmap, absent-digest behavior
without a configured cache, and write rejection. This validates the shared Rust
backend with container-local stores. Repositories outside that container, remote
credentials and linked-repository mounts remain unverified. Native Bazel 7.5
fixture builds pass the raw-xattr and same-size rebuild workload; this does not
qualify a real repository's digest-cache publication or a wholly mounted Bazel
workspace.

See [Apple's cache invalidation contract](https://developer.apple.com/documentation/fskit/fsvolume/kernelcachecoherencyaction/invalidate)
and the [shared digest implementation](../../lore-vfs/src/digest.rs).
