# Lore VFS implementation plan

The task breakdown for common code and Linux, including parallel ownership and
acceptance criteria, is in [the implementation plan](../../lore-vfs/PLAN.md).

## Scope and current implementation

Target native FSKit on macOS 27 and FUSE on Linux. Preserve the Windows SWFS
backend. Expose a trustworthy whole-file SHA-256 xattr for Bazel. Roll out explicit
backend selection before changing any platform default.

The executable prototype in this directory is independent of Lore repositories.
The `lore-vfs` crate contains the common read-only core, a pinned Lore revision
adapter, an experimental Linux FUSE adapter, verified digest indexing and
ingestion hashing, and bounded overlay namespace/checkpoint components. A native
working-tree provider and mounted writes still require integration.

The existing mount implementation is Windows SWFS:

- `lore-revision/build.rs` rejects SWFS builds on macOS/Linux.
- `lore/src/repository.rs` resolves `LoreVfsType::Default` to SWFS.
- `lore-revision/src/fs/filesystem_provider.rs` accepts injected external operations
  with explicit state and commit lifecycle hooks. Existing OS/SWFS dispatch remains
  static; the common VFS working-tree provider is a separate integration layer.
- `lore/src/service/initialization.rs` manages SWFS-specific startup. Mount teardown
  needs a real lifecycle rather than the current no-op unmount path.

Existing immutable range reads, content verification and caching should remain
shared. Do not create another content store or bypass authorization.

## Architecture

`lore-vfs` owns platform-neutral inode/handle state, revision views, an external
writable overlay, digest availability and mount lifecycle. It may depend on
`lore-revision`; the revision crate must not depend back on it. Extend the revision
provider boundary to accept an externally supplied implementation, retaining
static dispatch for existing providers where useful. Inject the provider factory
from the application/service layer.

The macOS extension is Swift with a small versioned C ABI into the Rust core.
Start with the Rust core hosted in the extension process. Repository ownership,
credential access and storage access must be proven under actual extension
permissions before adopting that deployment. If those permissions require a
service process, introduce a bounded authenticated local transport behind the
same core API. Do not put network/store logic in Swift.

The Linux adapter uses `fuser` against Linux FUSE. Pin a released version after
checking the supported kernel baseline and mount helper requirements. Do not
assume its macOS device transport can communicate with native FSKit.

The C ABI uses opaque retained handles, explicit buffer ownership/free functions,
fixed-width sizes, operation IDs and explicit error results. No Rust panic or
Swift exception may cross it. Cancellation must finish or abort exactly once;
unmount drains callbacks before releasing core state. Negotiate ABI version at
startup and reject incompatible pairs.

Store `.lore` metadata, overlay files, journal and caches outside the mount. No
backend operation may recurse into its own mounted namespace. Mount records bind
canonical mountpoint, repository, user, backend and instance token.

Linked-repository traversal is required in both adapters. Resolve every lookup,
enumeration and read to its owning repository, pinned revision and node, including
nested links and source-subtree remapping. Authorize the target partition separately;
parent access never grants child access. Keep repository and revision in inode/cache
identities so equal node numbers from different repositories cannot alias. Contract
tests must include directory enumeration across links, differently pinned instances
of one child, and a parent-readable but child-denied user.

## Digest contract

`user.sha256` is a read-only computed attribute containing **32 raw SHA-256 bytes**
of the complete visible regular file. It is not hexadecimal text, a BLAKE3 address
or the hash of a fragment/reference list. Unknown or mutable content has no digest:
ENOATTR on Darwin, ENODATA on Linux. Caller writes/removals are rejected. Implement
listxattr, size probes and ERANGE for undersized buffers consistently.

Associate persistent digest records with full Lore Address (including context),
whole-file size, algorithm and record version. Do not trust path metadata or size
as evidence of identity. `contrib/bazel/crates/rbe-lore/src/workspace.rs` contains
legacy SHA-256 metadata whose same-size staleness limitation requires explicit
handling; it is not a source of trusted fast digests without validation.

Compute SHA-256 during ingestion/commit over the same verified full-file stream
that establishes immutable content. Existing data can acquire digest records via
bounded background reads. A cache hit performs no content hydration. A miss returns
attribute absence promptly so Bazel hashes the file normally. Partial/error reads
never publish a record. Publish only if the inode generation and immutable content
identity still match the completed job. Records are a removable cache; old clients
can ignore the separate versioned index. No fragment format change is needed.

Revoke the digest before granting writable access, not merely when a write callback
arrives: writable mappings may dirty pages first. Keep it absent until all writable
handles/mappings are quiesced and an immutable version is established. Track aliases
at the inode level. Rename alone preserves identity; truncate, write, copy-on-write
and revision replacement invalidate it. `lore-vfs::digest` supplies generation
checks; the adapter still owns synchronization and writer/mapping tracking.

Bazel configuration:

```text
startup --digest_function=SHA256
startup --unix_digest_hash_attribute_name=user.sha256
```

Bazel's [UnixFileSystem fast digest path](https://github.com/bazelbuild/bazel/blob/master/src/main/java/com/google/devtools/build/lib/unix/UnixFileSystem.java)
uses the supplied xattr. The local Bazel 7.5.0 experiment confirms raw bytes work
and stale same-size attributes are trusted. Production acceptance must repeat this
on each real mount and the supported Bazel versions.

## Revision and write semantics

Assign stable inode identities independently of paths; keep removed nodes alive
while open. Read-only revision switches assign new nonroot inodes and retain the
old immutable view for open file reads and directory enumeration. Root-relative
lookups follow the active view because Linux LOOKUP carries no directory handle;
relative lookups through retained nonroot directory inodes stay pinned. Writable
revision adoption needs a separate conflict/writer policy. The FSKit probe tests
native cache and mapping capabilities before selecting its transition contract.
Directory cookies remain tied to their opened view. Handle names as filesystem
bytes where possible; validate the FSKit
round trip for non-UTF-8 names, normalization and case collisions before claiming
cross-platform namespace compatibility.

A production transition needs an operation barrier: drain earlier operations,
quiesce dirty state under the chosen conflict policy, publish the new immutable
view, invalidate data/attributes and affected parent name caches, then acknowledge
completion. Keep old views alive for outstanding callbacks. Do not hold inode or
revision locks while asking the OS to flush/invalidate; it may call back into the
filesystem. Sequence FSKit attribute objects under the same synchronization as
snapshots. On invalidation failure, stop serving the affected mount and report a
recoverable error rather than report a successful switch with stale kernel state.

[FSKit invalidate](https://developer.apple.com/documentation/fskit/fsvolume/kernelcachecoherencyaction/invalidate)
drops dirty data and clears attributes and directory name caches. Use it only for
immutable data. Writable state requires flushing and a transition protocol; prove
`pushInvalidate` and cache upgrades separately. Cache coherency is separate from
FSEvents/kqueue or inotify notifications. Test watcher requirements independently.
Linux uses inode and entry invalidation with kernel capability checks; see
[fuser](https://github.com/cberner/fuser).

Production writes use an external sparse overlay and durable checkpoints. Define
create/exclusive open, rename/replace, unlink of open files, hardlinks, modes,
symlinks, truncate/extension, mmap, flush, fsync and directory fsync. Successful
fsync means durable data and metadata according to the documented crash model.
A revision switch with conflicting local edits must reject or preserve those edits
explicitly; never discard them via kernel invalidation. The prototype's RAM-only
write behavior is not this durability implementation.

### Durability without a per-file synchronization callback

The fixture currently grants `readCache`, which the SDK defines as writes
bypassing the kernel cache. A candidate implementation can publish a durable
checkpoint before acknowledging every write and metadata mutation. This remains
an unqualified alternative; it must not rely on a later `synchronize` call.

- Use private storage and the common checkpoint ownership rules. Complete data,
  metadata and directory synchronization before returning success. An uncertain
  publication must stop further mutation and retain recovery data.
- Test ordinary writes, truncation, namespace mutations and writable mappings.
  Prove that `fsync` and `msync` cannot return before the corresponding write
  callbacks and durable publications complete. Reject unsupported mapping modes.
- Terminate only the owned test module after acknowledged operations, without
  orderly unmount, then restore into a fresh mount and compare bytes and metadata.
  Inject backing-file and directory-sync failures; successful RAM reads are not
  durability evidence.
- Measure small-write and build workloads before selecting this strategy. A full
  checkpoint per mutation may be too expensive, and batching cannot acknowledge
  persistence before its publication finishes.

This candidate does not address the independent directory/name-cache transition
gate. Keep live revision switching disabled until that gate passes.

## Ordered work packages and acceptance gates

| Stage | Work | Required evidence before advancing |
| --- | --- | --- |
| 0: Native capability probe | Build/sign bundle; load extension on macOS 27; run mounted fixture suite; diagnose entitlement, activation and generic URL mounting | Every fixture assertion passes with OS/Xcode versions recorded; no hangs or silent cache staleness |
| 1: Lifecycle and semantics probe | Repeated mount/unmount, busy descriptors, module termination, reconnect; concurrent readers during switches; permissions and name round trips; watcher checks | Explicit supported semantics; stable process/descriptor/Mach-resource counts over repeated cycles; permission path for real repository access established |
| 2: Shared core | External provider interface; stable inode/handle table, revision snapshots, range reads, generation-scoped directory iteration, cancellation and teardown | Fake-adapter tests cover replacement/deletion with open handles, stale callbacks/cookies, concurrent teardown, error propagation and bounded memory |
| 3: Trusted digests | Integrate streaming SHA-256 with ingestion/commit; versioned immutable digest index; background backfill; inode-level writer tracking | Empty, sparse and fragmented files; failed reads; same-size edits; dirty mmap; stale jobs; context changes; cache recovery; no I/O on xattr hit |
| 4: Read-only macOS mount | Bind Swift to Rust C ABI; mount real revisions; external cache/metadata; service activation and health | Real repository clone/read/switch, offline cached reads, network errors, auth isolation, symlinks and file modes; concurrent transition assertions |
| 5: Linux adapter | FUSE session lifecycle, read-only operations, raw digest xattr, entry/inode invalidation and cancellation | Same contract suite on supported kernels, including unprivileged mounting and daemon death; Bazel fast digest verified |
| 6: Durable writes | Overlay/checkpoints, namespace mutations, write/mmap coherence, commit handoff, transition conflicts | Crash/restart at publication boundaries, fsync guarantees, no lost edits, hardlink aliases, disk-full/short-write errors and commit/switch races on both OSes |
| 7: Service/CLI integration | Platform backend selection, mount ownership, stale mount recovery, diagnostics, packaging/signing, graceful shutdown | Existing Windows tests pass; platform defaults unchanged until enabled; clean install, upgrade, rollback and multiple simultaneous mounts tested |
| 8: Release qualification | macOS 27 and Linux CI runners, Bazel workload, performance/cancellation stress and documentation | Build-output equivalence with ordinary checkout; no stale digests; bounded memory/resources; acceptable cold/warm latency and throughput |

Stages 2–3 can proceed while runtime validation is pending. Stage 4 depends on
stages 0–3. Stage 5 shares stages 2–3 and may proceed independently of macOS rollout.
Stage 6 requires a stable adapter contract. No default-backend change precedes
stages 7–8. Keep the prototype available as a small OS regression reproducer.

For Bazel acceptance, run the same build on a normal checkout and each mount;
compare outputs and execution-log input SHA-256. Warm the caches, switch a same-size
input, repeat the build, then make a local same-size edit and writable-mmap edit.
Assert the action reruns with the new digest. Instrument file-read and digest
requests to distinguish actual fast hashing from ordinary file reads. Include
empty/large/fragmented files, renames and an absent digest. A stale digest is a
release blocker, regardless of build speed.

## Decisions still gated on evidence

- A development team supporting the FSKit Module capability is required. Xcode
  rejects Personal Team provisioning for this extension on macOS 27.0.1. Bundle
  signing, registration and enablement alone do not satisfy launch authorization.
- The provisioned generic-URL fixture mounts on macOS 27.0.1. The selected real
  repository/cache/credential access model still requires native validation.
- Whether live read mappings and negative name caches obey the proposed switch
  semantics on macOS 27; revise that contract if the native API cannot support it.
  Directory invalidation returns `kIOReturnBadArgument` with both
  `none`/`noCache` and `readWithCache`/`readCache` configurations on macOS 27.0.1.
- Whether URL-backed FSKit delivers a usable durability boundary. The native
  fixture's `fsync` returns without calling `synchronize` on macOS 27.0.1;
  successful data writes alone do not qualify a durable working tree.
- How watcher consumers learn about out-of-band revision switches. Invalidation
  alone is not proof of watcher delivery.
- Minimum Linux kernel/FUSE protocol and supported Bazel versions, selected from
  actual deployment requirements before release qualification.
- Durable overlay placement/migration and conflict policy, specified before stage 6.

Native FSKit is the preferred macOS path. macFUSE/FUSE-T remain fallback candidates
only if a required capability fails the native runtime gates; selecting a fallback
requires repeating the same correctness suite. A kernel extension is not required
by this design. Installation, signing and extension approval are part of the product
work rather than assumptions hidden in the adapter.
