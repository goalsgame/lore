# Shared read-only FSKit bridge

This experimental Rust C ABI retains the same `lore-vfs::Core` and pinned
`RevisionBackend` used by the Linux adapter. Swift supplies OS callbacks and
caller-owned buffers; it does not load repository trees, credentials or content.

The ABI is deliberately opt-in. Ordinary workspace builds have no native ABI
symbols. Enable `native-bridge` and use an unwind profile:

```sh
RUSTC_WRAPPER= CARGO_INCREMENTAL=0 cargo test --locked --offline \
  -p lore-fskit-bridge --features native-bridge
RUSTC_WRAPPER= CARGO_INCREMENTAL=0 cargo build --locked --offline \
  -p lore-fskit-bridge --features native-bridge --profile fskit
```

An enabled ABI rejects `panic=abort` and targets other than 64-bit. Normal workspace
release builds remain supported with the bridge feature disabled. The
`test-fixture` feature includes a shared-Core fixture for standalone Swift tests;
the extension build does not enable it.

## Ownership and bounds

[The header](include/lore_fskit_bridge.h) defines ABI version 1 and stable statuses.
Negotiate the version before opening a session. Session and operation IDs are
monotonic and never reused; file handles are scoped to their session. Every
accepted submission produces exactly one completion, including cancellation,
backend panic and dispatch failure. Rejected submissions produce no callback.
Callbacks may run before submission returns and must return promptly without
throwing or unwinding through C.

Completed results copy into caller-owned buffers. A short buffer returns the
required length for retry. Release each operation explicitly; no borrowed Rust
allocation escapes. Successful file/directory opens transfer ownership only after
claiming their result. Cancellation or release of an unclaimed open result closes
its handle automatically. Cancellation does not undo a handle release that already
completed. A completion that wins the cancellation race may still report success.

Close cancels bridge futures, drains their completion callbacks, invalidates all
remaining operation/file handles and shuts down the core. Call it off Tokio workers
and completion callbacks; reentrant callback close returns `Busy`. Swift dispatches
close on a background queue and concurrent close callers await the same drain.
The process's normal Lore runtime must remain alive until sessions are closed.
Underlying shared I/O retains its ordinary Lore cancellation/lifetime behavior.

Admission is bounded to 32 live/closing/setup sessions, at most four concurrent
bootstraps, 256 retained operations and 64 active operations per session. Reads are
at most 1 MiB per ABI call and directory pages at most 256 entries. The FSKit adapter
chunks larger kernel read buffers. Canonical-name lookup currently scans at most
16,384 parent entries and returns `ResourceLimit` beyond that bound. The common
core's inode and handle budgets also apply.

## Bootstrap and sandbox access

The real resource takes the path of an existing, regular, user-owned bootstrap
file with no group/other access, at most 8192 bytes. Its JSON schema is:

```json
{
  "repository": "/absolute/source/checkout",
  "revision": "main@LATEST",
  "identity": "optional-existing-identity",
  "digest_cache": "/absolute/private/digest-cache",
  "offline": true
}
```

`identity`, `digest_cache` and `offline` are optional. Unknown fields are rejected;
configuration parse failures do not render their values. Tokens do not belong in
this file or the resource URL. Setup uses the normal Lore repository/authentication
pipeline and credential stores. It captures the authorized backend context and
releases command locks before returning the pinned core.

Bootstrap runs on a bounded worker with a 30-second caller deadline. Cancellation
returns promptly to Swift; late success is closed instead of becoming a volume.
A blocked OS filesystem call may retain one of the four setup slots until it
returns. Timeout cancels the asynchronous repository setup and retains no public
session handle.

The extension must actually have OS-approved access to its configured repository,
normal credential/config stores and content stores. Container-local source data is
one layout to qualify; access to a normal checkout or host keychain cannot be
inferred from a linked static library. No credential copying, privileged IPC,
security-scoped access bypass or signing-account changes are implemented here.
Sandbox denial fails setup explicitly.

See [the native prototype](../contrib/fskit-prototype/README.md) for host preflight,
standalone Swift checks and remaining activation/sandbox/cache gates.
