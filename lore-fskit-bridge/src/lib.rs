// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Versioned read-only C boundary for the shared filesystem core.
//! All exported calls contain unwinding panics. Build with the `fskit` profile;
//! process-aborting allocation failures are outside Rust unwind recovery.

#![cfg(any(test, feature = "native-bridge"))]

#[cfg(not(target_pointer_width = "64"))]
compile_error!("lore-fskit-bridge ABI1 requires a 64-bit target");

#[cfg(panic = "abort")]
compile_error!("lore-fskit-bridge requires panic=unwind; use --profile fskit");

use std::cell::Cell;
use std::collections::BTreeMap;
use std::ffi::c_void;
use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use lore_vfs::core::Core;
use lore_vfs::core::Entry;
use lore_vfs::core::Error;
use lore_vfs::core::MAX_READ_SIZE;
use lore_vfs::core::NodeKind;

const OK: i32 = 0;
const PENDING: i32 = 1;
const INVALID: i32 = 6;
const BAD_HANDLE: i32 = 7;
const RANGE: i32 = 12;
const IO: i32 = 13;
const CLOSED: i32 = 14;
const LIMIT: i32 = 15;
const CANCELLED: i32 = 16;
const PANIC: i32 = 17;
const BUSY: i32 = 18;
const ABI_MISMATCH: i32 = 19;
const ABI: u32 = 1;
const MAX_SESSIONS: usize = 32;
const MAX_OPERATIONS: usize = 256;
const MAX_ACTIVE: usize = 64;
const MAX_PAGE: usize = 256;

#[repr(C)]
#[derive(Clone, Copy, Default, Debug, PartialEq, Eq)]
pub struct LvfsEntry {
    pub inode: u64,
    pub parent: u64,
    pub size: u64,
    pub mode: u32,
    pub kind: u32,
}
#[repr(C)]
#[derive(Clone, Copy)]
pub struct LvfsRequest {
    pub opcode: u32,
    pub reserved: u32,
    pub inode: u64,
    pub handle: u64,
    pub offset: u64,
    pub count: u32,
    pub name_length: u32,
    pub name: *const u8,
}
#[repr(C)]
#[derive(Clone, Copy, Default, Debug)]
pub struct LvfsResult {
    pub status: i32,
    pub payload_length: u32,
    pub value: u64,
    pub entry: LvfsEntry,
    pub eof: u32,
    pub reserved: u32,
}
type Completion = extern "C" fn(*mut c_void, u64);
struct Request {
    opcode: u32,
    inode: u64,
    handle: u64,
    offset: u64,
    count: usize,
    name: Vec<u8>,
}
struct HandleCleanup {
    core: Arc<Core>,
    handle: u64,
    claimed: bool,
}
impl Drop for HandleCleanup {
    fn drop(&mut self) {
        if !self.claimed {
            let _ = self.core.release(self.handle);
        }
    }
}
struct Outcome {
    result: LvfsResult,
    payload: Vec<u8>,
    cleanup: Option<HandleCleanup>,
}
impl Outcome {
    fn status(status: i32) -> Self {
        Self {
            result: LvfsResult {
                status,
                ..Default::default()
            },
            payload: Vec::new(),
            cleanup: None,
        }
    }
    fn bytes(payload: Vec<u8>) -> Self {
        Self {
            result: LvfsResult {
                payload_length: payload.len() as u32,
                ..Default::default()
            },
            payload,
            cleanup: None,
        }
    }
}
struct Session {
    id: u64,
    core: Arc<Core>,
    runtime: tokio::runtime::Handle,
    state: Mutex<SessionState>,
    drained: Condvar,
}
struct SessionState {
    closing: bool,
    active: BTreeMap<u64, Arc<Operation>>,
}
struct Operation {
    id: u64,
    session: Arc<Session>,
    cancelled: AtomicBool,
    state: Mutex<OperationState>,
    completion: Completion,
    context: usize,
}
struct OperationState {
    outcome: Option<Outcome>,
    abort: Option<tokio::task::AbortHandle>,
}
#[derive(Default)]
struct Registry {
    sessions: BTreeMap<u64, Arc<Session>>,
    operations: BTreeMap<u64, Arc<Operation>>,
    bootstraps: usize,
}
static REGISTRY: OnceLock<Mutex<Registry>> = OnceLock::new();
static NEXT: AtomicU64 = AtomicU64::new(1);
struct BootstrapGuard {
    active: bool,
}
impl BootstrapGuard {
    fn reserve() -> Result<Self, i32> {
        let mut registry = registry().lock().map_err(|_poison| IO)?;
        if registry.sessions.len() + registry.bootstraps >= MAX_SESSIONS {
            return Err(LIMIT);
        }
        if registry.bootstraps >= 4 {
            return Err(BUSY);
        }
        registry.bootstraps += 1;
        Ok(Self { active: true })
    }
    fn retain(mut self, core: Arc<Core>) -> Result<u64, i32> {
        let mut registry = registry().lock().map_err(|_poison| IO)?;
        let id = register(&mut registry, core)?;
        registry.bootstraps -= 1;
        self.active = false;
        Ok(id)
    }
}
impl Drop for BootstrapGuard {
    fn drop(&mut self) {
        if self.active
            && let Ok(mut registry) = registry().lock()
        {
            registry.bootstraps -= 1;
        }
    }
}
#[derive(Default)]
struct BootstrapCancel {
    cancelled: AtomicBool,
    notify: tokio::sync::Notify,
}
fn bootstrap_session(
    path: Vec<u8>,
    deadline: std::time::Duration,
    bootstrap: impl FnOnce(&[u8], Arc<BootstrapCancel>) -> Result<Arc<Core>, i32> + Send + 'static,
) -> Result<u64, i32> {
    let permit = BootstrapGuard::reserve()?;
    let cancel = Arc::new(BootstrapCancel::default());
    let worker_cancel = cancel.clone();
    let (sender, receiver) = std::sync::mpsc::sync_channel(1);
    std::thread::Builder::new()
        .name("lore-fskit-bootstrap".into())
        .spawn(move || {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let core = bootstrap(&path, worker_cancel.clone())?;
                if worker_cancel.cancelled.load(Ordering::Acquire) {
                    return Err(CANCELLED);
                }
                permit.retain(core)
            }))
            .unwrap_or(Err(PANIC));
            if let Err(undelivered) = sender.send(result)
                && let Ok(id) = undelivered.0
            {
                let _ = lore_fskit_session_close(id);
            }
        })
        .map_err(|_spawn| IO)?;
    match receiver.recv_timeout(deadline) {
        Ok(result) => result,
        Err(_timeout) => {
            cancel.cancelled.store(true, Ordering::Release);
            cancel.notify.notify_one();
            Err(IO)
        }
    }
}

thread_local! { static CALLBACK_SESSION:Cell<u64>=const { Cell::new(0) }; }
#[cfg(test)]
thread_local! { static DISPATCH_PANIC:Cell<bool>=const { Cell::new(false) }; }
fn registry() -> &'static Mutex<Registry> {
    REGISTRY.get_or_init(Mutex::default)
}
fn next() -> Result<u64, i32> {
    NEXT.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
        value.checked_add(1)
    })
    .map_err(|_overflow| LIMIT)
}
fn session(id: u64) -> Result<Arc<Session>, i32> {
    registry()
        .lock()
        .map_err(|_poison| IO)?
        .sessions
        .get(&id)
        .cloned()
        .ok_or(BAD_HANDLE)
}
fn operation(id: u64) -> Result<Arc<Operation>, i32> {
    registry()
        .lock()
        .map_err(|_poison| IO)?
        .operations
        .get(&id)
        .cloned()
        .ok_or(BAD_HANDLE)
}
fn ffi(call: impl FnOnce() -> Result<(), i32>) -> i32 {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(call)) {
        Ok(Ok(())) => OK,
        Ok(Err(status)) => status,
        Err(_panic) => PANIC,
    }
}
fn core_status(error: Error) -> i32 {
    match error {
        Error::NotFound => 2,
        Error::PermissionDenied => 3,
        Error::NotDirectory => 4,
        Error::IsDirectory => 5,
        Error::InvalidInput => INVALID,
        Error::BadHandle => BAD_HANDLE,
        Error::Stale => 8,
        Error::ReadOnly => 9,
        Error::Unsupported => 10,
        Error::NoAttribute => 11,
        Error::Range => RANGE,
        Error::Io => IO,
        Error::Unmounted => CLOSED,
        Error::ResourceLimit => LIMIT,
        Error::Busy => BUSY,
    }
}
fn entry(value: Entry) -> LvfsEntry {
    LvfsEntry {
        inode: value.inode,
        parent: value.parent,
        size: value.node.size,
        mode: u32::from(value.node.mode) & !0o222,
        kind: match value.node.kind {
            NodeKind::File => 1,
            NodeKind::Directory => 2,
            NodeKind::Symlink => 3,
        },
    }
}
#[cfg(any(test, feature = "test-fixture"))]
fn retain(core: Arc<Core>) -> Result<u64, i32> {
    let mut registry = registry().lock().map_err(|_poison| IO)?;
    if registry.sessions.len() + registry.bootstraps >= MAX_SESSIONS {
        return Err(LIMIT);
    }
    register(&mut registry, core)
}
fn register(registry: &mut Registry, core: Arc<Core>) -> Result<u64, i32> {
    let id = next()?;
    registry.sessions.insert(
        id,
        Arc::new(Session {
            id,
            core,
            runtime: lore::runtime(),
            state: Mutex::new(SessionState {
                closing: false,
                active: BTreeMap::new(),
            }),
            drained: Condvar::new(),
        }),
    );
    Ok(id)
}
impl Operation {
    fn finish(&self, mut outcome: Outcome) {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        if state.outcome.is_some() {
            return;
        }
        if self.cancelled.load(Ordering::Acquire) {
            outcome = Outcome::status(CANCELLED);
        }
        state.outcome = Some(outcome);
        drop(state);
        CALLBACK_SESSION.with(|current| {
            let previous = current.replace(self.session.id);
            (self.completion)(self.context as *mut c_void, self.id);
            current.set(previous);
        });
        if let Ok(mut state) = self.session.state.lock() {
            state.active.remove(&self.id);
            self.session.drained.notify_all();
        }
    }
    fn cancel(&self) -> Result<(), i32> {
        let state = self.state.lock().map_err(|_poison| IO)?;
        if state.outcome.is_some() {
            return Ok(());
        }
        self.cancelled.store(true, Ordering::Release);
        if let Some(abort) = &state.abort {
            abort.abort();
        }
        Ok(())
    }
}
struct CompletionGuard(Option<Arc<Operation>>);
impl CompletionGuard {
    fn finish(mut self, outcome: Outcome) {
        if let Some(op) = self.0.take() {
            op.finish(outcome);
        }
    }
}
impl Drop for CompletionGuard {
    fn drop(&mut self) {
        if let Some(op) = self.0.take() {
            op.finish(Outcome::status(PANIC));
        }
    }
}

// Drop the backend future before publishing cancellation/completion. This makes
// session drain include future-owned resources, not merely its C callback.
struct OperationFuture {
    inner: Option<std::pin::Pin<Box<dyn std::future::Future<Output = Outcome> + Send>>>,
    completion: Option<CompletionGuard>,
}
impl std::future::Future for OperationFuture {
    type Output = ();
    fn poll(
        self: std::pin::Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<()> {
        let this = self.get_mut();
        let result = match this.inner.as_mut() {
            Some(future) => future.as_mut().poll(context),
            None => return std::task::Poll::Ready(()),
        };
        match result {
            std::task::Poll::Pending => std::task::Poll::Pending,
            std::task::Poll::Ready(outcome) => {
                this.inner.take();
                if let Some(completion) = this.completion.take() {
                    completion.finish(outcome);
                }
                std::task::Poll::Ready(())
            }
        }
    }
}
impl Drop for OperationFuture {
    fn drop(&mut self) {
        self.inner.take();
        self.completion.take();
    }
}

async fn execute(core: Arc<Core>, request: Request) -> Result<Outcome, Error> {
    let mut result = Outcome::status(OK);
    match request.opcode {
        1 => {
            core.revalidate(request.inode).await?;
            result.result.entry = entry(core.getattr(request.inode)?);
        }
        2 => result.result.entry = entry(core.lookup(request.inode, &request.name).await?),
        3 | 4 => {
            core.revalidate(request.inode).await?;
            let handle = if request.opcode == 3 {
                core.open(request.inode)?
            } else {
                core.opendir(request.inode)?
            };
            result.result.value = handle;
            result.cleanup = Some(HandleCleanup {
                core,
                handle,
                claimed: false,
            });
        }
        5 => {
            result = Outcome::bytes(
                core.read(request.handle, request.offset, request.count)
                    .await?,
            );
        }
        6 => {
            let page = core
                .readdir(request.handle, request.offset, request.count)
                .await?;
            let mut bytes = Vec::new();
            for named in page.entries {
                let e = entry(named.entry);
                for number in [e.inode, e.parent, e.size] {
                    bytes.extend_from_slice(&number.to_le_bytes());
                }
                bytes.extend_from_slice(&e.mode.to_le_bytes());
                bytes.extend_from_slice(&e.kind.to_le_bytes());
                bytes.extend_from_slice(&named.cookie.to_le_bytes());
                bytes.extend_from_slice(&(named.name.len() as u32).to_le_bytes());
                bytes.extend_from_slice(&named.name);
            }
            result = Outcome::bytes(bytes);
            result.result.eof = u32::from(page.eof);
        }
        7 => core.release(request.handle)?,
        8 => {
            result = Outcome::bytes(
                core.digest(request.inode)
                    .await?
                    .ok_or(Error::NoAttribute)?
                    .to_vec(),
            );
        }
        9 => result = Outcome::bytes(core.readlink(request.inode).await?),
        10 => {
            let found = core.lookup(request.inode, &request.name).await?;
            let handle = core.opendir(request.inode)?;
            let _cleanup = HandleCleanup {
                core: core.clone(),
                handle,
                claimed: false,
            };
            let mut cookie = 0;
            let mut canonical = None;
            for _ in 0..64 {
                let page = core.readdir(handle, cookie, MAX_PAGE).await?;
                for named in page.entries {
                    cookie = named.cookie;
                    if named.entry.inode == found.inode {
                        canonical = Some(named.name);
                        break;
                    }
                }
                if canonical.is_some() {
                    break;
                }
                if page.eof {
                    return Err(Error::Io);
                }
            }
            result = Outcome::bytes(canonical.ok_or(Error::ResourceLimit)?);
            result.result.entry = entry(found);
        }
        11 => core.revalidate(request.inode).await?,
        _ => return Err(Error::InvalidInput),
    }
    Ok(result)
}

#[unsafe(no_mangle)]
pub extern "C" fn lore_fskit_abi_version() -> u32 {
    ABI
}

/// # Safety
/// `session_out` must be a writable u64. The input buffer must remain valid for this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lore_fskit_session_open(
    abi: u32,
    path: *const u8,
    length: u32,
    session_out: *mut u64,
) -> i32 {
    ffi(|| {
        if session_out.is_null() {
            return Err(INVALID);
        }
        // SAFETY: the caller supplies a valid writable output pointer.
        unsafe {
            session_out.write(0);
        }
        if abi != ABI {
            return Err(ABI_MISMATCH);
        }
        if path.is_null() || length == 0 || length > 4096 {
            return Err(INVALID);
        }
        if tokio::runtime::Handle::try_current().is_ok() {
            return Err(BUSY);
        }
        // SAFETY: the non-null input buffer has the documented caller-owned length.
        let path = unsafe { std::slice::from_raw_parts(path, length as usize) };
        let id = bootstrap_session(
            path.to_vec(),
            std::time::Duration::from_secs(30),
            bootstrap::open,
        )?;
        // SAFETY: validated caller-owned output pointer.
        unsafe {
            session_out.write(id);
        }
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn lore_fskit_session_close(id: u64) -> i32 {
    ffi(|| {
        if CALLBACK_SESSION.with(|current| current.get() == id)
            || tokio::runtime::Handle::try_current().is_ok()
        {
            return Err(BUSY);
        }
        let session = session(id)?;
        let active = {
            let mut state = session.state.lock().map_err(|_poison| IO)?;
            if state.closing {
                return Err(BAD_HANDLE);
            }
            state.closing = true;
            state.active.values().cloned().collect::<Vec<_>>()
        };
        for op in active {
            op.cancel()?;
        }
        let mut state = session.state.lock().map_err(|_poison| IO)?;
        while !state.active.is_empty() {
            state = session.drained.wait(state).map_err(|_poison| IO)?;
        }
        drop(state);
        {
            let mut registry = registry().lock().map_err(|_poison| IO)?;
            registry.operations.retain(|_, op| op.session.id != id);
            registry.sessions.remove(&id);
        }
        session.core.shutdown().map_err(core_status)
    })
}

/// # Safety
/// Request and output pointers must be valid. The completion/context must remain
/// callable until its exactly-once completion; it may reenter result/release APIs.
#[unsafe(no_mangle)]
#[allow(clippy::disallowed_methods)] // Explicit retained runtime; RevisionBackend restores its captured Lore context.
pub unsafe extern "C" fn lore_fskit_submit(
    id: u64,
    request: *const LvfsRequest,
    completion: Option<Completion>,
    context: *mut c_void,
    operation_out: *mut u64,
) -> i32 {
    let mut accepted = false;
    let status = ffi(|| {
        if request.is_null() || operation_out.is_null() {
            return Err(INVALID);
        }
        // SAFETY: caller-owned, properly aligned request/output pointers.
        let request = unsafe {
            operation_out.write(0);
            *request
        };
        let completion = completion.ok_or(INVALID)?;
        if request.reserved != 0
            || !(1..=11).contains(&request.opcode)
            || request.name_length > 1000
            || (request.name_length != 0 && request.name.is_null())
            || (request.opcode == 5 && request.count as usize > MAX_READ_SIZE)
            || (request.opcode == 6 && (request.count == 0 || request.count as usize > MAX_PAGE))
        {
            return Err(INVALID);
        }
        let name = if request.name_length == 0 {
            Vec::new()
        } else {
            // SAFETY: the supplied name buffer is valid and bounded before allocation.
            unsafe { std::slice::from_raw_parts(request.name, request.name_length as usize) }
                .to_vec()
        };
        let request = Request {
            opcode: request.opcode,
            inode: request.inode,
            handle: request.handle,
            offset: request.offset,
            count: request.count as usize,
            name,
        };
        let session = session(id)?;
        let mut registry = registry().lock().map_err(|_poison| IO)?;
        if registry.operations.len() >= MAX_OPERATIONS {
            return Err(LIMIT);
        }
        let mut state = session.state.lock().map_err(|_poison| IO)?;
        if state.closing {
            return Err(CLOSED);
        }
        if state.active.len() >= MAX_ACTIVE {
            return Err(LIMIT);
        }
        let operation_id = next()?;
        let op = Arc::new(Operation {
            id: operation_id,
            session: session.clone(),
            cancelled: AtomicBool::new(false),
            state: Mutex::new(OperationState {
                outcome: None,
                abort: None,
            }),
            completion,
            context: context as usize,
        });
        registry.operations.insert(operation_id, op.clone());
        state.active.insert(operation_id, op.clone());
        drop(state);
        drop(registry);
        let guard = CompletionGuard(Some(op.clone()));
        // SAFETY: the validated output is populated before callbacks can execute.
        unsafe {
            operation_out.write(operation_id);
        }
        accepted = true;
        #[cfg(test)]
        if DISPATCH_PANIC.with(|flag| flag.replace(false)) {
            panic!("injected dispatch panic after admission");
        }
        let core = session.core.clone();
        let task = session.runtime.spawn(OperationFuture {
            inner: Some(Box::pin(async move {
                execute(core, request)
                    .await
                    .unwrap_or_else(|error| Outcome::status(core_status(error)))
            })),
            completion: Some(guard),
        });
        let mut state = op.state.lock().map_err(|_poison| IO)?;
        if op.cancelled.load(Ordering::Acquire) {
            task.abort();
        }
        state.abort = Some(task.abort_handle());
        Ok(())
    });
    // After admission a callback owns the context even if dispatch unwinds.
    // Its completion guard publishes the error; never report a rejected submit.
    if accepted { OK } else { status }
}

#[unsafe(no_mangle)]
pub extern "C" fn lore_fskit_operation_cancel(id: u64) -> i32 {
    ffi(|| operation(id)?.cancel())
}

/// # Safety
/// `result` and any non-null output buffer must be valid writable storage.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lore_fskit_operation_result(
    id: u64,
    result: *mut LvfsResult,
    payload: *mut u8,
    capacity: u32,
) -> i32 {
    ffi(|| {
        if result.is_null() || (capacity != 0 && payload.is_null()) {
            return Err(INVALID);
        }
        let op = operation(id)?;
        let state = op.state.lock().map_err(|_poison| IO)?;
        let outcome = state.outcome.as_ref().ok_or(PENDING)?;
        // SAFETY: validated caller-owned output storage.
        unsafe {
            result.write(outcome.result);
        }
        if (capacity as usize) < outcome.payload.len() {
            return Err(RANGE);
        }
        if !outcome.payload.is_empty() {
            // SAFETY: capacity was checked and buffers cannot overlap Rust-owned storage.
            unsafe {
                std::ptr::copy_nonoverlapping(
                    outcome.payload.as_ptr(),
                    payload,
                    outcome.payload.len(),
                );
            }
        }
        Ok(())
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn lore_fskit_operation_claim(id: u64) -> i32 {
    ffi(|| {
        let op = operation(id)?;
        let mut state = op.state.lock().map_err(|_poison| IO)?;
        let outcome = state.outcome.as_mut().ok_or(PENDING)?;
        if let Some(cleanup) = &mut outcome.cleanup {
            if cleanup.claimed {
                return Err(BAD_HANDLE);
            }
            cleanup.claimed = true;
        }
        Ok(())
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn lore_fskit_operation_release(id: u64) -> i32 {
    ffi(|| {
        let mut registry = registry().lock().map_err(|_poison| IO)?;
        let op = registry.operations.get(&id).ok_or(BAD_HANDLE)?;
        if op.state.lock().map_err(|_poison| IO)?.outcome.is_none() {
            return Err(BUSY);
        }
        registry.operations.remove(&id);
        Ok(())
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn lore_fskit_handle_release(id: u64, handle: u64) -> i32 {
    ffi(|| session(id)?.core.release(handle).map_err(core_status))
}

mod bootstrap;
#[cfg(any(test, feature = "test-fixture"))]
mod fixture;
#[cfg(test)]
mod tests;
