// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
//! Owned Unix service runtime and exact-run lifecycle requests.
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;

use lore_revision::service_state::ServiceStateImpl;
use tokio::sync::watch;

use super::owner::ServiceOwner;
use super::service_main::ServiceMainError;
use crate::remote::network::UdsListener;
use crate::remote::network::UdsStream;
use crate::remote::service_process::ServiceStartGuard;
use crate::remote::service_process::begin_service_start_unchecked;
use crate::remote::service_process::register_service_process;
use crate::working_mount::worker::ThreadOwner;

const MAX_CONNECTIONS: usize = 128;
static CURRENT: Mutex<Option<Arc<RunState>>> = Mutex::new(None);
#[cfg(feature = "test-util")]
static TEST_FAULT: std::sync::atomic::AtomicU8 = std::sync::atomic::AtomicU8::new(0);
#[cfg(feature = "test-util")]
pub fn set_test_fault(point: u8) {
    TEST_FAULT.store(point, Ordering::Release);
}
#[cfg(feature = "test-util")]
fn test_fault(point: u8) {
    if TEST_FAULT
        .compare_exchange(point, 0, Ordering::AcqRel, Ordering::Acquire)
        .is_ok()
    {
        panic!("injected service lifecycle fault {point}");
    }
}
#[cfg(feature = "test-util")]
pub(crate) fn take_precondition_failure() -> bool {
    TEST_FAULT
        .compare_exchange(4, 0, Ordering::AcqRel, Ordering::Acquire)
        .is_ok()
}

#[derive(Clone)]
#[lore_macro::test_pub]
pub(crate) struct RunHandle(Arc<RunState>);
struct Command {
    next: u64,
    pending: Option<u64>,
}
pub(crate) struct RunState {
    owner: Arc<ServiceOwner>,
    start: Mutex<Option<ServiceStartGuard>>,
    runtime: OnceLock<tokio::runtime::Handle>,
    command: Mutex<Command>,
    requests: watch::Sender<u64>,
    attempts: watch::Sender<(u64, Option<Result<(), String>>)>,
    done: watch::Sender<Option<Result<(), String>>>,
    listener: Mutex<Option<Arc<UdsListener>>>,
    sockets: Mutex<Vec<(u64, UdsStream)>>,
    next_socket: Mutex<u64>,
    accepting: AtomicBool,
    accept_thread: Mutex<Option<Arc<ThreadOwner>>>,
    connections_changed: tokio::sync::Notify,
    actor_thread: ThreadOwner,
    published: AtomicBool,
    joined: AtomicBool,
    fatal: Mutex<Option<String>>,
    initialized: AtomicBool,
}
#[lore_macro::test_pub]
pub(crate) fn current_run() -> Option<RunHandle> {
    CURRENT
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .as_ref()
        .cloned()
        .map(RunHandle)
}
impl RunHandle {
    #[lore_macro::test_pub]
    pub(crate) fn has_completed(&self) -> bool {
        self.0.done.borrow().is_some()
    }
    #[cfg(feature = "test-util")]
    pub fn inject_retained_failure(&self) {
        self.0
            .record_failure("injected terminal actor uncertainty".into());
        self.0.owner.fence_lifecycle();
    }
    #[cfg(feature = "test-util")]
    pub fn exhaust_request_ids(&self) {
        self.0
            .command
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .next = u64::MAX;
        self.0.attempts.send_replace((u64::MAX, Some(Ok(()))));
    }
    fn request(&self) -> u64 {
        self.0.owner.fence_lifecycle();
        let mut command = self.0.command.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(pending) = command.pending {
            return pending;
        }
        let Some(next) = command.next.checked_add(1) else {
            let error = "service shutdown request identity exhausted; run retained".to_owned();
            self.0.record_failure(error.clone());
            self.0.attempts.send_replace((u64::MAX, Some(Err(error))));
            return u64::MAX;
        };
        command.next = next;
        let id = command.next;
        command.pending = Some(id);
        self.0.requests.send_replace(id);
        id
    }
    #[lore_macro::test_pub]
    pub(crate) fn request_shutdown(&self) {
        self.request();
    }
    #[lore_macro::test_pub]
    pub(crate) async fn request_and_wait_shutdown(&self) -> Result<(), String> {
        if let Some(error) = self
            .0
            .fatal
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
        {
            return Err(error);
        }
        if self.0.done.borrow().is_some() {
            return self.wait().await;
        }
        let id = self.request();
        let mut attempts = self.0.attempts.subscribe();
        loop {
            let (seen, result) = attempts.borrow_and_update().clone();
            if seen >= id
                && let Some(result) = result
            {
                result?;
                return self.wait().await;
            }
            let completed = self.0.done.borrow().is_some();
            if completed {
                return self.wait().await;
            }
            attempts
                .changed()
                .await
                .map_err(|_error| "service actor stopped without completion".to_owned())?;
        }
    }
    #[lore_macro::test_pub]
    pub(crate) async fn wait(&self) -> Result<(), String> {
        let mut done = self.0.done.subscribe();
        loop {
            let completed = done.borrow_and_update().clone();
            if let Some(result) = completed {
                self.join().await?;
                return result;
            }
            done.changed()
                .await
                .map_err(|_error| "service actor stopped without completion".to_owned())?;
        }
    }
    fn try_join(&self) -> Option<Result<(), String>> {
        if !self.0.published.load(Ordering::Acquire) || !self.has_completed() {
            return None;
        }
        if self.0.joined.load(Ordering::Acquire) {
            return Some(Ok(()));
        }
        match self.0.actor_thread.try_drain()? {
            Ok(()) => {}
            Err(error) => return Some(Err(format!("joining service actor: {error}"))),
        }
        self.0
            .start
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .take();
        let mut current = CURRENT.lock().unwrap_or_else(|p| p.into_inner());
        if current
            .as_ref()
            .is_some_and(|run| Arc::ptr_eq(run, &self.0))
        {
            current.take();
        }
        self.0.joined.store(true, Ordering::Release);
        Some(Ok(()))
    }
    async fn join(&self) -> Result<(), String> {
        loop {
            if let Some(result) = self.try_join() {
                return result;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }
}
struct CancelRun {
    handle: RunHandle,
    complete: bool,
}
impl Drop for CancelRun {
    fn drop(&mut self) {
        if !self.complete {
            self.handle.request_shutdown();
        }
    }
}
impl RunState {
    pub(crate) fn with_failure<T>(&self, publish: impl FnOnce(Option<&String>) -> T) -> T {
        let failure = self.fatal.lock().unwrap_or_else(|p| p.into_inner());
        publish(failure.as_ref())
    }
    fn record_failure(&self, error: String) {
        let mut failure = self.fatal.lock().unwrap_or_else(|p| p.into_inner());
        *failure = Some(error.clone());
        self.owner.retain_error(error);
    }
    pub(crate) fn failure(&self) -> Option<String> {
        self.fatal.lock().unwrap_or_else(|p| p.into_inner()).clone()
    }
    pub(crate) fn runtime(&self) -> Option<tokio::runtime::Handle> {
        self.runtime.get().cloned()
    }
    pub(crate) fn track_socket(
        self: &Arc<Self>,
        socket: &UdsStream,
    ) -> std::io::Result<Option<ConnectionLease>> {
        let mut sockets = self.sockets.lock().unwrap_or_else(|p| p.into_inner());
        if !self.accepting.load(Ordering::Acquire) || sockets.len() >= MAX_CONNECTIONS {
            return Ok(None);
        }
        let mut next = self.next_socket.lock().unwrap_or_else(|p| p.into_inner());
        let Some(id) = next.checked_add(1) else {
            return Ok(None);
        };
        *next = id;
        sockets.push((id, socket.try_clone()?));
        Ok(Some(ConnectionLease {
            run: self.clone(),
            id,
        }))
    }
    fn complete_attempt(&self, id: u64, result: Result<(), String>) {
        let mut command = self.command.lock().unwrap_or_else(|p| p.into_inner());
        self.attempts.send_replace((id, Some(result)));
        if command.pending == Some(id) {
            command.pending = None;
        }
    }
    async fn stop_network(&self) -> Result<(), String> {
        self.accepting.store(false, Ordering::Release);
        // The reply has been enqueued before its control ticket is released.
        tokio::time::sleep(Duration::from_millis(500)).await;
        if self
            .accept_thread
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .is_some()
        {
            let _wake = UdsStream::connect(crate::remote::service_socket_name())
                .map_err(|e| format!("failed waking owned accept worker: {e}"))?;
        }
        {
            let sockets = self.sockets.lock().unwrap_or_else(|p| p.into_inner());
            for (_, socket) in sockets.iter() {
                let _closed = socket.shutdown();
            }
        }
        loop {
            let notified = self.connections_changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self
                .sockets
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .is_empty()
            {
                break;
            }
            notified.await;
        }
        let thread = self
            .accept_thread
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone();
        if let Some(thread) = thread {
            thread
                .drain()
                .await
                .map_err(|error| format!("joining service accept worker: {error}"))?;
        }
        self.listener
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .take();
        Ok(())
    }
}
pub(crate) struct ConnectionLease {
    run: Arc<RunState>,
    id: u64,
}
impl Drop for ConnectionLease {
    fn drop(&mut self) {
        self.run
            .sockets
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .retain(|(id, _)| *id != self.id);
        self.run.connections_changed.notify_waiters();
    }
}

pub(crate) async fn run(
    globals: crate::interface::LoreGlobalArgs,
    state: Arc<ServiceStateImpl>,
) -> Result<(), ServiceMainError> {
    if let Some(previous) = current_run()
        && previous.has_completed()
    {
        // A cancelled waiter may leave a completed actor reserved. Finish its
        // exact native join before trying to publish a replacement service.
        previous.join().await.map_err(ServiceMainError::internal)?;
    }
    let handle =
        super::shutdown::with_start_admission(|| -> Result<RunHandle, ServiceMainError> {
            if let Some(previous) = current_run() {
                let _reaped = previous.try_join();
            }
            let start = begin_service_start_unchecked().ok_or_else(|| {
                ServiceMainError::internal("A Lore service is already running in this process")
            })?;
            let owner = ServiceOwner::for_state(state)
                .map_err(|e| ServiceMainError::internal(e.to_string()))?;
            if !start.bind_owner(&owner) {
                return Err(ServiceMainError::internal("service owner binding failed"));
            }
            let run = Arc::new(RunState {
                owner,
                start: Mutex::new(Some(start)),
                runtime: OnceLock::new(),
                command: Mutex::new(Command {
                    next: 0,
                    pending: None,
                }),
                requests: watch::channel(0).0,
                attempts: watch::channel((0, None)).0,
                done: watch::channel(None).0,
                listener: Mutex::new(None),
                sockets: Mutex::new(Vec::new()),
                next_socket: Mutex::new(0),
                accepting: AtomicBool::new(true),
                accept_thread: Mutex::new(None),
                connections_changed: tokio::sync::Notify::new(),
                actor_thread: ThreadOwner::new(),
                published: AtomicBool::new(false),
                joined: AtomicBool::new(false),
                fatal: Mutex::new(None),
                initialized: AtomicBool::new(false),
            });
            run.owner.attach_actor(&run);
            *CURRENT.lock().unwrap_or_else(|p| p.into_inner()) = Some(run.clone());
            let owned = run.clone();
            let mut publication = Publication {
                run: run.clone(),
                committed: false,
            };
            #[cfg(feature = "test-util")]
            test_fault(1);
            let thread = std::thread::Builder::new()
                .name("lore-service-owner".into())
                .spawn(move || actor_thread(owned, globals))
                .map_err(|e| ServiceMainError::internal(format!("starting service actor: {e}")))?;
            publication.committed = true;
            run.actor_thread.publish(Some(thread));
            run.published.store(true, Ordering::Release);
            #[cfg(feature = "test-util")]
            test_fault(2);
            Ok(RunHandle(run))
        })
        .map_err(|_error| {
            ServiceMainError::internal("library shutdown blocks service startup")
        })??;
    let mut cancel = CancelRun {
        handle,
        complete: false,
    };
    let result = cancel
        .handle
        .wait()
        .await
        .map_err(ServiceMainError::internal);
    cancel.complete = true;
    result
}
#[allow(clippy::disallowed_methods)]
fn actor_thread(run: Arc<RunState>, globals: crate::interface::LoreGlobalArgs) {
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .max_blocking_threads(MAX_CONNECTIONS)
        .enable_all()
        .thread_name("lore-service")
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            finish(&run, Err(format!("creating service runtime: {error}")));
            return;
        }
    };
    let _bound = run.runtime.set(runtime.handle().clone());
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        runtime.block_on(async {
            if let Err(error) = startup(run.clone(), globals).await {
                let cleanup = if run.initialized.load(Ordering::Acquire) {
                    run.owner.stop_lifecycle().await
                } else {
                    run.owner.stop_uninitialized().await
                };
                cleanup.map_err(|e| e.to_string())?;
                run.owner.state().close_and_drain_invocations().await;
                run.stop_network().await?;
                return Ok(Err(error));
            }
            actor_loop(run.clone()).await.map(Ok)
        })
    }));
    let result = match result {
        Ok(Ok(result)) => result,
        Ok(Err(error)) => retain_uncertain(&run, error),
        Err(_) => retain_uncertain(
            &run,
            "service actor panicked; lifecycle resources retained".into(),
        ),
    };
    // Runtime destruction joins queued and running blocking jobs. The exact run
    // guard and listener claims remain owned until it has completed.
    if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(runtime))).is_err() {
        retain_uncertain(
            &run,
            "service runtime teardown panicked; run retained".into(),
        );
    }
    finish(&run, result);
}
fn finish(run: &RunState, result: Result<(), String>) {
    run.owner.close();
    run.listener
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .take();
    run.done.send_replace(Some(result));
    let id = run
        .command
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .pending;
    if let Some(id) = id {
        run.complete_attempt(
            id,
            run.done.borrow().clone().expect("completed actor result"),
        );
    }
}
struct Publication {
    run: Arc<RunState>,
    committed: bool,
}
impl Drop for Publication {
    fn drop(&mut self) {
        if !self.committed {
            let mut current = CURRENT.lock().unwrap_or_else(|p| p.into_inner());
            if current
                .as_ref()
                .is_some_and(|run| Arc::ptr_eq(run, &self.run))
            {
                current.take();
            }
            self.run
                .start
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .take();
        }
    }
}
fn retain_uncertain(run: &RunState, error: String) -> ! {
    run.record_failure(error.clone());
    run.owner.fence_lifecycle();
    let pending = run
        .command
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .pending;
    if let Some(id) = pending {
        run.complete_attempt(id, Err(error));
    }
    loop {
        std::thread::park();
    }
}
async fn startup(
    run: Arc<RunState>,
    globals: crate::interface::LoreGlobalArgs,
) -> Result<(), String> {
    run.owner.state().initialize();
    let Some(claim) =
        UdsListener::claim(crate::remote::service_socket_name()).map_err(|e| e.to_string())?
    else {
        let stop = run
            .owner
            .stop_request()
            .expect("owned startup stop binding");
        let mut requests = run.requests.subscribe();
        let outcome = tokio::select! {
            outcome = crate::remote::service_process::wait_until_a_service_is_listening() => Some(outcome),
            () = stop.requested() => None,
            _ = requests.wait_for(|sequence| *sequence != 0) => None,
        };
        let suffix = match outcome {
            None => ", and stopped waiting for it on a quit",
            Some(Ok(true)) => "",
            Some(Ok(false)) => ", and it did not answer in time",
            Some(Err(error)) => {
                return Err(format!(
                    "waiting for the service holding the socket claim to answer: {error}"
                ));
            }
        };
        return Err(format!(
            "Another Lore service has claimed the socket {}{suffix}",
            crate::remote::service_socket_name()
        ));
    };
    run.initialized.store(true, Ordering::Release);
    super::initialization::initialize_service(globals, run.owner.state().clone())
        .await
        .map_err(|e| e.to_string())?;
    let listener = Arc::new(claim.listen().map_err(|e| e.to_string())?);
    *run.listener.lock().unwrap_or_else(|p| p.into_inner()) = Some(listener.clone());
    println!("Lore service listening");
    super::service_main::report_build_that_is_not_the_configured_one().await;
    {
        let start = run.start.lock().unwrap_or_else(|p| p.into_inner());
        register_service_process(start.as_ref().expect("owned service start guard"));
    }
    let owned = run.clone();
    let worker = std::thread::Builder::new()
        .name("lore-service-accept".into())
        .spawn(move || {
            let mut id = 0usize;
            while owned.accepting.load(Ordering::Acquire) {
                match listener.accept() {
                    Ok(socket) => {
                        if !owned.accepting.load(Ordering::Acquire) {
                            break;
                        }
                        crate::remote::connection::serve_owned_connection(
                            owned.owner.clone(),
                            crate::remote::connection::ConnectionId(id),
                            socket,
                        );
                        id = id.wrapping_add(1);
                    }
                    Err(error) => {
                        owned.owner.state().push_log(
                            lore_base::log::LoreLogLevel::Error,
                            format!("Failed when accepting: {error}"),
                        );
                    }
                }
            }
        })
        .map_err(|e| e.to_string())?;
    let thread = Arc::new(ThreadOwner::new());
    thread.publish(Some(worker));
    *run.accept_thread.lock().unwrap_or_else(|p| p.into_inner()) = Some(thread);
    Ok(())
}
async fn actor_loop(run: Arc<RunState>) -> Result<(), String> {
    #[cfg(feature = "test-util")]
    test_fault(3);
    let mut requests = run.requests.subscribe();
    let mut watch_early = true;
    loop {
        let stop = run.owner.stop_request().expect("owned run stop binding");
        let pending = run
            .command
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .pending;
        let command = if pending.is_some() {
            let _seen = *requests.borrow_and_update();
            pending
        } else {
            tokio::select! {
                biased;
                () = run.owner.ready_to_exit() => None,
                () = stop.requested(), if watch_early => { watch_early = false; Some(0) },
                changed = requests.changed() => {
                    if changed.is_err() { return Err("service lifecycle channel closed".into()); }
                    Some(*requests.borrow_and_update())
                },
            }
        };
        if let Some(id) = command {
            run.owner.fence_lifecycle();
            let result = run.owner.stop_lifecycle().await.map_err(|e| e.to_string());
            if let Err(error) = result {
                let error = run.with_failure(|fatal| {
                    let error = fatal.cloned().unwrap_or(error);
                    run.owner.retain_error(error.clone());
                    error
                });
                if id != 0 {
                    run.complete_attempt(id, Err(error));
                }
                continue;
            }
        }
        run.owner.state().close_and_drain_invocations().await;
        match run.stop_network().await {
            Ok(()) => return Ok(()),
            Err(error) => {
                run.owner.retain_error(error.clone());
                if let Some(id) = command.filter(|id| *id != 0) {
                    run.complete_attempt(id, Err(error.clone()));
                }
                // Networking ownership is uncertain; retain resources without
                // reporting completion or releasing the exact run reservation.
                return Err(error);
            }
        }
    }
}
