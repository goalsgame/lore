// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Service state for the Lore background service.
//!
//! This module provides [`ServiceStateImpl`], which holds service-level state:
//! uptime tracking, connection counting, buffered log messages and working-provider
//! routing. Unix service runs use fresh injected instances; legacy Windows and
//! SWFS callback paths retain [`ServiceStateImpl::global`]. Callers should pass
//! the state for their exact run rather than resolving a process-global owner.

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::OnceLock;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Instant;

use lore_base::log::LoreLogLevel;
use parking_lot::Mutex;

/// Maximum number of log messages to buffer before dropping oldest.
pub const MAX_BUFFER_SIZE: usize = 30;

/// Status and stop remain available during work retirement without unbounded
/// control tasks. Only trusted service dispatch classifies a control command.
pub const MAX_CONTROL_INVOCATIONS: u64 = 32;

/// A log message captured by the service.
#[derive(Debug, Clone)]
pub struct LogMessage {
    /// The severity level of the log message.
    pub level: LoreLogLevel,
    /// The log message text.
    pub message: String,
}

/// The state the service process shares, created on first access.
static SERVICE_STATE: LazyLock<Arc<ServiceStateImpl>> =
    LazyLock::new(|| Arc::new(ServiceStateImpl::new()));

/// Service-level state: when the service started, how many clients are
/// connected, command tasks still running, buffered logs, and its provider registry.
///
/// Every field carries its own synchronization, so callers share one instance
/// behind an [`Arc`] rather than a lock.
pub struct ServiceStateImpl {
    /// When the service was initialized, unset until [`Self::initialize`] runs.
    start_time: OnceLock<Instant>,
    /// Number of active client connections.
    connection_count: AtomicU32,
    /// Command tasks, including tasks whose originating connection has closed.
    invocation_count: AtomicU64,
    work_invocation_count: AtomicU64,
    control_invocation_count: AtomicU64,
    invocation_admission: Mutex<Admission>,
    invocations_changed: tokio::sync::Notify,
    /// Number of log messages dropped due to buffer overflow.
    dropped_count: AtomicU64,
    /// Buffered service-level log messages.
    log_buffer: Mutex<VecDeque<LogMessage>>,
    provider_registry: Arc<crate::fs::provider_registry::ProviderRegistry>,
}

struct Admission {
    work: bool,
    control: bool,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum InvocationKind {
    Work,
    Control,
}

/// RAII guard holding one unit of the active connection count.
///
/// [`ServiceStateImpl::increment_connections`] returns one, and dropping it
/// decrements the count. The count therefore cannot drift when a connection ends
/// on an early return, an error, or a cancelled task.
#[must_use = "the connection count drops back as soon as this guard is dropped"]
pub struct ConnectionGuard {
    state: Arc<ServiceStateImpl>,
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        self.state.decrement_connections();
    }
}

/// Retains command accounting independently of its originating connection.
#[must_use = "retain this guard through the complete invocation task"]
pub struct InvocationGuard {
    state: Arc<ServiceStateImpl>,
    kind: InvocationKind,
}

impl InvocationGuard {
    /// The service instance that admitted this command.
    pub fn service_state(&self) -> &Arc<ServiceStateImpl> {
        &self.state
    }

    /// Commit to graceful shutdown only when this is the sole admitted command.
    /// Admission and the count check are serialized with new command admission.
    /// A busy or already closed service returns false without changing admission.
    ///
    /// Call only after other shutdown preconditions have passed. Success is
    /// terminal even if this command is cancelled; the service owner must finish
    /// shutdown and drain after this guard drops, rather than await its own ticket.
    pub fn try_close_admission_if_last(&self) -> bool {
        let mut admitted = self.state.invocation_admission.lock();
        if !admitted.work || self.state.invocation_count.load(Ordering::Acquire) != 1 {
            return false;
        }
        admitted.work = false;
        admitted.control = false;
        true
    }

    /// Whether trusted dispatch admitted this as control rather than work.
    pub fn is_control(&self) -> bool {
        self.kind == InvocationKind::Control
    }

    /// A control command can begin graceful retirement only when no normal
    /// command is admitted. Other control tickets do not prevent retirement.
    /// Success leaves control admission open for status and retry-stop requests.
    /// Complete other fallible preconditions first, then synchronously request
    /// owner retirement after success, without a cancellation point in between.
    pub fn try_close_work_admission_if_idle(&self) -> bool {
        if !self.is_control() {
            return false;
        }
        let mut admission = self.state.invocation_admission.lock();
        if !admission.work || self.state.work_invocation_count() != 0 {
            return false;
        }
        admission.work = false;
        true
    }
}

impl Drop for InvocationGuard {
    fn drop(&mut self) {
        match self.kind {
            InvocationKind::Work => &self.state.work_invocation_count,
            InvocationKind::Control => &self.state.control_invocation_count,
        }
        .fetch_sub(1, Ordering::AcqRel);
        self.state.invocation_count.fetch_sub(1, Ordering::AcqRel);
        self.state.invocations_changed.notify_waiters();
    }
}

impl ServiceStateImpl {
    /// Creates state for a service that has not started yet.
    pub fn new() -> Self {
        Self {
            start_time: OnceLock::new(),
            connection_count: AtomicU32::new(0),
            invocation_count: AtomicU64::new(0),
            work_invocation_count: AtomicU64::new(0),
            control_invocation_count: AtomicU64::new(0),
            invocation_admission: Mutex::new(Admission {
                work: true,
                control: true,
            }),
            invocations_changed: tokio::sync::Notify::new(),
            dropped_count: AtomicU64::new(0),
            log_buffer: Mutex::new(VecDeque::new()),
            provider_registry: crate::fs::provider_registry::ProviderRegistry::new(
                Default::default(),
            ),
        }
    }

    /// Working providers registered by this service instance. Command contexts
    /// route through this same registry; independent service states are isolated.
    pub fn provider_registry(&self) -> &Arc<crate::fs::provider_registry::ProviderRegistry> {
        &self.provider_registry
    }

    /// Returns the state the service process shares.
    ///
    /// Retained for legacy Windows service state and SWFS C callback logging.
    /// Unix run ownership, status and command dispatch use their injected state.
    pub fn global() -> &'static Arc<Self> {
        &SERVICE_STATE
    }

    /// Starts tracking uptime. The first call sets the start time and later
    /// calls leave it where it is, so a second initialization cannot make a
    /// running service look freshly started.
    pub fn initialize(&self) {
        let _ = self.start_time.set(Instant::now());
    }

    /// Returns whether the service state has been initialized.
    pub fn is_initialized(&self) -> bool {
        self.start_time.get().is_some()
    }

    /// Pushes a log message to the buffer, dropping the oldest if full.
    pub fn push_log(&self, level: LoreLogLevel, message: String) {
        let mut buffer = self.log_buffer.lock();
        if buffer.len() >= MAX_BUFFER_SIZE {
            buffer.pop_front();
            self.dropped_count.fetch_add(1, Ordering::Relaxed);
        }
        buffer.push_back(LogMessage { level, message });
    }

    /// Drains all buffered log messages and returns them along with the drop count.
    /// The drop count is reset to zero after draining.
    pub fn drain_logs(&self) -> (Vec<LogMessage>, u64) {
        let mut buffer = self.log_buffer.lock();
        let messages: Vec<LogMessage> = buffer.drain(..).collect();
        let dropped = self.dropped_count.swap(0, Ordering::Relaxed);
        (messages, dropped)
    }

    /// Increments the active connection count, returning a [`ConnectionGuard`]
    /// that decrements it again when dropped. Hold the guard for as long as the
    /// connection is active.
    pub fn increment_connections(self: &Arc<Self>) -> ConnectionGuard {
        self.connection_count.fetch_add(1, Ordering::Relaxed);
        ConnectionGuard {
            state: Arc::clone(self),
        }
    }

    /// Acquire before spawning, so even an unpolled invocation is accounted for.
    /// Returns `None` once service command admission has permanently closed.
    pub fn try_begin_invocation(self: &Arc<Self>) -> Option<InvocationGuard> {
        let admitted = self.invocation_admission.lock();
        if !admitted.work {
            return None;
        }
        self.work_invocation_count.fetch_add(1, Ordering::AcqRel);
        self.invocation_count.fetch_add(1, Ordering::AcqRel);
        Some(InvocationGuard {
            state: Arc::clone(self),
            kind: InvocationKind::Work,
        })
    }

    /// Admit a bounded status/stop task. Call only after trusted dispatch has
    /// classified the typed command; serialized caller data cannot select this.
    pub fn try_begin_control_invocation(self: &Arc<Self>) -> Option<InvocationGuard> {
        let admission = self.invocation_admission.lock();
        if !admission.control
            || self.control_invocation_count.load(Ordering::Acquire) >= MAX_CONTROL_INVOCATIONS
        {
            return None;
        }
        self.control_invocation_count.fetch_add(1, Ordering::AcqRel);
        self.invocation_count.fetch_add(1, Ordering::AcqRel);
        Some(InvocationGuard {
            state: Arc::clone(self),
            kind: InvocationKind::Control,
        })
    }

    /// Fence normal commands while retaining bounded status/stop admission.
    /// This never reopens either kind of admission after terminal closure.
    pub fn close_work_admission(&self) {
        self.invocation_admission.lock().work = false;
    }

    /// Fence and drain normal commands, excluding control tickets. A cancelled
    /// wait leaves work closed and can be retried. Do not call from a work task.
    pub async fn close_and_drain_work_invocations(&self) {
        self.close_work_admission();
        loop {
            let changed = self.invocations_changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.work_invocation_count() == 0 {
                return;
            }
            changed.await;
        }
    }

    pub fn work_invocation_count(&self) -> u64 {
        self.work_invocation_count.load(Ordering::Acquire)
    }

    /// Permanently fences new commands, including commands on accepted sockets.
    /// Already admitted tasks keep their tickets until they finish or are cancelled.
    pub fn close_invocation_admission(&self) {
        let mut admission = self.invocation_admission.lock();
        admission.work = false;
        admission.control = false;
    }

    /// Fences commands and waits for admitted task tickets. Call from the service
    /// owner, never from an invocation holding its own ticket. This does not cancel
    /// commands or drain their separately owned disk jobs and checkpoint publishers.
    /// A cancelled wait leaves admission closed and can safely be retried.
    pub async fn close_and_drain_invocations(&self) {
        self.close_invocation_admission();
        loop {
            let changed = self.invocations_changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.invocation_count() == 0 {
                return;
            }
            changed.await;
        }
    }

    /// Includes detached commands. This is not an admission fence: a supervisor
    /// must stop new work before using a zero count to establish quiescence.
    /// Storage owners must separately drain background IO and publication jobs.
    pub fn invocation_count(&self) -> u64 {
        self.invocation_count.load(Ordering::Acquire)
    }

    /// Decrements the active connection count.
    ///
    /// Private so that the count is only ever released by dropping the
    /// [`ConnectionGuard`] that claimed it.
    fn decrement_connections(&self) {
        self.connection_count.fetch_sub(1, Ordering::Relaxed);
    }

    /// Returns the number of milliseconds since the service was initialized.
    /// Returns 0 if not yet initialized.
    pub fn uptime_ms(&self) -> u64 {
        self.start_time
            .get()
            .map_or(0, |t| t.elapsed().as_millis() as u64)
    }

    /// Returns the current active connection count.
    pub fn connection_count(&self) -> u32 {
        self.connection_count.load(Ordering::Relaxed)
    }
}

impl Default for ServiceStateImpl {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Each test owns its own `ServiceStateImpl`, so none of them can see a
    // count or a buffered message another test put there.

    #[test]
    fn draining_returns_what_was_pushed_and_empties_the_buffer() {
        let state = ServiceStateImpl::new();

        state.push_log(LoreLogLevel::Info, "first".to_string());
        state.push_log(LoreLogLevel::Error, "second".to_string());

        let (messages, dropped) = state.drain_logs();
        assert_eq!(dropped, 0);
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].level, LoreLogLevel::Info);
        assert_eq!(messages[0].message, "first");
        assert_eq!(messages[1].level, LoreLogLevel::Error);
        assert_eq!(messages[1].message, "second");

        let (messages, dropped) = state.drain_logs();
        assert!(messages.is_empty(), "The buffer should have been drained");
        assert_eq!(dropped, 0, "The drop count resets when it is reported");
    }

    #[test]
    fn a_full_buffer_drops_the_oldest_messages_and_counts_them() {
        let state = ServiceStateImpl::new();

        for index in 0..MAX_BUFFER_SIZE + 2 {
            state.push_log(LoreLogLevel::Info, index.to_string());
        }

        let (messages, dropped) = state.drain_logs();
        assert_eq!(messages.len(), MAX_BUFFER_SIZE, "The buffer stays bounded");
        assert_eq!(dropped, 2);
        assert_eq!(
            messages[0].message, "2",
            "The two oldest messages are the ones dropped"
        );
    }

    #[test]
    fn a_connection_guard_holds_one_unit_of_the_count() {
        let state = Arc::new(ServiceStateImpl::new());
        assert_eq!(state.connection_count(), 0);

        let guard = state.increment_connections();
        assert_eq!(state.connection_count(), 1);

        drop(guard);
        assert_eq!(state.connection_count(), 0);
    }

    #[test]
    fn uptime_reads_zero_until_the_state_is_initialized() {
        let state = ServiceStateImpl::new();
        assert!(!state.is_initialized());
        assert_eq!(state.uptime_ms(), 0);

        state.initialize();
        assert!(state.is_initialized());
    }

    #[test]
    fn the_global_state_is_a_single_shared_instance() {
        assert!(
            Arc::ptr_eq(ServiceStateImpl::global(), ServiceStateImpl::global()),
            "Every caller of global() should reach the same state"
        );
    }
}
