// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! One Unix service lifetime with a private working supervisor. Service entry
//! points expose no working-mount startup API.
#[cfg(target_os = "linux")]
mod mount;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use lore_revision::service_state::ServiceStateImpl;

use crate::remote::service_process::ServiceStopRequest;
use crate::working_mount::Limits;
use crate::working_mount::Supervisor;

#[lore_macro::test_pub]
pub(crate) struct ServiceOwner {
    state: Arc<ServiceStateImpl>,
    _supervisor: Arc<Supervisor>,
    stop: OnceLock<ServiceStopRequest>,
    closed: AtomicBool,
    stop_serial: Arc<tokio::sync::Mutex<()>>,
    shutdown: std::sync::Mutex<Shutdown>,
    ready: tokio::sync::watch::Sender<bool>,
    actor: OnceLock<std::sync::Weak<super::actor::RunState>>,
}
impl ServiceOwner {
    #[lore_macro::test_pub]
    pub(crate) fn for_state(
        state: Arc<ServiceStateImpl>,
    ) -> crate::working_mount::Result<Arc<Self>> {
        // Probe terminal admission without changing the state or reopening it.
        let probe = state
            .try_begin_invocation()
            .ok_or(crate::working_mount::Error::Closed)?;
        drop(probe);
        let supervisor = Supervisor::new(state.provider_registry().clone(), Limits::default())?;
        Ok(Arc::new(Self {
            state,
            _supervisor: supervisor,
            stop: OnceLock::new(),
            closed: AtomicBool::new(false),
            stop_serial: Arc::new(tokio::sync::Mutex::new(())),
            shutdown: std::sync::Mutex::new(Shutdown {
                phase: Phase::Running,
                error: None,
                attempt: None,
                preconditions_done: false,
            }),
            ready: tokio::sync::watch::channel(false).0,
            actor: OnceLock::new(),
        }))
    }
    pub(crate) fn state(&self) -> &Arc<ServiceStateImpl> {
        &self.state
    }
    pub(crate) fn attach_actor(&self, actor: &Arc<super::actor::RunState>) {
        let _bound = self.actor.set(Arc::downgrade(actor));
    }
    pub(crate) fn actor(&self) -> Option<Arc<super::actor::RunState>> {
        self.actor.get().and_then(std::sync::Weak::upgrade)
    }
    pub(crate) fn fence_lifecycle(&self) {
        self.state.close_work_admission();
        if let Err(error) = self._supervisor.request_retirement_all() {
            if let Some(fatal) = self.actor().and_then(|actor| actor.failure()) {
                self.retain_error(fatal);
            } else {
                self.retain_error(error.to_string());
            }
        }
    }
    pub(crate) fn retain_error(&self, error: String) {
        let mut state = self
            .shutdown
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        state.phase = Phase::Retained;
        state.error = Some(error);
    }
    pub(crate) fn bind_stop(&self, stop: ServiceStopRequest) -> bool {
        self.stop.set(stop).is_ok()
    }
    pub(crate) fn stop_request(&self) -> Option<ServiceStopRequest> {
        self.stop.get().cloned()
    }
    pub(crate) fn close(&self) {
        if !self.closed.swap(true, Ordering::AcqRel) {
            self.state.close_invocation_admission();
            let _retired = self._supervisor.request_retirement_all();
        }
    }
}
impl Drop for ServiceOwner {
    fn drop(&mut self) {
        self.close();
    }
}
/// Separate from Arc lifetime: an old accepted socket may keep this owner alive
/// after `service_main` exits, but must never admit another command on that run.
#[lore_macro::test_pub]
#[cfg(feature = "test-util")]
pub(crate) struct RunCleanup(Arc<ServiceOwner>);
#[cfg(feature = "test-util")]
impl RunCleanup {
    #[lore_macro::test_pub]
    pub(crate) fn new(owner: Arc<ServiceOwner>) -> Self {
        Self(owner)
    }
}
#[cfg(feature = "test-util")]
impl Drop for RunCleanup {
    fn drop(&mut self) {
        self.0.close();
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[lore_macro::test_pub]
pub(crate) enum Phase {
    Running,
    Stopping,
    Retained,
    ReadyToExit,
}
struct Shutdown {
    phase: Phase,
    error: Option<String>,
    attempt: Option<Arc<Attempt>>,
    preconditions_done: bool,
}
struct Attempt {
    done: tokio::sync::watch::Sender<Option<Result<(), String>>>,
}
impl Attempt {
    async fn wait(&self) -> Result<(), crate::service::ServiceStopError> {
        let mut done = self.done.subscribe();
        loop {
            if let Some(result) = done.borrow_and_update().clone() {
                return result.map_err(crate::service::ServiceStopError::internal);
            }
            done.changed().await.map_err(|_closed| {
                crate::service::ServiceStopError::internal("retirement completion channel closed")
            })?;
        }
    }
}
struct AttemptTask {
    owner: Arc<ServiceOwner>,
    attempt: Arc<Attempt>,
    complete: bool,
    _serial: tokio::sync::OwnedMutexGuard<()>,
}
impl AttemptTask {
    fn finish(&mut self, result: Result<(), String>) {
        if let Some(actor) = self.owner.actor() {
            actor.with_failure(|failure| self.publish(result, failure));
        } else {
            self.publish(result, None);
        }
    }
    fn publish(&mut self, result: Result<(), String>, failure: Option<&String>) {
        let result = failure.map_or(result, |error| Err(error.clone()));
        self.complete = true;
        let success = result.is_ok();
        if success && let Some(stop) = self.owner.stop.get() {
            stop.request_stop();
        }
        {
            let mut state = self
                .owner
                .shutdown
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            state.phase = if success {
                Phase::ReadyToExit
            } else {
                Phase::Retained
            };
            state.error = result.as_ref().err().cloned();
        }
        if success {
            self.owner.ready.send_replace(true);
        }
        self.attempt.done.send_replace(Some(result));
    }
}
impl Drop for AttemptTask {
    fn drop(&mut self) {
        if !self.complete {
            self.finish(Err(
                "retirement task ended without proving resource drain".into()
            ));
        }
    }
}
impl ServiceOwner {
    #[lore_macro::test_pub]
    pub(crate) fn status(&self) -> (Phase, Option<String>) {
        let state = self
            .shutdown
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        (state.phase, state.error.clone())
    }
    pub(crate) async fn ready_to_exit(&self) {
        let mut ready = self.ready.subscribe();
        while !*ready.borrow_and_update() {
            if ready.changed().await.is_err() {
                return;
            }
        }
    }
    #[lore_macro::test_pub]
    pub(crate) async fn stop_control(
        self: &Arc<Self>,
        force: bool,
    ) -> Result<(), crate::service::ServiceStopError> {
        if !super::state::current_control_for(self) {
            return Err(crate::service::ServiceStopError::internal(
                "service stop requires its own control ticket",
            ));
        }
        let result = self.stop_inner(force, false, true).await;
        self.check_actor()?;
        result
    }
    pub(crate) async fn stop_lifecycle(
        self: &Arc<Self>,
    ) -> Result<(), crate::service::ServiceStopError> {
        let result = self.stop_inner(true, true, true).await;
        self.check_actor()?;
        result
    }
    pub(crate) async fn stop_uninitialized(
        self: &Arc<Self>,
    ) -> Result<(), crate::service::ServiceStopError> {
        self.stop_inner(true, true, false).await
    }
    fn check_actor(&self) -> Result<(), crate::service::ServiceStopError> {
        if let Some(error) = self.actor().and_then(|actor| actor.failure()) {
            return Err(crate::service::ServiceStopError::internal(error));
        }
        Ok(())
    }
    async fn stop_inner(
        self: &Arc<Self>,
        force: bool,
        lifecycle: bool,
        legacy_mounts: bool,
    ) -> Result<(), crate::service::ServiceStopError> {
        use lore_error_set::prelude::*;
        use lore_revision::fs::swfs::mount_manager_state::MountManagerState;
        if let Some(error) = self.actor().and_then(|actor| actor.failure()) {
            return Err(crate::service::ServiceStopError::internal(error));
        }
        if self.stop.get().is_none_or(|stop| !stop.is_current()) {
            return Ok(());
        }
        let existing = {
            let state = self
                .shutdown
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            if state.phase == Phase::Stopping {
                state.attempt.clone()
            } else {
                None
            }
        };
        if let Some(attempt) = existing {
            return attempt.wait().await;
        }
        let serial = self.stop_serial.clone().lock_owned().await;
        self.check_actor()?;
        if self.stop.get().is_none_or(|stop| !stop.is_current()) {
            return Ok(());
        }
        let phase = self.status().0;
        if phase == Phase::ReadyToExit {
            return Ok(());
        }
        let preconditions_done = self
            .shutdown
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .preconditions_done;
        if !preconditions_done {
            // Fallible legacy preconditions precede the terminal work fence.
            if !force && self.state.work_invocation_count() != 0 {
                return Err(crate::service::ServiceStopError::internal(
                    "service has active commands; retry when idle or use force",
                ));
            }
            #[cfg(feature = "test-util")]
            if legacy_mounts && super::actor::take_precondition_failure() {
                return Err(crate::service::ServiceStopError::internal(
                    "injected legacy unmount precondition failure",
                ));
            }
            if legacy_mounts && let Some(manager) = MountManagerState::mount_manager() {
                manager
                    .unmount_all(force)
                    .await
                    .forward_any::<crate::service::ServiceStopError>(
                        "Unmounting before the service stops",
                    )?;
            }
            if self.stop.get().is_none_or(|stop| !stop.is_current()) {
                return Ok(());
            }
            self.check_actor()?;
            if force || phase == Phase::Retained {
                self.state.close_work_admission();
            } else if lifecycle || !super::state::close_current_work_if_idle(self) {
                return Err(crate::service::ServiceStopError::internal(
                    "service has active commands; retry when idle or use force",
                ));
            }
            self.shutdown
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .preconditions_done = true;
        }
        // No await may separate committing shutdown from fencing every owner.
        if let Err(error) = self._supervisor.request_retirement_all() {
            let mut state = self
                .shutdown
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            state.phase = Phase::Retained;
            state.error = Some(error.to_string());
            return Err(crate::service::ServiceStopError::internal(
                error.to_string(),
            ));
        }
        let attempt = Arc::new(Attempt {
            done: tokio::sync::watch::channel(None).0,
        });
        let install = |failure: Option<&String>| {
            if let Some(error) = failure {
                return Err(crate::service::ServiceStopError::internal(error.clone()));
            }
            let mut state = self
                .shutdown
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            state.phase = Phase::Stopping;
            state.error = None;
            state.attempt = Some(attempt.clone());
            Ok(())
        };
        if let Some(actor) = self.actor() {
            actor.with_failure(install)?;
        } else {
            install(None)?;
        }
        self.launch_retirement(attempt.clone(), serial);
        attempt.wait().await
    }
    /// The service owns this task independently of a direct caller's wait. No
    /// invocation callback, caller state or command context is captured.
    #[allow(clippy::disallowed_methods)]
    fn launch_retirement(
        self: &Arc<Self>,
        attempt: Arc<Attempt>,
        serial: tokio::sync::OwnedMutexGuard<()>,
    ) {
        let mut task = AttemptTask {
            owner: self.clone(),
            attempt,
            complete: false,
            _serial: serial,
        };
        let runtime = self
            .actor()
            .and_then(|actor| actor.runtime())
            .unwrap_or_else(lore_base::runtime::core_runtime);
        runtime.spawn(async move {
            task.owner.state.close_and_drain_work_invocations().await;
            let result = match task.owner._supervisor.close_and_drain().await {
                Ok(report) if report.failures.is_empty() => Ok(()),
                Ok(report) => Err(format!(
                    "{} working owner(s) remain retained: {}",
                    report.failures.len(),
                    report.failures[0].error
                )),
                Err(error) => Err(error.to_string()),
            };
            task.finish(result);
        });
    }
}
