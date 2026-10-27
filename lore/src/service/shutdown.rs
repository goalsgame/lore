// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
//! Admission fencing for retryable library shutdown.

use std::sync::Mutex;
use std::time::Duration;

use crate::ShutdownError;

#[derive(Clone, Copy)]
enum Phase {
    Open,
    Preparing(u64),
    Terminal,
    Unavailable(&'static str),
}

struct State {
    phase: Phase,
    epoch: u64,
}

/// Coordinates synchronous start publication with shutdown preflight. Waiting
/// for an actor and destroying resources always happen outside this mutex.
#[allow(private_interfaces)]
#[lore_macro::test_pub]
pub(crate) struct Coordinator {
    state: Mutex<State>,
}

impl Default for Coordinator {
    fn default() -> Self {
        Self::new()
    }
}

impl Coordinator {
    #[lore_macro::test_pub]
    pub(crate) const fn new() -> Self {
        Self {
            state: Mutex::new(State {
                phase: Phase::Open,
                epoch: 0,
            }),
        }
    }

    /// Publishes a start capability before shutdown can inspect the current run.
    /// The closure must be synchronous and must not wait for actor cleanup.
    /// It must publish an exact run capability before spawning any owned work,
    /// and unwinding must drop every unpublished resource. After spawning, the
    /// actor must keep that published capability alive independently of callers.
    #[lore_macro::test_pub]
    pub(crate) fn with_start_admission<T>(
        &self,
        publish: impl FnOnce() -> T,
    ) -> Result<T, ShutdownError> {
        let state = self.state.lock().map_err(|_poison| {
            ShutdownError::AdmissionUnavailable("service start publication panicked")
        })?;
        match state.phase {
            Phase::Open => Ok(publish()),
            Phase::Preparing(_) => Err(ShutdownError::InProgress),
            Phase::Terminal => Err(ShutdownError::AlreadyShutDown),
            Phase::Unavailable(reason) => Err(ShutdownError::AdmissionUnavailable(reason)),
        }
    }

    /// Only one attempt can preflight or tear down. Concurrent attempts return
    /// immediately; a failed preflight reopens start admission for a retry.
    #[lore_macro::test_pub]
    pub(crate) fn try_shutdown(
        &self,
        preflight: impl FnOnce() -> Result<(), ShutdownError>,
        teardown: impl FnOnce() -> Result<(), ShutdownError>,
    ) -> Result<(), ShutdownError> {
        let epoch = {
            let mut state = match self.state.try_lock() {
                Ok(state) => state,
                Err(std::sync::TryLockError::WouldBlock) => return Err(ShutdownError::InProgress),
                Err(std::sync::TryLockError::Poisoned(error)) => {
                    // Publication cannot leave an untracked actor, by the start
                    // closure invariant above. Fail this attempt closed; a retry
                    // may inspect the registry after the unwound start has ended.
                    drop(error.into_inner());
                    self.state.clear_poison();
                    return Err(ShutdownError::AdmissionUnavailable(
                        "service start publication panicked",
                    ));
                }
            };
            match state.phase {
                Phase::Preparing(_) => return Err(ShutdownError::InProgress),
                Phase::Terminal => return Err(ShutdownError::AlreadyShutDown),
                Phase::Unavailable(reason) => {
                    return Err(ShutdownError::AdmissionUnavailable(reason));
                }
                Phase::Open => {}
            }
            let Some(epoch) = state.epoch.checked_add(1) else {
                let reason = "shutdown attempt identity exhausted";
                state.phase = Phase::Unavailable(reason);
                return Err(ShutdownError::AdmissionUnavailable(reason));
            };
            state.epoch = epoch;
            state.phase = Phase::Preparing(epoch);
            epoch
        };
        let lease = Preparation {
            coordinator: self,
            epoch,
        };
        preflight()?;
        // Closing the fence permanently precedes the irreversible runtime claim.
        // Dropping the preparation after this point cannot reopen admission.
        self.state
            .lock()
            .map_err(|_poison| {
                ShutdownError::AdmissionUnavailable("service start publication panicked")
            })?
            .phase = Phase::Terminal;
        drop(lease);
        teardown()
    }
}

struct Preparation<'a> {
    coordinator: &'a Coordinator,
    epoch: u64,
}

impl Drop for Preparation<'_> {
    fn drop(&mut self) {
        if let Ok(mut state) = self.coordinator.state.lock()
            && matches!(state.phase, Phase::Preparing(epoch) if epoch == self.epoch)
        {
            state.phase = Phase::Open;
        }
    }
}

static COORDINATOR: Coordinator = Coordinator::new();

pub(crate) fn with_start_admission<T>(publish: impl FnOnce() -> T) -> Result<T, ShutdownError> {
    COORDINATOR.with_start_admission(publish)
}

pub(crate) fn try_shutdown(
    wait: Duration,
    teardown: impl FnOnce() -> Result<(), ShutdownError>,
) -> Result<(), ShutdownError> {
    COORDINATOR.try_shutdown(
        || match super::actor::current_run() {
            None if crate::remote::service_process::current_stop_request().is_some() => {
                Err(ShutdownError::ServiceRetained(
                    "an admitted service start has no tracked lifecycle actor".into(),
                ))
            }
            None => Ok(()),
            Some(run) => {
                wait_for_service(async move { run.request_and_wait_shutdown().await }, wait)
            }
        },
        teardown,
    )
}

/// Drives an exact run's preflight without claiming or stopping global runtimes.
#[lore_macro::test_pub]
pub(crate) fn wait_for_service(
    completion: impl Future<Output = Result<(), String>> + Send + 'static,
    wait: Duration,
) -> Result<(), ShutdownError> {
    let (sender, receiver) = std::sync::mpsc::sync_channel(1);
    if !lore_base::runtime::shutdown_block_on(
        async move {
            let _ = sender.send(completion.await);
        },
        wait,
    ) {
        return Err(ShutdownError::ServiceTimedOut);
    }
    match receiver
        .try_recv()
        .expect("completed service preflight has a result")
    {
        Ok(()) => Ok(()),
        Err(reason) => Err(ShutdownError::ServiceRetained(reason)),
    }
}
