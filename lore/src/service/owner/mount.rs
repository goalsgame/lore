// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use std::sync::Arc;

use lore_vfs::overlay::AccessGate;
use lore_vfs::overlay::AccessGateFactory;
use lore_vfs::overlay::InjectedGateFactory;

use super::Phase;
use super::ServiceOwner;
use crate::working_mount::Error;
use crate::working_mount::InstanceHandle;
use crate::working_mount::Options;
use crate::working_mount::OwnerLimits;
use crate::working_mount::PreparationError;
use crate::working_mount::RejectedStartup;
use crate::working_mount::StartupInput;

fn rejected(input: StartupInput, error: Error) -> PreparationError {
    PreparationError::Rejected(RejectedStartup {
        error,
        input: Box::new(input),
    })
}
impl ServiceOwner {
    fn startup_error(self: &Arc<Self>) -> Option<Error> {
        if !crate::service::state::current_work_for(self) {
            Some(Error::WrongOwner)
        } else if self.stop.get().is_none_or(|stop| !stop.is_current())
            || self.status().0 != Phase::Running
            || self.check_actor().is_err()
        {
            Some(Error::Closed)
        } else {
            None
        }
    }
    /// Adopt explicitly prepared resources with the supplied edit policy.
    /// This entry point exposes no IPC mount-start command.
    #[lore_macro::test_pub]
    #[allow(dead_code, reason = "mount startup is not exposed by command dispatch")]
    pub(crate) async fn start_working(
        self: &Arc<Self>,
        input: StartupInput,
        gate: Arc<dyn AccessGate>,
        limits: OwnerLimits,
        options: Options,
    ) -> Result<InstanceHandle, PreparationError> {
        self.start_with_factory(
            input,
            Arc::new(InjectedGateFactory::new(gate)),
            limits,
            options,
        )
        .await
    }
    /// Explicitly select local checkout edits for this admitted service work.
    /// Remote publication still follows the ordinary server checks.
    #[lore_macro::test_pub]
    #[allow(
        dead_code,
        reason = "local policy selection is not exposed by commands"
    )]
    pub(crate) async fn start_working_local(
        self: &Arc<Self>,
        input: StartupInput,
        limits: OwnerLimits,
        options: Options,
    ) -> Result<InstanceHandle, PreparationError> {
        if let Some(error) = self.startup_error() {
            return Err(rejected(input, error));
        }
        let admission = match crate::service::state::working_admission() {
            Ok(admission) => admission,
            Err(_denied) => return Err(rejected(input, Error::WrongOwner)),
        };
        let factory = match crate::working_mount::local_policy::LocalCheckoutFactory::new(
            admission,
            input.prepared.execution.clone(),
        ) {
            Ok(factory) => factory,
            Err(error) => {
                return Err(rejected(
                    input,
                    Error::Owner(format!("local edit policy: {error:?}")),
                ));
            }
        };
        self.start_with_factory(input, Arc::new(factory), limits, options)
            .await
    }
    async fn start_with_factory(
        self: &Arc<Self>,
        input: StartupInput,
        factory: Arc<dyn AccessGateFactory>,
        limits: OwnerLimits,
        options: Options,
    ) -> Result<InstanceHandle, PreparationError> {
        let owner = self.clone();
        if let Some(error) = owner.startup_error() {
            return Err(rejected(input, error));
        }
        let prepared = owner
            ._supervisor
            .prepare_instance_with_gate_factory(input, factory, limits, options)
            .await?;
        // SAFETY: These return the effective process identity without pointers;
        // no caller-selected UID or GID enters the kernel mount.
        let (uid, gid) = unsafe { (libc::geteuid(), libc::getegid()) };
        prepared.mount(uid, gid).await
    }
}

// Finite startup work outlives caller cancellation, but is not a service-owner
// reference and must not become part of the finished Input or Record.
struct WorkingBootstrapLease {
    _caller: Option<lore_io::lease::IoLease>,
    _ticket: lore_revision::service_state::InvocationGuard,
}
impl ServiceOwner {
    pub(crate) fn reserve_working_registration(
        self: &Arc<Self>,
        target: &std::path::Path,
        metadata: &std::path::Path,
        repository: lore_base::types::RepositoryId,
        instance: lore_revision::instance::InstanceId,
    ) -> Result<lore_revision::fs::provider_registry::Registration, Error> {
        if let Some(error) = self.startup_error() {
            return Err(error);
        }
        let registration = self
            .state
            .provider_registry()
            .reserve(target, instance)
            .map_err(|error| Error::Registry(error.to_string()))?;
        registration
            .reserve_metadata(metadata, repository)
            .map_err(|error| Error::Registry(error.to_string()))?;
        Ok(registration)
    }
    pub(crate) fn working_bootstrap_io(self: &Arc<Self>) -> Result<lore_io::lease::IoLease, Error> {
        if let Some(error) = self.startup_error() {
            return Err(error);
        }
        let ticket = self.state.try_begin_invocation().ok_or(Error::Closed)?;
        Ok(lore_io::lease::IoLease::new(Arc::new(
            WorkingBootstrapLease {
                _caller: lore_io::lease::IoLease::current(),
                _ticket: ticket,
            },
        )))
    }
    pub(crate) async fn bootstrap_working_local(
        self: &Arc<Self>,
        request: crate::working_mount::bootstrap::factory::BootstrapRequest,
        limits: OwnerLimits,
        options: Options,
    ) -> Result<InstanceHandle, crate::working_mount::bootstrap::factory::BootstrapError> {
        use crate::working_mount::bootstrap::factory::BootstrapError;
        use crate::working_mount::bootstrap::factory::BootstrapPolicy;
        let reject = |error| BootstrapError {
            handle: None,
            error,
        };
        if let Some(error) = self.startup_error() {
            return Err(reject(error));
        }
        let admission = crate::service::state::working_admission()
            .map_err(|_denied| reject(Error::WrongOwner))?;
        let lease = self.working_bootstrap_io().map_err(reject)?;
        lease
            .scope(self._supervisor.bootstrap_mount(
                request,
                BootstrapPolicy::Local(admission),
                limits,
                options,
            ))
            .await
    }
}
