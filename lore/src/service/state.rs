// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Service state and trusted admission scope for an IPC command.

use std::future::Future;
use std::sync::Arc;

pub use lore_revision::service_state::ConnectionGuard;
pub use lore_revision::service_state::InvocationGuard;
pub use lore_revision::service_state::LogMessage;
pub use lore_revision::service_state::MAX_BUFFER_SIZE;
pub use lore_revision::service_state::ServiceStateImpl;

tokio::task_local! {
    static INVOCATION: InvocationGuard;
}

/// Retain the admitted ticket through the complete command task. This scope is
/// not propagated by background task spawning or stored in an execution context.
pub async fn with_invocation<T>(ticket: InvocationGuard, work: impl Future<Output = T>) -> T {
    INVOCATION.scope(ticket, work).await
}

/// Resolve the exact service owner for this command, without a process-global
/// fallback that could point at another service lifetime.
pub fn current_service_state() -> Option<Arc<ServiceStateImpl>> {
    INVOCATION
        .try_with(|ticket| ticket.service_state().clone())
        .ok()
}

/// Commit graceful command admission closure using this command's own ticket.
/// None means the caller is outside trusted service dispatch. Success must be
/// followed synchronously by requesting owner shutdown, with no intervening await.
pub fn try_close_current_admission() -> Option<bool> {
    INVOCATION
        .try_with(InvocationGuard::try_close_admission_if_last)
        .ok()
}

#[cfg(unix)]
tokio::task_local! {
    static OWNER: Arc<crate::service::owner::ServiceOwner>;
}
#[cfg(unix)]
#[derive(Debug)]
#[lore_macro::test_pub]
pub(crate) struct OwnerScopeMismatch;
/// Only the accept loop supplies an owner. Caller-serialized context cannot
/// select it, and a mismatched ticket fails before constructing command work.
#[cfg(unix)]
#[lore_macro::test_pub]
pub(crate) async fn with_owner<T>(
    owner: Arc<crate::service::owner::ServiceOwner>,
    work: impl Future<Output = T>,
) -> Result<T, OwnerScopeMismatch> {
    if current_service_state().is_none_or(|state| !Arc::ptr_eq(&state, owner.state())) {
        return Err(OwnerScopeMismatch);
    }
    Ok(OWNER.scope(owner, work).await)
}
#[cfg(unix)]
#[lore_macro::test_pub]
pub(crate) fn resolve_owner(
    current: Option<Arc<crate::service::owner::ServiceOwner>>,
) -> Option<Arc<crate::service::owner::ServiceOwner>> {
    let ticket = current_service_state();
    if let Ok(owner) = OWNER.try_with(Arc::clone) {
        return ticket
            .filter(|state| Arc::ptr_eq(state, owner.state()))
            .map(|_state| owner);
    }
    if let Some(state) = ticket {
        return current.filter(|owner| Arc::ptr_eq(owner.state(), &state));
    }
    current
}
#[cfg(unix)]
pub(crate) fn current_owner() -> Option<Arc<crate::service::owner::ServiceOwner>> {
    resolve_owner(crate::remote::service_process::current_service_owner())
}
#[cfg(unix)]
#[lore_macro::test_pub]
pub(crate) fn capture_stop_request() -> Option<crate::remote::service_process::ServiceStopRequest> {
    if OWNER.try_with(|_owner| ()).is_ok() || current_service_state().is_some() {
        return current_owner().and_then(|owner| owner.stop_request());
    }
    crate::remote::service_process::current_stop_request()
}

#[cfg(unix)]
pub(crate) fn has_service_scope() -> bool {
    current_service_state().is_some() || OWNER.try_with(|_owner| ()).is_ok()
}

#[cfg(unix)]
pub(crate) fn current_control_for(owner: &Arc<crate::service::owner::ServiceOwner>) -> bool {
    current_owner().is_some_and(|current| Arc::ptr_eq(&current, owner))
        && INVOCATION
            .try_with(|ticket| {
                ticket.is_control() && Arc::ptr_eq(ticket.service_state(), owner.state())
            })
            .unwrap_or(false)
}
#[cfg(unix)]
pub(crate) fn current_work_for(owner: &Arc<crate::service::owner::ServiceOwner>) -> bool {
    OWNER
        .try_with(|current| Arc::ptr_eq(current, owner))
        .unwrap_or(false)
        && INVOCATION
            .try_with(|ticket| {
                !ticket.is_control() && Arc::ptr_eq(ticket.service_state(), owner.state())
            })
            .unwrap_or(false)
}

/// A locally minted editing selection tied to one retained service lifetime.
/// It carries no remote publishing permission or ownership of that service.
#[cfg(unix)]
#[derive(Clone)]
#[lore_macro::test_pub]
pub(crate) struct WorkingAdmission {
    owner: std::sync::Weak<crate::service::owner::ServiceOwner>,
    stop: crate::remote::service_process::ServiceStopRequest,
    uid: u32,
    gid: u32,
}
#[cfg(unix)]
impl WorkingAdmission {
    /// Retained cleanup uses the same token after ordinary work admission has
    /// closed. Its kernel/provider admission is fenced separately.
    #[lore_macro::test_pub]
    #[allow(
        dead_code,
        reason = "local policy selection is not exposed by commands"
    )]
    pub(crate) fn validate(&self) -> lore_vfs::core::Result<()> {
        // SAFETY: These return effective process identities without pointers.
        let identity = unsafe { (libc::geteuid(), libc::getegid()) };
        if identity != (self.uid, self.gid)
            || self.owner.upgrade().is_none()
            || !self.stop.is_current()
        {
            return Err(lore_vfs::core::Error::PermissionDenied);
        }
        Ok(())
    }
}
#[cfg(unix)]
#[lore_macro::test_pub]
#[allow(
    dead_code,
    reason = "local policy selection is not exposed by commands"
)]
pub(crate) fn working_admission() -> lore_vfs::core::Result<WorkingAdmission> {
    let owner = OWNER
        .try_with(Arc::clone)
        .map_err(|_scope| lore_vfs::core::Error::PermissionDenied)?;
    if !current_work_for(&owner)
        || owner.status().0 != crate::service::owner::Phase::Running
        || owner.actor().is_some_and(|actor| actor.failure().is_some())
    {
        return Err(lore_vfs::core::Error::PermissionDenied);
    }
    let stop = owner
        .stop_request()
        .filter(crate::remote::service_process::ServiceStopRequest::is_current)
        .ok_or(lore_vfs::core::Error::PermissionDenied)?;
    // SAFETY: No caller-selected UID/GID or credential data enters this token.
    let (uid, gid) = unsafe { (libc::geteuid(), libc::getegid()) };
    Ok(WorkingAdmission {
        owner: Arc::downgrade(&owner),
        stop,
        uid,
        gid,
    })
}
#[cfg(unix)]
pub(crate) fn close_current_work_if_idle(owner: &Arc<crate::service::owner::ServiceOwner>) -> bool {
    current_control_for(owner)
        && INVOCATION
            .try_with(InvocationGuard::try_close_work_admission_if_idle)
            .unwrap_or(false)
}
#[cfg(unix)]
#[derive(Debug)]
pub(crate) enum ControlError {
    Busy,
    WrongScope,
}
#[cfg(unix)]
pub(crate) enum ControlPermit {
    Scoped,
    Direct(InvocationGuard),
}
#[cfg(unix)]
pub(crate) fn admit_control(
    owner: &Arc<crate::service::owner::ServiceOwner>,
) -> Result<ControlPermit, ControlError> {
    if has_service_scope() {
        if !current_control_for(owner) {
            return Err(ControlError::WrongScope);
        }
        return Ok(ControlPermit::Scoped);
    }
    Ok(ControlPermit::Direct(
        owner
            .state()
            .try_begin_control_invocation()
            .ok_or(ControlError::Busy)?,
    ))
}
#[cfg(unix)]
pub(crate) async fn scope_control<T>(
    owner: Arc<crate::service::owner::ServiceOwner>,
    permit: ControlPermit,
    work: impl Future<Output = T>,
) -> T {
    match permit {
        ControlPermit::Scoped => work.await,
        ControlPermit::Direct(ticket) => with_invocation(ticket, OWNER.scope(owner, work)).await,
    }
}
