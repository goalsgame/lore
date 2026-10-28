// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Trusted owner setup retains pending exclusion until exact provider publication.
//! This is not command-loader authority or VFS recovery qualification.

mod anchor_reader;

use super::*;
use crate::fs::filesystem_provider::FilesystemProvider;

/// Ordered transfer to the trusted owner adapter after actual store jobs settle.
/// The context may be installed in an unpublished owner; the permit belongs to
/// the actual startup job, never its long-lived Input, Record, or Model.
pub struct OwnerBuildPayload {
    context: PreparedWorkingContext,
    publication: PendingPublication,
}
impl OwnerBuildPayload {
    /// Trusted adapter boundary, not a generic metadata/store extraction helper.
    /// Retain the permit through publication or actual failed setup cleanup,
    /// including after caller cancellation. This confers no loader admission.
    /// After success, retain the context's ordinary Registration in Input/Record:
    /// its last owner still withdraws readiness independently of permit cleanup.
    pub fn into_trusted_owner_parts(self) -> (PreparedWorkingContext, PendingPublication) {
        (self.context, self.publication)
    }
}
pub struct OwnerBuildBusy {
    context: Box<PendingWorkingContext>,
}
impl OwnerBuildBusy {
    pub fn into_context(self) -> PendingWorkingContext {
        *self.context
    }
}
impl std::fmt::Debug for OwnerBuildBusy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OwnerBuildBusy").finish_non_exhaustive()
    }
}
impl std::fmt::Display for OwnerBuildBusy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("pending owner still has retained setup IO")
    }
}
impl std::error::Error for OwnerBuildBusy {}
impl PendingWorkingContext {
    pub fn into_owner_build(self) -> std::result::Result<OwnerBuildPayload, OwnerBuildBusy> {
        let location = self.context.location.clone();
        let Self { context, _owner } = self;
        match Arc::try_unwrap(_owner) {
            Ok(owner) => Ok(OwnerBuildPayload {
                context,
                publication: PendingPublication {
                    location,
                    provider: None,
                    state: PublicationState::Shared(Arc::new(owner)),
                    failed: false,
                    preflighted: false,
                },
            }),
            Err(owner) => Err(OwnerBuildBusy {
                context: Box::new(Self {
                    context,
                    _owner: owner,
                }),
            }),
        }
    }
}

/// Noncloneable ownership retained by the external actual startup job. Retention
/// pins cover finite setup IO; this grants no new work or provider admission.
pub struct PendingPublication {
    location: WorkingLocation,
    provider: Option<Arc<dyn FilesystemProvider>>,
    state: PublicationState,
    failed: bool,
    preflighted: bool,
}
enum PublicationState {
    Shared(Arc<PendingIoOwner>),
    Unique(UniquePublicationOwner),
    Spent,
}
struct SetupIoOwner {
    _pending: Arc<PendingIoOwner>,
    _caller: Option<IoLease>,
}
struct SetupCompletion(Option<tokio::sync::oneshot::Sender<()>>);
impl Drop for SetupCompletion {
    fn drop(&mut self) {
        if let Some(sender) = self.0.take() {
            let _delivered = sender.send(());
        }
    }
}
struct RetainedSetup {
    _lease: IoLease,
    // Notify only after the final setup lease has released pending ownership.
    _completed: SetupCompletion,
}
impl PendingPublication {
    /// Retain actual owner setup IO and report its final release independently
    /// of cancellation of the caller waiting for it.
    pub fn setup_io(
        &self,
    ) -> std::result::Result<(IoLease, tokio::sync::oneshot::Receiver<()>), Error> {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let lease = IoLease::new(Arc::new(RetainedSetup {
            _lease: self.io_lease()?,
            _completed: SetupCompletion(Some(sender)),
        }));
        Ok((lease, receiver))
    }
    pub fn io_lease(&self) -> std::result::Result<IoLease, Error> {
        match &self.state {
            PublicationState::Shared(owner) if !self.failed => {
                Ok(IoLease::new(Arc::new(SetupIoOwner {
                    _pending: owner.clone(),
                    _caller: IoLease::current(),
                })))
            }
            _ => Err(Error::Runtime),
        }
    }
    fn make_unique(&mut self) -> std::result::Result<(), Error> {
        let state = std::mem::replace(&mut self.state, PublicationState::Spent);
        match state {
            PublicationState::Shared(owner) => {
                let owner = match Arc::try_unwrap(owner) {
                    Ok(owner) => owner,
                    Err(owner) => {
                        self.state = PublicationState::Shared(owner);
                        return Err(Error::Busy);
                    }
                };
                match UniquePublicationOwner::new(owner) {
                    Ok(owner) => {
                        self.state = PublicationState::Unique(owner);
                        Ok(())
                    }
                    Err((error, owner)) => {
                        self.state = PublicationState::Shared(Arc::new(owner));
                        Err(error)
                    }
                }
            }
            state => {
                self.state = state;
                Err(Error::Runtime)
            }
        }
    }
    /// Run on the setup thread OUTSIDE every Record, registry and gate lock.
    /// Busy before preflight retains the unchanged permit for retry. Any later
    /// error or callback unwind quarantines the exact registration, retaining
    /// Client, exclusive admission, provider and parent until external cleanup.
    pub fn prepare<'a>(
        &'a mut self,
        provider: &Arc<dyn FilesystemProvider>,
    ) -> std::result::Result<PreparedPendingPublication<'a>, Error> {
        if self.failed || self.provider.is_some() || self.preflighted {
            return Err(Error::Runtime);
        }
        if let Err(error) = self.make_unique() {
            if !matches!(&error, Error::Busy) {
                let _retired = self.quarantine();
            }
            return Err(error);
        }
        self.provider = Some(provider.clone());
        let PublicationState::Unique(resources) = &self.state else {
            let _retired = self.quarantine();
            return Err(Error::Runtime);
        };
        let mut fence = PreflightFence {
            registration: resources.retire.registration.clone(),
            failed: &mut self.failed,
            armed: true,
        };
        resources.qualification.preflight_publication(
            &resources.retire.registration,
            &self.location,
            provider,
        )?;
        fence.armed = false;
        drop(fence);
        self.preflighted = true;
        Ok(PreparedPendingPublication { permit: self })
    }
    /// Invoke after releasing ALL outer guards on a failed final commit. The
    /// exclusive gate remains closed until this terminal withdrawal completes.
    /// This does not release resources or perform the external owner's drain.
    pub fn quarantine(&mut self) -> std::result::Result<(), Error> {
        self.failed = true;
        match &self.state {
            PublicationState::Shared(owner) => {
                owner._retire.registration.retire().map_err(Into::into)
            }
            PublicationState::Unique(owner) => {
                owner.retire.registration.retire().map_err(Into::into)
            }
            PublicationState::Spent => Err(Error::Runtime),
        }
    }
}
struct PreflightFence<'a> {
    registration: Arc<Registration>,
    failed: &'a mut bool,
    armed: bool,
}
impl Drop for PreflightFence<'_> {
    fn drop(&mut self) {
        if self.armed {
            *self.failed = true;
            let _retired = self.registration.retire();
        }
    }
}
struct UniquePublicationOwner {
    retire: Retire,
    qualification: PendingQualification,
    client: Option<tokio::sync::OwnedMutexGuard<()>>,
    _parent: Option<IoLease>,
}
impl UniquePublicationOwner {
    fn new(mut owner: PendingIoOwner) -> std::result::Result<Self, (Error, PendingIoOwner)> {
        let Some(token) = owner.token.take() else {
            return Err((Error::Runtime, owner));
        };
        let RepositoryWriteToken::Client(client) = token else {
            owner.token = Some(token);
            return Err((Error::InvalidToken, owner));
        };
        let client = match Arc::try_unwrap(client) {
            Ok(client) => client,
            Err(client) => {
                owner.token = Some(RepositoryWriteToken::Client(client));
                return Err((Error::Busy, owner));
            }
        };
        Ok(Self {
            retire: owner._retire,
            qualification: owner._qualification,
            client: Some(client),
            _parent: owner._parent,
        })
    }
}

/// Borrows its external permit: callback unwinds never consume startup ownership.
/// Publication requires the exact outer startup Record arbitration separately.
pub struct PreparedPendingPublication<'a> {
    permit: &'a mut PendingPublication,
}
impl PreparedPendingPublication<'_> {
    /// Release the sole Client OUTSIDE every lock while exact exclusion remains.
    pub fn release_client(&mut self) -> std::result::Result<(), Error> {
        if self.permit.failed || !self.permit.preflighted {
            return Err(Error::Runtime);
        }
        let PublicationState::Unique(resources) = &mut self.permit.state else {
            return Err(Error::Runtime);
        };
        drop(resources.client.take());
        Ok(())
    }
    /// Run inside exact Record final arbitration; no IO, callbacks or destruction.
    /// Success returns retained cleanup; release it only AFTER unlocking Record,
    /// registry and gate. Failure retains closed ownership; after Record unlock,
    /// call `quarantine`, drain the failed owner, then drop its context and permit.
    pub fn commit(&mut self) -> std::result::Result<DeferredPublicationCleanup, Error> {
        if self.permit.failed || !self.permit.preflighted {
            return Err(Error::Runtime);
        }
        let PublicationState::Unique(resources) = &mut self.permit.state else {
            return Err(Error::Runtime);
        };
        if resources.client.is_some() {
            self.permit.failed = true;
            return Err(Error::InvalidToken);
        }
        if let Err(error) = resources.qualification.commit_publication(
            &resources.retire.registration,
            &self.permit.location,
            &mut self.permit.provider,
            &mut resources.retire.armed,
        ) {
            self.permit.failed = true;
            return Err(error.into());
        }
        self.permit.preflighted = false;
        Ok(DeferredPublicationCleanup {
            _state: std::mem::replace(&mut self.permit.state, PublicationState::Spent),
        })
    }
}
/// Retain outside ALL outer Record/registry/gate guards. Contains no provider or
/// successful Client token; terminal retirement was disarmed in the same commit.
#[must_use = "retain cleanup until every outer startup guard has been released"]
pub struct DeferredPublicationCleanup {
    _state: PublicationState,
}

#[cfg(test)]
mod tests;
