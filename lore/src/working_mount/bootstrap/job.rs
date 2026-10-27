// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Actual bootstrap jobs retain recovery, the sole Client and the same record.
//! This owner never stores metadata, a publication permit, Input, or Model.

use std::path::Path;
use std::sync::Arc;
use std::sync::Mutex;

use lore_io::lease::IoLease;
use lore_revision::fs::provider_registry::Registration;
use lore_revision::fs::provider_registry::WorkingLocation;
use lore_revision::repository::RepositoryWriteToken;
use lore_revision::repository::working_bootstrap::PendingMetadata;
use lore_vfs::working_storage::WorkingStorage;

use super::admission::RecoveryAdmission;
use super::binding::Binding;
use crate::working_mount::Error;
use crate::working_mount::OwnerLimits;
use crate::working_mount::Result;
use crate::working_mount::reservation::BootstrapReservation;

pub(super) enum RecoveryResource {
    Ready(RecoveryAdmission),
    Reconcile(lore_vfs::recovery::RecoveryReconciliation),
}
pub(super) struct AdmittedBacking {
    pub(super) recovery: RecoveryResource,
    pub(super) storage: Arc<WorkingStorage>,
}
pub(super) struct Job {
    backing: Mutex<Option<AdmittedBacking>>,
    token: Mutex<Option<RepositoryWriteToken>>,
    ticket: Mutex<Option<BootstrapReservation>>,
    registration: Arc<Registration>,
    location: WorkingLocation,
    // Actual resources and the startup ticket settle before caller accounting.
    _caller: Option<IoLease>,
}
impl Job {
    pub(super) fn new(
        ticket: BootstrapReservation,
        token: RepositoryWriteToken,
        registration: Arc<Registration>,
        location: WorkingLocation,
    ) -> Arc<Self> {
        Arc::new(Self {
            backing: Mutex::new(None),
            token: Mutex::new(Some(token)),
            ticket: Mutex::new(Some(ticket)),
            registration,
            location,
            _caller: IoLease::current(),
        })
    }
    pub(super) fn check_live(&self) -> Result<()> {
        self.ticket
            .lock()
            .map_err(|_poison| Error::Poisoned)?
            .as_ref()
            .ok_or(Error::Closed)?
            .check_live()
    }
    /// Blocking setup has no cache, session, normal store or provider side effect.
    pub(super) fn admit(&self, root: &Path, fresh: bool, limits: OwnerLimits) -> Result<()> {
        self.check_live()?;
        let storage = if fresh {
            WorkingStorage::create_new(
                root,
                self.location.target().path(),
                self.location.repository(),
                self.location.instance(),
                Default::default(),
            )
        } else {
            WorkingStorage::open_existing(
                root,
                self.location.target().path(),
                self.location.repository(),
                self.location.instance(),
                Default::default(),
            )
        }
        .map_err(storage_error)?;
        let recovery = if fresh {
            RecoveryResource::Ready(RecoveryAdmission::open(
                self.registration.clone(),
                self.location.clone(),
                storage.clone(),
                limits.recovery,
                false,
            )?)
        } else {
            match lore_vfs::recovery::StartupRecovery::open_existing(
                storage.clone(),
                limits.recovery,
            )
            .map_err(|error| Error::Owner(format!("recovery startup: {error:?}")))?
            {
                lore_vfs::recovery::StartupRecovery::Ready(recovery) => {
                    RecoveryResource::Ready(RecoveryAdmission::adopt(
                        self.registration.clone(),
                        self.location.clone(),
                        storage.clone(),
                        limits.recovery,
                        recovery,
                    )?)
                }
                lore_vfs::recovery::StartupRecovery::NeedsReconciliation(owner) => {
                    RecoveryResource::Reconcile(owner)
                }
            }
        };
        self.check_live()?;
        let mut slot = self.backing.lock().map_err(|_poison| Error::Poisoned)?;
        if slot.is_some() {
            return Err(Error::InvalidBinding);
        }
        *slot = Some(AdmittedBacking { recovery, storage });
        Ok(())
    }
    pub(super) fn root(&self) -> Result<std::path::PathBuf> {
        let slot = self.backing.lock().map_err(|_poison| Error::Poisoned)?;
        Ok(slot
            .as_ref()
            .ok_or(Error::Closed)?
            .storage
            .root()
            .to_path_buf())
    }
    pub(super) fn take_token(&self) -> Result<RepositoryWriteToken> {
        self.check_live()?;
        if self
            .backing
            .lock()
            .map_err(|_poison| Error::Poisoned)?
            .is_none()
        {
            return Err(Error::InvalidBinding);
        }
        self.token
            .lock()
            .map_err(|_poison| Error::Poisoned)?
            .take()
            .ok_or(Error::Closed)
    }
    pub(super) fn take_backing(&self) -> Result<AdmittedBacking> {
        self.check_live()?;
        self.backing
            .lock()
            .map_err(|_poison| Error::Poisoned)?
            .take()
            .ok_or(Error::Closed)
    }
    pub(super) async fn finish_backing(
        &self,
        payload: &mut lore_revision::repository::working_bootstrap::OwnerBuildPayload,
        limits: OwnerLimits,
    ) -> Result<(RecoveryAdmission, Arc<WorkingStorage>)> {
        let backing = self.take_backing()?;
        let recovery = match backing.recovery {
            RecoveryResource::Ready(recovery) => recovery,
            RecoveryResource::Reconcile(owner) => {
                let recovery = owner
                    .reconcile(payload)
                    .await
                    .map_err(|error| Error::Owner(format!("recovery reconciliation: {error:?}")))?;
                RecoveryAdmission::adopt(
                    self.registration.clone(),
                    self.location.clone(),
                    backing.storage.clone(),
                    limits.recovery,
                    recovery,
                )?
            }
        };
        self.check_live()?;
        Ok((recovery, backing.storage))
    }
    pub(super) fn take_ticket(&self) -> Result<BootstrapReservation> {
        self.check_live()?;
        self.ticket
            .lock()
            .map_err(|_poison| Error::Poisoned)?
            .take()
            .ok_or(Error::Closed)
    }
    pub(super) fn verify_backing(&self) -> Result<()> {
        self.check_live()?;
        let backing = self.backing.lock().map_err(|_poison| Error::Poisoned)?;
        let backing = backing.as_ref().ok_or(Error::Closed)?;
        backing.storage.verify_owner().map_err(storage_error)?;
        match &backing.recovery {
            RecoveryResource::Ready(recovery) => {
                recovery.verify()?;
            }
            RecoveryResource::Reconcile(recovery) => recovery
                .verify_owner()
                .map_err(|error| Error::Owner(format!("recovery inspection: {error:?}")))?,
        }
        Ok(())
    }
    /// The whole opaque metadata result owns pending exclusion through actual
    /// binding IO. Success transfers it unchanged; cancellation cannot shorten it.
    pub(super) fn bind(
        self: &Arc<Self>,
        metadata: PendingMetadata,
        fresh: bool,
    ) -> Result<PendingMetadata> {
        self.check_live()?;
        self.verify_backing()?;
        let root = self.root()?;
        let location = metadata.location();
        if *location != self.location {
            return Err(Error::InvalidBinding);
        }
        let binding = if fresh {
            Binding::write_new(
                location.metadata().path(),
                location.repository(),
                location.instance(),
                &root,
            )
        } else {
            Binding::read(
                location.metadata().path(),
                location.repository(),
                location.instance(),
            )
        }
        .map_err(|_binding| Error::InvalidBinding)?;
        if binding.root() != root {
            return Err(Error::InvalidBinding);
        }
        self.verify_backing()?;
        Ok(metadata)
    }
}
impl Drop for Job {
    fn drop(&mut self) {
        // Withdraw before a pre-qualification Client is released. After promotion
        // the slot is empty; publication ownership has its separate retire guard.
        if let Some(ticket) = self
            .ticket
            .get_mut()
            .unwrap_or_else(|poison| poison.into_inner())
            .as_ref()
        {
            let _retired = ticket.cancel();
        }
    }
}
fn storage_error(error: impl std::fmt::Display) -> Error {
    Error::Storage(error.to_string())
}
