// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Owned ordinary recovery resource, not registry bootstrap qualification.
use std::sync::Arc;

use lore_revision::fs::provider_registry::Registration;
use lore_revision::fs::provider_registry::WorkingLocation;
use lore_revision::repository::working_bootstrap::PreparedWorkingContext;
use lore_vfs::core::Node;
use lore_vfs::recovery::Limits;
use lore_vfs::recovery::Recovery;
use lore_vfs::working_storage::WorkingStorage;

use super::super::Error;
use super::super::Result;
use super::super::StartupInput;
use super::super::overlaps;

pub(in crate::working_mount) struct RecoveryAdmission {
    // Close ordinary recovery before releasing its backing-root owner.
    recovery: Recovery,
    storage: Arc<WorkingStorage>,
    registration: Arc<Registration>,
    location: WorkingLocation,
    limit: usize,
    recorded_binding: bool,
}
impl RecoveryAdmission {
    pub(in crate::working_mount) fn open(
        registration: Arc<Registration>,
        location: WorkingLocation,
        storage: Arc<WorkingStorage>,
        limits: Limits,
        existing_only: bool,
    ) -> Result<Self> {
        if registration.bootstrap_location().map_err(registry)? != location
            || storage.repository() != location.repository()
            || storage.instance() != location.instance()
            || overlaps(storage.root(), location.target().path())
            || overlaps(storage.root(), location.metadata().path())
        {
            return Err(Error::InvalidBinding);
        }
        storage.verify_owner().map_err(storage_error)?;
        let recovery = if existing_only {
            Recovery::open_existing(storage.recovery_directory(), limits)
        } else {
            Recovery::open(storage.recovery_directory(), limits)
        }
        .map_err(recovery_error)?;
        let admission = Self {
            recovery,
            storage,
            registration,
            location,
            limit: limits.max_checkpoint_bytes,
            recorded_binding: false,
        };
        admission.verify()?;
        Ok(admission)
    }
    pub(super) fn adopt(
        registration: Arc<Registration>,
        location: WorkingLocation,
        storage: Arc<WorkingStorage>,
        limits: Limits,
        recovery: Recovery,
    ) -> Result<Self> {
        if recovery.directory() != storage.recovery_directory()
            || recovery.byte_limit() != limits.max_checkpoint_bytes
            || registration.bootstrap_location().map_err(registry)? != location
            || storage.repository() != location.repository()
            || storage.instance() != location.instance()
            || overlaps(storage.root(), location.target().path())
            || overlaps(storage.root(), location.metadata().path())
        {
            return Err(Error::InvalidBinding);
        }
        let admission = Self {
            recovery,
            storage,
            registration,
            location,
            limit: limits.max_checkpoint_bytes,
            recorded_binding: false,
        };
        admission.verify()?;
        Ok(admission)
    }
    pub(super) fn verify(&self) -> Result<Option<Node>> {
        self.storage.verify_owner().map_err(storage_error)?;
        self.recovery.verify_owner().map_err(recovery_error)?;
        if self.recorded_binding {
            let binding = super::binding::Binding::read(
                self.location.metadata().path(),
                self.location.repository(),
                self.location.instance(),
            )
            .map_err(|_binding| Error::InvalidBinding)?;
            if binding.root() != self.storage.root() {
                return Err(Error::InvalidBinding);
            }
        }
        let original = self.recovery.original_root().map_err(recovery_error)?;
        if original
            .as_ref()
            .is_some_and(|root| root.key.repository != self.location.repository())
        {
            return Err(Error::InvalidBinding);
        }
        Ok(original)
    }
    pub(in crate::working_mount) fn take(
        self,
        prepared: &PreparedWorkingContext,
        storage: &Arc<WorkingStorage>,
        limits: Limits,
    ) -> Result<(Recovery, Option<Node>)> {
        let original = self.check_prepared(prepared, storage, limits)?;
        Ok((self.recovery, original))
    }
    fn check_prepared(
        &self,
        prepared: &PreparedWorkingContext,
        storage: &Arc<WorkingStorage>,
        limits: Limits,
    ) -> Result<Option<Node>> {
        if limits.max_checkpoint_bytes != self.limit {
            return Err(Error::InvalidLimits);
        }
        if !Arc::ptr_eq(&self.storage, storage)
            || !self.registration.same_reservation(&prepared.registration)
            || self.location != prepared.location
            || prepared.repository.id != self.location.repository()
            || prepared.repository.instance_id != self.location.instance()
            || prepared.repository.try_write_token().is_some()
            || self.registration.bootstrap_location().map_err(registry)? != self.location
        {
            return Err(Error::InvalidBinding);
        }
        self.verify()
    }
}
impl StartupInput {
    pub(in crate::working_mount) fn from_early_admission(
        prepared: PreparedWorkingContext,
        storage: Arc<WorkingStorage>,
        mut admission: RecoveryAdmission,
        limits: Limits,
    ) -> Result<Self> {
        admission.recorded_binding = true;
        admission.check_prepared(&prepared, &storage, limits)?;
        if prepared.execution.caller_state().is_some() {
            return Err(Error::InvalidBinding);
        }
        Ok(Self {
            prepared,
            storage,
            admission: std::sync::Mutex::new(Some(admission)),
        })
    }
    /// Test compatibility for already prepared stores. This acquires real
    /// recovery ownership but does not prove admission preceded store creation.
    #[cfg(feature = "test-util")]
    pub fn test_admit_prepared(
        prepared: PreparedWorkingContext,
        storage: Arc<WorkingStorage>,
        limits: Limits,
    ) -> Result<Self> {
        if prepared.repository.try_write_token().is_some()
            || prepared.execution.caller_state().is_some()
        {
            return Err(Error::InvalidBinding);
        }
        let admission = RecoveryAdmission::open(
            prepared.registration.clone(),
            prepared.location.clone(),
            storage.clone(),
            limits,
            false,
        )?;
        Ok(Self {
            prepared,
            storage,
            admission: std::sync::Mutex::new(Some(admission)),
        })
    }
    pub(in crate::working_mount) fn take_recovery(
        &self,
        limits: Limits,
    ) -> Result<(Recovery, Option<Node>)> {
        // Consume the only resource before any fallible verification. A failed
        // claim remains consumed; setup cannot repair or reopen it on retry.
        let admission = {
            let mut pending = self.admission.lock().map_err(|_poison| Error::Poisoned)?;
            pending.take().ok_or(Error::Closed)?
        };
        admission.take(&self.prepared, &self.storage, limits)
    }
    #[cfg(feature = "test-util")]
    pub fn test_claim_recovery(&self, limits: Limits) -> Result<()> {
        let resource = self.take_recovery(limits)?;
        drop(resource);
        Ok(())
    }
}
fn registry(error: impl std::fmt::Display) -> Error {
    Error::Registry(error.to_string())
}
fn storage_error(error: impl std::fmt::Display) -> Error {
    Error::Storage(error.to_string())
}
fn recovery_error(error: impl std::fmt::Debug) -> Error {
    Error::Owner(format!("recovery admission: {error:?}"))
}
