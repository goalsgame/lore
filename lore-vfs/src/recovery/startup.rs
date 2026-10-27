// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Existing-only startup keeps transition inspection separate from ordinary
//! admission. Only actual pending-owner anchor selection can finish it.
use std::sync::Arc;

use super::*;
use crate::capture_memory::CaptureLimits;
use crate::capture_memory::CaptureMemory;
use crate::working_storage::WorkingStorage;

pub enum StartupRecovery {
    Ready(Recovery),
    NeedsReconciliation(RecoveryReconciliation),
}
pub struct RecoveryReconciliation {
    inspection: inspection::RecoveryInspection,
    limits: Limits,
    // Retain the exact backing root until its recovery descriptor/lock drops.
    storage: Arc<WorkingStorage>,
}
impl StartupRecovery {
    pub fn open_existing(storage: Arc<WorkingStorage>, limits: Limits) -> Result<Self> {
        storage.verify_owner().map_err(failure)?;
        match Recovery::open_existing(storage.recovery_directory(), limits) {
            Ok(recovery) => Ok(Self::Ready(recovery)),
            Err(Error::TransitionPending) => {
                let inspection = inspection::RecoveryInspection::open_existing(
                    storage.recovery_directory(),
                    limits,
                )?;
                storage.verify_owner().map_err(failure)?;
                Ok(Self::NeedsReconciliation(RecoveryReconciliation {
                    inspection,
                    limits,
                    storage,
                }))
            }
            Err(error) => Err(error),
        }
    }
}
impl RecoveryReconciliation {
    pub fn verify_owner(&self) -> Result<()> {
        self.storage.verify_owner().map_err(failure)?;
        let current = std::fs::symlink_metadata(self.storage.recovery_directory())?;
        private_metadata(&current, true)?;
        self.inspection.check_directory_identity(&current)
    }
    /// Run inside the actual owned startup worker. Caller cancellation cannot
    /// release the pending payload or backing while its IO is still active.
    pub async fn reconcile(
        self,
        payload: &mut lore_revision::repository::working_bootstrap::OwnerBuildPayload,
    ) -> Result<Recovery> {
        self.verify_owner()?;
        let maximum = self.limits.max_checkpoint_bytes;
        let mut memory = CaptureMemory::new(CaptureLimits {
            max_buffer_bytes: maximum.checked_mul(24).ok_or(Error::ResourceLimit)?,
            max_metadata_nodes: maximum.checked_mul(4).ok_or(Error::ResourceLimit)?,
            max_name_bytes: maximum.checked_mul(4).ok_or(Error::ResourceLimit)?,
        })
        .map_err(|_limit| Error::ResourceLimit)?;
        let owner = Arc::new(self);
        let lease = lore_io::lease::IoLease::new(owner.clone());
        let plan = lease
            .scope(
                owner
                    .inspection
                    .prepare_reconciliation(&owner.storage, payload, &mut memory),
            )
            .await?;
        drop(lease);
        let owner = Arc::try_unwrap(owner).map_err(|_retained| Error::Busy)?;
        owner
            .inspection
            .finish_reconciliation(&owner.storage, plan, &mut memory)
    }
}
fn failure(error: impl std::fmt::Display) -> Error {
    Error::Io(std::io::Error::other(error.to_string()))
}
