// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Store setup preserves the exact pending owner through actual IO and its result.
use super::*;

/// Unpublished stores retain pending exclusion until terminal cleanup. No store,
/// repository, provider-publication or successful-settlement capability escapes.
pub struct PendingWorkingContext {
    pub(super) context: PreparedWorkingContext,
    pub(super) _owner: Arc<PendingIoOwner>,
}
impl PendingWorkingContext {
    pub fn location(&self) -> &WorkingLocation {
        &self.context.location
    }
    pub fn selection(&self) -> &Selection {
        &self.context.selection
    }
}
impl StoreInitializer {
    /// Consumes qualified metadata without exposing its generic store input.
    /// Recovery admission remains a separate requirement for a mounted owner.
    pub async fn prepare_pending(
        &self,
        pending: PendingMetadata,
        mode: StoreMode,
    ) -> std::result::Result<PendingWorkingContext, PendingError> {
        self.prepare_pending_with_binding(pending, mode, None).await
    }
    /// Checks the on-disk binding against the caller's independently admitted
    /// backing root before opening stores. The path is an expected identity,
    /// not `WorkingStorage` or recovery ownership. Only local stores with an
    /// offline or absent remote are supported by this bound path.
    pub async fn prepare_pending_bound(
        &self,
        pending: PendingMetadata,
        mode: StoreMode,
        expected_backing_root: std::path::PathBuf,
    ) -> std::result::Result<PendingWorkingContext, PendingError> {
        self.prepare_pending_with_binding(pending, mode, Some(expected_backing_root))
            .await
    }
    async fn prepare_pending_with_binding(
        &self,
        pending: PendingMetadata,
        mode: StoreMode,
        expected: Option<std::path::PathBuf>,
    ) -> std::result::Result<PendingWorkingContext, PendingError> {
        let PendingMetadata {
            metadata,
            _owner: owner,
        } = pending;
        // PreparedMetadata's inherited parent is this same owner. StoreLease
        // retains it alongside the current caller parent through actual jobs.
        // Keep our owner across the await and move it directly into the result.
        let result = match expected {
            Some(root) => Box::pin(self.prepare_bound(metadata, mode, root)).await,
            None => Box::pin(self.prepare(metadata, mode)).await,
        };
        match result {
            Ok(context) => Ok(PendingWorkingContext {
                context,
                _owner: owner,
            }),
            Err(error) => Err(PendingError::Retired(error)),
        }
    }
}
