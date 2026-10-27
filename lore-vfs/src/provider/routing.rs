// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Direct metadata requests retain one exact frozen source routing and policy.

use super::*;
use crate::namespace::operation_routing::OperationRouting;

impl WorkingProvider {
    pub(super) async fn capture_routing(
        &self,
    ) -> Result<(FrozenNamespace, OperationRouting), FsError> {
        if let Some(owner) = &self.checkpoint {
            owner.available().map_err(checkpoint_error)?;
        }
        let lease = self
            .inner
            .namespace
            .freeze_operation()
            .await
            .map_err(namespace_error)?;
        if self
            .checkpoint
            .as_ref()
            .is_some_and(|owner| !Arc::ptr_eq(owner.namespace(), lease.namespace()))
        {
            return Err(FsError::internal(
                "checkpoint owner belongs to another namespace",
            ));
        }
        let routing = self
            .inner
            .namespace
            .capture_operation_routing(&lease)
            .await
            .map_err(namespace_error)?;
        Ok((lease, routing))
    }
}
impl OperationState {
    /// `authority` is the already resolved original/source key. Committed proof
    /// keys are validated by the backend, never passed to the generic gate.
    pub(super) async fn source_metadata<T: Send>(
        &self,
        authority: &NodeKey,
        label: &'static str,
        future: impl std::future::Future<Output = crate::core::Result<T>> + Send,
    ) -> Result<T, FsError> {
        self.active()?;
        self.routing
            .authorize_source(&self.owner.namespace, self.token, authority)
            .await
            .map_err(namespace_error)?;
        let value = future
            .await
            .map_err(|error| FsError::internal(format!("{label}: {error:?}")))?;
        self.routing
            .authorize_source(&self.owner.namespace, self.token, authority)
            .await
            .map_err(namespace_error)?;
        self.active()?;
        Ok(value)
    }
}
