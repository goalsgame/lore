// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Per-handle immutable IO routing. A route retains no overlay, namespace or
//! operation freeze; releasing the handle drops its policy and backend references.

use super::*;
use crate::namespace::file_access::FileAccess;

pub(super) struct IoRoute {
    pub(super) backend: Arc<dyn Backend>,
    pub(super) gate: Arc<dyn AccessGate>,
    pub(super) key: NodeKey,
}
pub(super) struct OpenHandle {
    pub(super) access: Access,
    pub(super) route: Arc<IoRoute>,
}
impl OverlayFile {
    pub(super) fn default_route(&self) -> Arc<IoRoute> {
        Arc::new(IoRoute {
            backend: self.backend.clone(),
            gate: self.gate.clone(),
            key: self.node.key,
        })
    }
    pub(super) async fn open_default_route(&self, access: Access) -> Result<u64> {
        self.open_route(access, self.default_route()).await
    }
    pub(crate) async fn open_captured(&self, captured: &FileAccess, access: Access) -> Result<u64> {
        captured.check_file(self)?;
        self.open_route(
            access,
            Arc::new(IoRoute {
                backend: captured.backend(),
                gate: captured.gate(),
                key: captured.key(),
            }),
        )
        .await
    }
    async fn open_route(&self, access: Access, route: Arc<IoRoute>) -> Result<u64> {
        let mut state = self.state.lock().await;
        if access == Access::Write && self.operation_freeze.frozen() {
            return Err(Error::Busy);
        }
        route.gate.authorize(&route.key, access).await?;
        // Admission can close while policy IO is pending. Do not invalidate a
        // receipt or increment writer count after that asynchronous boundary.
        if access == Access::Write && self.operation_freeze.frozen() {
            return Err(Error::Busy);
        }
        if state.handles.len() >= self.limits.max_handles {
            return Err(Error::ResourceLimit);
        }
        let handle = state.next_handle;
        let next = handle.checked_add(1).ok_or(Error::ResourceLimit)?;
        if access == Access::Write {
            make_mutable(&mut state, &self.receipt, &self.mutation_gate)?;
            state.metadata.writers += 1;
        }
        state.next_handle = next;
        state.handles.insert(handle, OpenHandle { access, route });
        Ok(handle)
    }
    pub(crate) async fn metadata_for_handle(&self, handle: u64) -> Result<Metadata> {
        let state = self.state.lock().await;
        let opened = state.handles.get(&handle).ok_or(Error::BadHandle)?;
        opened
            .route
            .gate
            .authorize(&opened.route.key, Access::Read)
            .await?;
        Ok(state.metadata)
    }
}
