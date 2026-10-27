// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Contextual handleless reads through an exact opaque file proof. Standalone
//! overlay entry points retain their original default routing.

use super::*;
use crate::namespace::unpublished::FileReadAccess;

impl OverlayFile {
    pub(crate) async fn metadata_bound(&self, access: &FileReadAccess) -> Result<Metadata> {
        access.check_file(self)?;
        access.authorize().await?;
        let state = self.state.lock().await;
        access.authorize().await?;
        let metadata = state.metadata;
        access.authorize().await?;
        Ok(metadata)
    }
    pub(crate) async fn digest_bound(&self, access: &FileReadAccess) -> Result<Option<[u8; 32]>> {
        access.check_file(self)?;
        access.authorize().await?;
        let state = self.state.lock().await;
        access.authorize().await?;
        let digest = if let Some(receipt) = receipt_lock(&self.receipt)?.as_ref() {
            receipt.validate(
                access.inode(),
                self.node.key,
                state.metadata.generation,
                state.metadata.size,
                state.metadata.mode,
            )?;
            Some(receipt.sha256)
        } else {
            None
        };
        let digest = match digest {
            Some(value) => value,
            None if state.metadata.mutable => None,
            None => access.backend().digest(&access.key()).await?,
        };
        access.authorize().await?;
        Ok(digest)
    }
    pub(crate) async fn read_frozen_bound(
        &self,
        token: u64,
        access: &FileReadAccess,
        offset: u64,
        size: usize,
    ) -> Result<Vec<u8>> {
        if size > MAX_READ_SIZE {
            return Err(Error::InvalidInput);
        }
        access.check_file(self)?;
        access.authorize().await?;
        let state = self.state.lock().await;
        if !self.operation_freeze.matches(token) {
            return Err(Error::Stale);
        }
        let route = IoRoute {
            backend: access.backend(),
            gate: access.gate(),
            key: access.key(),
        };
        let bytes = self.read_locked(&state, &route, offset, size).await?;
        if !self.operation_freeze.matches(token) {
            return Err(Error::Stale);
        }
        Ok(bytes)
    }
}
