// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Source-bound, read-only target metadata validation. No file payload is read.

use std::collections::BTreeSet;

use super::*;
use crate::overlay::ProvenTargetMetadata;

pub(super) fn validate_metadata(node: &RevisionNode) -> Result<(), Error> {
    if node.is_discarded()
        || node.is_file()
            && (node.size != 0 && node.address.hash.is_zero()
                || node.mode & !NodeFileMode::Executable.bits() != 0)
    {
        return Err(Error::InvalidInput);
    }
    Ok(())
}

impl RevisionBackend {
    /// Validate the current original route and the immutable selection proof.
    /// An absent selection authorizes no node, including its existing ancestor.
    pub async fn revalidate_target_head(&self, proof: &ProvenTargetMetadata) -> Result<(), Error> {
        self.revalidate_selection_source(proof.selection()).await?;
        if proof.max_depth == 0 || proof.max_depth > 1024 {
            return Err(Error::InvalidInput);
        }
        if proof.state.revision() != proof.selection().requested_revision().hash {
            return Err(Error::Stale);
        }
        if let Some(root) = proof.root() {
            let source = proof.selection().original_scope();
            if root.key.repository != source.repository
                || root.key.exposure != source.exposure
                || root.key.revision != proof.selection().requested_revision()
            {
                return Err(Error::PermissionDenied);
            }
        }
        Ok(())
    }
    /// Validate one immutable metadata node, within its preparation depth cap (at most 1,024 steps).
    /// This never grants writes or permits a different linked exposure.
    pub async fn revalidate_target_node(
        &self,
        proof: &ProvenTargetMetadata,
        key: &NodeKey,
        write: bool,
    ) -> Result<(), Error> {
        if write {
            return Err(Error::ReadOnly);
        }
        self.revalidate_target_head(proof).await?;
        let root = proof.root().ok_or(Error::PermissionDenied)?;
        if key.repository != root.key.repository
            || key.revision != root.key.revision
            || key.exposure != root.key.exposure
        {
            return Err(Error::PermissionDenied);
        }
        LORE_CONTEXT
            .scope(self.execution.clone(), async {
                let original = self.owner(&proof.selection().original_scope())?;
                let mut node = proof
                    .state
                    .node(original.repository.clone(), key.node)
                    .await
                    .map_err(state_error)?;
                validate_metadata(&node)?;
                let mut id = key.node;
                let mut seen = BTreeSet::new();
                for depth in 0..=proof.max_depth {
                    if node.is_discarded() || !seen.insert(id) {
                        return Err(Error::Stale);
                    }
                    if id == root.key.node {
                        self.revalidate_target_head(proof).await?;
                        return Ok(());
                    }
                    if id == ROOT_NODE {
                        return Err(Error::PermissionDenied);
                    }
                    if depth == proof.max_depth {
                        return Err(Error::ResourceLimit);
                    }
                    id = node.parent;
                    node = proof
                        .state
                        .node(original.repository.clone(), id)
                        .await
                        .map_err(state_error)?;
                    if !node.is_directory() {
                        return Err(Error::Stale);
                    }
                }
                Err(Error::ResourceLimit)
            })
            .await
    }
}
