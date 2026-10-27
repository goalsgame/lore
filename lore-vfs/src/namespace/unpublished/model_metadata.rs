// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Bounded detached-inode bookkeeping for an unpublished Model image.

use super::*;

impl UnpublishedState {
    pub(crate) fn prepare_model_unlinked(
        &self,
        namespace: &Arc<Namespace>,
        budget: &mut Budget,
    ) -> crate::core::Result<BTreeSet<u64>> {
        if !Arc::ptr_eq(namespace, self.freeze.namespace()) || !self.freeze.valid() {
            return Err(Error::Stale);
        }
        if self.state.nodes.len() > namespace.limits.max_inodes {
            return Err(Error::ResourceLimit);
        }
        let count = self
            .state
            .nodes
            .values()
            .filter(|record| !record.attached)
            .count();
        // Reserve the entire logical allocation before constructing any nodes.
        for _ in 0..count {
            budget.node()?;
        }
        let mut unlinked = BTreeSet::new();
        for (&inode, record) in &self.state.nodes {
            if !record.attached {
                unlinked.insert(inode);
            }
        }
        Ok(unlinked)
    }
}
