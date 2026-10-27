// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Read-only layout access for unpublished resource preparation.

use super::*;
use crate::namespace::Namespace;
use crate::overlay::FileSnapshot;

pub(crate) struct PrivateFileSpec<'image> {
    pub(crate) new_inode: u64,
    pub(crate) source_inode: u64,
    pub(crate) image: &'image FileSnapshot,
}

impl PreparedNamespaceLayout<'_> {
    pub(crate) fn source_namespace(&self) -> &Arc<Namespace> {
        self.source.freeze.namespace()
    }
    pub(crate) fn prepared_binding(&self) -> &PreparedPolicyBinding {
        &self.binding
    }
    pub(crate) fn source_underlays(&self) -> &BTreeMap<NodeKey, ScopeHead> {
        &self.source.snapshot.active_underlays
    }
    pub(crate) fn check_namespace(&self, namespace: &Arc<Namespace>) -> Result<()> {
        if !Arc::ptr_eq(namespace, self.source_namespace())
            || !self.source.freeze.valid()
            || self.source_generation != self.source.snapshot.generation
            || self.source_next_inode != self.source.snapshot.next_inode
            || self.source_epoch != self.source.source_epoch
        {
            return Err(Error::InvalidScope);
        }
        let source = namespace.backend()?;
        let source = source
            .as_any()
            .and_then(|backend| backend.downcast_ref::<RevisionBackend>())
            .ok_or(Error::InvalidScope)?;
        let prepared = self.binding.prepared();
        if !prepared.matches_source(source)
            || prepared.scope() != self.source.intent.scope
            || prepared.head() != &self.source.intent.target
            || prepared.original_root() != &self.source.snapshot.original_root
            || prepared.source_epoch() != self.source_epoch
        {
            return Err(Error::InvalidScope);
        }
        if namespace.mutation_epoch()? != self.source_epoch {
            return Err(Error::StaleSource);
        }
        Ok(())
    }
    pub(crate) async fn validate_source(&self) -> Result<()> {
        self.check_namespace(self.source_namespace())?;
        self.source.validate_source().await
    }
    pub(crate) async fn authorize_materialization(&self) -> Result<()> {
        self.check_namespace(self.source_namespace())?;
        self.source.authorize_candidate_edits().await?;
        self.check_namespace(self.source_namespace())
    }
    pub(crate) fn private_files(&self) -> impl Iterator<Item = core::Result<PrivateFileSpec<'_>>> {
        self.fresh.iter().filter_map(|(&new_inode, node)| {
            let FileLayout::Private {
                source_inode,
                generation,
                base_end,
                receipt_source,
            } = node.file.as_ref()?
            else {
                return None;
            };
            Some((|| {
                let image = self
                    .source
                    .snapshot
                    .files
                    .get(source_inode)
                    .ok_or(core::Error::InvalidInput)?;
                if new_inode != node.entry.inode
                    || new_inode < self.source_next_inode
                    || new_inode >= self.next_inode
                    || new_inode == *source_inode
                    || *source_inode == 0
                    || node.entry.kind != NodeKind::File
                    || image.node.key != node.entry.authority
                    || !image.metadata.mutable
                    || image.metadata.generation != *generation
                    || image.base_end != *base_end
                    || image.receipt.as_ref().map(|receipt| receipt.inode) != *receipt_source
                    || node
                        .entry
                        .base
                        .as_ref()
                        .is_some_and(|base| *base != image.node)
                    || node.entry.base.is_none() && image.base_end != 0
                {
                    return Err(core::Error::InvalidInput);
                }
                Ok(PrivateFileSpec {
                    new_inode,
                    source_inode: *source_inode,
                    image,
                })
            })())
        })
    }
    pub(crate) fn resource_budget(&self) -> Result<Budget> {
        let mut budget = Budget::new(self.source.limits)?;
        reserve_source(
            self.source,
            self.binding.prepared().provenance().len(),
            &mut budget,
        )?;
        for node in self.fresh.values() {
            budget.node()?;
            budget.paths(node.path.len())?;
            if node.child_binding.is_some() {
                budget.node()?;
            }
            if let Some(names) = &node.names {
                for (name, _) in names {
                    budget.node()?;
                    budget.names(name.len())?;
                }
            }
        }
        for _ in &self.retire {
            budget.node()?;
        }
        for _ in &self.children {
            budget.node()?;
        }
        for _ in &self.projections {
            budget.node()?;
        }
        Ok(budget)
    }
    pub(crate) fn source_private_bytes(&self) -> core::Result<u64> {
        self.source
            .snapshot
            .files
            .values()
            .try_fold(0u64, |sum, image| {
                image.extents.iter().try_fold(sum, |sum, extent| {
                    sum.checked_add(
                        u64::try_from(extent.data.len())
                            .map_err(|_length| core::Error::ResourceLimit)?,
                    )
                    .ok_or(core::Error::ResourceLimit)
                })
            })
    }
}
