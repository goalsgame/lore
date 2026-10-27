// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Owned bookkeeping; no private byte images or publication rights.
use super::*;

pub(crate) struct OwnedLayout {
    binding: PreparedPolicyBinding,
    parts: OwnedLayoutParts,
}
pub(crate) struct OwnedNode {
    pub(crate) entry: Entry,
    pub(crate) names: Option<Vec<(Vec<u8>, u64)>>,
}
pub(crate) struct OwnedLayoutParts {
    pub(crate) issuer: crate::publication_ownership::successor::PreparedIssuer,
    pub(crate) source_footprint: crate::capture_memory::Footprint,
    pub(crate) freeze: crate::namespace::FrozenNamespace,
    pub(crate) epoch: u64,
    pub(crate) generation: u64,
    pub(crate) source_next: u64,
    pub(crate) next: u64,
    pub(crate) next_generation: u64,
    pub(crate) fresh: BTreeMap<u64, OwnedNode>,
    pub(crate) retire: Vec<u64>,
    pub(crate) children: Vec<ChildReparent>,
    pub(crate) parent: Option<(u64, Vec<u8>, u64, u64)>,
    pub(crate) projections: Vec<(HeldBaseline, ScopeProjection)>,
    pub(crate) scope: NodeKey,
    pub(crate) target: ScopeHead,
    pub(crate) expected: Option<HeldBaseline>,
    pub(crate) provenance: Vec<u8>,
}
impl PreparedNamespaceLayout<'_> {
    pub(crate) fn into_owned(self, budget: &mut Budget) -> Result<OwnedLayout> {
        self.check_namespace(self.source_namespace())?;
        budget.node()?;
        let issuer = self
            .source_namespace()
            .prepare_successor_issuer(self.binding.prepared())?;
        // Metadata borrowed from the plan must be independently bounded while
        // the plan and its snapshots remain alive.
        let parent = self
            .parent_patch
            .map(|patch| {
                budget.names(patch.spelling.len())?;
                Ok::<_, core::Error>((
                    patch.parent,
                    patch.spelling.to_vec(),
                    patch.old_child,
                    patch.new_child,
                ))
            })
            .transpose()?;
        let mut projections = Vec::new();
        for projection in self.projections {
            budget.node()?;
            projections.push((projection.expected.clone(), projection.next));
        }
        budget.paths(self.binding.prepared().provenance().len())?;
        let provenance = self.binding.prepared().provenance().to_vec();
        let mut fresh = BTreeMap::new();
        for (inode, node) in self.fresh {
            // Move names and entries, discarding borrowed labels/image IDs.
            budget.node()?;
            fresh.insert(
                inode,
                OwnedNode {
                    entry: node.entry,
                    names: node.names,
                },
            );
        }
        let parts = OwnedLayoutParts {
            issuer,
            source_footprint: self.source.capture_footprint()?,
            freeze: self.source.freeze.pin(),
            epoch: self.source_epoch,
            generation: self.source_generation,
            source_next: self.source_next_inode,
            next: self.next_inode,
            next_generation: self.next_generation,
            fresh,
            retire: self.retire,
            children: self.children,
            parent,
            projections,
            scope: self.source.intent.scope,
            target: self.source.intent.target.clone(),
            expected: self.source.intent.expected.clone(),
            provenance,
        };
        Ok(OwnedLayout {
            binding: self.binding,
            parts,
        })
    }
}

impl OwnedLayout {
    pub(crate) fn into_validated(
        self,
        namespace: &Arc<crate::namespace::Namespace>,
        view: &crate::namespace::prepared_view::PreparedEffectiveView,
    ) -> Result<OwnedLayoutParts> {
        let source = namespace.backend()?;
        let source = source
            .as_any()
            .and_then(|backend| backend.downcast_ref::<RevisionBackend>())
            .ok_or(Error::InvalidScope)?;
        let prepared = self.binding.prepared();
        let parts = &self.parts;
        if !Arc::ptr_eq(namespace, parts.freeze.namespace())
            || !parts.freeze.valid()
            || !prepared.matches_source(source)
            || prepared.source_epoch() != parts.epoch
            || prepared.scope() != parts.scope
            || prepared.head() != &parts.target
            || prepared.provenance() != parts.provenance
            || !view.matches_binding(&self.binding)
        {
            return Err(Error::InvalidScope);
        }
        Ok(self.parts)
    }
}
