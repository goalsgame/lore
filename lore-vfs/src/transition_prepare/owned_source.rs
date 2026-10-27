// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! A consumed preparation's source image. These facts grant no publication.
use super::*;
use crate::capture_memory::Footprint;
use crate::namespace::MetadataWitness;
use crate::namespace::operation_routing::OperationRouting;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SourceSelection {
    pub(crate) scope: NodeKey,
    pub(crate) target: ScopeHead,
}
pub(crate) struct SourceFacts {
    freeze: FrozenNamespace,
    routing: OperationRouting,
    epoch: u64,
    generation: u64,
    next_inode: u64,
    original_root: crate::core::Node,
    selection: SourceSelection,
    intent: IntentSpec,
    footprint: Footprint,
}
pub(crate) struct OwnedSourceImage {
    snapshot: NamespaceSnapshot,
    facts: SourceFacts,
}
impl PreparedTransition {
    pub(crate) fn into_owned_source(self) -> Result<OwnedSourceImage> {
        let footprint = self.capture_footprint()?;
        let selection = SourceSelection {
            scope: self.intent.scope,
            target: self.intent.target.clone(),
        };
        let facts = SourceFacts {
            freeze: self.freeze,
            routing: self.source_routing,
            epoch: self.source_epoch,
            generation: self.snapshot.generation,
            next_inode: self.snapshot.next_inode,
            original_root: self.snapshot.original_root.clone(),
            selection,
            intent: self.intent,
            footprint,
        };
        Ok(OwnedSourceImage {
            snapshot: self.snapshot,
            facts,
        })
    }
}
impl SourceFacts {
    pub(crate) fn original_root(&self) -> &crate::core::Node {
        &self.original_root
    }
    pub(crate) fn freeze(&self) -> &FrozenNamespace {
        &self.freeze
    }
    pub(crate) fn routing(&self) -> &OperationRouting {
        &self.routing
    }
    pub(crate) fn selection(&self) -> &SourceSelection {
        &self.selection
    }
    pub(crate) fn intent(&self) -> &IntentSpec {
        &self.intent
    }
    pub(crate) fn footprint(&self) -> Footprint {
        self.footprint
    }
    pub(crate) fn witness(&self) -> MetadataWitness {
        MetadataWitness {
            epoch: self.epoch,
            generation: self.generation,
            next_inode: self.next_inode,
        }
    }
}
impl OwnedSourceImage {
    pub(crate) fn into_parts(self) -> (NamespaceSnapshot, SourceFacts) {
        (self.snapshot, self.facts)
    }
    pub(crate) fn facts(&self) -> &SourceFacts {
        &self.facts
    }
    pub(crate) fn snapshot(&self) -> &NamespaceSnapshot {
        &self.snapshot
    }
    pub(crate) fn original_root(&self) -> &crate::core::Node {
        self.facts.original_root()
    }
    pub(crate) fn freeze(&self) -> &FrozenNamespace {
        self.facts.freeze()
    }
    pub(crate) fn routing(&self) -> &OperationRouting {
        self.facts.routing()
    }
    pub(crate) fn intent(&self) -> &IntentSpec {
        self.facts.intent()
    }
    pub(crate) fn witness(&self) -> MetadataWitness {
        self.facts.witness()
    }
}
#[cfg(test)]
impl PreparedTransition {
    pub(crate) fn test_source_witness(&self) -> (u64, u64, u64, u64) {
        (
            self.freeze.token(),
            self.source_epoch,
            self.snapshot.generation,
            self.snapshot.next_inode,
        )
    }
    pub(crate) fn test_source_buffers(&self) -> Vec<usize> {
        self.snapshot
            .files
            .values()
            .flat_map(|file| {
                file.extents
                    .iter()
                    .map(|extent| extent.data.as_ptr() as usize)
            })
            .collect()
    }
}
