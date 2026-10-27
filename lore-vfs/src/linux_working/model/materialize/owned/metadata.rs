// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Preallocated Model bookkeeping. Handle tables and counters remain live and
//! are never copied into a candidate. These witnesses grant no installation.

use super::*;
use crate::capture_memory::CaptureMemory;
use crate::transition_prepare::Budget;

pub(super) struct ModelMetadataWitness {
    source_generation: u64,
    next_generation: u64,
}
pub(super) struct PreparedModelMetadata {
    unlinked: BTreeSet<u64>,
    witness: ModelMetadataWitness,
}
impl Model {
    // The owned conversion holds `names` from this capture through its final
    // witness check. This waits out any admitted mutation's post-Namespace tail.
    pub(in crate::linux_working::model::materialize::owned) fn model_metadata_witness(
        &self,
    ) -> PreparedResult<ModelMetadataWitness> {
        let _state = self.state.lock().map_err(|_poison| Error::Poisoned)?;
        let source_generation = self.directory_generation.load(Ordering::Acquire);
        if source_generation == 0 {
            return Err(core::Error::ResourceLimit.into());
        }
        Ok(ModelMetadataWitness {
            source_generation,
            next_generation: self.next_directory_generation()?,
        })
    }
}
impl PreparedModelMetadata {
    pub(super) fn prepare(
        owner: &Model,
        state: &UnpublishedState,
        witness: ModelMetadataWitness,
        budget: &mut Budget,
    ) -> PreparedResult<Self> {
        let metadata = Self {
            unlinked: state.prepare_model_unlinked(&owner.namespace, budget)?,
            witness,
        };
        metadata.check_source(owner)?;
        Ok(metadata)
    }
    // A numeric witness only: the candidate separately checks its exact Model,
    // Namespace, freeze, routing and metadata epoch. Callers retain `names`.
    pub(super) fn check_source(&self, owner: &Model) -> PreparedResult<()> {
        let _state = owner.state.lock().map_err(|_poison| Error::Poisoned)?;
        if owner.directory_generation.load(Ordering::Acquire) != self.witness.source_generation {
            return Err(core::Error::Stale.into());
        }
        Ok(())
    }
    // The caller already holds Model state; never reacquire its mutex here.
    pub(super) fn check_locked(&self, owner: &Model) -> PreparedResult<()> {
        if owner.directory_generation.load(Ordering::Acquire) != self.witness.source_generation {
            return Err(core::Error::Stale.into());
        }
        Ok(())
    }
    pub(super) fn commit_locked(&mut self, owner: &Model, unlinked: &mut BTreeSet<u64>) {
        std::mem::swap(unlinked, &mut self.unlinked);
        owner
            .directory_generation
            .store(self.witness.next_generation, Ordering::Release);
    }
    pub(super) fn retain_capture_inputs(&self, memory: &mut CaptureMemory) -> core::Result<()> {
        memory.nodes(self.unlinked.len())
    }
}

#[cfg(test)]
impl UnpublishedCandidate {
    pub(crate) fn test_model_unlinked(&self) -> &BTreeSet<u64> {
        &self.metadata.unlinked
    }
    pub(crate) fn test_model_generations(&self) -> (u64, u64) {
        (
            self.metadata.witness.source_generation,
            self.metadata.witness.next_generation,
        )
    }
    pub(crate) fn test_check_model_metadata(&self) -> PreparedResult<()> {
        self.metadata.check_source(&self.admission.owner)
    }
    pub(crate) fn test_retain_model_metadata(
        &self,
        memory: &mut CaptureMemory,
    ) -> core::Result<()> {
        self.metadata.retain_capture_inputs(memory)
    }
}
