// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Budgeted source read routes selected under a frozen metadata witness. No
//! private bytes are copied, and no raw policy can be supplied by a caller.
use super::*;
use crate::capture_memory::CaptureMemory;
use crate::transition_prepare::OwnedSourceImage;

struct Authority {
    key: NodeKey,
    binding: Arc<NamespaceBinding>,
}
pub(crate) struct SourceAuthorities {
    routes: Vec<Authority>,
    binding: Arc<NamespaceBinding>,
    freeze: FrozenNamespace,
    witness: MetadataWitness,
}
impl SourceAuthorities {
    pub(crate) async fn authorize(&self) -> crate::core::Result<()> {
        for route in &self.routes {
            route
                .binding
                .gate
                .authorize(&route.key, Access::Read)
                .await?;
        }
        Ok(())
    }
}
impl Namespace {
    pub(crate) async fn capture_source_authorities(
        self: &Arc<Self>,
        source: &OwnedSourceImage,
        memory: &mut CaptureMemory,
    ) -> crate::transition_prepare::Result<SourceAuthorities> {
        if !Arc::ptr_eq(self, source.freeze().namespace()) || !source.freeze().valid() {
            return Err(Error::Stale.into());
        }
        let state = self.state.lock().await;
        let binding = self.current_binding_locked(&state)?;
        source.routing().check(self, source.freeze().token())?;
        let witness = source.witness();
        let snapshot = source.snapshot();
        if self.mutation_epoch()? != witness.epoch
            || state.generation != witness.generation
            || state.next_inode != witness.next_inode
            || state.nodes.len() != snapshot.nodes.len()
            || state.active_underlays != snapshot.active_underlays
            || state.baselines != snapshot.baselines
        {
            return Err(Error::Stale.into());
        }
        let count = state
            .nodes
            .len()
            .checked_add(state.active_underlays.len())
            .and_then(|count| count.checked_add(state.baselines.len()))
            .ok_or(Error::ResourceLimit)?;
        let mut routes = memory.vector(count)?;
        for (record, saved) in state.nodes.values().zip(&snapshot.nodes) {
            memory.nodes(1)?;
            if record.entry != saved.entry
                || record.attached != saved.attached
                || record.names.as_ref().map(BTreeMap::len) != saved.names.as_ref().map(Vec::len)
                || record
                    .names
                    .as_ref()
                    .zip(saved.names.as_ref())
                    .is_some_and(|(live, saved)| !live.values().eq(saved.iter()))
                || record.file.is_some() != snapshot.files.contains_key(&record.entry.inode)
            {
                return Err(Error::Stale.into());
            }
            let receipt = receipt_lock(&record.receipt)?;
            let expected = snapshot
                .files
                .get(&record.entry.inode)
                .map_or(saved.receipt.as_ref(), |file| file.receipt.as_ref());
            if receipt.as_ref() != expected {
                return Err(Error::Stale.into());
            }
            routes.push(Authority {
                key: record.entry.authority,
                binding: Self::record_binding(record, &binding)?,
            });
        }
        for scope in state.active_underlays.keys().chain(state.baselines.keys()) {
            memory.nodes(1)?;
            routes.push(Authority {
                key: *scope,
                binding: binding.clone(),
            });
        }
        Ok(SourceAuthorities {
            routes,
            binding,
            freeze: source.freeze().pin(),
            witness,
        })
    }
    pub(crate) async fn seal_source_authorities(
        self: &Arc<Self>,
        source: &OwnedSourceImage,
        authorities: &SourceAuthorities,
    ) -> crate::transition_prepare::Result<MetadataSeal> {
        let state = self.state.lock().await;
        self.confirms_binding(&authorities.binding)?;
        source.routing().check(self, source.freeze().token())?;
        if !authorities.freeze.same_acquisition(source.freeze())
            || !authorities.freeze.valid()
            || !Arc::ptr_eq(self, source.freeze().namespace())
            || state.generation != authorities.witness.generation
            || state.next_inode != authorities.witness.next_inode
            || self.mutation_epoch()? != authorities.witness.epoch
        {
            return Err(Error::Stale.into());
        }
        let seal = self.mutation_gate.seal(authorities.witness.epoch)?;
        Ok(MetadataSeal {
            _seal: seal,
            _freeze: source.freeze().pin(),
        })
    }
}
