// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Contextual capture of the opaque candidate; no live namespace mutation.
use super::*;
use crate::capture_memory::CaptureMemory;

impl UnpublishedState {
    pub(crate) async fn validate_source(&self) -> crate::transition_prepare::Result<()> {
        let namespace = self.freeze.namespace();
        let state = namespace.state.lock().await;
        namespace.confirms_binding(&self.source_binding)?;
        if !self.freeze.valid()
            || namespace.mutation_epoch()? != self.epoch
            || state.generation != self.generation
            || state.next_inode != self.source_next
        {
            return Err(crate::transition_prepare::Error::StaleSource);
        }
        Ok(())
    }
    pub(crate) fn retain_capture_inputs(
        &self,
        memory: &mut CaptureMemory,
    ) -> crate::core::Result<()> {
        memory.retain(self.source_footprint)?;
        memory.buffer(self.provenance.capacity())?;
        memory.nodes(self.state.backend_ids.len())?;
        memory.nodes(self.state.active_underlays.len())?;
        memory.nodes(self.state.baselines.len())?;
        for record in self.state.nodes.values() {
            memory.nodes(1)?;
            if record.file.is_some() {
                memory.nodes(1)?;
            }
            if let Some(names) = &record.names {
                for (folded, (spelling, _)) in names {
                    memory.nodes(1)?;
                    memory.buffer(folded.capacity())?;
                    memory.buffer(spelling.capacity())?;
                    memory.names(
                        folded
                            .len()
                            .checked_add(spelling.len())
                            .ok_or(Error::ResourceLimit)?,
                    )?;
                }
            }
        }
        Ok(())
    }
    async fn authorize_capture(&self) -> crate::core::Result<()> {
        for scope in self.state.active_underlays.keys() {
            self.binding.gate.authorize(scope, Access::Read).await?;
        }
        for record in self.state.nodes.values() {
            let binding = if record.attached {
                &self.binding
            } else {
                record.retired_binding.as_ref().ok_or(Error::Stale)?
            };
            binding
                .gate
                .authorize(&record.entry.authority, Access::Read)
                .await?;
        }
        Ok(())
    }
    pub(crate) async fn capture_snapshot(
        &self,
        memory: &mut CaptureMemory,
        max_data: usize,
        file_limits: FileLimits,
    ) -> crate::transition_prepare::Result<NamespaceSnapshot> {
        self.validate_source().await?;
        self.authorize_capture().await?;
        let mut nodes = memory.vector(self.state.nodes.len())?;
        for record in self.state.nodes.values() {
            memory.nodes(1)?;
            let names = if let Some(names) = &record.names {
                let mut output = memory.vector(names.len())?;
                for (name, inode) in names.values() {
                    memory.nodes(1)?;
                    memory.names(name.len())?;
                    let mut copy = memory.vector(name.len())?;
                    copy.extend_from_slice(name);
                    output.push((copy, *inode));
                }
                Some(output)
            } else {
                None
            };
            nodes.push(SnapshotNode {
                receipt: if record.file.is_none() {
                    receipt_lock(&record.receipt)?.clone()
                } else {
                    None
                },
                entry: record.entry.clone(),
                names,
                attached: record.attached,
            });
        }
        memory.nodes(self.state.active_underlays.len())?;
        memory.nodes(self.state.baselines.len())?;
        let mut provenance = memory.vector(self.provenance.len())?;
        provenance.extend_from_slice(&self.provenance);
        let mut snapshot = NamespaceSnapshot {
            original_root: self.original_root.clone(),
            active_underlays: self.state.active_underlays.clone(),
            nodes,
            next_inode: self.state.next_inode,
            generation: self.state.generation,
            files: BTreeMap::new(),
            backend_state: provenance,
            baselines: self.state.baselines.clone(),
        };
        memory.validator(&snapshot)?;
        let count = self
            .state
            .nodes
            .values()
            .filter(|record| record.file.is_some())
            .count();
        let mut guards = memory.vector(count)?;
        let mut scratch = 0usize;
        let mut private = 0usize;
        let mut output_capacity = 0usize;
        let mut output_nodes = count;
        let mut validation_scratch = 0usize;
        for (&inode, record) in &self.state.nodes {
            if let Some(file) = &record.file {
                let access = self.read_access(record)?;
                memory.nodes(1)?;
                access.authorize().await?;
                let guard = file.checkpoint_guard().await;
                if guard.metadata().writers != 0 {
                    return Err(Error::Busy.into());
                }
                validation_scratch = validation_scratch.max(
                    guard
                        .extent_count()
                        .checked_mul(std::mem::size_of::<(u64, u64)>())
                        .ok_or(Error::ResourceLimit)?,
                );
                memory.nodes(guard.extent_count())?;
                output_nodes = output_nodes
                    .checked_add(guard.extent_count())
                    .ok_or(Error::ResourceLimit)?;
                let bytes = guard.capture_private_bytes()?;
                private = private
                    .checked_add(bytes)
                    .filter(|size| *size <= max_data)
                    .ok_or(Error::ResourceLimit)?;
                scratch = scratch.max(guard.capture_read_scratch()?);
                output_capacity =
                    output_capacity
                        .checked_add(bytes)
                        .and_then(|total| {
                            total.checked_add(guard.extent_count().checked_mul(
                                std::mem::size_of::<crate::overlay::SnapshotExtent>(),
                            )?)
                        })
                        .ok_or(Error::ResourceLimit)?;
                guards.push((inode, guard, access));
            }
        }
        memory.buffer(scratch)?;
        memory.buffer(validation_scratch)?;
        memory.preflight_buffer(output_capacity)?;
        memory.preflight_nodes(output_nodes)?;
        for (inode, guard, access) in &guards {
            memory.nodes(1)?;
            let file = guard.capture_bound(access, memory, scratch).await?;
            file.validate(file_limits)?;
            snapshot.files.insert(*inode, file);
        }
        drop(guards);
        self.authorize_capture().await?;
        self.validate_source().await?;
        snapshot.validate(self.freeze.namespace().limits, file_limits)?;
        Ok(snapshot)
    }
}

#[cfg(test)]
impl UnpublishedState {
    pub(crate) fn test_read_gate(&self, inode: u64) -> Arc<dyn AccessGate> {
        self.read_access(&self.state.nodes[&inode])
            .unwrap()
            .selected_gate()
    }
}
