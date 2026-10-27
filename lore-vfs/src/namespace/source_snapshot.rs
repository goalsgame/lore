// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Source checkpoints acquire namespace then ordered file guards, clone an owned
//! header, and release the namespace before their own policy or storage IO.
//! File guards remain until private bytes are copied. Gate/store callbacks must
//! not reenter this namespace or a locked file: a competing namespace holder can
//! wait for our file guards. Transition epochs describe the captured header point;
//! ordinary checkpoints allow later namespace mutations.
//! Transition accounting is cumulative for owned capture buffers and read scratch,
//! not a global RAM bound or a limit on storage-internal/cancelled IO buffers.
use super::*;
use crate::namespace::unpublished::FileReadAccess;
use crate::transition_prepare::Budget;

type OwnedFile = (u64, Arc<OverlayFile>, FileReadAccess);
type ReadAuthority = (NodeKey, Arc<NamespaceBinding>);

fn reserve<T>(count: usize, budget: &mut Option<&mut Budget>) -> Result<Vec<T>> {
    if let Some(budget) = budget {
        budget.paths(
            count
                .checked_mul(std::mem::size_of::<T>())
                .ok_or(Error::ResourceLimit)?,
        )?;
    }
    let mut output = Vec::new();
    output
        .try_reserve_exact(count)
        .map_err(|_allocation| Error::ResourceLimit)?;
    if let Some(budget) = budget {
        budget.paths(
            output
                .capacity()
                .checked_sub(count)
                .and_then(|extra| extra.checked_mul(std::mem::size_of::<T>()))
                .ok_or(Error::ResourceLimit)?,
        )?;
    }
    Ok(output)
}
async fn authorize(authorities: &[ReadAuthority]) -> Result<()> {
    for (key, binding) in authorities {
        binding.gate.authorize(key, Access::Read).await?;
    }
    Ok(())
}

impl Namespace {
    pub(super) async fn capture_source_snapshot(
        &self,
        max_data_bytes: usize,
        token: Option<u64>,
        mut budget: Option<&mut Budget>,
    ) -> Result<(NamespaceSnapshot, u64)> {
        let state = self.state.lock().await;
        self.mutation_gate.open()?;
        if token.is_some_and(|token| !self.operation_freeze.matches(token)) {
            return Err(Error::Stale.into());
        }
        let binding = self.current_binding_locked(&state)?;
        let file_count = state
            .nodes
            .values()
            .filter(|record| record.file.is_some())
            .count();
        let mut files: Vec<OwnedFile> = reserve(file_count, &mut budget)?;
        for (&inode, record) in &state.nodes {
            if let Some(file) = &record.file {
                files.push((
                    inode,
                    file.clone(),
                    FileReadAccess::from_record(record, &binding)?,
                ));
            }
        }
        // These guards borrow the owned file list, not namespace records. No
        // namespace reacquisition is needed while they retain file locks.
        let mut guards = reserve(files.len(), &mut budget)?;
        for (inode, file, access) in &files {
            guards.push((*inode, file.checkpoint_guard().await, access));
        }
        let epoch = self.mutation_gate.epoch()?;
        self.mutation_gate.open()?;
        if token.is_some_and(|token| !self.operation_freeze.matches(token)) {
            return Err(Error::Stale.into());
        }
        let mut authorities: Vec<ReadAuthority> = reserve(
            state
                .nodes
                .len()
                .checked_add(state.active_underlays.len())
                .and_then(|count| count.checked_add(state.baselines.len()))
                .ok_or(Error::ResourceLimit)?,
            &mut budget,
        )?;
        let mut nodes = reserve(state.nodes.len(), &mut budget)?;
        for record in state.nodes.values() {
            if let Some(budget) = &mut budget {
                budget.node()?;
            }
            let selected = Self::record_binding(record, &binding)?;
            authorities.push((record.entry.authority, selected));
            let names = if let Some(names) = &record.names {
                let mut copied = reserve(names.len(), &mut budget)?;
                for (spelling, inode) in names.values() {
                    if let Some(budget) = &mut budget {
                        budget.node()?;
                        budget.names(spelling.len())?;
                    }
                    let mut name = reserve(spelling.len(), &mut budget)?;
                    name.extend_from_slice(spelling);
                    copied.push((name, *inode));
                }
                Some(copied)
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
        for scope in state.active_underlays.keys().chain(state.baselines.keys()) {
            authorities.push((*scope, binding.clone()));
            if let Some(budget) = &mut budget {
                budget.node()?;
            }
        }
        if let Some(budget) = &mut budget {
            budget.paths(
                state
                    .active_underlays
                    .len()
                    .checked_mul(std::mem::size_of::<(NodeKey, crate::core::ScopeHead)>())
                    .ok_or(Error::ResourceLimit)?,
            )?;
            budget.paths(
                state
                    .baselines
                    .len()
                    .checked_mul(std::mem::size_of::<(NodeKey, HeldBaseline)>())
                    .ok_or(Error::ResourceLimit)?,
            )?;
        }
        let mut snapshot = NamespaceSnapshot {
            original_root: self.original_root.clone(),
            active_underlays: state.active_underlays.clone(),
            baselines: state.baselines.clone(),
            next_inode: state.next_inode,
            generation: state.generation,
            nodes,
            files: BTreeMap::new(),
            backend_state: Vec::new(),
        };
        drop(state);
        authorize(&authorities).await?;
        if let Some(budget) = &mut budget {
            binding
                .backend
                .as_any()
                .and_then(|backend| backend.downcast_ref::<crate::revision::RevisionBackend>())
                .ok_or(Error::Unsupported)?
                .transition_reserve_provenance(budget)?;
        }
        snapshot.backend_state = binding.backend.checkpoint_state().await?;
        if snapshot.backend_state.capacity() > crate::core::MAX_BACKEND_STATE_SIZE {
            return Err(Error::ResourceLimit.into());
        }
        if let Some(budget) = &mut budget {
            budget.paths(snapshot.backend_state.capacity())?;
        }
        // Extra append-only routes are allowed in an ordinary snapshot; the
        // transition caller instead rejects every metadata epoch change below.
        let mut remaining = max_data_bytes;
        for (inode, guard, access) in &guards {
            let private = guard.capture_private_bytes()?;
            remaining = remaining.checked_sub(private).ok_or(Error::ResourceLimit)?;
            if let Some(budget) = &mut budget {
                budget.node()?;
            }
            let file = guard
                .capture_live_bound(access, private, budget.as_deref_mut())
                .await?;
            snapshot.files.insert(*inode, file);
        }
        authorize(&authorities).await?;
        drop(guards);
        if token.is_some_and(|token| !self.operation_freeze.matches(token)) {
            return Err(Error::Stale.into());
        }
        if token.is_some() {
            self.confirms_binding(&binding)?;
            if self.mutation_gate.epoch()? != epoch {
                return Err(Error::Stale.into());
            }
        }
        Ok((snapshot, epoch))
    }
}

#[cfg(all(test, unix))]
#[path = "source_snapshot_tests.rs"]
mod tests;
