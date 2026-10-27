// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Fresh private copies. Source bytes and identities remain unchanged.
#![allow(dead_code, reason = "Unpublished copies have no installation caller.")]

use super::*;

pub(crate) struct CopyLimits {
    pub(crate) file: Limits,
    pub(crate) extents: usize,
}

impl OverlayFile {
    #[cfg(test)]
    pub(crate) fn test_effective_gate(&self) -> &Arc<dyn AccessGate> {
        &self.gate
    }
    /// Copy a frozen private image into fresh, exclusively owned budgeted storage.
    /// The caller retains the exact storage owner and shared budget. This does
    /// not allocate an inode, authorize installation or publish checkpoint state.
    pub(crate) async fn copy_private(
        source: &FileSnapshot,
        old_inode: u64,
        new_inode: u64,
        backend: Arc<dyn Backend>,
        gate: Arc<dyn AccessGate>,
        store: Arc<dyn SparseStore>,
        limits: Limits,
    ) -> Result<Self> {
        Self::copy_private_bounded(
            source,
            old_inode,
            new_inode,
            backend,
            gate,
            store,
            CopyLimits {
                file: limits,
                extents: limits.max_extents,
            },
        )
        .await
    }

    pub(crate) async fn copy_private_bounded(
        source: &FileSnapshot,
        old_inode: u64,
        new_inode: u64,
        backend: Arc<dyn Backend>,
        gate: Arc<dyn AccessGate>,
        store: Arc<dyn SparseStore>,
        copy: CopyLimits,
    ) -> Result<Self> {
        let limits = copy.file;
        if old_inode == 0 || new_inode == 0 || old_inode == new_inode || !source.metadata.mutable {
            return Err(Error::InvalidInput);
        }
        source.validate(limits)?;
        if let Some(receipt) = &source.receipt {
            receipt.validate(
                old_inode,
                source.node.key,
                source.metadata.generation,
                source.metadata.size,
                source.metadata.mode,
            )?;
        }
        gate.authorize(&source.node.key, Access::Write).await?;
        if let Some(receipt) = &source.receipt {
            backend
                .validate_committed(&source.node.key, &receipt.proof)
                .await?;
            gate.authorize(&source.node.key, Access::Write).await?;
        }
        let mut metadata = source.metadata;
        // Historical source debt belongs to the retained old store. New debt
        // counts every requested append, including short-write retry ranges.
        metadata.reserved_storage_bytes = 0;
        let mut state = State {
            metadata,
            base_end: source.base_end,
            extents: BTreeMap::new(),
            handles: BTreeMap::new(),
            next_handle: 1,
        };
        for extent in &source.extents {
            let mut copied = 0;
            while copied < extent.data.len() {
                let count = (extent.data.len() - copied).min(MAX_READ_SIZE);
                let logical = extent.start + copied as u64;
                let stored = state.metadata.reserved_storage_bytes;
                let reserved = stored
                    .checked_add(count as u64)
                    .filter(|reserved| *reserved <= limits.max_storage_bytes)
                    .ok_or(Error::ResourceLimit)?;
                let merge = state
                    .extents
                    .iter()
                    .next_back()
                    .and_then(|(&start, previous)| {
                        (previous.end == logical
                            && previous.stored.checked_add(previous.end - start) == Some(stored))
                        .then_some(start)
                    });
                if state
                    .extents
                    .len()
                    .checked_add(usize::from(merge.is_none()))
                    .is_none_or(|count| count > limits.max_extents || count > copy.extents)
                {
                    return Err(Error::ResourceLimit);
                }
                gate.authorize(&source.node.key, Access::Write).await?;
                state.metadata.reserved_storage_bytes = reserved;
                let written = store
                    .write(stored, &extent.data[copied..copied + count])
                    .await?;
                if written == 0 || written > count {
                    return Err(Error::Io);
                }
                let end = logical + written as u64;
                if let Some(start) = merge {
                    state.extents.get_mut(&start).ok_or(Error::Io)?.end = end;
                } else {
                    state.extents.insert(logical, Extent { end, stored });
                }
                gate.authorize(&source.node.key, Access::Write).await?;
                copied += written;
            }
        }
        if let Some(receipt) = &source.receipt {
            backend
                .validate_committed(&source.node.key, &receipt.proof)
                .await?;
        }
        gate.authorize(&source.node.key, Access::Write).await?;
        let receipt = source.receipt.as_ref().map(|receipt| Receipt {
            inode: new_inode,
            ..receipt.clone()
        });
        Ok(Self {
            mutation_gate: crate::namespace::metadata_gate::for_backend(&backend),
            receipt: Arc::new(std::sync::Mutex::new(receipt)),
            operation_freeze: Arc::new(OperationFreeze::default()),
            node: source.node.clone(),
            backend,
            gate,
            store,
            limits,
            state: Mutex::new(state),
        })
    }
}
