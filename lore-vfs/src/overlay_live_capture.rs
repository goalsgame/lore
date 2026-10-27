// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Live checkpoint reads use a selected immutable route while retaining the file
//! guard. Open writers are serialized by this guard and are not persisted.
use super::*;
use crate::namespace::unpublished::FileReadAccess;
use crate::transition_prepare::Budget;

impl CheckpointGuard<'_> {
    pub(crate) async fn capture_live_bound(
        &self,
        access: &FileReadAccess,
        max_bytes: usize,
        mut budget: Option<&mut Budget>,
    ) -> Result<FileSnapshot> {
        access.check_file(self.file)?;
        access.authorize().await?;
        let private = self.capture_private_bytes()?;
        if private > max_bytes {
            return Err(Error::ResourceLimit);
        }
        let scratch = self.capture_read_scratch()?;
        if let Some(budget) = &mut budget {
            budget.paths(private)?;
            budget.paths(scratch)?;
            budget.paths(
                self.extent_count()
                    .checked_mul(std::mem::size_of::<SnapshotExtent>())
                    .ok_or(Error::ResourceLimit)?,
            )?;
            for _ in 0..self.extent_count() {
                budget.node()?;
            }
        }
        let mut extents = Vec::new();
        extents
            .try_reserve_exact(self.extent_count())
            .map_err(|_allocation| Error::ResourceLimit)?;
        if let Some(budget) = &mut budget {
            budget.paths(
                extents
                    .capacity()
                    .checked_sub(self.extent_count())
                    .and_then(|extra| extra.checked_mul(std::mem::size_of::<SnapshotExtent>()))
                    .ok_or(Error::ResourceLimit)?,
            )?;
        }
        for (&start, extent) in &self.state.extents {
            let length = usize::try_from(extent.end.checked_sub(start).ok_or(Error::InvalidInput)?)
                .map_err(|_length| Error::ResourceLimit)?;
            let mut data = Vec::new();
            data.try_reserve_exact(length)
                .map_err(|_allocation| Error::ResourceLimit)?;
            if let Some(budget) = &mut budget {
                budget.paths(
                    data.capacity()
                        .checked_sub(length)
                        .ok_or(Error::ResourceLimit)?,
                )?;
            }
            while data.len() < length {
                let count = (length - data.len()).min(MAX_READ_SIZE);
                access.authorize().await?;
                let bytes = self
                    .file
                    .store
                    .read(
                        extent
                            .stored
                            .checked_add(
                                u64::try_from(data.len())
                                    .map_err(|_length| Error::ResourceLimit)?,
                            )
                            .ok_or(Error::ResourceLimit)?,
                        count,
                    )
                    .await?;
                if bytes.len() != count {
                    return Err(Error::Io);
                }
                if bytes.capacity() > scratch {
                    return Err(Error::ResourceLimit);
                }
                access.authorize().await?;
                data.extend_from_slice(&bytes);
            }
            extents.push(SnapshotExtent {
                start,
                stored: extent.stored,
                data,
            });
        }
        let mut metadata = self.state.metadata;
        metadata.writers = 0;
        let snapshot = FileSnapshot {
            receipt: receipt_lock(&self.file.receipt)?.clone(),
            node: self.file.node.clone(),
            metadata,
            base_end: self.state.base_end,
            extents,
        };
        access.authorize().await?;
        Ok(snapshot)
    }
}
