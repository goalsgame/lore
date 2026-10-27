// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Bounded private-image capture through an opaque selected read route.
use super::*;
use crate::capture_memory::CaptureMemory;
use crate::namespace::unpublished::FileReadAccess;

impl CheckpointGuard<'_> {
    pub(crate) fn capture_private_bytes(&self) -> Result<usize> {
        self.state
            .extents
            .iter()
            .try_fold(0usize, |sum, (&start, extent)| {
                sum.checked_add(
                    usize::try_from(extent.end.checked_sub(start).ok_or(Error::InvalidInput)?)
                        .map_err(|_length| Error::ResourceLimit)?,
                )
                .ok_or(Error::ResourceLimit)
            })
    }
    pub(crate) fn capture_read_scratch(&self) -> Result<usize> {
        self.state
            .extents
            .iter()
            .try_fold(0usize, |largest, (&start, extent)| {
                Ok(largest.max(
                    usize::try_from(extent.end.checked_sub(start).ok_or(Error::InvalidInput)?)
                        .map_err(|_length| Error::ResourceLimit)?
                        .min(MAX_READ_SIZE),
                ))
            })
    }
    pub(crate) async fn capture_bound(
        &self,
        access: &FileReadAccess,
        memory: &mut CaptureMemory,
        scratch: usize,
    ) -> Result<FileSnapshot> {
        access.check_file(self.file)?;
        if self.state.metadata.writers != 0 {
            return Err(Error::Busy);
        }
        access.authorize().await?;
        let mut extents = memory.vector(self.state.extents.len())?;
        for (&start, extent) in &self.state.extents {
            memory.nodes(1)?;
            let length = usize::try_from(extent.end.checked_sub(start).ok_or(Error::InvalidInput)?)
                .map_err(|_length| Error::ResourceLimit)?;
            let mut data = memory.vector(length)?;
            while data.len() < length {
                let count = (length - data.len()).min(MAX_READ_SIZE);
                if count > scratch {
                    return Err(Error::ResourceLimit);
                }
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
        let metadata = self.state.metadata;
        let captured = FileSnapshot {
            receipt: receipt_lock(&self.file.receipt)?.clone(),
            node: self.file.node.clone(),
            metadata,
            base_end: self.state.base_end,
            extents,
        };
        access.authorize().await?;
        Ok(captured)
    }
}
