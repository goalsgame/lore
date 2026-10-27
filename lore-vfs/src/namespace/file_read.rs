// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Live read capture selects one current or retired binding without holding the
//! namespace lock across policy or file IO. This grants no writable admission.

use super::*;
use crate::namespace::unpublished::FileReadAccess;
use crate::overlay::Metadata as FileMetadata;

pub(super) struct CapturedRead {
    pub(super) entry: Entry,
    pub(super) file: Option<Arc<OverlayFile>>,
    pub(super) cell: ReceiptCell,
    pub(super) binding: Arc<NamespaceBinding>,
    pub(super) access: Option<FileReadAccess>,
}
impl CapturedRead {
    fn from_record(record: &Record, current: &Arc<NamespaceBinding>) -> Result<Self> {
        Ok(Self {
            entry: record.entry.clone(),
            file: record.file.clone(),
            cell: record.receipt.clone(),
            binding: Namespace::record_binding(record, current)?,
            access: record
                .file
                .as_ref()
                .map(|_file| FileReadAccess::from_record(record, current))
                .transpose()?,
        })
    }
    pub(super) async fn authorize(&self) -> Result<()> {
        self.binding
            .gate
            .authorize(&self.entry.authority, Access::Read)
            .await
            .map_err(Into::into)
    }
    pub(super) async fn metadata(&self) -> Result<FileMetadata> {
        if self.entry.kind != NodeKind::File {
            return Err(Error::IsDirectory.into());
        }
        if let (Some(file), Some(access)) = (&self.file, &self.access) {
            return file.metadata_bound(access).await.map_err(Into::into);
        }
        self.authorize().await?;
        let base = self.entry.base.as_ref().ok_or(Error::Io)?;
        let metadata = FileMetadata {
            size: base.size,
            mode: base.mode,
            generation: 0,
            writers: 0,
            mutable: false,
            reserved_storage_bytes: 0,
        };
        self.authorize().await?;
        Ok(metadata)
    }
}
impl Namespace {
    pub(super) async fn capture_record_read(
        &self,
        inode: u64,
        token: Option<u64>,
    ) -> Result<CapturedRead> {
        let state = self.state.lock().await;
        let binding = self.current_binding_locked(&state)?;
        if token.is_some_and(|token| !self.operation_freeze.matches(token)) {
            return Err(Error::Stale.into());
        }
        CapturedRead::from_record(node(&state, inode)?, &binding)
    }
    pub(super) async fn confirm_record_read(
        &self,
        read: &CapturedRead,
        token: Option<u64>,
    ) -> Result<()> {
        let state = self.state.lock().await;
        let binding = self.current_binding_locked(&state)?;
        if token.is_some_and(|token| !self.operation_freeze.matches(token)) {
            return Err(Error::Stale.into());
        }
        let record = node(&state, read.entry.inode)?;
        if record.entry.authority != read.entry.authority
            || record.entry.kind != read.entry.kind
            || record.entry.base != read.entry.base
            || !Arc::ptr_eq(&record.receipt, &read.cell)
            || !Arc::ptr_eq(&Self::record_binding(record, &binding)?, &read.binding)
            || !match (&record.file, &read.file) {
                (None, None) => true,
                (Some(current), Some(captured)) => Arc::ptr_eq(current, captured),
                _ => false,
            }
        {
            return Err(Error::Stale.into());
        }
        Ok(())
    }
    pub(crate) async fn file_metadata(&self, inode: u64) -> Result<(Entry, FileMetadata)> {
        let read = self.capture_record_read(inode, None).await?;
        let metadata = read.metadata().await?;
        self.confirm_record_read(&read, None).await?;
        Ok((read.entry, metadata))
    }
    pub(super) async fn scan_frozen_writers(&self, token: u64) -> Result<()> {
        let reads = {
            let state = self.state.lock().await;
            let binding = self.current_binding_locked(&state)?;
            if !self.operation_freeze.matches(token) {
                return Err(Error::Stale.into());
            }
            state
                .nodes
                .values()
                .filter(|record| record.file.is_some())
                .map(|record| CapturedRead::from_record(record, &binding))
                .collect::<Result<Vec<_>>>()?
        };
        for read in &reads {
            if read.metadata().await?.writers != 0 {
                return Err(NamespaceError::Busy);
            }
        }
        // Cache misses may have been materialized after capture, but the freeze
        // forbids their writable admission. They therefore cannot hide a writer.
        for read in &reads {
            self.confirm_record_read(read, Some(token)).await?;
        }
        Ok(())
    }
}

#[cfg(all(test, unix))]
#[path = "file_read_tests.rs"]
mod tests;
