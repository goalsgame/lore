// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Read-only routing selected from a validated namespace or unpublished record. This
//! proof is not accepted by live handle admission and grants no write authority.
use super::*;

pub(crate) struct FileReadAccess {
    inode: u64,
    file: Arc<OverlayFile>,
    receipt: ReceiptCell,
    key: NodeKey,
    binding: Arc<NamespaceBinding>,
}
impl UnpublishedState {
    pub(super) fn read_access(&self, record: &Record) -> crate::core::Result<FileReadAccess> {
        FileReadAccess::from_record(record, &self.binding)
    }
}
impl FileReadAccess {
    pub(in crate::namespace) fn from_record(
        record: &Record,
        current: &Arc<NamespaceBinding>,
    ) -> crate::core::Result<Self> {
        let file = record.file.as_ref().ok_or(Error::InvalidInput)?;
        if record.entry.kind != NodeKind::File
            || record.entry.authority != file.key()
            || !Arc::ptr_eq(&record.receipt, &file.receipt_cell())
        {
            return Err(Error::InvalidInput);
        }
        let binding = if record.attached {
            current.clone()
        } else {
            record.retired_binding.clone().ok_or(Error::Stale)?
        };
        Ok(Self {
            inode: record.entry.inode,
            file: file.clone(),
            receipt: record.receipt.clone(),
            key: record.entry.authority,
            binding,
        })
    }
    pub(crate) fn inode(&self) -> u64 {
        self.inode
    }
    pub(crate) fn backend(&self) -> Arc<dyn Backend> {
        self.binding.backend.clone()
    }
    pub(crate) fn gate(&self) -> Arc<dyn AccessGate> {
        self.binding.gate.clone()
    }
    pub(crate) fn key(&self) -> NodeKey {
        self.key
    }

    pub(crate) fn check_file(&self, file: &OverlayFile) -> crate::core::Result<()> {
        if !std::ptr::eq(file, self.file.as_ref())
            || file.key() != self.key
            || !Arc::ptr_eq(&self.receipt, &file.receipt_cell())
        {
            return Err(Error::PermissionDenied);
        }
        Ok(())
    }
    pub(crate) async fn authorize(&self) -> crate::core::Result<()> {
        self.binding.gate.authorize(&self.key, Access::Read).await
    }
}

#[cfg(test)]
impl FileReadAccess {
    pub(crate) fn selected_gate(&self) -> Arc<dyn AccessGate> {
        self.binding.gate.clone()
    }
}
