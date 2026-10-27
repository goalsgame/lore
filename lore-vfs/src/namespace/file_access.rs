// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Exact file/namespace admission. This temporary proof pins no freeze; opened
//! handles retain only the selected immutable IO route, not this proof.

use super::*;

pub(crate) struct FileAccess {
    namespace: std::sync::Weak<Namespace>,
    inode: u64,
    key: NodeKey,
    file: Arc<OverlayFile>,
    receipt: ReceiptCell,
    binding: Arc<NamespaceBinding>,
}
impl FileAccess {
    pub(crate) fn check_file(&self, file: &OverlayFile) -> crate::core::Result<()> {
        if self.namespace.upgrade().is_none()
            || !std::ptr::eq(self.file.as_ref(), file)
            || self.key != file.key()
            || !Arc::ptr_eq(&self.receipt, &file.receipt_cell())
        {
            return Err(Error::PermissionDenied);
        }
        Ok(())
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
}
impl Namespace {
    pub(crate) async fn capture_file_access(
        self: &Arc<Self>,
        inode: u64,
        file: &Arc<OverlayFile>,
    ) -> Result<FileAccess> {
        let state = self.state.lock().await;
        let binding = self.current_binding_locked(&state)?;
        let record = node(&state, inode)?;
        if record.entry.kind != NodeKind::File
            || record
                .file
                .as_ref()
                .is_none_or(|current| !Arc::ptr_eq(current, file))
            || record.entry.authority != file.key()
            || !Arc::ptr_eq(&record.receipt, &file.receipt_cell())
        {
            return Err(Error::Stale.into());
        }
        Ok(FileAccess {
            namespace: Arc::downgrade(self),
            inode,
            key: record.entry.authority,
            file: file.clone(),
            receipt: record.receipt.clone(),
            binding: Self::record_binding(record, &binding)?,
        })
    }
    /// Identity lookup for an already owned handle. The caller must authorize
    /// its handle route after this lookup before returning metadata. This never
    /// substitutes the namespace's current policy for an existing lease.
    pub(crate) async fn entry_for_file_lease(
        self: &Arc<Self>,
        access: &FileAccess,
    ) -> Result<Entry> {
        let state = self.state.lock().await;
        if !std::sync::Weak::ptr_eq(&access.namespace, &Arc::downgrade(self)) {
            return Err(Error::Stale.into());
        }
        let record = node(&state, access.inode)?;
        if record.entry.kind != NodeKind::File
            || record.entry.authority != access.key
            || record
                .file
                .as_ref()
                .is_none_or(|current| !Arc::ptr_eq(current, &access.file))
            || !Arc::ptr_eq(&record.receipt, &access.receipt)
        {
            return Err(Error::Stale.into());
        }
        Ok(record.entry.clone())
    }

    /// Runs after the inner handle has an owned cleanup lease. No file lock may
    /// be held here. A stale or cancelled confirmation must release that handle.
    pub(crate) async fn confirm_file_access(self: &Arc<Self>, access: &FileAccess) -> Result<()> {
        let state = self.state.lock().await;
        let binding = self.current_binding_locked(&state)?;
        if !std::sync::Weak::ptr_eq(&access.namespace, &Arc::downgrade(self)) {
            return Err(Error::Stale.into());
        }
        let record = node(&state, access.inode)?;
        if record.entry.kind != NodeKind::File
            || record.entry.authority != access.key
            || record
                .file
                .as_ref()
                .is_none_or(|current| !Arc::ptr_eq(current, &access.file))
            || !Arc::ptr_eq(&record.receipt, &access.receipt)
            || !Arc::ptr_eq(&Self::record_binding(record, &binding)?, &access.binding)
        {
            return Err(Error::Stale.into());
        }
        Ok(())
    }
}

#[cfg(all(test, unix))]
#[path = "file_access_tests.rs"]
pub(crate) mod tests;
