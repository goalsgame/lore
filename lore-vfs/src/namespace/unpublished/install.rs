// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Private access to prebuilt state; installation never rebuilds records or names.
use super::*;

impl UnpublishedState {
    pub(in crate::namespace) fn check_install_source(
        &self,
        namespace: &Namespace,
        current: &State,
    ) -> crate::core::Result<()> {
        if !std::ptr::eq(namespace, self.freeze.namespace().as_ref())
            || !self.freeze.valid()
            || namespace.original_root != self.original_root
            || current.generation != self.generation
            || current.next_inode != self.source_next
            || namespace.mutation_gate.epoch()? != self.epoch
        {
            return Err(Error::Stale);
        }
        namespace.confirms_binding(&self.source_binding)?;
        Ok(())
    }
    pub(in crate::namespace) fn install_issuer(
        &self,
    ) -> &crate::publication_ownership::successor::PreparedIssuer {
        &self.issuer
    }
    pub(in crate::namespace) fn check_install_state(
        &self,
        namespace: &Namespace,
        current: &State,
    ) -> crate::core::Result<()> {
        if !std::ptr::eq(namespace, self.freeze.namespace().as_ref())
            || namespace.original_root != self.original_root
            || current.generation != self.generation
            || current.next_inode != self.source_next
        {
            return Err(Error::Stale);
        }
        Ok(())
    }
    pub(in crate::namespace) fn install_source_binding(&self) -> &Arc<NamespaceBinding> {
        &self.source_binding
    }
    pub(in crate::namespace) fn install_binding(&self) -> &Arc<NamespaceBinding> {
        &self.binding
    }
    pub(in crate::namespace) fn install_epoch(&self) -> u64 {
        self.epoch
    }
    pub(in crate::namespace) fn check_install_seal(
        &self,
        namespace: &Namespace,
        seal: &MetadataSeal,
    ) -> crate::core::Result<()> {
        if !self.freeze.same_acquisition(&seal._freeze)
            || !seal._seal.matches_gate(&namespace.mutation_gate)
        {
            return Err(Error::Stale);
        }
        Ok(())
    }
    pub(in crate::namespace) fn swap_install_state(&mut self, state: &mut State) {
        std::mem::swap(&mut self.state, state);
    }
    pub(in crate::namespace) fn swap_install_binding(
        &mut self,
        binding: &mut Arc<NamespaceBinding>,
    ) {
        std::mem::swap(&mut self.binding, binding);
    }
}
