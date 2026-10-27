// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Staged installation locks. The coordinator acquires Model state between the
//! async namespace lock and the synchronous routing lock. Displaced resources
//! remain in the external candidate until every final-commit guard is released.
use std::sync::RwLockWriteGuard;

use super::*;
use crate::namespace::metadata_gate::GateInstallGuard;
use crate::namespace::unpublished::UnpublishedState;
use crate::publication_ownership::successor::PreparedIssuer;
use crate::publication_ownership::successor::SuccessorCommitGuard;

pub(crate) struct NamespaceInstallGuard<'a, 'candidate> {
    state: tokio::sync::MutexGuard<'a, State>,
    namespace: &'a Namespace,
    committed: bool,
    // The acknowledgment retains these records on cancellation or panic.
    prepared: &'candidate mut UnpublishedState,
}
pub(crate) struct RoutingInstallGuard<'a> {
    current: RwLockWriteGuard<'a, Arc<NamespaceBinding>>,
    namespace: &'a Namespace,
}
impl Namespace {
    pub(crate) async fn prepare_install<'a, 'candidate>(
        &'a self,
        prepared: &'candidate mut UnpublishedState,
    ) -> crate::core::Result<NamespaceInstallGuard<'a, 'candidate>> {
        let state = self.state.lock().await;
        prepared.check_install_source(self, &state)?;
        Ok(NamespaceInstallGuard {
            state,
            namespace: self,
            committed: false,
            prepared,
        })
    }
}
impl<'a> NamespaceInstallGuard<'a, '_> {
    /// Capture these before acquiring synchronous locks; allocator guards borrow
    /// the independent Arc owners rather than this mutable installation guard.
    pub(crate) fn backend_pair(&self) -> (Arc<dyn Backend>, Arc<dyn Backend>) {
        (
            self.prepared.install_source_binding().backend.clone(),
            self.prepared.install_binding().backend.clone(),
        )
    }
    pub(crate) fn issuer(&self) -> &PreparedIssuer {
        self.prepared.install_issuer()
    }
    pub(crate) fn epoch(&self) -> u64 {
        self.prepared.install_epoch()
    }
    /// Call after the Model state lock, before publication and allocator locks.
    pub(crate) fn lock_routing(&self) -> crate::core::Result<RoutingInstallGuard<'a>> {
        let current = self.namespace.routing.install_lock()?;
        if !Arc::ptr_eq(&current, self.prepared.install_source_binding()) {
            return Err(Error::Stale);
        }
        Ok(RoutingInstallGuard {
            current,
            namespace: self.namespace,
        })
    }
    /// Validate before publication-slot guards are taken; no slot lookup follows.
    pub(crate) fn check_seal(&self, seal: &MetadataSeal) -> crate::core::Result<()> {
        self.prepared.check_install_seal(self.namespace, seal)
    }
    /// Every fallible check precedes the first swap. The coordinator completes
    /// ownership, allocator and Model swaps infallibly before releasing guards.
    pub(crate) fn commit(
        &mut self,
        routing: &mut RoutingInstallGuard<'_>,
        ownership: &SuccessorCommitGuard<'_>,
        epoch: &mut GateInstallGuard<'_>,
    ) -> crate::core::Result<()> {
        if self.committed
            || !std::ptr::eq(self.namespace, routing.namespace)
            || !Arc::ptr_eq(&routing.current, self.prepared.install_source_binding())
            || !epoch.matches_gate(&self.namespace.mutation_gate)
            || !epoch.matches_epoch(self.prepared.install_epoch())
        {
            return Err(Error::Stale);
        }
        self.prepared
            .check_install_state(self.namespace, &self.state)?;
        ownership.check_issuer(self.prepared.install_issuer())?;
        self.prepared.swap_install_state(&mut self.state);
        self.prepared.swap_install_binding(&mut routing.current);
        epoch.commit();
        self.committed = true;
        Ok(())
    }
}
impl MetadataSeal {
    /// Acquire last, after exact owner slots and both allocator owner locks.
    pub(crate) fn prepare_install(
        &self,
        expected: u64,
    ) -> crate::core::Result<GateInstallGuard<'_>> {
        self._seal.prepare_install(expected)
    }
}
