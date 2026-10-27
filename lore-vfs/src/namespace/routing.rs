// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Short synchronous captures of the namespace's paired backend and policy.
//! Callers release the routing guard before authorization, I/O or ownership checks.

use std::sync::RwLock;

use super::*;

pub(super) struct Routing {
    current: RwLock<Arc<NamespaceBinding>>,
}
impl Routing {
    pub(super) fn new(binding: Arc<NamespaceBinding>) -> Self {
        Self {
            current: RwLock::new(binding),
        }
    }
    #[cfg(test)]
    pub(super) fn replace_test(&self, binding: Arc<NamespaceBinding>) {
        *self.current.write().unwrap() = binding;
    }
    pub(super) fn install_lock(
        &self,
    ) -> crate::core::Result<std::sync::RwLockWriteGuard<'_, Arc<NamespaceBinding>>> {
        self.current.write().map_err(|_poison| Error::Io)
    }
    pub(super) fn capture(&self) -> crate::core::Result<Arc<NamespaceBinding>> {
        let current = self.current.read().map_err(|_poison| Error::Io)?;
        Ok(current.clone())
    }
    pub(super) fn confirm(&self, binding: &Arc<NamespaceBinding>) -> crate::core::Result<()> {
        let current = self.current.read().map_err(|_poison| Error::Io)?;
        if Arc::ptr_eq(&current, binding) {
            Ok(())
        } else {
            Err(Error::Stale)
        }
    }
}

impl Namespace {
    /// Pair routing with metadata while the caller retains the namespace lock.
    pub(super) fn current_binding_locked(&self, _state: &State) -> Result<Arc<NamespaceBinding>> {
        self.current_binding().map_err(Into::into)
    }
    /// Capture only routing; this never acquires the asynchronous metadata lock.
    pub(super) fn current_binding(&self) -> crate::core::Result<Arc<NamespaceBinding>> {
        self.routing.capture()
    }
    pub(super) fn confirms_binding(
        &self,
        binding: &Arc<NamespaceBinding>,
    ) -> crate::core::Result<()> {
        self.routing.confirm(binding)
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
impl Namespace {
    pub(crate) fn test_effective_source_gate(&self) -> Arc<dyn AccessGate> {
        self.current_binding().unwrap().gate.clone()
    }
    pub(crate) fn test_replace_source_gate(&self, gate: Arc<dyn AccessGate>) {
        let current = self.current_binding().unwrap();
        self.routing.replace_test(Arc::new(NamespaceBinding {
            backend: current.backend.clone(),
            gate,
            policy: current.policy.clone(),
        }));
    }
}
