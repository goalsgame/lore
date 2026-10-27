// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Shared metadata and file-cache admission. Acquire namespace/owner/file/receipt
//! locks before this gate, never afterward. A mutation guard spans synchronous
//! local changes only; no policy callback, IO or await may run while it is held.
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::MutexGuard;

use crate::core::Error;
use crate::core::Result;
pub(crate) fn for_backend(backend: &Arc<dyn crate::core::Backend>) -> Arc<MutationGate> {
    backend
        .as_any()
        .and_then(|backend| backend.downcast_ref::<crate::revision::RevisionBackend>())
        .map_or_else(
            || Arc::new(MutationGate::default()),
            |backend| backend.shared_mutation_gate(),
        )
}
#[derive(Default)]
pub(crate) struct MutationGate {
    state: Mutex<State>,
}
#[derive(Default)]
struct State {
    epoch: u64,
    next_seal: u64,
    sealed: Option<u64>,
}
pub(crate) struct Mutation<'a> {
    _guard: MutexGuard<'a, State>,
}
pub(crate) struct Seal {
    gate: Arc<MutationGate>,
    id: u64,
}
impl MutationGate {
    #[cfg(test)]
    pub(crate) fn test_set_epoch(&self, epoch: u64) {
        self.state.lock().unwrap().epoch = epoch;
    }
    #[cfg(test)]
    pub(crate) fn test_poison(&self) {
        let _poisoned = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = self.state.lock().unwrap();
            panic!("test metadata gate poison");
        }));
    }
    pub(crate) fn epoch(&self) -> Result<u64> {
        Ok(self.state.lock().map_err(|_poison| Error::Io)?.epoch)
    }
    pub(crate) fn open(&self) -> Result<()> {
        if self
            .state
            .lock()
            .map_err(|_poison| Error::Io)?
            .sealed
            .is_some()
        {
            Err(Error::Busy)
        } else {
            Ok(())
        }
    }
    pub(crate) fn mutation(&self) -> Result<Mutation<'_>> {
        let mut state = self.state.lock().map_err(|_poison| Error::Io)?;
        if state.sealed.is_some() {
            return Err(Error::Busy);
        }
        state.epoch = state.epoch.checked_add(1).ok_or(Error::ResourceLimit)?;
        Ok(Mutation { _guard: state })
    }
    pub(crate) fn seal(self: &Arc<Self>, expected: u64) -> Result<Seal> {
        let mut state = self.state.lock().map_err(|_poison| Error::Io)?;
        if state.sealed.is_some() {
            return Err(Error::Busy);
        }
        if state.epoch != expected {
            return Err(Error::Stale);
        }
        state.epoch.checked_add(1).ok_or(Error::ResourceLimit)?;
        let id = state.next_seal.checked_add(1).ok_or(Error::ResourceLimit)?;
        state.next_seal = id;
        state.sealed = Some(id);
        Ok(Seal {
            gate: self.clone(),
            id,
        })
    }
}
impl Drop for Seal {
    fn drop(&mut self) {
        if let Ok(mut state) = self.gate.state.lock()
            && state.sealed == Some(self.id)
        {
            state.sealed = None;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn epoch_seal_and_drop_never_wrap_or_reopen_a_different_owner() {
        let gate = Arc::new(MutationGate::default());
        {
            let _change = gate.mutation().unwrap();
        }
        assert_eq!(gate.epoch().unwrap(), 1);
        assert!(matches!(gate.seal(0), Err(Error::Stale)));
        let seal = gate.seal(1).unwrap();
        assert!(matches!(gate.mutation(), Err(Error::Busy)));
        assert!(gate.open().is_err());
        drop(seal);
        assert!(gate.open().is_ok());
        {
            let _change = gate.mutation().unwrap();
        }
        assert_eq!(gate.epoch().unwrap(), 2);
        let gate = Arc::new(MutationGate {
            state: Mutex::new(State {
                epoch: u64::MAX,
                ..Default::default()
            }),
        });
        assert!(matches!(gate.mutation(), Err(Error::ResourceLimit)));
        assert!(matches!(gate.seal(u64::MAX), Err(Error::ResourceLimit)));
        assert!(gate.open().is_ok());
        let gate = Arc::new(MutationGate {
            state: Mutex::new(State {
                next_seal: u64::MAX,
                ..Default::default()
            }),
        });
        assert!(matches!(gate.seal(0), Err(Error::ResourceLimit)));
        assert!(gate.open().is_ok());
    }
}

#[path = "metadata_gate/install.rs"]
mod install;
pub(crate) use install::GateInstallGuard;
