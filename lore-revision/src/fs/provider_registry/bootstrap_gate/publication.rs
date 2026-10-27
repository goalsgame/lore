// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Exact pending-exclusive publication changes admission without dropping leases.

use std::sync::MutexGuard;

use super::*;

pub(in crate::fs::provider_registry) struct PublicationCommit<'a> {
    state: MutexGuard<'a, State>,
}
impl Gate {
    pub(in crate::fs::provider_registry) fn publication_commit<'a>(
        self: &'a Arc<Self>,
        exclusive: &ExclusiveReservation,
        issuer: &LoadLease,
    ) -> Result<PublicationCommit<'a>> {
        if !Arc::ptr_eq(self, &exclusive.exclusive.gate)
            || !Arc::ptr_eq(self, &issuer.principal.gate)
        {
            return Err(Error::WrongGate);
        }
        let state = self.state.lock().map_err(|_poison| Error::Poisoned)?;
        match state.phase {
            Phase::Retired => return Err(Error::Retired),
            Phase::Exclusive(nonce) if nonce == exclusive.exclusive.nonce => {}
            _ => return Err(Error::WrongGate),
        }
        if state.loads.len() != 1 || !state.loads.contains(&issuer.principal.id) || state.work != 0
        {
            return Err(Error::Busy);
        }
        Ok(PublicationCommit { state })
    }
}
impl PublicationCommit<'_> {
    /// Called only inside the paired registry commit after every check succeeds.
    /// No lease, provider, parent, or claim is destroyed by this state change.
    pub(in crate::fs::provider_registry) fn open_ready(&mut self) {
        self.state.phase = Phase::Ready;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn gate() -> Arc<Gate> {
        Gate::new(Limits {
            max_loads: 4,
            max_work: 4,
        })
        .unwrap()
    }
    #[test]
    fn exact_commit_opens_only_its_current_nonce_and_issuer() {
        let gate = gate();
        let issuer = gate.admit().unwrap();
        let exclusive = gate.try_exclusive(&issuer).unwrap();
        let foreign = self::gate();
        assert!(matches!(
            foreign.publication_commit(&exclusive, &issuer),
            Err(Error::WrongGate)
        ));
        assert!(matches!(gate.check_ready(), Err(Error::Busy)));
        gate.publication_commit(&exclusive, &issuer)
            .unwrap()
            .open_ready();
        assert!(gate.check_ready().is_ok());
        let next = gate.try_exclusive(&issuer).unwrap();
        assert!(matches!(
            gate.publication_commit(&exclusive, &issuer),
            Err(Error::WrongGate)
        ));
        // Dropping an older reservation cannot reopen its replacement.
        drop(exclusive);
        assert!(matches!(gate.check_ready(), Err(Error::Busy)));
        gate.publication_commit(&next, &issuer)
            .unwrap()
            .open_ready();
        drop(next);
        assert!(gate.check_ready().is_ok());
    }
    #[test]
    fn retired_and_poisoned_gates_cannot_commit() {
        for poison in [false, true] {
            let gate = gate();
            let issuer = gate.admit().unwrap();
            let exclusive = gate.try_exclusive(&issuer).unwrap();
            if poison {
                gate.test_poison();
            } else {
                gate.retire().unwrap();
            }
            assert!(matches!(
                gate.publication_commit(&exclusive, &issuer),
                Err(Error::Retired | Error::Poisoned)
            ));
            drop(exclusive);
            assert!(gate.check_ready().is_err());
        }
    }
}
