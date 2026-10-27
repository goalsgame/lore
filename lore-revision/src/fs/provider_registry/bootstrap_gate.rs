// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Private admission facts only. This gate is not bound to a Registration,
//! canonical path, Client token or store facade and proves none of their rights.
//! Separate load principals and retained finite work prevent excluding sibling
//! activity merely because it shares a command or write-token family.
#![allow(
    dead_code,
    reason = "Bootstrap admission is not connected to repository loaders or store lifetimes."
)]

mod publication;

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::Mutex;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Error {
    Busy,
    Retired,
    Limit,
    WrongGate,
    Poisoned,
    InvalidLimit,
}
type Result<T> = std::result::Result<T, Error>;
pub(super) struct Limits {
    pub(super) max_loads: usize,
    pub(super) max_work: usize,
}
enum Phase {
    Ready,
    Exclusive(u64),
    Retired,
}
struct State {
    phase: Phase,
    loads: BTreeSet<u64>,
    work: usize,
    next_load: u64,
    next_exclusive: u64,
}
pub(super) struct Gate {
    limits: Limits,
    state: Mutex<State>,
}
pub(super) struct LoadLease {
    principal: Arc<Principal>,
}
struct Principal {
    gate: Arc<Gate>,
    id: u64,
}
#[derive(Clone)]
pub(super) struct WorkLease {
    work: Arc<Work>,
}
struct Work {
    gate: Arc<Gate>,
}
// Final-pin rollback is pre-intent only. Any caller doing partial artifact IO
// must terminally retire/quarantine before releasing the reservation.
pub(super) struct ExclusiveReservation {
    exclusive: Arc<Exclusive>,
}
#[derive(Clone)]
pub(super) struct ExclusivePin {
    exclusive: Arc<Exclusive>,
}
struct Exclusive {
    gate: Arc<Gate>,
    nonce: u64,
}

impl Gate {
    pub(super) fn new(limits: Limits) -> Result<Arc<Self>> {
        if limits.max_loads == 0 || limits.max_work == 0 {
            return Err(Error::InvalidLimit);
        }
        Ok(Arc::new(Self {
            limits,
            state: Mutex::new(State {
                phase: Phase::Ready,
                loads: BTreeSet::new(),
                work: 0,
                next_load: 1,
                next_exclusive: 1,
            }),
        }))
    }
    pub(super) fn admit(self: &Arc<Self>) -> Result<LoadLease> {
        let mut state = self.state.lock().map_err(|_poison| Error::Poisoned)?;
        ready(&state)?;
        if state.loads.len() >= self.limits.max_loads {
            return Err(Error::Limit);
        }
        let id = state.next_load;
        let next = id.checked_add(1).ok_or(Error::Limit)?;
        state.loads.insert(id);
        state.next_load = next;
        Ok(LoadLease {
            principal: Arc::new(Principal {
                gate: self.clone(),
                id,
            }),
        })
    }
    pub(super) fn begin_work(self: &Arc<Self>, lease: &LoadLease) -> Result<WorkLease> {
        self.check_gate(lease)?;
        let mut state = self.state.lock().map_err(|_poison| Error::Poisoned)?;
        ready(&state)?;
        if !state.loads.contains(&lease.principal.id) {
            return Err(Error::WrongGate);
        }
        let work = state
            .work
            .checked_add(1)
            .filter(|work| *work <= self.limits.max_work)
            .ok_or(Error::Limit)?;
        state.work = work;
        Ok(WorkLease {
            work: Arc::new(Work { gate: self.clone() }),
        })
    }
    /// Never waits while an issuer holds its own command/token/serial permits.
    /// Only its dormant principal is excluded; every work lease still counts.
    pub(super) fn try_exclusive(
        self: &Arc<Self>,
        issuer: &LoadLease,
    ) -> Result<ExclusiveReservation> {
        self.check_gate(issuer)?;
        let mut state = self.state.lock().map_err(|_poison| Error::Poisoned)?;
        ready(&state)?;
        if !state.loads.contains(&issuer.principal.id) {
            return Err(Error::WrongGate);
        }
        if state.loads.len() != 1 || state.work != 0 {
            return Err(Error::Busy);
        }
        let nonce = state.next_exclusive;
        let next = nonce.checked_add(1).ok_or(Error::Limit)?;
        state.phase = Phase::Exclusive(nonce);
        state.next_exclusive = next;
        Ok(ExclusiveReservation {
            exclusive: Arc::new(Exclusive {
                gate: self.clone(),
                nonce,
            }),
        })
    }
    pub(super) fn check_ready(&self) -> Result<()> {
        let state = self.state.lock().map_err(|_poison| Error::Poisoned)?;
        ready(&state)
    }
    pub(super) fn retire(&self) -> Result<()> {
        let mut state = self.state.lock().map_err(|_poison| Error::Poisoned)?;
        state.phase = Phase::Retired;
        Ok(())
    }
    #[cfg(test)]
    pub(super) fn test_poison(&self) {
        let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _state = self.state.lock().unwrap();
            panic!("poison bootstrap gate");
        }));
        assert!(failed.is_err());
    }
    fn check_gate(self: &Arc<Self>, lease: &LoadLease) -> Result<()> {
        if Arc::ptr_eq(self, &lease.principal.gate) {
            Ok(())
        } else {
            Err(Error::WrongGate)
        }
    }
}
impl LoadLease {
    // Inherited contexts retain one principal. A new explicit load calls admit.
    pub(super) fn pin(&self) -> Self {
        Self {
            principal: self.principal.clone(),
        }
    }
}
impl ExclusiveReservation {
    /// Ownership retention only; a pin grants no store access or new work.
    pub(super) fn pin(&self) -> ExclusivePin {
        ExclusivePin {
            exclusive: self.exclusive.clone(),
        }
    }
}
fn ready(state: &State) -> Result<()> {
    match state.phase {
        Phase::Ready => Ok(()),
        Phase::Exclusive(_) => Err(Error::Busy),
        Phase::Retired => Err(Error::Retired),
    }
}
impl Drop for Principal {
    fn drop(&mut self) {
        if let Ok(mut state) = self.gate.state.lock() {
            state.loads.remove(&self.id);
        }
    }
}
impl Drop for Work {
    fn drop(&mut self) {
        if let Ok(mut state) = self.gate.state.lock() {
            if let Some(work) = state.work.checked_sub(1) {
                state.work = work;
            } else {
                state.phase = Phase::Retired;
            }
        }
    }
}
impl Drop for Exclusive {
    fn drop(&mut self) {
        // A poisoned gate stays unusable; drops never recover its mutex state.
        if let Ok(mut state) = self.gate.state.lock()
            && matches!(state.phase, Phase::Exclusive(nonce) if nonce == self.nonce)
        {
            state.phase = Phase::Ready;
        }
    }
}

#[cfg(test)]
mod tests;
