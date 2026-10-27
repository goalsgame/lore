// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Runtime working-owner identity, independent of policy and durable intent.
//! Committed slots are sticky for their object's lifetime. Pins identify one
//! lineage without retaining its Model, Namespace, storage or activation rights.

use std::sync::Arc;
use std::sync::Mutex;

use crate::core::Error;
use crate::core::Result;

pub(crate) mod successor;

struct Identity;
#[derive(Default)]
enum State {
    #[default]
    Vacant,
    Reserved(Arc<Identity>),
    Committed {
        owner: Arc<Identity>,
        active: Option<Arc<Slot>>,
    },
}
#[derive(Default)]
pub(crate) struct Slot {
    state: Mutex<State>,
}
#[derive(Clone)]
pub(crate) struct Lineage {
    identity: Arc<Identity>,
}
/// The actual Model owns this non-cloneable claim. Read-only pins are distinct.
pub(crate) struct OwnerClaim {
    namespace: Arc<Slot>,
    backend: Option<Arc<Slot>>,
    lineage: Lineage,
}
pub(crate) struct Reservation {
    namespace: Arc<Slot>,
    backend: Option<Arc<Slot>>,
    identity: Arc<Identity>,
    armed: bool,
}
fn same_backend(left: Option<&Arc<Slot>>, right: Option<&Arc<Slot>>) -> bool {
    match (left, right) {
        (None, None) => true,
        (Some(left), Some(right)) => Arc::ptr_eq(left, right),
        _ => false,
    }
}
impl Reservation {
    /// `quiescent` must inspect only synchronous freeze admission. Both slot
    /// guards are released before this function returns and before any await.
    pub(crate) fn reserve(
        namespace: Arc<Slot>,
        backend: Option<Arc<Slot>>,
        quiescent: impl FnOnce() -> bool,
    ) -> Result<Self> {
        if backend
            .as_ref()
            .is_some_and(|slot| Arc::ptr_eq(slot, &namespace))
        {
            return Err(Error::InvalidInput);
        }
        let identity = Arc::new(Identity);
        {
            let mut namespace_state = namespace.state.lock().map_err(|_poison| Error::Io)?;
            if !matches!(*namespace_state, State::Vacant) || !quiescent() {
                return Err(Error::Busy);
            }
            let mut backend_state = backend
                .as_ref()
                .map(|slot| slot.state.lock())
                .transpose()
                .map_err(|_poison| Error::Io)?;
            if backend_state
                .as_ref()
                .is_some_and(|state| !matches!(**state, State::Vacant))
            {
                return Err(Error::Busy);
            }
            *namespace_state = State::Reserved(identity.clone());
            if let Some(state) = backend_state.as_mut() {
                **state = State::Reserved(identity.clone());
            }
        }
        Ok(Self {
            namespace,
            backend,
            identity,
            armed: true,
        })
    }
    /// Call only after every fallible constructor validation has succeeded.
    pub(crate) fn commit(mut self) -> Result<OwnerClaim> {
        {
            let mut namespace = self.namespace.state.lock().map_err(|_poison| Error::Io)?;
            let mut backend = self
                .backend
                .as_ref()
                .map(|slot| slot.state.lock())
                .transpose()
                .map_err(|_poison| Error::Io)?;
            if !reserved(&namespace, &self.identity)
                || backend
                    .as_ref()
                    .is_some_and(|state| !reserved(state, &self.identity))
            {
                return Err(Error::Stale);
            }
            *namespace = State::Committed {
                owner: self.identity.clone(),
                active: self.backend.clone(),
            };
            if let Some(state) = backend.as_mut() {
                **state = State::Committed {
                    owner: self.identity.clone(),
                    active: None,
                };
            }
            self.armed = false;
        }
        Ok(OwnerClaim {
            namespace: self.namespace.clone(),
            backend: self.backend.clone(),
            lineage: Lineage {
                identity: self.identity.clone(),
            },
        })
    }
}
fn reserved(state: &State, identity: &Arc<Identity>) -> bool {
    matches!(state, State::Reserved(actual) if Arc::ptr_eq(actual, identity))
}
fn committed(state: &State, identity: &Arc<Identity>) -> bool {
    matches!(state, State::Committed { owner, .. } if Arc::ptr_eq(owner, identity))
}
fn active_backend(state: &State) -> Option<&Arc<Slot>> {
    match state {
        State::Committed { active, .. } => active.as_ref(),
        _ => None,
    }
}
impl Drop for Reservation {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        // Poisoned or inconsistent ownership fails closed; Drop never repairs it.
        let Ok(mut namespace) = self.namespace.state.lock() else {
            return;
        };
        let mut backend = match self
            .backend
            .as_ref()
            .map(|slot| slot.state.lock())
            .transpose()
        {
            Ok(backend) => backend,
            Err(_poison) => return,
        };
        if !reserved(&namespace, &self.identity)
            || backend
                .as_ref()
                .is_some_and(|state| !reserved(state, &self.identity))
        {
            return;
        }
        *namespace = State::Vacant;
        if let Some(state) = backend.as_mut() {
            **state = State::Vacant;
        }
    }
}
impl Slot {
    /// Run only the atomic freeze acquisition under the same lock as ownership
    /// reservation. This captures identity, not a cached authorization decision.
    pub(crate) fn acquire_freeze(
        &self,
        acquire: impl FnOnce() -> Result<Option<u64>>,
    ) -> Result<(u64, Option<Lineage>)> {
        let state = self.state.lock().map_err(|_poison| Error::Io)?;
        if matches!(*state, State::Reserved(_)) {
            return Err(Error::Busy);
        }
        let token = acquire()?.ok_or(Error::Busy)?;
        let lineage = match &*state {
            State::Committed { owner, .. } => Some(Lineage {
                identity: owner.clone(),
            }),
            State::Vacant => None,
            State::Reserved(_) => unreachable!(),
        };
        Ok((token, lineage))
    }
}
impl OwnerClaim {
    pub(crate) fn lineage(&self) -> Lineage {
        self.lineage.clone()
    }
    /// Validate exact slots and committed identity in namespace-before-backend
    /// order. Neither matching roots nor a shared metadata gate establishes this.
    pub(crate) fn check(&self, namespace: &Arc<Slot>, backend: Option<&Arc<Slot>>) -> Result<()> {
        if !Arc::ptr_eq(&self.namespace, namespace) {
            return Err(Error::Stale);
        }
        let namespace = self.namespace.state.lock().map_err(|_poison| Error::Io)?;
        if !committed(&namespace, &self.lineage.identity)
            || !same_backend(active_backend(&namespace), backend)
        {
            return Err(Error::Stale);
        }
        let active = active_backend(&namespace)
            .map(|slot| slot.state.lock().map_err(|_poison| Error::Io))
            .transpose()?;
        if active
            .as_ref()
            .is_some_and(|state| !committed(state, &self.lineage.identity))
        {
            return Err(Error::Stale);
        }
        Ok(())
    }
    pub(crate) fn check_lineage(&self, lineage: Option<&Lineage>) -> Result<()> {
        let active = {
            let namespace = self.namespace.state.lock().map_err(|_poison| Error::Io)?;
            if !committed(&namespace, &self.lineage.identity) {
                return Err(Error::Stale);
            }
            active_backend(&namespace).cloned()
        };
        self.check(&self.namespace, active.as_ref())?;
        if !lineage.is_some_and(|lineage| Arc::ptr_eq(&self.lineage.identity, &lineage.identity)) {
            return Err(Error::Stale);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
