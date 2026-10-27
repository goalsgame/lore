// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use lore_base::test_util::TempDir;

use super::*;
use crate::fs::filesystem_provider::FsError;
use crate::fs::filesystem_provider::InstanceOperationImpl;

struct Provider {
    registry: Weak<ProviderRegistry>,
    dropped: Arc<AtomicBool>,
}
#[async_trait::async_trait]
impl FilesystemProvider for Provider {
    fn holds_state(&self) -> bool {
        true
    }
    async fn begin_operation(&self) -> std::result::Result<Arc<InstanceOperationImpl>, FsError> {
        Err(FsError::internal(
            "claim fixture has no filesystem operations",
        ))
    }
}
impl Drop for Provider {
    fn drop(&mut self) {
        if let Some(registry) = self.registry.upgrade() {
            assert!(
                registry.state.try_lock().is_some(),
                "provider drops outside registry lock"
            );
        }
        self.dropped.store(true, Ordering::Release);
    }
}
fn fixture() -> (TempDir, Arc<ProviderRegistry>, Registration) {
    let directory = TempDir::new("lore-provider-claim-");
    let registry = ProviderRegistry::new(Limits::default());
    let owner = registry
        .reserve(directory.path(), InstanceId::generate())
        .unwrap();
    (directory, registry, owner)
}
fn install_fixture_provider(
    registry: &Arc<ProviderRegistry>,
    owner: &Registration,
) -> Arc<AtomicBool> {
    let dropped = Arc::new(AtomicBool::new(false));
    registry
        .state
        .lock()
        .entries
        .get_mut(owner.target().path())
        .unwrap()
        .provider = Some(Arc::new(Provider {
        registry: Arc::downgrade(registry),
        dropped: dropped.clone(),
    }));
    dropped
}

#[test]
fn owner_drop_withdraws_readiness_while_retention_pins_preserve_the_reservation() {
    let (_directory, registry, owner) = fixture();
    let target = owner.target().clone();
    let instance = registry.state.lock().entries[target.path()].instance;
    let pin = owner.claim.pin();
    let retained = pin.clone();
    let dropped = install_fixture_provider(&registry, &owner);
    {
        let state = registry.state.lock();
        let entry = &state.entries[target.path()];
        assert!(Arc::ptr_eq(&entry.gate, &owner.claim.gate));
        assert!(Weak::ptr_eq(&entry.claim, &Arc::downgrade(&owner.claim)));
    }
    drop(owner);
    assert!(dropped.load(Ordering::Acquire));
    assert!(matches!(
        registry.resolve(&target, instance),
        Err(Error::Retired)
    ));
    assert!(matches!(
        registry.reserve(target.path(), instance),
        Err(Error::Overlap)
    ));
    drop(pin);
    assert_eq!(registry.state.lock().entries.len(), 1);
    drop(retained);
    assert!(registry.state.lock().entries.is_empty());
    drop(registry.reserve(target.path(), instance).unwrap());
}

#[test]
fn retention_pin_is_the_same_claim_but_cannot_extend_publication_ownership() {
    let (_directory, registry, owner) = fixture();
    let pin = owner.claim.pin();
    assert!(Arc::ptr_eq(&pin._claim, &owner.claim));
    let target = owner.target().clone();
    drop(owner);
    assert!(registry.state.lock().entries[target.path()].retired);
    assert!(matches!(
        pin._claim.gate.admit(),
        Err(bootstrap_gate::Error::Retired)
    ));
    drop(pin);
}

#[test]
fn old_claim_last_drop_never_removes_a_later_registration() {
    let (_directory, registry, owner) = fixture();
    let target = owner.target().clone();
    let old = owner.claim.token;
    let pin = owner.claim.pin();
    registry.remove(&target, old);
    let replacement = registry
        .reserve(target.path(), InstanceId::generate())
        .unwrap();
    assert!(!owner.same_reservation(&replacement));
    assert!(replacement.claim.token > old);
    drop(owner);
    drop(pin);
    let state = registry.state.lock();
    let entry = &state.entries[target.path()];
    assert!(replacement.claim.matches(entry));
    assert!(!entry.retired);
}

#[test]
fn claim_and_registry_drop_after_the_last_pin_without_a_strong_entry_cycle() {
    let (_directory, registry, owner) = fixture();
    let weak_registry = Arc::downgrade(&registry);
    let weak_claim = Arc::downgrade(&owner.claim);
    let pin = owner.claim.pin();
    drop(owner);
    drop(registry);
    assert!(weak_registry.upgrade().is_some());
    assert!(weak_claim.upgrade().is_some());
    drop(pin);
    assert!(weak_claim.upgrade().is_none());
    assert!(weak_registry.upgrade().is_none());
}

#[test]
fn gate_poison_still_withdraws_provider_and_preserves_retired_overlap() {
    let (_directory, registry, owner) = fixture();
    let target = owner.target().clone();
    let instance = registry.state.lock().entries[target.path()].instance;
    let pin = owner.claim.pin();
    let dropped = install_fixture_provider(&registry, &owner);
    owner.claim.gate.test_poison();
    assert!(matches!(owner.retire(), Err(Error::Poisoned)));
    assert!(dropped.load(Ordering::Acquire));
    assert!(registry.state.lock().entries[target.path()].retired);
    assert!(matches!(
        registry.resolve(&target, instance),
        Err(Error::Retired)
    ));
    drop(owner);
    assert!(matches!(
        registry.reserve(target.path(), instance),
        Err(Error::Overlap)
    ));
    drop(pin);
    assert!(registry.state.lock().entries.is_empty());
}

#[cfg(unix)]
#[test]
fn readonly_owner_identity_and_reserved_location_grant_no_readiness() {
    let (_directory, registry, owner) = fixture();
    let metadata = TempDir::new("lore-provider-claim-metadata-");
    owner
        .reserve_metadata(metadata.path(), [71; 16].into())
        .unwrap();
    assert!(owner.same_reservation(&owner));
    let location = owner.bootstrap_location().unwrap();
    assert_eq!(location.target(), owner.target());
    assert_eq!(location.metadata().path(), metadata.path());
    assert!(matches!(
        registry.resolve_location(&location),
        Err(Error::NotReady)
    ));
    let other_registry = ProviderRegistry::new(Limits::default());
    let other = other_registry
        .reserve(owner.target().path(), location.instance())
        .unwrap();
    assert!(!owner.same_reservation(&other));
    owner.retire().unwrap();
    assert!(matches!(owner.bootstrap_location(), Err(Error::Retired)));
}
