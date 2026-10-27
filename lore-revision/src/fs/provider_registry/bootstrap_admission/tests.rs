// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

#![cfg(unix)]
#![allow(clippy::disallowed_methods)]

use std::sync::Weak;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use lore_base::test_util::TempDir;

use super::*;
use crate::fs::filesystem_provider::FsError;
use crate::fs::filesystem_provider::InstanceOperationImpl;
use crate::repository::RepositoryConfig;
use crate::repository::VfsConfig;
use crate::repository::VfsType;

struct Provider {
    registry: Weak<ProviderRegistry>,
    dropped: Arc<AtomicBool>,
}
#[async_trait::async_trait]
impl FilesystemProvider for Provider {
    async fn read_metadata_file(
        &self,
        path: &crate::util::path::RelativePath,
        _maximum: usize,
    ) -> std::result::Result<Vec<u8>, FsError> {
        Err(lore_base::error::FileNotFound {
            resource: path.to_string(),
        }
        .into())
    }
    fn holds_state(&self) -> bool {
        true
    }
    async fn begin_operation(&self) -> std::result::Result<Arc<InstanceOperationImpl>, FsError> {
        Err(FsError::internal("bootstrap fixture has no operations"))
    }
}
impl Drop for Provider {
    fn drop(&mut self) {
        if let Some(registry) = self.registry.upgrade() {
            assert!(
                registry
                    .state
                    .try_lock_for(std::time::Duration::from_secs(5))
                    .is_some(),
                "provider destruction cannot retain the registry lock"
            );
        }
        self.dropped.store(true, Ordering::Release);
    }
}
pub(super) struct Fixture {
    _target: TempDir,
    _metadata: TempDir,
    pub(super) registry: Arc<ProviderRegistry>,
    pub(super) owner: Registration,
    pub(super) location: WorkingLocation,
    dropped: Arc<AtomicBool>,
}
pub(super) fn fixture() -> Fixture {
    fixture_with_ready(true)
}
fn fixture_with_ready(ready: bool) -> Fixture {
    use std::os::unix::fs::PermissionsExt;
    let target = TempDir::new("lore-bootstrap-bound-target-");
    let metadata = TempDir::new("lore-bootstrap-bound-metadata-");
    std::fs::set_permissions(metadata.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let instance = InstanceId::generate();
    let repository: RepositoryId = [83; 16].into();
    let config = RepositoryConfig {
        vfs: Some(VfsConfig {
            vfs_type: VfsType::ExperimentalWorking,
        }),
        ..Default::default()
    };
    for (name, bytes) in [
        (crate::repository::ID, repository.data().to_vec()),
        (crate::repository::INSTANCE, instance.data().to_vec()),
        (
            crate::repository::CONFIG,
            toml::to_string(&config).unwrap().into_bytes(),
        ),
    ] {
        let path = metadata.path().join(name);
        std::fs::write(&path, bytes).unwrap();
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
    let registry = ProviderRegistry::new(Limits::default());
    let owner = registry.reserve(target.path(), instance).unwrap();
    owner.reserve_metadata(metadata.path(), repository).unwrap();
    let location = owner.bootstrap_location().unwrap();
    let dropped = Arc::new(AtomicBool::new(false));
    if ready {
        owner
            .publish(Arc::new(Provider {
                registry: Arc::downgrade(&registry),
                dropped: dropped.clone(),
            }))
            .unwrap();
    }
    Fixture {
        _target: target,
        _metadata: metadata,
        registry,
        owner,
        location,
        dropped,
    }
}

#[test]
fn bound_load_retains_exact_claim_after_publication_owner_retires() {
    let fixture = fixture();
    let (provider, load) = resolve(&fixture.registry, &fixture.location)
        .unwrap()
        .into_parts();
    assert_eq!(load.location(), &fixture.location);
    let pin = load.pin();
    drop(fixture.owner);
    assert!(matches!(
        resolve(&fixture.registry, &fixture.location),
        Err(Error::Retired)
    ));
    assert!(!fixture.dropped.load(Ordering::Acquire));
    drop(provider);
    assert!(fixture.dropped.load(Ordering::Acquire));
    drop(load);
    assert_eq!(fixture.registry.state.lock().entries.len(), 1);
    drop(pin);
    assert!(fixture.registry.state.lock().entries.is_empty());
}

#[test]
fn inherited_pins_share_one_principal_but_explicit_resolutions_are_distinct() {
    let fixture = fixture();
    let (_, load) = resolve(&fixture.registry, &fixture.location)
        .unwrap()
        .into_parts();
    let pin = load.pin();
    let exclusive = fixture
        .owner
        .claim
        .gate
        .try_exclusive(&load.principal)
        .unwrap();
    assert!(matches!(
        resolve(&fixture.registry, &fixture.location),
        Err(Error::AdmissionBusy)
    ));
    drop(exclusive);
    let (_, sibling) = resolve(&fixture.registry, &fixture.location)
        .unwrap()
        .into_parts();
    assert!(matches!(
        fixture.owner.claim.gate.try_exclusive(&load.principal),
        Err(bootstrap_gate::Error::Busy)
    ));
    drop(sibling);
    drop(load);
    assert!(
        fixture
            .owner
            .claim
            .gate
            .try_exclusive(&pin.principal)
            .is_ok()
    );
}

#[test]
fn wrong_registry_and_changed_location_are_not_admitted() {
    let fixture = fixture();
    let other = ProviderRegistry::new(Limits::default());
    assert!(matches!(
        resolve(&other, &fixture.location),
        Err(Error::Stale)
    ));
    let mut changed = fixture.location.clone();
    changed.registry_identity = Arc::new(());
    assert!(matches!(
        resolve(&fixture.registry, &changed),
        Err(Error::Stale)
    ));
    changed = fixture.location.clone();
    changed.token += 1;
    assert!(matches!(
        resolve(&fixture.registry, &changed),
        Err(Error::Stale)
    ));
}

#[test]
fn replaced_entry_cannot_accept_an_old_location_or_claim() {
    let fixture = fixture();
    fixture
        .registry
        .remove(fixture.owner.target(), fixture.owner.claim.token);
    let replacement = fixture
        .registry
        .reserve(fixture.owner.target().path(), fixture.location.instance())
        .unwrap();
    assert!(matches!(
        resolve(&fixture.registry, &fixture.location),
        Err(Error::Stale)
    ));
    assert!(!fixture.owner.same_reservation(&replacement));
}

#[test]
fn bound_resolution_checks_metadata_before_admission() {
    let fixture = fixture();
    std::fs::write(
        fixture
            .location
            .metadata()
            .path()
            .join(crate::repository::INSTANCE),
        [0; 16],
    )
    .unwrap();
    assert!(matches!(
        resolve(&fixture.registry, &fixture.location),
        Err(Error::InvalidMetadata)
    ));
    // No failed-resolution principal consumes the per-root load bound.
    let lease = fixture.owner.claim.gate.admit().unwrap();
    assert!(fixture.owner.claim.gate.try_exclusive(&lease).is_ok());
}

#[test]
fn gate_poison_and_capacity_fail_closed_without_retaining_failed_loads() {
    let fixture = fixture();
    let mut loads = Vec::new();
    for _ in 0..MAX_BOOTSTRAP_LOADS_PER_ROOT {
        loads.push(resolve(&fixture.registry, &fixture.location).unwrap());
    }
    assert!(matches!(
        resolve(&fixture.registry, &fixture.location),
        Err(Error::Capacity)
    ));
    loads.pop();
    assert!(resolve(&fixture.registry, &fixture.location).is_ok());
    drop(loads);
    fixture.owner.claim.gate.test_poison();
    assert!(matches!(
        resolve(&fixture.registry, &fixture.location),
        Err(Error::Poisoned)
    ));
    fixture.owner.retire().unwrap_err();
    assert!(fixture.dropped.load(Ordering::Acquire));
}

#[test]
fn concurrent_retirement_and_resolution_never_reopen_the_entry() {
    let fixture = fixture();
    let owner = Arc::new(fixture.owner);
    let barrier = Arc::new(std::sync::Barrier::new(2));
    let retire = {
        let owner = owner.clone();
        let barrier = barrier.clone();
        std::thread::spawn(move || {
            barrier.wait();
            owner.retire().unwrap();
        })
    };
    barrier.wait();
    let attempted = resolve(&fixture.registry, &fixture.location);
    retire.join().unwrap();
    assert!(attempted.is_ok() || matches!(attempted, Err(Error::Retired)));
    assert!(matches!(
        resolve(&fixture.registry, &fixture.location),
        Err(Error::Retired)
    ));
    drop(owner);
    assert_eq!(
        fixture.registry.state.lock().entries.len(),
        usize::from(attempted.is_ok())
    );
    drop(attempted);
    assert!(fixture.registry.state.lock().entries.is_empty());
}

struct LegacyResolver;
impl ProviderResolver for LegacyResolver {
    fn locate(&self, _target: &CanonicalTarget) -> Result<Option<WorkingLocation>> {
        Ok(None)
    }
    fn resolve_location(&self, _location: &WorkingLocation) -> Result<Arc<dyn FilesystemProvider>> {
        Err(Error::NotReady)
    }
    fn resolve(
        &self,
        _target: &CanonicalTarget,
        _instance: InstanceId,
    ) -> Result<Arc<dyn FilesystemProvider>> {
        Err(Error::NotReady)
    }
}
#[test]
fn resolver_without_bound_admission_support_fails_closed() {
    let fixture = fixture();
    assert!(matches!(
        LegacyResolver.resolve_bootstrap(&fixture.location),
        Err(Error::UnsupportedResolver)
    ));
}

#[test]
fn pending_registration_is_rejected_before_metadata_inspection() {
    let fixture = fixture_with_ready(false);
    std::fs::remove_file(
        fixture
            .location
            .metadata()
            .path()
            .join(crate::repository::CONFIG),
    )
    .unwrap();
    assert!(matches!(
        fixture.registry.resolve_bootstrap(&fixture.location),
        Err(Error::NotReady)
    ));
    let principal = fixture.owner.claim.gate.admit().unwrap();
    assert!(fixture.owner.claim.gate.try_exclusive(&principal).is_ok());
}
