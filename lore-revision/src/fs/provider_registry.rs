// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Process-local routing to service-owned working providers. Reserving a target
//! does not publish a usable provider. Registration leases revoke future resolution
//! on drop; callers that already resolved a provider retain their own `Arc`.

use std::collections::BTreeMap;
use std::fmt;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use lore_base::types::RepositoryId;
use parking_lot::Mutex;

mod bootstrap_admission;
pub use bootstrap_admission::BoundLoad;
pub use bootstrap_admission::BoundWork;
pub(crate) use bootstrap_admission::CommandExclusion;
pub use bootstrap_admission::ResolvedBootstrap;
mod bootstrap_gate;
mod claim;
#[cfg(unix)]
pub(crate) use bootstrap_admission::pending::PendingQualification;
#[cfg(unix)]
pub(crate) use bootstrap_admission::pending::qualify as qualify_pending_metadata;
use claim::RegistrationClaim;
mod metadata;
const MAX_BOOTSTRAP_LOADS_PER_ROOT: usize = 64;
const MAX_BOOTSTRAP_WORK_PER_ROOT: usize = 256;

#[cfg(unix)]
pub(crate) const INITIALIZING_METADATA: &str = ".working-initializing";
#[cfg(unix)]
pub(crate) const INITIALIZING_STORES: &str = ".working-store-initializing";

use super::filesystem_provider::FilesystemProvider;
use crate::instance::InstanceId;

#[derive(Debug)]
pub enum Error {
    Io(std::io::Error),
    InvalidTarget,
    InvalidInstance,
    Capacity,
    Overlap,
    Missing,
    WrongInstance,
    NotReady,
    Stale,
    AlreadyPublished,
    Retired,
    InvalidProvider,
    TokenExhausted,
    InvalidMetadata,
    MetadataAlreadyReserved,
    UnsupportedPlatform,
    Poisoned,
    AdmissionBusy,
    UnsupportedResolver,
}
impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "working provider target: {error}"),
            Self::InvalidTarget => {
                formatter.write_str("working provider target must be an existing bounded directory")
            }
            Self::InvalidInstance => {
                formatter.write_str("working provider requires a valid nonzero instance ID")
            }
            Self::Capacity => formatter.write_str("working provider registry capacity exceeded"),
            Self::Overlap => {
                formatter.write_str("working provider target overlaps an existing reservation")
            }
            Self::Missing => {
                formatter.write_str("experimental working instance has no running owner")
            }
            Self::WrongInstance => {
                formatter.write_str("working provider owner belongs to another instance ID")
            }
            Self::NotReady => formatter.write_str("working provider owner is not ready"),
            Self::Stale => formatter.write_str("working provider registration is stale"),
            Self::AlreadyPublished => {
                formatter.write_str("working provider registration is already published")
            }
            Self::Retired => formatter.write_str("working provider registration is retiring"),
            Self::InvalidProvider => {
                formatter.write_str("working provider must own its filesystem state")
            }
            Self::TokenExhausted => {
                formatter.write_str("working provider registration identity exhausted")
            }
            Self::InvalidMetadata => formatter.write_str("registered working metadata is unsafe, corrupt, or belongs to another instance/repository"),
            Self::MetadataAlreadyReserved => formatter.write_str("working metadata is already reserved"),
            Self::UnsupportedPlatform => formatter.write_str("experimental working metadata is unsupported on this platform"),
            Self::Poisoned => formatter.write_str("working provider bootstrap admission is poisoned"),
            Self::AdmissionBusy => formatter.write_str("working provider bootstrap admission is busy"),
            Self::UnsupportedResolver => formatter.write_str("working provider resolver does not support bound bootstrap admission"),
        }
    }
}
impl std::error::Error for Error {}
pub type Result<T> = std::result::Result<T, Error>;

/// An existing directory's canonical absolute path. This identifies a routing
/// target; it does not confer repository or filesystem authorization.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CanonicalTarget(PathBuf);
impl CanonicalTarget {
    /// Performs blocking filesystem queries; call before taking registry locks.
    pub fn resolve(path: &Path) -> Result<Self> {
        let path = std::fs::canonicalize(path).map_err(Error::Io)?;
        if !path.is_absolute() || !std::fs::metadata(&path).map_err(Error::Io)?.is_dir() {
            return Err(Error::InvalidTarget);
        }
        Ok(Self(path))
    }
    pub fn path(&self) -> &Path {
        &self.0
    }
}

/// Immutable routing metadata tied to one registration token. Repository loads
/// must resolve this exact location again after reading its identity files.
#[derive(Clone, Debug)]
pub struct WorkingLocation {
    registry_identity: Arc<()>,
    target: CanonicalTarget,
    metadata: CanonicalTarget,
    repository: RepositoryId,
    instance: InstanceId,
    token: u64,
}
impl PartialEq for WorkingLocation {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.registry_identity, &other.registry_identity)
            && self.target == other.target
            && self.metadata == other.metadata
            && self.repository == other.repository
            && self.instance == other.instance
            && self.token == other.token
    }
}
impl Eq for WorkingLocation {}
impl WorkingLocation {
    pub fn target(&self) -> &CanonicalTarget {
        &self.target
    }
    pub fn metadata(&self) -> &CanonicalTarget {
        &self.metadata
    }
    pub fn repository(&self) -> RepositoryId {
        self.repository
    }
    pub fn instance(&self) -> InstanceId {
        self.instance
    }
}

/// Inject this boundary into each service command's execution context. Resolution
/// never creates a provider or falls back to the OS filesystem.
pub trait ProviderResolver: Send + Sync {
    /// Locate a registered root or ancestor. Registered pending/invalid entries
    /// return errors; only an unregistered ordinary target returns `None`.
    fn locate(&self, target: &CanonicalTarget) -> Result<Option<WorkingLocation>>;
    fn resolve_location(&self, location: &WorkingLocation) -> Result<Arc<dyn FilesystemProvider>>;
    /// Admit one load against an exact ready registration. This does not admit
    /// provider/store operations or confer a Client token or exclusive handoff.
    fn resolve_bootstrap(&self, _location: &WorkingLocation) -> Result<ResolvedBootstrap> {
        Err(Error::UnsupportedResolver)
    }
    fn resolve(
        &self,
        target: &CanonicalTarget,
        instance: InstanceId,
    ) -> Result<Arc<dyn FilesystemProvider>>;
}

#[derive(Clone, Copy, Debug)]
pub struct Limits {
    pub max_registrations: usize,
    pub max_target_bytes: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            max_registrations: 128,
            max_target_bytes: 4096,
        }
    }
}
struct Entry {
    instance: InstanceId,
    token: u64,
    claim: std::sync::Weak<RegistrationClaim>,
    gate: Arc<bootstrap_gate::Gate>,
    provider: Option<Arc<dyn FilesystemProvider>>,
    location: Option<WorkingLocation>,
    retired: bool,
}
struct State {
    entries: BTreeMap<PathBuf, Entry>,
    next_token: u64,
}
pub struct ProviderRegistry {
    identity: Arc<()>,
    limits: Limits,
    state: Mutex<State>,
}
impl ProviderRegistry {
    pub fn new(limits: Limits) -> Arc<Self> {
        Arc::new(Self {
            identity: Arc::new(()),
            limits,
            state: Mutex::new(State {
                entries: BTreeMap::new(),
                next_token: 1,
            }),
        })
    }
    /// Reserve an existing directory before asynchronous owner setup. Both ready
    /// and pending targets count toward the same bound and overlap checks.
    pub fn reserve(self: &Arc<Self>, path: &Path, instance: InstanceId) -> Result<Registration> {
        if instance.is_zero() {
            return Err(Error::InvalidInstance);
        }
        let target = CanonicalTarget::resolve(path)?;
        self.check_target(&target)?;
        let mut state = self.state.lock();
        if state.entries.iter().any(|(path, entry)| {
            overlaps(path, &target.0)
                || entry
                    .location
                    .as_ref()
                    .is_some_and(|location| overlaps(location.metadata.path(), &target.0))
        }) {
            return Err(Error::Overlap);
        }
        if state.entries.len() >= self.limits.max_registrations {
            return Err(Error::Capacity);
        }
        let token = state.next_token;
        let next = token.checked_add(1).ok_or(Error::TokenExhausted)?;
        let gate = bootstrap_gate::Gate::new(bootstrap_gate::Limits {
            max_loads: MAX_BOOTSTRAP_LOADS_PER_ROOT,
            max_work: MAX_BOOTSTRAP_WORK_PER_ROOT,
        })
        .expect("nonzero private bootstrap limits");
        // No fallible tail follows actual claim creation under the registry lock.
        let claim = RegistrationClaim::new(self.clone(), target.clone(), token, gate.clone());
        state.entries.insert(
            target.0.clone(),
            Entry {
                instance,
                token,
                claim: Arc::downgrade(&claim),
                gate,
                provider: None,
                location: None,
                retired: false,
            },
        );
        state.next_token = next;
        Ok(Registration { claim })
    }
    fn check_target(&self, target: &CanonicalTarget) -> Result<()> {
        if target.0.as_os_str().len() > self.limits.max_target_bytes {
            return Err(Error::InvalidTarget);
        }
        Ok(())
    }
    #[lore_macro::test_pub]
    fn remove(&self, target: &CanonicalTarget, token: u64) {
        let removed = {
            let mut state = self.state.lock();
            if state
                .entries
                .get(&target.0)
                .is_some_and(|entry| entry.token == token)
            {
                state.entries.remove(&target.0)
            } else {
                None
            }
        };
        // Provider destruction may reenter service code.
        drop(removed);
    }
}
fn overlaps(first: &Path, second: &Path) -> bool {
    first.starts_with(second) || second.starts_with(first)
}
fn current_location<'a>(state: &'a State, location: &WorkingLocation) -> Result<&'a Entry> {
    let entry = state
        .entries
        .get(location.target.path())
        .filter(|entry| {
            entry.token == location.token
                && entry.instance == location.instance
                && entry.location.as_ref() == Some(location)
        })
        .ok_or(Error::Stale)?;
    if entry.retired {
        return Err(Error::Retired);
    }
    Ok(entry)
}
impl ProviderResolver for ProviderRegistry {
    fn resolve_bootstrap(&self, location: &WorkingLocation) -> Result<ResolvedBootstrap> {
        bootstrap_admission::resolve(self, location)
    }
    fn locate(&self, target: &CanonicalTarget) -> Result<Option<WorkingLocation>> {
        self.check_target(target)?;
        let location = {
            let state = self.state.lock();
            let Some((_, entry)) = state
                .entries
                .iter()
                .find(|(path, _)| target.path().starts_with(path))
            else {
                if state.entries.values().any(|entry| {
                    entry
                        .location
                        .as_ref()
                        .is_some_and(|location| target.path().starts_with(location.metadata.path()))
                }) {
                    return Err(Error::InvalidTarget);
                }
                return Ok(None);
            };
            if entry.retired {
                return Err(Error::Retired);
            }
            if entry.provider.is_none() {
                return Err(Error::NotReady);
            }
            entry.location.clone().ok_or(Error::NotReady)?
        };
        metadata::verify(&location)?;
        let state = self.state.lock();
        if current_location(&state, &location)?.provider.is_none() {
            return Err(Error::NotReady);
        }
        Ok(Some(location))
    }
    fn resolve_location(&self, location: &WorkingLocation) -> Result<Arc<dyn FilesystemProvider>> {
        {
            let state = self.state.lock();
            if current_location(&state, location)?.provider.is_none() {
                return Err(Error::NotReady);
            }
        }
        metadata::verify(location)?;
        let state = self.state.lock();
        current_location(&state, location)?
            .provider
            .clone()
            .ok_or(Error::NotReady)
    }
    fn resolve(
        &self,
        target: &CanonicalTarget,
        instance: InstanceId,
    ) -> Result<Arc<dyn FilesystemProvider>> {
        if instance.is_zero() {
            return Err(Error::InvalidInstance);
        }
        self.check_target(target)?;
        let location = {
            let state = self.state.lock();
            let entry = state.entries.get(target.path()).ok_or(Error::Missing)?;
            if entry.instance != instance {
                return Err(Error::WrongInstance);
            }
            if entry.retired {
                return Err(Error::Retired);
            }
            if entry.provider.is_none() {
                return Err(Error::NotReady);
            }
            entry.location.clone().ok_or(Error::NotReady)?
        };
        self.resolve_location(&location)
    }
}

/// Publication owner for one exact reservation. Dropping it withdraws readiness;
/// retention-only pins keep the retired reservation until their work finishes.
pub struct Registration {
    claim: Arc<RegistrationClaim>,
}
impl Registration {
    pub fn belongs_to(&self, registry: &Arc<ProviderRegistry>) -> bool {
        Arc::ptr_eq(&self.claim.registry, registry)
    }
    pub fn target(&self) -> &CanonicalTarget {
        &self.claim.target
    }
    pub(crate) fn pending_location(&self) -> Result<WorkingLocation> {
        let state = self.claim.registry.state.lock();
        let entry = state
            .entries
            .get(self.claim.target.path())
            .filter(|entry| self.claim.matches(entry))
            .ok_or(Error::Stale)?;
        if entry.retired {
            return Err(Error::Retired);
        }
        if entry.provider.is_some() {
            return Err(Error::AlreadyPublished);
        }
        entry.location.clone().ok_or(Error::NotReady)
    }
    pub(crate) fn check_pending_location(&self, location: &WorkingLocation) -> Result<()> {
        if self.pending_location()? != *location {
            return Err(Error::Stale);
        }
        Ok(())
    }
    /// Bind an external metadata directory before asynchronous owner setup. Its
    /// identity files may be initialized later; publication verifies them all.
    pub fn reserve_metadata(&self, path: &Path, repository: RepositoryId) -> Result<()> {
        if repository.is_zero() {
            return Err(Error::InvalidMetadata);
        }
        metadata::target_owner(&self.claim.target)?;
        let metadata = metadata::directory(path)?;
        self.claim.registry.check_target(&metadata)?;
        let mut state = self.claim.registry.state.lock();
        let entry = state
            .entries
            .get(self.claim.target.path())
            .filter(|entry| self.claim.matches(entry))
            .ok_or(Error::Stale)?;
        if entry.retired {
            return Err(Error::Retired);
        }
        if entry.location.is_some() {
            return Err(Error::MetadataAlreadyReserved);
        }
        if state.entries.iter().any(|(target, entry)| {
            overlaps(target, metadata.path())
                || entry
                    .location
                    .as_ref()
                    .is_some_and(|location| overlaps(location.metadata.path(), metadata.path()))
        }) {
            return Err(Error::Overlap);
        }
        let instance = entry.instance;
        state
            .entries
            .get_mut(self.claim.target.path())
            .ok_or(Error::Stale)?
            .location = Some(WorkingLocation {
            registry_identity: self.claim.registry.identity.clone(),
            target: self.claim.target.clone(),
            metadata,
            repository,
            instance,
            token: self.claim.token,
        });
        Ok(())
    }
    pub fn publish(&self, provider: Arc<dyn FilesystemProvider>) -> Result<()> {
        {
            let state = self.claim.registry.state.lock();
            let entry = state
                .entries
                .get(self.claim.target.path())
                .filter(|entry| self.claim.matches(entry))
                .ok_or(Error::Stale)?;
            if entry.retired {
                return Err(Error::Retired);
            }
            entry
                .gate
                .check_ready()
                .map_err(bootstrap_admission::admission_error)?;
        }
        // Provider code is never invoked while holding the registry lock.
        if !provider.holds_state() {
            return Err(Error::InvalidProvider);
        }
        let location = self.pending_location()?;
        metadata::verify(&location)?;
        let mut state = self.claim.registry.state.lock();
        current_location(&state, &location)?;
        let entry = state
            .entries
            .get_mut(&self.claim.target.0)
            .filter(|entry| self.claim.matches(entry))
            .ok_or(Error::Stale)?;
        if entry.provider.is_some() {
            return Err(Error::AlreadyPublished);
        }
        entry
            .gate
            .check_ready()
            .map_err(bootstrap_admission::admission_error)?;
        entry.provider = Some(provider);
        Ok(())
    }
    /// Withdraw future resolution while retaining the exact path/metadata
    /// reservation. Existing provider Arcs require their own admission fence.
    /// Retirement is terminal. The last exact claim pin removes the reservation.
    pub fn retire(&self) -> Result<()> {
        self.claim.retire()
    }
    /// Compare the exact reservation lifetime without exposing its token.
    pub fn same_reservation(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.claim, &other.claim)
    }
    /// Return the pending reserved descriptor. This is not metadata readiness,
    /// recovery validation, or permission to open stores.
    pub fn bootstrap_location(&self) -> Result<WorkingLocation> {
        self.pending_location()
    }
    #[cfg(feature = "test-util")]
    pub fn test_token(&self) -> u64 {
        self.claim.token
    }
}
impl Drop for Registration {
    fn drop(&mut self) {
        let _retired = self.claim.retire();
    }
}

#[cfg(feature = "test-util")]
impl ProviderRegistry {
    pub fn test_exhaust_tokens(&self) {
        self.state.lock().next_token = u64::MAX;
    }
    pub fn test_is_empty(&self) -> bool {
        self.state.lock().entries.is_empty()
    }
    pub fn test_insert_unbound_provider(
        &self,
        path: PathBuf,
        instance: InstanceId,
        provider: Arc<dyn FilesystemProvider>,
    ) {
        let gate = bootstrap_gate::Gate::new(bootstrap_gate::Limits {
            max_loads: 1,
            max_work: 1,
        })
        .unwrap();
        self.state.lock().entries.insert(
            path,
            Entry {
                instance,
                token: 1,
                claim: std::sync::Weak::new(),
                gate,
                provider: Some(provider),
                location: None,
                retired: false,
            },
        );
    }
}
