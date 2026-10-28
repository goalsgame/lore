// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

use std::sync::Arc;

use lore_base::types::BranchId;
use lore_base::types::Hash;
#[cfg(unix)]
use tokio::sync::Semaphore;

use super::Error;
use super::PreparedMetadata;
use super::Result;
use crate::fs::provider_registry::Registration;
use crate::fs::provider_registry::WorkingLocation;
use crate::repository::RepositoryContext;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Selection {
    pub branch: BranchId,
    pub branch_name: String,
    pub revision: Hash,
}
pub enum StoreMode {
    CreateNew(Selection),
    #[cfg(unix)]
    Import(Selection, super::ImmutableImport),
    Reuse,
    #[cfg(unix)]
    ReuseWithPolicy(super::ImportReadPolicy),
    #[cfg(unix)]
    RemoteCreate(Selection, super::RemoteContents),
    #[cfg(unix)]
    ReuseRemote(super::RemoteContents),
}

#[cfg(feature = "test-util")]
type MetadataPause = Box<dyn FnOnce() + Send>;

/// Prepares normal disk stores and instance anchors without publishing a provider.
/// Existing stores must use current formats and have no pending layout recovery.
#[lore_macro::test_pub]
pub struct StoreInitializer {
    #[cfg(unix)]
    jobs: Arc<Semaphore>,
    #[cfg(feature = "test-util")]
    before_metadata: std::sync::Mutex<Option<MetadataPause>>,
}
impl StoreInitializer {
    pub fn new(max_jobs: usize) -> Result<Arc<Self>> {
        if max_jobs == 0 || max_jobs > 4096 {
            return Err(Error::InvalidLimit);
        }
        Ok(Arc::new(Self {
            #[cfg(unix)]
            jobs: Arc::new(Semaphore::new(max_jobs)),
            #[cfg(feature = "test-util")]
            before_metadata: std::sync::Mutex::default(),
        }))
    }
    #[cfg(unix)]
    pub(super) async fn prepare_bound(
        &self,
        metadata: PreparedMetadata,
        mode: StoreMode,
        expected_backing_root: std::path::PathBuf,
    ) -> Result<PreparedWorkingContext> {
        Box::pin(platform::prepare(
            self,
            metadata,
            mode,
            Some(expected_backing_root),
        ))
        .await
    }

    /// Installs one private test pause inside the actual blocking recheck job.
    #[cfg(feature = "test-util")]
    pub fn pause_metadata_recheck(&self, pause: impl FnOnce() + Send + 'static) {
        *self.before_metadata.lock().expect("metadata pause lock") = Some(Box::new(pause));
    }
    pub async fn prepare(
        &self,
        metadata: PreparedMetadata,
        mode: StoreMode,
    ) -> Result<PreparedWorkingContext> {
        #[cfg(unix)]
        {
            Box::pin(platform::prepare(self, metadata, mode, None)).await
        }
        #[cfg(not(unix))]
        {
            let _unused = (metadata, mode);
            Err(Error::Unsupported)
        }
    }
}

/// Unpublished context with no working filesystem, command token or repository lock.
/// Preparing stores does not authorize or start a mounted owner.
pub struct PreparedWorkingContext {
    /// Detached owner context used for transport and backend tasks.
    pub execution: Arc<crate::interface::ExecutionContext>,
    pub registration: Arc<Registration>,
    pub location: WorkingLocation,
    pub repository: Arc<RepositoryContext>,
    pub selection: Selection,
}

#[cfg(unix)]
mod platform {
    use std::os::unix::ffi::OsStrExt;
    use std::path::Path;
    use std::sync::Mutex;

    use lore_base::lore_spawn_blocking_nocontext;
    use lore_io::lease::IoLease;
    use lore_storage::local::immutable_store::ImmutableStoreCreateOptions;
    use lore_storage::local::mutable_store::MutableStoreVersion;
    use lore_storage::local::store_lock::Intent;
    use lore_storage::local::store_lock::StoreGuard;
    use lore_storage::local::store_lock::StoreLock;
    use tokio::sync::OwnedSemaphorePermit;

    use super::*;
    use crate::branch;
    use crate::fs::invalid_filesystem_provider::InvalidFilesystemProvider;
    use crate::fs::provider_registry::INITIALIZING_STORES;
    use crate::instance;
    use crate::repository::RemoteState;
    use crate::repository::RepositoryConfig;
    use crate::repository::RepositoryPaths;
    use crate::repository::RepositoryWriteToken;
    use crate::repository::{self};
    use crate::state::State;

    // Claims/stores drop before the root token and registration. No returned context
    // captures this owner; submitted IO and finite spawned work retain it separately.
    struct Lease {
        retained: Mutex<Retained>,
        metadata: PreparedMetadata,
        _permit: OwnedSemaphorePermit,
        _metadata_parent: Option<IoLease>,
        _caller_parent: Option<IoLease>,
        // Last field: report completion only after every retained capability drops.
        _completion: Completion,
    }
    impl Lease {
        fn new(
            mut metadata: PreparedMetadata,
            permit: OwnedSemaphorePermit,
            completion: tokio::sync::oneshot::Sender<()>,
            caller_parent: Option<IoLease>,
        ) -> Self {
            let retained = Mutex::default();
            // No callback, allocation or fallible work may follow taking this
            // owner until all fields are in their ordered payload again.
            let metadata_parent = metadata.parent.take();
            Self {
                retained,
                metadata,
                _permit: permit,
                _metadata_parent: metadata_parent,
                _caller_parent: caller_parent,
                _completion: Completion(Some(completion)),
            }
        }
    }
    struct Completion(Option<tokio::sync::oneshot::Sender<()>>);
    impl Drop for Completion {
        fn drop(&mut self) {
            if let Some(sender) = self.0.take() {
                let _closed = sender.send(());
            }
        }
    }
    #[derive(Default)]
    struct Retained {
        stores: Option<(
            Arc<dyn crate::store::ImmutableStore>,
            Arc<dyn crate::store::MutableStore>,
        )>,
        repository_lock: Option<Arc<repository::RepositoryLock>>,
        guards: Vec<StoreGuard>,
    }
    fn failure(error: impl std::fmt::Display) -> Error {
        Error::Preparation(error.to_string())
    }
    fn retain_guard(lease: &Lease, guard: StoreGuard) -> Result<usize> {
        let mut retained = lease.retained.lock().map_err(|_poisoned| Error::Runtime)?;
        retained.guards.push(guard);
        Ok(retained.guards.len() - 1)
    }

    pub(super) async fn prepare(
        initializer: &StoreInitializer,
        metadata: PreparedMetadata,
        mode: StoreMode,
        expected_backing_root: Option<std::path::PathBuf>,
    ) -> Result<PreparedWorkingContext> {
        if let Some(root) = expected_backing_root.as_ref() {
            let bytes = root.as_os_str().as_bytes();
            if !root.is_absolute()
                || bytes.is_empty()
                || bytes.contains(&0)
                || bytes.len() > super::super::backing_binding::MAX_PATH
            {
                return Err(Error::InvalidBinding);
            }
        }
        let caller =
            crate::runtime::try_execution_context().ok_or(Error::MissingExecutionContext)?;
        if !caller.is_client() || caller.globals().in_memory() || caller.globals().dry_run() {
            return Err(Error::MissingExecutionContext);
        }
        let remote_contents = match &mode {
            StoreMode::RemoteCreate(_, contents) | StoreMode::ReuseRemote(contents) => {
                Some(contents)
            }
            _ => None,
        };
        if let Some(contents) = remote_contents {
            contents.validate(metadata.location.repository(), &metadata.config, &caller)?;
        }
        if expected_backing_root.is_some()
            && (metadata
                .config
                .shared_store_to_use
                .as_ref()
                .is_some_and(|shared| shared.use_shared_store == Some(true))
                || (remote_contents.is_none()
                    && !caller.globals().offline()
                    && metadata
                        .config
                        .remote_url
                        .as_deref()
                        .is_some_and(|url| !url.is_empty())))
        {
            return Err(Error::UnsupportedBoundStores);
        }
        if let StoreMode::Import(selection, seed) = &mode
            && (seed.repository_id() != metadata.location.repository()
                || seed.revision() != selection.revision)
        {
            return Err(Error::InvalidSelection);
        }
        if let StoreMode::ReuseWithPolicy(policy) = &mode
            && policy.repository_id() != metadata.location.repository()
        {
            return Err(Error::InvalidSelection);
        }
        let execution = Arc::new(caller.for_background_owner().await);
        if let StoreMode::CreateNew(selection)
        | StoreMode::Import(selection, _)
        | StoreMode::RemoteCreate(selection, _) = &mode
            && (selection.branch.is_zero()
                || selection.branch_name.len() > 255
                || !branch::is_valid_name(&selection.branch_name))
        {
            return Err(Error::InvalidSelection);
        }
        let permit = initializer
            .jobs
            .clone()
            .try_acquire_owned()
            .map_err(|_busy| Error::Busy)?;
        #[cfg(feature = "test-util")]
        let pause = initializer
            .before_metadata
            .lock()
            .map_err(|_poison| Error::Runtime)?
            .take();
        let (finished, finishing) = tokio::sync::oneshot::channel();
        let lease = Arc::new(Lease::new(metadata, permit, finished, IoLease::current()));
        let scope = IoLease::new(lease.clone());
        let owner_execution = execution.clone();
        let operation = scope.scope(async move {
            let location = lease.metadata.location.clone();
            let config = &lease.metadata.config;
            let check_owner = lease.clone();
            let check_location = location.clone();
            let check_backing = expected_backing_root.clone();
            let observed = lore_spawn_blocking_nocontext!(move || {
                #[cfg(feature = "test-util")]
                if let Some(pause) = pause {
                    pause();
                }
                recheck_metadata(&check_owner, &check_location, check_backing.as_deref())
            })
            .await
            .map_err(|_join| Error::Runtime)??;
            if toml::to_string(&observed).map_err(failure)?
                != toml::to_string(config).map_err(failure)?
            {
                return Err(Error::InvalidConfig);
            }
            let is_new = matches!(
                mode,
                StoreMode::CreateNew(_) | StoreMode::Import(_, _) | StoreMode::RemoteCreate(_, _)
            );
            if is_new {
                let path = location.metadata().path().to_path_buf();
                let expected = expected_backing_root.clone();
                let marker_location = location.clone();
                lore_spawn_blocking_nocontext!(move || match expected {
                    Some(root) => start_bound_marker(&marker_location, &root),
                    None => start_marker(&path),
                })
                .await
                .map_err(|_join| Error::Runtime)??;
            }
            let shared = crate::shared_store::get_existing_shared_store_path_for_repo(config)
                .await
                .map_err(failure)?;
            let store_root = shared
                .clone()
                .unwrap_or_else(|| location.metadata().path().to_path_buf());
            let existing = !is_new || shared.is_some();
            preflight_and_claim(&lease, &store_root, location.target().path(), existing).await?;
            let (immutable, opening) = repository::create_immutable_store_at_path(
                store_root.clone(),
                ImmutableStoreCreateOptions::none(),
                true,
            )
            .await
            .map_err(failure)?;
            let (immutable, mutable, stores) =
                repository::claim_opened_stores(immutable, opening, |immutable| {
                    repository::create_mutable_store_at_path(store_root, immutable)
                })
                .await
                .map_err(failure)?;
            if mutable.needs_upgrade() {
                return Err(Error::UnsupportedStore);
            }
            let (lock, stores) = repository::get_or_create_repository_lock(
                location.metadata().path().to_path_buf(),
                stores,
            )
            .await
            .map_err(failure)?;
            {
                let mut retained = lease.retained.lock().map_err(|_poisoned| Error::Runtime)?;
                retained.stores = Some((immutable.clone(), mutable.clone()));
                retained.repository_lock = Some(lock.clone());
            }
            let requested_identity = execution.globals().identity.to_string();
            let identity = if requested_identity.is_empty() {
                config.identity.clone()
            } else {
                Some(requested_identity)
            };
            let remote = if matches!(
                &mode,
                StoreMode::RemoteCreate(_, _) | StoreMode::ReuseRemote(_)
            ) {
                RemoteState::Offline
            } else {
                match super::super::start_transport(|| {
                    repository::connect(
                        location.repository(),
                        execution.globals(),
                        config,
                        identity,
                    )
                }) {
                    Ok(remote) => RemoteState::Pending(remote),
                    Err(lore_transport::ProtocolError::NoRemote(_offline)) => RemoteState::Offline,
                    Err(error) => return Err(failure(error)),
                }
            };
            let repository = RepositoryContext::new_with_state(
                Some(RepositoryPaths::new(
                    location.target().path().to_path_buf(),
                    location.metadata().path().to_path_buf(),
                )),
                immutable,
                mutable,
                location.repository(),
                location.instance(),
                remote,
                Arc::default(),
                Some(InvalidFilesystemProvider::new()),
            )
            .with_repository_lock(lock, stores)
            .with_write_token(lease.metadata.token.share());
            let repository = if expected_backing_root.is_some() {
                repository.with_link_read(Arc::new(|_id| false))
            } else {
                repository
            };
            let repository = Arc::new(match &mode {
                StoreMode::Import(_, seed) => seed.apply_link_policy(repository),
                StoreMode::ReuseWithPolicy(policy) => policy.apply(repository)?,
                StoreMode::RemoteCreate(_, contents) | StoreMode::ReuseRemote(contents) => {
                    contents.apply(repository, &execution).await?
                }
                _ => repository,
            });
            let mode = match mode {
                StoreMode::Import(selection, seed) => {
                    Box::pin(seed.copy_into(repository.clone())).await?;
                    StoreMode::CreateNew(selection)
                }
                StoreMode::RemoteCreate(selection, _) => StoreMode::CreateNew(selection),
                StoreMode::ReuseRemote(_) => StoreMode::Reuse,
                mode => mode,
            };
            let selection = match mode {
                StoreMode::CreateNew(selection) => {
                    State::deserialize(repository.clone(), selection.revision)
                        .await
                        .map_err(failure)?;
                    ensure_branch(&repository, &lease.metadata.token, &selection, config).await?;
                    let (key, kind) =
                        instance::instance_key(repository.salt(), location.instance());
                    match repository
                        .read_mutable_store()
                        .load(repository.id, key, kind)
                        .await
                    {
                        Ok(_existing) => return Err(Error::InvalidSelection),
                        Err(crate::store::StoreError::AddressNotFound(_)) => {}
                        Err(error) => return Err(failure(error)),
                    }
                    instance::register_instance(
                        &repository,
                        location.instance(),
                        &location.target().path().display().to_string(),
                    )
                    .await
                    .map_err(failure)?;
                    instance::store_current_anchor_branch(&repository, selection.branch)
                        .await
                        .map_err(failure)?;
                    instance::store_current_anchor(&repository, selection.revision)
                        .await
                        .map_err(failure)?;
                    repository.flush(true).await.map_err(failure)?;
                    selection
                }
                StoreMode::Reuse | StoreMode::ReuseWithPolicy(_) => {
                    read_selection(&repository, &location).await?
                }
                StoreMode::Import(_, _)
                | StoreMode::RemoteCreate(_, _)
                | StoreMode::ReuseRemote(_) => return Err(Error::Runtime),
            };
            lease
                .metadata
                .registration
                .check_pending_location(&location)?;
            if is_new {
                let path = location.metadata().path().to_path_buf();
                lore_spawn_blocking_nocontext!(move || finish_marker(&path))
                    .await
                    .map_err(|_join| Error::Runtime)??;
            }
            let check_owner = lease.clone();
            let check_location = location.clone();
            let observed = lore_spawn_blocking_nocontext!(move || recheck_metadata(
                &check_owner,
                &check_location,
                expected_backing_root.as_deref()
            ))
            .await
            .map_err(|_join| Error::Runtime)??;
            if toml::to_string(&observed).map_err(failure)?
                != toml::to_string(config).map_err(failure)?
            {
                return Err(Error::InvalidConfig);
            }
            Ok(PreparedWorkingContext {
                execution,
                registration: lease.metadata.registration.clone(),
                location,
                repository: Arc::new(repository.without_command_locks()),
                selection,
            })
        });
        drop(scope);
        let result = lore_base::runtime::LORE_CONTEXT
            .scope(owner_execution, operation)
            .await;
        finishing.await.map_err(|_lost| Error::Runtime)?;
        result
    }

    fn recheck_metadata(
        lease: &Lease,
        location: &WorkingLocation,
        expected_backing_root: Option<&Path>,
    ) -> Result<RepositoryConfig> {
        let config = super::super::initialize(
            &lease.metadata.registration,
            location,
            super::super::Mode::Reuse,
        )?;
        if let Some(root) = expected_backing_root {
            check_binding(location, root)?;
        }
        Ok(config)
    }
    fn check_binding(location: &WorkingLocation, expected: &Path) -> Result<()> {
        let binding = super::super::BackingBinding::read(
            location.metadata().path(),
            location.repository(),
            location.instance(),
        )
        .map_err(|_binding| Error::InvalidBinding)?;
        if binding.root() != expected {
            return Err(Error::InvalidBinding);
        }
        Ok(())
    }

    async fn ensure_branch(
        repository: &Arc<RepositoryContext>,
        token: &RepositoryWriteToken,
        selection: &Selection,
        config: &RepositoryConfig,
    ) -> Result<()> {
        // Seed only the private branch cache. The selected branch can already
        // exist remotely, and its name and identity must remain unchanged.
        let repository = Arc::new(repository.with_filter_and_remote(
            repository.filter.clone(),
            Err(lore_base::error::NoRemote.into()),
        ));
        match branch::load_name_to_id_local(repository.clone(), &selection.branch_name).await {
            Ok(id) if id == selection.branch => {}
            Ok(_other) => return Err(Error::InvalidSelection),
            Err(crate::branch::BranchError::BranchNotFound(_missing)) => {
                branch::create(
                    repository.clone(),
                    token,
                    selection.branch,
                    &selection.branch_name,
                    branch::default_category(),
                    config.identity.as_deref().unwrap_or_default(),
                    crate::util::time::timestamp(),
                    vec![],
                    false,
                    false,
                )
                .await
                .map_err(failure)?;
            }
            Err(error) => return Err(failure(error)),
        }
        let metadata = branch::metadata_local(repository.clone(), selection.branch)
            .await
            .map_err(failure)?;
        if metadata.get_string(branch::NAME).map_err(failure)? != selection.branch_name {
            return Err(Error::InvalidSelection);
        }
        Ok(())
    }

    async fn read_selection(
        repository: &Arc<RepositoryContext>,
        location: &WorkingLocation,
    ) -> Result<Selection> {
        let anchors = instance::strict_anchors::read(repository)
            .await
            .map_err(failure)?;
        let revision = anchors.current;
        let branch = anchors.branch;
        let (key, kind) = instance::instance_key(repository.salt(), location.instance());
        let hash = repository
            .read_mutable_store()
            .load(repository.id, key, kind)
            .await
            .map_err(failure)?;
        let instance = instance::load_instance_metadata(repository, hash)
            .await
            .map_err(failure)?;
        if instance.instance_id != location.instance()
            || Path::new(&instance.path) != location.target().path()
        {
            return Err(Error::InvalidSelection);
        }
        let metadata = branch::metadata_local(repository.clone(), branch)
            .await
            .map_err(failure)?;
        let branch_name = metadata
            .get_string(branch::NAME)
            .map_err(failure)?
            .to_string();
        if branch::load_name_to_id_local(repository.clone(), &branch_name)
            .await
            .map_err(failure)?
            != branch
        {
            return Err(Error::InvalidSelection);
        }
        State::deserialize(repository.clone(), revision)
            .await
            .map_err(failure)?;
        Ok(Selection {
            branch,
            branch_name,
            revision,
        })
    }

    async fn preflight_and_claim(
        lease: &Arc<Lease>,
        root: &Path,
        target: &Path,
        existing: bool,
    ) -> Result<()> {
        if existing {
            if !root.is_dir() {
                return Err(Error::UnsupportedStore);
            }
            for name in ["immutable", "mutable"] {
                let directory = root.join(name);
                if !std::fs::symlink_metadata(&directory)?.is_dir()
                    || !std::fs::symlink_metadata(directory.join("lock"))?.is_file()
                {
                    return Err(Error::UnsupportedStore);
                }
            }
        } else {
            let directory = root.to_path_buf();
            lore_spawn_blocking_nocontext!(move || {
                std::fs::create_dir(directory.join("immutable"))?;
                std::fs::create_dir(directory.join("mutable"))
            })
            .await
            .map_err(|_join| Error::Runtime)??;
        }
        let root = std::fs::canonicalize(root)?;
        if root.starts_with(target) || target.starts_with(&root) {
            return Err(Error::UnsafeDirectory);
        }
        let immutable = StoreLock::new(root.join("immutable"), crate::store::retain_stores())?;
        inventory_claim(
            lease,
            immutable.clone(),
            root.join("immutable"),
            existing,
            true,
        )
        .await?;
        let mutable = StoreLock::new_within(
            root.join("mutable"),
            crate::store::retain_stores(),
            &immutable,
        )?;
        inventory_claim(lease, mutable, root.join("mutable"), existing, false).await?;
        Ok(())
    }

    async fn inventory_claim(
        lease: &Arc<Lease>,
        lock: Arc<StoreLock>,
        directory: std::path::PathBuf,
        existing: bool,
        immutable: bool,
    ) -> Result<()> {
        let guard = lock.acquire(Intent::Read).await?;
        let index = retain_guard(lease, guard)?;
        if existing {
            lore_spawn_blocking_nocontext!(move || preflight_directory(&directory, immutable))
                .await
                .map_err(|_join| Error::Runtime)??;
        }
        // This observer owns only the preflight inventory. The real constructor
        // acquires its own observer and still surveys all store data normally.
        lease
            .retained
            .lock()
            .map_err(|_poisoned| Error::Runtime)?
            .guards[index]
            .note_refreshed();
        Ok(())
    }

    fn preflight_directory(path: &Path, immutable: bool) -> Result<()> {
        use std::io::Read;
        use std::os::unix::fs::OpenOptionsExt;
        if !std::fs::symlink_metadata(path)?.is_dir() {
            return Err(Error::UnsupportedStore);
        }
        let name = if immutable { "info" } else { "version" };
        let file = std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(path.join(name))?;
        let mut bytes = Vec::new();
        file.take(65).read_to_end(&mut bytes)?;
        if immutable {
            // The normal constructor validates this existing header; absence
            // must be rejected here because opening an absent info file creates it.
            if bytes.is_empty() || bytes.len() > 64 {
                return Err(Error::UnsupportedStore);
            }
        } else if bytes.as_slice() != (MutableStoreVersion::LazyFanOut as u32).to_ne_bytes()
            && bytes.as_slice() != (MutableStoreVersion::TypedItems as u32).to_ne_bytes()
        {
            return Err(Error::UnsupportedStore);
        }
        for group in 0..256 {
            match std::fs::symlink_metadata(
                lore_storage::local::fan_out::group_dir_path(&path.join("index"), group)
                    .join(lore_storage::local::fan_out::LEVEL_PENDING_FILENAME),
            ) {
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                _ => return Err(Error::UnsupportedStore),
            }
        }
        Ok(())
    }

    fn start_marker(path: &Path) -> Result<()> {
        for entry in std::fs::read_dir(path)? {
            let name = entry?.file_name();
            if ![repository::ID, repository::INSTANCE, repository::CONFIG]
                .iter()
                .any(|expected| name == *expected)
            {
                return Err(Error::NotEmpty);
            }
        }
        create_marker(path)
    }
    fn start_bound_marker(location: &WorkingLocation, expected: &Path) -> Result<()> {
        check_binding(location, expected)?;
        let path = location.metadata().path();
        for entry in std::fs::read_dir(path)? {
            let name = entry?.file_name();
            if ![
                repository::ID,
                repository::INSTANCE,
                repository::CONFIG,
                super::super::backing_binding::NAME,
            ]
            .iter()
            .any(|expected| name == *expected)
            {
                return Err(Error::NotEmpty);
            }
        }
        create_marker(path)
    }
    fn create_marker(path: &Path) -> Result<()> {
        use std::os::unix::fs::OpenOptionsExt;
        std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(path.join(INITIALIZING_STORES))?
            .sync_all()?;
        std::fs::File::open(path)?.sync_all()?;
        Ok(())
    }
    // The caller owns the exact root write token and registered metadata binding.
    #[allow(clippy::disallowed_methods)]
    fn finish_marker(path: &Path) -> Result<()> {
        std::fs::File::open(path)?.sync_all()?;
        std::fs::remove_file(path.join(INITIALIZING_STORES))?;
        std::fs::File::open(path)?.sync_all()?;
        Ok(())
    }
}
