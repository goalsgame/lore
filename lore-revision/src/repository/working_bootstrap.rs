// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Unpublished external metadata and normal disk-store preparation for an
//! experimental working instance. Neither initializer publishes a filesystem
//! provider or starts a mount.

use std::sync::Arc;

use tokio::sync::Semaphore;

use super::RepositoryConfig;
use super::RepositoryWriteToken;
use crate::fs::provider_registry::Registration;
use crate::fs::provider_registry::WorkingLocation;

#[cfg(unix)]
mod backing_binding;
#[cfg(unix)]
pub use backing_binding::BackingBinding;
#[cfg(unix)]
pub use backing_binding::Error as BackingBindingError;

#[cfg(unix)]
mod pending;
#[cfg(unix)]
pub use pending::DeferredPublicationCleanup;
#[cfg(unix)]
pub use pending::OwnerBuildBusy;
#[cfg(unix)]
pub use pending::OwnerBuildPayload;
#[cfg(unix)]
pub use pending::PendingError;
#[cfg(unix)]
pub use pending::PendingMetadata;
#[cfg(unix)]
pub use pending::PendingPublication;
#[cfg(unix)]
pub use pending::PendingWorkingContext;
#[cfg(unix)]
pub use pending::PreparedPendingPublication;
#[cfg(feature = "test-util")]
type MetadataPause = Box<dyn FnOnce() + Send>;
mod import;
mod metadata_job;
#[cfg(unix)]
mod remote_contents;
#[cfg(unix)]
pub use remote_contents::RemoteContents;
mod stores;
pub use import::ImmutableImport;
pub use import::ImportLimits;
pub use import::ImportReadPolicy;
pub use import::ImportStats;
pub use stores::PreparedWorkingContext;
pub use stores::Selection;
pub use stores::StoreInitializer;
pub use stores::StoreMode;

/// Connection setup owns transport-lifetime workers, independent of instance IO.
/// Retain the command's credentials/context while clearing only its local IO lease.
#[cfg(unix)]
#[lore_macro::test_pub]
fn start_transport<T>(setup: impl FnOnce() -> T) -> T {
    lore_io::lease::sync_scope(None, setup)
}

#[derive(Debug)]
pub enum Error {
    Io(std::io::Error),
    Registry(crate::fs::provider_registry::Error),
    InvalidLimit,
    Busy,
    InvalidToken,
    InvalidConfig,
    UnsafeDirectory,
    NotEmpty,
    Incomplete,
    Unsupported,
    Runtime,
    Preparation(String),
    InvalidSelection,
    MissingExecutionContext,
    UnsupportedStore,
    InvalidBinding,
    UnsupportedBoundStores,
}
impl std::fmt::Display for Error {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "working metadata: {error}"),
            Self::Registry(error) => write!(formatter, "working metadata: {error}"),
            Self::InvalidLimit => {
                formatter.write_str("metadata job limit must be between 1 and 4096")
            }
            Self::Busy => formatter.write_str("metadata initializer is at capacity"),
            Self::InvalidToken => {
                formatter.write_str("metadata token does not protect the canonical mount root")
            }
            Self::InvalidConfig => formatter
                .write_str("working metadata requires a bounded experimental configuration"),
            Self::UnsafeDirectory => formatter
                .write_str("working metadata requires its original private owned directory"),
            Self::NotEmpty => {
                formatter.write_str("new working metadata requires an empty directory")
            }
            Self::Incomplete => {
                formatter.write_str("working metadata initialization is incomplete")
            }
            Self::Unsupported => {
                formatter.write_str("working metadata initialization requires Unix ownership")
            }
            Self::Runtime => formatter.write_str("metadata initializer runtime is unavailable"),
            Self::Preparation(error) => write!(formatter, "working store preparation: {error}"),
            Self::InvalidSelection => {
                formatter.write_str("working store selection or instance anchors do not match")
            }
            Self::MissingExecutionContext => {
                formatter.write_str("working stores require a persistent client execution context")
            }
            Self::InvalidBinding => {
                formatter.write_str("working stores do not match their expected backing root")
            }
            Self::UnsupportedBoundStores => formatter.write_str(
                "bound working bootstrap requires local stores and an offline or absent remote",
            ),
            Self::UnsupportedStore => formatter.write_str(
                "working bootstrap requires existing current-format stores without migration",
            ),
        }
    }
}
impl std::error::Error for Error {}
impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}
impl From<crate::fs::provider_registry::Error> for Error {
    fn from(error: crate::fs::provider_registry::Error) -> Self {
        Self::Registry(error)
    }
}
pub type Result<T> = std::result::Result<T, Error>;

pub enum Mode {
    /// Requires an empty reserved directory. Failed partial creation is retained.
    CreateNew(RepositoryConfig),
    /// Reads existing identity/configuration without changing any files.
    Reuse,
}

/// Bounds actual queued/running initialization jobs, including cancelled callers.
#[lore_macro::test_pub]
pub struct MetadataInitializer {
    jobs: Arc<Semaphore>,
    #[cfg(feature = "test-util")]
    before_initialize: std::sync::Mutex<Option<MetadataPause>>,
}
impl MetadataInitializer {
    pub fn new(max_jobs: usize) -> Result<Arc<Self>> {
        if max_jobs == 0 || max_jobs > 4096 {
            return Err(Error::InvalidLimit);
        }
        Ok(Arc::new(Self {
            jobs: Arc::new(Semaphore::new(max_jobs)),
            #[cfg(feature = "test-util")]
            before_initialize: std::sync::Mutex::default(),
        }))
    }

    /// Installs one test pause in the actual blocking metadata job.
    #[cfg(feature = "test-util")]
    pub fn pause_initialize(&self, pause: impl FnOnce() + Send + 'static) {
        *self.before_initialize.lock().expect("metadata test pause") = Some(Box::new(pause));
    }
    pub async fn prepare(
        &self,
        registration: Arc<Registration>,
        token: RepositoryWriteToken,
        mode: Mode,
    ) -> Result<PreparedMetadata> {
        let location = registration.pending_location()?;
        let protects_root = match &token {
            RepositoryWriteToken::Client(guard) => Arc::ptr_eq(
                tokio::sync::OwnedMutexGuard::mutex(guard.as_ref()),
                &super::write_token::write_mutex_for_path(location.target().path()),
            ),
            _ => false,
        };
        if !protects_root {
            return Err(Error::InvalidToken);
        }
        let permit = self
            .jobs
            .clone()
            .try_acquire_owned()
            .map_err(|_busy| Error::Busy)?;
        let runtime = tokio::runtime::Handle::try_current().map_err(|_missing| Error::Runtime)?;
        #[cfg(feature = "test-util")]
        let pause = self
            .before_initialize
            .lock()
            .map_err(|_poison| Error::Runtime)?
            .take();
        // One ordered payload retains inherited ownership through actual work,
        // including cancellation of this waiter or its runtime.
        let job = metadata_job::MetadataJob::new(
            registration,
            token,
            location,
            mode,
            permit,
            lore_io::lease::IoLease::current(),
        );
        #[cfg(feature = "test-util")]
        let job = job.with_pause(pause);
        #[allow(clippy::disallowed_methods)]
        runtime
            .spawn_blocking(move || job.run())
            .await
            .map_err(|_join| Error::Runtime)?
    }
}

/// Verified metadata with its exclusive reservation and root token still held.
/// This is an unready instance: stores, anchors and a provider are not prepared.
pub struct PreparedMetadata {
    registration: Arc<Registration>,
    token: RepositoryWriteToken,
    location: WorkingLocation,
    config: RepositoryConfig,
    // Inherited work accounting follows every metadata/token capability.
    parent: Option<lore_io::lease::IoLease>,
}
impl PreparedMetadata {
    pub fn location(&self) -> &WorkingLocation {
        &self.location
    }
    pub fn config(&self) -> &RepositoryConfig {
        &self.config
    }
}

#[cfg(unix)]
// The caller's exact canonical-root write token remains owned by this std-only job.
#[allow(clippy::disallowed_methods)]
fn initialize(
    registration: &Registration,
    location: &WorkingLocation,
    mode: Mode,
) -> Result<RepositoryConfig> {
    use std::io::Read;
    use std::io::Write;
    use std::os::unix::fs::MetadataExt;
    use std::os::unix::fs::OpenOptionsExt;

    use crate::fs::provider_registry::INITIALIZING_METADATA;

    const CONFIG_LIMIT: usize = 64 * 1024;
    let path = location.metadata().path();
    registration.check_pending_location(location)?;
    let metadata = std::fs::symlink_metadata(path)?;
    // SAFETY: geteuid reads process identity and has no preconditions.
    let uid = unsafe { libc::geteuid() };
    if !metadata.is_dir()
        || metadata.file_type().is_symlink()
        || metadata.uid() != uid
        || metadata.mode() & 0o777 != 0o700
        || std::fs::canonicalize(path)? != path
    {
        return Err(Error::UnsafeDirectory);
    }
    let directory = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW)
        .open(path)?;
    let read = |name: &str, maximum: usize| -> Result<Vec<u8>> {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(path.join(name))?;
        let metadata = file.metadata()?;
        if !metadata.is_file()
            || metadata.uid() != uid
            || metadata.mode() & 0o022 != 0
            || metadata.len() > maximum as u64
        {
            return Err(Error::UnsafeDirectory);
        }
        let mut bytes = Vec::new();
        file.take(maximum as u64 + 1).read_to_end(&mut bytes)?;
        if bytes.len() > maximum {
            return Err(Error::InvalidConfig);
        }
        Ok(bytes)
    };
    let config = match mode {
        Mode::Reuse => {
            for marker in [
                INITIALIZING_METADATA,
                crate::fs::provider_registry::INITIALIZING_STORES,
            ] {
                match std::fs::symlink_metadata(path.join(marker)) {
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    _ => return Err(Error::Incomplete),
                }
            }
            if read(super::ID, 16)?.as_slice() != location.repository().data()
                || read(super::INSTANCE, 16)?.as_slice() != location.instance().data()
            {
                return Err(Error::Registry(
                    crate::fs::provider_registry::Error::InvalidMetadata,
                ));
            }
            let bytes = read(super::CONFIG, CONFIG_LIMIT)?;
            let text = std::str::from_utf8(&bytes).map_err(|_invalid| Error::InvalidConfig)?;
            toml::from_str::<RepositoryConfig>(text).map_err(|_invalid| Error::InvalidConfig)?
        }
        Mode::CreateNew(config) => {
            if !config.is_experimental_working() || config.validate().is_err() {
                return Err(Error::InvalidConfig);
            }
            let bytes = toml::to_string_pretty(&config)
                .map_err(|_invalid| Error::InvalidConfig)?
                .into_bytes();
            if bytes.len() > CONFIG_LIMIT {
                return Err(Error::InvalidConfig);
            }
            if std::fs::read_dir(path)?.next().transpose()?.is_some() {
                return Err(Error::NotEmpty);
            }
            let write = |name: &str, bytes: &[u8]| -> Result<()> {
                let mut file = std::fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .mode(0o600)
                    .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
                    .open(path.join(name))?;
                file.write_all(bytes)?;
                file.sync_all()?;
                Ok(())
            };
            write(INITIALIZING_METADATA, b"")?;
            directory.sync_all()?;
            write(super::ID, location.repository().data())?;
            write(super::INSTANCE, location.instance().data())?;
            write(super::CONFIG, &bytes)?;
            directory.sync_all()?;
            registration.check_pending_location(location)?;
            std::fs::remove_file(path.join(INITIALIZING_METADATA))?;
            directory.sync_all()?;
            config
        }
    };
    if !config.is_experimental_working() || config.validate().is_err() {
        return Err(Error::InvalidConfig);
    }
    let current = std::fs::symlink_metadata(path)?;
    if current.dev() != metadata.dev() || current.ino() != metadata.ino() {
        return Err(Error::UnsafeDirectory);
    }
    registration.check_pending_location(location)?;
    Ok(config)
}

#[cfg(not(unix))]
fn initialize(
    _registration: &Registration,
    _location: &WorkingLocation,
    _mode: Mode,
) -> Result<RepositoryConfig> {
    Err(Error::Unsupported)
}
