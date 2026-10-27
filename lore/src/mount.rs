// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Explicit experimental foreground mounts of one immutable revision.
//!
//! Repository setup uses the ordinary read-only command context. The mounted
//! backend retains authorization and stores after command locks are released.
//! No working-tree provider or configured filesystem default is changed.

use std::io;
use std::path::Component;
use std::path::Path;
use std::path::PathBuf;

/// Canonicalizes an existing absolute empty directory and refuses symlinks or repository
/// metadata. The caller must retain exclusive control of this directory until
/// the mount finishes. Validation is repeated immediately before mounting.
pub fn validate_mountpoint(path: &Path, metadata: Option<&Path>) -> io::Result<PathBuf> {
    if !path.is_absolute() || metadata.is_some_and(|metadata| !metadata.is_absolute()) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "mount validation requires absolute paths",
        ));
    }
    let entry = std::fs::symlink_metadata(path)?;
    if !entry.is_dir() || entry.file_type().is_symlink() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "mountpoint must be an existing directory, not a symlink",
        ));
    }
    let path = path.canonicalize()?;
    if path.components().any(|part| {
        matches!(part, Component::Normal(name)
            if name.eq_ignore_ascii_case(".lore") || name.eq_ignore_ascii_case(".urc"))
    }) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "mountpoint must be outside Lore metadata",
        ));
    }
    if let Some(metadata) = metadata {
        let metadata = metadata.canonicalize()?;
        if path.starts_with(&metadata) || metadata.starts_with(&path) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "mountpoint must not contain or hide repository metadata",
            ));
        }
    }
    #[cfg(target_os = "linux")]
    reject_existing_mount(&path)?;
    if std::fs::read_dir(&path)?.next().transpose()?.is_some() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "mountpoint must be empty, including hidden entries",
        ));
    }
    Ok(path)
}

/// Creates or validates an opt-in cache directory. Cached records never grant
/// repository access and this helper performs no content backfill.
#[cfg(unix)]
pub fn validate_digest_cache(path: &Path, mountpoint: &Path) -> io::Result<PathBuf> {
    validate_digest_cache_for(path, Some(mountpoint))
}

/// Validates a cache for adapters whose mountpoint is managed by the OS. Those
/// adapters must separately establish that the cache lies outside their mount.
#[cfg(unix)]
pub fn validate_digest_cache_directory(path: &Path) -> io::Result<PathBuf> {
    validate_digest_cache_for(path, None)
}

#[cfg(unix)]
fn validate_digest_cache_for(path: &Path, mountpoint: Option<&Path>) -> io::Result<PathBuf> {
    use std::os::unix::fs::DirBuilderExt;
    use std::os::unix::fs::MetadataExt;
    if !path.is_absolute() || mountpoint.is_some_and(|mountpoint| !mountpoint.is_absolute()) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "digest cache validation requires absolute paths",
        ));
    }
    let mountpoint = mountpoint.map(Path::canonicalize).transpose()?;
    let overlaps = |path: &Path| {
        mountpoint
            .as_ref()
            .is_some_and(|mountpoint| path.starts_with(mountpoint) || mountpoint.starts_with(path))
    };
    match std::fs::symlink_metadata(path) {
        Ok(_) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            let parent = path
                .parent()
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "digest cache requires an existing parent directory",
                    )
                })?
                .canonicalize()?;
            let name = path.file_name().ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "invalid digest cache directory",
                )
            })?;
            let target = parent.join(name);
            if overlaps(&target) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "digest cache and mountpoint must not contain one another",
                ));
            }
            match std::fs::DirBuilder::new().mode(0o700).create(&target) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
                Err(error) => return Err(error),
            }
        }
        Err(error) => return Err(error),
    }
    let metadata = std::fs::symlink_metadata(path)?;
    // SAFETY: getuid has no memory-safety preconditions.
    let uid = unsafe { libc::getuid() };
    if !metadata.is_dir()
        || metadata.file_type().is_symlink()
        || metadata.uid() != uid
        || metadata.mode() & 0o022 != 0
    {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "digest cache must be a user-owned directory without group or other write access",
        ));
    }
    let canonical = path.canonicalize()?;
    if overlaps(&canonical) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "digest cache and mountpoint must not contain one another",
        ));
    }
    Ok(canonical)
}

#[cfg(target_os = "linux")]
fn reject_existing_mount(path: &Path) -> io::Result<()> {
    use std::os::unix::ffi::OsStrExt;
    let bytes = std::fs::read("/proc/self/mountinfo")?;
    for line in bytes
        .split(|&byte| byte == b'\n')
        .filter(|line| !line.is_empty())
    {
        let encoded = line.split(|&byte| byte == b' ').nth(4).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "invalid Linux mount table")
        })?;
        let mut decoded = Vec::with_capacity(encoded.len());
        let mut cursor = 0;
        while cursor < encoded.len() {
            if encoded[cursor] == b'\\' {
                let octal = encoded
                    .get(cursor + 1..cursor + 4)
                    .filter(|bytes| bytes.iter().all(|byte| (b'0'..=b'7').contains(byte)))
                    .ok_or_else(|| {
                        io::Error::new(io::ErrorKind::InvalidData, "invalid Linux mount path")
                    })?;
                let value = (octal[0] - b'0') as u16 * 64
                    + (octal[1] - b'0') as u16 * 8
                    + (octal[2] - b'0') as u16;
                decoded.push(u8::try_from(value).map_err(|_range_error| {
                    io::Error::new(io::ErrorKind::InvalidData, "invalid Linux mount path")
                })?);
                cursor += 4;
            } else {
                decoded.push(encoded[cursor]);
                cursor += 1;
            }
        }
        if decoded == path.as_os_str().as_bytes() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "mountpoint already has a mounted filesystem",
            ));
        }
    }
    Ok(())
}

#[cfg(unix)]
mod platform {
    #[cfg(target_os = "linux")]
    use std::future::Future;
    use std::io;
    #[cfg(target_os = "linux")]
    use std::path::Path;
    use std::path::PathBuf;
    use std::sync::Arc;

    use lore_error_set::prelude::*;
    use lore_revision::errors::UnhandledError;
    use lore_revision::lore::execution_context;
    use lore_vfs::core::Core;
    use lore_vfs::revision::RevisionBackend;

    #[cfg(target_os = "linux")]
    use super::validate_digest_cache;
    #[cfg(target_os = "linux")]
    use super::validate_mountpoint;
    use crate::interface::LoreEventCallback;
    use crate::interface::LoreGlobalArgs;

    /// Prepared immutable view. Construction never starts a FUSE session.
    #[cfg(target_os = "linux")]
    pub struct PreparedMount {
        core: Arc<Core>,
        mountpoint: PathBuf,
        metadata: PathBuf,
    }
    #[cfg(target_os = "linux")]
    impl PreparedMount {
        pub fn mountpoint(&self) -> &Path {
            &self.mountpoint
        }

        pub async fn run(
            self,
            shutdown: impl Future<Output = ()>,
            ready: impl FnOnce(),
        ) -> io::Result<()> {
            foreground(
                self.core,
                self.mountpoint,
                Some(self.metadata),
                shutdown,
                ready,
            )
            .await
        }
    }

    /// Resolves the explicit revision with normal repository credentials. This
    /// command runs locally; a mount session is never relayed to the Lore service.
    /// The callback describes setup, which completes before the session starts.
    #[cfg(target_os = "linux")]
    pub async fn prepare(
        globals: LoreGlobalArgs,
        revision: String,
        mountpoint: PathBuf,
        digest_cache: Option<PathBuf>,
        callback: LoreEventCallback,
    ) -> Result<PreparedMount, i32> {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let status = crate::call::repository_call_read(
            globals,
            callback,
            (revision, mountpoint, digest_cache),
            "mount setup",
            move |repository, (revision, mountpoint, digest_cache)| async move {
                if execution_context().globals().dry_run() {
                    return Err(UnhandledError::from(lore_base::error::InvalidArguments {
                        reason: "foreground mounts do not support --dry-run".into(),
                    }));
                }
                if revision.trim().is_empty() {
                    return Err(UnhandledError::from(lore_base::error::InvalidArguments {
                        reason: "mount requires an explicit revision".into(),
                    }));
                }
                let metadata = repository.dot_dir_path()?.to_path_buf();
                let mountpoint = if mountpoint.is_absolute() {
                    mountpoint
                } else {
                    lore_revision::util::path::make_absolute(".")
                        .forward_any::<UnhandledError>("invalid caller working directory")?
                        .join(mountpoint)
                };
                let mountpoint = validate_mountpoint(&mountpoint, Some(&metadata))
                    .internal("invalid mountpoint")?;
                let digest_cache = if let Some(path) = digest_cache {
                    let path = if path.is_absolute() {
                        path
                    } else {
                        lore_revision::util::path::make_absolute(".")
                            .forward_any::<UnhandledError>("invalid caller working directory")?
                            .join(path)
                    };
                    Some(
                        validate_digest_cache(&path, &mountpoint)
                            .internal("invalid digest cache")?,
                    )
                } else {
                    None
                };
                let core = repository_core(repository, revision, digest_cache).await?;
                sender
                    .send(PreparedMount {
                        core,
                        mountpoint,
                        metadata,
                    })
                    .map_err(|_prepared| io::Error::other("mount setup caller was cancelled"))
                    .internal("mount setup cancelled")?;
                Ok::<(), UnhandledError>(())
            },
        )
        .await;
        if status != 0 {
            return Err(status);
        }
        receiver.await.map_err(|_cancelled| 1)
    }

    /// Opens one authorized pinned core and releases the repository command
    /// locks before returning it. OS adapters retain the captured context.
    pub async fn prepare_revision(
        globals: LoreGlobalArgs,
        revision: String,
        digest_cache: Option<PathBuf>,
        callback: LoreEventCallback,
    ) -> Result<Arc<Core>, i32> {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let status = crate::call::repository_call_read(
            globals,
            callback,
            (revision, digest_cache),
            "pinned filesystem setup",
            move |repository, (revision, digest_cache)| async move {
                let core = repository_core(repository, revision, digest_cache).await?;
                sender
                    .send(core)
                    .map_err(|_core| io::Error::other("filesystem setup caller was cancelled"))
                    .internal("filesystem setup cancelled")?;
                Ok::<(), UnhandledError>(())
            },
        )
        .await;
        if status != 0 {
            return Err(status);
        }
        receiver.await.map_err(|_cancelled| 1)
    }

    async fn repository_core(
        repository: Arc<lore_revision::repository::RepositoryContext>,
        revision: String,
        digest_cache: Option<PathBuf>,
    ) -> Result<Arc<Core>, UnhandledError> {
        if revision.trim().is_empty() || execution_context().globals().dry_run() {
            return Err(UnhandledError::from(lore_base::error::InvalidArguments { reason: "pinned filesystem setup requires an explicit revision and does not support dry-run".into() }));
        }
        let digest_cache = digest_cache
            .map(|path| {
                let path = if path.is_absolute() {
                    path
                } else {
                    lore_revision::util::path::make_absolute(".")
                        .forward_any::<UnhandledError>("invalid caller working directory")?
                        .join(path)
                };
                Ok::<_, UnhandledError>(
                    super::validate_digest_cache_directory(&path)
                        .internal("invalid digest cache")?,
                )
            })
            .transpose()?;
        let hash = lore_revision::revision::resolve_boxed(
            repository.clone(),
            revision,
            execution_context().globals().search_location(),
        )
        .await
        .forward_any::<UnhandledError>("invalid pinned revision")?;
        let backend = RevisionBackend::new(repository, hash)
            .await
            .map_err(|error| io::Error::other(format!("pinned revision backend: {error:?}")))
            .internal("could not load pinned revision")?;
        let backend = Arc::new(if let Some(path) = digest_cache {
            backend.with_digest_index(lore_vfs::digest_index::DigestIndex::new(path))
        } else {
            backend
        });
        let core = Arc::new(
            Core::new(backend.clone(), backend.root())
                .map_err(|error| io::Error::other(format!("pinned revision core: {error:?}")))
                .internal("could not construct pinned view")?,
        );
        Ok(core)
    }

    /// Runs one foreground session with injected shutdown, also usable by native
    /// fixture tests. The dedicated owner thread retains `Mounted` for its entire lifetime;
    /// cancelling this future closes its shutdown channel and requests teardown.
    /// The supplied runtime must remain alive until teardown has drained callbacks.
    #[cfg(target_os = "linux")]
    pub async fn foreground(
        core: Arc<Core>,
        mountpoint: PathBuf,
        metadata: Option<PathBuf>,
        shutdown: impl Future<Output = ()>,
        ready: impl FnOnce(),
    ) -> io::Result<()> {
        let runtime = tokio::runtime::Handle::current();
        if runtime.runtime_flavor() != tokio::runtime::RuntimeFlavor::MultiThread {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "foreground mounts require a multi-thread Tokio runtime",
            ));
        }
        let mountpoint = validate_mountpoint(&mountpoint, metadata.as_deref())?;
        let (stop_sender, stop_receiver) = std::sync::mpsc::channel::<()>();
        let (started_sender, started_receiver) = tokio::sync::oneshot::channel();
        let (finished_sender, finished_receiver) = tokio::sync::oneshot::channel();
        // A foreground session must not occupy Tokio's bounded blocking pool:
        // backend file I/O still needs that pool when its configured size is one.
        let owner = std::thread::Builder::new()
            .name("lore-mount-owner".into())
            .spawn(move || {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    let mountpoint = validate_mountpoint(&mountpoint, metadata.as_deref())?;
                    // SAFETY: these process identity queries have no preconditions.
                    let (uid, gid) = unsafe { (libc::getuid(), libc::getgid()) };
                    let mut mounted = lore_vfs::linux::mount(core, runtime, mountpoint, uid, gid)?;
                    let ended = mounted.take_session_end().ok_or_else(|| {
                        io::Error::other("mount session notification is unavailable")
                    })?;
                    if started_sender.send(ended).is_ok() {
                        let _ = stop_receiver.recv();
                    }
                    mounted.unmount()
                }))
                .unwrap_or_else(|_panic| Err(io::Error::other("foreground mount owner panicked")));
                let _ = finished_sender.send(result);
            })?;
        tokio::pin!(shutdown);
        let started = tokio::select! {
            started = started_receiver => started.ok(),
            _ = &mut shutdown => None,
        };
        if let Some(ended) = started {
            ready();
            tokio::select! {
                _ = ended => {},
                _ = &mut shutdown => {},
            }
        }
        drop(stop_sender);
        let result = finished_receiver.await.map_err(|_cancelled| {
            io::Error::other("foreground mount owner ended without a result")
        });
        // Completion means callback teardown has finished; joining now does not
        // retain a blocking-pool thread for the lifetime of the mount.
        lore_base::lore_spawn_blocking_nocontext!(move || owner.join())
            .await
            .map_err(|error| io::Error::other(format!("mount owner join task failed: {error}")))?
            .map_err(|_panic| io::Error::other("foreground mount owner panicked"))?;
        result?
    }
}

#[cfg(target_os = "linux")]
pub use platform::PreparedMount;
#[cfg(target_os = "linux")]
pub use platform::foreground;
#[cfg(target_os = "linux")]
pub use platform::prepare;
#[cfg(unix)]
pub use platform::prepare_revision;
