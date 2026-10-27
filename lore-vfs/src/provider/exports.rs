// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use std::fs::File;
#[cfg(unix)]
use std::fs::OpenOptions;
use std::fs::{self};
#[cfg(unix)]
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use lore_revision::fs::filesystem_provider::FsError;

use super::Limits;
use crate::checkpoint_owner::CheckpointOwner;

mod jobs;
#[cfg(test)]
pub(super) use jobs::Pause as ExportPause;

static NEXT: AtomicU64 = AtomicU64::new(1);
#[derive(Clone, Copy)]
struct Budget {
    files: usize,
    bytes: u64,
}
pub(super) struct Owner {
    path: PathBuf,
    _lock: File,
    limits: Limits,
    budget: Mutex<Budget>,
    jobs: Arc<jobs::Jobs>,
}
impl Owner {
    #[cfg(unix)]
    pub(super) fn open(path: PathBuf, limits: Limits) -> Result<Arc<Self>, FsError> {
        use std::os::unix::fs::DirBuilderExt;
        use std::os::unix::fs::OpenOptionsExt;
        if limits.max_export_files == 0 {
            return Err(FsError::internal("export file budget must be nonzero"));
        }
        match fs::DirBuilder::new().mode(0o700).create(&path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error.into()),
        }
        private(&path, true)?;
        let lock_path = path.join("owner.lock");
        let lock = match OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&lock_path)
        {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                private(&lock_path, false)?;
                OpenOptions::new()
                    .read(true)
                    .write(true)
                    .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
                    .open(&lock_path)?
            }
            Err(error) => return Err(error.into()),
        };
        lock.try_lock()
            .map_err(|_lock_error| FsError::internal("export owner is already locked"))?;
        // Unrecognized or abandoned copies are never silently deleted or omitted
        // from the disk budget. Use a dedicated empty export directory.
        for entry in fs::read_dir(&path)? {
            if entry?.file_name() != std::ffi::OsStr::new("owner.lock") {
                return Err(FsError::internal(
                    "export directory contains retained data; inspect or use an empty directory",
                ));
            }
        }
        Ok(Arc::new(Self {
            path,
            _lock: lock,
            limits,
            budget: Mutex::new(Budget { files: 0, bytes: 0 }),
            jobs: jobs::Jobs::new(),
        }))
    }
    #[cfg(not(unix))]
    pub(super) fn open(_path: PathBuf, _limits: Limits) -> Result<Arc<Self>, FsError> {
        Err(FsError::internal(
            "private operation exports require Unix ownership",
        ))
    }
    pub(super) async fn operation(
        self: &Arc<Self>,
        checkpoint: Option<Arc<CheckpointOwner>>,
    ) -> Result<Arc<OperationExports>, FsError> {
        let directory = self.jobs.directory()?;
        let owner = self.clone();
        self.jobs
            .run(jobs::Kind::Directory, move || {
                #[cfg(unix)]
                use std::os::unix::fs::DirBuilderExt;
                let id = NEXT
                    .fetch_update(Ordering::AcqRel, Ordering::Acquire, |id| id.checked_add(1))
                    .map_err(|_overflow| FsError::internal("export identity limit"))?;
                let path = owner
                    .path
                    .join(format!("operation-{}-{id}", std::process::id()));
                let mut builder = fs::DirBuilder::new();
                #[cfg(unix)]
                builder.mode(0o700);
                builder.create(&path)?;
                Ok(Arc::new(OperationExports {
                    owner,
                    path,
                    reserved: Mutex::new(Budget { files: 0, bytes: 0 }),
                    directory: Some(directory),
                    checkpoint,
                }))
            })
            .await
    }
    pub(super) async fn drain(&self) -> Result<(), FsError> {
        self.jobs.drain().await
    }
    pub(super) async fn cache_job<T: Send + 'static>(
        self: &Arc<Self>,
        checkpoint: Option<Arc<CheckpointOwner>>,
        work: impl FnOnce() -> Result<T, FsError> + Send + 'static,
    ) -> Result<T, FsError> {
        let owner = self.clone();
        self.jobs
            .run(jobs::Kind::Cache, move || {
                let _owner = owner;
                let _checkpoint = checkpoint;
                work()
            })
            .await
    }
    #[cfg(test)]
    pub(super) fn test_pause(&self) -> Arc<jobs::Pause> {
        self.jobs.pause(None)
    }
    #[cfg(test)]
    pub(super) fn test_pause_write(&self) -> Arc<jobs::Pause> {
        self.jobs.pause(Some(jobs::Kind::Write))
    }
    #[cfg(test)]
    pub(super) fn test_pause_cleanup(&self) -> Arc<jobs::Pause> {
        self.jobs.pause(Some(jobs::Kind::Cleanup))
    }
    #[cfg(test)]
    pub(super) fn test_fail_cleanup_spawn(&self) {
        self.jobs.fail_cleanup_spawn();
    }
}
#[cfg(unix)]
fn private(path: &Path, directory: bool) -> Result<(), FsError> {
    use std::os::unix::fs::MetadataExt;
    let metadata = fs::symlink_metadata(path)?;
    // SAFETY: geteuid has no arguments or preconditions.
    let uid = unsafe { libc::geteuid() };
    if metadata.file_type().is_symlink()
        || (directory && !metadata.is_dir())
        || (!directory && !metadata.is_file())
        || metadata.uid() != uid
        || metadata.mode() & 0o077 != 0
    {
        return Err(FsError::internal(
            "exports require private owned regular storage",
        ));
    }
    Ok(())
}
pub(super) struct OperationExports {
    owner: Arc<Owner>,
    path: PathBuf,
    reserved: Mutex<Budget>,
    directory: Option<jobs::Directory>,
    // Exact backing storage ownership, independent of provider/model lifetimes.
    checkpoint: Option<Arc<CheckpointOwner>>,
}
impl OperationExports {
    pub(super) async fn create(self: &Arc<Self>, path: PathBuf) -> Result<Arc<Writer>, FsError> {
        let exports = self.clone();
        self.owner
            .jobs
            .run(jobs::Kind::Create, move || {
                let mut options = fs::OpenOptions::new();
                options.write(true).create_new(true);
                #[cfg(unix)]
                {
                    use std::os::unix::fs::OpenOptionsExt;
                    options
                        .mode(0o600)
                        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
                }
                let file = options.open(path)?;
                Ok(Arc::new(Writer {
                    file: Mutex::new(file),
                    exports,
                }))
            })
            .await
    }
    pub(super) fn reserve(&self, size: u64) -> Result<PathBuf, FsError> {
        let mut total = self
            .owner
            .budget
            .lock()
            .map_err(|_poisoned| FsError::internal("export budget poisoned"))?;
        let mut local = self
            .reserved
            .lock()
            .map_err(|_poisoned| FsError::internal("operation export budget poisoned"))?;
        let bytes = total
            .bytes
            .checked_add(size)
            .filter(|value| *value <= self.owner.limits.max_export_bytes)
            .ok_or_else(|| FsError::internal("export byte limit"))?;
        if total.files >= self.owner.limits.max_export_files {
            return Err(FsError::internal("export file limit"));
        }
        total.bytes = bytes;
        total.files += 1;
        local.bytes += size;
        local.files += 1;
        Ok(self.path.join(format!("file-{}", local.files)))
    }
}
impl Drop for OperationExports {
    fn drop(&mut self) {
        let Some(directory) = self.directory.take() else {
            return;
        };
        let owner = self.owner.clone();
        let checkpoint = self.checkpoint.take();
        let path = self.path.clone();
        let local = self
            .reserved
            .get_mut()
            .map(|value| *value)
            .map_err(|_poison| ());
        self.owner.jobs.cleanup(directory, move || {
            let _checkpoint = checkpoint;
            fs::remove_dir_all(path)?;
            let local =
                local.map_err(|()| FsError::internal("operation export budget poisoned"))?;
            let mut total = owner
                .budget
                .lock()
                .map_err(|_poison| FsError::internal("export budget poisoned"))?;
            total.files = total
                .files
                .checked_sub(local.files)
                .ok_or_else(|| FsError::internal("export file accounting mismatch"))?;
            total.bytes = total
                .bytes
                .checked_sub(local.bytes)
                .ok_or_else(|| FsError::internal("export byte accounting mismatch"))?;
            Ok(())
        });
    }
}

pub(super) struct Writer {
    file: Mutex<File>,
    exports: Arc<OperationExports>,
}
impl Writer {
    pub(super) async fn write(
        self: &Arc<Self>,
        offset: u64,
        bytes: Vec<u8>,
    ) -> Result<(), FsError> {
        use std::io::Seek;
        use std::io::Write;
        let writer = self.clone();
        self.exports
            .owner
            .jobs
            .run(jobs::Kind::Write, move || {
                let mut file = writer
                    .file
                    .lock()
                    .map_err(|_poison| FsError::internal("export writer poisoned"))?;
                file.seek(std::io::SeekFrom::Start(offset))?;
                file.write_all(&bytes)?;
                Ok(())
            })
            .await
    }
}
