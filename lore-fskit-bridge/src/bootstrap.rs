// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

use super::*;

#[cfg(unix)]
pub(super) fn open(path: &[u8], cancel: Arc<BootstrapCancel>) -> Result<Arc<Core>, i32> {
    use std::io::Read;
    use std::os::unix::fs::MetadataExt;
    use std::os::unix::fs::OpenOptionsExt;
    use std::path::Path;
    use std::time::Duration;

    use serde::Deserialize;
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Config {
        repository: String,
        revision: String,
        identity: Option<String>,
        digest_cache: Option<String>,
        #[serde(default)]
        offline: bool,
    }
    let path = std::str::from_utf8(path).map_err(|_encoding| INVALID)?;
    if !Path::new(path).is_absolute() {
        return Err(INVALID);
    }
    let file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)
        .map_err(|error| {
            if error.kind() == std::io::ErrorKind::PermissionDenied {
                3
            } else {
                IO
            }
        })?;
    let metadata = file.metadata().map_err(|_stat| IO)?;
    // SAFETY: getuid has no memory-safety preconditions.
    let uid = unsafe { libc::getuid() };
    if !metadata.is_file()
        || metadata.uid() != uid
        || metadata.mode() & 0o077 != 0
        || metadata.len() > 8192
    {
        return Err(INVALID);
    }
    let mut bytes = Vec::new();
    file.take(8193)
        .read_to_end(&mut bytes)
        .map_err(|_read| IO)?;
    if bytes.len() > 8192 {
        return Err(LIMIT);
    }
    // Configuration syntax and field contents are never logged or returned.
    let config: Config = serde_json::from_slice(&bytes).map_err(|_config| INVALID)?;
    if !Path::new(&config.repository).is_absolute()
        || config.revision.is_empty()
        || config.revision.len() > 256
        || config
            .identity
            .as_ref()
            .is_some_and(|value| value.len() > 256)
        || config
            .digest_cache
            .as_ref()
            .is_some_and(|value| !Path::new(value).is_absolute())
    {
        return Err(INVALID);
    }
    let globals = lore::interface::LoreGlobalArgs {
        repository_path: config.repository.clone().into(),
        working_directory: config.repository.into(),
        identity: config.identity.unwrap_or_default().into(),
        offline: u8::from(config.offline),
        ..Default::default()
    };
    if cancel.cancelled.load(Ordering::Acquire) {
        return Err(CANCELLED);
    }
    lore::runtime().block_on(async move {
        tokio::select! {
        _=cancel.notify.notified()=>Err(CANCELLED),
        result=tokio::time::timeout(
            Duration::from_secs(30),
            lore::mount::prepare_revision(
                globals,
                config.revision,
                config.digest_cache.map(Into::into),
                None,
            ),
        )=>result.map_err(|_timeout| IO)?.map_err(|setup|match setup {16..=18=>3,89=>2,3|4=>INVALID,_=>IO}),
        }
    })
}
#[cfg(not(unix))]
pub(super) fn open(_path: &[u8], _cancel: Arc<BootstrapCancel>) -> Result<Arc<Core>, i32> {
    Err(10)
}
