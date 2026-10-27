// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use lore_vfs::digest_index::DigestIndex;

use super::Error;
use super::Phase;
#[cfg(target_os = "linux")]
use super::Record;
use super::Result;
use super::StartupReservation;
use super::overlaps;

fn problem(error: std::io::Error) -> Error {
    Error::Cache(error.to_string())
}
fn candidate(path: &Path) -> Result<PathBuf> {
    if !path.is_absolute() || path.as_os_str().as_encoded_bytes().len() > 4096 {
        return Err(Error::Cache(
            "cache requires an absolute path of at most 4096 bytes".into(),
        ));
    }
    let canonical = match std::fs::symlink_metadata(path) {
        Ok(metadata) => {
            if metadata.file_type().is_symlink() || !metadata.is_dir() {
                return Err(Error::Cache(
                    "cache endpoint must be a directory, not a symlink".into(),
                ));
            }
            path.canonicalize().map_err(problem)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let parent = path
                .parent()
                .ok_or_else(|| Error::Cache("cache requires an existing parent directory".into()))?
                .canonicalize()
                .map_err(problem)?;
            Ok(parent.join(
                path.file_name()
                    .ok_or_else(|| Error::Cache("cache directory name is missing".into()))?,
            ))
        }
        Err(error) => Err(problem(error)),
    }?;
    if canonical.as_os_str().as_encoded_bytes().len() > 4096 {
        return Err(Error::Cache(
            "canonical cache path exceeds 4096 bytes".into(),
        ));
    }
    Ok(canonical)
}
/// Actual directory creation runs only on the ticket-owning setup thread and
/// after the path has been reserved against every managed ownership root.
pub(super) fn prepare(
    ticket: &StartupReservation,
    path: Option<PathBuf>,
) -> Result<Option<DigestIndex>> {
    let Some(path) = path else {
        return Ok(None);
    };
    let path = candidate(&path)?;
    {
        let state = ticket
            .supervisor
            .state
            .lock()
            .map_err(|_poison| Error::Poisoned)?;
        if state.closed
            || state
                .records
                .get(&ticket.record.id)
                .is_none_or(|record| !Arc::ptr_eq(record, &ticket.record))
        {
            return Err(Error::Closed);
        }
        let phase = ticket
            .record
            .state
            .lock()
            .map_err(|_poison| Error::Poisoned)?;
        if phase.phase != Phase::Starting {
            return Err(Error::Closed);
        }
        for record in state.records.values() {
            if record
                .paths
                .paths()
                .iter()
                .any(|owned| overlaps(&path, owned))
            {
                return Err(Error::Overlap);
            }
        }
        let mut reserved = ticket
            .record
            .digest_cache
            .lock()
            .map_err(|_poison| Error::Poisoned)?;
        if reserved.as_ref().is_some_and(|previous| previous != &path) {
            return Err(Error::InvalidBinding);
        }
        *reserved = Some(path.clone());
    }
    let validated = crate::mount::validate_digest_cache(&path, ticket.record.paths.target())
        .map_err(problem)?;
    if validated != path {
        return Err(Error::Cache(
            "cache location changed during validation".into(),
        ));
    }
    ticket.check_live()?;
    Ok(Some(DigestIndex::new(validated)))
}
#[cfg(target_os = "linux")]
pub(super) fn revalidate(record: &Record) -> Result<()> {
    let cache = record
        .digest_cache
        .lock()
        .map_err(|_poison| Error::Poisoned)?
        .clone();
    if let Some(cache) = cache {
        if candidate(&cache)? != cache {
            return Err(Error::Cache(
                "cache location changed before mounting".into(),
            ));
        }
        let validated =
            crate::mount::validate_digest_cache(&cache, record.paths.target()).map_err(problem)?;
        if validated != cache {
            return Err(Error::Cache(
                "cache location changed before mounting".into(),
            ));
        }
    }
    Ok(())
}
