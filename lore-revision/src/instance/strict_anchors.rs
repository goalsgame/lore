// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Mutable-store-only anchor inspection for external filesystem recovery.
//! These reads neither migrate legacy files nor repair missing metadata.

use std::fmt;

use lore_base::types::Hash;
use lore_storage::StoreError;

use super::InstanceId;
use crate::lore::BranchId;
use crate::lore::RepositoryId;
use crate::repository::RepositoryContext;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Snapshot {
    pub repository: RepositoryId,
    pub instance: InstanceId,
    /// Zero denotes the canonical empty revision, including an absent key.
    pub current: Hash,
    pub branch: BranchId,
    pub staged: Option<Hash>,
}

#[derive(Debug)]
pub enum Error {
    InvalidIdentity,
    InvalidBranch,
    Store(StoreError),
}
impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidIdentity => formatter.write_str("anchor repository or instance is zero"),
            Self::InvalidBranch => formatter.write_str("branch anchor is absent or malformed"),
            Self::Store(error) => write!(formatter, "reading instance anchors: {error}"),
        }
    }
}
impl std::error::Error for Error {}

/// Read exact current, branch and staged anchors from the normal mutable store.
/// Only `AddressNotFound` or a zero value denotes absence. In particular,
/// inaccessible or missing backing payloads are errors, not empty revisions.
///
/// The caller must serialize anchor writers and retain the store claim for a
/// coherent snapshot. This function does not acquire a command lock, flush the
/// store, or establish a transaction across these three separate reads.
pub async fn read(repository: &RepositoryContext) -> Result<Snapshot, Error> {
    if repository.id.is_zero() || repository.instance_id.is_zero() {
        return Err(Error::InvalidIdentity);
    }
    let current = value(repository, super::ANCHOR_CURRENT)
        .await?
        .unwrap_or_default();
    let encoded_branch = value(repository, super::ANCHOR_CURRENT_BRANCH)
        .await?
        .ok_or(Error::InvalidBranch)?;
    let branch = encoded_branch.to_context();
    if branch.is_zero() || encoded_branch != Hash::from_context(branch) {
        return Err(Error::InvalidBranch);
    }
    let staged = value(repository, super::ANCHOR_STAGED).await?;
    Ok(Snapshot {
        repository: repository.id,
        instance: repository.instance_id,
        current,
        branch,
        staged,
    })
}

async fn value(repository: &RepositoryContext, function: &str) -> Result<Option<Hash>, Error> {
    let (key, kind) = super::anchor_key(repository.salt(), function, repository.instance_id);
    match repository
        .read_mutable_store()
        .load(repository.id, key, kind)
        .await
    {
        Ok(value) => Ok((!value.is_zero()).then_some(value)),
        Err(StoreError::AddressNotFound(_)) => Ok(None),
        Err(error) => Err(Error::Store(error)),
    }
}
