// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Explicit immutable import into newly owned stores. Source metadata and
//! working files are never copied or converted into an external working tree.
use std::collections::BTreeSet;
use std::sync::Arc;

use lore_base::types::Hash;
use lore_base::types::RepositoryId;

use super::Error;
use super::Result;
use crate::immutable;
use crate::repository::RepositoryContext;
use crate::state;
use crate::state::State;
use crate::util::request_tracker::StoreRequestTracker;

mod linked;

#[derive(Clone, Copy, Debug)]
pub struct ImportLimits {
    pub max_scopes: usize,
    pub max_nodes: usize,
    pub max_fragments: usize,
    pub max_bytes: u64,
}
impl Default for ImportLimits {
    fn default() -> Self {
        Self {
            max_scopes: 64,
            max_nodes: 262_144,
            max_fragments: 1_000_000,
            max_bytes: 16 * 1024 * 1024 * 1024,
        }
    }
}
#[derive(Debug, Default)]
pub struct ImportStats {
    pub fragments: usize,
    pub payload_bytes: u64,
}
pub(super) struct ImportScope {
    pub repository: Arc<RepositoryContext>,
    pub state: Arc<State>,
    pub revision: Hash,
}
/// A current source repository's immutable read decision, not a serialized
/// credential or a store claim. Reuse must reacquire this from a real source.
#[derive(Clone)]
pub struct ImportReadPolicy {
    repository: RepositoryId,
    read: crate::repository::CanReadRepository,
}
impl ImportReadPolicy {
    pub fn from_repository(source: &RepositoryContext) -> Result<Self> {
        if source.id.is_zero() {
            return Err(Error::InvalidSelection);
        }
        Ok(Self {
            repository: source.id,
            read: source.link_read.clone(),
        })
    }
    pub fn repository_id(&self) -> RepositoryId {
        self.repository
    }
    pub fn apply(&self, destination: RepositoryContext) -> Result<RepositoryContext> {
        if destination.id != self.repository {
            return Err(Error::InvalidBinding);
        }
        Ok(destination.with_link_read(self.read.clone()))
    }
}
/// A source selection checked through its pinned linked routes. This owns only
/// finite import work; it must not be retained by the finished mounted backend.
pub struct ImmutableImport {
    sources: linked::LinkedScopes,
    limits: ImportLimits,
}
fn failure(error: impl std::fmt::Display) -> Error {
    Error::Preparation(format!("immutable working import: {error}"))
}
impl ImmutableImport {
    pub async fn new(source: Arc<RepositoryContext>, revision: Hash) -> Result<Self> {
        Self::with_limits(source, revision, ImportLimits::default()).await
    }
    pub async fn with_limits(
        source: Arc<RepositoryContext>,
        revision: Hash,
        limits: ImportLimits,
    ) -> Result<Self> {
        if limits.max_scopes == 0
            || limits.max_nodes == 0
            || limits.max_fragments == 0
            || limits.max_bytes == 0
            || limits.max_scopes > 4096
        {
            return Err(Error::InvalidLimit);
        }
        let sources = linked::collect(source, revision, limits).await?;
        if sources.scopes.is_empty() {
            return Err(Error::InvalidSelection);
        }
        Ok(Self { sources, limits })
    }
    pub fn repository_id(&self) -> RepositoryId {
        self.sources.scopes[0].repository.id
    }
    pub fn revision(&self) -> Hash {
        self.sources.scopes[0].revision
    }
    /// Keep the exact repository-read predicate. Imported bytes do not grant a
    /// previously denied linked repository access through a different exposure.
    pub fn apply_link_policy(&self, destination: RepositoryContext) -> RepositoryContext {
        destination.with_link_read(self.sources.scopes[0].repository.link_read.clone())
    }
    pub async fn copy_into(&self, destination: Arc<RepositoryContext>) -> Result<ImportStats> {
        if destination.id != self.repository_id() {
            return Err(Error::InvalidBinding);
        }
        self.sources.revalidate().await?;
        let mut copied = BTreeSet::new();
        let mut stats = ImportStats::default();
        let mut node_capacity = 0usize;
        for scope in &self.sources.scopes {
            // Collection imports the full immutable state, including metadata
            // outside a linked exposure. Bound that work before collecting it.
            node_capacity = scope
                .state
                .block_count()
                .checked_mul(crate::node::BLOCK_NODE_COUNT)
                .and_then(|nodes| node_capacity.checked_add(nodes))
                .filter(|nodes| *nodes <= self.limits.max_nodes)
                .ok_or(Error::InvalidLimit)?;
            self.sources.revalidate().await?;
            let empty = State::deserialize(scope.repository.clone(), Hash::default())
                .await
                .map_err(failure)?;
            let addresses = state::collect_fragments(
                scope.repository.clone(),
                empty,
                scope.state.clone(),
                state::Collection::All,
                Arc::new(StoreRequestTracker::default()),
            )
            .await
            .map_err(failure)?;
            if addresses.len() > self.limits.max_fragments {
                return Err(Error::InvalidLimit);
            }
            for address in addresses {
                if address.is_zero() || copied.contains(&(scope.repository.id, address)) {
                    continue;
                }
                if copied.len() == self.limits.max_fragments {
                    return Err(Error::InvalidLimit);
                }
                self.sources.revalidate().await?;
                let options = lore_storage::ReadOptions {
                    local: true,
                    remote: false,
                    cache: false,
                    decompress: false,
                    ..immutable::read_options_from_repository(&scope.repository)
                };
                let (fragment, payload) =
                    immutable::load_raw(scope.repository.clone(), address, options)
                        .await
                        .map_err(failure)?;
                self.sources.revalidate().await?;
                let total = stats
                    .payload_bytes
                    .checked_add(payload.len() as u64)
                    .filter(|total| *total <= self.limits.max_bytes)
                    .ok_or(Error::InvalidLimit)?;
                let stored = lore_storage::store_fragment(
                    destination.immutable_store(),
                    scope.repository.id,
                    address,
                    fragment,
                    payload,
                    true,
                    None,
                    immutable::write_context(None),
                    None,
                )
                .await
                .map_err(failure)?;
                if stored.address != address {
                    return Err(Error::InvalidSelection);
                }
                copied.insert((scope.repository.id, address));
                stats.fragments += 1;
                stats.payload_bytes = total;
            }
        }
        self.sources.revalidate().await?;
        destination.flush(true).await.map_err(failure)?;
        self.sources.revalidate().await?;
        Ok(stats)
    }
}
