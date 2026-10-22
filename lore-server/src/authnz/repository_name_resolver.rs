// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;

use async_trait::async_trait;
use lore_base::runtime::LORE_CONTEXT;
use lore_base::types::Hash;
use lore_base::types::RepositoryId;
use lore_revision::repository;
use lore_revision::repository::RepositoryContext;
use lore_storage::ImmutableStore;
use lore_storage::MutableStore;
use tonic::Status;
use tracing::debug;

use crate::grpc::FilterSlowDownExt;
use crate::util::setup_execution;

/// Most repository names a [`CachedNameResolver`] holds before it starts over.
pub const NAME_CACHE_CAPACITY: usize = 4096;

/// Maps a repository id to its name, for authorization decisions that are
/// scoped by name while the request only carries an id.
#[async_trait]
pub trait RepositoryNameResolver: Send + Sync {
    /// The name of `repository_id`, or `Ok(None)` when no such repository exists.
    async fn resolve(&self, repository_id: RepositoryId) -> Result<Option<Arc<str>>, Status>;

    /// The name held by the metadata blob `hash` of `repository_id`.
    /// `Ok(None)` for the default hash, which stands for no metadata, and for
    /// a blob that cannot be read.
    async fn resolve_at(
        &self,
        repository_id: RepositoryId,
        hash: Hash,
    ) -> Result<Option<Arc<str>>, Status>;

    /// The hash of the repository's current metadata, read fresh. It changes
    /// when the repository is deleted and recreated, so a holder of grants
    /// derived from a name can tell they may no longer apply.
    async fn incarnation(&self, repository_id: RepositoryId) -> Result<Option<Hash>, Status>;
}

/// The two store reads behind a name lookup: the repository's current
/// metadata pointer, and the metadata blob that pointer names.
#[async_trait]
pub trait RepositoryNameSource: Send + Sync {
    /// The hash of the repository's current metadata, read fresh on every
    /// call. `Ok(None)` when the repository has no metadata.
    async fn metadata_hash(&self, repository_id: RepositoryId) -> Result<Option<Hash>, Status>;

    /// The name held by the metadata blob `hash`, or `Ok(None)` when the
    /// blob cannot be read.
    async fn name_at(
        &self,
        repository_id: RepositoryId,
        hash: Hash,
    ) -> Result<Option<Arc<str>>, Status>;
}

/// Reads names from the repository metadata held in the stores.
pub struct StoreNameSource {
    immutable_store: Arc<dyn ImmutableStore>,
    mutable_store: Arc<dyn MutableStore>,
}

impl StoreNameSource {
    pub fn new(
        immutable_store: Arc<dyn ImmutableStore>,
        mutable_store: Arc<dyn MutableStore>,
    ) -> Self {
        Self {
            immutable_store,
            mutable_store,
        }
    }

    fn repository(&self, repository_id: RepositoryId) -> Arc<RepositoryContext> {
        Arc::new(RepositoryContext::new_server_context(
            self.immutable_store.clone(),
            self.mutable_store.clone(),
            repository_id,
        ))
    }
}

#[async_trait]
impl RepositoryNameSource for StoreNameSource {
    async fn metadata_hash(&self, repository_id: RepositoryId) -> Result<Option<Hash>, Status> {
        let execution = setup_execution(module_path!(), String::new(), "<resolver>".to_string());
        let repository = self.repository(repository_id);
        LORE_CONTEXT
            .scope(execution, async move {
                match repository::metadata_hash(repository)
                    .await
                    .filter_slow_down()?
                {
                    Ok(hash) if hash != Hash::default() => Ok(Some(hash)),
                    Ok(_) => Ok(None),
                    Err(err) => {
                        debug!(%repository_id, "No repository metadata: {err}");
                        Ok(None)
                    }
                }
            })
            .await
    }

    async fn name_at(
        &self,
        repository_id: RepositoryId,
        hash: Hash,
    ) -> Result<Option<Arc<str>>, Status> {
        let execution = setup_execution(module_path!(), String::new(), "<resolver>".to_string());
        let repository = self.repository(repository_id);
        LORE_CONTEXT
            .scope(execution, async move {
                match repository::metadata(repository, hash)
                    .await
                    .filter_slow_down()?
                {
                    Ok(metadata) => Ok(Some(Arc::from(metadata.name))),
                    Err(err) => {
                        debug!(%repository_id, "Unreadable repository metadata: {err}");
                        Ok(None)
                    }
                }
            })
            .await
    }
}

/// Resolves names through a [`RepositoryNameSource`], remembering each name
/// by the hash of the metadata blob that holds it. The current hash is read
/// on every lookup and a blob's content never changes, so a deleted or
/// recreated repository never resolves to an old name. The cache is dropped
/// whole when it is full. Repositories without metadata are not remembered.
pub struct CachedNameResolver {
    source: Arc<dyn RepositoryNameSource>,
    capacity: usize,
    names: Mutex<HashMap<Hash, Arc<str>>>,
}

impl CachedNameResolver {
    pub fn new(source: Arc<dyn RepositoryNameSource>, capacity: usize) -> Self {
        Self {
            source,
            capacity: capacity.max(1),
            names: Mutex::new(HashMap::new()),
        }
    }

    /// The number of names currently held.
    pub fn len(&self) -> usize {
        self.names.lock().expect("name cache lock").len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[async_trait]
impl RepositoryNameResolver for CachedNameResolver {
    async fn resolve(&self, repository_id: RepositoryId) -> Result<Option<Arc<str>>, Status> {
        let Some(hash) = self.source.metadata_hash(repository_id).await? else {
            return Ok(None);
        };
        self.resolve_at(repository_id, hash).await
    }

    async fn resolve_at(
        &self,
        repository_id: RepositoryId,
        hash: Hash,
    ) -> Result<Option<Arc<str>>, Status> {
        if hash == Hash::default() {
            return Ok(None);
        }
        if let Some(name) = self.names.lock().expect("name cache lock").get(&hash) {
            return Ok(Some(name.clone()));
        }
        let Some(name) = self.source.name_at(repository_id, hash).await? else {
            return Ok(None);
        };
        let mut names = self.names.lock().expect("name cache lock");
        if names.len() >= self.capacity {
            names.clear();
        }
        names.insert(hash, name.clone());
        Ok(Some(name))
    }

    async fn incarnation(&self, repository_id: RepositoryId) -> Result<Option<Hash>, Status> {
        self.source.metadata_hash(repository_id).await
    }
}

/// The resolver the server uses: store-backed and cached.
pub fn repository_name_resolver(
    immutable_store: Arc<dyn ImmutableStore>,
    mutable_store: Arc<dyn MutableStore>,
) -> Arc<dyn RepositoryNameResolver> {
    Arc::new(CachedNameResolver::new(
        Arc::new(StoreNameSource::new(immutable_store, mutable_store)),
        NAME_CACHE_CAPACITY,
    ))
}
