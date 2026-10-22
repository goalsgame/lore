// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;

use async_trait::async_trait;
use lore_base::runtime::LORE_CONTEXT;
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
}

/// Reads names from the repository metadata held in the stores.
pub struct StoreNameResolver {
    immutable_store: Arc<dyn ImmutableStore>,
    mutable_store: Arc<dyn MutableStore>,
}

impl StoreNameResolver {
    pub fn new(
        immutable_store: Arc<dyn ImmutableStore>,
        mutable_store: Arc<dyn MutableStore>,
    ) -> Self {
        Self {
            immutable_store,
            mutable_store,
        }
    }
}

#[async_trait]
impl RepositoryNameResolver for StoreNameResolver {
    async fn resolve(&self, repository_id: RepositoryId) -> Result<Option<Arc<str>>, Status> {
        let execution = setup_execution(module_path!(), String::new(), "<resolver>".to_string());
        let repository = Arc::new(RepositoryContext::new_server_context(
            self.immutable_store.clone(),
            self.mutable_store.clone(),
            repository_id,
        ));
        LORE_CONTEXT
            .scope(execution, async move {
                let hash = match repository::metadata_hash(repository.clone())
                    .await
                    .filter_slow_down()?
                {
                    Ok(hash) => hash,
                    Err(err) => {
                        debug!(%repository_id, "No repository metadata: {err}");
                        return Ok(None);
                    }
                };
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

/// Remembers the names another resolver found. Names never change, so an
/// entry is kept until the cache is full and then dropped with the rest.
/// Repositories not found are not remembered.
pub struct CachedNameResolver {
    inner: Arc<dyn RepositoryNameResolver>,
    capacity: usize,
    names: Mutex<HashMap<RepositoryId, Arc<str>>>,
}

impl CachedNameResolver {
    pub fn new(inner: Arc<dyn RepositoryNameResolver>, capacity: usize) -> Self {
        Self {
            inner,
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
        if let Some(name) = self
            .names
            .lock()
            .expect("name cache lock")
            .get(&repository_id)
        {
            return Ok(Some(name.clone()));
        }
        let Some(name) = self.inner.resolve(repository_id).await? else {
            return Ok(None);
        };
        let mut names = self.names.lock().expect("name cache lock");
        if names.len() >= self.capacity {
            names.clear();
        }
        names.insert(repository_id, name.clone());
        Ok(Some(name))
    }
}

/// The resolver the server uses: store-backed and cached.
pub fn repository_name_resolver(
    immutable_store: Arc<dyn ImmutableStore>,
    mutable_store: Arc<dyn MutableStore>,
) -> Arc<dyn RepositoryNameResolver> {
    Arc::new(CachedNameResolver::new(
        Arc::new(StoreNameResolver::new(immutable_store, mutable_store)),
        NAME_CACHE_CAPACITY,
    ))
}
