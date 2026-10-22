// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use async_trait::async_trait;
use lore_base::types::Hash;
use lore_base::types::RepositoryId;
use lore_server::authnz::repository_name_resolver::RepositoryNameResolver;
use tonic::Status;

/// Serves a repository's metadata hash and name from tables a test can change
/// between calls, and counts the name reads and pointer reads.
#[derive(Default)]
pub struct SwitchableResolver {
    current: Mutex<HashMap<RepositoryId, Hash>>,
    names: Mutex<HashMap<Hash, Arc<str>>>,
    unpinned: Mutex<Option<Arc<str>>>,
    resolves: AtomicUsize,
    incarnations: AtomicUsize,
}

impl SwitchableResolver {
    pub fn new() -> Arc<Self> {
        Arc::default()
    }

    /// Points `repository` at the metadata blob `hash`, which holds `name`.
    pub fn set(&self, repository: RepositoryId, hash: Hash, name: &str) {
        self.current.lock().unwrap().insert(repository, hash);
        self.names.lock().unwrap().insert(hash, Arc::from(name));
    }

    /// Makes unpinned `resolve` answer `name` whatever the pointer holds.
    pub fn answer_unpinned(&self, name: &str) {
        *self.unpinned.lock().unwrap() = Some(Arc::from(name));
    }

    /// Name reads served, pinned or not.
    pub fn resolves(&self) -> usize {
        self.resolves.load(Ordering::SeqCst)
    }

    /// Pointer reads served.
    pub fn incarnations(&self) -> usize {
        self.incarnations.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl RepositoryNameResolver for SwitchableResolver {
    async fn resolve(&self, repository_id: RepositoryId) -> Result<Option<Arc<str>>, Status> {
        self.resolves.fetch_add(1, Ordering::SeqCst);
        if let Some(name) = self.unpinned.lock().unwrap().clone() {
            return Ok(Some(name));
        }
        let hash = self.current.lock().unwrap().get(&repository_id).copied();
        Ok(hash.and_then(|hash| self.names.lock().unwrap().get(&hash).cloned()))
    }

    async fn resolve_at(
        &self,
        _repository_id: RepositoryId,
        hash: Hash,
    ) -> Result<Option<Arc<str>>, Status> {
        self.resolves.fetch_add(1, Ordering::SeqCst);
        if hash == Hash::default() {
            return Ok(None);
        }
        Ok(self.names.lock().unwrap().get(&hash).cloned())
    }

    async fn incarnation(&self, repository_id: RepositoryId) -> Result<Option<Hash>, Status> {
        self.incarnations.fetch_add(1, Ordering::SeqCst);
        Ok(self.current.lock().unwrap().get(&repository_id).copied())
    }
}
