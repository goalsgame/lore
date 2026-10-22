// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use async_trait::async_trait;
use lore_base::runtime::LORE_CONTEXT;
use lore_base::types::Hash;
use lore_base::types::RepositoryId;
use lore_server::authnz::repository_name_resolver::*;
use tonic::Code;
use tonic::Status;

use crate::store::test_support::seed_repository;
use crate::store::test_support::test_store_create;

/// Serves metadata hashes and names from fixed tables and counts the reads
/// of each kind.
struct CountingSource {
    hashes: Mutex<HashMap<RepositoryId, Hash>>,
    names: Mutex<HashMap<Hash, Arc<str>>>,
    hash_reads: AtomicUsize,
    name_reads: AtomicUsize,
    fail: Option<Code>,
}

impl CountingSource {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            hashes: Mutex::default(),
            names: Mutex::default(),
            hash_reads: AtomicUsize::new(0),
            name_reads: AtomicUsize::new(0),
            fail: None,
        })
    }

    fn failing(code: Code) -> Arc<Self> {
        Arc::new(Self {
            hashes: Mutex::default(),
            names: Mutex::default(),
            hash_reads: AtomicUsize::new(0),
            name_reads: AtomicUsize::new(0),
            fail: Some(code),
        })
    }

    fn hash_reads(&self) -> usize {
        self.hash_reads.load(Ordering::SeqCst)
    }

    fn name_reads(&self) -> usize {
        self.name_reads.load(Ordering::SeqCst)
    }

    /// Points `id` at `hash`, which holds `name`.
    fn set(&self, id: RepositoryId, hash: Hash, name: &str) {
        self.hashes.lock().unwrap().insert(id, hash);
        self.names.lock().unwrap().insert(hash, Arc::from(name));
    }

    fn clear(&self, id: RepositoryId) {
        self.hashes.lock().unwrap().remove(&id);
    }
}

#[async_trait]
impl RepositoryNameSource for CountingSource {
    async fn metadata_hash(&self, repository_id: RepositoryId) -> Result<Option<Hash>, Status> {
        self.hash_reads.fetch_add(1, Ordering::SeqCst);
        if let Some(code) = self.fail {
            return Err(Status::new(code, "store down"));
        }
        Ok(self.hashes.lock().unwrap().get(&repository_id).copied())
    }

    async fn name_at(
        &self,
        _repository_id: RepositoryId,
        hash: Hash,
    ) -> Result<Option<Arc<str>>, Status> {
        self.name_reads.fetch_add(1, Ordering::SeqCst);
        Ok(self.names.lock().unwrap().get(&hash).cloned())
    }
}

fn id() -> RepositoryId {
    rand::random()
}

fn hash() -> Hash {
    Hash::from(rand::random::<[u8; 32]>())
}

#[tokio::test]
async fn a_found_name_is_read_once_per_hash() {
    let repository = id();
    let source = CountingSource::new();
    source.set(repository, hash(), "goalables/app");
    let cached = CachedNameResolver::new(source.clone(), 8);

    for _ in 0..3 {
        let name = cached.resolve(repository).await.unwrap().unwrap();
        assert_eq!(&*name, "goalables/app");
    }
    assert_eq!(source.hash_reads(), 3);
    assert_eq!(source.name_reads(), 1);
    assert_eq!(cached.len(), 1);
}

#[tokio::test]
async fn a_missing_repository_is_not_remembered() {
    let repository = id();
    let source = CountingSource::new();
    let cached = CachedNameResolver::new(source.clone(), 8);

    assert!(cached.resolve(repository).await.unwrap().is_none());
    assert!(cached.is_empty());
    assert_eq!(source.name_reads(), 0);

    source.set(repository, hash(), "goalables/app");
    let name = cached.resolve(repository).await.unwrap().unwrap();
    assert_eq!(&*name, "goalables/app");
    assert_eq!(source.hash_reads(), 2);
}

#[tokio::test]
async fn errors_pass_through_and_are_not_remembered() {
    let cached = CachedNameResolver::new(CountingSource::failing(Code::Unavailable), 8);
    let err = cached.resolve(id()).await.unwrap_err();
    assert_eq!(err.code(), Code::Unavailable);
    assert!(cached.is_empty());

    let cached = CachedNameResolver::new(CountingSource::failing(Code::ResourceExhausted), 8);
    let err = cached.resolve(id()).await.unwrap_err();
    assert_eq!(err.code(), Code::ResourceExhausted);
    assert!(cached.is_empty());
}

#[tokio::test]
async fn the_cache_never_grows_past_its_capacity() {
    let repositories: Vec<RepositoryId> = (0..10).map(|_| id()).collect();
    let source = CountingSource::new();
    for repository in &repositories {
        source.set(*repository, hash(), "goalables/app");
    }
    let cached = CachedNameResolver::new(source.clone(), 4);

    for repository in &repositories {
        cached.resolve(*repository).await.unwrap().unwrap();
        assert!(cached.len() <= 4, "cache holds {} names", cached.len());
    }
    for repository in &repositories {
        let name = cached.resolve(*repository).await.unwrap().unwrap();
        assert_eq!(&*name, "goalables/app");
    }
}

#[tokio::test]
async fn a_reused_repository_id_never_resolves_to_its_old_name() {
    let repository = id();
    let source = CountingSource::new();
    let (first, second) = (hash(), hash());
    source.set(repository, first, "goalables/app");
    let cached = CachedNameResolver::new(source.clone(), 8);

    for _ in 0..2 {
        let name = cached.resolve(repository).await.unwrap().unwrap();
        assert_eq!(&*name, "goalables/app");
    }
    assert_eq!(source.name_reads(), 1);

    source.clear(repository);
    assert!(cached.resolve(repository).await.unwrap().is_none());

    source.set(repository, second, "game");
    for _ in 0..2 {
        let name = cached.resolve(repository).await.unwrap().unwrap();
        assert_eq!(&*name, "game");
    }
}

#[tokio::test]
async fn repositories_sharing_a_metadata_hash_share_one_name_read() {
    let (first, second) = (id(), id());
    let shared = hash();
    let source = CountingSource::new();
    source.set(first, shared, "goalables/app");
    source.set(second, shared, "goalables/app");
    let cached = CachedNameResolver::new(source.clone(), 8);

    cached.resolve(first).await.unwrap().unwrap();
    cached.resolve(second).await.unwrap().unwrap();
    assert_eq!(source.name_reads(), 1);
    assert_eq!(cached.len(), 1);
}

#[tokio::test]
async fn the_store_source_reads_the_name_from_repository_metadata() {
    let (immutable_store, mutable_store, execution) =
        test_store_create().await.expect("Failed to create stores");
    LORE_CONTEXT
        .scope(execution, async move {
            let repository = id();
            seed_repository(
                immutable_store.clone(),
                mutable_store.clone(),
                repository,
                "goalables/app",
            )
            .await;
            let resolver = StoreNameSource::new(immutable_store, mutable_store);

            let hash = resolver.metadata_hash(repository).await.unwrap().unwrap();
            let name = resolver.name_at(repository, hash).await.unwrap().unwrap();
            assert_eq!(&*name, "goalables/app");
            assert!(resolver.metadata_hash(id()).await.unwrap().is_none());
        })
        .await;
}
