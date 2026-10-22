// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use async_trait::async_trait;
use lore_base::runtime::LORE_CONTEXT;
use lore_base::types::RepositoryId;
use lore_server::authnz::repository_name_resolver::*;
use tonic::Code;
use tonic::Status;

use crate::store::test_support::seed_repository;
use crate::store::test_support::test_store_create;

/// Answers from a fixed table and counts the lookups it served.
struct CountingResolver {
    names: Mutex<HashMap<RepositoryId, Arc<str>>>,
    lookups: AtomicUsize,
    fail: bool,
}

impl CountingResolver {
    fn new(entries: &[(RepositoryId, &str)]) -> Arc<Self> {
        Arc::new(Self {
            names: Mutex::new(
                entries
                    .iter()
                    .map(|(id, name)| (*id, Arc::from(*name)))
                    .collect(),
            ),
            lookups: AtomicUsize::new(0),
            fail: false,
        })
    }

    fn failing() -> Arc<Self> {
        Arc::new(Self {
            names: Mutex::default(),
            lookups: AtomicUsize::new(0),
            fail: true,
        })
    }

    fn lookups(&self) -> usize {
        self.lookups.load(Ordering::SeqCst)
    }

    fn add(&self, id: RepositoryId, name: &str) {
        self.names.lock().unwrap().insert(id, Arc::from(name));
    }
}

#[async_trait]
impl RepositoryNameResolver for CountingResolver {
    async fn resolve(&self, repository_id: RepositoryId) -> Result<Option<Arc<str>>, Status> {
        self.lookups.fetch_add(1, Ordering::SeqCst);
        if self.fail {
            return Err(Status::unavailable("store down"));
        }
        Ok(self.names.lock().unwrap().get(&repository_id).cloned())
    }
}

fn id() -> RepositoryId {
    rand::random()
}

#[tokio::test]
async fn a_found_name_is_looked_up_once() {
    let repository = id();
    let inner = CountingResolver::new(&[(repository, "goalables/app")]);
    let cached = CachedNameResolver::new(inner.clone(), 8);

    for _ in 0..3 {
        let name = cached.resolve(repository).await.unwrap().unwrap();
        assert_eq!(&*name, "goalables/app");
    }
    assert_eq!(inner.lookups(), 1);
    assert_eq!(cached.len(), 1);
}

#[tokio::test]
async fn a_missing_repository_is_not_remembered() {
    let repository = id();
    let inner = CountingResolver::new(&[]);
    let cached = CachedNameResolver::new(inner.clone(), 8);

    assert!(cached.resolve(repository).await.unwrap().is_none());
    assert!(cached.is_empty());

    inner.add(repository, "goalables/app");
    let name = cached.resolve(repository).await.unwrap().unwrap();
    assert_eq!(&*name, "goalables/app");
    assert_eq!(inner.lookups(), 2);
}

#[tokio::test]
async fn errors_pass_through_and_are_not_remembered() {
    let cached = CachedNameResolver::new(CountingResolver::failing(), 8);

    let err = cached.resolve(id()).await.unwrap_err();
    assert_eq!(err.code(), Code::Unavailable);
    assert!(cached.is_empty());
}

#[tokio::test]
async fn the_cache_never_grows_past_its_capacity() {
    let repositories: Vec<RepositoryId> = (0..10).map(|_| id()).collect();
    let entries: Vec<(RepositoryId, &str)> = repositories
        .iter()
        .map(|repository| (*repository, "goalables/app"))
        .collect();
    let inner = CountingResolver::new(&entries);
    let cached = CachedNameResolver::new(inner.clone(), 4);

    for repository in &repositories {
        cached.resolve(*repository).await.unwrap().unwrap();
        assert!(cached.len() <= 4, "cache holds {} names", cached.len());
    }
    // Names evicted with the cache are simply resolved again.
    for repository in &repositories {
        let name = cached.resolve(*repository).await.unwrap().unwrap();
        assert_eq!(&*name, "goalables/app");
    }
}

#[tokio::test]
async fn the_store_resolver_reads_the_name_from_repository_metadata() {
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
            let resolver = StoreNameResolver::new(immutable_store, mutable_store);

            let name = resolver.resolve(repository).await.unwrap().unwrap();
            assert_eq!(&*name, "goalables/app");
            assert!(resolver.resolve(id()).await.unwrap().is_none());
        })
        .await;
}
