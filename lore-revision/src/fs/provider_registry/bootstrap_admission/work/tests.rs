// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

#![cfg(unix)]
#![allow(clippy::disallowed_methods)]

use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;

use lore_base::test_util::TempDir;
use lore_storage::Hash;
use lore_storage::MutableStore;
use lore_storage::Partition;
use lore_storage::local::immutable_store;
use lore_storage::local::mutable_store::LocalMutableStore;
use lore_storage::local::mutable_store::MutableStoreSettings;
use lore_storage::test_util::PausePoint;

use super::super::tests::fixture;
use super::*;

async fn local_store() -> (TempDir, Arc<LocalMutableStore>) {
    let directory = TempDir::new("lore-bound-work-list-");
    let immutable = immutable_store::create(
        None::<&str>,
        immutable_store::ImmutableStoreCreateOptions::none(),
        false,
        immutable_store::ImmutableStoreSettings::default(),
    )
    .await
    .unwrap();
    let store = Arc::new(
        LocalMutableStore::new(
            Some(directory.path()),
            MutableStoreSettings {
                initial_fan_out_level: 1,
                reuse_state_across_holds: true,
                flush_delay_seconds: 0,
                ..Default::default()
            },
            immutable,
        )
        .await
        .unwrap(),
    );
    store
        .clone()
        .store(
            Partition::default(),
            Hash::from_u64(71),
            Hash::from_u64(72),
            lore_storage::KeyType::BranchMetadata,
        )
        .await
        .unwrap();
    store.clone().flush(true).await.unwrap();
    (directory, store)
}
async fn wait_until(mut predicate: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while !predicate() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}
fn assert_work_busy(gate: &Arc<bootstrap_gate::Gate>) {
    let probe = gate.admit().unwrap();
    assert!(matches!(
        gate.try_exclusive(&probe),
        Err(bootstrap_gate::Error::Busy)
    ));
}
fn assert_work_finished(gate: &Arc<bootstrap_gate::Gate>) {
    let probe = gate.admit().unwrap();
    assert!(gate.try_exclusive(&probe).is_ok());
}
struct Parent {
    dropped: Arc<AtomicBool>,
}
impl Drop for Parent {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::Release);
    }
}
fn work_scope(load: &BoundLoad) -> (IoLease, Arc<AtomicBool>) {
    let dropped = Arc::new(AtomicBool::new(false));
    let parent = IoLease::new(Arc::new(Parent {
        dropped: dropped.clone(),
    }));
    let work = parent.sync_scope(|| load.begin_work().unwrap().into_io_lease());
    drop(parent);
    (work, dropped)
}

#[tokio::test]
async fn actual_listing_producer_retains_work_after_load_and_receiver_drop() {
    let fixture = fixture();
    let (_, load) = resolve(&fixture.registry, &fixture.location)
        .unwrap()
        .into_parts();
    let (_directory, store) = local_store().await;
    let lock = store.lock.clone().unwrap();
    let pause = lore_storage::test_util::pause_at(PausePoint::Listing);
    let (scope, parent_dropped) = work_scope(&load);
    let stream = scope
        .scope(
            store
                .clone()
                .list(Partition::default(), lore_storage::KeyType::BranchMetadata),
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(10), pause.reached.notified())
        .await
        .unwrap();
    let receiver = stream.channel();
    drop(load);
    drop(store);
    drop(scope);
    drop(receiver);
    assert!(lock.is_held());
    assert!(!parent_dropped.load(Ordering::Acquire));
    assert_work_busy(&fixture.owner.claim.gate);
    pause.resume.notify_one();
    wait_until(|| parent_dropped.load(Ordering::Acquire)).await;
    wait_until(|| !lock.is_held()).await;
    assert_work_finished(&fixture.owner.claim.gate);
}

#[tokio::test]
async fn buffered_receiver_outlives_completed_producers_without_retaining_work() {
    let fixture = fixture();
    let (_, load) = resolve(&fixture.registry, &fixture.location)
        .unwrap()
        .into_parts();
    let (_directory, store) = local_store().await;
    let pause = lore_storage::test_util::pause_at(PausePoint::Listing);
    let (scope, parent_dropped) = work_scope(&load);
    let stream = scope
        .scope(
            store
                .clone()
                .list(Partition::default(), lore_storage::KeyType::BranchMetadata),
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(10), pause.reached.notified())
        .await
        .unwrap();
    let mut receiver = stream.channel();
    drop(load);
    drop(scope);
    assert_work_busy(&fixture.owner.claim.gate);
    pause.resume.notify_one();
    wait_until(|| parent_dropped.load(Ordering::Acquire)).await;
    assert_work_finished(&fixture.owner.claim.gate);
    let (_typed_key, value) = receiver.recv().await.unwrap();
    assert_eq!(value, Hash::from_u64(72));
    assert_eq!(receiver.recv().await, None);
    assert_work_finished(&fixture.owner.claim.gate);
}

#[tokio::test]
async fn cancelled_listing_future_retains_actual_blocked_producers() {
    let fixture = fixture();
    let (_, load) = resolve(&fixture.registry, &fixture.location)
        .unwrap()
        .into_parts();
    let (_directory, store) = local_store().await;
    let lock = store.lock.clone().unwrap();
    let blocker = store.group[0].bucket(0).clone().write_owned().await;
    let (scope, parent_dropped) = work_scope(&load);
    let mut listing = lore_base::lore_spawn!(
        scope.scope(
            store
                .clone()
                .list(Partition::default(), lore_storage::KeyType::BranchMetadata)
        )
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(30), &mut listing)
            .await
            .is_err()
    );
    listing.abort();
    assert!(listing.await.is_err_and(|error| error.is_cancelled()));
    drop(load);
    drop(scope);
    drop(store);
    assert!(!parent_dropped.load(Ordering::Acquire));
    assert!(lock.is_held());
    assert_work_busy(&fixture.owner.claim.gate);
    drop(blocker);
    wait_until(|| parent_dropped.load(Ordering::Acquire)).await;
    wait_until(|| !lock.is_held()).await;
    assert_work_finished(&fixture.owner.claim.gate);
}

#[test]
fn admission_rechecks_retirement_location_capacity_and_exact_claim() {
    let fixture = fixture();
    let (_, mut load) = resolve(&fixture.registry, &fixture.location)
        .unwrap()
        .into_parts();
    let original = load.location.clone();
    load.location.token += 1;
    assert!(matches!(load.begin_work(), Err(Error::Stale)));
    load.location = original;
    let mut work = Vec::new();
    for _ in 0..MAX_BOOTSTRAP_WORK_PER_ROOT {
        work.push(load.begin_work().unwrap());
    }
    assert!(matches!(load.begin_work(), Err(Error::Capacity)));
    work.pop();
    assert!(load.begin_work().is_ok());
    drop(work);
    fixture.owner.retire().unwrap();
    assert!(matches!(load.begin_work(), Err(Error::Retired)));
}

#[test]
fn inherited_parent_drops_after_bound_work_and_last_claim() {
    struct OrderedParent {
        registry: Arc<ProviderRegistry>,
        dropped: Arc<AtomicBool>,
    }
    impl Drop for OrderedParent {
        fn drop(&mut self) {
            assert!(self.registry.state.lock().entries.is_empty());
            self.dropped.store(true, Ordering::Release);
        }
    }
    let fixture = fixture();
    let (_, load) = resolve(&fixture.registry, &fixture.location)
        .unwrap()
        .into_parts();
    let dropped = Arc::new(AtomicBool::new(false));
    let parent = IoLease::new(Arc::new(OrderedParent {
        registry: fixture.registry.clone(),
        dropped: dropped.clone(),
    }));
    let scope = parent.sync_scope(|| load.begin_work().unwrap().into_io_lease());
    let retained = scope.clone();
    drop(parent);
    drop(scope);
    drop(fixture.owner);
    drop(load);
    assert!(!dropped.load(Ordering::Acquire));
    assert_eq!(fixture.registry.state.lock().entries.len(), 1);
    drop(retained);
    assert!(dropped.load(Ordering::Acquire));
}

#[test]
fn wrong_claim_and_poison_do_not_admit_work() {
    let first = fixture();
    let second = fixture();
    let (_, mut load) = resolve(&first.registry, &first.location)
        .unwrap()
        .into_parts();
    let (_, other) = resolve(&second.registry, &second.location)
        .unwrap()
        .into_parts();
    load.claim = other.claim.clone();
    assert!(matches!(load.begin_work(), Err(Error::Stale)));
    second.owner.claim.gate.test_poison();
    assert!(matches!(other.begin_work(), Err(Error::Poisoned)));
}

mod loader;
