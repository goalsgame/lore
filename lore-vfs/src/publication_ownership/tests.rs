// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

use std::sync::Barrier;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use super::*;
use crate::overlay::OperationFreeze;

fn slot() -> Arc<Slot> {
    Arc::new(Slot::default())
}
#[test]
fn reservation_rollback_releases_both_slots_and_commit_remains_sticky() {
    let namespace = slot();
    let backend = slot();
    let reservation =
        Reservation::reserve(namespace.clone(), Some(backend.clone()), || true).unwrap();
    assert!(matches!(
        Reservation::reserve(namespace.clone(), None, || true),
        Err(Error::Busy)
    ));
    let other_namespace = slot();
    assert!(matches!(
        Reservation::reserve(other_namespace.clone(), Some(backend.clone()), || true),
        Err(Error::Busy)
    ));
    drop(reservation);
    let claim = Reservation::reserve(namespace.clone(), Some(backend.clone()), || true)
        .unwrap()
        .commit()
        .unwrap();
    let pin = claim.lineage();
    claim.check(&namespace, Some(&backend)).unwrap();
    claim.check_lineage(Some(&pin)).unwrap();
    drop(claim);
    assert!(matches!(
        Reservation::reserve(namespace, None, || true),
        Err(Error::Busy)
    ));
    assert!(matches!(
        Reservation::reserve(other_namespace, Some(backend), || true),
        Err(Error::Busy)
    ));
}
#[test]
fn simultaneous_reservations_commit_exactly_one_owner() {
    let namespace = slot();
    let backend = slot();
    let start = Arc::new(Barrier::new(16));
    let successes = Arc::new(AtomicUsize::new(0));
    let tasks = (0..16)
        .map(|_| {
            let namespace = namespace.clone();
            let backend = backend.clone();
            let start = start.clone();
            let successes = successes.clone();
            std::thread::spawn(move || {
                start.wait();
                match Reservation::reserve(namespace, Some(backend), || true) {
                    Ok(reservation) => {
                        reservation.commit().unwrap();
                        successes.fetch_add(1, Ordering::AcqRel);
                    }
                    Err(Error::Busy) => {}
                    _ => panic!("unexpected reservation result"),
                }
            })
        })
        .collect::<Vec<_>>();
    for task in tasks {
        task.join().unwrap();
    }
    assert_eq!(successes.load(Ordering::Acquire), 1);
}
#[test]
fn freeze_and_reservation_share_admission_and_unclaimed_freeze_cannot_straddle_commit() {
    let namespace = slot();
    let freeze = OperationFreeze::default();
    let (old, lineage) = namespace.acquire_freeze(|| freeze.acquire()).unwrap();
    assert!(lineage.is_none());
    assert!(matches!(
        Reservation::reserve(namespace.clone(), None, || !freeze.frozen()),
        Err(Error::Busy)
    ));
    freeze.release(old);
    let reservation = Reservation::reserve(namespace.clone(), None, || !freeze.frozen()).unwrap();
    assert!(matches!(
        namespace.acquire_freeze(|| freeze.acquire()),
        Err(Error::Busy)
    ));
    assert!(!freeze.frozen());
    drop(reservation);
    let (cancelled, lineage) = namespace.acquire_freeze(|| freeze.acquire()).unwrap();
    assert!(lineage.is_none());
    freeze.release(cancelled);
    let claim = Reservation::reserve(namespace.clone(), None, || !freeze.frozen())
        .unwrap()
        .commit()
        .unwrap();
    let (owned, lineage) = namespace.acquire_freeze(|| freeze.acquire()).unwrap();
    claim.check_lineage(lineage.as_ref()).unwrap();
    assert_ne!(old, owned);
    freeze.release(owned);
}
#[tokio::test]
async fn cancelling_constructor_reservation_releases_exact_uncommitted_identity() {
    let namespace = slot();
    let backend = slot();
    let mut startup = Box::pin(async {
        let _reservation =
            Reservation::reserve(namespace.clone(), Some(backend.clone()), || true).unwrap();
        std::future::pending::<()>().await;
    });
    std::future::poll_fn(|context| {
        assert!(startup.as_mut().poll(context).is_pending());
        std::task::Poll::Ready(())
    })
    .await;
    assert!(matches!(
        Reservation::reserve(namespace.clone(), None, || true),
        Err(Error::Busy)
    ));
    drop(startup);
    Reservation::reserve(namespace, Some(backend), || true)
        .unwrap()
        .commit()
        .unwrap();
}
#[test]
fn unrelated_slots_and_unclaimed_checkpoint_pins_never_match() {
    let namespace = slot();
    let backend = slot();
    let claim = Reservation::reserve(namespace.clone(), Some(backend.clone()), || true)
        .unwrap()
        .commit()
        .unwrap();
    assert_eq!(claim.check(&slot(), Some(&backend)), Err(Error::Stale));
    assert_eq!(claim.check(&namespace, Some(&slot())), Err(Error::Stale));
    assert_eq!(claim.check(&namespace, None), Err(Error::Stale));
    assert_eq!(claim.check_lineage(None), Err(Error::Stale));
    let independent_namespace = slot();
    let independent_backend = slot();
    let independent = Reservation::reserve(
        independent_namespace.clone(),
        Some(independent_backend.clone()),
        || true,
    )
    .unwrap()
    .commit()
    .unwrap();
    independent
        .check(&independent_namespace, Some(&independent_backend))
        .unwrap();
    assert_eq!(
        claim.check_lineage(Some(&independent.lineage())),
        Err(Error::Stale)
    );
}
