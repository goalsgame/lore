// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

use std::sync::Barrier;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use super::*;

fn slot() -> Arc<Slot> {
    Arc::new(Slot::default())
}
fn make_owner() -> OwnerClaim {
    Reservation::reserve(slot(), Some(slot()), || true)
        .unwrap()
        .commit()
        .unwrap()
}
fn make_issuer(owner: &OwnerClaim, target: Arc<Slot>) -> PreparedIssuer {
    PreparedIssuer {
        namespace: owner.namespace.clone(),
        source: owner.backend.as_ref().unwrap().clone(),
        target,
        source_brand: Arc::new(()),
        target_brand: Arc::new(()),
    }
}
#[test]
fn target_reservation_is_exclusive_and_rollback_leaves_source_unchanged() {
    let owner = make_owner();
    let target = slot();
    let issuer = make_issuer(&owner, target.clone());
    let reservation = owner.reserve_successor(&issuer).unwrap();
    owner
        .check(&owner.namespace, owner.backend.as_ref())
        .unwrap();
    reservation.check(&owner, &issuer).unwrap();
    assert_eq!(
        owner.check(&owner.namespace, Some(&issuer.target)),
        Err(Error::Stale)
    );
    assert!(matches!(owner.reserve_successor(&issuer), Err(Error::Busy)));
    assert!(matches!(
        Reservation::reserve(slot(), Some(target.clone()), || true),
        Err(Error::Busy)
    ));
    drop(reservation);
    let independent = Reservation::reserve(slot(), Some(target), || true)
        .unwrap()
        .commit()
        .unwrap();
    assert!(matches!(owner.reserve_successor(&issuer), Err(Error::Busy)));
    owner
        .check(&owner.namespace, owner.backend.as_ref())
        .unwrap();
    independent
        .check(&independent.namespace, independent.backend.as_ref())
        .unwrap();
}
#[test]
fn competing_reservations_have_one_nonce_and_independent_targets_do_not_choose_association() {
    let owner = Arc::new(make_owner());
    let target = slot();
    let issuer = Arc::new(make_issuer(&owner, target));
    let start = Arc::new(Barrier::new(8));
    let finish = Arc::new(Barrier::new(8));
    let winners = Arc::new(AtomicUsize::new(0));
    let tasks = (0..8)
        .map(|_| {
            let owner = owner.clone();
            let issuer = issuer.clone();
            let start = start.clone();
            let finish = finish.clone();
            let winners = winners.clone();
            std::thread::spawn(move || {
                start.wait();
                let reservation = match owner.reserve_successor(&issuer) {
                    Ok(reservation) => {
                        winners.fetch_add(1, Ordering::AcqRel);
                        Some(reservation)
                    }
                    Err(Error::Busy) => None,
                    _ => panic!("unexpected reservation result"),
                };
                finish.wait();
                drop(reservation);
            })
        })
        .collect::<Vec<_>>();
    for task in tasks {
        task.join().unwrap();
    }
    assert_eq!(winners.load(Ordering::Acquire), 1);
    let first = owner.reserve_successor(&issuer).unwrap();
    let other_issuer = make_issuer(&owner, slot());
    let second = owner.reserve_successor(&other_issuer).unwrap();
    first.check(&owner, &issuer).unwrap();
    second.check(&owner, &other_issuer).unwrap();
    owner
        .check(&owner.namespace, owner.backend.as_ref())
        .unwrap();
}
#[test]
fn unrelated_issuer_and_existing_target_claim_are_refused() {
    let owner = make_owner();
    let issuer = make_issuer(&owner, slot());
    let unrelated = make_owner();
    assert!(matches!(
        unrelated.reserve_successor(&issuer),
        Err(Error::Stale)
    ));
    let reservation = owner.reserve_successor(&issuer).unwrap();
    let wrong_target = make_issuer(&owner, slot());
    assert_eq!(reservation.check(&owner, &wrong_target), Err(Error::Stale));
    assert_eq!(reservation.check(&unrelated, &issuer), Err(Error::Stale));
    let mut wrong_brand = make_issuer(&owner, issuer.target.clone());
    wrong_brand.source_brand = issuer.source_brand.clone();
    assert_eq!(reservation.check(&owner, &wrong_brand), Err(Error::Stale));
    let mut source_target = make_issuer(&owner, owner.backend.as_ref().unwrap().clone());
    assert!(matches!(
        owner.reserve_successor(&source_target),
        Err(Error::InvalidInput)
    ));
    source_target.target = owner.namespace.clone();
    assert!(matches!(
        owner.reserve_successor(&source_target),
        Err(Error::InvalidInput)
    ));
}
#[test]
fn generic_owner_cannot_reserve_revision_successor() {
    let owner = Reservation::reserve(slot(), None, || true)
        .unwrap()
        .commit()
        .unwrap();
    let issuer = PreparedIssuer {
        namespace: owner.namespace.clone(),
        source: slot(),
        target: slot(),
        source_brand: Arc::new(()),
        target_brand: Arc::new(()),
    };
    assert!(matches!(
        owner.reserve_successor(&issuer),
        Err(Error::Unsupported)
    ));
}
#[tokio::test]
async fn cancellation_releases_only_its_target_nonce() {
    let owner = make_owner();
    let target = slot();
    let issuer = make_issuer(&owner, target.clone());
    let mut prepare = Box::pin(async {
        let _reservation = owner.reserve_successor(&issuer).unwrap();
        std::future::pending::<()>().await;
    });
    std::future::poll_fn(|context| {
        assert!(prepare.as_mut().poll(context).is_pending());
        std::task::Poll::Ready(())
    })
    .await;
    assert!(matches!(
        Reservation::reserve(slot(), Some(target.clone()), || true),
        Err(Error::Busy)
    ));
    drop(prepare);
    let replacement = owner.reserve_successor(&issuer).unwrap();
    owner
        .check(&owner.namespace, owner.backend.as_ref())
        .unwrap();
    replacement.check(&owner, &issuer).unwrap();
}

#[test]
fn rollback_never_clears_a_different_reservation_nonce() {
    let owner = make_owner();
    let issuer = make_issuer(&owner, slot());
    let reservation = owner.reserve_successor(&issuer).unwrap();
    assert!(!Arc::ptr_eq(&reservation.nonce, &reservation.owner));
    let newer = Arc::new(Identity);
    *issuer.target.state.lock().unwrap() = State::Reserved(newer.clone());
    drop(reservation);
    assert!(reserved(&issuer.target.state.lock().unwrap(), &newer));
    owner
        .check(&owner.namespace, owner.backend.as_ref())
        .unwrap();
}
