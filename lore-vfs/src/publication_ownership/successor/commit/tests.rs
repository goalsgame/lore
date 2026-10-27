// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

use super::*;

fn slot() -> Arc<Slot> {
    Arc::new(Slot::default())
}
fn issuer(owner: &OwnerClaim, source: Arc<Slot>, target: Arc<Slot>) -> PreparedIssuer {
    PreparedIssuer {
        namespace: owner.namespace.clone(),
        source,
        target,
        source_brand: Arc::new(()),
        target_brand: Arc::new(()),
    }
}
#[test]
fn two_exact_commits_advance_active_association_and_leave_historical_slots_sticky() {
    let initial = slot();
    let owner = Reservation::reserve(slot(), Some(initial.clone()), || true)
        .unwrap()
        .commit()
        .unwrap();
    let lineage = owner.lineage();
    let first = issuer(&owner, initial.clone(), slot());
    let reservation = owner.reserve_successor(&first).unwrap();
    assert!(owner.check(&owner.namespace, Some(&first.target)).is_err());
    reservation.prepare_commit(&owner, &first).unwrap().commit();
    drop(reservation);
    owner.check(&owner.namespace, Some(&first.target)).unwrap();
    owner.check_lineage(Some(&lineage)).unwrap();
    assert_eq!(
        owner.check(&owner.namespace, Some(&initial)),
        Err(Error::Stale)
    );
    assert!(matches!(
        Reservation::reserve(slot(), Some(initial.clone()), || true),
        Err(Error::Busy)
    ));
    let second = issuer(&owner, first.target.clone(), slot());
    let next = owner.reserve_successor(&second).unwrap();
    next.check(&owner, &second).unwrap();
    next.prepare_commit(&owner, &second).unwrap().commit();
    drop(next);
    owner.check(&owner.namespace, Some(&second.target)).unwrap();
    owner.check_lineage(Some(&lineage)).unwrap();
    assert_eq!(
        owner.check(&owner.namespace, Some(&first.target)),
        Err(Error::Stale)
    );
    assert!(matches!(
        Reservation::reserve(slot(), Some(first.target.clone()), || true),
        Err(Error::Busy)
    ));
    assert!(matches!(
        Reservation::reserve(slot(), Some(second.target.clone()), || true),
        Err(Error::Busy)
    ));
}
#[test]
fn failed_guard_or_uncommitted_drop_never_changes_active_association() {
    let initial = slot();
    let owner = Reservation::reserve(slot(), Some(initial.clone()), || true)
        .unwrap()
        .commit()
        .unwrap();
    let first = issuer(&owner, initial.clone(), slot());
    let reservation = owner.reserve_successor(&first).unwrap();
    let wrong = issuer(&owner, initial.clone(), slot());
    assert!(reservation.prepare_commit(&owner, &wrong).is_err());
    owner.check(&owner.namespace, Some(&initial)).unwrap();
    drop(reservation);
    assert!(Reservation::reserve(slot(), Some(first.target.clone()), || true).is_ok());
    owner.check(&owner.namespace, Some(&initial)).unwrap();
}
