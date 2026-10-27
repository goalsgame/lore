// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use super::*;

#[test]
fn exact_seal_epoch_admission_is_inert_until_commit_and_stays_closed() {
    let gate = Arc::new(MutationGate::default());
    let seal = gate.seal(0).unwrap();
    assert!(matches!(seal.prepare_install(1), Err(Error::Stale)));
    assert_eq!(gate.epoch().unwrap(), 0);
    {
        let guard = seal.prepare_install(0).unwrap();
        assert!(guard.matches_gate(&gate));
        assert!(!guard.matches_gate(&Arc::new(MutationGate::default())));
        assert!(guard.matches_epoch(0));
    }
    assert_eq!(gate.epoch().unwrap(), 0);
    assert!(matches!(gate.mutation(), Err(Error::Busy)));
    {
        let mut guard = seal.prepare_install(0).unwrap();
        guard.commit();
    }
    assert_eq!(gate.epoch().unwrap(), 1);
    assert!(matches!(seal.prepare_install(0), Err(Error::Stale)));
    assert!(matches!(gate.mutation(), Err(Error::Busy)));
    drop(seal);
    drop(gate.mutation().unwrap());
    assert_eq!(gate.epoch().unwrap(), 2);
}

#[test]
fn wrong_seal_identity_does_not_unlock_or_advance_current_seal() {
    let gate = Arc::new(MutationGate::default());
    let seal = gate.seal(0).unwrap();
    let wrong = Seal {
        gate: gate.clone(),
        id: seal.id + 1,
    };
    assert!(matches!(wrong.prepare_install(0), Err(Error::Stale)));
    drop(wrong);
    assert!(matches!(gate.open(), Err(Error::Busy)));
    assert_eq!(gate.epoch().unwrap(), 0);
    drop(seal);
    assert!(gate.open().is_ok());
}
