// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

use super::*;

fn new_gate() -> Arc<Gate> {
    Gate::new(Limits {
        max_loads: 4,
        max_work: 4,
    })
    .unwrap()
}

#[test]
fn distinct_loads_are_not_exempted_as_the_issuers_family() {
    let gate = new_gate();
    let issuer = gate.admit().unwrap();
    let sibling = gate.admit().unwrap();
    assert!(matches!(gate.try_exclusive(&issuer), Err(Error::Busy)));
    drop(sibling);
    let inherited = issuer.pin();
    let exclusive = gate.try_exclusive(&issuer).unwrap();
    assert!(matches!(gate.admit(), Err(Error::Busy)));
    assert!(matches!(gate.begin_work(&inherited), Err(Error::Busy)));
    drop(exclusive);
    drop(gate.begin_work(&inherited).unwrap());
}

#[test]
fn issuer_work_and_retained_work_after_load_drop_still_block_exclusive() {
    let gate = new_gate();
    let issuer = gate.admit().unwrap();
    let own = gate.begin_work(&issuer).unwrap();
    assert!(matches!(gate.try_exclusive(&issuer), Err(Error::Busy)));
    drop(own);
    let sibling = gate.admit().unwrap();
    let work = gate.begin_work(&sibling).unwrap();
    let retained = work.clone();
    drop(work);
    drop(sibling);
    assert_eq!(gate.state.lock().unwrap().loads.len(), 1);
    assert!(matches!(gate.try_exclusive(&issuer), Err(Error::Busy)));
    drop(retained);
    drop(gate.try_exclusive(&issuer).unwrap());
}

#[test]
fn exclusive_survives_issuer_drop_and_ownership_pins() {
    let gate = new_gate();
    let issuer = gate.admit().unwrap();
    let exclusive = gate.try_exclusive(&issuer).unwrap();
    let pin = exclusive.pin();
    let retained = pin.clone();
    drop(issuer);
    drop(exclusive);
    drop(pin);
    assert!(matches!(gate.admit(), Err(Error::Busy)));
    drop(retained);
    drop(gate.admit().unwrap());
}

#[test]
fn exclusive_cancellation_reopens_only_its_nonce_and_retirement_is_terminal() {
    let gate = new_gate();
    let issuer = gate.admit().unwrap();
    let first = gate.try_exclusive(&issuer).unwrap();
    let stale = first.exclusive.nonce;
    drop(first);
    let current = gate.try_exclusive(&issuer).unwrap();
    drop(Exclusive {
        gate: gate.clone(),
        nonce: stale,
    });
    assert!(matches!(gate.admit(), Err(Error::Busy)));
    gate.retire().unwrap();
    drop(current);
    assert!(matches!(gate.admit(), Err(Error::Retired)));
    assert!(matches!(gate.begin_work(&issuer), Err(Error::Retired)));
    assert!(matches!(gate.try_exclusive(&issuer), Err(Error::Retired)));
}

#[test]
fn wrong_gate_and_capacity_fail_without_changing_admission() {
    let gate = Gate::new(Limits {
        max_loads: 1,
        max_work: 1,
    })
    .unwrap();
    let other = new_gate();
    let issuer = gate.admit().unwrap();
    let foreign = other.admit().unwrap();
    assert!(matches!(gate.begin_work(&foreign), Err(Error::WrongGate)));
    assert!(matches!(
        gate.try_exclusive(&foreign),
        Err(Error::WrongGate)
    ));
    assert!(matches!(gate.admit(), Err(Error::Limit)));
    let work = gate.begin_work(&issuer).unwrap();
    assert!(matches!(gate.begin_work(&issuer), Err(Error::Limit)));
    assert_eq!(gate.state.lock().unwrap().work, 1);
    drop(work);
    drop(gate.try_exclusive(&issuer).unwrap());
    assert!(matches!(
        Gate::new(Limits {
            max_loads: 0,
            max_work: 1
        }),
        Err(Error::InvalidLimit)
    ));
    assert!(matches!(
        Gate::new(Limits {
            max_loads: 1,
            max_work: 0
        }),
        Err(Error::InvalidLimit)
    ));
}

#[test]
fn identifier_overflow_never_partially_admits_or_excludes() {
    let gate = new_gate();
    gate.state.lock().unwrap().next_load = u64::MAX;
    assert!(matches!(gate.admit(), Err(Error::Limit)));
    assert!(gate.state.lock().unwrap().loads.is_empty());
    gate.state.lock().unwrap().next_load = 1;
    let issuer = gate.admit().unwrap();
    gate.state.lock().unwrap().next_exclusive = u64::MAX;
    assert!(matches!(gate.try_exclusive(&issuer), Err(Error::Limit)));
    drop(gate.begin_work(&issuer).unwrap());
    gate.state.lock().unwrap().work = usize::MAX;
    assert!(matches!(gate.begin_work(&issuer), Err(Error::Limit)));
    assert_eq!(gate.state.lock().unwrap().work, usize::MAX);
}

#[test]
fn poisoned_gate_never_recovers_or_reopens_on_guard_drop() {
    let gate = new_gate();
    let issuer = gate.admit().unwrap();
    let exclusive = gate.try_exclusive(&issuer).unwrap();
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _state = gate.state.lock().unwrap();
        panic!("poison bootstrap gate");
    }));
    assert!(panic.is_err());
    drop(exclusive);
    assert!(matches!(gate.admit(), Err(Error::Poisoned)));
    assert!(matches!(gate.begin_work(&issuer), Err(Error::Poisoned)));
    assert!(matches!(gate.try_exclusive(&issuer), Err(Error::Poisoned)));
    drop(issuer);
}

#[test]
fn concurrent_load_admission_and_exclusion_cannot_both_win() {
    for _ in 0..32 {
        let gate = new_gate();
        let issuer = gate.admit().unwrap();
        let barrier = Arc::new(std::sync::Barrier::new(2));
        std::thread::scope(|scope| {
            let gate = &gate;
            let ready = barrier.clone();
            let other = scope.spawn(move || {
                ready.wait();
                gate.admit()
            });
            barrier.wait();
            let exclusive = gate.try_exclusive(&issuer);
            let admitted = other.join().unwrap();
            assert!(!(exclusive.is_ok() && admitted.is_ok()));
            assert!(exclusive.is_ok() || admitted.is_ok());
        });
    }
}

#[test]
fn io_lease_retention_is_counted_after_the_issuing_future_is_cancelled() {
    let gate = new_gate();
    let issuer = gate.admit().unwrap();
    let sibling = gate.admit().unwrap();
    let work = Arc::new(gate.begin_work(&sibling).unwrap());
    let lease = lore_io::lease::IoLease::new(work);
    let mut retained = None;
    let mut operation = Box::pin(lease.scope(std::future::poll_fn(|_context| {
        retained = lore_io::lease::IoLease::current();
        std::task::Poll::<()>::Pending
    })));
    let mut context = std::task::Context::from_waker(std::task::Waker::noop());
    assert!(operation.as_mut().poll(&mut context).is_pending());
    drop(operation);
    drop(lease);
    drop(sibling);
    assert!(matches!(gate.try_exclusive(&issuer), Err(Error::Busy)));
    drop(retained);
    drop(gate.try_exclusive(&issuer).unwrap());
}
