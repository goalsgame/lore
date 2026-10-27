// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

use super::*;
use crate::checkpoint_owner::source_publication::HandoffGuard;
use crate::checkpoint_owner::source_publication::JoinerPanicGuard;
use crate::checkpoint_owner::source_publication::Progress;

fn fixture() -> (Directory, Arc<CheckpointOwner>, Arc<Progress>) {
    let directory = Directory::new();
    let namespace = Arc::new(
        Namespace::new(Arc::new(Base), root(), Arc::new(Gate), Default::default()).unwrap(),
    );
    let owner = owner(&directory, namespace);
    *owner.publications.active.lock().unwrap() += 1;
    let progress = Progress::new(owner.clone(), Publishing(owner.publications.clone()));
    (directory, owner, progress)
}

#[test]
fn source_progress_handoff_failure_poisons_before_the_publishers_last_drop() {
    let (_directory, owner, progress) = fixture();
    let publisher = progress.clone();
    let guard = HandoffGuard::new(&progress);
    drop(guard);
    assert!(owner.is_poisoned());
    drop(progress);
    assert_eq!(*owner.publications.active.lock().unwrap(), 1);
    drop(publisher);
    assert_eq!(*owner.publications.active.lock().unwrap(), 0);
}

#[test]
fn source_progress_confirmed_join_can_disarm_the_handoff_guard() {
    let (_directory, owner, progress) = fixture();
    let mut guard = HandoffGuard::new(&progress);
    guard.disarm();
    drop(guard);
    assert!(!owner.is_poisoned());
    drop(progress);
    assert_eq!(*owner.publications.active.lock().unwrap(), 0);
}

#[test]
fn source_progress_joiner_panic_poisons_while_publisher_resources_are_retained() {
    let (_directory, owner, progress) = fixture();
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _guard = JoinerPanicGuard::new(&owner);
        panic!("injected joiner panic");
    }));
    assert!(panic.is_err());
    assert!(owner.is_poisoned());
    assert_eq!(*owner.publications.active.lock().unwrap(), 1);
    drop(progress);
    assert_eq!(*owner.publications.active.lock().unwrap(), 0);
}
