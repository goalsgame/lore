// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
// Read-only reuse must not repair incomplete or replaced private roots.
#![allow(clippy::disallowed_methods)]
use std::os::unix::fs::MetadataExt;
use std::os::unix::fs::PermissionsExt;

use super::*;
fn private_dir(path: &Path) {
    fs::create_dir(path).unwrap();
    fs::set_permissions(path, fs::Permissions::from_mode(0o700)).unwrap();
}
#[test]
fn existing_recovery_refuses_missing_directory_or_lock_without_creating_or_syncing() {
    let temp = Temp::new();
    let root = temp.0.join("recovery");
    assert!(Recovery::open_existing(&root, Limits::default()).is_err());
    assert!(!root.exists());
    private_dir(&root);
    let before = fs::metadata(&root).unwrap().modified().unwrap();
    assert!(Recovery::open_existing(&root, Limits::default()).is_err());
    assert_eq!(fs::read_dir(&root).unwrap().count(), 0);
    assert_eq!(fs::metadata(&root).unwrap().modified().unwrap(), before);
}
#[test]
fn existing_recovery_retains_exact_owner_and_checkpoint() {
    let temp = Temp::new();
    let root = temp.0.join("recovery");
    let mut owner = Recovery::open(&root, Limits::default()).unwrap();
    owner.publish(&image(1)).unwrap();
    let bytes = fs::read(root.join("checkpoint")).unwrap();
    let inode = owner._lock.metadata().unwrap().ino();
    assert!(matches!(
        Recovery::open_existing(&root, Limits::default()),
        Err(Error::Busy)
    ));
    drop(owner);
    let owner = Recovery::open_existing(&root, Limits::default()).unwrap();
    owner.verify_owner().unwrap();
    assert_eq!(owner._lock.metadata().unwrap().ino(), inode);
    assert!(owner.load().unwrap().is_some());
    assert_eq!(fs::read(root.join("checkpoint")).unwrap(), bytes);
}
#[test]
fn existing_recovery_transition_presence_precedes_lock_creation_and_busy_owner() {
    for with_lock in [false, true] {
        for kind in 0..3 {
            let temp = Temp::new();
            let root = temp.0.join("recovery");
            let owner = if with_lock {
                Some(Recovery::open(&root, Limits::default()).unwrap())
            } else {
                private_dir(&root);
                None
            };
            let marker = root.join("transition");
            match kind {
                0 => fs::write(&marker, b"unknown").unwrap(),
                1 => private_dir(&marker),
                _ => std::os::unix::fs::symlink("absent", &marker).unwrap(),
            }
            assert!(matches!(
                Recovery::open_existing(&root, Limits::default()),
                Err(Error::TransitionPending)
            ));
            if let Some(owner) = owner {
                assert!(matches!(
                    owner.verify_owner(),
                    Err(Error::TransitionPending)
                ));
            } else {
                assert!(!root.join("owner.lock").exists());
            }
            assert!(fs::symlink_metadata(marker).is_ok());
        }
    }
}
#[test]
fn recovery_owner_verification_rejects_replaced_directory_with_same_lock() {
    let temp = Temp::new();
    let root = temp.0.join("recovery");
    let owner = Recovery::open(&root, Limits::default()).unwrap();
    let saved = temp.0.join("old");
    fs::rename(&root, &saved).unwrap();
    private_dir(&root);
    fs::rename(saved.join("owner.lock"), root.join("owner.lock")).unwrap();
    assert_eq!(
        owner._lock.metadata().unwrap().ino(),
        fs::metadata(root.join("owner.lock")).unwrap().ino()
    );
    assert!(owner.verify_owner().is_err());
    assert!(matches!(
        Recovery::open_existing(&root, Limits::default()),
        Err(Error::Busy)
    ));
    drop(owner);
    Recovery::open_existing(&root, Limits::default()).unwrap();
}
#[test]
fn recovery_owner_verification_rejects_replaced_lock_and_unsafe_files() {
    let temp = Temp::new();
    let root = temp.0.join("recovery");
    let owner = Recovery::open(&root, Limits::default()).unwrap();
    let old = temp.0.join("old.lock");
    fs::rename(root.join("owner.lock"), &old).unwrap();
    let replacement = private_options()
        .read(true)
        .write(true)
        .create_new(true)
        .open(root.join("owner.lock"))
        .unwrap();
    assert!(owner.verify_owner().is_err());
    fs::remove_file(root.join("owner.lock")).unwrap();
    std::os::unix::fs::symlink(&old, root.join("owner.lock")).unwrap();
    assert!(Recovery::open_existing(&root, Limits::default()).is_err());
    assert!(owner.verify_owner().is_err());
    fs::remove_file(root.join("owner.lock")).unwrap();
    fs::rename(&old, root.join("owner.lock")).unwrap();
    fs::set_permissions(root.join("owner.lock"), fs::Permissions::from_mode(0o644)).unwrap();
    assert!(Recovery::open_existing(&root, Limits::default()).is_err());
    fs::set_permissions(root.join("owner.lock"), fs::Permissions::from_mode(0o600)).unwrap();
    owner.verify_owner().unwrap();
    drop(replacement);
}
