// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
// Replacement fixtures preserve valid files to isolate held-owner identity.
#![allow(clippy::disallowed_methods)]
use std::os::unix::fs::MetadataExt;
use std::os::unix::fs::PermissionsExt;

use super::*;

#[test]
fn storage_owner_verification_is_read_only_and_does_not_resurvey_sessions() {
    let directory = directory();
    let mount = mount(&directory.0);
    let root = directory.0.join("backing");
    let storage = WorkingStorage::create_new(
        &root,
        &mount,
        repository(),
        InstanceId::generate(),
        Limits::default(),
    )
    .unwrap();
    let identity = fs::read(root.join("identity")).unwrap();
    storage.verify_owner().unwrap();
    let session = storage.begin_session().unwrap();
    storage.verify_owner().unwrap();
    assert_eq!(storage.retained_sessions().unwrap(), 1);
    assert_eq!(fs::read(root.join("identity")).unwrap(), identity);
    assert!(!root.join("recovery/owner.lock").exists());
    drop(session);
}
#[test]
fn storage_owner_verification_rejects_replaced_root_with_same_identity_and_lock() {
    let directory = directory();
    let mount = mount(&directory.0);
    let root = directory.0.join("backing");
    let storage = WorkingStorage::create_new(
        &root,
        &mount,
        repository(),
        InstanceId::generate(),
        Limits::default(),
    )
    .unwrap();
    let saved = directory.0.join("saved");
    fs::rename(&root, &saved).unwrap();
    fs::create_dir(&root).unwrap();
    fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
    for name in ["owner.lock", "identity", "recovery", "sessions"] {
        fs::rename(saved.join(name), root.join(name)).unwrap();
    }
    assert_eq!(
        storage._lock.metadata().unwrap().ino(),
        fs::metadata(root.join("owner.lock")).unwrap().ino()
    );
    assert!(storage.verify_owner().is_err());
    assert!(saved.is_dir());
}
#[test]
fn storage_owner_verification_rejects_lock_replacement_without_releasing_original() {
    let directory = directory();
    let mount = mount(&directory.0);
    let root = directory.0.join("backing");
    let instance = InstanceId::generate();
    let storage =
        WorkingStorage::create_new(&root, &mount, repository(), instance, Limits::default())
            .unwrap();
    let saved = directory.0.join("old.lock");
    fs::rename(root.join("owner.lock"), &saved).unwrap();
    let replacement = private_options()
        .read(true)
        .write(true)
        .create_new(true)
        .open(root.join("owner.lock"))
        .unwrap();
    assert!(storage.verify_owner().is_err());
    let original = private_options()
        .read(true)
        .write(true)
        .open(saved)
        .unwrap();
    assert!(matches!(
        original.try_lock(),
        Err(std::fs::TryLockError::WouldBlock)
    ));
    drop(storage);
    original.try_lock().unwrap();
    drop(replacement);
}
#[test]
fn storage_owner_verification_refuses_corruption_unknown_entries_and_unsafe_modes() {
    let directory = directory();
    let mount = mount(&directory.0);
    let root = directory.0.join("backing");
    let storage = WorkingStorage::create_new(
        &root,
        &mount,
        repository(),
        InstanceId::generate(),
        Limits::default(),
    )
    .unwrap();
    let identity = fs::read(root.join("identity")).unwrap();
    let mut corrupt = identity.clone();
    corrupt[8] ^= 1;
    fs::write(root.join("identity"), &corrupt).unwrap();
    assert!(storage.verify_owner().is_err());
    assert_eq!(fs::read(root.join("identity")).unwrap(), corrupt);
    fs::write(root.join("identity"), &identity).unwrap();
    storage.verify_owner().unwrap();
    fs::write(root.join("unknown"), b"retained").unwrap();
    assert!(storage.verify_owner().is_err());
    fs::remove_file(root.join("unknown")).unwrap();
    fs::set_permissions(root.join("sessions"), fs::Permissions::from_mode(0o755)).unwrap();
    assert!(storage.verify_owner().is_err());
    fs::set_permissions(root.join("sessions"), fs::Permissions::from_mode(0o700)).unwrap();
    storage.verify_owner().unwrap();
}
