// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
// Explicit setup modes must not turn failed reuse into a new backing root.
#![allow(clippy::disallowed_methods)]
use super::tests::directory;
use super::tests::mount;
use super::tests::repository;
use super::*;

#[test]
fn existing_only_storage_does_not_create_an_absent_root_or_parent() {
    let directory = directory();
    let mount = mount(&directory.0);
    for root in [
        directory.0.join("missing"),
        directory.0.join("absent/child"),
    ] {
        assert!(
            WorkingStorage::open_existing(
                &root,
                &mount,
                repository(),
                InstanceId::generate(),
                Limits::default()
            )
            .is_err()
        );
        assert!(!root.exists());
    }
    assert!(!directory.0.join("absent").exists());
}
#[test]
fn create_new_storage_never_adopts_an_existing_empty_directory() {
    let directory = directory();
    let mount = mount(&directory.0);
    let root = directory.0.join("existing");
    std::fs::create_dir(&root).unwrap();
    assert!(
        WorkingStorage::create_new(
            &root,
            &mount,
            repository(),
            InstanceId::generate(),
            Limits::default()
        )
        .is_err()
    );
    assert_eq!(std::fs::read_dir(&root).unwrap().count(), 0);
}
#[test]
fn existing_only_storage_does_not_initialize_an_existing_empty_directory() {
    use std::os::unix::fs::PermissionsExt;
    let directory = directory();
    let mount = mount(&directory.0);
    let root = directory.0.join("empty");
    std::fs::create_dir(&root).unwrap();
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700)).unwrap();
    assert!(
        WorkingStorage::open_existing(
            &root,
            &mount,
            repository(),
            InstanceId::generate(),
            Limits::default()
        )
        .is_err()
    );
    assert_eq!(std::fs::read_dir(&root).unwrap().count(), 0);
}

#[test]
fn explicit_storage_modes_retain_exact_identity_and_exclusive_owner() {
    let directory = directory();
    let mount = mount(&directory.0);
    let root = directory.0.join("backing");
    let instance = InstanceId::generate();
    let storage =
        WorkingStorage::create_new(&root, &mount, repository(), instance, Limits::default())
            .unwrap();
    let identity = std::fs::read(root.join("identity")).unwrap();
    assert!(matches!(
        WorkingStorage::open_existing(&root, &mount, repository(), instance, Limits::default()),
        Err(Error::Busy)
    ));
    drop(storage);
    assert!(
        WorkingStorage::open_existing(
            &root,
            &mount,
            repository(),
            InstanceId::generate(),
            Limits::default()
        )
        .is_err()
    );
    assert_eq!(std::fs::read(root.join("identity")).unwrap(), identity);
    let storage =
        WorkingStorage::open_existing(&root, &mount, repository(), instance, Limits::default())
            .unwrap();
    assert_eq!(storage.instance(), instance);
    assert_eq!(storage.repository(), repository());
}
