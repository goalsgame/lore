// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
#![allow(clippy::disallowed_methods)]
use std::os::unix::fs::DirBuilderExt;

use super::*;

#[test]
fn backing_binding_public_consumer_roundtrip() {
    let directory = lore_base::test_util::TempDir::new("working-binding-consumer-");
    let metadata = directory.path().join("metadata");
    let root = directory.path().join("backing");
    std::fs::DirBuilder::new()
        .mode(0o700)
        .create(&metadata)
        .unwrap();
    std::fs::DirBuilder::new()
        .mode(0o700)
        .create(&root)
        .unwrap();
    let repository = RepositoryId::from([211; 16]);
    let instance = InstanceId::generate();
    let root = root.canonicalize().unwrap();
    Binding::write_new(
        &metadata.canonicalize().unwrap(),
        repository,
        instance,
        &root,
    )
    .unwrap();
    assert_eq!(
        Binding::read(&metadata.canonicalize().unwrap(), repository, instance)
            .unwrap()
            .root(),
        root
    );
}
