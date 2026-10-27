// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

use super::tests::Directory;
use super::tests::WRITE;
use super::tests::fixture;
use super::*;

#[tokio::test]
async fn directory_generation_overflow_refuses_all_name_mutations_before_effects() {
    let directory = Directory::new("directory-generation-overflow");
    let (model, _backend, _gate, stores) = fixture(&directory, Default::default(), 4096).await;
    let before = model.namespace.snapshot(0).await.unwrap();
    let next_handle = model.state.lock().unwrap().next;
    for generation in [i64::MAX as u64, u64::MAX] {
        model
            .directory_generation
            .store(generation, Ordering::Release);
        assert!(matches!(
            model.create(1, b"new", 0o644, WRITE).await,
            Err(Error::Core(core::Error::ResourceLimit))
        ));
        assert!(matches!(
            model.mkdir(1, b"directory").await,
            Err(Error::Core(core::Error::ResourceLimit))
        ));
        assert!(matches!(
            model.remove(1, b"base", false).await,
            Err(Error::Core(core::Error::ResourceLimit))
        ));
        assert!(matches!(
            model.rename(1, b"base", 1, b"renamed").await,
            Err(Error::Core(core::Error::ResourceLimit))
        ));
        assert_eq!(
            model.directory_generation.load(Ordering::Acquire),
            generation
        );
        {
            let state = model.state.lock().unwrap();
            assert_eq!(state.next, next_handle);
            assert!(state.handles.is_empty());
            assert!(state.unlinked.is_empty());
        }
        assert_eq!(model.counters.handles.load(Ordering::Acquire), 0);
        assert_eq!(model.counters.stores.load(Ordering::Acquire), 0);
        assert_eq!(stores.count.load(Ordering::Acquire), 0);
        assert!(model.files.lock().await.is_empty());
        let after = model.namespace.snapshot(0).await.unwrap();
        assert_eq!(after.generation, before.generation);
        assert_eq!(after.next_inode, before.next_inode);
        assert_eq!(after.nodes.len(), before.nodes.len());
        for (left, right) in after.nodes.iter().zip(&before.nodes) {
            assert_eq!(left.entry, right.entry);
            assert_eq!(left.names, right.names);
            assert_eq!(left.attached, right.attached);
        }
        assert_eq!(after.files, before.files);
    }
}

#[tokio::test]
async fn directory_generation_changes_once_per_successful_name_mutation() {
    let directory = Directory::new("directory-generation-sequence");
    let (model, _backend, _gate, _stores) = fixture(&directory, Default::default(), 4096).await;
    let original = model.directory_generation.load(Ordering::Acquire);
    let (handle, _) = model.create(1, b"new", 0o644, WRITE).await.unwrap();
    assert_eq!(
        model.directory_generation.load(Ordering::Acquire),
        original + 1
    );
    model.release(handle).await.unwrap();
    model.mkdir(1, b"directory").await.unwrap();
    assert_eq!(
        model.directory_generation.load(Ordering::Acquire),
        original + 2
    );
    assert!(model.mkdir(1, b"directory").await.is_err());
    assert_eq!(
        model.directory_generation.load(Ordering::Acquire),
        original + 2
    );
    model.rename(1, b"new", 1, b"renamed").await.unwrap();
    assert_eq!(
        model.directory_generation.load(Ordering::Acquire),
        original + 3
    );
    model.rename(1, b"renamed", 1, b"renamed").await.unwrap();
    assert_eq!(
        model.directory_generation.load(Ordering::Acquire),
        original + 4
    );
    model.remove(1, b"renamed", false).await.unwrap();
    assert_eq!(
        model.directory_generation.load(Ordering::Acquire),
        original + 5
    );
    model.remove(1, b"directory", true).await.unwrap();
    assert_eq!(model.getattr(1).await.unwrap().generation, original + 6);
}

#[tokio::test]
async fn directory_generation_accepts_the_last_representable_value() {
    let directory = Directory::new("directory-generation-last");
    let (model, _backend, _gate, _stores) = fixture(&directory, Default::default(), 4096).await;
    model
        .directory_generation
        .store(i64::MAX as u64 - 1, Ordering::Release);
    let attributes = model.mkdir(1, b"last").await.unwrap();
    assert_eq!(attributes.generation, i64::MAX as u64);
    let before = model.namespace.snapshot(0).await.unwrap();
    assert!(matches!(
        model.mkdir(1, b"overflow").await,
        Err(Error::Core(core::Error::ResourceLimit))
    ));
    let after = model.namespace.snapshot(0).await.unwrap();
    assert_eq!(after.generation, before.generation);
    assert_eq!(after.next_inode, before.next_inode);
    assert_eq!(model.getattr(1).await.unwrap().generation, i64::MAX as u64);
}
