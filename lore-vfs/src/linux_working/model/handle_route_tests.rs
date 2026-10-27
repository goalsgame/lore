// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

use super::*;

#[tokio::test]
async fn old_lease_setattr_response_does_not_reauthorize_through_current_namespace_policy() {
    let fixture = crate::namespace::file_access::tests::fixture(true).await;
    let model = fixture.model.as_ref().unwrap();
    let file = model.file(fixture.inode).await.unwrap();
    let slot = model.slot().unwrap();
    let access = model
        .namespace
        .capture_file_access(fixture.inode, &file.overlay)
        .await
        .unwrap();
    // Represent a retained preexisting lease using the cell's original route.
    // No namespace publication or binding replacement is available here.
    let inner = file.overlay.open(Access::Write).await.unwrap();
    let lease = Arc::new(FileLease {
        file,
        inner: Mutex::new(Some(inner)),
        slot,
        cleanup: model.cleanup.clone(),
        flags: OpenFlags {
            write: true,
            append: false,
            truncate: false,
        },
        inode: fixture.inode,
        access,
    });
    let handle = model.insert(Handle::File(lease)).unwrap();
    fixture.current.denied.store(true, Ordering::SeqCst);
    assert!(model.getattr(fixture.inode).await.is_err());
    assert_eq!(
        model
            .getattr_with_handle(fixture.inode, Some(handle))
            .await
            .unwrap()
            .size,
        4
    );
    assert!(matches!(
        model.getattr_with_handle(1, Some(handle)).await,
        Err(Error::Core(core::Error::BadHandle))
    ));
    assert!(matches!(
        model
            .getattr_with_handle(fixture.inode, Some(u64::MAX))
            .await,
        Err(Error::Core(core::Error::BadHandle))
    ));
    let attributes = model
        .setattr(fixture.inode, Some(handle), Some(3), Some(0o755))
        .await
        .unwrap();
    assert_eq!(attributes.size, 3);
    assert_eq!(attributes.mode, 0o755);
    assert_eq!(attributes.entry.inode, fixture.inode);
    assert_eq!(model.read(handle, 0, 3).await.unwrap(), b"bas");
    fixture.old.denied.store(true, Ordering::SeqCst);
    assert!(
        model
            .setattr(fixture.inode, Some(handle), Some(2), None)
            .await
            .is_err()
    );
    model.release(handle).await.unwrap();
    assert!(matches!(
        model.getattr_with_handle(fixture.inode, Some(handle)).await,
        Err(Error::Core(core::Error::BadHandle))
    ));
    model.shutdown().await.unwrap();
}

#[tokio::test]
async fn directory_fstat_uses_its_retained_snapshot_and_never_falls_back_on_bad_fh() {
    let fixture = crate::namespace::file_access::tests::fixture(true).await;
    let model = fixture.model.as_ref().unwrap();
    let old_namespace = Namespace::new(
        fixture.old_reader.clone(),
        crate::namespace::file_access::tests::root(),
        fixture.old.clone(),
        namespace::Limits::default(),
    )
    .unwrap();
    let snapshot = old_namespace
        .pinned_directory_snapshot_bounded(1, 4)
        .await
        .unwrap();
    let generation = snapshot.snapshot().generation;
    let slot = model.slot().unwrap();
    let directory = Arc::new(Directory {
        snapshot,
        _charge: DirectoryCharge {
            counters: model.counters.clone(),
            entries: 0,
            bytes: 0,
        },
        _slot: slot,
    });
    let handle = model.insert(Handle::Directory(directory)).unwrap();
    fixture.current.denied.store(true, Ordering::SeqCst);
    assert!(model.getattr(1).await.is_err());
    let attributes = model.getattr_with_handle(1, Some(handle)).await.unwrap();
    assert_eq!(attributes.entry.inode, 1);
    assert_eq!(attributes.generation, generation);
    assert!(matches!(
        model.getattr_with_handle(fixture.inode, Some(handle)).await,
        Err(Error::Core(core::Error::BadHandle))
    ));
    fixture.old.denied.store(true, Ordering::SeqCst);
    assert!(model.getattr_with_handle(1, Some(handle)).await.is_err());
    model.release(handle).await.unwrap();
    assert!(matches!(
        model.getattr_with_handle(1, Some(handle)).await,
        Err(Error::Core(core::Error::BadHandle))
    ));
    model.shutdown().await.unwrap();
}
