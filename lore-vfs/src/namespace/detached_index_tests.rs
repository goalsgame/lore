// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

use super::*;

fn base_file() -> Node {
    Node {
        key: key(1),
        kind: NodeKind::File,
        size: 4,
        mode: 0o644,
    }
}

// Discard only the live directory cache, as a replacement namespace builder
// would do before resolving its new immutable view. Retained records stay owned.
async fn forget_directory_names(namespace: &Namespace) {
    let mut state = namespace.state.lock().await;
    state.nodes.get_mut(&1).unwrap().names = None;
}

#[tokio::test]
async fn detached_identical_keys_keep_distinct_inodes_bytes_and_restart_state() {
    let (namespace, gate) = fixture(vec![(b"file".to_vec(), base_file())], Limits::default());
    let mut retained = Vec::new();
    for bytes in [b"old1", b"old2", b"live"] {
        let entry = namespace.lookup(1, b"file").await.unwrap();
        let file = file(&namespace, entry.inode).await;
        let handle = file.open(Access::Write).await.unwrap();
        file.write(handle, 0, bytes).await.unwrap();
        file.release(handle).await.unwrap();
        let reader = file.open(Access::Read).await.unwrap();
        retained.push((entry.inode, file, reader, bytes));
        if bytes != b"live" {
            namespace.remove(1, b"file", false).await.unwrap();
            assert!(
                !namespace
                    .state
                    .lock()
                    .await
                    .backend_ids
                    .contains_key(&key(1))
            );
            forget_directory_names(&namespace).await;
        }
    }
    assert!(retained.windows(2).all(|pair| pair[0].0 < pair[1].0));
    for (_, file, reader, bytes) in &retained {
        assert_eq!(file.read(*reader, 0, 4).await.unwrap(), *bytes);
    }
    let live = retained.last().unwrap().0;
    {
        let mut state = namespace.state.lock().await;
        // Retiring an older record must not withdraw a newer live mapping.
        detach(
            &mut state,
            retained[0].0,
            &namespace.current_binding().unwrap(),
        );
        assert_eq!(state.backend_ids.get(&key(1)), Some(&live));
    }
    let snapshot = namespace.snapshot(100).await.unwrap();
    let mut duplicate_attached = snapshot.clone();
    duplicate_attached
        .nodes
        .iter_mut()
        .find(|node| node.entry.inode == retained[0].0)
        .unwrap()
        .attached = true;
    assert_eq!(
        duplicate_attached
            .validate(Limits::default(), FileLimits::default())
            .err(),
        Some(Error::InvalidInput.into())
    );
    let stores = snapshot
        .files
        .keys()
        .map(|&id| (id, Arc::new(Store::default()) as Arc<dyn SparseStore>))
        .collect();
    let restored = Namespace::restore(
        Arc::new(Base {
            entries: vec![(b"file".to_vec(), base_file())],
            fail: AtomicBool::new(false),
        }),
        root(),
        gate,
        snapshot,
        stores,
        Limits::default(),
        FileLimits::default(),
    )
    .await
    .unwrap();
    assert_eq!(restored.lookup(1, b"file").await.unwrap().inode, live);
    assert_eq!(
        restored.state.lock().await.backend_ids.get(&key(1)),
        Some(&live)
    );
    for (inode, _, _, bytes) in &retained {
        let file = file(&restored, *inode).await;
        let reader = file.open(Access::Read).await.unwrap();
        assert_eq!(file.read(reader, 0, 4).await.unwrap(), *bytes);
        file.release(reader).await.unwrap();
    }
    restored.remove(1, b"file", false).await.unwrap();
    forget_directory_names(&restored).await;
    assert!(restored.lookup(1, b"file").await.unwrap().inode > live);
}

#[tokio::test]
async fn stale_index_cannot_revive_detached_record_during_hydration() {
    let (namespace, _) = fixture(vec![(b"file".to_vec(), base_file())], Limits::default());
    let old = namespace.lookup(1, b"file").await.unwrap();
    namespace.remove(1, b"file", false).await.unwrap();
    forget_directory_names(&namespace).await;
    namespace
        .state
        .lock()
        .await
        .backend_ids
        .insert(key(1), old.inode);
    assert_eq!(
        namespace.lookup(1, b"file").await.err(),
        Some(Error::Io.into())
    );
    assert!(!namespace.state.lock().await.nodes[&old.inode].attached);
}

#[tokio::test]
async fn rename_replacement_withdraws_only_replaced_backend_identity() {
    let mut other = base_file();
    other.key = key(2);
    let (namespace, _) = fixture(
        vec![(b"a".to_vec(), base_file()), (b"b".to_vec(), other)],
        Limits::default(),
    );
    let source = namespace.lookup(1, b"a").await.unwrap();
    let target = namespace.lookup(1, b"b").await.unwrap();
    let source_file = file(&namespace, source.inode).await;
    let source_writer = source_file.open(Access::Write).await.unwrap();
    source_file.write(source_writer, 0, b"src1").await.unwrap();
    source_file.release(source_writer).await.unwrap();
    let target_file = file(&namespace, target.inode).await;
    let target_writer = target_file.open(Access::Write).await.unwrap();
    target_file.write(target_writer, 0, b"repl").await.unwrap();
    target_file.release(target_writer).await.unwrap();
    let retained_reader = target_file.open(Access::Read).await.unwrap();
    namespace.rename(1, b"a", 1, b"b").await.unwrap();
    let live = namespace.lookup(1, b"b").await.unwrap();
    assert_eq!(live.inode, source.inode);
    let live_file = file(&namespace, live.inode).await;
    let live_reader = live_file.open(Access::Read).await.unwrap();
    assert_eq!(live_file.read(live_reader, 0, 4).await.unwrap(), b"src1");
    assert_eq!(
        target_file.read(retained_reader, 0, 4).await.unwrap(),
        b"repl"
    );
    live_file.release(live_reader).await.unwrap();
    target_file.release(retained_reader).await.unwrap();
    let state = namespace.state.lock().await;
    assert_eq!(state.backend_ids.get(&key(1)), Some(&source.inode));
    assert!(!state.backend_ids.contains_key(&key(2)));
    assert!(!state.nodes[&target.inode].attached);
    assert_eq!(state.nodes[&target.inode].entry.authority, key(2));
}
