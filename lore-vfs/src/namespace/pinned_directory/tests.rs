// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

use std::sync::Mutex as SyncMutex;

use super::*;
use crate::core::BackendFuture;
use crate::core::BackendPage;

#[derive(Default)]
struct Policy {
    denied: SyncMutex<Option<NodeKey>>,
    reads: SyncMutex<Vec<NodeKey>>,
}
impl AccessGate for Policy {
    fn authorize<'a>(&'a self, key: &'a NodeKey, access: Access) -> BackendFuture<'a, ()> {
        Box::pin(async move {
            assert_eq!(access, Access::Read);
            self.reads.lock().unwrap().push(*key);
            if self.denied.lock().unwrap().as_ref() == Some(key) {
                return Err(Error::PermissionDenied);
            }
            Ok(())
        })
    }
}
#[derive(Default)]
struct Routing {
    reads: SyncMutex<Vec<NodeKey>>,
}
impl Backend for Routing {
    fn lookup<'a>(&'a self, _: &'a NodeKey, _: &'a [u8]) -> BackendFuture<'a, Node> {
        Box::pin(async { panic!("retained authorization does not perform lookup") })
    }
    fn list<'a>(&'a self, _: &'a NodeKey, _: u64, _: usize) -> BackendFuture<'a, BackendPage> {
        Box::pin(async { panic!("retained authorization does not enumerate") })
    }
    fn read<'a>(&'a self, _: &'a NodeKey, _: u64, _: usize) -> BackendFuture<'a, Vec<u8>> {
        Box::pin(async { panic!("retained authorization does not hydrate") })
    }
    fn digest<'a>(&'a self, _: &'a NodeKey) -> BackendFuture<'a, Option<[u8; 32]>> {
        Box::pin(async { panic!("retained authorization does not query digests") })
    }
    fn revalidate<'a>(&'a self, key: &'a NodeKey, write: bool) -> BackendFuture<'a, ()> {
        Box::pin(async move {
            assert!(!write);
            self.reads.lock().unwrap().push(*key);
            Ok(())
        })
    }
}
fn entry(inode: u64, node: u32) -> Entry {
    Entry {
        inode,
        parent: 1,
        kind: NodeKind::Directory,
        authority: NodeKey {
            repository: [1; 16].into(),
            revision: lore_base::types::Address::default(),
            node,
            exposure: 9,
        },
        base: None,
    }
}
fn fixture() -> (PinnedDirectorySnapshot, Arc<Policy>, Arc<Routing>) {
    let policy = Arc::new(Policy::default());
    let routing = Arc::new(Routing::default());
    let gate = Arc::new(BackendGate {
        underlays: BTreeMap::new(),
        backend: routing.clone(),
        gate: policy.clone(),
    });
    let snapshot = DirectorySnapshot {
        // Equal inode numbers must not alias the captured authorization keys.
        entry: entry(1, 11),
        parent: entry(1, 12),
        generation: 4,
        entries: vec![NamedEntry {
            name: b"saved".to_vec(),
            entry: entry(2, 13),
        }],
    };
    (
        PinnedDirectorySnapshot::new(snapshot, gate),
        policy,
        routing,
    )
}
#[tokio::test]
async fn captured_directory_parent_and_child_use_exact_effective_read_route() {
    let (pinned, policy, routing) = fixture();
    pinned.authorize_directory().await.unwrap();
    for position in 0..3 {
        pinned.authorize_position(position).await.unwrap();
    }
    let keys = vec![
        entry(1, 11).authority,
        entry(1, 11).authority,
        entry(1, 12).authority,
        entry(2, 13).authority,
    ];
    assert_eq!(*routing.reads.lock().unwrap(), keys);
    let expected = keys
        .into_iter()
        .flat_map(|key| [key, key])
        .collect::<Vec<_>>();
    assert_eq!(*policy.reads.lock().unwrap(), expected);
    assert_eq!(pinned.snapshot().generation, 4);
}
#[tokio::test]
async fn revocation_and_invalid_positions_never_return_a_cached_grant() {
    let (pinned, policy, routing) = fixture();
    pinned.authorize_position(2).await.unwrap();
    *policy.denied.lock().unwrap() = Some(entry(2, 13).authority);
    assert_eq!(
        pinned.authorize_position(2).await,
        Err(Error::PermissionDenied.into())
    );
    pinned.authorize_directory().await.unwrap();
    *policy.denied.lock().unwrap() = Some(entry(1, 11).authority);
    assert_eq!(
        pinned.authorize_directory().await,
        Err(Error::PermissionDenied.into())
    );
    *policy.denied.lock().unwrap() = None;
    pinned.authorize_directory().await.unwrap();
    let before = routing.reads.lock().unwrap().len();
    assert_eq!(pinned.authorize_position(3).await, Err(Error::Range.into()));
    assert_eq!(
        pinned.authorize_position(usize::MAX).await,
        Err(Error::Range.into())
    );
    assert_eq!(routing.reads.lock().unwrap().len(), before);
}
