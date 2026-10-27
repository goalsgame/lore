// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use super::*;
use crate::core::Backend;
use crate::core::BackendFuture;
use crate::core::BackendPage;
use crate::core::Error as CoreError;
use crate::core::Node;
use crate::core::NodeKey;
use crate::core::NodeKind;
use crate::namespace::Entry;
use crate::namespace::Namespace;
use crate::namespace::NamespaceSnapshot;
use crate::namespace::SnapshotNode;
use crate::overlay::Access;
use crate::overlay::AccessGate;
use crate::overlay::FileStore;
use crate::overlay::Limits as FileLimits;
use crate::overlay::SparseStore;

mod bounded_encoding;

static NEXT: AtomicU64 = AtomicU64::new(0);
struct Temp(PathBuf);
impl Temp {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "lore-recovery-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for Temp {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}
fn root() -> Node {
    Node {
        key: NodeKey {
            repository: [3; 16].into(),
            revision: lore_base::types::Address {
                context: [4; 16].into(),
                hash: [5; 32].into(),
            },
            node: 7,
            exposure: 99,
        },
        kind: NodeKind::Directory,
        size: 0,
        mode: 0o755,
    }
}
fn image(generation: u64) -> NamespaceSnapshot {
    let root = root();
    NamespaceSnapshot {
        original_root: root.clone(),
        active_underlays: BTreeMap::new(),
        baselines: BTreeMap::new(),
        backend_state: Vec::new(),
        nodes: vec![SnapshotNode {
            receipt: None,
            entry: Entry {
                inode: 1,
                parent: 1,
                kind: root.kind,
                authority: root.key,
                base: Some(root),
            },
            names: Some(Vec::new()),
            attached: true,
        }],
        next_inode: 2,
        generation,
        files: BTreeMap::new(),
    }
}
struct Empty;
impl Backend for Empty {
    fn lookup<'a>(&'a self, _: &'a NodeKey, _: &'a [u8]) -> BackendFuture<'a, Node> {
        Box::pin(async { Err(CoreError::NotFound) })
    }
    fn list<'a>(&'a self, _: &'a NodeKey, _: u64, _: usize) -> BackendFuture<'a, BackendPage> {
        Box::pin(async {
            Ok(BackendPage {
                entries: Vec::new(),
                next_offset: None,
            })
        })
    }
    fn read<'a>(&'a self, _: &'a NodeKey, _: u64, _: usize) -> BackendFuture<'a, Vec<u8>> {
        Box::pin(async { panic!("local files must never read base content") })
    }
    fn digest<'a>(&'a self, _: &'a NodeKey) -> BackendFuture<'a, Option<[u8; 32]>> {
        Box::pin(async { panic!("local files must never query base digests") })
    }
}
struct Gate(AtomicBool);
impl AccessGate for Gate {
    fn authorize<'a>(&'a self, key: &'a NodeKey, _: Access) -> BackendFuture<'a, ()> {
        Box::pin(async move {
            assert_eq!(*key, root().key);
            if self.0.load(Ordering::Relaxed) {
                Ok(())
            } else {
                Err(CoreError::PermissionDenied)
            }
        })
    }
}

#[tokio::test]
async fn checkpoint_reopens_actual_data_names_modes_and_sparse_truncation() {
    let temp = Temp::new();
    let backend = Arc::new(Empty);
    let gate = Arc::new(Gate(AtomicBool::new(true)));
    let ns = Namespace::new(
        backend.clone(),
        root(),
        gate.clone(),
        crate::namespace::Limits::default(),
    )
    .unwrap();
    let store = Arc::new(FileStore::create(&temp.0.join("data")).await.unwrap());
    let entry = ns
        .create_file(1, b"before", 0o644, store.clone(), FileLimits::default())
        .await
        .unwrap();
    let file = ns
        .file(entry.inode, store, FileLimits::default())
        .await
        .unwrap();
    let handle = file.open(Access::Write).await.unwrap();
    file.write(handle, 0, b"abcdefgh").await.unwrap();
    file.write(handle, 2, b"XY").await.unwrap();
    file.truncate(handle, 5).await.unwrap();
    file.truncate(handle, 9).await.unwrap();
    file.set_executable(handle, true).await.unwrap();
    ns.rename(1, b"before", 1, b"after").await.unwrap();
    let old_snapshot = ns.snapshot(1024).await.unwrap();
    file.write(handle, 0, b"a").await.unwrap();
    let snapshot = ns.snapshot(1024).await.unwrap();
    assert_eq!(old_snapshot.generation, snapshot.generation);
    let path = temp.0.join("recovery");
    let mut recovery = Recovery::open(&path, Limits::default()).unwrap();
    recovery.publish(&snapshot).unwrap();
    assert!(matches!(
        recovery.publish(&old_snapshot),
        Err(Error::StaleSnapshot)
    ));
    let mut conflict = snapshot.clone();
    conflict.files.get_mut(&entry.inode).unwrap().metadata.mode = 0o600;
    assert!(matches!(
        recovery.publish(&conflict),
        Err(Error::StaleSnapshot)
    ));
    drop(recovery);
    drop(file);
    drop(ns);
    // The checkpoint carries the bytes; its source data file is disposable now.
    fs::remove_file(temp.0.join("data")).unwrap();
    let recovery = Recovery::open(&path, Limits::default()).unwrap();
    let restored = recovery.load().unwrap().unwrap();
    assert_eq!(restored.files[&entry.inode].key(), root().key);
    let store: Arc<dyn SparseStore> =
        Arc::new(FileStore::create(&temp.0.join("restored")).await.unwrap());
    gate.0.store(false, Ordering::Relaxed);
    assert!(
        Namespace::restore(
            backend.clone(),
            root(),
            gate.clone(),
            restored.clone(),
            BTreeMap::from([(entry.inode, store.clone())]),
            crate::namespace::Limits::default(),
            FileLimits::default()
        )
        .await
        .is_err()
    );
    gate.0.store(true, Ordering::Relaxed);
    let ns = Namespace::restore(
        backend,
        root(),
        gate.clone(),
        restored,
        BTreeMap::from([(entry.inode, store.clone())]),
        crate::namespace::Limits::default(),
        FileLimits::default(),
    )
    .await
    .unwrap();
    assert!(ns.lookup(1, b"before").await.is_err());
    assert_eq!(ns.lookup(1, b"after").await.unwrap().inode, entry.inode);
    let file = ns
        .file(entry.inode, store, FileLimits::default())
        .await
        .unwrap();
    assert_eq!(file.read(handle, 0, 10).await, Err(CoreError::BadHandle));
    let handle = file.open(Access::Read).await.unwrap();
    assert_eq!(file.read(handle, 0, 20).await.unwrap(), b"abXYe\0\0\0\0");
    assert_eq!(file.metadata().await.unwrap().mode, 0o755);
    gate.0.store(false, Ordering::Relaxed);
    assert_eq!(
        file.read(handle, 0, 1).await,
        Err(CoreError::PermissionDenied)
    );
}

#[test]
fn strict_bounded_format_rejects_every_truncation_corruption_and_wrong_version() {
    let bytes = codec::encode(&image(1), 4096).unwrap();
    for length in 0..bytes.len() {
        assert!(
            codec::decode(&bytes[..length]).is_err(),
            "accepted prefix {length}"
        );
    }
    for offset in 8..bytes.len() {
        let mut corrupt = bytes.clone();
        corrupt[offset] ^= 1;
        assert!(matches!(codec::decode(&corrupt), Err(Error::Corrupt)));
    }
    let mut wrong = bytes.clone();
    wrong[7] ^= 1;
    assert!(matches!(codec::decode(&wrong), Err(Error::Incompatible)));
    assert!(matches!(
        codec::encode(&image(1), bytes.len() - 1),
        Err(Error::ResourceLimit)
    ));
    let mut malformed = image(1);
    malformed.nodes[0].entry.parent = 44;
    let temp = Temp::new();
    let mut recovery = Recovery::open(temp.0.join("overlay"), Limits::default()).unwrap();
    assert!(matches!(recovery.publish(&malformed), Err(Error::Corrupt)));
}

#[test]
fn owner_lock_is_exclusive_and_stale_lock_files_are_reusable() {
    let temp = Temp::new();
    let path = temp.0.join("overlay");
    let recovery = Recovery::open(&path, Limits::default()).unwrap();
    assert!(matches!(
        Recovery::open(&path, Limits::default()),
        Err(Error::Busy)
    ));
    drop(recovery);
    assert!(Recovery::open(&path, Limits::default()).is_ok());
}

#[test]
fn failed_publications_keep_previous_acknowledgment_and_retain_pending_data() {
    for boundary in [
        Boundary::Write,
        Boundary::FileSync,
        Boundary::Rename,
        Boundary::DirectorySync,
    ] {
        let temp = Temp::new();
        let path = temp.0.join("overlay");
        let mut recovery = Recovery::open(&path, Limits::default()).unwrap();
        recovery.publish(&image(1)).unwrap();
        recovery.fault = Some(boundary);
        assert!(recovery.publish(&image(2)).is_err());
        if boundary == Boundary::DirectorySync {
            assert!(matches!(recovery.publish(&image(3)), Err(Error::Uncertain)));
        } else {
            assert!(recovery.has_pending().unwrap());
            recovery.fault = None;
            assert!(matches!(recovery.publish(&image(3)), Err(Error::Pending)));
        }
        drop(recovery);
        let recovery = Recovery::open(&path, Limits::default()).unwrap();
        assert_eq!(
            recovery.load().unwrap().unwrap().generation,
            if boundary == Boundary::DirectorySync {
                2
            } else {
                1
            }
        );
    }
}

#[test]
fn committed_corruption_is_never_hidden_by_pending_data_or_rewritten() {
    let temp = Temp::new();
    let path = temp.0.join("overlay");
    let mut recovery = Recovery::open(&path, Limits::default()).unwrap();
    recovery.publish(&image(1)).unwrap();
    let checkpoint = path.join("checkpoint");
    let mut file = OpenOptions::new().write(true).open(&checkpoint).unwrap();
    file.write_all(b"corrupt!").unwrap();
    let before = fs::read(&checkpoint).unwrap();
    assert!(recovery.load().is_err());
    assert_eq!(fs::read(&checkpoint).unwrap(), before);
    assert!(recovery.publish(&image(2)).is_err());
    drop(recovery);
    let mut recovery = Recovery::open(&path, Limits::default()).unwrap();
    assert!(recovery.publish(&image(2)).is_err());
    assert_eq!(fs::read(&checkpoint).unwrap(), before);
}

#[test]
#[allow(clippy::exit)] // Deliberate abrupt exit validates OS ownership release without Drop.
fn ownership_child() {
    let Ok(path) = std::env::var("LORE_RECOVERY_CHILD") else {
        return;
    };
    let mut recovery = Recovery::open(path, Limits::default()).unwrap();
    let boundary = std::env::var("LORE_RECOVERY_BOUNDARY").unwrap();
    recovery.fault = match boundary.as_str() {
        "write" => Some(Boundary::Write),
        "sync" => Some(Boundary::FileSync),
        "rename" => Some(Boundary::Rename),
        "directory" => Some(Boundary::DirectorySync),
        "success" => None,
        _ => panic!("unknown boundary"),
    };
    let result = recovery.publish(&image(2));
    assert_eq!(result.is_ok(), boundary == "success");
    // Bypass destructors as a process crash would, including the lock guard.
    std::process::exit(0);
}

#[test]
fn process_exit_at_publication_boundaries_preserves_acknowledged_checkpoint() {
    for boundary in ["write", "sync", "rename", "directory", "success"] {
        let temp = Temp::new();
        let path = temp.0.join("overlay");
        let mut recovery = Recovery::open(&path, Limits::default()).unwrap();
        recovery.publish(&image(1)).unwrap();
        drop(recovery);
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "recovery::tests::ownership_child", "--nocapture"])
            .env("LORE_RECOVERY_CHILD", &path)
            .env("LORE_RECOVERY_BOUNDARY", boundary)
            .status()
            .unwrap();
        assert!(status.success());
        let recovery = Recovery::open(&path, Limits::default()).unwrap();
        assert_eq!(
            recovery.load().unwrap().unwrap().generation,
            if matches!(boundary, "directory" | "success") {
                2
            } else {
                1
            }
        );
    }
}

#[cfg(unix)]
#[test]
fn public_permissions_symlinks_and_oversized_records_are_rejected() {
    use std::os::unix::fs::PermissionsExt;
    use std::os::unix::fs::symlink;
    let temp = Temp::new();
    let path = temp.0.join("overlay");
    let mut recovery = Recovery::open(&path, Limits::default()).unwrap();
    recovery.publish(&image(1)).unwrap();
    drop(recovery);
    fs::set_permissions(&path, fs::Permissions::from_mode(0o755)).unwrap();
    assert!(matches!(
        Recovery::open(&path, Limits::default()),
        Err(Error::PrivateStorageRequired)
    ));
    fs::set_permissions(&path, fs::Permissions::from_mode(0o700)).unwrap();
    symlink(&path, temp.0.join("link")).unwrap();
    assert!(matches!(
        Recovery::open(temp.0.join("link"), Limits::default()),
        Err(Error::PrivateStorageRequired)
    ));
    let recovery = Recovery::open(
        &path,
        Limits {
            max_checkpoint_bytes: codec::MIN_SIZE,
        },
    )
    .unwrap();
    assert!(matches!(recovery.load(), Err(Error::ResourceLimit)));
}

#[test]
fn stale_checkpoint_cannot_replace_acknowledged_namespace_edits() {
    let temp = Temp::new();
    let mut recovery = Recovery::open(temp.0.join("overlay"), Limits::default()).unwrap();
    recovery.publish(&image(2)).unwrap();
    assert!(matches!(
        recovery.publish(&image(1)),
        Err(Error::StaleSnapshot)
    ));
    assert_eq!(recovery.load().unwrap().unwrap().generation, 2);
    assert!(!recovery.has_pending().unwrap());
}

#[test]
fn unrelated_root_and_conflicting_equal_generation_are_rejected() {
    let temp = Temp::new();
    let mut recovery = Recovery::open(temp.0.join("overlay"), Limits::default()).unwrap();
    recovery.publish(&image(2)).unwrap();
    let mut unrelated = image(3);
    unrelated.original_root.key.exposure += 1;
    unrelated.nodes[0].entry.authority.exposure += 1;
    unrelated.nodes[0].entry.base.as_mut().unwrap().key.exposure += 1;
    assert!(matches!(
        recovery.publish(&unrelated),
        Err(Error::StaleSnapshot)
    ));
    let mut conflict = image(2);
    conflict.nodes[0].names = None;
    assert!(matches!(
        recovery.publish(&conflict),
        Err(Error::StaleSnapshot)
    ));
}

#[test]
fn same_generation_directory_materialization_can_enrich_a_checkpoint() {
    let temp = Temp::new();
    let mut recovery = Recovery::open(temp.0.join("overlay"), Limits::default()).unwrap();
    let mut before = image(1);
    before.nodes[0].names = None;
    recovery.publish(&before).unwrap();
    recovery.publish(&image(1)).unwrap();
    assert!(recovery.load().unwrap().unwrap().nodes[0].names.is_some());
}

#[test]
fn backend_provenance_can_only_grow_by_appending_routes() {
    let temp = Temp::new();
    let mut recovery = Recovery::open(temp.0.join("recovery"), Limits::default()).unwrap();
    let mut first = image(1);
    first.backend_state = vec![1, 2];
    recovery.publish(&first).unwrap();
    let mut next = first.clone();
    next.backend_state.push(3);
    recovery.publish(&next).unwrap();
    let mut rebound = next.clone();
    rebound.backend_state[1] = 9;
    assert!(matches!(
        recovery.publish(&rebound),
        Err(Error::StaleSnapshot)
    ));
    assert!(matches!(
        recovery.publish(&first),
        Err(Error::StaleSnapshot)
    ));
    assert_eq!(
        recovery.load().unwrap().unwrap().backend_state,
        next.backend_state
    );
}

#[test]
fn receipt_enrichment_is_monotonic_and_versions_fail_closed() {
    use crate::overlay::CommittedProof;
    use crate::overlay::Receipt;
    let mut before = image(1);
    let mut file = before.nodes[0].clone();
    file.entry.inode = 2;
    file.entry.parent = 1;
    file.entry.kind = NodeKind::File;
    file.entry.authority.node = 8;
    let mut base = file.entry.base.clone().unwrap();
    base.key = file.entry.authority;
    base.kind = NodeKind::File;
    base.size = 3;
    base.mode = 0o444;
    file.entry.base = Some(base.clone());
    file.names = None;
    before.next_inode = 3;
    before.nodes[0].names = Some(vec![(b"file".to_vec(), 2)]);
    before.nodes.push(file);
    let mut committed = base;
    committed.key.revision = lore_base::types::Address::zero_context_hash([9; 32].into());
    let receipt = Receipt {
        inode: 2,
        authority: before.nodes[1].entry.authority,
        generation: 0,
        mode: 0o444,
        proof: CommittedProof {
            node: committed,
            address: lore_base::types::Address::zero_context_hash([10; 32].into()),
        },
        sha256: None,
    };
    let mut verified = before.clone();
    verified.nodes[1].receipt = Some(receipt);
    let temp = Temp::new();
    let mut recovery = Recovery::open(temp.0.join("recovery"), Limits::default()).unwrap();
    recovery.publish(&before).unwrap();
    recovery.publish(&verified).unwrap();
    let mut hashed = verified.clone();
    hashed.nodes[1].receipt.as_mut().unwrap().sha256 = Some([11; 32]);
    recovery.publish(&hashed).unwrap();
    assert_eq!(
        recovery.load().unwrap().unwrap().nodes[1].receipt,
        hashed.nodes[1].receipt
    );
    for invalid in [before, verified] {
        assert!(matches!(
            recovery.publish(&invalid),
            Err(Error::StaleSnapshot)
        ));
    }
    let mut rebound = hashed.clone();
    rebound.nodes[1]
        .receipt
        .as_mut()
        .unwrap()
        .proof
        .node
        .key
        .node = 99;
    assert!(matches!(
        recovery.publish(&rebound),
        Err(Error::StaleSnapshot)
    ));
    let mut conflict = hashed.clone();
    conflict.nodes[1].receipt.as_mut().unwrap().sha256 = Some([12; 32]);
    assert!(matches!(
        recovery.publish(&conflict),
        Err(Error::StaleSnapshot)
    ));
    let mut encoded = codec::encode(&hashed, Limits::default().max_checkpoint_bytes).unwrap();
    for version in *b"234" {
        encoded[7] = version;
        assert!(matches!(codec::decode(&encoded), Err(Error::Incompatible)));
    }
}

#[test]
fn independent_scoped_heads_round_trip_and_reject_regression_or_removal() {
    use crate::core::HeldBaseline;
    use crate::core::ScopeHead;
    use crate::core::ScopePin;
    use crate::core::ScopeProjection;
    let directory = Temp::new();
    let mut recovery = Recovery::open(directory.0.join("private"), Limits::default()).unwrap();
    let mut first = image(1);
    let parent = root().key;
    let child = NodeKey {
        exposure: 2,
        node: 7,
        ..parent
    };
    first.nodes[0].names = Some(vec![(b"child".to_vec(), 2)]);
    first.next_inode = 3;
    first.nodes.push(SnapshotNode {
        receipt: None,
        entry: Entry {
            inode: 2,
            parent: 1,
            kind: NodeKind::Directory,
            authority: child,
            base: Some(Node {
                key: child,
                kind: NodeKind::Directory,
                size: 0,
                mode: 0o755,
            }),
        },
        names: Some(vec![]),
        attached: true,
    });
    let head = |value| ScopeHead {
        revision: lore_base::types::Address {
            context: parent.revision.context,
            hash: [value; 32].into(),
        },
        root: Some(0),
    };
    first.baselines.insert(
        parent,
        HeldBaseline {
            scope: parent,
            head: head(1),
            sequence: 1,
            parent_projection: None,
        },
    );
    first.baselines.insert(
        child,
        HeldBaseline {
            scope: child,
            head: head(2),
            sequence: 1,
            parent_projection: Some(ScopeProjection {
                parent,
                head: head(1).revision,
                target: Some(ScopePin {
                    repository: child.repository,
                    revision: head(2).revision,
                    node: 7,
                }),
            }),
        },
    );
    recovery.publish(&first).unwrap();
    assert_eq!(recovery.load().unwrap().unwrap().baselines, first.baselines);
    let mut second = first.clone();
    second.generation += 1;
    second.baselines.get_mut(&parent).unwrap().sequence = 2;
    second.baselines.get_mut(&parent).unwrap().head = head(3);
    recovery.publish(&second).unwrap();
    let mut third = second.clone();
    third.generation += 1;
    let child_head = third.baselines.get_mut(&child).unwrap();
    child_head.sequence = 2;
    child_head.head = head(4);
    child_head.head.root = None;
    recovery.publish(&third).unwrap();
    assert_eq!(recovery.load().unwrap().unwrap().baselines, third.baselines);
    for corruption in 0..4 {
        let mut stale = third.clone();
        stale.generation += 1;
        match corruption {
            0 => {
                stale.baselines.remove(&child);
            }
            1 => {
                stale.baselines.get_mut(&child).unwrap().sequence = 1;
            }
            2 => {
                stale.baselines.get_mut(&child).unwrap().head = head(9);
            }
            _ => {
                let value = stale.baselines.remove(&child).unwrap();
                let foreign = NodeKey {
                    exposure: 3,
                    ..child
                };
                stale.baselines.insert(
                    foreign,
                    HeldBaseline {
                        scope: foreign,
                        ..value
                    },
                );
            }
        }
        assert!(recovery.publish(&stale).is_err());
        assert_eq!(recovery.load().unwrap().unwrap().baselines, third.baselines);
    }
}
#[test]
fn scoped_checkpoint_rejects_old_version_and_unbounded_count() {
    let bytes = codec::encode(&image(1), 4096).unwrap();
    let mut previous = bytes.clone();
    previous[..8].copy_from_slice(b"LOREOV04");
    assert!(matches!(codec::decode(&previous), Err(Error::Incompatible)));
    let mut malformed = bytes;
    let underlay_count = 16 + 76 + 1 + 8 + 2;
    malformed[underlay_count..underlay_count + 8].copy_from_slice(&u64::MAX.to_le_bytes());
    let checksum = malformed.len() - 32;
    let hash = ring::digest::digest(&ring::digest::SHA256, &malformed[..checksum]);
    malformed[checksum..].copy_from_slice(hash.as_ref());
    assert!(codec::decode(&malformed).is_err());
}

#[test]
fn original_root_preserves_exact_identity_and_rejects_unsettled_or_invalid_state() {
    let directory = Temp::new();
    let path = directory.0.join("private");
    let mut recovery = Recovery::open(&path, Limits::default()).unwrap();
    assert!(recovery.original_root().unwrap().is_none());
    recovery.publish(&image(1)).unwrap();
    assert_eq!(recovery.original_root().unwrap(), Some(root()));
    fs::write(path.join("pending"), b"retained partial data").unwrap();
    assert!(matches!(recovery.original_root(), Err(Error::Pending)));
    assert_eq!(
        fs::read(path.join("pending")).unwrap(),
        b"retained partial data"
    );
    fs::remove_file(path.join("pending")).unwrap();
    let bytes = fs::read(path.join("checkpoint")).unwrap();
    let mut corrupt = bytes.clone();
    *corrupt.last_mut().unwrap() ^= 1;
    fs::write(path.join("checkpoint"), &corrupt).unwrap();
    assert!(matches!(recovery.original_root(), Err(Error::Corrupt)));
    assert_eq!(fs::read(path.join("checkpoint")).unwrap(), corrupt);
    let mut incompatible = bytes.clone();
    incompatible[..8].copy_from_slice(b"LOREOV03");
    fs::write(path.join("checkpoint"), &incompatible).unwrap();
    assert!(matches!(recovery.original_root(), Err(Error::Incompatible)));
    fs::write(path.join("checkpoint"), bytes).unwrap();
    recovery.test_fault(Some(Boundary::DirectorySync));
    assert!(matches!(recovery.publish(&image(2)), Err(Error::Uncertain)));
    assert!(matches!(recovery.original_root(), Err(Error::Uncertain)));
    drop(recovery);
    assert_eq!(
        Recovery::open(path, Limits::default())
            .unwrap()
            .original_root()
            .unwrap(),
        Some(root())
    );
}

mod published;

mod transition_admission;

mod owner_validation;
