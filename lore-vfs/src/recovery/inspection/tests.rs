// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

use std::collections::BTreeMap;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::fs::symlink;
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use super::*;
use crate::capture_memory::CaptureLimits;
use crate::core::Node;
use crate::core::NodeKey;
use crate::core::NodeKind;
use crate::namespace::Entry;
use crate::namespace::NamespaceSnapshot;
use crate::namespace::SnapshotNode;
use crate::recovery::Recovery;
use crate::recovery::intent::CheckpointIdentity;

static NEXT: AtomicU64 = AtomicU64::new(0);
struct Temp(PathBuf);
impl Temp {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "lore-recovery-inspection-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for Temp {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}
fn image() -> NamespaceSnapshot {
    let root = Node {
        key: NodeKey {
            repository: [3; 16].into(),
            revision: Default::default(),
            node: 0,
            exposure: 1,
        },
        kind: NodeKind::Directory,
        size: 0,
        mode: 0o755,
    };
    NamespaceSnapshot {
        original_root: root.clone(),
        active_underlays: BTreeMap::new(),
        backend_state: Vec::new(),
        baselines: BTreeMap::new(),
        files: BTreeMap::new(),
        nodes: vec![SnapshotNode {
            receipt: None,
            entry: Entry {
                inode: 1,
                parent: 1,
                authority: root.key,
                kind: root.kind,
                base: Some(root),
            },
            attached: true,
            names: Some(Vec::new()),
        }],
        next_inode: 2,
        generation: 1,
    }
}
fn memory() -> CaptureMemory {
    CaptureMemory::new(CaptureLimits {
        max_buffer_bytes: 1024 * 1024,
        max_metadata_nodes: 100_000,
        max_name_bytes: 1024 * 1024,
    })
    .unwrap()
}
fn fifo(path: &Path) {
    use std::os::unix::ffi::OsStrExt;
    let path = std::ffi::CString::new(path.as_os_str().as_bytes()).unwrap();
    // SAFETY: this owned pathname is NUL-terminated and lives through mkfifo.
    assert_eq!(unsafe { libc::mkfifo(path.as_ptr(), 0o600) }, 0);
}
#[test]
fn inspection_never_creates_missing_directory_or_lock() {
    let temp = Temp::new();
    let path = temp.0.join("journal");
    assert!(RecoveryInspection::open_existing(&path, Limits::default()).is_err());
    assert!(!path.exists());
    std::fs::create_dir(&path).unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700)).unwrap();
    assert!(RecoveryInspection::open_existing(&path, Limits::default()).is_err());
    assert_eq!(std::fs::read_dir(&path).unwrap().count(), 0);
}
#[test]
fn actual_owner_lock_remains_exclusive_through_inspection_lifetime() {
    let temp = Temp::new();
    let path = temp.0.join("journal");
    let recovery = Recovery::open(&path, Limits::default()).unwrap();
    assert!(matches!(
        RecoveryInspection::open_existing(&path, Limits::default()),
        Err(Error::Busy)
    ));
    drop(recovery);
    let inspector = RecoveryInspection::open_existing(&path, Limits::default()).unwrap();
    assert!(matches!(
        Recovery::open(&path, Limits::default()),
        Err(Error::Busy)
    ));
    assert!(matches!(
        RecoveryInspection::open_existing(&path, Limits::default()),
        Err(Error::Busy)
    ));
    drop(inspector);
    assert!(Recovery::open(&path, Limits::default()).is_ok());
}
#[test]
fn unsafe_directory_lock_and_checkpoint_objects_fail_without_following_or_blocking() {
    for target in ["owner.lock", "checkpoint"] {
        for kind in 0..5 {
            let temp = Temp::new();
            let path = temp.0.join("journal");
            drop(Recovery::open(&path, Limits::default()).unwrap());
            let leaf = path.join(target);
            if leaf.exists() {
                std::fs::remove_file(&leaf).unwrap();
            }
            match kind {
                0 => symlink(temp.0.join("missing"), &leaf).unwrap(),
                1 => fifo(&leaf),
                2 => std::fs::create_dir(&leaf).unwrap(),
                3 => {
                    std::fs::write(&leaf, b"unsafe permissions").unwrap();
                    std::fs::set_permissions(&leaf, std::fs::Permissions::from_mode(0o644))
                        .unwrap();
                }
                4 => {
                    let shared = temp.0.join("shared");
                    std::fs::write(&shared, b"hardlinked").unwrap();
                    std::fs::set_permissions(&shared, std::fs::Permissions::from_mode(0o600))
                        .unwrap();
                    std::fs::hard_link(shared, &leaf).unwrap();
                }
                _ => unreachable!(),
            }
            if target == "owner.lock" {
                assert!(RecoveryInspection::open_existing(&path, Limits::default()).is_err());
            } else {
                let inspector =
                    RecoveryInspection::open_existing(&path, Limits::default()).unwrap();
                assert!(inspector.load_committed_budgeted(&mut memory()).is_err());
            }
            assert!(!temp.0.join("missing").exists());
        }
    }
    let temp = Temp::new();
    let real = temp.0.join("real");
    drop(Recovery::open(&real, Limits::default()).unwrap());
    let link = temp.0.join("link");
    symlink(&real, &link).unwrap();
    assert!(RecoveryInspection::open_existing(&link, Limits::default()).is_err());
    std::fs::set_permissions(&real, std::fs::Permissions::from_mode(0o755)).unwrap();
    assert!(RecoveryInspection::open_existing(&real, Limits::default()).is_err());
}
#[test]
fn transition_presence_is_a_nofollow_fact_not_ordinary_admission() {
    for kind in 0..3 {
        let temp = Temp::new();
        let path = temp.0.join("journal");
        drop(Recovery::open(&path, Limits::default()).unwrap());
        let inspector = RecoveryInspection::open_existing(&path, Limits::default()).unwrap();
        assert!(!inspector.transition_presence().unwrap());
        match kind {
            0 => std::fs::create_dir(path.join("transition")).unwrap(),
            1 => symlink(temp.0.join("missing"), path.join("transition")).unwrap(),
            2 => fifo(&path.join("transition")),
            _ => unreachable!(),
        }
        assert!(inspector.transition_presence().unwrap());
        assert!(
            inspector
                .load_committed_budgeted(&mut memory())
                .unwrap()
                .is_none()
        );
        assert!(!path.join("checkpoint").exists());
        assert!(!path.join("pending").exists());
        std::fs::write(path.join("checkpoint"), b"broken").unwrap();
        std::fs::set_permissions(
            path.join("checkpoint"),
            std::fs::Permissions::from_mode(0o600),
        )
        .unwrap();
        assert!(matches!(
            inspector.load_committed_budgeted(&mut memory()),
            Err(Error::Corrupt)
        ));
        assert!(inspector.transition_presence().unwrap());
        std::fs::set_permissions(
            path.join("checkpoint"),
            std::fs::Permissions::from_mode(0o644),
        )
        .unwrap();
        assert!(matches!(
            inspector.load_committed_budgeted(&mut memory()),
            Err(Error::PrivateStorageRequired)
        ));
        assert!(inspector.transition_presence().unwrap());
        drop(inspector);
        assert!(matches!(
            Recovery::open(&path, Limits::default()),
            Err(Error::TransitionPending)
        ));
    }
}
#[test]
fn checked_committed_bytes_are_budgeted_and_anchored_even_if_directory_path_moves() {
    let temp = Temp::new();
    let path = temp.0.join("journal");
    let mut recovery = Recovery::open(&path, Limits::default()).unwrap();
    recovery.publish(&image()).unwrap();
    drop(recovery);
    let expected = std::fs::read(path.join("checkpoint")).unwrap();
    let inspector = RecoveryInspection::open_existing(&path, Limits::default()).unwrap();
    let moved = temp.0.join("moved");
    std::fs::rename(&path, &moved).unwrap();
    let loaded = inspector
        .load_committed_budgeted(&mut memory())
        .unwrap()
        .unwrap();
    assert_eq!(loaded.bytes(), expected);
    assert_eq!(
        loaded.identity(),
        CheckpointIdentity::from_bytes(&expected, expected.len()).unwrap()
    );
    let mut exhausted = memory();
    exhausted.buffer(1024 * 1024).unwrap();
    assert!(matches!(
        inspector.load_committed_budgeted(&mut exhausted),
        Err(Error::ResourceLimit)
    ));
    assert_eq!(std::fs::read(moved.join("checkpoint")).unwrap(), expected);
    assert!(!moved.join("pending").exists());
    // Inspection exposes no writer; ordinary ownership remains busy at the
    // retained directory even after the pathname changes.
    assert!(matches!(
        Recovery::open(&moved, Limits::default()),
        Err(Error::Busy)
    ));
}

#[test]
fn non_directory_root_objects_are_never_opened_as_recovery_directories() {
    let temp = Temp::new();
    let regular = temp.0.join("regular");
    std::fs::write(&regular, b"owned file").unwrap();
    let pipe = temp.0.join("pipe");
    fifo(&pipe);
    for path in [regular, pipe] {
        assert!(RecoveryInspection::open_existing(&path, Limits::default()).is_err());
    }
}

#[test]
fn exact_noncanonical_wire_bytes_are_retained_without_reencoding() {
    use crate::overlay::FileSnapshot;
    use crate::overlay::Metadata;
    let temp = Temp::new();
    let path = temp.0.join("journal");
    let mut recovery = Recovery::open(&path, Limits::default()).unwrap();
    let mut snapshot = image();
    for inode in 2..=3 {
        let node = Node {
            key: NodeKey {
                node: inode as u32,
                ..snapshot.original_root.key
            },
            kind: NodeKind::File,
            size: 0,
            mode: 0o444,
        };
        snapshot.nodes[0]
            .names
            .as_mut()
            .unwrap()
            .push((format!("f{inode}").into_bytes(), inode));
        snapshot.nodes.push(SnapshotNode {
            receipt: None,
            entry: Entry {
                inode,
                parent: 1,
                kind: node.kind,
                authority: node.key,
                base: Some(node.clone()),
            },
            names: None,
            attached: true,
        });
        snapshot.files.insert(
            inode,
            FileSnapshot {
                receipt: None,
                node,
                metadata: Metadata {
                    size: 0,
                    mode: 0o444,
                    generation: 0,
                    mutable: false,
                    writers: 0,
                    reserved_storage_bytes: 0,
                },
                base_end: 0,
                extents: Vec::new(),
            },
        );
    }
    snapshot.next_inode = 4;
    recovery.publish(&snapshot).unwrap();
    drop(recovery);
    let canonical = std::fs::read(path.join("checkpoint")).unwrap();
    const KEY: usize = 16 + 16 + 32 + 4 + 8;
    const NODE: usize = KEY + 1 + 8 + 2;
    const FILE: usize = 8 + 8 + NODE + 8 + 2 + 8 + 1 + 8 + 8 + 1;
    let start = 16 + NODE + 4 * 8;
    let mut original = canonical.clone();
    original[start..start + FILE].copy_from_slice(&canonical[start + FILE..start + 2 * FILE]);
    original[start + FILE..start + 2 * FILE].copy_from_slice(&canonical[start..start + FILE]);
    let checksum = original.len() - 32;
    let hash = ring::digest::digest(&ring::digest::SHA256, &original[..checksum]);
    original[checksum..].copy_from_slice(hash.as_ref());
    std::fs::write(path.join("checkpoint"), &original).unwrap();
    let inspector = RecoveryInspection::open_existing(&path, Limits::default()).unwrap();
    let loaded = inspector
        .load_committed_budgeted(&mut memory())
        .unwrap()
        .unwrap();
    assert_eq!(loaded.bytes(), original);
    assert_eq!(
        loaded.identity(),
        CheckpointIdentity::from_bytes(&original, original.len()).unwrap()
    );
    assert_eq!(
        crate::recovery::codec::encode(loaded.snapshot(), canonical.len()).unwrap(),
        canonical
    );
    assert_ne!(loaded.bytes(), canonical);
}
