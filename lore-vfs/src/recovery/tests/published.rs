// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

use super::*;
use crate::capture_memory::CaptureLimits;
use crate::capture_memory::CaptureMemory;
use crate::capture_memory::Footprint;
use crate::overlay::FileSnapshot;
use crate::overlay::Metadata;
use crate::overlay::SnapshotExtent;
use crate::recovery::intent::CheckpointIdentity;

fn source(generation: u64, bytes: &[u8; 4]) -> NamespaceSnapshot {
    let mut snapshot = image(generation);
    let node = Node {
        key: NodeKey {
            node: 8,
            ..snapshot.original_root.key
        },
        kind: NodeKind::File,
        size: 4,
        mode: 0o644,
    };
    snapshot.nodes[0].names = Some(vec![(b"file".to_vec(), 2)]);
    snapshot.nodes.push(SnapshotNode {
        receipt: None,
        entry: Entry {
            inode: 2,
            parent: 1,
            kind: node.kind,
            authority: node.key,
            base: Some(node.clone()),
        },
        names: None,
        attached: true,
    });
    snapshot.files.insert(
        2,
        FileSnapshot {
            receipt: None,
            node,
            metadata: Metadata {
                size: 4,
                mode: 0o644,
                generation,
                mutable: true,
                writers: 0,
                reserved_storage_bytes: 4,
            },
            base_end: 0,
            extents: vec![SnapshotExtent {
                start: 0,
                stored: 0,
                data: bytes.to_vec(),
            }],
        },
    );
    snapshot.next_inode = 3;
    snapshot
}
fn memory(snapshot: &NamespaceSnapshot) -> CaptureMemory {
    let mut memory = CaptureMemory::new(CaptureLimits {
        max_buffer_bytes: 16 * 1024 * 1024,
        max_metadata_nodes: 100_000,
        max_name_bytes: 1024 * 1024,
    })
    .unwrap();
    memory
        .retain(Footprint::snapshot(snapshot).unwrap())
        .unwrap();
    memory
}

#[test]
fn retained_publication_returns_exact_durable_bytes_and_owns_no_recovery_lock() {
    let temp = Temp::new();
    let path = temp.0.join("journal");
    let mut recovery = Recovery::open(&path, Limits::default()).unwrap();
    recovery.publish(&source(1, b"old!")).unwrap();
    let snapshot = source(2, b"new!");
    let mut ledger = memory(&snapshot);
    let retained = recovery.publish_retained(snapshot, &mut ledger).unwrap();
    let bytes = fs::read(path.join("checkpoint")).unwrap();
    assert_eq!(retained.bytes(), bytes);
    assert_eq!(retained.encoded_capacity(), bytes.len());
    assert_eq!(
        retained.identity(),
        CheckpointIdentity::from_bytes(&bytes, bytes.len()).unwrap()
    );
    assert_eq!(retained.snapshot().files[&2].extents[0].data, b"new!");
    assert_eq!(retained.snapshot().generation, 2);
    assert!(!recovery.has_pending().unwrap());
    assert!(!recovery.requires_reopen());
    drop(recovery);
    let reopened = Recovery::open(&path, Limits::default()).unwrap();
    let loaded = reopened.load_exact().unwrap().unwrap();
    assert_eq!(loaded.bytes(), retained.bytes());
    assert_eq!(loaded.identity(), retained.identity());
    assert_eq!(loaded.snapshot().files[&2].extents[0].data, b"new!");
}

#[test]
fn every_fault_retains_existing_boundary_semantics_without_returning_an_image() {
    for boundary in [
        Boundary::Write,
        Boundary::FileSync,
        Boundary::Rename,
        Boundary::DirectorySync,
    ] {
        let temp = Temp::new();
        let path = temp.0.join("journal");
        let mut recovery = Recovery::open(&path, Limits::default()).unwrap();
        recovery.publish(&source(1, b"old!")).unwrap();
        let previous = fs::read(path.join("checkpoint")).unwrap();
        let snapshot = source(2, b"new!");
        let expected = codec::encode(&snapshot, Limits::default().max_checkpoint_bytes).unwrap();
        let mut ledger = memory(&snapshot);
        recovery.test_fault(Some(boundary));
        assert!(recovery.publish_retained(snapshot, &mut ledger).is_err());
        assert!(recovery.requires_reopen());
        if boundary == Boundary::DirectorySync {
            assert!(!recovery.has_pending().unwrap());
            assert_eq!(fs::read(path.join("checkpoint")).unwrap(), expected);
            let later = source(3, b"next");
            assert!(matches!(
                recovery.publish_retained(later.clone(), &mut memory(&later)),
                Err(Error::Uncertain)
            ));
        } else {
            assert!(recovery.has_pending().unwrap());
            assert_eq!(fs::read(path.join("checkpoint")).unwrap(), previous);
            let later = source(3, b"next");
            assert!(matches!(
                recovery.publish_retained(later.clone(), &mut memory(&later)),
                Err(Error::Pending)
            ));
        }
        drop(recovery);
        let reopened = Recovery::open(&path, Limits::default()).unwrap();
        let loaded = reopened.load_exact().unwrap().unwrap();
        assert_eq!(
            loaded.snapshot().files[&2].extents[0].data,
            if boundary == Boundary::DirectorySync {
                b"new!"
            } else {
                b"old!"
            }
        );
    }
}

#[test]
fn corrupt_current_and_stale_or_invalid_source_never_create_pending() {
    let temp = Temp::new();
    let path = temp.0.join("journal");
    let mut recovery = Recovery::open(&path, Limits::default()).unwrap();
    recovery.publish(&source(2, b"old!")).unwrap();
    let previous = fs::read(path.join("checkpoint")).unwrap();
    let stale = source(1, b"new!");
    assert!(matches!(
        recovery.publish_retained(stale.clone(), &mut memory(&stale)),
        Err(Error::StaleSnapshot)
    ));
    let mut invalid = source(3, b"new!");
    invalid.nodes[1].entry.parent = 999;
    assert!(matches!(
        recovery.publish_retained(invalid.clone(), &mut memory(&invalid)),
        Err(Error::Corrupt)
    ));
    assert_eq!(fs::read(path.join("checkpoint")).unwrap(), previous);
    assert!(!recovery.has_pending().unwrap());
    assert!(!recovery.requires_reopen());
    fs::write(path.join("checkpoint"), b"corrupt committed data").unwrap();
    let next = source(3, b"new!");
    assert!(matches!(
        recovery.publish_retained(next.clone(), &mut memory(&next)),
        Err(Error::Corrupt)
    ));
    assert_eq!(
        fs::read(path.join("checkpoint")).unwrap(),
        b"corrupt committed data"
    );
    assert!(!recovery.has_pending().unwrap());
}

#[test]
fn exhausted_operation_budget_does_not_start_publication() {
    let temp = Temp::new();
    let path = temp.0.join("journal");
    let mut recovery = Recovery::open(&path, Limits::default()).unwrap();
    recovery.publish(&source(1, b"old!")).unwrap();
    let previous = fs::read(path.join("checkpoint")).unwrap();
    let snapshot = source(2, b"new!");
    let mut ledger = memory(&snapshot);
    let used = ledger.reserved_buffers();
    ledger.buffer(16 * 1024 * 1024 - used).unwrap();
    assert!(matches!(
        recovery.publish_retained(snapshot, &mut ledger),
        Err(Error::ResourceLimit)
    ));
    assert_eq!(fs::read(path.join("checkpoint")).unwrap(), previous);
    assert!(!recovery.has_pending().unwrap());
    assert!(!recovery.requires_reopen());
}

#[test]
fn retained_pending_precheck_never_deletes_or_adopts_unacknowledged_data() {
    let temp = Temp::new();
    let path = temp.0.join("journal");
    let mut recovery = Recovery::open(&path, Limits::default()).unwrap();
    recovery.publish(&source(1, b"old!")).unwrap();
    let previous = fs::read(path.join("checkpoint")).unwrap();
    fs::write(path.join("pending"), b"unacknowledged").unwrap();
    let snapshot = source(2, b"new!");
    assert!(matches!(
        recovery.publish_retained(snapshot.clone(), &mut memory(&snapshot)),
        Err(Error::Pending)
    ));
    assert_eq!(fs::read(path.join("pending")).unwrap(), b"unacknowledged");
    assert_eq!(fs::read(path.join("checkpoint")).unwrap(), previous);
}

#[test]
fn encoding_budget_refusal_happens_before_creating_pending() {
    let first = Temp::new();
    let mut recovery = Recovery::open(first.0.join("journal"), Limits::default()).unwrap();
    recovery.publish(&source(1, b"old!")).unwrap();
    let snapshot = source(2, b"new!");
    let mut ledger = memory(&snapshot);
    let retained = recovery.publish_retained(snapshot, &mut ledger).unwrap();
    let total = ledger.reserved_buffers();
    let encoding = retained.encoded_capacity();

    let second = Temp::new();
    let path = second.0.join("journal");
    let mut recovery = Recovery::open(&path, Limits::default()).unwrap();
    recovery.publish(&source(1, b"old!")).unwrap();
    let previous = fs::read(path.join("checkpoint")).unwrap();
    let snapshot = source(2, b"new!");
    let mut ledger = CaptureMemory::new(CaptureLimits {
        max_buffer_bytes: total - 1,
        max_metadata_nodes: 100_000,
        max_name_bytes: 1024 * 1024,
    })
    .unwrap();
    ledger
        .retain(Footprint::snapshot(&snapshot).unwrap())
        .unwrap();
    assert!(matches!(
        recovery.publish_retained(snapshot, &mut ledger),
        Err(Error::ResourceLimit)
    ));
    assert_eq!(ledger.reserved_buffers(), total - encoding);
    assert_eq!(fs::read(path.join("checkpoint")).unwrap(), previous);
    assert!(!recovery.has_pending().unwrap());
    assert!(!recovery.requires_reopen());
}
