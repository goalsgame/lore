// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

use super::*;
use crate::capture_memory::CaptureLimits;
use crate::capture_memory::CaptureMemory;
use crate::capture_memory::Footprint;
use crate::overlay::SnapshotExtent;

fn memory(bytes: usize, nodes: usize, names: usize) -> CaptureMemory {
    CaptureMemory::new(CaptureLimits {
        max_buffer_bytes: bytes,
        max_metadata_nodes: nodes,
        max_name_bytes: names,
    })
    .unwrap()
}
pub(super) fn ample() -> CaptureMemory {
    memory(16 * 1024 * 1024, 100_000, 1024 * 1024)
}
fn data_image() -> NamespaceSnapshot {
    let mut snapshot = image();
    let root = snapshot.original_root.clone();
    let node = Node {
        key: NodeKey {
            node: 8,
            ..root.key
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
            metadata: FileMetadata {
                size: 4,
                mode: 0o644,
                generation: 1,
                mutable: true,
                writers: 0,
                reserved_storage_bytes: 4,
            },
            base_end: 0,
            extents: vec![SnapshotExtent {
                start: 0,
                stored: 0,
                data: b"data".to_vec(),
            }],
        },
    );
    snapshot.next_inode = 3;
    snapshot
}
fn encoded(snapshot: &NamespaceSnapshot) -> Vec<u8> {
    codec::encode(snapshot, 1024 * 1024).unwrap()
}

#[test]
fn consumed_aggregate_budget_refuses_before_any_payload_read() {
    struct NeverRead;
    impl Read for NeverRead {
        fn read(&mut self, _: &mut [u8]) -> std::io::Result<usize> {
            panic!("budget refusal reached payload IO")
        }
    }
    let mut budget = memory(16, 100, 100);
    budget.buffer(16).unwrap();
    assert!(matches!(
        read_bounded_with_memory(&mut NeverRead, 4, 16, Some(&mut budget)),
        Err(Error::ResourceLimit)
    ));
    assert_eq!(budget.reserved_buffers(), 16);
}

#[test]
fn exact_load_charges_encoded_decoded_and_validation_on_one_ledger() {
    let temp = Temp::new();
    let recovery = &mut Recovery::open(temp.0.join("journal"), Limits::default()).unwrap();
    let snapshot = data_image();
    recovery.publish(&snapshot).unwrap();
    let bytes = std::fs::read(recovery.directory.join("checkpoint")).unwrap();
    let mut too_small = memory(bytes.len(), 100_000, 1024 * 1024);
    assert!(matches!(
        recovery.load_exact_budgeted(&mut too_small),
        Err(Error::ResourceLimit)
    ));
    let mut aggregate = ample();
    aggregate.buffer(37).unwrap();
    let loaded = recovery
        .load_exact_budgeted(&mut aggregate)
        .unwrap()
        .unwrap();
    assert_eq!(loaded.bytes(), bytes);
    assert_eq!(loaded.snapshot().files[&2].extents[0].data, b"data");
    assert!(aggregate.reserved_buffers() > 37 + bytes.len());
    assert!(aggregate.reserved_nodes() > loaded.snapshot().nodes.len());
    let mut independently_reserved = ample();
    independently_reserved.buffer(37).unwrap();
    independently_reserved
        .buffer(loaded.encoded_capacity())
        .unwrap();
    independently_reserved
        .retain(Footprint::snapshot(loaded.snapshot()).unwrap())
        .unwrap();
    independently_reserved.validator(loaded.snapshot()).unwrap();
    independently_reserved
        .buffer(std::mem::size_of::<(u64, u64)>())
        .unwrap();
    assert_eq!(
        aggregate.reserved_buffers(),
        independently_reserved.reserved_buffers()
    );
    assert_eq!(
        aggregate.reserved_nodes(),
        independently_reserved.reserved_nodes()
    );
}

#[test]
fn decoder_charges_node_vector_before_reserving_its_capacity() {
    let bytes = encoded(&image());
    let mut budget = memory(1024 * 1024, 1, 1024);
    budget.nodes(1).unwrap();
    assert!(matches!(
        codec::decode_budgeted(&bytes, &mut budget),
        Err(Error::ResourceLimit)
    ));
    assert_eq!(budget.reserved_buffers(), 0);
}

#[test]
fn decoder_charges_backend_and_private_blobs_and_extent_vectors() {
    let mut backend = image();
    backend.backend_state = vec![3; 16];
    let bytes = encoded(&backend);
    let mut budget = memory(15, 100, 100);
    assert!(matches!(
        codec::decode_budgeted(&bytes, &mut budget),
        Err(Error::ResourceLimit)
    ));
    assert_eq!(budget.reserved_buffers(), 0);

    let bytes = encoded(&data_image());
    let extent_vector = std::mem::size_of::<SnapshotExtent>();
    let mut budget = memory(extent_vector - 1, 100, 100);
    assert!(matches!(
        codec::decode_budgeted(&bytes, &mut budget),
        Err(Error::ResourceLimit)
    ));
    assert_eq!(budget.reserved_buffers(), 0);
    let mut budget = memory(extent_vector + 3, 100, 100);
    assert!(matches!(
        codec::decode_budgeted(&bytes, &mut budget),
        Err(Error::ResourceLimit)
    ));
    assert_eq!(budget.reserved_buffers(), extent_vector);
}

#[test]
fn decoder_charges_name_pair_vectors_and_spelling_bytes() {
    let mut snapshot = image();
    let root = snapshot.original_root.clone();
    let child = Node {
        key: NodeKey {
            node: 8,
            ..root.key
        },
        ..root
    };
    snapshot.nodes[0].names = Some(vec![(b"name".to_vec(), 2)]);
    snapshot.nodes.push(SnapshotNode {
        receipt: None,
        entry: Entry {
            inode: 2,
            parent: 1,
            kind: NodeKind::Directory,
            authority: child.key,
            base: Some(child),
        },
        names: Some(Vec::new()),
        attached: true,
    });
    snapshot.next_inode = 3;
    let bytes = encoded(&snapshot);
    let node_vector = 2 * std::mem::size_of::<SnapshotNode>();
    let mut budget = memory(node_vector, 100, 100);
    assert!(matches!(
        codec::decode_budgeted(&bytes, &mut budget),
        Err(Error::ResourceLimit)
    ));
    assert_eq!(budget.reserved_buffers(), node_vector);
    let mut budget = memory(1024 * 1024, 100, 3);
    assert!(matches!(
        codec::decode_budgeted(&bytes, &mut budget),
        Err(Error::ResourceLimit)
    ));
    assert_eq!(
        budget.reserved_buffers(),
        node_vector + std::mem::size_of::<(Vec<u8>, u64)>()
    );
}

#[test]
fn decoder_charges_each_persistent_map_record() {
    let mut underlay = image();
    let scope = underlay.original_root.key;
    underlay.active_underlays.insert(
        scope,
        crate::core::ScopeHead {
            revision: scope.revision,
            root: Some(scope.node),
        },
    );
    let mut baseline = image();
    baseline.baselines.insert(
        scope,
        crate::core::HeldBaseline {
            scope,
            head: crate::core::ScopeHead {
                revision: scope.revision,
                root: Some(scope.node),
            },
            sequence: 1,
            parent_projection: None,
        },
    );
    let mut file = data_image();
    file.files.get_mut(&2).unwrap().extents.clear();
    for snapshot in [underlay, baseline, file] {
        let mut budget = memory(1024 * 1024, 1, 1024);
        budget.nodes(1).unwrap();
        assert!(matches!(
            codec::decode_budgeted(&encoded(&snapshot), &mut budget),
            Err(Error::ResourceLimit)
        ));
        assert_eq!(budget.reserved_buffers(), 0);
    }
}

#[test]
fn successor_scratch_is_reserved_before_unchanged_regression_checks() {
    let previous = data_image();
    let mut stale = previous.clone();
    stale.generation -= 1;
    let mut budget = memory(1024 * 1024, 1, 1024);
    assert!(matches!(
        crate::recovery::budgeted::successor(&previous, &stale, &mut budget),
        Err(Error::ResourceLimit)
    ));
    let mut budget = ample();
    assert!(matches!(
        crate::recovery::budgeted::successor(&previous, &stale, &mut budget),
        Err(Error::StaleSnapshot)
    ));
    let mut budget = ample();
    crate::recovery::budgeted::successor(&previous, &previous, &mut budget).unwrap();
    assert_eq!(
        budget.reserved_nodes(),
        previous.nodes.len() * 3 + previous.files.len() * 2
    );
}

#[test]
fn stored_range_validation_scratch_is_reserved_before_structural_validation() {
    let temp = Temp::new();
    let recovery = &mut Recovery::open(temp.0.join("journal"), Limits::default()).unwrap();
    recovery.publish(&data_image()).unwrap();
    let loaded = recovery.load_exact().unwrap().unwrap();
    let mut without_ranges = ample();
    without_ranges.buffer(loaded.encoded_capacity()).unwrap();
    without_ranges
        .retain(Footprint::snapshot(loaded.snapshot()).unwrap())
        .unwrap();
    without_ranges.validator(loaded.snapshot()).unwrap();
    let mut budget = memory(without_ranges.reserved_buffers(), 100_000, 1024 * 1024);
    assert!(matches!(
        recovery.load_exact_budgeted(&mut budget),
        Err(Error::ResourceLimit)
    ));
    assert_eq!(budget.reserved_buffers(), without_ranges.reserved_buffers());
}

fn signed(mut bytes: Vec<u8>) -> Vec<u8> {
    let end = bytes.len() - 32;
    bytes[8..16].copy_from_slice(&(end as u64).to_le_bytes());
    let checksum = ring::digest::digest(&ring::digest::SHA256, &bytes[..end]);
    bytes[end..].copy_from_slice(checksum.as_ref());
    bytes
}
fn category(result: Result<Option<LoadedCheckpoint>>) -> &'static str {
    match result {
        Err(Error::Corrupt) => "corrupt",
        Err(Error::Incompatible) => "incompatible",
        Err(Error::ResourceLimit) => "limit",
        other => panic!("unexpected load result: {}", other.is_ok()),
    }
}
#[test]
fn shared_decode_preserves_malformed_duplicate_and_wire_limit_rejections() {
    let temp = Temp::new();
    let recovery = &mut Recovery::open(temp.0.join("journal"), Limits::default()).unwrap();
    recovery.publish(&image()).unwrap();
    let canonical = encoded(&image());
    let mut old = canonical.clone();
    old[..8].copy_from_slice(b"LOREOV04");
    let mut bad_kind = canonical.clone();
    const KEY: usize = 16 + 16 + 32 + 4 + 8;
    const NODE: usize = KEY + 1 + 8 + 2;
    bad_kind[16 + KEY] = 9;
    let bad_kind = signed(bad_kind);
    let mut duplicate_nodes = image();
    duplicate_nodes.nodes.push(duplicate_nodes.nodes[0].clone());
    let mut underlay = image();
    let scope = underlay.original_root.key;
    underlay.active_underlays.insert(
        scope,
        crate::core::ScopeHead {
            revision: scope.revision,
            root: Some(scope.node),
        },
    );
    let source = encoded(&underlay);
    let count_at = 16 + NODE;
    let entry_at = count_at + 8;
    let entry_end = entry_at + KEY + 48 + 1 + 4;
    let mut duplicate_map = Vec::new();
    duplicate_map.extend_from_slice(&source[..count_at]);
    duplicate_map.extend_from_slice(&2u64.to_le_bytes());
    duplicate_map.extend_from_slice(&source[entry_at..entry_end]);
    duplicate_map.extend_from_slice(&source[entry_at..]);
    let duplicate_map = signed(duplicate_map);
    let mut bad_count = canonical.clone();
    bad_count[count_at..count_at + 8].copy_from_slice(&u64::MAX.to_le_bytes());
    let bad_count = signed(bad_count);
    for bytes in [
        Vec::new(),
        canonical[..canonical.len() - 1].to_vec(),
        old,
        bad_kind,
        encoded(&duplicate_nodes),
        duplicate_map,
        bad_count,
    ] {
        std::fs::write(recovery.directory.join("checkpoint"), bytes).unwrap();
        let old = category(recovery.load_exact());
        let budgeted = category(recovery.load_exact_budgeted(&mut ample()));
        assert_eq!(old, budgeted);
    }
}
