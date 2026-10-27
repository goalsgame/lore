// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

use std::collections::BTreeMap;

use super::*;
use crate::core::Node;
use crate::core::NodeKey;
use crate::core::NodeKind;
use crate::namespace::Entry;
use crate::namespace::SnapshotNode;

fn limits(bytes: usize) -> CaptureLimits {
    CaptureLimits {
        max_buffer_bytes: bytes,
        max_metadata_nodes: 1000,
        max_name_bytes: 1000,
    }
}
#[test]
fn retained_snapshot_counts_capacities_not_just_payload_lengths() {
    let root = Node {
        key: NodeKey {
            repository: [1; 16].into(),
            revision: Default::default(),
            node: 0,
            exposure: 1,
        },
        kind: NodeKind::Directory,
        size: 0,
        mode: 0o755,
    };
    let mut name = Vec::with_capacity(64);
    name.extend_from_slice(b"one");
    let mut names = Vec::with_capacity(16);
    names.push((name, 2));
    let mut nodes = Vec::with_capacity(8);
    nodes.push(SnapshotNode {
        receipt: None,
        entry: Entry {
            inode: 1,
            parent: 1,
            authority: root.key,
            kind: root.kind,
            base: Some(root.clone()),
        },
        names: Some(names),
        attached: true,
    });
    let snapshot = NamespaceSnapshot {
        original_root: root,
        active_underlays: BTreeMap::new(),
        nodes,
        next_inode: 3,
        generation: 1,
        files: BTreeMap::new(),
        backend_state: Vec::with_capacity(256),
        baselines: BTreeMap::new(),
    };
    let footprint = Footprint::snapshot(&snapshot).unwrap();
    let expected = snapshot.nodes.capacity() * std::mem::size_of::<SnapshotNode>()
        + snapshot.nodes[0].names.as_ref().unwrap().capacity()
            * std::mem::size_of::<(Vec<u8>, u64)>()
        + 64
        + 256;
    assert_eq!(footprint.buffers, expected);
    let mut memory = CaptureMemory::new(limits(expected)).unwrap();
    memory.retain(footprint).unwrap();
    assert!(memory.preflight_buffer(1).is_err());
    assert!(
        CaptureMemory::new(limits(expected - 1))
            .unwrap()
            .retain(footprint)
            .is_err()
    );
}
#[test]
fn known_allocations_and_arithmetic_overflow_fail_before_requesting_memory() {
    let mut memory = CaptureMemory::new(limits(7)).unwrap();
    assert!(memory.vector::<u8>(8).is_err());
    assert!(memory.vector::<u64>(usize::MAX).is_err());
    memory.vector::<u8>(7).unwrap();
    assert!(memory.buffer(usize::MAX).is_err());
    assert_eq!(memory.reserved_buffers(), 7);
    assert!(memory.nodes(1001).is_err());
    assert!(memory.names(1001).is_err());
}
