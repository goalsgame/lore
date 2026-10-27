// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

use std::collections::BTreeMap;
use std::os::unix::fs::OpenOptionsExt;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use super::ImageRelationError;
use crate::capture_memory::CaptureLimits;
use crate::capture_memory::CaptureMemory;
use crate::core::Node;
use crate::core::NodeKey;
use crate::core::NodeKind;
use crate::namespace::Entry;
use crate::namespace::NamespaceSnapshot;
use crate::namespace::SnapshotNode;
use crate::recovery::Limits;
use crate::recovery::Recovery;
use crate::recovery::inspection::RecoveryInspection;
use crate::recovery::intent;

static NEXT: AtomicU64 = AtomicU64::new(0);
struct Fixture {
    // Retain the inspection claim through owned fixture cleanup.
    inspector: RecoveryInspection,
    path: PathBuf,
}
impl Fixture {
    fn new(
        source: &NamespaceSnapshot,
        candidate: &NamespaceSnapshot,
        change: impl FnOnce(&mut intent::Intent),
    ) -> Self {
        let path = std::env::temp_dir().join(format!(
            "lore-artifact-relation-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        drop(Recovery::open(&path, Limits::default()).unwrap());
        let inspector = RecoveryInspection::open_existing(&path, Limits::default()).unwrap();
        let directory = path.join("transition");
        std::fs::create_dir(&directory).unwrap();
        std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o700)).unwrap();
        let limit = Limits::default().max_checkpoint_bytes;
        let source_bytes = crate::recovery::codec::encode(source, limit).unwrap();
        let candidate_bytes = crate::recovery::codec::encode(candidate, limit).unwrap();
        let old = intent::Anchors {
            repository: source.original_root.key.repository,
            instance: [4; 16],
            current: [5; 32].into(),
            branch: [6; 16].into(),
            staged: None,
        };
        let mut metadata = intent::Intent {
            transaction: [7; 32],
            original_root: source.original_root.key,
            scope: source.original_root.key,
            source: intent::CheckpointIdentity::from_bytes(&source_bytes, limit).unwrap(),
            candidate: intent::CheckpointIdentity::from_bytes(&candidate_bytes, limit).unwrap(),
            old,
            new: intent::Anchors {
                current: [8; 32].into(),
                ..old
            },
        };
        change(&mut metadata);
        let fixture = Self { inspector, path };
        fixture.write("source", &source_bytes);
        fixture.write("candidate", &candidate_bytes);
        fixture.write("intent", &metadata.encode(limit).unwrap());
        fixture
    }
    fn write(&self, name: &str, bytes: &[u8]) {
        use std::io::Write;
        let mut file = std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .mode(0o600)
            .open(self.path.join("transition").join(name))
            .unwrap();
        file.write_all(bytes).unwrap();
    }
    fn check(&self) -> Result<(), ImageRelationError> {
        let mut memory = CaptureMemory::new(CaptureLimits {
            max_buffer_bytes: 4 * 1024 * 1024,
            max_metadata_nodes: 100_000,
            max_name_bytes: 1024 * 1024,
        })
        .unwrap();
        self.inspector
            .load_transition_budgeted(&mut memory)
            .unwrap()
            .unwrap()
            .check_image_relation()
    }
    fn bytes(&self, name: &str) -> Vec<u8> {
        std::fs::read(self.path.join("transition").join(name)).unwrap()
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.path).unwrap();
    }
}
fn image(generation: u64) -> NamespaceSnapshot {
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
        generation,
    }
}
fn replace_root(snapshot: &mut NamespaceSnapshot, root: Node) {
    snapshot.original_root = root.clone();
    snapshot.nodes[0].entry.authority = root.key;
    snapshot.nodes[0].entry.kind = root.kind;
    snapshot.nodes[0].entry.base = Some(root);
}
#[test]
fn checked_distinct_images_match_without_selecting_or_changing_files() {
    let fixture = Fixture::new(&image(1), &image(2), |_| {});
    let source = fixture.bytes("source");
    let candidate = fixture.bytes("candidate");
    let intent = fixture.bytes("intent");
    assert_ne!(source, candidate);
    assert_eq!(fixture.check(), Ok(()));
    assert_eq!(fixture.bytes("source"), source);
    assert_eq!(fixture.bytes("candidate"), candidate);
    assert_eq!(fixture.bytes("intent"), intent);
}
#[test]
fn exact_identity_mismatch_is_distinguished_for_each_image() {
    let source = Fixture::new(&image(1), &image(2), |intent| intent.source.sha256[0] ^= 1);
    assert_eq!(source.check(), Err(ImageRelationError::SourceIdentity));
    let candidate = Fixture::new(&image(1), &image(2), |intent| intent.candidate.length += 1);
    assert_eq!(
        candidate.check(),
        Err(ImageRelationError::CandidateIdentity)
    );
}
#[test]
fn both_root_keys_and_full_descriptors_are_required() {
    let wrong_intent = Fixture::new(&image(1), &image(2), |intent| {
        intent.original_root.exposure += 1;
    });
    assert_eq!(
        wrong_intent.check(),
        Err(ImageRelationError::OriginalRootKey)
    );
    let mut candidate = image(2);
    let mut root = candidate.original_root.clone();
    root.key.exposure += 1;
    replace_root(&mut candidate, root);
    let wrong_candidate = Fixture::new(&image(1), &candidate, |_| {});
    assert_eq!(
        wrong_candidate.check(),
        Err(ImageRelationError::OriginalRootKey)
    );
    let mut candidate = image(2);
    let mut root = candidate.original_root.clone();
    root.mode = 0o700;
    replace_root(&mut candidate, root);
    let descriptor = Fixture::new(&image(1), &candidate, |_| {});
    assert_eq!(
        descriptor.check(),
        Err(ImageRelationError::OriginalRootDescriptor)
    );
}
#[test]
fn missing_invalid_unknown_excess_and_pending_facts_never_complete() {
    for name in ["source", "candidate", "intent"] {
        let missing = Fixture::new(&image(1), &image(2), |_| {});
        std::fs::remove_file(missing.path.join("transition").join(name)).unwrap();
        assert_eq!(missing.check(), Err(ImageRelationError::Incomplete));
        let invalid = Fixture::new(&image(1), &image(2), |_| {});
        std::fs::remove_file(invalid.path.join("transition").join(name)).unwrap();
        invalid.write(name, b"invalid");
        assert_eq!(invalid.check(), Err(ImageRelationError::Incomplete));
        assert_eq!(invalid.bytes(name), b"invalid");
    }
    for count in [1, 8] {
        let unknown = Fixture::new(&image(1), &image(2), |_| {});
        for index in 0..count {
            unknown.write(&format!("unknown{index}"), b"retained");
        }
        assert_eq!(unknown.check(), Err(ImageRelationError::Incomplete));
        assert_eq!(
            std::fs::read_dir(unknown.path.join("transition"))
                .unwrap()
                .count(),
            3 + count
        );
    }
    for valid in [false, true] {
        let pending = Fixture::new(&image(1), &image(2), |_| {});
        let bytes = if valid {
            pending.bytes("intent")
        } else {
            b"partial".to_vec()
        };
        pending.write("intent.pending", &bytes);
        assert_eq!(pending.check(), Err(ImageRelationError::Incomplete));
        assert_eq!(pending.bytes("intent.pending"), bytes);
    }
}
