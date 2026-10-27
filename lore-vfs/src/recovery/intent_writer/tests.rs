// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

use std::collections::BTreeMap;
use std::os::unix::fs::MetadataExt;
use std::os::unix::fs::PermissionsExt;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use super::*;
use crate::capture_memory::CaptureLimits;
use crate::capture_memory::Footprint;
use crate::core::Node;
use crate::core::NodeKey;
use crate::core::NodeKind;
use crate::namespace::Entry;
use crate::namespace::SnapshotNode;
use crate::recovery::inspection::RecoveryInspection;

static NEXT: AtomicU64 = AtomicU64::new(0);
struct Temp(PathBuf);
impl Temp {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "lore-intent-writer-{}-{}",
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
fn image(generation: u64) -> NamespaceSnapshot {
    let root = Node {
        key: NodeKey {
            repository: [1; 16].into(),
            revision: lore_base::types::Address::zero_context_hash([2; 32].into()),
            node: 0,
            exposure: 0,
        },
        kind: NodeKind::Directory,
        size: 0,
        mode: 0o755,
    };
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
fn memory() -> CaptureMemory {
    CaptureMemory::new(CaptureLimits {
        max_buffer_bytes: 4 * 1024 * 1024,
        max_metadata_nodes: 100_000,
        max_name_bytes: 1024 * 1024,
    })
    .unwrap()
}
struct Fixture {
    recovery: Recovery,
    source: PublishedCheckpointImage,
    candidate: NamespaceSnapshot,
    encoded: Vec<u8>,
    record: Intent,
    memory: CaptureMemory,
    temp: Temp,
}
impl Fixture {
    fn new() -> Self {
        let temp = Temp::new();
        let mut recovery = Recovery::open(temp.0.join("recovery"), Limits::default()).unwrap();
        let mut memory = memory();
        let snapshot = image(1);
        memory
            .retain(Footprint::snapshot(&snapshot).unwrap())
            .unwrap();
        let source = recovery.publish_retained(snapshot, &mut memory).unwrap();
        let candidate = image(2);
        memory
            .retain(Footprint::snapshot(&candidate).unwrap())
            .unwrap();
        let encoded = encode_candidate(
            &candidate,
            Limits::default().max_checkpoint_bytes,
            &mut memory,
        )
        .unwrap();
        let old = intent::Anchors {
            repository: source.snapshot().original_root.key.repository,
            instance: [3; 16],
            current: [2; 32].into(),
            branch: [4; 16].into(),
            staged: None,
        };
        let record = Intent {
            transaction: [6; 32],
            original_root: source.snapshot().original_root.key,
            scope: source.snapshot().original_root.key,
            source: source.identity(),
            candidate: CheckpointIdentity::from_bytes(
                &encoded,
                Limits::default().max_checkpoint_bytes,
            )
            .unwrap(),
            old,
            new: intent::Anchors {
                current: [5; 32].into(),
                ..old
            },
        };
        Self {
            recovery,
            source,
            candidate,
            encoded,
            record,
            memory,
            temp,
        }
    }
    fn transition(&self) -> PathBuf {
        self.temp.0.join("recovery/transition")
    }
    fn write(&mut self, before: Option<Step>) -> Result<DurableIntentReceipt> {
        // The private engine exercises filesystem fault boundaries. Production
        // entry accepts only CapturedCheckpointImage's inseparable byte pair.
        self.recovery.publish_transition_inner(
            &self.source,
            &self.candidate,
            &self.encoded,
            &self.record,
            &mut self.memory,
            &Fault { before },
        )
    }
}
#[test]
fn durable_receipt_preserves_exact_images_and_fences_ordinary_publication() {
    let mut fixture = Fixture::new();
    let receipt = fixture.write(None).unwrap();
    assert_eq!(receipt.record(), &fixture.record);
    assert!(!fixture.recovery.uncertain);
    let directory = fixture.transition();
    assert_eq!(
        fs::read(directory.join("source")).unwrap(),
        fixture.source.bytes()
    );
    assert_eq!(
        fs::read(directory.join("candidate")).unwrap(),
        fixture.encoded
    );
    assert!(!directory.join("intent.pending").exists());
    assert_eq!(
        Intent::decode(
            &fs::read(directory.join("intent")).unwrap(),
            Limits::default().max_checkpoint_bytes
        )
        .unwrap(),
        fixture.record
    );
    assert_eq!(
        fs::metadata(&directory).unwrap().permissions().mode() & 0o777,
        0o700
    );
    for name in ["source", "candidate", "intent"] {
        let metadata = fs::metadata(directory.join(name)).unwrap();
        assert_eq!(metadata.permissions().mode() & 0o777, 0o600);
        assert_eq!(metadata.nlink(), 1);
    }
    // Success is known-pending, not a reopened ordinary checkpoint writer.
    assert!(matches!(
        fixture.recovery.publish(&image(3)),
        Err(Error::TransitionPending)
    ));
    assert!(matches!(
        fixture.recovery.original_root(),
        Err(Error::TransitionPending)
    ));
    assert_eq!(
        fixture.recovery.load_exact().unwrap().unwrap().identity(),
        fixture.source.identity()
    );
    assert!(matches!(
        RecoveryInspection::open_existing(&fixture.temp.0.join("recovery"), Limits::default()),
        Err(Error::Busy)
    ));
    drop(fixture.recovery);
    assert!(matches!(
        Recovery::open_existing(fixture.temp.0.join("recovery"), Limits::default()),
        Err(Error::TransitionPending)
    ));
    let inspector =
        RecoveryInspection::open_existing(&fixture.temp.0.join("recovery"), Limits::default())
            .unwrap();
    let facts = inspector
        .load_transition_budgeted(&mut memory())
        .unwrap()
        .unwrap();
    facts.check_image_relation().unwrap();
}
#[test]
fn every_write_sync_and_rename_failure_retains_facts_and_exclusive_owner() {
    for step in [
        Step::ContainerSync,
        Step::SourceWrite,
        Step::SourceSync,
        Step::CandidateWrite,
        Step::CandidateSync,
        Step::ImageDirectorySync,
        Step::IntentWrite,
        Step::IntentSync,
        Step::Rename,
        Step::DirectorySync,
    ] {
        let mut fixture = Fixture::new();
        let committed = fs::read(fixture.temp.0.join("recovery/checkpoint")).unwrap();
        assert!(fixture.write(Some(step)).is_err());
        assert!(fixture.recovery.uncertain);
        assert!(fixture.transition().is_dir());
        assert_eq!(
            fs::read(fixture.temp.0.join("recovery/checkpoint")).unwrap(),
            committed
        );
        assert!(matches!(
            RecoveryInspection::open_existing(&fixture.temp.0.join("recovery"), Limits::default()),
            Err(Error::Busy)
        ));
        assert!(matches!(
            fixture.recovery.publish(&image(3)),
            Err(Error::Uncertain)
        ));
        if step == Step::DirectorySync {
            assert!(fixture.transition().join("intent").is_file());
            assert!(!fixture.transition().join("intent.pending").exists());
        }
    }
}
#[test]
fn failed_preflight_and_creation_do_not_create_or_replace_transition_facts() {
    let mut fixture = Fixture::new();
    assert!(fixture.write(Some(Step::CreateDirectory)).is_err());
    assert!(!fixture.transition().exists());
    assert!(!fixture.recovery.uncertain);
    fixture.record.candidate.sha256[0] ^= 1;
    assert!(matches!(fixture.write(None), Err(Error::Corrupt)));
    assert!(!fixture.transition().exists());
    fixture.record.candidate.sha256[0] ^= 1;
    fs::create_dir(fixture.transition()).unwrap();
    fs::set_permissions(fixture.transition(), fs::Permissions::from_mode(0o700)).unwrap();
    fs::write(
        fixture.transition().join("unknown"),
        b"preserve existing facts",
    )
    .unwrap();
    assert!(matches!(fixture.write(None), Err(Error::TransitionPending)));
    assert_eq!(
        fs::read(fixture.transition().join("unknown")).unwrap(),
        b"preserve existing facts"
    );
}
#[test]
fn stale_acknowledgment_and_exhausted_budget_fail_before_artifact_creation() {
    let mut fixture = Fixture::new();
    fixture.recovery.publish(&image(3)).unwrap();
    assert!(matches!(fixture.write(None), Err(Error::StaleSnapshot)));
    assert!(!fixture.transition().exists());
    let mut fixture = Fixture::new();
    let remaining = fixture.memory.remaining_buffer_bytes();
    fixture.memory.buffer(remaining).unwrap();
    assert!(matches!(fixture.write(None), Err(Error::ResourceLimit)));
    assert!(!fixture.transition().exists());
}

// Shared crash fixture uses only this test-only filesystem engine. Production
// publication still requires the opaque captured image pair.
pub(crate) fn publish_matching_fixture(
    recovery: &mut Recovery,
    source: &PublishedCheckpointImage,
    candidate: &NamespaceSnapshot,
    bytes: &[u8],
    record: &Intent,
    memory: &mut CaptureMemory,
) {
    recovery
        .publish_transition_inner(
            source,
            candidate,
            bytes,
            record,
            memory,
            &Fault { before: None },
        )
        .unwrap();
}
