// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

use std::os::unix::fs::OpenOptionsExt;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::fs::symlink;
use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use super::*;
use crate::capture_memory::CaptureLimits;
use crate::recovery::Limits;
use crate::recovery::Recovery;

static NEXT: AtomicU64 = AtomicU64::new(0);
struct Temp(PathBuf);
impl Temp {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "lore-transition-facts-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn inspector(&self) -> RecoveryInspection {
        drop(Recovery::open(self.0.join("recovery"), Limits::default()).unwrap());
        RecoveryInspection::open_existing(&self.0.join("recovery"), Limits::default()).unwrap()
    }
    fn transition(&self) -> PathBuf {
        let path = self.0.join("recovery/transition");
        std::fs::create_dir(&path).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700)).unwrap();
        path
    }
}
impl Drop for Temp {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
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
fn write(path: &Path, bytes: &[u8]) {
    use std::io::Write;
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
        .unwrap();
    file.write_all(bytes).unwrap();
}
fn intent_bytes() -> [u8; intent::ENCODED_SIZE] {
    let root = crate::core::NodeKey {
        repository: [1; 16].into(),
        revision: lore_base::types::Address {
            context: [2; 16].into(),
            hash: [3; 32].into(),
        },
        node: 0,
        exposure: 1,
    };
    let old = intent::Anchors {
        repository: root.repository,
        instance: [4; 16],
        current: [5; 32].into(),
        branch: [6; 16].into(),
        staged: None,
    };
    let new = intent::Anchors {
        current: [7; 32].into(),
        ..old
    };
    intent::Intent {
        transaction: [8; 32],
        original_root: root,
        scope: root,
        source: intent::CheckpointIdentity::from_bytes(b"source", 1024).unwrap(),
        candidate: intent::CheckpointIdentity::from_bytes(b"candidate", 1024).unwrap(),
        old,
        new,
    }
    .encode(Limits::default().max_checkpoint_bytes)
    .unwrap()
}
#[test]
fn missing_empty_and_repeated_inventory_preserve_disk() {
    let temp = Temp::new();
    let inspector = temp.inspector();
    assert!(
        inspector
            .load_transition_budgeted(&mut memory())
            .unwrap()
            .is_none()
    );
    let path = temp.transition();
    for _ in 0..3 {
        let facts = inspector
            .load_transition_budgeted(&mut memory())
            .unwrap()
            .unwrap();
        assert_eq!(facts.inventory().observed_known(), 0);
        assert!(!facts.inventory().unknown_or_duplicate());
        assert!(!facts.inventory().excess());
        assert!(matches!(facts.source(), Artifact::Missing));
        assert!(matches!(facts.intent(), Artifact::Missing));
    }
    assert_eq!(std::fs::read_dir(path).unwrap().count(), 0);
}
#[test]
fn partial_corrupt_and_exact_intent_facts_are_independent() {
    let temp = Temp::new();
    let inspector = temp.inspector();
    let path = temp.transition();
    let bytes = intent_bytes();
    write(&path.join("intent"), &bytes);
    write(&path.join("source"), b"broken");
    write(&path.join("intent.pending"), b"partial");
    let facts = inspector
        .load_transition_budgeted(&mut memory())
        .unwrap()
        .unwrap();
    assert_eq!(facts.inventory().observed_known(), 1 | 4 | 8);
    assert!(matches!(facts.source(), Artifact::Invalid(_)));
    assert!(matches!(facts.candidate(), Artifact::Missing));
    assert!(matches!(
        facts.pending_intent(),
        Artifact::Invalid(InvalidArtifact::Corrupt)
    ));
    let Artifact::Checked(intent) = facts.intent() else {
        panic!("exact intent was not checked")
    };
    assert_eq!(intent.bytes(), &bytes);
    assert_eq!(
        *intent.parsed(),
        intent::Intent::decode(&bytes, Limits::default().max_checkpoint_bytes).unwrap()
    );
    assert_eq!(std::fs::read(path.join("source")).unwrap(), b"broken");
    assert_eq!(
        std::fs::read(path.join("intent.pending")).unwrap(),
        b"partial"
    );
}
#[test]
fn unsafe_fixed_files_never_follow_or_block_and_other_facts_survive() {
    for kind in 0..4 {
        let temp = Temp::new();
        let inspector = temp.inspector();
        let path = temp.transition();
        match kind {
            0 => symlink("missing", path.join("source")).unwrap(),
            1 => {
                use std::os::unix::ffi::OsStrExt;
                let name =
                    std::ffi::CString::new(path.join("source").as_os_str().as_bytes()).unwrap();
                // SAFETY: the owned pathname lives through mkfifo.
                assert_eq!(unsafe { libc::mkfifo(name.as_ptr(), 0o600) }, 0);
            }
            2 => std::fs::create_dir(path.join("source")).unwrap(),
            _ => {
                write(&path.join("source"), b"unsafe");
                std::fs::set_permissions(
                    path.join("source"),
                    std::fs::Permissions::from_mode(0o644),
                )
                .unwrap();
            }
        }
        write(&path.join("intent"), &intent_bytes());
        let facts = inspector
            .load_transition_budgeted(&mut memory())
            .unwrap()
            .unwrap();
        assert!(matches!(facts.source(), Artifact::Invalid(_)));
        assert!(matches!(facts.intent(), Artifact::Checked(_)));
        assert!(std::fs::symlink_metadata(path.join("source")).is_ok());
    }
}
#[test]
fn unsafe_container_is_not_absence_and_unknown_inventory_is_observed_only() {
    let temp = Temp::new();
    let inspector = temp.inspector();
    symlink("missing", temp.0.join("recovery/transition")).unwrap();
    assert!(inspector.load_transition_budgeted(&mut memory()).is_err());
    std::fs::remove_file(temp.0.join("recovery/transition")).unwrap();
    let path = temp.transition();
    for index in 0..8 {
        write(&path.join(format!("unknown{index}")), b"retained");
    }
    let facts = inspector
        .load_transition_budgeted(&mut memory())
        .unwrap()
        .unwrap();
    assert!(facts.inventory().unknown_or_duplicate());
    assert!(facts.inventory().excess());
    assert_eq!(std::fs::read_dir(path).unwrap().count(), 8);
}
#[test]
fn aggregate_limits_abort_instead_of_becoming_invalid_facts() {
    let temp = Temp::new();
    let inspector = temp.inspector();
    let path = temp.transition();
    write(&path.join("intent"), &intent_bytes());
    let mut fixed_slots = CaptureMemory::new(CaptureLimits {
        max_buffer_bytes: 1,
        max_metadata_nodes: 1,
        max_name_bytes: 1,
    })
    .unwrap();
    assert!(matches!(
        inspector.load_transition_budgeted(&mut fixed_slots),
        Err(Error::ResourceLimit)
    ));
    let mut memory = CaptureMemory::new(CaptureLimits {
        max_buffer_bytes: intent::ENCODED_SIZE,
        max_metadata_nodes: 100,
        max_name_bytes: 1,
    })
    .unwrap();
    memory.buffer(1).unwrap();
    assert!(matches!(
        inspector.load_transition_budgeted(&mut memory),
        Err(Error::ResourceLimit)
    ));
    assert_eq!(std::fs::read(path.join("intent")).unwrap(), intent_bytes());
}

fn checkpoint_bytes() -> Vec<u8> {
    use std::collections::BTreeMap;

    use crate::core::Node;
    use crate::core::NodeKey;
    use crate::core::NodeKind;
    use crate::namespace::Entry;
    use crate::namespace::NamespaceSnapshot;
    use crate::namespace::SnapshotNode;
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
    let snapshot = NamespaceSnapshot {
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
    };
    crate::recovery::codec::encode(&snapshot, Limits::default().max_checkpoint_bytes).unwrap()
}
#[test]
fn images_and_both_intents_retain_exact_bytes_on_one_aggregate_ledger() {
    let temp = Temp::new();
    let inspector = temp.inspector();
    let path = temp.transition();
    let image = checkpoint_bytes();
    let intent = intent_bytes();
    write(&path.join("source"), &image);
    write(&path.join("candidate"), &image);
    write(&path.join("intent.pending"), &intent);
    write(&path.join("intent"), &intent);
    let mut ledger = memory();
    let facts = inspector
        .load_transition_budgeted(&mut ledger)
        .unwrap()
        .unwrap();
    assert_eq!(facts.inventory().observed_known(), 15);
    assert!(!facts.inventory().excess());
    assert!(!facts.inventory().unknown_or_duplicate());
    for artifact in [facts.source(), facts.candidate()] {
        let Artifact::Checked(checked) = artifact else {
            panic!("image not checked")
        };
        assert_eq!(checked.bytes(), image);
        assert_eq!(
            checked.identity(),
            intent::CheckpointIdentity::from_bytes(&image, Limits::default().max_checkpoint_bytes)
                .unwrap()
        );
    }
    assert!(matches!(facts.pending_intent(), Artifact::Checked(_)));
    assert!(matches!(facts.intent(), Artifact::Checked(_)));
    // Exactly enough raw capacity for both images and one intent is still less
    // than their decoded/validator scratch and the second retained intent.
    let mut limited = CaptureMemory::new(CaptureLimits {
        max_buffer_bytes: 2 * image.len() + intent.len(),
        max_metadata_nodes: 100_000,
        max_name_bytes: 1024 * 1024,
    })
    .unwrap();
    assert!(matches!(
        inspector.load_transition_budgeted(&mut limited),
        Err(Error::ResourceLimit)
    ));
    for name in ["source", "candidate"] {
        assert_eq!(std::fs::read(path.join(name)).unwrap(), image);
    }
    for name in ["intent", "intent.pending"] {
        assert_eq!(std::fs::read(path.join(name)).unwrap(), intent);
    }
}
#[test]
fn malformed_intent_keeps_syntax_distinct_and_other_intent_independent() {
    let temp = Temp::new();
    let inspector = temp.inspector();
    let path = temp.transition();
    let mut bytes = intent_bytes();
    bytes[8] ^= 1;
    write(&path.join("intent"), &bytes);
    write(&path.join("intent.pending"), &intent_bytes());
    let facts = inspector
        .load_transition_budgeted(&mut memory())
        .unwrap()
        .unwrap();
    assert!(matches!(
        facts.intent(),
        Artifact::Invalid(InvalidArtifact::Intent(intent::Error::Corrupt))
    ));
    assert!(matches!(facts.pending_intent(), Artifact::Checked(_)));
    assert_eq!(std::fs::read(path.join("intent")).unwrap(), bytes);
}
