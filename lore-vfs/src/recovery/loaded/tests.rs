// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

use std::collections::BTreeMap;
use std::io::Cursor;
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;

use super::*;
use crate::core::Node;
use crate::core::NodeKey;
use crate::core::NodeKind;
use crate::namespace::Entry;
use crate::namespace::SnapshotNode;
use crate::overlay::FileSnapshot;
use crate::overlay::Metadata as FileMetadata;
use crate::recovery::Limits;
use crate::recovery::Recovery;

static NEXT: AtomicU64 = AtomicU64::new(0);
struct Temp(PathBuf);
impl Temp {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "lore-loaded-checkpoint-{}-{}",
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
    };
    NamespaceSnapshot {
        original_root: root.clone(),
        active_underlays: BTreeMap::new(),
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
        generation: 4,
        files: BTreeMap::new(),
        baselines: BTreeMap::new(),
    }
}
#[test]
fn bounded_reader_rejects_short_growth_and_overflow_without_growing_its_capacity() {
    for length in [0, 1, 255, 256, 257, 4095, 4096, 4097, 65537] {
        let input = vec![0xa5; length];
        let bytes = read_bounded(&mut Cursor::new(&input), length as u64, length).unwrap();
        assert_eq!(bytes, input);
        assert!(bytes.capacity() <= length);
    }
    let small = read_bounded(&mut Cursor::new(b"tiny"), 4, usize::MAX).unwrap();
    assert_eq!(small.capacity(), 4);
    assert!(matches!(
        read_bounded(&mut Cursor::new(b"abc"), 4, 8),
        Err(Error::Corrupt)
    ));
    assert!(matches!(
        read_bounded(&mut Cursor::new(b"abcde"), 4, 8),
        Err(Error::Corrupt)
    ));
    assert!(matches!(
        read_bounded(&mut Cursor::new(b"abcde"), 4, 4),
        Err(Error::ResourceLimit)
    ));
    assert!(matches!(
        read_bounded(&mut Cursor::new(b"x"), 0, 8),
        Err(Error::Corrupt)
    ));
    struct NeverRead;
    impl Read for NeverRead {
        fn read(&mut self, _: &mut [u8]) -> std::io::Result<usize> {
            panic!("invalid length reached IO")
        }
    }
    assert!(matches!(
        read_bounded(&mut NeverRead, u64::MAX, 1024),
        Err(Error::ResourceLimit)
    ));
    assert!(matches!(
        read_bounded(&mut NeverRead, 9, 8),
        Err(Error::ResourceLimit)
    ));
}
#[test]
fn bounded_reader_retries_interrupted_reads_and_preserves_other_io_errors() {
    struct Interrupted {
        reader: Cursor<&'static [u8]>,
        interrupt: bool,
    }
    impl Read for Interrupted {
        fn read(&mut self, bytes: &mut [u8]) -> std::io::Result<usize> {
            self.interrupt = !self.interrupt;
            if self.interrupt {
                Err(ErrorKind::Interrupted.into())
            } else {
                self.reader.read(bytes)
            }
        }
    }
    let mut reader = Interrupted {
        reader: Cursor::new(b"data"),
        interrupt: false,
    };
    assert_eq!(read_bounded(&mut reader, 4, 4).unwrap(), b"data");
    struct Failed;
    impl Read for Failed {
        fn read(&mut self, _: &mut [u8]) -> std::io::Result<usize> {
            Err(ErrorKind::PermissionDenied.into())
        }
    }
    assert!(
        matches!(read_bounded(&mut Failed, 4, 4), Err(Error::Io(error)) if error.kind() == ErrorKind::PermissionDenied)
    );
}
#[test]
fn recovery_retains_exact_image_identity_and_public_load_pending_semantics() {
    let temp = Temp::new();
    let directory = temp.0.join("owner");
    let mut recovery = Recovery::open(&directory, Limits::default()).unwrap();
    assert!(recovery.load_exact().unwrap().is_none());
    assert!(recovery.load().unwrap().is_none());
    let snapshot = image();
    recovery.publish(&snapshot).unwrap();
    let original = std::fs::read(directory.join("checkpoint")).unwrap();
    std::fs::write(directory.join("pending"), b"retained unacknowledged bytes").unwrap();
    let loaded = recovery.load_exact().unwrap().unwrap();
    assert_eq!(loaded.bytes(), original);
    assert_eq!(loaded.encoded_capacity(), original.len());
    assert_eq!(
        loaded.identity(),
        CheckpointIdentity::from_bytes(&original, original.len()).unwrap()
    );
    assert_ne!(
        loaded.identity().sha256.as_slice(),
        &original[original.len() - 32..]
    );
    assert_eq!(loaded.snapshot().original_root, snapshot.original_root);
    assert_eq!(loaded.snapshot().generation, snapshot.generation);
    assert_eq!(loaded.into_snapshot().generation, snapshot.generation);
    assert_eq!(
        recovery.load().unwrap().unwrap().generation,
        snapshot.generation
    );
    assert!(recovery.has_pending().unwrap());
    assert!(matches!(recovery.original_root(), Err(Error::Pending)));
    assert_eq!(
        std::fs::read(directory.join("pending")).unwrap(),
        b"retained unacknowledged bytes"
    );
}
#[test]
fn corrupt_truncated_oversized_and_wrong_version_images_are_retained_and_rejected() {
    let temp = Temp::new();
    let directory = temp.0.join("owner");
    let bytes = codec::encode(&image(), 4096).unwrap();
    let mut recovery = Recovery::open(
        &directory,
        Limits {
            max_checkpoint_bytes: bytes.len(),
        },
    )
    .unwrap();
    recovery.publish(&image()).unwrap();
    let checkpoint = directory.join("checkpoint");
    for end in [0, 1, bytes.len() - 1] {
        std::fs::write(&checkpoint, &bytes[..end]).unwrap();
        assert!(matches!(recovery.load_exact(), Err(Error::Corrupt)));
        assert_eq!(std::fs::read(&checkpoint).unwrap(), &bytes[..end]);
    }
    let mut corrupt = bytes.clone();
    corrupt[20] ^= 1;
    std::fs::write(&checkpoint, &corrupt).unwrap();
    assert!(matches!(recovery.load_exact(), Err(Error::Corrupt)));
    corrupt = bytes.clone();
    corrupt[7] ^= 1;
    std::fs::write(&checkpoint, &corrupt).unwrap();
    assert!(matches!(recovery.load_exact(), Err(Error::Incompatible)));
    corrupt = bytes;
    corrupt.push(0);
    std::fs::write(&checkpoint, &corrupt).unwrap();
    assert!(matches!(recovery.load_exact(), Err(Error::ResourceLimit)));
    assert_eq!(std::fs::read(checkpoint).unwrap(), corrupt);
}
#[test]
fn original_wire_order_is_retained_even_when_decoded_maps_reencode_differently() {
    let temp = Temp::new();
    let directory = temp.0.join("owner");
    let mut recovery = Recovery::open(&directory, Limits::default()).unwrap();
    let mut snapshot = image();
    for (inode, name) in [(2, b"first".to_vec()), (3, b"second".to_vec())] {
        let node = Node {
            key: NodeKey {
                node: inode as u32 + 6,
                ..snapshot.original_root.key
            },
            kind: NodeKind::File,
            size: 4,
            mode: 0o444,
        };
        snapshot.nodes[0]
            .names
            .as_mut()
            .unwrap()
            .push((name, inode));
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
                node: node.clone(),
                metadata: FileMetadata {
                    size: 4,
                    mode: node.mode,
                    generation: 0,
                    writers: 0,
                    mutable: false,
                    reserved_storage_bytes: 0,
                },
                base_end: 4,
                extents: Vec::new(),
            },
        );
    }
    snapshot.next_inode = 4;
    recovery.publish(&snapshot).unwrap();
    let checkpoint = directory.join("checkpoint");
    let canonical = std::fs::read(&checkpoint).unwrap();
    // v5: fixed original node, then four empty/count headers before file records.
    const KEY_BYTES: usize = 16 + 16 + 32 + 4 + 8;
    const NODE_BYTES: usize = KEY_BYTES + 1 + 8 + 2;
    const FILE_BYTES: usize = 8 + 8 + NODE_BYTES + 8 + 2 + 8 + 1 + 8 + 8 + 1;
    let start = 8 + 8 + NODE_BYTES + 4 * 8;
    let mut original = canonical.clone();
    original[start..start + FILE_BYTES]
        .copy_from_slice(&canonical[start + FILE_BYTES..start + 2 * FILE_BYTES]);
    original[start + FILE_BYTES..start + 2 * FILE_BYTES]
        .copy_from_slice(&canonical[start..start + FILE_BYTES]);
    let checksum = original.len() - 32;
    let hash = ring::digest::digest(&ring::digest::SHA256, &original[..checksum]);
    original[checksum..].copy_from_slice(hash.as_ref());
    std::fs::write(checkpoint, &original).unwrap();
    let loaded = recovery.load_exact().unwrap().unwrap();
    assert_eq!(loaded.bytes(), original);
    let budgeted = recovery
        .load_exact_budgeted(&mut budgeted::ample())
        .unwrap()
        .unwrap();
    assert_eq!(budgeted.bytes(), original);
    assert_eq!(budgeted.identity(), loaded.identity());
    assert_eq!(
        codec::encode(loaded.snapshot(), canonical.len()).unwrap(),
        canonical
    );
    assert_ne!(
        loaded.identity(),
        CheckpointIdentity::from_bytes(&canonical, canonical.len()).unwrap()
    );
}
#[test]
fn consistency_stamp_detects_changed_length_and_modification() {
    let temp = Temp::new();
    let path = temp.0.join("stamp");
    std::fs::write(&path, b"data").unwrap();
    let file = File::options().read(true).write(true).open(path).unwrap();
    let before = Stamp::of(&file.metadata().unwrap());
    file.set_modified(before.modified.unwrap() + Duration::from_secs(2))
        .unwrap();
    assert!(before != Stamp::of(&file.metadata().unwrap()));
    let before = Stamp::of(&file.metadata().unwrap());
    file.set_len(2).unwrap();
    assert!(before != Stamp::of(&file.metadata().unwrap()));
}

#[path = "tests/budgeted.rs"]
mod budgeted;
