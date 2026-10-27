// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

use super::*;

const LIMIT: usize = 1024;
fn fixture() -> (Intent, Snapshot) {
    let old = Snapshot {
        repository: [1; 16].into(),
        instance: lore_revision::instance::InstanceId::generate(),
        current: [2; 32].into(),
        branch: [3; 16].into(),
        staged: Some([4; 32].into()),
    };
    let original_root = NodeKey {
        repository: [8; 16].into(),
        revision: lore_base::types::Address {
            context: [9; 16].into(),
            hash: [10; 32].into(),
        },
        node: 0,
        exposure: 0,
    };
    let scope = NodeKey {
        repository: old.repository,
        exposure: 71,
        ..original_root
    };
    let mut new = Anchors::from(old);
    new.current = [5; 32].into();
    new.staged = None;
    (
        Intent {
            transaction: [7; 32],
            original_root,
            scope,
            source: CheckpointIdentity::from_bytes(b"source", LIMIT).unwrap(),
            candidate: CheckpointIdentity::from_bytes(b"candidate", LIMIT).unwrap(),
            old: old.into(),
            new,
        },
        old,
    )
}
fn resign(bytes: &mut [u8; ENCODED_SIZE]) {
    let hash = digest(&SHA256, &bytes[..PAYLOAD_SIZE]);
    bytes[PAYLOAD_SIZE..].copy_from_slice(hash.as_ref());
}
#[test]
fn linked_scope_and_full_keys_round_trip_without_image_claims() {
    let (intent, _) = fixture();
    assert_ne!(intent.scope.repository, intent.original_root.repository);
    let bytes = intent.encode(LIMIT).unwrap();
    assert_eq!(Intent::decode(&bytes, LIMIT), Ok(intent));
    // These deliberately are not v5 images: identity hashing is not decoding.
    assert_eq!(intent.source.length, 6);
    assert_ne!(
        intent.source,
        CheckpointIdentity::from_bytes(b"sourcf", LIMIT).unwrap()
    );
    assert_eq!(
        CheckpointIdentity::from_bytes(b"", LIMIT),
        Err(Error::ResourceLimit)
    );
    assert_eq!(
        CheckpointIdentity::from_bytes(b"source", 5),
        Err(Error::ResourceLimit)
    );
    assert!(CheckpointIdentity::from_bytes(b"source", 6).is_ok());
}
#[test]
fn every_truncation_corruption_version_and_trailing_byte_refuses() {
    let (intent, _) = fixture();
    let bytes = intent.encode(LIMIT).unwrap();
    for end in 0..bytes.len() {
        assert!(Intent::decode(&bytes[..end], LIMIT).is_err());
    }
    for index in 0..bytes.len() {
        let mut corrupt = bytes;
        corrupt[index] ^= 1;
        assert!(Intent::decode(&corrupt, LIMIT).is_err());
    }
    let mut trailing = bytes.to_vec();
    trailing.push(0);
    assert_eq!(Intent::decode(&trailing, LIMIT), Err(Error::Corrupt));
    let mut version = bytes;
    version[7] = b'2';
    resign(&mut version);
    assert_eq!(Intent::decode(&version, LIMIT), Err(Error::Incompatible));
}
#[test]
fn rechecks_identity_and_bounds_after_checksum() {
    let (intent, _) = fixture();
    let mut invalid = intent;
    invalid.transaction = [0; 32];
    assert_eq!(invalid.encode(LIMIT), Err(Error::InvalidIdentity));
    for field in 0..6 {
        let mut invalid = intent;
        match field {
            0 => invalid.scope.repository = Partition::default(),
            1 => invalid.original_root.repository = Partition::default(),
            2 => invalid.old.instance = [0; 16],
            3 => invalid.new.repository = [12; 16].into(),
            4 => invalid.old.branch = Context::default(),
            _ => invalid.new.staged = Some(Hash::default()),
        }
        assert_eq!(invalid.encode(LIMIT), Err(Error::InvalidIdentity));
    }
    let bytes = intent.encode(LIMIT).unwrap();
    assert_eq!(Intent::decode(&bytes, 5), Err(Error::ResourceLimit));
    let mut invalid = bytes;
    invalid[8..8 + HASH_BYTES].fill(0);
    resign(&mut invalid);
    assert_eq!(Intent::decode(&invalid, LIMIT), Err(Error::InvalidIdentity));
    let mut invalid = intent;
    invalid.source.length = 0;
    assert_eq!(invalid.encode(LIMIT), Err(Error::ResourceLimit));
    invalid.source.length = u64::MAX;
    assert_eq!(invalid.encode(LIMIT), Err(Error::ResourceLimit));
}
#[test]
fn optional_staged_encoding_is_canonical() {
    let (intent, _) = fixture();
    let bytes = intent.encode(LIMIT).unwrap();
    let old_start = MAGIC.len() + HASH_BYTES + 2 * NODE_KEY_BYTES + 2 * CHECKPOINT_ID_BYTES;
    let flag = old_start + ID_BYTES + ID_BYTES + HASH_BYTES + ID_BYTES;
    for value in [0, 2] {
        let mut invalid = bytes;
        invalid[flag] = value;
        resign(&mut invalid);
        assert_eq!(Intent::decode(&invalid, LIMIT), Err(Error::Corrupt));
    }
    let mut invalid = bytes;
    invalid[flag + 1..flag + 1 + HASH_BYTES].fill(0);
    resign(&mut invalid);
    assert_eq!(Intent::decode(&invalid, LIMIT), Err(Error::Corrupt));
}
#[test]
fn exact_anchor_tuples_select_and_all_mixed_or_foreign_tuples_remain_unresolved() {
    let (intent, old) = fixture();
    assert_eq!(intent.classify(old, LIMIT), Ok(Selection::Source));
    let new = Snapshot {
        current: intent.new.current,
        staged: intent.new.staged,
        ..old
    };
    assert_eq!(intent.classify(new, LIMIT), Ok(Selection::Candidate));
    let changed = [
        Snapshot {
            current: new.current,
            ..old
        },
        Snapshot {
            staged: new.staged,
            ..old
        },
        Snapshot {
            repository: [31; 16].into(),
            ..old
        },
        Snapshot {
            instance: lore_revision::instance::InstanceId::generate(),
            ..old
        },
        Snapshot {
            branch: [32; 16].into(),
            ..old
        },
        Snapshot {
            current: Hash::default(),
            ..old
        },
    ];
    for observed in changed {
        assert_eq!(intent.classify(observed, LIMIT), Ok(Selection::Unresolved));
    }
    assert_eq!(
        intent.classify(
            Snapshot {
                branch: Context::default(),
                ..old
            },
            LIMIT
        ),
        Err(Error::InvalidIdentity)
    );
    let mut no_op = intent;
    no_op.new = no_op.old;
    assert_eq!(no_op.encode(LIMIT), Err(Error::AmbiguousAnchors));
    no_op.candidate = no_op.source;
    assert_eq!(no_op.encode(LIMIT), Err(Error::AmbiguousAnchors));
}
