// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

use super::*;

#[test]
fn checkpoint_encoding_bounds_allocated_capacity_at_exact_wire_limit() {
    // Exercise sizes around allocator growth boundaries with borrowed payloads.
    // The codec treats backend provenance as opaque; replay validates its schema.
    for payload_size in [0, 1, 255, 256, 257, 4095, 4096, 4097, 65537] {
        let mut snapshot = image(7);
        snapshot.backend_state = vec![0xa5; payload_size];
        let generous = codec::encode(&snapshot, 128 * 1024).unwrap();
        let exact = codec::encode(&snapshot, generous.len()).unwrap();
        assert_eq!(exact, generous);
        assert!(exact.capacity() <= exact.len());
        assert!(matches!(
            codec::encode(&snapshot, exact.len() - 1),
            Err(Error::ResourceLimit)
        ));
        let restored = codec::decode(&exact).unwrap();
        assert_eq!(restored.backend_state, snapshot.backend_state);
        assert_eq!(restored.generation, 7);
        assert_eq!(codec::encode(&restored, exact.len()).unwrap(), exact);
    }
}

#[test]
fn checkpoint_encoding_rejects_header_and_payload_limits_without_growth() {
    let snapshot = image(1);
    let exact = codec::encode(&snapshot, 4096).unwrap();
    for limit in 0..exact.len() {
        assert!(matches!(
            codec::encode(&snapshot, limit),
            Err(Error::ResourceLimit)
        ));
    }
    // A large configured ceiling must not cause allocation of that ceiling.
    let bounded = codec::encode(&snapshot, usize::MAX).unwrap();
    assert_eq!(bounded, exact);
    assert_eq!(bounded.capacity(), exact.len());
}
