// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
//! GCS/Firestore-backed store implementations, mirroring `lore_aws::store`.
//!
//! - [`immutable_store`] — GCS for payloads, Firestore for fragment existence/dedup tracking.
//! - [`lock_store`] — Firestore for file locks shared by every server using the collection.
//! - [`mutable_store`] — Firestore for the mutable branch-pointer store.
//! - [`object_metadata`] — how a [`lore_base::types::Fragment`] is encoded onto a GCS object's
//!   custom metadata, written atomically with the body.

pub mod immutable_store;
pub mod lock_store;
pub mod mutable_store;
pub mod object_metadata;
