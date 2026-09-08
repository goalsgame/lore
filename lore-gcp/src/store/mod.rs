// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
//! GCS/Firestore-backed store implementations, mirroring `lore_aws::store`.
//!
//! - [`immutable_store`] — GCS for payloads, Firestore for fragment existence/dedup tracking.
//! - [`mutable_store`] — Firestore for the mutable branch-pointer store.
//! - [`object_metadata`] — how a [`lore_base::types::Fragment`] is encoded onto a GCS object's
//!   custom metadata, written atomically with the body.
//!
//! There is no `lock_store` module here: a Firestore-backed lock store is explicitly out of
//! scope for this crate. `lock_store.mode = "local"` is the only supported option for a GCP
//! deployment today.

pub mod immutable_store;
pub mod mutable_store;
pub mod object_metadata;
