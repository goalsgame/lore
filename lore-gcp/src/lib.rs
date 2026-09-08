// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
//! GCP-native storage backend for `lore-server`: Google Cloud Storage (GCS) for the immutable,
//! content-addressed fragment store, and Firestore (native mode) for the mutable branch-pointer
//! store and for fragment existence/dedup tracking.
//!
//! This crate mirrors the structure of `lore-aws`: a thin client-construction layer
//! ([`clients`]), a crate-local error type ([`gcp_error`]), and the store implementations
//! themselves ([`store`]). Distributed locking is explicitly out of scope here — see
//! `lore-server/src/plugins/gcp.rs` for how this crate is wired in, and the workspace root's
//! deployment docs for why `lock_store.mode = "local"` is used instead.
//!
//! # Credentials
//!
//! Both the `google-cloud-storage` and `firestore` crates use Application Default Credentials
//! (ADC) out of the box. There is no manual credential-file plumbing here, unlike `lore-aws`'s
//! AWS default-credential-chain handling.

pub mod clients;
pub mod gcp_error;
pub mod store;
