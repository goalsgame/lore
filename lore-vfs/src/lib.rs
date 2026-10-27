// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Platform-independent primitives for Lore filesystem adapters.
//!
//! The experimental read-only core and Linux adapter expose a pinned revision.
//! An optional removable digest index supplies verified whole-file SHA-256 records.
//! Mutable namespaces, checkpoints and an injected stage/commit provider are
//! available independently; mounted writes and service ownership remain experimental work.

mod capture_memory;
pub mod checkpoint_owner;
pub mod core;
pub mod digest;
pub mod digest_index;
pub mod linux_working;
pub mod namespace;
pub mod overlay;
pub mod provider;
mod publication_ownership;
pub mod recovery;
pub mod revision;
pub mod storage_budget;
pub mod transition_prepare;
pub mod working_storage;

#[cfg(feature = "test-util")]
pub mod test_util;

#[cfg(target_os = "linux")]
pub mod linux;
