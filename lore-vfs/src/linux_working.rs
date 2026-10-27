// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Experimental mutable mount over one explicitly authorized namespace.
//!
//! File handles retain their overlay after rename/unlink. The common model is
//! portable; the Linux transport uses direct I/O, zero cache TTL and no mmap
//! capability. Fsync/fsyncdir and explicitly bound provider commit handoffs
//! acknowledge durable full checkpoints.
//! This module does not select a production write policy or change any default.

mod model;
pub use model::Attributes;
pub(crate) use model::CapturedCheckpointImage;
pub use model::DirectoryPage;
pub use model::Error;
pub use model::Limits;
pub use model::Model;
pub use model::OpenFlags;
pub(crate) use model::OwnedSourcePreparation;
pub use model::Result;
pub(crate) use model::SourceContinuation;
pub(crate) use model::SourcePreparationError;
pub use model::StoreFactory;

#[cfg(target_os = "linux")]
mod transport;
#[cfg(target_os = "linux")]
pub use transport::Mounted;
#[cfg(target_os = "linux")]
pub use transport::mount;
