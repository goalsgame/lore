// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
//! Crate-local error type wrapping the two backends this crate talks to, mirroring
//! `lore_aws::aws_error::AwsError`.

use std::fmt::Debug;

use thiserror::Error;

/// Errors produced by this crate's GCS and Firestore clients.
///
/// Both underlying SDK error types are boxed for the reason
/// [`lore_aws::aws_error::AwsError`] boxes its own: they carry a raw HTTP/gRPC response and run
/// to several hundred bytes, a cost every `Result<_, GcpError>` would otherwise pay on the
/// success path too.
#[derive(Debug, Error)]
pub enum GcpError {
    /// A failure returned by the `google-cloud-storage` client (covers both the data-plane
    /// `Storage` client and the `StorageControl` metadata client).
    #[error("GCS operation failed: {0:?}")]
    Gcs(Box<google_cloud_storage::Error>),
    /// A failure returned by the `firestore` client.
    #[error("Firestore operation failed: {0:?}")]
    Firestore(Box<firestore::errors::FirestoreError>),
    /// A Firestore document existed but could not be decoded into the shape this crate expects.
    #[error("Firestore document decode failed: {0}")]
    Decode(String),
    /// A GCS/Firestore client could not be constructed (credentials, transport init, etc). A
    /// distinct variant from [`GcpError::Gcs`] because client construction uses its own error
    /// type (`google_cloud_gax::client_builder::Error`), separate from the one runtime RPCs
    /// return.
    #[error("GCP client construction failed: {0:?}")]
    ClientBuild(Box<google_cloud_gax::client_builder::Error>),
}

impl GcpError {
    pub fn gcs(error: google_cloud_storage::Error) -> Self {
        Self::Gcs(Box::new(error))
    }

    pub fn firestore(error: firestore::errors::FirestoreError) -> Self {
        Self::Firestore(Box::new(error))
    }

    pub fn client_build(error: google_cloud_gax::client_builder::Error) -> Self {
        Self::ClientBuild(Box::new(error))
    }
}

impl From<google_cloud_storage::Error> for GcpError {
    fn from(error: google_cloud_storage::Error) -> Self {
        Self::gcs(error)
    }
}

impl From<firestore::errors::FirestoreError> for GcpError {
    fn from(error: firestore::errors::FirestoreError) -> Self {
        Self::firestore(error)
    }
}
