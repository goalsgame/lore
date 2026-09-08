// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
//! Thin client construction for GCS and Firestore, mirroring the spirit of
//! `lore_aws::clients` — but without the AWS crate's typestate builder, because neither
//! `google-cloud-storage` nor `firestore` needs one: both build a usable client from just a
//! project id (and, for GCS, nothing at all) using Application Default Credentials.
//!
//! # Why no `ensure_table`-equivalent for Firestore
//!
//! `lore_aws::clients::AwsClientBuilder::ensure_table` fails fast at startup if a configured
//! `DynamoDB` table does not exist, because `DynamoDB` tables are schema-bearing resources that
//! must be created (with their key schema and indexes) before they can be written to.
//!
//! Firestore collections carry no schema and are not resources in their own right: a collection
//! and every document in it come into existence on the first write, and there is no "describe
//! collection" API to probe. So there is nothing analogous to check here — a Firestore
//! "collection does not exist yet" is indistinguishable from, and exactly as harmless as, "no
//! documents have been written to it yet". This is a real difference from `DynamoDB`, not an
//! oversight: see the crate-level docs for the Firestore composite index this design *does*
//! require the infra team to have deployed (`firestore.indexes.json`), which cannot be checked
//! for cheaply at startup either.
//!
//! [`ensure_bucket_exists`] is the one check this module keeps, because a GCS bucket is a real
//! provisioned resource the same way an S3 bucket is.

use std::future::Future;
use std::time::Duration;

use firestore::FirestoreDb;
use firestore::FirestoreDbOptions;
use google_cloud_storage::client::Storage;
use google_cloud_storage::client::StorageControl;

use crate::gcp_error::GcpError;

/// The bucket resource name GCS's control-plane and data-plane APIs expect:
/// `projects/_/buckets/{bucket}`. The literal `_` stands in for the project, which is already
/// implied by the (globally unique) bucket name.
pub fn bucket_resource_name(bucket: &str) -> String {
    format!("projects/_/buckets/{bucket}")
}

/// Build the GCS data-plane (`Storage`) and control-plane (`StorageControl`) clients.
///
/// Both use Application Default Credentials by default: the caller does not configure
/// credentials here, matching the design decision to lean on ADC rather than reimplementing
/// `lore-aws`'s manual credential-chain handling.
pub async fn build_storage_clients(
    endpoint: Option<&str>,
) -> Result<(Storage, StorageControl), GcpError> {
    let mut storage_builder = Storage::builder();
    let mut control_builder = StorageControl::builder();
    if let Some(endpoint) = endpoint {
        storage_builder = storage_builder.with_endpoint(endpoint.to_string());
        control_builder = control_builder.with_endpoint(endpoint.to_string());
    }

    let storage = storage_builder
        .build()
        .await
        .map_err(GcpError::client_build)?;
    let control = control_builder
        .build()
        .await
        .map_err(GcpError::client_build)?;
    Ok((storage, control))
}

/// Build a Firestore client for `project_id`, against `database_id` (Firestore-native mode
/// supports named databases; `None` uses the default database, `"(default)"`).
pub async fn build_firestore_db(
    project_id: &str,
    database_id: Option<&str>,
) -> Result<FirestoreDb, GcpError> {
    let mut options = FirestoreDbOptions::new(project_id.to_string());
    if let Some(database_id) = database_id {
        options = options.with_database_id(database_id.to_string());
    }
    FirestoreDb::with_options(options)
        .await
        .map_err(GcpError::firestore)
}

/// Fail fast if the configured bucket does not exist, the way
/// `lore_aws::clients::AwsClientBuilder::ensure_bucket` does for S3. Bucket provisioning is a
/// separate Terraform workstream; this only confirms that workstream has run.
pub async fn ensure_bucket_exists(control: &StorageControl, bucket: &str) -> Result<(), GcpError> {
    control
        .get_bucket()
        .set_name(bucket_resource_name(bucket))
        .send()
        .await
        .map_err(GcpError::gcs)?;
    Ok(())
}

/// Run `fut`, bounding it to `timeout`. A timeout is reported through `on_timeout` rather than
/// baked into a fixed error type, since callers want it mapped to different `StoreError` variants
/// (`SlowDown` almost everywhere, but callers are free to choose).
pub async fn with_timeout<T, E>(
    timeout: Duration,
    fut: impl Future<Output = Result<T, E>>,
    on_timeout: impl FnOnce() -> E,
) -> Result<T, E> {
    match tokio::time::timeout(timeout, fut).await {
        Ok(result) => result,
        Err(_elapsed) => Err(on_timeout()),
    }
}
