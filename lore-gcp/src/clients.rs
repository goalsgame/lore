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
use std::sync::Once;
use std::time::Duration;

use firestore::FirestoreDb;
use firestore::FirestoreDbOptions;
use google_cloud_storage::client::Storage;
use google_cloud_storage::client::StorageControl;
use lore_base::error::SlowDown;
use lore_storage::StoreError;
use tracing::warn;

use crate::gcp_error::GcpError;

static CRYPTO_PROVIDER_INIT: Once = Once::new();

/// Defensively install the default `rustls` `CryptoProvider` (`ring`) if one is not already
/// installed, before this crate's first TLS handshake.
///
/// Both `google-cloud-storage` and `firestore` build on `rustls`'s "no default provider" feature
/// path (deliberately, to avoid pulling in `aws-lc-rs`; see this crate's `Cargo.toml`), which
/// means *something* in the process must call `rustls::crypto::CryptoProvider::install_default`
/// before the first handshake or every TLS connection panics. In production this already happens
/// once at server startup, via `lore_revision::interface::ExecutionContext::new_server`/
/// `new_client` (and in tests, via `setup_execution()`). But nothing enforces that every caller of
/// this crate went through one of those paths: a standalone tool — a migration script, an admin
/// CLI, a benchmark, or a test that forgets `setup_execution()` — would otherwise get a confusing
/// rustls panic on its first request instead of a clear, defensive fallback. Every public
/// constructor in this module calls this first, so the dependency on a process-wide provider is
/// made explicit here rather than left as an implicit contract on the caller.
///
/// `jsonwebtoken` (used transitively by `firestore`'s `gcloud-sdk` transport to sign self-signed
/// JWTs for service-account credentials) has an analogous process-wide `CryptoProvider`, but does
/// not need a runtime call here: this crate's `Cargo.toml` enables exactly one of its
/// `rust_crypto`/`aws_lc_rs` crate features (`rust_crypto`, to avoid `aws-lc-rs`), which makes
/// `jsonwebtoken` auto-select that provider at compile time with no installation step.
fn ensure_crypto_provider() {
    CRYPTO_PROVIDER_INIT.call_once(|| {
        if rustls::crypto::CryptoProvider::get_default().is_some() {
            return;
        }
        if rustls::crypto::ring::default_provider()
            .install_default()
            .is_err()
        {
            // Lost a race with another installer between the check above and this call — that
            // installer's provider is now the process default. Not necessarily `ring`, but
            // `get_default()` returning `Some` from here on is exactly the postcondition this
            // function promises, so there is nothing left to do.
            warn!(
                "rustls default CryptoProvider was installed by another caller between this \
                 function's check and its install attempt; leaving that installation in place"
            );
        }
    });
}

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
///
/// `control_endpoint` overrides `StorageControl`'s endpoint separately from `endpoint` (which
/// only applies to the data-plane `Storage` client): production never needs this (both point at
/// the real service), but a local test double may run its gRPC control-plane surface on a
/// different port than its HTTP data-plane surface — the Google Cloud Storage testbench is
/// exactly such a case. `None` leaves the corresponding client on its production default.
pub async fn build_storage_clients(
    endpoint: Option<&str>,
    control_endpoint: Option<&str>,
) -> Result<(Storage, StorageControl), GcpError> {
    ensure_crypto_provider();

    let mut storage_builder = Storage::builder();
    let mut control_builder = StorageControl::builder();
    if let Some(endpoint) = endpoint {
        storage_builder = storage_builder.with_endpoint(endpoint.to_string());
    }
    if let Some(control_endpoint) = control_endpoint {
        control_builder = control_builder.with_endpoint(control_endpoint.to_string());
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
    ensure_crypto_provider();

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

/// Run `fut`, bounding it to `timeout` and logging a warning if it takes longer than
/// `slow_threshold` to complete.
///
/// On timeout this reports [`StoreError::SlowDown`] rather than any GCS/Firestore-specific error
/// type: a stalled remote call should look exactly like the "ask the caller to retry" signal
/// every other transient failure in this crate maps to (see
/// `store::immutable_store::to_store_error`/`store::mutable_store::to_store_error`), not hang the
/// calling task indefinitely.
///
/// `slow_threshold` wires up `[plugins.gcp]`'s `*_slow_operation_threshold_millis` config knobs —
/// `lore_aws`'s equivalent (`AwsClientBuilder::with_slow_operation_threshold`) logs at the same
/// granularity, once per underlying SDK call rather than once per `ImmutableStore`/`MutableStore`
/// trait method, which is what calling this at every call site (rather than once around each
/// trait method) reproduces here.
pub async fn bounded<T>(
    timeout: Duration,
    slow_threshold: Duration,
    op: &'static str,
    fut: impl Future<Output = T>,
) -> Result<T, StoreError> {
    let start = std::time::Instant::now();
    let result = tokio::time::timeout(timeout, fut).await;
    let elapsed = start.elapsed();
    if elapsed > slow_threshold {
        warn!(
            op,
            ?elapsed,
            ?slow_threshold,
            "GCP operation exceeded the configured slow-operation threshold"
        );
    }
    result.map_err(|_elapsed| {
        warn!(
            op,
            ?timeout,
            "GCP operation timed out; asking the caller to retry"
        );
        StoreError::from(SlowDown)
    })
}

/// Whether a Firestore failure means "retry me". Shared by `store::immutable_store` and
/// `store::mutable_store` (previously two byte-identical copies, one per module — consolidated
/// here the same way the config-default functions were consolidated into `plugins::gcp`, since
/// nothing about this classification is specific to either store).
///
/// Delegates to the `firestore` crate's own `retry_possible` flag for the common cases
/// (`Aborted`/`Cancelled`/`Unavailable`/`ResourceExhausted`, plus a handful of transport-level
/// conditions `firestore` recognizes from the underlying `hyper` error), but does not trust it
/// alone: `firestore-0.47.1`'s `errors.rs` `impl From<gcloud_sdk::tonic::Status> for
/// FirestoreError` sets `retry_possible: false` for every gRPC status code outside that short
/// list — including `DEADLINE_EXCEEDED`, a server reporting that *it* hit its own deadline, which
/// is exactly as transient/retryable as `UNAVAILABLE` is. That status code is not lost, though:
/// every branch of that `From` impl (including the catch-all one `DEADLINE_EXCEEDED` falls into)
/// stamps `public.code` with `format!("{:?}", status.code())` — `tonic::Code`'s plain derived
/// `Debug`, so a `DEADLINE_EXCEEDED` status leaves `public.code == "DeadlineExceeded"` — which is
/// the one place that code survives on `FirestoreDatabaseError` for a caller to check, since the
/// struct does not otherwise expose the original `tonic::Code`. This is what the second half of
/// this function's condition checks, independent of `retry_possible`.
///
/// (This is distinct from — and layered on top of, not a replacement for — the client-side
/// timeout `bounded` in this module enforces on every call: this classifies a failure the
/// *server* already returned, including one where the server gave up on its own deadline;
/// `bounded` instead bounds calls that never returned at all.)
pub(crate) fn is_firestore_retryable(error: &firestore::errors::FirestoreError) -> bool {
    use firestore::errors::FirestoreError;
    match error {
        FirestoreError::DatabaseError(e) => e.retry_possible || e.public.code == "DeadlineExceeded",
        FirestoreError::NetworkError(_) => true,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ensure_crypto_provider_is_idempotent() {
        // Calling this repeatedly (as every client constructor does) must never panic, whether or
        // not some other test in the same process binary already installed a provider first.
        ensure_crypto_provider();
        ensure_crypto_provider();
        assert!(rustls::crypto::CryptoProvider::get_default().is_some());
    }

    /// Regression test for the exact gap the code review caught: a server-side
    /// `DEADLINE_EXCEEDED` status is `firestore::errors::FirestoreDatabaseError`'s `retry_possible
    /// = false` catch-all case, but must still be treated as retryable here.
    #[test]
    fn deadline_exceeded_is_retryable_even_though_the_firestore_crate_marks_it_permanent() {
        use firestore::errors::FirestoreDatabaseError;
        use firestore::errors::FirestoreError;
        use firestore::errors::FirestoreErrorPublicGenericDetails;

        let error = FirestoreError::DatabaseError(FirestoreDatabaseError::new(
            FirestoreErrorPublicGenericDetails::new("DeadlineExceeded".to_string()),
            "Status { code: DeadlineExceeded, message: \"\" }".to_string(),
            false,
        ));

        assert!(is_firestore_retryable(&error));
    }

    #[test]
    fn other_permanent_database_errors_stay_non_retryable() {
        use firestore::errors::FirestoreDatabaseError;
        use firestore::errors::FirestoreError;
        use firestore::errors::FirestoreErrorPublicGenericDetails;

        let error = FirestoreError::DatabaseError(FirestoreDatabaseError::new(
            FirestoreErrorPublicGenericDetails::new("InvalidArgument".to_string()),
            "Status { code: InvalidArgument, message: \"\" }".to_string(),
            false,
        ));

        assert!(!is_firestore_retryable(&error));
    }
}
