// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
//! Firestore-backed [`MutableStore`], mirroring `lore_aws::store::mutable_store`.
//!
//! # Schema
//!
//! One flat collection (default name `mutable_store`, configurable), one document per
//! `(partition, typed key)` pair:
//!
//! ```text
//! {collection}/{partition_hex}_{key_hex}  =>  { partition: hex, key: hex, value: hex }
//! ```
//!
//! `key_hex` is the hex encoding of the *typed* key — the 32-byte [`Hash`] with its first byte
//! overwritten by the [`KeyType`] discriminant, exactly as `lore_aws::store::mutable_store` and
//! the local store do. Hex encoding a fixed-width byte string preserves byte ordering
//! lexicographically, which is what lets [`list`](MutableStore::list) implement its key-type
//! range scan (`key_type_byte, 0x00…` to `key_type_byte, 0xFF…`) as a Firestore string range
//! filter on the `key` field.
//!
//! A flat collection — rather than a subcollection per partition — is what lets `list` with a
//! null partition answer from every partition in one query (a range filter alone, no equality
//! filter), and what lets `list` with a specific partition combine an equality filter on
//! `partition` with a range filter on `key` in a single query. That combination (equality on one
//! field, inequality/range on another) is exactly the shape Firestore requires an explicit
//! composite index for; see `firestore.indexes.json` at this crate's root, which the infra team
//! must deploy alongside the collection name configured in `[plugins.gcp]`.
//!
//! **Not independently verified against production Firestore:** [`list_typed`]'s query sets no
//! explicit `order_by`. Firestore's documented behavior is that a range filter on a field
//! implicitly orders by that field, which would make this correct as written, and
//! `lore-integration-tests`' `gcp_store_test.rs` (`mutable_list_filters_by_key_type`,
//! `gcp_mutable_store_satisfies_conformance_battery`) confirms `list` returns every matching
//! document — and only those — against the real Firestore emulator (Google's own server binary,
//! run via `gcloud emulators firestore start`, not a third-party reimplementation). That is
//! meaningful evidence this is correct, but it is still the emulator rather than the hosted
//! service; a reviewer with access to a real Firestore project should re-run that suite there at
//! least once (see `.github/workflows/pr-validate.yml`'s `gcp-integration` job and
//! `lore-integration-tests/tests/common/mod.rs`'s `gcp_common` module for how to point it at a
//! real project instead) before leaning on this as the final word.
//!
//! # Compare-and-swap
//!
//! [`MutableStore::compare_and_swap`] runs inside a Firestore transaction
//! (`FirestoreDb::run_transaction`), which is strongly consistent and auto-retries on write
//! conflicts — simpler than `DynamoDB`'s opt-in `consistent_read` and manual conditional
//! expressions. The read-compare-write is still exactly `lore_aws`'s
//! `CompareAndSwapCondition`-fixed semantics: a key that was never written and a key explicitly
//! holding a zero value are treated as the same starting state when `expected` is zero, so a CAS
//! that expects "nothing here yet" succeeds against either. See
//! `lore_aws::store::mutable_store::CompareAndSwapCondition` for the incident this fixed
//! (silently dropping the first push to a freshly created branch) — the same bug is possible
//! here if that unification is ever lost, so don't special-case it away.
//!
//! **Known limitation, not attempted here:** Firestore transactions (like most databases with
//! server-side retry) have an "ambiguous commit" edge case — if the client loses the response to
//! a commit that actually succeeded (a network blip after the server has already applied the
//! write), the client-side retry machinery in `firestore`/`gcloud-sdk` cannot always tell that
//! apart from a commit that never happened, and may report failure or retry a transaction whose
//! effect already landed. For [`compare_and_swap_typed`] specifically this could in principle
//! make a swap that actually succeeded look like it failed to the caller (returning a stale
//! "previous value" that no longer matches what is stored). This is a hard, general distributed
//! systems problem — not a defect in this port's CAS logic — and is not something this pass
//! attempts to solve; a caller that needs to rule it out must re-read after an unexpected CAS
//! failure rather than trust the returned "previous value" as an infallible oracle.
//!
//! # Every remote call is bounded and classified
//!
//! See the equivalent section in `store::immutable_store`'s module docs — the same
//! [`crate::clients::bounded`] timeout/slow-threshold wrapper and the same
//! [`crate::clients::is_firestore_retryable`] retryable-error classification apply to every
//! Firestore call this module makes.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use firestore::FirestoreDb;
use firestore::errors::BackoffError;
use futures::FutureExt;
use futures::StreamExt;
use lore_base::error::SlowDown;
use lore_base::types::Address;
use lore_base::types::Hash;
use lore_base::types::KeyType;
use lore_base::types::Partition;
use lore_storage::ImmutableStore;
use lore_storage::KeyValueStream;
use lore_storage::MutableStore as MutableStoreTrait;
use lore_storage::StoreError;
use lore_telemetry::InstrumentProvider;
use lore_telemetry::LabelArray;
use lore_telemetry::METRICS_OPERATION_LATENCY_METRIC_NAME;
use lore_telemetry::timed;
use lore_telemetry::timer::TimedResult;
use opentelemetry::KeyValue;
use serde::Deserialize;
use serde::Serialize;
use smallvec::SmallVec;
use tracing::Instrument;
use tracing::debug;
use tracing::warn;

use crate::clients::bounded;
use crate::gcp_error::GcpError;

/// Configuration for the Firestore mutable store.
///
/// Deliberately a plain struct rather than one deserialized directly from TOML a second time:
/// see `store::immutable_store::GcsStoreSettings`'s doc comment for why — TOML defaulting lives
/// once, in `lore-server`'s `plugins::gcp::GcpMutableStorePluginConfig`.
#[derive(Clone, Debug)]
pub struct FirestoreMutableStoreSettings {
    /// GCP project holding the Firestore database.
    pub firestore_project: String,
    /// Firestore database id. `None` uses the default database, `"(default)"`.
    pub firestore_database: Option<String>,
    /// Collection name for mutable store entries.
    pub firestore_mutable_store_collection: String,
    /// Force write mode. Kept for config-shape parity with `lore-aws`'s
    /// `AwsMutableStoreSettings`; unused here for the same reason it is unused there — nothing
    /// in this store's write path has a "trust the caller, skip the check" shortcut to bypass.
    pub force_write: bool,
    /// Timeout for individual Firestore operations.
    pub timeout_millis: u64,
    /// Slow-operation threshold for telemetry, in milliseconds.
    pub slow_operation_threshold_millis: u64,
}

/// A row in the mutable store collection.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct MutableEntry {
    /// Hex-encoded [`Partition`]. Kept as an explicit field (rather than relying solely on the
    /// document id) so the equality filter half of the `(partition, key)` composite index has
    /// something to filter on.
    partition: String,
    /// Hex-encoded typed key (the `Hash` with its first byte replaced by the `KeyType`
    /// discriminant). Also encoded into the document id, but kept as a field for the same
    /// reason as `partition`: Firestore range filters act on fields, not document ids, without
    /// resorting to `__name__` queries.
    key: String,
    /// Hex-encoded value `Hash`. May be the all-zero hash — see the module docs on
    /// compare-and-swap for why a zero-valued row is allowed to exist rather than being deleted.
    value: String,
}

fn hex_encode<const N: usize>(bytes: &[u8; N]) -> String {
    hex::encode(bytes)
}

fn parse_hash(value: &str) -> Result<Hash, GcpError> {
    let bytes = hex::decode(value)
        .map_err(|e| GcpError::Decode(format!("value {value:?} is not valid hex: {e}")))?;
    let bytes: [u8; 32] = bytes.try_into().map_err(|bytes: Vec<u8>| {
        GcpError::Decode(format!(
            "value {value:?} decodes to {} bytes, expected 32",
            bytes.len()
        ))
    })?;
    Ok(Hash::from(bytes))
}

fn to_store_error(error: GcpError) -> StoreError {
    if let GcpError::Firestore(inner) = &error
        && crate::clients::is_firestore_retryable(inner)
    {
        return StoreError::from(SlowDown);
    }
    StoreError::internal_with_context(error, "Firestore mutable store operation failed")
}

pub struct FirestoreMutableStore {
    db: FirestoreDb,
    collection: Arc<str>,
    timeout: Duration,
    slow_threshold: Duration,
    latency_histogram: opentelemetry::metrics::Histogram<f64>,
}

impl FirestoreMutableStore {
    #[allow(unused)]
    pub fn new(
        db: FirestoreDb,
        settings: &FirestoreMutableStoreSettings,
        immutable_store: Arc<dyn ImmutableStore>,
    ) -> Self {
        let provider = FirestoreMutableStoreInstrumentProvider;
        Self {
            db,
            collection: Arc::from(settings.firestore_mutable_store_collection.as_str()),
            timeout: Duration::from_millis(settings.timeout_millis.max(1)),
            slow_threshold: Duration::from_millis(settings.slow_operation_threshold_millis),
            latency_histogram: provider.latency_histogram_ms(METRICS_OPERATION_LATENCY_METRIC_NAME),
        }
    }

    /// Run a Firestore future, bounded by this store's configured timeout/slow threshold, and
    /// classify its error as a `StoreError` (retryable failures become
    /// [`StoreError::SlowDown`]; see [`crate::clients::is_firestore_retryable`]).
    async fn firestore_op<T>(
        &self,
        op: &'static str,
        fut: impl Future<Output = firestore::FirestoreResult<T>>,
    ) -> Result<T, StoreError> {
        bounded(self.timeout, self.slow_threshold, op, fut)
            .await?
            .map_err(GcpError::firestore)
            .map_err(to_store_error)
    }

    fn typed_key(mut key: Hash, key_type: KeyType) -> Hash {
        key.data_mut()[0] = key_type as u8;
        key
    }

    fn doc_id(partition: Partition, typed_key: Hash) -> String {
        format!(
            "{}_{}",
            hex_encode(partition.data()),
            hex_encode(typed_key.data())
        )
    }

    async fn load_typed(&self, partition: Partition, typed_key: Hash) -> Result<Hash, StoreError> {
        let doc_id = Self::doc_id(partition, typed_key);
        let entry: Option<MutableEntry> = self
            .firestore_op(
                "mutable_store.get",
                self.db
                    .fluent()
                    .select()
                    .by_id_in(self.collection.as_ref())
                    .obj()
                    .one(&doc_id),
            )
            .await?;

        match entry {
            Some(entry) => {
                let value = parse_hash(&entry.value).map_err(to_store_error)?;
                if value.is_zero() {
                    Err(StoreError::from(lore_base::error::AddressNotFound::from(
                        Address::zero_context_hash(typed_key),
                    )))
                } else {
                    Ok(value)
                }
            }
            None => Err(StoreError::from(lore_base::error::AddressNotFound::from(
                Address::zero_context_hash(typed_key),
            ))),
        }
    }

    async fn store_typed(
        &self,
        partition: Partition,
        typed_key: Hash,
        value: Hash,
    ) -> Result<(), StoreError> {
        let doc_id = Self::doc_id(partition, typed_key);

        if value.is_zero() {
            self.firestore_op(
                "mutable_store.delete",
                self.db
                    .fluent()
                    .delete()
                    .from(self.collection.as_ref())
                    .document_id(&doc_id)
                    .execute(),
            )
            .await?;
            return Ok(());
        }

        let entry = MutableEntry {
            partition: hex_encode(partition.data()),
            key: hex_encode(typed_key.data()),
            value: hex_encode(value.data()),
        };

        self.firestore_op::<MutableEntry>(
            "mutable_store.set",
            self.db
                .fluent()
                .update()
                .in_col(self.collection.as_ref())
                .document_id(&doc_id)
                .object(&entry)
                .execute(),
        )
        .await?;

        Ok(())
    }

    async fn compare_and_swap_typed(
        &self,
        partition: Partition,
        typed_key: Hash,
        expected: Hash,
        value: Hash,
    ) -> Result<Hash, StoreError> {
        let collection = self.collection.clone();
        let doc_id = Self::doc_id(partition, typed_key);
        let entry = MutableEntry {
            partition: hex_encode(partition.data()),
            key: hex_encode(typed_key.data()),
            value: hex_encode(value.data()),
        };

        bounded(
            self.timeout,
            self.slow_threshold,
            "mutable_store.compare_and_swap_transaction",
            self.db
                .run_transaction::<Hash, _, GcpError>(move |db, transaction| {
                    let collection = collection.clone();
                    let doc_id = doc_id.clone();
                    let entry = entry.clone();
                    async move {
                        let current: Option<MutableEntry> = db
                            .fluent()
                            .select()
                            .by_id_in(collection.as_ref())
                            .obj()
                            .one(&doc_id)
                            .await
                            .map_err(GcpError::firestore)
                            .map_err(BackoffError::Permanent)?;

                        let current_value = match current {
                            Some(entry) => {
                                parse_hash(&entry.value).map_err(BackoffError::Permanent)?
                            }
                            None => Hash::default(),
                        };

                        // Unify "row never written" and "row explicitly written as zero": both
                        // are the starting state a caller means by `expected == 0`. See the
                        // module docs.
                        let matches = if expected.is_zero() {
                            current_value.is_zero()
                        } else {
                            current_value == expected
                        };

                        if !matches {
                            return Ok(current_value);
                        }

                        db.fluent()
                            .update()
                            .in_col(collection.as_ref())
                            .document_id(&doc_id)
                            .object(&entry)
                            .add_to_transaction(transaction)
                            .map_err(GcpError::firestore)
                            .map_err(BackoffError::Permanent)?;

                        Ok(expected)
                    }
                    .boxed()
                }),
        )
        .await?
        .map_err(GcpError::firestore)
        .map_err(to_store_error)
    }

    fn list_typed(&self, partition: Partition, key_type: KeyType) -> KeyValueStream {
        let (stream, sender) = KeyValueStream::new();

        if key_type == KeyType::Untyped {
            return stream;
        }

        let mut key_start = [0u8; 32];
        key_start[0] = key_type as u8;
        let mut key_end = [0xFFu8; 32];
        key_end[0] = key_type as u8;
        let key_start_hex = hex_encode(&key_start);
        let key_end_hex = hex_encode(&key_end);

        let db = self.db.clone();
        let collection = self.collection.clone();
        let partition_hex = if partition.is_zero() {
            None
        } else {
            Some(hex_encode(partition.data()))
        };
        let timeout = self.timeout;
        let slow_threshold = self.slow_threshold;

        lore_base::lore_spawn!(
            async move {
                // Bounded as one whole operation (query setup through fully draining the
                // stream), the same way `store::immutable_store::associations_present`/
                // `states_for` bound their own batch-and-drain loops: a stalled or slow-loris
                // response here must not hang this spawned task forever, even though
                // `list`'s own return type has no room to carry a mid-stream timeout error to
                // the caller (the stream just ends early, which callers already have to treat
                // as "not necessarily every entry" the same way a query failure would leave it).
                let outcome = bounded(timeout, slow_threshold, "mutable_store.list", async {
                    let query = db
                        .fluent()
                        .select()
                        .from(collection.as_ref())
                        .filter(|q| {
                            let mut clauses = vec![
                                q.field("key").greater_than_or_equal(key_start_hex.clone()),
                                q.field("key").less_than_or_equal(key_end_hex.clone()),
                            ];
                            if let Some(partition_hex) = partition_hex.clone() {
                                clauses.push(q.field("partition").eq(partition_hex));
                            }
                            q.for_all(clauses)
                        })
                        .obj::<MutableEntry>()
                        .stream_query_with_errors()
                        .await;

                    let mut query = match query {
                        Ok(query) => query,
                        Err(err) => {
                            warn!(?err, "Firestore mutable store list query failed");
                            return;
                        }
                    };

                    while let Some(item) = query.next().await {
                        let entry = match item {
                            Ok(entry) => entry,
                            Err(err) => {
                                warn!(?err, "Firestore mutable store list item failed");
                                continue;
                            }
                        };

                        let (key, value) = match (parse_hash(&entry.key), parse_hash(&entry.value))
                        {
                            (Ok(key), Ok(value)) => (key, value),
                            _ => {
                                warn!(?entry, "Firestore mutable store row has unparsable hex");
                                continue;
                            }
                        };

                        if value.is_zero() {
                            continue;
                        }

                        if let Err(err) = sender.send((key, value)) {
                            debug!(%err, "Failed sending mutable list result");
                            return;
                        }
                    }
                })
                .await;

                if outcome.is_err() {
                    warn!("Firestore mutable store list timed out before draining fully");
                }
            }
            .in_current_span()
        );

        stream
    }
}

#[async_trait]
impl MutableStoreTrait for FirestoreMutableStore {
    #[lore_macro::lore_instrument]
    #[tracing::instrument(name = "FirestoreMutableStore::load" skip(self))]
    async fn load(
        self: Arc<Self>,
        partition: Partition,
        key: Hash,
        key_type: KeyType,
    ) -> Result<Hash, StoreError> {
        let typed_key = Self::typed_key(key, key_type);
        timed!(
            self.latency_histogram,
            &self.get_labels_for_operation_context("load"),
            { self.load_typed(partition, typed_key).await }
        )
        .into()
    }

    #[lore_macro::lore_instrument]
    #[tracing::instrument(name = "FirestoreMutableStore::store" skip(self))]
    async fn store(
        self: Arc<Self>,
        partition: Partition,
        key: Hash,
        value: Hash,
        key_type: KeyType,
    ) -> Result<(), StoreError> {
        let typed_key = Self::typed_key(key, key_type);
        timed!(
            self.latency_histogram,
            &self.get_labels_for_operation_context("store"),
            { self.store_typed(partition, typed_key, value).await }
        )
        .into()
    }

    #[lore_macro::lore_instrument]
    #[tracing::instrument(name = "FirestoreMutableStore::compare_and_swap" skip(self))]
    async fn compare_and_swap(
        self: Arc<Self>,
        partition: Partition,
        key: Hash,
        expected: Hash,
        value: Hash,
        key_type: KeyType,
    ) -> Result<Hash, StoreError> {
        let typed_key = Self::typed_key(key, key_type);
        timed!(
            self.latency_histogram,
            &self.get_labels_for_operation_context("compare_and_swap"),
            {
                self.compare_and_swap_typed(partition, typed_key, expected, value)
                    .await
            }
        )
        .into()
    }

    #[lore_macro::lore_instrument]
    #[tracing::instrument(name = "FirestoreMutableStore::list" skip(self))]
    async fn list(
        self: Arc<Self>,
        partition: Partition,
        key_type: KeyType,
    ) -> Result<KeyValueStream, StoreError> {
        Ok(self.list_typed(partition, key_type))
    }

    async fn flush(self: Arc<Self>, _sync_data: bool) -> Result<(), StoreError> {
        // Firestore writes are durable as soon as they are acknowledged; nothing to flush.
        Ok(())
    }
}

struct FirestoreMutableStoreInstrumentProvider;

impl InstrumentProvider for FirestoreMutableStoreInstrumentProvider {
    fn namespace(&self) -> &'static str {
        "urc.store.mutable.gcp"
    }
}

impl InstrumentProvider for FirestoreMutableStore {
    fn namespace(&self) -> &'static str {
        "urc.store.mutable.gcp"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn typed_key_overwrites_first_byte() {
        let key = Hash::from([0xAAu8; 32]);
        let typed = FirestoreMutableStore::typed_key(key, KeyType::BranchId);
        assert_eq!(typed.data()[0], KeyType::BranchId as u8);
        assert_eq!(&typed.data()[1..], &[0xAAu8; 31]);
    }

    #[test]
    fn hex_round_trips_a_hash() {
        let hash = Hash::from([0x42u8; 32]);
        let encoded = hex_encode(hash.data());
        assert_eq!(parse_hash(&encoded).unwrap(), hash);
    }

    #[test]
    fn parse_hash_rejects_bad_hex() {
        assert!(parse_hash("not-hex").is_err());
        assert!(parse_hash("ab").is_err(), "too short");
    }
}
