// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
//! GCS + Firestore-backed [`ImmutableStore`], mirroring `lore_aws::store::immutable_store`.
//!
//! GCS holds the payload, content-addressed under a flat namespace (object name = hex-encoded
//! hash, matching the S3 layout). Firestore holds the two things `DynamoDB` holds for the AWS
//! store:
//!
//! - `fragment_state` — one document per hash, `{hash_hex}` => `{ state }`, recording whether the
//!   payload is `Stored`, `Obliterating`, or `Obliterated`. See [`FragmentState`].
//! - `fragment_associations` (name configurable) — one document per `(hash, partition, context)`
//!   triple, `{hash_hex}_{partition_hex}_{context_hex}` => `{ hash, partition, context }`,
//!   recording which partitions/contexts reference a hash. A flat, top-level collection rather
//!   than a subcollection under `fragment_state`: it is what lets [`associations_present`] batch
//!   an existence check for many addresses in one Firestore call
//!   (`FirestoreDb::batch_stream_get_objects`, via the `.by_id_in().obj().batch(ids)` fluent
//!   call), the same way `lore_aws`'s `BatchGetItem` does. A subcollection per hash would need
//!   one round trip per distinct hash instead of one for the whole batch — and `query` is on the
//!   ingress write path, once per fragment stored, so that round trip count matters.
//!
//! What a payload *is* — its compression and its sizes — is never duplicated into Firestore. It
//! lives on the GCS object's own custom metadata, written in the same `write_object` call that
//! uploads the body (see [`object_metadata`]), for the identical reason `lore_aws` moved fragment
//! metadata onto the S3 object in ADR-00018: the representation and the bytes it describes can
//! never disagree, and existence can be answered without a second read.
//!
//! # Composite indexes this design needs
//!
//! [`has_partition_association`] answers "does `partition` hold *any* association for `hash`,
//! under any context" — used by [`ImmutableStore::copy`] when its source names no context. That
//! is `hash == X AND partition == Y`, an equality-only compound filter Firestore may or may not
//! serve from automatic indexing alone depending on the deployment's Firestore version; this
//! crate does not gamble on that and declares the index explicitly in `firestore.indexes.json`.
//! `has_associations` (any association at all, used by obliteration's "is this hash still
//! referenced" check) is a single equality filter and needs no declared index.
//!
//! # Every remote call is bounded and classified
//!
//! Every GCS and Firestore call in this module goes through [`crate::clients::bounded`], which
//! enforces the configured timeout (mapping an elapsed deadline to [`StoreError::SlowDown`] so a
//! stalled connection fails fast rather than hanging the calling task) and logs a warning once an
//! operation exceeds the configured slow-operation threshold — the same two knobs
//! `[plugins.gcp]`'s `timeout_millis`/`*_slow_operation_threshold_millis` control for `lore_aws`,
//! at the same granularity (per underlying SDK call, not per `ImmutableStore` trait method).
//! [`is_gcs_retryable`]/[`is_firestore_retryable`] then classify the failure itself: a throttle,
//! `UNAVAILABLE`, or `DEADLINE_EXCEEDED` also becomes `StoreError::SlowDown` rather than a hard
//! internal error, mirroring `lore_aws::store::immutable_store::is_dynamodb_overloaded` — so a
//! client retries a transient failure instead of surfacing it as permanent.
//!
//! # A test-double bug, not a `lore-gcp` bug: the GCS testbench rejects this store's writes
//!
//! [`GcpImmutableStore::write_payload_and_state`]'s `write_object` call carries custom metadata
//! (the fragment) in the same call as the body, which forces `google-cloud-storage` onto GCS's
//! `uploadType=multipart` wire format rather than a simple media upload. `google-cloud-storage`
//! 1.18.0 never attaches a `Content-Type` header to that request's raw "media" part (only to the
//! JSON "metadata" part's `contentType` field, which is a different thing). The [Google Cloud
//! Storage testbench][testbench] this crate's own integration tests run against — not real
//! GCS — mishandles that: its `parse_multipart`/`init_multipart` (`testbench/common.py`,
//! `gcs/object.py`) assign the *absence* of that header straight into a field it then feeds to a
//! protobuf `ParseDict` call, which raises on the resulting `None` and turns into an unhandled
//! HTTP 500, rather than defaulting the content type the way a real bucket does. Confirmed by
//! reproducing both directions by hand against the same testbench container: an otherwise
//! identical multipart request that *does* carry a `Content-Type` header on the media part
//! succeeds (`200`); the exact bytes this client sends, captured off the wire, do not carry one
//! and get the `500`. This is a bug in the testbench (and arguably in `google-cloud-storage`,
//! for never sending that header), not in this store's write path or its atomicity — nothing
//! about it is specific to GCP-as-opposed-to-AWS, and there is no source-level workaround from
//! here short of vendoring one of those two dependencies. It means `lore-integration-tests`'
//! `gcp_store_test.rs` cannot exercise the immutable-store side of the conformance battery
//! end-to-end in CI today; see `.github/workflows/pr-validate.yml`'s `gcp-integration` job comment
//! for how that is reflected there. The mutable-store side (Firestore only, no GCS writes) is not
//! affected and does pass against the emulator.
//!
//! [testbench]: https://github.com/googleapis/storage-testbench
//!
//! # Known limitations shared with `lore_aws` (not unique to this port)
//!
//! - **Obliteration drain window read-inconsistency.** Between [`ImmutableStore::obliterate`]
//!   taking the `Obliterating` mark and finishing its drain sleep, [`ImmutableStore::get`] (which
//!   only consults [`GcpImmutableStore::exists`], not fragment state) can still serve a payload
//!   whose association was already deleted moments earlier, while
//!   [`ImmutableStore::query`]/[`ImmutableStore::get_metadata`] (which do consult state) already
//!   report it obliterated. `lore_aws::store::immutable_store::AwsImmutableStore` has the
//!   identical inconsistency for the identical reason (its `get` also skips the state read this
//!   store's `do_query`/`do_query_batch` perform) — fixing it only here would just add drift
//!   between the two backends, not close a GCP-specific gap.
//! - **Copy-during-obliteration dangling-association race.** [`ImmutableStore::copy`]'s existence
//!   check and its `associate_fragment` write are not atomic with a concurrent
//!   [`ImmutableStore::obliterate`] of the same hash: a copy that reads "present" just before an
//!   obliteration's own association delete can still write a new association after the
//!   obliteration's re-check has already decided nothing references the hash, leaving that new
//!   association pointing at a payload GCS has already deleted. `lore_aws` has the same window
//!   for the same reason (its `copy`/`obliterate` are not one atomic operation there either).
//!
//! [`associations_present`]: GcpImmutableStore::associations_present
//! [`has_partition_association`]: GcpImmutableStore::has_partition_association

use std::collections::HashMap;
use std::collections::HashSet;
use std::future::Future;
use std::sync::Arc;
use std::sync::LazyLock;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use bytes::BytesMut;
use firestore::FirestoreDb;
use firestore::errors::BackoffError;
use futures::FutureExt;
use futures::StreamExt;
use google_cloud_gax::paginator::ItemPaginator;
use lore_base::error::AddressNotFound;
use lore_base::error::SlowDown;
use lore_base::types::Address;
use lore_base::types::Context;
use lore_base::types::FRAGMENT_SIZE_THRESHOLD;
use lore_base::types::Fragment;
use lore_base::types::FragmentFlags;
use lore_base::types::FragmentReference;
use lore_base::types::Hash;
use lore_base::types::Partition;
use lore_base::types::TypedBytes;
use lore_storage::ImmutableStore as ImmutableStoreTrait;
use lore_storage::Oversized;
use lore_storage::StoreError;
use lore_storage::StoreGetData;
use lore_storage::StoreMatch;
use lore_storage::StoreMatchResult;
use lore_storage::StoreObliterateStats;
use lore_storage::immutable_store::sanitise_fragment_behavior_flags;
use lore_telemetry::InstrumentProvider;
use lore_telemetry::LabelArray;
use lore_telemetry::METRICS_OPERATION_LATENCY_METRIC_NAME;
use lore_telemetry::timed;
use lore_telemetry::timer::TimedResult;
use opentelemetry::KeyValue;
use opentelemetry::metrics::Counter;
use opentelemetry::metrics::Histogram;
use smallvec::SmallVec;
use tokio::task::JoinSet;
use tracing::Instrument;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::trace;
use tracing::warn;

use crate::clients::bounded;
use crate::clients::bucket_resource_name;
use crate::gcp_error::GcpError;
use crate::store::object_metadata::from_object_metadata;
use crate::store::object_metadata::to_object_metadata;

fn hex_hash(hash: Hash) -> String {
    hex::encode(hash.data())
}

fn hex_partition(partition: Partition) -> String {
    hex::encode(partition.data())
}

fn hex_context(context: Context) -> String {
    hex::encode(context.data())
}

/// Whether a GCS failure means "retry me" as opposed to "this is a real, permanent failure" —
/// throttling, a transient `5xx`, or a transport-level hiccup. Mirrors
/// `lore_aws::store::immutable_store::is_dynamodb_overloaded`'s role for the AWS store: getting
/// this wrong is not cosmetic there, and is not here either — reporting an overload as a hard
/// failure denies a client the retry it would otherwise get, and reporting a real failure as
/// retryable can spin a caller against a request that will never succeed.
fn is_gcs_retryable(error: &google_cloud_storage::Error) -> bool {
    match error.http_status_code() {
        Some(429) => true,
        Some(code) if (500..600).contains(&code) => true,
        _ => error.is_timeout() || error.is_transport() || error.is_connect() || error.is_io(),
    }
}

/// Whether a Firestore failure means "retry me". `FirestoreDatabaseError::retry_possible` is the
/// `firestore` crate's own classification of the gRPC status it wrapped (throttling,
/// `UNAVAILABLE`, `DEADLINE_EXCEEDED`, ...), which is exactly the distinction
/// [`is_gcs_retryable`] draws by hand for GCS; `NetworkError` covers the transport-level failures
/// below the gRPC status layer (connection refused, DNS, ...), which are retryable by nature.
fn is_firestore_retryable(error: &firestore::errors::FirestoreError) -> bool {
    use firestore::errors::FirestoreError;
    match error {
        FirestoreError::DatabaseError(e) => e.retry_possible,
        FirestoreError::NetworkError(_) => true,
        _ => false,
    }
}

fn to_store_error_gcs(error: google_cloud_storage::Error, context: &'static str) -> StoreError {
    if is_gcs_retryable(&error) {
        StoreError::from(SlowDown)
    } else {
        StoreError::internal_with_context(GcpError::gcs(error), context)
    }
}

fn to_store_error(error: GcpError) -> StoreError {
    if let GcpError::Firestore(inner) = &error
        && is_firestore_retryable(inner)
    {
        return StoreError::from(SlowDown);
    }
    StoreError::internal_with_context(error, "GCP immutable store operation failed")
}

/// Whether a GCS/Firestore failure means "does not exist" as opposed to "operation failed".
fn is_not_found(error: &google_cloud_storage::Error) -> bool {
    error.http_status_code() == Some(404)
}

/// Where a payload is in its lifecycle. Identical in spirit to
/// `lore_aws::store::immutable_store::FragmentState`: the only thing Firestore records about a
/// payload, so that "does this hash exist, and may it be read" is answerable without a GCS
/// request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FragmentState {
    Stored,
    Obliterating,
    Obliterated,
}

impl FragmentState {
    fn from_bits(bits: u32) -> Self {
        if bits & FragmentFlags::PayloadObliterated == FragmentFlags::PayloadObliterated {
            Self::Obliterated
        } else if bits & FragmentFlags::PayloadObliterating == FragmentFlags::PayloadObliterating {
            Self::Obliterating
        } else {
            Self::Stored
        }
    }

    fn bits(self) -> u32 {
        match self {
            Self::Stored => 0,
            Self::Obliterating => FragmentFlags::PayloadObliterating.bits(),
            Self::Obliterated => FragmentFlags::PayloadObliterated.bits(),
        }
    }

    fn is_obliteration(self) -> bool {
        self != Self::Stored
    }
}

/// A row in the `fragment_state` collection.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
struct FragmentStateEntry {
    state: u32,
}

impl FragmentStateEntry {
    fn new(state: FragmentState) -> Self {
        Self {
            state: state.bits(),
        }
    }

    fn state(&self) -> FragmentState {
        FragmentState::from_bits(self.state)
    }
}

/// A row in the `fragment_associations` collection.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
struct AssociationEntry {
    hash: String,
    partition: String,
    context: String,
}

/// Mark a fragment as durably stored. Durability is a fact about this store, derived on read: an
/// object present in the bucket is durable by definition.
fn stored_durable(mut fragment: Fragment) -> Fragment {
    fragment.flags |= FragmentFlags::PayloadStoredDurable.bits();
    fragment
}

/// Configuration for the GCS half of the store.
///
/// Deliberately a plain struct rather than one that derives `Deserialize` itself: TOML defaulting
/// lives once, in `lore-server`'s `plugins::gcp::GcpImmutableStorePluginConfig` (mirroring how
/// `lore_aws::store::immutable_store::S3StoreSettings` is populated field-by-field from
/// `plugins::aws::AwsImmutableStorePluginConfig` rather than deserialized a second time).
#[derive(Clone, Debug)]
pub struct GcsStoreSettings {
    pub bucket: String,
    pub endpoint: Option<String>,
    pub slow_operation_threshold_millis: u64,
    pub timeout_millis: u64,
}

/// Configuration for the Firestore half of the immutable store. See [`GcsStoreSettings`] for why
/// this does not derive `Deserialize`.
#[derive(Clone, Debug)]
pub struct FirestoreImmutableStoreSettings {
    pub firestore_project: String,
    pub firestore_database: Option<String>,
    pub fragment_state_collection: String,
    pub fragment_associations_collection: String,
    pub slow_operation_threshold_millis: u64,
    pub timeout_millis: u64,
}

#[derive(Clone, Debug)]
pub struct GcpImmutableStoreSettings {
    pub gcs: GcsStoreSettings,
    pub firestore: FirestoreImmutableStoreSettings,
    pub force_write: bool,
}

impl GcpImmutableStoreSettings {
    pub fn new(
        gcs: GcsStoreSettings,
        firestore: FirestoreImmutableStoreSettings,
        force_write: bool,
    ) -> Self {
        Self {
            gcs,
            firestore,
            force_write,
        }
    }
}

/// Lower bound on the obliteration drain, regardless of how the Firestore timeout is configured.
/// See `lore_aws::store::immutable_store::MIN_OBLITERATION_DRAIN_MILLIS` for why this exists: a
/// put that had already passed its state probe needs time to land its own association before
/// obliteration re-checks whether any reference survives.
const MIN_OBLITERATION_DRAIN_MILLIS: u64 = 100;

static STORE_ATTRIBUTES_GCP: LazyLock<[KeyValue; 1]> =
    LazyLock::new(|| [KeyValue::new("store", "gcp")]);

struct GetGcsObjectContentsOutput {
    read: usize,
    bytes: BytesMut,
    fragment: Result<Fragment, super::object_metadata::ObjectMetadataError>,
}

pub struct GcpImmutableStore {
    storage: google_cloud_storage::client::Storage,
    control: google_cloud_storage::client::StorageControl,
    db: FirestoreDb,
    /// The bucket's resource name (`projects/_/buckets/{bucket}`), which is what every GCS call
    /// site actually needs; the plain bucket name from config does not outlive construction.
    bucket_resource: String,
    fragment_state_collection: Arc<str>,
    fragment_associations_collection: Arc<str>,
    force_write: bool,
    gcs_timeout: Duration,
    gcs_slow_threshold: Duration,
    firestore_timeout: Duration,
    firestore_slow_threshold: Duration,
    obliteration_drain: Duration,
    latency_histogram: Histogram<f64>,
    labels_get: LabelArray,
    labels_put: LabelArray,
    labels_obliterate: LabelArray,
    labels_copy: LabelArray,
    labels_get_metadata: LabelArray,
    labels_query: LabelArray,
    missing_payload_counter: Counter<u64>,
    association_without_state_counter: Counter<u64>,
}

impl GcpImmutableStore {
    pub fn new(
        storage: google_cloud_storage::client::Storage,
        control: google_cloud_storage::client::StorageControl,
        db: FirestoreDb,
        settings: &GcpImmutableStoreSettings,
    ) -> Self {
        let provider = GcpImmutableStoreInstrumentProvider;
        let latency_histogram =
            provider.latency_histogram_ms(METRICS_OPERATION_LATENCY_METRIC_NAME);
        let labels_get = provider.get_labels_for_operation_context("get");
        let labels_put = provider.get_labels_for_operation_context("put");
        let labels_obliterate = provider.get_labels_for_operation_context("obliterate");
        let labels_copy = provider.get_labels_for_operation_context("copy");
        let labels_get_metadata = provider.get_labels_for_operation_context("get_metadata");
        let labels_query = provider.get_labels_for_operation_context("query");
        let missing_payload_counter = provider.counter("missing_payload");
        let association_without_state_counter = provider.counter("association_without_state");

        Self {
            storage,
            control,
            db,
            bucket_resource: bucket_resource_name(&settings.gcs.bucket),
            fragment_state_collection: Arc::from(
                settings.firestore.fragment_state_collection.as_str(),
            ),
            fragment_associations_collection: Arc::from(
                settings.firestore.fragment_associations_collection.as_str(),
            ),
            force_write: settings.force_write,
            gcs_timeout: Duration::from_millis(settings.gcs.timeout_millis.max(1)),
            gcs_slow_threshold: Duration::from_millis(settings.gcs.slow_operation_threshold_millis),
            firestore_timeout: Duration::from_millis(settings.firestore.timeout_millis.max(1)),
            firestore_slow_threshold: Duration::from_millis(
                settings.firestore.slow_operation_threshold_millis,
            ),
            obliteration_drain: Duration::from_millis(
                settings
                    .firestore
                    .timeout_millis
                    .max(MIN_OBLITERATION_DRAIN_MILLIS),
            ),
            latency_histogram,
            labels_get,
            labels_put,
            labels_obliterate,
            labels_copy,
            labels_get_metadata,
            labels_query,
            missing_payload_counter,
            association_without_state_counter,
        }
    }

    /// Run a Firestore future, bounded by this store's configured Firestore timeout/slow
    /// threshold, and classify its error as a `StoreError` (retryable failures become
    /// [`StoreError::SlowDown`]; see [`is_firestore_retryable`]).
    async fn firestore_op<T>(
        &self,
        op: &'static str,
        fut: impl Future<Output = firestore::FirestoreResult<T>>,
    ) -> Result<T, StoreError> {
        bounded(
            self.firestore_timeout,
            self.firestore_slow_threshold,
            op,
            fut,
        )
        .await?
        .map_err(GcpError::firestore)
        .map_err(to_store_error)
    }

    /// Run a GCS future, bounded by this store's configured GCS timeout/slow threshold. Unlike
    /// [`Self::firestore_op`], this leaves the raw `google_cloud_storage::Error` for the caller:
    /// several GCS call sites need to distinguish "not found" from other failures themselves
    /// (`head_fragment`, `get_gcs_object_contents`, `delete_payload`'s best-effort deletes).
    async fn gcs_op<T>(
        &self,
        op: &'static str,
        fut: impl Future<Output = Result<T, google_cloud_storage::Error>>,
    ) -> Result<T, StoreError> {
        match bounded(self.gcs_timeout, self.gcs_slow_threshold, op, fut).await? {
            Ok(value) => Ok(value),
            Err(error) => Err(to_store_error_gcs(error, "GCS operation failed")),
        }
    }

    fn association_doc_id(hash: Hash, partition: Partition, context: Context) -> String {
        format!(
            "{}_{}_{}",
            hex_hash(hash),
            hex_partition(partition),
            hex_context(context)
        )
    }

    /// Whether this partition holds the exact association for this address. This store isolates
    /// partitions, so it reads no wider than the exact association.
    async fn exists(&self, partition: Partition, address: Address) -> Result<bool, StoreError> {
        let doc_id = Self::association_doc_id(address.hash, partition, address.context);
        let entry: Option<AssociationEntry> = self
            .firestore_op(
                "fragment_associations.get",
                self.db
                    .fluent()
                    .select()
                    .by_id_in(self.fragment_associations_collection.as_ref())
                    .obj()
                    .one(&doc_id),
            )
            .await?;

        Ok(entry.is_some())
    }

    /// The addresses among `addresses` that this partition holds an association for.
    async fn associations_present(
        &self,
        partition: Partition,
        addresses: &[Address],
    ) -> Result<HashSet<Address>, StoreError> {
        let distinct: HashSet<Address> = addresses.iter().copied().collect();
        if distinct.is_empty() {
            return Ok(HashSet::new());
        }

        let doc_ids: Vec<(Address, String)> = distinct
            .iter()
            .map(|address| {
                (
                    *address,
                    Self::association_doc_id(address.hash, partition, address.context),
                )
            })
            .collect();
        let ids: Vec<String> = doc_ids.iter().map(|(_, id)| id.clone()).collect();

        let found_ids = bounded(
            self.firestore_timeout,
            self.firestore_slow_threshold,
            "fragment_associations.batch_get",
            async {
                let mut stream = self
                    .db
                    .fluent()
                    .select()
                    .by_id_in(self.fragment_associations_collection.as_ref())
                    .obj::<AssociationEntry>()
                    .batch(ids)
                    .await
                    .map_err(GcpError::firestore)
                    .map_err(to_store_error)?;

                let mut found_ids = HashSet::new();
                while let Some((doc_id, entry)) = stream.next().await {
                    if entry.is_some() {
                        found_ids.insert(doc_id);
                    }
                }
                Ok::<_, StoreError>(found_ids)
            },
        )
        .await??;

        Ok(doc_ids
            .into_iter()
            .filter(|(_, id)| found_ids.contains(id))
            .map(|(address, _)| address)
            .collect())
    }

    async fn associate_fragment(
        &self,
        partition: Partition,
        address: Address,
    ) -> Result<(), StoreError> {
        let doc_id = Self::association_doc_id(address.hash, partition, address.context);
        let entry = AssociationEntry {
            hash: hex_hash(address.hash),
            partition: hex_partition(partition),
            context: hex_context(address.context),
        };

        self.firestore_op::<AssociationEntry>(
            "fragment_associations.set",
            self.db
                .fluent()
                .update()
                .in_col(self.fragment_associations_collection.as_ref())
                .document_id(&doc_id)
                .object(&entry)
                .execute(),
        )
        .await?;

        Ok(())
    }

    async fn delete_association(
        &self,
        partition: Partition,
        address: Address,
    ) -> Result<(), StoreError> {
        let doc_id = Self::association_doc_id(address.hash, partition, address.context);

        self.firestore_op(
            "fragment_associations.delete",
            self.db
                .fluent()
                .delete()
                .from(self.fragment_associations_collection.as_ref())
                .document_id(&doc_id)
                .execute(),
        )
        .await?;

        Ok(())
    }

    /// Whether `partition` holds any association for `hash`, whatever context it is under. Used
    /// only by [`ImmutableStoreTrait::copy`] when its source names no context.
    async fn has_partition_association(
        &self,
        partition: Partition,
        hash: Hash,
    ) -> Result<bool, StoreError> {
        let hash_hex = hex_hash(hash);
        let partition_hex = hex_partition(partition);

        let results: Vec<AssociationEntry> = self
            .firestore_op(
                "fragment_associations.query_by_partition",
                self.db
                    .fluent()
                    .select()
                    .from(self.fragment_associations_collection.as_ref())
                    .filter(|q| {
                        q.for_all([
                            q.field("hash").eq(hash_hex.clone()),
                            q.field("partition").eq(partition_hex.clone()),
                        ])
                    })
                    .limit(1)
                    .obj()
                    .query(),
            )
            .await?;

        Ok(!results.is_empty())
    }

    /// Whether any partition/context still references `hash`.
    async fn has_associations(&self, hash: Hash) -> Result<bool, StoreError> {
        let hash_hex = hex_hash(hash);

        let results: Vec<AssociationEntry> = self
            .firestore_op(
                "fragment_associations.query_by_hash",
                self.db
                    .fluent()
                    .select()
                    .from(self.fragment_associations_collection.as_ref())
                    .filter(|q| q.for_all([q.field("hash").eq(hash_hex.clone())]))
                    .limit(1)
                    .obj()
                    .query(),
            )
            .await?;

        Ok(!results.is_empty())
    }

    /// Read the lifecycle state of a hash. `None` means no document exists, so the hash is
    /// unknown.
    pub(crate) async fn load_state(&self, hash: Hash) -> Result<Option<FragmentState>, StoreError> {
        let entry: Option<FragmentStateEntry> = self
            .firestore_op(
                "fragment_state.get",
                self.db
                    .fluent()
                    .select()
                    .by_id_in(self.fragment_state_collection.as_ref())
                    .obj()
                    .one(&hex_hash(hash)),
            )
            .await?;

        Ok(entry.map(|entry| entry.state()))
    }

    /// The lifecycle state of each distinct hash among `addresses`, where a document exists.
    async fn states_for(
        &self,
        addresses: &[Address],
    ) -> Result<HashMap<Hash, FragmentState>, StoreError> {
        let distinct: HashSet<Hash> = addresses.iter().map(|address| address.hash).collect();
        if distinct.is_empty() {
            return Ok(HashMap::new());
        }

        // A lookup table from the hex doc id back to the `Hash` it names, built once so the
        // stream-draining loop below is O(1) per item rather than a linear scan
        // (`associations_present` already does this correctly with a `HashSet`; this mirrors it).
        let by_doc_id: HashMap<String, Hash> = distinct
            .iter()
            .map(|hash| (hex_hash(*hash), *hash))
            .collect();
        let id_strings: Vec<String> = by_doc_id.keys().cloned().collect();

        bounded(
            self.firestore_timeout,
            self.firestore_slow_threshold,
            "fragment_state.batch_get",
            async {
                let mut stream = self
                    .db
                    .fluent()
                    .select()
                    .by_id_in(self.fragment_state_collection.as_ref())
                    .obj::<FragmentStateEntry>()
                    .batch(id_strings)
                    .await
                    .map_err(GcpError::firestore)
                    .map_err(to_store_error)?;

                let mut states = HashMap::new();
                while let Some((doc_id, entry)) = stream.next().await {
                    if let Some(entry) = entry
                        && let Some(hash) = by_doc_id.get(&doc_id)
                    {
                        states.insert(*hash, entry.state());
                    }
                }
                Ok::<_, StoreError>(states)
            },
        )
        .await?
    }

    /// Record that a payload exists, without disturbing an obliteration that may hold the hash.
    /// Runs in a transaction so a concurrent obliteration's mark cannot be silently overwritten:
    /// the write only happens if no document exists yet, mirroring
    /// `lore_aws::store::immutable_store::RowAbsent`.
    async fn publish_state(&self, hash: Hash) -> Result<FragmentState, StoreError> {
        let collection = self.fragment_state_collection.clone();
        let doc_id = hex_hash(hash);

        bounded(
            self.firestore_timeout,
            self.firestore_slow_threshold,
            "fragment_state.publish_transaction",
            self.db
                .run_transaction::<FragmentState, _, GcpError>(move |db, transaction| {
                    let collection = collection.clone();
                    let doc_id = doc_id.clone();
                    async move {
                        let current: Option<FragmentStateEntry> = db
                            .fluent()
                            .select()
                            .by_id_in(collection.as_ref())
                            .obj()
                            .one(&doc_id)
                            .await
                            .map_err(GcpError::firestore)
                            .map_err(BackoffError::Permanent)?;

                        if let Some(current) = current {
                            return Ok(current.state());
                        }

                        let entry = FragmentStateEntry::new(FragmentState::Stored);
                        db.fluent()
                            .update()
                            .in_col(collection.as_ref())
                            .document_id(&doc_id)
                            .object(&entry)
                            .add_to_transaction(transaction)
                            .map_err(GcpError::firestore)
                            .map_err(BackoffError::Permanent)?;

                        Ok(FragmentState::Stored)
                    }
                    .boxed()
                }),
        )
        .await?
        .map_err(GcpError::firestore)
        .map_err(to_store_error)
    }

    /// Delete the state document for a hash, so the next put treats it as new content. Only
    /// called for a payload GCS has lost.
    async fn clear_state(&self, hash: Hash) -> Result<(), StoreError> {
        self.firestore_op(
            "fragment_state.delete",
            self.db
                .fluent()
                .delete()
                .from(self.fragment_state_collection.as_ref())
                .document_id(hex_hash(hash))
                .execute(),
        )
        .await?;

        Ok(())
    }

    /// Move the state document from one state to another, failing if it has moved underneath us.
    /// Obliteration uses this to take and release the mark.
    async fn advance_state(
        &self,
        hash: Hash,
        expected: FragmentState,
        updated: FragmentState,
    ) -> Result<(), StoreError> {
        let collection = self.fragment_state_collection.clone();
        let doc_id = hex_hash(hash);

        bounded(
            self.firestore_timeout,
            self.firestore_slow_threshold,
            "fragment_state.advance_transaction",
            self.db
                .run_transaction::<(), _, GcpError>(move |db, transaction| {
                    let collection = collection.clone();
                    let doc_id = doc_id.clone();
                    async move {
                        let current: Option<FragmentStateEntry> = db
                            .fluent()
                            .select()
                            .by_id_in(collection.as_ref())
                            .obj()
                            .one(&doc_id)
                            .await
                            .map_err(GcpError::firestore)
                            .map_err(BackoffError::Permanent)?;

                        if current.map(|entry| entry.state()) != Some(expected) {
                            return Err(BackoffError::Permanent(GcpError::Decode(format!(
                                "fragment state for {doc_id} was not {expected:?} when moving to \
                                 {updated:?}"
                            ))));
                        }

                        let entry = FragmentStateEntry::new(updated);
                        db.fluent()
                            .update()
                            .in_col(collection.as_ref())
                            .document_id(&doc_id)
                            .object(&entry)
                            .add_to_transaction(transaction)
                            .map_err(GcpError::firestore)
                            .map_err(BackoffError::Permanent)?;

                        Ok(())
                    }
                    .boxed()
                }),
        )
        .await?
        .map_err(|err| match err {
            firestore::errors::FirestoreError::ErrorInTransaction(inner) => {
                warn!(%inner, "Failed to update fragment state due to conflict");
                StoreError::internal("Failed to update fragment state due to conflict")
            }
            other => to_store_error(GcpError::firestore(other)),
        })
    }

    /// Move a tombstoned hash back to stored, now that its payload has been uploaded again.
    async fn revive_state(&self, hash: Hash) -> Result<(), StoreError> {
        if self
            .advance_state(hash, FragmentState::Obliterated, FragmentState::Stored)
            .await
            .is_ok()
        {
            return Ok(());
        }

        match self.load_state(hash).await? {
            Some(FragmentState::Stored) => {
                debug!(%hash, "Another writer revived this hash first");
                Ok(())
            }
            state => {
                info!(%hash, ?state, "Hash is no longer revivable, asking the caller to retry");
                Err(StoreError::from(SlowDown))
            }
        }
    }

    /// Record, and make repairable, a hash that is still referenced but whose payload is gone.
    async fn report_missing_payload(&self, address: Address, labels: &[KeyValue]) {
        self.missing_payload_counter.add(1, labels);
        error!(
            %address,
            "Fragment is referenced by a partition but absent from GCS; content for this hash \
             has been lost. Clearing its state so the content can be stored again."
        );

        match self.load_state(address.hash).await {
            Ok(Some(FragmentState::Stored)) => {
                if let Err(error) = self.clear_state(address.hash).await {
                    warn!(%address, ?error, "Failed to clear state for a lost payload");
                }
            }
            Ok(state) => {
                debug!(%address, ?state, "Leaving state alone for a lost payload");
            }
            Err(error) => {
                warn!(%address, ?error, "Failed to read state for a lost payload");
            }
        }
    }

    async fn write_payload_and_state(
        &self,
        hash: Hash,
        fragment: Fragment,
        payload: Bytes,
    ) -> Result<(), StoreError> {
        if payload.len() != fragment.size_payload as usize {
            warn!(
                expected_size = fragment.size_payload,
                received_size = payload.len(),
                %hash,
                "Failed to write fragment to immutable store for hash: payload size invalid"
            );
            return Err(StoreError::internal(format!(
                "Failed to store in immutable store for put {hash}"
            )));
        }

        let object_name = hex_hash(hash);
        self.gcs_op(
            "write_object",
            self.storage
                .write_object(&self.bucket_resource, object_name.clone(), payload)
                .set_metadata(to_object_metadata(&fragment))
                // The payload is an opaque, content-addressed fragment (compressed or not), never
                // a document a browser or CDN should render — the same value GCS itself defaults
                // an object's content type to when a request never names one. Naming it
                // explicitly is more honest about intent than leaning on that default, though it
                // is not a complete workaround for the Google Cloud Storage testbench
                // incompatibility documented in the module docs above: `google-cloud-storage`
                // 1.18.0 never attaches a `Content-Type` header to the multipart request's raw
                // "media" part regardless of this setting (it only affects the JSON "metadata"
                // part's `contentType` field, a different thing the testbench does not consult
                // for this check), so the testbench still answers a multipart upload with a 500.
                .set_content_type("application/octet-stream")
                // Content-addressed: the same bytes always hash to the same object name, so a
                // retried upload after a transient network failure is always safe to repeat.
                .with_idempotency(true)
                .send_unbuffered(),
        )
        .await
        .inspect_err(|error| {
            warn!(?error, %hash, %object_name, "Failed to write payload for hash");
        })?;

        match self.publish_state(hash).await? {
            FragmentState::Stored => {}
            FragmentState::Obliterating => {
                info!(
                    %hash,
                    "Payload was uploaded while an obliteration holds the hash; leaving it \
                     unassociated and asking the caller to retry"
                );
                return Err(StoreError::from(SlowDown));
            }
            FragmentState::Obliterated => {
                info!(%hash, "Payload revives a tombstoned hash");
                self.revive_state(hash).await?;
            }
        }

        Ok(())
    }

    /// Permanently delete a payload from GCS by removing every generation from the bucket.
    async fn delete_payload(&self, hash: Hash) -> Result<(), StoreError> {
        let object_name = hex_hash(hash);

        let mut generations = Vec::new();
        let mut items = self
            .control
            .list_objects()
            .set_parent(&self.bucket_resource)
            .set_prefix(&object_name)
            .set_versions(true)
            .by_item();
        loop {
            match bounded(
                self.gcs_timeout,
                self.gcs_slow_threshold,
                "list_objects.next",
                items.next(),
            )
            .await?
            {
                Some(Ok(object)) if object.name == object_name => {
                    generations.push(object.generation);
                }
                Some(Ok(_other)) => {
                    // The prefix matched, but the name is not an exact match. Fixed-length,
                    // hex-encoded hashes make this impossible in practice, but it costs nothing
                    // to skip rather than assume.
                }
                Some(Err(error)) => {
                    warn!(?error, %hash, "Failed to list versions for hash");
                    return Err(to_store_error_gcs(error, "GCS list object versions failed"));
                }
                None => break,
            }
        }

        if generations.is_empty() {
            // Either the bucket is not versioned, or the object is already gone. A best-effort
            // delete of the live object covers the first case; a not-found is not an error here,
            // since obliterate is meant to leave no payload behind either way. Bounded directly
            // (rather than through `gcs_op`) so the raw error is still available to test for
            // not-found — `gcs_op`'s conversion to `StoreError` collapses that distinction away.
            match bounded(
                self.gcs_timeout,
                self.gcs_slow_threshold,
                "delete_object",
                self.control
                    .delete_object()
                    .set_bucket(&self.bucket_resource)
                    .set_object(&object_name)
                    .send(),
            )
            .await?
            {
                Ok(()) => {}
                Err(error) if is_not_found(&error) => {}
                Err(error) => {
                    warn!(?error, %hash, "Failed to delete payload for hash");
                    return Err(to_store_error_gcs(error, "GCS delete object failed"));
                }
            }
            return Ok(());
        }

        for generation in generations {
            match bounded(
                self.gcs_timeout,
                self.gcs_slow_threshold,
                "delete_object_generation",
                self.control
                    .delete_object()
                    .set_bucket(&self.bucket_resource)
                    .set_object(&object_name)
                    .set_generation(generation)
                    .send(),
            )
            .await?
            {
                Ok(()) => {}
                Err(error) if is_not_found(&error) => {}
                Err(error) => {
                    warn!(?error, %hash, generation, "Failed to delete payload generation for hash");
                    return Err(to_store_error_gcs(
                        error,
                        "GCS delete object generation failed",
                    ));
                }
            }
        }

        Ok(())
    }

    /// Read a fragment without its payload, from the object's own custom metadata. This is the
    /// one path that spends a GCS request purely on metadata (`StorageControl::get_object`,
    /// which transfers no body).
    async fn head_fragment(&self, hash: Hash) -> Result<Fragment, StoreError> {
        let object_name = hex_hash(hash);
        let object = bounded(
            self.gcs_timeout,
            self.gcs_slow_threshold,
            "get_object",
            self.control
                .get_object()
                .set_bucket(&self.bucket_resource)
                .set_object(&object_name)
                .send(),
        )
        .await?
        .map_err(|error| {
            if is_not_found(&error) {
                debug!(%hash, "head_fragment: object not found");
                StoreError::from(AddressNotFound::from(Address::zero_context_hash(hash)))
            } else {
                to_store_error_gcs(error, "GCS get object failed")
            }
        })?;

        let fragment = from_object_metadata(&object.metadata).map_err(|e| {
            warn!(%hash, "Stored object carries unusable or absent fragment metadata: {e}");
            StoreError::internal_with_context(e, "GCS object carries no usable fragment metadata")
        })?;

        Ok(stored_durable(fragment))
    }

    async fn get_gcs_object_contents(
        &self,
        hash: Hash,
    ) -> Result<GetGcsObjectContentsOutput, StoreError> {
        let object_name = hex_hash(hash);
        let mut response = bounded(
            self.gcs_timeout,
            self.gcs_slow_threshold,
            "read_object.send",
            self.storage
                .read_object(&self.bucket_resource, &object_name)
                .send(),
        )
        .await?
        .map_err(|error| {
            if is_not_found(&error) {
                debug!(%hash, "get_gcs_object_contents: object not found");
                StoreError::from(AddressNotFound::from(Address::zero_context_hash(hash)))
            } else {
                to_store_error_gcs(error, "GCS get object failed")
            }
        })?;

        let object = response.object();

        // The hard cap this store ever allows an object to be. Checked twice: once here, against
        // the size GCS *declares* up front (a cheap early exit for the common case), and again,
        // unconditionally, inside the streaming loop below against the bytes actually received —
        // the declared size is attacker/corruption-influenced input, exactly like any other
        // metadata this store reads, and must never be the only thing standing between a
        // malicious or corrupted object and an unbounded read into memory.
        const MAX_OBJECT_SIZE: usize = FRAGMENT_SIZE_THRESHOLD + std::mem::size_of::<Fragment>();
        let declared_size = usize::try_from(object.size).ok().filter(|size| *size > 0);
        if let Some(size) = declared_size
            && size > MAX_OBJECT_SIZE
        {
            warn!(
                %hash,
                size,
                max = MAX_OBJECT_SIZE,
                "GCS object exceeds maximum allowed size; treating as malicious"
            );
            return Err(StoreError::from(Oversized {
                context: format!(
                    "GCS object size {size} for {hash} exceeds maximum {MAX_OBJECT_SIZE}"
                ),
            }));
        }

        let fragment = from_object_metadata(&object.metadata);
        if let Ok(fragment) = &fragment {
            lore_storage::validate_fragment_size(fragment)?;
        }

        let capacity = declared_size.map_or(MAX_OBJECT_SIZE, |size| size.min(MAX_OBJECT_SIZE));
        let mut buffer = BytesMut::with_capacity(capacity);
        let mut read = 0usize;
        loop {
            let next = bounded(
                self.gcs_timeout,
                self.gcs_slow_threshold,
                "read_object.chunk",
                response.next(),
            )
            .await?;
            let Some(chunk) = next else { break };
            let chunk = chunk.map_err(|error| {
                warn!("Failed to read bytes from GCS response for key: {hash}: {error:?}");
                to_store_error_gcs(error, "Failed to read bytes from GCS response stream")
            })?;
            read += chunk.len();

            // Enforced unconditionally, independent of whatever `object.size` claimed above: a
            // stream that keeps sending bytes past the declared length (or one seen when
            // `declared_size` was `None` because the field was absent, zero, or unparsable) must
            // still be cut off here rather than buffered without limit.
            if read > MAX_OBJECT_SIZE {
                warn!(
                    %hash,
                    read,
                    max = MAX_OBJECT_SIZE,
                    "GCS object exceeded maximum allowed size while streaming; aborting read"
                );
                return Err(StoreError::from(Oversized {
                    context: format!(
                        "GCS object for {hash} exceeded maximum size {MAX_OBJECT_SIZE} while \
                         streaming"
                    ),
                }));
            }

            trace!("Read {read} bytes from GCS stream");
            buffer.extend_from_slice(chunk.as_ref());
        }

        Ok(GetGcsObjectContentsOutput {
            bytes: buffer,
            read,
            fragment,
        })
    }

    fn read_payload(
        contents: GetGcsObjectContentsOutput,
        hash: Hash,
        fragment: Fragment,
    ) -> Result<Bytes, StoreError> {
        let payload_size = fragment.size_payload as usize;
        let buffer_size = contents.bytes.len();

        if buffer_size == payload_size {
            Ok(contents.bytes.freeze())
        } else {
            warn!(
                "Wrong number of bytes read from payload, expected {payload_size} but got {buffer_size}, from a total of {} bytes read",
                contents.read
            );
            Err(StoreError::internal(format!(
                "Failed to load from immutable store, size mismatch (load {buffer_size}, expected {payload_size}) for get {hash}"
            )))
        }
    }

    pub(crate) async fn load(&self, hash: Hash) -> Result<(Fragment, Bytes), StoreError> {
        let contents = self.get_gcs_object_contents(hash).await?;

        let fragment = contents.fragment.map_err(|e| {
            warn!(%hash, "Stored object carries unusable or absent fragment metadata: {e}");
            StoreError::internal_with_context(e, "GCS object carries no usable fragment metadata")
        })?;

        let fragment = stored_durable(fragment);
        lore_storage::validate_fragment_size(&fragment)?;

        let payload = Self::read_payload(contents, hash, fragment)?;
        Ok((fragment, payload))
    }

    /// Obliterate the fragments a fragmented payload points at, if it is one.
    async fn obliterate_sub_fragments(
        self: Arc<Self>,
        partition: Partition,
        address: Address,
        stats: Arc<StoreObliterateStats>,
    ) -> Result<(), StoreError> {
        let (fragment, payload) = match self.load(address.hash).await {
            Ok(loaded) => loaded,
            Err(e) if e.is_address_not_found() => {
                info!("Payload for {address} is already gone, no sub-fragments to obliterate");
                return Ok(());
            }
            Err(e) => return Err(e),
        };

        if fragment.flags & FragmentFlags::PayloadFragmented == 0 {
            return Ok(());
        }

        let payload = payload.to_aligned::<FragmentReference>();
        let sub_fragments = payload.as_type_slice::<FragmentReference>();
        info!(
            "Fragment {address} has {} sub-fragments",
            sub_fragments.len()
        );

        let span = tracing::Span::current();
        let mut join_set = JoinSet::new();
        for reference in sub_fragments.iter() {
            let self_clone = self.clone();
            let stats = stats.clone();
            let sub_address = Address {
                hash: reference.hash,
                context: address.context,
            };

            lore_base::lore_spawn!(
                join_set,
                async move {
                    self_clone
                        .obliterate(partition, sub_address, stats)
                        .await
                        .map_err(|e| (sub_address, e))
                }
                .instrument(span.clone())
            );
        }

        let mut failures = false;
        while let Some(result) = join_set.join_next().await {
            match result {
                Err(e) => {
                    failures = true;
                    warn!("Failed to join task for fragment reference obliterate: {e:?}");
                }
                Ok(Err((sub_address, e))) => {
                    failures = true;
                    warn!("Obliteration failed for sub-fragment {sub_address}: {e:?}");
                }
                Ok(Ok(())) => {}
            }
        }

        if failures {
            return Err(StoreError::internal(format!(
                "Failed to obliterate immutable {address}"
            )));
        }

        Ok(())
    }

    async fn do_query_batch(
        &self,
        partition: Partition,
        addresses: &[Address],
        results: &mut [StoreMatchResult],
    ) -> Result<(), StoreError> {
        let (associations, states) = tokio::join!(
            self.associations_present(partition, addresses),
            self.states_for(addresses)
        );
        let associations = associations?;
        let states = states?;

        for (address, result) in addresses.iter().zip(results.iter_mut()) {
            if !associations.contains(address) {
                *result = StoreMatchResult::default();
                continue;
            }

            let state = states.get(&address.hash).copied();
            if state.is_none() {
                self.association_without_state_counter
                    .add(1, &self.labels_query);
                trace!("Query found an association at {address} with no stored payload");
            }

            if !matches!(state, Some(FragmentState::Stored)) {
                *result = StoreMatchResult::default();
                continue;
            }

            *result = StoreMatchResult {
                match_made: StoreMatch::MatchFull,
                partition,
                context: address.context,
                stored_local: false,
                stored_durable: true,
            };
        }

        Ok(())
    }

    // Note: the `get`/`query`/`get_metadata` read-inconsistency during an obliteration's drain
    // window (see the module docs) means `do_query`/`do_query_batch` and `Self::get`
    // (`ImmutableStoreTrait::get`, below) can disagree about the same address for a short window;
    // that is the same behavior `lore_aws` has, not a regression introduced here.
    async fn do_query(
        &self,
        partition: Partition,
        address: Address,
        labels: &[KeyValue],
    ) -> Result<StoreMatchResult, StoreError> {
        let (associated, state) = tokio::join!(
            self.exists(partition, address),
            self.load_state(address.hash)
        );

        if !associated? {
            return Ok(StoreMatchResult::default());
        }

        match state? {
            Some(FragmentState::Stored) => Ok(StoreMatchResult {
                match_made: StoreMatch::MatchFull,
                partition,
                context: address.context,
                stored_local: false,
                stored_durable: true,
            }),
            Some(FragmentState::Obliterating | FragmentState::Obliterated) => {
                trace!("Query found obliterated fragment at address {address}");
                Ok(StoreMatchResult::default())
            }
            None => {
                self.association_without_state_counter.add(1, labels);
                trace!("Query found an association at {address} with no stored payload");
                Ok(StoreMatchResult::default())
            }
        }
    }
}

#[async_trait]
impl ImmutableStoreTrait for GcpImmutableStore {
    fn isolates_partitions(&self) -> bool {
        true
    }

    fn query_scope(&self) -> StoreMatch {
        StoreMatch::MatchFull
    }

    #[lore_macro::lore_instrument]
    #[tracing::instrument(name = "GcpImmutableStore::query" skip(self))]
    async fn query(
        self: Arc<Self>,
        partition: Partition,
        addresses: &[Address],
        results: &mut [StoreMatchResult],
    ) -> Result<(), StoreError> {
        debug_assert_eq!(addresses.len(), results.len());

        timed!(self.latency_histogram, &self.labels_query, {
            self.do_query_batch(partition, addresses, results).await
        })
        .into()
    }

    #[lore_macro::lore_instrument]
    #[tracing::instrument(name = "GcpImmutableStore::get_metadata" skip(self))]
    async fn get_metadata(
        self: Arc<Self>,
        partition: Partition,
        address: Address,
    ) -> Result<StoreGetData, StoreError> {
        timed!(self.latency_histogram, &self.labels_get_metadata, {
            let query_result = self
                .do_query(partition, address, &self.labels_get_metadata)
                .await?;

            if query_result.match_made == StoreMatch::MatchNone {
                return Ok(StoreGetData::default());
            }

            match self.head_fragment(address.hash).await {
                Ok(fragment) => Ok(StoreGetData::metadata(
                    fragment,
                    query_result.match_made,
                    partition,
                )),
                Err(e) if e.is_address_not_found() => {
                    self.report_missing_payload(address, &self.labels_get_metadata)
                        .await;
                    Ok(StoreGetData::default())
                }
                Err(e) => Err(e),
            }
        })
        .into()
    }

    #[lore_macro::lore_instrument]
    #[tracing::instrument(name = "GcpImmutableStore::get" skip(self))]
    async fn get(
        self: Arc<Self>,
        partition: Partition,
        address: Address,
    ) -> Result<StoreGetData, StoreError> {
        let result: Result<(Fragment, Bytes), StoreError> =
            timed!(self.latency_histogram, &self.labels_get, {
                if !self.exists(partition, address).await? {
                    return Err(StoreError::from(AddressNotFound::from(address)));
                }

                let load_result = self.load(address.hash).await;
                if load_result
                    .as_ref()
                    .err()
                    .is_some_and(StoreError::is_address_not_found)
                {
                    self.report_missing_payload(address, &self.labels_get).await;
                }
                load_result
            })
            .into();
        let (fragment, payload) = result?;
        lore_storage::validate_fragment_payload(&fragment, payload.len())?;
        Ok(StoreGetData {
            fragment,
            match_made: StoreMatch::MatchFull,
            partition,
            payload: Some(payload),
        })
    }

    #[lore_macro::lore_instrument]
    #[tracing::instrument(name = "GcpImmutableStore::put" skip(self, fragment, payload))]
    async fn put(
        self: Arc<Self>,
        partition: Partition,
        address: Address,
        mut fragment: Fragment,
        payload: Option<Bytes>,
        _force: bool,
    ) -> Result<(), StoreError> {
        sanitise_fragment_behavior_flags(&mut fragment);

        if let Some(payload) = payload.as_ref() {
            lore_storage::validate_fragment_payload(&fragment, payload.len())?;
        } else {
            lore_storage::validate_fragment_size(&fragment)?;
        }

        timed!(self.latency_histogram, &self.labels_put, {
            let probe = if self.force_write {
                (None, false)
            } else {
                let (associated, state) = tokio::join!(
                    self.exists(partition, address),
                    self.load_state(address.hash)
                );
                (state?, associated?)
            };

            match probe {
                (Some(FragmentState::Obliterating), _) => {
                    debug!(
                        "Received request to put fragment at {address} that is in the process of \
                         being obliterated"
                    );
                    Err(StoreError::from(SlowDown))
                }
                (Some(FragmentState::Stored), true) => Ok(()),
                (Some(FragmentState::Stored), false) if payload.is_some() => {
                    self.associate_fragment(partition, address).await
                }
                (Some(FragmentState::Stored), false) => {
                    Err(StoreError::internal("Payload buffer required"))
                }
                _ => match payload {
                    Some(payload) => {
                        self.write_payload_and_state(address.hash, fragment, payload)
                            .await?;
                        self.associate_fragment(partition, address).await?;
                        Ok(())
                    }
                    None => Err(StoreError::internal("Payload buffer required")),
                },
            }
        })
        .into()
    }

    #[lore_macro::lore_instrument]
    #[tracing::instrument(name = "GcpImmutableStore::obliterate" skip(self, stats))]
    async fn obliterate(
        self: Arc<Self>,
        partition: Partition,
        address: Address,
        stats: Arc<StoreObliterateStats>,
    ) -> Result<(), StoreError> {
        timed!(self.latency_histogram, &self.labels_obliterate, {
            let span = tracing::Span::current();

            let Some(state) = self
                .load_state(address.hash)
                .instrument(span.clone())
                .await?
            else {
                info!("No fragment state for {address}, nothing to obliterate");
                return Ok(());
            };

            if state.is_obliteration() {
                info!("Fragment {address} is already being, or has already been, obliterated");
                return Ok(());
            }

            self.delete_association(partition, address)
                .instrument(span.clone())
                .await?;
            stats
                .num_fragments
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

            self.advance_state(
                address.hash,
                FragmentState::Stored,
                FragmentState::Obliterating,
            )
            .instrument(span.clone())
            .await?;

            self.delete_association(partition, address)
                .instrument(span.clone())
                .await?;

            tokio::time::sleep(self.obliteration_drain).await;

            if self
                .has_associations(address.hash)
                .instrument(span.clone())
                .await?
            {
                info!("Fragment still associated, releasing the obliteration mark");
                return self
                    .advance_state(
                        address.hash,
                        FragmentState::Obliterating,
                        FragmentState::Stored,
                    )
                    .instrument(span.clone())
                    .await
                    .inspect_err(|e| {
                        warn!("Failed to release the obliteration mark: {e:?}");
                    });
            }

            self.clone()
                .obliterate_sub_fragments(partition, address, stats.clone())
                .instrument(span.clone())
                .await?;

            self.delete_payload(address.hash)
                .instrument(span.clone())
                .await?;

            stats
                .num_payloads
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

            self.advance_state(
                address.hash,
                FragmentState::Obliterating,
                FragmentState::Obliterated,
            )
            .await
            .inspect_err(|e| {
                warn!("Failed to finalize obliterate for {address}: {e:?}");
            })
        })
        .into()
    }

    // Note: this is not atomic with a concurrent `obliterate` of the same source hash — see the
    // "copy-during-obliteration dangling-association race" entry in the module docs. Shared with
    // `lore_aws`, not a GCP-specific gap.
    #[lore_macro::lore_instrument]
    #[tracing::instrument(name = "GcpImmutableStore::copy" skip(self))]
    async fn copy(
        self: Arc<Self>,
        source_partition: Partition,
        source_address: Address,
        destination_partition: Partition,
        destination_context: Context,
        _durable: bool,
    ) -> Result<(), StoreError> {
        let destination_address = Address {
            hash: source_address.hash,
            context: destination_context,
        };
        timed!(self.latency_histogram, &self.labels_copy, {
            let present = if source_address.context.is_zero() {
                self.has_partition_association(source_partition, source_address.hash)
                    .await?
            } else {
                self.exists(source_partition, source_address).await?
            };
            if !present {
                return Err(StoreError::from(AddressNotFound::from(source_address)));
            }

            self.associate_fragment(destination_partition, destination_address)
                .await
        })
        .into()
    }

    async fn evict(
        self: Arc<Self>,
        _max_capacity: usize,
        _sync_data: bool,
        _sink: Option<lore_storage::gc_event::GcEventSinkRef>,
    ) -> Result<usize, StoreError> {
        Ok(0)
    }

    async fn compact(
        self: Arc<Self>,
        _max_size: usize,
        _at: Option<usize>,
        _sync_data: bool,
        _sink: Option<lore_storage::gc_event::GcEventSinkRef>,
    ) -> Result<Option<usize>, StoreError> {
        Ok(None)
    }

    async fn compact_resume_at(self: Arc<Self>) -> Option<usize> {
        None
    }

    async fn verify(self: Arc<Self>, _heal: bool) -> Result<(), StoreError> {
        Ok(())
    }

    async fn flush(self: Arc<Self>, _sync_data: bool) -> Result<(), StoreError> {
        Ok(())
    }

    fn max_query_batch(&self) -> Option<usize> {
        // Firestore's batch document-get RPC caps at 100 names per call, same as DynamoDB's
        // BatchGetItem limit `lore_aws` bounds `query` batches to.
        Some(100)
    }
}

struct GcpImmutableStoreInstrumentProvider;

impl InstrumentProvider for GcpImmutableStoreInstrumentProvider {
    fn namespace(&self) -> &'static str {
        "urc.store.immutable.gcp"
    }

    fn labels(&self) -> &[KeyValue] {
        STORE_ATTRIBUTES_GCP.as_slice()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fragment_state_round_trips_through_bits() {
        for state in [
            FragmentState::Stored,
            FragmentState::Obliterating,
            FragmentState::Obliterated,
        ] {
            assert_eq!(FragmentState::from_bits(state.bits()), state);
        }
    }

    #[test]
    fn fragment_state_is_obliteration() {
        assert!(!FragmentState::Stored.is_obliteration());
        assert!(FragmentState::Obliterating.is_obliteration());
        assert!(FragmentState::Obliterated.is_obliteration());
    }

    #[test]
    fn association_doc_id_is_deterministic() {
        let hash = Hash::from([1u8; 32]);
        let partition = Partition::from([2u8; 16]);
        let context = Context::from([3u8; 16]);
        let first = GcpImmutableStore::association_doc_id(hash, partition, context);
        let second = GcpImmutableStore::association_doc_id(hash, partition, context);
        assert_eq!(first, second);
        assert_eq!(first.len(), 64 + 1 + 32 + 1 + 32);
    }
}
