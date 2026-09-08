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
//! [`associations_present`]: GcpImmutableStore::associations_present
//! [`has_partition_association`]: GcpImmutableStore::has_partition_association

use std::collections::HashMap;
use std::collections::HashSet;
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
use serde::Deserialize;
use serde::Serialize;
use smallvec::SmallVec;
use tokio::task::JoinSet;
use tracing::Instrument;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::trace;
use tracing::warn;

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

fn to_store_error(error: GcpError) -> StoreError {
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
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
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
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
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

#[derive(Clone, Debug, Deserialize)]
pub struct GcsStoreSettings {
    pub bucket: String,
    #[serde(default)]
    pub endpoint: Option<String>,
    #[serde(default = "default_slow_threshold")]
    pub slow_operation_threshold_millis: u64,
    #[serde(default = "crate::default_gcp_timeout_millis")]
    pub timeout_millis: u64,
}

#[derive(Clone, Debug, Deserialize)]
pub struct FirestoreImmutableStoreSettings {
    pub firestore_project: String,
    #[serde(default)]
    pub firestore_database: Option<String>,
    #[serde(default = "default_fragment_state_collection")]
    pub fragment_state_collection: String,
    #[serde(default = "default_fragment_associations_collection")]
    pub fragment_associations_collection: String,
    #[serde(default = "default_slow_threshold")]
    pub slow_operation_threshold_millis: u64,
    #[serde(default = "crate::default_gcp_timeout_millis")]
    pub timeout_millis: u64,
}

fn default_fragment_state_collection() -> String {
    "fragment_state".to_string()
}

fn default_fragment_associations_collection() -> String {
    "fragment_associations".to_string()
}

fn default_slow_threshold() -> u64 {
    u64::MAX
}

#[derive(Clone, Debug, Deserialize)]
pub struct GcpImmutableStoreSettings {
    pub gcs: GcsStoreSettings,
    pub firestore: FirestoreImmutableStoreSettings,
    #[serde(default)]
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
    timeout: Duration,
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
            timeout: Duration::from_millis(settings.gcs.timeout_millis.max(1)),
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
            .db
            .fluent()
            .select()
            .by_id_in(self.fragment_associations_collection.as_ref())
            .obj()
            .one(&doc_id)
            .await
            .map_err(GcpError::firestore)
            .map_err(to_store_error)?;

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

        self.db
            .fluent()
            .update()
            .in_col(self.fragment_associations_collection.as_ref())
            .document_id(&doc_id)
            .object(&entry)
            .execute::<AssociationEntry>()
            .await
            .map_err(GcpError::firestore)
            .map_err(to_store_error)?;

        Ok(())
    }

    async fn delete_association(
        &self,
        partition: Partition,
        address: Address,
    ) -> Result<(), StoreError> {
        let doc_id = Self::association_doc_id(address.hash, partition, address.context);

        self.db
            .fluent()
            .delete()
            .from(self.fragment_associations_collection.as_ref())
            .document_id(&doc_id)
            .execute()
            .await
            .map_err(GcpError::firestore)
            .map_err(to_store_error)?;

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
            .db
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
            .query()
            .await
            .map_err(GcpError::firestore)
            .map_err(to_store_error)?;

        Ok(!results.is_empty())
    }

    /// Whether any partition/context still references `hash`.
    async fn has_associations(&self, hash: Hash) -> Result<bool, StoreError> {
        let hash_hex = hex_hash(hash);

        let results: Vec<AssociationEntry> = self
            .db
            .fluent()
            .select()
            .from(self.fragment_associations_collection.as_ref())
            .filter(|q| q.for_all([q.field("hash").eq(hash_hex.clone())]))
            .limit(1)
            .obj()
            .query()
            .await
            .map_err(GcpError::firestore)
            .map_err(to_store_error)?;

        Ok(!results.is_empty())
    }

    /// Read the lifecycle state of a hash. `None` means no document exists, so the hash is
    /// unknown.
    pub(crate) async fn load_state(&self, hash: Hash) -> Result<Option<FragmentState>, StoreError> {
        let entry: Option<FragmentStateEntry> = self
            .db
            .fluent()
            .select()
            .by_id_in(self.fragment_state_collection.as_ref())
            .obj()
            .one(&hex_hash(hash))
            .await
            .map_err(GcpError::firestore)
            .map_err(to_store_error)?;

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

        let ids: Vec<(Hash, String)> = distinct
            .iter()
            .map(|hash| (*hash, hex_hash(*hash)))
            .collect();
        let id_strings: Vec<String> = ids.iter().map(|(_, id)| id.clone()).collect();

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
                && let Some((hash, _)) = ids.iter().find(|(_, id)| *id == doc_id)
            {
                states.insert(*hash, entry.state());
            }
        }

        Ok(states)
    }

    /// Record that a payload exists, without disturbing an obliteration that may hold the hash.
    /// Runs in a transaction so a concurrent obliteration's mark cannot be silently overwritten:
    /// the write only happens if no document exists yet, mirroring
    /// `lore_aws::store::immutable_store::RowAbsent`.
    async fn publish_state(&self, hash: Hash) -> Result<FragmentState, StoreError> {
        let collection = self.fragment_state_collection.clone();
        let doc_id = hex_hash(hash);

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
            })
            .await
            .map_err(GcpError::firestore)
            .map_err(to_store_error)
    }

    /// Delete the state document for a hash, so the next put treats it as new content. Only
    /// called for a payload GCS has lost.
    async fn clear_state(&self, hash: Hash) -> Result<(), StoreError> {
        self.db
            .fluent()
            .delete()
            .from(self.fragment_state_collection.as_ref())
            .document_id(hex_hash(hash))
            .execute()
            .await
            .map_err(GcpError::firestore)
            .map_err(to_store_error)?;

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
                            "fragment state for {doc_id} was not {expected:?} when moving to {updated:?}"
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
            })
            .await
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
        crate::clients::with_timeout(
            self.timeout,
            self.storage
                .write_object(&self.bucket_resource, object_name.clone(), payload)
                .set_metadata(to_object_metadata(&fragment))
                .send_unbuffered(),
            || google_cloud_storage::Error::io(std::io::Error::other("GCS write_object timed out")),
        )
        .await
        .map(|_| ())
        .map_err(|error| {
            warn!(?error, %hash, %object_name, "Failed to write payload for hash");
            StoreError::internal_with_context(GcpError::gcs(error), "GCS write object failed")
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
            match items.next().await {
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
                    return Err(StoreError::internal_with_context(
                        GcpError::gcs(error),
                        "GCS list object versions failed",
                    ));
                }
                None => break,
            }
        }

        if generations.is_empty() {
            // Either the bucket is not versioned, or the object is already gone. A best-effort
            // delete of the live object covers the first case; a not-found is not an error here,
            // since obliterate is meant to leave no payload behind either way.
            match self
                .control
                .delete_object()
                .set_bucket(&self.bucket_resource)
                .set_object(&object_name)
                .send()
                .await
            {
                Ok(()) => {}
                Err(error) if is_not_found(&error) => {}
                Err(error) => {
                    warn!(?error, %hash, "Failed to delete payload for hash");
                    return Err(StoreError::internal_with_context(
                        GcpError::gcs(error),
                        "GCS delete object failed",
                    ));
                }
            }
            return Ok(());
        }

        for generation in generations {
            match self
                .control
                .delete_object()
                .set_bucket(&self.bucket_resource)
                .set_object(&object_name)
                .set_generation(generation)
                .send()
                .await
            {
                Ok(()) => {}
                Err(error) if is_not_found(&error) => {}
                Err(error) => {
                    warn!(?error, %hash, generation, "Failed to delete payload generation for hash");
                    return Err(StoreError::internal_with_context(
                        GcpError::gcs(error),
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
        let object = self
            .control
            .get_object()
            .set_bucket(&self.bucket_resource)
            .set_object(&object_name)
            .send()
            .await
            .map_err(|error| {
                if is_not_found(&error) {
                    debug!(%hash, "head_fragment: object not found");
                    StoreError::from(AddressNotFound::from(Address::zero_context_hash(hash)))
                } else {
                    StoreError::internal_with_context(GcpError::gcs(error), "GCS get object failed")
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
        let mut response = self
            .storage
            .read_object(&self.bucket_resource, &object_name)
            .send()
            .await
            .map_err(|error| {
                if is_not_found(&error) {
                    debug!(%hash, "get_gcs_object_contents: object not found");
                    StoreError::from(AddressNotFound::from(Address::zero_context_hash(hash)))
                } else {
                    StoreError::internal_with_context(GcpError::gcs(error), "GCS get object failed")
                }
            })?;

        let object = response.object();

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
        while let Some(chunk) = response.next().await {
            let chunk = chunk.map_err(|error| {
                warn!("Failed to read bytes from GCS response for key: {hash}: {error:?}");
                StoreError::internal_with_context(
                    GcpError::gcs(error),
                    "Failed to read bytes from GCS response stream",
                )
            })?;
            read += chunk.len();
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
