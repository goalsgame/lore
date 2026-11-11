// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
//! Firestore-backed [`LockStore`], with the semantics of `lore_aws::store::lock_store`.
//!
//! # Schema
//!
//! One flat collection (default name `locks`, configurable), one document per locked resource:
//!
//! ```text
//! {collection}/{repository_hex}_{branch_hex}_{hash_hex}
//!     => { repository, branch, repository_branch, hash, description, owner, locked_at }
//! ```
//!
//! `repository`, `branch` and `hash` are lowercase hex; `repository_branch` is the two hex strings
//! concatenated, so a query on one branch is a single equality filter. `locked_at` is milliseconds
//! since the Unix epoch.
//!
//! [`LockQuery::HashRepositoryBranch`] reads one document by id. Every other query is an equality
//! filter on one or two of `hash`, `owner`, `repository`, `repository_branch` and `description`;
//! the composite indexes the two-field ones need are listed in this crate's
//! `firestore.indexes.json`.
//!
//! # Atomicity
//!
//! [`LockStore::lock_resources`] and [`LockStore::unlock_resources`] each read every document they
//! touch and write their changes in one Firestore transaction, so either every resource changes or
//! none does, and two servers sharing the collection never both acquire one resource. Each create
//! also carries an `exists = false` precondition and each delete an `exists = true` one; a commit
//! that fails one of them is run again from the read.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use firestore::FirestoreDb;
use firestore::FirestoreWritePrecondition;
use firestore::errors::BackoffError;
use firestore::errors::FirestoreError;
use futures::FutureExt;
use futures::StreamExt;
use lore_base::error::LockNotFound;
use lore_base::error::LockNotOwned;
use lore_base::error::SlowDown;
use lore_base::types::BranchId;
use lore_base::types::LockData;
use lore_base::types::LockResource;
use lore_base::types::RepositoryId;
use lore_revision::lock::LockError;
use lore_revision::lock::LockQuery;
use lore_revision::lock::LockStore;
use serde::Deserialize;
use serde::Serialize;
use tracing::debug;
use tracing::warn;

use crate::clients::bounded;
use crate::gcp_error::GcpError;
use crate::store::mutable_store::parse_hash;

/// How many documents one batch read asks Firestore for.
const READ_CHUNK: usize = 100;

/// How many times a lock or unlock transaction is run when its commit fails a precondition.
const PRECONDITION_ATTEMPTS: usize = 3;

/// Configuration for the Firestore lock store.
#[derive(Clone, Debug)]
pub struct FirestoreLockStoreSettings {
    /// GCP project holding the Firestore database.
    pub firestore_project: String,
    /// Firestore database id. `None` uses the default database, `"(default)"`.
    pub firestore_database: Option<String>,
    /// Collection name for lock documents.
    pub firestore_lock_store_collection: String,
    /// Timeout for one operation, including every attempt of a transaction.
    pub timeout_millis: u64,
    /// Slow-operation threshold for logging, in milliseconds.
    pub slow_operation_threshold_millis: u64,
}

/// A lock document.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct LockEntry {
    repository: String,
    branch: String,
    repository_branch: String,
    hash: String,
    description: String,
    owner: String,
    locked_at: i64,
}

impl LockEntry {
    fn new(repository: RepositoryId, resource: &LockResource, owner: &str, locked_at: u64) -> Self {
        let repository = hex::encode(repository.data());
        let branch = hex::encode(resource.branch.data());
        Self {
            repository_branch: format!("{repository}{branch}"),
            repository,
            branch,
            hash: hex::encode(resource.hash.data()),
            description: resource.description.clone(),
            owner: owner.to_string(),
            locked_at: i64::try_from(locked_at).unwrap_or(i64::MAX),
        }
    }

    fn to_lock_data(&self) -> Result<LockData, GcpError> {
        let branch = hex::decode(&self.branch)
            .ok()
            .and_then(|bytes| <[u8; 16]>::try_from(bytes).ok())
            .ok_or_else(|| {
                GcpError::Decode(format!("lock branch {:?} is not 16 hex bytes", self.branch))
            })?;
        Ok(LockData {
            resource: LockResource {
                branch: BranchId::from(branch),
                hash: parse_hash(&self.hash)?,
                description: self.description.clone(),
            },
            owner: self.owner.clone(),
            locked_at: u64::try_from(self.locked_at).unwrap_or_default(),
        })
    }
}

fn doc_id(repository: RepositoryId, resource: &LockResource) -> String {
    format!(
        "{}_{}_{}",
        hex::encode(repository.data()),
        hex::encode(resource.branch.data()),
        hex::encode(resource.hash.data())
    )
}

fn repository_branch(repository: RepositoryId, branch: BranchId) -> String {
    format!(
        "{}{}",
        hex::encode(repository.data()),
        hex::encode(branch.data())
    )
}

fn to_lock_error(error: GcpError) -> LockError {
    if let GcpError::Firestore(inner) = &error {
        if crate::clients::is_firestore_retryable(inner) {
            return LockError::from(SlowDown);
        }
        if let FirestoreError::ErrorInTransaction(in_transaction) = inner.as_ref()
            && let Some(GcpError::Firestore(cause)) =
                in_transaction.source.downcast_ref::<GcpError>()
            && crate::clients::is_firestore_retryable(cause)
        {
            return LockError::from(SlowDown);
        }
    }
    LockError::internal_with_context(error, "Firestore lock store operation failed")
}

/// Whether a commit failed one of its write preconditions.
fn is_precondition_failure(error: &GcpError) -> bool {
    match error {
        GcpError::Firestore(inner) => match inner.as_ref() {
            FirestoreError::DataConflictError(_) | FirestoreError::DataNotFoundError(_) => true,
            FirestoreError::DatabaseError(e) => e.public.code == "FailedPrecondition",
            _ => false,
        },
        _ => false,
    }
}

/// Reads `ids` from `collection`, answering the documents that exist by id.
async fn read_entries(
    db: &FirestoreDb,
    collection: &str,
    ids: &[String],
) -> Result<HashMap<String, LockEntry>, GcpError> {
    let mut found = HashMap::with_capacity(ids.len());
    for chunk in ids.chunks(READ_CHUNK) {
        let mut stream = db
            .fluent()
            .select()
            .by_id_in(collection)
            .obj::<LockEntry>()
            .batch_with_errors(chunk.iter())
            .await?;
        while let Some(item) = stream.next().await {
            if let (id, Some(entry)) = item? {
                found.insert(id, entry);
            }
        }
    }
    Ok(found)
}

/// What a lock transaction decided.
enum Acquire {
    Acquired(Vec<LockEntry>),
    HeldByAnother,
}

/// What an unlock transaction decided.
enum Release {
    Released,
    NotOwned,
    NotFound,
}

pub struct FirestoreLockStore {
    db: FirestoreDb,
    collection: Arc<str>,
    timeout: Duration,
    slow_threshold: Duration,
}

impl FirestoreLockStore {
    pub fn new(db: FirestoreDb, settings: &FirestoreLockStoreSettings) -> Self {
        Self {
            db,
            collection: Arc::from(settings.firestore_lock_store_collection.as_str()),
            timeout: Duration::from_millis(settings.timeout_millis.max(1)),
            slow_threshold: Duration::from_millis(settings.slow_operation_threshold_millis),
        }
    }

    async fn firestore_op<T>(
        &self,
        op: &'static str,
        fut: impl Future<Output = Result<T, GcpError>>,
    ) -> Result<T, LockError> {
        bounded(self.timeout, self.slow_threshold, op, fut)
            .await
            .map_err(|_| LockError::from(SlowDown))?
            .map_err(to_lock_error)
    }

    /// Runs `attempt` until its commit passes its preconditions, at most
    /// [`PRECONDITION_ATTEMPTS`] times.
    async fn with_precondition_retry<T, F, Fut>(&self, attempt: F) -> Result<T, GcpError>
    where
        F: Fn() -> Fut,
        Fut: Future<Output = Result<T, GcpError>>,
    {
        let mut tries = 1;
        loop {
            match attempt().await {
                Err(error) if tries < PRECONDITION_ATTEMPTS && is_precondition_failure(&error) => {
                    debug!(%error, tries, "Lock transaction failed a precondition; running it again");
                    tries += 1;
                }
                outcome => return outcome,
            }
        }
    }

    async fn acquire(
        &self,
        wanted: Arc<BTreeMap<String, LockEntry>>,
        owner: Arc<str>,
    ) -> Result<Acquire, GcpError> {
        let collection = self.collection.clone();
        self.db
            .run_transaction::<Acquire, _, GcpError>(move |db, transaction| {
                let collection = collection.clone();
                let wanted = wanted.clone();
                let owner = owner.clone();
                async move {
                    let ids: Vec<String> = wanted.keys().cloned().collect();
                    let held = read_entries(&db, &collection, &ids)
                        .await
                        .map_err(BackoffError::Permanent)?;

                    let mut acquired = Vec::with_capacity(wanted.len());
                    for (id, entry) in wanted.iter() {
                        match held.get(id) {
                            Some(existing) if existing.owner == *owner => continue,
                            Some(_) => return Ok(Acquire::HeldByAnother),
                            None => acquired.push((id, entry)),
                        }
                    }

                    for (id, entry) in &acquired {
                        db.fluent()
                            .update()
                            .in_col(&collection)
                            .precondition(FirestoreWritePrecondition::Exists(false))
                            .document_id(id.as_str())
                            .object(*entry)
                            .add_to_transaction(transaction)
                            .map_err(GcpError::firestore)
                            .map_err(BackoffError::Permanent)?;
                    }

                    Ok(Acquire::Acquired(
                        acquired
                            .into_iter()
                            .map(|(_, entry)| entry.clone())
                            .collect(),
                    ))
                }
                .boxed()
            })
            .await
            .map_err(GcpError::firestore)
    }

    async fn release(
        &self,
        ids: Arc<Vec<String>>,
        owner: Arc<str>,
        validate_user: bool,
    ) -> Result<Release, GcpError> {
        let collection = self.collection.clone();
        self.db
            .run_transaction::<Release, _, GcpError>(move |db, transaction| {
                let collection = collection.clone();
                let ids = ids.clone();
                let owner = owner.clone();
                async move {
                    let held = read_entries(&db, &collection, &ids)
                        .await
                        .map_err(BackoffError::Permanent)?;

                    if validate_user {
                        if held.values().any(|entry| entry.owner != *owner) {
                            return Ok(Release::NotOwned);
                        }
                        if held.len() != ids.len() {
                            return Ok(Release::NotFound);
                        }
                    }

                    for id in ids.iter().filter(|id| held.contains_key(*id)) {
                        db.fluent()
                            .delete()
                            .from(&collection)
                            .precondition(FirestoreWritePrecondition::Exists(true))
                            .document_id(id.as_str())
                            .add_to_transaction(transaction)
                            .map_err(GcpError::firestore)
                            .map_err(BackoffError::Permanent)?;
                    }

                    Ok(Release::Released)
                }
                .boxed()
            })
            .await
            .map_err(GcpError::firestore)
    }

    async fn query(
        &self,
        clauses: Vec<(&'static str, String)>,
    ) -> Result<Vec<LockEntry>, GcpError> {
        let mut stream = self
            .db
            .fluent()
            .select()
            .from(self.collection.as_ref())
            .filter(|q| {
                q.for_all(
                    clauses
                        .iter()
                        .map(|(field, value)| q.field(*field).eq(value.clone())),
                )
            })
            .obj::<LockEntry>()
            .stream_query_with_errors()
            .await?;

        let mut entries = Vec::new();
        while let Some(entry) = stream.next().await {
            entries.push(entry?);
        }
        Ok(entries)
    }
}

#[async_trait]
impl LockStore for FirestoreLockStore {
    #[lore_macro::lore_instrument]
    #[tracing::instrument(name = "FirestoreLockStore::lock_resources", skip_all)]
    async fn lock_resources(
        &self,
        owner_id: &str,
        repository: RepositoryId,
        resources: &[LockResource],
    ) -> Result<Vec<LockData>, LockError> {
        let locked_at = lore_revision::util::time::timestamp();
        let mut wanted = BTreeMap::new();
        for resource in resources {
            wanted
                .entry(doc_id(repository, resource))
                .or_insert_with(|| LockEntry::new(repository, resource, owner_id, locked_at));
        }
        if wanted.is_empty() {
            return Ok(Vec::new());
        }

        let wanted = Arc::new(wanted);
        let owner: Arc<str> = Arc::from(owner_id);
        let outcome = self
            .firestore_op(
                "lock_store.lock_transaction",
                self.with_precondition_retry(|| self.acquire(wanted.clone(), owner.clone())),
            )
            .await?;

        match outcome {
            Acquire::Acquired(entries) => entries
                .iter()
                .map(|entry| entry.to_lock_data().map_err(to_lock_error))
                .collect(),
            Acquire::HeldByAnother => Err(LockNotOwned.into()),
        }
    }

    #[lore_macro::lore_instrument]
    #[tracing::instrument(name = "FirestoreLockStore::query_locks", skip_all)]
    async fn query_locks(&self, query: LockQuery) -> Result<Vec<LockData>, LockError> {
        let hex_of = |repository: RepositoryId| hex::encode(repository.data());
        let clauses = match query {
            LockQuery::HashRepositoryBranch(hash, repository, branch) => {
                let id = doc_id(
                    repository,
                    &LockResource {
                        branch,
                        hash,
                        description: String::new(),
                    },
                );
                let found = self
                    .firestore_op(
                        "lock_store.get",
                        read_entries(&self.db, &self.collection, &[id]),
                    )
                    .await?;
                return found
                    .values()
                    .map(|entry| entry.to_lock_data().map_err(to_lock_error))
                    .collect();
            }
            LockQuery::Hash(hash) => vec![("hash", hex::encode(hash.data()))],
            LockQuery::HashRepository(hash, repository) => vec![
                ("hash", hex::encode(hash.data())),
                ("repository", hex_of(repository)),
            ],
            LockQuery::Owner(owner) => vec![("owner", owner)],
            LockQuery::OwnerRepository(owner, repository) => {
                vec![("owner", owner), ("repository", hex_of(repository))]
            }
            LockQuery::OwnerRepositoryBranch(owner, repository, branch) => vec![
                ("owner", owner),
                ("repository_branch", repository_branch(repository, branch)),
            ],
            LockQuery::Repository(repository) => vec![("repository", hex_of(repository))],
            LockQuery::RepositoryBranch(repository, branch) => {
                vec![("repository_branch", repository_branch(repository, branch))]
            }
            LockQuery::RepositoryBranchDescription(repository, branch, description) => vec![
                ("repository_branch", repository_branch(repository, branch)),
                ("description", description),
            ],
        };

        let entries = self
            .firestore_op("lock_store.query", self.query(clauses))
            .await?;
        entries
            .iter()
            .map(|entry| entry.to_lock_data().map_err(to_lock_error))
            .collect()
    }

    #[lore_macro::lore_instrument]
    #[tracing::instrument(name = "FirestoreLockStore::check_locks_status", skip_all)]
    async fn check_locks_status(
        &self,
        repository: RepositoryId,
        resources: &[LockResource],
    ) -> Result<Vec<LockData>, LockError> {
        let mut ids: Vec<String> = resources
            .iter()
            .map(|resource| doc_id(repository, resource))
            .collect();
        ids.sort();
        ids.dedup();

        let found = self
            .firestore_op(
                "lock_store.check_status",
                read_entries(&self.db, &self.collection, &ids),
            )
            .await?;
        found
            .values()
            .map(|entry| entry.to_lock_data().map_err(to_lock_error))
            .collect()
    }

    #[lore_macro::lore_instrument]
    #[tracing::instrument(name = "FirestoreLockStore::unlock_resources", skip_all)]
    async fn unlock_resources(
        &self,
        owner_id: &str,
        validate_user: bool,
        repository: RepositoryId,
        resources: &[LockResource],
    ) -> Result<Vec<LockResource>, LockError> {
        let mut resources = resources.to_vec();
        resources.sort();
        resources.dedup();
        if resources.is_empty() {
            return Ok(resources);
        }

        let mut ids: Vec<String> = resources
            .iter()
            .map(|resource| doc_id(repository, resource))
            .collect();
        ids.sort();
        ids.dedup();

        let ids = Arc::new(ids);
        let owner: Arc<str> = Arc::from(owner_id);
        let outcome = self
            .firestore_op(
                "lock_store.unlock_transaction",
                self.with_precondition_retry(|| {
                    self.release(ids.clone(), owner.clone(), validate_user)
                }),
            )
            .await?;

        match outcome {
            Release::Released => Ok(resources),
            Release::NotOwned => {
                warn!(
                    owner_id,
                    %repository,
                    "Could not unlock resources: a lock is owned by another user"
                );
                Err(LockNotOwned.into())
            }
            Release::NotFound => Err(LockNotFound.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use lore_base::types::Hash;

    use super::*;

    fn resource(branch: u8, hash: u8, description: &str) -> LockResource {
        LockResource {
            branch: BranchId::from([branch; 16]),
            hash: Hash::from([hash; 32]),
            description: description.to_string(),
        }
    }

    #[test]
    fn an_entry_round_trips_to_the_lock_it_was_made_from() {
        let repository = RepositoryId::from([0x11u8; 16]);
        let resource = resource(0x22, 0x33, "Content/Foo.uasset");

        let entry = LockEntry::new(repository, &resource, "someone", 1_700_000_000_123);

        assert_eq!(
            entry.to_lock_data().unwrap(),
            LockData {
                resource,
                owner: "someone".to_string(),
                locked_at: 1_700_000_000_123,
            }
        );
        assert_eq!(
            entry.repository_branch,
            format!("{}{}", "11".repeat(16), "22".repeat(16))
        );
    }

    #[test]
    fn the_document_id_ignores_the_description() {
        let repository = RepositoryId::from([0x11u8; 16]);
        assert_eq!(
            doc_id(repository, &resource(1, 2, "a")),
            doc_id(repository, &resource(1, 2, "b"))
        );
        assert_ne!(
            doc_id(repository, &resource(1, 2, "a")),
            doc_id(repository, &resource(1, 3, "a"))
        );
    }

    #[test]
    fn a_malformed_branch_does_not_decode() {
        let repository = RepositoryId::from([0x11u8; 16]);
        let mut entry = LockEntry::new(repository, &resource(1, 2, "a"), "someone", 1);
        entry.branch = "abcd".to_string();
        assert!(entry.to_lock_data().is_err());
    }

    #[test]
    fn only_write_conflicts_count_as_precondition_failures() {
        use firestore::errors::FirestoreDataConflictError;
        use firestore::errors::FirestoreDatabaseError;
        use firestore::errors::FirestoreErrorPublicGenericDetails;

        let conflict = GcpError::firestore(FirestoreError::DataConflictError(
            FirestoreDataConflictError::new(
                FirestoreErrorPublicGenericDetails::new("AlreadyExists".to_string()),
                String::new(),
            ),
        ));
        let failed_precondition =
            GcpError::firestore(FirestoreError::DatabaseError(FirestoreDatabaseError::new(
                FirestoreErrorPublicGenericDetails::new("FailedPrecondition".to_string()),
                String::new(),
                false,
            )));
        let invalid =
            GcpError::firestore(FirestoreError::DatabaseError(FirestoreDatabaseError::new(
                FirestoreErrorPublicGenericDetails::new("InvalidArgument".to_string()),
                String::new(),
                false,
            )));

        assert!(is_precondition_failure(&conflict));
        assert!(is_precondition_failure(&failed_precondition));
        assert!(!is_precondition_failure(&invalid));
        assert!(!is_precondition_failure(&GcpError::Decode(String::new())));
    }
}

/// Tests against the Firestore emulator from `lore-integration-tests/compose.yaml`.
#[cfg(all(test, feature = "integration_tests"))]
mod emulator_tests {
    use std::collections::HashSet;

    use lore_base::types::Hash;
    use tokio::task::JoinSet;

    use super::*;

    const PROJECT: &str = "lore-gcp-emulator-tests";

    struct EmulatorToken;

    #[async_trait]
    impl gcloud_sdk::Source for EmulatorToken {
        async fn token(&self) -> gcloud_sdk::error::Result<gcloud_sdk::Token> {
            Ok(gcloud_sdk::Token::new(
                "Bearer".to_owned(),
                gcloud_sdk::SecretValue::from("owner"),
                chrono::Utc::now() + chrono::Duration::hours(1),
            ))
        }
    }

    /// A store on a collection unique to this call, so tests sharing one emulator cannot see
    /// each other's locks.
    async fn emulator_store() -> FirestoreLockStore {
        let collection = format!("locks_{}", uuid::Uuid::new_v4().simple());
        store_on(&collection).await
    }

    /// A store with its own client on `collection`, standing in for one server.
    async fn store_on(collection: &str) -> FirestoreLockStore {
        crate::clients::ensure_crypto_provider();
        let db = FirestoreDb::with_options_token_source(
            firestore::FirestoreDbOptions::new(PROJECT.to_owned()),
            Vec::new(),
            gcloud_sdk::TokenSourceType::ExternalSource(Box::new(EmulatorToken)),
        )
        .await
        .expect("connecting to the Firestore emulator; is FIRESTORE_EMULATOR_HOST set?");

        FirestoreLockStore::new(
            db,
            &FirestoreLockStoreSettings {
                firestore_project: PROJECT.to_owned(),
                firestore_database: None,
                firestore_lock_store_collection: collection.to_owned(),
                timeout_millis: 60_000,
                slow_operation_threshold_millis: 60_000,
            },
        )
    }

    fn random_resource(branch: BranchId, description: &str) -> LockResource {
        LockResource {
            branch,
            hash: rand::random::<Hash>(),
            description: description.to_string(),
        }
    }

    fn owners(locks: &[LockData]) -> HashSet<String> {
        locks.iter().map(|lock| lock.owner.clone()).collect()
    }

    fn held(locks: &[LockData]) -> HashSet<LockResource> {
        locks.iter().map(|lock| lock.resource.clone()).collect()
    }

    #[tokio::test]
    async fn locking_twice_returns_only_new_locks_and_another_owner_is_refused() {
        let store = emulator_store().await;
        let repository = rand::random::<RepositoryId>();
        let branch = rand::random::<BranchId>();
        let foo = random_resource(branch, "src/foo.rs");
        let bar = random_resource(branch, "src/bar.rs");

        let locks = store
            .lock_resources(
                "alice",
                repository,
                &[foo.clone(), bar.clone(), foo.clone()],
            )
            .await
            .unwrap();
        assert_eq!(held(&locks), HashSet::from([foo.clone(), bar.clone()]));
        assert_eq!(owners(&locks), HashSet::from(["alice".to_string()]));

        let again = store
            .lock_resources("alice", repository, std::slice::from_ref(&foo))
            .await
            .unwrap();
        assert!(
            again.is_empty(),
            "a lock already held is not returned again"
        );

        let refused = store
            .lock_resources("bob", repository, std::slice::from_ref(&foo))
            .await;
        assert!(matches!(refused, Err(LockError::LockNotOwned(_))));
    }

    #[tokio::test]
    async fn a_refused_lock_acquires_nothing() {
        let store = emulator_store().await;
        let repository = rand::random::<RepositoryId>();
        let branch = rand::random::<BranchId>();
        let taken = random_resource(branch, "taken");
        let free = random_resource(branch, "free");

        store
            .lock_resources("alice", repository, std::slice::from_ref(&taken))
            .await
            .unwrap();
        let refused = store
            .lock_resources("bob", repository, &[free.clone(), taken.clone()])
            .await;
        assert!(matches!(refused, Err(LockError::LockNotOwned(_))));

        let status = store
            .check_locks_status(repository, &[free.clone(), taken.clone()])
            .await
            .unwrap();
        assert_eq!(held(&status), HashSet::from([taken]));
    }

    #[tokio::test]
    async fn unlocking_checks_the_owner_only_when_asked() {
        let store = emulator_store().await;
        let repository = rand::random::<RepositoryId>();
        let branch = rand::random::<BranchId>();
        let resources = vec![
            random_resource(branch, "src/foo.rs"),
            random_resource(branch, "src/bar.rs"),
        ];
        store
            .lock_resources("alice", repository, &resources)
            .await
            .unwrap();

        let refused = store
            .unlock_resources("bob", true, repository, &resources)
            .await;
        assert!(matches!(refused, Err(LockError::LockNotOwned(_))));
        assert_eq!(
            store
                .check_locks_status(repository, &resources)
                .await
                .unwrap()
                .len(),
            2,
            "a refused unlock releases nothing"
        );

        let released = store
            .unlock_resources("bob", false, repository, &resources)
            .await
            .unwrap();
        assert_eq!(released.len(), 2);
        assert!(
            store
                .check_locks_status(repository, &resources)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn unlocking_a_missing_lock() {
        let store = emulator_store().await;
        let repository = rand::random::<RepositoryId>();
        let branch = rand::random::<BranchId>();
        let locked = random_resource(branch, "locked");
        let missing = random_resource(branch, "missing");
        store
            .lock_resources("alice", repository, std::slice::from_ref(&locked))
            .await
            .unwrap();

        let refused = store
            .unlock_resources(
                "alice",
                true,
                repository,
                &[locked.clone(), missing.clone()],
            )
            .await;
        assert!(matches!(refused, Err(LockError::LockNotFound(_))));
        assert_eq!(
            store
                .check_locks_status(repository, std::slice::from_ref(&locked))
                .await
                .unwrap()
                .len(),
            1,
            "an unlock that names a missing lock releases nothing when the owner is checked"
        );

        store
            .unlock_resources("alice", false, repository, &[locked.clone(), missing])
            .await
            .unwrap();
        assert!(
            store
                .check_locks_status(repository, &[locked])
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn status_answers_the_stored_locks_exactly() {
        let store = emulator_store().await;
        let repository = rand::random::<RepositoryId>();
        let branch = rand::random::<BranchId>();
        let resources: Vec<LockResource> = (0..150)
            .map(|i| random_resource(branch, &format!("file-{i}")))
            .collect();

        let acquired = store
            .lock_resources("alice", repository, &resources[..120])
            .await
            .unwrap();
        let mut asked = resources.clone();
        asked.push(resources[0].clone());
        let status = store.check_locks_status(repository, &asked).await.unwrap();

        assert_eq!(status.len(), 120);
        for lock in &acquired {
            assert!(status.contains(lock), "{lock:?} missing from the status");
        }
    }

    #[tokio::test]
    async fn every_query_answers_its_own_locks() {
        let store = emulator_store().await;
        let repo_one = rand::random::<RepositoryId>();
        let repo_two = rand::random::<RepositoryId>();
        let branch_one = rand::random::<BranchId>();
        let branch_two = rand::random::<BranchId>();
        let shared_hash = rand::random::<Hash>();

        let a = LockResource {
            branch: branch_one,
            hash: shared_hash,
            description: "a".to_string(),
        };
        let b = random_resource(branch_one, "b");
        let c = random_resource(branch_two, "c");
        let d = LockResource {
            branch: branch_one,
            hash: shared_hash,
            description: "a".to_string(),
        };

        store
            .lock_resources("alice", repo_one, &[a.clone(), b.clone()])
            .await
            .unwrap();
        store
            .lock_resources("bob", repo_one, std::slice::from_ref(&c))
            .await
            .unwrap();
        store
            .lock_resources("alice", repo_two, std::slice::from_ref(&d))
            .await
            .unwrap();

        let ask = |query| store.query_locks(query);

        assert_eq!(
            ask(LockQuery::Hash(shared_hash)).await.unwrap().len(),
            2,
            "one hash locked in two repositories"
        );
        assert_eq!(
            held(
                &ask(LockQuery::HashRepository(shared_hash, repo_one))
                    .await
                    .unwrap()
            ),
            HashSet::from([a.clone()])
        );
        assert_eq!(
            held(
                &ask(LockQuery::HashRepositoryBranch(
                    shared_hash,
                    repo_one,
                    branch_one
                ))
                .await
                .unwrap()
            ),
            HashSet::from([a.clone()])
        );
        assert!(
            ask(LockQuery::HashRepositoryBranch(
                shared_hash,
                repo_one,
                branch_two
            ))
            .await
            .unwrap()
            .is_empty()
        );
        assert_eq!(
            ask(LockQuery::Owner("alice".to_string()))
                .await
                .unwrap()
                .len(),
            3
        );
        assert_eq!(
            held(
                &ask(LockQuery::OwnerRepository("alice".to_string(), repo_one))
                    .await
                    .unwrap()
            ),
            HashSet::from([a.clone(), b.clone()])
        );
        assert_eq!(
            held(
                &ask(LockQuery::OwnerRepositoryBranch(
                    "bob".to_string(),
                    repo_one,
                    branch_two
                ))
                .await
                .unwrap()
            ),
            HashSet::from([c.clone()])
        );
        assert!(
            ask(LockQuery::OwnerRepositoryBranch(
                "bob".to_string(),
                repo_one,
                branch_one
            ))
            .await
            .unwrap()
            .is_empty()
        );
        assert_eq!(
            held(&ask(LockQuery::Repository(repo_one)).await.unwrap()),
            HashSet::from([a.clone(), b.clone(), c.clone()])
        );
        assert_eq!(
            held(
                &ask(LockQuery::RepositoryBranch(repo_one, branch_one))
                    .await
                    .unwrap()
            ),
            HashSet::from([a.clone(), b.clone()])
        );
        assert_eq!(
            held(
                &ask(LockQuery::RepositoryBranchDescription(
                    repo_one,
                    branch_one,
                    "b".to_string()
                ))
                .await
                .unwrap()
            ),
            HashSet::from([b])
        );
    }

    /// Two stores with their own clients on one collection, as two servers would have, race for
    /// the same resource. Exactly one owner may ever hold it.
    #[tokio::test]
    async fn two_servers_racing_for_one_resource_never_both_win() {
        let collection = format!("locks_{}", uuid::Uuid::new_v4().simple());
        let stores = [
            Arc::new(store_on(&collection).await),
            Arc::new(store_on(&collection).await),
        ];
        let repository = rand::random::<RepositoryId>();
        let branch = rand::random::<BranchId>();

        for round in 0..10 {
            let resource = random_resource(branch, &format!("round-{round}"));
            let mut tasks = JoinSet::new();
            for contender in 0..8 {
                let store = stores[contender % 2].clone();
                let resource = resource.clone();
                lore_base::lore_spawn!(tasks, async move {
                    let owner = format!("owner-{contender}");
                    let outcome = store.lock_resources(&owner, repository, &[resource]).await;
                    (owner, outcome)
                });
            }

            let mut winners = Vec::new();
            while let Some(task) = tasks.join_next().await {
                let (owner, outcome) = task.unwrap();
                match outcome {
                    Ok(locks) => {
                        assert_eq!(locks.len(), 1, "a first acquire returns its lock");
                        winners.push(owner);
                    }
                    Err(LockError::LockNotOwned(_)) => {}
                    Err(error) => panic!("unexpected error from a contended lock: {error:?}"),
                }
            }
            assert_eq!(winners.len(), 1, "round {round}: winners {winners:?}");

            let stored = stores[0]
                .check_locks_status(repository, &[resource])
                .await
                .unwrap();
            assert_eq!(owners(&stored), HashSet::from([winners[0].clone()]));
        }
    }

    /// Overlapping multi-resource requests from two servers are all-or-nothing: no resource ends
    /// up held by an owner whose request was refused, and each resource has at most one owner.
    #[tokio::test]
    async fn overlapping_requests_from_two_servers_are_all_or_nothing() {
        let collection = format!("locks_{}", uuid::Uuid::new_v4().simple());
        let stores = [
            Arc::new(store_on(&collection).await),
            Arc::new(store_on(&collection).await),
        ];
        let repository = rand::random::<RepositoryId>();
        let branch = rand::random::<BranchId>();
        let pool: Vec<LockResource> = (0..6)
            .map(|i| random_resource(branch, &format!("file-{i}")))
            .collect();

        let mut tasks = JoinSet::new();
        for contender in 0..12 {
            let store = stores[contender % 2].clone();
            let wanted: Vec<LockResource> = (0..3)
                .map(|offset| pool[(contender + offset) % pool.len()].clone())
                .collect();
            lore_base::lore_spawn!(tasks, async move {
                let owner = format!("owner-{contender}");
                let outcome = store.lock_resources(&owner, repository, &wanted).await;
                (owner, wanted, outcome)
            });
        }

        let mut granted: HashMap<String, Vec<LockResource>> = HashMap::new();
        while let Some(task) = tasks.join_next().await {
            let (owner, wanted, outcome) = task.unwrap();
            match outcome {
                Ok(locks) => {
                    assert_eq!(held(&locks), wanted.iter().cloned().collect());
                    granted.insert(owner, wanted);
                }
                Err(LockError::LockNotOwned(_)) => {}
                Err(error) => panic!("unexpected error from a contended lock: {error:?}"),
            }
        }

        let stored = stores[1]
            .check_locks_status(repository, &pool)
            .await
            .unwrap();
        for lock in &stored {
            let wanted = granted
                .get(&lock.owner)
                .unwrap_or_else(|| panic!("{} holds a lock but was refused", lock.owner));
            assert!(wanted.contains(&lock.resource));
        }
        let stored_count = stored.len();
        let granted_count: usize = granted.values().map(Vec::len).sum();
        assert_eq!(
            stored_count, granted_count,
            "every granted lock is stored, once"
        );
    }
}
