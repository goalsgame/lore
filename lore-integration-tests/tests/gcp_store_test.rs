// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
//! Integration tests for `lore-gcp`'s GCS/Firestore-backed stores, modeled on
//! `aws_store_test.rs`. Unlike that file, there is no docker-compose service standing in for a
//! real backend (no widely-used, single-container emulator covers both GCS and Firestore the
//! way LocalStack covers S3 and `DynamoDB`), so every test here checks `common::gcp_common::env()`
//! first and skips — reporting why, then returning `Ok(())` — when the environment does not name
//! a real GCP project and bucket to run against. That is the expected outcome in this sandbox
//! and in ordinary CI; see `common::gcp_common` for the environment variables that opt in to a
//! real run.
#[cfg(all(test, feature = "integration_tests"))]
mod gcp_store_tests {
    use std::error::Error;
    use std::sync::Arc;

    use lore_base::runtime::LORE_CONTEXT;
    use lore_base::types::Address;
    use lore_base::types::Context;
    use lore_base::types::Hash;
    use lore_base::types::KeyType;
    use lore_gcp::store::immutable_store::FirestoreImmutableStoreSettings;
    use lore_gcp::store::immutable_store::GcpImmutableStore;
    use lore_gcp::store::immutable_store::GcpImmutableStoreSettings;
    use lore_gcp::store::immutable_store::GcsStoreSettings;
    use lore_gcp::store::mutable_store::FirestoreMutableStore;
    use lore_gcp::store::mutable_store::FirestoreMutableStoreSettings;
    use lore_revision::fragment;
    use lore_revision::lore::RepositoryId;
    use lore_storage::ImmutableStore;
    use lore_storage::MutableStore;
    use lore_storage::StoreGetData;
    use lore_storage::StoreMatch;
    use lore_storage::immutable_store::query_one;
    use rand::random;

    use crate::common::gcp_common;
    use crate::setup_execution;

    type TestResult = Result<(), Box<dyn Error>>;

    /// Env var names surfaced in every skip message, so a developer sees exactly what to set.
    const SKIP_HINT: &str = "set LORE_GCP_TEST_PROJECT and LORE_GCP_TEST_BUCKET (and optionally \
         LORE_GCP_TEST_FIRESTORE_DATABASE) to run against a real GCP project";

    /// Builds a fresh [`GcpImmutableStore`] scoped to Firestore collections unique to this test
    /// run, or `None` if the environment does not name a real GCP project/bucket to test against.
    async fn build_immutable_store(
        suffix: &str,
    ) -> Result<Option<Arc<GcpImmutableStore>>, Box<dyn Error>> {
        let Some(env) = gcp_common::env() else {
            return Ok(None);
        };
        let (storage, control, db) = gcp_common::clients(&env).await?;

        let settings = GcpImmutableStoreSettings::new(
            GcsStoreSettings {
                bucket: env.bucket.clone(),
                endpoint: None,
                slow_operation_threshold_millis: u64::MAX,
                timeout_millis: 30_000,
            },
            FirestoreImmutableStoreSettings {
                firestore_project: env.project.clone(),
                firestore_database: env.database.clone(),
                fragment_state_collection: format!("lore_test_fragment_state_{suffix}"),
                fragment_associations_collection: format!(
                    "lore_test_fragment_associations_{suffix}"
                ),
                slow_operation_threshold_millis: u64::MAX,
                timeout_millis: 30_000,
            },
            false,
        );

        Ok(Some(Arc::new(GcpImmutableStore::new(
            storage, control, db, &settings,
        ))))
    }

    /// Builds a fresh [`FirestoreMutableStore`] scoped to a Firestore collection unique to this
    /// test run, or `None` if the environment is not configured for a live GCP run.
    async fn build_mutable_store(
        suffix: &str,
        immutable_store: Arc<dyn ImmutableStore>,
    ) -> Result<Option<Arc<FirestoreMutableStore>>, Box<dyn Error>> {
        let Some(env) = gcp_common::env() else {
            return Ok(None);
        };
        let db =
            lore_gcp::clients::build_firestore_db(&env.project, env.database.as_deref()).await?;

        let settings = FirestoreMutableStoreSettings {
            firestore_project: env.project.clone(),
            firestore_database: env.database.clone(),
            firestore_mutable_store_collection: format!("lore_test_mutable_store_{suffix}"),
            force_write: false,
            timeout_millis: 30_000,
            slow_operation_threshold_millis: u64::MAX,
        };

        Ok(Some(Arc::new(FirestoreMutableStore::new(
            db,
            &settings,
            immutable_store,
        ))))
    }

    /// The contract every `ImmutableStore` owes its callers, checked against the GCP store
    /// backed by real GCS and Firestore. This is the authoritative conformance check for the GCP
    /// implementation, the same role `aws_immutable_store_satisfies_the_conformance_contract`
    /// plays for `lore-aws`.
    #[tokio::test]
    async fn gcp_immutable_store_satisfies_the_conformance_contract() -> TestResult {
        let execution = setup_execution("test".to_string());
        LORE_CONTEXT
            .scope(execution, async move {
                let Some(store) = build_immutable_store(&gcp_common::unique_suffix()).await?
                else {
                    eprintln!(
                        "skipping gcp_immutable_store_satisfies_the_conformance_contract: {SKIP_HINT}"
                    );
                    return Ok(());
                };

                lore_storage::conformance::verify_immutable_store(
                    store,
                    lore_storage::conformance::Capabilities::new("GcpImmutableStore/integration"),
                )
                .await;

                Ok(())
            })
            .await
    }

    /// The contract every `MutableStore` owes its callers, checked against Firestore.
    #[tokio::test]
    async fn gcp_mutable_store_satisfies_conformance_battery() -> TestResult {
        let execution = setup_execution("test".to_string());
        LORE_CONTEXT
            .scope(execution, async move {
                let suffix = gcp_common::unique_suffix();
                let Some(immutable_store) = build_immutable_store(&suffix).await? else {
                    eprintln!(
                        "skipping gcp_mutable_store_satisfies_conformance_battery: {SKIP_HINT}"
                    );
                    return Ok(());
                };
                let Some(mutable_store) = build_mutable_store(&suffix, immutable_store).await?
                else {
                    eprintln!(
                        "skipping gcp_mutable_store_satisfies_conformance_battery: {SKIP_HINT}"
                    );
                    return Ok(());
                };

                lore_storage::mutable_conformance::verify_mutable_store(
                    mutable_store,
                    lore_storage::mutable_conformance::Capabilities::new("FirestoreMutableStore"),
                )
                .await;

                Ok(())
            })
            .await
    }

    #[tokio::test]
    async fn put_then_get_roundtrips_the_payload() -> TestResult {
        let execution = setup_execution("test".to_string());
        LORE_CONTEXT
            .scope(execution, async move {
                let Some(store) = build_immutable_store(&gcp_common::unique_suffix()).await? else {
                    eprintln!("skipping put_then_get_roundtrips_the_payload: {SKIP_HINT}");
                    return Ok(());
                };

                let repository = random::<RepositoryId>();
                let (fragment, address, payload) = fragment::generate_random();

                store
                    .clone()
                    .put(repository, address, fragment, Some(payload.clone()), false)
                    .await?;

                let (got_fragment, got_payload) = store
                    .get(repository, address)
                    .await
                    .and_then(StoreGetData::into_payload)?;

                assert_eq!(got_payload, payload, "payload must read back intact");
                assert_eq!(got_fragment.size_content, fragment.size_content);

                Ok(())
            })
            .await
    }

    #[tokio::test]
    async fn query_reports_full_match_after_put() -> TestResult {
        let execution = setup_execution("test".to_string());
        LORE_CONTEXT
            .scope(execution, async move {
                let Some(store) = build_immutable_store(&gcp_common::unique_suffix()).await? else {
                    eprintln!("skipping query_reports_full_match_after_put: {SKIP_HINT}");
                    return Ok(());
                };

                let repository = random::<RepositoryId>();
                let (fragment, address, payload) = fragment::generate_random();

                store
                    .clone()
                    .put(repository, address, fragment, Some(payload), false)
                    .await?;

                let result =
                    query_one(&(store as Arc<dyn ImmutableStore>), repository, address).await?;
                assert_eq!(result.match_made, StoreMatch::MatchFull);
                assert!(result.stored_durable);

                Ok(())
            })
            .await
    }

    #[tokio::test]
    async fn obliterate_removes_the_association() -> TestResult {
        let execution = setup_execution("test".to_string());
        LORE_CONTEXT
            .scope(execution, async move {
                let Some(store) = build_immutable_store(&gcp_common::unique_suffix()).await? else {
                    eprintln!("skipping obliterate_removes_the_association: {SKIP_HINT}");
                    return Ok(());
                };

                let repository = random::<RepositoryId>();
                let (fragment, address, payload) = fragment::generate_random();

                store
                    .clone()
                    .put(repository, address, fragment, Some(payload), false)
                    .await?;

                store
                    .clone()
                    .obliterate(
                        repository,
                        address,
                        Arc::new(lore_storage::StoreObliterateStats::default()),
                    )
                    .await?;

                let result = query_one(
                    &(store.clone() as Arc<dyn ImmutableStore>),
                    repository,
                    address,
                )
                .await?;
                assert_eq!(result.match_made, StoreMatch::MatchNone);

                assert!(
                    store.get(repository, address).await.is_err(),
                    "an obliterated address must not be servable"
                );

                Ok(())
            })
            .await
    }

    /// Deduplication across contexts: a hash already held under one context in a partition is
    /// registered under a second context via `copy` rather than transferred again.
    #[tokio::test]
    async fn copy_deduplicates_across_contexts_in_the_same_partition() -> TestResult {
        let execution = setup_execution("test".to_string());
        LORE_CONTEXT
            .scope(execution, async move {
                let Some(store) = build_immutable_store(&gcp_common::unique_suffix()).await?
                else {
                    eprintln!(
                        "skipping copy_deduplicates_across_contexts_in_the_same_partition: {SKIP_HINT}"
                    );
                    return Ok(());
                };

                let repository = random::<RepositoryId>();
                let (fragment, first, payload) = fragment::generate_random();
                let second = Address {
                    hash: first.hash,
                    context: random::<Context>(),
                };

                store
                    .clone()
                    .put(repository, first, fragment, Some(payload.clone()), false)
                    .await?;

                store
                    .clone()
                    .copy(repository, first, repository, second.context, false)
                    .await?;

                let (_fragment, copied_payload) = store
                    .get(repository, second)
                    .await
                    .and_then(StoreGetData::into_payload)?;
                assert_eq!(copied_payload, payload);

                Ok(())
            })
            .await
    }

    #[tokio::test]
    async fn mutable_load_store_round_trip() -> TestResult {
        let execution = setup_execution("test".to_string());
        LORE_CONTEXT
            .scope(execution, async move {
                let suffix = gcp_common::unique_suffix();
                let Some(immutable_store) = build_immutable_store(&suffix).await? else {
                    eprintln!("skipping mutable_load_store_round_trip: {SKIP_HINT}");
                    return Ok(());
                };
                let Some(mutable_store) = build_mutable_store(&suffix, immutable_store).await?
                else {
                    eprintln!("skipping mutable_load_store_round_trip: {SKIP_HINT}");
                    return Ok(());
                };

                let repository = random::<RepositoryId>();
                let key = random::<Hash>();
                let value = random::<Hash>();

                mutable_store
                    .clone()
                    .store(repository, key, value, KeyType::BranchId)
                    .await?;

                assert_eq!(
                    value,
                    mutable_store
                        .load(repository, key, KeyType::BranchId)
                        .await?
                );

                Ok(())
            })
            .await
    }

    /// Regression test for the bug `lore_aws::store::mutable_store::CompareAndSwapCondition`
    /// fixed, checked here against the real Firestore transaction path: a key initialized to
    /// `{value: 0}` (what `branch::create` writes for a default branch with no commits) must
    /// still accept a `compare_and_swap(expected = 0, ..)` for the first real push. Before the
    /// fix on the AWS side, a row explicitly holding zero was not recognized as equivalent to "no
    /// row yet", and the swap silently no-oped.
    #[tokio::test]
    async fn compare_and_swap_zero_expected_succeeds_when_row_holds_zero_value() -> TestResult {
        let execution = setup_execution("test".to_string());
        LORE_CONTEXT
            .scope(execution, async move {
                let suffix = gcp_common::unique_suffix();
                let Some(immutable_store) = build_immutable_store(&suffix).await? else {
                    eprintln!(
                        "skipping compare_and_swap_zero_expected_succeeds_when_row_holds_zero_value: {SKIP_HINT}"
                    );
                    return Ok(());
                };
                let Some(mutable_store) = build_mutable_store(&suffix, immutable_store).await?
                else {
                    eprintln!(
                        "skipping compare_and_swap_zero_expected_succeeds_when_row_holds_zero_value: {SKIP_HINT}"
                    );
                    return Ok(());
                };

                let repository = random::<RepositoryId>();
                let key = random::<Hash>();
                let new_value = random::<Hash>();

                // Replicate branch::create's CAS(0, 0): writes a document holding value=0,
                // distinct from no document at all.
                assert_eq!(
                    Hash::default(),
                    mutable_store
                        .clone()
                        .compare_and_swap(
                            repository,
                            key,
                            Hash::default(),
                            Hash::default(),
                            KeyType::BranchLatestPointer,
                        )
                        .await?,
                    "initialisation CAS must succeed"
                );

                assert!(
                    mutable_store
                        .clone()
                        .load(repository, key, KeyType::BranchLatestPointer)
                        .await
                        .is_err(),
                    "a zero-valued row must look absent to load"
                );

                // The first real push: CAS from zero against a row that already holds zero.
                assert_eq!(
                    Hash::default(),
                    mutable_store
                        .clone()
                        .compare_and_swap(
                            repository,
                            key,
                            Hash::default(),
                            new_value,
                            KeyType::BranchLatestPointer,
                        )
                        .await?,
                    "push CAS must succeed against the zero-valued row"
                );

                assert_eq!(
                    new_value,
                    mutable_store
                        .load(repository, key, KeyType::BranchLatestPointer)
                        .await?,
                    "branch pointer must reflect the pushed revision"
                );

                Ok(())
            })
            .await
    }

    #[tokio::test]
    async fn mutable_list_filters_by_key_type() -> TestResult {
        let execution = setup_execution("test".to_string());
        LORE_CONTEXT
            .scope(execution, async move {
                let suffix = gcp_common::unique_suffix();
                let Some(immutable_store) = build_immutable_store(&suffix).await? else {
                    eprintln!("skipping mutable_list_filters_by_key_type: {SKIP_HINT}");
                    return Ok(());
                };
                let Some(mutable_store) = build_mutable_store(&suffix, immutable_store).await?
                else {
                    eprintln!("skipping mutable_list_filters_by_key_type: {SKIP_HINT}");
                    return Ok(());
                };

                let repository = random::<RepositoryId>();
                let branch_key = random::<Hash>();
                let metadata_key = random::<Hash>();
                let branch_value = random::<Hash>();
                let metadata_value = random::<Hash>();

                mutable_store
                    .clone()
                    .store(repository, branch_key, branch_value, KeyType::BranchId)
                    .await?;
                mutable_store
                    .clone()
                    .store(
                        repository,
                        metadata_key,
                        metadata_value,
                        KeyType::BranchMetadata,
                    )
                    .await?;

                let mut branch_channel = mutable_store
                    .clone()
                    .list(repository, KeyType::BranchId)
                    .await?
                    .channel();
                let mut branch_results = Vec::new();
                while let Some(pair) = branch_channel.recv().await {
                    branch_results.push(pair);
                }
                assert_eq!(branch_results.len(), 1);
                assert_eq!(branch_results[0].1, branch_value);

                let mut metadata_channel = mutable_store
                    .clone()
                    .list(repository, KeyType::BranchMetadata)
                    .await?
                    .channel();
                let mut metadata_results = Vec::new();
                while let Some(pair) = metadata_channel.recv().await {
                    metadata_results.push(pair);
                }
                assert_eq!(metadata_results.len(), 1);
                assert_eq!(metadata_results[0].1, metadata_value);

                Ok(())
            })
            .await
    }
}
