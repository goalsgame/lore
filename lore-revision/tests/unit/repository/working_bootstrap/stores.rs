// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
// Fixtures own their metadata, stores and source checkout files.
#![allow(clippy::disallowed_methods)]

use std::sync::Arc;
use std::time::Duration;

use lore_base::runtime::LORE_CONTEXT;
use lore_base::types::Hash;
use lore_revision::interface::LoreGlobalArgs;
use lore_revision::repository::RepositoryConfig;
use lore_revision::repository::RepositoryWriteToken;
use lore_revision::repository::SharedStoreToUseConfig;
use lore_revision::repository::working_bootstrap::Selection;
use lore_revision::repository::working_bootstrap::StoreInitializer;
use lore_revision::repository::working_bootstrap::StoreMode;

use super::Fixture;
use super::MetadataInitializer;
use super::Mode;
use super::config;
use crate::fs::filesystem_provider::test_execution_with;

fn selection() -> Selection {
    Selection {
        branch: [62; 16].into(),
        branch_name: "main".into(),
        revision: Hash::default(),
    }
}
async fn prepare_metadata(
    fixture: &Fixture,
    config: RepositoryConfig,
    fresh: bool,
) -> lore_revision::repository::working_bootstrap::PreparedMetadata {
    MetadataInitializer::new(1)
        .unwrap()
        .prepare(
            fixture.registration.clone(),
            fixture.token().await,
            if fresh {
                Mode::CreateNew(config)
            } else {
                Mode::Reuse
            },
        )
        .await
        .unwrap()
}

#[tokio::test]
async fn new_local_stores_and_fresh_reuse_keep_unpublished_context_and_release_root_token() {
    LORE_CONTEXT
        .scope(
            test_execution_with(LoreGlobalArgs {
                offline: 1,
                ..Default::default()
            }),
            async {
                let fixture = Fixture::new();
                std::fs::write(fixture.target.join("untouched"), b"source bytes").unwrap();
                let metadata = prepare_metadata(&fixture, config(), true).await;
                let initializer = StoreInitializer::new(1).unwrap();
                let result = tokio::time::timeout(
                    Duration::from_secs(20),
                    initializer.prepare(metadata, StoreMode::CreateNew(selection())),
                )
                .await
                .unwrap()
                .unwrap();
                assert_eq!(result.selection, selection());
                assert_eq!(result.repository.id, result.location.repository());
                assert_eq!(result.repository.instance_id, result.location.instance());
                assert!(result.repository.try_write_token().is_none());
                assert!(
                    result
                        .repository
                        .file_system()
                        .begin_operation()
                        .await
                        .is_err()
                );
                assert!(
                    !fixture
                        .metadata
                        .join(".working-store-initializing")
                        .exists()
                );
                assert_eq!(
                    std::fs::read(fixture.target.join("untouched")).unwrap(),
                    b"source bytes"
                );
                assert!(!fixture.target.join(".lore").exists());
                let token = tokio::time::timeout(Duration::from_secs(1), fixture.token())
                    .await
                    .expect("successful handoff releases canonical root token");
                drop(token);
                drop(result);
                let reused = prepare_metadata(&fixture, config(), false).await;
                let result = tokio::time::timeout(
                    Duration::from_secs(20),
                    initializer.prepare(reused, StoreMode::Reuse),
                )
                .await
                .unwrap()
                .unwrap();
                assert_eq!(result.selection, selection());
                let _token = tokio::time::timeout(Duration::from_secs(1), fixture.token())
                    .await
                    .unwrap();
            },
        )
        .await;
}

#[tokio::test]
async fn missing_selection_retains_store_marker_and_does_not_repair_on_reuse() {
    use lore_revision::repository::working_bootstrap::Error;
    LORE_CONTEXT
        .scope(
            test_execution_with(LoreGlobalArgs {
                offline: 1,
                ..Default::default()
            }),
            async {
                let fixture = Fixture::new();
                let metadata = prepare_metadata(&fixture, config(), true).await;
                let mut absent = selection();
                absent.revision = Hash::from_u64(877);
                let result = tokio::time::timeout(
                    Duration::from_secs(20),
                    StoreInitializer::new(1)
                        .unwrap()
                        .prepare(metadata, StoreMode::CreateNew(absent)),
                )
                .await
                .unwrap();
                assert!(result.is_err());
                assert!(
                    fixture
                        .metadata
                        .join(".working-store-initializing")
                        .is_file()
                );
                let _token = tokio::time::timeout(Duration::from_secs(1), fixture.token())
                    .await
                    .unwrap();
                drop(_token);
                let result = MetadataInitializer::new(1)
                    .unwrap()
                    .prepare(
                        fixture.registration.clone(),
                        fixture.token().await,
                        Mode::Reuse,
                    )
                    .await;
                assert!(matches!(result, Err(Error::Incomplete)));
                assert!(
                    fixture
                        .metadata
                        .join(".working-store-initializing")
                        .is_file()
                );
            },
        )
        .await;
}

#[tokio::test]
async fn reuse_refuses_pending_layout_unknown_version_and_malformed_info_without_rewriting() {
    use lore_revision::repository::working_bootstrap::Error;
    Box::pin(LORE_CONTEXT.scope(
        test_execution_with(LoreGlobalArgs {
            offline: 1,
            ..Default::default()
        }),
        async {
            let fixture = Fixture::new();
            let initializer = StoreInitializer::new(1).unwrap();
            drop(
                initializer
                    .prepare(
                        prepare_metadata(&fixture, config(), true).await,
                        StoreMode::CreateNew(selection()),
                    )
                    .await
                    .unwrap(),
            );
            let group = fixture.metadata.join("immutable/index/00");
            std::fs::create_dir_all(&group).unwrap();
            let pending = group.join("level.pending");
            std::fs::write(&pending, b"retained unfinished transaction").unwrap();
            let before = contents(&fixture.metadata);
            let result = initializer
                .prepare(
                    prepare_metadata(&fixture, config(), false).await,
                    StoreMode::Reuse,
                )
                .await;
            assert!(matches!(result, Err(Error::UnsupportedStore)));
            assert_eq!(contents(&fixture.metadata), before);
            assert_eq!(
                std::fs::read(&pending).unwrap(),
                b"retained unfinished transaction"
            );
            std::fs::remove_file(pending).unwrap();
            let version = fixture.metadata.join("mutable/version");
            let normal_version = std::fs::read(&version).unwrap();
            std::fs::write(&version, 998u32.to_ne_bytes()).unwrap();
            let before = contents(&fixture.metadata);
            let result = initializer
                .prepare(
                    prepare_metadata(&fixture, config(), false).await,
                    StoreMode::Reuse,
                )
                .await;
            assert!(matches!(result, Err(Error::UnsupportedStore)));
            assert_eq!(contents(&fixture.metadata), before);
            assert_eq!(std::fs::read(&version).unwrap(), 998u32.to_ne_bytes());
            std::fs::write(&version, normal_version).unwrap();
            let info = fixture.metadata.join("immutable/info");
            std::fs::write(&info, [0xF0; 12]).unwrap();
            let before = contents(&fixture.metadata);
            let result = initializer
                .prepare(
                    prepare_metadata(&fixture, config(), false).await,
                    StoreMode::Reuse,
                )
                .await;
            assert!(result.is_err());
            assert_eq!(contents(&fixture.metadata), before);
        },
    ))
    .await;
}

#[tokio::test]
async fn reuse_rejects_noncanonical_branch_anchor_without_repair() {
    Box::pin(LORE_CONTEXT.scope(
        test_execution_with(LoreGlobalArgs {
            offline: 1,
            ..Default::default()
        }),
        async {
            let fixture = Fixture::new();
            let initializer = StoreInitializer::new(1).unwrap();
            let prepared = initializer
                .prepare(
                    prepare_metadata(&fixture, config(), true).await,
                    StoreMode::CreateNew(selection()),
                )
                .await
                .unwrap();
            let token = fixture.token().await;
            let repository = &prepared.repository;
            let (key, kind) = lore_revision::instance::anchor_key(
                repository.salt(),
                lore_revision::instance::ANCHOR_CURRENT_BRANCH,
                repository.instance_id,
            );
            // Preserve the valid branch prefix: truncating to_context would
            // otherwise silently accept this corrupted representation.
            let mut bytes = [0; 32];
            bytes[..16].copy_from_slice(selection().branch.data());
            bytes[31] = 1;
            let writer = repository.write_mutable_store(&token);
            writer
                .store(repository.id, key, Hash::from(bytes), kind)
                .await
                .unwrap();
            writer.flush(true).await.unwrap();
            drop(token);
            drop(prepared);
            let before = contents(&fixture.metadata);
            let result = initializer
                .prepare(
                    prepare_metadata(&fixture, config(), false).await,
                    StoreMode::Reuse,
                )
                .await;
            assert!(result.is_err());
            assert_eq!(contents(&fixture.metadata), before);
            let token = tokio::time::timeout(Duration::from_secs(1), fixture.token())
                .await
                .expect("failed reuse releases its root token");
            drop(token);
        },
    ))
    .await;
}

fn contents(
    root: &std::path::Path,
) -> std::collections::BTreeMap<std::path::PathBuf, Option<Vec<u8>>> {
    fn walk(
        root: &std::path::Path,
        path: &std::path::Path,
        output: &mut std::collections::BTreeMap<std::path::PathBuf, Option<Vec<u8>>>,
    ) {
        for entry in std::fs::read_dir(path).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            let relative = path.strip_prefix(root).unwrap().to_path_buf();
            if entry.file_type().unwrap().is_dir() {
                output.insert(relative, None);
                walk(root, &path, output);
            } else {
                output.insert(relative, Some(std::fs::read(path).unwrap()));
            }
        }
    }
    let mut output = std::collections::BTreeMap::new();
    walk(root, root, &mut output);
    output
}

fn shared_config(base: &std::path::Path, remote: &str) -> RepositoryConfig {
    RepositoryConfig {
        remote_url: Some(remote.into()),
        shared_store_to_use: Some(SharedStoreToUseConfig {
            use_shared_store: Some(true),
            shared_store_path: Some(base.display().to_string()),
        }),
        ..config()
    }
}
#[tokio::test]
async fn existing_shared_store_keeps_source_instance_and_nonzero_pin_independent() {
    use lore_revision::instance;
    use lore_revision::repository;
    use lore_revision::shared_store;
    use lore_revision::state::State;
    Box::pin(LORE_CONTEXT.scope(
        test_execution_with(LoreGlobalArgs {
            offline: 1,
            ..Default::default()
        }),
        async {
            let source = lore_base::test_util::TempDir::new("lore-bootstrap-source-");
            let shared = lore_base::test_util::TempDir::new("lore-bootstrap-shared-");
            let remote = "lore://bootstrap.invalid";
            shared_store::create_shared_store(
                Some(shared.path().to_path_buf()),
                remote.into(),
                false,
            )
            .await
            .unwrap();
            let mut normal = shared_config(shared.path(), remote);
            normal.vfs = None;
            let source_token = RepositoryWriteToken::acquire(source.path()).await;
            let source_repository = repository::create_local(
                source.path(),
                &source_token,
                [51; 16].into(),
                selection().branch,
                "main".into(),
                normal,
                false,
            )
            .await
            .unwrap();
            std::fs::write(
                source.join("unchanged"),
                b"source checkout remains unchanged",
            )
            .unwrap();
            let state = State::new();
            state.mark_dirty();
            let revision = state
                .serialize(source_repository.clone(), &source_token)
                .await
                .unwrap();
            assert!(!revision.is_zero());
            instance::store_current_anchor(&source_repository, revision)
                .await
                .unwrap();
            source_repository.flush(true).await.unwrap();
            let source_id = std::fs::read(source.join(".lore/id")).unwrap();
            let source_instance = std::fs::read(source.join(".lore/instance")).unwrap();
            let source_config = std::fs::read(source.join(".lore/config.toml")).unwrap();
            let fixture = Fixture::new();
            let mut target = selection();
            target.revision = revision;
            let prepared =
                prepare_metadata(&fixture, shared_config(shared.path(), remote), true).await;
            let result = tokio::time::timeout(
                Duration::from_secs(20),
                StoreInitializer::new(1)
                    .unwrap()
                    .prepare(prepared, StoreMode::CreateNew(target.clone())),
            )
            .await
            .unwrap()
            .unwrap();
            assert_eq!(result.selection, target);
            assert_ne!(result.repository.instance_id, source_repository.instance_id);
            assert_eq!(
                instance::load_current_anchor_boxed(&source_repository)
                    .await
                    .unwrap(),
                (revision, target.branch)
            );
            assert_eq!(std::fs::read(source.join(".lore/id")).unwrap(), source_id);
            assert_eq!(
                std::fs::read(source.join(".lore/instance")).unwrap(),
                source_instance
            );
            assert_eq!(
                std::fs::read(source.join(".lore/config.toml")).unwrap(),
                source_config
            );
            assert_eq!(
                std::fs::read(source.join("unchanged")).unwrap(),
                b"source checkout remains unchanged"
            );
            drop(result);
            let reused = StoreInitializer::new(1)
                .unwrap()
                .prepare(
                    prepare_metadata(&fixture, config(), false).await,
                    StoreMode::Reuse,
                )
                .await
                .unwrap();
            assert_eq!(reused.selection, target);
        },
    ))
    .await;
}

#[tokio::test]
async fn shared_resolution_never_creates_or_migrates_legacy_storage() {
    use lore_revision::shared_store;
    LORE_CONTEXT
        .scope(
            test_execution_with(LoreGlobalArgs {
                offline: 1,
                ..Default::default()
            }),
            async {
                for kind in ["absent", "legacy-directory", "legacy-config"] {
                    let fixture = Fixture::new();
                    let shared = lore_base::test_util::TempDir::new("lore-bootstrap-policy-");
                    let remote = "lore://bootstrap-policy.invalid";
                    let parent = shared.join(shared_store::shared_store_subdir_for_remote(remote));
                    let store = parent.join(if kind == "legacy-directory" {
                        "global_store"
                    } else {
                        "shared_store"
                    });
                    let config_name = if kind == "legacy-config" {
                        "global.toml"
                    } else {
                        "shared_store.toml"
                    };
                    let bytes = toml::to_string(&shared_store::SharedStoreConfig {
                        remote_url: Some(remote.into()),
                        ..Default::default()
                    })
                    .unwrap()
                    .into_bytes();
                    if kind != "absent" {
                        std::fs::create_dir_all(&store).unwrap();
                        std::fs::write(store.join(config_name), &bytes).unwrap();
                    }
                    let result = StoreInitializer::new(1)
                        .unwrap()
                        .prepare(
                            prepare_metadata(&fixture, shared_config(shared.path(), remote), true)
                                .await,
                            StoreMode::CreateNew(selection()),
                        )
                        .await;
                    assert!(result.is_err(), "{kind} must fail closed");
                    if kind == "absent" {
                        assert!(!parent.exists());
                    } else {
                        assert_eq!(std::fs::read(store.join(config_name)).unwrap(), bytes);
                        assert_eq!(std::fs::read_dir(&store).unwrap().count(), 1);
                        if kind == "legacy-directory" {
                            assert!(!parent.join("shared_store").exists());
                        }
                        if kind == "legacy-config" {
                            assert!(!store.join("shared_store.toml").exists());
                            // Ordinary resolution retains its existing migration behavior.
                            let resolved = shared_store::get_shared_store_path_for_repo(
                                &shared_config(shared.path(), remote),
                            )
                            .await
                            .unwrap()
                            .unwrap();
                            assert_eq!(resolved, store);
                            assert!(store.join("shared_store.toml").is_file());
                            assert!(!store.join("global.toml").exists());
                        }
                    }
                }
            },
        )
        .await;
}

#[tokio::test]
async fn cancelled_after_marker_never_adopts_or_repairs_replacement_owner() {
    use lore_revision::repository::working_bootstrap::Error;
    use lore_revision::shared_store;
    use lore_storage::local::store_lock::Intent;
    use lore_storage::local::store_lock::StoreLock;
    LORE_CONTEXT
        .scope(
            test_execution_with(LoreGlobalArgs {
                offline: 1,
                ..Default::default()
            }),
            async {
                let fixture = Fixture::new();
                let shared = lore_base::test_util::TempDir::new("lore-bootstrap-cancel-");
                let remote = "lore://bootstrap-cancel.invalid";
                shared_store::create_shared_store(
                    Some(shared.path().to_path_buf()),
                    remote.into(),
                    false,
                )
                .await
                .unwrap();
                let store_root = shared_store::get_shared_store_path_for_repo(&shared_config(
                    shared.path(),
                    remote,
                ))
                .await
                .unwrap()
                .unwrap();
                let lock = StoreLock::new(store_root.join("immutable"), false).unwrap();
                let blocking_guard = lock.acquire(Intent::Read).await.unwrap();
                assert!(blocking_guard.is_stale());
                let initializer = StoreInitializer::new(1).unwrap();
                let metadata =
                    prepare_metadata(&fixture, shared_config(shared.path(), remote), true).await;
                let instance = metadata.location().instance();
                let task = lore_base::lore_spawn!({
                    let initializer = initializer.clone();
                    async move {
                        initializer
                            .prepare(metadata, StoreMode::CreateNew(selection()))
                            .await
                    }
                });
                tokio::time::timeout(Duration::from_secs(10), async {
                    while !fixture
                        .metadata
                        .join(".working-store-initializing")
                        .exists()
                    {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .unwrap();
                task.abort();
                assert!(matches!(task.await, Err(error) if error.is_cancelled()));
                drop(blocking_guard);
                let token = tokio::time::timeout(Duration::from_secs(1), fixture.token())
                    .await
                    .unwrap();
                drop(token);
                let Fixture {
                    _root,
                    target,
                    metadata,
                    registry,
                    registration,
                } = fixture;
                drop(registration);
                let replacement = Arc::new(registry.reserve(&target, instance).unwrap());
                // Bind the retained identity, rather than creating another instance over it.
                replacement
                    .reserve_metadata(&metadata, [51; 16].into())
                    .unwrap();
                let result = MetadataInitializer::new(1)
                    .unwrap()
                    .prepare(
                        replacement,
                        RepositoryWriteToken::acquire(&std::fs::canonicalize(target).unwrap())
                            .await,
                        Mode::Reuse,
                    )
                    .await;
                assert!(matches!(result, Err(Error::Incomplete)));
                assert!(metadata.join(".working-store-initializing").is_file());
                assert!(!metadata.join("immutable").exists());
                drop(_root);
            },
        )
        .await;
}

#[tokio::test]
async fn normal_connection_graph_keeps_credentials_without_pinning_initializer_io() {
    use lore_io::lease::IoLease;
    use lore_revision::protocol::shared_connect;
    use lore_revision::repository::working_bootstrap::start_transport;
    let owner = Arc::new(());
    let weak = Arc::downgrade(&owner);
    let lease = IoLease::new(owner);
    let expected = lease.clone();
    let (worker, worker_handle) = tokio::sync::oneshot::channel();
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let (ready, readiness) = tokio::sync::oneshot::channel();
    LORE_CONTEXT
        .scope(
            test_execution_with(LoreGlobalArgs {
                identity: "bootstrap-identity".into(),
                ..Default::default()
            }),
            lease.scope(async move {
                let connect = start_transport(|| {
                    shared_connect(async move {
                        assert!(IoLease::current().is_none());
                        // Eager connection setup owns authentication refreshers that outlive
                        // this setup task and are retained by the returned connection.
                        let handle = lore_base::lore_spawn_net!(async move {
                            assert!(IoLease::current().is_none());
                            let execution = lore_revision::runtime::execution_context();
                            assert_eq!(execution.globals().identity.as_ref(), "bootstrap-identity");
                            assert_eq!(execution.user_id().await, "test-user");
                            ready.send(()).unwrap();
                            let _stopped = stopped.await;
                        });
                        worker.send(handle).unwrap();
                        Err(lore_transport::ProtocolError::from(
                            lore_base::error::NoRemote,
                        ))
                    })
                });
                assert!(
                    IoLease::current().unwrap().ptr_eq(&expected),
                    "local store IO retains its original scope"
                );
                assert!(connect.await.is_err());
            }),
        )
        .await;
    readiness.await.unwrap();
    let worker = worker_handle.await.unwrap();
    drop(lease);
    assert!(
        weak.upgrade().is_none(),
        "a live transport worker must not pin the initializer"
    );
    stop.send(()).unwrap();
    worker.await.unwrap();
}

mod parent_lease;
