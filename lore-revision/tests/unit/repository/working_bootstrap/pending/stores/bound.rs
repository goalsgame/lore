// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
// Fixtures change only their private records and inspect failed setup effects.
#![allow(clippy::disallowed_methods)]

use std::path::Path;
use std::path::PathBuf;

use lore_revision::repository::SharedStoreToUseConfig;
use lore_revision::repository::working_bootstrap::BackingBinding;

use super::*;

const BINDING: &str = ".working-backing-v1";
fn backing(fixture: &Fixture, name: &str) -> PathBuf {
    use std::os::unix::fs::DirBuilderExt;
    let path = fixture._root.join(name);
    std::fs::DirBuilder::new()
        .mode(0o700)
        .create(&path)
        .unwrap();
    path.canonicalize().unwrap()
}
fn write_binding(fixture: &Fixture, root: &Path) {
    let location = fixture.registration.bootstrap_location().unwrap();
    BackingBinding::write_new(
        location.metadata().path(),
        location.repository(),
        location.instance(),
        root,
    )
    .unwrap();
}
fn no_store_effects(fixture: &Fixture) {
    for name in [".working-store-initializing", "immutable", "mutable"] {
        assert!(
            !fixture.metadata.join(name).exists(),
            "unexpected store effect: {name}"
        );
    }
}

#[tokio::test]
async fn bound_local_create_then_independent_reuse_preserve_record_and_selection() {
    LORE_CONTEXT
        .scope(execution(), async {
            let mut fixture = Fixture::new();
            let root = backing(&fixture, "backing");
            let instance = fixture
                .registration
                .bootstrap_location()
                .unwrap()
                .instance();
            let pending = pending_metadata(&fixture, Mode::CreateNew(config())).await;
            write_binding(&fixture, &root);
            let original = std::fs::read(fixture.metadata.join(BINDING)).unwrap();
            let working = StoreInitializer::new(1)
                .unwrap()
                .prepare_pending_bound(pending, StoreMode::CreateNew(selection()), root.clone())
                .await
                .unwrap();
            assert_eq!(working.selection(), &selection());
            assert_eq!(
                std::fs::read(fixture.metadata.join(BINDING)).unwrap(),
                original
            );
            drop(working);
            fixture = reregister(fixture, instance);
            let pending = pending_metadata(&fixture, Mode::Reuse).await;
            let working = StoreInitializer::new(1)
                .unwrap()
                .prepare_pending_bound(pending, StoreMode::Reuse, root)
                .await
                .unwrap();
            assert_eq!(working.selection(), &selection());
            assert_eq!(
                std::fs::read(fixture.metadata.join(BINDING)).unwrap(),
                original
            );
            assert!(
                !fixture
                    .metadata
                    .join(".working-store-initializing")
                    .exists()
            );
            drop(working);
        })
        .await;
}

#[tokio::test]
async fn missing_foreign_corrupt_or_wrong_expected_binding_fails_before_store_effects() {
    LORE_CONTEXT
        .scope(execution(), async {
            for case in 0..6 {
                let fixture = Fixture::new();
                let root = backing(&fixture, "backing");
                let expected = match case {
                    3 => backing(&fixture, "wrong"),
                    4 => PathBuf::from("relative"),
                    5 => PathBuf::from(format!("/{}", "x".repeat(4096))),
                    _ => root.clone(),
                };
                let pending = pending_metadata(&fixture, Mode::CreateNew(config())).await;
                let location = fixture.registration.bootstrap_location().unwrap();
                match case {
                    0 => {}
                    1 => {
                        BackingBinding::write_new(
                            location.metadata().path(),
                            [52; 16].into(),
                            location.instance(),
                            &root,
                        )
                        .unwrap();
                    }
                    _ => write_binding(&fixture, &root),
                }
                if case == 2 {
                    let path = fixture.metadata.join(BINDING);
                    let mut bytes = std::fs::read(&path).unwrap();
                    bytes[0] ^= 1;
                    std::fs::write(path, bytes).unwrap();
                }
                let result = StoreInitializer::new(1)
                    .unwrap()
                    .prepare_pending_bound(pending, StoreMode::CreateNew(selection()), expected)
                    .await;
                assert!(
                    matches!(result, Err(PendingError::Retired(Error::InvalidBinding))),
                    "case {case}"
                );
                no_store_effects(&fixture);
            }
        })
        .await;
}

#[tokio::test]
async fn actual_metadata_recheck_rejects_changed_binding_before_store_effects() {
    LORE_CONTEXT
        .scope(execution(), async {
            let fixture = Fixture::new();
            let root = backing(&fixture, "backing");
            let wrong = backing(&fixture, "wrong");
            let pending = pending_metadata(&fixture, Mode::CreateNew(config())).await;
            write_binding(&fixture, &root);
            let location = fixture.registration.bootstrap_location().unwrap();
            let jobs = StoreInitializer::new(1).unwrap();
            jobs.pause_metadata_recheck(move || {
                std::fs::remove_file(location.metadata().path().join(BINDING)).unwrap();
                BackingBinding::write_new(
                    location.metadata().path(),
                    location.repository(),
                    location.instance(),
                    &wrong,
                )
                .unwrap();
            });
            assert!(matches!(
                jobs.prepare_pending_bound(pending, StoreMode::CreateNew(selection()), root)
                    .await,
                Err(PendingError::Retired(Error::InvalidBinding))
            ));
            no_store_effects(&fixture);
        })
        .await;
}

#[tokio::test]
async fn binding_is_allowed_only_in_strict_bound_new_inventory() {
    LORE_CONTEXT
        .scope(execution(), async {
            for bound in [false, true] {
                let fixture = Fixture::new();
                let root = backing(&fixture, "backing");
                let pending = pending_metadata(&fixture, Mode::CreateNew(config())).await;
                write_binding(&fixture, &root);
                let jobs = StoreInitializer::new(1).unwrap();
                let result = if bound {
                    std::fs::write(fixture.metadata.join("unknown"), b"unrecognized").unwrap();
                    jobs.prepare_pending_bound(pending, StoreMode::CreateNew(selection()), root)
                        .await
                } else {
                    jobs.prepare_pending(pending, StoreMode::CreateNew(selection()))
                        .await
                };
                assert!(matches!(
                    result,
                    Err(PendingError::Retired(Error::NotEmpty))
                ));
                no_store_effects(&fixture);
            }
        })
        .await;
}

#[tokio::test]
async fn bound_online_remote_and_enabled_shared_configuration_refused_before_effects() {
    for shared in [false, true] {
        let context = test_execution_with(LoreGlobalArgs {
            offline: u8::from(shared),
            ..Default::default()
        });
        LORE_CONTEXT
            .scope(context, async {
                let fixture = Fixture::new();
                let root = backing(&fixture, "backing");
                let mut configuration = config();
                configuration.remote_url = Some("http://127.0.0.1:1".to_string());
                if shared {
                    configuration.shared_store_to_use = Some(SharedStoreToUseConfig {
                        use_shared_store: Some(true),
                        ..Default::default()
                    });
                }
                let pending = pending_metadata(&fixture, Mode::CreateNew(configuration)).await;
                write_binding(&fixture, &root);
                assert!(matches!(
                    StoreInitializer::new(1)
                        .unwrap()
                        .prepare_pending_bound(pending, StoreMode::CreateNew(selection()), root)
                        .await,
                    Err(PendingError::Retired(Error::UnsupportedBoundStores))
                ));
                no_store_effects(&fixture);
            })
            .await;
    }
}

#[tokio::test]
async fn bound_offline_configuration_keeps_persisted_remote_without_connecting() {
    LORE_CONTEXT
        .scope(execution(), async {
            let fixture = Fixture::new();
            let root = backing(&fixture, "backing");
            let mut configuration = config();
            configuration.remote_url = Some("http://127.0.0.1:1".to_string());
            let pending = pending_metadata(&fixture, Mode::CreateNew(configuration)).await;
            write_binding(&fixture, &root);
            let working = StoreInitializer::new(1)
                .unwrap()
                .prepare_pending_bound(pending, StoreMode::CreateNew(selection()), root)
                .await
                .unwrap();
            assert_eq!(working.selection(), &selection());
            drop(working);
        })
        .await;
}

#[test]
fn bound_cancellation_retains_actual_job_parents_and_client_after_runtime_drop() {
    super::cancelled_store_job(true);
}
