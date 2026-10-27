// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use lore_base::types::Address;
use lore_revision::node::Node;
use lore_revision::node::NodeFlags;
use lore_revision::node::ROOT_NODE;
use lore_revision::repository::working_bootstrap::ImmutableImport;
use lore_revision::repository::working_bootstrap::ImportReadPolicy;
use lore_revision::state::State;

use super::*;

#[tokio::test]
async fn linked_reuse_requires_current_policy_and_observes_revocation() {
    Box::pin(
        LORE_CONTEXT.scope(super::super::working_mount::execution(), async {
            let mut fixture = Fixture::new();
            let source = Box::pin(Source::new(&fixture)).await;
            let allowed = Arc::new(AtomicBool::new(true));
            let decision = allowed.clone();
            let current = Arc::new(
                source
                    .repository
                    .without_command_locks()
                    .with_link_read(Arc::new(move |_repository| decision.load(Ordering::SeqCst))),
            );
            let token = RepositoryWriteToken::acquire(&source.path).await;
            let writable = Arc::new(
                current
                    .without_command_locks()
                    .with_write_token(token.share()),
            );
            let linked = State::deserialize(writable.clone(), source.revision)
                .await
                .unwrap();
            linked
                .node_add(
                    writable.clone(),
                    ROOT_NODE,
                    Node {
                        flags: NodeFlags::Link.bits(),
                        name_hash: lore_storage::hash::hash_string("pinned"),
                        address: Address {
                            hash: source.revision,
                            context: source.repository.id.into(),
                        },
                        child: ROOT_NODE,
                        ..Default::default()
                    },
                    "pinned",
                )
                .await
                .unwrap();
            let revision = linked.serialize(writable.clone(), &token).await.unwrap();
            writable.flush(true).await.unwrap();
            drop(writable);
            drop(token);
            let seed = ImmutableImport::new(current.clone(), revision)
                .await
                .unwrap();
            let mut fresh = source.request(&fixture).await;
            if let BootstrapMode::Import {
                selection,
                seed: destination,
                ..
            } = &mut fresh.mode
            {
                selection.revision = revision;
                *destination = seed;
            } else {
                unreachable!()
            }
            for request in [Some(fresh), None] {
                let request = request.unwrap_or_else(|| BootstrapRequest {
                    registration: fixture.registration.as_ref().unwrap().clone(),
                    mode: BootstrapMode::ReuseWithPolicy {
                        policy: ImportReadPolicy::from_repository(&current).unwrap(),
                    },
                });
                let (pause, model, resume) = pause();
                let supervisor = fixture.supervisor.clone();
                let mut preparing = Box::pin(supervisor.test_bootstrap_prepare(
                    request,
                    policy(),
                    OwnerLimits::default(),
                    Default::default(),
                    pause,
                ));
                let model = tokio::select! {
                    model = bounded(model) => model.unwrap(),
                    result = &mut preparing => panic!("linked prepare ended early: {result:?}"),
                };
                let directory = model.lookup(1, b"pinned").await.unwrap();
                let file = model
                    .lookup(directory.entry.inode, b"original.txt")
                    .await
                    .unwrap();
                let handle = model
                    .open(
                        file.entry.inode,
                        OpenFlags {
                            write: false,
                            append: false,
                            truncate: false,
                        },
                    )
                    .await
                    .unwrap();
                assert_eq!(
                    model.read(handle, 0, 100).await.unwrap(),
                    b"source committed bytes"
                );
                allowed.store(false, Ordering::SeqCst);
                assert!(model.read(handle, 0, 100).await.is_err());
                allowed.store(true, Ordering::SeqCst);
                model.release(handle).await.unwrap();
                drop(model);
                resume.send(()).unwrap();
                let handle = bounded(preparing).await.unwrap();
                remove(&fixture, &handle).await;
                source.assert_unchanged().await;
                fixture.register_again();
            }
            let foreign = current.to_link_context([77; 16].into()).await;
            let binding_before =
                std::fs::read(fixture.metadata.join(".working-backing-v1")).unwrap();
            let (wrong_pause, wrong_model, wrong_resume) = pause();
            wrong_resume.send(()).unwrap();
            drop(wrong_model);
            let rejected = bounded(fixture.supervisor.test_bootstrap_prepare(
                BootstrapRequest {
                    registration: fixture.registration.as_ref().unwrap().clone(),
                    mode: BootstrapMode::ReuseWithPolicy {
                        policy: ImportReadPolicy::from_repository(&foreign).unwrap(),
                    },
                },
                policy(),
                OwnerLimits::default(),
                Default::default(),
                wrong_pause,
            ))
            .await
            .unwrap_err();
            assert!(rejected.handle.is_none());
            assert_eq!(
                std::fs::read(fixture.metadata.join(".working-backing-v1")).unwrap(),
                binding_before
            );
            let (pause, model, resume) = pause();
            let supervisor = fixture.supervisor.clone();
            let mut preparing = Box::pin(supervisor.test_bootstrap_prepare(
                fixture.request(false),
                policy(),
                OwnerLimits::default(),
                Default::default(),
                pause,
            ));
            tokio::select! {
                model = bounded(model) => {
                    if let Ok(model) = model {
                        assert!(model.lookup(1, b"pinned").await.is_err());
                        drop(model);
                        resume.send(()).unwrap();
                        let handle = bounded(preparing).await.unwrap();
                        remove(&fixture, &handle).await;
                    } else {
                        let failure = bounded(preparing).await.unwrap_err();
                        if let Some(handle) = failure.handle {
                            remove(&fixture, &handle).await;
                        }
                    }
                }
                result = &mut preparing => {
                    let failure = result.unwrap_err();
                    if let Some(handle) = failure.handle {
                            remove(&fixture, &handle).await;
                        }
                }
            }
            source.assert_unchanged().await;
            if std::env::var_os("LORE_KEEP_TEST_DATA").is_some() {
                eprintln!(
                    "linked CLI source={} revision={}",
                    source.path.display(),
                    revision
                );
            }
        }),
    )
    .await;
}
