// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

use lore_revision::repository::working_bootstrap::RemoteContents;

use super::*;

#[path = "../../../../lore-revision/tests/fixtures/remote_contents.rs"]
mod remote_fixture;
use remote_fixture::RemoteFixture;

async fn request(
    server: &Arc<RemoteFixture>,
    fixture: &Fixture,
    source: &Source,
    fresh: bool,
) -> BootstrapRequest {
    let contents: RemoteContents = server
        .contents(lore_revision::lore::execution_context())
        .await;
    assert_eq!(contents.remote_url(), server.url());
    BootstrapRequest {
        registration: fixture.registration.as_ref().unwrap().clone(),
        mode: if fresh {
            BootstrapMode::RemoteCreate {
                backing: fixture.backing.clone(),
                config: RepositoryConfig {
                    remote_url: Some(contents.remote_url().to_owned()),
                    identity: Some(contents.identity().to_owned()),
                    vfs: Some(VfsConfig {
                        vfs_type: VfsType::ExperimentalWorking,
                    }),
                    ..Default::default()
                },
                selection: Selection {
                    branch: [214; 16].into(),
                    branch_name: "main".into(),
                    revision: source.revision,
                },
                contents,
            }
        } else {
            BootstrapMode::ReuseRemote { contents }
        },
    }
}
#[tokio::test]
async fn online_factory_hydrates_only_after_read_and_reuses_local_checkpoint() {
    let execution = Arc::new(
        lore_revision::interface::ExecutionContext::new_client_with_user_id(
            lore_revision::interface::LoreGlobalArgs::default(),
            lore_revision::relay::EventDispatcher::no_dispatch(),
            "hydration-user".into(),
        ),
    );
    Box::pin(LORE_CONTEXT.scope(execution, async {
        let mut fixture = Fixture::new();
        let source = Box::pin(Source::new(&fixture)).await;
        let state =
            lore_revision::state::State::deserialize(source.repository.clone(), source.revision)
                .await
                .unwrap();
        let node = state
            .find_node(source.repository.clone(), "original.txt")
            .await
            .unwrap();
        let server = RemoteFixture::new(source.repository.clone(), vec![node.address.hash]);
        let mut gets_after_first_read = 0;
        for fresh in [true, false] {
            let request = request(&server, &fixture, &source, fresh).await;
            let (pause, model, resume) = pause();
            let supervisor = fixture.supervisor.clone();
            let mut preparing = Box::pin(supervisor.test_bootstrap_prepare(
                request,
                policy(),
                OwnerLimits::default(),
                lore::working_mount::Options {
                    digest_cache: Some(fixture._root.join("digests")),
                },
                pause,
            ));
            let model = tokio::select! {
                model = bounded(model) => model.unwrap(),
                result = &mut preparing => panic!("online factory ended early: {result:?}"),
            };
            let attrs = model.lookup(1, b"original.txt").await.unwrap();
            assert!(server.metadata_gets() > 0);
            assert_eq!(server.branch_queries(), 0);
            assert_eq!(server.mutations(), 0);
            assert_eq!(model.digest(attrs.entry.inode).await.unwrap(), None);
            assert_eq!(
                server.payload_gets(),
                gets_after_first_read,
                "mount, metadata and cold digest must not fetch file contents"
            );
            let handle = model
                .open(
                    attrs.entry.inode,
                    OpenFlags {
                        write: true,
                        append: false,
                        truncate: false,
                    },
                )
                .await
                .unwrap();
            let expected: &[u8] = if fresh {
                b"source committed bytes"
            } else {
                b"private target changes"
            };
            if fresh {
                server.set_payload_denied(true);
                assert!(model.read(handle, 0, 100).await.is_err());
                server.set_payload_denied(false);
            }
            assert_eq!(model.read(handle, 0, 100).await.unwrap(), expected);
            if fresh {
                gets_after_first_read = server.payload_gets();
                assert!(gets_after_first_read > 0);
                assert!(server.sessions_detached());
                assert_eq!(model.read(handle, 0, 100).await.unwrap(), expected);
                assert_eq!(server.payload_gets(), gets_after_first_read);
                model
                    .write(handle, 0, b"private target changes")
                    .await
                    .unwrap();
                model.fsync(handle, false).await.unwrap();
            }
            model.release(handle).await.unwrap();
            drop(model);
            resume.send(()).unwrap();
            let handle = bounded(preparing).await.unwrap();
            remove(&fixture, &handle).await;
            source.assert_unchanged().await;
            if fresh {
                fixture.register_again();
            }
        }
    }))
    .await;
}

#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Linux /dev/fuse and unprivileged mount support"]
async fn mounted_online_factory_fetches_on_first_read_then_preserves_private_bytes_on_reuse() {
    let execution = Arc::new(
        lore_revision::interface::ExecutionContext::new_client_with_user_id(
            lore_revision::interface::LoreGlobalArgs::default(),
            lore_revision::relay::EventDispatcher::no_dispatch(),
            "hydration-mounted-user".into(),
        ),
    );
    Box::pin(LORE_CONTEXT.scope(execution, async {
        let mut fixture = Fixture::new();
        let source = Box::pin(Source::new(&fixture)).await;
        let state =
            lore_revision::state::State::deserialize(source.repository.clone(), source.revision)
                .await
                .unwrap();
        let node = state
            .find_node(source.repository.clone(), "original.txt")
            .await
            .unwrap();
        let server = RemoteFixture::new(source.repository.clone(), vec![node.address.hash]);
        let mut fetched = 0;
        for fresh in [true, false] {
            let handle = bounded(fixture.supervisor.bootstrap_mount(
                request(&server, &fixture, &source, fresh).await,
                policy(),
                OwnerLimits::default(),
                lore::working_mount::Options {
                    digest_cache: Some(fixture._root.join("digests")),
                },
            ))
            .await
            .unwrap();
            let path = fixture.target.join("original.txt");
            let names: Vec<_> = std::fs::read_dir(&fixture.target)
                .unwrap()
                .map(|entry| entry.unwrap().file_name())
                .collect();
            assert!(names.iter().any(|name| name == "original.txt"));
            assert_private_bytes_have_no_immutable_hash(&path);
            assert_eq!(
                server.payload_gets(),
                fetched,
                "listing and xattr must not hydrate contents"
            );
            let expected: &[u8] = if fresh {
                b"source committed bytes"
            } else {
                b"online private bytes"
            };
            assert_eq!(bounded(tokio::fs::read(&path)).await.unwrap(), expected);
            if fresh {
                fetched = server.payload_gets();
                assert!(fetched > 0);
                assert_eq!(bounded(tokio::fs::read(&path)).await.unwrap(), expected);
                assert_eq!(server.payload_gets(), fetched);
                bounded(async {
                    use tokio::io::AsyncWriteExt;
                    let mut file = tokio::fs::OpenOptions::new()
                        .write(true)
                        .truncate(true)
                        .open(&path)
                        .await
                        .unwrap();
                    file.write_all(b"online private bytes").await.unwrap();
                    file.sync_all().await.unwrap();
                })
                .await;
                assert_private_bytes_have_no_immutable_hash(&path);
            }
            remove(&fixture, &handle).await;
            source.assert_unchanged().await;
            if fresh {
                fixture.register_again();
            }
        }
    }))
    .await;
}
#[tokio::test]
async fn online_linked_contents_keep_exact_pin_and_recheck_current_policy_after_reuse() {
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::Ordering;

    use lore_base::types::Address;
    use lore_base::types::Context;
    use lore_revision::node::Node;
    use lore_revision::node::NodeFlags;
    use lore_revision::node::ROOT_NODE;
    use lore_revision::state::State;

    let execution = Arc::new(
        lore_revision::interface::ExecutionContext::new_client_with_user_id(
            lore_revision::interface::LoreGlobalArgs::default(),
            lore_revision::relay::EventDispatcher::no_dispatch(),
            "linked-hydration-user".into(),
        ),
    );
    Box::pin(LORE_CONTEXT.scope(execution, async {
        let mut fixture = Fixture::new();
        let mut source = Box::pin(Source::new(&fixture)).await;
        let child_id = lore_base::types::RepositoryId::from([218; 16]);
        let allowed = Arc::new(AtomicBool::new(true));
        let link_policy = allowed.clone();
        let root_id = source.repository.id;
        source.repository = Arc::new(source.repository.without_command_locks().with_link_read(
            Arc::new(move |id| {
                id == root_id || (id == child_id && link_policy.load(Ordering::SeqCst))
            }),
        ));
        let token = RepositoryWriteToken::acquire(&source.path).await;
        let writable = Arc::new(
            source
                .repository
                .without_command_locks()
                .with_write_token(token.share()),
        );
        let child = writable.to_link_context(child_id).await;
        let address = lore_revision::immutable::write_borrowed(
            child.clone(),
            Context::from([17; 16]),
            b"pinned online child bytes",
            Default::default(),
        )
        .await
        .unwrap();
        let child_state = State::new();
        child_state
            .node_add(
                child.clone(),
                ROOT_NODE,
                Node {
                    flags: NodeFlags::File.bits(),
                    size: 25,
                    address,
                    name_hash: lore_storage::hash::hash_string("remote.txt"),
                    ..Default::default()
                },
                "remote.txt",
            )
            .await
            .unwrap();
        let child_pin = child_state
            .serialize(child.clone(), child.try_write_token().unwrap())
            .await
            .unwrap();
        let state = State::new();
        state
            .node_add(
                writable.clone(),
                ROOT_NODE,
                Node {
                    flags: NodeFlags::Link.bits(),
                    child: ROOT_NODE,
                    address: Address {
                        hash: child_pin,
                        context: child_id.into(),
                    },
                    name_hash: lore_storage::hash::hash_string("vendor"),
                    ..Default::default()
                },
                "vendor",
            )
            .await
            .unwrap();
        let selected = state
            .serialize(writable.clone(), writable.try_write_token().unwrap())
            .await
            .unwrap();
        writable.flush(true).await.unwrap();
        drop(child);
        drop(writable);
        drop(token);
        let server = RemoteFixture::new(source.repository.clone(), vec![address.hash]);
        let mut fetched = 0;
        for fresh in [true, false] {
            let mut request = request(&server, &fixture, &source, fresh).await;
            if let BootstrapMode::RemoteCreate { selection, .. } = &mut request.mode {
                selection.revision = selected;
            }
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
                result = &mut preparing => panic!("linked online factory ended early: {result:?}"),
            };
            let directory = model.lookup(1, b"vendor").await.unwrap();
            let entry = model
                .lookup(directory.entry.inode, b"remote.txt")
                .await
                .unwrap();
            assert_eq!(
                server.payload_gets(),
                fetched,
                "linked metadata must not fetch payload"
            );
            let handle = model
                .open(
                    entry.entry.inode,
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
                b"pinned online child bytes"
            );
            fetched = server.payload_gets();
            assert!(fetched > 0);
            allowed.store(false, Ordering::SeqCst);
            assert!(
                model.read(handle, 0, 100).await.is_err(),
                "cached child contents must not bypass current source policy"
            );
            allowed.store(true, Ordering::SeqCst);
            assert_eq!(
                model.read(handle, 0, 100).await.unwrap(),
                b"pinned online child bytes"
            );
            assert_eq!(server.payload_gets(), fetched);
            model.fsync(handle, false).await.unwrap();
            model.release(handle).await.unwrap();
            drop(model);
            resume.send(()).unwrap();
            let handle = bounded(preparing).await.unwrap();
            remove(&fixture, &handle).await;
            source.assert_unchanged().await;
            if fresh {
                fixture.register_again();
            }
        }
        assert!(server.sessions_detached());
    }))
    .await;
}
#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Linux /dev/fuse and unprivileged mount support"]
async fn remote_mount_command_uses_current_source_and_releases_callback_work_while_mounted() {
    use lore::remote::command::LoreCommand;
    use lore::service::owner::RunCleanup;
    use lore::service::state::with_invocation;
    use lore::service::state::with_owner;
    use lore_revision::interface::LoreGlobalArgs;

    let execution = Arc::new(
        lore_revision::interface::ExecutionContext::new_client_with_user_id(
            LoreGlobalArgs::default(),
            lore_revision::relay::EventDispatcher::no_dispatch(),
            "remote-command-user".into(),
        ),
    );
    Box::pin(LORE_CONTEXT.scope(execution, async {
        let mut fixture = Fixture::new();
        let mut source = Box::pin(Source::new(&fixture)).await;
        drop(fixture.registration.take());
        let state =
            lore_revision::state::State::deserialize(source.repository.clone(), source.revision)
                .await
                .unwrap();
        let node = state
            .find_node(source.repository.clone(), "original.txt")
            .await
            .unwrap();
        let server = RemoteFixture::new(source.repository.clone(), vec![node.address.hash]);
        let config_path = source.path.join(".lore/config.toml");
        let config = RepositoryConfig {
            remote_url: Some(server.url().to_owned()),
            identity: Some("fixture".into()),
            ..Default::default()
        };
        let config_bytes = toml::to_string(&config).unwrap().into_bytes();
        std::fs::write(&config_path, &config_bytes).unwrap();
        for (path, bytes) in &mut source.metadata {
            if *path == config_path {
                *bytes = config_bytes.clone();
            }
        }
        let (owner, _guard, _admission) =
            super::super::working_mount_local_policy::admitted().await;
        let _cleanup = RunCleanup::new(owner.clone());
        let callback_owner = Arc::new(());
        let weak_callback = Arc::downgrade(&callback_owner);
        let callback: lore::interface::LoreEventCallback = Some(Box::new(move |_event| {
            let _retained = &callback_owner;
        }));
        let globals = LoreGlobalArgs {
            repository_path: source.path.as_path().into(),
            ..Default::default()
        };
        let args = lore::mount_working::LoreMountWorkingArgs {
            experimental: true,
            remote_contents: true,
            source: source.path.as_path().into(),
            revision: format!("main@{}", source.revision).into(),
            mountpoint: fixture.target.as_path().into(),
            metadata: fixture.metadata.as_path().into(),
            backing: fixture.backing.as_path().into(),
            digest_cache: fixture._root.join("digests").as_path().into(),
            ..Default::default()
        };
        let code = bounded(with_invocation(
            owner.state.try_begin_invocation().unwrap(),
            with_owner(
                owner.clone(),
                Box::pin(LoreCommand::MountWorking(args).invoke_local(globals, callback)),
            ),
        ))
        .await
        .unwrap();
        assert_eq!(code, 0);
        bounded(async {
            while owner.state.work_invocation_count() != 0 || weak_callback.upgrade().is_some() {
                tokio::task::yield_now().await;
            }
        })
        .await;
        let token = bounded(RepositoryWriteToken::acquire(&source.path)).await;
        drop(token);
        let path = fixture.target.join("original.txt");
        assert!(path.metadata().unwrap().is_file());
        assert_private_bytes_have_no_immutable_hash(&path);
        assert_eq!(server.payload_gets(), 0);
        assert_eq!(
            bounded(tokio::fs::read(&path)).await.unwrap(),
            b"source committed bytes"
        );
        let fetched = server.payload_gets();
        assert!(fetched > 0);
        assert!(server.sessions_detached());
        assert_eq!(
            bounded(tokio::fs::read(&path)).await.unwrap(),
            b"source committed bytes"
        );
        assert_eq!(server.payload_gets(), fetched);
        assert_eq!(owner.state.work_invocation_count(), 0);
        let control = owner.state.try_begin_control_invocation().unwrap();
        bounded(with_invocation(
            control,
            with_owner(owner.clone(), async { owner.stop_control(true).await }),
        ))
        .await
        .unwrap()
        .unwrap();
        assert!(!path.exists());
        source.assert_unchanged().await;
    }))
    .await;
}

mod transition_tests;
