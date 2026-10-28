// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

use lore_base::runtime::LORE_CONTEXT;
use lore_base::types::Context;
use lore_base::types::Hash;
use lore_revision::interface::ExecutionContext;
use lore_revision::interface::LoreGlobalArgs;
use lore_revision::node::Node;
use lore_revision::node::NodeFlags;
use lore_revision::node::ROOT_NODE;
use lore_revision::relay::EventDispatcher;
use lore_revision::repository::RepositoryContext;
use lore_revision::repository::working_bootstrap::BackingBinding;
use lore_revision::repository::working_bootstrap::Selection;
use lore_revision::repository::working_bootstrap::StoreInitializer;
use lore_revision::repository::working_bootstrap::StoreMode;
use lore_revision::state::State;

use super::*;

#[path = "../../../fixtures/remote_contents.rs"]
mod serving;

async fn source() -> (Arc<RepositoryContext>, Hash, lore_base::types::Address) {
    let immutable = lore_storage::local::immutable_store::create(
        None::<&str>,
        lore_storage::local::immutable_store::ImmutableStoreCreateOptions::none(),
        false,
        Default::default(),
    )
    .await
    .unwrap();
    let mutable = lore_storage::local::mutable_store::create(
        None::<&str>,
        Default::default(),
        immutable.clone(),
    )
    .await
    .unwrap();
    let repository = Arc::new(RepositoryContext::new_server_context(
        immutable,
        mutable,
        [51; 16].into(),
    ));
    let address = lore_revision::immutable::write_borrowed(
        repository.clone(),
        Context::from([9; 16]),
        b"verified lazy file bytes",
        lore_storage::options::WriteOptions::default(),
    )
    .await
    .unwrap();
    let state = State::new();
    state
        .node_add(
            repository.clone(),
            ROOT_NODE,
            Node {
                flags: NodeFlags::File.bits(),
                size: 24,
                address,
                name_hash: lore_storage::hash::hash_string("cold.txt"),
                ..Default::default()
            },
            "cold.txt",
        )
        .await
        .unwrap();
    let revision = state
        .serialize(repository.clone(), repository.try_write_token().unwrap())
        .await
        .unwrap();
    lore_revision::branch::create(
        repository.clone(),
        repository.try_write_token().unwrap(),
        [62; 16].into(),
        "main",
        lore_revision::branch::default_category(),
        "fixture",
        1,
        vec![],
        false,
        false,
    )
    .await
    .unwrap();
    repository.flush(true).await.unwrap();
    (repository, revision, address)
}

#[tokio::test]
async fn bound_remote_state_is_metadata_only_then_verified_payload_is_cached_after_caller_drop() {
    Box::pin(async {
        let tag = Arc::new(());
        let weak = Arc::downgrade(&tag);
        let mut caller =
            ExecutionContext::new_client(LoreGlobalArgs::default(), EventDispatcher::no_dispatch());
        caller.set_caller_state(tag);
        let caller = Arc::new(caller);
        let detached = Arc::new(caller.for_background_owner().await);
        let (source, revision, address) = LORE_CONTEXT.scope(detached, source()).await;
        let server = serving::RemoteFixture::new(source, vec![address.hash]);
        let fixture = Fixture::new();
        let contents = server.contents(caller.clone()).await;
        let configuration = RepositoryConfig {
            remote_url: Some(contents.remote_url().to_owned()),
            identity: Some(contents.identity().to_owned()),
            vfs: Some(VfsConfig {
                vfs_type: VfsType::ExperimentalWorking,
            }),
            ..Default::default()
        };
        let root = fixture._root.join("backing");
        std::fs::create_dir(&root).unwrap();
        let selection = Selection {
            branch: [62; 16].into(),
            branch_name: "main".into(),
            revision,
        };
        let prepared = LORE_CONTEXT
            .scope(caller.clone(), async {
                let pending = MetadataInitializer::new(1)
                    .unwrap()
                    .prepare_pending(
                        fixture.registration.clone(),
                        fixture.token().await,
                        Mode::CreateNew(configuration),
                    )
                    .await
                    .unwrap();
                let location = pending.location();
                BackingBinding::write_new(
                    location.metadata().path(),
                    location.repository(),
                    location.instance(),
                    &root,
                )
                .unwrap();
                StoreInitializer::new(1)
                    .unwrap()
                    .prepare_pending_bound(
                        pending,
                        StoreMode::RemoteCreate(selection.clone(), contents),
                        root,
                    )
                    .await
                    .unwrap()
            })
            .await;
        assert_eq!(prepared.selection(), &selection);
        assert_eq!(server.branch_queries(), 0);
        assert_eq!(server.mutations(), 0);
        assert!(server.metadata_gets() > 0);
        assert_eq!(server.payload_gets(), 0);
        let payload = prepared.into_owner_build().unwrap();
        let (context, publication) = payload.into_trusted_owner_parts();
        drop(caller);
        assert!(weak.upgrade().is_none());
        server.set_payload_denied(true);
        let options = lore_revision::immutable::read_options_from_repository(&context.repository);
        assert!(!options.trust_remote);
        let read = LORE_CONTEXT
            .scope(context.execution.clone(), async {
                lore_revision::immutable::read(context.repository.clone(), address, None, options)
                    .await
            })
            .await;
        assert!(read.is_err());
        let failed = server.payload_gets();
        assert!(failed > 0);
        server.set_payload_denied(false);
        for attempt in 0..2 {
            let bytes = LORE_CONTEXT
                .scope(context.execution.clone(), async {
                    lore_revision::immutable::read(
                        context.repository.clone(),
                        address,
                        None,
                        options,
                    )
                    .await
                })
                .await
                .unwrap();
            assert_eq!(bytes.as_ref(), b"verified lazy file bytes");
            if attempt == 0 {
                context.repository.flush(true).await.unwrap();
            }
        }
        assert_eq!(server.payload_gets(), failed + 1);
        assert!(server.sessions_detached());
        assert!(server.url().starts_with("workinghydration"));
        drop(context);
        drop(publication);
    })
    .await;
}
