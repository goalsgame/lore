// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

use super::*;
use crate::fs::filesystem_provider::HoldOptions;
use crate::fs::filesystem_provider::RootHoldRequest;
use crate::interface::LoreArray;
use crate::interface::LoreGlobalArgs;
use crate::interface::LoreString;
use crate::node::NodeFlags;
use crate::state::State;

#[tokio::test]
async fn dirty_staged_preparation_rebases_through_real_view_without_changing_anchors() {
    let fixture = Fixture::new().await;
    let resolver: Arc<dyn ProviderResolver> = fixture.registry.clone();
    let mut execution = ExecutionContext::new_client_with_user_id(
        LoreGlobalArgs {
            offline: 1,
            ..Default::default()
        },
        crate::relay::EventDispatcher::no_dispatch(),
        "working-handoff-fixture".into(),
    );
    execution.set_provider_resolver(&resolver);
    LORE_CONTEXT
        .scope(Arc::new(execution), async {
            let repository = fixture.load(RepositoryAccess::ReadWrite).await;
            let token = repository.try_write_token().unwrap();
            let (target_hash, staged_hash) = repository
                .prepare_io(async {
                    // Seed genuine committed metadata through normal OS stage/commit.
                    // The actual request below still uses the registered held provider.
                    let normal = Arc::new(RepositoryContext {
                        file_system: Arc::new(crate::fs::os::OsFilesystem::new(
                            fixture.registration.target().path(),
                        )),
                        ..repository.inherited()
                    });
                    let dirty_path = fixture.registration.target().path().join("dirty.txt");
                    std::fs::write(&dirty_path, b"base committed bytes").unwrap();
                    Box::pin(crate::file::stage::stage(
                        normal.clone(),
                        token,
                        LoreArray::from_vec(vec![LoreString::from(&dirty_path)]),
                        crate::stage::StageOptions {
                            scan: true,
                            ..Default::default()
                        },
                    ))
                    .await
                    .unwrap();
                    let current_hash = crate::commit::commit_boxed(
                        normal.clone(),
                        token,
                        crate::commit::CommitOptions::new("base".into()),
                    )
                    .await
                    .unwrap();
                    let target_path = fixture.registration.target().path().join("target-only.txt");
                    std::fs::write(&target_path, b"target committed bytes").unwrap();
                    Box::pin(crate::file::stage::stage(
                        normal.clone(),
                        token,
                        LoreArray::from_vec(vec![LoreString::from(&target_path)]),
                        crate::stage::StageOptions {
                            scan: true,
                            ..Default::default()
                        },
                    ))
                    .await
                    .unwrap();
                    let target_hash = crate::commit::commit_boxed(
                        normal,
                        token,
                        crate::commit::CommitOptions::new("target".into()),
                    )
                    .await
                    .unwrap();
                    assert_ne!(current_hash, target_hash);
                    crate::instance::store_current_anchor(&repository, current_hash)
                        .await
                        .unwrap();
                    let staged = State::deserialize(repository.clone(), current_hash)
                        .await
                        .unwrap();
                    let file = staged
                        .find_node_link(repository.clone(), "dirty.txt")
                        .await
                        .unwrap()
                        .node;
                    staged
                        .node_mark_dirty(repository.clone(), file, NodeFlags::DirtyModify, true)
                        .await
                        .unwrap();
                    staged.reparent_onto(current_hash);
                    let staged_hash = staged.serialize(repository.clone(), token).await.unwrap();
                    crate::instance::store_staged_anchor(&repository, staged_hash)
                        .await
                        .unwrap();
                    std::fs::write(&dirty_path, b"private dirty bytes").unwrap();
                    repository.flush(true).await.unwrap();
                    Ok((target_hash, staged_hash))
                })
                .await
                .unwrap();
            let old = strict_anchors::read(&repository).await.unwrap();
            let request = RootHoldRequest::prepare(
                &repository,
                target_hash,
                old.branch,
                HoldOptions::default(),
            )
            .await
            .unwrap();
            assert_eq!(request.old_anchors(), old);
            assert_eq!(request.handoff().read_anchors().await.unwrap(), old);
            assert_eq!(request.new_anchors().current, target_hash);
            let rebased = request
                .new_anchors()
                .staged
                .expect("dirty marker must survive");
            assert_ne!(rebased, staged_hash);
            let raw = Arc::new(request.handoff().anchors.inherited());
            let state = State::deserialize(raw.clone(), rebased).await.unwrap();
            assert_eq!(state.committed_revision(), target_hash);
            assert!(
                state
                    .find_node(raw.clone(), "dirty.txt")
                    .await
                    .unwrap()
                    .is_dirty_modify()
            );
            assert!(
                !state
                    .find_node(raw, "target-only.txt")
                    .await
                    .unwrap()
                    .is_dirty()
            );
            drop(request);
            assert_eq!(strict_anchors::read(&repository).await.unwrap(), old);
            assert_eq!(
                std::fs::read(fixture.registration.target().path().join("dirty.txt")).unwrap(),
                b"private dirty bytes"
            );
        })
        .await;
}
