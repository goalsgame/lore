// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use std::sync::Arc;

use lore_base::lore_spawn;
use lore_base::runtime::LORE_CONTEXT;
use lore_base::types::Address;
use lore_base::types::Context;
use lore_base::types::Hash;
use lore_revision::commit::BasedOn;
use lore_revision::commit::weave_history;
use lore_revision::lore::BranchId;
use lore_revision::node::*;
use lore_revision::repository::RepositoryContext;
use lore_revision::revision::LastChangedResolver;
use lore_revision::state::State;

use crate::helper::in_memory;
use crate::helper::in_memory::add;
use crate::helper::in_memory::add_staged;
use crate::helper::in_memory::branch_id;
use crate::helper::in_memory::commit;
use crate::helper::in_memory::file;
use crate::helper::in_memory::metadata_on;
use crate::helper::in_memory::revision_state;
use crate::helper::in_memory::token;
use crate::helper::test_store_create;

/// Stages a content change to the file at `path` of `state`, staged with `staged`.
async fn modify(state: &State, repository: &Arc<RepositoryContext>, path: &str, staged: NodeFlags) {
    let node_id = node_id_at(state, repository, path).await;
    let address = Address {
        hash: Hash::from(rand::random::<[u8; 32]>()),
        context: Context::default(),
    };
    state
        .node_modify(repository.clone(), node_id, 0o644, 11, address)
        .await
        .expect("modifying the file");
    state
        .node_mark_staged(repository.clone(), node_id, staged, NodeFlags::DirtyModify)
        .await
        .expect("staging the modification");
}

async fn node_id_at(state: &State, repository: &Arc<RepositoryContext>, path: &str) -> NodeID {
    state
        .find_node_link(repository.clone(), path)
        .await
        .expect("finding the node")
        .node
}

/// Commits the changes staged on `state` as a merge of `parent_other` into `parent_self` on
/// `branch`, weaving the history of both parents onto the tree, which was built from the
/// revision `based_on` names.
async fn commit_merge(
    repository: &Arc<RepositoryContext>,
    state: &Arc<State>,
    parent_self: Hash,
    parent_other: Hash,
    based_on: BasedOn,
    branch: BranchId,
) -> Hash {
    let frozen = commit(repository, state, Hash::default(), branch_id()).await;
    let merge = revision_state(repository, frozen).await;
    merge.set_parent_self(parent_self);
    merge.set_parent_other(parent_other);
    merge.set_metadata_hash(
        metadata_on(branch)
            .serialize(repository.clone())
            .await
            .expect("serializing the metadata"),
    );
    weave_history(repository.clone(), merge.clone(), based_on)
        .await
        .expect("weaving the merge history");
    merge.mark_dirty();
    merge
        .serialize(repository.clone(), &token())
        .await
        .expect("serializing the merge")
}

/// The revision that last changed `path`, as of `revision`.
async fn last_changed(repository: &Arc<RepositoryContext>, revision: Hash, path: &str) -> Hash {
    let state = revision_state(repository, revision).await;
    let link = state
        .find_node_link(repository.clone(), path)
        .await
        .expect("finding the node");
    LastChangedResolver::new(repository.clone())
        .resolve(&link)
        .await
        .expect("resolving the last change")
}

/// A merge takes a modification and an addition from the merged branch. Both report the merge,
/// in the merge itself and in a later revision that leaves them alone, while files the merge
/// did not touch keep reporting their own change on the first-parent line.
#[tokio::test]
async fn a_merge_reports_itself_for_what_it_changed_and_keeps_the_first_parent_change_otherwise() {
    let (immutable, mutable, execution) = test_store_create().await.expect("creating the stores");
    lore_spawn!(LORE_CONTEXT.scope(execution, async move {
        let repository = in_memory::repository(immutable, mutable);
        let main = branch_id();
        let feature = branch_id();

        let state = State::new();
        for name in ["a.txt", "keep.txt", "shared.txt"] {
            add(&state, repository.clone(), ROOT_NODE, file(name), name).await;
        }
        let base = commit(&repository, &state, Hash::default(), main).await;

        let state = revision_state(&repository, base).await;
        modify(&state, &repository, "shared.txt", NodeFlags::StagedModify).await;
        add(
            &state,
            repository.clone(),
            ROOT_NODE,
            file("new.txt"),
            "new.txt",
        )
        .await;
        let feature_tip = commit(&repository, &state, base, feature).await;

        let state = revision_state(&repository, base).await;
        modify(&state, &repository, "keep.txt", NodeFlags::StagedModify).await;
        let main_tip = commit(&repository, &state, base, main).await;

        let state = revision_state(&repository, main_tip).await;
        modify(
            &state,
            &repository,
            "shared.txt",
            NodeFlags::StagedModify | NodeFlags::StagedMerge,
        )
        .await;
        add_staged(
            &state,
            repository.clone(),
            ROOT_NODE,
            file("new.txt"),
            "new.txt",
            NodeFlags::StagedAdd | NodeFlags::StagedMerge,
        )
        .await;
        let merge = commit_merge(
            &repository,
            &state,
            main_tip,
            feature_tip,
            BasedOn::FirstParent,
            main,
        )
        .await;

        let state = revision_state(&repository, merge).await;
        modify(&state, &repository, "a.txt", NodeFlags::StagedModify).await;
        let after = commit(&repository, &state, merge, branch_id()).await;

        for (revision, path, expected) in [
            (merge, "shared.txt", merge),
            (merge, "new.txt", merge),
            (merge, "keep.txt", main_tip),
            (merge, "a.txt", base),
            (after, "shared.txt", merge),
            (after, "new.txt", merge),
            (after, "keep.txt", main_tip),
            (after, "a.txt", after),
        ] {
            assert_eq!(
                last_changed(&repository, revision, path).await,
                expected,
                "{path} as of {revision}"
            );
        }
    }))
    .await
    .expect("Task failed");
}

/// A merge of divergent history builds its tree from the second parent and takes the first
/// parent's changes. A file only the second parent added is in no first-parent revision's delta,
/// so it records no first-parent change and reports the second parent's, in the merge and in a
/// later revision that leaves it alone.
#[tokio::test]
async fn a_file_with_no_first_parent_history_reports_its_second_parent_change() {
    let (immutable, mutable, execution) = test_store_create().await.expect("creating the stores");
    lore_spawn!(LORE_CONTEXT.scope(execution, async move {
        let repository = in_memory::repository(immutable, mutable);
        let main = branch_id();

        let state = State::new();
        add(
            &state,
            repository.clone(),
            ROOT_NODE,
            file("base.txt"),
            "base.txt",
        )
        .await;
        let base = commit(&repository, &state, Hash::default(), main).await;

        let state = revision_state(&repository, base).await;
        add(
            &state,
            repository.clone(),
            ROOT_NODE,
            file("local.txt"),
            "local.txt",
        )
        .await;
        let local_latest = commit(&repository, &state, base, branch_id()).await;

        let state = revision_state(&repository, base).await;
        add(
            &state,
            repository.clone(),
            ROOT_NODE,
            file("remote.txt"),
            "remote.txt",
        )
        .await;
        let remote_latest = commit(&repository, &state, base, branch_id()).await;

        let state = revision_state(&repository, local_latest).await;
        add_staged(
            &state,
            repository.clone(),
            ROOT_NODE,
            file("remote.txt"),
            "remote.txt",
            NodeFlags::StagedAdd | NodeFlags::StagedMerge,
        )
        .await;
        let merge = commit_merge(
            &repository,
            &state,
            remote_latest,
            local_latest,
            BasedOn::Other,
            main,
        )
        .await;

        let state = revision_state(&repository, merge).await;
        modify(&state, &repository, "base.txt", NodeFlags::StagedModify).await;
        let after = commit(&repository, &state, merge, branch_id()).await;

        let after_state = revision_state(&repository, after).await;
        let local_node = node_id_at(&after_state, &repository, "local.txt").await;
        let metadata_node = node_to_file_metadata(local_node);
        let history = *after_state
            .block_file_metadata(
                repository.clone(),
                NodeFileMetadataBlock::index(metadata_node),
            )
            .await
            .expect("reading the file metadata block")
            .read()
            .node(NodeFileMetadata::index(metadata_node));
        assert!(
            history.revision[0].is_zero(),
            "local.txt records no first-parent change"
        );

        for (revision, path, expected) in [
            (merge, "local.txt", local_latest),
            (merge, "remote.txt", merge),
            (after, "local.txt", local_latest),
            (after, "remote.txt", merge),
            (after, "base.txt", after),
        ] {
            assert_eq!(
                last_changed(&repository, revision, path).await,
                expected,
                "{path} as of {revision}"
            );
        }
    }))
    .await
    .expect("Task failed");
}
