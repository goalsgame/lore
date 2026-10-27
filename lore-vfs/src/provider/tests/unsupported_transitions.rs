// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use super::*;

async fn metadata_revision(
    repository: &Arc<RepositoryContext>,
    token: &RepositoryWriteToken,
    branch: lore_revision::lore::BranchId,
    message: &str,
    parent: Hash,
) -> Hash {
    let mut metadata = lore_revision::metadata::Metadata::new();
    metadata.set_branch(branch).unwrap();
    metadata.set_string("message", message).unwrap();
    let state = State::new();
    state.set_metadata_hash(metadata.serialize(repository.clone()).await.unwrap());
    state.set_parent_self(parent);
    state.set_revision_number(if parent.is_zero() { 1 } else { 2 });
    state.serialize(repository.clone(), token).await.unwrap()
}

#[tokio::test]
async fn unsupported_replacements_preserve_instance_anchors_carry_and_metadata() {
    LORE_CONTEXT
        .scope(
            Arc::new(ExecutionContext::new_client_with_user_id(
                LoreGlobalArgs {
                    offline: 1,
                    ..Default::default()
                },
                EventDispatcher::no_dispatch(),
                "provider-test".into(),
            )),
            async {
                let directory = crate::digest_index::tests::Directory::new();
                std::fs::create_dir(&directory.0).unwrap();
                let root = directory.0.join("repo");
                std::fs::create_dir(&root).unwrap();
                std::fs::create_dir(root.join(".lore")).unwrap();
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
                let token = RepositoryWriteToken::acquire(&root).await;
                let instance = lore_revision::instance::InstanceId::generate();
                let make = |provider: Option<Arc<dyn FilesystemProvider>>| {
                    Arc::new(
                        RepositoryContext::new(RepositoryContextCreationArgs {
                            paths: Some(RepositoryPaths::new(root.clone(), root.join(".lore"))),
                            immutable_store: immutable.clone(),
                            mutable_store: mutable.clone(),
                            id: [161; 16].into(),
                            instance_id: instance,
                            remote: Err(lore_base::error::NoRemote.into()),
                            filter: Arc::default(),
                            filesystem_provider: provider,
                        })
                        .with_write_token(token.share()),
                    )
                };
                let original = make(None);
                let branch = [162; 16].into();
                let base =
                    metadata_revision(&original, &token, branch, "base", Hash::default()).await;
                let current = metadata_revision(&original, &token, branch, "local", base).await;
                let incoming = metadata_revision(&original, &token, branch, "incoming", base).await;
                assert_ne!(current, incoming);
                lore_revision::instance::store_current_anchor_branch(&original, branch)
                    .await
                    .unwrap();
                lore_revision::instance::store_current_anchor(&original, current)
                    .await
                    .unwrap();
                lore_revision::branch::store_latest(
                    original.clone(),
                    branch,
                    Hash::default(),
                    incoming,
                    lore_revision::branch::BranchLatestStatus::Convergent,
                )
                .await
                .unwrap();
                let backend = Arc::new(RevisionBackend::new(original, current).await.unwrap());
                let namespace = Arc::new(
                    Namespace::new(
                        backend.clone(),
                        backend.root(),
                        Arc::new(Gate::default()),
                        Default::default(),
                    )
                    .unwrap(),
                );
                let provider = Arc::new(
                    WorkingProvider::new(
                        namespace,
                        backend,
                        directory.0.join("exports"),
                        Default::default(),
                    )
                    .unwrap(),
                );
                let repository = make(Some(provider.clone()));
                let before = lore_revision::instance::strict_anchors::read(&repository)
                    .await
                    .unwrap();
                let operation = provider.begin_operation().await.unwrap();
                assert!(
                    operation
                        .hold_state(
                            repository.clone(),
                            State::deserialize(repository.clone(), incoming)
                                .await
                                .unwrap(),
                            Default::default()
                        )
                        .await
                        .is_err()
                );
                operation.abort();
                drop(operation);
                let merged = lore_revision::branch::merge::merge_start(
                    repository.clone(),
                    &token,
                    branch,
                    lore_revision::branch::merge::MergeStartOptions {
                        message: String::new(),
                        link_messages: Default::default(),
                        no_commit: true,
                        scope: lore_revision::branch::merge::MergeScope::MainOnly,
                        inherit_metadata: Default::default(),
                    },
                )
                .await;
                let after = lore_revision::instance::strict_anchors::read(&repository)
                    .await
                    .unwrap();
                let error = merged.unwrap_err().to_string();
                assert!(
                    error.contains("Revision replacement and merge-state"),
                    "{error}"
                );
                assert_eq!(before, after);
                assert!(!provider.supports_revision_replacement());
                refused_commands(repository.clone(), &token, branch, current, incoming).await;
                let root_provider = repository.file_system();
                let linked = repository.to_link_context([163; 16].into()).await;
                let filtered = Arc::new(repository.to_filter_context(Arc::default()));
                for inherited in [linked, filtered] {
                    assert!(Arc::ptr_eq(&root_provider, &inherited.file_system()));
                    assert!(!inherited.file_system().supports_revision_replacement());
                    let before = mutable_snapshot(&inherited).await;
                    let error = lore_revision::revision::sync::sync_boxed(
                        inherited.clone(),
                        &token,
                        Default::default(),
                    )
                    .await
                    .unwrap_err()
                    .to_string();
                    assert!(error.contains("Revision replacement and merge-state"));
                    assert_eq!(before, mutable_snapshot(&inherited).await);
                }
                let detached = Arc::new(repository.without_command_locks());
                assert!(!detached.file_system().supports_revision_replacement());
                let before = mutable_snapshot(&detached).await;
                let error = lore_revision::repository::branch_switch(
                    detached.clone(),
                    &token,
                    "missing".into(),
                    lore_revision::repository::BranchSwitchOptions {
                        bare: true,
                        ..Default::default()
                    },
                )
                .await
                .unwrap_err()
                .to_string();
                assert!(error.contains("Revision replacement and merge-state"));
                assert_eq!(before, mutable_snapshot(&detached).await);

                provider.close_admission();
                provider.drain_exports().await.unwrap();
            },
        )
        .await;
}

type Command<'a> =
    std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send + 'a>>;
fn command<'a, T: Send + 'a, E: std::fmt::Display + Send + 'a>(
    future: impl std::future::Future<Output = Result<T, E>> + Send + 'a,
) -> Command<'a> {
    Box::pin(async move {
        future
            .await
            .map(|_value| ())
            .map_err(|error| error.to_string())
    })
}
pub(super) async fn mutable_snapshot(
    repository: &RepositoryContext,
) -> std::collections::BTreeMap<(u8, Hash), Hash> {
    let mut snapshot = std::collections::BTreeMap::new();
    // Untyped keys are deliberately omitted by the store's list API.
    let mut untyped = [
        lore_revision::instance::ANCHOR_CURRENT,
        lore_revision::instance::ANCHOR_CURRENT_BRANCH,
        lore_revision::instance::ANCHOR_STAGED,
    ]
    .map(|function| {
        lore_revision::instance::anchor_key(repository.salt(), function, repository.instance_id).0
    })
    .to_vec();
    untyped.push(lore_storage::hash::hash_function(
        repository.salt(),
        "merge-dirty-tracking",
    ));
    for key in untyped {
        match repository
            .read_mutable_store()
            .load(repository.id, key, lore_base::types::KeyType::Untyped)
            .await
        {
            Ok(value) => {
                snapshot.insert((0, key), value);
            }
            Err(lore_storage::StoreError::AddressNotFound(_)) => {}
            Err(error) => panic!("reading mutable snapshot: {error}"),
        }
    }
    for kind in 1..=7u8 {
        let mut entries = repository
            .read_mutable_store()
            .list(
                repository.id,
                lore_base::types::KeyType::try_from(kind).unwrap(),
            )
            .await
            .unwrap()
            .channel();
        while let Some((key, value)) = entries.recv().await {
            snapshot.insert((kind, key), value);
        }
    }
    snapshot
}
async fn refused_commands(
    repository: Arc<RepositoryContext>,
    token: &RepositoryWriteToken,
    branch: lore_revision::lore::BranchId,
    current: Hash,
    incoming: Hash,
) {
    use lore_revision::branch::merge;
    use lore_revision::revision::cherry_pick;
    use lore_revision::revision::restore;
    use lore_revision::revision::revert;
    use lore_revision::revision::sync;
    let staged = State::deserialize(repository.clone(), current)
        .await
        .unwrap();
    staged.set_parent_self(current);
    staged.set_parent_other(incoming);
    staged.set_revision_number(0);
    staged.set_merge_conflict();
    let signature = staged.serialize(repository.clone(), token).await.unwrap();
    lore_revision::instance::store_staged_anchor(&repository, signature)
        .await
        .unwrap();
    lore_revision::merge_carry::store(
        repository.clone(),
        current,
        incoming,
        &["untouched".parse().unwrap()],
    )
    .await
    .unwrap();
    let metadata = repository.require_path().unwrap().join(".lore/sentinel");
    std::fs::write(&metadata, b"unchanged working metadata").unwrap();
    let before = mutable_snapshot(&repository).await;
    let diff = || lore_revision::revision::DiffResult {
        base: current,
        source: incoming,
        target: current,
        changes: Vec::new(),
        conflicts: Vec::new(),
    };
    let options = || merge::MergeStartOptions {
        message: String::new(),
        link_messages: Default::default(),
        no_commit: true,
        scope: merge::MergeScope::MainOnly,
        inherit_metadata: Default::default(),
    };
    let inherit = lore_revision::metadata::MetadataInherit::default();
    let checks: Vec<(&str, Command<'_>)> = vec![
        (
            "sync",
            command(sync::sync_boxed(
                repository.clone(),
                token,
                Default::default(),
            )),
        ),
        (
            "sync-view",
            command(sync::sync_boxed(
                repository.clone(),
                token,
                sync::SyncOptions {
                    view: Some(metadata.with_file_name("missing-view")),
                    ..Default::default()
                },
            )),
        ),
        (
            "switch",
            command(lore_revision::repository::branch_switch(
                repository.clone(),
                token,
                "missing".into(),
                Default::default(),
            )),
        ),
        (
            "switch-bare",
            command(lore_revision::repository::branch_switch(
                repository.clone(),
                token,
                "missing".into(),
                lore_revision::repository::BranchSwitchOptions {
                    bare: true,
                    ..Default::default()
                },
            )),
        ),
        (
            "switch-reset",
            command(lore_revision::repository::branch_switch(
                repository.clone(),
                token,
                "missing".into(),
                lore_revision::repository::BranchSwitchOptions {
                    reset: true,
                    ..Default::default()
                },
            )),
        ),
        (
            "restore",
            command(restore::restore_boxed(
                repository.clone(),
                token,
                restore::RestoreOptions { message: None },
            )),
        ),
        (
            "merge-start",
            command(merge::merge_start(
                repository.clone(),
                token,
                branch,
                options(),
            )),
        ),
        (
            "branch-abort",
            command(merge::branch_merge_abort(
                repository.clone(),
                token,
                None,
                false,
            )),
        ),
        (
            "branch-abort-link",
            command(merge::branch_merge_abort(
                repository.clone(),
                token,
                Some("missing".into()),
                false,
            )),
        ),
        (
            "branch-abort-main",
            command(merge::branch_merge_abort(
                repository.clone(),
                token,
                None,
                true,
            )),
        ),
        (
            "merge-abort",
            command(merge::merge_abort(
                repository.clone(),
                merge::MergeType::BranchMerge,
            )),
        ),
        (
            "merge-restart",
            command(merge::merge_restart(
                repository.clone(),
                token,
                Default::default(),
            )),
        ),
        (
            "merge-unresolve",
            command(merge::merge_unresolve(
                repository.clone(),
                token,
                Default::default(),
                merge::MergeType::BranchMerge,
            )),
        ),
        (
            "merge-resolve",
            command(merge::merge_resolve(
                repository.clone(),
                token,
                Default::default(),
                merge::MergeType::BranchMerge,
            )),
        ),
        (
            "merge-mine",
            command(merge::merge_resolve_mine(
                repository.clone(),
                token,
                Default::default(),
            )),
        ),
        (
            "merge-theirs",
            command(merge::merge_resolve_theirs(
                repository.clone(),
                token,
                Default::default(),
            )),
        ),
        (
            "apply-diff",
            command(merge::apply_diff(
                repository.clone(),
                token,
                diff(),
                staged.clone(),
                merge::MergeType::BranchMerge,
                false,
                branch,
                &inherit,
            )),
        ),
        (
            "restart-diff",
            command(merge::apply_restart_diff(
                repository.clone(),
                token,
                Default::default(),
                diff(),
                staged.clone(),
                merge::MergeType::BranchMerge,
            )),
        ),
        (
            "cherry-pick",
            command(cherry_pick::cherry_pick_boxed(
                repository.clone(),
                token,
                incoming,
                cherry_pick::CherryPickOptions {
                    message: String::new(),
                    no_commit: true,
                    inherit_metadata: Default::default(),
                },
            )),
        ),
        (
            "cherry-abort",
            command(cherry_pick::cherry_pick_abort_boxed(repository.clone())),
        ),
        (
            "cherry-restart",
            command(cherry_pick::cherry_pick_restart_boxed(
                repository.clone(),
                token,
                Default::default(),
            )),
        ),
        (
            "cherry-unresolve",
            command(cherry_pick::cherry_pick_unresolve_boxed(
                repository.clone(),
                token,
                Default::default(),
            )),
        ),
        (
            "cherry-resolve",
            command(cherry_pick::cherry_pick_resolve_boxed(
                repository.clone(),
                token,
                Default::default(),
            )),
        ),
        (
            "cherry-mine",
            command(cherry_pick::cherry_pick_resolve_mine_boxed(
                repository.clone(),
                token,
                Default::default(),
            )),
        ),
        (
            "cherry-theirs",
            command(cherry_pick::cherry_pick_resolve_theirs_boxed(
                repository.clone(),
                token,
                Default::default(),
            )),
        ),
        (
            "revert",
            command(revert::revert_boxed(
                repository.clone(),
                token,
                incoming,
                revert::RevertOptions {
                    message: String::new(),
                    no_commit: true,
                },
            )),
        ),
        (
            "revert-abort",
            command(revert::revert_abort_boxed(repository.clone())),
        ),
        (
            "revert-restart",
            command(revert::revert_restart_boxed(
                repository.clone(),
                token,
                Default::default(),
            )),
        ),
        (
            "revert-unresolve",
            command(revert::revert_unresolve_boxed(
                repository.clone(),
                token,
                Default::default(),
            )),
        ),
        (
            "revert-resolve",
            command(revert::revert_resolve_boxed(
                repository.clone(),
                token,
                Default::default(),
            )),
        ),
        (
            "revert-mine",
            command(revert::revert_resolve_mine_boxed(
                repository.clone(),
                token,
                Default::default(),
            )),
        ),
        (
            "revert-theirs",
            command(revert::revert_resolve_theirs_boxed(
                repository.clone(),
                token,
                Default::default(),
            )),
        ),
    ];
    for (name, check) in checks {
        let error = check.await.expect_err(name);
        assert!(
            error.contains("Revision replacement and merge-state"),
            "{name}: {error}"
        );
        assert_eq!(mutable_snapshot(&repository).await, before, "{name}");
        assert_eq!(
            std::fs::read(&metadata).unwrap(),
            b"unchanged working metadata",
            "{name}"
        );
    }
}
