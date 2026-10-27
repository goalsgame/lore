// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

use lore_revision::fs::filesystem_provider::FilesystemProvider;
use lore_revision::interface::LoreArray;
use lore_revision::interface::LoreString;
use lore_revision::repository::RepositoryContextCreationArgs;
use lore_revision::repository::RepositoryPaths;
use lore_revision::repository::RepositoryWriteToken;

use super::*;
use crate::provider::WorkingProvider;

async fn file_state(
    repository: &Arc<RepositoryContext>,
    name: &str,
    bytes: &[u8],
) -> (Arc<State>, Address) {
    let address = lore_revision::immutable::write_borrowed(
        repository.clone(),
        [231; 16].into(),
        bytes,
        lore_storage::options::WriteOptions::default().with_fixed_size_chunk(3),
    )
    .await
    .unwrap();
    let state = State::new();
    let node = state
        .node_add(
            repository.clone(),
            0,
            RevisionNode {
                flags: NodeFlags::File.bits(),
                name_hash: lore_storage::hash::hash_string(name),
                address,
                size: bytes.len() as u64,
                ..Default::default()
            },
            name,
        )
        .await
        .unwrap();
    state
        .node_mark_staged(
            repository.clone(),
            node,
            NodeFlags::StagedAdd,
            NodeFlags::DirtyAdd,
        )
        .await
        .unwrap();
    (state, address)
}
async fn commit_state(
    repository: &Arc<RepositoryContext>,
    token: &RepositoryWriteToken,
    state: Arc<State>,
    parent: Hash,
    branch: lore_base::types::Context,
) -> Hash {
    let mut metadata = lore_revision::metadata::Metadata::new();
    metadata.set_branch(branch).unwrap();
    lore_revision::commit::commit_in_memory_revision(
        repository.clone(),
        token,
        state,
        metadata,
        parent,
        branch,
    )
    .await
    .unwrap()
}

struct Fixture {
    _directory: crate::digest_index::tests::Directory,
    root: std::path::PathBuf,
    token: RepositoryWriteToken,
    original: Arc<RepositoryContext>,
    repository: Arc<RepositoryContext>,
    namespace: Arc<Namespace>,
    provider: Arc<WorkingProvider>,
    scope: NodeKey,
    gate: Arc<Gate>,
    logical: Hash,
    inode: u64,
    local_content: Address,
}
async fn fixture(directory_entry: bool, replacement: bool) -> Fixture {
    let directory = crate::digest_index::tests::Directory::new();
    std::fs::create_dir(&directory.0).unwrap();
    let root = directory.0.join("repo");
    std::fs::create_dir(&root).unwrap();
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
    let id = RepositoryId::from([229; 16]);
    let branch = [230; 16].into();
    let created = lore_revision::repository::create_local(
        &root,
        &token,
        id,
        branch,
        lore_revision::branch::DEFAULT_DEFAULT_NAME.to_string(),
        Default::default(),
        false,
    )
    .await
    .unwrap();
    let make = |provider: Option<Arc<dyn FilesystemProvider>>| {
        Arc::new(
            RepositoryContext::new(RepositoryContextCreationArgs {
                paths: Some(RepositoryPaths::new(root.clone(), root.join(".lore"))),
                immutable_store: immutable.clone(),
                mutable_store: mutable.clone(),
                id,
                instance_id: created.instance_id,
                remote: Err(lore_base::error::NoRemote.into()),
                filter: Arc::default(),
                filesystem_provider: provider,
            })
            .with_write_token(token.share()),
        )
    };
    let original = make(None);
    lore_revision::instance::store_current_anchor_branch(&original, branch)
        .await
        .unwrap();
    let state = if directory_entry {
        let state = State::new();
        add(&state, &original, 0, "old", true, 0).await;
        state
    } else {
        file_state(&original, "old", b"original local payload")
            .await
            .0
    };
    let pin = commit_state(&original, &token, state, Hash::default(), branch).await;
    lore_revision::instance::store_current_anchor(&original, pin)
        .await
        .unwrap();
    let backend = Arc::new(RevisionBackend::new(original.clone(), pin).await.unwrap());
    let scope = backend.root().key;
    let gate = Arc::new(Gate::default());
    let namespace = Arc::new(
        Namespace::new(
            backend.clone(),
            backend.root(),
            gate.clone(),
            Default::default(),
        )
        .unwrap(),
    );
    let provider = Arc::new(
        WorkingProvider::new(
            namespace.clone(),
            backend,
            directory.0.join("exports"),
            Default::default(),
        )
        .unwrap(),
    );
    let repository = make(Some(provider.clone()));
    let old = namespace.lookup(1, b"old").await.unwrap();
    namespace.rename(1, b"old", 1, b"middle").await.unwrap();
    if replacement {
        write_local(&namespace, "new", b"replaced destination payload").await;
    }
    lore_revision::file::stage::stage(
        repository.clone(),
        &token,
        LoreArray::from_vec(vec![LoreString::from(&root)]),
        lore_revision::stage::StageOptions {
            scan: true,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let logical = lore_revision::commit::commit_boxed(
        repository.clone(),
        &token,
        lore_revision::commit::CommitOptions::new("renamed through virtual provider".into()),
    )
    .await
    .unwrap();
    assert_ne!(pin, logical);
    let baselines = namespace.held_baselines().await.unwrap();
    assert_eq!(baselines.len(), 1);
    assert_eq!(baselines[0].head.revision.hash, logical);
    let middle = namespace.lookup(1, b"middle").await.unwrap();
    assert_eq!(middle.inode, old.inode);
    assert_eq!(middle.base.as_ref().unwrap().key.revision.hash, pin);
    let logical_state = State::deserialize(repository.clone(), logical)
        .await
        .unwrap();
    let logical_middle = logical_state
        .find_relative_node_link(repository.clone(), 0, "middle")
        .await
        .unwrap();
    let local_content = logical_state
        .node(repository.clone(), logical_middle.node)
        .await
        .unwrap()
        .address;
    if !directory_entry {
        let committed_backend = RevisionBackend::new(original.clone(), logical)
            .await
            .unwrap();
        let committed_middle = committed_backend
            .lookup(&committed_backend.root().key, b"middle")
            .await
            .unwrap();
        assert_eq!(
            committed_backend
                .read(&committed_middle.key, 0, b"original local payload".len())
                .await
                .unwrap(),
            b"original local payload"
        );
        let receipts = provider.receipts().await.unwrap();
        assert!(receipts.iter().any(|receipt| receipt.inode == old.inode
            && receipt.authority == old.authority
            && receipt.committed_revision == logical
            && receipt.address == local_content));
    } else {
        assert!(provider.receipts().await.unwrap().is_empty());
    }
    Fixture {
        _directory: directory,
        root,
        token,
        original,
        repository,
        namespace,
        provider,
        scope,
        gate,
        logical,
        inode: old.inode,
        local_content,
    }
}

async fn write_local(namespace: &Arc<Namespace>, name: &str, bytes: &[u8]) {
    let entry = namespace
        .create_file(
            1,
            name.as_bytes(),
            0o644,
            Arc::new(Store::default()),
            Default::default(),
        )
        .await
        .unwrap();
    rewrite(namespace, entry.inode, bytes).await;
}
async fn rewrite(namespace: &Arc<Namespace>, inode: u64, bytes: &[u8]) {
    let file = namespace
        .file(inode, Arc::new(Store::default()), Default::default())
        .await
        .unwrap();
    let writer = file.open(Access::Write).await.unwrap();
    file.truncate(writer, 0).await.unwrap();
    file.write(writer, 0, bytes).await.unwrap();
    file.release(writer).await.unwrap();
}
async fn normal_commit(f: &Fixture) -> Hash {
    lore_revision::file::stage::stage(
        f.repository.clone(),
        &f.token,
        LoreArray::from_vec(vec![LoreString::from(&f.root)]),
        lore_revision::stage::StageOptions {
            scan: true,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    lore_revision::commit::commit_boxed(
        f.repository.clone(),
        &f.token,
        lore_revision::commit::CommitOptions::new("unrelated local addition".into()),
    )
    .await
    .unwrap()
}
async fn incoming(f: &Fixture, name: &str, parent: Hash) -> Address {
    let (state, _) = file_state(&f.original, name, b"incoming changed payload").await;
    let target = Address::zero_context_hash(
        commit_state(&f.original, &f.token, state, parent, [232; 16].into()).await,
    );
    *f.gate.target.lock().unwrap() = Some(target);
    target
}
async fn conflicting_prepare(
    f: &Fixture,
    target: Address,
    path: &str,
    expected: Option<Address>,
    require_rename: bool,
) {
    let before = f.namespace.snapshot(1 << 20).await.unwrap();
    let anchors = lore_revision::instance::strict_anchors::read(&f.repository)
        .await
        .unwrap();
    let rejected = prepare(
        f.namespace.clone(),
        f.scope,
        target,
        Options::default(),
        Limits::default(),
    )
    .await;
    let conflicts = match rejected {
        Err(Error::Conflicts(conflicts)) => conflicts,
        _ => panic!("default preparation must reject overlapping local changes"),
    };
    if require_rename {
        assert!(
            conflicts
                .iter()
                .any(|conflict| conflict.kind == ConflictKind::Rename)
        );
    }
    let kept = prepare(
        f.namespace.clone(),
        f.scope,
        target,
        Options {
            local: LocalPolicy::KeepLocal,
            ..Default::default()
        },
        Limits::default(),
    )
    .await
    .unwrap();
    assert_eq!(kept.candidate_node(path).unwrap().immutable, expected);
    drop(kept);
    assert!(matches!(
        prepare(
            f.namespace.clone(),
            f.scope,
            target,
            Options {
                local: LocalPolicy::OverwriteLocal,
                ..Default::default()
            },
            Limits::default()
        )
        .await,
        Err(Error::Unsupported("overwriting conflicted renames"))
    ));
    unchanged_metadata(&before, &f.namespace.snapshot(1 << 20).await.unwrap());
    assert_eq!(
        lore_revision::instance::strict_anchors::read(&f.repository)
            .await
            .unwrap(),
        anchors
    );
}
async fn unconflicted_overwrite(f: &Fixture, head: Hash, path: &str) {
    let target = Address::zero_context_hash(head);
    *f.gate.target.lock().unwrap() = Some(target);
    let plan = prepare(
        f.namespace.clone(),
        f.scope,
        target,
        Options {
            local: LocalPolicy::OverwriteLocal,
            ..Default::default()
        },
        Limits::default(),
    )
    .await
    .unwrap();
    assert!(plan.conflicts().is_empty());
    assert!(plan.candidate_node(path).is_some());
}
async fn close(f: &Fixture) {
    f.provider.close_admission();
    f.provider.drain_exports().await.unwrap();
}

#[tokio::test]
async fn rename_after_normal_commit_retains_rename_conflicts_and_refuses_forced_overwrite() {
    LORE_CONTEXT
        .scope(execution(), async {
            let f = fixture(false, false).await;
            f.namespace.rename(1, b"middle", 1, b"new").await.unwrap();
            let target = incoming(&f, "middle", f.logical).await;
            conflicting_prepare(&f, target, "new", Some(f.local_content), true).await;
            assert_eq!(f.namespace.lookup(1, b"new").await.unwrap().inode, f.inode);
            close(&f).await;
        })
        .await;
}
#[tokio::test]
async fn rename_replacing_existing_destination_uses_exact_current_receipt() {
    LORE_CONTEXT
        .scope(execution(), async {
            let f = fixture(false, true).await;
            assert_ne!(f.namespace.lookup(1, b"new").await.unwrap().inode, f.inode);
            f.namespace.rename(1, b"middle", 1, b"new").await.unwrap();
            let target = incoming(&f, "middle", f.logical).await;
            conflicting_prepare(&f, target, "new", Some(f.local_content), true).await;
            close(&f).await;
        })
        .await;
}
#[tokio::test]
async fn older_receipt_requires_conservative_conflicted_overwrite_refusal() {
    LORE_CONTEXT
        .scope(execution(), async {
            let f = fixture(false, false).await;
            write_local(&f.namespace, "unrelated", b"unrelated addition").await;
            let next = normal_commit(&f).await;
            assert_ne!(next, f.logical);
            assert!(
                f.provider
                    .receipts()
                    .await
                    .unwrap()
                    .iter()
                    .any(|receipt| receipt.inode == f.inode
                        && receipt.committed_revision == f.logical)
            );
            f.namespace.rename(1, b"middle", 1, b"new").await.unwrap();
            let target = incoming(&f, "middle", next).await;
            conflicting_prepare(&f, target, "new", Some(f.local_content), false).await;
            unconflicted_overwrite(&f, next, "new").await;
            close(&f).await;
        })
        .await;
}
#[tokio::test]
async fn revoked_receipt_requires_conservative_conflicted_overwrite_refusal() {
    LORE_CONTEXT
        .scope(execution(), async {
            let f = fixture(false, false).await;
            rewrite(&f.namespace, f.inode, b"private modified payload").await;
            assert!(
                f.provider
                    .receipts()
                    .await
                    .unwrap()
                    .iter()
                    .all(|receipt| receipt.inode != f.inode)
            );
            f.namespace.rename(1, b"middle", 1, b"new").await.unwrap();
            let target = incoming(&f, "middle", f.logical).await;
            conflicting_prepare(&f, target, "new", None, false).await;
            unconflicted_overwrite(&f, f.logical, "new").await;
            close(&f).await;
        })
        .await;
}
#[tokio::test]
async fn directory_without_receipt_requires_conservative_conflicted_overwrite_refusal() {
    LORE_CONTEXT
        .scope(execution(), async {
            let f = fixture(true, false).await;
            f.namespace.rename(1, b"middle", 1, b"new").await.unwrap();
            let target = incoming(&f, "middle", f.logical).await;
            conflicting_prepare(&f, target, "new", None, false).await;
            unconflicted_overwrite(&f, f.logical, "new").await;
            close(&f).await;
        })
        .await;
}
#[tokio::test]
async fn local_only_addition_requires_conservative_conflicted_overwrite_refusal() {
    LORE_CONTEXT
        .scope(execution(), async {
            let f = fixture(false, false).await;
            write_local(&f.namespace, "fresh", b"uncommitted local payload").await;
            let target = incoming(&f, "fresh", f.logical).await;
            conflicting_prepare(&f, target, "fresh", None, false).await;
            unconflicted_overwrite(&f, f.logical, "fresh").await;
            close(&f).await;
        })
        .await;
}

#[tokio::test]
async fn exact_receipt_lineage_rechecks_current_policy_and_bounds_its_indexes() {
    LORE_CONTEXT
        .scope(execution(), async {
            let f = fixture(false, false).await;
            f.namespace.rename(1, b"middle", 1, b"new").await.unwrap();
            let freeze = f.namespace.freeze_operation().await.unwrap();
            let snapshot = f.namespace.snapshot(1 << 20).await.unwrap();
            let owner = f.namespace.backend().unwrap();
            let backend = owner
                .as_any()
                .unwrap()
                .downcast_ref::<RevisionBackend>()
                .unwrap();
            let policy = f.namespace.transition_policy().unwrap();
            let mut budget = Budget::new(Limits::default()).unwrap();
            let origin = backend
                .transition_view(f.scope, f.scope.revision, policy.clone(), &mut budget)
                .await
                .unwrap();
            let current = backend
                .transition_view(
                    f.scope,
                    Address::zero_context_hash(f.logical),
                    policy.clone(),
                    &mut budget,
                )
                .await
                .unwrap();
            let local = inventory(&snapshot, backend, f.scope, 1, policy.clone(), &mut budget)
                .await
                .unwrap();
            let endpoints =
                rename_endpoints(&current.entries, &local, &origin.entries, &mut budget).unwrap();
            *f.gate.target.lock().unwrap() = Some(current.head.revision);
            f.gate
                .revoke_after_target_node
                .store(true, Ordering::SeqCst);
            let result = rename_origins::resolve(
                &snapshot,
                &local,
                rename_origins::Views {
                    original: &origin.entries,
                    current: &current,
                },
                backend,
                &policy,
                endpoints.clone(),
                &mut budget,
            )
            .await;
            assert!(matches!(result, Err(core::Error::PermissionDenied)));
            assert!(f.gate.deny_target.load(Ordering::SeqCst));
            f.gate.deny_target.store(false, Ordering::SeqCst);
            let mut bounded = Budget::new(Limits {
                max_nodes: 1,
                ..Default::default()
            })
            .unwrap();
            assert!(matches!(
                rename_origins::resolve(
                    &snapshot,
                    &local,
                    rename_origins::Views {
                        original: &origin.entries,
                        current: &current
                    },
                    backend,
                    &policy,
                    endpoints,
                    &mut bounded
                )
                .await,
                Err(core::Error::ResourceLimit)
            ));
            unchanged_metadata(&snapshot, &f.namespace.snapshot(1 << 20).await.unwrap());
            drop(freeze);
            close(&f).await;
        })
        .await;
}
