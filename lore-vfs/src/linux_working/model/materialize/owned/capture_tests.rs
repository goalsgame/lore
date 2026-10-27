// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

use super::*;
use crate::capture_memory::CaptureLimits;

fn capture_limits() -> CaptureLimits {
    CaptureLimits {
        max_buffer_bytes: 8 * 1024 * 1024,
        max_metadata_nodes: 100_000,
        max_name_bytes: 1024 * 1024,
    }
}

#[tokio::test]
async fn captured_root_pairs_exact_provenance_dirty_images_and_reissued_receipts() {
    LORE_CONTEXT
        .scope(execution(), async {
            let f = Fixture::new(Default::default(), 4096, b"DIRTY123", true).await;
            let plan = f.plan().await;
            let resources = f
                .model
                .prepare_private_resources(f.layout(&plan).await)
                .await
                .unwrap();
            let new_inode = *resources.files.keys().next().unwrap();
            let candidate = resources.into_owned_candidate().await.unwrap();
            drop(plan);
            let captured = candidate
                .capture_checkpoint(None, capture_limits())
                .await
                .unwrap();
            let image = captured.test_snapshot();
            assert_eq!(image.original_root.key, f.scope);
            assert_eq!(image.active_underlays[&f.scope].revision, f.target);
            assert_ne!(
                image.backend_state,
                f.model
                    .namespace
                    .backend()
                    .unwrap()
                    .checkpoint_state()
                    .await
                    .unwrap()
            );
            assert_eq!(image.files[&f.entry.inode].extents[0].data, b"DIRTY123");
            assert_eq!(image.files[&new_inode].extents[0].data, b"DIRTY123");
            assert_eq!(
                image.files[&f.entry.inode].receipt.as_ref().unwrap().inode,
                f.entry.inode
            );
            assert_eq!(
                image.files[&new_inode].receipt.as_ref().unwrap().inode,
                new_inode
            );
            assert!(
                !image
                    .nodes
                    .iter()
                    .find(|record| record.entry.inode == f.entry.inode)
                    .unwrap()
                    .attached
            );
            assert!(captured.test_encoding().starts_with(b"LOREOV05"));
            assert!(captured.test_memory() >= captured.test_encoding().capacity());
            assert!(f.model.checkpoint.test_snapshot().unwrap().is_none());
            assert_eq!(
                f.model.namespace.lookup(1, b"file").await.unwrap().inode,
                f.entry.inode
            );
            drop(captured);
            assert_eq!(
                f.model.requests.available_permits(),
                f.model.limits.max_requests
            );
            drop(f.model.namespace.freeze_operation().await.unwrap());
        })
        .await;
}

#[tokio::test]
async fn captured_sparse_copy_keeps_original_base_end_and_zero_holes() {
    LORE_CONTEXT
        .scope(execution(), async {
            let f = Fixture::new(Default::default(), 4096, b"Z", false).await;
            let writer = f.model.open(f.entry.inode, WRITE).await.unwrap();
            f.model
                .setattr(f.entry.inode, Some(writer), Some(3), None)
                .await
                .unwrap();
            f.model.write(writer, 6, b"XY").await.unwrap();
            f.model.release(writer).await.unwrap();
            let plan = f.plan().await;
            let resources = f
                .model
                .prepare_private_resources(f.layout(&plan).await)
                .await
                .unwrap();
            let inode = *resources.files.keys().next().unwrap();
            let candidate = resources.into_owned_candidate().await.unwrap();
            drop(plan);
            let captured = candidate
                .capture_checkpoint(None, capture_limits())
                .await
                .unwrap();
            let snapshot = captured.test_snapshot();
            assert_eq!(snapshot.files[&inode].base_end, 3);
            assert_eq!(snapshot.files[&inode].node.key, f.entry.authority);
            assert_ne!(snapshot.files[&inode].node.key.revision, f.target);
            assert_eq!(snapshot.files[&inode].extents.len(), 2);
            let source_backend = f.model.namespace.backend().unwrap();
            let stores = snapshot
                .files
                .keys()
                .map(|id| {
                    (
                        *id,
                        Arc::new(Store {
                            data: AsyncMutex::new(Vec::new()),
                            control: Arc::new(Control::default()),
                        }) as Arc<dyn SparseStore>,
                    )
                })
                .collect();
            let restored = Namespace::restore(
                source_backend,
                snapshot.original_root.clone(),
                f.gate.clone(),
                captured.into_test_snapshot(),
                stores,
                Default::default(),
                Default::default(),
            )
            .await
            .unwrap();
            let file = restored.materialized_file(inode).await.unwrap().unwrap();
            let reader = file.open(Access::Read).await.unwrap();
            assert_eq!(file.read(reader, 0, 8).await.unwrap(), b"Zbc\0\0\0XY");
            file.release(reader).await.unwrap();
        })
        .await;
}

fn source_evidence(bytes: &[u8]) -> crate::recovery::LoadedCheckpoint {
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let directory = Directory::new(&format!(
        "capture-evidence-{}",
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    let recovery = Recovery::open(directory.0.join("evidence"), Default::default()).unwrap();
    let mut file = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .open(directory.0.join("evidence/checkpoint"))
        .unwrap();
    file.write_all(bytes).unwrap();
    drop(file);
    // An isolated checked fixture is accounting evidence, not the Model's source
    // owner/currentness proof. Its identity must never become an intent permit.
    recovery.load_exact().unwrap().unwrap()
}

#[tokio::test]
async fn aggregate_capacity_boundary_counts_plan_source_decoding_scratch_and_encoding() {
    LORE_CONTEXT
        .scope(execution(), async {
            let f = Fixture::new(Default::default(), 4096, b"DIRTY123", false).await;
            let plan = f.plan().await;
            let candidate = f
                .model
                .prepare_private_resources(f.layout(&plan).await)
                .await
                .unwrap()
                .into_owned_candidate()
                .await
                .unwrap();
            let captured = candidate
                .capture_checkpoint(None, capture_limits())
                .await
                .unwrap();
            let required = captured.test_memory();
            let required_nodes = captured.test_nodes();
            assert!(required > captured.test_encoding().capacity());
            let source = source_evidence(captured.test_encoding());
            let source_identity = source.identity().sha256;
            let mut source_memory =
                crate::capture_memory::CaptureMemory::new(capture_limits()).unwrap();
            source_memory.buffer(source.encoded_capacity()).unwrap();
            source_memory
                .retain(crate::capture_memory::Footprint::snapshot(source.snapshot()).unwrap())
                .unwrap();
            let source_capacity = source_memory.reserved_buffers();
            drop(captured);
            drop(plan);
            let plan = f.plan().await;
            let candidate = f
                .model
                .prepare_private_resources(f.layout(&plan).await)
                .await
                .unwrap()
                .into_owned_candidate()
                .await
                .unwrap();
            let limits = CaptureLimits {
                max_buffer_bytes: required + source_capacity,
                ..capture_limits()
            };
            let captured = candidate
                .capture_checkpoint(Some(source), limits)
                .await
                .unwrap();
            assert_eq!(captured.test_memory(), required + source_capacity);
            assert_eq!(captured.test_source_hash(), Some(source_identity));
            assert!(f.model.checkpoint.test_snapshot().unwrap().is_none());
            drop(captured);
            drop(plan);
            let plan = f.plan().await;
            let candidate = f
                .model
                .prepare_private_resources(f.layout(&plan).await)
                .await
                .unwrap()
                .into_owned_candidate()
                .await
                .unwrap();
            let before = f.stores.control.reads.load(Ordering::Acquire);
            assert!(
                candidate
                    .capture_checkpoint(
                        None,
                        CaptureLimits {
                            max_buffer_bytes: 1,
                            ..capture_limits()
                        }
                    )
                    .await
                    .is_err()
            );
            assert_eq!(f.stores.control.reads.load(Ordering::Acquire), before);
            drop(plan);
            let plan = f.plan().await;
            let candidate = f
                .model
                .prepare_private_resources(f.layout(&plan).await)
                .await
                .unwrap()
                .into_owned_candidate()
                .await
                .unwrap();
            assert!(
                candidate
                    .capture_checkpoint(
                        None,
                        CaptureLimits {
                            max_buffer_bytes: required - 1,
                            ..capture_limits()
                        }
                    )
                    .await
                    .is_err()
            );
            assert_eq!(
                f.model.requests.available_permits(),
                f.model.limits.max_requests
            );
            drop(plan);
            let plan = f.plan().await;
            let candidate = f
                .model
                .prepare_private_resources(f.layout(&plan).await)
                .await
                .unwrap()
                .into_owned_candidate()
                .await
                .unwrap();
            let before = f.stores.control.reads.load(Ordering::Acquire);
            assert!(
                candidate
                    .capture_checkpoint(
                        None,
                        CaptureLimits {
                            max_metadata_nodes: required_nodes - 1,
                            ..capture_limits()
                        }
                    )
                    .await
                    .is_err()
            );
            assert_eq!(f.stores.control.reads.load(Ordering::Acquire), before);
        })
        .await;
}

#[tokio::test]
async fn capture_cancellation_close_and_midread_revocation_release_resources_without_writes() {
    LORE_CONTEXT.scope(execution(), async {
        for outcome in 0..4 {
            let f = Fixture::new(Default::default(), 4096, b"DIRTY123", false).await;
            let plan = f.plan().await;
            let candidate = f.model.prepare_private_resources(f.layout(&plan).await).await.unwrap().into_owned_candidate().await.unwrap();
            drop(plan);
            let writes = f.stores.control.writes.load(Ordering::Acquire);
            f.stores.control.read_block.store(true, Ordering::Release);
            let mut capture = Box::pin(candidate.capture_checkpoint(None, capture_limits()));
            tokio::select! { _=f.stores.control.read_entered.notified()=>{}, result=&mut capture=>panic!("capture finished before read pause: {}",result.is_ok()) }
            match outcome {
                0 => {},
                1 => { f.model.close_requests(); assert!(tokio::time::timeout(Duration::from_secs(2), &mut capture).await.unwrap().is_err()); },
                _ => {
                    if outcome == 2 { f.gate.source_read.store(true, Ordering::Release); } else { f.gate.target_read.store(true, Ordering::Release); }
                    f.stores.control.read_block.store(false, Ordering::Release);
                    f.stores.control.read_resume.notify_one();
                    assert!(tokio::time::timeout(Duration::from_secs(2), &mut capture).await.unwrap().is_err());
                }
            }
            drop(capture);
            assert_eq!(f.stores.control.writes.load(Ordering::Acquire), writes);
            assert_eq!(f.model.requests.available_permits(), f.model.limits.max_requests);
            assert!(f.model.checkpoint.test_snapshot().unwrap().is_none());
            if outcome != 1 {
                f.gate.source_read.store(false, Ordering::Release);
                f.gate.target_read.store(false, Ordering::Release);
                drop(f.model.namespace.freeze_operation().await.unwrap());
            }
        }
    }).await;
}

async fn commit_state(
    repository: &Arc<RepositoryContext>,
    state: Arc<RevisionState>,
    seed: u8,
) -> Hash {
    let branch = [seed; 16].into();
    let mut metadata = lore_revision::metadata::Metadata::new();
    metadata.set_branch(branch).unwrap();
    lore_revision::commit::commit_in_memory_revision(
        repository.clone(),
        repository.try_write_token().unwrap(),
        state,
        metadata,
        Hash::default(),
        branch,
    )
    .await
    .unwrap()
}
async fn parent_links(
    parent: &Arc<RepositoryContext>,
    child: &Arc<RepositoryContext>,
    pin: Hash,
    extra: bool,
) -> Arc<RevisionState> {
    use lore_revision::node::NodeFlags;
    let state = RevisionState::new();
    for name in ["left", "right"] {
        let id = state
            .node_add(
                parent.clone(),
                0,
                lore_revision::node::Node {
                    flags: NodeFlags::Link.bits(),
                    child: 0,
                    address: Address {
                        context: child.id.into(),
                        hash: pin,
                    },
                    name_hash: lore_storage::hash::hash_string(name),
                    ..Default::default()
                },
                name,
            )
            .await
            .unwrap();
        state
            .node_mark_staged(
                parent.clone(),
                id,
                NodeFlags::StagedAdd,
                NodeFlags::DirtyAdd,
            )
            .await
            .unwrap();
    }
    if extra {
        let id = state
            .node_add(
                parent.clone(),
                0,
                lore_revision::node::Node {
                    name_hash: lore_storage::hash::hash_string("extra"),
                    ..Default::default()
                },
                "extra",
            )
            .await
            .unwrap();
        state
            .node_mark_staged(
                parent.clone(),
                id,
                NodeFlags::StagedAdd,
                NodeFlags::DirtyAdd,
            )
            .await
            .unwrap();
    }
    state
}
pub(super) async fn linked_fixture() -> (
    Fixture,
    Entry,
    Entry,
    core::HeldBaseline,
    Arc<RepositoryContext>,
) {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let directory = Directory::new(&format!(
        "capture-links-{}",
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    let parent = repository().await;
    let child = parent.to_link_context(RepositoryId::from([239; 16])).await;
    let (pin, _) = commit_file(&child, b"linked-original", 31).await;
    let source = commit_state(&parent, parent_links(&parent, &child, pin, false).await, 33).await;
    let target = Address::zero_context_hash(
        commit_state(&parent, parent_links(&parent, &child, pin, true).await, 34).await,
    );
    let backend = Arc::new(RevisionBackend::new(parent, source).await.unwrap());
    let scope = backend.root().key;
    let gate = Arc::new(Gate {
        target,
        ..Default::default()
    });
    let namespace = Arc::new(
        Namespace::new(
            backend.clone(),
            backend.root(),
            gate.clone(),
            Default::default(),
        )
        .unwrap(),
    );
    let stores = Arc::new(Stores::default());
    let model = Model::new(
        namespace,
        stores.clone(),
        Arc::new(StorageBudget::new(4096)),
        Recovery::open(directory.0.join("recovery"), Default::default()).unwrap(),
        Default::default(),
    )
    .await
    .unwrap();
    let left = model.lookup(1, b"left").await.unwrap().entry;
    let right = model.lookup(1, b"right").await.unwrap().entry;
    let file = model.lookup(left.inode, b"file").await.unwrap().entry;
    let writer = model.open(file.inode, WRITE).await.unwrap();
    model.write(writer, 0, b"EDIT").await.unwrap();
    model.release(writer).await.unwrap();
    let baseline = core::HeldBaseline {
        scope: left.authority,
        head: core::ScopeHead {
            revision: Address::zero_context_hash(pin),
            root: Some(0),
        },
        sequence: 1,
        parent_projection: Some(core::ScopeProjection {
            parent: scope,
            head: scope.revision,
            target: Some(core::ScopePin {
                repository: child.id,
                revision: Address::zero_context_hash(pin),
                node: 0,
            }),
        }),
    };
    let freeze = model.namespace.freeze_operation().await.unwrap();
    model
        .namespace
        .adopt_committed(
            freeze.token(),
            Vec::new(),
            vec![crate::namespace::BaselineUpdate {
                expected: None,
                next: baseline.clone(),
            }],
            || Ok(()),
        )
        .await
        .unwrap();
    drop(freeze);
    let f = Fixture {
        model,
        stores,
        gate,
        scope,
        target,
        entry: file.clone(),
        _directory: directory,
    };
    (f, left, right, baseline, child)
}
#[tokio::test]
async fn capture_reuses_linked_dirty_files_with_selected_route_and_distinct_exposures() {
    LORE_CONTEXT
        .scope(execution(), async {
            let (f, left, right, baseline, _) = linked_fixture().await;
            let file = f.entry.clone();
            let target = f.target;
            let reader = f.model.open(file.inode, READ).await.unwrap();
            let old = f.model.file_handle(reader).unwrap();
            let plan = f.plan().await;
            let resources = f
                .model
                .prepare_private_resources(f.layout(&plan).await)
                .await
                .unwrap();
            assert!(
                resources.files.is_empty(),
                "unchanged child must not be privately recopied"
            );
            let candidate = resources.into_owned_candidate().await.unwrap();
            drop(plan);
            let captured = candidate
                .capture_checkpoint(None, capture_limits())
                .await
                .unwrap();
            let image = captured.test_snapshot();
            assert_ne!(left.authority.exposure, right.authority.exposure);
            for boundary in [left, right] {
                assert!(
                    image
                        .nodes
                        .iter()
                        .any(|node| node.entry == boundary && node.attached)
                );
            }
            assert!(
                image
                    .nodes
                    .iter()
                    .any(|node| node.entry == file && node.attached)
            );
            assert_eq!(image.files[&file.inode].extents[0].data, b"EDIT");
            assert_eq!(image.baselines[&baseline.scope].head, baseline.head);
            assert_eq!(
                image.baselines[&baseline.scope]
                    .parent_projection
                    .as_ref()
                    .unwrap()
                    .head,
                target
            );
            assert!(!Arc::ptr_eq(
                &captured.test_read_gate(file.inode),
                old.file.overlay.test_effective_gate()
            ));
            assert!(f.model.checkpoint.test_snapshot().unwrap().is_none());
            drop(captured);
            f.model.release(reader).await.unwrap();
        })
        .await;
}
