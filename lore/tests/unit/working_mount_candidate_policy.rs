// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
#![cfg(unix)]

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use lore::remote::service_process::ServiceStartGuard;
use lore::service::owner::ServiceOwner;
use lore::working_mount::local_policy::LocalCheckoutFactory;
use lore_base::runtime::LORE_CONTEXT;
use lore_base::types::Address;
use lore_base::types::Hash;
use lore_vfs::core::Backend;
use lore_vfs::core::BackendFuture;
use lore_vfs::core::Error;
use lore_vfs::core::NodeKey;
use lore_vfs::namespace::Namespace;
use lore_vfs::overlay::Access;
use lore_vfs::overlay::AccessGate;
use lore_vfs::overlay::CandidateEdit;
use lore_vfs::overlay::CandidateEditAction;
use lore_vfs::overlay::CandidateLocation;
use lore_vfs::overlay::ProvenTargetMetadata;
use lore_vfs::overlay::SparseStore;
use lore_vfs::overlay::TargetSelection;
use lore_vfs::revision::RevisionBackend;
use lore_vfs::transition_prepare::LocalPolicy;
use lore_vfs::transition_prepare::NodeSummary;
use lore_vfs::transition_prepare::Options;
use lore_vfs::transition_prepare::PreparedTransition;
use lore_vfs::transition_prepare::prepare;

use super::working_mount_local_policy::add_file;
use super::working_mount_local_policy::admitted;
use super::working_mount_local_policy::commit_state;
use super::working_mount_local_policy::repository;

enum ObservedLocation {
    Head,
    Node(NodeKey),
    Parent(NodeKey, Vec<u8>),
    Absent,
}
struct Observation {
    action: CandidateEditAction,
    path: Vec<u8>,
    location: ObservedLocation,
    source: Option<NodeSummary>,
    target: Option<NodeSummary>,
    after: Option<NodeSummary>,
}
struct Probe {
    inner: Arc<dyn AccessGate>,
    observations: Mutex<Vec<Observation>>,
    deny_source: AtomicBool,
    deny_source_node: Mutex<Option<NodeKey>>,
    deny_candidate: AtomicBool,
    deny_target_parent: AtomicBool,
    pause_file: AtomicBool,
    entered: tokio::sync::Notify,
    resume: tokio::sync::Notify,
}
impl Probe {
    fn new(inner: Arc<dyn AccessGate>) -> Self {
        Self {
            inner,
            observations: Mutex::new(Vec::new()),
            deny_source: AtomicBool::new(false),
            deny_source_node: Mutex::new(None),
            deny_candidate: AtomicBool::new(false),
            deny_target_parent: AtomicBool::new(false),
            pause_file: AtomicBool::new(false),
            entered: tokio::sync::Notify::new(),
            resume: tokio::sync::Notify::new(),
        }
    }
}
impl AccessGate for Probe {
    fn authorize<'a>(&'a self, key: &'a NodeKey, access: Access) -> BackendFuture<'a, ()> {
        Box::pin(async move {
            if access == Access::Write
                && (self.deny_source.load(Ordering::Acquire)
                    || self.deny_source_node.lock().unwrap().as_ref() == Some(key))
            {
                return Err(Error::PermissionDenied);
            }
            self.inner.authorize(key, access).await
        })
    }
    fn authorize_selection<'a>(&'a self, selection: &'a TargetSelection) -> BackendFuture<'a, ()> {
        self.inner.authorize_selection(selection)
    }
    fn authorize_target_head<'a>(
        &'a self,
        proof: &'a ProvenTargetMetadata,
    ) -> BackendFuture<'a, ()> {
        self.inner.authorize_target_head(proof)
    }
    fn authorize_target_node<'a>(
        &'a self,
        proof: &'a ProvenTargetMetadata,
        key: &'a NodeKey,
        access: Access,
    ) -> BackendFuture<'a, ()> {
        self.inner.authorize_target_node(proof, key, access)
    }
    fn authorize_candidate_edit<'a>(&'a self, edit: &'a CandidateEdit) -> BackendFuture<'a, ()> {
        Box::pin(async move {
            if edit.path() == b"file" && self.pause_file.load(Ordering::Acquire) {
                self.entered.notify_one();
                self.resume.notified().await;
            }
            self.inner.authorize_candidate_edit(edit).await?;
            if self.deny_target_parent.load(Ordering::Acquire)
                && matches!(
                    edit.location(),
                    CandidateLocation::TargetDirectoryParent { .. }
                )
            {
                return Err(Error::PermissionDenied);
            }
            let location = match edit.location() {
                CandidateLocation::SelectedHead => ObservedLocation::Head,
                CandidateLocation::TargetNode(node) => ObservedLocation::Node(node.key),
                CandidateLocation::TargetDirectoryParent { parent, tail } => {
                    ObservedLocation::Parent(parent.key, tail.to_vec())
                }
                CandidateLocation::AbsentSelectedScope => ObservedLocation::Absent,
            };
            self.observations.lock().unwrap().push(Observation {
                action: edit.action(),
                path: edit.path().to_vec(),
                location,
                source: edit.source_before().cloned(),
                target: edit.target_before().cloned(),
                after: edit.candidate_after().cloned(),
            });
            if self.deny_candidate.load(Ordering::Acquire) {
                return Err(Error::PermissionDenied);
            }
            Ok(())
        })
    }
}
/// Source writes remain allowed; the candidate decision uses its default
/// denial independently of successful read-only target preparation.
struct NoCandidate(Arc<Probe>);
impl AccessGate for NoCandidate {
    fn authorize<'a>(&'a self, key: &'a NodeKey, access: Access) -> BackendFuture<'a, ()> {
        self.0.authorize(key, access)
    }
    fn authorize_selection<'a>(&'a self, selection: &'a TargetSelection) -> BackendFuture<'a, ()> {
        self.0.authorize_selection(selection)
    }
    fn authorize_target_head<'a>(
        &'a self,
        proof: &'a ProvenTargetMetadata,
    ) -> BackendFuture<'a, ()> {
        self.0.authorize_target_head(proof)
    }
    fn authorize_target_node<'a>(
        &'a self,
        proof: &'a ProvenTargetMetadata,
        key: &'a NodeKey,
        access: Access,
    ) -> BackendFuture<'a, ()> {
        self.0.authorize_target_node(proof, key, access)
    }
}

struct Fixture {
    _owner: Arc<ServiceOwner>,
    _guard: ServiceStartGuard,
    namespace: Arc<Namespace>,
    backend: Arc<RevisionBackend>,
    scope: NodeKey,
    target: Address,
    source_file: u32,
    target_file: u32,
    gate: Arc<Probe>,
}
impl Fixture {
    async fn new(same_bytes: bool, shifted_target_ids: bool, default_denial: bool) -> Self {
        let (owner, guard, admission) = admitted().await;
        let execution = super::working_mount::execution();
        let (backend, target, source_file, target_file) = LORE_CONTEXT
            .scope(execution.clone(), async {
                let repository = repository().await;
                let source = lore_revision::state::State::new();
                let source_file = add_file(&repository, &source, 0, "file", b"source bytes").await;
                let source = commit_state(&repository, source, Hash::default()).await;
                let target = lore_revision::state::State::new();
                if shifted_target_ids {
                    add_file(&repository, &target, 0, "shift", b"other target").await;
                }
                let target_file = add_file(
                    &repository,
                    &target,
                    0,
                    "file",
                    if same_bytes {
                        b"source bytes"
                    } else {
                        b"target bytes"
                    },
                )
                .await;
                let target = commit_state(&repository, target, source).await;
                assert_ne!(source, target);
                (
                    Arc::new(RevisionBackend::new(repository, source).await.unwrap()),
                    Address::zero_context_hash(target),
                    source_file,
                    target_file,
                )
            })
            .await;
        let scope = backend.root().key;
        let gate = Arc::new(Probe::new(
            LocalCheckoutFactory::new(admission, execution)
                .unwrap()
                .bind(backend.clone())
                .unwrap(),
        ));
        let policy: Arc<dyn AccessGate> = if default_denial {
            Arc::new(NoCandidate(gate.clone()))
        } else {
            gate.clone()
        };
        let namespace = Arc::new(
            Namespace::new(backend.clone(), backend.root(), policy, Default::default()).unwrap(),
        );
        Self {
            _owner: owner,
            _guard: guard,
            namespace,
            backend,
            scope,
            target,
            source_file,
            target_file,
            gate,
        }
    }
    async fn plan(&self, options: Options) -> PreparedTransition {
        prepare(
            self.namespace.clone(),
            self.scope,
            self.target,
            options,
            Default::default(),
        )
        .await
        .unwrap()
    }
}

#[derive(Default)]
struct Store {
    bytes: tokio::sync::Mutex<Vec<u8>>,
    writes: AtomicUsize,
}
impl SparseStore for Store {
    fn read<'a>(&'a self, offset: u64, size: usize) -> BackendFuture<'a, Vec<u8>> {
        Box::pin(async move {
            let start = usize::try_from(offset).map_err(|_offset| Error::InvalidInput)?;
            let end = start.checked_add(size).ok_or(Error::InvalidInput)?;
            self.bytes
                .lock()
                .await
                .get(start..end)
                .map(<[u8]>::to_vec)
                .ok_or(Error::Io)
        })
    }
    fn write<'a>(&'a self, offset: u64, bytes: &'a [u8]) -> BackendFuture<'a, usize> {
        Box::pin(async move {
            let start = usize::try_from(offset).map_err(|_offset| Error::InvalidInput)?;
            let end = start.checked_add(bytes.len()).ok_or(Error::InvalidInput)?;
            let mut stored = self.bytes.lock().await;
            let length = stored.len().max(end);
            stored.resize(length, 0);
            stored[start..end].copy_from_slice(bytes);
            self.writes.fetch_add(1, Ordering::Relaxed);
            Ok(bytes.len())
        })
    }
}

#[tokio::test]
async fn candidate_head_is_explicit_even_without_entry_changes_and_defaults_to_denied() {
    let allowed = Fixture::new(true, false, false).await;
    let before = allowed.backend.checkpoint_state().await.unwrap();
    let plan = allowed.plan(Default::default()).await;
    assert!(plan.changes().is_empty());
    plan.authorize_candidate_edits().await.unwrap();
    assert!(
        allowed
            .gate
            .observations
            .lock()
            .unwrap()
            .iter()
            .any(|edit| edit.action == CandidateEditAction::AdvanceSelectedHead)
    );
    plan.validate_source().await.unwrap();
    assert_eq!(allowed.backend.checkpoint_state().await.unwrap(), before);
    assert_eq!(
        allowed.namespace.getattr(1).await.unwrap().authority,
        allowed.scope
    );
    drop(plan);

    let denied = Fixture::new(true, false, true).await;
    let plan = denied.plan(Default::default()).await;
    assert!(plan.changes().is_empty());
    assert!(matches!(
        plan.authorize_candidate_edits().await,
        Err(lore_vfs::transition_prepare::Error::Backend(
            Error::Unsupported
        ))
    ));
    plan.validate_source().await.unwrap();
}

#[tokio::test]
async fn clean_read_materialization_does_not_require_leaf_write_or_candidate_edit() {
    let fixture = Fixture::new(true, false, false).await;
    let file = fixture.namespace.lookup(1, b"file").await.unwrap();
    let store = Arc::new(Store::default());
    let overlay = fixture
        .namespace
        .file(file.inode, store, Default::default())
        .await
        .unwrap();
    let reader = overlay.open(Access::Read).await.unwrap();
    assert_eq!(overlay.read(reader, 0, 64).await.unwrap(), b"source bytes");
    overlay.release(reader).await.unwrap();
    *fixture.gate.deny_source_node.lock().unwrap() = Some(file.authority);

    let plan = fixture.plan(Default::default()).await;
    assert!(plan.changes().is_empty());
    plan.authorize_candidate_edits().await.unwrap();
    assert!(
        fixture
            .gate
            .observations
            .lock()
            .unwrap()
            .iter()
            .all(|edit| edit.action == CandidateEditAction::AdvanceSelectedHead)
    );
    plan.validate_source().await.unwrap();
}

#[tokio::test]
async fn carried_local_deletion_requires_target_parent_decision() {
    let fixture = Fixture::new(true, false, false).await;
    fixture.namespace.remove(1, b"file", false).await.unwrap();
    let plan = fixture
        .plan(Options {
            local: LocalPolicy::KeepLocal,
            ..Default::default()
        })
        .await;
    assert!(plan.changes().is_empty());
    fixture
        .gate
        .deny_target_parent
        .store(true, Ordering::Release);
    assert!(plan.authorize_candidate_edits().await.is_err());
    fixture
        .gate
        .deny_target_parent
        .store(false, Ordering::Release);
    plan.authorize_candidate_edits().await.unwrap();
    assert!(
        fixture
            .gate
            .observations
            .lock()
            .unwrap()
            .iter()
            .any(|edit| edit.path == b"file"
                && matches!(&edit.location, ObservedLocation::Parent(_, tail) if tail == b"file")
                && edit.source.is_none()
                && edit.target.is_some()
                && edit.after.is_none())
    );
    plan.validate_source().await.unwrap();
}

#[tokio::test]
async fn carried_local_edits_use_actual_target_authority_and_write_no_candidate_bytes() {
    let fixture = Fixture::new(false, true, false).await;
    assert_ne!(fixture.source_file, fixture.target_file);
    let file = fixture.namespace.lookup(1, b"file").await.unwrap();
    let store = Arc::new(Store::default());
    let overlay = fixture
        .namespace
        .file(file.inode, store.clone(), Default::default())
        .await
        .unwrap();
    let writer = overlay.open(Access::Write).await.unwrap();
    overlay
        .write(writer, 0, b"kept private local bytes")
        .await
        .unwrap();
    overlay.release(writer).await.unwrap();
    let new_store = Arc::new(Store::default());
    let local = fixture
        .namespace
        .create_file(1, b"local", 0o644, new_store.clone(), Default::default())
        .await
        .unwrap();
    let local = fixture
        .namespace
        .materialized_file(local.inode)
        .await
        .unwrap()
        .unwrap();
    let writer = local.open(Access::Write).await.unwrap();
    local.write(writer, 0, b"local-only bytes").await.unwrap();
    local.release(writer).await.unwrap();
    let writes = (
        store.writes.load(Ordering::Relaxed),
        new_store.writes.load(Ordering::Relaxed),
    );
    let plan = fixture
        .plan(Options {
            local: LocalPolicy::KeepLocal,
            ..Default::default()
        })
        .await;
    assert!(
        !plan
            .changes()
            .iter()
            .any(|change| change.path == b"file" || change.path == b"local")
    );
    assert_eq!(
        plan.candidate_origin("file").unwrap().node,
        fixture.source_file
    );
    plan.authorize_candidate_edits().await.unwrap();
    {
        let edits = fixture.gate.observations.lock().unwrap();
        let kept = edits.iter().find(|edit| edit.path == b"file").unwrap();
        assert!(
            matches!(kept.location, ObservedLocation::Node(key) if key.node == fixture.target_file)
        );
        assert_eq!(kept.source, kept.after);
        assert_ne!(kept.target, kept.after);
        let new = edits.iter().find(|edit| edit.path == b"local").unwrap();
        assert!(
            matches!(&new.location, ObservedLocation::Parent(key, tail) if key.node == 0 && tail == b"local")
        );
        assert!(new.source.is_some() && new.target.is_none());
        assert_eq!(new.source, new.after);
    }
    assert_eq!(
        (
            store.writes.load(Ordering::Relaxed),
            new_store.writes.load(Ordering::Relaxed)
        ),
        writes
    );
    plan.validate_source().await.unwrap();
    let view = plan.prepare_target_view().await.unwrap();
    let target_file = view
        .backend()
        .lookup(&view.root().unwrap().key, b"file")
        .await
        .unwrap();
    assert_eq!(
        view.backend().revalidate(&target_file.key, true).await,
        Err(Error::ReadOnly)
    );
}

#[tokio::test]
async fn source_write_and_candidate_edit_denials_remain_independent() {
    let fixture = Fixture::new(false, false, false).await;
    let plan = fixture.plan(Default::default()).await;
    fixture.gate.deny_source.store(true, Ordering::Release);
    assert!(plan.authorize_candidate_edits().await.is_err());
    assert!(fixture.gate.observations.lock().unwrap().is_empty());
    fixture.gate.deny_source.store(false, Ordering::Release);
    fixture.gate.deny_candidate.store(true, Ordering::Release);
    assert!(plan.authorize_candidate_edits().await.is_err());
    assert!(!fixture.gate.observations.lock().unwrap().is_empty());
    fixture.gate.deny_candidate.store(false, Ordering::Release);
    plan.authorize_candidate_edits().await.unwrap();
    plan.validate_source().await.unwrap();
}

#[tokio::test]
async fn source_write_revoked_while_candidate_check_waits_is_rechecked() {
    let fixture = Fixture::new(false, false, false).await;
    let plan = fixture.plan(Default::default()).await;
    fixture.gate.pause_file.store(true, Ordering::Release);
    let mut authorization = Box::pin(plan.authorize_candidate_edits());
    tokio::select! {
        _=fixture.gate.entered.notified()=>{},
        result=&mut authorization=>panic!("candidate did not reach pending policy: {result:?}"),
        _=tokio::time::sleep(Duration::from_secs(10))=>panic!("candidate policy never started"),
    }
    fixture.gate.deny_source.store(true, Ordering::Release);
    fixture.gate.pause_file.store(false, Ordering::Release);
    fixture.gate.resume.notify_one();
    assert!(authorization.await.is_err());
    plan.validate_source().await.unwrap();
}

#[tokio::test]
async fn cancelled_candidate_check_preserves_source_and_releases_freeze() {
    let fixture = Fixture::new(false, false, false).await;
    let plan = fixture.plan(Default::default()).await;
    fixture.gate.pause_file.store(true, Ordering::Release);
    let mut authorization = Box::pin(plan.authorize_candidate_edits());
    tokio::select! {
        _=fixture.gate.entered.notified()=>{},
        result=&mut authorization=>panic!("candidate did not reach cancellation point: {result:?}"),
        _=tokio::time::sleep(Duration::from_secs(10))=>panic!("candidate policy never started"),
    }
    drop(authorization);
    plan.validate_source().await.unwrap();
    drop(plan);
    drop(fixture.namespace.freeze_operation().await.unwrap());
}

async fn directory(
    repository: &Arc<lore_revision::repository::RepositoryContext>,
    state: &lore_revision::state::State,
    name: &str,
) -> u32 {
    let node = state
        .node_add(
            repository.clone(),
            0,
            lore_revision::node::Node {
                name_hash: lore_storage::hash::hash_string(name),
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
            lore_revision::node::NodeFlags::StagedAdd,
            lore_revision::node::NodeFlags::DirtyAdd,
        )
        .await
        .unwrap();
    node
}

#[tokio::test]
async fn source_parent_write_is_required_for_deletion_without_overrestricting_content_edits() {
    for (deletion, carried) in [(false, false), (true, false), (true, true)] {
        let (_owner, _guard, admission) = admitted().await;
        let execution = super::working_mount::execution();
        let (backend, target, source_parent) = LORE_CONTEXT
            .scope(execution.clone(), async {
                let repository = repository().await;
                let source = lore_revision::state::State::new();
                let parent = directory(&repository, &source, "dir").await;
                add_file(&repository, &source, parent, "file", b"source bytes").await;
                let source_pin = commit_state(&repository, source, Hash::default()).await;
                let target = lore_revision::state::State::new();
                let target_parent = directory(&repository, &target, "dir").await;
                if !deletion || carried {
                    add_file(
                        &repository,
                        &target,
                        target_parent,
                        "file",
                        if carried {
                            b"source bytes"
                        } else {
                            b"changed bytes"
                        },
                    )
                    .await;
                }
                let target = commit_state(&repository, target, source_pin).await;
                (
                    Arc::new(RevisionBackend::new(repository, source_pin).await.unwrap()),
                    Address::zero_context_hash(target),
                    parent,
                )
            })
            .await;
        let scope = backend.root().key;
        let gate = Arc::new(Probe::new(
            LocalCheckoutFactory::new(admission, execution)
                .unwrap()
                .bind(backend.clone())
                .unwrap(),
        ));
        let namespace = Arc::new(
            Namespace::new(
                backend.clone(),
                backend.root(),
                gate.clone(),
                Default::default(),
            )
            .unwrap(),
        );
        if carried {
            let parent = namespace.lookup(1, b"dir").await.unwrap();
            namespace
                .remove(parent.inode, b"file", false)
                .await
                .unwrap();
        }
        let plan = prepare(
            namespace,
            scope,
            target,
            Options {
                local: LocalPolicy::KeepLocal,
                ..Default::default()
            },
            Default::default(),
        )
        .await
        .unwrap();
        if carried {
            assert!(plan.changes().is_empty());
        }
        *gate.deny_source_node.lock().unwrap() = Some(NodeKey {
            node: source_parent,
            ..scope
        });
        let result = plan.authorize_candidate_edits().await;
        if deletion {
            assert!(
                result.is_err(),
                "source directory denial must prevent deleting its child"
            );
        } else {
            result.unwrap();
        }
        *gate.deny_source_node.lock().unwrap() = None;
        plan.authorize_candidate_edits().await.unwrap();
        plan.validate_source().await.unwrap();
    }
}

#[tokio::test]
async fn readable_changed_link_metadata_does_not_grant_candidate_link_rewrites() {
    let (_owner, _guard, admission) = admitted().await;
    let execution = super::working_mount::execution();
    let (backend, target) = LORE_CONTEXT
        .scope(execution.clone(), async {
            let parent = repository().await;
            let child = parent.to_link_context([242; 16].into()).await;
            let child_target = lore_revision::state::State::new();
            add_file(&child, &child_target, 0, "file", b"child target").await;
            let child_target = commit_state(&child, child_target, Hash::default()).await;
            let mut pins = Vec::new();
            let mut previous = Hash::default();
            for pin in [Hash::default(), child_target] {
                let state = lore_revision::state::State::new();
                let link = state
                    .node_add(
                        parent.clone(),
                        0,
                        lore_revision::node::Node {
                            flags: lore_revision::node::NodeFlags::Link.bits(),
                            child: 0,
                            address: Address {
                                context: child.id.into(),
                                hash: pin,
                            },
                            name_hash: lore_storage::hash::hash_string("child"),
                            ..Default::default()
                        },
                        "child",
                    )
                    .await
                    .unwrap();
                state
                    .node_mark_staged(
                        parent.clone(),
                        link,
                        lore_revision::node::NodeFlags::StagedAdd,
                        lore_revision::node::NodeFlags::DirtyAdd,
                    )
                    .await
                    .unwrap();
                previous = commit_state(&parent, state, previous).await;
                pins.push(previous);
            }
            (
                Arc::new(RevisionBackend::new(parent, pins[0]).await.unwrap()),
                Address::zero_context_hash(pins[1]),
            )
        })
        .await;
    let scope = backend.root().key;
    let gate = Arc::new(Probe::new(
        LocalCheckoutFactory::new(admission, execution)
            .unwrap()
            .bind(backend.clone())
            .unwrap(),
    ));
    let namespace = Arc::new(
        Namespace::new(
            backend.clone(),
            backend.root(),
            gate.clone(),
            Default::default(),
        )
        .unwrap(),
    );
    let before = backend.checkpoint_state().await.unwrap();
    let plan = prepare(
        namespace,
        scope,
        target,
        Default::default(),
        Default::default(),
    )
    .await
    .unwrap();
    assert!(plan.candidate_node("child").unwrap().link.is_some());
    assert!(plan.authorize_candidate_edits().await.is_err());
    assert_eq!(backend.checkpoint_state().await.unwrap(), before);
    plan.validate_source().await.unwrap();
}

#[tokio::test]
async fn target_parent_denial_prevents_type_replacement_despite_node_write_decision() {
    let (_owner, _guard, admission) = admitted().await;
    let execution = super::working_mount::execution();
    let (backend, target) = LORE_CONTEXT
        .scope(execution.clone(), async {
            let repository = repository().await;
            let source = lore_revision::state::State::new();
            let parent = directory(&repository, &source, "dir").await;
            add_file(&repository, &source, parent, "entry", b"source file").await;
            let source_pin = commit_state(&repository, source, Hash::default()).await;
            let target = lore_revision::state::State::new();
            let parent = directory(&repository, &target, "dir").await;
            let entry = target
                .node_add(
                    repository.clone(),
                    parent,
                    lore_revision::node::Node {
                        name_hash: lore_storage::hash::hash_string("entry"),
                        ..Default::default()
                    },
                    "entry",
                )
                .await
                .unwrap();
            target
                .node_mark_staged(
                    repository.clone(),
                    entry,
                    lore_revision::node::NodeFlags::StagedAdd,
                    lore_revision::node::NodeFlags::DirtyAdd,
                )
                .await
                .unwrap();
            let target_pin = commit_state(&repository, target, source_pin).await;
            (
                Arc::new(RevisionBackend::new(repository, source_pin).await.unwrap()),
                Address::zero_context_hash(target_pin),
            )
        })
        .await;
    let scope = backend.root().key;
    let gate = Arc::new(Probe::new(
        LocalCheckoutFactory::new(admission, execution)
            .unwrap()
            .bind(backend.clone())
            .unwrap(),
    ));
    let namespace = Arc::new(
        Namespace::new(
            backend.clone(),
            backend.root(),
            gate.clone(),
            Default::default(),
        )
        .unwrap(),
    );
    let plan = prepare(
        namespace,
        scope,
        target,
        Default::default(),
        Default::default(),
    )
    .await
    .unwrap();
    gate.deny_target_parent.store(true, Ordering::Release);
    assert!(plan.authorize_candidate_edits().await.is_err());
    assert!(gate.observations.lock().unwrap().iter().any(
        |edit| edit.path == b"dir/entry" && matches!(edit.location, ObservedLocation::Node(_))
    ));
    gate.deny_target_parent.store(false, Ordering::Release);
    plan.authorize_candidate_edits().await.unwrap();
    plan.validate_source().await.unwrap();
}
