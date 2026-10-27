// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
#![cfg(unix)]

#[path = "working_mount_complete_policy.rs"]
mod complete_backend;

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;

use lore::remote::service_process::ServiceStartGuard;
use lore::remote::service_process::StopRequest;
use lore::remote::service_process::begin_service_start_for;
use lore::remote::service_process::register_service_process;
use lore::service::owner::RunCleanup;
use lore::service::owner::ServiceOwner;
use lore::service::state::ServiceStateImpl;
use lore::service::state::WorkingAdmission;
use lore::service::state::with_invocation;
use lore::service::state::with_owner;
use lore::service::state::working_admission;
use lore::working_mount::local_policy::LocalCheckoutFactory;
use lore_base::runtime::LORE_CONTEXT;
use lore_base::types::Address;
use lore_base::types::Hash;
use lore_base::types::RepositoryId;
use lore_revision::fs::provider_registry::ProviderResolver;
use lore_revision::interface::ExecutionContext;
use lore_revision::node::Node as RevisionNode;
use lore_revision::node::NodeFlags;
use lore_revision::repository::RepositoryContext;
use lore_revision::state::State;
use lore_vfs::core::Backend;
use lore_vfs::core::BackendFuture;
use lore_vfs::core::BackendPage;
use lore_vfs::core::Error;
use lore_vfs::core::Node;
use lore_vfs::core::NodeKey;
use lore_vfs::overlay::Access;
use lore_vfs::overlay::AccessGate;
use lore_vfs::overlay::AccessGateFactory;
use lore_vfs::overlay::CandidateEdit;
use lore_vfs::overlay::ProvenTargetMetadata;
use lore_vfs::overlay::TargetSelection;
use lore_vfs::revision::RevisionBackend;

pub(super) async fn admitted() -> (Arc<ServiceOwner>, ServiceStartGuard, WorkingAdmission) {
    let state = Arc::new(ServiceStateImpl::new());
    state.initialize();
    let owner = ServiceOwner::for_state(state.clone()).unwrap();
    let request = Arc::new(StopRequest::new());
    let guard = begin_service_start_for(request).unwrap();
    assert!(guard.bind_owner(&owner));
    register_service_process(&guard);
    let admission = with_invocation(
        state.try_begin_invocation().unwrap(),
        with_owner(owner.clone(), async { working_admission().unwrap() }),
    )
    .await
    .unwrap();
    (owner, guard, admission)
}

pub(super) async fn repository() -> Arc<RepositoryContext> {
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
    Arc::new(RepositoryContext::new_server_context(
        immutable,
        mutable,
        RepositoryId::from([231; 16]),
    ))
}

async fn linked_backend(
    execution: Arc<ExecutionContext>,
    allowed: Arc<AtomicBool>,
) -> Arc<RevisionBackend> {
    linked_backend_with_check(
        execution,
        Arc::new(move |_repository| allowed.load(Ordering::Acquire)),
    )
    .await
}

async fn linked_backend_with_check(
    execution: Arc<ExecutionContext>,
    check: lore_revision::state::CanReadRepository,
) -> Arc<RevisionBackend> {
    linked_backend_parts(execution, check).await.0
}

async fn linked_backend_parts(
    execution: Arc<ExecutionContext>,
    check: lore_revision::state::CanReadRepository,
) -> (Arc<RevisionBackend>, Arc<RepositoryContext>, Hash) {
    LORE_CONTEXT
        .scope(execution, async {
            let repository = repository().await;
            let state = State::new();
            for name in ["child", "sibling"] {
                state
                    .node_add(
                        repository.clone(),
                        0,
                        RevisionNode {
                            flags: NodeFlags::Link.bits(),
                            child: 0,
                            address: Address {
                                context: RepositoryId::from([232; 16]).into(),
                                hash: Hash::default(),
                            },
                            name_hash: lore_storage::hash::hash_string(name),
                            ..Default::default()
                        },
                        name,
                    )
                    .await
                    .unwrap();
            }
            let revision = state
                .serialize(repository.clone(), repository.try_write_token().unwrap())
                .await
                .unwrap();
            let repository = Arc::new(repository.without_command_locks().with_link_read(check));
            let backend = Arc::new(
                RevisionBackend::new(repository.clone(), revision)
                    .await
                    .unwrap(),
            );
            (backend, repository, revision)
        })
        .await
}

#[tokio::test]
async fn local_selection_allows_exact_source_read_write_and_rejects_unproven_keys() {
    let (_owner, _guard, admission) = admitted().await;
    let execution = super::working_mount::execution();
    let backend = linked_backend(execution.clone(), Arc::new(AtomicBool::new(true))).await;
    let root = backend.root().key;
    let gate = LocalCheckoutFactory::new(admission, execution)
        .unwrap()
        .bind(backend)
        .unwrap();
    gate.authorize(&root, Access::Read).await.unwrap();
    gate.authorize(&root, Access::Write).await.unwrap();
    for key in [
        NodeKey {
            repository: [233; 16].into(),
            ..root
        },
        NodeKey {
            exposure: u64::MAX,
            ..root
        },
        NodeKey {
            revision: Address::zero_context_hash([234; 32].into()),
            ..root
        },
        NodeKey {
            node: u32::MAX,
            ..root
        },
    ] {
        assert!(gate.authorize(&key, Access::Read).await.is_err());
        assert!(gate.authorize(&key, Access::Write).await.is_err());
    }
}

#[tokio::test]
async fn linked_source_is_rechecked_and_opaque_link_nodes_cannot_be_written() {
    let (_owner, _guard, admission) = admitted().await;
    let execution = super::working_mount::execution();
    let allowed = Arc::new(AtomicBool::new(true));
    let backend = linked_backend(execution.clone(), allowed.clone()).await;
    let child = backend.lookup(&backend.root().key, b"child").await.unwrap();
    let gate = LocalCheckoutFactory::new(admission.clone(), execution.clone())
        .unwrap()
        .bind(backend)
        .unwrap();
    gate.authorize(&child.key, Access::Read).await.unwrap();
    gate.authorize(&child.key, Access::Write).await.unwrap();
    allowed.store(false, Ordering::Release);
    assert_eq!(
        gate.authorize(&child.key, Access::Read).await,
        Err(Error::PermissionDenied)
    );
    assert_eq!(
        gate.authorize(&child.key, Access::Write).await,
        Err(Error::PermissionDenied)
    );

    let backend = linked_backend(execution.clone(), Arc::new(AtomicBool::new(false))).await;
    let opaque = backend.lookup(&backend.root().key, b"child").await.unwrap();
    let gate = LocalCheckoutFactory::new(admission, execution)
        .unwrap()
        .bind(backend)
        .unwrap();
    gate.authorize(&opaque.key, Access::Read).await.unwrap();
    assert_eq!(
        gate.authorize(&opaque.key, Access::Write).await,
        Err(Error::PermissionDenied)
    );
}

#[tokio::test]
async fn retained_policy_supports_cleanup_but_does_not_keep_owner_or_stale_run_alive() {
    let (owner, guard, admission) = admitted().await;
    let execution = super::working_mount::execution();
    let backend = linked_backend(execution.clone(), Arc::new(AtomicBool::new(true))).await;
    let root = backend.root().key;
    let gate = LocalCheckoutFactory::new(admission, execution)
        .unwrap()
        .bind(backend)
        .unwrap();
    drop(RunCleanup::new(owner.clone()));
    assert!(owner.state.try_begin_invocation().is_none());
    // No trusted Work scope remains, and normal admission is closed. Durable
    // retirement still needs reads through the exact retained policy.
    gate.authorize(&root, Access::Read).await.unwrap();
    drop(guard);
    assert_eq!(
        gate.authorize(&root, Access::Read).await,
        Err(Error::PermissionDenied)
    );

    let (owner, _guard, admission) = admitted().await;
    let execution = super::working_mount::execution();
    let backend = linked_backend(execution.clone(), Arc::new(AtomicBool::new(true))).await;
    let root = backend.root().key;
    let gate = LocalCheckoutFactory::new(admission, execution)
        .unwrap()
        .bind(backend)
        .unwrap();
    let weak = Arc::downgrade(&owner);
    drop(owner);
    assert!(weak.upgrade().is_none());
    assert_eq!(
        gate.authorize(&root, Access::Read).await,
        Err(Error::PermissionDenied)
    );
}

struct UnprovenBackend;
impl Backend for UnprovenBackend {
    fn lookup<'a>(&'a self, _parent: &'a NodeKey, _name: &'a [u8]) -> BackendFuture<'a, Node> {
        Box::pin(async { Err(Error::Unsupported) })
    }
    fn list<'a>(
        &'a self,
        _directory: &'a NodeKey,
        _offset: u64,
        _limit: usize,
    ) -> BackendFuture<'a, BackendPage> {
        Box::pin(async { Err(Error::Unsupported) })
    }
    fn read<'a>(
        &'a self,
        _file: &'a NodeKey,
        _offset: u64,
        _size: usize,
    ) -> BackendFuture<'a, Vec<u8>> {
        Box::pin(async { Err(Error::Unsupported) })
    }
    fn digest<'a>(&'a self, _file: &'a NodeKey) -> BackendFuture<'a, Option<[u8; 32]>> {
        Box::pin(async { Err(Error::Unsupported) })
    }
}

#[tokio::test]
async fn factory_rejects_default_backend_revalidation_and_a_different_captured_context() {
    let (_owner, _guard, admission) = admitted().await;
    let execution = super::working_mount::execution();
    let backend = linked_backend(execution.clone(), Arc::new(AtomicBool::new(true))).await;
    let factory = LocalCheckoutFactory::new(admission.clone(), execution).unwrap();
    assert!(matches!(
        factory.bind(Arc::new(UnprovenBackend)),
        Err(Error::Unsupported)
    ));
    let unrelated =
        LocalCheckoutFactory::new(admission, super::working_mount::execution()).unwrap();
    assert!(matches!(unrelated.bind(backend), Err(Error::InvalidInput)));
}

#[tokio::test]
async fn run_revoked_during_concrete_route_revalidation_cannot_authorize_success() {
    let (_owner, guard, admission) = admitted().await;
    let guard = Arc::new(Mutex::new(Some(guard)));
    let revoke = Arc::new(AtomicBool::new(false));
    let checked_guard = guard.clone();
    let checked_revoke = revoke.clone();
    let execution = super::working_mount::execution();
    let backend = linked_backend_with_check(
        execution.clone(),
        Arc::new(move |_repository| {
            if checked_revoke.load(Ordering::Acquire) {
                checked_guard.lock().unwrap().take();
            }
            true
        }),
    )
    .await;
    let child = backend.lookup(&backend.root().key, b"child").await.unwrap();
    let gate = LocalCheckoutFactory::new(admission, execution)
        .unwrap()
        .bind(backend)
        .unwrap();
    revoke.store(true, Ordering::Release);
    assert_eq!(
        gate.authorize(&child.key, Access::Read).await,
        Err(Error::PermissionDenied)
    );
    assert!(guard.lock().unwrap().is_none());
}

#[tokio::test]
async fn local_factory_checkpoints_owned_bytes_and_rebinds_to_a_recovered_owner() {
    use lore_vfs::linux_working::OpenFlags;

    LORE_CONTEXT
        .scope(super::working_mount::execution(), async {
            let root = lore_base::test_util::TempDir::new("lore-local-policy-recovery-");
            let (owner, guard, admission) = admitted().await;
            let supervisor = owner._supervisor.clone();
            let input = super::working_mount::input(&root, owner.state.provider_registry()).await;
            let instance = input.prepared.location.instance();
            let factory = Arc::new(
                LocalCheckoutFactory::new(admission, input.prepared.execution.clone()).unwrap(),
            );
            let prepared = supervisor
                .prepare_instance_with_gate_factory(
                    input,
                    factory,
                    Default::default(),
                    Default::default(),
                )
                .await
                .unwrap();
            let handle = prepared.handle();
            let model = prepared.test_model();
            let root_key = model.namespace().getattr(1).await.unwrap().authority;
            let selection = lore_vfs::transition_prepare::prepare(
                model.namespace().clone(),
                root_key,
                Address::zero_context_hash([237; 32].into()),
                Default::default(),
                Default::default(),
            )
            .await;
            assert!(selection.is_err());
            assert!(
                !matches!(
                    selection,
                    Err(lore_vfs::transition_prepare::Error::Backend(
                        Error::Unsupported
                    ))
                ),
                "owner context wrapper must forward local selection preflight"
            );
            let unchanged = lore_vfs::transition_prepare::prepare(
                model.namespace().clone(),
                root_key,
                root_key.revision,
                Default::default(),
                Default::default(),
            )
            .await
            .unwrap();
            unchanged.authorize_candidate_edits().await.unwrap();
            drop(unchanged);
            let flags = OpenFlags {
                write: true,
                append: false,
                truncate: false,
            };
            let (file, _) = model.create(1, b"local", 0o644, flags).await.unwrap();
            model.write(file, 0, b"private local bytes").await.unwrap();
            model.release(file).await.unwrap();
            drop(model);
            drop(prepared);
            tokio::time::timeout(
                Duration::from_secs(30),
                supervisor.retire_unstarted(&handle),
            )
            .await
            .unwrap()
            .unwrap();
            assert!(supervisor.instances().unwrap().is_empty());
            drop(supervisor);
            drop(guard);
            drop(owner);

            let (owner, _guard, admission) = admitted().await;
            let supervisor = owner._supervisor.clone();
            let input =
                super::working_mount::reuse_input(&root, owner.state.provider_registry(), instance)
                    .await;
            let factory = Arc::new(
                LocalCheckoutFactory::new(admission, input.prepared.execution.clone()).unwrap(),
            );
            let prepared = supervisor
                .prepare_instance_with_gate_factory(
                    input,
                    factory,
                    Default::default(),
                    Default::default(),
                )
                .await
                .unwrap();
            let handle = prepared.handle();
            let model = prepared.test_model();
            let entry = model.lookup(1, b"local").await.unwrap();
            let file = model
                .open(
                    entry.entry.inode,
                    OpenFlags {
                        write: false,
                        ..flags
                    },
                )
                .await
                .unwrap();
            assert_eq!(
                model.read(file, 0, 64).await.unwrap(),
                b"private local bytes"
            );
            model.release(file).await.unwrap();
            drop(model);
            drop(prepared);
            tokio::time::timeout(
                Duration::from_secs(30),
                supervisor.retire_unstarted(&handle),
            )
            .await
            .unwrap()
            .unwrap();
        })
        .await;
}

struct PausedFactory {
    inner: LocalCheckoutFactory,
    entered: tokio::sync::Notify,
    release: Mutex<std::sync::mpsc::Receiver<()>>,
}
impl AccessGateFactory for PausedFactory {
    fn bind(&self, backend: Arc<dyn Backend>) -> lore_vfs::core::Result<Arc<dyn AccessGate>> {
        self.entered.notify_one();
        self.release
            .lock()
            .unwrap()
            .recv_timeout(Duration::from_secs(10))
            .unwrap();
        self.inner.bind(backend)
    }
}

#[tokio::test]
async fn cancelled_factory_binding_remains_owned_through_terminal_drain() {
    LORE_CONTEXT.scope(super::working_mount::execution(), async {
        let root = lore_base::test_util::TempDir::new("lore-local-policy-cancel-");
        let (owner, _guard, admission) = admitted().await;
        let supervisor = owner._supervisor.clone();
        let input = super::working_mount::input(&root, owner.state.provider_registry()).await;
        let location = input.prepared.location.clone();
        let (release, receiver) = std::sync::mpsc::channel();
        let factory = Arc::new(PausedFactory {
            inner: LocalCheckoutFactory::new(admission, input.prepared.execution.clone()).unwrap(),
            entered: tokio::sync::Notify::new(),
            release: Mutex::new(receiver),
        });
        let mut preparation = Box::pin(supervisor.prepare_instance_with_gate_factory(input, factory.clone(), Default::default(), Default::default()));
        tokio::select! {
            _=factory.entered.notified()=>{},
            result=&mut preparation=>panic!("binding did not reach owned pause: {}", result.is_ok()),
            _=tokio::time::sleep(Duration::from_secs(10))=>panic!("factory binding never started"),
        }
        drop(preparation);
        assert_eq!(supervisor.instances().unwrap().len(), 1);
        let mut drain = Box::pin(supervisor.close_and_drain());
        assert!(tokio::time::timeout(Duration::from_millis(20), &mut drain).await.is_err());
        release.send(()).unwrap();
        let report = tokio::time::timeout(Duration::from_secs(30), drain).await.unwrap().unwrap();
        assert!(report.failures.is_empty());
        assert!(supervisor.instances().unwrap().is_empty());
        assert!(owner.state.provider_registry().resolve_location(&location).is_err());
    }).await;
}

#[tokio::test]
async fn local_factory_binds_restored_repeated_link_exposures_to_the_final_backend() {
    let (_owner, _guard, admission) = admitted().await;
    let execution = super::working_mount::execution();
    let allowed = Arc::new(AtomicBool::new(true));
    let checked = allowed.clone();
    let (backend, repository, revision) = linked_backend_parts(
        execution.clone(),
        Arc::new(move |_repository| checked.load(Ordering::Acquire)),
    )
    .await;
    let root = backend.root();
    let factory = Arc::new(LocalCheckoutFactory::new(admission, execution.clone()).unwrap());
    let namespace = lore_vfs::namespace::Namespace::new_with_gate_factory(
        backend,
        root.clone(),
        factory.clone(),
        Default::default(),
    )
    .unwrap();
    let left = namespace.lookup(1, b"child").await.unwrap();
    let right = namespace.lookup(1, b"sibling").await.unwrap();
    assert_ne!(left.authority.exposure, right.authority.exposure);
    let snapshot = namespace.snapshot(0).await.unwrap();
    let fresh = LORE_CONTEXT
        .scope(execution, RevisionBackend::new(repository, revision))
        .await
        .unwrap();
    assert!(
        fresh.revalidate(&left.authority, false).await.is_err(),
        "fresh source backend has no restored exposure capability"
    );
    let restored = lore_vfs::namespace::Namespace::restore_with_gate_factory(
        Arc::new(fresh),
        root,
        factory,
        snapshot,
        Default::default(),
        Default::default(),
        Default::default(),
    )
    .await
    .unwrap();
    assert_eq!(
        restored.getattr(left.inode).await.unwrap().authority,
        left.authority
    );
    assert_eq!(
        restored.getattr(right.inode).await.unwrap().authority,
        right.authority
    );
    allowed.store(false, Ordering::Release);
    assert!(restored.getattr(left.inode).await.is_err());
    assert!(restored.getattr(right.inode).await.is_err());
}

struct SelectionProbe {
    inner: Arc<dyn AccessGate>,
    other_backend: Arc<dyn AccessGate>,
    expected_source: NodeKey,
    requested: Address,
    seen_requested: AtomicBool,
}
impl AccessGate for SelectionProbe {
    fn authorize_target_head<'a>(
        &'a self,
        proof: &'a ProvenTargetMetadata,
    ) -> BackendFuture<'a, ()> {
        Box::pin(async move {
            self.inner.authorize_target_head(proof).await?;
            assert!(
                self.other_backend
                    .authorize_target_head(proof)
                    .await
                    .is_err()
            );
            Ok(())
        })
    }
    fn authorize_target_node<'a>(
        &'a self,
        proof: &'a ProvenTargetMetadata,
        key: &'a NodeKey,
        access: Access,
    ) -> BackendFuture<'a, ()> {
        self.inner.authorize_target_node(proof, key, access)
    }
    fn authorize<'a>(&'a self, key: &'a NodeKey, access: Access) -> BackendFuture<'a, ()> {
        self.inner.authorize(key, access)
    }
    fn authorize_selection<'a>(&'a self, selection: &'a TargetSelection) -> BackendFuture<'a, ()> {
        Box::pin(async move {
            assert_eq!(selection.original_scope(), self.expected_source);
            self.inner.authorize_selection(selection).await?;
            assert!(
                self.other_backend
                    .authorize_selection(selection)
                    .await
                    .is_err(),
                "same-key unrelated backend must not consume the descriptor"
            );
            if selection.requested_revision() == self.requested {
                self.seen_requested.store(true, Ordering::Release);
            }
            Ok(())
        })
    }
}

#[tokio::test]
async fn local_selection_preflight_is_backend_bound_and_grants_no_target_node_access() {
    let (_owner, _guard, admission) = admitted().await;
    let execution = super::working_mount::execution();
    let (backend, other) = LORE_CONTEXT
        .scope(execution.clone(), async {
            let repository = repository().await;
            (
                Arc::new(
                    RevisionBackend::new(repository.clone(), Hash::default())
                        .await
                        .unwrap(),
                ),
                Arc::new(
                    RevisionBackend::new(repository, Hash::default())
                        .await
                        .unwrap(),
                ),
            )
        })
        .await;
    let root = backend.root();
    let factory = LocalCheckoutFactory::new(admission, execution).unwrap();
    let requested = Address::zero_context_hash([238; 32].into());
    let probe = Arc::new(SelectionProbe {
        inner: factory.bind(backend.clone()).unwrap(),
        other_backend: factory.bind(other).unwrap(),
        expected_source: root.key,
        requested,
        seen_requested: AtomicBool::new(false),
    });
    let namespace = Arc::new(
        lore_vfs::namespace::Namespace::new(
            backend,
            root.clone(),
            probe.clone(),
            Default::default(),
        )
        .unwrap(),
    );
    let result = lore_vfs::transition_prepare::prepare(
        namespace.clone(),
        root.key,
        requested,
        Default::default(),
        Default::default(),
    )
    .await;
    assert!(
        result.is_err(),
        "requested target metadata is deliberately absent"
    );
    assert!(
        probe.seen_requested.load(Ordering::Acquire),
        "exact local selection preflight must precede target metadata access"
    );
    let fabricated_target = NodeKey {
        revision: requested,
        ..root.key
    };
    assert!(
        probe
            .inner
            .authorize(&fabricated_target, Access::Read)
            .await
            .is_err()
    );
    assert!(
        probe
            .inner
            .authorize(&fabricated_target, Access::Write)
            .await
            .is_err()
    );
    drop(namespace.freeze_operation().await.unwrap());
}

struct RevokingSelection {
    inner: Arc<dyn AccessGate>,
    revoke: Arc<AtomicBool>,
}
impl AccessGate for RevokingSelection {
    fn authorize<'a>(&'a self, key: &'a NodeKey, access: Access) -> BackendFuture<'a, ()> {
        self.inner.authorize(key, access)
    }
    fn authorize_selection<'a>(&'a self, selection: &'a TargetSelection) -> BackendFuture<'a, ()> {
        self.revoke.store(true, Ordering::Release);
        self.inner.authorize_selection(selection)
    }
}

#[tokio::test]
async fn run_revoked_during_selection_source_revalidation_is_denied() {
    let (_owner, guard, admission) = admitted().await;
    let guard = Arc::new(Mutex::new(Some(guard)));
    let checked_guard = guard.clone();
    let revoke = Arc::new(AtomicBool::new(false));
    let checked_revoke = revoke.clone();
    let execution = super::working_mount::execution();
    let backend = linked_backend_with_check(
        execution.clone(),
        Arc::new(move |_repository| {
            if checked_revoke.load(Ordering::Acquire) {
                checked_guard.lock().unwrap().take();
            }
            true
        }),
    )
    .await;
    let root = backend.root();
    let local = LocalCheckoutFactory::new(admission, execution)
        .unwrap()
        .bind(backend.clone())
        .unwrap();
    let namespace = Arc::new(
        lore_vfs::namespace::Namespace::new(
            backend,
            root,
            Arc::new(RevokingSelection {
                inner: local,
                revoke,
            }),
            Default::default(),
        )
        .unwrap(),
    );
    let child = namespace.lookup(1, b"child").await.unwrap();
    let result = lore_vfs::transition_prepare::prepare(
        namespace.clone(),
        child.authority,
        Address::default(),
        Default::default(),
        Default::default(),
    )
    .await;
    assert!(matches!(
        result,
        Err(lore_vfs::transition_prepare::Error::Backend(
            Error::PermissionDenied
        ))
    ));
    assert!(guard.lock().unwrap().is_none());
    drop(namespace.freeze_operation().await.unwrap());
}

pub(super) async fn commit_state(
    repository: &Arc<RepositoryContext>,
    state: Arc<State>,
    parent: Hash,
) -> Hash {
    let branch = [239; 16].into();
    let mut metadata = lore_revision::metadata::Metadata::new();
    metadata.set_branch(branch).unwrap();
    lore_revision::commit::commit_in_memory_revision(
        repository.clone(),
        repository.try_write_token().unwrap(),
        state,
        metadata,
        parent,
        branch,
    )
    .await
    .unwrap()
}

pub(super) async fn add_file(
    repository: &Arc<RepositoryContext>,
    state: &State,
    parent: u32,
    name: &str,
    bytes: &[u8],
) -> u32 {
    let address = lore_revision::immutable::write_borrowed(
        repository.clone(),
        [240; 16].into(),
        bytes,
        lore_storage::options::WriteOptions::default().with_fixed_size_chunk(3),
    )
    .await
    .unwrap();
    let file = state
        .node_add(
            repository.clone(),
            parent,
            RevisionNode {
                flags: NodeFlags::File.bits(),
                size: bytes.len() as u64,
                address,
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
            file,
            NodeFlags::StagedAdd,
            NodeFlags::DirtyAdd,
        )
        .await
        .unwrap();
    file
}

#[tokio::test]
async fn local_readonly_target_proof_reads_the_selected_pin_without_source_gate_fallback() {
    let (_owner, guard, admission) = admitted().await;
    let execution = super::working_mount::execution();
    let (backend, target) = LORE_CONTEXT
        .scope(execution.clone(), async {
            let repository = repository().await;
            let source = State::new();
            add_file(&repository, &source, 0, "file", b"original bytes").await;
            let source = commit_state(&repository, source, Hash::default()).await;
            let target = State::new();
            add_file(&repository, &target, 0, "file", b"selected target bytes").await;
            let target = commit_state(&repository, target, source).await;
            (
                Arc::new(RevisionBackend::new(repository, source).await.unwrap()),
                Address::zero_context_hash(target),
            )
        })
        .await;
    let root = backend.root();
    let source_gate = LocalCheckoutFactory::new(admission, execution)
        .unwrap()
        .bind(backend.clone())
        .unwrap();
    let namespace = Arc::new(
        lore_vfs::namespace::Namespace::new(
            backend.clone(),
            root.clone(),
            source_gate.clone(),
            Default::default(),
        )
        .unwrap(),
    );
    let before = backend.checkpoint_state().await.unwrap();
    let plan = lore_vfs::transition_prepare::prepare(
        namespace.clone(),
        root.key,
        target,
        Default::default(),
        Default::default(),
    )
    .await
    .unwrap();
    let view = plan.prepare_target_view().await.unwrap();
    let capability = view.backend();
    let target_root = view.root().unwrap();
    let file = capability.lookup(&target_root.key, b"file").await.unwrap();
    assert_eq!(
        capability.read(&file.key, 0, 64).await.unwrap(),
        b"selected target bytes"
    );
    assert_eq!(
        capability.revalidate(&file.key, true).await,
        Err(Error::ReadOnly)
    );
    assert!(
        source_gate
            .authorize(&file.key, Access::Read)
            .await
            .is_err()
    );
    assert!(
        source_gate
            .authorize(&file.key, Access::Write)
            .await
            .is_err()
    );
    source_gate
        .authorize(&root.key, Access::Write)
        .await
        .unwrap();
    assert_eq!(namespace.getattr(1).await.unwrap().authority, root.key);
    assert_eq!(backend.checkpoint_state().await.unwrap(), before);
    drop(guard);
    assert_eq!(
        capability.read(&file.key, 0, 64).await,
        Err(Error::PermissionDenied)
    );
    drop(capability);
    drop(view);
    drop(namespace.freeze_operation().await.unwrap());
}

struct AbsentProofProbe {
    inner: Arc<dyn AccessGate>,
    missing: Address,
    seen: AtomicBool,
}
impl AccessGate for AbsentProofProbe {
    fn authorize_candidate_edit<'a>(&'a self, edit: &'a CandidateEdit) -> BackendFuture<'a, ()> {
        self.inner.authorize_candidate_edit(edit)
    }
    fn authorize<'a>(&'a self, key: &'a NodeKey, access: Access) -> BackendFuture<'a, ()> {
        self.inner.authorize(key, access)
    }
    fn authorize_selection<'a>(&'a self, selection: &'a TargetSelection) -> BackendFuture<'a, ()> {
        self.inner.authorize_selection(selection)
    }
    fn authorize_target_head<'a>(
        &'a self,
        proof: &'a ProvenTargetMetadata,
    ) -> BackendFuture<'a, ()> {
        Box::pin(async move {
            self.inner.authorize_target_head(proof).await?;
            if proof.head().revision == self.missing && proof.root().is_none() {
                self.seen.store(true, Ordering::Release);
                let source = proof.selection().original_scope();
                for key in [
                    source,
                    NodeKey {
                        revision: self.missing,
                        node: 0,
                        ..source
                    },
                    NodeKey {
                        revision: self.missing,
                        node: 0,
                        exposure: source.exposure + 1,
                        ..source
                    },
                ] {
                    assert!(
                        self.inner
                            .authorize_target_node(proof, &key, Access::Read)
                            .await
                            .is_err(),
                        "absence proof cannot grant an ancestor or sibling read"
                    );
                    assert!(
                        self.inner
                            .authorize_target_node(proof, &key, Access::Write)
                            .await
                            .is_err()
                    );
                }
            }
            Ok(())
        })
    }
    fn authorize_target_node<'a>(
        &'a self,
        proof: &'a ProvenTargetMetadata,
        key: &'a NodeKey,
        access: Access,
    ) -> BackendFuture<'a, ()> {
        self.inner.authorize_target_node(proof, key, access)
    }
}

#[tokio::test]
async fn local_absent_target_head_has_no_ancestor_or_repeated_exposure_node_authority() {
    let (_owner, _guard, admission) = admitted().await;
    let execution = super::working_mount::execution();
    let (backend, missing) = LORE_CONTEXT
        .scope(execution.clone(), async {
            let parent = repository().await;
            let child = parent.to_link_context(RepositoryId::from([241; 16])).await;
            let source = State::new();
            let selected = source
                .node_add(
                    child.clone(),
                    0,
                    RevisionNode {
                        name_hash: lore_storage::hash::hash_string("selected"),
                        ..Default::default()
                    },
                    "selected",
                )
                .await
                .unwrap();
            source
                .node_mark_staged(
                    child.clone(),
                    selected,
                    NodeFlags::StagedAdd,
                    NodeFlags::DirtyAdd,
                )
                .await
                .unwrap();
            add_file(&child, &source, selected, "file", b"child bytes").await;
            let pin = commit_state(&child, source, Hash::default()).await;
            let missing = commit_state(&child, State::new(), pin).await;
            let source = State::new();
            for name in ["left", "right"] {
                let link = source
                    .node_add(
                        parent.clone(),
                        0,
                        RevisionNode {
                            flags: NodeFlags::Link.bits(),
                            child: selected,
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
                source
                    .node_mark_staged(
                        parent.clone(),
                        link,
                        NodeFlags::StagedAdd,
                        NodeFlags::DirtyAdd,
                    )
                    .await
                    .unwrap();
            }
            let parent_pin = commit_state(&parent, source, Hash::default()).await;
            (
                Arc::new(RevisionBackend::new(parent, parent_pin).await.unwrap()),
                Address::zero_context_hash(missing),
            )
        })
        .await;
    let root = backend.root();
    let gate = Arc::new(AbsentProofProbe {
        inner: LocalCheckoutFactory::new(admission, execution)
            .unwrap()
            .bind(backend.clone())
            .unwrap(),
        missing,
        seen: AtomicBool::new(false),
    });
    let namespace = Arc::new(
        lore_vfs::namespace::Namespace::new(backend, root, gate.clone(), Default::default())
            .unwrap(),
    );
    let left = namespace.lookup(1, b"left").await.unwrap();
    let right = namespace.lookup(1, b"right").await.unwrap();
    assert_ne!(left.authority.exposure, right.authority.exposure);
    let plan = lore_vfs::transition_prepare::prepare(
        namespace.clone(),
        left.authority,
        missing,
        Default::default(),
        Default::default(),
    )
    .await
    .unwrap();
    assert!(plan.intent().target().root.is_none());
    plan.authorize_candidate_edits().await.unwrap();
    let view = plan.prepare_target_view().await.unwrap();
    assert!(view.root().is_none());
    assert!(gate.seen.load(Ordering::Acquire));
    assert!(
        view.backend()
            .read(
                &NodeKey {
                    revision: missing,
                    node: 0,
                    ..left.authority
                },
                0,
                1
            )
            .await
            .is_err()
    );
    drop(view);
    let complete = lore_vfs::test_util::prepare_complete_policy(
        namespace.clone(),
        left.authority,
        missing,
        Default::default(),
        Default::default(),
    )
    .await
    .unwrap();
    complete
        .policy()
        .authorize(&left.authority, Access::Read)
        .await
        .unwrap();
    for exposure in [left.authority.exposure, right.authority.exposure] {
        let absent = NodeKey {
            revision: missing,
            node: 0,
            exposure,
            ..left.authority
        };
        assert!(complete.backend().revalidate(&absent, false).await.is_err());
        assert!(
            complete
                .policy()
                .authorize(&absent, Access::Read)
                .await
                .is_err()
        );
        assert!(
            complete
                .policy()
                .authorize(&absent, Access::Write)
                .await
                .is_err()
        );
    }
    drop(namespace.freeze_operation().await.unwrap());
}

struct RevokingTarget {
    inner: Arc<dyn AccessGate>,
    revoke: Arc<AtomicBool>,
    at_node: bool,
}
impl AccessGate for RevokingTarget {
    fn authorize<'a>(&'a self, key: &'a NodeKey, access: Access) -> BackendFuture<'a, ()> {
        self.inner.authorize(key, access)
    }
    fn authorize_selection<'a>(&'a self, selection: &'a TargetSelection) -> BackendFuture<'a, ()> {
        self.inner.authorize_selection(selection)
    }
    fn authorize_target_head<'a>(
        &'a self,
        proof: &'a ProvenTargetMetadata,
    ) -> BackendFuture<'a, ()> {
        if !self.at_node {
            self.revoke.store(true, Ordering::Release);
        }
        self.inner.authorize_target_head(proof)
    }
    fn authorize_target_node<'a>(
        &'a self,
        proof: &'a ProvenTargetMetadata,
        key: &'a NodeKey,
        access: Access,
    ) -> BackendFuture<'a, ()> {
        if self.at_node {
            self.revoke.store(true, Ordering::Release);
        }
        self.inner.authorize_target_node(proof, key, access)
    }
}

#[tokio::test]
async fn run_revoked_during_target_head_or_node_validation_cannot_authorize_success() {
    for at_node in [false, true] {
        let (_owner, guard, admission) = admitted().await;
        let guard = Arc::new(Mutex::new(Some(guard)));
        let checked_guard = guard.clone();
        let revoke = Arc::new(AtomicBool::new(false));
        let checked_revoke = revoke.clone();
        let execution = super::working_mount::execution();
        let backend = linked_backend_with_check(
            execution.clone(),
            Arc::new(move |_repository| {
                if checked_revoke.load(Ordering::Acquire) {
                    checked_guard.lock().unwrap().take();
                }
                true
            }),
        )
        .await;
        let root = backend.root();
        let gate = Arc::new(RevokingTarget {
            inner: LocalCheckoutFactory::new(admission, execution)
                .unwrap()
                .bind(backend.clone())
                .unwrap(),
            revoke,
            at_node,
        });
        let namespace = Arc::new(
            lore_vfs::namespace::Namespace::new(backend, root, gate, Default::default()).unwrap(),
        );
        let child = namespace.lookup(1, b"child").await.unwrap();
        let result = lore_vfs::transition_prepare::prepare(
            namespace.clone(),
            child.authority,
            Address::default(),
            Default::default(),
            Default::default(),
        )
        .await;
        assert!(matches!(
            result,
            Err(lore_vfs::transition_prepare::Error::Backend(
                Error::PermissionDenied
            ))
        ));
        assert!(guard.lock().unwrap().is_none());
        drop(namespace.freeze_operation().await.unwrap());
    }
}
