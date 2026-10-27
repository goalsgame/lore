// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use lore_base::runtime::LORE_CONTEXT;
use lore_base::types::RepositoryId;
use lore_revision::interface::ExecutionContext;
use lore_revision::interface::LoreArray;
use lore_revision::interface::LoreGlobalArgs;
use lore_revision::interface::LoreString;
use lore_revision::relay::EventDispatcher;
use lore_revision::repository::RepositoryContextCreationArgs;
use lore_revision::repository::RepositoryPaths;
use lore_revision::repository::RepositoryWriteToken;

use super::*;
use crate::core::BackendFuture;
use crate::core::Error;
use crate::overlay::Access;
use crate::overlay::AccessGate;
use crate::overlay::SparseStore;

struct ResumeExports(Arc<exports::ExportPause>);
impl Drop for ResumeExports {
    fn drop(&mut self) {
        self.0.resume();
    }
}
struct ResumePublisher(Arc<crate::checkpoint_owner::PublishPause>);
impl Drop for ResumePublisher {
    fn drop(&mut self) {
        self.0.resume();
    }
}

#[tokio::test]
async fn cancelling_provider_cannot_thaw_an_admitted_checkpoint_publisher() {
    LORE_CONTEXT
        .scope(execution(), async {
            let fixture = fixture_options(Limits::default(), false, false, 0, true).await;
            write_through_model(&fixture, 1, b"file", b"durable despite cancelled callback").await;
            stage(&fixture, "file").await;
            let model = fixture.model.as_ref().unwrap();
            let owner = model.checkpoint_owner();
            let pause = owner.test_pause();
            let _resume = ResumePublisher(pause.clone());
            let commit = lore_revision::commit::commit_boxed(
                fixture.repository.clone(),
                &fixture.token,
                lore_revision::commit::CommitOptions::new("cancelled handoff".into()),
            );
            tokio::pin!(commit);
            tokio::select! {
                _ = pause.started.notified() => {},
                result = &mut commit => panic!("publisher did not block: {result:?}"),
            }
            fixture.provider.cancel_operations();
            assert!(commit.await.is_err());
            fixture.provider.drain_operations().await.unwrap();
            assert!(model.namespace().freeze_operation().await.is_err());
            let shutdown = model.shutdown();
            tokio::pin!(shutdown);
            assert!(
                tokio::time::timeout(std::time::Duration::from_millis(20), &mut shutdown)
                    .await
                    .is_err()
            );
            pause.resume();
            tokio::time::timeout(std::time::Duration::from_secs(5), shutdown)
                .await
                .unwrap()
                .unwrap();
            drop(model.namespace().freeze_operation().await.unwrap());
            assert!(!owner.test_snapshot().unwrap().unwrap().baselines.is_empty());
            fixture.provider.drain_exports().await.unwrap();
        })
        .await;
}

#[tokio::test]
async fn bound_export_sources_and_actual_cleanup_retain_working_storage_root() {
    LORE_CONTEXT
        .scope(execution(), async {
            use crate::working_storage::WorkingStorage;
            let directory = crate::digest_index::tests::Directory::new();
            std::fs::create_dir(&directory.0).unwrap();
            let target = directory.0.join("mount");
            std::fs::create_dir(&target).unwrap();
            let backing = directory.0.join("storage");
            let instance = lore_revision::instance::InstanceId::generate();
            let storage = WorkingStorage::open(
                &backing,
                &target,
                [91; 16].into(),
                instance,
                Default::default(),
            )
            .unwrap();
            let session = storage.begin_session().unwrap();
            let fixture = fixture_factory(
                Limits::default(),
                true,
                false,
                0,
                true,
                Some(session.factory()),
            )
            .await;
            let operation = fixture.provider.begin_operation().await.unwrap();
            let source = operation
                .content_source(&"vendor/content".parse().unwrap())
                .await
                .unwrap();
            let exported = source.file_path().unwrap().to_owned();
            let (handle, _) = source.open_once().await.unwrap();
            operation.abort();
            drop(operation);
            let pause = fixture.provider.inner.exports.test_pause();
            let _resume = ResumeExports(pause.clone());
            let Fixture {
                provider,
                repository,
                token,
                model,
                _directory: retained_exports,
                ..
            } = fixture;
            drop(model);
            drop(provider);
            drop(repository);
            drop(token);
            drop(session);
            drop(storage);
            let busy = || {
                matches!(
                    WorkingStorage::open(
                        &backing,
                        &target,
                        [91; 16].into(),
                        instance,
                        Default::default()
                    ),
                    Err(crate::working_storage::Error::Busy)
                )
            };
            assert!(busy(), "issued source retains the backing root");
            drop(source);
            assert!(
                busy(),
                "opened handle independently retains the backing root"
            );
            drop(handle);
            tokio::time::timeout(std::time::Duration::from_secs(5), pause.started.notified())
                .await
                .unwrap();
            assert!(
                busy(),
                "actual cleanup retains the backing root after every outer owner drops"
            );
            pause.resume();
            let start = std::time::Instant::now();
            let reopened = loop {
                match WorkingStorage::open(
                    &backing,
                    &target,
                    [91; 16].into(),
                    instance,
                    Default::default(),
                ) {
                    Ok(storage) => break storage,
                    Err(crate::working_storage::Error::Busy) => {
                        assert!(start.elapsed() < std::time::Duration::from_secs(5));
                        tokio::task::yield_now().await;
                    }
                    Err(error) => panic!("unexpected reopen: {error:?}"),
                }
            };
            assert!(!exported.exists());
            drop(reopened);
            drop(retained_exports);
        })
        .await;
}

#[test]
fn abandoned_export_creation_owns_backing_storage_after_runtime_drop() {
    abandoned_export_job_keeps_root_after_runtime_drop(false);
}
#[test]
fn abandoned_export_write_owns_backing_storage_after_runtime_drop() {
    abandoned_export_job_keeps_root_after_runtime_drop(true);
}
fn abandoned_export_job_keeps_root_after_runtime_drop(write: bool) {
    use crate::working_storage::WorkingStorage;
    let directory = crate::digest_index::tests::Directory::new();
    std::fs::create_dir(&directory.0).unwrap();
    let target = directory.0.join("mount");
    std::fs::create_dir(&target).unwrap();
    let backing = directory.0.join("storage");
    let instance = lore_revision::instance::InstanceId::generate();
    let storage = WorkingStorage::open(
        &backing,
        &target,
        [91; 16].into(),
        instance,
        Default::default(),
    )
    .unwrap();
    let session = storage.begin_session().unwrap();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let fixture = runtime.block_on(LORE_CONTEXT.scope(execution(), async {
        let fixture = fixture_factory(
            Limits::default(),
            false,
            false,
            0,
            true,
            Some(session.factory()),
        )
        .await;
        if write {
            write_through_model(&fixture, 1, b"file", b"owned write").await;
        }
        fixture
    }));
    let pause = if write {
        fixture.provider.inner.exports.test_pause_write()
    } else {
        fixture.provider.inner.exports.test_pause()
    };
    let _resume = ResumeExports(pause.clone());
    runtime.block_on(async {
        if write {
            let operation = fixture.provider.begin_operation().await.unwrap();
            let path: RelativePath = "file".parse().unwrap();
            {
                let export = operation.content_source(&path);
                tokio::pin!(export);
                tokio::select! {
                    _ = pause.started.notified() => {},
                    result = &mut export => panic!("write job did not block: {}", result.is_ok()),
                }
            }
            operation.abort();
        } else {
            let operation = fixture.provider.begin_operation();
            tokio::pin!(operation);
            tokio::select! {
                _ = pause.started.notified() => {},
                result = &mut operation => panic!("directory job did not block: {}", result.is_ok()),
            }
        }
    });
    let Fixture {
        provider,
        repository,
        token,
        model,
        _directory: retained_exports,
        ..
    } = fixture;
    drop(model);
    drop(provider);
    drop(repository);
    drop(token);
    drop(session);
    drop(storage);
    drop(runtime);
    assert!(matches!(
        WorkingStorage::open(
            &backing,
            &target,
            [91; 16].into(),
            instance,
            Default::default()
        ),
        Err(crate::working_storage::Error::Busy)
    ));
    pause.resume();
    let start = std::time::Instant::now();
    let reopened = loop {
        match WorkingStorage::open(
            &backing,
            &target,
            [91; 16].into(),
            instance,
            Default::default(),
        ) {
            Ok(storage) => break storage,
            Err(crate::working_storage::Error::Busy) => {
                assert!(start.elapsed() < std::time::Duration::from_secs(5));
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
            Err(error) => panic!("unexpected reopen: {error:?}"),
        }
    };
    assert_eq!(
        std::fs::read_dir(retained_exports.0.join("exports"))
            .unwrap()
            .count(),
        1
    );
    drop(reopened);
}

#[tokio::test]
async fn kernel_request_close_preserves_admitted_provider_commit_durability() {
    LORE_CONTEXT
        .scope(execution(), async {
            let fixture = fixture_options(Limits::default(), false, false, 0, true).await;
            let inode = write_through_model(&fixture, 1, b"file", b"after unmount").await;
            let model = fixture.model.as_ref().unwrap();
            model.close_requests();
            model.drain_requests().await.unwrap();
            assert!(model.getattr(inode).await.is_err());
            stage(&fixture, "file").await;
            lore_revision::commit::commit_boxed(
                fixture.repository.clone(),
                &fixture.token,
                lore_revision::commit::CommitOptions::new("after requests closed".into()),
            )
            .await
            .unwrap();
            assert!(
                model
                    .checkpoint_owner()
                    .test_snapshot()
                    .unwrap()
                    .unwrap()
                    .files[&inode]
                    .receipt
                    .is_some()
            );
            fixture.provider.close_admission();
            fixture.provider.drain_exports().await.unwrap();
            model.shutdown().await.unwrap();
        })
        .await;
}

#[tokio::test]
async fn closed_admission_preserves_admitted_reads_and_drains_retained_source_handles() {
    LORE_CONTEXT
        .scope(execution(), async {
            let fixture = fixture(Limits::default()).await;
            write(&fixture, "file", b"leased bytes").await;
            let operation = fixture.provider.begin_operation().await.unwrap();
            let path: RelativePath = "file".parse().unwrap();
            let source = operation.content_source(&path).await.unwrap();
            let (handle, _) = source.open_once().await.unwrap();
            fixture.provider.close_admission();
            assert!(fixture.provider.begin_operation().await.is_err());
            assert!(
                fixture
                    .provider
                    .read_metadata_file(&path, 32)
                    .await
                    .is_err()
            );
            assert!(operation.file_info(&path).await.is_ok());
            operation.finalize().await.unwrap();
            fixture.provider.drain_operations().await.unwrap();
            let drain = fixture.provider.drain_exports();
            let other_drain = fixture.provider.drain_exports();
            tokio::pin!(drain);
            tokio::pin!(other_drain);
            assert!(
                tokio::time::timeout(std::time::Duration::from_millis(20), &mut drain)
                    .await
                    .is_err()
            );
            assert!(
                tokio::time::timeout(std::time::Duration::from_millis(20), &mut other_drain)
                    .await
                    .is_err()
            );
            drop(source);
            assert!(
                tokio::time::timeout(std::time::Duration::from_millis(20), &mut drain)
                    .await
                    .is_err()
            );
            drop(handle);
            tokio::time::timeout(std::time::Duration::from_secs(5), async {
                let (first, second) = tokio::join!(drain, other_drain);
                first?;
                second
            })
            .await
            .unwrap()
            .unwrap();
            assert!(operation.file_info(&path).await.is_err());
        })
        .await;
}

#[tokio::test]
async fn cleanup_spawn_failure_is_reported_without_deleting_exports_or_dirty_bytes() {
    failed_export_cleanup_case(true).await;
}

#[tokio::test]
async fn aggregate_retained_directories_are_bounded_including_empty_cancelled_operations() {
    LORE_CONTEXT
        .scope(execution(), async {
            let fixture = fixture(Limits::default()).await;
            let pause = fixture.provider.inner.exports.test_pause_cleanup();
            let _resume = ResumeExports(pause.clone());
            let mut retained = 0;
            for _ in 0..128 {
                let operation = match fixture.provider.begin_operation().await {
                    Ok(operation) => operation,
                    Err(_) => break,
                };
                operation.abort();
                retained += 1;
            }
            assert_eq!(retained, 64);
            drop(
                fixture
                    .provider
                    .namespace()
                    .freeze_operation()
                    .await
                    .unwrap(),
            );
            let drain = fixture.provider.drain_exports();
            tokio::pin!(drain);
            assert!(
                tokio::time::timeout(std::time::Duration::from_millis(20), &mut drain)
                    .await
                    .is_err()
            );
            pause.resume();
            tokio::time::timeout(std::time::Duration::from_secs(5), drain)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                std::fs::read_dir(fixture._directory.0.join("exports"))
                    .unwrap()
                    .count(),
                1
            );
        })
        .await;
}
#[tokio::test]
async fn actual_cleanup_permission_failure_is_reported_and_preserves_retained_exports() {
    failed_export_cleanup_case(false).await;
}
async fn failed_export_cleanup_case(spawn: bool) {
    use std::os::unix::fs::PermissionsExt;
    // SAFETY: geteuid only reads the process identity.
    if !spawn && unsafe { libc::geteuid() } == 0 {
        return;
    }
    struct RestoreMode(PathBuf);
    impl Drop for RestoreMode {
        fn drop(&mut self) {
            let _ = std::fs::set_permissions(&self.0, std::fs::Permissions::from_mode(0o700));
        }
    }
    LORE_CONTEXT
        .scope(execution(), async {
            let fixture = fixture(Limits::default()).await;
            let (_, file) = write(&fixture, "file", b"kept dirty bytes").await;
            let operation = fixture.provider.begin_operation().await.unwrap();
            let source = operation
                .content_source(&"file".parse().unwrap())
                .await
                .unwrap();
            let exported = source.file_path().unwrap().to_owned();
            operation.abort();
            let exports = fixture._directory.0.join("exports");
            let protected = if spawn {
                exports
            } else {
                exported.parent().unwrap().to_owned()
            };
            let _mode = RestoreMode(protected.clone());
            if spawn {
                fixture.provider.inner.exports.test_fail_cleanup_spawn();
            } else {
                std::fs::set_permissions(&protected, std::fs::Permissions::from_mode(0o500))
                    .unwrap();
            }
            drop(source);
            let (first, second) = tokio::time::timeout(std::time::Duration::from_secs(5), async {
                tokio::join!(
                    fixture.provider.drain_exports(),
                    fixture.provider.drain_exports()
                )
            })
            .await
            .unwrap();
            assert!(first.is_err());
            assert!(second.is_err());
            assert!(
                exported.is_file(),
                "failed cleanup leaves its copy for inspection"
            );
            let reader = file.open(Access::Read).await.unwrap();
            assert_eq!(
                file.read(reader, 0, 100).await.unwrap(),
                b"kept dirty bytes"
            );
            file.release(reader).await.unwrap();
        })
        .await;
}

#[tokio::test]
async fn cancellation_drains_blocked_reads_and_releases_inactive_phase_ownership() {
    blocked_provider_read_case(true).await;
}
#[tokio::test]
async fn explicit_phase_abort_cancels_blocked_reads_without_closing_publication() {
    blocked_provider_read_case(false).await;
}
async fn blocked_provider_read_case(cancel_all: bool) {
    LORE_CONTEXT
        .scope(execution(), async {
            let fixture = fixture(Limits::default()).await;
            let (_, file) = write(&fixture, "file", b"kept").await;
            let operation = fixture.provider.begin_operation().await.unwrap();
            let path: RelativePath = "file".parse().unwrap();
            fixture.gate.block.store(true, Ordering::SeqCst);
            let read = operation.file_info(&path);
            tokio::pin!(read);
            tokio::select! {
                _ = fixture.gate.entered.notified() => {},
                result = &mut read => panic!("read did not block: {result:?}"),
            }
            if cancel_all {
                fixture.provider.cancel_operations();
            } else {
                operation.abort();
            }
            assert!(read.await.is_err());
            tokio::time::timeout(
                std::time::Duration::from_secs(5),
                fixture.provider.drain_operations(),
            )
            .await
            .unwrap()
            .unwrap();
            tokio::time::timeout(
                std::time::Duration::from_secs(5),
                fixture.provider.drain_exports(),
            )
            .await
            .unwrap()
            .unwrap();
            fixture.gate.block.store(false, Ordering::SeqCst);
            let writer = file.open(Access::Write).await.unwrap();
            file.release(writer).await.unwrap();
            assert!(operation.file_info(&path).await.is_err());
        })
        .await;
}

#[tokio::test]
async fn closing_preparing_operation_fences_admission_and_drains_actual_directory_job() {
    LORE_CONTEXT.scope(execution(), async {
        let fixture = fixture(Limits::default()).await;
        let pause = fixture.provider.inner.exports.test_pause();
        let _resume = ResumeExports(pause.clone());
        let operation = fixture.provider.begin_operation();
        tokio::pin!(operation);
        tokio::select! {
            _ = pause.started.notified() => {},
            result = &mut operation => panic!("directory job did not block: {}", result.is_ok()),
        }
        fixture.provider.close_admission();
        assert!(operation.await.is_err());
        fixture.provider.drain_operations().await.unwrap();
        drop(fixture.provider.namespace().freeze_operation().await.unwrap());
        let drain = fixture.provider.drain_exports();
        tokio::pin!(drain);
        assert!(tokio::time::timeout(std::time::Duration::from_millis(20), &mut drain).await.is_err());
        pause.resume();
        tokio::time::timeout(std::time::Duration::from_secs(5), drain).await.unwrap().unwrap();
    }).await;
}

#[tokio::test]
async fn metadata_reads_use_current_overlay_bounds_and_authorization_without_exports() {
    LORE_CONTEXT
        .scope(execution(), async {
            let fixture = fixture(Limits {
                max_export_files: 1,
                max_export_bytes: 0,
            })
            .await;
            let path: RelativePath = ".loreignore".parse().unwrap();
            assert!(matches!(
                fixture.provider.read_metadata_file(&path, 32).await,
                Err(FsError::FileNotFound(_))
            ));
            let (_, file) = write(&fixture, ".loreignore", b"secret\n").await;
            assert_eq!(
                fixture
                    .provider
                    .read_metadata_file(&path, 32)
                    .await
                    .unwrap(),
                b"secret\n"
            );
            assert!(fixture.provider.read_metadata_file(&path, 6).await.is_err());
            assert!(
                fixture
                    .provider
                    .read_metadata_file(&path, lore_revision::repository::MAX_FILTER_FILE_BYTES + 1)
                    .await
                    .is_err()
            );
            let writer = file.open(Access::Write).await.unwrap();
            assert!(
                fixture
                    .provider
                    .read_metadata_file(&path, 32)
                    .await
                    .is_err()
            );
            file.release(writer).await.unwrap();
            fixture.gate.deny.store(true, Ordering::SeqCst);
            let denied = fixture.provider.read_metadata_file(&path, 32).await;
            assert!(denied.is_err());
            assert!(!matches!(denied, Err(FsError::FileNotFound(_))));
            fixture.gate.deny.store(false, Ordering::SeqCst);
            write(&fixture, ".loreignore", b"changed\n").await;
            assert_eq!(
                fixture
                    .provider
                    .read_metadata_file(&path, 32)
                    .await
                    .unwrap(),
                b"changed\n"
            );
            assert_eq!(
                std::fs::read_dir(fixture._directory.0.join("exports"))
                    .unwrap()
                    .count(),
                1
            );
            let writer = file.open(Access::Write).await.unwrap();
            file.release(writer).await.unwrap();
        })
        .await;
}

#[tokio::test]
async fn cancelled_metadata_read_releases_freeze_without_detached_exports() {
    LORE_CONTEXT
        .scope(execution(), async {
            let fixture = fixture(Limits::default()).await;
            let (_, file) = write(&fixture, ".loreignore", b"secret\n").await;
            let path: RelativePath = ".loreignore".parse().unwrap();
            fixture.gate.block.store(true, Ordering::SeqCst);
            {
                let read = fixture.provider.read_metadata_file(&path, 32);
                tokio::pin!(read);
                tokio::select! {
                    _ = fixture.gate.entered.notified() => {},
                    result = &mut read => panic!("unexpected completion: {result:?}"),
                }
            }
            fixture.gate.block.store(false, Ordering::SeqCst);
            let writer = file.open(Access::Write).await.unwrap();
            file.release(writer).await.unwrap();
            assert_eq!(
                fixture
                    .provider
                    .read_metadata_file(&path, 32)
                    .await
                    .unwrap(),
                b"secret\n"
            );
        })
        .await;
}

#[derive(Default)]
struct Store(tokio::sync::Mutex<Vec<u8>>);
impl SparseStore for Store {
    fn read<'a>(&'a self, offset: u64, size: usize) -> BackendFuture<'a, Vec<u8>> {
        Box::pin(async move {
            Ok(self.0.lock().await[offset as usize..offset as usize + size].to_vec())
        })
    }
    fn write<'a>(&'a self, offset: u64, bytes: &'a [u8]) -> BackendFuture<'a, usize> {
        Box::pin(async move {
            let mut data = self.0.lock().await;
            data.resize(offset as usize + bytes.len(), 0);
            data[offset as usize..].copy_from_slice(bytes);
            Ok(bytes.len())
        })
    }
}
struct FixtureStores;
impl crate::linux_working::StoreFactory for FixtureStores {
    fn create(&self) -> BackendFuture<'_, Arc<dyn SparseStore>> {
        Box::pin(async { Ok(Arc::new(Store::default()) as Arc<dyn SparseStore>) })
    }
}
struct Gate {
    deny: AtomicBool,
    read_calls: AtomicUsize,
    deny_on_call: AtomicUsize,
    block: AtomicBool,
    entered: tokio::sync::Notify,
    resume: tokio::sync::Notify,
}
impl Default for Gate {
    fn default() -> Self {
        Self {
            deny: AtomicBool::new(false),
            read_calls: AtomicUsize::new(0),
            deny_on_call: AtomicUsize::new(usize::MAX),
            block: AtomicBool::new(false),
            entered: tokio::sync::Notify::new(),
            resume: tokio::sync::Notify::new(),
        }
    }
}
impl AccessGate for Gate {
    fn authorize<'a>(&'a self, _: &'a NodeKey, access: Access) -> BackendFuture<'a, ()> {
        Box::pin(async move {
            if self.block.load(Ordering::SeqCst) {
                self.entered.notify_one();
                self.resume.notified().await;
            }
            if self.deny.load(Ordering::SeqCst)
                || (access == Access::Read
                    && self.read_calls.fetch_add(1, Ordering::SeqCst)
                        >= self.deny_on_call.load(Ordering::SeqCst))
            {
                Err(Error::PermissionDenied)
            } else {
                Ok(())
            }
        })
    }
}
fn execution() -> Arc<ExecutionContext> {
    Arc::new(ExecutionContext::new_client_with_user_id(
        LoreGlobalArgs::default(),
        EventDispatcher::no_dispatch(),
        "provider-test".into(),
    ))
}
struct Fixture {
    provider: Arc<WorkingProvider>,
    repository: Arc<RepositoryContext>,
    token: RepositoryWriteToken,
    root: PathBuf,
    _directory: crate::digest_index::tests::Directory,
    gate: Arc<Gate>,
    model: Option<Arc<crate::linux_working::Model>>,
}
async fn fixture(limits: Limits) -> Fixture {
    fixture_inner(limits, false).await
}
async fn fixture_inner(limits: Limits, linked: bool) -> Fixture {
    fixture_config(limits, linked, true).await
}
async fn empty_staged_node(
    state: &State,
    repository: &Arc<RepositoryContext>,
    parent: u32,
    name: &str,
    directory: bool,
    mode: u16,
) -> u32 {
    use lore_revision::node::NodeFlags;
    let id = state
        .node_add(
            repository.clone(),
            parent,
            RevisionNode {
                flags: if directory {
                    NodeFlags::empty().bits()
                } else {
                    NodeFlags::File.bits()
                },
                mode,
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
            id,
            NodeFlags::StagedAdd,
            NodeFlags::DirtyAdd,
        )
        .await
        .unwrap();
    id
}
async fn pinned_intermediate(
    repository: &Arc<RepositoryContext>,
    child: &Arc<RepositoryContext>,
    pin: Hash,
    branch: lore_revision::lore::BranchId,
    token: &RepositoryWriteToken,
) -> Hash {
    use lore_revision::node::NodeFlags;
    let state = State::new();
    let mount = state
        .node_add(
            repository.clone(),
            0,
            RevisionNode {
                flags: NodeFlags::Link.bits(),
                address: Address {
                    context: child.id.into(),
                    hash: pin,
                },
                name_hash: lore_storage::hash::hash_string("inner"),
                ..Default::default()
            },
            "inner",
        )
        .await
        .unwrap();
    state
        .link_add(
            repository.clone(),
            child.id,
            branch,
            pin,
            mount,
            lore_revision::link::LinkFlags::NoFlags,
        )
        .await
        .unwrap();
    state
        .node_mark_staged(
            repository.clone(),
            mount,
            NodeFlags::StagedAdd,
            NodeFlags::DirtyAdd,
        )
        .await
        .unwrap();
    let mut metadata = lore_revision::metadata::Metadata::new();
    metadata.set_branch(branch).unwrap();
    lore_revision::commit::commit_in_memory_revision(
        repository.clone(),
        token,
        state,
        metadata,
        Hash::default(),
        branch,
    )
    .await
    .unwrap()
}
async fn fixture_config(limits: Limits, linked: bool, index: bool) -> Fixture {
    fixture_variants(limits, linked, index, 0).await
}
async fn fixture_variants(limits: Limits, linked: bool, index: bool, variant: u8) -> Fixture {
    fixture_options(limits, linked, index, variant, false).await
}
async fn fixture_options(
    limits: Limits,
    linked: bool,
    index: bool,
    variant: u8,
    durable: bool,
) -> Fixture {
    fixture_factory(limits, linked, index, variant, durable, None).await
}
async fn fixture_factory(
    limits: Limits,
    linked: bool,
    index: bool,
    variant: u8,
    durable: bool,
    factory: Option<Arc<dyn crate::linux_working::StoreFactory>>,
) -> Fixture {
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
    let id = RepositoryId::from([91; 16]);
    let branch = [92; 16].into();
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
    let pinned = if linked {
        use lore_revision::node::NodeFlags;
        let child_id = RepositoryId::from([93; 16]);
        let child = original.to_link_context(child_id).await;
        let child_state = State::new();
        let file = child_state
            .node_add(
                child.clone(),
                0,
                RevisionNode {
                    flags: NodeFlags::File.bits(),
                    name_hash: lore_storage::hash::hash_string("content"),
                    ..Default::default()
                },
                "content",
            )
            .await
            .unwrap();
        child_state
            .node_mark_staged(
                child.clone(),
                file,
                NodeFlags::StagedAdd,
                NodeFlags::DirtyAdd,
            )
            .await
            .unwrap();
        let (primary_root, mirror_root) = if variant == 2 {
            let left = empty_staged_node(&child_state, &child, 0, "left", true, 0).await;
            let right = empty_staged_node(&child_state, &child, 0, "right", true, 0).await;
            empty_staged_node(&child_state, &child, left, "content", false, 0).await;
            empty_staged_node(&child_state, &child, right, "content", false, 0).await;
            (left, right)
        } else {
            (0, 0)
        };
        let branch_a = if variant == 0 {
            branch
        } else {
            [102; 16].into()
        };
        let branch_b = if variant == 1 {
            [103; 16].into()
        } else {
            branch_a
        };
        let child_pin = if variant == 0 {
            child_state.serialize(child.clone(), &token).await.unwrap()
        } else {
            let mut metadata = lore_revision::metadata::Metadata::new();
            metadata.set_branch(branch_a).unwrap();
            lore_revision::commit::commit_in_memory_revision(
                child.clone(),
                &token,
                child_state,
                metadata,
                Hash::default(),
                branch_a,
            )
            .await
            .unwrap()
        };
        let mirror_pin = if variant == 1 {
            let other = State::new();
            empty_staged_node(
                &other,
                &child,
                0,
                "content",
                false,
                NodeFileMode::Executable.bits(),
            )
            .await;
            let mut metadata = lore_revision::metadata::Metadata::new();
            metadata.set_branch(branch_b).unwrap();
            lore_revision::commit::commit_in_memory_revision(
                child.clone(),
                &token,
                other,
                metadata,
                Hash::default(),
                branch_b,
            )
            .await
            .unwrap()
        } else {
            child_pin
        };
        let (primary_id, primary_pin, primary_root, mirror_id, mirror_pin, mirror_root) =
            if variant == 1 {
                let a = original
                    .to_link_context(RepositoryId::from([104; 16]))
                    .await;
                let b = original
                    .to_link_context(RepositoryId::from([105; 16]))
                    .await;
                let a_pin = pinned_intermediate(&a, &child, child_pin, branch_a, &token).await;
                let b_pin = pinned_intermediate(&b, &child, mirror_pin, branch_b, &token).await;
                (a.id, a_pin, 0, b.id, b_pin, 0)
            } else {
                (
                    child_id,
                    child_pin,
                    primary_root,
                    child_id,
                    mirror_pin,
                    mirror_root,
                )
            };
        let state = State::new();
        let mount = state
            .node_add(
                original.clone(),
                0,
                RevisionNode {
                    flags: NodeFlags::Link.bits(),
                    child: primary_root,
                    address: Address {
                        context: primary_id.into(),
                        hash: primary_pin,
                    },
                    name_hash: lore_storage::hash::hash_string("vendor"),
                    ..Default::default()
                },
                "vendor",
            )
            .await
            .unwrap();
        state
            .link_add(
                original.clone(),
                primary_id,
                branch_a,
                primary_pin,
                mount,
                lore_revision::link::LinkFlags::NoFlags,
            )
            .await
            .unwrap();
        state
            .node_mark_staged(
                original.clone(),
                mount,
                NodeFlags::StagedAdd,
                NodeFlags::DirtyAdd,
            )
            .await
            .unwrap();
        if variant != 0 {
            let mirror = state
                .node_add(
                    original.clone(),
                    0,
                    RevisionNode {
                        flags: NodeFlags::Link.bits(),
                        child: mirror_root,
                        address: Address {
                            context: mirror_id.into(),
                            hash: mirror_pin,
                        },
                        name_hash: lore_storage::hash::hash_string("mirror"),
                        ..Default::default()
                    },
                    "mirror",
                )
                .await
                .unwrap();
            state
                .link_add(
                    original.clone(),
                    mirror_id,
                    branch_b,
                    mirror_pin,
                    mirror,
                    lore_revision::link::LinkFlags::NoFlags,
                )
                .await
                .unwrap();
            state
                .node_mark_staged(
                    original.clone(),
                    mirror,
                    NodeFlags::StagedAdd,
                    NodeFlags::DirtyAdd,
                )
                .await
                .unwrap();
        }
        let pin = state.serialize(original.clone(), &token).await.unwrap();
        lore_revision::instance::store_staged_anchor(&original, pin)
            .await
            .unwrap();
        pin
    } else {
        Hash::default()
    };
    let backend = Arc::new(RevisionBackend::new(original, pinned).await.unwrap());
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
    let model = if durable {
        Some(
            crate::linux_working::Model::new(
                namespace.clone(),
                factory.unwrap_or_else(|| Arc::new(FixtureStores)),
                Arc::new(crate::storage_budget::StorageBudget::new(1 << 20)),
                crate::recovery::Recovery::open(directory.0.join("recovery"), Default::default())
                    .unwrap(),
                crate::linux_working::Limits {
                    max_checkpoint_data_bytes: 1 << 20,
                    ..Default::default()
                },
            )
            .await
            .unwrap(),
        )
    } else {
        None
    };
    let mut provider =
        WorkingProvider::new(namespace, backend, directory.0.join("exports"), limits).unwrap();
    if index {
        provider = provider.with_digest_index(DigestIndex::new(directory.0.join("digests")));
    }
    if let Some(model) = &model {
        provider = provider
            .with_checkpoint_owner(model.checkpoint_owner())
            .unwrap();
    }
    let provider = Arc::new(provider);
    let repository = make(Some(provider.clone()));
    Fixture {
        provider,
        repository,
        token,
        root,
        _directory: directory,
        gate,
        model,
    }
}
async fn write(
    fixture: &Fixture,
    name: &str,
    bytes: &[u8],
) -> (u64, Arc<crate::overlay::OverlayFile>) {
    if let Some(model) = &fixture.model {
        let (handle, inode) = match model.namespace().lookup(1, name.as_bytes()).await {
            Ok(entry) => (
                model
                    .open(
                        entry.inode,
                        crate::linux_working::OpenFlags {
                            write: true,
                            append: false,
                            truncate: true,
                        },
                    )
                    .await
                    .unwrap(),
                entry.inode,
            ),
            Err(NamespaceError::Backend(Error::NotFound)) => {
                let (handle, entry) = model
                    .create(
                        1,
                        name.as_bytes(),
                        0o644,
                        crate::linux_working::OpenFlags {
                            write: true,
                            append: false,
                            truncate: false,
                        },
                    )
                    .await
                    .unwrap();
                (handle, entry.entry.inode)
            }
            Err(error) => panic!("lookup: {error:?}"),
        };
        model.write(handle, 0, bytes).await.unwrap();
        model.release(handle).await.unwrap();
        return (
            inode,
            model
                .namespace()
                .materialized_file(inode)
                .await
                .unwrap()
                .unwrap(),
        );
    }
    let ns = fixture.provider.namespace();
    let entry = match ns.lookup(1, name.as_bytes()).await {
        Ok(entry) => entry,
        Err(NamespaceError::Backend(Error::NotFound)) => ns
            .create_file(
                1,
                name.as_bytes(),
                0o644,
                Arc::new(Store::default()),
                Default::default(),
            )
            .await
            .unwrap(),
        Err(error) => panic!("lookup: {error:?}"),
    };
    let file = ns
        .file(entry.inode, Arc::new(Store::default()), Default::default())
        .await
        .unwrap();
    let writer = file.open(Access::Write).await.unwrap();
    file.truncate(writer, 0).await.unwrap();
    file.write(writer, 0, bytes).await.unwrap();
    file.release(writer).await.unwrap();
    (entry.inode, file)
}
async fn stage(fixture: &Fixture, name: &str) {
    lore_revision::file::stage::stage(
        fixture.repository.clone(),
        &fixture.token,
        LoreArray::from_vec(vec![LoreString::from(&fixture.root.join(name))]),
        lore_revision::stage::StageOptions {
            scan: true,
            ..Default::default()
        },
    )
    .await
    .unwrap();
}
#[tokio::test]
async fn normal_stage_partial_commit_preserves_unselected_edits_and_generation_receipts() {
    LORE_CONTEXT
        .scope(execution(), async {
            let fixture = fixture(Limits::default()).await;
            let (selected, file) = write(&fixture, "selected", b"committed").await;
            let (unselected, bystander) = write(&fixture, "bystander", b"not committed").await;
            assert!(!fixture.root.join("selected").exists());
            stage(&fixture, "selected").await;
            let revision = lore_revision::commit::commit_boxed(
                fixture.repository.clone(),
                &fixture.token,
                lore_revision::commit::CommitOptions::new("partial virtual commit".into()),
            )
            .await
            .unwrap();
            let state = State::deserialize(fixture.repository.clone(), revision)
                .await
                .unwrap();
            let node = state
                .find_relative_node_link(fixture.repository.clone(), 0, "selected")
                .await
                .unwrap();
            let committed = state
                .node(fixture.repository.clone(), node.node)
                .await
                .unwrap();
            assert!(
                state
                    .find_relative_node_link(fixture.repository.clone(), 0, "bystander")
                    .await
                    .is_err()
            );
            let backend = RevisionBackend::new(fixture.repository.clone(), revision)
                .await
                .unwrap()
                .with_digest_index(DigestIndex::new(fixture._directory.0.join("digests")));
            let selected_node = backend
                .lookup(&backend.root().key, b"selected")
                .await
                .unwrap();
            assert_eq!(
                backend.read(&selected_node.key, 0, 100).await.unwrap(),
                b"committed"
            );
            let expected = ring::digest::digest(&ring::digest::SHA256, b"committed");
            assert_eq!(
                backend
                    .digest(&selected_node.key)
                    .await
                    .unwrap()
                    .unwrap()
                    .as_slice(),
                expected.as_ref()
            );
            let receipts = fixture.provider.receipts().await.unwrap();
            assert_eq!(receipts.len(), 1);
            assert_eq!(receipts[0].inode, selected);
            assert_ne!(receipts[0].inode, unselected);
            assert_eq!(receipts[0].address, committed.address);
            let reader = bystander.open(Access::Read).await.unwrap();
            assert_eq!(
                bystander.read(reader, 0, 100).await.unwrap(),
                b"not committed"
            );
            bystander.release(reader).await.unwrap();
            // The verified ingestion hash describes retained overlay bytes.
            assert_eq!(
                file.digest().await.unwrap().unwrap().as_slice(),
                expected.as_ref()
            );
            write(&fixture, "selected", b"edited!!!").await;
            let operation = fixture.provider.begin_operation().await.unwrap();
            let path = "selected".parse().unwrap();
            assert!(matches!(
                operation
                    .file_holds_content(
                        fixture.repository.clone(),
                        &path,
                        committed.address,
                        committed.size,
                        &Default::default()
                    )
                    .await
                    .unwrap(),
                NodeComparison::Differs
            ));
            operation.finalize().await.unwrap();
            assert_eq!(file.digest().await.unwrap(), None);
            stage(&fixture, "").await;
            let next = lore_revision::commit::commit_boxed(
                fixture.repository.clone(),
                &fixture.token,
                lore_revision::commit::CommitOptions::new("full virtual scan".into()),
            )
            .await
            .unwrap();
            let next = State::deserialize(fixture.repository.clone(), next)
                .await
                .unwrap();
            assert!(
                next.find_relative_node_link(fixture.repository.clone(), 0, "bystander")
                    .await
                    .is_ok()
            );
            assert_eq!(fixture.provider.receipts().await.unwrap().len(), 2);
        })
        .await;
}
#[tokio::test]
async fn operation_exports_are_bounded_private_lazy_and_read_authorized() {
    LORE_CONTEXT
        .scope(execution(), async {
            let fixture = fixture(Limits {
                max_export_files: 1,
                max_export_bytes: 4,
            })
            .await;
            let (_, file) = write(&fixture, "first", b"four").await;
            write(&fixture, "second", b"next").await;
            let operation = fixture.provider.begin_operation().await.unwrap();
            assert_eq!(file.open(Access::Write).await, Err(Error::Busy));
            assert!(
                operation
                    .content_source(&"first".parse().unwrap())
                    .await
                    .is_ok()
            );
            assert!(
                operation
                    .content_source(&"second".parse().unwrap())
                    .await
                    .is_err()
            );
            fixture.gate.deny.store(true, Ordering::SeqCst);
            assert!(
                operation
                    .content_source(&"first".parse().unwrap())
                    .await
                    .is_err()
            );
            fixture.gate.deny.store(false, Ordering::SeqCst);
            operation.abort();
            assert!(
                operation
                    .file_info(&"first".parse().unwrap())
                    .await
                    .is_err()
            );
            let writer = file.open(Access::Write).await.unwrap();
            file.release(writer).await.unwrap();
            drop(operation);
            fixture.provider.inner.exports.drain().await.unwrap();
            let operation = fixture.provider.begin_operation().await.unwrap();
            assert!(
                operation
                    .content_source(&"second".parse().unwrap())
                    .await
                    .is_ok()
            );
            operation.finalize().await.unwrap();
        })
        .await;
}
#[tokio::test]
async fn active_writers_failed_commands_and_cancelled_operations_preserve_data_and_thaw() {
    LORE_CONTEXT
        .scope(execution(), async {
            let fixture = fixture(Limits::default()).await;
            let (_, file) = write(&fixture, "file", b"kept").await;
            let writer = file.open(Access::Write).await.unwrap();
            assert!(fixture.provider.begin_operation().await.is_err());
            file.release(writer).await.unwrap();
            with_operation(fixture.provider.clone(), async |operation| {
                operation
                    .write_file(&"file".parse().unwrap(), Bytes::from_static(b"refused"))
                    .await
            })
            .await
            .unwrap_err();
            let reader = file.open(Access::Read).await.unwrap();
            assert_eq!(file.read(reader, 0, 100).await.unwrap(), b"kept");
            file.release(reader).await.unwrap();
            let operation = fixture.provider.begin_operation().await.unwrap();
            let clone = operation.clone();
            operation.abort();
            let later = fixture.provider.begin_operation().await.unwrap();
            assert!(
                clone
                    .content_source(&"file".parse().unwrap())
                    .await
                    .is_err()
            );
            drop(clone);
            drop(operation);
            assert_eq!(file.open(Access::Write).await, Err(Error::Busy));
            later.finalize().await.unwrap();
            let writer = file.open(Access::Write).await.unwrap();
            file.release(writer).await.unwrap();
        })
        .await;
}

async fn edit_child(fixture: &Fixture) -> (u64, Arc<crate::overlay::OverlayFile>) {
    let ns = fixture.provider.namespace();
    let mount = ns.lookup(1, b"vendor").await.unwrap();
    let entry = ns.lookup(mount.inode, b"content").await.unwrap();
    if let Some(model) = &fixture.model {
        let handle = model
            .open(
                entry.inode,
                crate::linux_working::OpenFlags {
                    write: true,
                    append: false,
                    truncate: false,
                },
            )
            .await
            .unwrap();
        model.write(handle, 0, b"child bytes").await.unwrap();
        model.release(handle).await.unwrap();
        return (
            entry.inode,
            ns.materialized_file(entry.inode).await.unwrap().unwrap(),
        );
    }
    let file = ns
        .file(entry.inode, Arc::new(Store::default()), Default::default())
        .await
        .unwrap();
    let writer = file.open(Access::Write).await.unwrap();
    file.write(writer, 0, b"child bytes").await.unwrap();
    file.release(writer).await.unwrap();
    (entry.inode, file)
}
#[tokio::test]
async fn linked_parent_commit_validates_child_ownership_and_preserves_live_overlay() {
    LORE_CONTEXT
        .scope(execution(), async {
            let fixture = fixture_inner(Limits::default(), true).await;
            let (inode, file) = edit_child(&fixture).await;
            let revision = lore_revision::commit::commit_boxed(
                fixture.repository.clone(),
                &fixture.token,
                lore_revision::commit::CommitOptions::new("linked virtual commit".into()),
            )
            .await
            .unwrap();
            let root = State::deserialize(fixture.repository.clone(), revision)
                .await
                .unwrap();
            let pin = root
                .find_node(fixture.repository.clone(), "vendor")
                .await
                .unwrap()
                .address
                .hash;
            let receipts = fixture.provider.receipts().await.unwrap();
            assert_eq!(receipts.len(), 1);
            assert_eq!(receipts[0].inode, inode);
            assert_eq!(
                receipts[0].authority.repository,
                RepositoryId::from([93; 16])
            );
            assert_eq!(receipts[0].committed_revision, pin);
            assert_eq!(
                file.digest().await.unwrap().unwrap().as_slice(),
                ring::digest::digest(&ring::digest::SHA256, b"child bytes").as_ref()
            );
            let reader = file.open(Access::Read).await.unwrap();
            assert_eq!(file.read(reader, 0, 100).await.unwrap(), b"child bytes");
            file.release(reader).await.unwrap();
            let backend = RevisionBackend::new(fixture.repository.clone(), revision)
                .await
                .unwrap()
                .with_digest_index(DigestIndex::new(fixture._directory.0.join("digests")));
            let vendor = backend
                .lookup(&backend.root().key, b"vendor")
                .await
                .unwrap();
            let content = backend.lookup(&vendor.key, b"content").await.unwrap();
            assert_eq!(
                backend.read(&content.key, 0, 100).await.unwrap(),
                b"child bytes"
            );
            assert!(backend.digest(&content.key).await.unwrap().is_some());
            let operation = fixture.provider.begin_operation().await.unwrap();
            assert!(
                operation
                    .file_holds_content(
                        fixture.repository.clone(),
                        &"vendor/content".parse().unwrap(),
                        receipts[0].address,
                        receipts[0].size,
                        &Default::default()
                    )
                    .await
                    .is_err()
            );
            operation.finalize().await.unwrap();
        })
        .await;
}
async fn failed_parent_receipt_case(durable: bool) {
    LORE_CONTEXT
        .scope(execution(), async {
            use lore_revision::node::NodeFlags;
            let fixture = fixture_options(Limits::default(), true, true, 0, durable).await;
            let (inode, file) = edit_child(&fixture).await;
            let (_, parent_dirty) = write(&fixture, "parent-dirty", b"preserved").await;
            let child = fixture
                .repository
                .to_link_context(RepositoryId::from([93; 16]))
                .await;
            let actual = Arc::new(Mutex::new(None));
            let outcome = with_operation(fixture.provider.clone(), async |operation| {
                let path = "vendor/content".parse().unwrap();
                let source = operation.content_source(&path).await?;
                let (address, size) = lore_revision::immutable::write_from_file_with_tracker(
                    child.clone(),
                    &source,
                    [94; 16].into(),
                    Default::default(),
                    None,
                )
                .await
                .map_err(|error| FsError::internal(error.to_string()))?;
                operation.stored(&path, address, size, 0);
                let state = State::new();
                let node = state
                    .node_add(
                        child.clone(),
                        0,
                        RevisionNode {
                            flags: NodeFlags::File.bits(),
                            address,
                            size,
                            name_hash: lore_storage::hash::hash_string("content"),
                            ..Default::default()
                        },
                        "content",
                    )
                    .await
                    .unwrap();
                state
                    .node_mark_staged(
                        child.clone(),
                        node,
                        NodeFlags::StagedAdd,
                        NodeFlags::DirtyAdd,
                    )
                    .await
                    .unwrap();
                let branch = [92; 16].into();
                let mut metadata = lore_revision::metadata::Metadata::new();
                metadata.set_branch(branch).unwrap();
                let revision = lore_revision::commit::commit_in_memory_revision(
                    child.clone(),
                    &fixture.token,
                    state,
                    metadata,
                    Hash::default(),
                    branch,
                )
                .await
                .map_err(|error| FsError::internal(format!("child commit: {error:?}")))?;
                child.flush(true).await.unwrap();
                let committed = State::deserialize(child.clone(), revision).await.unwrap();
                let origin = fixture
                    .provider
                    .namespace()
                    .lookup(1, b"vendor")
                    .await
                    .unwrap()
                    .authority;
                let source_state = State::deserialize(child.clone(), origin.revision.hash)
                    .await
                    .unwrap();
                operation
                    .committed_from_source(
                        lore_revision::state::NodeMapping {
                            repository: child.clone(),
                            state: source_state,
                            path: "vendor".parse().unwrap(),
                            node: origin.node,
                        },
                        committed,
                    )
                    .await?;
                if let Some(model) = &fixture.model {
                    let checkpoint = model.checkpoint_owner().test_snapshot().unwrap().unwrap();
                    let saved = checkpoint
                        .baselines
                        .values()
                        .find(|head| head.scope.repository == child.id)
                        .unwrap();
                    assert_eq!(saved.head.revision.hash, revision);
                    assert!(saved.parent_projection.as_ref().unwrap().target.is_none());
                    assert!(
                        checkpoint.files.values().any(|file| file
                            .extents
                            .iter()
                            .any(|extent| extent.data == b"preserved")),
                        "unrelated edits were not checkpointed before child acknowledgment"
                    );
                }
                *actual.lock().unwrap() = Some((revision, address, size));
                Err::<(), _>(FsError::internal(
                    "parent failed after durable child commit",
                ))
            })
            .await;
            assert!(outcome.is_err());
            let (revision, address, size) = actual.lock().unwrap().unwrap();
            let receipts = fixture.provider.receipts().await.unwrap();
            assert_eq!(receipts.len(), 1);
            assert_eq!(receipts[0].inode, inode);
            assert_eq!(receipts[0].committed_revision, revision);
            let baselines = fixture.provider.namespace().held_baselines().await.unwrap();
            let child_head = baselines
                .iter()
                .find(|head| head.scope.repository == child.id)
                .unwrap();
            assert_eq!(child_head.head.revision.hash, revision);
            let projection = child_head.parent_projection.as_ref().unwrap();
            assert_eq!(projection.head.hash, Hash::default());
            assert!(
                projection.target.is_none(),
                "the parent has not committed its staged link"
            );

            assert_eq!(file.digest().await.unwrap(), None); // no ingestion hash was supplied
            assert_eq!(parent_dirty.digest().await.unwrap(), None);
            let snapshot = fixture
                .provider
                .namespace()
                .snapshot(1024 * 1024)
                .await
                .unwrap();
            let root = snapshot
                .nodes
                .iter()
                .find(|node| node.entry.inode == 1)
                .unwrap()
                .entry
                .base
                .clone()
                .unwrap();
            let fresh = Arc::new(
                RevisionBackend::new(fixture.repository.clone(), root.key.revision.hash)
                    .await
                    .unwrap(),
            );
            let stores = snapshot
                .files
                .keys()
                .map(|&id| (id, Arc::new(Store::default()) as Arc<dyn SparseStore>))
                .collect();
            let restored = Arc::new(
                Namespace::restore(
                    fresh,
                    root,
                    fixture.gate.clone(),
                    snapshot,
                    stores,
                    Default::default(),
                    Default::default(),
                )
                .await
                .unwrap(),
            );
            let recovered = WorkingProvider::from_restored_namespace(
                restored,
                fixture._directory.0.join("child-restored-exports"),
                Limits::default(),
            )
            .await
            .unwrap();
            assert_eq!(recovered.receipts().await.unwrap(), receipts);
            let reader = file.open(Access::Read).await.unwrap();
            assert_eq!(file.read(reader, 0, 100).await.unwrap(), b"child bytes");
            file.release(reader).await.unwrap();
            let reader = parent_dirty.open(Access::Read).await.unwrap();
            assert_eq!(
                parent_dirty.read(reader, 0, 100).await.unwrap(),
                b"preserved"
            );
            parent_dirty.release(reader).await.unwrap();
            let operation = fixture.provider.begin_operation().await.unwrap();
            assert!(matches!(
                operation
                    .file_holds_content(
                        child,
                        &"vendor/content".parse().unwrap(),
                        address,
                        size,
                        &Default::default()
                    )
                    .await
                    .unwrap(),
                NodeComparison::Matches
            ));
            operation.finalize().await.unwrap();
        })
        .await;
}

#[tokio::test]
async fn durable_child_receipt_survives_failed_parent_work_without_discarding_other_edits() {
    failed_parent_receipt_case(false).await;
}
#[tokio::test]
async fn configured_child_handoff_is_checkpointed_before_parent_failure() {
    failed_parent_receipt_case(true).await;
}

#[tokio::test]
async fn denied_stat_or_directory_listing_never_stages_inaccessible_files_as_deleted() {
    LORE_CONTEXT
        .scope(execution(), async {
            for deny_on_call in [0, 2] {
                let fixture = fixture(Limits::default()).await;
                write(&fixture, "tracked", b"kept").await;
                stage(&fixture, "tracked").await;
                let revision = lore_revision::commit::commit_boxed(
                    fixture.repository.clone(),
                    &fixture.token,
                    lore_revision::commit::CommitOptions::new("tracked".into()),
                )
                .await
                .unwrap();
                let state = State::deserialize(fixture.repository.clone(), revision)
                    .await
                    .unwrap();
                let tracked = state
                    .find_relative_node_link(fixture.repository.clone(), 0, "tracked")
                    .await
                    .unwrap()
                    .node;
                let result = with_operation(fixture.provider.clone(), async |operation| {
                    fixture.gate.read_calls.store(0, Ordering::SeqCst);
                    fixture
                        .gate
                        .deny_on_call
                        .store(deny_on_call, Ordering::SeqCst);
                    let tree = || FilesystemDiffTree {
                        repository: fixture.repository.clone(),
                        state: state.clone(),
                    };
                    let stream = lore_revision::state::diff_filesystem(
                        &operation,
                        tree(),
                        tree(),
                        None,
                        lore_revision::filter::FilterMode::Full,
                        FilesystemDiffIntent::Stage(StageIntent::default()),
                        Arc::new(Vec::new()),
                    )
                    .await?;
                    stream.collect().await
                })
                .await;
                assert!(
                    result.is_err(),
                    "denied call {deny_on_call} was treated as an absent path"
                );
                fixture
                    .gate
                    .deny_on_call
                    .store(usize::MAX, Ordering::SeqCst);
                let node = state
                    .node(fixture.repository.clone(), tracked)
                    .await
                    .unwrap();
                assert!(!node.is_staged_delete());
                assert!(!node.is_dirty());
                assert!(fixture.provider.begin_operation().await.is_ok());
            }
        })
        .await;
}

#[tokio::test]
async fn committed_hashes_survive_coherent_restart_and_disappear_before_rewrite() {
    LORE_CONTEXT
        .scope(execution(), async {
            let fixture = fixture_config(Limits::default(), false, false).await;
            let bytes = vec![b'x'; 300_123];
            let (inode, file) = write(&fixture, "fragmented", &bytes).await;
            let writer = file.open(Access::Write).await.unwrap();
            file.write(writer, 17, b"patch").await.unwrap();
            file.release(writer).await.unwrap();
            let mut bytes = bytes;
            bytes[17..22].copy_from_slice(b"patch");
            let (empty, empty_file) = write(&fixture, "empty", b"").await;
            let old_handle = file.open(Access::Read).await.unwrap();
            let before = fixture
                .provider
                .namespace()
                .snapshot(1024 * 1024)
                .await
                .unwrap();
            let mut recovery = crate::recovery::Recovery::open(
                fixture._directory.0.join("recovery"),
                Default::default(),
            )
            .unwrap();
            recovery.publish(&before).unwrap();
            stage(&fixture, "").await;
            lore_revision::commit::commit_boxed(
                fixture.repository.clone(),
                &fixture.token,
                lore_revision::commit::CommitOptions::new("verified hash restart".into()),
            )
            .await
            .unwrap();
            let sha: [u8; 32] = ring::digest::digest(&ring::digest::SHA256, &bytes)
                .as_ref()
                .try_into()
                .unwrap();
            let empty_sha: [u8; 32] = ring::digest::digest(&ring::digest::SHA256, b"")
                .as_ref()
                .try_into()
                .unwrap();
            assert_eq!(file.digest().await.unwrap(), Some(sha));
            assert_eq!(empty_file.digest().await.unwrap(), Some(empty_sha));
            assert_eq!(file.read(old_handle, 0, 100).await.unwrap(), bytes[..100]);
            file.release(old_handle).await.unwrap();
            let snapshot = fixture
                .provider
                .namespace()
                .snapshot(1024 * 1024)
                .await
                .unwrap();
            assert!(snapshot.generation > before.generation);
            recovery.publish(&snapshot).unwrap();
            drop(recovery);
            let recovery = crate::recovery::Recovery::open(
                fixture._directory.0.join("recovery"),
                Default::default(),
            )
            .unwrap();
            let loaded = recovery.load().unwrap().unwrap();
            let root = loaded
                .nodes
                .iter()
                .find(|node| node.entry.inode == 1)
                .unwrap()
                .entry
                .base
                .clone()
                .unwrap();
            let backend = Arc::new(
                RevisionBackend::new(fixture.repository.clone(), root.key.revision.hash)
                    .await
                    .unwrap(),
            );
            for corruption in 0..4 {
                let mut invalid = loaded.clone();
                let receipt = invalid
                    .files
                    .get_mut(&inode)
                    .unwrap()
                    .receipt
                    .as_mut()
                    .unwrap();
                match corruption {
                    0 => receipt.proof.address.hash = [87; 32].into(),
                    1 => receipt.proof.node.key.node = u32::MAX,
                    2 => receipt.proof.node.key.exposure += 1,
                    _ => receipt.proof.node.key.repository = [88; 16].into(),
                }
                let stores = invalid
                    .files
                    .keys()
                    .map(|&inode| (inode, Arc::new(Store::default()) as Arc<dyn SparseStore>))
                    .collect();
                assert!(
                    Namespace::restore(
                        backend.clone(),
                        root.clone(),
                        fixture.gate.clone(),
                        invalid,
                        stores,
                        Default::default(),
                        Default::default()
                    )
                    .await
                    .is_err()
                );
            }
            let stores = loaded
                .files
                .keys()
                .map(|&inode| (inode, Arc::new(Store::default()) as Arc<dyn SparseStore>))
                .collect();
            let namespace = Arc::new(
                Namespace::restore(
                    backend,
                    root,
                    fixture.gate.clone(),
                    loaded.clone(),
                    stores,
                    Default::default(),
                    Default::default(),
                )
                .await
                .unwrap(),
            );
            let provider = WorkingProvider::from_restored_namespace(
                namespace.clone(),
                fixture._directory.0.join("restored-exports"),
                Limits::default(),
            )
            .await
            .unwrap();
            assert_eq!(provider.receipts().await.unwrap().len(), 2);
            assert_eq!(namespace.digest(inode).await.unwrap(), Some(sha));
            assert_eq!(namespace.digest(empty).await.unwrap(), Some(empty_sha));
            let restored = namespace
                .file(inode, Arc::new(Store::default()), Default::default())
                .await
                .unwrap();
            let writer = restored.open(Access::Write).await.unwrap();
            assert_eq!(namespace.digest(inode).await.unwrap(), None);
            restored.write(writer, 0, b"changed").await.unwrap();
            restored.release(writer).await.unwrap();
            assert_eq!(restored.digest().await.unwrap(), None);
            let mut obsolete = loaded.files[&inode].receipt.clone().unwrap();
            obsolete.generation = restored.metadata().await.unwrap().generation;
            let lease = namespace.freeze_operation().await.unwrap();
            let expired = lease.token();
            let file_guard = restored.checkpoint_guard().await;
            let mut adoption =
                Box::pin(namespace.adopt_receipts(expired, vec![obsolete.clone()], || Ok(())));
            tokio::select! {
                biased;
                result = &mut adoption => panic!("adoption bypassed locked file: {result:?}"),
                () = tokio::task::yield_now() => {}
            }
            // Cancellation releases the namespace lock without adopting or thawing
            // another operation; abort never waits for the held file lock.
            drop(adoption);
            drop(lease);
            drop(file_guard);
            let current_lease = namespace.freeze_operation().await.unwrap();
            assert!(
                namespace
                    .adopt_receipts(expired, vec![obsolete], || Ok(()))
                    .await
                    .is_err()
            );
            assert_eq!(restored.digest().await.unwrap(), None);
            drop(current_lease);
            let after = namespace.snapshot(1024 * 1024).await.unwrap();
            // A revoked owner cannot reuse checkpointed dirty data or its verified hash.
            fixture.gate.deny.store(true, Ordering::SeqCst);
            let root = after
                .nodes
                .iter()
                .find(|node| node.entry.inode == 1)
                .unwrap()
                .entry
                .base
                .clone()
                .unwrap();
            let backend = Arc::new(
                RevisionBackend::new(fixture.repository.clone(), root.key.revision.hash)
                    .await
                    .unwrap(),
            );
            let stores = after
                .files
                .keys()
                .map(|&inode| (inode, Arc::new(Store::default()) as Arc<dyn SparseStore>))
                .collect();
            assert!(
                Namespace::restore(
                    backend,
                    root,
                    fixture.gate.clone(),
                    after,
                    stores,
                    Default::default(),
                    Default::default()
                )
                .await
                .is_err()
            );
        })
        .await;
}

#[tokio::test]
async fn ignored_critical_receipt_failure_still_fails_successful_finalization() {
    LORE_CONTEXT
        .scope(execution(), async {
            let fixture = fixture(Limits::default()).await;
            let (_, file) = write(&fixture, "untouched", b"critical-failure-preserved").await;
            let outcome = with_operation(fixture.provider.clone(), async |operation| {
                operation.stored(
                    &"unexported".parse().unwrap(),
                    Address::zero_context_hash([85; 32].into()),
                    4,
                    0,
                );
                assert!(
                    operation
                        .committed_from_source(
                            lore_revision::state::NodeMapping::root(
                                fixture.repository.clone(),
                                State::new()
                            ),
                            State::new()
                        )
                        .await
                        .is_err()
                );
                Ok::<_, FsError>(())
            })
            .await;
            assert!(outcome.is_err());
            assert!(fixture.provider.receipts().await.unwrap().is_empty());
            assert_eq!(file.digest().await.unwrap(), None);
            let writer = file.open(Access::Write).await.unwrap();
            file.release(writer).await.unwrap();
        })
        .await;
}

#[tokio::test]
async fn pristine_receipt_survives_lazy_file_materialization_without_identity_change() {
    LORE_CONTEXT
        .scope(execution(), async {
            let fixture = fixture_inner(Limits::default(), true).await;
            let namespace = fixture.provider.namespace();
            let vendor = namespace.lookup(1, b"vendor").await.unwrap();
            let entry = namespace.lookup(vendor.inode, b"content").await.unwrap();
            let base = entry.base.clone().unwrap();
            let identity = namespace
                .backend()
                .unwrap()
                .content_identity(&entry.authority)
                .await
                .unwrap()
                .unwrap();
            let proof = crate::overlay::CommittedProof {
                node: base,
                address: identity.address,
            };
            namespace
                .backend()
                .unwrap()
                .validate_committed(&entry.authority, &proof)
                .await
                .unwrap();
            let hash = ring::digest::digest(&ring::digest::SHA256, b"")
                .as_ref()
                .try_into()
                .unwrap();
            let lease = namespace.freeze_operation().await.unwrap();
            namespace
                .adopt_receipts(
                    lease.token(),
                    vec![crate::overlay::Receipt {
                        inode: entry.inode,
                        generation: 0,
                        authority: entry.authority,
                        mode: 0o444,
                        proof,
                        sha256: Some(hash),
                    }],
                    || Ok(()),
                )
                .await
                .unwrap();
            drop(lease);
            let before = namespace.snapshot(1024).await.unwrap();
            assert!(before.files.is_empty());
            assert_eq!(namespace.digest(entry.inode).await.unwrap(), Some(hash));
            let file = namespace
                .file(entry.inode, Arc::new(Store::default()), Default::default())
                .await
                .unwrap();
            assert_eq!(file.digest().await.unwrap(), Some(hash));
            let after = namespace.snapshot(1024).await.unwrap();
            let mut recovery = crate::recovery::Recovery::open(
                fixture._directory.0.join("pristine-recovery"),
                Default::default(),
            )
            .unwrap();
            recovery.publish(&before).unwrap();
            recovery.publish(&after).unwrap();
            let writer = file.open(Access::Write).await.unwrap();
            assert_eq!(file.digest().await.unwrap(), None);
            file.release(writer).await.unwrap();
        })
        .await;
}

struct ProofBeforeWrites;
impl SparseStore for ProofBeforeWrites {
    fn read<'a>(&'a self, _offset: u64, _size: usize) -> BackendFuture<'a, Vec<u8>> {
        Box::pin(async { panic!("invalid baseline restored private data") })
    }
    fn write<'a>(&'a self, _offset: u64, _bytes: &'a [u8]) -> BackendFuture<'a, usize> {
        Box::pin(async { panic!("invalid baseline wrote private data before validation") })
    }
}

#[tokio::test]
async fn deletion_only_logical_head_survives_checkpoint_and_reauthorization() {
    LORE_CONTEXT
        .scope(execution(), async {
            let fixture = fixture(Limits::default()).await;
            write(&fixture, "tracked", b"payload").await;
            stage(&fixture, "tracked").await;
            let first = lore_revision::commit::commit_boxed(
                fixture.repository.clone(),
                &fixture.token,
                lore_revision::commit::CommitOptions::new("initial tracked".into()),
            )
            .await
            .unwrap();
            let namespace = fixture.provider.namespace();
            namespace.remove(1, b"tracked", false).await.unwrap();
            stage(&fixture, "").await;
            let deleted = lore_revision::commit::commit_boxed(
                fixture.repository.clone(),
                &fixture.token,
                lore_revision::commit::CommitOptions::new("deletion only".into()),
            )
            .await
            .unwrap();
            assert_ne!(first, deleted);
            let baselines = namespace.held_baselines().await.unwrap();
            assert_eq!(baselines.len(), 1);
            assert_eq!(baselines[0].head.revision.hash, deleted);
            assert_eq!(baselines[0].head.root, Some(0));
            assert_eq!(baselines[0].sequence, 2);
            let snapshot = namespace.snapshot(1 << 20).await.unwrap();
            let root = snapshot
                .nodes
                .iter()
                .find(|node| node.entry.inode == 1)
                .unwrap()
                .entry
                .base
                .clone()
                .unwrap();
            let mut recovery = crate::recovery::Recovery::open(
                fixture._directory.0.join("baseline-recovery"),
                Default::default(),
            )
            .unwrap();
            recovery.publish(&snapshot).unwrap();
            drop(recovery);
            let recovery = crate::recovery::Recovery::open(
                fixture._directory.0.join("baseline-recovery"),
                Default::default(),
            )
            .unwrap();
            let loaded = recovery.load().unwrap().unwrap();
            for corruption in 0..2 {
                let mut forged = loaded.clone();
                let baseline = forged.baselines.values_mut().next().unwrap();
                if corruption == 0 {
                    baseline.head.root = Some(u32::MAX);
                } else {
                    baseline.head.revision.hash = [79; 32].into();
                }
                let stores = forged
                    .files
                    .keys()
                    .map(|inode| (*inode, Arc::new(ProofBeforeWrites) as Arc<dyn SparseStore>))
                    .collect();
                assert!(
                    Namespace::restore(
                        namespace.backend().unwrap(),
                        root.clone(),
                        fixture.gate.clone(),
                        forged,
                        stores,
                        Default::default(),
                        Default::default()
                    )
                    .await
                    .is_err()
                );
            }
            let stores = loaded
                .files
                .keys()
                .map(|inode| (*inode, Arc::new(Store::default()) as Arc<dyn SparseStore>))
                .collect();
            let restored = Arc::new(
                Namespace::restore(
                    namespace.backend().unwrap(),
                    root.clone(),
                    fixture.gate.clone(),
                    loaded.clone(),
                    stores,
                    Default::default(),
                    Default::default(),
                )
                .await
                .unwrap(),
            );
            let provider = WorkingProvider::from_restored_namespace(
                restored.clone(),
                fixture._directory.0.join("baseline-restored-exports"),
                Limits::default(),
            )
            .await
            .unwrap();
            assert_eq!(
                provider.namespace().held_baselines().await.unwrap(),
                baselines
            );
            assert!(restored.lookup(1, b"tracked").await.is_err());
            fixture.gate.deny.store(true, Ordering::SeqCst);
            let stores = loaded
                .files
                .keys()
                .map(|inode| (*inode, Arc::new(Store::default()) as Arc<dyn SparseStore>))
                .collect();
            assert!(
                Namespace::restore(
                    namespace.backend().unwrap(),
                    root,
                    fixture.gate.clone(),
                    loaded,
                    stores,
                    Default::default(),
                    Default::default()
                )
                .await
                .is_err()
            );
        })
        .await;
}
#[tokio::test]
async fn scoped_notifications_are_idempotent_and_stale_heads_cannot_regress() {
    LORE_CONTEXT
        .scope(execution(), async {
            let fixture = fixture(Limits::default()).await;
            write(&fixture, "file", b"first").await;
            stage(&fixture, "file").await;
            let first = lore_revision::commit::commit_boxed(
                fixture.repository.clone(),
                &fixture.token,
                lore_revision::commit::CommitOptions::new("first head".into()),
            )
            .await
            .unwrap();
            let before = fixture
                .provider
                .namespace()
                .snapshot(1 << 20)
                .await
                .unwrap();
            let committed = State::deserialize(fixture.repository.clone(), first)
                .await
                .unwrap();
            with_operation(fixture.provider.clone(), async |operation| {
                for _ in 0..2 {
                    operation
                        .committed_from_source(
                            lore_revision::state::NodeMapping::root(
                                fixture.repository.clone(),
                                State::new(),
                            ),
                            committed.clone(),
                        )
                        .await?;
                }
                Ok::<(), FsError>(())
            })
            .await
            .unwrap();
            let after = fixture
                .provider
                .namespace()
                .snapshot(1 << 20)
                .await
                .unwrap();
            assert_eq!(before.baselines, after.baselines);
            assert_eq!(before.generation, after.generation);
            write(&fixture, "file", b"second").await;
            stage(&fixture, "file").await;
            let second = lore_revision::commit::commit_boxed(
                fixture.repository.clone(),
                &fixture.token,
                lore_revision::commit::CommitOptions::new("second head".into()),
            )
            .await
            .unwrap();
            assert_ne!(first, second);
            let before = fixture.provider.namespace().held_baselines().await.unwrap();
            let rejected = with_operation(fixture.provider.clone(), async |operation| {
                operation
                    .committed_from_source(
                        lore_revision::state::NodeMapping::root(
                            fixture.repository.clone(),
                            State::new(),
                        ),
                        committed,
                    )
                    .await
            })
            .await;
            assert!(rejected.is_err());
            assert_eq!(
                fixture.provider.namespace().held_baselines().await.unwrap(),
                before
            );
        })
        .await;
}

#[tokio::test]
async fn repeated_repository_exposures_keep_independent_pins_and_selected_subtrees() {
    LORE_CONTEXT
        .scope(execution(), async {
            for variant in [1, 2] {
                let fixture = fixture_variants(Limits::default(), true, false, variant).await;
                let namespace = fixture.provider.namespace();
                let mut vendor = namespace.lookup(1, b"vendor").await.unwrap();
                let mut mirror = namespace.lookup(1, b"mirror").await.unwrap();
                if variant == 1 {
                    vendor = namespace.lookup(vendor.inode, b"inner").await.unwrap();
                    mirror = namespace.lookup(mirror.inode, b"inner").await.unwrap();
                }
                assert_eq!(vendor.authority.repository, mirror.authority.repository);
                assert_ne!(vendor.authority.exposure, mirror.authority.exposure);
                if variant == 1 {
                    assert_ne!(vendor.authority.revision, mirror.authority.revision);
                } else {
                    assert_eq!(vendor.authority.revision, mirror.authority.revision);
                    assert_ne!(vendor.authority.node, mirror.authority.node);
                }
                let mirror_file = namespace.lookup(mirror.inode, b"content").await.unwrap();
                let source = namespace.lookup(vendor.inode, b"content").await.unwrap();
                let edited = namespace
                    .file(source.inode, Arc::new(Store::default()), Default::default())
                    .await
                    .unwrap();
                let handle = edited.open(Access::Write).await.unwrap();
                edited.write(handle, 0, b"child bytes").await.unwrap();
                edited.release(handle).await.unwrap();
                stage(
                    &fixture,
                    if variant == 1 {
                        "vendor/inner/content"
                    } else {
                        "vendor/content"
                    },
                )
                .await;
                let revision = lore_revision::commit::commit_boxed(
                    fixture.repository.clone(),
                    &fixture.token,
                    lore_revision::commit::CommitOptions::new("selected owning exposure".into()),
                )
                .await
                .unwrap();
                let baselines = namespace.held_baselines().await.unwrap();
                let a = baselines
                    .iter()
                    .find(|head| head.scope == vendor.authority)
                    .unwrap();
                let b = baselines
                    .iter()
                    .find(|head| head.scope == mirror.authority)
                    .unwrap();
                assert_ne!(a.head.revision, vendor.authority.revision);
                assert_eq!(b.head.revision, mirror.authority.revision);
                for child in [a, b] {
                    let projection = child.parent_projection.as_ref().unwrap();
                    let parent = baselines
                        .iter()
                        .find(|head| head.scope == projection.parent)
                        .unwrap();
                    assert_eq!(projection.head, parent.head.revision);
                }
                assert_eq!(
                    baselines
                        .iter()
                        .find(|head| head.scope.exposure == 0)
                        .unwrap()
                        .head
                        .revision
                        .hash,
                    revision
                );
                assert_eq!(
                    a.parent_projection
                        .as_ref()
                        .unwrap()
                        .target
                        .as_ref()
                        .unwrap()
                        .revision,
                    a.head.revision
                );
                assert_eq!(
                    b.parent_projection
                        .as_ref()
                        .unwrap()
                        .target
                        .as_ref()
                        .unwrap()
                        .revision,
                    b.head.revision
                );
                assert_eq!(edited.metadata().await.unwrap().size, 11);
                assert_eq!(
                    namespace
                        .getattr(mirror_file.inode)
                        .await
                        .unwrap()
                        .base
                        .unwrap()
                        .size,
                    0
                );
                assert!(
                    fixture
                        .provider
                        .receipts()
                        .await
                        .unwrap()
                        .iter()
                        .all(|receipt| receipt.authority.exposure == vendor.authority.exposure)
                );
            }
        })
        .await;
}

async fn write_through_model(fixture: &Fixture, parent: u64, name: &[u8], bytes: &[u8]) -> u64 {
    let model = fixture.model.as_ref().unwrap();
    let (handle, entry) = model
        .create(
            parent,
            name,
            0o644,
            crate::linux_working::OpenFlags {
                write: true,
                append: false,
                truncate: false,
            },
        )
        .await
        .unwrap();
    model.write(handle, 0, bytes).await.unwrap();
    model.release(handle).await.unwrap();
    entry.entry.inode
}
#[tokio::test]
async fn configured_commit_acknowledges_hash_and_baseline_without_manual_fsync() {
    LORE_CONTEXT
        .scope(execution(), async {
            let fixture = fixture_options(Limits::default(), false, false, 0, true).await;
            let inode = write_through_model(&fixture, 1, b"file", b"committed bytes").await;
            stage(&fixture, "file").await;
            let revision = lore_revision::commit::commit_boxed(
                fixture.repository.clone(),
                &fixture.token,
                lore_revision::commit::CommitOptions::new("checkpointed handoff".into()),
            )
            .await
            .unwrap();
            let owner = fixture.model.as_ref().unwrap().checkpoint_owner();
            let snapshot = owner.test_snapshot().unwrap().unwrap();
            let expected: [u8; 32] =
                ring::digest::digest(&ring::digest::SHA256, b"committed bytes")
                    .as_ref()
                    .try_into()
                    .unwrap();
            assert_eq!(
                snapshot.files[&inode].receipt.as_ref().unwrap().sha256,
                Some(expected)
            );
            assert_eq!(
                snapshot
                    .baselines
                    .values()
                    .next()
                    .unwrap()
                    .head
                    .revision
                    .hash,
                revision
            );
            let context = Arc::new(fixture.repository.without_command_locks());
            fixture.model.as_ref().unwrap().shutdown().await.unwrap();
            drop(owner);
            let Fixture {
                provider,
                repository,
                token,
                root: _root,
                _directory,
                gate,
                model,
            } = fixture;
            drop(model);
            drop(provider);
            drop(repository);
            drop(token);
            let recovery =
                crate::recovery::Recovery::open(_directory.0.join("recovery"), Default::default())
                    .unwrap();
            let original = recovery.original_root().unwrap().unwrap();
            assert_ne!(original.key.revision.hash, revision);
            assert_eq!(
                lore_revision::instance::load_current_anchor_boxed(&context)
                    .await
                    .unwrap()
                    .0,
                revision
            );
            let backend = Arc::new(
                RevisionBackend::new(context, original.key.revision.hash)
                    .await
                    .unwrap(),
            );
            assert_eq!(backend.root(), original);
            let restored = crate::linux_working::Model::restore(
                backend,
                original,
                gate,
                Default::default(),
                Arc::new(FixtureStores),
                Arc::new(crate::storage_budget::StorageBudget::new(1 << 20)),
                recovery,
                Default::default(),
            )
            .await
            .unwrap();
            assert_eq!(
                restored.namespace().digest(inode).await.unwrap(),
                Some(expected)
            );
            let handle = restored
                .open(
                    inode,
                    crate::linux_working::OpenFlags {
                        write: false,
                        append: false,
                        truncate: false,
                    },
                )
                .await
                .unwrap();
            assert_eq!(
                restored.read(handle, 0, 100).await.unwrap(),
                b"committed bytes"
            );
            restored.release(handle).await.unwrap();
            let handle = restored
                .open(
                    inode,
                    crate::linux_working::OpenFlags {
                        write: true,
                        append: false,
                        truncate: false,
                    },
                )
                .await
                .unwrap();
            assert_eq!(restored.namespace().digest(inode).await.unwrap(), None);
            restored.release(handle).await.unwrap();
            restored.shutdown().await.unwrap();
        })
        .await;
}
#[tokio::test]
async fn configured_publisher_rejects_a_different_namespace() {
    LORE_CONTEXT
        .scope(execution(), async {
            let a = fixture_options(Limits::default(), false, false, 0, true).await;
            let b = fixture(Limits::default()).await;
            assert!(
                b.provider
                    .as_ref()
                    .clone()
                    .with_checkpoint_owner(a.model.as_ref().unwrap().checkpoint_owner())
                    .is_err()
            );
            a.model.as_ref().unwrap().shutdown().await.unwrap();
        })
        .await;
}

#[tokio::test]
async fn configured_commit_never_reports_success_after_partial_or_uncertain_checkpoint() {
    LORE_CONTEXT
        .scope(execution(), async {
            for boundary in [
                crate::recovery::Boundary::Write,
                crate::recovery::Boundary::DirectorySync,
            ] {
                let fixture = fixture_options(Limits::default(), false, false, 0, true).await;
                write_through_model(&fixture, 1, b"file", b"committed but checkpoint failed").await;
                stage(&fixture, "file").await;
                let model = fixture.model.as_ref().unwrap();
                let owner = model.checkpoint_owner();
                owner.test_fault(Some(boundary));
                let result = lore_revision::commit::commit_boxed(
                    fixture.repository.clone(),
                    &fixture.token,
                    lore_revision::commit::CommitOptions::new("publication failure".into()),
                )
                .await;
                assert!(result.is_err());
                assert!(owner.is_poisoned());
                assert!(
                    !lore_revision::instance::load_current_anchor_boxed(&fixture.repository)
                        .await
                        .unwrap()
                        .0
                        .is_zero(),
                    "immutable commit unexpectedly disappeared"
                );
                assert!(
                    model
                        .create(
                            1,
                            b"blocked",
                            0o644,
                            crate::linux_working::OpenFlags {
                                write: true,
                                append: false,
                                truncate: false
                            }
                        )
                        .await
                        .is_err()
                );
                model.shutdown().await.unwrap();
            }
        })
        .await;
}

#[path = "tests/unsupported_transitions.rs"]
mod unsupported_transitions;

#[path = "tests/link_replacements.rs"]
mod link_replacements;

#[path = "tests/routing.rs"]
mod operation_routing;
