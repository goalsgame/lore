// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! The outer mutex checks deferred-destruction discipline, not Lore's actual
//! startup Record arbitration. Fixtures use real qualified metadata and stores.

#![allow(clippy::disallowed_methods)]

use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;

use lore_base::runtime::LORE_CONTEXT;
use lore_base::test_util::TempDir;
use lore_storage::Hash;
use lore_storage::MutableStore;
use lore_storage::Partition;
use lore_storage::local::immutable_store;
use lore_storage::local::mutable_store::LocalMutableStore;
use lore_storage::local::mutable_store::MutableStoreSettings;
use lore_storage::test_util::PausePoint;

use super::*;
use crate::fs::filesystem_provider::FilesystemProvider;
use crate::fs::filesystem_provider::FsError;
use crate::fs::filesystem_provider::InstanceOperationImpl;
use crate::fs::provider_registry::ProviderRegistry;
use crate::fs::provider_registry::ProviderResolver;
use crate::fs::provider_registry::{self};
use crate::instance::InstanceId;
use crate::interface::ExecutionContext;
use crate::interface::LoreGlobalArgs;
use crate::relay::EventDispatcher;
use crate::repository::VfsConfig;
use crate::repository::VfsType;

struct Provider {
    record: Arc<Mutex<()>>,
    dropped: Arc<AtomicBool>,
    valid: bool,
    panic: bool,
}
#[async_trait::async_trait]
impl FilesystemProvider for Provider {
    fn holds_state(&self) -> bool {
        assert!(
            self.record.try_lock().is_ok(),
            "callback under Record guard"
        );
        assert!(!self.panic, "provider callback panic");
        self.valid
    }
    async fn begin_operation(&self) -> std::result::Result<Arc<InstanceOperationImpl>, FsError> {
        Err(FsError::internal("publication fixture has no operations"))
    }
}
impl Drop for Provider {
    fn drop(&mut self) {
        assert!(
            self.record.try_lock().is_ok(),
            "provider drop under Record guard"
        );
        self.dropped.store(true, Ordering::Release);
    }
}
struct Parent {
    registration: Arc<Registration>,
    record: Arc<Mutex<()>>,
    client: Arc<tokio::sync::Mutex<()>>,
    success: Arc<AtomicBool>,
    provider_dropped: Arc<AtomicBool>,
    dropped: Arc<AtomicBool>,
}
impl Drop for Parent {
    fn drop(&mut self) {
        assert!(
            self.record.try_lock().is_ok(),
            "parent drop under Record guard"
        );
        assert!(self.client.try_lock().is_ok(), "parent outlived no Client");
        let location = self.registration.bootstrap_location();
        if self.success.load(Ordering::Acquire) {
            assert!(matches!(
                location,
                Err(provider_registry::Error::AlreadyPublished)
            ));
        } else {
            assert!(matches!(location, Err(provider_registry::Error::Retired)));
            assert!(self.provider_dropped.load(Ordering::Acquire));
        }
        self.dropped.store(true, Ordering::Release);
    }
}
struct Fixture {
    working: Option<PendingWorkingContext>,
    registration: Arc<Registration>,
    registry: Arc<ProviderRegistry>,
    location: WorkingLocation,
    client: Arc<tokio::sync::Mutex<()>>,
    record: Arc<Mutex<()>>,
    success: Arc<AtomicBool>,
    parent_dropped: Arc<AtomicBool>,
    provider_dropped: Arc<AtomicBool>,
    _metadata: TempDir,
    _target: TempDir,
}
impl Fixture {
    async fn new() -> Self {
        use std::os::unix::fs::PermissionsExt;
        let target = TempDir::new("lore-publication-target-");
        let metadata = TempDir::new("lore-publication-metadata-");
        std::fs::set_permissions(metadata.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let registry = ProviderRegistry::new(Default::default());
        let registration = Arc::new(
            registry
                .reserve(target.path(), InstanceId::generate())
                .unwrap(),
        );
        registration
            .reserve_metadata(metadata.path(), [51; 16].into())
            .unwrap();
        let location = registration.bootstrap_location().unwrap();
        let token = RepositoryWriteToken::acquire(target.path()).await;
        let RepositoryWriteToken::Client(guard) = &token else {
            unreachable!()
        };
        let client = tokio::sync::OwnedMutexGuard::mutex(guard.as_ref()).clone();
        let record = Arc::new(Mutex::new(()));
        let success = Arc::new(AtomicBool::new(false));
        let parent_dropped = Arc::new(AtomicBool::new(false));
        let provider_dropped = Arc::new(AtomicBool::new(true));
        let parent = IoLease::new(Arc::new(Parent {
            registration: registration.clone(),
            record: record.clone(),
            client: client.clone(),
            success: success.clone(),
            provider_dropped: provider_dropped.clone(),
            dropped: parent_dropped.clone(),
        }));
        let execution = Arc::new(ExecutionContext::new_client_with_user_id(
            LoreGlobalArgs {
                offline: 1,
                ..Default::default()
            },
            EventDispatcher::no_dispatch(),
            "test-user".into(),
        ));
        let working = LORE_CONTEXT
            .scope(
                execution,
                parent.scope(async {
                    let metadata = MetadataInitializer::new(1)
                        .unwrap()
                        .prepare_pending(
                            registration.clone(),
                            token,
                            Mode::CreateNew(RepositoryConfig {
                                vfs: Some(VfsConfig {
                                    vfs_type: VfsType::ExperimentalWorking,
                                }),
                                ..Default::default()
                            }),
                        )
                        .await
                        .unwrap();
                    StoreInitializer::new(1)
                        .unwrap()
                        .prepare_pending(
                            metadata,
                            StoreMode::CreateNew(Selection {
                                branch: [62; 16].into(),
                                branch_name: "main".into(),
                                revision: Hash::default(),
                            }),
                        )
                        .await
                        .unwrap()
                }),
            )
            .await;
        drop(parent);
        Self {
            working: Some(working),
            registration,
            registry,
            location,
            client,
            record,
            success,
            parent_dropped,
            provider_dropped,
            _metadata: metadata,
            _target: target,
        }
    }
    fn provider(&self, valid: bool, panic: bool) -> Arc<dyn FilesystemProvider> {
        self.provider_dropped.store(false, Ordering::Release);
        Arc::new(Provider {
            record: self.record.clone(),
            dropped: self.provider_dropped.clone(),
            valid,
            panic,
        })
    }
    fn parts(&mut self) -> (PreparedWorkingContext, PendingPublication) {
        self.working
            .take()
            .unwrap()
            .into_owner_build()
            .ok()
            .unwrap()
            .into_trusted_owner_parts()
    }
    fn assert_closed(&self) {
        let probe: Arc<dyn FilesystemProvider> = Arc::new(Provider {
            record: self.record.clone(),
            dropped: Arc::new(AtomicBool::new(false)),
            valid: true,
            panic: false,
        });
        assert!(matches!(
            self.registration.publish(probe),
            Err(provider_registry::Error::AdmissionBusy)
        ));
        assert!(matches!(
            self.registry.resolve_location(&self.location),
            Err(provider_registry::Error::NotReady)
        ));
    }
}

#[tokio::test]
async fn exact_publication_releases_client_before_ready_and_defers_all_cleanup() {
    let mut f = Fixture::new().await;
    let (context, mut permit) = f.parts();
    let provider = f.provider(true, false);
    // The cleanup slot outlives the outer guard even if a postcommit assertion unwinds.
    let mut cleanup = None;
    {
        let mut prepared = permit.prepare(&provider).unwrap();
        assert!(f.client.try_lock().is_err());
        prepared.release_client().unwrap();
        assert!(f.client.try_lock().is_ok());
        f.assert_closed();
        f.success.store(true, Ordering::Release);
        let _record = f.record.lock().unwrap();
        assert!(cleanup.replace(prepared.commit().unwrap()).is_none());
        assert!(!f.parent_dropped.load(Ordering::Acquire));
        assert!(matches!(prepared.commit(), Err(Error::Runtime)));
    }
    assert!(!f.parent_dropped.load(Ordering::Acquire));
    drop(cleanup);
    assert!(f.parent_dropped.load(Ordering::Acquire));
    assert!(f.registry.resolve_location(&f.location).is_ok());
    assert!(context.repository.try_write_token().is_none());
    drop(context);
}

#[tokio::test]
async fn retirement_after_preflight_is_terminal_and_keeps_cleanup_outside_record() {
    let mut f = Fixture::new().await;
    let (context, mut permit) = f.parts();
    let provider = f.provider(true, false);
    {
        let mut prepared = permit.prepare(&provider).unwrap();
        prepared.release_client().unwrap();
        f.registration.retire().unwrap();
        let _record = f.record.lock().unwrap();
        assert!(matches!(
            prepared.commit(),
            Err(Error::Registry(provider_registry::Error::Retired))
        ));
        assert!(!f.parent_dropped.load(Ordering::Acquire));
    }
    permit.quarantine().unwrap();
    drop(context);
    drop(provider);
    drop(permit);
    assert!(f.parent_dropped.load(Ordering::Acquire));
    assert!(matches!(
        f.registry.resolve_location(&f.location),
        Err(provider_registry::Error::Retired)
    ));
}

#[tokio::test]
async fn commit_before_client_release_cannot_open_ready_or_release_cleanup() {
    let mut f = Fixture::new().await;
    let (context, mut permit) = f.parts();
    let provider = f.provider(true, false);
    {
        let mut prepared = permit.prepare(&provider).unwrap();
        let _record = f.record.lock().unwrap();
        assert!(matches!(prepared.commit(), Err(Error::InvalidToken)));
        assert!(f.client.try_lock().is_err());
        assert!(!f.parent_dropped.load(Ordering::Acquire));
    }
    assert!(matches!(
        f.registry.resolve_location(&f.location),
        Err(provider_registry::Error::NotReady)
    ));
    permit.quarantine().unwrap();
    assert!(f.client.try_lock().is_err());
    drop(context);
    drop(provider);
    drop(permit);
    assert!(f.parent_dropped.load(Ordering::Acquire));
}

#[tokio::test]
async fn dropping_prepared_borrow_keeps_exclusion_and_cannot_restart_preflight() {
    let mut f = Fixture::new().await;
    let (context, mut permit) = f.parts();
    let provider = f.provider(true, false);
    {
        let _prepared = permit.prepare(&provider).unwrap();
    }
    assert!(f.client.try_lock().is_err());
    f.assert_closed();
    assert!(permit.io_lease().is_err());
    assert!(matches!(permit.prepare(&provider), Err(Error::Runtime)));
    assert!(!f.parent_dropped.load(Ordering::Acquire));
    permit.quarantine().unwrap();
    drop(context);
    drop(provider);
    drop(permit);
    assert!(f.parent_dropped.load(Ordering::Acquire));
}

#[tokio::test]
async fn preflight_callback_failure_and_panic_retain_quarantined_cleanup() {
    for panic in [false, true] {
        let mut f = Fixture::new().await;
        let (context, mut permit) = f.parts();
        let provider = f.provider(panic, panic);
        if panic {
            assert!(
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    let _result = permit.prepare(&provider);
                }))
                .is_err()
            );
        } else {
            assert!(matches!(
                permit.prepare(&provider),
                Err(Error::Registry(provider_registry::Error::InvalidProvider))
            ));
        }
        assert!(!f.parent_dropped.load(Ordering::Acquire));
        assert!(f.client.try_lock().is_err());
        assert!(matches!(
            f.registration.bootstrap_location(),
            Err(provider_registry::Error::Retired)
        ));
        assert!(permit.io_lease().is_err());
        drop(context);
        drop(provider);
        drop(permit);
        assert!(f.parent_dropped.load(Ordering::Acquire));
        assert!(f.client.try_lock().is_ok());
    }
}

#[tokio::test]
async fn foreign_claim_cannot_commit_or_consume_the_preflighted_provider() {
    let mut f = Fixture::new().await;
    let foreign = Fixture::new().await;
    let (context, mut permit) = f.parts();
    let mut provider = Some(f.provider(true, false));
    let original = provider.as_ref().unwrap().clone();
    let mut armed = true;
    let PublicationState::Shared(owner) = &mut permit.state else {
        unreachable!()
    };
    let owner = Arc::get_mut(owner).unwrap();
    assert!(matches!(
        owner._qualification.commit_publication(
            &foreign.registration,
            &foreign.location,
            &mut provider,
            &mut armed,
        ),
        Err(provider_registry::Error::Stale)
    ));
    assert!(armed);
    assert!(Arc::ptr_eq(provider.as_ref().unwrap(), &original));
    drop(original);
    drop(provider);
    drop(context);
    drop(permit);
    assert!(f.parent_dropped.load(Ordering::Acquire));
}

struct Finished(Arc<AtomicBool>);
impl Drop for Finished {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}
struct ResumeListing(Arc<lore_storage::test_util::Pause>);
impl Drop for ResumeListing {
    fn drop(&mut self) {
        self.0.resume.notify_one();
    }
}
struct ActualIo {
    _lease: IoLease,
    _finished: Finished,
}
async fn local_store() -> (TempDir, Arc<LocalMutableStore>) {
    let directory = TempDir::new("lore-publication-actual-io-");
    let immutable = immutable_store::create(
        None::<&str>,
        immutable_store::ImmutableStoreCreateOptions::none(),
        false,
        Default::default(),
    )
    .await
    .unwrap();
    let store = Arc::new(
        LocalMutableStore::new(
            Some(directory.path()),
            MutableStoreSettings {
                initial_fan_out_level: 1,
                reuse_state_across_holds: true,
                flush_delay_seconds: 0,
                ..Default::default()
            },
            immutable,
        )
        .await
        .unwrap(),
    );
    store
        .clone()
        .store(
            Partition::default(),
            Hash::from_u64(71),
            Hash::from_u64(72),
            lore_storage::KeyType::BranchMetadata,
        )
        .await
        .unwrap();
    store.clone().flush(true).await.unwrap();
    (directory, store)
}
async fn wait_finished(done: &AtomicBool) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while !done.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn actual_listing_io_makes_both_unique_owner_boundaries_retryable_busy() {
    let mut f = Fixture::new().await;
    let (_directory, store) = local_store().await;
    let done = Arc::new(AtomicBool::new(false));
    let scope = IoLease::new(Arc::new(ActualIo {
        _lease: IoLease::new(f.working.as_ref().unwrap()._owner.clone()),
        _finished: Finished(done.clone()),
    }));
    let pause = lore_storage::test_util::pause_at(PausePoint::Listing);
    let _resume = ResumeListing(pause.clone());
    let stream = scope
        .scope(
            store
                .clone()
                .list(Partition::default(), lore_storage::KeyType::BranchMetadata),
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(10), pause.reached.notified())
        .await
        .unwrap();
    drop(scope);
    drop(stream.channel());
    let working = f
        .working
        .take()
        .unwrap()
        .into_owner_build()
        .err()
        .unwrap()
        .into_context();
    assert!(f.client.try_lock().is_err());
    assert!(!done.load(Ordering::Acquire));
    pause.resume.notify_one();
    wait_finished(&done).await;
    f.working = Some(working);
    let (context, mut permit) = f.parts();
    let done = Arc::new(AtomicBool::new(false));
    let scope = IoLease::new(Arc::new(ActualIo {
        _lease: permit.io_lease().unwrap(),
        _finished: Finished(done.clone()),
    }));
    let pause = lore_storage::test_util::pause_at(PausePoint::Listing);
    let _resume = ResumeListing(pause.clone());
    let stream = scope
        .scope(
            store
                .clone()
                .list(Partition::default(), lore_storage::KeyType::BranchMetadata),
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(10), pause.reached.notified())
        .await
        .unwrap();
    drop(scope);
    drop(stream.channel());
    let provider = f.provider(true, false);
    assert!(matches!(permit.prepare(&provider), Err(Error::Busy)));
    assert!(!done.load(Ordering::Acquire));
    pause.resume.notify_one();
    wait_finished(&done).await;
    {
        let _prepared = permit.prepare(&provider).unwrap();
    }
    drop(context);
    drop(provider);
    drop(permit);
    assert!(f.parent_dropped.load(Ordering::Acquire));
}

struct CurrentCaller {
    registration: Arc<Registration>,
    client: Arc<tokio::sync::Mutex<()>>,
    older_parent_dropped: Arc<AtomicBool>,
    done: std::sync::mpsc::Sender<bool>,
}
impl Drop for CurrentCaller {
    fn drop(&mut self) {
        let safe = self.older_parent_dropped.load(Ordering::Acquire)
            && self.client.try_lock().is_ok()
            && matches!(
                self.registration.bootstrap_location(),
                Err(provider_registry::Error::Retired)
            );
        let _closed = self.done.send(safe);
    }
}
#[test]
fn setup_listing_retains_distinct_current_caller_after_cancellation_and_runtime_drop() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut f = runtime.block_on(Fixture::new());
    let (_directory, store) = runtime.block_on(local_store());
    let (context, permit) = f.parts();
    let (done, completed) = std::sync::mpsc::channel();
    let caller = Arc::new(CurrentCaller {
        registration: f.registration.clone(),
        client: f.client.clone(),
        older_parent_dropped: f.parent_dropped.clone(),
        done,
    });
    let weak = Arc::downgrade(&caller);
    let caller = IoLease::new(caller);
    let setup = caller.sync_scope(|| permit.io_lease().unwrap());
    let (started, starting) = std::sync::mpsc::channel();
    let watched = weak.clone();
    let owner = std::thread::Builder::new()
        .name("publication-setup-io".into())
        .spawn(move || {
            let owner_runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            owner_runtime.block_on(async move {
                // This runtime owns the producer. Its lifetime is independent of the
                // caller runtime; the observer retains no caller or pending owner.
                let pause = lore_storage::test_util::pause_at(PausePoint::Listing);
                let _resume = ResumeListing(pause.clone());
                let stream = setup
                    .scope(store.list(Partition::default(), lore_storage::KeyType::BranchMetadata))
                    .await
                    .unwrap();
                drop(stream.channel());
                drop(setup);
                started.send(pause).unwrap();
                tokio::time::timeout(Duration::from_secs(10), async {
                    while watched.upgrade().is_some() {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .unwrap();
            });
        })
        .unwrap();
    let pause = starting.recv_timeout(Duration::from_secs(10)).unwrap();
    let _resume = ResumeListing(pause.clone());
    runtime.block_on(async {
        tokio::time::timeout(Duration::from_secs(10), pause.reached.notified())
            .await
            .unwrap();
    });
    drop(caller);
    runtime.shutdown_timeout(Duration::ZERO);
    drop(context);
    drop(permit);
    assert!(weak.upgrade().is_some());
    assert!(!f.parent_dropped.load(Ordering::Acquire));
    assert!(f.client.try_lock().is_err());
    assert!(completed.try_recv().is_err());
    pause.resume.notify_one();
    assert!(completed.recv_timeout(Duration::from_secs(10)).unwrap());
    owner.join().unwrap();
    assert!(weak.upgrade().is_none());
    assert!(f.parent_dropped.load(Ordering::Acquire));
    assert!(f.client.try_lock().is_ok());
}

#[tokio::test]
async fn setup_completion_waits_for_actual_io_after_a_cancelled_wait() {
    let mut f = Fixture::new().await;
    let (_directory, store) = local_store().await;
    let (context, mut permit) = f.parts();
    let (setup, mut completed) = permit.setup_io().unwrap();
    let pause = lore_storage::test_util::pause_at(PausePoint::Listing);
    let _resume = ResumeListing(pause.clone());
    let stream = setup
        .scope(store.list(Partition::default(), lore_storage::KeyType::BranchMetadata))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(10), pause.reached.notified())
        .await
        .unwrap();
    drop(stream.channel());
    drop(setup);
    let provider = f.provider(true, false);
    assert!(matches!(permit.prepare(&provider), Err(Error::Busy)));
    assert!(f.client.try_lock().is_err());
    let mut wait = Box::pin(async { (&mut completed).await });
    tokio::select! {
        biased;
        result = &mut wait => panic!("setup completed before actual IO: {result:?}"),
        () = tokio::task::yield_now() => {},
    }
    drop(wait);
    assert!(f.client.try_lock().is_err());
    assert!(!f.parent_dropped.load(Ordering::Acquire));
    pause.resume.notify_one();
    tokio::time::timeout(Duration::from_secs(10), completed)
        .await
        .unwrap()
        .unwrap();
    let cleanup = {
        let mut prepared = permit.prepare(&provider).unwrap();
        prepared.release_client().unwrap();
        f.success.store(true, Ordering::Release);
        prepared.commit().unwrap()
    };
    drop(cleanup);
    drop(permit);
    drop(context);
    drop(provider);
    assert!(f.client.try_lock().is_ok());
    assert!(f.parent_dropped.load(Ordering::Acquire));
}
