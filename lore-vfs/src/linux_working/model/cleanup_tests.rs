// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

use super::*;
use crate::overlay::Access;
use crate::working_storage::WorkingStorage;

fn counters() -> Arc<Counters> {
    Arc::new(Counters {
        handles: AtomicU64::new(1),
        stores: AtomicU64::new(0),
        dir_entries: AtomicU64::new(0),
        dir_bytes: AtomicU64::new(0),
    })
}
async fn file(store: Arc<dyn SparseStore>) -> (Arc<File>, u64) {
    let overlay = Arc::new(
        OverlayFile::empty(
            super::tests::root().key,
            0o644,
            Arc::new(super::tests::Base::new()),
            Arc::new(super::tests::Gate::default()),
            store,
            Default::default(),
        )
        .unwrap(),
    );
    let inner = overlay.open(Access::Read).await.unwrap();
    (
        Arc::new(File {
            overlay,
            mutations: AsyncMutex::new(()),
        }),
        inner,
    )
}
fn job(file: Arc<File>, inner: u64, counters: Arc<Counters>) -> Arc<CleanupJob> {
    Arc::new(CleanupJob {
        state: AsyncMutex::new(CleanupState {
            resources: Some(CleanupResources {
                file,
                inner,
                _slot: Arc::new(HandleSlot(counters, 1)),
            }),
            result: None,
        }),
    })
}

#[tokio::test]
async fn retained_completed_cleanup_job_drops_file_store_and_actual_session_lease() {
    let directory = super::tests::Directory::new("cleanup-completed");
    let mount = directory.0.join("mount");
    std::fs::create_dir(&mount).unwrap();
    let path = directory.0.join("storage");
    let instance = lore_revision::instance::InstanceId::generate();
    let storage =
        WorkingStorage::open(&path, &mount, [1; 16].into(), instance, Default::default()).unwrap();
    let session = storage.begin_session().unwrap();
    let factory = session.factory();
    let store = factory.create().await.unwrap();
    let weak_store = Arc::downgrade(&store);
    let (file, inner) = file(store).await;
    let weak_file = Arc::downgrade(&file);
    let counters = counters();
    let job = job(file, inner, counters.clone());
    drop(factory);
    drop(session);
    drop(storage);
    assert!(matches!(
        WorkingStorage::open(&path, &mount, [1; 16].into(), instance, Default::default()),
        Err(crate::working_storage::Error::Busy)
    ));
    job.run().await.unwrap();
    assert!(weak_file.upgrade().is_none());
    assert!(weak_store.upgrade().is_none());
    assert_eq!(counters.handles.load(Ordering::Acquire), 0);
    assert!(job.state.lock().await.resources.is_none());
    job.run().await.unwrap(); // A retained detached-task clone sees only result.
    let reopened =
        WorkingStorage::open(&path, &mount, [1; 16].into(), instance, Default::default()).unwrap();
    drop(reopened);
    drop(job);
}

#[tokio::test]
async fn cancelled_blocked_release_retains_resources_and_retry_drops_before_result() {
    let store = super::tests::Stores::default().create().await.unwrap();
    let weak_store = Arc::downgrade(&store);
    let (file, inner) = file(store).await;
    let weak_file = Arc::downgrade(&file);
    let overlay = file.overlay.clone();
    let counters = counters();
    let job = job(file, inner, counters.clone());
    let hold = overlay.checkpoint_guard().await;
    let mut release = Box::pin(job.run());
    tokio::select! { _=tokio::task::yield_now()=>{}, result=&mut release=>panic!("release passed held overlay guard: {result:?}") }
    drop(release);
    assert!(job.state.lock().await.resources.is_some());
    assert!(job.state.lock().await.result.is_none());
    assert!(weak_file.upgrade().is_some());
    assert_eq!(counters.handles.load(Ordering::Acquire), 1);
    drop(hold);
    drop(overlay);
    job.run().await.unwrap();
    assert!(weak_file.upgrade().is_none());
    assert!(weak_store.upgrade().is_none());
    assert_eq!(counters.handles.load(Ordering::Acquire), 0);
    assert_eq!(job.state.lock().await.result, Some(Ok(())));
}
