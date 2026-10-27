// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

use super::*;

#[test]
fn completed_io_keeps_tail_freeze_serial_and_storage_until_capture_destruction() {
    use crate::working_storage::WorkingStorage;
    let directory = Directory::new();
    let mountpoint = directory.0.join("mount");
    std::fs::create_dir(&mountpoint).unwrap();
    let storage_path = directory.0.join("storage");
    let instance = lore_revision::instance::InstanceId::generate();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let ns = namespace();
    let storage = WorkingStorage::open(
        &storage_path,
        &mountpoint,
        [1; 16].into(),
        instance,
        Default::default(),
    )
    .unwrap();
    let session = storage.begin_session().unwrap();
    let factory = session.factory();
    let weak_factory = Arc::downgrade(&factory);
    let owner = CheckpointOwner::new(
        ns.clone(),
        factory.clone(),
        Recovery::open(storage.recovery_directory(), Default::default()).unwrap(),
        1 << 20,
    );
    let tail = Arc::new(PublishPause {
        started: tokio::sync::Notify::new(),
        resumed: Mutex::new(false),
        wake: Condvar::new(),
    });
    *owner.tail_pause.lock().unwrap() = Some(tail.clone());
    runtime.block_on(async {
        let lease = ns.freeze_operation().await.unwrap();
        let publication = owner.publish_frozen(lease);
        tokio::pin!(publication);
        tokio::select! { result=&mut publication=>panic!("tail did not pause: {result:?}"),_=tail.started.notified()=>{} }
        // Durable IO is complete; only destruction remains deliberately paused.
        assert!(owner.recovery.lock().unwrap().load().unwrap().is_some());
        assert_eq!(*owner.publications.active.lock().unwrap(), 1);
        assert!(owner.serial.try_lock().is_err());
        assert!(ns.freeze_operation().await.is_err());
    }); // Cancel the original request without cancelling the admitted OS task.
    owner.close();
    let mut drains = Vec::new();
    let (sender, receiver) = std::sync::mpsc::channel();
    for _ in 0..2 {
        let owner = owner.clone();
        let sender = sender.clone();
        drains.push(std::thread::spawn(move || {
            sender.send(owner.drain()).unwrap();
        }));
    }
    drop(sender);
    drop(owner);
    drop(factory);
    drop(session);
    drop(storage);
    drop(runtime); // No new Tokio blocking job may make this wait for the tail.
    assert!(weak_factory.upgrade().is_some());
    assert!(matches!(
        WorkingStorage::open(
            &storage_path,
            &mountpoint,
            [1; 16].into(),
            instance,
            Default::default()
        ),
        Err(crate::working_storage::Error::Busy)
    ));
    assert!(receiver.recv_timeout(Duration::from_millis(20)).is_err());
    tail.resume();
    for _ in 0..2 {
        receiver
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap();
    }
    for drain in drains {
        drain.join().unwrap();
    }
    assert!(weak_factory.upgrade().is_none());
    let reopened = WorkingStorage::open(
        &storage_path,
        &mountpoint,
        [1; 16].into(),
        instance,
        Default::default(),
    )
    .unwrap();
    drop(reopened);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        drop(ns.freeze_operation().await.unwrap());
    });
}

#[tokio::test]
async fn capture_destructor_panic_poisons_before_releasing_serial_and_completion() {
    let directory = Directory::new();
    let ns = namespace();
    let owner = owner(&directory, ns.clone());
    owner.tail_panic.store(true, Ordering::Release);
    let lease = ns.freeze_operation().await.unwrap();
    assert!(matches!(
        owner.publish_frozen(lease).await,
        Err(Error::Core(core::Error::Io))
    ));
    owner.drain().unwrap();
    assert_eq!(*owner.publications.active.lock().unwrap(), 0);
    assert!(owner.is_poisoned());
    assert!(owner.serial.try_lock().is_ok());
    drop(ns.freeze_operation().await.unwrap());
    assert!(owner.recovery.lock().unwrap().load().unwrap().is_some());
    assert!(matches!(owner.publish().await, Err(Error::Poisoned)));
}
