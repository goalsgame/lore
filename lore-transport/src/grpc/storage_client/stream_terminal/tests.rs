// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

use std::sync::Weak;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use tokio::sync::Semaphore;
use tokio::sync::oneshot;

use super::*;
use crate::grpc::storage_client::request_ownership::Operation;
use crate::grpc::storage_client::request_ownership::Reply;

struct QueueProbe {
    stream: Weak<StreamLife<QueueProbe, ()>>,
    unlocked: Arc<AtomicBool>,
}
impl Drop for QueueProbe {
    fn drop(&mut self) {
        let unlocked = self
            .stream
            .upgrade()
            .is_some_and(|stream| stream.state.try_lock().is_ok());
        self.unlocked.store(unlocked, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn terminal_extracts_payload_and_reply_before_resource_destruction() {
    let session = SessionLife::new();
    let limiter = Arc::new(Semaphore::new(1));
    let (tx, rx) = mpsc::channel(1);
    let stream = StreamLife::new(rx);
    let unlocked = Arc::new(AtomicBool::new(false));
    let owner = Arc::new(());
    let weak = Arc::downgrade(&owner);
    let lease = lore_io::lease::IoLease::new(owner);
    let operation = lease
        .scope(Operation::acquire(&limiter, &session))
        .await
        .unwrap();
    let (reply, receiver) = oneshot::channel();
    tx.send((
        QueueProbe {
            stream: Arc::downgrade(&stream),
            unlocked: unlocked.clone(),
        },
        Reply::new(reply, operation),
    ))
    .await
    .ok()
    .unwrap();
    drop(lease);
    drop(receiver);
    stream.terminate(ProtocolError::internal("response EOF"));
    assert!(stream.admit().is_err());
    assert!(weak.upgrade().is_some());
    let item = stream.take_failed().expect("accepted queued item");
    assert!(weak.upgrade().is_some());
    drop(item);
    assert!(unlocked.load(Ordering::SeqCst));
    assert!(weak.upgrade().is_none());
    assert_eq!(limiter.available_permits(), 1);
    assert!(stream.take_failed().is_none());
    assert!(stream.next(&session).await.is_none());
    assert!(
        session.check_open().is_ok(),
        "stream EOF does not stop session"
    );
}

#[tokio::test]
async fn graceful_stop_drains_accepted_queue_without_admitting_a_new_item() {
    let session = SessionLife::new();
    let limiter = Arc::new(Semaphore::new(2));
    let (tx, rx) = mpsc::channel(2);
    let stream = StreamLife::new(rx);
    for value in [1u8, 2] {
        let operation = Operation::acquire(&limiter, &session).await.unwrap();
        let (reply, _receiver) = oneshot::channel::<Result<(), ProtocolError>>();
        tx.send((value, Reply::new(reply, operation)))
            .await
            .ok()
            .unwrap();
    }
    session.close();
    assert!(session.admit().is_err());
    let first = stream.next(&session).await.unwrap();
    let second = stream.next(&session).await.unwrap();
    assert_eq!((first.0, second.0), (1, 2));
    assert!(stream.next(&session).await.is_none());
    drop(first);
    drop(second);
    assert_eq!(limiter.available_permits(), 2);
    assert!(tx.is_closed());
}
