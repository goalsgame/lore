// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use tokio::sync::mpsc;

use super::*;

struct OwnerProbe {
    limiter: Arc<Semaphore>,
    life: Arc<SessionLife>,
    payload_dropped: Arc<AtomicBool>,
    observed: Arc<AtomicBool>,
}
impl Drop for OwnerProbe {
    fn drop(&mut self) {
        self.observed.store(
            self.limiter.available_permits() == 1
                && self.life.closed.try_lock().is_ok()
                && self.payload_dropped.load(Ordering::SeqCst),
            Ordering::SeqCst,
        );
    }
}
struct PayloadProbe {
    limiter: Arc<Semaphore>,
    dropped: Arc<AtomicBool>,
    observed: Arc<AtomicBool>,
}
impl Drop for PayloadProbe {
    fn drop(&mut self) {
        self.observed
            .store(self.limiter.available_permits() == 0, Ordering::SeqCst);
        self.dropped.store(true, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn undelivered_reply_drops_payload_then_capacity_then_io_owner() {
    let life = SessionLife::new();
    let limiter = Arc::new(Semaphore::new(1));
    let payload_dropped = Arc::new(AtomicBool::new(false));
    let owner_observed = Arc::new(AtomicBool::new(false));
    let payload_observed = Arc::new(AtomicBool::new(false));
    let owner = Arc::new(OwnerProbe {
        limiter: limiter.clone(),
        life: life.clone(),
        payload_dropped: payload_dropped.clone(),
        observed: owner_observed.clone(),
    });
    let weak = Arc::downgrade(&owner);
    let lease = IoLease::new(owner);
    let operation = lease
        .scope(Operation::acquire(&limiter, &life))
        .await
        .unwrap();
    let (tx, rx) = oneshot::channel();
    let reply = Reply::new(tx, operation.clone());
    drop(operation);
    drop(rx);
    drop(lease);
    assert!(weak.upgrade().is_some());
    assert_eq!(limiter.available_permits(), 0);
    reply.send(Ok(PayloadProbe {
        limiter: limiter.clone(),
        dropped: payload_dropped,
        observed: payload_observed.clone(),
    }));
    assert!(weak.upgrade().is_none());
    assert!(payload_observed.load(Ordering::SeqCst));
    assert!(owner_observed.load(Ordering::SeqCst));
}

#[tokio::test]
async fn stopped_capacity_wait_releases_captured_owner_without_admission() {
    let life = SessionLife::new();
    let limiter = Arc::new(Semaphore::new(0));
    let owner = Arc::new(());
    let weak = Arc::downgrade(&owner);
    let lease = IoLease::new(owner);
    let mut waiting = Box::pin(lease.scope(Operation::acquire(&limiter, &life)));
    assert!(futures::poll!(&mut waiting).is_pending());
    drop(lease);
    assert!(weak.upgrade().is_some());
    life.close();
    assert!(waiting.await.is_err());
    assert!(weak.upgrade().is_none());
    assert!(life.check_open().is_err());
    assert_eq!(limiter.available_permits(), 0);
}

#[tokio::test]
async fn coalesced_replies_keep_distinct_logical_capacity_until_each_finishes() {
    let life = SessionLife::new();
    let limiter = Arc::new(Semaphore::new(2));
    let first = Operation::acquire(&limiter, &life).await.unwrap();
    let second = Operation::acquire(&limiter, &life).await.unwrap();
    let (first_tx, first_rx) = oneshot::channel::<Result<(), ProtocolError>>();
    let (second_tx, second_rx) = oneshot::channel::<Result<(), ProtocolError>>();
    let first_reply = Reply::new(first_tx, first.clone());
    let second_reply = Reply::new(second_tx, second.clone());
    drop(first);
    drop(second);
    drop(first_rx);
    drop(second_rx);
    assert_eq!(limiter.available_permits(), 0);
    first_reply.send(Ok(()));
    assert_eq!(limiter.available_permits(), 1);
    second_reply.send(Ok(()));
    assert_eq!(limiter.available_permits(), 2);
}

struct InflightOwnerProbe {
    map: std::sync::Weak<super::super::Inflight<()>>,
    key: lore_base::types::Address,
    life: Arc<SessionLife>,
    limiter: Arc<Semaphore>,
    observed: Arc<AtomicBool>,
}
impl Drop for InflightOwnerProbe {
    fn drop(&mut self) {
        let map_unlocked = self.map.upgrade().is_some_and(|map| {
            !matches!(
                map.try_get(&self.key),
                dashmap::try_result::TryResult::Locked
            )
        });
        self.observed.store(
            map_unlocked
                && self.life.closed.try_lock().is_ok()
                && self.limiter.available_permits() == 1,
            Ordering::SeqCst,
        );
    }
}

#[tokio::test]
async fn inflight_failure_disposes_final_io_owner_outside_map_and_life_locks() {
    let life = SessionLife::new();
    let limiter = Arc::new(Semaphore::new(1));
    let map = Arc::new(super::super::Inflight::new());
    let key = lore_base::types::Address::zero_context_hash(lore_base::types::Hash::from([7; 32]));
    let observed = Arc::new(AtomicBool::new(false));
    let lease = IoLease::new(Arc::new(InflightOwnerProbe {
        map: Arc::downgrade(&map),
        key,
        life: life.clone(),
        limiter: limiter.clone(),
        observed: observed.clone(),
    }));
    let operation = lease
        .scope(Operation::acquire(&limiter, &life))
        .await
        .unwrap();
    let (tx, rx) = oneshot::channel();
    assert!(super::super::register(&map, key, Reply::new(tx, operation)));
    drop(rx);
    drop(lease);
    super::super::fail_inflight(
        &map,
        super::super::Verb::Get,
        &ProtocolError::internal("response EOF"),
    );
    assert!(observed.load(Ordering::SeqCst));
    assert!(map.is_empty());
}

fn successful_stream(session: Arc<SessionLife>) -> super::super::PreparedStream<u8, ()> {
    let (sender, receiver) = mpsc::channel(1);
    let stream = super::super::StreamLife::new(receiver);
    let handle = Arc::new(super::super::StreamState {
        sender,
        opened: Arc::new(AtomicBool::new(true)),
        session: session.clone(),
        stream: stream.clone(),
    });
    super::super::PreparedStream::new(handle, async move {
        let (_, reply) = stream.next(&session).await.expect("accepted request");
        reply.send(Ok(()));
    })
}

#[tokio::test]
async fn cached_unopened_refusal_is_preserved_and_evicted_before_next_request() {
    let ctx = super::super::GrpcSessionContext::new(
        lore_base::types::Partition::from(lore_base::types::Context::from([1; 16])),
        "cached-refusal".to_string(),
        String::new(),
    );
    let limiter = Arc::new(Semaphore::new(1));
    let operation = Operation::acquire(&limiter, &ctx.life).await.unwrap();
    let cache = super::super::StreamCache::<u8, ()>::new();
    let key = (7, super::super::Verb::Get);
    let (sender, receiver) = mpsc::channel(1);
    let stream = super::super::StreamLife::new(receiver);
    stream.terminate(ProtocolError::from(lore_base::error::NotAuthorized));
    cache.streams.insert(
        key,
        Arc::new(super::super::StreamState {
            sender,
            opened: Arc::new(AtomicBool::new(false)),
            session: ctx.life.clone(),
            stream,
        }),
    );
    let error = cache
        .request(key, &ctx, 1, &operation, || {
            panic!("refusal must not retry")
        })
        .await
        .unwrap_err();
    assert!(error.is_not_authorized());
    assert!(
        cache.streams.is_empty(),
        "dead pre-open refusal must not poison later calls"
    );
    cache
        .request(key, &ctx, 2, &operation, || {
            successful_stream(ctx.life.clone())
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn cached_opened_terminal_rotates_instead_of_returning_stale_internal_error() {
    let ctx = super::super::GrpcSessionContext::new(
        lore_base::types::Partition::from(lore_base::types::Context::from([1; 16])),
        "cached-opened-terminal".to_string(),
        String::new(),
    );
    let limiter = Arc::new(Semaphore::new(1));
    let operation = Operation::acquire(&limiter, &ctx.life).await.unwrap();
    let cache = super::super::StreamCache::<u8, ()>::new();
    let key = (7, super::super::Verb::Get);
    let (sender, receiver) = mpsc::channel(1);
    let stream = super::super::StreamLife::new(receiver);
    stream.terminate(ProtocolError::internal("old response stream ended"));
    cache.streams.insert(
        key,
        Arc::new(super::super::StreamState {
            sender,
            opened: Arc::new(AtomicBool::new(true)),
            session: ctx.life.clone(),
            stream,
        }),
    );
    let prepared = std::sync::atomic::AtomicUsize::new(0);
    cache
        .request(key, &ctx, 1, &operation, || {
            prepared.fetch_add(1, Ordering::SeqCst);
            successful_stream(ctx.life.clone())
        })
        .await
        .unwrap();
    assert_eq!(prepared.load(Ordering::SeqCst), 1);
}
