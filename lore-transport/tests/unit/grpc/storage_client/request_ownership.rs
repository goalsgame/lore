// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

use std::collections::VecDeque;
use std::future::Future;
use std::time::Duration;

use lore_io::lease::IoLease;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

use super::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Wire {
    Get,
    Metadata,
    Put,
    Copy,
    GetResolved,
    PutResolved,
}
struct Seen {
    wire: Wire,
    stream: usize,
    address: Bytes,
    target: Bytes,
    request_id: u64,
    reply: oneshot::Sender<()>,
    terminal: CancellationToken,
}
enum Event {
    Seen(Seen),
    End(usize, bool),
}
#[derive(Clone)]
struct ControlledServer {
    events: mpsc::UnboundedSender<Event>,
    streams: Arc<AtomicUsize>,
    shutdown: CancellationToken,
}
impl ControlledServer {
    fn open<K, S>(
        &self,
        wire: Wire,
        mut requests: Streaming<K>,
        response: impl Fn(K) -> (Bytes, Bytes, u64, S) + Send + 'static,
    ) -> ResponseStream<S>
    where
        K: Send + 'static,
        S: Send + 'static,
    {
        let stream = self.streams.fetch_add(1, Ordering::SeqCst);
        let events = self.events.clone();
        let terminal = self.shutdown.child_token();
        let body_terminal = terminal.clone();
        let shutdown = self.shutdown.clone();
        let (tx, mut rx) = mpsc::unbounded_channel();
        // The incoming body deliberately outlives the response body. Client
        // terminal cleanup must close it without relying on tonic to do so.
        #[allow(clippy::disallowed_methods)] // Test-local server ownership probe.
        tokio::spawn(async move {
            loop {
                let item = tokio::select! {
                    _ = shutdown.cancelled() => break,
                    item = requests.next() => item,
                };
                let Some(Ok(request)) = item else {
                    let _ = events.send(Event::End(stream, item.is_none()));
                    break;
                };
                let (address, target, request_id, response) = response(request);
                let (reply, wait) = oneshot::channel();
                let _ = events.send(Event::Seen(Seen {
                    wire,
                    stream,
                    address,
                    target,
                    request_id,
                    reply,
                    terminal: terminal.clone(),
                }));
                let tx = tx.clone();
                let shutdown = shutdown.clone();
                #[allow(clippy::disallowed_methods)] // Independently controlled wire response.
                tokio::spawn(async move {
                    tokio::select! {
                        _ = shutdown.cancelled() => {},
                        result = wait => if result.is_ok() { let _ = tx.send(response); },
                    }
                });
            }
        });
        Box::pin(async_stream::stream! {
            loop {
                tokio::select! {
                    _ = body_terminal.cancelled() => break,
                    response = rx.recv() => match response {
                        Some(response) => yield Ok(response),
                        None => break,
                    },
                }
            }
        })
    }
}
fn wire_fragment() -> model_v1::Fragment {
    model_v1::Fragment {
        flags: 0,
        size_payload: TEST_PAYLOAD.len() as u32,
        size_content: TEST_PAYLOAD.len() as u64,
    }
}
#[tonic::async_trait]
impl StorageServiceV1 for ControlledServer {
    type GetStream = ResponseStream<storage_v1::GetResponse>;
    async fn get(
        &self,
        request: Request<Streaming<model_v1::Address>>,
    ) -> Result<Response<Self::GetStream>, Status> {
        Ok(Response::new(self.open(
            Wire::Get,
            request.into_inner(),
            |address| {
                let key = address.hash.clone();
                (
                    key,
                    Bytes::new(),
                    0,
                    storage_v1::GetResponse {
                        address: Some(address),
                        fragment: Some(wire_fragment()),
                        payload: Bytes::from_static(TEST_PAYLOAD),
                        status: None,
                    },
                )
            },
        )))
    }
    type GetMetadataStream = ResponseStream<storage_v1::GetResponse>;
    async fn get_metadata(
        &self,
        request: Request<Streaming<model_v1::Address>>,
    ) -> Result<Response<Self::GetMetadataStream>, Status> {
        Ok(Response::new(self.open(
            Wire::Metadata,
            request.into_inner(),
            |address| {
                let key = address.hash.clone();
                (
                    key,
                    Bytes::new(),
                    0,
                    storage_v1::GetResponse {
                        address: Some(address),
                        fragment: Some(wire_fragment()),
                        payload: Bytes::new(),
                        status: None,
                    },
                )
            },
        )))
    }
    type PutStream = ResponseStream<storage_v1::PutResponse>;
    async fn put(
        &self,
        request: Request<Streaming<storage_v1::PutRequest>>,
    ) -> Result<Response<Self::PutStream>, Status> {
        Ok(Response::new(self.open(
            Wire::Put,
            request.into_inner(),
            |request| {
                (
                    request.address.as_ref().unwrap().hash.clone(),
                    Bytes::new(),
                    0,
                    storage_v1::PutResponse {
                        address: request.address,
                        status: None,
                    },
                )
            },
        )))
    }
    type CopyStream = ResponseStream<storage_v1::CopyResponse>;
    async fn copy(
        &self,
        request: Request<Streaming<storage_v1::CopyRequest>>,
    ) -> Result<Response<Self::CopyStream>, Status> {
        Ok(Response::new(self.open(
            Wire::Copy,
            request.into_inner(),
            |request| {
                (
                    request.source_address.as_ref().unwrap().hash.clone(),
                    request.target_context.clone(),
                    0,
                    storage_v1::CopyResponse {
                        source_repository_id: request.source_repository_id,
                        source_address: request.source_address,
                        status: None,
                    },
                )
            },
        )))
    }
    type GetResolvedStream = ResponseStream<storage_v1::GetResolvedResponse>;
    async fn get_resolved(
        &self,
        request: Request<Streaming<storage_v1::GetResolvedRequest>>,
    ) -> Result<Response<Self::GetResolvedStream>, Status> {
        Ok(Response::new(self.open(
            Wire::GetResolved,
            request.into_inner(),
            |request| {
                (
                    request.key.as_ref().unwrap().hash.clone(),
                    Bytes::new(),
                    request.request_id,
                    storage_v1::GetResolvedResponse {
                        request_id: request.request_id,
                        resolved: Bytes::from_static(&[0x22; 32]),
                        fragment: Some(wire_fragment()),
                        payload: Bytes::from_static(TEST_PAYLOAD),
                        ..Default::default()
                    },
                )
            },
        )))
    }
    type PutResolvedStream = ResponseStream<storage_v1::PutResolvedResponse>;
    async fn put_resolved(
        &self,
        request: Request<Streaming<storage_v1::PutResolvedRequest>>,
    ) -> Result<Response<Self::PutResolvedStream>, Status> {
        Ok(Response::new(self.open(
            Wire::PutResolved,
            request.into_inner(),
            |request| {
                (
                    request.address.as_ref().unwrap().hash.clone(),
                    Bytes::new(),
                    request.request_id,
                    storage_v1::PutResolvedResponse {
                        request_id: request.request_id,
                        ..Default::default()
                    },
                )
            },
        )))
    }
    async fn query(
        &self,
        _request: Request<storage_v1::QueryRequest>,
    ) -> Result<Response<storage_v1::QueryResponse>, Status> {
        Err(Status::unimplemented("not used by this test"))
    }

    async fn verify(
        &self,
        _request: Request<storage_v1::VerifyRequest>,
    ) -> Result<Response<storage_v1::VerifyResponse>, Status> {
        Err(Status::unimplemented("not used by this test"))
    }

    async fn mutable_load(
        &self,
        _request: Request<storage_v1::MutableLoadRequest>,
    ) -> Result<Response<storage_v1::MutableLoadResponse>, Status> {
        Err(Status::unimplemented("not used by this test"))
    }

    async fn mutable_store(
        &self,
        _request: Request<storage_v1::MutableStoreRequest>,
    ) -> Result<Response<storage_v1::MutableStoreResponse>, Status> {
        Err(Status::unimplemented("not used by this test"))
    }

    async fn mutable_compare_and_swap(
        &self,
        _request: Request<storage_v1::MutableCompareAndSwapRequest>,
    ) -> Result<Response<storage_v1::MutableCompareAndSwapResponse>, Status> {
        Err(Status::unimplemented("not used by this test"))
    }
}

struct Fixture {
    service: Arc<StorageService>,
    events: mpsc::UnboundedReceiver<Event>,
    pending: VecDeque<Seen>,
    ended: Vec<usize>,
    streams: Arc<AtomicUsize>,
    shutdown: CancellationToken,
}
impl Fixture {
    async fn new() -> Self {
        let shutdown = CancellationToken::new();
        let streams = Arc::new(AtomicUsize::new(0));
        let (tx, events) = mpsc::unbounded_channel();
        let server = ControlledServer {
            events: tx,
            streams: streams.clone(),
            shutdown: shutdown.clone(),
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let stopped = shutdown.clone();
        #[allow(clippy::disallowed_methods)] // Local mock server, explicitly stopped by Fixture.
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(StorageServiceServer::new(server))
                .serve_with_incoming_shutdown(
                    tokio_stream::wrappers::TcpListenerStream::new(listener),
                    stopped.cancelled_owned(),
                )
                .await
                .unwrap();
        });
        let channel = tonic::transport::Channel::from_shared(format!("http://{addr}"))
            .unwrap()
            .connect()
            .await
            .unwrap();
        let channel = tower::ServiceBuilder::new()
            .layer(lore_transport::grpc::RequestLoggerLayer {})
            .service(channel);
        let connection = Arc::new(lore_transport::grpc::GRPCConnection::for_test(
            format!("http://{addr}").parse().unwrap(),
            channel,
        ));
        Self {
            service: Arc::new(StorageService::new(connection)),
            events,
            pending: VecDeque::new(),
            ended: vec![],
            streams,
            shutdown,
        }
    }
    async fn seen(&mut self) -> Seen {
        if let Some(item) = self.pending.pop_front() {
            return item;
        }
        loop {
            match bounded(self.events.recv()).await.unwrap() {
                Event::Seen(item) => return item,
                Event::End(stream, clean) => {
                    assert!(
                        clean,
                        "request body must end with EOF, not a transport reset"
                    );
                    self.ended.push(stream);
                }
            }
        }
    }
    async fn eof(&mut self, expected: usize) {
        while !self.ended.contains(&expected) {
            match bounded(self.events.recv()).await.unwrap() {
                Event::Seen(item) => self.pending.push_back(item),
                Event::End(stream, clean) => {
                    assert!(
                        clean,
                        "request body must end with EOF, not a transport reset"
                    );
                    self.ended.push(stream);
                }
            }
        }
    }
    async fn warm(&mut self, ctx: &GrpcSessionContext, wire: Wire) {
        let call = request(self.service.clone(), ctx.clone(), wire, 0x11, 0x31);
        tokio::pin!(call);
        assert!(futures::poll!(&mut call).is_pending());
        let seen = self.seen().await;
        assert_eq!(seen.wire, wire);
        seen.reply.send(()).unwrap();
        bounded(call).await.unwrap();
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}
async fn bounded<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(5), future)
        .await
        .expect("wire progress")
}
fn context() -> GrpcSessionContext {
    GrpcSessionContext::new(
        Partition::from(Context::from([0x11; 16])),
        "ownership-test".to_string(),
        String::new(),
    )
}
fn request(
    service: Arc<StorageService>,
    ctx: GrpcSessionContext,
    wire: Wire,
    key: u8,
    target: u8,
) -> futures::future::BoxFuture<'static, Result<(), ProtocolError>> {
    Box::pin(async move {
        let address = Address::zero_context_hash(Hash::from([key; 32]));
        let fragment = Fragment {
            flags: 0,
            size_payload: TEST_PAYLOAD.len() as u32,
            size_content: TEST_PAYLOAD.len() as u64,
        };
        match wire {
            Wire::Get => service.get(7, &ctx, &address).await.map(|_| ()),
            Wire::Metadata => service.get_metadata(7, &ctx, &address).await.map(|_| ()),
            Wire::Put => {
                service
                    .put(
                        7,
                        &ctx,
                        address,
                        fragment,
                        Some(Bytes::from_static(TEST_PAYLOAD)),
                    )
                    .await
            }
            Wire::Copy => {
                service
                    .copy(7, &ctx, ctx.partition, address, Context::from([target; 16]))
                    .await
            }
            Wire::GetResolved => service
                .get_resolved(7, &ctx, &address.hash, &address.context, 0)
                .await
                .map(|_| ()),
            Wire::PutResolved => {
                service
                    .put_resolved(
                        7,
                        &ctx,
                        &address.hash,
                        address,
                        fragment,
                        Some(Bytes::from_static(TEST_PAYLOAD)),
                    )
                    .await
            }
        }
    })
}

fn owner() -> (IoLease, std::sync::Weak<()>) {
    let owner = Arc::new(());
    let weak = Arc::downgrade(&owner);
    (IoLease::new(owner), weak)
}
async fn released(owner: &std::sync::Weak<()>) {
    bounded(async {
        while owner.upgrade().is_some() {
            tokio::task::yield_now().await;
        }
    })
    .await;
}

#[tokio::test]
async fn idle_cached_streams_half_close_all_six_request_bodies_on_stop() {
    let mut fixture = Fixture::new().await;
    let ctx = context();
    for wire in [
        Wire::Get,
        Wire::Metadata,
        Wire::Put,
        Wire::Copy,
        Wire::GetResolved,
        Wire::PutResolved,
    ] {
        fixture.warm(&ctx, wire).await;
    }
    assert_eq!(fixture.streams.load(Ordering::SeqCst), 6);
    fixture.service.remove_session_streams(7, &ctx);
    for stream in 0..6 {
        fixture.eof(stream).await;
    }
    assert!(
        request(fixture.service.clone(), ctx, Wire::Get, 0x66, 0)
            .await
            .is_err()
    );
    assert_eq!(fixture.streams.load(Ordering::SeqCst), 6);
}

#[tokio::test]
async fn cached_second_request_retains_its_own_lease_after_waiter_cancellation() {
    let mut fixture = Fixture::new().await;
    let ctx = context();
    let (first, first_weak) = owner();
    let mut warm = Box::pin(first.scope(request(
        fixture.service.clone(),
        ctx.clone(),
        Wire::Get,
        1,
        0,
    )));
    assert!(futures::poll!(&mut warm).is_pending());
    fixture.seen().await.reply.send(()).unwrap();
    bounded(warm).await.unwrap();
    drop(first);
    released(&first_weak).await;
    let (second, second_weak) = owner();
    let mut call = Box::pin(second.scope(request(fixture.service.clone(), ctx, Wire::Get, 2, 0)));
    assert!(futures::poll!(&mut call).is_pending());
    let seen = fixture.seen().await;
    drop(call);
    drop(second);
    assert!(second_weak.upgrade().is_some());
    seen.reply.send(()).unwrap();
    released(&second_weak).await;
    assert_eq!(fixture.streams.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn coalesced_waiters_keep_distinct_leases_until_single_wire_reply() {
    let mut fixture = Fixture::new().await;
    let ctx = context();
    fixture.warm(&ctx, Wire::Get).await;
    let (a, a_weak) = owner();
    let (b, b_weak) = owner();
    let mut first = Box::pin(a.scope(request(
        fixture.service.clone(),
        ctx.clone(),
        Wire::Get,
        2,
        0,
    )));
    assert!(futures::poll!(&mut first).is_pending());
    let seen = fixture.seen().await;
    let mut second = Box::pin(b.scope(request(
        fixture.service.clone(),
        ctx.clone(),
        Wire::Get,
        2,
        0,
    )));
    assert!(futures::poll!(&mut second).is_pending());
    let mut fence = Box::pin(request(fixture.service.clone(), ctx, Wire::Get, 3, 0));
    assert!(futures::poll!(&mut fence).is_pending());
    let fence_seen = fixture.seen().await;
    assert_eq!(
        fence_seen.address,
        Bytes::from_static(&[3; 32]),
        "FIFO fence proves second waiter was registered without another wire request"
    );
    drop(first);
    drop(second);
    drop(a);
    drop(b);
    assert!(a_weak.upgrade().is_some());
    assert!(b_weak.upgrade().is_some());
    seen.reply.send(()).unwrap();
    released(&a_weak).await;
    released(&b_weak).await;
    fence_seen.reply.send(()).unwrap();
    bounded(fence).await.unwrap();
}

#[tokio::test]
async fn response_eof_ends_old_body_and_retains_operation_through_retry() {
    let mut fixture = Fixture::new().await;
    let (lease, weak) = owner();
    let mut call =
        Box::pin(lease.scope(request(fixture.service.clone(), context(), Wire::Get, 4, 0)));
    assert!(futures::poll!(&mut call).is_pending());
    let first = fixture.seen().await;
    first.terminal.cancel();
    let next = tokio::select! {
        result = &mut call => panic!("operation ended before reissue: {result:?}"),
        next = fixture.seen() => next,
    };
    assert_ne!(first.stream, next.stream);
    fixture.eof(first.stream).await;
    drop(first);
    drop(lease);
    assert!(weak.upgrade().is_some());
    next.reply.send(()).unwrap();
    bounded(call).await.unwrap();
    released(&weak).await;
}

#[tokio::test]
async fn resolved_requests_keep_distinct_ids_and_canceled_request_ownership() {
    for wire in [Wire::GetResolved, Wire::PutResolved] {
        let mut fixture = Fixture::new().await;
        let ctx = context();
        fixture.warm(&ctx, wire).await;
        let mut first = Box::pin(request(fixture.service.clone(), ctx.clone(), wire, 5, 0));
        assert!(futures::poll!(&mut first).is_pending());
        let a = fixture.seen().await;
        let (lease, weak) = owner();
        let mut second = Box::pin(lease.scope(request(fixture.service.clone(), ctx, wire, 5, 0)));
        assert!(futures::poll!(&mut second).is_pending());
        let b = fixture.seen().await;
        assert_ne!(a.request_id, 0);
        assert_ne!(a.request_id, b.request_id);
        drop(second);
        drop(lease);
        assert!(weak.upgrade().is_some());
        a.reply.send(()).unwrap();
        bounded(first).await.unwrap();
        assert!(weak.upgrade().is_some());
        b.reply.send(()).unwrap();
        released(&weak).await;
    }
}

#[tokio::test]
async fn graceful_stop_yields_accepted_queued_copy_before_request_eof() {
    let mut fixture = Fixture::new().await;
    let ctx = context();
    fixture.warm(&ctx, Wire::Copy).await;
    let mut a = Box::pin(request(
        fixture.service.clone(),
        ctx.clone(),
        Wire::Copy,
        8,
        1,
    ));
    assert!(futures::poll!(&mut a).is_pending());
    let seen_a = fixture.seen().await;
    let (lease, weak) = owner();
    let mut b = Box::pin(lease.scope(request(
        fixture.service.clone(),
        ctx.clone(),
        Wire::Copy,
        8,
        2,
    )));
    assert!(futures::poll!(&mut b).is_pending());
    let mut fence = Box::pin(request(
        fixture.service.clone(),
        ctx.clone(),
        Wire::Copy,
        9,
        3,
    ));
    assert!(futures::poll!(&mut fence).is_pending());
    let seen_fence = fixture.seen().await;
    assert_eq!(seen_fence.address, Bytes::from_static(&[9; 32]));
    fixture.service.remove_session_streams(7, &ctx);
    drop(b);
    drop(lease);
    assert!(weak.upgrade().is_some());
    seen_a.reply.send(()).unwrap();
    bounded(a).await.unwrap();
    let seen_b = fixture.seen().await;
    assert_eq!(seen_b.target, Bytes::from_static(&[2; 16]));
    assert_eq!(seen_b.stream, seen_fence.stream);
    // Half-close follows the last accepted wire yield, not its eventual reply.
    fixture.eof(seen_b.stream).await;
    assert!(weak.upgrade().is_some());
    seen_b.reply.send(()).unwrap();
    released(&weak).await;
    seen_fence.reply.send(()).unwrap();
    bounded(fence).await.unwrap();
}

#[tokio::test]
async fn terminal_failure_discards_queued_copy_and_releases_canceled_operations() {
    let mut fixture = Fixture::new().await;
    let ctx = context();
    fixture.warm(&ctx, Wire::Copy).await;
    let (a_owner, a_weak) = owner();
    let (b_owner, b_weak) = owner();
    let mut a = Box::pin(a_owner.scope(request(
        fixture.service.clone(),
        ctx.clone(),
        Wire::Copy,
        8,
        1,
    )));
    assert!(futures::poll!(&mut a).is_pending());
    let seen_a = fixture.seen().await;
    let mut b = Box::pin(b_owner.scope(request(
        fixture.service.clone(),
        ctx.clone(),
        Wire::Copy,
        8,
        2,
    )));
    assert!(futures::poll!(&mut b).is_pending());
    let mut fence = Box::pin(request(fixture.service.clone(), ctx, Wire::Copy, 9, 3));
    assert!(futures::poll!(&mut fence).is_pending());
    let seen_fence = fixture.seen().await;
    assert_eq!(seen_fence.address, Bytes::from_static(&[9; 32]));
    drop(a);
    drop(b);
    drop(fence);
    drop(a_owner);
    drop(b_owner);
    assert!(a_weak.upgrade().is_some());
    assert!(b_weak.upgrade().is_some());
    seen_a.terminal.cancel();
    fixture.eof(seen_a.stream).await;
    released(&a_weak).await;
    released(&b_weak).await;
    assert!(
        fixture.pending.is_empty(),
        "terminal failure must not yield queued copy B"
    );
    assert_eq!(fixture.streams.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn stopped_generation_refuses_every_verb_without_opening_a_stream() {
    let fixture = Fixture::new().await;
    let ctx = context();
    fixture.service.remove_session_streams(7, &ctx);
    for wire in [
        Wire::Get,
        Wire::Metadata,
        Wire::Put,
        Wire::Copy,
        Wire::GetResolved,
        Wire::PutResolved,
    ] {
        let error = bounded(request(fixture.service.clone(), ctx.clone(), wire, 1, 2))
            .await
            .unwrap_err();
        assert!(
            !matches!(error, ProtocolError::Disconnected(_)),
            "stopped generation must not trigger channel reconnection"
        );
    }
    assert_eq!(fixture.streams.load(Ordering::SeqCst), 0);
}
