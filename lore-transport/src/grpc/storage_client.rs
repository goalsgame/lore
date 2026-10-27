// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use std::collections::HashMap;
use std::collections::VecDeque;
use std::collections::hash_map::Entry;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use bytes::BufMut;
use bytes::Bytes;
use bytes::BytesMut;
use dashmap::DashMap;
use lore_base::error::Disconnected;
use lore_base::error::NotFound;
use lore_base::error::SlowDown;
use lore_base::lore_debug;
use lore_base::lore_error;
use lore_base::types::Address;
use lore_base::types::Context;
use lore_base::types::Fragment;
use lore_base::types::Hash;
use lore_base::types::HealResult;
use lore_base::types::KeyType;
use lore_base::types::Partition;
use lore_base::types::VerifyResult;
use lore_error_set::prelude::*;
use lore_proto::lore::model::v1 as model_v1;
use lore_proto::lore::storage::v1 as storage_v1;
use lore_proto::lore::storage::v1::storage_service_client::StorageServiceClient;
use tokio::sync::Semaphore;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio_stream::StreamExt;
use tonic::metadata::MetadataValue;

use super::CORRELATION_ID_HEADER;
use super::PARTITION_ID_KEY;
use super::REPOSITORY_ID_KEY;
use crate::error::ProtocolError;

mod request_ownership;
mod stream_terminal;

use request_ownership::Item as StreamItem;
pub(super) use request_ownership::Operation;
use request_ownership::PreparedStream;
use request_ownership::ReaderCleanup;
use request_ownership::Reply;
use request_ownership::SessionLife;
use stream_terminal::StreamLife;

/// Translate a response's in-band `status` into a [`ProtocolError`], or `None` when the item
/// succeeded. Absence means `OK`, so a peer that predates the field reads as success.
fn item_status_error(
    status: Option<&lore_proto::lore::model::v1::ItemStatus>,
) -> Option<ProtocolError> {
    let status = status?;
    let code = tonic::Code::from_i32(status.code as i32);
    if code == tonic::Code::Ok {
        return None;
    }
    Some(ProtocolError::from(tonic::Status::new(
        code,
        status.message.clone(),
    )))
}

const STREAM_WRITE_BUFFER_SIZE: usize = 32 * 1024;
const INFLIGHT_COMMAND_LIMIT: usize = 10000;

/// Bound on stream-level reissues before handing off to connection-level reconnect.
///
/// Reissuing here only covers a stream dying on an otherwise healthy channel, which needs no
/// backoff. Anything the channel itself is responsible for belongs to
/// `GRPCConnection::reconnect`, which owns the epoch, single-flight guard and backoff — so
/// this only has to be large enough to absorb a server resetting individual streams.
const MAX_STREAM_REISSUES: usize = 8;

/// Session context for gRPC metadata injection. Cached at `session_start` time.
#[derive(Clone)]
pub struct GrpcSessionContext {
    pub partition: Partition,
    pub correlation_id: String,
    pub auth_token: String,
    life: Arc<SessionLife>,
}

impl GrpcSessionContext {
    pub(super) fn check_open(&self) -> Result<(), ProtocolError> {
        self.life.check_open()
    }
    pub fn new(partition: Partition, correlation_id: String, auth_token: String) -> Self {
        Self {
            partition,
            correlation_id,
            auth_token,
            life: SessionLife::new(),
        }
    }
}

/// Which streaming RPC a cached stream belongs to.
///
/// Part of the stream-cache key so rotating a dead Get stream leaves the session's
/// `GetMetadata`, Put and Copy streams untouched — they're independent RPCs and one dying
/// says nothing about the others.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum Verb {
    Get,
    GetMetadata,
    Put,
    Copy,
    GetResolved,
    PutResolved,
}

impl Verb {
    fn label(self) -> &'static str {
        match self {
            Verb::Get => "get",
            Verb::GetMetadata => "get_metadata",
            Verb::Put => "put",
            Verb::Copy => "copy",
            Verb::GetResolved => "get_resolved",
            Verb::PutResolved => "put_resolved",
        }
    }
}

/// A live stream's request channel plus whether its RPC ever opened.
///
/// `opened` is what lets `StreamCache::request` tell a stream that died mid-flight from one
/// that never established, since the sender is handed out before the RPC is attempted. By
/// the time a request observes a failure the flag has settled: either the reader task
/// answered it, or the task exited and dropped the channel.
struct StreamState<K, S> {
    sender: mpsc::Sender<StreamItem<K, S>>,
    opened: Arc<AtomicBool>,
    session: Arc<SessionLife>,
    stream: Arc<StreamLife<K, S>>,
}

type StreamHandle<K, S> = Arc<StreamState<K, S>>;

/// Keyed by the address a request reads or writes: the address the server echoes back, or for a
/// copy the target, see [`CopyRouting`]. A `Vec` per key so concurrent requests for the same
/// address coalesce onto one round trip and all get woken by its response.
type Inflight<S> = DashMap<Address, Vec<Reply<S>>>;

/// Returns true for the first waiter on `key`, meaning the caller should put the request on
/// the wire; later waiters ride along on that one round trip.
fn register<S>(inflight: &Inflight<S>, key: Address, sender: Reply<S>) -> bool {
    let mut first = false;
    #[allow(clippy::disallowed_methods)] // Brief write lock; no await while held.
    inflight
        .entry(key)
        .or_insert_with(|| {
            first = true;
            Vec::new()
        })
        .push(sender);
    first
}

/// Answering explicitly rather than letting the senders drop: a dropped sender reaches the
/// caller as an opaque `RecvError` that the storage layer can only classify as an internal
/// fault, whereas a real error tells it whether a retry is worth attempting.
fn fail_inflight<S>(inflight: &Inflight<S>, verb: Verb, err: &ProtocolError) {
    let mut failed = 0usize;
    loop {
        // Copy a key only; release the iterator/shard guard before removing or
        // disposing replies whose final IO owner can run arbitrary destructors.
        let key = inflight.iter().next().map(|entry| *entry.key());
        let Some(key) = key else {
            break;
        };
        if let Some((_, senders)) = inflight.remove(&key) {
            for sender in senders {
                failed += 1;
                sender.send(Err(err.clone()));
            }
        }
    }
    if failed > 0 {
        lore_debug!(
            "{} stream ended with {failed} request(s) outstanding: {err}",
            verb.label()
        );
    }
}
fn fail_buffered<K, S>(life: &StreamLife<K, S>, error: &ProtocolError) {
    while let Some((payload, reply)) = life.take_failed() {
        drop(payload);
        reply.send(Err(error.clone()));
    }
}
fn finish_address<K, S>(
    life: &StreamLife<K, S>,
    inflight: &Inflight<S>,
    verb: Verb,
    error: ProtocolError,
) {
    let error = life.terminate(error);
    fail_buffered(life, &error);
    fail_inflight(inflight, verb, &error);
}
fn finish_resolved<K, S>(
    life: &StreamLife<K, S>,
    pending: &DashMap<u64, Reply<S>>,
    error: ProtocolError,
) {
    let error = life.terminate(error);
    fail_buffered(life, &error);
    loop {
        let key = pending.iter().next().map(|entry| *entry.key());
        let Some(key) = key else {
            break;
        };
        if let Some((_, reply)) = pending.remove(&key) {
            reply.send(Err(error.clone()));
        }
    }
}

/// Lets one generic pump serve every streaming verb: the only per-verb differences are which
/// field carries the routing address and where an in-band per-item failure lives.
trait StreamResponse: Sized {
    type Success: Clone + Send + 'static;

    /// The key this response's request was registered under.
    fn key(&self) -> Option<Address>;

    /// Preserves the server's status code, which callers pattern-match on — a remote
    /// `NotFound` is a routine answer on the metadata path rather than a fault.
    ///
    /// An absent status is success: the field postdates the original response, so a peer
    /// without it signals a per-item failure by ending the stream instead.
    fn into_result(self) -> Result<Self::Success, ProtocolError>;
}

impl StreamResponse for storage_v1::GetResponse {
    type Success = (model_v1::Fragment, Bytes);

    fn key(&self) -> Option<Address> {
        self.address.as_ref().map(Address::from)
    }

    fn into_result(self) -> Result<Self::Success, ProtocolError> {
        if let Some(status) = self.status.as_ref().filter(|status| !status.is_ok()) {
            return Err(ProtocolError::from(tonic::Status::from(status)));
        }
        let fragment = self
            .fragment
            .ok_or_else(|| ProtocolError::internal("get: successful response has no fragment"))?;
        Ok((fragment, self.payload))
    }
}

impl StreamResponse for storage_v1::PutResponse {
    type Success = ();

    fn key(&self) -> Option<Address> {
        self.address.as_ref().map(Address::from)
    }

    fn into_result(self) -> Result<Self::Success, ProtocolError> {
        match self.status.as_ref().filter(|status| !status.is_ok()) {
            Some(status) => Err(ProtocolError::from(tonic::Status::from(status))),
            None => Ok(()),
        }
    }
}

impl StreamResponse for storage_v1::CopyResponse {
    type Success = ();

    fn key(&self) -> Option<Address> {
        self.source_address.as_ref().map(Address::from)
    }

    fn into_result(self) -> Result<Self::Success, ProtocolError> {
        match self.status.as_ref().filter(|status| !status.is_ok()) {
            Some(status) => Err(ProtocolError::from(tonic::Status::from(status))),
            None => Ok(()),
        }
    }
}

/// The copies on a copy stream, by source address. A copy response names only its source, so a
/// source address is on the wire for one target at a time, and a copy of it to another target
/// waits for the response to the one before.
#[derive(Default)]
struct CopyRouting {
    /// The target each source address on the wire is being copied to.
    wire: HashMap<Address, Address>,
    /// The copies waiting for a source address on the wire, in arrival order.
    queued: HashMap<Address, VecDeque<storage_v1::CopyRequest>>,
    ready: VecDeque<storage_v1::CopyRequest>,
    not_yielded: usize,
}

impl CopyRouting {
    /// The source and target addresses of `request`. The target is in the session's partition,
    /// with the source's hash and the target context, or the source's context when it names
    /// none.
    fn addresses(request: &storage_v1::CopyRequest) -> Option<(Address, Address)> {
        let source = Address::from(request.source_address.as_ref()?);
        let context = match request.target_context.as_ref() {
            [] => source.context,
            bytes => Context::from(<[u8; 16]>::try_from(bytes).ok()?),
        };
        Some((
            source,
            Address {
                hash: source.hash,
                context,
            },
        ))
    }

    /// Put `request` on the wire for `source`, returning it, or queue it behind the copy of
    /// `source` on the wire.
    fn admit(
        &mut self,
        source: Address,
        target: Address,
        request: storage_v1::CopyRequest,
    ) -> Option<storage_v1::CopyRequest> {
        self.not_yielded = self
            .not_yielded
            .checked_add(1)
            .expect("accepted copies are bounded by retained operation permits");
        match self.wire.entry(source) {
            Entry::Occupied(_) => {
                self.queued.entry(source).or_default().push_back(request);
                None
            }
            Entry::Vacant(wire) => {
                wire.insert(target);
                Some(request)
            }
        }
    }

    /// Take the answered copy of `source` off the wire, returning its target and the next copy
    /// of `source`, which goes on the wire in its place.
    fn answered(&mut self, source: Address) -> (Option<Address>, Option<storage_v1::CopyRequest>) {
        let target = self.wire.remove(&source);
        let Some(queue) = self.queued.get_mut(&source) else {
            return (target, None);
        };
        let next = queue.pop_front();
        if queue.is_empty() {
            self.queued.remove(&source);
        }
        if let Some((_, next_target)) = next.as_ref().and_then(Self::addresses) {
            self.wire.insert(source, next_target);
        }
        (target, next)
    }
}

/// A per-item failure arrives in-band on an `Ok` message and costs only that request. An
/// `Err` is terminal by construction — tonic surfaces a stream status once and then reports
/// the stream exhausted — so it ends the loop and everything still outstanding fails.
///
/// Those outstanding requests are failed as `Disconnected` whatever the terminal code says,
/// because a stream status describes the stream rather than any one request on it. That is
/// also what lets `StreamCache::request` replay them: the ordinary ways a connection dies
/// arrive as `Internal` or `Cancelled` (`Status::from_h2_error`), neither of which a caller
/// would otherwise retry. The real status is kept on the error's trace.
///
/// `route` maps the address a response echoes to the key its request was registered under.
async fn pump_responses<R>(
    mut stream: tonic::Streaming<R>,
    inflight: Arc<Inflight<R::Success>>,
    verb: Verb,
    mut route: impl FnMut(Address) -> Option<Address>,
) -> ProtocolError
where
    R: StreamResponse,
{
    let mut terminal = ProtocolError::from(Disconnected);

    while let Some(message) = stream.next().await {
        let response = match message {
            Ok(response) => response,
            Err(status) => {
                let mut err = ProtocolError::from(Disconnected);
                err.push_trace(lore_error_set::Location::with_context(
                    file!(),
                    line!(),
                    column!(),
                    Arc::from(format!("{} stream terminated: {status}", verb.label())),
                ));
                terminal = err;
                break;
            }
        };

        let Some(address) = response.key() else {
            lore_error!("{} response missing address", verb.label());
            continue;
        };
        let Some((_, senders)) = route(address).and_then(|key| inflight.remove(&key)) else {
            lore_error!(
                "{} received unexpected result for address {address}",
                verb.label()
            );
            continue;
        };

        let result = response.into_result();
        let mut senders = senders;
        if let Some(last) = senders.pop() {
            for sender in senders {
                sender.send(result.clone());
            }
            last.send(result);
        }
    }

    terminal
}

/// The hot path is a single `DashMap` read plus an `Arc` clone — no write lock and no second
/// level of indirection. A dead stream announces itself by failing the send, so nothing polls
/// for liveness and the reader tasks never touch this map: the request path is its only
/// mutator, which is what makes rotation race-free.
struct StreamCache<K, S> {
    streams: DashMap<(u32, Verb), StreamHandle<K, S>>,
}

struct Installed<K, S> {
    handle: Result<StreamHandle<K, S>, ProtocolError>,
    start: Option<PreparedStream<K, S>>,
    discard: Option<PreparedStream<K, S>>,
    retired: Option<StreamHandle<K, S>>,
}
impl<K: Clone, S> StreamCache<K, S> {
    fn new() -> Self {
        Self {
            streams: DashMap::new(),
        }
    }
    fn remove(&self, session_id: u32, verb: Verb, ctx: &GrpcSessionContext) {
        let removed = self.streams.remove_if(&(session_id, verb), |_, current| {
            Arc::ptr_eq(&current.session, &ctx.life)
        });
        drop(removed);
    }
    fn discard(&self, key: (u32, Verb), failed: &StreamHandle<K, S>) {
        let removed = self
            .streams
            .remove_if(&key, |_, current| Arc::ptr_eq(current, failed));
        drop(removed);
    }
    fn install(
        &self,
        key: (u32, Verb),
        failed: Option<&StreamHandle<K, S>>,
        ctx: &GrpcSessionContext,
        fresh: PreparedStream<K, S>,
    ) -> Installed<K, S> {
        use dashmap::mapref::entry::Entry;
        // Install is synchronous; callbacks, task starts and retired resource
        // destruction all occur after the caller releases Life/cache guards.
        #[allow(clippy::disallowed_methods)]
        match self.streams.entry(key) {
            Entry::Occupied(mut occupied) => {
                if !Arc::ptr_eq(&occupied.get().session, &ctx.life) {
                    return Installed {
                        handle: Err(request_ownership::stopped()),
                        start: None,
                        discard: Some(fresh),
                        retired: None,
                    };
                }
                if failed.is_none_or(|failed| !Arc::ptr_eq(occupied.get(), failed)) {
                    return Installed {
                        handle: Ok(occupied.get().clone()),
                        start: None,
                        discard: Some(fresh),
                        retired: None,
                    };
                }
                let handle = fresh.handle.clone();
                let retired = occupied.insert(handle.clone());
                Installed {
                    handle: Ok(handle),
                    start: Some(fresh),
                    discard: None,
                    retired: Some(retired),
                }
            }
            Entry::Vacant(vacant) => {
                let handle = fresh.handle.clone();
                vacant.insert(handle.clone());
                Installed {
                    handle: Ok(handle),
                    start: Some(fresh),
                    discard: None,
                    retired: None,
                }
            }
        }
    }
    fn enqueue(
        handle: &StreamHandle<K, S>,
        ctx: &GrpcSessionContext,
        permit: mpsc::Permit<'_, StreamItem<K, S>>,
        payload: &K,
        reply: Reply<S>,
    ) -> Result<(), ProtocolError> {
        let _session = ctx.life.admit()?;
        let _stream = handle.stream.admit()?;
        permit.send((payload.clone(), reply));
        Ok(())
    }
    async fn request(
        &self,
        key: (u32, Verb),
        ctx: &GrpcSessionContext,
        payload: K,
        operation: &Arc<Operation>,
        prepare: impl Fn() -> PreparedStream<K, S>,
    ) -> Result<S, ProtocolError> {
        let mut failed: Option<StreamHandle<K, S>> = None;
        for _ in 0..MAX_STREAM_REISSUES {
            let current = {
                let admission = ctx.life.admit()?;
                let current = self.streams.get(&key).map(|entry| entry.value().clone());
                drop(admission);
                current
            };
            let handle = match (&failed, current) {
                (None, Some(current)) => {
                    if !Arc::ptr_eq(&current.session, &ctx.life) {
                        return Err(request_ownership::stopped());
                    }
                    current
                }
                (_, current) => {
                    drop(current);
                    let fresh = prepare(); // Prepare callbacks run outside Life/cache locks.
                    let admission = ctx.life.admit()?;
                    let installed = self.install(key, failed.as_ref(), ctx, fresh);
                    drop(admission);
                    drop(installed.discard);
                    drop(installed.retired);
                    let handle = installed.handle?;
                    if let Some(start) = installed.start {
                        start.start();
                    }
                    handle
                }
            };
            let answer = {
                let reserved = tokio::select! {
                    _ = ctx.life.wait_closed() => return Err(request_ownership::stopped()),
                    reserved = handle.sender.reserve() => reserved,
                };
                match reserved {
                    Ok(permit) => {
                        let (tx, rx) = oneshot::channel();
                        let reply = Reply::new(tx, operation.clone());
                        match Self::enqueue(&handle, ctx, permit, &payload, reply) {
                            Ok(()) => rx.await.ok(),
                            Err(error) => Some(Err(error)),
                        }
                    }
                    Err(_closed) if !handle.opened.load(Ordering::Relaxed) => {
                        handle.stream.terminal_error().map(Err)
                    }
                    Err(_closed) => None,
                }
            };
            if let Some(result) =
                answer.filter(|answer| !matches!(answer,Err(error) if error.is_disconnected()))
            {
                if !handle.opened.load(Ordering::Relaxed) {
                    self.discard(key, &handle);
                }
                return result;
            }
            ctx.life.check_open()?;
            if !handle.opened.load(Ordering::Relaxed) {
                self.discard(key, &handle);
                return Err(ProtocolError::from(Disconnected));
            }
            failed = Some(handle);
        }
        Err(ProtocolError::from(Disconnected))
    }
}

pub struct StorageService {
    /// Resolved to a client per stream open rather than cached, so a channel rebuilt by
    /// `GRPCConnection::reconnect` is picked up by the next rotation without costing the
    /// request path a lock.
    connection: Arc<super::GRPCConnection>,
    /// Get and `GetMetadata` share a cache: same request and response shape, told apart by
    /// `Verb`.
    get_streams: StreamCache<Address, (model_v1::Fragment, Bytes)>,
    put_streams: StreamCache<storage_v1::PutRequest, ()>,
    copy_streams: StreamCache<storage_v1::CopyRequest, ()>,
    /// Correlates resolved requests with their responses; never handed out as zero, which the
    /// server treats as uncorrelatable and stream-fatal.
    resolved_counter: AtomicU64,
    /// The resolved verbs correlate by `request_id`, not by address: one key can resolve to any
    /// content, so the address is an answer rather than a question. They carry their own reader
    /// instead of `pump_responses`.
    get_resolved_streams:
        StreamCache<storage_v1::GetResolvedRequest, Arc<storage_v1::GetResolvedResponse>>,
    put_resolved_streams:
        StreamCache<storage_v1::PutResolvedRequest, Arc<storage_v1::PutResolvedResponse>>,
    get_put_limiter: Arc<Semaphore>,
}

fn inject_metadata<T>(request: &mut tonic::Request<T>, ctx: &GrpcSessionContext) {
    let md = request.metadata_mut();
    md.insert_bin(
        PARTITION_ID_KEY,
        tonic::metadata::BinaryMetadataValue::from_bytes(ctx.partition.data()),
    );
    md.insert_bin(
        REPOSITORY_ID_KEY,
        tonic::metadata::BinaryMetadataValue::from_bytes(ctx.partition.data()),
    );
    if !ctx.correlation_id.is_empty()
        && let Ok(val) = MetadataValue::from_str(&ctx.correlation_id)
    {
        md.insert(CORRELATION_ID_HEADER, val);
    }
    if !ctx.auth_token.is_empty()
        && let Ok(mut val) = MetadataValue::from_str(&format!("Bearer {}", ctx.auth_token))
    {
        val.set_sensitive(true);
        md.insert("authorization", val);
    }
}

impl StorageService {
    pub fn new(connection: Arc<super::GRPCConnection>) -> Self {
        Self {
            connection,
            get_streams: StreamCache::new(),
            put_streams: StreamCache::new(),
            copy_streams: StreamCache::new(),
            resolved_counter: AtomicU64::new(0),
            get_resolved_streams: StreamCache::new(),
            put_resolved_streams: StreamCache::new(),
            get_put_limiter: Arc::new(Semaphore::new(INFLIGHT_COMMAND_LIMIT)),
        }
    }

    /// Reject new work for this exact generation and initiate graceful request
    /// half-close. Accepted operations retain ownership until reply/terminal;
    /// returning here does not acknowledge synchronous RPC/task drainage.
    pub fn remove_session_streams(&self, session_id: u32, ctx: &GrpcSessionContext) {
        ctx.life.close();
        self.get_streams.remove(session_id, Verb::Get, ctx);
        self.get_streams.remove(session_id, Verb::GetMetadata, ctx);
        self.put_streams.remove(session_id, Verb::Put, ctx);
        self.copy_streams.remove(session_id, Verb::Copy, ctx);
        self.get_resolved_streams
            .remove(session_id, Verb::GetResolved, ctx);
        self.put_resolved_streams
            .remove(session_id, Verb::PutResolved, ctx);
    }
    pub(super) async fn operation(
        &self,
        ctx: &GrpcSessionContext,
    ) -> Result<Arc<Operation>, ProtocolError> {
        Operation::acquire(&self.get_put_limiter, &ctx.life).await
    }

    #[cfg(feature = "test-util")]
    pub async fn get(
        &self,
        session_id: u32,
        ctx: &GrpcSessionContext,
        address: &Address,
    ) -> Result<(Fragment, Bytes), ProtocolError> {
        let operation = self.operation(ctx).await?;
        self.get_with_operation(session_id, ctx, address, &operation)
            .await
    }
    pub(super) async fn get_with_operation(
        &self,
        session_id: u32,
        ctx: &GrpcSessionContext,
        address: &Address,
        operation: &Arc<Operation>,
    ) -> Result<(Fragment, Bytes), ProtocolError> {
        lore_debug!("gRPC get fragment: {}", address);

        let (fragment, payload) = self
            .get_streams
            .request((session_id, Verb::Get), ctx, *address, operation, || {
                self.spawn_get_stream(ctx)
            })
            .await?;

        let fragment = Fragment {
            flags: fragment.flags,
            size_payload: fragment.size_payload,
            size_content: fragment.size_content,
        };

        if let Err(reason) = lore_base::types::validate_fragment_response(&fragment) {
            lore_error!("Invalid fragment in get response {fragment:?}: {reason}");
            return Err(ProtocolError::internal(format!(
                "get: invalid fragment: {reason}"
            )));
        }
        if payload.len() != fragment.size_payload as usize {
            lore_error!(
                "Fragment payload is invalid in get response : {} bytes, expected {}",
                payload.len(),
                fragment.size_payload
            );
            return Err(ProtocolError::internal("get: Invalid payload"));
        }

        Ok((fragment, payload))
    }

    /// Fetch only the fragment metadata for an address. Same wire request as `get` (just an
    /// `Address`), but the server's response carries `Fragment` only — no payload bytes — so
    /// callers that don't need the payload skip the transfer cost. Used by the storage API's
    /// query op for remote-hit metadata lookups.
    ///
    /// A `NotFound` here is an ordinary answer rather than a fault — see
    /// `RemoteImmutableStore::get_metadata`, which maps it to `MatchNone` — so it arrives
    /// in-band and costs only this lookup.
    #[cfg(feature = "test-util")]
    pub async fn get_metadata(
        &self,
        session_id: u32,
        ctx: &GrpcSessionContext,
        address: &Address,
    ) -> Result<Fragment, ProtocolError> {
        let operation = self.operation(ctx).await?;
        self.get_metadata_with_operation(session_id, ctx, address, &operation)
            .await
    }
    pub(super) async fn get_metadata_with_operation(
        &self,
        session_id: u32,
        ctx: &GrpcSessionContext,
        address: &Address,
        operation: &Arc<Operation>,
    ) -> Result<Fragment, ProtocolError> {
        lore_debug!("gRPC get_metadata fragment: {}", address);

        let (fragment, _payload) = self
            .get_streams
            .request(
                (session_id, Verb::GetMetadata),
                ctx,
                *address,
                operation,
                || self.spawn_get_metadata_stream(ctx),
            )
            .await?;

        let fragment = Fragment {
            flags: fragment.flags,
            size_payload: fragment.size_payload,
            size_content: fragment.size_content,
        };

        if let Err(reason) = lore_base::types::validate_fragment_response(&fragment) {
            lore_error!("Invalid fragment in get_metadata response {fragment:?}: {reason}");
            return Err(ProtocolError::internal(format!(
                "get_metadata: invalid fragment: {reason}"
            )));
        }

        Ok(fragment)
    }

    #[cfg(feature = "test-util")]
    pub async fn put(
        &self,
        session_id: u32,
        ctx: &GrpcSessionContext,
        address: Address,
        fragment: Fragment,
        payload: Option<Bytes>,
    ) -> Result<(), ProtocolError> {
        let operation = self.operation(ctx).await?;
        self.put_with_operation(session_id, ctx, address, fragment, payload, &operation)
            .await
    }
    pub(super) async fn put_with_operation(
        &self,
        session_id: u32,
        ctx: &GrpcSessionContext,
        address: Address,
        fragment: Fragment,
        payload: Option<Bytes>,
        operation: &Arc<Operation>,
    ) -> Result<(), ProtocolError> {
        lore_debug!("Put fragment: {address}");

        let request = storage_v1::PutRequest {
            address: Some(address.into()),
            fragment: Some(fragment.into()),
            payload,
        };

        self.put_streams
            .request((session_id, Verb::Put), ctx, request, operation, || {
                self.spawn_put_stream(ctx)
            })
            .await
    }

    pub async fn query(
        &self,
        ctx: &GrpcSessionContext,
        address: &[Address],
    ) -> Result<Bytes, ProtocolError> {
        lore_debug!("Query {} fragments", address.len());

        let request = storage_v1::QueryRequest {
            addresses: address.iter().map(model_v1::Address::from).collect(),
        };
        let mut client = StorageServiceClient::new(self.connection.channel());
        let mut req = tonic::Request::new(request);
        inject_metadata(&mut req, ctx);

        let res = client
            .query(req)
            .await
            .map(|res| res.into_inner())
            .map_err(|err| match err.code() {
                tonic::Code::Unavailable => ProtocolError::from(SlowDown),
                _ => ProtocolError::internal_with_context(err, "query"),
            })?;

        let mut buffer = BytesMut::with_capacity(res.results.len());
        for value in res.results.iter() {
            buffer.put_u8(*value as u8);
        }
        Ok(buffer.freeze())
    }

    pub async fn verify(
        &self,
        ctx: &GrpcSessionContext,
        address: &Address,
        heal: bool,
    ) -> Result<VerifyResult, ProtocolError> {
        lore_debug!("Verify fragment: {address}");

        let request = storage_v1::VerifyRequest {
            address: Some((*address).into()),
            heal,
        };
        let mut client = StorageServiceClient::new(self.connection.channel());
        let mut req = tonic::Request::new(request);
        inject_metadata(&mut req, ctx);

        client
            .verify(req)
            .await
            .map(|res| res.into_inner())
            .map_err(|err| match err.code() {
                tonic::Code::Unavailable => ProtocolError::from(SlowDown),
                tonic::Code::NotFound => ProtocolError::from(NotFound),
                tonic::Code::Unimplemented => ProtocolError::internal("unsupported: verify"),
                _ => ProtocolError::internal_with_context(err, "verify"),
            })
            .map(|res| VerifyResult {
                corrupted: res.corrupted,
                healed: HealResult::from(res.healed),
            })
    }

    #[cfg(feature = "test-util")]
    pub async fn copy(
        &self,
        session_id: u32,
        ctx: &GrpcSessionContext,
        source_partition: Partition,
        source_address: Address,
        target_context: Context,
    ) -> Result<(), ProtocolError> {
        let operation = self.operation(ctx).await?;
        self.copy_with_operation(
            session_id,
            ctx,
            source_partition,
            source_address,
            target_context,
            &operation,
        )
        .await
    }
    pub(super) async fn copy_with_operation(
        &self,
        session_id: u32,
        ctx: &GrpcSessionContext,
        source_partition: Partition,
        source_address: Address,
        target_context: Context,
        operation: &Arc<Operation>,
    ) -> Result<(), ProtocolError> {
        lore_debug!(
            "gRPC copy fragment: {} from partition {} (target context {})",
            source_address,
            source_partition,
            target_context
        );

        let request = storage_v1::CopyRequest {
            source_repository_id: Bytes::copy_from_slice(source_partition.data()),
            source_address: Some(source_address.into()),
            target_context: Bytes::copy_from_slice(zerocopy::IntoBytes::as_bytes(&target_context)),
        };

        self.copy_streams
            .request((session_id, Verb::Copy), ctx, request, operation, || {
                self.spawn_copy_stream(ctx)
            })
            .await
    }

    pub async fn mutable_load(
        &self,
        ctx: &GrpcSessionContext,
        key: &Hash,
        key_type: KeyType,
    ) -> Result<Hash, ProtocolError> {
        lore_debug!("gRPC mutable_load: {}", key);

        let request = storage_v1::MutableLoadRequest {
            key: Bytes::copy_from_slice(key.data()),
            key_type: key_type as u32,
        };
        let mut client = StorageServiceClient::new(self.connection.channel());
        let mut req = tonic::Request::new(request);
        inject_metadata(&mut req, ctx);

        let res = client
            .mutable_load(req)
            .await
            .map(|res| res.into_inner())
            .map_err(|err| match err.code() {
                tonic::Code::Unavailable => ProtocolError::from(SlowDown),
                tonic::Code::NotFound => ProtocolError::from(NotFound),
                tonic::Code::Unimplemented => ProtocolError::internal("unsupported: mutable_load"),
                _ => ProtocolError::internal_with_context(err, "mutable_load"),
            })?;

        Ok(Hash::from(&res.value[..]))
    }

    pub async fn mutable_store(
        &self,
        ctx: &GrpcSessionContext,
        key: Hash,
        value: Hash,
        key_type: KeyType,
    ) -> Result<(), ProtocolError> {
        lore_debug!("gRPC mutable_store: {}", key);

        let request = storage_v1::MutableStoreRequest {
            key: Bytes::copy_from_slice(key.data()),
            value: Bytes::copy_from_slice(value.data()),
            key_type: key_type as u32,
        };
        let mut client = StorageServiceClient::new(self.connection.channel());
        let mut req = tonic::Request::new(request);
        inject_metadata(&mut req, ctx);

        client
            .mutable_store(req)
            .await
            .map(|_| ())
            .map_err(|err| match err.code() {
                tonic::Code::Unavailable => ProtocolError::from(SlowDown),
                tonic::Code::Unimplemented => ProtocolError::internal("unsupported: mutable_store"),
                _ => ProtocolError::internal_with_context(err, "mutable_store"),
            })
    }

    pub async fn mutable_compare_and_swap(
        &self,
        ctx: &GrpcSessionContext,
        key: Hash,
        expected: Hash,
        value: Hash,
        key_type: KeyType,
    ) -> Result<Hash, ProtocolError> {
        lore_debug!("gRPC mutable_cas: {}", key);

        let request = storage_v1::MutableCompareAndSwapRequest {
            key: Bytes::copy_from_slice(key.data()),
            expected: Bytes::copy_from_slice(expected.data()),
            value: Bytes::copy_from_slice(value.data()),
            key_type: key_type as u32,
        };
        let mut client = StorageServiceClient::new(self.connection.channel());
        let mut req = tonic::Request::new(request);
        inject_metadata(&mut req, ctx);

        let res = client
            .mutable_compare_and_swap(req)
            .await
            .map(|res| res.into_inner())
            .map_err(|err| match err.code() {
                tonic::Code::Unavailable => ProtocolError::from(SlowDown),
                tonic::Code::Unimplemented => ProtocolError::internal("unsupported: mutable_cas"),
                _ => ProtocolError::internal_with_context(err, "mutable_cas"),
            })?;

        Ok(Hash::from(&res.current_value[..]))
    }

    /// A failure to open the RPC keeps its real status rather than becoming a disconnect: it
    /// applies equally to everything queued behind it, and an `Unauthenticated` or
    /// `Unimplemented` there should reach the caller instead of being replayed.
    /// Reader for a resolved stream, correlating by `request_id`.
    ///
    /// `pump_responses` keys on the address the server echoes back, which the resolved verbs
    /// cannot use: the address is what the request is asking for, not what identifies it. The
    /// `opened` flag and the drain on exit follow the same contract as the address-keyed verbs,
    /// so a stream that dies mid-flight fails its waiters rather than leaving them parked.
    pub async fn get_resolved(
        &self,
        session_id: u32,
        ctx: &GrpcSessionContext,
        key: &Hash,
        context: &Context,
        flags: u32,
    ) -> Result<(Hash, Fragment, Bytes), ProtocolError> {
        let key_address = Address {
            hash: *key,
            context: *context,
        };
        lore_debug!("gRPC get_resolved key: {}", key_address);

        let request_id = self
            .resolved_counter
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |id| id.checked_add(1))
            .map_err(|_exhausted| ProtocolError::internal("resolved request id exhausted"))?
            + 1;
        let request = storage_v1::GetResolvedRequest {
            request_id,
            key: Some(key_address.into()),
            flags,
        };

        let operation = self.operation(ctx).await?;

        let res = self
            .get_resolved_streams
            .request(
                (session_id, Verb::GetResolved),
                ctx,
                request,
                &operation,
                || self.spawn_get_resolved_stream(ctx),
            )
            .await?;

        if res.resolved.len() != size_of::<Hash>() {
            lore_error!(
                "Invalid get_resolved response, resolved hash is {} bytes, expected {}",
                res.resolved.len(),
                size_of::<Hash>()
            );
            return Err(ProtocolError::internal(
                "get_resolved: Invalid resolved hash length",
            ));
        }

        let Some(fragment) = res.fragment else {
            lore_error!("Invalid get_resolved response, missing fragment");
            return Err(ProtocolError::internal("get_resolved: Missing fragment"));
        };

        let fragment = Fragment {
            flags: fragment.flags,
            size_payload: fragment.size_payload,
            size_content: fragment.size_content,
        };

        if let Err(reason) = lore_base::types::validate_fragment_response(&fragment) {
            lore_error!("Invalid fragment in get_resolved response {fragment:?}: {reason}");
            return Err(ProtocolError::internal(format!(
                "get_resolved: invalid fragment: {reason}"
            )));
        }
        if res.payload.len() != fragment.size_payload as usize {
            lore_error!(
                "Fragment payload is invalid in get_resolved response: {} bytes, expected {}",
                res.payload.len(),
                fragment.size_payload
            );
            return Err(ProtocolError::internal("get_resolved: Invalid payload"));
        }

        Ok((Hash::from(&res.resolved[..]), fragment, res.payload.clone()))
    }

    pub async fn put_resolved(
        &self,
        session_id: u32,
        ctx: &GrpcSessionContext,
        key: &Hash,
        address: Address,
        fragment: Fragment,
        payload: Option<Bytes>,
    ) -> Result<(), ProtocolError> {
        lore_debug!("gRPC put_resolved key: {} -> {}", key, address);

        let request_id = self
            .resolved_counter
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |id| id.checked_add(1))
            .map_err(|_exhausted| ProtocolError::internal("resolved request id exhausted"))?
            + 1;
        let request = storage_v1::PutResolvedRequest {
            request_id,
            key: Bytes::from_owner(*key),
            address: Some(address.into()),
            fragment: Some(fragment.into()),
            payload: payload.unwrap_or_default(),
        };

        let operation = self.operation(ctx).await?;

        self.put_resolved_streams
            .request(
                (session_id, Verb::PutResolved),
                ctx,
                request,
                &operation,
                || self.spawn_put_resolved_stream(ctx),
            )
            .await
            .map(|_| ())
    }

    fn spawn_get_stream(
        &self,
        ctx: &GrpcSessionContext,
    ) -> PreparedStream<Address, (model_v1::Fragment, Bytes)> {
        let mut client = StorageServiceClient::new(self.connection.channel());
        let (tx, rx) = mpsc::channel(STREAM_WRITE_BUFFER_SIZE);
        let opened = Arc::new(AtomicBool::new(false));
        let stream = StreamLife::new(rx);
        let handle = Arc::new(StreamState {
            sender: tx,
            opened: opened.clone(),
            session: ctx.life.clone(),
            stream: stream.clone(),
        });
        let inflight: Arc<Inflight<(model_v1::Fragment, Bytes)>> = Arc::new(DashMap::new());
        let request_inflight = inflight.clone();
        let queue = stream.clone();
        let session = ctx.life.clone();
        let requests = async_stream::stream! {
            while let Some((address, reply)) = queue.next(&session).await {
                let admission = match queue.admit() {
                    Ok(admission) => admission,
                    Err(error) => {
                        reply.send(Err(error));
                        continue;
                    }
                };
                let first = register(&request_inflight, address, reply);
                drop(admission);
                if first {
                    yield model_v1::Address::from(address);
                }
            }
        };
        let cleanup = ReaderCleanup::new({
            let stream = stream.clone();
            let inflight = inflight.clone();
            move || {
                finish_address(
                    &stream,
                    &inflight,
                    Verb::Get,
                    ProtocolError::from(Disconnected),
                );
            }
        });
        let ctx = ctx.clone();
        PreparedStream::new(handle, async move {
            let _cleanup = cleanup;
            let mut request = tonic::Request::new(requests);
            inject_metadata(&mut request, &ctx);
            let terminal = match client.get(request).await {
                Ok(response) => {
                    opened.store(true, Ordering::Relaxed);
                    pump_responses(response.into_inner(), inflight.clone(), Verb::Get, Some).await
                }
                Err(status) => ProtocolError::from(status),
            };
            finish_address(&stream, &inflight, Verb::Get, terminal);
        })
    }

    fn spawn_get_metadata_stream(
        &self,
        ctx: &GrpcSessionContext,
    ) -> PreparedStream<Address, (model_v1::Fragment, Bytes)> {
        let mut client = StorageServiceClient::new(self.connection.channel());
        let (tx, rx) = mpsc::channel(STREAM_WRITE_BUFFER_SIZE);
        let opened = Arc::new(AtomicBool::new(false));
        let stream = StreamLife::new(rx);
        let handle = Arc::new(StreamState {
            sender: tx,
            opened: opened.clone(),
            session: ctx.life.clone(),
            stream: stream.clone(),
        });
        let inflight: Arc<Inflight<(model_v1::Fragment, Bytes)>> = Arc::new(DashMap::new());
        let request_inflight = inflight.clone();
        let queue = stream.clone();
        let session = ctx.life.clone();
        let requests = async_stream::stream! {
            while let Some((address, reply)) = queue.next(&session).await {
                let admission = match queue.admit() {
                    Ok(admission) => admission,
                    Err(error) => {
                        reply.send(Err(error));
                        continue;
                    }
                };
                let first = register(&request_inflight, address, reply);
                drop(admission);
                if first {
                    yield model_v1::Address::from(address);
                }
            }
        };
        let cleanup = ReaderCleanup::new({
            let stream = stream.clone();
            let inflight = inflight.clone();
            move || {
                finish_address(
                    &stream,
                    &inflight,
                    Verb::GetMetadata,
                    ProtocolError::from(Disconnected),
                );
            }
        });
        let ctx = ctx.clone();
        PreparedStream::new(handle, async move {
            let _cleanup = cleanup;
            let mut request = tonic::Request::new(requests);
            inject_metadata(&mut request, &ctx);
            let terminal = match client.get_metadata(request).await {
                Ok(response) => {
                    opened.store(true, Ordering::Relaxed);
                    pump_responses(
                        response.into_inner(),
                        inflight.clone(),
                        Verb::GetMetadata,
                        Some,
                    )
                    .await
                }
                Err(status) => ProtocolError::from(status),
            };
            finish_address(&stream, &inflight, Verb::GetMetadata, terminal);
        })
    }

    fn spawn_put_stream(
        &self,
        ctx: &GrpcSessionContext,
    ) -> PreparedStream<storage_v1::PutRequest, ()> {
        let mut client = StorageServiceClient::new(self.connection.channel());
        let (tx, rx) = mpsc::channel(STREAM_WRITE_BUFFER_SIZE);
        let opened = Arc::new(AtomicBool::new(false));
        let stream = StreamLife::new(rx);
        let handle = Arc::new(StreamState {
            sender: tx,
            opened: opened.clone(),
            session: ctx.life.clone(),
            stream: stream.clone(),
        });
        let inflight: Arc<Inflight<()>> = Arc::new(DashMap::new());
        let request_inflight = inflight.clone();
        let queue = stream.clone();
        let session = ctx.life.clone();
        let requests = async_stream::stream! {
            while let Some((request, reply)) = queue.next(&session).await {
                let request: storage_v1::PutRequest = request;
                let Some(address) = request.address.as_ref().map(Address::from) else {
                    drop(request);
                    reply.send(Err(ProtocolError::internal("put: missing address")));
                    continue;
                };
                let admission = match queue.admit() {
                    Ok(admission) => admission,
                    Err(error) => {
                        drop(request);
                        reply.send(Err(error));
                        continue;
                    }
                };
                let first = register(&request_inflight, address, reply);
                drop(admission);
                if first {
                    yield request;
                }
            }
        };
        let cleanup = ReaderCleanup::new({
            let stream = stream.clone();
            let inflight = inflight.clone();
            move || {
                finish_address(
                    &stream,
                    &inflight,
                    Verb::Put,
                    ProtocolError::from(Disconnected),
                );
            }
        });
        let ctx = ctx.clone();
        PreparedStream::new(handle, async move {
            let _cleanup = cleanup;
            let mut request = tonic::Request::new(requests);
            inject_metadata(&mut request, &ctx);
            let terminal = match client.put(request).await {
                Ok(response) => {
                    opened.store(true, Ordering::Relaxed);
                    pump_responses(response.into_inner(), inflight.clone(), Verb::Put, Some).await
                }
                Err(status) => ProtocolError::from(status),
            };
            finish_address(&stream, &inflight, Verb::Put, terminal);
        })
    }

    fn spawn_get_resolved_stream(
        &self,
        ctx: &GrpcSessionContext,
    ) -> PreparedStream<storage_v1::GetResolvedRequest, Arc<storage_v1::GetResolvedResponse>> {
        let mut client = StorageServiceClient::new(self.connection.channel());
        let (tx, rx) = mpsc::channel(STREAM_WRITE_BUFFER_SIZE);
        let opened = Arc::new(AtomicBool::new(false));
        let stream = StreamLife::new(rx);
        let handle = Arc::new(StreamState {
            sender: tx,
            opened: opened.clone(),
            session: ctx.life.clone(),
            stream: stream.clone(),
        });
        let pending = Arc::new(DashMap::<u64, Reply<Arc<storage_v1::GetResolvedResponse>>>::new());
        let request_pending = pending.clone();
        let queue = stream.clone();
        let session = ctx.life.clone();
        let requests = async_stream::stream! {
            while let Some((request, reply)) = queue.next(&session).await {
                let request: storage_v1::GetResolvedRequest = request;
                let admission = match queue.admit() {
                    Ok(admission) => admission,
                    Err(error) => {
                        drop(request);
                        reply.send(Err(error));
                        continue;
                    }
                };
                let replaced = request_pending.insert(request.request_id, reply);
                drop(admission);
                if let Some(replaced) = replaced {
                    replaced.send(Err(ProtocolError::internal(
                        "duplicate resolved request id",
                    )));
                }
                yield request;
            }
        };
        let cleanup = ReaderCleanup::new({
            let stream = stream.clone();
            let pending = pending.clone();
            move || finish_resolved(&stream, &pending, ProtocolError::from(Disconnected))
        });
        let ctx = ctx.clone();
        PreparedStream::new(handle, async move {
            let _cleanup = cleanup;
            let mut request = tonic::Request::new(requests);
            inject_metadata(&mut request, &ctx);
            let mut responses = match client.get_resolved(request).await {
                Ok(response) => {
                    opened.store(true, Ordering::Relaxed);
                    response.into_inner()
                }
                Err(status) => {
                    finish_resolved(&stream, &pending, ProtocolError::from(status));
                    return;
                }
            };
            let terminal = loop {
                match responses.next().await {
                    Some(Ok(response)) => {
                        let Some((_, reply)) = pending.remove(&response.request_id) else {
                            lore_error!("unexpected resolved response id {}", response.request_id);
                            continue;
                        };
                        let result = match item_status_error(response.status.as_ref()) {
                            Some(error) => Err(error),
                            None => Ok(Arc::new(response)),
                        };
                        reply.send(result);
                    }
                    Some(Err(status)) => break ProtocolError::from(status),
                    None => {
                        break ProtocolError::internal(
                            "get_resolved: stream closed before responding",
                        );
                    }
                }
            };
            finish_resolved(&stream, &pending, terminal);
        })
    }

    fn spawn_put_resolved_stream(
        &self,
        ctx: &GrpcSessionContext,
    ) -> PreparedStream<storage_v1::PutResolvedRequest, Arc<storage_v1::PutResolvedResponse>> {
        let mut client = StorageServiceClient::new(self.connection.channel());
        let (tx, rx) = mpsc::channel(STREAM_WRITE_BUFFER_SIZE);
        let opened = Arc::new(AtomicBool::new(false));
        let stream = StreamLife::new(rx);
        let handle = Arc::new(StreamState {
            sender: tx,
            opened: opened.clone(),
            session: ctx.life.clone(),
            stream: stream.clone(),
        });
        let pending = Arc::new(DashMap::<u64, Reply<Arc<storage_v1::PutResolvedResponse>>>::new());
        let request_pending = pending.clone();
        let queue = stream.clone();
        let session = ctx.life.clone();
        let requests = async_stream::stream! {
            while let Some((request, reply)) = queue.next(&session).await {
                let request: storage_v1::PutResolvedRequest = request;
                let admission = match queue.admit() {
                    Ok(admission) => admission,
                    Err(error) => {
                        drop(request);
                        reply.send(Err(error));
                        continue;
                    }
                };
                let replaced = request_pending.insert(request.request_id, reply);
                drop(admission);
                if let Some(replaced) = replaced {
                    replaced.send(Err(ProtocolError::internal(
                        "duplicate resolved request id",
                    )));
                }
                yield request;
            }
        };
        let cleanup = ReaderCleanup::new({
            let stream = stream.clone();
            let pending = pending.clone();
            move || finish_resolved(&stream, &pending, ProtocolError::from(Disconnected))
        });
        let ctx = ctx.clone();
        PreparedStream::new(handle, async move {
            let _cleanup = cleanup;
            let mut request = tonic::Request::new(requests);
            inject_metadata(&mut request, &ctx);
            let mut responses = match client.put_resolved(request).await {
                Ok(response) => {
                    opened.store(true, Ordering::Relaxed);
                    response.into_inner()
                }
                Err(status) => {
                    finish_resolved(&stream, &pending, ProtocolError::from(status));
                    return;
                }
            };
            let terminal = loop {
                match responses.next().await {
                    Some(Ok(response)) => {
                        let Some((_, reply)) = pending.remove(&response.request_id) else {
                            lore_error!("unexpected resolved response id {}", response.request_id);
                            continue;
                        };
                        let result = match item_status_error(response.status.as_ref()) {
                            Some(error) => Err(error),
                            None => Ok(Arc::new(response)),
                        };
                        reply.send(result);
                    }
                    Some(Err(status)) => break ProtocolError::from(status),
                    None => {
                        break ProtocolError::internal(
                            "put_resolved: stream closed before responding",
                        );
                    }
                }
            };
            finish_resolved(&stream, &pending, terminal);
        })
    }

    fn spawn_copy_stream(
        &self,
        ctx: &GrpcSessionContext,
    ) -> PreparedStream<storage_v1::CopyRequest, ()> {
        let mut client = StorageServiceClient::new(self.connection.channel());
        let (tx, rx) = mpsc::channel(STREAM_WRITE_BUFFER_SIZE);
        let opened = Arc::new(AtomicBool::new(false));
        let stream = StreamLife::new(rx);
        let handle = Arc::new(StreamState {
            sender: tx,
            opened: opened.clone(),
            session: ctx.life.clone(),
            stream: stream.clone(),
        });
        let inflight: Arc<Inflight<()>> = Arc::new(DashMap::new());
        let routing = Arc::new(parking_lot::Mutex::new(CopyRouting::default()));
        let ready = Arc::new(tokio::sync::Notify::new());
        let request_inflight = inflight.clone();
        let request_routing = routing.clone();
        let queue = stream.clone();
        let session = ctx.life.clone();
        let request_ready = ready.clone();
        let requests = async_stream::stream! {
            let mut primary_done = false;
            loop {
                let notified = request_ready.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                let next = request_routing.lock().ready.pop_front();
                if let Some(next) = next {
                    let admission = match queue.admit() {
                        Ok(admission) => admission,
                        Err(_terminal) => {
                            drop(next);
                            break;
                        }
                    };
                    {
                        let mut routing = request_routing.lock();
                        routing.not_yielded = routing
                            .not_yielded
                            .checked_sub(1)
                            .expect("one emission per accepted copy");
                    }
                    drop(admission);
                    yield next;
                    continue;
                }
                if primary_done && request_routing.lock().not_yielded == 0 {
                    break;
                }
                let item = tokio::select! {
                    _ = queue.wait_terminal() => break,
                    _ = notified => continue,
                    item = queue.next(&session), if !primary_done => item,
                };
                let Some((request, reply)) = item else {
                    primary_done = true;
                    continue;
                };
                let Some((source, target)) = CopyRouting::addresses(&request) else {
                    drop(request);
                    reply.send(Err(ProtocolError::internal("copy: invalid request")));
                    continue;
                };
                let admission = match queue.admit() {
                    Ok(admission) => admission,
                    Err(error) => {
                        drop(request);
                        reply.send(Err(error));
                        continue;
                    }
                };
                if !register(&request_inflight, target, reply) {
                    drop(admission);
                    drop(request);
                    continue;
                }
                let next = request_routing.lock().admit(source, target, request);
                if next.is_some() {
                    let mut routing = request_routing.lock();
                    routing.not_yielded = routing
                        .not_yielded
                        .checked_sub(1)
                        .expect("one emission per accepted copy");
                }
                drop(admission);
                if let Some(next) = next {
                    yield next;
                }
            }
        };
        let route = {
            let routing = routing.clone();
            let ready = ready.clone();
            move |source| {
                let target = {
                    let mut routing = routing.lock();
                    let (target, next) = routing.answered(source);
                    if let Some(next) = next {
                        routing.ready.push_back(next);
                    }
                    target
                };
                ready.notify_one();
                target
            }
        };
        let finish = {
            let stream = stream.clone();
            let inflight = inflight.clone();
            let routing = routing.clone();
            let ready = ready.clone();
            move |error| {
                let error = stream.terminate(error);
                let retired = {
                    let mut routing = routing.lock();
                    std::mem::take(&mut *routing)
                };
                drop(retired);
                ready.notify_waiters();
                fail_buffered(&stream, &error);
                fail_inflight(&inflight, Verb::Copy, &error);
            }
        };
        let cleanup = ReaderCleanup::new({
            let finish = finish.clone();
            move || finish(ProtocolError::from(Disconnected))
        });
        let ctx = ctx.clone();
        PreparedStream::new(handle, async move {
            let _cleanup = cleanup;
            let mut request = tonic::Request::new(requests);
            inject_metadata(&mut request, &ctx);
            let terminal = match client.copy(request).await {
                Ok(response) => {
                    opened.store(true, Ordering::Relaxed);
                    pump_responses(response.into_inner(), inflight.clone(), Verb::Copy, route).await
                }
                Err(status) => ProtocolError::from(status),
            };
            finish(terminal);
        })
    }
}
