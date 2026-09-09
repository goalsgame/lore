// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use std::str::FromStr;

use bytes::Bytes;
use http_body_util::Empty;
use lore_base::types::Context;
use lore_proto::lore::storage::v1 as storage_v1;
use lore_transport::grpc::CORRELATION_ID_HEADER;
use lore_transport::grpc::PARTITION_ID_KEY;
use tonic::Request;
use tonic::Streaming;
use tonic::codec::BufferSettings;
use tonic::metadata::MetadataValue;
use tonic_prost::ProstDecoder;

pub(crate) fn make_request_with_metadata<T>(
    inner: T,
    repository: Context,
    correlation_id: &str,
) -> Request<T> {
    let mut request = Request::new(inner);
    request.metadata_mut().insert_bin(
        PARTITION_ID_KEY,
        tonic::metadata::BinaryMetadataValue::from_bytes(repository.data()),
    );
    if !correlation_id.is_empty() {
        request.metadata_mut().insert(
            CORRELATION_ID_HEADER,
            MetadataValue::from_str(correlation_id).unwrap(),
        );
    }
    request
}

/// A request wrapping a `Streaming<Address>` that carries no items — it decodes an empty body,
/// so `stream.next()` resolves to `None` immediately — plus `repository`'s metadata. For
/// `storage::v1::get::handler` and `storage::v1::get_metadata::handler` (both take a stream of
/// `Address`).
///
/// Exists for exercising a streaming handler's one-time authorization gate, which runs before
/// `request.into_inner()` ever reads the stream, without needing to encode real protobuf frames
/// onto the wire: a denied check returns from `handler` before the stream is ever touched, and
/// an authorized check only needs `handler` to return `Ok(_)`, regardless of whether the
/// (background) per-item loop ever has anything to process.
pub(crate) fn make_empty_get_stream_request(
    repository: Context,
) -> Request<Streaming<lore_proto::lore::model::v1::Address>> {
    let decoder = ProstDecoder::new(BufferSettings::default());
    let streaming = Streaming::new_request(decoder, Empty::<Bytes>::new(), None, None);
    make_request_with_metadata(streaming, repository, "")
}

/// Same as [`make_empty_get_stream_request`], for `storage::v1::put::handler` (a stream of
/// `PutRequest`).
pub(crate) fn make_empty_put_stream_request(
    repository: Context,
) -> Request<Streaming<storage_v1::PutRequest>> {
    let decoder = ProstDecoder::new(BufferSettings::default());
    let streaming = Streaming::new_request(decoder, Empty::<Bytes>::new(), None, None);
    make_request_with_metadata(streaming, repository, "")
}
