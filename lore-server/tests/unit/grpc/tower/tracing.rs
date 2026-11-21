// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use lore_base::types::Context;
use lore_server::grpc::tower::tracing::*;
use lore_transport::grpc::REPOSITORY_ID_KEY;
use rand::random;
use tonic::metadata::MetadataMap;

#[test]
fn can_extract_repository_id() {
    let repository = random::<Context>();
    let mut metadata = MetadataMap::new();
    metadata.insert_bin(
        REPOSITORY_ID_KEY,
        tonic::metadata::BinaryMetadataValue::from_bytes(repository.data()),
    );

    let string_from_helper = repository_id_field(&metadata);
    let expected_string = repository.to_string();
    assert_eq!(string_from_helper, expected_string);
}

#[test]
fn handles_no_repository_id() {
    let metadata = MetadataMap::new();

    let string_from_helper = repository_id_field(&metadata);
    assert_eq!(string_from_helper, "<no_repo_id>".to_string());
}

#[test]
fn continues_the_callers_trace() {
    use opentelemetry::trace::TraceContextExt;

    let mut headers = http::HeaderMap::new();
    headers.insert(
        "traceparent",
        http::HeaderValue::from_static("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"),
    );

    let context = remote_context(&headers);
    let span = context.span();
    let parent = span.span_context();
    assert!(parent.is_remote());
    assert!(parent.is_sampled());
    assert_eq!(
        parent.trace_id().to_string(),
        "4bf92f3577b34da6a3ce929d0e0e4736"
    );
    assert_eq!(parent.span_id().to_string(), "00f067aa0ba902b7");
}

#[test]
fn starts_a_new_trace_without_trace_context() {
    use opentelemetry::trace::TraceContextExt;

    let context = remote_context(&http::HeaderMap::new());
    assert!(!context.span().span_context().is_valid());
}
