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

#[test]
fn ignores_the_current_context_without_trace_context() {
    use opentelemetry::trace::SpanContext;
    use opentelemetry::trace::SpanId;
    use opentelemetry::trace::TraceContextExt;
    use opentelemetry::trace::TraceFlags;
    use opentelemetry::trace::TraceId;
    use opentelemetry::trace::TraceState;

    let current = opentelemetry::Context::new().with_remote_span_context(SpanContext::new(
        TraceId::from_hex("4bf92f3577b34da6a3ce929d0e0e4736").unwrap(),
        SpanId::from_hex("00f067aa0ba902b7").unwrap(),
        TraceFlags::SAMPLED,
        true,
        TraceState::default(),
    ));
    let _attached = current.attach();

    let mut invalid = http::HeaderMap::new();
    invalid.insert(
        "traceparent",
        http::HeaderValue::from_static("not-a-traceparent"),
    );
    for headers in [http::HeaderMap::new(), invalid] {
        assert!(!remote_context(&headers).span().span_context().is_valid());
    }
}

mod layer {
    use std::convert::Infallible;

    use opentelemetry::trace::SpanId;
    use opentelemetry::trace::TracerProvider as _;
    use opentelemetry_sdk::trace::InMemorySpanExporter;
    use opentelemetry_sdk::trace::Sampler;
    use opentelemetry_sdk::trace::SdkTracerProvider;
    use opentelemetry_sdk::trace::SpanData;
    use tonic::body::Body;
    use tower::Layer;
    use tower::Service;
    use tracing_subscriber::layer::SubscriberExt;

    use super::LoreTracingLayer;

    const TRACE_ID: &str = "4bf92f3577b34da6a3ce929d0e0e4736";
    const PARENT_SPAN_ID: &str = "00f067aa0ba902b7";

    fn serve(traceparent: Option<&'static str>) -> Vec<SpanData> {
        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_sampler(Sampler::ParentBased(Box::new(Sampler::AlwaysOn)))
            .with_simple_exporter(exporter.clone())
            .build();
        let subscriber = tracing_subscriber::registry()
            .with(tracing_opentelemetry::layer().with_tracer(provider.tracer("test")));

        let mut request = http::Request::builder()
            .uri("/lore.revision.v1.RevisionService/BranchGet")
            .body(Body::empty())
            .unwrap();
        if let Some(value) = traceparent {
            request
                .headers_mut()
                .insert("traceparent", http::HeaderValue::from_static(value));
        }
        let inner = tower::service_fn(|_: http::Request<Body>| async {
            Ok::<_, Infallible>(http::Response::new(Body::empty()))
        });
        let mut service = LoreTracingLayer {}.layer(inner);

        tracing::subscriber::with_default(subscriber, || {
            futures::executor::block_on(service.call(request)).unwrap();
        });
        provider.force_flush().unwrap();
        exporter
            .get_finished_spans()
            .unwrap()
            .into_iter()
            .filter(|span| span.name == "lore_tracing")
            .collect()
    }

    #[test]
    fn a_sampled_caller_gets_the_request_span_in_its_trace() {
        let spans = serve(Some(
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
        ));
        assert_eq!(spans.len(), 1);
        let span = &spans[0];
        assert_eq!(span.span_context.trace_id().to_string(), TRACE_ID);
        assert_eq!(span.parent_span_id.to_string(), PARENT_SPAN_ID);
        assert!(span.parent_span_is_remote);
    }

    #[test]
    fn an_unsampled_caller_keeps_the_request_span_unsampled() {
        let spans = serve(Some(
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00",
        ));
        assert!(spans.is_empty());
    }

    #[test]
    fn a_request_without_trace_context_starts_a_new_trace() {
        let spans = serve(None);
        assert_eq!(spans.len(), 1);
        let span = &spans[0];
        assert_ne!(span.span_context.trace_id().to_string(), TRACE_ID);
        assert_eq!(span.parent_span_id, SpanId::INVALID);
    }
}
