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

#[test]
fn combines_tracestate_split_across_header_fields() {
    use opentelemetry::trace::TraceContextExt;

    let mut headers = http::HeaderMap::new();
    headers.insert(
        "traceparent",
        http::HeaderValue::from_static("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"),
    );
    headers.append("tracestate", http::HeaderValue::from_static("a=1"));
    headers.append("tracestate", http::HeaderValue::from_static("b=2,c=3"));

    let context = remote_context(&headers);
    let state = context.span().span_context().trace_state().clone();
    assert_eq!(state.get("a"), Some("1"));
    assert_eq!(state.get("b"), Some("2"));
    assert_eq!(state.get("c"), Some("3"));
}

mod layer {
    use std::convert::Infallible;

    use lore_server::correlation::layer::CorrelationIdLayerBuilder;
    use lore_server::correlation::layer::TraceLayerConfig;
    use opentelemetry::trace::SpanId;
    use opentelemetry::trace::TracerProvider as _;
    use opentelemetry_sdk::trace::InMemorySpanExporter;
    use opentelemetry_sdk::trace::Sampler;
    use opentelemetry_sdk::trace::SdkTracerProvider;
    use opentelemetry_sdk::trace::SpanData;
    use tonic::body::Body;
    use tower::Service;
    use tower::ServiceBuilder;
    use tracing_subscriber::layer::SubscriberExt;

    use super::LoreTracingLayer;

    const REQUEST_SPANS: [&str; 2] = ["urc", "lore_tracing"];

    const TRACE_ID: &str = "4bf92f3577b34da6a3ce929d0e0e4736";
    const PARENT_SPAN_ID: &str = "00f067aa0ba902b7";

    fn record(names: &[&str], run: impl FnOnce()) -> Vec<SpanData> {
        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_sampler(Sampler::ParentBased(Box::new(Sampler::AlwaysOn)))
            .with_simple_exporter(exporter.clone())
            .build();
        let subscriber = tracing_subscriber::registry()
            .with(tracing_opentelemetry::layer().with_tracer(provider.tracer("test")));
        tracing::subscriber::with_default(subscriber, run);
        provider.force_flush().unwrap();
        exporter
            .get_finished_spans()
            .unwrap()
            .into_iter()
            .filter(|span| names.contains(&span.name.as_ref()))
            .collect()
    }

    fn serve(traceparent: Option<&'static str>) -> Vec<SpanData> {
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
        let mut service = ServiceBuilder::new()
            .layer(
                CorrelationIdLayerBuilder::new()
                    .with_grpc_tracer(TraceLayerConfig::default())
                    .build(),
            )
            .layer(LoreTracingLayer {})
            .service(inner);

        record(&REQUEST_SPANS, || {
            futures::executor::block_on(service.call(request)).unwrap();
        })
    }

    fn request_spans(spans: &[SpanData]) -> (&SpanData, &SpanData) {
        assert_eq!(spans.len(), REQUEST_SPANS.len());
        let find = |name: &str| spans.iter().find(|span| span.name == name).unwrap();
        (find("urc"), find("lore_tracing"))
    }

    #[test]
    fn a_sampled_caller_gets_the_request_spans_in_its_trace() {
        let spans = serve(Some(
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
        ));
        let (urc, lore) = request_spans(&spans);
        assert_eq!(urc.span_context.trace_id().to_string(), TRACE_ID);
        assert_eq!(urc.parent_span_id.to_string(), PARENT_SPAN_ID);
        assert!(urc.parent_span_is_remote);
        assert_eq!(lore.span_context.trace_id().to_string(), TRACE_ID);
        assert_eq!(lore.parent_span_id, urc.span_context.span_id());
    }

    #[test]
    fn an_unsampled_caller_keeps_the_request_spans_unsampled() {
        let spans = serve(Some(
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00",
        ));
        assert!(spans.is_empty());
    }

    #[test]
    fn a_request_without_trace_context_starts_a_new_trace() {
        let spans = serve(None);
        let (urc, lore) = request_spans(&spans);
        assert_ne!(urc.span_context.trace_id().to_string(), TRACE_ID);
        assert_eq!(urc.parent_span_id, SpanId::INVALID);
        assert_eq!(lore.span_context.trace_id(), urc.span_context.trace_id());
        assert_eq!(lore.parent_span_id, urc.span_context.span_id());
    }

    #[test]
    fn an_http_request_continues_the_callers_trace() {
        use tower::ServiceExt;

        let router = axum::Router::new()
            .route("/x", axum::routing::get(|| async {}))
            .layer(axum::middleware::from_fn(
                lore_server::http::tracing::lore_http_tracing,
            ))
            .layer(CorrelationIdLayerBuilder::new().with_http_tracer().build());
        let request = http::Request::builder()
            .uri("/x")
            .header(
                "traceparent",
                "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
            )
            .body(axum::body::Body::empty())
            .unwrap();

        let spans = record(&["urc", "lore_http_tracing"], || {
            tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap()
                .block_on(router.oneshot(request))
                .unwrap();
        });
        assert_eq!(spans.len(), 2);
        let urc = spans.iter().find(|span| span.name == "urc").unwrap();
        let http = spans
            .iter()
            .find(|span| span.name == "lore_http_tracing")
            .unwrap();
        assert_eq!(urc.span_context.trace_id().to_string(), TRACE_ID);
        assert_eq!(urc.parent_span_id.to_string(), PARENT_SPAN_ID);
        assert_eq!(http.span_context.trace_id().to_string(), TRACE_ID);
        assert_eq!(http.parent_span_id, urc.span_context.span_id());
    }
}
