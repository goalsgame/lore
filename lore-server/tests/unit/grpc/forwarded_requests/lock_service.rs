// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use std::str::FromStr;

use lore_proto::lock::QueryRequest;
use lore_server::grpc::forwarded_requests::lock_service::*;
use tonic::Request;
use tonic::transport::Channel;

/// An unreachable peer must reach the caller as an `InternalClientError`
/// rather than as the peer's own answer, so the forwarding handler logs it
/// and substitutes a status of its own.
#[tokio::test]
async fn unreachable_peer_is_an_internal_client_error() {
    // Port 1 on loopback refuses immediately, and connect_lazy defers the
    // connection to the call so no peer has to exist to build the client.
    let uri = http::Uri::from_str("http://127.0.0.1:1/").expect("valid uri");
    let channel = Channel::builder(uri).connect_lazy();
    let mut client = GrpcForwardedLockServiceClient::new(channel, 1024);

    let request = Request::new(QueryRequest {
        branch: None,
        owner: Some("alice".into()),
        description: None,
    });

    let err = client
        .query(request)
        .await
        .expect_err("an unreachable peer is a client error");
    assert!(
        err.to_string().contains("did not reach the peer"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn forwarded_response_is_bounded_while_decoding() {
    use lore_proto::lock;
    use lore_proto::lock::forwarded_lock_service_server::ForwardedLockService;
    use lore_proto::lock::forwarded_lock_service_server::ForwardedLockServiceServer;

    struct Peer;
    #[async_trait::async_trait]
    impl ForwardedLockService for Peer {
        async fn query(
            &self,
            _request: Request<lock::QueryRequest>,
        ) -> Result<tonic::Response<lock::QueryResponse>, tonic::Status> {
            Ok(tonic::Response::new(lock::QueryResponse {
                result: vec![lock::Lock {
                    owner: "a".repeat(4096),
                    ..Default::default()
                }],
            }))
        }
    }
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let incoming = async_stream::stream! {
        loop { yield listener.accept().await.map(|(socket, _)| socket); }
    };
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(ForwardedLockServiceServer::new(Peer))
            .serve_with_incoming(incoming)
            .await
            .unwrap();
    });
    let channel = Channel::from_shared(format!("http://{address}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut bounded = GrpcForwardedLockServiceClient::new(channel.clone(), 128);
    assert!(!matches!(
        bounded
            .query(Request::new(lock::QueryRequest::default()))
            .await,
        Ok(Ok(_))
    ));
    let mut allowed = GrpcForwardedLockServiceClient::new(channel, 8192);
    assert_eq!(
        allowed
            .query(Request::new(lock::QueryRequest::default()))
            .await
            .unwrap()
            .unwrap()
            .into_inner()
            .result[0]
            .owner
            .len(),
        4096
    );
    server.abort();
    let _ = server.await;
}
