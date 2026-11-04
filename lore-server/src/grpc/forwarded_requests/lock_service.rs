// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

use async_trait::async_trait;
use lore_proto::lock;
use lore_proto::lock::forwarded_lock_service_client::ForwardedLockServiceClient as ProtoForwardedLockServiceClient;
use tonic::Request;
use tonic::transport::Channel;

use crate::grpc::forwarded_requests::ForwardedRequestResult;
use crate::grpc::forwarded_requests::classify_forwarded_result;

#[async_trait]
pub trait ForwardedLockServiceClient: Send + Sync {
    async fn query(
        &mut self,
        request: Request<lock::QueryRequest>,
    ) -> ForwardedRequestResult<lock::QueryResponse>;
}

pub struct GrpcForwardedLockServiceClient {
    client: ProtoForwardedLockServiceClient<Channel>,
}

impl GrpcForwardedLockServiceClient {
    pub fn new(channel: Channel, max_response_bytes: usize) -> Self {
        let client = ProtoForwardedLockServiceClient::new(channel)
            .max_decoding_message_size(max_response_bytes);
        Self { client }
    }
}

#[async_trait]
impl ForwardedLockServiceClient for GrpcForwardedLockServiceClient {
    async fn query(
        &mut self,
        request: Request<lock::QueryRequest>,
    ) -> ForwardedRequestResult<lock::QueryResponse> {
        classify_forwarded_result(self.client.query(request).await)
    }
}
