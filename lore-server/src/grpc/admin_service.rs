// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use lore_base::types::RepositoryId;
use lore_base::version::LORE_LIBRARY_VERSION;
use lore_proto::rpc::HostInfo;
use lore_proto::rpc::ServerInfoRequest;
use lore_proto::rpc::ServerInfoResponse;
use lore_proto::rpc::admin_service_server::AdminService;
use lore_revision::notification::NotificationSender;
use sysinfo::CpuRefreshKind;
use sysinfo::MemoryRefreshKind;
use sysinfo::RefreshKind;
use tonic::Request;
use tonic::Response;
use tonic::Status;
use tracing::info;
use tracing::instrument;
use tracing::warn;

use super::handlers::obliterate;
use super::timeout_grpc;
use crate::auth::jwt::JwtVerifier;
use crate::auth::jwt_interceptor::extract_bearer_token;
use crate::authnz::repository_authorizer::AllowAllRepositoryAuthorizer;
use crate::authnz::repository_authorizer::READ_ACTION;
use crate::authnz::repository_authorizer::RepositoryAuthorizer;
use crate::authnz::repository_authorizer::VerifiedToken;
use crate::hooks::HookDispatcher;

pub struct LoreAdminService {
    server_info: ServerInfoResponse,
    immutable_store: Arc<dyn lore_storage::ImmutableStore>,
    mutable_store: Arc<dyn lore_storage::MutableStore>,
    jwt_verifier: Arc<Option<JwtVerifier>>,
    notification: Arc<dyn NotificationSender>,
    hook_dispatcher: Arc<HookDispatcher>,
    rpc_timeout: Duration,
    /// Checks the `obliterate` action (LEP 2026-08-20-oidc-oauth2-authentication,
    /// D4/D8). Defaults to [`AllowAllRepositoryAuthorizer`] until
    /// [`Self::set_repository_authorizer`] runs, matching this service's
    /// existing "unauthenticated until `set_jwt_verifier` runs" shape.
    repository_authorizer: Arc<dyn RepositoryAuthorizer>,
}

impl LoreAdminService {
    pub fn new(
        settings: HashMap<String, String>,
        features: Vec<String>,
        immutable_store: Arc<dyn lore_storage::ImmutableStore>,
        mutable_store: Arc<dyn lore_storage::MutableStore>,
        notification: Arc<dyn NotificationSender>,
        hook_dispatcher: Arc<HookDispatcher>,
    ) -> Self {
        let mut sys =
            sysinfo::System::new_with_specifics(RefreshKind::everything().without_processes());

        sys.refresh_cpu_specifics(CpuRefreshKind::nothing().with_frequency());
        sys.refresh_memory_specifics(MemoryRefreshKind::nothing().with_ram());

        let cores = sys.cpus().len();
        let cpu = sys
            .cpus()
            .iter()
            .next()
            .map_or("unknown".to_string(), |cpu| {
                format!(
                    "{} ({} cores) {:.1}ghz",
                    cpu.brand(),
                    cores,
                    cpu.frequency() as f64 / 1000f64
                )
            });

        let ram = format!("{} GiB", sys.total_memory() / 1024 / 1024 / 1024);

        Self {
            server_info: ServerInfoResponse {
                version: LORE_LIBRARY_VERSION.to_string(),
                features,
                settings,
                host: Some(HostInfo {
                    arch: env!("VERGEN_RUSTC_HOST_TRIPLE").to_string(),
                    cpu,
                    ram,
                    hostname: sysinfo::System::host_name().unwrap_or("unknown".to_string()),
                    environment: std::env::var("LORE_ENV").unwrap_or("unknown".to_string()),
                }),
            },
            immutable_store,
            mutable_store,
            jwt_verifier: Arc::new(None),
            notification,
            hook_dispatcher,
            rpc_timeout: Duration::from_secs(60),
            repository_authorizer: Arc::new(AllowAllRepositoryAuthorizer),
        }
    }

    pub fn set_jwt_verifier(&mut self, jwt_verifier: Option<JwtVerifier>) {
        if jwt_verifier.is_none() {
            warn!(
                "No JWT verifier - Admin Service RPCs including obliterate will be unauthenticated"
            );
        }
        self.jwt_verifier = Arc::new(jwt_verifier);
    }

    pub fn set_repository_authorizer(
        &mut self,
        repository_authorizer: Arc<dyn RepositoryAuthorizer>,
    ) {
        self.repository_authorizer = repository_authorizer;
    }

    pub fn set_rpc_timeout(&mut self, rpc_timeout: Duration) {
        self.rpc_timeout = rpc_timeout;
    }
}

#[tonic::async_trait]
impl AdminService for LoreAdminService {
    /// **Behavior change**, not merely "add a missing action check to an
    /// already-authenticated RPC" (which is what every other RPC in this
    /// effort is doing): `ServerInfo` was previously fully anonymous — any
    /// caller, no token needed — because `AdminService` is mounted with no
    /// JWT interceptor at all, unconditionally
    /// (`AdminServiceServer::new(admin_svc)` in `grpc/server.rs`, unlike
    /// every other public gRPC service). `obliterate` on this same service
    /// already compensates for that missing interceptor by authenticating
    /// itself inline; this does the same self-contained
    /// extract-bearer-token-then-verify dance `obliterate` uses.
    ///
    /// Once `[server.auth]` is configured (`self.jwt_verifier` is `Some`),
    /// this RPC now requires a valid bearer token *and* the `read` action.
    /// `ServerInfo` reports hostname/CPU/RAM/settings, not repository data,
    /// so it is not partition-scoped: the check is made against
    /// `RepositoryId::default()` (the zero/sentinel repository), which
    /// Tier 1's `GlobalGrantsAuthorizer` ignores entirely regardless of
    /// what is passed.
    ///
    /// When no verifier is configured at all (`self.jwt_verifier` is
    /// `None`, e.g. no `[server.auth]` block), the RPC answers exactly as
    /// before — unauthenticated, no check performed — matching
    /// `obliterate`'s own "unauthenticated until `set_jwt_verifier` runs"
    /// shape (see its `proceeds_without_auth_when_no_verifier_configured`
    /// test).
    #[instrument(name = "AdminService::ServerInfo", skip_all)]
    async fn server_info(
        &self,
        request: Request<ServerInfoRequest>,
    ) -> Result<Response<ServerInfoResponse>, Status> {
        info!("Request for ServerInfo");

        if let Some(verifier) = &*self.jwt_verifier {
            let raw_token = extract_bearer_token(request.metadata())
                .ok_or_else(|| Status::unauthenticated("authorization header required"))?;
            let claims = verifier
                .verify_token(&raw_token)
                .await
                .map_err(|e| Status::unauthenticated(format!("invalid token ({e:?})")))?;
            let verified_token = VerifiedToken::new(&raw_token, &claims);

            self.repository_authorizer
                .check_repository_access(
                    Some(&verified_token),
                    RepositoryId::default(),
                    Some(READ_ACTION),
                )
                .await
                .map_err(|_err| {
                    warn!(
                        "Attempt to read ServerInfo, but user does not have the correct permissions"
                    );
                    Status::permission_denied("Permission denied")
                })?;
        }

        Ok(Response::new(self.server_info.clone()))
    }

    async fn obliterate(
        &self,
        request: Request<lore_proto::ObliterateRequest>,
    ) -> Result<Response<lore_proto::ObliterateResponse>, Status> {
        timeout_grpc(
            self.rpc_timeout,
            obliterate::handler(
                request,
                self.immutable_store.clone(),
                self.mutable_store.clone(),
                self.notification.clone(),
                &self.hook_dispatcher,
                &self.jwt_verifier,
                self.repository_authorizer.clone(),
            ),
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Add;
    use std::time::Duration;
    use std::time::SystemTime;
    use std::time::UNIX_EPOCH;

    use async_trait::async_trait;
    use jsonwebtoken::Algorithm;
    use jsonwebtoken::DecodingKey;
    use jsonwebtoken::EncodingKey;
    use jsonwebtoken::Header;
    use jsonwebtoken::encode;
    use tonic::Code;
    use tonic::Request;
    use tonic::metadata::MetadataValue;

    use super::*;
    use crate::auth::jwk::JWKService;
    use crate::auth::jwk::JWKServiceError;
    use crate::auth::jwt::AuthorizationToken;
    use crate::authnz::repository_authorizer::GlobalGrantsAuthorizer;
    use crate::hooks::HookDispatcher;
    use crate::notification::testing::MockNotificationSender;
    use crate::store::test_store_create;

    /// `groups` is an ordinary named `AuthorizationToken` field (the Dex
    /// convention), matching `obliterate`'s own test setup — see its
    /// `GROUPS_CLAIM` doc comment.
    const GROUPS_CLAIM: &str = "groups";

    fn tier1_authorizer() -> Arc<dyn RepositoryAuthorizer> {
        Arc::new(GlobalGrantsAuthorizer::new(Some(GROUPS_CLAIM.to_string())))
    }

    const ALGORITHM: Algorithm = Algorithm::HS256;
    const SIGNING_SECRET: &str = "admin-service-test-secret";
    const TEST_AUDIENCE: &str = "lore-test";

    mockall::mock! {
        TestJWKService {}

        #[async_trait]
        impl JWKService for TestJWKService {
            async fn get_key(
                &self,
                kid: &str,
            ) -> Result<(DecodingKey, jsonwebtoken::Algorithm), JWKServiceError>;

            fn get_cached_key(
                &self,
                kid: &str,
            ) -> Option<(DecodingKey, jsonwebtoken::Algorithm)>;

            async fn refresh_key(
                &self,
                kid: &str,
            ) -> Result<Option<(DecodingKey, jsonwebtoken::Algorithm)>, JWKServiceError>;
        }
    }

    fn make_verifier(jwk_service: MockTestJWKService) -> JwtVerifier {
        JwtVerifier {
            jwk_service: Arc::new(jwk_service),
            jwt_issuer: None,
            jwt_audience: Some(vec![TEST_AUDIENCE.to_string()]),
        }
    }

    fn make_jwt(groups: Option<Vec<String>>) -> String {
        let claims = AuthorizationToken {
            user_id: "test-user".to_string(),
            issuer: "test-issuer".to_string(),
            issued_at: 1,
            expires: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .add(Duration::from_secs(60))
                .as_secs(),
            audience: vec![TEST_AUDIENCE.to_string()],
            env: Some("test".to_string()),
            name: Some("test".to_string()),
            preferred_username: Some("test".to_string()),
            client_id: None,
            resources: None,
            groups,
            is_service_account: Some(false),
            idp: Some("test".to_string()),
            extra: Default::default(),
        };
        let key = EncodingKey::from_secret(SIGNING_SECRET.as_ref());
        let mut header = Header::new(ALGORITHM);
        header.kid = Some("test-kid".to_string());
        encode(&header, &claims, &key).unwrap()
    }

    fn good_key_service() -> MockTestJWKService {
        let mut service = MockTestJWKService::new();
        service
            .expect_get_key()
            .returning(|_| Ok((DecodingKey::from_secret(SIGNING_SECRET.as_ref()), ALGORITHM)));
        service
    }

    fn make_service(
        jwt_verifier: Option<JwtVerifier>,
        repository_authorizer: Arc<dyn RepositoryAuthorizer>,
        immutable_store: Arc<dyn lore_storage::ImmutableStore>,
        mutable_store: Arc<dyn lore_storage::MutableStore>,
    ) -> LoreAdminService {
        let mut service = LoreAdminService::new(
            HashMap::new(),
            Vec::new(),
            immutable_store,
            mutable_store,
            Arc::new(MockNotificationSender::new()),
            Arc::new(HookDispatcher::empty()),
        );
        service.set_jwt_verifier(jwt_verifier);
        service.set_repository_authorizer(repository_authorizer);
        service
    }

    fn make_request(auth_header: Option<String>) -> Request<ServerInfoRequest> {
        let mut request = Request::new(ServerInfoRequest {});
        if let Some(token) = auth_header {
            let value: MetadataValue<tonic::metadata::Ascii> =
                format!("Bearer {token}").parse().unwrap();
            request.metadata_mut().insert("authorization", value);
        }
        request
    }

    /// `ServerInfo` was fully anonymous before this change, and stays that
    /// way when no `[server.auth]` is configured — matching `obliterate`'s
    /// own `proceeds_without_auth_when_no_verifier_configured` test.
    #[tokio::test]
    async fn no_verifier_configured_answers_without_auth() {
        let (immutable_store, mutable_store, _) = test_store_create().await.unwrap();
        let service = make_service(
            None,
            Arc::new(AllowAllRepositoryAuthorizer),
            immutable_store,
            mutable_store,
        );

        let response = service
            .server_info(make_request(None))
            .await
            .expect("no verifier configured must not require a token")
            .into_inner();
        assert_eq!(response.version, service.server_info.version);
    }

    #[tokio::test]
    async fn verifier_configured_without_token_is_unauthenticated() {
        let (immutable_store, mutable_store, _) = test_store_create().await.unwrap();
        // Key service not expected to be called — fail fast if it is.
        let verifier = make_verifier(MockTestJWKService::new());
        let service = make_service(
            Some(verifier),
            tier1_authorizer(),
            immutable_store,
            mutable_store,
        );

        let err = service.server_info(make_request(None)).await.unwrap_err();
        assert_eq!(err.code(), Code::Unauthenticated);
    }

    /// An authenticated caller that does not hold the `read` action is
    /// denied, once `[server.auth]` is configured.
    #[tokio::test]
    async fn verifier_configured_token_without_read_is_denied() {
        let (immutable_store, mutable_store, _) = test_store_create().await.unwrap();
        let verifier = make_verifier(good_key_service());
        let service = make_service(
            Some(verifier),
            tier1_authorizer(),
            immutable_store,
            mutable_store,
        );
        let groups = vec!["obliterate".to_string()];

        let err = service
            .server_info(make_request(Some(make_jwt(Some(groups)))))
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::PermissionDenied);
    }

    /// An authenticated caller holding the `read` action succeeds, once
    /// `[server.auth]` is configured.
    #[tokio::test]
    async fn verifier_configured_token_with_read_succeeds() {
        let (immutable_store, mutable_store, _) = test_store_create().await.unwrap();
        let verifier = make_verifier(good_key_service());
        let service = make_service(
            Some(verifier),
            tier1_authorizer(),
            immutable_store,
            mutable_store,
        );
        let groups = vec!["read".to_string()];

        let response = service
            .server_info(make_request(Some(make_jwt(Some(groups)))))
            .await
            .expect("token holding read must succeed")
            .into_inner();
        assert_eq!(response.version, service.server_info.version);
    }
}
