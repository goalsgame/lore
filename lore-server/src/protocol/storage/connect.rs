// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use lore_revision::lore::RepositoryId;
use lore_telemetry::tracing::fields::USER_ID;
use tracing::debug;
use tracing::warn;

use crate::auth::jwt::JwtVerifier;
use crate::authnz::repository_authorizer::ReachabilityAuthorizer;
use crate::authnz::repository_authorizer::VerifiedToken;
use crate::authnz::repository_authorizer::resolve_baseline_actions;
use crate::correlation::CorrelationId;
use crate::protocol::attribute_map::AttributeMap;
use crate::protocol::storage::messages::ConnectionAuthorization;
use crate::protocol::storage::messages::LoreResponse;
use crate::protocol::storage::messages::Message;
use crate::protocol::storage::messages::MessageHandleError;
use crate::protocol::storage::messages::MessageParseError;
use crate::protocol::storage::messages::Response;
use crate::util::get_user_id_from_token;

#[derive(Clone, Debug, PartialEq)]
pub struct Connect {
    pub repository: RepositoryId,
    pub auth_token: Option<String>,
}

impl Connect {
    pub fn parse(bytes: Bytes) -> Result<Self, MessageParseError>
    where
        Self: Sized,
    {
        if bytes.len() < size_of::<RepositoryId>() {
            return Err(MessageParseError::InvalidFieldLength);
        }

        let mut bytes = bytes;
        let context = bytes.split_to(size_of::<RepositoryId>()).into();

        let auth_token: Option<String> = if !bytes.is_empty() {
            String::from_utf8(bytes.to_vec()).ok()
        } else {
            None
        };

        Ok(Self {
            repository: context,
            auth_token,
        })
    }
}

#[async_trait]
impl Message for Connect {
    #[tracing::instrument(name = "Connect::handle_auth", skip_all)]
    async fn handle_auth(
        &self,
        context: Arc<AttributeMap>,
        jwt_verifier: Arc<Option<JwtVerifier>>,
        reachability_authorizer: ReachabilityAuthorizer,
    ) -> Result<LoreResponse, MessageHandleError> {
        // Make sure a correlation ID exists
        if context.get::<CorrelationId>().is_none() {
            warn!("Connection is missing correlation ID");
            let correlation_id = CorrelationId::default();

            if let Some(span) = context.get::<tracing::Span>() {
                span.record("correlation_id", correlation_id.to_string());
            }

            context.insert(correlation_id);
        }

        if let Some(span) = context.get::<tracing::Span>() {
            span.record("repository_id", self.repository.to_string());
        }

        debug!("Handling connect request");

        if let Some(jwt_verifier) = jwt_verifier.as_ref() {
            match self.auth_token.as_ref() {
                Some(auth_token) => {
                    let authorization = jwt_verifier
                        .verify_token(auth_token)
                        .await
                        .map_err(|err| MessageHandleError::AuthorizationFailure(err.to_string()))?;
                    // Once per connection, so — unlike the per-fragment `Copy`
                    // check that reuses this same authorizer below — this can
                    // afford to go through the configured authorizer
                    // unconditionally, `AuthClientAuthorizer`'s online check
                    // included, at the granularity that authorizer was built
                    // for. Resolves both baseline actions rather than plain
                    // reachability: together they subsume it (a legacy
                    // deployment answers both from the same "listed among
                    // the allowed resources at all" check reachability used
                    // — see `RepositoryAuthorizer`'s doc comment on
                    // `read`/`push` having no legacy equivalent), and the
                    // central dispatcher (`quic/storage_service.rs`) checks
                    // the cached result on every subsequent request on this
                    // connection instead of asking again.
                    let verified_token = VerifiedToken::new(auth_token, &authorization);
                    // Two independent calls against the configured
                    // authorizer, each of which can fail on its own (most
                    // notably `AuthClientAuthorizer`'s online call timing
                    // out or erroring) rather than answering "denied" —
                    // `resolve_baseline_actions` keeps that distinguishable
                    // rather than folding it into "holds neither", which
                    // would otherwise get cached on `ConnectionAuthorization`
                    // for this connection's entire lifetime.
                    let (holds_read, holds_push) = resolve_baseline_actions(
                        reachability_authorizer.authorizer.as_ref(),
                        &verified_token,
                        self.repository,
                    )
                    .await
                    .map_err(|status| {
                        warn!("Failed to resolve read/push actions: {status}");
                        MessageHandleError::InternalError
                    })?;
                    if !holds_read && !holds_push {
                        return Err(MessageHandleError::AuthorizationFailure(
                            "caller holds neither read nor push".to_string(),
                        ));
                    }
                    if let Some(span) = context.get::<tracing::Span>() {
                        span.record(USER_ID, get_user_id_from_token(Some(authorization.clone())));
                    }
                    // `Copy` (protocol/storage/copy.rs) checks the *source*
                    // repository per fragment, which may differ from the
                    // repository this connection authorized against, so it
                    // needs its own authorization check. Bundling the token
                    // and the authorizer into one `ConnectionAuthorization`
                    // entry (rather than two independent ones) lets it reuse
                    // the same legacy-aware reachability logic and makes
                    // "one present without the other" impossible.
                    context.insert(ConnectionAuthorization::Verified {
                        token: Box::new(authorization),
                        reachability_authorizer,
                        holds_read,
                        holds_push,
                    });
                }
                None => {
                    return Err(MessageHandleError::MissingToken);
                }
            }
        } else {
            // No verifier configured for this deployment: `Copy` must still
            // see an explicit marker rather than infer "no auth" from
            // absence, so a message handled before `Connect` (or a future
            // transport that forgets to call it) is denied instead of
            // silently treated the same way.
            context.insert(ConnectionAuthorization::Open);
        }

        if let Some(id) = context.get::<RepositoryId>() {
            if *id != self.repository {
                warn!("Attempted to set repository id for connection, but it was already set!");
                Err(MessageHandleError::AlreadyConnected)
            } else {
                Ok(LoreResponse::Connect(ConnectResponse::default()))
            }
        } else {
            context.insert(self.repository);
            Ok(LoreResponse::Connect(ConnectResponse::default()))
        }
    }
}

#[derive(Debug, Default, PartialEq)]
pub struct ConnectResponse {}

impl Response for ConnectResponse {
    fn data(&self) -> Vec<Bytes> {
        vec![]
    }
}

#[cfg(test)]
mod tests {
    use rand::random;
    use zerocopy::IntoBytes;

    use super::*;

    /// No `[server.auth]` configured: `AllowAllRepositoryAuthorizer`, the
    /// same "auth disabled" shape these tests already exercise via
    /// `Arc::new(None)` for `jwt_verifier`.
    fn no_auth_reachability() -> ReachabilityAuthorizer {
        ReachabilityAuthorizer::new(None, None).expect("no config never fails to construct")
    }

    /// Regression for the error-collapsing bug: `Connect::handle_auth`
    /// resolves `read` and `push` as two independent calls against the
    /// configured authorizer. A failure of one of those calls (e.g. a
    /// legacy `AuthClientAuthorizer` deployment's online check timing out)
    /// must surface as a real error, not get silently folded into "does not
    /// hold this action" and cached on `ConnectionAuthorization` for the
    /// connection's entire lifetime.
    mod authorize_infra_failures {
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
        use tonic::Status;

        use super::*;
        use crate::auth::jwk::JWKService;
        use crate::auth::jwk::JWKServiceError;
        use crate::auth::jwt::AuthorizationToken;
        use crate::auth::jwt::JwtVerifier;
        use crate::authnz::repository_authorizer::PUSH_ACTION;
        use crate::authnz::repository_authorizer::RepositoryAuthorizer;

        const ALGORITHM: Algorithm = Algorithm::HS256;
        const SIGNING_SECRET: &str = "connect-infra-failure-test-secret";
        const AUDIENCE: &str = "lore-test";

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

        fn make_jwt() -> String {
            let claims = AuthorizationToken {
                user_id: "test-user".to_string(),
                issuer: "test-issuer".to_string(),
                issued_at: 1,
                expires: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .add(Duration::from_secs(60))
                    .as_secs(),
                audience: vec![AUDIENCE.to_string()],
                ..Default::default()
            };
            let key = EncodingKey::from_secret(SIGNING_SECRET.as_ref());
            let mut header = Header::new(ALGORITHM);
            header.kid = Some("test-kid".to_string());
            encode(&header, &claims, &key).unwrap()
        }

        fn jwt_verifier() -> Arc<Option<JwtVerifier>> {
            let mut jwk_service = MockTestJWKService::new();
            jwk_service
                .expect_get_key()
                .returning(|_| Ok((DecodingKey::from_secret(SIGNING_SECRET.as_ref()), ALGORITHM)));
            Arc::new(Some(JwtVerifier {
                jwk_service: Arc::new(jwk_service),
                jwt_issuer: None,
                jwt_audience: Some(vec![AUDIENCE.to_string()]),
            }))
        }

        /// Answers `read` normally but fails `push` with an infrastructure
        /// error, simulating an online authorizer whose call for one of the
        /// two actions errors or times out while the other succeeds.
        struct FlakyPushAuthorizer;

        #[async_trait]
        impl RepositoryAuthorizer for FlakyPushAuthorizer {
            async fn check_repository_access(
                &self,
                _token: Option<&VerifiedToken<'_>>,
                _repository: RepositoryId,
                action: Option<&str>,
            ) -> Result<(), Status> {
                if action == Some(PUSH_ACTION) {
                    Err(Status::internal("simulated auth service timeout"))
                } else {
                    Ok(())
                }
            }
        }

        fn flaky_push_reachability() -> ReachabilityAuthorizer {
            ReachabilityAuthorizer {
                authorizer: Arc::new(FlakyPushAuthorizer),
                legacy_resource_claim: false,
            }
        }

        #[tokio::test]
        async fn handle_auth_surfaces_infra_failure_instead_of_read_only_fallback() {
            let message = Connect {
                repository: random::<RepositoryId>(),
                auth_token: Some(make_jwt()),
            };
            let context = Arc::new(AttributeMap::default());

            let err = message
                .handle_auth(context, jwt_verifier(), flaky_push_reachability())
                .await
                .expect_err("a failed push check must not be silently treated as 'push not held'");

            // Must be a real, distinguishable failure — never the same
            // `AuthorizationFailure` a genuine denial produces, which would
            // be indistinguishable from "this caller holds neither action"
            // and would pin the connection read-only for its entire
            // lifetime.
            assert!(
                matches!(err, MessageHandleError::InternalError),
                "expected InternalError for a failed check, got {err:?}"
            );
        }

        /// Round 5 regression: a legacy `AuthClientAuthorizer` deployment's
        /// *only* way of denying `read`/`push` is the requested resource
        /// being absent from `CheckUserPermissionResponse
        /// .allowed_resource_permission` — which
        /// `interpret_check_user_permission_response`
        /// (`authnz/repository_authorizer.rs`) maps to
        /// `Status::permission_denied`, not `Status::internal`. This mock
        /// reproduces exactly that shape (denies both actions with
        /// `PermissionDenied`, mirroring what a real `AuthClientAuthorizer`
        /// now returns when a repository is simply unreachable) and proves
        /// `action_held` recognizes it as a real denial: a clean
        /// `AuthorizationFailure`, never `InternalError`.
        struct AuthClientShapedDenialAuthorizer;

        #[async_trait]
        impl RepositoryAuthorizer for AuthClientShapedDenialAuthorizer {
            async fn check_repository_access(
                &self,
                _token: Option<&VerifiedToken<'_>>,
                _repository: RepositoryId,
                action: Option<&str>,
            ) -> Result<(), Status> {
                Err(Status::permission_denied(format!(
                    "caller has no permissions for resource (action: {action:?})"
                )))
            }
        }

        fn auth_client_shaped_denial_reachability() -> ReachabilityAuthorizer {
            ReachabilityAuthorizer {
                authorizer: Arc::new(AuthClientShapedDenialAuthorizer),
                legacy_resource_claim: false,
            }
        }

        #[tokio::test]
        async fn handle_auth_gives_a_clean_rejection_for_an_auth_client_shaped_denial() {
            let message = Connect {
                repository: random::<RepositoryId>(),
                auth_token: Some(make_jwt()),
            };
            let context = Arc::new(AttributeMap::default());

            let err = message
                .handle_auth(
                    context,
                    jwt_verifier(),
                    auth_client_shaped_denial_reachability(),
                )
                .await
                .expect_err("a caller denied both actions must be rejected");

            assert!(
                matches!(err, MessageHandleError::AuthorizationFailure(_)),
                "expected a clean AuthorizationFailure for a real denial, got {err:?}"
            );
        }
    }

    #[test]
    fn test_parse() {
        let repository = random::<RepositoryId>();
        let auth_token: String = "my_auth_token".to_string();

        let message = Connect {
            repository,
            auth_token: Some(auth_token.clone()),
        };

        let mut message_bytes = bytes::BytesMut::new();
        message_bytes.extend_from_slice(repository.as_bytes());
        message_bytes.extend_from_slice(auth_token.as_bytes());

        assert_eq!(Connect::parse(message_bytes.freeze()), Ok(message));
    }

    #[tokio::test]
    async fn test_handle() {
        let repository = random::<RepositoryId>();

        let message = Connect {
            repository,
            auth_token: None,
        };

        let context = Arc::new(AttributeMap::default());

        assert_eq!(
            LoreResponse::Connect(ConnectResponse::default()),
            message
                .handle_auth(context.clone(), Arc::new(None), no_auth_reachability())
                .await
                .unwrap()
        );

        assert_eq!(repository, *context.get::<RepositoryId>().unwrap());
    }

    #[test]
    fn test_set_repository_not_enough_bytes() {
        let hash = random::<[u8; 12]>();
        let bytes = Bytes::copy_from_slice(hash.as_bytes());
        Connect::parse(bytes)
            .expect_err("Should have failed to parse, provided repo hash was not long enough");
    }

    #[tokio::test]
    async fn test_set_repository_already_set() {
        let message = Connect {
            repository: random::<RepositoryId>(),
            auth_token: None,
        };

        let context = Arc::new(AttributeMap::default());
        context.insert(random::<RepositoryId>());

        assert!(matches!(
            message
                .handle_auth(context, Arc::new(None), no_auth_reachability())
                .await
                .expect_err("expected error"),
            MessageHandleError::AlreadyConnected,
        ));
    }

    #[tokio::test]
    async fn test_set_repository_already_set_value_matched() {
        let repository = random::<RepositoryId>();

        let context = Arc::new(AttributeMap::default());
        context.insert(repository);

        let message = Connect {
            repository,
            auth_token: None,
        };

        assert_eq!(
            LoreResponse::Connect(ConnectResponse::default()),
            message
                .handle_auth(context, Arc::new(None), no_auth_reachability())
                .await
                .unwrap()
        );
    }
}
