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
use crate::auth::jwt::verify_authorization;
use crate::correlation::CorrelationId;
use crate::protocol::attribute_map::AttributeMap;
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

        // Before any verification or context mutation: a rejected Connect
        // must not touch a connection already bound to another repository,
        // or it leaves that connection holding this request's token.
        if let Some(id) = context.get::<RepositoryId>()
            && *id != self.repository
        {
            warn!("Attempted to set repository id for connection, but it was already set!");
            return Err(MessageHandleError::AlreadyConnected);
        }

        if let Some(jwt_verifier) = jwt_verifier.as_ref() {
            match self.auth_token.as_ref() {
                Some(auth_token) => {
                    let authorization = jwt_verifier
                        .verify_token(auth_token)
                        .await
                        .map_err(|err| MessageHandleError::AuthorizationFailure(err.to_string()))?;
                    verify_authorization(&authorization, self.repository)
                        .map_err(|err| MessageHandleError::AuthorizationFailure(err.to_string()))?;
                    context.insert(authorization.clone());
                    if let Some(span) = context.get::<tracing::Span>() {
                        span.record(USER_ID, get_user_id_from_token(Some(authorization)));
                    }
                }
                None => {
                    return Err(MessageHandleError::MissingToken);
                }
            }
        }

        // A first connect, or a reconnect to the same repository; the
        // mismatched case returned above.
        context.insert(self.repository);
        Ok(LoreResponse::Connect(ConnectResponse::default()))
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
    use crate::auth::jwt::AuthorizationToken;

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
                .handle_auth(context.clone(), Arc::new(None))
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
                .handle_auth(context, Arc::new(None))
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
            message.handle_auth(context, Arc::new(None)).await.unwrap()
        );
    }

    /// A reconnect naming a different repository is refused, and refusing it
    /// must leave the connection's existing token alone: upstream verified
    /// and inserted the new token before noticing the mismatch, so a
    /// rejected Connect could rebind an established connection's identity.
    #[tokio::test]
    async fn a_rejected_reconnect_leaves_the_existing_token_alone() {
        let established = random::<RepositoryId>();
        let context = Arc::new(AttributeMap::default());
        context.insert(established);
        let original = AuthorizationToken {
            user_id: "original".to_string(),
            ..Default::default()
        };
        context.insert(original);

        let message = Connect {
            repository: random::<RepositoryId>(),
            auth_token: Some("would-be-verified".to_string()),
        };
        let result = message.handle_auth(context.clone(), Arc::new(None)).await;

        assert!(matches!(result, Err(MessageHandleError::AlreadyConnected)));
        assert_eq!(
            context
                .get::<AuthorizationToken>()
                .expect("token should survive a rejected reconnect")
                .user_id,
            "original",
        );
        assert_eq!(
            *context.get::<RepositoryId>().expect("repository"),
            established
        );
    }
}
