// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use lore_base::runtime::LORE_CONTEXT;
use lore_io::lease::IoLease;

use super::*;

// Supplied credentials never read or write the user's credential store. Both
// tokens name alice and example.com, expire in 2033, and differ in scope.
const AUTHN_ONE: &str = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJsb3JlIiwic3ViIjoiYWxpY2UiLCJleHAiOjIwMDAwMDAwMDAsImF1ZCI6WyJleGFtcGxlLmNvbSJdLCJuYW1lIjoiQWxpY2UifQ.signature";
const AUTHN_TWO: &str = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJsb3JlIiwic3ViIjoiYWxpY2UiLCJleHAiOjIwMDAwMDAwMDAsImF1ZCI6WyJleGFtcGxlLmNvbSJdLCJuYW1lIjoiQWxpY2UiLCJzY29wZSI6InJlYWQifQ.signature";
const AUTHZ_ONE: &str = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJsb3JlIiwic3ViIjoiYWxpY2UiLCJleHAiOjIwMDAwMDAwMDAsImF1ZCI6WyJleGFtcGxlLmNvbSJdLCJuYW1lIjoiQWxpY2UiLCJhdXRoeiI6Im9uZSJ9.signature";
const AUTHZ_TWO: &str = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJsb3JlIiwic3ViIjoiYWxpY2UiLCJleHAiOjIwMDAwMDAwMDAsImF1ZCI6WyJleGFtcGxlLmNvbSJdLCJuYW1lIjoiQWxpY2UiLCJhdXRoeiI6InR3byJ9.signature";

struct ContextMarker;
struct Parent;
struct Cleanup(Arc<parking_lot::RwLock<GRPCAuth>>);
impl Drop for Cleanup {
    fn drop(&mut self) {
        let refresher = self.0.write().refresher.take();
        if let Some(refresher) = refresher {
            refresher.abort();
        }
    }
}
struct Observation {
    token: String,
    parent_io: bool,
    context: bool,
}
struct AuthenticationProbe {
    context: Arc<ContextMarker>,
    observations: parking_lot::Mutex<Vec<Observation>>,
}
impl AuthenticationProbe {
    fn exchange(&self, token: &str) -> Result<AuthorizationToken, ProtocolError> {
        let same_context = lore_base::runtime::try_lore_context()
            .and_then(|context| context.downcast::<ContextMarker>().ok())
            .is_some_and(|context| Arc::ptr_eq(&context, &self.context));
        self.observations.lock().push(Observation {
            token: token.to_string(),
            parent_io: IoLease::current().is_some(),
            context: same_context,
        });
        Ok(AuthorizationToken {
            token: if token == AUTHN_ONE {
                AUTHZ_ONE
            } else {
                AUTHZ_TWO
            }
            .to_string(),
            expires_ms: 0,
            acceptable_root_domains: vec!["example.com".to_string()],
        })
    }
}
fn unsupported() -> ProtocolError {
    ProtocolError::internal("fixture only exchanges supplied credentials")
}
#[async_trait]
impl Authentication for AuthenticationProbe {
    async fn start_auth_session(
        &self,
        _url: &str,
        _state: &str,
        _correlation: &str,
    ) -> Result<AuthSession, ProtocolError> {
        Err(unsupported())
    }
    async fn poll_auth_session(
        &self,
        _url: &str,
        _state: &str,
        _code: &str,
        _correlation: &str,
    ) -> Result<AuthSessionPoll, ProtocolError> {
        Err(unsupported())
    }
    async fn exchange_external_token(
        &self,
        _url: &str,
        _token: &str,
        _kind: &str,
        _correlation: &str,
    ) -> Result<AuthenticationToken, ProtocolError> {
        Err(unsupported())
    }
    async fn authenticate_client(
        &self,
        _url: &str,
        _id: &str,
        _secret: &str,
        _correlation: &str,
    ) -> Result<AuthenticationToken, ProtocolError> {
        Err(unsupported())
    }
    async fn refresh_authentication(
        &self,
        _url: &str,
        _token: &str,
        _correlation: &str,
    ) -> Result<AuthenticationToken, ProtocolError> {
        Err(unsupported())
    }
    async fn exchange_for_repository(
        &self,
        _url: &str,
        token: &str,
        _repository: RepositoryId,
        _correlation: &str,
    ) -> Result<AuthorizationToken, ProtocolError> {
        self.exchange(token)
    }
    async fn exchange_for_custom_resource(
        &self,
        _url: &str,
        token: &str,
        _resource: &str,
        _correlation: &str,
    ) -> Result<AuthorizationToken, ProtocolError> {
        self.exchange(token)
    }
}
async fn lifecycle(custom_resource: bool) {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    let context = Arc::new(ContextMarker);
    let probe = Arc::new(AuthenticationProbe {
        context: context.clone(),
        observations: parking_lot::Mutex::new(Vec::new()),
    });
    let endpoint = AuthEndpoint::new(
        format!(
            "https://auth.example.com/refresher-lease/{}",
            NEXT.fetch_add(1, Ordering::Relaxed)
        ),
        probe.clone(),
    );
    let credentials = Arc::new(SuppliedCredentials::new(AUTHN_ONE, ""));
    let parent = Arc::new(Parent);
    let weak_parent = Arc::downgrade(&parent);
    let lease = IoLease::new(parent);
    let auth = LORE_CONTEXT
        .scope(
            context,
            lease.scope(async {
                if custom_resource {
                    GRPCAuth::new_for_custom_resource(
                        &endpoint,
                        "example.com",
                        "alice",
                        "lease-resource",
                        &credentials,
                    )
                    .await
                } else {
                    GRPCAuth::new(
                        &endpoint,
                        "example.com",
                        "alice",
                        [91; 16].into(),
                        &credentials,
                    )
                    .await
                }
            }),
        )
        .await;
    let cleanup = Cleanup(auth.clone());
    assert_eq!(auth.read().authorization_token, AUTHZ_ONE);
    drop(lease);
    assert!(
        weak_parent.upgrade().is_none(),
        "the live refresher must not retain instance IO ownership"
    );
    assert!(!auth.read().refresher.as_ref().unwrap().is_finished());
    {
        let seen = probe.observations.lock();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].token, AUTHN_ONE);
        assert!(seen[0].parent_io, "the initial exchange remains scoped");
        assert!(seen[0].context);
    }
    credentials.update(AUTHN_TWO, "");
    tokio::time::timeout(Duration::from_secs(10), async {
        while auth.read().authorization_token != AUTHZ_TWO {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    {
        let seen = probe.observations.lock();
        assert_eq!(seen.len(), 2);
        assert_eq!(seen[1].token, AUTHN_TWO);
        assert!(!seen[1].parent_io);
        assert!(seen[1].context, "only IO ownership is detached");
    }
    let refresher = auth.write().refresher.take().unwrap();
    drop(cleanup);
    drop(auth);
    credentials.update(AUTHN_ONE, "");
    tokio::time::timeout(Duration::from_secs(10), refresher)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        probe.observations.lock().len(),
        2,
        "retired auth performs no further exchange"
    );
}
#[tokio::test]
async fn repository_refresher_releases_parent_io_and_preserves_context_and_rotation() {
    lifecycle(false).await;
}
#[tokio::test]
async fn custom_resource_refresher_releases_parent_io_and_preserves_context_and_rotation() {
    lifecycle(true).await;
}
