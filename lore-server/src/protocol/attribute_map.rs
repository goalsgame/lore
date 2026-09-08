// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use std::any::Any;
use std::any::TypeId;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;

use lore_revision::lore::RepositoryId;
use tracing::warn;

use crate::protocol::storage::messages::ConnectionAuthorization;
use crate::util::get_user_id_from_token;

type AnyMap = HashMap<TypeId, Arc<dyn Any + Send + Sync>>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConnectionId(pub usize);

#[derive(Default)]
pub struct AttributeMap {
    map: Arc<RwLock<AnyMap>>,
}

impl AttributeMap {
    pub fn insert<T: Send + Sync + 'static>(&self, val: T) {
        match self.map.write() {
            Ok(mut m) => {
                m.insert(TypeId::of::<T>(), Arc::new(val));
            }
            Err(e) => {
                warn!("Failed to get write lock when writing to attribute map: {e:?}");
            }
        }
    }

    pub fn get<T: Send + Sync + 'static>(&self) -> Option<Arc<T>> {
        match self.map.read() {
            Ok(m) => m
                .get(&TypeId::of::<T>())
                .and_then(|boxed| boxed.clone().downcast().ok()),
            Err(e) => {
                warn!("Failed to get read lock when reading from attribute map: {e:?}");
                None
            }
        }
    }

    #[allow(clippy::type_complexity)]
    pub fn get_five<
        T1: Send + Sync + 'static,
        T2: Send + Sync + 'static,
        T3: Send + Sync + 'static,
        T4: Send + Sync + 'static,
        T5: Send + Sync + 'static,
    >(
        &self,
    ) -> (
        Option<Arc<T1>>,
        Option<Arc<T2>>,
        Option<Arc<T3>>,
        Option<Arc<T4>>,
        Option<Arc<T5>>,
    ) {
        match self.map.read() {
            Ok(m) => {
                let v1 = m
                    .get(&TypeId::of::<T1>())
                    .and_then(|boxed| boxed.clone().downcast().ok());
                let v2 = m
                    .get(&TypeId::of::<T2>())
                    .and_then(|boxed| boxed.clone().downcast().ok());
                let v3 = m
                    .get(&TypeId::of::<T3>())
                    .and_then(|boxed| boxed.clone().downcast().ok());
                let v4 = m
                    .get(&TypeId::of::<T4>())
                    .and_then(|boxed| boxed.clone().downcast().ok());
                let v5 = m
                    .get(&TypeId::of::<T5>())
                    .and_then(|boxed| boxed.clone().downcast().ok());
                (v1, v2, v3, v4, v5)
            }
            Err(e) => {
                warn!("Failed to get read lock when reading from attribute map: {e:?}");
                (None, None, None, None, None)
            }
        }
    }

    pub fn get_or<T: Send + Sync + 'static, E>(&self, err: E) -> Result<Arc<T>, E> {
        match self.get::<T>() {
            Some(v) => Ok(v),
            None => Err(err),
        }
    }
}

/// Resolves the connection's user id for attribution (audit logs,
/// notifications, hooks). Reads `ConnectionAuthorization` — not a bare
/// `AuthorizationToken` — because `Connect::handle_auth`
/// (`protocol/storage/connect.rs`) inserts the verified token only as part
/// of that single combined entry (see its docs for why); a direct
/// `context.get::<AuthorizationToken>()` here would always return `None`
/// and silently attribute every authenticated request to `"<unknown>"`.
pub fn get_user_id_from_context(context: &Arc<AttributeMap>) -> String {
    let token = context
        .get::<ConnectionAuthorization>()
        .and_then(|auth| match auth.as_ref() {
            ConnectionAuthorization::Verified { token, .. } => Some((**token).clone()),
            ConnectionAuthorization::Open => None,
        });
    get_user_id_from_token(token)
}

pub fn repository_id_from_context(context: &Arc<AttributeMap>) -> String {
    context
        .get::<RepositoryId>()
        .map_or_else(|| "<no_repo_id>".to_string(), |id| id.to_string())
}

#[cfg(test)]
mod tests {
    use lore_base::lore_spawn;

    use super::*;
    use crate::auth::jwt::AuthorizationToken;
    use crate::authnz::repository_authorizer::ReachabilityAuthorizer;

    /// Regression coverage for the user-attribution bug fix #4 introduced:
    /// `Connect::handle_auth` stopped inserting a bare `AuthorizationToken`
    /// once it consolidated to a single `ConnectionAuthorization` entry, so
    /// `get_user_id_from_context` must read the token out of
    /// `ConnectionAuthorization::Verified` rather than looking for the type
    /// that is no longer inserted — otherwise every authenticated request
    /// silently attributes to `"<unknown>"`.
    #[test]
    fn get_user_id_from_context_returns_real_user_id_when_verified() {
        let context = Arc::new(AttributeMap::default());
        context.insert(ConnectionAuthorization::Verified {
            token: Box::new(AuthorizationToken {
                user_id: "alice".to_string(),
                ..Default::default()
            }),
            reachability_authorizer: ReachabilityAuthorizer::new(None, None)
                .expect("no config never fails to construct"),
        });

        assert_eq!(get_user_id_from_context(&context), "alice");
    }

    #[test]
    fn get_user_id_from_context_returns_unknown_when_open() {
        let context = Arc::new(AttributeMap::default());
        context.insert(ConnectionAuthorization::Open);

        assert_eq!(get_user_id_from_context(&context), "<unknown>");
    }

    #[test]
    fn get_user_id_from_context_returns_unknown_when_absent() {
        let context = Arc::new(AttributeMap::default());

        assert_eq!(get_user_id_from_context(&context), "<unknown>");
    }

    #[derive(Clone, Debug, Eq, PartialEq)]
    struct TestData {
        foo: &'static str,
        bar: Vec<u8>,
    }

    #[tokio::test]
    async fn test_attribute_map() {
        let map = Arc::new(AttributeMap::default());

        let m: Arc<AttributeMap> = Arc::clone(&map);
        lore_spawn!(async move {
            m.insert(42);
        })
        .await
        .expect("failed to await");

        assert_eq!(&42, &*map.get::<i32>().unwrap());

        let m: Arc<AttributeMap> = Arc::clone(&map);
        lore_spawn!(async move {
            m.insert(834);
        })
        .await
        .expect("failed to await");

        assert_eq!(&834, &*map.get::<i32>().unwrap());

        let data = TestData {
            foo: "bar",
            bar: b"hello".to_vec(),
        };
        let data_clone = data.clone();

        let m: Arc<AttributeMap> = Arc::clone(&map);
        lore_spawn!(async move {
            m.insert(data_clone);
        })
        .await
        .expect("failed to await");

        assert_eq!(&data, &*map.get::<TestData>().unwrap());
    }

    #[test]
    fn test_get_or() {
        let map = AttributeMap::default();

        map.insert(42);

        assert_eq!(
            &42,
            &*map.get_or::<i32, &str>("Not Found").expect("failed to get")
        );

        assert_eq!(
            "Not Found",
            map.get_or::<TestData, &str>("Not Found")
                .expect_err("should have returned an error")
        );
    }
}
