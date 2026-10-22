// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use lore_base::types::Context;
use lore_base::types::RepositoryId;
use lore_proto::auth::CheckUserPermissionResponse;
use lore_proto::auth::ResourcePermission;
use lore_server::auth::jwt::AuthorizationToken;
use lore_server::authnz::repository_authorizer::*;
use lore_server::settings::AuthSettings;
use tonic::Code;
use tonic::Status;

fn response(entries: Vec<ResourcePermission>) -> CheckUserPermissionResponse {
    CheckUserPermissionResponse {
        allowed_resource_permission: entries,
        denied_resource_permission: vec![],
    }
}

fn entry(resource_id: &str, permissions: &[&str]) -> ResourcePermission {
    ResourcePermission {
        resource_id: resource_id.to_string(),
        permission: permissions.iter().map(ToString::to_string).collect(),
    }
}

#[tokio::test]
async fn allow_all_permits_every_token_action_combination() {
    let claims = AuthorizationToken::default();
    let token = VerifiedToken {
        raw: "raw",
        claims: &claims,
    };
    let repository: RepositoryId = Context::default().into();
    for token in [None, Some(&token)] {
        for action in [None, Some("obliterate")] {
            AllowAllRepositoryAuthorizer
                .check_repository_access(token, repository, action)
                .await
                .unwrap();
        }
    }
}

mod resources_claim {
    use std::str::FromStr;

    use lore_base::types::Context;

    use super::*;

    fn repository() -> RepositoryId {
        Context::from_str("0194b726b34e72b0b45550b88a967076")
            .unwrap()
            .into()
    }

    fn access_token_claims(
        resource_id: &str,
        permissions: &[&str],
    ) -> lore_server::auth::jwt::AuthorizationToken {
        AuthorizationToken {
            resources: Some(vec![lore_server::auth::jwt::ResourcePermission {
                resource_id: resource_id.to_string(),
                permission: permissions.iter().map(ToString::to_string).collect(),
            }]),
            ..Default::default()
        }
    }

    async fn check(claims: &AuthorizationToken, action: Option<&str>) -> Result<(), Status> {
        // A URL nothing listens on: reaching for the network here would
        // hang or error, so a verdict proves the claim answered in place.
        AuthClientAuthorizer::new("https://auth.invalid".to_string())
            .check_repository_access(
                Some(&VerifiedToken {
                    raw: "raw.jwt",
                    claims,
                }),
                repository(),
                action,
            )
            .await
    }

    /// The auth service refuses exchanged access tokens as
    /// `CheckUserPermission` credentials, so a token carrying a
    /// `resources` claim must be answered from the claim, never sent
    /// upstream.
    #[tokio::test]
    async fn an_access_token_is_answered_from_its_claim() {
        let granted = access_token_claims(
            &format!("urc-{}", repository()),
            &["read", "write", "migrate"],
        );
        check(&granted, None).await.unwrap();
        check(&granted, Some("migrate")).await.unwrap();
        check(&granted, Some("obliterate")).await.unwrap_err();

        let other_partition = access_token_claims("urc-somewhere-else", &["read"]);
        check(&other_partition, None).await.unwrap_err();

        // A wildcard grant reaches every partition, as everywhere else.
        let wildcard = access_token_claims("urc-*", &["read"]);
        check(&wildcard, None).await.unwrap();
    }

    /// A `resources` claim scoped to no partition denies everything —
    /// present-but-empty is a verdict, not a fallback to the network.
    #[tokio::test]
    async fn an_empty_resources_claim_denies() {
        let empty = AuthorizationToken {
            resources: Some(vec![]),
            ..Default::default()
        };
        check(&empty, None).await.unwrap_err();
        check(&empty, Some("read")).await.unwrap_err();
    }

    /// The enumeration and the per-question path answer from the same
    /// derivation, so their verdicts must agree for every access token.
    #[tokio::test]
    async fn enumeration_agrees_with_the_per_question_path() {
        let authorizer = AuthClientAuthorizer::new("https://auth.invalid".to_string());
        let tokens = [
            access_token_claims(&format!("urc-{}", repository()), &["read", "migrate"]),
            access_token_claims("urc-somewhere-else", &["read"]),
            access_token_claims("urc-*", &[]),
            AuthorizationToken {
                resources: Some(vec![]),
                ..Default::default()
            },
        ];
        for claims in &tokens {
            let token = VerifiedToken {
                raw: "raw.jwt",
                claims,
            };
            let grants = authorizer
                .granted_actions(Some(&token), repository())
                .await
                .unwrap()
                .expect("access tokens are enumerable");
            assert_eq!(
                grants.reachable(),
                check(claims, None).await.is_ok(),
                "reachability must agree for {claims:?}"
            );
            for action in ["read", "migrate", "obliterate"] {
                assert_eq!(
                    grants.permits(action),
                    check(claims, Some(action)).await.is_ok(),
                    "verdict for {action} must agree for {claims:?}"
                );
            }
        }
    }
}

mod sync_check {
    use std::str::FromStr;

    use lore_base::types::Context;

    use super::*;

    fn repository() -> RepositoryId {
        Context::from_str("0194b726b34e72b0b45550b88a967076")
            .unwrap()
            .into()
    }

    /// Implements only the required method, so the sync check is the
    /// trait's default.
    struct PolicyOnly;

    #[async_trait]
    impl RepositoryAuthorizer for PolicyOnly {
        async fn check_repository_access(
            &self,
            _token: Option<&VerifiedToken<'_>>,
            _repository_id: RepositoryId,
            _action: Option<&str>,
        ) -> Result<(), Status> {
            Ok(())
        }
    }

    #[test]
    fn default_cannot_answer_without_io() {
        assert!(
            PolicyOnly
                .check_repository_access_sync(None, repository(), None)
                .is_none()
        );
    }

    #[test]
    fn allow_all_answers_everything() {
        let claims = AuthorizationToken::default();
        let token = VerifiedToken {
            raw: "raw",
            claims: &claims,
        };
        for token in [None, Some(&token)] {
            for action in [None, Some("obliterate")] {
                AllowAllRepositoryAuthorizer
                    .check_repository_access_sync(token, repository(), action)
                    .expect("allow-all needs no I/O")
                    .unwrap();
            }
        }
    }

    /// An access token's `resources` claim is the auth service's own
    /// signed answer, so the legacy authorizer answers it in place and
    /// agrees with its async path. An identity token needs the network.
    #[tokio::test]
    async fn auth_client_answers_access_tokens_and_declines_identity_tokens() {
        let authorizer = AuthClientAuthorizer::new("https://auth.invalid".to_string());
        let granted = AuthorizationToken {
            resources: Some(vec![lore_server::auth::jwt::ResourcePermission {
                resource_id: format!("urc-{}", repository()),
                permission: vec!["migrate".to_string()],
            }]),
            ..Default::default()
        };
        let elsewhere = AuthorizationToken {
            resources: Some(vec![lore_server::auth::jwt::ResourcePermission {
                resource_id: "urc-somewhere-else".to_string(),
                permission: vec![],
            }]),
            ..Default::default()
        };
        for claims in [&granted, &elsewhere] {
            let token = VerifiedToken {
                raw: "raw.jwt",
                claims,
            };
            for action in [None, Some("migrate"), Some("obliterate")] {
                let sync = authorizer
                    .check_repository_access_sync(Some(&token), repository(), action)
                    .expect("access tokens are answered in place");
                let asynchronous = authorizer
                    .check_repository_access(Some(&token), repository(), action)
                    .await;
                assert_eq!(
                    sync.is_ok(),
                    asynchronous.is_ok(),
                    "{action:?} on {claims:?}"
                );
            }
        }

        let identity = AuthorizationToken::default();
        let token = VerifiedToken {
            raw: "raw.jwt",
            claims: &identity,
        };
        assert!(
            authorizer
                .check_repository_access_sync(Some(&token), repository(), None)
                .is_none()
        );
        assert!(
            authorizer
                .check_repository_access_sync(None, repository(), None)
                .is_none()
        );
    }
}

mod granted_access {
    use std::str::FromStr;

    use lore_base::types::Context;
    use lore_server::grpc::no_repository_access_status;

    use super::*;

    fn repository() -> RepositoryId {
        Context::from_str("0194b726b34e72b0b45550b88a967076")
            .unwrap()
            .into()
    }

    /// Non-enumerable: `granted_actions` keeps its `Ok(None)` default and
    /// only the per-question path answers.
    struct PolicyOnly(bool);

    #[async_trait]
    impl RepositoryAuthorizer for PolicyOnly {
        async fn check_repository_access(
            &self,
            _token: Option<&VerifiedToken<'_>>,
            _repository_id: RepositoryId,
            action: Option<&str>,
        ) -> Result<(), Status> {
            assert_eq!(action, None, "granted_access asks reachability only");
            if self.0 {
                Ok(())
            } else {
                Err(Status::permission_denied("the policy's own reason"))
            }
        }
    }

    #[tokio::test]
    async fn enumerable_authorizer_yields_its_grants() {
        let authorizer: Arc<dyn RepositoryAuthorizer> = Arc::new(AllowAllRepositoryAuthorizer);
        let grants = authorizer.granted_access(None, repository()).await.unwrap();
        assert_eq!(grants, Some(Grants::All));
    }

    #[tokio::test]
    async fn non_enumerable_authorizer_answers_reachability_with_no_grants() {
        let authorizer: Arc<dyn RepositoryAuthorizer> = Arc::new(PolicyOnly(true));
        let grants = authorizer.granted_access(None, repository()).await.unwrap();
        assert_eq!(grants, None);
    }

    /// Denials flatten to the uniform status whichever path produced
    /// them, so an unauthorized caller learns nothing from the reason.
    #[tokio::test]
    async fn denials_flatten_to_the_uniform_status() {
        let non_enumerable: Arc<dyn RepositoryAuthorizer> = Arc::new(PolicyOnly(false));
        let err = non_enumerable
            .granted_access(None, repository())
            .await
            .unwrap_err();
        assert_eq!(err.code(), no_repository_access_status().code());
        assert_eq!(err.message(), no_repository_access_status().message());

        let claims = AuthorizationToken {
            resources: Some(vec![lore_server::auth::jwt::ResourcePermission {
                resource_id: "urc-somewhere-else".to_string(),
                permission: vec![],
            }]),
            ..Default::default()
        };
        let token = VerifiedToken {
            raw: "raw.jwt",
            claims: &claims,
        };
        let enumerable: Arc<dyn RepositoryAuthorizer> = Arc::new(AuthClientAuthorizer::new(
            "https://auth.invalid".to_string(),
        ));
        let err = enumerable
            .granted_access(Some(&token), repository())
            .await
            .unwrap_err();
        assert_eq!(err.message(), no_repository_access_status().message());
    }
}

mod permits_helper {
    use std::str::FromStr;

    use lore_base::types::Context;
    use lore_server::authnz::repository_authorizer::RawToken;

    use super::*;

    fn repository() -> RepositoryId {
        Context::from_str("0194b726b34e72b0b45550b88a967076")
            .unwrap()
            .into()
    }

    fn extensions_with(grants: Option<PartitionGrants>, token: bool) -> tonic::Extensions {
        let mut extensions = tonic::Extensions::new();
        if let Some(grants) = grants {
            extensions.insert(grants);
        }
        if token {
            extensions.insert(AuthorizationToken::default());
            extensions.insert(RawToken("raw.jwt".into()));
        }
        extensions
    }

    /// The layer's enumerated grants answer first — proven by pairing an
    /// allow-all authorizer with grants that deny: the denial wins.
    #[tokio::test]
    async fn enumerated_grants_answer_before_the_authorizer() {
        let authorizer: Arc<dyn RepositoryAuthorizer> = Arc::new(AllowAllRepositoryAuthorizer);
        let extensions = extensions_with(
            Some(PartitionGrants {
                repository_id: repository(),
                grants: Grants::Actions(HashSet::new()),
                incarnation: None,
            }),
            true,
        );
        assert!(
            !authorizer
                .permits(&extensions, repository(), "migrate")
                .await
        );
    }

    /// Grants naming another partition are not consumed; the check falls
    /// back to the authorizer with the verified token.
    #[tokio::test]
    async fn grants_for_another_partition_fall_back_to_the_authorizer() {
        let authorizer: Arc<dyn RepositoryAuthorizer> = Arc::new(AllowAllRepositoryAuthorizer);
        let other: RepositoryId = Context::from_str("f6ca55437aa34198ba0f0fdc33154d51")
            .unwrap()
            .into();
        let extensions = extensions_with(
            Some(PartitionGrants {
                repository_id: other,
                grants: Grants::Denied,
                incarnation: None,
            }),
            true,
        );
        assert!(
            authorizer
                .permits(&extensions, repository(), "migrate")
                .await
        );
    }

    /// Without a verified token nothing is granted, whatever the
    /// authorizer would say.
    #[tokio::test]
    async fn no_token_and_no_grants_deny_even_under_allow_all() {
        let authorizer: Arc<dyn RepositoryAuthorizer> = Arc::new(AllowAllRepositoryAuthorizer);
        let extensions = extensions_with(None, false);
        assert!(
            !authorizer
                .permits(&extensions, repository(), "migrate")
                .await
        );
    }
}

mod grants {
    use super::*;

    #[test]
    fn permits_and_reachable_follow_the_variant() {
        assert!(Grants::All.reachable());
        assert!(Grants::All.permits("anything"));

        assert!(!Grants::Denied.reachable());
        assert!(!Grants::Denied.permits("anything"));

        let actions = Grants::Actions(["migrate".to_string()].into());
        assert!(actions.reachable());
        assert!(actions.permits("migrate"));
        assert!(!actions.permits("obliterate"));

        // Reachable with nothing granted: a matched entry with an empty
        // permission list, or Tier 1 without a permission claim.
        let none = Grants::Actions(HashSet::new());
        assert!(none.reachable());
        assert!(!none.permits("read"));
    }

    /// The response-derived grants merge every entry naming the
    /// resource and ignore the rest.
    #[test]
    fn response_grants_merge_matching_entries() {
        let response = response(vec![
            entry("urc-abc", &["read"]),
            entry("urc-other", &["obliterate"]),
            entry("urc-abc", &["migrate"]),
        ]);
        let grants = grants_from_response(&response, "urc-abc");
        assert!(grants.reachable());
        assert!(grants.permits("read"));
        assert!(grants.permits("migrate"));
        assert!(!grants.permits("obliterate"));

        assert_eq!(
            grants_from_response(&response, "urc-absent"),
            Grants::Denied
        );
    }
}

#[test]
fn bearer_header_rebuilds_the_forwarded_header() {
    let claims = AuthorizationToken::default();
    let token = VerifiedToken {
        raw: "abc.def.ghi",
        claims: &claims,
    };
    assert_eq!(
        bearer_header(Some(&token)),
        Some("Bearer abc.def.ghi".to_string())
    );
    assert_eq!(bearer_header(None), None);
}

#[test]
fn upstream_request_carries_resource_and_authorization() {
    let request =
        check_user_permission_request("urc-abc".into(), Some("Bearer tok".into())).unwrap();
    assert_eq!(request.get_ref().resource_id, vec!["urc-abc".to_string()]);
    assert_eq!(request.get_ref().target_user, None);
    assert_eq!(
        request
            .metadata()
            .get("authorization")
            .unwrap()
            .to_str()
            .unwrap(),
        "Bearer tok"
    );
}

#[test]
fn named_action_requires_membership_in_the_permission_list() {
    let response = response(vec![entry("urc-abc", &["obliterate"])]);
    evaluate_check_user_permission(&response, "urc-abc", Some("obliterate")).unwrap();
    evaluate_check_user_permission(&response, "urc-abc", None).unwrap();
    // The fail-open case: an authorizer that ignores the action would
    // permit this.
    let err = evaluate_check_user_permission(&response, "urc-abc", Some("presign")).unwrap_err();
    assert_eq!(err.code(), Code::PermissionDenied);
}

#[test]
fn empty_permission_list_denies_every_named_action() {
    let response = response(vec![entry("urc-abc", &[])]);
    let err = evaluate_check_user_permission(&response, "urc-abc", Some("obliterate")).unwrap_err();
    assert_eq!(err.code(), Code::PermissionDenied);
    // The resource still appears, which is all `None` asks.
    evaluate_check_user_permission(&response, "urc-abc", None).unwrap();
}

#[test]
fn absent_resource_denies_named_actions_and_plain_access() {
    let response = response(vec![]);
    let err = evaluate_check_user_permission(&response, "urc-abc", Some("obliterate")).unwrap_err();
    assert_eq!(err.code(), Code::PermissionDenied);
    let err = evaluate_check_user_permission(&response, "urc-abc", None).unwrap_err();
    assert_eq!(err.code(), Code::PermissionDenied);
}

#[test]
fn mismatched_resource_denies() {
    let response = response(vec![entry("urc-other", &["obliterate"])]);
    let err = evaluate_check_user_permission(&response, "urc-abc", Some("obliterate")).unwrap_err();
    assert_eq!(err.code(), Code::PermissionDenied);
    let err = evaluate_check_user_permission(&response, "urc-abc", None).unwrap_err();
    assert_eq!(err.code(), Code::PermissionDenied);
}

#[test]
fn plain_access_finds_the_resource_past_the_first_entry() {
    let response = response(vec![
        entry("urc-other", &["obliterate"]),
        entry("urc-abc", &[]),
    ]);
    evaluate_check_user_permission(&response, "urc-abc", None).unwrap();
    let err = evaluate_check_user_permission(&response, "urc-abc", Some("obliterate")).unwrap_err();
    assert_eq!(err.code(), Code::PermissionDenied);
}

/// `[server.auth]` settings with the mandatory pair present and `extra`
/// TOML appended, mirroring how a config file builds them.
fn auth_settings(extra: &str) -> AuthSettings {
    toml::from_str(&format!(
        "jwt_issuer = \"https://auth.example.com\"\njwt_audience = [\"lore\"]\n{extra}"
    ))
    .unwrap()
}

const AUTH_URL: Option<&str> = Some("https://legacy-auth.example.com");

#[tokio::test]
async fn selection_follows_the_flowchart() {
    // (auth settings, legacy auth_url) → selected implementation.
    let table = [
        (None, None, AuthorizerSelection::AllowAll),
        (
            Some(auth_settings("")),
            AUTH_URL,
            AuthorizerSelection::AuthClient,
        ),
        (
            Some(auth_settings("resource_claim = \"resources\"")),
            None,
            AuthorizerSelection::ResourceGrants,
        ),
        (
            Some(auth_settings("")),
            None,
            AuthorizerSelection::GlobalGrants,
        ),
        (
            Some(auth_settings("permission_claim = \"realm_access.roles\"")),
            None,
            AuthorizerSelection::GlobalGrants,
        ),
    ];
    for (auth, auth_url, expected) in table {
        assert_eq!(
            select_repository_authorizer(auth.as_ref(), auth_url).unwrap(),
            expected,
            "auth: {auth:?}, auth_url: {auth_url:?}"
        );
        // Construction agrees with selection for every valid row.
        let (immutable_store, mutable_store, _execution) =
            crate::store::test_support::test_store_create()
                .await
                .unwrap();
        repository_authorizer(
            auth.as_ref(),
            auth_url.map(ToString::to_string),
            immutable_store,
            mutable_store,
        )
        .unwrap();
    }
}

/// The check that makes a `UrcAuthApi` deployment's move onto the token
/// claims a deliberate step: setting `resource_claim` while `auth_url`
/// is still configured refuses to start instead of silently staying on
/// the auth service.
#[test]
fn auth_url_with_resource_claim_refuses_startup_naming_both() {
    let auth = auth_settings("resource_claim = \"resources\"");
    let message = select_repository_authorizer(Some(&auth), AUTH_URL)
        .unwrap_err()
        .to_string();
    assert!(message.contains("auth_url"), "{message}");
    assert!(message.contains("resource_claim"), "{message}");
}

/// `auth_url` names an authorization service, but without `[server.auth]`
/// nothing verifies tokens: the server would run open while the operator
/// believed they had configured authorization. Refused, naming both.
#[test]
fn auth_url_without_server_auth_refuses_startup_naming_both() {
    let message = select_repository_authorizer(None, AUTH_URL)
        .unwrap_err()
        .to_string();
    assert!(message.contains("auth_url"), "{message}");
    assert!(message.contains("[server.auth]"), "{message}");
}

/// The startup log prints the `Display` form, so an operator can tell
/// which implementation they got.
#[test]
fn selection_display_names_the_implementation() {
    assert_eq!(
        AuthorizerSelection::AllowAll.to_string(),
        "AllowAllRepositoryAuthorizer"
    );
    assert_eq!(
        AuthorizerSelection::AuthClient.to_string(),
        "AuthClientAuthorizer"
    );
    assert_eq!(
        AuthorizerSelection::GlobalGrants.to_string(),
        "GlobalGrantsAuthorizer"
    );
    assert_eq!(
        AuthorizerSelection::ResourceGrants.to_string(),
        "ResourceGrantsAuthorizer"
    );
}

mod authorize_create {
    use lore_server::auth::jwt::ResourcePermission as ClaimEntry;

    use super::*;

    fn claims(resources: Option<Vec<(&str, &[&str])>>) -> AuthorizationToken {
        AuthorizationToken {
            resources: resources.map(|entries| {
                entries
                    .into_iter()
                    .map(|(resource_id, permissions)| ClaimEntry {
                        resource_id: resource_id.to_string(),
                        permission: permissions.iter().map(ToString::to_string).collect(),
                    })
                    .collect()
            }),
            ..Default::default()
        }
    }

    fn check(claims: &AuthorizationToken, name: &str) -> Result<(), Status> {
        let token = VerifiedToken { raw: "raw", claims };
        authorize_repository_create(Some(&token), name)
    }

    #[test]
    fn no_token_or_no_resources_claim_is_not_restricted() {
        authorize_repository_create(None, "game").unwrap();
        check(&claims(None), "game").unwrap();
    }

    #[test]
    fn token_without_name_entries_is_not_restricted() {
        let claims = claims(Some(vec![("urc-*", &["read"])]));
        check(&claims, "game").unwrap();
    }

    #[test]
    fn scoped_token_creates_matching_names_only() {
        let claims = claims(Some(vec![
            ("urc-*", &["read"]),
            ("urc-name:goalables/*", &["create"]),
        ]));
        check(&claims, "goalables/app").unwrap();
        for name in ["game", "goalables/a/b", "goalables", "goalables2/app"] {
            let err = check(&claims, name).expect_err(name);
            assert_eq!(err.code(), Code::PermissionDenied, "{name}");
        }
    }

    #[test]
    fn missing_create_permission_denies() {
        let claims = claims(Some(vec![("urc-name:goalables/*", &["read", "push"])]));
        let err = check(&claims, "goalables/app").expect_err("no create");
        assert_eq!(err.code(), Code::PermissionDenied);
    }

    #[test]
    fn multiple_patterns() {
        let claims = claims(Some(vec![
            ("urc-name:goalables/*", &["create"]),
            ("urc-name:tools/**", &["create"]),
        ]));
        check(&claims, "goalables/app").unwrap();
        check(&claims, "tools/a/b").unwrap();
        assert!(check(&claims, "game").is_err());
    }
}

mod name_scoped_grants {
    use std::collections::HashMap;

    use lore_server::auth::jwt::ResourcePermission as ClaimEntry;
    use lore_server::authnz::repository_name_resolver::RepositoryNameResolver;

    use super::*;

    struct FixedNames(HashMap<RepositoryId, Arc<str>>);

    #[async_trait]
    impl RepositoryNameResolver for FixedNames {
        async fn resolve(&self, repository_id: RepositoryId) -> Result<Option<Arc<str>>, Status> {
            Ok(self.0.get(&repository_id).cloned())
        }

        async fn incarnation(
            &self,
            _repository_id: RepositoryId,
        ) -> Result<Option<lore_base::types::Hash>, Status> {
            Ok(None)
        }
    }

    struct Fixture {
        authorizer: AuthClientAuthorizer,
        app: RepositoryId,
        game: RepositoryId,
        unknown: RepositoryId,
    }

    fn fixture() -> Fixture {
        let app: RepositoryId = rand::random();
        let game: RepositoryId = rand::random();
        let names = FixedNames(HashMap::from([
            (app, Arc::from("goalables/app")),
            (game, Arc::from("game")),
        ]));
        Fixture {
            // A URL nothing listens on: a verdict proves the claim answered in place.
            authorizer: AuthClientAuthorizer::new("https://auth.invalid".to_string())
                .with_name_resolver(Arc::new(names)),
            app,
            game,
            unknown: rand::random(),
        }
    }

    fn claims(entries: &[(&str, &[&str])]) -> AuthorizationToken {
        AuthorizationToken {
            resources: Some(
                entries
                    .iter()
                    .map(|(resource_id, permissions)| ClaimEntry {
                        resource_id: resource_id.to_string(),
                        permission: permissions.iter().map(ToString::to_string).collect(),
                    })
                    .collect(),
            ),
            ..Default::default()
        }
    }

    async fn check(
        authorizer: &AuthClientAuthorizer,
        claims: &AuthorizationToken,
        repository: RepositoryId,
        action: Option<&str>,
    ) -> bool {
        authorizer
            .check_repository_access(
                Some(&VerifiedToken {
                    raw: "raw.jwt",
                    claims,
                }),
                repository,
                action,
            )
            .await
            .is_ok()
    }

    #[tokio::test]
    async fn a_matching_name_grants_the_listed_permissions() {
        let f = fixture();
        let token = claims(&[("urc-name:goalables/*", &["read", "push"])]);

        assert!(check(&f.authorizer, &token, f.app, None).await);
        assert!(check(&f.authorizer, &token, f.app, Some("read")).await);
        assert!(check(&f.authorizer, &token, f.app, Some("push")).await);
        assert!(!check(&f.authorizer, &token, f.app, Some("obliterate")).await);
    }

    #[tokio::test]
    async fn a_non_matching_name_is_denied() {
        let f = fixture();
        let token = claims(&[("urc-name:goalables/*", &["read", "push"])]);

        assert!(!check(&f.authorizer, &token, f.game, None).await);
        assert!(!check(&f.authorizer, &token, f.game, Some("read")).await);
        assert!(!check(&f.authorizer, &token, f.unknown, None).await);
    }

    #[tokio::test]
    async fn an_entry_with_no_permissions_still_reaches_the_repository() {
        let f = fixture();
        let token = claims(&[("urc-name:goalables/*", &[])]);

        assert!(check(&f.authorizer, &token, f.app, None).await);
        assert!(!check(&f.authorizer, &token, f.app, Some("read")).await);
    }

    #[tokio::test]
    async fn name_and_id_grants_merge() {
        let f = fixture();
        let token = claims(&[
            ("urc-name:goalables/*", &["read"]),
            (&format!("urc-{}", f.app), &["push"]),
            (&format!("urc-{}", f.game), &["read"]),
        ]);

        assert!(check(&f.authorizer, &token, f.app, Some("read")).await);
        assert!(check(&f.authorizer, &token, f.app, Some("push")).await);
        assert!(check(&f.authorizer, &token, f.game, Some("read")).await);
        assert!(!check(&f.authorizer, &token, f.game, Some("push")).await);
    }

    #[tokio::test]
    async fn without_a_resolver_name_entries_grant_nothing() {
        let f = fixture();
        let bare = AuthClientAuthorizer::new("https://auth.invalid".to_string());
        let token = claims(&[("urc-name:goalables/*", &["read"])]);

        assert!(!check(&bare, &token, f.app, None).await);
        assert!(!check(&bare, &token, f.app, Some("read")).await);
    }

    struct FailingNames(Status);

    #[async_trait]
    impl RepositoryNameResolver for FailingNames {
        async fn resolve(&self, _repository_id: RepositoryId) -> Result<Option<Arc<str>>, Status> {
            Err(self.0.clone())
        }

        async fn incarnation(
            &self,
            _repository_id: RepositoryId,
        ) -> Result<Option<lore_base::types::Hash>, Status> {
            Ok(None)
        }
    }

    async fn granted_access_with_resolver_error(status: Status) -> Status {
        let authorizer = AuthClientAuthorizer::new("https://auth.invalid".to_string())
            .with_name_resolver(Arc::new(FailingNames(status)));
        let authorizer: &dyn RepositoryAuthorizer = &authorizer;
        let token = claims(&[("urc-name:goalables/*", &["read"])]);
        let verified = VerifiedToken {
            raw: "raw.jwt",
            claims: &token,
        };
        authorizer
            .granted_access(Some(&verified), rand::random())
            .await
            .expect_err("resolver failed")
    }

    #[tokio::test]
    async fn backpressure_from_the_resolver_reaches_the_caller() {
        let err = granted_access_with_resolver_error(Status::resource_exhausted("slow down")).await;
        assert_eq!(err.code(), Code::ResourceExhausted);
    }

    #[tokio::test]
    async fn other_resolver_errors_are_the_uniform_denial() {
        let err = granted_access_with_resolver_error(Status::unavailable("store down")).await;
        assert_eq!(err.code(), Code::PermissionDenied);
        assert_eq!(err.message(), "Unauthorized");
    }

    #[tokio::test]
    async fn enumeration_merges_name_and_id_grants() {
        let f = fixture();
        let token = claims(&[
            ("urc-name:goalables/*", &["read"]),
            (&format!("urc-{}", f.app), &["push"]),
        ]);
        let verified = VerifiedToken {
            raw: "raw.jwt",
            claims: &token,
        };

        let app = f
            .authorizer
            .granted_actions(Some(&verified), f.app)
            .await
            .unwrap()
            .unwrap();
        assert!(app.permits("read") && app.permits("push") && !app.permits("admin"));

        let game = f
            .authorizer
            .granted_actions(Some(&verified), f.game)
            .await
            .unwrap()
            .unwrap();
        assert!(!game.reachable());
    }

    #[tokio::test]
    async fn enumeration_agrees_with_the_per_question_path() {
        let f = fixture();
        let tokens = [
            claims(&[("urc-name:goalables/*", &["read", "push"])]),
            claims(&[("urc-name:goalables/*", &["read"]), ("urc-*", &["admin"])]),
            claims(&[("urc-name:game", &[])]),
        ];
        for token in &tokens {
            let verified = VerifiedToken {
                raw: "raw.jwt",
                claims: token,
            };
            for repository in [f.app, f.game, f.unknown] {
                let grants = f
                    .authorizer
                    .granted_actions(Some(&verified), repository)
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(
                    grants.reachable(),
                    check(&f.authorizer, token, repository, None).await
                );
                for action in ["read", "push", "admin"] {
                    assert_eq!(
                        grants.permits(action),
                        check(&f.authorizer, token, repository, Some(action)).await,
                        "{action} on {repository}"
                    );
                }
            }
        }
    }

    #[tokio::test]
    async fn the_sync_check_denies_rather_than_guesses() {
        let f = fixture();
        let sync = |token: &AuthorizationToken, action: Option<&str>| {
            f.authorizer.check_repository_access_sync(
                Some(&VerifiedToken {
                    raw: "raw.jwt",
                    claims: token,
                }),
                f.app,
                action,
            )
        };

        let name_only = claims(&[("urc-name:goalables/*", &["read"])]);
        assert!(sync(&name_only, None).is_none());
        assert!(sync(&name_only, Some("read")).is_none());

        let by_id = claims(&[
            ("urc-name:goalables/*", &["read"]),
            (&format!("urc-{}", f.app), &["read"]),
        ]);
        assert!(sync(&by_id, Some("read")).unwrap().is_ok());
    }

    #[tokio::test]
    async fn tokens_without_name_entries_are_unchanged() {
        let f = fixture();
        let by_id = claims(&[(&format!("urc-{}", f.app), &["read"])]);

        assert!(check(&f.authorizer, &by_id, f.app, Some("read")).await);
        assert!(!check(&f.authorizer, &by_id, f.game, None).await);
        let sync = f.authorizer.check_repository_access_sync(
            Some(&VerifiedToken {
                raw: "raw.jwt",
                claims: &by_id,
            }),
            f.game,
            None,
        );
        assert!(sync.unwrap().is_err());
    }
}
