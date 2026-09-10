// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use jsonwebtoken::TokenData;
use lore_base::lore_debug;
use lore_error_set::prelude::*;
use serde::Deserialize;
use serde_with::OneOrMany;
use serde_with::formats::PreferMany;
use serde_with::serde_as;
use url::Url;

use crate::token_store::IdentityToken;

#[derive(Debug, Default, Clone)]
pub struct UserInfo {
    pub id: String,
    pub name: String,
    pub token: String,
    pub preferred_username: String,
    pub is_service_account: bool,
    pub expires: u64,
}

/// The claim a conforming issuer uses to say where its tokens may be sent.
///
/// Fixed rather than configurable on purpose: a claim name that could be
/// steered by an environment response would let a rogue server point the client
/// at a claim it controls.
pub const ROOT_DOMAINS_CLAIM: &str = "root_domains";

/// The namespaced spelling of [`ROOT_DOMAINS_CLAIM`], for providers that
/// require custom claims to be namespaced.
///
/// A collision-resistant name under a project-controlled domain (RFC 7519
/// §4.2). It is a name, never a URL that is fetched.
pub const NAMESPACED_ROOT_DOMAINS_CLAIM: &str = "https://lore.org/claims/root_domains";

// An AuthN or an AuthZ token
#[serde_as]
#[derive(Debug, Deserialize, Clone)]
pub struct JWTUserInfo {
    #[serde(rename = "iss")]
    pub issuer: String,
    #[serde(rename = "sub")]
    pub user_id: String,
    pub name: Option<String>,
    pub preferred_username: Option<String>,
    pub is_service_account: Option<bool>,
    #[serde(rename = "exp")]
    pub expires: u64,
    #[serde_as(as = "OneOrMany<_, PreferMany>")]
    #[serde(rename = "aud")]
    pub audience: Vec<String>,
    /// The issuer's own statement of where this token may be sent, read from
    /// [`ROOT_DOMAINS_CLAIM`]. `OneOrMany`, same as `audience` above: some
    /// providers' claim editors only accept a bare string, not an array.
    #[serde_as(as = "OneOrMany<_, PreferMany>")]
    #[serde(default, rename = "root_domains")]
    pub root_domains: Vec<String>,
    /// The same statement under [`NAMESPACED_ROOT_DOMAINS_CLAIM`].
    #[serde_as(as = "OneOrMany<_, PreferMany>")]
    #[serde(default, rename = "https://lore.org/claims/root_domains")]
    pub namespaced_root_domains: Vec<String>,
}

/// Reads a domain suffix out of one `iss` or `aud` entry, if it has a shape
/// that carries one.
///
/// An `https` URI contributes its host: RFC 9068 §3 wants the resource URI in
/// `aud`, and an issuer is a URL by definition, so this is the common case for
/// a conforming provider. A bare domain contributes itself, which is what the
/// legacy custom scheme mints (`.epicgames.net`) and what keeps those tokens
/// working unchanged. An opaque identifier such as `lore-server` or the legacy
/// `URC` keyword names no host and contributes nothing — matching it against a
/// hostname was never meaningful.
fn domain_from_claim_entry(entry: &str) -> Option<String> {
    if let Ok(url) = Url::parse(entry)
        && matches!(url.scheme(), "http" | "https")
    {
        return url.host_str().map(str::to_string);
    }

    // A domain has a dot separating labels and none of the punctuation that
    // would make it a URI, an email address or a path.
    let candidate = entry.strip_prefix('.').unwrap_or(entry);
    let looks_like_a_domain = candidate.contains('.')
        && !candidate.is_empty()
        && candidate
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_'));
    looks_like_a_domain.then(|| entry.to_string())
}

impl JWTUserInfo {
    /// The root domains this token may be presented to.
    ///
    /// Two issuer-signed sources, per LEP `2026-08-20-oidc-oauth2-authentication`
    /// D5. The dedicated [`ROOT_DOMAINS_CLAIM`] (or its namespaced alias) is
    /// what a conforming provider is configured to mint. Failing that, `iss`
    /// and `aud` contribute whatever domain their entries actually carry.
    ///
    /// The list has to come from the token, never from the deployment's own
    /// environment response: that response is served by the same party a stolen
    /// token would be replayed against, so a rogue server could otherwise
    /// advertise the real issuer plus its own domain, let a real login complete,
    /// and collect a token valid against the real servers.
    ///
    /// An empty result is the fail-closed case and is deliberate. The only
    /// place a domain is added outside the token is
    /// `token_store::store_user_token`, which appends the endpoint the token
    /// came from — the token genuinely can go back there, which is what the
    /// refresh and exchange grants need.
    pub fn acceptable_root_domains(&self) -> Vec<String> {
        let mut domains: Vec<String> = self
            .root_domains
            .iter()
            .chain(self.namespaced_root_domains.iter())
            .filter(|domain| !domain.is_empty())
            .cloned()
            .collect();

        // Whoever issued it already knows about the token, so it can be given
        // back to that endpoint; and the audience is who it was minted for.
        for entry in std::iter::once(&self.issuer).chain(self.audience.iter()) {
            if let Some(domain) = domain_from_claim_entry(entry)
                && !domains.contains(&domain)
            {
                domains.push(domain);
            }
        }

        domains
    }
}

pub async fn user_info<P>(
    auth_url: &str,
    identity: &str,
    token_filter: P,
    identity_token: &str,
    access_token: &str,
) -> Option<UserInfo>
where
    P: FnMut(&&IdentityToken) -> bool,
{
    lore_debug!("Get user {identity} info from {auth_url}");

    let Ok(token) = crate::token_store::load_user_token(
        auth_url,
        identity,
        token_filter,
        identity_token,
        access_token,
    )
    .await
    else {
        return None;
    };

    user_info_from_token(token)
}

pub fn identity_from_token(token: &str) -> String {
    user_info_from_token(token.to_string())
        .map(|info| info.id)
        .unwrap_or_default()
}

pub fn insecure_decode_token(
    token: &str,
) -> Result<TokenData<JWTUserInfo>, jsonwebtoken::errors::Error> {
    let header = jsonwebtoken::decode_header(token)?;
    let key = jsonwebtoken::DecodingKey::from_secret(&[]);
    let mut validation = jsonwebtoken::Validation::new(header.alg);
    validation.insecure_disable_signature_validation();
    validation.validate_aud = false;
    validation.validate_exp = false;
    validation.validate_nbf = false;
    jsonwebtoken::decode::<JWTUserInfo>(token, &key, &validation)
}

pub fn user_info_from_token(token: String) -> Option<UserInfo> {
    let Ok(token_data) = insecure_decode_token(&token) else {
        return None;
    };
    Some(UserInfo {
        id: token_data.claims.user_id.clone(),
        name: token_data.claims.name.clone().unwrap_or_default(),
        token,
        preferred_username: token_data.claims.preferred_username.unwrap_or_default(),
        is_service_account: token_data.claims.is_service_account.unwrap_or_default(),
        // JWT has number of seconds since UNIX epoch in UTC - we want milliseconds like
        // all other timestamps in Lore, also in UTC
        expires: token_data.claims.expires * 1000,
    })
}

#[error_set]
pub enum JwtUsageError {}

pub fn domain_in_root_domains(domain: &str, root_domains: &[String]) -> bool {
    root_domains.iter().any(|acceptable_root| {
        // Require a label boundary, not a raw suffix, so `epicgames.net`
        // rejects a look-alike such as `evilepicgames.net`. A leading `.`
        // is optional and does not change the match.
        let apex = acceptable_root.strip_prefix('.').unwrap_or(acceptable_root);
        domain == apex || domain.ends_with(&format!(".{apex}"))
    })
}

pub fn verify_jwt_usage_for_remote(
    jwt: &JWTUserInfo,
    remote_domain: &str,
) -> Result<(), JwtUsageError> {
    let root_domains = jwt.acceptable_root_domains();
    if domain_in_root_domains(remote_domain, &root_domains) {
        return Ok(());
    }

    lore_debug!(
        "JWT acceptable domains '{root_domains:?}' does not contain '{remote_domain}' - forbidding JWT leak"
    );
    if root_domains.is_empty() {
        // The fail-closed case: nothing in the token says where it may go, so
        // it goes nowhere. Naming both fixes here is the difference between an
        // operator seeing what to configure and seeing a dead end.
        return Err(JwtUsageError::internal(format!(
            "the token names no domain it may be sent to, so it cannot be sent to \
             '{remote_domain}'. Configure the issuer to mint a '{ROOT_DOMAINS_CLAIM}' claim \
             (or '{NAMESPACED_ROOT_DOMAINS_CLAIM}') listing the deployment's domains, or set \
             the token's audience to the deployment's canonical URL or domain."
        )));
    }
    Err(JwtUsageError::internal(format!(
        "the token may be sent to {root_domains:?}, which does not cover the remote domain \
         '{remote_domain}'"
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `{"iss":"lore","sub":"alice","name":"Alice","exp":2000000000,"aud":["example.com"]}`
    const ALICE_TOKEN: &str = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJsb3JlIiwic3ViIjoiYWxpY2UiLCJuYW1lIjoiQWxpY2UiLCJleHAiOjIwMDAwMDAwMDAsImF1ZCI6WyJleGFtcGxlLmNvbSJdfQ.signature";

    #[test]
    fn identity_from_jwt_is_its_subject() {
        assert_eq!(identity_from_token(ALICE_TOKEN), "alice");
    }

    #[test]
    fn identity_from_undecodable_token_is_empty() {
        assert!(identity_from_token("").is_empty());
        assert!(identity_from_token("not-a-jwt").is_empty());
        // Well-formed base64 segments that are not JWT claims.
        assert!(identity_from_token("aaaa.bbbb.cccc").is_empty());
    }

    fn token_with_claims(claims: &str) -> String {
        use base64::Engine;
        use base64::engine::general_purpose::URL_SAFE_NO_PAD;

        let header = URL_SAFE_NO_PAD.encode(r#"{"alg":"HS256","typ":"JWT"}"#);
        let claims = URL_SAFE_NO_PAD.encode(claims);
        format!("{header}.{claims}.signature")
    }

    /// A Keycloak-shaped token carries no `name` claim. It must still decode,
    /// with the subject as the id, rather than failing the whole decode.
    #[test]
    fn a_token_without_a_name_decodes_with_its_subject_as_the_id() {
        let token = token_with_claims(
            r#"{"iss":"lore","sub":"alice","exp":2000000000,"aud":["example.com"]}"#,
        );

        let info = user_info_from_token(token).expect("a nameless token decodes");
        assert_eq!(info.id, "alice");
        assert!(info.name.is_empty());
    }

    fn claims(claims: &str) -> JWTUserInfo {
        insecure_decode_token(&token_with_claims(claims))
            .expect("the test token decodes")
            .claims
    }

    /// The legacy custom scheme mints a keyword issuer and an audience holding
    /// one keyword and one domain suffix. That pairing must keep yielding
    /// exactly the suffix it does today.
    #[test]
    fn a_legacy_token_yields_the_same_domain_suffix_it_always_did() {
        let token = claims(
            r#"{"iss":"URC","sub":"alice","exp":2000000000,"aud":["URC",".epicgames.net"]}"#,
        );

        assert_eq!(token.acceptable_root_domains(), vec![".epicgames.net"]);
        verify_jwt_usage_for_remote(&token, "lore.epicgames.net").expect("a real host matches");
        verify_jwt_usage_for_remote(&token, "evil.example.com").expect_err("nothing else does");
    }

    /// A conforming provider's access token: the issuer is a URL and the
    /// audience is an opaque resource name. Only the issuer's host survives,
    /// which is what lets the token go back to the token endpoint but not to
    /// the Lore server.
    #[test]
    fn an_oidc_token_yields_its_issuers_host_and_nothing_from_an_opaque_audience() {
        let token = claims(
            r#"{"iss":"https://idp.example.com/-/","sub":"alice","exp":2000000000,
                "aud":"lore-server"}"#,
        );

        assert_eq!(token.acceptable_root_domains(), vec!["idp.example.com"]);
    }

    /// RFC 9068 §3 wants the resource URI in `aud`, so an operator who sets the
    /// audience to the deployment's canonical URL gets the domain for free.
    #[test]
    fn a_url_shaped_audience_contributes_its_host() {
        let token = claims(
            r#"{"iss":"https://idp.example.com/-/","sub":"alice","exp":2000000000,
                "aud":["lore-server","https://lore.example.com"]}"#,
        );

        assert_eq!(
            token.acceptable_root_domains(),
            vec!["idp.example.com", "lore.example.com"]
        );
        verify_jwt_usage_for_remote(&token, "lore.example.com").expect("the deployment matches");
    }

    /// At least one real provider's claim editor has no array-valued field,
    /// only a single string -- a claim minted that way must parse exactly
    /// like a one-element array would.
    #[test]
    fn a_bare_string_root_domains_claim_parses_the_same_as_a_one_element_array() {
        let token = claims(
            r#"{"iss":"https://idp.example.com/-/","sub":"alice","exp":2000000000,
                "aud":"lore-server","root_domains":".playgoals.com"}"#,
        );
        assert_eq!(
            token.acceptable_root_domains(),
            vec![".playgoals.com", "idp.example.com"]
        );
        verify_jwt_usage_for_remote(&token, "lore.playgoals.com")
            .expect("the granted suffix matches");
    }

    /// The dedicated claim is the provider-configured answer, and it composes
    /// with whatever `iss` and `aud` carry rather than replacing it.
    #[test]
    fn the_root_domains_claim_is_read_under_both_of_its_names() {
        let plain = claims(
            r#"{"iss":"https://idp.example.com/-/","sub":"alice","exp":2000000000,
                "aud":"lore-server","root_domains":[".playgoals.com"]}"#,
        );
        assert_eq!(
            plain.acceptable_root_domains(),
            vec![".playgoals.com", "idp.example.com"]
        );
        verify_jwt_usage_for_remote(&plain, "lore.playgoals.com")
            .expect("the granted suffix matches");

        let namespaced = claims(
            r#"{"iss":"https://idp.example.com/-/","sub":"alice","exp":2000000000,
                "aud":"lore-server",
                "https://lore.org/claims/root_domains":[".playgoals.com"]}"#,
        );
        assert_eq!(
            namespaced.acceptable_root_domains(),
            vec![".playgoals.com", "idp.example.com"]
        );
    }

    /// A token whose claims name no domain at all reaches nobody, and says why.
    #[test]
    fn a_token_naming_no_domain_fails_closed_and_names_the_two_fixes() {
        let token = claims(r#"{"iss":"URC","sub":"alice","exp":2000000000,"aud":"lore-server"}"#);

        assert!(token.acceptable_root_domains().is_empty());
        let error = verify_jwt_usage_for_remote(&token, "lore.example.com")
            .expect_err("a token naming no domain must go nowhere");
        assert!(error.to_string().contains(ROOT_DOMAINS_CLAIM), "{error}");
        assert!(error.to_string().contains("audience"), "{error}");
    }

    /// Entries that name no host must not be matched against one. `lore-server`
    /// is an audience, not a hostname, and an issuer keyword is not one either.
    #[test]
    fn opaque_claim_entries_contribute_no_domain() {
        assert_eq!(domain_from_claim_entry("lore-server"), None);
        assert_eq!(domain_from_claim_entry("URC"), None);
        assert_eq!(domain_from_claim_entry(""), None);
        assert_eq!(domain_from_claim_entry("urn:example:resource"), None);
        assert_eq!(
            domain_from_claim_entry("https://idp.example.com/realms/lore").as_deref(),
            Some("idp.example.com")
        );
        assert_eq!(
            domain_from_claim_entry("lore.example.com").as_deref(),
            Some("lore.example.com")
        );
        assert_eq!(
            domain_from_claim_entry(".example.com").as_deref(),
            Some(".example.com"),
            "the leading dot is preserved for the suffix matcher"
        );
    }

    #[test]
    fn a_domain_is_never_listed_twice() {
        let token = claims(
            r#"{"iss":"https://idp.example.com/","sub":"alice","exp":2000000000,
                "aud":["https://idp.example.com/","idp.example.com"]}"#,
        );
        assert_eq!(token.acceptable_root_domains(), vec!["idp.example.com"]);
    }

    /// Regression: a required `name` made `user_info_from_token` return `None`
    /// for a nameless token, so callers reading `expires` silently skipped the
    /// expiry check. An expired nameless token must report its expiry.
    #[test]
    fn an_expired_nameless_token_still_reports_its_expiry() {
        let token = token_with_claims(
            r#"{"iss":"lore","sub":"alice","exp":1000000000,"aud":["example.com"]}"#,
        );

        let info = user_info_from_token(token).expect("an expired nameless token still decodes");
        assert_eq!(info.expires, 1_000_000_000_000, "expiry in milliseconds");
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        assert!(info.expires < now_ms, "the token reads as expired");
    }
}
