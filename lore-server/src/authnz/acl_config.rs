// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Config-driven, per-repository group grants (GOALS fork).
//!
//! Maps `FoxIDs group -> repository name pattern(s) -> permission(s)` via a
//! static TOML file, as an alternative to [`GlobalGrantsAuthorizer`]'s
//! server-wide, repository-unaware grants. See
//! `docs/proposals/2026-09-09-goals-repository-acl-config.md` for the full
//! design.
//!
//! [`GlobalGrantsAuthorizer`]: super::repository_authorizer
//!
//! This module is deliberately self-contained: [`resolve`] is a pure
//! function operating on already-resolved plain values (a repository name, a
//! caller's group list, an optional action string). It has no dependency on
//! `RepositoryAuthorizer`, `RepositoryId`, `VerifiedToken`, or any tonic/async
//! type. The type that wraps it into a real [`RepositoryAuthorizer`] --
//! `ConfiguredGrantsAuthorizer`, including the `RepositoryId -> name` cache
//! and the sync/async bridge described in the design doc's "the load-bearing
//! open question" section -- lives in `repository_authorizer.rs`, and is
//! selected by that module's `repository_authorizer_with_stores` factory.
//!
//! [`RepositoryAuthorizer`]: super::repository_authorizer::RepositoryAuthorizer

use std::path::Path;

use serde::Deserialize;
use serde::Deserializer;
use serde::de::Error as _;

/// The complete set of actions this mechanism understands, covering all
/// eight actions the design doc calls out uniformly (no separate "global"
/// code path for any of them).
///
/// A `permissions` entry that isn't one of these fails config parsing loudly
/// (see [`validate_permission`]) rather than silently becoming a no-op
/// grant -- a typo'd action name (`"admn"`) must not parse successfully.
pub const KNOWN_ACTIONS: &[&str] = &[
    "read",
    "push",
    "push-protected",
    "owner",
    "admin",
    "migrate",
    "obliterate",
    "presign",
];

/// A parsed ACL grant config:
///
/// ```toml
/// [[grant]]
/// group = "engineering"
/// repos = ["*"]
/// permissions = ["read", "push"]
/// ```
///
/// `#[serde(deny_unknown_fields)]` is set deliberately here -- it is *not*
/// the existing convention for this codebase's settings structs (see
/// `lore-server/src/settings.rs`, where it is commented out everywhere with a
/// standing TODO), so this struct opts in explicitly rather than assuming
/// that precedent applies.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AclConfig {
    /// One entry per `[[grant]]` table. Absent entirely (an empty or
    /// grant-less file) is a valid, if useless, config: every [`resolve`]
    /// call against it denies.
    #[serde(default, rename = "grant")]
    pub grants: Vec<Grant>,
}

/// One `[[grant]]` table: a group's permissions on repositories matching any
/// of `repos`.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Grant {
    /// The FoxIDs group this grant applies to, as it appears in the
    /// caller's `groups` claim.
    pub group: String,
    /// Glob patterns (see [`pattern_matches`]) matched against repository
    /// names. A repository name matches this grant if *any* pattern here
    /// matches it.
    pub repos: Vec<String>,
    /// The actions this grant confers, once matched. Validated against
    /// [`KNOWN_ACTIONS`] at parse time.
    #[serde(deserialize_with = "deserialize_permissions")]
    pub permissions: Vec<String>,
}

/// Deserializes `permissions`, rejecting any entry that is not one of
/// [`KNOWN_ACTIONS`] with a parse error naming the offending value.
///
/// This is what turns a typo'd action name into a loud parse failure instead
/// of a silently-parsed, permanently-inert grant.
fn deserialize_permissions<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: Deserializer<'de>,
{
    let raw = Vec::<String>::deserialize(deserializer)?;
    for permission in &raw {
        validate_permission(permission).map_err(D::Error::custom)?;
    }
    Ok(raw)
}

/// Checks one permission string against [`KNOWN_ACTIONS`].
fn validate_permission(permission: &str) -> Result<(), String> {
    if KNOWN_ACTIONS.contains(&permission) {
        Ok(())
    } else {
        Err(format!(
            "unknown permission `{permission}`, expected one of: {}",
            KNOWN_ACTIONS.join(", ")
        ))
    }
}

/// Error loading or parsing an [`AclConfig`].
#[derive(Debug, thiserror::Error)]
pub enum AclConfigError {
    /// The config file could not be read.
    #[error("failed to read ACL config file {path}: {source}")]
    Io {
        path: String,
        #[source]
        source: std::io::Error,
    },
    /// The config's TOML failed to parse or deserialize -- including an
    /// unknown field, a missing required field, or an unrecognized
    /// `permissions` entry.
    #[error("failed to parse ACL config: {0}")]
    Parse(#[from] toml::de::Error),
}

/// Parses an [`AclConfig`] from TOML source text.
///
/// Fails closed: any unknown field, missing required field, or unrecognized
/// `permissions` entry is a parse error, never a silently-defaulted or
/// silently-dropped value.
pub fn parse(source: &str) -> Result<AclConfig, AclConfigError> {
    toml::from_str(source).map_err(AclConfigError::from)
}

/// Reads and parses an [`AclConfig`] from a file path.
///
/// Intended for the deferred `[server.auth]` wiring described in the design
/// doc (loaded once at startup); does no caching and is plain synchronous
/// file I/O, matching how the rest of `[server.auth]`'s settings are loaded
/// today.
pub fn load_from_path(path: &Path) -> Result<AclConfig, AclConfigError> {
    let source = std::fs::read_to_string(path).map_err(|source| AclConfigError::Io {
        path: path.display().to_string(),
        source,
    })?;
    parse(&source)
}

/// Whether `pattern` (one entry of a grant's `repos` list) matches
/// `repository_name`.
///
/// Delegates to the vendored, gitignore-style `glob-match` crate
/// (`vendor/glob-match`) rather than a plain glob library, because
/// repository names are themselves hierarchical, `/`-separated segments
/// (`lore_revision::repository::is_valid_name` explicitly allows and
/// validates names like `art/characters`), and a bare `*` must not cross a
/// `/` the way it would with e.g. `globset`'s default configuration:
///
/// - `*` matches exactly one path segment: `art-*` matches `art-x` but not
///   `art-x/y/z`, and `art/*` matches `art/characters` but not
///   `art/characters/hero`.
/// - `**` matches across segment boundaries: `art/**` matches both
///   `art/characters` and `art/characters/hero` (though not `art` itself --
///   a trailing `**` requires at least one segment beneath it).
///
/// This means a bare `repos = ["*"]` grant -- see the design doc's own
/// example -- matches only single-segment repository names, not nested ones
/// like `art/characters`; `repos = ["**"]` (or `["*", "**"]`) is what
/// matches every repository regardless of nesting depth. Grant authors
/// intending "everywhere" for a studio that uses nested repository names
/// should write `**`, not `*`.
fn pattern_matches(pattern: &str, repository_name: &str) -> bool {
    glob_match::glob_match(pattern, repository_name)
}

/// Resolves whether `caller_groups` is granted `action` on
/// `repository_name`, according to `config`.
///
/// Unions the `permissions` of every [`Grant`] whose `repos` pattern matches
/// `repository_name` **and** whose `group` is present in `caller_groups`.
///
/// - `action: Some(name)` succeeds iff `name` is in that union.
/// - `action: None` is plain reachability: it succeeds iff the union is
///   non-empty at all, i.e. the caller holds *some* grant here -- not
///   necessarily `read` specifically. This matches `AuthClientAuthorizer`'s
///   existing handling of `action: None` (see the design doc). **A bare
///   reachability success authorizes nothing beyond reachability**: no
///   caller of this function may skip its own `action: Some(name)` check
///   just because a `None` check passed.
///
/// This is a pure function: no I/O, no async, and no knowledge of
/// `RepositoryId`, `VerifiedToken`, or any tonic type -- its inputs are
/// already fully resolved by the (future) caller.
pub fn resolve(
    config: &AclConfig,
    repository_name: &str,
    caller_groups: &[String],
    action: Option<&str>,
) -> bool {
    let mut granted = config
        .grants
        .iter()
        .filter(|grant| caller_groups.contains(&grant.group))
        .filter(|grant| {
            grant
                .repos
                .iter()
                .any(|pattern| pattern_matches(pattern, repository_name))
        })
        .flat_map(|grant| grant.permissions.iter());

    match action {
        None => granted.next().is_some(),
        Some(name) => granted.any(|permission| permission == name),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config_with(grants: Vec<Grant>) -> AclConfig {
        AclConfig { grants }
    }

    fn grant(group: &str, repos: &[&str], permissions: &[&str]) -> Grant {
        Grant {
            group: group.to_string(),
            repos: repos.iter().map(|s| s.to_string()).collect(),
            permissions: permissions.iter().map(|s| s.to_string()).collect(),
        }
    }

    fn groups(names: &[&str]) -> Vec<String> {
        names.iter().map(|s| s.to_string()).collect()
    }

    // -- basic resolution ----------------------------------------------

    #[test]
    fn single_grant_match_grants_its_permission() {
        let config = config_with(vec![grant("engineering", &["*"], &["read", "push"])]);
        assert!(resolve(
            &config,
            "backend",
            &groups(&["engineering"]),
            Some("read")
        ));
        assert!(resolve(
            &config,
            "backend",
            &groups(&["engineering"]),
            Some("push")
        ));
    }

    #[test]
    fn single_grant_match_denies_unlisted_permission() {
        let config = config_with(vec![grant("engineering", &["*"], &["read"])]);
        assert!(!resolve(
            &config,
            "backend",
            &groups(&["engineering"]),
            Some("push")
        ));
    }

    #[test]
    fn multiple_grants_union_across_matching_grants() {
        // Two grants, both matching the same repository for the same caller
        // group, each contributing a different permission: the caller should
        // hold the union, not just the permissions of whichever grant is
        // listed first.
        let config = config_with(vec![
            grant("engineering", &["*"], &["read"]),
            grant("engineering", &["backend*"], &["push"]),
        ]);
        assert!(resolve(
            &config,
            "backend-api",
            &groups(&["engineering"]),
            Some("read")
        ));
        assert!(resolve(
            &config,
            "backend-api",
            &groups(&["engineering"]),
            Some("push")
        ));
        assert!(!resolve(
            &config,
            "backend-api",
            &groups(&["engineering"]),
            Some("admin")
        ));
    }

    #[test]
    fn caller_in_multiple_groups_unions_each_groups_grants() {
        // Distinct groups, each with their own grant on the same
        // repository; a caller belonging to both groups gets both.
        let config = config_with(vec![
            grant("art-leads", &["art/*"], &["read", "push-protected"]),
            grant("qa", &["art/*"], &["push"]),
        ]);
        let caller_groups = groups(&["art-leads", "qa"]);
        assert!(resolve(
            &config,
            "art/hero",
            &caller_groups,
            Some("push-protected")
        ));
        assert!(resolve(&config, "art/hero", &caller_groups, Some("push")));
        // A caller in only one of the two groups gets only that group's
        // permissions.
        assert!(!resolve(
            &config,
            "art/hero",
            &groups(&["qa"]),
            Some("push-protected")
        ));
    }

    #[test]
    fn no_matching_grant_denies() {
        let config = config_with(vec![grant("engineering", &["backend*"], &["read"])]);
        // Right group, wrong repository pattern.
        assert!(!resolve(
            &config,
            "frontend",
            &groups(&["engineering"]),
            Some("read")
        ));
        // Right repository, wrong group.
        assert!(!resolve(
            &config,
            "backend-api",
            &groups(&["nobody"]),
            Some("read")
        ));
    }

    #[test]
    fn empty_groups_list_denies() {
        let config = config_with(vec![grant("engineering", &["*"], &["read", "push"])]);
        assert!(!resolve(&config, "backend", &[], Some("read")));
        assert!(!resolve(&config, "backend", &[], None));
    }

    #[test]
    fn zero_grants_denies_everything() {
        let config = config_with(vec![]);
        assert!(!resolve(
            &config,
            "anything",
            &groups(&["engineering"]),
            Some("read")
        ));
        assert!(!resolve(
            &config,
            "anything",
            &groups(&["engineering"]),
            None
        ));
    }

    // -- pattern semantics: `*` (single segment) vs `**` (any depth) ---

    #[test]
    fn single_star_does_not_cross_a_path_segment() {
        let config = config_with(vec![grant("art-leads", &["art/*"], &["read"])]);
        // Matches its immediate child...
        assert!(resolve(
            &config,
            "art/characters",
            &groups(&["art-leads"]),
            Some("read")
        ));
        // ...but not a grandchild: `*` does not cross the `/`.
        assert!(!resolve(
            &config,
            "art/characters/hero",
            &groups(&["art-leads"]),
            Some("read")
        ));
    }

    #[test]
    fn double_star_crosses_path_segments() {
        let config = config_with(vec![grant("art-leads", &["art/**"], &["read"])]);
        assert!(resolve(
            &config,
            "art/characters",
            &groups(&["art-leads"]),
            Some("read")
        ));
        assert!(resolve(
            &config,
            "art/characters/hero",
            &groups(&["art-leads"]),
            Some("read")
        ));
    }

    #[test]
    fn bare_star_matches_only_single_segment_names() {
        // The design doc's own illustrative example (`repos = ["*"]`) reads
        // as "everywhere," but a bare `*` only matches a single-segment
        // repository name -- it does not cross into `art/characters`. This
        // is the distinction a grant author needs `**` for.
        let config = config_with(vec![grant("engineering", &["*"], &["read"])]);
        assert!(resolve(
            &config,
            "backend",
            &groups(&["engineering"]),
            Some("read")
        ));
        assert!(!resolve(
            &config,
            "art/characters",
            &groups(&["engineering"]),
            Some("read")
        ));
    }

    #[test]
    fn bare_double_star_matches_every_repository_name() {
        let config = config_with(vec![grant("engineering", &["**"], &["read"])]);
        assert!(resolve(
            &config,
            "backend",
            &groups(&["engineering"]),
            Some("read")
        ));
        assert!(resolve(
            &config,
            "art/characters/hero",
            &groups(&["engineering"]),
            Some("read")
        ));
    }

    // -- reachability (`action: None`) is not authorization -------------

    #[test]
    fn reachability_succeeds_on_any_grant_but_does_not_imply_a_specific_action() {
        // Holds `push` only -- no `read` grant at all.
        let config = config_with(vec![grant("engineering", &["*"], &["push"])]);
        let caller_groups = groups(&["engineering"]);

        // Bare reachability succeeds: the caller holds *something* here.
        assert!(resolve(&config, "backend", &caller_groups, None));
        // The action it actually holds succeeds too.
        assert!(resolve(&config, "backend", &caller_groups, Some("push")));
        // But a specific `read` check must still fail -- reachability must
        // never be treated as if it authorized `read` (or anything else)
        // beyond itself.
        assert!(!resolve(&config, "backend", &caller_groups, Some("read")));
    }

    #[test]
    fn reachability_fails_when_no_grant_matches_at_all() {
        let config = config_with(vec![grant("engineering", &["backend*"], &["push"])]);
        assert!(!resolve(
            &config,
            "frontend",
            &groups(&["engineering"]),
            None
        ));
    }

    // -- TOML parsing: success -------------------------------------------

    #[test]
    fn parses_the_design_docs_example_config() {
        let toml = r#"
            [[grant]]
            group = "engineering"
            repos = ["*"]
            permissions = ["read", "push"]

            [[grant]]
            group = "art-leads"
            repos = ["art-*"]
            permissions = ["read", "push", "push-protected"]

            [[grant]]
            group = "admins"
            repos = ["*"]
            permissions = ["admin", "owner", "obliterate", "migrate", "presign"]
        "#;
        let config = parse(toml).expect("valid config should parse");
        assert_eq!(config.grants.len(), 3);
        assert_eq!(config.grants[0].group, "engineering");
        assert_eq!(config.grants[1].repos, vec!["art-*".to_string()]);
        assert_eq!(
            config.grants[2].permissions,
            vec![
                "admin".to_string(),
                "owner".to_string(),
                "obliterate".to_string(),
                "migrate".to_string(),
                "presign".to_string(),
            ]
        );
    }

    #[test]
    fn empty_config_parses_to_zero_grants() {
        let config = parse("").expect("an empty file is a valid, if useless, config");
        assert!(config.grants.is_empty());
    }

    #[test]
    fn every_known_action_is_individually_accepted() {
        for action in KNOWN_ACTIONS {
            let toml = format!(
                "[[grant]]\ngroup = \"g\"\nrepos = [\"*\"]\npermissions = [\"{action}\"]\n"
            );
            parse(&toml).unwrap_or_else(|err| panic!("{action} should be valid: {err}"));
        }
    }

    // -- TOML parsing: failures must be loud, not silent or a panic -----

    #[test]
    fn unknown_top_level_field_is_rejected() {
        let toml = r#"
            unexpected = "field"

            [[grant]]
            group = "g"
            repos = ["*"]
            permissions = ["read"]
        "#;
        let err = parse(toml).expect_err("an unknown top-level field must fail to parse");
        assert!(matches!(err, AclConfigError::Parse(_)));
    }

    #[test]
    fn unknown_field_within_a_grant_is_rejected() {
        let toml = r#"
            [[grant]]
            group = "g"
            repos = ["*"]
            permissions = ["read"]
            typo_field = "oops"
        "#;
        let err = parse(toml).expect_err("an unknown grant field must fail to parse");
        assert!(matches!(err, AclConfigError::Parse(_)));
    }

    #[test]
    fn unknown_permission_name_is_rejected_not_silently_dropped() {
        let toml = r#"
            [[grant]]
            group = "g"
            repos = ["*"]
            permissions = ["read", "admn"]
        "#;
        let err = parse(toml).expect_err("a typo'd action name must fail to parse");
        assert!(matches!(err, AclConfigError::Parse(_)));
        assert!(
            err.to_string().contains("admn"),
            "error should name the offending permission, got: {err}"
        );
    }

    #[test]
    fn missing_required_field_is_rejected() {
        // `permissions` is missing entirely, not merely empty.
        let toml = r#"
            [[grant]]
            group = "g"
            repos = ["*"]
        "#;
        let err = parse(toml).expect_err("a missing required field must fail to parse");
        assert!(matches!(err, AclConfigError::Parse(_)));
    }

    #[test]
    fn load_from_path_reports_a_clear_error_for_a_missing_file() {
        let err = load_from_path(Path::new("/nonexistent/path/does-not-exist.toml"))
            .expect_err("a missing file must fail, not panic");
        assert!(matches!(err, AclConfigError::Io { .. }));
    }

    #[test]
    fn load_from_path_round_trips_a_real_file() {
        let dir = std::env::temp_dir();
        let path = dir.join(format!("lore-acl-config-test-{}.toml", std::process::id()));
        std::fs::write(
            &path,
            "[[grant]]\ngroup = \"g\"\nrepos = [\"*\"]\npermissions = [\"read\"]\n",
        )
        .expect("scratch file should be writable");
        let result = load_from_path(&path);
        let _ = std::fs::remove_file(&path);
        let config = result.expect("a well-formed file should load");
        assert_eq!(config.grants.len(), 1);
    }
}
