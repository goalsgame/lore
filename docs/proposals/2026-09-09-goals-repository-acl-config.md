---
lep: 2026-09-09-goals-repository-acl-config
title: Config-driven, per-repository group grants (GOALS fork)
authors:
  - GOALS platform team
status: Draft
created: 2026-09-09
updated: 2026-09-09
discussion: N/A — GOALS-internal fork proposal, not intended for upstream
---

# Config-driven, per-repository group grants

## Summary

Replace `GlobalGrantsAuthorizer`'s flat, server-wide action model with a single config-driven mechanism that maps `FoxIDs group → repository name pattern(s) → permission(s)`, covering all eight actions (`read`, `push`, `push-protected`, `owner`, `admin`, `migrate`, `obliterate`, `presign`) uniformly. A grant with pattern `*` expresses "everywhere" as an ordinary case, not a structurally different one — there is no separate "global" code path.

## Motivation

`GlobalGrantsAuthorizer` (Tier 1 of `2026-08-20-oidc-oauth2-authentication.md`) checks whether an action string is present in the caller's `groups` claim, with no awareness of which repository is being accessed. Every grant is server-wide: membership in `push` lets you push to every repository on the server, membership in `admin` lets you administer every repository. This was an acceptable simplification while closing the six pre-existing privileged-action gaps, but GOALS wants real per-repository scoping now — e.g. an `art-team` group that can read/push only repositories matching `art-*`, without granting that same access everywhere.

The LEP's own answer to this is Tier 2 (`ResourceGrantsAuthorizer`): grants travel inside the token itself, via RFC 8693 token exchange against a broker service. GOALS does not want to stand up a token-exchange broker or extend FoxIDs to support resource indicators right now. This proposal is a deliberately simpler alternative that achieves the same practical outcome — per-repository scoping — by keeping grants entirely server-side in a static config file, resolved against the plain `groups` claim FoxIDs already provides. It is explicitly not an implementation of the LEP's Tier 2 design; it is a different mechanism reaching a similar goal by a cheaper path, appropriate for GOALS' current operational needs ("we'll always be able to patch the config file").

## Goals / Non-Goals

### Goals

- One config file expresses every group's permissions on every repository pattern, for all eight actions uniformly.
- No FoxIDs-side changes beyond what already exists (the `groups` claim). No token exchange, no resource indicators, no broker service.
- Fail closed: a repository/action combination with no matching grant is denied; a malformed config file refuses server startup. (This is new practice for this file specifically, not an existing guarantee `[server.auth]` already provides today — see Security Considerations.)
- Reuse `GlobalGrantsAuthorizer`'s existing claim-extraction and factory-selection machinery where it still applies (`AuthorizationToken::claim_at`, the `repository_authorizer()` factory).

### Non-Goals

- No hot-reload / dynamic config source (e.g. Firestore-backed) in this iteration. The config is loaded once at startup. A dynamic backend is a plausible future evolution, not attempted here.
- No per-user grants — only per-group. A single-person "group" is the workaround if that's ever needed.
- No implementation of Tier 2 / RFC 8693 token exchange. This proposal and Tier 2 are alternatives, not layers — a deployment picks one.
- No config-authoring tooling or UI. Hand-edited TOML only.

## Proposed Design

### Config shape

A new, separate file (not inlined into `[server.auth]`, since the grant list is expected to grow large and mixing it into the main auth table would make both harder to read — see "Unresolved Questions" for the case against inlining):

```toml
[[grant]]
group = "engineering"
repos = ["**"]
permissions = ["read", "push"]

[[grant]]
group = "art-leads"
repos = ["art/*"]
permissions = ["read", "push", "push-protected"]

[[grant]]
group = "admins"
repos = ["**"]
permissions = ["admin", "owner", "obliterate", "migrate", "presign"]
```

Note the `**` for "everywhere," not a bare `*`: the vendored `glob-match` matcher (confirmed empirically, not assumed, during implementation of the grant-resolution engine) treats `/` as a real segment boundary, so a bare `*` matches only single-segment names and will *not* match a nested name like `art/characters`. A grant meant to apply regardless of nesting depth must use `**`. This is exactly the component-aware behavior "Pattern matching" below argues for over `globset`, but it means authors need to know the distinction — a config author writing `repos = ["*"]` expecting "every repository" would silently under-grant the moment any repository name contains a `/`.

Referenced from `[server.auth]` by a new path field (name TBD, e.g. `acl_config_path`). Loaded and parsed once at server startup; a parse error or an unknown field fails startup immediately, matching how `[server.auth]`'s own fields are validated today.

### New authorizer: `ConfiguredGrantsAuthorizer`

Implements the existing `RepositoryAuthorizer` trait unchanged:

```rust
async fn check_repository_access(
    &self,
    token: Option<&VerifiedToken<'_>>,
    repository: RepositoryId,
    action: Option<&str>,
) -> Result<(), Status>;
```

`GlobalGrantsAuthorizer`'s claim-extraction (`permission_claim` → caller's group set, via `AuthorizationToken::claim_at`) is reused as-is. What's new: instead of checking whether `action` is a literal member of the caller's group set, this authorizer:

1. Resolves `repository: RepositoryId` (which is `type RepositoryId = Partition` — an opaque identifier, not a name) to its human-readable name via a `RepositoryMetadata` lookup.
2. Matches that name against every grant's `repos` patterns.
3. Unions the `permissions` of every grant whose pattern matched **and** whose `group` is in the caller's group set.
4. `action: None` (plain reachability) succeeds if that union is non-empty for this repository at all — i.e. the caller holds *some* grant here, not necessarily `read` specifically. This matches `AuthClientAuthorizer`'s existing handling of `action: None` (treating "listed as accessible at all" as sufficient), so it's continuing established precedent, not introducing a new semantic. Implementations must treat a bare reachability success as authorizing nothing beyond reachability — no handler may skip its own specific-action check just because reachability passed.
5. `action: Some(name)` succeeds iff `name` is in that union.

Both `GlobalGrantsAuthorizer` and `ConfiguredGrantsAuthorizer` continue to exist as distinct, selectable implementations — this is not a replacement. A deployment that has no need for per-repository scoping (a smaller studio, a single-team server) can keep using the simpler flat model. GOALS' own deployment would select `ConfiguredGrantsAuthorizer` via the new `acl_config_path` setting.

### The load-bearing open question: where can this check actually run?

`check_repository_access` is already an `async fn` — nothing about the trait signature blocks an I/O-bound implementation. The problem is entirely about *which caller* invokes it, and a design review of an earlier draft of this proposal (see the design-review record for 2026-09-09) found that draft's answer to this question was both incomplete and built on a misreading of unrelated work. Corrected below.

**There are two synchronous call sites, not one**, both reached through `ReachabilityAuthorizer::check_reachability_sync` (`lore-server/src/authnz/repository_authorizer.rs`):

- `JWTInterceptor::call` (`lore-server/src/auth/jwt_interceptor.rs`) — the centralized per-request reachability gate.
- `link_read_authorizer` (`lore-server/src/grpc/mod.rs`), which builds `lore_revision::state::CanReadRepository = Arc<dyn Fn(RepositoryId) -> bool + Send + Sync>` — a **plain synchronous closure type baked into `lore-revision`'s cross-partition link-following/tree-walk API**, documented as possibly invoked many times *per request* during revision-graph traversal. This is not a handler boundary at all; there is no handler to fold this check into.

`check_reachability_sync` is implemented as `self.check_reachability(claims, repository).now_or_never().expect(...)` — `now_or_never()` polls the future once and returns `None` on `Pending`. **An async, I/O-bound `ConfiguredGrantsAuthorizer` implementation plugged into this unchanged does not error, it panics** on the first repository-metadata read that doesn't resolve synchronously. This is worse than a design tradeoff — it is a crash-on-first-real-request landmine that could easily survive local/dev testing (small or cache-warm stores resolving "fast enough" to dodge the pending state) and then panic under real production I/O latency.

An earlier draft of this proposal proposed retiring the centralized interceptor check and folding reachability into per-handler checks, citing the concurrent read/push baseline-actions work's finding that tonic's `Interceptor` trait cannot see which RPC method is being called. That citation does not support that conclusion: the RPC-method-invisibility finding explains why *action-specific* checks (read vs. push, which differ per RPC) had to move to handlers — it says nothing about reachability, which only asks "does this caller hold *anything* here" and never needed to know the RPC method. Worse, that approach does not even cover the `link_read_authorizer` call site above, which isn't a handler at all — making a change to `lore-revision`'s public `CanReadRepository` type across `state.rs`/`revision.rs`/`repository.rs` a real, unstated prerequisite of that approach.

**The recommended resolution instead reuses a pattern already present in the same file.** `JWTInterceptor`'s own `authorize()` function already solves "a sync interceptor needs a value backed by async I/O" for JWT verification: a sync cache lookup (`try_verify_token_cached`) on the hot path, falling back to `task::block_in_place(|| runtime().block_on(verify_token(...)))` only on a cache miss. The same shape — a `DashMap<RepositoryId, String>` name cache, populated via `block_in_place`/`block_on` on first lookup, read synchronously thereafter — lets both `check_reachability_sync` call sites keep working exactly as they do today, with no change to `lore-revision`'s public types and no retirement of the interceptor's authorization role. Cache correctness is simpler than it would be in most systems: repository renaming is not a supported operation (`NAME` is a `READ_ONLY_KEY`; `RepositoryMetadataSet` rejects any attempt to change it), so once a `RepositoryId → name` entry is populated it is valid for the lifetime of the process — no TTL or invalidation logic needed, only a memory bound (e.g. LRU-capped, mirroring the existing JWK cache in `lore-server/src/auth/jwk.rs`, a closer precedent than `lore-aws`'s unrelated use of `DashMap`).

This is the resolution to implement; the "fold into handlers" alternative is retained below only as a rejected alternative, since it was this proposal's original (incorrect) answer and the reasoning for rejecting it is itself informative.

### Pattern matching

Use `vendor/glob-match` — the vendored, patched fork of crates.io's `glob-match` already a `lore-revision` workspace dependency, already exercised by `lore-revision/tests/filter_gitignore.rs`, and already fixed here for a `**`-backtracking defect upstream has. This is a better answer than adding a second pattern-matching dependency (`globset` was considered and rejected — see below): it's proper component-aware (`/`-splitting) gitignore-style matching, and repository names are themselves hierarchical (`lore_revision::repository::is_valid_name` explicitly allows and validates `/`-separated segments, e.g. `art/characters`), so the same `*`-vs-`**` distinction `.loreignore` relies on to distinguish "this segment" from "this and everything under it" is directly relevant to how ACL authors will actually want to write patterns (`art/*` vs `art/**`), not just a nice-to-have consistency win.

`globset` was the original candidate and is rejected: its `GlobBuilder::literal_separator` defaults to `false`, meaning a bare `*` *crosses* `/` unless explicitly configured otherwise — the opposite of what "gitignore-style" implies, and a silent over-grant footgun (`repos = ["art-*"]` would match `art-x/y/z` at any depth) if that configuration step were ever missed.

### Caching

Resolving a repository's name is two async reads (`lore_revision::repository::metadata_hash` → `metadata`: an uncached mutable-store `load` for the current hash pointer, then an immutable-store `read` for the content) — not free, and this proposal puts it on the hot path of every authorized request (previously, `GlobalGrantsAuthorizer` needed zero I/O per check). Confirmed this has no network dependency on the server's own authorization path specifically (`RepositoryContext::new_server_context` sets `remote: RemoteState::Offline`, so it's local-store latency only, not a remote round-trip). A `DashMap<RepositoryId, String>` cache (see "the load-bearing open question" above for why this is required, not just a nice-to-have) avoids a store round-trip per request. Because repository renaming is not a supported operation (see below), a populated entry never goes stale — the cache needs only a memory bound (e.g. LRU-capped), no TTL or invalidation logic.

## Compatibility

- **Wire format** — N/A, server-side authorization only.
- **Client/server protocols** — N/A.
- **On-disk format** — New: the ACL grant config file format defined above.
- **CLI and public API** — N/A.

## Non-Functional Considerations

- **Concurrency** — Config is loaded once at startup and held read-only behind an `Arc` thereafter, same as other settings. The name cache needs a concurrent-safe structure — `DashMap` is the right choice and has a closer precedent in this codebase than initially cited: `lore-server/src/auth/jwk.rs`'s JWK cache (`Arc<DashMap<String, JWKServiceKey>>`) is the same "cache fronting an otherwise-I/O-bound sync call site" shape this proposal needs, including its cache-population pattern.
- **Memory** — Bounded by grant count (expected small, tens to low hundreds of entries) plus the name cache's bound.
- **Statelessness** — The server process remains stateless with respect to this feature; the config is static per-process (no hot reload in this iteration, see Non-Goals).
- **Determinism** — Fully deterministic given a fixed config file and a fixed set of token claims.

## Migration Plan

1. Ship `ConfiguredGrantsAuthorizer` as an additional, opt-in `repository_authorizer()` factory branch, selected by the new config field — `GlobalGrantsAuthorizer` remains the default/existing behavior for any deployment not setting it.
2. GOALS' own deployment adopts it with an initial config granting broad access (e.g. one `repos = ["*"]` grant per existing group) to avoid a hard cutover, then narrows individual grants to real repository patterns over time as needed.
3. The six existing action names, `read`, and `push` all move onto this mechanism at once for GOALS — there is no intermediate state where some actions are per-repo and others remain global, since that would mean maintaining two authorizers active simultaneously for one deployment, which `repository_authorizer()`'s single-selection factory does not support and this proposal does not add.

## Security Considerations

- Fail-closed on both "no matching grant" and "the ACL config file failed to parse." The latter must refuse server startup entirely — but note this is *new* practice being introduced for this file specifically, not, as an earlier draft of this proposal claimed, something that already matches `[server.auth]`'s existing behavior: `#[serde(deny_unknown_fields)]` is in fact commented out on every settings struct in `lore-server/src/settings.rs` today, with a standing TODO acknowledging it should be enabled but isn't yet. `validate_auth_config` today is narrow, hand-written semantic validation (non-empty `jwt_issuer`/`jwt_audience`, mutual exclusivity checks), not blanket unknown-field rejection. The new ACL config struct should set `deny_unknown_fields` itself rather than relying on a precedent that doesn't actually exist yet. Separately: validate `permissions` entries against the known 8-action set at parse time — a plain `Vec<String>` lets a typo'd action (`"admn"`) parse successfully into a silent no-op grant.
- **Repository creation can still be used to squat privileged name patterns, even after the concurrent read/push work lands — resolved only partially, not fully.** The concurrent baseline-actions work has landed on: `RepositoryCreate` now requires `push`, checked as a **global** Tier-1 action (`GlobalGrantsAuthorizer` ignores the repository parameter for every action, so there's no per-name distinction to make there regardless). This closes the "any authenticated caller, no group needed" version of the gap, but not the name-pattern-squatting version this proposal is specifically concerned with: the call site passes `RepositoryId::default()` as a sentinel for this check (there is no real repository yet to identify), and **critically, the requested repository name is not plumbed into the authorization check at all today** — confirmed directly from the implementer, not assumed. So even under `ConfiguredGrantsAuthorizer`, a caller holding `push` on *any* pattern (e.g. `temp/*`) would satisfy this global check and could still create `art/whatever`, which then falls under `art-leads`' `art/*` grant. Fully closing this requires call-site work beyond what exists today: threading the requested name into `check_repository_access` for this specific call (replacing the `RepositoryId::default()` sentinel with something `ConfiguredGrantsAuthorizer` can pattern-match against directly, since there is no `RepositoryMetadata` to resolve for a not-yet-existing repository — this is a difference in kind from every other call site, not just a missing lookup), and deciding whether `GlobalGrantsAuthorizer` needs to stop ignoring the repository parameter for this one action or whether this becomes a `ConfiguredGrantsAuthorizer`-specific special case. This is now a concrete, scoped implementation task for the wiring phase, not an open risk to merely document — do not ship `ConfiguredGrantsAuthorizer` claiming name-scoped creation control without actually implementing this.
- Must not be used to answer `RepositoryList`/repository-existence questions — that's `baseline_access`/`RepositoryDirectory`'s job, a deliberately separate mechanism per the prior authz review round (confirmed still true: `repository_create.rs`/`repository_list.rs` carry doc comments stating this explicitly). This proposal's grants answer "can you act on a repository you already know the id of," not "which repositories exist for you to discover." Conflating the two would reopen a decision that's already been made correctly.
- Pattern matching via the vendored `glob-match` fork avoids regex-style catastrophic backtracking by construction (it isn't a general regex engine); this needs reconfirming if the crate choice changes.

## Privacy Considerations

No new PII. Group names are already carried in the `groups` claim under the existing Tier 1 design; this proposal only changes how they're evaluated, not what's collected.

## Risks and Assumptions

**Assumptions**

- **Assumption:** repository names are stable for the lifetime of a repository. — Confirmed true, not merely assumed: `NAME` is a `READ_ONLY_KEY` (`lore-revision/src/metadata/repository.rs`) and `RepositoryMetadataSet`'s `validate_read_only_fields` explicitly rejects any attempt to change it. There is no `RepositoryRename` RPC. The name cache's design (no invalidation, memory-bound only) depends on this and can rely on it as a hard invariant, not a best-effort guess.
- **Assumption:** the number of grants stays small enough (tens to low hundreds) that a linear scan of all grants per authorization check is not a performance concern. — *invalidated if:* GOALS ends up with grant lists in the thousands (e.g. one grant per repository rather than per pattern), at which point a linear scan becomes worth indexing. Note this scan is redone per action even within one request when multiple actions are checked against the same repository (e.g. `repository_delete.rs`'s `owner`/`admin` check) — not a correctness issue, but worth an eye if grant lists grow large, since only the name-resolution step is cached, not the grant-matching step.

**Risks**

- **Risk:** the repository-name lookup this proposal adds to every authorized request becomes a latency/availability dependency that `GlobalGrantsAuthorizer` never had. — *mitigation:* the caching layer described above; needs load-testing before this ships to production, similar to how the Firestore lock store's transaction limits were load-tested before being trusted.
- **Risk:** a static, load-once-at-startup config (an explicit Non-Goal to change) creates a config-drift failure mode `GlobalGrantsAuthorizer` never had, since its "config" is IdP-asserted token claims that are inherently consistent across every node. During a rolling restart of a multi-node deployment, the same request could get a different authorization outcome depending on which node happens to handle it, until every node has picked up the same config file. — *mitigation:* none proposed for this iteration beyond keeping rollout windows short and the config file identically deployed to every node; revisit if this proves operationally painful (see the deferred dynamic-config-backend alternative below).

## Drawbacks

- Two authorizer implementations to maintain (`GlobalGrantsAuthorizer` and `ConfiguredGrantsAuthorizer`) where the LEP envisioned two *tiers* building on each other, not two independent alternatives. This is an explicit, accepted divergence from the LEP's shape, not an oversight.
- The static config file will get harder to review by hand as the grant list grows; no tooling is proposed to help with that in this iteration.
- Adds an async I/O dependency (repository-name resolution) to a code path that previously had none.

## Alternatives Considered

### Implement the LEP's Tier 2 (`ResourceGrantsAuthorizer`) as designed

RFC 8693 token exchange against a broker service (conceptually similar in shape to `hylla-exchange`), with grants embedded in the exchanged token rather than resolved from local config.

*Rejected because:* requires standing up a broker service and likely extending FoxIDs to support resource indicators — real new infrastructure GOALS does not want to build right now, for a static-config outcome this proposal can reach more cheaply. Worth revisiting if per-repository grants ever need to be dynamic and identity-provider-driven rather than config-file-driven.

### Match grants by repository ID instead of name

Avoids the async `RepositoryMetadata` lookup and its caching burden entirely.

*Rejected because:* `RepositoryId` is a client-supplied, opaque 16-byte value (`Context::from(req.id)` in `repository_create.rs`) with no room for a meaningful-prefix scheme without a breaking identity-format migration — there is nothing for a glob pattern like `art-*` to match against, and the caller picks the ID, not the server. This would force one grant entry per repository ID rather than per naming convention, defeating the purpose of pattern-based grouping. The caching burden this alternative would avoid is also smaller than it first appears, since repository names never change (see Risks and Assumptions) — but that doesn't change the conclusion, since the fundamental problem (opaque IDs carry no matchable structure) isn't a caching problem at all.

### Hot-reloadable / dynamic config backend (e.g. Firestore-backed, matching `lore-gcp`'s existing connectivity)

*Not rejected, deferred.* A static file is what GOALS asked for now ("we'll always be able to patch the config file"). A dynamic backend removes the redeploy-per-ACL-change friction and is a natural next step if that friction becomes real, but is out of scope here.

## Prior Art

Group-to-resource-pattern permission mapping is a well-worn shape — GitHub's team-based repository permissions and Kubernetes RBAC `RoleBinding`s both express "this group of principals gets this set of verbs on resources matching this selector," which is structurally what this proposal does with FoxIDs groups, lore actions, and glob-matched repository names in place of GitHub teams/repos or Kubernetes subjects/resources.

## Unresolved Questions

Resolved during design review (kept here briefly for the record, since the original draft posed them as open): the glob-matching crate (use the vendored `glob-match`, not `globset`), the reachability semantic (matches existing `AuthClientAuthorizer` precedent), whether repository renaming exists (it doesn't — confirmed, not assumed), and whether the interceptor's centralized check needs retiring (it doesn't, with the `DashMap`-cached sync/async-fallback approach). See the relevant sections above for the reasoning, not just the conclusion.

Genuinely still open:

- Separate ACL file vs. inlining the grant list into `[server.auth]` — leaning separate file for readability at scale, but this trades off against having one fewer file to keep in sync across environments.
- Exact new setting name(s) for pointing `[server.auth]` at the ACL config file path.

No longer open, now a scoped implementation task rather than a question (see Security Considerations for the detail): `RepositoryCreate` needs the requested repository name threaded into its authorization check so `ConfiguredGrantsAuthorizer` can pattern-match it, replacing the `RepositoryId::default()` sentinel the concurrent read/push work's global `push` check uses today. This is real, unavoidable work in the wiring phase, not a design choice to weigh.
