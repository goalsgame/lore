// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering;

use dashmap::DashMap;
use lore_revision::lore::RepositoryId;

pub(crate) const MAX_CONCURRENT_SESSIONS: u32 = 10_000;

pub struct SessionEntry {
    pub repository: RepositoryId,
    pub correlation_id: String,
    pub user_id: String,
    /// Whether `AuthorizeStart` found this session's caller holding the
    /// baseline `read` action on `repository`, resolved once at session
    /// start (LEP 2026-08-20-oidc-oauth2-authentication, D9's "once per
    /// session rather than per operation" principle, applied to `read`/
    /// `push` the same way it already applies to plain reachability).
    /// Every `StorageCommand` on this session consults this cached value
    /// rather than asking the authorizer again.
    pub holds_read: bool,
    /// Same as `holds_read`, for the baseline `push` action.
    pub holds_push: bool,
}

/// Per-connection session state for the `lore-storage/0.4` protocol.
///
/// Tracks active sessions mapping session IDs to repository, correlation ID, user ID and cached
/// `read`/`push` grants, plus — separately — which repositories have been authorized *for read*
/// on this connection at all, for `Copy`'s cross-partition source check. Each `start()` always
/// allocates a new session ID — deduplication is handled client-side by `StorageConnector`.
pub struct SessionMap {
    entries: DashMap<u32, SessionEntry>,
    /// Value is whether the *most recent* `start()` for that repository found `read` held.
    /// `Copy`'s source check (`has_read_access`) is the only reader; a repository that was only
    /// ever started with `push` (no `read`) is never eligible as a `Copy` source, even though a
    /// session exists for it.
    authorized_repos: DashMap<RepositoryId, bool>,
    counter: AtomicU32,
}

#[derive(Debug, PartialEq)]
pub enum SessionError {
    LimitReached,
    CounterExhausted,
    NotFound,
}

impl Default for SessionMap {
    fn default() -> Self {
        Self {
            entries: DashMap::new(),
            authorized_repos: DashMap::new(),
            counter: AtomicU32::new(1),
        }
    }
}

impl SessionMap {
    /// Start a new session. Always allocates a fresh session ID — deduplication
    /// is the client's responsibility (`StorageConnector`).
    ///
    /// `holds_read`/`holds_push` are resolved by the caller once, at
    /// `AuthorizeStart` time (see `quic/storage_service_v4.rs`), and cached
    /// here for every subsequent `StorageCommand` on this session — and,
    /// for `holds_read`, for any later `Copy` naming this repository as a
    /// source from another session on the same connection.
    pub fn start(
        &self,
        repository: RepositoryId,
        correlation_id: String,
        user_id: String,
        holds_read: bool,
        holds_push: bool,
    ) -> Result<(u32, String), SessionError> {
        if self.entries.len() >= MAX_CONCURRENT_SESSIONS as usize {
            return Err(SessionError::LimitReached);
        }

        let session_id = self.counter.fetch_add(1, Ordering::Relaxed);
        if session_id == 0 {
            return Err(SessionError::CounterExhausted);
        }

        let correlation_id = if correlation_id.is_empty() {
            uuid::Uuid::new_v4().to_string()
        } else {
            correlation_id
        };

        self.authorized_repos.insert(repository, holds_read);

        self.entries.insert(
            session_id,
            SessionEntry {
                repository,
                correlation_id: correlation_id.clone(),
                user_id,
                holds_read,
                holds_push,
            },
        );

        Ok((session_id, correlation_id))
    }

    /// Stop an active session. The repository remains in the authorized set
    /// for Copy source-repo checks.
    pub fn stop(&self, session_id: u32) -> Result<(), SessionError> {
        match self.entries.remove(&session_id) {
            Some(_) => Ok(()),
            None => Err(SessionError::NotFound),
        }
    }

    pub fn get(&self, session_id: u32) -> Option<dashmap::mapref::one::Ref<'_, u32, SessionEntry>> {
        self.entries.get(&session_id)
    }

    /// O(1) check whether a repository was authorized *for `read`* by some
    /// `AuthorizeStart` on this connection (not necessarily the session
    /// making the current call) — used by `Copy`'s source-repository check,
    /// which may name a repository other than the calling session's own.
    /// `false` both when the repository was never started at all, and when
    /// it was started but the caller did not hold `read` on it.
    pub fn has_read_access(&self, repository: RepositoryId) -> bool {
        self.authorized_repos.get(&repository).is_some_and(|v| *v)
    }
}

#[cfg(test)]
mod tests {
    use rand::random;

    use super::*;

    #[test]
    fn start_assigns_session_id_from_one() {
        let map = SessionMap::default();
        let (id, _) = map
            .start(random(), "corr-1".into(), String::new(), true, true)
            .unwrap();
        assert_eq!(id, 1);
    }

    #[test]
    fn start_increments_session_id() {
        let map = SessionMap::default();
        let repo = random::<RepositoryId>();
        let (id1, _) = map
            .start(repo, "corr-1".into(), String::new(), true, true)
            .unwrap();
        let (id2, _) = map
            .start(repo, "corr-2".into(), String::new(), true, true)
            .unwrap();
        assert_eq!(id1, 1);
        assert_eq!(id2, 2);
    }

    #[test]
    fn start_always_allocates_new_id() {
        let map = SessionMap::default();
        let repo = random::<RepositoryId>();
        let (id1, _) = map
            .start(repo, "corr-1".into(), String::new(), true, true)
            .unwrap();
        let (id2, _) = map
            .start(repo, "corr-1".into(), String::new(), true, true)
            .unwrap();
        assert_ne!(id1, id2);
    }

    #[test]
    fn start_empty_correlation_generates_uuid() {
        let map = SessionMap::default();
        let repo = random::<RepositoryId>();
        let (id1, corr1) = map
            .start(repo, String::new(), String::new(), true, true)
            .unwrap();
        let (id2, corr2) = map
            .start(repo, String::new(), String::new(), true, true)
            .unwrap();
        assert_ne!(id1, id2);
        assert!(!corr1.is_empty());
        assert!(!corr2.is_empty());
        assert_ne!(corr1, corr2);
    }

    #[test]
    fn stop_removes_session() {
        let map = SessionMap::default();
        let repo = random::<RepositoryId>();
        let (id, _) = map
            .start(repo, "corr-1".into(), String::new(), true, true)
            .unwrap();
        assert!(map.get(id).is_some());
        map.stop(id).unwrap();
        assert!(map.get(id).is_none());
    }

    #[test]
    fn stop_unknown_returns_not_found() {
        let map = SessionMap::default();
        assert_eq!(map.stop(999), Err(SessionError::NotFound));
    }

    #[test]
    fn stop_already_stopped_returns_not_found() {
        let map = SessionMap::default();
        let (id, _) = map
            .start(random(), "corr-1".into(), String::new(), true, true)
            .unwrap();
        map.stop(id).unwrap();
        assert_eq!(map.stop(id), Err(SessionError::NotFound));
    }

    #[test]
    fn start_after_stop_allocates_new_id() {
        let map = SessionMap::default();
        let repo = random::<RepositoryId>();
        let (id1, _) = map
            .start(repo, "corr-1".into(), String::new(), true, true)
            .unwrap();
        map.stop(id1).unwrap();
        let (id2, _) = map
            .start(repo, "corr-1".into(), String::new(), true, true)
            .unwrap();
        assert_ne!(id1, id2);
    }

    #[test]
    fn get_returns_entry_with_user_id() {
        let map = SessionMap::default();
        let repo = random::<RepositoryId>();
        let (id, _) = map
            .start(repo, "corr-1".into(), "user-42".into(), true, true)
            .unwrap();
        let entry = map.get(id).unwrap();
        assert_eq!(entry.repository, repo);
        assert_eq!(entry.correlation_id, "corr-1");
        assert_eq!(entry.user_id, "user-42");
    }

    #[test]
    fn get_returns_none_for_unknown() {
        let map = SessionMap::default();
        assert!(map.get(42).is_none());
    }

    /// `holds_read`/`holds_push` are cached exactly as passed to `start()`,
    /// independently of one another.
    #[test]
    fn get_returns_cached_read_and_push_flags() {
        let map = SessionMap::default();
        let repo = random::<RepositoryId>();

        let (read_only, _) = map
            .start(repo, "corr-1".into(), String::new(), true, false)
            .unwrap();
        assert!(map.get(read_only).unwrap().holds_read);
        assert!(!map.get(read_only).unwrap().holds_push);

        let (push_only, _) = map
            .start(repo, "corr-2".into(), String::new(), false, true)
            .unwrap();
        assert!(!map.get(push_only).unwrap().holds_read);
        assert!(map.get(push_only).unwrap().holds_push);
    }

    #[test]
    fn has_read_access() {
        let map = SessionMap::default();
        let repo_a = random::<RepositoryId>();
        let repo_b = random::<RepositoryId>();
        map.start(repo_a, "corr-1".into(), String::new(), true, true)
            .unwrap();

        assert!(map.has_read_access(repo_a));
        assert!(!map.has_read_access(repo_b));
    }

    /// A repository started with `push` but not `read` is not a valid
    /// `Copy` source, even though a session exists for it — closing the
    /// exfiltration path where push-only access to a repository could be
    /// used to name it as a `Copy` source.
    #[test]
    fn has_read_access_is_false_for_push_only_repository() {
        let map = SessionMap::default();
        let repo = random::<RepositoryId>();
        map.start(repo, "corr-1".into(), String::new(), false, true)
            .unwrap();

        assert!(!map.has_read_access(repo));
    }

    #[test]
    fn stop_does_not_remove_authorized_repo() {
        let map = SessionMap::default();
        let repo = random::<RepositoryId>();
        let (id, _) = map
            .start(repo, "corr-1".into(), String::new(), true, true)
            .unwrap();
        map.stop(id).unwrap();
        assert!(map.has_read_access(repo));
    }

    #[test]
    fn concurrent_session_limit() {
        let map = SessionMap::default();
        let repo = random::<RepositoryId>();
        for i in 0..MAX_CONCURRENT_SESSIONS {
            map.start(repo, format!("corr-{i}"), String::new(), true, true)
                .unwrap();
        }
        assert_eq!(
            map.start(repo, "one-more".into(), String::new(), true, true),
            Err(SessionError::LimitReached)
        );
    }

    #[test]
    fn limit_freed_by_stop() {
        let map = SessionMap::default();
        let repo = random::<RepositoryId>();
        let mut ids = Vec::new();
        for i in 0..MAX_CONCURRENT_SESSIONS {
            let (id, _) = map
                .start(repo, format!("corr-{i}"), String::new(), true, true)
                .unwrap();
            ids.push(id);
        }
        assert_eq!(
            map.start(repo, "blocked".into(), String::new(), true, true),
            Err(SessionError::LimitReached)
        );
        map.stop(ids[0]).unwrap();
        map.start(repo, "freed".into(), String::new(), true, true)
            .unwrap();
    }
}
