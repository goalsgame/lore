// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Isolated immutable reads for a prepared target. This is not the merged dirty
//! candidate and cannot publish keys into a live namespace or old checkpoint.

use lore_base::runtime::LORE_CONTEXT;

use super::*;
use crate::core::Backend;
use crate::core::BackendFuture;
use crate::core::BackendPage;
use crate::core::Node;
use crate::digest::ContentIdentity;
use crate::overlay::Access;
use crate::overlay::AccessGate;

/// Retains the source freeze until every returned backend capability is dropped.
/// Reads hydrate only explicitly requested immutable ranges; construction,
/// metadata and digest lookup never read file payloads. New/changed link routes
/// and unchanged routes not already independently resolved are unsupported.
pub struct PreparedTargetView {
    backend: Arc<ViewBackend>,
    root: Option<Node>,
}
impl PreparedTargetView {
    pub fn root(&self) -> Option<&Node> {
        self.root.as_ref()
    }
    pub fn backend(&self) -> Arc<dyn Backend> {
        self.backend.clone()
    }
    pub fn preparation(&self) -> &PreparedTransition {
        &self.backend.preparation
    }
}
impl PreparedTransition {
    pub async fn prepare_target_view(self) -> Result<PreparedTargetView> {
        self.validate_source().await?;
        let original = self.source_routing.backend().clone();
        let backend = original
            .as_any()
            .and_then(|backend| backend.downcast_ref::<RevisionBackend>())
            .ok_or(Error::Unsupported("scoped revision metadata reader"))?;
        let policy = self.source_routing.policy();
        let mut budget = Budget::new(self.limits)?;
        let scope = self.intent.scope;
        let origin = backend
            .transition_view(scope, scope.revision, policy.clone(), &mut budget)
            .await?;
        let target = backend
            .transition_view(
                scope,
                self.intent.target.revision,
                policy.clone(),
                &mut budget,
            )
            .await?;
        if target.head != self.intent.target {
            return Err(Error::StaleSource);
        }
        let fork = backend
            .transition_fork(scope, &origin, &target, &mut budget)
            .await?;
        let root = fork.as_ref().map(|backend| backend.root());
        self.validate_source().await?;
        policy.authorize_target_head(&target.proof).await?;
        let views = fork
            .into_iter()
            .map(|backend| ((scope.exposure, target.head.revision), backend))
            .collect();
        let execution = backend.transition_execution();
        Ok(PreparedTargetView {
            root,
            backend: Arc::new(ViewBackend {
                original,
                execution,
                views,
                proof: target.proof,
                policy,
                active: Arc::new(tokio::sync::Semaphore::new(64)),
                preparation: self,
            }),
        })
    }
}

struct ViewBackend {
    execution: Arc<lore_revision::interface::ExecutionContext>,
    original: Arc<dyn Backend>,
    // Fixed after construction; existing keys never change their immutable State.
    views: BTreeMap<(u64, Address), Arc<RevisionBackend>>,
    proof: Arc<crate::overlay::ProvenTargetMetadata>,
    policy: Arc<dyn AccessGate>,
    active: Arc<tokio::sync::Semaphore>,
    preparation: PreparedTransition,
}
impl ViewBackend {
    fn operation<'a, T: 'a>(
        &'a self,
        future: impl std::future::Future<Output = core::Result<T>> + Send + 'a,
    ) -> BackendFuture<'a, T> {
        Box::pin(LORE_CONTEXT.scope(self.execution.clone(), future))
    }
    fn admit(&self) -> core::Result<tokio::sync::OwnedSemaphorePermit> {
        self.active
            .clone()
            .try_acquire_owned()
            .map_err(|_full| core::Error::ResourceLimit)
    }
    fn selected(&self, key: &NodeKey) -> core::Result<&dyn Backend> {
        if let Some(view) = self.views.get(&(key.exposure, key.revision)) {
            return Ok(view.as_ref());
        }
        let original = self
            .original
            .as_any()
            .and_then(|backend| backend.downcast_ref::<RevisionBackend>())
            .ok_or(core::Error::Unsupported)?;
        original.transition_allows_origin(self.preparation.intent.scope, key)?;
        Ok(self.original.as_ref())
    }
    async fn authorize(&self, key: &NodeKey) -> core::Result<()> {
        let scope = self.preparation.intent.scope;
        self.policy.authorize(&scope, Access::Read).await?;
        self.policy.authorize_target_head(&self.proof).await?;
        if self.views.contains_key(&(key.exposure, key.revision)) {
            self.policy
                .authorize_target_node(&self.proof, key, Access::Read)
                .await
        } else {
            self.policy.authorize(key, Access::Read).await
        }
    }
    async fn check(&self, key: &NodeKey) -> core::Result<()> {
        let scope = self.preparation.intent.scope;
        self.original.revalidate(&scope, false).await?;
        self.authorize(key).await?;
        self.selected(key)?.revalidate(key, false).await?;
        self.authorize(key).await
    }
}
impl Backend for ViewBackend {
    fn checkpoint_state(&self) -> BackendFuture<'_, Vec<u8>> {
        self.operation(async { Err(core::Error::Unsupported) })
    }
    fn prepare_restore<'a>(
        &'a self,
        _state: &'a [u8],
    ) -> BackendFuture<'a, Option<Arc<dyn Backend>>> {
        self.operation(async { Err(core::Error::Unsupported) })
    }
    fn revalidate<'a>(&'a self, key: &'a NodeKey, write: bool) -> BackendFuture<'a, ()> {
        self.operation(async move {
            let _permit = self.admit()?;
            if write {
                return Err(core::Error::ReadOnly);
            }
            self.check(key).await
        })
    }
    fn owning_scope<'a>(&'a self, key: &'a NodeKey) -> BackendFuture<'a, ScopeInfo> {
        self.operation(async move {
            let _permit = self.admit()?;
            self.check(key).await?;
            let anchor = if self.views.contains_key(&(key.exposure, key.revision)) {
                &self.preparation.intent.scope
            } else {
                key
            };
            let info = self.original.owning_scope(anchor).await?;
            self.check(key).await?;
            Ok(info)
        })
    }
    fn lookup<'a>(&'a self, parent: &'a NodeKey, name: &'a [u8]) -> BackendFuture<'a, Node> {
        self.operation(async move {
            let _permit = self.admit()?;
            self.check(parent).await?;
            let node = self.selected(parent)?.lookup(parent, name).await?;
            self.check(parent).await?;
            self.check(&node.key).await?;
            Ok(node)
        })
    }
    fn list<'a>(
        &'a self,
        directory: &'a NodeKey,
        offset: u64,
        limit: usize,
    ) -> BackendFuture<'a, BackendPage> {
        self.operation(async move {
            let _permit = self.admit()?;
            if limit == 0 || limit > 1024 {
                return Err(core::Error::InvalidInput);
            }
            self.check(directory).await?;
            let page = self
                .selected(directory)?
                .list(directory, offset, limit)
                .await?;
            for entry in &page.entries {
                self.check(&entry.node.key).await?;
            }
            self.check(directory).await?;
            Ok(page)
        })
    }
    fn read<'a>(
        &'a self,
        file: &'a NodeKey,
        offset: u64,
        size: usize,
    ) -> BackendFuture<'a, Vec<u8>> {
        self.operation(async move {
            let _permit = self.admit()?;
            if size > 1024 * 1024 {
                return Err(core::Error::InvalidInput);
            }
            self.check(file).await?;
            let bytes = self.selected(file)?.read(file, offset, size).await?;
            self.check(file).await?;
            Ok(bytes)
        })
    }
    fn digest<'a>(&'a self, file: &'a NodeKey) -> BackendFuture<'a, Option<[u8; 32]>> {
        self.operation(async move {
            let _permit = self.admit()?;
            self.check(file).await?;
            let digest = self.selected(file)?.digest(file).await?;
            self.check(file).await?;
            Ok(digest)
        })
    }
    fn content_identity<'a>(
        &'a self,
        file: &'a NodeKey,
    ) -> BackendFuture<'a, Option<ContentIdentity>> {
        self.operation(async move {
            let _permit = self.admit()?;
            self.check(file).await?;
            let identity = self.selected(file)?.content_identity(file).await?;
            self.check(file).await?;
            Ok(identity)
        })
    }
}
