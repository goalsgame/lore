// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
//! Read-only, pinned revision access through an existing Lore repository context.
//!
//! Construction captures the ambient Lore execution context for adapter callbacks.
//! The caller retains responsibility for authorizing the initial repository;
//! every crossed repository link is separately checked against its context policy.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::Mutex;

use lore_base::types::Address;
use lore_base::types::Hash;
use lore_base::types::RepositoryId;
use lore_revision::immutable;
use lore_revision::interface::ExecutionContext;
use lore_revision::node::Node as RevisionNode;
use lore_revision::node::NodeFileMode;
use lore_revision::node::NodeID;
use lore_revision::node::ROOT_NODE;
use lore_revision::node::SiblingCycleGuard;
use lore_revision::repository::RepositoryContext;
use lore_revision::repository::is_reserved_node_name;
use lore_revision::runtime::LORE_CONTEXT;
use lore_revision::state::State;
use lore_revision::state::StateError;

use crate::core::Backend;
use crate::core::BackendFuture;
use crate::core::BackendPage;
use crate::core::Error;
use crate::core::NamedNode;
use crate::core::Node;
use crate::core::NodeKey;
use crate::core::NodeKind;
use crate::digest::ContentIdentity;
use crate::digest_index::DigestIndex;

const MAX_LINK_DEPTH: usize = 64;
const MAX_EXPOSURES: usize = 65_536;
const MAX_RETAINED_VIEWS: usize = 1_024;

struct Access {
    parent: Option<Arc<Access>>,
    repository: Arc<RepositoryContext>,
    target: RepositoryId,
}

#[derive(Clone)]
struct Owner {
    source_root: NodeID,
    link_depth: usize,
    access: Option<Arc<Access>>,
    repository: Arc<RepositoryContext>,
    state: Arc<State>,
    exposure: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AllocationState {
    Prepared,
    Active,
    #[cfg_attr(
        not(test),
        allow(
            dead_code,
            reason = "Retirement is not exposed by the current backend API."
        )
    )]
    Retired,
}
impl AllocationState {
    fn require_active(self) -> Result<(), Error> {
        match self {
            Self::Prepared => Err(Error::Unsupported),
            Self::Active => Ok(()),
            Self::Retired => Err(Error::Stale),
        }
    }
}

#[derive(Clone)]
struct Owners {
    views: BTreeMap<(u64, Address), RetainedView>,
    bindings: BTreeMap<(u64, Address, NodeID), u64>,
    provenance: Vec<checkpoint::Record>,
    provenance_bytes: usize,
    by_exposure: BTreeMap<u64, Owner>,
    // The parent exposure makes two independent mounts of the same child distinct.
    links: BTreeMap<(u64, NodeID), u64>,
    next: u64,
    allocation: AllocationState,
    provenance_complete: bool,
}
#[derive(Clone)]
struct RetainedView {
    state: Arc<State>,
    root: Option<NodeID>,
    authority: NodeKey,
}

/// A pinned revision backend sharing the caller's stores, sessions and link policy.
/// No working-tree paths, mutable state or legacy path digest metadata are read.
pub struct RevisionBackend {
    pub(crate) selection_identity: Arc<()>,
    publication: Arc<crate::publication_ownership::Slot>,
    mutation_gate: Arc<crate::namespace::metadata_gate::MutationGate>,
    execution: Arc<ExecutionContext>,
    owners: Mutex<Owners>,
    root: Node,
    digest_index: Option<DigestIndex>,
}

impl RevisionBackend {
    pub(crate) fn publication_slot(&self) -> &Arc<crate::publication_ownership::Slot> {
        &self.publication
    }
    /// Exact captured execution context for independently injected policy
    /// binding. This does not authorize keys or enable writable access.
    pub fn captured_execution(&self) -> Arc<ExecutionContext> {
        self.execution.clone()
    }
    /// Loads one immutable revision. A zero hash denotes Lore's empty revision.
    /// The supplied context must already authorize access to its own repository.
    pub async fn new(repository: Arc<RepositoryContext>, revision: Hash) -> Result<Self, Error> {
        let execution =
            lore_revision::runtime::try_execution_context().ok_or(Error::InvalidInput)?;
        // A mount outlives the command; retain its authorization and stores without
        // retaining the command write token or filesystem provider.
        let repository = Arc::new(repository.without_command_locks());
        let state = State::deserialize(repository.clone(), revision)
            .await
            .map_err(state_error)?;
        let owner = Owner {
            source_root: ROOT_NODE,
            link_depth: 0,
            access: None,
            repository,
            state,
            exposure: 0,
        };
        let root_node = owner
            .state
            .node(owner.repository.clone(), ROOT_NODE)
            .await
            .map_err(state_error)?;
        let root = describe(&owner, ROOT_NODE, &root_node);
        Ok(Self {
            selection_identity: Arc::new(()),
            publication: Arc::new(crate::publication_ownership::Slot::default()),
            mutation_gate: Arc::new(crate::namespace::metadata_gate::MutationGate::default()),
            execution,
            owners: Mutex::new(Owners {
                views: BTreeMap::new(),
                bindings: BTreeMap::new(),
                provenance: vec![checkpoint::Record::Floor(1)],
                provenance_bytes: 9,
                by_exposure: BTreeMap::from([(0, owner)]),
                links: BTreeMap::new(),
                next: 1,
                allocation: AllocationState::Active,
                provenance_complete: true,
            }),
            root,
            digest_index: None,
        })
    }

    /// Enable digest records in a private, caller-owned cache directory.
    pub fn with_digest_index(mut self, index: DigestIndex) -> Self {
        self.digest_index = Some(index);
        self
    }

    /// Explicitly verify and hash one complete immutable file. No xattr lookup
    /// invokes this operation. Reject files larger than the caller's byte budget.
    /// Publication requires successful stream completion and exact whole-file length.
    pub async fn backfill_digest(&self, file: &NodeKey, max_bytes: u64) -> Result<[u8; 32], Error> {
        LORE_CONTEXT
            .scope(self.execution.clone(), async move {
                let index = self.digest_index.as_ref().ok_or(Error::InvalidInput)?;
                let (owner, node) = self.node(file).await?;
                if !node.is_file() {
                    return Err(Error::IsDirectory);
                }
                if node.size > max_bytes {
                    return Err(Error::ResourceLimit);
                }
                let content = ContentIdentity {
                    address: node.address,
                    size: node.size,
                };
                let mut hash = ring::digest::Context::new(&ring::digest::SHA256);
                // Lore represents an empty file with a zero address and no stored payload.
                if node.address != Address::default() || node.size != 0 {
                    let mut options = immutable::read_options_from_repository(&owner.repository);
                    options.trust_remote = false;
                    let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
                    let producer = immutable::read_stream(
                        owner.repository.clone(),
                        node.address,
                        None,
                        options,
                        sender,
                    );
                    let consumer = async {
                        let mut received = 0u64;
                        while let Some(bytes) = receiver.recv().await {
                            let bytes = match bytes {
                                Ok(bytes) => bytes,
                                Err(_error) => {
                                    receiver.close();
                                    return Err(Error::Io);
                                }
                            };
                            let Some(total) = received.checked_add(bytes.len() as u64) else {
                                receiver.close();
                                return Err(Error::Io);
                            };
                            received = total;
                            if received > node.size {
                                receiver.close();
                                return Err(Error::Io);
                            }
                            hash.update(&bytes);
                        }
                        Ok(received)
                    };
                    let (advertised, received) = tokio::join!(producer, consumer);
                    let advertised = advertised.map_err(immutable_error)?;
                    if advertised != node.size || received? != node.size {
                        return Err(Error::Io);
                    }
                }
                let mut digest = [0; 32];
                digest.copy_from_slice(hash.finish().as_ref());
                // Cached child identities never bypass current link authorization.
                self.owner(file)?;
                index
                    .publish(owner.repository.id, content, digest)
                    .map_err(|_error| Error::Io)?;
                Ok(digest)
            })
            .await
    }

    /// Trusted immutable content identity, with current owning-route checks.
    /// This reads tree metadata only and never hydrates payload bytes.
    pub async fn content_identity(&self, key: &NodeKey) -> Result<ContentIdentity, Error> {
        LORE_CONTEXT
            .scope(self.execution.clone(), async move {
                let (_, node) = self.node(key).await?;
                if !node.is_file() {
                    return Err(Error::IsDirectory);
                }
                Ok(ContentIdentity {
                    address: node.address,
                    size: node.size,
                })
            })
            .await
    }
    pub fn root(&self) -> Node {
        self.root.clone()
    }

    fn owner(&self, key: &NodeKey) -> Result<Owner, Error> {
        let owners = self.owners.lock().map_err(|_error| Error::Io)?;
        let mut owner = owners
            .by_exposure
            .get(&key.exposure)
            .ok_or(Error::Stale)?
            .clone();
        if Address::zero_context_hash(owner.state.revision()) != key.revision {
            let view = owners
                .views
                .get(&(key.exposure, key.revision))
                .ok_or(Error::Stale)?;
            owner.state = view.state.clone();
            owner.source_root = view.root.ok_or(Error::Stale)?;
        }
        drop(owners);
        if owner.repository.id != key.repository
            || Address::zero_context_hash(owner.state.revision()) != key.revision
        {
            return Err(Error::Stale);
        }
        let mut access = owner.access.as_deref();
        while let Some(gate) = access {
            if !gate.repository.can_read_link(gate.target) {
                return Err(Error::PermissionDenied);
            }
            access = gate.parent.as_deref();
        }
        Ok(owner)
    }

    async fn node(&self, key: &NodeKey) -> Result<(Owner, RevisionNode), Error> {
        let owner = self.owner(key)?;
        let node = checkpoint::scoped_node(&owner, key.node).await?;
        if node.is_link() {
            return Err(Error::PermissionDenied);
        }
        Ok((owner, node))
    }

    async fn resolve(&self, mut owner: Owner, mut id: NodeID) -> Result<Node, Error> {
        for _ in 0..MAX_LINK_DEPTH {
            let node = owner
                .state
                .node(owner.repository.clone(), id)
                .await
                .map_err(state_error)?;
            if !node.is_link() {
                return Ok(describe(&owner, id, &node));
            }
            if owner.link_depth >= MAX_LINK_DEPTH {
                return Err(Error::Io);
            }
            let link = node.linked_node();
            if !owner.repository.can_read_link(link.repository) {
                // Expose only the parent's mount point, never target metadata.
                // Traversal of this opaque entry fails in `node`.
                return Ok(describe(&owner, id, &node));
            }
            let cached = {
                let owners = self.owners.lock().map_err(|_error| Error::Io)?;
                let original = owners
                    .by_exposure
                    .get(&owner.exposure)
                    .ok_or(Error::Stale)?;
                let is_original = owner.state.revision() == original.state.revision();
                let cached = if is_original {
                    owners.links.get(&(owner.exposure, id))
                } else {
                    owners.bindings.get(&(
                        owner.exposure,
                        Address::zero_context_hash(owner.state.revision()),
                        id,
                    ))
                }
                .and_then(|exposure| owners.by_exposure.get(exposure))
                .cloned();
                if cached.is_none() {
                    owners.allocation.require_active()?;
                    if !is_original {
                        return Err(Error::Unsupported);
                    }
                    self.mutation_gate.open()?;
                }
                cached
            };
            let child = if let Some(child) = cached {
                child
            } else {
                // Never deserialize or read child content in the parent's partition.
                let repository = owner.repository.to_link_context(link.repository).await;
                let state = State::deserialize(repository.clone(), link.revision)
                    .await
                    .map_err(state_error)?;
                let mut owners = self.owners.lock().map_err(|_error| Error::Io)?;
                if let Some(exposure) = owners.links.get(&(owner.exposure, id)) {
                    owners.by_exposure.get(exposure).ok_or(Error::Io)?.clone()
                } else {
                    // Child metadata IO can outlive a change in allocation admission.
                    owners.allocation.require_active()?;
                    let _mutation = self.mutation_gate.mutation()?;
                    if owners.by_exposure.len() >= MAX_EXPOSURES {
                        return Err(Error::ResourceLimit);
                    }
                    let exposure = owners.next;
                    let child = Owner {
                        source_root: link.node,
                        link_depth: owner.link_depth + 1,
                        access: Some(Arc::new(Access {
                            parent: owner.access.clone(),
                            repository: owner.repository.clone(),
                            target: link.repository,
                        })),
                        repository,
                        state,
                        exposure,
                    };
                    checkpoint::record_origin(
                        &mut owners,
                        NodeKey {
                            node: id,
                            ..scopes::root(&owner)
                        },
                        checkpoint::key(&child),
                    )?;
                    owners.links.insert((owner.exposure, id), exposure);
                    owners.by_exposure.insert(exposure, child.clone());
                    child
                }
            };
            owner = child;
            id = link.node;
        }
        Err(Error::Io)
    }
}

fn describe(owner: &Owner, id: NodeID, node: &RevisionNode) -> Node {
    Node {
        key: NodeKey {
            repository: owner.repository.id,
            revision: Address::zero_context_hash(owner.state.revision()),
            node: id,
            exposure: owner.exposure,
        },
        kind: if !node.is_file() {
            NodeKind::Directory
        } else {
            NodeKind::File
        },
        size: if node.is_file() { node.size } else { 0 },
        mode: if !node.is_file() || node.mode & NodeFileMode::Executable.bits() != 0 {
            0o555
        } else {
            0o444
        },
    }
}

fn immutable_error(error: immutable::ImmutableError) -> Error {
    match error {
        immutable::ImmutableError::NotAuthorized(_)
        | immutable::ImmutableError::NotAuthenticated(_) => Error::PermissionDenied,
        _ => Error::Io,
    }
}

fn state_error(error: StateError) -> Error {
    match error {
        StateError::NodeNotFound(_)
        | StateError::NotFound(_)
        | StateError::LinkNotFound(_)
        | StateError::RevisionNotFound(_)
        | StateError::AddressNotFound(_) => Error::NotFound,
        StateError::NotAuthorized(_) | StateError::NotAuthenticated(_) => Error::PermissionDenied,
        _ => Error::Io,
    }
}

impl Backend for RevisionBackend {
    fn underlay_view<'a>(
        &'a self,
        scope: &'a NodeKey,
        head: &'a crate::core::ScopeHead,
    ) -> BackendFuture<'a, NodeKey> {
        Box::pin(LORE_CONTEXT.scope(self.execution.clone(), async move {
            self.owner(scope)?;
            let original = self
                .owners
                .lock()
                .map_err(|_poison| Error::Io)?
                .by_exposure
                .get(&scope.exposure)
                .ok_or(Error::Stale)?
                .clone();
            if scopes::root(&original) != *scope {
                return Err(Error::InvalidInput);
            }
            checkpoint::scoped_node(&original, scope.node).await?;
            if head.revision == scope.revision && head.root == Some(scope.node) {
                return Ok(*scope);
            }
            let owners = self.owners.lock().map_err(|_poison| Error::Io)?;
            let view = owners
                .views
                .get(&(scope.exposure, head.revision))
                .ok_or(Error::Stale)?;
            if view.root != head.root {
                return Err(Error::Stale);
            }
            Ok(view.authority)
        }))
    }
    fn as_any(&self) -> Option<&dyn std::any::Any> {
        Some(self)
    }
    fn owning_scope<'a>(&'a self, key: &'a NodeKey) -> BackendFuture<'a, crate::core::ScopeInfo> {
        Box::pin(LORE_CONTEXT.scope(self.execution.clone(), scopes::info(self, key)))
    }
    fn scope_head<'a>(
        &'a self,
        scope: &'a NodeKey,
        revision: Address,
    ) -> BackendFuture<'a, crate::core::ScopeHead> {
        Box::pin(LORE_CONTEXT.scope(self.execution.clone(), scopes::head(self, scope, revision)))
    }
    fn scope_projection<'a>(
        &'a self,
        scope: &'a NodeKey,
        revision: Address,
    ) -> BackendFuture<'a, Option<crate::core::ScopeProjection>> {
        Box::pin(LORE_CONTEXT.scope(
            self.execution.clone(),
            scopes::projection(self, scope, revision),
        ))
    }

    fn content_identity<'a>(
        &'a self,
        key: &'a NodeKey,
    ) -> BackendFuture<'a, Option<ContentIdentity>> {
        Box::pin(async move { RevisionBackend::content_identity(self, key).await.map(Some) })
    }
    fn validate_committed<'a>(
        &'a self,
        anchor: &'a NodeKey,
        proof: &'a crate::overlay::CommittedProof,
    ) -> BackendFuture<'a, ()> {
        Box::pin(LORE_CONTEXT.scope(self.execution.clone(), async move {
            if proof.node.key.repository != anchor.repository
                || proof.node.key.exposure != anchor.exposure
                || !proof.node.key.revision.context.is_zero()
            {
                return Err(Error::Stale);
            }
            let mut owner = self.owner(anchor)?;
            let original = checkpoint::scoped_node(&owner, anchor.node).await?;
            if original.is_link() {
                return Err(Error::PermissionDenied);
            }
            let scope = {
                let owners = self.owners.lock().map_err(|_poison| Error::Io)?;
                scopes::root(
                    owners
                        .by_exposure
                        .get(&anchor.exposure)
                        .ok_or(Error::Stale)?,
                )
            };
            owner.source_root = scopes::head(self, &scope, proof.node.key.revision)
                .await?
                .root
                .ok_or(Error::Stale)?;
            owner.state =
                State::deserialize(owner.repository.clone(), proof.node.key.revision.hash)
                    .await
                    .map_err(state_error)?;
            let actual = checkpoint::scoped_node(&owner, proof.node.key.node).await?;
            if !actual.is_file()
                || actual.address != proof.address
                || describe(&owner, proof.node.key.node, &actual) != proof.node
            {
                return Err(Error::Stale);
            }
            self.owner(anchor)?;
            Ok(())
        }))
    }
    fn checkpoint_state(&self) -> BackendFuture<'_, Vec<u8>> {
        Box::pin(LORE_CONTEXT.scope(
            self.execution.clone(),
            async move { checkpoint::capture(self) },
        ))
    }
    fn prepare_restore<'a>(
        &'a self,
        state: &'a [u8],
    ) -> BackendFuture<'a, Option<Arc<dyn Backend>>> {
        Box::pin(LORE_CONTEXT.scope(self.execution.clone(), checkpoint::prepare(self, state)))
    }
    fn revalidate<'a>(&'a self, key: &'a NodeKey, write: bool) -> BackendFuture<'a, ()> {
        Box::pin(LORE_CONTEXT.scope(self.execution.clone(), async move {
            let owner = self.owner(key)?;
            let node = checkpoint::scoped_node(&owner, key.node).await?;
            if write && node.is_link() {
                return Err(Error::PermissionDenied);
            }
            Ok(())
        }))
    }
    fn lookup<'a>(&'a self, parent: &'a NodeKey, name: &'a [u8]) -> BackendFuture<'a, Node> {
        Box::pin(LORE_CONTEXT.scope(self.execution.clone(), async move {
            let name = std::str::from_utf8(name).map_err(|_error| Error::InvalidInput)?;
            if name.is_empty()
                || name.len() > 1000
                || name.contains(['/', '\\', '\0'])
                || name == "."
                || name == ".."
            {
                return Err(Error::InvalidInput);
            }
            if is_reserved_node_name(name) {
                return Err(Error::NotFound);
            }
            let (owner, directory) = self.node(parent).await?;
            if !directory.is_directory() {
                return Err(Error::NotDirectory);
            }
            let child = owner
                .state
                .find_relative_node_link(owner.repository.clone(), parent.node, name)
                .await
                .map_err(state_error)?;
            self.resolve(owner, child.node).await
        }))
    }

    fn list<'a>(
        &'a self,
        directory: &'a NodeKey,
        offset: u64,
        limit: usize,
    ) -> BackendFuture<'a, BackendPage> {
        Box::pin(LORE_CONTEXT.scope(self.execution.clone(), async move {
            if limit == 0 || limit > 1024 {
                return Err(Error::InvalidInput);
            }
            let (owner, node) = self.node(directory).await?;
            if !node.is_directory() {
                return Err(Error::NotDirectory);
            }
            let mut cursor = node.child();
            let mut cycle = SiblingCycleGuard::new(directory.node);
            let mut ordinal = 0u64;
            let mut entries = Vec::new();
            while let Some(id) = cursor {
                let child = owner
                    .state
                    .node(owner.repository.clone(), id)
                    .await
                    .map_err(state_error)?;
                child
                    .walk_step(id, directory.node, &mut cycle)
                    .map_err(|_error| Error::Io)?;
                cursor = child.sibling();
                let Some(name) = owner
                    .state
                    .node_name_ref_or_skip(owner.repository.clone(), id)
                    .await
                    .map_err(state_error)?
                else {
                    continue;
                };
                if ordinal >= offset {
                    if entries.len() == limit {
                        return Ok(BackendPage {
                            entries,
                            next_offset: Some(ordinal),
                        });
                    }
                    let name: &str = name.as_ref();
                    entries.push(NamedNode {
                        name: name.as_bytes().to_vec(),
                        node: self.resolve(owner.clone(), id).await?,
                    });
                }
                ordinal += 1;
            }
            Ok(BackendPage {
                entries,
                next_offset: None,
            })
        }))
    }

    fn read<'a>(
        &'a self,
        file: &'a NodeKey,
        offset: u64,
        size: usize,
    ) -> BackendFuture<'a, Vec<u8>> {
        Box::pin(LORE_CONTEXT.scope(self.execution.clone(), async move {
            if size > 1024 * 1024 {
                return Err(Error::InvalidInput);
            }
            let (owner, node) = self.node(file).await?;
            if !node.is_file() {
                return Err(Error::IsDirectory);
            }
            let count = (node.size.saturating_sub(offset)).min(size as u64) as usize;
            if count == 0 {
                return Ok(Vec::new());
            }
            let start = usize::try_from(offset).map_err(|_error| Error::Range)?;
            let end = start.checked_add(count).ok_or(Error::Range)?;
            let mut bytes = vec![0; count];
            let mut options = immutable::read_options_from_repository(&owner.repository);
            options.trust_remote = false;
            immutable::read_into(
                owner.repository,
                node.address,
                Some(start..end),
                &mut bytes,
                options,
            )
            .await
            .map_err(immutable_error)?;
            Ok(bytes)
        }))
    }

    fn digest<'a>(&'a self, file: &'a NodeKey) -> BackendFuture<'a, Option<[u8; 32]>> {
        Box::pin(LORE_CONTEXT.scope(self.execution.clone(), async move {
            let (owner, node) = self.node(file).await?;
            if !node.is_file() {
                return Err(Error::IsDirectory);
            }
            self.owner(file)?;
            Ok(self.digest_index.as_ref().and_then(|index| {
                index.lookup(
                    owner.repository.id,
                    ContentIdentity {
                        address: node.address,
                        size: node.size,
                    },
                )
            }))
        }))
    }
}

#[cfg(test)]
#[path = "revision_tests.rs"]
mod tests;

#[path = "revision_checkpoint.rs"]
mod checkpoint;

#[path = "revision_scopes.rs"]
mod scopes;

#[path = "revision_transition.rs"]
mod transition;

#[path = "revision_target.rs"]
mod target;

#[path = "revision_candidate.rs"]
mod candidate;

pub(crate) use transition::MissingTargetPath;
pub(crate) use transition::VerifiedImage;

#[allow(
    dead_code,
    reason = "Independent complete routing preparation is not a namespace installation capability."
)]
#[path = "revision_complete.rs"]
mod complete;
pub(crate) use complete::PreparedCompleteBackend;

#[allow(
    dead_code,
    reason = "Scope identity facts do not replace namespace mutation or access checks."
)]
#[path = "revision_membership.rs"]
mod membership;
#[allow(
    unused_imports,
    reason = "Membership facts authorize no namespace mutations."
)]
pub(crate) use membership::OwnerMembership;
#[allow(
    unused_imports,
    reason = "Membership facts authorize no namespace mutations."
)]
pub(crate) use membership::OwnerMembershipKind;

#[path = "revision_inventory.rs"]
mod inventory;

#[path = "revision_install.rs"]
mod install;

#[path = "revision_dependencies.rs"]
mod dependencies;
