// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Frozen inventory retains every immutable base version independently.

use super::*;

pub(super) async fn inventory(
    snapshot: &NamespaceSnapshot,
    backend: &RevisionBackend,
    scope: NodeKey,
    inode: u64,
    policy: Arc<dyn crate::overlay::AccessGate>,
    budget: &mut Budget,
) -> core::Result<Image> {
    Box::pin(lore_revision::runtime::LORE_CONTEXT.scope(
        backend.transition_execution(),
        inventory_active(snapshot, backend, scope, inode, policy, budget),
    ))
    .await
}

async fn inventory_active(
    snapshot: &NamespaceSnapshot,
    backend: &RevisionBackend,
    scope: NodeKey,
    inode: u64,
    policy: Arc<dyn crate::overlay::AccessGate>,
    budget: &mut Budget,
) -> core::Result<Image> {
    use crate::core::NodeKind;
    let mut records = BTreeMap::new();
    for record in &snapshot.nodes {
        budget.node()?;
        if records.insert(record.entry.inode, record).is_some() {
            return Err(core::Error::InvalidInput);
        }
    }
    let mut catalogue = Catalogue {
        versions: BTreeMap::new(),
        backend,
        scope,
        policy,
    };
    let mut image = Image::new();
    let mut pending = vec![(inode, String::new(), 0usize)];
    let mut seen = std::collections::BTreeSet::new();
    let mut authorities = std::collections::BTreeSet::new();
    while let Some((inode, path, depth)) = pending.pop() {
        budget.node()?;
        budget.depth(depth)?;
        if !seen.insert(inode) {
            return Err(core::Error::InvalidInput);
        }
        let record = records.get(&inode).ok_or(core::Error::InvalidInput)?;
        if !record.attached {
            return Err(core::Error::Stale);
        }
        budget.node()?;
        authorities.insert(record.entry.authority);
        catalogue
            .policy
            .authorize(&record.entry.authority, crate::overlay::Access::Read)
            .await?;
        let fact = backend
            .prove_owner_membership(&record.entry.authority, budget)
            .await?;
        if !fact.matches_key(backend, &record.entry.authority) {
            return Err(core::Error::Stale);
        }
        let owner = fact
            .original_scope()
            .or(fact.boundary_source_scope())
            .ok_or(core::Error::PermissionDenied)?;
        if owner != scope || fact.kind() == crate::revision::OwnerMembershipKind::Boundary {
            let parent_link = if owner != scope {
                let revision = snapshot
                    .active_underlays
                    .get(&scope)
                    .map_or(scope.revision, |head| head.revision);
                backend.inventory_parent_link(scope, revision, owner, budget)?
            } else {
                record.entry.authority
            };
            let version = catalogue.version(parent_link, budget).await?;
            let source = version.source(&parent_link)?;
            if source.summary.kind != Kind::Link {
                return Err(core::Error::InvalidInput);
            }
            if owner != scope
                && source.summary.link.as_ref()
                    != Some(&ScopePin {
                        repository: owner.repository,
                        revision: owner.revision,
                        node: owner.node,
                    })
            {
                return Err(core::Error::Unsupported);
            }
            let mut entry = copy_at(source, &path, budget)?;
            if owner != scope {
                if let Some(baseline) = snapshot.baselines.get(&owner) {
                    entry.summary.link = baseline.head.root.map(|node| ScopePin {
                        repository: owner.repository,
                        revision: baseline.head.revision,
                        node,
                    });
                }
                // Child state is preserved opaquely. A materialized child may
                // contain structural changes; without inspecting another scope,
                // treat that state conservatively when an incoming link overlaps.
                if record.names.is_some() || snapshot.files.contains_key(&inode) {
                    entry.uncertain = true;
                }
            }
            insert(&mut image, entry)?;
            continue;
        }
        let version = if let Some(base) = &record.entry.base {
            Some(catalogue.version(base.key, budget).await?)
        } else {
            None
        };
        let original = match (&record.entry.base, version) {
            (Some(base), Some(version)) => Some(version.source(&base.key)?),
            _ => None,
        };
        if record.entry.kind == NodeKind::Directory {
            if let Some(names) = &record.names {
                let entry = Metadata {
                    path: path.clone(),
                    summary: NodeSummary {
                        kind: Kind::Directory,
                        size: 0,
                        executable: false,
                        immutable: None,
                        link: None,
                    },
                    identity: record.entry.base.as_ref().map(|base| base.key),
                    node: record.entry.authority.node,
                    flags: 0,
                    local: Some((inode, 0)),
                    revision: record
                        .entry
                        .base
                        .as_ref()
                        .map_or(record.entry.authority.revision, |base| base.key.revision),
                    uncertain: false,
                };
                insert(&mut image, entry)?;
                for (name, child) in names {
                    let name =
                        std::str::from_utf8(name).map_err(|_invalid| core::Error::InvalidInput)?;
                    let child_path = join(&path, name, budget)?;
                    budget.depth(depth + 1)?;
                    pending.push((*child, child_path, depth + 1));
                }
            } else {
                let source = original.ok_or(core::Error::InvalidInput)?;
                if source.summary.kind != Kind::Directory {
                    return Err(core::Error::InvalidInput);
                }
                budget.paths(
                    source
                        .path
                        .len()
                        .checked_add(1)
                        .ok_or(core::Error::ResourceLimit)?
                        .checked_mul(8)
                        .ok_or(core::Error::ResourceLimit)?,
                )?;
                let prefix = source.path.to_lowercase();
                insert(&mut image, copy_at(source, &path, budget)?)?;
                let descendants = if prefix.is_empty() {
                    String::new()
                } else {
                    format!("{prefix}/")
                };
                let immutable = &version.ok_or(core::Error::InvalidInput)?.view.entries;
                for (key, value) in immutable.range(descendants.clone()..) {
                    if key.is_empty() {
                        continue;
                    }
                    if !key.starts_with(&descendants) {
                        break;
                    }
                    let suffix = value
                        .path
                        .strip_prefix(&source.path)
                        .ok_or(core::Error::InvalidInput)?
                        .trim_start_matches('/');
                    let new_path = join(&path, suffix, budget)?;
                    budget
                        .depth(depth + suffix.split('/').filter(|part| !part.is_empty()).count())?;
                    insert(&mut image, copy_at(value, &new_path, budget)?)?;
                }
            }
            continue;
        }
        if record.entry.kind != NodeKind::File {
            return Err(core::Error::Unsupported);
        }
        let file = snapshot.files.get(&inode);
        let size = file.map_or_else(
            || record.entry.base.as_ref().map_or(0, |base| base.size),
            |file| file.metadata.size,
        );
        let mode = file.map_or_else(
            || record.entry.base.as_ref().map_or(0, |base| base.mode),
            |file| file.metadata.mode,
        );
        let generation = file.map_or(0, |file| file.metadata.generation);
        let receipt = file
            .and_then(|file| file.receipt.as_ref())
            .or(record.receipt.as_ref());
        let mut metadata = if let Some(source) = original {
            copy_at(source, &path, budget)?
        } else {
            budget.paths(
                path.len()
                    .checked_mul(8)
                    .ok_or(core::Error::ResourceLimit)?,
            )?;
            Metadata {
                path: path.clone(),
                summary: NodeSummary {
                    kind: Kind::File,
                    size,
                    executable: mode & 0o111 != 0,
                    immutable: None,
                    link: None,
                },
                identity: None,
                node: record.entry.authority.node,
                flags: 0,
                local: Some((inode, generation)),
                revision: record
                    .entry
                    .base
                    .as_ref()
                    .map_or(record.entry.authority.revision, |base| base.key.revision),
                uncertain: true,
            }
        };
        metadata.summary.size = size;
        metadata.summary.executable = mode & 0o111 != 0;
        if let Some(receipt) = receipt {
            receipt.validate(inode, record.entry.authority, generation, size, mode)?;
            metadata.summary.immutable = Some(receipt.proof.address);
            metadata.flags = 0;
            metadata.uncertain = false;
        } else if file.is_some_and(|file| file.metadata.mutable) {
            metadata.summary.immutable = None;
            metadata.uncertain = true;
        }
        if file.is_some() {
            metadata.local = Some((inode, generation));
        }
        insert(&mut image, metadata)?;
    }
    catalogue.revalidate(budget).await?;
    for key in &authorities {
        budget.node()?;
        catalogue
            .policy
            .authorize(key, crate::overlay::Access::Read)
            .await?;
    }
    Ok(image)
}

struct RegisteredVersion {
    view: ScopeView,
    nodes: BTreeMap<NodeKey, String>,
}
impl RegisteredVersion {
    fn source(&self, key: &NodeKey) -> core::Result<&Metadata> {
        let path = self.nodes.get(key).ok_or(core::Error::Stale)?;
        self.view.entries.get(path).ok_or(core::Error::Stale)
    }
}
struct Catalogue<'a> {
    versions: BTreeMap<Address, RegisteredVersion>,
    backend: &'a RevisionBackend,
    scope: NodeKey,
    policy: Arc<dyn crate::overlay::AccessGate>,
}
impl Catalogue<'_> {
    async fn revalidate(&self, budget: &mut Budget) -> core::Result<()> {
        for version in self.versions.values() {
            for metadata in version.view.entries.values() {
                budget.node()?;
                let key = metadata.identity.ok_or(core::Error::Stale)?;
                self.backend
                    .revalidate_target_node(&version.view.proof, &key, false)
                    .await?;
                self.policy
                    .authorize_target_node(&version.view.proof, &key, crate::overlay::Access::Read)
                    .await?;
            }
            self.backend
                .revalidate_target_head(&version.view.proof)
                .await?;
            self.policy
                .authorize_target_head(&version.view.proof)
                .await?;
        }
        self.policy
            .authorize(&self.scope, crate::overlay::Access::Read)
            .await
    }

    async fn version(
        &mut self,
        key: NodeKey,
        budget: &mut Budget,
    ) -> core::Result<&RegisteredVersion> {
        // Membership is checked even on a cache hit: an arbitrary node sharing
        // the version/exposure may lie outside the registered selected subtree.
        let fact = self.backend.prove_owner_membership(&key, budget).await?;
        if !fact.matches_key(self.backend, &key)
            || fact.original_scope().or(fact.boundary_source_scope()) != Some(self.scope)
        {
            return Err(core::Error::PermissionDenied);
        }
        if !self.versions.contains_key(&key.revision) {
            budget.node()?;
            let view = self
                .backend
                .inventory_registered_view(self.scope, key, self.policy.clone(), budget)
                .await?;
            let mut nodes = BTreeMap::new();
            for (path, metadata) in view.entries.iter() {
                budget.node()?;
                budget.paths(path.len())?;
                let full_key = metadata.identity.ok_or(core::Error::Stale)?;
                if nodes.insert(full_key, path.clone()).is_some() {
                    return Err(core::Error::InvalidInput);
                }
            }
            self.versions
                .insert(key.revision, RegisteredVersion { view, nodes });
        }
        let version = self.versions.get(&key.revision).ok_or(core::Error::Stale)?;
        self.backend
            .revalidate_target_head(&version.view.proof)
            .await?;
        self.policy
            .authorize_target_head(&version.view.proof)
            .await?;
        self.policy
            .authorize_target_node(&version.view.proof, &key, crate::overlay::Access::Read)
            .await?;
        Ok(version)
    }
}
