// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Ephemeral rename lineage from exact-current, verified commit receipts.
//! Physical base identity and checkpoint state remain unchanged. Missing lineage
//! is uncertainty, never permission to infer a source from node IDs or content.

use super::*;
use crate::core::Backend;
use crate::overlay::Access;
use crate::overlay::AccessGate;
use crate::overlay::Receipt;

pub(super) struct Renames {
    pub(super) endpoints: std::collections::BTreeSet<String>,
    pub(super) unproven: bool,
}

pub(super) struct Views<'a> {
    pub(super) original: &'a Image,
    pub(super) current: &'a ScopeView,
}

pub(super) async fn resolve(
    snapshot: &NamespaceSnapshot,
    local: &Image,
    views: Views<'_>,
    backend: &RevisionBackend,
    policy: &Arc<dyn AccessGate>,
    mut endpoints: std::collections::BTreeSet<String>,
    budget: &mut Budget,
) -> core::Result<Renames> {
    let Views { original, current } = views;
    if current.head.revision == current.info.root.revision {
        return Ok(Renames {
            endpoints,
            unproven: false,
        });
    }
    let mut current_keys = BTreeMap::new();
    for (path, metadata) in current.entries.iter() {
        budget.node()?;
        if let Some(key) = metadata.identity
            && current_keys.insert(key, (path, metadata)).is_some()
        {
            return Err(core::Error::InvalidInput);
        }
    }
    let mut original_keys = BTreeMap::new();
    for (path, metadata) in original {
        budget.node()?;
        if let Some(key) = metadata.identity
            && original_keys.insert(key, path).is_some()
        {
            return Err(core::Error::InvalidInput);
        }
    }
    let mut records = BTreeMap::new();
    let mut bases = BTreeMap::new();
    for record in &snapshot.nodes {
        budget.node()?;
        if !record.attached {
            continue;
        }
        if records.insert(record.entry.inode, record).is_some() {
            return Err(core::Error::InvalidInput);
        }
        if let Some(base) = &record.entry.base {
            budget.node()?;
            if bases.insert(base.key, record).is_some() {
                return Err(core::Error::InvalidInput);
            }
        }
    }
    let mut unproven = false;
    for (path, metadata) in local {
        budget.compare()?;
        if endpoints.contains(path) {
            continue;
        }
        let potential_relocation = !current.entries.contains_key(path)
            || metadata
                .identity
                .and_then(|key| original_keys.get(&key))
                .is_some_and(|original_path| *original_path != path)
            || metadata.identity.is_none() && metadata.local.is_some();
        if !potential_relocation {
            continue;
        }
        let record = if let Some((inode, generation)) = metadata.local {
            let record = records.get(&inode).copied().ok_or(core::Error::Stale)?;
            if snapshot
                .files
                .get(&inode)
                .map_or(0, |file| file.metadata.generation)
                != generation
            {
                return Err(core::Error::Stale);
            }
            Some(record)
        } else {
            metadata.identity.and_then(|key| bases.get(&key).copied())
        };
        let receipt = record.and_then(|record| {
            snapshot
                .files
                .get(&record.entry.inode)
                .and_then(|file| file.receipt.as_ref())
                .or(record.receipt.as_ref())
        });
        let Some((record, receipt)) = record
            .zip(receipt)
            .filter(|(_, receipt)| receipt.proof.node.key.revision == current.head.revision)
        else {
            unproven = true;
            continue;
        };
        let file = snapshot.files.get(&record.entry.inode);
        let generation = file.map_or(0, |file| file.metadata.generation);
        let size = file.map_or_else(
            || record.entry.base.as_ref().map_or(0, |base| base.size),
            |file| file.metadata.size,
        );
        let mode = file.map_or_else(
            || record.entry.base.as_ref().map_or(0, |base| base.mode),
            |file| file.metadata.mode,
        );
        receipt.validate(
            record.entry.inode,
            record.entry.authority,
            generation,
            size,
            mode,
        )?;
        if metadata.identity != record.entry.base.as_ref().map(|base| base.key)
            || metadata.summary.kind != Kind::File
            || metadata.summary.size != size
            || metadata.summary.executable != (mode & 0o111 != 0)
            || metadata.summary.immutable != Some(receipt.proof.address)
        {
            return Err(core::Error::Stale);
        }
        let (source_path, source) = current_keys
            .get(&receipt.proof.node.key)
            .copied()
            .ok_or(core::Error::Stale)?;
        if source.summary != metadata.summary {
            return Err(core::Error::Stale);
        }
        authorize(backend, policy, current, record.entry.authority, receipt).await?;
        backend
            .validate_committed(&record.entry.authority, &receipt.proof)
            .await?;
        authorize(backend, policy, current, record.entry.authority, receipt).await?;
        if source_path == path {
            continue;
        }
        if local.contains_key(source_path) {
            unproven = true;
            continue;
        }
        for endpoint in [path, source_path] {
            budget.node()?;
            budget.paths(endpoint.len())?;
            endpoints.insert(endpoint.clone());
        }
    }
    Ok(Renames {
        endpoints,
        unproven,
    })
}

async fn authorize(
    backend: &RevisionBackend,
    policy: &Arc<dyn AccessGate>,
    current: &ScopeView,
    authority: NodeKey,
    receipt: &Receipt,
) -> core::Result<()> {
    backend.revalidate(&authority, false).await?;
    policy.authorize(&authority, Access::Read).await?;
    policy.authorize_target_head(&current.proof).await?;
    backend
        .revalidate_target_node(&current.proof, &receipt.proof.node.key, false)
        .await?;
    policy
        .authorize_target_node(&current.proof, &receipt.proof.node.key, Access::Read)
        .await
}
