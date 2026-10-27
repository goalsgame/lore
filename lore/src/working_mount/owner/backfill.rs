// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Explicit verified whole-file hashing during locally authorized import setup.
//! It does not materialize overlay files or add work to a later xattr lookup.
use std::collections::BTreeSet;
use std::sync::Arc;

use lore_vfs::core::Backend;
use lore_vfs::core::NodeKind;
use lore_vfs::overlay::Access;
use lore_vfs::revision::RevisionBackend;

use super::Error;
use super::OwnerLimits;
use super::Result;
use super::problem;
use crate::working_mount::local_policy::LocalCheckoutFactory;

pub(super) async fn populate(
    backend: Arc<RevisionBackend>,
    policy: &LocalCheckoutFactory,
    limits: OwnerLimits,
) -> Result<()> {
    let erased: Arc<dyn Backend> = backend.clone();
    let gate = policy.bind(erased).map_err(problem)?;
    let mut pending = vec![backend.root()];
    let mut visited = BTreeSet::new();
    let mut remaining =
        lore_revision::repository::working_bootstrap::ImportLimits::default().max_bytes;
    while let Some(node) = pending.pop() {
        if !visited.insert(node.key) {
            continue;
        }
        if visited.len() > limits.namespace.max_inodes {
            return Err(Error::Owner("digest backfill inode limit exceeded".into()));
        }
        gate.authorize(&node.key, Access::Read)
            .await
            .map_err(problem)?;
        match node.kind {
            NodeKind::File => {
                if node.size > remaining {
                    return Err(Error::Owner("digest backfill byte limit exceeded".into()));
                }
                backend
                    .backfill_digest(&node.key, remaining)
                    .await
                    .map_err(problem)?;
                gate.authorize(&node.key, Access::Read)
                    .await
                    .map_err(problem)?;
                remaining -= node.size;
            }
            NodeKind::Directory => {
                let mut offset = 0u64;
                loop {
                    gate.authorize(&node.key, Access::Read)
                        .await
                        .map_err(problem)?;
                    let page = backend
                        .list(&node.key, offset, 1024)
                        .await
                        .map_err(problem)?;
                    gate.authorize(&node.key, Access::Read)
                        .await
                        .map_err(problem)?;
                    let end = offset
                        .checked_add(page.entries.len() as u64)
                        .filter(|end| *end <= limits.namespace.max_directory_entries as u64)
                        .ok_or_else(|| {
                            Error::Owner("digest backfill directory limit exceeded".into())
                        })?;
                    for child in page.entries {
                        if visited
                            .len()
                            .checked_add(pending.len())
                            .is_none_or(|count| count >= limits.namespace.max_inodes)
                        {
                            return Err(Error::Owner(
                                "digest backfill inode limit exceeded".into(),
                            ));
                        }
                        pending.try_reserve(1).map_err(|_capacity| {
                            Error::Owner("digest backfill allocation failed".into())
                        })?;
                        pending.push(child.node);
                    }
                    match page.next_offset {
                        None => break,
                        Some(next) if next == end && next > offset => offset = next,
                        Some(_) => {
                            return Err(Error::Owner(
                                "digest backfill invalid directory cursor".into(),
                            ));
                        }
                    }
                }
            }
            NodeKind::Symlink => {}
        }
        gate.authorize(&node.key, Access::Read)
            .await
            .map_err(problem)?;
    }
    Ok(())
}
