// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Credential-free, append-only provenance for pinned exposure identities.
use std::collections::BTreeSet;

use super::*;

const MAGIC: &[u8; 8] = b"LORERT02";
const KEY_SIZE: usize = 16 + 16 + 32 + 4 + 8;
const HEADER_SIZE: usize = 8 + KEY_SIZE;
const MAX_RECORDS: usize = MAX_EXPOSURES * 4;

fn encode_key(out: &mut Vec<u8>, key: NodeKey) {
    out.extend_from_slice(key.repository.data());
    out.extend_from_slice(key.revision.context.data());
    out.extend_from_slice(key.revision.hash.data());
    out.extend_from_slice(&key.node.to_le_bytes());
    out.extend_from_slice(&key.exposure.to_le_bytes());
}
pub(super) fn key(owner: &Owner) -> NodeKey {
    NodeKey {
        repository: owner.repository.id,
        revision: Address::zero_context_hash(owner.state.revision()),
        node: owner.source_root,
        exposure: owner.exposure,
    }
}
fn access(owner: &Owner) -> Result<(), Error> {
    let mut route = owner.access.as_deref();
    while let Some(gate) = route {
        if !gate.repository.can_read_link(gate.target) {
            return Err(Error::PermissionDenied);
        }
        route = gate.parent.as_deref();
    }
    Ok(())
}
/// A persisted child key cannot escape the source subtree of its exposure.
pub(super) async fn scoped_node(owner: &Owner, id: NodeID) -> Result<RevisionNode, Error> {
    access(owner)?;
    let node = owner
        .state
        .node(owner.repository.clone(), id)
        .await
        .map_err(state_error)?;
    if owner.source_root == ROOT_NODE || id == owner.source_root {
        access(owner)?;
        return Ok(node);
    }
    let mut ancestor = node.parent;
    let mut seen = BTreeSet::from([id]);
    for _ in 0..MAX_EXPOSURES {
        if ancestor == owner.source_root {
            access(owner)?;
            return Ok(node);
        }
        if ancestor == ROOT_NODE || !seen.insert(ancestor) {
            return Err(Error::Stale);
        }
        ancestor = owner
            .state
            .node(owner.repository.clone(), ancestor)
            .await
            .map_err(state_error)?
            .parent;
    }
    Err(Error::ResourceLimit)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Record {
    Origin {
        parent: NodeKey,
        child: NodeKey,
    },
    View {
        scope: NodeKey,
        head: crate::core::ScopeHead,
    },
    Binding {
        source: NodeKey,
        child: NodeKey,
    },
    Floor(u64),
}
impl Record {
    pub(super) fn size(&self) -> usize {
        match self {
            Self::Origin { .. } | Self::Binding { .. } => 1 + 2 * KEY_SIZE,
            Self::View { head, .. } => 1 + KEY_SIZE + 48 + 1 + usize::from(head.root.is_some()) * 4,
            Self::Floor(_) => 9,
        }
    }
}
fn reserve_records(owners: &Owners, extra: &[Record]) -> Result<(), Error> {
    if owners
        .provenance
        .len()
        .checked_add(extra.len())
        .is_none_or(|count| count > MAX_RECORDS)
    {
        return Err(Error::ResourceLimit);
    }
    let length = extra.iter().try_fold(
        owners
            .provenance_bytes
            .checked_add(HEADER_SIZE)
            .ok_or(Error::ResourceLimit)?,
        |size, record| size.checked_add(record.size()).ok_or(Error::ResourceLimit),
    )?;
    if length > crate::core::MAX_BACKEND_STATE_SIZE {
        return Err(Error::ResourceLimit);
    }
    Ok(())
}
pub(super) fn encoded_size(owners: &Owners) -> Result<usize, Error> {
    owners
        .provenance_bytes
        .checked_add(HEADER_SIZE)
        .ok_or(Error::ResourceLimit)
}
pub(super) fn record_origin(
    owners: &mut Owners,
    parent: NodeKey,
    child: NodeKey,
) -> Result<(), Error> {
    if child.exposure != owners.next {
        return Err(Error::Stale);
    }
    let next = child.exposure.checked_add(1).ok_or(Error::ResourceLimit)?;
    if next == u64::MAX {
        return Err(Error::ResourceLimit);
    }
    let records = [Record::Origin { parent, child }, Record::Floor(next)];
    reserve_records(owners, &records)?;
    owners.provenance_bytes += records.iter().map(Record::size).sum::<usize>();
    owners.provenance.extend(records);
    owners.next = next;
    Ok(())
}
pub(super) fn capture(backend: &RevisionBackend) -> Result<Vec<u8>, Error> {
    let owners = backend.owners.lock().map_err(|_poison| Error::Io)?;
    if !owners.provenance_complete {
        return Err(Error::Unsupported);
    }
    if owners.by_exposure.len() > MAX_EXPOSURES || owners.views.len() > MAX_RETAINED_VIEWS {
        return Err(Error::ResourceLimit);
    }
    reserve_records(&owners, &[])?;
    for owner in owners.by_exposure.values() {
        access(owner)?;
    }
    encode(backend.root.key, &owners.provenance)
}
pub(super) fn encode(root: NodeKey, records: &[Record]) -> Result<Vec<u8>, Error> {
    let length = records.iter().try_fold(HEADER_SIZE, |bytes, record| {
        bytes.checked_add(record.size()).ok_or(Error::ResourceLimit)
    })?;
    if records.len() > MAX_RECORDS || length > crate::core::MAX_BACKEND_STATE_SIZE {
        return Err(Error::ResourceLimit);
    }
    let mut bytes = Vec::with_capacity(length);
    bytes.extend_from_slice(MAGIC);
    encode_key(&mut bytes, root);
    for record in records {
        match record {
            Record::Origin { parent, child } => {
                bytes.push(1);
                encode_key(&mut bytes, *parent);
                encode_key(&mut bytes, *child);
            }
            Record::View { scope, head } => {
                bytes.push(2);
                encode_key(&mut bytes, *scope);
                bytes.extend_from_slice(head.revision.context.data());
                bytes.extend_from_slice(head.revision.hash.data());
                bytes.push(u8::from(head.root.is_some()));
                if let Some(root) = head.root {
                    bytes.extend_from_slice(&root.to_le_bytes());
                }
            }
            Record::Binding { source, child } => {
                bytes.push(3);
                encode_key(&mut bytes, *source);
                encode_key(&mut bytes, *child);
            }
            Record::Floor(next) => {
                bytes.push(4);
                bytes.extend_from_slice(&next.to_le_bytes());
            }
        }
    }
    Ok(bytes)
}
struct Input<'a>(&'a [u8]);
impl Input<'_> {
    fn array<const N: usize>(&mut self) -> Result<[u8; N], Error> {
        let (value, remaining) = self.0.split_at_checked(N).ok_or(Error::InvalidInput)?;
        self.0 = remaining;
        value.try_into().map_err(|_invalid| Error::InvalidInput)
    }
    fn u64(&mut self) -> Result<u64, Error> {
        Ok(u64::from_le_bytes(self.array()?))
    }
    fn u32(&mut self) -> Result<u32, Error> {
        Ok(u32::from_le_bytes(self.array()?))
    }
    fn address(&mut self) -> Result<Address, Error> {
        Ok(Address {
            context: self.array::<16>()?.into(),
            hash: self.array::<32>()?.into(),
        })
    }
    fn key(&mut self) -> Result<NodeKey, Error> {
        Ok(NodeKey {
            repository: self.array::<16>()?.into(),
            revision: self.address()?,
            node: self.u32()?,
            exposure: self.u64()?,
        })
    }
}
#[cfg(test)]
fn decode(bytes: &[u8], root: NodeKey) -> Result<Vec<Record>, Error> {
    decode_bounded(bytes, root, None)
}
fn decode_bounded(
    bytes: &[u8],
    root: NodeKey,
    mut budget: Option<&mut crate::transition_prepare::Budget>,
) -> Result<Vec<Record>, Error> {
    if bytes.len() < HEADER_SIZE + 9
        || bytes.len() > crate::core::MAX_BACKEND_STATE_SIZE
        || &bytes[..8] != MAGIC
    {
        return Err(Error::InvalidInput);
    }
    if let Some(budget) = &mut budget {
        budget.paths(bytes.len())?;
    }
    let mut input = Input(&bytes[8..]);
    if input.key()? != root {
        return Err(Error::Stale);
    }
    let mut records = Vec::new();
    while !input.0.is_empty() {
        if records.len() >= MAX_RECORDS {
            return Err(Error::ResourceLimit);
        }
        let record = match input.array::<1>()?[0] {
            1 => Record::Origin {
                parent: input.key()?,
                child: input.key()?,
            },
            2 => {
                let scope = input.key()?;
                let revision = input.address()?;
                let root = match input.array::<1>()?[0] {
                    0 => None,
                    1 => Some(input.u32()?),
                    _ => return Err(Error::InvalidInput),
                };
                Record::View {
                    scope,
                    head: crate::core::ScopeHead { revision, root },
                }
            }
            3 => Record::Binding {
                source: input.key()?,
                child: input.key()?,
            },
            4 => Record::Floor(input.u64()?),
            _ => return Err(Error::InvalidInput),
        };
        if let Some(budget) = &mut budget {
            budget.node()?;
        }
        records.push(record);
    }
    if records.first() != Some(&Record::Floor(1)) {
        return Err(Error::InvalidInput);
    }
    // Validate the full record graph and allocator ordering before any store IO.
    if let Some(budget) = &mut budget {
        budget.node()?;
    }
    let mut origins = BTreeMap::from([(0, root)]);
    let mut routes = BTreeSet::new();
    let mut views = BTreeSet::new();
    let mut bindings = BTreeSet::new();
    let mut next = 1u64;
    let mut pending_floor = false;
    for (index, record) in records.iter().enumerate() {
        if let Some(budget) = &mut budget {
            budget.node()?;
            budget.node()?;
        }
        if pending_floor && !matches!(record, Record::Floor(_)) {
            return Err(Error::InvalidInput);
        }
        match record {
            Record::Origin { parent, child } => {
                let origin = origins.get(&parent.exposure).ok_or(Error::InvalidInput)?;
                if parent.repository != origin.repository
                    || parent.revision != origin.revision
                    || child.exposure != next
                    || child.exposure <= parent.exposure
                    || origins.len() >= MAX_EXPOSURES
                    || !routes.insert(*parent)
                    || origins.insert(child.exposure, *child).is_some()
                {
                    return Err(Error::InvalidInput);
                }
                next = next.checked_add(1).ok_or(Error::ResourceLimit)?;
                pending_floor = true;
            }
            Record::Floor(floor) => {
                if *floor < next
                    || *floor == u64::MAX
                    || (index > 0 && !pending_floor && *floor == next)
                {
                    return Err(Error::InvalidInput);
                }
                next = *floor;
                pending_floor = false;
            }
            Record::View { scope, head } => {
                if views.len() >= MAX_RETAINED_VIEWS {
                    return Err(Error::ResourceLimit);
                }
                if origins.get(&scope.exposure) != Some(scope)
                    || scope.revision == head.revision
                    || scope.revision.context != head.revision.context
                    || !views.insert((scope.exposure, head.revision))
                {
                    return Err(Error::InvalidInput);
                }
            }
            Record::Binding { source, child } => {
                let parent = origins.get(&source.exposure).ok_or(Error::InvalidInput)?;
                if child.exposure <= source.exposure
                    || source.repository != parent.repository
                    || !views.contains(&(source.exposure, source.revision))
                    || origins.get(&child.exposure) != Some(child)
                    || !bindings.insert(*source)
                {
                    return Err(Error::InvalidInput);
                }
            }
        }
    }
    if pending_floor {
        return Err(Error::InvalidInput);
    }
    Ok(records)
}
pub(super) async fn prepare(
    backend: &RevisionBackend,
    bytes: &[u8],
) -> Result<Option<Arc<dyn Backend>>, Error> {
    let mut budget =
        crate::transition_prepare::Budget::new(crate::transition_prepare::Limits::default())?;
    Ok(
        prepare_bounded(backend, bytes, &mut budget, AllocationState::Active)
            .await?
            .map(|backend| backend as Arc<dyn Backend>),
    )
}
pub(super) async fn prepare_bounded(
    backend: &RevisionBackend,
    bytes: &[u8],
    budget: &mut crate::transition_prepare::Budget,
    allocation: AllocationState,
) -> Result<Option<Arc<RevisionBackend>>, Error> {
    let records = decode_bounded(bytes, backend.root.key, Some(budget))?;
    let root = backend
        .owners
        .lock()
        .map_err(|_poison| Error::Io)?
        .by_exposure
        .get(&0)
        .ok_or(Error::Io)?
        .clone();
    let mut owners = Owners {
        by_exposure: BTreeMap::from([(0, root)]),
        links: BTreeMap::new(),
        views: BTreeMap::new(),
        bindings: BTreeMap::new(),
        provenance: Vec::new(),
        provenance_bytes: 0,
        next: 1,
        allocation,
        provenance_complete: true,
    };
    budget.node()?;
    for record in &records {
        budget.node()?;
        match record {
            Record::Floor(next) => {
                owners.next = *next;
            }
            Record::Origin {
                parent: source_key,
                child: expected,
            } => {
                let parent = owners
                    .by_exposure
                    .get(&source_key.exposure)
                    .ok_or(Error::InvalidInput)?
                    .clone();
                if parent.link_depth >= MAX_LINK_DEPTH {
                    return Err(Error::ResourceLimit);
                }
                let source = scoped_node(&parent, source_key.node).await?;
                if !source.is_link() {
                    return Err(Error::Stale);
                }
                let link = source.linked_node();
                if link.repository != expected.repository
                    || Address::zero_context_hash(link.revision) != expected.revision
                    || link.node != expected.node
                {
                    return Err(Error::Stale);
                }
                if !parent.repository.can_read_link(link.repository) {
                    return Err(Error::PermissionDenied);
                }
                let repository = parent.repository.to_link_context(link.repository).await;
                let state = State::deserialize(repository.clone(), link.revision)
                    .await
                    .map_err(state_error)?;
                state
                    .node(repository.clone(), link.node)
                    .await
                    .map_err(state_error)?;
                let child = Owner {
                    source_root: link.node,
                    link_depth: parent.link_depth + 1,
                    access: Some(Arc::new(Access {
                        parent: parent.access.clone(),
                        repository: parent.repository.clone(),
                        target: link.repository,
                    })),
                    repository,
                    state,
                    exposure: expected.exposure,
                };
                owners
                    .links
                    .insert((source_key.exposure, source_key.node), expected.exposure);
                owners.by_exposure.insert(expected.exposure, child);
                owners.next = expected
                    .exposure
                    .checked_add(1)
                    .ok_or(Error::ResourceLimit)?;
            }
            Record::View { scope, head } => {
                let origin = owners
                    .by_exposure
                    .get(&scope.exposure)
                    .ok_or(Error::InvalidInput)?;
                let view = super::transition::restore_view(origin, head, budget).await?;
                owners.views.insert((scope.exposure, head.revision), view);
            }
            Record::Binding { source, child } => {
                let origin = owners
                    .by_exposure
                    .get(&source.exposure)
                    .ok_or(Error::InvalidInput)?;
                let view = owners
                    .views
                    .get(&(source.exposure, source.revision))
                    .ok_or(Error::InvalidInput)?;
                let child_owner = owners
                    .by_exposure
                    .get(&child.exposure)
                    .ok_or(Error::InvalidInput)?;
                super::transition::verify_binding(
                    origin,
                    view,
                    source.node,
                    child_owner,
                    &owners.links,
                    budget,
                )
                .await?;
                owners.bindings.insert(
                    (source.exposure, source.revision, source.node),
                    child.exposure,
                );
            }
        }
        owners.provenance_bytes = owners
            .provenance_bytes
            .checked_add(record.size())
            .ok_or(Error::ResourceLimit)?;
        owners.provenance.push(record.clone());
        tokio::task::yield_now().await;
    }
    for owner in owners.by_exposure.values() {
        access(owner)?;
    }
    for ((exposure, revision), view) in &owners.views {
        let origin = owners
            .by_exposure
            .get(exposure)
            .ok_or(Error::InvalidInput)?;
        for link in super::transition::view_links(origin, view, budget).await? {
            if !owners.bindings.contains_key(&(*exposure, *revision, link)) {
                return Err(Error::Unsupported);
            }
        }
    }
    Ok(Some(Arc::new(RevisionBackend {
        selection_identity: Arc::new(()),
        publication: Arc::new(crate::publication_ownership::Slot::default()),
        mutation_gate: backend.mutation_gate.clone(),
        execution: backend.execution.clone(),
        owners: Mutex::new(owners),
        root: backend.root.clone(),
        digest_index: backend.digest_index.clone(),
    })))
}

#[cfg(test)]
impl RevisionBackend {
    pub(crate) fn test_view_provenance(
        &self,
        views: &[(NodeKey, crate::core::ScopeHead)],
        bindings: &[(NodeKey, NodeKey)],
        floor: Option<u64>,
    ) -> Result<Vec<u8>, Error> {
        let owners = self.owners.lock().map_err(|_poison| Error::Io)?;
        let mut records = owners.provenance.clone();
        records.extend(views.iter().map(|(scope, head)| Record::View {
            scope: *scope,
            head: head.clone(),
        }));
        records.extend(bindings.iter().map(|(source, child)| Record::Binding {
            source: *source,
            child: *child,
        }));
        if let Some(floor) = floor {
            records.push(Record::Floor(floor));
        }
        encode(self.root.key, &records)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn root() -> NodeKey {
        NodeKey {
            repository: [8; 16].into(),
            revision: Address::zero_context_hash(Hash::from_u64(8)),
            node: 0,
            exposure: 0,
        }
    }
    #[test]
    fn typed_log_rejects_old_versions_truncation_and_bad_order_before_rebuild() {
        let root = root();
        let child = NodeKey {
            exposure: 1,
            ..root
        };
        let origin = Record::Origin {
            parent: NodeKey { node: 2, ..root },
            child,
        };
        let valid = vec![Record::Floor(1), origin.clone(), Record::Floor(2)];
        let bytes = encode(root, &valid).unwrap();
        assert_eq!(decode(&bytes, root).unwrap(), valid);
        for end in HEADER_SIZE + 10..bytes.len() {
            assert!(decode(&bytes[..end], root).is_err());
        }
        let mut old = bytes.clone();
        old[..8].copy_from_slice(b"LORERT01");
        assert!(decode(&old, root).is_err());
        for records in [
            vec![origin.clone(), Record::Floor(2)],
            vec![Record::Floor(1), origin.clone()],
            vec![Record::Floor(1), Record::Floor(0)],
            vec![Record::Floor(1), Record::Floor(1)],
            vec![Record::Floor(1), Record::Floor(4), Record::Floor(3)],
            vec![Record::Floor(1), Record::Floor(u64::MAX)],
        ] {
            assert!(decode(&encode(root, &records).unwrap(), root).is_err());
        }
        let head = crate::core::ScopeHead {
            revision: Address::zero_context_hash(Hash::from_u64(9)),
            root: Some(0),
        };
        let view = Record::View { scope: root, head };
        let duplicate = encode(root, &[Record::Floor(1), view.clone(), view]).unwrap();
        assert!(decode(&duplicate, root).is_err());
        let early_binding = encode(
            root,
            &[
                Record::Floor(1),
                Record::Binding {
                    source: root,
                    child,
                },
            ],
        )
        .unwrap();
        assert!(decode(&early_binding, root).is_err());
    }
    #[test]
    fn allocator_floors_burn_ids_without_reuse_or_overflow() {
        let root = root();
        let records = vec![
            Record::Floor(1),
            Record::Floor(40),
            Record::Origin {
                parent: NodeKey { node: 1, ..root },
                child: NodeKey {
                    exposure: 40,
                    ..root
                },
            },
            Record::Floor(41),
        ];
        assert_eq!(
            decode(&encode(root, &records).unwrap(), root).unwrap(),
            records
        );
        let bad = vec![
            Record::Floor(1),
            Record::Floor(u64::MAX - 1),
            Record::Origin {
                parent: root,
                child: NodeKey {
                    exposure: u64::MAX - 1,
                    ..root
                },
            },
            Record::Floor(u64::MAX),
        ];
        assert!(decode(&encode(root, &bad).unwrap(), root).is_err());
        let mut owners = Owners {
            by_exposure: BTreeMap::new(),
            links: BTreeMap::new(),
            views: BTreeMap::new(),
            bindings: BTreeMap::new(),
            provenance: vec![Record::Floor(u64::MAX - 1)],
            provenance_bytes: 9,
            next: u64::MAX - 1,
            allocation: AllocationState::Active,
            provenance_complete: true,
        };
        for next in [u64::MAX - 1, u64::MAX] {
            owners.next = next;
            assert!(matches!(
                record_origin(
                    &mut owners,
                    root,
                    NodeKey {
                        exposure: next,
                        ..root
                    }
                ),
                Err(Error::ResourceLimit)
            ));
            assert_eq!(owners.next, next);
            assert_eq!(owners.provenance_bytes, 9);
            assert_eq!(owners.provenance.len(), 1);
        }
    }
    #[test]
    fn retained_view_count_is_rejected_before_any_state_rebuild() {
        let root = root();
        let mut records = vec![Record::Floor(1)];
        records.extend((0..=MAX_RETAINED_VIEWS).map(|index| Record::View {
            scope: root,
            head: crate::core::ScopeHead {
                revision: Address::zero_context_hash(Hash::from_u64(index as u64 + 100)),
                root: Some(0),
            },
        }));
        assert!(matches!(
            decode(&encode(root, &records).unwrap(), root),
            Err(Error::ResourceLimit)
        ));
    }
}
