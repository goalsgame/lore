// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use lore_base::runtime::LORE_CONTEXT;
use lore_base::types::Context;

use super::*;
use crate::immutable;
use crate::interface::ExecutionContext;
use crate::interface::LoreGlobalArgs;
use crate::node::NodeFlags;
use crate::relay::EventDispatcher;

struct Fixture {
    immutable: Arc<dyn lore_storage::ImmutableStore>,
    mutable: Arc<dyn lore_storage::MutableStore>,
}
impl Fixture {
    async fn new() -> Self {
        let immutable = lore_storage::local::immutable_store::create(
            None::<&str>,
            lore_storage::local::immutable_store::ImmutableStoreCreateOptions::none(),
            false,
            Default::default(),
        )
        .await
        .unwrap();
        let mutable = lore_storage::local::mutable_store::create(
            None::<&str>,
            Default::default(),
            immutable.clone(),
        )
        .await
        .unwrap();
        Self { immutable, mutable }
    }
    fn repository(&self, id: u8) -> Arc<RepositoryContext> {
        Arc::new(RepositoryContext::new_server_context(
            self.immutable.clone(),
            self.mutable.clone(),
            [id; 16].into(),
        ))
    }
}
fn execution() -> Arc<ExecutionContext> {
    Arc::new(ExecutionContext::new_client_with_user_id(
        LoreGlobalArgs {
            offline: 1,
            ..Default::default()
        },
        EventDispatcher::no_dispatch(),
        "linked-import-test".into(),
    ))
}
async fn directory(
    state: &State,
    repo: &Arc<RepositoryContext>,
    parent: NodeID,
    name: &str,
) -> NodeID {
    state
        .node_add(
            repo.clone(),
            parent,
            Node {
                name_hash: lore_storage::hash::hash_string(name),
                ..Default::default()
            },
            name,
        )
        .await
        .unwrap()
}
async fn link(
    state: &State,
    repo: &Arc<RepositoryContext>,
    parent: NodeID,
    name: &str,
    target: &Arc<RepositoryContext>,
    revision: Hash,
    root: NodeID,
) {
    state
        .node_add(
            repo.clone(),
            parent,
            Node {
                flags: NodeFlags::Link.bits(),
                name_hash: lore_storage::hash::hash_string(name),
                address: Address {
                    hash: revision,
                    context: target.id.into(),
                },
                child: root,
                ..Default::default()
            },
            name,
        )
        .await
        .unwrap();
}
async fn file(
    state: &State,
    repo: &Arc<RepositoryContext>,
    name: &str,
    bytes: &[u8],
) -> (NodeID, Address) {
    let address = immutable::write_borrowed(
        repo.clone(),
        Context::from([9; 16]),
        bytes,
        lore_storage::options::WriteOptions::default().with_fixed_size_chunk(3),
    )
    .await
    .unwrap();
    let id = state
        .node_add(
            repo.clone(),
            ROOT_NODE,
            Node {
                flags: NodeFlags::File.bits(),
                name_hash: lore_storage::hash::hash_string(name),
                address,
                size: bytes.len() as u64,
                ..Default::default()
            },
            name,
        )
        .await
        .unwrap();
    (id, address)
}
async fn freeze(state: &Arc<State>, repo: &Arc<RepositoryContext>) -> Hash {
    state
        .serialize(repo.clone(), repo.try_write_token().unwrap())
        .await
        .unwrap()
}

#[tokio::test]
async fn repeated_subtree_mounts_follow_only_actual_exposed_nodes_and_exact_nested_pins() {
    LORE_CONTEXT
        .scope(execution(), async {
            let fixture = Fixture::new().await;
            let leaf = fixture.repository(1);
            let middle = fixture.repository(2);
            let parent = fixture.repository(3);
            let leaf_state = State::new();
            file(&leaf_state, &leaf, "data", b"pinned bytes").await;
            let leaf_pin = freeze(&leaf_state, &leaf).await;
            let middle_state = State::new();
            let visible = directory(&middle_state, &middle, ROOT_NODE, "visible").await;
            link(
                &middle_state,
                &middle,
                visible,
                "leaf",
                &leaf,
                leaf_pin,
                ROOT_NODE,
            )
            .await;
            // This is outside the exposed subtree and must never be resolved.
            link(
                &middle_state,
                &middle,
                ROOT_NODE,
                "unexposed",
                &fixture.repository(4),
                Hash::from([99; 32]),
                ROOT_NODE,
            )
            .await;
            let middle_pin = freeze(&middle_state, &middle).await;
            let parent_state = State::new();
            for name in ["first", "second"] {
                link(
                    &parent_state,
                    &parent,
                    ROOT_NODE,
                    name,
                    &middle,
                    middle_pin,
                    visible,
                )
                .await;
            }
            let parent_pin = freeze(&parent_state, &parent).await;
            let scopes = collect(
                parent,
                parent_pin,
                ImportLimits {
                    max_scopes: 3,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
            assert_eq!(scopes.scopes.len(), 3);
            assert!(
                scopes
                    .scopes
                    .iter()
                    .any(|scope| scope.repository.id == leaf.id
                        && scope.state.revision() == leaf_pin)
            );
            assert!(
                !scopes
                    .scopes
                    .iter()
                    .any(|scope| scope.repository.id == RepositoryId::from([4; 16]))
            );
            assert!(scopes.proofs.iter().any(|proof| proof.pin
                == Address {
                    hash: leaf_pin,
                    context: leaf.id.into()
                }));
            scopes.revalidate().await.unwrap();
        })
        .await;
}

#[tokio::test]
async fn same_repository_different_pins_are_not_collapsed_to_a_single_revision() {
    LORE_CONTEXT
        .scope(execution(), async {
            let fixture = Fixture::new().await;
            let child = fixture.repository(5);
            let parent = fixture.repository(6);
            let old = State::new();
            file(&old, &child, "data", b"old").await;
            let old_pin = freeze(&old, &child).await;
            let new = State::new();
            file(&new, &child, "data", b"new").await;
            let new_pin = freeze(&new, &child).await;
            let state = State::new();
            link(
                &state, &parent, ROOT_NODE, "old", &child, old_pin, ROOT_NODE,
            )
            .await;
            link(
                &state, &parent, ROOT_NODE, "new", &child, new_pin, ROOT_NODE,
            )
            .await;
            let pin = freeze(&state, &parent).await;
            let scopes = collect(parent, pin, ImportLimits::default()).await.unwrap();
            let actual: BTreeSet<_> = scopes
                .scopes
                .iter()
                .filter(|scope| scope.repository.id == child.id)
                .map(|scope| scope.state.revision())
                .collect();
            assert_eq!(actual, BTreeSet::from([old_pin, new_pin]));
        })
        .await;
}

#[tokio::test]
async fn denied_links_stay_opaque_and_both_authorization_directions_are_rechecked() {
    LORE_CONTEXT
        .scope(execution(), async {
            let fixture = Fixture::new().await;
            let child = fixture.repository(7);
            let parent = fixture.repository(8);
            let state = State::new();
            link(
                &state,
                &parent,
                ROOT_NODE,
                "opaque",
                &child,
                Hash::from([77; 32]),
                ROOT_NODE,
            )
            .await;
            let pin = freeze(&state, &parent).await;
            let allowed = Arc::new(AtomicBool::new(false));
            let check = allowed.clone();
            let parent = Arc::new(
                parent
                    .without_command_locks()
                    .with_link_read(Arc::new(move |_| check.load(Ordering::SeqCst))),
            );
            let scopes = collect(parent, pin, ImportLimits::default()).await.unwrap();
            assert_eq!(
                scopes.scopes.len(),
                1,
                "denied target need not exist locally"
            );
            scopes.revalidate().await.unwrap();
            allowed.store(true, Ordering::SeqCst);
            assert!(scopes.revalidate().await.is_err());

            let state = State::new();
            let parent = fixture.repository(9);
            link(
                &state,
                &parent,
                ROOT_NODE,
                "allowed",
                &child,
                Hash::default(),
                ROOT_NODE,
            )
            .await;
            let pin = freeze(&state, &parent).await;
            let check = allowed.clone();
            let parent = Arc::new(
                parent
                    .without_command_locks()
                    .with_link_read(Arc::new(move |_| check.load(Ordering::SeqCst))),
            );
            let scopes = collect(parent, pin, ImportLimits::default()).await.unwrap();
            assert_eq!(scopes.scopes.len(), 2);
            allowed.store(false, Ordering::SeqCst);
            assert!(scopes.revalidate().await.is_err());
        })
        .await;
}

#[tokio::test]
async fn scope_and_node_limits_fail_closed_before_copying_any_destination_bytes() {
    LORE_CONTEXT
        .scope(execution(), async {
            let fixture = Fixture::new().await;
            let parent = fixture.repository(10);
            let child = fixture.repository(11);
            let state = State::new();
            link(
                &state,
                &parent,
                ROOT_NODE,
                "child",
                &child,
                Hash::from([55; 32]),
                ROOT_NODE,
            )
            .await;
            let pin = freeze(&state, &parent).await;
            for limits in [
                ImportLimits {
                    max_scopes: 1,
                    ..Default::default()
                },
                ImportLimits {
                    max_nodes: 1,
                    ..Default::default()
                },
            ] {
                assert!(matches!(
                    collect(parent.clone(), pin, limits).await,
                    Err(Error::InvalidLimit)
                ));
            }
        })
        .await;
}

#[tokio::test]
async fn imported_child_payloads_keep_their_repository_partition_and_full_address_context() {
    LORE_CONTEXT
        .scope(execution(), async {
            let source = Fixture::new().await;
            let destination = Fixture::new().await;
            let parent = source.repository(12);
            let child = source.repository(13);
            let child_state = State::new();
            let (_, address) = file(
                &child_state,
                &child,
                "payload",
                b"fragmented linked payload",
            )
            .await;
            assert_eq!(address.context, Context::from([9; 16]));
            let child_pin = freeze(&child_state, &child).await;
            let parent_state = State::new();
            link(
                &parent_state,
                &parent,
                ROOT_NODE,
                "child",
                &child,
                child_pin,
                ROOT_NODE,
            )
            .await;
            let parent_pin = freeze(&parent_state, &parent).await;
            let import = super::super::ImmutableImport::new(parent, parent_pin)
                .await
                .unwrap();
            let target = destination.repository(12);
            import.copy_into(target.clone()).await.unwrap();
            let child_target = target.to_link_context(child.id).await;
            let loaded = State::deserialize(child_target.clone(), child_pin)
                .await
                .unwrap();
            let file = loaded
                .find_relative_node_link(child_target.clone(), ROOT_NODE, "payload")
                .await
                .unwrap();
            let node = loaded.node(child_target.clone(), file.node).await.unwrap();
            assert_eq!(node.address, address);
            let options = lore_storage::ReadOptions {
                local: true,
                remote: false,
                cache: false,
                decompress: false,
                ..Default::default()
            };
            let original = immutable::load_raw(child.clone(), address, options)
                .await
                .unwrap();
            assert_eq!(
                immutable::read(child_target.clone(), address, None, options)
                    .await
                    .unwrap()
                    .as_ref(),
                b"fragmented linked payload"
            );
            let copied = immutable::load_raw(child_target, address, options)
                .await
                .unwrap();
            assert_eq!(copied, original);
            let exact = destination
                .immutable
                .clone()
                .get(child.id, address)
                .await
                .unwrap();
            assert_eq!(exact.match_made, lore_storage::StoreMatch::MatchFull);
            assert_eq!(exact.partition, child.id);
            // Local stores may serve identical hashes across partitions. The
            // reported association must still belong to the linked repository.
            let fallback = destination
                .immutable
                .clone()
                .get(target.id, address)
                .await
                .unwrap();
            assert_eq!(fallback.match_made, lore_storage::StoreMatch::MatchHash);
            assert_eq!(fallback.partition, child.id);
        })
        .await;
}

#[tokio::test]
async fn root_without_links_keeps_its_original_expected_revision() {
    LORE_CONTEXT
        .scope(execution(), async {
            let fixture = Fixture::new().await;
            let repository = fixture.repository(14);
            let state = State::new();
            file(&state, &repository, "file", b"root witness").await;
            let revision = freeze(&state, &repository).await;
            let scopes = collect(repository, revision, ImportLimits::default())
                .await
                .unwrap();
            assert!(scopes.proofs.is_empty());
            assert_eq!(scopes.scopes[0].revision, revision);
            scopes.scopes[0].state.set_revision(Hash::from([88; 32]));
            assert!(matches!(
                scopes.revalidate().await,
                Err(Error::InvalidSelection)
            ));
        })
        .await;
}
