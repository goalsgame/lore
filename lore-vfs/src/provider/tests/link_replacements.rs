// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use super::*;

#[tokio::test]
async fn unsupported_link_pin_changes_preserve_staged_and_live_views() {
    LORE_CONTEXT
        .scope(execution(), async {
            let fixture = fixture_variants(Default::default(), true, false, 1).await;
            let repository = fixture.repository.clone();
            let (_, staged, _) = State::deserialize_current_and_staged(repository.clone())
                .await
                .unwrap();
            let state = staged.unwrap();
            let mount = state
                .find_node_link(repository.clone(), "vendor")
                .await
                .unwrap();
            let link_node = state.node(repository.clone(), mount.node).await.unwrap();
            let child = repository
                .to_link_context(link_node.address.context.into())
                .await;
            let old_pin = link_node.address.hash;
            let incoming = State::deserialize(child.clone(), old_pin).await.unwrap();
            let mut metadata = lore_revision::metadata::Metadata::new();
            metadata
                .set_string("message", "metadata-only link pin")
                .unwrap();
            incoming.set_metadata_hash(metadata.serialize(child.clone()).await.unwrap());
            incoming.set_parent_self(old_pin);
            incoming.set_revision_number(incoming.revision_number() + 1);
            let new_pin = incoming
                .serialize(child.clone(), &fixture.token)
                .await
                .unwrap();
            assert_ne!(new_pin, old_pin);
            let live = fixture
                .provider
                .namespace()
                .lookup(1, b"vendor")
                .await
                .unwrap();
            assert_eq!(live.authority.revision.hash, old_pin);
            let before = lore_revision::instance::strict_anchors::read(&repository)
                .await
                .unwrap();
            let error = lore_revision::link::stage_link_pin(
                repository.clone(),
                &state,
                &child,
                "vendor".parse().unwrap(),
                link_node,
                old_pin,
                new_pin,
                [102; 16].into(),
                lore_revision::link::LinkPinRealize::WorkingTree,
            )
            .await
            .unwrap_err()
            .to_string();
            assert!(error.contains("Revision replacement and merge-state"));
            let error = lore_revision::link::realize_link_pin_change(
                repository.clone(),
                child.clone(),
                "vendor".parse().unwrap(),
                old_pin,
                new_pin,
                mount.node,
            )
            .await
            .unwrap_err()
            .to_string();
            assert!(error.contains("Revision replacement and merge-state"));
            // State-only staging is used for metadata composition without replacing the held view.
            lore_revision::link::stage_link_pin(
                repository.clone(),
                &state,
                &child,
                "vendor".parse().unwrap(),
                link_node,
                old_pin,
                new_pin,
                [102; 16].into(),
                lore_revision::link::LinkPinRealize::StateOnly,
            )
            .await
            .unwrap();
            let after = lore_revision::instance::strict_anchors::read(&repository)
                .await
                .unwrap();
            assert_eq!(before, after);
            assert_eq!(
                fixture
                    .provider
                    .namespace()
                    .lookup(1, b"vendor")
                    .await
                    .unwrap()
                    .authority
                    .revision
                    .hash,
                old_pin
            );
            fixture.provider.close_admission();
            fixture.provider.drain_exports().await.unwrap();
        })
        .await;
}

#[tokio::test]
async fn unsupported_link_and_file_resets_preserve_metadata() {
    LORE_CONTEXT
        .scope(execution(), async {
            let fixture = fixture_variants(Default::default(), true, false, 1).await;
            let repository = fixture.repository.clone();
            std::fs::write(fixture.root.join(".lore/sentinel"), b"keep").unwrap();
            let before = super::unsupported_transitions::mutable_snapshot(&repository).await;
            for command in 0..5 {
                let error = match command {
                    0 => lore_revision::link::add::add(
                        repository.clone(),
                        &fixture.token,
                        "added".parse().unwrap(),
                        "invalid://".into(),
                        "".parse().unwrap(),
                        None,
                        false,
                    )
                    .await
                    .unwrap_err()
                    .to_string(),
                    1 => lore_revision::link::update::update_boxed(
                        repository.clone(),
                        &fixture.token,
                        "vendor".parse().unwrap(),
                        None,
                    )
                    .await
                    .unwrap_err()
                    .to_string(),
                    2 => lore_revision::link::remove::remove_boxed(
                        repository.clone(),
                        &fixture.token,
                        "vendor".parse().unwrap(),
                    )
                    .await
                    .unwrap_err()
                    .to_string(),
                    3 => lore_revision::file::reset::reset(
                        repository.clone(),
                        &fixture.token,
                        Default::default(),
                        Default::default(),
                        Default::default(),
                    )
                    .await
                    .unwrap_err()
                    .to_string(),
                    _ => lore_revision::file::reset::reset_to_last_merged(
                        repository.clone(),
                        Default::default(),
                        Default::default(),
                        Default::default(),
                        Default::default(),
                    )
                    .await
                    .unwrap_err()
                    .to_string(),
                };
                assert!(
                    error.contains("Revision replacement and merge-state"),
                    "{command}: {error}"
                );
                assert_eq!(
                    before,
                    super::unsupported_transitions::mutable_snapshot(&repository).await
                );
                assert_eq!(
                    std::fs::read(fixture.root.join(".lore/sentinel")).unwrap(),
                    b"keep"
                );
            }
            fixture.provider.close_admission();
            fixture.provider.drain_exports().await.unwrap();
        })
        .await;
}

#[tokio::test]
async fn restricted_branch_reset_preserves_current_and_allows_unrelated_branch() {
    let context = Arc::new(ExecutionContext::new_client_with_user_id(
        LoreGlobalArgs {
            offline: 1,
            ..Default::default()
        },
        EventDispatcher::no_dispatch(),
        "provider-test".into(),
    ));
    LORE_CONTEXT
        .scope(context, async {
            let fixture = fixture(Default::default()).await;
            let repository = fixture.repository.clone();
            let anchors = lore_revision::instance::strict_anchors::read(&repository)
                .await
                .unwrap();
            let before = super::unsupported_transitions::mutable_snapshot(&repository).await;
            for name in [String::new(), anchors.branch.to_string()] {
                let error = lore_revision::branch::reset::reset(
                    repository.clone(),
                    &fixture.token,
                    name,
                    "invalid-revision".into(),
                )
                .await
                .unwrap_err()
                .to_string();
                assert!(
                    error.contains("Revision replacement and merge-state"),
                    "{error}"
                );
                assert_eq!(
                    before,
                    super::unsupported_transitions::mutable_snapshot(&repository).await
                );
            }
            let other = [164; 16].into();
            let mut metadata = lore_revision::metadata::Metadata::new();
            metadata
                .set_string(lore_revision::branch::NAME, "other")
                .unwrap();
            let hash = metadata.serialize(repository.clone()).await.unwrap();
            lore_revision::branch::mutable_store_metadata(repository.clone(), other, hash)
                .await
                .unwrap();
            let mut metadata = lore_revision::metadata::Metadata::new();
            metadata.set_branch(other).unwrap();
            let state = State::new();
            state.set_metadata_hash(metadata.serialize(repository.clone()).await.unwrap());
            state.set_revision_number(1);
            let target = state
                .serialize(repository.clone(), &fixture.token)
                .await
                .unwrap();
            lore_revision::branch::reset::reset(
                repository.clone(),
                &fixture.token,
                other.to_string(),
                target.to_string(),
            )
            .await
            .unwrap();
            assert_eq!(
                lore_revision::branch::load_latest(repository.clone(), other)
                    .await
                    .unwrap(),
                target
            );
            assert_eq!(
                anchors,
                lore_revision::instance::strict_anchors::read(&repository)
                    .await
                    .unwrap()
            );
            fixture.provider.close_admission();
            fixture.provider.drain_exports().await.unwrap();
        })
        .await;
}

#[tokio::test]
async fn branch_reset_refuses_unknown_current_identity_without_legacy_migration() {
    LORE_CONTEXT
        .scope(execution(), async {
            let fixture = fixture(Default::default()).await;
            let repository = fixture.repository.clone();
            let legacy = fixture.root.join(".lore/current");
            let mut bytes = [0u8; 48];
            bytes[32..].fill(92);
            std::fs::write(&legacy, bytes).unwrap();
            let (key, kind) = lore_revision::instance::anchor_key(
                repository.salt(),
                lore_revision::instance::ANCHOR_CURRENT_BRANCH,
                repository.instance_id,
            );
            for value in [Hash::default(), Hash::from([9; 32])] {
                repository
                    .write_mutable_store(&fixture.token)
                    .store(repository.id, key, value, kind)
                    .await
                    .unwrap();
                let before = super::unsupported_transitions::mutable_snapshot(&repository).await;
                let error = lore_revision::branch::reset::reset(
                    repository.clone(),
                    &fixture.token,
                    "other".into(),
                    "invalid".into(),
                )
                .await
                .unwrap_err()
                .to_string();
                assert!(
                    error.contains("Cannot prove current instance branch"),
                    "{error}"
                );
                assert_eq!(
                    before,
                    super::unsupported_transitions::mutable_snapshot(&repository).await
                );
                assert_eq!(std::fs::read(&legacy).unwrap(), bytes);
            }
            fixture.provider.close_admission();
            fixture.provider.drain_exports().await.unwrap();
        })
        .await;
}

#[tokio::test]
async fn staged_link_unstage_refuses_before_metadata_or_host_changes() {
    LORE_CONTEXT
        .scope(execution(), async {
            let fixture = fixture_variants(Default::default(), true, false, 1).await;
            let repository = fixture.repository.clone();
            write(&fixture, "ordinary", b"staged file").await;
            stage(&fixture, "ordinary").await;
            let (_, staged, _) = State::deserialize_current_and_staged(repository.clone())
                .await
                .unwrap();
            let staged = staged.unwrap();
            let ordinary = staged
                .find_node_link(repository.clone(), "ordinary")
                .await
                .unwrap();
            assert!(
                staged
                    .node(repository.clone(), ordinary.node)
                    .await
                    .unwrap()
                    .is_staged()
            );
            let host_link = fixture.root.join("vendor");
            std::fs::create_dir(&host_link).unwrap();
            std::fs::write(host_link.join("sentinel"), b"keep").unwrap();
            let before = super::unsupported_transitions::mutable_snapshot(&repository).await;
            for selected in [
                vec![host_link.clone()],
                vec![fixture.root.join("VeNdOr")],
                vec![fixture.root.clone()],
                vec![fixture.root.join("ordinary"), host_link.clone()],
            ] {
                let paths = selected
                    .iter()
                    .map(|path| LoreString::from(path.to_string_lossy().as_ref()))
                    .collect::<Vec<_>>()
                    .into_boxed_slice()
                    .into();
                let error = lore_revision::file::unstage::unstage(
                    repository.clone(),
                    &fixture.token,
                    paths,
                    Default::default(),
                )
                .await
                .unwrap_err()
                .to_string();
                assert!(
                    error.contains("Revision replacement and merge-state"),
                    "{error}"
                );
                assert_eq!(
                    before,
                    super::unsupported_transitions::mutable_snapshot(&repository).await
                );
                assert_eq!(std::fs::read(host_link.join("sentinel")).unwrap(), b"keep");
                assert!(
                    fixture
                        .provider
                        .namespace()
                        .lookup(1, b"vendor")
                        .await
                        .is_ok()
                );
            }
            let root_path = vec![LoreString::from(fixture.root.to_string_lossy().as_ref())]
                .into_boxed_slice()
                .into();
            lore_revision::file::unstage::unstage(
                repository.clone(),
                &fixture.token,
                root_path,
                lore_revision::file::unstage::UnstageOptions { single_node: true },
            )
            .await
            .unwrap();
            assert_eq!(
                before,
                super::unsupported_transitions::mutable_snapshot(&repository).await
            );
            fixture.provider.close_admission();
            fixture.provider.drain_exports().await.unwrap();
        })
        .await;
}

#[tokio::test]
async fn regular_file_unstage_inside_link_remains_supported() {
    LORE_CONTEXT
        .scope(execution(), async {
            let fixture = fixture_variants(Default::default(), true, false, 0).await;
            let repository = fixture.repository.clone();
            let paths = vec![LoreString::from(
                fixture
                    .root
                    .join("vendor/content")
                    .to_string_lossy()
                    .as_ref(),
            )]
            .into_boxed_slice()
            .into();
            lore_revision::file::unstage::unstage(
                repository.clone(),
                &fixture.token,
                paths,
                Default::default(),
            )
            .await
            .unwrap();
            let (_, staged, _) = State::deserialize_current_and_staged(repository.clone())
                .await
                .unwrap();
            let staged = staged.unwrap();
            let mount = staged
                .find_node_link(repository.clone(), "vendor")
                .await
                .unwrap();
            let node = staged.node(repository.clone(), mount.node).await.unwrap();
            assert!(node.is_staged_add());
            let child = repository
                .to_link_context(node.linked_node().repository)
                .await;
            let state = State::deserialize(child.clone(), node.address.hash)
                .await
                .unwrap();
            let file = state
                .find_node_link(child.clone(), "content")
                .await
                .unwrap();
            let file = state.node(child, file.node).await.unwrap();
            assert!(!file.is_staged());
            assert!(file.is_dirty());
            assert!(
                fixture
                    .provider
                    .namespace()
                    .lookup(1, b"vendor")
                    .await
                    .is_ok()
            );
            fixture.provider.close_admission();
            fixture.provider.drain_exports().await.unwrap();
        })
        .await;
}

#[tokio::test]
async fn unsafe_layer_unstage_is_checked_before_parent_file_changes() {
    LORE_CONTEXT
        .scope(execution(), async {
            let fixture = fixture_variants(Default::default(), true, false, 1).await;
            let repository = fixture.repository.clone();
            write(&fixture, "ordinary", b"staged file").await;
            stage(&fixture, "ordinary").await;
            let anchors = lore_revision::instance::strict_anchors::read(&repository)
                .await
                .unwrap();
            let layers = std::collections::BTreeMap::from([(
                "layers",
                vec![lore_revision::layer::Layer {
                    target_path: "layer".into(),
                    source_path: String::new(),
                    repository: repository.id,
                    metadata: None,
                    current: anchors.current,
                    staged: anchors.staged.unwrap(),
                }],
            )]);
            let config = lore_revision::layer::layer_config_path(&repository).unwrap();
            lore_revision::util::config::save(&layers, &config)
                .await
                .unwrap();
            let bytes = std::fs::read(&config).unwrap();
            let before = super::unsupported_transitions::mutable_snapshot(&repository).await;
            let paths = LoreArray::from_vec(vec![
                LoreString::from(&fixture.root.join("ordinary")),
                LoreString::from(&fixture.root.join("layer/vendor")),
            ]);
            let error = lore_revision::file::unstage::unstage(
                repository.clone(),
                &fixture.token,
                paths,
                Default::default(),
            )
            .await
            .unwrap_err()
            .to_string();
            assert!(
                error.contains("Revision replacement and merge-state"),
                "{error}"
            );
            assert_eq!(
                before,
                super::unsupported_transitions::mutable_snapshot(&repository).await
            );
            assert_eq!(std::fs::read(config).unwrap(), bytes);
            fixture.provider.close_admission();
            fixture.provider.drain_exports().await.unwrap();
        })
        .await;
}

#[tokio::test]
async fn staged_link_pin_and_removal_unstage_preserve_anchors_and_held_view() {
    LORE_CONTEXT
        .scope(execution(), async {
            for removal in [false, true] {
                let fixture = fixture_variants(Default::default(), true, false, 1).await;
                let repository = fixture.repository.clone();
                let (_, staged, _) = State::deserialize_current_and_staged(repository.clone())
                    .await
                    .unwrap();
                let mut metadata = lore_revision::metadata::Metadata::new();
                metadata.set_branch([92; 16].into()).unwrap();
                let current = lore_revision::commit::commit_in_memory_revision(
                    repository.clone(),
                    &fixture.token,
                    staged.unwrap(),
                    metadata,
                    Hash::default(),
                    [92; 16].into(),
                )
                .await
                .unwrap();
                lore_revision::instance::store_current_anchor(&repository, current)
                    .await
                    .unwrap();
                let staged = State::deserialize(repository.clone(), current)
                    .await
                    .unwrap();
                let mount = staged
                    .find_node_link(repository.clone(), "vendor")
                    .await
                    .unwrap();
                let node = staged.node(repository.clone(), mount.node).await.unwrap();
                let old_pin = node.address.hash;
                if removal {
                    staged
                        .node_mark_staged(
                            repository.clone(),
                            mount.node,
                            lore_revision::node::NodeFlags::StagedDelete,
                            lore_revision::node::NodeFlags::DirtyDelete,
                        )
                        .await
                        .unwrap();
                    staged
                        .link_remove(
                            repository.clone(),
                            node.linked_node().repository,
                            mount.node,
                        )
                        .await
                        .unwrap();
                } else {
                    let child = repository
                        .to_link_context(node.linked_node().repository)
                        .await;
                    let incoming = State::deserialize(child.clone(), old_pin).await.unwrap();
                    let mut metadata = lore_revision::metadata::Metadata::new();
                    metadata.set_string("message", "new immutable pin").unwrap();
                    incoming.set_metadata_hash(metadata.serialize(child.clone()).await.unwrap());
                    incoming.set_parent_self(old_pin);
                    incoming.set_revision_number(incoming.revision_number() + 1);
                    let pin = incoming
                        .serialize(child.clone(), &fixture.token)
                        .await
                        .unwrap();
                    lore_revision::link::stage_link_pin(
                        repository.clone(),
                        &staged,
                        &child,
                        "vendor".parse().unwrap(),
                        node,
                        old_pin,
                        pin,
                        [102; 16].into(),
                        lore_revision::link::LinkPinRealize::StateOnly,
                    )
                    .await
                    .unwrap();
                }
                staged.set_parent_self(current);
                staged.set_revision_number(0);
                let pin = staged
                    .serialize(repository.clone(), &fixture.token)
                    .await
                    .unwrap();
                lore_revision::instance::store_staged_anchor(&repository, pin)
                    .await
                    .unwrap();
                let host_link = fixture.root.join("vendor");
                std::fs::create_dir(&host_link).unwrap();
                std::fs::write(host_link.join("sentinel"), b"keep").unwrap();
                let before = super::unsupported_transitions::mutable_snapshot(&repository).await;
                let paths = LoreArray::from_vec(vec![LoreString::from(&host_link)]);
                let error = lore_revision::file::unstage::unstage(
                    repository.clone(),
                    &fixture.token,
                    paths,
                    Default::default(),
                )
                .await
                .unwrap_err()
                .to_string();
                assert!(
                    error.contains("Revision replacement and merge-state"),
                    "removal={removal}: {error}"
                );
                assert_eq!(
                    before,
                    super::unsupported_transitions::mutable_snapshot(&repository).await
                );
                assert_eq!(std::fs::read(host_link.join("sentinel")).unwrap(), b"keep");
                assert_eq!(
                    fixture
                        .provider
                        .namespace()
                        .lookup(1, b"vendor")
                        .await
                        .unwrap()
                        .authority
                        .revision
                        .hash,
                    old_pin
                );
                fixture.provider.close_admission();
                fixture.provider.drain_exports().await.unwrap();
            }
        })
        .await;
}
