// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
#![allow(clippy::disallowed_methods)]
use std::collections::BTreeMap;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::fs::symlink;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use lore_base::runtime::LORE_CONTEXT;
use lore_base::test_util::TempDir;
use lore_revision::fs::provider_registry::ProviderRegistry;
use lore_revision::fs::provider_registry::ProviderResolver;
use lore_revision::instance::InstanceId;
use lore_revision::interface::LoreGlobalArgs;
use lore_revision::repository::CONFIG;
use lore_revision::repository::ID;
use lore_revision::repository::INSTANCE;
use lore_revision::repository::RepositoryConfig;
use lore_revision::repository::VfsConfig;
use lore_revision::repository::VfsType;
use lore_revision::repository::clone::CloneOptions;

use crate::fs::filesystem_provider::TestFilesystemProvider;
use crate::fs::filesystem_provider::test_execution_with;

fn snapshot(path: &Path) -> BTreeMap<String, Vec<u8>> {
    std::fs::read_dir(path)
        .unwrap()
        .map(|entry| {
            let entry = entry.unwrap();
            (
                entry.file_name().into_string().unwrap(),
                std::fs::read(entry.path()).unwrap(),
            )
        })
        .collect()
}
fn private_file(path: &Path, bytes: &[u8]) {
    std::fs::write(path, bytes).unwrap();
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).unwrap();
}
#[tokio::test]
async fn force_clone_refuses_registered_root_nested_and_alias_before_metadata_cleanup() {
    let root = TempDir::new("lore-clone-working-target-");
    let target = root.join("working");
    let nested = target.join("nested");
    let metadata = root.join("external-metadata");
    std::fs::create_dir_all(&nested).unwrap();
    std::fs::create_dir(&metadata).unwrap();
    std::fs::set_permissions(&metadata, std::fs::Permissions::from_mode(0o700)).unwrap();
    let instance = InstanceId::generate();
    let repository = [17; 16];
    let config = RepositoryConfig {
        vfs: Some(VfsConfig {
            vfs_type: VfsType::ExperimentalWorking,
        }),
        ..Default::default()
    };
    private_file(
        &metadata.join(CONFIG),
        toml::to_string(&config).unwrap().as_bytes(),
    );
    private_file(&metadata.join(ID), &repository);
    private_file(&metadata.join(INSTANCE), instance.data());
    private_file(
        &metadata.join("sentinel"),
        b"owned metadata must survive failed clone",
    );
    let registry = ProviderRegistry::new(Default::default());
    let registration = registry.reserve(&target, instance).unwrap();
    registration
        .reserve_metadata(&metadata, repository.into())
        .unwrap();
    let filesystem = Arc::new(TestFilesystemProvider::holding_state());
    registration.publish(filesystem.clone()).unwrap();
    let resolver: Arc<dyn ProviderResolver> = registry;
    let mut context = test_execution_with(LoreGlobalArgs {
        force: 1,
        offline: 1,
        ..Default::default()
    });
    Arc::get_mut(&mut context)
        .unwrap()
        .set_provider_resolver(&resolver);
    let alias = root.join("alias");
    let nested_alias = root.join("nested-alias");
    symlink(&target, &alias).unwrap();
    symlink(&nested, &nested_alias).unwrap();
    let before = snapshot(&metadata);
    Box::pin(LORE_CONTEXT.scope(context, async {
        for path in [&target, &nested, &alias, &nested_alias] {
            let error = Box::pin(lore_revision::repository::clone::clone(
                "http://",
                "",
                path,
                None,
                None,
                None,
                CloneOptions::default(),
            ))
            .await
            .unwrap_err();
            assert!(
                error.to_string().contains("service-owned working instance"),
                "{error}"
            );
            assert_eq!(snapshot(&metadata), before);
            assert_eq!(filesystem.begin_count.load(Ordering::Relaxed), 0);
        }
    }))
    .await;
}

#[tokio::test]
async fn ordinary_force_cleanup_and_swfs_existing_repository_refusal_are_unchanged() {
    let root = TempDir::new("lore-clone-working-controls-");
    let resolver: Arc<dyn ProviderResolver> = ProviderRegistry::new(Default::default());
    let mut context = test_execution_with(LoreGlobalArgs {
        force: 1,
        offline: 1,
        ..Default::default()
    });
    Arc::get_mut(&mut context)
        .unwrap()
        .set_provider_resolver(&resolver);
    Box::pin(LORE_CONTEXT.scope(context, async {
        for (name, vfs) in [("ordinary", VfsType::None), ("swfs", VfsType::Swfs)] {
            let is_swfs = vfs.is_swfs();
            let target = root.join(name);
            let metadata = target.join(".lore");
            std::fs::create_dir_all(&metadata).unwrap();
            let config = RepositoryConfig {
                vfs: Some(VfsConfig { vfs_type: vfs }),
                ..Default::default()
            };
            private_file(
                &metadata.join(CONFIG),
                toml::to_string(&config).unwrap().as_bytes(),
            );
            private_file(&metadata.join("sentinel"), b"ordinary clone controls");
            let before = snapshot(&metadata);
            assert!(
                Box::pin(lore_revision::repository::clone::clone(
                    "http://",
                    "",
                    &target,
                    None,
                    None,
                    None,
                    CloneOptions::default()
                ))
                .await
                .is_err()
            );
            if is_swfs {
                assert_eq!(snapshot(&metadata), before);
            } else {
                assert!(!metadata.exists());
            }
        }
    }))
    .await;
}
