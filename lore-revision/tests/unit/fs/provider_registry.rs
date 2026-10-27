// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
// Fixtures construct and corrupt private metadata to exercise fail-closed loading.
#![allow(clippy::disallowed_methods)]

use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use lore_base::test_util::TempDir;
use lore_revision::fs::filesystem_provider::FilesystemProvider;
use lore_revision::fs::filesystem_provider::FsError;
use lore_revision::fs::filesystem_provider::InstanceOperationImpl;
use lore_revision::fs::provider_registry::*;
use lore_revision::instance::InstanceId;
use lore_revision::repository::RepositoryConfig;
use lore_revision::repository::VfsConfig;
use lore_revision::repository::VfsType;

#[cfg(unix)]
struct Provider(bool);
#[cfg(unix)]
#[async_trait]
impl FilesystemProvider for Provider {
    async fn read_metadata_file(
        &self,
        path: &lore_revision::util::path::RelativePath,
        _maximum: usize,
    ) -> std::result::Result<Vec<u8>, FsError> {
        Err(lore_base::error::FileNotFound {
            resource: path.to_string(),
        }
        .into())
    }
    fn holds_state(&self) -> bool {
        self.0
    }
    async fn begin_operation(&self) -> std::result::Result<Arc<InstanceOperationImpl>, FsError> {
        Err(FsError::internal(
            "registry fixture does not run operations",
        ))
    }
}
#[cfg(unix)]
fn provider() -> Arc<dyn FilesystemProvider> {
    Arc::new(Provider(true))
}
fn config(vfs_type: VfsType) -> RepositoryConfig {
    RepositoryConfig {
        vfs: Some(VfsConfig { vfs_type }),
        ..Default::default()
    }
}

#[cfg(unix)]
fn write_metadata(path: &Path, instance: InstanceId, repository: [u8; 16]) {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).unwrap();
    for (name, bytes) in [
        (lore_revision::repository::ID, repository.to_vec()),
        (
            lore_revision::repository::INSTANCE,
            instance.data().to_vec(),
        ),
        (
            lore_revision::repository::CONFIG,
            toml::to_string(&config(VfsType::ExperimentalWorking))
                .unwrap()
                .into_bytes(),
        ),
    ] {
        let file = path.join(name);
        std::fs::write(&file, bytes).unwrap();
        std::fs::set_permissions(file, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
}
#[cfg(unix)]
fn metadata_fixture(instance: InstanceId, repository: [u8; 16]) -> TempDir {
    let directory = TempDir::new("lore-working-external-metadata-");
    write_metadata(directory.path(), instance, repository);
    directory
}

#[cfg(unix)]
#[test]
fn reservations_publish_exact_owner_and_drop_revoke_future_resolution() {
    let directory = TempDir::new("lore-provider-registry-");
    let registry = ProviderRegistry::new(Limits::default());
    let instance = InstanceId::generate();
    let registration = registry.reserve(directory.path(), instance).unwrap();
    assert!(matches!(
        registry.resolve(registration.target(), instance),
        Err(Error::NotReady)
    ));
    assert!(matches!(
        registry.resolve(registration.target(), InstanceId::generate()),
        Err(Error::WrongInstance)
    ));
    assert!(matches!(
        registry.resolve(registration.target(), InstanceId::default()),
        Err(Error::InvalidInstance)
    ));
    assert!(matches!(
        registration.publish(Arc::new(Provider(false))),
        Err(Error::InvalidProvider)
    ));
    let provider = provider();
    assert!(matches!(
        registration.publish(provider.clone()),
        Err(Error::NotReady)
    ));
    let metadata = metadata_fixture(instance, [71; 16]);
    registration
        .reserve_metadata(metadata.path(), [71; 16].into())
        .unwrap();
    registration.publish(provider.clone()).unwrap();
    let resolved = registry.resolve(registration.target(), instance).unwrap();
    assert!(Arc::ptr_eq(&provider, &resolved));
    assert!(matches!(
        registration.publish(provider.clone()),
        Err(Error::AlreadyPublished)
    ));
    let target = registration.target().clone();
    let old_token = registration.test_token();
    drop(registration);
    assert!(matches!(
        registry.resolve(&target, instance),
        Err(Error::Missing)
    ));
    // Already admitted commands retain their provider; future commands need a new owner.
    assert!(Arc::ptr_eq(&provider, &resolved));
    let later = registry.reserve(directory.path(), instance).unwrap();
    assert!(later.test_token() > old_token);
    later
        .reserve_metadata(metadata.path(), [71; 16].into())
        .unwrap();
    later.publish(provider.clone()).unwrap();
    registry.remove(&target, old_token);
    assert!(Arc::ptr_eq(
        &provider,
        &registry.resolve(&target, instance).unwrap()
    ));
}

#[test]
fn reservations_are_bounded_and_reject_overlap_and_missing_targets() {
    let directory = TempDir::new("lore-provider-registry-bounds-");
    let child = directory.path().join("child");
    std::fs::create_dir(&child).unwrap();
    let other = TempDir::new("lore-provider-registry-other-");
    let instance = InstanceId::generate();
    let registry = ProviderRegistry::new(Limits {
        max_registrations: 1,
        ..Limits::default()
    });
    assert!(matches!(
        registry.reserve(directory.path(), InstanceId::default()),
        Err(Error::InvalidInstance)
    ));
    let registration = registry.reserve(&child, instance).unwrap();
    assert!(matches!(
        registry.reserve(&child, instance),
        Err(Error::Overlap)
    ));
    assert!(matches!(
        registry.reserve(directory.path(), instance),
        Err(Error::Overlap)
    ));
    assert!(matches!(
        registry.reserve(other.path(), instance),
        Err(Error::Capacity)
    ));
    drop(registration);
    let registration = registry.reserve(directory.path(), instance).unwrap();
    assert!(matches!(
        registry.reserve(&child, instance),
        Err(Error::Overlap)
    ));
    drop(registration);
    assert!(
        registry
            .reserve(&directory.path().join("missing"), instance)
            .is_err()
    );
    let file = directory.path().join("file");
    std::fs::write(&file, []).unwrap();
    assert!(matches!(
        registry.reserve(&file, instance),
        Err(Error::InvalidTarget)
    ));
    let bounded = ProviderRegistry::new(Limits {
        max_target_bytes: 0,
        ..Limits::default()
    });
    assert!(matches!(
        bounded.reserve(directory.path(), instance),
        Err(Error::InvalidTarget)
    ));
    registry.test_exhaust_tokens();
    assert!(matches!(
        registry.reserve(directory.path(), instance),
        Err(Error::TokenExhausted)
    ));
    assert!(registry.test_is_empty());
}

#[cfg(unix)]
#[test]
fn symlink_aliases_cannot_reserve_the_same_tree_twice() {
    let directory = TempDir::new("lore-provider-registry-alias-");
    let target = directory.path().join("target");
    let alias = directory.path().join("alias");
    std::fs::create_dir(&target).unwrap();
    std::os::unix::fs::symlink(&target, &alias).unwrap();
    let registry = ProviderRegistry::new(Limits::default());
    let instance = InstanceId::generate();
    let registration = registry.reserve(&alias, instance).unwrap();
    let provider = provider();
    let metadata = metadata_fixture(instance, [71; 16]);
    registration
        .reserve_metadata(metadata.path(), [71; 16].into())
        .unwrap();
    registration.publish(provider.clone()).unwrap();
    assert!(matches!(
        registry.reserve(&target, instance),
        Err(Error::Overlap)
    ));
    assert!(Arc::ptr_eq(
        &provider,
        &registry
            .resolve(&CanonicalTarget::resolve(&target).unwrap(), instance)
            .unwrap()
    ));
}

#[test]
fn ordinary_and_swfs_selection_do_not_invoke_experimental_resolution() {
    for config in [
        RepositoryConfig::default(),
        config(VfsType::None),
        config(VfsType::Swfs),
    ] {
        assert!(
            lore_revision::repository::resolve_experimental_provider(
                &config,
                Path::new("missing"),
                Path::new("missing-instance"),
                InstanceId::default(),
                None,
                None
            )
            .unwrap()
            .is_none()
        );
    }
    assert!(!config(VfsType::None).is_swfs());
    assert!(config(VfsType::Swfs).is_swfs());
    assert!(!config(VfsType::ExperimentalWorking).is_swfs());
    let encoded = toml::to_string(&config(VfsType::ExperimentalWorking)).unwrap();
    assert!(
        toml::from_str::<RepositoryConfig>(&encoded)
            .unwrap()
            .is_experimental_working()
    );
}

#[cfg(unix)]
#[tokio::test]
async fn configured_experiment_requires_exact_ready_owner_and_valid_persisted_identity() {
    let directory = TempDir::new("lore-provider-resolution-");
    let instance = InstanceId::generate();
    let metadata = metadata_fixture(instance, [71; 16]);
    let instance_path = metadata.path().join(lore_revision::repository::INSTANCE);
    let registry = ProviderRegistry::new(Limits::default());
    let resolver: Arc<dyn ProviderResolver> = registry.clone();
    let config = config(VfsType::ExperimentalWorking);
    let resolve = || {
        let target = CanonicalTarget::resolve(directory.path()).unwrap();
        let location = registry.locate(&target).ok().flatten();
        lore_revision::repository::resolve_experimental_provider(
            &config,
            directory.path(),
            &instance_path,
            instance,
            Some(&resolver),
            location.as_ref(),
        )
    };
    assert!(resolve().is_err());
    assert!(
        lore_revision::repository::resolve_experimental_provider(
            &config,
            directory.path(),
            &instance_path,
            instance,
            None,
            None
        )
        .is_err()
    );
    let owner = registry.reserve(directory.path(), instance).unwrap();
    assert!(resolve().is_err());
    let provider = provider();
    owner
        .reserve_metadata(metadata.path(), [71; 16].into())
        .unwrap();
    owner.publish(provider.clone()).unwrap();
    assert!(Arc::ptr_eq(&provider, &resolve().unwrap().unwrap()));
    assert!(
        lore_revision::repository::resolve_experimental_provider(
            &config,
            directory.path(),
            &instance_path,
            InstanceId::generate(),
            Some(&resolver),
            None
        )
        .is_err()
    );
    for bytes in [vec![], vec![1; 15], vec![1; 17], vec![0; 16]] {
        std::fs::write(&instance_path, &bytes).unwrap();
        let read = InstanceId::read_from_file(instance_path.clone()).unwrap_or_default();
        assert!(
            lore_revision::repository::resolve_experimental_provider(
                &config,
                directory.path(),
                &instance_path,
                read,
                Some(&resolver),
                None
            )
            .is_err()
        );
        assert_eq!(std::fs::read(&instance_path).unwrap(), bytes);
    }
}

#[cfg(not(unix))]
#[test]
fn configured_experiment_is_explicitly_unsupported() {
    let error = lore_revision::repository::resolve_experimental_provider(
        &config(VfsType::ExperimentalWorking),
        Path::new("missing"),
        Path::new("missing-instance"),
        InstanceId::generate(),
        None,
        None,
    )
    .err()
    .unwrap();
    assert!(error.to_string().contains("unsupported"));
}

#[cfg(unix)]
#[tokio::test]
async fn repository_load_uses_registered_arc_and_never_repairs_missing_experimental_identity() {
    use lore_revision::interface::ExecutionContext;
    use lore_revision::repository::RepositoryAccess;
    use lore_revision::repository::RepositoryWriteToken;
    use lore_revision::repository::{self};
    use lore_revision::runtime::LORE_CONTEXT;
    let directory = TempDir::new("lore-provider-load-");
    let registry = ProviderRegistry::new(Limits::default());
    let mut context = ExecutionContext::default();
    let resolver: Arc<dyn ProviderResolver> = registry.clone();
    context.set_provider_resolver(&resolver);
    LORE_CONTEXT
        .scope(Arc::new(context), async {
            let token = RepositoryWriteToken::acquire(directory.path()).await;
            let original = repository::create_local(
                directory.path(),
                &token,
                [79; 16].into(),
                [80; 16].into(),
                "main".into(),
                Default::default(),
                false,
            )
            .await
            .unwrap();
            let instance = original.instance_id;
            assert!(!original.file_system().holds_state());
            drop(original);
            drop(token);
            let ordinary =
                repository::load_and_connect(directory.path(), RepositoryAccess::ReadOnly)
                    .await
                    .unwrap();
            assert!(!ordinary.file_system().holds_state());
            drop(ordinary);
            let external = TempDir::new("lore-provider-load-external-");
            let dot = external.path().join("metadata");
            std::fs::rename(directory.path().join(repository::DOT_LORE), &dot).unwrap();
            std::fs::write(
                dot.join(repository::CONFIG),
                toml::to_string(&config(VfsType::ExperimentalWorking)).unwrap(),
            )
            .unwrap();
            assert!(
                repository::load_and_connect(directory.path(), RepositoryAccess::ReadOnly)
                    .await
                    .is_err()
            );
            let owner = registry.reserve(directory.path(), instance).unwrap();
            assert!(
                repository::load_and_connect(directory.path(), RepositoryAccess::ReadOnly)
                    .await
                    .is_err()
            );
            let provider = provider();
            write_metadata(&dot, instance, [79; 16]);
            owner.reserve_metadata(&dot, [79; 16].into()).unwrap();
            owner.publish(provider.clone()).unwrap();
            let loaded = repository::load_and_connect(directory.path(), RepositoryAccess::ReadOnly)
                .await
                .unwrap();
            assert!(Arc::ptr_eq(&provider, &loaded.file_system()));
            drop(loaded);
            let nested = directory.path().join("nested");
            std::fs::create_dir(&nested).unwrap();
            let nested_loaded = repository::load_and_connect(&nested, RepositoryAccess::ReadOnly)
                .await
                .unwrap();
            assert_eq!(nested_loaded.require_path().unwrap(), owner.target().path());
            assert!(Arc::ptr_eq(&provider, &nested_loaded.file_system()));
            drop(nested_loaded);
            let direct_read =
                repository::load_and_connect_with_token(&nested, RepositoryAccess::ReadOnly, None)
                    .await
                    .unwrap();
            assert_eq!(direct_read.require_path().unwrap(), owner.target().path());
            drop(direct_read);
            let wrong_token = RepositoryWriteToken::acquire(&nested).await;
            assert!(
                repository::load_and_connect_with_token(
                    &nested,
                    RepositoryAccess::ReadWrite,
                    Some(wrong_token)
                )
                .await
                .is_err()
            );
            let wrong_token = RepositoryWriteToken::acquire(&nested).await;
            assert!(
                repository::load_and_connect_with_token(
                    owner.target().path(),
                    RepositoryAccess::ReadWrite,
                    Some(wrong_token)
                )
                .await
                .is_err()
            );
            let sibling = TempDir::new("lore-working-sibling-token-");
            let wrong = RepositoryWriteToken::acquire(sibling.path()).await;
            assert!(
                repository::load_and_connect_with_token(
                    owner.target().path(),
                    RepositoryAccess::ReadWrite,
                    Some(wrong)
                )
                .await
                .is_err()
            );
            let alias = external.path().join("alias-root");
            std::os::unix::fs::symlink(owner.target().path(), &alias).unwrap();
            let wrong = RepositoryWriteToken::acquire(&alias).await;
            assert!(
                repository::load_and_connect_with_token(
                    &alias,
                    RepositoryAccess::ReadWrite,
                    Some(wrong)
                )
                .await
                .is_err()
            );
            let correct = RepositoryWriteToken::acquire(owner.target().path()).await;
            let accepted = repository::load_and_connect_with_token(
                owner.target().path(),
                RepositoryAccess::ReadWrite,
                Some(correct),
            )
            .await
            .unwrap();
            drop(accepted);
            let correct = RepositoryWriteToken::acquire(owner.target().path()).await;
            let accepted = repository::load_and_connect_with_token(
                &nested,
                RepositoryAccess::ReadWrite,
                Some(correct),
            )
            .await
            .unwrap();
            assert_eq!(accepted.require_path().unwrap(), owner.target().path());
            drop(accepted);
            let correct = RepositoryWriteToken::acquire(owner.target().path()).await;
            let accepted = repository::load_and_connect_with_token(
                &alias,
                RepositoryAccess::ReadWrite,
                Some(correct),
            )
            .await
            .unwrap();
            drop(accepted);
            let writable = repository::load_and_connect(&nested, RepositoryAccess::ReadWrite)
                .await
                .unwrap();
            assert!(writable.try_write_token().is_some());
            drop(writable);
            let instance_path = dot.join(repository::INSTANCE);
            std::fs::remove_file(&instance_path).unwrap();
            assert!(
                repository::load_and_connect(directory.path(), RepositoryAccess::ReadWrite)
                    .await
                    .is_err()
            );
            assert!(
                !instance_path.exists(),
                "missing experimental identity must not be regenerated"
            );
            drop(owner);
        })
        .await;
}

#[cfg(unix)]
#[test]
fn revocation_drops_provider_outside_registry_lock() {
    struct ReentrantProvider {
        registry: std::sync::Weak<ProviderRegistry>,
        target: CanonicalTarget,
        instance: InstanceId,
        dropped: std::sync::mpsc::Sender<()>,
    }
    #[async_trait]
    impl FilesystemProvider for ReentrantProvider {
        fn holds_state(&self) -> bool {
            true
        }
        async fn begin_operation(
            &self,
        ) -> std::result::Result<Arc<InstanceOperationImpl>, FsError> {
            Err(FsError::internal("registry drop fixture"))
        }
    }
    impl Drop for ReentrantProvider {
        fn drop(&mut self) {
            let registry = self.registry.upgrade().unwrap();
            assert!(matches!(
                registry.resolve(&self.target, self.instance),
                Err(Error::Retired)
            ));
            self.dropped.send(()).unwrap();
        }
    }
    let directory = TempDir::new("lore-provider-drop-");
    let registry = ProviderRegistry::new(Limits::default());
    let instance = InstanceId::generate();
    let owner = registry.reserve(directory.path(), instance).unwrap();
    let metadata = metadata_fixture(instance, [71; 16]);
    owner
        .reserve_metadata(metadata.path(), [71; 16].into())
        .unwrap();
    let (dropped, notice) = std::sync::mpsc::channel();
    owner
        .publish(Arc::new(ReentrantProvider {
            registry: Arc::downgrade(&registry),
            target: owner.target().clone(),
            instance,
            dropped,
        }))
        .unwrap();
    let target = owner.target().clone();
    let teardown = std::thread::spawn(move || drop(owner));
    notice
        .recv_timeout(std::time::Duration::from_secs(5))
        .expect("provider destruction deadlocked reentering registry");
    teardown.join().unwrap();
    assert!(matches!(
        registry.resolve(&target, instance),
        Err(Error::Missing)
    ));
}

#[test]
fn captured_execution_does_not_keep_published_registry_alive() {
    struct CapturedProvider(Arc<lore_revision::interface::ExecutionContext>);
    #[async_trait]
    impl FilesystemProvider for CapturedProvider {
        fn holds_state(&self) -> bool {
            self.0.provider_resolver().is_some()
        }
        async fn begin_operation(
            &self,
        ) -> std::result::Result<Arc<InstanceOperationImpl>, FsError> {
            Err(FsError::internal("captured context fixture"))
        }
    }
    let directory = TempDir::new("lore-provider-context-cycle-");
    let registry = ProviderRegistry::new(Limits::default());
    let resolver: Arc<dyn ProviderResolver> = registry.clone();
    let mut execution = lore_revision::interface::ExecutionContext::default();
    execution.set_provider_resolver(&resolver);
    let execution = Arc::new(execution);
    let provider: Arc<dyn FilesystemProvider> = Arc::new(CapturedProvider(execution.clone()));
    // A published owner entry without relying on registration Drop to break it.
    registry.test_insert_unbound_provider(
        directory.path().to_path_buf(),
        InstanceId::generate(),
        provider,
    );
    let weak = Arc::downgrade(&registry);
    drop(resolver);
    drop(registry);
    assert!(weak.upgrade().is_none());
    assert!(execution.provider_resolver().is_none());
}

#[cfg(unix)]
#[test]
fn metadata_reservations_bound_both_trees_before_publication() {
    let root = TempDir::new("lore-location-reservations-");
    let first = root.path().join("first");
    let second = root.path().join("second");
    let metadata = root.path().join("metadata");
    let nested = metadata.join("nested");
    for path in [&first, &second, &metadata] {
        std::fs::create_dir(path).unwrap();
    }
    std::fs::create_dir(&nested).unwrap();
    let registry = ProviderRegistry::new(Limits::default());
    let instance = InstanceId::generate();
    let owner = registry.reserve(&first, instance).unwrap();
    owner.reserve_metadata(&metadata, [71; 16].into()).unwrap();
    assert!(matches!(
        registry.reserve(&nested, instance),
        Err(Error::Overlap)
    ));
    let other = registry.reserve(&second, InstanceId::generate()).unwrap();
    let inside = second.join("metadata");
    std::fs::create_dir(&inside).unwrap();
    assert!(matches!(
        other.reserve_metadata(&inside, [71; 16].into()),
        Err(Error::Overlap)
    ));
    assert!(matches!(
        other.reserve_metadata(&nested, [71; 16].into()),
        Err(Error::Overlap)
    ));
    assert!(matches!(
        owner.reserve_metadata(&metadata, [71; 16].into()),
        Err(Error::MetadataAlreadyReserved)
    ));
    let target = CanonicalTarget::resolve(&first).unwrap();
    assert!(matches!(registry.locate(&target), Err(Error::NotReady)));
    assert!(owner.publish(provider()).is_err());
    assert!(matches!(registry.locate(&target), Err(Error::NotReady)));
    write_metadata(&metadata, instance, [71; 16]);
    owner.publish(provider()).unwrap();
    std::fs::create_dir(first.join("child")).unwrap();
    let location = registry
        .locate(&CanonicalTarget::resolve(&first.join("child")).unwrap())
        .unwrap()
        .unwrap();
    assert_eq!(location.target(), &target);
    assert_eq!(
        location.metadata().path(),
        CanonicalTarget::resolve(&metadata).unwrap().path()
    );
    assert!(matches!(
        registry.locate(&CanonicalTarget::resolve(&metadata).unwrap()),
        Err(Error::InvalidTarget)
    ));
}

#[cfg(unix)]
#[test]
fn metadata_identity_permissions_and_endpoint_rechecks_fail_closed() {
    use std::os::unix::fs::PermissionsExt;
    let root = TempDir::new("lore-location-proof-");
    let registry = ProviderRegistry::new(Limits::default());
    let instance = InstanceId::generate();
    let metadata = metadata_fixture(instance, [71; 16]);
    let owner = registry.reserve(root.path(), instance).unwrap();
    let alias = root.path().join("metadata-alias");
    std::os::unix::fs::symlink(metadata.path(), &alias).unwrap();
    assert!(owner.reserve_metadata(&alias, [71; 16].into()).is_err());
    std::fs::set_permissions(metadata.path(), std::fs::Permissions::from_mode(0o777)).unwrap();
    assert!(
        owner
            .reserve_metadata(metadata.path(), [71; 16].into())
            .is_err()
    );
    write_metadata(metadata.path(), instance, [71; 16]);
    owner
        .reserve_metadata(metadata.path(), [71; 16].into())
        .unwrap();
    owner.publish(provider()).unwrap();
    let target = owner.target().clone();
    let location = registry.locate(&target).unwrap().unwrap();
    let id = metadata.path().join(lore_revision::repository::ID);
    for bytes in [vec![], vec![1; 15], vec![1; 17], vec![72; 16]] {
        std::fs::write(&id, bytes).unwrap();
        assert!(registry.locate(&target).is_err());
        assert!(registry.resolve_location(&location).is_err());
    }
    write_metadata(metadata.path(), instance, [71; 16]);
    std::fs::set_permissions(&id, std::fs::Permissions::from_mode(0o666)).unwrap();
    assert!(registry.locate(&target).is_err());
    write_metadata(metadata.path(), instance, [71; 16]);
    let alternate = metadata.path().join("other-id");
    std::fs::write(&alternate, [71; 16]).unwrap();
    std::fs::remove_file(&id).unwrap();
    std::os::unix::fs::symlink(&alternate, &id).unwrap();
    assert!(registry.locate(&target).is_err());
    std::fs::remove_file(&id).unwrap();
    write_metadata(metadata.path(), instance, [71; 16]);
    // A visible ordinary marker cannot turn a registered corrupt owner into an OS checkout.
    std::fs::create_dir(root.path().join(".lore")).unwrap();
    std::fs::write(
        metadata.path().join(lore_revision::repository::CONFIG),
        "[vfs]\nvfs_type = \"None\"\n",
    )
    .unwrap();
    let resolver: Arc<dyn ProviderResolver> = registry.clone();
    assert!(
        lore_revision::repository::resolve_repository_paths(root.path(), Some(&resolver)).is_err()
    );
    write_metadata(metadata.path(), instance, [71; 16]);
    assert!(registry.resolve_location(&location).is_ok());
    let old_token = owner.test_token();
    drop(owner);
    let next = registry.reserve(root.path(), instance).unwrap();
    assert!(next.test_token() > old_token);
    next.reserve_metadata(metadata.path(), [71; 16].into())
        .unwrap();
    next.publish(provider()).unwrap();
    assert!(matches!(
        registry.resolve_location(&location),
        Err(Error::Stale)
    ));
}

#[cfg(unix)]
#[test]
fn provider_handoff_uses_original_location_and_refuses_late_registration() {
    use lore_revision::repository::CONFIG;
    use lore_revision::repository::INSTANCE;
    use lore_revision::repository::{self};
    let root = TempDir::new("lore-location-handoff-");
    let instance = InstanceId::generate();
    let first_metadata = metadata_fixture(instance, [71; 16]);
    let second_metadata = metadata_fixture(instance, [71; 16]);
    let registry = ProviderRegistry::new(Limits::default());
    let resolver: Arc<dyn ProviderResolver> = registry.clone();
    let first = registry.reserve(root.path(), instance).unwrap();
    first
        .reserve_metadata(first_metadata.path(), [71; 16].into())
        .unwrap();
    first.publish(provider()).unwrap();
    let original = repository::resolve_repository_paths(root.path(), Some(&resolver)).unwrap();
    let loaded_config: RepositoryConfig =
        toml::from_str(&std::fs::read_to_string(original.metadata.join(CONFIG)).unwrap()).unwrap();
    let instance_path = original.metadata.join(INSTANCE);
    let before = std::fs::read(&instance_path).unwrap();
    drop(first);
    let replacement = registry.reserve(root.path(), instance).unwrap();
    replacement
        .reserve_metadata(second_metadata.path(), [71; 16].into())
        .unwrap();
    let next_provider = provider();
    replacement.publish(next_provider.clone()).unwrap();
    assert!(
        repository::resolve_experimental_provider(
            &loaded_config,
            &original.root,
            &instance_path,
            instance,
            Some(&resolver),
            original.working.as_ref()
        )
        .is_err()
    );
    let fresh = repository::resolve_repository_paths(root.path(), Some(&resolver)).unwrap();
    let fresh_config =
        toml::from_str(&std::fs::read_to_string(fresh.metadata.join(CONFIG)).unwrap()).unwrap();
    let accepted = repository::resolve_experimental_provider(
        &fresh_config,
        &fresh.root,
        &fresh.metadata.join(INSTANCE),
        instance,
        Some(&resolver),
        fresh.working.as_ref(),
    )
    .unwrap()
    .unwrap();
    assert!(Arc::ptr_eq(&accepted, &next_provider));
    assert_eq!(std::fs::read(&instance_path).unwrap(), before);
    drop(replacement);
    let old_dot = root.path().join(repository::DOT_LORE);
    std::fs::create_dir(&old_dot).unwrap();
    write_metadata(&old_dot, instance, [71; 16]);
    let unregistered = repository::resolve_repository_paths(root.path(), Some(&resolver)).unwrap();
    assert!(unregistered.working.is_none());
    let config: RepositoryConfig =
        toml::from_str(&std::fs::read_to_string(unregistered.metadata.join(CONFIG)).unwrap())
            .unwrap();
    let late = registry.reserve(root.path(), instance).unwrap();
    late.reserve_metadata(first_metadata.path(), [71; 16].into())
        .unwrap();
    late.publish(provider()).unwrap();
    assert!(
        repository::resolve_experimental_provider(
            &config,
            &unregistered.root,
            &unregistered.metadata.join(INSTANCE),
            instance,
            Some(&resolver),
            unregistered.working.as_ref()
        )
        .is_err()
    );
}

#[cfg(unix)]
#[test]
fn metadata_endpoints_are_nonblocking_owned_and_size_bounded() {
    use std::os::unix::ffi::OsStrExt;
    use std::os::unix::fs::PermissionsExt;
    let root = TempDir::new("lore-location-special-files-");
    let registry = ProviderRegistry::new(Limits::default());
    let instance = InstanceId::generate();
    let metadata = metadata_fixture(instance, [71; 16]);
    let owner = registry.reserve(root.path(), instance).unwrap();
    owner
        .reserve_metadata(metadata.path(), [71; 16].into())
        .unwrap();
    owner.publish(provider()).unwrap();
    let target = owner.target().clone();
    let id = metadata.path().join(lore_revision::repository::ID);
    std::fs::remove_file(&id).unwrap();
    let path = std::ffi::CString::new(id.as_os_str().as_bytes()).unwrap();
    // SAFETY: the path is a live NUL-terminated string in the owned fixture.
    assert_eq!(unsafe { libc::mkfifo(path.as_ptr(), 0o600) }, 0);
    let (done, outcome) = std::sync::mpsc::channel();
    let worker = {
        let registry = registry.clone();
        let target = target.clone();
        std::thread::spawn(move || done.send(registry.locate(&target).is_err()).unwrap())
    };
    assert!(
        outcome
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("opening metadata FIFO blocked")
    );
    worker.join().unwrap();
    std::fs::remove_file(&id).unwrap();
    write_metadata(metadata.path(), instance, [71; 16]);
    // SAFETY: geteuid reads process identity and has no preconditions.
    let uid = unsafe { libc::geteuid() };
    if uid == 0 {
        // SAFETY: chown addresses only the owned fixture's ID file.
        assert_eq!(unsafe { libc::chown(path.as_ptr(), 65534, !0) }, 0);
        assert!(registry.locate(&target).is_err());
        // SAFETY: restore the fixture's original owner.
        assert_eq!(unsafe { libc::chown(path.as_ptr(), uid, !0) }, 0);
    }
    std::fs::set_permissions(metadata.path(), std::fs::Permissions::from_mode(0o777)).unwrap();
    assert!(registry.locate(&target).is_err());
    write_metadata(metadata.path(), instance, [71; 16]);
    std::fs::write(
        metadata.path().join(lore_revision::repository::CONFIG),
        vec![b' '; 64 * 1024 + 1],
    )
    .unwrap();
    assert!(registry.locate(&target).is_err());
    write_metadata(metadata.path(), instance, [71; 16]);
    assert!(registry.locate(&target).unwrap().is_some());
    let long = root.path().join("long-metadata-directory-name");
    std::fs::create_dir(&long).unwrap();
    std::fs::set_permissions(&long, std::fs::Permissions::from_mode(0o700)).unwrap();
    let bounded = ProviderRegistry::new(Limits {
        max_target_bytes: owner.target().path().as_os_str().len(),
        ..Limits::default()
    });
    let reservation = bounded.reserve(root.path(), instance).unwrap();
    assert!(matches!(
        reservation.reserve_metadata(&long, [71; 16].into()),
        Err(Error::InvalidTarget)
    ));
}

#[test]
fn unregistered_paths_keep_ordinary_and_legacy_metadata_discovery_outside_context() {
    let root = TempDir::new("lore-unregistered-metadata-");
    let legacy = root.path().join(lore_revision::repository::DOT_URC);
    std::fs::create_dir(&legacy).unwrap();
    std::fs::create_dir(root.path().join(lore_revision::repository::DOT_LORE)).unwrap();
    let resolver: Arc<dyn ProviderResolver> = ProviderRegistry::new(Limits::default());
    let paths =
        lore_revision::repository::resolve_repository_paths(root.path(), Some(&resolver)).unwrap();
    assert!(paths.working.is_none());
    assert_eq!(paths.root, root.path());
    assert_eq!(paths.metadata, legacy);
    assert_eq!(
        lore_revision::repository::get_dot_lore_path(root.path()).unwrap(),
        legacy
    );
}

#[cfg(unix)]
#[test]
fn locations_cannot_cross_isolated_registries_even_with_identical_bindings() {
    let root = TempDir::new("lore-location-registry-identity-");
    let first = ProviderRegistry::new(Limits::default());
    let second = ProviderRegistry::new(Limits::default());
    let instance = InstanceId::generate();
    let metadata = metadata_fixture(instance, [71; 16]);
    let a = first.reserve(root.path(), instance).unwrap();
    let b = second.reserve(root.path(), instance).unwrap();
    for owner in [&a, &b] {
        owner
            .reserve_metadata(metadata.path(), [71; 16].into())
            .unwrap();
        owner.publish(provider()).unwrap();
    }
    let location = first.locate(a.target()).unwrap().unwrap();
    assert!(first.resolve_location(&location).is_ok());
    assert!(matches!(
        second.resolve_location(&location),
        Err(Error::Stale)
    ));
}

#[cfg(unix)]
#[test]
fn terminal_retirement_withdraws_resolution_and_keeps_both_reservations() {
    let root = TempDir::new("lore-provider-retire-");
    let instance = InstanceId::generate();
    let metadata = metadata_fixture(instance, [71; 16]);
    let registry = ProviderRegistry::new(Limits::default());
    let registration = registry.reserve(root.path(), instance).unwrap();
    registration
        .reserve_metadata(metadata.path(), [71; 16].into())
        .unwrap();
    let retained = provider();
    registration.publish(retained.clone()).unwrap();
    let location = registry.locate(registration.target()).unwrap().unwrap();
    registration.retire().unwrap();
    registration.retire().unwrap();
    assert!(matches!(
        registry.locate(registration.target()),
        Err(Error::Retired)
    ));
    assert!(matches!(
        registry.resolve_location(&location),
        Err(Error::Retired)
    ));
    assert!(matches!(
        registry.resolve(registration.target(), instance),
        Err(Error::Retired)
    ));
    assert!(matches!(
        registration.publish(retained.clone()),
        Err(Error::Retired)
    ));
    assert!(matches!(
        registration.reserve_metadata(metadata.path(), [71; 16].into()),
        Err(Error::Retired)
    ));
    assert!(matches!(
        registry.reserve(root.path(), instance),
        Err(Error::Overlap)
    ));
    assert!(matches!(
        registry.reserve(metadata.path(), instance),
        Err(Error::Overlap)
    ));
    assert!(
        retained.holds_state(),
        "existing Arcs retain their independent lifetime"
    );
    drop(registration);
    let replacement = registry.reserve(root.path(), instance).unwrap();
    replacement
        .reserve_metadata(metadata.path(), [71; 16].into())
        .unwrap();
    replacement.publish(provider()).unwrap();
    assert!(matches!(
        registry.resolve_location(&location),
        Err(Error::Stale)
    ));
}

#[cfg(unix)]
#[test]
fn retirement_drops_provider_outside_registry_lock() {
    struct Reentrant {
        registry: std::sync::Weak<ProviderRegistry>,
        target: CanonicalTarget,
        instance: InstanceId,
        dropped: std::sync::mpsc::Sender<()>,
    }
    #[async_trait]
    impl FilesystemProvider for Reentrant {
        fn holds_state(&self) -> bool {
            true
        }
        async fn begin_operation(
            &self,
        ) -> std::result::Result<Arc<InstanceOperationImpl>, FsError> {
            Err(FsError::internal("retirement fixture has no operations"))
        }
    }
    impl Drop for Reentrant {
        fn drop(&mut self) {
            assert!(matches!(
                self.registry
                    .upgrade()
                    .unwrap()
                    .resolve(&self.target, self.instance),
                Err(Error::Retired)
            ));
            self.dropped.send(()).unwrap();
        }
    }
    let target = TempDir::new("lore-retirement-reentry-");
    let registry = ProviderRegistry::new(Limits::default());
    let instance = InstanceId::generate();
    let metadata = metadata_fixture(instance, [71; 16]);
    let registration = registry.reserve(target.path(), instance).unwrap();
    registration
        .reserve_metadata(metadata.path(), [71; 16].into())
        .unwrap();
    let (sender, receiver) = std::sync::mpsc::channel();
    registration
        .publish(Arc::new(Reentrant {
            registry: Arc::downgrade(&registry),
            target: registration.target().clone(),
            instance,
            dropped: sender,
        }))
        .unwrap();
    let retiring = std::thread::spawn(move || {
        registration.retire().unwrap();
        registration
    });
    receiver
        .recv_timeout(std::time::Duration::from_secs(2))
        .unwrap();
    let registration = retiring.join().unwrap();
    assert!(matches!(
        registry.reserve(target.path(), instance),
        Err(Error::Overlap)
    ));
    drop(registration);
}
