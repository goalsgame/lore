// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
// Fixtures control a private runtime and corrupt their own metadata endpoints.
#![allow(clippy::disallowed_methods)]

use std::sync::Arc;

use lore_base::test_util::TempDir;
use lore_revision::fs::provider_registry::Limits;
use lore_revision::fs::provider_registry::ProviderRegistry;
use lore_revision::fs::provider_registry::Registration;
use lore_revision::instance::InstanceId;
use lore_revision::repository::RepositoryConfig;
use lore_revision::repository::RepositoryWriteToken;
use lore_revision::repository::VfsConfig;
use lore_revision::repository::VfsType;
use lore_revision::repository::working_bootstrap::Error;
use lore_revision::repository::working_bootstrap::MetadataInitializer;
use lore_revision::repository::working_bootstrap::Mode;

struct Provider;
#[async_trait::async_trait]
impl lore_revision::fs::filesystem_provider::FilesystemProvider for Provider {
    fn holds_state(&self) -> bool {
        true
    }
    async fn begin_operation(
        &self,
    ) -> std::result::Result<
        Arc<lore_revision::fs::filesystem_provider::InstanceOperationImpl>,
        lore_revision::fs::filesystem_provider::FsError,
    > {
        Err(lore_revision::fs::filesystem_provider::FsError::internal(
            "metadata fixture has no operations",
        ))
    }
}

struct Fixture {
    _root: TempDir,
    target: std::path::PathBuf,
    metadata: std::path::PathBuf,
    registry: Arc<ProviderRegistry>,
    registration: Arc<Registration>,
}
impl Fixture {
    fn new() -> Self {
        use std::os::unix::fs::PermissionsExt;
        let root = TempDir::new("lore-working-bootstrap-");
        let target = root.join("mount");
        let metadata = root.join("metadata");
        std::fs::create_dir(&target).unwrap();
        std::fs::create_dir(&metadata).unwrap();
        std::fs::set_permissions(&metadata, std::fs::Permissions::from_mode(0o700)).unwrap();
        let registry = ProviderRegistry::new(Limits::default());
        let registration = Arc::new(registry.reserve(&target, InstanceId::generate()).unwrap());
        registration
            .reserve_metadata(&metadata, [51; 16].into())
            .unwrap();
        Self {
            _root: root,
            target,
            metadata,
            registry,
            registration,
        }
    }
    async fn token(&self) -> RepositoryWriteToken {
        RepositoryWriteToken::acquire(self.registration.target().path()).await
    }
}
fn config() -> RepositoryConfig {
    RepositoryConfig {
        identity: Some("named-local-identity".into()),
        vfs: Some(VfsConfig {
            vfs_type: VfsType::ExperimentalWorking,
        }),
        ..Default::default()
    }
}

#[tokio::test]
async fn create_and_readonly_reuse_preserve_exact_binding_and_unready_state() {
    use std::os::unix::fs::MetadataExt;

    use lore_revision::fs::provider_registry::Error as RegistryError;
    use lore_revision::fs::provider_registry::ProviderResolver;
    let fixture = Fixture::new();
    let initializer = MetadataInitializer::new(1).unwrap();
    let result = initializer
        .prepare(
            fixture.registration.clone(),
            fixture.token().await,
            Mode::CreateNew(config()),
        )
        .await
        .unwrap();
    assert_eq!(
        result.location().target().path(),
        std::fs::canonicalize(&fixture.target).unwrap()
    );
    assert_eq!(
        result.config().identity.as_deref(),
        Some("named-local-identity")
    );
    assert_eq!(std::fs::read_dir(&fixture.metadata).unwrap().count(), 3);
    let files: Vec<_> = ["id", "instance", "config.toml"]
        .iter()
        .map(|name| {
            let path = fixture.metadata.join(name);
            let metadata = std::fs::metadata(&path).unwrap();
            assert_eq!(metadata.mode() & 0o777, 0o600);
            (
                path.clone(),
                std::fs::read(path).unwrap(),
                metadata.modified().unwrap(),
            )
        })
        .collect();
    assert!(matches!(
        fixture
            .registry
            .resolve(fixture.registration.target(), result.location().instance()),
        Err(RegistryError::NotReady)
    ));
    let expected = result.location().clone();
    drop(result);
    let reused = initializer
        .prepare(
            fixture.registration.clone(),
            fixture.token().await,
            Mode::Reuse,
        )
        .await
        .unwrap();
    assert_eq!(reused.location(), &expected);
    for (path, bytes, modified) in files {
        assert_eq!(std::fs::read(&path).unwrap(), bytes);
        assert_eq!(
            std::fs::metadata(path).unwrap().modified().unwrap(),
            modified
        );
    }
    assert!(!fixture.target.join(".lore").exists());
    assert!(!fixture.metadata.join("mutable").exists());
}

#[tokio::test]
async fn wrong_tokens_invalid_config_and_nonempty_create_do_not_change_files() {
    let fixture = Fixture::new();
    let initializer = MetadataInitializer::new(1).unwrap();
    let wrong = RepositoryWriteToken::acquire(&fixture.metadata).await;
    assert!(matches!(
        initializer
            .prepare(
                fixture.registration.clone(),
                wrong,
                Mode::CreateNew(config())
            )
            .await,
        Err(Error::InvalidToken)
    ));
    assert!(matches!(
        initializer
            .prepare(
                fixture.registration.clone(),
                fixture.token().await,
                Mode::CreateNew(RepositoryConfig::default())
            )
            .await,
        Err(Error::InvalidConfig)
    ));
    assert_eq!(std::fs::read_dir(&fixture.metadata).unwrap().count(), 0);
    std::fs::write(fixture.metadata.join("retained"), b"untouched").unwrap();
    assert!(matches!(
        initializer
            .prepare(
                fixture.registration.clone(),
                fixture.token().await,
                Mode::CreateNew(config())
            )
            .await,
        Err(Error::NotEmpty)
    ));
    assert_eq!(
        std::fs::read(fixture.metadata.join("retained")).unwrap(),
        b"untouched"
    );
    assert_eq!(std::fs::read_dir(&fixture.metadata).unwrap().count(), 1);
    assert!(MetadataInitializer::new(0).is_err());
    assert!(MetadataInitializer::new(4097).is_err());
}

#[tokio::test]
async fn reuse_rejects_incomplete_wrong_identity_and_symlink_without_repair() {
    use std::os::unix::fs::symlink;
    let fixture = Fixture::new();
    let initializer = MetadataInitializer::new(1).unwrap();
    drop(
        initializer
            .prepare(
                fixture.registration.clone(),
                fixture.token().await,
                Mode::CreateNew(config()),
            )
            .await
            .unwrap(),
    );
    std::fs::write(fixture.metadata.join(".working-initializing"), b"").unwrap();
    assert!(fixture.registration.publish(Arc::new(Provider)).is_err());
    assert!(matches!(
        initializer
            .prepare(
                fixture.registration.clone(),
                fixture.token().await,
                Mode::CreateNew(config())
            )
            .await,
        Err(Error::NotEmpty)
    ));
    assert!(matches!(
        initializer
            .prepare(
                fixture.registration.clone(),
                fixture.token().await,
                Mode::Reuse
            )
            .await,
        Err(Error::Incomplete)
    ));
    std::fs::remove_file(fixture.metadata.join(".working-initializing")).unwrap();
    let id = fixture.metadata.join("id");
    std::fs::write(&id, [9; 16]).unwrap();
    assert!(matches!(
        initializer
            .prepare(
                fixture.registration.clone(),
                fixture.token().await,
                Mode::Reuse
            )
            .await,
        Err(Error::Registry(_))
    ));
    assert_eq!(std::fs::read(&id).unwrap(), [9; 16]);
    std::fs::remove_file(&id).unwrap();
    symlink(fixture.metadata.join("instance"), &id).unwrap();
    assert!(
        initializer
            .prepare(
                fixture.registration.clone(),
                fixture.token().await,
                Mode::Reuse
            )
            .await
            .is_err()
    );
    assert!(
        std::fs::symlink_metadata(id)
            .unwrap()
            .file_type()
            .is_symlink()
    );
}

#[test]
fn cancelled_queued_job_retains_registration_and_root_token_across_runtime_drop() {
    use std::time::Duration;
    let fixture = Fixture::new();
    let initializer = MetadataInitializer::new(1).unwrap();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()
        .unwrap();
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    runtime.spawn_blocking(move || {
        entered_tx.send(()).unwrap();
        release_rx.recv().unwrap();
    });
    entered_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    let token = runtime.block_on(fixture.token());
    let registration = fixture.registration.clone();
    let weak = Arc::downgrade(&registration);
    let jobs = initializer.clone();
    let handle = runtime.spawn(async move {
        jobs.prepare(registration, token, Mode::CreateNew(config()))
            .await
    });
    runtime.block_on(async {
        while initializer.jobs.available_permits() != 0 {
            tokio::task::yield_now().await;
        }
    });
    let other = Fixture::new();
    assert!(matches!(
        runtime.block_on(async {
            initializer
                .prepare(
                    other.registration.clone(),
                    other.token().await,
                    Mode::CreateNew(config()),
                )
                .await
        }),
        Err(Error::Busy)
    ));
    assert_eq!(std::fs::read_dir(&other.metadata).unwrap().count(), 0);
    handle.abort();
    runtime.block_on(async {
        tokio::task::yield_now().await;
    });
    drop(handle);
    let Fixture {
        _root,
        target,
        metadata,
        registry,
        registration,
    } = fixture;
    let instance = InstanceId::generate();
    drop(registration);
    runtime.shutdown_timeout(Duration::ZERO);
    assert!(
        weak.upgrade().is_some(),
        "queued actual job owns registration"
    );
    assert!(registry.reserve(&target, instance).is_err());
    let observer = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    assert!(
        observer
            .block_on(async {
                tokio::time::timeout(
                    Duration::from_millis(20),
                    RepositoryWriteToken::acquire(&std::fs::canonicalize(&target).unwrap()),
                )
                .await
            })
            .is_err()
    );
    release_tx.send(()).unwrap();
    let started = std::time::Instant::now();
    while weak.upgrade().is_some() {
        assert!(started.elapsed() < Duration::from_secs(10));
        std::thread::yield_now();
    }
    let _next = registry.reserve(&target, instance).unwrap();
    let _token = observer.block_on(RepositoryWriteToken::acquire(
        &std::fs::canonicalize(&target).unwrap(),
    ));
    assert!(metadata.join("config.toml").is_file());
    assert!(!metadata.join(".working-initializing").exists());
    drop(_root);
}

#[tokio::test]
async fn retained_marker_rejects_every_published_location_resolution() {
    use lore_revision::fs::provider_registry::ProviderResolver;
    let fixture = Fixture::new();
    let initializer = MetadataInitializer::new(1).unwrap();
    let prepared = initializer
        .prepare(
            fixture.registration.clone(),
            fixture.token().await,
            Mode::CreateNew(config()),
        )
        .await
        .unwrap();
    let location = prepared.location().clone();
    drop(prepared);
    fixture.registration.publish(Arc::new(Provider)).unwrap();
    std::fs::write(fixture.metadata.join(".working-initializing"), b"").unwrap();
    assert!(fixture.registry.locate(location.target()).is_err());
    assert!(fixture.registry.resolve_location(&location).is_err());
    assert!(
        fixture
            .registry
            .resolve(location.target(), location.instance())
            .is_err()
    );
    assert!(fixture.metadata.join(".working-initializing").is_file());
}

mod stores;

mod parent_lease;

mod pending;

mod remote_contents;
