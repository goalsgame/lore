// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
// The private fixture writes a repository config selecting the service-owned experiment.
#![allow(clippy::disallowed_methods)]

use std::sync::Arc;

use lore::call::setup_execution;
use lore::call::with_provider_resolver;
use lore::call::with_relayed_environment;
use lore_base::env::CallEnvironment;
use lore_base::runtime::LORE_CONTEXT;
use lore_revision::fs::provider_registry::ProviderRegistry;
use lore_revision::fs::provider_registry::ProviderResolver;

fn resolver() -> Arc<dyn ProviderResolver> {
    ProviderRegistry::new(Default::default())
}
fn assert_resolver(expected: &Arc<dyn ProviderResolver>) {
    let execution = setup_execution(Default::default(), None);
    assert!(Arc::ptr_eq(
        expected,
        &execution.provider_resolver().unwrap()
    ));
}

#[tokio::test]
async fn resolver_scopes_are_nested_and_never_leak_to_default_contexts() {
    assert!(
        setup_execution(Default::default(), None)
            .provider_resolver()
            .is_none()
    );
    let outer = resolver();
    let inner = resolver();
    let captured = with_provider_resolver(outer.clone(), async {
        assert_resolver(&outer);
        with_provider_resolver(inner.clone(), async {
            tokio::task::yield_now().await;
            assert_resolver(&inner);
        })
        .await;
        assert_resolver(&outer);
        setup_execution(Default::default(), None)
    })
    .await;
    assert!(
        setup_execution(Default::default(), None)
            .provider_resolver()
            .is_none()
    );
    assert!(Arc::ptr_eq(&outer, &captured.provider_resolver().unwrap()));
    drop(outer);
    assert!(captured.provider_resolver().is_none());
}

#[tokio::test]
async fn concurrent_resolvers_keep_their_relayed_environment_and_do_not_cross() {
    let first = resolver();
    let second = resolver();
    let run = |resolver: Arc<dyn ProviderResolver>, label: &'static str| async move {
        let environment = CallEnvironment {
            values: [Some(label.to_string()), None],
        };
        with_relayed_environment(
            environment,
            with_provider_resolver(resolver.clone(), async move {
                for _ in 0..10 {
                    tokio::task::yield_now().await;
                    let execution = setup_execution(Default::default(), None);
                    assert!(Arc::ptr_eq(
                        &resolver,
                        &execution.provider_resolver().unwrap()
                    ));
                    LORE_CONTEXT
                        .scope(execution, async {
                            assert_eq!(
                                lore_base::env::var(lore_base::env::GLOBAL_PATH_VAR).as_deref(),
                                Some(label)
                            );
                            assert_eq!(lore_base::env::var(lore_base::env::AUTH_PATH_VAR), None);
                        })
                        .await;
                }
            }),
        )
        .await;
    };
    tokio::join!(
        run(first, "/first-service-call"),
        run(second, "/second-service-call")
    );
    assert!(
        setup_execution(Default::default(), None)
            .provider_resolver()
            .is_none()
    );
}

#[tokio::test]
async fn cancelling_a_scope_releases_its_strong_resolver_without_leaking() {
    let registry = resolver();
    let weak = Arc::downgrade(&registry);
    let mut scope = Box::pin(with_provider_resolver(registry.clone(), async {
        std::future::pending::<()>().await;
    }));
    drop(registry);
    tokio::select! {
        biased;
        () = &mut scope => panic!("pending scope completed"),
        () = tokio::task::yield_now() => {}
    }
    assert!(weak.upgrade().is_some());
    drop(scope);
    assert!(weak.upgrade().is_none());
    assert!(
        setup_execution(Default::default(), None)
            .provider_resolver()
            .is_none()
    );
}

#[cfg(unix)]
#[tokio::test]
async fn service_command_invocation_routes_to_that_service_owner_only() {
    use std::path::Path;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use async_trait::async_trait;
    use lore::call::RepositoryWriteToken;
    use lore_revision::fs::filesystem_provider::FilesystemProvider;
    use lore_revision::fs::filesystem_provider::FsError;
    use lore_revision::fs::filesystem_provider::InstanceOperationImpl;
    use lore_revision::interface::LoreGlobalArgs;
    use lore_revision::repository;
    use lore_revision::repository::RepositoryConfig;
    use lore_revision::repository::VfsConfig;
    use lore_revision::repository::VfsType;
    use lore_revision::service_state::ServiceStateImpl;

    struct Provider {
        calls: AtomicUsize,
        observed: std::sync::Mutex<Vec<(String, Option<String>)>>,
    }
    #[async_trait]
    impl FilesystemProvider for Provider {
        async fn read_metadata_file(
            &self,
            path: &lore_revision::util::path::RelativePath,
            _maximum: usize,
        ) -> Result<Vec<u8>, FsError> {
            Err(lore_base::error::FileNotFound {
                resource: path.to_string(),
            }
            .into())
        }
        fn holds_state(&self) -> bool {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if let Some(context) = lore_revision::runtime::try_execution_context() {
                self.observed.lock().unwrap().push((
                    context.globals().repository_path.to_string(),
                    context.globals().working_directory().map(str::to_owned),
                ));
            }
            true
        }
        async fn begin_operation(&self) -> Result<Arc<InstanceOperationImpl>, FsError> {
            Err(FsError::internal(
                "revision-only command must not open an operation",
            ))
        }
    }
    let directory = lore_base::test_util::TempDir::new("lore-service-provider-command-");
    let instance = LORE_CONTEXT
        .scope(setup_execution(Default::default(), None), async {
            let token = RepositoryWriteToken::acquire(directory.path()).await;
            let repository = repository::create_local(
                directory.path(),
                &token,
                [67; 16].into(),
                [68; 16].into(),
                "main".into(),
                Default::default(),
                false,
            )
            .await
            .unwrap();
            repository.instance_id
        })
        .await;
    let config = RepositoryConfig {
        vfs: Some(VfsConfig {
            vfs_type: VfsType::ExperimentalWorking,
        }),
        ..Default::default()
    };
    let external = lore_base::test_util::TempDir::new("lore-service-command-external-");
    let metadata = external.path().join("metadata");
    std::fs::rename(directory.path().join(repository::DOT_LORE), &metadata).unwrap();
    std::fs::write(
        metadata.join(repository::CONFIG),
        toml::to_string(&config).unwrap(),
    )
    .unwrap();
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(&metadata, std::fs::Permissions::from_mode(0o700)).unwrap();
    for name in [repository::ID, repository::INSTANCE, repository::CONFIG] {
        std::fs::set_permissions(metadata.join(name), std::fs::Permissions::from_mode(0o600))
            .unwrap();
    }
    let nested = directory.path().join("nested");
    std::fs::create_dir(&nested).unwrap();
    assert!(!directory.path().join(".lore").exists());
    assert!(!directory.path().join(".urc").exists());
    let state = ServiceStateImpl::new();
    let owner = state
        .provider_registry()
        .reserve(directory.path(), instance)
        .unwrap();
    let provider = Arc::new(Provider {
        calls: AtomicUsize::new(0),
        observed: std::sync::Mutex::new(Vec::new()),
    });
    owner.reserve_metadata(&metadata, [67; 16].into()).unwrap();
    owner.publish(provider.clone()).unwrap();
    provider.calls.store(0, Ordering::SeqCst);
    let command = || lore::remote::message::MessageToServer {
        globals: LoreGlobalArgs {
            repository_path: nested.display().to_string().into(),
            working_directory: nested.display().to_string().into(),
            ..Default::default()
        },
        command: lore::remote::command::LoreCommand::RepositoryStatus(
            lore::repository::LoreRepositoryStatusArgs {
                staged: 0,
                scan: 0,
                check_dirty: 0,
                reset: 0,
                sync_point: 0,
                revision_only: 1,
                count: 0,
                paths: Default::default(),
            },
        ),
        environment: CallEnvironment::default(),
    };
    assert_eq!(
        lore::remote::connection::invoke_service_command(command(), None, &state).await,
        0
    );
    assert!(provider.calls.load(Ordering::SeqCst) > 0);
    for (repository, cwd) in provider.observed.lock().unwrap().iter() {
        assert_eq!(Path::new(repository), owner.target().path());
        assert_eq!(cwd.as_deref(), Some(nested.to_str().unwrap()));
    }
    let before = provider.calls.load(Ordering::SeqCst);
    let other = ServiceStateImpl::new();
    assert_ne!(
        lore::remote::connection::invoke_service_command(command(), None, &other).await,
        0
    );
    assert_eq!(provider.calls.load(Ordering::SeqCst), before);
    assert!(
        setup_execution(Default::default(), None)
            .provider_resolver()
            .is_none()
    );
}
