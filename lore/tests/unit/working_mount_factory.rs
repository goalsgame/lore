// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
// Real bootstrap fixtures keep recovery exclusive before mutable setup.
#![cfg(unix)]
#![allow(clippy::disallowed_methods)]

use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use lore::working_mount::Error;
use lore::working_mount::InstanceHandle;
use lore::working_mount::OwnerLimits;
use lore::working_mount::Phase;
use lore::working_mount::Supervisor;
use lore::working_mount::bootstrap::factory::BootstrapMode;
use lore::working_mount::bootstrap::factory::BootstrapPause;
use lore::working_mount::bootstrap::factory::BootstrapPolicy;
use lore::working_mount::bootstrap::factory::BootstrapRequest;
use lore_base::runtime::LORE_CONTEXT;
use lore_base::test_util::TempDir;
use lore_base::types::RepositoryId;
use lore_io::lease::IoLease;
use lore_revision::fs::provider_registry::ProviderRegistry;
use lore_revision::fs::provider_registry::Registration;
use lore_revision::instance::InstanceId;
use lore_revision::repository::RepositoryConfig;
use lore_revision::repository::RepositoryWriteToken;
use lore_revision::repository::VfsConfig;
use lore_revision::repository::VfsType;
use lore_revision::repository::working_bootstrap::BackingBinding;
use lore_revision::repository::working_bootstrap::Selection;
use lore_vfs::linux_working::Model;
use lore_vfs::linux_working::OpenFlags;
use lore_vfs::overlay::InjectedGateFactory;
use lore_vfs::recovery::Recovery;

struct Fixture {
    _root: TempDir,
    registry: Arc<ProviderRegistry>,
    supervisor: Arc<Supervisor>,
    registration: Option<Arc<Registration>>,
    target: PathBuf,
    metadata: PathBuf,
    backing: PathBuf,
    repository: RepositoryId,
    instance: InstanceId,
}
impl Fixture {
    fn new() -> Self {
        use std::os::unix::fs::DirBuilderExt;
        let root = TempDir::new("working-factory-");
        let target = root.join("mount");
        let metadata = root.join("metadata");
        std::fs::create_dir(&target).unwrap();
        std::fs::DirBuilder::new()
            .mode(0o700)
            .create(&metadata)
            .unwrap();
        let target = target.canonicalize().unwrap();
        let metadata = metadata.canonicalize().unwrap();
        let registry = ProviderRegistry::new(Default::default());
        let supervisor = Supervisor::new(registry.clone(), Default::default()).unwrap();
        let repository = RepositoryId::from([213; 16]);
        let instance = InstanceId::generate();
        let registration = Arc::new(registry.reserve(&target, instance).unwrap());
        registration
            .reserve_metadata(&metadata, repository)
            .unwrap();
        Self {
            backing: root.join("backing"),
            _root: root,
            registry,
            supervisor,
            registration: Some(registration),
            target,
            metadata,
            repository,
            instance,
        }
    }
    fn request(&self, fresh: bool) -> BootstrapRequest {
        BootstrapRequest {
            registration: self.registration.as_ref().unwrap().clone(),
            mode: if fresh {
                BootstrapMode::CreateNew {
                    backing: self.backing.clone(),
                    config: RepositoryConfig {
                        vfs: Some(VfsConfig {
                            vfs_type: VfsType::ExperimentalWorking,
                        }),
                        ..Default::default()
                    },
                    selection: Selection {
                        branch: [214; 16].into(),
                        branch_name: "main".into(),
                        revision: Default::default(),
                    },
                }
            } else {
                BootstrapMode::Reuse
            },
        }
    }
    fn register_again(&mut self) {
        drop(self.registration.take());
        let registration = Arc::new(self.registry.reserve(&self.target, self.instance).unwrap());
        registration
            .reserve_metadata(&self.metadata, self.repository)
            .unwrap();
        self.registration = Some(registration);
    }
    fn recovery(&self) -> PathBuf {
        self.backing.join("recovery")
    }
}
fn policy() -> BootstrapPolicy {
    BootstrapPolicy::Injected(Arc::new(InjectedGateFactory::new(Arc::new(
        super::working_mount::ExplicitEdits,
    ))))
}
async fn bounded<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(40), future)
        .await
        .expect("factory progress")
}
fn pause() -> (
    BootstrapPause,
    tokio::sync::oneshot::Receiver<Arc<Model>>,
    std::sync::mpsc::Sender<()>,
) {
    let (prepared, model) = tokio::sync::oneshot::channel();
    let (resume, resuming) = std::sync::mpsc::channel();
    (
        BootstrapPause {
            prepared,
            resume: resuming,
        },
        model,
        resume,
    )
}
async fn remove(fixture: &Fixture, handle: &InstanceHandle) {
    bounded(async {
        loop {
            match fixture.supervisor.retire_instance(handle).await {
                Err(Error::Busy) => tokio::task::yield_now().await,
                result => break result.unwrap(),
            }
        }
    })
    .await;
}

#[tokio::test]
async fn factory_fresh_edit_checkpoint_retire_then_reuse_transfers_same_recovery_owner() {
    Box::pin(
        LORE_CONTEXT.scope(super::working_mount::execution(), async {
            let mut fixture = Fixture::new();
            for fresh in [true, false] {
                let (pause, model, resume) = pause();
                let supervisor = fixture.supervisor.clone();
                let request = fixture.request(fresh);
                let mut preparing = Box::pin(supervisor.test_bootstrap_prepare(
                    request,
                    policy(),
                    OwnerLimits::default(),
                    Default::default(),
                    pause,
                ));
                let model = tokio::select! {
                    model=bounded(model)=>model.unwrap(),
                    result=&mut preparing=>panic!("factory ended before model handoff: {result:?}"),
                };
                assert!(matches!(
                    Recovery::open_existing(fixture.recovery(), OwnerLimits::default().recovery),
                    Err(lore_vfs::recovery::Error::Busy)
                ));
                let flags = OpenFlags {
                    write: true,
                    append: false,
                    truncate: false,
                };
                if fresh {
                    let (file, _) = model.create(1, b"retained", 0o644, flags).await.unwrap();
                    model
                        .write(file, 0, b"factory checkpoint bytes")
                        .await
                        .unwrap();
                    model.fsync(file, false).await.unwrap();
                    model.release(file).await.unwrap();
                } else {
                    let attrs = model.lookup(1, b"retained").await.unwrap();
                    let file = model
                        .open(
                            attrs.entry.inode,
                            OpenFlags {
                                write: false,
                                ..flags
                            },
                        )
                        .await
                        .unwrap();
                    assert_eq!(
                        model.read(file, 0, 64).await.unwrap(),
                        b"factory checkpoint bytes"
                    );
                    model.release(file).await.unwrap();
                }
                drop(model);
                resume.send(()).unwrap();
                let handle = bounded(preparing).await.unwrap();
                assert_eq!(
                    fixture.supervisor.status(&handle).unwrap().phase,
                    Phase::Retiring
                );
                remove(&fixture, &handle).await;
                assert!(fixture.supervisor.instances().unwrap().is_empty());
                let binding =
                    BackingBinding::read(&fixture.metadata, fixture.repository, fixture.instance)
                        .unwrap();
                assert_eq!(binding.root(), fixture.backing.canonicalize().unwrap());
                let recovery =
                    Recovery::open_existing(fixture.recovery(), OwnerLimits::default().recovery)
                        .unwrap();
                assert!(recovery.load().unwrap().is_some());
                drop(recovery);
                if fresh {
                    fixture.register_again();
                }
            }
        }),
    )
    .await;
}

#[tokio::test]
async fn canceled_factory_waiter_retains_client_recovery_and_caller_until_actual_worker_exit() {
    Box::pin(
        LORE_CONTEXT.scope(super::working_mount::execution(), async {
            let fixture = Fixture::new();
            let (pause, model, resume) = pause();
            let caller = Arc::new(());
            let weak = Arc::downgrade(&caller);
            let lease = IoLease::new(caller);
            let supervisor = fixture.supervisor.clone();
            let request = fixture.request(true);
            let task = tokio::spawn(LORE_CONTEXT.scope(
                super::working_mount::execution(),
                async move {
                    lease
                        .scope(supervisor.test_bootstrap_prepare(
                            request,
                            policy(),
                            OwnerLimits::default(),
                            Default::default(),
                            pause,
                        ))
                        .await
                },
            ));
            let model = bounded(model).await.unwrap();
            let handle = fixture.supervisor.instances().unwrap().pop().unwrap().0;
            let mut client = Box::pin(RepositoryWriteToken::acquire(&fixture.target));
            assert!(futures::poll!(&mut client).is_pending());
            drop(client);
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
            assert!(weak.upgrade().is_some());
            assert_eq!(
                fixture.supervisor.status(&handle).unwrap().phase,
                Phase::Retiring
            );
            assert!(matches!(
                Recovery::open_existing(fixture.recovery(), OwnerLimits::default().recovery),
                Err(lore_vfs::recovery::Error::Busy)
            ));
            let mut client = Box::pin(RepositoryWriteToken::acquire(&fixture.target));
            assert!(futures::poll!(&mut client).is_pending());
            drop(client);
            drop(model);
            resume.send(()).unwrap();
            remove(&fixture, &handle).await;
            assert!(weak.upgrade().is_none());
            drop(bounded(RepositoryWriteToken::acquire(&fixture.target)).await);
            Recovery::open_existing(fixture.recovery(), OwnerLimits::default().recovery).unwrap();
        }),
    )
    .await;
}

#[tokio::test]
async fn reuse_foreign_binding_refuses_before_backing_or_normal_store_effects() {
    Box::pin(
        LORE_CONTEXT.scope(super::working_mount::execution(), async {
            let fixture = Fixture::new();
            let foreign = fixture._root.join("foreign");
            std::fs::create_dir(&foreign).unwrap();
            BackingBinding::write_new(
                &fixture.metadata,
                [215; 16].into(),
                fixture.instance,
                &foreign.canonicalize().unwrap(),
            )
            .unwrap();
            let record = fixture.metadata.join(".working-backing-v1");
            let original = std::fs::read(&record).unwrap();
            let (pause, _model, _resume) = pause();
            let error = bounded(fixture.supervisor.test_bootstrap_prepare(
                fixture.request(false),
                policy(),
                OwnerLimits::default(),
                Default::default(),
                pause,
            ))
            .await
            .unwrap_err();
            assert!(matches!(error.error, Error::InvalidBinding));
            assert!(error.handle.is_none());
            assert!(fixture.supervisor.instances().unwrap().is_empty());
            assert!(!fixture.backing.exists());
            assert_eq!(std::fs::read(record).unwrap(), original);
            for name in ["immutable", "mutable", ".working-store-initializing"] {
                assert!(!fixture.metadata.join(name).exists());
            }
        }),
    )
    .await;
}

#[cfg(target_os = "linux")]
fn assert_private_bytes_have_no_immutable_hash(path: &std::path::Path) {
    let path = std::ffi::CString::new(path.as_os_str().as_encoded_bytes()).unwrap();
    let mut digest = [0u8; 32];
    // SAFETY: the path/name are terminated and the writable buffer has its exact size.
    let size = unsafe {
        libc::getxattr(
            path.as_ptr(),
            c"user.sha256".as_ptr(),
            digest.as_mut_ptr().cast(),
            digest.len(),
        )
    };
    assert_eq!(
        size, -1,
        "private bytes have no immutable ingestion receipt"
    );
    assert_eq!(
        std::io::Error::last_os_error().raw_os_error(),
        Some(libc::ENODATA)
    );
}

#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Linux /dev/fuse and unprivileged mount support"]
async fn mounted_factory_fresh_os_edit_fsync_retire_then_reuse_reads_checkpoint() {
    Box::pin(
        LORE_CONTEXT.scope(super::working_mount::execution(), async {
            let mut fixture = Fixture::new();
            let first = bounded(fixture.supervisor.bootstrap_mount(
                fixture.request(true),
                policy(),
                OwnerLimits::default(),
                Default::default(),
            ))
            .await
            .unwrap();
            assert_eq!(
                fixture.supervisor.status(&first).unwrap().phase,
                Phase::Ready
            );
            let path = fixture.target.join("retained");
            let file = bounded(async {
                use tokio::io::AsyncWriteExt;
                let mut file = tokio::fs::File::create(&path).await.unwrap();
                file.write_all(b"mounted factory bytes").await.unwrap();
                file.sync_all().await.unwrap();
                file
            })
            .await;
            drop(file);
            assert_private_bytes_have_no_immutable_hash(&path);
            remove(&fixture, &first).await;
            assert!(!path.exists());
            fixture.register_again();
            let second = bounded(fixture.supervisor.bootstrap_mount(
                fixture.request(false),
                policy(),
                OwnerLimits::default(),
                Default::default(),
            ))
            .await
            .unwrap();
            assert_eq!(
                bounded(tokio::fs::read(&path)).await.unwrap(),
                b"mounted factory bytes"
            );
            assert_private_bytes_have_no_immutable_hash(&path);
            remove(&fixture, &second).await;
            assert!(fixture.supervisor.instances().unwrap().is_empty());
        }),
    )
    .await;
}

struct Source {
    path: PathBuf,
    repository: Arc<lore_revision::repository::RepositoryContext>,
    revision: lore_base::types::Hash,
    metadata: Vec<(PathBuf, Vec<u8>)>,
}
impl Source {
    async fn new(fixture: &Fixture) -> Self {
        use lore_revision::interface::LoreArray;
        use lore_revision::interface::LoreString;
        let path = fixture._root.join("source");
        std::fs::create_dir(&path).unwrap();
        let path = path.canonicalize().unwrap();
        let token = RepositoryWriteToken::acquire(&path).await;
        let repository = Box::pin(lore_revision::repository::create_local(
            &path,
            &token,
            fixture.repository,
            [214; 16].into(),
            "main".into(),
            RepositoryConfig::default(),
            false,
        ))
        .await
        .unwrap();
        std::fs::write(path.join("original.txt"), b"source committed bytes").unwrap();
        Box::pin(lore_revision::file::stage::stage(
            repository.clone(),
            &token,
            LoreArray::from_vec(vec![LoreString::from(&path.join("original.txt"))]),
            lore_revision::stage::StageOptions {
                scan: true,
                ..Default::default()
            },
        ))
        .await
        .unwrap();
        let revision = lore_revision::commit::commit_boxed(
            repository.clone(),
            &token,
            lore_revision::commit::CommitOptions::new("factory source".into()),
        )
        .await
        .unwrap();
        assert!(!revision.is_zero());
        repository.flush(true).await.unwrap();
        let metadata = ["id", "instance", "config.toml"]
            .into_iter()
            .map(|name| {
                let path = path.join(".lore").join(name);
                let bytes = std::fs::read(&path).unwrap();
                (path, bytes)
            })
            .collect();
        let unlocked = Arc::new(repository.without_command_locks());
        drop(repository);
        drop(token);
        Self {
            path,
            repository: unlocked,
            revision,
            metadata,
        }
    }
    async fn request(&self, fixture: &Fixture) -> BootstrapRequest {
        let seed = lore_revision::repository::working_bootstrap::ImmutableImport::new(
            self.repository.clone(),
            self.revision,
        )
        .await
        .unwrap();
        BootstrapRequest {
            registration: fixture.registration.as_ref().unwrap().clone(),
            mode: BootstrapMode::Import {
                backing: fixture.backing.clone(),
                config: RepositoryConfig {
                    vfs: Some(VfsConfig {
                        vfs_type: VfsType::ExperimentalWorking,
                    }),
                    ..Default::default()
                },
                selection: Selection {
                    branch: [214; 16].into(),
                    branch_name: "main".into(),
                    revision: self.revision,
                },
                seed,
            },
        }
    }
    async fn assert_unchanged(&self) {
        assert_eq!(
            std::fs::read(self.path.join("original.txt")).unwrap(),
            b"source committed bytes"
        );
        for (path, bytes) in &self.metadata {
            assert_eq!(&std::fs::read(path).unwrap(), bytes);
        }
        assert_eq!(
            lore_revision::instance::load_current_anchor_boxed(&self.repository)
                .await
                .unwrap()
                .0,
            self.revision
        );
    }
}

#[tokio::test]
async fn populated_normal_commit_import_edit_checkpoint_and_reuse_preserve_source() {
    Box::pin(
        LORE_CONTEXT.scope(super::working_mount::execution(), async {
            let mut fixture = Fixture::new();
            let source = Box::pin(Source::new(&fixture)).await;
            for fresh in [true, false] {
                let request = if fresh {
                    source.request(&fixture).await
                } else {
                    fixture.request(false)
                };
                let (pause, model, resume) = pause();
                let supervisor = fixture.supervisor.clone();
                let mut preparing = Box::pin(supervisor.test_bootstrap_prepare(
                    request,
                    policy(),
                    OwnerLimits::default(),
                    Default::default(),
                    pause,
                ));
                let model = tokio::select! {
                    model=bounded(model)=>model.unwrap(),
                    result=&mut preparing=>panic!("populated factory ended early: {result:?}"),
                };
                let attrs = model.lookup(1, b"original.txt").await.unwrap();
                let file = model
                    .open(
                        attrs.entry.inode,
                        OpenFlags {
                            write: true,
                            append: false,
                            truncate: false,
                        },
                    )
                    .await
                    .unwrap();
                let expected: &[u8] = if fresh {
                    b"source committed bytes"
                } else {
                    b"private target changes"
                };
                assert_eq!(model.read(file, 0, 100).await.unwrap(), expected);
                if fresh {
                    model
                        .write(file, 0, b"private target changes")
                        .await
                        .unwrap();
                    model.fsync(file, false).await.unwrap();
                }
                model.release(file).await.unwrap();
                drop(model);
                resume.send(()).unwrap();
                let handle = bounded(preparing).await.unwrap();
                remove(&fixture, &handle).await;
                source.assert_unchanged().await;
                if fresh {
                    fixture.register_again();
                }
            }
        }),
    )
    .await;
}

#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Linux /dev/fuse and unprivileged mount support"]
async fn mounted_factory_imports_real_commit_then_os_edit_fsync_and_reuse_preserve_source() {
    Box::pin(
        LORE_CONTEXT.scope(super::working_mount::execution(), async {
            let mut fixture = Fixture::new();
            let source = Box::pin(Source::new(&fixture)).await;
            let first = bounded(fixture.supervisor.bootstrap_mount(
                source.request(&fixture).await,
                policy(),
                OwnerLimits::default(),
                Default::default(),
            ))
            .await
            .unwrap();
            let path = fixture.target.join("original.txt");
            assert_eq!(
                bounded(tokio::fs::read(&path)).await.unwrap(),
                b"source committed bytes"
            );
            let file = bounded(async {
                use tokio::io::AsyncWriteExt;
                let mut file = tokio::fs::OpenOptions::new()
                    .write(true)
                    .truncate(true)
                    .open(&path)
                    .await
                    .unwrap();
                file.write_all(b"target private bytes").await.unwrap();
                file.sync_all().await.unwrap();
                file
            })
            .await;
            drop(file);
            assert_private_bytes_have_no_immutable_hash(&path);
            remove(&fixture, &first).await;
            source.assert_unchanged().await;
            fixture.register_again();
            let second = bounded(fixture.supervisor.bootstrap_mount(
                fixture.request(false),
                policy(),
                OwnerLimits::default(),
                Default::default(),
            ))
            .await
            .unwrap();
            assert_eq!(
                bounded(tokio::fs::read(&path)).await.unwrap(),
                b"target private bytes"
            );
            assert_private_bytes_have_no_immutable_hash(&path);
            remove(&fixture, &second).await;
            source.assert_unchanged().await;
        }),
    )
    .await;
}

mod policy;
mod remote;

#[tokio::test]
async fn prepublication_failure_with_successful_cleanup_exits_worker_and_releases_owners() {
    Box::pin(
        LORE_CONTEXT.scope(super::working_mount::execution(), async {
            let fixture = Fixture::new();
            let (pause, model, resume) = pause();
            let supervisor = fixture.supervisor.clone();
            let mut preparing = Box::pin(supervisor.test_bootstrap_prepare(
                fixture.request(true),
                policy(),
                OwnerLimits::default(),
                Default::default(),
                pause,
            ));
            let model = tokio::select! {
                model = bounded(model) => model.unwrap(),
                result = &mut preparing => panic!("prepare ended before pause: {result:?}"),
            };
            drop(model);
            drop(resume);
            let failure = bounded(preparing).await.unwrap_err();
            assert!(matches!(failure.error, Error::Closed));
            let handle = failure.handle.unwrap();
            remove(&fixture, &handle).await;
            let storage = lore_vfs::working_storage::WorkingStorage::open_existing(
                &fixture.backing,
                &fixture.target,
                fixture.repository,
                fixture.instance,
                Default::default(),
            )
            .unwrap();
            let recovery =
                Recovery::open_existing(fixture.recovery(), OwnerLimits::default().recovery)
                    .unwrap();
            let token = bounded(RepositoryWriteToken::acquire(&fixture.target)).await;
            drop(token);
            drop(recovery);
            drop(storage);
        }),
    )
    .await;
}
