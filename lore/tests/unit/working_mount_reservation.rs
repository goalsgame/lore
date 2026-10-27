// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
// Compatibility inputs do not prove earlier bootstrap recovery admission.
#![cfg(unix)]
#![allow(clippy::disallowed_methods)]
use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use lore::working_mount::Error;
use lore::working_mount::Limits;
use lore::working_mount::Phase;
use lore::working_mount::Supervisor;
use lore_base::runtime::LORE_CONTEXT;
use lore_base::test_util::TempDir;
use lore_io::lease::IoLease;
use lore_revision::fs::provider_registry::ProviderRegistry;
use lore_revision::fs::provider_registry::Registration;
use lore_revision::instance::InstanceId;
use lore_revision::repository::RepositoryConfig;
use lore_revision::repository::RepositoryWriteToken;
use lore_revision::repository::VfsConfig;
use lore_revision::repository::VfsType;
use lore_revision::repository::working_bootstrap::MetadataInitializer;
use lore_revision::repository::working_bootstrap::Mode;

struct Fixture {
    _root: TempDir,
    registration: Arc<Registration>,
    backing: PathBuf,
    metadata: PathBuf,
}
impl Fixture {
    fn new(registry: &Arc<ProviderRegistry>) -> Self {
        use std::os::unix::fs::DirBuilderExt;
        let root = TempDir::new("working-preinput-reservation-");
        let target = root.join("target");
        let metadata = root.join("metadata");
        std::fs::create_dir(&target).unwrap();
        std::fs::DirBuilder::new()
            .mode(0o700)
            .create(&metadata)
            .unwrap();
        let registration = Arc::new(registry.reserve(&target, InstanceId::generate()).unwrap());
        registration
            .reserve_metadata(&metadata, [231; 16].into())
            .unwrap();
        Self {
            backing: root.join("backing"),
            metadata,
            _root: root,
            registration,
        }
    }
}
#[tokio::test]
async fn preinput_reservation_refuses_overlap_and_bad_metadata_before_creation() {
    use std::os::unix::fs::PermissionsExt;
    let registry = ProviderRegistry::new(Default::default());
    let supervisor = Supervisor::new(registry.clone(), Limits::default()).unwrap();
    let fixture = Fixture::new(&registry);
    for backing in [
        fixture.metadata.join("inside"),
        fixture.registration.target().path().join("inside"),
    ] {
        assert!(matches!(
            supervisor.reserve_bootstrap(fixture.registration.clone(), &backing),
            Err(Error::Overlap)
        ));
        assert!(!backing.exists());
    }
    std::fs::set_permissions(&fixture.metadata, std::fs::Permissions::from_mode(0o755)).unwrap();
    assert!(
        supervisor
            .reserve_bootstrap(fixture.registration.clone(), &fixture.backing)
            .is_err()
    );
    assert!(!fixture.backing.exists());
    assert_eq!(std::fs::read_dir(&fixture.metadata).unwrap().count(), 0);
    assert!(supervisor.instances().unwrap().is_empty());
}
#[tokio::test]
async fn preinput_promotion_preserves_same_capacity_slot_and_handle() {
    LORE_CONTEXT.scope(super::working_mount::execution(),async {
        let registry=ProviderRegistry::new(Default::default());
        let supervisor=Supervisor::new(registry.clone(),Limits {max_instances:1}).unwrap();
        let root=TempDir::new("working-preinput-promote-");
        let input=super::working_mount::input(&root,&registry).await;
        let reserved=supervisor.reserve_bootstrap(input.prepared.registration.clone(),input.storage.root()).unwrap();
        let handle=reserved.handle();
        let second=TempDir::new("working-preinput-capacity-");
        let other=super::working_mount::input(&second,&registry).await;
        assert!(matches!(supervisor.reserve_startup(other),Err(rejected) if matches!(rejected.error,Error::Capacity)));
        let ticket=match reserved.promote(input) {Ok(ticket)=>ticket,Err(error)=>panic!("exact promotion refused: {:?}", error.error)};
        assert_eq!(ticket.handle().id(),handle.id());
        assert_eq!(supervisor.instances().unwrap().len(),1);
        drop(ticket);
        supervisor.retire_unstarted(&handle).await.unwrap();
        assert!(supervisor.instances().unwrap().is_empty());
    }).await;
}
#[tokio::test]
async fn preinput_promotion_refuses_foreign_input_and_terminal_admission() {
    LORE_CONTEXT
        .scope(super::working_mount::execution(), async {
            for stop in 0..3 {
                let registry = ProviderRegistry::new(Default::default());
                let supervisor = Supervisor::new(registry.clone(), Limits::default()).unwrap();
                let root = TempDir::new("working-preinput-wrong-");
                let input = super::working_mount::input(&root, &registry).await;
                let reserved = supervisor
                    .reserve_bootstrap(input.prepared.registration.clone(), input.storage.root())
                    .unwrap();
                let handle = reserved.handle();
                let other = TempDir::new("working-preinput-foreign-");
                let promoted = if stop != 0 {
                    if stop == 1 {
                        supervisor.close_startup_admission().unwrap();
                    } else {
                        supervisor.request_retirement(&handle).unwrap();
                    }
                    input
                } else {
                    let foreign = super::working_mount::input(&other, &registry).await;
                    // Both contexts retain their own real identity; no fabricated input.
                    drop(input);
                    foreign
                };
                let error = match reserved.promote(promoted) {
                    Err(error) => error,
                    Ok(_) => panic!("invalid promotion accepted"),
                };
                assert_eq!(error.handle.id(), handle.id());
                assert!(matches!(error.error, Error::Closed | Error::InvalidBinding));
                drop(error);
                assert_eq!(supervisor.status(&handle).unwrap().phase, Phase::Retiring);
                supervisor.retire_unstarted(&handle).await.unwrap();
            }
        })
        .await;
}
struct Resume(Option<std::sync::mpsc::Sender<()>>);
impl Drop for Resume {
    fn drop(&mut self) {
        if let Some(sender) = self.0.take() {
            let _closed = sender.send(());
        }
    }
}
#[test]
fn cancelled_actual_metadata_job_retains_preinput_record_until_its_ticket_settles() {
    let registry = ProviderRegistry::new(Default::default());
    let supervisor = Supervisor::new(registry.clone(), Limits { max_instances: 1 }).unwrap();
    let fixture = Fixture::new(&registry);
    let reservation = supervisor
        .reserve_bootstrap(fixture.registration.clone(), &fixture.backing)
        .unwrap();
    let handle = reservation.handle();
    let owner = Arc::new(reservation);
    let weak = Arc::downgrade(&owner);
    let lease = IoLease::new(owner);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()
        .unwrap();
    let (entered, entering) = std::sync::mpsc::channel();
    let (resume, resuming) = std::sync::mpsc::channel();
    let resume = Resume(Some(resume));
    runtime.spawn_blocking(move || {
        entered.send(()).unwrap();
        resuming.recv().unwrap();
    });
    entering.recv_timeout(Duration::from_secs(10)).unwrap();
    let token = runtime.block_on(RepositoryWriteToken::acquire(
        fixture.registration.target().path(),
    ));
    let initializer = MetadataInitializer::new(1).unwrap();
    let registration = fixture.registration.clone();
    let mut command = Box::pin(lease.scope(initializer.prepare(
        registration,
        token,
        Mode::CreateNew(RepositoryConfig {
            vfs: Some(VfsConfig {
                vfs_type: VfsType::ExperimentalWorking,
            }),
            ..Default::default()
        }),
    )));
    // prepare has no suspension before submitting its blocking job; this first
    // pending poll proves it is queued behind the channel-held actual worker.
    runtime.block_on(async {
        assert!(matches!(
            std::future::poll_fn(|cx| std::task::Poll::Ready(command.as_mut().poll(cx))).await,
            std::task::Poll::Pending
        ));
    });
    supervisor.cancel_startup(&handle).unwrap();
    drop(command);
    drop(lease);
    runtime.shutdown_timeout(Duration::ZERO);
    assert!(weak.upgrade().is_some());
    assert_eq!(supervisor.instances().unwrap().len(), 1);
    let observer = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    assert!(matches!(
        observer.block_on(supervisor.retire_unstarted(&handle)),
        Err(Error::Busy)
    ));
    let mut drain = Box::pin(supervisor.close_and_drain());
    observer.block_on(async {
        assert!(matches!(
            std::future::poll_fn(|cx| std::task::Poll::Ready(drain.as_mut().poll(cx))).await,
            std::task::Poll::Pending
        ));
    });
    assert_eq!(std::fs::read_dir(&fixture.metadata).unwrap().count(), 0);
    assert!(!fixture.backing.exists());
    drop(resume);
    observer.block_on(async {
        let report = tokio::time::timeout(Duration::from_secs(10), drain)
            .await
            .unwrap()
            .unwrap();
        assert!(report.failures.is_empty());
    });
    assert!(weak.upgrade().is_none());
    assert!(supervisor.instances().unwrap().is_empty());
}

#[tokio::test]
async fn preinput_reservations_share_existing_record_and_cache_overlap_checks() {
    LORE_CONTEXT
        .scope(super::working_mount::execution(), async {
            let registry = ProviderRegistry::new(Default::default());
            let supervisor = Supervisor::new(registry.clone(), Limits::default()).unwrap();
            let root = TempDir::new("working-preinput-cache-");
            let input = super::working_mount::input(&root, &registry).await;
            let cache = root.join("digest-cache");
            let prepared = supervisor
                .prepare_instance_with_options(
                    input,
                    Arc::new(super::working_mount::ExplicitEdits),
                    Default::default(),
                    lore::working_mount::Options {
                        digest_cache: Some(cache.clone()),
                    },
                )
                .await
                .unwrap();
            let handle = prepared.handle();
            let fixture = Fixture::new(&registry);
            for path in [root.join("backing/nested"), cache.join("nested")] {
                assert!(matches!(
                    supervisor.reserve_bootstrap(fixture.registration.clone(), &path),
                    Err(Error::Overlap)
                ));
                assert!(!path.exists());
            }
            assert_eq!(supervisor.instances().unwrap().len(), 1);
            assert_eq!(std::fs::read_dir(&fixture.metadata).unwrap().count(), 0);
            drop(prepared);
            supervisor.retire_unstarted(&handle).await.unwrap();
        })
        .await;
}

#[tokio::test]
async fn recovery_admission_cannot_be_duplicated_after_promotion() {
    LORE_CONTEXT
        .scope(super::working_mount::execution(), async {
            let registry = ProviderRegistry::new(Default::default());
            let supervisor = Supervisor::new(registry.clone(), Limits::default()).unwrap();
            let root = TempDir::new("working-preinput-exclusive-");
            let (prepared, storage) = super::working_mount::input_parts(&root, &registry).await;
            let duplicate = lore_revision::repository::working_bootstrap::PreparedWorkingContext {
                execution: prepared.execution.clone(),
                registration: prepared.registration.clone(),
                location: prepared.location.clone(),
                repository: prepared.repository.clone(),
                selection: prepared.selection.clone(),
            };
            let input = lore::working_mount::StartupInput::test_admit_prepared(
                prepared,
                storage.clone(),
                lore::working_mount::OwnerLimits::default().recovery,
            )
            .unwrap();
            let weak = Arc::downgrade(&storage);
            let reserved = supervisor
                .reserve_bootstrap(input.prepared.registration.clone(), input.storage.root())
                .unwrap();
            let handle = reserved.handle();
            let ticket = match reserved.promote(input) {
                Ok(ticket) => ticket,
                Err(error) => panic!("promotion refused: {:?}", error.error),
            };
            assert!(
                lore::working_mount::StartupInput::test_admit_prepared(
                    duplicate,
                    storage.clone(),
                    lore::working_mount::OwnerLimits::default().recovery
                )
                .is_err()
            );
            assert!(matches!(
                lore_vfs::recovery::Recovery::open_existing(
                    storage.recovery_directory(),
                    Default::default()
                ),
                Err(lore_vfs::recovery::Error::Busy)
            ));
            assert_eq!(ticket.handle().id(), handle.id());
            drop(storage);
            drop(ticket);
            assert!(weak.upgrade().is_some());
            supervisor.retire_unstarted(&handle).await.unwrap();
            assert!(weak.upgrade().is_none());
        })
        .await;
}

#[tokio::test]
async fn cache_preparation_checks_a_preinput_record_without_requiring_its_input() {
    LORE_CONTEXT
        .scope(super::working_mount::execution(), async {
            let registry = ProviderRegistry::new(Default::default());
            let supervisor = Supervisor::new(registry.clone(), Limits::default()).unwrap();
            let fixture = Fixture::new(&registry);
            let reserved = supervisor
                .reserve_bootstrap(fixture.registration.clone(), &fixture.backing)
                .unwrap();
            let reserved_handle = reserved.handle();
            let root = TempDir::new("working-preinput-cache-reverse-");
            let input = super::working_mount::input(&root, &registry).await;
            let result = supervisor
                .prepare_instance_with_options(
                    input,
                    Arc::new(super::working_mount::ExplicitEdits),
                    Default::default(),
                    lore::working_mount::Options {
                        digest_cache: Some(fixture.backing.clone()),
                    },
                )
                .await;
            let handle = match result {
                Err(lore::working_mount::PreparationError::Retiring {
                    handle,
                    error: Error::Overlap,
                }) => handle,
                _ => panic!("cache path did not find reserved pre-input backing root"),
            };
            assert!(!fixture.backing.exists());
            assert_eq!(
                supervisor.status(&reserved_handle).unwrap().phase,
                Phase::Starting
            );
            supervisor.retire_unstarted(&handle).await.unwrap();
            drop(reserved);
            supervisor.retire_unstarted(&reserved_handle).await.unwrap();
        })
        .await;
}
