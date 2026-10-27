// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
#![cfg(target_os = "linux")]
#![allow(clippy::disallowed_methods)]

use std::sync::Arc;

use lore::remote::service_process::ServiceStartGuard;
use lore::remote::service_process::StopRequest;
use lore::remote::service_process::begin_service_start_for;
use lore::remote::service_process::register_service_process;
use lore::service::owner::RunCleanup;
use lore::service::owner::ServiceOwner;
use lore::service::state::ServiceStateImpl;
use lore::service::state::with_invocation;
use lore::service::state::with_owner;
use lore::working_mount::Error;
use lore::working_mount::PreparationError;
use lore::working_mount::StartupInput;
use lore_base::runtime::LORE_CONTEXT;
use lore_base::test_util::TempDir;
use lore_revision::fs::provider_registry::ProviderResolver;

use super::working_mount::ExplicitEdits;
use super::working_mount::execution;
use super::working_mount::input;

fn owner() -> (Arc<ServiceOwner>, ServiceStartGuard, RunCleanup) {
    let state = Arc::new(ServiceStateImpl::new());
    let owner = ServiceOwner::for_state(state).unwrap();
    let guard = begin_service_start_for(Arc::new(StopRequest::new())).unwrap();
    assert!(guard.bind_owner(&owner));
    register_service_process(&guard);
    let cleanup = RunCleanup::new(owner.clone());
    (owner, guard, cleanup)
}
fn rejected(
    result: Result<lore::working_mount::InstanceHandle, PreparationError>,
    expected: fn(&Error) -> bool,
) -> StartupInput {
    match result {
        Err(PreparationError::Rejected(rejected)) => {
            assert!(expected(&rejected.error), "{}", rejected.error);
            *rejected.input
        }
        _ => panic!("startup unexpectedly admitted prepared resources"),
    }
}
#[tokio::test]
async fn startup_requires_exact_owner_work_scope_and_preserves_rejected_input() {
    LORE_CONTEXT
        .scope(execution(), async {
            let root = TempDir::new("lore-owned-start-scope-");
            let (owner, _guard, _cleanup) = owner();
            let input = input(&root, owner.state.provider_registry()).await;
            let storage = input.storage.clone();
            let input = rejected(
                owner
                    .start_working(
                        input,
                        Arc::new(ExplicitEdits),
                        Default::default(),
                        Default::default(),
                    )
                    .await,
                |error| matches!(error, Error::WrongOwner),
            );
            let input = rejected(
                owner
                    .start_working_local(input, Default::default(), Default::default())
                    .await,
                |error| matches!(error, Error::WrongOwner),
            );
            let control = owner.state.try_begin_control_invocation().unwrap();
            let input = rejected(
                with_invocation(
                    control,
                    with_owner(owner.clone(), async {
                        owner
                            .start_working_local(input, Default::default(), Default::default())
                            .await
                    }),
                )
                .await
                .unwrap(),
                |error| matches!(error, Error::WrongOwner),
            );
            let work = owner.state.try_begin_invocation().unwrap();
            let input = rejected(
                with_invocation(work, async {
                    owner
                        .start_working(
                            input,
                            Arc::new(ExplicitEdits),
                            Default::default(),
                            Default::default(),
                        )
                        .await
                })
                .await,
                |error| matches!(error, Error::WrongOwner),
            );
            assert_eq!(storage.retained_sessions().unwrap(), 0);
            assert!(owner._supervisor.instances().unwrap().is_empty());
            assert!(Arc::ptr_eq(&input.storage, &storage));
        })
        .await;
}
#[tokio::test]
async fn foreign_scope_or_registry_cannot_adopt_prepared_resources() {
    LORE_CONTEXT
        .scope(execution(), async {
            let root = TempDir::new("lore-owned-start-foreign-");
            let (first, _first_guard, _first_cleanup) = owner();
            let (second, _second_guard, _second_cleanup) = owner();
            let input = input(&root, first.state.provider_registry()).await;
            let storage = input.storage.clone();
            let work = second.state.try_begin_invocation().unwrap();
            let input = rejected(
                with_invocation(
                    work,
                    with_owner(second.clone(), async {
                        first
                            .start_working(
                                input,
                                Arc::new(ExplicitEdits),
                                Default::default(),
                                Default::default(),
                            )
                            .await
                    }),
                )
                .await
                .unwrap(),
                |error| matches!(error, Error::WrongOwner),
            );
            let work = second.state.try_begin_invocation().unwrap();
            let input = rejected(
                with_invocation(
                    work,
                    with_owner(second.clone(), async {
                        second
                            .start_working(
                                input,
                                Arc::new(ExplicitEdits),
                                Default::default(),
                                Default::default(),
                            )
                            .await
                    }),
                )
                .await
                .unwrap(),
                |error| matches!(error, Error::InvalidBinding),
            );
            assert_eq!(storage.retained_sessions().unwrap(), 0);
            assert!(Arc::ptr_eq(&input.storage, &storage));
            assert!(first._supervisor.instances().unwrap().is_empty());
            assert!(second._supervisor.instances().unwrap().is_empty());
        })
        .await;
}
#[tokio::test]
async fn admitted_work_cannot_start_after_terminal_owner_retirement() {
    LORE_CONTEXT
        .scope(execution(), async {
            let root = TempDir::new("lore-owned-start-closed-");
            let (owner, _guard, _cleanup) = owner();
            let input = input(&root, owner.state.provider_registry()).await;
            let storage = input.storage.clone();
            let work = owner.state.try_begin_invocation().unwrap();
            with_invocation(
                work,
                with_owner(owner.clone(), async {
                    owner._supervisor.request_retirement_all().unwrap();
                    tokio::task::yield_now().await;
                    rejected(
                        owner
                            .start_working(
                                input,
                                Arc::new(ExplicitEdits),
                                Default::default(),
                                Default::default(),
                            )
                            .await,
                        |error| matches!(error, Error::Closed),
                    );
                }),
            )
            .await
            .unwrap();
            assert_eq!(storage.retained_sessions().unwrap(), 0);
            assert!(owner._supervisor.instances().unwrap().is_empty());
        })
        .await;
}

#[tokio::test]
async fn stale_admitted_work_after_await_cannot_start_on_successor_owner() {
    LORE_CONTEXT
        .scope(execution(), async {
            let request = Arc::new(StopRequest::new());
            let state = Arc::new(ServiceStateImpl::new());
            let old = ServiceOwner::for_state(state).unwrap();
            let guard = begin_service_start_for(request.clone()).unwrap();
            assert!(guard.bind_owner(&old));
            register_service_process(&guard);
            let root = TempDir::new("lore-owned-start-stale-");
            let input = input(&root, old.state.provider_registry()).await;
            let storage = input.storage.clone();
            let work = old.state.try_begin_invocation().unwrap();
            with_invocation(
                work,
                with_owner(old.clone(), async {
                    drop(guard);
                    let new = ServiceOwner::for_state(Arc::new(ServiceStateImpl::new())).unwrap();
                    let _new_cleanup = RunCleanup::new(new.clone());
                    let new_guard = begin_service_start_for(request).unwrap();
                    assert!(new_guard.bind_owner(&new));
                    register_service_process(&new_guard);
                    tokio::task::yield_now().await;
                    rejected(
                        old.start_working(
                            input,
                            Arc::new(ExplicitEdits),
                            Default::default(),
                            Default::default(),
                        )
                        .await,
                        |error| matches!(error, Error::Closed),
                    );
                    assert!(new._supervisor.instances().unwrap().is_empty());
                    assert!(new.state.try_begin_invocation().is_some());
                }),
            )
            .await
            .unwrap();
            assert_eq!(storage.retained_sessions().unwrap(), 0);
            assert!(old._supervisor.instances().unwrap().is_empty());
        })
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Linux /dev/fuse and unprivileged mount support"]
async fn mounted_exact_service_work_adopts_commits_and_stops_owned_mount() {
    use std::time::Duration;

    use lore_revision::interface::LoreArray;
    use lore_revision::interface::LoreString;
    let state = Arc::new(ServiceStateImpl::new());
    let resolver: Arc<dyn ProviderResolver> = state.provider_registry().clone();
    let mut context = execution();
    Arc::get_mut(&mut context)
        .unwrap()
        .set_provider_resolver(&resolver);
    Box::pin(LORE_CONTEXT.scope(context, async {
        let root = TempDir::new("lore-service-owned-mount-");
        // Normal stores are prepared before service initialization sets its
        // background-store flush policy. No mount is exposed by preparation.
        let input = input(&root, state.provider_registry()).await;
        let location = input.prepared.location.clone();
        let instance = location.instance();
        let service = tokio::spawn(lore::service::service_main::service_main(
            lore::interface::LoreGlobalArgs {
                offline: 1,
                ..Default::default()
            },
            state.clone(),
        ));
        let owner = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if lore::remote::service_process::service_runs_in_this_process()
                    && let Some(owner) = lore::remote::service_process::current_service_owner()
                {
                    break owner;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let work = state.try_begin_invocation().unwrap();
        let handle = Box::pin(with_invocation(
            work,
            with_owner(owner.clone(), async {
                let handle = tokio::time::timeout(
                    Duration::from_secs(20),
                    owner.start_working_local(input, Default::default(), Default::default()),
                )
                .await
                .unwrap()
                .unwrap();
                assert_eq!(
                    owner._supervisor.status(&handle).unwrap().phase,
                    lore::working_mount::Phase::Ready
                );
                let published = state
                    .provider_registry()
                    .resolve_location(&location)
                    .unwrap();
                tokio::fs::write(root.join("mount/selected"), b"service committed bytes")
                    .await
                    .unwrap();
                tokio::fs::write(root.join("mount/unselected"), b"retained dirty bytes")
                    .await
                    .unwrap();
                let repository = lore_revision::repository::load_and_connect(
                    root.join("mount"),
                    lore_revision::repository::RepositoryAccess::ReadWrite,
                )
                .await
                .unwrap();
                assert!(Arc::ptr_eq(&repository.file_system(), &published));
                let token = repository.try_write_token().unwrap();
                lore_revision::file::stage::stage(
                    repository.clone(),
                    token,
                    LoreArray::from_vec(vec![LoreString::from(&root.join("mount/selected"))]),
                    lore_revision::stage::StageOptions {
                        scan: true,
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
                let revision = tokio::time::timeout(
                    Duration::from_secs(30),
                    lore_revision::commit::commit_boxed(
                        repository.clone(),
                        token,
                        lore_revision::commit::CommitOptions::new("exact service work".into()),
                    ),
                )
                .await
                .unwrap()
                .unwrap();
                assert!(!revision.is_zero());
                assert_eq!(
                    tokio::fs::read(root.join("mount/selected")).await.unwrap(),
                    b"service committed bytes"
                );
                assert_eq!(
                    tokio::fs::read(root.join("mount/unselected"))
                        .await
                        .unwrap(),
                    b"retained dirty bytes"
                );
                drop(repository);
                drop(published);
                handle
            }),
        ))
        .await
        .unwrap();
        tokio::time::timeout(
            Duration::from_secs(30),
            owner._supervisor.retire_instance(&handle),
        )
        .await
        .unwrap()
        .unwrap();
        let restored_input =
            super::working_mount::reuse_input(&root, state.provider_registry(), instance).await;
        let restored_location = restored_input.prepared.location.clone();
        let work = state.try_begin_invocation().unwrap();
        let restored = Box::pin(with_invocation(
            work,
            with_owner(owner.clone(), async {
                let restored = tokio::time::timeout(
                    Duration::from_secs(20),
                    owner.start_working_local(
                        restored_input,
                        Default::default(),
                        Default::default(),
                    ),
                )
                .await
                .unwrap()
                .unwrap();
                assert_eq!(
                    tokio::fs::read(root.join("mount/selected")).await.unwrap(),
                    b"service committed bytes"
                );
                assert_eq!(
                    tokio::fs::read(root.join("mount/unselected"))
                        .await
                        .unwrap(),
                    b"retained dirty bytes"
                );
                use std::os::unix::ffi::OsStrExt;
                let path =
                    std::ffi::CString::new(root.join("mount/selected").as_os_str().as_bytes())
                        .unwrap();
                let name = std::ffi::CString::new(lore_vfs::digest::XATTR_NAME).unwrap();
                let mut digest = [0u8; 32];
                // SAFETY: The path/name are terminated, and the destination covers
                // exactly the advertised byte count.
                let size = unsafe {
                    libc::getxattr(
                        path.as_ptr(),
                        name.as_ptr(),
                        digest.as_mut_ptr().cast(),
                        digest.len(),
                    )
                };
                assert_eq!(size, 32);
                assert_eq!(
                    digest,
                    [
                        0xaf, 0xcd, 0xf7, 0xe2, 0x19, 0xbe, 0x48, 0x74, 0x2d, 0x3f, 0xee, 0xa7,
                        0x1c, 0xe8, 0xd1, 0x41, 0xd0, 0x41, 0x7d, 0x7e, 0x3d, 0x68, 0x62, 0x4c,
                        0x3d, 0x07, 0xd6, 0x23, 0x93, 0x1f, 0xea, 0xa6
                    ]
                );
                restored
            }),
        ))
        .await
        .unwrap();
        let run = lore::service::actor::current_run().unwrap();
        tokio::time::timeout(Duration::from_secs(30), run.request_and_wait_shutdown())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            owner._supervisor.status(&handle),
            Err(Error::Missing)
        ));
        assert!(matches!(
            owner._supervisor.status(&restored),
            Err(Error::Missing)
        ));
        assert!(
            state
                .provider_registry()
                .resolve_location(&restored_location)
                .is_err()
        );
        assert!(owner._supervisor.instances().unwrap().is_empty());
        assert!(
            state
                .provider_registry()
                .resolve_location(&location)
                .is_err()
        );
        service.await.unwrap().unwrap();
        assert!(lore::service::actor::current_run().is_none());
    }))
    .await;
}

struct PausedRead {
    entered: tokio::sync::Notify,
}
impl lore_vfs::overlay::AccessGate for PausedRead {
    fn authorize<'a>(
        &'a self,
        _key: &'a lore_vfs::core::NodeKey,
        _access: lore_vfs::overlay::Access,
    ) -> lore_vfs::core::BackendFuture<'a, ()> {
        Box::pin(async move {
            self.entered.notify_waiters();
            std::future::pending().await
        })
    }
}
async fn start_in_work(
    owner: Arc<ServiceOwner>,
    input: StartupInput,
    gate: Arc<dyn lore_vfs::overlay::AccessGate>,
) -> Result<lore::working_mount::InstanceHandle, PreparationError> {
    let work = owner.state.try_begin_invocation().unwrap();
    with_invocation(
        work,
        with_owner(owner.clone(), async move {
            owner
                .start_working(input, gate, Default::default(), Default::default())
                .await
        }),
    )
    .await
    .unwrap()
}
async fn stop_in_control(owner: Arc<ServiceOwner>) -> Result<(), lore::service::ServiceStopError> {
    let control = owner.state.try_begin_control_invocation().unwrap();
    with_invocation(
        control,
        with_owner(owner.clone(), async move { owner.stop_control(true).await }),
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn cancelled_owned_start_keeps_discoverable_record_until_actual_setup_drains() {
    use std::time::Duration;
    Box::pin(LORE_CONTEXT.scope(execution(), async {
        let root = TempDir::new("lore-service-owned-cancel-");
        let (owner, _guard, _cleanup) = owner();
        let input = input(&root, owner.state.provider_registry()).await;
        let location = input.prepared.location.clone();
        let gate = Arc::new(PausedRead { entered: tokio::sync::Notify::new() });
        let entered = gate.entered.notified();
        tokio::pin!(entered);
        entered.as_mut().enable();
        let mut starting = Box::pin(start_in_work(owner.clone(), input, gate.clone()));
        tokio::time::timeout(Duration::from_secs(10), async {
            tokio::select! {
                () = &mut entered => {},
                result = &mut starting => panic!("setup did not retain its policy wait: {result:?}"),
            }
        }).await.unwrap();
        drop(starting);
        let records = owner._supervisor.instances().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].1.phase, lore::working_mount::Phase::Retiring);
        assert!(owner.state.provider_registry().resolve_location(&location).is_err());
        let drained = tokio::time::timeout(Duration::from_secs(20), owner._supervisor.close_and_drain()).await.unwrap().unwrap();
        assert!(drained.failures.is_empty());
        assert!(owner._supervisor.instances().unwrap().is_empty());
        assert_eq!(owner.state.work_invocation_count(), 0);
    })).await;
}

#[tokio::test]
async fn forced_service_retirement_cancels_admitted_start_and_drains_without_self_ticket() {
    use std::time::Duration;
    Box::pin(LORE_CONTEXT.scope(execution(), async {
        let root = TempDir::new("lore-service-owned-retire-race-");
        let (owner, _guard, _cleanup) = owner();
        let input = input(&root, owner.state.provider_registry()).await;
        let location = input.prepared.location.clone();
        let gate = Arc::new(PausedRead { entered: tokio::sync::Notify::new() });
        let entered = gate.entered.notified();
        tokio::pin!(entered);
        entered.as_mut().enable();
        let mut starting = Box::pin(start_in_work(owner.clone(), input, gate.clone()));
        tokio::time::timeout(Duration::from_secs(10), async {
            tokio::select! {
                () = &mut entered => {},
                result = &mut starting => panic!("setup did not retain its policy wait: {result:?}"),
            }
        }).await.unwrap();
        let (started, stopped) = tokio::time::timeout(Duration::from_secs(20), async {
            tokio::join!(starting, stop_in_control(owner.clone()))
        }).await.unwrap();
        assert!(matches!(started, Err(PreparationError::Retiring { .. })));
        stopped.unwrap();
        assert!(owner._supervisor.instances().unwrap().is_empty());
        assert!(owner.state.provider_registry().resolve_location(&location).is_err());
        assert!(owner.state.try_begin_invocation().is_none());
        assert_eq!(owner.status().0, lore::service::owner::Phase::ReadyToExit);
    })).await;
}
