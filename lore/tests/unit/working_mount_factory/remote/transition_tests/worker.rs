// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

use lore_revision::fs::provider_registry::CanonicalTarget;
use lore_revision::fs::provider_registry::ProviderResolver;
use lore_vfs::checkpoint_owner::TransitionTestPause;
use lore_vfs::checkpoint_owner::TransitionTestStep;

use super::*;

struct Release(Arc<TransitionTestPause>);
impl Drop for Release {
    fn drop(&mut self) {
        self.0.release();
    }
}

async fn wait_publications(model: &Arc<lore_vfs::linux_working::Model>) {
    bounded(async {
        while model.checkpoint_owner().test_transition_active() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await;
}

async fn run(step: TransitionTestStep, fail: bool, cancel: bool) {
    let execution = Arc::new(
        lore_revision::interface::ExecutionContext::new_client_with_user_id(
            LoreGlobalArgs::default(),
            lore_revision::relay::EventDispatcher::no_dispatch(),
            "worker-lifecycle".into(),
        ),
    );
    Box::pin(LORE_CONTEXT.scope(execution, async {
        let mut fixture = Fixture::new();
        let mut history = Box::pin(History::new(&fixture)).await;
        drop(fixture.registration.take());
        let mut payloads = Vec::new();
        for pin in [history.first, history.second, history.third] {
            let state = lore_revision::state::State::deserialize(history.source.repository.clone(), pin).await.unwrap();
            payloads.push(state.find_node(history.source.repository.clone(), "original.txt").await.unwrap().address.hash);
        }
        let server = RemoteFixture::new(history.source.repository.clone(), payloads);
        let config = RepositoryConfig {
            remote_url: Some(server.url().to_owned()), identity: Some("fixture".into()), ..Default::default()
        };
        std::fs::write(history.source.path.join(".lore/config.toml"), toml::to_string(&config).unwrap()).unwrap();
        history.record_metadata();
        let (owner, _guard, _admission) = super::super::super::super::working_mount_local_policy::admitted().await;
        let _cleanup = RunCleanup::new(owner.clone());
        let fresh = lore::mount_working::LoreMountWorkingArgs {
            experimental: true, remote_contents: true, source: history.source.path.as_path().into(),
            revision: format!("main@{}", history.first).into(), mountpoint: fixture.target.as_path().into(),
            metadata: fixture.metadata.as_path().into(), backing: fixture.backing.as_path().into(), ..Default::default()
        };
        assert_eq!(command(&owner, LoreGlobalArgs::default(), LoreCommand::MountWorking(fresh), None).await, 0);
        let instances = owner._supervisor.instances().unwrap();
        assert_eq!(instances.len(), 1);
        let model = owner._supervisor.test_instance_model(&instances[0].0).unwrap();
        let pause = model.checkpoint_owner().test_transition_pause(step, fail);
        let _release = Release(pause.clone());
        let globals = LoreGlobalArgs { repository_path: fixture.target.as_path().into(), ..Default::default() };
        let sync_owner = owner.clone();
        let sync_globals = globals.clone();
        let target = history.second;
        let mut task = lore_base::lore_spawn!(async move { sync(&sync_owner, sync_globals, target, false, false).await });
        tokio::select! {
            _ = bounded(pause.entered()) => {},
            result = &mut task => panic!("actual transition exited before requested phase: {result:?}"),
        }
        let location = owner.state.provider_registry().locate(&CanonicalTarget::resolve(&fixture.target).unwrap()).unwrap().unwrap();
        assert!(owner.state.provider_registry().resolve_bootstrap(&location).is_err(), "ordinary facade admission must remain excluded through the actual join");
        if cancel {
            assert_ne!(model.checkpoint_owner().test_transition_active(), 0);
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
            // Caller cancellation does not remove the durable record or free
            // the command root and Model publisher resources prematurely.
            assert!(fixture.backing.join("recovery/transition/intent").is_file());
            assert_ne!(model.checkpoint_owner().test_transition_active(), 0);
            pause.release();
            wait_publications(&model).await;
        } else {
            pause.release();
            let result = bounded(task).await.unwrap();
            if fail { assert_ne!(result, 0); } else { assert_eq!(result, 0); }
            wait_publications(&model).await;
        }
        if fail {
            assert!(owner.state.provider_registry().resolve_location(&location).is_err(), "error after intent must withdraw exact READY routing");
            assert!(model.getattr(1).await.is_err(), "quarantined Model cannot resume ordinary access");
            if step == TransitionTestStep::AfterAnchors {
                assert!(fixture.backing.join("recovery/transition/intent").is_file());
            }
        } else {
            assert!(owner.state.provider_registry().resolve_bootstrap(&location).is_ok());
            assert_eq!(tokio::fs::read(fixture.target.join("original.txt")).await.unwrap(), b"target second bytes");
            assert!(!fixture.backing.join("recovery/transition").exists());
        }
        drop(model);
        stop(&owner).await;
        history.assert_source_unchanged().await;
        if step == TransitionTestStep::AfterAnchors {
            let (reopened, _reopened_guard, _reopened_admission) = super::super::super::super::working_mount_local_policy::admitted().await;
            let _cleanup = RunCleanup::new(reopened.clone());
            let reuse = lore::mount_working::LoreMountWorkingArgs {
                experimental: true, remote_contents: true, reuse: true,
                source: history.source.path.as_path().into(), mountpoint: fixture.target.as_path().into(),
                metadata: fixture.metadata.as_path().into(), ..Default::default()
            };
            assert_eq!(command(&reopened, LoreGlobalArgs::default(), LoreCommand::MountWorking(reuse), None).await, 0);
            assert_eq!(tokio::fs::read(fixture.target.join("original.txt")).await.unwrap(), b"target second bytes");
            assert!(!fixture.backing.join("recovery/transition").exists());
            stop(&reopened).await;
        }
    })).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Linux /dev/fuse and unprivileged mount support"]
async fn cancellation_after_intent_retains_owned_worker_until_actual_completion() {
    Box::pin(run(TransitionTestStep::AfterIntent, false, true)).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Linux /dev/fuse and unprivileged mount support"]
async fn successful_delivery_keeps_facades_excluded_until_actual_thread_join() {
    Box::pin(run(TransitionTestStep::BeforeThreadExit, false, false)).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Linux /dev/fuse and unprivileged mount support"]
async fn error_after_anchors_quarantines_and_reuse_reconciles_exact_candidate() {
    Box::pin(run(TransitionTestStep::AfterAnchors, true, false)).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Linux /dev/fuse and unprivileged mount support"]
async fn publisher_destructor_panic_withdraws_ready_after_actions_complete() {
    Box::pin(run(TransitionTestStep::PublisherDestructor, true, false)).await;
}
