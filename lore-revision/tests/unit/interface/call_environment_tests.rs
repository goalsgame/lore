// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use std::any::Any;
use std::sync::Arc;

use lore_base::env::AUTH_PATH_VAR;
use lore_base::env::CallEnvironment;
use lore_base::env::GLOBAL_PATH_VAR;
use lore_base::runtime::LORE_CONTEXT;
use lore_revision::interface::ExecutionContext;
use lore_revision::relay::EventDispatcher;

fn context(environment: Option<CallEnvironment>) -> Arc<dyn Any + Send + Sync> {
    let context = ExecutionContext::new_client(Default::default(), EventDispatcher::new(None));
    Arc::new(match environment {
        Some(environment) => context.with_environment(environment),
        None => context,
    })
}

/// A relayed call reads its caller's values, an unset one included, rather than this process's.
#[tokio::test]
async fn a_relayed_call_reads_its_callers_environment() {
    let environment = CallEnvironment {
        values: [Some("/caller/global".to_string()), None],
    };

    let (global, auth) = LORE_CONTEXT
        .scope(context(Some(environment)), async {
            (
                lore_base::env::var(GLOBAL_PATH_VAR),
                lore_base::env::var(AUTH_PATH_VAR),
            )
        })
        .await;

    assert_eq!(global.as_deref(), Some("/caller/global"));
    assert_eq!(
        auth, None,
        "a variable the caller left unset is unset for the call"
    );
}

/// A call made in this process, and code outside any call, read this process's environment.
#[tokio::test]
async fn a_local_call_reads_this_processes_environment() {
    let in_call = LORE_CONTEXT
        .scope(context(None), async {
            lore_base::env::var(GLOBAL_PATH_VAR)
        })
        .await;

    assert_eq!(in_call, std::env::var(GLOBAL_PATH_VAR).ok());
    assert_eq!(
        lore_base::env::var(GLOBAL_PATH_VAR),
        std::env::var(GLOBAL_PATH_VAR).ok()
    );
}

/// A task a relayed call spawns runs under the call's context, so it reads the call's environment.
#[tokio::test]
async fn a_task_a_relayed_call_spawns_reads_its_environment() {
    let environment = CallEnvironment {
        values: [None, Some("/caller/auth".to_string())],
    };

    let auth = LORE_CONTEXT
        .scope(context(Some(environment)), async {
            lore_base::lore_spawn!(async { lore_base::env::var(AUTH_PATH_VAR) })
                .await
                .expect("the task must finish")
        })
        .await;

    assert_eq!(auth.as_deref(), Some("/caller/auth"));
}

#[tokio::test]
async fn background_owner_preserves_identity_without_retaining_command_state() {
    use lore_revision::fs::provider_registry::ProviderRegistry;
    use lore_revision::fs::provider_registry::ProviderResolver;
    use lore_revision::interface::LoreGlobalArgs;
    use lore_revision::logging::LoreLogLevel;

    let callback_owner = Arc::new(());
    let callback_weak = Arc::downgrade(&callback_owner);
    let dispatcher = EventDispatcher::new(Some(Box::new(move |_event| {
        let _retained = &callback_owner;
    })));
    let mut original = ExecutionContext::new_client_with_user_id(
        LoreGlobalArgs {
            identity: "owner-identity".into(),
            ..Default::default()
        },
        dispatcher,
        "owner-user".into(),
    )
    .with_environment(CallEnvironment {
        values: [None, Some("/owner/auth".into())],
    });
    original.log_level = LoreLogLevel::Trace;
    let caller = Arc::new(());
    let caller_weak = Arc::downgrade(&caller);
    original.set_caller_state(caller);
    let resolver: Arc<dyn ProviderResolver> = ProviderRegistry::new(Default::default());
    original.set_provider_resolver(&resolver);
    let original_stats = original.fragment_stats().clone();
    let owner = Arc::new(original.for_background_owner().await);
    assert_eq!(owner.globals(), original.globals());
    assert!(owner.is_client());
    assert_eq!(owner.user_id().await, "owner-user");
    assert_eq!(owner.log_level, LoreLogLevel::Trace);
    assert!(owner.caller_state().is_none());
    assert!(owner.dispatcher.weak_sender.is_none());
    assert!(owner.dispatcher.keep_open().is_none());
    assert!(!Arc::ptr_eq(owner.fragment_stats(), &original_stats));
    assert!(Arc::ptr_eq(&owner.provider_resolver().unwrap(), &resolver));
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        original.dispatcher.drain(),
    )
    .await
    .unwrap();
    drop(original);
    assert!(caller_weak.upgrade().is_none());
    assert!(callback_weak.upgrade().is_none());
    let environment = LORE_CONTEXT
        .scope(owner.clone(), async {
            (
                lore_base::env::var(GLOBAL_PATH_VAR),
                lore_base::env::var(AUTH_PATH_VAR),
            )
        })
        .await;
    assert_eq!(environment, (None, Some("/owner/auth".into())));
    drop(resolver);
    assert!(
        owner.provider_resolver().is_none(),
        "owner must retain only a weak resolver"
    );
}

#[tokio::test]
async fn background_owner_preserves_server_mode_and_has_independent_user_state() {
    let original = ExecutionContext::new_server(
        Default::default(),
        EventDispatcher::no_dispatch(),
        "server-user".into(),
    );
    let owner = original.for_background_owner().await;
    assert!(owner.is_server());
    assert_eq!(owner.user_id().await, "server-user");
    original.set_user_id("changed-command-user").await;
    assert_eq!(owner.user_id().await, "server-user");
}
