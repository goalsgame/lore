// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
#[cfg(all(unix, not(feature = "test-util")))]
pub(crate) mod actor;
#[cfg(all(unix, feature = "test-util"))]
pub mod actor;
pub mod initialization;
#[cfg(unix)]
pub mod owner;
pub mod service_main;
#[cfg(all(unix, not(feature = "test-util")))]
pub(crate) mod shutdown;
#[cfg(all(unix, feature = "test-util"))]
pub mod shutdown;
pub mod state;

use std::sync::Arc;

use lore_base::error::MountsBusy;
use lore_base::log::LoreLogLevel;
use lore_error_set::prelude::*;
use lore_macro::LoreArgs;
use lore_revision::event::LoreServiceMessageEventData;
use lore_revision::event::LoreServiceStatusEventData;
use lore_revision::fs::swfs::mount_manager_state::MountManagerState;
use lore_revision::global::GlobalConfig;
use lore_revision::interface::LoreGlobalArgs;
use lore_revision::lore::execution_context;
use lore_revision::lore_info;
use lore_revision::lore_warn;
use lore_revision::service_state::ServiceStateImpl;
use lore_revision::util::config::SaveableConfig;

use crate::call::no_repository_call;
use crate::interface::LoreEvent;
use crate::interface::LoreEventCallback;
use crate::interface::LoreString;
use crate::remote::call::service_call_over;
use crate::remote::service_process::ServiceProcessError;
use crate::remote::service_process::collect_exited_services;
use crate::remote::service_process::connect_or_spawn_service;
use crate::remote::service_process::connect_to_running_service;
use crate::remote::service_process::forget_whether_service_is_in_use;
use crate::remote::service_process::report_settings_update_that_will_not_relay;
#[cfg(not(unix))]
use crate::remote::service_process::request_service_stop;
use crate::remote::service_process::service_runs_in_this_process;
use crate::remote::service_process::wait_until_no_service_is_listening;
use crate::service::service_main::service_main;

#[repr(C)]
#[derive(Debug, Clone, PartialEq, Default, LoreArgs, bitcode::Encode, bitcode::Decode)]
#[handler(start_local)]
/// Arguments for starting the Lore service process (no parameters).
pub struct LoreServiceStartArgs {}

/// Start the Lore service process, unless one is already running.
///
/// Connects to the running service, and starts one when nothing is listening.
/// Succeeds once a service is reachable, whether it was already running or was
/// started here.
///
/// # Events
///
/// ## Standard Events
///
/// These events are emitted by all interface functions:
///
/// | Event | Description |
/// |-------|-------------|
/// | [`LoreEvent::Log`](crate::interface::LoreEvent::Log) | Diagnostic messages throughout execution |
/// | [`LoreEvent::Error`](crate::interface::LoreEvent::Error) | Emitted for a non-fatal error during the operation |
/// | [`LoreEvent::Complete`](crate::interface::LoreEvent::Complete) | Always emitted at the end; `status` is `0` on success or the error code on failure |
/// | [`LoreEvent::End`](crate::interface::LoreEvent::End) | Always emitted after `Complete` to signal callback termination |
pub async fn start(
    globals: LoreGlobalArgs,
    args: LoreServiceStartArgs,
    callback: LoreEventCallback,
) -> i32 {
    // Starting the service is not work the service does for a caller: it acts on
    // whichever service is reachable from here, so it runs where it was called
    // rather than routing through the service the way the other commands do.
    start_local(globals, args, callback).await
}

fn start_local(
    globals: LoreGlobalArgs,
    args: LoreServiceStartArgs,
    callback: LoreEventCallback,
) -> impl Future<Output = i32> {
    no_repository_call(globals, callback, args, start, move |_args| async move {
        if service_runs_in_this_process() {
            // The command reached the service itself, which is running, and a
            // start asks for nothing more than that.
            return Ok(());
        }

        // The connection is proof that a service is reachable and nothing more.
        // A start asks for one to be running, not for anything to be sent to it,
        // so it is closed again here.
        connect_or_spawn_service().await.map(|_connection| {
            lore_info!("Lore service is running");
        })
    })
}

#[repr(C)]
#[derive(Debug, Clone, PartialEq, Default, LoreArgs, bitcode::Encode, bitcode::Decode)]
#[handler(stop_local)]
/// Arguments for stopping the Lore service process.
pub struct LoreServiceStopArgs {}

#[error_set]
pub enum ServiceStopError {
    MountsBusy,
}

/// Stop the running Lore service process.
///
/// Does not start a service to stop, and succeeds when none is running, since
/// that is the state it asks for. A mount the service cannot release because
/// something holds it refuses the stop with `MountsBusy`, unless the global
/// `force` argument is set, which detaches busy mounts lazily.
///
/// # Events
///
/// ## Standard Events
///
/// These events are emitted by all interface functions:
///
/// | Event | Description |
/// |-------|-------------|
/// | [`LoreEvent::Log`](crate::interface::LoreEvent::Log) | Diagnostic messages throughout execution |
/// | [`LoreEvent::Error`](crate::interface::LoreEvent::Error) | Emitted for a non-fatal error during the operation |
/// | [`LoreEvent::Complete`](crate::interface::LoreEvent::Complete) | Always emitted at the end; `status` is `0` on success or the error code on failure |
/// | [`LoreEvent::End`](crate::interface::LoreEvent::End) | Always emitted after `Complete` to signal callback termination |
pub async fn stop(
    globals: LoreGlobalArgs,
    args: LoreServiceStopArgs,
    callback: LoreEventCallback,
) -> i32 {
    // Stopping the service acts on whichever service is reachable from here, so
    // like [`start`] it runs where it was called. Routing it through the service
    // branch of the dispatch would also make a stop start one.
    stop_local(globals, args, callback).await
}

async fn stop_local(
    globals: LoreGlobalArgs,
    args: LoreServiceStopArgs,
    callback: LoreEventCallback,
) -> i32 {
    #[cfg(unix)]
    {
        let target = state::current_owner();
        let early = state::capture_stop_request();
        if let Some(owner) = target {
            let permit = match state::admit_control(&owner) {
                Ok(permit) => permit,
                Err(error) => {
                    return no_repository_call(
                        globals,
                        callback,
                        args,
                        stop,
                        move |_args| async move {
                            Err::<(), ServiceStopError>(ServiceStopError::internal(format!(
                                "service control admission: {error:?}"
                            )))
                        },
                    )
                    .await;
                }
            };
            let target = owner.clone();
            return state::scope_control(owner, permit, async move {
                no_repository_call(globals, callback, args, stop, move |_args| async move {
                    target
                        .stop_control(execution_context().globals().force())
                        .await
                })
                .await
            })
            .await;
        }
        if state::has_service_scope() || early.is_some() {
            return no_repository_call(globals, callback, args, stop, move |_args| async move {
                let target = early.ok_or_else(|| {
                    ServiceStopError::internal("no trusted service run is bound to this command")
                })?;
                target.request_stop();
                Ok::<(), ServiceStopError>(())
            })
            .await;
        }
    }
    #[cfg(not(unix))]
    if service_runs_in_this_process() {
        return no_repository_call(globals, callback, args, stop, move |_args| async move {
            if let Some(manager) = MountManagerState::mount_manager() {
                manager
                    .unmount_all(execution_context().globals().force())
                    .await
                    .forward_any::<ServiceStopError>("Unmounting before the service stops")?;
            }
            request_service_stop();
            Ok::<(), ServiceStopError>(())
        })
        .await;
    }

    // Reached in the process the caller ran. Hand the command to the service so
    // that the branch above runs there, over a connection made here: a stop acts
    // on a service that is running and must not be what starts one.
    match connect_to_running_service().await {
        Ok(Some(connection)) => {
            let status = service_call_over(connection, globals, args.into(), callback).await;
            if status != 0 {
                return status;
            }

            // The service acknowledges before it lets go of the socket, so a
            // stop that returned on the acknowledgement alone would leave the
            // next start racing this shutdown.
            match wait_until_no_service_is_listening().await {
                Ok(()) => {
                    // Nothing is listening, so a service this process started has
                    // gone. Collected here as well as before the next start,
                    // because a program that starts one service and stops it
                    // without starting another would otherwise hold the exited
                    // child for as long as it ran.
                    collect_exited_services();
                    0
                }
                Err(error) => {
                    // Reported through the return value alone. The service
                    // already sent the `Complete` for the stop it carried out,
                    // and that stop did succeed; this is the wait after it. See
                    // the note in the commit for what making the two agree
                    // would cost.
                    lore_warn!("{error}");
                    error.ffi_code()
                }
            }
        }
        Ok(None) => {
            // Nothing was listening, so a service started here has already gone.
            collect_exited_services();
            no_repository_call(globals, callback, args, stop, move |_args| async move {
                // A stop asks for no service to be running, which is already so.
                lore_info!("No Lore service is running");
                Ok::<(), ServiceProcessError>(())
            })
            .await
        }
        Err(error) => {
            no_repository_call(globals, callback, args, stop, move |_args| async move {
                Err::<(), ServiceProcessError>(error)
            })
            .await
        }
    }
}

#[repr(C)]
#[derive(Debug, Clone, PartialEq, Default, LoreArgs, bitcode::Encode, bitcode::Decode)]
#[handler(set_executable_local)]
/// Arguments for naming the executable the Lore service runs from.
pub struct LoreServiceSetExecutableArgs {
    /// Path of the executable to start as the service. Empty clears the setting,
    /// which prevents auto-starting the service but can still can connect to an
    /// already running service.
    pub executable: LoreString,
}

/// Name the executable the Lore service runs from, for this machine.
///
/// Stored in the user-level global config, so it holds for later commands and
/// for clients that read it afterwards. Naming it decides which build serves the
/// machine, rather than leaving that to whichever client starts a service first.
///
/// # Events
///
/// ## Standard Events
///
/// These events are emitted by all interface functions:
///
/// | Event | Description |
/// |-------|-------------|
/// | [`LoreEvent::Log`](crate::interface::LoreEvent::Log) | Diagnostic messages throughout execution |
/// | [`LoreEvent::Error`](crate::interface::LoreEvent::Error) | Emitted for a non-fatal error during the operation |
/// | [`LoreEvent::Complete`](crate::interface::LoreEvent::Complete) | Always emitted at the end; `status` is `0` on success or the error code on failure |
/// | [`LoreEvent::End`](crate::interface::LoreEvent::End) | Always emitted after `Complete` to signal callback termination |
pub async fn set_executable(
    globals: LoreGlobalArgs,
    args: LoreServiceSetExecutableArgs,
    callback: LoreEventCallback,
) -> i32 {
    // Like [`start`] and [`stop`], this runs where it was called. A setter that
    // routed would start a service in order to be told which executable to have
    // started.
    set_executable_local(globals, args, callback).await
}

async fn set_executable_local(
    globals: LoreGlobalArgs,
    args: LoreServiceSetExecutableArgs,
    callback: LoreEventCallback,
) -> i32 {
    let dry_run = globals.dry_run();
    let command =
        async move |args: LoreServiceSetExecutableArgs| -> Result<(), ServiceProcessError> {
            let (mut config, lock) = GlobalConfig::load_locked()
                .await
                .forward::<ServiceProcessError>("loading global config")?;

            // Blank clears rather than stores, matching how a blank value already
            // reads as unset everywhere it is resolved.
            let executable = args.executable.as_str().trim();
            config.service.executable = (!executable.is_empty()).then(|| executable.to_string());
            let tense = if dry_run { "would be " } else { "" };
            match config.service_executable() {
                Some(executable) => {
                    lore_info!("Lore service executable {tense}set to {executable}");
                }
                None => lore_info!("Lore service executable {tense}cleared"),
            }
            // Clearing the executable is one of the two ways to end up with a pair
            // that does not relay.
            report_settings_update_that_will_not_relay(&config);
            if dry_run {
                return Ok(());
            }

            config
                .save(lock)
                .await
                .forward::<ServiceProcessError>("saving global config")?;

            // Whether calls relay depends on this as well as on the setting, and
            // this process decided that once. Without forgetting it, an embedder
            // that names an executable goes on relaying nothing, or one that
            // clears it goes on relaying to a build it no longer names.
            forget_whether_service_is_in_use();
            Ok(())
        };
    no_repository_call(globals, callback, args, set_executable, command).await
}

#[repr(C)]
#[derive(Debug, Clone, PartialEq, Default, LoreArgs, bitcode::Encode, bitcode::Decode)]
#[handler(set_use_automatically_local)]
/// Arguments for setting whether commands are carried out by the Lore service.
pub struct LoreServiceSetUseAutomaticallyArgs {
    /// Carry out commands in the service rather than in the process that was run
    pub enabled: u8,
}

/// Set whether commands are carried out by the Lore service, for this machine.
///
/// Stored in the user-level global config, so the service stays in use for later
/// commands rather than for one command at a time.
///
/// # Events
///
/// ## Standard Events
///
/// These events are emitted by all interface functions:
///
/// | Event | Description |
/// |-------|-------------|
/// | [`LoreEvent::Log`](crate::interface::LoreEvent::Log) | Diagnostic messages throughout execution |
/// | [`LoreEvent::Error`](crate::interface::LoreEvent::Error) | Emitted for a non-fatal error during the operation |
/// | [`LoreEvent::Complete`](crate::interface::LoreEvent::Complete) | Always emitted at the end; `status` is `0` on success or the error code on failure |
/// | [`LoreEvent::End`](crate::interface::LoreEvent::End) | Always emitted after `Complete` to signal callback termination |
pub async fn set_use_automatically(
    globals: LoreGlobalArgs,
    args: LoreServiceSetUseAutomaticallyArgs,
    callback: LoreEventCallback,
) -> i32 {
    // Runs where it was called for the same reason [`set_executable`] does: a
    // setter that routed would have to reach a service in order to be told not
    // to use one.
    set_use_automatically_local(globals, args, callback).await
}

async fn set_use_automatically_local(
    globals: LoreGlobalArgs,
    args: LoreServiceSetUseAutomaticallyArgs,
    callback: LoreEventCallback,
) -> i32 {
    let dry_run = globals.dry_run();
    let command =
        async move |args: LoreServiceSetUseAutomaticallyArgs| -> Result<(), ServiceProcessError> {
            let (mut config, lock) = GlobalConfig::load_locked()
                .await
                .forward::<ServiceProcessError>("loading global config")?;

            let enabled = args.enabled != 0;
            config.service.use_automatically = enabled.then_some(true);
            if !dry_run {
                config
                    .save(lock)
                    .await
                    .forward::<ServiceProcessError>("saving global config")?;

                // This process may go on to make more calls, and it decided once whether
                // to relay them.
                forget_whether_service_is_in_use();
            }

            let tense = if dry_run { "would" } else { "will" };
            if enabled {
                lore_info!("Lore commands {tense} be carried out by the service");
            } else {
                lore_info!("Lore commands {tense} be carried out in the process that runs them");
            }
            // Reported after the line above rather than instead of it: the setting
            // was stored, and this says what it will and will not do on its own.
            report_settings_update_that_will_not_relay(&config);
            Ok(())
        };
    no_repository_call(globals, callback, args, set_use_automatically, command).await
}

#[repr(C)]
#[derive(Debug, Clone, PartialEq, Default, LoreArgs, bitcode::Encode, bitcode::Decode)]
#[handler(status_local)]
/// Arguments for querying the Lore service status (no parameters).
pub struct LoreServiceStatusArgs {}

/// Query the Lore service status.
///
/// Reports whether the service is running and, if so, its metadata: binary path,
/// uptime, active connection count, and SWFS mount count. Also drains any buffered
/// service-level log messages.
///
/// # Events
///
/// ## Standard Events
///
/// These events are emitted by all interface functions:
///
/// | Event | Description |
/// |-------|-------------|
/// | [`LoreEvent::Log`](crate::interface::LoreEvent::Log) | Diagnostic messages throughout execution |
/// | [`LoreEvent::Error`](crate::interface::LoreEvent::Error) | Emitted for a non-fatal error during the operation |
/// | [`LoreEvent::Complete`](crate::interface::LoreEvent::Complete) | Always emitted at the end; `status` is `0` on success or the error code on failure |
/// | [`LoreEvent::End`](crate::interface::LoreEvent::End) | Always emitted after `Complete` to signal callback termination |
///
/// ## Operation-Specific Events
///
/// | Event | Description |
/// |-------|-------------|
/// | [`LoreEvent::ServiceStatus`](crate::interface::LoreEvent::ServiceStatus) | Service running state and metadata |
/// | [`LoreEvent::ServiceMessage`](crate::interface::LoreEvent::ServiceMessage) | Buffered service-level log message |
pub async fn status(
    globals: LoreGlobalArgs,
    args: LoreServiceStatusArgs,
    callback: LoreEventCallback,
) -> i32 {
    // Status runs where it was called, like start and stop. When in the service
    // process, it queries local state. When in a client, it tries to connect.
    status_local(globals, args, callback).await
}

async fn status_local(
    globals: LoreGlobalArgs,
    args: LoreServiceStatusArgs,
    callback: LoreEventCallback,
) -> i32 {
    #[cfg(unix)]
    {
        let target = state::current_owner();
        if let Some(owner) = target {
            let permit = match state::admit_control(&owner) {
                Ok(permit) => permit,
                Err(error) => {
                    return no_repository_call(
                        globals,
                        callback,
                        args,
                        status,
                        move |_args| async move {
                            Err::<(), ServiceProcessError>(ServiceProcessError::internal(format!(
                                "service control admission: {error:?}"
                            )))
                        },
                    )
                    .await;
                }
            };
            let target = owner.clone();
            return state::scope_control(owner, permit, async move {
                no_repository_call(globals, callback, args, status, move |_args| async move {
                    emit_status_from_owner(Some(target));
                    Ok::<(), ServiceProcessError>(())
                })
                .await
            })
            .await;
        }
        if state::has_service_scope() {
            return no_repository_call(globals, callback, args, status, move |_args| async move {
                emit_not_running_status();
                Ok::<(), ServiceProcessError>(())
            })
            .await;
        }
    }
    #[cfg(not(unix))]
    if service_runs_in_this_process() {
        return no_repository_call(globals, callback, args, status, move |_args| async move {
            emit_status_from_service();
            Ok::<(), ServiceProcessError>(())
        })
        .await;
    }

    // Running in a client process: try to connect to the service.
    match connect_to_running_service().await {
        Ok(Some(connection)) => {
            // Service is running; relay the status command to it.
            service_call_over(connection, globals, args.into(), callback).await
        }
        Ok(None) => {
            // No service is listening; emit not-running status.
            no_repository_call(globals, callback, args, status, move |_args| async move {
                emit_not_running_status();
                Ok::<(), ServiceProcessError>(())
            })
            .await
        }
        Err(error) => {
            no_repository_call(globals, callback, args, status, move |_args| async move {
                Err::<(), ServiceProcessError>(error)
            })
            .await
        }
    }
}

/// Emit `ServiceStatus` and `ServiceMessage` events from within the service process.
#[cfg(not(unix))]
fn emit_status_from_service() {
    emit_status_state(ServiceStateImpl::global().clone());
}
#[cfg(unix)]
fn emit_status_from_owner(owner: Option<Arc<owner::ServiceOwner>>) {
    let Some(owner) = owner else {
        emit_not_running_status();
        return;
    };
    if owner.stop_request().is_none_or(|stop| !stop.is_running()) {
        emit_not_running_status();
        return;
    }
    emit_status_state(owner.state().clone());
    let (phase, error) = owner.status();
    if phase != owner::Phase::Running {
        LoreEvent::ServiceMessage(LoreServiceMessageEventData {
            level: if error.is_some() {
                LoreLogLevel::Error
            } else {
                LoreLogLevel::Info
            },
            message: LoreString::from(
                format!(
                    "service retirement {phase:?}{}",
                    error.map_or(String::new(), |error| format!(": {error}"))
                )
                .as_str(),
            ),
        })
        .send();
    }
}
fn emit_status_state(service_state: Arc<ServiceStateImpl>) {
    // Check if service state is initialized.
    if !service_state.is_initialized() {
        // Shouldn't happen if wired correctly, but emit not-running as fallback.
        emit_not_running_status();
        return;
    }

    // Get binary path.
    let binary_path = std::env::current_exe()
        .map(|p| p.display().to_string())
        .unwrap_or_default();

    // Get mount count from MountManagerState.
    let swfs_mount_count = MountManagerState::mount_manager().map_or(0, |m| m.mount_count() as u32);

    // Emit ServiceStatus event.
    LoreEvent::ServiceStatus(LoreServiceStatusEventData {
        running: 1,
        binary_path: LoreString::from(binary_path.as_str()),
        uptime_ms: service_state.uptime_ms(),
        connection_count: service_state.connection_count(),
        swfs_mount_count,
    })
    .send();

    // Drain buffered logs and emit ServiceMessage events.
    let (messages, dropped_count) = service_state.drain_logs();
    // SWFS's native callback sink remains process-global on Unix; it does not
    // supply command admission, provider routing, uptime or connection identity.
    #[cfg(unix)]
    let (messages, dropped_count) = {
        let mut messages = messages;
        let (legacy, dropped) = ServiceStateImpl::global().drain_logs();
        messages.extend(legacy);
        (messages, dropped_count.saturating_add(dropped))
    };
    for message in messages {
        LoreEvent::ServiceMessage(LoreServiceMessageEventData {
            level: message.level,
            message: LoreString::from(message.message.as_str()),
        })
        .send();
    }

    // If messages were dropped, emit a synthetic error message.
    if dropped_count > 0 {
        LoreEvent::ServiceMessage(LoreServiceMessageEventData {
            level: LoreLogLevel::Error,
            message: LoreString::from(
                format!("{dropped_count} service log messages were dropped due to buffer overflow")
                    .as_str(),
            ),
        })
        .send();
    }
}

/// Emit a `ServiceStatus` event indicating the service is not running.
fn emit_not_running_status() {
    LoreEvent::ServiceStatus(LoreServiceStatusEventData {
        running: 0,
        binary_path: LoreString::default(),
        uptime_ms: 0,
        connection_count: 0,
        swfs_mount_count: 0,
    })
    .send();
}

#[repr(C)]
#[derive(Debug, Clone, PartialEq, Default)]
/// Arguments for running the Lore service in the current process (no parameters).
///
/// Does not derive `LoreArgs`: running the service is always local, as it is the
/// service that handles IPC and so cannot itself be routed through it.
pub struct LoreServiceRunArgs {}

/// Run the Lore service in the current process.
///
/// Returns once the service has stopped, either by `lore service stop` over IPC
/// or by [`quit`] from within this process.
///
/// # Events
///
/// ## Standard Events
///
/// These events are emitted by all interface functions:
///
/// | Event | Description |
/// |-------|-------------|
/// | [`LoreEvent::Log`](crate::interface::LoreEvent::Log) | Diagnostic messages throughout execution |
/// | [`LoreEvent::Error`](crate::interface::LoreEvent::Error) | Emitted for a non-fatal error during the operation |
/// | [`LoreEvent::Complete`](crate::interface::LoreEvent::Complete) | Always emitted at the end; `status` is `0` on success or the error code on failure |
/// | [`LoreEvent::End`](crate::interface::LoreEvent::End) | Always emitted after `Complete` to signal callback termination |
pub async fn run(
    globals: LoreGlobalArgs,
    args: LoreServiceRunArgs,
    callback: LoreEventCallback,
) -> i32 {
    let service_globals = globals.clone();
    no_repository_call(globals, callback, args, run, async move |_args| {
        #[cfg(unix)]
        let service_state = Arc::new(ServiceStateImpl::new());
        #[cfg(not(unix))]
        let service_state = Arc::clone(ServiceStateImpl::global());
        service_main(service_globals, service_state).await
    })
    .await
}

#[repr(C)]
#[derive(Debug, Clone, PartialEq, Default)]
/// Arguments for requesting the Lore service running in this process to quit (no parameters).
pub struct LoreServiceQuitArgs {}

/// Request the Lore service running in this process to quit.
///
/// This is an in-process cancellation mechanism: it asks a service started with
/// [`run`] in this process to shut down cleanly, and does not reach a service in
/// another process (use [`stop`] for that). When no service runs in this
/// process, this is a no-op that returns success.
///
/// # Events
///
/// ## Standard Events
///
/// These events are emitted by all interface functions:
///
/// | Event | Description |
/// |-------|-------------|
/// | [`LoreEvent::Log`](crate::interface::LoreEvent::Log) | Diagnostic messages throughout execution |
/// | [`LoreEvent::Error`](crate::interface::LoreEvent::Error) | Emitted for a non-fatal error during the operation |
/// | [`LoreEvent::Complete`](crate::interface::LoreEvent::Complete) | Always emitted at the end; `status` is `0` on success or the error code on failure |
/// | [`LoreEvent::End`](crate::interface::LoreEvent::End) | Always emitted after `Complete` to signal callback termination |
pub async fn quit(
    globals: LoreGlobalArgs,
    args: LoreServiceQuitArgs,
    callback: LoreEventCallback,
) -> i32 {
    // Like [`stop`], this acts where it was called: routed through the service
    // it would start one just to be told to quit.
    quit_local(globals, args, callback).await
}

async fn quit_local(
    globals: LoreGlobalArgs,
    args: LoreServiceQuitArgs,
    callback: LoreEventCallback,
) -> i32 {
    #[cfg(unix)]
    {
        let target = state::current_owner();
        let early = state::capture_stop_request();
        if let Some(owner) = target {
            let permit = match state::admit_control(&owner) {
                Ok(permit) => permit,
                Err(state::ControlError::Busy)
                    if !state::has_service_scope()
                        && owner.status().0 == owner::Phase::ReadyToExit =>
                {
                    // Retirement has succeeded and the actor may already have
                    // closed control admission while draining its last calls.
                    // A repeated direct quit has no further work to admit.
                    return no_repository_call(
                        globals,
                        callback,
                        args,
                        quit,
                        move |_args| async move {
                            if let Some(error) = owner.actor().and_then(|actor| actor.failure()) {
                                return Err(ServiceProcessError::internal(error));
                            }
                            Ok::<(), ServiceProcessError>(())
                        },
                    )
                    .await;
                }
                Err(error) => {
                    return no_repository_call(
                        globals,
                        callback,
                        args,
                        quit,
                        move |_args| async move {
                            Err::<(), ServiceProcessError>(ServiceProcessError::internal(format!(
                                "service control admission: {error:?}"
                            )))
                        },
                    )
                    .await;
                }
            };
            let target = owner.clone();
            return state::scope_control(owner, permit, async move {
                no_repository_call(globals, callback, args, quit, move |_args| async move {
                    target
                        .stop_control(execution_context().globals().force())
                        .await
                        .map_err(|error| ServiceProcessError::internal(error.to_string()))
                })
                .await
            })
            .await;
        }
        let scoped = state::has_service_scope();
        no_repository_call(globals, callback, args, quit, move |_args| async move {
            if scoped && early.is_none() {
                return Err(ServiceProcessError::internal(
                    "no trusted service run is bound to this command",
                ));
            }
            if let Some(target) = early {
                target.request_stop();
            }
            Ok::<(), ServiceProcessError>(())
        })
        .await
    }
    #[cfg(not(unix))]
    no_repository_call(globals, callback, args, quit, move |_args| async move {
        request_service_stop();
        Ok::<(), ServiceProcessError>(())
    })
    .await
}
