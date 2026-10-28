// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

use std::path::PathBuf;

use clap::Args;
use lore::interface::LoreGlobalArgs;

#[derive(Args)]
pub struct MountArgs {
    /// Explicitly opt into experimental mounting
    #[arg(long, required = true)]
    pub experimental: bool,
    /// Revision to resolve once and retain for the mount lifetime
    #[arg(long, required_unless_present = "reuse", value_name = "revision")]
    #[arg(value_parser = parse_revision)]
    pub revision: Option<String>,
    /// Service-owned writable overlay with private external metadata/backing
    #[arg(long)]
    pub writable: bool,
    /// Fetch immutable file contents lazily from the source's authenticated remote; requires online execution
    #[arg(long, requires_all = ["writable", "source"])]
    pub remote_contents: bool,
    /// Read this normal checkout's pinned revision and access policy (never convert it)
    #[arg(long, requires = "writable", value_name = "checkout")]
    pub source: Option<PathBuf>,
    /// Existing private empty metadata directory, or the same directory for reuse
    #[arg(
        long,
        requires = "writable",
        required_if_eq("writable", "true"),
        value_name = "private-directory"
    )]
    pub metadata: Option<PathBuf>,
    /// Fresh external backing root; reuse reads the recorded path instead
    #[arg(
        long,
        requires = "writable",
        conflicts_with = "reuse",
        value_name = "new-private-directory"
    )]
    pub backing: Option<PathBuf>,
    /// Restore the same working instance; --source reauthorizes linked reads
    #[arg(long, requires = "writable", conflicts_with = "revision")]
    pub reuse: bool,
    /// Existing empty directory outside repository metadata
    #[arg(value_name = "mountpoint")]
    pub mountpoint: PathBuf,
    /// Verified digest index; offline fresh imports precompute hashes, online reads use cached hashes
    #[arg(long, value_name = "private-directory")]
    pub digest_cache: Option<PathBuf>,
}

impl MountArgs {
    /// Check execution mode after global arguments have been resolved.
    pub fn validate_execution_mode(&self, offline: bool) -> Result<(), &'static str> {
        if self.writable && self.remote_contents == offline {
            Err(
                "Choose --offline for import, or --remote-contents without --offline for lazy online reads.",
            )
        } else {
            Ok(())
        }
    }
}

fn parse_revision(value: &str) -> Result<String, String> {
    if value.trim().is_empty() {
        Err("revision must not be empty".into())
    } else {
        Ok(value.to_owned())
    }
}

pub fn handle_mount(globals: LoreGlobalArgs, args: &MountArgs) -> u8 {
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        let _ = (globals, args);
        crate::eprintln!("Experimental foreground mounts are supported only on Linux.");
        1
    }
    #[cfg(target_os = "macos")]
    {
        if args.writable || args.reuse || args.remote_contents || args.digest_cache.is_some() {
            crate::eprintln!(
                "macOS currently supports only offline pinned read-only mounts without a digest cache."
            );
            return 1;
        }
        if !globals.offline()
            || globals.dry_run()
            || crate::cli::output_formatter().is_some()
            || globals.identity().is_some()
            || !globals.identity_token().is_empty()
            || !globals.access_token().is_empty()
            || globals.remote()
            || globals.trust_remote()
        {
            crate::eprintln!(
                "macOS FSKit mounts require --offline and do not support remote options, identity overrides, --json or --dry-run."
            );
            return 1;
        }
        let Some(helper) = std::env::var_os("LORE_FSKIT_MOUNT_HELPER") else {
            crate::eprintln!("Set LORE_FSKIT_MOUNT_HELPER to the installed FSKit mount helper.");
            return 1;
        };
        let Some(config_dir) = std::env::var_os("LORE_FSKIT_CONFIG_DIR") else {
            crate::eprintln!(
                "Set LORE_FSKIT_CONFIG_DIR to a private directory in the FSKit extension container."
            );
            return 1;
        };
        let result = std::process::Command::new(helper)
            .arg(globals.repository_path())
            .arg(args.revision.as_deref().unwrap_or_default())
            .arg(&args.mountpoint)
            .arg(config_dir)
            .status();
        return match result {
            Ok(status) if status.success() => 0,
            Ok(status) => {
                crate::eprintln!("FSKit mount helper failed: {status}");
                1
            }
            Err(error) => {
                crate::eprintln!("Could not start FSKit mount helper: {error}");
                1
            }
        };
    }
    #[cfg(target_os = "linux")]
    {
        use crate::cli::EventCallbackExt;
        use crate::cli::EventCallbackFn;
        use crate::cli::output_formatter;
        use crate::util::TerminationSignals;
        if globals.dry_run() {
            crate::eprintln!("The experimental foreground mount does not support --dry-run.");
            return 1;
        }
        if output_formatter().is_some() {
            crate::eprintln!("The experimental foreground mount does not support --json.");
            return 1;
        }
        let callback = Some(
            (Box::new(|_event: &lore::interface::LoreEvent| {}) as EventCallbackFn).with_defaults(),
        );
        if args.writable {
            if let Err(error) = args.validate_execution_mode(globals.offline()) {
                crate::eprintln!("{error}");
                return 1;
            }
            // This runs in the caller's CLI process before service IPC. Send
            // absolute paths so the service never substitutes its own cwd.
            #[allow(clippy::disallowed_methods)]
            let absolute = |path: Option<&PathBuf>| -> Result<String, String> {
                let Some(path) = path else {
                    return Ok(String::new());
                };
                let path = if path.is_absolute() {
                    path.clone()
                } else {
                    std::env::current_dir()
                        .map_err(|error| error.to_string())?
                        .join(path)
                };
                path.to_str()
                    .map(str::to_owned)
                    .ok_or_else(|| "Mount paths must be UTF-8".into())
            };
            let arguments = (|| -> Result<lore::mount_working::LoreMountWorkingArgs, String> {
                if args.metadata.is_none() {
                    return Err("--writable requires --metadata".into());
                }
                if !args.reuse && (args.source.is_none() || args.backing.is_none()) {
                    return Err("A fresh writable mount requires --source and --backing".into());
                }
                Ok(lore::mount_working::LoreMountWorkingArgs {
                    experimental: args.experimental,
                    reuse: args.reuse,
                    remote_contents: args.remote_contents,
                    source: absolute(args.source.as_ref())?.into(),
                    revision: args.revision.clone().unwrap_or_default().into(),
                    mountpoint: absolute(Some(&args.mountpoint))?.into(),
                    metadata: absolute(args.metadata.as_ref())?.into(),
                    backing: absolute(args.backing.as_ref())?.into(),
                    digest_cache: absolute(args.digest_cache.as_ref())?.into(),
                })
            })();
            let arguments = match arguments {
                Ok(arguments) => arguments,
                Err(error) => {
                    crate::eprintln!("{error}");
                    return 1;
                }
            };
            return lore::runtime()
                .block_on(lore::mount_working::start(globals, arguments, callback))
                as u8;
        }
        let revision = args.revision.clone().unwrap_or_default();
        let mountpoint = args.mountpoint.clone();
        let digest_cache = args.digest_cache.clone();
        lore::runtime().block_on(async move {
            let mut signals = match TerminationSignals::register() {
                Ok(signals) => signals,
                Err(error) => {
                    crate::eprintln!("Could not register mount shutdown signals: {error}");
                    return 1;
                }
            };
            let prepared = match tokio::select! {
                result = lore::mount::prepare(globals, revision, mountpoint, digest_cache, callback) => result,
                _ = signals.recv() => return 0,
            } {
                Ok(prepared) => prepared,
                Err(status) => return status as u8,
            };
            let display = prepared.mountpoint().display().to_string();
            match prepared
                .run(signals.recv(), || {
                    crate::println!(
                        "Mounted pinned revision read-only at {display}. Press Ctrl-C to unmount."
                    );
                })
                .await
            {
                Ok(()) => 0,
                Err(error) => {
                    crate::eprintln!("Experimental mount failed: {error}");
                    1
                }
            }
        })
    }
}
