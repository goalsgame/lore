// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Explicit service-owned Linux working mounts with separate metadata/backing.
//! Fresh mounts import offline objects or fetch online contents lazily; source checkouts remain separate.
use lore_macro::LoreArgs;

use crate::interface::LoreEventCallback;
use crate::interface::LoreGlobalArgs;
use crate::interface::LoreString;

#[repr(C)]
#[derive(Debug, Clone, PartialEq, Default, LoreArgs, bitcode::Encode, bitcode::Decode)]
#[handler(start_local)]
pub struct LoreMountWorkingArgs {
    pub experimental: bool,
    pub reuse: bool,
    pub source: LoreString,
    pub revision: LoreString,
    pub mountpoint: LoreString,
    pub metadata: LoreString,
    pub backing: LoreString,
    pub digest_cache: LoreString,
    pub remote_contents: bool,
}

/// Start a writable mount on the service owning the provider registry. Normal
/// repository command loading is deliberately not performed for its new target.
pub async fn start(
    globals: LoreGlobalArgs,
    args: LoreMountWorkingArgs,
    callback: LoreEventCallback,
) -> i32 {
    crate::remote::call::service_call(globals, args.into(), callback).await
}

async fn start_local(
    globals: LoreGlobalArgs,
    args: LoreMountWorkingArgs,
    callback: LoreEventCallback,
) -> i32 {
    crate::call::no_repository_call(globals, callback, args, start, |args| async move {
        #[cfg(target_os = "linux")]
        {
            start_admitted(args).await
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = args;
            Err::<(), lore_revision::repository::RepositoryError>(
                lore_revision::repository::RepositoryError::internal(
                    "experimental writable mounts require Linux",
                ),
            )
        }
    })
    .await
}

#[cfg(target_os = "linux")]
async fn start_admitted(
    args: LoreMountWorkingArgs,
) -> Result<(), lore_revision::repository::RepositoryError> {
    let owner = crate::service::state::current_owner().ok_or_else(|| {
        lore_revision::repository::RepositoryError::internal(
            "writable mount requires an admitted service owner",
        )
    })?;
    let work = owner.working_bootstrap_io().map_err(|error| {
        lore_revision::repository::RepositoryError::internal(format!(
            "working mount admission: {error}"
        ))
    })?;
    work.scope(start_owned(args, owner)).await
}

#[cfg(target_os = "linux")]
async fn start_owned(
    args: LoreMountWorkingArgs,
    owner: std::sync::Arc<crate::service::owner::ServiceOwner>,
) -> Result<(), lore_revision::repository::RepositoryError> {
    use std::path::Path;
    use std::path::PathBuf;
    use std::sync::Arc;

    use lore_error_set::prelude::*;
    use lore_revision::branch;
    use lore_revision::instance::InstanceId;
    use lore_revision::lore::execution_context;
    use lore_revision::repository;
    use lore_revision::repository::RepositoryAccess;
    use lore_revision::repository::RepositoryConfig;
    use lore_revision::repository::RepositoryError;
    use lore_revision::repository::VfsConfig;
    use lore_revision::repository::VfsType;
    use lore_revision::repository::working_bootstrap::ImmutableImport;
    use lore_revision::repository::working_bootstrap::ImportReadPolicy;
    use lore_revision::repository::working_bootstrap::RemoteContents;
    use lore_revision::repository::working_bootstrap::Selection;
    use lore_revision::revision;

    use crate::working_mount::Options;
    use crate::working_mount::OwnerLimits;
    use crate::working_mount::bootstrap::factory::BootstrapMode;
    use crate::working_mount::bootstrap::factory::BootstrapRequest;

    fn bad(message: impl Into<String>) -> RepositoryError {
        RepositoryError::internal(message)
    }
    let execution = execution_context();
    if !args.experimental || execution.globals().dry_run() || execution.globals().in_memory() {
        return Err(bad(
            "writable mounts require --experimental and disk-backed execution",
        ));
    }
    if args.remote_contents == execution.globals().offline() {
        return Err(bad(
            "use --offline for immutable import, or --remote-contents without --offline for lazy online reads",
        ));
    }
    if args.remote_contents && execution.globals().trust_remote() {
        return Err(bad(
            "--remote-contents requires verified remote reads; omit --trust-remote",
        ));
    }
    if args.remote_contents && args.source.is_empty() {
        return Err(bad(
            "--remote-contents requires --source to authorize the remote repository",
        ));
    }
    let target = PathBuf::from(args.mountpoint.as_str());
    let metadata = PathBuf::from(args.metadata.as_str());
    if !target.is_absolute() || !metadata.is_absolute() {
        return Err(bad("mount and external metadata paths must be absolute"));
    }
    let target = crate::mount::validate_mountpoint(&target, Some(&metadata))
        .map_err(|error| bad(format!("invalid writable mountpoint: {error}")))?;
    let metadata = metadata.canonicalize().map_err(|error| {
        bad(format!(
            "external metadata directory must already exist: {error}"
        ))
    })?;
    // The donor connection can start persistent authentication tasks before its
    // owner source is minted. Scope all source setup to a detached owner context
    // while retaining the finite bootstrap IO lease.
    let source_execution = Arc::new(execution.for_background_owner().await);
    let (repository_id, instance, mode) = lore_base::runtime::LORE_CONTEXT
        .scope(source_execution, async {
            let prepared = if args.reuse {
                if !args.revision.is_empty() || !args.backing.is_empty() {
                    return Err(bad(
                        "reuse accepts only the recorded revision and backing; add --source to authorize linked reads",
                    ));
                }
                let repository_id = repository::read_id_from_file(metadata.join(repository::ID))
                    .map_err(|error| bad(format!("read recorded repository identity: {error}")))?;
                let instance = InstanceId::read_from_file(metadata.join(repository::INSTANCE))
                    .map_err(|error| bad(format!("read recorded working instance: {error}")))?;
                let mode = if args.source.is_empty() {
                    BootstrapMode::Reuse
                } else {
                    let source_path = Path::new(args.source.as_str());
                    if !source_path.is_absolute() {
                        return Err(bad("source path must be absolute"));
                    }
                    let source = repository::load_and_connect_with_token(
                        source_path,
                        RepositoryAccess::ReadOnly,
                        None,
                    )
                    .await?;
                    if source.id != repository_id {
                        return Err(bad("reuse source belongs to a different repository"));
                    }
                    let source_root = source.require_path()?;
                    if metadata.starts_with(source_root) || source_root.starts_with(&metadata) {
                        return Err(bad(
                            "reuse metadata must be separate from the source checkout",
                        ));
                    }
                    let policy = ImportReadPolicy::from_repository(&source)
                        .map_err(|error| bad(format!("capture current source read policy: {error}")))?;
                    if args.remote_contents {
                        let connection = source
                            .remote()
                            .await
                            .forward_any::<RepositoryError>("connect current source repository")?;
                        let authority = connection
                            .owner_source()
                            .await
                            .forward_any::<RepositoryError>("capture source remote authority")?;
                        let contents = RemoteContents::new(authority, policy)
                            .map_err(|error| bad(format!("validate remote contents: {error}")))?;
                        BootstrapMode::ReuseRemote { contents }
                    } else {
                        BootstrapMode::ReuseWithPolicy { policy }
                    }
                };
                (repository_id, instance, mode)
            } else {
                if args.source.is_empty() || args.revision.is_empty() || args.backing.is_empty() {
                    return Err(bad(
                        "fresh writable mounts require source, revision and separate backing paths",
                    ));
                }
                let source_path = Path::new(args.source.as_str());
                let backing = PathBuf::from(args.backing.as_str());
                if !source_path.is_absolute() || !backing.is_absolute() {
                    return Err(bad("source and backing paths must be absolute"));
                }
                let source =
                    repository::load_and_connect_with_token(source_path, RepositoryAccess::ReadOnly, None)
                        .await?;
                let source_root = source.require_path()?;
                let backing = if backing.exists() {
                    let info = std::fs::symlink_metadata(&backing)
                        .map_err(|error| bad(format!("inspect backing path: {error}")))?;
                    if !info.is_dir() || info.file_type().is_symlink() {
                        return Err(bad("backing must be a directory, never a symlink"));
                    }
                    backing
                        .canonicalize()
                        .map_err(|error| bad(format!("canonicalize backing: {error}")))?
                } else {
                    let parent = backing
                        .parent()
                        .ok_or_else(|| bad("backing needs an existing parent"))?
                        .canonicalize()
                        .map_err(|error| bad(format!("canonicalize backing parent: {error}")))?;
                    parent.join(
                        backing
                            .file_name()
                            .ok_or_else(|| bad("backing needs a directory name"))?,
                    )
                };
                if metadata.starts_with(source_root)
                    || source_root.starts_with(&metadata)
                    || backing.starts_with(source_root)
                    || source_root.starts_with(&backing)
                {
                    return Err(bad(
                        "external metadata/backing must be separate from the source checkout",
                    ));
                }
                crate::mount::validate_mountpoint(&target, Some(source.dot_dir_path()?))
                    .map_err(|error| bad(format!("mountpoint overlaps source metadata: {error}")))?;
                let resolved = revision::resolve_in_branch(
                    source.clone(),
                    args.revision.as_str(),
                    if args.remote_contents {
                        revision::ResolveSearchLocation::RemoteOrLocal
                    } else {
                        revision::ResolveSearchLocation::Local
                    },
                )
                .await
                .forward_any::<RepositoryError>("resolve pinned source revision")?;
                let branch_metadata = match branch::metadata_local(source.clone(), resolved.branch).await {
                    Ok(metadata) => Ok(metadata),
                    Err(error) if args.remote_contents && (
                        error.is_branch_not_found() || error.is_address_not_found() || error.is_payload_not_found()
                    ) => {
                        let connection = source.remote().await
                            .forward_any::<RepositoryError>("connect source branch metadata")?;
                        branch::metadata_remote(connection, source.clone(), resolved.branch).await
                    }
                    Err(error) => Err(error),
                }.forward_any::<RepositoryError>("read source branch metadata")?;
                let selection = Selection {
                    revision: resolved.revision,
                    branch: resolved.branch,
                    branch_name: branch::name(&branch_metadata)
                        .forward_any::<RepositoryError>("read source branch name")?
                        .to_owned(),
                };
                let repository_id = source.id;
                let mut config = RepositoryConfig {
                    vfs: Some(VfsConfig {
                        vfs_type: VfsType::ExperimentalWorking,
                    }),
                    ..Default::default()
                };
                let mode = if args.remote_contents {
                    let policy = ImportReadPolicy::from_repository(&source)
                        .map_err(|error| bad(format!("capture current source read policy: {error}")))?;
                    let connection = source
                        .remote()
                        .await
                        .forward_any::<RepositoryError>("connect source contents")?;
                    let authority = connection
                        .owner_source()
                        .await
                        .forward_any::<RepositoryError>("capture source remote authority")?;
                    let contents = RemoteContents::new(authority, policy)
                        .map_err(|error| bad(format!("validate remote contents: {error}")))?;
                    config.remote_url = Some(contents.remote_url().to_owned());
                    config.identity = Some(contents.identity().to_owned());
                    BootstrapMode::RemoteCreate {
                        backing,
                        config,
                        selection,
                        contents,
                    }
                } else {
                    let seed = ImmutableImport::new(source, selection.revision)
                        .await
                        .map_err(|error| bad(format!("validate immutable source import: {error}")))?;
                    BootstrapMode::Import {
                        backing,
                        config,
                        selection,
                        seed,
                    }
                };
                (repository_id, InstanceId::generate(), mode)
            };
            Ok::<_, RepositoryError>(prepared)
        })
        .await?;
    let registration = owner
        .reserve_working_registration(&target, &metadata, repository_id, instance)
        .map_err(|error| bad(format!("reserve working mount: {error}")))?;
    let options = Options {
        digest_cache: if args.digest_cache.is_empty() {
            None
        } else {
            let path = Path::new(args.digest_cache.as_str());
            if !path.is_absolute() {
                return Err(bad("digest cache path must be absolute"));
            }
            // Creation and identity checks happen only after the startup ticket
            // reserves this path against all managed metadata/backing roots.
            Some(path.to_path_buf())
        },
    };
    let reuse_hint = if args.reuse && args.source.is_empty() {
        "; reuse without --source refuses linked repository access"
    } else {
        ""
    };
    let handle = owner
        .bootstrap_working_local(
            BootstrapRequest {
                registration: Arc::new(registration),
                mode,
            },
            OwnerLimits::default(),
            options,
        )
        .await
        .map_err(|error| {
            bad(format!(
                "working mount startup: {} (retained owner {:?}){}",
                error.error,
                error.handle.map(|handle| handle.id()),
                reuse_hint
            ))
        })?;
    lore_revision::lore_info!(
        "Mounted experimental writable instance {} at {}. Use 'lore service stop' to unmount all service-owned mounts cleanly.",
        handle.id(),
        target.display()
    );
    Ok(())
}
