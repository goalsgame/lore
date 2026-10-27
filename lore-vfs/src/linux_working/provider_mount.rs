// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use lore_base::runtime::LORE_CONTEXT;
use lore_revision::interface::ExecutionContext;
use lore_revision::interface::LoreArray;
use lore_revision::interface::LoreGlobalArgs;
use lore_revision::interface::LoreString;
use lore_revision::relay::EventDispatcher;
use lore_revision::repository::RepositoryContext;
use lore_revision::repository::RepositoryContextCreationArgs;
use lore_revision::repository::RepositoryPaths;
use lore_revision::repository::RepositoryWriteToken;

use super::*;
use crate::provider::WorkingProvider;
use crate::revision::RevisionBackend;

#[test]
#[ignore = "requires native Linux FUSE; uses isolated local Lore stores only"]
fn mounted_normal_commit_adopts_shared_namespace_digest_and_restores() {
    let directory = Directory::new("normal-commit");
    let runtime = runtime();
    let execution = Arc::new(ExecutionContext::new_client_with_user_id(
        LoreGlobalArgs::default(),
        EventDispatcher::no_dispatch(),
        "working-mount-fixture".into(),
    ));
    let root_path = directory.0.join("repo");
    std::fs::create_dir(&root_path).unwrap();
    let stores = directory.0.join("stores");
    std::fs::create_dir(&stores).unwrap();
    let mountpoint = directory.0.join("mount");
    std::fs::create_dir(&mountpoint).unwrap();
    let factory = Arc::new(DiskStores {
        directory: stores,
        next: AtomicU64::new(0),
    });
    let gate = Arc::new(Gate::default());
    let (model, backend, repository, token) =
        runtime.block_on(LORE_CONTEXT.scope(execution.clone(), async {
            let immutable = lore_storage::local::immutable_store::create(
                None::<&str>,
                lore_storage::local::immutable_store::ImmutableStoreCreateOptions::none(),
                false,
                Default::default(),
            )
            .await
            .unwrap();
            let mutable = lore_storage::local::mutable_store::create(
                None::<&str>,
                Default::default(),
                immutable.clone(),
            )
            .await
            .unwrap();
            let token = RepositoryWriteToken::acquire(&root_path).await;
            let id = [81; 16].into();
            let branch = [82; 16].into();
            let created = lore_revision::repository::create_local(
                &root_path,
                &token,
                id,
                branch,
                lore_revision::branch::DEFAULT_DEFAULT_NAME.to_string(),
                Default::default(),
                false,
            )
            .await
            .unwrap();
            let make = |provider: Option<
                Arc<dyn lore_revision::fs::filesystem_provider::FilesystemProvider>,
            >| {
                Arc::new(
                    RepositoryContext::new(RepositoryContextCreationArgs {
                        paths: Some(RepositoryPaths::new(
                            root_path.clone(),
                            root_path.join(".lore"),
                        )),
                        immutable_store: immutable.clone(),
                        mutable_store: mutable.clone(),
                        id,
                        instance_id: created.instance_id,
                        remote: Err(lore_base::error::NoRemote.into()),
                        filter: Arc::default(),
                        filesystem_provider: provider,
                    })
                    .with_write_token(token.share()),
                )
            };
            let original = make(None);
            lore_revision::instance::store_current_anchor_branch(&original, branch)
                .await
                .unwrap();
            let backend = Arc::new(
                RevisionBackend::new(original, Default::default())
                    .await
                    .unwrap(),
            );
            let namespace = Arc::new(
                Namespace::new(
                    backend.clone(),
                    backend.root(),
                    gate.clone(),
                    Default::default(),
                )
                .unwrap(),
            );
            let model = Model::new(
                namespace.clone(),
                factory.clone(),
                Arc::new(StorageBudget::new(1024)),
                Recovery::open(directory.0.join("recovery"), Default::default()).unwrap(),
                Default::default(),
            )
            .await
            .unwrap();
            let provider = Arc::new(
                WorkingProvider::new(
                    namespace,
                    backend.clone(),
                    directory.0.join("exports"),
                    Default::default(),
                )
                .unwrap()
                .with_checkpoint_owner(model.checkpoint_owner())
                .unwrap(),
            );
            let repository = make(Some(provider));
            (model, backend, repository, token)
        }));
    let mounted = mount(model.clone(), &runtime, &mountpoint);
    let path = mountpoint.join("file");
    let mut file = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&path)
        .unwrap();
    file.write_all(b"committed bytes").unwrap();
    assert!(
        runtime
            .block_on(model.namespace().freeze_operation())
            .is_err()
    );
    assert_eq!(
        digest(&path, 32).unwrap_err().raw_os_error(),
        Some(libc::ENODATA)
    );
    drop(file);
    runtime.block_on(async {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let entry = model.namespace().lookup(1, b"file").await.unwrap();
                if model
                    .namespace()
                    .materialized_file(entry.inode)
                    .await
                    .unwrap()
                    .unwrap()
                    .metadata()
                    .await
                    .unwrap()
                    .writers
                    == 0
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    });
    runtime.block_on(LORE_CONTEXT.scope(execution.clone(), async {
        lore_revision::file::stage::stage(
            repository.clone(),
            &token,
            LoreArray::from_vec(vec![LoreString::from(&root_path.join("file"))]),
            lore_revision::stage::StageOptions {
                scan: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        lore_revision::commit::commit_boxed(
            repository,
            &token,
            lore_revision::commit::CommitOptions::new("mounted fixture".into()),
        )
        .await
        .unwrap();
    }));
    assert!(
        !root_path.join("file").exists(),
        "normal commit must consume the shared provider"
    );
    let expected = ring::digest::digest(&ring::digest::SHA256, b"committed bytes");
    assert_eq!(digest(&path, 32).unwrap(), expected.as_ref());
    mounted.unmount().unwrap();
    drop(model);
    let restored = runtime
        .block_on(LORE_CONTEXT.scope(
            execution,
            Model::restore(
                backend.clone(),
                backend.root(),
                gate,
                Default::default(),
                factory,
                Arc::new(StorageBudget::new(1024)),
                Recovery::open(directory.0.join("recovery"), Default::default()).unwrap(),
                Default::default(),
            ),
        ))
        .unwrap();
    let mounted = mount(restored, &runtime, &mountpoint);
    assert_eq!(digest(&path, 32).unwrap(), expected.as_ref());
    let mut file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
    assert_eq!(
        digest(&path, 32).unwrap_err().raw_os_error(),
        Some(libc::ENODATA)
    );
    file.write_all(b"rewritten bytes").unwrap();
    drop(file);
    assert_eq!(std::fs::read(&path).unwrap(), b"rewritten bytes");
    mounted.unmount().unwrap();
}
