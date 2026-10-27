// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
//! Mounted command transitions over the ordinary owned remote backend.
#![cfg(target_os = "linux")]

use std::sync::Mutex;

use lore::remote::command::LoreCommand;
use lore::service::owner::RunCleanup;
use lore::service::owner::ServiceOwner;
use lore::service::state::with_invocation;
use lore::service::state::with_owner;
use lore_revision::interface::LoreArray;
use lore_revision::interface::LoreGlobalArgs;
use lore_revision::interface::LoreString;
use lore_revision::repository::status::LoreRepositoryStatusRevisionEventData;

use super::*;

struct History {
    source: Source,
    first: lore_base::types::Hash,
    second: lore_base::types::Hash,
    third: lore_base::types::Hash,
    expected_files: Vec<(PathBuf, Vec<u8>)>,
    expected_metadata: Vec<(PathBuf, Vec<u8>)>,
}
impl History {
    async fn new(fixture: &Fixture) -> Self {
        let source = Box::pin(Source::new(fixture)).await;
        let mut pins = Vec::new();
        for (label, contents, remove) in [
            ("only-a", &b"source committed bytes"[..], None),
            ("only-b", &b"target second bytes"[..], Some("only-a")),
            ("only-c", &b"target third bytes"[..], Some("only-b")),
        ] {
            std::fs::write(source.path.join("original.txt"), contents).unwrap();
            std::fs::write(source.path.join(label), label.as_bytes()).unwrap();
            if let Some(remove) = remove {
                std::fs::remove_file(source.path.join(remove)).unwrap();
            }
            let token = RepositoryWriteToken::acquire(&source.path).await;
            let repository = lore_revision::repository::load_and_connect_with_token(
                &source.path,
                lore_revision::repository::RepositoryAccess::ReadWrite,
                Some(token.share()),
            )
            .await
            .unwrap();
            Box::pin(lore_revision::file::stage::stage(
                repository.clone(),
                &token,
                LoreArray::from_vec(vec![LoreString::from(&source.path)]),
                lore_revision::stage::StageOptions {
                    scan: true,
                    ..Default::default()
                },
            ))
            .await
            .unwrap();
            let pin = lore_revision::commit::commit_boxed(
                repository.clone(),
                &token,
                lore_revision::commit::CommitOptions::new(label.into()),
            )
            .await
            .unwrap();
            repository.flush(true).await.unwrap();
            pins.push(pin);
        }
        let expected_files = ["original.txt", "only-c"]
            .into_iter()
            .map(|name| {
                let path = source.path.join(name);
                let bytes = std::fs::read(&path).unwrap();
                (path, bytes)
            })
            .collect();
        Self {
            source,
            first: pins[0],
            second: pins[1],
            third: pins[2],
            expected_files,
            expected_metadata: Vec::new(),
        }
    }
    fn record_metadata(&mut self) {
        self.expected_metadata = ["id", "instance", "config.toml"]
            .into_iter()
            .map(|name| {
                let path = self.source.path.join(".lore").join(name);
                let bytes = std::fs::read(&path).unwrap();
                (path, bytes)
            })
            .collect();
    }
    async fn assert_source_unchanged(&self) {
        for (path, bytes) in self.expected_files.iter().chain(&self.expected_metadata) {
            assert_eq!(&std::fs::read(path).unwrap(), bytes);
        }
        assert_eq!(
            lore_revision::instance::load_current_anchor_boxed(&self.source.repository)
                .await
                .unwrap()
                .0,
            self.third
        );
    }
}
fn assert_hash(path: &std::path::Path, expected: &[u8; 32]) {
    let path = std::ffi::CString::new(path.as_os_str().as_encoded_bytes()).unwrap();
    let mut actual = [0u8; 32];
    // SAFETY: terminated path/name and a writable buffer with its exact length.
    let length = unsafe {
        libc::getxattr(
            path.as_ptr(),
            c"user.sha256".as_ptr(),
            actual.as_mut_ptr().cast(),
            actual.len(),
        )
    };
    assert_eq!(length, 32);
    assert_eq!(&actual, expected);
}

async fn command(
    owner: &Arc<ServiceOwner>,
    globals: LoreGlobalArgs,
    command: LoreCommand,
    callback: lore::interface::LoreEventCallback,
) -> i32 {
    let observer: lore::interface::LoreEventCallback = Some(Box::new(move |event| {
        if let lore::interface::LoreEvent::Complete(completion) = event
            && completion.status != 0
        {
            eprintln!(
                "Command failed ({}): {}",
                completion.status,
                completion.error.message_with_trace()
            );
        }
        if let Some(callback) = &callback {
            callback(event);
        }
    }));
    bounded(with_invocation(
        owner.state.try_begin_invocation().unwrap(),
        with_owner(
            owner.clone(),
            lore::call::with_provider_resolver(
                owner.state.provider_registry().clone(),
                Box::pin(command.invoke_local(globals, observer)),
            ),
        ),
    ))
    .await
    .unwrap()
}
async fn sync(
    owner: &Arc<ServiceOwner>,
    mut globals: LoreGlobalArgs,
    pin: lore_base::types::Hash,
    preserve: bool,
    force: bool,
) -> i32 {
    globals.force = u8::from(force);
    command(
        owner,
        globals,
        LoreCommand::RevisionSync(lore::revision::LoreRevisionSyncArgs {
            revision: format!("main@{pin}").into(),
            forward_changes: u8::from(preserve),
            ..Default::default()
        }),
        None,
    )
    .await
}
async fn anchors(
    owner: &Arc<ServiceOwner>,
    globals: LoreGlobalArgs,
) -> (
    lore_base::types::Hash,
    lore_base::types::Hash,
    lore_base::types::Hash,
    lore_base::types::Hash,
) {
    let seen = Arc::new(Mutex::new(None::<LoreRepositoryStatusRevisionEventData>));
    let output = seen.clone();
    let callback: lore::interface::LoreEventCallback = Some(Box::new(move |event| {
        if let lore::interface::LoreEvent::RepositoryStatusRevision(status) = event {
            *output.lock().unwrap() = Some(status.clone());
        }
    }));
    assert_eq!(
        command(
            owner,
            globals,
            LoreCommand::RepositoryStatus(lore::repository::LoreRepositoryStatusArgs {
                staged: 0,
                scan: 0,
                check_dirty: 0,
                reset: 0,
                sync_point: 0,
                revision_only: 1,
                count: 0,
                paths: Default::default(),
            }),
            callback
        )
        .await,
        0
    );
    let status = seen.lock().unwrap().take().unwrap();
    (
        status.revision,
        status.revision_staged,
        status.revision_merged,
        status.revision_local,
    )
}
async fn stop(owner: &Arc<ServiceOwner>) {
    bounded(with_invocation(
        owner.state.try_begin_control_invocation().unwrap(),
        with_owner(owner.clone(), async { owner.stop_control(true).await }),
    ))
    .await
    .unwrap()
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Linux /dev/fuse and unprivileged mount support"]
async fn mounted_sync_preserves_old_handles_refuses_conflicts_and_reopens_successive_views() {
    use tokio::io::AsyncReadExt;
    use tokio::io::AsyncSeekExt;
    use tokio::io::AsyncWriteExt;
    let execution = Arc::new(
        lore_revision::interface::ExecutionContext::new_client_with_user_id(
            LoreGlobalArgs::default(),
            lore_revision::relay::EventDispatcher::no_dispatch(),
            "live-sync-user".into(),
        ),
    );
    Box::pin(LORE_CONTEXT.scope(execution, async {
        let mut fixture = Fixture::new();
        let mut history = Box::pin(History::new(&fixture)).await;
        drop(fixture.registration.take());
        let mut payloads = Vec::new();
        for pin in [history.first, history.second, history.third] {
            let state =
                lore_revision::state::State::deserialize(history.source.repository.clone(), pin)
                    .await
                    .unwrap();
            payloads.push(
                state
                    .find_node(history.source.repository.clone(), "original.txt")
                    .await
                    .unwrap()
                    .address
                    .hash,
            );
        }
        // Explicitly seed verified whole-file records from the local source.
        // Online mount/xattr paths themselves still perform no backfill.
        use lore_vfs::core::Backend;
        let digest_path = fixture._root.join("digests");
        let index = lore_vfs::digest_index::DigestIndex::new(&digest_path);
        let mut digests = Vec::new();
        for pin in [history.first, history.second, history.third] {
            let backend =
                lore_vfs::revision::RevisionBackend::new(history.source.repository.clone(), pin)
                    .await
                    .unwrap()
                    .with_digest_index(index.clone());
            let node = backend
                .lookup(&backend.root().key, b"original.txt")
                .await
                .unwrap();
            digests.push(backend.backfill_digest(&node.key, 1024).await.unwrap());
        }
        assert_ne!(digests[0], digests[1]);
        assert_ne!(digests[1], digests[2]);
        let server = RemoteFixture::new(history.source.repository.clone(), payloads);
        let config = RepositoryConfig {
            remote_url: Some(server.url().to_owned()),
            identity: Some("fixture".into()),
            ..Default::default()
        };
        std::fs::write(
            history.source.path.join(".lore/config.toml"),
            toml::to_string(&config).unwrap(),
        )
        .unwrap();
        history.record_metadata();
        let (owner, _guard, _admission) =
            super::super::super::working_mount_local_policy::admitted().await;
        let _cleanup = RunCleanup::new(owner.clone());
        let args = lore::mount_working::LoreMountWorkingArgs {
            experimental: true,
            remote_contents: true,
            source: history.source.path.as_path().into(),
            revision: format!("main@{}", history.first).into(),
            mountpoint: fixture.target.as_path().into(),
            metadata: fixture.metadata.as_path().into(),
            digest_cache: digest_path.as_path().into(),
            backing: fixture.backing.as_path().into(),
            ..Default::default()
        };
        assert_eq!(
            command(
                &owner,
                LoreGlobalArgs::default(),
                LoreCommand::MountWorking(args),
                None
            )
            .await,
            0
        );
        let globals = LoreGlobalArgs {
            repository_path: fixture.target.as_path().into(),
            ..Default::default()
        };
        let path = fixture.target.join("original.txt");
        assert_hash(&path, &digests[0]);
        let mut old = tokio::fs::File::open(&path).await.unwrap();
        let old_directory = std::fs::read_dir(&fixture.target).unwrap();
        assert_eq!(
            sync(&owner, globals.clone(), history.second, false, false).await,
            0
        );
        assert_eq!(
            tokio::fs::read(&path).await.unwrap(),
            b"target second bytes"
        );
        assert_hash(&path, &digests[1]);
        assert!(!fixture.target.join("only-a").exists());
        assert!(fixture.target.join("only-b").exists());
        let mut old_bytes = Vec::new();
        old.seek(std::io::SeekFrom::Start(0)).await.unwrap();
        old.read_to_end(&mut old_bytes).await.unwrap();
        assert_eq!(old_bytes, b"source committed bytes");
        let old_names: Vec<_> = old_directory
            .map(|entry| entry.unwrap().file_name())
            .collect();
        assert!(old_names.iter().any(|name| name == "only-a"));
        assert!(!old_names.iter().any(|name| name == "only-b"));
        {
            let mut file = tokio::fs::OpenOptions::new()
                .write(true)
                .truncate(true)
                .open(&path)
                .await
                .unwrap();
            file.write_all(b"local preserved bytes").await.unwrap();
            file.sync_all().await.unwrap();
        }
        assert_private_bytes_have_no_immutable_hash(&path);
        let before = anchors(&owner, globals.clone()).await;
        assert_ne!(
            sync(&owner, globals.clone(), history.third, false, false).await,
            0
        );
        assert_eq!(anchors(&owner, globals.clone()).await, before);
        assert_eq!(
            tokio::fs::read(&path).await.unwrap(),
            b"local preserved bytes"
        );
        assert!(fixture.target.join("only-b").exists());
        assert!(!fixture.target.join("only-c").exists());
        assert_eq!(
            sync(&owner, globals.clone(), history.third, true, false).await,
            0
        );
        assert_eq!(
            tokio::fs::read(&path).await.unwrap(),
            b"local preserved bytes"
        );
        assert!(fixture.target.join("only-c").exists());
        assert_private_bytes_have_no_immutable_hash(&path);
        assert_eq!(
            sync(&owner, globals.clone(), history.second, false, true).await,
            0
        );
        assert_eq!(
            tokio::fs::read(&path).await.unwrap(),
            b"target second bytes"
        );
        assert_hash(&path, &digests[1]);
        assert!(fixture.target.join("only-b").exists());
        assert!(!fixture.target.join("only-c").exists());
        tokio::fs::File::open(&path)
            .await
            .unwrap()
            .sync_all()
            .await
            .unwrap();
        let private = fixture.target.join("private-final.txt");
        {
            let mut file = tokio::fs::File::create(&private).await.unwrap();
            file.write_all(b"final independent private bytes")
                .await
                .unwrap();
            file.sync_all().await.unwrap();
        }
        assert_private_bytes_have_no_immutable_hash(&private);
        assert_hash(&path, &digests[1]);
        drop(old);
        stop(&owner).await;
        history.assert_source_unchanged().await;
        let (reopened, _reopened_guard, _reopened_admission) =
            super::super::super::working_mount_local_policy::admitted().await;
        let _reopened_cleanup = RunCleanup::new(reopened.clone());
        let args = lore::mount_working::LoreMountWorkingArgs {
            experimental: true,
            remote_contents: true,
            reuse: true,
            source: history.source.path.as_path().into(),
            mountpoint: fixture.target.as_path().into(),
            metadata: fixture.metadata.as_path().into(),
            digest_cache: digest_path.as_path().into(),
            ..Default::default()
        };
        assert_eq!(
            command(
                &reopened,
                LoreGlobalArgs::default(),
                LoreCommand::MountWorking(args),
                None
            )
            .await,
            0
        );
        assert_eq!(
            tokio::fs::read(&path).await.unwrap(),
            b"target second bytes"
        );
        assert_hash(&path, &digests[1]);
        assert!(fixture.target.join("only-b").exists());
        assert!(!fixture.target.join("only-c").exists());
        assert_eq!(
            tokio::fs::read(&private).await.unwrap(),
            b"final independent private bytes"
        );
        assert_private_bytes_have_no_immutable_hash(&private);
        stop(&reopened).await;
        history.assert_source_unchanged().await;
    }))
    .await;
}

mod worker;
