// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
#![allow(unused_variables)]

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use lore_base::types::Address;
use lore_base::types::Fragment;
use lore_revision::fs::filesystem_provider::*;
use lore_revision::node::Node;
use lore_revision::repository::RepositoryContext;
use lore_revision::state::ChangeStream;
use lore_revision::state::FilesystemDiffStats;
use lore_revision::state::NodeComparison;
use lore_revision::state::State;
use lore_revision::util::path::RelativePath;
use parking_lot::Mutex;

include!("helper.rs");

#[derive(Default)]
struct Events {
    calls: Vec<&'static str>,
    finalized: Vec<bool>,
    outcomes: Vec<OperationOutcome>,
    holds: Vec<(Arc<State>, HoldOptions)>,
    commits: Vec<Arc<State>>,
    scopes: Vec<(String, lore_base::types::Hash, u32, String)>,
    stored: Vec<(String, Address, u64, u16)>,
}

#[derive(Clone, Default)]
struct FakeProvider {
    events: Arc<Mutex<Events>>,
    fail: bool,
    holds_state: bool,
    durable_handoff: bool,
    block_finalize: Option<(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>)>,
    block_commit: Option<(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>)>,
}

#[async_trait]
impl FilesystemProvider for FakeProvider {
    async fn begin_operation(&self) -> Result<Arc<InstanceOperationImpl>, FsError> {
        Ok(Arc::new(InstanceOperationImpl::external(
            self.clone(),
            self.holds_state,
        )))
    }
    fn holds_state(&self) -> bool {
        self.holds_state
    }
}

struct Listing(bool);
#[async_trait]
impl ExternalDirectoryListing for Listing {
    async fn next(&mut self) -> Option<Result<DirectoryEntry, FsError>> {
        if std::mem::replace(&mut self.0, false) {
            Some(Ok(DirectoryEntry {
                name: "child".into(),
                name_hash: 42,
                info: FileInfo::Directory,
            }))
        } else {
            None
        }
    }
}

impl InstanceOperation for FakeProvider {
    fn changes_from_filesystem_to_state(
        &self,
        diff: FilesystemDiffContext,
    ) -> ChangeStream<FilesystemDiffStats> {
        self.events
            .lock()
            .calls
            .push("changes_from_filesystem_to_state");
        ChangeStream::nothing()
    }
    async fn file_info(&self, path: &RelativePath) -> Result<FileInfo, FsError> {
        self.events.lock().calls.push("file_info");
        Ok(FileInfo::Directory)
    }
    async fn untracked_file_info(&self, path: &RelativePath) -> Result<FileInfo, FsError> {
        self.events.lock().calls.push("untracked_file_info");
        Ok(FileInfo::Directory)
    }
    async fn holds_name_exactly(&self, path: &RelativePath) -> Option<bool> {
        self.events.lock().calls.push("holds_name_exactly");
        Some(true)
    }
    async fn names_folding_to(
        &self,
        path: &RelativePath,
        name: &str,
    ) -> Result<Vec<String>, FsError> {
        self.events.lock().calls.push("names_folding_to");
        Err(FsError::internal("injected operation error"))
    }
    async fn read_directory(&self, path: &RelativePath) -> Result<DirectoryListing, FsError> {
        self.events.lock().calls.push("read_directory");
        Ok(DirectoryListing::new(
            StaticDispatchDirectoryListing::External(Box::new(Listing(true))),
        ))
    }
    async fn content_source(
        &self,
        path: &RelativePath,
    ) -> Result<lore_storage::ContentSource<'static>, FsError> {
        self.events.lock().calls.push("content_source");
        Err(FsError::internal("injected operation error"))
    }
    async fn file_holds_content(
        &self,
        repository: Arc<RepositoryContext>,
        path: &RelativePath,
        previous: Address,
        previous_size: u64,
        established: &lore_storage::ContentHashes,
    ) -> Result<NodeComparison, FsError> {
        self.events.lock().calls.push("file_holds_content");
        Err(FsError::internal("injected operation error"))
    }
    async fn make_executable(&self, path: &RelativePath, executable: bool) -> Result<(), FsError> {
        self.events.lock().calls.push("make_executable");
        Err(FsError::internal("injected operation error"))
    }
    async fn create_dir_all(&self, path: &RelativePath) -> Result<(), FsError> {
        self.events.lock().calls.push("create_dir_all");
        Err(FsError::internal("injected operation error"))
    }
    async fn write_file(&self, path: &RelativePath, contents: Bytes) -> Result<(), FsError> {
        self.events.lock().calls.push("write_file");
        Err(FsError::internal("injected operation error"))
    }
    async fn rename(&self, from: &RelativePath, to: &RelativePath) -> Result<(), FsError> {
        self.events.lock().calls.push("rename");
        Err(FsError::internal("injected operation error"))
    }
    async fn remove(&self, path: &RelativePath) -> Result<(), FsError> {
        self.events.lock().calls.push("remove");
        Err(FsError::internal("injected operation error"))
    }
    async fn remove_recursive(&self, path: &RelativePath) -> Result<(), FsError> {
        self.events.lock().calls.push("remove_recursive");
        Err(FsError::internal("injected operation error"))
    }
    async fn write_node(
        &self,
        repository: Arc<RepositoryContext>,
        node: &Node,
        path: &RelativePath,
    ) -> Result<FileInfo, FsError> {
        self.events.lock().calls.push("write_node");
        Err(FsError::internal("injected operation error"))
    }
    async fn set_file_to_immutable_store_contents(
        &self,
        repository: Arc<RepositoryContext>,
        address: Address,
        path: &RelativePath,
    ) -> Result<(Fragment, Option<FileInfo>), FsError> {
        self.events
            .lock()
            .calls
            .push("set_file_to_immutable_store_contents");
        Err(FsError::internal("injected operation error"))
    }
    async fn copy_file(
        &self,
        source_path: &RelativePath,
        destination_path: &RelativePath,
    ) -> Result<(), FsError> {
        self.events.lock().calls.push("copy_file");
        Err(FsError::internal("injected operation error"))
    }
    async fn infer_is_diffable(&self, path: &RelativePath) -> Result<bool, FsError> {
        self.events.lock().calls.push("infer_is_diffable");
        Err(FsError::internal("injected operation error"))
    }
    async fn infer_is_conflicted(&self, path: &RelativePath) -> Result<bool, FsError> {
        self.events.lock().calls.push("infer_is_conflicted");
        Err(FsError::internal("injected operation error"))
    }
}

#[async_trait]
impl ExternalOperation for FakeProvider {
    fn requires_durable_commit_handoff(&self) -> bool {
        self.durable_handoff
    }
    async fn finalize(&self, changes_made: bool, outcome: OperationOutcome) -> Result<(), FsError> {
        self.events.lock().outcomes.push(outcome);
        self.events.lock().finalized.push(changes_made);
        if let Some((started, resume)) = &self.block_finalize {
            started.notify_one();
            resume.notified().await;
        }
        if self.fail {
            Err(FsError::internal("injected finalize error"))
        } else {
            Ok(())
        }
    }
    fn abort(&self) {
        self.events.lock().outcomes.push(OperationOutcome::Aborted);
    }
    async fn hold_state(
        &self,
        repository: Arc<RepositoryContext>,
        target: Arc<State>,
        options: HoldOptions,
    ) -> Result<(), FsError> {
        self.events.lock().holds.push((target, options));
        if self.fail {
            Err(FsError::internal("injected hold error"))
        } else {
            Ok(())
        }
    }
    fn stored(&self, path: &RelativePath, address: Address, size: u64, mode: u16) {
        self.events
            .lock()
            .stored
            .push((path.to_string(), address, size, mode));
    }
    async fn committed(
        &self,
        repository: Arc<RepositoryContext>,
        committed: Arc<State>,
        scope: CommittedScope,
        _digests: CommittedContentDigests,
    ) -> Result<(), FsError> {
        self.events.lock().scopes.push((
            scope.mount_path().as_str().to_owned(),
            scope.source_revision(),
            scope.source_root(),
            scope.source_path().to_owned(),
        ));
        self.events.lock().commits.push(committed);
        if let Some((started, resume)) = &self.block_commit {
            started.notify_one();
            resume.notified().await;
        }
        if self.fail {
            Err(FsError::internal("injected commit error"))
        } else {
            Ok(())
        }
    }
}

#[path = "external_provider/durable_handoff.rs"]
mod durable_handoff;

async fn repository() -> Arc<RepositoryContext> {
    let (immutable, mutable, _) = test_store_create().await.unwrap();
    in_memory::repository(immutable, mutable)
}

#[tokio::test]
async fn injected_provider_dispatches_reads_writes_and_lazy_listings() {
    let provider = Arc::new(FakeProvider::default());
    let path = "file".parse::<RelativePath>().unwrap();
    with_operation(provider.clone(), async |operation| {
        assert_eq!(operation.file_info(&path).await?, FileInfo::Directory);
        assert_eq!(
            operation.untracked_file_info(&path).await?,
            FileInfo::Directory
        );
        assert_eq!(operation.holds_name_exactly(&path).await, Some(true));
        let mut listing = operation.read_directory(&path).await?;
        assert_eq!(listing.next().await.unwrap()?.name, "child");
        assert!(listing.next().await.is_none());
        assert!(operation.names_folding_to(&path, "FILE").await.is_err());
        assert!(operation.content_source(&path).await.is_err());
        assert!(operation.infer_is_diffable(&path).await.is_err());
        assert!(operation.infer_is_conflicted(&path).await.is_err());
        // Failed writes must still mark the snapshot changed for finalize.
        operation
            .write_file(&path, Bytes::from_static(b"bytes"))
            .await
    })
    .await
    .expect_err("the injected write error reaches the caller");
    let events = provider.events.lock();
    assert_eq!(
        events.calls,
        [
            "file_info",
            "untracked_file_info",
            "holds_name_exactly",
            "read_directory",
            "names_folding_to",
            "content_source",
            "infer_is_diffable",
            "infer_is_conflicted",
            "write_file"
        ]
    );
    assert_eq!(events.finalized, [true]);
}

#[tokio::test]
async fn every_external_mutation_and_content_comparison_dispatches_errors() {
    let provider = FakeProvider::default();
    let operation = provider.begin_operation().await.unwrap();
    let repository = repository().await;
    let path = "file".parse::<RelativePath>().unwrap();
    let node = Node::default();
    let hashes = lore_storage::ContentHashes::default();
    assert!(
        operation
            .file_holds_content(repository.clone(), &path, Address::default(), 0, &hashes)
            .await
            .is_err()
    );
    assert!(operation.make_executable(&path, true).await.is_err());
    assert!(operation.create_dir_all(&path).await.is_err());
    assert!(operation.rename(&path, &path).await.is_err());
    assert!(operation.remove(&path).await.is_err());
    assert!(operation.remove_recursive(&path).await.is_err());
    assert!(
        operation
            .write_node(repository.clone(), &node, &path)
            .await
            .is_err()
    );
    assert!(
        operation
            .set_file_to_immutable_store_contents(repository, Address::default(), &path)
            .await
            .is_err()
    );
    assert!(operation.copy_file(&path, &path).await.is_err());
    operation.finalize().await.unwrap();
    let events = provider.events.lock();
    assert_eq!(
        events.calls,
        [
            "file_holds_content",
            "make_executable",
            "create_dir_all",
            "rename",
            "remove",
            "remove_recursive",
            "write_node",
            "set_file_to_immutable_store_contents",
            "copy_file"
        ]
    );
    assert_eq!(events.finalized, [true]);
}

#[tokio::test]
async fn injected_state_and_commit_handoffs_reach_the_implementation() {
    let provider = FakeProvider {
        holds_state: true,
        ..Default::default()
    };
    let operation = provider.begin_operation().await.unwrap();
    let repository = repository().await;
    let target = State::new();
    let options = HoldOptions {
        keep_local: true,
        ..Default::default()
    };
    operation
        .hold_state(repository.clone(), target.clone(), options)
        .await
        .unwrap();
    let path = "file".parse::<RelativePath>().unwrap();
    operation.stored(&path, Address::default(), 12, 0o755);
    operation
        .committed_from_source(
            lore_revision::state::NodeMapping::root(repository, target.clone()),
            target.clone(),
        )
        .await
        .unwrap();
    operation.finalize().await.unwrap();
    assert!(operation.finalize().await.is_err());
    let events = provider.events.lock();
    assert!(Arc::ptr_eq(&events.holds[0].0, &target));
    assert_eq!(events.holds[0].1, options);
    assert!(Arc::ptr_eq(&events.commits[0], &target));
    assert_eq!(
        events.stored,
        [("file".into(), Address::default(), 12, 0o755)]
    );
    assert_eq!(events.finalized, [true]);
}

#[tokio::test]
async fn injected_lifecycle_errors_are_propagated_and_finalize_is_once_only() {
    let provider = FakeProvider {
        holds_state: true,
        fail: true,
        ..Default::default()
    };
    let operation = provider.begin_operation().await.unwrap();
    let repository = repository().await;
    let target = State::new();
    assert!(
        operation
            .hold_state(repository.clone(), target.clone(), HoldOptions::default())
            .await
            .is_err()
    );
    assert!(
        operation
            .committed_from_source(
                lore_revision::state::NodeMapping::root(repository, target.clone()),
                target
            )
            .await
            .is_err()
    );
    assert!(operation.finalize().await.is_err());
    assert!(operation.finalize().await.is_err());
    assert_eq!(provider.events.lock().finalized, [true]);
}

#[tokio::test]
async fn dry_hold_preserves_options_and_non_state_provider_refuses_handoffs() {
    let provider = FakeProvider {
        holds_state: true,
        ..Default::default()
    };
    let operation = provider.begin_operation().await.unwrap();
    let repository = repository().await;
    let target = State::new();
    let options = HoldOptions {
        dry_run: true,
        overwrite_local: true,
        ..Default::default()
    };
    operation
        .hold_state(repository.clone(), target.clone(), options)
        .await
        .unwrap();
    operation.finalize().await.unwrap();
    assert_eq!(provider.events.lock().holds[0].1, options);
    assert_eq!(provider.events.lock().finalized, [false]);

    let provider = FakeProvider::default();
    let operation = provider.begin_operation().await.unwrap();
    assert!(
        operation
            .hold_state(repository.clone(), target.clone(), options)
            .await
            .is_err()
    );
    operation
        .committed_from_source(
            lore_revision::state::NodeMapping::root(repository, target.clone()),
            target,
        )
        .await
        .unwrap();
    operation.stored(
        &"file".parse::<RelativePath>().unwrap(),
        Address::default(),
        0,
        0,
    );
    operation.finalize().await.unwrap();
    let events = provider.events.lock();
    assert!(events.holds.is_empty());
    assert!(events.commits.is_empty());
    assert!(events.stored.is_empty());
    assert_eq!(events.finalized, [false]);
}

#[tokio::test]
async fn external_diff_stream_dispatches_without_materializing_files() {
    use lore_revision::filter::FilterMode;
    use lore_revision::filter::FilterStates;
    use lore_revision::state::NodeMapping;

    let provider = FakeProvider::default();
    let operation = provider.begin_operation().await.unwrap();
    let repository = repository().await;
    let state = State::new();
    let mut changes = operation.changes_from_filesystem_to_state(FilesystemDiffContext {
        operation: operation.clone(),
        from: NodeMapping::root(repository.clone(), state.clone()),
        current: NodeMapping::root(repository, state),
        filesystem_path: RelativePath::new(),
        states: FilterStates::ROOT,
        from_states: FilterStates::ROOT,
        filter_mode: FilterMode::Full,
        intent: FilesystemDiffIntent::Report,
        layer_mounts: Arc::new(Vec::new()),
        link_mounts: Arc::new(Vec::new()),
        discards: None,
    });
    assert!(changes.next().await.is_none());
    changes.finish().await.unwrap();
    operation.finalize().await.unwrap();
    assert_eq!(
        provider.events.lock().calls,
        ["changes_from_filesystem_to_state"]
    );
    assert_eq!(provider.events.lock().finalized, [false]);
}

#[tokio::test]
async fn operation_outcome_distinguishes_success_failure_and_cancelled_retained_capabilities() {
    let success = Arc::new(FakeProvider::default());
    with_operation(success.clone(), async |_operation| Ok::<_, FsError>(()))
        .await
        .unwrap();
    assert_eq!(
        success.events.lock().outcomes,
        [OperationOutcome::Succeeded]
    );
    let failed = Arc::new(FakeProvider::default());
    with_operation(failed.clone(), async |_operation| {
        Err::<(), _>(FsError::internal("work failed"))
    })
    .await
    .unwrap_err();
    assert_eq!(failed.events.lock().outcomes, [OperationOutcome::Failed]);
    let cancelled = Arc::new(FakeProvider::default());
    let started = Arc::new(tokio::sync::Notify::new());
    let retained = Arc::new(Mutex::new(None));
    #[allow(clippy::disallowed_methods)]
    let task = {
        let provider = cancelled.clone();
        let started = started.clone();
        let retained = retained.clone();
        tokio::spawn(async move {
            with_operation(provider, async move |operation| {
                *retained.lock() = Some(operation);
                started.notify_one();
                std::future::pending::<Result<(), FsError>>().await
            })
            .await
        })
    };
    started.notified().await;
    task.abort();
    let _ = task.await;
    assert_eq!(
        cancelled.events.lock().outcomes,
        [OperationOutcome::Aborted]
    );
    assert!(retained.lock().is_some());
    retained.lock().take();
    assert_eq!(
        cancelled.events.lock().outcomes,
        [OperationOutcome::Aborted]
    );
}
#[tokio::test]
async fn cancellation_during_finalization_aborts_once_and_partial_commits_remain_reported() {
    let started = Arc::new(tokio::sync::Notify::new());
    let resume = Arc::new(tokio::sync::Notify::new());
    let provider = Arc::new(FakeProvider {
        block_finalize: Some((started.clone(), resume)),
        ..Default::default()
    });
    #[allow(clippy::disallowed_methods)]
    let task = {
        let provider = provider.clone();
        tokio::spawn(async move {
            with_operation(provider, async |_operation| Ok::<_, FsError>(())).await
        })
    };
    started.notified().await;
    task.abort();
    let _ = task.await;
    assert_eq!(
        provider.events.lock().outcomes,
        [OperationOutcome::Succeeded, OperationOutcome::Aborted]
    );
    let provider = Arc::new(FakeProvider {
        holds_state: true,
        ..Default::default()
    });
    let child = repository().await;
    let committed = State::new();
    with_operation(provider.clone(), async move |operation| {
        operation
            .committed_from_source(
                lore_revision::state::NodeMapping::root(child, committed.clone()),
                committed,
            )
            .await?;
        Err::<(), _>(FsError::internal("parent failed after child handoff"))
    })
    .await
    .unwrap_err();
    let events = provider.events.lock();
    assert_eq!(events.commits.len(), 1);
    assert_eq!(events.outcomes, [OperationOutcome::Failed]);
}

#[tokio::test]
async fn explicit_abort_racing_successful_finalizer_cannot_report_success() {
    let started = Arc::new(tokio::sync::Notify::new());
    let resume = Arc::new(tokio::sync::Notify::new());
    let provider = Arc::new(FakeProvider {
        block_finalize: Some((started.clone(), resume.clone())),
        ..Default::default()
    });
    let retained = Arc::new(Mutex::new(None));
    #[allow(clippy::disallowed_methods)]
    let task = {
        let provider = provider.clone();
        let retained = retained.clone();
        tokio::spawn(async move {
            with_operation(provider, async move |operation| {
                *retained.lock() = Some(operation);
                Ok::<_, FsError>(())
            })
            .await
        })
    };
    started.notified().await;
    retained.lock().as_ref().unwrap().abort();
    resume.notify_one();
    assert!(task.await.unwrap().is_err());
    assert_eq!(
        provider.events.lock().outcomes,
        [OperationOutcome::Succeeded, OperationOutcome::Aborted]
    );
}

#[tokio::test]
async fn scoped_handoff_preserves_mount_source_and_rejects_invalid_source_before_callback() {
    let provider = FakeProvider {
        holds_state: true,
        ..Default::default()
    };
    let operation = provider.begin_operation().await.unwrap();
    let repository = repository().await;
    let mut source = lore_revision::state::NodeMapping::root(repository.clone(), State::new());
    source.path = "vendor/sub".parse().unwrap();
    operation
        .committed_from_source(source, State::new())
        .await
        .unwrap();
    assert_eq!(
        provider.events.lock().scopes,
        [(
            "vendor/sub".to_string(),
            Default::default(),
            0,
            String::new()
        )]
    );
    operation.finalize().await.unwrap();
    let operation = provider.begin_operation().await.unwrap();
    let mut invalid = lore_revision::state::NodeMapping::root(repository, State::new());
    invalid.node = u32::MAX;
    assert!(
        operation
            .committed_from_source(invalid, State::new())
            .await
            .is_err()
    );
    assert_eq!(provider.events.lock().commits.len(), 1);
    assert!(operation.finalize().await.is_err());
}
#[tokio::test]
async fn cancelled_scoped_handoff_cannot_finalize_as_successful() {
    let started = Arc::new(tokio::sync::Notify::new());
    let resume = Arc::new(tokio::sync::Notify::new());
    let provider = FakeProvider {
        holds_state: true,
        block_commit: Some((started.clone(), resume)),
        ..Default::default()
    };
    let operation = provider.begin_operation().await.unwrap();
    let repository = repository().await;
    {
        let handoff = operation.committed_from_source(
            lore_revision::state::NodeMapping::root(repository, State::new()),
            State::new(),
        );
        tokio::pin!(handoff);
        tokio::select! {result=&mut handoff=>panic!("handoff did not block: {result:?}"),_=started.notified()=>{}}
    }
    assert!(operation.finalize().await.is_err());
    assert_eq!(provider.events.lock().commits.len(), 1);
}
