// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use bytes::Bytes;
use lore_base::types::Address;
use lore_base::types::Fragment;
use lore_revision::fs::filesystem_provider::*;
use lore_revision::node::Node;
use lore_revision::state::ChangeStream;
use lore_revision::state::FilesystemDiffStats;
use lore_revision::state::NodeComparison;
use lore_revision::util::path::RelativePath;
use parking_lot::Mutex;

use super::*;

#[derive(Clone)]
struct DigestProvider {
    root: std::path::PathBuf,
    batches: Arc<Mutex<Vec<CommittedContentDigests>>>,
    fail_finalize: bool,
}
struct DigestOperation {
    inner: lore_revision::fs::os::OsOperation,
    provider: DigestProvider,
    finalized: std::sync::atomic::AtomicBool,
}
#[async_trait]
impl FilesystemProvider for DigestProvider {
    fn holds_state(&self) -> bool {
        true
    }
    async fn begin_operation(&self) -> Result<Arc<InstanceOperationImpl>, FsError> {
        let inner = OsFilesystem::new(&self.root).begin_operation();
        Ok(Arc::new(InstanceOperationImpl::external(
            DigestOperation {
                inner,
                provider: self.clone(),
                finalized: std::sync::atomic::AtomicBool::new(false),
            },
            true,
        )))
    }
}
#[async_trait]
impl ExternalOperation for DigestOperation {
    fn wants_content_sha256(&self) -> bool {
        true
    }
    async fn committed_digests(&self, digests: CommittedContentDigests) -> Result<(), FsError> {
        assert!(
            self.finalized.load(Ordering::Acquire),
            "digest delivered before finalize"
        );
        self.provider.batches.lock().push(digests);
        Ok(())
    }
    async fn finalize(
        &self,
        _changes_made: bool,
        _outcome: lore_revision::fs::filesystem_provider::OperationOutcome,
    ) -> Result<(), FsError> {
        self.finalized.store(true, Ordering::Release);
        if self.provider.fail_finalize {
            return Err(FsError::internal("injected finalize failure"));
        }
        Ok(())
    }
    fn abort(&self) {}
    async fn hold_state(
        &self,
        _repository: Arc<RepositoryContext>,
        _target: Arc<State>,
        _options: HoldOptions,
    ) -> Result<(), FsError> {
        Ok(())
    }
    fn stored(&self, _path: &RelativePath, _address: Address, _size: u64, _mode: u16) {}
    async fn committed(
        &self,
        _repository: Arc<RepositoryContext>,
        _committed: Arc<State>,
        _scope: CommittedScope,
        _digests: CommittedContentDigests,
    ) -> Result<(), FsError> {
        Ok(())
    }
}
impl InstanceOperation for DigestOperation {
    fn changes_from_filesystem_to_state(
        &self,
        diff: FilesystemDiffContext,
    ) -> ChangeStream<FilesystemDiffStats> {
        InstanceOperation::changes_from_filesystem_to_state(&self.inner, diff)
    }
    async fn file_info(&self, path: &RelativePath) -> Result<FileInfo, FsError> {
        InstanceOperation::file_info(&self.inner, path).await
    }
    async fn untracked_file_info(&self, path: &RelativePath) -> Result<FileInfo, FsError> {
        InstanceOperation::untracked_file_info(&self.inner, path).await
    }
    async fn holds_name_exactly(&self, path: &RelativePath) -> Option<bool> {
        InstanceOperation::holds_name_exactly(&self.inner, path).await
    }
    async fn names_folding_to(
        &self,
        path: &RelativePath,
        name: &str,
    ) -> Result<Vec<String>, FsError> {
        InstanceOperation::names_folding_to(&self.inner, path, name).await
    }
    async fn read_directory(&self, path: &RelativePath) -> Result<DirectoryListing, FsError> {
        InstanceOperation::read_directory(&self.inner, path).await
    }
    async fn content_source(
        &self,
        path: &RelativePath,
    ) -> Result<lore_storage::ContentSource<'static>, FsError> {
        InstanceOperation::content_source(&self.inner, path).await
    }
    async fn file_holds_content(
        &self,
        repository: Arc<RepositoryContext>,
        path: &RelativePath,
        previous: Address,
        previous_size: u64,
        established: &lore_storage::ContentHashes,
    ) -> Result<NodeComparison, FsError> {
        self.inner
            .file_holds_content(repository, path, previous, previous_size, established)
            .await
    }
    async fn make_executable(&self, path: &RelativePath, executable: bool) -> Result<(), FsError> {
        InstanceOperation::make_executable(&self.inner, path, executable).await
    }
    async fn create_dir_all(&self, path: &RelativePath) -> Result<(), FsError> {
        InstanceOperation::create_dir_all(&self.inner, path).await
    }
    async fn write_file(&self, path: &RelativePath, contents: Bytes) -> Result<(), FsError> {
        InstanceOperation::write_file(&self.inner, path, contents).await
    }
    async fn rename(&self, from: &RelativePath, to: &RelativePath) -> Result<(), FsError> {
        InstanceOperation::rename(&self.inner, from, to).await
    }
    async fn remove(&self, path: &RelativePath) -> Result<(), FsError> {
        InstanceOperation::remove(&self.inner, path).await
    }
    async fn remove_recursive(&self, path: &RelativePath) -> Result<(), FsError> {
        InstanceOperation::remove_recursive(&self.inner, path).await
    }
    async fn write_node(
        &self,
        repository: Arc<RepositoryContext>,
        node: &Node,
        path: &RelativePath,
    ) -> Result<FileInfo, FsError> {
        InstanceOperation::write_node(&self.inner, repository, node, path).await
    }
    async fn set_file_to_immutable_store_contents(
        &self,
        repository: Arc<RepositoryContext>,
        address: Address,
        path: &RelativePath,
    ) -> Result<(Fragment, Option<FileInfo>), FsError> {
        InstanceOperation::set_file_to_immutable_store_contents(
            &self.inner,
            repository,
            address,
            path,
        )
        .await
    }
    async fn copy_file(
        &self,
        source_path: &RelativePath,
        destination_path: &RelativePath,
    ) -> Result<(), FsError> {
        InstanceOperation::copy_file(&self.inner, source_path, destination_path).await
    }
    async fn infer_is_diffable(&self, path: &RelativePath) -> Result<bool, FsError> {
        InstanceOperation::infer_is_diffable(&self.inner, path).await
    }
    async fn infer_is_conflicted(&self, path: &RelativePath) -> Result<bool, FsError> {
        InstanceOperation::infer_is_conflicted(&self.inner, path).await
    }
}

#[tokio::test]
async fn successful_commit_delivers_exact_ingestion_identities_after_finalize() {
    let dir = lore_base::test_util::TempDir::new("lore-ingestion-");
    let bytes: Vec<u8> = (0..lore_storage::FRAGMENT_SIZE_THRESHOLD * 3 + 17)
        .map(|i| (i.wrapping_mul(137) % 251) as u8)
        .collect();
    std::fs::write(dir.path().join("large"), &bytes).unwrap();
    std::fs::write(dir.path().join("empty"), []).unwrap();
    let provider = DigestProvider {
        root: dir.to_path_buf(),
        batches: Arc::default(),
        fail_finalize: false,
    };
    let observed = provider.batches.clone();
    let root = dir.to_path_buf();
    let (immutable, mutable, execution) = test_store_create().await.unwrap();
    let (signature, repository, nodes) = runtime()
        .spawn(LORE_CONTEXT.scope(execution, async move {
            let (fixture, committing) = committing_repository(
                &root,
                immutable,
                mutable,
                Arc::new(provider),
                Arc::default(),
            )
            .await;
            stage_working_tree(&fixture, &root).await;
            let signature = commit_staged(&committing, &fixture.token, test_commit())
                .await
                .unwrap();
            let state = State::deserialize(committing.clone(), signature)
                .await
                .unwrap();
            let nodes = [
                state.find_node(committing.clone(), "large").await.unwrap(),
                state.find_node(committing.clone(), "empty").await.unwrap(),
            ];
            (signature, committing.id, nodes)
        }))
        .await
        .unwrap();
    let batches = observed.lock();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].repository(), repository);
    assert_eq!(batches[0].revision(), signature);
    assert_eq!(batches[0].content().len(), 2);
    for (node, input) in nodes.iter().zip([bytes.as_slice(), &[]]) {
        let digest = batches[0]
            .content()
            .iter()
            .find(|d| d.address() == node.address)
            .unwrap();
        assert_eq!(digest.size(), node.size);
        assert_eq!(
            digest.sha256().as_slice(),
            ring::digest::digest(&ring::digest::SHA256, input).as_ref()
        );
    }
}

#[tokio::test]
async fn failed_dry_and_failed_finalize_commits_cannot_deliver_digests() {
    for failure in ["missing", "dry", "finalize"] {
        let dir = lore_base::test_util::TempDir::new("lore-ingestion-failure-");
        std::fs::write(dir.path().join("file"), b"content").unwrap();
        let provider = DigestProvider {
            root: dir.to_path_buf(),
            batches: Arc::default(),
            fail_finalize: failure == "finalize",
        };
        let observed = provider.batches.clone();
        let root = dir.to_path_buf();
        let (immutable, mutable, execution) = test_store_create().await.unwrap();
        runtime()
            .spawn(LORE_CONTEXT.scope(execution, async move {
                let (fixture, committing) = committing_repository(
                    &root,
                    immutable,
                    mutable,
                    Arc::new(provider),
                    Arc::default(),
                )
                .await;
                stage_working_tree(&fixture, &root).await;
                if failure == "missing" {
                    std::fs::remove_file(root.join("file")).unwrap();
                }
                if failure == "dry" {
                    let execution = test_execution_with(LoreGlobalArgs {
                        dry_run: 1,
                        ..Default::default()
                    });
                    LORE_CONTEXT
                        .scope(
                            execution,
                            commit_staged(&committing, &fixture.token, test_commit()),
                        )
                        .await
                        .unwrap();
                } else {
                    assert!(
                        commit_staged(&committing, &fixture.token, test_commit())
                            .await
                            .is_err()
                    );
                }
            }))
            .await
            .unwrap();
        assert!(observed.lock().is_empty(), "{failure} published a batch");
    }
}

#[tokio::test]
async fn digest_revision_stays_committed_when_dirty_nodes_preserve_a_staged_anchor() {
    let dir = lore_base::test_util::TempDir::new("lore-ingestion-dirty-");
    for name in ["selected", "dirty"] {
        std::fs::write(dir.path().join(name), b"before").unwrap();
    }
    let provider = DigestProvider {
        root: dir.to_path_buf(),
        batches: Arc::default(),
        fail_finalize: false,
    };
    let observed = provider.batches.clone();
    let root = dir.to_path_buf();
    let (immutable, mutable, execution) = test_store_create().await.unwrap();
    let (signature, staged_revision) = runtime()
        .spawn(LORE_CONTEXT.scope(execution, async move {
            let (fixture, committing) = committing_repository(
                &root,
                immutable,
                mutable,
                Arc::new(provider),
                Arc::default(),
            )
            .await;
            stage_working_tree(&fixture, &root).await;
            commit_staged(&fixture.repository, &fixture.token, test_commit())
                .await
                .unwrap();
            std::fs::write(root.join("selected"), b"after").unwrap();
            lore_revision::file::stage::stage(
                fixture.repository.clone(),
                &fixture.token,
                LoreArray::from_vec(vec![LoreString::from(&root.join("selected"))]),
                lore_revision::stage::StageOptions {
                    scan: true,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
            let (_, staged, _) = State::deserialize_current_and_staged(fixture.repository.clone())
                .await
                .unwrap();
            let staged = staged.unwrap();
            let dirty = staged
                .find_node_link(fixture.repository.clone(), "dirty")
                .await
                .unwrap();
            staged
                .node_mark_dirty(
                    fixture.repository.clone(),
                    dirty.node,
                    lore_revision::node::NodeFlags::DirtyModify,
                    true,
                )
                .await
                .unwrap();
            let anchor = staged
                .serialize(fixture.repository.clone(), &fixture.token)
                .await
                .unwrap();
            lore_revision::instance::store_staged_anchor(&fixture.repository, anchor)
                .await
                .unwrap();
            let signature = commit_staged(&committing, &fixture.token, test_commit())
                .await
                .unwrap();
            let (_, staged, _) = State::deserialize_current_and_staged(committing)
                .await
                .unwrap();
            (signature, staged.unwrap().revision())
        }))
        .await
        .unwrap();
    assert_ne!(signature, staged_revision);
    let batches = observed.lock();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].revision(), signature);
    assert_eq!(batches[0].content().len(), 1);
}

#[tokio::test]
async fn queued_repository_batches_stay_separate_and_failed_work_discards_all() {
    for abort in [false, true] {
        let dir = lore_base::test_util::TempDir::new("lore-ingestion-queue-");
        let path = dir.path().join("content");
        std::fs::write(&path, b"ingested bytes").unwrap();
        let (store, _, execution) = test_store_create().await.unwrap();
        let provider = DigestProvider {
            root: dir.to_path_buf(),
            batches: Arc::default(),
            fail_finalize: false,
        };
        let observed = provider.batches.clone();
        let a = RepositoryId::from([0xa1; 16]);
        let b = RepositoryId::from([0xb2; 16]);
        runtime()
            .spawn(LORE_CONTEXT.scope(execution, async move {
                let mut records = Vec::new();
                for owner in [a, b] {
                    let (_, digest) = lore_storage::write::write_from_file_with_sha256(
                        store.clone(),
                        owner,
                        &lore_storage::ContentSource::file(&path),
                        lore_base::types::Context::default(),
                        lore_storage::WriteOptions::default(),
                        None,
                        lore_storage::WriteContext::none(),
                    )
                    .await
                    .unwrap();
                    records.push(digest);
                }
                let result: Result<(), FsError> =
                    with_operation(Arc::new(provider), async |operation| {
                        operation.record_ingested(a, records[0].clone());
                        operation.record_ingested(b, records[1].clone());
                        operation.queue_content_digests(b, Hash::from_u64(2));
                        operation.queue_content_digests(a, Hash::from_u64(1));
                        if abort {
                            Err(FsError::internal("later work failed"))
                        } else {
                            Ok(())
                        }
                    })
                    .await;
                assert_eq!(result.is_err(), abort);
            }))
            .await
            .unwrap();
        let batches = observed.lock();
        if abort {
            assert!(batches.is_empty());
        } else {
            assert_eq!(batches.len(), 2);
            assert_eq!(
                (batches[0].repository(), batches[0].revision()),
                (b, Hash::from_u64(2))
            );
            assert_eq!(
                (batches[1].repository(), batches[1].revision()),
                (a, Hash::from_u64(1))
            );
            assert!(batches.iter().all(|batch| batch.content().len() == 1));
        }
    }
}

#[tokio::test]
async fn nested_child_commit_delivers_the_child_identity_and_revision() {
    use lore_revision::node::NodeFlags;
    use lore_revision::node::ROOT_NODE;
    let dir = lore_base::test_util::TempDir::new("lore-ingestion-linked-");
    std::fs::create_dir(dir.path().join("vendor")).unwrap();
    std::fs::write(dir.path().join("vendor/content"), b"child bytes").unwrap();
    let provider = DigestProvider {
        root: dir.to_path_buf(),
        batches: Arc::default(),
        fail_finalize: false,
    };
    let observed = provider.batches.clone();
    let root = dir.to_path_buf();
    let (immutable, mutable, execution) = test_store_create().await.unwrap();
    let (child_id, pin, node) = runtime()
        .spawn(LORE_CONTEXT.scope(execution, async move {
            let (fixture, committing) = committing_repository(
                &root,
                immutable,
                mutable,
                Arc::new(provider),
                Arc::default(),
            )
            .await;
            let child_id = RepositoryId::from([0xcc; 16]);
            let child = committing.to_link_context(child_id).await;
            let child_state = State::new();
            let file = child_state
                .node_add(
                    child.clone(),
                    ROOT_NODE,
                    Node {
                        flags: NodeFlags::File.bits(),
                        name_hash: lore_storage::hash::hash_string("content"),
                        ..Default::default()
                    },
                    "content",
                )
                .await
                .unwrap();
            child_state
                .node_mark_staged(
                    child.clone(),
                    file,
                    NodeFlags::StagedAdd,
                    NodeFlags::DirtyAdd,
                )
                .await
                .unwrap();
            let uncommitted_child = child_state
                .serialize(child.clone(), &fixture.token)
                .await
                .unwrap();
            let parent_state = State::new();
            let link = parent_state
                .node_add(
                    committing.clone(),
                    ROOT_NODE,
                    Node {
                        flags: NodeFlags::Link.bits(),
                        child: ROOT_NODE,
                        address: Address {
                            context: child_id.into(),
                            hash: uncommitted_child,
                        },
                        name_hash: lore_storage::hash::hash_string("vendor"),
                        ..Default::default()
                    },
                    "vendor",
                )
                .await
                .unwrap();
            let (_, branch) = lore_revision::instance::load_current_anchor_boxed(&committing)
                .await
                .unwrap();
            parent_state
                .link_add(
                    committing.clone(),
                    child_id,
                    branch,
                    uncommitted_child,
                    link,
                    lore_revision::link::LinkFlags::NoFlags,
                )
                .await
                .unwrap();
            parent_state
                .node_mark_staged(
                    committing.clone(),
                    link,
                    NodeFlags::StagedAdd,
                    NodeFlags::DirtyAdd,
                )
                .await
                .unwrap();
            let staged = parent_state
                .serialize(committing.clone(), &fixture.token)
                .await
                .unwrap();
            lore_revision::instance::store_staged_anchor(&committing, staged)
                .await
                .unwrap();
            let signature = commit_staged(&committing, &fixture.token, test_commit())
                .await
                .unwrap();
            let parent = State::deserialize(committing.clone(), signature)
                .await
                .unwrap();
            let pin = parent
                .find_node(committing, "vendor")
                .await
                .unwrap()
                .address
                .hash;
            let state = State::deserialize(child.clone(), pin).await.unwrap();
            let node = state.find_node(child, "content").await.unwrap();
            (child_id, pin, node)
        }))
        .await
        .unwrap();
    let batches = observed.lock();
    let child = batches
        .iter()
        .find(|batch| batch.repository() == child_id)
        .expect("child digest batch");
    assert_eq!(child.revision(), pin);
    assert_eq!(child.content().len(), 1);
    assert_eq!(child.content()[0].address(), node.address);
    assert_eq!(child.content()[0].size(), node.size);
    assert_eq!(
        child.content()[0].sha256().as_slice(),
        ring::digest::digest(&ring::digest::SHA256, b"child bytes").as_ref()
    );
}
