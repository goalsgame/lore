// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
//! Type erasure for injected providers; built-in providers retain static dispatch.

use super::*;

/// Lifecycle required of an injected operation in addition to [`InstanceOperation`].
///
/// State handoffs and commit notifications are explicit requirements, so a provider serving
/// revisions cannot accidentally inherit the no-op hooks of a materialized filesystem.
#[async_trait]
pub trait ExternalOperation: InstanceOperation + 'static {
    /// Require media synchronization of repository stores before the critical
    /// commit handoff. Private durable checkpoints depend on those immutable
    /// proofs. This does not make later anchor changes atomic with adoption.
    fn requires_durable_commit_handoff(&self) -> bool {
        false
    }
    /// Opt in to whole-file SHA-256 computed while commit reads each file.
    fn wants_content_sha256(&self) -> bool {
        false
    }

    /// Publish optional cache records after the enclosing operation succeeds, the stores
    /// are flushed, and `finalize` succeeds. Keep this cache callback usable after finalization.
    /// Errors are logged as cache availability warnings and do not fail the commit.
    async fn committed_digests(&self, _digests: CommittedContentDigests) -> Result<(), FsError> {
        Ok(())
    }

    /// Finish the snapshot, including after failed writes. Called at most once.
    async fn finalize(&self, changes_made: bool, outcome: OperationOutcome) -> Result<(), FsError>;

    /// Synchronous cancellation cleanup. Must be idempotent with a failed or
    /// cancelled finalizer, preserve edits/commit receipts, and release admission
    /// leases without starting asynchronous adoption.
    fn abort(&self);

    /// Hold the target revision, respecting conflict and dry-run options. Failure must not
    /// acknowledge a completed transition.
    async fn hold_state(
        &self,
        repository: Arc<RepositoryContext>,
        target: Arc<State>,
        options: HoldOptions,
    ) -> Result<(), FsError>;

    /// Record the immutable identity established while reading a file for commit.
    fn stored(&self, path: &RelativePath, address: Address, size: u64, mode: u16);

    /// Receive one owning repository revision after the stores are flushed.
    /// Durable checkpoint providers opt into media synchronization above. Child revisions
    /// may arrive before the parent commits, including when later parent work fails.
    /// Keep their exact immutable identities and preserve unrelated working edits.
    /// `digests` is opaque ingestion evidence for this repository, supplied before
    /// optional post-success cache publication. It may be empty without opt-in.
    /// The immutable commit may already exist when adoption fails. A failed or
    /// cancelled required handoff prevents successful operation finalization.
    async fn committed(
        &self,
        repository: Arc<RepositoryContext>,
        committed: Arc<State>,
        scope: CommittedScope,
        digests: CommittedContentDigests,
    ) -> Result<(), FsError>;
}

/// An injected operation, with dynamic dispatch confined to the external provider boundary.
/// Construct through [`InstanceOperationImpl::external`].
pub struct ExternalOperationDispatch(pub(super) Box<dyn ErasedOperation>);

#[async_trait]
pub(super) trait ErasedOperation: Send + Sync {
    fn requires_durable_commit_handoff(&self) -> bool;
    fn uses_host_filesystem(&self) -> bool;
    fn wants_content_sha256(&self) -> bool;
    async fn committed_digests(&self, digests: CommittedContentDigests) -> Result<(), FsError>;
    async fn finalize(&self, changes_made: bool, outcome: OperationOutcome) -> Result<(), FsError>;

    /// Synchronous cancellation cleanup. Must be idempotent with a failed or
    /// cancelled finalizer, preserve edits/commit receipts, and release admission
    /// leases without starting asynchronous adoption.
    fn abort(&self);
    async fn hold_state(
        &self,
        repository: Arc<RepositoryContext>,
        target: Arc<State>,
        options: HoldOptions,
    ) -> Result<(), FsError>;
    fn changes_from_filesystem_to_state(
        &self,
        diff: FilesystemDiffContext,
    ) -> ChangeStream<FilesystemDiffStats>;
    async fn file_info(&self, path: &RelativePath) -> Result<FileInfo, FsError>;
    async fn untracked_file_info(&self, path: &RelativePath) -> Result<FileInfo, FsError>;
    async fn holds_name_exactly(&self, path: &RelativePath) -> Option<bool>;
    async fn names_folding_to(
        &self,
        path: &RelativePath,
        name: &str,
    ) -> Result<Vec<String>, FsError>;
    async fn read_directory(&self, path: &RelativePath) -> Result<DirectoryListing, FsError>;
    async fn content_source(
        &self,
        path: &RelativePath,
    ) -> Result<lore_storage::ContentSource<'static>, FsError>;
    async fn file_holds_content(
        &self,
        repository: Arc<RepositoryContext>,
        path: &RelativePath,
        previous: Address,
        previous_size: u64,
        established: &lore_storage::ContentHashes,
    ) -> Result<NodeComparison, FsError>;
    async fn make_executable(&self, path: &RelativePath, executable: bool) -> Result<(), FsError>;
    async fn create_dir_all(&self, path: &RelativePath) -> Result<(), FsError>;
    async fn write_file(&self, path: &RelativePath, contents: Bytes) -> Result<(), FsError>;
    async fn rename(&self, from: &RelativePath, to: &RelativePath) -> Result<(), FsError>;
    async fn remove(&self, path: &RelativePath) -> Result<(), FsError>;
    async fn remove_recursive(&self, path: &RelativePath) -> Result<(), FsError>;
    async fn write_node(
        &self,
        repository: Arc<RepositoryContext>,
        node: &Node,
        path: &RelativePath,
    ) -> Result<FileInfo, FsError>;
    async fn set_file_to_immutable_store_contents(
        &self,
        repository: Arc<RepositoryContext>,
        address: Address,
        path: &RelativePath,
    ) -> Result<(Fragment, Option<FileInfo>), FsError>;
    async fn copy_file(
        &self,
        source_path: &RelativePath,
        destination_path: &RelativePath,
    ) -> Result<(), FsError>;
    async fn infer_is_diffable(&self, path: &RelativePath) -> Result<bool, FsError>;
    async fn infer_is_conflicted(&self, path: &RelativePath) -> Result<bool, FsError>;
    fn stored(&self, path: &RelativePath, address: Address, size: u64, mode: u16);
    async fn committed(
        &self,
        repository: Arc<RepositoryContext>,
        committed: Arc<State>,
        scope: CommittedScope,
        digests: CommittedContentDigests,
    ) -> Result<(), FsError>;
}

#[async_trait]
impl<T: ExternalOperation> ErasedOperation for T {
    fn requires_durable_commit_handoff(&self) -> bool {
        ExternalOperation::requires_durable_commit_handoff(self)
    }
    fn uses_host_filesystem(&self) -> bool {
        InstanceOperation::uses_host_filesystem(self)
    }
    fn wants_content_sha256(&self) -> bool {
        ExternalOperation::wants_content_sha256(self)
    }
    async fn committed_digests(&self, digests: CommittedContentDigests) -> Result<(), FsError> {
        ExternalOperation::committed_digests(self, digests).await
    }
    async fn finalize(&self, changes_made: bool, outcome: OperationOutcome) -> Result<(), FsError> {
        ExternalOperation::finalize(self, changes_made, outcome).await
    }
    fn abort(&self) {
        ExternalOperation::abort(self);
    }

    async fn hold_state(
        &self,
        repository: Arc<RepositoryContext>,
        target: Arc<State>,
        options: HoldOptions,
    ) -> Result<(), FsError> {
        ExternalOperation::hold_state(self, repository, target, options).await
    }
    fn changes_from_filesystem_to_state(
        &self,
        diff: FilesystemDiffContext,
    ) -> ChangeStream<FilesystemDiffStats> {
        InstanceOperation::changes_from_filesystem_to_state(self, diff)
    }
    async fn file_info(&self, path: &RelativePath) -> Result<FileInfo, FsError> {
        InstanceOperation::file_info(self, path).await
    }
    async fn untracked_file_info(&self, path: &RelativePath) -> Result<FileInfo, FsError> {
        InstanceOperation::untracked_file_info(self, path).await
    }
    async fn holds_name_exactly(&self, path: &RelativePath) -> Option<bool> {
        InstanceOperation::holds_name_exactly(self, path).await
    }
    async fn names_folding_to(
        &self,
        path: &RelativePath,
        name: &str,
    ) -> Result<Vec<String>, FsError> {
        InstanceOperation::names_folding_to(self, path, name).await
    }
    async fn read_directory(&self, path: &RelativePath) -> Result<DirectoryListing, FsError> {
        InstanceOperation::read_directory(self, path).await
    }
    async fn content_source(
        &self,
        path: &RelativePath,
    ) -> Result<lore_storage::ContentSource<'static>, FsError> {
        InstanceOperation::content_source(self, path).await
    }
    async fn file_holds_content(
        &self,
        repository: Arc<RepositoryContext>,
        path: &RelativePath,
        previous: Address,
        previous_size: u64,
        established: &lore_storage::ContentHashes,
    ) -> Result<NodeComparison, FsError> {
        InstanceOperation::file_holds_content(
            self,
            repository,
            path,
            previous,
            previous_size,
            established,
        )
        .await
    }
    async fn make_executable(&self, path: &RelativePath, executable: bool) -> Result<(), FsError> {
        InstanceOperation::make_executable(self, path, executable).await
    }
    async fn create_dir_all(&self, path: &RelativePath) -> Result<(), FsError> {
        InstanceOperation::create_dir_all(self, path).await
    }
    async fn write_file(&self, path: &RelativePath, contents: Bytes) -> Result<(), FsError> {
        InstanceOperation::write_file(self, path, contents).await
    }
    async fn rename(&self, from: &RelativePath, to: &RelativePath) -> Result<(), FsError> {
        InstanceOperation::rename(self, from, to).await
    }
    async fn remove(&self, path: &RelativePath) -> Result<(), FsError> {
        InstanceOperation::remove(self, path).await
    }
    async fn remove_recursive(&self, path: &RelativePath) -> Result<(), FsError> {
        InstanceOperation::remove_recursive(self, path).await
    }
    async fn write_node(
        &self,
        repository: Arc<RepositoryContext>,
        node: &Node,
        path: &RelativePath,
    ) -> Result<FileInfo, FsError> {
        InstanceOperation::write_node(self, repository, node, path).await
    }
    async fn set_file_to_immutable_store_contents(
        &self,
        repository: Arc<RepositoryContext>,
        address: Address,
        path: &RelativePath,
    ) -> Result<(Fragment, Option<FileInfo>), FsError> {
        InstanceOperation::set_file_to_immutable_store_contents(self, repository, address, path)
            .await
    }
    async fn copy_file(
        &self,
        source_path: &RelativePath,
        destination_path: &RelativePath,
    ) -> Result<(), FsError> {
        InstanceOperation::copy_file(self, source_path, destination_path).await
    }
    async fn infer_is_diffable(&self, path: &RelativePath) -> Result<bool, FsError> {
        InstanceOperation::infer_is_diffable(self, path).await
    }
    async fn infer_is_conflicted(&self, path: &RelativePath) -> Result<bool, FsError> {
        InstanceOperation::infer_is_conflicted(self, path).await
    }
    fn stored(&self, path: &RelativePath, address: Address, size: u64, mode: u16) {
        ExternalOperation::stored(self, path, address, size, mode);
    }
    async fn committed(
        &self,
        repository: Arc<RepositoryContext>,
        committed: Arc<State>,
        scope: CommittedScope,
        digests: CommittedContentDigests,
    ) -> Result<(), FsError> {
        ExternalOperation::committed(self, repository, committed, scope, digests).await
    }
}
