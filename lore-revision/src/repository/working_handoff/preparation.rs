// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Drain only jobs submitted by this preparation, never another command's work.

use super::*;

struct Finished(Option<tokio::sync::oneshot::Sender<()>>);
impl Drop for Finished {
    fn drop(&mut self) {
        if let Some(sender) = self.0.take() {
            let _delivered = sender.send(());
        }
    }
}
// The final notification is a field destructor after all retained resources and
// inherited accounting. No completion is reported from an early Drop body.
struct PreparationOwner {
    _resources: Arc<WorkingResources>,
    _repository_lock: Option<Arc<RepositoryLock>>,
    _store_hold: Option<Arc<lore_storage::local::store_lock::StoreHold>>,
    _token: RepositoryWriteToken,
    _parent: Option<IoLease>,
    _finished: Finished,
}
impl RepositoryContext {
    pub(super) async fn prepare_io<T>(
        &self,
        future: impl std::future::Future<Output = Result<T, RepositoryError>>,
    ) -> Result<T, RepositoryError> {
        self.validate_handoff_source()?;
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let lease = IoLease::new(Arc::new(PreparationOwner {
            _resources: self.working_resources.as_ref().ok_or_else(refused)?.clone(),
            _repository_lock: self.repo_lock.clone(),
            _store_hold: self.own_stores.clone(),
            _token: self.write_token.as_ref().ok_or_else(refused)?.share(),
            _parent: IoLease::current(),
            _finished: Finished(Some(sender)),
        }));
        let result = lease.scope(future).await;
        // Submitted jobs have inherited pins; this initiating pin must disappear
        // before awaiting their actual final cleanup, including error cleanup.
        drop(lease);
        receiver.await.map_err(|_closed| {
            RepositoryError::internal("Working preparation completion was lost")
        })?;
        result
    }
}
