// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! A sender that delivers every event to both the in-process sender and a
//! notification plugin.
//!
//! The gRPC Subscribe RPC is served by [`crate::grpc::notification_service`],
//! which holds the in-process sender concretely: it needs that sender's
//! `register`, and `register` is not on the [`NotificationSender`] trait because
//! a plugin publishing to a remote broker has nothing to return a broadcast
//! receiver from. So configuring a plugin cannot move Subscribe to it.
//!
//! Without this, configuring one means losing the other: a deployment that
//! published to a broker served `UNIMPLEMENTED` to every client still
//! subscribing. Running both senders costs one extra broadcast send per event
//! and keeps the two independent.

use std::sync::Arc;

use async_trait::async_trait;
use lore_base::types::Address;
use lore_base::types::Hash;
use lore_base::types::LockResource;
use lore_revision::lore::BranchId;
use lore_revision::lore::RepositoryId;
use lore_revision::notification::NotificationError;
use lore_revision::notification::NotificationSender as NotificationSenderTrait;

/// Delivers to the in-process sender and to a plugin, in that order.
pub struct NotificationSender {
    local: Arc<crate::notification::local::NotificationSender>,
    plugin: Arc<dyn NotificationSenderTrait>,
}

impl NotificationSender {
    pub fn new(
        local: Arc<crate::notification::local::NotificationSender>,
        plugin: Arc<dyn NotificationSenderTrait>,
    ) -> Self {
        Self { local, plugin }
    }
}

#[async_trait]
impl NotificationSenderTrait for NotificationSender {
    async fn branch_created(&self, repository: RepositoryId, branch: BranchId) {
        self.local.branch_created(repository, branch).await;
        self.plugin.branch_created(repository, branch).await;
    }

    async fn branch_pushed(
        &self,
        repository: RepositoryId,
        branch: BranchId,
        user_id: &str,
        revision: Hash,
        revision_number: u64,
    ) {
        self.local
            .branch_pushed(repository, branch, user_id, revision, revision_number)
            .await;
        self.plugin
            .branch_pushed(repository, branch, user_id, revision, revision_number)
            .await;
    }

    async fn branch_deleted(&self, repository: RepositoryId, branch: BranchId) {
        self.local.branch_deleted(repository, branch).await;
        self.plugin.branch_deleted(repository, branch).await;
    }

    async fn resource_locked(
        &self,
        repository: RepositoryId,
        branch: BranchId,
        user_id: &str,
        resources: &[LockResource],
    ) {
        self.local
            .resource_locked(repository, branch, user_id, resources)
            .await;
        self.plugin
            .resource_locked(repository, branch, user_id, resources)
            .await;
    }

    async fn resource_unlocked(
        &self,
        repository: RepositoryId,
        branch: BranchId,
        user_id: &str,
        resources: &[LockResource],
    ) {
        self.local
            .resource_unlocked(repository, branch, user_id, resources)
            .await;
        self.plugin
            .resource_unlocked(repository, branch, user_id, resources)
            .await;
    }

    /// The plugin's answer is the one that counts. Obliterate propagation is
    /// what a plugin's receivers exist for; the in-process sender has no
    /// propagation to report on and answers `Ok` unconditionally, so reporting
    /// its success over a plugin's failure would claim a propagation that did
    /// not happen.
    async fn obliterate(
        &self,
        repository: RepositoryId,
        address: Address,
    ) -> Result<(), NotificationError> {
        let _ = self.local.obliterate(repository, address).await;
        self.plugin.obliterate(repository, address).await
    }

    #[allow(clippy::too_many_arguments)]
    async fn compliance_check(
        &self,
        stream_name: &str,
        repository: RepositoryId,
        branch: BranchId,
        user_id: &str,
        revision: Hash,
        revision_number: u64,
        ip_addr: Option<String>,
    ) {
        self.local
            .compliance_check(
                stream_name,
                repository,
                branch,
                user_id,
                revision,
                revision_number,
                ip_addr.clone(),
            )
            .await;
        self.plugin
            .compliance_check(
                stream_name,
                repository,
                branch,
                user_id,
                revision,
                revision_number,
                ip_addr,
            )
            .await;
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use super::*;

    #[derive(Default)]
    struct CountingSender {
        calls: AtomicUsize,
        obliterate_fails: bool,
    }

    #[async_trait]
    impl NotificationSenderTrait for CountingSender {
        async fn branch_created(&self, _: RepositoryId, _: BranchId) {
            self.calls.fetch_add(1, Ordering::SeqCst);
        }
        async fn branch_pushed(&self, _: RepositoryId, _: BranchId, _: &str, _: Hash, _: u64) {
            self.calls.fetch_add(1, Ordering::SeqCst);
        }
        async fn branch_deleted(&self, _: RepositoryId, _: BranchId) {
            self.calls.fetch_add(1, Ordering::SeqCst);
        }
        async fn resource_locked(&self, _: RepositoryId, _: BranchId, _: &str, _: &[LockResource]) {
            self.calls.fetch_add(1, Ordering::SeqCst);
        }
        async fn resource_unlocked(
            &self,
            _: RepositoryId,
            _: BranchId,
            _: &str,
            _: &[LockResource],
        ) {
            self.calls.fetch_add(1, Ordering::SeqCst);
        }
        async fn obliterate(&self, _: RepositoryId, _: Address) -> Result<(), NotificationError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self.obliterate_fails {
                return Err(NotificationError::internal("propagation failed"));
            }
            Ok(())
        }
        #[allow(clippy::too_many_arguments)]
        async fn compliance_check(
            &self,
            _: &str,
            _: RepositoryId,
            _: BranchId,
            _: &str,
            _: Hash,
            _: u64,
            _: Option<String>,
        ) {
            self.calls.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn ids() -> (RepositoryId, BranchId) {
        (
            RepositoryId::from(&[0x01u8; 16][..]),
            BranchId::from(&[0x02u8; 16][..]),
        )
    }

    /// The in-process subscriber must still see an event while a plugin is
    /// configured: losing Subscribe is the thing this type exists to prevent.
    #[tokio::test]
    async fn an_event_reaches_an_in_process_subscriber_and_the_plugin() {
        let (repository, branch) = ids();
        let local = Arc::new(crate::notification::local::NotificationSender::default());
        let mut subscriber = local.register(repository);
        let plugin = Arc::new(CountingSender::default());

        let fanout = NotificationSender::new(local, plugin.clone());
        fanout
            .branch_pushed(repository, branch, "someone", Hash::from([0x03u8; 32]), 7)
            .await;

        assert_eq!(
            plugin.calls.load(Ordering::SeqCst),
            1,
            "the plugin was told"
        );
        let event = subscriber
            .try_recv()
            .expect("the in-process subscriber was told too");
        assert!(matches!(
            event.event,
            Some(lore_proto::lore::notification::event::Event::BranchPushed(
                _
            ))
        ));
    }

    /// A plugin that cannot propagate an obliteration must not have its failure
    /// masked by the in-process sender, which always answers `Ok`.
    #[tokio::test]
    async fn obliterate_reports_the_plugin_failure_rather_than_the_local_success() {
        let (repository, _) = ids();
        let local = Arc::new(crate::notification::local::NotificationSender::default());
        let plugin = Arc::new(CountingSender {
            calls: AtomicUsize::new(0),
            obliterate_fails: true,
        });

        let fanout = NotificationSender::new(local, plugin);
        let result = fanout.obliterate(repository, Address::default()).await;

        assert!(result.is_err(), "the plugin's failure is the answer");
    }
}
