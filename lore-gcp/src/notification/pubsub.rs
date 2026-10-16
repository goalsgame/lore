// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! A [`NotificationSender`] that publishes branch and lock events to a Pub/Sub topic.
//!
//! The payload is the encoded `lore.notification.Event` protobuf, the same message
//! `lore_server::notification::local` broadcasts in process, so one decoder serves
//! both backends. Each message also carries attributes — `event_type`, `repository`
//! and, where the event names one, `branch` — so a subscription can filter without
//! decoding the payload.
//!
//! This adds a destination rather than replacing one: the server fans each event
//! out to the in-process sender as well, so the gRPC Subscribe RPC keeps working
//! alongside the topic.

use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::SystemTime;

use async_trait::async_trait;
use bytes::Bytes;
use google_cloud_gax::error::rpc::Code;
use google_cloud_pubsub::client::Publisher;
use google_cloud_pubsub::client::TopicAdmin;
use google_cloud_pubsub::error::PublishError;
use google_cloud_pubsub::model::Message as PubsubMessage;
use lore_base::types::Address;
use lore_base::types::Context;
use lore_base::types::Hash;
use lore_base::types::LockResource;
use lore_proto::lock;
use lore_proto::lore::notification;
use lore_proto::lore::notification::BranchCreated;
use lore_proto::lore::notification::BranchDeleted;
use lore_proto::lore::notification::BranchPushed;
use lore_proto::lore::notification::ResourceLocked;
use lore_proto::lore::notification::ResourceUnlocked;
use lore_revision::lore::BranchId;
use lore_revision::lore::RepositoryId;
use lore_revision::notification::NotificationError;
use lore_revision::notification::NotificationSender;
use prost::Message as _;
use tokio::sync::Semaphore;
use tracing::debug;
use tracing::warn;
use uuid::Uuid;

use crate::gcp_error::GcpError;

/// Configuration for the Pub/Sub notification sender.
#[derive(Clone, Debug)]
pub struct PubSubSettings {
    /// GCP project holding the topic.
    pub project: String,
    /// Topic id within that project, not the full resource name.
    pub topic: String,
    /// Endpoint override, for the emulator or a regional endpoint.
    pub endpoint: Option<String>,
    /// Caps how many publishes this sender is waiting on the outcome of at once.
    pub outcome_concurrency_limit: usize,
}

impl PubSubSettings {
    /// The topic's full resource name, which is what the client dials.
    pub fn topic_resource_name(&self) -> String {
        format!("projects/{}/topics/{}", self.project, self.topic)
    }
}

/// Publishes notification events to a Pub/Sub topic.
pub struct PubSubNotificationSender {
    publisher: Publisher,
    outcomes: OutcomeGate,
}

/// Bounds how many publish outcomes are being awaited at once.
///
/// A Pub/Sub outage would otherwise leave one task per event with nothing
/// draining them. Past the bound the observation is dropped and counted; the
/// publish it belonged to has already been handed to the client either way, so
/// what is lost is the log line, never the message.
#[derive(Clone)]
struct OutcomeGate {
    permits: Arc<Semaphore>,
    unobserved: Arc<AtomicU64>,
}

impl OutcomeGate {
    fn new(limit: usize) -> Self {
        Self {
            permits: Arc::new(Semaphore::new(limit.max(1))),
            unobserved: Arc::new(AtomicU64::new(0)),
        }
    }

    fn unobserved(&self) -> u64 {
        self.unobserved.load(Ordering::Relaxed)
    }

    /// Awaits `outcome` on its own task, or drops the observation and counts it
    /// when no permit is free. Answers whether a permit was taken.
    fn observe<F>(&self, outcome: F, event_type: &'static str, repository: String) -> bool
    where
        F: Future<Output = Result<String, PublishError>> + Send + 'static,
    {
        let Ok(permit) = self.permits.clone().try_acquire_owned() else {
            let dropped = self.unobserved.fetch_add(1, Ordering::Relaxed) + 1;
            debug!(
                event_type,
                repository, dropped, "Publish outcome left unobserved; the gate is full"
            );
            return false;
        };

        lore_base::lore_spawn!("pubsub.publish_outcome", async move {
            let _permit = permit;
            if let Err(error) = outcome.await {
                warn!(
                    event_type,
                    repository,
                    %error,
                    "Failed to publish a notification event to Pub/Sub"
                );
            }
        });
        true
    }
}

impl PubSubNotificationSender {
    /// Builds a publisher for `settings`' topic.
    ///
    /// The publisher is built from the topic's *name* and never contacts it, so a
    /// mistyped topic or a missing `roles/pubsub.publisher` grant yields a sender
    /// that looks healthy and discards everything — delivery is best-effort, so
    /// nothing downstream reports it either. [`report_topic_reachability`] logs
    /// what it can about that at startup; read its documentation for why it cannot
    /// refuse, and do not read this constructor as validating the destination.
    ///
    /// # Errors
    /// [`GcpError::ClientBuild`] when a client cannot be constructed at all. No
    /// outcome of the topic lookup fails construction.
    pub async fn new(settings: &PubSubSettings) -> Result<Self, GcpError> {
        // As `build_storage_clients` and `build_firestore_db` do: a caller that
        // reaches this before any other GCP client must not depend on something
        // else having installed the rustls provider first.
        crate::clients::ensure_crypto_provider();

        let mut builder = Publisher::builder(settings.topic_resource_name());
        if let Some(endpoint) = settings.endpoint.as_ref() {
            builder = builder.with_endpoint(endpoint.clone());
        }

        let publisher = builder.build().await.map_err(GcpError::client_build)?;

        let mut admin_builder = TopicAdmin::builder();
        if let Some(endpoint) = settings.endpoint.as_ref() {
            admin_builder = admin_builder.with_endpoint(endpoint.clone());
        }
        let admin = admin_builder
            .build()
            .await
            .map_err(GcpError::client_build)?;
        report_topic_reachability(&admin, &settings.topic_resource_name()).await;

        Ok(Self {
            publisher,
            outcomes: OutcomeGate::new(settings.outcome_concurrency_limit),
        })
    }

    /// Publishes whose outcome went unobserved because the gate was full.
    pub fn unobserved_publishes(&self) -> u64 {
        self.outcomes.unobserved()
    }

    /// Encodes `event` and hands it to the publisher.
    ///
    /// `NotificationSender`'s event methods return no error for a caller to act on,
    /// so a failed publish can only be reported, not propagated. Awaiting the
    /// outcome inline would put Pub/Sub's latency in front of every push to buy
    /// nothing but a log line, so the message is enqueued here — which is what
    /// `publish` does, synchronously — and its outcome is awaited on a separate
    /// task.
    fn send(&self, repository: RepositoryId, event: notification::event::Event) {
        let (event_type, branch) = describe(&event);
        let repository_hex = hex::encode(Context::from(repository).data());

        let envelope = notification::Event {
            id: Uuid::new_v4().to_string(),
            time: Some(prost_types::Timestamp::from(SystemTime::now())),
            repository: Bytes::from_owner(Context::from(repository)),
            event: Some(event),
        };

        // set_attributes replaces the attribute map rather than extending it, so
        // every attribute has to go in one call.
        let mut attributes = vec![
            ("event_type", event_type.to_string()),
            ("repository", repository_hex.clone()),
        ];
        if let Some(branch) = branch {
            attributes.push(("branch", branch));
        }

        let message = PubsubMessage::new()
            .set_data(envelope.encode_to_vec())
            .set_attributes(attributes);

        self.outcomes
            .observe(self.publisher.publish(message), event_type, repository_hex);
    }
}

/// How long the startup lookup may take before it is abandoned. It is advisory:
/// the server is not waiting on it for anything it needs, so it must not be able
/// to hold readiness behind a Pub/Sub that is slow or unreachable.
const TOPIC_LOOKUP_TIMEOUT: Duration = Duration::from_secs(5);

/// Looks the topic up at startup and says what it finds, so a mistyped topic has
/// something pointing at it before anyone notices the silence.
///
/// Reports rather than refuses, and that is not timidity. `roles/pubsub.publisher`
/// grants exactly `pubsub.topics.publish` -- not `pubsub.topics.get` -- so the
/// correctly scoped, publisher-only identity this plugin is meant to run as is
/// answered `PermissionDenied` here. Failing on that would refuse to start
/// precisely the deployment the documentation asks for. Nor can the failure be
/// narrowed to `NotFound`: without `topics.get`, Pub/Sub answers `PermissionDenied`
/// for an absent topic just as for a present one, so the two cannot be told apart
/// from here.
///
/// Verifying publish permission directly would need `TestIamPermissions`, which
/// `google-cloud-pubsub`'s `TopicAdmin` does not expose. Until it does, or until
/// the deployment also carries metadata read access, a startup log line is the
/// honest extent of this: it catches a typo for anyone reading the log, and it
/// never rejects a working configuration.
///
/// Takes the client rather than building one, so a test can supply the anonymous
/// credentials an emulator needs without that being a production option.
async fn report_topic_reachability(admin: &TopicAdmin, topic: &str) {
    let lookup = admin.get_topic().set_topic(topic).send();
    let Ok(result) = tokio::time::timeout(TOPIC_LOOKUP_TIMEOUT, lookup).await else {
        warn!(
            %topic,
            timeout = ?TOPIC_LOOKUP_TIMEOUT,
            "Gave up confirming the Pub/Sub topic at startup; continuing without it"
        );
        return;
    };

    match result {
        Ok(_) => debug!(%topic, "Pub/Sub topic confirmed at startup"),
        Err(error) => match error.status().map(|status| status.code) {
            Some(Code::NotFound) => warn!(
                %topic,
                "Pub/Sub topic does not exist; it is not created automatically, and every \
                 notification will publish into nothing until it is"
            ),
            // At warn, not debug: the server filters to warn by default, this is
            // the answer the documented publisher-only identity always gets, and it
            // is also the answer a wrong topic name gets. Logging it quietly would
            // hide the startup line the configuration tells operators to look for.
            // It says the check was inconclusive, not that publishing is broken.
            Some(Code::PermissionDenied) => warn!(
                %topic,
                "Could not confirm the Pub/Sub topic: reading its metadata needs \
                 pubsub.topics.get, which roles/pubsub.publisher does not grant. Whether it \
                 exists and accepts writes is unverified -- check the name and the grant"
            ),
            _ => warn!(%topic, %error, "Could not reach Pub/Sub to confirm the topic at startup"),
        },
    }
}

/// The attribute values describing an event: its type, and the branch it names if
/// it names one. `resource_locked` and `resource_unlocked` carry their branch per
/// resource rather than on the event, so they report none.
fn describe(event: &notification::event::Event) -> (&'static str, Option<String>) {
    use notification::event::Event;
    match event {
        Event::BranchPushed(pushed) => ("branch_pushed", Some(hex::encode(&pushed.branch))),
        Event::BranchCreated(created) => ("branch_created", Some(hex::encode(&created.branch))),
        Event::BranchDeleted(deleted) => ("branch_deleted", Some(hex::encode(&deleted.branch))),
        Event::ResourceLocked(_) => ("resource_locked", None),
        Event::ResourceUnlocked(_) => ("resource_unlocked", None),
        Event::Other(_) => ("other", None),
        Event::Obliterate(_) => ("obliterate", None),
    }
}

fn resources(resources: &[LockResource]) -> Vec<lock::Resource> {
    resources
        .iter()
        .map(|resource| lock::Resource {
            branch: Bytes::from(resource.branch),
            hash: Bytes::from(resource.hash),
            description: resource.description.clone(),
        })
        .collect()
}

#[async_trait]
impl NotificationSender for PubSubNotificationSender {
    async fn branch_created(&self, repository: RepositoryId, branch: BranchId) {
        self.send(
            repository,
            notification::event::Event::BranchCreated(BranchCreated {
                branch: Bytes::from_owner(branch),
            }),
        );
    }

    async fn branch_pushed(
        &self,
        repository: RepositoryId,
        branch: BranchId,
        user_id: &str,
        revision: Hash,
        revision_number: u64,
    ) {
        self.send(
            repository,
            notification::event::Event::BranchPushed(BranchPushed {
                revision: Bytes::from_owner(revision),
                revision_number,
                branch: Bytes::from_owner(branch),
                user_id: user_id.to_string(),
            }),
        );
    }

    async fn branch_deleted(&self, repository: RepositoryId, branch: BranchId) {
        self.send(
            repository,
            notification::event::Event::BranchDeleted(BranchDeleted {
                branch: Bytes::from_owner(branch),
            }),
        );
    }

    async fn resource_locked(
        &self,
        repository: RepositoryId,
        _branch: BranchId,
        user_id: &str,
        locked: &[LockResource],
    ) {
        self.send(
            repository,
            notification::event::Event::ResourceLocked(ResourceLocked {
                user_id: user_id.to_string(),
                resources: resources(locked),
            }),
        );
    }

    async fn resource_unlocked(
        &self,
        repository: RepositoryId,
        _branch: BranchId,
        user_id: &str,
        unlocked: &[LockResource],
    ) {
        self.send(
            repository,
            notification::event::Event::ResourceUnlocked(ResourceUnlocked {
                user_id: user_id.to_string(),
                resources: resources(unlocked),
            }),
        );
    }

    /// Obliteration between server instances is coordinated by a receiver, which
    /// this sender does not provide. Reporting success without propagating is only
    /// correct for a single-instance deployment; a second instance needs a
    /// subscriber that applies the obliteration before this may return `Ok`.
    async fn obliterate(
        &self,
        _repository: RepositoryId,
        _address: Address,
    ) -> Result<(), NotificationError> {
        Ok(())
    }

    async fn compliance_check(
        &self,
        _stream_name: &str,
        _repository: RepositoryId,
        _branch: BranchId,
        _user_id: &str,
        _revision: Hash,
        _revision_number: u64,
        _ip_addr: Option<String>,
    ) {
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn topic_resource_name_is_the_full_path() {
        let settings = PubSubSettings {
            project: "my-project".to_string(),
            topic: "lore-events".to_string(),
            endpoint: None,
            outcome_concurrency_limit: 64,
        };

        assert_eq!(
            settings.topic_resource_name(),
            "projects/my-project/topics/lore-events"
        );
    }

    #[test]
    fn branch_events_describe_their_branch() {
        let branch = Bytes::from_static(&[0xab, 0xcd]);

        assert_eq!(
            describe(&notification::event::Event::BranchPushed(BranchPushed {
                revision: Bytes::new(),
                revision_number: 7,
                branch: branch.clone(),
                user_id: String::new(),
            })),
            ("branch_pushed", Some("abcd".to_string()))
        );
        assert_eq!(
            describe(&notification::event::Event::BranchCreated(BranchCreated {
                branch: branch.clone(),
            })),
            ("branch_created", Some("abcd".to_string()))
        );
        assert_eq!(
            describe(&notification::event::Event::BranchDeleted(BranchDeleted {
                branch,
            })),
            ("branch_deleted", Some("abcd".to_string()))
        );
    }

    /// Saturating the gate must not stop anything being published: the gate holds
    /// outcome *observers*, and a publish is handed to the client before one is
    /// taken. This drives it directly, since reaching it through the sender needs
    /// a live Publisher.
    #[tokio::test]
    async fn a_saturated_gate_drops_observations_and_nothing_else() {
        let gate = OutcomeGate::new(2);
        let (release, mut rx) = tokio::sync::broadcast::channel::<()>(1);

        // Two observers that park until released, taking both permits.
        for _ in 0..2 {
            let mut parked = release.subscribe();
            assert!(
                gate.observe(
                    async move {
                        let _ = parked.recv().await;
                        Ok(String::new())
                    },
                    "branch_pushed",
                    "repo".to_string()
                ),
                "a free permit should have been taken"
            );
        }
        // Let both tasks reach their await before the gate is probed again.
        tokio::task::yield_now().await;

        let mut outcomes_run = 0;
        for _ in 0..8 {
            let observed = gate.observe(
                async { Ok::<_, PublishError>(String::new()) },
                "branch_pushed",
                "repo".to_string(),
            );
            if observed {
                outcomes_run += 1;
            }
        }

        assert_eq!(
            outcomes_run, 0,
            "the gate was full, so none should be taken"
        );
        assert_eq!(gate.unobserved(), 8, "every dropped observation is counted");

        // Releasing the parked observers returns the permits.
        let _ = release.send(());
        let _ = rx.recv().await;
        for _ in 0..20 {
            tokio::task::yield_now().await;
            if gate.permits.available_permits() == 2 {
                break;
            }
        }
        assert!(
            gate.observe(
                async { Ok(String::new()) },
                "branch_pushed",
                "repo".to_string()
            ),
            "a permit should be free again once the parked observers finish"
        );
    }

    /// A lock event's branch lives on each resource, not on the event, so there is
    /// no single branch to attribute it to.
    #[test]
    fn lock_events_describe_no_branch() {
        assert_eq!(
            describe(&notification::event::Event::ResourceLocked(
                ResourceLocked::default()
            )),
            ("resource_locked", None)
        );
        assert_eq!(
            describe(&notification::event::Event::ResourceUnlocked(
                ResourceUnlocked::default()
            )),
            ("resource_unlocked", None)
        );
    }
}

/// Tests against the Pub/Sub emulator from `lore-integration-tests/compose.yaml`.
///
/// `google-cloud-pubsub` has no emulator support of its own — it reads no
/// `PUBSUB_EMULATOR_HOST` — so these read that variable themselves and hand the
/// endpoint and anonymous credentials to each builder.
#[cfg(all(test, feature = "integration_tests"))]
mod emulator_tests {
    use std::time::Duration;

    use google_cloud_auth::credentials::anonymous::Builder as AnonymousCredentials;
    use google_cloud_pubsub::client::Subscriber;
    use google_cloud_pubsub::client::SubscriptionAdmin;
    use google_cloud_pubsub::client::TopicAdmin;
    use lore_base::types::Context;

    use super::*;

    const PROJECT: &str = "lore-gcp-emulator-tests";

    /// The emulator's endpoint, and the crypto provider every client built here
    /// needs. `PubSubNotificationSender::new` installs it for the production
    /// path; these tests build their clients directly and so must do it too.
    fn endpoint() -> String {
        crate::clients::ensure_crypto_provider();
        let host = std::env::var("PUBSUB_EMULATOR_HOST")
            .expect("PUBSUB_EMULATOR_HOST; is the Pub/Sub emulator running?");
        format!("http://{host}")
    }

    /// A topic and a subscription on it, named for this test alone so tests
    /// sharing one emulator cannot read each other's messages.
    async fn topic_and_subscription() -> (String, String) {
        let suffix = uuid::Uuid::new_v4().simple().to_string();
        let topic = format!("projects/{PROJECT}/topics/events-{suffix}");
        let subscription = format!("projects/{PROJECT}/subscriptions/reader-{suffix}");

        TopicAdmin::builder()
            .with_endpoint(endpoint())
            .with_credentials(AnonymousCredentials::new().build())
            .build()
            .await
            .expect("building the topic admin client")
            .create_topic()
            .set_name(&topic)
            .send()
            .await
            .expect("creating the topic");

        SubscriptionAdmin::builder()
            .with_endpoint(endpoint())
            .with_credentials(AnonymousCredentials::new().build())
            .build()
            .await
            .expect("building the subscription admin client")
            .create_subscription()
            .set_name(&subscription)
            .set_topic(&topic)
            .send()
            .await
            .expect("creating the subscription");

        (topic, subscription)
    }

    /// A sender publishing to `topic`. Built here rather than through
    /// `PubSubNotificationSender::new` because the emulator needs anonymous
    /// credentials, which are not a production configuration option.
    async fn sender_for(topic: &str) -> PubSubNotificationSender {
        let publisher = Publisher::builder(topic.to_string())
            .with_endpoint(endpoint())
            .with_credentials(AnonymousCredentials::new().build())
            .build()
            .await
            .expect("building the publisher");

        PubSubNotificationSender {
            publisher,
            outcomes: OutcomeGate::new(64),
        }
    }

    /// Reads one message, returning its decoded event and its attributes.
    async fn read_one(
        subscription: &str,
    ) -> (
        notification::Event,
        std::collections::HashMap<String, String>,
    ) {
        let subscriber = Subscriber::builder()
            .with_endpoint(endpoint())
            .with_credentials(AnonymousCredentials::new().build())
            .build()
            .await
            .expect("building the subscriber");

        let mut stream = subscriber.subscribe(subscription.to_string()).build();
        let (message, handler) = tokio::time::timeout(Duration::from_secs(30), stream.next())
            .await
            .expect("a message within 30s")
            .expect("the stream yielded nothing")
            .expect("the stream failed");
        handler.ack();

        let event = notification::Event::decode(message.data.as_ref())
            .expect("the payload decodes as a lore.notification.Event");
        (event, message.attributes.clone())
    }

    /// An admin client against the emulator. The production constructor builds its
    /// own, which reaches for Application Default Credentials -- fine on a server,
    /// absent in CI -- so the check is driven directly here with anonymous ones.
    async fn emulator_topic_admin() -> TopicAdmin {
        TopicAdmin::builder()
            .with_endpoint(endpoint())
            .with_credentials(AnonymousCredentials::new().build())
            .build()
            .await
            .expect("building the topic admin client")
    }

    /// A missing topic is reported, not refused -- see `report_topic_reachability`
    /// for why refusing would reject the publisher-only identity this is meant to
    /// run as. The log line is the whole deliverable, so the test asserts the call
    /// completes against an absent topic rather than that it fails.
    #[tokio::test]
    #[tracing_test::traced_test]
    async fn a_missing_topic_is_reported_and_not_refused() {
        let absent = format!(
            "projects/{PROJECT}/topics/absent-{}",
            uuid::Uuid::new_v4().simple()
        );

        report_topic_reachability(&emulator_topic_admin().await, &absent).await;

        assert!(
            logs_contain("Pub/Sub topic does not exist"),
            "a missing topic must leave something in the log to find it by"
        );
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn an_existing_topic_is_not_reported_as_missing() {
        let (topic, _subscription) = topic_and_subscription().await;

        report_topic_reachability(&emulator_topic_admin().await, &topic).await;

        assert!(
            !logs_contain("does not exist"),
            "a topic that exists must not be reported missing"
        );
    }

    #[tokio::test]
    async fn a_pushed_branch_arrives_decodable_with_its_fields_intact() {
        let (topic, subscription) = topic_and_subscription().await;
        let sender = sender_for(&topic).await;

        let repository = RepositoryId::from(&[0x11u8; 16][..]);
        let branch = BranchId::from(&[0x22u8; 16][..]);
        let revision = Hash::from([0x33u8; 32]);

        sender
            .branch_pushed(repository, branch, "someone@example.com", revision, 469)
            .await;

        let (event, attributes) = read_one(&subscription).await;

        assert_eq!(
            event.repository.as_ref(),
            Context::from(repository).data(),
            "the envelope names the repository"
        );
        assert!(!event.id.is_empty(), "every event carries an id");
        assert!(event.time.is_some(), "every event carries a time");

        let Some(notification::event::Event::BranchPushed(pushed)) = event.event else {
            panic!("expected a BranchPushed payload, got {:?}", event.event);
        };
        assert_eq!(pushed.user_id, "someone@example.com");
        assert_eq!(pushed.revision_number, 469);
        assert_eq!(pushed.revision.as_ref(), revision.as_ref());
        assert_eq!(pushed.branch.as_ref(), branch.data());

        assert_eq!(
            attributes.get("event_type").map(String::as_str),
            Some("branch_pushed")
        );
        assert_eq!(
            attributes.get("repository").map(String::as_str),
            Some(hex::encode(Context::from(repository).data()).as_str())
        );
        assert_eq!(
            attributes.get("branch").map(String::as_str),
            Some(hex::encode(branch.data()).as_str()),
            "a branch event is filterable by branch"
        );
    }

    /// A lock event's branch is per resource, so the event carries no `branch`
    /// attribute. A subscription filtering on `branch` must not match one.
    #[tokio::test]
    async fn a_lock_event_arrives_with_its_resources_and_no_branch_attribute() {
        let (topic, subscription) = topic_and_subscription().await;
        let sender = sender_for(&topic).await;

        let repository = RepositoryId::from(&[0x44u8; 16][..]);
        let branch = BranchId::from(&[0x55u8; 16][..]);
        let resource = LockResource {
            branch: (*branch.data()).into(),
            hash: [0x66u8; 32].into(),
            description: "Content/Foo.uasset".to_string(),
        };

        sender
            .resource_locked(repository, branch, "someone@example.com", &[resource])
            .await;

        let (event, attributes) = read_one(&subscription).await;

        let Some(notification::event::Event::ResourceLocked(locked)) = event.event else {
            panic!("expected a ResourceLocked payload, got {:?}", event.event);
        };
        assert_eq!(locked.user_id, "someone@example.com");
        assert_eq!(locked.resources.len(), 1);
        assert_eq!(locked.resources[0].description, "Content/Foo.uasset");

        assert_eq!(
            attributes.get("event_type").map(String::as_str),
            Some("resource_locked")
        );
        assert!(
            !attributes.contains_key("branch"),
            "a lock event names no single branch, so it must not claim one"
        );
    }
}
