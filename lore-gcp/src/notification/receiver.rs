// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Receives the events other server instances publish to the notification topic.
//!
//! Each instance reads the topic through a subscription of its own: one it creates at startup,
//! named `{prefix}-{origin}` and given an expiration policy and a short message retention, or one
//! configured by name. Every message is acknowledged on receipt. A message whose `origin`
//! attribute is this instance's own is skipped, since the instance delivered that event locally
//! when it published it. The rest are decoded and handed to a callback, once per event id.
//!
//! At shutdown a subscription this receiver created is deleted. Failing to delete it is logged
//! and otherwise ignored; the expiration policy removes it later.

use std::collections::HashSet;
use std::collections::VecDeque;
use std::future::Future;
use std::time::Duration;

use google_cloud_gax::error::rpc::Code;
use google_cloud_pubsub::client::Subscriber;
use google_cloud_pubsub::client::SubscriptionAdmin;
use google_cloud_pubsub::model::ExpirationPolicy;
use google_cloud_pubsub::model::Message as PubsubMessage;
use lore_proto::lore::notification;
use prost::Message as _;
use tracing::debug;
use tracing::info;
use tracing::warn;

use crate::gcp_error::GcpError;
use crate::notification::pubsub::ORIGIN_ATTRIBUTE;

/// How many recent event ids the receiver remembers to drop redelivered messages.
const RECENT_EVENT_IDS: usize = 4096;

/// How long deleting the subscription at shutdown may take.
const DELETE_TIMEOUT: Duration = Duration::from_secs(5);

/// The first and the longest wait before reopening a failed subscription stream.
const REOPEN_BACKOFF: (Duration, Duration) = (Duration::from_secs(1), Duration::from_secs(60));

/// Configuration for the Pub/Sub notification receiver.
#[derive(Clone, Debug)]
pub struct PubSubReceiverSettings {
    /// GCP project holding the topic and the subscription.
    pub project: String,
    /// Topic id within that project, not the full resource name.
    pub topic: String,
    /// Endpoint override, for the emulator or a regional endpoint.
    pub endpoint: Option<String>,
    /// Subscription id of an existing subscription to read. `None` creates one per instance.
    pub subscription: Option<String>,
    /// Prefix of the subscription id this receiver creates.
    pub subscription_prefix: String,
    /// How long a created subscription may go unused before Pub/Sub deletes it.
    pub subscription_ttl: Duration,
    /// How long a created subscription retains unacknowledged messages.
    pub message_retention: Duration,
}

impl PubSubReceiverSettings {
    fn topic_resource_name(&self) -> String {
        format!("projects/{}/topics/{}", self.project, self.topic)
    }

    fn subscription_resource_name(&self, id: &str) -> String {
        format!("projects/{}/subscriptions/{id}", self.project)
    }
}

/// Reads one subscription on the notification topic and delivers what other instances publish.
pub struct PubSubNotificationReceiver {
    subscriber: Subscriber,
    admin: SubscriptionAdmin,
    settings: PubSubReceiverSettings,
    subscription: String,
    created: bool,
    origin: String,
}

impl PubSubNotificationReceiver {
    /// Builds the clients and creates this instance's subscription, unless one is configured.
    ///
    /// `origin` is the value the instance's own publisher sets as the `origin` attribute.
    ///
    /// # Errors
    /// [`GcpError::ClientBuild`] when a client cannot be constructed, and [`GcpError::PubSub`]
    /// when the subscription cannot be created.
    pub async fn new(settings: &PubSubReceiverSettings, origin: &str) -> Result<Self, GcpError> {
        crate::clients::ensure_crypto_provider();

        let mut subscriber = Subscriber::builder();
        let mut admin = SubscriptionAdmin::builder();
        if let Some(endpoint) = settings.endpoint.as_ref() {
            subscriber = subscriber.with_endpoint(endpoint.clone());
            admin = admin.with_endpoint(endpoint.clone());
        }
        let subscriber = subscriber.build().await.map_err(GcpError::client_build)?;
        let admin = admin.build().await.map_err(GcpError::client_build)?;

        Self::with_clients(subscriber, admin, settings, origin).await
    }

    async fn with_clients(
        subscriber: Subscriber,
        admin: SubscriptionAdmin,
        settings: &PubSubReceiverSettings,
        origin: &str,
    ) -> Result<Self, GcpError> {
        let (subscription, created) = match settings.subscription.as_deref() {
            Some(id) => (settings.subscription_resource_name(id), false),
            None => {
                let id = format!("{}-{origin}", settings.subscription_prefix);
                (settings.subscription_resource_name(&id), true)
            }
        };

        let receiver = Self {
            subscriber,
            admin,
            settings: settings.clone(),
            subscription,
            created,
            origin: origin.to_string(),
        };
        if created {
            receiver.create_subscription().await?;
        }
        Ok(receiver)
    }

    /// The full resource name of the subscription this receiver reads.
    pub fn subscription(&self) -> &str {
        &self.subscription
    }

    async fn create_subscription(&self) -> Result<(), GcpError> {
        let ttl = google_cloud_wkt::Duration::try_from(self.settings.subscription_ttl)
            .map_err(|e| GcpError::Config(format!("subscription TTL: {e}")))?;
        let retention = google_cloud_wkt::Duration::try_from(self.settings.message_retention)
            .map_err(|e| GcpError::Config(format!("message retention: {e}")))?;

        let created = self
            .admin
            .create_subscription()
            .set_name(&self.subscription)
            .set_topic(self.settings.topic_resource_name())
            .set_expiration_policy(ExpirationPolicy::new().set_ttl(ttl))
            .set_message_retention_duration(retention)
            .send()
            .await;

        match created {
            Ok(_) => {
                info!(subscription = %self.subscription, "Created Pub/Sub notification subscription");
                Ok(())
            }
            Err(error) if error.status().map(|status| status.code) == Some(Code::AlreadyExists) => {
                debug!(subscription = %self.subscription, "Pub/Sub notification subscription exists");
                Ok(())
            }
            Err(error) => Err(GcpError::pubsub(error)),
        }
    }

    async fn delete_subscription(&self) {
        let deleted = tokio::time::timeout(
            DELETE_TIMEOUT,
            self.admin
                .delete_subscription()
                .set_subscription(&self.subscription)
                .send(),
        )
        .await;

        match deleted {
            Ok(Ok(())) => {
                info!(subscription = %self.subscription, "Deleted Pub/Sub notification subscription");
            }
            Ok(Err(error)) => warn!(
                subscription = %self.subscription,
                %error,
                "Could not delete the Pub/Sub notification subscription; its expiration policy \
                 removes it once unused"
            ),
            Err(_) => warn!(
                subscription = %self.subscription,
                timeout = ?DELETE_TIMEOUT,
                "Gave up deleting the Pub/Sub notification subscription; its expiration policy \
                 removes it once unused"
            ),
        }
    }

    /// Delivers events from the subscription to `deliver` until `shutdown` completes, then
    /// deletes the subscription if this receiver created it.
    ///
    /// A failed stream is reopened with backoff; a subscription this receiver created is
    /// recreated if it has disappeared.
    pub async fn run<F>(self, deliver: F, shutdown: impl Future<Output = ()>)
    where
        F: Fn(notification::Event),
    {
        let mut shutdown = std::pin::pin!(shutdown);
        let mut recent = RecentIds::new(RECENT_EVENT_IDS);
        let mut backoff = REOPEN_BACKOFF.0;

        loop {
            let mut stream = self.subscriber.subscribe(&self.subscription).build();
            loop {
                tokio::select! {
                    () = &mut shutdown => {
                        drop(stream);
                        if self.created {
                            self.delete_subscription().await;
                        }
                        return;
                    }
                    next = stream.next() => match next {
                        Some(Ok((message, handler))) => {
                            handler.ack();
                            backoff = REOPEN_BACKOFF.0;
                            if let Some(event) = accept(&self.origin, &message, &mut recent) {
                                deliver(event);
                            }
                        }
                        Some(Err(error)) => {
                            warn!(
                                subscription = %self.subscription,
                                %error,
                                "Pub/Sub notification subscription stream failed"
                            );
                            if self.created
                                && error.status().map(|status| status.code) == Some(Code::NotFound)
                                && let Err(error) = self.create_subscription().await
                            {
                                warn!(%error, "Could not recreate the Pub/Sub notification subscription");
                            }
                            break;
                        }
                        None => break,
                    }
                }
            }

            tokio::select! {
                () = &mut shutdown => {
                    if self.created {
                        self.delete_subscription().await;
                    }
                    return;
                }
                () = tokio::time::sleep(backoff) => {}
            }
            backoff = (backoff * 2).min(REOPEN_BACKOFF.1);
        }
    }
}

/// The event `message` carries, unless the sender with `origin` published it, it does not
/// decode, or an event with its id is in `recent`.
fn accept(
    origin: &str,
    message: &PubsubMessage,
    recent: &mut RecentIds,
) -> Option<notification::Event> {
    if message.attributes.get(ORIGIN_ATTRIBUTE).map(String::as_str) == Some(origin) {
        return None;
    }
    let event = match notification::Event::decode(message.data.as_ref()) {
        Ok(event) => event,
        Err(error) => {
            warn!(%error, "Dropping a Pub/Sub message that is not a lore.notification.Event");
            return None;
        }
    };
    if !event.id.is_empty() && !recent.insert(&event.id) {
        debug!(id = %event.id, "Dropping a redelivered notification event");
        return None;
    }
    Some(event)
}

/// A bounded set of the most recently inserted ids.
struct RecentIds {
    order: VecDeque<String>,
    ids: HashSet<String>,
    capacity: usize,
}

impl RecentIds {
    fn new(capacity: usize) -> Self {
        Self {
            order: VecDeque::with_capacity(capacity),
            ids: HashSet::with_capacity(capacity),
            capacity: capacity.max(1),
        }
    }

    /// Inserts `id`, forgetting the oldest id when full. Answers whether `id` was new.
    fn insert(&mut self, id: &str) -> bool {
        if self.ids.contains(id) {
            return false;
        }
        if self.order.len() == self.capacity
            && let Some(oldest) = self.order.pop_front()
        {
            self.ids.remove(&oldest);
        }
        self.order.push_back(id.to_string());
        self.ids.insert(id.to_string());
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn message(id: &str, origin: Option<&str>) -> PubsubMessage {
        let event = notification::Event {
            id: id.to_string(),
            ..Default::default()
        };
        let mut attributes = vec![("event_type", "branch_pushed".to_string())];
        if let Some(origin) = origin {
            attributes.push((ORIGIN_ATTRIBUTE, origin.to_string()));
        }
        PubsubMessage::new()
            .set_data(event.encode_to_vec())
            .set_attributes(attributes)
    }

    #[test]
    fn an_event_from_another_origin_is_accepted_once() {
        let mut recent = RecentIds::new(8);

        let first = accept("self", &message("e1", Some("other")), &mut recent);
        assert_eq!(first.map(|event| event.id), Some("e1".to_string()));
        assert!(
            accept("self", &message("e1", Some("other")), &mut recent).is_none(),
            "a redelivered event is dropped"
        );
    }

    #[test]
    fn an_event_from_this_origin_is_skipped() {
        let mut recent = RecentIds::new(8);
        assert!(accept("self", &message("e1", Some("self")), &mut recent).is_none());
    }

    #[test]
    fn an_event_without_an_origin_is_accepted() {
        let mut recent = RecentIds::new(8);
        assert!(accept("self", &message("e1", None), &mut recent).is_some());
    }

    #[test]
    fn a_payload_that_does_not_decode_is_dropped() {
        let mut recent = RecentIds::new(8);
        let garbage = PubsubMessage::new().set_data(vec![0xff, 0xff, 0xff]);
        assert!(accept("self", &garbage, &mut recent).is_none());
    }

    #[test]
    fn recent_ids_forget_the_oldest_when_full() {
        let mut recent = RecentIds::new(2);
        assert!(recent.insert("a"));
        assert!(recent.insert("b"));
        assert!(!recent.insert("a"));
        assert!(recent.insert("c"));
        assert!(recent.insert("a"), "the oldest id was forgotten");
        assert!(!recent.insert("c"));
    }
}

/// Tests against the Pub/Sub emulator from `lore-integration-tests/compose.yaml`.
#[cfg(all(test, feature = "integration_tests"))]
mod emulator_tests {
    use std::sync::Arc;

    use google_cloud_auth::credentials::anonymous::Builder as AnonymousCredentials;
    use google_cloud_pubsub::client::Publisher;
    use google_cloud_pubsub::client::TopicAdmin;
    use lore_base::types::Hash;
    use lore_revision::lore::BranchId;
    use lore_revision::lore::RepositoryId;
    use lore_revision::notification::NotificationSender;
    use tokio::sync::mpsc;
    use tokio::sync::oneshot;

    use super::*;
    use crate::notification::PubSubNotificationSender;

    const PROJECT: &str = "lore-gcp-emulator-tests";

    fn endpoint() -> String {
        crate::clients::ensure_crypto_provider();
        let host = std::env::var("PUBSUB_EMULATOR_HOST")
            .expect("PUBSUB_EMULATOR_HOST; is the Pub/Sub emulator running?");
        format!("http://{host}")
    }

    async fn admin() -> SubscriptionAdmin {
        SubscriptionAdmin::builder()
            .with_endpoint(endpoint())
            .with_credentials(AnonymousCredentials::new().build())
            .build()
            .await
            .expect("building the subscription admin client")
    }

    /// A topic named for this test alone.
    async fn topic() -> String {
        let id = format!("events-{}", uuid::Uuid::new_v4().simple());
        TopicAdmin::builder()
            .with_endpoint(endpoint())
            .with_credentials(AnonymousCredentials::new().build())
            .build()
            .await
            .expect("building the topic admin client")
            .create_topic()
            .set_name(format!("projects/{PROJECT}/topics/{id}"))
            .send()
            .await
            .expect("creating the topic");
        id
    }

    fn settings(topic: &str, subscription: Option<String>) -> PubSubReceiverSettings {
        PubSubReceiverSettings {
            project: PROJECT.to_string(),
            topic: topic.to_string(),
            endpoint: Some(endpoint()),
            subscription,
            subscription_prefix: "lore-server".to_string(),
            subscription_ttl: Duration::from_secs(86_400),
            message_retention: Duration::from_secs(600),
        }
    }

    async fn sender(topic: &str) -> PubSubNotificationSender {
        let publisher = Publisher::builder(format!("projects/{PROJECT}/topics/{topic}"))
            .with_endpoint(endpoint())
            .with_credentials(AnonymousCredentials::new().build())
            .build()
            .await
            .expect("building the publisher");
        PubSubNotificationSender::with_publisher(publisher, 64)
    }

    async fn receiver(
        settings: &PubSubReceiverSettings,
        origin: &str,
    ) -> PubSubNotificationReceiver {
        let subscriber = Subscriber::builder()
            .with_endpoint(endpoint())
            .with_credentials(AnonymousCredentials::new().build())
            .build()
            .await
            .expect("building the subscriber");
        PubSubNotificationReceiver::with_clients(subscriber, admin().await, settings, origin)
            .await
            .expect("creating the receiver")
    }

    /// An instance: a sender, and a receiver running until the returned sender is used.
    struct Instance {
        sender: Arc<PubSubNotificationSender>,
        events: mpsc::UnboundedReceiver<notification::Event>,
        stop: oneshot::Sender<()>,
        running: tokio::task::JoinHandle<()>,
        subscription: String,
    }

    async fn instance(topic: &str) -> Instance {
        let sender = Arc::new(sender(topic).await);
        let receiver = receiver(&settings(topic, None), sender.origin()).await;
        let subscription = receiver.subscription().to_string();
        let (deliver, events) = mpsc::unbounded_channel();
        let (stop, stopped) = oneshot::channel::<()>();
        let running = lore_base::lore_spawn!(async move {
            receiver
                .run(
                    move |event| {
                        let _ = deliver.send(event);
                    },
                    async move {
                        let _ = stopped.await;
                    },
                )
                .await;
        });
        Instance {
            sender,
            events,
            stop,
            running,
            subscription,
        }
    }

    async fn subscription_exists(name: &str) -> bool {
        match admin()
            .await
            .get_subscription()
            .set_subscription(name)
            .send()
            .await
        {
            Ok(_) => true,
            Err(error) if error.status().map(|status| status.code) == Some(Code::NotFound) => false,
            Err(error) => panic!("looking up {name}: {error}"),
        }
    }

    #[tokio::test]
    async fn each_instance_receives_the_others_events_and_not_its_own() {
        let topic = topic().await;
        let mut a = instance(&topic).await;
        let mut b = instance(&topic).await;
        assert_ne!(a.subscription, b.subscription);

        let repository = RepositoryId::from(&[0x11u8; 16][..]);
        let branch = BranchId::from(&[0x22u8; 16][..]);
        a.sender
            .branch_pushed(repository, branch, "alice", Hash::from([0x33u8; 32]), 7)
            .await;
        b.sender.branch_created(repository, branch).await;

        let at_b = tokio::time::timeout(Duration::from_secs(30), b.events.recv())
            .await
            .expect("B receives A's event within 30s")
            .expect("B's receiver is running");
        assert!(matches!(
            at_b.event,
            Some(notification::event::Event::BranchPushed(ref pushed)) if pushed.revision_number == 7
        ));

        let at_a = tokio::time::timeout(Duration::from_secs(30), a.events.recv())
            .await
            .expect("A receives B's event within 30s")
            .expect("A's receiver is running");
        assert!(matches!(
            at_a.event,
            Some(notification::event::Event::BranchCreated(_))
        ));

        tokio::time::sleep(Duration::from_secs(2)).await;
        assert!(
            a.events.try_recv().is_err(),
            "A does not receive its own event"
        );
        assert!(
            b.events.try_recv().is_err(),
            "B does not receive its own event"
        );

        for instance in [a, b] {
            let _ = instance.stop.send(());
            instance.running.await.unwrap();
        }
    }

    #[tokio::test]
    async fn a_created_subscription_is_deleted_at_shutdown() {
        let topic = topic().await;
        let instance = instance(&topic).await;
        assert!(subscription_exists(&instance.subscription).await);

        let _ = instance.stop.send(());
        instance.running.await.unwrap();

        assert!(!subscription_exists(&instance.subscription).await);
    }

    #[tokio::test]
    async fn a_configured_subscription_is_read_and_kept() {
        let topic = topic().await;
        let id = format!("configured-{}", uuid::Uuid::new_v4().simple());
        let name = format!("projects/{PROJECT}/subscriptions/{id}");
        admin()
            .await
            .create_subscription()
            .set_name(&name)
            .set_topic(format!("projects/{PROJECT}/topics/{topic}"))
            .send()
            .await
            .expect("creating the configured subscription");

        let receiver = receiver(&settings(&topic, Some(id)), "self").await;
        assert_eq!(receiver.subscription(), name);

        let (deliver, mut events) = mpsc::unbounded_channel();
        let (stop, stopped) = oneshot::channel::<()>();
        let running = lore_base::lore_spawn!(async move {
            receiver
                .run(
                    move |event| {
                        let _ = deliver.send(event);
                    },
                    async move {
                        let _ = stopped.await;
                    },
                )
                .await;
        });

        let other = sender(&topic).await;
        other
            .branch_deleted(
                RepositoryId::from(&[0x44u8; 16][..]),
                BranchId::from(&[0x55u8; 16][..]),
            )
            .await;
        let received = tokio::time::timeout(Duration::from_secs(30), events.recv())
            .await
            .expect("an event within 30s")
            .expect("the receiver is running");
        assert!(matches!(
            received.event,
            Some(notification::event::Event::BranchDeleted(_))
        ));

        let _ = stop.send(());
        running.await.unwrap();
        assert!(
            subscription_exists(&name).await,
            "a configured subscription is not deleted"
        );
    }

    #[tokio::test]
    async fn a_created_subscription_that_disappears_is_recreated() {
        let topic = topic().await;
        let mut instance = instance(&topic).await;

        admin()
            .await
            .delete_subscription()
            .set_subscription(&instance.subscription)
            .send()
            .await
            .expect("deleting the subscription");

        let mut recreated = false;
        for _ in 0..60 {
            if subscription_exists(&instance.subscription).await {
                recreated = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        assert!(recreated, "the receiver recreates its subscription");

        sender(&topic)
            .await
            .branch_created(
                RepositoryId::from(&[0x66u8; 16][..]),
                BranchId::from(&[0x77u8; 16][..]),
            )
            .await;
        let received = tokio::time::timeout(Duration::from_secs(30), instance.events.recv())
            .await
            .expect("an event within 30s")
            .expect("the receiver is running");
        assert!(matches!(
            received.event,
            Some(notification::event::Event::BranchCreated(_))
        ));

        let _ = instance.stop.send(());
        instance.running.await.unwrap();
    }

    #[tokio::test]
    async fn a_created_subscription_expires_and_retains_briefly() {
        let topic = topic().await;
        let instance = instance(&topic).await;

        let subscription = admin()
            .await
            .get_subscription()
            .set_subscription(&instance.subscription)
            .send()
            .await
            .expect("reading the created subscription");
        assert_eq!(
            subscription
                .expiration_policy
                .and_then(|policy| policy.ttl)
                .map(|ttl| ttl.seconds()),
            Some(86_400)
        );
        assert_eq!(
            subscription
                .message_retention_duration
                .map(|retention| retention.seconds()),
            Some(600)
        );

        let _ = instance.stop.send(());
        instance.running.await.unwrap();
    }
}
