// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
//! GCP store plugin factories.
//!
//! This module provides plugin factories for GCP-backed stores, mirroring `plugins/aws.rs`:
//! - [`GcpImmutableStorePluginFactory`] - Creates GCS/Firestore-backed immutable stores
//! - [`GcpMutableStorePluginFactory`] - Creates Firestore-backed mutable stores
//!
//! There is no `GcpLockStorePluginFactory`: a Firestore-backed lock store is explicitly out of
//! scope for the `lore-gcp` crate today. A GCP deployment configures `lock_store.mode = "local"`
//! instead.
//!
//! # Configuration shape
//!
//! Firestore project/database are naturally shared between the immutable and mutable stores of
//! one deployment (both stores in one Firestore database), so they are expected at the shared
//! `[plugins.gcp]` level and inherited by both nested store configs via
//! [`lore_server::store::configuration::resolve_plugin_config`]'s parent-merge behavior — the
//! same mechanism `[plugins.aws.http]` uses for settings shared between the AWS stores:
//!
//! ```toml
//! [plugins.gcp]
//! firestore_project = "my-gcp-project"
//!
//! [plugins.gcp.immutable_store]
//! gcs_bucket = "my-lore-fragments"
//!
//! [plugins.gcp.mutable_store]
//! # firestore_project inherited from [plugins.gcp] above
//! ```

use std::sync::Arc;

use lore_base::error::PluginConfigError;
use lore_base::error::PluginInitError;
use lore_base::runtime::runtime;
use lore_gcp::clients;
use lore_gcp::notification::PubSubNotificationSender;
use lore_gcp::notification::PubSubSettings;
use lore_gcp::store::immutable_store::FirestoreImmutableStoreSettings;
use lore_gcp::store::immutable_store::GcpImmutableStore;
use lore_gcp::store::immutable_store::GcpImmutableStoreSettings;
use lore_gcp::store::immutable_store::GcsStoreSettings;
use lore_gcp::store::mutable_store::FirestoreMutableStore;
use lore_gcp::store::mutable_store::FirestoreMutableStoreSettings;
use lore_storage::ImmutableStore;
use lore_storage::MutableStore;
use serde::Deserialize;
use tracing::info;

use crate::plugins::ImmutableStorePluginFactory;
use crate::plugins::MutableStorePluginFactory;
use crate::plugins::NotificationPlugin;
use crate::plugins::NotificationPluginContext;
use crate::plugins::NotificationPluginFactory;
use crate::plugins::PluginError;
use crate::plugins::PluginRegistry;

const PLUGIN_NAME: &str = "gcp";

// =============================================================================
// Configuration Structs
// =============================================================================

/// Configuration for the GCP immutable store plugin.
#[derive(Debug, Clone, Deserialize)]
pub struct GcpImmutableStorePluginConfig {
    /// GCS bucket name for storing fragment payloads.
    pub gcs_bucket: String,

    /// Optional GCP project, kept for config-shape parity and for a future use such as
    /// Requester-Pays billing (`with_quota_project`). Unused today: the GCS resource-name
    /// convention (`projects/_/buckets/{bucket}`) does not require a project id, and
    /// Application Default Credentials already resolve which project's quota is charged.
    #[serde(default)]
    pub gcs_project: Option<String>,

    /// Optional GCS API endpoint override, for a GCS-compatible test double.
    #[serde(default)]
    pub gcs_endpoint_url: Option<String>,

    /// GCP project holding the Firestore database.
    pub firestore_project: String,

    /// Firestore database id. Firestore-native mode supports named databases; `None` uses the
    /// default database, `"(default)"`.
    #[serde(default)]
    pub firestore_database: Option<String>,

    /// Firestore collection name for fragment lifecycle state.
    #[serde(default = "default_fragment_state_collection")]
    pub firestore_fragment_state_collection: String,

    /// Firestore collection name for `(hash, partition, context)` reference associations.
    #[serde(default = "default_fragment_associations_collection")]
    pub firestore_fragment_associations_collection: String,

    /// Slow operation threshold in milliseconds for GCS operations.
    #[serde(default = "default_slow_threshold")]
    pub gcs_slow_operation_threshold_millis: u64,

    /// Slow operation threshold in milliseconds for Firestore operations.
    #[serde(default = "default_firestore_slow_threshold")]
    pub firestore_slow_operation_threshold_millis: u64,

    /// Timeout in milliseconds for GCS operations.
    #[serde(default = "default_timeout")]
    pub timeout_millis: u64,

    /// Timeout in milliseconds for Firestore operations, including the full
    /// `fragment_state.advance_transaction` read-modify-write round trip. Separate from
    /// `timeout_millis`: that read-modify-write can legitimately take longer than a single GCS
    /// call under `firestore_write_concurrency_limit`-bounded contention, and `bounded()` drops
    /// the transaction future outright on expiry rather than rolling it back (`firestore`'s own
    /// "Transaction was neither committed nor rolled back" warning), so an over-tight timeout
    /// here compounds contention instead of just reporting it.
    #[serde(default = "default_firestore_timeout")]
    pub firestore_timeout_millis: u64,

    /// Force write mode (bypasses the existence probe before uploading a payload).
    #[serde(default)]
    pub force_write: bool,

    /// Caps how many `fragment_state` writes run at once (see
    /// `FirestoreImmutableStoreSettings::write_concurrency_limit`).
    #[serde(default = "default_firestore_write_concurrency_limit")]
    pub firestore_write_concurrency_limit: usize,

    /// Caps how many Firestore calls outside that write path run at once (see
    /// `FirestoreImmutableStoreSettings::read_concurrency_limit`).
    #[serde(default = "default_firestore_read_concurrency_limit")]
    pub firestore_read_concurrency_limit: usize,
}

/// Configuration for the GCP mutable store plugin.
#[derive(Debug, Clone, Deserialize)]
pub struct GcpMutableStorePluginConfig {
    /// GCP project holding the Firestore database.
    pub firestore_project: String,

    /// Firestore database id. `None` uses the default database, `"(default)"`.
    #[serde(default)]
    pub firestore_database: Option<String>,

    /// Firestore collection name for mutable store entries.
    #[serde(default = "default_mutable_store_collection")]
    pub firestore_mutable_store_collection: String,

    /// Slow operation threshold in milliseconds for Firestore operations.
    #[serde(default = "default_firestore_slow_threshold")]
    pub slow_operation_threshold_millis: u64,

    /// Timeout in milliseconds for Firestore operations.
    #[serde(default = "default_firestore_timeout")]
    pub timeout_millis: u64,

    /// Force write mode. Kept for config-shape parity with the immutable store's plugin config
    /// and with `lore-aws`'s `AwsMutableStorePluginConfig`; unused by
    /// [`FirestoreMutableStore`] for the same reason it is unused there.
    #[serde(default)]
    pub force_write: bool,
}

fn default_fragment_state_collection() -> String {
    "fragment_state".to_string()
}

fn default_fragment_associations_collection() -> String {
    "fragment_associations".to_string()
}

fn default_mutable_store_collection() -> String {
    "mutable_store".to_string()
}

fn default_slow_threshold() -> u64 {
    u64::MAX
}

/// Unlike GCS's `default_slow_threshold` (off by default), a bounded value: without this,
/// `firestore_timeout_millis` firing was the first signal a Firestore transaction was
/// contended at all, with no warning while it was still merely slow.
fn default_firestore_slow_threshold() -> u64 {
    2_000
}

fn default_timeout() -> u64 {
    5000
}

/// See `GcpImmutableStorePluginConfig::firestore_timeout_millis`'s doc comment.
fn default_firestore_timeout() -> u64 {
    30_000
}

fn default_firestore_write_concurrency_limit() -> usize {
    32
}

/// Read concurrency is bounded for a different reason than write concurrency: not Firestore
/// document contention but the single HTTP/2 connection every call shares, whose server-advertised
/// stream budget is 100. Past it the surplus calls queue inside the transport, and Firestore
/// eventually sheds the connection with a `GOAWAY` instead. 64 leaves room for the 32 concurrent
/// writes `default_firestore_write_concurrency_limit` allows alongside them.
fn default_firestore_read_concurrency_limit() -> usize {
    64
}

// =============================================================================
// Plugin Factory Implementations
// =============================================================================

/// Plugin factory for creating GCP immutable stores.
///
/// This factory creates [`GcpImmutableStore`] instances backed by GCS (for payloads) and
/// Firestore (for fragment lifecycle state and reference associations).
pub struct GcpImmutableStorePluginFactory;

impl ImmutableStorePluginFactory for GcpImmutableStorePluginFactory {
    fn name(&self) -> &'static str {
        PLUGIN_NAME
    }

    fn validate_config(&self, config: &toml::Value) -> Result<(), PluginError> {
        let plugin_name = self.name();

        let _plugin_config: GcpImmutableStorePluginConfig =
            config.clone().try_into().map_err(|e| {
                PluginError::from(PluginConfigError {
                    plugin_name: plugin_name.to_string(),
                    message: format!("Failed to deserialize GCP immutable store config: {e}"),
                })
            })?;

        Ok(())
    }

    fn create(&self, config: &toml::Value) -> Result<Arc<dyn ImmutableStore>, PluginError> {
        let plugin_name = self.name();

        let plugin_config: GcpImmutableStorePluginConfig =
            config.clone().try_into().map_err(|e| {
                PluginError::from(PluginConfigError {
                    plugin_name: plugin_name.to_string(),
                    message: format!("Failed to deserialize GCP immutable store config: {e}"),
                })
            })?;

        info!(
            plugin_name = plugin_name,
            gcs_bucket = %plugin_config.gcs_bucket,
            firestore_project = %plugin_config.firestore_project,
            "Creating GCP immutable store: {plugin_config:?}"
        );

        // Plugin construction is a synchronous trait method. It runs once at startup, one plugin
        // at a time, so at most one core is handed off at a time — see `plugins/aws.rs` for the
        // same pattern with the same reasoning.
        #[allow(clippy::disallowed_methods)]
        let (storage, control, db) = tokio::task::block_in_place(|| {
            runtime().block_on(async {
                // Production has one GCS endpoint at most (or none, meaning the real service):
                // the data-plane/control-plane split only matters for a test double whose two
                // surfaces listen on different ports, so both clients take the same override here.
                let (storage, control) = clients::build_storage_clients(
                    plugin_config.gcs_endpoint_url.as_deref(),
                    plugin_config.gcs_endpoint_url.as_deref(),
                )
                .await
                .map_err(|e| {
                    PluginError::from(PluginInitError {
                        plugin_name: plugin_name.to_string(),
                        message: format!("Failed to create GCS clients: {e}"),
                    })
                })?;

                clients::ensure_bucket_exists(&control, &plugin_config.gcs_bucket)
                    .await
                    .map_err(|e| {
                        PluginError::from(PluginInitError {
                            plugin_name: plugin_name.to_string(),
                            message: format!(
                                "GCS bucket {:?} is not reachable: {e}",
                                plugin_config.gcs_bucket
                            ),
                        })
                    })?;

                let db = clients::build_firestore_db(
                    &plugin_config.firestore_project,
                    plugin_config.firestore_database.as_deref(),
                )
                .await
                .map_err(|e| {
                    PluginError::from(PluginInitError {
                        plugin_name: plugin_name.to_string(),
                        message: format!("Failed to create Firestore client: {e}"),
                    })
                })?;

                Ok::<_, PluginError>((storage, control, db))
            })
        })?;

        let gcs_settings = GcsStoreSettings {
            bucket: plugin_config.gcs_bucket,
            endpoint: plugin_config.gcs_endpoint_url,
            slow_operation_threshold_millis: plugin_config.gcs_slow_operation_threshold_millis,
            timeout_millis: plugin_config.timeout_millis,
        };

        let firestore_settings = FirestoreImmutableStoreSettings {
            firestore_project: plugin_config.firestore_project,
            firestore_database: plugin_config.firestore_database,
            fragment_state_collection: plugin_config.firestore_fragment_state_collection,
            fragment_associations_collection: plugin_config
                .firestore_fragment_associations_collection,
            slow_operation_threshold_millis: plugin_config
                .firestore_slow_operation_threshold_millis,
            timeout_millis: plugin_config.firestore_timeout_millis,
            write_concurrency_limit: plugin_config.firestore_write_concurrency_limit,
            read_concurrency_limit: plugin_config.firestore_read_concurrency_limit,
        };

        let store_settings = GcpImmutableStoreSettings::new(
            gcs_settings,
            firestore_settings,
            plugin_config.force_write,
        );

        let store = GcpImmutableStore::new(storage, control, db, &store_settings);

        Ok(Arc::new(store))
    }
}

/// Plugin factory for creating GCP mutable stores.
///
/// This factory creates [`FirestoreMutableStore`] instances backed by Firestore.
pub struct GcpMutableStorePluginFactory;

impl MutableStorePluginFactory for GcpMutableStorePluginFactory {
    fn name(&self) -> &'static str {
        PLUGIN_NAME
    }

    fn validate_config(&self, config: &toml::Value) -> Result<(), PluginError> {
        let plugin_name = self.name();

        let _plugin_config: GcpMutableStorePluginConfig =
            config.clone().try_into().map_err(|e| {
                PluginError::from(PluginConfigError {
                    plugin_name: plugin_name.to_string(),
                    message: format!("Failed to deserialize GCP mutable store config: {e}"),
                })
            })?;

        Ok(())
    }

    fn create(
        &self,
        config: &toml::Value,
        immutable_store: Arc<dyn ImmutableStore>,
    ) -> Result<Arc<dyn MutableStore>, PluginError> {
        let plugin_name = self.name();

        let plugin_config: GcpMutableStorePluginConfig =
            config.clone().try_into().map_err(|e| {
                PluginError::from(PluginConfigError {
                    plugin_name: plugin_name.to_string(),
                    message: format!("Failed to deserialize GCP mutable store config: {e}"),
                })
            })?;

        info!(
            plugin_name = plugin_name,
            firestore_project = %plugin_config.firestore_project,
            firestore_collection = %plugin_config.firestore_mutable_store_collection,
            "Creating GCP mutable store: {plugin_config:?}"
        );

        #[allow(clippy::disallowed_methods)]
        let db = tokio::task::block_in_place(|| {
            runtime().block_on(clients::build_firestore_db(
                &plugin_config.firestore_project,
                plugin_config.firestore_database.as_deref(),
            ))
        })
        .map_err(|e| {
            PluginError::from(PluginInitError {
                plugin_name: plugin_name.to_string(),
                message: format!("Failed to create Firestore client: {e}"),
            })
        })?;

        let settings = FirestoreMutableStoreSettings {
            firestore_project: plugin_config.firestore_project,
            firestore_database: plugin_config.firestore_database,
            firestore_mutable_store_collection: plugin_config.firestore_mutable_store_collection,
            force_write: plugin_config.force_write,
            timeout_millis: plugin_config.timeout_millis,
            slow_operation_threshold_millis: plugin_config.slow_operation_threshold_millis,
        };

        let store = FirestoreMutableStore::new(db, &settings, immutable_store);

        Ok(Arc::new(store))
    }
}

/// Configuration for the GCP notification plugin.
#[derive(Debug, Clone, Deserialize)]
pub struct GcpNotificationPluginConfig {
    /// GCP project holding the Pub/Sub topic.
    pub pubsub_project: String,

    /// Topic id within that project, not the full resource name.
    pub pubsub_topic: String,

    /// Endpoint override, for the Pub/Sub emulator or a regional endpoint.
    #[serde(default)]
    pub pubsub_endpoint: Option<String>,

    /// Caps how many publishes the sender awaits the outcome of at once (see
    /// `PubSubNotificationSender::observe`). Past it a publish still happens; only
    /// the log line for its failure is dropped.
    #[serde(default = "default_pubsub_outcome_concurrency_limit")]
    pub pubsub_outcome_concurrency_limit: usize,
}

fn default_pubsub_outcome_concurrency_limit() -> usize {
    256
}

/// Plugin factory for creating the GCP notification sender.
///
/// Publishes to Pub/Sub. It contributes no receivers, so obliterate propagation
/// between server instances is not covered; see
/// [`PubSubNotificationSender::obliterate`].
pub struct GcpNotificationPluginFactory;

#[async_trait::async_trait]
impl NotificationPluginFactory for GcpNotificationPluginFactory {
    fn name(&self) -> &'static str {
        PLUGIN_NAME
    }

    fn validate_config(&self, config: &toml::Value) -> Result<(), PluginError> {
        let plugin_name = self.name();

        let _plugin_config: GcpNotificationPluginConfig =
            config.clone().try_into().map_err(|e| {
                PluginError::from(PluginConfigError {
                    plugin_name: plugin_name.to_string(),
                    message: format!("Failed to deserialize GCP notification config: {e}"),
                })
            })?;

        Ok(())
    }

    async fn create(
        &self,
        config: &toml::Value,
        _context: &NotificationPluginContext,
    ) -> Result<NotificationPlugin, PluginError> {
        let plugin_name = self.name();

        let plugin_config: GcpNotificationPluginConfig =
            config.clone().try_into().map_err(|e| {
                PluginError::from(PluginConfigError {
                    plugin_name: plugin_name.to_string(),
                    message: format!("Failed to deserialize GCP notification config: {e}"),
                })
            })?;

        let settings = PubSubSettings {
            project: plugin_config.pubsub_project,
            topic: plugin_config.pubsub_topic,
            endpoint: plugin_config.pubsub_endpoint,
            outcome_concurrency_limit: plugin_config.pubsub_outcome_concurrency_limit,
        };

        info!(
            plugin_name = plugin_name,
            topic = %settings.topic_resource_name(),
            "Creating GCP notification sender"
        );

        let sender = PubSubNotificationSender::new(&settings)
            .await
            .map_err(|e| {
                PluginError::from(PluginInitError {
                    plugin_name: plugin_name.to_string(),
                    message: format!("Failed to create Pub/Sub publisher: {e}"),
                })
            })?;

        Ok(NotificationPlugin {
            sender: Arc::new(sender),
            receivers: Vec::new(),
        })
    }
}

// =============================================================================
// Registration
// =============================================================================

/// Registers the GCP plugin factories with the given registry.
///
/// Unlike `plugins/aws.rs`, this registers no resource detector: `lore-telemetry` has no GCP
/// environment detector today, which is a gap this crate does not attempt to close.
pub fn register(registry: &mut PluginRegistry) {
    registry.register_immutable_store_plugin(Box::new(GcpImmutableStorePluginFactory));
    registry.register_mutable_store_plugin(Box::new(GcpMutableStorePluginFactory));
    registry.register_notification_plugin(Box::new(GcpNotificationPluginFactory));
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_immutable_store_factory_name() {
        let factory = GcpImmutableStorePluginFactory;
        assert_eq!(factory.name(), PLUGIN_NAME);
    }

    #[test]
    fn test_mutable_store_factory_name() {
        let factory = GcpMutableStorePluginFactory;
        assert_eq!(factory.name(), PLUGIN_NAME);
    }

    #[tokio::test]
    async fn test_register_adds_all_plugins() {
        let mut registry = PluginRegistry::new();
        register(&mut registry);

        let immutable_plugins = registry.list_immutable_store_plugins();
        assert!(
            immutable_plugins.contains(&PLUGIN_NAME.to_string()),
            "Expected 'gcp' in immutable store plugins, found: {immutable_plugins:?}"
        );

        let mutable_plugins = registry.list_mutable_store_plugins();
        assert!(
            mutable_plugins.contains(&PLUGIN_NAME.to_string()),
            "Expected 'gcp' in mutable store plugins, found: {mutable_plugins:?}"
        );

        let notification_plugins = registry.list_notification_plugins();
        assert!(
            notification_plugins.contains(&PLUGIN_NAME.to_string()),
            "Expected 'gcp' in notification plugins, found: {notification_plugins:?}"
        );

        // No lock store plugin: a Firestore-backed lock store is out of scope.
        let lock_plugins = registry.list_lock_store_plugins();
        assert!(!lock_plugins.contains(&PLUGIN_NAME.to_string()));
    }

    #[test]
    fn test_notification_factory_name() {
        let factory = GcpNotificationPluginFactory;
        assert_eq!(factory.name(), PLUGIN_NAME);
    }

    #[test]
    fn test_notification_config_deserialization_with_all_fields() {
        let config_str = r#"
            pubsub_project = "my-project"
            pubsub_topic = "lore-events"
            pubsub_endpoint = "http://localhost:8085"
            pubsub_outcome_concurrency_limit = 8
        "#;

        let config: toml::Value = toml::from_str(config_str).unwrap();
        let plugin_config: GcpNotificationPluginConfig = config.try_into().unwrap();

        assert_eq!(plugin_config.pubsub_project, "my-project");
        assert_eq!(plugin_config.pubsub_topic, "lore-events");
        assert_eq!(
            plugin_config.pubsub_endpoint,
            Some("http://localhost:8085".to_string())
        );
        assert_eq!(plugin_config.pubsub_outcome_concurrency_limit, 8);
    }

    #[test]
    fn test_notification_config_deserialization_with_defaults() {
        let config_str = r#"
            pubsub_project = "my-project"
            pubsub_topic = "lore-events"
        "#;

        let config: toml::Value = toml::from_str(config_str).unwrap();
        let plugin_config: GcpNotificationPluginConfig = config.try_into().unwrap();

        assert!(plugin_config.pubsub_endpoint.is_none());
        assert_eq!(plugin_config.pubsub_outcome_concurrency_limit, 256);
    }

    /// `configure_notification` (`server.rs`) hands the plugin the whole
    /// `[plugins.gcp]` table rather than resolving a `notification` sub-table the
    /// way the stores resolve theirs. So the Pub/Sub settings have to parse out of
    /// a table that also carries the Firestore settings and both store sub-tables,
    /// and documenting them under `[plugins.gcp.notification]` would mean they were
    /// never read.
    #[test]
    fn test_notification_config_parses_from_the_whole_gcp_plugin_table() {
        let config_str = r#"
            firestore_project = "my-gcp-project"
            firestore_database = "my-db"
            pubsub_project = "my-gcp-project"
            pubsub_topic = "lore-events"

            [immutable_store]
            gcs_bucket = "my-bucket"

            [mutable_store]
            firestore_mutable_store_collection = "mutable_store"
        "#;

        let config: toml::Value = toml::from_str(config_str).unwrap();
        let plugin_config: GcpNotificationPluginConfig = config.clone().try_into().unwrap();

        assert_eq!(plugin_config.pubsub_project, "my-gcp-project");
        assert_eq!(plugin_config.pubsub_topic, "lore-events");

        // And the factory accepts that same table.
        assert!(
            GcpNotificationPluginFactory
                .validate_config(&config)
                .is_ok()
        );
    }

    /// The settings living one level down is exactly the mistake this guards: the
    /// table reaching the plugin would carry no `pubsub_project` at all.
    #[test]
    fn test_notification_config_under_a_sub_table_is_not_found() {
        let config_str = r#"
            firestore_project = "my-gcp-project"

            [notification]
            pubsub_project = "my-gcp-project"
            pubsub_topic = "lore-events"
        "#;

        let config: toml::Value = toml::from_str(config_str).unwrap();

        assert!(
            GcpNotificationPluginFactory
                .validate_config(&config)
                .is_err()
        );
    }

    /// The topic is not defaulted: publishing branch events to the wrong topic is
    /// worse than refusing to start.
    #[tokio::test]
    async fn test_notification_config_requires_a_topic() {
        let factory = GcpNotificationPluginFactory;
        let config: toml::Value = toml::from_str(r#"pubsub_project = "my-project""#).unwrap();

        assert!(factory.validate_config(&config).is_err());
    }

    #[tokio::test]
    async fn test_immutable_store_config_parsing_error() {
        let factory = GcpImmutableStorePluginFactory;

        // Invalid config - missing required fields (gcs_bucket, firestore_project).
        let config = toml::Value::Table(toml::map::Map::new());
        let result = factory.validate_config(&config);

        let err = result.expect_err("should fail");
        let config_err = err
            .as_plugin_config_error()
            .expect("should be PluginConfigError");
        assert_eq!(config_err.plugin_name, PLUGIN_NAME);
        assert!(config_err.message.contains("Failed to deserialize"));
    }

    #[tokio::test]
    async fn test_mutable_store_config_parsing_error() {
        let factory = GcpMutableStorePluginFactory;

        // Invalid config - missing required field (firestore_project).
        let config = toml::Value::Table(toml::map::Map::new());
        let result = factory.validate_config(&config);

        let err = result.expect_err("should fail");
        let config_err = err
            .as_plugin_config_error()
            .expect("should be PluginConfigError");
        assert_eq!(config_err.plugin_name, PLUGIN_NAME);
        assert!(config_err.message.contains("Failed to deserialize"));
    }

    #[test]
    fn test_immutable_config_deserialization_with_all_fields() {
        let config_str = r#"
            gcs_bucket = "test-bucket"
            gcs_project = "my-project"
            gcs_endpoint_url = "http://localhost:9000"
            firestore_project = "my-project"
            firestore_database = "my-db"
            firestore_fragment_state_collection = "state"
            firestore_fragment_associations_collection = "associations"
            gcs_slow_operation_threshold_millis = 1000
            firestore_slow_operation_threshold_millis = 500
            timeout_millis = 3000
            firestore_timeout_millis = 15000
            firestore_write_concurrency_limit = 8
            firestore_read_concurrency_limit = 16
            force_write = true
        "#;

        let config: toml::Value = toml::from_str(config_str).unwrap();
        let plugin_config: GcpImmutableStorePluginConfig = config.try_into().unwrap();

        assert_eq!(plugin_config.gcs_bucket, "test-bucket");
        assert_eq!(plugin_config.gcs_project, Some("my-project".to_string()));
        assert_eq!(
            plugin_config.gcs_endpoint_url,
            Some("http://localhost:9000".to_string())
        );
        assert_eq!(plugin_config.firestore_project, "my-project");
        assert_eq!(plugin_config.firestore_database, Some("my-db".to_string()));
        assert_eq!(plugin_config.firestore_fragment_state_collection, "state");
        assert_eq!(
            plugin_config.firestore_fragment_associations_collection,
            "associations"
        );
        assert_eq!(plugin_config.gcs_slow_operation_threshold_millis, 1000);
        assert_eq!(plugin_config.firestore_slow_operation_threshold_millis, 500);
        assert_eq!(plugin_config.timeout_millis, 3000);
        assert_eq!(plugin_config.firestore_timeout_millis, 15000);
        assert_eq!(plugin_config.firestore_write_concurrency_limit, 8);
        assert_eq!(plugin_config.firestore_read_concurrency_limit, 16);
        assert!(plugin_config.force_write);
    }

    #[test]
    fn test_immutable_config_deserialization_with_defaults() {
        let config_str = r#"
            gcs_bucket = "test-bucket"
            firestore_project = "my-project"
        "#;

        let config: toml::Value = toml::from_str(config_str).unwrap();
        let plugin_config: GcpImmutableStorePluginConfig = config.try_into().unwrap();

        assert_eq!(plugin_config.gcs_bucket, "test-bucket");
        assert!(plugin_config.gcs_project.is_none());
        assert!(plugin_config.gcs_endpoint_url.is_none());
        assert_eq!(plugin_config.firestore_project, "my-project");
        assert!(plugin_config.firestore_database.is_none());
        assert_eq!(
            plugin_config.firestore_fragment_state_collection,
            "fragment_state"
        );
        assert_eq!(
            plugin_config.firestore_fragment_associations_collection,
            "fragment_associations"
        );
        assert_eq!(plugin_config.gcs_slow_operation_threshold_millis, u64::MAX);
        assert_eq!(
            plugin_config.firestore_slow_operation_threshold_millis,
            2000
        );
        assert_eq!(plugin_config.timeout_millis, 5000);
        assert_eq!(plugin_config.firestore_timeout_millis, 30000);
        assert_eq!(plugin_config.firestore_write_concurrency_limit, 32);
        assert_eq!(plugin_config.firestore_read_concurrency_limit, 64);
        assert!(!plugin_config.force_write);
    }

    #[test]
    fn test_mutable_config_deserialization_with_defaults() {
        let config_str = r#"
            firestore_project = "my-project"
        "#;

        let config: toml::Value = toml::from_str(config_str).unwrap();
        let plugin_config: GcpMutableStorePluginConfig = config.try_into().unwrap();

        assert_eq!(plugin_config.firestore_project, "my-project");
        assert!(plugin_config.firestore_database.is_none());
        assert_eq!(
            plugin_config.firestore_mutable_store_collection,
            "mutable_store"
        );
        assert_eq!(plugin_config.slow_operation_threshold_millis, 2000);
        assert_eq!(plugin_config.timeout_millis, 30000);
        assert!(!plugin_config.force_write);
    }

    #[test]
    fn test_immutable_config_requires_gcs_bucket() {
        let config_str = r#"
            firestore_project = "my-project"
        "#;
        let config: toml::Value = toml::from_str(config_str).unwrap();
        config
            .try_into::<GcpImmutableStorePluginConfig>()
            .expect_err("gcs_bucket must be configured explicitly");
    }

    #[test]
    fn test_immutable_config_requires_firestore_project() {
        let config_str = r#"
            gcs_bucket = "test-bucket"
        "#;
        let config: toml::Value = toml::from_str(config_str).unwrap();
        config
            .try_into::<GcpImmutableStorePluginConfig>()
            .expect_err("firestore_project must be configured explicitly");
    }
}
