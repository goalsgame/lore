// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Pub/Sub-backed notification sender and receiver.
//!
//! Publishes the same `lore.notification.Event` protobuf the in-process sender
//! broadcasts, so a subscriber decodes one schema whichever backend produced it.
//! The receiver reads those events back, so each server instance can deliver the
//! events every other instance publishes.

pub mod pubsub;
pub mod receiver;

pub use pubsub::PubSubNotificationSender;
pub use pubsub::PubSubSettings;
pub use receiver::PubSubNotificationReceiver;
pub use receiver::PubSubReceiverSettings;
