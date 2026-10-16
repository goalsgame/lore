// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Pub/Sub-backed notification sender.
//!
//! Publishes the same `lore.notification.Event` protobuf the in-process sender
//! broadcasts, so a subscriber decodes one schema whichever backend produced it.

pub mod pubsub;

pub use pubsub::PubSubNotificationSender;
pub use pubsub::PubSubSettings;
