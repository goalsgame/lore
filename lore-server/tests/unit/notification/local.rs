// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

use bytes::Bytes;
use lore_base::types::Context;
use lore_proto::lore::notification;
use lore_revision::lore::RepositoryId;
use lore_server::notification::local::NotificationSender;

fn event(id: &str, repository: Bytes) -> notification::Event {
    notification::Event {
        id: id.to_string(),
        time: None,
        repository,
        event: Some(notification::event::Event::BranchCreated(
            notification::BranchCreated {
                branch: Bytes::from_static(&[0x22; 16]),
            },
        )),
    }
}

#[tokio::test]
async fn a_delivered_event_reaches_its_repositorys_subscribers_unchanged() {
    let sender = NotificationSender::default();
    let repository = RepositoryId::from(Context::from([0x11u8; 16]));
    let mut subscribed = sender.register(repository);

    let delivered = event(
        "from-elsewhere",
        Bytes::from_owner(Context::from(repository)),
    );
    sender.deliver(delivered.clone());

    assert_eq!(subscribed.try_recv().expect("an event"), delivered);
}

#[tokio::test]
async fn a_delivered_event_reaches_no_other_repository() {
    let sender = NotificationSender::default();
    let mut subscribed = sender.register(RepositoryId::from(Context::from([0x11u8; 16])));

    sender.deliver(event("other", Bytes::from_static(&[0x33; 16])));

    assert!(subscribed.try_recv().is_err());
}

#[tokio::test]
async fn an_event_without_a_repository_id_is_dropped() {
    let sender = NotificationSender::default();
    let mut subscribed = sender.register(RepositoryId::from(Context::from([0x11u8; 16])));

    sender.deliver(event("short", Bytes::from_static(&[0x11; 4])));

    assert!(subscribed.try_recv().is_err());
}
