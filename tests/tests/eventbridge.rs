#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    clippy::unreachable,
    clippy::indexing_slicing,
    clippy::assertions_on_constants,
    clippy::missing_panics_doc,
    clippy::missing_errors_doc
)]
#[path = "common/runtime.rs"]
mod runtime;
#[path = "common/sdk.rs"]
mod sdk;

use aws_sdk_eventbridge::Client as EventBridgeClient;
use aws_sdk_eventbridge::types::{PutEventsRequestEntry, Target};
use aws_sdk_sqs::Client as SqsClient;
use runtime::RuntimeServer;
use sdk::SdkSmokeTarget;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

static NEXT_ID: AtomicUsize = AtomicUsize::new(0);

fn unique_name(prefix: &str) -> String {
    let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}-{id}")
}

#[tokio::test]
async fn eventbridge_bus_rule_target_and_delivery_round_trip() {
    let runtime = RuntimeServer::spawn("sdk-eventbridge-roundtrip").await;
    let target = SdkSmokeTarget::new(
        format!("http://{}", runtime.address()),
        "eu-west-2",
    );
    let config = target.load().await;
    let eventbridge = EventBridgeClient::new(&config);
    let sqs = SqsClient::new(&config);

    let bus_name = unique_name("orders-bus");
    let queue_name = unique_name("orders-queue");
    let queue_arn = format!("arn:aws:sqs:eu-west-2:000000000000:{queue_name}");
    let queue_url = sqs
        .create_queue()
        .queue_name(&queue_name)
        .send()
        .await
        .expect("queue should create")
        .queue_url()
        .expect("queue url should exist")
        .to_owned();

    let create_bus = eventbridge
        .create_event_bus()
        .name(&bus_name)
        .send()
        .await
        .expect("event bus should create");
    assert_eq!(
        create_bus.event_bus_arn(),
        Some(
            format!(
                "arn:aws:events:eu-west-2:000000000000:event-bus/{bus_name}"
            )
            .as_str()
        )
    );

    let put_rule = eventbridge
        .put_rule()
        .event_bus_name(&bus_name)
        .name("orders-created")
        .event_pattern(
            r#"{"source":["orders"],"detail-type":["Created"],"detail":{"kind":["queued"]}}"#,
        )
        .send()
        .await
        .expect("rule should create");
    assert_eq!(
        put_rule.rule_arn(),
        Some(
            format!(
                "arn:aws:events:eu-west-2:000000000000:rule/{bus_name}/orders-created"
            )
            .as_str()
        )
    );

    let put_targets = eventbridge
        .put_targets()
        .event_bus_name(&bus_name)
        .rule("orders-created")
        .targets(
            Target::builder()
                .id("queue")
                .arn(&queue_arn)
                .build()
                .expect("target should build"),
        )
        .send()
        .await
        .expect("target should attach");
    assert_eq!(put_targets.failed_entry_count(), 0);

    let described = eventbridge
        .describe_rule()
        .event_bus_name(&bus_name)
        .name("orders-created")
        .send()
        .await
        .expect("rule should describe");
    assert_eq!(described.event_bus_name(), Some(bus_name.as_str()));

    let listed_targets = eventbridge
        .list_targets_by_rule()
        .event_bus_name(&bus_name)
        .rule("orders-created")
        .send()
        .await
        .expect("targets should list");
    assert_eq!(listed_targets.targets().len(), 1);

    let put_events = eventbridge
        .put_events()
        .entries(
            PutEventsRequestEntry::builder()
                .source("orders")
                .detail_type("Created")
                .detail(r#"{"kind":"queued"}"#)
                .event_bus_name(&bus_name)
                .build(),
        )
        .send()
        .await
        .expect("events should publish");
    assert_eq!(put_events.failed_entry_count(), 0);
    assert_eq!(put_events.entries().len(), 1);
    assert!(put_events.entries()[0].event_id().is_some());

    let mut body = None;
    for _ in 0..20 {
        let received = sqs
            .receive_message()
            .queue_url(&queue_url)
            .send()
            .await
            .expect("receive should succeed");
        if let Some(message) = received.messages().first() {
            body = message.body().map(str::to_owned);
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let body = body.expect("event should reach SQS");
    let payload: serde_json::Value =
        serde_json::from_str(&body).expect("SQS body should be JSON");
    assert_eq!(payload["source"], "orders");
    assert_eq!(payload["detail-type"], "Created");
    assert_eq!(payload["detail"]["kind"], "queued");

    eventbridge
        .remove_targets()
        .event_bus_name(&bus_name)
        .rule("orders-created")
        .ids("queue")
        .send()
        .await
        .expect("targets should remove");
    eventbridge
        .delete_rule()
        .event_bus_name(&bus_name)
        .name("orders-created")
        .send()
        .await
        .expect("rule should delete");
    eventbridge
        .delete_event_bus()
        .name(&bus_name)
        .send()
        .await
        .expect("event bus should delete");
    runtime.shutdown().await;
}

#[tokio::test]
async fn eventbridge_reports_explicit_batch_failures() {
    let runtime = RuntimeServer::spawn("sdk-eventbridge-failures").await;
    let target = SdkSmokeTarget::new(
        format!("http://{}", runtime.address()),
        "eu-west-2",
    );
    let config = target.load().await;
    let eventbridge = EventBridgeClient::new(&config);

    let output = eventbridge
        .put_events()
        .entries(
            PutEventsRequestEntry::builder()
                .source("orders")
                .detail_type("Created")
                .detail(r#"{"kind":"queued"}"#)
                .event_bus_name("missing-bus")
                .build(),
        )
        .entries(
            PutEventsRequestEntry::builder()
                .source("orders")
                .detail_type("Created")
                .detail(r#"{"kind":"queued"}"#)
                .build(),
        )
        .send()
        .await
        .expect("mixed batch should return partial results");

    assert_eq!(output.failed_entry_count(), 1);
    assert_eq!(output.entries().len(), 2);
    assert!(output.entries()[0].event_id().is_none());
    assert_eq!(
        output.entries()[0].error_code(),
        Some("ResourceNotFoundException")
    );
    assert!(output.entries()[1].event_id().is_some());

    runtime.shutdown().await;
}
