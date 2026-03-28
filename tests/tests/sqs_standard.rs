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

use aws_sdk_sqs::Client;
use aws_sdk_sqs::error::ProvideErrorMetadata;
use aws_sdk_sqs::types::QueueAttributeName;
use runtime::RuntimeServer;
use sdk::SdkSmokeTarget;

#[tokio::test]
async fn sqs_standard_queue_lifecycle_round_trips() {
    let runtime = RuntimeServer::spawn("sdk-sqs-standard").await;
    let target = SdkSmokeTarget::new(
        format!("http://{}", runtime.address()),
        "eu-west-2",
    );
    let config = target.load().await;
    let client = Client::new(&config);

    let created = client
        .create_queue()
        .queue_name("orders")
        .attributes(QueueAttributeName::VisibilityTimeout, "45")
        .send()
        .await
        .expect("queue should be created");
    let queue_url =
        created.queue_url().expect("queue URL should be returned").to_owned();

    let looked_up = client
        .get_queue_url()
        .queue_name("orders")
        .send()
        .await
        .expect("queue URL lookup should succeed");
    assert_eq!(looked_up.queue_url(), Some(queue_url.as_str()));

    let listed = client
        .list_queues()
        .queue_name_prefix("ord")
        .send()
        .await
        .expect("queues should list");
    assert_eq!(listed.queue_urls(), std::slice::from_ref(&queue_url));

    client
        .set_queue_attributes()
        .queue_url(&queue_url)
        .attributes(QueueAttributeName::DelaySeconds, "2")
        .send()
        .await
        .expect("queue attributes should update");
    let attributes = client
        .get_queue_attributes()
        .queue_url(&queue_url)
        .attribute_names(QueueAttributeName::VisibilityTimeout)
        .attribute_names(QueueAttributeName::DelaySeconds)
        .attribute_names(QueueAttributeName::QueueArn)
        .send()
        .await
        .expect("queue attributes should fetch");
    let attributes = attributes.attributes().expect("attributes should exist");
    assert_eq!(
        attributes
            .get(&QueueAttributeName::VisibilityTimeout)
            .map(String::as_str),
        Some("45")
    );
    assert_eq!(
        attributes.get(&QueueAttributeName::DelaySeconds).map(String::as_str),
        Some("2")
    );
    assert_eq!(
        attributes.get(&QueueAttributeName::QueueArn).map(String::as_str),
        Some("arn:aws:sqs:eu-west-2:000000000000:orders")
    );

    client
        .tag_queue()
        .queue_url(&queue_url)
        .tags("env", "dev")
        .tags("team", "platform")
        .send()
        .await
        .expect("queue tags should apply");
    let tags = client
        .list_queue_tags()
        .queue_url(&queue_url)
        .send()
        .await
        .expect("queue tags should list");
    let tags = tags.tags().expect("queue tags should exist");
    assert_eq!(tags.get("env").map(String::as_str), Some("dev"));
    assert_eq!(tags.get("team").map(String::as_str), Some("platform"));
    client
        .untag_queue()
        .queue_url(&queue_url)
        .tag_keys("env")
        .send()
        .await
        .expect("queue tag should be removed");

    client
        .send_message()
        .queue_url(&queue_url)
        .message_body("payload")
        .send()
        .await
        .expect("message should send");
    let received = client
        .receive_message()
        .queue_url(&queue_url)
        .visibility_timeout(45)
        .wait_time_seconds(0)
        .send()
        .await
        .expect("message should receive");
    let message =
        received.messages().first().expect("received message should exist");
    let receipt_handle = message
        .receipt_handle()
        .expect("receipt handle should be returned")
        .to_owned();
    assert_eq!(message.body(), Some("payload"));
    assert!(message.message_id().is_some());

    client
        .change_message_visibility()
        .queue_url(&queue_url)
        .receipt_handle(&receipt_handle)
        .visibility_timeout(30)
        .send()
        .await
        .expect("visibility timeout should update");
    let hidden = client
        .receive_message()
        .queue_url(&queue_url)
        .wait_time_seconds(0)
        .send()
        .await
        .expect("hidden receive should succeed");
    assert!(hidden.messages().is_empty());

    client
        .delete_message()
        .queue_url(&queue_url)
        .receipt_handle(&receipt_handle)
        .send()
        .await
        .expect("message should delete");
    let empty = client
        .receive_message()
        .queue_url(&queue_url)
        .wait_time_seconds(0)
        .send()
        .await
        .expect("queue should be empty after delete");
    assert!(empty.messages().is_empty());

    client
        .send_message()
        .queue_url(&queue_url)
        .message_body("purge-me")
        .send()
        .await
        .expect("purge message should send");
    client
        .purge_queue()
        .queue_url(&queue_url)
        .send()
        .await
        .expect("purge should succeed");
    let purged = client
        .receive_message()
        .queue_url(&queue_url)
        .wait_time_seconds(0)
        .send()
        .await
        .expect("purged queue should be empty");
    assert!(purged.messages().is_empty());

    assert!(runtime.state_directory().exists());
    runtime.shutdown().await;
}

#[tokio::test]
async fn sqs_standard_invalid_receipt_handle_surfaces_explicit_error() {
    let runtime = RuntimeServer::spawn("sdk-sqs-standard-invalid").await;
    let target = SdkSmokeTarget::new(
        format!("http://{}", runtime.address()),
        "eu-west-2",
    );
    let config = target.load().await;
    let client = Client::new(&config);

    let created = client
        .create_queue()
        .queue_name("orders")
        .send()
        .await
        .expect("queue should be created");
    let queue_url =
        created.queue_url().expect("queue URL should be returned").to_owned();

    let error = client
        .delete_message()
        .queue_url(&queue_url)
        .receipt_handle("garbage")
        .send()
        .await
        .expect_err("invalid receipt handle should fail");

    assert_eq!(error.code(), Some("ReceiptHandleIsInvalid"));
    assert!(
        error.message().is_some_and(|message| message.contains("garbage"))
    );
    assert!(runtime.state_directory().exists());
    runtime.shutdown().await;
}
