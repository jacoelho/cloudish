#![allow(
    clippy::unreachable,
    clippy::assertions_on_constants,
    clippy::missing_panics_doc,
    clippy::missing_errors_doc,
    clippy::expect_used,
    clippy::panic
)]
use tests::common::runtime;
use tests::common::sdk;

use aws_sdk_sqs::Client;
use aws_sdk_sqs::error::ProvideErrorMetadata;
use aws_sdk_sqs::types::{QueueAttributeName, SendMessageBatchRequestEntry};
use runtime::RuntimeServer;
use sdk::SdkSmokeTarget;

#[tokio::test]
async fn sqs_fifo_batch_and_redrive_round_trips() {
    let runtime = RuntimeServer::spawn("sdk-sqs-fifo").await;
    let target = SdkSmokeTarget::new(
        format!("http://{}", runtime.address()),
        "eu-west-2",
    );
    let config = target.load().await;
    let client = Client::new(&config);

    let dlq = client
        .create_queue()
        .queue_name("orders-dlq.fifo")
        .attributes(QueueAttributeName::FifoQueue, "true")
        .attributes(QueueAttributeName::ContentBasedDeduplication, "true")
        .send()
        .await
        .expect("dlq should be created");
    let dlq_url =
        dlq.queue_url().expect("dlq URL should be returned").to_owned();
    let dlq_attributes = client
        .get_queue_attributes()
        .queue_url(&dlq_url)
        .attribute_names(QueueAttributeName::QueueArn)
        .send()
        .await
        .expect("dlq attributes should fetch");
    let dlq_arn = dlq_attributes
        .attributes()
        .and_then(|attributes| attributes.get(&QueueAttributeName::QueueArn))
        .expect("dlq arn should exist")
        .to_owned();

    let source = client
        .create_queue()
        .queue_name("orders-source.fifo")
        .attributes(QueueAttributeName::FifoQueue, "true")
        .attributes(QueueAttributeName::ContentBasedDeduplication, "true")
        .attributes(
            QueueAttributeName::RedrivePolicy,
            format!(
                r#"{{"deadLetterTargetArn":"{dlq_arn}","maxReceiveCount":1}}"#
            ),
        )
        .send()
        .await
        .expect("source queue should be created");
    let source_url =
        source.queue_url().expect("source URL should be returned").to_owned();

    let batch = client
        .send_message_batch()
        .queue_url(&source_url)
        .entries(
            SendMessageBatchRequestEntry::builder()
                .id("first")
                .message_body("payload")
                .message_group_id("group-1")
                .build()
                .expect("valid batch entry should build"),
        )
        .entries(
            SendMessageBatchRequestEntry::builder()
                .id("second")
                .message_body("ignored")
                .build()
                .expect("invalid batch entry shape should still build"),
        )
        .send()
        .await
        .expect("batch send should return per-entry results");
    assert_eq!(batch.successful().len(), 1);
    assert_eq!(batch.failed().len(), 1);
    assert_eq!(batch.failed()[0].code(), "MissingParameter");

    let received = client
        .receive_message()
        .queue_url(&source_url)
        .visibility_timeout(0)
        .wait_time_seconds(0)
        .send()
        .await
        .expect("first receive should succeed");
    assert_eq!(received.messages().len(), 1);
    let emptied = client
        .receive_message()
        .queue_url(&source_url)
        .visibility_timeout(0)
        .wait_time_seconds(0)
        .send()
        .await
        .expect("second receive should move the message to the dlq");
    assert!(emptied.messages().is_empty());

    let sources = client
        .list_dead_letter_source_queues()
        .queue_url(&dlq_url)
        .max_results(1)
        .send()
        .await
        .expect("dlq sources should list");
    assert_eq!(sources.queue_urls(), std::slice::from_ref(&source_url));
    assert!(sources.next_token().is_none());

    let moved = client
        .start_message_move_task()
        .source_arn(&dlq_arn)
        .send()
        .await
        .expect("move task should succeed");
    assert!(moved.task_handle().is_some());
    let tasks = client
        .list_message_move_tasks()
        .source_arn(&dlq_arn)
        .max_results(1)
        .send()
        .await
        .expect("move tasks should list");
    assert_eq!(tasks.results().len(), 1);
    assert_eq!(tasks.results()[0].status(), Some("COMPLETED"));

    let redriven = client
        .receive_message()
        .queue_url(&source_url)
        .visibility_timeout(0)
        .wait_time_seconds(0)
        .send()
        .await
        .expect("redriven message should receive");
    let message =
        redriven.messages().first().expect("redriven message should exist");
    assert_eq!(message.body(), Some("payload"));

    let error = client
        .send_message()
        .queue_url(&source_url)
        .message_body("missing-group")
        .send()
        .await
        .expect_err("fifo sends without a group should fail");
    assert_eq!(error.code(), Some("MissingParameter"));

    assert!(runtime.state_directory().exists());
    runtime.shutdown().await;
}

#[tokio::test]
async fn sqs_fifo_receive_request_attempt_id_reuses_the_first_receive() {
    let runtime = RuntimeServer::spawn("sdk-sqs-fifo-attempt").await;
    let target = SdkSmokeTarget::new(
        format!("http://{}", runtime.address()),
        "eu-west-2",
    );
    let config = target.load().await;
    let client = Client::new(&config);

    let queue_url = client
        .create_queue()
        .queue_name("attempts.fifo")
        .attributes(QueueAttributeName::FifoQueue, "true")
        .attributes(QueueAttributeName::ContentBasedDeduplication, "true")
        .send()
        .await
        .expect("fifo queue should be created")
        .queue_url()
        .expect("queue URL should be returned")
        .to_owned();

    client
        .send_message()
        .queue_url(&queue_url)
        .message_body("payload")
        .message_group_id("group-1")
        .send()
        .await
        .expect("fifo message should send");

    let first = client
        .receive_message()
        .queue_url(&queue_url)
        .receive_request_attempt_id("attempt-1")
        .visibility_timeout(30)
        .wait_time_seconds(0)
        .send()
        .await
        .expect("first receive should succeed");
    let second = client
        .receive_message()
        .queue_url(&queue_url)
        .receive_request_attempt_id("attempt-1")
        .visibility_timeout(30)
        .wait_time_seconds(0)
        .send()
        .await
        .expect("repeated receive should reuse the cached response");

    let first_message =
        first.messages().first().expect("first message should exist");
    let second_message =
        second.messages().first().expect("second message should exist");
    assert_eq!(first_message.message_id(), second_message.message_id());
    assert_eq!(
        first_message.receipt_handle(),
        second_message.receipt_handle()
    );

    runtime.shutdown().await;
}

#[tokio::test]
async fn sqs_fifo_receive_request_attempt_id_does_not_replay_deleted_messages()
{
    let runtime = RuntimeServer::spawn("sdk-sqs-fifo-attempt-delete").await;
    let target = SdkSmokeTarget::new(
        format!("http://{}", runtime.address()),
        "eu-west-2",
    );
    let config = target.load().await;
    let client = Client::new(&config);

    let queue_url = client
        .create_queue()
        .queue_name("attempt-delete.fifo")
        .attributes(QueueAttributeName::FifoQueue, "true")
        .attributes(QueueAttributeName::ContentBasedDeduplication, "true")
        .send()
        .await
        .expect("fifo queue should be created")
        .queue_url()
        .expect("queue URL should be returned")
        .to_owned();

    client
        .send_message()
        .queue_url(&queue_url)
        .message_body("payload")
        .message_group_id("group-1")
        .send()
        .await
        .expect("fifo message should send");

    let first = client
        .receive_message()
        .queue_url(&queue_url)
        .receive_request_attempt_id("attempt-1")
        .visibility_timeout(30)
        .wait_time_seconds(0)
        .send()
        .await
        .expect("first receive should succeed");
    let receipt_handle = first.messages()[0]
        .receipt_handle()
        .expect("receipt handle should exist");
    client
        .delete_message()
        .queue_url(&queue_url)
        .receipt_handle(receipt_handle)
        .send()
        .await
        .expect("message should delete");

    let second = client
        .receive_message()
        .queue_url(&queue_url)
        .receive_request_attempt_id("attempt-1")
        .visibility_timeout(30)
        .wait_time_seconds(0)
        .send()
        .await
        .expect("retry after delete should succeed");
    assert!(second.messages().is_empty());

    runtime.shutdown().await;
}
