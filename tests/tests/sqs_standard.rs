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
use aws_sdk_sqs::primitives::Blob;
use aws_sdk_sqs::types::{
    MessageAttributeValue, MessageSystemAttributeName,
    MessageSystemAttributeNameForSends, MessageSystemAttributeValue,
    QueueAttributeName,
};
use runtime::SharedRuntimeLease;
use sdk::SdkSmokeTarget;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

static SHARED_RUNTIME: runtime::SharedRuntime =
    runtime::SharedRuntime::new("sqs_standard");

async fn shared_runtime() -> SharedRuntimeLease<'static> {
    SHARED_RUNTIME.acquire().await
}

fn unique_name(prefix: &str) -> String {
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock should be after unix epoch")
        .as_nanos();
    format!("{prefix}-{suffix}")
}

#[tokio::test]
async fn sqs_standard_queue_lifecycle_round_trips() {
    let runtime = shared_runtime().await;
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
}

#[tokio::test]
async fn sqs_standard_invalid_receipt_handle_surfaces_explicit_error() {
    let runtime = shared_runtime().await;
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
}

#[tokio::test]
async fn sqs_standard_stale_receipt_handles_fail_after_a_second_receive() {
    let runtime = shared_runtime().await;
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

    client
        .send_message()
        .queue_url(&queue_url)
        .message_body("payload")
        .send()
        .await
        .expect("message should send");
    let first = client
        .receive_message()
        .queue_url(&queue_url)
        .visibility_timeout(1)
        .wait_time_seconds(0)
        .send()
        .await
        .expect("first receive should succeed");
    let first_handle = first
        .messages()
        .first()
        .and_then(|message| message.receipt_handle())
        .expect("first receipt handle should exist")
        .to_owned();

    tokio::time::sleep(Duration::from_secs(2)).await;

    let second = client
        .receive_message()
        .queue_url(&queue_url)
        .visibility_timeout(1)
        .wait_time_seconds(0)
        .send()
        .await
        .expect("second receive should succeed");
    let second_handle = second
        .messages()
        .first()
        .and_then(|message| message.receipt_handle())
        .expect("second receipt handle should exist")
        .to_owned();
    assert_ne!(first_handle, second_handle);

    let error = client
        .delete_message()
        .queue_url(&queue_url)
        .receipt_handle(first_handle)
        .send()
        .await
        .expect_err("stale receipt handles should fail");

    assert_eq!(error.code(), Some("ReceiptHandleIsInvalid"));
    assert!(runtime.state_directory().exists());
}

#[tokio::test]
async fn sqs_standard_message_attributes_and_purge_cooldown_match_sdk_shapes()
{
    let runtime = shared_runtime().await;
    let target = SdkSmokeTarget::new(
        format!("http://{}", runtime.address()),
        "eu-west-2",
    );
    let config = target.load().await;
    let client = Client::new(&config);
    let queue_name = unique_name("attributes");

    let queue_url = client
        .create_queue()
        .queue_name(&queue_name)
        .send()
        .await
        .expect("queue should be created")
        .queue_url()
        .expect("queue URL should be returned")
        .to_owned();

    let sent = client
        .send_message()
        .queue_url(&queue_url)
        .message_body("payload")
        .message_attributes(
            "store",
            MessageAttributeValue::builder()
                .data_type("String")
                .string_value("eu-west")
                .build()
                .expect("string attribute should build"),
        )
        .message_attributes(
            "payload-bytes",
            MessageAttributeValue::builder()
                .data_type("Binary")
                .binary_value(Blob::new([0x01, 0x02, 0x03]))
                .build()
                .expect("binary attribute should build"),
        )
        .message_system_attributes(
            MessageSystemAttributeNameForSends::AwsTraceHeader,
            MessageSystemAttributeValue::builder()
                .data_type("String")
                .string_value("Root=1-67891233-abcdef012345678912345678")
                .build()
                .expect("trace header should build"),
        )
        .send()
        .await
        .expect("message with attributes should send");
    assert!(sent.md5_of_message_attributes().is_some());
    assert!(sent.md5_of_message_system_attributes().is_some());

    let received = client
        .receive_message()
        .queue_url(&queue_url)
        .message_attribute_names("All")
        .message_system_attribute_names(MessageSystemAttributeName::All)
        .visibility_timeout(0)
        .wait_time_seconds(0)
        .send()
        .await
        .expect("message should receive");
    let message =
        received.messages().first().expect("received message should exist");
    let message_attributes =
        message.message_attributes().expect("message attributes should exist");
    assert_eq!(
        message_attributes
            .get("store")
            .and_then(|value| value.string_value())
            .map(str::to_owned),
        Some("eu-west".to_owned())
    );
    assert_eq!(
        message_attributes
            .get("payload-bytes")
            .and_then(|value| value.binary_value())
            .map(|value| value.as_ref().to_vec()),
        Some(vec![0x01, 0x02, 0x03])
    );
    let attributes = message.attributes().expect("attributes should exist");
    assert!(
        attributes.contains_key(&MessageSystemAttributeName::SentTimestamp)
    );
    assert!(
        attributes.contains_key(
            &MessageSystemAttributeName::ApproximateReceiveCount
        )
    );
    assert!(
        attributes.contains_key(&MessageSystemAttributeName::AwsTraceHeader)
    );

    client
        .purge_queue()
        .queue_url(&queue_url)
        .send()
        .await
        .expect("first purge should succeed");
    let error = client
        .purge_queue()
        .queue_url(&queue_url)
        .send()
        .await
        .expect_err("second purge inside cooldown should fail");
    assert_eq!(
        error.code(),
        Some("AWS.SimpleQueueService.PurgeQueueInProgress")
    );
}

#[tokio::test]
async fn sqs_standard_batch_send_returns_md5_of_message_system_attributes() {
    let runtime = shared_runtime().await;
    let target = SdkSmokeTarget::new(
        format!("http://{}", runtime.address()),
        "eu-west-2",
    );
    let config = target.load().await;
    let client = Client::new(&config);
    let queue_name = unique_name("batch-system-attributes");

    let queue_url = client
        .create_queue()
        .queue_name(&queue_name)
        .send()
        .await
        .expect("queue should be created")
        .queue_url()
        .expect("queue URL should be returned")
        .to_owned();

    let sent = client
        .send_message_batch()
        .queue_url(&queue_url)
        .entries(
            aws_sdk_sqs::types::SendMessageBatchRequestEntry::builder()
                .id("entry-1")
                .message_body("payload")
                .message_system_attributes(
                    MessageSystemAttributeNameForSends::AwsTraceHeader,
                    MessageSystemAttributeValue::builder()
                        .data_type("String")
                        .string_value(
                            "Root=1-67891233-abcdef012345678912345678",
                        )
                        .build()
                        .expect("trace header should build"),
                )
                .build()
                .expect("batch entry should build"),
        )
        .send()
        .await
        .expect("batch send should succeed");

    let entry = sent
        .successful()
        .first()
        .expect("successful batch entry should exist");
    assert!(entry.md5_of_message_system_attributes().is_some());
}

#[tokio::test]
async fn sqs_standard_rejects_reserved_attribute_names_and_invalid_trace_headers()
 {
    let runtime = shared_runtime().await;
    let target = SdkSmokeTarget::new(
        format!("http://{}", runtime.address()),
        "eu-west-2",
    );
    let config = target.load().await;
    let client = Client::new(&config);
    let queue_name = unique_name("invalid-attributes");

    let queue_url = client
        .create_queue()
        .queue_name(&queue_name)
        .send()
        .await
        .expect("queue should be created")
        .queue_url()
        .expect("queue URL should be returned")
        .to_owned();

    let reserved_name = client
        .send_message()
        .queue_url(&queue_url)
        .message_body("payload")
        .message_attributes(
            "AWS.TraceId",
            MessageAttributeValue::builder()
                .data_type("String")
                .string_value("eu-west")
                .build()
                .expect("string attribute should build"),
        )
        .send()
        .await
        .expect_err("reserved prefixes should fail");
    let invalid_trace_header = client
        .send_message()
        .queue_url(&queue_url)
        .message_body("payload")
        .message_system_attributes(
            MessageSystemAttributeNameForSends::AwsTraceHeader,
            MessageSystemAttributeValue::builder()
                .data_type("Binary")
                .binary_value(Blob::new([0x01, 0x02, 0x03]))
                .build()
                .expect("binary trace header should build"),
        )
        .send()
        .await
        .expect_err("binary trace headers should fail");

    assert_eq!(reserved_name.code(), Some("InvalidParameterValue"));
    assert_eq!(
        reserved_name.message(),
        Some(
            "Value AWS.TraceId for parameter MessageAttributeName is invalid."
        )
    );
    assert_eq!(invalid_trace_header.code(), Some("InvalidParameterValue"));
    assert_eq!(
        invalid_trace_header.message(),
        Some(
            "Value AWSTraceHeader for parameter MessageSystemAttributes is invalid."
        )
    );
}

#[tokio::test]
async fn sqs_standard_rejects_invalid_message_attribute_data_types() {
    let runtime = shared_runtime().await;
    let target = SdkSmokeTarget::new(
        format!("http://{}", runtime.address()),
        "eu-west-2",
    );
    let config = target.load().await;
    let client = Client::new(&config);
    let queue_name = unique_name("invalid-datatypes");

    let queue_url = client
        .create_queue()
        .queue_name(&queue_name)
        .send()
        .await
        .expect("queue should be created")
        .queue_url()
        .expect("queue URL should be returned")
        .to_owned();

    let bogus_type = client
        .send_message()
        .queue_url(&queue_url)
        .message_body("payload")
        .message_attributes(
            "example",
            MessageAttributeValue::builder()
                .data_type("Bogus")
                .string_value("value")
                .build()
                .expect("string attribute should build"),
        )
        .send()
        .await
        .expect_err("unknown data types should fail");
    let binary_with_string = client
        .send_message()
        .queue_url(&queue_url)
        .message_body("payload")
        .message_attributes(
            "example",
            MessageAttributeValue::builder()
                .data_type("Binary")
                .string_value("value")
                .build()
                .expect("mismatched attribute should build"),
        )
        .send()
        .await
        .expect_err("binary types should reject string values");
    let invalid_custom_label = client
        .send_message()
        .queue_url(&queue_url)
        .message_body("payload")
        .message_attributes(
            "example",
            MessageAttributeValue::builder()
                .data_type("String.!")
                .string_value("value")
                .build()
                .expect("string attribute should build"),
        )
        .send()
        .await
        .expect_err("invalid custom labels should fail");

    assert_eq!(bogus_type.code(), Some("InvalidParameterValue"));
    assert_eq!(
        bogus_type.message(),
        Some("Value Bogus for parameter DataType is invalid.")
    );
    assert_eq!(binary_with_string.code(), Some("InvalidParameterValue"));
    assert_eq!(
        binary_with_string.message(),
        Some("Value Binary for parameter DataType is invalid.")
    );
    assert_eq!(invalid_custom_label.code(), Some("InvalidParameterValue"));
    assert_eq!(
        invalid_custom_label.message(),
        Some("Value String.! for parameter DataType is invalid.")
    );
}

#[tokio::test]
async fn sqs_standard_add_permission_rejects_invalid_actions() {
    let runtime = shared_runtime().await;
    let target = SdkSmokeTarget::new(
        format!("http://{}", runtime.address()),
        "eu-west-2",
    );
    let config = target.load().await;
    let client = Client::new(&config);
    let queue_name = unique_name("invalid-actions");

    let queue_url = client
        .create_queue()
        .queue_name(&queue_name)
        .send()
        .await
        .expect("queue should be created")
        .queue_url()
        .expect("queue URL should be returned")
        .to_owned();

    let error = client
        .add_permission()
        .queue_url(&queue_url)
        .label("AllowSend")
        .aws_account_ids("111122223333")
        .actions("FlyMessage")
        .send()
        .await
        .expect_err("invalid add-permission actions should fail");

    assert_eq!(error.code(), Some("InvalidParameterValue"));
    assert_eq!(
        error.message(),
        Some("Value FlyMessage for parameter ActionName is invalid.")
    );
}

#[tokio::test]
async fn sqs_standard_permission_apis_update_the_policy_attribute() {
    let runtime = shared_runtime().await;
    let target = SdkSmokeTarget::new(
        format!("http://{}", runtime.address()),
        "eu-west-2",
    );
    let config = target.load().await;
    let client = Client::new(&config);
    let queue_name = unique_name("policy");

    let queue_url = client
        .create_queue()
        .queue_name(&queue_name)
        .send()
        .await
        .expect("queue should be created")
        .queue_url()
        .expect("queue URL should be returned")
        .to_owned();

    client
        .add_permission()
        .queue_url(&queue_url)
        .label("AllowSend")
        .aws_account_ids("111122223333")
        .actions("SendMessage")
        .send()
        .await
        .expect("permission should be added");
    let attributes = client
        .get_queue_attributes()
        .queue_url(&queue_url)
        .attribute_names(QueueAttributeName::Policy)
        .send()
        .await
        .expect("policy should fetch");
    let policy = attributes
        .attributes()
        .and_then(|attributes| attributes.get(&QueueAttributeName::Policy))
        .expect("policy should exist");
    assert!(policy.contains("\"Sid\":\"AllowSend\""));
    assert!(policy.contains("\"SQS:SendMessage\""));

    client
        .remove_permission()
        .queue_url(&queue_url)
        .label("AllowSend")
        .send()
        .await
        .expect("permission should be removed");
    let attributes = client
        .get_queue_attributes()
        .queue_url(&queue_url)
        .attribute_names(QueueAttributeName::Policy)
        .send()
        .await
        .expect("policy should fetch");
    assert_eq!(
        attributes
            .attributes()
            .and_then(|attributes| attributes.get(&QueueAttributeName::Policy))
            .map(String::as_str),
        Some("{\"Statement\":[],\"Version\":\"2012-10-17\"}")
    );
}

#[tokio::test]
async fn sqs_standard_list_queues_paginates_with_next_token() {
    let runtime = shared_runtime().await;
    let target = SdkSmokeTarget::new(
        format!("http://{}", runtime.address()),
        "eu-west-2",
    );
    let config = target.load().await;
    let client = Client::new(&config);
    let prefix = unique_name("page");

    for suffix in ["a", "b", "c"] {
        client
            .create_queue()
            .queue_name(format!("{prefix}-{suffix}"))
            .send()
            .await
            .expect("queue should be created");
    }

    let first = client
        .list_queues()
        .queue_name_prefix(&prefix)
        .max_results(2)
        .send()
        .await
        .expect("first page should load");
    assert_eq!(first.queue_urls().len(), 2);
    let next_token =
        first.next_token().expect("first page should include a token");

    let second = client
        .list_queues()
        .queue_name_prefix(&prefix)
        .max_results(2)
        .next_token(next_token)
        .send()
        .await
        .expect("second page should load");
    assert_eq!(second.queue_urls().len(), 1);
    assert!(second.next_token().is_none());
}

#[tokio::test]
async fn sqs_standard_list_queues_rejects_invalid_max_results() {
    let runtime = shared_runtime().await;
    let target = SdkSmokeTarget::new(
        format!("http://{}", runtime.address()),
        "eu-west-2",
    );
    let config = target.load().await;
    let client = Client::new(&config);

    let zero = client
        .list_queues()
        .max_results(0)
        .send()
        .await
        .expect_err("zero max results should fail");
    let too_large = client
        .list_queues()
        .max_results(1_001)
        .send()
        .await
        .expect_err("oversized max results should fail");

    assert_eq!(zero.code(), Some("InvalidParameterValue"));
    assert_eq!(
        zero.message(),
        Some("Value 0 for parameter MaxResults is invalid.")
    );
    assert_eq!(too_large.code(), Some("InvalidParameterValue"));
    assert_eq!(
        too_large.message(),
        Some("Value 1001 for parameter MaxResults is invalid.")
    );
}
