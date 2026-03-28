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

use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3::config::Builder as S3ConfigBuilder;
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::primitives::{ByteStream, DateTime};
use aws_sdk_s3::types::{
    CsvInput, CsvOutput, DefaultRetention, Event as NotificationEvent,
    ExpressionType, FileHeaderInfo, FilterRule, FilterRuleName,
    InputSerialization, NotificationConfiguration,
    NotificationConfigurationFilter, ObjectLockConfiguration,
    ObjectLockEnabled, ObjectLockLegalHold, ObjectLockLegalHoldStatus,
    ObjectLockMode, ObjectLockRetention, ObjectLockRetentionMode,
    ObjectLockRule, OutputSerialization, QueueConfiguration, RestoreRequest,
    S3KeyFilter, SelectObjectContentEventStream,
};
use aws_sdk_sqs::Client as SqsClient;
use aws_sdk_sqs::types::QueueAttributeName;
use runtime::RuntimeServer;
use sdk::SdkSmokeTarget;

async fn shared_clients(target: &SdkSmokeTarget) -> (S3Client, SqsClient) {
    let shared = target.load().await;
    let s3 = S3Client::from_conf(
        S3ConfigBuilder::from(&shared).force_path_style(true).build(),
    );
    let sqs = SqsClient::new(&shared);

    (s3, sqs)
}

fn assert_target(target: &SdkSmokeTarget, runtime: &RuntimeServer) {
    assert!(runtime.state_directory().exists());
    assert_eq!(target.access_key_id(), "test");
    assert_eq!(target.secret_access_key(), "test");
    assert_eq!(target.region(), "eu-west-2");
    assert_eq!(target.endpoint_url(), format!("http://{}", runtime.address()));
}

#[tokio::test]
async fn s3_notifications_sqs_event_configuration_and_delivery_round_trip() {
    let runtime = RuntimeServer::spawn("sdk-s3-notifications-events").await;
    let target = SdkSmokeTarget::new(
        format!("http://{}", runtime.address()),
        "eu-west-2",
    );
    assert_target(&target, &runtime);
    let (s3, sqs) = shared_clients(&target).await;

    s3.create_bucket()
        .bucket("sdk-s3-notifications-events")
        .send()
        .await
        .expect("bucket should be created");

    let queue = sqs
        .create_queue()
        .queue_name("sdk-s3-notifications-events")
        .send()
        .await
        .expect("queue should be created");
    let queue_url =
        queue.queue_url().expect("queue URL should be returned").to_owned();
    let queue_attributes = sqs
        .get_queue_attributes()
        .queue_url(&queue_url)
        .attribute_names(QueueAttributeName::QueueArn)
        .send()
        .await
        .expect("queue attributes should load");
    let queue_arn = queue_attributes
        .attributes()
        .and_then(|attributes| attributes.get(&QueueAttributeName::QueueArn))
        .cloned()
        .expect("queue ARN should be returned");

    let queue_configuration = QueueConfiguration::builder()
        .id("orders-events")
        .queue_arn(&queue_arn)
        .events(NotificationEvent::S3ObjectCreatedPut)
        .filter(
            NotificationConfigurationFilter::builder()
                .key(
                    S3KeyFilter::builder()
                        .filter_rules(
                            FilterRule::builder()
                                .name(FilterRuleName::Prefix)
                                .value("reports/")
                                .build(),
                        )
                        .filter_rules(
                            FilterRule::builder()
                                .name(FilterRuleName::Suffix)
                                .value(".csv")
                                .build(),
                        )
                        .build(),
                )
                .build(),
        )
        .build()
        .expect("queue notification configuration should build");

    s3.put_bucket_notification_configuration()
        .bucket("sdk-s3-notifications-events")
        .notification_configuration(
            NotificationConfiguration::builder()
                .queue_configurations(queue_configuration)
                .build(),
        )
        .send()
        .await
        .expect("notification configuration should apply");

    let configuration = s3
        .get_bucket_notification_configuration()
        .bucket("sdk-s3-notifications-events")
        .send()
        .await
        .expect("notification configuration should read");
    let queue_configuration = configuration
        .queue_configurations()
        .first()
        .expect("queue configuration should be returned");

    assert_eq!(configuration.queue_configurations().len(), 1);
    assert_eq!(queue_configuration.id(), Some("orders-events"));
    assert_eq!(queue_configuration.queue_arn(), queue_arn.as_str());
    assert_eq!(
        queue_configuration.events(),
        &[NotificationEvent::S3ObjectCreatedPut]
    );
    assert!(queue_configuration.filter().is_some());

    s3.put_object()
        .bucket("sdk-s3-notifications-events")
        .key("reports/data.csv")
        .body(ByteStream::from_static(b"id,total\n1,25\n"))
        .send()
        .await
        .expect("matching object should be stored");

    let received = sqs
        .receive_message()
        .queue_url(&queue_url)
        .wait_time_seconds(0)
        .send()
        .await
        .expect("notification should be readable from SQS");
    let message = received
        .messages()
        .first()
        .expect("matching upload should emit one notification");
    let notification: serde_json::Value = serde_json::from_str(
        message.body().expect("notification should have a body"),
    )
    .expect("notification should be valid JSON");

    assert_eq!(notification["Records"][0]["eventName"], "ObjectCreated:Put");
    assert_eq!(
        notification["Records"][0]["s3"]["bucket"]["name"],
        "sdk-s3-notifications-events"
    );
    assert_eq!(
        notification["Records"][0]["s3"]["object"]["key"],
        "reports%2Fdata.csv"
    );

    sqs.delete_message()
        .queue_url(&queue_url)
        .receipt_handle(
            message
                .receipt_handle()
                .expect("notification should expose a receipt handle"),
        )
        .send()
        .await
        .expect("notification should delete cleanly");

    s3.put_object()
        .bucket("sdk-s3-notifications-events")
        .key("logs/data.csv")
        .body(ByteStream::from_static(b"id,total\n2,50\n"))
        .send()
        .await
        .expect("non-matching object should still store");

    let filtered = sqs
        .receive_message()
        .queue_url(&queue_url)
        .wait_time_seconds(0)
        .send()
        .await
        .expect("filtered receive should succeed");
    assert!(filtered.messages().is_empty());

    runtime.shutdown().await;
}

#[tokio::test]
async fn s3_notifications_object_lock_select_and_restore_behave_explicitly() {
    let runtime = RuntimeServer::spawn("sdk-s3-notifications-advanced").await;
    let target = SdkSmokeTarget::new(
        format!("http://{}", runtime.address()),
        "eu-west-2",
    );
    assert_target(&target, &runtime);
    let (s3, _) = shared_clients(&target).await;

    s3.create_bucket()
        .bucket("sdk-s3-notifications-lock")
        .object_lock_enabled_for_bucket(true)
        .send()
        .await
        .expect("object-lock bucket should be created");
    s3.put_object_lock_configuration()
        .bucket("sdk-s3-notifications-lock")
        .object_lock_configuration(
            ObjectLockConfiguration::builder()
                .object_lock_enabled(ObjectLockEnabled::Enabled)
                .rule(
                    ObjectLockRule::builder()
                        .default_retention(
                            DefaultRetention::builder()
                                .mode(ObjectLockRetentionMode::Governance)
                                .days(1)
                                .build(),
                        )
                        .build(),
                )
                .build(),
        )
        .send()
        .await
        .expect("object-lock configuration should apply");

    let bucket_lock = s3
        .get_object_lock_configuration()
        .bucket("sdk-s3-notifications-lock")
        .send()
        .await
        .expect("object-lock configuration should load");
    let bucket_lock = bucket_lock
        .object_lock_configuration()
        .expect("object-lock configuration should exist");
    let default_retention = bucket_lock
        .rule()
        .and_then(|rule| rule.default_retention())
        .expect("default retention should exist");

    assert_eq!(
        bucket_lock.object_lock_enabled(),
        Some(&ObjectLockEnabled::Enabled)
    );
    assert_eq!(
        default_retention.mode(),
        Some(&ObjectLockRetentionMode::Governance)
    );
    assert_eq!(default_retention.days(), Some(1));

    let initial_retain_until = DateTime::from_secs(1_893_456_000);
    let created = s3
        .put_object()
        .bucket("sdk-s3-notifications-lock")
        .key("records/report.csv")
        .body(ByteStream::from_static(b"id,total\n1,25\n2,40\n"))
        .object_lock_mode(ObjectLockMode::Governance)
        .object_lock_retain_until_date(initial_retain_until)
        .send()
        .await
        .expect("governance-locked object should write");
    let version_id = created
        .version_id()
        .expect("versioned object should expose a version id")
        .to_owned();

    let current = s3
        .get_object()
        .bucket("sdk-s3-notifications-lock")
        .key("records/report.csv")
        .send()
        .await
        .expect("stored object should be readable");
    assert_eq!(current.object_lock_mode(), Some(&ObjectLockMode::Governance));
    assert_eq!(
        current
            .object_lock_retain_until_date()
            .expect("retention date should be present")
            .secs(),
        initial_retain_until.secs()
    );

    let current_retention = s3
        .get_object_retention()
        .bucket("sdk-s3-notifications-lock")
        .key("records/report.csv")
        .version_id(&version_id)
        .send()
        .await
        .expect("object retention should read");
    let current_retention =
        current_retention.retention().expect("retention should exist");
    assert_eq!(
        current_retention.mode(),
        Some(&ObjectLockRetentionMode::Governance)
    );
    assert_eq!(
        current_retention
            .retain_until_date()
            .expect("retain-until date should exist")
            .secs(),
        initial_retain_until.secs()
    );

    let delete_error = s3
        .delete_object()
        .bucket("sdk-s3-notifications-lock")
        .key("records/report.csv")
        .version_id(&version_id)
        .bypass_governance_retention(true)
        .send()
        .await
        .expect_err(
            "governance bypass without authorization should be rejected",
        );
    assert_eq!(delete_error.code(), Some("AccessDenied"));

    let extended_retain_until = DateTime::from_secs(1_893_542_400);
    s3.put_object_retention()
        .bucket("sdk-s3-notifications-lock")
        .key("records/report.csv")
        .version_id(&version_id)
        .retention(
            ObjectLockRetention::builder()
                .mode(ObjectLockRetentionMode::Governance)
                .retain_until_date(extended_retain_until)
                .build(),
        )
        .send()
        .await
        .expect("retention should be extendable");
    let updated_retention = s3
        .get_object_retention()
        .bucket("sdk-s3-notifications-lock")
        .key("records/report.csv")
        .version_id(&version_id)
        .send()
        .await
        .expect("updated retention should read");
    assert_eq!(
        updated_retention
            .retention()
            .and_then(|retention| retention.retain_until_date())
            .expect("updated retain-until date should exist")
            .secs(),
        extended_retain_until.secs()
    );

    s3.put_object_legal_hold()
        .bucket("sdk-s3-notifications-lock")
        .key("records/report.csv")
        .version_id(&version_id)
        .legal_hold(
            ObjectLockLegalHold::builder()
                .status(ObjectLockLegalHoldStatus::On)
                .build(),
        )
        .send()
        .await
        .expect("legal hold should enable");
    let legal_hold = s3
        .get_object_legal_hold()
        .bucket("sdk-s3-notifications-lock")
        .key("records/report.csv")
        .version_id(&version_id)
        .send()
        .await
        .expect("legal hold should read");
    assert_eq!(
        legal_hold.legal_hold().and_then(|legal_hold| legal_hold.status()),
        Some(&ObjectLockLegalHoldStatus::On)
    );

    s3.create_bucket()
        .bucket("sdk-s3-notifications-select")
        .send()
        .await
        .expect("select bucket should be created");
    s3.put_object()
        .bucket("sdk-s3-notifications-select")
        .key("reports/input.csv")
        .body(ByteStream::from_static(
            b"name,total\napples,25\nbananas,10\noranges,40\n",
        ))
        .send()
        .await
        .expect("select source object should write");

    let select = s3
        .select_object_content()
        .bucket("sdk-s3-notifications-select")
        .key("reports/input.csv")
        .expression("SELECT name,total FROM S3Object s WHERE total > 20")
        .expression_type(ExpressionType::Sql)
        .input_serialization(
            InputSerialization::builder()
                .csv(
                    CsvInput::builder()
                        .file_header_info(FileHeaderInfo::Use)
                        .build(),
                )
                .build(),
        )
        .output_serialization(
            OutputSerialization::builder()
                .csv(CsvOutput::builder().build())
                .build(),
        )
        .send()
        .await
        .expect("select query should succeed");

    let mut payload = select.payload;
    let mut records = Vec::new();
    let mut stats = None;
    let mut saw_end = false;
    while let Some(event) =
        payload.recv().await.expect("select event stream should decode")
    {
        match event {
            SelectObjectContentEventStream::Records(record) => {
                if let Some(chunk) = record.payload() {
                    records.extend_from_slice(chunk.as_ref());
                }
            }
            SelectObjectContentEventStream::Stats(event) => {
                stats = event.details().cloned();
            }
            SelectObjectContentEventStream::End(_) => saw_end = true,
            _ => {}
        }
    }

    let records = String::from_utf8(records)
        .expect("select records should be valid UTF-8");
    let stats = stats.expect("select stats should be present");

    assert_eq!(records, "apples,25\noranges,40\n");
    assert_eq!(stats.bytes_scanned(), Some(43));
    assert_eq!(stats.bytes_processed(), Some(43));
    assert_eq!(stats.bytes_returned(), Some(21));
    assert!(saw_end);

    let invalid_select = s3
        .select_object_content()
        .bucket("sdk-s3-notifications-select")
        .key("reports/input.csv")
        .expression("SELECT missing FROM S3Object s")
        .expression_type(ExpressionType::Sql)
        .input_serialization(
            InputSerialization::builder()
                .csv(
                    CsvInput::builder()
                        .file_header_info(FileHeaderInfo::Use)
                        .build(),
                )
                .build(),
        )
        .output_serialization(
            OutputSerialization::builder()
                .csv(CsvOutput::builder().build())
                .build(),
        )
        .send()
        .await
        .expect_err("invalid select input should fail explicitly");
    assert_eq!(invalid_select.code(), Some("InvalidArgument"));

    let restore_error = s3
        .restore_object()
        .bucket("sdk-s3-notifications-select")
        .key("reports/input.csv")
        .restore_request(RestoreRequest::builder().days(1).build())
        .send()
        .await
        .expect_err("restore should surface the explicit stub error");
    assert_eq!(restore_error.code(), Some("NotImplemented"));
    assert_eq!(
        restore_error.message(),
        Some("RestoreObject lifecycle is not implemented.")
    );

    runtime.shutdown().await;
}
