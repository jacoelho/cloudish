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

use aws_sdk_cloudwatchlogs::Client as CloudWatchLogsClient;
use aws_sdk_cloudwatchlogs::error::ProvideErrorMetadata;
use aws_sdk_cloudwatchlogs::types::InputLogEvent;
use runtime::RuntimeServer;
use sdk::SdkSmokeTarget;
use std::sync::atomic::{AtomicUsize, Ordering};

static NEXT_ID: AtomicUsize = AtomicUsize::new(0);

fn unique_root() -> String {
    let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    format!("sdk-cloudwatch-logs-{id}")
}

#[tokio::test]
async fn cloudwatch_logs_group_stream_and_event_flow_round_trips() {
    let runtime = RuntimeServer::spawn("sdk-cloudwatch-logs").await;
    let target = SdkSmokeTarget::new(
        format!("http://{}", runtime.address()),
        "eu-west-2",
    );
    let config = target.load().await;
    let client = CloudWatchLogsClient::new(&config);

    let root = unique_root();
    let log_group_name = format!("/app/{root}");
    let log_stream_name = "api";

    client
        .create_log_group()
        .log_group_name(&log_group_name)
        .tags("env", "dev")
        .send()
        .await
        .expect("log group should create");
    let duplicate = client
        .create_log_group()
        .log_group_name(&log_group_name)
        .send()
        .await
        .expect_err("duplicate log group should fail");
    assert_eq!(duplicate.code(), Some("ResourceAlreadyExistsException"));

    client
        .create_log_stream()
        .log_group_name(&log_group_name)
        .log_stream_name(log_stream_name)
        .send()
        .await
        .expect("log stream should create");

    let put_output = client
        .put_log_events()
        .log_group_name(&log_group_name)
        .log_stream_name(log_stream_name)
        .log_events(
            InputLogEvent::builder()
                .timestamp(1_000)
                .message("boot complete")
                .build()
                .expect("log event should build"),
        )
        .log_events(
            InputLogEvent::builder()
                .timestamp(2_000)
                .message("match this line")
                .build()
                .expect("log event should build"),
        )
        .send()
        .await
        .expect("log events should write");
    assert_eq!(put_output.next_sequence_token(), Some("2"));

    let groups = client
        .describe_log_groups()
        .log_group_name_prefix(&log_group_name)
        .send()
        .await
        .expect("log groups should describe");
    assert_eq!(groups.log_groups().len(), 1);
    assert_eq!(
        groups.log_groups()[0].log_group_name(),
        Some(log_group_name.as_str())
    );

    let streams = client
        .describe_log_streams()
        .log_group_name(&log_group_name)
        .log_stream_name_prefix(log_stream_name)
        .send()
        .await
        .expect("log streams should describe");
    assert_eq!(streams.log_streams().len(), 1);
    assert_eq!(
        streams.log_streams()[0].log_stream_name(),
        Some(log_stream_name)
    );

    client
        .put_retention_policy()
        .log_group_name(&log_group_name)
        .retention_in_days(7)
        .send()
        .await
        .expect("retention policy should write");

    let events = client
        .get_log_events()
        .log_group_name(&log_group_name)
        .log_stream_name(log_stream_name)
        .start_from_head(true)
        .send()
        .await
        .expect("log events should load");
    assert_eq!(events.events().len(), 2);
    assert_eq!(events.events()[0].message(), Some("boot complete"));
    assert_eq!(events.events()[1].message(), Some("match this line"));

    let filtered = client
        .filter_log_events()
        .log_group_name(&log_group_name)
        .filter_pattern("match")
        .send()
        .await
        .expect("filtered log events should load");
    assert_eq!(filtered.events().len(), 1);
    assert_eq!(filtered.events()[0].message(), Some("match this line"));

    let missing_stream = client
        .get_log_events()
        .log_group_name(&log_group_name)
        .log_stream_name("missing")
        .send()
        .await
        .expect_err("missing stream should fail");
    assert_eq!(missing_stream.code(), Some("ResourceNotFoundException"));

    assert!(runtime.state_directory().exists());
    runtime.shutdown().await;
}
