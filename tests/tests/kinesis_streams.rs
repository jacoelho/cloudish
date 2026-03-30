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
use tests::common::runtime;
use tests::common::sdk;

use aws_sdk_kinesis::Client as KinesisClient;
use aws_sdk_kinesis::error::ProvideErrorMetadata;
use aws_sdk_kinesis::primitives::Blob;
use aws_sdk_kinesis::types::{
    EncryptionType, PutRecordsRequestEntry, ShardIteratorType,
};
use runtime::SharedRuntimeLease;
use sdk::SdkSmokeTarget;

static SHARED_RUNTIME: runtime::SharedRuntime =
    runtime::SharedRuntime::new("kinesis_streams");

async fn shared_runtime() -> SharedRuntimeLease<'static> {
    SHARED_RUNTIME.acquire().await
}

use std::sync::atomic::{AtomicUsize, Ordering};

static NEXT_ID: AtomicUsize = AtomicUsize::new(0);

fn unique_stream(prefix: &str) -> String {
    let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}-{id}")
}

#[tokio::test]
async fn kinesis_stream_lifecycle_and_records_round_trip() {
    let runtime = shared_runtime().await;
    let target = SdkSmokeTarget::new(
        format!("http://{}", runtime.address()),
        "eu-west-2",
    );
    let config = target.load().await;
    let client = KinesisClient::new(&config);

    let stream_name = unique_stream("sdk-kinesis-records");
    client
        .create_stream()
        .stream_name(&stream_name)
        .shard_count(1)
        .send()
        .await
        .expect("stream should create");

    let describe = client
        .describe_stream()
        .stream_name(&stream_name)
        .send()
        .await
        .expect("stream should describe");
    let shard_id = describe
        .stream_description()
        .and_then(|description| description.shards().first())
        .map(|shard| shard.shard_id())
        .expect("stream should expose a shard");

    let put = client
        .put_record()
        .stream_name(&stream_name)
        .partition_key("pk")
        .data(Blob::new(b"one"))
        .send()
        .await
        .expect("record should write");
    assert_eq!(put.shard_id(), shard_id);

    let batch = client
        .put_records()
        .stream_name(&stream_name)
        .records(
            PutRecordsRequestEntry::builder()
                .partition_key("pk")
                .data(Blob::new(b"two"))
                .build()
                .expect("first batch record should build"),
        )
        .records(
            PutRecordsRequestEntry::builder()
                .partition_key("pk")
                .data(Blob::new(b"three"))
                .build()
                .expect("second batch record should build"),
        )
        .send()
        .await
        .expect("batch should write");
    assert_eq!(batch.failed_record_count(), Some(0));

    let iterator = client
        .get_shard_iterator()
        .stream_name(&stream_name)
        .shard_id(shard_id)
        .shard_iterator_type(ShardIteratorType::TrimHorizon)
        .send()
        .await
        .expect("iterator should create");
    let records = client
        .get_records()
        .shard_iterator(
            iterator.shard_iterator().expect("iterator should exist"),
        )
        .limit(10)
        .send()
        .await
        .expect("records should read");

    assert_eq!(records.records().len(), 3);
    assert_eq!(records.records()[0].data().as_ref(), b"one");
    assert_eq!(records.records()[1].data().as_ref(), b"two");
    assert_eq!(records.records()[2].data().as_ref(), b"three");
    assert!(records.next_shard_iterator().is_some());

    client
        .delete_stream()
        .stream_name(&stream_name)
        .send()
        .await
        .expect("stream should delete");
}

#[tokio::test]
async fn kinesis_consumers_topology_tags_and_encryption_round_trip() {
    let runtime = shared_runtime().await;
    let target = SdkSmokeTarget::new(
        format!("http://{}", runtime.address()),
        "eu-west-2",
    );
    let config = target.load().await;
    let client = KinesisClient::new(&config);

    let stream_name = unique_stream("sdk-kinesis-meta");
    client
        .create_stream()
        .stream_name(&stream_name)
        .shard_count(1)
        .send()
        .await
        .expect("stream should create");

    let summary = client
        .describe_stream_summary()
        .stream_name(&stream_name)
        .send()
        .await
        .expect("summary should load");
    let stream_arn = summary
        .stream_description_summary()
        .map(|summary| summary.stream_arn())
        .expect("summary should expose an ARN")
        .to_owned();
    let consumer = client
        .register_stream_consumer()
        .stream_arn(&stream_arn)
        .consumer_name("analytics")
        .send()
        .await
        .expect("consumer should register");
    let consumer_arn = consumer
        .consumer()
        .map(|consumer| consumer.consumer_arn())
        .expect("consumer arn should exist")
        .to_owned();

    let described_consumer = client
        .describe_stream_consumer()
        .consumer_arn(&consumer_arn)
        .send()
        .await
        .expect("consumer should describe");
    assert_eq!(
        described_consumer
            .consumer_description()
            .map(|consumer| consumer.consumer_name()),
        Some("analytics")
    );

    let listed_consumers = client
        .list_stream_consumers()
        .stream_arn(&stream_arn)
        .max_results(10)
        .send()
        .await
        .expect("consumers should list");
    assert_eq!(listed_consumers.consumers().len(), 1);

    client
        .add_tags_to_stream()
        .stream_name(&stream_name)
        .tags("env", "dev")
        .tags("team", "core")
        .send()
        .await
        .expect("tags should attach");
    let tags = client
        .list_tags_for_stream()
        .stream_name(&stream_name)
        .send()
        .await
        .expect("tags should list");
    assert_eq!(tags.tags().len(), 2);

    client
        .start_stream_encryption()
        .stream_name(&stream_name)
        .encryption_type(EncryptionType::Kms)
        .key_id("alias/cloudish")
        .send()
        .await
        .expect("encryption should start");
    let encrypted_summary = client
        .describe_stream_summary()
        .stream_name(&stream_name)
        .send()
        .await
        .expect("encrypted summary should load");
    assert_eq!(
        encrypted_summary
            .stream_description_summary()
            .and_then(|summary| summary.encryption_type()),
        Some(&EncryptionType::Kms)
    );

    let shard_id = client
        .describe_stream()
        .stream_name(&stream_name)
        .send()
        .await
        .expect("stream should describe")
        .stream_description()
        .and_then(|description| description.shards().first())
        .map(|shard| shard.shard_id())
        .expect("stream should expose a shard")
        .to_owned();
    client
        .split_shard()
        .stream_name(&stream_name)
        .shard_to_split(&shard_id)
        .new_starting_hash_key((u128::MAX / 2).to_string())
        .send()
        .await
        .expect("shard should split");
    let listed_shards = client
        .list_shards()
        .stream_name(&stream_name)
        .max_results(10)
        .send()
        .await
        .expect("shards should list");
    let child_ids = listed_shards
        .shards()
        .iter()
        .filter(|shard| shard.parent_shard_id().is_some())
        .map(|shard| shard.shard_id())
        .collect::<Vec<_>>();
    assert_eq!(child_ids.len(), 2);
    client
        .merge_shards()
        .stream_name(&stream_name)
        .shard_to_merge(child_ids[0])
        .adjacent_shard_to_merge(child_ids[1])
        .send()
        .await
        .expect("child shards should merge");

    client
        .remove_tags_from_stream()
        .stream_name(&stream_name)
        .tag_keys("env")
        .send()
        .await
        .expect("tag should remove");
    let tags = client
        .list_tags_for_stream()
        .stream_name(&stream_name)
        .send()
        .await
        .expect("remaining tags should list");
    assert_eq!(tags.tags().len(), 1);

    client
        .stop_stream_encryption()
        .stream_name(&stream_name)
        .encryption_type(EncryptionType::Kms)
        .key_id("alias/cloudish")
        .send()
        .await
        .expect("encryption should stop");
    client
        .deregister_stream_consumer()
        .consumer_arn(&consumer_arn)
        .send()
        .await
        .expect("consumer should deregister");
    client
        .delete_stream()
        .stream_name(&stream_name)
        .send()
        .await
        .expect("stream should delete");
}

#[tokio::test]
async fn kinesis_reports_explicit_errors() {
    let runtime = shared_runtime().await;
    let target = SdkSmokeTarget::new(
        format!("http://{}", runtime.address()),
        "eu-west-2",
    );
    let config = target.load().await;
    let client = KinesisClient::new(&config);

    let stream_name = unique_stream("sdk-kinesis-errors");
    client
        .create_stream()
        .stream_name(&stream_name)
        .shard_count(1)
        .send()
        .await
        .expect("stream should create");

    let duplicate = client
        .create_stream()
        .stream_name(&stream_name)
        .shard_count(1)
        .send()
        .await
        .expect_err("duplicate stream should fail");
    assert_eq!(duplicate.code(), Some("ResourceInUseException"));

    let invalid_iterator = client
        .get_records()
        .shard_iterator("bogus")
        .limit(1)
        .send()
        .await
        .expect_err("invalid iterator should fail");
    assert_eq!(invalid_iterator.code(), Some("InvalidArgumentException"));
}
