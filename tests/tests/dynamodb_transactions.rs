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

use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::error::ProvideErrorMetadata;
use aws_sdk_dynamodb::types::{
    AttributeDefinition, AttributeValue, BillingMode, Get, KeySchemaElement,
    KeyType, Put, ScalarAttributeType, StreamSpecification, StreamViewType,
    Tag, TimeToLiveSpecification, TransactGetItem, TransactWriteItem,
};
use aws_sdk_dynamodbstreams::Client as StreamsClient;
use aws_sdk_dynamodbstreams::types::{
    OperationType, ShardIteratorType, StreamStatus as StreamsStatus,
    StreamViewType as StreamsViewType,
};
use runtime::SharedRuntimeLease;
use sdk::SdkSmokeTarget;

static SHARED_RUNTIME: runtime::SharedRuntime =
    runtime::SharedRuntime::new("dynamodb_transactions");

async fn shared_runtime() -> SharedRuntimeLease<'static> {
    SHARED_RUNTIME.acquire().await
}

use std::collections::HashMap;

fn string(value: &str) -> AttributeValue {
    AttributeValue::S(value.to_owned())
}

fn order_item(
    tenant: &str,
    order: &str,
    status: &str,
) -> HashMap<String, AttributeValue> {
    HashMap::from([
        ("tenant".to_owned(), string(tenant)),
        ("order".to_owned(), string(order)),
        ("status".to_owned(), string(status)),
    ])
}

async fn create_orders_table(client: &Client) -> String {
    let created = client
        .create_table()
        .table_name("orders")
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("tenant")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .expect("tenant attribute definition should build"),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("order")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .expect("order attribute definition should build"),
        )
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("tenant")
                .key_type(KeyType::Hash)
                .build()
                .expect("partition key should build"),
        )
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("order")
                .key_type(KeyType::Range)
                .build()
                .expect("sort key should build"),
        )
        .billing_mode(BillingMode::PayPerRequest)
        .send()
        .await
        .expect("table should be created");

    created
        .table_description()
        .and_then(|table| table.table_arn())
        .expect("table arn should be returned")
        .to_owned()
}

#[tokio::test]
async fn dynamodb_transactions_round_trip_ttl_tags_and_streams() {
    let runtime = shared_runtime().await;
    let target = SdkSmokeTarget::new(
        format!("http://{}", runtime.address()),
        "eu-west-2",
    );
    let config = target.load().await;
    let client = Client::new(&config);
    let streams = StreamsClient::new(&config);

    let table_arn = create_orders_table(&client).await;

    let updated = client
        .update_table()
        .table_name("orders")
        .stream_specification(
            StreamSpecification::builder()
                .stream_enabled(true)
                .stream_view_type(StreamViewType::NewAndOldImages)
                .build()
                .expect("stream specification should build"),
        )
        .send()
        .await
        .expect("stream update should succeed");
    let table = updated
        .table_description()
        .expect("table description should be returned");
    assert_eq!(
        table
            .stream_specification()
            .and_then(|specification| specification.stream_view_type())
            .map(|view| view.as_str()),
        Some("NEW_AND_OLD_IMAGES")
    );
    let stream_arn = table
        .latest_stream_arn()
        .expect("latest stream arn should be present")
        .to_owned();

    let ttl = client
        .update_time_to_live()
        .table_name("orders")
        .time_to_live_specification(
            TimeToLiveSpecification::builder()
                .attribute_name("expires_at")
                .enabled(true)
                .build()
                .expect("ttl specification should build"),
        )
        .send()
        .await
        .expect("ttl update should succeed");
    assert_eq!(
        ttl.time_to_live_specification()
            .expect("ttl specification should be returned")
            .attribute_name(),
        "expires_at"
    );

    let ttl_description = client
        .describe_time_to_live()
        .table_name("orders")
        .send()
        .await
        .expect("ttl description should succeed");
    let ttl_description = ttl_description
        .time_to_live_description()
        .expect("ttl description should be returned");
    assert_eq!(ttl_description.attribute_name(), Some("expires_at"));
    assert_eq!(
        ttl_description
            .time_to_live_status()
            .expect("ttl status should be present")
            .as_str(),
        "ENABLING"
    );

    client
        .tag_resource()
        .resource_arn(&table_arn)
        .tags(
            Tag::builder()
                .key("env")
                .value("test")
                .build()
                .expect("tag should build"),
        )
        .tags(
            Tag::builder()
                .key("team")
                .value("payments")
                .build()
                .expect("tag should build"),
        )
        .send()
        .await
        .expect("tagging should succeed");
    let tags = client
        .list_tags_of_resource()
        .resource_arn(&table_arn)
        .send()
        .await
        .expect("list tags should succeed");
    assert_eq!(tags.tags().len(), 2);
    assert_eq!(tags.tags()[0].key(), "env");
    assert_eq!(tags.tags()[1].key(), "team");

    client
        .untag_resource()
        .resource_arn(&table_arn)
        .tag_keys("env")
        .send()
        .await
        .expect("untag should succeed");
    let tags = client
        .list_tags_of_resource()
        .resource_arn(&table_arn)
        .send()
        .await
        .expect("list tags should succeed");
    assert_eq!(tags.tags().len(), 1);
    assert_eq!(tags.tags()[0].key(), "team");
    assert_eq!(tags.tags()[0].value(), "payments");

    client
        .transact_write_items()
        .transact_items(
            TransactWriteItem::builder()
                .put(
                    Put::builder()
                        .table_name("orders")
                        .set_item(Some(order_item(
                            "tenant-a", "001", "pending",
                        )))
                        .build()
                        .expect("transaction put should build"),
                )
                .build(),
        )
        .transact_items(
            TransactWriteItem::builder()
                .put(
                    Put::builder()
                        .table_name("orders")
                        .set_item(Some(order_item(
                            "tenant-a",
                            "002",
                            "confirmed",
                        )))
                        .build()
                        .expect("transaction put should build"),
                )
                .build(),
        )
        .send()
        .await
        .expect("transaction write should succeed");

    let fetched = client
        .transact_get_items()
        .transact_items(
            TransactGetItem::builder()
                .get(
                    Get::builder()
                        .table_name("orders")
                        .key("tenant", string("tenant-a"))
                        .key("order", string("001"))
                        .build()
                        .expect("transaction get should build"),
                )
                .build(),
        )
        .transact_items(
            TransactGetItem::builder()
                .get(
                    Get::builder()
                        .table_name("orders")
                        .key("tenant", string("tenant-a"))
                        .key("order", string("002"))
                        .build()
                        .expect("transaction get should build"),
                )
                .build(),
        )
        .send()
        .await
        .expect("transaction get should succeed");
    assert_eq!(fetched.responses().len(), 2);
    assert_eq!(
        fetched.responses()[0].item().and_then(|item| item.get("status")),
        Some(&string("pending"))
    );
    assert_eq!(
        fetched.responses()[1].item().and_then(|item| item.get("status")),
        Some(&string("confirmed"))
    );

    let listed = streams
        .list_streams()
        .table_name("orders")
        .send()
        .await
        .expect("list streams should succeed");
    assert_eq!(listed.streams().len(), 1);
    assert_eq!(listed.streams()[0].stream_arn(), Some(stream_arn.as_str()));

    let described = streams
        .describe_stream()
        .stream_arn(&stream_arn)
        .send()
        .await
        .expect("describe stream should succeed");
    let stream = described
        .stream_description()
        .expect("stream description should be returned");
    assert_eq!(stream.stream_status(), Some(&StreamsStatus::Enabled));
    assert_eq!(
        stream.stream_view_type(),
        Some(&StreamsViewType::NewAndOldImages)
    );
    let shard_id = stream.shards()[0]
        .shard_id()
        .expect("shard id should be present")
        .to_owned();

    let iterator = streams
        .get_shard_iterator()
        .stream_arn(&stream_arn)
        .shard_id(&shard_id)
        .shard_iterator_type(ShardIteratorType::TrimHorizon)
        .send()
        .await
        .expect("get shard iterator should succeed");
    let records = streams
        .get_records()
        .shard_iterator(
            iterator.shard_iterator().expect("iterator should be returned"),
        )
        .send()
        .await
        .expect("get records should succeed");
    assert_eq!(records.records().len(), 2);
    assert_eq!(
        records.records()[0].event_name(),
        Some(&OperationType::Insert)
    );
    assert_eq!(
        records.records()[0]
            .dynamodb()
            .and_then(|record| record.sequence_number()),
        Some("000000000000000000001")
    );
    assert_eq!(
        records.records()[1]
            .dynamodb()
            .and_then(|record| record.new_image())
            .and_then(|image| image.get("status")),
        Some(&aws_sdk_dynamodbstreams::types::AttributeValue::S(
            "confirmed".to_owned()
        ))
    );
    assert!(records.next_shard_iterator().is_some());

    assert!(runtime.state_directory().exists());
}

#[tokio::test]
async fn dynamodb_transactions_invalid_stream_iterator_is_explicit() {
    let runtime = shared_runtime().await;
    let target = SdkSmokeTarget::new(
        format!("http://{}", runtime.address()),
        "eu-west-2",
    );
    let config = target.load().await;
    let streams = StreamsClient::new(&config);

    let invalid_iterator = streams
        .get_records()
        .shard_iterator("not-base64")
        .send()
        .await
        .expect_err("invalid shard iterator should fail");
    assert_eq!(invalid_iterator.code(), Some("ValidationException"));

    assert!(runtime.state_directory().exists());
}
