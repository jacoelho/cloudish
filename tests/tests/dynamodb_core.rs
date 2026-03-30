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

use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::error::ProvideErrorMetadata;
use aws_sdk_dynamodb::types::{
    AttributeDefinition, AttributeValue, BillingMode, KeySchemaElement,
    KeyType, ProvisionedThroughput, ReturnValue, ScalarAttributeType,
};
use runtime::SharedRuntimeLease;
use sdk::SdkSmokeTarget;

static SHARED_RUNTIME: runtime::SharedRuntime =
    runtime::SharedRuntime::new("dynamodb_core");

async fn shared_runtime() -> SharedRuntimeLease<'static> {
    SHARED_RUNTIME.acquire().await
}

use std::collections::HashMap;

fn string(value: &str) -> AttributeValue {
    AttributeValue::S(value.to_owned())
}

fn number(value: &str) -> AttributeValue {
    AttributeValue::N(value.to_owned())
}

fn order_item(
    tenant: &str,
    order: &str,
    status: &str,
    total: &str,
) -> HashMap<String, AttributeValue> {
    HashMap::from([
        ("tenant".to_owned(), string(tenant)),
        ("order".to_owned(), string(order)),
        ("status".to_owned(), string(status)),
        ("total".to_owned(), number(total)),
    ])
}

async fn create_orders_table(client: &Client) {
    client
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
}

#[tokio::test]
async fn dynamodb_core_table_item_and_access_paths_round_trip() {
    let runtime = shared_runtime().await;
    let target = SdkSmokeTarget::new(
        format!("http://{}", runtime.address()),
        "eu-west-2",
    );
    let config = target.load().await;
    let client = Client::new(&config);

    create_orders_table(&client).await;

    for (tenant, order, status, total) in [
        ("tenant-a", "001", "pending", "10"),
        ("tenant-a", "002", "shipped", "20"),
        ("tenant-b", "001", "pending", "30"),
    ] {
        client
            .put_item()
            .table_name("orders")
            .set_item(Some(order_item(tenant, order, status, total)))
            .send()
            .await
            .expect("item should be written");
    }

    let fetched = client
        .get_item()
        .table_name("orders")
        .key("tenant", string("tenant-a"))
        .key("order", string("001"))
        .send()
        .await
        .expect("item should be fetched");
    let fetched = fetched.item().expect("item should exist");
    assert_eq!(fetched.get("status"), Some(&string("pending")));

    let updated = client
        .update_item()
        .table_name("orders")
        .key("tenant", string("tenant-a"))
        .key("order", string("001"))
        .update_expression("SET total = :next, note = :note")
        .expression_attribute_values(":next", number("11"))
        .expression_attribute_values(":note", string("priority"))
        .return_values(ReturnValue::AllNew)
        .send()
        .await
        .expect("item should update");
    let updated =
        updated.attributes().expect("updated attributes should exist");
    assert_eq!(updated.get("total"), Some(&number("11")));
    assert_eq!(updated.get("note"), Some(&string("priority")));

    let queried = client
        .query()
        .table_name("orders")
        .key_condition_expression(
            "tenant = :tenant AND begins_with(#order, :prefix)",
        )
        .expression_attribute_names("#order", "order")
        .expression_attribute_values(":tenant", string("tenant-a"))
        .expression_attribute_values(":prefix", string("00"))
        .send()
        .await
        .expect("query should succeed");
    assert_eq!(queried.count(), 2);
    assert_eq!(queried.scanned_count(), 2);
    assert_eq!(queried.items().len(), 2);

    let scanned = client
        .scan()
        .table_name("orders")
        .filter_expression("total BETWEEN :low AND :high")
        .expression_attribute_values(":low", number("15"))
        .expression_attribute_values(":high", number("40"))
        .limit(2)
        .send()
        .await
        .expect("scan should succeed");
    assert_eq!(scanned.count(), 1);
    assert_eq!(scanned.scanned_count(), 2);
    assert!(scanned.last_evaluated_key().is_some());

    let updated_table = client
        .update_table()
        .table_name("orders")
        .billing_mode(BillingMode::Provisioned)
        .provisioned_throughput(
            ProvisionedThroughput::builder()
                .read_capacity_units(4)
                .write_capacity_units(8)
                .build()
                .expect("throughput should build"),
        )
        .send()
        .await
        .expect("table should update");
    let table = updated_table
        .table_description()
        .expect("table description should be returned");
    assert_eq!(
        table
            .billing_mode_summary()
            .and_then(|summary| summary.billing_mode()),
        Some(&BillingMode::Provisioned)
    );
    let throughput = table
        .provisioned_throughput()
        .expect("provisioned throughput should be returned");
    assert_eq!(throughput.read_capacity_units(), Some(4));
    assert_eq!(throughput.write_capacity_units(), Some(8));

    assert!(runtime.state_directory().exists());
}

#[tokio::test]
async fn dynamodb_core_duplicate_table_and_conditional_put_surface_explicit_errors()
 {
    let runtime = shared_runtime().await;
    let target = SdkSmokeTarget::new(
        format!("http://{}", runtime.address()),
        "eu-west-2",
    );
    let config = target.load().await;
    let client = Client::new(&config);

    create_orders_table(&client).await;

    let duplicate = client
        .create_table()
        .table_name("orders")
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("tenant")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .expect("tenant attribute definition should build"),
        )
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("tenant")
                .key_type(KeyType::Hash)
                .build()
                .expect("partition key should build"),
        )
        .billing_mode(BillingMode::PayPerRequest)
        .send()
        .await
        .expect_err("duplicate table should fail");
    assert_eq!(duplicate.code(), Some("ResourceInUseException"));

    client
        .put_item()
        .table_name("orders")
        .set_item(Some(order_item("tenant-a", "001", "pending", "10")))
        .send()
        .await
        .expect("seed item should be written");

    let conditional = client
        .put_item()
        .table_name("orders")
        .set_item(Some(order_item("tenant-a", "001", "updated", "12")))
        .condition_expression("attribute_not_exists(tenant)")
        .send()
        .await
        .expect_err("conditional put should fail");
    assert_eq!(conditional.code(), Some("ConditionalCheckFailedException"));

    let stored = client
        .get_item()
        .table_name("orders")
        .key("tenant", string("tenant-a"))
        .key("order", string("001"))
        .send()
        .await
        .expect("item should still be readable");
    let stored = stored.item().expect("seed item should remain");
    assert_eq!(stored.get("status"), Some(&string("pending")));
    assert_eq!(stored.get("total"), Some(&number("10")));

    assert!(runtime.state_directory().exists());
}
