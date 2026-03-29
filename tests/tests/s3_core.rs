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

use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3::config::Builder as S3ConfigBuilder;
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::primitives::ByteStream;
use runtime::SharedRuntimeLease;
use sdk::SdkSmokeTarget;

static SHARED_RUNTIME: runtime::SharedRuntime =
    runtime::SharedRuntime::new("s3_core");

async fn shared_runtime() -> SharedRuntimeLease<'static> {
    SHARED_RUNTIME.acquire().await
}

async fn s3_client(target: &SdkSmokeTarget) -> S3Client {
    let shared = target.load().await;
    let config = S3ConfigBuilder::from(&shared).force_path_style(true).build();
    S3Client::from_conf(config)
}

#[tokio::test]
async fn s3_core_put_get_head_copy_and_list_objects() {
    let runtime = shared_runtime().await;
    assert!(runtime.state_directory().exists());
    let target = SdkSmokeTarget::new(
        format!("http://{}", runtime.address()),
        "us-east-1",
    );
    assert_eq!(target.access_key_id(), "test");
    assert_eq!(target.secret_access_key(), "test");
    assert_eq!(target.region(), "us-east-1");
    assert_eq!(target.endpoint_url(), format!("http://{}", runtime.address()));
    let client = s3_client(&target).await;

    client
        .create_bucket()
        .bucket("sdk-s3-core-flow")
        .send()
        .await
        .expect("bucket should be created");

    let put = client
        .put_object()
        .bucket("sdk-s3-core-flow")
        .key("reports/data.txt")
        .content_type("application/x-amz-json-1.0")
        .metadata("trace", "abc123")
        .body(ByteStream::from_static(b"payload"))
        .send()
        .await
        .expect("object should be written");
    let get = client
        .get_object()
        .bucket("sdk-s3-core-flow")
        .key("reports/data.txt")
        .send()
        .await
        .expect("object should be readable");
    let head = client
        .head_object()
        .bucket("sdk-s3-core-flow")
        .key("reports/data.txt")
        .send()
        .await
        .expect("head should succeed");
    let copied = client
        .copy_object()
        .bucket("sdk-s3-core-flow")
        .key("reports/data-copy.txt")
        .copy_source("sdk-s3-core-flow/reports/data.txt")
        .send()
        .await
        .expect("copy should succeed");
    let copied_head = client
        .head_object()
        .bucket("sdk-s3-core-flow")
        .key("reports/data-copy.txt")
        .send()
        .await
        .expect("copied object should be readable");
    let listed = client
        .list_objects_v2()
        .bucket("sdk-s3-core-flow")
        .prefix("reports/")
        .send()
        .await
        .expect("list should succeed");
    let get_content_type = get.content_type().map(str::to_owned);

    let body =
        get.body.collect().await.expect("get body should stream").into_bytes();

    assert_eq!(put.e_tag(), head.e_tag());
    assert_eq!(
        head.e_tag(),
        copied.copy_object_result().and_then(|result| result.e_tag())
    );
    assert_eq!(head.e_tag(), copied_head.e_tag());
    assert_eq!(head.content_length(), Some(7));
    assert_eq!(
        get_content_type.as_deref(),
        Some("application/x-amz-json-1.0")
    );
    assert_eq!(head.content_type(), Some("application/x-amz-json-1.0"));
    assert_eq!(
        head.metadata().and_then(|metadata| metadata.get("trace")),
        Some(&"abc123".to_owned())
    );
    assert_eq!(body.as_ref(), b"payload");
    assert_eq!(
        listed
            .contents()
            .iter()
            .filter_map(|object| object.key())
            .collect::<Vec<_>>(),
        vec!["reports/data-copy.txt", "reports/data.txt"]
    );
}

#[tokio::test]
async fn s3_core_missing_key_and_non_empty_bucket_errors_are_shaped() {
    let runtime = shared_runtime().await;
    assert!(runtime.state_directory().exists());
    let target = SdkSmokeTarget::new(
        format!("http://{}", runtime.address()),
        "us-east-1",
    );
    assert_eq!(target.access_key_id(), "test");
    assert_eq!(target.secret_access_key(), "test");
    assert_eq!(target.region(), "us-east-1");
    assert_eq!(target.endpoint_url(), format!("http://{}", runtime.address()));
    let client = s3_client(&target).await;

    client
        .create_bucket()
        .bucket("sdk-s3-core-errors")
        .send()
        .await
        .expect("bucket should be created");
    client
        .put_object()
        .bucket("sdk-s3-core-errors")
        .key("hello.txt")
        .body(ByteStream::from_static(b"body"))
        .send()
        .await
        .expect("object should be written");

    let missing = client
        .get_object()
        .bucket("sdk-s3-core-errors")
        .key("missing.txt")
        .send()
        .await
        .expect_err("missing object should fail");
    let non_empty = client
        .delete_bucket()
        .bucket("sdk-s3-core-errors")
        .send()
        .await
        .expect_err("non-empty bucket delete should fail");

    assert_eq!(missing.code(), Some("NoSuchKey"));
    assert_eq!(non_empty.code(), Some("BucketNotEmpty"));
}
