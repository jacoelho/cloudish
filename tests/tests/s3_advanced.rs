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

use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3::config::Builder as S3ConfigBuilder;
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::presigning::{PresignedRequest, PresigningConfig};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    BucketLocationConstraint,
    BucketVersioningStatus as SdkBucketVersioningStatus,
    CreateBucketConfiguration, VersioningConfiguration,
};
use reqwest::Method;
use runtime::RuntimeServer;
use runtime::SharedRuntimeLease;
use sdk::SdkSmokeTarget;

static SHARED_RUNTIME: runtime::SharedRuntime =
    runtime::SharedRuntime::new("s3_advanced");

async fn shared_runtime() -> SharedRuntimeLease<'static> {
    SHARED_RUNTIME.acquire().await
}

use std::time::{Duration, SystemTime};

async fn s3_client(target: &SdkSmokeTarget) -> S3Client {
    let shared = target.load().await;
    let config = S3ConfigBuilder::from(&shared).force_path_style(true).build();
    S3Client::from_conf(config)
}

fn s3_target(runtime: &RuntimeServer) -> SdkSmokeTarget {
    s3_target_in_region(runtime, "eu-west-2")
}

fn s3_target_in_region(
    runtime: &RuntimeServer,
    region: &str,
) -> SdkSmokeTarget {
    SdkSmokeTarget::new(runtime.localhost_endpoint_url(), region)
}

async fn create_bucket(client: &S3Client, bucket: &str) {
    client
        .create_bucket()
        .bucket(bucket)
        .create_bucket_configuration(
            CreateBucketConfiguration::builder()
                .location_constraint(BucketLocationConstraint::EuWest2)
                .build(),
        )
        .send()
        .await
        .expect("bucket should be created");
}

fn assert_target(target: &SdkSmokeTarget, runtime: &RuntimeServer) {
    assert!(runtime.state_directory().exists());
    assert_eq!(target.access_key_id(), "test");
    assert_eq!(target.secret_access_key(), "test");
    assert_eq!(target.region(), "eu-west-2");
    assert_eq!(target.endpoint_url(), runtime.localhost_endpoint_url());
}

async fn execute_presigned(
    presigned: &PresignedRequest,
    body: Option<Vec<u8>>,
) -> reqwest::Result<reqwest::Response> {
    execute_presigned_with_headers(presigned, body, &[]).await
}

async fn execute_presigned_with_headers(
    presigned: &PresignedRequest,
    body: Option<Vec<u8>>,
    headers: &[(&str, &str)],
) -> reqwest::Result<reqwest::Response> {
    let client = reqwest::Client::new();
    let method = Method::from_bytes(presigned.method().as_bytes())
        .expect("pre-signed request method should be valid");
    let mut request = client.request(method, presigned.uri());

    for (name, value) in presigned.headers() {
        request = request.header(name, value);
    }
    for (name, value) in headers {
        request = request.header(*name, *value);
    }
    if let Some(body) = body {
        request = request.body(body);
    }

    request.send().await
}

#[tokio::test]
async fn s3_advanced_versioning_delete_markers_and_historical_reads() {
    let runtime = shared_runtime().await;
    let target = s3_target(&runtime);
    assert_target(&target, &runtime);
    let client = s3_client(&target).await;

    create_bucket(&client, "sdk-s3-advanced-versioning").await;
    client
        .put_bucket_versioning()
        .bucket("sdk-s3-advanced-versioning")
        .versioning_configuration(
            VersioningConfiguration::builder()
                .status(SdkBucketVersioningStatus::Enabled)
                .build(),
        )
        .send()
        .await
        .expect("versioning should enable");

    let put_v1 = client
        .put_object()
        .bucket("sdk-s3-advanced-versioning")
        .key("reports/data.txt")
        .body(ByteStream::from_static(b"version1"))
        .send()
        .await
        .expect("first version should write");
    let put_v2 = client
        .put_object()
        .bucket("sdk-s3-advanced-versioning")
        .key("reports/data.txt")
        .body(ByteStream::from_static(b"version2"))
        .send()
        .await
        .expect("second version should write");
    let delete = client
        .delete_object()
        .bucket("sdk-s3-advanced-versioning")
        .key("reports/data.txt")
        .send()
        .await
        .expect("delete marker should be created");
    let missing = client
        .get_object()
        .bucket("sdk-s3-advanced-versioning")
        .key("reports/data.txt")
        .send()
        .await
        .expect_err("current key should be hidden by delete marker");
    let historical = client
        .get_object()
        .bucket("sdk-s3-advanced-versioning")
        .key("reports/data.txt")
        .version_id(
            put_v1
                .version_id()
                .expect("versioned write should expose a version id"),
        )
        .send()
        .await
        .expect("historical version should remain readable");
    let listed = client
        .list_object_versions()
        .bucket("sdk-s3-advanced-versioning")
        .prefix("reports/")
        .send()
        .await
        .expect("version listing should succeed");
    let historical_version_id = historical.version_id().map(str::to_owned);

    let historical_body = historical
        .body
        .collect()
        .await
        .expect("historical body should stream")
        .into_bytes();

    assert_ne!(put_v1.version_id(), put_v2.version_id());
    assert_eq!(delete.delete_marker(), Some(true));
    assert_eq!(missing.code(), Some("NoSuchKey"));
    assert_eq!(
        historical_version_id.as_deref(),
        put_v1.version_id(),
        "historical GET should return the requested version id"
    );
    assert_eq!(historical_body.as_ref(), b"version1");
    assert_eq!(listed.versions().len(), 2);
    assert_eq!(listed.delete_markers().len(), 1);
    assert_eq!(
        listed.delete_markers()[0].version_id(),
        delete.version_id(),
        "list versions should surface the delete marker id"
    );
}

#[tokio::test]
async fn s3_advanced_presigned_put_get_and_expiry_work() {
    let runtime = shared_runtime().await;
    let target = s3_target(&runtime);
    assert_target(&target, &runtime);
    let client = s3_client(&target).await;

    create_bucket(&client, "sdk-s3-advanced-presign").await;

    let put_presigned = client
        .put_object()
        .bucket("sdk-s3-advanced-presign")
        .key("presigned.txt")
        .presigned(
            PresigningConfig::expires_in(Duration::from_secs(300)).unwrap(),
        )
        .await
        .expect("put presign should build");
    let put_response = execute_presigned(
        &put_presigned,
        Some(b"payload-via-presign".to_vec()),
    )
    .await
    .expect("presigned PUT should succeed");
    let get_signed = client
        .get_object()
        .bucket("sdk-s3-advanced-presign")
        .key("presigned.txt")
        .send()
        .await
        .expect("signed GET should read the pre-signed PUT payload");
    let get_presigned = client
        .get_object()
        .bucket("sdk-s3-advanced-presign")
        .key("presigned.txt")
        .presigned(
            PresigningConfig::expires_in(Duration::from_secs(300)).unwrap(),
        )
        .await
        .expect("get presign should build");
    let get_response = execute_presigned(&get_presigned, None)
        .await
        .expect("presigned GET should succeed");
    let expired_get = client
        .get_object()
        .bucket("sdk-s3-advanced-presign")
        .key("presigned.txt")
        .presigned(
            PresigningConfig::builder()
                .start_time(
                    SystemTime::now()
                        .checked_sub(Duration::from_secs(10))
                        .expect("system time should support subtraction"),
                )
                .expires_in(Duration::from_secs(1))
                .build()
                .expect("expired presign should build"),
        )
        .await
        .expect("expired get presign should build");
    let expired_response = execute_presigned(&expired_get, None)
        .await
        .expect("expired presigned GET should return an S3 error response");
    let put_status = put_response.status();
    let get_status = get_response.status();
    let expired_status = expired_response.status();

    let signed_body = get_signed
        .body
        .collect()
        .await
        .expect("signed GET body should stream")
        .into_bytes();
    let get_body =
        get_response.bytes().await.expect("pre-signed GET body should stream");
    let expired_body = expired_response
        .text()
        .await
        .expect("expired pre-signed GET body should decode");

    assert_eq!(put_status, reqwest::StatusCode::OK);
    assert_eq!(get_status, reqwest::StatusCode::OK);
    assert_eq!(get_body.as_ref(), b"payload-via-presign");
    assert_eq!(signed_body.as_ref(), b"payload-via-presign");
    assert_eq!(expired_status, reqwest::StatusCode::FORBIDDEN);
    assert!(expired_body.contains("<Code>AccessDenied</Code>"));
    assert!(expired_body.contains("<Message>Request has expired.</Message>"));
}

#[tokio::test]
async fn s3_advanced_head_bucket_range_and_delete_marker_responses_are_aws_shaped()
 {
    let runtime = shared_runtime().await;
    let target = s3_target(&runtime);
    assert_target(&target, &runtime);
    let client = s3_client(&target).await;

    create_bucket(&client, "sdk-s3-advanced-read-shapes").await;
    client
        .put_bucket_versioning()
        .bucket("sdk-s3-advanced-read-shapes")
        .versioning_configuration(
            VersioningConfiguration::builder()
                .status(SdkBucketVersioningStatus::Enabled)
                .build(),
        )
        .send()
        .await
        .expect("versioning should enable");
    client
        .head_bucket()
        .bucket("sdk-s3-advanced-read-shapes")
        .send()
        .await
        .expect("head bucket should succeed");
    let expected_owner_mismatch_error = client
        .head_bucket()
        .bucket("sdk-s3-advanced-read-shapes")
        .expected_bucket_owner("111111111111")
        .send()
        .await
        .expect_err("expected-owner mismatch should fail");

    let put = client
        .put_object()
        .bucket("sdk-s3-advanced-read-shapes")
        .key("reports/data.txt")
        .body(ByteStream::from_static(b"abcdefghij"))
        .send()
        .await
        .expect("object should be stored");
    let etag = put.e_tag().expect("put should expose an etag").to_owned();
    let overridden = client
        .get_object()
        .bucket("sdk-s3-advanced-read-shapes")
        .key("reports/data.txt")
        .response_content_type("text/plain")
        .response_content_disposition("attachment")
        .send()
        .await
        .expect("override get should succeed");
    let overridden_content_type = overridden.content_type().map(str::to_owned);
    let overridden_content_disposition =
        overridden.content_disposition().map(str::to_owned);
    let overridden_body = overridden
        .body
        .collect()
        .await
        .expect("override body should stream")
        .into_bytes();
    let expected_owner_get_error = client
        .get_object()
        .bucket("sdk-s3-advanced-read-shapes")
        .key("reports/data.txt")
        .expected_bucket_owner("111111111111")
        .send()
        .await
        .expect_err("object get expected-owner mismatch should fail");
    let overridden_head = client
        .head_object()
        .bucket("sdk-s3-advanced-read-shapes")
        .key("reports/data.txt")
        .expected_bucket_owner("000000000000")
        .response_content_type("text/plain")
        .response_content_disposition("attachment")
        .send()
        .await
        .expect("override head should succeed");
    let overridden_head_content_type =
        overridden_head.content_type().map(str::to_owned);
    let overridden_head_content_disposition =
        overridden_head.content_disposition().map(str::to_owned);
    let expected_owner_head_error = client
        .head_object()
        .bucket("sdk-s3-advanced-read-shapes")
        .key("reports/data.txt")
        .expected_bucket_owner("111111111111")
        .send()
        .await
        .expect_err("object head expected-owner mismatch should fail");
    let ranged = client
        .get_object()
        .bucket("sdk-s3-advanced-read-shapes")
        .key("reports/data.txt")
        .range("bytes=2-5")
        .presigned(
            PresigningConfig::expires_in(Duration::from_secs(300)).unwrap(),
        )
        .await
        .expect("range presign should build");
    let ranged_response = execute_presigned(&ranged, None)
        .await
        .expect("range get should complete");
    let ranged_status = ranged_response.status();
    let ranged_content_range = ranged_response
        .headers()
        .get("content-range")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let ranged_body =
        ranged_response.bytes().await.expect("range body should stream");
    let ranged_head = client
        .head_object()
        .bucket("sdk-s3-advanced-read-shapes")
        .key("reports/data.txt")
        .range("bytes=2-5")
        .presigned(
            PresigningConfig::expires_in(Duration::from_secs(300)).unwrap(),
        )
        .await
        .expect("range head presign should build");
    let ranged_head_response = execute_presigned(&ranged_head, None)
        .await
        .expect("range head should complete");
    let ranged_head_status = ranged_head_response.status();
    let ranged_head_content_range = ranged_head_response
        .headers()
        .get("content-range")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let ranged_head_content_length = ranged_head_response
        .headers()
        .get("content-length")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let invalid_ranged_head = client
        .head_object()
        .bucket("sdk-s3-advanced-read-shapes")
        .key("reports/data.txt")
        .presigned(
            PresigningConfig::expires_in(Duration::from_secs(300)).unwrap(),
        )
        .await
        .expect("invalid range head presign should build");
    let invalid_ranged_head_response = execute_presigned_with_headers(
        &invalid_ranged_head,
        None,
        &[("Range", "bytes=999-")],
    )
    .await
    .expect("invalid range head should complete");
    let invalid_ranged_head_status = invalid_ranged_head_response.status();
    let invalid_ranged_head_content_length = invalid_ranged_head_response
        .headers()
        .get("content-length")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let invalid_ranged_head_content_range = invalid_ranged_head_response
        .headers()
        .get("content-range")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let invalid_ranged_head_content_type = invalid_ranged_head_response
        .headers()
        .get("content-type")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let invalid_ranged_head_body = invalid_ranged_head_response
        .bytes()
        .await
        .expect("invalid range head body should read");
    let conditional_get = client
        .get_object()
        .bucket("sdk-s3-advanced-read-shapes")
        .key("reports/data.txt")
        .presigned(
            PresigningConfig::expires_in(Duration::from_secs(300)).unwrap(),
        )
        .await
        .expect("conditional get presign should build");
    let conditional_get_response = execute_presigned_with_headers(
        &conditional_get,
        None,
        &[("If-None-Match", &etag)],
    )
    .await
    .expect("conditional get should complete");
    let conditional_get_status = conditional_get_response.status();
    let weak_conditional_get = client
        .get_object()
        .bucket("sdk-s3-advanced-read-shapes")
        .key("reports/data.txt")
        .presigned(
            PresigningConfig::expires_in(Duration::from_secs(300)).unwrap(),
        )
        .await
        .expect("weak conditional get presign should build");
    let weak_conditional_get_response = execute_presigned_with_headers(
        &weak_conditional_get,
        None,
        &[("If-None-Match", &format!("W/{etag}"))],
    )
    .await
    .expect("weak conditional get should complete");
    let weak_conditional_get_status = weak_conditional_get_response.status();
    let conditional_invalid_range = client
        .get_object()
        .bucket("sdk-s3-advanced-read-shapes")
        .key("reports/data.txt")
        .presigned(
            PresigningConfig::expires_in(Duration::from_secs(300)).unwrap(),
        )
        .await
        .expect("conditional invalid range presign should build");
    let conditional_invalid_range_response = execute_presigned_with_headers(
        &conditional_invalid_range,
        None,
        &[("If-None-Match", &etag), ("Range", "bytes=999-")],
    )
    .await
    .expect("conditional invalid range should complete");
    let conditional_invalid_range_status =
        conditional_invalid_range_response.status();
    let conditional_head = client
        .head_object()
        .bucket("sdk-s3-advanced-read-shapes")
        .key("reports/data.txt")
        .presigned(
            PresigningConfig::expires_in(Duration::from_secs(300)).unwrap(),
        )
        .await
        .expect("conditional head presign should build");
    let conditional_head_response = execute_presigned_with_headers(
        &conditional_head,
        None,
        &[("If-Match", "\"different-etag\"")],
    )
    .await
    .expect("conditional head should complete");
    let conditional_head_status = conditional_head_response.status();
    let mixed_if_match_get = client
        .get_object()
        .bucket("sdk-s3-advanced-read-shapes")
        .key("reports/data.txt")
        .presigned(
            PresigningConfig::expires_in(Duration::from_secs(300)).unwrap(),
        )
        .await
        .expect("mixed if-match get presign should build");
    let mixed_if_match_get_response = execute_presigned_with_headers(
        &mixed_if_match_get,
        None,
        &[
            ("If-Match", &etag),
            ("If-Unmodified-Since", "Wed, 21 Oct 2015 07:28:00 GMT"),
        ],
    )
    .await
    .expect("mixed if-match get should complete");
    let mixed_if_match_get_status = mixed_if_match_get_response.status();
    let mixed_if_match_get_body = mixed_if_match_get_response
        .bytes()
        .await
        .expect("mixed if-match get body should stream");
    let mixed_if_none_match_get = client
        .get_object()
        .bucket("sdk-s3-advanced-read-shapes")
        .key("reports/data.txt")
        .presigned(
            PresigningConfig::expires_in(Duration::from_secs(300)).unwrap(),
        )
        .await
        .expect("mixed if-none-match get presign should build");
    let mixed_if_none_match_get_response = execute_presigned_with_headers(
        &mixed_if_none_match_get,
        None,
        &[
            ("If-None-Match", "\"different-etag\""),
            ("If-Modified-Since", "Wed, 21 Oct 2099 07:28:00 GMT"),
        ],
    )
    .await
    .expect("mixed if-none-match get should complete");
    let mixed_if_none_match_get_status =
        mixed_if_none_match_get_response.status();
    let if_range_get = client
        .get_object()
        .bucket("sdk-s3-advanced-read-shapes")
        .key("reports/data.txt")
        .presigned(
            PresigningConfig::expires_in(Duration::from_secs(300)).unwrap(),
        )
        .await
        .expect("if-range get presign should build");
    let if_range_response = execute_presigned_with_headers(
        &if_range_get,
        None,
        &[("Range", "bytes=2-5"), ("If-Range", "\"stale-etag\"")],
    )
    .await
    .expect("if-range get should complete");
    let if_range_status = if_range_response.status();
    let if_range_content_range = if_range_response
        .headers()
        .get("content-range")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let if_range_body =
        if_range_response.bytes().await.expect("if-range body should stream");

    let delete = client
        .delete_object()
        .bucket("sdk-s3-advanced-read-shapes")
        .key("reports/data.txt")
        .send()
        .await
        .expect("delete marker should be created");
    let current_get = client
        .get_object()
        .bucket("sdk-s3-advanced-read-shapes")
        .key("reports/data.txt")
        .presigned(
            PresigningConfig::expires_in(Duration::from_secs(300)).unwrap(),
        )
        .await
        .expect("current delete marker get presign should build");
    let current_get_response = execute_presigned(&current_get, None)
        .await
        .expect("current delete marker get should complete");
    let current_get_status = current_get_response.status();
    let current_get_delete_marker = current_get_response
        .headers()
        .get("x-amz-delete-marker")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let current_get_content_length = current_get_response
        .headers()
        .get("content-length")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let current_get_content_type = current_get_response
        .headers()
        .get("content-type")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let current_get_body = current_get_response
        .bytes()
        .await
        .expect("current delete marker body should read");
    let current_head = client
        .head_object()
        .bucket("sdk-s3-advanced-read-shapes")
        .key("reports/data.txt")
        .presigned(
            PresigningConfig::expires_in(Duration::from_secs(300)).unwrap(),
        )
        .await
        .expect("current delete marker head presign should build");
    let current_head_response = execute_presigned(&current_head, None)
        .await
        .expect("current delete marker head should complete");
    let current_head_status = current_head_response.status();
    let current_head_delete_marker = current_head_response
        .headers()
        .get("x-amz-delete-marker")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let current_head_content_length = current_head_response
        .headers()
        .get("content-length")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let versioned_get = client
        .get_object()
        .bucket("sdk-s3-advanced-read-shapes")
        .key("reports/data.txt")
        .version_id(
            delete
                .version_id()
                .expect("delete marker should expose a version id"),
        )
        .presigned(
            PresigningConfig::expires_in(Duration::from_secs(300)).unwrap(),
        )
        .await
        .expect("versioned delete marker get presign should build");
    let versioned_get_response = execute_presigned(&versioned_get, None)
        .await
        .expect("versioned delete marker get should complete");
    let versioned_get_status = versioned_get_response.status();
    let versioned_get_delete_marker = versioned_get_response
        .headers()
        .get("x-amz-delete-marker")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let versioned_get_content_length = versioned_get_response
        .headers()
        .get("content-length")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let versioned_get_content_type = versioned_get_response
        .headers()
        .get("content-type")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let versioned_get_body = versioned_get_response
        .bytes()
        .await
        .expect("versioned delete marker body should read");
    let versioned_head = client
        .head_object()
        .bucket("sdk-s3-advanced-read-shapes")
        .key("reports/data.txt")
        .version_id(
            delete
                .version_id()
                .expect("delete marker should expose a version id"),
        )
        .presigned(
            PresigningConfig::expires_in(Duration::from_secs(300)).unwrap(),
        )
        .await
        .expect("versioned delete marker head presign should build");
    let versioned_head_response = execute_presigned(&versioned_head, None)
        .await
        .expect("versioned delete marker head should complete");
    let versioned_head_status = versioned_head_response.status();
    let versioned_head_delete_marker = versioned_head_response
        .headers()
        .get("x-amz-delete-marker")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let versioned_head_content_length = versioned_head_response
        .headers()
        .get("content-length")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);

    assert_eq!(ranged_status, reqwest::StatusCode::PARTIAL_CONTENT);
    assert_eq!(overridden_content_type.as_deref(), Some("text/plain"));
    assert_eq!(overridden_content_disposition.as_deref(), Some("attachment"));
    assert_eq!(overridden_body.as_ref(), b"abcdefghij");
    assert_eq!(overridden_head_content_type.as_deref(), Some("text/plain"));
    assert_eq!(
        overridden_head_content_disposition.as_deref(),
        Some("attachment")
    );
    assert_eq!(ranged_content_range.as_deref(), Some("bytes 2-5/10"));
    assert_eq!(ranged_body.as_ref(), b"cdef");
    assert_eq!(ranged_head_status, reqwest::StatusCode::OK);
    assert_eq!(ranged_head_content_length.as_deref(), Some("4"));
    assert_eq!(ranged_head_content_range, None);
    assert_eq!(
        invalid_ranged_head_status,
        reqwest::StatusCode::RANGE_NOT_SATISFIABLE
    );
    assert_eq!(invalid_ranged_head_content_length.as_deref(), Some("0"));
    assert_eq!(
        invalid_ranged_head_content_range.as_deref(),
        Some("bytes */10")
    );
    assert_eq!(invalid_ranged_head_content_type, None);
    assert!(invalid_ranged_head_body.is_empty());
    assert_eq!(conditional_get_status, reqwest::StatusCode::NOT_MODIFIED);
    assert_eq!(weak_conditional_get_status, reqwest::StatusCode::NOT_MODIFIED);
    assert_eq!(
        conditional_invalid_range_status,
        reqwest::StatusCode::NOT_MODIFIED
    );
    assert_eq!(
        conditional_head_status,
        reqwest::StatusCode::PRECONDITION_FAILED
    );
    assert_eq!(mixed_if_match_get_status, reqwest::StatusCode::OK);
    assert_eq!(mixed_if_match_get_body.as_ref(), b"abcdefghij");
    assert_eq!(
        mixed_if_none_match_get_status,
        reqwest::StatusCode::NOT_MODIFIED
    );
    assert_eq!(if_range_status, reqwest::StatusCode::OK);
    assert_eq!(if_range_content_range, None);
    assert_eq!(if_range_body.as_ref(), b"abcdefghij");
    assert_eq!(current_get_status, reqwest::StatusCode::NOT_FOUND);
    assert_eq!(current_get_delete_marker.as_deref(), Some("true"));
    assert_eq!(current_get_content_type.as_deref(), Some("application/xml"));
    assert!(
        std::str::from_utf8(current_get_body.as_ref())
            .expect("current delete marker body should be UTF-8")
            .contains("<Code>NoSuchKey</Code>")
    );
    assert_ne!(current_get_content_length.as_deref(), Some("0"));
    assert_eq!(current_head_status, reqwest::StatusCode::NOT_FOUND);
    assert_eq!(current_head_delete_marker.as_deref(), Some("true"));
    assert_eq!(current_head_content_length.as_deref(), Some("0"));
    assert_eq!(versioned_get_status, reqwest::StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(versioned_get_delete_marker.as_deref(), Some("true"));
    assert_eq!(versioned_get_content_type.as_deref(), Some("application/xml"));
    assert!(
        std::str::from_utf8(versioned_get_body.as_ref())
            .expect("versioned delete marker body should be UTF-8")
            .contains("<Code>MethodNotAllowed</Code>")
    );
    assert_ne!(versioned_get_content_length.as_deref(), Some("0"));
    assert_eq!(versioned_head_status, reqwest::StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(versioned_head_delete_marker.as_deref(), Some("true"));
    assert_eq!(versioned_head_content_length.as_deref(), Some("0"));

    let wrong_region_client =
        s3_client(&s3_target_in_region(&runtime, "us-east-1")).await;
    let wrong_region_error = wrong_region_client
        .head_bucket()
        .bucket("sdk-s3-advanced-read-shapes")
        .send()
        .await
        .expect_err("wrong-region head bucket should fail");

    assert_eq!(expected_owner_mismatch_error.code(), None);
    assert_eq!(expected_owner_get_error.code(), Some("AccessDenied"));
    assert_eq!(expected_owner_head_error.code(), None);
    assert_eq!(wrong_region_error.code(), None);
}

#[tokio::test]
async fn s3_advanced_list_object_version_markers_fail_explicitly() {
    let runtime = shared_runtime().await;
    let target = s3_target(&runtime);
    let client = s3_client(&target).await;

    create_bucket(&client, "sdk-s3-advanced-version-markers").await;

    let error = client
        .list_object_versions()
        .bucket("sdk-s3-advanced-version-markers")
        .key_marker("reports/data.txt")
        .version_id_marker("00000000000000000001")
        .send()
        .await
        .expect_err("unsupported version markers should fail explicitly");

    assert_eq!(error.code(), Some("NotImplemented"));
}

#[tokio::test]
async fn s3_advanced_multipart_abort_cleanup_and_validation_failures() {
    let runtime = shared_runtime().await;
    let target = s3_target(&runtime);
    assert_target(&target, &runtime);
    let client = s3_client(&target).await;

    create_bucket(&client, "sdk-s3-advanced-multipart").await;

    let aborted = client
        .create_multipart_upload()
        .bucket("sdk-s3-advanced-multipart")
        .key("aborted.bin")
        .send()
        .await
        .expect("aborted upload should start");
    let aborted_upload_id = aborted
        .upload_id()
        .expect("multipart upload should expose an upload id")
        .to_owned();
    client
        .upload_part()
        .bucket("sdk-s3-advanced-multipart")
        .key("aborted.bin")
        .upload_id(&aborted_upload_id)
        .part_number(1)
        .body(ByteStream::from_static(b"parts"))
        .send()
        .await
        .expect("multipart part should upload");
    client
        .abort_multipart_upload()
        .bucket("sdk-s3-advanced-multipart")
        .key("aborted.bin")
        .upload_id(&aborted_upload_id)
        .send()
        .await
        .expect("multipart upload should abort");
    let uploads_after_abort = client
        .list_multipart_uploads()
        .bucket("sdk-s3-advanced-multipart")
        .send()
        .await
        .expect("multipart listing should succeed");
    let missing_object = client
        .get_object()
        .bucket("sdk-s3-advanced-multipart")
        .key("aborted.bin")
        .send()
        .await
        .expect_err("aborted upload should not create an object");

    let invalid = client
        .create_multipart_upload()
        .bucket("sdk-s3-advanced-multipart")
        .key("invalid.bin")
        .send()
        .await
        .expect("invalid upload should start");
    let invalid_upload_id = invalid
        .upload_id()
        .expect("multipart upload should expose an upload id")
        .to_owned();
    client
        .upload_part()
        .bucket("sdk-s3-advanced-multipart")
        .key("invalid.bin")
        .upload_id(&invalid_upload_id)
        .part_number(1)
        .body(ByteStream::from_static(b"part"))
        .send()
        .await
        .expect("invalid multipart part should upload");
    let malformed_body = "<CompleteMultipartUpload></CompleteMultipartUpload>";
    let malformed_response = reqwest::Client::new()
        .post(format!(
            "{}/sdk-s3-advanced-multipart/invalid.bin?uploadId={invalid_upload_id}",
            runtime.localhost_endpoint_url()
        ))
        .header("content-type", "application/xml")
        .body(malformed_body.to_owned())
        .send()
        .await
        .expect("malformed completion should return a shaped response");
    let malformed_status = malformed_response.status();
    let malformed_body = malformed_response
        .text()
        .await
        .expect("malformed completion body should be UTF-8");

    assert_eq!(uploads_after_abort.uploads().len(), 0);
    assert_eq!(missing_object.code(), Some("NoSuchKey"));
    assert_eq!(malformed_status, reqwest::StatusCode::BAD_REQUEST);
    assert!(malformed_body.contains("<Code>MalformedXML</Code>"));
}
