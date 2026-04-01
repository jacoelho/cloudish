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
    SdkSmokeTarget::new(runtime.localhost_endpoint_url(), "eu-west-2")
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
    let client = reqwest::Client::new();
    let method = Method::from_bytes(presigned.method().as_bytes())
        .expect("pre-signed request method should be valid");
    let mut request = client.request(method, presigned.uri());

    for (name, value) in presigned.headers() {
        request = request.header(name, value);
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
