use crate::query::QueryParameters;
use crate::s3_bucket_request_parsing::parse_create_bucket_region;
use crate::s3_error_encoding::head_bucket_response;
use crate::s3_object_reads::{
    object_read_precondition_response, parse_object_read_request,
};
use crate::s3_object_responses::{
    current_delete_marker_route_error, head_current_delete_marker_response,
    head_invalid_range_response, head_requested_delete_marker_response,
    head_response, requested_delete_marker_route_error,
};

use super::{
    EdgeRequest, RequestTarget, S3RouteError, normalize_request,
    parse_copy_source, s3_error_response_with_headers, wrong_region_response,
};
use auth::{
    AwsChunkedMode, AwsChunkedSigningContext, VerifiedPayload,
    VerifiedRequest, VerifiedSignature,
};
use aws::{
    AccountId, AdvertisedEdge, Arn, ArnResource, AwsPrincipalType,
    CallerCredentialKind, CallerIdentity, CredentialScope, Partition,
    RegionId, ServiceName, StableAwsPrincipal,
};
use s3::object_reads::eligible_object_range;
use services::{
    HeadBucketOutput, HeadObjectOutput, IfRangeCondition, ObjectRange,
    ObjectReadConditions, ObjectReadMetadata, ObjectReadRequest,
    ObjectReadResponseOverrides, S3Error, S3Scope,
};
use std::collections::BTreeMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[test]
fn normalize_request_decodes_streaming_payload_and_strips_aws_chunked() {
    let mut request = EdgeRequest::new(
        "PUT",
        "/bucket/object.txt",
        vec![
            ("Host".to_owned(), "localhost".to_owned()),
            ("Content-Encoding".to_owned(), "aws-chunked,gzip".to_owned()),
            ("X-Amz-Decoded-Content-Length".to_owned(), "66560".to_owned()),
        ],
        payload_example_body(),
    );
    let verified_signature = streaming_verified_signature(
        AwsChunkedMode::Payload,
        "4f232c4386841ef735655705268965c44a0e4690baa4adea153f7db9fa80a0a9",
    );

    normalize_request(&mut request, Some(&verified_signature))
        .expect("streaming payload should decode");

    assert_eq!(request.body(), vec![b'a'; 66_560].as_slice());
    assert_eq!(request.header("content-encoding"), Some("gzip"));
    assert!(request.header("x-amz-decoded-content-length").is_none());
}

#[test]
fn normalize_request_merges_verified_trailing_headers() {
    let mut request = EdgeRequest::new(
        "PUT",
        "/bucket/object.txt",
        vec![
            ("Host".to_owned(), "localhost".to_owned()),
            ("Content-Encoding".to_owned(), "aws-chunked".to_owned()),
            ("X-Amz-Decoded-Content-Length".to_owned(), "66560".to_owned()),
            ("X-Amz-Trailer".to_owned(), "x-amz-checksum-crc32c".to_owned()),
        ],
        trailer_example_body(),
    );
    let verified_signature = streaming_verified_signature(
        AwsChunkedMode::PayloadTrailer,
        "106e2a8a18243abcf37539882f36619c00e2dfc72633413f02d3b74544bfeb8e",
    );

    normalize_request(&mut request, Some(&verified_signature))
        .expect("streaming trailing headers should decode");

    assert_eq!(request.body(), vec![b'a'; 66_560].as_slice());
    assert!(request.header("content-encoding").is_none());
    assert_eq!(request.header("x-amz-checksum-crc32c"), Some("sOO8/Q=="));
}

#[test]
fn normalize_request_rejects_unsigned_aws_chunked_payloads() {
    let mut request = EdgeRequest::new(
        "PUT",
        "/bucket/object.txt",
        vec![
            ("Host".to_owned(), "localhost".to_owned()),
            ("Content-Encoding".to_owned(), "aws-chunked".to_owned()),
            ("X-Amz-Decoded-Content-Length".to_owned(), "66560".to_owned()),
        ],
        payload_example_body(),
    );

    let error = normalize_request(&mut request, None)
        .expect_err("unsigned aws-chunked payloads must fail");

    assert_eq!(error.code(), "InvalidRequest");
}

#[test]
fn request_target_parse_resolves_path_style_public_host_requests() {
    let request = EdgeRequest::new(
        "GET",
        "/demo/reports/data%20file.txt",
        vec![("Host".to_owned(), "cloudish.test:4566".to_owned())],
        Vec::new(),
    );

    let target = RequestTarget::parse(
        &request,
        &AdvertisedEdge::new("http", "cloudish.test", 4566),
    )
    .expect("path-style request should parse");

    assert_eq!(target.bucket.as_deref(), Some("demo"));
    assert_eq!(target.key.as_deref(), Some("reports/data file.txt"));
}

#[test]
fn request_target_parse_rejects_virtual_host_public_host_requests() {
    let request = EdgeRequest::new(
        "GET",
        "/reports/data%20file.txt",
        vec![("Host".to_owned(), "demo.cloudish.test:4566".to_owned())],
        Vec::new(),
    );

    let error = RequestTarget::parse(
        &request,
        &AdvertisedEdge::new("http", "cloudish.test", 4566),
    )
    .expect_err("virtual-host request should fail");

    assert_eq!(error.code(), "InvalidRequest");
}

#[test]
fn wrong_region_response_includes_region_header_and_xml_error_body() {
    let response = wrong_region_response("eu-west-2");
    let response_text = String::from_utf8(response.to_http_bytes())
        .expect("response should be UTF-8");

    assert!(response_text.starts_with("HTTP/1.1 400 Bad Request"));
    assert!(
        response_text.contains("x-amz-bucket-region: eu-west-2")
            || response_text.contains("X-Amz-Bucket-Region: eu-west-2")
    );
    assert!(response_text.contains("<Code>IncorrectEndpoint</Code>"));
    assert!(response_text.contains("another Region"));
}

#[test]
fn head_bucket_response_includes_bucket_region_header() {
    let response = head_bucket_response(&HeadBucketOutput::Found {
        region: "eu-west-2".to_owned(),
    });
    let response_text = String::from_utf8(response.to_http_bytes())
        .expect("response should be UTF-8");

    assert!(response_text.starts_with("HTTP/1.1 200 OK"));
    assert!(
        response_text.contains("x-amz-bucket-region: eu-west-2")
            || response_text.contains("X-Amz-Bucket-Region: eu-west-2")
    );
}

#[test]
fn head_bucket_error_responses_stay_empty_and_keep_region_header() {
    let response = head_bucket_response(&HeadBucketOutput::Forbidden {
        region: "eu-west-2".to_owned(),
    });
    let response_text = String::from_utf8(response.to_http_bytes())
        .expect("response should be UTF-8");

    assert!(response_text.starts_with("HTTP/1.1 403 Forbidden"));
    assert!(
        response_text.contains("x-amz-bucket-region: eu-west-2")
            || response_text.contains("X-Amz-Bucket-Region: eu-west-2")
    );
    assert!(!response_text.contains("<Error>"));
}

#[test]
fn head_bucket_wrong_region_response_stays_empty_and_uses_redirect_status() {
    let response = head_bucket_response(&HeadBucketOutput::WrongRegion {
        region: "eu-west-2".to_owned(),
    });
    let response_text = String::from_utf8(response.to_http_bytes())
        .expect("response should be UTF-8");

    assert!(response_text.starts_with("HTTP/1.1 301 Moved Permanently"));
    assert!(
        response_text.contains("x-amz-bucket-region: eu-west-2")
            || response_text.contains("X-Amz-Bucket-Region: eu-west-2")
    );
    assert!(!response_text.contains("<Error>"));
}

#[test]
fn head_bucket_missing_response_stays_empty_without_region_header() {
    let response = head_bucket_response(&HeadBucketOutput::Missing);
    let response_text = String::from_utf8(response.to_http_bytes())
        .expect("response should be UTF-8");

    assert!(response_text.starts_with("HTTP/1.1 404 Not Found"));
    assert!(!response_text.contains("x-amz-bucket-region:"));
    assert!(!response_text.contains("<Error>"));
}

#[test]
fn head_delete_marker_responses_stay_empty() {
    let current =
        head_current_delete_marker_response(Some("delete-marker-version"));
    let current_text = String::from_utf8(current.to_http_bytes())
        .expect("response should be UTF-8");

    assert!(current_text.starts_with("HTTP/1.1 404 Not Found"));
    assert!(
        current_text.contains("x-amz-delete-marker: true")
            || current_text.contains("X-Amz-Delete-Marker: true")
    );
    assert!(
        current_text.contains("x-amz-version-id: delete-marker-version")
            || current_text
                .contains("X-Amz-Version-Id: delete-marker-version")
    );
    assert!(!current_text.contains("<Error>"));

    let requested = head_requested_delete_marker_response(1, "v1");
    let requested_text = String::from_utf8(requested.to_http_bytes())
        .expect("response should be UTF-8");

    assert!(requested_text.starts_with("HTTP/1.1 405 Method Not Allowed"));
    assert!(
        requested_text.contains("x-amz-delete-marker: true")
            || requested_text.contains("X-Amz-Delete-Marker: true")
    );
    assert!(
        requested_text.contains("x-amz-version-id: v1")
            || requested_text.contains("X-Amz-Version-Id: v1")
    );
    assert!(!requested_text.contains("<Error>"));
}

#[test]
fn get_delete_marker_responses_keep_rest_xml_error_bodies() {
    let current =
        match current_delete_marker_route_error(Some("delete-marker-version"))
        {
            S3RouteError::AwsWithHeaders { error, headers } => {
                s3_error_response_with_headers(&error, &headers)
            }
            other => panic!("unexpected route error shape: {other:?}"),
        };
    let current_text = String::from_utf8(current.to_http_bytes())
        .expect("response should be UTF-8");

    assert!(current_text.starts_with("HTTP/1.1 404 Not Found"));
    assert!(
        current_text.contains("x-amz-delete-marker: true")
            || current_text.contains("X-Amz-Delete-Marker: true")
    );
    assert!(
        current_text.contains("x-amz-version-id: delete-marker-version")
            || current_text
                .contains("X-Amz-Version-Id: delete-marker-version")
    );
    assert!(
        current_text.contains("Content-Type: application/xml")
            || current_text.contains("content-type: application/xml")
    );
    assert!(current_text.contains("<Code>NoSuchKey</Code>"));

    let requested = match requested_delete_marker_route_error(1, "v1") {
        S3RouteError::AwsWithHeaders { error, headers } => {
            s3_error_response_with_headers(&error, &headers)
        }
        other => panic!("unexpected route error shape: {other:?}"),
    };
    let requested_text = String::from_utf8(requested.to_http_bytes())
        .expect("response should be UTF-8");

    assert!(requested_text.starts_with("HTTP/1.1 405 Method Not Allowed"));
    assert!(
        requested_text.contains("x-amz-delete-marker: true")
            || requested_text.contains("X-Amz-Delete-Marker: true")
    );
    assert!(
        requested_text.contains("x-amz-version-id: v1")
            || requested_text.contains("X-Amz-Version-Id: v1")
    );
    assert!(
        requested_text.contains("Content-Type: application/xml")
            || requested_text.contains("content-type: application/xml")
    );
    assert!(requested_text.contains("<Code>MethodNotAllowed</Code>"));
}

#[test]
fn head_invalid_range_response_stays_empty() {
    let response = head_invalid_range_response(10);
    let response_text = String::from_utf8(response.to_http_bytes())
        .expect("response should be UTF-8");

    assert!(response_text.starts_with("HTTP/1.1 416"));
    assert!(response_text.contains("Content-Length: 0"));
    assert!(response_text.contains("Content-Range: bytes */10"));
    assert!(!response_text.contains("Content-Type:"));
    assert!(!response_text.contains("<Error>"));
}

#[test]
fn global_s3_route_error_conversion_keeps_delete_markers_operation_neutral() {
    let route_error =
        S3RouteError::from(S3Error::CurrentVersionIsDeleteMarker {
            version_id: Some("v1".to_owned()),
        });

    assert!(matches!(route_error, S3RouteError::Aws(_)));
}

#[test]
fn object_read_request_accepts_conditions_and_response_overrides() {
    let request = EdgeRequest::new(
        "GET",
        "/demo/reports/data.txt",
        vec![
            ("If-None-Match".to_owned(), "\"etag-value\"".to_owned()),
            (
                "x-amz-expected-bucket-owner".to_owned(),
                "000000000000".to_owned(),
            ),
            (
                "If-Modified-Since".to_owned(),
                "Wed, 21 Oct 2015 07:28:00 GMT".to_owned(),
            ),
        ],
        Vec::new(),
    );
    let query = QueryParameters::parse(
            b"response-content-type=text%2Fplain&response-content-disposition=attachment",
        )
        .expect("query should parse");

    let parsed = parse_object_read_request(
        &request,
        &query,
        "demo",
        "reports/data.txt",
    )
    .expect("request should parse");

    assert_eq!(parsed.bucket, "demo");
    assert_eq!(
        parsed.conditions.if_none_match.as_deref(),
        Some("\"etag-value\"")
    );
    assert_eq!(parsed.expected_bucket_owner.as_deref(), Some("000000000000"));
    assert!(parsed.conditions.if_modified_since.is_some());
    assert_eq!(parsed.key, "reports/data.txt");
    assert_eq!(
        parsed.response_overrides.content_type.as_deref(),
        Some("text/plain")
    );
    assert_eq!(
        parsed.response_overrides.content_disposition.as_deref(),
        Some("attachment")
    );
    assert_eq!(parsed.version_id, None);
}

#[test]
fn object_read_preconditions_follow_documented_s3_mixed_validator_rules() {
    let object = HeadObjectOutput {
        content_length: 10,
        metadata: ObjectReadMetadata {
            content_type: "text/plain".to_owned(),
            delete_marker: false,
            etag: "\"etag-value\"".to_owned(),
            key: "reports/data.txt".to_owned(),
            last_modified_epoch_seconds: 1_700_000_000,
            metadata: BTreeMap::new(),
            object_lock_legal_hold_status: None,
            object_lock_mode: None,
            object_lock_retain_until_epoch_seconds: None,
            version_id: None,
        },
    };

    let if_match_and_stale_if_unmodified_since = ObjectReadConditions {
        if_match: Some("\"etag-value\"".to_owned()),
        if_modified_since: None,
        if_none_match: None,
        if_unmodified_since: Some(UNIX_EPOCH),
    };
    assert!(
        object_read_precondition_response(
            &object,
            &if_match_and_stale_if_unmodified_since
        )
        .is_none()
    );

    let if_none_match_miss_and_future_if_modified_since =
        ObjectReadConditions {
            if_match: None,
            if_modified_since: Some(
                SystemTime::now() + Duration::from_secs(60),
            ),
            if_none_match: Some("\"different\"".to_owned()),
            if_unmodified_since: None,
        };
    let response = object_read_precondition_response(
        &object,
        &if_none_match_miss_and_future_if_modified_since,
    )
    .expect("s3 mixed validator case should return not modified");

    assert!(
        String::from_utf8(response.to_http_bytes())
            .expect("response should be UTF-8")
            .starts_with("HTTP/1.1 304")
    );
}

#[test]
fn object_read_preconditions_use_weak_match_for_if_none_match_and_strong_match_for_if_match()
 {
    let object = HeadObjectOutput {
        content_length: 10,
        metadata: ObjectReadMetadata {
            content_type: "text/plain".to_owned(),
            delete_marker: false,
            etag: "\"etag-value\"".to_owned(),
            key: "reports/data.txt".to_owned(),
            last_modified_epoch_seconds: 1_700_000_000,
            metadata: BTreeMap::new(),
            object_lock_legal_hold_status: None,
            object_lock_mode: None,
            object_lock_retain_until_epoch_seconds: None,
            version_id: None,
        },
    };

    let weak_if_none_match = ObjectReadConditions {
        if_match: None,
        if_modified_since: None,
        if_none_match: Some("W/\"etag-value\"".to_owned()),
        if_unmodified_since: None,
    };
    let weak_if_match = ObjectReadConditions {
        if_match: Some("W/\"etag-value\"".to_owned()),
        if_modified_since: None,
        if_none_match: None,
        if_unmodified_since: None,
    };

    let not_modified =
        object_read_precondition_response(&object, &weak_if_none_match)
            .expect("weak if-none-match should short-circuit");
    let precondition_failed =
        object_read_precondition_response(&object, &weak_if_match)
            .expect("weak if-match should fail strongly");

    assert!(
        String::from_utf8(not_modified.to_http_bytes())
            .expect("response should be UTF-8")
            .starts_with("HTTP/1.1 304")
    );
    assert!(
        String::from_utf8(precondition_failed.to_http_bytes())
            .expect("response should be UTF-8")
            .starts_with("HTTP/1.1 412")
    );
}

#[test]
fn eligible_object_range_respects_if_range() {
    let object = HeadObjectOutput {
        content_length: 10,
        metadata: ObjectReadMetadata {
            content_type: "text/plain".to_owned(),
            delete_marker: false,
            etag: "\"etag-value\"".to_owned(),
            key: "reports/data.txt".to_owned(),
            last_modified_epoch_seconds: 1_700_000_000,
            metadata: BTreeMap::new(),
            object_lock_legal_hold_status: None,
            object_lock_mode: None,
            object_lock_retain_until_epoch_seconds: None,
            version_id: None,
        },
    };
    let range = ObjectRange::StartEnd { start: 2, end: 5 };
    let matching = ObjectReadRequest {
        bucket: "demo".to_owned(),
        conditions: ObjectReadConditions::default(),
        expected_bucket_owner: None,
        if_range: Some(IfRangeCondition::ETag("\"etag-value\"".to_owned())),
        key: "reports/data.txt".to_owned(),
        range: Some(range.clone()),
        response_overrides: ObjectReadResponseOverrides::default(),
        version_id: None,
    };
    let stale = ObjectReadRequest {
        bucket: "demo".to_owned(),
        conditions: ObjectReadConditions::default(),
        expected_bucket_owner: None,
        if_range: Some(IfRangeCondition::ETag("\"stale\"".to_owned())),
        key: "reports/data.txt".to_owned(),
        range: Some(range),
        response_overrides: ObjectReadResponseOverrides::default(),
        version_id: None,
    };

    assert!(
        eligible_object_range(
            &object.metadata.etag,
            object.metadata.last_modified_epoch_seconds,
            matching.range.as_ref(),
            matching.if_range.as_ref(),
        )
        .is_some()
    );
    assert!(
        eligible_object_range(
            &object.metadata.etag,
            object.metadata.last_modified_epoch_seconds,
            stale.range.as_ref(),
            stale.if_range.as_ref(),
        )
        .is_none()
    );
}

#[test]
fn eligible_object_range_requires_strong_if_range_etag_match() {
    let object = HeadObjectOutput {
        content_length: 10,
        metadata: ObjectReadMetadata {
            content_type: "text/plain".to_owned(),
            delete_marker: false,
            etag: "\"etag-value\"".to_owned(),
            key: "reports/data.txt".to_owned(),
            last_modified_epoch_seconds: 1_700_000_000,
            metadata: BTreeMap::new(),
            object_lock_legal_hold_status: None,
            object_lock_mode: None,
            object_lock_retain_until_epoch_seconds: None,
            version_id: None,
        },
    };
    let weak_if_range = ObjectReadRequest {
        bucket: "demo".to_owned(),
        conditions: ObjectReadConditions::default(),
        expected_bucket_owner: None,
        if_range: Some(IfRangeCondition::ETag("W/\"etag-value\"".to_owned())),
        key: "reports/data.txt".to_owned(),
        range: Some(ObjectRange::StartEnd { start: 2, end: 5 }),
        response_overrides: ObjectReadResponseOverrides::default(),
        version_id: None,
    };

    assert!(
        eligible_object_range(
            &object.metadata.etag,
            object.metadata.last_modified_epoch_seconds,
            weak_if_range.range.as_ref(),
            weak_if_range.if_range.as_ref(),
        )
        .is_none()
    );
}

#[test]
fn head_response_for_a_ranged_object_stays_200_ok_without_content_range() {
    let response = head_response(HeadObjectOutput {
        content_length: 4,
        metadata: ObjectReadMetadata {
            content_type: "application/octet-stream".to_owned(),
            delete_marker: false,
            etag: "\"etag\"".to_owned(),
            key: "reports/data.txt".to_owned(),
            last_modified_epoch_seconds: 1,
            metadata: BTreeMap::new(),
            object_lock_legal_hold_status: None,
            object_lock_mode: None,
            object_lock_retain_until_epoch_seconds: None,
            version_id: Some("v1".to_owned()),
        },
    });
    let response_text = String::from_utf8(response.to_http_bytes())
        .expect("response should be UTF-8");

    assert!(response_text.starts_with("HTTP/1.1 200 OK"));
    assert!(response_text.contains("Content-Length: 4"));
    assert!(!response_text.contains("Content-Range: bytes 2-5/10"));
}

#[test]
fn parse_copy_source_supports_version_id_and_percent_decoding() {
    let request = EdgeRequest::new(
        "PUT",
        "/demo/dst.txt",
        vec![(
            "x-amz-copy-source".to_owned(),
            "/source/reports%2Fdata%20file.txt?versionId=v1%2B2".to_owned(),
        )],
        Vec::new(),
    );

    let (bucket, key, version_id) =
        parse_copy_source(&request).expect("copy source should parse");

    assert_eq!(bucket, "source");
    assert_eq!(key, "reports/data file.txt");
    assert_eq!(version_id.as_deref(), Some("v1+2"));
}

#[test]
fn parse_copy_source_rejects_invalid_query_strings() {
    let unsupported = EdgeRequest::new(
        "PUT",
        "/demo/dst.txt",
        vec![(
            "x-amz-copy-source".to_owned(),
            "/source/key?partNumber=1".to_owned(),
        )],
        Vec::new(),
    );
    let malformed = EdgeRequest::new(
        "PUT",
        "/demo/dst.txt",
        vec![(
            "x-amz-copy-source".to_owned(),
            "/source/key?versionId".to_owned(),
        )],
        Vec::new(),
    );

    assert_eq!(
        parse_copy_source(&unsupported)
            .expect_err("unsupported query should fail")
            .code(),
        "InvalidArgument"
    );
    assert_eq!(
        parse_copy_source(&malformed)
            .expect_err("malformed query should fail")
            .code(),
        "InvalidArgument"
    );
}

#[test]
fn parse_create_bucket_region_enforces_nonempty_constraint_outside_us_east_1()
{
    let east_scope = s3_scope("us-east-1");
    let west_scope = s3_scope("eu-west-2");
    let empty_request =
        EdgeRequest::new("PUT", "/demo", Vec::new(), Vec::new());
    let missing_tag = EdgeRequest::new(
            "PUT",
            "/demo",
            Vec::new(),
            br#"<CreateBucketConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></CreateBucketConfiguration>"#.to_vec(),
        );
    let blank_tag = EdgeRequest::new(
            "PUT",
            "/demo",
            Vec::new(),
            br#"<CreateBucketConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><LocationConstraint>   </LocationConstraint></CreateBucketConfiguration>"#.to_vec(),
        );

    assert_eq!(
        parse_create_bucket_region(&empty_request, &east_scope)
            .expect("us-east-1 empty body should default")
            .as_str(),
        "us-east-1"
    );
    assert_eq!(
        parse_create_bucket_region(&empty_request, &west_scope)
            .expect_err("non-us-east-1 empty body should fail")
            .code(),
        "InvalidLocationConstraint"
    );
    assert_eq!(
        parse_create_bucket_region(&missing_tag, &west_scope)
            .expect_err("missing tag should fail outside us-east-1")
            .code(),
        "InvalidLocationConstraint"
    );
    assert_eq!(
        parse_create_bucket_region(&blank_tag, &west_scope)
            .expect_err("blank tag should fail outside us-east-1")
            .code(),
        "InvalidLocationConstraint"
    );
}

fn s3_scope(region: &str) -> S3Scope {
    S3Scope::new(
        "000000000000".parse::<AccountId>().expect("account should parse"),
        region.parse::<RegionId>().expect("region should parse"),
    )
}

fn streaming_verified_signature(
    mode: AwsChunkedMode,
    seed_signature: &str,
) -> VerifiedSignature {
    let account_id: AccountId =
        "000000000000".parse().expect("account id should parse");
    let caller_identity = CallerIdentity::try_new(
        Arn::trusted_new(
            Partition::aws(),
            ServiceName::Iam,
            None,
            Some(account_id.clone()),
            ArnResource::Generic("root".to_owned()),
        ),
        account_id.as_str(),
    )
    .expect("caller identity should build");
    let verified_request = VerifiedRequest::new(
        account_id,
        CallerCredentialKind::LongTerm(StableAwsPrincipal::new(
            Arn::trusted_new(
                Partition::aws(),
                ServiceName::Iam,
                None,
                Some("000000000000".parse().expect("account id should parse")),
                ArnResource::Generic("root".to_owned()),
            ),
            AwsPrincipalType::Account,
            None,
        )),
        caller_identity,
        CredentialScope::new(
            "eu-west-2".parse().expect("region should parse"),
            ServiceName::S3,
        ),
        None,
    );

    VerifiedSignature::new(
        verified_request,
        VerifiedPayload::AwsChunked(AwsChunkedSigningContext::new(
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            "20130524",
            "us-east-1",
            "s3",
            "20130524T000000Z",
            seed_signature,
            mode,
        )),
    )
}

fn payload_example_body() -> Vec<u8> {
    [
        format!(
            "10000;chunk-signature={}\r\n",
            "ad80c730a21e5b8d04586a2213dd63b9a0e99e0e2307b0ade35a65485a288648"
        )
        .into_bytes(),
        vec![b'a'; 65_536],
        b"\r\n".to_vec(),
        format!(
            "400;chunk-signature={}\r\n",
            "0055627c9e194cb4542bae2aa5492e3c1575bbb81b612b7d234b86a503ef5497"
        )
        .into_bytes(),
        vec![b'a'; 1_024],
        b"\r\n".to_vec(),
        format!(
            "0;chunk-signature={}\r\n\r\n",
            "b6c6ea8a5354eaf15b3cb7646744f4275b71ea724fed81ceb9323e279d449df9"
        )
        .into_bytes(),
    ]
    .concat()
}

fn trailer_example_body() -> Vec<u8> {
    [
            format!(
                "10000;chunk-signature={}\r\n",
                "b474d8862b1487a5145d686f57f013e54db672cee1c953b3010fb58501ef5aa2"
            )
            .into_bytes(),
            vec![b'a'; 65_536],
            b"\r\n".to_vec(),
            format!(
                "400;chunk-signature={}\r\n",
                "1c1344b170168f8e65b41376b44b20fe354e373826ccbbe2c1d40a8cae51e5c7"
            )
            .into_bytes(),
            vec![b'a'; 1_024],
            b"\r\n".to_vec(),
            format!(
                "0;chunk-signature={}\r\n\r\n",
                "2ca2aba2005185cf7159c6277faf83795951dd77a3a99e6e65d5c9f85863f992"
            )
            .into_bytes(),
            b"x-amz-checksum-crc32c:sOO8/Q==\r\nx-amz-trailer-signature:d81f82fc3505edab99d459891051a732e8730629a2e4a59689829ca17fe2e435\r\n\r\n"
                .to_vec(),
        ]
        .concat()
}
