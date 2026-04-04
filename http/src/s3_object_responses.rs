use crate::aws_error_shape::AwsErrorShape;
use crate::runtime::EdgeResponse;
use crate::s3::S3RouteError;
use crate::s3_response_encoding::apply_object_lock_headers;
use services::{
    GetObjectOutput, HeadObjectOutput, ObjectReadMetadata, S3Error,
};
use std::time::{Duration, UNIX_EPOCH};

pub(crate) fn head_response(object: HeadObjectOutput) -> EdgeResponse {
    let HeadObjectOutput { content_length, metadata } = object;
    let ObjectReadMetadata {
        content_type,
        etag,
        last_modified_epoch_seconds,
        metadata,
        object_lock_legal_hold_status,
        object_lock_mode,
        object_lock_retain_until_epoch_seconds,
        version_id,
        ..
    } = metadata;
    let mut response = EdgeResponse::empty(200)
        .set_header("Content-Type", content_type)
        .set_header("Content-Length", content_length.to_string())
        .set_header("ETag", etag)
        .set_header(
            "Last-Modified",
            http_timestamp(last_modified_epoch_seconds),
        );

    if let Some(version_id) = version_id {
        response = response.set_header("x-amz-version-id", version_id);
    }

    for (name, value) in metadata {
        response = response.set_header(format!("x-amz-meta-{name}"), value);
    }

    apply_object_lock_headers(
        response,
        object_lock_mode,
        object_lock_retain_until_epoch_seconds,
        object_lock_legal_hold_status,
    )
}

pub(crate) fn object_response(object: GetObjectOutput) -> EdgeResponse {
    let GetObjectOutput {
        body,
        content_length,
        content_range,
        is_partial,
        metadata,
    } = object;
    let ObjectReadMetadata {
        content_type,
        etag,
        last_modified_epoch_seconds,
        metadata,
        object_lock_legal_hold_status,
        object_lock_mode,
        object_lock_retain_until_epoch_seconds,
        version_id,
        ..
    } = metadata;
    let mut response = EdgeResponse::bytes(
        if is_partial { 206 } else { 200 },
        &content_type,
        body,
    )
    .set_header("Content-Length", content_length.to_string())
    .set_header("ETag", etag)
    .set_header("Last-Modified", http_timestamp(last_modified_epoch_seconds));
    if let Some(content_range) = content_range {
        response = response.set_header("Content-Range", content_range);
    }

    if let Some(version_id) = version_id {
        response = response.set_header("x-amz-version-id", version_id);
    }

    for (name, value) in metadata {
        response = response.set_header(format!("x-amz-meta-{name}"), value);
    }

    apply_object_lock_headers(
        response,
        object_lock_mode,
        object_lock_retain_until_epoch_seconds,
        object_lock_legal_hold_status,
    )
}

pub(crate) fn current_delete_marker_route_error(
    version_id: Option<&str>,
) -> S3RouteError {
    let mut headers =
        vec![("x-amz-delete-marker".to_owned(), "true".to_owned())];
    if let Some(version_id) = version_id {
        headers.push(("x-amz-version-id".to_owned(), version_id.to_owned()));
    }

    S3RouteError::AwsWithHeaders {
        error: S3Error::CurrentVersionIsDeleteMarker {
            version_id: version_id.map(str::to_owned),
        }
        .to_aws_error(),
        headers,
    }
}

pub(crate) fn head_current_delete_marker_response(
    version_id: Option<&str>,
) -> EdgeResponse {
    let mut response =
        EdgeResponse::empty(404).set_header("x-amz-delete-marker", "true");
    if let Some(version_id) = version_id {
        response = response.set_header("x-amz-version-id", version_id);
    }
    response
}

pub(crate) fn requested_delete_marker_route_error(
    last_modified_epoch_seconds: u64,
    version_id: &str,
) -> S3RouteError {
    S3RouteError::AwsWithHeaders {
        error: S3Error::RequestedVersionIsDeleteMarker {
            last_modified_epoch_seconds,
            version_id: version_id.to_owned(),
        }
        .to_aws_error(),
        headers: vec![
            (
                "Last-Modified".to_owned(),
                http_timestamp(last_modified_epoch_seconds),
            ),
            ("x-amz-delete-marker".to_owned(), "true".to_owned()),
            ("x-amz-version-id".to_owned(), version_id.to_owned()),
        ],
    }
}

pub(crate) fn head_requested_delete_marker_response(
    last_modified_epoch_seconds: u64,
    version_id: &str,
) -> EdgeResponse {
    EdgeResponse::empty(405)
        .set_header(
            "Last-Modified",
            http_timestamp(last_modified_epoch_seconds),
        )
        .set_header("x-amz-delete-marker", "true")
        .set_header("x-amz-version-id", version_id)
}

pub(crate) fn head_invalid_range_response(size: u64) -> EdgeResponse {
    EdgeResponse::empty(416)
        .set_header("Accept-Ranges", "bytes")
        .set_header("Content-Range", format!("bytes */{size}"))
}

fn http_timestamp(epoch_seconds: u64) -> String {
    let timestamp = UNIX_EPOCH
        .checked_add(Duration::from_secs(epoch_seconds))
        .unwrap_or(UNIX_EPOCH);
    httpdate::fmt_http_date(timestamp)
}
