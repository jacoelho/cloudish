use crate::query::QueryParameters;
use crate::request::HttpRequest;
use crate::runtime::EdgeResponse;
use crate::s3_query_parsing::optional_query;
use crate::s3_request_parsing::not_implemented_message;
use aws::{AwsError, AwsErrorFamily};
use s3::ObjectRange;
use s3::object_reads::evaluate_object_read_preconditions;
use services::{
    GetObjectOutput, HeadObjectOutput, IfRangeCondition, ObjectReadConditions,
    ObjectReadPreconditionOutcome, ObjectReadRequest,
    ObjectReadResponseOverrides,
};
use std::time::{Duration, UNIX_EPOCH};

pub(crate) fn parse_object_read_request(
    request: &HttpRequest<'_>,
    query: &QueryParameters,
    bucket: &str,
    key: &str,
) -> Result<ObjectReadRequest, AwsError> {
    reject_unsupported_query_parameter(
        query,
        "partNumber",
        "GetObject partNumber reads are not implemented.",
    )?;
    reject_unsupported_header(
        request,
        "x-amz-checksum-mode",
        "Checksum-mode object reads are not implemented.",
    )?;
    reject_unsupported_header(
        request,
        "x-amz-request-payer",
        "RequestPayer handling is not implemented.",
    )?;
    let range = request.header("range").map(parse_range_header).transpose()?;

    Ok(ObjectReadRequest {
        bucket: bucket.to_owned(),
        conditions: parse_object_read_conditions(request),
        expected_bucket_owner: request
            .header("x-amz-expected-bucket-owner")
            .map(str::to_owned),
        if_range: parse_if_range_condition(request),
        key: key.to_owned(),
        range,
        response_overrides: parse_object_read_response_overrides(query),
        version_id: optional_query(query, "versionId"),
    })
}

pub(crate) fn object_read_precondition_response(
    object: &HeadObjectOutput,
    conditions: &ObjectReadConditions,
) -> Option<EdgeResponse> {
    match evaluate_object_read_preconditions(
        &object.metadata.etag,
        object.metadata.last_modified_epoch_seconds,
        conditions,
    ) {
        ObjectReadPreconditionOutcome::NotModified => {
            Some(not_modified_response(object))
        }
        ObjectReadPreconditionOutcome::PreconditionFailed => {
            Some(precondition_failed_response(object))
        }
        ObjectReadPreconditionOutcome::Proceed => None,
    }
}

pub(crate) fn apply_range_to_head_object_output(
    object: &mut HeadObjectOutput,
    range: Option<&ObjectRange>,
) -> Result<(), AwsError> {
    let Some(range) = range else {
        return Ok(());
    };
    let size = object.content_length;
    let (start, end) = resolve_range_bounds(range, size)?;
    object.content_length = end - start + 1;
    Ok(())
}

pub(crate) fn apply_range_to_get_object_output(
    object: &mut GetObjectOutput,
    range: Option<&ObjectRange>,
) -> Result<(), AwsError> {
    let Some(range) = range else {
        return Ok(());
    };
    let size = object.content_length;
    let (start, end) = resolve_range_bounds(range, size)?;
    let start = usize::try_from(start).map_err(|_| invalid_range_error())?;
    let end = usize::try_from(end).map_err(|_| invalid_range_error())?;
    object.body = object.body[start..=end].to_vec();
    object.content_length = (end - start + 1) as u64;
    object.content_range = Some(format!("bytes {}-{}/{size}", start, end));
    object.is_partial = true;
    Ok(())
}

pub(crate) fn apply_object_read_response_overrides(
    mut response: EdgeResponse,
    overrides: &ObjectReadResponseOverrides,
) -> EdgeResponse {
    if let Some(value) = overrides.cache_control.as_deref() {
        response = response.set_header("Cache-Control", value);
    }
    if let Some(value) = overrides.content_disposition.as_deref() {
        response = response.set_header("Content-Disposition", value);
    }
    if let Some(value) = overrides.content_encoding.as_deref() {
        response = response.set_header("Content-Encoding", value);
    }
    if let Some(value) = overrides.content_language.as_deref() {
        response = response.set_header("Content-Language", value);
    }
    if let Some(value) = overrides.content_type.as_deref() {
        response = response.set_header("Content-Type", value);
    }
    if let Some(value) = overrides.expires.as_deref() {
        response = response.set_header("Expires", value);
    }
    response
}

fn parse_object_read_conditions(
    request: &HttpRequest<'_>,
) -> ObjectReadConditions {
    ObjectReadConditions {
        if_match: request.header("if-match").map(str::to_owned),
        if_modified_since: request
            .header("if-modified-since")
            .and_then(|value| httpdate::parse_http_date(value).ok()),
        if_none_match: request.header("if-none-match").map(str::to_owned),
        if_unmodified_since: request
            .header("if-unmodified-since")
            .and_then(|value| httpdate::parse_http_date(value).ok()),
    }
}

fn parse_object_read_response_overrides(
    query: &QueryParameters,
) -> ObjectReadResponseOverrides {
    ObjectReadResponseOverrides {
        cache_control: optional_query(query, "response-cache-control"),
        content_disposition: optional_query(
            query,
            "response-content-disposition",
        ),
        content_encoding: optional_query(query, "response-content-encoding"),
        content_language: optional_query(query, "response-content-language"),
        content_type: optional_query(query, "response-content-type"),
        expires: optional_query(query, "response-expires"),
    }
}

fn parse_if_range_condition(
    request: &HttpRequest<'_>,
) -> Option<IfRangeCondition> {
    let value = request.header("if-range")?;
    if value.starts_with('"') || value.starts_with("W/\"") {
        return Some(IfRangeCondition::ETag(value.to_owned()));
    }

    httpdate::parse_http_date(value).ok().map(IfRangeCondition::LastModified)
}

fn resolve_range_bounds(
    range: &ObjectRange,
    size: u64,
) -> Result<(u64, u64), AwsError> {
    if size == 0 {
        return Err(invalid_range_error());
    }
    match range {
        ObjectRange::Start { start } if *start < size => {
            Ok((*start, size - 1))
        }
        ObjectRange::StartEnd { start, end }
            if start <= end && *start < size =>
        {
            Ok((*start, (*end).min(size - 1)))
        }
        ObjectRange::Suffix { length } if *length > 0 => {
            let content_length = (*length).min(size);
            Ok((size - content_length, size - 1))
        }
        _ => Err(invalid_range_error()),
    }
}

fn not_modified_response(object: &HeadObjectOutput) -> EdgeResponse {
    let mut response = EdgeResponse::empty(304)
        .set_header("ETag", &object.metadata.etag)
        .set_header(
            "Last-Modified",
            http_timestamp(object.metadata.last_modified_epoch_seconds),
        );
    if let Some(version_id) = object.metadata.version_id.as_deref() {
        response = response.set_header("x-amz-version-id", version_id);
    }
    response
}

fn precondition_failed_response(object: &HeadObjectOutput) -> EdgeResponse {
    let mut response = EdgeResponse::empty(412)
        .set_header("ETag", &object.metadata.etag)
        .set_header(
            "Last-Modified",
            http_timestamp(object.metadata.last_modified_epoch_seconds),
        );
    if let Some(version_id) = object.metadata.version_id.as_deref() {
        response = response.set_header("x-amz-version-id", version_id);
    }
    response
}

fn parse_range_header(value: &str) -> Result<ObjectRange, AwsError> {
    let value = value.trim();
    let Some(raw_range) = value.strip_prefix("bytes=") else {
        return Err(invalid_range_error());
    };
    if raw_range.contains(',') {
        return Err(invalid_range_error());
    }
    let Some((start, end)) = raw_range.split_once('-') else {
        return Err(invalid_range_error());
    };
    if start.is_empty() {
        let length = end.parse::<u64>().map_err(|_| invalid_range_error())?;
        return Ok(ObjectRange::Suffix { length });
    }
    let start = start.parse::<u64>().map_err(|_| invalid_range_error())?;
    if end.is_empty() {
        return Ok(ObjectRange::Start { start });
    }
    let end = end.parse::<u64>().map_err(|_| invalid_range_error())?;
    Ok(ObjectRange::StartEnd { end, start })
}

fn invalid_range_error() -> AwsError {
    AwsError::trusted_custom(
        AwsErrorFamily::Validation,
        "InvalidRange",
        "The requested range is not satisfiable.",
        416,
        true,
    )
}

fn reject_unsupported_header(
    request: &HttpRequest<'_>,
    header: &str,
    message: &str,
) -> Result<(), AwsError> {
    if request.header(header).is_some() {
        return Err(not_implemented_message(message));
    }

    Ok(())
}

fn reject_unsupported_query_parameter(
    query: &QueryParameters,
    name: &str,
    message: &str,
) -> Result<(), AwsError> {
    if query.optional(name).is_some() {
        return Err(not_implemented_message(message));
    }

    Ok(())
}

fn http_timestamp(epoch_seconds: u64) -> String {
    httpdate::fmt_http_date(UNIX_EPOCH + Duration::from_secs(epoch_seconds))
}
