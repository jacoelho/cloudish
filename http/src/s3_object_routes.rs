use crate::request::HttpRequest;
use crate::runtime::EdgeResponse;
use crate::s3::{RequestTarget, S3RouteError};
use crate::s3_copy_parsing::parse_copy_source;
use crate::s3_error_encoding::{
    get_object_route_error, head_object_route_error,
};
use crate::s3_object_encoding::{
    complete_multipart_upload_response, copy_object_response,
    create_multipart_upload_response, delete_object_response,
    object_legal_hold_response, object_retention_response,
    object_tagging_response, tagging_write_response,
};
use crate::s3_object_lock_parsing::{
    parse_bypass_governance_retention, request_object_lock,
};
use crate::s3_object_reads::{
    apply_object_read_response_overrides, apply_range_to_get_object_output,
    apply_range_to_head_object_output, object_read_precondition_response,
    parse_object_read_request,
};
use crate::s3_object_request_parsing::{request_metadata, request_tags};
use crate::s3_object_responses::{
    head_invalid_range_response, head_response, object_response,
};
use crate::s3_query_parsing::{optional_query, required_query, required_u16};
use crate::s3_request_parsing::not_implemented_error;
use crate::s3_response_encoding::empty_xml_response;
use crate::s3_select_encoding::select_object_content_response;
use crate::s3_xml::{
    parse_complete_multipart_upload, parse_object_legal_hold,
    parse_object_retention, parse_select_object_content_request,
    parse_tagging,
};
use bytes::Bytes;
use s3::object_reads::eligible_object_range;
use services::{
    CompleteMultipartUploadInput, CopyObjectInput, CreateMultipartUploadInput,
    DeleteObjectInput, ObjectReadRequest, PutObjectInput,
    PutObjectLegalHoldInput, PutObjectRetentionInput, S3Scope, S3Service,
    TaggingInput, UploadPartInput,
};

pub(crate) fn handle_object_request(
    request: &HttpRequest<'_>,
    target: &RequestTarget,
    scope: &S3Scope,
    s3: &S3Service,
) -> Result<Option<EdgeResponse>, S3RouteError> {
    let (Some(bucket), Some(key)) =
        (target.bucket.as_deref(), target.key.as_deref())
    else {
        return Ok(None);
    };

    if let Some(response) = handle_object_subresource_request(
        request, target, scope, s3, bucket, key,
    )? {
        return Ok(Some(response));
    }

    if request.method() == "PUT"
        && request.header("x-amz-copy-source").is_some()
    {
        let (source_bucket, source_key, source_version_id) =
            parse_copy_source(request)?;
        let copied = s3
            .copy_object(
                scope,
                CopyObjectInput {
                    destination_bucket: bucket.to_owned(),
                    destination_key: key.to_owned(),
                    source_bucket,
                    source_key,
                    source_version_id,
                },
            )
            .map_err(S3RouteError::from)?;
        return Ok(Some(copy_object_response(
            &copied.etag,
            copied.last_modified_epoch_seconds,
            copied.version_id.as_deref(),
        )));
    }

    let object_read = match request.method() {
        "GET" | "HEAD" => {
            parse_object_read_request(request, &target.query, bucket, key)
                .map_err(S3RouteError::from)?
        }
        _ => ObjectReadRequest::default(),
    };

    let response = match request.method() {
        "PUT" => {
            let put = s3
                .put_object(
                    scope,
                    PutObjectInput {
                        body: request.body().to_vec(),
                        bucket: bucket.to_owned(),
                        content_type: request
                            .header("content-type")
                            .map(str::to_owned),
                        key: key.to_owned(),
                        metadata: request_metadata(request),
                        object_lock: request_object_lock(request)?,
                        tags: request_tags(request)?,
                    },
                )
                .map_err(S3RouteError::from)?;
            let mut response =
                EdgeResponse::bytes(200, "application/xml", Vec::new())
                    .set_header("ETag", put.etag);
            if let Some(version_id) = put.version_id {
                response = response.set_header("x-amz-version-id", version_id);
            }
            response
        }
        "GET" => {
            let mut service_read = object_read.clone();
            service_read.range = None;
            let metadata = s3
                .read_object_metadata(scope, &service_read)
                .map_err(get_object_route_error)?;
            if let Some(response) = object_read_precondition_response(
                &metadata,
                &object_read.conditions,
            ) {
                response
            } else {
                let range = eligible_object_range(
                    &metadata.metadata.etag,
                    metadata.metadata.last_modified_epoch_seconds,
                    object_read.range.as_ref(),
                    object_read.if_range.as_ref(),
                );
                let mut object = s3
                    .get_object(scope, &service_read)
                    .map_err(get_object_route_error)?;
                apply_range_to_get_object_output(&mut object, range)
                    .map_err(S3RouteError::from)?;
                apply_object_read_response_overrides(
                    object_response(object),
                    &object_read.response_overrides,
                )
            }
        }
        "HEAD" => {
            let mut service_read = object_read.clone();
            service_read.range = None;
            let mut object = s3
                .head_object(scope, &service_read)
                .map_err(head_object_route_error)?;
            if let Some(response) = object_read_precondition_response(
                &object,
                &object_read.conditions,
            ) {
                response
            } else {
                let range = eligible_object_range(
                    &object.metadata.etag,
                    object.metadata.last_modified_epoch_seconds,
                    object_read.range.as_ref(),
                    object_read.if_range.as_ref(),
                );
                match apply_range_to_head_object_output(&mut object, range) {
                    Ok(()) => apply_object_read_response_overrides(
                        head_response(object),
                        &object_read.response_overrides,
                    ),
                    Err(error) if error.code() == "InvalidRange" => {
                        head_invalid_range_response(object.content_length)
                    }
                    Err(error) => return Err(error.into()),
                }
            }
        }
        "DELETE" => {
            let deleted = s3
                .delete_object(
                    scope,
                    DeleteObjectInput {
                        bypass_governance: parse_bypass_governance_retention(
                            request,
                        )?,
                        bypass_governance_authorized: false,
                        bucket: bucket.to_owned(),
                        key: key.to_owned(),
                        version_id: optional_query(&target.query, "versionId"),
                    },
                )
                .map_err(S3RouteError::from)?;
            delete_object_response(&deleted)
        }
        _ => return Ok(None),
    };

    Ok(Some(response))
}

fn handle_object_subresource_request(
    request: &HttpRequest<'_>,
    target: &RequestTarget,
    scope: &S3Scope,
    s3: &S3Service,
    bucket: &str,
    key: &str,
) -> Result<Option<EdgeResponse>, S3RouteError> {
    if target.query.optional("uploads").is_some() {
        return match request.method() {
            "POST" => {
                let created = s3
                    .create_multipart_upload(
                        scope,
                        CreateMultipartUploadInput {
                            bucket: bucket.to_owned(),
                            content_type: request
                                .header("content-type")
                                .map(str::to_owned),
                            key: key.to_owned(),
                            metadata: request_metadata(request),
                            tags: request_tags(request)?,
                        },
                    )
                    .map_err(S3RouteError::from)?;
                Ok(Some(create_multipart_upload_response(
                    bucket,
                    key,
                    &created.upload_id,
                )))
            }
            _ => Ok(None),
        };
    }
    if target.query.optional("restore").is_some() {
        return match request.method() {
            "POST" => {
                s3.restore_object(
                    scope,
                    bucket,
                    key,
                    optional_query(&target.query, "versionId").as_deref(),
                )
                .map_err(S3RouteError::from)?;
                Ok(Some(empty_xml_response(202)))
            }
            _ => Ok(None),
        };
    }
    if target.query.optional("select").is_some()
        && target.query.optional("select-type") == Some("2")
    {
        return match request.method() {
            "POST" => {
                let input = parse_select_object_content_request(
                    Bytes::copy_from_slice(request.body()),
                    bucket.to_owned(),
                    key.to_owned(),
                    optional_query(&target.query, "versionId"),
                )?;
                let output = s3
                    .select_object_content(scope, input)
                    .map_err(S3RouteError::from)?;
                select_object_content_response(&output)
                    .map(Some)
                    .map_err(S3RouteError::from)
            }
            _ => Ok(None),
        };
    }
    if target.query.optional("uploadId").is_some() {
        return handle_multipart_request(
            request, target, scope, s3, bucket, key,
        )
        .map(Some);
    }
    if target.query.optional("tagging").is_some() {
        return handle_object_tagging_request(
            request, target, scope, s3, bucket, key,
        )
        .map(Some);
    }
    if target.query.optional("retention").is_some() {
        return handle_object_retention_request(
            request, target, scope, s3, bucket, key,
        )
        .map(Some);
    }
    if target.query.optional("legal-hold").is_some() {
        return handle_object_legal_hold_request(
            request, target, scope, s3, bucket, key,
        )
        .map(Some);
    }

    Ok(None)
}

fn handle_multipart_request(
    request: &HttpRequest<'_>,
    target: &RequestTarget,
    scope: &S3Scope,
    s3: &S3Service,
    bucket: &str,
    key: &str,
) -> Result<EdgeResponse, S3RouteError> {
    let upload_id = required_query(&target.query, "uploadId", "uploadId")?;

    match request.method() {
        "POST" => {
            let parts = parse_complete_multipart_upload(
                Bytes::copy_from_slice(request.body()),
            )?;
            let completed = s3
                .complete_multipart_upload(
                    scope,
                    CompleteMultipartUploadInput {
                        bucket: bucket.to_owned(),
                        key: key.to_owned(),
                        parts,
                        upload_id,
                    },
                )
                .map_err(S3RouteError::from)?;
            Ok(complete_multipart_upload_response(
                bucket,
                key,
                &completed.etag,
                completed.version_id.as_deref(),
            ))
        }
        "PUT" => {
            let part_number =
                required_u16(&target.query, "partNumber", "partNumber")?;
            let uploaded = s3
                .upload_part(
                    scope,
                    UploadPartInput {
                        body: request.body().to_vec(),
                        bucket: bucket.to_owned(),
                        key: key.to_owned(),
                        part_number,
                        upload_id,
                    },
                )
                .map_err(S3RouteError::from)?;
            Ok(EdgeResponse::bytes(200, "application/xml", Vec::new())
                .set_header("ETag", uploaded.etag))
        }
        "DELETE" => {
            s3.abort_multipart_upload(scope, bucket, key, &upload_id)
                .map_err(S3RouteError::from)?;
            Ok(empty_xml_response(204))
        }
        _ => Err(not_implemented_error().into()),
    }
}

fn handle_object_tagging_request(
    request: &HttpRequest<'_>,
    target: &RequestTarget,
    scope: &S3Scope,
    s3: &S3Service,
    bucket: &str,
    key: &str,
) -> Result<EdgeResponse, S3RouteError> {
    match request.method() {
        "PUT" => {
            let output = s3
                .put_object_tagging(
                    scope,
                    TaggingInput {
                        bucket: bucket.to_owned(),
                        key: Some(key.to_owned()),
                        tags: parse_tagging(Bytes::copy_from_slice(
                            request.body(),
                        ))?,
                        version_id: optional_query(&target.query, "versionId"),
                    },
                )
                .map_err(S3RouteError::from)?;
            Ok(tagging_write_response(200, &output))
        }
        "GET" => {
            let output = s3
                .get_object_tagging(
                    scope,
                    bucket,
                    key,
                    optional_query(&target.query, "versionId").as_deref(),
                )
                .map_err(S3RouteError::from)?;
            Ok(object_tagging_response(&output))
        }
        "DELETE" => {
            let output = s3
                .delete_object_tagging(
                    scope,
                    bucket,
                    key,
                    optional_query(&target.query, "versionId").as_deref(),
                )
                .map_err(S3RouteError::from)?;
            Ok(tagging_write_response(204, &output))
        }
        _ => Err(not_implemented_error().into()),
    }
}

fn handle_object_retention_request(
    request: &HttpRequest<'_>,
    target: &RequestTarget,
    scope: &S3Scope,
    s3: &S3Service,
    bucket: &str,
    key: &str,
) -> Result<EdgeResponse, S3RouteError> {
    match request.method() {
        "PUT" => {
            let retention = parse_object_retention(Bytes::copy_from_slice(
                request.body(),
            ))?;
            s3.put_object_retention(
                scope,
                PutObjectRetentionInput {
                    bucket: bucket.to_owned(),
                    bypass_governance: parse_bypass_governance_retention(
                        request,
                    )?,
                    bypass_governance_authorized: false,
                    key: key.to_owned(),
                    retention,
                    version_id: optional_query(&target.query, "versionId"),
                },
            )
            .map_err(S3RouteError::from)?;
            Ok(empty_xml_response(200))
        }
        "GET" => {
            let output = s3
                .get_object_retention(
                    scope,
                    bucket,
                    key,
                    optional_query(&target.query, "versionId").as_deref(),
                )
                .map_err(S3RouteError::from)?;
            Ok(object_retention_response(&output))
        }
        _ => Err(not_implemented_error().into()),
    }
}

fn handle_object_legal_hold_request(
    request: &HttpRequest<'_>,
    target: &RequestTarget,
    scope: &S3Scope,
    s3: &S3Service,
    bucket: &str,
    key: &str,
) -> Result<EdgeResponse, S3RouteError> {
    match request.method() {
        "PUT" => {
            let status = parse_object_legal_hold(Bytes::copy_from_slice(
                request.body(),
            ))?;
            s3.put_object_legal_hold(
                scope,
                PutObjectLegalHoldInput {
                    bucket: bucket.to_owned(),
                    key: key.to_owned(),
                    status,
                    version_id: optional_query(&target.query, "versionId"),
                },
            )
            .map_err(S3RouteError::from)?;
            Ok(empty_xml_response(200))
        }
        "GET" => {
            let output = s3
                .get_object_legal_hold(
                    scope,
                    bucket,
                    key,
                    optional_query(&target.query, "versionId").as_deref(),
                )
                .map_err(S3RouteError::from)?;
            Ok(object_legal_hold_response(&output))
        }
        _ => Err(not_implemented_error().into()),
    }
}
