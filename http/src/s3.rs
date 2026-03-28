pub(crate) use crate::aws_error_shape::AwsErrorShape;
use crate::query::QueryParameters;
use crate::request::{EdgeRequest, HttpRequest};
use crate::runtime::EdgeResponse;
use crate::s3_xml::{
    parse_bucket_notification_configuration, parse_bucket_versioning,
    parse_complete_multipart_upload, parse_object_legal_hold,
    parse_object_lock_configuration, parse_object_retention,
    parse_select_object_content_request, parse_tagging,
};
use crate::xml::XmlBuilder;
use auth::{AwsChunkedMode, VerifiedPayload, VerifiedSignature};
use aws::{AwsError, AwsErrorFamily};
use aws_smithy_eventstream::frame::write_message_to;
use aws_smithy_types::event_stream::{Header, HeaderValue, Message};
use bytes::Bytes;
use services::{
    BucketNotificationConfiguration, BucketObjectLockConfiguration,
    BucketTaggingOutput, BucketVersioningOutput, CannedAcl,
    CompleteMultipartUploadInput, CopyObjectInput, CreateBucketInput,
    CreateMultipartUploadInput, DeleteObjectInput, DeleteObjectOutput,
    GetBucketLocationOutput, GetObjectOutput, HeadObjectOutput,
    ListBucketsOutput, ListMultipartUploadsOutput, ListObjectVersionsInput,
    ListObjectVersionsOutput, ListObjectsInput, ListObjectsOutput,
    ListObjectsV2Input, ListObjectsV2Output, ObjectLegalHoldOutput,
    ObjectLockMode, ObjectRetention, ObjectTaggingOutput, PutObjectInput,
    PutObjectLegalHoldInput, PutObjectRetentionInput, S3Error, S3Scope,
    S3Service, SelectObjectContentOutput, StoredBucketAclInput, TaggingInput,
    UploadPartInput,
};
use std::collections::BTreeMap;
use std::time::{Duration, UNIX_EPOCH};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

const S3_XMLNS: &str = "http://s3.amazonaws.com/doc/2006-03-01/";
const REQUEST_ID: &str = "0000000000000000";

pub(crate) fn is_rest_xml_request(request: &HttpRequest<'_>) -> bool {
    let path = request.path_without_query();

    matches!(request.method(), "DELETE" | "GET" | "HEAD" | "POST" | "PUT")
        && request.header("host").is_some()
        && !path.starts_with("/__")
}

pub(crate) fn normalize_request(
    request: &mut EdgeRequest,
    verified_signature: Option<&VerifiedSignature>,
) -> Result<(), AwsError> {
    let uses_aws_chunked = request
        .header_values("content-encoding")
        .into_iter()
        .flat_map(|value| value.split(','))
        .map(str::trim)
        .any(|token| token.eq_ignore_ascii_case("aws-chunked"));
    let signing_context = verified_signature.and_then(|verified_signature| {
        match verified_signature.payload() {
            VerifiedPayload::AwsChunked(context) => Some(context),
            VerifiedPayload::SignedBody | VerifiedPayload::UnsignedPayload => {
                None
            }
        }
    });

    if !uses_aws_chunked && signing_context.is_none() {
        return Ok(());
    }

    let Some(signing_context) = signing_context else {
        return Err(invalid_request_error(
            "aws-chunked S3 requests must use streaming SigV4 payload signing.",
        ));
    };
    let decoded_content_length = request
        .header("x-amz-decoded-content-length")
        .ok_or_else(|| {
            invalid_request_error(
                "aws-chunked S3 requests must include X-Amz-Decoded-Content-Length.",
            )
        })?
        .parse::<usize>()
        .map_err(|_| {
            invalid_request_error(
                "X-Amz-Decoded-Content-Length must be a decimal byte count.",
            )
        })?;
    let trailer_names = request
        .header("x-amz-trailer")
        .map(|value| {
            value
                .split(',')
                .map(str::trim)
                .filter(|name| !name.is_empty())
                .map(str::to_owned)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    match signing_context.mode() {
        AwsChunkedMode::Payload if !trailer_names.is_empty() => {
            return Err(invalid_request_error(
                "STREAMING-AWS4-HMAC-SHA256-PAYLOAD requests must not include X-Amz-Trailer.",
            ));
        }
        AwsChunkedMode::PayloadTrailer if trailer_names.is_empty() => {
            return Err(invalid_request_error(
                "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER requests must include X-Amz-Trailer.",
            ));
        }
        AwsChunkedMode::Payload | AwsChunkedMode::PayloadTrailer => {}
    }

    let decoded = signing_context.decode(
        request.body(),
        decoded_content_length,
        &trailer_names,
    )?;
    request.set_body(decoded.decoded_body().to_vec());
    request.remove_header("x-amz-decoded-content-length");
    request.remove_header("x-amz-trailer");
    normalize_content_encoding(request);
    for (name, value) in decoded.trailer_headers() {
        request.append_header(name.clone(), value.clone());
    }

    Ok(())
}

fn normalize_content_encoding(request: &mut EdgeRequest) {
    let remaining = request
        .header_values("content-encoding")
        .into_iter()
        .flat_map(|value| value.split(','))
        .map(str::trim)
        .filter(|token| {
            !token.is_empty() && !token.eq_ignore_ascii_case("aws-chunked")
        })
        .collect::<Vec<_>>();

    if remaining.is_empty() {
        request.remove_header("content-encoding");
    } else {
        request.set_header("Content-Encoding", remaining.join(","));
    }
}

fn invalid_request_error(message: impl Into<String>) -> AwsError {
    AwsError::trusted_custom(
        AwsErrorFamily::Validation,
        "InvalidRequest",
        message,
        400,
        true,
    )
}

pub(crate) fn handle(
    request: &HttpRequest<'_>,
    scope: &S3Scope,
    s3: &S3Service,
) -> Result<EdgeResponse, AwsError> {
    let target = RequestTarget::parse(request)?;

    if let Some(response) =
        handle_root_request(request.method(), &target, scope, s3)
    {
        return Ok(response);
    }

    if let Some(response) = handle_bucket_request(request, &target, scope, s3)?
    {
        return Ok(response);
    }

    if let Some(response) = handle_object_request(request, &target, scope, s3)?
    {
        return Ok(response);
    }

    Err(not_implemented_error())
}

fn handle_root_request(
    method: &str,
    target: &RequestTarget,
    scope: &S3Scope,
    s3: &S3Service,
) -> Option<EdgeResponse> {
    if method == "GET" && target.bucket.is_none() && target.key.is_none() {
        Some(list_buckets_response(scope, &s3.list_buckets(scope)))
    } else {
        None
    }
}

fn handle_bucket_request(
    request: &HttpRequest<'_>,
    target: &RequestTarget,
    scope: &S3Scope,
    s3: &S3Service,
) -> Result<Option<EdgeResponse>, AwsError> {
    let Some(bucket) = target.bucket.as_deref() else {
        return Ok(None);
    };
    if target.key.is_some() {
        return Ok(None);
    }

    if let Some(response) =
        handle_bucket_subresource_request(request, target, scope, s3, bucket)?
    {
        return Ok(Some(response));
    }

    let response = match request.method() {
        "PUT" => {
            let region = parse_create_bucket_region(request, scope)?;
            let object_lock_enabled =
                parse_create_bucket_object_lock_enabled(request)?;
            let created = s3
                .create_bucket(
                    scope,
                    CreateBucketInput {
                        name: bucket.to_owned(),
                        object_lock_enabled,
                        region,
                    },
                )
                .map_err(|error| error.to_aws_error())?;

            EdgeResponse::bytes(200, "application/xml", Vec::new())
                .set_header("Location", format!("/{}", created.name))
        }
        "DELETE" => {
            s3.delete_bucket(scope, bucket)
                .map_err(|error| error.to_aws_error())?;
            EdgeResponse::bytes(204, "application/xml", Vec::new())
        }
        "GET" if target.query.optional("list-type") == Some("2") => {
            let output = s3
                .list_objects_v2(
                    scope,
                    ListObjectsV2Input {
                        bucket: bucket.to_owned(),
                        continuation_token: optional_query(
                            &target.query,
                            "continuation-token",
                        ),
                        delimiter: optional_query(&target.query, "delimiter"),
                        max_keys: optional_usize(&target.query, "max-keys")?,
                        prefix: optional_query(&target.query, "prefix"),
                        start_after: optional_query(
                            &target.query,
                            "start-after",
                        ),
                    },
                )
                .map_err(|error| error.to_aws_error())?;
            list_objects_v2_response(&output)
        }
        "GET" => {
            let output = s3
                .list_objects(
                    scope,
                    ListObjectsInput {
                        bucket: bucket.to_owned(),
                        delimiter: optional_query(&target.query, "delimiter"),
                        marker: optional_query(&target.query, "marker"),
                        max_keys: optional_usize(&target.query, "max-keys")?,
                        prefix: optional_query(&target.query, "prefix"),
                    },
                )
                .map_err(|error| error.to_aws_error())?;
            list_objects_response(&output)
        }
        _ => return Ok(None),
    };

    Ok(Some(response))
}

fn handle_bucket_subresource_request(
    request: &HttpRequest<'_>,
    target: &RequestTarget,
    scope: &S3Scope,
    s3: &S3Service,
    bucket: &str,
) -> Result<Option<EdgeResponse>, AwsError> {
    if target.query.optional("versioning").is_some() {
        return handle_bucket_versioning_request(request, scope, s3, bucket)
            .map(Some);
    }
    if target.query.optional("notification").is_some() {
        return handle_bucket_notification_request(request, scope, s3, bucket)
            .map(Some);
    }
    if target.query.optional("object-lock").is_some() {
        return handle_bucket_object_lock_request(request, scope, s3, bucket)
            .map(Some);
    }
    if target.query.optional("tagging").is_some() {
        return handle_bucket_tagging_request(request, scope, s3, bucket)
            .map(Some);
    }
    if target.query.optional("policy").is_some() {
        return handle_bucket_policy_request(request, scope, s3, bucket)
            .map(Some);
    }
    if target.query.optional("cors").is_some() {
        return handle_bucket_cors_request(request, scope, s3, bucket)
            .map(Some);
    }
    if target.query.optional("lifecycle").is_some() {
        return handle_bucket_lifecycle_request(request, scope, s3, bucket)
            .map(Some);
    }
    if target.query.optional("encryption").is_some() {
        return handle_bucket_encryption_request(request, scope, s3, bucket)
            .map(Some);
    }
    if target.query.optional("acl").is_some() {
        return handle_bucket_acl_request(request, scope, s3, bucket)
            .map(Some);
    }
    if target.query.optional("uploads").is_some() {
        return match request.method() {
            "GET" => {
                let uploads = s3
                    .list_multipart_uploads(scope, bucket)
                    .map_err(|error| error.to_aws_error())?;
                Ok(Some(list_multipart_uploads_response(&uploads)))
            }
            _ => Ok(None),
        };
    }
    if target.query.optional("versions").is_some() {
        return match request.method() {
            "GET" => {
                let versions = s3
                    .list_object_versions(
                        scope,
                        ListObjectVersionsInput {
                            bucket: bucket.to_owned(),
                            max_keys: optional_usize(
                                &target.query,
                                "max-keys",
                            )?,
                            prefix: optional_query(&target.query, "prefix"),
                        },
                    )
                    .map_err(|error| error.to_aws_error())?;
                Ok(Some(list_object_versions_response(&versions)))
            }
            _ => Ok(None),
        };
    }
    if target.query.optional("location").is_some() {
        return match request.method() {
            "GET" => {
                let location = s3
                    .get_bucket_location(scope, bucket)
                    .map_err(|error| error.to_aws_error())?;
                Ok(Some(get_bucket_location_response(&location)))
            }
            _ => Ok(None),
        };
    }

    Ok(None)
}

fn handle_object_request(
    request: &HttpRequest<'_>,
    target: &RequestTarget,
    scope: &S3Scope,
    s3: &S3Service,
) -> Result<Option<EdgeResponse>, AwsError> {
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
        let (source_bucket, source_key) = parse_copy_source(request)?;
        let copied = s3
            .copy_object(
                scope,
                CopyObjectInput {
                    destination_bucket: bucket.to_owned(),
                    destination_key: key.to_owned(),
                    source_bucket,
                    source_key,
                },
            )
            .map_err(|error| error.to_aws_error())?;
        return Ok(Some(copy_object_response(
            &copied.etag,
            copied.last_modified_epoch_seconds,
            copied.version_id.as_deref(),
        )));
    }

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
                .map_err(|error| error.to_aws_error())?;
            let mut response =
                EdgeResponse::bytes(200, "application/xml", Vec::new())
                    .set_header("ETag", put.etag);
            if let Some(version_id) = put.version_id {
                response = response.set_header("x-amz-version-id", version_id);
            }
            response
        }
        "GET" => {
            let object = s3
                .get_object(
                    scope,
                    bucket,
                    key,
                    optional_query(&target.query, "versionId").as_deref(),
                )
                .map_err(|error| error.to_aws_error())?;
            object_response(object)
        }
        "HEAD" => {
            let object = s3
                .head_object(
                    scope,
                    bucket,
                    key,
                    optional_query(&target.query, "versionId").as_deref(),
                )
                .map_err(|error| error.to_aws_error())?;
            head_response(object)
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
                .map_err(|error| error.to_aws_error())?;
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
) -> Result<Option<EdgeResponse>, AwsError> {
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
                    .map_err(|error| error.to_aws_error())?;
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
                .map_err(|error| error.to_aws_error())?;
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
                    .map_err(|error| error.to_aws_error())?;
                select_object_content_response(&output).map(Some)
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

fn handle_bucket_versioning_request(
    request: &HttpRequest<'_>,
    scope: &S3Scope,
    s3: &S3Service,
    bucket: &str,
) -> Result<EdgeResponse, AwsError> {
    match request.method() {
        "PUT" => {
            let status = parse_bucket_versioning(Bytes::copy_from_slice(
                request.body(),
            ))?;
            s3.put_bucket_versioning(scope, bucket, status)
                .map_err(|error| error.to_aws_error())?;
            Ok(empty_xml_response(200))
        }
        "GET" => {
            let output = s3
                .get_bucket_versioning(scope, bucket)
                .map_err(|error| error.to_aws_error())?;
            Ok(bucket_versioning_response(&output))
        }
        _ => Err(not_implemented_error()),
    }
}

fn handle_bucket_notification_request(
    request: &HttpRequest<'_>,
    scope: &S3Scope,
    s3: &S3Service,
    bucket: &str,
) -> Result<EdgeResponse, AwsError> {
    match request.method() {
        "PUT" => {
            let configuration = parse_bucket_notification_configuration(
                Bytes::copy_from_slice(request.body()),
            )?;
            s3.put_bucket_notification_configuration(
                scope,
                bucket,
                configuration,
            )
            .map_err(|error| error.to_aws_error())?;
            Ok(empty_xml_response(200))
        }
        "GET" => {
            let output = s3
                .get_bucket_notification_configuration(scope, bucket)
                .map_err(|error| error.to_aws_error())?;
            Ok(notification_configuration_response(&output))
        }
        _ => Err(not_implemented_error()),
    }
}

fn handle_bucket_object_lock_request(
    request: &HttpRequest<'_>,
    scope: &S3Scope,
    s3: &S3Service,
    bucket: &str,
) -> Result<EdgeResponse, AwsError> {
    match request.method() {
        "PUT" => {
            let configuration = parse_object_lock_configuration(
                Bytes::copy_from_slice(request.body()),
            )?;
            s3.put_object_lock_configuration(scope, bucket, configuration)
                .map_err(|error| error.to_aws_error())?;
            Ok(empty_xml_response(200))
        }
        "GET" => {
            let output = s3
                .get_object_lock_configuration(scope, bucket)
                .map_err(|error| error.to_aws_error())?;
            Ok(object_lock_configuration_response(&output))
        }
        _ => Err(not_implemented_error()),
    }
}

fn handle_bucket_tagging_request(
    request: &HttpRequest<'_>,
    scope: &S3Scope,
    s3: &S3Service,
    bucket: &str,
) -> Result<EdgeResponse, AwsError> {
    match request.method() {
        "PUT" => {
            let tags = parse_tagging(Bytes::copy_from_slice(request.body()))?;
            s3.put_bucket_tagging(scope, bucket, tags)
                .map_err(|error| error.to_aws_error())?;
            Ok(empty_xml_response(200))
        }
        "GET" => {
            let output = s3
                .get_bucket_tagging(scope, bucket)
                .map_err(|error| error.to_aws_error())?;
            Ok(bucket_tagging_response(&output))
        }
        "DELETE" => {
            s3.delete_bucket_tagging(scope, bucket)
                .map_err(|error| error.to_aws_error())?;
            Ok(empty_xml_response(204))
        }
        _ => Err(not_implemented_error()),
    }
}

fn handle_bucket_policy_request(
    request: &HttpRequest<'_>,
    scope: &S3Scope,
    s3: &S3Service,
    bucket: &str,
) -> Result<EdgeResponse, AwsError> {
    match request.method() {
        "PUT" => {
            let policy = request_body_utf8(request)?;
            s3.put_bucket_policy(scope, bucket, policy)
                .map_err(|error| error.to_aws_error())?;
            Ok(empty_xml_response(204))
        }
        "GET" => {
            let policy = s3
                .get_bucket_policy(scope, bucket)
                .map_err(|error| error.to_aws_error())?;
            Ok(EdgeResponse::bytes(
                200,
                "application/json",
                policy.into_bytes(),
            ))
        }
        "DELETE" => {
            s3.delete_bucket_policy(scope, bucket)
                .map_err(|error| error.to_aws_error())?;
            Ok(empty_xml_response(204))
        }
        _ => Err(not_implemented_error()),
    }
}

fn handle_bucket_cors_request(
    request: &HttpRequest<'_>,
    scope: &S3Scope,
    s3: &S3Service,
    bucket: &str,
) -> Result<EdgeResponse, AwsError> {
    match request.method() {
        "PUT" => {
            let body = request_body_utf8(request)?;
            s3.put_bucket_cors(scope, bucket, body)
                .map_err(|error| error.to_aws_error())?;
            Ok(empty_xml_response(200))
        }
        "GET" => {
            let cors = s3
                .get_bucket_cors(scope, bucket)
                .map_err(|error| error.to_aws_error())?;
            Ok(EdgeResponse::bytes(200, "application/xml", cors.into_bytes()))
        }
        "DELETE" => {
            s3.delete_bucket_cors(scope, bucket)
                .map_err(|error| error.to_aws_error())?;
            Ok(empty_xml_response(204))
        }
        _ => Err(not_implemented_error()),
    }
}

fn handle_bucket_lifecycle_request(
    request: &HttpRequest<'_>,
    scope: &S3Scope,
    s3: &S3Service,
    bucket: &str,
) -> Result<EdgeResponse, AwsError> {
    match request.method() {
        "PUT" => {
            let body = request_body_utf8(request)?;
            s3.put_bucket_lifecycle(scope, bucket, body)
                .map_err(|error| error.to_aws_error())?;
            Ok(empty_xml_response(200))
        }
        "GET" => {
            let lifecycle = s3
                .get_bucket_lifecycle(scope, bucket)
                .map_err(|error| error.to_aws_error())?;
            Ok(EdgeResponse::bytes(
                200,
                "application/xml",
                lifecycle.into_bytes(),
            ))
        }
        "DELETE" => {
            s3.delete_bucket_lifecycle(scope, bucket)
                .map_err(|error| error.to_aws_error())?;
            Ok(empty_xml_response(204))
        }
        _ => Err(not_implemented_error()),
    }
}

fn handle_bucket_encryption_request(
    request: &HttpRequest<'_>,
    scope: &S3Scope,
    s3: &S3Service,
    bucket: &str,
) -> Result<EdgeResponse, AwsError> {
    match request.method() {
        "PUT" => {
            let body = request_body_utf8(request)?;
            s3.put_bucket_encryption(scope, bucket, body)
                .map_err(|error| error.to_aws_error())?;
            Ok(empty_xml_response(200))
        }
        "GET" => {
            let encryption = s3
                .get_bucket_encryption(scope, bucket)
                .map_err(|error| error.to_aws_error())?;
            Ok(EdgeResponse::bytes(
                200,
                "application/xml",
                encryption.into_bytes(),
            ))
        }
        "DELETE" => {
            s3.delete_bucket_encryption(scope, bucket)
                .map_err(|error| error.to_aws_error())?;
            Ok(empty_xml_response(204))
        }
        _ => Err(not_implemented_error()),
    }
}

fn handle_bucket_acl_request(
    request: &HttpRequest<'_>,
    scope: &S3Scope,
    s3: &S3Service,
    bucket: &str,
) -> Result<EdgeResponse, AwsError> {
    match request.method() {
        "PUT" => {
            let acl = parse_bucket_acl(request)?;
            s3.put_bucket_acl(scope, bucket, acl)
                .map_err(|error| error.to_aws_error())?;
            Ok(empty_xml_response(200))
        }
        "GET" => {
            let acl = s3
                .get_bucket_acl(scope, bucket)
                .map_err(|error| error.to_aws_error())?;
            Ok(EdgeResponse::bytes(200, "application/xml", acl.into_bytes()))
        }
        _ => Err(not_implemented_error()),
    }
}

fn handle_multipart_request(
    request: &HttpRequest<'_>,
    target: &RequestTarget,
    scope: &S3Scope,
    s3: &S3Service,
    bucket: &str,
    key: &str,
) -> Result<EdgeResponse, AwsError> {
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
                .map_err(|error| error.to_aws_error())?;
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
                .map_err(|error| error.to_aws_error())?;
            Ok(EdgeResponse::bytes(200, "application/xml", Vec::new())
                .set_header("ETag", uploaded.etag))
        }
        "DELETE" => {
            s3.abort_multipart_upload(scope, bucket, key, &upload_id)
                .map_err(|error| error.to_aws_error())?;
            Ok(empty_xml_response(204))
        }
        _ => Err(not_implemented_error()),
    }
}

fn handle_object_tagging_request(
    request: &HttpRequest<'_>,
    target: &RequestTarget,
    scope: &S3Scope,
    s3: &S3Service,
    bucket: &str,
    key: &str,
) -> Result<EdgeResponse, AwsError> {
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
                .map_err(|error| error.to_aws_error())?;
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
                .map_err(|error| error.to_aws_error())?;
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
                .map_err(|error| error.to_aws_error())?;
            Ok(tagging_write_response(204, &output))
        }
        _ => Err(not_implemented_error()),
    }
}

fn handle_object_retention_request(
    request: &HttpRequest<'_>,
    target: &RequestTarget,
    scope: &S3Scope,
    s3: &S3Service,
    bucket: &str,
    key: &str,
) -> Result<EdgeResponse, AwsError> {
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
            .map_err(|error| error.to_aws_error())?;
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
                .map_err(|error| error.to_aws_error())?;
            Ok(object_retention_response(&output))
        }
        _ => Err(not_implemented_error()),
    }
}

fn handle_object_legal_hold_request(
    request: &HttpRequest<'_>,
    target: &RequestTarget,
    scope: &S3Scope,
    s3: &S3Service,
    bucket: &str,
    key: &str,
) -> Result<EdgeResponse, AwsError> {
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
            .map_err(|error| error.to_aws_error())?;
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
                .map_err(|error| error.to_aws_error())?;
            Ok(object_legal_hold_response(&output))
        }
        _ => Err(not_implemented_error()),
    }
}

pub(crate) fn s3_error_response(error: &AwsError) -> EdgeResponse {
    let body = XmlBuilder::new()
        .start("Error", Some(S3_XMLNS))
        .elem("Code", error.code())
        .elem("Message", error.message())
        .elem("RequestId", REQUEST_ID)
        .end("Error")
        .build()
        .into_bytes();

    EdgeResponse::bytes(error.status_code(), "application/xml", body)
}

#[derive(Debug)]
struct RequestTarget {
    bucket: Option<String>,
    key: Option<String>,
    query: QueryParameters,
}

impl RequestTarget {
    fn parse(request: &HttpRequest<'_>) -> Result<Self, AwsError> {
        let query = QueryParameters::parse(
            request.query_string().unwrap_or_default().as_bytes(),
        )?;
        let path = request.path_without_query();
        let host_bucket =
            host_bucket_name(request.header("host").unwrap_or_default());

        if let Some(bucket) = host_bucket {
            let key = path
                .strip_prefix('/')
                .unwrap_or(path)
                .strip_prefix('/')
                .unwrap_or(path.strip_prefix('/').unwrap_or(path));
            let key = if key.is_empty() {
                None
            } else {
                Some(percent_decode_path(key)?)
            };

            return Ok(Self { bucket: Some(bucket), key, query });
        }

        let path = path.strip_prefix('/').unwrap_or(path);
        if path.is_empty() {
            return Ok(Self { bucket: None, key: None, query });
        }

        let (bucket, key) = match path.split_once('/') {
            Some((bucket, "")) => (bucket, None),
            Some((bucket, key)) => (bucket, Some(percent_decode_path(key)?)),
            None => (path, None),
        };

        Ok(Self { bucket: Some(percent_decode_path(bucket)?), key, query })
    }
}

fn empty_xml_response(status_code: u16) -> EdgeResponse {
    EdgeResponse::bytes(status_code, "application/xml", Vec::new())
}

fn copy_object_response(
    etag: &str,
    last_modified_epoch_seconds: u64,
    version_id: Option<&str>,
) -> EdgeResponse {
    let body = XmlBuilder::new()
        .start("CopyObjectResult", Some(S3_XMLNS))
        .elem("ETag", etag)
        .elem("LastModified", &iso_timestamp(last_modified_epoch_seconds))
        .end("CopyObjectResult")
        .build()
        .into_bytes();

    let mut response = EdgeResponse::bytes(200, "application/xml", body);
    if let Some(version_id) = version_id {
        response = response.set_header("x-amz-version-id", version_id);
    }
    response
}

fn create_multipart_upload_response(
    bucket: &str,
    key: &str,
    upload_id: &str,
) -> EdgeResponse {
    let body = XmlBuilder::new()
        .start("InitiateMultipartUploadResult", Some(S3_XMLNS))
        .elem("Bucket", bucket)
        .elem("Key", key)
        .elem("UploadId", upload_id)
        .end("InitiateMultipartUploadResult")
        .build()
        .into_bytes();

    EdgeResponse::bytes(200, "application/xml", body)
}

fn complete_multipart_upload_response(
    bucket: &str,
    key: &str,
    etag: &str,
    version_id: Option<&str>,
) -> EdgeResponse {
    let body = XmlBuilder::new()
        .start("CompleteMultipartUploadResult", Some(S3_XMLNS))
        .elem("Bucket", bucket)
        .elem("Key", key)
        .elem("ETag", etag)
        .end("CompleteMultipartUploadResult")
        .build()
        .into_bytes();

    let mut response = EdgeResponse::bytes(200, "application/xml", body);
    if let Some(version_id) = version_id {
        response = response.set_header("x-amz-version-id", version_id);
    }
    response
}

fn bucket_tagging_response(output: &BucketTaggingOutput) -> EdgeResponse {
    let mut xml = XmlBuilder::new()
        .start("Tagging", Some(S3_XMLNS))
        .start("TagSet", None);
    for (key, value) in &output.tags {
        xml = xml
            .start("Tag", None)
            .elem("Key", key)
            .elem("Value", value)
            .end("Tag");
    }

    EdgeResponse::bytes(
        200,
        "application/xml",
        xml.end("TagSet").end("Tagging").build().into_bytes(),
    )
}

fn object_tagging_response(output: &ObjectTaggingOutput) -> EdgeResponse {
    let mut response = bucket_tagging_response(&BucketTaggingOutput {
        tags: output.tags.clone(),
    });
    if let Some(version_id) = output.version_id.as_deref() {
        response = response.set_header("x-amz-version-id", version_id);
    }
    response
}

fn tagging_write_response(
    status_code: u16,
    output: &ObjectTaggingOutput,
) -> EdgeResponse {
    let mut response = empty_xml_response(status_code);
    if let Some(version_id) = output.version_id.as_deref() {
        response = response.set_header("x-amz-version-id", version_id);
    }
    response
}

fn bucket_versioning_response(
    output: &BucketVersioningOutput,
) -> EdgeResponse {
    let mut xml =
        XmlBuilder::new().start("VersioningConfiguration", Some(S3_XMLNS));
    if let Some(status) = output.status {
        xml = xml.elem("Status", status.as_str());
    }

    EdgeResponse::bytes(
        200,
        "application/xml",
        xml.end("VersioningConfiguration").build().into_bytes(),
    )
}

fn notification_configuration_response(
    output: &BucketNotificationConfiguration,
) -> EdgeResponse {
    let mut xml =
        XmlBuilder::new().start("NotificationConfiguration", Some(S3_XMLNS));

    for queue in &output.queue_configurations {
        xml = xml.start("QueueConfiguration", None);
        if let Some(id) = queue.id.as_deref() {
            xml = xml.elem("Id", id);
        }
        xml = xml.elem("Queue", &queue.queue_arn.to_string());
        for event in &queue.events {
            xml = xml.elem("Event", event);
        }
        xml = append_notification_filter(xml, queue.filter.as_ref());
        xml = xml.end("QueueConfiguration");
    }

    for topic in &output.topic_configurations {
        xml = xml.start("TopicConfiguration", None);
        if let Some(id) = topic.id.as_deref() {
            xml = xml.elem("Id", id);
        }
        xml = xml.elem("Topic", &topic.topic_arn.to_string());
        for event in &topic.events {
            xml = xml.elem("Event", event);
        }
        xml = append_notification_filter(xml, topic.filter.as_ref());
        xml = xml.end("TopicConfiguration");
    }

    EdgeResponse::bytes(
        200,
        "application/xml",
        xml.end("NotificationConfiguration").build().into_bytes(),
    )
}

fn append_notification_filter(
    mut xml: XmlBuilder,
    filter: Option<&services::NotificationFilter>,
) -> XmlBuilder {
    let Some(filter) = filter else {
        return xml;
    };
    if filter.prefix.is_none() && filter.suffix.is_none() {
        return xml;
    }

    xml = xml.start("Filter", None).start("S3Key", None);
    if let Some(prefix) = filter.prefix.as_deref() {
        xml = xml
            .start("FilterRule", None)
            .elem("Name", "prefix")
            .elem("Value", prefix)
            .end("FilterRule");
    }
    if let Some(suffix) = filter.suffix.as_deref() {
        xml = xml
            .start("FilterRule", None)
            .elem("Name", "suffix")
            .elem("Value", suffix)
            .end("FilterRule");
    }
    xml.end("S3Key").end("Filter")
}

fn object_lock_configuration_response(
    output: &BucketObjectLockConfiguration,
) -> EdgeResponse {
    let mut xml =
        XmlBuilder::new().start("ObjectLockConfiguration", Some(S3_XMLNS));
    if output.object_lock_enabled {
        xml = xml.elem("ObjectLockEnabled", "Enabled");
    }
    if let Some(retention) = output.default_retention.as_ref() {
        xml = xml.start("Rule", None).start("DefaultRetention", None);
        xml = xml.elem("Mode", retention.mode.as_str());
        xml = match retention.period {
            services::DefaultRetentionPeriod::Days(days) => {
                xml.elem("Days", &days.to_string())
            }
            services::DefaultRetentionPeriod::Years(years) => {
                xml.elem("Years", &years.to_string())
            }
        };
        xml = xml.end("DefaultRetention").end("Rule");
    }

    EdgeResponse::bytes(
        200,
        "application/xml",
        xml.end("ObjectLockConfiguration").build().into_bytes(),
    )
}

fn object_retention_response(output: &ObjectRetention) -> EdgeResponse {
    let mut xml = XmlBuilder::new().start("Retention", Some(S3_XMLNS));
    if let Some(mode) = output.mode {
        xml = xml.elem("Mode", mode.as_str());
    }
    if let Some(retain_until_epoch_seconds) = output.retain_until_epoch_seconds
    {
        xml = xml.elem(
            "RetainUntilDate",
            &rfc3339_timestamp(retain_until_epoch_seconds),
        );
    }

    EdgeResponse::bytes(
        200,
        "application/xml",
        xml.end("Retention").build().into_bytes(),
    )
}

fn object_legal_hold_response(output: &ObjectLegalHoldOutput) -> EdgeResponse {
    let body = XmlBuilder::new()
        .start("LegalHold", Some(S3_XMLNS))
        .elem("Status", output.status.as_str())
        .end("LegalHold")
        .build()
        .into_bytes();

    EdgeResponse::bytes(200, "application/xml", body)
}

fn select_object_content_response(
    output: &SelectObjectContentOutput,
) -> Result<EdgeResponse, AwsError> {
    let body = encode_select_object_content_body(output)?;
    Ok(EdgeResponse::bytes(200, "application/vnd.amazon.eventstream", body)
        .set_header("x-amz-request-id", REQUEST_ID)
        .set_header("x-amz-id-2", REQUEST_ID))
}

fn list_multipart_uploads_response(
    output: &ListMultipartUploadsOutput,
) -> EdgeResponse {
    let mut xml = XmlBuilder::new()
        .start("ListMultipartUploadsResult", Some(S3_XMLNS))
        .elem("Bucket", &output.bucket);
    for upload in &output.uploads {
        xml = xml
            .start("Upload", None)
            .elem("Key", &upload.key)
            .elem("UploadId", &upload.upload_id)
            .elem("Initiated", &iso_timestamp(upload.initiated_epoch_seconds))
            .end("Upload");
    }

    EdgeResponse::bytes(
        200,
        "application/xml",
        xml.end("ListMultipartUploadsResult").build().into_bytes(),
    )
}

fn list_object_versions_response(
    output: &ListObjectVersionsOutput,
) -> EdgeResponse {
    let mut xml = XmlBuilder::new()
        .start("ListVersionsResult", Some(S3_XMLNS))
        .elem("Name", &output.bucket)
        .elem("Prefix", output.prefix.as_deref().unwrap_or_default())
        .elem("KeyMarker", "")
        .elem("VersionIdMarker", "")
        .elem("MaxKeys", &output.max_keys.to_string())
        .elem("IsTruncated", bool_text(output.is_truncated));

    for entry in &output.entries {
        match entry {
            services::ListedVersionEntry::DeleteMarker(marker) => {
                xml = xml
                    .start("DeleteMarker", None)
                    .elem("Key", &marker.key)
                    .elem("VersionId", &marker.version_id)
                    .elem("IsLatest", bool_text(marker.is_latest))
                    .elem(
                        "LastModified",
                        &iso_timestamp(marker.last_modified_epoch_seconds),
                    )
                    .end("DeleteMarker");
            }
            services::ListedVersionEntry::Version(version) => {
                xml = xml
                    .start("Version", None)
                    .elem("Key", &version.key)
                    .elem("VersionId", &version.version_id)
                    .elem("IsLatest", bool_text(version.is_latest))
                    .elem(
                        "LastModified",
                        &iso_timestamp(version.last_modified_epoch_seconds),
                    )
                    .elem("ETag", &version.etag)
                    .elem("Size", &version.size.to_string())
                    .elem("StorageClass", &version.storage_class)
                    .end("Version");
            }
        }
    }

    EdgeResponse::bytes(
        200,
        "application/xml",
        xml.end("ListVersionsResult").build().into_bytes(),
    )
}

fn get_bucket_location_response(
    location: &GetBucketLocationOutput,
) -> EdgeResponse {
    let value = if location.region == "us-east-1" {
        String::new()
    } else {
        location.region.clone()
    };
    let body = XmlBuilder::new()
        .start("LocationConstraint", Some(S3_XMLNS))
        .raw(&crate::xml::escape(&value))
        .end("LocationConstraint")
        .build()
        .into_bytes();

    EdgeResponse::bytes(200, "application/xml", body)
}

fn delete_object_response(output: &DeleteObjectOutput) -> EdgeResponse {
    let mut response = EdgeResponse::bytes(204, "application/xml", Vec::new());
    if output.delete_marker {
        response = response.set_header("x-amz-delete-marker", "true");
    }
    if let Some(version_id) = output.version_id.as_deref() {
        response = response.set_header("x-amz-version-id", version_id);
    }
    response
}

fn head_response(object: HeadObjectOutput) -> EdgeResponse {
    let metadata = object.metadata.clone();
    let mut response =
        EdgeResponse::bytes(200, &object.content_type, Vec::new())
            .set_header("Content-Length", object.size.to_string())
            .set_header("ETag", object.etag.clone())
            .set_header(
                "Last-Modified",
                http_timestamp(object.last_modified_epoch_seconds),
            );

    if let Some(version_id) = object.version_id.clone() {
        response = response.set_header("x-amz-version-id", version_id);
    }

    for (name, value) in metadata {
        response = response.set_header(format!("x-amz-meta-{name}"), value);
    }

    apply_object_lock_headers(response, &object)
}

fn host_bucket_name(host: &str) -> Option<String> {
    let host =
        host.split_once(':').map(|(value, _)| value).unwrap_or(host).trim();

    host.split_once(".s3.")
        .map(|(bucket, _)| bucket.to_owned())
        .filter(|bucket| !bucket.is_empty())
}

fn http_timestamp(epoch_seconds: u64) -> String {
    httpdate::fmt_http_date(UNIX_EPOCH + Duration::from_secs(epoch_seconds))
}

fn iso_timestamp(epoch_seconds: u64) -> String {
    OffsetDateTime::from_unix_timestamp(epoch_seconds as i64)
        .ok()
        .and_then(|timestamp| timestamp.format(&Rfc3339).ok())
        .and_then(|formatted| {
            formatted.strip_suffix('Z').map(|value| format!("{value}.000Z"))
        })
        .unwrap_or_else(|| "1970-01-01T00:00:00.000Z".to_owned())
}

fn list_buckets_response(
    scope: &S3Scope,
    output: &ListBucketsOutput,
) -> EdgeResponse {
    let mut xml = XmlBuilder::new()
        .start("ListAllMyBucketsResult", Some(S3_XMLNS))
        .start("Owner", None)
        .elem("ID", scope.account_id().as_str())
        .elem("DisplayName", scope.account_id().as_str())
        .end("Owner")
        .start("Buckets", None);

    for bucket in &output.buckets {
        xml = xml
            .start("Bucket", None)
            .elem("Name", &bucket.name)
            .elem(
                "CreationDate",
                &iso_timestamp(bucket.created_at_epoch_seconds),
            )
            .end("Bucket");
    }

    let body =
        xml.end("Buckets").end("ListAllMyBucketsResult").build().into_bytes();

    EdgeResponse::bytes(200, "application/xml", body)
}

fn list_objects_response(output: &ListObjectsOutput) -> EdgeResponse {
    let mut xml = XmlBuilder::new()
        .start("ListBucketResult", Some(S3_XMLNS))
        .elem("Name", &output.bucket)
        .elem("Prefix", output.prefix.as_deref().unwrap_or_default())
        .elem("Marker", output.marker.as_deref().unwrap_or_default())
        .elem("MaxKeys", &output.max_keys.to_string())
        .elem("IsTruncated", bool_text(output.is_truncated));

    if let Some(delimiter) = output.delimiter.as_deref() {
        xml = xml.elem("Delimiter", delimiter);
    }

    if let Some(next_marker) = output.next_marker.as_deref() {
        xml = xml.elem("NextMarker", next_marker);
    }

    for object in &output.contents {
        xml = object_xml(xml, object);
    }
    for prefix in &output.common_prefixes {
        xml = xml
            .start("CommonPrefixes", None)
            .elem("Prefix", prefix)
            .end("CommonPrefixes");
    }

    EdgeResponse::bytes(
        200,
        "application/xml",
        xml.end("ListBucketResult").build().into_bytes(),
    )
}

fn list_objects_v2_response(output: &ListObjectsV2Output) -> EdgeResponse {
    let mut xml = XmlBuilder::new()
        .start("ListBucketResult", Some(S3_XMLNS))
        .elem("Name", &output.bucket)
        .elem("Prefix", output.prefix.as_deref().unwrap_or_default())
        .elem("KeyCount", &output.key_count.to_string())
        .elem("MaxKeys", &output.max_keys.to_string())
        .elem("IsTruncated", bool_text(output.is_truncated));

    if let Some(token) = output.continuation_token.as_deref() {
        xml = xml.elem("ContinuationToken", token);
    }
    if let Some(token) = output.next_continuation_token.as_deref() {
        xml = xml.elem("NextContinuationToken", token);
    }
    if let Some(start_after) = output.start_after.as_deref() {
        xml = xml.elem("StartAfter", start_after);
    }
    if let Some(delimiter) = output.delimiter.as_deref() {
        xml = xml.elem("Delimiter", delimiter);
    }

    for object in &output.contents {
        xml = object_xml(xml, object);
    }
    for prefix in &output.common_prefixes {
        xml = xml
            .start("CommonPrefixes", None)
            .elem("Prefix", prefix)
            .end("CommonPrefixes");
    }

    EdgeResponse::bytes(
        200,
        "application/xml",
        xml.end("ListBucketResult").build().into_bytes(),
    )
}

fn not_implemented_error() -> AwsError {
    AwsError::trusted_custom(
        AwsErrorFamily::UnsupportedOperation,
        "NotImplemented",
        "The requested S3 operation is not implemented.",
        501,
        true,
    )
}

fn object_response(object: GetObjectOutput) -> EdgeResponse {
    let head = object.head;
    let mut response =
        EdgeResponse::bytes(200, &head.content_type, object.body)
            .set_header("Content-Length", head.size.to_string())
            .set_header("ETag", head.etag.clone())
            .set_header(
                "Last-Modified",
                http_timestamp(head.last_modified_epoch_seconds),
            );

    if let Some(version_id) = head.version_id.clone() {
        response = response.set_header("x-amz-version-id", version_id);
    }

    for (name, value) in head.metadata.clone() {
        response = response.set_header(format!("x-amz-meta-{name}"), value);
    }

    apply_object_lock_headers(response, &head)
}

fn object_xml(
    mut xml: XmlBuilder,
    object: &services::ListedObject,
) -> XmlBuilder {
    xml = xml
        .start("Contents", None)
        .elem("Key", &object.key)
        .elem(
            "LastModified",
            &iso_timestamp(object.last_modified_epoch_seconds),
        )
        .elem("ETag", &object.etag)
        .elem("Size", &object.size.to_string())
        .elem("StorageClass", &object.storage_class)
        .end("Contents");
    xml
}

fn optional_query(query: &QueryParameters, name: &str) -> Option<String> {
    query.optional(name).map(str::to_owned)
}

fn required_query(
    query: &QueryParameters,
    name: &str,
    parameter: &str,
) -> Result<String, AwsError> {
    query.optional(name).map(str::to_owned).ok_or_else(|| {
        S3Error::InvalidArgument {
            code: "InvalidArgument",
            message: format!(
                "The request parameter `{parameter}` is not valid."
            ),
            status_code: 400,
        }
        .to_aws_error()
    })
}

fn optional_usize(
    query: &QueryParameters,
    name: &str,
) -> Result<Option<usize>, AwsError> {
    match query.optional(name) {
        Some(value) => value.parse::<usize>().map(Some).map_err(|_| {
            S3Error::InvalidArgument {
                code: "InvalidArgument",
                message: format!(
                    "The request parameter `{name}` is not valid."
                ),
                status_code: 400,
            }
            .to_aws_error()
        }),
        None => Ok(None),
    }
}

fn required_u16(
    query: &QueryParameters,
    name: &str,
    parameter: &str,
) -> Result<u16, AwsError> {
    let value = required_query(query, name, parameter)?;
    value.parse::<u16>().map_err(|_| {
        S3Error::InvalidArgument {
            code: "InvalidArgument",
            message: format!(
                "The request parameter `{parameter}` is not valid."
            ),
            status_code: 400,
        }
        .to_aws_error()
    })
}

fn parse_copy_source(
    request: &HttpRequest<'_>,
) -> Result<(String, String), AwsError> {
    let source = request
        .header("x-amz-copy-source")
        .ok_or_else(not_implemented_error)?
        .trim();
    let source = source.strip_prefix('/').unwrap_or(source);
    let (bucket, key) = source.split_once('/').ok_or_else(|| {
        S3Error::InvalidArgument {
            code: "InvalidArgument",
            message: "The copy source must include a bucket and key."
                .to_owned(),
            status_code: 400,
        }
        .to_aws_error()
    })?;

    Ok((percent_decode_path(bucket)?, percent_decode_path(key)?))
}

fn parse_bucket_acl(
    request: &HttpRequest<'_>,
) -> Result<StoredBucketAclInput, AwsError> {
    if let Some(acl) = request.header("x-amz-acl") {
        let Ok(acl) = acl.trim().parse::<CannedAcl>() else {
            return Err(S3Error::InvalidArgument {
                code: "InvalidArgument",
                message: "The canned ACL is not valid.".to_owned(),
                status_code: 400,
            }
            .to_aws_error());
        };
        return Ok(StoredBucketAclInput::Canned(acl));
    }

    if request.body().is_empty() {
        return Err(S3Error::InvalidArgument {
            code: "InvalidArgument",
            message: "The bucket ACL request did not contain a supported ACL."
                .to_owned(),
            status_code: 400,
        }
        .to_aws_error());
    }

    Ok(StoredBucketAclInput::Xml(request_body_utf8(request)?))
}

fn parse_create_bucket_object_lock_enabled(
    request: &HttpRequest<'_>,
) -> Result<bool, AwsError> {
    request
        .header("x-amz-bucket-object-lock-enabled")
        .map(|value| {
            parse_boolean_header(value, "x-amz-bucket-object-lock-enabled")
        })
        .transpose()
        .map(|value| value.unwrap_or(false))
}

fn request_object_lock(
    request: &HttpRequest<'_>,
) -> Result<Option<services::ObjectLockWriteOptions>, AwsError> {
    let legal_hold_status = request
        .header("x-amz-object-lock-legal-hold")
        .map(parse_object_lock_legal_hold_header)
        .transpose()?;
    let mode = request
        .header("x-amz-object-lock-mode")
        .map(parse_object_lock_mode_header)
        .transpose()?;
    let retain_until_epoch_seconds = request
        .header("x-amz-object-lock-retain-until-date")
        .map(parse_rfc3339_header)
        .transpose()?;

    if legal_hold_status.is_none()
        && mode.is_none()
        && retain_until_epoch_seconds.is_none()
    {
        return Ok(None);
    }

    Ok(Some(services::ObjectLockWriteOptions {
        legal_hold_status,
        mode,
        retain_until_epoch_seconds,
    }))
}

fn parse_bypass_governance_retention(
    request: &HttpRequest<'_>,
) -> Result<bool, AwsError> {
    request
        .header("x-amz-bypass-governance-retention")
        .map(|value| {
            parse_boolean_header(value, "x-amz-bypass-governance-retention")
        })
        .transpose()
        .map(|value| value.unwrap_or(false))
}

fn parse_create_bucket_region(
    request: &HttpRequest<'_>,
    scope: &S3Scope,
) -> Result<aws::RegionId, AwsError> {
    let body = request.body();
    if body.is_empty() {
        return Ok(scope.region().clone());
    }

    let body = std::str::from_utf8(body).map_err(|_| malformed_xml_error())?;
    let start_tag = "<LocationConstraint>";
    let end_tag = "</LocationConstraint>";
    let Some(start) =
        body.find(start_tag).map(|index| index + start_tag.len())
    else {
        return Ok(scope.region().clone());
    };
    let Some(end) = body[start..].find(end_tag).map(|index| start + index)
    else {
        return Err(malformed_xml_error());
    };
    let region = body[start..end].trim();

    if region.is_empty() {
        return Ok(scope.region().clone());
    }

    region.parse().map_err(|_| {
        S3Error::InvalidArgument {
            code: "InvalidLocationConstraint",
            message: "The specified location constraint is not valid."
                .to_owned(),
            status_code: 400,
        }
        .to_aws_error()
    })
}

fn request_body_utf8(request: &HttpRequest<'_>) -> Result<String, AwsError> {
    std::str::from_utf8(request.body())
        .map(str::to_owned)
        .map_err(|_| malformed_xml_error())
}

fn parse_boolean_header(value: &str, header: &str) -> Result<bool, AwsError> {
    match value.trim() {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => Err(S3Error::InvalidArgument {
            code: "InvalidArgument",
            message: format!("The header `{header}` is not valid."),
            status_code: 400,
        }
        .to_aws_error()),
    }
}

fn parse_object_lock_mode_header(
    value: &str,
) -> Result<ObjectLockMode, AwsError> {
    match value.trim() {
        "COMPLIANCE" => Ok(ObjectLockMode::Compliance),
        "GOVERNANCE" => Ok(ObjectLockMode::Governance),
        _ => Err(S3Error::InvalidArgument {
            code: "InvalidArgument",
            message: "Unknown wormMode directive.".to_owned(),
            status_code: 400,
        }
        .to_aws_error()),
    }
}

fn parse_object_lock_legal_hold_header(
    value: &str,
) -> Result<services::LegalHoldStatus, AwsError> {
    match value.trim() {
        "ON" => Ok(services::LegalHoldStatus::On),
        "OFF" => Ok(services::LegalHoldStatus::Off),
        _ => Err(S3Error::InvalidArgument {
            code: "InvalidArgument",
            message: "The header `x-amz-object-lock-legal-hold` is not valid."
                .to_owned(),
            status_code: 400,
        }
        .to_aws_error()),
    }
}

fn parse_rfc3339_header(value: &str) -> Result<u64, AwsError> {
    let timestamp =
        OffsetDateTime::parse(value.trim(), &Rfc3339).map_err(|_| {
            S3Error::InvalidArgument {
            code: "InvalidArgument",
            message:
                "The header `x-amz-object-lock-retain-until-date` is not valid."
                    .to_owned(),
            status_code: 400,
        }
        .to_aws_error()
        })?;
    Ok(timestamp.unix_timestamp().max(0) as u64)
}

fn malformed_xml_error() -> AwsError {
    AwsError::trusted_custom(
        AwsErrorFamily::Validation,
        "MalformedXML",
        "The XML you provided was not well-formed or did not validate against our published schema.",
        400,
        true,
    )
}

fn percent_decode_path(value: &str) -> Result<String, AwsError> {
    let bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;

    while let Some(&byte) = bytes.get(index) {
        match byte {
            b'%' => {
                let Some(&high_byte) = bytes.get(index + 1) else {
                    return Err(malformed_xml_error());
                };
                let Some(&low_byte) = bytes.get(index + 2) else {
                    return Err(malformed_xml_error());
                };
                decoded.push((hex(high_byte)? << 4) | hex(low_byte)?);
                index += 3;
            }
            other => {
                decoded.push(other);
                index += 1;
            }
        }
    }

    String::from_utf8(decoded).map_err(|_| malformed_xml_error())
}

fn request_metadata(request: &HttpRequest<'_>) -> BTreeMap<String, String> {
    request
        .headers()
        .filter_map(|(name, value)| {
            name.strip_prefix("x-amz-meta-")
                .map(|name| (name.to_owned(), value.to_owned()))
                .or_else(|| {
                    name.to_ascii_lowercase()
                        .strip_prefix("x-amz-meta-")
                        .map(|name| (name.to_owned(), value.to_owned()))
                })
        })
        .collect()
}

fn request_tags(
    request: &HttpRequest<'_>,
) -> Result<BTreeMap<String, String>, AwsError> {
    let Some(header) = request.header("x-amz-tagging") else {
        return Ok(BTreeMap::new());
    };
    let mut tags = BTreeMap::new();
    for pair in header.split('&').filter(|pair| !pair.is_empty()) {
        let (name, value) = pair.split_once('=').unwrap_or((pair, ""));
        let name = percent_decode_tag_component(name)?;
        let value = percent_decode_tag_component(value)?;
        if tags.insert(name, value).is_some() {
            return Err(S3Error::InvalidArgument {
                code: "InvalidTag",
                message: "The header 'x-amz-tagging' shall be encoded as UTF-8 then URLEncoded URL query parameters without tag name duplicates."
                    .to_owned(),
                status_code: 400,
            }
            .to_aws_error());
        }
    }

    Ok(tags)
}

fn bool_text(value: bool) -> &'static str {
    if value { "true" } else { "false" }
}

fn apply_object_lock_headers(
    mut response: EdgeResponse,
    object: &HeadObjectOutput,
) -> EdgeResponse {
    if let Some(status) = object.object_lock_legal_hold_status {
        response = response
            .set_header("x-amz-object-lock-legal-hold", status.as_str());
    }
    if let Some(mode) = object.object_lock_mode {
        response =
            response.set_header("x-amz-object-lock-mode", mode.as_str());
    }
    if let Some(retain_until_epoch_seconds) =
        object.object_lock_retain_until_epoch_seconds
    {
        response = response.set_header(
            "x-amz-object-lock-retain-until-date",
            rfc3339_timestamp(retain_until_epoch_seconds),
        );
    }
    response
}

fn rfc3339_timestamp(epoch_seconds: u64) -> String {
    OffsetDateTime::from_unix_timestamp(epoch_seconds as i64)
        .unwrap_or(OffsetDateTime::UNIX_EPOCH)
        .format(&Rfc3339)
        .unwrap_or_else(|_| "1970-01-01T00:00:00Z".to_owned())
}

fn encode_select_object_content_body(
    output: &SelectObjectContentOutput,
) -> Result<Vec<u8>, AwsError> {
    let mut body = Vec::new();
    if !output.records.is_empty() {
        write_message_to(
            &event_stream_message(
                "Records",
                Some("application/octet-stream"),
                output.records.as_bytes().to_vec(),
            ),
            &mut body,
        )
        .map_err(select_object_content_encode_error)?;
    }
    write_message_to(
        &event_stream_message(
            "Stats",
            Some("text/xml"),
            select_stats_xml(&output.stats).into_bytes(),
        ),
        &mut body,
    )
    .map_err(select_object_content_encode_error)?;
    write_message_to(
        &event_stream_message("End", None, Vec::new()),
        &mut body,
    )
    .map_err(select_object_content_encode_error)?;
    Ok(body)
}

fn select_object_content_encode_error(
    error: aws_smithy_eventstream::error::Error,
) -> AwsError {
    AwsError::trusted_custom(
        AwsErrorFamily::Internal,
        "InternalError",
        format!("Failed to encode S3 Select event stream response: {error}"),
        500,
        false,
    )
}

fn event_stream_message(
    event_type: &str,
    content_type: Option<&str>,
    payload: Vec<u8>,
) -> Message {
    let mut message = Message::new(payload)
        .add_header(Header::new(
            ":message-type",
            HeaderValue::String("event".into()),
        ))
        .add_header(Header::new(
            ":event-type",
            HeaderValue::String(event_type.to_owned().into()),
        ));
    if let Some(content_type) = content_type {
        message = message.add_header(Header::new(
            ":content-type",
            HeaderValue::String(content_type.to_owned().into()),
        ));
    }
    message
}

fn select_stats_xml(stats: &services::SelectObjectStats) -> String {
    XmlBuilder::new()
        .start("Stats", Some(""))
        .elem("BytesScanned", &stats.bytes_scanned.to_string())
        .elem("BytesProcessed", &stats.bytes_processed.to_string())
        .elem("BytesReturned", &stats.bytes_returned.to_string())
        .end("Stats")
        .build()
}

fn hex(value: u8) -> Result<u8, AwsError> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        b'A'..=b'F' => Ok(value - b'A' + 10),
        _ => Err(malformed_xml_error()),
    }
}

fn percent_decode_tag_component(value: &str) -> Result<String, AwsError> {
    let bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;

    while let Some(&byte) = bytes.get(index) {
        match byte {
            b'+' => {
                decoded.push(b' ');
                index += 1;
            }
            b'%' => {
                let Some(&high_byte) = bytes.get(index + 1) else {
                    return Err(malformed_xml_error());
                };
                let Some(&low_byte) = bytes.get(index + 2) else {
                    return Err(malformed_xml_error());
                };
                decoded.push((hex(high_byte)? << 4) | hex(low_byte)?);
                index += 3;
            }
            other => {
                decoded.push(other);
                index += 1;
            }
        }
    }

    String::from_utf8(decoded).map_err(|_| malformed_xml_error())
}

#[cfg(test)]
mod tests {
    use super::{AwsChunkedMode, EdgeRequest, normalize_request};
    use auth::{
        AwsChunkedSigningContext, VerifiedPayload, VerifiedRequest,
        VerifiedSignature,
    };
    use aws::{
        AccountId, Arn, ArnResource, AwsPrincipalType, CallerCredentialKind,
        CallerIdentity, CredentialScope, Partition, ServiceName,
        StableAwsPrincipal,
    };

    #[test]
    fn normalize_request_decodes_streaming_payload_and_strips_aws_chunked() {
        let mut request = EdgeRequest::new(
            "PUT",
            "/bucket/object.txt",
            vec![
                ("Host".to_owned(), "localhost".to_owned()),
                ("Content-Encoding".to_owned(), "aws-chunked,gzip".to_owned()),
                (
                    "X-Amz-Decoded-Content-Length".to_owned(),
                    "66560".to_owned(),
                ),
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
                (
                    "X-Amz-Decoded-Content-Length".to_owned(),
                    "66560".to_owned(),
                ),
                (
                    "X-Amz-Trailer".to_owned(),
                    "x-amz-checksum-crc32c".to_owned(),
                ),
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
                (
                    "X-Amz-Decoded-Content-Length".to_owned(),
                    "66560".to_owned(),
                ),
            ],
            payload_example_body(),
        );

        let error = normalize_request(&mut request, None)
            .expect_err("unsigned aws-chunked payloads must fail");

        assert_eq!(error.code(), "InvalidRequest");
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
                    Some(
                        "000000000000"
                            .parse()
                            .expect("account id should parse"),
                    ),
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
}
