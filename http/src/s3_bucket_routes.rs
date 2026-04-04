use crate::request::HttpRequest;
use crate::runtime::EdgeResponse;
use crate::s3::{RequestTarget, S3RouteError};
use crate::s3_bucket_encoding::{
    bucket_tagging_response, bucket_versioning_response,
    get_bucket_location_response, list_multipart_uploads_response,
    list_object_versions_response, list_objects_response,
    list_objects_v2_response, notification_configuration_response,
    object_lock_configuration_response,
};
use crate::s3_bucket_request_parsing::{
    parse_bucket_acl, parse_create_bucket_region,
};
use crate::s3_error_encoding::head_bucket_response;
use crate::s3_object_lock_parsing::parse_create_bucket_object_lock_enabled;
use crate::s3_query_parsing::{
    optional_query, optional_usize, reject_unsupported_query_parameter,
};
use crate::s3_request_parsing::{not_implemented_error, request_body_utf8};
use crate::s3_response_encoding::empty_xml_response;
use crate::s3_xml::{
    parse_bucket_notification_configuration, parse_bucket_versioning,
    parse_object_lock_configuration, parse_tagging,
};
use bytes::Bytes;
use services::{
    CreateBucketInput, HeadBucketInput, ListObjectVersionsInput,
    ListObjectsInput, ListObjectsV2Input, S3Scope, S3Service,
};

pub(crate) fn handle_bucket_request(
    request: &HttpRequest<'_>,
    target: &RequestTarget,
    scope: &S3Scope,
    s3: &S3Service,
) -> Result<Option<EdgeResponse>, S3RouteError> {
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
                .map_err(S3RouteError::from)?;

            EdgeResponse::bytes(200, "application/xml", Vec::new()).set_header(
                "Location",
                format!("/{bucket}", bucket = created.name),
            )
        }
        "DELETE" => {
            s3.delete_bucket(scope, bucket).map_err(S3RouteError::from)?;
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
                .map_err(S3RouteError::from)?;
            list_objects_v2_response(&output)
        }
        "HEAD" => {
            let output = s3.head_bucket(
                scope,
                bucket,
                &HeadBucketInput {
                    expected_bucket_owner: request
                        .header("x-amz-expected-bucket-owner")
                        .map(str::to_owned),
                },
            );
            head_bucket_response(&output)
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
                .map_err(S3RouteError::from)?;
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
) -> Result<Option<EdgeResponse>, S3RouteError> {
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
        reject_unsupported_query_parameter(
            &target.query,
            "key-marker",
            "ListMultipartUploads pagination markers are not implemented.",
        )?;
        reject_unsupported_query_parameter(
            &target.query,
            "upload-id-marker",
            "ListMultipartUploads pagination markers are not implemented.",
        )?;
        return match request.method() {
            "GET" => {
                let uploads = s3
                    .list_multipart_uploads(scope, bucket)
                    .map_err(S3RouteError::from)?;
                Ok(Some(list_multipart_uploads_response(&uploads)))
            }
            _ => Ok(None),
        };
    }
    if target.query.optional("versions").is_some() {
        reject_unsupported_query_parameter(
            &target.query,
            "key-marker",
            "ListObjectVersions pagination markers are not implemented.",
        )?;
        reject_unsupported_query_parameter(
            &target.query,
            "version-id-marker",
            "ListObjectVersions pagination markers are not implemented.",
        )?;
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
                    .map_err(S3RouteError::from)?;
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
                    .map_err(S3RouteError::from)?;
                Ok(Some(get_bucket_location_response(&location)))
            }
            _ => Ok(None),
        };
    }

    Ok(None)
}

fn handle_bucket_versioning_request(
    request: &HttpRequest<'_>,
    scope: &S3Scope,
    s3: &S3Service,
    bucket: &str,
) -> Result<EdgeResponse, S3RouteError> {
    match request.method() {
        "PUT" => {
            let status = parse_bucket_versioning(Bytes::copy_from_slice(
                request.body(),
            ))?;
            s3.put_bucket_versioning(scope, bucket, status)
                .map_err(S3RouteError::from)?;
            Ok(empty_xml_response(200))
        }
        "GET" => {
            let output = s3
                .get_bucket_versioning(scope, bucket)
                .map_err(S3RouteError::from)?;
            Ok(bucket_versioning_response(&output))
        }
        _ => Err(not_implemented_error().into()),
    }
}

fn handle_bucket_notification_request(
    request: &HttpRequest<'_>,
    scope: &S3Scope,
    s3: &S3Service,
    bucket: &str,
) -> Result<EdgeResponse, S3RouteError> {
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
            .map_err(S3RouteError::from)?;
            Ok(empty_xml_response(200))
        }
        "GET" => {
            let output = s3
                .get_bucket_notification_configuration(scope, bucket)
                .map_err(S3RouteError::from)?;
            Ok(notification_configuration_response(&output))
        }
        _ => Err(not_implemented_error().into()),
    }
}

fn handle_bucket_object_lock_request(
    request: &HttpRequest<'_>,
    scope: &S3Scope,
    s3: &S3Service,
    bucket: &str,
) -> Result<EdgeResponse, S3RouteError> {
    match request.method() {
        "PUT" => {
            let configuration = parse_object_lock_configuration(
                Bytes::copy_from_slice(request.body()),
            )?;
            s3.put_object_lock_configuration(scope, bucket, configuration)
                .map_err(S3RouteError::from)?;
            Ok(empty_xml_response(200))
        }
        "GET" => {
            let output = s3
                .get_object_lock_configuration(scope, bucket)
                .map_err(S3RouteError::from)?;
            Ok(object_lock_configuration_response(&output))
        }
        _ => Err(not_implemented_error().into()),
    }
}

fn handle_bucket_tagging_request(
    request: &HttpRequest<'_>,
    scope: &S3Scope,
    s3: &S3Service,
    bucket: &str,
) -> Result<EdgeResponse, S3RouteError> {
    match request.method() {
        "PUT" => {
            let tags = parse_tagging(Bytes::copy_from_slice(request.body()))?;
            s3.put_bucket_tagging(scope, bucket, tags)
                .map_err(S3RouteError::from)?;
            Ok(empty_xml_response(200))
        }
        "GET" => {
            let output = s3
                .get_bucket_tagging(scope, bucket)
                .map_err(S3RouteError::from)?;
            Ok(bucket_tagging_response(&output))
        }
        "DELETE" => {
            s3.delete_bucket_tagging(scope, bucket)
                .map_err(S3RouteError::from)?;
            Ok(empty_xml_response(204))
        }
        _ => Err(not_implemented_error().into()),
    }
}

fn handle_bucket_policy_request(
    request: &HttpRequest<'_>,
    scope: &S3Scope,
    s3: &S3Service,
    bucket: &str,
) -> Result<EdgeResponse, S3RouteError> {
    match request.method() {
        "PUT" => {
            let policy = request_body_utf8(request)?;
            s3.put_bucket_policy(scope, bucket, policy)
                .map_err(S3RouteError::from)?;
            Ok(empty_xml_response(204))
        }
        "GET" => {
            let policy = s3
                .get_bucket_policy(scope, bucket)
                .map_err(S3RouteError::from)?;
            Ok(EdgeResponse::bytes(
                200,
                "application/json",
                policy.into_bytes(),
            ))
        }
        "DELETE" => {
            s3.delete_bucket_policy(scope, bucket)
                .map_err(S3RouteError::from)?;
            Ok(empty_xml_response(204))
        }
        _ => Err(not_implemented_error().into()),
    }
}

fn handle_bucket_cors_request(
    request: &HttpRequest<'_>,
    scope: &S3Scope,
    s3: &S3Service,
    bucket: &str,
) -> Result<EdgeResponse, S3RouteError> {
    match request.method() {
        "PUT" => {
            let body = request_body_utf8(request)?;
            s3.put_bucket_cors(scope, bucket, body)
                .map_err(S3RouteError::from)?;
            Ok(empty_xml_response(200))
        }
        "GET" => {
            let cors = s3
                .get_bucket_cors(scope, bucket)
                .map_err(S3RouteError::from)?;
            Ok(EdgeResponse::bytes(200, "application/xml", cors.into_bytes()))
        }
        "DELETE" => {
            s3.delete_bucket_cors(scope, bucket)
                .map_err(S3RouteError::from)?;
            Ok(empty_xml_response(204))
        }
        _ => Err(not_implemented_error().into()),
    }
}

fn handle_bucket_lifecycle_request(
    request: &HttpRequest<'_>,
    scope: &S3Scope,
    s3: &S3Service,
    bucket: &str,
) -> Result<EdgeResponse, S3RouteError> {
    match request.method() {
        "PUT" => {
            let body = request_body_utf8(request)?;
            s3.put_bucket_lifecycle(scope, bucket, body)
                .map_err(S3RouteError::from)?;
            Ok(empty_xml_response(200))
        }
        "GET" => {
            let lifecycle = s3
                .get_bucket_lifecycle(scope, bucket)
                .map_err(S3RouteError::from)?;
            Ok(EdgeResponse::bytes(
                200,
                "application/xml",
                lifecycle.into_bytes(),
            ))
        }
        "DELETE" => {
            s3.delete_bucket_lifecycle(scope, bucket)
                .map_err(S3RouteError::from)?;
            Ok(empty_xml_response(204))
        }
        _ => Err(not_implemented_error().into()),
    }
}

fn handle_bucket_encryption_request(
    request: &HttpRequest<'_>,
    scope: &S3Scope,
    s3: &S3Service,
    bucket: &str,
) -> Result<EdgeResponse, S3RouteError> {
    match request.method() {
        "PUT" => {
            let body = request_body_utf8(request)?;
            s3.put_bucket_encryption(scope, bucket, body)
                .map_err(S3RouteError::from)?;
            Ok(empty_xml_response(200))
        }
        "GET" => {
            let encryption = s3
                .get_bucket_encryption(scope, bucket)
                .map_err(S3RouteError::from)?;
            Ok(EdgeResponse::bytes(
                200,
                "application/xml",
                encryption.into_bytes(),
            ))
        }
        "DELETE" => {
            s3.delete_bucket_encryption(scope, bucket)
                .map_err(S3RouteError::from)?;
            Ok(empty_xml_response(204))
        }
        _ => Err(not_implemented_error().into()),
    }
}

fn handle_bucket_acl_request(
    request: &HttpRequest<'_>,
    scope: &S3Scope,
    s3: &S3Service,
    bucket: &str,
) -> Result<EdgeResponse, S3RouteError> {
    match request.method() {
        "PUT" => {
            let acl = parse_bucket_acl(request)?;
            s3.put_bucket_acl(scope, bucket, acl)
                .map_err(S3RouteError::from)?;
            Ok(empty_xml_response(200))
        }
        "GET" => {
            let acl = s3
                .get_bucket_acl(scope, bucket)
                .map_err(S3RouteError::from)?;
            Ok(EdgeResponse::bytes(200, "application/xml", acl.into_bytes()))
        }
        _ => Err(not_implemented_error().into()),
    }
}
