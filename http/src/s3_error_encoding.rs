use crate::runtime::EdgeResponse;
use crate::s3::S3RouteError;
use crate::s3_object_responses::{
    current_delete_marker_route_error, head_current_delete_marker_response,
    head_requested_delete_marker_response,
    requested_delete_marker_route_error,
};
use aws::{AwsError, S3EdgeRequestTargetError};
use services::{HeadBucketOutput, S3Error};

use crate::s3_request_parsing::malformed_xml_error;
use crate::s3_response_encoding::invalid_request_error;

pub(crate) fn head_bucket_response(output: &HeadBucketOutput) -> EdgeResponse {
    match output {
        HeadBucketOutput::Found { region } => {
            EdgeResponse::empty(200).set_header("x-amz-bucket-region", region)
        }
        HeadBucketOutput::WrongRegion { region } => {
            EdgeResponse::empty(301).set_header("x-amz-bucket-region", region)
        }
        HeadBucketOutput::Forbidden { region } => {
            EdgeResponse::empty(403).set_header("x-amz-bucket-region", region)
        }
        HeadBucketOutput::Missing => EdgeResponse::empty(404),
    }
}

pub(crate) fn head_object_route_error(error: S3Error) -> S3RouteError {
    match error {
        S3Error::CurrentVersionIsDeleteMarker { version_id } => {
            S3RouteError::Response(head_current_delete_marker_response(
                version_id.as_deref(),
            ))
        }
        S3Error::RequestedVersionIsDeleteMarker {
            last_modified_epoch_seconds,
            version_id,
        } => S3RouteError::Response(head_requested_delete_marker_response(
            last_modified_epoch_seconds,
            &version_id,
        )),
        other => S3RouteError::from(other),
    }
}

pub(crate) fn get_object_route_error(error: S3Error) -> S3RouteError {
    match error {
        S3Error::CurrentVersionIsDeleteMarker { version_id } => {
            current_delete_marker_route_error(version_id.as_deref())
        }
        S3Error::RequestedVersionIsDeleteMarker {
            last_modified_epoch_seconds,
            version_id,
        } => requested_delete_marker_route_error(
            last_modified_epoch_seconds,
            &version_id,
        ),
        other => S3RouteError::from(other),
    }
}

pub(crate) fn s3_edge_target_error(
    error: S3EdgeRequestTargetError,
) -> AwsError {
    match error {
        S3EdgeRequestTargetError::InvalidPercentEncoding
        | S3EdgeRequestTargetError::InvalidUtf8 => malformed_xml_error(),
        S3EdgeRequestTargetError::UnsupportedHost
        | S3EdgeRequestTargetError::UnsupportedVirtualHostStyle => {
            invalid_request_error(
                "S3 requests must use the advertised edge host with path-style URLs.",
            )
        }
    }
}
