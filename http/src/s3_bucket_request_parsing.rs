use crate::aws_error_shape::AwsErrorShape;
use crate::request::HttpRequest;
use crate::s3_request_parsing::{malformed_xml_error, request_body_utf8};
use aws::AwsError;
use services::{CannedAcl, S3Error, S3Scope, StoredBucketAclInput};

pub(crate) fn parse_bucket_acl(
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

pub(crate) fn parse_create_bucket_region(
    request: &HttpRequest<'_>,
    scope: &S3Scope,
) -> Result<aws::RegionId, AwsError> {
    let body = request.body();
    if body.is_empty() {
        return default_create_bucket_region(scope);
    }

    let body = std::str::from_utf8(body).map_err(|_| malformed_xml_error())?;
    let start_tag = "<LocationConstraint>";
    let end_tag = "</LocationConstraint>";
    let Some(start) =
        body.find(start_tag).map(|index| index + start_tag.len())
    else {
        return default_create_bucket_region(scope);
    };
    let Some(end) = body[start..].find(end_tag).map(|index| start + index)
    else {
        return Err(malformed_xml_error());
    };
    let region = body[start..end].trim();

    if region.is_empty() {
        return default_create_bucket_region(scope);
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

fn default_create_bucket_region(
    scope: &S3Scope,
) -> Result<aws::RegionId, AwsError> {
    if scope.region().as_str() == "us-east-1" {
        return Ok(scope.region().clone());
    }

    Err(S3Error::InvalidArgument {
        code: "InvalidLocationConstraint",
        message: "The specified location constraint is not valid.".to_owned(),
        status_code: 400,
    }
    .to_aws_error())
}
