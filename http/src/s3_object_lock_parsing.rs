use crate::aws_error_shape::AwsErrorShape;
use crate::request::HttpRequest;
use aws::AwsError;
use services::{LegalHoldStatus, ObjectLockMode, S3Error};

pub(crate) fn parse_create_bucket_object_lock_enabled(
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

pub(crate) fn request_object_lock(
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

pub(crate) fn parse_bypass_governance_retention(
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
            message: "The object lock mode header is not valid.".to_owned(),
            status_code: 400,
        }
        .to_aws_error()),
    }
}

fn parse_object_lock_legal_hold_header(
    value: &str,
) -> Result<LegalHoldStatus, AwsError> {
    match value.trim() {
        "ON" => Ok(LegalHoldStatus::On),
        "OFF" => Ok(LegalHoldStatus::Off),
        _ => Err(S3Error::InvalidArgument {
            code: "InvalidArgument",
            message: "The object lock legal hold header is not valid."
                .to_owned(),
            status_code: 400,
        }
        .to_aws_error()),
    }
}

fn parse_rfc3339_header(value: &str) -> Result<u64, AwsError> {
    time::OffsetDateTime::parse(
        value.trim(),
        &time::format_description::well_known::Rfc3339,
    )
    .map_err(|_| {
        S3Error::InvalidArgument {
            code: "InvalidArgument",
            message: "The object lock retain-until header is not valid."
                .to_owned(),
            status_code: 400,
        }
        .to_aws_error()
    })
    .and_then(|timestamp| {
        u64::try_from(timestamp.unix_timestamp()).map_err(|_| {
            S3Error::InvalidArgument {
                code: "InvalidArgument",
                message: "The object lock retain-until header is not valid."
                    .to_owned(),
                status_code: 400,
            }
            .to_aws_error()
        })
    })
}
