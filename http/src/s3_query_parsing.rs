use crate::aws_error_shape::AwsErrorShape;
use crate::query::QueryParameters;
use aws::AwsError;
use services::S3Error;

pub(crate) fn optional_query(
    query: &QueryParameters,
    name: &str,
) -> Option<String> {
    query.optional(name).map(str::to_owned)
}

pub(crate) fn reject_unsupported_query_parameter(
    query: &QueryParameters,
    name: &str,
    message: &str,
) -> Result<(), crate::s3::S3RouteError> {
    if query.optional(name).is_some() {
        return Err(crate::s3_request_parsing::not_implemented_message(
            message,
        )
        .into());
    }

    Ok(())
}

pub(crate) fn required_query(
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

pub(crate) fn optional_usize(
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

pub(crate) fn required_u16(
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
