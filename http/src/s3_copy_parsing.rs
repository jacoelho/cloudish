use crate::aws_error_shape::AwsErrorShape;
use crate::request::HttpRequest;
use aws::AwsError;
use services::S3Error;

pub(crate) fn parse_copy_source(
    request: &HttpRequest<'_>,
) -> Result<(String, String, Option<String>), AwsError> {
    let source = request
        .header("x-amz-copy-source")
        .ok_or_else(crate::s3_request_parsing::not_implemented_error)?
        .trim();
    let (source, query) = source.split_once('?').unwrap_or((source, ""));
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

    let source_version_id = if query.is_empty() {
        None
    } else {
        parse_copy_source_version_id(query)?
    };

    Ok((
        percent_decode_path(bucket)?,
        percent_decode_path(key)?,
        source_version_id,
    ))
}

fn parse_copy_source_version_id(
    query: &str,
) -> Result<Option<String>, AwsError> {
    let mut version_id = None;

    for pair in query.split('&') {
        let (name, value) = pair.split_once('=').ok_or_else(|| {
            S3Error::InvalidArgument {
                code: "InvalidArgument",
                message: "The copy source query string is not valid."
                    .to_owned(),
                status_code: 400,
            }
            .to_aws_error()
        })?;
        if name != "versionId" || version_id.is_some() {
            return Err(S3Error::InvalidArgument {
                code: "InvalidArgument",
                message: "The copy source query string is not valid."
                    .to_owned(),
                status_code: 400,
            }
            .to_aws_error());
        }
        version_id = Some(percent_decode_path(value)?);
    }

    Ok(version_id)
}

pub(crate) fn percent_decode_path(value: &str) -> Result<String, AwsError> {
    let decoded = urlencoding::decode(value).map_err(|_| {
        S3Error::InvalidArgument {
            code: "InvalidArgument",
            message: "The request path is not valid.".to_owned(),
            status_code: 400,
        }
        .to_aws_error()
    })?;
    Ok(decoded.into_owned())
}
