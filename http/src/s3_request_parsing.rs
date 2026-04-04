use crate::request::HttpRequest;
use aws::{AwsError, AwsErrorFamily};

pub(crate) fn request_body_utf8(
    request: &HttpRequest<'_>,
) -> Result<String, AwsError> {
    std::str::from_utf8(request.body())
        .map(str::to_owned)
        .map_err(|_| malformed_xml_error())
}

pub(crate) fn not_implemented_error() -> AwsError {
    not_implemented_message("The requested S3 operation is not implemented.")
}

pub(crate) fn not_implemented_message(message: impl Into<String>) -> AwsError {
    AwsError::trusted_custom(
        AwsErrorFamily::UnsupportedOperation,
        "NotImplemented",
        message,
        501,
        true,
    )
}

pub(crate) fn malformed_xml_error() -> AwsError {
    AwsError::trusted_custom(
        AwsErrorFamily::Validation,
        "MalformedXML",
        "The XML you provided was not well-formed or did not validate against our published schema.",
        400,
        true,
    )
}
