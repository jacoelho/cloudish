use crate::request::{EdgeRequest, HttpRequest};
use auth::{AwsChunkedMode, VerifiedPayload, VerifiedSignature};
use aws::{
    AdvertisedEdge, AwsError, AwsErrorFamily, S3EdgeHostRouting,
    classify_s3_edge_host,
};

pub(crate) fn is_rest_xml_request(
    request: &HttpRequest<'_>,
    advertised_edge: &AdvertisedEdge,
) -> bool {
    let path = request.path_without_query();
    let Some(host) = request.header("host") else {
        return false;
    };
    let host_routing = classify_s3_edge_host(advertised_edge, host);

    matches!(request.method(), "DELETE" | "GET" | "HEAD" | "POST" | "PUT")
        && !path.starts_with("/__")
        && matches!(
            host_routing,
            S3EdgeHostRouting::PathStyle
                | S3EdgeHostRouting::UnsupportedVirtualHostStyle
        )
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
