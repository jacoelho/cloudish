pub(crate) use crate::aws_error_shape::AwsErrorShape;
use crate::query::QueryParameters;
use crate::request::HttpRequest;
#[cfg(test)]
pub(crate) use crate::request::EdgeRequest;
use crate::runtime::EdgeResponse;
use crate::s3_bucket_encoding::list_buckets_response;
use crate::s3_bucket_routes::handle_bucket_request;
#[cfg(test)]
pub(crate) use crate::s3_copy_parsing::parse_copy_source;
use crate::s3_error_encoding::s3_edge_target_error;
use crate::s3_object_routes::handle_object_request;
pub(crate) use crate::s3_request_normalization::{
    is_rest_xml_request, normalize_request,
};
pub(crate) use crate::s3_request_parsing::not_implemented_error;
use crate::xml::XmlBuilder;
use aws::{AdvertisedEdge, AwsError, parse_s3_edge_request_target};
use services::{S3Error, S3Scope, S3Service};

const S3_XMLNS: &str = "http://s3.amazonaws.com/doc/2006-03-01/";
const REQUEST_ID: &str = "0000000000000000";

#[derive(Debug)]
pub(crate) enum S3RouteError {
    Aws(AwsError),
    AwsWithHeaders { error: AwsError, headers: Vec<(String, String)> },
    Response(EdgeResponse),
}

impl From<AwsError> for S3RouteError {
    fn from(error: AwsError) -> Self {
        Self::Aws(error)
    }
}

impl From<S3Error> for S3RouteError {
    fn from(error: S3Error) -> Self {
        match error {
            S3Error::WrongRegion { region, .. } => {
                Self::Response(wrong_region_response(&region))
            }
            other => Self::Aws(other.to_aws_error()),
        }
    }
}

pub(crate) fn handle(
    request: &HttpRequest<'_>,
    scope: &S3Scope,
    s3: &S3Service,
    advertised_edge: &AdvertisedEdge,
) -> Result<EdgeResponse, AwsError> {
    match handle_inner(request, scope, s3, advertised_edge) {
        Ok(response) => Ok(response),
        Err(S3RouteError::Aws(error)) => Err(error),
        Err(S3RouteError::AwsWithHeaders { error, headers }) => {
            Ok(s3_error_response_with_headers(&error, &headers))
        }
        Err(S3RouteError::Response(response)) => Ok(response),
    }
}

fn handle_inner(
    request: &HttpRequest<'_>,
    scope: &S3Scope,
    s3: &S3Service,
    advertised_edge: &AdvertisedEdge,
) -> Result<EdgeResponse, S3RouteError> {
    let target = RequestTarget::parse(request, advertised_edge)?;

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

    Err(not_implemented_error().into())
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

pub(crate) fn s3_error_response(error: &AwsError) -> EdgeResponse {
    s3_error_response_with_headers(error, &[])
}

fn s3_error_response_with_headers(
    error: &AwsError,
    headers: &[(String, String)],
) -> EdgeResponse {
    let body = XmlBuilder::new()
        .start("Error", Some(S3_XMLNS))
        .elem("Code", error.code())
        .elem("Message", error.message())
        .elem("RequestId", REQUEST_ID)
        .end("Error")
        .build()
        .into_bytes();

    headers.iter().fold(
        EdgeResponse::bytes(error.status_code(), "application/xml", body),
        |response, (name, value)| response.set_header(name, value),
    )
}

fn wrong_region_response(region: &str) -> EdgeResponse {
    let body = XmlBuilder::new()
        .start("Error", Some(S3_XMLNS))
        .elem("Code", "IncorrectEndpoint")
        .elem(
            "Message",
            "The specified bucket exists in another Region. Direct requests to the correct endpoint.",
        )
        .elem("RequestId", REQUEST_ID)
        .end("Error")
        .build()
        .into_bytes();

    EdgeResponse::bytes(400, "application/xml", body)
        .set_header("x-amz-bucket-region", region)
}

#[derive(Debug)]
pub(crate) struct RequestTarget {
    pub(crate) bucket: Option<String>,
    pub(crate) key: Option<String>,
    pub(crate) query: QueryParameters,
}

impl RequestTarget {
    pub(crate) fn parse(
        request: &HttpRequest<'_>,
        advertised_edge: &AdvertisedEdge,
    ) -> Result<Self, AwsError> {
        let query = QueryParameters::parse(
            request.query_string().unwrap_or_default().as_bytes(),
        )?;
        let target = parse_s3_edge_request_target(
            advertised_edge,
            request.header("host").unwrap_or_default(),
            request.path_without_query(),
        )
        .map_err(s3_edge_target_error)?;

        Ok(Self {
            bucket: target.bucket().map(str::to_owned),
            key: target.key().map(str::to_owned),
            query,
        })
    }
}

#[cfg(test)]
#[path = "s3_tests.rs"]
mod tests;
