use crate::{AccountId, Arn, RegionId};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::{Arc, RwLock};

pub const AWS_PATH_PREFIX: &str = "/__aws";
const EXECUTE_API_PREFIX: &str = "/__aws/execute-api/";
const LAMBDA_URL_PREFIX: &str = "/__aws/lambda-url/";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3EdgeRequestTarget {
    bucket: Option<String>,
    key: Option<String>,
}

impl S3EdgeRequestTarget {
    pub fn bucket(&self) -> Option<&str> {
        self.bucket.as_deref()
    }

    pub fn key(&self) -> Option<&str> {
        self.key.as_deref()
    }

    pub fn into_parts(self) -> (Option<String>, Option<String>) {
        (self.bucket, self.key)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum S3EdgeRequestTargetError {
    InvalidPercentEncoding,
    InvalidUtf8,
    UnsupportedHost,
    UnsupportedVirtualHostStyle,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum S3EdgeHostRouting {
    PathStyle,
    UnsupportedHost,
    UnsupportedVirtualHostStyle,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdvertisedEdge {
    public_host: String,
    public_port: u16,
    scheme: String,
}

impl AdvertisedEdge {
    pub fn new(
        scheme: impl Into<String>,
        public_host: impl Into<String>,
        public_port: u16,
    ) -> Self {
        Self {
            public_host: public_host.into(),
            public_port,
            scheme: scheme.into(),
        }
    }

    pub fn localhost(public_port: u16) -> Self {
        Self::new("http", "localhost", public_port)
    }

    pub fn authority(&self) -> String {
        format!("{}:{}", format_host(&self.public_host), self.public_port)
    }

    pub fn execute_api_endpoint(&self, api_id: &str) -> String {
        format!("{}/__aws/execute-api/{api_id}", self.origin())
    }

    pub fn cognito_issuer(&self, user_pool_id: &str) -> String {
        format!("{}/{user_pool_id}", self.origin())
    }

    pub fn cognito_jwks_uri(&self, user_pool_id: &str) -> String {
        format!("{}/.well-known/jwks.json", self.cognito_issuer(user_pool_id))
    }

    pub fn lambda_function_domain_name(&self) -> String {
        self.authority()
    }

    pub fn lambda_function_url(
        &self,
        region: &RegionId,
        url_id: &str,
    ) -> String {
        format!(
            "{}/__aws/lambda-url/{}/{url_id}/",
            self.origin(),
            region.as_str(),
        )
    }

    pub fn origin(&self) -> String {
        format!("{}://{}", self.scheme, self.authority())
    }

    pub fn public_host(&self) -> &str {
        &self.public_host
    }

    pub fn public_port(&self) -> u16 {
        self.public_port
    }

    pub fn scheme(&self) -> &str {
        &self.scheme
    }

    pub fn sns_confirm_subscription_url(
        &self,
        topic_arn: &Arn,
        token: &str,
    ) -> String {
        format!(
            "{}/?Action=ConfirmSubscription&TopicArn={}&Token={}",
            self.origin(),
            urlencoding::encode(&topic_arn.to_string()),
            urlencoding::encode(token),
        )
    }

    pub fn sns_unsubscribe_url(&self, subscription_arn: &Arn) -> String {
        format!(
            "{}/?Action=Unsubscribe&SubscriptionArn={}",
            self.origin(),
            urlencoding::encode(&subscription_arn.to_string()),
        )
    }

    pub fn sns_signing_cert_url(&self) -> String {
        format!("{}/__aws/sns/signing-cert.pem", self.origin())
    }

    pub fn sqs_queue_url(
        &self,
        account_id: &AccountId,
        queue_name: &str,
    ) -> String {
        format!("{}/{account_id}/{queue_name}", self.origin())
    }
}

impl Default for AdvertisedEdge {
    fn default() -> Self {
        Self::localhost(4566)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdvertisedEdgeTemplate {
    public_host: String,
    public_port: Option<u16>,
    scheme: String,
}

impl AdvertisedEdgeTemplate {
    pub fn new(
        scheme: impl Into<String>,
        public_host: impl Into<String>,
        public_port: Option<u16>,
    ) -> Self {
        Self {
            public_host: public_host.into(),
            public_port,
            scheme: scheme.into(),
        }
    }

    pub fn localhost(public_port: Option<u16>) -> Self {
        Self::new("http", "localhost", public_port)
    }

    pub fn resolve(&self, bound_port: u16) -> AdvertisedEdge {
        AdvertisedEdge::new(
            self.scheme.clone(),
            self.public_host.clone(),
            self.public_port.unwrap_or(bound_port),
        )
    }
}

impl Default for AdvertisedEdgeTemplate {
    fn default() -> Self {
        Self::localhost(Some(4566))
    }
}

#[derive(Clone)]
pub struct SharedAdvertisedEdge {
    edge: Arc<RwLock<AdvertisedEdge>>,
}

impl SharedAdvertisedEdge {
    pub fn new(edge: AdvertisedEdge) -> Self {
        Self { edge: Arc::new(RwLock::new(edge)) }
    }

    pub fn current(&self) -> AdvertisedEdge {
        self.edge.read().unwrap_or_else(|poison| poison.into_inner()).clone()
    }

    pub fn update(&self, edge: AdvertisedEdge) {
        *self.edge.write().unwrap_or_else(|poison| poison.into_inner()) = edge;
    }
}

impl Default for SharedAdvertisedEdge {
    fn default() -> Self {
        Self::new(AdvertisedEdge::default())
    }
}

impl fmt::Debug for SharedAdvertisedEdge {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SharedAdvertisedEdge")
            .field("edge", &self.current())
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReservedExecuteApiPath<'a> {
    api_id: &'a str,
    request_path: String,
}

impl<'a> ReservedExecuteApiPath<'a> {
    pub fn api_id(&self) -> &'a str {
        self.api_id
    }

    pub fn request_path(&self) -> &str {
        &self.request_path
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReservedLambdaFunctionUrlPath<'a> {
    region: RegionId,
    request_path: String,
    url_id: &'a str,
}

impl ReservedLambdaFunctionUrlPath<'_> {
    pub fn region(&self) -> &RegionId {
        &self.region
    }

    pub fn request_path(&self) -> &str {
        &self.request_path
    }

    pub fn url_id(&self) -> &str {
        self.url_id
    }
}

pub fn parse_reserved_execute_api_path(
    path: &str,
) -> Option<ReservedExecuteApiPath<'_>> {
    let remainder = path.strip_prefix(EXECUTE_API_PREFIX)?;
    let (api_id, request_path) = split_route_identity(remainder);
    if api_id.is_empty() {
        return None;
    }

    Some(ReservedExecuteApiPath { api_id, request_path })
}

pub fn parse_reserved_lambda_function_url_path(
    path: &str,
) -> Option<ReservedLambdaFunctionUrlPath<'_>> {
    let remainder = path.strip_prefix(LAMBDA_URL_PREFIX)?;
    let (region, remainder) = remainder.split_once('/')?;
    let region = region.parse::<RegionId>().ok()?;
    let (url_id, request_path) = split_route_identity(remainder);
    if url_id.is_empty() {
        return None;
    }

    Some(ReservedLambdaFunctionUrlPath { region, request_path, url_id })
}

/// # Errors
///
/// Returns [`S3EdgeRequestTargetError`] when the request targets an
/// unsupported host or when the bucket or key cannot be percent-decoded into a
/// valid UTF-8 string.
pub fn parse_s3_edge_request_target(
    advertised_edge: &AdvertisedEdge,
    host: &str,
    path: &str,
) -> Result<S3EdgeRequestTarget, S3EdgeRequestTargetError> {
    match classify_s3_edge_host(advertised_edge, host) {
        S3EdgeHostRouting::PathStyle => path_style_target(path),
        S3EdgeHostRouting::UnsupportedHost => {
            Err(S3EdgeRequestTargetError::UnsupportedHost)
        }
        S3EdgeHostRouting::UnsupportedVirtualHostStyle => {
            Err(S3EdgeRequestTargetError::UnsupportedVirtualHostStyle)
        }
    }
}

pub fn classify_s3_edge_host(
    advertised_edge: &AdvertisedEdge,
    host: &str,
) -> S3EdgeHostRouting {
    let normalized_host = normalize_host(host);
    let public_host = normalize_host(advertised_edge.public_host());
    let public_host_suffix = format!(".{public_host}");
    if normalized_host == public_host {
        return S3EdgeHostRouting::PathStyle;
    }
    let advertised_virtual_host = if public_host == "localhost" {
        is_single_label_bucket_host_suffix(
            &normalized_host,
            &public_host_suffix,
        )
    } else {
        is_bucket_host_suffix(&normalized_host, &public_host_suffix)
    };
    if advertised_virtual_host
        || is_bucket_host_suffix(&normalized_host, ".s3.localhost")
    {
        return S3EdgeHostRouting::UnsupportedVirtualHostStyle;
    }

    S3EdgeHostRouting::UnsupportedHost
}

fn split_route_identity(remainder: &str) -> (&str, String) {
    match remainder.split_once('/') {
        Some((identity, path)) if !path.is_empty() => {
            (identity, format!("/{path}"))
        }
        Some((identity, _)) => (identity, "/".to_owned()),
        None => (remainder, "/".to_owned()),
    }
}

fn format_host(host: &str) -> String {
    if host.contains(':') && !host.starts_with('[') {
        return format!("[{host}]");
    }

    host.to_owned()
}

fn path_style_target(
    path: &str,
) -> Result<S3EdgeRequestTarget, S3EdgeRequestTargetError> {
    let path = path.strip_prefix('/').unwrap_or(path);
    if path.is_empty() {
        return Ok(S3EdgeRequestTarget { bucket: None, key: None });
    }

    let (bucket, key) = match path.split_once('/') {
        Some((bucket, "")) => (bucket, None),
        Some((bucket, key)) => (bucket, Some(percent_decode_path(key)?)),
        None => (path, None),
    };

    Ok(S3EdgeRequestTarget { bucket: Some(percent_decode_path(bucket)?), key })
}

fn is_bucket_host_suffix(host: &str, suffix: &str) -> bool {
    host.strip_suffix(suffix).is_some_and(|bucket| !bucket.is_empty())
}

fn is_single_label_bucket_host_suffix(host: &str, suffix: &str) -> bool {
    host.strip_suffix(suffix)
        .is_some_and(|bucket| !bucket.is_empty() && !bucket.contains('.'))
}

fn normalize_host(host: &str) -> String {
    let host = host.trim();
    if let Some(stripped) = host.strip_prefix('[')
        && let Some((value, _)) = stripped.split_once(']')
    {
        return value.to_ascii_lowercase();
    }

    host.rsplit_once(':')
        .filter(|(value, port)| {
            !value.is_empty()
                && !value.contains(':')
                && port.parse::<u16>().is_ok()
        })
        .map(|(value, _)| value)
        .unwrap_or(host)
        .to_ascii_lowercase()
}

fn percent_decode_path(
    value: &str,
) -> Result<String, S3EdgeRequestTargetError> {
    let bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;

    while let Some(&byte) = bytes.get(index) {
        match byte {
            b'%' => {
                let Some(high_index) = index.checked_add(1) else {
                    return Err(
                        S3EdgeRequestTargetError::InvalidPercentEncoding,
                    );
                };
                let Some(&high_byte) = bytes.get(high_index) else {
                    return Err(
                        S3EdgeRequestTargetError::InvalidPercentEncoding,
                    );
                };
                let Some(low_index) = index.checked_add(2) else {
                    return Err(
                        S3EdgeRequestTargetError::InvalidPercentEncoding,
                    );
                };
                let Some(&low_byte) = bytes.get(low_index) else {
                    return Err(
                        S3EdgeRequestTargetError::InvalidPercentEncoding,
                    );
                };
                decoded
                    .push((hex_value(high_byte)? << 4) | hex_value(low_byte)?);
                index = index
                    .checked_add(3)
                    .ok_or(S3EdgeRequestTargetError::InvalidPercentEncoding)?;
            }
            other => {
                decoded.push(other);
                index = index
                    .checked_add(1)
                    .ok_or(S3EdgeRequestTargetError::InvalidPercentEncoding)?;
            }
        }
    }

    String::from_utf8(decoded)
        .map_err(|_| S3EdgeRequestTargetError::InvalidUtf8)
}

fn hex_value(byte: u8) -> Result<u8, S3EdgeRequestTargetError> {
    char::from(byte)
        .to_digit(16)
        .and_then(|value| u8::try_from(value).ok())
        .ok_or(S3EdgeRequestTargetError::InvalidPercentEncoding)
}

#[cfg(test)]
mod tests {
    use super::{
        AWS_PATH_PREFIX, AdvertisedEdge, AdvertisedEdgeTemplate,
        S3EdgeHostRouting, S3EdgeRequestTargetError, classify_s3_edge_host,
        parse_reserved_execute_api_path,
        parse_reserved_lambda_function_url_path, parse_s3_edge_request_target,
    };

    #[test]
    fn advertised_edge_builds_public_urls() {
        let edge = AdvertisedEdge::new("http", "127.0.0.1", 4510);

        assert_eq!(edge.origin(), "http://127.0.0.1:4510");
        assert_eq!(
            edge.execute_api_endpoint("abc123"),
            "http://127.0.0.1:4510/__aws/execute-api/abc123"
        );
        assert_eq!(
            edge.lambda_function_url(
                &"eu-west-2".parse().expect("region should parse"),
                "url123",
            ),
            "http://127.0.0.1:4510/__aws/lambda-url/eu-west-2/url123/"
        );
    }

    #[test]
    fn advertised_edge_template_uses_bound_port_when_unset() {
        let edge = AdvertisedEdgeTemplate::new("http", "localhost", None)
            .resolve(4873);

        assert_eq!(edge.origin(), "http://localhost:4873");
    }

    #[test]
    fn parse_reserved_execute_api_path_extracts_api_id_and_inner_path() {
        let route = parse_reserved_execute_api_path(
            "/__aws/execute-api/abc123/dev/orders",
        )
        .expect("execute-api path should parse");

        assert_eq!(route.api_id(), "abc123");
        assert_eq!(route.request_path(), "/dev/orders");
    }

    #[test]
    fn parse_reserved_execute_api_path_defaults_to_root() {
        let route =
            parse_reserved_execute_api_path("/__aws/execute-api/abc123")
                .expect("execute-api path should parse");

        assert_eq!(route.request_path(), "/");
    }

    #[test]
    fn parse_reserved_lambda_function_url_path_extracts_route_parts() {
        let route = parse_reserved_lambda_function_url_path(
            "/__aws/lambda-url/eu-west-2/url123/invoke/test",
        )
        .expect("lambda function URL path should parse");

        assert_eq!(route.region().as_str(), "eu-west-2");
        assert_eq!(route.url_id(), "url123");
        assert_eq!(route.request_path(), "/invoke/test");
    }

    #[test]
    fn aws_path_prefix_is_reserved() {
        assert_eq!(AWS_PATH_PREFIX, "/__aws");
    }

    #[test]
    fn parse_s3_edge_request_target_supports_public_host_path_style_urls() {
        let target = parse_s3_edge_request_target(
            &AdvertisedEdge::new("http", "cloudish.test", 4566),
            "cloudish.test:4566",
            "/demo/reports/data%20file.txt",
        )
        .expect("path-style target should parse");

        assert_eq!(target.bucket(), Some("demo"));
        assert_eq!(target.key(), Some("reports/data file.txt"));
    }

    #[test]
    fn parse_s3_edge_request_target_rejects_public_host_virtual_hosts() {
        let error = parse_s3_edge_request_target(
            &AdvertisedEdge::new("http", "cloudish.test", 4566),
            "demo.cloudish.test:4566",
            "/reports/data%20file.txt",
        )
        .expect_err("virtual-host target should fail");

        assert_eq!(
            error,
            S3EdgeRequestTargetError::UnsupportedVirtualHostStyle
        );
    }

    #[test]
    fn parse_s3_edge_request_target_rejects_localhost_compatibility_host() {
        let error = parse_s3_edge_request_target(
            &AdvertisedEdge::new("http", "cloudish.test", 4566),
            "demo.s3.localhost:4566",
            "/reports/data.txt",
        )
        .expect_err("localhost compatibility target should fail");

        assert_eq!(
            error,
            S3EdgeRequestTargetError::UnsupportedVirtualHostStyle
        );
    }

    #[test]
    fn parse_s3_edge_request_target_rejects_unrelated_s3_domains() {
        let error = parse_s3_edge_request_target(
            &AdvertisedEdge::new("http", "cloudish.test", 4566),
            "demo.s3.evil.example.com:4566",
            "/",
        )
        .expect_err("unrelated domains should fail");

        assert_eq!(error, S3EdgeRequestTargetError::UnsupportedHost);
    }

    #[test]
    fn classify_s3_edge_host_accepts_only_the_advertised_path_style_host() {
        let edge = AdvertisedEdge::new("http", "cloudish.test", 4566);

        assert_eq!(
            classify_s3_edge_host(&edge, "cloudish.test:4566"),
            S3EdgeHostRouting::PathStyle
        );
        assert_eq!(
            classify_s3_edge_host(&edge, "demo.cloudish.test:4566"),
            S3EdgeHostRouting::UnsupportedVirtualHostStyle
        );
        assert_eq!(
            classify_s3_edge_host(&edge, "api.example.test:4566"),
            S3EdgeHostRouting::UnsupportedHost
        );
    }

    #[test]
    fn classify_s3_edge_host_treats_multi_label_localhost_hosts_as_non_s3() {
        let edge = AdvertisedEdge::localhost(4566);

        assert_eq!(
            classify_s3_edge_host(&edge, "demo.localhost:4566"),
            S3EdgeHostRouting::UnsupportedVirtualHostStyle
        );
        assert_eq!(
            classify_s3_edge_host(&edge, "apiid.execute-api.localhost:4566",),
            S3EdgeHostRouting::UnsupportedHost
        );
    }
}
