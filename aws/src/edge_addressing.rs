use crate::{AccountId, Arn, RegionId};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::{Arc, RwLock};

pub const AWS_PATH_PREFIX: &str = "/__aws";
const EXECUTE_API_PREFIX: &str = "/__aws/execute-api/";
const LAMBDA_URL_PREFIX: &str = "/__aws/lambda-url/";

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

#[cfg(test)]
mod tests {
    use super::{
        AWS_PATH_PREFIX, AdvertisedEdge, AdvertisedEdgeTemplate,
        parse_reserved_execute_api_path,
        parse_reserved_lambda_function_url_path,
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
}
