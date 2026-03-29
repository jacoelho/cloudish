use super::{
    ApiGatewayError, ApiGatewayScope, ApiGatewayService, ExecuteApiError,
    ExecuteApiIntegrationPlan, ExecuteApiInvocation,
    ExecuteApiLambdaProxyPlan, ExecuteApiPreparedResponse, ExecuteApiRequest,
    HTTP_API_AUTHORIZER_ID_WIDTH, HTTP_DEPLOYMENT_ID_WIDTH,
    HTTP_INTEGRATION_ID_WIDTH, HTTP_ROUTE_ID_WIDTH, ROOT_PATH,
    StoredApiGatewayState, next_identifier, optional_empty_to_none, paginate,
    validate_name, validate_stage_name, validate_tags,
};
use crate::http_api::{
    HttpApiRouteDefinition, MatchedHttpRoute, match_http_route,
    normalize_route_key, select_http_stage,
};
use aws::{
    AccountId, AdvertisedEdge, CallerIdentity, Endpoint, ExecuteApiSourceArn,
    HttpForwardRequest, LambdaFunctionTarget, RegionId,
};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use time::OffsetDateTime;
use time::format_description::parse;
use time::format_description::well_known::Rfc3339;

const DEFAULT_HTTP_API_IP_ADDRESS_TYPE: &str = "ipv4";
const DEFAULT_HTTP_ROUTE_SELECTION_EXPRESSION: &str =
    "${request.method} ${request.path}";
const DEFAULT_HTTP_AUTHORIZATION_TYPE: &str = "NONE";
const DEFAULT_HTTP_CONNECTION_TYPE: &str = "INTERNET";
const DEFAULT_HTTP_PAYLOAD_FORMAT_VERSION: &str = "2.0";
const EXECUTE_API_REQUEST_ID: &str = "0000000000000000";

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HttpApi {
    pub api_endpoint: String,
    pub api_id: String,
    pub created_date: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub disable_execute_api_endpoint: bool,
    pub ip_address_type: String,
    pub name: String,
    pub protocol_type: String,
    pub route_selection_expression: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub tags: BTreeMap<String, String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HttpApiCollection<T> {
    pub items: Vec<T>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_token: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HttpApiRoute {
    pub api_key_required: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub authorization_scopes: Vec<String>,
    pub authorization_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub authorizer_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operation_name: Option<String>,
    pub route_id: String,
    pub route_key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HttpApiIntegration {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub connection_id: Option<String>,
    pub connection_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub credentials_arn: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub integration_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub integration_method: Option<String>,
    pub integration_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub integration_uri: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload_format_version: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub request_parameters: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub request_templates: BTreeMap<String, String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub template_selection_expression: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout_in_millis: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JwtConfiguration {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub audience: Vec<String>,
    pub issuer: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HttpApiAuthorizer {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub authorizer_credentials_arn: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub authorizer_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub authorizer_payload_format_version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub authorizer_result_ttl_in_seconds: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub authorizer_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub authorizer_uri: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub enable_simple_responses: Option<bool>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub identity_source: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub identity_validation_expression: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub jwt_configuration: Option<JwtConfiguration>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HttpApiStage {
    pub auto_deploy: bool,
    pub created_date: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deployment_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub last_updated_date: String,
    pub stage_name: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub stage_variables: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub tags: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HttpApiDeployment {
    pub auto_deployed: bool,
    pub created_date: String,
    pub deployment_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub deployment_status: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateHttpApiInput {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub disable_execute_api_endpoint: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ip_address_type: Option<String>,
    pub name: String,
    pub protocol_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub route_selection_expression: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub tags: BTreeMap<String, String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateHttpApiInput {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub disable_execute_api_endpoint: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ip_address_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub route_selection_expression: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateHttpApiRouteInput {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub authorization_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub authorizer_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operation_name: Option<String>,
    pub route_key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateHttpApiRouteInput {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub authorization_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub authorizer_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operation_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub route_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateHttpApiIntegrationInput {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub connection_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub connection_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub credentials_arn: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub integration_method: Option<String>,
    pub integration_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub integration_uri: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload_format_version: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub request_parameters: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub request_templates: BTreeMap<String, String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub template_selection_expression: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout_in_millis: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateHttpApiIntegrationInput {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub connection_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub connection_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub credentials_arn: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub integration_method: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub integration_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub integration_uri: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload_format_version: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub request_parameters: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub request_templates: BTreeMap<String, String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub template_selection_expression: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout_in_millis: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateHttpApiAuthorizerInput {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub authorizer_credentials_arn: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub authorizer_payload_format_version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub authorizer_result_ttl_in_seconds: Option<u32>,
    pub authorizer_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub authorizer_uri: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub enable_simple_responses: Option<bool>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub identity_source: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub identity_validation_expression: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub jwt_configuration: Option<JwtConfiguration>,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateHttpApiAuthorizerInput {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub authorizer_credentials_arn: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub authorizer_payload_format_version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub authorizer_result_ttl_in_seconds: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub authorizer_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub authorizer_uri: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub enable_simple_responses: Option<bool>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub identity_source: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub identity_validation_expression: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub jwt_configuration: Option<JwtConfiguration>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateHttpApiStageInput {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auto_deploy: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deployment_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub stage_name: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub stage_variables: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub tags: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateHttpApiStageInput {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auto_deploy: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deployment_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub stage_variables: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub tags: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateHttpApiDeploymentInput {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stage_name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct StoredHttpApi {
    api_id: String,
    created_date: u64,
    description: Option<String>,
    disable_execute_api_endpoint: bool,
    ip_address_type: String,
    name: String,
    protocol_type: String,
    route_selection_expression: String,
    tags: BTreeMap<String, String>,
    version: Option<String>,
    routes: BTreeMap<String, StoredHttpRoute>,
    integrations: BTreeMap<String, StoredHttpIntegration>,
    authorizers: BTreeMap<String, StoredHttpAuthorizer>,
    deployments: BTreeMap<String, StoredHttpDeployment>,
    stages: BTreeMap<String, StoredHttpStage>,
}

impl StoredHttpApi {
    fn to_api(
        &self,
        _scope: &ApiGatewayScope,
        advertised_edge: &AdvertisedEdge,
    ) -> Result<HttpApi, ApiGatewayError> {
        Ok(HttpApi {
            api_endpoint: advertised_edge.execute_api_endpoint(&self.api_id),
            api_id: self.api_id.clone(),
            created_date: format_timestamp(self.created_date)?,
            description: self.description.clone(),
            disable_execute_api_endpoint: self.disable_execute_api_endpoint,
            ip_address_type: self.ip_address_type.clone(),
            name: self.name.clone(),
            protocol_type: self.protocol_type.clone(),
            route_selection_expression: self
                .route_selection_expression
                .clone(),
            tags: self.tags.clone(),
            version: self.version.clone(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct StoredHttpRoute {
    api_key_required: bool,
    authorization_scopes: Vec<String>,
    authorization_type: String,
    authorizer_id: Option<String>,
    operation_name: Option<String>,
    route_id: String,
    route_key: String,
    target: Option<String>,
}

impl StoredHttpRoute {
    fn to_route(&self) -> HttpApiRoute {
        HttpApiRoute {
            api_key_required: self.api_key_required,
            authorization_scopes: self.authorization_scopes.clone(),
            authorization_type: self.authorization_type.clone(),
            authorizer_id: self.authorizer_id.clone(),
            operation_name: self.operation_name.clone(),
            route_id: self.route_id.clone(),
            route_key: self.route_key.clone(),
            target: self.target.clone(),
        }
    }
}

impl HttpApiRouteDefinition for StoredHttpRoute {
    fn authorization_type(&self) -> &str {
        &self.authorization_type
    }

    fn route_id(&self) -> &str {
        &self.route_id
    }

    fn route_key(&self) -> &str {
        &self.route_key
    }

    fn target(&self) -> Option<&str> {
        self.target.as_deref()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct StoredHttpIntegration {
    connection_id: Option<String>,
    connection_type: String,
    credentials_arn: Option<String>,
    description: Option<String>,
    integration_id: String,
    integration_method: Option<String>,
    integration_type: String,
    integration_uri: Option<String>,
    payload_format_version: Option<String>,
    request_parameters: BTreeMap<String, String>,
    request_templates: BTreeMap<String, String>,
    template_selection_expression: Option<String>,
    timeout_in_millis: Option<u32>,
}

impl StoredHttpIntegration {
    fn to_integration(&self) -> HttpApiIntegration {
        HttpApiIntegration {
            connection_id: self.connection_id.clone(),
            connection_type: self.connection_type.clone(),
            credentials_arn: self.credentials_arn.clone(),
            description: self.description.clone(),
            integration_id: Some(self.integration_id.clone()),
            integration_method: self.integration_method.clone(),
            integration_type: self.integration_type.clone(),
            integration_uri: self.integration_uri.clone(),
            payload_format_version: self.payload_format_version.clone(),
            request_parameters: self.request_parameters.clone(),
            request_templates: self.request_templates.clone(),
            template_selection_expression: self
                .template_selection_expression
                .clone(),
            timeout_in_millis: self.timeout_in_millis,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct StoredHttpAuthorizer {
    authorizer_credentials_arn: Option<String>,
    authorizer_id: String,
    authorizer_payload_format_version: Option<String>,
    authorizer_result_ttl_in_seconds: Option<u32>,
    authorizer_type: String,
    authorizer_uri: Option<String>,
    enable_simple_responses: Option<bool>,
    identity_source: Vec<String>,
    identity_validation_expression: Option<String>,
    jwt_configuration: Option<JwtConfiguration>,
    name: String,
}

impl StoredHttpAuthorizer {
    fn to_authorizer(&self) -> HttpApiAuthorizer {
        HttpApiAuthorizer {
            authorizer_credentials_arn: self
                .authorizer_credentials_arn
                .clone(),
            authorizer_id: Some(self.authorizer_id.clone()),
            authorizer_payload_format_version: self
                .authorizer_payload_format_version
                .clone(),
            authorizer_result_ttl_in_seconds: self
                .authorizer_result_ttl_in_seconds,
            authorizer_type: Some(self.authorizer_type.clone()),
            authorizer_uri: self.authorizer_uri.clone(),
            enable_simple_responses: self.enable_simple_responses,
            identity_source: self.identity_source.clone(),
            identity_validation_expression: self
                .identity_validation_expression
                .clone(),
            jwt_configuration: self.jwt_configuration.clone(),
            name: Some(self.name.clone()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct StoredHttpStage {
    auto_deploy: bool,
    created_date: u64,
    deployment_id: Option<String>,
    description: Option<String>,
    last_updated_date: u64,
    stage_name: String,
    stage_variables: BTreeMap<String, String>,
    tags: BTreeMap<String, String>,
}

impl StoredHttpStage {
    fn to_stage(&self) -> Result<HttpApiStage, ApiGatewayError> {
        Ok(HttpApiStage {
            auto_deploy: self.auto_deploy,
            created_date: format_timestamp(self.created_date)?,
            deployment_id: self.deployment_id.clone(),
            description: self.description.clone(),
            last_updated_date: format_timestamp(self.last_updated_date)?,
            stage_name: self.stage_name.clone(),
            stage_variables: self.stage_variables.clone(),
            tags: self.tags.clone(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct StoredHttpDeployment {
    auto_deployed: bool,
    created_date: u64,
    deployment_id: String,
    description: Option<String>,
    snapshot: StoredHttpDeploymentSnapshot,
}

impl StoredHttpDeployment {
    fn to_deployment(&self) -> Result<HttpApiDeployment, ApiGatewayError> {
        Ok(HttpApiDeployment {
            auto_deployed: self.auto_deployed,
            created_date: format_timestamp(self.created_date)?,
            deployment_id: self.deployment_id.clone(),
            description: self.description.clone(),
            deployment_status: "DEPLOYED".to_owned(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct StoredHttpDeploymentSnapshot {
    authorizers: BTreeMap<String, StoredHttpAuthorizer>,
    integrations: BTreeMap<String, StoredHttpIntegration>,
    routes: BTreeMap<String, StoredHttpRoute>,
}

impl ApiGatewayService {
    #[doc = "# Errors\n\nReturns `ApiGatewayError` when the request fails validation, referenced resources are missing, or the API Gateway HTTP control-plane state cannot be loaded or persisted."]
    pub fn create_http_api(
        &self,
        scope: &ApiGatewayScope,
        input: CreateHttpApiInput,
    ) -> Result<HttpApi, ApiGatewayError> {
        validate_name("name", &input.name)?;
        validate_http_api_protocol_type(&input.protocol_type)?;
        validate_tags(&input.tags)?;
        let route_selection_expression = normalize_route_selection_expression(
            input.route_selection_expression,
        )?;
        let ip_address_type =
            normalize_ip_address_type(input.ip_address_type.as_deref())?;

        let _guard = self.lock_state();
        let mut state = self.load_state(scope);
        let api_id = self.allocate_execute_api_id()?;
        let api = StoredHttpApi {
            api_id: api_id.clone(),
            created_date: self.now_epoch_seconds(),
            description: optional_empty_to_none(input.description),
            disable_execute_api_endpoint: input
                .disable_execute_api_endpoint
                .unwrap_or(false),
            ip_address_type,
            name: input.name,
            protocol_type: input.protocol_type,
            route_selection_expression,
            tags: input.tags,
            version: optional_empty_to_none(input.version),
            routes: BTreeMap::new(),
            integrations: BTreeMap::new(),
            authorizers: BTreeMap::new(),
            deployments: BTreeMap::new(),
            stages: BTreeMap::new(),
        };
        let response = api.to_api(scope, &self.advertised_edge.current())?;
        state.http_apis.insert(api_id, api);
        self.save_state(scope, state)?;

        Ok(response)
    }

    #[doc = "# Errors\n\nReturns `ApiGatewayError` when the request fails validation, referenced resources are missing, or the API Gateway HTTP control-plane state cannot be loaded or persisted."]
    pub fn get_http_api(
        &self,
        scope: &ApiGatewayScope,
        api_id: &str,
    ) -> Result<HttpApi, ApiGatewayError> {
        let state = self.load_state(scope);
        http_api(&state, api_id)?
            .to_api(scope, &self.advertised_edge.current())
    }

    #[doc = "# Errors\n\nReturns `ApiGatewayError` when the request fails validation, referenced resources are missing, or the API Gateway HTTP control-plane state cannot be loaded or persisted."]
    pub fn get_http_apis(
        &self,
        scope: &ApiGatewayScope,
        next_token: Option<&str>,
        max_results: Option<u32>,
    ) -> Result<HttpApiCollection<HttpApi>, ApiGatewayError> {
        let state = self.load_state(scope);
        let apis = state
            .http_apis
            .values()
            .map(|api| api.to_api(scope, &self.advertised_edge.current()))
            .collect::<Result<Vec<_>, _>>()?;

        paginate_http_collection(apis, next_token, max_results)
    }

    #[doc = "# Errors\n\nReturns `ApiGatewayError` when the request fails validation, referenced resources are missing, or the API Gateway HTTP control-plane state cannot be loaded or persisted."]
    pub fn update_http_api(
        &self,
        scope: &ApiGatewayScope,
        api_id: &str,
        input: UpdateHttpApiInput,
    ) -> Result<HttpApi, ApiGatewayError> {
        let mut state = self.load_state(scope);
        let api = http_api_mut(&mut state, api_id)?;
        if let Some(name) = input.name {
            validate_name("name", &name)?;
            api.name = name;
        }
        if let Some(description) = input.description {
            api.description = optional_empty_to_none(Some(description));
        }
        if let Some(disable_execute_api_endpoint) =
            input.disable_execute_api_endpoint
        {
            api.disable_execute_api_endpoint = disable_execute_api_endpoint;
        }
        if let Some(ip_address_type) = input.ip_address_type {
            api.ip_address_type =
                normalize_ip_address_type(Some(ip_address_type.as_str()))?;
        }
        if let Some(route_selection_expression) =
            input.route_selection_expression
        {
            api.route_selection_expression =
                normalize_route_selection_expression(Some(
                    route_selection_expression,
                ))?;
        }
        if let Some(version) = input.version {
            api.version = optional_empty_to_none(Some(version));
        }

        let response = api.to_api(scope, &self.advertised_edge.current())?;
        self.save_state(scope, state)?;
        Ok(response)
    }

    #[doc = "# Errors\n\nReturns `ApiGatewayError` when the request fails validation, referenced resources are missing, or the API Gateway HTTP control-plane state cannot be loaded or persisted."]
    pub fn delete_http_api(
        &self,
        scope: &ApiGatewayScope,
        api_id: &str,
    ) -> Result<(), ApiGatewayError> {
        let mut state = self.load_state(scope);
        if state.http_apis.remove(api_id).is_none() {
            return Err(ApiGatewayError::NotFound {
                resource: format!("API {api_id}"),
            });
        }
        self.save_state(scope, state)
    }

    #[doc = "# Errors\n\nReturns `ApiGatewayError` when the request fails validation, referenced resources are missing, or the API Gateway HTTP control-plane state cannot be loaded or persisted."]
    pub fn create_http_api_route(
        &self,
        scope: &ApiGatewayScope,
        api_id: &str,
        input: CreateHttpApiRouteInput,
    ) -> Result<HttpApiRoute, ApiGatewayError> {
        let mut state = self.load_state(scope);
        let route_id = next_identifier(
            &mut state.next_http_route_sequence,
            HTTP_ROUTE_ID_WIDTH,
        );
        let api = http_api_mut(&mut state, api_id)?;
        let route_key = normalize_route_key(&input.route_key)?;
        validate_route_dependencies(
            api,
            input.authorization_type.as_deref(),
            input.authorizer_id.as_deref(),
            input.target.as_deref(),
        )?;
        ensure_unique_route_key(api, &route_key, None)?;

        let route = StoredHttpRoute {
            api_key_required: false,
            authorization_scopes: Vec::new(),
            authorization_type: input
                .authorization_type
                .unwrap_or_else(|| DEFAULT_HTTP_AUTHORIZATION_TYPE.to_owned()),
            authorizer_id: input.authorizer_id,
            operation_name: optional_empty_to_none(input.operation_name),
            route_id: route_id.clone(),
            route_key,
            target: input.target,
        };
        let response = route.to_route();
        api.routes.insert(route_id, route);
        self.save_state(scope, state)?;
        Ok(response)
    }

    #[doc = "# Errors\n\nReturns `ApiGatewayError` when the request fails validation, referenced resources are missing, or the API Gateway HTTP control-plane state cannot be loaded or persisted."]
    pub fn get_http_api_route(
        &self,
        scope: &ApiGatewayScope,
        api_id: &str,
        route_id: &str,
    ) -> Result<HttpApiRoute, ApiGatewayError> {
        let state = self.load_state(scope);
        let api = http_api(&state, api_id)?;
        Ok(http_route(api, route_id)?.to_route())
    }

    #[doc = "# Errors\n\nReturns `ApiGatewayError` when the request fails validation, referenced resources are missing, or the API Gateway HTTP control-plane state cannot be loaded or persisted."]
    pub fn get_http_api_routes(
        &self,
        scope: &ApiGatewayScope,
        api_id: &str,
        next_token: Option<&str>,
        max_results: Option<u32>,
    ) -> Result<HttpApiCollection<HttpApiRoute>, ApiGatewayError> {
        let state = self.load_state(scope);
        let api = http_api(&state, api_id)?;
        let routes = api
            .routes
            .values()
            .map(StoredHttpRoute::to_route)
            .collect::<Vec<_>>();

        paginate_http_collection(routes, next_token, max_results)
    }

    #[doc = "# Errors\n\nReturns `ApiGatewayError` when the request fails validation, referenced resources are missing, or the API Gateway HTTP control-plane state cannot be loaded or persisted."]
    pub fn update_http_api_route(
        &self,
        scope: &ApiGatewayScope,
        api_id: &str,
        route_id: &str,
        input: UpdateHttpApiRouteInput,
    ) -> Result<HttpApiRoute, ApiGatewayError> {
        let mut state = self.load_state(scope);
        let api = http_api_mut(&mut state, api_id)?;
        let next_route_key =
            input.route_key.as_deref().map(normalize_route_key).transpose()?;
        let next_authorization_type =
            input.authorization_type.as_deref().or_else(|| {
                http_route(api, route_id)
                    .ok()
                    .map(|route| route.authorization_type.as_str())
            });
        let next_authorizer_id =
            input.authorizer_id.as_deref().or_else(|| {
                http_route(api, route_id)
                    .ok()
                    .and_then(|route| route.authorizer_id.as_deref())
            });
        let next_target = input.target.as_deref().or_else(|| {
            http_route(api, route_id)
                .ok()
                .and_then(|route| route.target.as_deref())
        });
        validate_route_dependencies(
            api,
            next_authorization_type,
            next_authorizer_id,
            next_target,
        )?;
        if let Some(route_key) = next_route_key.as_deref() {
            ensure_unique_route_key(api, route_key, Some(route_id))?;
            let current_route = http_route(api, route_id)?;
            if current_route.route_key == "$default" || route_key == "$default"
            {
                return Err(ApiGatewayError::Validation {
                    message: "the $default route key cannot be modified"
                        .to_owned(),
                });
            }
        }

        let route = http_route_mut(api, route_id)?;
        if let Some(route_key) = next_route_key {
            route.route_key = route_key;
        }
        if let Some(authorization_type) = input.authorization_type {
            route.authorization_type = authorization_type;
        }
        if input.authorizer_id.is_some() {
            route.authorizer_id = input.authorizer_id;
        }
        if input.operation_name.is_some() {
            route.operation_name =
                optional_empty_to_none(input.operation_name);
        }
        if input.target.is_some() {
            route.target = input.target;
        }

        let response = route.to_route();
        self.save_state(scope, state)?;
        Ok(response)
    }

    #[doc = "# Errors\n\nReturns `ApiGatewayError` when the request fails validation, referenced resources are missing, or the API Gateway HTTP control-plane state cannot be loaded or persisted."]
    pub fn delete_http_api_route(
        &self,
        scope: &ApiGatewayScope,
        api_id: &str,
        route_id: &str,
    ) -> Result<(), ApiGatewayError> {
        let mut state = self.load_state(scope);
        let api = http_api_mut(&mut state, api_id)?;
        if api.routes.remove(route_id).is_none() {
            return Err(ApiGatewayError::NotFound {
                resource: format!("Route {route_id}"),
            });
        }
        self.save_state(scope, state)
    }

    #[doc = "# Errors\n\nReturns `ApiGatewayError` when the request fails validation, referenced resources are missing, or the API Gateway HTTP control-plane state cannot be loaded or persisted."]
    pub fn create_http_api_integration(
        &self,
        scope: &ApiGatewayScope,
        api_id: &str,
        input: CreateHttpApiIntegrationInput,
    ) -> Result<HttpApiIntegration, ApiGatewayError> {
        validate_http_integration_input(&input)?;
        let mut state = self.load_state(scope);
        let integration_id = next_identifier(
            &mut state.next_http_integration_sequence,
            HTTP_INTEGRATION_ID_WIDTH,
        );
        let api = http_api_mut(&mut state, api_id)?;
        let integration = StoredHttpIntegration {
            connection_id: input.connection_id,
            connection_type: input
                .connection_type
                .unwrap_or_else(|| DEFAULT_HTTP_CONNECTION_TYPE.to_owned()),
            credentials_arn: input.credentials_arn,
            description: optional_empty_to_none(input.description),
            integration_id: integration_id.clone(),
            integration_method: input.integration_method,
            integration_type: input.integration_type,
            integration_uri: input.integration_uri,
            payload_format_version: input.payload_format_version,
            request_parameters: input.request_parameters,
            request_templates: input.request_templates,
            template_selection_expression: input.template_selection_expression,
            timeout_in_millis: input.timeout_in_millis,
        };
        let response = integration.to_integration();
        api.integrations.insert(integration_id, integration);
        self.save_state(scope, state)?;
        Ok(response)
    }

    #[doc = "# Errors\n\nReturns `ApiGatewayError` when the request fails validation, referenced resources are missing, or the API Gateway HTTP control-plane state cannot be loaded or persisted."]
    pub fn get_http_api_integration(
        &self,
        scope: &ApiGatewayScope,
        api_id: &str,
        integration_id: &str,
    ) -> Result<HttpApiIntegration, ApiGatewayError> {
        let state = self.load_state(scope);
        let api = http_api(&state, api_id)?;
        Ok(http_integration(api, integration_id)?.to_integration())
    }

    #[doc = "# Errors\n\nReturns `ApiGatewayError` when the request fails validation, referenced resources are missing, or the API Gateway HTTP control-plane state cannot be loaded or persisted."]
    pub fn get_http_api_integrations(
        &self,
        scope: &ApiGatewayScope,
        api_id: &str,
        next_token: Option<&str>,
        max_results: Option<u32>,
    ) -> Result<HttpApiCollection<HttpApiIntegration>, ApiGatewayError> {
        let state = self.load_state(scope);
        let api = http_api(&state, api_id)?;
        let integrations = api
            .integrations
            .values()
            .map(StoredHttpIntegration::to_integration)
            .collect::<Vec<_>>();

        paginate_http_collection(integrations, next_token, max_results)
    }

    #[doc = "# Errors\n\nReturns `ApiGatewayError` when the request fails validation, referenced resources are missing, or the API Gateway HTTP control-plane state cannot be loaded or persisted."]
    pub fn update_http_api_integration(
        &self,
        scope: &ApiGatewayScope,
        api_id: &str,
        integration_id: &str,
        input: UpdateHttpApiIntegrationInput,
    ) -> Result<HttpApiIntegration, ApiGatewayError> {
        let mut state = self.load_state(scope);
        let api = http_api_mut(&mut state, api_id)?;
        let mut next = http_integration(api, integration_id)?.clone();
        if let Some(connection_id) = input.connection_id {
            next.connection_id = Some(connection_id);
        }
        if let Some(connection_type) = input.connection_type {
            next.connection_type = connection_type;
        }
        if let Some(credentials_arn) = input.credentials_arn {
            next.credentials_arn = Some(credentials_arn);
        }
        if let Some(description) = input.description {
            next.description = optional_empty_to_none(Some(description));
        }
        if let Some(integration_method) = input.integration_method {
            next.integration_method = Some(integration_method);
        }
        if let Some(integration_type) = input.integration_type {
            next.integration_type = integration_type;
        }
        if let Some(integration_uri) = input.integration_uri {
            next.integration_uri = Some(integration_uri);
        }
        if let Some(payload_format_version) = input.payload_format_version {
            next.payload_format_version = Some(payload_format_version);
        }
        if !input.request_parameters.is_empty() {
            next.request_parameters = input.request_parameters;
        }
        if !input.request_templates.is_empty() {
            next.request_templates = input.request_templates;
        }
        if let Some(template_selection_expression) =
            input.template_selection_expression
        {
            next.template_selection_expression =
                Some(template_selection_expression);
        }
        if let Some(timeout_in_millis) = input.timeout_in_millis {
            next.timeout_in_millis = Some(timeout_in_millis);
        }
        validate_stored_http_integration(&next)?;

        let integration = http_integration_mut(api, integration_id)?;
        *integration = next;
        let response = integration.to_integration();
        self.save_state(scope, state)?;
        Ok(response)
    }

    #[doc = "# Errors\n\nReturns `ApiGatewayError` when the request fails validation, referenced resources are missing, or the API Gateway HTTP control-plane state cannot be loaded or persisted."]
    pub fn delete_http_api_integration(
        &self,
        scope: &ApiGatewayScope,
        api_id: &str,
        integration_id: &str,
    ) -> Result<(), ApiGatewayError> {
        let mut state = self.load_state(scope);
        let api = http_api_mut(&mut state, api_id)?;
        if api.integrations.remove(integration_id).is_none() {
            return Err(ApiGatewayError::NotFound {
                resource: format!("Integration {integration_id}"),
            });
        }
        self.save_state(scope, state)
    }

    #[doc = "# Errors\n\nReturns `ApiGatewayError` when the request fails validation, referenced resources are missing, or the API Gateway HTTP control-plane state cannot be loaded or persisted."]
    pub fn create_http_api_authorizer(
        &self,
        scope: &ApiGatewayScope,
        api_id: &str,
        input: CreateHttpApiAuthorizerInput,
    ) -> Result<HttpApiAuthorizer, ApiGatewayError> {
        validate_http_authorizer_input(&input)?;
        let mut state = self.load_state(scope);
        let authorizer_id = next_identifier(
            &mut state.next_http_authorizer_sequence,
            HTTP_API_AUTHORIZER_ID_WIDTH,
        );
        let api = http_api_mut(&mut state, api_id)?;
        let authorizer = StoredHttpAuthorizer {
            authorizer_credentials_arn: input.authorizer_credentials_arn,
            authorizer_id: authorizer_id.clone(),
            authorizer_payload_format_version: input
                .authorizer_payload_format_version,
            authorizer_result_ttl_in_seconds: input
                .authorizer_result_ttl_in_seconds,
            authorizer_type: input.authorizer_type,
            authorizer_uri: input.authorizer_uri,
            enable_simple_responses: input.enable_simple_responses,
            identity_source: input.identity_source,
            identity_validation_expression: input
                .identity_validation_expression,
            jwt_configuration: input.jwt_configuration,
            name: input.name,
        };
        let response = authorizer.to_authorizer();
        api.authorizers.insert(authorizer_id, authorizer);
        self.save_state(scope, state)?;
        Ok(response)
    }

    #[doc = "# Errors\n\nReturns `ApiGatewayError` when the request fails validation, referenced resources are missing, or the API Gateway HTTP control-plane state cannot be loaded or persisted."]
    pub fn get_http_api_authorizer(
        &self,
        scope: &ApiGatewayScope,
        api_id: &str,
        authorizer_id: &str,
    ) -> Result<HttpApiAuthorizer, ApiGatewayError> {
        let state = self.load_state(scope);
        let api = http_api(&state, api_id)?;
        Ok(http_authorizer(api, authorizer_id)?.to_authorizer())
    }

    #[doc = "# Errors\n\nReturns `ApiGatewayError` when the request fails validation, referenced resources are missing, or the API Gateway HTTP control-plane state cannot be loaded or persisted."]
    pub fn get_http_api_authorizers(
        &self,
        scope: &ApiGatewayScope,
        api_id: &str,
        next_token: Option<&str>,
        max_results: Option<u32>,
    ) -> Result<HttpApiCollection<HttpApiAuthorizer>, ApiGatewayError> {
        let state = self.load_state(scope);
        let api = http_api(&state, api_id)?;
        let authorizers = api
            .authorizers
            .values()
            .map(StoredHttpAuthorizer::to_authorizer)
            .collect::<Vec<_>>();

        paginate_http_collection(authorizers, next_token, max_results)
    }

    #[doc = "# Errors\n\nReturns `ApiGatewayError` when the request fails validation, referenced resources are missing, or the API Gateway HTTP control-plane state cannot be loaded or persisted."]
    pub fn update_http_api_authorizer(
        &self,
        scope: &ApiGatewayScope,
        api_id: &str,
        authorizer_id: &str,
        input: UpdateHttpApiAuthorizerInput,
    ) -> Result<HttpApiAuthorizer, ApiGatewayError> {
        let mut state = self.load_state(scope);
        let api = http_api_mut(&mut state, api_id)?;
        let mut next = http_authorizer(api, authorizer_id)?.clone();
        if let Some(authorizer_credentials_arn) =
            input.authorizer_credentials_arn
        {
            next.authorizer_credentials_arn = Some(authorizer_credentials_arn);
        }
        if let Some(authorizer_payload_format_version) =
            input.authorizer_payload_format_version
        {
            next.authorizer_payload_format_version =
                Some(authorizer_payload_format_version);
        }
        if let Some(authorizer_result_ttl_in_seconds) =
            input.authorizer_result_ttl_in_seconds
        {
            next.authorizer_result_ttl_in_seconds =
                Some(authorizer_result_ttl_in_seconds);
        }
        if let Some(authorizer_type) = input.authorizer_type {
            next.authorizer_type = authorizer_type;
        }
        if let Some(authorizer_uri) = input.authorizer_uri {
            next.authorizer_uri = Some(authorizer_uri);
        }
        if input.enable_simple_responses.is_some() {
            next.enable_simple_responses = input.enable_simple_responses;
        }
        if !input.identity_source.is_empty() {
            next.identity_source = input.identity_source;
        }
        if let Some(identity_validation_expression) =
            input.identity_validation_expression
        {
            next.identity_validation_expression =
                Some(identity_validation_expression);
        }
        if input.jwt_configuration.is_some() {
            next.jwt_configuration = input.jwt_configuration;
        }
        if let Some(name) = input.name {
            next.name = name;
        }
        validate_stored_http_authorizer(&next)?;

        let authorizer = http_authorizer_mut(api, authorizer_id)?;
        *authorizer = next;
        let response = authorizer.to_authorizer();
        self.save_state(scope, state)?;
        Ok(response)
    }

    #[doc = "# Errors\n\nReturns `ApiGatewayError` when the request fails validation, referenced resources are missing, or the API Gateway HTTP control-plane state cannot be loaded or persisted."]
    pub fn delete_http_api_authorizer(
        &self,
        scope: &ApiGatewayScope,
        api_id: &str,
        authorizer_id: &str,
    ) -> Result<(), ApiGatewayError> {
        let mut state = self.load_state(scope);
        let api = http_api_mut(&mut state, api_id)?;
        if api.authorizers.remove(authorizer_id).is_none() {
            return Err(ApiGatewayError::NotFound {
                resource: format!("Authorizer {authorizer_id}"),
            });
        }
        for route in api.routes.values_mut() {
            if route.authorizer_id.as_deref() == Some(authorizer_id) {
                route.authorizer_id = None;
                route.authorization_type =
                    DEFAULT_HTTP_AUTHORIZATION_TYPE.to_owned();
            }
        }
        self.save_state(scope, state)
    }

    #[doc = "# Errors\n\nReturns `ApiGatewayError` when the request fails validation, referenced resources are missing, or the API Gateway HTTP control-plane state cannot be loaded or persisted."]
    pub fn create_http_api_stage(
        &self,
        scope: &ApiGatewayScope,
        api_id: &str,
        input: CreateHttpApiStageInput,
    ) -> Result<HttpApiStage, ApiGatewayError> {
        validate_stage_name(&input.stage_name)?;
        validate_tags(&input.tags)?;
        validate_stage_variables(&input.stage_variables)?;
        let mut state = self.load_state(scope);
        let api = http_api_mut(&mut state, api_id)?;
        if api.stages.contains_key(&input.stage_name) {
            return Err(ApiGatewayError::Conflict {
                resource: format!("Stage {}", input.stage_name),
            });
        }
        if let Some(deployment_id) = input.deployment_id.as_deref() {
            http_deployment(api, deployment_id)?;
        }
        let created_date = self.now_epoch_seconds();
        let stage = StoredHttpStage {
            auto_deploy: input.auto_deploy.unwrap_or(false),
            created_date,
            deployment_id: input.deployment_id,
            description: optional_empty_to_none(input.description),
            last_updated_date: created_date,
            stage_name: input.stage_name.clone(),
            stage_variables: input.stage_variables,
            tags: input.tags,
        };
        let response = stage.to_stage()?;
        api.stages.insert(input.stage_name, stage);
        self.save_state(scope, state)?;
        Ok(response)
    }

    #[doc = "# Errors\n\nReturns `ApiGatewayError` when the request fails validation, referenced resources are missing, or the API Gateway HTTP control-plane state cannot be loaded or persisted."]
    pub fn get_http_api_stage(
        &self,
        scope: &ApiGatewayScope,
        api_id: &str,
        stage_name: &str,
    ) -> Result<HttpApiStage, ApiGatewayError> {
        let state = self.load_state(scope);
        let api = http_api(&state, api_id)?;
        http_stage(api, stage_name)?.to_stage()
    }

    #[doc = "# Errors\n\nReturns `ApiGatewayError` when the request fails validation, referenced resources are missing, or the API Gateway HTTP control-plane state cannot be loaded or persisted."]
    pub fn get_http_api_stages(
        &self,
        scope: &ApiGatewayScope,
        api_id: &str,
        next_token: Option<&str>,
        max_results: Option<u32>,
    ) -> Result<HttpApiCollection<HttpApiStage>, ApiGatewayError> {
        let state = self.load_state(scope);
        let api = http_api(&state, api_id)?;
        let stages = api
            .stages
            .values()
            .map(StoredHttpStage::to_stage)
            .collect::<Result<Vec<_>, _>>()?;

        paginate_http_collection(stages, next_token, max_results)
    }

    #[doc = "# Errors\n\nReturns `ApiGatewayError` when the request fails validation, referenced resources are missing, or the API Gateway HTTP control-plane state cannot be loaded or persisted."]
    pub fn update_http_api_stage(
        &self,
        scope: &ApiGatewayScope,
        api_id: &str,
        stage_name: &str,
        input: UpdateHttpApiStageInput,
    ) -> Result<HttpApiStage, ApiGatewayError> {
        validate_tags(&input.tags)?;
        validate_stage_variables(&input.stage_variables)?;
        let mut state = self.load_state(scope);
        let api = http_api_mut(&mut state, api_id)?;
        if let Some(deployment_id) = input.deployment_id.as_deref() {
            http_deployment(api, deployment_id)?;
        }
        let now = self.now_epoch_seconds();
        let stage = http_stage_mut(api, stage_name)?;
        if let Some(auto_deploy) = input.auto_deploy {
            stage.auto_deploy = auto_deploy;
        }
        if input.deployment_id.is_some() {
            stage.deployment_id = input.deployment_id;
        }
        if input.description.is_some() {
            stage.description = optional_empty_to_none(input.description);
        }
        if !input.stage_variables.is_empty() {
            stage.stage_variables = input.stage_variables;
        }
        if !input.tags.is_empty() {
            stage.tags = input.tags;
        }
        stage.last_updated_date = now;

        let response = stage.to_stage()?;
        self.save_state(scope, state)?;
        Ok(response)
    }

    #[doc = "# Errors\n\nReturns `ApiGatewayError` when the request fails validation, referenced resources are missing, or the API Gateway HTTP control-plane state cannot be loaded or persisted."]
    pub fn delete_http_api_stage(
        &self,
        scope: &ApiGatewayScope,
        api_id: &str,
        stage_name: &str,
    ) -> Result<(), ApiGatewayError> {
        let mut state = self.load_state(scope);
        let api = http_api_mut(&mut state, api_id)?;
        if api.stages.remove(stage_name).is_none() {
            return Err(ApiGatewayError::NotFound {
                resource: format!("Stage {stage_name}"),
            });
        }
        self.save_state(scope, state)
    }

    #[doc = "# Errors\n\nReturns `ApiGatewayError` when the request fails validation, referenced resources are missing, or the API Gateway HTTP control-plane state cannot be loaded or persisted."]
    pub fn create_http_api_deployment(
        &self,
        scope: &ApiGatewayScope,
        api_id: &str,
        input: CreateHttpApiDeploymentInput,
    ) -> Result<HttpApiDeployment, ApiGatewayError> {
        let mut state = self.load_state(scope);
        let deployment_id = next_identifier(
            &mut state.next_http_deployment_sequence,
            HTTP_DEPLOYMENT_ID_WIDTH,
        );
        let api = http_api_mut(&mut state, api_id)?;
        let deployment = StoredHttpDeployment {
            auto_deployed: false,
            created_date: self.now_epoch_seconds(),
            deployment_id: deployment_id.clone(),
            description: optional_empty_to_none(input.description),
            snapshot: StoredHttpDeploymentSnapshot {
                authorizers: api.authorizers.clone(),
                integrations: api.integrations.clone(),
                routes: api.routes.clone(),
            },
        };
        if let Some(stage_name) = input.stage_name.as_deref() {
            let stage = http_stage_mut(api, stage_name)?;
            stage.deployment_id = Some(deployment_id.clone());
            stage.last_updated_date = self.now_epoch_seconds();
        }
        let response = deployment.to_deployment()?;
        api.deployments.insert(deployment_id, deployment);
        self.save_state(scope, state)?;
        Ok(response)
    }

    #[doc = "# Errors\n\nReturns `ApiGatewayError` when the request fails validation, referenced resources are missing, or the API Gateway HTTP control-plane state cannot be loaded or persisted."]
    pub fn get_http_api_deployment(
        &self,
        scope: &ApiGatewayScope,
        api_id: &str,
        deployment_id: &str,
    ) -> Result<HttpApiDeployment, ApiGatewayError> {
        let state = self.load_state(scope);
        let api = http_api(&state, api_id)?;
        http_deployment(api, deployment_id)?.to_deployment()
    }

    #[doc = "# Errors\n\nReturns `ApiGatewayError` when the request fails validation, referenced resources are missing, or the API Gateway HTTP control-plane state cannot be loaded or persisted."]
    pub fn get_http_api_deployments(
        &self,
        scope: &ApiGatewayScope,
        api_id: &str,
        next_token: Option<&str>,
        max_results: Option<u32>,
    ) -> Result<HttpApiCollection<HttpApiDeployment>, ApiGatewayError> {
        let state = self.load_state(scope);
        let api = http_api(&state, api_id)?;
        let deployments = api
            .deployments
            .values()
            .map(StoredHttpDeployment::to_deployment)
            .collect::<Result<Vec<_>, _>>()?;

        paginate_http_collection(deployments, next_token, max_results)
    }

    #[doc = "# Errors\n\nReturns `ApiGatewayError` when the request fails validation, referenced resources are missing, or the API Gateway HTTP control-plane state cannot be loaded or persisted."]
    pub fn delete_http_api_deployment(
        &self,
        scope: &ApiGatewayScope,
        api_id: &str,
        deployment_id: &str,
    ) -> Result<(), ApiGatewayError> {
        let mut state = self.load_state(scope);
        let api = http_api_mut(&mut state, api_id)?;
        if api.stages.values().any(|stage| {
            stage.deployment_id.as_deref() == Some(deployment_id)
                && !stage.auto_deploy
        }) {
            return Err(ApiGatewayError::Conflict {
                resource: format!("Deployment {deployment_id}"),
            });
        }
        if api.deployments.remove(deployment_id).is_none() {
            return Err(ApiGatewayError::NotFound {
                resource: format!("Deployment {deployment_id}"),
            });
        }
        self.save_state(scope, state)
    }
}

pub(super) fn prepare_http_api_execute_api(
    scope: &ApiGatewayScope,
    state: &StoredApiGatewayState,
    api_id: &str,
    request: &ExecuteApiRequest,
    caller_identity: Option<&CallerIdentity>,
) -> Result<ExecuteApiInvocation, ExecuteApiError> {
    let api =
        http_api(state, api_id).map_err(|_| ExecuteApiError::NotFound)?;
    if api.disable_execute_api_endpoint {
        return Err(ExecuteApiError::Forbidden);
    }

    let request_path = normalize_request_path(request.path())?;
    let (stage, routed_path) =
        select_http_stage(&api.stages, &request_path)?.into_parts();
    let snapshot = if stage.auto_deploy {
        StoredHttpDeploymentSnapshot {
            authorizers: api.authorizers.clone(),
            integrations: api.integrations.clone(),
            routes: api.routes.clone(),
        }
    } else {
        let deployment_id =
            stage.deployment_id.as_deref().ok_or(ExecuteApiError::NotFound)?;
        http_deployment(api, deployment_id)
            .map_err(|_| ExecuteApiError::NotFound)?
            .snapshot
            .clone()
    };
    let matched =
        match_http_route(&snapshot.routes, request.method(), &routed_path)?;
    if matched.authorization_type() != DEFAULT_HTTP_AUTHORIZATION_TYPE {
        return Err(ExecuteApiError::IntegrationFailure {
            message: "HTTP API authorizer execution is not supported."
                .to_owned(),
            status_code: 501,
        });
    }
    let integration_id = matched
        .target()
        .and_then(parse_http_route_target)
        .ok_or_else(|| ExecuteApiError::IntegrationFailure {
            message: "No integration configured for HTTP API route."
                .to_owned(),
            status_code: 500,
        })?;
    let integration = snapshot.integrations.get(integration_id).ok_or_else(
        || ExecuteApiError::IntegrationFailure {
            message: format!(
                "HTTP API route targets missing integration {integration_id}."
            ),
            status_code: 500,
        },
    )?;
    let host = normalize_authority(request.host());
    let execution = HttpApiExecutionContext {
        scope,
        api_id,
        stage,
        request,
        host: &host,
        matched: &matched,
        caller_identity,
    };
    let plan = prepare_http_integration_plan(&execution, integration)?;

    Ok(ExecuteApiInvocation::new(
        api.api_id.clone(),
        plan,
        routed_path,
        matched.route_id().to_owned(),
        matched.route_key().to_owned(),
        stage.stage_name.clone(),
    ))
}

/// # Errors
///
/// Returns [`ExecuteApiError::IntegrationFailure`] when the Lambda response
/// declares base64-encoded data that cannot be decoded or otherwise violates
/// the HTTP API Lambda proxy response contract.
pub fn map_lambda_proxy_response_v2(
    payload: &[u8],
) -> Result<ExecuteApiPreparedResponse, ExecuteApiError> {
    let value = serde_json::from_slice::<Value>(payload).ok();
    let Some(value) = value else {
        return Ok(ExecuteApiPreparedResponse::new(
            200,
            vec![("content-type".to_owned(), "application/json".to_owned())],
            payload.to_vec(),
        ));
    };
    let Some(object) = value.as_object() else {
        return Ok(ExecuteApiPreparedResponse::new(
            200,
            vec![("content-type".to_owned(), "application/json".to_owned())],
            payload.to_vec(),
        ));
    };

    let status_code = object
        .get("statusCode")
        .and_then(Value::as_u64)
        .and_then(|value| u16::try_from(value).ok())
        .filter(|value| (100..=599).contains(value))
        .unwrap_or(200);
    let mut headers = headers_from_json_map(object.get("headers"))
        .into_iter()
        .collect::<Vec<_>>();
    headers.extend(cookie_headers(object.get("cookies")));
    if !headers
        .iter()
        .any(|(name, _)| name.eq_ignore_ascii_case("content-type"))
    {
        headers
            .push(("content-type".to_owned(), "application/json".to_owned()));
    }
    let body = match object.get("body") {
        Some(Value::String(body))
            if object
                .get("isBase64Encoded")
                .and_then(Value::as_bool)
                .unwrap_or(false) =>
        {
            BASE64_STANDARD.decode(body).map_err(|_| {
                ExecuteApiError::IntegrationFailure {
                    message: "Malformed HTTP API Lambda proxy response."
                        .to_owned(),
                    status_code: 502,
                }
            })?
        }
        Some(Value::String(body)) => body.as_bytes().to_vec(),
        Some(body) => serde_json::to_vec(body).map_err(|error| {
            ExecuteApiError::IntegrationFailure {
                message: error.to_string(),
                status_code: 500,
            }
        })?,
        None => Vec::new(),
    };

    Ok(ExecuteApiPreparedResponse::new(status_code, headers, body))
}

fn http_api<'a>(
    state: &'a StoredApiGatewayState,
    api_id: &str,
) -> Result<&'a StoredHttpApi, ApiGatewayError> {
    state.http_apis.get(api_id).ok_or_else(|| ApiGatewayError::NotFound {
        resource: format!("API {api_id}"),
    })
}

fn http_api_mut<'a>(
    state: &'a mut StoredApiGatewayState,
    api_id: &str,
) -> Result<&'a mut StoredHttpApi, ApiGatewayError> {
    state.http_apis.get_mut(api_id).ok_or_else(|| ApiGatewayError::NotFound {
        resource: format!("API {api_id}"),
    })
}

fn http_route<'a>(
    api: &'a StoredHttpApi,
    route_id: &str,
) -> Result<&'a StoredHttpRoute, ApiGatewayError> {
    api.routes.get(route_id).ok_or_else(|| ApiGatewayError::NotFound {
        resource: format!("Route {route_id}"),
    })
}

fn http_route_mut<'a>(
    api: &'a mut StoredHttpApi,
    route_id: &str,
) -> Result<&'a mut StoredHttpRoute, ApiGatewayError> {
    api.routes.get_mut(route_id).ok_or_else(|| ApiGatewayError::NotFound {
        resource: format!("Route {route_id}"),
    })
}

fn http_integration<'a>(
    api: &'a StoredHttpApi,
    integration_id: &str,
) -> Result<&'a StoredHttpIntegration, ApiGatewayError> {
    api.integrations.get(integration_id).ok_or_else(|| {
        ApiGatewayError::NotFound {
            resource: format!("Integration {integration_id}"),
        }
    })
}

fn http_integration_mut<'a>(
    api: &'a mut StoredHttpApi,
    integration_id: &str,
) -> Result<&'a mut StoredHttpIntegration, ApiGatewayError> {
    api.integrations.get_mut(integration_id).ok_or_else(|| {
        ApiGatewayError::NotFound {
            resource: format!("Integration {integration_id}"),
        }
    })
}

fn http_authorizer<'a>(
    api: &'a StoredHttpApi,
    authorizer_id: &str,
) -> Result<&'a StoredHttpAuthorizer, ApiGatewayError> {
    api.authorizers.get(authorizer_id).ok_or_else(|| {
        ApiGatewayError::NotFound {
            resource: format!("Authorizer {authorizer_id}"),
        }
    })
}

fn http_authorizer_mut<'a>(
    api: &'a mut StoredHttpApi,
    authorizer_id: &str,
) -> Result<&'a mut StoredHttpAuthorizer, ApiGatewayError> {
    api.authorizers.get_mut(authorizer_id).ok_or_else(|| {
        ApiGatewayError::NotFound {
            resource: format!("Authorizer {authorizer_id}"),
        }
    })
}

fn http_stage<'a>(
    api: &'a StoredHttpApi,
    stage_name: &str,
) -> Result<&'a StoredHttpStage, ApiGatewayError> {
    api.stages.get(stage_name).ok_or_else(|| ApiGatewayError::NotFound {
        resource: format!("Stage {stage_name}"),
    })
}

fn http_stage_mut<'a>(
    api: &'a mut StoredHttpApi,
    stage_name: &str,
) -> Result<&'a mut StoredHttpStage, ApiGatewayError> {
    api.stages.get_mut(stage_name).ok_or_else(|| ApiGatewayError::NotFound {
        resource: format!("Stage {stage_name}"),
    })
}

fn http_deployment<'a>(
    api: &'a StoredHttpApi,
    deployment_id: &str,
) -> Result<&'a StoredHttpDeployment, ApiGatewayError> {
    api.deployments.get(deployment_id).ok_or_else(|| {
        ApiGatewayError::NotFound {
            resource: format!("Deployment {deployment_id}"),
        }
    })
}

fn paginate_http_collection<T: Clone>(
    items: Vec<T>,
    next_token: Option<&str>,
    max_results: Option<u32>,
) -> Result<HttpApiCollection<T>, ApiGatewayError> {
    let collection = paginate(items, next_token, max_results)?;
    Ok(HttpApiCollection {
        items: collection.item,
        next_token: collection.position,
    })
}

fn validate_http_api_protocol_type(
    value: &str,
) -> Result<(), ApiGatewayError> {
    if value == "HTTP" {
        Ok(())
    } else {
        Err(ApiGatewayError::Validation {
            message: format!("unsupported protocolType {value:?}"),
        })
    }
}

fn normalize_route_selection_expression(
    value: Option<String>,
) -> Result<String, ApiGatewayError> {
    let value = value
        .unwrap_or_else(|| DEFAULT_HTTP_ROUTE_SELECTION_EXPRESSION.to_owned());
    if value == DEFAULT_HTTP_ROUTE_SELECTION_EXPRESSION {
        Ok(value)
    } else {
        Err(ApiGatewayError::Validation {
            message: format!("unsupported routeSelectionExpression {value:?}"),
        })
    }
}

fn normalize_ip_address_type(
    value: Option<&str>,
) -> Result<String, ApiGatewayError> {
    match value.unwrap_or(DEFAULT_HTTP_API_IP_ADDRESS_TYPE) {
        "ipv4" => Ok("ipv4".to_owned()),
        "dualstack" => Ok("dualstack".to_owned()),
        other => Err(ApiGatewayError::Validation {
            message: format!("unsupported ipAddressType {other:?}"),
        }),
    }
}

fn validate_route_dependencies(
    api: &StoredHttpApi,
    authorization_type: Option<&str>,
    authorizer_id: Option<&str>,
    target: Option<&str>,
) -> Result<(), ApiGatewayError> {
    let authorization_type =
        authorization_type.unwrap_or(DEFAULT_HTTP_AUTHORIZATION_TYPE);
    validate_http_authorization_type(authorization_type)?;
    if let Some(authorizer_id) = authorizer_id {
        http_authorizer(api, authorizer_id)?;
        if authorization_type == DEFAULT_HTTP_AUTHORIZATION_TYPE {
            return Err(ApiGatewayError::Validation {
                message:
                    "authorizationType must not be NONE when authorizerId is set"
                        .to_owned(),
            });
        }
    }
    if authorization_type == "JWT" {
        return Err(ApiGatewayError::UnsupportedOperation {
            message:
                "HTTP API JWT authorizers are not supported in this build."
                    .to_owned(),
        });
    }
    if let Some(target) = target {
        let integration_id =
            parse_http_route_target(target).ok_or_else(|| {
                ApiGatewayError::Validation {
                    message: format!("invalid route target {target:?}"),
                }
            })?;
        http_integration(api, integration_id)?;
    }
    Ok(())
}

fn validate_http_authorization_type(
    value: &str,
) -> Result<(), ApiGatewayError> {
    if matches!(value, "NONE" | "AWS_IAM" | "CUSTOM" | "JWT") {
        Ok(())
    } else {
        Err(ApiGatewayError::Validation {
            message: format!("unsupported authorizationType {value:?}"),
        })
    }
}

fn ensure_unique_route_key(
    api: &StoredHttpApi,
    route_key: &str,
    current_route_id: Option<&str>,
) -> Result<(), ApiGatewayError> {
    if api.routes.values().any(|route| {
        Some(route.route_id.as_str()) != current_route_id
            && route.route_key == route_key
    }) {
        return Err(ApiGatewayError::Conflict {
            resource: format!("Route key {route_key}"),
        });
    }
    Ok(())
}

fn validate_http_integration_input(
    input: &CreateHttpApiIntegrationInput,
) -> Result<(), ApiGatewayError> {
    validate_stored_http_integration(&StoredHttpIntegration {
        connection_id: input.connection_id.clone(),
        connection_type: input
            .connection_type
            .clone()
            .unwrap_or_else(|| DEFAULT_HTTP_CONNECTION_TYPE.to_owned()),
        credentials_arn: input.credentials_arn.clone(),
        description: input.description.clone(),
        integration_id: "preview".to_owned(),
        integration_method: input.integration_method.clone(),
        integration_type: input.integration_type.clone(),
        integration_uri: input.integration_uri.clone(),
        payload_format_version: input.payload_format_version.clone(),
        request_parameters: input.request_parameters.clone(),
        request_templates: input.request_templates.clone(),
        template_selection_expression: input
            .template_selection_expression
            .clone(),
        timeout_in_millis: input.timeout_in_millis,
    })
}

fn validate_stored_http_integration(
    integration: &StoredHttpIntegration,
) -> Result<(), ApiGatewayError> {
    if integration.connection_type != DEFAULT_HTTP_CONNECTION_TYPE {
        return Err(ApiGatewayError::UnsupportedOperation {
            message: "HTTP API private integrations are not supported."
                .to_owned(),
        });
    }
    if integration
        .integration_uri
        .as_deref()
        .is_some_and(contains_stage_variable_reference)
    {
        return Err(ApiGatewayError::UnsupportedOperation {
            message:
                "HTTP API stage-variable integration URIs are not supported."
                    .to_owned(),
        });
    }
    if integration
        .request_parameters
        .values()
        .any(|value| contains_stage_variable_reference(value))
    {
        return Err(ApiGatewayError::UnsupportedOperation {
            message:
                "HTTP API stage-variable request parameter mappings are not supported."
                    .to_owned(),
        });
    }
    match integration.integration_type.as_str() {
        "AWS_PROXY" => {
            let uri =
                integration.integration_uri.as_deref().ok_or_else(|| {
                    ApiGatewayError::Validation {
                        message: "integrationUri is required".to_owned(),
                    }
                })?;
            if parse_lambda_integration_target(uri).is_none() {
                return Err(ApiGatewayError::Validation {
                    message: format!("invalid integrationUri {uri:?}"),
                });
            }
            let payload_format_version = integration
                .payload_format_version
                .as_deref()
                .unwrap_or(DEFAULT_HTTP_PAYLOAD_FORMAT_VERSION);
            if payload_format_version != DEFAULT_HTTP_PAYLOAD_FORMAT_VERSION {
                return Err(ApiGatewayError::UnsupportedOperation {
                    message:
                        "HTTP API Lambda integrations only support payloadFormatVersion 2.0."
                            .to_owned(),
                });
            }
            Ok(())
        }
        "HTTP_PROXY" => {
            let uri =
                integration.integration_uri.as_deref().ok_or_else(|| {
                    ApiGatewayError::Validation {
                        message: "integrationUri is required".to_owned(),
                    }
                })?;
            if !(uri.starts_with("http://") || uri.starts_with("https://")) {
                return Err(ApiGatewayError::Validation {
                    message: format!("invalid integrationUri {uri:?}"),
                });
            }
            Ok(())
        }
        "HTTP" => Err(ApiGatewayError::UnsupportedOperation {
            message: "HTTP API custom HTTP integrations are not supported."
                .to_owned(),
        }),
        other => Err(ApiGatewayError::Validation {
            message: format!("unsupported integrationType {other:?}"),
        }),
    }
}

fn validate_http_authorizer_input(
    input: &CreateHttpApiAuthorizerInput,
) -> Result<(), ApiGatewayError> {
    validate_stored_http_authorizer(&StoredHttpAuthorizer {
        authorizer_credentials_arn: input.authorizer_credentials_arn.clone(),
        authorizer_id: "preview".to_owned(),
        authorizer_payload_format_version: input
            .authorizer_payload_format_version
            .clone(),
        authorizer_result_ttl_in_seconds: input
            .authorizer_result_ttl_in_seconds,
        authorizer_type: input.authorizer_type.clone(),
        authorizer_uri: input.authorizer_uri.clone(),
        enable_simple_responses: input.enable_simple_responses,
        identity_source: input.identity_source.clone(),
        identity_validation_expression: input
            .identity_validation_expression
            .clone(),
        jwt_configuration: input.jwt_configuration.clone(),
        name: input.name.clone(),
    })
}

fn validate_stored_http_authorizer(
    authorizer: &StoredHttpAuthorizer,
) -> Result<(), ApiGatewayError> {
    validate_name("name", &authorizer.name)?;
    match authorizer.authorizer_type.as_str() {
        "REQUEST" => {
            let authorizer_uri = authorizer
                .authorizer_uri
                .as_deref()
                .ok_or_else(|| ApiGatewayError::Validation {
                    message: "authorizerUri is required".to_owned(),
                })?;
            validate_http_authorizer_uri(authorizer_uri)?;
            if authorizer.identity_source.is_empty() {
                return Err(ApiGatewayError::Validation {
                    message: "identitySource is required".to_owned(),
                });
            }
            if authorizer
                .identity_source
                .iter()
                .any(|value| contains_stage_variable_reference(value))
            {
                return Err(ApiGatewayError::UnsupportedOperation {
                    message:
                        "HTTP API stage-variable authorizer identities are not supported."
                            .to_owned(),
                });
            }
            if let Some(payload_format_version) =
                authorizer.authorizer_payload_format_version.as_deref()
                && payload_format_version
                    != DEFAULT_HTTP_PAYLOAD_FORMAT_VERSION
            {
                return Err(ApiGatewayError::UnsupportedOperation {
                        message:
                            "HTTP API REQUEST authorizers only support payload format version 2.0."
                                .to_owned(),
                    });
            }
            Ok(())
        }
        "JWT" => Err(ApiGatewayError::UnsupportedOperation {
            message:
                "HTTP API JWT authorizers are not supported in this build."
                    .to_owned(),
        }),
        other => Err(ApiGatewayError::Validation {
            message: format!("unsupported authorizerType {other:?}"),
        }),
    }
}

fn validate_http_authorizer_uri(uri: &str) -> Result<(), ApiGatewayError> {
    if uri.starts_with("arn:aws:apigateway:") && uri.ends_with("/invocations")
    {
        Ok(())
    } else {
        Err(ApiGatewayError::Validation {
            message: format!("invalid authorizerUri {uri:?}"),
        })
    }
}

fn validate_stage_variables(
    stage_variables: &BTreeMap<String, String>,
) -> Result<(), ApiGatewayError> {
    for (name, value) in stage_variables {
        if !name.chars().all(|character| {
            character.is_ascii_alphanumeric() || character == '_'
        }) {
            return Err(ApiGatewayError::Validation {
                message: format!("invalid stage variable name {name:?}"),
            });
        }
        if !value.chars().all(|character| {
            character.is_ascii_alphanumeric()
                || matches!(
                    character,
                    '-' | '.'
                        | '_'
                        | '~'
                        | ':'
                        | '/'
                        | '?'
                        | '#'
                        | '&'
                        | '='
                        | ','
                )
        }) {
            return Err(ApiGatewayError::Validation {
                message: format!("invalid stage variable value for {name:?}"),
            });
        }
    }
    Ok(())
}

struct HttpApiExecutionContext<'a, 'b> {
    scope: &'a ApiGatewayScope,
    api_id: &'a str,
    stage: &'a StoredHttpStage,
    request: &'a ExecuteApiRequest,
    host: &'a str,
    matched: &'a MatchedHttpRoute<'b, StoredHttpRoute>,
    caller_identity: Option<&'a CallerIdentity>,
}

fn prepare_http_integration_plan(
    execution: &HttpApiExecutionContext<'_, '_>,
    integration: &StoredHttpIntegration,
) -> Result<ExecuteApiIntegrationPlan, ExecuteApiError> {
    match integration.integration_type.as_str() {
        "AWS_PROXY" => {
            let target = parse_lambda_integration_target(
                integration.integration_uri.as_deref().unwrap_or_default(),
            )
            .ok_or_else(|| {
                ExecuteApiError::IntegrationFailure {
                    message: "HTTP API Lambda integration URI is invalid."
                        .to_owned(),
                    status_code: 500,
                }
            })?;
            Ok(ExecuteApiIntegrationPlan::LambdaProxy(Box::new(
                ExecuteApiLambdaProxyPlan::new(
                    target,
                    build_http_api_lambda_event(
                        execution.scope,
                        execution.api_id,
                        execution.stage,
                        execution.request,
                        execution.host,
                        execution.matched,
                        execution.caller_identity,
                    )?,
                    execute_api_source_arn_v2(
                        execution.scope.region(),
                        execution.scope.account_id(),
                        execution.api_id,
                        &execution.stage.stage_name,
                        execution.matched.route_key(),
                    )?,
                    true,
                ),
            )))
        }
        "HTTP_PROXY" => {
            Ok(ExecuteApiIntegrationPlan::Http(build_http_proxy_request(
                integration,
                execution.request,
                execution.matched,
            )?))
        }
        other => Err(ExecuteApiError::IntegrationFailure {
            message: format!(
                "Unsupported HTTP API integration type {other:?}."
            ),
            status_code: 500,
        }),
    }
}

fn build_http_api_lambda_event(
    scope: &ApiGatewayScope,
    api_id: &str,
    stage: &StoredHttpStage,
    request: &ExecuteApiRequest,
    host: &str,
    matched: &MatchedHttpRoute<'_, StoredHttpRoute>,
    _caller_identity: Option<&CallerIdentity>,
) -> Result<Vec<u8>, ExecuteApiError> {
    let headers = multi_value_headers(request.headers());
    let single_headers = single_value_headers(&headers);
    let query_string_parameters =
        single_value_query_string_parameters(request.query_string())?;
    let user_agent = single_headers
        .get("user-agent")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();
    let path_parameters = if matched.path_parameters().is_empty() {
        Value::Null
    } else {
        Value::Object(
            matched
                .path_parameters()
                .iter()
                .map(|(name, value)| {
                    (name.clone(), Value::String(value.clone()))
                })
                .collect(),
        )
    };
    let stage_variables = if stage.stage_variables.is_empty() {
        Value::Null
    } else {
        Value::Object(
            stage
                .stage_variables
                .iter()
                .map(|(name, value)| {
                    (name.clone(), Value::String(value.clone()))
                })
                .collect(),
        )
    };
    let (body, is_base64_encoded) = if request.body().is_empty() {
        (Value::Null, false)
    } else if let Ok(body) = std::str::from_utf8(request.body()) {
        (Value::String(body.to_owned()), false)
    } else {
        (Value::String(BASE64_STANDARD.encode(request.body())), true)
    };
    let now_epoch_millis = stage.last_updated_date.saturating_mul(1000);
    let event = json!({
        "version": DEFAULT_HTTP_PAYLOAD_FORMAT_VERSION,
        "routeKey": matched.route_key(),
        "rawPath": matched.request_path(),
        "rawQueryString": request.query_string().unwrap_or_default(),
        "cookies": extract_cookies(request.headers()),
        "headers": Value::Object(single_headers),
        "queryStringParameters": if query_string_parameters.is_empty() {
            Value::Null
        } else {
            Value::Object(
                query_string_parameters
                    .into_iter()
                    .map(|(name, value)| (name, Value::String(value)))
                    .collect(),
            )
        },
        "requestContext": {
            "accountId": scope.account_id().as_str(),
            "apiId": api_id,
            "authentication": Value::Null,
            "authorizer": Value::Null,
            "domainName": host,
            "domainPrefix": host.split('.').next().unwrap_or_default(),
            "http": {
                "method": request.method(),
                "path": matched.request_path(),
                "protocol": request.protocol().unwrap_or("HTTP/1.1"),
                "sourceIp": request.source_ip().unwrap_or("127.0.0.1"),
                "userAgent": user_agent,
            },
            "requestId": EXECUTE_API_REQUEST_ID,
            "routeKey": matched.route_key(),
            "stage": stage.stage_name,
            "time": format_request_context_time(now_epoch_millis)?,
            "timeEpoch": now_epoch_millis,
        },
        "body": body,
        "pathParameters": path_parameters,
        "stageVariables": stage_variables,
        "isBase64Encoded": is_base64_encoded,
    });

    serde_json::to_vec(&event).map_err(|error| {
        ExecuteApiError::IntegrationFailure {
            message: error.to_string(),
            status_code: 500,
        }
    })
}

fn build_http_proxy_request(
    integration: &StoredHttpIntegration,
    request: &ExecuteApiRequest,
    matched: &MatchedHttpRoute<'_, StoredHttpRoute>,
) -> Result<HttpForwardRequest, ExecuteApiError> {
    let uri = integration.integration_uri.as_deref().ok_or_else(|| {
        ExecuteApiError::IntegrationFailure {
            message: "HTTP API integration URI is invalid.".to_owned(),
            status_code: 500,
        }
    })?;
    let parsed = parse_http_uri(uri)?;
    let path = render_http_proxy_path(
        &parsed.path,
        matched,
        parsed.query.as_deref(),
        request.query_string(),
    )?;
    let method =
        integration.integration_method.as_deref().unwrap_or(request.method());
    let mut forward = HttpForwardRequest::new(parsed.endpoint, method, path)
        .with_body(request.body().to_vec());
    for (name, value) in request.headers() {
        if name.eq_ignore_ascii_case("host")
            || name.eq_ignore_ascii_case("content-length")
            || name.eq_ignore_ascii_case("connection")
        {
            continue;
        }
        forward = forward.with_header(name.clone(), value.clone());
    }

    Ok(forward)
}

fn render_http_proxy_path(
    template_path: &str,
    matched: &MatchedHttpRoute<'_, StoredHttpRoute>,
    template_query: Option<&str>,
    request_query: Option<&str>,
) -> Result<String, ExecuteApiError> {
    let mut rendered_path = template_path.to_owned();
    let mut replaced = false;
    for (name, value) in matched.path_parameters() {
        let placeholder = format!("{{{name}}}");
        if rendered_path.contains(&placeholder) {
            rendered_path = rendered_path
                .replace(&placeholder, &urlencoding::encode(value));
            replaced = true;
        }
    }
    if rendered_path.contains('{') || rendered_path.contains('}') {
        return Err(ExecuteApiError::IntegrationFailure {
            message:
                "HTTP API integration URI references missing path variables."
                    .to_owned(),
            status_code: 500,
        });
    }
    if !replaced {
        rendered_path =
            join_proxy_paths(template_path, matched.request_path());
    }
    Ok(combine_query_strings(&rendered_path, template_query, request_query))
}

fn join_proxy_paths(base_path: &str, request_path: &str) -> String {
    if base_path == ROOT_PATH {
        request_path.to_owned()
    } else if request_path == ROOT_PATH {
        base_path.to_owned()
    } else {
        format!(
            "{}/{}",
            base_path.trim_end_matches('/'),
            request_path.trim_start_matches('/')
        )
    }
}

fn parse_http_route_target(target: &str) -> Option<&str> {
    target.strip_prefix("integrations/")
}

fn contains_stage_variable_reference(value: &str) -> bool {
    value.contains("${stageVariables.")
        || value.contains("$stageVariables.")
        || value.contains("stageVariables.")
}

fn normalize_request_path(path: &str) -> Result<String, ExecuteApiError> {
    let segments = path
        .split('/')
        .filter(|segment| !segment.is_empty())
        .map(|segment| {
            urlencoding::decode(segment)
                .map(|segment| segment.into_owned())
                .map_err(|_| ExecuteApiError::BadRequest {
                    message: format!(
                        "invalid request path segment {segment:?}"
                    ),
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    if segments.is_empty() {
        Ok(ROOT_PATH.to_owned())
    } else {
        Ok(format!("/{}", segments.join("/")))
    }
}

fn normalize_authority(host: &str) -> String {
    host.trim()
        .rsplit_once(':')
        .filter(|(_, port)| port.parse::<u16>().is_ok())
        .map(|(host, _)| host)
        .unwrap_or(host.trim())
        .to_ascii_lowercase()
}

fn parse_lambda_integration_function_name(uri: &str) -> Option<String> {
    if uri.starts_with("arn:aws:lambda:") {
        return Some(uri.to_owned());
    }
    let function = uri.split("/functions/").nth(1)?;
    let function = function.strip_suffix("/invocations")?;
    Some(function.to_owned())
}

fn parse_lambda_integration_target(uri: &str) -> Option<LambdaFunctionTarget> {
    let function_name = parse_lambda_integration_function_name(uri)?;

    LambdaFunctionTarget::parse(function_name).ok()
}

fn execute_api_source_arn_v2(
    region: &RegionId,
    account_id: &AccountId,
    api_id: &str,
    stage_name: &str,
    route_key: &str,
) -> Result<ExecuteApiSourceArn, ExecuteApiError> {
    let resource = if route_key == "$default" {
        format!("{api_id}/{stage_name}/$default")
    } else {
        let (method, route_path) =
            route_key.split_once(' ').ok_or_else(|| {
                ExecuteApiError::IntegrationFailure {
                    message: format!(
                        "stored route key {route_key:?} is invalid"
                    ),
                    status_code: 500,
                }
            })?;
        format!(
            "{api_id}/{stage_name}/{method}/{}",
            route_path.trim_start_matches('/'),
        )
    };

    ExecuteApiSourceArn::from_resource(region, account_id, resource).map_err(
        |error| ExecuteApiError::IntegrationFailure {
            message: error.to_string(),
            status_code: 500,
        },
    )
}

fn format_timestamp(epoch_seconds: u64) -> Result<String, ApiGatewayError> {
    let seconds = i64::try_from(epoch_seconds).map_err(|error| {
        ApiGatewayError::Internal { message: error.to_string() }
    })?;
    let timestamp =
        OffsetDateTime::from_unix_timestamp(seconds).map_err(|error| {
            ApiGatewayError::Internal { message: error.to_string() }
        })?;
    timestamp.format(&Rfc3339).map_err(|error| ApiGatewayError::Internal {
        message: error.to_string(),
    })
}

fn format_request_context_time(
    epoch_millis: u64,
) -> Result<String, ExecuteApiError> {
    let timestamp = OffsetDateTime::from_unix_timestamp_nanos(
        i128::from(epoch_millis) * 1_000_000,
    )
    .map_err(|error| ExecuteApiError::IntegrationFailure {
        message: error.to_string(),
        status_code: 500,
    })?;
    let format = parse(
        "[day padding:none]/[month repr:short]/[year]:[hour]:[minute]:[second] +0000",
    )
    .map_err(|error| ExecuteApiError::IntegrationFailure {
        message: error.to_string(),
        status_code: 500,
    })?;

    timestamp.format(&format).map_err(|error| {
        ExecuteApiError::IntegrationFailure {
            message: error.to_string(),
            status_code: 500,
        }
    })
}

fn headers_from_json_map(value: Option<&Value>) -> BTreeMap<String, String> {
    value
        .and_then(Value::as_object)
        .map(|headers| {
            headers
                .iter()
                .filter_map(|(name, value)| {
                    Some((name.to_owned(), value.as_str()?.to_owned()))
                })
                .collect::<BTreeMap<_, _>>()
        })
        .unwrap_or_default()
}

fn cookie_headers(value: Option<&Value>) -> Vec<(String, String)> {
    value
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(|value| ("set-cookie".to_owned(), value.to_owned()))
        .collect()
}

fn multi_value_headers(
    headers: &[(String, String)],
) -> BTreeMap<String, Vec<String>> {
    let mut values = BTreeMap::<String, Vec<String>>::new();
    for (name, value) in headers {
        values
            .entry(name.to_ascii_lowercase())
            .or_default()
            .push(value.clone());
    }
    values
}

fn single_value_headers(
    headers: &BTreeMap<String, Vec<String>>,
) -> serde_json::Map<String, Value> {
    headers
        .iter()
        .map(|(name, values)| {
            (
                name.clone(),
                Value::String(values.last().cloned().unwrap_or_default()),
            )
        })
        .collect()
}

fn single_value_query_string_parameters(
    raw_query: Option<&str>,
) -> Result<BTreeMap<String, String>, ExecuteApiError> {
    let mut values = BTreeMap::<String, String>::new();
    for pair in raw_query
        .unwrap_or_default()
        .split('&')
        .filter(|pair| !pair.is_empty())
    {
        let (name, value) = pair.split_once('=').unwrap_or((pair, ""));
        let name = urlencoding::decode(name)
            .map_err(|_| ExecuteApiError::BadRequest {
                message: format!("invalid query parameter name {name:?}"),
            })?
            .into_owned();
        let value = urlencoding::decode(value)
            .map_err(|_| ExecuteApiError::BadRequest {
                message: format!("invalid query parameter value {value:?}"),
            })?
            .into_owned();
        values.insert(name, value);
    }
    Ok(values)
}

fn extract_cookies(headers: &[(String, String)]) -> Value {
    headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("cookie"))
        .map(|(_, value)| {
            Value::Array(
                value
                    .split(';')
                    .map(str::trim)
                    .filter(|cookie| !cookie.is_empty())
                    .map(|cookie| Value::String(cookie.to_owned()))
                    .collect(),
            )
        })
        .unwrap_or(Value::Null)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ParsedHttpUri {
    endpoint: Endpoint,
    path: String,
    query: Option<String>,
}

fn parse_http_uri(uri: &str) -> Result<ParsedHttpUri, ExecuteApiError> {
    let (scheme, rest) = uri.split_once("://").ok_or_else(|| {
        ExecuteApiError::IntegrationFailure {
            message: "HTTP API integration URI is invalid.".to_owned(),
            status_code: 500,
        }
    })?;
    if !matches!(scheme, "http" | "https") {
        return Err(ExecuteApiError::IntegrationFailure {
            message: "HTTP API integration URI is invalid.".to_owned(),
            status_code: 500,
        });
    }
    let (authority, path_and_query) = rest
        .split_once('/')
        .map(|(authority, path)| (authority, format!("/{path}")))
        .unwrap_or((rest, ROOT_PATH.to_owned()));
    if authority.is_empty() {
        return Err(ExecuteApiError::IntegrationFailure {
            message: "HTTP API integration URI is invalid.".to_owned(),
            status_code: 500,
        });
    }
    let (host, port) = authority
        .rsplit_once(':')
        .and_then(|(host, port)| {
            port.parse::<u16>()
                .ok()
                .map(|parsed_port| (host.to_owned(), parsed_port))
        })
        .unwrap_or_else(|| {
            (authority.to_owned(), if scheme == "https" { 443 } else { 80 })
        });
    let (path, query) = path_and_query
        .split_once('?')
        .map(|(path, query)| (path.to_owned(), Some(query.to_owned())))
        .unwrap_or_else(|| (path_and_query, None));

    Ok(ParsedHttpUri { endpoint: Endpoint::new(host, port), path, query })
}

fn combine_query_strings(
    path: &str,
    template_query: Option<&str>,
    request_query: Option<&str>,
) -> String {
    let combined = [template_query, request_query]
        .into_iter()
        .flatten()
        .filter(|query| !query.is_empty())
        .collect::<Vec<_>>()
        .join("&");
    if combined.is_empty() {
        path.to_owned()
    } else {
        format!("{path}?{combined}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ApiGatewayService;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::SystemTime;
    use std::time::UNIX_EPOCH;
    use storage::{StorageConfig, StorageFactory, StorageMode};

    struct FullCrudFixture {
        api_id: String,
        second_api_id: String,
        authorizer_id: String,
        route_id: String,
        default_route_id: String,
        deployment_id: String,
        proxy_integration_id: String,
        lambda_integration_id: String,
    }

    struct TestClock;

    impl aws::Clock for TestClock {
        fn now(&self) -> SystemTime {
            UNIX_EPOCH
        }
    }

    fn service(label: &str) -> ApiGatewayService {
        static NEXT_ID: AtomicUsize = AtomicUsize::new(0);

        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        let root = std::env::temp_dir().join(format!(
            "cloudish-apigw-v2-{label}-{}-{id}",
            std::process::id()
        ));
        ApiGatewayService::new(
            &StorageFactory::new(StorageConfig::new(
                root,
                StorageMode::Memory,
            )),
            Arc::new(TestClock),
        )
    }

    fn scope() -> ApiGatewayScope {
        ApiGatewayScope::new(
            "000000000000".parse().expect("account should parse"),
            "eu-west-2".parse().expect("region should parse"),
        )
    }

    fn lambda_uri(function_name: &str) -> String {
        format!(
            "arn:aws:lambda:eu-west-2:000000000000:function:{function_name}"
        )
    }

    fn authorizer_uri(function_name: &str) -> String {
        format!(
            "arn:aws:apigateway:eu-west-2:lambda:path/2015-03-31/functions/\
             arn:aws:lambda:eu-west-2:000000000000:function:{function_name}/\
             invocations"
        )
    }

    fn stored_api(api_id: &str) -> StoredHttpApi {
        StoredHttpApi {
            api_id: api_id.to_owned(),
            created_date: 0,
            description: None,
            disable_execute_api_endpoint: false,
            ip_address_type: DEFAULT_HTTP_API_IP_ADDRESS_TYPE.to_owned(),
            name: "demo".to_owned(),
            protocol_type: "HTTP".to_owned(),
            route_selection_expression:
                DEFAULT_HTTP_ROUTE_SELECTION_EXPRESSION.to_owned(),
            tags: BTreeMap::new(),
            version: None,
            routes: BTreeMap::new(),
            integrations: BTreeMap::new(),
            authorizers: BTreeMap::new(),
            deployments: BTreeMap::new(),
            stages: BTreeMap::new(),
        }
    }

    fn stored_route(
        route_id: &str,
        route_key: &str,
        authorization_type: &str,
        authorizer_id: Option<&str>,
        target: Option<&str>,
    ) -> StoredHttpRoute {
        StoredHttpRoute {
            api_key_required: false,
            authorization_scopes: Vec::new(),
            authorization_type: authorization_type.to_owned(),
            authorizer_id: authorizer_id.map(str::to_owned),
            operation_name: None,
            route_id: route_id.to_owned(),
            route_key: route_key.to_owned(),
            target: target.map(str::to_owned),
        }
    }

    fn stored_integration(
        integration_id: &str,
        integration_type: &str,
        integration_uri: Option<&str>,
    ) -> StoredHttpIntegration {
        StoredHttpIntegration {
            connection_id: None,
            connection_type: DEFAULT_HTTP_CONNECTION_TYPE.to_owned(),
            credentials_arn: None,
            description: None,
            integration_id: integration_id.to_owned(),
            integration_method: None,
            integration_type: integration_type.to_owned(),
            integration_uri: integration_uri.map(str::to_owned),
            payload_format_version: None,
            request_parameters: BTreeMap::new(),
            request_templates: BTreeMap::new(),
            template_selection_expression: None,
            timeout_in_millis: None,
        }
    }

    fn stored_authorizer(authorizer_id: &str) -> StoredHttpAuthorizer {
        StoredHttpAuthorizer {
            authorizer_credentials_arn: None,
            authorizer_id: authorizer_id.to_owned(),
            authorizer_payload_format_version: Some(
                DEFAULT_HTTP_PAYLOAD_FORMAT_VERSION.to_owned(),
            ),
            authorizer_result_ttl_in_seconds: Some(60),
            authorizer_type: "REQUEST".to_owned(),
            authorizer_uri: Some(authorizer_uri("auth")),
            enable_simple_responses: Some(true),
            identity_source: vec!["$request.header.Authorization".to_owned()],
            identity_validation_expression: None,
            jwt_configuration: None,
            name: "auth".to_owned(),
        }
    }

    fn stored_stage(
        stage_name: &str,
        auto_deploy: bool,
        deployment_id: Option<&str>,
    ) -> StoredHttpStage {
        StoredHttpStage {
            auto_deploy,
            created_date: 0,
            deployment_id: deployment_id.map(str::to_owned),
            description: None,
            last_updated_date: 0,
            stage_name: stage_name.to_owned(),
            stage_variables: BTreeMap::new(),
            tags: BTreeMap::new(),
        }
    }

    fn stored_deployment(
        deployment_id: &str,
        routes: BTreeMap<String, StoredHttpRoute>,
        integrations: BTreeMap<String, StoredHttpIntegration>,
    ) -> StoredHttpDeployment {
        StoredHttpDeployment {
            auto_deployed: false,
            created_date: 0,
            deployment_id: deployment_id.to_owned(),
            description: None,
            snapshot: StoredHttpDeploymentSnapshot {
                authorizers: BTreeMap::new(),
                integrations,
                routes,
            },
        }
    }

    #[test]
    fn apigw_v2_control_full_resource_round_trip_and_conflicts() {
        let service = service("full-crud");
        let scope = scope();
        let (api_id, second_api_id) =
            assert_http_api_metadata_and_pagination(&service, &scope);
        let (proxy_integration_id, lambda_integration_id) =
            assert_http_api_integrations(&service, &scope, &api_id);
        let authorizer_id =
            assert_http_api_authorizer(&service, &scope, &api_id);
        let (route_id, default_route_id) = assert_http_api_routes(
            &service,
            &scope,
            &api_id,
            &authorizer_id,
            &lambda_integration_id,
            &proxy_integration_id,
        );
        let fixture = FullCrudFixture {
            api_id: api_id.clone(),
            second_api_id,
            authorizer_id,
            route_id,
            default_route_id,
            deployment_id: assert_http_api_stage_and_deployment(
                &service, &scope, &api_id,
            ),
            proxy_integration_id,
            lambda_integration_id,
        };
        assert_http_api_cleanup_and_conflicts(&service, &scope, &fixture);
    }

    fn assert_http_api_metadata_and_pagination(
        service: &ApiGatewayService,
        scope: &ApiGatewayScope,
    ) -> (String, String) {
        let first_api = service
            .create_http_api(
                scope,
                CreateHttpApiInput {
                    description: Some("first".to_owned()),
                    disable_execute_api_endpoint: Some(false),
                    ip_address_type: Some("ipv4".to_owned()),
                    name: "first".to_owned(),
                    protocol_type: "HTTP".to_owned(),
                    route_selection_expression: Some(
                        DEFAULT_HTTP_ROUTE_SELECTION_EXPRESSION.to_owned(),
                    ),
                    tags: BTreeMap::new(),
                    version: Some("v1".to_owned()),
                },
            )
            .expect("first api should create");
        let second_api = service
            .create_http_api(
                scope,
                CreateHttpApiInput {
                    description: Some("second".to_owned()),
                    disable_execute_api_endpoint: Some(false),
                    ip_address_type: None,
                    name: "second".to_owned(),
                    protocol_type: "HTTP".to_owned(),
                    route_selection_expression: None,
                    tags: BTreeMap::new(),
                    version: None,
                },
            )
            .expect("second api should create");

        let first_page = service
            .get_http_apis(scope, None, Some(1))
            .expect("first page should list");
        assert_eq!(first_page.items.len(), 1);
        let second_page = service
            .get_http_apis(scope, first_page.next_token.as_deref(), Some(10))
            .expect("second page should list");
        assert_eq!(second_page.items.len(), 1);

        let api_id = first_api.api_id;
        let fetched_api =
            service.get_http_api(scope, &api_id).expect("api should fetch");
        assert_eq!(fetched_api.name, "first");
        let updated_api = service
            .update_http_api(
                scope,
                &api_id,
                UpdateHttpApiInput {
                    description: Some(String::new()),
                    disable_execute_api_endpoint: Some(true),
                    ip_address_type: Some("dualstack".to_owned()),
                    name: Some("first-updated".to_owned()),
                    route_selection_expression: Some(
                        DEFAULT_HTTP_ROUTE_SELECTION_EXPRESSION.to_owned(),
                    ),
                    version: Some(String::new()),
                },
            )
            .expect("api should update");
        assert_eq!(updated_api.name, "first-updated");
        assert_eq!(updated_api.description, None);
        assert_eq!(updated_api.version, None);

        (api_id, second_api.api_id)
    }

    fn assert_http_api_integrations(
        service: &ApiGatewayService,
        scope: &ApiGatewayScope,
        api_id: &str,
    ) -> (String, String) {
        let proxy_integration = service
            .create_http_api_integration(
                scope,
                api_id,
                CreateHttpApiIntegrationInput {
                    connection_id: None,
                    connection_type: None,
                    credentials_arn: None,
                    description: Some("proxy".to_owned()),
                    integration_method: None,
                    integration_type: "HTTP_PROXY".to_owned(),
                    integration_uri: Some(
                        "https://backend.example/base".to_owned(),
                    ),
                    payload_format_version: None,
                    request_parameters: BTreeMap::new(),
                    request_templates: BTreeMap::new(),
                    template_selection_expression: None,
                    timeout_in_millis: None,
                },
            )
            .expect("proxy integration should create");
        let proxy_integration_id = proxy_integration
            .integration_id
            .expect("proxy integration id should exist");
        let lambda_integration = service
            .create_http_api_integration(
                scope,
                api_id,
                CreateHttpApiIntegrationInput {
                    connection_id: None,
                    connection_type: None,
                    credentials_arn: None,
                    description: Some("lambda".to_owned()),
                    integration_method: Some("POST".to_owned()),
                    integration_type: "AWS_PROXY".to_owned(),
                    integration_uri: Some(lambda_uri("handler:live")),
                    payload_format_version: Some(
                        DEFAULT_HTTP_PAYLOAD_FORMAT_VERSION.to_owned(),
                    ),
                    request_parameters: BTreeMap::new(),
                    request_templates: BTreeMap::new(),
                    template_selection_expression: None,
                    timeout_in_millis: Some(10_000),
                },
            )
            .expect("lambda integration should create");
        let lambda_integration_id = lambda_integration
            .integration_id
            .expect("lambda integration id should exist");

        let listed_integrations = service
            .get_http_api_integrations(scope, api_id, None, Some(1))
            .expect("integrations should list");
        assert_eq!(listed_integrations.items.len(), 1);
        let fetched_integration = service
            .get_http_api_integration(scope, api_id, &proxy_integration_id)
            .expect("integration should fetch");
        assert_eq!(
            fetched_integration.integration_uri.as_deref(),
            Some("https://backend.example/base")
        );
        let updated_integration = service
            .update_http_api_integration(
                scope,
                api_id,
                &proxy_integration_id,
                UpdateHttpApiIntegrationInput {
                    connection_id: Some("conn-123".to_owned()),
                    connection_type: Some(
                        DEFAULT_HTTP_CONNECTION_TYPE.to_owned(),
                    ),
                    credentials_arn: Some(
                        "arn:aws:iam::000000000000:role/proxy".to_owned(),
                    ),
                    description: Some(String::new()),
                    integration_method: Some("PATCH".to_owned()),
                    integration_type: Some("AWS_PROXY".to_owned()),
                    integration_uri: Some(lambda_uri("proxy-handler:live")),
                    payload_format_version: Some(
                        DEFAULT_HTTP_PAYLOAD_FORMAT_VERSION.to_owned(),
                    ),
                    request_parameters: BTreeMap::from([(
                        "overwrite:path".to_owned(),
                        "$request.path.proxy".to_owned(),
                    )]),
                    request_templates: BTreeMap::from([(
                        "application/json".to_owned(),
                        "{}".to_owned(),
                    )]),
                    template_selection_expression: Some(
                        "$request.body".to_owned(),
                    ),
                    timeout_in_millis: Some(8_000),
                },
            )
            .expect("integration should update");
        assert_eq!(updated_integration.description, None);
        assert_eq!(
            updated_integration.integration_type,
            "AWS_PROXY".to_owned()
        );

        (proxy_integration_id, lambda_integration_id)
    }

    fn assert_http_api_authorizer(
        service: &ApiGatewayService,
        scope: &ApiGatewayScope,
        api_id: &str,
    ) -> String {
        let authorizer = service
            .create_http_api_authorizer(
                scope,
                api_id,
                CreateHttpApiAuthorizerInput {
                    authorizer_credentials_arn: Some(
                        "arn:aws:iam::000000000000:role/auth".to_owned(),
                    ),
                    authorizer_payload_format_version: Some(
                        DEFAULT_HTTP_PAYLOAD_FORMAT_VERSION.to_owned(),
                    ),
                    authorizer_result_ttl_in_seconds: Some(300),
                    authorizer_type: "REQUEST".to_owned(),
                    authorizer_uri: Some(authorizer_uri("auth")),
                    enable_simple_responses: Some(true),
                    identity_source: vec![
                        "$request.header.Authorization".to_owned(),
                    ],
                    identity_validation_expression: Some("token".to_owned()),
                    jwt_configuration: None,
                    name: "request-authorizer".to_owned(),
                },
            )
            .expect("authorizer should create");
        let authorizer_id =
            authorizer.authorizer_id.expect("authorizer id should exist");

        let fetched_authorizer = service
            .get_http_api_authorizer(scope, api_id, &authorizer_id)
            .expect("authorizer should fetch");
        assert_eq!(
            fetched_authorizer.name.as_deref(),
            Some("request-authorizer")
        );
        let listed_authorizers = service
            .get_http_api_authorizers(scope, api_id, None, Some(10))
            .expect("authorizers should list");
        assert_eq!(listed_authorizers.items.len(), 1);
        let updated_authorizer = service
            .update_http_api_authorizer(
                scope,
                api_id,
                &authorizer_id,
                UpdateHttpApiAuthorizerInput {
                    authorizer_credentials_arn: Some(
                        "arn:aws:iam::000000000000:role/auth-v2".to_owned(),
                    ),
                    authorizer_payload_format_version: Some(
                        DEFAULT_HTTP_PAYLOAD_FORMAT_VERSION.to_owned(),
                    ),
                    authorizer_result_ttl_in_seconds: Some(120),
                    authorizer_type: Some("REQUEST".to_owned()),
                    authorizer_uri: Some(authorizer_uri("auth-v2")),
                    enable_simple_responses: Some(false),
                    identity_source: vec!["$request.header.X-Auth".to_owned()],
                    identity_validation_expression: Some("expr".to_owned()),
                    jwt_configuration: Some(JwtConfiguration {
                        audience: vec!["aud".to_owned()],
                        issuer: "https://issuer.example".to_owned(),
                    }),
                    name: Some("request-authorizer-v2".to_owned()),
                },
            )
            .expect("authorizer should update");
        assert_eq!(
            updated_authorizer.name.as_deref(),
            Some("request-authorizer-v2")
        );

        authorizer_id
    }

    fn assert_http_api_routes(
        service: &ApiGatewayService,
        scope: &ApiGatewayScope,
        api_id: &str,
        authorizer_id: &str,
        lambda_integration_id: &str,
        proxy_integration_id: &str,
    ) -> (String, String) {
        let primary_route = service
            .create_http_api_route(
                scope,
                api_id,
                CreateHttpApiRouteInput {
                    authorization_type: Some("CUSTOM".to_owned()),
                    authorizer_id: Some(authorizer_id.to_owned()),
                    operation_name: Some("FetchItem".to_owned()),
                    route_key: "GET /items/{id}".to_owned(),
                    target: Some(format!(
                        "integrations/{lambda_integration_id}"
                    )),
                },
            )
            .expect("primary route should create");
        let default_route = service
            .create_http_api_route(
                scope,
                api_id,
                CreateHttpApiRouteInput {
                    authorization_type: Some("NONE".to_owned()),
                    authorizer_id: None,
                    operation_name: None,
                    route_key: "$default".to_owned(),
                    target: Some(format!(
                        "integrations/{proxy_integration_id}"
                    )),
                },
            )
            .expect("default route should create");
        let route_id = primary_route.route_id;

        let listed_routes = service
            .get_http_api_routes(scope, api_id, None, Some(1))
            .expect("routes should list");
        assert_eq!(listed_routes.items.len(), 1);
        let fetched_route = service
            .get_http_api_route(scope, api_id, &route_id)
            .expect("route should fetch");
        assert_eq!(fetched_route.route_key, "GET /items/{id}");
        let updated_route = service
            .update_http_api_route(
                scope,
                api_id,
                &route_id,
                UpdateHttpApiRouteInput {
                    authorization_type: Some("CUSTOM".to_owned()),
                    authorizer_id: Some(authorizer_id.to_owned()),
                    operation_name: Some(String::new()),
                    route_key: Some("ANY /items/{id}".to_owned()),
                    target: Some(format!(
                        "integrations/{proxy_integration_id}"
                    )),
                },
            )
            .expect("route should update");
        assert_eq!(updated_route.route_key, "ANY /items/{id}");
        assert_eq!(updated_route.operation_name, None);

        (route_id, default_route.route_id)
    }

    fn assert_http_api_stage_and_deployment(
        service: &ApiGatewayService,
        scope: &ApiGatewayScope,
        api_id: &str,
    ) -> String {
        let stage = service
            .create_http_api_stage(
                scope,
                api_id,
                CreateHttpApiStageInput {
                    auto_deploy: Some(true),
                    deployment_id: None,
                    description: Some("dev".to_owned()),
                    stage_name: "dev".to_owned(),
                    stage_variables: BTreeMap::new(),
                    tags: BTreeMap::new(),
                },
            )
            .expect("stage should create");
        assert_eq!(stage.stage_name, "dev");
        let fetched_stage = service
            .get_http_api_stage(scope, api_id, "dev")
            .expect("stage should fetch");
        assert_eq!(fetched_stage.stage_name, "dev");
        let listed_stages = service
            .get_http_api_stages(scope, api_id, None, Some(10))
            .expect("stages should list");
        assert_eq!(listed_stages.items.len(), 1);

        let deployment = service
            .create_http_api_deployment(
                scope,
                api_id,
                CreateHttpApiDeploymentInput {
                    description: Some(String::new()),
                    stage_name: Some("dev".to_owned()),
                },
            )
            .expect("deployment should create");
        let deployment_id = deployment.deployment_id;
        let fetched_deployment = service
            .get_http_api_deployment(scope, api_id, &deployment_id)
            .expect("deployment should fetch");
        assert_eq!(fetched_deployment.description, None);
        let listed_deployments = service
            .get_http_api_deployments(scope, api_id, None, Some(10))
            .expect("deployments should list");
        assert_eq!(listed_deployments.items.len(), 1);

        let updated_stage = service
            .update_http_api_stage(
                scope,
                api_id,
                "dev",
                UpdateHttpApiStageInput {
                    auto_deploy: Some(false),
                    deployment_id: Some(deployment_id.clone()),
                    description: Some(String::new()),
                    stage_variables: BTreeMap::from([(
                        "mode".to_owned(),
                        "blue".to_owned(),
                    )]),
                    tags: BTreeMap::from([(
                        "env".to_owned(),
                        "test".to_owned(),
                    )]),
                },
            )
            .expect("stage should update");
        assert!(!updated_stage.auto_deploy);
        assert_eq!(updated_stage.description, None);

        deployment_id
    }

    fn assert_http_api_cleanup_and_conflicts(
        service: &ApiGatewayService,
        scope: &ApiGatewayScope,
        fixture: &FullCrudFixture,
    ) {
        let deployment_in_use = service.delete_http_api_deployment(
            scope,
            &fixture.api_id,
            &fixture.deployment_id,
        );
        assert!(matches!(
            deployment_in_use,
            Err(ApiGatewayError::Conflict { resource })
                if resource.contains(&fixture.deployment_id)
        ));

        service
            .update_http_api_stage(
                scope,
                &fixture.api_id,
                "dev",
                UpdateHttpApiStageInput {
                    auto_deploy: Some(true),
                    deployment_id: Some(fixture.deployment_id.clone()),
                    description: Some("stage".to_owned()),
                    stage_variables: BTreeMap::from([(
                        "mode".to_owned(),
                        "green".to_owned(),
                    )]),
                    tags: BTreeMap::from([(
                        "env".to_owned(),
                        "prod".to_owned(),
                    )]),
                },
            )
            .expect("stage should update for deletion");

        service
            .delete_http_api_authorizer(
                scope,
                &fixture.api_id,
                &fixture.authorizer_id,
            )
            .expect("authorizer should delete");
        let route_without_authorizer = service
            .get_http_api_route(scope, &fixture.api_id, &fixture.route_id)
            .expect("route should reload");
        assert_eq!(
            route_without_authorizer.authorization_type,
            DEFAULT_HTTP_AUTHORIZATION_TYPE
        );
        assert_eq!(route_without_authorizer.authorizer_id, None);

        service
            .delete_http_api_deployment(
                scope,
                &fixture.api_id,
                &fixture.deployment_id,
            )
            .expect("deployment should delete");
        assert!(matches!(
            service.delete_http_api_deployment(
                scope,
                &fixture.api_id,
                &fixture.deployment_id,
            ),
            Err(ApiGatewayError::NotFound { resource })
                if resource.contains(&fixture.deployment_id)
        ));

        service
            .delete_http_api_route(
                scope,
                &fixture.api_id,
                &fixture.default_route_id,
            )
            .expect("default route should delete");
        service
            .delete_http_api_route(scope, &fixture.api_id, &fixture.route_id)
            .expect("route should delete");
        assert!(matches!(
            service.delete_http_api_route(
                scope,
                &fixture.api_id,
                &fixture.route_id,
            ),
            Err(ApiGatewayError::NotFound { resource })
                if resource.contains(&fixture.route_id)
        ));

        service
            .delete_http_api_stage(scope, &fixture.api_id, "dev")
            .expect("stage should delete");
        assert!(matches!(
            service.delete_http_api_stage(scope, &fixture.api_id, "dev"),
            Err(ApiGatewayError::NotFound { resource })
                if resource.contains("dev")
        ));

        service
            .delete_http_api_integration(
                scope,
                &fixture.api_id,
                &fixture.proxy_integration_id,
            )
            .expect("proxy integration should delete");
        service
            .delete_http_api_integration(
                scope,
                &fixture.api_id,
                &fixture.lambda_integration_id,
            )
            .expect("lambda integration should delete");
        assert!(matches!(
            service.delete_http_api_integration(
                scope,
                &fixture.api_id,
                &fixture.proxy_integration_id,
            ),
            Err(ApiGatewayError::NotFound { resource })
                if resource.contains(&fixture.proxy_integration_id)
        ));
        assert!(matches!(
            service.delete_http_api_authorizer(
                scope,
                &fixture.api_id,
                &fixture.authorizer_id,
            ),
            Err(ApiGatewayError::NotFound { resource })
                if resource.contains(&fixture.authorizer_id)
        ));

        service
            .delete_http_api(scope, &fixture.api_id)
            .expect("api should delete");
        assert!(matches!(
            service.delete_http_api(scope, &fixture.api_id),
            Err(ApiGatewayError::NotFound { resource })
                if resource.contains(&fixture.api_id)
        ));
        service
            .delete_http_api(scope, &fixture.second_api_id)
            .expect("second api should delete");
    }

    #[test]
    fn apigw_v2_control_private_validation_helpers_cover_edges() {
        let (api, mut state) = http_api_validation_fixture();
        assert_protocol_normalization_helpers();
        assert_http_api_lookup_helpers(&mut state);
        assert_http_route_lookup_helpers(&api);
        assert_http_integration_lookup_helpers(&api);
        assert_http_authorizer_stage_lookup_helpers(&api);
        assert_http_pagination_helpers();
        assert_route_dependency_helpers(&api);
        assert_private_integration_rejection_helpers();
        assert_lambda_integration_rejection_helpers();
        assert_http_proxy_integration_rejection_helpers();
        assert_http_integration_acceptance_helpers();
        assert_http_authorizer_validation_helpers();
        assert_http_authorizer_uri_validation_helpers();
        assert_stage_variable_validation_helpers();
    }

    fn http_api_validation_fixture() -> (StoredHttpApi, StoredApiGatewayState)
    {
        let mut api = stored_api("api1234567890");
        api.integrations.insert(
            "itg1234567890".to_owned(),
            stored_integration(
                "itg1234567890",
                "HTTP_PROXY",
                Some("https://backend.example"),
            ),
        );
        api.authorizers.insert(
            "auth1234567".to_owned(),
            stored_authorizer("auth1234567"),
        );
        api.routes.insert(
            "route1234567".to_owned(),
            stored_route(
                "route1234567",
                "GET /pets",
                "NONE",
                None,
                Some("integrations/itg1234567890"),
            ),
        );
        api.stages.insert("dev".to_owned(), stored_stage("dev", true, None));
        api.deployments.insert(
            "dep1234567890".to_owned(),
            stored_deployment(
                "dep1234567890",
                BTreeMap::new(),
                BTreeMap::new(),
            ),
        );

        let mut state = StoredApiGatewayState::default();
        state.http_apis.insert(api.api_id.clone(), api.clone());

        (api, state)
    }

    fn assert_protocol_normalization_helpers() {
        assert!(validate_http_api_protocol_type("HTTP").is_ok());
        assert!(matches!(
            validate_http_api_protocol_type("WEBSOCKET"),
            Err(ApiGatewayError::Validation { message })
                if message.contains("protocolType")
        ));
        assert_eq!(
            normalize_route_selection_expression(None)
                .expect("default expression should normalize"),
            DEFAULT_HTTP_ROUTE_SELECTION_EXPRESSION
        );
        assert!(matches!(
            normalize_route_selection_expression(Some("$request.path".to_owned())),
            Err(ApiGatewayError::Validation { message })
                if message.contains("routeSelectionExpression")
        ));
        assert_eq!(
            normalize_ip_address_type(None)
                .expect("default ip type should normalize"),
            DEFAULT_HTTP_API_IP_ADDRESS_TYPE
        );
        assert!(matches!(
            normalize_ip_address_type(Some("ipv6")),
            Err(ApiGatewayError::Validation { message })
                if message.contains("ipAddressType")
        ));
    }

    fn assert_http_api_lookup_helpers(state: &mut StoredApiGatewayState) {
        assert!(http_api(state, "api1234567890").is_ok());
        assert!(matches!(
            http_api(state, "missing"),
            Err(ApiGatewayError::NotFound { resource })
                if resource.contains("missing")
        ));
        assert!(matches!(
            http_api_mut(state, "missing"),
            Err(ApiGatewayError::NotFound { resource })
                if resource.contains("missing")
        ));
    }

    fn assert_http_route_lookup_helpers(api: &StoredHttpApi) {
        assert!(matches!(
            http_route(api, "missing"),
            Err(ApiGatewayError::NotFound { resource })
                if resource.contains("missing")
        ));
        assert!(matches!(
            http_route_mut(&mut api.clone(), "missing"),
            Err(ApiGatewayError::NotFound { resource })
                if resource.contains("missing")
        ));
    }

    fn assert_http_integration_lookup_helpers(api: &StoredHttpApi) {
        assert!(matches!(
            http_integration(api, "missing"),
            Err(ApiGatewayError::NotFound { resource })
                if resource.contains("missing")
        ));
        assert!(matches!(
            http_integration_mut(&mut api.clone(), "missing"),
            Err(ApiGatewayError::NotFound { resource })
                if resource.contains("missing")
        ));
    }

    fn assert_http_authorizer_stage_lookup_helpers(api: &StoredHttpApi) {
        assert!(matches!(
            http_authorizer(api, "missing"),
            Err(ApiGatewayError::NotFound { resource })
                if resource.contains("missing")
        ));
        assert!(matches!(
            http_authorizer_mut(&mut api.clone(), "missing"),
            Err(ApiGatewayError::NotFound { resource })
                if resource.contains("missing")
        ));
        assert!(matches!(
            http_stage(api, "missing"),
            Err(ApiGatewayError::NotFound { resource })
                if resource.contains("missing")
        ));
        assert!(matches!(
            http_stage_mut(&mut api.clone(), "missing"),
            Err(ApiGatewayError::NotFound { resource })
                if resource.contains("missing")
        ));
        assert!(matches!(
            http_deployment(api, "missing"),
            Err(ApiGatewayError::NotFound { resource })
                if resource.contains("missing")
        ));
    }

    fn assert_http_pagination_helpers() {
        let page = paginate_http_collection(vec![1, 2], None, Some(1))
            .expect("pagination should succeed");
        assert_eq!(page.items, vec![1]);
        assert!(page.next_token.is_some());
        assert!(paginate_http_collection(vec![1], Some("bad"), None).is_err());
    }

    fn assert_route_dependency_helpers(api: &StoredHttpApi) {
        assert!(
            validate_route_dependencies(
                api,
                Some("CUSTOM"),
                Some("auth1234567"),
                Some("integrations/itg1234567890")
            )
            .is_ok()
        );
        assert!(matches!(
            validate_route_dependencies(api, Some("BOGUS"), None, None),
            Err(ApiGatewayError::Validation { message })
                if message.contains("authorizationType")
        ));
        assert!(matches!(
            validate_route_dependencies(api, Some("NONE"), Some("auth1234567"), None),
            Err(ApiGatewayError::Validation { message })
                if message.contains("authorizerId")
        ));
        assert!(matches!(
            validate_route_dependencies(api, Some("JWT"), None, None),
            Err(ApiGatewayError::UnsupportedOperation { message })
                if message.contains("JWT")
        ));
        assert!(matches!(
            validate_route_dependencies(api, Some("CUSTOM"), None, Some("bad-target")),
            Err(ApiGatewayError::Validation { message })
                if message.contains("target")
        ));
        assert!(matches!(
            validate_http_authorization_type("BOGUS"),
            Err(ApiGatewayError::Validation { message })
                if message.contains("authorizationType")
        ));
        assert!(matches!(
            ensure_unique_route_key(api, "GET /pets", None),
            Err(ApiGatewayError::Conflict { resource })
                if resource.contains("GET /pets")
        ));
        assert!(
            ensure_unique_route_key(api, "GET /pets", Some("route1234567"))
                .is_ok()
        );
    }

    fn assert_private_integration_rejection_helpers() {
        assert!(matches!(
            validate_stored_http_integration(&StoredHttpIntegration {
                connection_type: "VPC_LINK".to_owned(),
                ..stored_integration(
                    "itgvpc",
                    "HTTP_PROXY",
                    Some("https://backend.example")
                )
            }),
            Err(ApiGatewayError::UnsupportedOperation { message })
                if message.contains("private integrations")
        ));
        assert!(matches!(
            validate_stored_http_integration(&StoredHttpIntegration {
                integration_uri: Some(
                    "https://${stageVariables.backend}/orders".to_owned()
                ),
                ..stored_integration(
                    "itgstg",
                    "HTTP_PROXY",
                    Some("https://backend.example")
                )
            }),
            Err(ApiGatewayError::UnsupportedOperation { message })
                if message.contains("stage-variable integration URIs")
        ));
        assert!(matches!(
            validate_stored_http_integration(&StoredHttpIntegration {
                request_parameters: BTreeMap::from([(
                    "overwrite:path".to_owned(),
                    "${stageVariables.path}".to_owned(),
                )]),
                ..stored_integration(
                    "itgrq",
                    "HTTP_PROXY",
                    Some("https://backend.example")
                )
            }),
            Err(ApiGatewayError::UnsupportedOperation { message })
                if message.contains("request parameter mappings")
        ));
    }

    fn assert_lambda_integration_rejection_helpers() {
        assert!(matches!(
            validate_stored_http_integration(&stored_integration(
                "itglambda",
                "AWS_PROXY",
                None,
            )),
            Err(ApiGatewayError::Validation { message })
                if message.contains("integrationUri")
        ));
        assert!(matches!(
            validate_stored_http_integration(&stored_integration(
                "itglambda",
                "AWS_PROXY",
                Some("https://backend.example"),
            )),
            Err(ApiGatewayError::Validation { message })
                if message.contains("integrationUri")
        ));
        assert!(matches!(
            validate_stored_http_integration(&StoredHttpIntegration {
                payload_format_version: Some("1.0".to_owned()),
                ..stored_integration(
                    "itglambda",
                    "AWS_PROXY",
                    Some(&lambda_uri("handler"))
                )
            }),
            Err(ApiGatewayError::UnsupportedOperation { message })
                if message.contains("payloadFormatVersion 2.0")
        ));
    }

    fn assert_http_proxy_integration_rejection_helpers() {
        assert!(matches!(
            validate_stored_http_integration(&stored_integration(
                "itgbad",
                "HTTP_PROXY",
                None,
            )),
            Err(ApiGatewayError::Validation { message })
                if message.contains("integrationUri")
        ));
        assert!(matches!(
            validate_stored_http_integration(&stored_integration(
                "itgbad",
                "HTTP_PROXY",
                Some("ftp://backend.example"),
            )),
            Err(ApiGatewayError::Validation { message })
                if message.contains("integrationUri")
        ));
        assert!(matches!(
            validate_stored_http_integration(&stored_integration(
                "itghttp",
                "HTTP",
                Some("https://backend.example"),
            )),
            Err(ApiGatewayError::UnsupportedOperation { message })
                if message.contains("custom HTTP integrations")
        ));
        assert!(matches!(
            validate_stored_http_integration(&stored_integration(
                "itgother",
                "MOCK",
                Some("https://backend.example"),
            )),
            Err(ApiGatewayError::Validation { message })
                if message.contains("integrationType")
        ));
    }

    fn assert_http_integration_acceptance_helpers() {
        assert!(
            validate_stored_http_integration(&stored_integration(
                "itggood",
                "HTTP_PROXY",
                Some("https://backend.example")
            ))
            .is_ok()
        );
        assert!(
            validate_stored_http_integration(&StoredHttpIntegration {
                payload_format_version: Some(
                    DEFAULT_HTTP_PAYLOAD_FORMAT_VERSION.to_owned(),
                ),
                ..stored_integration(
                    "itglambdaok",
                    "AWS_PROXY",
                    Some(&lambda_uri("handler"))
                )
            })
            .is_ok()
        );
    }

    fn assert_http_authorizer_validation_helpers() {
        assert!(matches!(
            validate_stored_http_authorizer(&StoredHttpAuthorizer {
                authorizer_uri: None,
                ..stored_authorizer("missing-uri")
            }),
            Err(ApiGatewayError::Validation { message })
                if message.contains("authorizerUri")
        ));
        assert!(matches!(
            validate_stored_http_authorizer(&StoredHttpAuthorizer {
                authorizer_uri: Some("https://bad.example".to_owned()),
                ..stored_authorizer("bad-uri")
            }),
            Err(ApiGatewayError::Validation { message })
                if message.contains("authorizerUri")
        ));
        assert!(matches!(
            validate_stored_http_authorizer(&StoredHttpAuthorizer {
                identity_source: Vec::new(),
                ..stored_authorizer("missing-identity")
            }),
            Err(ApiGatewayError::Validation { message })
                if message.contains("identitySource")
        ));
        assert!(matches!(
            validate_stored_http_authorizer(&StoredHttpAuthorizer {
                identity_source: vec![
                    "${stageVariables.identity}".to_owned(),
                ],
                ..stored_authorizer("stage-vars")
            }),
            Err(ApiGatewayError::UnsupportedOperation { message })
                if message.contains("stage-variable authorizer identities")
        ));
        assert!(matches!(
            validate_stored_http_authorizer(&StoredHttpAuthorizer {
                authorizer_payload_format_version: Some("1.0".to_owned()),
                ..stored_authorizer("bad-payload")
            }),
            Err(ApiGatewayError::UnsupportedOperation { message })
                if message.contains("payload format version 2.0")
        ));
        assert!(matches!(
            validate_stored_http_authorizer(&StoredHttpAuthorizer {
                authorizer_type: "JWT".to_owned(),
                jwt_configuration: Some(JwtConfiguration {
                    audience: vec!["aud".to_owned()],
                    issuer: "https://issuer.example".to_owned(),
                }),
                ..stored_authorizer("jwt")
            }),
            Err(ApiGatewayError::UnsupportedOperation { message })
                if message.contains("JWT")
        ));
        assert!(matches!(
            validate_stored_http_authorizer(&StoredHttpAuthorizer {
                authorizer_type: "OTHER".to_owned(),
                ..stored_authorizer("other")
            }),
            Err(ApiGatewayError::Validation { message })
                if message.contains("authorizerType")
        ));
        assert!(
            validate_stored_http_authorizer(&stored_authorizer("good"))
                .is_ok()
        );
    }

    fn assert_http_authorizer_uri_validation_helpers() {
        assert!(validate_http_authorizer_uri(&authorizer_uri("auth")).is_ok());
        assert!(matches!(
            validate_http_authorizer_uri("arn:aws:lambda:bad"),
            Err(ApiGatewayError::Validation { message })
                if message.contains("authorizerUri")
        ));
    }

    fn assert_stage_variable_validation_helpers() {
        assert!(matches!(
            validate_stage_variables(&BTreeMap::from([(
                "bad-name".to_owned(),
                "ok".to_owned(),
            )])),
            Err(ApiGatewayError::Validation { message })
                if message.contains("stage variable name")
        ));
        assert!(matches!(
            validate_stage_variables(&BTreeMap::from([(
                "good".to_owned(),
                "bad space".to_owned(),
            )])),
            Err(ApiGatewayError::Validation { message })
                if message.contains("stage variable value")
        ));
        assert!(
            validate_stage_variables(&BTreeMap::from([(
                "good_name".to_owned(),
                "ok-value:/".to_owned(),
            )]))
            .is_ok()
        );
    }

    #[test]
    fn apigw_v2_runtime_lambda_and_helper_paths_cover_edges() {
        let service = service("runtime-helpers");
        let scope = scope();
        let api_id = setup_lambda_runtime_api(&service, &scope);
        assert_lambda_proxy_invocation(&service, &scope, &api_id);
        assert_lambda_proxy_response_mapping();
        assert_runtime_http_proxy_and_error_paths(&scope);
    }

    fn setup_lambda_runtime_api(
        service: &ApiGatewayService,
        scope: &ApiGatewayScope,
    ) -> String {
        let api = service
            .create_http_api(
                scope,
                CreateHttpApiInput {
                    description: None,
                    disable_execute_api_endpoint: Some(false),
                    ip_address_type: None,
                    name: "runtime".to_owned(),
                    protocol_type: "HTTP".to_owned(),
                    route_selection_expression: None,
                    tags: BTreeMap::new(),
                    version: None,
                },
            )
            .expect("api should create");
        let api_id = api.api_id;
        let lambda_integration = service
            .create_http_api_integration(
                scope,
                &api_id,
                CreateHttpApiIntegrationInput {
                    connection_id: None,
                    connection_type: None,
                    credentials_arn: None,
                    description: None,
                    integration_method: None,
                    integration_type: "AWS_PROXY".to_owned(),
                    integration_uri: Some(lambda_uri("items:live")),
                    payload_format_version: Some(
                        DEFAULT_HTTP_PAYLOAD_FORMAT_VERSION.to_owned(),
                    ),
                    request_parameters: BTreeMap::new(),
                    request_templates: BTreeMap::new(),
                    template_selection_expression: None,
                    timeout_in_millis: None,
                },
            )
            .expect("lambda integration should create")
            .integration_id
            .expect("lambda integration id should exist");
        service
            .create_http_api_route(
                scope,
                &api_id,
                CreateHttpApiRouteInput {
                    authorization_type: Some("NONE".to_owned()),
                    authorizer_id: None,
                    operation_name: None,
                    route_key: "GET /items/{id}".to_owned(),
                    target: Some(format!("integrations/{lambda_integration}")),
                },
            )
            .expect("route should create");
        service
            .create_http_api_stage(
                scope,
                &api_id,
                CreateHttpApiStageInput {
                    auto_deploy: Some(true),
                    deployment_id: None,
                    description: None,
                    stage_name: "$default".to_owned(),
                    stage_variables: BTreeMap::from([(
                        "mode".to_owned(),
                        "blue".to_owned(),
                    )]),
                    tags: BTreeMap::new(),
                },
            )
            .expect("stage should create");

        api_id
    }

    fn assert_lambda_proxy_invocation(
        service: &ApiGatewayService,
        _scope: &ApiGatewayScope,
        api_id: &str,
    ) {
        let invocation = service
            .resolve_execute_api_by_api_id(
                api_id,
                &ExecuteApiRequest::new("localhost:443", "GET", "/items/42")
                    .with_query_string("first=1&second=two")
                    .with_header("Cookie", "session=a; theme=dark")
                    .with_header("User-Agent", "sdk-test")
                    .with_header("X-Test", "true")
                    .with_protocol("HTTP/2")
                    .with_source_ip("203.0.113.10")
                    .with_body([0xff, 0x00]),
                None,
            )
            .expect("lambda route should resolve");
        let invocation = invocation.invocation();

        match invocation.integration() {
            ExecuteApiIntegrationPlan::LambdaProxy(plan) => {
                assert_eq!(plan.function_name(), lambda_uri("items:live"));
                assert_eq!(plan.qualifier(), Some("live"));
                assert!(plan.response_is_v2());
                assert!(
                    plan.source_arn().to_string().contains("/GET/items/{id}")
                );

                let event: Value = serde_json::from_slice(plan.payload())
                    .expect("lambda payload should decode");
                assert_eq!(event["version"], "2.0");
                assert_eq!(event["pathParameters"]["id"], "42");
                assert_eq!(event["stageVariables"]["mode"], "blue");
                assert_eq!(
                    event["headers"]["x-test"],
                    Value::String("true".to_owned())
                );
                assert_eq!(
                    event["cookies"],
                    serde_json::json!(["session=a", "theme=dark"])
                );
                assert_eq!(
                    event["requestContext"]["http"]["protocol"],
                    "HTTP/2"
                );
                assert_eq!(event["requestContext"]["domainName"], "localhost");
                assert!(
                    event["isBase64Encoded"]
                        .as_bool()
                        .expect("base64 flag should be present")
                );
            }
            other => {
                panic!("expected lambda proxy integration, got {other:?}")
            }
        }
    }

    fn assert_lambda_proxy_response_mapping() {
        let mapped = map_lambda_proxy_response_v2(
            br#"{"statusCode":201,"headers":{"x-test":"ok"},"cookies":["a=b"],"body":"eyJvayI6dHJ1ZX0=","isBase64Encoded":true}"#,
        )
        .expect("structured lambda response should map");
        assert_eq!(mapped.status_code(), 201);
        assert!(
            mapped
                .headers()
                .iter()
                .any(|(name, value)| name == "set-cookie" && value == "a=b")
        );
        assert_eq!(mapped.body(), br#"{"ok":true}"#);
        assert!(matches!(
            map_lambda_proxy_response_v2(
                br#"{"body":"***","isBase64Encoded":true}"#
            ),
            Err(ExecuteApiError::IntegrationFailure { status_code, .. })
                if status_code == 502
        ));
        let object_body = map_lambda_proxy_response_v2(
            br#"{"statusCode":202,"body":{"ok":true}}"#,
        )
        .expect("JSON body should serialize");
        assert_eq!(object_body.body(), br#"{"ok":true}"#);
        let passthrough = map_lambda_proxy_response_v2(b"not-json")
            .expect("plain payload should pass through");
        assert_eq!(passthrough.body(), b"not-json");
    }

    fn assert_runtime_http_proxy_and_error_paths(scope: &ApiGatewayScope) {
        let mut disabled_state = StoredApiGatewayState::default();
        let mut disabled_api = stored_api("disabled12345");
        disabled_api.disable_execute_api_endpoint = true;
        disabled_api.stages.insert(
            "$default".to_owned(),
            stored_stage("$default", true, None),
        );
        disabled_state
            .http_apis
            .insert(disabled_api.api_id.clone(), disabled_api);
        assert!(matches!(
            prepare_http_api_execute_api(
                scope,
                &disabled_state,
                "disabled12345",
                &ExecuteApiRequest::new("localhost", "GET", "/"),
                None,
            ),
            Err(ExecuteApiError::Forbidden)
        ));

        let mut runtime_api = stored_api("runtime12345");
        runtime_api.integrations.insert(
            "proxy12345678".to_owned(),
            stored_integration(
                "proxy12345678",
                "HTTP_PROXY",
                Some("https://backend.example/{proxy}?fixed=1"),
            ),
        );
        runtime_api.routes.insert(
            "route11111111".to_owned(),
            stored_route(
                "route11111111",
                "ANY /files/{proxy+}",
                "NONE",
                None,
                Some("integrations/proxy12345678"),
            ),
        );
        runtime_api.stages.insert(
            "$default".to_owned(),
            stored_stage("$default", true, None),
        );

        let request =
            ExecuteApiRequest::new("localhost:8443", "POST", "/files/a%2Fb")
                .with_query_string("q=2")
                .with_header("Host", "ignore")
                .with_header("Content-Length", "99")
                .with_header("Connection", "keep-alive")
                .with_header("X-Test", "value")
                .with_body("ok");
        let invocation = prepare_http_api_execute_api(
            scope,
            &StoredApiGatewayState {
                http_apis: BTreeMap::from([(
                    runtime_api.api_id.clone(),
                    runtime_api.clone(),
                )]),
                ..StoredApiGatewayState::default()
            },
            &runtime_api.api_id,
            &request,
            None,
        )
        .expect("runtime api should prepare");
        match invocation.integration() {
            ExecuteApiIntegrationPlan::Http(forward) => {
                assert_eq!(forward.method(), "POST");
                assert_eq!(forward.endpoint().host(), "backend.example");
                assert_eq!(forward.path(), "/a%2Fb?fixed=1&q=2");
                assert_eq!(
                    forward.headers(),
                    &[("X-Test".to_owned(), "value".to_owned())]
                );
                assert_eq!(forward.body(), b"ok");
            }
            other => panic!("expected http integration, got {other:?}"),
        }

        assert!(matches!(
            prepare_http_api_execute_api(
                scope,
                &StoredApiGatewayState {
                    http_apis: BTreeMap::from([(
                        "missing-stage".to_owned(),
                        stored_api("missing-stage"),
                    )]),
                    ..StoredApiGatewayState::default()
                },
                "missing-stage",
                &ExecuteApiRequest::new("localhost", "GET", "/",),
                None,
            ),
            Err(ExecuteApiError::NotFound)
        ));
        assert!(matches!(
            prepare_http_api_execute_api(
                scope,
                &StoredApiGatewayState {
                    http_apis: BTreeMap::from([(
                        "badpath123456".to_owned(),
                        runtime_api,
                    )]),
                    ..StoredApiGatewayState::default()
                },
                "badpath123456",
                &ExecuteApiRequest::new("localhost", "GET", "/bad/%FF",),
                None,
            ),
            Err(ExecuteApiError::BadRequest { .. })
        ));
    }

    #[test]
    fn apigw_v2_runtime_private_helpers_cover_matching_parsing_and_headers() {
        assert_http_runtime_path_helpers();
        assert_http_runtime_lambda_helpers();
        assert_http_runtime_header_and_uri_helpers();
    }

    fn assert_http_runtime_path_helpers() {
        let greedy_route = stored_route(
            "route-greedy",
            "ANY /pets/{proxy+}",
            "NONE",
            None,
            Some("integrations/greedy"),
        );
        let matched_proxy = MatchedHttpRoute::new(
            &greedy_route,
            "/files/a/b".to_owned(),
            BTreeMap::from([("proxy".to_owned(), "a/b".to_owned())]),
        );
        assert_eq!(
            render_http_proxy_path(
                "/base/{proxy}",
                &matched_proxy,
                Some("fixed=1"),
                Some("q=2"),
            )
            .expect("proxy path should render"),
            "/base/a%2Fb?fixed=1&q=2"
        );
        assert!(matches!(
            render_http_proxy_path(
                "/base/{missing}",
                &matched_proxy,
                None,
                None,
            ),
            Err(ExecuteApiError::IntegrationFailure { status_code, .. })
                if status_code == 500
        ));
        assert_eq!(join_proxy_paths(ROOT_PATH, "/pets"), "/pets");
        assert_eq!(join_proxy_paths("/base", ROOT_PATH), "/base");
        assert_eq!(join_proxy_paths("/base", "/pets"), "/base/pets");
        assert_eq!(
            parse_http_route_target("integrations/abc123"),
            Some("abc123")
        );
        assert_eq!(parse_http_route_target("routes/abc123"), None);
        assert!(contains_stage_variable_reference("${stageVariables.path}"));
        assert!(contains_stage_variable_reference("$stageVariables.path"));
        assert!(contains_stage_variable_reference("stageVariables.path"));
        assert_eq!(
            normalize_request_path("/pets/a%2Fb")
                .expect("request path should decode"),
            "/pets/a/b"
        );
        assert!(matches!(
            normalize_request_path("/pets/%FF"),
            Err(ExecuteApiError::BadRequest { .. })
        ));
        assert_eq!(
            normalize_authority("API.EXAMPLE.test:4566"),
            "api.example.test"
        );
    }

    fn assert_http_runtime_lambda_helpers() {
        assert_eq!(
            parse_lambda_integration_function_name(&lambda_uri("fn"))
                .as_deref(),
            Some(lambda_uri("fn").as_str())
        );
        assert_eq!(
            parse_lambda_integration_function_name(&authorizer_uri("fn"))
                .as_deref(),
            Some("arn:aws:lambda:eu-west-2:000000000000:function:fn")
        );
        assert_eq!(
            parse_lambda_integration_target(&lambda_uri("fn:live"))
                .and_then(|target| target.qualifier().map(str::to_owned))
                .as_deref(),
            Some("live")
        );
        assert_eq!(
            parse_lambda_integration_target("plain-function")
                .and_then(|target| target.qualifier().map(str::to_owned)),
            None
        );
        assert!(
            execute_api_source_arn_v2(
                scope().region(),
                scope().account_id(),
                "api1234567890",
                "$default",
                "$default",
            )
            .expect("source ARN should build")
            .to_string()
            .ends_with("/$default")
        );
        assert_eq!(
            format_timestamp(0).expect("timestamp should format"),
            "1970-01-01T00:00:00Z"
        );
        assert_eq!(
            format_request_context_time(0)
                .expect("request context time should format"),
            "1/Jan/1970:00:00:00 +0000"
        );
        let runtime_error = ExecuteApiError::IntegrationFailure {
            message: "bad".to_owned(),
            status_code: 500,
        };
        assert_eq!(runtime_error.status_code(), 500);
        assert_eq!(runtime_error.message(), "bad");
    }

    fn assert_http_runtime_header_and_uri_helpers() {
        assert_eq!(
            headers_from_json_map(Some(&json!({
                "x-test": "ok",
                "ignored": 3,
            }))),
            BTreeMap::from([("x-test".to_owned(), "ok".to_owned())])
        );
        assert_eq!(
            cookie_headers(Some(&json!(["a=b", "c=d"]))),
            vec![
                ("set-cookie".to_owned(), "a=b".to_owned()),
                ("set-cookie".to_owned(), "c=d".to_owned()),
            ]
        );
        let multi_headers = multi_value_headers(&[
            ("X-Test".to_owned(), "one".to_owned()),
            ("x-test".to_owned(), "two".to_owned()),
        ]);
        assert_eq!(
            single_value_headers(&multi_headers)["x-test"],
            Value::String("two".to_owned())
        );
        assert_eq!(
            single_value_query_string_parameters(Some("a=1&b=two"))
                .expect("query should decode"),
            BTreeMap::from([
                ("a".to_owned(), "1".to_owned()),
                ("b".to_owned(), "two".to_owned()),
            ])
        );
        assert!(matches!(
            single_value_query_string_parameters(Some("a=%FF")),
            Err(ExecuteApiError::BadRequest { .. })
        ));
        assert_eq!(
            extract_cookies(&[("Cookie".to_owned(), "a=b; c=d".to_owned())]),
            json!(["a=b", "c=d"])
        );

        let parsed_http =
            parse_http_uri("https://api.example.test/orders?q=1")
                .expect("uri should parse");
        assert_eq!(parsed_http.endpoint.host(), "api.example.test");
        assert_eq!(parsed_http.endpoint.port(), 443);
        assert_eq!(parsed_http.path, "/orders");
        assert_eq!(parsed_http.query.as_deref(), Some("q=1"));
        assert_eq!(
            parse_http_uri("http://api.example.test")
                .expect("http uri should parse")
                .endpoint
                .port(),
            80
        );
        assert!(matches!(
            parse_http_uri("ftp://bad"),
            Err(ExecuteApiError::IntegrationFailure { status_code, .. })
                if status_code == 500
        ));
        assert!(matches!(
            parse_http_uri("http:///missing-host"),
            Err(ExecuteApiError::IntegrationFailure { status_code, .. })
                if status_code == 500
        ));
        assert_eq!(
            combine_query_strings("/orders", Some("a=1"), Some("b=2")),
            "/orders?a=1&b=2"
        );
        assert_eq!(combine_query_strings("/orders", None, None), "/orders");
    }

    #[test]
    fn apigw_v2_control_http_api_crud_round_trip() {
        let service = service("crud");
        let scope = scope();
        let api = service
            .create_http_api(
                &scope,
                CreateHttpApiInput {
                    description: Some("demo".to_owned()),
                    disable_execute_api_endpoint: Some(false),
                    ip_address_type: Some("ipv4".to_owned()),
                    name: "demo".to_owned(),
                    protocol_type: "HTTP".to_owned(),
                    route_selection_expression: None,
                    tags: BTreeMap::from([(
                        "env".to_owned(),
                        "test".to_owned(),
                    )]),
                    version: Some("v1".to_owned()),
                },
            )
            .expect("http api should create");
        let api_id = api.api_id;

        let integration = service
            .create_http_api_integration(
                &scope,
                &api_id,
                CreateHttpApiIntegrationInput {
                    connection_id: None,
                    connection_type: None,
                    credentials_arn: None,
                    description: Some("proxy".to_owned()),
                    integration_method: None,
                    integration_type: "HTTP_PROXY".to_owned(),
                    integration_uri: Some(
                        "http://127.0.0.1/backend".to_owned(),
                    ),
                    payload_format_version: None,
                    request_parameters: BTreeMap::new(),
                    request_templates: BTreeMap::new(),
                    template_selection_expression: None,
                    timeout_in_millis: Some(5_000),
                },
            )
            .expect("integration should create");
        let integration_id = integration
            .integration_id
            .expect("integration id should be present");

        let authorizer = service
            .create_http_api_authorizer(
                &scope,
                &api_id,
                CreateHttpApiAuthorizerInput {
                    authorizer_credentials_arn: None,
                    authorizer_payload_format_version: Some("2.0".to_owned()),
                    authorizer_result_ttl_in_seconds: Some(300),
                    authorizer_type: "REQUEST".to_owned(),
                    authorizer_uri: Some("arn:aws:apigateway:eu-west-2:lambda:path/2015-03-31/functions/arn:aws:lambda:eu-west-2:000000000000:function:auth/invocations".to_owned()),
                    enable_simple_responses: Some(true),
                    identity_source: vec!["$request.header.Authorization".to_owned()],
                    identity_validation_expression: None,
                    jwt_configuration: None,
                    name: "auth".to_owned(),
                },
            )
            .expect("authorizer should create");
        let authorizer_id =
            authorizer.authorizer_id.expect("authorizer id should be present");

        let route = service
            .create_http_api_route(
                &scope,
                &api_id,
                CreateHttpApiRouteInput {
                    authorization_type: Some("NONE".to_owned()),
                    authorizer_id: None,
                    operation_name: Some("ListPets".to_owned()),
                    route_key: "GET /pets".to_owned(),
                    target: Some(format!("integrations/{integration_id}")),
                },
            )
            .expect("route should create");
        let route_id = route.route_id;

        let deployment = service
            .create_http_api_deployment(
                &scope,
                &api_id,
                CreateHttpApiDeploymentInput {
                    description: Some("first".to_owned()),
                    stage_name: None,
                },
            )
            .expect("deployment should create");
        let deployment_id = deployment.deployment_id;

        let stage = service
            .create_http_api_stage(
                &scope,
                &api_id,
                CreateHttpApiStageInput {
                    auto_deploy: Some(false),
                    deployment_id: Some(deployment_id.clone()),
                    description: Some("dev".to_owned()),
                    stage_name: "dev".to_owned(),
                    stage_variables: BTreeMap::from([(
                        "mode".to_owned(),
                        "blue".to_owned(),
                    )]),
                    tags: BTreeMap::from([(
                        "stage".to_owned(),
                        "dev".to_owned(),
                    )]),
                },
            )
            .expect("stage should create");
        assert_eq!(stage.stage_name, "dev");

        let updated_api = service
            .update_http_api(
                &scope,
                &api_id,
                UpdateHttpApiInput {
                    description: Some("updated".to_owned()),
                    disable_execute_api_endpoint: Some(true),
                    ip_address_type: Some("dualstack".to_owned()),
                    name: Some("demo-v2".to_owned()),
                    route_selection_expression: None,
                    version: Some("v2".to_owned()),
                },
            )
            .expect("api should update");
        assert_eq!(updated_api.name, "demo-v2");
        assert!(updated_api.disable_execute_api_endpoint);

        let updated_integration = service
            .update_http_api_integration(
                &scope,
                &api_id,
                &integration_id,
                UpdateHttpApiIntegrationInput {
                    connection_id: None,
                    connection_type: None,
                    credentials_arn: None,
                    description: Some("proxy-v2".to_owned()),
                    integration_method: None,
                    integration_type: None,
                    integration_uri: Some(
                        "http://127.0.0.1/backend-v2".to_owned(),
                    ),
                    payload_format_version: None,
                    request_parameters: BTreeMap::new(),
                    request_templates: BTreeMap::new(),
                    template_selection_expression: None,
                    timeout_in_millis: Some(8_000),
                },
            )
            .expect("integration should update");
        assert_eq!(
            updated_integration.integration_uri.as_deref(),
            Some("http://127.0.0.1/backend-v2")
        );

        let updated_authorizer = service
            .update_http_api_authorizer(
                &scope,
                &api_id,
                &authorizer_id,
                UpdateHttpApiAuthorizerInput {
                    authorizer_credentials_arn: None,
                    authorizer_payload_format_version: Some("2.0".to_owned()),
                    authorizer_result_ttl_in_seconds: Some(120),
                    authorizer_type: Some("REQUEST".to_owned()),
                    authorizer_uri: Some("arn:aws:apigateway:eu-west-2:lambda:path/2015-03-31/functions/arn:aws:lambda:eu-west-2:000000000000:function:auth-v2/invocations".to_owned()),
                    enable_simple_responses: Some(false),
                    identity_source: vec!["$request.header.X-Auth".to_owned()],
                    identity_validation_expression: None,
                    jwt_configuration: None,
                    name: Some("auth-v2".to_owned()),
                },
            )
            .expect("authorizer should update");
        assert_eq!(updated_authorizer.name.as_deref(), Some("auth-v2"));

        let updated_route = service
            .update_http_api_route(
                &scope,
                &api_id,
                &route_id,
                UpdateHttpApiRouteInput {
                    authorization_type: Some("CUSTOM".to_owned()),
                    authorizer_id: Some(authorizer_id.clone()),
                    operation_name: Some("ListPetsV2".to_owned()),
                    route_key: Some("GET /pets/all".to_owned()),
                    target: Some(format!("integrations/{integration_id}")),
                },
            )
            .expect("route should update");
        assert_eq!(updated_route.route_key, "GET /pets/all");
        assert_eq!(
            updated_route.authorizer_id.as_deref(),
            Some(authorizer_id.as_str())
        );

        let updated_stage = service
            .update_http_api_stage(
                &scope,
                &api_id,
                "dev",
                UpdateHttpApiStageInput {
                    auto_deploy: Some(true),
                    deployment_id: Some(deployment_id.clone()),
                    description: Some("dev-auto".to_owned()),
                    stage_variables: BTreeMap::from([(
                        "mode".to_owned(),
                        "green".to_owned(),
                    )]),
                    tags: BTreeMap::from([(
                        "stage".to_owned(),
                        "updated".to_owned(),
                    )]),
                },
            )
            .expect("stage should update");
        assert!(updated_stage.auto_deploy);
        assert_eq!(
            updated_stage.stage_variables.get("mode").map(String::as_str),
            Some("green")
        );

        let listed_routes = service
            .get_http_api_routes(&scope, &api_id, None, None)
            .expect("routes should list");
        assert_eq!(listed_routes.items.len(), 1);
        let listed_integrations = service
            .get_http_api_integrations(&scope, &api_id, None, None)
            .expect("integrations should list");
        assert_eq!(listed_integrations.items.len(), 1);
        let listed_authorizers = service
            .get_http_api_authorizers(&scope, &api_id, None, None)
            .expect("authorizers should list");
        assert_eq!(listed_authorizers.items.len(), 1);
        let listed_stages = service
            .get_http_api_stages(&scope, &api_id, None, None)
            .expect("stages should list");
        assert_eq!(listed_stages.items.len(), 1);
        let listed_deployments = service
            .get_http_api_deployments(&scope, &api_id, None, None)
            .expect("deployments should list");
        assert_eq!(listed_deployments.items.len(), 1);

        service
            .delete_http_api_route(&scope, &api_id, &route_id)
            .expect("route should delete");
        service
            .delete_http_api_authorizer(&scope, &api_id, &authorizer_id)
            .expect("authorizer should delete");
        service
            .delete_http_api_stage(&scope, &api_id, "dev")
            .expect("stage should delete");
        service
            .delete_http_api_deployment(&scope, &api_id, &deployment_id)
            .expect("deployment should delete");
        service
            .delete_http_api_integration(&scope, &api_id, &integration_id)
            .expect("integration should delete");
        service.delete_http_api(&scope, &api_id).expect("api should delete");
    }

    #[test]
    fn apigw_v2_runtime_exact_route_beats_greedy_and_default() {
        let service = service("route-precedence");
        let scope = scope();
        let api = service
            .create_http_api(
                &scope,
                CreateHttpApiInput {
                    description: None,
                    disable_execute_api_endpoint: Some(false),
                    ip_address_type: None,
                    name: "demo".to_owned(),
                    protocol_type: "HTTP".to_owned(),
                    route_selection_expression: None,
                    tags: BTreeMap::new(),
                    version: None,
                },
            )
            .expect("api should create");
        let api_id = api.api_id;

        let exact = service
            .create_http_api_integration(
                &scope,
                &api_id,
                CreateHttpApiIntegrationInput {
                    connection_id: None,
                    connection_type: None,
                    credentials_arn: None,
                    description: None,
                    integration_method: None,
                    integration_type: "HTTP_PROXY".to_owned(),
                    integration_uri: Some(
                        "http://exact.example/base".to_owned(),
                    ),
                    payload_format_version: None,
                    request_parameters: BTreeMap::new(),
                    request_templates: BTreeMap::new(),
                    template_selection_expression: None,
                    timeout_in_millis: None,
                },
            )
            .expect("exact integration should create")
            .integration_id
            .expect("exact integration id should exist");
        let greedy = service
            .create_http_api_integration(
                &scope,
                &api_id,
                CreateHttpApiIntegrationInput {
                    connection_id: None,
                    connection_type: None,
                    credentials_arn: None,
                    description: None,
                    integration_method: None,
                    integration_type: "HTTP_PROXY".to_owned(),
                    integration_uri: Some(
                        "http://greedy.example/base/{proxy}".to_owned(),
                    ),
                    payload_format_version: None,
                    request_parameters: BTreeMap::new(),
                    request_templates: BTreeMap::new(),
                    template_selection_expression: None,
                    timeout_in_millis: None,
                },
            )
            .expect("greedy integration should create")
            .integration_id
            .expect("greedy integration id should exist");
        let fallback = service
            .create_http_api_integration(
                &scope,
                &api_id,
                CreateHttpApiIntegrationInput {
                    connection_id: None,
                    connection_type: None,
                    credentials_arn: None,
                    description: None,
                    integration_method: None,
                    integration_type: "HTTP_PROXY".to_owned(),
                    integration_uri: Some("http://default.example".to_owned()),
                    payload_format_version: None,
                    request_parameters: BTreeMap::new(),
                    request_templates: BTreeMap::new(),
                    template_selection_expression: None,
                    timeout_in_millis: None,
                },
            )
            .expect("default integration should create")
            .integration_id
            .expect("default integration id should exist");

        service
            .create_http_api_route(
                &scope,
                &api_id,
                CreateHttpApiRouteInput {
                    authorization_type: Some("NONE".to_owned()),
                    authorizer_id: None,
                    operation_name: None,
                    route_key: "GET /pets/dog/1".to_owned(),
                    target: Some(format!("integrations/{exact}")),
                },
            )
            .expect("exact route should create");
        service
            .create_http_api_route(
                &scope,
                &api_id,
                CreateHttpApiRouteInput {
                    authorization_type: Some("NONE".to_owned()),
                    authorizer_id: None,
                    operation_name: None,
                    route_key: "GET /pets/{proxy+}".to_owned(),
                    target: Some(format!("integrations/{greedy}")),
                },
            )
            .expect("greedy route should create");
        service
            .create_http_api_route(
                &scope,
                &api_id,
                CreateHttpApiRouteInput {
                    authorization_type: Some("NONE".to_owned()),
                    authorizer_id: None,
                    operation_name: None,
                    route_key: "$default".to_owned(),
                    target: Some(format!("integrations/{fallback}")),
                },
            )
            .expect("default route should create");
        service
            .create_http_api_stage(
                &scope,
                &api_id,
                CreateHttpApiStageInput {
                    auto_deploy: Some(true),
                    deployment_id: None,
                    description: None,
                    stage_name: "$default".to_owned(),
                    stage_variables: BTreeMap::new(),
                    tags: BTreeMap::new(),
                },
            )
            .expect("default stage should create");

        let exact_invocation = service
            .resolve_execute_api_by_api_id(
                &api_id,
                &ExecuteApiRequest::new("localhost", "GET", "/pets/dog/1"),
                None,
            )
            .expect("exact route should resolve");
        let greedy_invocation = service
            .resolve_execute_api_by_api_id(
                &api_id,
                &ExecuteApiRequest::new("localhost", "GET", "/pets/cat/2"),
                None,
            )
            .expect("greedy route should resolve");
        let default_invocation = service
            .resolve_execute_api_by_api_id(
                &api_id,
                &ExecuteApiRequest::new("localhost", "POST", "/orders/5"),
                None,
            )
            .expect("default route should resolve");

        assert_eq!(
            exact_invocation.invocation().resource_path(),
            "GET /pets/dog/1"
        );
        assert_eq!(
            greedy_invocation.invocation().resource_path(),
            "GET /pets/{proxy+}"
        );
        assert_eq!(
            default_invocation.invocation().resource_path(),
            "$default"
        );

        match greedy_invocation.invocation().integration() {
            ExecuteApiIntegrationPlan::Http(request) => {
                assert_eq!(request.endpoint().host(), "greedy.example");
                assert_eq!(request.path(), "/base/cat%2F2");
            }
            other => panic!("expected HTTP integration, got {other:?}"),
        }
        match default_invocation.invocation().integration() {
            ExecuteApiIntegrationPlan::Http(request) => {
                assert_eq!(request.endpoint().host(), "default.example");
                assert_eq!(request.path(), "/orders/5");
            }
            other => panic!("expected HTTP integration, got {other:?}"),
        }
    }

    #[test]
    fn apigw_v2_runtime_invalid_route_keys_and_unsupported_stage_variables_fail_explicitly()
     {
        let service = service("validation");
        let scope = scope();
        let api = service
            .create_http_api(
                &scope,
                CreateHttpApiInput {
                    description: None,
                    disable_execute_api_endpoint: Some(false),
                    ip_address_type: None,
                    name: "demo".to_owned(),
                    protocol_type: "HTTP".to_owned(),
                    route_selection_expression: None,
                    tags: BTreeMap::new(),
                    version: None,
                },
            )
            .expect("api should create");

        let invalid_route = service.create_http_api_route(
            &scope,
            &api.api_id,
            CreateHttpApiRouteInput {
                authorization_type: Some("NONE".to_owned()),
                authorizer_id: None,
                operation_name: None,
                route_key: "GET pets".to_owned(),
                target: None,
            },
        );
        assert!(matches!(
            invalid_route,
            Err(ApiGatewayError::Validation { message })
                if message.contains("invalid routeKey")
        ));

        let stage_variable_integration = service.create_http_api_integration(
            &scope,
            &api.api_id,
            CreateHttpApiIntegrationInput {
                connection_id: None,
                connection_type: None,
                credentials_arn: None,
                description: None,
                integration_method: None,
                integration_type: "HTTP_PROXY".to_owned(),
                integration_uri: Some(
                    "https://${stageVariables.backend}/orders".to_owned(),
                ),
                payload_format_version: None,
                request_parameters: BTreeMap::new(),
                request_templates: BTreeMap::new(),
                template_selection_expression: None,
                timeout_in_millis: None,
            },
        );
        assert!(matches!(
            stage_variable_integration,
            Err(ApiGatewayError::UnsupportedOperation { message })
                if message.contains("stage-variable")
        ));

        let jwt_authorizer = service.create_http_api_authorizer(
            &scope,
            &api.api_id,
            CreateHttpApiAuthorizerInput {
                authorizer_credentials_arn: None,
                authorizer_payload_format_version: None,
                authorizer_result_ttl_in_seconds: None,
                authorizer_type: "JWT".to_owned(),
                authorizer_uri: None,
                enable_simple_responses: None,
                identity_source: vec![
                    "$request.header.Authorization".to_owned(),
                ],
                identity_validation_expression: None,
                jwt_configuration: Some(JwtConfiguration {
                    audience: vec!["test".to_owned()],
                    issuer: "https://issuer.example".to_owned(),
                }),
                name: "jwt".to_owned(),
            },
        );
        assert!(matches!(
            jwt_authorizer,
            Err(ApiGatewayError::UnsupportedOperation { message })
                if message.contains("JWT")
        ));
    }
}
