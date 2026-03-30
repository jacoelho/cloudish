use aws::{AccountId, Clock, RegionId, SharedAdvertisedEdge};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::UNIX_EPOCH;
use storage::{StorageFactory, StorageHandle};

use crate::{
    errors::ApiGatewayError, execute_api, scope::ApiGatewayScope, v2,
};
const DEFAULT_API_KEY_SOURCE: &str = "HEADER";
const EXECUTE_API_ID_WIDTH: usize = 10;
const GLOBAL_ROUTING_STATE_KEY: &str = "global";
const RESOURCE_ID_WIDTH: usize = 8;
const DEPLOYMENT_ID_WIDTH: usize = 10;
const AUTHORIZER_ID_WIDTH: usize = 8;
const REQUEST_VALIDATOR_ID_WIDTH: usize = 8;
const API_KEY_ID_WIDTH: usize = 10;
const USAGE_PLAN_ID_WIDTH: usize = 10;
pub(crate) const ROOT_PATH: &str = "/";
const TAGS_LIMIT: usize = 50;
pub(crate) const HTTP_ROUTE_ID_WIDTH: usize = 10;
pub(crate) const HTTP_INTEGRATION_ID_WIDTH: usize = 10;
pub(crate) const HTTP_DEPLOYMENT_ID_WIDTH: usize = 10;
pub(crate) const HTTP_API_AUTHORIZER_ID_WIDTH: usize = 10;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EndpointConfiguration {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ip_address_type: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub types: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub vpc_endpoint_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RestApi {
    pub api_key_source: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub binary_media_types: Vec<String>,
    pub created_date: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub disable_execute_api_endpoint: bool,
    pub endpoint_configuration: EndpointConfiguration,
    pub id: String,
    pub name: String,
    pub root_resource_id: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub tags: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ApiResource {
    pub id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_id: Option<String>,
    pub path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path_part: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MethodResponse {
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub response_models: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub response_parameters: BTreeMap<String, bool>,
    pub status_code: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Integration {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub cache_key_parameters: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache_namespace: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub connection_id: Option<String>,
    pub connection_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_handling: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub credentials: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub http_method: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub integration_http_method: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub passthrough_behavior: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub request_parameters: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub request_templates: BTreeMap<String, String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout_in_millis: Option<u32>,
    pub type_: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uri: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ApiMethod {
    pub api_key_required: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub authorizer_id: Option<String>,
    pub authorization_type: String,
    pub http_method: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub method_integration: Option<Integration>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub method_responses: BTreeMap<String, MethodResponse>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operation_name: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub request_models: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub request_parameters: BTreeMap<String, bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_validator_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Deployment {
    pub created_date: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Stage {
    pub created_date: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deployment_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub last_updated_date: u64,
    pub stage_name: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub tags: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub variables: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Authorizer {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub authorizer_credentials: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub authorizer_result_ttl_in_seconds: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub authorizer_uri: Option<String>,
    pub id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub identity_source: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub identity_validation_expression: Option<String>,
    pub name: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub provider_arns: Vec<String>,
    pub type_: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RequestValidator {
    pub id: String,
    pub name: String,
    pub validate_request_body: bool,
    pub validate_request_parameters: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ApiKey {
    pub created_date: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub customer_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub enabled: bool,
    pub id: String,
    pub last_updated_date: u64,
    pub name: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub stage_keys: Vec<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub tags: BTreeMap<String, String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UsagePlanApiStage {
    pub api_id: String,
    pub stage: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ThrottleSettings {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub burst_limit: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rate_limit: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QuotaSettings {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub offset: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub period: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UsagePlan {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub api_stages: Vec<UsagePlanApiStage>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub id: String,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub product_code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quota: Option<QuotaSettings>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub tags: BTreeMap<String, String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub throttle: Option<ThrottleSettings>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UsagePlanKey {
    pub id: String,
    pub name: String,
    pub type_: String,
    pub value: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct GetTagsOutput {
    pub tags: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ItemCollection<T> {
    pub item: Vec<T>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub position: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct UsagePlanKeyCollection {
    pub item: Vec<UsagePlanKey>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub position: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PatchOperation {
    pub op: String,
    pub path: String,
    pub value: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateRestApiInput {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub binary_media_types: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub disable_execute_api_endpoint: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub endpoint_configuration: Option<EndpointConfiguration>,
    pub name: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub tags: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateResourceInput {
    pub path_part: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PutMethodInput {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub api_key_required: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub authorizer_id: Option<String>,
    pub authorization_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operation_name: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub request_models: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub request_parameters: BTreeMap<String, bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_validator_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PutMethodResponseInput {
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub response_models: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub response_parameters: BTreeMap<String, bool>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PutIntegrationInput {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub cache_key_parameters: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache_namespace: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub connection_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub connection_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_handling: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub credentials: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub http_method: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub integration_http_method: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub passthrough_behavior: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub request_parameters: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub request_templates: BTreeMap<String, String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout_in_millis: Option<u32>,
    #[serde(rename = "type")]
    pub type_: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uri: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateDeploymentInput {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stage_description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stage_name: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub variables: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateStageInput {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache_cluster_enabled: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache_cluster_size: Option<String>,
    pub deployment_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub stage_name: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub tags: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub variables: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateAuthorizerInput {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub authorizer_credentials: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub authorizer_result_ttl_in_seconds: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub authorizer_uri: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub identity_source: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub identity_validation_expression: Option<String>,
    pub name: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub provider_arns: Vec<String>,
    #[serde(rename = "type")]
    pub type_: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateRequestValidatorInput {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub validate_request_body: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub validate_request_parameters: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateApiKeyInput {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub customer_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub enabled: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub stage_keys: Vec<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub tags: BTreeMap<String, String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateUsagePlanInput {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub api_stages: Vec<UsagePlanApiStage>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub product_code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quota: Option<QuotaSettings>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub tags: BTreeMap<String, String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub throttle: Option<ThrottleSettings>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateUsagePlanKeyInput {
    pub key_id: String,
    pub key_type: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct TagResourceInput {
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub tags: BTreeMap<String, String>,
}

#[derive(Clone)]
pub struct ApiGatewayService {
    pub(crate) advertised_edge: SharedAdvertisedEdge,
    clock: Arc<dyn Clock>,
    global_store: StorageHandle<String, StoredApiGatewayGlobalState>,
    mutation_lock: Arc<Mutex<()>>,
    state_store: StorageHandle<ApiGatewayStateKey, StoredApiGatewayState>,
}

impl ApiGatewayService {
    pub fn new(factory: &StorageFactory, clock: Arc<dyn Clock>) -> Self {
        Self::with_advertised_edge(
            factory,
            clock,
            SharedAdvertisedEdge::default(),
        )
    }

    pub fn with_advertised_edge(
        factory: &StorageFactory,
        clock: Arc<dyn Clock>,
        advertised_edge: SharedAdvertisedEdge,
    ) -> Self {
        Self {
            advertised_edge,
            clock,
            global_store: factory.create("apigateway", "runtime-routing"),
            mutation_lock: Arc::new(Mutex::new(())),
            state_store: factory.create("apigateway", "v1-control"),
        }
    }

    #[doc = "# Errors\n\nReturns `ApiGatewayError` when the request fails validation, referenced resources are missing, or the API Gateway control-plane state cannot be loaded or persisted."]
    pub fn create_rest_api(
        &self,
        scope: &ApiGatewayScope,
        input: CreateRestApiInput,
    ) -> Result<RestApi, ApiGatewayError> {
        validate_name("name", &input.name)?;
        validate_tags(&input.tags)?;
        let endpoint_configuration =
            normalize_rest_api_endpoint_configuration(
                input.endpoint_configuration,
            )?;

        let _guard = self.lock_state();
        let mut state = self.load_state(scope);
        let api_id = self.allocate_execute_api_id()?;
        let root_resource_id = next_identifier(
            &mut state.next_resource_sequence,
            RESOURCE_ID_WIDTH,
        );
        let created_date = self.now_epoch_seconds();
        let root = StoredApiResource {
            id: root_resource_id.clone(),
            methods: BTreeMap::new(),
            parent_id: None,
            path: ROOT_PATH.to_owned(),
            path_part: None,
        };
        let api = StoredRestApi {
            api_key_source: DEFAULT_API_KEY_SOURCE.to_owned(),
            binary_media_types: input.binary_media_types,
            created_date,
            description: optional_empty_to_none(input.description),
            disable_execute_api_endpoint: input
                .disable_execute_api_endpoint
                .unwrap_or(false),
            endpoint_configuration,
            id: api_id.clone(),
            name: input.name,
            resources: BTreeMap::from([(root_resource_id.clone(), root)]),
            root_resource_id,
            deployments: BTreeMap::new(),
            stages: BTreeMap::new(),
            authorizers: BTreeMap::new(),
            request_validators: BTreeMap::new(),
            tags: input.tags,
        };
        let response = api.to_rest_api();

        state.rest_apis.insert(api_id, api);
        self.save_state(scope, state)?;

        Ok(response)
    }

    #[doc = "# Errors\n\nReturns `ApiGatewayError` when the request fails validation, referenced resources are missing, or the API Gateway control-plane state cannot be loaded or persisted."]
    pub fn get_rest_api(
        &self,
        scope: &ApiGatewayScope,
        api_id: &str,
    ) -> Result<RestApi, ApiGatewayError> {
        let state = self.load_state(scope);
        let api = rest_api(&state, api_id)?;

        Ok(api.to_rest_api())
    }

    #[doc = "# Errors\n\nReturns `ApiGatewayError` when the request fails validation, referenced resources are missing, or the API Gateway control-plane state cannot be loaded or persisted."]
    pub fn get_rest_apis(
        &self,
        scope: &ApiGatewayScope,
        position: Option<&str>,
        limit: Option<u32>,
    ) -> Result<ItemCollection<RestApi>, ApiGatewayError> {
        let state = self.load_state(scope);
        let apis = state
            .rest_apis
            .values()
            .map(StoredRestApi::to_rest_api)
            .collect::<Vec<_>>();

        paginate(apis, position, limit)
    }

    #[doc = "# Errors\n\nReturns `ApiGatewayError` when the request fails validation, referenced resources are missing, or the API Gateway control-plane state cannot be loaded or persisted."]
    pub fn update_rest_api(
        &self,
        scope: &ApiGatewayScope,
        api_id: &str,
        patch_operations: &[PatchOperation],
    ) -> Result<RestApi, ApiGatewayError> {
        let mut state = self.load_state(scope);
        let api = rest_api_mut(&mut state, api_id)?;

        for operation in patch_operations {
            match patch_path(operation, "rest api")? {
                ("replace", "/name") => {
                    let value = required_patch_value(operation)?;
                    validate_name("name", value)?;
                    api.name = value.to_owned();
                }
                ("replace", "/description") => {
                    api.description =
                        optional_empty_to_none(operation.value.clone());
                }
                ("replace", "/disableExecuteApiEndpoint") => {
                    api.disable_execute_api_endpoint =
                        parse_patch_bool(operation)?;
                }
                ("add" | "replace", "/binaryMediaTypes") => {
                    return Err(ApiGatewayError::UnsupportedPatchPath {
                        resource: "rest api",
                        path: operation.path.clone(),
                    });
                }
                _ => {
                    return Err(ApiGatewayError::UnsupportedPatchPath {
                        resource: "rest api",
                        path: operation.path.clone(),
                    });
                }
            }
        }

        let response = api.to_rest_api();
        self.save_state(scope, state)?;

        Ok(response)
    }

    #[doc = "# Errors\n\nReturns `ApiGatewayError` when the request fails validation, referenced resources are missing, or the API Gateway control-plane state cannot be loaded or persisted."]
    pub fn delete_rest_api(
        &self,
        scope: &ApiGatewayScope,
        api_id: &str,
    ) -> Result<(), ApiGatewayError> {
        let mut state = self.load_state(scope);
        if state.rest_apis.remove(api_id).is_none() {
            return Err(ApiGatewayError::NotFound {
                resource: format!("Rest API {api_id}"),
            });
        }

        for usage_plan in state.usage_plans.values_mut() {
            usage_plan.api_stages.retain(|stage| stage.api_id != api_id);
        }

        self.save_state(scope, state)
    }

    #[doc = "# Errors\n\nReturns `ApiGatewayError` when the request fails validation, referenced resources are missing, or the API Gateway control-plane state cannot be loaded or persisted."]
    pub fn create_resource(
        &self,
        scope: &ApiGatewayScope,
        api_id: &str,
        parent_id: &str,
        input: CreateResourceInput,
    ) -> Result<ApiResource, ApiGatewayError> {
        validate_path_part(&input.path_part)?;

        let mut state = self.load_state(scope);
        let resource_id = next_identifier(
            &mut state.next_resource_sequence,
            RESOURCE_ID_WIDTH,
        );
        let api = rest_api_mut(&mut state, api_id)?;
        let parent_path = resource(api, parent_id)?.path.clone();
        validate_resource_hierarchy(api, parent_id, &input.path_part)?;
        let path = join_resource_path(&parent_path, &input.path_part);
        let resource = StoredApiResource {
            id: resource_id.clone(),
            methods: BTreeMap::new(),
            parent_id: Some(parent_id.to_owned()),
            path,
            path_part: Some(input.path_part),
        };
        let response = resource.to_resource();
        api.resources.insert(resource_id, resource);
        self.save_state(scope, state)?;

        Ok(response)
    }

    #[doc = "# Errors\n\nReturns `ApiGatewayError` when the request fails validation, referenced resources are missing, or the API Gateway control-plane state cannot be loaded or persisted."]
    pub fn get_resource(
        &self,
        scope: &ApiGatewayScope,
        api_id: &str,
        resource_id: &str,
    ) -> Result<ApiResource, ApiGatewayError> {
        let state = self.load_state(scope);
        let api = rest_api(&state, api_id)?;
        let resource = resource(api, resource_id)?;

        Ok(resource.to_resource())
    }

    #[doc = "# Errors\n\nReturns `ApiGatewayError` when the request fails validation, referenced resources are missing, or the API Gateway control-plane state cannot be loaded or persisted."]
    pub fn get_resources(
        &self,
        scope: &ApiGatewayScope,
        api_id: &str,
        position: Option<&str>,
        limit: Option<u32>,
    ) -> Result<ItemCollection<ApiResource>, ApiGatewayError> {
        let state = self.load_state(scope);
        let api = rest_api(&state, api_id)?;
        let resources = api
            .resources
            .values()
            .map(|resource| resource.to_resource())
            .collect::<Vec<_>>();

        paginate(resources, position, limit)
    }

    #[doc = "# Errors\n\nReturns `ApiGatewayError` when the request fails validation, referenced resources are missing, or the API Gateway control-plane state cannot be loaded or persisted."]
    pub fn update_resource(
        &self,
        scope: &ApiGatewayScope,
        api_id: &str,
        resource_id: &str,
        patch_operations: &[PatchOperation],
    ) -> Result<ApiResource, ApiGatewayError> {
        let mut state = self.load_state(scope);
        let api = rest_api_mut(&mut state, api_id)?;
        for operation in patch_operations {
            match patch_path(operation, "resource")? {
                ("replace", "/pathPart") => {
                    let value = required_patch_value(operation)?;
                    validate_path_part(value)?;
                    validate_resource_patch(api, resource_id, value, None)?;
                    let resource = resource_mut(api, resource_id)?;
                    resource.path_part = Some(value.to_owned());
                }
                ("replace", "/parentId") => {
                    let value = required_patch_value(operation)?;
                    validate_resource_patch(
                        api,
                        resource_id,
                        current_path_part(api, resource_id)?,
                        Some(value),
                    )?;
                    let resource = resource_mut(api, resource_id)?;
                    resource.parent_id = Some(value.to_owned());
                }
                _ => {
                    return Err(ApiGatewayError::UnsupportedPatchPath {
                        resource: "resource",
                        path: operation.path.clone(),
                    });
                }
            }
        }

        recompute_resource_paths(api)?;
        let response = resource(api, resource_id)?.to_resource();
        self.save_state(scope, state)?;

        Ok(response)
    }

    #[doc = "# Errors\n\nReturns `ApiGatewayError` when the request fails validation, referenced resources are missing, or the API Gateway control-plane state cannot be loaded or persisted."]
    pub fn delete_resource(
        &self,
        scope: &ApiGatewayScope,
        api_id: &str,
        resource_id: &str,
    ) -> Result<(), ApiGatewayError> {
        let mut state = self.load_state(scope);
        let api = rest_api_mut(&mut state, api_id)?;
        if api.root_resource_id == resource_id {
            return Err(ApiGatewayError::Validation {
                message: "the root resource cannot be deleted".to_owned(),
            });
        }
        resource(api, resource_id)?;
        let to_delete = descendant_resource_ids(api, resource_id);
        for id in to_delete {
            api.resources.remove(&id);
        }
        self.save_state(scope, state)
    }

    #[doc = "# Errors\n\nReturns `ApiGatewayError` when the request fails validation, referenced resources are missing, or the API Gateway control-plane state cannot be loaded or persisted."]
    pub fn put_method(
        &self,
        scope: &ApiGatewayScope,
        api_id: &str,
        resource_id: &str,
        http_method: &str,
        input: PutMethodInput,
    ) -> Result<ApiMethod, ApiGatewayError> {
        let http_method = normalize_http_method(http_method)?;
        validate_authorization_type(&input.authorization_type)?;

        let mut state = self.load_state(scope);
        let api = rest_api_mut(&mut state, api_id)?;
        resource(api, resource_id)?;
        if resource(api, resource_id)?.methods.contains_key(&http_method) {
            return Err(ApiGatewayError::Conflict {
                resource: format!(
                    "Method {http_method} for resource {resource_id}"
                ),
            });
        }
        if let Some(authorizer_id) = input.authorizer_id.as_deref() {
            authorizer(api, authorizer_id)?;
        }
        if let Some(request_validator_id) =
            input.request_validator_id.as_deref()
        {
            request_validator(api, request_validator_id)?;
        }
        let method = StoredApiMethod {
            api_key_required: input.api_key_required.unwrap_or(false),
            authorizer_id: input.authorizer_id,
            authorization_type: input.authorization_type,
            http_method: http_method.clone(),
            integration: resource(api, resource_id)?
                .methods
                .get(&http_method)
                .and_then(|method| method.integration.clone()),
            method_responses: resource(api, resource_id)?
                .methods
                .get(&http_method)
                .map(|method| method.method_responses.clone())
                .unwrap_or_default(),
            operation_name: optional_empty_to_none(input.operation_name),
            request_models: input.request_models,
            request_parameters: input.request_parameters,
            request_validator_id: input.request_validator_id,
        };
        let response = method.to_method();
        resource_mut(api, resource_id)?.methods.insert(http_method, method);
        self.save_state(scope, state)?;

        Ok(response)
    }

    #[doc = "# Errors\n\nReturns `ApiGatewayError` when the request fails validation, referenced resources are missing, or the API Gateway control-plane state cannot be loaded or persisted."]
    pub fn get_method(
        &self,
        scope: &ApiGatewayScope,
        api_id: &str,
        resource_id: &str,
        http_method: &str,
    ) -> Result<ApiMethod, ApiGatewayError> {
        let state = self.load_state(scope);
        let api = rest_api(&state, api_id)?;
        let method = method(api, resource_id, http_method)?;

        Ok(method.to_method())
    }

    #[doc = "# Errors\n\nReturns `ApiGatewayError` when the request fails validation, referenced resources are missing, or the API Gateway control-plane state cannot be loaded or persisted."]
    pub fn update_method(
        &self,
        scope: &ApiGatewayScope,
        api_id: &str,
        resource_id: &str,
        http_method: &str,
        patch_operations: &[PatchOperation],
    ) -> Result<ApiMethod, ApiGatewayError> {
        let normalized = normalize_http_method(http_method)?;
        let mut state = self.load_state(scope);
        let api = rest_api_mut(&mut state, api_id)?;

        for operation in patch_operations {
            let value = operation.value.as_deref();
            match patch_path(operation, "method")? {
                ("replace", "/authorizationType") => {
                    let value = required_patch_value(operation)?;
                    validate_authorization_type(value)?;
                    method_mut(api, resource_id, &normalized)?
                        .authorization_type = value.to_owned();
                }
                ("replace", "/apiKeyRequired") => {
                    method_mut(api, resource_id, &normalized)?
                        .api_key_required = parse_patch_bool(operation)?;
                }
                ("replace", "/authorizerId") => {
                    let authorizer_id = required_patch_value(operation)?;
                    authorizer(api, authorizer_id)?;
                    method_mut(api, resource_id, &normalized)?.authorizer_id =
                        Some(authorizer_id.to_owned());
                }
                ("remove", "/authorizerId") => {
                    method_mut(api, resource_id, &normalized)?.authorizer_id =
                        None;
                }
                ("replace", "/requestValidatorId") => {
                    let validator_id = required_patch_value(operation)?;
                    request_validator(api, validator_id)?;
                    method_mut(api, resource_id, &normalized)?
                        .request_validator_id = Some(validator_id.to_owned());
                }
                ("remove", "/requestValidatorId") => {
                    method_mut(api, resource_id, &normalized)?
                        .request_validator_id = None;
                }
                ("replace", "/operationName") => {
                    method_mut(api, resource_id, &normalized)?
                        .operation_name = value.map(str::to_owned);
                }
                ("remove", "/operationName") => {
                    method_mut(api, resource_id, &normalized)?
                        .operation_name = None;
                }
                ("add" | "replace", path)
                    if path.starts_with("/requestParameters/") =>
                {
                    let name = path.trim_start_matches("/requestParameters/");
                    method_mut(api, resource_id, &normalized)?
                        .request_parameters
                        .insert(name.to_owned(), parse_patch_bool(operation)?);
                }
                ("remove", path)
                    if path.starts_with("/requestParameters/") =>
                {
                    let name = path.trim_start_matches("/requestParameters/");
                    method_mut(api, resource_id, &normalized)?
                        .request_parameters
                        .remove(name);
                }
                ("add" | "replace", path)
                    if path.starts_with("/requestModels/") =>
                {
                    let name = decode_patch_segment(
                        path.trim_start_matches("/requestModels/"),
                    )?;
                    let model = required_patch_value(operation)?;
                    method_mut(api, resource_id, &normalized)?
                        .request_models
                        .insert(name, model.to_owned());
                }
                ("remove", path) if path.starts_with("/requestModels/") => {
                    let name = decode_patch_segment(
                        path.trim_start_matches("/requestModels/"),
                    )?;
                    method_mut(api, resource_id, &normalized)?
                        .request_models
                        .remove(&name);
                }
                _ => {
                    return Err(ApiGatewayError::UnsupportedPatchPath {
                        resource: "method",
                        path: operation.path.clone(),
                    });
                }
            }
        }

        let response = method(api, resource_id, &normalized)?.to_method();
        self.save_state(scope, state)?;

        Ok(response)
    }

    #[doc = "# Errors\n\nReturns `ApiGatewayError` when the request fails validation, referenced resources are missing, or the API Gateway control-plane state cannot be loaded or persisted."]
    pub fn delete_method(
        &self,
        scope: &ApiGatewayScope,
        api_id: &str,
        resource_id: &str,
        http_method: &str,
    ) -> Result<(), ApiGatewayError> {
        let normalized = normalize_http_method(http_method)?;
        let mut state = self.load_state(scope);
        let api = rest_api_mut(&mut state, api_id)?;
        let removed =
            resource_mut(api, resource_id)?.methods.remove(&normalized);
        if removed.is_none() {
            return Err(ApiGatewayError::NotFound {
                resource: format!(
                    "Method {normalized} for resource {resource_id}"
                ),
            });
        }
        self.save_state(scope, state)
    }

    #[doc = "# Errors\n\nReturns `ApiGatewayError` when the request fails validation, referenced resources are missing, or the API Gateway control-plane state cannot be loaded or persisted."]
    pub fn put_method_response(
        &self,
        scope: &ApiGatewayScope,
        api_id: &str,
        resource_id: &str,
        http_method: &str,
        status_code: &str,
        input: PutMethodResponseInput,
    ) -> Result<MethodResponse, ApiGatewayError> {
        validate_status_code(status_code)?;
        let normalized = normalize_http_method(http_method)?;
        let mut state = self.load_state(scope);
        let api = rest_api_mut(&mut state, api_id)?;
        let method = method_mut(api, resource_id, &normalized)?;
        let response = MethodResponse {
            response_models: input.response_models,
            response_parameters: input.response_parameters,
            status_code: status_code.to_owned(),
        };
        method
            .method_responses
            .insert(status_code.to_owned(), response.clone());
        self.save_state(scope, state)?;

        Ok(response)
    }

    #[doc = "# Errors\n\nReturns `ApiGatewayError` when the request fails validation, referenced resources are missing, or the API Gateway control-plane state cannot be loaded or persisted."]
    pub fn get_method_response(
        &self,
        scope: &ApiGatewayScope,
        api_id: &str,
        resource_id: &str,
        http_method: &str,
        status_code: &str,
    ) -> Result<MethodResponse, ApiGatewayError> {
        let state = self.load_state(scope);
        let api = rest_api(&state, api_id)?;
        let method = method(api, resource_id, http_method)?;
        method.method_responses.get(status_code).cloned().ok_or_else(|| {
            ApiGatewayError::NotFound {
                resource: format!(
                    "Method response {status_code} for {http_method}"
                ),
            }
        })
    }

    #[doc = "# Errors\n\nReturns `ApiGatewayError` when the request fails validation, referenced resources are missing, or the API Gateway control-plane state cannot be loaded or persisted."]
    pub fn delete_method_response(
        &self,
        scope: &ApiGatewayScope,
        api_id: &str,
        resource_id: &str,
        http_method: &str,
        status_code: &str,
    ) -> Result<(), ApiGatewayError> {
        let normalized = normalize_http_method(http_method)?;
        let mut state = self.load_state(scope);
        let api = rest_api_mut(&mut state, api_id)?;
        let removed = method_mut(api, resource_id, &normalized)?
            .method_responses
            .remove(status_code);
        if removed.is_none() {
            return Err(ApiGatewayError::NotFound {
                resource: format!(
                    "Method response {status_code} for {normalized}"
                ),
            });
        }
        self.save_state(scope, state)
    }

    #[doc = "# Errors\n\nReturns `ApiGatewayError` when the request fails validation, referenced resources are missing, or the API Gateway control-plane state cannot be loaded or persisted."]
    pub fn put_integration(
        &self,
        scope: &ApiGatewayScope,
        api_id: &str,
        resource_id: &str,
        http_method: &str,
        input: PutIntegrationInput,
    ) -> Result<Integration, ApiGatewayError> {
        let normalized = normalize_http_method(http_method)?;
        validate_integration(&input)?;

        let mut state = self.load_state(scope);
        let api = rest_api_mut(&mut state, api_id)?;
        let method = method_mut(api, resource_id, &normalized)?;
        let integration = Integration {
            cache_key_parameters: input.cache_key_parameters,
            cache_namespace: input.cache_namespace,
            connection_id: input.connection_id,
            connection_type: input
                .connection_type
                .unwrap_or_else(|| "INTERNET".to_owned()),
            content_handling: input.content_handling,
            credentials: input.credentials,
            http_method: input.http_method,
            integration_http_method: input.integration_http_method,
            passthrough_behavior: input.passthrough_behavior,
            request_parameters: input.request_parameters,
            request_templates: input.request_templates,
            timeout_in_millis: input.timeout_in_millis,
            type_: input.type_,
            uri: input.uri,
        };
        method.integration = Some(integration.clone());
        self.save_state(scope, state)?;

        Ok(integration)
    }

    #[doc = "# Errors\n\nReturns `ApiGatewayError` when the request fails validation, referenced resources are missing, or the API Gateway control-plane state cannot be loaded or persisted."]
    pub fn get_integration(
        &self,
        scope: &ApiGatewayScope,
        api_id: &str,
        resource_id: &str,
        http_method: &str,
    ) -> Result<Integration, ApiGatewayError> {
        let normalized = normalize_http_method(http_method)?;
        let state = self.load_state(scope);
        let api = rest_api(&state, api_id)?;
        method(api, resource_id, http_method)?.integration.clone().ok_or_else(
            || ApiGatewayError::NotFound {
                resource: format!(
                    "Integration for method {normalized} on resource {resource_id}"
                ),
            },
        )
    }

    #[doc = "# Errors\n\nReturns `ApiGatewayError` when the request fails validation, referenced resources are missing, or the API Gateway control-plane state cannot be loaded or persisted."]
    pub fn delete_integration(
        &self,
        scope: &ApiGatewayScope,
        api_id: &str,
        resource_id: &str,
        http_method: &str,
    ) -> Result<(), ApiGatewayError> {
        let normalized = normalize_http_method(http_method)?;
        let mut state = self.load_state(scope);
        let api = rest_api_mut(&mut state, api_id)?;
        let method = method_mut(api, resource_id, &normalized)?;
        if method.integration.take().is_none() {
            return Err(ApiGatewayError::NotFound {
                resource: format!(
                    "Integration for method {normalized} on resource {resource_id}"
                ),
            });
        }
        self.save_state(scope, state)
    }

    #[doc = "# Errors\n\nReturns `ApiGatewayError` when the request fails validation, referenced resources are missing, or the API Gateway control-plane state cannot be loaded or persisted."]
    pub fn create_deployment(
        &self,
        scope: &ApiGatewayScope,
        api_id: &str,
        input: CreateDeploymentInput,
    ) -> Result<Deployment, ApiGatewayError> {
        let mut state = self.load_state(scope);
        let deployment_id = next_identifier(
            &mut state.next_deployment_sequence,
            DEPLOYMENT_ID_WIDTH,
        );
        let created_date = self.now_epoch_seconds();
        let stage_name = input.stage_name.clone();
        let stage_description = input.stage_description.clone();
        let stage_variables = input.variables.clone();
        let api = rest_api_mut(&mut state, api_id)?;
        let deployment = StoredDeployment {
            created_date,
            description: optional_empty_to_none(input.description),
            id: deployment_id.clone(),
            resources: api.resources.clone(),
        };
        let response = deployment.to_deployment();
        api.deployments.insert(deployment_id.clone(), deployment);

        if let Some(stage_name) = stage_name {
            validate_stage_name(&stage_name)?;
            let description = optional_empty_to_none(stage_description);
            let tags = api
                .stages
                .get(&stage_name)
                .map(|stage| stage.tags.clone())
                .unwrap_or_default();
            let created = api
                .stages
                .get(&stage_name)
                .map(|stage| stage.created_date)
                .unwrap_or(created_date);
            api.stages.insert(
                stage_name.clone(),
                StoredStage {
                    created_date: created,
                    deployment_id: Some(deployment_id),
                    description,
                    last_updated_date: created_date,
                    stage_name,
                    tags,
                    variables: stage_variables,
                },
            );
        }

        self.save_state(scope, state)?;

        Ok(response)
    }

    #[doc = "# Errors\n\nReturns `ApiGatewayError` when the request fails validation, referenced resources are missing, or the API Gateway control-plane state cannot be loaded or persisted."]
    pub fn get_deployment(
        &self,
        scope: &ApiGatewayScope,
        api_id: &str,
        deployment_id: &str,
    ) -> Result<Deployment, ApiGatewayError> {
        let state = self.load_state(scope);
        let api = rest_api(&state, api_id)?;
        let deployment = deployment(api, deployment_id)
            .map(StoredDeployment::to_deployment)?;

        Ok(deployment)
    }

    #[doc = "# Errors\n\nReturns `ApiGatewayError` when the request fails validation, referenced resources are missing, or the API Gateway control-plane state cannot be loaded or persisted."]
    pub fn get_deployments(
        &self,
        scope: &ApiGatewayScope,
        api_id: &str,
        position: Option<&str>,
        limit: Option<u32>,
    ) -> Result<ItemCollection<Deployment>, ApiGatewayError> {
        let state = self.load_state(scope);
        let api = rest_api(&state, api_id)?;
        let deployments = api
            .deployments
            .values()
            .map(StoredDeployment::to_deployment)
            .collect::<Vec<_>>();

        paginate(deployments, position, limit)
    }

    #[doc = "# Errors\n\nReturns `ApiGatewayError` when the request fails validation, referenced resources are missing, or the API Gateway control-plane state cannot be loaded or persisted."]
    pub fn delete_deployment(
        &self,
        scope: &ApiGatewayScope,
        api_id: &str,
        deployment_id: &str,
    ) -> Result<(), ApiGatewayError> {
        let mut state = self.load_state(scope);
        let api = rest_api_mut(&mut state, api_id)?;
        if api
            .stages
            .values()
            .any(|stage| stage.deployment_id.as_deref() == Some(deployment_id))
        {
            return Err(ApiGatewayError::Conflict {
                resource: format!(
                    "deployment {deployment_id} is still referenced by a stage"
                ),
            });
        }
        if api.deployments.remove(deployment_id).is_none() {
            return Err(ApiGatewayError::NotFound {
                resource: format!("Deployment {deployment_id}"),
            });
        }
        self.save_state(scope, state)
    }

    #[doc = "# Errors\n\nReturns `ApiGatewayError` when the request fails validation, referenced resources are missing, or the API Gateway control-plane state cannot be loaded or persisted."]
    pub fn create_stage(
        &self,
        scope: &ApiGatewayScope,
        api_id: &str,
        input: CreateStageInput,
    ) -> Result<Stage, ApiGatewayError> {
        validate_stage_name(&input.stage_name)?;
        validate_tags(&input.tags)?;
        if input.cache_cluster_enabled.is_some()
            || input.cache_cluster_size.is_some()
        {
            return Err(ApiGatewayError::UnsupportedOperation {
                message: "API Gateway cache configuration is not supported."
                    .to_owned(),
            });
        }

        let mut state = self.load_state(scope);
        let api = rest_api_mut(&mut state, api_id)?;
        if api.stages.contains_key(&input.stage_name) {
            return Err(ApiGatewayError::Conflict {
                resource: format!("Stage {}", input.stage_name),
            });
        }
        deployment(api, &input.deployment_id)?;
        let created_date = self.now_epoch_seconds();
        let stage = StoredStage {
            created_date,
            deployment_id: Some(input.deployment_id),
            description: optional_empty_to_none(input.description),
            last_updated_date: created_date,
            stage_name: input.stage_name.clone(),
            tags: input.tags,
            variables: input.variables,
        };
        let response = stage.to_stage();
        api.stages.insert(input.stage_name, stage);
        self.save_state(scope, state)?;

        Ok(response)
    }

    #[doc = "# Errors\n\nReturns `ApiGatewayError` when the request fails validation, referenced resources are missing, or the API Gateway control-plane state cannot be loaded or persisted."]
    pub fn get_stage(
        &self,
        scope: &ApiGatewayScope,
        api_id: &str,
        stage_name: &str,
    ) -> Result<Stage, ApiGatewayError> {
        let state = self.load_state(scope);
        let api = rest_api(&state, api_id)?;
        let stage = stage(api, stage_name)?;

        Ok(stage.to_stage())
    }

    #[doc = "# Errors\n\nReturns `ApiGatewayError` when the request fails validation, referenced resources are missing, or the API Gateway control-plane state cannot be loaded or persisted."]
    pub fn get_stages(
        &self,
        scope: &ApiGatewayScope,
        api_id: &str,
        deployment_id: Option<&str>,
        position: Option<&str>,
        limit: Option<u32>,
    ) -> Result<ItemCollection<Stage>, ApiGatewayError> {
        let state = self.load_state(scope);
        let api = rest_api(&state, api_id)?;
        let stages = api
            .stages
            .values()
            .filter(|stage| {
                deployment_id.is_none()
                    || stage.deployment_id.as_deref() == deployment_id
            })
            .map(StoredStage::to_stage)
            .collect::<Vec<_>>();

        paginate(stages, position, limit)
    }

    #[doc = "# Errors\n\nReturns `ApiGatewayError` when the request fails validation, referenced resources are missing, or the API Gateway control-plane state cannot be loaded or persisted."]
    pub fn update_stage(
        &self,
        scope: &ApiGatewayScope,
        api_id: &str,
        stage_name: &str,
        patch_operations: &[PatchOperation],
    ) -> Result<Stage, ApiGatewayError> {
        let mut state = self.load_state(scope);
        let api = rest_api_mut(&mut state, api_id)?;

        for operation in patch_operations {
            match patch_path(operation, "stage")? {
                ("replace", "/deploymentId") => {
                    let deployment_id = required_patch_value(operation)?;
                    deployment(api, deployment_id)?;
                    let stage = stage_mut(api, stage_name)?;
                    stage.deployment_id = Some(deployment_id.to_owned());
                    stage.last_updated_date = self.now_epoch_seconds();
                }
                ("replace", "/description") => {
                    let stage = stage_mut(api, stage_name)?;
                    stage.description =
                        operation.value.clone().and_then(empty_to_none);
                    stage.last_updated_date = self.now_epoch_seconds();
                }
                ("add" | "replace", path)
                    if path.starts_with("/variables/") =>
                {
                    let key = decode_patch_segment(
                        path.trim_start_matches("/variables/"),
                    )?;
                    let value = required_patch_value(operation)?;
                    let stage = stage_mut(api, stage_name)?;
                    stage.variables.insert(key, value.to_owned());
                    stage.last_updated_date = self.now_epoch_seconds();
                }
                ("remove", path) if path.starts_with("/variables/") => {
                    let key = decode_patch_segment(
                        path.trim_start_matches("/variables/"),
                    )?;
                    let stage = stage_mut(api, stage_name)?;
                    stage.variables.remove(&key);
                    stage.last_updated_date = self.now_epoch_seconds();
                }
                (_, path)
                    if path.starts_with("/cacheCluster")
                        || path.contains("/caching/") =>
                {
                    return Err(ApiGatewayError::UnsupportedOperation {
                        message:
                            "API Gateway cache configuration is not supported."
                                .to_owned(),
                    });
                }
                _ => {
                    return Err(ApiGatewayError::UnsupportedPatchPath {
                        resource: "stage",
                        path: operation.path.clone(),
                    });
                }
            }
        }

        let response = stage(api, stage_name)?.to_stage();
        self.save_state(scope, state)?;

        Ok(response)
    }

    #[doc = "# Errors\n\nReturns `ApiGatewayError` when the request fails validation, referenced resources are missing, or the API Gateway control-plane state cannot be loaded or persisted."]
    pub fn delete_stage(
        &self,
        scope: &ApiGatewayScope,
        api_id: &str,
        stage_name: &str,
    ) -> Result<(), ApiGatewayError> {
        let mut state = self.load_state(scope);
        let api = rest_api_mut(&mut state, api_id)?;
        if api.stages.remove(stage_name).is_none() {
            return Err(ApiGatewayError::NotFound {
                resource: format!("Stage {stage_name}"),
            });
        }
        self.save_state(scope, state)
    }

    #[doc = "# Errors\n\nReturns `ApiGatewayError` when the request fails validation, referenced resources are missing, or the API Gateway control-plane state cannot be loaded or persisted."]
    pub fn create_authorizer(
        &self,
        scope: &ApiGatewayScope,
        api_id: &str,
        input: CreateAuthorizerInput,
    ) -> Result<Authorizer, ApiGatewayError> {
        validate_name("name", &input.name)?;
        validate_authorizer_input(&input)?;

        let mut state = self.load_state(scope);
        let authorizer_id = next_identifier(
            &mut state.next_authorizer_sequence,
            AUTHORIZER_ID_WIDTH,
        );
        let api = rest_api_mut(&mut state, api_id)?;
        let authorizer = StoredAuthorizer {
            authorizer_credentials: input.authorizer_credentials,
            authorizer_result_ttl_in_seconds: input
                .authorizer_result_ttl_in_seconds,
            authorizer_uri: input.authorizer_uri,
            id: authorizer_id.clone(),
            identity_source: input.identity_source,
            identity_validation_expression: input
                .identity_validation_expression,
            name: input.name,
            provider_arns: input.provider_arns,
            type_: input.type_,
        };
        let response = authorizer.to_authorizer();
        api.authorizers.insert(authorizer_id, authorizer);
        self.save_state(scope, state)?;

        Ok(response)
    }

    #[doc = "# Errors\n\nReturns `ApiGatewayError` when the request fails validation, referenced resources are missing, or the API Gateway control-plane state cannot be loaded or persisted."]
    pub fn get_authorizer(
        &self,
        scope: &ApiGatewayScope,
        api_id: &str,
        authorizer_id: &str,
    ) -> Result<Authorizer, ApiGatewayError> {
        let state = self.load_state(scope);
        let api = rest_api(&state, api_id)?;
        let authorizer = authorizer(api, authorizer_id)?;

        Ok(authorizer.to_authorizer())
    }

    #[doc = "# Errors\n\nReturns `ApiGatewayError` when the request fails validation, referenced resources are missing, or the API Gateway control-plane state cannot be loaded or persisted."]
    pub fn get_authorizers(
        &self,
        scope: &ApiGatewayScope,
        api_id: &str,
        position: Option<&str>,
        limit: Option<u32>,
    ) -> Result<ItemCollection<Authorizer>, ApiGatewayError> {
        let state = self.load_state(scope);
        let api = rest_api(&state, api_id)?;
        let items = api
            .authorizers
            .values()
            .map(StoredAuthorizer::to_authorizer)
            .collect::<Vec<_>>();

        paginate(items, position, limit)
    }

    #[doc = "# Errors\n\nReturns `ApiGatewayError` when the request fails validation, referenced resources are missing, or the API Gateway control-plane state cannot be loaded or persisted."]
    pub fn update_authorizer(
        &self,
        scope: &ApiGatewayScope,
        api_id: &str,
        authorizer_id: &str,
        patch_operations: &[PatchOperation],
    ) -> Result<Authorizer, ApiGatewayError> {
        let mut state = self.load_state(scope);
        let api = rest_api_mut(&mut state, api_id)?;
        for operation in patch_operations {
            match patch_path(operation, "authorizer")? {
                ("replace", "/name") => {
                    let value = required_patch_value(operation)?;
                    validate_name("name", value)?;
                    authorizer_mut(api, authorizer_id)?.name =
                        value.to_owned();
                }
                ("replace", "/authorizerUri") => {
                    let value = required_patch_value(operation)?;
                    validate_authorizer_uri(value)?;
                    authorizer_mut(api, authorizer_id)?.authorizer_uri =
                        Some(value.to_owned());
                }
                ("replace", "/identitySource") => {
                    let value = required_patch_value(operation)?;
                    authorizer_mut(api, authorizer_id)?.identity_source =
                        Some(value.to_owned());
                }
                ("replace", "/authorizerResultTtlInSeconds") => {
                    authorizer_mut(api, authorizer_id)?
                        .authorizer_result_ttl_in_seconds =
                        Some(parse_patch_u32(operation)?);
                }
                ("replace", "/type") => {
                    let value = required_patch_value(operation)?;
                    validate_authorizer_type(value)?;
                    authorizer_mut(api, authorizer_id)?.type_ =
                        value.to_owned();
                }
                _ => {
                    return Err(ApiGatewayError::UnsupportedPatchPath {
                        resource: "authorizer",
                        path: operation.path.clone(),
                    });
                }
            }
        }

        let response = authorizer(api, authorizer_id)?.to_authorizer();
        self.save_state(scope, state)?;

        Ok(response)
    }

    #[doc = "# Errors\n\nReturns `ApiGatewayError` when the request fails validation, referenced resources are missing, or the API Gateway control-plane state cannot be loaded or persisted."]
    pub fn delete_authorizer(
        &self,
        scope: &ApiGatewayScope,
        api_id: &str,
        authorizer_id: &str,
    ) -> Result<(), ApiGatewayError> {
        let mut state = self.load_state(scope);
        let api = rest_api_mut(&mut state, api_id)?;
        if api.authorizers.remove(authorizer_id).is_none() {
            return Err(ApiGatewayError::NotFound {
                resource: format!("Authorizer {authorizer_id}"),
            });
        }
        for resource in api.resources.values_mut() {
            for method in resource.methods.values_mut() {
                if method.authorizer_id.as_deref() == Some(authorizer_id) {
                    method.authorizer_id = None;
                }
            }
        }
        self.save_state(scope, state)
    }

    #[doc = "# Errors\n\nReturns `ApiGatewayError` when the request fails validation, referenced resources are missing, or the API Gateway control-plane state cannot be loaded or persisted."]
    pub fn create_request_validator(
        &self,
        scope: &ApiGatewayScope,
        api_id: &str,
        input: CreateRequestValidatorInput,
    ) -> Result<RequestValidator, ApiGatewayError> {
        validate_name("name", &input.name)?;
        let mut state = self.load_state(scope);
        let request_validator_id = next_identifier(
            &mut state.next_request_validator_sequence,
            REQUEST_VALIDATOR_ID_WIDTH,
        );
        let api = rest_api_mut(&mut state, api_id)?;
        let validator = StoredRequestValidator {
            id: request_validator_id.clone(),
            name: input.name,
            validate_request_body: input
                .validate_request_body
                .unwrap_or(false),
            validate_request_parameters: input
                .validate_request_parameters
                .unwrap_or(false),
        };
        let response = validator.to_request_validator();
        api.request_validators.insert(request_validator_id, validator);
        self.save_state(scope, state)?;

        Ok(response)
    }

    #[doc = "# Errors\n\nReturns `ApiGatewayError` when the request fails validation, referenced resources are missing, or the API Gateway control-plane state cannot be loaded or persisted."]
    pub fn get_request_validator(
        &self,
        scope: &ApiGatewayScope,
        api_id: &str,
        request_validator_id: &str,
    ) -> Result<RequestValidator, ApiGatewayError> {
        let state = self.load_state(scope);
        let api = rest_api(&state, api_id)?;
        let validator = request_validator(api, request_validator_id)?;

        Ok(validator.to_request_validator())
    }

    #[doc = "# Errors\n\nReturns `ApiGatewayError` when the request fails validation, referenced resources are missing, or the API Gateway control-plane state cannot be loaded or persisted."]
    pub fn get_request_validators(
        &self,
        scope: &ApiGatewayScope,
        api_id: &str,
        position: Option<&str>,
        limit: Option<u32>,
    ) -> Result<ItemCollection<RequestValidator>, ApiGatewayError> {
        let state = self.load_state(scope);
        let api = rest_api(&state, api_id)?;
        let items = api
            .request_validators
            .values()
            .map(StoredRequestValidator::to_request_validator)
            .collect::<Vec<_>>();

        paginate(items, position, limit)
    }

    #[doc = "# Errors\n\nReturns `ApiGatewayError` when the request fails validation, referenced resources are missing, or the API Gateway control-plane state cannot be loaded or persisted."]
    pub fn update_request_validator(
        &self,
        scope: &ApiGatewayScope,
        api_id: &str,
        request_validator_id: &str,
        patch_operations: &[PatchOperation],
    ) -> Result<RequestValidator, ApiGatewayError> {
        let mut state = self.load_state(scope);
        let api = rest_api_mut(&mut state, api_id)?;
        for operation in patch_operations {
            match patch_path(operation, "request validator")? {
                ("replace", "/name") => {
                    let value = required_patch_value(operation)?;
                    validate_name("name", value)?;
                    request_validator_mut(api, request_validator_id)?.name =
                        value.to_owned();
                }
                ("replace", "/validateRequestBody") => {
                    request_validator_mut(api, request_validator_id)?
                        .validate_request_body =
                        patch_bool_with_default(operation, false)?;
                }
                ("replace", "/validateRequestParameters") => {
                    request_validator_mut(api, request_validator_id)?
                        .validate_request_parameters =
                        patch_bool_with_default(operation, false)?;
                }
                _ => {
                    return Err(ApiGatewayError::UnsupportedPatchPath {
                        resource: "request validator",
                        path: operation.path.clone(),
                    });
                }
            }
        }

        let response = request_validator(api, request_validator_id)?
            .to_request_validator();
        self.save_state(scope, state)?;

        Ok(response)
    }

    #[doc = "# Errors\n\nReturns `ApiGatewayError` when the request fails validation, referenced resources are missing, or the API Gateway control-plane state cannot be loaded or persisted."]
    pub fn delete_request_validator(
        &self,
        scope: &ApiGatewayScope,
        api_id: &str,
        request_validator_id: &str,
    ) -> Result<(), ApiGatewayError> {
        let mut state = self.load_state(scope);
        let api = rest_api_mut(&mut state, api_id)?;
        if api.request_validators.remove(request_validator_id).is_none() {
            return Err(ApiGatewayError::NotFound {
                resource: format!("Request validator {request_validator_id}"),
            });
        }
        for resource in api.resources.values_mut() {
            for method in resource.methods.values_mut() {
                if method.request_validator_id.as_deref()
                    == Some(request_validator_id)
                {
                    method.request_validator_id = None;
                }
            }
        }
        self.save_state(scope, state)
    }

    #[doc = "# Errors\n\nReturns `ApiGatewayError` when the request fails validation, referenced resources are missing, or the API Gateway control-plane state cannot be loaded or persisted."]
    pub fn create_api_key(
        &self,
        scope: &ApiGatewayScope,
        input: CreateApiKeyInput,
    ) -> Result<ApiKey, ApiGatewayError> {
        validate_tags(&input.tags)?;
        let mut state = self.load_state(scope);
        let id = next_identifier(
            &mut state.next_api_key_sequence,
            API_KEY_ID_WIDTH,
        );
        let timestamp = self.now_epoch_seconds();
        let api_key = StoredApiKey {
            created_date: timestamp,
            customer_id: input.customer_id,
            description: optional_empty_to_none(input.description),
            enabled: input.enabled.unwrap_or(true),
            id: id.clone(),
            last_updated_date: timestamp,
            name: input.name.unwrap_or_else(|| format!("key-{id}")),
            stage_keys: input.stage_keys,
            tags: input.tags,
            value: input
                .value
                .unwrap_or_else(|| format!("cloudish-api-key-{id}")),
        };
        let response = api_key.to_api_key(true);
        state.api_keys.insert(id, api_key);
        self.save_state(scope, state)?;

        Ok(response)
    }

    #[doc = "# Errors\n\nReturns `ApiGatewayError` when the request fails validation, referenced resources are missing, or the API Gateway control-plane state cannot be loaded or persisted."]
    pub fn get_api_key(
        &self,
        scope: &ApiGatewayScope,
        api_key_id: &str,
        include_value: bool,
    ) -> Result<ApiKey, ApiGatewayError> {
        let state = self.load_state(scope);
        let key = api_key(&state, api_key_id)?;

        Ok(key.to_api_key(include_value))
    }

    #[doc = "# Errors\n\nReturns `ApiGatewayError` when the request fails validation, referenced resources are missing, or the API Gateway control-plane state cannot be loaded or persisted."]
    pub fn get_api_keys(
        &self,
        scope: &ApiGatewayScope,
        include_values: bool,
        position: Option<&str>,
        limit: Option<u32>,
    ) -> Result<ItemCollection<ApiKey>, ApiGatewayError> {
        let state = self.load_state(scope);
        let keys = state
            .api_keys
            .values()
            .map(|key| key.to_api_key(include_values))
            .collect::<Vec<_>>();

        paginate(keys, position, limit)
    }

    #[doc = "# Errors\n\nReturns `ApiGatewayError` when the request fails validation, referenced resources are missing, or the API Gateway control-plane state cannot be loaded or persisted."]
    pub fn update_api_key(
        &self,
        scope: &ApiGatewayScope,
        api_key_id: &str,
        patch_operations: &[PatchOperation],
    ) -> Result<ApiKey, ApiGatewayError> {
        let mut state = self.load_state(scope);
        for operation in patch_operations {
            match patch_path(operation, "api key")? {
                ("replace", "/name") => {
                    let value = required_patch_value(operation)?;
                    validate_name("name", value)?;
                    api_key_mut(&mut state, api_key_id)?.name =
                        value.to_owned();
                }
                ("replace", "/description") => {
                    api_key_mut(&mut state, api_key_id)?.description =
                        operation.value.clone().and_then(empty_to_none);
                }
                ("replace", "/enabled") => {
                    api_key_mut(&mut state, api_key_id)?.enabled =
                        parse_patch_bool(operation)?;
                }
                ("replace", "/value") => {
                    let value = required_patch_value(operation)?;
                    api_key_mut(&mut state, api_key_id)?.value =
                        value.to_owned();
                }
                ("replace", "/customerId") => {
                    api_key_mut(&mut state, api_key_id)?.customer_id =
                        operation.value.clone().and_then(empty_to_none);
                }
                _ => {
                    return Err(ApiGatewayError::UnsupportedPatchPath {
                        resource: "api key",
                        path: operation.path.clone(),
                    });
                }
            }
        }

        api_key_mut(&mut state, api_key_id)?.last_updated_date =
            self.now_epoch_seconds();
        let response = api_key(&state, api_key_id)?.to_api_key(true);
        self.save_state(scope, state)?;

        Ok(response)
    }

    #[doc = "# Errors\n\nReturns `ApiGatewayError` when the request fails validation, referenced resources are missing, or the API Gateway control-plane state cannot be loaded or persisted."]
    pub fn delete_api_key(
        &self,
        scope: &ApiGatewayScope,
        api_key_id: &str,
    ) -> Result<(), ApiGatewayError> {
        let mut state = self.load_state(scope);
        if state.api_keys.remove(api_key_id).is_none() {
            return Err(ApiGatewayError::NotFound {
                resource: format!("API key {api_key_id}"),
            });
        }
        for usage_plan in state.usage_plans.values_mut() {
            usage_plan.usage_plan_keys.remove(api_key_id);
        }
        self.save_state(scope, state)
    }

    #[doc = "# Errors\n\nReturns `ApiGatewayError` when the request fails validation, referenced resources are missing, or the API Gateway control-plane state cannot be loaded or persisted."]
    pub fn create_usage_plan(
        &self,
        scope: &ApiGatewayScope,
        input: CreateUsagePlanInput,
    ) -> Result<UsagePlan, ApiGatewayError> {
        validate_name("name", &input.name)?;
        validate_tags(&input.tags)?;

        let mut state = self.load_state(scope);
        validate_usage_plan_api_stages(&state, &input.api_stages)?;
        let id = next_identifier(
            &mut state.next_usage_plan_sequence,
            USAGE_PLAN_ID_WIDTH,
        );
        let plan = StoredUsagePlan {
            api_stages: input.api_stages,
            description: optional_empty_to_none(input.description),
            id: id.clone(),
            name: input.name,
            product_code: optional_empty_to_none(input.product_code),
            quota: input.quota,
            tags: input.tags,
            throttle: input.throttle,
            usage_plan_keys: BTreeMap::new(),
        };
        let response = plan.to_usage_plan();
        state.usage_plans.insert(id, plan);
        self.save_state(scope, state)?;

        Ok(response)
    }

    #[doc = "# Errors\n\nReturns `ApiGatewayError` when the request fails validation, referenced resources are missing, or the API Gateway control-plane state cannot be loaded or persisted."]
    pub fn get_usage_plan(
        &self,
        scope: &ApiGatewayScope,
        usage_plan_id: &str,
    ) -> Result<UsagePlan, ApiGatewayError> {
        let state = self.load_state(scope);
        let plan = usage_plan(&state, usage_plan_id)?;

        Ok(plan.to_usage_plan())
    }

    #[doc = "# Errors\n\nReturns `ApiGatewayError` when the request fails validation, referenced resources are missing, or the API Gateway control-plane state cannot be loaded or persisted."]
    pub fn get_usage_plans(
        &self,
        scope: &ApiGatewayScope,
        position: Option<&str>,
        limit: Option<u32>,
    ) -> Result<ItemCollection<UsagePlan>, ApiGatewayError> {
        let state = self.load_state(scope);
        let plans = state
            .usage_plans
            .values()
            .map(StoredUsagePlan::to_usage_plan)
            .collect::<Vec<_>>();

        paginate(plans, position, limit)
    }

    #[doc = "# Errors\n\nReturns `ApiGatewayError` when the request fails validation, referenced resources are missing, or the API Gateway control-plane state cannot be loaded or persisted."]
    pub fn update_usage_plan(
        &self,
        scope: &ApiGatewayScope,
        usage_plan_id: &str,
        patch_operations: &[PatchOperation],
    ) -> Result<UsagePlan, ApiGatewayError> {
        let mut state = self.load_state(scope);
        for operation in patch_operations {
            match patch_path(operation, "usage plan")? {
                ("replace", "/name") => {
                    let value = required_patch_value(operation)?;
                    validate_name("name", value)?;
                    usage_plan_mut(&mut state, usage_plan_id)?.name =
                        value.to_owned();
                }
                ("replace", "/description") => {
                    usage_plan_mut(&mut state, usage_plan_id)?.description =
                        operation.value.clone().and_then(empty_to_none);
                }
                _ => {
                    return Err(ApiGatewayError::UnsupportedPatchPath {
                        resource: "usage plan",
                        path: operation.path.clone(),
                    });
                }
            }
        }

        let response = usage_plan(&state, usage_plan_id)?.to_usage_plan();
        self.save_state(scope, state)?;

        Ok(response)
    }

    #[doc = "# Errors\n\nReturns `ApiGatewayError` when the request fails validation, referenced resources are missing, or the API Gateway control-plane state cannot be loaded or persisted."]
    pub fn delete_usage_plan(
        &self,
        scope: &ApiGatewayScope,
        usage_plan_id: &str,
    ) -> Result<(), ApiGatewayError> {
        let mut state = self.load_state(scope);
        if state.usage_plans.remove(usage_plan_id).is_none() {
            return Err(ApiGatewayError::NotFound {
                resource: format!("Usage plan {usage_plan_id}"),
            });
        }
        self.save_state(scope, state)
    }

    #[doc = "# Errors\n\nReturns `ApiGatewayError` when the request fails validation, referenced resources are missing, or the API Gateway control-plane state cannot be loaded or persisted."]
    pub fn create_usage_plan_key(
        &self,
        scope: &ApiGatewayScope,
        usage_plan_id: &str,
        input: CreateUsagePlanKeyInput,
    ) -> Result<UsagePlanKey, ApiGatewayError> {
        if input.key_type != "API_KEY" {
            return Err(ApiGatewayError::UnsupportedOperation {
                message: "Only API_KEY usage plan keys are supported."
                    .to_owned(),
            });
        }

        let mut state = self.load_state(scope);
        let api_key = api_key(&state, &input.key_id)?.clone();
        let usage_plan = usage_plan_mut(&mut state, usage_plan_id)?;
        if usage_plan.usage_plan_keys.contains_key(&input.key_id) {
            return Err(ApiGatewayError::Conflict {
                resource: format!(
                    "Usage plan key {} on usage plan {}",
                    input.key_id, usage_plan_id
                ),
            });
        }
        let key = StoredUsagePlanKey {
            id: api_key.id,
            name: api_key.name,
            type_: input.key_type,
            value: api_key.value,
        };
        let response = key.to_usage_plan_key();
        usage_plan.usage_plan_keys.insert(key.id.clone(), key);
        self.save_state(scope, state)?;

        Ok(response)
    }

    #[doc = "# Errors\n\nReturns `ApiGatewayError` when the request fails validation, referenced resources are missing, or the API Gateway control-plane state cannot be loaded or persisted."]
    pub fn get_usage_plan_key(
        &self,
        scope: &ApiGatewayScope,
        usage_plan_id: &str,
        key_id: &str,
    ) -> Result<UsagePlanKey, ApiGatewayError> {
        let state = self.load_state(scope);
        let plan = usage_plan(&state, usage_plan_id)?;
        let key = usage_plan_key(plan, key_id)?;

        Ok(key.to_usage_plan_key())
    }

    #[doc = "# Errors\n\nReturns `ApiGatewayError` when the request fails validation, referenced resources are missing, or the API Gateway control-plane state cannot be loaded or persisted."]
    pub fn get_usage_plan_keys(
        &self,
        scope: &ApiGatewayScope,
        usage_plan_id: &str,
        position: Option<&str>,
        limit: Option<u32>,
    ) -> Result<UsagePlanKeyCollection, ApiGatewayError> {
        let state = self.load_state(scope);
        let plan = usage_plan(&state, usage_plan_id)?;
        let keys = plan
            .usage_plan_keys
            .values()
            .map(StoredUsagePlanKey::to_usage_plan_key)
            .collect::<Vec<_>>();
        let page = paginate(keys, position, limit)?;

        Ok(UsagePlanKeyCollection { item: page.item, position: page.position })
    }

    #[doc = "# Errors\n\nReturns `ApiGatewayError` when the request fails validation, referenced resources are missing, or the API Gateway control-plane state cannot be loaded or persisted."]
    pub fn delete_usage_plan_key(
        &self,
        scope: &ApiGatewayScope,
        usage_plan_id: &str,
        key_id: &str,
    ) -> Result<(), ApiGatewayError> {
        let mut state = self.load_state(scope);
        let plan = usage_plan_mut(&mut state, usage_plan_id)?;
        if plan.usage_plan_keys.remove(key_id).is_none() {
            return Err(ApiGatewayError::NotFound {
                resource: format!(
                    "Usage plan key {key_id} on usage plan {usage_plan_id}"
                ),
            });
        }
        self.save_state(scope, state)
    }

    #[doc = "# Errors\n\nReturns `ApiGatewayError` when the request fails validation, referenced resources are missing, or the API Gateway control-plane state cannot be loaded or persisted."]
    pub fn get_tags(
        &self,
        scope: &ApiGatewayScope,
        resource_arn: &str,
    ) -> Result<GetTagsOutput, ApiGatewayError> {
        let state = self.load_state(scope);
        let tags = match parse_tag_target(scope.region(), resource_arn)? {
            TagTarget::RestApi(api_id) => {
                rest_api(&state, api_id)?.tags.clone()
            }
            TagTarget::Stage { api_id, stage_name } => {
                stage(rest_api(&state, api_id)?, stage_name)?.tags.clone()
            }
            TagTarget::ApiKey(api_key_id) => {
                api_key(&state, api_key_id)?.tags.clone()
            }
            TagTarget::UsagePlan(usage_plan_id) => {
                usage_plan(&state, usage_plan_id)?.tags.clone()
            }
        };

        Ok(GetTagsOutput { tags })
    }

    #[doc = "# Errors\n\nReturns `ApiGatewayError` when the request fails validation, referenced resources are missing, or the API Gateway control-plane state cannot be loaded or persisted."]
    pub fn tag_resource(
        &self,
        scope: &ApiGatewayScope,
        resource_arn: &str,
        tags: BTreeMap<String, String>,
    ) -> Result<(), ApiGatewayError> {
        validate_tags(&tags)?;

        let mut state = self.load_state(scope);
        match parse_tag_target(scope.region(), resource_arn)? {
            TagTarget::RestApi(api_id) => {
                rest_api_mut(&mut state, api_id)?.tags.extend(tags);
            }
            TagTarget::Stage { api_id, stage_name } => {
                stage_mut(rest_api_mut(&mut state, api_id)?, stage_name)?
                    .tags
                    .extend(tags);
            }
            TagTarget::ApiKey(api_key_id) => {
                api_key_mut(&mut state, api_key_id)?.tags.extend(tags);
            }
            TagTarget::UsagePlan(usage_plan_id) => {
                usage_plan_mut(&mut state, usage_plan_id)?.tags.extend(tags);
            }
        }
        self.save_state(scope, state)
    }

    #[doc = "# Errors\n\nReturns `ApiGatewayError` when the request fails validation, referenced resources are missing, or the API Gateway control-plane state cannot be loaded or persisted."]
    pub fn untag_resource(
        &self,
        scope: &ApiGatewayScope,
        resource_arn: &str,
        tag_keys: &[String],
    ) -> Result<(), ApiGatewayError> {
        let mut state = self.load_state(scope);
        match parse_tag_target(scope.region(), resource_arn)? {
            TagTarget::RestApi(api_id) => {
                remove_tag_keys(
                    &mut rest_api_mut(&mut state, api_id)?.tags,
                    tag_keys,
                );
            }
            TagTarget::Stage { api_id, stage_name } => {
                remove_tag_keys(
                    &mut stage_mut(
                        rest_api_mut(&mut state, api_id)?,
                        stage_name,
                    )?
                    .tags,
                    tag_keys,
                );
            }
            TagTarget::ApiKey(api_key_id) => {
                remove_tag_keys(
                    &mut api_key_mut(&mut state, api_key_id)?.tags,
                    tag_keys,
                );
            }
            TagTarget::UsagePlan(usage_plan_id) => {
                remove_tag_keys(
                    &mut usage_plan_mut(&mut state, usage_plan_id)?.tags,
                    tag_keys,
                );
            }
        }
        self.save_state(scope, state)
    }

    pub(crate) fn load_state(
        &self,
        scope: &ApiGatewayScope,
    ) -> StoredApiGatewayState {
        self.state_store.get(&scope_key(scope)).unwrap_or_default()
    }

    pub(crate) fn all_states(
        &self,
    ) -> Vec<(ApiGatewayScope, StoredApiGatewayState)> {
        let mut keys = self.state_store.keys();
        keys.sort();
        keys.into_iter()
            .filter_map(|key| {
                self.state_store
                    .get(&key)
                    .map(|state| (scope_from_key(&key), state))
            })
            .collect()
    }

    pub(crate) fn save_state(
        &self,
        scope: &ApiGatewayScope,
        state: StoredApiGatewayState,
    ) -> Result<(), ApiGatewayError> {
        self.state_store
            .put(scope_key(scope), state)
            .map_err(ApiGatewayError::Store)
    }

    pub(crate) fn now_epoch_seconds(&self) -> u64 {
        self.clock
            .now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
    }

    pub(crate) fn lock_state(&self) -> MutexGuard<'_, ()> {
        self.mutation_lock.lock().unwrap_or_else(|poison| poison.into_inner())
    }

    pub(crate) fn allocate_execute_api_id(
        &self,
    ) -> Result<String, ApiGatewayError> {
        let mut global = self
            .global_store
            .get(&GLOBAL_ROUTING_STATE_KEY.to_owned())
            .unwrap_or_default();
        loop {
            let api_id = next_identifier(
                &mut global.next_execute_api_sequence,
                EXECUTE_API_ID_WIDTH,
            );
            if !self.execute_api_id_exists_globally(&api_id) {
                self.global_store
                    .put(GLOBAL_ROUTING_STATE_KEY.to_owned(), global)
                    .map_err(ApiGatewayError::Store)?;
                return Ok(api_id);
            }
        }
    }

    fn execute_api_id_exists_globally(&self, api_id: &str) -> bool {
        self.all_states().into_iter().any(|(_, state)| {
            state.rest_apis.contains_key(api_id)
                || state.http_apis.contains_key(api_id)
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub(crate) struct StoredApiGatewayGlobalState {
    pub(crate) next_execute_api_sequence: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub(crate) struct StoredApiGatewayState {
    pub(crate) next_rest_api_sequence: u64,
    pub(crate) next_http_api_sequence: u64,
    pub(crate) next_resource_sequence: u64,
    pub(crate) next_deployment_sequence: u64,
    pub(crate) next_http_deployment_sequence: u64,
    pub(crate) next_authorizer_sequence: u64,
    pub(crate) next_http_authorizer_sequence: u64,
    pub(crate) next_request_validator_sequence: u64,
    pub(crate) next_api_key_sequence: u64,
    pub(crate) next_usage_plan_sequence: u64,
    pub(crate) next_http_route_sequence: u64,
    pub(crate) next_http_integration_sequence: u64,
    pub(crate) rest_apis: BTreeMap<String, StoredRestApi>,
    pub(crate) http_apis: BTreeMap<String, v2::StoredHttpApi>,
    pub(crate) api_keys: BTreeMap<String, StoredApiKey>,
    pub(crate) usage_plans: BTreeMap<String, StoredUsagePlan>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct StoredRestApi {
    pub(crate) api_key_source: String,
    pub(crate) binary_media_types: Vec<String>,
    pub(crate) created_date: u64,
    pub(crate) description: Option<String>,
    pub(crate) disable_execute_api_endpoint: bool,
    pub(crate) endpoint_configuration: EndpointConfiguration,
    pub(crate) id: String,
    pub(crate) name: String,
    pub(crate) resources: BTreeMap<String, StoredApiResource>,
    pub(crate) root_resource_id: String,
    pub(crate) deployments: BTreeMap<String, StoredDeployment>,
    pub(crate) stages: BTreeMap<String, StoredStage>,
    pub(crate) authorizers: BTreeMap<String, StoredAuthorizer>,
    pub(crate) request_validators: BTreeMap<String, StoredRequestValidator>,
    pub(crate) tags: BTreeMap<String, String>,
}

impl StoredRestApi {
    fn to_rest_api(&self) -> RestApi {
        RestApi {
            api_key_source: self.api_key_source.clone(),
            binary_media_types: self.binary_media_types.clone(),
            created_date: self.created_date,
            description: self.description.clone(),
            disable_execute_api_endpoint: self.disable_execute_api_endpoint,
            endpoint_configuration: self.endpoint_configuration.clone(),
            id: self.id.clone(),
            name: self.name.clone(),
            root_resource_id: self.root_resource_id.clone(),
            tags: self.tags.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct StoredApiResource {
    pub(crate) id: String,
    pub(crate) methods: BTreeMap<String, StoredApiMethod>,
    pub(crate) parent_id: Option<String>,
    pub(crate) path: String,
    pub(crate) path_part: Option<String>,
}

impl StoredApiResource {
    fn to_resource(&self) -> ApiResource {
        ApiResource {
            id: self.id.clone(),
            parent_id: self.parent_id.clone(),
            path: self.path.clone(),
            path_part: self.path_part.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct StoredApiMethod {
    pub(crate) api_key_required: bool,
    pub(crate) authorizer_id: Option<String>,
    pub(crate) authorization_type: String,
    pub(crate) http_method: String,
    pub(crate) integration: Option<Integration>,
    pub(crate) method_responses: BTreeMap<String, MethodResponse>,
    pub(crate) operation_name: Option<String>,
    pub(crate) request_models: BTreeMap<String, String>,
    pub(crate) request_parameters: BTreeMap<String, bool>,
    pub(crate) request_validator_id: Option<String>,
}

impl StoredApiMethod {
    fn to_method(&self) -> ApiMethod {
        ApiMethod {
            api_key_required: self.api_key_required,
            authorizer_id: self.authorizer_id.clone(),
            authorization_type: self.authorization_type.clone(),
            http_method: self.http_method.clone(),
            method_integration: self.integration.clone(),
            method_responses: self.method_responses.clone(),
            operation_name: self.operation_name.clone(),
            request_models: self.request_models.clone(),
            request_parameters: self.request_parameters.clone(),
            request_validator_id: self.request_validator_id.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct StoredDeployment {
    pub(crate) created_date: u64,
    pub(crate) description: Option<String>,
    pub(crate) id: String,
    pub(crate) resources: BTreeMap<String, StoredApiResource>,
}

impl StoredDeployment {
    fn to_deployment(&self) -> Deployment {
        Deployment {
            created_date: self.created_date,
            description: self.description.clone(),
            id: self.id.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct StoredStage {
    pub(crate) created_date: u64,
    pub(crate) deployment_id: Option<String>,
    pub(crate) description: Option<String>,
    pub(crate) last_updated_date: u64,
    pub(crate) stage_name: String,
    pub(crate) tags: BTreeMap<String, String>,
    pub(crate) variables: BTreeMap<String, String>,
}

impl StoredStage {
    fn to_stage(&self) -> Stage {
        Stage {
            created_date: self.created_date,
            deployment_id: self.deployment_id.clone(),
            description: self.description.clone(),
            last_updated_date: self.last_updated_date,
            stage_name: self.stage_name.clone(),
            tags: self.tags.clone(),
            variables: self.variables.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct StoredAuthorizer {
    authorizer_credentials: Option<String>,
    authorizer_result_ttl_in_seconds: Option<u32>,
    authorizer_uri: Option<String>,
    id: String,
    identity_source: Option<String>,
    identity_validation_expression: Option<String>,
    name: String,
    provider_arns: Vec<String>,
    type_: String,
}

impl StoredAuthorizer {
    fn to_authorizer(&self) -> Authorizer {
        Authorizer {
            authorizer_credentials: self.authorizer_credentials.clone(),
            authorizer_result_ttl_in_seconds: self
                .authorizer_result_ttl_in_seconds,
            authorizer_uri: self.authorizer_uri.clone(),
            id: self.id.clone(),
            identity_source: self.identity_source.clone(),
            identity_validation_expression: self
                .identity_validation_expression
                .clone(),
            name: self.name.clone(),
            provider_arns: self.provider_arns.clone(),
            type_: self.type_.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct StoredRequestValidator {
    pub(crate) id: String,
    pub(crate) name: String,
    pub(crate) validate_request_body: bool,
    pub(crate) validate_request_parameters: bool,
}

impl StoredRequestValidator {
    fn to_request_validator(&self) -> RequestValidator {
        RequestValidator {
            id: self.id.clone(),
            name: self.name.clone(),
            validate_request_body: self.validate_request_body,
            validate_request_parameters: self.validate_request_parameters,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct StoredApiKey {
    created_date: u64,
    customer_id: Option<String>,
    description: Option<String>,
    enabled: bool,
    id: String,
    last_updated_date: u64,
    name: String,
    stage_keys: Vec<String>,
    tags: BTreeMap<String, String>,
    value: String,
}

impl StoredApiKey {
    fn to_api_key(&self, include_value: bool) -> ApiKey {
        ApiKey {
            created_date: self.created_date,
            customer_id: self.customer_id.clone(),
            description: self.description.clone(),
            enabled: self.enabled,
            id: self.id.clone(),
            last_updated_date: self.last_updated_date,
            name: self.name.clone(),
            stage_keys: self.stage_keys.clone(),
            tags: self.tags.clone(),
            value: include_value.then(|| self.value.clone()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct StoredUsagePlan {
    api_stages: Vec<UsagePlanApiStage>,
    description: Option<String>,
    id: String,
    name: String,
    product_code: Option<String>,
    quota: Option<QuotaSettings>,
    tags: BTreeMap<String, String>,
    throttle: Option<ThrottleSettings>,
    usage_plan_keys: BTreeMap<String, StoredUsagePlanKey>,
}

impl StoredUsagePlan {
    fn to_usage_plan(&self) -> UsagePlan {
        UsagePlan {
            api_stages: self.api_stages.clone(),
            description: self.description.clone(),
            id: self.id.clone(),
            name: self.name.clone(),
            product_code: self.product_code.clone(),
            quota: self.quota.clone(),
            tags: self.tags.clone(),
            throttle: self.throttle.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct StoredUsagePlanKey {
    id: String,
    name: String,
    type_: String,
    value: String,
}

impl StoredUsagePlanKey {
    fn to_usage_plan_key(&self) -> UsagePlanKey {
        UsagePlanKey {
            id: self.id.clone(),
            name: self.name.clone(),
            type_: self.type_.clone(),
            value: self.value.clone(),
        }
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
struct ApiGatewayStateKey {
    account_id: AccountId,
    region: RegionId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TagTarget<'a> {
    RestApi(&'a str),
    Stage { api_id: &'a str, stage_name: &'a str },
    ApiKey(&'a str),
    UsagePlan(&'a str),
}

fn scope_key(scope: &ApiGatewayScope) -> ApiGatewayStateKey {
    ApiGatewayStateKey {
        account_id: scope.account_id().clone(),
        region: scope.region().clone(),
    }
}

fn scope_from_key(key: &ApiGatewayStateKey) -> ApiGatewayScope {
    ApiGatewayScope::new(key.account_id.clone(), key.region.clone())
}

pub(crate) fn rest_api<'a>(
    state: &'a StoredApiGatewayState,
    api_id: &str,
) -> Result<&'a StoredRestApi, ApiGatewayError> {
    state.rest_apis.get(api_id).ok_or_else(|| ApiGatewayError::NotFound {
        resource: format!("Rest API {api_id}"),
    })
}

fn rest_api_mut<'a>(
    state: &'a mut StoredApiGatewayState,
    api_id: &str,
) -> Result<&'a mut StoredRestApi, ApiGatewayError> {
    state.rest_apis.get_mut(api_id).ok_or_else(|| ApiGatewayError::NotFound {
        resource: format!("Rest API {api_id}"),
    })
}

fn resource<'a>(
    api: &'a StoredRestApi,
    resource_id: &str,
) -> Result<&'a StoredApiResource, ApiGatewayError> {
    api.resources.get(resource_id).ok_or_else(|| ApiGatewayError::NotFound {
        resource: format!("Resource {resource_id}"),
    })
}

fn resource_mut<'a>(
    api: &'a mut StoredRestApi,
    resource_id: &str,
) -> Result<&'a mut StoredApiResource, ApiGatewayError> {
    api.resources.get_mut(resource_id).ok_or_else(|| {
        ApiGatewayError::NotFound {
            resource: format!("Resource {resource_id}"),
        }
    })
}

fn method<'a>(
    api: &'a StoredRestApi,
    resource_id: &str,
    http_method: &str,
) -> Result<&'a StoredApiMethod, ApiGatewayError> {
    let normalized = normalize_http_method(http_method)?;
    resource(api, resource_id)?.methods.get(&normalized).ok_or_else(|| {
        ApiGatewayError::NotFound {
            resource: format!("Method {normalized} on resource {resource_id}"),
        }
    })
}

fn method_mut<'a>(
    api: &'a mut StoredRestApi,
    resource_id: &str,
    http_method: &str,
) -> Result<&'a mut StoredApiMethod, ApiGatewayError> {
    let normalized = normalize_http_method(http_method)?;
    resource_mut(api, resource_id)?.methods.get_mut(&normalized).ok_or_else(
        || ApiGatewayError::NotFound {
            resource: format!("Method {normalized} on resource {resource_id}"),
        },
    )
}

pub(crate) fn deployment<'a>(
    api: &'a StoredRestApi,
    deployment_id: &str,
) -> Result<&'a StoredDeployment, ApiGatewayError> {
    api.deployments.get(deployment_id).ok_or_else(|| {
        ApiGatewayError::NotFound {
            resource: format!("Deployment {deployment_id}"),
        }
    })
}

pub(crate) fn stage<'a>(
    api: &'a StoredRestApi,
    stage_name: &str,
) -> Result<&'a StoredStage, ApiGatewayError> {
    api.stages.get(stage_name).ok_or_else(|| ApiGatewayError::NotFound {
        resource: format!("Stage {stage_name}"),
    })
}

fn stage_mut<'a>(
    api: &'a mut StoredRestApi,
    stage_name: &str,
) -> Result<&'a mut StoredStage, ApiGatewayError> {
    api.stages.get_mut(stage_name).ok_or_else(|| ApiGatewayError::NotFound {
        resource: format!("Stage {stage_name}"),
    })
}

fn authorizer<'a>(
    api: &'a StoredRestApi,
    authorizer_id: &str,
) -> Result<&'a StoredAuthorizer, ApiGatewayError> {
    api.authorizers.get(authorizer_id).ok_or_else(|| {
        ApiGatewayError::NotFound {
            resource: format!("Authorizer {authorizer_id}"),
        }
    })
}

fn authorizer_mut<'a>(
    api: &'a mut StoredRestApi,
    authorizer_id: &str,
) -> Result<&'a mut StoredAuthorizer, ApiGatewayError> {
    api.authorizers.get_mut(authorizer_id).ok_or_else(|| {
        ApiGatewayError::NotFound {
            resource: format!("Authorizer {authorizer_id}"),
        }
    })
}

fn request_validator<'a>(
    api: &'a StoredRestApi,
    request_validator_id: &str,
) -> Result<&'a StoredRequestValidator, ApiGatewayError> {
    api.request_validators.get(request_validator_id).ok_or_else(|| {
        ApiGatewayError::NotFound {
            resource: format!("Request validator {request_validator_id}"),
        }
    })
}

fn request_validator_mut<'a>(
    api: &'a mut StoredRestApi,
    request_validator_id: &str,
) -> Result<&'a mut StoredRequestValidator, ApiGatewayError> {
    api.request_validators.get_mut(request_validator_id).ok_or_else(|| {
        ApiGatewayError::NotFound {
            resource: format!("Request validator {request_validator_id}"),
        }
    })
}

pub(crate) fn execute_api_request_validator<'a>(
    api: &'a StoredRestApi,
    resource_id: &str,
    http_method: &str,
) -> Option<&'a StoredRequestValidator> {
    execute_api_method(api, resource_id, http_method)
        .and_then(|method| method.request_validator_id.as_deref())
        .and_then(|request_validator_id| {
            api.request_validators.get(request_validator_id)
        })
}

pub(crate) fn is_valid_execute_api_key(
    state: &StoredApiGatewayState,
    api_id: &str,
    stage_name: &str,
    key_value: &str,
) -> bool {
    let stage_key = format!("{api_id}/{stage_name}");
    state.api_keys.values().any(|api_key| {
        api_key.enabled
            && api_key.value == key_value
            && (api_key
                .stage_keys
                .iter()
                .any(|candidate| candidate == &stage_key)
                || state.usage_plans.values().any(|usage_plan| {
                    usage_plan.api_stages.iter().any(|api_stage| {
                        api_stage.api_id == api_id
                            && api_stage.stage == stage_name
                    }) && usage_plan.usage_plan_keys.contains_key(&api_key.id)
                }))
    })
}

fn api_key<'a>(
    state: &'a StoredApiGatewayState,
    api_key_id: &str,
) -> Result<&'a StoredApiKey, ApiGatewayError> {
    state.api_keys.get(api_key_id).ok_or_else(|| ApiGatewayError::NotFound {
        resource: format!("API key {api_key_id}"),
    })
}

fn api_key_mut<'a>(
    state: &'a mut StoredApiGatewayState,
    api_key_id: &str,
) -> Result<&'a mut StoredApiKey, ApiGatewayError> {
    state.api_keys.get_mut(api_key_id).ok_or_else(|| {
        ApiGatewayError::NotFound { resource: format!("API key {api_key_id}") }
    })
}

fn usage_plan<'a>(
    state: &'a StoredApiGatewayState,
    usage_plan_id: &str,
) -> Result<&'a StoredUsagePlan, ApiGatewayError> {
    state.usage_plans.get(usage_plan_id).ok_or_else(|| {
        ApiGatewayError::NotFound {
            resource: format!("Usage plan {usage_plan_id}"),
        }
    })
}

fn usage_plan_mut<'a>(
    state: &'a mut StoredApiGatewayState,
    usage_plan_id: &str,
) -> Result<&'a mut StoredUsagePlan, ApiGatewayError> {
    state.usage_plans.get_mut(usage_plan_id).ok_or_else(|| {
        ApiGatewayError::NotFound {
            resource: format!("Usage plan {usage_plan_id}"),
        }
    })
}

fn usage_plan_key<'a>(
    plan: &'a StoredUsagePlan,
    key_id: &str,
) -> Result<&'a StoredUsagePlanKey, ApiGatewayError> {
    plan.usage_plan_keys.get(key_id).ok_or_else(|| ApiGatewayError::NotFound {
        resource: format!("Usage plan key {key_id}"),
    })
}

fn execute_api_method<'a>(
    api: &'a StoredRestApi,
    resource_id: &str,
    http_method: &str,
) -> Option<&'a StoredApiMethod> {
    let normalized = normalize_http_method(http_method).ok()?;
    let resource = api.resources.get(resource_id)?;

    resource.methods.get(&normalized).or_else(|| resource.methods.get("ANY"))
}

pub(crate) fn paginate<T: Clone>(
    items: Vec<T>,
    position: Option<&str>,
    limit: Option<u32>,
) -> Result<ItemCollection<T>, ApiGatewayError> {
    let start = position.map(parse_position).transpose()?.unwrap_or(0);
    let limit = limit.unwrap_or(25) as usize;
    let end = start.saturating_add(limit).min(items.len());
    let position = (end < items.len()).then(|| end.to_string());
    let page = items.get(start.min(items.len())..end).unwrap_or(&[]).to_vec();

    Ok(ItemCollection { item: page, position })
}

pub(crate) fn next_identifier(counter: &mut u64, width: usize) -> String {
    *counter += 1;
    encode_base36(*counter, width)
}

fn encode_base36(mut value: u64, width: usize) -> String {
    let mut digits = Vec::new();
    while value > 0 {
        let digit = (value % 36) as u8;
        digits.push(match digit {
            0..=9 => (b'0' + digit) as char,
            _ => (b'a' + (digit - 10)) as char,
        });
        value /= 36;
    }
    while digits.len() < width {
        digits.push('0');
    }
    digits.reverse();
    digits.into_iter().collect()
}

fn parse_position(position: &str) -> Result<usize, ApiGatewayError> {
    position.parse::<usize>().map_err(|_| ApiGatewayError::Validation {
        message: format!("invalid pagination position {position:?}"),
    })
}

fn patch_path<'a>(
    operation: &'a PatchOperation,
    resource: &'static str,
) -> Result<(&'a str, &'a str), ApiGatewayError> {
    let op = operation.op.trim();
    if !matches!(op, "add" | "remove" | "replace") {
        return Err(ApiGatewayError::Validation {
            message: format!(
                "unsupported patch operation {op:?} for {resource}"
            ),
        });
    }
    if !operation.path.starts_with('/') {
        return Err(ApiGatewayError::Validation {
            message: format!(
                "patch path {:?} for {resource} must start with '/'",
                operation.path
            ),
        });
    }

    Ok((op, operation.path.as_str()))
}

fn required_patch_value(
    operation: &PatchOperation,
) -> Result<&str, ApiGatewayError> {
    operation.value.as_deref().ok_or_else(|| ApiGatewayError::Validation {
        message: format!(
            "patch operation {:?} requires a value",
            operation.path
        ),
    })
}

fn parse_patch_bool(
    operation: &PatchOperation,
) -> Result<bool, ApiGatewayError> {
    required_patch_value(operation)?.parse::<bool>().map_err(|_| {
        ApiGatewayError::Validation {
            message: format!(
                "patch path {:?} requires a boolean value",
                operation.path
            ),
        }
    })
}

fn patch_bool_with_default(
    operation: &PatchOperation,
    default: bool,
) -> Result<bool, ApiGatewayError> {
    match operation.value.as_deref() {
        Some(_) => parse_patch_bool(operation),
        None => Ok(default),
    }
}

fn parse_patch_u32(
    operation: &PatchOperation,
) -> Result<u32, ApiGatewayError> {
    required_patch_value(operation)?.parse::<u32>().map_err(|_| {
        ApiGatewayError::Validation {
            message: format!(
                "patch path {:?} requires an integer value",
                operation.path
            ),
        }
    })
}

fn decode_patch_segment(segment: &str) -> Result<String, ApiGatewayError> {
    urlencoding::decode(&segment.replace("~1", "/"))
        .map(|decoded| decoded.into_owned())
        .map_err(|_| ApiGatewayError::Validation {
            message: format!("invalid patch path segment {segment:?}"),
        })
}

pub(crate) fn normalize_http_method(
    method: &str,
) -> Result<String, ApiGatewayError> {
    let method = method.trim().to_ascii_uppercase();
    if matches!(
        method.as_str(),
        "ANY"
            | "DELETE"
            | "GET"
            | "HEAD"
            | "OPTIONS"
            | "PATCH"
            | "POST"
            | "PUT"
    ) {
        Ok(method)
    } else {
        Err(ApiGatewayError::Validation {
            message: format!("unsupported HTTP method {method:?}"),
        })
    }
}

pub(crate) fn validate_status_code(
    status_code: &str,
) -> Result<(), ApiGatewayError> {
    if status_code.len() == 3
        && status_code.chars().all(|character| character.is_ascii_digit())
    {
        Ok(())
    } else {
        Err(ApiGatewayError::Validation {
            message: format!("invalid status code {status_code:?}"),
        })
    }
}

pub(crate) fn validate_name(
    field: &str,
    value: &str,
) -> Result<(), ApiGatewayError> {
    if value.trim().is_empty() {
        return Err(ApiGatewayError::Validation {
            message: format!("{field} must not be blank"),
        });
    }

    Ok(())
}

fn validate_path_part(path_part: &str) -> Result<(), ApiGatewayError> {
    if path_part.trim().is_empty() || path_part.contains('/') {
        return Err(ApiGatewayError::Validation {
            message: format!("invalid resource path part {path_part:?}"),
        });
    }
    if path_part.starts_with('{') && !path_part.ends_with('}') {
        return Err(ApiGatewayError::Validation {
            message: format!("invalid resource path part {path_part:?}"),
        });
    }
    Ok(())
}

fn validate_resource_hierarchy(
    api: &StoredRestApi,
    parent_id: &str,
    path_part: &str,
) -> Result<(), ApiGatewayError> {
    let parent = resource(api, parent_id)?;
    if parent.path_part.as_deref().is_some_and(is_greedy_path_part) {
        return Err(ApiGatewayError::Validation {
            message: "greedy proxy resources cannot contain child resources"
                .to_owned(),
        });
    }

    let siblings = api
        .resources
        .values()
        .filter(|resource| resource.parent_id.as_deref() == Some(parent_id))
        .collect::<Vec<_>>();
    if siblings
        .iter()
        .any(|resource| resource.path_part.as_deref() == Some(path_part))
    {
        return Err(ApiGatewayError::Conflict {
            resource: format!(
                "Resource path part {path_part} under parent {parent_id}"
            ),
        });
    }

    if is_variable_path_part(path_part)
        && siblings.iter().any(|resource| {
            resource.path_part.as_deref().is_some_and(is_variable_path_part)
        })
    {
        return Err(ApiGatewayError::Validation {
            message:
                "a parent resource cannot have multiple variable child resources"
                    .to_owned(),
        });
    }

    Ok(())
}

fn validate_resource_patch(
    api: &StoredRestApi,
    resource_id: &str,
    path_part: &str,
    next_parent_id: Option<&str>,
) -> Result<(), ApiGatewayError> {
    if api.root_resource_id == resource_id {
        return Err(ApiGatewayError::Validation {
            message: "the root resource cannot be updated".to_owned(),
        });
    }

    let parent_id = next_parent_id.or_else(|| {
        resource(api, resource_id)
            .ok()
            .and_then(|resource| resource.parent_id.as_deref())
    });
    let Some(parent_id) = parent_id else {
        return Err(ApiGatewayError::Validation {
            message: format!("resource {resource_id} is missing its parent"),
        });
    };

    if descendant_resource_ids(api, resource_id)
        .iter()
        .skip(1)
        .any(|id| id == parent_id)
    {
        return Err(ApiGatewayError::Validation {
            message:
                "resources cannot be reparented beneath their own descendants"
                    .to_owned(),
        });
    }

    validate_resource_hierarchy(api, parent_id, path_part)
}

fn recompute_resource_paths(
    api: &mut StoredRestApi,
) -> Result<(), ApiGatewayError> {
    let root_id = api.root_resource_id.clone();
    recompute_resource_path(api, &root_id, ROOT_PATH)
}

fn recompute_resource_path(
    api: &mut StoredRestApi,
    resource_id: &str,
    path: &str,
) -> Result<(), ApiGatewayError> {
    let children = api
        .resources
        .values()
        .filter(|resource| resource.parent_id.as_deref() == Some(resource_id))
        .map(|resource| resource.id.clone())
        .collect::<Vec<_>>();
    resource_mut(api, resource_id)?.path = path.to_owned();
    for child_id in children {
        let path_part =
            resource(api, &child_id)?.path_part.clone().unwrap_or_default();
        let child_path = join_resource_path(path, &path_part);
        recompute_resource_path(api, &child_id, &child_path)?;
    }
    Ok(())
}

fn current_path_part<'a>(
    api: &'a StoredRestApi,
    resource_id: &str,
) -> Result<&'a str, ApiGatewayError> {
    resource(api, resource_id)?.path_part.as_deref().ok_or_else(|| {
        ApiGatewayError::Validation {
            message: format!(
                "resource {resource_id} does not have a path part"
            ),
        }
    })
}

fn descendant_resource_ids(
    api: &StoredRestApi,
    resource_id: &str,
) -> Vec<String> {
    let mut ordered = Vec::new();
    let mut pending = vec![resource_id.to_owned()];
    while let Some(next) = pending.pop() {
        ordered.push(next.clone());
        let children = api
            .resources
            .values()
            .filter(|resource| {
                resource.parent_id.as_deref() == Some(next.as_str())
            })
            .map(|resource| resource.id.clone())
            .collect::<Vec<_>>();
        pending.extend(children);
    }
    ordered
}

fn join_resource_path(parent_path: &str, path_part: &str) -> String {
    if parent_path == ROOT_PATH {
        format!("/{path_part}")
    } else {
        format!("{parent_path}/{path_part}")
    }
}

fn is_variable_path_part(path_part: &str) -> bool {
    path_part.starts_with('{') && path_part.ends_with('}')
}

fn is_greedy_path_part(path_part: &str) -> bool {
    is_variable_path_part(path_part) && path_part.contains('+')
}

fn validate_authorization_type(value: &str) -> Result<(), ApiGatewayError> {
    if matches!(value, "NONE" | "AWS_IAM" | "CUSTOM" | "COGNITO_USER_POOLS") {
        Ok(())
    } else {
        Err(ApiGatewayError::Validation {
            message: format!("unsupported authorization type {value:?}"),
        })
    }
}

fn validate_integration(
    input: &PutIntegrationInput,
) -> Result<(), ApiGatewayError> {
    if !input.cache_key_parameters.is_empty()
        || input.cache_namespace.is_some()
    {
        return Err(ApiGatewayError::UnsupportedOperation {
            message: "API Gateway cache configuration is not supported."
                .to_owned(),
        });
    }

    let connection_type = input
        .connection_type
        .as_deref()
        .unwrap_or("INTERNET")
        .to_ascii_uppercase();
    if connection_type == "VPC_LINK" || connection_type == "PRIVATE" {
        return Err(ApiGatewayError::UnsupportedOperation {
            message: "API Gateway private integrations are not supported."
                .to_owned(),
        });
    }

    match input.type_.as_str() {
        "MOCK" => Ok(()),
        "HTTP" | "HTTP_PROXY" => {
            let Some(uri) = input.uri.as_deref() else {
                return Err(ApiGatewayError::Validation {
                    message: "integration uri is required".to_owned(),
                });
            };
            if !(uri.starts_with("http://") || uri.starts_with("https://")) {
                return Err(ApiGatewayError::Validation {
                    message: format!("invalid HTTP integration uri {uri:?}"),
                });
            }
            Ok(())
        }
        "AWS_PROXY" => {
            let Some(uri) = input.uri.as_deref() else {
                return Err(ApiGatewayError::Validation {
                    message: "integration uri is required".to_owned(),
                });
            };
            if !uri.starts_with("arn:aws:apigateway:")
                || !uri.contains(":lambda:path/")
                || !uri.ends_with("/invocations")
            {
                return Err(ApiGatewayError::Validation {
                    message: format!(
                        "unsupported integration uri {uri:?}; only Lambda AWS_PROXY integrations are supported"
                    ),
                });
            }
            if execute_api::lambda_function_target(input.uri.as_deref())
                .is_none()
            {
                return Err(ApiGatewayError::Validation {
                    message: format!(
                        "unsupported integration uri {uri:?}; only Lambda AWS_PROXY integrations are supported"
                    ),
                });
            }
            if input
                .integration_http_method
                .as_deref()
                .or(input.http_method.as_deref())
                != Some("POST")
            {
                return Err(ApiGatewayError::Validation {
                    message:
                        "Lambda proxy integrations require integrationHttpMethod POST."
                            .to_owned(),
                });
            }
            Ok(())
        }
        "AWS" => Err(ApiGatewayError::UnsupportedOperation {
            message:
                "AWS service integrations beyond Lambda are not supported."
                    .to_owned(),
        }),
        other => Err(ApiGatewayError::Validation {
            message: format!("unsupported integration type {other:?}"),
        }),
    }
}

pub(crate) fn validate_stage_name(
    stage_name: &str,
) -> Result<(), ApiGatewayError> {
    if stage_name.trim().is_empty() || stage_name.contains('/') {
        return Err(ApiGatewayError::Validation {
            message: format!("invalid stage name {stage_name:?}"),
        });
    }
    Ok(())
}

fn validate_authorizer_input(
    input: &CreateAuthorizerInput,
) -> Result<(), ApiGatewayError> {
    validate_authorizer_type(&input.type_)?;
    match input.type_.as_str() {
        "TOKEN" | "REQUEST" => {
            validate_authorizer_uri(
                input.authorizer_uri.as_deref().ok_or_else(|| {
                    ApiGatewayError::Validation {
                        message: "authorizerUri is required".to_owned(),
                    }
                })?,
            )?;
            validate_name(
                "identitySource",
                input.identity_source.as_deref().ok_or_else(|| {
                    ApiGatewayError::Validation {
                        message: "identitySource is required".to_owned(),
                    }
                })?,
            )
        }
        "COGNITO_USER_POOLS" => {
            if input.provider_arns.is_empty() {
                return Err(ApiGatewayError::Validation {
                    message:
                        "providerARNs are required for Cognito authorizers."
                            .to_owned(),
                });
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn validate_authorizer_type(value: &str) -> Result<(), ApiGatewayError> {
    if matches!(value, "TOKEN" | "REQUEST" | "COGNITO_USER_POOLS") {
        Ok(())
    } else {
        Err(ApiGatewayError::Validation {
            message: format!("unsupported authorizer type {value:?}"),
        })
    }
}

fn validate_authorizer_uri(uri: &str) -> Result<(), ApiGatewayError> {
    if uri.starts_with("arn:aws:apigateway:") && uri.ends_with("/invocations")
    {
        Ok(())
    } else {
        Err(ApiGatewayError::Validation {
            message: format!("invalid authorizerUri {uri:?}"),
        })
    }
}

pub(crate) fn validate_tags(
    tags: &BTreeMap<String, String>,
) -> Result<(), ApiGatewayError> {
    if tags.len() > TAGS_LIMIT {
        return Err(ApiGatewayError::Validation {
            message: format!("a maximum of {TAGS_LIMIT} tags is supported"),
        });
    }
    for (key, value) in tags {
        validate_name("tag key", key)?;
        if value.trim().is_empty() {
            return Err(ApiGatewayError::Validation {
                message: format!(
                    "tag value for key {key:?} must not be blank"
                ),
            });
        }
    }
    Ok(())
}

fn validate_usage_plan_api_stages(
    state: &StoredApiGatewayState,
    api_stages: &[UsagePlanApiStage],
) -> Result<(), ApiGatewayError> {
    for stage in api_stages {
        validate_stage_reference(state, &stage.api_id, &stage.stage)?;
    }
    Ok(())
}

fn validate_stage_reference(
    state: &StoredApiGatewayState,
    api_id: &str,
    stage_name: &str,
) -> Result<(), ApiGatewayError> {
    let api = rest_api(state, api_id)?;
    stage(api, stage_name)?;
    Ok(())
}

fn normalize_rest_api_endpoint_configuration(
    endpoint_configuration: Option<EndpointConfiguration>,
) -> Result<EndpointConfiguration, ApiGatewayError> {
    let mut endpoint_configuration =
        endpoint_configuration.unwrap_or_else(|| EndpointConfiguration {
            ip_address_type: None,
            types: vec!["EDGE".to_owned()],
            vpc_endpoint_ids: Vec::new(),
        });
    if endpoint_configuration.types.is_empty() {
        endpoint_configuration.types.push("EDGE".to_owned());
    }
    let Some(type_) = endpoint_configuration.types.first().map(String::as_str)
    else {
        return Err(ApiGatewayError::Validation {
            message: "endpointConfiguration.types must not be empty"
                .to_owned(),
        });
    };
    if !matches!(type_, "EDGE" | "REGIONAL" | "PRIVATE") {
        return Err(ApiGatewayError::Validation {
            message: format!("unsupported endpoint type {type_:?}"),
        });
    }
    endpoint_configuration.ip_address_type =
        Some(endpoint_configuration.ip_address_type.unwrap_or_else(|| {
            if type_ == "PRIVATE" {
                "dualstack".to_owned()
            } else {
                "ipv4".to_owned()
            }
        }));

    Ok(endpoint_configuration)
}

fn empty_to_none(value: impl Into<String>) -> Option<String> {
    let value = value.into();
    (!value.trim().is_empty()).then_some(value)
}

pub(crate) fn optional_empty_to_none(value: Option<String>) -> Option<String> {
    value.and_then(empty_to_none)
}

fn remove_tag_keys(tags: &mut BTreeMap<String, String>, tag_keys: &[String]) {
    let keys = tag_keys.iter().map(String::as_str).collect::<BTreeSet<_>>();
    tags.retain(|key, _| !keys.contains(key.as_str()));
}

fn parse_tag_target<'a>(
    region: &RegionId,
    resource_arn: &'a str,
) -> Result<TagTarget<'a>, ApiGatewayError> {
    let prefix = format!("arn:aws:apigateway:{}::/", region.as_str());
    let Some(path) = resource_arn.strip_prefix(&prefix) else {
        return Err(ApiGatewayError::Validation {
            message: format!("unsupported API Gateway ARN {resource_arn:?}"),
        });
    };
    let segments = path.split('/').collect::<Vec<_>>();
    match segments.as_slice() {
        ["restapis", api_id] => Ok(TagTarget::RestApi(api_id)),
        ["restapis", api_id, "stages", stage_name] => {
            Ok(TagTarget::Stage { api_id, stage_name })
        }
        ["apikeys", api_key_id] => Ok(TagTarget::ApiKey(api_key_id)),
        ["usageplans", usage_plan_id] => {
            Ok(TagTarget::UsagePlan(usage_plan_id))
        }
        ["domainnames", _domain_name] => {
            Err(ApiGatewayError::UnsupportedOperation {
                message:
                    "API Gateway custom domains and base path mappings are not supported."
                        .to_owned(),
            })
        }
        _ => Err(ApiGatewayError::Validation {
            message: format!("unsupported API Gateway ARN {resource_arn:?}"),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ApiGatewayScope, ApiGatewayService, CreateApiKeyInput,
        CreateAuthorizerInput, CreateDeploymentInput,
        CreateRequestValidatorInput, CreateResourceInput, CreateRestApiInput,
        CreateStageInput, CreateUsagePlanInput, CreateUsagePlanKeyInput,
        PatchOperation, PutIntegrationInput, PutMethodInput,
        PutMethodResponseInput, UsagePlanApiStage, is_valid_execute_api_key,
    };
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::SystemTime;
    use std::time::UNIX_EPOCH;
    use storage::{StorageConfig, StorageFactory, StorageMode};

    struct TestClock;

    impl aws::Clock for TestClock {
        fn now(&self) -> SystemTime {
            UNIX_EPOCH
        }
    }

    fn service(label: &str) -> ApiGatewayService {
        static NEXT_ID: AtomicUsize = AtomicUsize::new(0);

        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        let root = std::env::temp_dir()
            .join(format!("cloudish-{label}-{}-{id}", std::process::id()));
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

    #[test]
    fn apigw_v1_control_rest_api_resource_method_and_stage_round_trip() {
        let service = service("apigw-v1-control-round-trip");
        let scope = scope();
        let api = service
            .create_rest_api(
                &scope,
                CreateRestApiInput {
                    binary_media_types: Vec::new(),
                    description: Some("demo".to_owned()),
                    disable_execute_api_endpoint: Some(false),
                    endpoint_configuration: None,
                    name: "demo".to_owned(),
                    tags: BTreeMap::from([(
                        "env".to_owned(),
                        "test".to_owned(),
                    )]),
                },
            )
            .expect("rest api should be created");
        assert_eq!(api.name, "demo");
        assert_eq!(api.root_resource_id.len(), 8);

        let resource = service
            .create_resource(
                &scope,
                &api.id,
                &api.root_resource_id,
                CreateResourceInput { path_part: "pets".to_owned() },
            )
            .expect("resource should be created");
        assert_eq!(resource.path, "/pets");

        let method = service
            .put_method(
                &scope,
                &api.id,
                &resource.id,
                "GET",
                PutMethodInput {
                    api_key_required: Some(false),
                    authorizer_id: None,
                    authorization_type: "NONE".to_owned(),
                    operation_name: Some("ListPets".to_owned()),
                    request_models: BTreeMap::new(),
                    request_parameters: BTreeMap::new(),
                    request_validator_id: None,
                },
            )
            .expect("method should be created");
        assert_eq!(method.http_method, "GET");

        let integration = service
            .put_integration(
                &scope,
                &api.id,
                &resource.id,
                "GET",
                PutIntegrationInput {
                    cache_key_parameters: Vec::new(),
                    cache_namespace: None,
                    connection_id: None,
                    connection_type: None,
                    content_handling: None,
                    credentials: None,
                    http_method: Some("GET".to_owned()),
                    integration_http_method: None,
                    passthrough_behavior: Some("WHEN_NO_MATCH".to_owned()),
                    request_parameters: BTreeMap::new(),
                    request_templates: BTreeMap::new(),
                    timeout_in_millis: None,
                    type_: "HTTP_PROXY".to_owned(),
                    uri: Some("https://example.com/pets".to_owned()),
                },
            )
            .expect("integration should be created");
        assert_eq!(integration.type_, "HTTP_PROXY");

        let method_response = service
            .put_method_response(
                &scope,
                &api.id,
                &resource.id,
                "GET",
                "200",
                PutMethodResponseInput {
                    response_models: BTreeMap::from([(
                        "application/json".to_owned(),
                        "Empty".to_owned(),
                    )]),
                    response_parameters: BTreeMap::from([(
                        "method.response.header.Content-Type".to_owned(),
                        true,
                    )]),
                },
            )
            .expect("method response should be created");
        assert_eq!(method_response.status_code, "200");

        let deployment = service
            .create_deployment(
                &scope,
                &api.id,
                CreateDeploymentInput {
                    description: Some("first".to_owned()),
                    stage_description: None,
                    stage_name: None,
                    variables: BTreeMap::new(),
                },
            )
            .expect("deployment should be created");
        let stage = service
            .create_stage(
                &scope,
                &api.id,
                CreateStageInput {
                    cache_cluster_enabled: None,
                    cache_cluster_size: None,
                    deployment_id: deployment.id.clone(),
                    description: Some("dev".to_owned()),
                    stage_name: "dev".to_owned(),
                    tags: BTreeMap::new(),
                    variables: BTreeMap::from([(
                        "lambdaAlias".to_owned(),
                        "live".to_owned(),
                    )]),
                },
            )
            .expect("stage should be created");
        assert_eq!(stage.stage_name, "dev");
        assert_eq!(
            stage.deployment_id.as_deref(),
            Some(deployment.id.as_str())
        );

        let fetched_apis = service
            .get_rest_apis(&scope, None, None)
            .expect("rest apis should list");
        assert_eq!(fetched_apis.item.len(), 1);
        let fetched_resources = service
            .get_resources(&scope, &api.id, None, None)
            .expect("resources should list");
        assert_eq!(fetched_resources.item.len(), 2);
        let fetched_method = service
            .get_method(&scope, &api.id, &resource.id, "GET")
            .expect("method should fetch");
        assert_eq!(fetched_method.operation_name.as_deref(), Some("ListPets"));
        let fetched_stage = service
            .get_stage(&scope, &api.id, "dev")
            .expect("stage should fetch");
        assert_eq!(
            fetched_stage.variables.get("lambdaAlias"),
            Some(&"live".to_owned())
        );
    }

    #[test]
    fn apigw_v1_control_invalid_resource_hierarchy_fails_explicitly() {
        let service = service("apigw-v1-control-resource-validation");
        let scope = scope();
        let api = service
            .create_rest_api(
                &scope,
                CreateRestApiInput {
                    binary_media_types: Vec::new(),
                    description: None,
                    disable_execute_api_endpoint: None,
                    endpoint_configuration: None,
                    name: "demo".to_owned(),
                    tags: BTreeMap::new(),
                },
            )
            .expect("rest api should be created");
        let proxy = service
            .create_resource(
                &scope,
                &api.id,
                &api.root_resource_id,
                CreateResourceInput { path_part: "{proxy+}".to_owned() },
            )
            .expect("proxy resource should be created");

        let sibling_error = service
            .create_resource(
                &scope,
                &api.id,
                &api.root_resource_id,
                CreateResourceInput { path_part: "{child}".to_owned() },
            )
            .expect_err("dynamic sibling should fail");
        assert!(
            sibling_error
                .to_string()
                .contains("multiple variable child resources")
        );

        let child_error = service
            .create_resource(
                &scope,
                &api.id,
                &proxy.id,
                CreateResourceInput { path_part: "nested".to_owned() },
            )
            .expect_err("child beneath greedy proxy should fail");
        assert!(
            child_error.to_string().contains("cannot contain child resources")
        );
    }

    #[test]
    fn apigw_v1_control_private_integrations_and_cache_settings_fail_explicitly()
     {
        let service = service("apigw-v1-control-integration-validation");
        let scope = scope();
        let api = service
            .create_rest_api(
                &scope,
                CreateRestApiInput {
                    binary_media_types: Vec::new(),
                    description: None,
                    disable_execute_api_endpoint: None,
                    endpoint_configuration: None,
                    name: "demo".to_owned(),
                    tags: BTreeMap::new(),
                },
            )
            .expect("rest api should be created");
        service
            .put_method(
                &scope,
                &api.id,
                &api.root_resource_id,
                "GET",
                PutMethodInput {
                    api_key_required: Some(false),
                    authorizer_id: None,
                    authorization_type: "NONE".to_owned(),
                    operation_name: None,
                    request_models: BTreeMap::new(),
                    request_parameters: BTreeMap::new(),
                    request_validator_id: None,
                },
            )
            .expect("method should be created");

        let private_error = service
            .put_integration(
                &scope,
                &api.id,
                &api.root_resource_id,
                "GET",
                PutIntegrationInput {
                    cache_key_parameters: Vec::new(),
                    cache_namespace: None,
                    connection_id: Some("vpclink-123".to_owned()),
                    connection_type: Some("VPC_LINK".to_owned()),
                    content_handling: None,
                    credentials: None,
                    http_method: Some("GET".to_owned()),
                    integration_http_method: Some("POST".to_owned()),
                    passthrough_behavior: None,
                    request_parameters: BTreeMap::new(),
                    request_templates: BTreeMap::new(),
                    timeout_in_millis: None,
                    type_: "HTTP_PROXY".to_owned(),
                    uri: Some("https://example.com".to_owned()),
                },
            )
            .expect_err("private integrations should fail");
        assert!(
            private_error
                .to_string()
                .contains("private integrations are not supported")
        );

        let cache_error = service
            .put_integration(
                &scope,
                &api.id,
                &api.root_resource_id,
                "GET",
                PutIntegrationInput {
                    cache_key_parameters: vec!["method.request.path.id".to_owned()],
                    cache_namespace: Some("cache".to_owned()),
                    connection_id: None,
                    connection_type: None,
                    content_handling: None,
                    credentials: None,
                    http_method: Some("GET".to_owned()),
                    integration_http_method: Some("POST".to_owned()),
                    passthrough_behavior: None,
                    request_parameters: BTreeMap::new(),
                    request_templates: BTreeMap::new(),
                    timeout_in_millis: None,
                    type_: "AWS_PROXY".to_owned(),
                    uri: Some("arn:aws:apigateway:eu-west-2:lambda:path/2015-03-31/functions/arn:aws:lambda:eu-west-2:000000000000:function:demo/invocations".to_owned()),
                },
            )
            .expect_err("cache configuration should fail");
        assert!(
            cache_error
                .to_string()
                .contains("cache configuration is not supported")
        );

        let deployment = service
            .create_deployment(
                &scope,
                &api.id,
                CreateDeploymentInput {
                    description: None,
                    stage_description: None,
                    stage_name: None,
                    variables: BTreeMap::new(),
                },
            )
            .expect("deployment should be created");
        let stage_error = service
            .create_stage(
                &scope,
                &api.id,
                CreateStageInput {
                    cache_cluster_enabled: Some(true),
                    cache_cluster_size: Some("0.5".to_owned()),
                    deployment_id: deployment.id,
                    description: None,
                    stage_name: "dev".to_owned(),
                    tags: BTreeMap::new(),
                    variables: BTreeMap::new(),
                },
            )
            .expect_err("stage cache configuration should fail");
        assert!(
            stage_error
                .to_string()
                .contains("cache configuration is not supported")
        );
    }

    #[test]
    fn apigw_v1_control_duplicate_method_creation_fails_explicitly() {
        let service = service("apigw-v1-control-duplicate-method");
        let scope = scope();
        let api = service
            .create_rest_api(
                &scope,
                CreateRestApiInput {
                    binary_media_types: Vec::new(),
                    description: None,
                    disable_execute_api_endpoint: None,
                    endpoint_configuration: None,
                    name: "demo".to_owned(),
                    tags: BTreeMap::new(),
                },
            )
            .expect("rest api should be created");
        service
            .put_method(
                &scope,
                &api.id,
                &api.root_resource_id,
                "GET",
                PutMethodInput {
                    api_key_required: Some(false),
                    authorizer_id: None,
                    authorization_type: "NONE".to_owned(),
                    operation_name: None,
                    request_models: BTreeMap::new(),
                    request_parameters: BTreeMap::new(),
                    request_validator_id: None,
                },
            )
            .expect("method should be created");

        let duplicate_error = service
            .put_method(
                &scope,
                &api.id,
                &api.root_resource_id,
                "GET",
                PutMethodInput {
                    api_key_required: Some(false),
                    authorizer_id: None,
                    authorization_type: "NONE".to_owned(),
                    operation_name: Some("duplicate".to_owned()),
                    request_models: BTreeMap::new(),
                    request_parameters: BTreeMap::new(),
                    request_validator_id: None,
                },
            )
            .expect_err("duplicate method should fail");

        assert!(matches!(
            duplicate_error,
            super::ApiGatewayError::Conflict { .. }
        ));
        assert!(
            duplicate_error.to_string().contains("Method GET for resource")
        );
    }

    #[test]
    fn apigw_v1_control_tags_api_keys_and_usage_plans_round_trip() {
        let service = service("apigw-v1-control-tags");
        let scope = scope();
        let api = service
            .create_rest_api(
                &scope,
                CreateRestApiInput {
                    binary_media_types: Vec::new(),
                    description: None,
                    disable_execute_api_endpoint: None,
                    endpoint_configuration: None,
                    name: "demo".to_owned(),
                    tags: BTreeMap::from([(
                        "service".to_owned(),
                        "pets".to_owned(),
                    )]),
                },
            )
            .expect("rest api should be created");
        service
            .tag_resource(
                &scope,
                &format!("arn:aws:apigateway:eu-west-2::/restapis/{}", api.id),
                BTreeMap::from([("team".to_owned(), "edge".to_owned())]),
            )
            .expect("tag resource should succeed");
        let tags = service
            .get_tags(
                &scope,
                &format!("arn:aws:apigateway:eu-west-2::/restapis/{}", api.id),
            )
            .expect("tags should fetch");
        assert_eq!(tags.tags.get("service"), Some(&"pets".to_owned()));
        assert_eq!(tags.tags.get("team"), Some(&"edge".to_owned()));

        let api_key = service
            .create_api_key(
                &scope,
                CreateApiKeyInput {
                    customer_id: None,
                    description: Some("client key".to_owned()),
                    enabled: Some(true),
                    name: Some("demo-key".to_owned()),
                    stage_keys: Vec::new(),
                    tags: BTreeMap::new(),
                    value: Some("demo-secret".to_owned()),
                },
            )
            .expect("api key should create");
        assert_eq!(api_key.value.as_deref(), Some("demo-secret"));

        let deployment = service
            .create_deployment(
                &scope,
                &api.id,
                CreateDeploymentInput {
                    description: None,
                    stage_description: None,
                    stage_name: None,
                    variables: BTreeMap::new(),
                },
            )
            .expect("deployment should create");
        service
            .create_stage(
                &scope,
                &api.id,
                CreateStageInput {
                    cache_cluster_enabled: None,
                    cache_cluster_size: None,
                    deployment_id: deployment.id,
                    description: None,
                    stage_name: "dev".to_owned(),
                    tags: BTreeMap::new(),
                    variables: BTreeMap::new(),
                },
            )
            .expect("stage should create");
        let usage_plan = service
            .create_usage_plan(
                &scope,
                CreateUsagePlanInput {
                    api_stages: vec![UsagePlanApiStage {
                        api_id: api.id,
                        stage: "dev".to_owned(),
                    }],
                    description: Some("plan".to_owned()),
                    name: "basic".to_owned(),
                    product_code: None,
                    quota: None,
                    tags: BTreeMap::from([(
                        "tier".to_owned(),
                        "basic".to_owned(),
                    )]),
                    throttle: None,
                },
            )
            .expect("usage plan should create");
        service
            .create_usage_plan_key(
                &scope,
                &usage_plan.id,
                CreateUsagePlanKeyInput {
                    key_id: api_key.id,
                    key_type: "API_KEY".to_owned(),
                },
            )
            .expect("usage plan key should create");
        let usage_plan_keys = service
            .get_usage_plan_keys(&scope, &usage_plan.id, None, None)
            .expect("usage plan keys should list");
        assert_eq!(usage_plan_keys.item.len(), 1);
    }

    #[test]
    fn apigw_v1_control_domain_tag_arns_fail_explicitly() {
        let service = service("apigw-v1-control-domain-tag-error");
        let error = service
            .get_tags(
                &scope(),
                "arn:aws:apigateway:eu-west-2::/domainnames/api.example.test",
            )
            .expect_err("domain tag arns should fail explicitly");

        assert!(matches!(
            error,
            super::ApiGatewayError::UnsupportedOperation { .. }
        ));
        assert!(error.to_string().contains(
            "custom domains and base path mappings are not supported"
        ));
    }

    #[test]
    fn apigw_v1_execute_api_key_validation_uses_usage_plans_and_stage_keys() {
        let service = service("apigw-v1-execute-api-keys");
        let scope = scope();
        let api = service
            .create_rest_api(
                &scope,
                CreateRestApiInput {
                    binary_media_types: Vec::new(),
                    description: None,
                    disable_execute_api_endpoint: None,
                    endpoint_configuration: None,
                    name: "demo".to_owned(),
                    tags: BTreeMap::new(),
                },
            )
            .expect("rest api should be created");
        let deployment = service
            .create_deployment(
                &scope,
                &api.id,
                CreateDeploymentInput {
                    description: None,
                    stage_description: None,
                    stage_name: None,
                    variables: BTreeMap::new(),
                },
            )
            .expect("deployment should create");
        service
            .create_stage(
                &scope,
                &api.id,
                CreateStageInput {
                    cache_cluster_enabled: None,
                    cache_cluster_size: None,
                    deployment_id: deployment.id,
                    description: None,
                    stage_name: "dev".to_owned(),
                    tags: BTreeMap::new(),
                    variables: BTreeMap::new(),
                },
            )
            .expect("stage should create");
        let bound_stage_key = service
            .create_api_key(
                &scope,
                CreateApiKeyInput {
                    customer_id: None,
                    description: None,
                    enabled: Some(true),
                    name: Some("stage-key".to_owned()),
                    stage_keys: vec![format!("{}/dev", api.id)],
                    tags: BTreeMap::new(),
                    value: Some("stage-secret".to_owned()),
                },
            )
            .expect("stage key should create");
        let bound_usage_plan_key = service
            .create_api_key(
                &scope,
                CreateApiKeyInput {
                    customer_id: None,
                    description: None,
                    enabled: Some(true),
                    name: Some("usage-key".to_owned()),
                    stage_keys: Vec::new(),
                    tags: BTreeMap::new(),
                    value: Some("usage-secret".to_owned()),
                },
            )
            .expect("usage plan key should create");
        let usage_plan = service
            .create_usage_plan(
                &scope,
                CreateUsagePlanInput {
                    api_stages: vec![UsagePlanApiStage {
                        api_id: api.id.clone(),
                        stage: "dev".to_owned(),
                    }],
                    description: None,
                    name: "basic".to_owned(),
                    product_code: None,
                    quota: None,
                    tags: BTreeMap::new(),
                    throttle: None,
                },
            )
            .expect("usage plan should create");
        service
            .create_usage_plan_key(
                &scope,
                &usage_plan.id,
                CreateUsagePlanKeyInput {
                    key_id: bound_usage_plan_key.id.clone(),
                    key_type: "API_KEY".to_owned(),
                },
            )
            .expect("usage plan key should bind");

        let state = service.load_state(&scope);

        assert!(is_valid_execute_api_key(
            &state,
            &api.id,
            "dev",
            bound_stage_key
                .value
                .as_deref()
                .expect("stage key value should exist")
        ));
        assert!(is_valid_execute_api_key(
            &state,
            &api.id,
            "dev",
            bound_usage_plan_key
                .value
                .as_deref()
                .expect("usage plan key value should exist")
        ));
        assert!(!is_valid_execute_api_key(
            &state,
            &api.id,
            "dev",
            "invalid-secret"
        ));
    }

    #[test]
    fn apigw_v1_control_update_paths_apply_explicit_mutations() {
        let service = service("apigw-v1-control-updates");
        let scope = scope();
        let api = service
            .create_rest_api(
                &scope,
                CreateRestApiInput {
                    binary_media_types: Vec::new(),
                    description: Some("old".to_owned()),
                    disable_execute_api_endpoint: Some(false),
                    endpoint_configuration: None,
                    name: "demo".to_owned(),
                    tags: BTreeMap::new(),
                },
            )
            .expect("rest api should create");
        let updated_api = service
            .update_rest_api(
                &scope,
                &api.id,
                &[
                    PatchOperation {
                        op: "replace".to_owned(),
                        path: "/name".to_owned(),
                        value: Some("demo-v2".to_owned()),
                    },
                    PatchOperation {
                        op: "replace".to_owned(),
                        path: "/disableExecuteApiEndpoint".to_owned(),
                        value: Some("true".to_owned()),
                    },
                ],
            )
            .expect("rest api should update");
        assert_eq!(updated_api.name, "demo-v2");
        assert!(updated_api.disable_execute_api_endpoint);

        let authorizer = service
            .create_authorizer(
                &scope,
                &api.id,
                CreateAuthorizerInput {
                    authorizer_credentials: None,
                    authorizer_result_ttl_in_seconds: Some(300),
                    authorizer_uri: Some("arn:aws:apigateway:eu-west-2:lambda:path/2015-03-31/functions/arn:aws:lambda:eu-west-2:000000000000:function:auth/invocations".to_owned()),
                    identity_source: Some("method.request.header.Authorization".to_owned()),
                    identity_validation_expression: None,
                    name: "auth".to_owned(),
                    provider_arns: Vec::new(),
                    type_: "TOKEN".to_owned(),
                },
            )
            .expect("authorizer should create");
        let updated_authorizer = service
            .update_authorizer(
                &scope,
                &api.id,
                &authorizer.id,
                &[PatchOperation {
                    op: "replace".to_owned(),
                    path: "/authorizerResultTtlInSeconds".to_owned(),
                    value: Some("900".to_owned()),
                }],
            )
            .expect("authorizer should update");
        assert_eq!(
            updated_authorizer.authorizer_result_ttl_in_seconds,
            Some(900)
        );

        let validator = service
            .create_request_validator(
                &scope,
                &api.id,
                CreateRequestValidatorInput {
                    name: "validator".to_owned(),
                    validate_request_body: Some(false),
                    validate_request_parameters: Some(false),
                },
            )
            .expect("validator should create");
        let updated_validator = service
            .update_request_validator(
                &scope,
                &api.id,
                &validator.id,
                &[PatchOperation {
                    op: "replace".to_owned(),
                    path: "/validateRequestBody".to_owned(),
                    value: Some("true".to_owned()),
                }],
            )
            .expect("validator should update");
        assert!(updated_validator.validate_request_body);
    }
}
