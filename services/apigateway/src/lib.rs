pub mod authorizers;
pub mod deployments;
pub mod errors;
#[path = "execute_api.rs"]
pub mod execute_api;
pub mod http_api;
pub mod integrations;
pub mod rest_api;
pub mod scope;
pub mod stages;
mod state;
#[path = "v2.rs"]
mod v2;

pub(crate) use state::{
    HTTP_API_AUTHORIZER_ID_WIDTH, HTTP_DEPLOYMENT_ID_WIDTH,
    HTTP_INTEGRATION_ID_WIDTH, HTTP_ROUTE_ID_WIDTH, ROOT_PATH,
    StoredApiGatewayState, StoredApiMethod, StoredApiResource,
    StoredDeployment, StoredDomainName, StoredStage, deployment, domain,
    next_identifier, normalize_http_method, optional_empty_to_none, paginate,
    rest_api, stage, validate_name, validate_stage_name, validate_status_code,
    validate_tags,
};

pub use errors::ApiGatewayError;
pub use execute_api::{
    ExecuteApiError, ExecuteApiIntegrationExecutor, ExecuteApiIntegrationPlan,
    ExecuteApiInvocation, ExecuteApiLambdaProxyPlan,
    ExecuteApiPreparedResponse, ExecuteApiRequest, ResolvedExecuteApiTarget,
    map_lambda_proxy_response,
};
pub use http_api::{
    CreateHttpApiAuthorizerInput, CreateHttpApiDeploymentInput,
    CreateHttpApiInput, CreateHttpApiIntegrationInput,
    CreateHttpApiRouteInput, CreateHttpApiStageInput, HttpApi,
    HttpApiAuthorizer, HttpApiCollection, HttpApiDeployment,
    HttpApiIntegration, HttpApiRoute, HttpApiStage, JwtConfiguration,
    UpdateHttpApiAuthorizerInput, UpdateHttpApiInput,
    UpdateHttpApiIntegrationInput, UpdateHttpApiRouteInput,
    UpdateHttpApiStageInput, map_lambda_proxy_response_v2,
};
pub use rest_api::{
    ApiGatewayService, ApiKey, ApiMethod, ApiResource, Authorizer,
    BasePathMapping, CreateApiKeyInput, CreateAuthorizerInput,
    CreateBasePathMappingInput, CreateDeploymentInput, CreateDomainNameInput,
    CreateRequestValidatorInput, CreateResourceInput, CreateRestApiInput,
    CreateStageInput, CreateUsagePlanInput, CreateUsagePlanKeyInput,
    Deployment, DomainName, EndpointConfiguration, GetTagsOutput, Integration,
    ItemCollection, MethodResponse, PatchOperation, PutIntegrationInput,
    PutMethodInput, PutMethodResponseInput, QuotaSettings, RequestValidator,
    RestApi, Stage, TagResourceInput, ThrottleSettings, UsagePlan,
    UsagePlanApiStage, UsagePlanKey, UsagePlanKeyCollection,
};
pub use scope::ApiGatewayScope;
