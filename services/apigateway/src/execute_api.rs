use aws::{
    AccountId, CallerIdentity, Endpoint, ExecuteApiSourceArn,
    HttpForwardRequest, LambdaFunctionTarget, RegionId,
};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use serde_json::{Value, json};
use std::collections::BTreeMap;

use super::{
    ApiGatewayError, ApiGatewayScope, ApiGatewayService, Integration,
    ROOT_PATH, StoredApiGatewayState, StoredApiMethod, StoredApiResource,
    StoredDeployment, deployment, domain, rest_api, stage,
    validate_status_code,
};

const EXECUTE_API_REQUEST_ID: &str = "0000000000000000";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecuteApiRequest {
    body: Vec<u8>,
    headers: Vec<(String, String)>,
    host: String,
    method: String,
    path: String,
    protocol: Option<String>,
    query_string: Option<String>,
    source_ip: Option<String>,
}

impl ExecuteApiRequest {
    pub fn new(
        host: impl Into<String>,
        method: impl Into<String>,
        path: impl Into<String>,
    ) -> Self {
        Self {
            body: Vec::new(),
            headers: Vec::new(),
            host: host.into(),
            method: method.into(),
            path: path.into(),
            protocol: None,
            query_string: None,
            source_ip: None,
        }
    }

    pub fn with_body(mut self, body: impl Into<Vec<u8>>) -> Self {
        self.body = body.into();
        self
    }

    pub fn with_header(
        mut self,
        name: impl Into<String>,
        value: impl Into<String>,
    ) -> Self {
        self.headers.push((name.into(), value.into()));
        self
    }

    pub fn with_protocol(mut self, protocol: impl Into<String>) -> Self {
        self.protocol = Some(protocol.into());
        self
    }

    pub fn with_query_string(
        mut self,
        query_string: impl Into<String>,
    ) -> Self {
        self.query_string = Some(query_string.into());
        self
    }

    pub fn with_source_ip(mut self, source_ip: impl Into<String>) -> Self {
        self.source_ip = Some(source_ip.into());
        self
    }

    pub fn body(&self) -> &[u8] {
        &self.body
    }

    pub fn headers(&self) -> &[(String, String)] {
        &self.headers
    }

    pub fn host(&self) -> &str {
        &self.host
    }

    pub fn method(&self) -> &str {
        &self.method
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    pub fn protocol(&self) -> Option<&str> {
        self.protocol.as_deref()
    }

    pub fn query_string(&self) -> Option<&str> {
        self.query_string.as_deref()
    }

    pub fn source_ip(&self) -> Option<&str> {
        self.source_ip.as_deref()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecuteApiInvocation {
    api_id: String,
    integration: ExecuteApiIntegrationPlan,
    request_path: String,
    resource_id: String,
    resource_path: String,
    stage_name: String,
}

impl ExecuteApiInvocation {
    pub fn new(
        api_id: String,
        integration: ExecuteApiIntegrationPlan,
        request_path: String,
        resource_id: String,
        resource_path: String,
        stage_name: String,
    ) -> Self {
        Self {
            api_id,
            integration,
            request_path,
            resource_id,
            resource_path,
            stage_name,
        }
    }

    pub fn api_id(&self) -> &str {
        &self.api_id
    }

    pub fn integration(&self) -> &ExecuteApiIntegrationPlan {
        &self.integration
    }

    pub fn request_path(&self) -> &str {
        &self.request_path
    }

    pub fn resource_id(&self) -> &str {
        &self.resource_id
    }

    pub fn resource_path(&self) -> &str {
        &self.resource_path
    }

    pub fn stage_name(&self) -> &str {
        &self.stage_name
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedExecuteApiTarget {
    invocation: ExecuteApiInvocation,
    scope: ApiGatewayScope,
}

impl ResolvedExecuteApiTarget {
    pub fn new(
        scope: ApiGatewayScope,
        invocation: ExecuteApiInvocation,
    ) -> Self {
        Self { invocation, scope }
    }

    pub fn invocation(&self) -> &ExecuteApiInvocation {
        &self.invocation
    }

    pub fn scope(&self) -> &ApiGatewayScope {
        &self.scope
    }

    pub fn into_parts(self) -> (ApiGatewayScope, ExecuteApiInvocation) {
        (self.scope, self.invocation)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecuteApiIntegrationPlan {
    Http(HttpForwardRequest),
    LambdaProxy(Box<ExecuteApiLambdaProxyPlan>),
    Mock(ExecuteApiPreparedResponse),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecuteApiLambdaProxyPlan {
    function_name: String,
    payload: Vec<u8>,
    target: LambdaFunctionTarget,
    response_is_v2: bool,
    source_arn: ExecuteApiSourceArn,
}

impl ExecuteApiLambdaProxyPlan {
    pub fn new(
        target: LambdaFunctionTarget,
        payload: Vec<u8>,
        source_arn: ExecuteApiSourceArn,
        response_is_v2: bool,
    ) -> Self {
        Self {
            function_name: target.to_string(),
            payload,
            target,
            response_is_v2,
            source_arn,
        }
    }

    pub fn function_name(&self) -> &str {
        &self.function_name
    }

    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    pub fn qualifier(&self) -> Option<&str> {
        self.target.qualifier()
    }

    pub fn response_is_v2(&self) -> bool {
        self.response_is_v2
    }

    pub fn source_arn(&self) -> &ExecuteApiSourceArn {
        &self.source_arn
    }

    pub fn target(&self) -> &LambdaFunctionTarget {
        &self.target
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecuteApiPreparedResponse {
    body: Vec<u8>,
    headers: Vec<(String, String)>,
    status_code: u16,
}

impl ExecuteApiPreparedResponse {
    pub fn new(
        status_code: u16,
        headers: Vec<(String, String)>,
        body: Vec<u8>,
    ) -> Self {
        Self { body, headers, status_code }
    }

    pub fn body(&self) -> &[u8] {
        &self.body
    }

    pub fn headers(&self) -> &[(String, String)] {
        &self.headers
    }

    pub fn status_code(&self) -> u16 {
        self.status_code
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecuteApiError {
    BadRequest { message: String },
    Forbidden,
    IntegrationFailure { message: String, status_code: u16 },
    MethodNotAllowed,
    NotFound,
}

pub trait ExecuteApiIntegrationExecutor: Send + Sync {
    /// # Errors
    ///
    /// Returns [`ExecuteApiError`] when the downstream integration cannot be
    /// invoked or its response cannot be mapped into an execute-api response.
    fn execute(
        &self,
        scope: &ApiGatewayScope,
        invocation: &ExecuteApiInvocation,
    ) -> Result<ExecuteApiPreparedResponse, ExecuteApiError>;
}

impl ExecuteApiError {
    pub fn message(&self) -> &str {
        match self {
            Self::BadRequest { message }
            | Self::IntegrationFailure { message, .. } => message,
            Self::Forbidden => "Forbidden",
            Self::MethodNotAllowed => "Method Not Allowed",
            Self::NotFound => "Not Found",
        }
    }

    pub fn status_code(&self) -> u16 {
        match self {
            Self::BadRequest { .. } => 400,
            Self::Forbidden => 403,
            Self::IntegrationFailure { status_code, .. } => *status_code,
            Self::MethodNotAllowed => 405,
            Self::NotFound => 404,
        }
    }
}

impl ApiGatewayService {
    pub fn has_execute_api_host(&self, host: &str) -> bool {
        let host = normalize_authority(host);
        if let Some(default_host) = parse_default_execute_api_host(&host) {
            return self.all_states().into_iter().any(|(_, state)| {
                state.rest_apis.contains_key(&default_host.api_id)
                    || state.http_apis.contains_key(&default_host.api_id)
            });
        }

        self.all_states()
            .into_iter()
            .any(|(_, state)| find_domain_name(&state, &host).is_some())
    }

    pub fn resolve_execute_api(
        &self,
        request: &ExecuteApiRequest,
        caller_identity: Option<&CallerIdentity>,
    ) -> Option<Result<ResolvedExecuteApiTarget, ExecuteApiError>> {
        let host = normalize_authority(request.host());
        if let Some(default_host) = parse_default_execute_api_host(&host) {
            return self.resolve_default_execute_api(
                &default_host,
                request,
                caller_identity,
            );
        }

        self.resolve_custom_domain_execute_api(&host, request, caller_identity)
    }

    pub fn resolve_execute_api_by_api_id(
        &self,
        api_id: &str,
        request: &ExecuteApiRequest,
        caller_identity: Option<&CallerIdentity>,
    ) -> Option<Result<ResolvedExecuteApiTarget, ExecuteApiError>> {
        let mut matches =
            self.all_states().into_iter().filter(|(_, state)| {
                state.rest_apis.contains_key(api_id)
                    || state.http_apis.contains_key(api_id)
            });
        let (scope, state) = matches.next()?;
        if matches.next().is_some() {
            return Some(Err(ambiguous_execute_api_target_error(format!(
                "multiple execute-api targets matched api id {api_id}",
            ))));
        }

        Some(if state.http_apis.contains_key(api_id) {
            super::v2::prepare_http_api_execute_api(
                &scope,
                &state,
                api_id,
                request,
                caller_identity,
            )
            .map(|invocation| ResolvedExecuteApiTarget::new(scope, invocation))
        } else {
            let default_host =
                DefaultExecuteApiHost { api_id: api_id.to_owned() };
            self.prepare_default_host_execute_api(
                &scope,
                &state,
                &default_host,
                request,
                caller_identity,
            )
            .map(|invocation| ResolvedExecuteApiTarget::new(scope, invocation))
        })
    }

    pub fn prepare_execute_api(
        &self,
        scope: &ApiGatewayScope,
        request: &ExecuteApiRequest,
        caller_identity: Option<&CallerIdentity>,
    ) -> Option<Result<ExecuteApiInvocation, ExecuteApiError>> {
        let state = self.load_state(scope);
        let host = normalize_authority(request.host());
        let host = if let Some(default_host) =
            parse_default_execute_api_host(&host)
        {
            ExecuteApiHost::Default(default_host)
        } else if let Some(domain_name) = find_domain_name(&state, &host) {
            ExecuteApiHost::CustomDomain(domain_name.to_owned())
        } else {
            return None;
        };

        Some(match host {
            ExecuteApiHost::Default(host) => self
                .prepare_default_host_execute_api(
                    scope,
                    &state,
                    &host,
                    request,
                    caller_identity,
                ),
            ExecuteApiHost::CustomDomain(domain_name) => self
                .prepare_custom_domain_execute_api(
                    scope,
                    &state,
                    &domain_name,
                    request,
                    caller_identity,
                ),
        })
    }

    fn resolve_default_execute_api(
        &self,
        host: &DefaultExecuteApiHost,
        request: &ExecuteApiRequest,
        caller_identity: Option<&CallerIdentity>,
    ) -> Option<Result<ResolvedExecuteApiTarget, ExecuteApiError>> {
        let mut matches =
            self.all_states().into_iter().filter(|(_, state)| {
                state.rest_apis.contains_key(&host.api_id)
                    || state.http_apis.contains_key(&host.api_id)
            });
        let (scope, state) = matches.next()?;
        if matches.next().is_some() {
            return Some(Err(ambiguous_execute_api_target_error(format!(
                "multiple execute-api targets matched host {}",
                request.host(),
            ))));
        }

        Some(
            self.prepare_default_host_execute_api(
                &scope,
                &state,
                host,
                request,
                caller_identity,
            )
            .map(|invocation| {
                ResolvedExecuteApiTarget::new(scope, invocation)
            }),
        )
    }

    fn resolve_custom_domain_execute_api(
        &self,
        host: &str,
        request: &ExecuteApiRequest,
        caller_identity: Option<&CallerIdentity>,
    ) -> Option<Result<ResolvedExecuteApiTarget, ExecuteApiError>> {
        let mut matches =
            self.all_states().into_iter().filter_map(|(scope, state)| {
                let domain_name =
                    find_domain_name(&state, host).map(str::to_owned);
                domain_name.map(|domain_name| (scope, state, domain_name))
            });
        let (scope, state, domain_name) = matches.next()?;
        if matches.next().is_some() {
            return Some(Err(ambiguous_execute_api_target_error(format!(
                "multiple execute-api custom domains matched host {}",
                request.host(),
            ))));
        }

        Some(
            self.prepare_custom_domain_execute_api(
                &scope,
                &state,
                &domain_name,
                request,
                caller_identity,
            )
            .map(|invocation| {
                ResolvedExecuteApiTarget::new(scope, invocation)
            }),
        )
    }
}

/// # Errors
///
/// Returns [`ExecuteApiError::IntegrationFailure`] when the Lambda proxy
/// payload cannot be decoded as a valid API Gateway proxy response.
pub fn map_lambda_proxy_response(
    payload: &[u8],
) -> Result<ExecuteApiPreparedResponse, ExecuteApiError> {
    let value = serde_json::from_slice::<Value>(payload).map_err(|_| {
        ExecuteApiError::IntegrationFailure {
            message: "Malformed Lambda proxy response.".to_owned(),
            status_code: 502,
        }
    })?;
    let object = value.as_object().ok_or_else(|| {
        ExecuteApiError::IntegrationFailure {
            message: "Malformed Lambda proxy response.".to_owned(),
            status_code: 502,
        }
    })?;
    let status_code = object
        .get("statusCode")
        .and_then(Value::as_u64)
        .and_then(|value| u16::try_from(value).ok())
        .filter(|value| (100..=599).contains(value))
        .ok_or_else(|| ExecuteApiError::IntegrationFailure {
            message: "Malformed Lambda proxy response.".to_owned(),
            status_code: 502,
        })?;
    let mut headers = headers_from_json_map(object.get("headers"))
        .into_iter()
        .collect::<Vec<_>>();
    headers.extend(multi_value_headers_from_json_map(
        object.get("multiValueHeaders"),
    ));
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
                    message: "Malformed Lambda proxy response.".to_owned(),
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

impl ApiGatewayService {
    fn prepare_default_host_execute_api(
        &self,
        scope: &ApiGatewayScope,
        state: &StoredApiGatewayState,
        host: &DefaultExecuteApiHost,
        request: &ExecuteApiRequest,
        caller_identity: Option<&CallerIdentity>,
    ) -> Result<ExecuteApiInvocation, ExecuteApiError> {
        if state.http_apis.contains_key(&host.api_id) {
            return super::v2::prepare_http_api_execute_api(
                scope,
                state,
                &host.api_id,
                request,
                caller_identity,
            );
        }
        let api = rest_api(state, &host.api_id).map_err(not_found_error)?;
        if api.disable_execute_api_endpoint {
            return Err(ExecuteApiError::Forbidden);
        }

        let path = normalize_path(request.path())?;
        let Some((stage_name, resource_path)) =
            default_host_request_target(&path)
        else {
            return Err(ExecuteApiError::NotFound);
        };
        let stage = stage(api, &stage_name).map_err(not_found_error)?;
        let deployment_id =
            stage.deployment_id.as_deref().ok_or(ExecuteApiError::NotFound)?;
        let deployment =
            deployment(api, deployment_id).map_err(not_found_error)?;
        let context = ExecuteApiRequestContext {
            scope,
            api_id: &host.api_id,
            stage,
            request,
            caller_identity,
            host: &normalize_authority(request.host()),
        };

        build_execute_api_invocation(&context, deployment, &resource_path)
    }

    fn prepare_custom_domain_execute_api(
        &self,
        scope: &ApiGatewayScope,
        state: &StoredApiGatewayState,
        domain_name: &str,
        request: &ExecuteApiRequest,
        caller_identity: Option<&CallerIdentity>,
    ) -> Result<ExecuteApiInvocation, ExecuteApiError> {
        let domain = domain(state, domain_name).map_err(not_found_error)?;
        let path = normalize_path(request.path())?;
        let mapping = resolve_base_path_mapping(domain, &path)
            .ok_or(ExecuteApiError::NotFound)?;
        let api =
            rest_api(state, &mapping.rest_api_id).map_err(not_found_error)?;
        let stage = stage(api, &mapping.stage).map_err(not_found_error)?;
        let deployment_id =
            stage.deployment_id.as_deref().ok_or(ExecuteApiError::NotFound)?;
        let deployment =
            deployment(api, deployment_id).map_err(not_found_error)?;
        let resource_path = strip_base_path(&path, &mapping.base_path)?;
        let context = ExecuteApiRequestContext {
            scope,
            api_id: &mapping.rest_api_id,
            stage,
            request,
            caller_identity,
            host: &normalize_authority(request.host()),
        };

        build_execute_api_invocation(&context, deployment, &resource_path)
    }
}

struct ExecuteApiRequestContext<'a> {
    scope: &'a ApiGatewayScope,
    api_id: &'a str,
    stage: &'a super::StoredStage,
    request: &'a ExecuteApiRequest,
    caller_identity: Option<&'a CallerIdentity>,
    host: &'a str,
}

fn build_execute_api_invocation(
    context: &ExecuteApiRequestContext<'_>,
    deployment: &StoredDeployment,
    resource_path: &str,
) -> Result<ExecuteApiInvocation, ExecuteApiError> {
    let match_result = match_resource(deployment, resource_path)?;
    let method =
        resolve_method(match_result.resource, context.request.method())?;
    let integration = method.integration.as_ref().ok_or_else(|| {
        ExecuteApiError::IntegrationFailure {
            message: "No integration configured for API Gateway method."
                .to_owned(),
            status_code: 500,
        }
    })?;
    let integration =
        prepare_integration_plan(context, integration, &match_result)?;

    Ok(ExecuteApiInvocation::new(
        context.api_id.to_owned(),
        integration,
        resource_path.to_owned(),
        match_result.resource.id.clone(),
        match_result.resource.path.clone(),
        context.stage.stage_name.clone(),
    ))
}

fn prepare_integration_plan(
    context: &ExecuteApiRequestContext<'_>,
    integration: &Integration,
    resource: &MatchedResource<'_>,
) -> Result<ExecuteApiIntegrationPlan, ExecuteApiError> {
    match integration.type_.as_str() {
        "AWS_PROXY" => Ok(ExecuteApiIntegrationPlan::LambdaProxy(Box::new(
            build_lambda_proxy_plan(context, integration, resource)?,
        ))),
        "HTTP" | "HTTP_PROXY" => Ok(ExecuteApiIntegrationPlan::Http(
            build_http_forward_request(integration, context.request)?,
        )),
        "MOCK" => Ok(ExecuteApiIntegrationPlan::Mock(build_mock_response(
            integration,
            context.request,
        )?)),
        other => Err(ExecuteApiError::IntegrationFailure {
            message: format!(
                "Unsupported API Gateway integration type {other:?}."
            ),
            status_code: 500,
        }),
    }
}

fn build_lambda_proxy_plan(
    context: &ExecuteApiRequestContext<'_>,
    integration: &Integration,
    resource: &MatchedResource<'_>,
) -> Result<ExecuteApiLambdaProxyPlan, ExecuteApiError> {
    let target = lambda_function_target(integration.uri.as_deref())
        .ok_or_else(|| ExecuteApiError::IntegrationFailure {
            message: "Lambda proxy integration URI is invalid.".to_owned(),
            status_code: 500,
        })?;
    let payload = build_lambda_proxy_event(context, resource)?;
    let source_arn = execute_api_source_arn(
        context.scope.region(),
        context.scope.account_id(),
        context.api_id,
        &context.stage.stage_name,
        context.request.method(),
        &resource.request_path,
    )?;

    Ok(ExecuteApiLambdaProxyPlan::new(target, payload, source_arn, false))
}

fn build_lambda_proxy_event(
    context: &ExecuteApiRequestContext<'_>,
    resource: &MatchedResource<'_>,
) -> Result<Vec<u8>, ExecuteApiError> {
    let multi_value_headers = multi_value_headers(context.request.headers());
    let headers = single_value_headers(&multi_value_headers);
    let multi_value_query_string_parameters =
        multi_value_query_string_parameters(context.request.query_string())?;
    let query_string_parameters = single_value_query_string_parameters(
        &multi_value_query_string_parameters,
    );
    let path_parameters = if resource.path_parameters.is_empty() {
        Value::Null
    } else {
        Value::Object(
            resource
                .path_parameters
                .iter()
                .map(|(name, value)| {
                    (name.clone(), Value::String(value.clone()))
                })
                .collect(),
        )
    };
    let stage_variables = if context.stage.variables.is_empty() {
        Value::Null
    } else {
        Value::Object(
            context
                .stage
                .variables
                .iter()
                .map(|(name, value)| {
                    (name.clone(), Value::String(value.clone()))
                })
                .collect(),
        )
    };
    let (body, is_base64_encoded) = if context.request.body().is_empty() {
        (Value::Null, false)
    } else if let Ok(body) = std::str::from_utf8(context.request.body()) {
        (Value::String(body.to_owned()), false)
    } else {
        (Value::String(BASE64_STANDARD.encode(context.request.body())), true)
    };
    let request_context_path = if resource.request_path == ROOT_PATH {
        format!("/{}", context.stage.stage_name)
    } else {
        format!("/{}{}", context.stage.stage_name, resource.request_path)
    };
    let identity = json!({
        "accessKey": Value::Null,
        "accountId": context
            .caller_identity
            .and_then(|caller_identity| caller_identity.arn().account_id())
            .map(|account_id| account_id.as_str())
            .unwrap_or(context.scope.account_id().as_str()),
        "caller": context
            .caller_identity
            .map(CallerIdentity::principal_id)
            .unwrap_or_default(),
        "cognitoAuthenticationProvider": Value::Null,
        "cognitoAuthenticationType": Value::Null,
        "cognitoIdentityId": Value::Null,
        "cognitoIdentityPoolId": Value::Null,
        "principalOrgId": Value::Null,
        "sourceIp": context.request.source_ip().unwrap_or("127.0.0.1"),
        "user": Value::Null,
        "userAgent": headers.get("user-agent").cloned().unwrap_or(Value::Null),
        "userArn": context
            .caller_identity
            .map(|caller_identity| Value::String(caller_identity.arn().to_string()))
            .unwrap_or(Value::Null),
    });
    let event = json!({
        "resource": resource.resource.path,
        "path": resource.request_path,
        "httpMethod": context.request.method(),
        "headers": Value::Object(headers),
        "multiValueHeaders": if multi_value_headers.is_empty() {
            Value::Null
        } else {
            Value::Object(
                multi_value_headers
                    .iter()
                    .map(|(name, values)| {
                        (
                            name.clone(),
                            Value::Array(
                                values
                                    .iter()
                                    .map(|value| Value::String(value.clone()))
                                    .collect(),
                            ),
                        )
                    })
                    .collect(),
            )
        },
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
        "multiValueQueryStringParameters": if multi_value_query_string_parameters.is_empty() {
            Value::Null
        } else {
            Value::Object(
                multi_value_query_string_parameters
                    .into_iter()
                    .map(|(name, values)| {
                        (
                            name,
                            Value::Array(
                                values
                                    .into_iter()
                                    .map(Value::String)
                                    .collect(),
                            ),
                        )
                    })
                    .collect(),
            )
        },
        "pathParameters": path_parameters,
        "stageVariables": stage_variables,
        "requestContext": {
            "accountId": context.scope.account_id().as_str(),
            "apiId": context.api_id,
            "domainName": context.host,
            "domainPrefix": context.host.split('.').next().unwrap_or_default(),
            "extendedRequestId": EXECUTE_API_REQUEST_ID,
            "httpMethod": context.request.method(),
            "identity": identity,
            "path": request_context_path,
            "protocol": context.request.protocol().unwrap_or("HTTP/1.1"),
            "region": context.scope.region().as_str(),
            "requestId": EXECUTE_API_REQUEST_ID,
            "resourceId": resource.resource.id,
            "resourcePath": resource.resource.path,
            "stage": context.stage.stage_name,
        },
        "body": body,
        "isBase64Encoded": is_base64_encoded,
    });

    serde_json::to_vec(&event).map_err(|error| {
        ExecuteApiError::IntegrationFailure {
            message: error.to_string(),
            status_code: 500,
        }
    })
}

fn build_http_forward_request(
    integration: &Integration,
    request: &ExecuteApiRequest,
) -> Result<HttpForwardRequest, ExecuteApiError> {
    let uri = integration.uri.as_deref().ok_or_else(|| {
        ExecuteApiError::IntegrationFailure {
            message: "HTTP integration URI is invalid.".to_owned(),
            status_code: 500,
        }
    })?;
    let parsed = parse_http_uri(uri)?;
    let path = combine_query_strings(
        &parsed.path,
        parsed.query.as_deref(),
        request.query_string(),
    );
    let method = integration
        .integration_http_method
        .as_deref()
        .or(integration.http_method.as_deref())
        .unwrap_or(request.method());
    let body = select_http_request_body(integration, request);
    let mut forward =
        HttpForwardRequest::new(parsed.endpoint, method, path).with_body(body);
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

fn build_mock_response(
    integration: &Integration,
    request: &ExecuteApiRequest,
) -> Result<ExecuteApiPreparedResponse, ExecuteApiError> {
    let Some(template) = selected_request_template(integration, request)
    else {
        return Ok(ExecuteApiPreparedResponse::new(
            200,
            Vec::new(),
            Vec::new(),
        ));
    };
    let template = serde_json::from_str::<Value>(template).map_err(|_| {
        ExecuteApiError::IntegrationFailure {
            message: "MOCK integration template must be valid JSON."
                .to_owned(),
            status_code: 500,
        }
    })?;
    let object = template.as_object().ok_or_else(|| {
        ExecuteApiError::IntegrationFailure {
            message: "MOCK integration template must be a JSON object."
                .to_owned(),
            status_code: 500,
        }
    })?;
    let status_code = match object.get("statusCode") {
        Some(Value::Number(number)) => number
            .as_u64()
            .and_then(|value| u16::try_from(value).ok())
            .ok_or_else(|| ExecuteApiError::IntegrationFailure {
                message: "MOCK integration statusCode must be a valid HTTP status code.".to_owned(),
                status_code: 500,
            })?,
        Some(Value::String(status_code)) => {
            validate_status_code(status_code).map_err(runtime_error)?;
            status_code.parse::<u16>().map_err(|_| {
                ExecuteApiError::IntegrationFailure {
                    message:
                        "MOCK integration statusCode must be a valid HTTP status code."
                            .to_owned(),
                    status_code: 500,
                }
            })?
        }
        Some(_) => {
            return Err(ExecuteApiError::IntegrationFailure {
                message: "MOCK integration statusCode must be a valid HTTP status code.".to_owned(),
                status_code: 500,
            });
        }
        None => 200,
    };
    let headers = headers_from_json_map(object.get("headers"))
        .into_iter()
        .collect::<Vec<_>>();
    let body = match object.get("body") {
        Some(Value::String(body))
            if object
                .get("isBase64Encoded")
                .and_then(Value::as_bool)
                .unwrap_or(false) =>
        {
            BASE64_STANDARD.decode(body).map_err(|_| {
                ExecuteApiError::IntegrationFailure {
                    message: "MOCK integration body is not valid base64."
                        .to_owned(),
                    status_code: 500,
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

pub(crate) fn lambda_function_target(
    uri: Option<&str>,
) -> Option<LambdaFunctionTarget> {
    let function_name = lambda_function_name(uri)?;

    LambdaFunctionTarget::parse(function_name).ok()
}

fn lambda_function_name(uri: Option<&str>) -> Option<String> {
    let uri = uri?;
    let function = uri.split("/functions/").nth(1)?;
    let function = function.strip_suffix("/invocations")?;

    Some(function.to_owned())
}

fn execute_api_source_arn(
    region: &RegionId,
    account_id: &AccountId,
    api_id: &str,
    stage_name: &str,
    method: &str,
    resource_path: &str,
) -> Result<ExecuteApiSourceArn, ExecuteApiError> {
    let resource = resource_path.trim_start_matches('/');
    let resource = if resource.is_empty() {
        format!("{api_id}/{stage_name}/{}/", method.to_ascii_uppercase())
    } else {
        format!(
            "{api_id}/{stage_name}/{}/{}",
            method.to_ascii_uppercase(),
            resource,
        )
    };

    ExecuteApiSourceArn::from_resource(region, account_id, resource).map_err(
        |error| ExecuteApiError::IntegrationFailure {
            message: error.to_string(),
            status_code: 500,
        },
    )
}

fn not_found_error(_error: ApiGatewayError) -> ExecuteApiError {
    ExecuteApiError::NotFound
}

fn runtime_error(error: ApiGatewayError) -> ExecuteApiError {
    ExecuteApiError::IntegrationFailure {
        message: error.to_string(),
        status_code: 500,
    }
}

fn ambiguous_execute_api_target_error(message: String) -> ExecuteApiError {
    ExecuteApiError::IntegrationFailure { message, status_code: 500 }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DefaultExecuteApiHost {
    api_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ExecuteApiHost {
    CustomDomain(String),
    Default(DefaultExecuteApiHost),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct MatchedResource<'a> {
    path_parameters: BTreeMap<String, String>,
    request_path: String,
    resource: &'a StoredApiResource,
}

fn parse_default_execute_api_host(
    host: &str,
) -> Option<DefaultExecuteApiHost> {
    let (api_id, _) = host.split_once(".execute-api")?;
    if api_id.is_empty() {
        return None;
    }

    Some(DefaultExecuteApiHost { api_id: api_id.to_owned() })
}

fn find_domain_name<'a>(
    state: &'a StoredApiGatewayState,
    host: &str,
) -> Option<&'a str> {
    state
        .domains
        .keys()
        .find(|domain_name| domain_name.eq_ignore_ascii_case(host))
        .map(String::as_str)
}

fn default_host_request_target(path: &str) -> Option<(String, String)> {
    let trimmed = path.trim_start_matches('/');
    if trimmed.is_empty() {
        return None;
    }
    let mut segments = trimmed.splitn(2, '/');
    let stage_name = segments.next()?.to_owned();
    let resource_path = segments
        .next()
        .map(|path| format!("/{path}"))
        .unwrap_or_else(|| ROOT_PATH.to_owned());

    Some((stage_name, resource_path))
}

fn normalize_authority(host: &str) -> String {
    host.trim()
        .rsplit_once(':')
        .filter(|(_, port)| port.parse::<u16>().is_ok())
        .map(|(host, _)| host)
        .unwrap_or(host.trim())
        .to_ascii_lowercase()
}

fn normalize_path(path: &str) -> Result<String, ExecuteApiError> {
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

fn resolve_base_path_mapping<'a>(
    domain: &'a super::StoredDomainName,
    path: &str,
) -> Option<&'a super::BasePathMapping> {
    let mut candidates = domain
        .base_path_mappings
        .values()
        .filter(|mapping| {
            if mapping.base_path == "(none)" {
                return true;
            }
            path == format!("/{}", mapping.base_path)
                || path.starts_with(&format!("/{}/", mapping.base_path))
        })
        .collect::<Vec<_>>();
    candidates.sort_by_key(|mapping| {
        (
            mapping.base_path == "(none)",
            std::cmp::Reverse(mapping.base_path.len()),
        )
    });
    candidates.into_iter().next()
}

fn strip_base_path(
    path: &str,
    base_path: &str,
) -> Result<String, ExecuteApiError> {
    if base_path == "(none)" {
        return Ok(path.to_owned());
    }

    if path == format!("/{base_path}") {
        return Ok(ROOT_PATH.to_owned());
    }

    path.strip_prefix(&format!("/{base_path}"))
        .filter(|path| path.starts_with('/'))
        .map(str::to_owned)
        .ok_or(ExecuteApiError::NotFound)
}

fn match_resource<'a>(
    deployment: &'a StoredDeployment,
    request_path: &str,
) -> Result<MatchedResource<'a>, ExecuteApiError> {
    let segments = request_path
        .trim_start_matches('/')
        .split('/')
        .filter(|segment| !segment.is_empty())
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let root = deployment
        .resources
        .values()
        .find(|resource| resource.parent_id.is_none())
        .ok_or_else(|| ExecuteApiError::IntegrationFailure {
            message: "Deployment is missing its root API Gateway resource."
                .to_owned(),
            status_code: 500,
        })?;

    match_resource_recursive(
        &deployment.resources,
        root,
        &segments,
        BTreeMap::new(),
        request_path,
    )
    .ok_or(ExecuteApiError::NotFound)
}

fn match_resource_recursive<'a>(
    resources: &'a BTreeMap<String, StoredApiResource>,
    current: &'a StoredApiResource,
    segments: &[String],
    path_parameters: BTreeMap<String, String>,
    request_path: &str,
) -> Option<MatchedResource<'a>> {
    if segments.is_empty() {
        return Some(MatchedResource {
            path_parameters,
            request_path: request_path.to_owned(),
            resource: current,
        });
    }
    let (segment, remaining_segments) = segments.split_first()?;

    let children = resources
        .values()
        .filter(|resource| resource.parent_id.as_deref() == Some(&current.id))
        .collect::<Vec<_>>();

    if let Some(exact) = children.iter().copied().find(|resource| {
        resource.path_part.as_deref() == Some(segment.as_str())
    }) {
        return match_resource_recursive(
            resources,
            exact,
            remaining_segments,
            path_parameters,
            request_path,
        );
    }

    if let Some((variable, path_part)) =
        children.iter().copied().find_map(|resource| {
            resource.path_part.as_deref().and_then(|path_part| {
                (path_part.starts_with('{')
                    && path_part.ends_with('}')
                    && !path_part.contains('+'))
                .then_some((resource, path_part))
            })
        })
    {
        let mut next_parameters = path_parameters;
        next_parameters
            .insert(path_parameter_name(path_part), segment.clone());
        return match_resource_recursive(
            resources,
            variable,
            remaining_segments,
            next_parameters,
            request_path,
        );
    }

    if let Some((greedy, path_part)) =
        children.iter().copied().find_map(|resource| {
            resource.path_part.as_deref().and_then(|path_part| {
                (path_part.starts_with('{') && path_part.ends_with("+}"))
                    .then_some((resource, path_part))
            })
        })
    {
        let mut next_parameters = path_parameters;
        next_parameters
            .insert(path_parameter_name(path_part), segments.join("/"));
        return Some(MatchedResource {
            path_parameters: next_parameters,
            request_path: request_path.to_owned(),
            resource: greedy,
        });
    }

    None
}

fn path_parameter_name(path_part: &str) -> String {
    path_part
        .trim_start_matches('{')
        .trim_end_matches('}')
        .trim_end_matches('+')
        .to_owned()
}

fn resolve_method<'a>(
    resource: &'a StoredApiResource,
    method: &str,
) -> Result<&'a StoredApiMethod, ExecuteApiError> {
    let method = method.to_ascii_uppercase();
    resource
        .methods
        .get(&method)
        .or_else(|| resource.methods.get("ANY"))
        .ok_or(ExecuteApiError::MethodNotAllowed)
}

fn selected_request_template<'a>(
    integration: &'a Integration,
    request: &ExecuteApiRequest,
) -> Option<&'a str> {
    let content_type = request
        .headers()
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("content-type"))
        .map(|(_, value)| value.as_str());
    content_type
        .and_then(|content_type| {
            integration.request_templates.get(content_type)
        })
        .map(String::as_str)
        .or_else(|| {
            integration
                .request_templates
                .get("application/json")
                .map(String::as_str)
        })
        .or_else(|| {
            integration.request_templates.values().next().map(String::as_str)
        })
}

fn select_http_request_body(
    integration: &Integration,
    request: &ExecuteApiRequest,
) -> Vec<u8> {
    if integration.type_ == "HTTP"
        && let Some(template) = selected_request_template(integration, request)
    {
        return template.as_bytes().to_vec();
    }

    request.body().to_vec()
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
            message: "HTTP integration URI is invalid.".to_owned(),
            status_code: 500,
        }
    })?;
    if !matches!(scheme, "http" | "https") {
        return Err(ExecuteApiError::IntegrationFailure {
            message: "HTTP integration URI is invalid.".to_owned(),
            status_code: 500,
        });
    }
    let (authority, path_and_query) = rest
        .split_once('/')
        .map(|(authority, path)| (authority, format!("/{path}")))
        .unwrap_or((rest, ROOT_PATH.to_owned()));
    if authority.is_empty() {
        return Err(ExecuteApiError::IntegrationFailure {
            message: "HTTP integration URI is invalid.".to_owned(),
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

fn multi_value_query_string_parameters(
    raw_query: Option<&str>,
) -> Result<BTreeMap<String, Vec<String>>, ExecuteApiError> {
    let mut values = BTreeMap::<String, Vec<String>>::new();
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
        values.entry(name).or_default().push(value);
    }
    Ok(values)
}

fn single_value_query_string_parameters(
    values: &BTreeMap<String, Vec<String>>,
) -> BTreeMap<String, String> {
    values
        .iter()
        .map(|(name, values)| {
            (name.clone(), values.last().cloned().unwrap_or_default())
        })
        .collect()
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

fn multi_value_headers_from_json_map(
    value: Option<&Value>,
) -> Vec<(String, String)> {
    value
        .and_then(Value::as_object)
        .into_iter()
        .flat_map(|headers| headers.iter())
        .flat_map(|(name, value)| {
            value
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|value| value.as_str())
                .map(|value| (name.to_owned(), value.to_owned()))
                .collect::<Vec<_>>()
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        CreateBasePathMappingInput, CreateDeploymentInput,
        CreateDomainNameInput, CreateResourceInput, CreateRestApiInput,
        CreateStageInput, PutIntegrationInput, PutMethodInput,
    };
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};
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
        let root = std::env::temp_dir().join(format!(
            "cloudish-apigw-runtime-{label}-{}-{id}",
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

    fn alternate_scope() -> ApiGatewayScope {
        ApiGatewayScope::new(
            "111111111111".parse().expect("account should parse"),
            "eu-west-2".parse().expect("region should parse"),
        )
    }

    fn deploy_http_api(
        service: &ApiGatewayService,
        integration_type: &str,
        uri: &str,
        disable_default: bool,
    ) -> (String, String, String) {
        deploy_http_api_in_scope(
            service,
            &scope(),
            integration_type,
            uri,
            disable_default,
        )
    }

    fn deploy_http_api_in_scope(
        service: &ApiGatewayService,
        scope: &ApiGatewayScope,
        integration_type: &str,
        uri: &str,
        disable_default: bool,
    ) -> (String, String, String) {
        let api = service
            .create_rest_api(
                scope,
                CreateRestApiInput {
                    binary_media_types: Vec::new(),
                    description: None,
                    disable_execute_api_endpoint: Some(disable_default),
                    endpoint_configuration: None,
                    name: "demo".to_owned(),
                    tags: BTreeMap::new(),
                },
            )
            .expect("api should create");
        let resource = service
            .create_resource(
                scope,
                &api.id,
                &api.root_resource_id,
                CreateResourceInput { path_part: "pets".to_owned() },
            )
            .expect("resource should create");
        service
            .put_method(
                scope,
                &api.id,
                &resource.id,
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
            .expect("method should create");
        service
            .put_integration(
                scope,
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
                    integration_http_method: Some("GET".to_owned()),
                    passthrough_behavior: None,
                    request_parameters: BTreeMap::new(),
                    request_templates: BTreeMap::new(),
                    timeout_in_millis: None,
                    type_: integration_type.to_owned(),
                    uri: Some(uri.to_owned()),
                },
            )
            .expect("integration should create");
        let deployment = service
            .create_deployment(
                scope,
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
                scope,
                &api.id,
                CreateStageInput {
                    cache_cluster_enabled: None,
                    cache_cluster_size: None,
                    deployment_id: deployment.id.clone(),
                    description: None,
                    stage_name: "dev".to_owned(),
                    tags: BTreeMap::new(),
                    variables: BTreeMap::from([(
                        "alias".to_owned(),
                        "live".to_owned(),
                    )]),
                },
            )
            .expect("stage should create");

        (api.id, resource.id, deployment.id)
    }

    #[test]
    fn apigw_v1_runtime_default_host_resolves_http_proxy_plan() {
        let service = service("default-host");
        let scope = scope();
        let (api_id, _, _) = deploy_http_api(
            &service,
            "HTTP_PROXY",
            "http://127.0.0.1:8080/backend",
            false,
        );
        let request = ExecuteApiRequest::new(
            format!("{api_id}.execute-api.localhost"),
            "GET",
            "/dev/pets",
        )
        .with_query_string("mode=test")
        .with_header("Accept", "application/json");

        let invocation = service
            .prepare_execute_api(&scope, &request, None)
            .expect("execute-api host should be recognized")
            .expect("invocation should resolve");

        assert_eq!(invocation.api_id(), api_id);
        assert_eq!(invocation.resource_path(), "/pets");
        assert_eq!(invocation.request_path(), "/pets");
        match invocation.integration() {
            ExecuteApiIntegrationPlan::Http(request) => {
                assert_eq!(request.method(), "GET");
                assert_eq!(request.endpoint().host(), "127.0.0.1");
                assert_eq!(request.endpoint().port(), 8080);
                assert_eq!(request.path(), "/backend?mode=test");
            }
            other => panic!("expected HTTP integration plan, got {other:?}"),
        }
    }

    #[test]
    fn apigw_runtime_execute_api_ids_are_unique_across_accounts() {
        let service = service("global-api-ids");

        let default_api = service
            .create_rest_api(
                &scope(),
                CreateRestApiInput {
                    binary_media_types: Vec::new(),
                    description: None,
                    disable_execute_api_endpoint: None,
                    endpoint_configuration: None,
                    name: "default".to_owned(),
                    tags: BTreeMap::new(),
                },
            )
            .expect("default-scope API should create");
        let alternate_api = service
            .create_rest_api(
                &alternate_scope(),
                CreateRestApiInput {
                    binary_media_types: Vec::new(),
                    description: None,
                    disable_execute_api_endpoint: None,
                    endpoint_configuration: None,
                    name: "alternate".to_owned(),
                    tags: BTreeMap::new(),
                },
            )
            .expect("alternate-scope API should create");

        assert_ne!(default_api.id, alternate_api.id);
    }

    #[test]
    fn apigw_runtime_resolve_execute_api_returns_the_owning_scope() {
        let service = service("cross-scope-default-host");
        let alternate_scope = alternate_scope();
        let (api_id, _, _) = deploy_http_api_in_scope(
            &service,
            &alternate_scope,
            "HTTP_PROXY",
            "http://127.0.0.1:8080/backend",
            false,
        );

        let resolved = service
            .resolve_execute_api(
                &ExecuteApiRequest::new(
                    format!("{api_id}.execute-api.localhost"),
                    "GET",
                    "/dev/pets",
                ),
                None,
            )
            .expect("execute-api host should be recognized")
            .expect("alternate-scope invocation should resolve");

        assert_eq!(resolved.scope(), &alternate_scope);
        assert_eq!(resolved.invocation().api_id(), api_id);
    }

    #[test]
    fn apigw_v1_runtime_custom_domain_longest_base_path_wins() {
        let service = service("custom-domain");
        let scope = scope();
        let (api_id, _, _) = deploy_http_api(
            &service,
            "HTTP",
            "http://127.0.0.1:8080/api",
            false,
        );
        service
            .create_domain_name(
                &scope,
                CreateDomainNameInput {
                    certificate_arn: None,
                    certificate_name: None,
                    domain_name: "api.example.test".to_owned(),
                    endpoint_configuration: None,
                    security_policy: None,
                    tags: BTreeMap::new(),
                },
            )
            .expect("domain should create");
        service
            .create_base_path_mapping(
                &scope,
                "api.example.test",
                CreateBasePathMappingInput {
                    base_path: Some("v1".to_owned()),
                    rest_api_id: api_id.clone(),
                    stage: "dev".to_owned(),
                },
            )
            .expect("base path mapping should create");
        service
            .create_base_path_mapping(
                &scope,
                "api.example.test",
                CreateBasePathMappingInput {
                    base_path: Some("(none)".to_owned()),
                    rest_api_id: api_id.clone(),
                    stage: "dev".to_owned(),
                },
            )
            .expect("default mapping should create");
        let request =
            ExecuteApiRequest::new("api.example.test", "GET", "/v1/pets");

        let invocation = service
            .prepare_execute_api(&scope, &request, None)
            .expect("domain host should be recognized")
            .expect("invocation should resolve");
        assert_eq!(invocation.api_id(), api_id);
        assert_eq!(invocation.request_path(), "/pets");
    }

    #[test]
    fn apigw_runtime_rejects_duplicate_custom_domains_across_accounts() {
        let service = service("duplicate-custom-domain");

        service
            .create_domain_name(
                &scope(),
                CreateDomainNameInput {
                    certificate_arn: None,
                    certificate_name: None,
                    domain_name: "api.example.test".to_owned(),
                    endpoint_configuration: None,
                    security_policy: None,
                    tags: BTreeMap::new(),
                },
            )
            .expect("default-scope custom domain should create");
        let error = service
            .create_domain_name(
                &alternate_scope(),
                CreateDomainNameInput {
                    certificate_arn: None,
                    certificate_name: None,
                    domain_name: "API.example.test".to_owned(),
                    endpoint_configuration: None,
                    security_policy: None,
                    tags: BTreeMap::new(),
                },
            )
            .expect_err("custom domain should be globally unique");

        assert!(matches!(error, ApiGatewayError::Conflict { .. }));
    }

    #[test]
    fn apigw_v1_runtime_exact_resource_wins_before_variable_and_any_falls_back()
     {
        let service = service("resource-match");
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
            .expect("api should create");
        let root = &api.root_resource_id;
        let pets = service
            .create_resource(
                &scope,
                &api.id,
                root,
                CreateResourceInput { path_part: "pets".to_owned() },
            )
            .expect("static resource should create");
        let pet_id = service
            .create_resource(
                &scope,
                &api.id,
                &pets.id,
                CreateResourceInput { path_part: "{id}".to_owned() },
            )
            .expect("variable resource should create");
        let list = service
            .create_resource(
                &scope,
                &api.id,
                &pets.id,
                CreateResourceInput { path_part: "list".to_owned() },
            )
            .expect("exact resource should create");
        service
            .put_method(
                &scope,
                &api.id,
                &pet_id.id,
                "ANY",
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
            .expect("variable ANY should create");
        service
            .put_method(
                &scope,
                &api.id,
                &list.id,
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
            .expect("exact GET should create");
        service
            .put_integration(
                &scope,
                &api.id,
                &pet_id.id,
                "ANY",
                PutIntegrationInput {
                    cache_key_parameters: Vec::new(),
                    cache_namespace: None,
                    connection_id: None,
                    connection_type: None,
                    content_handling: None,
                    credentials: None,
                    http_method: Some("GET".to_owned()),
                    integration_http_method: Some("GET".to_owned()),
                    passthrough_behavior: None,
                    request_parameters: BTreeMap::new(),
                    request_templates: BTreeMap::new(),
                    timeout_in_millis: None,
                    type_: "MOCK".to_owned(),
                    uri: None,
                },
            )
            .expect("variable integration should create");
        service
            .put_integration(
                &scope,
                &api.id,
                &list.id,
                "GET",
                PutIntegrationInput {
                    cache_key_parameters: Vec::new(),
                    cache_namespace: None,
                    connection_id: None,
                    connection_type: None,
                    content_handling: None,
                    credentials: None,
                    http_method: Some("GET".to_owned()),
                    integration_http_method: Some("GET".to_owned()),
                    passthrough_behavior: None,
                    request_parameters: BTreeMap::new(),
                    request_templates: BTreeMap::from([(
                        "application/json".to_owned(),
                        r#"{"statusCode":200,"body":"exact"}"#.to_owned(),
                    )]),
                    timeout_in_millis: None,
                    type_: "MOCK".to_owned(),
                    uri: None,
                },
            )
            .expect("exact integration should create");
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

        let exact = service
            .prepare_execute_api(
                &scope,
                &ExecuteApiRequest::new(
                    format!("{}.execute-api.localhost", api.id),
                    "GET",
                    "/dev/pets/list",
                ),
                None,
            )
            .expect("host should resolve")
            .expect("exact resource should resolve");
        assert_eq!(exact.resource_path(), "/pets/list");

        let fallback = service
            .prepare_execute_api(
                &scope,
                &ExecuteApiRequest::new(
                    format!("{}.execute-api.localhost", api.id),
                    "PATCH",
                    "/dev/pets/123",
                ),
                None,
            )
            .expect("host should resolve")
            .expect("ANY resource should resolve");
        assert_eq!(fallback.resource_path(), "/pets/{id}");
    }

    #[test]
    fn apigw_v1_runtime_disabled_default_endpoint_and_missing_method_are_explicit()
     {
        let disabled_service = service("negative-paths");
        let disabled_scope = scope();
        let (api_id, _, _) = deploy_http_api(
            &disabled_service,
            "HTTP_PROXY",
            "http://127.0.0.1:8080/backend",
            true,
        );
        let disabled = disabled_service
            .prepare_execute_api(
                &disabled_scope,
                &ExecuteApiRequest::new(
                    format!("{api_id}.execute-api.localhost"),
                    "GET",
                    "/dev/pets",
                ),
                None,
            )
            .expect("execute-api host should be recognized")
            .expect_err("disabled endpoint should fail");
        assert_eq!(disabled, ExecuteApiError::Forbidden);

        let missing_method_service = service("missing-method");
        let missing_method_scope = scope();
        let (api_id, _, _) = deploy_http_api(
            &missing_method_service,
            "HTTP_PROXY",
            "http://127.0.0.1:8080/backend",
            false,
        );
        let missing_method = missing_method_service
            .prepare_execute_api(
                &missing_method_scope,
                &ExecuteApiRequest::new(
                    format!("{api_id}.execute-api.localhost"),
                    "POST",
                    "/dev/pets",
                ),
                None,
            )
            .expect("execute-api host should be recognized")
            .expect_err("missing method should fail");
        assert_eq!(missing_method, ExecuteApiError::MethodNotAllowed);
    }

    #[test]
    fn apigw_v1_runtime_unsupported_integration_family_is_explicit() {
        let service = service("unsupported-integration");
        let scope = scope();
        let (api_id, resource_id, deployment_id) = deploy_http_api(
            &service,
            "HTTP_PROXY",
            "http://127.0.0.1:8080/backend",
            false,
        );
        let mut state = service.load_state(&scope);
        let api =
            state.rest_apis.get_mut(&api_id).expect("api should be stored");
        let deployment = api
            .deployments
            .get_mut(&deployment_id)
            .expect("deployment should be stored");
        deployment
            .resources
            .get_mut(&resource_id)
            .expect("resource should be stored")
            .methods
            .get_mut("GET")
            .expect("method should be stored")
            .integration
            .as_mut()
            .expect("integration should be stored")
            .type_ = "AWS".to_owned();
        service
            .save_state(&scope, state)
            .expect("state update should persist");

        let error = service
            .prepare_execute_api(
                &scope,
                &ExecuteApiRequest::new(
                    format!("{api_id}.execute-api.localhost"),
                    "GET",
                    "/dev/pets",
                ),
                None,
            )
            .expect("execute-api host should be recognized")
            .expect_err("unsupported integration should fail");

        assert_eq!(
            error,
            ExecuteApiError::IntegrationFailure {
                message: "Unsupported API Gateway integration type \"AWS\"."
                    .to_owned(),
                status_code: 500,
            }
        );
    }

    #[test]
    fn apigw_v1_runtime_lambda_proxy_response_parser_decodes_headers_and_base64_body()
     {
        let parsed = map_lambda_proxy_response(
            br#"{"statusCode":201,"headers":{"x-one":"1"},"multiValueHeaders":{"set-cookie":["a=1","b=2"]},"body":"aGVsbG8=","isBase64Encoded":true}"#,
        )
        .expect("lambda proxy response should parse");

        assert_eq!(parsed.status_code(), 201);
        assert_eq!(parsed.body(), b"hello");
        assert!(
            parsed.headers().contains(&("x-one".to_owned(), "1".to_owned(),))
        );
        assert!(
            parsed
                .headers()
                .contains(&("set-cookie".to_owned(), "a=1".to_owned(),))
        );
        assert!(
            parsed
                .headers()
                .contains(&("set-cookie".to_owned(), "b=2".to_owned(),))
        );
    }
}
