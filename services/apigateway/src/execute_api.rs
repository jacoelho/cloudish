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
    StoredDeployment, deployment, execute_api_request_validator,
    is_valid_execute_api_key, rest_api, stage, validate_status_code,
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
    /// # Errors
    ///
    /// Returns [`ExecuteApiError`] when the API id is missing, ambiguous, or
    /// the resolved API cannot prepare an invocation for the request.
    pub fn resolve_execute_api_by_api_id(
        &self,
        api_id: &str,
        request: &ExecuteApiRequest,
        caller_identity: Option<&CallerIdentity>,
    ) -> Result<ResolvedExecuteApiTarget, ExecuteApiError> {
        let mut matches =
            self.all_states().into_iter().filter(|(_, state)| {
                state.rest_apis.contains_key(api_id)
                    || state.http_apis.contains_key(api_id)
            });
        let Some((scope, state)) = matches.next() else {
            return Err(ExecuteApiError::NotFound);
        };
        if matches.next().is_some() {
            return Err(ambiguous_execute_api_target_error(format!(
                "multiple execute-api targets matched api id {api_id}",
            )));
        }

        self.prepare_api_id_execute_api(
            &scope,
            &state,
            api_id,
            request,
            caller_identity,
        )
        .map(|invocation| ResolvedExecuteApiTarget::new(scope, invocation))
    }

    fn prepare_api_id_execute_api(
        &self,
        scope: &ApiGatewayScope,
        state: &StoredApiGatewayState,
        api_id: &str,
        request: &ExecuteApiRequest,
        caller_identity: Option<&CallerIdentity>,
    ) -> Result<ExecuteApiInvocation, ExecuteApiError> {
        if state.http_apis.contains_key(api_id) {
            return super::v2::prepare_http_api_execute_api(
                scope,
                state,
                api_id,
                request,
                caller_identity,
            );
        }

        let api = rest_api(state, api_id).map_err(not_found_error)?;
        if api.disable_execute_api_endpoint {
            return Err(ExecuteApiError::Forbidden);
        }

        let path = normalize_path(request.path())?;
        let Some((stage_name, resource_path)) = api_id_request_target(&path)
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
            state,
            api_id,
            stage,
            request,
            caller_identity,
            host: &normalize_authority(request.host()),
        };

        build_execute_api_invocation(&context, api, deployment, &resource_path)
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

struct ExecuteApiRequestContext<'a> {
    scope: &'a ApiGatewayScope,
    state: &'a StoredApiGatewayState,
    api_id: &'a str,
    stage: &'a super::StoredStage,
    request: &'a ExecuteApiRequest,
    caller_identity: Option<&'a CallerIdentity>,
    host: &'a str,
}

fn build_execute_api_invocation(
    context: &ExecuteApiRequestContext<'_>,
    api: &super::StoredRestApi,
    deployment: &StoredDeployment,
    resource_path: &str,
) -> Result<ExecuteApiInvocation, ExecuteApiError> {
    let match_result = match_resource(deployment, resource_path)?;
    let method =
        resolve_method(match_result.resource, context.request.method())?;
    validate_execute_api_method(context, api, method, &match_result)?;
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

fn validate_execute_api_method(
    context: &ExecuteApiRequestContext<'_>,
    api: &super::StoredRestApi,
    method: &StoredApiMethod,
    resource: &MatchedResource<'_>,
) -> Result<(), ExecuteApiError> {
    validate_execute_api_authorization(method)?;
    validate_execute_api_key(context, api, method)?;
    validate_execute_api_request(context, api, method, resource)?;

    Ok(())
}

fn validate_execute_api_authorization(
    method: &StoredApiMethod,
) -> Result<(), ExecuteApiError> {
    if method.authorization_type == "NONE" {
        return Ok(());
    }

    Err(ExecuteApiError::IntegrationFailure {
        message: format!(
            "Unsupported API Gateway authorization type {:?}.",
            method.authorization_type
        ),
        status_code: 501,
    })
}

fn validate_execute_api_key(
    context: &ExecuteApiRequestContext<'_>,
    api: &super::StoredRestApi,
    method: &StoredApiMethod,
) -> Result<(), ExecuteApiError> {
    if !method.api_key_required || api.api_key_source != "HEADER" {
        return Ok(());
    }

    let key_value = request_header(context.request, "x-api-key")
        .ok_or(ExecuteApiError::Forbidden)?;
    if is_valid_execute_api_key(
        context.state,
        context.api_id,
        &context.stage.stage_name,
        key_value,
    ) {
        Ok(())
    } else {
        Err(ExecuteApiError::Forbidden)
    }
}

fn validate_execute_api_request(
    context: &ExecuteApiRequestContext<'_>,
    api: &super::StoredRestApi,
    method: &StoredApiMethod,
    resource: &MatchedResource<'_>,
) -> Result<(), ExecuteApiError> {
    let Some(validator) = execute_api_request_validator(
        api,
        &resource.resource.id,
        context.request.method(),
    ) else {
        return Ok(());
    };

    if validator.validate_request_parameters
        && required_request_parameter_missing(
            context.request,
            resource,
            method,
        )?
    {
        return Err(invalid_execute_api_request_error());
    }
    if validator.validate_request_body && context.request.body().is_empty() {
        return Err(invalid_execute_api_request_error());
    }

    Ok(())
}

fn invalid_execute_api_request_error() -> ExecuteApiError {
    ExecuteApiError::BadRequest { message: "Invalid request input".to_owned() }
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
            build_http_forward_request(context, integration, resource)?,
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
    context: &ExecuteApiRequestContext<'_>,
    integration: &Integration,
    resource: &MatchedResource<'_>,
) -> Result<HttpForwardRequest, ExecuteApiError> {
    let uri = integration.uri.as_deref().ok_or_else(|| {
        ExecuteApiError::IntegrationFailure {
            message: "HTTP integration URI is invalid.".to_owned(),
            status_code: 500,
        }
    })?;
    let parsed = parse_http_uri(uri)?;
    let request_query_parameters =
        multi_value_query_string_parameters(context.request.query_string())?;
    let request_query_parameters =
        single_value_query_string_parameters(&request_query_parameters);
    let mapped_parameters = mapped_http_request_parameters(
        integration,
        context.request,
        resource,
        &request_query_parameters,
    )?;
    let path = render_mapped_http_path(
        &parsed.path,
        &mapped_parameters.path_parameters,
    )?;
    let path = render_http_query_string(
        &path,
        parsed.query.as_deref(),
        context.request.query_string(),
        &mapped_parameters.query_parameters,
    )?;
    let method = integration
        .integration_http_method
        .as_deref()
        .or(integration.http_method.as_deref())
        .unwrap_or(context.request.method());
    let body = select_http_request_body(integration, context.request);
    let mut forward =
        HttpForwardRequest::new(parsed.endpoint, method, path).with_body(body);
    for (name, value) in render_http_headers(
        context.request,
        &mapped_parameters.header_parameters,
    ) {
        forward = forward.with_header(name.clone(), value.clone());
    }

    Ok(forward)
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct MappedHttpRequestParameters {
    header_parameters: BTreeMap<String, String>,
    path_parameters: BTreeMap<String, String>,
    query_parameters: BTreeMap<String, String>,
}

fn mapped_http_request_parameters(
    integration: &Integration,
    request: &ExecuteApiRequest,
    resource: &MatchedResource<'_>,
    request_query_parameters: &BTreeMap<String, String>,
) -> Result<MappedHttpRequestParameters, ExecuteApiError> {
    let mut mapped = MappedHttpRequestParameters::default();

    for (target, source) in &integration.request_parameters {
        let source_value = resolve_http_request_parameter_source(
            source,
            request,
            resource,
            request_query_parameters,
        )?;
        match parse_http_request_parameter_target(target)? {
            HttpRequestParameterTarget::Header(name) => {
                if let Some(value) = source_value {
                    mapped.header_parameters.insert(name.to_owned(), value);
                }
            }
            HttpRequestParameterTarget::Path(name) => {
                if let Some(value) = source_value {
                    mapped.path_parameters.insert(name.to_owned(), value);
                }
            }
            HttpRequestParameterTarget::QueryString(name) => {
                if let Some(value) = source_value {
                    mapped.query_parameters.insert(name.to_owned(), value);
                }
            }
        }
    }

    Ok(mapped)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HttpRequestParameterTarget<'a> {
    Header(&'a str),
    Path(&'a str),
    QueryString(&'a str),
}

fn parse_http_request_parameter_target(
    target: &str,
) -> Result<HttpRequestParameterTarget<'_>, ExecuteApiError> {
    if let Some(name) = target.strip_prefix("integration.request.header.") {
        return non_empty_mapping_name(name)
            .map(HttpRequestParameterTarget::Header);
    }
    if let Some(name) = target.strip_prefix("integration.request.path.") {
        return non_empty_mapping_name(name)
            .map(HttpRequestParameterTarget::Path);
    }
    if let Some(name) = target.strip_prefix("integration.request.querystring.")
    {
        return non_empty_mapping_name(name)
            .map(HttpRequestParameterTarget::QueryString);
    }

    Err(integration_mapping_error(format!(
        "Unsupported API Gateway request parameter target {target:?}."
    )))
}

fn resolve_http_request_parameter_source(
    source: &str,
    request: &ExecuteApiRequest,
    resource: &MatchedResource<'_>,
    request_query_parameters: &BTreeMap<String, String>,
) -> Result<Option<String>, ExecuteApiError> {
    if let Some(name) = source.strip_prefix("method.request.header.") {
        return Ok(request_header(request, non_empty_mapping_name(name)?)
            .map(str::to_owned));
    }
    if let Some(name) = source.strip_prefix("method.request.path.") {
        return Ok(resource
            .path_parameters
            .get(non_empty_mapping_name(name)?)
            .cloned());
    }
    if let Some(name) = source.strip_prefix("method.request.querystring.") {
        return Ok(request_query_parameters
            .get(non_empty_mapping_name(name)?)
            .cloned());
    }

    Err(integration_mapping_error(format!(
        "Unsupported API Gateway request parameter mapping {source:?}."
    )))
}

fn non_empty_mapping_name(name: &str) -> Result<&str, ExecuteApiError> {
    if name.is_empty() {
        Err(integration_mapping_error(
            "API Gateway request parameter mappings must name a source or target."
                .to_owned(),
        ))
    } else {
        Ok(name)
    }
}

fn render_mapped_http_path(
    path: &str,
    mapped_path_parameters: &BTreeMap<String, String>,
) -> Result<String, ExecuteApiError> {
    let mut rendered_path = path.to_owned();

    for (name, value) in mapped_path_parameters {
        let placeholder = format!("{{{name}}}");
        if !rendered_path.contains(&placeholder) {
            return Err(integration_mapping_error(format!(
                "HTTP integration URI does not declare path parameter {name:?}."
            )));
        }
        rendered_path =
            rendered_path.replace(&placeholder, &urlencoding::encode(value));
    }
    if rendered_path.contains('{') || rendered_path.contains('}') {
        return Err(integration_mapping_error(
            "HTTP integration URI references missing path variables."
                .to_owned(),
        ));
    }

    Ok(rendered_path)
}

fn render_http_query_string(
    path: &str,
    template_query: Option<&str>,
    request_query: Option<&str>,
    mapped_query_parameters: &BTreeMap<String, String>,
) -> Result<String, ExecuteApiError> {
    let mut parameters = single_value_query_string_parameters(
        &multi_value_query_string_parameters(template_query)?,
    );
    parameters.extend(single_value_query_string_parameters(
        &multi_value_query_string_parameters(request_query)?,
    ));
    parameters.extend(mapped_query_parameters.clone());
    if parameters.is_empty() {
        return Ok(path.to_owned());
    }

    let query = parameters
        .into_iter()
        .map(|(name, value)| {
            format!(
                "{}={}",
                urlencoding::encode(&name),
                urlencoding::encode(&value)
            )
        })
        .collect::<Vec<_>>()
        .join("&");

    Ok(format!("{path}?{query}"))
}

fn render_http_headers(
    request: &ExecuteApiRequest,
    mapped_header_parameters: &BTreeMap<String, String>,
) -> Vec<(String, String)> {
    let mut headers = request
        .headers()
        .iter()
        .filter(|(name, _)| {
            !name.eq_ignore_ascii_case("host")
                && !name.eq_ignore_ascii_case("content-length")
                && !name.eq_ignore_ascii_case("connection")
                && !mapped_header_parameters
                    .keys()
                    .any(|target| target.eq_ignore_ascii_case(name))
        })
        .cloned()
        .collect::<Vec<_>>();
    headers.extend(
        mapped_header_parameters
            .iter()
            .map(|(name, value)| (name.clone(), value.clone())),
    );
    headers
}

fn integration_mapping_error(message: String) -> ExecuteApiError {
    ExecuteApiError::IntegrationFailure { message, status_code: 500 }
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
struct MatchedResource<'a> {
    path_parameters: BTreeMap<String, String>,
    request_path: String,
    resource: &'a StoredApiResource,
}

fn api_id_request_target(path: &str) -> Option<(String, String)> {
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

fn request_header<'a>(
    request: &'a ExecuteApiRequest,
    name: &str,
) -> Option<&'a str> {
    request
        .headers()
        .iter()
        .rev()
        .find(|(header_name, _)| header_name.eq_ignore_ascii_case(name))
        .map(|(_, value)| value.as_str())
}

fn required_request_parameter_missing(
    request: &ExecuteApiRequest,
    resource: &MatchedResource<'_>,
    method: &StoredApiMethod,
) -> Result<bool, ExecuteApiError> {
    let query_parameters = single_value_query_string_parameters(
        &multi_value_query_string_parameters(request.query_string())?,
    );

    for (name, required) in &method.request_parameters {
        if !required {
            continue;
        }

        let present = if let Some(header_name) =
            name.strip_prefix("method.request.header.")
        {
            request_header(request, header_name).is_some()
        } else if let Some(parameter_name) =
            name.strip_prefix("method.request.querystring.")
        {
            query_parameters.contains_key(parameter_name)
        } else if let Some(parameter_name) =
            name.strip_prefix("method.request.path.")
        {
            resource.path_parameters.contains_key(parameter_name)
        } else {
            false
        };
        if !present {
            return Ok(true);
        }
    }

    Ok(false)
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
        CreateDeploymentInput, CreateResourceInput, CreateRestApiInput,
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
    fn apigw_v1_runtime_api_id_route_resolves_http_proxy_plan() {
        let service = service("api-id-route");
        let (api_id, _, _) = deploy_http_api(
            &service,
            "HTTP_PROXY",
            "http://127.0.0.1:8080/backend",
            false,
        );
        let request = ExecuteApiRequest::new("localhost", "GET", "/dev/pets")
            .with_query_string("mode=test")
            .with_header("Accept", "application/json");

        let resolved = service
            .resolve_execute_api_by_api_id(&api_id, &request, None)
            .expect("invocation should resolve");
        let invocation = resolved.invocation();

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
        let service = service("cross-scope-api-id");
        let alternate_scope = alternate_scope();
        let (api_id, _, _) = deploy_http_api_in_scope(
            &service,
            &alternate_scope,
            "HTTP_PROXY",
            "http://127.0.0.1:8080/backend",
            false,
        );

        let resolved = service
            .resolve_execute_api_by_api_id(
                &api_id,
                &ExecuteApiRequest::new("localhost", "GET", "/dev/pets"),
                None,
            )
            .expect("alternate-scope invocation should resolve");

        assert_eq!(resolved.scope(), &alternate_scope);
        assert_eq!(resolved.invocation().api_id(), api_id);
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
            .resolve_execute_api_by_api_id(
                &api.id,
                &ExecuteApiRequest::new("localhost", "GET", "/dev/pets/list"),
                None,
            )
            .expect("exact resource should resolve");
        assert_eq!(exact.invocation().resource_path(), "/pets/list");

        let fallback = service
            .resolve_execute_api_by_api_id(
                &api.id,
                &ExecuteApiRequest::new("localhost", "PATCH", "/dev/pets/123"),
                None,
            )
            .expect("ANY resource should resolve");
        assert_eq!(fallback.invocation().resource_path(), "/pets/{id}");
    }

    #[test]
    fn apigw_v1_runtime_disabled_default_endpoint_and_missing_method_are_explicit()
     {
        let disabled_service = service("negative-paths");
        let (api_id, _, _) = deploy_http_api(
            &disabled_service,
            "HTTP_PROXY",
            "http://127.0.0.1:8080/backend",
            true,
        );
        let disabled = disabled_service
            .resolve_execute_api_by_api_id(
                &api_id,
                &ExecuteApiRequest::new("localhost", "GET", "/dev/pets"),
                None,
            )
            .expect_err("disabled endpoint should fail");
        assert_eq!(disabled, ExecuteApiError::Forbidden);

        let missing_method_service = service("missing-method");
        let (api_id, _, _) = deploy_http_api(
            &missing_method_service,
            "HTTP_PROXY",
            "http://127.0.0.1:8080/backend",
            false,
        );
        let missing_method = missing_method_service
            .resolve_execute_api_by_api_id(
                &api_id,
                &ExecuteApiRequest::new("localhost", "POST", "/dev/pets"),
                None,
            )
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
            .resolve_execute_api_by_api_id(
                &api_id,
                &ExecuteApiRequest::new("localhost", "GET", "/dev/pets"),
                None,
            )
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
    fn apigw_v1_runtime_unsupported_rest_authorization_fails_closed() {
        let service = service("unsupported-auth");
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
        let resource = service
            .create_resource(
                &scope,
                &api.id,
                &api.root_resource_id,
                CreateResourceInput { path_part: "pets".to_owned() },
            )
            .expect("resource should create");
        service
            .put_method(
                &scope,
                &api.id,
                &resource.id,
                "GET",
                PutMethodInput {
                    api_key_required: Some(false),
                    authorizer_id: None,
                    authorization_type: "CUSTOM".to_owned(),
                    operation_name: None,
                    request_models: BTreeMap::new(),
                    request_parameters: BTreeMap::new(),
                    request_validator_id: None,
                },
            )
            .expect("method should create");
        service
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
                    integration_http_method: Some("GET".to_owned()),
                    passthrough_behavior: None,
                    request_parameters: BTreeMap::new(),
                    request_templates: BTreeMap::new(),
                    timeout_in_millis: None,
                    type_: "MOCK".to_owned(),
                    uri: None,
                },
            )
            .expect("integration should create");
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

        let error = service
            .resolve_execute_api_by_api_id(
                &api.id,
                &ExecuteApiRequest::new("localhost", "GET", "/dev/pets"),
                None,
            )
            .expect_err("unsupported auth should fail");

        assert_eq!(
            error,
            ExecuteApiError::IntegrationFailure {
                message:
                    "Unsupported API Gateway authorization type \"CUSTOM\"."
                        .to_owned(),
                status_code: 501,
            }
        );
    }

    #[test]
    fn apigw_v1_runtime_http_request_parameter_mappings_are_applied() {
        let service = service("http-request-mappings");
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
        let pets = service
            .create_resource(
                &scope,
                &api.id,
                &api.root_resource_id,
                CreateResourceInput { path_part: "pets".to_owned() },
            )
            .expect("pets resource should create");
        let pet = service
            .create_resource(
                &scope,
                &api.id,
                &pets.id,
                CreateResourceInput { path_part: "{pet_id}".to_owned() },
            )
            .expect("pet resource should create");
        service
            .put_method(
                &scope,
                &api.id,
                &pet.id,
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
                &scope,
                &api.id,
                &pet.id,
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
                    request_parameters: BTreeMap::from([
                        (
                            "integration.request.header.X-Upstream-Test"
                                .to_owned(),
                            "method.request.header.X-Test".to_owned(),
                        ),
                        (
                            "integration.request.path.proxy".to_owned(),
                            "method.request.path.pet_id".to_owned(),
                        ),
                        (
                            "integration.request.querystring.mode".to_owned(),
                            "method.request.querystring.view".to_owned(),
                        ),
                    ]),
                    request_templates: BTreeMap::new(),
                    timeout_in_millis: None,
                    type_: "HTTP_PROXY".to_owned(),
                    uri: Some(
                        "http://backend.example/orders/{proxy}?fixed=1"
                            .to_owned(),
                    ),
                },
            )
            .expect("integration should create");
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

        let invocation = service
            .resolve_execute_api_by_api_id(
                &api.id,
                &ExecuteApiRequest::new("localhost", "GET", "/dev/pets/cat-1")
                    .with_header("X-Test", "true")
                    .with_query_string("view=full"),
                None,
            )
            .expect("mapped request should resolve");

        match invocation.invocation().integration() {
            ExecuteApiIntegrationPlan::Http(request) => {
                assert_eq!(request.endpoint().host(), "backend.example");
                assert_eq!(request.endpoint().port(), 80);
                assert_eq!(
                    request.path(),
                    "/orders/cat-1?fixed=1&mode=full&view=full"
                );
                assert_eq!(
                    request.headers(),
                    &[
                        ("X-Test".to_owned(), "true".to_owned()),
                        ("X-Upstream-Test".to_owned(), "true".to_owned(),),
                    ]
                );
            }
            other => panic!("expected HTTP integration plan, got {other:?}"),
        }
    }

    #[test]
    fn apigw_v1_runtime_unsupported_http_request_parameter_mapping_is_explicit()
     {
        let service = service("http-request-mappings-invalid");
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
        let resource = service
            .create_resource(
                &scope,
                &api.id,
                &api.root_resource_id,
                CreateResourceInput { path_part: "pets".to_owned() },
            )
            .expect("resource should create");
        service
            .put_method(
                &scope,
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
                    integration_http_method: Some("GET".to_owned()),
                    passthrough_behavior: None,
                    request_parameters: BTreeMap::from([(
                        "integration.request.header.X-Upstream-Test"
                            .to_owned(),
                        "$context.requestId".to_owned(),
                    )]),
                    request_templates: BTreeMap::new(),
                    timeout_in_millis: None,
                    type_: "HTTP_PROXY".to_owned(),
                    uri: Some("http://backend.example/orders".to_owned()),
                },
            )
            .expect("integration should create");
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

        let error = service
            .resolve_execute_api_by_api_id(
                &api.id,
                &ExecuteApiRequest::new("localhost", "GET", "/dev/pets"),
                None,
            )
            .expect_err("unsupported mapping should fail");

        assert_eq!(
            error,
            ExecuteApiError::IntegrationFailure {
                message:
                    "Unsupported API Gateway request parameter mapping \"$context.requestId\"."
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
