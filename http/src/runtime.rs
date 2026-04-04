use crate::aws_error_shape::AwsErrorShape as _;
use crate::cognito;
use crate::query::{
    missing_action_error, parse_request as parse_query_request,
};
use crate::request::{EdgeRequest, HttpRequest};
use crate::routing;
use crate::s3;
use crate::xml::XmlBuilder;
use auth::{
    Authenticator, RequestAuth, RequestHeader, VerifiedRequest,
    VerifiedSignature,
};
use aws::RegionId;
use aws::parse_reserved_execute_api_path;
use aws::parse_reserved_lambda_function_url_path;
use aws::{
    AwsError, AwsErrorFamily, CredentialScope, ProtocolFamily, RequestContext,
    RuntimeDefaults, ServiceName, SharedAdvertisedEdge,
};
use ciborium::into_writer;
use edge_protocol::http_reason_phrase;
use edge_runtime::RuntimeServices;
use httpdate::fmt_http_date;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use services::FunctionUrlInvocationInput;
use services::S3Scope;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::SystemTime;

const REQUEST_ID: &str = "0000000000000000";
static NEVER_CANCELLED: AtomicBool = AtomicBool::new(false);

#[derive(Clone)]
pub struct EdgeRouter {
    advertised_edge: SharedAdvertisedEdge,
    defaults: RuntimeDefaults,
    authenticator: Authenticator,
    runtime: RuntimeServices,
}

#[derive(Clone)]
pub struct EdgeRequestExecutor {
    router: EdgeRouter,
}

impl EdgeRequestExecutor {
    pub fn execute(
        &self,
        request: EdgeRequest,
        request_cancellation: &AtomicBool,
    ) -> EdgeResponse {
        self.router.route(request, request_cancellation)
    }
}

impl EdgeRouter {
    pub fn new(
        defaults: RuntimeDefaults,
        advertised_edge: SharedAdvertisedEdge,
        authenticator: Authenticator,
        runtime: RuntimeServices,
    ) -> Self {
        Self { advertised_edge, defaults, authenticator, runtime }
    }

    pub fn begin_shutdown(&self) {
        self.runtime.begin_shutdown();
    }

    pub fn shutdown(&self) -> Vec<edge_runtime::ShutdownWarning> {
        self.begin_shutdown();
        self.runtime.shutdown()
    }

    pub fn handle_bytes(&self, request: &[u8]) -> EdgeResponse {
        match EdgeRequest::parse(request) {
            Ok(request) => self.handle_request(request),
            Err(error) => EdgeResponse::json(
                400,
                json!({ "message": error.to_string() }),
            ),
        }
    }

    pub fn handle_request(&self, request: EdgeRequest) -> EdgeResponse {
        self.route(request, &NEVER_CANCELLED)
    }

    pub fn request_executor(&self) -> EdgeRequestExecutor {
        EdgeRequestExecutor { router: self.clone() }
    }

    pub(crate) fn advertised_edge(&self) -> &SharedAdvertisedEdge {
        &self.advertised_edge
    }

    pub(crate) fn runtime_services(&self) -> &RuntimeServices {
        &self.runtime
    }

    fn route(
        &self,
        request: EdgeRequest,
        request_cancellation: &AtomicBool,
    ) -> EdgeResponse {
        if let Some(response) = self.internal_route(&request) {
            return response;
        }

        if let Some(response) = self
            .route_reserved_lambda_function_url(&request, request_cancellation)
        {
            return response;
        }

        if let Some(response) =
            self.route_reserved_execute_api(&request, request_cancellation)
        {
            return response;
        }

        if let Some(response) =
            self.route_function_url(&request, request_cancellation)
        {
            return response;
        }

        let advertised_edge = self.advertised_edge.current();
        let Some(protocol) =
            routing::detect_generic_protocol(&request, &advertised_edge)
        else {
            if s3::is_rest_xml_request(&request, &advertised_edge) {
                let verified_signature =
                    match self.authenticate_details(&request) {
                        Ok(verified_signature) => verified_signature,
                        Err(error) => return s3::s3_error_response(&error),
                    };

                return self
                    .route_rest_xml(request, verified_signature.as_ref());
            }

            return EdgeResponse::json(404, json!({ "message": "not found" }));
        };

        let verified_signature = match self.authenticate_details(&request) {
            Ok(verified_signature) => verified_signature,
            Err(error) => {
                return if protocol == ProtocolFamily::RestXml {
                    s3::s3_error_response(&error)
                } else {
                    EdgeResponse::aws(protocol, &error)
                };
            }
        };
        let verified_request = verified_signature
            .as_ref()
            .map(VerifiedSignature::verified_request);

        let response = match protocol {
            ProtocolFamily::Query => self.route_query(
                &request,
                verified_request,
                request_cancellation,
            ),
            ProtocolFamily::AwsJson10 | ProtocolFamily::AwsJson11 => self
                .route_json(
                    &request,
                    protocol,
                    verified_request,
                    request_cancellation,
                ),
            ProtocolFamily::RestJson => self.route_rest_json(
                &request,
                verified_request,
                request_cancellation,
            ),
            ProtocolFamily::SmithyRpcV2Cbor => {
                self.route_smithy_cbor(&request, verified_request)
            }
            ProtocolFamily::RestXml => {
                self.route_rest_xml(request, verified_signature.as_ref())
            }
        };

        response
    }
    fn route_reserved_lambda_function_url(
        &self,
        request: &HttpRequest<'_>,
        request_cancellation: &AtomicBool,
    ) -> Option<EdgeResponse> {
        let function_url = parse_reserved_lambda_function_url_path(
            request.path_without_query(),
        )?;
        self.route_lambda_function_url_request(
            request,
            function_url.region(),
            function_url.url_id(),
            function_url.request_path(),
            request_cancellation,
        )
    }

    fn route_function_url(
        &self,
        request: &HttpRequest<'_>,
        request_cancellation: &AtomicBool,
    ) -> Option<EdgeResponse> {
        let host = request.header("host")?;
        let function_url = parse_function_url_host(host)?;

        self.route_lambda_function_url_request(
            request,
            &function_url.region,
            &function_url.url_id,
            request.path_without_query(),
            request_cancellation,
        )
    }
    fn route_lambda_function_url_request(
        &self,
        request: &HttpRequest<'_>,
        region: &RegionId,
        url_id: &str,
        request_path: &str,
        request_cancellation: &AtomicBool,
    ) -> Option<EdgeResponse> {
        let verified_request = match self.authenticate(request) {
            Ok(verified_request) => verified_request,
            Err(error) => return Some(function_url_error_response(&error)),
        };
        if let Some(verified_request) = verified_request.as_ref() {
            if verified_request.scope().service() != ServiceName::Lambda {
                return Some(function_url_error_response(
                    &signature_scope_mismatch_error(format!(
                        "Credential scope service {} does not match Lambda function URL requests.",
                        verified_request.scope().service().as_str(),
                    )),
                ));
            }
            if verified_request.scope().region() != region {
                return Some(function_url_error_response(
                    &signature_scope_mismatch_error(format!(
                        "Credential scope region {} does not match Lambda function URL region {}.",
                        verified_request.scope().region().as_str(),
                        region.as_str(),
                    )),
                ));
            }
        }
        let resolved =
            match self.runtime.lambda().resolve_function_url(region, url_id) {
                Ok(resolved) => resolved,
                Err(error) => {
                    return Some(function_url_lambda_error_response(&error));
                }
            };
        let output =
            match self.runtime.lambda_requests().invoke_resolved_function_url(
                &resolved,
                FunctionUrlInvocationInput {
                    body: request.body().to_vec(),
                    headers: request
                        .headers()
                        .map(|(name, value)| {
                            (name.to_owned(), value.to_owned())
                        })
                        .collect(),
                    method: request.method().to_owned(),
                    path: request_path.to_owned(),
                    protocol: None,
                    query_string: request.query_string().map(str::to_owned),
                    source_ip: request.source_ip().map(str::to_owned),
                    domain_name: self
                        .advertised_edge
                        .current()
                        .lambda_function_domain_name(),
                },
                verified_request
                    .as_ref()
                    .map(VerifiedRequest::caller_identity),
                &|| request_cancellation.load(Ordering::SeqCst),
            ) {
                Ok(output) => output,
                Err(error) => {
                    return Some(function_url_lambda_error_response(&error));
                }
            };

        let content_type = output
            .headers()
            .iter()
            .find_map(|(name, value)| {
                name.eq_ignore_ascii_case("content-type")
                    .then_some(value.as_str())
            })
            .unwrap_or("application/json");
        let mut response = EdgeResponse::bytes(
            output.status_code(),
            content_type,
            output.body().to_vec(),
        );
        for (name, value) in output.headers() {
            if name.eq_ignore_ascii_case("content-type") {
                response = response.set_header(name, value);
            } else if should_skip_function_url_response_header(name) {
                continue;
            } else {
                response = response.with_header(name, value);
            }
        }

        Some(response)
    }

    fn route_reserved_execute_api(
        &self,
        request: &HttpRequest<'_>,
        request_cancellation: &AtomicBool,
    ) -> Option<EdgeResponse> {
        let execute_api =
            parse_reserved_execute_api_path(request.path_without_query())?;
        Some(self.route_execute_api_request(
            request,
            execute_api.api_id(),
            execute_api.request_path(),
            request_cancellation,
        ))
    }

    fn route_execute_api_request(
        &self,
        request: &HttpRequest<'_>,
        api_id: &str,
        request_path: &str,
        request_cancellation: &AtomicBool,
    ) -> EdgeResponse {
        let execute_request = build_execute_api_request(request, request_path);

        let verified_request = match self.authenticate(request) {
            Ok(verified_request) => verified_request,
            Err(error) => {
                return execute_api_auth_error_response(&error);
            }
        };
        if let Some(verified_request) = verified_request.as_ref()
            && verified_request.scope().service() != ServiceName::ApiGateway
        {
            return execute_api_auth_error_response(
                &signature_scope_mismatch_error(format!(
                    "Credential scope service {} does not match execute-api requests.",
                    verified_request.scope().service().as_str(),
                )),
            );
        }
        let resolved =
            self.runtime.apigateway().resolve_execute_api_by_api_id(
                api_id,
                &execute_request,
                verified_request
                    .as_ref()
                    .map(VerifiedRequest::caller_identity),
            );
        let (scope, invocation) = match resolved {
            Ok(target) => target.into_parts(),
            Err(error) => return execute_api_error_response(&error),
        };
        if let Some(verified_request) = verified_request.as_ref()
            && verified_request.scope().region() != scope.region()
        {
            return execute_api_auth_error_response(
                &signature_scope_mismatch_error(format!(
                    "Credential scope region {} does not match execute-api region {}.",
                    verified_request.scope().region().as_str(),
                    scope.region().as_str(),
                )),
            );
        }

        let is_cancelled = || request_cancellation.load(Ordering::SeqCst);
        match self.runtime.execute_api_executor().execute_with_cancellation(
            &scope,
            &invocation,
            &is_cancelled,
        ) {
            Ok(response) => {
                let content_type = response
                    .headers()
                    .iter()
                    .find_map(|(name, value)| {
                        name.eq_ignore_ascii_case("content-type")
                            .then_some(value.as_str())
                    })
                    .unwrap_or("application/json");
                let mut edge = EdgeResponse::bytes(
                    response.status_code(),
                    content_type,
                    response.body().to_vec(),
                );
                for (name, value) in response.headers() {
                    if name.eq_ignore_ascii_case("content-type") {
                        edge = edge.set_header(name, value);
                    } else if should_skip_function_url_response_header(name) {
                        continue;
                    } else {
                        edge = edge.with_header(name, value);
                    }
                }
                edge
            }
            Err(error) => execute_api_error_response(&error),
        }
    }

    fn internal_route(
        &self,
        request: &HttpRequest<'_>,
    ) -> Option<EdgeResponse> {
        if let Some((user_pool_id, document)) =
            cognito_well_known_route(request.path_without_query())
        {
            if request.method() != "GET" {
                return Some(
                    EdgeResponse::json(
                        405,
                        json!({ "message": "method not allowed" }),
                    )
                    .with_header("Allow", "GET"),
                );
            }
            let response = match document {
                CognitoWellKnownDocument::OpenIdConfiguration => {
                    cognito::open_id_configuration(
                        self.runtime.cognito(),
                        user_pool_id,
                    )
                }
                CognitoWellKnownDocument::Jwks => cognito::jwks_document(
                    self.runtime.cognito(),
                    user_pool_id,
                ),
            };

            return Some(match response {
                Ok(body) => EdgeResponse::bytes(200, "application/json", body),
                Err(error) => EdgeResponse::json(
                    error.status_code(),
                    json!({
                        "__type": error.code(),
                        "message": error.message(),
                    }),
                ),
            });
        }
        if request.path_without_query() == "/__aws/sns/signing-cert.pem" {
            if request.method() != "GET" {
                return Some(
                    EdgeResponse::json(
                        405,
                        json!({ "message": "method not allowed" }),
                    )
                    .with_header("Allow", "GET"),
                );
            }
            return Some(EdgeResponse::bytes(
                200,
                "application/x-pem-file",
                edge_runtime::cloudish_sns_signing_cert_pem().to_vec(),
            ));
        }

        match (request.method(), request.path_without_query()) {
            ("GET", "/__cloudish/health") => Some(self.health_response()),
            ("GET", "/__cloudish/status") => Some(self.status_response()),
            (_, "/__cloudish/health") | (_, "/__cloudish/status") => Some(
                EdgeResponse::json(
                    405,
                    json!({ "message": "method not allowed" }),
                )
                .with_header("Allow", "GET"),
            ),
            _ => None,
        }
    }

    fn route_query(
        &self,
        request: &HttpRequest<'_>,
        verified_request: Option<&VerifiedRequest>,
        request_cancellation: &AtomicBool,
    ) -> EdgeResponse {
        let parsed_query = match parse_query_request(request) {
            Ok(Some(parsed_query)) => parsed_query,
            Ok(None) => {
                return EdgeResponse::aws(
                    ProtocolFamily::Query,
                    &missing_action_error(),
                );
            }
            Err(error) => {
                return EdgeResponse::aws(ProtocolFamily::Query, &error);
            }
        };
        let params = parsed_query.parameters();
        let Some(action) = params.action() else {
            return EdgeResponse::aws(
                ProtocolFamily::Query,
                &missing_action_error(),
            );
        };
        let action = action.to_owned();
        let version = match params.required("Version") {
            Ok(version) => version,
            Err(error) => {
                return EdgeResponse::aws(ProtocolFamily::Query, &error);
            }
        };
        let scope = verified_request.map(VerifiedRequest::scope);

        let service = match scope {
            Some(scope) => {
                if !routing::supports_protocol(
                    scope.service(),
                    ProtocolFamily::Query,
                ) {
                    let error = signature_scope_mismatch_error(format!(
                        "Credential scope service {} does not support Query requests.",
                        scope.service().as_str()
                    ));
                    return EdgeResponse::aws(ProtocolFamily::Query, &error);
                }

                match routing::validate_query_for_service(
                    scope.service(),
                    &action,
                    version,
                ) {
                    routing::ScopedQueryValidation::Valid => {}
                    routing::ScopedQueryValidation::ActionOutsideService => {
                        let error = signature_scope_mismatch_error(format!(
                            "Credential scope service {} does not match query action {}.",
                            scope.service().as_str(),
                            action,
                        ));
                        return EdgeResponse::aws(
                            ProtocolFamily::Query,
                            &error,
                        );
                    }
                    routing::ScopedQueryValidation::InvalidVersion => {
                        let error =
                            invalid_query_version_error(&action, version);
                        return EdgeResponse::aws(
                            ProtocolFamily::Query,
                            &error,
                        );
                    }
                }

                scope.service()
            }
            None => match routing::resolve_unsigned_query_service(
                &action,
                Some(version),
            ) {
                Some(service) => service,
                None => {
                    let error = invalid_query_action_error(&action, None);
                    return EdgeResponse::aws(ProtocolFamily::Query, &error);
                }
            },
        };
        if !routing::query_action_matches_version(service, &action, version) {
            let error = invalid_query_version_error(&action, version);
            return EdgeResponse::aws(ProtocolFamily::Query, &error);
        }

        let context = self.request_context(
            verified_request,
            service,
            ProtocolFamily::Query,
            &action,
        );
        let mut normalized_request = request.clone();
        normalized_request.set_body(parsed_query.raw_parameters().to_vec());

        if let Some(response) = routing::dispatch_query(
            self,
            service,
            &normalized_request,
            &context,
            verified_request,
            request_cancellation,
        ) {
            return response;
        }

        EdgeResponse::aws(
            ProtocolFamily::Query,
            &unknown_query_error(&context),
        )
    }

    fn route_json(
        &self,
        request: &HttpRequest<'_>,
        protocol: ProtocolFamily,
        verified_request: Option<&VerifiedRequest>,
        request_cancellation: &AtomicBool,
    ) -> EdgeResponse {
        let target = request.header("x-amz-target");
        let route = match resolve_json_route(
            protocol,
            target,
            verified_request.map(VerifiedRequest::scope),
        ) {
            Ok(route) => route,
            Err(error) => return EdgeResponse::aws(protocol, &error),
        };

        let context = self.request_context(
            verified_request,
            route.service,
            protocol,
            route.operation.as_deref().unwrap_or("UnknownOperation"),
        );

        if route.operation.is_some()
            && let Some(response) = routing::dispatch_json(
                self,
                route.service,
                request,
                &context,
                request_cancellation,
            )
        {
            return response;
        }

        EdgeResponse::aws(
            protocol,
            &unknown_json_error(
                &context,
                route.missing_target,
                route.original_target,
            ),
        )
    }

    fn route_smithy_cbor(
        &self,
        request: &HttpRequest<'_>,
        verified_request: Option<&VerifiedRequest>,
    ) -> EdgeResponse {
        let route = match resolve_smithy_route(
            request,
            verified_request.map(VerifiedRequest::scope),
        ) {
            Ok(route) => route,
            Err(error) => {
                return EdgeResponse::aws(
                    ProtocolFamily::SmithyRpcV2Cbor,
                    &error,
                );
            }
        };

        let context = self.request_context(
            verified_request,
            route.service,
            ProtocolFamily::SmithyRpcV2Cbor,
            route.operation.as_deref().unwrap_or("UnknownOperation"),
        );

        if route.operation.is_some()
            && let Some(response) = routing::dispatch_smithy(
                self,
                route.service,
                request,
                &context,
            )
        {
            return response;
        }

        EdgeResponse::aws(
            ProtocolFamily::SmithyRpcV2Cbor,
            &unknown_json_error(
                &context,
                route.missing_target,
                route.original_target,
            ),
        )
    }

    fn route_rest_json(
        &self,
        request: &HttpRequest<'_>,
        verified_request: Option<&VerifiedRequest>,
        request_cancellation: &AtomicBool,
    ) -> EdgeResponse {
        let Some(service) = routing::rest_json_service(request) else {
            return EdgeResponse::json(404, json!({ "message": "not found" }));
        };

        if let Some(verified_request) = verified_request
            && verified_request.scope().service() != service
        {
            let error = signature_scope_mismatch_error(format!(
                "Credential scope service {} does not match {} REST JSON requests.",
                verified_request.scope().service().as_str(),
                service.as_str(),
            ));
            return EdgeResponse::aws(ProtocolFamily::RestJson, &error);
        }

        let context = self.request_context(
            verified_request,
            service,
            ProtocolFamily::RestJson,
            "UnknownOperation",
        );

        match routing::dispatch_rest_json(
            self,
            service,
            request,
            &context,
            request_cancellation,
        ) {
            Some(response) => response,
            None => EdgeResponse::aws(
                ProtocolFamily::RestJson,
                &unknown_rest_json_error(&context),
            ),
        }
    }
    fn route_rest_xml(
        &self,
        mut request: EdgeRequest,
        verified_signature: Option<&VerifiedSignature>,
    ) -> EdgeResponse {
        let verified_request =
            verified_signature.map(VerifiedSignature::verified_request);
        if let Some(verified_request) = verified_request
            && verified_request.scope().service() != ServiceName::S3
        {
            let error = signature_scope_mismatch_error(format!(
                "Credential scope service {} does not match S3 REST XML requests.",
                verified_request.scope().service().as_str(),
            ));
            return s3::s3_error_response(&error);
        }

        if let Err(error) =
            s3::normalize_request(&mut request, verified_signature)
        {
            return s3::s3_error_response(&error);
        }

        let scope = S3Scope::new(
            verified_request
                .map(|verified_request| verified_request.account_id().clone())
                .unwrap_or_else(|| self.defaults.default_account_id().clone()),
            verified_request
                .map(|verified_request| {
                    verified_request.scope().region().clone()
                })
                .unwrap_or_else(|| self.defaults.default_region_id().clone()),
        );

        match s3::handle(
            &request,
            &scope,
            self.runtime.s3(),
            &self.advertised_edge.current(),
        ) {
            Ok(response) => response,
            Err(error) => s3::s3_error_response(&error),
        }
    }

    fn request_context(
        &self,
        verified_request: Option<&VerifiedRequest>,
        service: ServiceName,
        protocol: ProtocolFamily,
        operation: &str,
    ) -> RequestContext {
        RequestContext::trusted_new(
            verified_request
                .map(|verified_request| verified_request.account_id().clone())
                .unwrap_or_else(|| self.defaults.default_account_id().clone()),
            verified_request
                .map(|verified_request| {
                    verified_request.scope().region().clone()
                })
                .unwrap_or_else(|| self.defaults.default_region_id().clone()),
            service,
            protocol,
            operation,
            verified_request.map(|verified_request| {
                verified_request.caller_identity().clone()
            }),
            verified_request.is_some(),
        )
    }

    fn authenticate(
        &self,
        request: &HttpRequest<'_>,
    ) -> Result<Option<VerifiedRequest>, AwsError> {
        self.authenticator.verify(
            &RequestAuth::new(
                request.method(),
                request.path(),
                request
                    .headers()
                    .map(|(name, value)| RequestHeader::new(name, value))
                    .collect(),
                request.body(),
            ),
            self.runtime.iam(),
            self.runtime.sts(),
        )
    }

    fn authenticate_details(
        &self,
        request: &HttpRequest<'_>,
    ) -> Result<Option<VerifiedSignature>, AwsError> {
        self.authenticator.verify_details(
            &RequestAuth::new(
                request.method(),
                request.path(),
                request
                    .headers()
                    .map(|(name, value)| RequestHeader::new(name, value))
                    .collect(),
                request.body(),
            ),
            self.runtime.iam(),
            self.runtime.sts(),
        )
    }

    fn health_response(&self) -> EdgeResponse {
        EdgeResponse::json(200, json!({ "status": "ok" }))
    }

    fn status_response(&self) -> EdgeResponse {
        EdgeResponse::json(
            200,
            json!({
                "defaultAccount": self.defaults.default_account(),
                "defaultRegion": self.defaults.default_region(),
                "stateDirectory": self.defaults.state_directory().display().to_string(),
            }),
        )
    }
}

#[derive(Debug, Clone)]
pub struct EdgeResponse {
    status_code: u16,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl EdgeResponse {
    pub(crate) fn aws(protocol: ProtocolFamily, error: &AwsError) -> Self {
        match protocol {
            ProtocolFamily::Query | ProtocolFamily::RestXml => {
                let body = query_error_body(error).into_bytes();
                Self::bytes(error.status_code(), "text/xml", body)
            }
            ProtocolFamily::AwsJson10 => Self::bytes(
                error.status_code(),
                "application/x-amz-json-1.0",
                aws_json_error_body(error),
            ),
            ProtocolFamily::AwsJson11 => Self::bytes(
                error.status_code(),
                "application/x-amz-json-1.1",
                aws_json_error_body(error),
            ),
            ProtocolFamily::RestJson => Self::bytes(
                error.status_code(),
                "application/json",
                rest_json_error_body(error),
            )
            .set_header("x-amzn-errortype", error.code()),
            ProtocolFamily::SmithyRpcV2Cbor => Self::bytes(
                error.status_code(),
                "application/cbor",
                smithy_cbor_error_body(error),
            )
            .with_header("Smithy-Protocol", "rpc-v2-cbor"),
        }
    }

    pub(crate) fn bytes(
        status_code: u16,
        content_type: &str,
        body: Vec<u8>,
    ) -> Self {
        Self {
            status_code,
            headers: vec![
                ("Date".to_owned(), fmt_http_date(SystemTime::now())),
                ("Content-Type".to_owned(), content_type.to_owned()),
                ("Content-Length".to_owned(), body.len().to_string()),
                ("Connection".to_owned(), "close".to_owned()),
            ],
            body,
        }
    }

    pub(crate) fn empty(status_code: u16) -> Self {
        Self {
            status_code,
            headers: vec![
                ("Date".to_owned(), fmt_http_date(SystemTime::now())),
                ("Content-Length".to_owned(), "0".to_owned()),
                ("Connection".to_owned(), "close".to_owned()),
            ],
            body: Vec::new(),
        }
    }

    pub(crate) fn json(status_code: u16, body: Value) -> Self {
        match serde_json::to_vec(&body) {
            Ok(body) => Self::bytes(status_code, "application/json", body),
            Err(error) => Self::bytes(
                500,
                "application/json",
                format!(
                    "{{\"message\":\"Failed to serialize JSON response: {error}\"}}"
                )
                .into_bytes(),
            ),
        }
    }

    pub(crate) fn with_header(mut self, name: &str, value: &str) -> Self {
        self.headers.push((name.to_owned(), value.to_owned()));
        self
    }

    pub(crate) fn set_header(
        mut self,
        name: impl Into<String>,
        value: impl Into<String>,
    ) -> Self {
        let name = name.into();
        let value = value.into();

        if let Some((_, existing_value)) =
            self.headers.iter_mut().find(|(existing_name, _)| {
                existing_name.eq_ignore_ascii_case(&name)
            })
        {
            *existing_value = value;
            return self;
        }

        self.headers.push((name, value));
        self
    }

    pub fn to_http_bytes(&self) -> Vec<u8> {
        let mut response = format!(
            "HTTP/1.1 {} {}\r\n",
            self.status_code,
            http_reason_phrase(self.status_code)
        )
        .into_bytes();

        for (name, value) in &self.headers {
            response.extend_from_slice(name.as_bytes());
            response.extend_from_slice(b": ");
            response.extend_from_slice(value.as_bytes());
            response.extend_from_slice(b"\r\n");
        }

        response.extend_from_slice(b"\r\n");
        response.extend_from_slice(&self.body);
        response
    }

    pub fn into_parts(self) -> (u16, Vec<(String, String)>, Vec<u8>) {
        (self.status_code, self.headers, self.body)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct JsonRoute<'a> {
    service: ServiceName,
    operation: Option<String>,
    missing_target: bool,
    original_target: Option<&'a str>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CognitoWellKnownDocument {
    Jwks,
    OpenIdConfiguration,
}
#[derive(Debug, Clone, PartialEq, Eq)]
struct FunctionUrlHost {
    region: RegionId,
    url_id: String,
}
fn cognito_well_known_route(
    path: &str,
) -> Option<(&str, CognitoWellKnownDocument)> {
    if let Some(pool_path) =
        path.strip_suffix("/.well-known/openid-configuration")
    {
        return cognito_pool_id_segment(pool_path).map(|pool_id| {
            (pool_id, CognitoWellKnownDocument::OpenIdConfiguration)
        });
    }
    if let Some(pool_path) = path.strip_suffix("/.well-known/jwks.json") {
        return cognito_pool_id_segment(pool_path)
            .map(|pool_id| (pool_id, CognitoWellKnownDocument::Jwks));
    }

    None
}
fn cognito_pool_id_segment(pool_path: &str) -> Option<&str> {
    let pool_id = pool_path.strip_prefix('/')?;
    (!pool_id.is_empty() && !pool_id.contains('/')).then_some(pool_id)
}
fn parse_function_url_host(host: &str) -> Option<FunctionUrlHost> {
    let authority = host.trim().to_ascii_lowercase();
    let host = authority
        .rsplit_once(':')
        .filter(|(_, port)| port.parse::<u16>().is_ok())
        .map(|(host, _)| host)
        .unwrap_or(authority.as_str());
    let (url_id, suffix) = host.split_once(".lambda-url.")?;
    let region =
        suffix.strip_suffix(".localhost")?.parse::<RegionId>().ok()?;
    if url_id.is_empty() {
        return None;
    }

    Some(FunctionUrlHost { region, url_id: url_id.to_owned() })
}
fn function_url_error_response(error: &AwsError) -> EdgeResponse {
    EdgeResponse::json(
        error.status_code(),
        json!({ "Message": error.message() }),
    )
}
fn function_url_lambda_error_response(
    error: &services::LambdaError,
) -> EdgeResponse {
    function_url_error_response(&error.to_aws_error())
}
fn execute_api_auth_error_response(error: &AwsError) -> EdgeResponse {
    EdgeResponse::json(
        error.status_code(),
        json!({ "message": error.message() }),
    )
}
fn execute_api_error_response(
    error: &services::ExecuteApiError,
) -> EdgeResponse {
    EdgeResponse::json(
        error.status_code(),
        json!({ "message": error.message() }),
    )
}
fn should_skip_function_url_response_header(name: &str) -> bool {
    name.eq_ignore_ascii_case("connection")
        || name.eq_ignore_ascii_case("content-length")
        || name.eq_ignore_ascii_case("date")
}
fn build_execute_api_request(
    request: &HttpRequest<'_>,
    request_path: &str,
) -> services::ExecuteApiRequest {
    let mut execute_request = services::ExecuteApiRequest::new(
        request.header("host").unwrap_or_default().to_owned(),
        request.method().to_owned(),
        request_path.to_owned(),
    )
    .with_body(request.body().to_vec())
    .with_protocol("HTTP/1.1");
    if let Some(query_string) = request.query_string() {
        execute_request = execute_request.with_query_string(query_string);
    }
    if let Some(source_ip) = request.source_ip() {
        execute_request = execute_request.with_source_ip(source_ip);
    }
    for (name, value) in request.headers() {
        execute_request =
            execute_request.with_header(name.to_owned(), value.to_owned());
    }

    execute_request
}

fn resolve_json_route<'a>(
    protocol: ProtocolFamily,
    target: Option<&'a str>,
    scope: Option<&CredentialScope>,
) -> Result<JsonRoute<'a>, AwsError> {
    if let Some(target) = target {
        if let Some((service, operation)) =
            routing::json_target(protocol, target)
        {
            if let Some(scope) =
                scope.filter(|scope| scope.service() != service)
            {
                return Err(signature_scope_mismatch_error(format!(
                    "Credential scope service {} does not match JSON target {}.",
                    scope.service().as_str(),
                    target,
                )));
            }

            return Ok(JsonRoute {
                service,
                operation: non_empty_operation(operation),
                missing_target: false,
                original_target: Some(target),
            });
        }

        if let Some(scope) = scope.filter(|scope| {
            routing::supports_protocol(scope.service(), protocol)
        }) {
            return Ok(JsonRoute {
                service: scope.service(),
                operation: None,
                missing_target: false,
                original_target: Some(target),
            });
        }

        if let Some(scope) = scope {
            return Err(signature_scope_mismatch_error(format!(
                "Credential scope service {} does not match JSON target {}.",
                scope.service().as_str(),
                target,
            )));
        }

        return Err(unknown_json_operation_without_context(target));
    }

    let Some(scope) = scope else {
        return Err(unknown_json_operation_without_context(
            "missing X-Amz-Target header",
        ));
    };

    if !routing::supports_protocol(scope.service(), protocol) {
        return Err(signature_scope_mismatch_error(format!(
            "Credential scope service {} does not support the requested JSON protocol.",
            scope.service().as_str(),
        )));
    }

    Ok(JsonRoute {
        service: scope.service(),
        operation: None,
        missing_target: true,
        original_target: None,
    })
}

fn resolve_smithy_route<'a>(
    request: &'a HttpRequest<'a>,
    scope: Option<&CredentialScope>,
) -> Result<JsonRoute<'a>, AwsError> {
    if let Some(path) = smithy_path(request.path_without_query()) {
        let Some(service) = routing::service_from_smithy_id(path.service_id)
        else {
            return Err(unknown_json_operation_without_context(
                path.service_id,
            ));
        };

        if let Some(scope) = scope.filter(|scope| scope.service() != service) {
            return Err(signature_scope_mismatch_error(format!(
                "Credential scope service {} does not match Smithy path service {}.",
                scope.service().as_str(),
                path.service_id,
            )));
        }

        return Ok(JsonRoute {
            service,
            operation: non_empty_operation(path.operation),
            missing_target: false,
            original_target: None,
        });
    }

    if let Some(target) = request.header("x-amz-target") {
        if let Some((service, operation)) =
            routing::json_target(ProtocolFamily::AwsJson10, target)
                .filter(|(service, _)| *service == ServiceName::CloudWatch)
        {
            if let Some(scope) =
                scope.filter(|scope| scope.service() != service)
            {
                return Err(signature_scope_mismatch_error(format!(
                    "Credential scope service {} does not match JSON target {}.",
                    scope.service().as_str(),
                    target,
                )));
            }

            return Ok(JsonRoute {
                service,
                operation: non_empty_operation(operation),
                missing_target: false,
                original_target: Some(target),
            });
        }

        return Err(unknown_json_operation_without_context(target));
    }

    let Some(scope) = scope else {
        return Err(unknown_json_operation_without_context(
            "missing Smithy RPC operation target",
        ));
    };

    if !routing::supports_protocol(
        scope.service(),
        ProtocolFamily::SmithyRpcV2Cbor,
    ) {
        return Err(signature_scope_mismatch_error(format!(
            "Credential scope service {} does not support Smithy RPC v2 CBOR.",
            scope.service().as_str(),
        )));
    }

    Ok(JsonRoute {
        service: scope.service(),
        operation: None,
        missing_target: true,
        original_target: None,
    })
}

fn non_empty_operation(operation: &str) -> Option<String> {
    let operation = operation.trim();

    if operation.is_empty() { None } else { Some(operation.to_owned()) }
}

fn invalid_query_action_error(
    action: &str,
    service: Option<ServiceName>,
) -> AwsError {
    let message = match service {
        Some(service) => {
            format!(
                "Unknown action {action} for service {}.",
                service.as_str()
            )
        }
        None => format!("Unknown action {action}."),
    };

    AwsError::trusted_custom(
        AwsErrorFamily::Validation,
        "InvalidAction",
        message,
        400,
        true,
    )
}

fn invalid_query_version_error(action: &str, version: &str) -> AwsError {
    AwsError::trusted_custom(
        AwsErrorFamily::Validation,
        "InvalidAction",
        format!("Could not find operation {action} for version {version}."),
        400,
        true,
    )
}

fn unknown_query_error(context: &RequestContext) -> AwsError {
    AwsError::trusted_custom(
        AwsErrorFamily::Validation,
        "InvalidAction",
        format!(
            "Unknown action {} for service {}.",
            context.operation(),
            context.service().as_str()
        ),
        400,
        true,
    )
}

fn unknown_json_error(
    context: &RequestContext,
    missing_target: bool,
    original_target: Option<&str>,
) -> AwsError {
    let message = if missing_target {
        format!(
            "Unknown operation for service {}: missing X-Amz-Target header.",
            context.service().as_str()
        )
    } else if let Some(target) = original_target {
        format!(
            "Unknown operation {target} for service {}.",
            context.service().as_str()
        )
    } else {
        format!(
            "Unknown operation {} for service {}.",
            context.operation(),
            context.service().as_str()
        )
    };

    AwsError::trusted_custom(
        AwsErrorFamily::UnsupportedOperation,
        "UnknownOperationException",
        message,
        400,
        true,
    )
}

fn unknown_rest_json_error(context: &RequestContext) -> AwsError {
    AwsError::trusted_custom(
        AwsErrorFamily::UnsupportedOperation,
        "UnknownOperationException",
        format!(
            "Unknown operation {} for service {}.",
            context.operation(),
            context.service().as_str()
        ),
        400,
        true,
    )
}

fn unknown_json_operation_without_context(detail: &str) -> AwsError {
    AwsError::trusted_custom(
        AwsErrorFamily::UnsupportedOperation,
        "UnknownOperationException",
        format!("Unknown operation: {detail}."),
        400,
        true,
    )
}

fn signature_scope_mismatch_error(message: impl Into<String>) -> AwsError {
    AwsError::trusted_custom(
        AwsErrorFamily::AccessDenied,
        "SignatureDoesNotMatch",
        message,
        403,
        true,
    )
}

fn query_error_body(error: &AwsError) -> String {
    let fault_type = if error.sender_fault() { "Sender" } else { "Receiver" };

    XmlBuilder::new()
        .start("ErrorResponse", None)
        .start("Error", None)
        .elem("Type", fault_type)
        .elem("Code", error.code())
        .elem("Message", error.message())
        .end("Error")
        .elem("RequestId", REQUEST_ID)
        .end("ErrorResponse")
        .build()
}

fn aws_json_error_body(error: &AwsError) -> Vec<u8> {
    match serde_json::to_vec(&AwsJsonErrorBody::from(error)) {
        Ok(body) => body,
        Err(serialization_error) => format!(
            "{{\"__type\":\"InternalFailure\",\"message\":\"Failed to serialize AWS JSON error body: {serialization_error}\"}}"
        )
        .into_bytes(),
    }
}

fn rest_json_error_body(error: &AwsError) -> Vec<u8> {
    match serde_json::to_vec(&json!({
        "Type": if error.sender_fault() { "User" } else { "Service" },
        "message": error.message(),
    })) {
        Ok(body) => body,
        Err(serialization_error) => format!(
            "{{\"Type\":\"Service\",\"message\":\"Failed to serialize REST JSON error body: {serialization_error}\"}}"
        )
        .into_bytes(),
    }
}

fn smithy_cbor_error_body(error: &AwsError) -> Vec<u8> {
    let mut body = Vec::new();
    if into_writer(&AwsJsonErrorBody::from(error), &mut body).is_err() {
        return Vec::new();
    }
    body
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct AwsJsonErrorBody {
    #[serde(rename = "__type")]
    error_type: String,
    message: String,
}

impl From<&AwsError> for AwsJsonErrorBody {
    fn from(error: &AwsError) -> Self {
        Self {
            error_type: error.code().to_owned(),
            message: error.message().to_owned(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SmithyPath<'a> {
    service_id: &'a str,
    operation: &'a str,
}

fn smithy_path(path: &str) -> Option<SmithyPath<'_>> {
    let path = path.trim_matches('/');
    let mut segments = path.split('/');
    let service_segment = segments.next()?;
    let service_id = segments.next()?;
    let operation_segment = segments.next()?;
    let operation = segments.next()?;

    if service_segment != "service"
        || operation_segment != "operation"
        || segments.next().is_some()
    {
        return None;
    }

    Some(SmithyPath { service_id, operation })
}

#[cfg(test)]
mod tests {
    use super::{
        AwsJsonErrorBody, CognitoWellKnownDocument, EdgeRouter,
        cognito_well_known_route,
    };
    use crate::request::HttpRequest;
    use crate::test_runtime;
    use crate::{iam_query, sts_query};
    use auth::VerifiedRequest;
    use aws::{
        Arn, AwsPrincipalType, CallerCredentialKind, CallerIdentity,
        CredentialScope, ProtocolFamily, ServiceName, StableAwsPrincipal,
    };
    use aws_smithy_eventstream::frame::read_message_from;
    use base64::Engine as _;
    use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
    use bytes::{Buf, Bytes};
    use ciborium::value::Value as CborValue;
    use ciborium::{from_reader, into_writer};
    use edge_runtime::RuntimeServices;
    use hmac::{Hmac, Mac};
    use httpdate::parse_http_date;
    use iam::{CreateRoleInput, CreateUserInput, IamScope};
    use serde_json::{Value, json};
    use services::{
        AddLambdaPermissionInput, ApiGatewayScope, CreateDeploymentInput,
        CreateFunctionInput, CreateFunctionUrlConfigInput,
        CreateHttpApiDeploymentInput, CreateHttpApiInput,
        CreateHttpApiIntegrationInput, CreateHttpApiRouteInput,
        CreateHttpApiStageInput, CreateQueueInput, CreateResourceInput,
        CreateRestApiInput, CreateStageInput, CreateTopicInput,
        LambdaCodeInput, LambdaExecutor, LambdaFunctionUrlAuthType,
        LambdaFunctionUrlInvokeMode, LambdaInvocationRequest,
        LambdaInvocationResult, LambdaPackageType, LambdaScope,
        ReceiveMessageInput, SnsScope, SqsScope,
    };
    use sha2::{Digest, Sha256};
    use std::collections::BTreeMap;
    use std::fmt::Write as _;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::{Arc, Mutex};
    use std::thread::{self, JoinHandle};
    use time::OffsetDateTime;

    const PROVIDED_BOOTSTRAP_ZIP_BASE64: &str = "UEsDBBQAAAAAAAAAIQCEK9lNDgAAAA4AAAAJAAAAYm9vdHN0cmFwIyEvYmluL3NoCmNhdApQSwECFAMUAAAAAAAAACEAhCvZTQ4AAAAOAAAACQAAAAAAAAAAAAAA7QEAAAAAYm9vdHN0cmFwUEsFBgAAAAABAAEANwAAADUAAAAAAA==";

    fn provided_bootstrap_zip() -> Vec<u8> {
        BASE64_STANDARD
            .decode(PROVIDED_BOOTSTRAP_ZIP_BASE64)
            .expect("provided bootstrap ZIP fixture should decode")
    }

    fn router() -> EdgeRouter {
        test_runtime::build_router(
            edge_runtime::TestRuntimeBuilder::new("http-runtime"),
            "http-runtime",
        )
    }

    fn router_with_runtime(label: &str) -> (EdgeRouter, RuntimeServices) {
        test_runtime::router_with_runtime(label)
    }

    fn router_with_lambda_executor(
        label: &str,
        executor: Arc<dyn LambdaExecutor + Send + Sync>,
    ) -> (EdgeRouter, RuntimeServices) {
        test_runtime::router_with_lambda_executor(label, executor)
    }

    fn apigateway_scope() -> ApiGatewayScope {
        ApiGatewayScope::new(
            "000000000000".parse().expect("account should parse"),
            "eu-west-2".parse().expect("region should parse"),
        )
    }

    fn alternate_apigateway_scope() -> ApiGatewayScope {
        ApiGatewayScope::new(
            "111111111111".parse().expect("account should parse"),
            "eu-west-2".parse().expect("region should parse"),
        )
    }

    fn alternate_lambda_scope() -> LambdaScope {
        LambdaScope::new(
            "111111111111".parse().expect("account should parse"),
            "us-east-1".parse().expect("region should parse"),
        )
    }

    fn iam_scope(scope: &ApiGatewayScope) -> IamScope {
        IamScope::new(scope.account_id().clone(), scope.region().clone())
    }

    fn lambda_iam_scope(scope: &LambdaScope) -> IamScope {
        IamScope::new(scope.account_id().clone(), scope.region().clone())
    }

    fn role_arn_for(scope: &LambdaScope) -> String {
        format!("arn:aws:iam::{}:role/lambda-role", scope.account_id())
    }

    fn deploy_execute_api(
        runtime: &RuntimeServices,
        integration_type: &str,
        uri: Option<String>,
        request_templates: BTreeMap<String, String>,
        disable_default_endpoint: bool,
    ) -> String {
        deploy_execute_api_in_scope(
            runtime,
            &apigateway_scope(),
            integration_type,
            uri,
            request_templates,
            disable_default_endpoint,
        )
    }

    fn deploy_execute_api_in_scope(
        runtime: &RuntimeServices,
        scope: &ApiGatewayScope,
        integration_type: &str,
        uri: Option<String>,
        request_templates: BTreeMap<String, String>,
        disable_default_endpoint: bool,
    ) -> String {
        let api = runtime.apigateway().create_rest_api(
            scope,
            CreateRestApiInput {
                binary_media_types: Vec::new(),
                description: None,
                disable_execute_api_endpoint: Some(disable_default_endpoint),
                endpoint_configuration: None,
                name: format!("runtime-{integration_type}"),
                tags: BTreeMap::new(),
            },
        );
        let api = api.expect("REST API should create");
        let resource = runtime.apigateway().create_resource(
            scope,
            &api.id,
            &api.root_resource_id,
            CreateResourceInput { path_part: "pets".to_owned() },
        );
        let resource = resource.expect("resource should create");
        let method = runtime.apigateway().put_method(
            scope,
            &api.id,
            &resource.id,
            "GET",
            services::PutMethodInput {
                api_key_required: Some(false),
                authorizer_id: None,
                authorization_type: "NONE".to_owned(),
                operation_name: None,
                request_models: BTreeMap::new(),
                request_parameters: BTreeMap::new(),
                request_validator_id: None,
            },
        );
        method.expect("method should create");
        let integration = runtime.apigateway().put_integration(
            scope,
            &api.id,
            &resource.id,
            "GET",
            services::PutIntegrationInput {
                cache_key_parameters: Vec::new(),
                cache_namespace: None,
                connection_id: None,
                connection_type: None,
                content_handling: None,
                credentials: None,
                http_method: Some("GET".to_owned()),
                integration_http_method: Some(
                    if integration_type == "AWS_PROXY" {
                        "POST"
                    } else {
                        "GET"
                    }
                    .to_owned(),
                ),
                passthrough_behavior: None,
                request_parameters: BTreeMap::new(),
                request_templates,
                timeout_in_millis: None,
                type_: integration_type.to_owned(),
                uri,
            },
        );
        integration.expect("integration should create");
        let deployment = runtime.apigateway().create_deployment(
            scope,
            &api.id,
            CreateDeploymentInput {
                description: None,
                stage_description: None,
                stage_name: None,
                variables: BTreeMap::new(),
            },
        );
        let deployment = deployment.expect("deployment should create");
        let stage = runtime.apigateway().create_stage(
            scope,
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
        );
        stage.expect("stage should create");

        api.id
    }

    fn deploy_http_api_v2(
        runtime: &RuntimeServices,
        exact_uri: String,
        greedy_uri: String,
        default_uri: String,
    ) -> String {
        let scope = apigateway_scope();
        let api = runtime.apigateway().create_http_api(
            &scope,
            CreateHttpApiInput {
                description: None,
                disable_execute_api_endpoint: Some(false),
                ip_address_type: None,
                name: "http-api-runtime".to_owned(),
                protocol_type: "HTTP".to_owned(),
                route_selection_expression: None,
                tags: BTreeMap::new(),
                version: None,
            },
        );
        let api = api.expect("HTTP API should create");
        let exact = runtime.apigateway().create_http_api_integration(
            &scope,
            &api.api_id,
            CreateHttpApiIntegrationInput {
                connection_id: None,
                connection_type: None,
                credentials_arn: None,
                description: None,
                integration_method: None,
                integration_type: "HTTP_PROXY".to_owned(),
                integration_uri: Some(exact_uri),
                payload_format_version: None,
                request_parameters: BTreeMap::new(),
                request_templates: BTreeMap::new(),
                template_selection_expression: None,
                timeout_in_millis: None,
            },
        );
        let exact = exact.expect("exact integration should create");
        let greedy = runtime.apigateway().create_http_api_integration(
            &scope,
            &api.api_id,
            CreateHttpApiIntegrationInput {
                connection_id: None,
                connection_type: None,
                credentials_arn: None,
                description: None,
                integration_method: None,
                integration_type: "HTTP_PROXY".to_owned(),
                integration_uri: Some(greedy_uri),
                payload_format_version: None,
                request_parameters: BTreeMap::new(),
                request_templates: BTreeMap::new(),
                template_selection_expression: None,
                timeout_in_millis: None,
            },
        );
        let greedy = greedy.expect("greedy integration should create");
        let default_integration =
            runtime.apigateway().create_http_api_integration(
                &scope,
                &api.api_id,
                CreateHttpApiIntegrationInput {
                    connection_id: None,
                    connection_type: None,
                    credentials_arn: None,
                    description: None,
                    integration_method: None,
                    integration_type: "HTTP_PROXY".to_owned(),
                    integration_uri: Some(default_uri),
                    payload_format_version: None,
                    request_parameters: BTreeMap::new(),
                    request_templates: BTreeMap::new(),
                    template_selection_expression: None,
                    timeout_in_millis: None,
                },
            );
        let default_integration =
            default_integration.expect("default integration should create");
        let exact_route = runtime.apigateway().create_http_api_route(
            &scope,
            &api.api_id,
            CreateHttpApiRouteInput {
                authorization_type: Some("NONE".to_owned()),
                authorizer_id: None,
                operation_name: None,
                route_key: "GET /pets/dog/1".to_owned(),
                target: Some(format!(
                    "integrations/{}",
                    exact
                        .integration_id
                        .expect("exact integration id should exist")
                )),
            },
        );
        exact_route.expect("exact route should create");
        let greedy_route = runtime.apigateway().create_http_api_route(
            &scope,
            &api.api_id,
            CreateHttpApiRouteInput {
                authorization_type: Some("NONE".to_owned()),
                authorizer_id: None,
                operation_name: None,
                route_key: "GET /pets/{proxy+}".to_owned(),
                target: Some(format!(
                    "integrations/{}",
                    greedy
                        .integration_id
                        .expect("greedy integration id should exist")
                )),
            },
        );
        greedy_route.expect("greedy route should create");
        let default_route = runtime.apigateway().create_http_api_route(
            &scope,
            &api.api_id,
            CreateHttpApiRouteInput {
                authorization_type: Some("NONE".to_owned()),
                authorizer_id: None,
                operation_name: None,
                route_key: "$default".to_owned(),
                target: Some(format!(
                    "integrations/{}",
                    default_integration
                        .integration_id
                        .expect("default integration id should exist")
                )),
            },
        );
        default_route.expect("default route should create");
        let stage = runtime.apigateway().create_http_api_stage(
            &scope,
            &api.api_id,
            CreateHttpApiStageInput {
                auto_deploy: Some(true),
                deployment_id: None,
                description: None,
                stage_name: "$default".to_owned(),
                stage_variables: BTreeMap::new(),
                tags: BTreeMap::new(),
            },
        );
        stage.expect("default stage should create");
        let deployment = runtime.apigateway().create_http_api_deployment(
            &scope,
            &api.api_id,
            CreateHttpApiDeploymentInput {
                description: Some("first".to_owned()),
                stage_name: None,
            },
        );
        deployment.expect("deployment should create");

        api.api_id
    }

    fn execute_api_request(
        method: &str,
        host: &str,
        path: &str,
        headers: &[(&str, &str)],
        body: &[u8],
    ) -> Vec<u8> {
        let mut request = format!(
            "{method} {path} HTTP/1.1\r\nHost: {host}\r\nContent-Length: {}\r\n",
            body.len()
        )
        .into_bytes();
        for (name, value) in headers {
            request.extend_from_slice(name.as_bytes());
            request.extend_from_slice(b": ");
            request.extend_from_slice(value.as_bytes());
            request.extend_from_slice(b"\r\n");
        }
        request.extend_from_slice(b"\r\n");
        request.extend_from_slice(body);

        request
    }

    fn reserved_execute_api_path(api_id: &str, path: &str) -> String {
        format!("/__aws/execute-api/{api_id}{path}")
    }

    fn reserved_execute_api_request(
        method: &str,
        api_id: &str,
        path: &str,
        headers: &[(&str, &str)],
        body: &[u8],
    ) -> Vec<u8> {
        execute_api_request(
            method,
            "localhost",
            &reserved_execute_api_path(api_id, path),
            headers,
            body,
        )
    }

    fn legacy_execute_api_host_request(
        method: &str,
        api_id: &str,
        path: &str,
        headers: &[(&str, &str)],
        body: &[u8],
    ) -> Vec<u8> {
        execute_api_request(
            method,
            &format!("{api_id}.execute-api.localhost"),
            path,
            headers,
            body,
        )
    }

    fn create_access_key_for_scope(
        runtime: &RuntimeServices,
        scope: &ApiGatewayScope,
        user_name: &str,
    ) -> (String, String) {
        runtime
            .iam()
            .create_user(
                &iam_scope(scope),
                CreateUserInput {
                    path: "/".to_owned(),
                    tags: Vec::new(),
                    user_name: user_name.to_owned(),
                },
            )
            .expect("IAM user should create");
        let access_key = runtime
            .iam()
            .create_access_key(&iam_scope(scope), user_name)
            .expect("access key should create");

        (access_key.access_key_id, access_key.secret_access_key)
    }

    fn ensure_lambda_role(runtime: &RuntimeServices, scope: &LambdaScope) {
        runtime
            .iam()
            .create_role(
                &lambda_iam_scope(scope),
                CreateRoleInput {
                    assume_role_policy_document: r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}"#.to_owned(),
                    description: String::new(),
                    max_session_duration: 3_600,
                    path: "/".to_owned(),
                    role_name: "lambda-role".to_owned(),
                    tags: Vec::new(),
                },
            )
            .expect("lambda execution role should create");
    }

    fn signed_reserved_execute_api_request(
        api_id: &str,
        path: &str,
        access_key_id: &str,
        secret_access_key: &str,
        region: &str,
    ) -> Vec<u8> {
        let now = OffsetDateTime::now_utc();
        let amz_date = format!(
            "{:04}{:02}{:02}T{:02}{:02}{:02}Z",
            now.year(),
            u8::from(now.month()),
            now.day(),
            now.hour(),
            now.minute(),
            now.second(),
        );
        let scope_date = &amz_date[..8];
        let host = "localhost";
        let reserved_path = reserved_execute_api_path(api_id, path);
        let request_target = reserved_path.split_once('?');
        let canonical_uri = request_target
            .map(|(uri, _)| uri)
            .unwrap_or(reserved_path.as_str());
        let canonical_query =
            request_target.map(|(_, query)| query).unwrap_or("");
        let signed_headers = "host;x-amz-date";
        let canonical_request = format!(
            "GET\n{canonical_uri}\n{canonical_query}\nhost:{host}\nx-amz-date:{amz_date}\n\n{signed_headers}\n{}",
            hash_hex(b""),
        );
        let signature = build_signature(
            secret_access_key,
            scope_date,
            region,
            "apigateway",
            &amz_date,
            &canonical_request,
        );
        let authorization = format!(
            "AWS4-HMAC-SHA256 Credential={access_key_id}/{scope_date}/{region}/apigateway/aws4_request, SignedHeaders={signed_headers}, Signature={signature}",
        );

        execute_api_request(
            "GET",
            host,
            &reserved_path,
            &[
                ("X-Amz-Date", amz_date.as_str()),
                ("Authorization", &authorization),
            ],
            b"",
        )
    }

    fn build_signature(
        secret_access_key: &str,
        date: &str,
        region: &str,
        service: &str,
        amz_date: &str,
        canonical_request: &str,
    ) -> String {
        let scope = format!("{date}/{region}/{service}/aws4_request");
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
            hash_hex(canonical_request.as_bytes()),
        );
        let signing_key =
            signing_key(secret_access_key, date, region, service);

        hex_encode(&hmac_bytes(&signing_key, string_to_sign.as_bytes()))
    }

    fn signing_key(
        secret_access_key: &str,
        date: &str,
        region: &str,
        service: &str,
    ) -> [u8; 32] {
        let date_key = hmac_bytes(
            format!("AWS4{secret_access_key}").as_bytes(),
            date.as_bytes(),
        );
        let region_key = hmac_bytes(&date_key, region.as_bytes());
        let service_key = hmac_bytes(&region_key, service.as_bytes());
        hmac_bytes(&service_key, b"aws4_request")
    }

    fn hmac_bytes(key: &[u8], data: &[u8]) -> [u8; 32] {
        let mut mac =
            Hmac::<Sha256>::new_from_slice(key).expect("HMAC accepts any key");
        mac.update(data);
        mac.finalize().into_bytes().into()
    }

    fn hash_hex(bytes: &[u8]) -> String {
        let mut hasher = Sha256::new();
        hasher.update(bytes);
        hex_encode(&hasher.finalize())
    }

    fn hex_encode(bytes: &[u8]) -> String {
        let mut encoded = String::with_capacity(bytes.len() * 2);
        for byte in bytes {
            write!(&mut encoded, "{byte:02x}")
                .expect("hex encoding should write to String");
        }
        encoded
    }

    fn lambda_integration_uri(function_name: &str) -> String {
        format!(
            "arn:aws:apigateway:eu-west-2:lambda:path/2015-03-31/functions/arn:aws:lambda:eu-west-2:000000000000:function:{function_name}/invocations"
        )
    }

    fn s3_create_bucket_request(path: &str, host: &str) -> Vec<u8> {
        s3_create_bucket_request_with_headers(path, host, "")
    }

    fn s3_create_bucket_request_with_headers(
        path: &str,
        host: &str,
        extra_headers: &str,
    ) -> Vec<u8> {
        let body = r#"<CreateBucketConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><LocationConstraint>eu-west-2</LocationConstraint></CreateBucketConfiguration>"#;
        format!(
            "PUT {path} HTTP/1.1\r\nHost: {host}\r\n{extra_headers}Content-Type: application/xml\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        )
        .into_bytes()
    }

    #[derive(Debug, Default)]
    struct RecordingLambdaExecutor {
        requests: Mutex<Vec<Vec<u8>>>,
    }

    impl RecordingLambdaExecutor {
        fn requests(&self) -> Vec<Vec<u8>> {
            self.requests.lock().expect("executor lock should succeed").clone()
        }
    }

    impl LambdaExecutor for RecordingLambdaExecutor {
        fn invoke(
            &self,
            request: &LambdaInvocationRequest,
        ) -> Result<LambdaInvocationResult, services::InfrastructureError>
        {
            self.requests
                .lock()
                .expect("executor lock should succeed")
                .push(request.payload().to_vec());
            let payload = serde_json::to_vec(&json!({
                "statusCode": 200,
                "headers": {
                    "content-type": "application/json",
                    "x-apigw-runtime": "lambda-proxy",
                },
                "body": BASE64_STANDARD.encode(request.payload()),
                "isBase64Encoded": true,
            }))
            .expect("lambda proxy response should serialize");

            Ok(LambdaInvocationResult::new(payload, Option::<String>::None))
        }

        fn invoke_async(
            &self,
            _request: LambdaInvocationRequest,
        ) -> Result<(), services::InfrastructureError> {
            Ok(())
        }

        fn validate_zip(
            &self,
            _runtime: &str,
            _handler: &str,
            _archive: &[u8],
        ) -> Result<(), services::InfrastructureError> {
            Ok(())
        }
    }

    struct CapturingHttpServer {
        address: std::net::SocketAddr,
        handle: Option<JoinHandle<()>>,
        request: Arc<Mutex<String>>,
    }

    impl CapturingHttpServer {
        fn spawn(response: Vec<u8>) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0")
                .expect("HTTP fixture should bind");
            let address = listener
                .local_addr()
                .expect("HTTP fixture should expose an address");
            let request = Arc::new(Mutex::new(String::new()));
            let captured = request.clone();
            let handle = thread::spawn(move || {
                let (mut stream, _) = listener
                    .accept()
                    .expect("fixture should accept a request");
                let mut request_bytes = Vec::new();
                stream
                    .read_to_end(&mut request_bytes)
                    .expect("request should be readable");
                *captured.lock().expect("capture lock should succeed") =
                    String::from_utf8(request_bytes)
                        .expect("captured request should be UTF-8");
                stream
                    .write_all(&response)
                    .expect("response should be writable");
            });

            Self { address, handle: Some(handle), request }
        }

        fn address(&self) -> std::net::SocketAddr {
            self.address
        }

        fn request(&self) -> String {
            self.request.lock().expect("capture lock should succeed").clone()
        }

        fn join(mut self) -> String {
            let request = self.request();
            let handle =
                self.handle.take().expect("fixture should keep its worker");
            let joined = handle.join();
            assert!(joined.is_ok(), "fixture worker should finish");
            request
        }
    }

    fn split_response(response: &[u8]) -> (&str, Vec<(&str, &str)>, &[u8]) {
        let header_end = response
            .windows(4)
            .position(|window| window == b"\r\n\r\n")
            .expect("response should contain a header terminator");
        let headers = std::str::from_utf8(
            response
                .get(..header_end)
                .expect("response should contain header bytes"),
        )
        .expect("response headers should be valid UTF-8");
        let mut lines = headers.split("\r\n");
        let status = lines.next().expect("response should have a status line");
        let mut parsed_headers = Vec::new();
        for line in lines {
            let (name, value) =
                line.split_once(':').expect("header should contain ':'");
            parsed_headers.push((name, value.trim()));
        }

        let body = response
            .get(header_end + 4..)
            .expect("response should contain a body slice");
        (status, parsed_headers, body)
    }

    fn header_value<'a>(
        headers: &'a [(&'a str, &'a str)],
        name: &str,
    ) -> Option<&'a str> {
        headers
            .iter()
            .find(|(header, _)| header.eq_ignore_ascii_case(name))
            .map(|(_, value)| *value)
    }

    fn owned_header_value<'a>(
        headers: &'a [(String, String)],
        name: &str,
    ) -> Option<&'a str> {
        headers
            .iter()
            .find(|(header, _)| header.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }

    fn json_request(
        method: &str,
        path: &str,
        host: &str,
        body: &str,
    ) -> Vec<u8> {
        format!(
            "{method} {path} HTTP/1.1\r\nHost: {host}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        )
        .into_bytes()
    }

    fn aws_json_request(version: &str, target: &str, body: &str) -> Vec<u8> {
        format!(
            "POST / HTTP/1.1\r\nHost: localhost\r\nX-Amz-Target: {target}\r\nContent-Type: application/x-amz-json-{version}\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        )
        .into_bytes()
    }

    fn query_request(body: &str) -> Vec<u8> {
        format!(
            "POST / HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/x-www-form-urlencoded\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        )
        .into_bytes()
    }

    fn smithy_cbor_request(path: &str, body: &Value) -> Vec<u8> {
        let mut encoded = Vec::new();
        into_writer(body, &mut encoded)
            .expect("Smithy CBOR request body should encode");
        smithy_cbor_request_bytes(path, &encoded)
    }

    fn smithy_cbor_request_bytes(path: &str, body: &[u8]) -> Vec<u8> {
        let mut request = format!(
            "POST {path} HTTP/1.1\r\nHost: localhost\r\nSmithy-Protocol: rpc-v2-cbor\r\nContent-Type: application/cbor\r\nContent-Length: {}\r\n\r\n",
            body.len()
        )
        .into_bytes();
        request.extend_from_slice(body);
        request
    }

    fn cbor_map_field<'a>(
        value: &'a CborValue,
        key: &str,
    ) -> Option<&'a CborValue> {
        let CborValue::Map(entries) = value else {
            return None;
        };
        entries.iter().find_map(|(entry_key, entry_value)| match entry_key {
            CborValue::Text(text) if text == key => Some(entry_value),
            _ => None,
        })
    }

    fn cbor_to_test_json(value: &CborValue) -> Value {
        match value {
            CborValue::Null => Value::Null,
            CborValue::Bool(value) => Value::Bool(*value),
            CborValue::Integer(value) => {
                let value = i128::from(*value);
                if let Ok(value) = i64::try_from(value) {
                    Value::Number(value.into())
                } else {
                    Value::Number(
                        u64::try_from(value)
                            .expect("CBOR integer should fit in u64")
                            .into(),
                    )
                }
            }
            CborValue::Float(value) => Value::Number(
                serde_json::Number::from_f64(*value)
                    .expect("finite CBOR floats should convert to JSON"),
            ),
            CborValue::Text(value) => Value::String(value.clone()),
            CborValue::Array(values) => {
                Value::Array(values.iter().map(cbor_to_test_json).collect())
            }
            CborValue::Map(entries) => Value::Object(
                entries
                    .iter()
                    .map(|(key, value)| {
                        let key = match key {
                            CborValue::Text(key) => key.clone(),
                            other => format!("{other:?}"),
                        };
                        (key, cbor_to_test_json(value))
                    })
                    .collect(),
            ),
            CborValue::Tag(_, value) => cbor_to_test_json(value),
            other => Value::String(format!("{other:?}")),
        }
    }

    fn parse_json_slice<T>(bytes: &[u8], context: &str) -> T
    where
        T: serde::de::DeserializeOwned,
    {
        serde_json::from_slice(bytes).expect(context)
    }

    fn utf8_text(bytes: &[u8], context: &str) -> String {
        std::str::from_utf8(bytes).expect(context).to_owned()
    }

    fn json_field<'a>(
        value: &'a Value,
        key: &str,
        context: &str,
    ) -> &'a Value {
        value.get(key).expect(context)
    }

    fn json_array_item<'a>(
        value: &'a Value,
        index: usize,
        context: &str,
    ) -> &'a Value {
        let values = value.as_array().expect(context);
        values.get(index).expect(context)
    }

    fn json_string<'a>(value: &'a Value, context: &str) -> &'a str {
        value.as_str().expect(context)
    }

    fn json_u64(value: &Value, context: &str) -> u64 {
        value.as_u64().expect(context)
    }

    struct CloudWatchLogsRoundTripBodies {
        create_status: String,
        duplicate_status: String,
        stream_status: String,
        retention_status: String,
        tag_status: String,
        create_headers: Vec<(String, String)>,
        duplicate_headers: Vec<(String, String)>,
        create_body: Vec<u8>,
        stream_body: Vec<u8>,
        retention_body: Vec<u8>,
        tag_body: Vec<u8>,
        duplicate_body: Vec<u8>,
        list_tags_body: Vec<u8>,
        describe_groups_body: Vec<u8>,
        put_events_body: Vec<u8>,
        describe_streams_body: Vec<u8>,
        get_headers: Vec<(String, String)>,
        get_body: Vec<u8>,
        filter_body: Vec<u8>,
    }

    fn cloudwatch_logs_round_trip_bodies(
        router: &EdgeRouter,
    ) -> CloudWatchLogsRoundTripBodies {
        let create_group = router.handle_bytes(&aws_json_request(
            "1.1",
            "Logs_20140328.CreateLogGroup",
            r#"{"logGroupName":"/app/demo","tags":{"env":"dev"}}"#,
        ));
        let duplicate_group = router.handle_bytes(&aws_json_request(
            "1.1",
            "Logs_20140328.CreateLogGroup",
            r#"{"logGroupName":"/app/demo"}"#,
        ));
        let create_stream = router.handle_bytes(&aws_json_request(
            "1.1",
            "Logs_20140328.CreateLogStream",
            r#"{"logGroupName":"/app/demo","logStreamName":"api"}"#,
        ));
        let put_retention = router.handle_bytes(&aws_json_request(
            "1.1",
            "Logs_20140328.PutRetentionPolicy",
            r#"{"logGroupName":"/app/demo","retentionInDays":7}"#,
        ));
        let tag_group = router.handle_bytes(&aws_json_request(
            "1.1",
            "Logs_20140328.TagLogGroup",
            r#"{"logGroupName":"/app/demo","tags":{"team":"platform"}}"#,
        ));
        let list_tags = router.handle_bytes(&aws_json_request(
            "1.1",
            "Logs_20140328.ListTagsLogGroup",
            r#"{"logGroupName":"/app/demo"}"#,
        ));
        let describe_groups = router.handle_bytes(&aws_json_request(
            "1.1",
            "Logs_20140328.DescribeLogGroups",
            r#"{"logGroupNamePrefix":"/app"}"#,
        ));
        let put_events = router.handle_bytes(&aws_json_request(
            "1.1",
            "Logs_20140328.PutLogEvents",
            r#"{"logGroupName":"/app/demo","logStreamName":"api","logEvents":[{"timestamp":1000,"message":"boot complete"},{"timestamp":2000,"message":"match this line"}]}"#,
        ));
        let describe_streams = router.handle_bytes(&aws_json_request(
            "1.1",
            "Logs_20140328.DescribeLogStreams",
            r#"{"logGroupName":"/app/demo","logStreamNamePrefix":"a"}"#,
        ));
        let get_events = router.handle_bytes(&aws_json_request(
            "1.1",
            "Logs_20140328.GetLogEvents",
            r#"{"logGroupName":"/app/demo","logStreamName":"api","startFromHead":true}"#,
        ));
        let filter_events = router.handle_bytes(&aws_json_request(
            "1.1",
            "Logs_20140328.FilterLogEvents",
            r#"{"logGroupName":"/app/demo","filterPattern":"match"}"#,
        ));

        let create_group_bytes = create_group.to_http_bytes();
        let duplicate_group_bytes = duplicate_group.to_http_bytes();
        let create_stream_bytes = create_stream.to_http_bytes();
        let put_retention_bytes = put_retention.to_http_bytes();
        let tag_group_bytes = tag_group.to_http_bytes();
        let list_tags_bytes = list_tags.to_http_bytes();
        let describe_groups_bytes = describe_groups.to_http_bytes();
        let put_events_bytes = put_events.to_http_bytes();
        let describe_streams_bytes = describe_streams.to_http_bytes();
        let get_events_bytes = get_events.to_http_bytes();
        let filter_events_bytes = filter_events.to_http_bytes();

        let (create_status, create_headers, create_body) =
            split_response(&create_group_bytes);
        let (duplicate_status, duplicate_headers, duplicate_body) =
            split_response(&duplicate_group_bytes);
        let (stream_status, _, stream_body) =
            split_response(&create_stream_bytes);
        let (retention_status, _, retention_body) =
            split_response(&put_retention_bytes);
        let (tag_status, _, tag_body) = split_response(&tag_group_bytes);
        let (_, _, list_tags_body) = split_response(&list_tags_bytes);
        let (_, _, describe_groups_body) =
            split_response(&describe_groups_bytes);
        let (_, _, put_events_body) = split_response(&put_events_bytes);
        let (_, _, describe_streams_body) =
            split_response(&describe_streams_bytes);
        let (_, get_headers, get_body) = split_response(&get_events_bytes);
        let (_, _, filter_body) = split_response(&filter_events_bytes);

        CloudWatchLogsRoundTripBodies {
            create_status: create_status.to_owned(),
            duplicate_status: duplicate_status.to_owned(),
            stream_status: stream_status.to_owned(),
            retention_status: retention_status.to_owned(),
            tag_status: tag_status.to_owned(),
            create_headers: create_headers
                .into_iter()
                .map(|(name, value)| (name.to_owned(), value.to_owned()))
                .collect(),
            duplicate_headers: duplicate_headers
                .into_iter()
                .map(|(name, value)| (name.to_owned(), value.to_owned()))
                .collect(),
            create_body: create_body.to_vec(),
            stream_body: stream_body.to_vec(),
            retention_body: retention_body.to_vec(),
            tag_body: tag_body.to_vec(),
            duplicate_body: duplicate_body.to_vec(),
            list_tags_body: list_tags_body.to_vec(),
            describe_groups_body: describe_groups_body.to_vec(),
            put_events_body: put_events_body.to_vec(),
            describe_streams_body: describe_streams_body.to_vec(),
            get_headers: get_headers
                .into_iter()
                .map(|(name, value)| (name.to_owned(), value.to_owned()))
                .collect(),
            get_body: get_body.to_vec(),
            filter_body: filter_body.to_vec(),
        }
    }

    fn assert_cloudwatch_log_write_statuses(
        bodies: &CloudWatchLogsRoundTripBodies,
    ) {
        assert_eq!(bodies.create_status, "HTTP/1.1 200 OK");
        assert_eq!(
            owned_header_value(&bodies.create_headers, "content-type"),
            Some("application/x-amz-json-1.1")
        );
        assert!(bodies.create_body.is_empty() || bodies.create_body == b"{}");
        assert_eq!(bodies.stream_status, "HTTP/1.1 200 OK");
        assert!(bodies.stream_body.is_empty() || bodies.stream_body == b"{}");
        assert_eq!(bodies.retention_status, "HTTP/1.1 200 OK");
        assert!(
            bodies.retention_body.is_empty() || bodies.retention_body == b"{}"
        );
        assert_eq!(bodies.tag_status, "HTTP/1.1 200 OK");
        assert!(bodies.tag_body.is_empty() || bodies.tag_body == b"{}");
    }

    fn assert_cloudwatch_log_duplicate_error(
        bodies: &CloudWatchLogsRoundTripBodies,
    ) {
        let duplicate_error: AwsJsonErrorBody = parse_json_slice(
            &bodies.duplicate_body,
            "duplicate log group error should decode",
        );
        assert_eq!(bodies.duplicate_status, "HTTP/1.1 400 Bad Request");
        assert_eq!(
            owned_header_value(&bodies.duplicate_headers, "content-type"),
            Some("application/x-amz-json-1.1")
        );
        assert_eq!(
            duplicate_error.error_type,
            "ResourceAlreadyExistsException"
        );
        assert!(duplicate_error.message.contains("/app/demo"));
    }

    fn assert_cloudwatch_log_query_bodies(
        bodies: &CloudWatchLogsRoundTripBodies,
    ) {
        let list_tags_body: Value = parse_json_slice(
            &bodies.list_tags_body,
            "tags response should decode",
        );
        let describe_groups_body: Value = parse_json_slice(
            &bodies.describe_groups_body,
            "describe groups response should decode",
        );
        let put_events_body: Value = parse_json_slice(
            &bodies.put_events_body,
            "put events response should decode",
        );
        let describe_streams_body: Value = parse_json_slice(
            &bodies.describe_streams_body,
            "describe streams response should decode",
        );
        let get_body: Value =
            parse_json_slice(&bodies.get_body, "get events should decode");
        let filter_body: Value = parse_json_slice(
            &bodies.filter_body,
            "filter events should decode",
        );

        let tags =
            json_field(&list_tags_body, "tags", "tags field should exist");
        assert_eq!(
            json_string(
                json_field(tags, "env", "env tag should exist"),
                "env tag should be a string",
            ),
            "dev"
        );
        assert_eq!(
            json_string(
                json_field(tags, "team", "team tag should exist"),
                "team tag should be a string",
            ),
            "platform"
        );
        let first_group = json_array_item(
            json_field(
                &describe_groups_body,
                "logGroups",
                "log groups field should exist",
            ),
            0,
            "first log group should exist",
        );
        assert_eq!(
            json_string(
                json_field(
                    first_group,
                    "logGroupName",
                    "log group name should exist"
                ),
                "log group name should be a string",
            ),
            "/app/demo"
        );
        assert_eq!(
            json_u64(
                json_field(
                    first_group,
                    "retentionInDays",
                    "retention should exist",
                ),
                "retention should be a u64",
            ),
            7
        );
        assert_eq!(
            json_string(
                json_field(
                    &put_events_body,
                    "nextSequenceToken",
                    "next sequence token should exist",
                ),
                "next sequence token should be a string",
            ),
            "2"
        );
        let first_stream = json_array_item(
            json_field(
                &describe_streams_body,
                "logStreams",
                "log streams field should exist",
            ),
            0,
            "first log stream should exist",
        );
        assert_eq!(
            json_string(
                json_field(
                    first_stream,
                    "logStreamName",
                    "log stream name should exist",
                ),
                "log stream name should be a string",
            ),
            "api"
        );
        assert_eq!(
            json_u64(
                json_field(
                    first_stream,
                    "firstEventTimestamp",
                    "first event timestamp should exist",
                ),
                "first event timestamp should be a u64",
            ),
            1000
        );
        assert_eq!(
            owned_header_value(&bodies.get_headers, "content-type"),
            Some("application/x-amz-json-1.1")
        );
        let get_events =
            json_field(&get_body, "events", "events field should exist");
        assert_eq!(
            get_events.as_array().expect("events should be an array").len(),
            2
        );
        assert_eq!(
            json_string(
                json_field(
                    json_array_item(get_events, 0, "first event should exist"),
                    "message",
                    "first event message should exist",
                ),
                "first event message should be a string",
            ),
            "boot complete"
        );
        let filtered_events =
            json_field(&filter_body, "events", "filtered events should exist");
        assert_eq!(
            filtered_events
                .as_array()
                .expect("filtered events should be an array")
                .len(),
            1
        );
        assert_eq!(
            json_string(
                json_field(
                    json_array_item(
                        filtered_events,
                        0,
                        "filtered event should exist",
                    ),
                    "message",
                    "filtered event message should exist",
                ),
                "filtered event message should be a string",
            ),
            "match this line"
        );
    }

    fn assert_cloudwatch_logs_round_trip(
        bodies: CloudWatchLogsRoundTripBodies,
    ) {
        assert_cloudwatch_log_write_statuses(&bodies);
        assert_cloudwatch_log_duplicate_error(&bodies);
        assert_cloudwatch_log_query_bodies(&bodies);
    }

    struct CloudWatchMetricDataBodies {
        query_status: String,
        json_status: String,
        cbor_status: String,
        query_headers: Vec<(String, String)>,
        json_headers: Vec<(String, String)>,
        cbor_headers: Vec<(String, String)>,
        query_body: String,
        json_body: Value,
        cbor_body: CborValue,
    }

    fn cloudwatch_metric_data_bodies(
        router: &EdgeRouter,
    ) -> CloudWatchMetricDataBodies {
        let put_metric_data = router.handle_bytes(&aws_json_request(
            "1.0",
            "GraniteServiceVersion20100801.PutMetricData",
            r#"{"Namespace":"Demo","MetricData":[{"MetricName":"Latency","Dimensions":[{"Name":"Service","Value":"api"}],"Timestamp":60,"Unit":"Count","Value":3.0},{"MetricName":"Latency","Dimensions":[{"Name":"Service","Value":"api"}],"Timestamp":120,"Unit":"Count","Value":5.0}]}"#,
        ));
        let put_metric_data_bytes = put_metric_data.to_http_bytes();
        let (put_status, _, put_body) = split_response(&put_metric_data_bytes);
        assert_eq!(put_status, "HTTP/1.1 200 OK");
        assert!(put_body.is_empty() || put_body == b"{}");

        let query_body = concat!(
            "Action=GetMetricData",
            "&Version=2010-08-01",
            "&StartTime=1970-01-01T00:00:00Z",
            "&EndTime=1970-01-01T00:03:00Z",
            "&ScanBy=TimestampAscending",
            "&MetricDataQueries.member.1.Id=m1",
            "&MetricDataQueries.member.1.MetricStat.Metric.Namespace=Demo",
            "&MetricDataQueries.member.1.MetricStat.Metric.MetricName=Latency",
            "&MetricDataQueries.member.1.MetricStat.Metric.Dimensions.member.1.Name=Service",
            "&MetricDataQueries.member.1.MetricStat.Metric.Dimensions.member.1.Value=api",
            "&MetricDataQueries.member.1.MetricStat.Period=60",
            "&MetricDataQueries.member.1.MetricStat.Stat=Average",
        );
        let query_response = router.handle_bytes(&query_request(query_body));
        let json_response = router.handle_bytes(&aws_json_request(
            "1.0",
            "GraniteServiceVersion20100801.GetMetricData",
            r#"{"StartTime":0,"EndTime":180,"ScanBy":"TimestampAscending","MetricDataQueries":[{"Id":"m1","MetricStat":{"Metric":{"Namespace":"Demo","MetricName":"Latency","Dimensions":[{"Name":"Service","Value":"api"}]},"Period":60,"Stat":"Average"}}]}"#,
        ));
        let cbor_response = router.handle_bytes(&smithy_cbor_request(
            "/service/GraniteServiceVersion20100801/operation/GetMetricData",
            &json!({
                "StartTime": 0,
                "EndTime": 180,
                "ScanBy": "TimestampAscending",
                "MetricDataQueries": [{
                    "Id": "m1",
                    "MetricStat": {
                        "Metric": {
                            "Namespace": "Demo",
                            "MetricName": "Latency",
                            "Dimensions": [{
                                "Name": "Service",
                                "Value": "api"
                            }]
                        },
                        "Period": 60,
                        "Stat": "Average"
                    }
                }]
            }),
        ));

        let query_response_bytes = query_response.to_http_bytes();
        let json_response_bytes = json_response.to_http_bytes();
        let cbor_response_bytes = cbor_response.to_http_bytes();

        let (query_status, query_headers, query_body) =
            split_response(&query_response_bytes);
        let (json_status, json_headers, json_body) =
            split_response(&json_response_bytes);
        let (cbor_status, cbor_headers, cbor_body) =
            split_response(&cbor_response_bytes);

        CloudWatchMetricDataBodies {
            query_status: query_status.to_owned(),
            json_status: json_status.to_owned(),
            cbor_status: cbor_status.to_owned(),
            query_headers: query_headers
                .into_iter()
                .map(|(name, value)| (name.to_owned(), value.to_owned()))
                .collect(),
            json_headers: json_headers
                .into_iter()
                .map(|(name, value)| (name.to_owned(), value.to_owned()))
                .collect(),
            cbor_headers: cbor_headers
                .into_iter()
                .map(|(name, value)| (name.to_owned(), value.to_owned()))
                .collect(),
            query_body: utf8_text(query_body, "query body should be UTF-8"),
            json_body: parse_json_slice(json_body, "JSON body should decode"),
            cbor_body: from_reader(cbor_body)
                .expect("CBOR body should decode"),
        }
    }

    fn assert_cloudwatch_metric_protocol_responses(
        bodies: &CloudWatchMetricDataBodies,
    ) {
        assert_eq!(bodies.query_status, "HTTP/1.1 200 OK");
        assert_eq!(bodies.json_status, "HTTP/1.1 200 OK");
        assert_eq!(bodies.cbor_status, "HTTP/1.1 200 OK");
        assert_eq!(
            owned_header_value(&bodies.query_headers, "content-type"),
            Some("text/xml")
        );
        assert_eq!(
            owned_header_value(&bodies.json_headers, "content-type"),
            Some("application/x-amz-json-1.0")
        );
        assert_eq!(
            owned_header_value(&bodies.cbor_headers, "content-type"),
            Some("application/cbor")
        );
        assert_eq!(
            owned_header_value(&bodies.cbor_headers, "smithy-protocol"),
            Some("rpc-v2-cbor")
        );
    }

    fn assert_cloudwatch_metric_query_body(
        bodies: &CloudWatchMetricDataBodies,
    ) {
        assert!(bodies.query_body.contains("<Id>m1</Id>"));
        assert!(bodies.query_body.contains("<Label>Latency</Label>"));
        assert!(
            bodies
                .query_body
                .contains("<member>1970-01-01T00:01:00Z</member>")
        );
        assert!(
            bodies
                .query_body
                .contains("<member>1970-01-01T00:02:00Z</member>")
        );
        assert!(bodies.query_body.contains("<member>3.0</member>"));
        assert!(bodies.query_body.contains("<member>5.0</member>"));
    }

    fn assert_cloudwatch_metric_json_body(
        bodies: &CloudWatchMetricDataBodies,
    ) {
        let results = json_field(
            &bodies.json_body,
            "MetricDataResults",
            "metric data results should exist",
        );
        let first_result =
            json_array_item(results, 0, "first metric result should exist");
        assert_eq!(
            json_string(
                json_field(
                    first_result,
                    "Id",
                    "metric result id should exist"
                ),
                "metric result id should be a string",
            ),
            "m1"
        );
        assert_eq!(
            json_string(
                json_field(
                    first_result,
                    "Label",
                    "metric result label should exist",
                ),
                "metric result label should be a string",
            ),
            "Latency"
        );
        assert_eq!(
            json_field(
                first_result,
                "Timestamps",
                "metric result timestamps should exist",
            )
            .clone(),
            json!([60.0, 120.0])
        );
        assert_eq!(
            json_field(
                first_result,
                "Values",
                "metric result values should exist"
            )
            .clone(),
            json!([3.0, 5.0])
        );
        assert_eq!(bodies.json_body, cbor_to_test_json(&bodies.cbor_body));
    }

    fn assert_cloudwatch_metric_cbor_body(
        bodies: &CloudWatchMetricDataBodies,
    ) {
        let results = cbor_map_field(&bodies.cbor_body, "MetricDataResults")
            .expect("MetricDataResults should exist");
        assert!(
            matches!(results, CborValue::Array(_)),
            "MetricDataResults should be an array"
        );
        let CborValue::Array(results) = results else {
            panic!("MetricDataResults should be an array");
        };
        let first_result =
            results.first().expect("first metric result should exist");
        let timestamps = cbor_map_field(first_result, "Timestamps")
            .expect("Timestamps should exist");
        assert!(
            matches!(timestamps, CborValue::Array(_)),
            "Timestamps should be an array"
        );
        let CborValue::Array(timestamps) = timestamps else {
            panic!("Timestamps should be an array");
        };
        assert!(matches!(
            timestamps.first().expect("first timestamp should exist"),
            CborValue::Tag(1, _)
        ));
        assert!(matches!(
            timestamps.get(1).expect("second timestamp should exist"),
            CborValue::Tag(1, _)
        ));
    }

    fn assert_cloudwatch_metric_data_bodies(
        bodies: CloudWatchMetricDataBodies,
    ) {
        assert_cloudwatch_metric_protocol_responses(&bodies);
        assert_cloudwatch_metric_query_body(&bodies);
        assert_cloudwatch_metric_json_body(&bodies);
        assert_cloudwatch_metric_cbor_body(&bodies);
    }

    #[test]
    fn apigw_v1_runtime_http_proxy_forwards_request_and_shapes_response() {
        let server = CapturingHttpServer::spawn(
            b"HTTP/1.1 201 Created\r\nContent-Type: text/plain\r\nX-Upstream: ok\r\nSet-Cookie: a=1\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok".to_vec(),
        );
        let (router, runtime) = router_with_runtime("apigw-v1-runtime-http");
        let api_id = deploy_execute_api(
            &runtime,
            "HTTP_PROXY",
            Some(format!(
                "http://127.0.0.1:{}/backend",
                server.address().port()
            )),
            BTreeMap::new(),
            false,
        );

        let response = router.handle_bytes(&reserved_execute_api_request(
            "GET",
            &api_id,
            "/dev/pets?mode=test",
            &[("X-Test", "true")],
            b"",
        ));
        let response_bytes = response.to_http_bytes();
        let (status, headers, body) = split_response(&response_bytes);
        let captured = server.join();

        assert_eq!(status, "HTTP/1.1 201 Created");
        assert_eq!(header_value(&headers, "content-type"), Some("text/plain"));
        assert_eq!(header_value(&headers, "x-upstream"), Some("ok"));
        assert_eq!(header_value(&headers, "set-cookie"), Some("a=1"));
        assert_eq!(body, b"ok");
        assert!(captured.starts_with("GET /backend?mode=test HTTP/1.1\r\n"));
        assert!(captured.contains("Host: 127.0.0.1:"));
        assert!(captured.contains("X-Test: true\r\n"));
    }

    #[test]
    fn apigw_v1_runtime_mock_and_custom_hosts_do_not_resolve_execute_api() {
        let (router, runtime) = router_with_runtime("apigw-v1-runtime-mock");
        let api_id = deploy_execute_api(
            &runtime,
            "MOCK",
            None,
            BTreeMap::from([(
                "application/json".to_owned(),
                r#"{"statusCode":202,"headers":{"x-mock":"ok"},"body":"ready"}"#
                    .to_owned(),
            )]),
            false,
        );

        let mock = router.handle_bytes(&reserved_execute_api_request(
            "GET",
            &api_id,
            "/dev/pets",
            &[("Content-Type", "application/json")],
            b"",
        ));
        let missing_stage =
            router.handle_bytes(&reserved_execute_api_request(
                "GET",
                &api_id,
                "/missing/pets",
                &[],
                b"",
            ));
        let custom_host = router.handle_bytes(&execute_api_request(
            "GET",
            "api.example.test",
            "/pets",
            &[],
            b"",
        ));

        let mock_bytes = mock.to_http_bytes();
        let missing_stage_bytes = missing_stage.to_http_bytes();
        let custom_host_bytes = custom_host.to_http_bytes();
        let (mock_status, mock_headers, mock_body) =
            split_response(&mock_bytes);
        let (missing_stage_status, _, missing_stage_body) =
            split_response(&missing_stage_bytes);
        let (custom_host_status, _, custom_host_body) =
            split_response(&custom_host_bytes);
        let missing_stage_body: Value =
            serde_json::from_slice(missing_stage_body)
                .expect("missing-stage body should decode");
        let custom_host_body: Value = serde_json::from_slice(custom_host_body)
            .expect("custom-host body should decode");

        assert_eq!(mock_status, "HTTP/1.1 202 Accepted");
        assert_eq!(header_value(&mock_headers, "x-mock"), Some("ok"));
        assert_eq!(mock_body, b"ready");
        assert_eq!(missing_stage_status, "HTTP/1.1 404 Not Found");
        assert_eq!(missing_stage_body["message"], "Not Found");
        assert_eq!(custom_host_status, "HTTP/1.1 404 Not Found");
        assert_eq!(custom_host_body["message"], "not found");
    }

    #[test]
    fn apigw_v1_runtime_legacy_execute_api_hosts_do_not_resolve() {
        let (router, runtime) =
            router_with_runtime("apigw-v1-runtime-legacy-host");
        let api_id = deploy_execute_api(
            &runtime,
            "MOCK",
            None,
            BTreeMap::from([(
                "application/json".to_owned(),
                r#"{"statusCode":202,"headers":{"x-mock":"ok"},"body":"ready"}"#
                    .to_owned(),
            )]),
            false,
        );

        let legacy_host =
            router.handle_bytes(&legacy_execute_api_host_request(
                "GET",
                &api_id,
                "/dev/pets",
                &[("Content-Type", "application/json")],
                b"",
            ));
        let legacy_host_bytes = legacy_host.to_http_bytes();
        let (legacy_host_status, _, legacy_host_body) =
            split_response(&legacy_host_bytes);
        let legacy_host_body: Value = serde_json::from_slice(legacy_host_body)
            .expect("legacy-host body should decode");

        assert_eq!(legacy_host_status, "HTTP/1.1 404 Not Found");
        assert_eq!(legacy_host_body["message"], "not found");
    }

    #[test]
    fn apigw_runtime_missing_reserved_api_ids_stay_on_the_execute_api_surface()
    {
        let (router, _) = router_with_runtime("apigw-runtime-missing-api");

        let missing = router.handle_bytes(&reserved_execute_api_request(
            "GET",
            "missing12345",
            "/dev/pets",
            &[],
            b"",
        ));
        let missing_bytes = missing.to_http_bytes();
        let (status, _, body) = split_response(&missing_bytes);
        let body: Value =
            serde_json::from_slice(body).expect("body should decode");

        assert_eq!(status, "HTTP/1.1 404 Not Found");
        assert_eq!(body["message"], "Not Found");
    }

    #[test]
    fn apigw_runtime_signed_requests_resolve_execute_api_hosts_outside_the_default_account()
     {
        let backend = CapturingHttpServer::spawn(
            b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nX-Upstream: scoped\r\nContent-Length: 6\r\nConnection: close\r\n\r\nscoped".to_vec(),
        );
        let (router, runtime) =
            router_with_runtime("apigw-runtime-cross-account-signed");
        let alternate_scope = alternate_apigateway_scope();
        let (access_key_id, secret_access_key) =
            create_access_key_for_scope(&runtime, &alternate_scope, "scoped");
        let api_id = deploy_execute_api_in_scope(
            &runtime,
            &alternate_scope,
            "HTTP_PROXY",
            Some(format!(
                "http://127.0.0.1:{}/backend",
                backend.address().port()
            )),
            BTreeMap::new(),
            false,
        );

        let response =
            router.handle_bytes(&signed_reserved_execute_api_request(
                &api_id,
                "/dev/pets",
                &access_key_id,
                &secret_access_key,
                "eu-west-2",
            ));
        let response_bytes = response.to_http_bytes();
        let (status, headers, body) = split_response(&response_bytes);
        let captured = backend.join();

        assert_eq!(status, "HTTP/1.1 200 OK");
        assert_eq!(header_value(&headers, "x-upstream"), Some("scoped"));
        assert_eq!(body, b"scoped");
        assert!(captured.starts_with("GET /backend HTTP/1.1\r\n"));
    }

    #[test]
    fn apigw_runtime_unsigned_requests_resolve_execute_api_hosts_outside_the_default_account()
     {
        let backend = CapturingHttpServer::spawn(
            b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nX-Upstream: public\r\nContent-Length: 6\r\nConnection: close\r\n\r\npublic".to_vec(),
        );
        let (router, runtime) =
            router_with_runtime("apigw-runtime-cross-account-unsigned");
        let alternate_scope = alternate_apigateway_scope();
        let api_id = deploy_execute_api_in_scope(
            &runtime,
            &alternate_scope,
            "HTTP_PROXY",
            Some(format!(
                "http://127.0.0.1:{}/backend",
                backend.address().port()
            )),
            BTreeMap::new(),
            false,
        );

        let response = router.handle_bytes(&reserved_execute_api_request(
            "GET",
            &api_id,
            "/dev/pets",
            &[],
            b"",
        ));
        let response_bytes = response.to_http_bytes();
        let (status, headers, body) = split_response(&response_bytes);
        let captured = backend.join();

        assert_eq!(status, "HTTP/1.1 200 OK");
        assert_eq!(header_value(&headers, "x-upstream"), Some("public"));
        assert_eq!(body, b"public");
        assert!(captured.starts_with("GET /backend HTTP/1.1\r\n"));
    }

    #[test]
    fn apigw_v2_runtime_route_precedence_and_default_stage_forward_requests() {
        let exact_server = CapturingHttpServer::spawn(
            b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nX-Upstream: exact\r\nContent-Length: 5\r\nConnection: close\r\n\r\nexact".to_vec(),
        );
        let greedy_server = CapturingHttpServer::spawn(
            b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nX-Upstream: greedy\r\nContent-Length: 6\r\nConnection: close\r\n\r\ngreedy".to_vec(),
        );
        let default_server = CapturingHttpServer::spawn(
            b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nX-Upstream: default\r\nContent-Length: 7\r\nConnection: close\r\n\r\ndefault".to_vec(),
        );
        let (router, runtime) = router_with_runtime("apigw-v2-runtime-http");
        let api_id = deploy_http_api_v2(
            &runtime,
            format!(
                "http://127.0.0.1:{}/exact",
                exact_server.address().port()
            ),
            format!(
                "http://127.0.0.1:{}/greedy/{{proxy}}",
                greedy_server.address().port()
            ),
            format!("http://127.0.0.1:{}", default_server.address().port()),
        );

        let exact = router.handle_bytes(&reserved_execute_api_request(
            "GET",
            &api_id,
            "/pets/dog/1",
            &[],
            b"",
        ));
        let greedy = router.handle_bytes(&reserved_execute_api_request(
            "GET",
            &api_id,
            "/pets/cat/2?view=full",
            &[("X-Test", "true")],
            b"",
        ));
        let fallback = router.handle_bytes(&reserved_execute_api_request(
            "POST",
            &api_id,
            "/orders/5",
            &[],
            br#"{"ok":true}"#,
        ));

        let exact_bytes = exact.to_http_bytes();
        let (status, headers, body) = split_response(&exact_bytes);
        assert_eq!(status, "HTTP/1.1 200 OK");
        assert_eq!(header_value(&headers, "x-upstream"), Some("exact"));
        assert_eq!(body, b"exact");

        let greedy_bytes = greedy.to_http_bytes();
        let (status, headers, body) = split_response(&greedy_bytes);
        assert_eq!(status, "HTTP/1.1 200 OK");
        assert_eq!(header_value(&headers, "x-upstream"), Some("greedy"));
        assert_eq!(body, b"greedy");

        let fallback_bytes = fallback.to_http_bytes();
        let (status, headers, body) = split_response(&fallback_bytes);
        assert_eq!(status, "HTTP/1.1 200 OK");
        assert_eq!(header_value(&headers, "x-upstream"), Some("default"));
        assert_eq!(body, b"default");

        let exact_request = exact_server.join();
        let greedy_request = greedy_server.join();
        let default_request = default_server.join();

        assert!(
            exact_request.starts_with("GET /exact/pets/dog/1 HTTP/1.1\r\n")
        );
        assert!(
            greedy_request
                .starts_with("GET /greedy/cat%2F2?view=full HTTP/1.1\r\n")
        );
        assert!(greedy_request.contains("X-Test: true\r\n"));
        assert!(default_request.starts_with("POST /orders/5 HTTP/1.1\r\n"));
        assert!(default_request.contains(r#"{"ok":true}"#));
    }

    #[test]
    fn apigw_v1_runtime_lambda_proxy_requires_permission_and_shapes_event() {
        let executor = Arc::new(RecordingLambdaExecutor::default());
        let (router, runtime) = router_with_lambda_executor(
            "apigw-v1-runtime-lambda",
            executor.clone(),
        );
        let lambda_scope = LambdaScope::new(
            "000000000000".parse().expect("account should parse"),
            "eu-west-2".parse().expect("region should parse"),
        );
        runtime
            .lambda()
            .create_function(
                &lambda_scope,
                CreateFunctionInput {
                    code: LambdaCodeInput::InlineZip {
                        archive: b"ignored".to_vec(),
                    },
                    dead_letter_target_arn: None,
                    description: None,
                    environment: BTreeMap::new(),
                    function_name: "pet-handler".to_owned(),
                    handler: Some("bootstrap.handler".to_owned()),
                    memory_size: None,
                    package_type: LambdaPackageType::Zip,
                    publish: false,
                    role: "arn:aws:iam::000000000000:role/lambda-role"
                        .to_owned(),
                    runtime: Some("provided.al2".to_owned()),
                    timeout: None,
                },
            )
            .expect("lambda should create");
        let api_id = deploy_execute_api(
            &runtime,
            "AWS_PROXY",
            Some(lambda_integration_uri("pet-handler")),
            BTreeMap::new(),
            false,
        );
        let request = reserved_execute_api_request(
            "GET",
            &api_id,
            "/dev/pets?view=full",
            &[("User-Agent", "runtime-test"), ("X-Test", "true")],
            b"",
        );

        let denied = router.handle_bytes(&request);
        let denied_bytes = denied.to_http_bytes();
        let (denied_status, _, denied_body) = split_response(&denied_bytes);
        let denied_body: Value = serde_json::from_slice(denied_body)
            .expect("denied body should decode");

        assert_eq!(denied_status, "HTTP/1.1 500 Internal Server Error");
        assert_eq!(denied_body["message"], "Internal server error");
        assert!(executor.requests().is_empty());

        runtime
            .lambda()
            .add_permission(
                &lambda_scope,
                "pet-handler",
                None,
                AddLambdaPermissionInput {
                    action: "lambda:InvokeFunction".to_owned(),
                    function_url_auth_type: None,
                    principal: "apigateway.amazonaws.com".to_owned(),
                    revision_id: None,
                    source_arn: Some(format!(
                        "arn:aws:execute-api:eu-west-2:000000000000:{api_id}/dev/GET/pets"
                    )),
                    statement_id: "allow-apigw".to_owned(),
                },
            )
            .expect("permission should be added");

        let success = router.handle_bytes(&request);
        let success_bytes = success.to_http_bytes();
        let (success_status, success_headers, success_body) =
            split_response(&success_bytes);
        let event: Value = serde_json::from_slice(success_body)
            .expect("lambda proxy event should decode");
        let requests = executor.requests();

        assert_eq!(success_status, "HTTP/1.1 200 OK");
        assert_eq!(
            header_value(&success_headers, "x-apigw-runtime"),
            Some("lambda-proxy")
        );
        assert_eq!(requests.len(), 1);
        assert_eq!(event["resource"], "/pets");
        assert_eq!(event["path"], "/pets");
        assert_eq!(event["httpMethod"], "GET");
        assert_eq!(event["headers"]["x-test"], "true");
        assert_eq!(event["queryStringParameters"]["view"], "full");
        assert_eq!(event["requestContext"]["stage"], "dev");
        assert_eq!(event["requestContext"]["path"], "/dev/pets");
    }

    #[test]
    fn lambda_url_runtime_public_hosts_resolve_the_owning_scope() {
        let executor = Arc::new(RecordingLambdaExecutor::default());
        let (router, runtime) = router_with_lambda_executor(
            "lambda-url-runtime-cross-account",
            executor,
        );
        let alternate_scope = alternate_lambda_scope();
        ensure_lambda_role(&runtime, &alternate_scope);
        runtime
            .lambda()
            .create_function(
                &alternate_scope,
                CreateFunctionInput {
                    code: LambdaCodeInput::InlineZip {
                        archive: provided_bootstrap_zip(),
                    },
                    dead_letter_target_arn: None,
                    description: None,
                    environment: BTreeMap::new(),
                    function_name: "public-url".to_owned(),
                    handler: Some("bootstrap.handler".to_owned()),
                    memory_size: None,
                    package_type: LambdaPackageType::Zip,
                    publish: false,
                    role: role_arn_for(&alternate_scope),
                    runtime: Some("provided.al2".to_owned()),
                    timeout: None,
                },
            )
            .expect("lambda should create");
        let url_config = runtime
            .lambda()
            .create_function_url_config(
                &alternate_scope,
                "public-url",
                None,
                CreateFunctionUrlConfigInput {
                    auth_type: LambdaFunctionUrlAuthType::None,
                    invoke_mode: LambdaFunctionUrlInvokeMode::Buffered,
                },
            )
            .expect("function URL should create");
        runtime
            .lambda()
            .add_permission(
                &alternate_scope,
                "public-url",
                None,
                AddLambdaPermissionInput {
                    action: "lambda:InvokeFunctionUrl".to_owned(),
                    function_url_auth_type: Some(
                        LambdaFunctionUrlAuthType::None,
                    ),
                    principal: "*".to_owned(),
                    revision_id: None,
                    source_arn: None,
                    statement_id: "allow-public".to_owned(),
                },
            )
            .expect("function URL permission should create");

        let response = router.handle_bytes(&execute_api_request(
            "POST",
            &format!(
                "{}.lambda-url.{}.localhost",
                url_config.url_id(),
                alternate_scope.region().as_str(),
            ),
            "/custom/path?mode=test",
            &[],
            br#"{"hello":"world"}"#,
        ));
        let response_bytes = response.to_http_bytes();
        let (status, _, body) = split_response(&response_bytes);
        let event: Value = serde_json::from_slice(body)
            .expect("function URL body should decode");

        assert_eq!(status, "HTTP/1.1 200 OK");
        assert_eq!(event["rawPath"], "/custom/path");
        assert_eq!(event["rawQueryString"], "mode=test");
        assert_eq!(event["requestContext"]["accountId"], "111111111111");
        assert_eq!(
            event["requestContext"]["domainPrefix"],
            url_config.url_id()
        );
    }

    #[test]
    fn runtime_health_and_status_endpoints_return_internal_json() {
        let health = router().handle_bytes(
            b"GET /__cloudish/health HTTP/1.1\r\nHost: localhost\r\n\r\n",
        );
        let status = router().handle_bytes(
            b"GET /__cloudish/status HTTP/1.1\r\nHost: localhost\r\n\r\n",
        );
        let health_bytes = health.to_http_bytes();
        let status_bytes = status.to_http_bytes();

        let (health_status, health_headers, health_body) =
            split_response(&health_bytes);
        let (_, status_headers, status_body) = split_response(&status_bytes);
        let health_json: Value = serde_json::from_slice(health_body)
            .expect("health response should be valid JSON");
        let status_json: Value = serde_json::from_slice(status_body)
            .expect("status response should be valid JSON");

        assert_eq!(health_status, "HTTP/1.1 200 OK");
        assert_eq!(
            header_value(&health_headers, "content-type"),
            Some("application/json")
        );
        parse_http_date(
            header_value(&health_headers, "date").expect("Date header"),
        )
        .expect("Date header should use an HTTP-compatible format");
        parse_http_date(
            header_value(&status_headers, "date").expect("Date header"),
        )
        .expect("Date header should use an HTTP-compatible format");
        assert_eq!(health_json["status"], "ok");
        assert_eq!(
            health_json.as_object().map(|object| object.len()),
            Some(1)
        );
        assert_eq!(status_json["defaultAccount"], "000000000000");
        assert_eq!(status_json["defaultRegion"], "eu-west-2");
        assert!(status_json["stateDirectory"].is_string());
        assert!(status_json.get("status").is_none());
        assert!(status_json.get("ready").is_none());
        assert!(health_json.get("defaultAccount").is_none());
        assert!(health_json.get("defaultRegion").is_none());
    }

    #[test]
    fn runtime_sns_signing_cert_route_serves_the_embedded_pem() {
        let response = router().handle_bytes(
            b"GET /__aws/sns/signing-cert.pem HTTP/1.1\r\nHost: localhost\r\n\r\n",
        );
        let response_bytes = response.to_http_bytes();
        let (status, headers, body) = split_response(&response_bytes);

        assert_eq!(status, "HTTP/1.1 200 OK");
        assert_eq!(
            header_value(&headers, "content-type"),
            Some("application/x-pem-file")
        );
        assert_eq!(body, edge_runtime::cloudish_sns_signing_cert_pem());
    }

    #[test]
    fn runtime_sns_signing_cert_route_rejects_non_get_methods() {
        let response = router().handle_bytes(
            b"POST /__aws/sns/signing-cert.pem HTTP/1.1\r\nHost: localhost\r\n\r\n",
        );
        let response_bytes = response.to_http_bytes();
        let (status, headers, body) = split_response(&response_bytes);
        let body: Value = serde_json::from_slice(body)
            .expect("error body should be valid JSON");

        assert_eq!(status, "HTTP/1.1 405 Method Not Allowed");
        assert_eq!(header_value(&headers, "allow"), Some("GET"));
        assert_eq!(body["message"], "method not allowed");
    }

    #[test]
    fn runtime_internal_endpoints_reject_non_get_methods() {
        let response = router().handle_bytes(
            b"POST /__cloudish/status HTTP/1.1\r\nHost: localhost\r\n\r\n",
        );
        let response_bytes = response.to_http_bytes();
        let (status, headers, body) = split_response(&response_bytes);
        let body: Value = serde_json::from_slice(body)
            .expect("status body should be valid JSON");

        assert_eq!(status, "HTTP/1.1 405 Method Not Allowed");
        assert_eq!(header_value(&headers, "allow"), Some("GET"));
        assert_eq!(body["message"], "method not allowed");
    }

    #[test]
    fn lambda_url_runtime_reserved_path_invocation_round_trips_plain_http() {
        let router = router();
        let create_function_body = json!({
            "Code": {
                "ZipFile": PROVIDED_BOOTSTRAP_ZIP_BASE64,
            },
            "FunctionName": "demo",
            "Handler": "bootstrap.handler",
            "Role": "arn:aws:iam::000000000000:role/service-role/lambda-role",
            "Runtime": "provided.al2",
        })
        .to_string();
        let create_function = router.handle_bytes(&json_request(
            "POST",
            "/2015-03-31/functions",
            "localhost:4566",
            &create_function_body,
        ));
        let create_function_bytes = create_function.to_http_bytes();
        let (create_status, _, _) = split_response(&create_function_bytes);
        assert_eq!(create_status, "HTTP/1.1 201 Created");

        let create_url = router.handle_bytes(&json_request(
            "POST",
            "/2021-10-31/functions/demo/url",
            "localhost:4566",
            r#"{"AuthType":"NONE"}"#,
        ));
        let create_url_bytes = create_url.to_http_bytes();
        let (_, _, create_url_body) = split_response(&create_url_bytes);
        let create_url_json: Value =
            serde_json::from_slice(create_url_body).unwrap();
        let function_url = create_url_json["FunctionUrl"]
            .as_str()
            .expect("function URL should be returned")
            .to_owned();
        let function_url_path = function_url
            .trim_start_matches("http://localhost:4566")
            .trim_end_matches('/')
            .to_owned();

        let add_permission = router.handle_bytes(&json_request(
            "POST",
            "/2015-03-31/functions/demo/policy",
            "localhost:4566",
            r#"{"Action":"lambda:InvokeFunctionUrl","FunctionUrlAuthType":"NONE","Principal":"*","StatementId":"allow-public"}"#,
        ));
        let add_permission_bytes = add_permission.to_http_bytes();
        let (permission_status, _, _) = split_response(&add_permission_bytes);
        assert_eq!(permission_status, "HTTP/1.1 201 Created");

        let invoke_request = format!(
            "POST {function_url_path}/custom/path?mode=test HTTP/1.1\r\nHost: localhost:4566\r\nContent-Type: application/json\r\nCookie: theme=light\r\nContent-Length: 17\r\n\r\n{{\"hello\":\"world\"}}"
        )
        .into_bytes();
        let invoke = router.handle_bytes(&invoke_request);
        let invoke_bytes = invoke.to_http_bytes();
        let (invoke_status, headers, body) = split_response(&invoke_bytes);
        let event: Value = serde_json::from_slice(body).unwrap();

        assert_eq!(invoke_status, "HTTP/1.1 200 OK");
        assert_eq!(
            header_value(&headers, "content-type"),
            Some("application/json")
        );
        assert_eq!(event["version"], "2.0");
        assert_eq!(event["rawPath"], "/custom/path");
        assert_eq!(event["rawQueryString"], "mode=test");
        assert_eq!(event["cookies"][0], "theme=light");
        assert_eq!(event["requestContext"]["domainName"], "localhost:4566");
        assert_eq!(event["queryStringParameters"]["mode"], "test");
    }

    #[test]
    fn cognito_well_known_route_requires_exactly_one_leading_pool_id_segment()
    {
        assert!(matches!(
            cognito_well_known_route(
                "/pool-id/.well-known/openid-configuration"
            ),
            Some(("pool-id", CognitoWellKnownDocument::OpenIdConfiguration,))
        ));
        assert!(matches!(
            cognito_well_known_route("/pool-id/.well-known/jwks.json"),
            Some(("pool-id", CognitoWellKnownDocument::Jwks))
        ));
        assert_eq!(
            cognito_well_known_route("/a/b/.well-known/jwks.json"),
            None
        );
        assert_eq!(
            cognito_well_known_route("//.well-known/openid-configuration"),
            None
        );
        assert_eq!(
            cognito_well_known_route("/orders/.well-known/not-cognito.json"),
            None
        );
    }

    #[test]
    fn s3_core_rest_xml_path_style_object_flow_round_trips() {
        let router = router();

        let create = router
            .handle_bytes(&s3_create_bucket_request("/demo", "localhost"));
        let put = router.handle_bytes(
            b"PUT /demo/reports/data.txt HTTP/1.1\r\nHost: localhost\r\nContent-Type: text/plain\r\nx-amz-meta-trace: abc123\r\nContent-Length: 7\r\n\r\npayload",
        );
        let head = router.handle_bytes(
            b"HEAD /demo/reports/data.txt HTTP/1.1\r\nHost: localhost\r\n\r\n",
        );
        let get = router.handle_bytes(
            b"GET /demo/reports/data.txt HTTP/1.1\r\nHost: localhost\r\n\r\n",
        );
        let copy = router.handle_bytes(
            b"PUT /demo/reports/data-copy.txt HTTP/1.1\r\nHost: localhost\r\nx-amz-copy-source: /demo/reports/data.txt\r\nContent-Length: 0\r\n\r\n",
        );
        let list = router.handle_bytes(
            b"GET /demo?list-type=2&prefix=reports/ HTTP/1.1\r\nHost: localhost\r\n\r\n",
        );

        let create_bytes = create.to_http_bytes();
        let put_bytes = put.to_http_bytes();
        let head_bytes = head.to_http_bytes();
        let get_bytes = get.to_http_bytes();
        let copy_bytes = copy.to_http_bytes();
        let list_bytes = list.to_http_bytes();

        let (create_status, create_headers, create_body) =
            split_response(&create_bytes);
        let (put_status, put_headers, put_body) = split_response(&put_bytes);
        let (head_status, head_headers, head_body) =
            split_response(&head_bytes);
        let (get_status, get_headers, get_body) = split_response(&get_bytes);
        let (copy_status, _, copy_body) = split_response(&copy_bytes);
        let (list_status, _, list_body) = split_response(&list_bytes);

        assert_eq!(create_status, "HTTP/1.1 200 OK");
        assert_eq!(header_value(&create_headers, "location"), Some("/demo"));
        assert!(create_body.is_empty());
        assert_eq!(put_status, "HTTP/1.1 200 OK");
        assert!(put_body.is_empty());
        assert!(header_value(&put_headers, "etag").is_some());
        assert_eq!(head_status, "HTTP/1.1 200 OK");
        assert_eq!(
            header_value(&head_headers, "content-type"),
            Some("text/plain")
        );
        assert_eq!(header_value(&head_headers, "content-length"), Some("7"));
        assert_eq!(
            header_value(&head_headers, "x-amz-meta-trace"),
            Some("abc123")
        );
        assert!(head_body.is_empty());
        assert_eq!(get_status, "HTTP/1.1 200 OK");
        assert_eq!(
            header_value(&get_headers, "etag"),
            header_value(&head_headers, "etag")
        );
        assert_eq!(get_body, b"payload");
        assert_eq!(copy_status, "HTTP/1.1 200 OK");
        assert!(
            std::str::from_utf8(copy_body)
                .expect("copy body should be UTF-8")
                .contains("<CopyObjectResult")
        );
        assert_eq!(list_status, "HTTP/1.1 200 OK");
        let list_body =
            std::str::from_utf8(list_body).expect("list body should be UTF-8");
        assert!(list_body.contains("<Key>reports/data.txt</Key>"));
        assert!(list_body.contains("<Key>reports/data-copy.txt</Key>"));
    }

    #[test]
    fn s3_core_rest_xml_put_object_ignores_jsonish_object_content_types() {
        let router = router();

        let create = router.handle_bytes(&s3_create_bucket_request(
            "/jsonish-content",
            "localhost",
        ));
        let put = router.handle_bytes(
            b"PUT /jsonish-content/object.bin HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/x-amz-json-1.0\r\nContent-Length: 7\r\n\r\npayload",
        );
        let head = router.handle_bytes(
            b"HEAD /jsonish-content/object.bin HTTP/1.1\r\nHost: localhost\r\n\r\n",
        );
        let get = router.handle_bytes(
            b"GET /jsonish-content/object.bin HTTP/1.1\r\nHost: localhost\r\n\r\n",
        );

        let create_bytes = create.to_http_bytes();
        let put_bytes = put.to_http_bytes();
        let head_bytes = head.to_http_bytes();
        let get_bytes = get.to_http_bytes();

        let (create_status, _, _) = split_response(&create_bytes);
        let (put_status, _, put_body) = split_response(&put_bytes);
        let (head_status, head_headers, _) = split_response(&head_bytes);
        let (get_status, get_headers, get_body) = split_response(&get_bytes);

        assert_eq!(create_status, "HTTP/1.1 200 OK");
        assert_eq!(put_status, "HTTP/1.1 200 OK");
        assert!(put_body.is_empty());
        assert_eq!(head_status, "HTTP/1.1 200 OK");
        assert_eq!(
            header_value(&head_headers, "content-type"),
            Some("application/x-amz-json-1.0")
        );
        assert_eq!(get_status, "HTTP/1.1 200 OK");
        assert_eq!(
            header_value(&get_headers, "content-type"),
            Some("application/x-amz-json-1.0")
        );
        assert_eq!(get_body, b"payload");
    }

    #[test]
    fn s3_core_rest_xml_put_object_ignores_generic_cbor_content_type() {
        let router = router();

        let create = router.handle_bytes(&s3_create_bucket_request(
            "/cborish-content",
            "localhost",
        ));
        let put = router.handle_bytes(
            b"PUT /cborish-content/object.bin HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/cbor\r\nContent-Length: 7\r\n\r\npayload",
        );
        let head = router.handle_bytes(
            b"HEAD /cborish-content/object.bin HTTP/1.1\r\nHost: localhost\r\n\r\n",
        );
        let get = router.handle_bytes(
            b"GET /cborish-content/object.bin HTTP/1.1\r\nHost: localhost\r\n\r\n",
        );

        let create_bytes = create.to_http_bytes();
        let put_bytes = put.to_http_bytes();
        let head_bytes = head.to_http_bytes();
        let get_bytes = get.to_http_bytes();
        let (create_status, _, _) = split_response(&create_bytes);
        let (put_status, _, put_body) = split_response(&put_bytes);
        let (head_status, head_headers, _) = split_response(&head_bytes);
        let (get_status, get_headers, get_body) = split_response(&get_bytes);

        assert_eq!(create_status, "HTTP/1.1 200 OK");
        assert_eq!(put_status, "HTTP/1.1 200 OK");
        assert!(put_body.is_empty());
        assert_eq!(head_status, "HTTP/1.1 200 OK");
        assert_eq!(
            header_value(&head_headers, "content-type"),
            Some("application/cbor")
        );
        assert_eq!(get_status, "HTTP/1.1 200 OK");
        assert_eq!(
            header_value(&get_headers, "content-type"),
            Some("application/cbor")
        );
        assert_eq!(get_body, b"payload");
    }

    #[test]
    fn s3_core_rest_xml_virtual_host_requests_fail_explicitly() {
        let router = router();

        let create = router.handle_bytes(&s3_create_bucket_request(
            "/",
            "virtual-bucket.s3.localhost",
        ));
        let get = router.handle_bytes(
            b"GET /missing.txt HTTP/1.1\r\nHost: virtual-bucket.s3.localhost\r\n\r\n",
        );

        let create_bytes = create.to_http_bytes();
        let get_bytes = get.to_http_bytes();
        let (create_status, _, _) = split_response(&create_bytes);
        let (get_status, _, get_body) = split_response(&get_bytes);

        assert_eq!(create_status, "HTTP/1.1 400 Bad Request");
        assert_eq!(get_status, "HTTP/1.1 400 Bad Request");
        assert!(
            std::str::from_utf8(get_body)
                .expect("body should be UTF-8")
                .contains("<Code>InvalidRequest</Code>")
        );
        assert!(
            std::str::from_utf8(get_body)
                .expect("body should be UTF-8")
                .contains("path-style URLs")
        );
    }

    #[test]
    fn s3_advanced_rest_xml_versioning_delete_markers_and_tagging_work() {
        let router = router();

        let create = router.handle_bytes(&s3_create_bucket_request(
            "/advanced-demo",
            "localhost",
        ));
        let enable_versioning = router.handle_bytes(
            b"PUT /advanced-demo?versioning HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/xml\r\nContent-Length: 132\r\n\r\n<VersioningConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Status>Enabled</Status></VersioningConfiguration>",
        );
        let put_v1 = router.handle_bytes(
            b"PUT /advanced-demo/reports/data.txt HTTP/1.1\r\nHost: localhost\r\nx-amz-tagging: env=dev&team=platform\r\nContent-Length: 7\r\n\r\nversion1",
        );
        let put_v2 = router.handle_bytes(
            b"PUT /advanced-demo/reports/data.txt HTTP/1.1\r\nHost: localhost\r\nx-amz-tagging: env=prod&team=platform\r\nContent-Length: 7\r\n\r\nversion2",
        );
        let delete_current = router.handle_bytes(
            b"DELETE /advanced-demo/reports/data.txt HTTP/1.1\r\nHost: localhost\r\n\r\n",
        );

        let create_bytes = create.to_http_bytes();
        let enable_bytes = enable_versioning.to_http_bytes();
        let put_v1_bytes = put_v1.to_http_bytes();
        let put_v2_bytes = put_v2.to_http_bytes();
        let delete_bytes = delete_current.to_http_bytes();

        let (create_status, _, _) = split_response(&create_bytes);
        let (enable_status, _, _) = split_response(&enable_bytes);
        let (put_v1_status, put_v1_headers, _) = split_response(&put_v1_bytes);
        let (put_v2_status, put_v2_headers, _) = split_response(&put_v2_bytes);
        let (delete_status, delete_headers, _) = split_response(&delete_bytes);
        let version_id_v1 = header_value(&put_v1_headers, "x-amz-version-id")
            .expect("first write should return a version id")
            .to_owned();
        let version_id_v2 = header_value(&put_v2_headers, "x-amz-version-id")
            .expect("second write should return a version id")
            .to_owned();
        let delete_marker_version =
            header_value(&delete_headers, "x-amz-version-id")
                .expect("delete marker should return a version id")
                .to_owned();

        let get_current = router.handle_bytes(
            b"GET /advanced-demo/reports/data.txt HTTP/1.1\r\nHost: localhost\r\n\r\n",
        );
        let get_v1 = router.handle_bytes(
            format!(
                "GET /advanced-demo/reports/data.txt?versionId={version_id_v1} HTTP/1.1\r\nHost: localhost\r\n\r\n"
            )
            .as_bytes(),
        );
        let get_v2_tags = router.handle_bytes(
            format!(
                "GET /advanced-demo/reports/data.txt?tagging&versionId={version_id_v2} HTTP/1.1\r\nHost: localhost\r\n\r\n"
            )
            .as_bytes(),
        );
        let list_versions = router.handle_bytes(
            b"GET /advanced-demo?versions&prefix=reports/ HTTP/1.1\r\nHost: localhost\r\n\r\n",
        );

        let get_current_bytes = get_current.to_http_bytes();
        let get_v1_bytes = get_v1.to_http_bytes();
        let get_v2_tags_bytes = get_v2_tags.to_http_bytes();
        let list_versions_bytes = list_versions.to_http_bytes();

        let (get_current_status, _, get_current_body) =
            split_response(&get_current_bytes);
        let (get_v1_status, get_v1_headers, get_v1_body) =
            split_response(&get_v1_bytes);
        let (get_v2_tags_status, get_v2_tags_headers, get_v2_tags_body) =
            split_response(&get_v2_tags_bytes);
        let (list_versions_status, _, list_versions_body) =
            split_response(&list_versions_bytes);
        let get_current_body = std::str::from_utf8(get_current_body)
            .expect("body should be UTF-8");
        let get_v2_tags_body = std::str::from_utf8(get_v2_tags_body)
            .expect("tagging body should be UTF-8");
        let list_versions_body = std::str::from_utf8(list_versions_body)
            .expect("versions body should be UTF-8");

        assert_eq!(create_status, "HTTP/1.1 200 OK");
        assert_eq!(enable_status, "HTTP/1.1 200 OK");
        assert_eq!(put_v1_status, "HTTP/1.1 200 OK");
        assert_eq!(put_v2_status, "HTTP/1.1 200 OK");
        assert_eq!(delete_status, "HTTP/1.1 204 No Content");
        assert_eq!(
            header_value(&delete_headers, "x-amz-delete-marker"),
            Some("true")
        );
        assert_ne!(version_id_v1, version_id_v2);
        assert_ne!(version_id_v2, delete_marker_version);
        assert_eq!(get_current_status, "HTTP/1.1 404 Not Found");
        assert!(get_current_body.contains("<Code>NoSuchKey</Code>"));
        assert_eq!(get_v1_status, "HTTP/1.1 200 OK");
        assert_eq!(
            header_value(&get_v1_headers, "x-amz-version-id"),
            Some(version_id_v1.as_str())
        );
        assert_eq!(get_v1_body, b"version1");
        assert_eq!(get_v2_tags_status, "HTTP/1.1 200 OK");
        assert_eq!(
            header_value(&get_v2_tags_headers, "x-amz-version-id"),
            Some(version_id_v2.as_str())
        );
        assert!(get_v2_tags_body.contains("<Key>env</Key>"));
        assert!(get_v2_tags_body.contains("<Value>prod</Value>"));
        assert_eq!(list_versions_status, "HTTP/1.1 200 OK");
        assert!(list_versions_body.contains("<ListVersionsResult"));
        assert!(
            list_versions_body
                .contains(&format!("<VersionId>{version_id_v1}</VersionId>"))
        );
        assert!(
            list_versions_body
                .contains(&format!("<VersionId>{version_id_v2}</VersionId>"))
        );
        assert!(list_versions_body.contains(&format!(
            "<VersionId>{delete_marker_version}</VersionId>"
        )));
        assert!(list_versions_body.contains("<DeleteMarker>"));
    }

    #[test]
    fn s3_advanced_rest_xml_multipart_lifecycle_and_validation_errors_work() {
        let router = router();

        let create = router.handle_bytes(&s3_create_bucket_request(
            "/multipart-demo",
            "localhost",
        ));
        let initiate = router.handle_bytes(
            b"POST /multipart-demo/archive.bin?uploads HTTP/1.1\r\nHost: localhost\r\nx-amz-tagging: stage=parts\r\nContent-Length: 0\r\n\r\n",
        );
        let create_bytes = create.to_http_bytes();
        let initiate_bytes = initiate.to_http_bytes();
        let (create_status, _, _) = split_response(&create_bytes);
        let (initiate_status, _, initiate_body) =
            split_response(&initiate_bytes);
        let initiate_body = std::str::from_utf8(initiate_body)
            .expect("initiate body should be UTF-8");
        let upload_id = initiate_body
            .split("<UploadId>")
            .nth(1)
            .and_then(|value| value.split("</UploadId>").next())
            .expect("initiate response should include an upload id")
            .to_owned();

        let upload_part_1 = router.handle_bytes(
            format!(
                "PUT /multipart-demo/archive.bin?partNumber=1&uploadId={upload_id} HTTP/1.1\r\nHost: localhost\r\nContent-Length: 5\r\n\r\nhello"
            )
            .as_bytes(),
        );
        let upload_part_2 = router.handle_bytes(
            format!(
                "PUT /multipart-demo/archive.bin?partNumber=2&uploadId={upload_id} HTTP/1.1\r\nHost: localhost\r\nContent-Length: 5\r\n\r\nworld"
            )
            .as_bytes(),
        );
        let list_uploads = router.handle_bytes(
            b"GET /multipart-demo?uploads HTTP/1.1\r\nHost: localhost\r\n\r\n",
        );
        let upload_part_1_bytes = upload_part_1.to_http_bytes();
        let upload_part_2_bytes = upload_part_2.to_http_bytes();
        let list_uploads_bytes = list_uploads.to_http_bytes();
        let (upload_part_1_status, upload_part_1_headers, _) =
            split_response(&upload_part_1_bytes);
        let (upload_part_2_status, upload_part_2_headers, _) =
            split_response(&upload_part_2_bytes);
        let (list_uploads_status, _, list_uploads_body) =
            split_response(&list_uploads_bytes);
        let etag_part_1 = header_value(&upload_part_1_headers, "etag")
            .expect("upload part 1 should return an etag")
            .to_owned();
        let etag_part_2 = header_value(&upload_part_2_headers, "etag")
            .expect("upload part 2 should return an etag")
            .to_owned();
        let list_uploads_body = std::str::from_utf8(list_uploads_body)
            .expect("list uploads body should be UTF-8");
        let complete_body = format!(
            "<CompleteMultipartUpload><Part><ETag>{etag_part_1}</ETag><PartNumber>1</PartNumber></Part><Part><ETag>{etag_part_2}</ETag><PartNumber>2</PartNumber></Part></CompleteMultipartUpload>"
        );

        let complete = router.handle_bytes(
            format!(
                "POST /multipart-demo/archive.bin?uploadId={upload_id} HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/xml\r\nContent-Length: {}\r\n\r\n{}",
                complete_body.len(),
                complete_body
            )
            .as_bytes(),
        );
        let get_completed = router.handle_bytes(
            b"GET /multipart-demo/archive.bin HTTP/1.1\r\nHost: localhost\r\n\r\n",
        );
        let complete_bytes = complete.to_http_bytes();
        let get_completed_bytes = get_completed.to_http_bytes();
        let (complete_status, complete_headers, complete_body) =
            split_response(&complete_bytes);
        let (get_completed_status, _, get_completed_body) =
            split_response(&get_completed_bytes);
        let complete_body = std::str::from_utf8(complete_body)
            .expect("complete body should be UTF-8");

        let aborted_initiate = router.handle_bytes(
            b"POST /multipart-demo/aborted.bin?uploads HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\n\r\n",
        );
        let aborted_initiate_bytes = aborted_initiate.to_http_bytes();
        let (_, _, aborted_initiate_body) =
            split_response(&aborted_initiate_bytes);
        let aborted_upload_id = std::str::from_utf8(aborted_initiate_body)
            .expect("aborted initiate body should be UTF-8")
            .split("<UploadId>")
            .nth(1)
            .and_then(|value| value.split("</UploadId>").next())
            .expect("aborted initiate should include upload id")
            .to_owned();
        let _aborted_part = router.handle_bytes(
            format!(
                "PUT /multipart-demo/aborted.bin?partNumber=1&uploadId={aborted_upload_id} HTTP/1.1\r\nHost: localhost\r\nContent-Length: 5\r\n\r\nparts"
            )
            .as_bytes(),
        );
        let abort = router.handle_bytes(
            format!(
                "DELETE /multipart-demo/aborted.bin?uploadId={aborted_upload_id} HTTP/1.1\r\nHost: localhost\r\n\r\n"
            )
            .as_bytes(),
        );
        let list_after_abort = router.handle_bytes(
            b"GET /multipart-demo?uploads HTTP/1.1\r\nHost: localhost\r\n\r\n",
        );
        let get_aborted = router.handle_bytes(
            b"GET /multipart-demo/aborted.bin HTTP/1.1\r\nHost: localhost\r\n\r\n",
        );
        let abort_bytes = abort.to_http_bytes();
        let list_after_abort_bytes = list_after_abort.to_http_bytes();
        let get_aborted_bytes = get_aborted.to_http_bytes();
        let (abort_status, _, _) = split_response(&abort_bytes);
        let (list_after_abort_status, _, list_after_abort_body) =
            split_response(&list_after_abort_bytes);
        let (get_aborted_status, _, get_aborted_body) =
            split_response(&get_aborted_bytes);
        let list_after_abort_body = std::str::from_utf8(list_after_abort_body)
            .expect("post-abort list body should be UTF-8");
        let get_aborted_body = std::str::from_utf8(get_aborted_body)
            .expect("aborted get body should be UTF-8");

        let invalid_initiate = router.handle_bytes(
            b"POST /multipart-demo/invalid.bin?uploads HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\n\r\n",
        );
        let invalid_initiate_bytes = invalid_initiate.to_http_bytes();
        let (_, _, invalid_initiate_body) =
            split_response(&invalid_initiate_bytes);
        let invalid_upload_id = std::str::from_utf8(invalid_initiate_body)
            .expect("invalid initiate body should be UTF-8")
            .split("<UploadId>")
            .nth(1)
            .and_then(|value| value.split("</UploadId>").next())
            .expect("invalid initiate should include upload id")
            .to_owned();
        let _invalid_part = router.handle_bytes(
            format!(
                "PUT /multipart-demo/invalid.bin?partNumber=1&uploadId={invalid_upload_id} HTTP/1.1\r\nHost: localhost\r\nContent-Length: 4\r\n\r\npart"
            )
            .as_bytes(),
        );
        let malformed_body =
            "<CompleteMultipartUpload></CompleteMultipartUpload>";
        let malformed_complete = router.handle_bytes(
            format!(
                "POST /multipart-demo/invalid.bin?uploadId={invalid_upload_id} HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/xml\r\nContent-Length: {}\r\n\r\n{malformed_body}",
                malformed_body.len(),
            )
            .as_bytes(),
        );
        let malformed_complete_bytes = malformed_complete.to_http_bytes();
        let (malformed_status, _, malformed_body) =
            split_response(&malformed_complete_bytes);
        let malformed_body = std::str::from_utf8(malformed_body)
            .expect("malformed error body should be UTF-8");

        assert_eq!(create_status, "HTTP/1.1 200 OK");
        assert_eq!(initiate_status, "HTTP/1.1 200 OK");
        assert_eq!(upload_part_1_status, "HTTP/1.1 200 OK");
        assert_eq!(upload_part_2_status, "HTTP/1.1 200 OK");
        assert_eq!(list_uploads_status, "HTTP/1.1 200 OK");
        assert!(list_uploads_body.contains(&upload_id));
        assert_eq!(complete_status, "HTTP/1.1 200 OK");
        assert!(header_value(&complete_headers, "etag").is_none());
        assert!(complete_body.contains("<CompleteMultipartUploadResult"));
        assert_eq!(get_completed_status, "HTTP/1.1 200 OK");
        assert_eq!(get_completed_body, b"helloworld");
        assert_eq!(abort_status, "HTTP/1.1 204 No Content");
        assert_eq!(list_after_abort_status, "HTTP/1.1 200 OK");
        assert!(!list_after_abort_body.contains(&aborted_upload_id));
        assert_eq!(get_aborted_status, "HTTP/1.1 404 Not Found");
        assert!(get_aborted_body.contains("<Code>NoSuchKey</Code>"));
        assert_eq!(malformed_status, "HTTP/1.1 400 Bad Request");
        assert!(malformed_body.contains("<Code>MalformedXML</Code>"));
    }

    #[test]
    fn s3_notifications_rest_xml_notification_and_object_lock_routes_work() {
        let (router, runtime) = router_with_runtime("http-runtime-notify");
        let account = "000000000000"
            .parse::<aws::AccountId>()
            .expect("account should parse");
        let region =
            "eu-west-2".parse::<aws::RegionId>().expect("region should parse");
        let sqs_scope = SqsScope::new(account.clone(), region.clone());
        let sns_scope = SnsScope::new(account, region);
        let queue = runtime
            .sqs()
            .create_queue(
                &sqs_scope,
                CreateQueueInput {
                    attributes: BTreeMap::new(),
                    queue_name: "s3-events".to_owned(),
                },
            )
            .expect("queue should be created");
        let queue_arn = runtime
            .sqs()
            .get_queue_attributes(&queue, &["QueueArn"])
            .expect("queue attributes should load")
            .get("QueueArn")
            .expect("queue arn should be present")
            .to_owned();
        let topic_arn = runtime
            .sns()
            .create_topic(
                &sns_scope,
                CreateTopicInput {
                    attributes: BTreeMap::new(),
                    name: "s3-events".to_owned(),
                },
            )
            .expect("topic should be created");

        let create =
            router.handle_bytes(&s3_create_bucket_request_with_headers(
                "/notify-demo",
                "localhost",
                "x-amz-bucket-object-lock-enabled: true\r\n",
            ));
        let notification_body = format!(
            "<NotificationConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><QueueConfiguration><Id>queue-config</Id><Queue>{queue_arn}</Queue><Event>s3:ObjectCreated:*</Event></QueueConfiguration><TopicConfiguration><Id>topic-config</Id><Topic>{topic_arn}</Topic><Event>s3:ObjectCreated:Put</Event></TopicConfiguration></NotificationConfiguration>"
        );
        let put_notification = router.handle_bytes(
            format!(
                "PUT /notify-demo?notification HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/xml\r\nContent-Length: {}\r\n\r\n{}",
                notification_body.len(),
                notification_body
            )
            .as_bytes(),
        );
        let object_lock_body = "<ObjectLockConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><ObjectLockEnabled>Enabled</ObjectLockEnabled><Rule><DefaultRetention><Mode>GOVERNANCE</Mode><Days>1</Days></DefaultRetention></Rule></ObjectLockConfiguration>";
        let put_bucket_lock = router.handle_bytes(
            format!(
                "PUT /notify-demo?object-lock HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/xml\r\nContent-Length: {}\r\n\r\n{object_lock_body}",
                object_lock_body.len(),
            )
            .as_bytes(),
        );
        let put_object = router.handle_bytes(
            b"PUT /notify-demo/reports/data.csv HTTP/1.1\r\nHost: localhost\r\nContent-Type: text/csv\r\nx-amz-object-lock-mode: GOVERNANCE\r\nx-amz-object-lock-retain-until-date: 2030-01-01T00:00:00Z\r\nContent-Length: 7\r\n\r\npayload",
        );

        let create_bytes = create.to_http_bytes();
        let put_notification_bytes = put_notification.to_http_bytes();
        let put_bucket_lock_bytes = put_bucket_lock.to_http_bytes();
        let put_object_bytes = put_object.to_http_bytes();
        let (create_status, _, _) = split_response(&create_bytes);
        let (put_notification_status, _, _) =
            split_response(&put_notification_bytes);
        let (put_bucket_lock_status, _, _) =
            split_response(&put_bucket_lock_bytes);
        let (put_object_status, put_object_headers, _) =
            split_response(&put_object_bytes);
        let version_id = header_value(&put_object_headers, "x-amz-version-id")
            .expect("versioned object writes should expose a version id")
            .to_owned();

        let head = router.handle_bytes(
            b"HEAD /notify-demo/reports/data.csv HTTP/1.1\r\nHost: localhost\r\n\r\n",
        );
        let get_notification = router.handle_bytes(
            b"GET /notify-demo?notification HTTP/1.1\r\nHost: localhost\r\n\r\n",
        );
        let get_retention = router.handle_bytes(
            format!(
                "GET /notify-demo/reports/data.csv?retention&versionId={version_id} HTTP/1.1\r\nHost: localhost\r\n\r\n"
            )
            .as_bytes(),
        );
        let legal_hold_body = "<LegalHold xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Status>ON</Status></LegalHold>";
        let put_legal_hold = router.handle_bytes(
            format!(
                "PUT /notify-demo/reports/data.csv?legal-hold&versionId={version_id} HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/xml\r\nContent-Length: {}\r\n\r\n{legal_hold_body}",
                legal_hold_body.len(),
            )
            .as_bytes(),
        );
        let get_legal_hold = router.handle_bytes(
            format!(
                "GET /notify-demo/reports/data.csv?legal-hold&versionId={version_id} HTTP/1.1\r\nHost: localhost\r\n\r\n"
            )
            .as_bytes(),
        );
        let delete_version = router.handle_bytes(
            format!(
                "DELETE /notify-demo/reports/data.csv?versionId={version_id} HTTP/1.1\r\nHost: localhost\r\nx-amz-bypass-governance-retention: true\r\n\r\n"
            )
            .as_bytes(),
        );
        let restore = router.handle_bytes(
            b"POST /notify-demo/reports/data.csv?restore HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\n\r\n",
        );
        let received = runtime
            .sqs()
            .receive_message(
                &queue,
                ReceiveMessageInput {
                    attribute_names: Vec::new(),
                    max_number_of_messages: Some(10),
                    message_attribute_names: Vec::new(),
                    message_system_attribute_names: Vec::new(),
                    receive_request_attempt_id: None,
                    visibility_timeout: None,
                    wait_time_seconds: None,
                },
            )
            .expect("notification should be receivable");

        let head_bytes = head.to_http_bytes();
        let get_notification_bytes = get_notification.to_http_bytes();
        let get_retention_bytes = get_retention.to_http_bytes();
        let put_legal_hold_bytes = put_legal_hold.to_http_bytes();
        let get_legal_hold_bytes = get_legal_hold.to_http_bytes();
        let delete_version_bytes = delete_version.to_http_bytes();
        let restore_bytes = restore.to_http_bytes();
        let (head_status, head_headers, _) = split_response(&head_bytes);
        let (get_notification_status, _, get_notification_body) =
            split_response(&get_notification_bytes);
        let (get_retention_status, _, get_retention_body) =
            split_response(&get_retention_bytes);
        let (put_legal_hold_status, _, _) =
            split_response(&put_legal_hold_bytes);
        let (get_legal_hold_status, _, get_legal_hold_body) =
            split_response(&get_legal_hold_bytes);
        let (delete_version_status, _, delete_version_body) =
            split_response(&delete_version_bytes);
        let (restore_status, _, restore_body) = split_response(&restore_bytes);
        let get_notification_body = std::str::from_utf8(get_notification_body)
            .expect("notification body should be UTF-8");
        let get_retention_body = std::str::from_utf8(get_retention_body)
            .expect("retention body should be UTF-8");
        let get_legal_hold_body = std::str::from_utf8(get_legal_hold_body)
            .expect("legal hold body should be UTF-8");
        let delete_version_body = std::str::from_utf8(delete_version_body)
            .expect("delete error body should be UTF-8");
        let restore_body = std::str::from_utf8(restore_body)
            .expect("restore body should be UTF-8");

        assert_eq!(create_status, "HTTP/1.1 200 OK");
        assert_eq!(put_notification_status, "HTTP/1.1 200 OK");
        assert_eq!(put_bucket_lock_status, "HTTP/1.1 200 OK");
        assert_eq!(put_object_status, "HTTP/1.1 200 OK");
        assert_eq!(head_status, "HTTP/1.1 200 OK");
        assert_eq!(
            header_value(&head_headers, "x-amz-object-lock-mode"),
            Some("GOVERNANCE")
        );
        assert_eq!(
            header_value(&head_headers, "x-amz-object-lock-retain-until-date"),
            Some("2030-01-01T00:00:00Z")
        );
        assert_eq!(get_notification_status, "HTTP/1.1 200 OK");
        let topic_arn_text = topic_arn.to_string();
        assert!(get_notification_body.contains(queue_arn.as_str()));
        assert!(get_notification_body.contains(&topic_arn_text));
        assert_eq!(get_retention_status, "HTTP/1.1 200 OK");
        assert!(get_retention_body.contains("<Mode>GOVERNANCE</Mode>"));
        assert!(get_retention_body.contains("2030-01-01T00:00:00Z"));
        assert_eq!(put_legal_hold_status, "HTTP/1.1 200 OK");
        assert_eq!(get_legal_hold_status, "HTTP/1.1 200 OK");
        assert!(get_legal_hold_body.contains("<Status>ON</Status>"));
        assert_eq!(delete_version_status, "HTTP/1.1 403 Forbidden");
        assert!(delete_version_body.contains("<Code>AccessDenied</Code>"));
        assert_eq!(restore_status, "HTTP/1.1 501 Not Implemented");
        assert!(restore_body.contains("<Code>NotImplemented</Code>"));
        assert_eq!(received.len(), 1);
        assert!(
            received[0].body.contains("\"eventName\":\"ObjectCreated:Put\"")
        );
        assert!(received[0].body.contains("reports%2Fdata.csv"));
    }

    #[test]
    fn s3_notifications_rest_xml_select_stream_and_invalid_input_work() {
        let (router, _) = router_with_runtime("http-runtime-select");
        let create = router.handle_bytes(&s3_create_bucket_request(
            "/select-demo",
            "localhost",
        ));
        let put = router.handle_bytes(
            b"PUT /select-demo/records.csv HTTP/1.1\r\nHost: localhost\r\nContent-Type: text/csv\r\nContent-Length: 24\r\n\r\nName,Age\nJane,30\nBob,20\n",
        );
        let select_body = "<SelectObjectContentRequest xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Expression>SELECT s.&quot;Name&quot; FROM s3object s WHERE s.&quot;Age&quot; &gt; 21</Expression><ExpressionType>SQL</ExpressionType><InputSerialization><CSV><FileHeaderInfo>USE</FileHeaderInfo></CSV><CompressionType>NONE</CompressionType></InputSerialization><OutputSerialization><CSV></CSV></OutputSerialization></SelectObjectContentRequest>";
        let select = router.handle_bytes(
            format!(
                "POST /select-demo/records.csv?select&select-type=2 HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/xml\r\nContent-Length: {}\r\n\r\n{select_body}",
                select_body.len(),
            )
            .as_bytes(),
        );
        let invalid_body = "<SelectObjectContentRequest xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Expression>SELECT</Expression><ExpressionType>SQL</ExpressionType><InputSerialization><CSV><FileHeaderInfo>USE</FileHeaderInfo></CSV><CompressionType>NONE</CompressionType></InputSerialization><OutputSerialization><CSV></CSV></OutputSerialization></SelectObjectContentRequest>";
        let invalid_select = router.handle_bytes(
            format!(
                "POST /select-demo/records.csv?select&select-type=2 HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/xml\r\nContent-Length: {}\r\n\r\n{invalid_body}",
                invalid_body.len(),
            )
            .as_bytes(),
        );

        let create_bytes = create.to_http_bytes();
        let put_bytes = put.to_http_bytes();
        let select_bytes = select.to_http_bytes();
        let invalid_select_bytes = invalid_select.to_http_bytes();
        let (create_status, _, _) = split_response(&create_bytes);
        let (put_status, _, _) = split_response(&put_bytes);
        let (select_status, select_headers, select_body_bytes) =
            split_response(&select_bytes);
        let (invalid_status, _, invalid_body_bytes) =
            split_response(&invalid_select_bytes);
        let invalid_body = std::str::from_utf8(invalid_body_bytes)
            .expect("invalid select body should be UTF-8");

        let mut stream = Bytes::copy_from_slice(select_body_bytes);
        let mut frames = Vec::new();
        while stream.has_remaining() {
            frames.push(
                read_message_from(&mut stream).expect("frame should decode"),
            );
        }

        assert_eq!(create_status, "HTTP/1.1 200 OK");
        assert_eq!(put_status, "HTTP/1.1 200 OK");
        assert_eq!(select_status, "HTTP/1.1 200 OK");
        assert_eq!(
            header_value(&select_headers, "content-type"),
            Some("application/vnd.amazon.eventstream")
        );
        assert_eq!(frames.len(), 3);
        assert_eq!(frames[0].payload().as_ref(), b"Jane\n");
        assert!(
            std::str::from_utf8(frames[1].payload().as_ref())
                .expect("stats payload should be UTF-8")
                .contains("<BytesReturned>")
        );
        assert_eq!(frames[2].payload().len(), 0);
        assert_eq!(invalid_status, "HTTP/1.1 400 Bad Request");
        assert!(invalid_body.contains("<Code>InvalidArgument</Code>"));
    }

    #[test]
    fn runtime_signed_json_request_without_target_uses_scope_service() {
        let request = HttpRequest::parse(
            b"POST / HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/x-amz-json-1.0\r\nContent-Length: 2\r\n\r\n{}",
        )
        .expect("request should parse");
        let verified_request = VerifiedRequest::new(
            "000000000000".parse().expect("account should parse"),
            CallerCredentialKind::LongTerm(StableAwsPrincipal::new(
                "arn:aws:iam::000000000000:root"
                    .parse::<Arn>()
                    .expect("root ARN should parse"),
                AwsPrincipalType::Account,
                None,
            )),
            CallerIdentity::try_new(
                "arn:aws:iam::000000000000:root"
                    .parse::<Arn>()
                    .expect("root ARN should parse"),
                "000000000000",
            )
            .expect("caller identity should build"),
            CredentialScope::new(
                "us-west-2".parse().expect("region should parse"),
                ServiceName::DynamoDb,
            ),
            None,
        );
        let response = router().route_json(
            &request,
            ProtocolFamily::AwsJson10,
            Some(&verified_request),
            &super::NEVER_CANCELLED,
        );
        let response_bytes = response.to_http_bytes();
        let (status, headers, body) = split_response(&response_bytes);
        let body: AwsJsonErrorBody = serde_json::from_slice(body)
            .expect("body should be a JSON AWS error");

        assert_eq!(status, "HTTP/1.1 400 Bad Request");
        assert_eq!(
            header_value(&headers, "content-type"),
            Some("application/x-amz-json-1.0")
        );
        assert_eq!(body.error_type, "UnknownOperationException");
        assert!(
            body.message.contains("dynamodb"),
            "expected the error to mention the routed service, got {}",
            body.message
        );
        assert!(
            body.message.contains("missing X-Amz-Target"),
            "expected the error to mention the missing target, got {}",
            body.message
        );
    }

    #[test]
    fn runtime_query_unknown_action_returns_xml_error() {
        let body = "Action=UnknownAction&Version=2011-06-15";
        let response = router().handle_bytes(
            format!(
                "POST / HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/x-www-form-urlencoded\r\nContent-Length: {}\r\n\r\n{body}",
                body.len()
            )
            .as_bytes(),
        );
        let response_bytes = response.to_http_bytes();
        let (status, headers, body) = split_response(&response_bytes);
        let body =
            std::str::from_utf8(body).expect("query body should be UTF-8");

        assert_eq!(status, "HTTP/1.1 400 Bad Request");
        assert_eq!(header_value(&headers, "content-type"), Some("text/xml"));
        assert!(body.contains("<Code>InvalidAction</Code>"));
        assert!(
            body.contains("<Message>Unknown action UnknownAction.</Message>")
        );
    }

    #[test]
    fn runtime_query_get_requests_route_to_sts() {
        let response = router().handle_bytes(
            b"GET /?Action=GetCallerIdentity&Version=2011-06-15 HTTP/1.1\r\nHost: localhost\r\n\r\n",
        );
        let response_bytes = response.to_http_bytes();
        let (status, headers, body) = split_response(&response_bytes);
        let body =
            std::str::from_utf8(body).expect("query body should be UTF-8");

        assert_eq!(status, "HTTP/1.1 403 Forbidden");
        assert_eq!(header_value(&headers, "content-type"), Some("text/xml"));
        assert!(body.contains("<Code>MissingAuthenticationToken</Code>"));
        assert!(!body.contains("<ListAllMyBucketsResult"));
    }
    #[test]
    fn runtime_query_requests_require_version_before_mutating_state() {
        let router = router();
        let create_body = "Action=CreateQueue&QueueName=demo";
        let create_response = router.handle_bytes(
            format!(
                "POST / HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/x-www-form-urlencoded\r\nContent-Length: {}\r\n\r\n{create_body}",
                create_body.len()
            )
            .as_bytes(),
        );
        let create_response_bytes = create_response.to_http_bytes();
        let (create_status, create_headers, create_body) =
            split_response(&create_response_bytes);
        let create_body = std::str::from_utf8(create_body)
            .expect("query body should be UTF-8");

        assert_eq!(create_status, "HTTP/1.1 400 Bad Request");
        assert_eq!(
            header_value(&create_headers, "content-type"),
            Some("text/xml")
        );
        assert!(create_body.contains("<Code>MissingParameter</Code>"));
        assert!(
            create_body.contains(
                "<Message>The request must contain the parameter Version.</Message>"
            )
        );

        let list_body = "Action=ListQueues&Version=2012-11-05";
        let list_response = router.handle_bytes(
            format!(
                "POST / HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/x-www-form-urlencoded\r\nContent-Length: {}\r\n\r\n{list_body}",
                list_body.len()
            )
            .as_bytes(),
        );
        let list_response_bytes = list_response.to_http_bytes();
        let (list_status, list_headers, list_body) =
            split_response(&list_response_bytes);
        let list_body = std::str::from_utf8(list_body)
            .expect("query body should be UTF-8");

        assert_eq!(list_status, "HTTP/1.1 200 OK");
        assert_eq!(
            header_value(&list_headers, "content-type"),
            Some("text/xml")
        );
        assert!(list_body.contains("<ListQueuesResult"));
        assert!(!list_body.contains("demo"));
    }

    #[test]
    fn runtime_known_query_actions_reject_unsupported_versions() {
        let cases = [
            ("GetCallerIdentity", iam_query::IAM_QUERY_VERSION),
            ("ListUsers", sts_query::STS_QUERY_VERSION),
            ("ListQueues", sts_query::STS_QUERY_VERSION),
        ];

        for (action, version) in cases {
            let body = format!("Action={action}&Version={version}");
            let response = router().handle_bytes(
                format!(
                    "POST / HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/x-www-form-urlencoded\r\nContent-Length: {}\r\n\r\n{body}",
                    body.len()
                )
                .as_bytes(),
            );
            let response_bytes = response.to_http_bytes();
            let (status, headers, body) = split_response(&response_bytes);
            let body =
                std::str::from_utf8(body).expect("query body should be UTF-8");

            assert_eq!(status, "HTTP/1.1 400 Bad Request");
            assert_eq!(
                header_value(&headers, "content-type"),
                Some("text/xml")
            );
            assert!(body.contains("<Code>InvalidAction</Code>"));
            assert!(body.contains(&format!(
                "<Message>Could not find operation {action} for version {version}.</Message>"
            )));
        }
    }
    #[test]
    fn runtime_query_overlaps_route_create_user_to_elasticache_version() {
        let body = concat!(
            "Action=CreateUser&Version=2015-02-02",
            "&UserId=user-a&UserName=alice",
            "&AuthenticationMode.Type=no-password-required"
        );
        let response = router().handle_bytes(
            format!(
                "POST / HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/x-www-form-urlencoded\r\nContent-Length: {}\r\n\r\n{body}",
                body.len()
            )
            .as_bytes(),
        );
        let response_bytes = response.to_http_bytes();
        let (status, headers, body) = split_response(&response_bytes);
        let body =
            std::str::from_utf8(body).expect("query body should be UTF-8");

        assert_eq!(status, "HTTP/1.1 200 OK");
        assert_eq!(header_value(&headers, "content-type"), Some("text/xml"));
        assert!(body.contains("<CreateUserResponse"));
    }

    #[test]
    fn runtime_elasticache_known_action_with_wrong_version_is_invalid() {
        let body = concat!(
            "Action=CreateUser&Version=2014-10-31",
            "&UserId=user-a&UserName=alice",
            "&AuthenticationMode.Type=no-password-required"
        );
        let response = router().handle_bytes(
            format!(
                "POST / HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/x-www-form-urlencoded\r\nContent-Length: {}\r\n\r\n{body}",
                body.len()
            )
            .as_bytes(),
        );
        let response_bytes = response.to_http_bytes();
        let (status, headers, body) = split_response(&response_bytes);
        let body =
            std::str::from_utf8(body).expect("query body should be UTF-8");

        assert_eq!(status, "HTTP/1.1 400 Bad Request");
        assert_eq!(header_value(&headers, "content-type"), Some("text/xml"));
        assert!(body.contains("<Code>InvalidAction</Code>"));
        assert!(body.contains("CreateUser"));
        assert!(!body.contains("<CreateUserResponse"));
    }

    #[test]
    fn runtime_json_markers_win_over_query_heuristics() {
        let body = "Action=GetCallerIdentity&Version=2011-06-15";
        let response = router().handle_bytes(
            format!(
                "POST / HTTP/1.1\r\nHost: localhost\r\nX-Amz-Target: AmazonSSM.GetParameter\r\nContent-Type: application/x-amz-json-1.1\r\nContent-Length: {}\r\n\r\n{body}",
                body.len()
            )
            .as_bytes(),
        );
        let response_bytes = response.to_http_bytes();
        let (status, headers, body) = split_response(&response_bytes);
        let body: AwsJsonErrorBody = serde_json::from_slice(body)
            .expect("body should be a JSON AWS error");

        assert_eq!(status, "HTTP/1.1 400 Bad Request");
        assert_eq!(
            header_value(&headers, "content-type"),
            Some("application/x-amz-json-1.1")
        );
        assert_eq!(body.error_type, "ValidationException");
    }

    #[test]
    fn runtime_protocol_mismatch_returns_protocol_specific_json_error() {
        let response = router().handle_bytes(
            b"POST / HTTP/1.1\r\nHost: localhost\r\nX-Amz-Target: DynamoDB_20120810.ListTables\r\nContent-Type: application/x-amz-json-1.1\r\nContent-Length: 2\r\n\r\n{}",
        );
        let response_bytes = response.to_http_bytes();
        let (status, headers, body) = split_response(&response_bytes);
        let body: AwsJsonErrorBody = serde_json::from_slice(body)
            .expect("body should be a JSON AWS error");

        assert_eq!(status, "HTTP/1.1 400 Bad Request");
        assert_eq!(
            header_value(&headers, "content-type"),
            Some("application/x-amz-json-1.1")
        );
        assert_eq!(body.error_type, "UnknownOperationException");
        assert!(body.message.contains("DynamoDB_20120810.ListTables"));
    }

    #[test]
    fn runtime_smithy_cbor_path_returns_cbor_error_shape() {
        let response = router().handle_bytes(
            b"POST /service/GraniteServiceVersion20100801/operation/GetMetricData HTTP/1.1\r\nHost: localhost\r\nSmithy-Protocol: rpc-v2-cbor\r\nContent-Type: application/cbor\r\nContent-Length: 1\r\n\r\n\xa0",
        );
        let response_bytes = response.to_http_bytes();
        let (status, headers, body) = split_response(&response_bytes);
        let body: AwsJsonErrorBody =
            from_reader(body).expect("body should be a CBOR AWS error");

        assert_eq!(status, "HTTP/1.1 400 Bad Request");
        assert_eq!(
            header_value(&headers, "content-type"),
            Some("application/cbor")
        );
        assert_eq!(
            header_value(&headers, "smithy-protocol"),
            Some("rpc-v2-cbor")
        );
        assert_eq!(body.error_type, "MissingParameter");
        assert!(body.message.contains("EndTime"));
    }

    #[test]
    fn cloudwatch_logs_json_round_trip_returns_written_events() {
        let router = router();
        let bodies = cloudwatch_logs_round_trip_bodies(&router);
        assert_cloudwatch_logs_round_trip(bodies);
    }

    #[test]
    fn cloudwatch_metrics_query_json_and_cbor_get_metric_data_stay_consistent()
    {
        let router = router();
        let bodies = cloudwatch_metric_data_bodies(&router);
        assert_cloudwatch_metric_data_bodies(bodies);
    }

    #[test]
    fn cloudwatch_metrics_smithy_cbor_metric_math_returns_explicit_error() {
        let router = router();
        let response = router.handle_bytes(&smithy_cbor_request(
            "/service/GraniteServiceVersion20100801/operation/GetMetricData",
            &json!({
                "StartTime": 0,
                "EndTime": 180,
                "MetricDataQueries": [{
                    "Id": "e1",
                    "Expression": "m1*2"
                }]
            }),
        ));
        let response_bytes = response.to_http_bytes();
        let (status, headers, body) = split_response(&response_bytes);
        let body: AwsJsonErrorBody =
            from_reader(body).expect("CBOR error body should decode");

        assert_eq!(status, "HTTP/1.1 400 Bad Request");
        assert_eq!(
            header_value(&headers, "content-type"),
            Some("application/cbor")
        );
        assert_eq!(
            header_value(&headers, "smithy-protocol"),
            Some("rpc-v2-cbor")
        );
        assert_eq!(body.error_type, "InvalidParameterValue");
        assert!(body.message.contains("Metric math expressions"));
    }

    #[test]
    fn runtime_malformed_signature_returns_aws_shaped_error() {
        let response = router().handle_bytes(
            b"POST / HTTP/1.1\r\nHost: localhost\r\nAuthorization: AWS4-HMAC-SHA256 Credential=broken\r\nX-Amz-Date: 20260325T120000Z\r\nContent-Type: application/x-amz-json-1.0\r\n\r\n{}",
        );
        let response_bytes = response.to_http_bytes();
        let (status, headers, body) = split_response(&response_bytes);
        let body: AwsJsonErrorBody = serde_json::from_slice(body)
            .expect("body should be a JSON AWS error");

        assert_eq!(status, "HTTP/1.1 400 Bad Request");
        assert_eq!(
            header_value(&headers, "content-type"),
            Some("application/x-amz-json-1.0")
        );
        assert_eq!(body.error_type, "IncompleteSignature");
    }

    #[test]
    fn runtime_signed_query_scope_mismatch_returns_signature_error() {
        let request = HttpRequest::parse(
            b"POST / HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/x-www-form-urlencoded\r\nContent-Length: 51\r\n\r\nAction=CreateUser&UserName=alice&Version=2010-05-08",
        )
        .expect("request should parse");
        let verified_request = VerifiedRequest::new(
            "000000000000".parse().expect("account should parse"),
            CallerCredentialKind::LongTerm(StableAwsPrincipal::new(
                "arn:aws:iam::000000000000:root"
                    .parse::<Arn>()
                    .expect("root ARN should parse"),
                AwsPrincipalType::Account,
                None,
            )),
            CallerIdentity::try_new(
                "arn:aws:iam::000000000000:root"
                    .parse::<Arn>()
                    .expect("root ARN should parse"),
                "000000000000",
            )
            .expect("caller identity should build"),
            CredentialScope::new(
                "eu-west-2".parse().expect("region should parse"),
                ServiceName::Sts,
            ),
            None,
        );
        let response = router().route_query(
            &request,
            Some(&verified_request),
            &super::NEVER_CANCELLED,
        );
        let response_bytes = response.to_http_bytes();
        let (status, headers, body) = split_response(&response_bytes);
        let body =
            std::str::from_utf8(body).expect("query body should be UTF-8");

        assert_eq!(status, "HTTP/1.1 403 Forbidden");
        assert_eq!(header_value(&headers, "content-type"), Some("text/xml"));
        assert!(body.contains("<Code>SignatureDoesNotMatch</Code>"));
    }

    #[test]
    fn runtime_signed_query_overlap_with_wrong_version_returns_invalid_action()
    {
        let request = HttpRequest::parse(
            b"POST / HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/x-www-form-urlencoded\r\nContent-Length: 87\r\n\r\nAction=CreateUser&Version=2015-02-02&UserName=alice&UserId=user-a&AuthenticationMode.Type=no-password-required",
        )
        .expect("request should parse");
        let verified_request = VerifiedRequest::new(
            "000000000000".parse().expect("account should parse"),
            CallerCredentialKind::LongTerm(StableAwsPrincipal::new(
                "arn:aws:iam::000000000000:root"
                    .parse::<Arn>()
                    .expect("root ARN should parse"),
                AwsPrincipalType::Account,
                None,
            )),
            CallerIdentity::try_new(
                "arn:aws:iam::000000000000:root"
                    .parse::<Arn>()
                    .expect("root ARN should parse"),
                "000000000000",
            )
            .expect("caller identity should build"),
            CredentialScope::new(
                "eu-west-2".parse().expect("region should parse"),
                ServiceName::Iam,
            ),
            None,
        );
        let response = router().route_query(
            &request,
            Some(&verified_request),
            &super::NEVER_CANCELLED,
        );
        let response_bytes = response.to_http_bytes();
        let (status, headers, body) = split_response(&response_bytes);
        let body =
            std::str::from_utf8(body).expect("query body should be UTF-8");

        assert_eq!(status, "HTTP/1.1 400 Bad Request");
        assert_eq!(header_value(&headers, "content-type"), Some("text/xml"));
        assert!(body.contains("<Code>InvalidAction</Code>"));
        assert!(!body.contains("<Code>SignatureDoesNotMatch</Code>"));
    }

    #[test]
    fn runtime_bad_request_parse_errors_use_internal_json() {
        let response = router().handle_bytes(&[0xff, 0xfe]);
        let response_bytes = response.to_http_bytes();
        let (status, headers, body) = split_response(&response_bytes);
        let body: Value = serde_json::from_slice(body)
            .expect("bad request body should be valid JSON");

        assert_eq!(status, "HTTP/1.1 400 Bad Request");
        assert_eq!(
            header_value(&headers, "content-type"),
            Some("application/json")
        );
        assert_eq!(body["message"], "request headers are not valid UTF-8");
    }
}
