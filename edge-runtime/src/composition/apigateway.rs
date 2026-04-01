#[cfg(feature = "apigateway")]
use apigateway::{
    ApiGatewayScope, ExecuteApiError, ExecuteApiIntegrationExecutor,
    ExecuteApiIntegrationPlan, ExecuteApiInvocation,
    ExecuteApiPreparedResponse, map_lambda_proxy_response,
    map_lambda_proxy_response_v2,
};
#[cfg(feature = "apigateway")]
use aws::HttpForwarder;
#[cfg(feature = "apigateway")]
use lambda::request_runtime::LambdaRequestRuntime;
#[cfg(feature = "apigateway")]
use lambda::{ApiGatewayInvokeInput, LambdaScope};
#[cfg(feature = "apigateway")]
use std::sync::Arc;

#[cfg(feature = "apigateway")]
#[derive(Clone)]
pub struct ApiGatewayIntegrationExecutor {
    http_forwarder: Option<Arc<dyn HttpForwarder + Send + Sync>>,
    lambda: Option<LambdaRequestRuntime>,
}

#[cfg(feature = "apigateway")]
impl ApiGatewayIntegrationExecutor {
    pub fn new(
        lambda: Option<LambdaRequestRuntime>,
        http_forwarder: Option<Arc<dyn HttpForwarder + Send + Sync>>,
    ) -> Self {
        Self { http_forwarder, lambda }
    }
}

#[cfg(feature = "apigateway")]
impl ExecuteApiIntegrationExecutor for ApiGatewayIntegrationExecutor {
    fn execute(
        &self,
        scope: &ApiGatewayScope,
        invocation: &ExecuteApiInvocation,
    ) -> Result<ExecuteApiPreparedResponse, ExecuteApiError> {
        self.execute_with_cancellation(scope, invocation, &|| false)
    }

    fn execute_with_cancellation(
        &self,
        scope: &ApiGatewayScope,
        invocation: &ExecuteApiInvocation,
        is_cancelled: &(dyn Fn() -> bool + Send + Sync),
    ) -> Result<ExecuteApiPreparedResponse, ExecuteApiError> {
        match invocation.integration() {
            ExecuteApiIntegrationPlan::Mock(response) => Ok(response.clone()),
            ExecuteApiIntegrationPlan::Http(request) => {
                let Some(forwarder) = self.http_forwarder.as_deref() else {
                    return Err(ExecuteApiError::IntegrationFailure {
                        message: "Internal server error".to_owned(),
                        status_code: 500,
                    });
                };
                forwarder
                    .forward_with_cancellation(request, is_cancelled)
                    .map(|response| {
                        ExecuteApiPreparedResponse::new(
                            response.status_code(),
                            response.headers().to_vec(),
                            response.body().to_vec(),
                        )
                    })
                    .map_err(|_| ExecuteApiError::IntegrationFailure {
                        message: "Internal server error".to_owned(),
                        status_code: 502,
                    })
            }
            ExecuteApiIntegrationPlan::LambdaProxy(plan) => {
                let Some(lambda) = self.lambda.as_ref() else {
                    return Err(ExecuteApiError::IntegrationFailure {
                        message: "Internal server error".to_owned(),
                        status_code: 500,
                    });
                };
                let output = lambda
                    .invoke_apigateway(
                        &LambdaScope::new(
                            scope.account_id().clone(),
                            scope.region().clone(),
                        ),
                        &ApiGatewayInvokeInput {
                            payload: plan.payload().to_vec(),
                            source_arn: plan.source_arn().clone(),
                            target: plan.target().clone(),
                        },
                        is_cancelled,
                    )
                    .map_err(|_| ExecuteApiError::IntegrationFailure {
                        message: "Internal server error".to_owned(),
                        status_code: 500,
                    })?;
                if output.function_error().is_some() {
                    return Err(ExecuteApiError::IntegrationFailure {
                        message: "Internal server error".to_owned(),
                        status_code: 502,
                    });
                }
                if plan.response_is_v2() {
                    map_lambda_proxy_response_v2(output.payload())
                } else {
                    map_lambda_proxy_response(output.payload())
                }
            }
        }
    }
}
