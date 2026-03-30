use crate::{
    ApiGatewayInvokeInput, FunctionUrlInvocationInput,
    FunctionUrlInvocationOutput, InvokeInput, InvokeOutput, LambdaError,
    LambdaScope, LambdaService, ResolvedFunctionUrlTarget,
};
use aws::CallerIdentity;

#[derive(Clone)]
pub struct LambdaRequestRuntime {
    service: LambdaService,
}

impl LambdaRequestRuntime {
    pub fn new(service: LambdaService) -> Self {
        Self { service }
    }

    /// # Errors
    ///
    /// Returns an error when the Lambda target cannot be resolved, payload
    /// validation fails, or execution/enqueue dependencies reject the request.
    pub fn invoke(
        &self,
        scope: &LambdaScope,
        function_name: &str,
        qualifier: Option<&str>,
        input: InvokeInput,
        is_cancelled: &(dyn Fn() -> bool + Send + Sync),
    ) -> Result<InvokeOutput, LambdaError> {
        self.service.invoke_with_cancellation(
            scope,
            function_name,
            qualifier,
            input,
            is_cancelled,
        )
    }

    /// # Errors
    ///
    /// Returns an error when the resolved function URL target is missing,
    /// permissions reject the caller, or Lambda execution fails.
    pub fn invoke_resolved_function_url(
        &self,
        target: &ResolvedFunctionUrlTarget,
        input: FunctionUrlInvocationInput,
        caller_identity: Option<&CallerIdentity>,
        is_cancelled: &(dyn Fn() -> bool + Send + Sync),
    ) -> Result<FunctionUrlInvocationOutput, LambdaError> {
        self.service.invoke_resolved_function_url_with_cancellation(
            target,
            input,
            caller_identity,
            is_cancelled,
        )
    }

    /// # Errors
    ///
    /// Returns an error when the Lambda target cannot be resolved, API Gateway
    /// lacks invoke permission, or execution fails.
    pub fn invoke_apigateway(
        &self,
        scope: &LambdaScope,
        input: &ApiGatewayInvokeInput,
        is_cancelled: &(dyn Fn() -> bool + Send + Sync),
    ) -> Result<InvokeOutput, LambdaError> {
        self.service.invoke_apigateway_with_cancellation(
            scope,
            input,
            is_cancelled,
        )
    }
}
