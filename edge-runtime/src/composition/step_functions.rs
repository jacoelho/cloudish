#[cfg(feature = "step-functions")]
use lambda::{InvokeInput, LambdaInvocationType, LambdaScope, LambdaService};
#[cfg(feature = "step-functions")]
use step_functions::{
    StepFunctionsScope, StepFunctionsTaskAdapter, TaskInvocationFailure,
    TaskInvocationRequest, TaskInvocationResult,
};

#[cfg(feature = "step-functions")]
#[derive(Clone)]
pub struct LambdaStepFunctionsTaskAdapter {
    lambda: Option<LambdaService>,
}

#[cfg(feature = "step-functions")]
impl LambdaStepFunctionsTaskAdapter {
    pub fn new(lambda: Option<LambdaService>) -> Self {
        Self { lambda }
    }
}

#[cfg(feature = "step-functions")]
impl StepFunctionsTaskAdapter for LambdaStepFunctionsTaskAdapter {
    fn invoke(
        &self,
        scope: &StepFunctionsScope,
        request: &TaskInvocationRequest,
    ) -> Result<TaskInvocationResult, TaskInvocationFailure> {
        let Some(lambda) = self.lambda.as_ref() else {
            return Err(TaskInvocationFailure::new(
                "ResourceNotFoundException",
                "Lambda service is disabled.",
                request.resource(),
                request.resource_type(),
            ));
        };
        let payload =
            serde_json::to_vec(request.payload()).map_err(|error| {
                TaskInvocationFailure::new(
                    "States.Runtime",
                    format!("Failed to encode Lambda payload: {error}"),
                    request.resource(),
                    request.resource_type(),
                )
            })?;
        let lambda_scope = LambdaScope::new(
            scope.account_id().clone(),
            scope.region().clone(),
        );
        let output = lambda
            .invoke_target(
                &lambda_scope,
                request.target(),
                InvokeInput {
                    invocation_type: LambdaInvocationType::RequestResponse,
                    payload,
                },
            )
            .map_err(|error| {
                let (code, message) = lambda_task_failure_details(&error);
                TaskInvocationFailure::new(
                    code,
                    message,
                    request.resource(),
                    request.resource_type(),
                )
            })?;

        if let Some(function_error) = output.function_error() {
            let cause = String::from_utf8_lossy(output.payload()).into_owned();
            return Err(TaskInvocationFailure::new(
                "Lambda.AWSLambdaException",
                cause,
                request.resource(),
                request.resource_type(),
            )
            .with_error_override(function_error));
        }

        let payload = if output.payload().is_empty() {
            serde_json::Value::Null
        } else {
            serde_json::from_slice(output.payload()).map_err(|error| {
                TaskInvocationFailure::new(
                    "States.Runtime",
                    format!("Lambda response is not valid JSON: {error}"),
                    request.resource(),
                    request.resource_type(),
                )
            })?
        };

        Ok(TaskInvocationResult::new(
            output.executed_version(),
            payload,
            output.status_code(),
        ))
    }
}

#[cfg(feature = "step-functions")]
fn lambda_task_failure_details(
    error: &lambda::LambdaError,
) -> (&'static str, String) {
    match error {
        lambda::LambdaError::Blob(source)
        | lambda::LambdaError::InvokeBackend(source)
        | lambda::LambdaError::Random(source) => {
            ("ServiceException", source.to_string())
        }
        lambda::LambdaError::Internal { message } => {
            ("ServiceException", message.clone())
        }
        lambda::LambdaError::Store(source) => {
            ("ServiceException", source.to_string())
        }
        lambda::LambdaError::AccessDenied { message } => {
            ("AccessDeniedException", message.clone())
        }
        lambda::LambdaError::InvalidParameterValue { message } => {
            ("InvalidParameterValueException", message.clone())
        }
        lambda::LambdaError::InvalidRequestContent { message } => {
            ("InvalidRequestContentException", message.clone())
        }
        lambda::LambdaError::RequestTooLarge { message } => {
            ("RequestTooLargeException", message.clone())
        }
        lambda::LambdaError::ResourceConflict { message } => {
            ("ResourceConflictException", message.clone())
        }
        lambda::LambdaError::ResourceNotFound { message } => {
            ("ResourceNotFoundException", message.clone())
        }
        lambda::LambdaError::PreconditionFailed { message } => {
            ("PreconditionFailedException", message.clone())
        }
        lambda::LambdaError::UnsupportedMediaType { message } => {
            ("UnsupportedMediaTypeException", message.clone())
        }
        lambda::LambdaError::UnsupportedOperation { message } => {
            ("UnsupportedOperationException", message.clone())
        }
        lambda::LambdaError::Validation { message } => {
            ("ValidationException", message.clone())
        }
    }
}
