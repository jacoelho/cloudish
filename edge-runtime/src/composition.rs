#[cfg(feature = "lambda")]
use crate::ManagedBackgroundTasks;
#[cfg(feature = "apigateway")]
use apigateway::{
    ApiGatewayScope, ExecuteApiError, ExecuteApiIntegrationExecutor,
    ExecuteApiIntegrationPlan, ExecuteApiInvocation,
    ExecuteApiPreparedResponse, map_lambda_proxy_response,
    map_lambda_proxy_response_v2,
};
use aws::ServiceName;
use aws::{Arn, HttpForwardRequest, HttpForwarder, SharedAdvertisedEdge};
#[cfg(feature = "lambda")]
use aws::{BackgroundScheduler, InfrastructureError};
#[cfg(feature = "eventbridge")]
use eventbridge::{
    EventBridgeDeliveryDispatcher, EventBridgeError,
    EventBridgePlannedDelivery, EventBridgeScope, EventBridgeTarget,
};
#[cfg(any(
    feature = "apigateway",
    feature = "eventbridge",
    feature = "lambda"
))]
use lambda::LambdaService;
#[cfg(feature = "apigateway")]
use lambda::{ApiGatewayInvokeInput, LambdaScope};
#[cfg(any(feature = "sns", feature = "step-functions"))]
use lambda::{InvokeInput, LambdaInvocationType};
#[cfg(feature = "s3")]
use s3::{S3Error, S3EventNotification, S3NotificationTransport, S3Scope};
#[cfg(feature = "sns")]
use sha1::{Digest, Sha1};
#[cfg(feature = "eventbridge")]
use sns::PublishInput;
#[cfg(feature = "sns")]
use sns::{
    ConfirmationDelivery, DeliveryEndpoint, PlannedDelivery,
    SnsDeliveryTransport, SnsHttpSigner, SnsIdentifierSource, SnsService,
};
#[cfg(any(feature = "eventbridge", feature = "s3", feature = "sns"))]
use sqs::{SendMessageInput, SqsService};
use std::collections::BTreeMap;
use std::sync::Arc;
#[cfg(feature = "eventbridge")]
use std::thread;
#[cfg(feature = "lambda")]
use std::time::Duration;
#[cfg(feature = "sns")]
use std::time::SystemTime;
#[cfg(feature = "step-functions")]
use step_functions::{
    StepFunctionsScope, StepFunctionsTaskAdapter, TaskInvocationFailure,
    TaskInvocationRequest, TaskInvocationResult,
};
#[cfg(any(feature = "s3", feature = "sns"))]
use time::OffsetDateTime;
#[cfg(any(feature = "s3", feature = "sns"))]
use time::format_description::well_known::Rfc3339;

#[cfg(feature = "lambda")]
pub(crate) fn start_lambda_background_tasks(
    lambda: LambdaService,
    scheduler: Arc<dyn BackgroundScheduler>,
) -> Result<ManagedBackgroundTasks, InfrastructureError> {
    let async_service = lambda.clone();
    let async_handle = scheduler.schedule_repeating(
        "lambda-async-invocations".to_owned(),
        Duration::from_millis(25),
        Arc::new(move || {
            let _ = async_service.run_async_invocation_cycle();
        }),
    )?;
    let mapping_service = lambda;
    let mapping_handle = scheduler.schedule_repeating(
        "lambda-sqs-mappings".to_owned(),
        Duration::from_millis(25),
        Arc::new(move || {
            let _ = mapping_service.run_sqs_event_source_mapping_cycle();
        }),
    )?;

    Ok(ManagedBackgroundTasks::new(vec![async_handle, mapping_handle]))
}

#[cfg(feature = "apigateway")]
#[derive(Clone)]
pub struct ApiGatewayIntegrationExecutor {
    http_forwarder: Option<Arc<dyn HttpForwarder + Send + Sync>>,
    lambda: LambdaService,
}

#[cfg(feature = "apigateway")]
impl ApiGatewayIntegrationExecutor {
    pub fn new(
        lambda: LambdaService,
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
                    .forward(request)
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
                let output = self
                    .lambda
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

#[cfg(feature = "sns")]
#[derive(Clone, Default)]
pub struct SnsServiceDependencies {
    pub advertised_edge: SharedAdvertisedEdge,
    pub http_forwarder: Option<Arc<dyn HttpForwarder + Send + Sync>>,
    pub http_signer: Option<Arc<dyn SnsHttpSigner + Send + Sync>>,
    pub lambda: Option<LambdaService>,
    pub sqs: Option<SqsService>,
}

#[cfg(feature = "sns")]
pub fn build_sns_service(
    advertised_edge: SharedAdvertisedEdge,
    time_source: Arc<dyn Fn() -> SystemTime + Send + Sync>,
    identifier_source: Arc<dyn SnsIdentifierSource + Send + Sync>,
    mut dependencies: SnsServiceDependencies,
) -> SnsService {
    dependencies.advertised_edge = advertised_edge;
    if dependencies.http_signer.is_none() {
        dependencies.http_signer =
            Some(Arc::new(CloudishSnsHttpSigner::default()));
    }
    SnsService::with_transport(
        time_source,
        identifier_source,
        Arc::new(SnsDeliveryDispatcher { dependencies }),
    )
}

#[cfg(feature = "sns")]
#[derive(Clone, Default)]
struct CloudishSnsHttpSigner;

#[cfg(feature = "sns")]
impl SnsHttpSigner for CloudishSnsHttpSigner {
    fn sign(&self, string_to_sign: &str) -> String {
        let mut hasher = Sha1::new();
        hasher.update(string_to_sign.as_bytes());
        format!("{:x}", hasher.finalize())
    }

    fn signing_cert_url(&self) -> String {
        "https://cloudish.invalid/sns.pem".to_owned()
    }
}

#[cfg(feature = "eventbridge")]
#[derive(Clone, Default)]
pub(crate) struct EventBridgeDispatcherDependencies {
    pub lambda: Option<LambdaService>,
    pub sns: Option<SnsService>,
    pub sqs: Option<SqsService>,
}

#[cfg(feature = "eventbridge")]
pub(crate) fn build_eventbridge_dispatcher(
    dependencies: EventBridgeDispatcherDependencies,
) -> Arc<dyn EventBridgeDeliveryDispatcher> {
    Arc::new(EventBridgeDispatcher { dependencies })
}

#[cfg(feature = "s3")]
#[derive(Clone, Default)]
pub struct S3NotificationDispatcher {
    pub sns: Option<SnsService>,
    pub sqs: Option<SqsService>,
}

#[cfg(feature = "s3")]
impl S3NotificationTransport for S3NotificationDispatcher {
    fn publish(&self, notification: &S3EventNotification) {
        let body = s3_notification_message_body(notification);
        match notification.destination_arn.service() {
            ServiceName::Sns => {
                let Some(sns) = self.sns.as_ref() else {
                    return;
                };
                let _ = sns.publish(PublishInput {
                    message: body,
                    message_attributes: BTreeMap::new(),
                    message_deduplication_id: None,
                    message_group_id: None,
                    subject: None,
                    target_arn: None,
                    topic_arn: Some(notification.destination_arn.clone()),
                });
            }
            ServiceName::Sqs => {
                let Some(sqs) = self.sqs.as_ref() else {
                    return;
                };
                let Ok(queue) = sqs::SqsQueueIdentity::from_arn(
                    &notification.destination_arn,
                ) else {
                    return;
                };
                let _ = sqs.send_message(
                    &queue,
                    SendMessageInput {
                        body,
                        delay_seconds: None,
                        message_deduplication_id: None,
                        message_group_id: None,
                    },
                );
            }
            _ => {}
        }
    }

    fn validate_destination(
        &self,
        _scope: &S3Scope,
        _bucket_name: &str,
        _bucket_owner_account_id: &aws::AccountId,
        _bucket_region: &aws::RegionId,
        destination_arn: &Arn,
    ) -> Result<(), S3Error> {
        match destination_arn.service() {
            ServiceName::Sns => {
                let sns = self
                    .sns
                    .as_ref()
                    .ok_or_else(|| invalid_destination(destination_arn))?;
                sns.get_topic_attributes(destination_arn)
                    .map(|_| ())
                    .map_err(|_| invalid_destination(destination_arn))
            }
            ServiceName::Sqs => {
                let sqs = self
                    .sqs
                    .as_ref()
                    .ok_or_else(|| invalid_destination(destination_arn))?;
                let queue =
                    sqs::SqsQueueIdentity::from_arn(destination_arn)
                        .map_err(|_| invalid_destination(destination_arn))?;
                sqs.get_queue_attributes(&queue, &Vec::new())
                    .map(|_| ())
                    .map_err(|_| invalid_destination(destination_arn))
            }
            _ => Err(invalid_destination(destination_arn)),
        }
    }
}

#[cfg(feature = "sns")]
#[derive(Clone)]
struct SnsDeliveryDispatcher {
    dependencies: SnsServiceDependencies,
}

#[cfg(feature = "eventbridge")]
#[derive(Clone)]
struct EventBridgeDispatcher {
    dependencies: EventBridgeDispatcherDependencies,
}

#[cfg(feature = "sns")]
impl SnsDeliveryTransport for SnsDeliveryDispatcher {
    fn deliver_confirmation(
        &self,
        delivery: &ConfirmationDelivery,
        message_id: String,
        timestamp: String,
    ) {
        let Some(forwarder) = self.dependencies.http_forwarder.as_ref() else {
            return;
        };
        let advertised_edge = self.dependencies.advertised_edge.current();
        let request = delivery.http_request(
            &message_id,
            &timestamp,
            &advertised_edge,
            self.http_signer().as_ref(),
        );
        let _ = forwarder.forward(&self.http_forward_request(
            delivery.endpoint.endpoint.clone(),
            delivery.endpoint.path.clone(),
            &request,
        ));
    }

    fn deliver_notification(&self, delivery: &PlannedDelivery) {
        let advertised_edge = self.dependencies.advertised_edge.current();
        match &delivery.endpoint {
            DeliveryEndpoint::Http(parsed) => {
                let Some(forwarder) =
                    self.dependencies.http_forwarder.as_ref()
                else {
                    return;
                };
                let request = delivery.payload.http_request(
                    delivery.raw_message_delivery,
                    &advertised_edge,
                    self.http_signer().as_ref(),
                );
                let _ = forwarder.forward(&self.http_forward_request(
                    parsed.endpoint.clone(),
                    parsed.path.clone(),
                    &request,
                ));
            }
            DeliveryEndpoint::Lambda(arn) => {
                let Some(lambda) = self.dependencies.lambda.as_ref() else {
                    return;
                };
                let Some(account_id) = arn.account_id().cloned() else {
                    return;
                };
                let Some(region) = arn.region().cloned() else {
                    return;
                };
                let _ = lambda.invoke(
                    &LambdaScope::new(account_id, region),
                    &arn.to_string(),
                    None,
                    InvokeInput {
                        invocation_type: LambdaInvocationType::Event,
                        payload: delivery
                            .payload
                            .lambda_event(&advertised_edge),
                    },
                );
            }
            DeliveryEndpoint::Sqs(queue_arn) => {
                let Some(sqs) = self.dependencies.sqs.as_ref() else {
                    return;
                };
                let Ok(queue) = sqs::SqsQueueIdentity::from_arn(queue_arn)
                else {
                    return;
                };
                let _ = sqs.send_message(
                    &queue,
                    SendMessageInput {
                        body: delivery.payload.sqs_body(
                            delivery.raw_message_delivery,
                            &advertised_edge,
                        ),
                        delay_seconds: None,
                        message_deduplication_id: delivery
                            .payload
                            .message_deduplication_id
                            .clone(),
                        message_group_id: delivery
                            .payload
                            .message_group_id
                            .clone(),
                    },
                );
            }
        }
    }
}

#[cfg(feature = "sns")]
impl SnsDeliveryDispatcher {
    fn http_forward_request(
        &self,
        endpoint: aws::Endpoint,
        path: String,
        request: &sns::SnsHttpRequest,
    ) -> HttpForwardRequest {
        let mut forward = HttpForwardRequest::new(endpoint, "POST", path)
            .with_body(request.body().to_vec());
        for (name, value) in request.headers() {
            forward = forward.with_header(name.clone(), value.clone());
        }

        forward
    }

    fn http_signer(&self) -> Arc<dyn SnsHttpSigner + Send + Sync> {
        self.dependencies
            .http_signer
            .clone()
            .unwrap_or_else(|| Arc::new(CloudishSnsHttpSigner::default()))
    }
}

#[cfg(feature = "eventbridge")]
impl EventBridgeDeliveryDispatcher for EventBridgeDispatcher {
    fn validate_target(
        &self,
        _scope: &EventBridgeScope,
        _rule_arn: &Arn,
        target: &EventBridgeTarget,
    ) -> Result<(), EventBridgeError> {
        match target.arn.service() {
            ServiceName::Lambda => {
                let Some(lambda) = self.dependencies.lambda.as_ref() else {
                    return Err(missing_target_error(&target.arn));
                };
                let scope = target_scope(&target.arn)?;
                lambda
                    .get_function(&scope, &target.arn.to_string(), None)
                    .map(|_| ())
                    .map_err(|_| missing_target_error(&target.arn))
            }
            ServiceName::Sns => {
                let Some(sns) = self.dependencies.sns.as_ref() else {
                    return Err(missing_target_error(&target.arn));
                };
                sns.get_topic_attributes(&target.arn)
                    .map(|_| ())
                    .map_err(|_| missing_target_error(&target.arn))
            }
            ServiceName::Sqs => {
                let Some(sqs) = self.dependencies.sqs.as_ref() else {
                    return Err(missing_target_error(&target.arn));
                };
                let queue = sqs::SqsQueueIdentity::from_arn(&target.arn)
                    .map_err(|_| missing_target_error(&target.arn))?;
                sqs.get_queue_attributes(&queue, &[])
                    .map(|_| ())
                    .map_err(|_| missing_target_error(&target.arn))
            }
            _ => Err(EventBridgeError::UnsupportedOperation {
                message: format!(
                    "Target Arn service {} is not supported by this Cloudish EventBridge subset.",
                    target.arn.service().as_str()
                ),
            }),
        }
    }

    fn dispatch(&self, deliveries: Vec<EventBridgePlannedDelivery>) {
        let dependencies = self.dependencies.clone();
        let _ = thread::Builder::new()
            .name("eventbridge-dispatch".to_owned())
            .spawn(move || {
                for delivery in deliveries {
                    dispatch_eventbridge_delivery(&dependencies, delivery);
                }
            });
    }
}

#[cfg(feature = "step-functions")]
#[derive(Clone)]
pub struct LambdaStepFunctionsTaskAdapter {
    lambda: LambdaService,
}

#[cfg(feature = "step-functions")]
impl LambdaStepFunctionsTaskAdapter {
    pub fn new(lambda: LambdaService) -> Self {
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
        let output = self
            .lambda
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

#[cfg(feature = "s3")]
fn invalid_destination(destination_arn: &Arn) -> S3Error {
    S3Error::InvalidArgument {
        code: "InvalidArgument",
        message: format!("Unable to validate destination {destination_arn}."),
        status_code: 400,
    }
}

#[cfg(feature = "s3")]
fn s3_notification_message_body(notification: &S3EventNotification) -> String {
    serde_json::json!({
        "Records": [{
            "eventVersion": "2.1",
            "eventSource": "aws:s3",
            "awsRegion": notification.region,
            "eventTime": format_epoch_rfc3339(notification.event_time_epoch_seconds),
            "eventName": notification.event_name,
            "userIdentity": {
                "principalId": notification.requester_account_id,
            },
            "requestParameters": {
                "sourceIPAddress": "127.0.0.1",
            },
            "responseElements": {
                "x-amz-request-id": "0000000000000000",
                "x-amz-id-2": "0000000000000000",
            },
            "s3": {
                "s3SchemaVersion": "1.0",
                "configurationId": notification.configuration_id,
                "bucket": {
                    "name": notification.bucket_name,
                    "ownerIdentity": {
                        "principalId": notification.bucket_owner_account_id,
                    },
                    "arn": notification.bucket_arn,
                },
                "object": {
                    "key": urlencoding::encode(&notification.key).into_owned(),
                    "size": notification.size,
                    "eTag": notification.etag,
                    "versionId": notification.version_id,
                    "sequencer": notification.sequencer,
                },
            },
        }],
    })
    .to_string()
}

#[cfg(feature = "eventbridge")]
fn dispatch_eventbridge_delivery(
    dependencies: &EventBridgeDispatcherDependencies,
    delivery: EventBridgePlannedDelivery,
) {
    match delivery.target.arn.service() {
        ServiceName::Lambda => {
            let Some(lambda) = dependencies.lambda.as_ref() else {
                return;
            };
            let Ok(scope) = target_scope(&delivery.target.arn) else {
                return;
            };
            let _ = lambda.invoke_eventbridge(
                &scope,
                &delivery.target.arn.to_string(),
                &delivery.rule_arn.to_string(),
                delivery.payload,
            );
        }
        ServiceName::Sns => {
            let Some(sns) = dependencies.sns.as_ref() else {
                return;
            };
            let scope = target_sns_scope(&delivery.target.arn);
            let _ = sns.publish(PublishInput {
                message: String::from_utf8_lossy(&delivery.payload)
                    .into_owned(),
                message_attributes: BTreeMap::new(),
                message_deduplication_id: None,
                message_group_id: None,
                subject: None,
                target_arn: None,
                topic_arn: Some(scope),
            });
        }
        ServiceName::Sqs => {
            let Some(sqs) = dependencies.sqs.as_ref() else {
                return;
            };
            let Ok(queue) =
                sqs::SqsQueueIdentity::from_arn(&delivery.target.arn)
            else {
                return;
            };
            let _ = sqs.send_message(
                &queue,
                SendMessageInput {
                    body: String::from_utf8_lossy(&delivery.payload)
                        .into_owned(),
                    delay_seconds: None,
                    message_deduplication_id: None,
                    message_group_id: None,
                },
            );
        }
        _ => {}
    }
}

#[cfg(any(feature = "s3", feature = "sns"))]
fn format_epoch_rfc3339(epoch_seconds: u64) -> String {
    OffsetDateTime::from_unix_timestamp(epoch_seconds as i64)
        .unwrap_or(OffsetDateTime::UNIX_EPOCH)
        .format(&Rfc3339)
        .unwrap_or_else(|_| "1970-01-01T00:00:00Z".to_owned())
}

#[cfg(feature = "eventbridge")]
fn target_scope(target_arn: &Arn) -> Result<LambdaScope, EventBridgeError> {
    let Some(account_id) = target_arn.account_id().cloned() else {
        return Err(missing_target_error(target_arn));
    };
    let Some(region) = target_arn.region().cloned() else {
        return Err(missing_target_error(target_arn));
    };

    Ok(LambdaScope::new(account_id, region))
}

#[cfg(feature = "eventbridge")]
fn target_sns_scope(target_arn: &Arn) -> Arn {
    target_arn.clone()
}

#[cfg(feature = "eventbridge")]
fn missing_target_error(target_arn: &Arn) -> EventBridgeError {
    EventBridgeError::ResourceNotFound {
        message: format!("Target Arn {target_arn} does not exist."),
    }
}

#[cfg(all(test, feature = "all-services"))]
mod tests {
    use super::{
        LambdaStepFunctionsTaskAdapter, S3NotificationDispatcher,
        SnsServiceDependencies, build_sns_service,
        start_lambda_background_tasks,
    };
    use crate::{
        FixedClock, ManualBackgroundScheduler, MemoryBlobStore,
        SequenceRandomSource,
    };
    use aws::{
        AccountId, Arn, BackgroundScheduler, HttpForwardResponse,
        HttpForwarder, InfrastructureError, LambdaExecutor,
        LambdaInvocationRequest, LambdaInvocationResult, RegionId,
        SharedAdvertisedEdge,
    };
    use iam::{CreateRoleInput, IamScope, IamService};
    use lambda::{
        CreateFunctionInput, LambdaCodeInput, LambdaPackageType, LambdaScope,
        LambdaService, LambdaServiceDependencies,
    };
    use s3::{
        S3EventNotification, S3NotificationTransport, S3Scope, S3Service,
    };
    use serde_json::json;
    use sns::{CreateTopicInput, SnsHttpSigner, SnsScope, SubscribeInput};
    use sqs::{CreateQueueInput, ReceiveMessageInput, SqsScope, SqsService};
    use std::collections::BTreeMap;
    use std::error::Error;
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, UNIX_EPOCH};
    use step_functions::{
        CreateStateMachineInput, DescribeExecutionInput, ExecutionStatus,
        StartExecutionInput, StepFunctionsExecutionSpawner,
        StepFunctionsScope, StepFunctionsService,
        StepFunctionsServiceDependencies, StepFunctionsSleeper,
    };
    use storage::{StorageConfig, StorageFactory, StorageMode};

    type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct RecordedForwardRequest {
        body: String,
        headers: Vec<(String, String)>,
    }

    #[derive(Debug, Default)]
    struct RecordingForwarder {
        requests: Mutex<Vec<RecordedForwardRequest>>,
    }

    impl RecordingForwarder {
        fn bodies(&self) -> Vec<String> {
            self.requests
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .iter()
                .map(|request| request.body.clone())
                .collect()
        }

        fn requests(&self) -> Vec<RecordedForwardRequest> {
            self.requests
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone()
        }
    }

    impl HttpForwarder for RecordingForwarder {
        fn forward(
            &self,
            request: &aws::HttpForwardRequest,
        ) -> Result<HttpForwardResponse, InfrastructureError> {
            self.requests
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(RecordedForwardRequest {
                    body: String::from_utf8_lossy(request.body()).into_owned(),
                    headers: request.headers().to_vec(),
                });
            Ok(HttpForwardResponse::new(200, "OK", Vec::new(), Vec::new()))
        }
    }

    #[derive(Debug, Clone)]
    struct TestSnsHttpSigner {
        signature: String,
        signing_cert_url: String,
    }

    impl SnsHttpSigner for TestSnsHttpSigner {
        fn sign(&self, _string_to_sign: &str) -> String {
            self.signature.clone()
        }

        fn signing_cert_url(&self) -> String {
            self.signing_cert_url.clone()
        }
    }

    #[derive(Debug, Clone)]
    struct FakeLambdaExecutor {
        response: Arc<Mutex<LambdaInvocationResult>>,
    }

    impl FakeLambdaExecutor {
        fn success(payload: serde_json::Value) -> TestResult<Self> {
            let payload = serde_json::to_vec(&payload)?;

            Ok(Self {
                response: Arc::new(Mutex::new(LambdaInvocationResult::new(
                    payload,
                    Option::<String>::None,
                ))),
            })
        }

        fn function_error(payload: &str, error: &str) -> Self {
            Self {
                response: Arc::new(Mutex::new(LambdaInvocationResult::new(
                    payload.as_bytes().to_vec(),
                    Some(error.to_owned()),
                ))),
            }
        }
    }

    impl LambdaExecutor for FakeLambdaExecutor {
        fn invoke(
            &self,
            _request: &LambdaInvocationRequest,
        ) -> Result<LambdaInvocationResult, InfrastructureError> {
            Ok(self
                .response
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone())
        }

        fn invoke_async(
            &self,
            _request: LambdaInvocationRequest,
        ) -> Result<(), InfrastructureError> {
            Ok(())
        }

        fn validate_zip(
            &self,
            _runtime: &str,
            _handler: &str,
            _archive: &[u8],
        ) -> Result<(), InfrastructureError> {
            Ok(())
        }
    }

    #[derive(Debug, Default)]
    struct InlineSpawner;

    impl StepFunctionsExecutionSpawner for InlineSpawner {
        fn spawn(
            &self,
            _task_name: &str,
            task: Box<dyn FnOnce() + Send>,
        ) -> Result<(), InfrastructureError> {
            task();
            Ok(())
        }
    }

    #[derive(Debug, Default)]
    struct NoopSleeper;

    impl StepFunctionsSleeper for NoopSleeper {
        fn sleep(
            &self,
            _duration: Duration,
        ) -> Result<(), InfrastructureError> {
            Ok(())
        }
    }

    fn account_id() -> TestResult<AccountId> {
        Ok("000000000000".parse::<AccountId>()?)
    }

    fn region_id() -> TestResult<RegionId> {
        Ok("eu-west-2".parse::<RegionId>()?)
    }

    fn iam() -> TestResult<IamService> {
        let iam = IamService::new();
        iam.create_role(
            &IamScope::new(account_id()?, region_id()?),
            CreateRoleInput {
                assume_role_policy_document: r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}"#.to_owned(),
                description: String::new(),
                max_session_duration: 3_600,
                path: "/".to_owned(),
                role_name: "lambda-role".to_owned(),
                tags: Vec::new(),
            },
        )?;
        Ok(iam)
    }

    fn lambda_scope() -> TestResult<LambdaScope> {
        Ok(LambdaScope::new(account_id()?, region_id()?))
    }

    fn sns_scope() -> TestResult<SnsScope> {
        Ok(SnsScope::new(account_id()?, region_id()?))
    }

    fn sqs_scope() -> TestResult<SqsScope> {
        Ok(SqsScope::new(account_id()?, region_id()?))
    }

    fn s3_scope() -> TestResult<S3Scope> {
        Ok(S3Scope::new(account_id()?, region_id()?))
    }

    fn step_functions_scope() -> TestResult<StepFunctionsScope> {
        Ok(StepFunctionsScope::new(account_id()?, region_id()?))
    }

    fn queue_arn(queue: &sqs::SqsQueueIdentity) -> TestResult<Arn> {
        Ok(format!(
            "arn:aws:sqs:{}:{}:{}",
            queue.region().as_str(),
            queue.account_id().as_str(),
            queue.queue_name(),
        )
        .parse()?)
    }

    fn step_functions_service(
        label: &str,
        task_adapter: Arc<dyn step_functions::StepFunctionsTaskAdapter>,
    ) -> StepFunctionsService {
        StepFunctionsService::new(
            &StorageFactory::new(StorageConfig::new(
                std::env::temp_dir()
                    .join(format!("app-step-functions-{label}")),
                StorageMode::Memory,
            )),
            StepFunctionsServiceDependencies {
                clock: Arc::new(FixedClock::new(
                    UNIX_EPOCH + Duration::from_secs(1),
                )),
                execution_spawner: Arc::new(InlineSpawner),
                sleeper: Arc::new(NoopSleeper),
                task_adapter,
            },
        )
    }

    fn lambda_service(
        label: &str,
        executor: Arc<dyn LambdaExecutor + Send + Sync>,
        sqs: SqsService,
    ) -> TestResult<LambdaService> {
        let factory = StorageFactory::new(StorageConfig::new(
            std::env::temp_dir().join(format!("app-composition-{label}")),
            StorageMode::Memory,
        ));
        let s3 = S3Service::new(
            &factory,
            Arc::new(MemoryBlobStore::new()),
            Arc::new(FixedClock::new(UNIX_EPOCH)),
        )?;
        let iam = iam()?;
        let lambda = LambdaService::new(
            &factory,
            LambdaServiceDependencies {
                blob_store: Arc::new(MemoryBlobStore::new()),
                executor,
                iam,
                s3,
                sqs,
                clock: Arc::new(FixedClock::new(UNIX_EPOCH)),
                random: Arc::new(SequenceRandomSource::new(vec![
                    0x10, 0x20, 0x30, 0x40,
                ])),
            },
        )?;
        lambda.create_function(
            &lambda_scope()?,
            CreateFunctionInput {
                code: LambdaCodeInput::InlineZip { archive: vec![1, 2, 3] },
                dead_letter_target_arn: None,
                description: None,
                environment: BTreeMap::new(),
                function_name: "demo".to_owned(),
                handler: Some("bootstrap.handler".to_owned()),
                memory_size: None,
                package_type: LambdaPackageType::Zip,
                publish: false,
                role: "arn:aws:iam::000000000000:role/lambda-role".to_owned(),
                runtime: Some("provided.al2".to_owned()),
                timeout: None,
            },
        )?;
        Ok(lambda)
    }

    #[test]
    fn lambda_background_tasks_are_owned_by_app_runtime() {
        let scheduler = Arc::new(ManualBackgroundScheduler::new());
        let sqs = SqsService::new();
        let lambda = lambda_service(
            "lambda-background",
            Arc::new(
                FakeLambdaExecutor::success(json!({ "ok": true }))
                    .expect("payload should serialize"),
            ),
            sqs,
        )
        .expect("lambda service should build");

        let tasks = start_lambda_background_tasks(
            lambda,
            Arc::clone(&scheduler) as Arc<dyn BackgroundScheduler>,
        )
        .expect("background tasks should start");

        assert_eq!(scheduler.active_task_count(), 2);
        scheduler.run_pending();

        tasks.cancel().expect("tasks should cancel");
        assert_eq!(scheduler.active_task_count(), 0);
    }

    #[test]
    fn s3_notification_dispatcher_validates_and_delivers_sqs_targets() {
        let sqs = SqsService::new();
        let queue = sqs
            .create_queue(
                &sqs_scope().expect("SQS scope should build"),
                CreateQueueInput {
                    queue_name: "events".to_owned(),
                    attributes: BTreeMap::new(),
                },
            )
            .expect("queue should create");
        let queue_arn = queue_arn(&queue).expect("queue ARN should build");
        let dispatcher =
            S3NotificationDispatcher { sns: None, sqs: Some(sqs.clone()) };
        let notification = S3EventNotification {
            bucket_arn: "arn:aws:s3:::demo-bucket"
                .parse()
                .expect("bucket ARN should parse"),
            bucket_name: "demo-bucket".to_owned(),
            bucket_owner_account_id: "000000000000"
                .parse()
                .expect("account should parse"),
            configuration_id: Some("config".to_owned()),
            destination_arn: queue_arn.clone(),
            etag: Some("etag".to_owned()),
            event_name: "ObjectCreated:Put".to_owned(),
            event_time_epoch_seconds: 1,
            key: "path/file.txt".to_owned(),
            region: "eu-west-2".parse().expect("region should parse"),
            requester_account_id: "000000000000"
                .parse()
                .expect("account should parse"),
            sequencer: "1".to_owned(),
            size: 3,
            version_id: Some("v1".to_owned()),
        };

        dispatcher
            .validate_destination(
                &s3_scope().expect("S3 scope should build"),
                "demo-bucket",
                &"000000000000".parse().expect("account should parse"),
                &"eu-west-2".parse().expect("region should parse"),
                &queue_arn,
            )
            .expect("queue destination should validate");
        dispatcher.publish(&notification);

        let received = sqs
            .receive_message(
                &queue,
                ReceiveMessageInput {
                    max_number_of_messages: None,
                    visibility_timeout: None,
                    wait_time_seconds: None,
                },
            )
            .expect("receive should succeed");

        assert_eq!(received.len(), 1);
        assert!(received[0].body.contains("\"eventSource\":\"aws:s3\""));
    }

    #[test]
    fn sns_delivery_dispatcher_forwards_http_confirmation_and_notification() {
        let forwarder = Arc::new(RecordingForwarder::default());
        let sqs = SqsService::new();
        let lambda = lambda_service(
            "sns-http",
            Arc::new(
                FakeLambdaExecutor::success(json!({ "ok": true }))
                    .expect("payload should serialize"),
            ),
            sqs.clone(),
        )
        .expect("lambda service should build");
        let sns = build_sns_service(
            SharedAdvertisedEdge::default(),
            Arc::new(|| UNIX_EPOCH + Duration::from_secs(1)),
            Arc::new(sns::SequentialSnsIdentifierSource::default()),
            SnsServiceDependencies {
                advertised_edge: SharedAdvertisedEdge::default(),
                http_forwarder: Some(Arc::clone(&forwarder)
                    as Arc<dyn HttpForwarder + Send + Sync>),
                http_signer: Some(Arc::new(TestSnsHttpSigner {
                    signature: "signed-by-test".to_owned(),
                    signing_cert_url: "https://example.com/cert.pem"
                        .to_owned(),
                })),
                lambda: Some(lambda),
                sqs: Some(sqs),
            },
        );
        let topic_arn = sns
            .create_topic(
                &sns_scope().expect("SNS scope should build"),
                CreateTopicInput {
                    attributes: BTreeMap::new(),
                    name: "topic".to_owned(),
                },
            )
            .expect("topic should create");

        sns.subscribe(SubscribeInput {
            attributes: BTreeMap::new(),
            endpoint: "http://example.com/hooks".to_owned(),
            protocol: "http".to_owned(),
            return_subscription_arn: false,
            topic_arn: topic_arn.clone(),
        })
        .expect("http subscription should create");
        let confirmation = forwarder.bodies();
        let confirmation_body =
            serde_json::from_str::<serde_json::Value>(&confirmation[0])
                .expect("confirmation body should be json");
        let token = confirmation_body["Token"]
            .as_str()
            .expect("confirmation token should be present")
            .to_owned();
        sns.confirm_subscription(&topic_arn, &token)
            .expect("subscription should confirm");
        sns.publish(sns::PublishInput {
            message: "hello".to_owned(),
            message_attributes: BTreeMap::new(),
            message_deduplication_id: None,
            message_group_id: None,
            subject: None,
            target_arn: None,
            topic_arn: Some(topic_arn),
        })
        .expect("publish should succeed");

        let requests = forwarder.requests();
        assert_eq!(requests.len(), 2);
        assert!(
            requests[0].body.contains("\"Type\":\"SubscriptionConfirmation\"")
        );
        assert!(requests[1].body.contains("\"Type\":\"Notification\""));
        assert!(requests[0].headers.contains(&(
            "x-amz-sns-message-type".to_owned(),
            "SubscriptionConfirmation".to_owned(),
        )));
        assert!(requests[1].headers.contains(&(
            "x-amz-sns-message-type".to_owned(),
            "Notification".to_owned(),
        )));
        assert!(requests[1].headers.contains(&(
            "Content-Type".to_owned(),
            "text/plain; charset=UTF-8".to_owned(),
        )));
        assert!(requests[0].body.contains("\"Signature\":\"signed-by-test\""));
        assert!(requests[1].body.contains("\"Signature\":\"signed-by-test\""));
        assert!(
            requests[1].body.contains(
                "\"SigningCertURL\":\"https://example.com/cert.pem\""
            )
        );
        assert!(!requests[0].body.contains("\"Signature\":\"CLOUDISH\""));
    }

    #[test]
    fn sns_delivery_dispatcher_forwards_http_headers_without_optional_dependencies()
     {
        let forwarder = Arc::new(RecordingForwarder::default());
        let signer = Arc::new(TestSnsHttpSigner {
            signature: "signature-a".to_owned(),
            signing_cert_url: "https://example.com/cert-a.pem".to_owned(),
        });
        let sns = build_sns_service(
            SharedAdvertisedEdge::default(),
            Arc::new(|| UNIX_EPOCH + Duration::from_secs(1)),
            Arc::new(sns::SequentialSnsIdentifierSource::default()),
            SnsServiceDependencies {
                advertised_edge: SharedAdvertisedEdge::default(),
                http_forwarder: Some(Arc::clone(&forwarder)
                    as Arc<dyn HttpForwarder + Send + Sync>),
                http_signer: Some(signer),
                lambda: None,
                sqs: None,
            },
        );
        let topic_arn = sns
            .create_topic(
                &sns_scope().expect("SNS scope should build"),
                CreateTopicInput {
                    attributes: BTreeMap::new(),
                    name: "headers".to_owned(),
                },
            )
            .expect("topic should create");

        sns.subscribe(SubscribeInput {
            attributes: BTreeMap::new(),
            endpoint: "http://example.com/hooks".to_owned(),
            protocol: "http".to_owned(),
            return_subscription_arn: false,
            topic_arn: topic_arn.clone(),
        })
        .expect("http subscription should create");
        let confirmation_body = forwarder.bodies()[0].clone();
        let confirmation: serde_json::Value =
            serde_json::from_str(&confirmation_body)
                .expect("confirmation body should be JSON");
        let token = confirmation["Token"]
            .as_str()
            .expect("confirmation token should be present")
            .to_owned();
        sns.confirm_subscription(&topic_arn, &token)
            .expect("subscription should confirm");
        sns.publish(sns::PublishInput {
            message: "hello".to_owned(),
            message_attributes: BTreeMap::new(),
            message_deduplication_id: None,
            message_group_id: None,
            subject: None,
            target_arn: None,
            topic_arn: Some(topic_arn),
        })
        .expect("publish should succeed");

        let requests = forwarder.requests();
        assert_eq!(requests.len(), 2);
        assert!(requests[0].headers.contains(&(
            "x-amz-sns-topic-arn".to_owned(),
            "arn:aws:sns:eu-west-2:000000000000:headers".to_owned(),
        )));
        assert!(
            requests[1]
                .headers
                .iter()
                .any(|(name, _)| name == "x-amz-sns-subscription-arn")
        );
        assert!(requests[0].body.contains("\"Signature\":\"signature-a\""));
        assert!(requests[1].body.contains("\"Signature\":\"signature-a\""));
    }

    #[test]
    fn lambda_step_functions_task_adapter_invokes_lambda_and_returns_json() {
        let sqs = SqsService::new();
        let lambda = lambda_service(
            "step-functions-success",
            Arc::new(
                FakeLambdaExecutor::success(json!({ "ok": true }))
                    .expect("payload should serialize"),
            ),
            sqs,
        )
        .expect("lambda service should build");
        let function_arn =
            "arn:aws:lambda:eu-west-2:000000000000:function:demo".to_owned();
        let service = step_functions_service(
            "success",
            Arc::new(LambdaStepFunctionsTaskAdapter::new(lambda)),
        );
        let definition = json!({
            "StartAt": "Task",
            "States": {
                "Task": {
                    "Type": "Task",
                    "Resource": function_arn,
                    "End": true
                }
            }
        })
        .to_string();

        let created = service
            .create_state_machine(
                &step_functions_scope()
                    .expect("Step Functions scope should build"),
                CreateStateMachineInput {
                    definition,
                    name: "success".to_owned(),
                    role_arn: "arn:aws:iam::000000000000:role/demo".to_owned(),
                    state_machine_type: None,
                },
            )
            .expect("state machine should create");
        let started = service
            .start_execution(
                &step_functions_scope()
                    .expect("Step Functions scope should build"),
                StartExecutionInput {
                    input: Some(json!({ "value": 1 }).to_string()),
                    name: Some("run-1".to_owned()),
                    state_machine_arn: created.state_machine_arn,
                    trace_header: None,
                },
            )
            .expect("execution should start");
        let described = service
            .describe_execution(
                &step_functions_scope()
                    .expect("Step Functions scope should build"),
                DescribeExecutionInput {
                    execution_arn: started.execution_arn,
                },
            )
            .expect("execution should describe");

        assert_eq!(described.status, ExecutionStatus::Succeeded);
        assert_eq!(described.output, Some(json!({ "ok": true }).to_string()));
    }

    #[test]
    fn lambda_step_functions_task_adapter_surfaces_function_errors() {
        let sqs = SqsService::new();
        let lambda = lambda_service(
            "step-functions-error",
            Arc::new(FakeLambdaExecutor::function_error(
                "{\"errorMessage\":\"boom\"}",
                "Handled",
            )),
            sqs,
        )
        .expect("lambda service should build");
        let function_arn =
            "arn:aws:lambda:eu-west-2:000000000000:function:demo".to_owned();
        let service = step_functions_service(
            "error",
            Arc::new(LambdaStepFunctionsTaskAdapter::new(lambda)),
        );
        let definition = json!({
            "StartAt": "Task",
            "States": {
                "Task": {
                    "Type": "Task",
                    "Resource": function_arn,
                    "End": true
                }
            }
        })
        .to_string();

        let created = service
            .create_state_machine(
                &step_functions_scope()
                    .expect("Step Functions scope should build"),
                CreateStateMachineInput {
                    definition,
                    name: "failure".to_owned(),
                    role_arn: "arn:aws:iam::000000000000:role/demo".to_owned(),
                    state_machine_type: None,
                },
            )
            .expect("state machine should create");
        let started = service
            .start_execution(
                &step_functions_scope()
                    .expect("Step Functions scope should build"),
                StartExecutionInput {
                    input: None,
                    name: Some("run-2".to_owned()),
                    state_machine_arn: created.state_machine_arn,
                    trace_header: None,
                },
            )
            .expect("execution should start");
        let described = service
            .describe_execution(
                &step_functions_scope()
                    .expect("Step Functions scope should build"),
                DescribeExecutionInput {
                    execution_arn: started.execution_arn,
                },
            )
            .expect("execution should describe");

        assert_eq!(described.status, ExecutionStatus::Failed);
        assert_eq!(described.error.as_deref(), Some("Handled"));
        assert_eq!(
            described.cause.as_deref(),
            Some("{\"errorMessage\":\"boom\"}")
        );
    }
}
