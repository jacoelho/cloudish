use crate::EventBridgeDeliveryShutdown;
use crate::adapters::{ThreadWorkQueue, ThreadWorkQueueStopToken};
use aws::{Arn, InfrastructureError, ServiceName};
use eventbridge::{
    EventBridgeDeliveryDispatcher, EventBridgeError,
    EventBridgePlannedDelivery, EventBridgeScope, EventBridgeTarget,
};
use lambda::{LambdaScope, LambdaService};
use sns::{PublishInput, SnsService};
use sqs::{SendMessageInput, SqsService};
use std::collections::BTreeMap;
use std::sync::Arc;

#[derive(Clone, Default)]
pub(crate) struct EventBridgeDispatcherDependencies {
    pub lambda: Option<LambdaService>,
    pub sns: Option<SnsService>,
    pub sqs: Option<SqsService>,
}

pub(crate) struct EventBridgeDispatcherAssembly {
    pub dispatcher: Arc<dyn EventBridgeDeliveryDispatcher>,
    pub shutdown: EventBridgeDeliveryShutdown,
}

pub(crate) fn build_eventbridge_dispatcher(
    dependencies: EventBridgeDispatcherDependencies,
) -> Result<EventBridgeDispatcherAssembly, InfrastructureError> {
    let worker_dependencies = dependencies.clone();
    let worker = Arc::new(ThreadWorkQueue::spawn(
        "eventbridge-delivery".to_owned(),
        move |deliveries: Vec<EventBridgePlannedDelivery>, stop_token| {
            dispatch_eventbridge_deliveries(
                &worker_dependencies,
                deliveries,
                stop_token,
            );
        },
    )?);
    let shutdown_worker = Arc::clone(&worker);
    let shutdown_hard_stop_worker = Arc::clone(&worker);
    let shutdown_finish_worker = Arc::clone(&worker);

    Ok(EventBridgeDispatcherAssembly {
        dispatcher: Arc::new(EventBridgeDispatcher {
            dependencies,
            delivery_worker: worker,
        }),
        shutdown: EventBridgeDeliveryShutdown::new(
            Arc::new(move || shutdown_worker.begin_shutdown()),
            Arc::new(move || shutdown_hard_stop_worker.request_hard_stop()),
            Arc::new(move |timeout| {
                shutdown_finish_worker.finish_shutdown(Some(timeout))
            }),
        ),
    })
}

#[derive(Clone)]
struct EventBridgeDispatcher {
    dependencies: EventBridgeDispatcherDependencies,
    delivery_worker: Arc<ThreadWorkQueue<Vec<EventBridgePlannedDelivery>>>,
}

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

    fn dispatch(
        &self,
        deliveries: Vec<EventBridgePlannedDelivery>,
    ) -> Result<(), EventBridgeError> {
        self.delivery_worker.enqueue(deliveries).map_err(|error| {
            EventBridgeError::InternalFailure { message: error.to_string() }
        })
    }
}

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
                    message_attributes: BTreeMap::new(),
                    message_deduplication_id: None,
                    message_group_id: None,
                    message_system_attributes: BTreeMap::new(),
                },
            );
        }
        _ => {}
    }
}

fn dispatch_eventbridge_deliveries(
    dependencies: &EventBridgeDispatcherDependencies,
    deliveries: Vec<EventBridgePlannedDelivery>,
    stop_token: &ThreadWorkQueueStopToken,
) {
    for delivery in deliveries {
        if stop_token.is_stop_requested() {
            break;
        }
        dispatch_eventbridge_delivery(dependencies, delivery);
    }
}

fn target_scope(target_arn: &Arn) -> Result<LambdaScope, EventBridgeError> {
    let Some(account_id) = target_arn.account_id().cloned() else {
        return Err(missing_target_error(target_arn));
    };
    let Some(region) = target_arn.region().cloned() else {
        return Err(missing_target_error(target_arn));
    };

    Ok(LambdaScope::new(account_id, region))
}

fn target_sns_scope(target_arn: &Arn) -> Arn {
    target_arn.clone()
}

fn missing_target_error(target_arn: &Arn) -> EventBridgeError {
    EventBridgeError::ResourceNotFound {
        message: format!("Target Arn {target_arn} does not exist."),
    }
}
