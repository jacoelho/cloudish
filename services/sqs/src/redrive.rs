use crate::errors::SqsError;
use crate::messages::MessageRecord;
use crate::queues::{QueueRecord, SqsService, SqsWorld};
use crate::scope::SqsQueueIdentity;
use aws::Arn;
use base64::Engine;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StartMessageMoveTaskInput {
    pub destination_arn: Option<Arn>,
    pub max_number_of_messages_per_second: Option<u32>,
    pub source_arn: Arn,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StartMessageMoveTaskOutput {
    pub task_handle: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct RedrivePolicyDocument {
    #[serde(rename = "deadLetterTargetArn")]
    pub(crate) dead_letter_target_arn: String,
    #[serde(rename = "maxReceiveCount")]
    pub(crate) max_receive_count: u32,
}

impl SqsService {
    /// Lists queues whose dead-letter policy targets the provided queue.
    ///
    /// # Errors
    ///
    /// Returns [`SqsError::QueueDoesNotExist`] when the target queue is unknown.
    pub fn list_dead_letter_source_queues(
        &self,
        queue: &SqsQueueIdentity,
    ) -> Result<Vec<SqsQueueIdentity>, SqsError> {
        let state =
            self.state.lock().unwrap_or_else(|poison| poison.into_inner());

        state.list_dead_letter_source_queues(queue)
    }

    /// Starts a dead-letter redrive task from the provided source queue ARN.
    ///
    /// # Errors
    ///
    /// Returns [`SqsError`] when the source or destination queue is invalid, missing,
    /// or uses an unsupported message-move option.
    pub fn start_message_move_task(
        &self,
        input: StartMessageMoveTaskInput,
    ) -> Result<StartMessageMoveTaskOutput, SqsError> {
        let mut state =
            self.state.lock().unwrap_or_else(|poison| poison.into_inner());

        state.start_message_move_task(input)
    }
}

impl SqsWorld {
    pub(crate) fn list_dead_letter_source_queues(
        &self,
        queue: &SqsQueueIdentity,
    ) -> Result<Vec<SqsQueueIdentity>, SqsError> {
        let target_queue =
            self.queues.get(queue).ok_or(SqsError::QueueDoesNotExist)?;
        let target_arn = target_queue.queue_arn();

        Ok(self
            .queues
            .values()
            .filter_map(|candidate| {
                candidate
                    .redrive_policy_document()
                    .filter(|policy| {
                        policy.dead_letter_target_arn == target_arn
                    })
                    .map(|_| candidate.identity.clone())
            })
            .collect())
    }

    pub(crate) fn start_message_move_task(
        &mut self,
        input: StartMessageMoveTaskInput,
    ) -> Result<StartMessageMoveTaskOutput, SqsError> {
        if input.max_number_of_messages_per_second.is_some() {
            return Err(SqsError::UnsupportedOperation {
                message: "StartMessageMoveTask does not support MaxNumberOfMessagesPerSecond in this Cloudish subset.".to_owned(),
            });
        }

        let source_identity =
            parse_queue_identity_from_arn(&input.source_arn, "SourceArn")?;
        let source_is_dlq = self.queues.values().any(|queue| {
            queue.redrive_policy_document().is_some_and(|policy| {
                policy.dead_letter_target_arn == input.source_arn.to_string()
            })
        });
        if !source_is_dlq {
            return Err(SqsError::InvalidParameterValue {
                message:
                    "Source queue must be configured as a Dead Letter Queue."
                        .to_owned(),
            });
        }

        let destination_arn = if let Some(destination_arn) =
            input.destination_arn
        {
            parse_queue_identity_from_arn(&destination_arn, "DestinationArn")?;
            if !self.queues.contains_key(&parse_queue_identity_from_arn(
                &destination_arn,
                "DestinationArn",
            )?) {
                return Err(SqsError::ResourceNotFound {
                    message: "The resource that you specified for the DestinationArn parameter doesn't exist.".to_owned(),
                });
            }
            Some(destination_arn)
        } else {
            None
        };

        let moved_messages = {
            let queue = self
                .queues
                .get_mut(&source_identity)
                .ok_or_else(|| SqsError::ResourceNotFound {
                    message: "The resource that you specified for the SourceArn parameter doesn't exist.".to_owned(),
                })?;
            queue.drain_messages_for_move_task()
        };

        for message in moved_messages {
            let target_arn = destination_arn
                .as_ref()
                .cloned()
                .or_else(|| {
                    message
                        .dead_letter_source_arn
                        .as_deref()
                        .and_then(|value| value.parse().ok())
                })
                .ok_or_else(|| SqsError::InvalidParameterValue {
                    message: "Messages in the dead-letter queue are missing their original source metadata.".to_owned(),
                })?;
            self.enqueue_moved_message(&target_arn, message)?;
        }

        Ok(StartMessageMoveTaskOutput {
            task_handle: encode_move_task_handle(&input.source_arn),
        })
    }

    pub(crate) fn enqueue_moved_message(
        &mut self,
        target_arn: &Arn,
        mut message: MessageRecord,
    ) -> Result<(), SqsError> {
        let target_identity =
            parse_queue_identity_from_arn(target_arn, "QueueArn")?;
        let target_queue = self
            .queues
            .get_mut(&target_identity)
            .ok_or(SqsError::QueueDoesNotExist)?;
        message.deleted = false;
        message.issued_receipt_handles.clear();
        message.latest_receipt_handle = None;
        message.receive_count = 0;
        message.visible_at_millis = message.sent_timestamp_millis;
        if target_queue.is_fifo() && message.message_group_id.is_none() {
            return Err(SqsError::InvalidParameterValue {
                message:
                    "The moved FIFO message is missing its MessageGroupId."
                        .to_owned(),
            });
        }
        if target_queue.queue_arn() != target_arn.to_string() {
            return Err(SqsError::QueueDoesNotExist);
        }
        if target_queue.is_fifo() && message.sequence_number.is_none() {
            message.sequence_number =
                Some(target_queue.next_sequence_number());
        }
        target_queue.messages.push_back(message);

        Ok(())
    }
}

impl QueueRecord {
    pub(crate) fn drain_messages_for_move_task(
        &mut self,
    ) -> Vec<MessageRecord> {
        let mut moved = Vec::new();

        while let Some(mut message) = self.messages.pop_front() {
            if message.deleted {
                continue;
            }
            message.deleted = false;
            moved.push(message);
        }
        self.deduplication_records.clear();

        moved
    }

    pub(crate) fn redrive_policy_document(
        &self,
    ) -> Option<RedrivePolicyDocument> {
        self.attributes
            .get("RedrivePolicy")
            .and_then(|policy| serde_json::from_str(policy).ok())
    }
}

pub(crate) fn encode_move_task_handle(source_arn: &Arn) -> String {
    base64::engine::general_purpose::STANDARD.encode(source_arn.to_string())
}

pub(crate) fn parse_queue_identity_from_arn(
    arn: &Arn,
    parameter_name: &str,
) -> Result<SqsQueueIdentity, SqsError> {
    SqsQueueIdentity::from_arn(arn).map_err(|_| {
        SqsError::InvalidParameterValue {
            message: format!(
                "Value {arn} for parameter {parameter_name} is invalid."
            ),
        }
    })
}

pub(crate) fn validate_redrive_policy_target(
    world: &SqsWorld,
    source_identity: &SqsQueueIdentity,
    attributes: &BTreeMap<String, String>,
) -> Result<(), SqsError> {
    let Some(policy) = attributes.get("RedrivePolicy") else {
        return Ok(());
    };
    let document = parse_redrive_policy(policy)?;
    let target_arn =
        document.dead_letter_target_arn.parse().map_err(|_| {
            SqsError::InvalidParameterValue {
                message: format!(
                    "Value {policy} for parameter RedrivePolicy is invalid."
                ),
            }
        })?;
    let target_identity =
        parse_queue_identity_from_arn(&target_arn, "RedrivePolicy")?;
    let Some(target_queue) = world.queues.get(&target_identity) else {
        return Err(SqsError::InvalidParameterValue {
            message: format!(
                "Value {policy} for parameter RedrivePolicy is invalid. Reason: deadLetterTargetArn must reference an existing queue."
            ),
        });
    };

    if source_identity.queue_name().ends_with(".fifo")
        != target_queue.is_fifo()
    {
        return Err(SqsError::InvalidParameterValue {
            message: format!(
                "Value {policy} for parameter RedrivePolicy is invalid. Reason: dead-letter queue type must match the source queue type."
            ),
        });
    }

    Ok(())
}

pub(crate) fn parse_redrive_policy(
    value: &str,
) -> Result<RedrivePolicyDocument, SqsError> {
    let parsed =
        serde_json::from_str::<serde_json::Value>(value).map_err(|_| {
            SqsError::InvalidParameterValue {
                message: format!(
                    "Value {value} for parameter RedrivePolicy is invalid."
                ),
            }
        })?;
    let object =
        parsed.as_object().ok_or_else(|| SqsError::InvalidParameterValue {
            message: format!(
                "Value {value} for parameter RedrivePolicy is invalid."
            ),
        })?;
    let dead_letter_target_arn = object
        .get("deadLetterTargetArn")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| SqsError::InvalidParameterValue {
            message: "The required parameter 'deadLetterTargetArn' is missing"
                .to_owned(),
        })?
        .to_owned();
    let max_receive_count = match object.get("maxReceiveCount") {
        Some(serde_json::Value::Number(number)) => {
            number.as_u64().and_then(|value| value.try_into().ok())
        }
        Some(serde_json::Value::String(value)) => value.parse::<u32>().ok(),
        _ => None,
    }
    .ok_or_else(|| SqsError::InvalidParameterValue {
        message: "The required parameter 'maxReceiveCount' is missing"
            .to_owned(),
    })?;

    if max_receive_count == 0 || max_receive_count > 1_000 {
        return Err(SqsError::InvalidParameterValue {
            message: format!(
                "Value {value} for parameter RedrivePolicy is invalid. Reason: Invalid value for maxReceiveCount: {max_receive_count}, valid values are from 1 to 1000 both inclusive."
            ),
        });
    }

    Ok(RedrivePolicyDocument { dead_letter_target_arn, max_receive_count })
}
