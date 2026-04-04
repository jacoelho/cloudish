use crate::errors::SqsError;
use crate::messages::{
    ListDeadLetterSourceQueuesInput, PaginatedDeadLetterSourceQueues,
    PendingQueueMove, timestamp_seconds,
};
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListMessageMoveTasksInput {
    pub max_results: Option<i32>,
    pub source_arn: Arn,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListMessageMoveTasksOutput {
    pub results: Vec<ListMessageMoveTasksResultEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListMessageMoveTasksResultEntry {
    pub approximate_number_of_messages_moved: i64,
    pub approximate_number_of_messages_to_move: Option<i64>,
    pub destination_arn: Option<String>,
    pub failure_reason: Option<String>,
    pub max_number_of_messages_per_second: Option<i32>,
    pub source_arn: String,
    pub started_timestamp: i64,
    pub status: String,
    pub task_handle: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CancelMessageMoveTaskInput {
    pub task_handle: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CancelMessageMoveTaskOutput {
    pub approximate_number_of_messages_moved: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MessageMoveTaskRecord {
    pub(crate) approximate_number_of_messages_moved: i64,
    pub(crate) approximate_number_of_messages_to_move: Option<i64>,
    pub(crate) destination_arn: Option<String>,
    pub(crate) failure_reason: Option<String>,
    pub(crate) max_number_of_messages_per_second: Option<i32>,
    pub(crate) source_arn: String,
    pub(crate) started_timestamp: i64,
    pub(crate) status: String,
    pub(crate) task_handle: String,
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

        Ok(state
            .list_dead_letter_source_queues_page(
                queue,
                &ListDeadLetterSourceQueuesInput {
                    max_results: None,
                    next_token: None,
                },
            )?
            .queue_urls)
    }

    /// Returns one page of queues whose dead-letter policy targets `queue`.
    ///
    /// # Errors
    ///
    /// Returns [`SqsError::QueueDoesNotExist`] when the target queue is
    /// unknown, or a validation error when the pagination request is invalid.
    pub fn list_dead_letter_source_queues_page(
        &self,
        queue: &SqsQueueIdentity,
        input: &ListDeadLetterSourceQueuesInput,
    ) -> Result<PaginatedDeadLetterSourceQueues, SqsError> {
        let state =
            self.state.lock().unwrap_or_else(|poison| poison.into_inner());

        state.list_dead_letter_source_queues_page(queue, input)
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

        state.start_message_move_task(
            input,
            timestamp_seconds((self.time_source)()),
        )
    }

    /// Lists message move tasks for the supplied dead-letter queue source ARN.
    ///
    /// # Errors
    ///
    /// Returns [`SqsError`] when the source ARN is invalid or does not resolve
    /// to an existing queue.
    pub fn list_message_move_tasks(
        &self,
        input: ListMessageMoveTasksInput,
    ) -> Result<ListMessageMoveTasksOutput, SqsError> {
        let state =
            self.state.lock().unwrap_or_else(|poison| poison.into_inner());

        state.list_message_move_tasks(input)
    }

    /// Cancels a running message move task.
    ///
    /// # Errors
    ///
    /// Returns [`SqsError`] when the task handle is invalid, unknown, or does
    /// not refer to a running task.
    pub fn cancel_message_move_task(
        &self,
        input: CancelMessageMoveTaskInput,
    ) -> Result<CancelMessageMoveTaskOutput, SqsError> {
        let mut state =
            self.state.lock().unwrap_or_else(|poison| poison.into_inner());

        state.cancel_message_move_task(input)
    }
}

impl SqsWorld {
    pub(crate) fn list_dead_letter_source_queues_page(
        &self,
        queue: &SqsQueueIdentity,
        input: &ListDeadLetterSourceQueuesInput,
    ) -> Result<PaginatedDeadLetterSourceQueues, SqsError> {
        let target_queue =
            self.queues.get(queue).ok_or(SqsError::QueueDoesNotExist)?;
        let target_arn = target_queue.queue_arn();
        let mut queue_urls = self
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
            .collect::<Vec<_>>();
        queue_urls.sort();
        let context = crate::queues::pagination_context(
            "ListDeadLetterSourceQueues",
            &serde_json::json!({ "queue": target_arn }),
        );
        let (queue_urls, next_token) = crate::queues::paginate_items(
            &queue_urls,
            input.max_results,
            input.next_token.as_deref(),
            &context,
        )?;

        Ok(PaginatedDeadLetterSourceQueues { next_token, queue_urls })
    }

    pub(crate) fn start_message_move_task(
        &mut self,
        input: StartMessageMoveTaskInput,
        now_seconds: u64,
    ) -> Result<StartMessageMoveTaskOutput, SqsError> {
        if input.max_number_of_messages_per_second.is_some() {
            return Err(SqsError::UnsupportedOperation {
                message: "StartMessageMoveTask does not support MaxNumberOfMessagesPerSecond in this Cloudish subset.".to_owned(),
            });
        }

        let source_identity = self
            .resolve_existing_queue_from_arn(&input.source_arn, "SourceArn")?;
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

        let destination_arn =
            if let Some(destination_arn) = input.destination_arn {
                let _ = self.resolve_existing_queue_from_arn(
                    &destination_arn,
                    "DestinationArn",
                )?;
                Some(destination_arn)
            } else {
                None
            };

        let staged_moves = {
            let queue = self.queues.get(&source_identity).ok_or_else(|| {
                SqsError::ResourceNotFound {
                    message: "The resource that you specified for the SourceArn parameter doesn't exist.".to_owned(),
                }
            })?;
            queue.staged_messages_for_move_task()
        }
        .into_iter()
        .map(|message| {
            let target_arn = destination_arn
                .as_ref()
                .cloned()
                .or_else(|| {
                    message
                        .message
                        .dead_letter_source_arn
                        .as_deref()
                        .and_then(|value| value.parse().ok())
                })
                .ok_or_else(|| SqsError::InvalidParameterValue {
                    message: "Messages in the dead-letter queue are missing their original source metadata.".to_owned(),
                })?;
            self.stage_message_move(
                &source_identity,
                message.source_message_index,
                &target_arn,
                message.message,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
        let approximate_number_of_messages_moved =
            i64::try_from(staged_moves.len()).unwrap_or(i64::MAX);
        let approximate_number_of_messages_to_move =
            Some(approximate_number_of_messages_moved);
        self.commit_staged_message_moves(staged_moves)?;
        let started_timestamp = i64::try_from(now_seconds).unwrap_or(i64::MAX);
        let task_handle =
            encode_move_task_handle(&input.source_arn, started_timestamp);
        self.message_move_tasks.push(MessageMoveTaskRecord {
            approximate_number_of_messages_moved,
            approximate_number_of_messages_to_move,
            destination_arn: destination_arn.as_ref().map(ToString::to_string),
            failure_reason: None,
            max_number_of_messages_per_second: input
                .max_number_of_messages_per_second
                .map(|value| i32::try_from(value).unwrap_or(i32::MAX)),
            source_arn: input.source_arn.to_string(),
            started_timestamp,
            status: "COMPLETED".to_owned(),
            task_handle: task_handle.clone(),
        });

        Ok(StartMessageMoveTaskOutput { task_handle })
    }

    pub(crate) fn list_message_move_tasks(
        &self,
        input: ListMessageMoveTasksInput,
    ) -> Result<ListMessageMoveTasksOutput, SqsError> {
        let _ = self
            .resolve_existing_queue_from_arn(&input.source_arn, "SourceArn")?;
        let source_arn = input.source_arn.to_string();
        let results = self
            .message_move_tasks
            .iter()
            .filter(|task| task.source_arn == source_arn)
            .rev()
            .take(validate_list_message_move_task_max_results(
                input.max_results,
            )?)
            .map(MessageMoveTaskRecord::as_result_entry)
            .collect();

        Ok(ListMessageMoveTasksOutput { results })
    }

    fn resolve_existing_queue_from_arn(
        &self,
        arn: &Arn,
        parameter_name: &str,
    ) -> Result<SqsQueueIdentity, SqsError> {
        let identity = parse_queue_identity_from_arn(arn, parameter_name)?;
        if self.queues.contains_key(&identity) {
            Ok(identity)
        } else {
            Err(SqsError::ResourceNotFound {
                message: format!(
                    "The resource that you specified for the {parameter_name} parameter doesn't exist."
                ),
            })
        }
    }

    pub(crate) fn cancel_message_move_task(
        &mut self,
        input: CancelMessageMoveTaskInput,
    ) -> Result<CancelMessageMoveTaskOutput, SqsError> {
        let Some(task) = self
            .message_move_tasks
            .iter_mut()
            .find(|task| task.task_handle == input.task_handle)
        else {
            return Err(SqsError::ResourceNotFound {
                message: "The resource that you specified for the TaskHandle parameter doesn't exist.".to_owned(),
            });
        };
        if task.status != "RUNNING" {
            return Err(SqsError::UnsupportedOperation {
                message: "Only running message move tasks can be cancelled."
                    .to_owned(),
            });
        }

        task.status = "CANCELLED".to_owned();
        Ok(CancelMessageMoveTaskOutput {
            approximate_number_of_messages_moved: task
                .approximate_number_of_messages_moved,
        })
    }
}

impl QueueRecord {
    pub(crate) fn staged_messages_for_move_task(
        &self,
    ) -> Vec<PendingQueueMove> {
        self.messages
            .iter()
            .enumerate()
            .filter(|(_, message)| !message.deleted)
            .map(|(index, message)| PendingQueueMove {
                message: message.staged_for_queue_move(
                    message.dead_letter_source_arn.clone(),
                ),
                source_message_index: index,
            })
            .collect()
    }

    pub(crate) fn redrive_policy_document(
        &self,
    ) -> Option<RedrivePolicyDocument> {
        self.attributes
            .get("RedrivePolicy")
            .and_then(|policy| serde_json::from_str(policy).ok())
    }
}

impl MessageMoveTaskRecord {
    pub(crate) fn as_result_entry(&self) -> ListMessageMoveTasksResultEntry {
        ListMessageMoveTasksResultEntry {
            approximate_number_of_messages_moved: self
                .approximate_number_of_messages_moved,
            approximate_number_of_messages_to_move: self
                .approximate_number_of_messages_to_move,
            destination_arn: self.destination_arn.clone(),
            failure_reason: self.failure_reason.clone(),
            max_number_of_messages_per_second: self
                .max_number_of_messages_per_second,
            source_arn: self.source_arn.clone(),
            started_timestamp: self.started_timestamp,
            status: self.status.clone(),
            task_handle: (self.status == "RUNNING")
                .then(|| self.task_handle.clone()),
        }
    }
}

pub(crate) fn encode_move_task_handle(
    source_arn: &Arn,
    started_timestamp: i64,
) -> String {
    base64::engine::general_purpose::STANDARD
        .encode(format!("{source_arn}|{started_timestamp}"))
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

fn validate_list_message_move_task_max_results(
    max_results: Option<i32>,
) -> Result<usize, SqsError> {
    let max_results = max_results.unwrap_or(1);
    if !(1..=10).contains(&max_results) {
        return Err(SqsError::InvalidParameterValue {
            message: format!(
                "Value {max_results} for parameter MaxResults is invalid."
            ),
        });
    }

    usize::try_from(max_results).map_err(|_| SqsError::InvalidParameterValue {
        message: format!(
            "Value {max_results} for parameter MaxResults is invalid."
        ),
    })
}
