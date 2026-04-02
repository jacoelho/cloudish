use crate::attributes::{
    DEFAULT_DELAY_SECONDS, MAX_DELAY_SECONDS, MAX_VISIBILITY_TIMEOUT,
};
use crate::errors::SqsError;
use crate::message_attributes::{
    MessageAttributeValue, md5_of_message_attributes,
    md5_of_message_system_attributes, message_attribute_size,
    selected_message_attributes, validate_message_attribute_name,
};
use crate::queues::{
    CallbackSqsReceiveCancellation, QueueRecord, SqsReceiveWaitOutcome,
    SqsService, SqsWorld,
};
use crate::scope::SqsQueueIdentity;
use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const MAX_NUMBER_OF_MESSAGES: u32 = 10;
const MAX_RECEIVE_WAIT_TIME_SECONDS: u32 = 20;
const MAX_BATCH_ENTRY_ID_LENGTH: usize = 80;
const LONG_POLL_INTERVAL: Duration = Duration::from_millis(250);
const RECEIVE_REQUEST_ATTEMPT_ID_WINDOW_MILLIS: u64 = 5 * 60 * 1_000;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ListQueuesInput {
    pub max_results: Option<u32>,
    pub next_token: Option<String>,
    pub queue_name_prefix: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PaginatedQueues {
    pub next_token: Option<String>,
    pub queue_urls: Vec<SqsQueueIdentity>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListDeadLetterSourceQueuesInput {
    pub max_results: Option<u32>,
    pub next_token: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PaginatedDeadLetterSourceQueues {
    pub next_token: Option<String>,
    pub queue_urls: Vec<SqsQueueIdentity>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SendMessageInput {
    pub body: String,
    pub delay_seconds: Option<u32>,
    pub message_attributes: BTreeMap<String, MessageAttributeValue>,
    pub message_deduplication_id: Option<String>,
    pub message_group_id: Option<String>,
    pub message_system_attributes: BTreeMap<String, MessageAttributeValue>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SendMessageOutput {
    pub md5_of_message_body: String,
    pub md5_of_message_attributes: Option<String>,
    pub md5_of_message_system_attributes: Option<String>,
    pub message_id: String,
    pub sequence_number: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ReceiveMessageInput {
    pub attribute_names: Vec<String>,
    pub max_number_of_messages: Option<u32>,
    pub message_attribute_names: Vec<String>,
    pub message_system_attribute_names: Vec<String>,
    pub receive_request_attempt_id: Option<String>,
    pub visibility_timeout: Option<u32>,
    pub wait_time_seconds: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReceivedMessage {
    pub attributes: BTreeMap<String, String>,
    pub body: String,
    pub md5_of_body: String,
    pub md5_of_message_attributes: Option<String>,
    pub message_attributes: BTreeMap<String, MessageAttributeValue>,
    pub message_id: String,
    pub receipt_handle: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SendMessageBatchEntryInput {
    pub body: String,
    pub delay_seconds: Option<u32>,
    pub id: String,
    pub message_attributes: BTreeMap<String, MessageAttributeValue>,
    pub message_deduplication_id: Option<String>,
    pub message_group_id: Option<String>,
    pub message_system_attributes: BTreeMap<String, MessageAttributeValue>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SendMessageBatchSuccess {
    pub id: String,
    pub md5_of_message_body: String,
    pub md5_of_message_attributes: Option<String>,
    pub md5_of_message_system_attributes: Option<String>,
    pub message_id: String,
    pub sequence_number: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchFailure {
    pub code: String,
    pub id: String,
    pub message: String,
    pub sender_fault: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SendMessageBatchOutput {
    pub failed: Vec<BatchFailure>,
    pub successful: Vec<SendMessageBatchSuccess>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteMessageBatchEntryInput {
    pub id: String,
    pub receipt_handle: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteMessageBatchSuccess {
    pub id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteMessageBatchOutput {
    pub failed: Vec<BatchFailure>,
    pub successful: Vec<DeleteMessageBatchSuccess>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangeMessageVisibilityBatchEntryInput {
    pub id: String,
    pub receipt_handle: String,
    pub visibility_timeout: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangeMessageVisibilityBatchSuccess {
    pub id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangeMessageVisibilityBatchOutput {
    pub failed: Vec<BatchFailure>,
    pub successful: Vec<ChangeMessageVisibilityBatchSuccess>,
}

#[derive(Debug, Default)]
pub(crate) struct ReceiveOutcome {
    pub(crate) dead_letter_messages: Vec<PendingQueueMove>,
    pub(crate) received: Vec<ReceivedMessage>,
}

#[derive(Debug, Clone)]
pub(crate) struct PendingQueueMove {
    pub(crate) message: MessageRecord,
    pub(crate) source_message_index: usize,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct ReceiveMessageAttempt {
    pub(crate) effective_wait_time_seconds: u32,
    pub(crate) received: Vec<ReceivedMessage>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReceiveSelectors {
    pub(crate) message_attribute_names: Vec<String>,
    pub(crate) system_attribute_names: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ResolvedReceiveRequest {
    pub(crate) max_number_of_messages: u32,
    pub(crate) receive_request_attempt_id: Option<String>,
    pub(crate) selectors: ReceiveSelectors,
    pub(crate) visibility_timeout: u32,
    pub(crate) wait_time_seconds: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReceiveAttemptRecord {
    pub(crate) expires_at_millis: u64,
    pub(crate) receipt_handles_by_message_id: BTreeMap<String, String>,
    pub(crate) received: Vec<ReceivedMessage>,
}

impl SqsService {
    /// Stores a message in the target queue.
    ///
    /// # Errors
    ///
    /// Returns [`SqsError`] when the queue does not exist or the request is invalid for the queue type.
    pub fn send_message(
        &self,
        queue: &SqsQueueIdentity,
        input: SendMessageInput,
    ) -> Result<SendMessageOutput, SqsError> {
        let mut state =
            self.state.lock().unwrap_or_else(|poison| poison.into_inner());

        state.send_message(
            queue,
            input,
            timestamp_millis((self.time_source)()),
            self.identifier_source.next_message_id(),
        )
    }

    /// Receives up to the requested number of messages from a queue.
    ///
    /// # Errors
    ///
    /// Returns [`SqsError`] when the queue does not exist or the receive parameters are invalid.
    pub fn receive_message(
        &self,
        queue: &SqsQueueIdentity,
        input: ReceiveMessageInput,
    ) -> Result<Vec<ReceivedMessage>, SqsError> {
        self.receive_message_with_cancellation(queue, input, &|| false)
    }

    /// Receives messages and allows request-scoped callers to stop a long poll
    /// when their own shutdown boundary is reached.
    ///
    /// # Errors
    ///
    /// Returns [`SqsError`] when the queue does not exist or the receive parameters are invalid.
    pub(crate) fn receive_message_with_cancellation(
        &self,
        queue: &SqsQueueIdentity,
        input: ReceiveMessageInput,
        is_cancelled: &(dyn Fn() -> bool + Send + Sync),
    ) -> Result<Vec<ReceivedMessage>, SqsError> {
        let mut next_receipt_handle =
            || self.identifier_source.next_receipt_handle();
        let now_millis = timestamp_millis((self.time_source)());
        let resolved = {
            let state =
                self.state.lock().unwrap_or_else(|poison| poison.into_inner());
            state.resolve_receive_request(queue, &input)?
        };
        let first_attempt = self.receive_message_attempt(
            queue,
            &resolved,
            now_millis,
            &mut next_receipt_handle,
        )?;
        if !first_attempt.received.is_empty()
            || first_attempt.effective_wait_time_seconds == 0
        {
            return Ok(first_attempt.received);
        }
        let cancellation = CallbackSqsReceiveCancellation::new(is_cancelled);
        if is_cancelled() {
            return Ok(Vec::new());
        }

        let deadline_millis = now_millis.saturating_add(
            u64::from(first_attempt.effective_wait_time_seconds)
                .saturating_mul(1_000),
        );
        let mut current_millis = now_millis;

        while current_millis < deadline_millis {
            if self.receive_waiter.wait(
                long_poll_wait_duration(deadline_millis - current_millis),
                &cancellation,
            ) == SqsReceiveWaitOutcome::Cancelled
            {
                return Ok(Vec::new());
            }
            current_millis = timestamp_millis((self.time_source)());
            if current_millis >= deadline_millis {
                break;
            }

            let attempt = self.receive_message_attempt(
                queue,
                &resolved,
                current_millis,
                &mut next_receipt_handle,
            )?;
            if !attempt.received.is_empty() {
                return Ok(attempt.received);
            }
        }

        Ok(Vec::new())
    }

    fn receive_message_attempt(
        &self,
        queue: &SqsQueueIdentity,
        input: &ResolvedReceiveRequest,
        now_millis: u64,
        next_receipt_handle: &mut dyn FnMut() -> String,
    ) -> Result<ReceiveMessageAttempt, SqsError> {
        let mut state =
            self.state.lock().unwrap_or_else(|poison| poison.into_inner());

        state.receive_message(queue, input, now_millis, next_receipt_handle)
    }

    /// Sends multiple messages and returns per-entry successes and failures.
    ///
    /// # Errors
    ///
    /// Returns [`SqsError`] when the queue does not exist or the batch request is structurally invalid.
    pub fn send_message_batch(
        &self,
        queue: &SqsQueueIdentity,
        entries: Vec<SendMessageBatchEntryInput>,
    ) -> Result<SendMessageBatchOutput, SqsError> {
        let mut state =
            self.state.lock().unwrap_or_else(|poison| poison.into_inner());
        let mut next_message_id = || self.identifier_source.next_message_id();

        state.send_message_batch(
            queue,
            entries,
            timestamp_millis((self.time_source)()),
            &mut next_message_id,
        )
    }

    /// Deletes a message by receipt handle.
    ///
    /// # Errors
    ///
    /// Returns [`SqsError`] when the queue does not exist or the receipt handle is invalid.
    pub fn delete_message(
        &self,
        queue: &SqsQueueIdentity,
        receipt_handle: &str,
    ) -> Result<(), SqsError> {
        let mut state =
            self.state.lock().unwrap_or_else(|poison| poison.into_inner());

        state.delete_message(queue, receipt_handle)
    }

    /// Deletes multiple messages and reports per-entry results.
    ///
    /// # Errors
    ///
    /// Returns [`SqsError`] when the queue does not exist or the batch request is structurally invalid.
    pub fn delete_message_batch(
        &self,
        queue: &SqsQueueIdentity,
        entries: Vec<DeleteMessageBatchEntryInput>,
    ) -> Result<DeleteMessageBatchOutput, SqsError> {
        let mut state =
            self.state.lock().unwrap_or_else(|poison| poison.into_inner());

        state.delete_message_batch(queue, entries)
    }

    /// Updates the visibility timeout for a previously received message.
    ///
    /// # Errors
    ///
    /// Returns [`SqsError`] when the queue does not exist or the receipt handle cannot be used.
    pub fn change_message_visibility(
        &self,
        queue: &SqsQueueIdentity,
        receipt_handle: &str,
        visibility_timeout: u32,
    ) -> Result<(), SqsError> {
        let mut state =
            self.state.lock().unwrap_or_else(|poison| poison.into_inner());

        state.change_message_visibility(
            queue,
            receipt_handle,
            visibility_timeout,
            timestamp_millis((self.time_source)()),
        )
    }

    /// Updates visibility for multiple receipt handles and reports per-entry results.
    ///
    /// # Errors
    ///
    /// Returns [`SqsError`] when the queue does not exist or the batch request is structurally invalid.
    pub fn change_message_visibility_batch(
        &self,
        queue: &SqsQueueIdentity,
        entries: Vec<ChangeMessageVisibilityBatchEntryInput>,
    ) -> Result<ChangeMessageVisibilityBatchOutput, SqsError> {
        let mut state =
            self.state.lock().unwrap_or_else(|poison| poison.into_inner());

        state.change_message_visibility_batch(
            queue,
            entries,
            timestamp_millis((self.time_source)()),
        )
    }

    /// Removes all currently stored messages from a queue.
    ///
    /// # Errors
    ///
    /// Returns [`SqsError`] when the queue does not exist.
    pub fn purge_queue(
        &self,
        queue: &SqsQueueIdentity,
    ) -> Result<(), SqsError> {
        let mut state =
            self.state.lock().unwrap_or_else(|poison| poison.into_inner());

        state.purge_queue(queue, timestamp_millis((self.time_source)()))
    }
}

impl SqsWorld {
    pub(crate) fn send_message(
        &mut self,
        queue: &SqsQueueIdentity,
        input: SendMessageInput,
        now_millis: u64,
        message_id: String,
    ) -> Result<SendMessageOutput, SqsError> {
        self.queues
            .get_mut(queue)
            .ok_or(SqsError::QueueDoesNotExist)?
            .send_message(input, now_millis, message_id)
    }

    pub(crate) fn send_message_batch(
        &mut self,
        queue: &SqsQueueIdentity,
        entries: Vec<SendMessageBatchEntryInput>,
        now_millis: u64,
        next_message_id: &mut dyn FnMut() -> String,
    ) -> Result<SendMessageBatchOutput, SqsError> {
        validate_batch_request(&entries)?;
        let queue =
            self.queues.get_mut(queue).ok_or(SqsError::QueueDoesNotExist)?;
        let mut successful = Vec::new();
        let mut failed = Vec::new();

        for entry in entries {
            match queue.send_message(
                SendMessageInput {
                    body: entry.body,
                    delay_seconds: entry.delay_seconds,
                    message_attributes: entry.message_attributes,
                    message_deduplication_id: entry.message_deduplication_id,
                    message_group_id: entry.message_group_id,
                    message_system_attributes: entry.message_system_attributes,
                },
                now_millis,
                next_message_id(),
            ) {
                Ok(result) => successful.push(SendMessageBatchSuccess {
                    id: entry.id,
                    md5_of_message_body: result.md5_of_message_body,
                    md5_of_message_attributes: result
                        .md5_of_message_attributes,
                    md5_of_message_system_attributes: result
                        .md5_of_message_system_attributes,
                    message_id: result.message_id,
                    sequence_number: result.sequence_number,
                }),
                Err(error) => failed.push(batch_failure(&entry.id, &error)),
            }
        }

        Ok(SendMessageBatchOutput { failed, successful })
    }

    pub(crate) fn receive_message(
        &mut self,
        queue: &SqsQueueIdentity,
        input: &ResolvedReceiveRequest,
        now_millis: u64,
        next_receipt_handle: &mut dyn FnMut() -> String,
    ) -> Result<ReceiveMessageAttempt, SqsError> {
        let (
            received,
            dead_letter_messages,
            wait_time_seconds,
            dead_letter_target_arn,
        ) = {
            let queue = self
                .queues
                .get_mut(queue)
                .ok_or(SqsError::QueueDoesNotExist)?;
            if queue.is_fifo()
                && let Some(attempt_id) =
                    input.receive_request_attempt_id.as_deref()
            {
                queue.prune_expired_receive_attempts(now_millis);
                if let Some(cached) = queue.replay_receive_attempt(
                    attempt_id,
                    now_millis,
                    input.visibility_timeout,
                ) {
                    return Ok(ReceiveMessageAttempt {
                        effective_wait_time_seconds: input.wait_time_seconds,
                        received: cached,
                    });
                }
            }

            let outcome = if queue.is_fifo() {
                queue.receive_fifo_messages(
                    input.max_number_of_messages,
                    now_millis,
                    &input.selectors,
                    input.visibility_timeout,
                    next_receipt_handle,
                )
            } else {
                queue.receive_standard_messages(
                    input.max_number_of_messages,
                    now_millis,
                    &input.selectors,
                    input.visibility_timeout,
                    next_receipt_handle,
                )
            };

            if queue.is_fifo()
                && !outcome.received.is_empty()
                && let Some(attempt_id) =
                    input.receive_request_attempt_id.as_deref()
            {
                queue.receive_attempts.insert(
                    attempt_id.to_owned(),
                    ReceiveAttemptRecord {
                        expires_at_millis: now_millis.saturating_add(
                            RECEIVE_REQUEST_ATTEMPT_ID_WINDOW_MILLIS,
                        ),
                        receipt_handles_by_message_id: outcome
                            .received
                            .iter()
                            .map(|message| {
                                (
                                    message.message_id.clone(),
                                    message.receipt_handle.clone(),
                                )
                            })
                            .collect(),
                        received: outcome.received.clone(),
                    },
                );
            }

            (
                outcome.received,
                outcome.dead_letter_messages,
                input.wait_time_seconds,
                queue
                    .redrive_policy_document()
                    .map(|policy| policy.dead_letter_target_arn),
            )
        };

        if let Some(target_arn) =
            dead_letter_target_arn.and_then(|value| value.parse().ok())
        {
            let staged_moves = dead_letter_messages
                .into_iter()
                .map(|message| {
                    self.stage_message_move(
                        queue,
                        message.source_message_index,
                        &target_arn,
                        message.message,
                    )
                })
                .collect::<Result<Vec<_>, _>>()?;
            self.commit_staged_message_moves(staged_moves)?;
        }

        Ok(ReceiveMessageAttempt {
            effective_wait_time_seconds: wait_time_seconds,
            received,
        })
    }

    pub(crate) fn resolve_receive_request(
        &self,
        queue: &SqsQueueIdentity,
        input: &ReceiveMessageInput,
    ) -> Result<ResolvedReceiveRequest, SqsError> {
        let queue =
            self.queues.get(queue).ok_or(SqsError::QueueDoesNotExist)?;
        let max_number_of_messages = input.max_number_of_messages.unwrap_or(1);
        validate_max_number_of_messages(max_number_of_messages)?;
        let wait_time_seconds = input
            .wait_time_seconds
            .unwrap_or_else(|| queue.receive_wait_time());
        validate_wait_time_seconds(wait_time_seconds)?;
        let visibility_timeout = input
            .visibility_timeout
            .unwrap_or_else(|| queue.visibility_timeout());
        validate_visibility_timeout(visibility_timeout)?;

        Ok(ResolvedReceiveRequest {
            max_number_of_messages,
            receive_request_attempt_id: input
                .receive_request_attempt_id
                .clone(),
            selectors: receive_selectors(input)?,
            visibility_timeout,
            wait_time_seconds,
        })
    }

    pub(crate) fn delete_message(
        &mut self,
        queue: &SqsQueueIdentity,
        receipt_handle: &str,
    ) -> Result<(), SqsError> {
        let queue =
            self.queues.get_mut(queue).ok_or(SqsError::QueueDoesNotExist)?;
        let Some(message) = queue.messages.iter_mut().find(|message| {
            message.latest_receipt_handle.as_deref() == Some(receipt_handle)
        }) else {
            return Err(SqsError::ReceiptHandleIsInvalid {
                receipt_handle: receipt_handle.to_owned(),
            });
        };

        if message.deleted {
            return Ok(());
        }

        let message_id = message.message_id.clone();
        message.deleted = true;
        queue.invalidate_receive_attempts_for_message(&message_id);
        Ok(())
    }

    pub(crate) fn delete_message_batch(
        &mut self,
        queue: &SqsQueueIdentity,
        entries: Vec<DeleteMessageBatchEntryInput>,
    ) -> Result<DeleteMessageBatchOutput, SqsError> {
        validate_batch_request(&entries)?;
        let mut successful = Vec::new();
        let mut failed = Vec::new();

        for entry in entries {
            match self.delete_message(queue, &entry.receipt_handle) {
                Ok(()) => {
                    successful
                        .push(DeleteMessageBatchSuccess { id: entry.id });
                }
                Err(error) => failed.push(batch_failure(&entry.id, &error)),
            }
        }

        Ok(DeleteMessageBatchOutput { failed, successful })
    }

    pub(crate) fn change_message_visibility(
        &mut self,
        queue: &SqsQueueIdentity,
        receipt_handle: &str,
        visibility_timeout: u32,
        now_millis: u64,
    ) -> Result<(), SqsError> {
        validate_visibility_timeout(visibility_timeout)?;
        let queue =
            self.queues.get_mut(queue).ok_or(SqsError::QueueDoesNotExist)?;
        let Some(message) = queue.messages.iter_mut().find(|message| {
            message.latest_receipt_handle.as_deref() == Some(receipt_handle)
        }) else {
            return Err(SqsError::ReceiptHandleIsInvalid {
                receipt_handle: receipt_handle.to_owned(),
            });
        };

        if message.deleted {
            return Err(SqsError::InvalidReceiptHandleForVisibility {
                receipt_handle: receipt_handle.to_owned(),
            });
        }

        let message_id = message.message_id.clone();
        message.visible_at_millis =
            now_millis + u64::from(visibility_timeout).saturating_mul(1_000);
        queue.invalidate_receive_attempts_for_message(&message_id);

        Ok(())
    }

    pub(crate) fn change_message_visibility_batch(
        &mut self,
        queue: &SqsQueueIdentity,
        entries: Vec<ChangeMessageVisibilityBatchEntryInput>,
        now_millis: u64,
    ) -> Result<ChangeMessageVisibilityBatchOutput, SqsError> {
        validate_batch_request(&entries)?;
        let mut successful = Vec::new();
        let mut failed = Vec::new();

        for entry in entries {
            match self.change_message_visibility(
                queue,
                &entry.receipt_handle,
                entry.visibility_timeout,
                now_millis,
            ) {
                Ok(()) => {
                    successful.push(ChangeMessageVisibilityBatchSuccess {
                        id: entry.id,
                    });
                }
                Err(error) => failed.push(batch_failure(&entry.id, &error)),
            }
        }

        Ok(ChangeMessageVisibilityBatchOutput { failed, successful })
    }

    pub(crate) fn purge_queue(
        &mut self,
        queue: &SqsQueueIdentity,
        now_millis: u64,
    ) -> Result<(), SqsError> {
        let queue =
            self.queues.get_mut(queue).ok_or(SqsError::QueueDoesNotExist)?;
        if queue.last_purged_timestamp_millis.is_some_and(|last_purged| {
            now_millis < last_purged.saturating_add(60_000)
        }) {
            return Err(SqsError::PurgeQueueInProgress {
                message: "Only one PurgeQueue operation on a queue is allowed every 60 seconds.".to_owned(),
            });
        }
        queue.messages.clear();
        queue.deduplication_records.clear();
        queue.receive_attempts.clear();
        queue.last_purged_timestamp_millis = Some(now_millis);

        Ok(())
    }
}

impl QueueRecord {
    pub(crate) fn send_message(
        &mut self,
        input: SendMessageInput,
        now_millis: u64,
        message_id: String,
    ) -> Result<SendMessageOutput, SqsError> {
        validate_message_body(&input.body)?;
        validate_message_attributes(&input.message_attributes)?;
        validate_message_system_attributes(&input.message_system_attributes)?;
        validate_message_size(
            self.maximum_message_size(),
            &input.body,
            &input.message_attributes,
        )?;
        let body = input.body.clone();
        let md5_of_message_body = md5_hex(&body);
        let md5_of_message_attributes =
            md5_of_message_attributes(&input.message_attributes);
        let md5_of_message_system_attributes =
            md5_of_message_system_attributes(&input.message_system_attributes);

        if self.is_fifo() {
            return self.send_fifo_message(
                input,
                now_millis,
                message_id,
                md5_of_message_body,
                md5_of_message_attributes,
                md5_of_message_system_attributes,
            );
        }

        let delay_seconds =
            input.delay_seconds.unwrap_or(DEFAULT_DELAY_SECONDS);
        validate_delay_seconds(delay_seconds)?;
        self.messages.push_back(MessageRecord {
            body,
            dead_letter_source_arn: None,
            deleted: false,
            deduplication_id: None,
            first_receive_timestamp_millis: None,
            issued_receipt_handles: BTreeSet::new(),
            latest_receipt_handle: None,
            md5_of_body: md5_of_message_body.clone(),
            md5_of_message_attributes: md5_of_message_attributes.clone(),
            message_attributes: input.message_attributes,
            message_group_id: None,
            message_id: message_id.clone(),
            message_system_attributes: input.message_system_attributes,
            receive_count: 0,
            sent_timestamp_millis: now_millis,
            sequence_number: None,
            visible_at_millis: now_millis
                + u64::from(delay_seconds).saturating_mul(1_000),
        });

        Ok(SendMessageOutput {
            md5_of_message_body,
            md5_of_message_attributes,
            md5_of_message_system_attributes,
            message_id,
            sequence_number: None,
        })
    }

    pub(crate) fn receive_standard_messages(
        &mut self,
        max_number_of_messages: u32,
        now_millis: u64,
        selectors: &ReceiveSelectors,
        visibility_timeout: u32,
        next_receipt_handle: &mut dyn FnMut() -> String,
    ) -> ReceiveOutcome {
        let mut outcome = ReceiveOutcome::default();
        let queue_arn = self.queue_arn();
        let queue_identity = self.identity.clone();
        let max_receive_count = self
            .redrive_policy_document()
            .map(|policy| policy.max_receive_count);

        for (index, message) in self.messages.iter_mut().enumerate() {
            if outcome.received.len() == max_number_of_messages as usize {
                break;
            }
            if !message.is_receivable(now_millis) {
                continue;
            }

            if max_receive_count
                .is_some_and(|limit| message.receive_count + 1 > limit)
            {
                outcome.dead_letter_messages.push(PendingQueueMove {
                    message: message
                        .staged_for_queue_move(Some(queue_arn.clone())),
                    source_message_index: index,
                });
                continue;
            }

            outcome.received.push(message.receive(
                now_millis,
                selectors,
                &queue_identity,
                visibility_timeout,
                next_receipt_handle(),
            ));
        }

        outcome
    }

    pub(crate) fn prune_expired_receive_attempts(&mut self, now_millis: u64) {
        self.receive_attempts
            .retain(|_, attempt| attempt.expires_at_millis > now_millis);
    }

    pub(crate) fn invalidate_receive_attempts_for_message(
        &mut self,
        message_id: &str,
    ) {
        self.receive_attempts.retain(|_, attempt| {
            !attempt.receipt_handles_by_message_id.contains_key(message_id)
        });
    }

    pub(crate) fn replay_receive_attempt(
        &mut self,
        attempt_id: &str,
        now_millis: u64,
        visibility_timeout: u32,
    ) -> Option<Vec<ReceivedMessage>> {
        let cached = self.receive_attempts.get(attempt_id)?.clone();
        for (message_id, receipt_handle) in
            &cached.receipt_handles_by_message_id
        {
            let message = self
                .messages
                .iter()
                .find(|message| message.message_id == *message_id)?;
            if message.deleted
                || message.latest_receipt_handle.as_deref()
                    != Some(receipt_handle.as_str())
                || message.visible_at_millis <= now_millis
            {
                self.receive_attempts.remove(attempt_id);
                return None;
            }
        }

        let visible_at_millis =
            now_millis + u64::from(visibility_timeout).saturating_mul(1_000);
        for message_id in cached.receipt_handles_by_message_id.keys() {
            let Some(message) = self
                .messages
                .iter_mut()
                .find(|message| message.message_id == *message_id)
            else {
                self.receive_attempts.remove(attempt_id);
                return None;
            };
            message.visible_at_millis = visible_at_millis;
        }

        Some(cached.received)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct MessageRecord {
    pub(crate) body: String,
    pub(crate) dead_letter_source_arn: Option<String>,
    pub(crate) deleted: bool,
    pub(crate) deduplication_id: Option<String>,
    pub(crate) first_receive_timestamp_millis: Option<u64>,
    pub(crate) issued_receipt_handles: BTreeSet<String>,
    pub(crate) latest_receipt_handle: Option<String>,
    pub(crate) md5_of_body: String,
    pub(crate) md5_of_message_attributes: Option<String>,
    pub(crate) message_attributes: BTreeMap<String, MessageAttributeValue>,
    pub(crate) message_group_id: Option<String>,
    pub(crate) message_id: String,
    pub(crate) message_system_attributes:
        BTreeMap<String, MessageAttributeValue>,
    pub(crate) receive_count: u32,
    pub(crate) sent_timestamp_millis: u64,
    pub(crate) sequence_number: Option<String>,
    pub(crate) visible_at_millis: u64,
}

impl MessageRecord {
    pub(crate) fn is_receivable(&self, now_millis: u64) -> bool {
        !self.deleted && self.visible_at_millis <= now_millis
    }

    pub(crate) fn receive(
        &mut self,
        now_millis: u64,
        selectors: &ReceiveSelectors,
        queue: &SqsQueueIdentity,
        visibility_timeout: u32,
        receipt_handle: String,
    ) -> ReceivedMessage {
        self.receive_count += 1;
        self.first_receive_timestamp_millis.get_or_insert(now_millis);
        self.visible_at_millis =
            now_millis + u64::from(visibility_timeout).saturating_mul(1_000);
        self.issued_receipt_handles.clear();
        self.issued_receipt_handles.insert(receipt_handle.clone());
        self.latest_receipt_handle = Some(receipt_handle.clone());

        ReceivedMessage {
            attributes: self.selected_attributes(selectors, queue),
            body: self.body.clone(),
            md5_of_body: self.md5_of_body.clone(),
            md5_of_message_attributes: self.md5_of_message_attributes.clone(),
            message_attributes: selected_message_attributes(
                &self.message_attributes,
                &selectors.message_attribute_names,
            ),
            message_id: self.message_id.clone(),
            receipt_handle,
        }
    }

    pub(crate) fn staged_for_queue_move(
        &self,
        dead_letter_source_arn: Option<String>,
    ) -> MessageRecord {
        MessageRecord {
            body: self.body.clone(),
            dead_letter_source_arn,
            deleted: false,
            deduplication_id: self.deduplication_id.clone(),
            first_receive_timestamp_millis: None,
            issued_receipt_handles: BTreeSet::new(),
            latest_receipt_handle: None,
            md5_of_body: self.md5_of_body.clone(),
            md5_of_message_attributes: self.md5_of_message_attributes.clone(),
            message_attributes: self.message_attributes.clone(),
            message_group_id: self.message_group_id.clone(),
            message_id: self.message_id.clone(),
            message_system_attributes: self.message_system_attributes.clone(),
            receive_count: 0,
            sent_timestamp_millis: self.sent_timestamp_millis,
            sequence_number: self.sequence_number.clone(),
            visible_at_millis: self.sent_timestamp_millis,
        }
    }

    fn selected_attributes(
        &self,
        selectors: &ReceiveSelectors,
        queue: &SqsQueueIdentity,
    ) -> BTreeMap<String, String> {
        let mut selected = BTreeMap::new();
        for name in &selectors.system_attribute_names {
            match name.as_str() {
                "AWSTraceHeader" => {
                    if let Some(value) = self
                        .message_system_attributes
                        .get("AWSTraceHeader")
                        .and_then(|value| value.string_value.as_deref())
                    {
                        selected.insert(name.clone(), value.to_owned());
                    }
                }
                "ApproximateFirstReceiveTimestamp" => {
                    if let Some(value) = self.first_receive_timestamp_millis {
                        selected.insert(name.clone(), value.to_string());
                    }
                }
                "ApproximateReceiveCount" => {
                    selected
                        .insert(name.clone(), self.receive_count.to_string());
                }
                "DeadLetterQueueSourceArn" => {
                    if let Some(value) = self.dead_letter_source_arn.as_ref() {
                        selected.insert(name.clone(), value.clone());
                    }
                }
                "MessageDeduplicationId" => {
                    if let Some(value) = self.deduplication_id.as_ref() {
                        selected.insert(name.clone(), value.clone());
                    }
                }
                "MessageGroupId" => {
                    if let Some(value) = self.message_group_id.as_ref() {
                        selected.insert(name.clone(), value.clone());
                    }
                }
                "SenderId" => {
                    selected.insert(
                        name.clone(),
                        queue.account_id().as_str().to_owned(),
                    );
                }
                "SentTimestamp" => {
                    selected.insert(
                        name.clone(),
                        self.sent_timestamp_millis.to_string(),
                    );
                }
                "SequenceNumber" => {
                    if let Some(value) = self.sequence_number.as_ref() {
                        selected.insert(name.clone(), value.clone());
                    }
                }
                _ => {}
            }
        }

        selected
    }
}

fn long_poll_wait_duration(remaining_millis: u64) -> Duration {
    let remaining = Duration::from_millis(remaining_millis);
    remaining.min(LONG_POLL_INTERVAL)
}

pub(crate) trait BatchEntry {
    fn id(&self) -> &str;
}

impl BatchEntry for SendMessageBatchEntryInput {
    fn id(&self) -> &str {
        &self.id
    }
}

impl BatchEntry for DeleteMessageBatchEntryInput {
    fn id(&self) -> &str {
        &self.id
    }
}

impl BatchEntry for ChangeMessageVisibilityBatchEntryInput {
    fn id(&self) -> &str {
        &self.id
    }
}

fn batch_failure(id: &str, error: &SqsError) -> BatchFailure {
    let (code, message) = match error {
        SqsError::InvalidAttributeName { attribute_name } => (
            "InvalidAttributeName",
            format!("Unknown Attribute {attribute_name}."),
        ),
        SqsError::InvalidAddress { message } => {
            ("InvalidAddress", message.clone())
        }
        SqsError::BatchEntryIdsNotDistinct { message } => (
            "AWS.SimpleQueueService.BatchEntryIdsNotDistinct",
            message.clone(),
        ),
        SqsError::BatchRequestTooLong { message } => {
            ("BatchRequestTooLong", message.clone())
        }
        SqsError::EmptyBatchRequest { message } => {
            ("AWS.SimpleQueueService.EmptyBatchRequest", message.clone())
        }
        SqsError::InvalidBatchEntryId { message } => {
            ("AWS.SimpleQueueService.InvalidBatchEntryId", message.clone())
        }
        SqsError::InvalidMessageContents { message } => {
            ("InvalidMessageContents", message.clone())
        }
        SqsError::InvalidParameterValue { message } => {
            ("InvalidParameterValue", message.clone())
        }
        SqsError::MissingParameter { message } => {
            ("MissingParameter", message.clone())
        }
        SqsError::OverLimit { message } => ("OverLimit", message.clone()),
        SqsError::PurgeQueueInProgress { message } => {
            ("AWS.SimpleQueueService.PurgeQueueInProgress", message.clone())
        }
        SqsError::QueueAlreadyExists { message } => {
            ("QueueAlreadyExists", message.clone())
        }
        SqsError::QueueDoesNotExist => (
            "AWS.SimpleQueueService.NonExistentQueue",
            "The specified queue does not exist.".to_owned(),
        ),
        SqsError::ReceiptHandleIsInvalid { receipt_handle } => (
            "ReceiptHandleIsInvalid",
            format!(
                "The input receipt handle \"{receipt_handle}\" is not a valid receipt handle."
            ),
        ),
        SqsError::InvalidReceiptHandleForVisibility { receipt_handle } => (
            "InvalidParameterValue",
            format!(
                "Value {receipt_handle} for parameter ReceiptHandle is invalid. Reason: Message does not exist or is not available for visibility timeout change."
            ),
        ),
        SqsError::MessageNotInflight { message } => {
            ("MessageNotInflight", message.clone())
        }
        SqsError::ResourceNotFound { message } => {
            ("ResourceNotFoundException", message.clone())
        }
        SqsError::UnsupportedOperation { message } => {
            ("AWS.SimpleQueueService.UnsupportedOperation", message.clone())
        }
    };

    BatchFailure {
        code: code.to_owned(),
        id: id.to_owned(),
        message,
        sender_fault: true,
    }
}

pub(crate) fn validate_batch_request<T>(entries: &[T]) -> Result<(), SqsError>
where
    T: BatchEntry,
{
    if entries.is_empty() {
        return Err(SqsError::EmptyBatchRequest {
            message: "Batch requests must contain at least one entry."
                .to_owned(),
        });
    }
    if entries.len() > MAX_NUMBER_OF_MESSAGES as usize {
        return Err(SqsError::InvalidParameterValue {
            message: format!(
                "Maximum number of entries per request are {MAX_NUMBER_OF_MESSAGES}. You have sent {}.",
                entries.len()
            ),
        });
    }

    let mut seen = BTreeSet::new();
    for entry in entries {
        validate_batch_entry_id(entry.id())?;
        if !seen.insert(entry.id().to_owned()) {
            return Err(SqsError::BatchEntryIdsNotDistinct {
                message:
                    "Two or more batch entries in the request have the same Id."
                        .to_owned(),
            });
        }
    }

    Ok(())
}

pub(crate) fn validate_batch_entry_id(id: &str) -> Result<(), SqsError> {
    if id.is_empty()
        || id.len() > MAX_BATCH_ENTRY_ID_LENGTH
        || !id.chars().all(|character| {
            character.is_ascii_alphanumeric() || matches!(character, '-' | '_')
        })
    {
        return Err(SqsError::InvalidBatchEntryId {
            message: "A batch entry id can only contain alphanumeric characters, hyphens and underscores. It can be at most 80 letters long.".to_owned(),
        });
    }

    Ok(())
}

pub(crate) fn validate_delay_seconds(
    delay_seconds: u32,
) -> Result<(), SqsError> {
    if delay_seconds > MAX_DELAY_SECONDS {
        return Err(SqsError::InvalidParameterValue {
            message: format!(
                "Value {delay_seconds} for parameter DelaySeconds is invalid. Reason: Must be <= {MAX_DELAY_SECONDS}."
            ),
        });
    }

    Ok(())
}

pub(crate) fn validate_max_number_of_messages(
    max_number_of_messages: u32,
) -> Result<(), SqsError> {
    if !(1..=MAX_NUMBER_OF_MESSAGES).contains(&max_number_of_messages) {
        return Err(SqsError::InvalidParameterValue {
            message: format!(
                "Value {max_number_of_messages} for parameter MaxNumberOfMessages is invalid. Reason: Must be between 1 and 10, if provided."
            ),
        });
    }

    Ok(())
}

pub(crate) fn validate_wait_time_seconds(
    wait_time_seconds: u32,
) -> Result<(), SqsError> {
    if wait_time_seconds > MAX_RECEIVE_WAIT_TIME_SECONDS {
        return Err(SqsError::InvalidParameterValue {
            message: format!(
                "Value {wait_time_seconds} for parameter WaitTimeSeconds is invalid. Reason: Must be >= 0 and <= 20, if provided."
            ),
        });
    }

    Ok(())
}

pub(crate) fn validate_visibility_timeout(
    visibility_timeout: u32,
) -> Result<(), SqsError> {
    if visibility_timeout > MAX_VISIBILITY_TIMEOUT {
        return Err(SqsError::InvalidParameterValue {
            message: format!(
                "Value {visibility_timeout} for parameter VisibilityTimeout is invalid. Reason: Must be >= 0 and <= {MAX_VISIBILITY_TIMEOUT}, if provided."
            ),
        });
    }

    Ok(())
}

pub(crate) fn timestamp_seconds(time: SystemTime) -> u64 {
    time.duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0))
        .as_secs()
}

pub(crate) fn timestamp_millis(time: SystemTime) -> u64 {
    let millis = time
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0))
        .as_millis();

    u64::try_from(millis).unwrap_or(u64::MAX)
}

pub(crate) fn md5_hex(value: &str) -> String {
    format!("{:x}", md5::compute(value.as_bytes()))
}

fn receive_selectors(
    input: &ReceiveMessageInput,
) -> Result<ReceiveSelectors, SqsError> {
    let mut system_attribute_names = BTreeSet::new();
    let requested_system_attributes = input
        .attribute_names
        .iter()
        .chain(&input.message_system_attribute_names)
        .cloned()
        .collect::<Vec<_>>();
    if requested_system_attributes.iter().any(|name| name == "All") {
        for name in [
            "AWSTraceHeader",
            "ApproximateFirstReceiveTimestamp",
            "ApproximateReceiveCount",
            "DeadLetterQueueSourceArn",
            "MessageDeduplicationId",
            "MessageGroupId",
            "SenderId",
            "SentTimestamp",
            "SequenceNumber",
        ] {
            system_attribute_names.insert(name.to_owned());
        }
    } else {
        for name in requested_system_attributes {
            validate_system_attribute_name(&name)?;
            system_attribute_names.insert(name);
        }
    }

    let message_attribute_names =
        normalize_message_attribute_names(&input.message_attribute_names)?;

    Ok(ReceiveSelectors {
        message_attribute_names,
        system_attribute_names: system_attribute_names.into_iter().collect(),
    })
}

fn validate_system_attribute_name(name: &str) -> Result<(), SqsError> {
    if matches!(
        name,
        "AWSTraceHeader"
            | "ApproximateFirstReceiveTimestamp"
            | "ApproximateReceiveCount"
            | "DeadLetterQueueSourceArn"
            | "MessageDeduplicationId"
            | "MessageGroupId"
            | "SenderId"
            | "SentTimestamp"
            | "SequenceNumber"
    ) {
        Ok(())
    } else {
        Err(SqsError::InvalidAttributeName { attribute_name: name.to_owned() })
    }
}

fn normalize_message_attribute_names(
    names: &[String],
) -> Result<Vec<String>, SqsError> {
    if names.is_empty() {
        return Ok(Vec::new());
    }
    if names.iter().any(|name| name == "All" || name == ".*") {
        return Ok(vec!["All".to_owned()]);
    }
    for name in names {
        validate_message_attribute_selector_name(name)?;
    }

    Ok(names.to_vec())
}

fn validate_message_attribute_selector_name(
    name: &str,
) -> Result<(), SqsError> {
    if let Some(prefix) = name.strip_suffix(".*") {
        validate_message_attribute_name(prefix)?;
        return Ok(());
    }

    validate_message_attribute_name(name)
}

fn validate_message_body(body: &str) -> Result<(), SqsError> {
    if body.chars().any(|character| {
        let code = u32::from(character);
        !(code == 0x9
            || code == 0xA
            || code == 0xD
            || (0x20..=0xD7FF).contains(&code)
            || (0xE000..=0xFFFD).contains(&code)
            || (0x10000..=0x10FFFF).contains(&code))
    }) {
        return Err(SqsError::InvalidMessageContents {
            message:
                "The message contains characters outside the allowed set."
                    .to_owned(),
        });
    }

    Ok(())
}

fn validate_message_attributes(
    attributes: &BTreeMap<String, MessageAttributeValue>,
) -> Result<(), SqsError> {
    crate::message_attributes::validate_message_attributes(
        attributes,
        validate_message_body,
    )
}

fn validate_message_system_attributes(
    attributes: &BTreeMap<String, MessageAttributeValue>,
) -> Result<(), SqsError> {
    crate::message_attributes::validate_message_system_attributes(
        attributes,
        validate_message_body,
    )
}

fn validate_message_size(
    maximum_message_size: u32,
    body: &str,
    message_attributes: &BTreeMap<String, MessageAttributeValue>,
) -> Result<(), SqsError> {
    let body_size = body.len();
    let message_attributes_size = message_attributes
        .iter()
        .map(|(name, value)| message_attribute_size(name, value))
        .sum::<usize>();
    let total_size = body_size.saturating_add(message_attributes_size);
    if total_size > maximum_message_size as usize {
        return Err(SqsError::InvalidParameterValue {
            message: format!(
                "One or more parameters are invalid. Reason: Message must be shorter than {maximum_message_size} bytes."
            ),
        });
    }

    Ok(())
}
