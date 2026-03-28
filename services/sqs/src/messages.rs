use crate::attributes::{
    DEFAULT_DELAY_SECONDS, MAX_DELAY_SECONDS, MAX_VISIBILITY_TIMEOUT,
};
use crate::errors::SqsError;
use crate::queues::{QueueRecord, SqsService, SqsWorld};
use crate::scope::SqsQueueIdentity;
use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const MAX_NUMBER_OF_MESSAGES: u32 = 10;
const MAX_RECEIVE_WAIT_TIME_SECONDS: u32 = 20;
const MAX_BATCH_ENTRY_ID_LENGTH: usize = 80;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SendMessageInput {
    pub body: String,
    pub delay_seconds: Option<u32>,
    pub message_deduplication_id: Option<String>,
    pub message_group_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SendMessageOutput {
    pub md5_of_message_body: String,
    pub message_id: String,
    pub sequence_number: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReceiveMessageInput {
    pub max_number_of_messages: Option<u32>,
    pub visibility_timeout: Option<u32>,
    pub wait_time_seconds: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReceivedMessage {
    pub attributes: BTreeMap<String, String>,
    pub body: String,
    pub md5_of_body: String,
    pub message_id: String,
    pub receipt_handle: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SendMessageBatchEntryInput {
    pub body: String,
    pub delay_seconds: Option<u32>,
    pub id: String,
    pub message_deduplication_id: Option<String>,
    pub message_group_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SendMessageBatchSuccess {
    pub id: String,
    pub md5_of_message_body: String,
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
    pub(crate) dead_letter_messages: Vec<MessageRecord>,
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
        let mut state =
            self.state.lock().unwrap_or_else(|poison| poison.into_inner());
        let mut next_receipt_handle =
            || self.identifier_source.next_receipt_handle();

        state.receive_message(
            queue,
            input,
            timestamp_millis((self.time_source)()),
            &mut next_receipt_handle,
        )
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

        state.purge_queue(queue)
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
                    message_deduplication_id: entry.message_deduplication_id,
                    message_group_id: entry.message_group_id,
                },
                now_millis,
                next_message_id(),
            ) {
                Ok(result) => successful.push(SendMessageBatchSuccess {
                    id: entry.id,
                    md5_of_message_body: result.md5_of_message_body,
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
        input: ReceiveMessageInput,
        now_millis: u64,
        next_receipt_handle: &mut dyn FnMut() -> String,
    ) -> Result<Vec<ReceivedMessage>, SqsError> {
        let (received, dead_letter_messages, dead_letter_target_arn) = {
            let queue = self
                .queues
                .get_mut(queue)
                .ok_or(SqsError::QueueDoesNotExist)?;
            let max_number_of_messages =
                input.max_number_of_messages.unwrap_or(1);
            validate_max_number_of_messages(max_number_of_messages)?;
            let wait_time_seconds = input
                .wait_time_seconds
                .unwrap_or_else(|| queue.receive_wait_time());
            validate_wait_time_seconds(wait_time_seconds)?;
            let visibility_timeout = input
                .visibility_timeout
                .unwrap_or_else(|| queue.visibility_timeout());
            validate_visibility_timeout(visibility_timeout)?;

            let outcome = if queue.is_fifo() {
                queue.receive_fifo_messages(
                    max_number_of_messages,
                    now_millis,
                    visibility_timeout,
                    next_receipt_handle,
                )
            } else {
                queue.receive_standard_messages(
                    max_number_of_messages,
                    now_millis,
                    visibility_timeout,
                    next_receipt_handle,
                )
            };

            (
                outcome.received,
                outcome.dead_letter_messages,
                queue
                    .redrive_policy_document()
                    .map(|policy| policy.dead_letter_target_arn),
            )
        };

        if let Some(target_arn) =
            dead_letter_target_arn.and_then(|value| value.parse().ok())
        {
            for message in dead_letter_messages {
                self.enqueue_moved_message(&target_arn, message)?;
            }
        }

        Ok(received)
    }

    pub(crate) fn delete_message(
        &mut self,
        queue: &SqsQueueIdentity,
        receipt_handle: &str,
    ) -> Result<(), SqsError> {
        let queue =
            self.queues.get_mut(queue).ok_or(SqsError::QueueDoesNotExist)?;
        let Some(message) = queue.messages.iter_mut().find(|message| {
            message.issued_receipt_handles.contains(receipt_handle)
        }) else {
            return Err(SqsError::ReceiptHandleIsInvalid {
                receipt_handle: receipt_handle.to_owned(),
            });
        };

        if message.deleted {
            return Ok(());
        }

        message.deleted = true;
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
            message.issued_receipt_handles.contains(receipt_handle)
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

        message.visible_at_millis =
            now_millis + u64::from(visibility_timeout).saturating_mul(1_000);

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
    ) -> Result<(), SqsError> {
        let queue =
            self.queues.get_mut(queue).ok_or(SqsError::QueueDoesNotExist)?;
        queue.messages.clear();
        queue.deduplication_records.clear();

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
        let body = input.body.clone();
        let md5_of_message_body = md5_hex(&body);

        if self.is_fifo() {
            return self.send_fifo_message(
                input,
                now_millis,
                message_id,
                md5_of_message_body,
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
            issued_receipt_handles: BTreeSet::new(),
            latest_receipt_handle: None,
            md5_of_body: md5_of_message_body.clone(),
            message_group_id: None,
            message_id: message_id.clone(),
            receive_count: 0,
            sent_timestamp_millis: now_millis,
            sequence_number: None,
            visible_at_millis: now_millis
                + u64::from(delay_seconds).saturating_mul(1_000),
        });

        Ok(SendMessageOutput {
            md5_of_message_body,
            message_id,
            sequence_number: None,
        })
    }

    pub(crate) fn receive_standard_messages(
        &mut self,
        max_number_of_messages: u32,
        now_millis: u64,
        visibility_timeout: u32,
        next_receipt_handle: &mut dyn FnMut() -> String,
    ) -> ReceiveOutcome {
        let mut outcome = ReceiveOutcome::default();
        let queue_arn = self.queue_arn();
        let max_receive_count = self
            .redrive_policy_document()
            .map(|policy| policy.max_receive_count);

        for message in &mut self.messages {
            if outcome.received.len() == max_number_of_messages as usize {
                break;
            }
            if !message.is_receivable(now_millis) {
                continue;
            }

            if max_receive_count
                .is_some_and(|limit| message.receive_count + 1 > limit)
            {
                outcome.dead_letter_messages.push(
                    message.take_for_queue_move(Some(queue_arn.clone())),
                );
                continue;
            }

            outcome.received.push(message.receive(
                now_millis,
                visibility_timeout,
                next_receipt_handle(),
            ));
        }

        outcome
    }
}

#[derive(Debug, Clone)]
pub(crate) struct MessageRecord {
    pub(crate) body: String,
    pub(crate) dead_letter_source_arn: Option<String>,
    pub(crate) deleted: bool,
    pub(crate) deduplication_id: Option<String>,
    pub(crate) issued_receipt_handles: BTreeSet<String>,
    pub(crate) latest_receipt_handle: Option<String>,
    pub(crate) md5_of_body: String,
    pub(crate) message_group_id: Option<String>,
    pub(crate) message_id: String,
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
        visibility_timeout: u32,
        receipt_handle: String,
    ) -> ReceivedMessage {
        self.receive_count += 1;
        self.visible_at_millis =
            now_millis + u64::from(visibility_timeout).saturating_mul(1_000);
        self.issued_receipt_handles.insert(receipt_handle.clone());
        self.latest_receipt_handle = Some(receipt_handle.clone());

        let mut attributes = BTreeMap::new();
        attributes.insert(
            "ApproximateReceiveCount".to_owned(),
            self.receive_count.to_string(),
        );
        attributes.insert(
            "SentTimestamp".to_owned(),
            self.sent_timestamp_millis.to_string(),
        );

        ReceivedMessage {
            attributes,
            body: self.body.clone(),
            md5_of_body: self.md5_of_body.clone(),
            message_id: self.message_id.clone(),
            receipt_handle,
        }
    }

    pub(crate) fn take_for_queue_move(
        &mut self,
        dead_letter_source_arn: Option<String>,
    ) -> MessageRecord {
        self.deleted = true;

        MessageRecord {
            body: self.body.clone(),
            dead_letter_source_arn,
            deleted: false,
            deduplication_id: self.deduplication_id.clone(),
            issued_receipt_handles: BTreeSet::new(),
            latest_receipt_handle: None,
            md5_of_body: self.md5_of_body.clone(),
            message_group_id: self.message_group_id.clone(),
            message_id: self.message_id.clone(),
            receive_count: 0,
            sent_timestamp_millis: self.sent_timestamp_millis,
            sequence_number: self.sequence_number.clone(),
            visible_at_millis: self.sent_timestamp_millis,
        }
    }
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
        SqsError::BatchEntryIdsNotDistinct { message } => (
            "AWS.SimpleQueueService.BatchEntryIdsNotDistinct",
            message.clone(),
        ),
        SqsError::EmptyBatchRequest { message } => {
            ("AWS.SimpleQueueService.EmptyBatchRequest", message.clone())
        }
        SqsError::InvalidBatchEntryId { message } => {
            ("AWS.SimpleQueueService.InvalidBatchEntryId", message.clone())
        }
        SqsError::InvalidParameterValue { message } => {
            ("InvalidParameterValue", message.clone())
        }
        SqsError::MissingParameter { message } => {
            ("MissingParameter", message.clone())
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
        SqsError::ResourceNotFound { message } => {
            ("ResourceNotFoundException", message.clone())
        }
        SqsError::UnsupportedOperation { message } => {
            ("UnsupportedOperation", message.clone())
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
