use crate::attributes::{
    ensure_create_attributes_are_compatible, normalize_queue_attributes,
};
use crate::errors::SqsError;
use crate::fifo::DeduplicationRecord;
use crate::messages::{
    ListQueuesInput, MessageRecord, PaginatedQueues, ReceiveAttemptRecord,
    timestamp_seconds,
};
use crate::redrive::{MessageMoveTaskRecord, validate_redrive_policy_target};
use crate::scope::{SqsQueueIdentity, SqsScope};
use aws::Arn;
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use serde_json::{Value, json};
use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateQueueInput {
    pub attributes: BTreeMap<String, String>,
    pub queue_name: String,
}

pub trait SqsIdentifierSource: Send + Sync {
    fn next_message_id(&self) -> String;
    fn next_receipt_handle(&self) -> String;
}

pub(crate) trait SqsReceiveCancellation: Send + Sync {
    fn is_cancelled(&self) -> bool;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SqsReceiveWaitOutcome {
    Cancelled,
    Completed,
}

pub(crate) trait SqsReceiveWaiter: Send + Sync {
    fn wait(
        &self,
        duration: Duration,
        cancellation: &dyn SqsReceiveCancellation,
    ) -> SqsReceiveWaitOutcome;
}

#[derive(Debug, Default)]
pub(crate) struct ThreadSleepSqsReceiveWaiter;

const RECEIVE_CANCELLATION_POLL_INTERVAL: Duration = Duration::from_millis(50);

impl SqsReceiveWaiter for ThreadSleepSqsReceiveWaiter {
    fn wait(
        &self,
        duration: Duration,
        cancellation: &dyn SqsReceiveCancellation,
    ) -> SqsReceiveWaitOutcome {
        let mut remaining = duration;
        while !remaining.is_zero() {
            if cancellation.is_cancelled() {
                return SqsReceiveWaitOutcome::Cancelled;
            }

            let wait_for = remaining.min(RECEIVE_CANCELLATION_POLL_INTERVAL);
            std::thread::sleep(wait_for);
            remaining = remaining.saturating_sub(wait_for);
        }

        if cancellation.is_cancelled() {
            SqsReceiveWaitOutcome::Cancelled
        } else {
            SqsReceiveWaitOutcome::Completed
        }
    }
}

pub(crate) struct CallbackSqsReceiveCancellation<'a, F: ?Sized> {
    is_cancelled: &'a F,
}

impl<'a, F: ?Sized> CallbackSqsReceiveCancellation<'a, F> {
    pub(crate) fn new(is_cancelled: &'a F) -> Self {
        Self { is_cancelled }
    }
}

impl<F> SqsReceiveCancellation for CallbackSqsReceiveCancellation<'_, F>
where
    F: Fn() -> bool + Send + Sync + ?Sized,
{
    fn is_cancelled(&self) -> bool {
        (self.is_cancelled)()
    }
}

#[derive(Debug, Default)]
pub struct SequentialSqsIdentifierSource {
    next_message_id: AtomicU64,
    next_receipt_handle: AtomicU64,
}

impl SqsIdentifierSource for SequentialSqsIdentifierSource {
    fn next_message_id(&self) -> String {
        let id = self.next_message_id.fetch_add(1, Ordering::Relaxed) + 1;
        format!("00000000-0000-0000-0000-{id:012}")
    }

    fn next_receipt_handle(&self) -> String {
        let id = self.next_receipt_handle.fetch_add(1, Ordering::Relaxed) + 1;
        format!("AQEB{id:020}")
    }
}

#[derive(Clone)]
pub struct SqsService {
    pub(crate) identifier_source: Arc<dyn SqsIdentifierSource + Send + Sync>,
    pub(crate) receive_waiter: Arc<dyn SqsReceiveWaiter + Send + Sync>,
    pub(crate) state: Arc<Mutex<SqsWorld>>,
    pub(crate) time_source: Arc<dyn Fn() -> SystemTime + Send + Sync>,
}

impl Default for SqsService {
    fn default() -> Self {
        Self::new()
    }
}

impl SqsService {
    pub fn new() -> Self {
        Self::with_waiter_sources(
            Arc::new(SystemTime::now),
            Arc::new(SequentialSqsIdentifierSource::default()),
            Arc::new(ThreadSleepSqsReceiveWaiter),
        )
    }

    pub fn with_sources(
        time_source: Arc<dyn Fn() -> SystemTime + Send + Sync>,
        identifier_source: Arc<dyn SqsIdentifierSource + Send + Sync>,
    ) -> Self {
        Self::with_waiter_sources(
            time_source,
            identifier_source,
            Arc::new(ThreadSleepSqsReceiveWaiter),
        )
    }

    pub(crate) fn with_waiter_sources(
        time_source: Arc<dyn Fn() -> SystemTime + Send + Sync>,
        identifier_source: Arc<dyn SqsIdentifierSource + Send + Sync>,
        receive_waiter: Arc<dyn SqsReceiveWaiter + Send + Sync>,
    ) -> Self {
        Self {
            identifier_source,
            receive_waiter,
            state: Arc::default(),
            time_source,
        }
    }

    /// Creates an SQS queue within the provided account and region scope.
    ///
    /// # Errors
    ///
    /// Returns [`SqsError`] when the queue name or attributes are invalid, or when
    /// the same queue name already exists with incompatible attributes.
    pub fn create_queue(
        &self,
        scope: &SqsScope,
        input: CreateQueueInput,
    ) -> Result<SqsQueueIdentity, SqsError> {
        let mut state =
            self.state.lock().unwrap_or_else(|poison| poison.into_inner());

        state.create_queue(
            scope,
            input,
            timestamp_seconds((self.time_source)()),
        )
    }

    /// Deletes an existing queue.
    ///
    /// # Errors
    ///
    /// Returns [`SqsError::QueueDoesNotExist`] when the queue is unknown.
    pub fn delete_queue(
        &self,
        queue: &SqsQueueIdentity,
    ) -> Result<(), SqsError> {
        let mut state =
            self.state.lock().unwrap_or_else(|poison| poison.into_inner());

        state.delete_queue(queue)
    }

    /// Resolves a queue name to its scoped identity.
    ///
    /// # Errors
    ///
    /// Returns [`SqsError`] when the queue name is invalid or no queue exists in the given scope.
    pub fn get_queue_url(
        &self,
        scope: &SqsScope,
        queue_name: &str,
    ) -> Result<SqsQueueIdentity, SqsError> {
        let state =
            self.state.lock().unwrap_or_else(|poison| poison.into_inner());

        state.get_queue(scope, queue_name)
    }

    pub fn list_queues(
        &self,
        scope: &SqsScope,
        queue_name_prefix: Option<&str>,
    ) -> Vec<SqsQueueIdentity> {
        let state =
            self.state.lock().unwrap_or_else(|poison| poison.into_inner());

        state.list_queues(scope, queue_name_prefix)
    }

    pub fn list_queues_page(
        &self,
        scope: &SqsScope,
        input: &ListQueuesInput,
    ) -> Result<PaginatedQueues, SqsError> {
        let state =
            self.state.lock().unwrap_or_else(|poison| poison.into_inner());

        state.list_queues_page(scope, input)
    }
}

#[derive(Debug, Default)]
pub(crate) struct SqsWorld {
    pub(crate) message_move_tasks: Vec<MessageMoveTaskRecord>,
    pub(crate) queues: BTreeMap<SqsQueueIdentity, QueueRecord>,
}

#[derive(Debug, Clone)]
pub(crate) struct StagedMessageMove {
    pub(crate) message: MessageRecord,
    pub(crate) source_message_index: usize,
    pub(crate) source_queue: SqsQueueIdentity,
    pub(crate) target_queue: SqsQueueIdentity,
}

impl SqsWorld {
    pub(crate) fn create_queue(
        &mut self,
        scope: &SqsScope,
        input: CreateQueueInput,
        now_seconds: u64,
    ) -> Result<SqsQueueIdentity, SqsError> {
        let identity = SqsQueueIdentity::new(
            scope.account_id().clone(),
            scope.region().clone(),
            input.queue_name,
        )?;
        let attributes = normalize_queue_attributes(
            &identity,
            input.attributes,
            crate::attributes::AttributeMode::Create,
        )?;

        if let Some(existing) = self.queues.get(&identity) {
            ensure_create_attributes_are_compatible(existing, &attributes)?;
            return Ok(identity);
        }

        validate_redrive_policy_target(self, &identity, &attributes)?;
        self.queues.insert(
            identity.clone(),
            QueueRecord::new(identity.clone(), attributes, now_seconds),
        );

        Ok(identity)
    }

    pub(crate) fn delete_queue(
        &mut self,
        queue: &SqsQueueIdentity,
    ) -> Result<(), SqsError> {
        if self.queues.remove(queue).is_none() {
            return Err(SqsError::QueueDoesNotExist);
        }

        Ok(())
    }

    pub(crate) fn get_queue(
        &self,
        scope: &SqsScope,
        queue_name: &str,
    ) -> Result<SqsQueueIdentity, SqsError> {
        let identity = SqsQueueIdentity::new(
            scope.account_id().clone(),
            scope.region().clone(),
            queue_name.to_owned(),
        )?;

        self.queues
            .contains_key(&identity)
            .then_some(identity)
            .ok_or(SqsError::QueueDoesNotExist)
    }

    pub(crate) fn list_queues(
        &self,
        scope: &SqsScope,
        queue_name_prefix: Option<&str>,
    ) -> Vec<SqsQueueIdentity> {
        self.queues
            .keys()
            .filter(|queue| {
                queue.account_id() == scope.account_id()
                    && queue.region() == scope.region()
                    && queue_name_prefix.is_none_or(|prefix| {
                        queue.queue_name().starts_with(prefix)
                    })
            })
            .cloned()
            .collect()
    }

    pub(crate) fn list_queues_page(
        &self,
        scope: &SqsScope,
        input: &ListQueuesInput,
    ) -> Result<PaginatedQueues, SqsError> {
        let queues =
            self.list_queues(scope, input.queue_name_prefix.as_deref());
        let context = pagination_context(
            "ListQueues",
            &json!({
                "account_id": scope.account_id().as_str(),
                "region": scope.region().as_ref(),
                "prefix": input.queue_name_prefix,
            }),
        );
        let (queue_urls, next_token) = paginate_items(
            &queues,
            input.max_results,
            input.next_token.as_deref(),
            &context,
        )?;

        Ok(PaginatedQueues { next_token, queue_urls })
    }

    pub(crate) fn stage_message_move(
        &self,
        source_queue: &SqsQueueIdentity,
        source_message_index: usize,
        target_arn: &Arn,
        message: MessageRecord,
    ) -> Result<StagedMessageMove, SqsError> {
        let target_queue = crate::redrive::parse_queue_identity_from_arn(
            target_arn, "QueueArn",
        )?;

        Ok(StagedMessageMove {
            message,
            source_message_index,
            source_queue: source_queue.clone(),
            target_queue,
        })
    }

    pub(crate) fn commit_staged_message_moves(
        &mut self,
        staged_moves: Vec<StagedMessageMove>,
    ) -> Result<(), SqsError> {
        for staged in &staged_moves {
            let _ = self
                .queues
                .get(&staged.source_queue)
                .ok_or(SqsError::QueueDoesNotExist)?;
            let target_queue = self
                .queues
                .get(&staged.target_queue)
                .ok_or(SqsError::QueueDoesNotExist)?;
            if target_queue.is_fifo()
                && staged.message.message_group_id.is_none()
            {
                return Err(SqsError::InvalidParameterValue {
                    message:
                        "The moved FIFO message is missing its MessageGroupId."
                            .to_owned(),
                });
            }
        }

        for staged in &staged_moves {
            let source_queue = self
                .queues
                .get_mut(&staged.source_queue)
                .ok_or(SqsError::QueueDoesNotExist)?;
            let (message_id, deduplication_id) = {
                let Some(source_message) =
                    source_queue.messages.get_mut(staged.source_message_index)
                else {
                    return Err(SqsError::QueueDoesNotExist);
                };
                source_message.deleted = true;
                (
                    source_message.message_id.clone(),
                    source_message.deduplication_id.clone(),
                )
            };
            source_queue.invalidate_receive_attempts_for_message(&message_id);
            if let Some(deduplication_id) = deduplication_id.as_ref() {
                source_queue.deduplication_records.remove(deduplication_id);
            }
        }

        for staged in staged_moves {
            let target_queue = self
                .queues
                .get_mut(&staged.target_queue)
                .ok_or(SqsError::QueueDoesNotExist)?;
            let mut message = staged.message;
            if target_queue.is_fifo() && message.sequence_number.is_none() {
                message.sequence_number =
                    Some(target_queue.next_sequence_number());
            }
            target_queue.messages.push_back(message);
        }

        Ok(())
    }
}

#[derive(Debug, Clone)]
pub(crate) struct QueueRecord {
    pub(crate) attributes: BTreeMap<String, String>,
    pub(crate) created_timestamp: u64,
    pub(crate) deduplication_records: BTreeMap<String, DeduplicationRecord>,
    pub(crate) identity: SqsQueueIdentity,
    pub(crate) last_purged_timestamp_millis: Option<u64>,
    pub(crate) last_modified_timestamp: u64,
    pub(crate) messages: VecDeque<MessageRecord>,
    pub(crate) next_sequence_number: u128,
    pub(crate) receive_attempts: BTreeMap<String, ReceiveAttemptRecord>,
    pub(crate) tags: BTreeMap<String, String>,
}

impl QueueRecord {
    pub(crate) fn new(
        identity: SqsQueueIdentity,
        attributes: BTreeMap<String, String>,
        now_seconds: u64,
    ) -> Self {
        Self {
            attributes,
            created_timestamp: now_seconds,
            deduplication_records: BTreeMap::new(),
            identity,
            last_purged_timestamp_millis: None,
            last_modified_timestamp: now_seconds,
            messages: VecDeque::new(),
            next_sequence_number: 0,
            receive_attempts: BTreeMap::new(),
            tags: BTreeMap::new(),
        }
    }
}

pub(crate) fn pagination_context(
    operation: &'static str,
    anchor: &Value,
) -> Value {
    json!({
        "operation": operation,
        "anchor": anchor,
    })
}

pub(crate) fn paginate_items<T: Clone>(
    items: &[T],
    max_results: Option<u32>,
    next_token: Option<&str>,
    context: &Value,
) -> Result<(Vec<T>, Option<String>), SqsError> {
    let page_size = validate_paginated_max_results(max_results)?;
    let start = decode_next_token(next_token, context)?;
    if start > items.len() {
        return Err(invalid_next_token());
    }
    let end = start.saturating_add(page_size).min(items.len());
    let next_token = (end < items.len())
        .then(|| encode_next_token(context, end))
        .transpose()?;

    Ok((items[start..end].to_vec(), next_token))
}

fn validate_paginated_max_results(
    max_results: Option<u32>,
) -> Result<usize, SqsError> {
    let max_results = max_results.unwrap_or(1_000);
    if !(1..=1_000).contains(&max_results) {
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

fn encode_next_token(
    context: &Value,
    start: usize,
) -> Result<String, SqsError> {
    let payload = json!({
        "operation": context["operation"],
        "anchor": context["anchor"],
        "start": start,
    });
    let bytes =
        serde_json::to_vec(&payload).map_err(|_| invalid_next_token())?;

    Ok(URL_SAFE_NO_PAD.encode(bytes))
}

fn decode_next_token(
    next_token: Option<&str>,
    context: &Value,
) -> Result<usize, SqsError> {
    let Some(next_token) = next_token else {
        return Ok(0);
    };
    let decoded = URL_SAFE_NO_PAD
        .decode(next_token)
        .map_err(|_| invalid_next_token())?;
    let payload: Value =
        serde_json::from_slice(&decoded).map_err(|_| invalid_next_token())?;
    if payload.get("operation") != context.get("operation")
        || payload.get("anchor") != context.get("anchor")
    {
        return Err(invalid_next_token());
    }
    let start = payload
        .get("start")
        .and_then(Value::as_u64)
        .ok_or_else(invalid_next_token)?;

    usize::try_from(start).map_err(|_| invalid_next_token())
}

fn invalid_next_token() -> SqsError {
    SqsError::InvalidParameterValue {
        message: "Invalid value for the parameter NextToken.".to_owned(),
    }
}
