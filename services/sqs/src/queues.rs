use crate::attributes::{
    ensure_create_attributes_are_compatible, normalize_queue_attributes,
};
use crate::errors::SqsError;
use crate::fifo::DeduplicationRecord;
use crate::messages::{MessageRecord, timestamp_seconds};
use crate::redrive::validate_redrive_policy_target;
use crate::scope::{SqsQueueIdentity, SqsScope};
use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateQueueInput {
    pub attributes: BTreeMap<String, String>,
    pub queue_name: String,
}

pub trait SqsIdentifierSource: Send + Sync {
    fn next_message_id(&self) -> String;
    fn next_receipt_handle(&self) -> String;
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
        Self::with_sources(
            Arc::new(SystemTime::now),
            Arc::new(SequentialSqsIdentifierSource::default()),
        )
    }

    pub fn with_sources(
        time_source: Arc<dyn Fn() -> SystemTime + Send + Sync>,
        identifier_source: Arc<dyn SqsIdentifierSource + Send + Sync>,
    ) -> Self {
        Self { identifier_source, state: Arc::default(), time_source }
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
}

#[derive(Debug, Default)]
pub(crate) struct SqsWorld {
    pub(crate) queues: BTreeMap<SqsQueueIdentity, QueueRecord>,
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
}

#[derive(Debug, Clone)]
pub(crate) struct QueueRecord {
    pub(crate) attributes: BTreeMap<String, String>,
    pub(crate) created_timestamp: u64,
    pub(crate) deduplication_records: BTreeMap<String, DeduplicationRecord>,
    pub(crate) identity: SqsQueueIdentity,
    pub(crate) last_modified_timestamp: u64,
    pub(crate) messages: VecDeque<MessageRecord>,
    pub(crate) next_sequence_number: u128,
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
            last_modified_timestamp: now_seconds,
            messages: VecDeque::new(),
            next_sequence_number: 0,
            tags: BTreeMap::new(),
        }
    }
}
