use super::MessageAttributeValue;
use super::attributes::{
    AttributeMode, ensure_create_attributes_are_compatible,
    normalize_queue_attributes, normalize_requested_attribute_names,
    parse_boolean_attribute, validate_numeric_attribute,
    validate_positive_attribute, validate_queue_name,
};
use super::errors::SqsError;
use super::fifo::validate_fifo_identifier;
use super::messages::{
    ChangeMessageVisibilityBatchEntryInput, DeleteMessageBatchEntryInput,
    ListDeadLetterSourceQueuesInput, ListQueuesInput, MessageRecord,
    ReceiveMessageInput, SendMessageBatchEntryInput, SendMessageInput,
    md5_hex, validate_batch_entry_id, validate_batch_request,
    validate_delay_seconds, validate_max_number_of_messages,
    validate_visibility_timeout, validate_wait_time_seconds,
};
use super::queues::{
    CreateQueueInput, QueueRecord, SqsIdentifierSource,
    SqsReceiveCancellation, SqsReceiveWaitOutcome, SqsReceiveWaiter,
    SqsService,
};
use super::redrive::{
    CancelMessageMoveTaskInput, ListMessageMoveTasksInput,
    StartMessageMoveTaskInput, encode_move_task_handle,
    parse_queue_identity_from_arn, parse_redrive_policy,
};
use super::request_runtime::SqsRequestRuntime;
use super::scope::{SqsQueueIdentity, SqsScope};
use aws::{AccountId, Arn, RegionId};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[derive(Debug)]
struct TestIdentifierSource {
    message_ids: Mutex<VecDeque<String>>,
    receipt_handles: Mutex<VecDeque<String>>,
}

impl TestIdentifierSource {
    fn new(message_ids: &[&str], receipt_handles: &[&str]) -> Self {
        Self {
            message_ids: Mutex::new(
                message_ids.iter().map(|value| (*value).to_owned()).collect(),
            ),
            receipt_handles: Mutex::new(
                receipt_handles
                    .iter()
                    .map(|value| (*value).to_owned())
                    .collect(),
            ),
        }
    }
}

impl SqsIdentifierSource for TestIdentifierSource {
    fn next_message_id(&self) -> String {
        self.message_ids
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .pop_front()
            .expect("test message ids should be available")
    }

    fn next_receipt_handle(&self) -> String {
        self.receipt_handles
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .pop_front()
            .expect("test receipt handles should be available")
    }
}

#[derive(Debug, Clone)]
struct ManualClock {
    now: Arc<Mutex<SystemTime>>,
}

impl ManualClock {
    fn new(now: SystemTime) -> Self {
        Self { now: Arc::new(Mutex::new(now)) }
    }

    fn advance(&self, duration: Duration) {
        let mut now =
            self.now.lock().unwrap_or_else(|poison| poison.into_inner());
        *now += duration;
    }

    fn source(&self) -> Arc<dyn Fn() -> SystemTime + Send + Sync> {
        let now = self.now.clone();

        Arc::new(move || {
            *now.lock().unwrap_or_else(|poison| poison.into_inner())
        })
    }
}

#[derive(Debug, Clone)]
struct TestWaiter {
    clock: ManualClock,
    waits: Arc<Mutex<Vec<Duration>>>,
}

impl TestWaiter {
    fn new(clock: ManualClock) -> Self {
        Self { clock, waits: Arc::new(Mutex::new(Vec::new())) }
    }

    fn total_wait(&self) -> Duration {
        self.waits
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .iter()
            .copied()
            .fold(Duration::ZERO, |total, wait| total + wait)
    }
}

impl SqsReceiveWaiter for TestWaiter {
    fn wait(
        &self,
        duration: Duration,
        cancellation: &dyn SqsReceiveCancellation,
    ) -> SqsReceiveWaitOutcome {
        if cancellation.is_cancelled() {
            return SqsReceiveWaitOutcome::Cancelled;
        }
        self.waits
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .push(duration);
        self.clock.advance(duration);
        if cancellation.is_cancelled() {
            SqsReceiveWaitOutcome::Cancelled
        } else {
            SqsReceiveWaitOutcome::Completed
        }
    }
}

#[derive(Clone)]
struct ActionWaiter {
    action: Arc<Mutex<Option<Box<dyn Fn() + Send + Sync>>>>,
    clock: ManualClock,
    triggered: Arc<AtomicBool>,
}

impl ActionWaiter {
    fn new(clock: ManualClock) -> Self {
        Self {
            action: Arc::new(Mutex::new(None)),
            clock,
            triggered: Arc::new(AtomicBool::new(false)),
        }
    }

    fn set_action(&self, action: impl Fn() + Send + Sync + 'static) {
        let mut slot =
            self.action.lock().unwrap_or_else(|poison| poison.into_inner());
        *slot = Some(Box::new(action));
    }
}

impl SqsReceiveWaiter for ActionWaiter {
    fn wait(
        &self,
        duration: Duration,
        cancellation: &dyn SqsReceiveCancellation,
    ) -> SqsReceiveWaitOutcome {
        if cancellation.is_cancelled() {
            return SqsReceiveWaitOutcome::Cancelled;
        }
        if !self.triggered.swap(true, Ordering::SeqCst)
            && let Some(action) = self
                .action
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .take()
        {
            action();
        }
        self.clock.advance(duration);
        if cancellation.is_cancelled() {
            SqsReceiveWaitOutcome::Cancelled
        } else {
            SqsReceiveWaitOutcome::Completed
        }
    }
}

fn scope() -> SqsScope {
    SqsScope::new(
        "000000000000".parse::<AccountId>().expect("account should parse"),
        "eu-west-2".parse::<RegionId>().expect("region should parse"),
    )
}

fn queue_input(name: &str) -> CreateQueueInput {
    CreateQueueInput {
        attributes: BTreeMap::new(),
        queue_name: name.to_owned(),
    }
}

fn send_input(body: &str, delay_seconds: Option<u32>) -> SendMessageInput {
    SendMessageInput {
        body: body.to_owned(),
        delay_seconds,
        message_attributes: BTreeMap::new(),
        message_deduplication_id: None,
        message_group_id: None,
        message_system_attributes: BTreeMap::new(),
    }
}

fn fifo_send_input(
    body: &str,
    message_group_id: &str,
    message_deduplication_id: Option<&str>,
) -> SendMessageInput {
    SendMessageInput {
        body: body.to_owned(),
        delay_seconds: None,
        message_attributes: BTreeMap::new(),
        message_deduplication_id: message_deduplication_id.map(str::to_owned),
        message_group_id: Some(message_group_id.to_owned()),
        message_system_attributes: BTreeMap::new(),
    }
}

fn message_record(
    body: &str,
    message_id: &str,
    receive_count: u32,
    visible_at_millis: u64,
    deleted: bool,
) -> MessageRecord {
    MessageRecord {
        body: body.to_owned(),
        dead_letter_source_arn: None,
        deleted,
        deduplication_id: None,
        first_receive_timestamp_millis: None,
        issued_receipt_handles: BTreeSet::new(),
        latest_receipt_handle: None,
        md5_of_body: md5_hex(body),
        md5_of_message_attributes: None,
        message_attributes: BTreeMap::new(),
        message_group_id: None,
        message_id: message_id.to_owned(),
        message_system_attributes: BTreeMap::new(),
        receive_count,
        sent_timestamp_millis: 0,
        sequence_number: None,
        visible_at_millis,
    }
}

fn receive_input(
    max_number_of_messages: Option<u32>,
    visibility_timeout: Option<u32>,
    wait_time_seconds: Option<u32>,
) -> ReceiveMessageInput {
    ReceiveMessageInput {
        attribute_names: Vec::new(),
        max_number_of_messages,
        message_attribute_names: Vec::new(),
        message_system_attribute_names: Vec::new(),
        receive_request_attempt_id: None,
        visibility_timeout,
        wait_time_seconds,
    }
}

#[test]
fn sqs_standard_create_queue_is_idempotent_and_rejects_conflicts() {
    let clock = ManualClock::new(UNIX_EPOCH);
    let service = SqsService::with_sources(
        clock.source(),
        Arc::new(TestIdentifierSource::new(&["m-1"], &["r-1"])),
    );
    let scope = scope();
    let mut attributes = BTreeMap::new();
    attributes.insert("VisibilityTimeout".to_owned(), "45".to_owned());

    let queue = service
        .create_queue(
            &scope,
            CreateQueueInput {
                attributes: attributes.clone(),
                queue_name: "orders".to_owned(),
            },
        )
        .expect("queue should be created");
    let same_queue = service
        .create_queue(
            &scope,
            CreateQueueInput { attributes, queue_name: "orders".to_owned() },
        )
        .expect("same attributes should be idempotent");

    let conflict = service
        .create_queue(
            &scope,
            CreateQueueInput {
                attributes: BTreeMap::from([(
                    "VisibilityTimeout".to_owned(),
                    "30".to_owned(),
                )]),
                queue_name: "orders".to_owned(),
            },
        )
        .expect_err("different attributes should fail");

    assert_eq!(queue, same_queue);
    assert_eq!(
        conflict,
        SqsError::QueueAlreadyExists {
            message: "A queue already exists with the same name and a different value for attribute VisibilityTimeout".to_owned(),
        }
    );
}

#[test]
fn sqs_standard_queue_attributes_and_tags_round_trip() {
    let clock =
        ManualClock::new(UNIX_EPOCH + Duration::from_secs(1_700_000_000));
    let service = SqsService::with_sources(
        clock.source(),
        Arc::new(TestIdentifierSource::new(&["m-1"], &["r-1"])),
    );
    let scope = scope();
    let queue = service
        .create_queue(
            &scope,
            CreateQueueInput {
                attributes: BTreeMap::from([
                    ("VisibilityTimeout".to_owned(), "60".to_owned()),
                    (
                        "ReceiveMessageWaitTimeSeconds".to_owned(),
                        "5".to_owned(),
                    ),
                ]),
                queue_name: "orders".to_owned(),
            },
        )
        .expect("queue should be created");

    service
        .set_queue_attributes(
            &queue,
            BTreeMap::from([("DelaySeconds".to_owned(), "3".to_owned())]),
        )
        .expect("mutable attributes should update");
    service
        .tag_queue(
            &queue,
            BTreeMap::from([
                ("env".to_owned(), "dev".to_owned()),
                ("team".to_owned(), "platform".to_owned()),
            ]),
        )
        .expect("queue tags should update");
    service
        .untag_queue(&queue, vec!["env".to_owned()])
        .expect("queue tag should be removed");

    let attributes = service
        .get_queue_attributes(
            &queue,
            &[
                "VisibilityTimeout".to_owned(),
                "DelaySeconds".to_owned(),
                "QueueArn".to_owned(),
            ],
        )
        .expect("queue attributes should return");
    let tags =
        service.list_queue_tags(&queue).expect("queue tags should list");

    assert_eq!(attributes["VisibilityTimeout"], "60");
    assert_eq!(attributes["DelaySeconds"], "3");
    assert_eq!(
        attributes["QueueArn"],
        "arn:aws:sqs:eu-west-2:000000000000:orders"
    );
    assert_eq!(
        tags,
        BTreeMap::from([("team".to_owned(), "platform".to_owned())])
    );
}

#[test]
fn sqs_standard_send_receive_change_visibility_delete_and_purge_flow() {
    let clock = ManualClock::new(UNIX_EPOCH + Duration::from_secs(1_700));
    let service = SqsService::with_sources(
        clock.source(),
        Arc::new(TestIdentifierSource::new(
            &["msg-1", "msg-2"],
            &["handle-1", "handle-2"],
        )),
    );
    let scope = scope();
    let queue = service
        .create_queue(&scope, queue_input("orders"))
        .expect("queue should be created");

    let sent = service
        .send_message(&queue, send_input("first", None))
        .expect("message should send");
    assert_eq!(sent.message_id, "msg-1");
    assert_eq!(sent.md5_of_message_body, "8b04d5e3775d298e78455efc5ca404d5");

    let received = service
        .receive_message(
            &queue,
            ReceiveMessageInput {
                attribute_names: vec![
                    "ApproximateReceiveCount".to_owned(),
                    "SentTimestamp".to_owned(),
                ],
                ..receive_input(Some(1), None, Some(0))
            },
        )
        .expect("message should receive");
    assert_eq!(received.len(), 1);
    assert_eq!(received[0].body, "first");
    assert_eq!(received[0].receipt_handle, "handle-1");
    assert_eq!(received[0].attributes["ApproximateReceiveCount"], "1");

    let hidden = service
        .receive_message(&queue, receive_input(Some(1), None, Some(0)))
        .expect("hidden receive should succeed");
    assert!(hidden.is_empty());

    service
        .change_message_visibility(&queue, "handle-1", 45)
        .expect("visibility timeout should update");
    clock.advance(Duration::from_secs(44));
    let still_hidden = service
        .receive_message(&queue, receive_input(Some(1), None, Some(0)))
        .expect("receive should succeed while hidden");
    assert!(still_hidden.is_empty());

    service
        .delete_message(&queue, "handle-1")
        .expect("delete should remove the message");
    let deleted = service
        .receive_message(&queue, receive_input(Some(1), Some(0), Some(0)))
        .expect("deleted queue should receive empty result");
    assert!(deleted.is_empty());

    service
        .send_message(&queue, send_input("second", Some(0)))
        .expect("second message should send");
    service.purge_queue(&queue).expect("purge should succeed");
    let purged = service
        .receive_message(&queue, receive_input(Some(1), Some(0), Some(0)))
        .expect("purged queue should be empty");
    assert!(purged.is_empty());
}

#[test]
fn sqs_standard_receipt_handles_are_renewed_and_invalid_handles_fail_explicitly()
 {
    let clock = ManualClock::new(UNIX_EPOCH + Duration::from_secs(5_000));
    let service = SqsService::with_sources(
        clock.source(),
        Arc::new(TestIdentifierSource::new(
            &["msg-1"],
            &["handle-1", "handle-2"],
        )),
    );
    let scope = scope();
    let queue = service
        .create_queue(&scope, queue_input("orders"))
        .expect("queue should be created");

    service
        .send_message(&queue, send_input("payload", None))
        .expect("message should send");

    let first = service
        .receive_message(&queue, receive_input(Some(1), Some(1), Some(0)))
        .expect("first receive should succeed");
    clock.advance(Duration::from_secs(2));
    let second = service
        .receive_message(&queue, receive_input(Some(1), Some(1), Some(0)))
        .expect("second receive should succeed");

    let first_handle = first[0].receipt_handle.clone();
    let second_handle = second[0].receipt_handle.clone();

    assert_ne!(first_handle, second_handle);

    let invalid_delete = service
        .delete_message(&queue, "garbage")
        .expect_err("garbage handle should fail");
    assert_eq!(
        invalid_delete,
        SqsError::ReceiptHandleIsInvalid {
            receipt_handle: "garbage".to_owned(),
        }
    );
    let stale_delete = service
        .delete_message(&queue, &first_handle)
        .expect_err("stale handles should fail deletes");
    assert_eq!(
        stale_delete,
        SqsError::ReceiptHandleIsInvalid {
            receipt_handle: first_handle.clone(),
        }
    );
    let stale_visibility = service
        .change_message_visibility(&queue, &first_handle, 10)
        .expect_err("stale handles should fail visibility updates");
    assert_eq!(
        stale_visibility,
        SqsError::ReceiptHandleIsInvalid { receipt_handle: first_handle }
    );

    service
        .delete_message(&queue, &second_handle)
        .expect("latest handle should delete");
    service
        .delete_message(&queue, &second_handle)
        .expect("repeated delete should be a no-op");
    let invalid_visibility = service
        .change_message_visibility(&queue, &second_handle, 10)
        .expect_err("deleted message handle should fail visibility change");
    assert_eq!(
        invalid_visibility,
        SqsError::InvalidReceiptHandleForVisibility {
            receipt_handle: second_handle,
        }
    );
}

#[test]
fn sqs_standard_list_get_url_and_missing_queue_are_scoped() {
    let clock = ManualClock::new(UNIX_EPOCH);
    let service = SqsService::with_sources(
        clock.source(),
        Arc::new(TestIdentifierSource::new(&["m-1"], &["r-1"])),
    );
    let west_scope = scope();
    let east_scope = SqsScope::new(
        "000000000000".parse().expect("account should parse"),
        "us-east-1".parse().expect("region should parse"),
    );

    let west_queue = service
        .create_queue(&west_scope, queue_input("orders"))
        .expect("west queue should create");
    let east_queue = service
        .create_queue(&east_scope, queue_input("orders"))
        .expect("east queue should create");

    let west_list = service.list_queues(&west_scope, Some("ord"));
    let east_list = service.list_queues(&east_scope, Some("ord"));
    let west_lookup = service
        .get_queue_url(&west_scope, "orders")
        .expect("west queue should look up");
    let missing = service
        .get_queue_url(&west_scope, "missing")
        .expect_err("missing queue should fail");

    assert_eq!(west_list, vec![west_queue.clone()]);
    assert_eq!(east_list, vec![east_queue]);
    assert_eq!(west_lookup, west_queue);
    assert_eq!(missing, SqsError::QueueDoesNotExist);
}

#[test]
fn sqs_fifo_validation_rejects_invalid_fifo_inputs() {
    let clock = ManualClock::new(UNIX_EPOCH);
    let service = SqsService::with_sources(
        clock.source(),
        Arc::new(TestIdentifierSource::new(&["m-1"], &["r-1"])),
    );
    let scope = scope();

    let missing_suffix = service
        .create_queue(
            &scope,
            CreateQueueInput {
                attributes: BTreeMap::from([(
                    "FifoQueue".to_owned(),
                    "true".to_owned(),
                )]),
                queue_name: "orders".to_owned(),
            },
        )
        .expect_err("fifo queues must use the .fifo suffix");
    let queue = service
        .create_queue(
            &scope,
            CreateQueueInput {
                attributes: BTreeMap::from([
                    ("FifoQueue".to_owned(), "true".to_owned()),
                    (
                        "ContentBasedDeduplication".to_owned(),
                        "true".to_owned(),
                    ),
                ]),
                queue_name: "orders.fifo".to_owned(),
            },
        )
        .expect("fifo queue should be created");
    let missing_group = service
        .send_message(&queue, send_input("payload", None))
        .expect_err("fifo sends require a message group");
    let invalid_redrive = service
        .create_queue(&scope, queue_input("standard-dlq"))
        .and_then(|_| {
            service.create_queue(
                &scope,
                CreateQueueInput {
                    attributes: BTreeMap::from([
                        ("FifoQueue".to_owned(), "true".to_owned()),
                        (
                            "RedrivePolicy".to_owned(),
                            r#"{"deadLetterTargetArn":"arn:aws:sqs:eu-west-2:000000000000:standard-dlq","maxReceiveCount":1}"#
                                .to_owned(),
                        ),
                    ]),
                    queue_name: "redrive-source.fifo".to_owned(),
                },
            )
        })
        .expect_err("fifo sources cannot target standard dlqs");
    let invalid_receive = service
        .create_queue(&scope, queue_input("orders"))
        .and_then(|queue| {
            service.receive_message(
                &queue,
                receive_input(Some(11), None, Some(0)),
            )
        })
        .expect_err("invalid max message count should fail");

    assert_eq!(
        missing_suffix,
        SqsError::InvalidParameterValue {
            message:
                "The name of a FIFO queue can only include alphanumeric characters, hyphens, or underscores, must end with .fifo suffix and be 1 to 80 in length"
                    .to_owned(),
        }
    );
    assert_eq!(
        missing_group,
        SqsError::MissingParameter {
            message: "The request must contain the parameter MessageGroupId."
                .to_owned(),
        }
    );
    assert_eq!(
        invalid_redrive,
        SqsError::InvalidParameterValue {
            message:
                "Value {\"deadLetterTargetArn\":\"arn:aws:sqs:eu-west-2:000000000000:standard-dlq\",\"maxReceiveCount\":1} for parameter RedrivePolicy is invalid. Reason: dead-letter queue type must match the source queue type."
                    .to_owned(),
        }
    );
    assert_eq!(
        invalid_receive,
        SqsError::InvalidParameterValue {
            message:
                "Value 11 for parameter MaxNumberOfMessages is invalid. Reason: Must be between 1 and 10, if provided."
                    .to_owned(),
        }
    );
}

#[test]
fn sqs_fifo_deduplicates_and_preserves_group_order() {
    let clock = ManualClock::new(UNIX_EPOCH + Duration::from_secs(1_700));
    let service = SqsService::with_sources(
        clock.source(),
        Arc::new(TestIdentifierSource::new(
            &["msg-1", "msg-2", "msg-3", "msg-4"],
            &["handle-1", "handle-2", "handle-3"],
        )),
    );
    let scope = scope();
    let queue = service
        .create_queue(
            &scope,
            CreateQueueInput {
                attributes: BTreeMap::from([
                    ("FifoQueue".to_owned(), "true".to_owned()),
                    (
                        "ContentBasedDeduplication".to_owned(),
                        "true".to_owned(),
                    ),
                ]),
                queue_name: "orders.fifo".to_owned(),
            },
        )
        .expect("fifo queue should be created");

    let first = service
        .send_message(&queue, fifo_send_input("alpha", "g1", None))
        .expect("first fifo send should succeed");
    let duplicate = service
        .send_message(&queue, fifo_send_input("alpha", "g1", None))
        .expect("duplicate fifo send should be idempotent");
    let second = service
        .send_message(&queue, fifo_send_input("beta", "g1", None))
        .expect("second fifo send should succeed");
    let third = service
        .send_message(&queue, fifo_send_input("gamma", "g2", None))
        .expect("third fifo send should succeed");

    let received = service
        .receive_message(&queue, receive_input(Some(10), Some(0), Some(0)))
        .expect("fifo receive should succeed");

    assert_eq!(duplicate.message_id, first.message_id);
    assert_eq!(duplicate.sequence_number, first.sequence_number);
    assert_eq!(second.sequence_number.as_deref(), Some("2"));
    assert_eq!(third.sequence_number.as_deref(), Some("3"));
    assert_eq!(received.len(), 3);
    assert_eq!(
        received
            .iter()
            .map(|message| message.body.as_str())
            .collect::<Vec<_>>(),
        vec!["alpha", "beta", "gamma"]
    );
}

#[test]
fn sqs_fifo_batch_apis_report_partial_failures() {
    let clock = ManualClock::new(UNIX_EPOCH + Duration::from_secs(2_000));
    let service = SqsService::with_sources(
        clock.source(),
        Arc::new(TestIdentifierSource::new(
            &["msg-1", "msg-2", "msg-3"],
            &["handle-1", "handle-2", "handle-3", "handle-4"],
        )),
    );
    let scope = scope();
    let queue = service
        .create_queue(
            &scope,
            CreateQueueInput {
                attributes: BTreeMap::from([(
                    "FifoQueue".to_owned(),
                    "true".to_owned(),
                )]),
                queue_name: "batch.fifo".to_owned(),
            },
        )
        .expect("fifo queue should be created");

    let sent = service
        .send_message_batch(
            &queue,
            vec![
                SendMessageBatchEntryInput {
                    body: "alpha".to_owned(),
                    delay_seconds: None,
                    id: "first".to_owned(),
                    message_attributes: BTreeMap::new(),
                    message_deduplication_id: Some("dedup-1".to_owned()),
                    message_group_id: Some("group-1".to_owned()),
                    message_system_attributes: BTreeMap::new(),
                },
                SendMessageBatchEntryInput {
                    body: "beta".to_owned(),
                    delay_seconds: None,
                    id: "second".to_owned(),
                    message_attributes: BTreeMap::new(),
                    message_deduplication_id: Some("dedup-2".to_owned()),
                    message_group_id: None,
                    message_system_attributes: BTreeMap::new(),
                },
            ],
        )
        .expect("batch send should report per-entry results");
    assert_eq!(sent.successful.len(), 1);
    assert_eq!(sent.successful[0].id, "first");
    assert_eq!(sent.failed.len(), 1);
    assert_eq!(sent.failed[0].id, "second");
    assert_eq!(sent.failed[0].code, "MissingParameter");

    let received = service
        .receive_message(&queue, receive_input(Some(1), Some(0), Some(0)))
        .expect("message should receive");
    let receipt_handle = received[0].receipt_handle.clone();

    let deleted = service
        .delete_message_batch(
            &queue,
            vec![
                DeleteMessageBatchEntryInput {
                    id: "delete-ok".to_owned(),
                    receipt_handle,
                },
                DeleteMessageBatchEntryInput {
                    id: "delete-bad".to_owned(),
                    receipt_handle: "garbage".to_owned(),
                },
            ],
        )
        .expect("delete batch should report per-entry results");
    assert_eq!(deleted.successful.len(), 1);
    assert_eq!(deleted.failed.len(), 1);
    assert_eq!(deleted.failed[0].code, "ReceiptHandleIsInvalid");

    service
        .send_message(
            &queue,
            fifo_send_input("gamma", "group-1", Some("dedup-3")),
        )
        .expect("follow-up fifo send should succeed");
    let received = service
        .receive_message(&queue, receive_input(Some(1), Some(0), Some(0)))
        .expect("follow-up message should receive");
    let receipt_handle = received[0].receipt_handle.clone();

    let changed = service
        .change_message_visibility_batch(
            &queue,
            vec![
                ChangeMessageVisibilityBatchEntryInput {
                    id: "change-ok".to_owned(),
                    receipt_handle,
                    visibility_timeout: 30,
                },
                ChangeMessageVisibilityBatchEntryInput {
                    id: "change-bad".to_owned(),
                    receipt_handle: "garbage".to_owned(),
                    visibility_timeout: 30,
                },
            ],
        )
        .expect("visibility batch should report per-entry results");
    assert_eq!(changed.successful.len(), 1);
    assert_eq!(changed.failed.len(), 1);
    assert_eq!(changed.failed[0].code, "ReceiptHandleIsInvalid");
    assert!(
        service
            .receive_message(&queue, receive_input(Some(1), Some(0), Some(0)),)
            .expect("hidden receive should succeed")
            .is_empty()
    );
}

#[test]
fn sqs_standard_failed_automatic_redrive_keeps_source_message() {
    let clock = ManualClock::new(UNIX_EPOCH + Duration::from_secs(2_400));
    let service = SqsService::with_sources(
        clock.source(),
        Arc::new(TestIdentifierSource::new(
            &["msg-1"],
            &["handle-1", "handle-2"],
        )),
    );
    let scope = scope();
    let dlq = service
        .create_queue(&scope, queue_input("orders-dlq"))
        .expect("dlq should be created");
    let dlq_arn =
        format!("arn:aws:sqs:eu-west-2:000000000000:{}", dlq.queue_name());
    let source = service
        .create_queue(
            &scope,
            CreateQueueInput {
                attributes: BTreeMap::from([(
                    "RedrivePolicy".to_owned(),
                    format!(
                        r#"{{"deadLetterTargetArn":"{dlq_arn}","maxReceiveCount":1}}"#
                    ),
                )]),
                queue_name: "orders".to_owned(),
            },
        )
        .expect("source queue should be created");

    service
        .send_message(&source, send_input("payload", None))
        .expect("message should send");
    assert_eq!(
        service
            .receive_message(
                &source,
                receive_input(Some(1), Some(0), Some(0)),
            )
            .expect("first receive should succeed")
            .len(),
        1
    );
    service.delete_queue(&dlq).expect("deleting the DLQ should succeed");

    let error = service
        .receive_message(&source, receive_input(Some(1), Some(0), Some(0)))
        .expect_err("redrive should fail when the target queue is missing");
    assert_eq!(error, SqsError::QueueDoesNotExist);

    let state =
        service.state.lock().unwrap_or_else(|poison| poison.into_inner());
    let source_queue =
        state.queues.get(&source).expect("source queue should still exist");
    let message = source_queue
        .messages
        .iter()
        .find(|message| !message.deleted)
        .expect("source message should be retained");
    assert_eq!(message.body, "payload");
    assert_eq!(message.receive_count, 1);
}

#[test]
fn sqs_move_task_default_destination_failure_keeps_dlq_message() {
    let clock = ManualClock::new(UNIX_EPOCH + Duration::from_secs(2_450));
    let service = SqsService::with_sources(
        clock.source(),
        Arc::new(TestIdentifierSource::new(
            &["msg-1"],
            &["handle-1", "handle-2"],
        )),
    );
    let scope = scope();
    let dlq = service
        .create_queue(&scope, queue_input("orders-dlq"))
        .expect("dlq should be created");
    let dlq_arn: Arn =
        format!("arn:aws:sqs:eu-west-2:000000000000:{}", dlq.queue_name())
            .parse()
            .expect("dlq arn should parse");
    let source = service
        .create_queue(
            &scope,
            CreateQueueInput {
                attributes: BTreeMap::from([(
                    "RedrivePolicy".to_owned(),
                    format!(
                        r#"{{"deadLetterTargetArn":"{dlq_arn}","maxReceiveCount":1}}"#
                    ),
                )]),
                queue_name: "orders".to_owned(),
            },
        )
        .expect("source queue should be created");
    let _other_source = service
        .create_queue(
            &scope,
            CreateQueueInput {
                attributes: BTreeMap::from([(
                    "RedrivePolicy".to_owned(),
                    format!(
                        r#"{{"deadLetterTargetArn":"{dlq_arn}","maxReceiveCount":1}}"#
                    ),
                )]),
                queue_name: "orders-secondary".to_owned(),
            },
        )
        .expect("second source queue should be created");

    service
        .send_message(&source, send_input("payload", None))
        .expect("message should send");
    assert_eq!(
        service
            .receive_message(
                &source,
                receive_input(Some(1), Some(0), Some(0)),
            )
            .expect("first receive should succeed")
            .len(),
        1
    );
    assert!(
        service
            .receive_message(
                &source,
                receive_input(Some(1), Some(0), Some(0)),
            )
            .expect("second receive should move to the dlq")
            .is_empty()
    );
    service
        .delete_queue(&source)
        .expect("deleting the original source should succeed");

    let error = service
        .start_message_move_task(StartMessageMoveTaskInput {
            destination_arn: None,
            max_number_of_messages_per_second: None,
            source_arn: dlq_arn,
        })
        .expect_err("default destination resolution should fail");
    assert_eq!(error, SqsError::QueueDoesNotExist);

    let state =
        service.state.lock().unwrap_or_else(|poison| poison.into_inner());
    let dlq_queue = state.queues.get(&dlq).expect("dlq should still exist");
    let message = dlq_queue
        .messages
        .iter()
        .find(|message| !message.deleted)
        .expect("dlq message should be retained");
    assert_eq!(message.body, "payload");
}

#[test]
fn sqs_fifo_redrive_helpers_move_messages_from_dlq() {
    let clock = ManualClock::new(UNIX_EPOCH + Duration::from_secs(2_500));
    let service = SqsService::with_sources(
        clock.source(),
        Arc::new(TestIdentifierSource::new(
            &["msg-1", "msg-2"],
            &["handle-1", "handle-2", "handle-3", "handle-4"],
        )),
    );
    let scope = scope();
    let dlq = service
        .create_queue(
            &scope,
            CreateQueueInput {
                attributes: BTreeMap::from([
                    ("FifoQueue".to_owned(), "true".to_owned()),
                    (
                        "ContentBasedDeduplication".to_owned(),
                        "true".to_owned(),
                    ),
                ]),
                queue_name: "orders-dlq.fifo".to_owned(),
            },
        )
        .expect("dlq should be created");
    let dlq_arn: Arn =
        format!("arn:aws:sqs:eu-west-2:000000000000:{}", dlq.queue_name())
            .parse()
            .expect("DLQ ARN should parse");
    let source = service
        .create_queue(
            &scope,
            CreateQueueInput {
                attributes: BTreeMap::from([
                    ("FifoQueue".to_owned(), "true".to_owned()),
                    (
                        "ContentBasedDeduplication".to_owned(),
                        "true".to_owned(),
                    ),
                    (
                        "RedrivePolicy".to_owned(),
                        format!(
                            r#"{{"deadLetterTargetArn":"{dlq_arn}","maxReceiveCount":1}}"#
                        ),
                    ),
                ]),
                queue_name: "orders-source.fifo".to_owned(),
            },
        )
        .expect("source queue should be created");

    assert_eq!(
        service
            .list_dead_letter_source_queues(&dlq)
            .expect("dlq sources should list"),
        vec![source.clone()]
    );

    service
        .send_message(&source, fifo_send_input("payload", "group-1", None))
        .expect("fifo message should send");
    assert_eq!(
        service
            .receive_message(
                &source,
                receive_input(Some(1), Some(0), Some(0)),
            )
            .expect("first receive should succeed")
            .len(),
        1
    );
    assert!(
        service
            .receive_message(
                &source,
                receive_input(Some(1), Some(0), Some(0)),
            )
            .expect("second receive should move to the dlq")
            .is_empty()
    );

    let dlq_message = service
        .receive_message(&dlq, receive_input(Some(1), Some(0), Some(0)))
        .expect("dlq receive should succeed");
    assert_eq!(dlq_message[0].body, "payload");

    let moved = service
        .start_message_move_task(StartMessageMoveTaskInput {
            destination_arn: None,
            max_number_of_messages_per_second: None,
            source_arn: dlq_arn.clone(),
        })
        .expect("move task should succeed");
    assert_eq!(moved.task_handle, encode_move_task_handle(&dlq_arn, 2_500));
    let listed = service
        .list_message_move_tasks(ListMessageMoveTasksInput {
            max_results: Some(1),
            source_arn: dlq_arn.clone(),
        })
        .expect("move tasks should list");
    assert_eq!(listed.results.len(), 1);
    assert_eq!(listed.results[0].status, "COMPLETED");
    assert_eq!(listed.results[0].task_handle, None);

    let redriven = service
        .receive_message(&source, receive_input(Some(1), Some(0), Some(0)))
        .expect("redriven message should arrive");
    assert_eq!(redriven[0].body, "payload");
    assert_eq!(
        service
            .cancel_message_move_task(CancelMessageMoveTaskInput {
                task_handle: moved.task_handle,
            })
            .expect_err("completed move tasks cannot be cancelled"),
        SqsError::UnsupportedOperation {
            message: "Only running message move tasks can be cancelled."
                .to_owned(),
        }
    );
}

#[test]
fn sqs_fifo_receive_request_attempt_id_invalidates_after_delete() {
    let clock = ManualClock::new(UNIX_EPOCH + Duration::from_secs(3_000));
    let service = SqsService::with_sources(
        clock.source(),
        Arc::new(TestIdentifierSource::new(
            &["msg-1", "msg-2"],
            &["handle-1", "handle-2", "handle-3"],
        )),
    );
    let scope = scope();
    let queue = service
        .create_queue(
            &scope,
            CreateQueueInput {
                attributes: BTreeMap::from([
                    ("FifoQueue".to_owned(), "true".to_owned()),
                    (
                        "ContentBasedDeduplication".to_owned(),
                        "true".to_owned(),
                    ),
                ]),
                queue_name: "attempts.fifo".to_owned(),
            },
        )
        .expect("queue should be created");
    service
        .send_message(&queue, fifo_send_input("payload", "group-1", None))
        .expect("message should send");

    let mut first_input = receive_input(Some(1), Some(30), Some(0));
    first_input.receive_request_attempt_id = Some("attempt-1".to_owned());
    let first = service
        .receive_message(&queue, first_input.clone())
        .expect("first receive should succeed");
    let receipt_handle = first[0].receipt_handle.clone();
    service
        .delete_message(&queue, &receipt_handle)
        .expect("message should delete");

    let second = service
        .receive_message(&queue, first_input)
        .expect("retry after delete should succeed");
    assert!(second.is_empty());
}

#[test]
fn sqs_fifo_receive_request_attempt_id_does_not_cache_empty_long_poll() {
    let clock = ManualClock::new(UNIX_EPOCH + Duration::from_secs(3_100));
    let waiter = ActionWaiter::new(clock.clone());
    let identifier_source =
        Arc::new(TestIdentifierSource::new(&["msg-1"], &["handle-1"]));
    let service = SqsService::with_waiter_sources(
        clock.source(),
        identifier_source.clone(),
        Arc::new(waiter.clone()),
    );
    let scope = scope();
    let queue = service
        .create_queue(
            &scope,
            CreateQueueInput {
                attributes: BTreeMap::from([
                    ("FifoQueue".to_owned(), "true".to_owned()),
                    (
                        "ContentBasedDeduplication".to_owned(),
                        "true".to_owned(),
                    ),
                ]),
                queue_name: "attempt-long-poll.fifo".to_owned(),
            },
        )
        .expect("queue should be created");
    let state = service.state.clone();
    let queue_for_wait = queue.clone();

    waiter.set_action(move || {
        let now_millis = clock.source()()
            .duration_since(UNIX_EPOCH)
            .expect("clock should be after unix epoch")
            .as_millis() as u64;
        state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .send_message(
                &queue_for_wait,
                fifo_send_input("payload", "group-1", None),
                now_millis,
                identifier_source.next_message_id(),
            )
            .expect("message should be staged during the wait");
    });

    let mut input = receive_input(Some(1), Some(30), Some(1));
    input.receive_request_attempt_id = Some("attempt-1".to_owned());
    let received = service
        .receive_message(&queue, input)
        .expect("long poll should receive the arrived message");

    assert_eq!(received.len(), 1);
    assert_eq!(received[0].body, "payload");
    assert_eq!(received[0].receipt_handle, "handle-1");
}

#[test]
fn sqs_list_queue_pagination_rejects_invalid_max_results() {
    let clock = ManualClock::new(UNIX_EPOCH);
    let service = SqsService::with_sources(
        clock.source(),
        Arc::new(TestIdentifierSource::new(&["m-1"], &["r-1"])),
    );
    let scope = scope();
    let queue = service
        .create_queue(&scope, queue_input("orders"))
        .expect("queue should be created");

    let zero = service
        .list_queues_page(
            &scope,
            &ListQueuesInput {
                max_results: Some(0),
                next_token: None,
                queue_name_prefix: None,
            },
        )
        .expect_err("zero max results should fail");
    let too_large = service
        .list_queues_page(
            &scope,
            &ListQueuesInput {
                max_results: Some(1_001),
                next_token: None,
                queue_name_prefix: None,
            },
        )
        .expect_err("oversized max results should fail");
    let dlq_zero = service
        .list_dead_letter_source_queues_page(
            &queue,
            &ListDeadLetterSourceQueuesInput {
                max_results: Some(0),
                next_token: None,
            },
        )
        .expect_err("dlq zero max results should fail");

    assert_eq!(
        zero,
        SqsError::InvalidParameterValue {
            message: "Value 0 for parameter MaxResults is invalid.".to_owned(),
        }
    );
    assert_eq!(
        too_large,
        SqsError::InvalidParameterValue {
            message: "Value 1001 for parameter MaxResults is invalid."
                .to_owned(),
        }
    );
    assert_eq!(
        dlq_zero,
        SqsError::InvalidParameterValue {
            message: "Value 0 for parameter MaxResults is invalid.".to_owned(),
        }
    );
}

#[test]
fn sqs_message_attributes_reject_list_values() {
    let clock = ManualClock::new(UNIX_EPOCH);
    let service = SqsService::with_sources(
        clock.source(),
        Arc::new(TestIdentifierSource::new(&["m-1"], &["r-1"])),
    );
    let scope = scope();
    let queue = service
        .create_queue(&scope, queue_input("attributes"))
        .expect("queue should be created");

    let error = service
        .send_message(
            &queue,
            SendMessageInput {
                body: "payload".to_owned(),
                delay_seconds: None,
                message_attributes: BTreeMap::from([(
                    "example".to_owned(),
                    MessageAttributeValue {
                        binary_list_values: Vec::new(),
                        binary_value: None,
                        data_type: "String".to_owned(),
                        string_list_values: vec!["bad\u{0}value".to_owned()],
                        string_value: None,
                    },
                )]),
                message_deduplication_id: None,
                message_group_id: None,
                message_system_attributes: BTreeMap::new(),
            },
        )
        .expect_err("invalid string-list contents should fail");

    assert_eq!(
        error,
        SqsError::InvalidParameterValue {
            message:
                "StringListValues and BinaryListValues are not supported."
                    .to_owned(),
        }
    );
}

#[test]
fn sqs_add_permission_updates_queue_policy() {
    let clock = ManualClock::new(UNIX_EPOCH);
    let service = SqsService::with_sources(
        clock.source(),
        Arc::new(TestIdentifierSource::new(
            &["msg-1", "msg-2"],
            &["handle-1"],
        )),
    );
    let scope = scope();
    let queue = service
        .create_queue(&scope, queue_input("policy"))
        .expect("queue should be created");

    service
        .add_permission(
            &queue,
            super::attributes::AddPermissionInput {
                actions: vec!["SendMessage".to_owned()],
                aws_account_ids: vec!["111122223333".to_owned()],
                label: "AllowSend".to_owned(),
            },
        )
        .expect("permission should be added");
    let attributes = service
        .get_queue_attributes(&queue, &["Policy"])
        .expect("policy should fetch");
    let policy = attributes.get("Policy").expect("policy should exist");
    let policy: serde_json::Value =
        serde_json::from_str(policy).expect("policy JSON should parse");
    assert_eq!(policy["Statement"][0]["Sid"], "AllowSend");
    assert_eq!(policy["Statement"][0]["Action"][0], "SQS:SendMessage");
    assert_eq!(
        policy["Statement"][0]["Principal"]["AWS"][0],
        "arn:aws:iam::111122223333:root"
    );

    service
        .remove_permission(
            &queue,
            super::attributes::RemovePermissionInput {
                label: "AllowSend".to_owned(),
            },
        )
        .expect("permission should remove");
    let attributes = service
        .get_queue_attributes(&queue, &["Policy"])
        .expect("policy should fetch");
    let policy: serde_json::Value = serde_json::from_str(
        attributes.get("Policy").expect("policy should exist"),
    )
    .expect("policy JSON should parse");
    assert_eq!(policy["Statement"], serde_json::json!([]));
}

#[test]
fn sqs_add_permission_rejects_unsupported_action_names() {
    let clock = ManualClock::new(UNIX_EPOCH);
    let service = SqsService::with_sources(
        clock.source(),
        Arc::new(TestIdentifierSource::new(
            &["msg-1", "msg-2"],
            &["handle-1"],
        )),
    );
    let scope = scope();
    let queue = service
        .create_queue(&scope, queue_input("policy"))
        .expect("queue should be created");

    let error = service
        .add_permission(
            &queue,
            super::attributes::AddPermissionInput {
                actions: vec!["FlyMessage".to_owned()],
                aws_account_ids: vec!["111122223333".to_owned()],
                label: "AllowSend".to_owned(),
            },
        )
        .expect_err("unsupported permission actions should fail");

    assert_eq!(
        error,
        SqsError::InvalidParameterValue {
            message: "Value FlyMessage for parameter ActionName is invalid."
                .to_owned(),
        }
    );
}

#[test]
fn sqs_start_message_move_task_rejects_unsupported_throughput_without_side_effects()
 {
    let clock = ManualClock::new(UNIX_EPOCH + Duration::from_secs(2_500));
    let service = SqsService::with_sources(
        clock.source(),
        Arc::new(TestIdentifierSource::new(
            &["msg-1", "msg-2"],
            &["handle-1", "handle-2"],
        )),
    );
    let scope = scope();
    let dlq = service
        .create_queue(
            &scope,
            CreateQueueInput {
                attributes: BTreeMap::from([
                    ("FifoQueue".to_owned(), "true".to_owned()),
                    (
                        "ContentBasedDeduplication".to_owned(),
                        "true".to_owned(),
                    ),
                ]),
                queue_name: "orders-dlq.fifo".to_owned(),
            },
        )
        .expect("dlq should be created");
    let dlq_arn: Arn =
        format!("arn:aws:sqs:eu-west-2:000000000000:{}", dlq.queue_name())
            .parse()
            .expect("DLQ ARN should parse");
    let source = service
        .create_queue(
            &scope,
            CreateQueueInput {
                attributes: BTreeMap::from([
                    ("FifoQueue".to_owned(), "true".to_owned()),
                    (
                        "ContentBasedDeduplication".to_owned(),
                        "true".to_owned(),
                    ),
                    (
                        "RedrivePolicy".to_owned(),
                        format!(
                            r#"{{"deadLetterTargetArn":"{dlq_arn}","maxReceiveCount":1}}"#
                        ),
                    ),
                ]),
                queue_name: "orders-source.fifo".to_owned(),
            },
        )
        .expect("source queue should be created");
    service
        .send_message(&source, fifo_send_input("payload", "group-1", None))
        .expect("message should send");
    assert_eq!(
        service
            .receive_message(&source, receive_input(Some(1), Some(0), Some(0)))
            .expect("first receive should succeed")
            .len(),
        1
    );
    assert!(
        service
            .receive_message(&source, receive_input(Some(1), Some(0), Some(0)))
            .expect("second receive should move to dlq")
            .is_empty()
    );

    let error = service
        .start_message_move_task(StartMessageMoveTaskInput {
            destination_arn: None,
            max_number_of_messages_per_second: Some(1),
            source_arn: dlq_arn.clone(),
        })
        .expect_err("unsupported move throughput should fail");
    assert_eq!(
        error,
        SqsError::UnsupportedOperation {
            message: "StartMessageMoveTask does not support MaxNumberOfMessagesPerSecond in this Cloudish subset.".to_owned(),
        }
    );
    let listed = service
        .list_message_move_tasks(ListMessageMoveTasksInput {
            max_results: Some(10),
            source_arn: dlq_arn.clone(),
        })
        .expect("move tasks should list");
    assert!(listed.results.is_empty());
    let dlq_messages = service
        .receive_message(&dlq, receive_input(Some(1), Some(0), Some(0)))
        .expect("dlq receive should succeed");
    assert_eq!(dlq_messages.len(), 1);
    assert_eq!(dlq_messages[0].body, "payload");
}

#[test]
fn sqs_start_message_move_task_returns_resource_not_found_for_missing_source()
{
    let clock = ManualClock::new(UNIX_EPOCH + Duration::from_secs(2_500));
    let service = SqsService::with_sources(
        clock.source(),
        Arc::new(TestIdentifierSource::new(&["msg-1"], &["handle-1"])),
    );
    let source_arn: Arn = "arn:aws:sqs:eu-west-2:000000000000:missing-dlq"
        .parse()
        .expect("source arn should parse");

    let error = service
        .start_message_move_task(StartMessageMoveTaskInput {
            destination_arn: None,
            max_number_of_messages_per_second: None,
            source_arn,
        })
        .expect_err("missing move-task sources should fail");

    assert_eq!(
        error,
        SqsError::ResourceNotFound {
            message:
                "The resource that you specified for the SourceArn parameter doesn't exist."
                    .to_owned(),
        }
    );
}

#[test]
fn sqs_message_attributes_reject_reserved_prefixes() {
    let clock = ManualClock::new(UNIX_EPOCH);
    let service = SqsService::with_sources(
        clock.source(),
        Arc::new(TestIdentifierSource::new(
            &["msg-1", "msg-2"],
            &["handle-1"],
        )),
    );
    let scope = scope();
    let queue = service
        .create_queue(&scope, queue_input("reserved-attributes"))
        .expect("queue should be created");
    let attribute = MessageAttributeValue {
        binary_list_values: Vec::new(),
        binary_value: None,
        data_type: "String".to_owned(),
        string_list_values: Vec::new(),
        string_value: Some("value".to_owned()),
    };

    let aws_prefix = service
        .send_message(
            &queue,
            SendMessageInput {
                body: "payload".to_owned(),
                delay_seconds: None,
                message_attributes: BTreeMap::from([(
                    "AWS.TraceId".to_owned(),
                    attribute.clone(),
                )]),
                message_deduplication_id: None,
                message_group_id: None,
                message_system_attributes: BTreeMap::new(),
            },
        )
        .expect_err("AWS-prefixed attributes should fail");
    let amazon_prefix = service
        .send_message(
            &queue,
            SendMessageInput {
                body: "payload".to_owned(),
                delay_seconds: None,
                message_attributes: BTreeMap::from([(
                    "amazon.TraceId".to_owned(),
                    attribute,
                )]),
                message_deduplication_id: None,
                message_group_id: None,
                message_system_attributes: BTreeMap::new(),
            },
        )
        .expect_err("Amazon-prefixed attributes should fail");

    assert_eq!(
        aws_prefix,
        SqsError::InvalidParameterValue {
            message:
                "Value AWS.TraceId for parameter MessageAttributeName is invalid."
                    .to_owned(),
        }
    );
    assert_eq!(
        amazon_prefix,
        SqsError::InvalidParameterValue {
            message:
                "Value amazon.TraceId for parameter MessageAttributeName is invalid."
                    .to_owned(),
        }
    );
}

#[test]
fn sqs_message_system_attributes_require_string_trace_headers() {
    let clock = ManualClock::new(UNIX_EPOCH);
    let service = SqsService::with_sources(
        clock.source(),
        Arc::new(TestIdentifierSource::new(
            &["msg-1", "msg-2"],
            &["handle-1"],
        )),
    );
    let scope = scope();
    let queue = service
        .create_queue(&scope, queue_input("trace-headers"))
        .expect("queue should be created");

    let binary_error = service
        .send_message(
            &queue,
            SendMessageInput {
                body: "payload".to_owned(),
                delay_seconds: None,
                message_attributes: BTreeMap::new(),
                message_deduplication_id: None,
                message_group_id: None,
                message_system_attributes: BTreeMap::from([(
                    "AWSTraceHeader".to_owned(),
                    MessageAttributeValue {
                        binary_list_values: Vec::new(),
                        binary_value: Some(vec![0x01, 0x02]),
                        data_type: "Binary".to_owned(),
                        string_list_values: Vec::new(),
                        string_value: None,
                    },
                )]),
            },
        )
        .expect_err("binary trace headers should fail");
    let invalid_format = service
        .send_message(
            &queue,
            SendMessageInput {
                body: "payload".to_owned(),
                delay_seconds: None,
                message_attributes: BTreeMap::new(),
                message_deduplication_id: None,
                message_group_id: None,
                message_system_attributes: BTreeMap::from([(
                    "AWSTraceHeader".to_owned(),
                    MessageAttributeValue {
                        binary_list_values: Vec::new(),
                        binary_value: None,
                        data_type: "String".to_owned(),
                        string_list_values: Vec::new(),
                        string_value: Some("Root=1-abc".to_owned()),
                    },
                )]),
            },
        )
        .expect_err("malformed trace headers should fail");

    assert_eq!(
        binary_error,
        SqsError::InvalidParameterValue {
            message:
                "Value AWSTraceHeader for parameter MessageSystemAttributes is invalid."
                    .to_owned(),
        }
    );
    assert_eq!(
        invalid_format,
        SqsError::InvalidParameterValue {
            message:
                "Value AWSTraceHeader for parameter MessageSystemAttributes is invalid."
                    .to_owned(),
        }
    );
}

#[test]
fn sqs_message_system_attributes_do_not_count_toward_message_size() {
    let clock = ManualClock::new(UNIX_EPOCH);
    let service = SqsService::with_sources(
        clock.source(),
        Arc::new(TestIdentifierSource::new(&["msg-1"], &["handle-1"])),
    );
    let scope = scope();
    let queue = service
        .create_queue(
            &scope,
            CreateQueueInput {
                attributes: BTreeMap::from([(
                    "MaximumMessageSize".to_owned(),
                    "1024".to_owned(),
                )]),
                queue_name: "message-size".to_owned(),
            },
        )
        .expect("queue should be created");

    let result = service.send_message(
        &queue,
        SendMessageInput {
            body: "a".repeat(1024),
            delay_seconds: None,
            message_attributes: BTreeMap::new(),
            message_deduplication_id: None,
            message_group_id: None,
            message_system_attributes: BTreeMap::from([(
                "AWSTraceHeader".to_owned(),
                MessageAttributeValue {
                    binary_list_values: Vec::new(),
                    binary_value: None,
                    data_type: "String".to_owned(),
                    string_list_values: Vec::new(),
                    string_value: Some(
                        "Root=1-67891233-abcdef012345678912345678".to_owned(),
                    ),
                },
            )]),
        },
    );

    assert!(result.is_ok());
}

#[test]
fn sqs_send_message_returns_md5_of_message_system_attributes() {
    let clock = ManualClock::new(UNIX_EPOCH);
    let service = SqsService::with_sources(
        clock.source(),
        Arc::new(TestIdentifierSource::new(&["msg-1"], &["handle-1"])),
    );
    let scope = scope();
    let queue = service
        .create_queue(&scope, queue_input("trace-header-digest"))
        .expect("queue should be created");

    let sent = service
        .send_message(
            &queue,
            SendMessageInput {
                body: "payload".to_owned(),
                delay_seconds: None,
                message_attributes: BTreeMap::new(),
                message_deduplication_id: None,
                message_group_id: None,
                message_system_attributes: BTreeMap::from([(
                    "AWSTraceHeader".to_owned(),
                    MessageAttributeValue {
                        binary_list_values: Vec::new(),
                        binary_value: None,
                        data_type: "String".to_owned(),
                        string_list_values: Vec::new(),
                        string_value: Some(
                            "Root=1-67891233-abcdef012345678912345678"
                                .to_owned(),
                        ),
                    },
                )]),
            },
        )
        .expect("send should succeed");

    assert!(sent.md5_of_message_system_attributes.is_some());
}

#[test]
fn sqs_send_message_batch_returns_md5_of_message_system_attributes() {
    let clock = ManualClock::new(UNIX_EPOCH);
    let service = SqsService::with_sources(
        clock.source(),
        Arc::new(TestIdentifierSource::new(
            &["msg-1", "msg-2"],
            &["handle-1", "handle-2"],
        )),
    );
    let scope = scope();
    let queue = service
        .create_queue(&scope, queue_input("trace-header-batch-digest"))
        .expect("queue should be created");

    let sent = service
        .send_message_batch(
            &queue,
            vec![SendMessageBatchEntryInput {
                body: "payload".to_owned(),
                delay_seconds: None,
                id: "entry-1".to_owned(),
                message_attributes: BTreeMap::new(),
                message_deduplication_id: None,
                message_group_id: None,
                message_system_attributes: BTreeMap::from([(
                    "AWSTraceHeader".to_owned(),
                    MessageAttributeValue {
                        binary_list_values: Vec::new(),
                        binary_value: None,
                        data_type: "String".to_owned(),
                        string_list_values: Vec::new(),
                        string_value: Some(
                            "Root=1-67891233-abcdef012345678912345678"
                                .to_owned(),
                        ),
                    },
                )]),
            }],
        )
        .expect("batch send should succeed");

    assert_eq!(sent.successful.len(), 1);
    assert!(
        sent.successful[0]
            .md5_of_message_system_attributes
            .as_deref()
            .is_some()
    );
}

#[test]
fn sqs_message_attributes_reject_invalid_data_types_and_value_pairings() {
    let clock = ManualClock::new(UNIX_EPOCH);
    let service = SqsService::with_sources(
        clock.source(),
        Arc::new(TestIdentifierSource::new(
            &["msg-1", "msg-2", "msg-3", "msg-4", "msg-5", "msg-6"],
            &["handle-1"],
        )),
    );
    let scope = scope();
    let queue = service
        .create_queue(&scope, queue_input("attribute-types"))
        .expect("queue should be created");

    let bogus_type = service
        .send_message(
            &queue,
            SendMessageInput {
                body: "payload".to_owned(),
                delay_seconds: None,
                message_attributes: BTreeMap::from([(
                    "example".to_owned(),
                    MessageAttributeValue {
                        binary_list_values: Vec::new(),
                        binary_value: None,
                        data_type: "Bogus".to_owned(),
                        string_list_values: Vec::new(),
                        string_value: Some("value".to_owned()),
                    },
                )]),
                message_deduplication_id: None,
                message_group_id: None,
                message_system_attributes: BTreeMap::new(),
            },
        )
        .expect_err("unknown logical types should fail");
    let binary_with_string = service
        .send_message(
            &queue,
            SendMessageInput {
                body: "payload".to_owned(),
                delay_seconds: None,
                message_attributes: BTreeMap::from([(
                    "example".to_owned(),
                    MessageAttributeValue {
                        binary_list_values: Vec::new(),
                        binary_value: None,
                        data_type: "Binary".to_owned(),
                        string_list_values: Vec::new(),
                        string_value: Some("value".to_owned()),
                    },
                )]),
                message_deduplication_id: None,
                message_group_id: None,
                message_system_attributes: BTreeMap::new(),
            },
        )
        .expect_err("binary types require BinaryValue");
    let number_with_binary = service
        .send_message(
            &queue,
            SendMessageInput {
                body: "payload".to_owned(),
                delay_seconds: None,
                message_attributes: BTreeMap::from([(
                    "example".to_owned(),
                    MessageAttributeValue {
                        binary_list_values: Vec::new(),
                        binary_value: Some(vec![0x01]),
                        data_type: "Number.int".to_owned(),
                        string_list_values: Vec::new(),
                        string_value: None,
                    },
                )]),
                message_deduplication_id: None,
                message_group_id: None,
                message_system_attributes: BTreeMap::new(),
            },
        )
        .expect_err("number types require StringValue");
    let invalid_number = service
        .send_message(
            &queue,
            SendMessageInput {
                body: "payload".to_owned(),
                delay_seconds: None,
                message_attributes: BTreeMap::from([(
                    "example".to_owned(),
                    MessageAttributeValue {
                        binary_list_values: Vec::new(),
                        binary_value: None,
                        data_type: "Number".to_owned(),
                        string_list_values: Vec::new(),
                        string_value: Some("12x".to_owned()),
                    },
                )]),
                message_deduplication_id: None,
                message_group_id: None,
                message_system_attributes: BTreeMap::new(),
            },
        )
        .expect_err("invalid numeric strings should fail");
    let invalid_label = service
        .send_message(
            &queue,
            SendMessageInput {
                body: "payload".to_owned(),
                delay_seconds: None,
                message_attributes: BTreeMap::from([(
                    "example".to_owned(),
                    MessageAttributeValue {
                        binary_list_values: Vec::new(),
                        binary_value: None,
                        data_type: "String.!".to_owned(),
                        string_list_values: Vec::new(),
                        string_value: Some("value".to_owned()),
                    },
                )]),
                message_deduplication_id: None,
                message_group_id: None,
                message_system_attributes: BTreeMap::new(),
            },
        )
        .expect_err("invalid custom labels should fail");
    let invalid_label_whitespace = service
        .send_message(
            &queue,
            SendMessageInput {
                body: "payload".to_owned(),
                delay_seconds: None,
                message_attributes: BTreeMap::from([(
                    "example".to_owned(),
                    MessageAttributeValue {
                        binary_list_values: Vec::new(),
                        binary_value: None,
                        data_type: "Number.foo bar".to_owned(),
                        string_list_values: Vec::new(),
                        string_value: Some("123".to_owned()),
                    },
                )]),
                message_deduplication_id: None,
                message_group_id: None,
                message_system_attributes: BTreeMap::new(),
            },
        )
        .expect_err("whitespace in custom labels should fail");

    assert_eq!(
        bogus_type,
        SqsError::InvalidParameterValue {
            message: "Value Bogus for parameter DataType is invalid."
                .to_owned(),
        }
    );
    assert_eq!(
        binary_with_string,
        SqsError::InvalidParameterValue {
            message: "Value Binary for parameter DataType is invalid."
                .to_owned(),
        }
    );
    assert_eq!(
        number_with_binary,
        SqsError::InvalidParameterValue {
            message: "Value Number.int for parameter DataType is invalid."
                .to_owned(),
        }
    );
    assert_eq!(
        invalid_number,
        SqsError::InvalidParameterValue {
            message: "Value 12x for parameter StringValue is invalid."
                .to_owned(),
        }
    );
    assert_eq!(
        invalid_label,
        SqsError::InvalidParameterValue {
            message: "Value String.! for parameter DataType is invalid."
                .to_owned(),
        }
    );
    assert_eq!(
        invalid_label_whitespace,
        SqsError::InvalidParameterValue {
            message: "Value Number.foo bar for parameter DataType is invalid."
                .to_owned(),
        }
    );
}

#[test]
fn sqs_message_attribute_wildcards_match_only_dotted_namespaces() {
    let clock = ManualClock::new(UNIX_EPOCH);
    let service = SqsService::with_sources(
        clock.source(),
        Arc::new(TestIdentifierSource::new(&["msg-1"], &["handle-1"])),
    );
    let scope = scope();
    let queue = service
        .create_queue(&scope, queue_input("selectors"))
        .expect("queue should be created");
    let attribute = |value: &str| MessageAttributeValue {
        binary_list_values: Vec::new(),
        binary_value: None,
        data_type: "String".to_owned(),
        string_list_values: Vec::new(),
        string_value: Some(value.to_owned()),
    };
    service
        .send_message(
            &queue,
            SendMessageInput {
                body: "payload".to_owned(),
                delay_seconds: None,
                message_attributes: BTreeMap::from([
                    ("foo".to_owned(), attribute("root")),
                    ("foo.bar".to_owned(), attribute("child")),
                    ("foobar".to_owned(), attribute("sibling")),
                ]),
                message_deduplication_id: None,
                message_group_id: None,
                message_system_attributes: BTreeMap::new(),
            },
        )
        .expect("message should send");

    let received = service
        .receive_message(
            &queue,
            ReceiveMessageInput {
                message_attribute_names: vec!["foo.*".to_owned()],
                ..receive_input(Some(1), Some(0), Some(0))
            },
        )
        .expect("message should receive");
    assert_eq!(
        received[0].message_attributes,
        BTreeMap::from([("foo.bar".to_owned(), attribute("child"))])
    );
}

#[test]
fn sqs_receive_message_uses_queue_default_wait_time_when_request_omits_it() {
    let clock = ManualClock::new(UNIX_EPOCH + Duration::from_secs(3_000));
    let waiter = Arc::new(TestWaiter::new(clock.clone()));
    let service = SqsService::with_waiter_sources(
        clock.source(),
        Arc::new(TestIdentifierSource::new(&["msg-1"], &["handle-1"])),
        waiter.clone(),
    );
    let scope = scope();
    let queue = service
        .create_queue(
            &scope,
            CreateQueueInput {
                attributes: BTreeMap::from([(
                    "ReceiveMessageWaitTimeSeconds".to_owned(),
                    "2".to_owned(),
                )]),
                queue_name: "orders".to_owned(),
            },
        )
        .expect("queue should be created");

    let received = service
        .receive_message(&queue, receive_input(Some(1), None, None))
        .expect("receive should succeed");

    assert!(received.is_empty());
    assert_eq!(waiter.total_wait(), Duration::from_secs(2));
}

#[test]
fn sqs_receive_message_request_wait_time_overrides_queue_default() {
    let clock = ManualClock::new(UNIX_EPOCH + Duration::from_secs(3_100));
    let waiter = Arc::new(TestWaiter::new(clock.clone()));
    let service = SqsService::with_waiter_sources(
        clock.source(),
        Arc::new(TestIdentifierSource::new(&["msg-1"], &["handle-1"])),
        waiter.clone(),
    );
    let scope = scope();
    let queue = service
        .create_queue(
            &scope,
            CreateQueueInput {
                attributes: BTreeMap::from([(
                    "ReceiveMessageWaitTimeSeconds".to_owned(),
                    "5".to_owned(),
                )]),
                queue_name: "orders".to_owned(),
            },
        )
        .expect("queue should be created");

    let received = service
        .receive_message(&queue, receive_input(Some(1), None, Some(1)))
        .expect("receive should succeed");

    assert!(received.is_empty());
    assert_eq!(waiter.total_wait(), Duration::from_secs(1));
}

#[test]
fn sqs_receive_message_cancellation_is_scoped_to_the_current_receive() {
    let service = SqsService::new();
    let request_runtime = SqsRequestRuntime::new(service.clone());
    let scope = scope();
    let queue = service
        .create_queue(&scope, queue_input("orders"))
        .expect("queue should be created");
    let receive_queue = queue.clone();
    let cancelled = Arc::new(AtomicBool::new(false));
    let receive_cancelled = Arc::clone(&cancelled);
    let receive = std::thread::spawn(move || {
        let started_at = Instant::now();
        let is_cancelled = move || receive_cancelled.load(Ordering::SeqCst);
        let received = request_runtime
            .receive_message(
                &receive_queue,
                receive_input(Some(1), None, Some(5)),
                &is_cancelled,
            )
            .expect("receive should succeed");

        (started_at.elapsed(), received)
    });

    std::thread::sleep(Duration::from_millis(100));
    cancelled.store(true, Ordering::SeqCst);

    let (elapsed, received) = receive.join().expect("receive should finish");

    assert!(received.is_empty());
    assert!(elapsed < Duration::from_secs(1));

    let started_at = Instant::now();
    let received = service
        .receive_message(&queue, receive_input(Some(1), None, Some(1)))
        .expect("ordinary receive should still succeed");

    assert!(received.is_empty());
    assert!(started_at.elapsed() >= Duration::from_millis(900));
}

#[test]
fn sqs_receive_message_returns_messages_arriving_during_wait_window() {
    let service = SqsService::new();
    let scope = scope();
    let queue = service
        .create_queue(
            &scope,
            CreateQueueInput {
                attributes: BTreeMap::from([(
                    "ReceiveMessageWaitTimeSeconds".to_owned(),
                    "1".to_owned(),
                )]),
                queue_name: "orders".to_owned(),
            },
        )
        .expect("queue should be created");
    let sender = service.clone();
    let delayed_queue = queue.clone();
    let delayed_send = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(50));
        sender
            .send_message(&delayed_queue, send_input("payload", Some(0)))
            .expect("message should send");
    });

    let received = service
        .receive_message(&queue, receive_input(Some(1), None, None))
        .expect("receive should succeed");
    delayed_send.join().expect("delayed sender should finish");

    assert_eq!(received.len(), 1);
    assert_eq!(received[0].body, "payload");
}

fn queue_identities() -> (SqsQueueIdentity, SqsQueueIdentity) {
    let identity = SqsQueueIdentity::new(
        "000000000000".parse().expect("account should parse"),
        "eu-west-2".parse().expect("region should parse"),
        "orders",
    )
    .expect("queue identity should build");
    let fifo_identity = SqsQueueIdentity::new(
        "000000000000".parse().expect("account should parse"),
        "eu-west-2".parse().expect("region should parse"),
        "orders.fifo",
    )
    .expect("fifo queue identity should build");

    (identity, fifo_identity)
}

#[test]
fn sqs_standard_internal_attribute_validation_helpers_cover_remaining_edges() {
    let (identity, fifo_identity) = queue_identities();

    assert_eq!(
        normalize_requested_attribute_names(&[] as &[&str])
            .expect("empty requests should normalize"),
        Vec::<&str>::new()
    );
    assert_eq!(
        normalize_requested_attribute_names(&["All".to_owned()])
            .expect("All should normalize"),
        Vec::<&str>::new()
    );
    assert_eq!(
        normalize_requested_attribute_names(&["Bogus".to_owned()])
            .expect_err("unknown attributes should fail"),
        SqsError::InvalidAttributeName { attribute_name: "Bogus".to_owned() }
    );

    let existing = QueueRecord::new(identity.clone(), BTreeMap::new(), 0);
    ensure_create_attributes_are_compatible(
        &existing,
        &BTreeMap::from([("DelaySeconds".to_owned(), "0".to_owned())]),
    )
    .expect("extra attributes should remain compatible");

    assert_eq!(
        normalize_queue_attributes(
            &identity,
            BTreeMap::from([("DelaySeconds".to_owned(), "".to_owned())]),
            AttributeMode::Create,
        )
        .expect_err("blank attribute values should fail"),
        SqsError::InvalidParameterValue {
            message: "Value for attribute DelaySeconds must not be empty."
                .to_owned(),
        }
    );
    assert_eq!(
        normalize_queue_attributes(
            &identity,
            BTreeMap::from([("QueueArn".to_owned(), "value".to_owned())]),
            AttributeMode::Create,
        )
        .expect_err("computed attributes should fail"),
        SqsError::InvalidAttributeName {
            attribute_name: "QueueArn".to_owned(),
        }
    );
    assert_eq!(
        normalize_queue_attributes(
            &identity,
            BTreeMap::from([("Custom".to_owned(), "value".to_owned())]),
            AttributeMode::Create,
        )
        .expect_err("unknown create attributes should fail"),
        SqsError::InvalidAttributeName { attribute_name: "Custom".to_owned() }
    );
    assert_eq!(
        normalize_queue_attributes(
            &identity,
            BTreeMap::from([("Custom".to_owned(), "value".to_owned())]),
            AttributeMode::Set,
        )
        .expect_err("unknown mutable attributes should fail"),
        SqsError::InvalidAttributeName { attribute_name: "Custom".to_owned() }
    );
    assert_eq!(
        normalize_queue_attributes(
            &identity,
            BTreeMap::from([(
                "MaximumMessageSize".to_owned(),
                "0".to_owned()
            )]),
            AttributeMode::Create,
        )
        .expect_err("zero positive attributes should fail"),
        SqsError::InvalidParameterValue {
            message: "Value 0 for attribute MaximumMessageSize is invalid."
                .to_owned(),
        }
    );
    assert_eq!(
        normalize_queue_attributes(
            &identity,
            BTreeMap::from([("DelaySeconds".to_owned(), "bad".to_owned())]),
            AttributeMode::Create,
        )
        .expect_err("non-numeric attributes should fail"),
        SqsError::InvalidParameterValue {
            message: "Value bad for attribute DelaySeconds is invalid."
                .to_owned(),
        }
    );
    assert_eq!(
        normalize_queue_attributes(
            &identity,
            BTreeMap::from([("DelaySeconds".to_owned(), "901".to_owned())]),
            AttributeMode::Create,
        )
        .expect_err("out-of-range numeric attributes should fail"),
        SqsError::InvalidParameterValue {
            message: "Value 901 for attribute DelaySeconds is invalid."
                .to_owned(),
        }
    );
    assert_eq!(
        normalize_queue_attributes(
            &fifo_identity,
            BTreeMap::from([("FifoQueue".to_owned(), "true".to_owned())]),
            AttributeMode::Create,
        )
        .expect("fifo queues should normalize"),
        BTreeMap::from([
            ("ContentBasedDeduplication".to_owned(), "false".to_owned()),
            ("DelaySeconds".to_owned(), "0".to_owned()),
            ("FifoQueue".to_owned(), "true".to_owned()),
            ("MaximumMessageSize".to_owned(), "262144".to_owned()),
            ("MessageRetentionPeriod".to_owned(), "345600".to_owned()),
            ("ReceiveMessageWaitTimeSeconds".to_owned(), "0".to_owned()),
            ("VisibilityTimeout".to_owned(), "30".to_owned()),
        ])
    );
    assert_eq!(
        normalize_queue_attributes(
            &identity,
            BTreeMap::from([(
                "ContentBasedDeduplication".to_owned(),
                "true".to_owned(),
            )]),
            AttributeMode::Create,
        )
        .expect_err("standard queues cannot use fifo dedup settings"),
        SqsError::InvalidParameterValue {
            message:
                "ContentBasedDeduplication is valid only for FIFO queues."
                    .to_owned(),
        }
    );
}

#[test]
fn sqs_standard_internal_request_validation_helpers_cover_remaining_edges() {
    let (_, fifo_identity) = queue_identities();

    assert_eq!(
        validate_queue_name("").expect_err("blank queue names should fail"),
        SqsError::InvalidParameterValue {
            message: "Queue name must not be empty.".to_owned(),
        }
    );
    assert_eq!(
        validate_queue_name(&"a".repeat(81))
            .expect_err("long queue names should fail"),
        SqsError::InvalidParameterValue {
            message: "Queue name must be 80 characters or fewer.".to_owned(),
        }
    );
    assert_eq!(
        validate_queue_name("bad/name")
            .expect_err("invalid queue characters should fail"),
        SqsError::InvalidParameterValue {
            message: "Queue name contains invalid character '/'.".to_owned(),
        }
    );
    assert_eq!(
        validate_queue_name("bad.name")
            .expect_err("only .fifo suffix may use dots"),
        SqsError::InvalidParameterValue {
            message: "Queue name contains invalid character '.'.".to_owned(),
        }
    );
    assert_eq!(
        parse_boolean_attribute("FifoQueue", "banana")
            .expect_err("invalid booleans should fail"),
        SqsError::InvalidParameterValue {
            message: "Value banana for attribute FifoQueue is invalid."
                .to_owned(),
        }
    );
    assert_eq!(
        validate_fifo_identifier(" ", "MessageGroupId")
            .expect_err("invalid fifo ids should fail"),
        SqsError::InvalidParameterValue {
            message: "Value   for parameter MessageGroupId is invalid."
                .to_owned(),
        }
    );
    assert_eq!(
        parse_redrive_policy("{}")
            .expect_err("redrive policy must include a target"),
        SqsError::InvalidParameterValue {
            message: "The required parameter 'deadLetterTargetArn' is missing"
                .to_owned(),
        }
    );
    assert_eq!(
        validate_batch_request::<SendMessageBatchEntryInput>(&[])
            .expect_err("empty batches should fail"),
        SqsError::EmptyBatchRequest {
            message: "Batch requests must contain at least one entry."
                .to_owned(),
        }
    );
    assert_eq!(
        validate_batch_request(&[
            SendMessageBatchEntryInput {
                body: "a".to_owned(),
                delay_seconds: None,
                id: "dup".to_owned(),
                message_attributes: BTreeMap::new(),
                message_deduplication_id: None,
                message_group_id: None,
                message_system_attributes: BTreeMap::new(),
            },
            SendMessageBatchEntryInput {
                body: "b".to_owned(),
                delay_seconds: None,
                id: "dup".to_owned(),
                message_attributes: BTreeMap::new(),
                message_deduplication_id: None,
                message_group_id: None,
                message_system_attributes: BTreeMap::new(),
            },
        ])
        .expect_err("duplicate batch ids should fail"),
        SqsError::BatchEntryIdsNotDistinct {
            message:
                "Two or more batch entries in the request have the same Id."
                    .to_owned(),
        }
    );
    assert_eq!(
        validate_batch_entry_id("bad id")
            .expect_err("invalid batch ids should fail"),
        SqsError::InvalidBatchEntryId {
            message: "A batch entry id can only contain alphanumeric characters, hyphens and underscores. It can be at most 80 letters long.".to_owned(),
        }
    );
    assert_eq!(
        parse_queue_identity_from_arn(
            &"arn:aws:sqs:eu-west-2:000000000000:orders.fifo"
                .parse()
                .expect("queue ARN should parse"),
            "SourceArn",
        )
        .expect("queue arn should parse"),
        fifo_identity
    );
    assert_eq!(
        validate_positive_attribute("MaximumMessageSize", "bad")
            .expect_err("non-numeric positive attributes should fail"),
        SqsError::InvalidParameterValue {
            message: "Value bad for attribute MaximumMessageSize is invalid."
                .to_owned(),
        }
    );
    assert_eq!(
        validate_numeric_attribute("VisibilityTimeout", "bad", 43_200)
            .expect_err("non-numeric attributes should fail"),
        SqsError::InvalidParameterValue {
            message: "Value bad for attribute VisibilityTimeout is invalid."
                .to_owned(),
        }
    );
    assert_eq!(
        validate_delay_seconds(901)
            .expect_err("delay bounds should be enforced"),
        SqsError::InvalidParameterValue {
            message:
                "Value 901 for parameter DelaySeconds is invalid. Reason: Must be <= 900."
                    .to_owned(),
        }
    );
    assert_eq!(
        validate_max_number_of_messages(0)
            .expect_err("receive max bounds should be enforced"),
        SqsError::InvalidParameterValue {
            message:
                "Value 0 for parameter MaxNumberOfMessages is invalid. Reason: Must be between 1 and 10, if provided."
                    .to_owned(),
        }
    );
    assert_eq!(
        validate_wait_time_seconds(21)
            .expect_err("wait time bounds should be enforced"),
        SqsError::InvalidParameterValue {
            message:
                "Value 21 for parameter WaitTimeSeconds is invalid. Reason: Must be >= 0 and <= 20, if provided."
                    .to_owned(),
        }
    );
    assert_eq!(
        validate_visibility_timeout(43_201)
            .expect_err("visibility bounds should be enforced"),
        SqsError::InvalidParameterValue {
            message:
                "Value 43201 for parameter VisibilityTimeout is invalid. Reason: Must be >= 0 and <= 43200, if provided."
                    .to_owned(),
        }
    );
}

#[test]
fn sqs_standard_internal_error_mapping_covers_remaining_edges() {
    let invalid_attribute =
        SqsError::InvalidAttributeName { attribute_name: "Bogus".to_owned() }
            .to_aws_error();
    assert_eq!(invalid_attribute.code(), "InvalidAttributeName");
    assert_eq!(invalid_attribute.message(), "Unknown Attribute Bogus.");
    assert_eq!(invalid_attribute.status_code(), 400);

    let invalid_parameter = SqsError::InvalidParameterValue {
        message: "bad parameter".to_owned(),
    }
    .to_aws_error();
    assert_eq!(invalid_parameter.code(), "InvalidParameterValue");
    assert_eq!(invalid_parameter.message(), "bad parameter");

    let missing_parameter =
        SqsError::MissingParameter { message: "missing parameter".to_owned() }
            .to_aws_error();
    assert_eq!(missing_parameter.code(), "MissingParameter");
    assert_eq!(missing_parameter.message(), "missing parameter");

    let queue_exists =
        SqsError::QueueAlreadyExists { message: "queue exists".to_owned() }
            .to_aws_error();
    assert_eq!(queue_exists.code(), "QueueAlreadyExists");
    assert_eq!(queue_exists.message(), "queue exists");

    let queue_missing = SqsError::QueueDoesNotExist.to_aws_error();
    assert_eq!(
        queue_missing.code(),
        "AWS.SimpleQueueService.NonExistentQueue"
    );
    assert_eq!(queue_missing.message(), "The specified queue does not exist.");

    let invalid_receipt = SqsError::ReceiptHandleIsInvalid {
        receipt_handle: "garbage".to_owned(),
    }
    .to_aws_error();
    assert_eq!(invalid_receipt.code(), "ReceiptHandleIsInvalid");
    assert!(invalid_receipt.message().contains("garbage"));

    let invalid_visibility = SqsError::InvalidReceiptHandleForVisibility {
        receipt_handle: "garbage".to_owned(),
    }
    .to_aws_error();
    assert_eq!(invalid_visibility.code(), "InvalidParameterValue");
    assert!(invalid_visibility.message().contains("garbage"));

    let unsupported =
        SqsError::UnsupportedOperation { message: "unsupported".to_owned() }
            .to_aws_error();
    assert_eq!(
        unsupported.code(),
        "AWS.SimpleQueueService.UnsupportedOperation"
    );
    assert_eq!(unsupported.message(), "unsupported");

    let batch_ids_not_distinct = SqsError::BatchEntryIdsNotDistinct {
        message: "duplicate ids".to_owned(),
    }
    .to_aws_error();
    assert_eq!(
        batch_ids_not_distinct.code(),
        "AWS.SimpleQueueService.BatchEntryIdsNotDistinct"
    );

    let empty_batch =
        SqsError::EmptyBatchRequest { message: "empty".to_owned() }
            .to_aws_error();
    assert_eq!(empty_batch.code(), "AWS.SimpleQueueService.EmptyBatchRequest");

    let invalid_batch_id =
        SqsError::InvalidBatchEntryId { message: "invalid".to_owned() }
            .to_aws_error();
    assert_eq!(
        invalid_batch_id.code(),
        "AWS.SimpleQueueService.InvalidBatchEntryId"
    );

    let resource_not_found =
        SqsError::ResourceNotFound { message: "missing".to_owned() }
            .to_aws_error();
    assert_eq!(resource_not_found.code(), "ResourceNotFoundException");
}

#[test]
fn sqs_standard_internal_queue_metrics_and_wrapper_defaults_cover_remaining_edges()
 {
    let default_service = SqsService::default();
    let missing_queue = SqsQueueIdentity::new(
        "000000000000".parse().expect("account should parse"),
        "eu-west-2".parse().expect("region should parse"),
        "missing",
    )
    .expect("missing queue identity should build");
    assert_eq!(
        default_service
            .delete_queue(&missing_queue)
            .expect_err("missing queues should fail deletes"),
        SqsError::QueueDoesNotExist
    );

    let identity = SqsQueueIdentity::new(
        "000000000000".parse().expect("account should parse"),
        "eu-west-2".parse().expect("region should parse"),
        "counts",
    )
    .expect("queue identity should build");
    let mut queue_record = QueueRecord::new(identity, BTreeMap::new(), 5);
    queue_record
        .attributes
        .insert("ReceiveMessageWaitTimeSeconds".to_owned(), "bad".to_owned());
    queue_record
        .attributes
        .insert("VisibilityTimeout".to_owned(), "bad".to_owned());
    queue_record.messages = vec![
        message_record("visible", "m-visible", 0, 4_000, false),
        message_record("delayed", "m-delayed", 0, 6_000, false),
        message_record("in-flight", "m-in-flight", 1, 7_000, false),
        message_record("deleted", "m-deleted", 0, 4_000, true),
    ]
    .into();
    assert_eq!(queue_record.message_counts(5), (1, 1, 1));
    assert_eq!(queue_record.receive_wait_time(), 0);
    assert_eq!(queue_record.visibility_timeout(), 30);

    let clock = ManualClock::new(UNIX_EPOCH + Duration::from_secs(10));
    let service = SqsService::with_sources(
        clock.source(),
        Arc::new(TestIdentifierSource::new(
            &["m-1", "m-2"],
            &["handle-1", "handle-2"],
        )),
    );
    let scope = scope();
    let queue = service
        .create_queue(&scope, queue_input("orders"))
        .expect("queue should be created");
    service
        .send_message(&queue, send_input("first", Some(0)))
        .expect("first message should send");
    service
        .send_message(&queue, send_input("second", Some(0)))
        .expect("second message should send");

    let received = service
        .receive_message(&queue, receive_input(Some(1), Some(1), Some(0)))
        .expect("receive should succeed");
    assert_eq!(received.len(), 1);
    let all_attributes = service
        .get_queue_attributes(&queue, &[] as &[&str])
        .expect("all queue attributes should return");
    let filtered_attributes = service
        .get_queue_attributes(
            &queue,
            &["ApproximateNumberOfMessagesNotVisible"],
        )
        .expect("filtered queue attributes should return");
    assert_eq!(all_attributes["ApproximateNumberOfMessages"], "1");
    assert_eq!(all_attributes["ApproximateNumberOfMessagesNotVisible"], "1");
    assert_eq!(
        filtered_attributes["ApproximateNumberOfMessagesNotVisible"],
        "1"
    );
    assert_eq!(
        service
            .change_message_visibility(&queue, "garbage", 0)
            .expect_err("unknown receipt handles should fail"),
        SqsError::ReceiptHandleIsInvalid {
            receipt_handle: "garbage".to_owned(),
        }
    );

    service.delete_queue(&queue).expect("queue deletes should succeed");
    assert_eq!(
        service
            .get_queue_attributes(&queue, &[] as &[&str])
            .expect_err("deleted queues should not resolve"),
        SqsError::QueueDoesNotExist
    );
}
