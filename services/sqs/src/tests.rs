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
    MessageRecord, ReceiveMessageInput, SendMessageBatchEntryInput,
    SendMessageInput, md5_hex, validate_batch_entry_id,
    validate_batch_request, validate_delay_seconds,
    validate_max_number_of_messages, validate_visibility_timeout,
    validate_wait_time_seconds,
};
use super::queues::{
    CreateQueueInput, QueueRecord, SqsIdentifierSource, SqsService,
};
use super::redrive::{
    StartMessageMoveTaskInput, encode_move_task_handle,
    parse_queue_identity_from_arn, parse_redrive_policy,
};
use super::scope::{SqsQueueIdentity, SqsScope};
use aws::{AccountId, Arn, RegionId};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

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
        message_deduplication_id: None,
        message_group_id: None,
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
        message_deduplication_id: message_deduplication_id.map(str::to_owned),
        message_group_id: Some(message_group_id.to_owned()),
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
        issued_receipt_handles: BTreeSet::new(),
        latest_receipt_handle: None,
        md5_of_body: md5_hex(body),
        message_group_id: None,
        message_id: message_id.to_owned(),
        receive_count,
        sent_timestamp_millis: 0,
        sequence_number: None,
        visible_at_millis,
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
                max_number_of_messages: Some(1),
                visibility_timeout: None,
                wait_time_seconds: Some(0),
            },
        )
        .expect("message should receive");
    assert_eq!(received.len(), 1);
    assert_eq!(received[0].body, "first");
    assert_eq!(received[0].receipt_handle, "handle-1");
    assert_eq!(received[0].attributes["ApproximateReceiveCount"], "1");

    let hidden = service
        .receive_message(
            &queue,
            ReceiveMessageInput {
                max_number_of_messages: Some(1),
                visibility_timeout: None,
                wait_time_seconds: Some(0),
            },
        )
        .expect("hidden receive should succeed");
    assert!(hidden.is_empty());

    service
        .change_message_visibility(&queue, "handle-1", 45)
        .expect("visibility timeout should update");
    clock.advance(Duration::from_secs(44));
    let still_hidden = service
        .receive_message(
            &queue,
            ReceiveMessageInput {
                max_number_of_messages: Some(1),
                visibility_timeout: None,
                wait_time_seconds: Some(0),
            },
        )
        .expect("receive should succeed while hidden");
    assert!(still_hidden.is_empty());

    service
        .delete_message(&queue, "handle-1")
        .expect("delete should remove the message");
    let deleted = service
        .receive_message(
            &queue,
            ReceiveMessageInput {
                max_number_of_messages: Some(1),
                visibility_timeout: Some(0),
                wait_time_seconds: Some(0),
            },
        )
        .expect("deleted queue should receive empty result");
    assert!(deleted.is_empty());

    service
        .send_message(&queue, send_input("second", Some(0)))
        .expect("second message should send");
    service.purge_queue(&queue).expect("purge should succeed");
    let purged = service
        .receive_message(
            &queue,
            ReceiveMessageInput {
                max_number_of_messages: Some(1),
                visibility_timeout: Some(0),
                wait_time_seconds: Some(0),
            },
        )
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
        .receive_message(
            &queue,
            ReceiveMessageInput {
                max_number_of_messages: Some(1),
                visibility_timeout: Some(1),
                wait_time_seconds: Some(0),
            },
        )
        .expect("first receive should succeed");
    clock.advance(Duration::from_secs(2));
    let second = service
        .receive_message(
            &queue,
            ReceiveMessageInput {
                max_number_of_messages: Some(1),
                visibility_timeout: Some(1),
                wait_time_seconds: Some(0),
            },
        )
        .expect("second receive should succeed");

    assert_ne!(first[0].receipt_handle, second[0].receipt_handle);

    let invalid_delete = service
        .delete_message(&queue, "garbage")
        .expect_err("garbage handle should fail");
    assert_eq!(
        invalid_delete,
        SqsError::ReceiptHandleIsInvalid {
            receipt_handle: "garbage".to_owned(),
        }
    );

    service
        .delete_message(&queue, "handle-2")
        .expect("latest handle should delete");
    service
        .delete_message(&queue, "handle-2")
        .expect("repeated delete should be a no-op");
    let invalid_visibility = service
        .change_message_visibility(&queue, "handle-2", 10)
        .expect_err("deleted message handle should fail visibility change");
    assert_eq!(
        invalid_visibility,
        SqsError::InvalidReceiptHandleForVisibility {
            receipt_handle: "handle-2".to_owned(),
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
                ReceiveMessageInput {
                    max_number_of_messages: Some(11),
                    visibility_timeout: None,
                    wait_time_seconds: Some(0),
                },
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
        .receive_message(
            &queue,
            ReceiveMessageInput {
                max_number_of_messages: Some(10),
                visibility_timeout: Some(0),
                wait_time_seconds: Some(0),
            },
        )
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
                    message_deduplication_id: Some("dedup-1".to_owned()),
                    message_group_id: Some("group-1".to_owned()),
                },
                SendMessageBatchEntryInput {
                    body: "beta".to_owned(),
                    delay_seconds: None,
                    id: "second".to_owned(),
                    message_deduplication_id: Some("dedup-2".to_owned()),
                    message_group_id: None,
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
        .receive_message(
            &queue,
            ReceiveMessageInput {
                max_number_of_messages: Some(1),
                visibility_timeout: Some(0),
                wait_time_seconds: Some(0),
            },
        )
        .expect("message should receive");
    let receipt_handle = received[0].receipt_handle.clone();

    let deleted = service
        .delete_message_batch(
            &queue,
            vec![
                DeleteMessageBatchEntryInput {
                    id: "delete-ok".to_owned(),
                    receipt_handle: receipt_handle.clone(),
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
        .receive_message(
            &queue,
            ReceiveMessageInput {
                max_number_of_messages: Some(1),
                visibility_timeout: Some(0),
                wait_time_seconds: Some(0),
            },
        )
        .expect("follow-up message should receive");
    let receipt_handle = received[0].receipt_handle.clone();

    let changed = service
        .change_message_visibility_batch(
            &queue,
            vec![
                ChangeMessageVisibilityBatchEntryInput {
                    id: "change-ok".to_owned(),
                    receipt_handle: receipt_handle.clone(),
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
            .receive_message(
                &queue,
                ReceiveMessageInput {
                    max_number_of_messages: Some(1),
                    visibility_timeout: Some(0),
                    wait_time_seconds: Some(0),
                },
            )
            .expect("hidden receive should succeed")
            .is_empty()
    );
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
                ReceiveMessageInput {
                    max_number_of_messages: Some(1),
                    visibility_timeout: Some(0),
                    wait_time_seconds: Some(0),
                },
            )
            .expect("first receive should succeed")
            .len(),
        1
    );
    assert!(
        service
            .receive_message(
                &source,
                ReceiveMessageInput {
                    max_number_of_messages: Some(1),
                    visibility_timeout: Some(0),
                    wait_time_seconds: Some(0),
                },
            )
            .expect("second receive should move to the dlq")
            .is_empty()
    );

    let dlq_message = service
        .receive_message(
            &dlq,
            ReceiveMessageInput {
                max_number_of_messages: Some(1),
                visibility_timeout: Some(0),
                wait_time_seconds: Some(0),
            },
        )
        .expect("dlq receive should succeed");
    assert_eq!(dlq_message[0].body, "payload");

    let moved = service
        .start_message_move_task(StartMessageMoveTaskInput {
            destination_arn: None,
            max_number_of_messages_per_second: None,
            source_arn: dlq_arn.clone(),
        })
        .expect("move task should succeed");
    assert_eq!(moved.task_handle, encode_move_task_handle(&dlq_arn));

    let redriven = service
        .receive_message(
            &source,
            ReceiveMessageInput {
                max_number_of_messages: Some(1),
                visibility_timeout: Some(0),
                wait_time_seconds: Some(0),
            },
        )
        .expect("redriven message should arrive");
    assert_eq!(redriven[0].body, "payload");
    assert_eq!(
        service
            .start_message_move_task(StartMessageMoveTaskInput {
                destination_arn: None,
                max_number_of_messages_per_second: Some(1),
                source_arn: dlq_arn,
            })
            .expect_err("unsupported move throughput should fail"),
        SqsError::UnsupportedOperation {
            message: "StartMessageMoveTask does not support MaxNumberOfMessagesPerSecond in this Cloudish subset.".to_owned(),
        }
    );
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
        normalize_requested_attribute_names(&[])
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
                message_deduplication_id: None,
                message_group_id: None,
            },
            SendMessageBatchEntryInput {
                body: "b".to_owned(),
                delay_seconds: None,
                id: "dup".to_owned(),
                message_deduplication_id: None,
                message_group_id: None,
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
    assert_eq!(unsupported.code(), "UnsupportedOperation");
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
        .receive_message(
            &queue,
            ReceiveMessageInput {
                max_number_of_messages: Some(1),
                visibility_timeout: Some(1),
                wait_time_seconds: Some(0),
            },
        )
        .expect("receive should succeed");
    assert_eq!(received.len(), 1);
    let all_attributes = service
        .get_queue_attributes(&queue, &[])
        .expect("all queue attributes should return");
    let filtered_attributes = service
        .get_queue_attributes(
            &queue,
            &["ApproximateNumberOfMessagesNotVisible".to_owned()],
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
            .get_queue_attributes(&queue, &[])
            .expect_err("deleted queues should not resolve"),
        SqsError::QueueDoesNotExist
    );
}
