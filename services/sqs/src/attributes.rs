use crate::errors::SqsError;
use crate::messages::timestamp_seconds;
use crate::queues::{QueueRecord, SqsService, SqsWorld};
use crate::redrive::{parse_redrive_policy, validate_redrive_policy_target};
use crate::scope::SqsQueueIdentity;
use serde_json::{Map, Value, json};
use std::collections::BTreeMap;

pub(crate) const DEFAULT_DELAY_SECONDS: u32 = 0;
pub(crate) const DEFAULT_MAXIMUM_MESSAGE_SIZE: u32 = 262_144;
pub(crate) const DEFAULT_MESSAGE_RETENTION_PERIOD: u32 = 345_600;
pub(crate) const DEFAULT_RECEIVE_WAIT_TIME_SECONDS: u32 = 0;
pub(crate) const DEFAULT_VISIBILITY_TIMEOUT: u32 = 30;
pub(crate) const MAX_DELAY_SECONDS: u32 = 900;
pub(crate) const MAX_MAXIMUM_MESSAGE_SIZE: u32 = 262_144;
pub(crate) const MAX_MESSAGE_RETENTION_PERIOD: u32 = 1_209_600;
pub(crate) const MIN_MAXIMUM_MESSAGE_SIZE: u32 = 1_024;
pub(crate) const MIN_MESSAGE_RETENTION_PERIOD: u32 = 60;
pub(crate) const MAX_RECEIVE_WAIT_TIME_SECONDS: u32 = 20;
pub(crate) const MAX_VISIBILITY_TIMEOUT: u32 = 43_200;

const COMPUTED_ATTRIBUTES: &[&str] = &[
    "ApproximateNumberOfMessages",
    "ApproximateNumberOfMessagesDelayed",
    "ApproximateNumberOfMessagesNotVisible",
    "CreatedTimestamp",
    "LastModifiedTimestamp",
    "QueueArn",
];
const MUTABLE_ATTRIBUTES: &[&str] = &[
    "ContentBasedDeduplication",
    "DelaySeconds",
    "MaximumMessageSize",
    "MessageRetentionPeriod",
    "Policy",
    "ReceiveMessageWaitTimeSeconds",
    "RedrivePolicy",
    "VisibilityTimeout",
];
const SUPPORTED_ATTRIBUTES: &[&str] = &[
    "ApproximateNumberOfMessages",
    "ApproximateNumberOfMessagesDelayed",
    "ApproximateNumberOfMessagesNotVisible",
    "ContentBasedDeduplication",
    "CreatedTimestamp",
    "DelaySeconds",
    "FifoQueue",
    "LastModifiedTimestamp",
    "MaximumMessageSize",
    "MessageRetentionPeriod",
    "Policy",
    "QueueArn",
    "ReceiveMessageWaitTimeSeconds",
    "RedrivePolicy",
    "VisibilityTimeout",
];
const UNSUPPORTED_CREATE_ATTRIBUTES: &[&str] = &[
    "DeduplicationScope",
    "FifoThroughputLimit",
    "KmsDataKeyReusePeriodSeconds",
    "KmsMasterKeyId",
    "RedriveAllowPolicy",
    "SqsManagedSseEnabled",
];
const SUPPORTED_PERMISSION_ACTIONS: &[&str] = &[
    "*",
    "AddPermission",
    "CancelMessageMoveTask",
    "ChangeMessageVisibility",
    "ChangeMessageVisibilityBatch",
    "CreateQueue",
    "DeleteMessage",
    "DeleteMessageBatch",
    "DeleteQueue",
    "GetQueueAttributes",
    "GetQueueUrl",
    "ListDeadLetterSourceQueues",
    "ListMessageMoveTasks",
    "ListQueueTags",
    "ListQueues",
    "PurgeQueue",
    "ReceiveMessage",
    "RemovePermission",
    "SendMessage",
    "SendMessageBatch",
    "SetQueueAttributes",
    "StartMessageMoveTask",
    "TagQueue",
    "UntagQueue",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AttributeMode {
    Create,
    Set,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AddPermissionInput {
    pub actions: Vec<String>,
    pub aws_account_ids: Vec<String>,
    pub label: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemovePermissionInput {
    pub label: String,
}

impl SqsService {
    /// Returns queue attributes for the requested names, or all supported attributes.
    ///
    /// # Errors
    ///
    /// Returns [`SqsError`] when the queue does not exist or an attribute name is unsupported.
    pub fn get_queue_attributes(
        &self,
        queue: &SqsQueueIdentity,
        attribute_names: &[impl AsRef<str>],
    ) -> Result<BTreeMap<String, String>, SqsError> {
        let state =
            self.state.lock().unwrap_or_else(|poison| poison.into_inner());

        state.get_queue_attributes(
            queue,
            attribute_names,
            timestamp_seconds((self.time_source)()),
        )
    }

    /// Updates the mutable attributes stored for a queue.
    ///
    /// # Errors
    ///
    /// Returns [`SqsError`] when the queue does not exist, an attribute is invalid,
    /// or the redrive policy points at an invalid target queue.
    pub fn set_queue_attributes(
        &self,
        queue: &SqsQueueIdentity,
        attributes: BTreeMap<String, String>,
    ) -> Result<(), SqsError> {
        let mut state =
            self.state.lock().unwrap_or_else(|poison| poison.into_inner());

        state.set_queue_attributes(
            queue,
            attributes,
            timestamp_seconds((self.time_source)()),
        )
    }

    /// Adds or replaces tags on a queue.
    ///
    /// # Errors
    ///
    /// Returns [`SqsError`] when the queue does not exist.
    pub fn tag_queue(
        &self,
        queue: &SqsQueueIdentity,
        tags: BTreeMap<String, String>,
    ) -> Result<(), SqsError> {
        let mut state =
            self.state.lock().unwrap_or_else(|poison| poison.into_inner());

        state.tag_queue(queue, tags, timestamp_seconds((self.time_source)()))
    }

    /// Removes tags from a queue by key.
    ///
    /// # Errors
    ///
    /// Returns [`SqsError`] when the queue does not exist.
    pub fn untag_queue(
        &self,
        queue: &SqsQueueIdentity,
        tag_keys: Vec<String>,
    ) -> Result<(), SqsError> {
        let mut state =
            self.state.lock().unwrap_or_else(|poison| poison.into_inner());

        state.untag_queue(
            queue,
            tag_keys,
            timestamp_seconds((self.time_source)()),
        )
    }

    /// Returns the tags currently attached to a queue.
    ///
    /// # Errors
    ///
    /// Returns [`SqsError`] when the queue does not exist.
    pub fn list_queue_tags(
        &self,
        queue: &SqsQueueIdentity,
    ) -> Result<BTreeMap<String, String>, SqsError> {
        let state =
            self.state.lock().unwrap_or_else(|poison| poison.into_inner());

        state.list_queue_tags(queue)
    }

    /// Adds a permission statement to the queue policy.
    ///
    /// # Errors
    ///
    /// Returns [`SqsError`] when the queue does not exist, the input is invalid,
    /// or the existing policy document is not valid JSON.
    pub fn add_permission(
        &self,
        queue: &SqsQueueIdentity,
        input: AddPermissionInput,
    ) -> Result<(), SqsError> {
        let mut state =
            self.state.lock().unwrap_or_else(|poison| poison.into_inner());

        state.add_permission(
            queue,
            input,
            timestamp_seconds((self.time_source)()),
        )
    }

    /// Removes a permission statement from the queue policy by label.
    ///
    /// # Errors
    ///
    /// Returns [`SqsError`] when the queue does not exist or the label is invalid.
    pub fn remove_permission(
        &self,
        queue: &SqsQueueIdentity,
        input: RemovePermissionInput,
    ) -> Result<(), SqsError> {
        let mut state =
            self.state.lock().unwrap_or_else(|poison| poison.into_inner());

        state.remove_permission(
            queue,
            input,
            timestamp_seconds((self.time_source)()),
        )
    }
}

impl SqsWorld {
    pub(crate) fn get_queue_attributes(
        &self,
        queue: &SqsQueueIdentity,
        attribute_names: &[impl AsRef<str>],
        now_seconds: u64,
    ) -> Result<BTreeMap<String, String>, SqsError> {
        let queue =
            self.queues.get(queue).ok_or(SqsError::QueueDoesNotExist)?;
        let requested = normalize_requested_attribute_names(attribute_names)?;
        let all_attributes = queue.attributes(now_seconds)?;

        if requested.is_empty() {
            return Ok(all_attributes);
        }

        Ok(requested
            .into_iter()
            .filter_map(|name| {
                all_attributes
                    .get(name)
                    .cloned()
                    .map(|value| (name.to_owned(), value))
            })
            .collect())
    }

    pub(crate) fn set_queue_attributes(
        &mut self,
        queue: &SqsQueueIdentity,
        attributes: BTreeMap<String, String>,
        now_seconds: u64,
    ) -> Result<(), SqsError> {
        let existing =
            self.queues.get(queue).ok_or(SqsError::QueueDoesNotExist)?;
        let normalized = normalize_queue_attributes(
            &existing.identity,
            attributes,
            AttributeMode::Set,
        )?;
        let mut merged = existing.attributes.clone();
        for (name, value) in &normalized {
            merged.insert(name.clone(), value.clone());
        }
        validate_redrive_policy_target(self, &existing.identity, &merged)?;

        let Some(queue_record) = self.queues.get_mut(queue) else {
            return Err(SqsError::QueueDoesNotExist);
        };
        for (name, value) in normalized {
            queue_record.attributes.insert(name, value);
        }
        queue_record.last_modified_timestamp = now_seconds;

        Ok(())
    }

    pub(crate) fn tag_queue(
        &mut self,
        queue: &SqsQueueIdentity,
        tags: BTreeMap<String, String>,
        now_seconds: u64,
    ) -> Result<(), SqsError> {
        let queue =
            self.queues.get_mut(queue).ok_or(SqsError::QueueDoesNotExist)?;

        for (key, value) in tags {
            queue.tags.insert(key, value);
        }
        queue.last_modified_timestamp = now_seconds;

        Ok(())
    }

    pub(crate) fn untag_queue(
        &mut self,
        queue: &SqsQueueIdentity,
        tag_keys: Vec<String>,
        now_seconds: u64,
    ) -> Result<(), SqsError> {
        let queue =
            self.queues.get_mut(queue).ok_or(SqsError::QueueDoesNotExist)?;

        for key in tag_keys {
            queue.tags.remove(&key);
        }
        queue.last_modified_timestamp = now_seconds;

        Ok(())
    }

    pub(crate) fn list_queue_tags(
        &self,
        queue: &SqsQueueIdentity,
    ) -> Result<BTreeMap<String, String>, SqsError> {
        Ok(self
            .queues
            .get(queue)
            .ok_or(SqsError::QueueDoesNotExist)?
            .tags
            .clone())
    }

    pub(crate) fn add_permission(
        &mut self,
        queue: &SqsQueueIdentity,
        input: AddPermissionInput,
        now_seconds: u64,
    ) -> Result<(), SqsError> {
        validate_add_permission_input(&input)?;
        let queue_record =
            self.queues.get_mut(queue).ok_or(SqsError::QueueDoesNotExist)?;
        let mut document = queue_record.policy_document()?;
        let statement = permission_statement(queue_record, &input);
        let statements = document
            .entry("Statement".to_owned())
            .or_insert_with(|| Value::Array(Vec::new()));
        let Some(statements) = statements.as_array_mut() else {
            return Err(invalid_policy_document());
        };

        statements.retain(|statement| {
            statement.get("Sid") != Some(&json!(input.label))
        });
        statements.push(statement);
        queue_record.set_policy_document(document, now_seconds)?;

        Ok(())
    }

    pub(crate) fn remove_permission(
        &mut self,
        queue: &SqsQueueIdentity,
        input: RemovePermissionInput,
        now_seconds: u64,
    ) -> Result<(), SqsError> {
        validate_permission_label(&input.label)?;
        let queue_record =
            self.queues.get_mut(queue).ok_or(SqsError::QueueDoesNotExist)?;
        let mut document = queue_record.policy_document()?;
        let statements = document
            .entry("Statement".to_owned())
            .or_insert_with(|| Value::Array(Vec::new()));
        let Some(statements) = statements.as_array_mut() else {
            return Err(invalid_policy_document());
        };
        statements.retain(|statement| {
            statement.get("Sid") != Some(&json!(input.label))
        });
        queue_record.set_policy_document(document, now_seconds)?;

        Ok(())
    }
}

impl QueueRecord {
    pub(crate) fn attributes(
        &self,
        now_seconds: u64,
    ) -> Result<BTreeMap<String, String>, SqsError> {
        let mut attributes = self.attributes.clone();
        let (visible, delayed, in_flight) = self.message_counts(now_seconds);
        attributes.insert(
            "ApproximateNumberOfMessages".to_owned(),
            visible.to_string(),
        );
        attributes.insert(
            "ApproximateNumberOfMessagesDelayed".to_owned(),
            delayed.to_string(),
        );
        attributes.insert(
            "ApproximateNumberOfMessagesNotVisible".to_owned(),
            in_flight.to_string(),
        );
        attributes.insert(
            "CreatedTimestamp".to_owned(),
            self.created_timestamp.to_string(),
        );
        attributes.insert(
            "LastModifiedTimestamp".to_owned(),
            self.last_modified_timestamp.to_string(),
        );
        attributes.insert("QueueArn".to_owned(), self.queue_arn());

        Ok(attributes)
    }

    pub(crate) fn message_counts(
        &self,
        now_seconds: u64,
    ) -> (usize, usize, usize) {
        let now_millis = now_seconds.saturating_mul(1_000);
        let blocked_groups = self.blocked_fifo_groups(now_millis);
        let mut visible = 0;
        let mut delayed = 0;
        let mut in_flight = 0;

        for message in &self.messages {
            if message.deleted {
                continue;
            }

            if message.visible_at_millis > now_millis {
                if message.receive_count == 0 {
                    delayed += 1;
                } else {
                    in_flight += 1;
                }
                continue;
            }

            if self.is_fifo()
                && message
                    .message_group_id
                    .as_ref()
                    .is_some_and(|group| blocked_groups.contains(group))
            {
                in_flight += 1;
            } else {
                visible += 1;
            }
        }

        (visible, delayed, in_flight)
    }

    pub(crate) fn queue_arn(&self) -> String {
        format!(
            "arn:aws:sqs:{}:{}:{}",
            self.identity.region(),
            self.identity.account_id(),
            self.identity.queue_name()
        )
    }

    pub(crate) fn receive_wait_time(&self) -> u32 {
        self.attributes
            .get("ReceiveMessageWaitTimeSeconds")
            .and_then(|value| value.parse().ok())
            .unwrap_or(DEFAULT_RECEIVE_WAIT_TIME_SECONDS)
    }

    pub(crate) fn visibility_timeout(&self) -> u32 {
        self.attributes
            .get("VisibilityTimeout")
            .and_then(|value| value.parse().ok())
            .unwrap_or(DEFAULT_VISIBILITY_TIMEOUT)
    }

    pub(crate) fn queue_delay_seconds(&self) -> u32 {
        self.attributes
            .get("DelaySeconds")
            .and_then(|value| value.parse().ok())
            .unwrap_or(DEFAULT_DELAY_SECONDS)
    }

    pub(crate) fn maximum_message_size(&self) -> u32 {
        self.attributes
            .get("MaximumMessageSize")
            .and_then(|value| value.parse().ok())
            .unwrap_or(DEFAULT_MAXIMUM_MESSAGE_SIZE)
    }

    pub(crate) fn policy_document(
        &self,
    ) -> Result<Map<String, Value>, SqsError> {
        match self.attributes.get("Policy") {
            Some(policy) => serde_json::from_str(policy)
                .map_err(|_| invalid_policy_document()),
            None => Ok(Map::from_iter([
                ("Version".to_owned(), Value::String("2012-10-17".to_owned())),
                ("Statement".to_owned(), Value::Array(Vec::new())),
            ])),
        }
    }

    pub(crate) fn set_policy_document(
        &mut self,
        document: Map<String, Value>,
        now_seconds: u64,
    ) -> Result<(), SqsError> {
        let policy =
            serde_json::to_string(&Value::Object(document)).map_err(|_| {
                SqsError::InvalidParameterValue {
                    message: "Value for attribute Policy must be valid JSON."
                        .to_owned(),
                }
            })?;
        self.attributes.insert("Policy".to_owned(), policy);
        self.last_modified_timestamp = now_seconds;

        Ok(())
    }
}

fn validate_add_permission_input(
    input: &AddPermissionInput,
) -> Result<(), SqsError> {
    validate_permission_label(&input.label)?;
    if input.aws_account_ids.is_empty() || input.actions.is_empty() {
        return Err(SqsError::InvalidParameterValue {
            message: "AddPermission requires at least one AWS account ID and one action.".to_owned(),
        });
    }
    for account_id in &input.aws_account_ids {
        if account_id.parse::<u64>().is_err() || account_id.len() != 12 {
            return Err(SqsError::InvalidParameterValue {
                message: format!(
                    "Value {account_id} for parameter AWSAccountId is invalid."
                ),
            });
        }
    }
    for action in &input.actions {
        if action.trim().is_empty() {
            return Err(SqsError::InvalidParameterValue {
                message: "ActionName entries must not be empty.".to_owned(),
            });
        }
        if !SUPPORTED_PERMISSION_ACTIONS.contains(&action.as_str()) {
            return Err(SqsError::InvalidParameterValue {
                message: format!(
                    "Value {action} for parameter ActionName is invalid."
                ),
            });
        }
    }

    Ok(())
}

fn validate_permission_label(label: &str) -> Result<(), SqsError> {
    if label.is_empty()
        || label.len() > 80
        || !label.chars().all(|character| {
            character.is_ascii_alphanumeric() || matches!(character, '-' | '_')
        })
    {
        return Err(SqsError::InvalidParameterValue {
            message: format!("Value {label} for parameter Label is invalid."),
        });
    }

    Ok(())
}

fn permission_statement(
    queue: &QueueRecord,
    input: &AddPermissionInput,
) -> Value {
    let actions = input
        .actions
        .iter()
        .map(|action| Value::String(permission_action(action)))
        .collect::<Vec<_>>();
    let principals = input
        .aws_account_ids
        .iter()
        .map(|account_id| {
            Value::String(format!("arn:aws:iam::{account_id}:root"))
        })
        .collect::<Vec<_>>();

    json!({
        "Sid": input.label,
        "Effect": "Allow",
        "Principal": { "AWS": principals },
        "Action": actions,
        "Resource": queue.queue_arn(),
    })
}

fn permission_action(action: &str) -> String {
    let action = action.strip_prefix("SQS:").unwrap_or(action);
    format!("SQS:{action}")
}

fn invalid_policy_document() -> SqsError {
    SqsError::InvalidParameterValue {
        message: "Value for attribute Policy must be valid JSON.".to_owned(),
    }
}

pub(crate) fn ensure_create_attributes_are_compatible(
    existing: &QueueRecord,
    requested: &BTreeMap<String, String>,
) -> Result<(), SqsError> {
    for (name, value) in requested {
        let Some(existing_value) = existing.attributes.get(name) else {
            continue;
        };

        if existing_value != value {
            return Err(SqsError::QueueAlreadyExists {
                message: format!(
                    "A queue already exists with the same name and a different value for attribute {name}"
                ),
            });
        }
    }

    Ok(())
}

pub(crate) fn normalize_requested_attribute_names(
    attribute_names: &[impl AsRef<str>],
) -> Result<Vec<&str>, SqsError> {
    if attribute_names.is_empty()
        || attribute_names.iter().any(|name| name.as_ref() == "All")
    {
        return Ok(Vec::new());
    }

    let mut requested = Vec::new();
    for name in attribute_names {
        let name = name.as_ref();
        if !SUPPORTED_ATTRIBUTES.contains(&name) {
            return Err(SqsError::InvalidAttributeName {
                attribute_name: name.to_owned(),
            });
        }
        requested.push(name);
    }

    Ok(requested)
}

pub(crate) fn normalize_queue_attributes(
    identity: &SqsQueueIdentity,
    attributes: BTreeMap<String, String>,
    mode: AttributeMode,
) -> Result<BTreeMap<String, String>, SqsError> {
    let mut normalized = BTreeMap::new();
    let explicit_fifo_queue = attributes
        .get("FifoQueue")
        .map(|value| parse_boolean_attribute("FifoQueue", value))
        .transpose()?;
    let queue_name_is_fifo = identity.queue_name().ends_with(".fifo");
    let is_fifo_queue = match mode {
        AttributeMode::Create => explicit_fifo_queue.unwrap_or(false),
        AttributeMode::Set => queue_name_is_fifo,
    };

    for (name, value) in attributes {
        if value.trim().is_empty() {
            return Err(SqsError::InvalidParameterValue {
                message: format!(
                    "Value for attribute {name} must not be empty."
                ),
            });
        }

        if UNSUPPORTED_CREATE_ATTRIBUTES.contains(&name.as_str()) {
            return Err(SqsError::InvalidParameterValue {
                message: format!(
                    "Attribute {name} is not supported by this SQS story."
                ),
            });
        }

        if COMPUTED_ATTRIBUTES.contains(&name.as_str()) {
            return Err(SqsError::InvalidAttributeName {
                attribute_name: name,
            });
        }

        if mode == AttributeMode::Set && name == "FifoQueue" {
            return Err(SqsError::InvalidParameterValue {
                message:
                    "Invalid value for the parameter FifoQueue. Reason: Modifying queue type is not supported."
                        .to_owned(),
            });
        }

        if mode == AttributeMode::Set
            && !MUTABLE_ATTRIBUTES.contains(&name.as_str())
        {
            return Err(SqsError::InvalidAttributeName {
                attribute_name: name,
            });
        }

        if mode == AttributeMode::Create
            && name != "FifoQueue"
            && !MUTABLE_ATTRIBUTES.contains(&name.as_str())
        {
            return Err(SqsError::InvalidAttributeName {
                attribute_name: name,
            });
        }

        let normalized_value = match name.as_str() {
            "ContentBasedDeduplication" => {
                if !is_fifo_queue {
                    return Err(SqsError::InvalidParameterValue {
                        message: "ContentBasedDeduplication is valid only for FIFO queues.".to_owned(),
                    });
                }
                normalize_boolean_attribute(&name, &value)?
            }
            "DelaySeconds" => {
                validate_numeric_attribute(&name, &value, MAX_DELAY_SECONDS)?
            }
            "FifoQueue" => {
                let normalized_fifo =
                    normalize_boolean_attribute(&name, &value)?;
                if normalized_fifo != "true" {
                    return Err(SqsError::InvalidParameterValue {
                        message: "Invalid value for the parameter FifoQueue. Reason: FifoQueue must be true for FIFO queues.".to_owned(),
                    });
                }
                normalized_fifo
            }
            "MaximumMessageSize" => validate_attribute_range(
                &name,
                &value,
                MIN_MAXIMUM_MESSAGE_SIZE,
                MAX_MAXIMUM_MESSAGE_SIZE,
            )?,
            "MessageRetentionPeriod" => validate_attribute_range(
                &name,
                &value,
                MIN_MESSAGE_RETENTION_PERIOD,
                MAX_MESSAGE_RETENTION_PERIOD,
            )?,
            "Policy" => value,
            "ReceiveMessageWaitTimeSeconds" => validate_numeric_attribute(
                &name,
                &value,
                MAX_RECEIVE_WAIT_TIME_SECONDS,
            )?,
            "RedrivePolicy" => {
                parse_redrive_policy(&value)?;
                value
            }
            "VisibilityTimeout" => validate_numeric_attribute(
                &name,
                &value,
                MAX_VISIBILITY_TIMEOUT,
            )?,
            _ => {
                return Err(SqsError::InvalidAttributeName {
                    attribute_name: name,
                });
            }
        };
        normalized.insert(name, normalized_value);
    }

    if is_fifo_queue && !queue_name_is_fifo {
        return Err(SqsError::InvalidParameterValue {
            message:
                "The name of a FIFO queue can only include alphanumeric characters, hyphens, or underscores, must end with .fifo suffix and be 1 to 80 in length"
                    .to_owned(),
        });
    }
    if queue_name_is_fifo && !is_fifo_queue {
        return Err(SqsError::InvalidParameterValue {
            message:
                "Can only include alphanumeric characters, hyphens, or underscores. 1 to 80 in length."
                    .to_owned(),
        });
    }

    if mode == AttributeMode::Create {
        normalized
            .entry("DelaySeconds".to_owned())
            .or_insert_with(|| DEFAULT_DELAY_SECONDS.to_string());
        normalized
            .entry("MaximumMessageSize".to_owned())
            .or_insert_with(|| DEFAULT_MAXIMUM_MESSAGE_SIZE.to_string());
        normalized
            .entry("MessageRetentionPeriod".to_owned())
            .or_insert_with(|| DEFAULT_MESSAGE_RETENTION_PERIOD.to_string());
        normalized
            .entry("ReceiveMessageWaitTimeSeconds".to_owned())
            .or_insert_with(|| DEFAULT_RECEIVE_WAIT_TIME_SECONDS.to_string());
        normalized
            .entry("VisibilityTimeout".to_owned())
            .or_insert_with(|| DEFAULT_VISIBILITY_TIMEOUT.to_string());
        if is_fifo_queue {
            normalized
                .entry("FifoQueue".to_owned())
                .or_insert_with(|| "true".to_owned());
            normalized
                .entry("ContentBasedDeduplication".to_owned())
                .or_insert_with(|| "false".to_owned());
        }
    }

    Ok(normalized)
}

pub(crate) fn validate_queue_name(queue_name: &str) -> Result<(), SqsError> {
    if queue_name.is_empty() {
        return Err(SqsError::InvalidParameterValue {
            message: "Queue name must not be empty.".to_owned(),
        });
    }
    if queue_name.len() > 80 {
        return Err(SqsError::InvalidParameterValue {
            message: "Queue name must be 80 characters or fewer.".to_owned(),
        });
    }
    let base_name = queue_name.strip_suffix(".fifo").unwrap_or(queue_name);
    if let Some(character) = base_name.chars().find(|character| {
        !character.is_ascii_alphanumeric()
            && *character != '-'
            && *character != '_'
    }) {
        return Err(SqsError::InvalidParameterValue {
            message: format!(
                "Queue name contains invalid character {character:?}."
            ),
        });
    }
    if queue_name.contains('.') && !queue_name.ends_with(".fifo") {
        return Err(SqsError::InvalidParameterValue {
            message: "Queue name contains invalid character '.'.".to_owned(),
        });
    }

    Ok(())
}

pub(crate) fn parse_boolean_attribute(
    name: &str,
    value: &str,
) -> Result<bool, SqsError> {
    match value {
        "true" | "True" | "TRUE" => Ok(true),
        "false" | "False" | "FALSE" => Ok(false),
        _ => Err(SqsError::InvalidParameterValue {
            message: format!("Value {value} for attribute {name} is invalid."),
        }),
    }
}

pub(crate) fn normalize_boolean_attribute(
    name: &str,
    value: &str,
) -> Result<String, SqsError> {
    Ok(parse_boolean_attribute(name, value)?.to_string())
}

#[cfg(test)]
pub(crate) fn validate_positive_attribute(
    name: &str,
    value: &str,
) -> Result<String, SqsError> {
    validate_attribute_range(name, value, 1, u32::MAX)
}

pub(crate) fn validate_attribute_range(
    name: &str,
    value: &str,
    min_value: u32,
    max_value: u32,
) -> Result<String, SqsError> {
    let parsed =
        value.parse::<u32>().map_err(|_| SqsError::InvalidParameterValue {
            message: format!("Value {value} for attribute {name} is invalid."),
        })?;
    if !(min_value..=max_value).contains(&parsed) {
        return Err(SqsError::InvalidParameterValue {
            message: format!("Value {value} for attribute {name} is invalid."),
        });
    }

    Ok(parsed.to_string())
}

pub(crate) fn validate_numeric_attribute(
    name: &str,
    value: &str,
    max_value: u32,
) -> Result<String, SqsError> {
    let parsed =
        value.parse::<u32>().map_err(|_| SqsError::InvalidParameterValue {
            message: format!("Value {value} for attribute {name} is invalid."),
        })?;
    if parsed > max_value {
        return Err(SqsError::InvalidParameterValue {
            message: format!("Value {value} for attribute {name} is invalid."),
        });
    }

    Ok(parsed.to_string())
}
