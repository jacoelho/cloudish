use crate::{errors::CloudWatchLogsError, scope::CloudWatchScope};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use storage::StorageHandle;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateLogGroupInput {
    pub log_group_name: String,
    pub tags: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeLogGroupsInput {
    pub log_group_name_prefix: Option<String>,
    pub next_token: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogGroupDescription {
    pub arn: String,
    pub creation_time_millis: u64,
    pub log_group_arn: String,
    pub log_group_name: String,
    pub metric_filter_count: i32,
    pub retention_in_days: Option<i32>,
    pub stored_bytes: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeLogGroupsOutput {
    pub log_groups: Vec<LogGroupDescription>,
    pub next_token: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateLogStreamInput {
    pub log_group_name: String,
    pub log_stream_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeLogStreamsInput {
    pub log_group_name: String,
    pub log_stream_name_prefix: Option<String>,
    pub next_token: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogStreamDescription {
    pub arn: String,
    pub creation_time_millis: u64,
    pub first_event_timestamp_millis: Option<u64>,
    pub last_event_timestamp_millis: Option<u64>,
    pub last_ingestion_time_millis: Option<u64>,
    pub log_stream_name: String,
    pub stored_bytes: i64,
    pub upload_sequence_token: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeLogStreamsOutput {
    pub log_streams: Vec<LogStreamDescription>,
    pub next_token: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InputLogEvent {
    pub message: String,
    pub timestamp_millis: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PutLogEventsInput {
    pub log_events: Vec<InputLogEvent>,
    pub log_group_name: String,
    pub log_stream_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PutLogEventsOutput {
    pub next_sequence_token: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetLogEventsInput {
    pub end_time_millis: Option<u64>,
    pub limit: Option<u32>,
    pub log_group_name: String,
    pub log_stream_name: String,
    pub next_token: Option<String>,
    pub start_from_head: bool,
    pub start_time_millis: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutputLogEvent {
    pub ingestion_time_millis: u64,
    pub message: String,
    pub timestamp_millis: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetLogEventsOutput {
    pub events: Vec<OutputLogEvent>,
    pub next_backward_token: String,
    pub next_forward_token: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FilterLogEventsInput {
    pub end_time_millis: Option<u64>,
    pub filter_pattern: Option<String>,
    pub limit: Option<u32>,
    pub log_group_name: String,
    pub log_stream_name_prefix: Option<String>,
    pub log_stream_names: Vec<String>,
    pub next_token: Option<String>,
    pub start_time_millis: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FilteredLogEvent {
    pub event_id: String,
    pub ingestion_time_millis: u64,
    pub log_stream_name: String,
    pub message: String,
    pub timestamp_millis: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchedLogStream {
    pub log_stream_name: String,
    pub searched_completely: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FilterLogEventsOutput {
    pub events: Vec<FilteredLogEvent>,
    pub next_token: Option<String>,
    pub searched_log_streams: Vec<SearchedLogStream>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PutRetentionPolicyInput {
    pub log_group_name: String,
    pub retention_in_days: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteRetentionPolicyInput {
    pub log_group_name: String,
}

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
pub(crate) struct LogGroupStorageKey {
    pub(crate) account_id: aws::AccountId,
    pub(crate) name: String,
    pub(crate) region: aws::RegionId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct StoredLogGroup {
    pub(crate) creation_time_millis: u64,
    pub(crate) retention_in_days: Option<i32>,
    pub(crate) tags: BTreeMap<String, String>,
}

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
pub(crate) struct LogStreamStorageKey {
    pub(crate) account_id: aws::AccountId,
    pub(crate) log_group_name: String,
    pub(crate) log_stream_name: String,
    pub(crate) region: aws::RegionId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct StoredLogEvent {
    pub(crate) ingestion_time_millis: u64,
    pub(crate) message: String,
    pub(crate) timestamp_millis: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct StoredLogStream {
    pub(crate) creation_time_millis: u64,
    pub(crate) events: Vec<StoredLogEvent>,
}

pub(crate) fn log_group_key(
    scope: &CloudWatchScope,
    name: &str,
) -> LogGroupStorageKey {
    LogGroupStorageKey {
        account_id: scope.account_id().clone(),
        name: name.to_owned(),
        region: scope.region().clone(),
    }
}

pub(crate) fn log_stream_key(
    scope: &CloudWatchScope,
    log_group_name: &str,
    log_stream_name: &str,
) -> LogStreamStorageKey {
    LogStreamStorageKey {
        account_id: scope.account_id().clone(),
        log_group_name: log_group_name.to_owned(),
        log_stream_name: log_stream_name.to_owned(),
        region: scope.region().clone(),
    }
}

pub(crate) fn ensure_log_group_exists(
    store: &StorageHandle<LogGroupStorageKey, StoredLogGroup>,
    key: &LogGroupStorageKey,
    name: &str,
) -> Result<StoredLogGroup, CloudWatchLogsError> {
    store.get(key).ok_or_else(|| {
        CloudWatchLogsError::ResourceNotFoundException {
            message: format!("The specified log group {name} does not exist."),
        }
    })
}

pub(crate) fn ensure_log_stream_exists(
    store: &StorageHandle<LogStreamStorageKey, StoredLogStream>,
    key: &LogStreamStorageKey,
    log_group_name: &str,
    log_stream_name: &str,
) -> Result<StoredLogStream, CloudWatchLogsError> {
    store.get(key).ok_or_else(|| CloudWatchLogsError::ResourceNotFoundException {
        message: format!(
            "The specified log stream {log_stream_name} does not exist in log \
             group {log_group_name}."
        ),
    })
}

pub(crate) fn build_log_group_description(
    scope: &CloudWatchScope,
    name: &str,
    stored: StoredLogGroup,
) -> LogGroupDescription {
    LogGroupDescription {
        arn: format_log_group_arn(scope, name, true),
        creation_time_millis: stored.creation_time_millis,
        log_group_arn: format_log_group_arn(scope, name, false),
        log_group_name: name.to_owned(),
        metric_filter_count: 0,
        retention_in_days: stored.retention_in_days,
        stored_bytes: 0,
    }
}

pub(crate) fn build_log_stream_description(
    scope: &CloudWatchScope,
    log_group_name: &str,
    log_stream_name: &str,
    stored: StoredLogStream,
) -> LogStreamDescription {
    let first_event_timestamp_millis =
        stored.events.first().map(|event| event.timestamp_millis);
    let last_event_timestamp_millis =
        stored.events.last().map(|event| event.timestamp_millis);
    let last_ingestion_time_millis =
        stored.events.last().map(|event| event.ingestion_time_millis);

    LogStreamDescription {
        arn: format_log_stream_arn(scope, log_group_name, log_stream_name),
        creation_time_millis: stored.creation_time_millis,
        first_event_timestamp_millis,
        last_event_timestamp_millis,
        last_ingestion_time_millis,
        log_stream_name: log_stream_name.to_owned(),
        stored_bytes: 0,
        upload_sequence_token: Some(stored.events.len().to_string()),
    }
}

pub(crate) fn validate_log_name(
    resource: &str,
    value: &str,
) -> Result<String, CloudWatchLogsError> {
    let value = value.trim();
    if value.is_empty() {
        return Err(CloudWatchLogsError::InvalidParameterException {
            message: format!("The {resource} name must not be blank."),
        });
    }

    Ok(value.to_owned())
}

pub(crate) fn validate_log_events(
    events: &[InputLogEvent],
) -> Result<(), CloudWatchLogsError> {
    let mut previous_timestamp = None;
    for event in events {
        if previous_timestamp
            .is_some_and(|previous| event.timestamp_millis < previous)
        {
            return Err(CloudWatchLogsError::InvalidParameterException {
                message: "Log events must be sorted by timestamp.".to_owned(),
            });
        }
        previous_timestamp = Some(event.timestamp_millis);
    }

    Ok(())
}

pub(crate) fn reject_logs_pagination(
    operation: &str,
    next_token: Option<&str>,
) -> Result<(), CloudWatchLogsError> {
    if next_token.is_some() {
        return Err(CloudWatchLogsError::InvalidNextToken {
            message: format!(
                "{operation} pagination tokens are not supported."
            ),
        });
    }

    Ok(())
}

pub(crate) fn resolved_log_limit(
    limit: Option<u32>,
    max_allowed: usize,
) -> usize {
    limit
        .map(|value| value as usize)
        .unwrap_or(max_allowed)
        .min(max_allowed)
        .max(1)
}

pub(crate) fn parse_logs_token(
    token: &str,
) -> Result<usize, CloudWatchLogsError> {
    token.parse::<usize>().map_err(|_| CloudWatchLogsError::InvalidNextToken {
        message: "The provided next token is invalid.".to_owned(),
    })
}

pub(crate) fn timestamp_in_range(
    timestamp_millis: u64,
    start_time_millis: Option<u64>,
    end_time_millis: Option<u64>,
) -> bool {
    if start_time_millis.is_some_and(|start| timestamp_millis < start) {
        return false;
    }
    if end_time_millis.is_some_and(|end| timestamp_millis > end) {
        return false;
    }

    true
}

pub(crate) fn format_log_group_arn(
    scope: &CloudWatchScope,
    log_group_name: &str,
    wildcard_suffix: bool,
) -> String {
    let suffix = if wildcard_suffix { ":*" } else { "" };
    format!(
        "arn:aws:logs:{}:{}:log-group:{log_group_name}{suffix}",
        scope.region().as_str(),
        scope.account_id().as_str()
    )
}

pub(crate) fn format_log_stream_arn(
    scope: &CloudWatchScope,
    log_group_name: &str,
    log_stream_name: &str,
) -> String {
    format!(
        "arn:aws:logs:{}:{}:log-group:{log_group_name}:log-stream:{log_stream_name}",
        scope.region().as_str(),
        scope.account_id().as_str()
    )
}
