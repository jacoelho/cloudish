use crate::errors::KinesisError;
use crate::identifiers::{self, KinesisStreamIdentifier, KinesisStreamName};
use crate::scope::KinesisScope;
use crate::shards::{KinesisShard, StoredShard};
use aws::{AccountId, Arn, ArnResource, Partition, RegionId, ServiceName};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateStreamInput {
    pub shard_count: usize,
    pub stream_name: KinesisStreamName,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteStreamInput {
    pub enforce_consumer_deletion: Option<bool>,
    pub stream: KinesisStreamIdentifier,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListStreamsInput {
    pub exclusive_start_stream_name: Option<KinesisStreamName>,
    pub limit: Option<u32>,
    pub next_token: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct ListStreamsOutput {
    pub has_more_streams: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_token: Option<String>,
    pub stream_names: Vec<KinesisStreamName>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeStreamInput {
    pub exclusive_start_shard_id: Option<String>,
    pub limit: Option<u32>,
    pub stream: KinesisStreamIdentifier,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeStreamSummaryInput {
    pub stream: KinesisStreamIdentifier,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegisterStreamConsumerInput {
    pub consumer_name: String,
    pub stream_arn: Arn,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeStreamConsumerInput {
    pub consumer_arn: Option<String>,
    pub consumer_name: Option<String>,
    pub stream_arn: Option<Arn>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeregisterStreamConsumerInput {
    pub consumer_arn: Option<String>,
    pub consumer_name: Option<String>,
    pub stream_arn: Option<Arn>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListStreamConsumersInput {
    pub max_results: Option<u32>,
    pub next_token: Option<String>,
    pub stream_arn: Option<Arn>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct ListStreamConsumersOutput {
    pub consumers: Vec<KinesisConsumer>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_token: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AddTagsToStreamInput {
    pub stream: KinesisStreamIdentifier,
    pub tags: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoveTagsFromStreamInput {
    pub stream: KinesisStreamIdentifier,
    pub tag_keys: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListTagsForStreamInput {
    pub exclusive_start_tag_key: Option<String>,
    pub limit: Option<u32>,
    pub stream: KinesisStreamIdentifier,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct ListTagsForStreamOutput {
    pub has_more_tags: bool,
    pub tags: Vec<KinesisTag>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StartStreamEncryptionInput {
    pub encryption_type: String,
    pub key_id: String,
    pub stream: KinesisStreamIdentifier,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StopStreamEncryptionInput {
    pub encryption_type: String,
    pub key_id: String,
    pub stream: KinesisStreamIdentifier,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum KinesisEncryptionType {
    #[serde(rename = "KMS")]
    Kms,
    #[serde(rename = "NONE")]
    None,
}

impl KinesisEncryptionType {
    pub(crate) fn parse(value: &str) -> Result<Self, KinesisError> {
        match value {
            "KMS" => Ok(Self::Kms),
            "NONE" => Ok(Self::None),
            _ => Err(KinesisError::InvalidArgument {
                message: format!("EncryptionType {value} is not supported."),
            }),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum KinesisConsumerStatus {
    #[serde(rename = "ACTIVE")]
    Active,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum KinesisStreamStatus {
    #[serde(rename = "ACTIVE")]
    Active,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum KinesisStreamMode {
    #[serde(rename = "PROVISIONED")]
    Provisioned,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct KinesisStreamModeDetails {
    pub stream_mode: KinesisStreamMode,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct KinesisEnhancedMetrics {
    pub shard_level_metrics: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct KinesisStreamDescription {
    pub encryption_type: KinesisEncryptionType,
    pub enhanced_monitoring: Vec<KinesisEnhancedMetrics>,
    pub has_more_shards: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key_id: Option<String>,
    pub retention_period_hours: u32,
    pub shards: Vec<KinesisShard>,
    #[serde(rename = "StreamARN")]
    pub stream_arn: Arn,
    pub stream_creation_timestamp: u64,
    pub stream_mode_details: KinesisStreamModeDetails,
    pub stream_name: KinesisStreamName,
    pub stream_status: KinesisStreamStatus,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct KinesisStreamDescriptionSummary {
    pub consumer_count: u32,
    pub encryption_type: KinesisEncryptionType,
    pub enhanced_monitoring: Vec<KinesisEnhancedMetrics>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key_id: Option<String>,
    pub open_shard_count: u32,
    pub retention_period_hours: u32,
    #[serde(rename = "StreamARN")]
    pub stream_arn: Arn,
    pub stream_creation_timestamp: u64,
    pub stream_mode_details: KinesisStreamModeDetails,
    pub stream_name: KinesisStreamName,
    pub stream_status: KinesisStreamStatus,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct KinesisConsumer {
    #[serde(rename = "ConsumerARN")]
    pub consumer_arn: String,
    pub consumer_creation_timestamp: u64,
    pub consumer_name: String,
    pub consumer_status: KinesisConsumerStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stream_arn: Option<Arn>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct KinesisTag {
    pub key: String,
    pub value: String,
}

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
pub(crate) struct StreamStorageKey {
    pub account_id: AccountId,
    pub region: RegionId,
    pub stream_name: KinesisStreamName,
}

impl StreamStorageKey {
    pub(crate) fn new(
        scope: &KinesisScope,
        stream_name: &KinesisStreamName,
    ) -> Self {
        Self {
            account_id: scope.account_id().clone(),
            region: scope.region().clone(),
            stream_name: stream_name.clone(),
        }
    }

    pub(crate) fn stream_arn(&self) -> Arn {
        Arn::trusted_new(
            Partition::aws(),
            ServiceName::Kinesis,
            Some(self.region.clone()),
            Some(self.account_id.clone()),
            ArnResource::Generic(format!("stream/{}", self.stream_name)),
        )
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
pub(crate) struct ConsumerStorageKey {
    pub account_id: AccountId,
    pub consumer_arn: String,
    pub region: RegionId,
}

impl ConsumerStorageKey {
    pub(crate) fn new(scope: &KinesisScope, consumer_arn: &str) -> Self {
        Self {
            account_id: scope.account_id().clone(),
            consumer_arn: consumer_arn.to_owned(),
            region: scope.region().clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct StoredStream {
    pub encryption_type: KinesisEncryptionType,
    pub key_id: Option<String>,
    pub next_sequence_number: u64,
    pub next_shard_index: u64,
    pub retention_period_hours: u32,
    pub shards: Vec<StoredShard>,
    pub stream_creation_timestamp: u64,
    pub stream_name: KinesisStreamName,
    pub stream_status: KinesisStreamStatus,
    pub tags: BTreeMap<String, String>,
}

pub(crate) fn stream_mode_details() -> KinesisStreamModeDetails {
    KinesisStreamModeDetails { stream_mode: KinesisStreamMode::Provisioned }
}

pub(crate) fn validate_consumer_name(
    value: &str,
) -> Result<&str, KinesisError> {
    if value.is_empty() || value.len() > 128 {
        return Err(KinesisError::InvalidArgument {
            message: "ConsumerName must be between 1 and 128 characters."
                .to_owned(),
        });
    }
    if !value.chars().all(|character| {
        character.is_ascii_alphanumeric()
            || matches!(character, '_' | '-' | '.')
    }) {
        return Err(KinesisError::InvalidArgument {
            message:
                "ConsumerName may only contain alphanumeric characters, hyphens, underscores, and periods."
                    .to_owned(),
        });
    }

    Ok(value)
}

pub(crate) fn stream_name_from_arn(
    scope: &KinesisScope,
    stream_arn: &Arn,
) -> Result<KinesisStreamName, KinesisError> {
    if stream_arn.region() != Some(scope.region())
        || stream_arn.account_id() != Some(scope.account_id())
    {
        return Err(KinesisError::ResourceNotFound {
            message: format!("Stream {stream_arn} was not found."),
        });
    }

    identifiers::stream_name_from_arn(stream_arn)
}
