use aws::{AccountId, Arn, ArnResource, RegionId, ServiceName};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use regex_lite::{Captures, Regex};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};
use storage::{StorageError, StorageFactory, StorageHandle};

use crate::{
    errors::{DynamoDbError, DynamoDbInitError},
    scope::{DynamoDbScope, TableName},
};

const DYNAMODB_NAMESPACE: &str = "dynamodb";
const MAX_BATCH_GET_KEYS: usize = 100;
const MAX_BATCH_WRITE_ITEMS: usize = 25;
const MAX_GET_RECORDS_LIMIT: usize = 1_000;
const MAX_TRANSACT_ITEMS: usize = 100;
const STREAM_SHARD_ID: &str = "shardId-00000000000000000001-cloudish";
const TTL_TRANSITION_SECONDS: u64 = 3_600;

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
pub enum AttributeValue {
    Binary(Vec<u8>),
    Bool(bool),
    BinarySet(Vec<Vec<u8>>),
    List(Vec<AttributeValue>),
    Map(BTreeMap<String, AttributeValue>),
    Number(String),
    NumberSet(Vec<String>),
    Null(bool),
    String(String),
    StringSet(Vec<String>),
}

impl AttributeValue {
    fn is_empty_string(&self) -> bool {
        matches!(self, Self::String(value) if value.is_empty())
    }

    fn is_empty_binary(&self) -> bool {
        matches!(self, Self::Binary(value) if value.is_empty())
    }

    fn is_empty_set(&self) -> bool {
        match self {
            Self::BinarySet(values) => values.is_empty(),
            Self::NumberSet(values) => values.is_empty(),
            Self::StringSet(values) => values.is_empty(),
            _ => false,
        }
    }

    fn is_scalar_key_value(&self) -> bool {
        matches!(self, Self::Binary(_) | Self::Number(_) | Self::String(_))
    }

    fn starts_with(&self, prefix: &AttributeValue) -> Option<bool> {
        match (self, prefix) {
            (Self::Binary(value), Self::Binary(prefix)) => {
                Some(value.starts_with(prefix))
            }
            (Self::String(value), Self::String(prefix)) => {
                Some(value.starts_with(prefix))
            }
            _ => None,
        }
    }
}

pub type Item = BTreeMap<String, AttributeValue>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ScalarAttributeType {
    Binary,
    Number,
    String,
}

impl ScalarAttributeType {
    fn matches(self, value: &AttributeValue) -> bool {
        matches!(
            (self, value),
            (Self::Binary, AttributeValue::Binary(_))
                | (Self::Number, AttributeValue::Number(_))
                | (Self::String, AttributeValue::String(_))
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttributeDefinition {
    pub attribute_name: String,
    pub attribute_type: ScalarAttributeType,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum KeyType {
    Hash,
    Range,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeySchemaElement {
    pub attribute_name: String,
    pub key_type: KeyType,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BillingMode {
    PayPerRequest,
    Provisioned,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProvisionedThroughput {
    pub read_capacity_units: u64,
    pub write_capacity_units: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProjectionType {
    All,
    Include,
    KeysOnly,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Projection {
    pub non_key_attributes: Vec<String>,
    pub projection_type: ProjectionType,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GlobalSecondaryIndexDefinition {
    pub index_name: String,
    pub key_schema: Vec<KeySchemaElement>,
    pub projection: Projection,
    pub provisioned_throughput: Option<ProvisionedThroughput>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalSecondaryIndexDefinition {
    pub index_name: String,
    pub key_schema: Vec<KeySchemaElement>,
    pub projection: Projection,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableStatus {
    Active,
    Deleting,
}

impl TableStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Active => "ACTIVE",
            Self::Deleting => "DELETING",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TimeToLiveStatus {
    Disabled,
    Disabling,
    Enabled,
    Enabling,
}

impl TimeToLiveStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Disabled => "DISABLED",
            Self::Disabling => "DISABLING",
            Self::Enabled => "ENABLED",
            Self::Enabling => "ENABLING",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct StoredTimeToLiveState {
    attribute_name: Option<String>,
    status: TimeToLiveStatus,
    transition_complete_at_epoch_seconds: Option<u64>,
}

impl Default for StoredTimeToLiveState {
    fn default() -> Self {
        Self {
            attribute_name: None,
            status: TimeToLiveStatus::Disabled,
            transition_complete_at_epoch_seconds: None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StreamViewType {
    KeysOnly,
    NewImage,
    OldImage,
    NewAndOldImages,
}

impl StreamViewType {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::KeysOnly => "KEYS_ONLY",
            Self::NewImage => "NEW_IMAGE",
            Self::OldImage => "OLD_IMAGE",
            Self::NewAndOldImages => "NEW_AND_OLD_IMAGES",
        }
    }

    /// # Errors
    ///
    /// Returns an error when the stream view type is unsupported.
    pub fn parse(value: &str) -> Result<Self, DynamoDbError> {
        match value {
            "KEYS_ONLY" => Ok(Self::KeysOnly),
            "NEW_IMAGE" => Ok(Self::NewImage),
            "OLD_IMAGE" => Ok(Self::OldImage),
            "NEW_AND_OLD_IMAGES" => Ok(Self::NewAndOldImages),
            _ => Err(validation_error(format!(
                "Unsupported StreamViewType value `{value}`."
            ))),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StreamStatus {
    Disabled,
    Enabled,
}

impl StreamStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Disabled => "DISABLED",
            Self::Enabled => "ENABLED",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamSpecification {
    pub stream_enabled: bool,
    pub stream_view_type: Option<StreamViewType>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct StoredStream {
    created_at_epoch_seconds: u64,
    next_sequence_number: u64,
    stream_arn: String,
    stream_label: String,
    stream_status: StreamStatus,
    stream_view_type: StreamViewType,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct StoredTable {
    attribute_definitions: Vec<AttributeDefinition>,
    billing_mode: BillingMode,
    created_at_epoch_seconds: u64,
    global_secondary_indexes: Vec<GlobalSecondaryIndexDefinition>,
    item_count: u64,
    key_schema: Vec<KeySchemaElement>,
    local_secondary_indexes: Vec<LocalSecondaryIndexDefinition>,
    provisioned_throughput: Option<ProvisionedThroughput>,
    #[serde(default)]
    stream: Option<StoredStream>,
    table_name: TableName,
    table_size_bytes: u64,
    #[serde(default)]
    tags: BTreeMap<String, String>,
    #[serde(default)]
    ttl: StoredTimeToLiveState,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableDescription {
    pub attribute_definitions: Vec<AttributeDefinition>,
    pub billing_mode: BillingMode,
    pub created_at_epoch_seconds: u64,
    pub global_secondary_indexes: Vec<GlobalSecondaryIndexDefinition>,
    pub item_count: u64,
    pub key_schema: Vec<KeySchemaElement>,
    pub latest_stream_arn: Option<String>,
    pub latest_stream_label: Option<String>,
    pub local_secondary_indexes: Vec<LocalSecondaryIndexDefinition>,
    pub provisioned_throughput: Option<ProvisionedThroughput>,
    pub stream_specification: Option<StreamSpecification>,
    pub table_arn: String,
    pub table_name: TableName,
    pub table_size_bytes: u64,
    pub table_status: TableStatus,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTableInput {
    pub attribute_definitions: Vec<AttributeDefinition>,
    pub billing_mode: Option<BillingMode>,
    pub global_secondary_indexes: Vec<GlobalSecondaryIndexDefinition>,
    pub key_schema: Vec<KeySchemaElement>,
    pub local_secondary_indexes: Vec<LocalSecondaryIndexDefinition>,
    pub provisioned_throughput: Option<ProvisionedThroughput>,
    pub table_name: TableName,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReturnValues {
    AllNew,
    AllOld,
    None,
    UpdatedNew,
    UpdatedOld,
}

impl ReturnValues {
    /// # Errors
    ///
    /// Returns an error when the return-values mode is unsupported.
    pub fn parse(value: Option<&str>) -> Result<Self, DynamoDbError> {
        match value.unwrap_or("NONE") {
            "ALL_NEW" => Ok(Self::AllNew),
            "ALL_OLD" => Ok(Self::AllOld),
            "NONE" => Ok(Self::None),
            "UPDATED_NEW" => Ok(Self::UpdatedNew),
            "UPDATED_OLD" => Ok(Self::UpdatedOld),
            value => Err(validation_error(format!(
                "Unsupported ReturnValues value `{value}`."
            ))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PutItemInput {
    pub condition_expression: Option<String>,
    pub expression_attribute_names: BTreeMap<String, String>,
    pub expression_attribute_values: BTreeMap<String, AttributeValue>,
    pub item: Item,
    pub return_values: ReturnValues,
    pub table_name: TableName,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PutItemOutput {
    pub attributes: Option<Item>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetItemInput {
    pub key: Item,
    pub table_name: TableName,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetItemOutput {
    pub item: Option<Item>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteItemInput {
    pub condition_expression: Option<String>,
    pub expression_attribute_names: BTreeMap<String, String>,
    pub expression_attribute_values: BTreeMap<String, AttributeValue>,
    pub key: Item,
    pub return_values: ReturnValues,
    pub table_name: TableName,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteItemOutput {
    pub attributes: Option<Item>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AttributeUpdateAction {
    Delete,
    Put,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttributeUpdate {
    pub action: AttributeUpdateAction,
    pub value: Option<AttributeValue>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateItemInput {
    pub attribute_updates: BTreeMap<String, AttributeUpdate>,
    pub condition_expression: Option<String>,
    pub expression_attribute_names: BTreeMap<String, String>,
    pub expression_attribute_values: BTreeMap<String, AttributeValue>,
    pub key: Item,
    pub return_values: ReturnValues,
    pub table_name: TableName,
    pub update_expression: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateItemOutput {
    pub attributes: Option<Item>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryInput {
    pub exclusive_start_key: Option<Item>,
    pub expression_attribute_names: BTreeMap<String, String>,
    pub expression_attribute_values: BTreeMap<String, AttributeValue>,
    pub filter_expression: Option<String>,
    pub index_name: Option<String>,
    pub key_condition_expression: String,
    pub limit: Option<usize>,
    pub scan_index_forward: bool,
    pub table_name: TableName,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryOutput {
    pub count: usize,
    pub items: Vec<Item>,
    pub last_evaluated_key: Option<Item>,
    pub scanned_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScanInput {
    pub exclusive_start_key: Option<Item>,
    pub expression_attribute_names: BTreeMap<String, String>,
    pub expression_attribute_values: BTreeMap<String, AttributeValue>,
    pub filter_expression: Option<String>,
    pub index_name: Option<String>,
    pub limit: Option<usize>,
    pub table_name: TableName,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScanOutput {
    pub count: usize,
    pub items: Vec<Item>,
    pub last_evaluated_key: Option<Item>,
    pub scanned_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BatchWriteRequest {
    Delete { key: Item },
    Put { item: Item },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchWriteItemInput {
    pub request_items: BTreeMap<TableName, Vec<BatchWriteRequest>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchWriteItemOutput {
    pub unprocessed_items: BTreeMap<TableName, Vec<BatchWriteRequest>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeysAndAttributes {
    pub keys: Vec<Item>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchGetItemInput {
    pub request_items: BTreeMap<TableName, KeysAndAttributes>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchGetItemOutput {
    pub responses: BTreeMap<TableName, Vec<Item>>,
    pub unprocessed_keys: BTreeMap<TableName, KeysAndAttributes>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateTableInput {
    pub billing_mode: Option<BillingMode>,
    pub provisioned_throughput: Option<ProvisionedThroughput>,
    pub stream_specification: Option<StreamSpecification>,
    pub table_name: TableName,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimeToLiveDescription {
    pub attribute_name: Option<String>,
    pub time_to_live_status: TimeToLiveStatus,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimeToLiveSpecification {
    pub attribute_name: String,
    pub enabled: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateTimeToLiveInput {
    pub table_name: String,
    pub time_to_live_specification: TimeToLiveSpecification,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourceTag {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TagResourceInput {
    pub resource_arn: String,
    pub tags: Vec<ResourceTag>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UntagResourceInput {
    pub resource_arn: String,
    pub tag_keys: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListTagsOfResourceInput {
    pub next_token: Option<String>,
    pub resource_arn: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListTagsOfResourceOutput {
    pub next_token: Option<String>,
    pub tags: Vec<ResourceTag>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransactGetItem {
    pub key: Item,
    pub table_name: TableName,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransactGetItemsInput {
    pub transact_items: Vec<TransactGetItem>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransactGetItemOutput {
    pub item: Option<Item>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransactGetItemsOutput {
    pub responses: Vec<TransactGetItemOutput>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransactConditionCheck {
    pub condition_expression: String,
    pub expression_attribute_names: BTreeMap<String, String>,
    pub expression_attribute_values: BTreeMap<String, AttributeValue>,
    pub key: Item,
    pub table_name: TableName,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransactDeleteItem {
    pub condition_expression: Option<String>,
    pub expression_attribute_names: BTreeMap<String, String>,
    pub expression_attribute_values: BTreeMap<String, AttributeValue>,
    pub key: Item,
    pub table_name: TableName,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransactPutItem {
    pub condition_expression: Option<String>,
    pub expression_attribute_names: BTreeMap<String, String>,
    pub expression_attribute_values: BTreeMap<String, AttributeValue>,
    pub item: Item,
    pub table_name: TableName,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransactUpdateItem {
    pub condition_expression: Option<String>,
    pub expression_attribute_names: BTreeMap<String, String>,
    pub expression_attribute_values: BTreeMap<String, AttributeValue>,
    pub key: Item,
    pub table_name: TableName,
    pub update_expression: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransactWriteItem {
    ConditionCheck(TransactConditionCheck),
    Delete(TransactDeleteItem),
    Put(TransactPutItem),
    Update(TransactUpdateItem),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransactWriteItemsInput {
    pub transact_items: Vec<TransactWriteItem>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CancellationReason {
    pub code: Option<String>,
    pub message: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListStreamsInput {
    pub exclusive_start_stream_arn: Option<String>,
    pub limit: Option<usize>,
    pub table_name: Option<TableName>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamSummary {
    pub stream_arn: String,
    pub stream_label: String,
    pub table_name: TableName,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListStreamsOutput {
    pub last_evaluated_stream_arn: Option<String>,
    pub streams: Vec<StreamSummary>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SequenceNumberRange {
    pub ending_sequence_number: Option<String>,
    pub starting_sequence_number: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamShard {
    pub parent_shard_id: Option<String>,
    pub sequence_number_range: SequenceNumberRange,
    pub shard_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamDescription {
    pub creation_request_date_time: u64,
    pub key_schema: Vec<KeySchemaElement>,
    pub last_evaluated_shard_id: Option<String>,
    pub shards: Vec<StreamShard>,
    pub stream_arn: String,
    pub stream_label: String,
    pub stream_status: StreamStatus,
    pub stream_view_type: StreamViewType,
    pub table_name: TableName,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeStreamInput {
    pub exclusive_start_shard_id: Option<String>,
    pub limit: Option<usize>,
    pub stream_arn: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShardIteratorType {
    AfterSequenceNumber,
    AtSequenceNumber,
    Latest,
    TrimHorizon,
}

impl ShardIteratorType {
    /// # Errors
    ///
    /// Returns an error when the shard iterator type is unsupported.
    pub fn parse(value: &str) -> Result<Self, DynamoDbError> {
        match value {
            "AFTER_SEQUENCE_NUMBER" => Ok(Self::AfterSequenceNumber),
            "AT_SEQUENCE_NUMBER" => Ok(Self::AtSequenceNumber),
            "LATEST" => Ok(Self::Latest),
            "TRIM_HORIZON" => Ok(Self::TrimHorizon),
            _ => Err(validation_error(format!(
                "Unsupported ShardIteratorType value `{value}`."
            ))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetShardIteratorInput {
    pub sequence_number: Option<String>,
    pub shard_id: String,
    pub shard_iterator_type: ShardIteratorType,
    pub stream_arn: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetShardIteratorOutput {
    pub shard_iterator: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetRecordsInput {
    pub limit: Option<usize>,
    pub shard_iterator: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamRecordData {
    pub approximate_creation_date_time: u64,
    pub keys: Item,
    pub new_image: Option<Item>,
    pub old_image: Option<Item>,
    pub sequence_number: String,
    pub size_bytes: u64,
    pub stream_view_type: StreamViewType,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamRecord {
    pub aws_region: String,
    pub dynamodb: StreamRecordData,
    pub event_id: String,
    pub event_name: String,
    pub event_source: String,
    pub event_version: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetRecordsOutput {
    pub next_shard_iterator: Option<String>,
    pub records: Vec<StreamRecord>,
}

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
struct TableStorageKey {
    account_id: AccountId,
    region: RegionId,
    table_name: TableName,
}

impl TableStorageKey {
    fn new(scope: &DynamoDbScope, table_name: &TableName) -> Self {
        Self {
            account_id: scope.account_id().clone(),
            region: scope.region().clone(),
            table_name: table_name.clone(),
        }
    }

    fn table_arn(&self) -> String {
        format!(
            "arn:aws:{DYNAMODB_NAMESPACE}:{}:{}:table/{}",
            self.region.as_str(),
            self.account_id.as_str(),
            self.table_name
        )
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
struct ItemStorageKey {
    account_id: AccountId,
    partition_key: AttributeValue,
    region: RegionId,
    sort_key: Option<AttributeValue>,
    table_name: TableName,
}

impl ItemStorageKey {
    fn new(
        table: &TableStorageKey,
        partition_key: AttributeValue,
        sort_key: Option<AttributeValue>,
    ) -> Self {
        Self {
            account_id: table.account_id.clone(),
            partition_key,
            region: table.region.clone(),
            sort_key,
            table_name: table.table_name.clone(),
        }
    }

    fn belongs_to(&self, table: &TableStorageKey) -> bool {
        self.account_id == table.account_id
            && self.region == table.region
            && self.table_name == table.table_name
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
struct StreamRecordStorageKey {
    account_id: AccountId,
    region: RegionId,
    sequence_number: String,
    stream_arn: String,
}

impl StreamRecordStorageKey {
    fn new(
        scope: &DynamoDbScope,
        stream_arn: impl Into<String>,
        sequence_number: impl Into<String>,
    ) -> Self {
        Self {
            account_id: scope.account_id().clone(),
            region: scope.region().clone(),
            sequence_number: sequence_number.into(),
            stream_arn: stream_arn.into(),
        }
    }

    fn belongs_to_stream(
        &self,
        scope: &DynamoDbScope,
        stream_arn: &str,
    ) -> bool {
        self.account_id == *scope.account_id()
            && self.region == *scope.region()
            && self.stream_arn == stream_arn
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum QuerySource<'a> {
    Table(&'a StoredTable),
    GlobalSecondaryIndex(&'a GlobalSecondaryIndexDefinition),
    LocalSecondaryIndex(&'a LocalSecondaryIndexDefinition),
}

impl QuerySource<'_> {
    fn key_schema(&self) -> &[KeySchemaElement] {
        match self {
            Self::Table(table) => &table.key_schema,
            Self::GlobalSecondaryIndex(index) => &index.key_schema,
            Self::LocalSecondaryIndex(index) => &index.key_schema,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct QueryKeyCondition {
    partition_attribute: String,
    partition_value: AttributeValue,
    sort_condition: Option<ExpressionClause>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ComparisonOperator {
    Equal,
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual,
    NotEqual,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ExpressionClause {
    AttributeExists {
        attribute_name: String,
        negated: bool,
    },
    BeginsWith {
        attribute_name: String,
        value: AttributeValue,
    },
    Between {
        attribute_name: String,
        lower: AttributeValue,
        upper: AttributeValue,
    },
    Comparison {
        attribute_name: String,
        operator: ComparisonOperator,
        value: AttributeValue,
    },
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct UpdateResult {
    changed_attributes: BTreeSet<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ItemMutationEvent {
    keys: Item,
    new_item: Option<Item>,
    old_item: Option<Item>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum TransactWritePlan {
    Delete {
        existing: Option<Item>,
        item_key: ItemStorageKey,
        table: StoredTable,
        table_key: TableStorageKey,
    },
    Put {
        existing: Option<Item>,
        item: Item,
        item_key: ItemStorageKey,
        table: StoredTable,
        table_key: TableStorageKey,
    },
    Update {
        current: Item,
        existing: Option<Item>,
        item_key: ItemStorageKey,
        table: StoredTable,
        table_key: TableStorageKey,
    },
}

#[derive(Clone)]
pub struct DynamoDbService {
    item_store: StorageHandle<ItemStorageKey, Item>,
    mutation_lock: Arc<Mutex<()>>,
    stream_record_store: StorageHandle<StreamRecordStorageKey, StreamRecord>,
    table_store: StorageHandle<TableStorageKey, StoredTable>,
    time_source: Arc<dyn Fn() -> u64 + Send + Sync>,
}

impl DynamoDbService {
    /// # Errors
    ///
    /// Returns an error when the `DynamoDB` backing stores cannot be loaded.
    pub fn new(factory: &StorageFactory) -> Result<Self, DynamoDbInitError> {
        Self::with_stores(
            factory.create("dynamodb", "tables"),
            factory.create("dynamodb", "items"),
            factory.create("dynamodb", "stream-records"),
            Arc::new(system_epoch_seconds),
        )
    }

    fn with_stores(
        table_store: StorageHandle<TableStorageKey, StoredTable>,
        item_store: StorageHandle<ItemStorageKey, Item>,
        stream_record_store: StorageHandle<
            StreamRecordStorageKey,
            StreamRecord,
        >,
        time_source: Arc<dyn Fn() -> u64 + Send + Sync>,
    ) -> Result<Self, DynamoDbInitError> {
        table_store.load().map_err(DynamoDbInitError::Tables)?;
        item_store.load().map_err(DynamoDbInitError::Items)?;
        stream_record_store
            .load()
            .map_err(DynamoDbInitError::StreamRecords)?;

        Ok(Self {
            item_store,
            mutation_lock: Arc::new(Mutex::new(())),
            stream_record_store,
            table_store,
            time_source,
        })
    }

    /// # Errors
    ///
    /// Returns an error when validation fails or the new table metadata cannot
    /// be persisted.
    pub fn create_table(
        &self,
        scope: &DynamoDbScope,
        input: CreateTableInput,
    ) -> Result<TableDescription, DynamoDbError> {
        validate_create_table_input(&input)?;
        let table_key = TableStorageKey::new(scope, &input.table_name);
        let _guard = self.lock_state();

        if self.table_store.get(&table_key).is_some() {
            return Err(DynamoDbError::TableAlreadyExists {
                table_name: input.table_name,
            });
        }

        let billing_mode = billing_mode_for_create(
            input.billing_mode,
            input.provisioned_throughput.as_ref(),
        )?;
        let stored = StoredTable {
            attribute_definitions: input.attribute_definitions,
            billing_mode,
            created_at_epoch_seconds: (self.time_source)(),
            global_secondary_indexes: input.global_secondary_indexes,
            item_count: 0,
            key_schema: input.key_schema,
            local_secondary_indexes: input.local_secondary_indexes,
            provisioned_throughput: input.provisioned_throughput,
            stream: None,
            table_name: table_key.table_name.clone(),
            table_size_bytes: 0,
            tags: BTreeMap::new(),
            ttl: StoredTimeToLiveState::default(),
        };
        self.table_store.put(table_key.clone(), stored.clone()).map_err(
            |source| storage_error("writing table metadata", source),
        )?;

        Ok(to_table_description(&table_key, &stored, TableStatus::Active))
    }

    /// # Errors
    ///
    /// Returns an error when the requested table does not exist.
    pub fn describe_table(
        &self,
        scope: &DynamoDbScope,
        table_name: &TableName,
    ) -> Result<TableDescription, DynamoDbError> {
        let table_key = TableStorageKey::new(scope, table_name);
        let stored = self
            .table_store
            .get(&table_key)
            .ok_or_else(|| table_not_found(table_name))?;

        Ok(to_table_description(&table_key, &stored, TableStatus::Active))
    }

    pub fn list_tables(&self, scope: &DynamoDbScope) -> Vec<TableName> {
        let account_id = scope.account_id().clone();
        let region = scope.region().clone();
        let mut names = self
            .table_store
            .keys()
            .into_iter()
            .filter(|key| key.account_id == account_id && key.region == region)
            .map(|key| key.table_name)
            .collect::<Vec<_>>();
        names.sort();
        names
    }

    /// # Errors
    ///
    /// Returns an error when the requested table does not exist or its stored
    /// metadata or records cannot be removed.
    pub fn delete_table(
        &self,
        scope: &DynamoDbScope,
        table_name: &TableName,
    ) -> Result<TableDescription, DynamoDbError> {
        let table_key = TableStorageKey::new(scope, table_name);
        let _guard = self.lock_state();
        let stored = self
            .table_store
            .get(&table_key)
            .ok_or_else(|| table_not_found(table_name))?;
        if let Some(stream) = stored.stream.as_ref() {
            self.delete_stream_records(&table_key, &stream.stream_arn)?;
        }

        for item_key in self
            .item_store
            .keys()
            .into_iter()
            .filter(|key| key.belongs_to(&table_key))
        {
            self.item_store.delete(&item_key).map_err(|source| {
                storage_error("deleting table items", source)
            })?;
        }
        self.table_store.delete(&table_key).map_err(|source| {
            storage_error("deleting table metadata", source)
        })?;

        Ok(to_table_description(&table_key, &stored, TableStatus::Deleting))
    }

    /// # Errors
    ///
    /// Returns an error when validation or conditional checks fail or the item
    /// mutation cannot be persisted.
    pub fn put_item(
        &self,
        scope: &DynamoDbScope,
        input: PutItemInput,
    ) -> Result<PutItemOutput, DynamoDbError> {
        let table_key = TableStorageKey::new(scope, &input.table_name);
        let _guard = self.lock_state();
        let mut table = self.load_table(&table_key)?;
        validate_item(&table, &input.item)?;
        let item_key = item_storage_key(&table_key, &table, &input.item)?;
        let existing = self.item_store.get(&item_key);

        evaluate_condition_expression(
            existing.as_ref(),
            input.condition_expression.as_deref(),
            &input.expression_attribute_names,
            &input.expression_attribute_values,
        )?;

        let old_size =
            existing.as_ref().map(item_size_bytes).transpose()?.unwrap_or(0);
        let new_size = item_size_bytes(&input.item)?;
        let new_item = input.item;
        self.item_store
            .put(item_key.clone(), new_item.clone())
            .map_err(|source| storage_error("writing item state", source))?;
        update_table_statistics(&mut table, old_size, Some(new_size));
        self.record_stream_event(
            &table_key,
            &mut table,
            &item_key,
            existing.as_ref(),
            Some(&new_item),
        )?;
        self.persist_table(&table_key, table)?;

        Ok(PutItemOutput {
            attributes: matches!(input.return_values, ReturnValues::AllOld)
                .then_some(existing)
                .flatten(),
        })
    }

    /// # Errors
    ///
    /// Returns an error when the target table or key does not match the table
    /// schema.
    pub fn get_item(
        &self,
        scope: &DynamoDbScope,
        input: GetItemInput,
    ) -> Result<GetItemOutput, DynamoDbError> {
        let table_key = TableStorageKey::new(scope, &input.table_name);
        let table = self.load_table(&table_key)?;
        let item_key =
            key_storage_from_key_item(&table_key, &table, &input.key)?;

        Ok(GetItemOutput { item: self.item_store.get(&item_key) })
    }

    /// # Errors
    ///
    /// Returns an error when validation or conditional checks fail or the item
    /// mutation cannot be persisted.
    pub fn delete_item(
        &self,
        scope: &DynamoDbScope,
        input: DeleteItemInput,
    ) -> Result<DeleteItemOutput, DynamoDbError> {
        let table_key = TableStorageKey::new(scope, &input.table_name);
        let _guard = self.lock_state();
        let mut table = self.load_table(&table_key)?;
        let item_key =
            key_storage_from_key_item(&table_key, &table, &input.key)?;
        let existing = self.item_store.get(&item_key);

        evaluate_condition_expression(
            existing.as_ref(),
            input.condition_expression.as_deref(),
            &input.expression_attribute_names,
            &input.expression_attribute_values,
        )?;

        if let Some(existing) = existing.as_ref() {
            let old_size = item_size_bytes(existing)?;
            self.item_store.delete(&item_key).map_err(|source| {
                storage_error("deleting item state", source)
            })?;
            update_table_statistics(&mut table, old_size, None);
            self.record_stream_event(
                &table_key,
                &mut table,
                &item_key,
                Some(existing),
                None,
            )?;
            self.persist_table(&table_key, table)?;
        }

        Ok(DeleteItemOutput {
            attributes: matches!(input.return_values, ReturnValues::AllOld)
                .then_some(existing)
                .flatten(),
        })
    }

    /// # Errors
    ///
    /// Returns an error when validation or conditional checks fail or the item
    /// mutation cannot be persisted.
    pub fn update_item(
        &self,
        scope: &DynamoDbScope,
        input: UpdateItemInput,
    ) -> Result<UpdateItemOutput, DynamoDbError> {
        let table_key = TableStorageKey::new(scope, &input.table_name);
        let _guard = self.lock_state();
        let mut table = self.load_table(&table_key)?;
        let item_key =
            key_storage_from_key_item(&table_key, &table, &input.key)?;
        let existing = self.item_store.get(&item_key);

        evaluate_condition_expression(
            existing.as_ref(),
            input.condition_expression.as_deref(),
            &input.expression_attribute_names,
            &input.expression_attribute_values,
        )?;

        let mut current =
            existing.clone().unwrap_or_else(|| input.key.clone());
        let update = if let Some(expression) =
            input.update_expression.as_deref()
        {
            apply_update_expression(
                &mut current,
                expression,
                &input.expression_attribute_names,
                &input.expression_attribute_values,
            )?
        } else if !input.attribute_updates.is_empty() {
            apply_attribute_updates(&mut current, &input.attribute_updates)?
        } else {
            return Err(validation_error(
                "UpdateItem requires AttributeUpdates or UpdateExpression.",
            ));
        };

        ensure_item_matches_key(&table, &input.key, &current)?;
        validate_item(&table, &current)?;

        let old_size =
            existing.as_ref().map(item_size_bytes).transpose()?.unwrap_or(0);
        let new_size = item_size_bytes(&current)?;
        self.item_store
            .put(item_key.clone(), current.clone())
            .map_err(|source| storage_error("writing item state", source))?;
        update_table_statistics(&mut table, old_size, Some(new_size));
        self.record_stream_event(
            &table_key,
            &mut table,
            &item_key,
            existing.as_ref(),
            Some(&current),
        )?;
        self.persist_table(&table_key, table)?;

        Ok(UpdateItemOutput {
            attributes: update_item_return_values(
                existing.as_ref(),
                &current,
                &update.changed_attributes,
                input.return_values,
            ),
        })
    }

    /// # Errors
    ///
    /// Returns an error when the query expression, pagination key, or backing
    /// table/index state is invalid.
    pub fn query(
        &self,
        scope: &DynamoDbScope,
        input: QueryInput,
    ) -> Result<QueryOutput, DynamoDbError> {
        let table_key = TableStorageKey::new(scope, &input.table_name);
        let table = self.load_table(&table_key)?;
        let source =
            resolve_query_source(&table, input.index_name.as_deref())?;
        let key_condition = parse_query_key_condition(
            source.key_schema(),
            &input.key_condition_expression,
            &input.expression_attribute_names,
            &input.expression_attribute_values,
        )?;
        let filter_expression = parse_expression_clauses(
            input.filter_expression.as_deref(),
            &input.expression_attribute_names,
            &input.expression_attribute_values,
        )?;
        let start_key = input
            .exclusive_start_key
            .as_ref()
            .map(|key| key_storage_from_key_item(&table_key, &table, key))
            .transpose()?;
        let mut entries = self
            .query_entries(&table_key, &table, &source)
            .into_iter()
            .filter_map(|(key, item)| {
                query_key_matches(&key_condition, &item)
                    .map(|matches| matches.then_some((key, item)))
                    .transpose()
            })
            .collect::<Result<Vec<_>, _>>()?;
        sort_entries(&table, source.key_schema(), &mut entries);
        if !input.scan_index_forward {
            entries.reverse();
        }

        let start_index =
            exclusive_start_index(&entries, start_key.as_ref()).unwrap_or(0);
        let after_start = entries.get(start_index..).unwrap_or(&[]);
        let limit = input.limit.unwrap_or(after_start.len());
        let evaluated = after_start
            .iter()
            .take(limit)
            .map(|(key, item)| (key.clone(), item.clone()))
            .collect::<Vec<_>>();
        let items = evaluated
            .iter()
            .map(|(_, item)| {
                evaluate_expression_clauses(item, filter_expression.as_ref())
                    .map(|matches| matches.then_some(item.clone()))
            })
            .filter_map(|result| result.transpose())
            .collect::<Result<Vec<_>, _>>()?;
        let last_evaluated_key = (after_start.len() > limit
            && !evaluated.is_empty())
        .then(|| {
            evaluated
                .get(limit.saturating_sub(1))
                .map(|(key, _)| key_item_from_storage_key(&table, key))
                .transpose()
        })
        .transpose()?
        .flatten();

        Ok(QueryOutput {
            count: items.len(),
            items,
            last_evaluated_key,
            scanned_count: evaluated.len(),
        })
    }

    /// # Errors
    ///
    /// Returns an error when the scan filter, pagination key, or backing
    /// table/index state is invalid.
    pub fn scan(
        &self,
        scope: &DynamoDbScope,
        input: ScanInput,
    ) -> Result<ScanOutput, DynamoDbError> {
        let table_key = TableStorageKey::new(scope, &input.table_name);
        let table = self.load_table(&table_key)?;
        let source =
            resolve_query_source(&table, input.index_name.as_deref())?;
        let filter_expression = parse_expression_clauses(
            input.filter_expression.as_deref(),
            &input.expression_attribute_names,
            &input.expression_attribute_values,
        )?;
        let start_key = input
            .exclusive_start_key
            .as_ref()
            .map(|key| key_storage_from_key_item(&table_key, &table, key))
            .transpose()?;
        let mut entries = self.query_entries(&table_key, &table, &source);
        sort_entries(&table, &table.key_schema, &mut entries);

        let start_index =
            exclusive_start_index(&entries, start_key.as_ref()).unwrap_or(0);
        let after_start = entries.get(start_index..).unwrap_or(&[]);
        let limit = input.limit.unwrap_or(after_start.len());
        let evaluated = after_start
            .iter()
            .take(limit)
            .map(|(key, item)| (key.clone(), item.clone()))
            .collect::<Vec<_>>();
        let items = evaluated
            .iter()
            .map(|(_, item)| {
                evaluate_expression_clauses(item, filter_expression.as_ref())
                    .map(|matches| matches.then_some(item.clone()))
            })
            .filter_map(|result| result.transpose())
            .collect::<Result<Vec<_>, _>>()?;
        let last_evaluated_key = (after_start.len() > limit
            && !evaluated.is_empty())
        .then(|| {
            evaluated
                .get(limit.saturating_sub(1))
                .map(|(key, _)| key_item_from_storage_key(&table, key))
                .transpose()
        })
        .transpose()?
        .flatten();

        Ok(ScanOutput {
            count: items.len(),
            items,
            last_evaluated_key,
            scanned_count: evaluated.len(),
        })
    }

    /// # Errors
    ///
    /// Returns an error when validation fails or any batch mutation cannot be
    /// persisted.
    pub fn batch_write_item(
        &self,
        scope: &DynamoDbScope,
        input: BatchWriteItemInput,
    ) -> Result<BatchWriteItemOutput, DynamoDbError> {
        let total_operations =
            input.request_items.values().map(Vec::len).sum::<usize>();
        if total_operations > MAX_BATCH_WRITE_ITEMS {
            return Err(validation_error(format!(
                "BatchWriteItem supports at most {MAX_BATCH_WRITE_ITEMS} write requests."
            )));
        }

        let _guard = self.lock_state();
        let mut seen_keys = BTreeSet::new();
        let mut planned_writes = Vec::new();

        for (table_name, requests) in input.request_items {
            let table_key = TableStorageKey::new(scope, &table_name);
            let table = self.load_table(&table_key)?;
            for request in requests {
                match request {
                    BatchWriteRequest::Put { item } => {
                        validate_item(&table, &item)?;
                        let item_key =
                            item_storage_key(&table_key, &table, &item)?;
                        if !seen_keys.insert(item_key.clone()) {
                            return Err(validation_error(
                                "BatchWriteItem cannot operate on the same item more than once in a single request.",
                            ));
                        }
                        let existing = self.item_store.get(&item_key);
                        planned_writes.push((
                            table_key.clone(),
                            table.clone(),
                            item_key,
                            Some(item),
                            existing,
                        ));
                    }
                    BatchWriteRequest::Delete { key } => {
                        let item_key = key_storage_from_key_item(
                            &table_key, &table, &key,
                        )?;
                        if !seen_keys.insert(item_key.clone()) {
                            return Err(validation_error(
                                "BatchWriteItem cannot operate on the same item more than once in a single request.",
                            ));
                        }
                        let existing = self.item_store.get(&item_key);
                        planned_writes.push((
                            table_key.clone(),
                            table.clone(),
                            item_key,
                            None,
                            existing,
                        ));
                    }
                }
            }
        }

        let mut updated_tables = BTreeMap::new();
        for (table_key, table, item_key, new_item, existing) in planned_writes
        {
            let table_entry =
                updated_tables.entry(table_key.clone()).or_insert(table);
            match new_item {
                Some(item) => {
                    let old_size = existing
                        .as_ref()
                        .map(item_size_bytes)
                        .transpose()?
                        .unwrap_or(0);
                    let new_size = item_size_bytes(&item)?;
                    self.item_store
                        .put(item_key.clone(), item.clone())
                        .map_err(|source| {
                            storage_error("writing batch item state", source)
                        })?;
                    update_table_statistics(
                        table_entry,
                        old_size,
                        Some(new_size),
                    );
                    self.record_stream_event(
                        &table_key,
                        table_entry,
                        &item_key,
                        existing.as_ref(),
                        Some(&item),
                    )?;
                }
                None => {
                    if let Some(existing) = existing.as_ref() {
                        self.item_store.delete(&item_key).map_err(
                            |source| {
                                storage_error(
                                    "deleting batch item state",
                                    source,
                                )
                            },
                        )?;
                        update_table_statistics(
                            table_entry,
                            item_size_bytes(existing)?,
                            None,
                        );
                        self.record_stream_event(
                            &table_key,
                            table_entry,
                            &item_key,
                            Some(existing),
                            None,
                        )?;
                    }
                }
            }
        }

        for (table_key, table) in updated_tables {
            self.persist_table(&table_key, table)?;
        }

        Ok(BatchWriteItemOutput { unprocessed_items: BTreeMap::new() })
    }

    /// # Errors
    ///
    /// Returns an error when any requested table or key is invalid.
    pub fn batch_get_item(
        &self,
        scope: &DynamoDbScope,
        input: BatchGetItemInput,
    ) -> Result<BatchGetItemOutput, DynamoDbError> {
        let total_keys = input
            .request_items
            .values()
            .map(|keys| keys.keys.len())
            .sum::<usize>();
        if total_keys > MAX_BATCH_GET_KEYS {
            return Err(validation_error(format!(
                "BatchGetItem supports at most {MAX_BATCH_GET_KEYS} keys."
            )));
        }

        let mut responses = BTreeMap::new();
        for (table_name, keys) in input.request_items {
            let table_key = TableStorageKey::new(scope, &table_name);
            let table = self.load_table(&table_key)?;
            let items = keys
                .keys
                .into_iter()
                .map(|key| {
                    key_storage_from_key_item(&table_key, &table, &key)
                        .map(|item_key| self.item_store.get(&item_key))
                })
                .collect::<Result<Vec<_>, _>>()?
                .into_iter()
                .flatten()
                .collect::<Vec<_>>();
            responses.insert(table_name, items);
        }

        Ok(BatchGetItemOutput { responses, unprocessed_keys: BTreeMap::new() })
    }

    /// # Errors
    ///
    /// Returns an error when validation fails or the updated table metadata
    /// cannot be persisted.
    pub fn update_table(
        &self,
        scope: &DynamoDbScope,
        input: UpdateTableInput,
    ) -> Result<TableDescription, DynamoDbError> {
        let table_key = TableStorageKey::new(scope, &input.table_name);
        let _guard = self.lock_state();
        let mut table = self.load_table(&table_key)?;

        if let Some(billing_mode) = input.billing_mode {
            if billing_mode == BillingMode::Provisioned
                && input.provisioned_throughput.is_none()
                && table.provisioned_throughput.is_none()
            {
                return Err(validation_error(
                    "Provisioned throughput is required when BillingMode is PROVISIONED.",
                ));
            }
            table.billing_mode = billing_mode;
        }
        if let Some(provisioned_throughput) = input.provisioned_throughput {
            validate_provisioned_throughput(&provisioned_throughput)?;
            table.provisioned_throughput = Some(provisioned_throughput);
        }
        if table.billing_mode == BillingMode::PayPerRequest {
            table.provisioned_throughput = None;
        }
        if let Some(stream_specification) = input.stream_specification {
            table.stream = update_stream_state(
                &table_key,
                table.stream.as_ref(),
                &stream_specification,
                (self.time_source)(),
            )?;
        }

        self.persist_table(&table_key, table.clone())?;

        Ok(to_table_description(&table_key, &table, TableStatus::Active))
    }

    /// # Errors
    ///
    /// Returns an error when the referenced table cannot be resolved.
    pub fn describe_time_to_live(
        &self,
        scope: &DynamoDbScope,
        table_reference: &str,
    ) -> Result<TimeToLiveDescription, DynamoDbError> {
        let table_key = table_key_from_identifier(scope, table_reference)?;
        let _guard = self.lock_state();
        let mut table = self.load_table(&table_key)?;
        let now = (self.time_source)();

        if materialize_ttl_state(&mut table.ttl, now) {
            self.persist_table(&table_key, table.clone())?;
        }

        Ok(to_time_to_live_description(&table.ttl))
    }

    /// # Errors
    ///
    /// Returns an error when the referenced table cannot be resolved or the
    /// updated TTL state cannot be persisted.
    pub fn update_time_to_live(
        &self,
        scope: &DynamoDbScope,
        input: UpdateTimeToLiveInput,
    ) -> Result<TimeToLiveSpecification, DynamoDbError> {
        let table_key = table_key_from_identifier(scope, &input.table_name)?;
        let _guard = self.lock_state();
        let mut table = self.load_table(&table_key)?;
        let now = (self.time_source)();

        materialize_ttl_state(&mut table.ttl, now);
        table.ttl = transition_ttl_state(
            &table_key.table_name,
            &table.ttl,
            &input.time_to_live_specification,
            now,
        )?;
        self.persist_table(&table_key, table)?;

        Ok(input.time_to_live_specification)
    }

    /// # Errors
    ///
    /// Returns an error when removing expired items or persisting the updated
    /// table state fails.
    pub fn purge_expired_items(&self) -> Result<usize, DynamoDbError> {
        let _guard = self.lock_state();
        let now = (self.time_source)();
        let table_keys = self.table_store.keys();
        let mut deleted = 0usize;

        for table_key in table_keys {
            let mut table = self.load_table(&table_key)?;
            let ttl_changed = materialize_ttl_state(&mut table.ttl, now);
            let mut table_changed = ttl_changed;
            let Some(attribute_name) = active_ttl_attribute_name(&table.ttl)
            else {
                if table_changed {
                    self.persist_table(&table_key, table)?;
                }
                continue;
            };

            let expired_items = self
                .item_store
                .keys()
                .into_iter()
                .filter(|key| key.belongs_to(&table_key))
                .filter_map(|key| {
                    self.item_store.get(&key).and_then(|item| {
                        ttl_item_has_expired(&item, attribute_name, now)
                            .then_some((key, item))
                    })
                })
                .collect::<Vec<_>>();

            for (item_key, item) in expired_items {
                self.item_store.delete(&item_key).map_err(|source| {
                    storage_error("deleting TTL-expired item state", source)
                })?;
                update_table_statistics(
                    &mut table,
                    item_size_bytes(&item)?,
                    None,
                );
                self.record_stream_event(
                    &table_key,
                    &mut table,
                    &item_key,
                    Some(&item),
                    None,
                )?;
                table_changed = true;
                deleted = deleted.saturating_add(1);
            }

            if table_changed {
                self.persist_table(&table_key, table)?;
            }
        }

        Ok(deleted)
    }

    /// # Errors
    ///
    /// Returns an error when the referenced table cannot be resolved or tags
    /// cannot be persisted.
    pub fn tag_resource(
        &self,
        scope: &DynamoDbScope,
        input: TagResourceInput,
    ) -> Result<(), DynamoDbError> {
        let table_key =
            table_key_from_resource_arn(scope, &input.resource_arn)?;
        let _guard = self.lock_state();
        let mut table = self.load_table(&table_key)?;

        for tag in input.tags {
            if tag.key.trim().is_empty() {
                return Err(validation_error("Tag keys must not be empty."));
            }
            table.tags.insert(tag.key, tag.value);
        }
        self.persist_table(&table_key, table)
    }

    /// # Errors
    ///
    /// Returns an error when the referenced table cannot be resolved or tags
    /// cannot be persisted.
    pub fn untag_resource(
        &self,
        scope: &DynamoDbScope,
        input: UntagResourceInput,
    ) -> Result<(), DynamoDbError> {
        let table_key =
            table_key_from_resource_arn(scope, &input.resource_arn)?;
        let _guard = self.lock_state();
        let mut table = self.load_table(&table_key)?;

        for key in input.tag_keys {
            table.tags.remove(&key);
        }
        self.persist_table(&table_key, table)
    }

    /// # Errors
    ///
    /// Returns an error when the referenced table cannot be resolved.
    pub fn list_tags_of_resource(
        &self,
        scope: &DynamoDbScope,
        input: ListTagsOfResourceInput,
    ) -> Result<ListTagsOfResourceOutput, DynamoDbError> {
        if input.next_token.is_some() {
            return Err(validation_error(
                "ListTagsOfResource NextToken is not supported yet.",
            ));
        }

        let table_key =
            table_key_from_resource_arn(scope, &input.resource_arn)?;
        let table = self.load_table(&table_key)?;
        let tags = table
            .tags
            .into_iter()
            .map(|(key, value)| ResourceTag { key, value })
            .collect();

        Ok(ListTagsOfResourceOutput { next_token: None, tags })
    }

    /// # Errors
    ///
    /// Returns an error when validation fails or any requested table or key is
    /// invalid.
    pub fn transact_get_items(
        &self,
        scope: &DynamoDbScope,
        input: TransactGetItemsInput,
    ) -> Result<TransactGetItemsOutput, DynamoDbError> {
        validate_transaction_item_count(input.transact_items.len())?;
        let responses = input
            .transact_items
            .into_iter()
            .map(|item| {
                self.get_item(
                    scope,
                    GetItemInput {
                        key: item.key,
                        table_name: item.table_name,
                    },
                )
                .map(|output| TransactGetItemOutput { item: output.item })
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(TransactGetItemsOutput { responses })
    }

    /// # Errors
    ///
    /// Returns an error when validation or condition checks fail or any
    /// transactional mutation cannot be persisted.
    pub fn transact_write_items(
        &self,
        scope: &DynamoDbScope,
        input: TransactWriteItemsInput,
    ) -> Result<(), DynamoDbError> {
        validate_transaction_item_count(input.transact_items.len())?;
        let _guard = self.lock_state();
        let mut cancellation_reasons =
            Vec::with_capacity(input.transact_items.len());
        let mut plans = Vec::new();
        let mut seen_items = BTreeSet::new();
        let mut has_failed = false;

        for transact_item in input.transact_items {
            match transact_item {
                TransactWriteItem::ConditionCheck(item) => {
                    let table_key =
                        TableStorageKey::new(scope, &item.table_name);
                    let table = self.load_table(&table_key)?;
                    let item_key = key_storage_from_key_item(
                        &table_key, &table, &item.key,
                    )?;
                    if !seen_items
                        .insert((table_key.clone(), item_key.clone()))
                    {
                        return Err(validation_error(
                            "TransactWriteItems cannot operate on the same item more than once in a single request.",
                        ));
                    }
                    let existing = self.item_store.get(&item_key);
                    match evaluate_condition_expression(
                        existing.as_ref(),
                        Some(&item.condition_expression),
                        &item.expression_attribute_names,
                        &item.expression_attribute_values,
                    ) {
                        Ok(()) => cancellation_reasons
                            .push(success_cancellation_reason()),
                        Err(DynamoDbError::ConditionalCheckFailed) => {
                            has_failed = true;
                            cancellation_reasons
                                .push(conditional_check_cancellation_reason());
                        }
                        Err(error) => return Err(error),
                    }
                }
                TransactWriteItem::Delete(item) => {
                    let table_key =
                        TableStorageKey::new(scope, &item.table_name);
                    let table = self.load_table(&table_key)?;
                    let item_key = key_storage_from_key_item(
                        &table_key, &table, &item.key,
                    )?;
                    if !seen_items
                        .insert((table_key.clone(), item_key.clone()))
                    {
                        return Err(validation_error(
                            "TransactWriteItems cannot operate on the same item more than once in a single request.",
                        ));
                    }
                    let existing = self.item_store.get(&item_key);
                    match evaluate_condition_expression(
                        existing.as_ref(),
                        item.condition_expression.as_deref(),
                        &item.expression_attribute_names,
                        &item.expression_attribute_values,
                    ) {
                        Ok(()) => {
                            cancellation_reasons
                                .push(success_cancellation_reason());
                            plans.push(TransactWritePlan::Delete {
                                existing,
                                item_key,
                                table,
                                table_key,
                            });
                        }
                        Err(DynamoDbError::ConditionalCheckFailed) => {
                            has_failed = true;
                            cancellation_reasons
                                .push(conditional_check_cancellation_reason());
                        }
                        Err(error) => return Err(error),
                    }
                }
                TransactWriteItem::Put(item) => {
                    let table_key =
                        TableStorageKey::new(scope, &item.table_name);
                    let table = self.load_table(&table_key)?;
                    validate_item(&table, &item.item)?;
                    let item_key =
                        item_storage_key(&table_key, &table, &item.item)?;
                    if !seen_items
                        .insert((table_key.clone(), item_key.clone()))
                    {
                        return Err(validation_error(
                            "TransactWriteItems cannot operate on the same item more than once in a single request.",
                        ));
                    }
                    let existing = self.item_store.get(&item_key);
                    match evaluate_condition_expression(
                        existing.as_ref(),
                        item.condition_expression.as_deref(),
                        &item.expression_attribute_names,
                        &item.expression_attribute_values,
                    ) {
                        Ok(()) => {
                            cancellation_reasons
                                .push(success_cancellation_reason());
                            plans.push(TransactWritePlan::Put {
                                existing,
                                item: item.item,
                                item_key,
                                table,
                                table_key,
                            });
                        }
                        Err(DynamoDbError::ConditionalCheckFailed) => {
                            has_failed = true;
                            cancellation_reasons
                                .push(conditional_check_cancellation_reason());
                        }
                        Err(error) => return Err(error),
                    }
                }
                TransactWriteItem::Update(item) => {
                    let table_key =
                        TableStorageKey::new(scope, &item.table_name);
                    let table = self.load_table(&table_key)?;
                    let item_key = key_storage_from_key_item(
                        &table_key, &table, &item.key,
                    )?;
                    if !seen_items
                        .insert((table_key.clone(), item_key.clone()))
                    {
                        return Err(validation_error(
                            "TransactWriteItems cannot operate on the same item more than once in a single request.",
                        ));
                    }
                    let existing = self.item_store.get(&item_key);
                    match evaluate_condition_expression(
                        existing.as_ref(),
                        item.condition_expression.as_deref(),
                        &item.expression_attribute_names,
                        &item.expression_attribute_values,
                    ) {
                        Ok(()) => {
                            let mut current = existing
                                .clone()
                                .unwrap_or_else(|| item.key.clone());
                            apply_update_expression(
                                &mut current,
                                &item.update_expression,
                                &item.expression_attribute_names,
                                &item.expression_attribute_values,
                            )?;
                            ensure_item_matches_key(
                                &table, &item.key, &current,
                            )?;
                            validate_item(&table, &current)?;
                            cancellation_reasons
                                .push(success_cancellation_reason());
                            plans.push(TransactWritePlan::Update {
                                current,
                                existing,
                                item_key,
                                table,
                                table_key,
                            });
                        }
                        Err(DynamoDbError::ConditionalCheckFailed) => {
                            has_failed = true;
                            cancellation_reasons
                                .push(conditional_check_cancellation_reason());
                        }
                        Err(error) => return Err(error),
                    }
                }
            }
        }

        if has_failed {
            return Err(transaction_canceled_error(cancellation_reasons));
        }

        let mut updated_tables = BTreeMap::new();
        for plan in plans {
            match plan {
                TransactWritePlan::Delete {
                    existing,
                    item_key,
                    table,
                    table_key,
                } => {
                    let table_entry = updated_tables
                        .entry(table_key.clone())
                        .or_insert(table);
                    if let Some(existing) = existing.as_ref() {
                        self.item_store.delete(&item_key).map_err(
                            |source| {
                                storage_error(
                                    "deleting transactional item state",
                                    source,
                                )
                            },
                        )?;
                        update_table_statistics(
                            table_entry,
                            item_size_bytes(existing)?,
                            None,
                        );
                        self.record_stream_event(
                            &table_key,
                            table_entry,
                            &item_key,
                            Some(existing),
                            None,
                        )?;
                    }
                }
                TransactWritePlan::Put {
                    existing,
                    item,
                    item_key,
                    table,
                    table_key,
                } => {
                    let table_entry = updated_tables
                        .entry(table_key.clone())
                        .or_insert(table);
                    let old_size = existing
                        .as_ref()
                        .map(item_size_bytes)
                        .transpose()?
                        .unwrap_or(0);
                    let new_size = item_size_bytes(&item)?;
                    self.item_store
                        .put(item_key.clone(), item.clone())
                        .map_err(|source| {
                            storage_error(
                                "writing transactional item state",
                                source,
                            )
                        })?;
                    update_table_statistics(
                        table_entry,
                        old_size,
                        Some(new_size),
                    );
                    self.record_stream_event(
                        &table_key,
                        table_entry,
                        &item_key,
                        existing.as_ref(),
                        Some(&item),
                    )?;
                }
                TransactWritePlan::Update {
                    current,
                    existing,
                    item_key,
                    table,
                    table_key,
                } => {
                    let table_entry = updated_tables
                        .entry(table_key.clone())
                        .or_insert(table);
                    let old_size = existing
                        .as_ref()
                        .map(item_size_bytes)
                        .transpose()?
                        .unwrap_or(0);
                    let new_size = item_size_bytes(&current)?;
                    self.item_store
                        .put(item_key.clone(), current.clone())
                        .map_err(|source| {
                            storage_error(
                                "writing transactional item state",
                                source,
                            )
                        })?;
                    update_table_statistics(
                        table_entry,
                        old_size,
                        Some(new_size),
                    );
                    self.record_stream_event(
                        &table_key,
                        table_entry,
                        &item_key,
                        existing.as_ref(),
                        Some(&current),
                    )?;
                }
            }
        }

        for (table_key, table) in updated_tables {
            self.persist_table(&table_key, table)?;
        }

        Ok(())
    }

    /// # Errors
    ///
    /// Returns an error when validation fails or the referenced table cannot
    /// be resolved.
    pub fn list_streams(
        &self,
        scope: &DynamoDbScope,
        input: ListStreamsInput,
    ) -> Result<ListStreamsOutput, DynamoDbError> {
        if matches!(input.limit, Some(0)) {
            return Err(validation_error(
                "ListStreams Limit must be greater than zero.",
            ));
        }
        if let Some(table_name) = input.table_name.as_ref() {
            let table_key = TableStorageKey::new(scope, table_name);
            let _ = self.load_table(&table_key)?;
        }

        let mut streams = self
            .table_store
            .keys()
            .into_iter()
            .filter(|key| {
                key.account_id == *scope.account_id()
                    && key.region == *scope.region()
            })
            .filter_map(|table_key| {
                self.table_store.get(&table_key).and_then(|table| {
                    table.stream.map(|stream| StreamSummary {
                        stream_arn: stream.stream_arn,
                        stream_label: stream.stream_label,
                        table_name: table.table_name,
                    })
                })
            })
            .collect::<Vec<_>>();
        if let Some(table_name) = input.table_name {
            streams.retain(|stream| stream.table_name == table_name);
        }
        streams.sort_by(|left, right| left.stream_arn.cmp(&right.stream_arn));

        let start_index = input
            .exclusive_start_stream_arn
            .as_ref()
            .and_then(|stream_arn| {
                streams
                    .iter()
                    .position(|stream| &stream.stream_arn == stream_arn)
            })
            .map(|index| index.saturating_add(1))
            .unwrap_or(0);
        let remaining =
            streams.get(start_index.min(streams.len())..).unwrap_or(&[]);
        let limit = input.limit.unwrap_or(remaining.len());
        let selected =
            remaining.iter().take(limit).cloned().collect::<Vec<_>>();
        let last_evaluated_stream_arn = (remaining.len() > limit
            && !selected.is_empty())
        .then(|| selected.last().map(|stream| stream.stream_arn.clone()))
        .flatten();

        Ok(ListStreamsOutput { last_evaluated_stream_arn, streams: selected })
    }

    /// # Errors
    ///
    /// Returns an error when validation fails or the referenced stream cannot
    /// be resolved.
    pub fn describe_stream(
        &self,
        scope: &DynamoDbScope,
        input: DescribeStreamInput,
    ) -> Result<StreamDescription, DynamoDbError> {
        if matches!(input.limit, Some(0)) {
            return Err(validation_error(
                "DescribeStream Limit must be greater than zero.",
            ));
        }

        let (table, stream) = self.load_stream(scope, &input.stream_arn)?;
        let include_shard = match input.exclusive_start_shard_id.as_deref() {
            None => true,
            Some(STREAM_SHARD_ID) => false,
            Some(_) => {
                return Err(validation_error(
                    "ExclusiveStartShardId did not match the available shard.",
                ));
            }
        };
        let mut shards = Vec::new();
        if include_shard && input.limit.unwrap_or(1) > 0 {
            shards.push(StreamShard {
                parent_shard_id: None,
                sequence_number_range: SequenceNumberRange {
                    ending_sequence_number: None,
                    starting_sequence_number: format_sequence_number(1),
                },
                shard_id: STREAM_SHARD_ID.to_owned(),
            });
        }

        Ok(StreamDescription {
            creation_request_date_time: stream.created_at_epoch_seconds,
            key_schema: table.key_schema,
            last_evaluated_shard_id: None,
            shards,
            stream_arn: stream.stream_arn,
            stream_label: stream.stream_label,
            stream_status: stream.stream_status,
            stream_view_type: stream.stream_view_type,
            table_name: table.table_name,
        })
    }

    /// # Errors
    ///
    /// Returns an error when validation fails or the referenced stream cannot
    /// be resolved.
    pub fn get_shard_iterator(
        &self,
        scope: &DynamoDbScope,
        input: GetShardIteratorInput,
    ) -> Result<GetShardIteratorOutput, DynamoDbError> {
        if input.shard_id != STREAM_SHARD_ID {
            return Err(validation_error(format!(
                "Unknown shard `{}`.",
                input.shard_id
            )));
        }

        let (_, stream) = self.load_stream(scope, &input.stream_arn)?;
        let records =
            self.stream_records_for_stream(scope, &stream.stream_arn);
        let position = match input.shard_iterator_type {
            ShardIteratorType::TrimHorizon => 0,
            ShardIteratorType::Latest => records.len(),
            ShardIteratorType::AtSequenceNumber => {
                let Some(sequence_number) = input.sequence_number.as_deref()
                else {
                    return Err(validation_error(
                        "SequenceNumber is required for AT_SEQUENCE_NUMBER iterators.",
                    ));
                };
                find_sequence_position(&records, sequence_number, false)
            }
            ShardIteratorType::AfterSequenceNumber => {
                let Some(sequence_number) = input.sequence_number.as_deref()
                else {
                    return Err(validation_error(
                        "SequenceNumber is required for AFTER_SEQUENCE_NUMBER iterators.",
                    ));
                };
                find_sequence_position(&records, sequence_number, true)
            }
        };

        Ok(GetShardIteratorOutput {
            shard_iterator: encode_shard_iterator(
                &stream.stream_arn,
                position,
            ),
        })
    }

    /// # Errors
    ///
    /// Returns an error when validation fails or the referenced stream cannot
    /// be resolved.
    pub fn get_records(
        &self,
        scope: &DynamoDbScope,
        input: GetRecordsInput,
    ) -> Result<GetRecordsOutput, DynamoDbError> {
        let limit = input.limit.unwrap_or(100);
        if !(1..=MAX_GET_RECORDS_LIMIT).contains(&limit) {
            return Err(validation_error(format!(
                "GetRecords Limit must be between 1 and {MAX_GET_RECORDS_LIMIT}.",
            )));
        }

        let (stream_arn, position) =
            decode_shard_iterator(&input.shard_iterator)?;
        let _ = self.load_stream(scope, &stream_arn)?;
        let records = self.stream_records_for_stream(scope, &stream_arn);
        if position > records.len() {
            return Err(DynamoDbError::InvalidShardIterator);
        }

        let end = position.saturating_add(limit).min(records.len());
        let next_shard_iterator =
            Some(encode_shard_iterator(&stream_arn, end));

        Ok(GetRecordsOutput {
            next_shard_iterator,
            records: records.get(position..end).unwrap_or(&[]).to_vec(),
        })
    }

    fn load_table(
        &self,
        table_key: &TableStorageKey,
    ) -> Result<StoredTable, DynamoDbError> {
        self.table_store
            .get(table_key)
            .ok_or_else(|| table_not_found(&table_key.table_name))
    }

    fn persist_table(
        &self,
        table_key: &TableStorageKey,
        table: StoredTable,
    ) -> Result<(), DynamoDbError> {
        self.table_store
            .put(table_key.clone(), table)
            .map_err(|source| storage_error("writing table metadata", source))
    }

    fn load_stream(
        &self,
        scope: &DynamoDbScope,
        stream_arn: &str,
    ) -> Result<(StoredTable, StoredStream), DynamoDbError> {
        let (table_key, _) =
            table_key_and_name_from_stream_arn(scope, stream_arn)?;
        let table = self.load_table(&table_key)?;
        let stream = table
            .stream
            .clone()
            .filter(|stream| stream.stream_arn == stream_arn)
            .ok_or_else(|| resource_not_found(stream_arn))?;

        Ok((table, stream))
    }

    fn stream_records_for_stream(
        &self,
        scope: &DynamoDbScope,
        stream_arn: &str,
    ) -> Vec<StreamRecord> {
        let mut keys = self
            .stream_record_store
            .keys()
            .into_iter()
            .filter(|key| key.belongs_to_stream(scope, stream_arn))
            .collect::<Vec<_>>();
        keys.sort_by(|left, right| {
            left.sequence_number.cmp(&right.sequence_number)
        });
        keys.into_iter()
            .filter_map(|key| self.stream_record_store.get(&key))
            .collect()
    }

    fn delete_stream_records(
        &self,
        table_key: &TableStorageKey,
        stream_arn: &str,
    ) -> Result<(), DynamoDbError> {
        for record_key in
            self.stream_record_store.keys().into_iter().filter(|record_key| {
                record_key.account_id == table_key.account_id
                    && record_key.region == table_key.region
                    && record_key.stream_arn == stream_arn
            })
        {
            self.stream_record_store.delete(&record_key).map_err(
                |source| storage_error("deleting stream record state", source),
            )?;
        }

        Ok(())
    }

    fn record_stream_event(
        &self,
        table_key: &TableStorageKey,
        table: &mut StoredTable,
        item_key: &ItemStorageKey,
        old_item: Option<&Item>,
        new_item: Option<&Item>,
    ) -> Result<(), DynamoDbError> {
        let Some(event) = mutation_event(table, item_key, old_item, new_item)?
        else {
            return Ok(());
        };
        let Some(stream) = table.stream.as_mut() else {
            return Ok(());
        };
        if stream.stream_status != StreamStatus::Enabled {
            return Ok(());
        }

        let sequence_number =
            format_sequence_number(stream.next_sequence_number);
        let scope = DynamoDbScope::new(
            table_key.account_id.clone(),
            table_key.region.clone(),
        );
        let record = stream_record_from_event(
            &scope,
            stream,
            &sequence_number,
            (self.time_source)(),
            event,
        )?;
        self.stream_record_store
            .put(
                StreamRecordStorageKey::new(
                    &scope,
                    &stream.stream_arn,
                    &sequence_number,
                ),
                record,
            )
            .map_err(|source| {
                storage_error("writing stream record state", source)
            })?;
        stream.next_sequence_number =
            stream.next_sequence_number.saturating_add(1);

        Ok(())
    }

    fn query_entries(
        &self,
        table_key: &TableStorageKey,
        table: &StoredTable,
        source: &QuerySource<'_>,
    ) -> Vec<(ItemStorageKey, Item)> {
        self.item_store
            .keys()
            .into_iter()
            .filter(|key| key.belongs_to(table_key))
            .filter_map(|key| {
                self.item_store.get(&key).and_then(|item| {
                    item_in_query_source(table, source, &item)
                        .then_some((key, item))
                })
            })
            .collect()
    }

    fn lock_state(&self) -> MutexGuard<'_, ()> {
        self.mutation_lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

fn system_epoch_seconds() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs()
}

fn to_table_description(
    table_key: &TableStorageKey,
    stored: &StoredTable,
    table_status: TableStatus,
) -> TableDescription {
    TableDescription {
        attribute_definitions: stored.attribute_definitions.clone(),
        billing_mode: stored.billing_mode,
        created_at_epoch_seconds: stored.created_at_epoch_seconds,
        global_secondary_indexes: stored.global_secondary_indexes.clone(),
        item_count: stored.item_count,
        key_schema: stored.key_schema.clone(),
        latest_stream_arn: stored
            .stream
            .as_ref()
            .map(|stream| stream.stream_arn.clone()),
        latest_stream_label: stored
            .stream
            .as_ref()
            .map(|stream| stream.stream_label.clone()),
        local_secondary_indexes: stored.local_secondary_indexes.clone(),
        provisioned_throughput: stored.provisioned_throughput.clone(),
        stream_specification: stored.stream.as_ref().map(|stream| {
            StreamSpecification {
                stream_enabled: stream.stream_status == StreamStatus::Enabled,
                stream_view_type: Some(stream.stream_view_type),
            }
        }),
        table_arn: table_key.table_arn(),
        table_name: stored.table_name.clone(),
        table_size_bytes: stored.table_size_bytes,
        table_status,
    }
}

fn to_time_to_live_description(
    ttl: &StoredTimeToLiveState,
) -> TimeToLiveDescription {
    TimeToLiveDescription {
        attribute_name: ttl.attribute_name.clone(),
        time_to_live_status: ttl.status,
    }
}

fn materialize_ttl_state(ttl: &mut StoredTimeToLiveState, now: u64) -> bool {
    let Some(transition_complete_at_epoch_seconds) =
        ttl.transition_complete_at_epoch_seconds
    else {
        return false;
    };
    if now < transition_complete_at_epoch_seconds {
        return false;
    }

    match ttl.status {
        TimeToLiveStatus::Disabling => {
            ttl.attribute_name = None;
            ttl.status = TimeToLiveStatus::Disabled;
        }
        TimeToLiveStatus::Enabling => ttl.status = TimeToLiveStatus::Enabled,
        TimeToLiveStatus::Disabled | TimeToLiveStatus::Enabled => {
            return false;
        }
    }
    ttl.transition_complete_at_epoch_seconds = None;
    true
}

fn transition_ttl_state(
    table_name: &TableName,
    current: &StoredTimeToLiveState,
    specification: &TimeToLiveSpecification,
    now: u64,
) -> Result<StoredTimeToLiveState, DynamoDbError> {
    if specification.attribute_name.trim().is_empty() {
        return Err(validation_error(
            "TimeToLiveSpecification AttributeName is required.",
        ));
    }
    if matches!(
        current.status,
        TimeToLiveStatus::Disabling | TimeToLiveStatus::Enabling
    ) {
        return Err(DynamoDbError::TtlTransitionInProgress {
            message: format!(
                "Time to live state for table `{table_name}` is still \
                 transitioning."
            ),
        });
    }

    Ok(StoredTimeToLiveState {
        attribute_name: Some(specification.attribute_name.clone()),
        status: if specification.enabled {
            TimeToLiveStatus::Enabling
        } else {
            TimeToLiveStatus::Disabling
        },
        transition_complete_at_epoch_seconds: Some(
            now.saturating_add(TTL_TRANSITION_SECONDS),
        ),
    })
}

fn active_ttl_attribute_name(ttl: &StoredTimeToLiveState) -> Option<&str> {
    (ttl.status == TimeToLiveStatus::Enabled)
        .then_some(ttl.attribute_name.as_deref())
        .flatten()
}

fn ttl_item_has_expired(item: &Item, attribute_name: &str, now: u64) -> bool {
    item.get(attribute_name)
        .and_then(|attribute| match attribute {
            AttributeValue::Number(value) => value.parse::<u64>().ok(),
            _ => None,
        })
        .is_some_and(|expires_at| expires_at <= now)
}

fn update_stream_state(
    table_key: &TableStorageKey,
    current: Option<&StoredStream>,
    specification: &StreamSpecification,
    now: u64,
) -> Result<Option<StoredStream>, DynamoDbError> {
    if specification.stream_enabled {
        let Some(stream_view_type) = specification.stream_view_type else {
            return Err(validation_error(
                "StreamViewType is required when StreamEnabled is true.",
            ));
        };
        let stream_label = format_stream_label(now);
        return Ok(Some(StoredStream {
            created_at_epoch_seconds: now,
            next_sequence_number: 1,
            stream_arn: format!(
                "{}/stream/{stream_label}",
                table_key.table_arn()
            ),
            stream_label,
            stream_status: StreamStatus::Enabled,
            stream_view_type,
        }));
    }

    Ok(current.cloned().map(|mut stream| {
        stream.stream_status = StreamStatus::Disabled;
        stream
    }))
}

fn format_stream_label(now: u64) -> String {
    format!("{now}.000")
}

fn mutation_event(
    table: &StoredTable,
    item_key: &ItemStorageKey,
    old_item: Option<&Item>,
    new_item: Option<&Item>,
) -> Result<Option<ItemMutationEvent>, DynamoDbError> {
    if old_item.is_none() && new_item.is_none() {
        return Ok(None);
    }

    Ok(Some(ItemMutationEvent {
        keys: key_item_from_storage_key(table, item_key)?,
        new_item: new_item.cloned(),
        old_item: old_item.cloned(),
    }))
}

fn stream_record_from_event(
    scope: &DynamoDbScope,
    stream: &StoredStream,
    sequence_number: &str,
    approximate_creation_date_time: u64,
    event: ItemMutationEvent,
) -> Result<StreamRecord, DynamoDbError> {
    let event_name =
        stream_event_name(event.old_item.as_ref(), event.new_item.as_ref())
            .to_owned();
    let (new_image, old_image) =
        stream_images(stream.stream_view_type, event.old_item, event.new_item);
    let size_bytes = stream_record_size_bytes(
        &event.keys,
        old_image.as_ref(),
        new_image.as_ref(),
    )?;

    Ok(StreamRecord {
        aws_region: scope.region().as_str().to_owned(),
        dynamodb: StreamRecordData {
            approximate_creation_date_time,
            keys: event.keys,
            new_image,
            old_image,
            sequence_number: sequence_number.to_owned(),
            size_bytes,
            stream_view_type: stream.stream_view_type,
        },
        event_id: format!("{}:{sequence_number}", stream.stream_arn),
        event_name,
        event_source: "aws:dynamodb".to_owned(),
        event_version: "1.1".to_owned(),
    })
}

fn stream_event_name(
    old_item: Option<&Item>,
    new_item: Option<&Item>,
) -> &'static str {
    match (old_item.is_some(), new_item.is_some()) {
        (false, true) => "INSERT",
        (true, false) => "REMOVE",
        (true, true) => "MODIFY",
        (false, false) => "MODIFY",
    }
}

fn stream_images(
    stream_view_type: StreamViewType,
    old_item: Option<Item>,
    new_item: Option<Item>,
) -> (Option<Item>, Option<Item>) {
    match stream_view_type {
        StreamViewType::KeysOnly => (None, None),
        StreamViewType::NewImage => (new_item, None),
        StreamViewType::OldImage => (None, old_item),
        StreamViewType::NewAndOldImages => (new_item, old_item),
    }
}

fn stream_record_size_bytes(
    keys: &Item,
    old_image: Option<&Item>,
    new_image: Option<&Item>,
) -> Result<u64, DynamoDbError> {
    let mut size = item_size_bytes(keys)? as u64;
    if let Some(old_image) = old_image {
        size = size.saturating_add(item_size_bytes(old_image)? as u64);
    }
    if let Some(new_image) = new_image {
        size = size.saturating_add(item_size_bytes(new_image)? as u64);
    }
    Ok(size)
}

fn format_sequence_number(sequence_number: u64) -> String {
    format!("{sequence_number:021}")
}

fn validate_transaction_item_count(count: usize) -> Result<(), DynamoDbError> {
    if count == 0 || count > MAX_TRANSACT_ITEMS {
        return Err(validation_error(format!(
            "Transact operations support between 1 and {MAX_TRANSACT_ITEMS} items."
        )));
    }

    Ok(())
}

fn success_cancellation_reason() -> CancellationReason {
    CancellationReason { code: None, message: None }
}

fn conditional_check_cancellation_reason() -> CancellationReason {
    CancellationReason {
        code: Some("ConditionalCheckFailed".to_owned()),
        message: Some("The conditional request failed".to_owned()),
    }
}

fn transaction_canceled_error(
    cancellation_reasons: Vec<CancellationReason>,
) -> DynamoDbError {
    let codes = cancellation_reasons
        .iter()
        .map(|reason| reason.code.as_deref().unwrap_or("None"))
        .collect::<Vec<_>>();

    DynamoDbError::TransactionCanceled {
        message: format!(
            "Transaction cancelled, please refer cancellation reasons for \
             specific reasons [{}]",
            codes.join(", ")
        ),
        cancellation_reasons,
    }
}

fn table_key_from_identifier(
    scope: &DynamoDbScope,
    identifier: &str,
) -> Result<TableStorageKey, DynamoDbError> {
    if identifier.starts_with("arn:") {
        return table_key_from_resource_arn(scope, identifier);
    }

    Ok(TableStorageKey::new(scope, &TableName::parse(identifier)?))
}

fn table_key_from_resource_arn(
    scope: &DynamoDbScope,
    resource_arn: &str,
) -> Result<TableStorageKey, DynamoDbError> {
    let arn = dynamodb_arn(scope, resource_arn)?;
    let ArnResource::Generic(resource) = arn.resource() else {
        return Err(resource_not_found(resource_arn));
    };
    let Some(table_name) = resource.strip_prefix("table/") else {
        return Err(resource_not_found(resource_arn));
    };
    if table_name.is_empty() || table_name.contains('/') {
        return Err(resource_not_found(resource_arn));
    }

    Ok(TableStorageKey::new(scope, &TableName::parse(table_name)?))
}

fn table_key_and_name_from_stream_arn(
    scope: &DynamoDbScope,
    stream_arn: &str,
) -> Result<(TableStorageKey, String), DynamoDbError> {
    let arn = dynamodb_arn(scope, stream_arn)?;
    let ArnResource::Generic(resource) = arn.resource() else {
        return Err(resource_not_found(stream_arn));
    };
    let parts = resource.split('/').collect::<Vec<_>>();
    let [table_keyword, table_name, stream_keyword, stream_label] =
        parts.as_slice()
    else {
        return Err(resource_not_found(stream_arn));
    };
    if *table_keyword != "table"
        || *stream_keyword != "stream"
        || table_name.is_empty()
        || stream_label.is_empty()
    {
        return Err(resource_not_found(stream_arn));
    }

    Ok((
        TableStorageKey::new(scope, &TableName::parse(*table_name)?),
        (*stream_label).to_owned(),
    ))
}

fn dynamodb_arn(
    scope: &DynamoDbScope,
    resource_arn: &str,
) -> Result<Arn, DynamoDbError> {
    let arn = resource_arn
        .parse::<Arn>()
        .map_err(|_| resource_not_found(resource_arn))?;
    if arn.service() != ServiceName::DynamoDb
        || arn.region() != Some(scope.region())
        || arn.account_id() != Some(scope.account_id())
    {
        return Err(resource_not_found(resource_arn));
    }

    Ok(arn)
}

fn encode_shard_iterator(stream_arn: &str, position: usize) -> String {
    BASE64_STANDARD.encode(format!("{stream_arn}|{position}"))
}

fn decode_shard_iterator(
    shard_iterator: &str,
) -> Result<(String, usize), DynamoDbError> {
    let decoded = BASE64_STANDARD
        .decode(shard_iterator)
        .map_err(|_| DynamoDbError::InvalidShardIterator)?;
    let decoded = String::from_utf8(decoded)
        .map_err(|_| DynamoDbError::InvalidShardIterator)?;
    let (stream_arn, position) =
        decoded.rsplit_once('|').ok_or(DynamoDbError::InvalidShardIterator)?;
    let position = position
        .parse::<usize>()
        .map_err(|_| DynamoDbError::InvalidShardIterator)?;

    Ok((stream_arn.to_owned(), position))
}

fn find_sequence_position(
    records: &[StreamRecord],
    sequence_number: &str,
    after: bool,
) -> usize {
    for (index, record) in records.iter().enumerate() {
        let ordering =
            record.dynamodb.sequence_number.as_str().cmp(sequence_number);
        if after {
            if ordering == Ordering::Greater {
                return index;
            }
        } else if matches!(ordering, Ordering::Equal | Ordering::Greater) {
            return index;
        }
    }

    records.len()
}

fn billing_mode_for_create(
    billing_mode: Option<BillingMode>,
    provisioned_throughput: Option<&ProvisionedThroughput>,
) -> Result<BillingMode, DynamoDbError> {
    match billing_mode.unwrap_or(BillingMode::Provisioned) {
        BillingMode::PayPerRequest => {
            if provisioned_throughput.is_some() {
                return Err(validation_error(
                    "Neither ReadCapacityUnits nor WriteCapacityUnits can be specified when BillingMode is PAY_PER_REQUEST.",
                ));
            }
            Ok(BillingMode::PayPerRequest)
        }
        BillingMode::Provisioned => {
            let Some(provisioned_throughput) = provisioned_throughput else {
                return Err(validation_error(
                    "Provisioned throughput is required when BillingMode is PROVISIONED.",
                ));
            };
            validate_provisioned_throughput(provisioned_throughput)?;
            Ok(BillingMode::Provisioned)
        }
    }
}

fn validate_provisioned_throughput(
    provisioned_throughput: &ProvisionedThroughput,
) -> Result<(), DynamoDbError> {
    if provisioned_throughput.read_capacity_units == 0
        || provisioned_throughput.write_capacity_units == 0
    {
        return Err(validation_error(
            "Provisioned throughput values must be greater than zero.",
        ));
    }

    Ok(())
}

fn validate_create_table_input(
    input: &CreateTableInput,
) -> Result<(), DynamoDbError> {
    let attribute_types = attribute_type_lookup(&input.attribute_definitions)?;
    validate_primary_key_schema(&input.key_schema, &attribute_types)?;

    let table_schema = normalized_key_schema(&input.key_schema)?;
    let mut seen_index_names = BTreeSet::new();
    for index in &input.global_secondary_indexes {
        if !seen_index_names.insert(index.index_name.clone()) {
            return Err(validation_error(format!(
                "Duplicate index name `{}` is not allowed.",
                index.index_name
            )));
        }
        validate_primary_key_schema(&index.key_schema, &attribute_types)?;
        validate_projection(&index.projection)?;
        if let Some(provisioned_throughput) =
            index.provisioned_throughput.as_ref()
        {
            validate_provisioned_throughput(provisioned_throughput)?;
        }
    }
    for index in &input.local_secondary_indexes {
        if !seen_index_names.insert(index.index_name.clone()) {
            return Err(validation_error(format!(
                "Duplicate index name `{}` is not allowed.",
                index.index_name
            )));
        }
        validate_primary_key_schema(&index.key_schema, &attribute_types)?;
        validate_projection(&index.projection)?;
        let schema = normalized_key_schema(&index.key_schema)?;
        if schema.partition_attribute != table_schema.partition_attribute {
            return Err(validation_error(
                "Local secondary index partition key must match the table partition key.",
            ));
        }
    }

    billing_mode_for_create(
        input.billing_mode,
        input.provisioned_throughput.as_ref(),
    )?;
    Ok(())
}

fn attribute_type_lookup(
    attribute_definitions: &[AttributeDefinition],
) -> Result<BTreeMap<String, ScalarAttributeType>, DynamoDbError> {
    let mut attribute_types = BTreeMap::new();
    for definition in attribute_definitions {
        if definition.attribute_name.trim().is_empty() {
            return Err(validation_error(
                "AttributeDefinitions entries require a non-empty AttributeName.",
            ));
        }
        if attribute_types
            .insert(
                definition.attribute_name.clone(),
                definition.attribute_type,
            )
            .is_some()
        {
            return Err(validation_error(format!(
                "Duplicate AttributeDefinition for `{}` is not allowed.",
                definition.attribute_name
            )));
        }
    }
    if attribute_types.is_empty() {
        return Err(validation_error(
            "AttributeDefinitions must describe the table key schema.",
        ));
    }

    Ok(attribute_types)
}

fn validate_primary_key_schema(
    key_schema: &[KeySchemaElement],
    attribute_types: &BTreeMap<String, ScalarAttributeType>,
) -> Result<(), DynamoDbError> {
    normalized_key_schema(key_schema)?;
    for element in key_schema {
        let Some(attribute_type) =
            attribute_types.get(&element.attribute_name)
        else {
            return Err(validation_error(format!(
                "Key schema attribute `{}` is missing from AttributeDefinitions.",
                element.attribute_name
            )));
        };
        if !matches!(
            *attribute_type,
            ScalarAttributeType::Binary
                | ScalarAttributeType::Number
                | ScalarAttributeType::String
        ) {
            return Err(validation_error(format!(
                "Key schema attribute `{}` must use a scalar attribute type.",
                element.attribute_name
            )));
        }
    }

    Ok(())
}

fn validate_projection(projection: &Projection) -> Result<(), DynamoDbError> {
    if projection.projection_type != ProjectionType::Include
        && !projection.non_key_attributes.is_empty()
    {
        return Err(validation_error(
            "NonKeyAttributes are only valid for INCLUDE projections.",
        ));
    }

    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct NormalizedKeySchema {
    partition_attribute: String,
    sort_attribute: Option<String>,
}

fn normalized_key_schema(
    key_schema: &[KeySchemaElement],
) -> Result<NormalizedKeySchema, DynamoDbError> {
    if !(1..=2).contains(&key_schema.len()) {
        return Err(validation_error(
            "KeySchema must contain one HASH key and an optional RANGE key.",
        ));
    }
    let mut partition_attribute = None;
    let mut sort_attribute = None;
    for element in key_schema {
        match element.key_type {
            KeyType::Hash => {
                if partition_attribute
                    .replace(element.attribute_name.clone())
                    .is_some()
                {
                    return Err(validation_error(
                        "KeySchema cannot contain more than one HASH key.",
                    ));
                }
            }
            KeyType::Range => {
                if sort_attribute
                    .replace(element.attribute_name.clone())
                    .is_some()
                {
                    return Err(validation_error(
                        "KeySchema cannot contain more than one RANGE key.",
                    ));
                }
            }
        }
    }
    let Some(partition_attribute) = partition_attribute else {
        return Err(validation_error("KeySchema must include a HASH key."));
    };

    Ok(NormalizedKeySchema { partition_attribute, sort_attribute })
}

fn validate_item(
    table: &StoredTable,
    item: &Item,
) -> Result<(), DynamoDbError> {
    validate_attribute_values(item)?;
    let attribute_types = attribute_type_lookup(&table.attribute_definitions)?;
    let key_schema = normalized_key_schema(&table.key_schema)?;
    validate_key_attribute(
        item.get(&key_schema.partition_attribute),
        &key_schema.partition_attribute,
        attribute_types.get(&key_schema.partition_attribute).copied(),
    )?;
    if let Some(sort_attribute) = key_schema.sort_attribute.as_deref() {
        validate_key_attribute(
            item.get(sort_attribute),
            sort_attribute,
            attribute_types.get(sort_attribute).copied(),
        )?;
    }

    Ok(())
}

fn validate_attribute_values(item: &Item) -> Result<(), DynamoDbError> {
    for (name, value) in item {
        validate_attribute_value(name, value)?;
    }
    Ok(())
}

fn validate_attribute_value(
    attribute_name: &str,
    value: &AttributeValue,
) -> Result<(), DynamoDbError> {
    if value.is_empty_string() || value.is_empty_binary() {
        return Err(validation_error(format!(
            "Attribute `{attribute_name}` cannot be empty."
        )));
    }
    if value.is_empty_set() {
        return Err(validation_error(format!(
            "Attribute `{attribute_name}` cannot be an empty set."
        )));
    }
    if let AttributeValue::List(values) = value {
        for nested in values {
            validate_attribute_value(attribute_name, nested)?;
        }
    }
    if let AttributeValue::Map(values) = value {
        for (name, nested) in values {
            validate_attribute_value(name, nested)?;
        }
    }

    Ok(())
}

fn validate_key_attribute(
    value: Option<&AttributeValue>,
    attribute_name: &str,
    attribute_type: Option<ScalarAttributeType>,
) -> Result<(), DynamoDbError> {
    let Some(value) = value else {
        return Err(validation_error(
            "The provided key element does not match the schema",
        ));
    };
    let Some(attribute_type) = attribute_type else {
        return Err(validation_error(format!(
            "Key schema attribute `{attribute_name}` is not defined."
        )));
    };
    if !value.is_scalar_key_value() || !attribute_type.matches(value) {
        return Err(validation_error(
            "The provided key element does not match the schema",
        ));
    }

    Ok(())
}

fn item_storage_key(
    table_key: &TableStorageKey,
    table: &StoredTable,
    item: &Item,
) -> Result<ItemStorageKey, DynamoDbError> {
    let schema = normalized_key_schema(&table.key_schema)?;
    let partition_key =
        item.get(&schema.partition_attribute).cloned().ok_or_else(|| {
            validation_error(
                "The provided key element does not match the schema",
            )
        })?;
    let sort_key = schema
        .sort_attribute
        .as_deref()
        .map(|attribute_name| {
            item.get(attribute_name).cloned().ok_or_else(|| {
                validation_error(
                    "The provided key element does not match the schema",
                )
            })
        })
        .transpose()?;

    Ok(ItemStorageKey::new(table_key, partition_key, sort_key))
}

fn key_storage_from_key_item(
    table_key: &TableStorageKey,
    table: &StoredTable,
    key: &Item,
) -> Result<ItemStorageKey, DynamoDbError> {
    let schema = normalized_key_schema(&table.key_schema)?;
    let expected_len =
        usize::from(schema.sort_attribute.is_some()).saturating_add(1);
    if key.len() != expected_len {
        return Err(validation_error(
            "The provided key element does not match the schema",
        ));
    }
    let attribute_types = attribute_type_lookup(&table.attribute_definitions)?;
    let partition_key =
        key.get(&schema.partition_attribute).cloned().ok_or_else(|| {
            validation_error(
                "The provided key element does not match the schema",
            )
        })?;
    validate_key_attribute(
        Some(&partition_key),
        &schema.partition_attribute,
        attribute_types.get(&schema.partition_attribute).copied(),
    )?;
    let sort_key = schema
        .sort_attribute
        .as_deref()
        .map(|attribute_name| {
            let value = key.get(attribute_name).cloned().ok_or_else(|| {
                validation_error(
                    "The provided key element does not match the schema",
                )
            })?;
            validate_key_attribute(
                Some(&value),
                attribute_name,
                attribute_types.get(attribute_name).copied(),
            )?;
            Ok(value)
        })
        .transpose()?;

    Ok(ItemStorageKey::new(table_key, partition_key, sort_key))
}

fn ensure_item_matches_key(
    table: &StoredTable,
    key: &Item,
    item: &Item,
) -> Result<(), DynamoDbError> {
    let schema = normalized_key_schema(&table.key_schema)?;
    let partition =
        item.get(&schema.partition_attribute).ok_or_else(|| {
            validation_error(
                "The provided key element does not match the schema",
            )
        })?;
    if key.get(&schema.partition_attribute) != Some(partition) {
        return Err(validation_error(
            "The primary key attributes cannot be updated.",
        ));
    }
    if let Some(sort_attribute) = schema.sort_attribute.as_deref() {
        let sort = item.get(sort_attribute).ok_or_else(|| {
            validation_error(
                "The provided key element does not match the schema",
            )
        })?;
        if key.get(sort_attribute) != Some(sort) {
            return Err(validation_error(
                "The primary key attributes cannot be updated.",
            ));
        }
    }

    Ok(())
}

fn item_size_bytes(item: &Item) -> Result<usize, DynamoDbError> {
    serde_json::to_vec(item)
        .map(|bytes| bytes.len())
        .map_err(|error| validation_error(error.to_string()))
}

fn update_table_statistics(
    table: &mut StoredTable,
    old_size: usize,
    new_size: Option<usize>,
) {
    match new_size {
        Some(new_size) if old_size == 0 => {
            table.item_count = table.item_count.saturating_add(1);
            table.table_size_bytes = table
                .table_size_bytes
                .saturating_add(new_size as u64);
        }
        Some(new_size) => {
            table.table_size_bytes = table
                .table_size_bytes
                .saturating_add(new_size as u64)
                .saturating_sub(old_size as u64);
        }
        None if old_size > 0 => {
            table.item_count = table.item_count.saturating_sub(1);
            table.table_size_bytes =
                table.table_size_bytes.saturating_sub(old_size as u64);
        }
        None => {}
    }
}

fn resolve_query_source<'a>(
    table: &'a StoredTable,
    index_name: Option<&str>,
) -> Result<QuerySource<'a>, DynamoDbError> {
    let Some(index_name) = index_name else {
        return Ok(QuerySource::Table(table));
    };
    if let Some(index) = table
        .global_secondary_indexes
        .iter()
        .find(|index| index.index_name == index_name)
    {
        return Ok(QuerySource::GlobalSecondaryIndex(index));
    }
    if let Some(index) = table
        .local_secondary_indexes
        .iter()
        .find(|index| index.index_name == index_name)
    {
        return Ok(QuerySource::LocalSecondaryIndex(index));
    }

    Err(validation_error(format!(
        "The table does not have the specified index: {index_name}"
    )))
}

fn item_in_query_source(
    table: &StoredTable,
    source: &QuerySource<'_>,
    item: &Item,
) -> bool {
    match source {
        QuerySource::Table(_) => normalized_key_schema(&table.key_schema)
            .map(|schema| {
                item.contains_key(&schema.partition_attribute)
                    && schema.sort_attribute.as_deref().is_none_or(
                        |attribute_name| item.contains_key(attribute_name),
                    )
            })
            .unwrap_or(false),
        QuerySource::GlobalSecondaryIndex(index) => {
            index_keys_present(&index.key_schema, item)
        }
        QuerySource::LocalSecondaryIndex(index) => {
            index_keys_present(&index.key_schema, item)
        }
    }
}

fn index_keys_present(key_schema: &[KeySchemaElement], item: &Item) -> bool {
    key_schema.iter().all(|element| item.contains_key(&element.attribute_name))
}

fn parse_query_key_condition(
    key_schema: &[KeySchemaElement],
    expression: &str,
    expression_attribute_names: &BTreeMap<String, String>,
    expression_attribute_values: &BTreeMap<String, AttributeValue>,
) -> Result<QueryKeyCondition, DynamoDbError> {
    let schema = normalized_key_schema(key_schema)?;
    let clauses = parse_expression_clauses(
        Some(expression),
        expression_attribute_names,
        expression_attribute_values,
    )?
    .unwrap_or_default();
    if clauses.is_empty() || clauses.len() > 2 {
        return Err(validation_error(
            "Query requires an equality condition on the partition key and an optional sort-key condition.",
        ));
    }

    let mut partition_value = None;
    let mut sort_condition = None;
    for clause in clauses {
        match &clause {
            ExpressionClause::Comparison {
                attribute_name,
                operator: ComparisonOperator::Equal,
                value,
            } if attribute_name == &schema.partition_attribute => {
                partition_value = Some(value.clone());
            }
            _ if schema.sort_attribute.as_deref().is_some_and(
                |sort_attribute| {
                    clause_attribute_name(&clause) == sort_attribute
                },
            ) =>
            {
                sort_condition = Some(clause);
            }
            _ => {
                return Err(validation_error(
                    "Query key condition did not match the table or index key schema.",
                ));
            }
        }
    }
    let Some(partition_value) = partition_value else {
        return Err(validation_error(
            "Query requires an equality condition on the partition key.",
        ));
    };

    Ok(QueryKeyCondition {
        partition_attribute: schema.partition_attribute,
        partition_value,
        sort_condition,
    })
}

fn parse_expression_clauses(
    expression: Option<&str>,
    expression_attribute_names: &BTreeMap<String, String>,
    expression_attribute_values: &BTreeMap<String, AttributeValue>,
) -> Result<Option<Vec<ExpressionClause>>, DynamoDbError> {
    let Some(expression) = expression else {
        return Ok(None);
    };
    let mut clauses = Vec::new();
    for clause in split_expression_clauses(expression) {
        clauses.push(parse_expression_clause(
            clause,
            expression_attribute_names,
            expression_attribute_values,
        )?);
    }
    Ok(Some(clauses))
}

fn split_expression_clauses(expression: &str) -> Vec<&str> {
    let mut clauses = Vec::new();
    let mut start = 0;
    let mut depth: usize = 0;
    let mut between_pending = false;
    let bytes = expression.as_bytes();
    let mut index = 0;
    while let Some(byte) = bytes.get(index) {
        match *byte {
            b'(' => depth = depth.saturating_add(1),
            b')' => depth = depth.saturating_sub(1),
            _ => {}
        }
        if depth == 0 && starts_with_keyword(expression, index, "BETWEEN") {
            between_pending = true;
            index = index.saturating_add("BETWEEN".len());
            continue;
        }
        if depth == 0 && starts_with_keyword(expression, index, "AND") {
            if between_pending {
                between_pending = false;
            } else {
                clauses.push(expression[start..index].trim());
                index = index.saturating_add("AND".len());
                start = index;
                continue;
            }
        }
        index = index.saturating_add(1);
    }
    clauses.push(expression[start..].trim());
    clauses.into_iter().filter(|clause| !clause.is_empty()).collect()
}

fn capture_group<'a>(
    captures: &'a Captures<'a>,
    name: &str,
    expression: &str,
) -> Result<&'a str, DynamoDbError> {
    captures.name(name).map(|capture| capture.as_str()).ok_or_else(|| {
        validation_error(format!(
            "Unsupported expression clause `{expression}`."
        ))
    })
}

fn starts_with_keyword(expression: &str, index: usize, keyword: &str) -> bool {
    let end = index.saturating_add(keyword.len());
    if !expression
        .get(index..end)
        .is_some_and(|slice| slice.eq_ignore_ascii_case(keyword))
    {
        return false;
    }

    let before = expression[..index].chars().next_back();
    let after = expression[end..].chars().next();
    before
        .map(|character| {
            character.is_whitespace() || matches!(character, '(' | ')' | ',')
        })
        .unwrap_or(true)
        && after
            .map(|character| {
                character.is_whitespace()
                    || matches!(character, '(' | ')' | ',')
            })
            .unwrap_or(true)
}

fn parse_expression_clause(
    expression: &str,
    expression_attribute_names: &BTreeMap<String, String>,
    expression_attribute_values: &BTreeMap<String, AttributeValue>,
) -> Result<ExpressionClause, DynamoDbError> {
    if let Some(captures) = between_regex()?.captures(expression) {
        return Ok(ExpressionClause::Between {
            attribute_name: resolve_attribute_name(
                capture_group(&captures, "attribute", expression)?,
                expression_attribute_names,
            )?,
            lower: resolve_attribute_value(
                capture_group(&captures, "lower", expression)?,
                expression_attribute_values,
            )?
            .clone(),
            upper: resolve_attribute_value(
                capture_group(&captures, "upper", expression)?,
                expression_attribute_values,
            )?
            .clone(),
        });
    }
    if let Some(captures) = begins_with_regex()?.captures(expression) {
        return Ok(ExpressionClause::BeginsWith {
            attribute_name: resolve_attribute_name(
                capture_group(&captures, "attribute", expression)?,
                expression_attribute_names,
            )?,
            value: resolve_attribute_value(
                capture_group(&captures, "value", expression)?,
                expression_attribute_values,
            )?
            .clone(),
        });
    }
    if let Some(captures) = attribute_exists_regex()?.captures(expression) {
        return Ok(ExpressionClause::AttributeExists {
            attribute_name: resolve_attribute_name(
                capture_group(&captures, "attribute", expression)?,
                expression_attribute_names,
            )?,
            negated: capture_group(&captures, "kind", expression)?
                == "not_exists",
        });
    }
    if let Some(captures) = comparison_regex()?.captures(expression) {
        return Ok(ExpressionClause::Comparison {
            attribute_name: resolve_attribute_name(
                capture_group(&captures, "attribute", expression)?,
                expression_attribute_names,
            )?,
            operator: comparison_operator(capture_group(
                &captures, "operator", expression,
            )?)?,
            value: resolve_attribute_value(
                capture_group(&captures, "value", expression)?,
                expression_attribute_values,
            )?
            .clone(),
        });
    }

    Err(validation_error(format!(
        "Unsupported expression clause `{expression}`."
    )))
}

fn clause_attribute_name(clause: &ExpressionClause) -> &str {
    match clause {
        ExpressionClause::AttributeExists { attribute_name, .. }
        | ExpressionClause::BeginsWith { attribute_name, .. }
        | ExpressionClause::Between { attribute_name, .. }
        | ExpressionClause::Comparison { attribute_name, .. } => {
            attribute_name
        }
    }
}

fn comparison_operator(
    operator: &str,
) -> Result<ComparisonOperator, DynamoDbError> {
    match operator {
        "=" => Ok(ComparisonOperator::Equal),
        "<>" => Ok(ComparisonOperator::NotEqual),
        "<" => Ok(ComparisonOperator::LessThan),
        "<=" => Ok(ComparisonOperator::LessThanOrEqual),
        ">" => Ok(ComparisonOperator::GreaterThan),
        ">=" => Ok(ComparisonOperator::GreaterThanOrEqual),
        operator => Err(validation_error(format!(
            "Unsupported comparison operator `{operator}`."
        ))),
    }
}

fn resolve_attribute_name(
    token: &str,
    expression_attribute_names: &BTreeMap<String, String>,
) -> Result<String, DynamoDbError> {
    if token.starts_with('#') {
        expression_attribute_names.get(token).cloned().ok_or_else(|| {
            validation_error(format!(
                "Expression attribute name `{token}` was not provided."
            ))
        })
    } else {
        Ok(token.to_owned())
    }
}

fn resolve_attribute_value<'a>(
    token: &str,
    expression_attribute_values: &'a BTreeMap<String, AttributeValue>,
) -> Result<&'a AttributeValue, DynamoDbError> {
    expression_attribute_values.get(token).ok_or_else(|| {
        validation_error(format!(
            "Expression attribute value `{token}` was not provided."
        ))
    })
}

fn evaluate_condition_expression(
    item: Option<&Item>,
    expression: Option<&str>,
    expression_attribute_names: &BTreeMap<String, String>,
    expression_attribute_values: &BTreeMap<String, AttributeValue>,
) -> Result<(), DynamoDbError> {
    let Some(clauses) = parse_expression_clauses(
        expression,
        expression_attribute_names,
        expression_attribute_values,
    )?
    else {
        return Ok(());
    };
    let empty = Item::new();
    let item = item.unwrap_or(&empty);
    if evaluate_expression_clauses(item, Some(&clauses))? {
        Ok(())
    } else {
        Err(DynamoDbError::ConditionalCheckFailed)
    }
}

fn evaluate_expression_clauses(
    item: &Item,
    clauses: Option<&Vec<ExpressionClause>>,
) -> Result<bool, DynamoDbError> {
    let Some(clauses) = clauses else {
        return Ok(true);
    };
    for clause in clauses {
        if !evaluate_expression_clause(item, clause)? {
            return Ok(false);
        }
    }
    Ok(true)
}

fn evaluate_expression_clause(
    item: &Item,
    clause: &ExpressionClause,
) -> Result<bool, DynamoDbError> {
    match clause {
        ExpressionClause::AttributeExists { attribute_name, negated } => {
            Ok(item.contains_key(attribute_name) != *negated)
        }
        ExpressionClause::BeginsWith { attribute_name, value } => Ok(item
            .get(attribute_name)
            .and_then(|attribute| attribute.starts_with(value))
            .unwrap_or(false)),
        ExpressionClause::Between { attribute_name, lower, upper } => item
            .get(attribute_name)
            .map(|attribute| compare_between(attribute, lower, upper))
            .transpose()
            .map(|matches| matches.unwrap_or(false)),
        ExpressionClause::Comparison { attribute_name, operator, value } => {
            Ok(item.get(attribute_name).is_some_and(|attribute| {
                compare_attribute(attribute, operator, value)
            }))
        }
    }
}

fn compare_between(
    attribute: &AttributeValue,
    lower: &AttributeValue,
    upper: &AttributeValue,
) -> Result<bool, DynamoDbError> {
    let lower_cmp = compare_attribute_values(attribute, lower)?;
    let upper_cmp = compare_attribute_values(attribute, upper)?;
    Ok(matches!(lower_cmp, Ordering::Equal | Ordering::Greater)
        && matches!(upper_cmp, Ordering::Equal | Ordering::Less))
}

fn compare_attribute(
    left: &AttributeValue,
    operator: &ComparisonOperator,
    right: &AttributeValue,
) -> bool {
    match operator {
        ComparisonOperator::Equal => left == right,
        ComparisonOperator::NotEqual => left != right,
        ComparisonOperator::GreaterThan => {
            compare_attribute_values(left, right)
                .is_ok_and(|ordering| ordering == Ordering::Greater)
        }
        ComparisonOperator::GreaterThanOrEqual => {
            compare_attribute_values(left, right).is_ok_and(|ordering| {
                matches!(ordering, Ordering::Equal | Ordering::Greater)
            })
        }
        ComparisonOperator::LessThan => compare_attribute_values(left, right)
            .is_ok_and(|ordering| ordering == Ordering::Less),
        ComparisonOperator::LessThanOrEqual => {
            compare_attribute_values(left, right).is_ok_and(|ordering| {
                matches!(ordering, Ordering::Equal | Ordering::Less)
            })
        }
    }
}

fn compare_attribute_values(
    left: &AttributeValue,
    right: &AttributeValue,
) -> Result<Ordering, DynamoDbError> {
    match (left, right) {
        (AttributeValue::Binary(left), AttributeValue::Binary(right)) => {
            Ok(left.cmp(right))
        }
        (AttributeValue::Number(left), AttributeValue::Number(right)) => {
            let left = left.parse::<f64>().map_err(|_| {
                validation_error(format!(
                    "Invalid numeric comparison value `{left}`."
                ))
            })?;
            let right = right.parse::<f64>().map_err(|_| {
                validation_error(format!(
                    "Invalid numeric comparison value `{right}`."
                ))
            })?;
            Ok(left.partial_cmp(&right).unwrap_or(Ordering::Equal))
        }
        (AttributeValue::String(left), AttributeValue::String(right)) => {
            Ok(left.cmp(right))
        }
        _ => Err(validation_error(
            "Expression comparisons require matching comparable attribute types.",
        )),
    }
}

fn query_key_matches(
    condition: &QueryKeyCondition,
    item: &Item,
) -> Result<bool, DynamoDbError> {
    let Some(partition) = item.get(&condition.partition_attribute) else {
        return Ok(false);
    };
    if partition != &condition.partition_value {
        return Ok(false);
    }
    let Some(sort_condition) = condition.sort_condition.as_ref() else {
        return Ok(true);
    };

    evaluate_expression_clause(item, sort_condition)
}

fn exclusive_start_index(
    entries: &[(ItemStorageKey, Item)],
    exclusive_start_key: Option<&ItemStorageKey>,
) -> Option<usize> {
    let exclusive_start_key = exclusive_start_key?;
    entries
        .iter()
        .position(|(key, _)| key == exclusive_start_key)
        .map(|index| index.saturating_add(1))
}

fn sort_entries(
    table: &StoredTable,
    key_schema: &[KeySchemaElement],
    entries: &mut [(ItemStorageKey, Item)],
) {
    let attribute_types = attribute_type_lookup(&table.attribute_definitions)
        .unwrap_or_default();
    let schema = normalized_key_schema(key_schema).ok();
    entries.sort_by(|(_, left), (_, right)| {
        let Some(schema) = schema.as_ref() else {
            return Ordering::Equal;
        };
        let partition = compare_key_component(
            left.get(&schema.partition_attribute),
            right.get(&schema.partition_attribute),
            attribute_types.get(&schema.partition_attribute).copied(),
        );
        if partition != Ordering::Equal {
            return partition;
        }
        if let Some(sort_attribute) = schema.sort_attribute.as_deref() {
            let sort = compare_key_component(
                left.get(sort_attribute),
                right.get(sort_attribute),
                attribute_types.get(sort_attribute).copied(),
            );
            if sort != Ordering::Equal {
                return sort;
            }
        }

        Ordering::Equal
    });
}

fn compare_key_component(
    left: Option<&AttributeValue>,
    right: Option<&AttributeValue>,
    attribute_type: Option<ScalarAttributeType>,
) -> Ordering {
    match (left, right, attribute_type) {
        (Some(left), Some(right), Some(_)) => {
            compare_attribute_values(left, right).unwrap_or(Ordering::Equal)
        }
        (None, None, _) => Ordering::Equal,
        (None, Some(_), _) => Ordering::Less,
        (Some(_), None, _) => Ordering::Greater,
        _ => Ordering::Equal,
    }
}

fn key_item_from_storage_key(
    table: &StoredTable,
    storage_key: &ItemStorageKey,
) -> Result<Item, DynamoDbError> {
    let schema = normalized_key_schema(&table.key_schema)?;
    let mut key = Item::new();
    key.insert(schema.partition_attribute, storage_key.partition_key.clone());
    if let Some(sort_attribute) = schema.sort_attribute
        && let Some(sort_key) = storage_key.sort_key.clone()
    {
        key.insert(sort_attribute, sort_key);
    }
    Ok(key)
}

fn apply_update_expression(
    item: &mut Item,
    expression: &str,
    expression_attribute_names: &BTreeMap<String, String>,
    expression_attribute_values: &BTreeMap<String, AttributeValue>,
) -> Result<UpdateResult, DynamoDbError> {
    let mut result = UpdateResult::default();
    let expression = expression.trim();
    if let Some(assignments) = expression.strip_prefix("SET ") {
        for assignment in assignments.split(',') {
            let (attribute_name, value_token) =
                assignment.split_once('=').ok_or_else(|| {
                    validation_error(format!(
                        "Unsupported update assignment `{assignment}`."
                    ))
                })?;
            let attribute_name = resolve_attribute_name(
                attribute_name.trim(),
                expression_attribute_names,
            )?;
            let value = resolve_attribute_value(
                value_token.trim(),
                expression_attribute_values,
            )?
            .clone();
            item.insert(attribute_name.clone(), value);
            result.changed_attributes.insert(attribute_name);
        }
        return Ok(result);
    }

    Err(validation_error(format!(
        "Unsupported UpdateExpression `{expression}`."
    )))
}

fn apply_attribute_updates(
    item: &mut Item,
    updates: &BTreeMap<String, AttributeUpdate>,
) -> Result<UpdateResult, DynamoDbError> {
    let mut result = UpdateResult::default();
    for (attribute_name, update) in updates {
        match update.action {
            AttributeUpdateAction::Delete => {
                item.remove(attribute_name);
            }
            AttributeUpdateAction::Put => {
                let Some(value) = update.value.clone() else {
                    return Err(validation_error(format!(
                        "AttributeUpdates for `{attribute_name}` requires a Value."
                    )));
                };
                item.insert(attribute_name.clone(), value);
            }
        }
        result.changed_attributes.insert(attribute_name.clone());
    }
    Ok(result)
}

fn update_item_return_values(
    existing: Option<&Item>,
    current: &Item,
    changed_attributes: &BTreeSet<String>,
    return_values: ReturnValues,
) -> Option<Item> {
    match return_values {
        ReturnValues::AllNew => Some(current.clone()),
        ReturnValues::AllOld => existing.cloned(),
        ReturnValues::None => None,
        ReturnValues::UpdatedNew => Some(
            changed_attributes
                .iter()
                .filter_map(|name| {
                    current
                        .get(name)
                        .cloned()
                        .map(|value| (name.clone(), value))
                })
                .collect(),
        ),
        ReturnValues::UpdatedOld => Some(
            changed_attributes
                .iter()
                .filter_map(|name| {
                    existing
                        .and_then(|existing| existing.get(name))
                        .cloned()
                        .map(|value| (name.clone(), value))
                })
                .collect(),
        ),
    }
}

fn storage_error(
    context: &'static str,
    source: StorageError,
) -> DynamoDbError {
    DynamoDbError::Storage { context, source }
}

fn table_not_found(table_name: &TableName) -> DynamoDbError {
    DynamoDbError::TableNotFound { table_name: table_name.clone() }
}

fn resource_not_found(resource: &str) -> DynamoDbError {
    DynamoDbError::ResourceNotFound { resource: resource.to_owned() }
}

fn validation_error(message: impl Into<String>) -> DynamoDbError {
    DynamoDbError::Validation { message: message.into() }
}

fn comparison_regex() -> Result<&'static Regex, DynamoDbError> {
    static REGEX: OnceLock<Result<Regex, String>> = OnceLock::new();
    match REGEX.get_or_init(|| {
        Regex::new(
            r"^\s*(?P<attribute>[#A-Za-z0-9_\.]+)\s*(?P<operator>=|<>|<=|<|>=|>)\s*(?P<value>:[A-Za-z0-9_]+)\s*$",
        )
        .map_err(|error| error.to_string())
    }) {
        Ok(regex) => Ok(regex),
        Err(message) => Err(validation_error(message.clone())),
    }
}

fn begins_with_regex() -> Result<&'static Regex, DynamoDbError> {
    static REGEX: OnceLock<Result<Regex, String>> = OnceLock::new();
    match REGEX.get_or_init(|| {
        Regex::new(
            r"^\s*begins_with\s*\(\s*(?P<attribute>[#A-Za-z0-9_\.]+)\s*,\s*(?P<value>:[A-Za-z0-9_]+)\s*\)\s*$",
        )
        .map_err(|error| error.to_string())
    }) {
        Ok(regex) => Ok(regex),
        Err(message) => Err(validation_error(message.clone())),
    }
}

fn between_regex() -> Result<&'static Regex, DynamoDbError> {
    static REGEX: OnceLock<Result<Regex, String>> = OnceLock::new();
    match REGEX.get_or_init(|| {
        Regex::new(
            r"^\s*(?P<attribute>[#A-Za-z0-9_\.]+)\s+BETWEEN\s+(?P<lower>:[A-Za-z0-9_]+)\s+AND\s+(?P<upper>:[A-Za-z0-9_]+)\s*$",
        )
        .map_err(|error| error.to_string())
    }) {
        Ok(regex) => Ok(regex),
        Err(message) => Err(validation_error(message.clone())),
    }
}

fn attribute_exists_regex() -> Result<&'static Regex, DynamoDbError> {
    static REGEX: OnceLock<Result<Regex, String>> = OnceLock::new();
    match REGEX.get_or_init(|| {
        Regex::new(
            r"^\s*attribute_(?P<kind>exists|not_exists)\s*\(\s*(?P<attribute>[#A-Za-z0-9_\.]+)\s*\)\s*$",
        )
        .map_err(|error| error.to_string())
    }) {
        Ok(regex) => Ok(regex),
        Err(message) => Err(validation_error(message.clone())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
    use storage::{StorageConfig, StorageFactory, StorageMode};

    fn service() -> DynamoDbService {
        let factory = StorageFactory::new(StorageConfig::new(
            "/tmp/cloudish-dynamodb-service-tests",
            StorageMode::Memory,
        ));

        DynamoDbService::with_stores(
            factory.create("dynamodb", "tables"),
            factory.create("dynamodb", "items"),
            factory.create("dynamodb", "stream-records"),
            Arc::new(|| 1_763_320_000),
        )
        .expect("dynamodb service should build")
    }

    fn service_with_time(time: Arc<AtomicU64>) -> DynamoDbService {
        let factory = StorageFactory::new(StorageConfig::new(
            "/tmp/cloudish-dynamodb-service-tests",
            StorageMode::Memory,
        ));

        DynamoDbService::with_stores(
            factory.create("dynamodb", "tables"),
            factory.create("dynamodb", "items"),
            factory.create("dynamodb", "stream-records"),
            Arc::new(move || time.load(AtomicOrdering::SeqCst)),
        )
        .expect("dynamodb service should build")
    }

    fn scope() -> DynamoDbScope {
        DynamoDbScope::new(
            "000000000000".parse().expect("account should parse"),
            "eu-west-2".parse().expect("region should parse"),
        )
    }

    fn string(value: &str) -> AttributeValue {
        AttributeValue::String(value.to_owned())
    }

    fn number(value: &str) -> AttributeValue {
        AttributeValue::Number(value.to_owned())
    }

    fn table_name(value: &str) -> TableName {
        TableName::parse(value).expect("table name should parse")
    }

    fn table_map<V, const N: usize>(
        entries: [(&str, V); N],
    ) -> BTreeMap<TableName, V> {
        entries
            .into_iter()
            .map(|(name, value)| (table_name(name), value))
            .collect()
    }

    fn table_input(
        name: &str,
        key_schema: Vec<KeySchemaElement>,
        attribute_definitions: Vec<AttributeDefinition>,
    ) -> CreateTableInput {
        CreateTableInput {
            attribute_definitions,
            billing_mode: Some(BillingMode::PayPerRequest),
            global_secondary_indexes: Vec::new(),
            key_schema,
            local_secondary_indexes: Vec::new(),
            provisioned_throughput: None,
            table_name: table_name(name),
        }
    }

    #[test]
    fn dynamodb_core_create_table_rejects_duplicate_and_invalid_key_schema() {
        let service = service();
        let scope = scope();
        let invalid = service
            .create_table(
                &scope,
                CreateTableInput {
                    attribute_definitions: vec![AttributeDefinition {
                        attribute_name: "pk".to_owned(),
                        attribute_type: ScalarAttributeType::String,
                    }],
                    billing_mode: Some(BillingMode::PayPerRequest),
                    global_secondary_indexes: Vec::new(),
                    key_schema: vec![
                        KeySchemaElement {
                            attribute_name: "pk".to_owned(),
                            key_type: KeyType::Hash,
                        },
                        KeySchemaElement {
                            attribute_name: "sk".to_owned(),
                            key_type: KeyType::Hash,
                        },
                    ],
                    local_secondary_indexes: vec![
                        LocalSecondaryIndexDefinition {
                            index_name: "by-status".to_owned(),
                            key_schema: vec![
                                KeySchemaElement {
                                    attribute_name: "other".to_owned(),
                                    key_type: KeyType::Hash,
                                },
                                KeySchemaElement {
                                    attribute_name: "status".to_owned(),
                                    key_type: KeyType::Range,
                                },
                            ],
                            projection: Projection {
                                non_key_attributes: Vec::new(),
                                projection_type: ProjectionType::All,
                            },
                        },
                    ],
                    provisioned_throughput: None,
                    table_name: table_name("orders"),
                },
            )
            .expect_err("invalid key schema should fail");
        assert!(matches!(invalid, DynamoDbError::Validation { .. }));

        let created = service
            .create_table(
                &scope,
                table_input(
                    "orders",
                    vec![
                        KeySchemaElement {
                            attribute_name: "tenant".to_owned(),
                            key_type: KeyType::Hash,
                        },
                        KeySchemaElement {
                            attribute_name: "order".to_owned(),
                            key_type: KeyType::Range,
                        },
                    ],
                    vec![
                        AttributeDefinition {
                            attribute_name: "tenant".to_owned(),
                            attribute_type: ScalarAttributeType::String,
                        },
                        AttributeDefinition {
                            attribute_name: "order".to_owned(),
                            attribute_type: ScalarAttributeType::String,
                        },
                    ],
                ),
            )
            .expect("table should create");
        assert_eq!(created.table_status, TableStatus::Active);

        let duplicate = service
            .create_table(
                &scope,
                table_input(
                    "orders",
                    vec![KeySchemaElement {
                        attribute_name: "tenant".to_owned(),
                        key_type: KeyType::Hash,
                    }],
                    vec![AttributeDefinition {
                        attribute_name: "tenant".to_owned(),
                        attribute_type: ScalarAttributeType::String,
                    }],
                ),
            )
            .expect_err("duplicate table should fail");
        assert!(matches!(duplicate, DynamoDbError::TableAlreadyExists { .. }));
    }

    #[test]
    fn dynamodb_core_item_crud_query_and_scan_preserve_key_semantics() {
        let service = service();
        let scope = scope();
        service
            .create_table(
                &scope,
                CreateTableInput {
                    attribute_definitions: vec![
                        AttributeDefinition {
                            attribute_name: "tenant".to_owned(),
                            attribute_type: ScalarAttributeType::String,
                        },
                        AttributeDefinition {
                            attribute_name: "order".to_owned(),
                            attribute_type: ScalarAttributeType::String,
                        },
                        AttributeDefinition {
                            attribute_name: "status".to_owned(),
                            attribute_type: ScalarAttributeType::String,
                        },
                    ],
                    billing_mode: Some(BillingMode::PayPerRequest),
                    global_secondary_indexes: vec![
                        GlobalSecondaryIndexDefinition {
                            index_name: "status-index".to_owned(),
                            key_schema: vec![KeySchemaElement {
                                attribute_name: "status".to_owned(),
                                key_type: KeyType::Hash,
                            }],
                            projection: Projection {
                                non_key_attributes: Vec::new(),
                                projection_type: ProjectionType::All,
                            },
                            provisioned_throughput: None,
                        },
                    ],
                    key_schema: vec![
                        KeySchemaElement {
                            attribute_name: "tenant".to_owned(),
                            key_type: KeyType::Hash,
                        },
                        KeySchemaElement {
                            attribute_name: "order".to_owned(),
                            key_type: KeyType::Range,
                        },
                    ],
                    local_secondary_indexes: Vec::new(),
                    provisioned_throughput: None,
                    table_name: table_name("orders"),
                },
            )
            .expect("table should create");

        for (tenant, order, status, total) in [
            ("tenant-a", "001", "pending", "10"),
            ("tenant-a", "002", "shipped", "20"),
            ("tenant-b", "001", "pending", "30"),
        ] {
            service
                .put_item(
                    &scope,
                    PutItemInput {
                        condition_expression: None,
                        expression_attribute_names: BTreeMap::new(),
                        expression_attribute_values: BTreeMap::new(),
                        item: BTreeMap::from([
                            ("tenant".to_owned(), string(tenant)),
                            ("order".to_owned(), string(order)),
                            ("status".to_owned(), string(status)),
                            ("total".to_owned(), number(total)),
                        ]),
                        return_values: ReturnValues::None,
                        table_name: table_name("orders"),
                    },
                )
                .expect("item should write");
        }

        let fetched = service
            .get_item(
                &scope,
                GetItemInput {
                    key: BTreeMap::from([
                        ("tenant".to_owned(), string("tenant-a")),
                        ("order".to_owned(), string("001")),
                    ]),
                    table_name: table_name("orders"),
                },
            )
            .expect("get item should succeed");
        assert_eq!(
            fetched.item.as_ref().and_then(|item| item.get("status")),
            Some(&string("pending"))
        );

        let queried = service
            .query(
                &scope,
                QueryInput {
                    exclusive_start_key: None,
                    expression_attribute_names: BTreeMap::new(),
                    expression_attribute_values: BTreeMap::from([
                        (":tenant".to_owned(), string("tenant-a")),
                        (":prefix".to_owned(), string("00")),
                    ]),
                    filter_expression: None,
                    index_name: None,
                    key_condition_expression:
                        "tenant = :tenant AND begins_with(order, :prefix)"
                            .to_owned(),
                    limit: None,
                    scan_index_forward: true,
                    table_name: table_name("orders"),
                },
            )
            .expect("query should succeed");
        assert_eq!(queried.count, 2);
        assert_eq!(queried.scanned_count, 2);

        let indexed = service
            .query(
                &scope,
                QueryInput {
                    exclusive_start_key: None,
                    expression_attribute_names: BTreeMap::new(),
                    expression_attribute_values: BTreeMap::from([(
                        ":status".to_owned(),
                        string("pending"),
                    )]),
                    filter_expression: None,
                    index_name: Some("status-index".to_owned()),
                    key_condition_expression: "status = :status".to_owned(),
                    limit: None,
                    scan_index_forward: true,
                    table_name: table_name("orders"),
                },
            )
            .expect("index query should succeed");
        assert_eq!(indexed.count, 2);

        let paged_scan = service
            .scan(
                &scope,
                ScanInput {
                    exclusive_start_key: None,
                    expression_attribute_names: BTreeMap::new(),
                    expression_attribute_values: BTreeMap::from([
                        (":low".to_owned(), number("15")),
                        (":high".to_owned(), number("40")),
                    ]),
                    filter_expression: Some(
                        "total BETWEEN :low AND :high".to_owned(),
                    ),
                    index_name: None,
                    limit: Some(2),
                    table_name: table_name("orders"),
                },
            )
            .expect("scan should succeed");
        assert_eq!(paged_scan.scanned_count, 2);
        assert_eq!(paged_scan.count, 1);
        assert!(paged_scan.last_evaluated_key.is_some());

        let deleted = service
            .delete_item(
                &scope,
                DeleteItemInput {
                    condition_expression: Some(
                        "attribute_exists(tenant)".to_owned(),
                    ),
                    expression_attribute_names: BTreeMap::new(),
                    expression_attribute_values: BTreeMap::new(),
                    key: BTreeMap::from([
                        ("tenant".to_owned(), string("tenant-b")),
                        ("order".to_owned(), string("001")),
                    ]),
                    return_values: ReturnValues::AllOld,
                    table_name: table_name("orders"),
                },
            )
            .expect("delete item should succeed");
        assert_eq!(
            deleted.attributes.as_ref().and_then(|item| item.get("status")),
            Some(&string("pending"))
        );

        let invalid_index = service
            .query(
                &scope,
                QueryInput {
                    exclusive_start_key: None,
                    expression_attribute_names: BTreeMap::new(),
                    expression_attribute_values: BTreeMap::from([(
                        ":status".to_owned(),
                        string("pending"),
                    )]),
                    filter_expression: None,
                    index_name: Some("missing-index".to_owned()),
                    key_condition_expression: "status = :status".to_owned(),
                    limit: None,
                    scan_index_forward: true,
                    table_name: table_name("orders"),
                },
            )
            .expect_err("missing index should fail");
        assert!(matches!(invalid_index, DynamoDbError::Validation { .. }));
    }

    #[test]
    fn dynamodb_core_conditional_put_and_update_do_not_mutate_state() {
        let service = service();
        let scope = scope();
        service
            .create_table(
                &scope,
                table_input(
                    "users",
                    vec![KeySchemaElement {
                        attribute_name: "id".to_owned(),
                        key_type: KeyType::Hash,
                    }],
                    vec![AttributeDefinition {
                        attribute_name: "id".to_owned(),
                        attribute_type: ScalarAttributeType::String,
                    }],
                ),
            )
            .expect("table should create");
        service
            .put_item(
                &scope,
                PutItemInput {
                    condition_expression: None,
                    expression_attribute_names: BTreeMap::new(),
                    expression_attribute_values: BTreeMap::new(),
                    item: BTreeMap::from([
                        ("id".to_owned(), string("user-1")),
                        ("version".to_owned(), number("1")),
                    ]),
                    return_values: ReturnValues::None,
                    table_name: table_name("users"),
                },
            )
            .expect("initial item should write");

        let conditional_put = service
            .put_item(
                &scope,
                PutItemInput {
                    condition_expression: Some(
                        "attribute_not_exists(id)".to_owned(),
                    ),
                    expression_attribute_names: BTreeMap::new(),
                    expression_attribute_values: BTreeMap::new(),
                    item: BTreeMap::from([
                        ("id".to_owned(), string("user-1")),
                        ("version".to_owned(), number("2")),
                    ]),
                    return_values: ReturnValues::None,
                    table_name: table_name("users"),
                },
            )
            .expect_err("conditional put should fail");
        assert!(matches!(
            conditional_put,
            DynamoDbError::ConditionalCheckFailed
        ));

        let updated = service
            .update_item(
                &scope,
                UpdateItemInput {
                    attribute_updates: BTreeMap::new(),
                    condition_expression: Some(
                        "version = :expected".to_owned(),
                    ),
                    expression_attribute_names: BTreeMap::new(),
                    expression_attribute_values: BTreeMap::from([
                        (":expected".to_owned(), number("1")),
                        (":next".to_owned(), number("2")),
                        (":name".to_owned(), string("alice")),
                    ]),
                    key: BTreeMap::from([("id".to_owned(), string("user-1"))]),
                    return_values: ReturnValues::AllNew,
                    table_name: table_name("users"),
                    update_expression: Some(
                        "SET version = :next, name = :name".to_owned(),
                    ),
                },
            )
            .expect("conditional update should succeed");
        assert_eq!(
            updated.attributes.as_ref().and_then(|item| item.get("version")),
            Some(&number("2"))
        );

        let failed_update = service
            .update_item(
                &scope,
                UpdateItemInput {
                    attribute_updates: BTreeMap::from([(
                        "name".to_owned(),
                        AttributeUpdate {
                            action: AttributeUpdateAction::Put,
                            value: Some(string("bob")),
                        },
                    )]),
                    condition_expression: Some(
                        "version = :expected".to_owned(),
                    ),
                    expression_attribute_names: BTreeMap::new(),
                    expression_attribute_values: BTreeMap::from([(
                        ":expected".to_owned(),
                        number("7"),
                    )]),
                    key: BTreeMap::from([("id".to_owned(), string("user-1"))]),
                    return_values: ReturnValues::None,
                    table_name: table_name("users"),
                    update_expression: None,
                },
            )
            .expect_err("failed condition should reject update");
        assert!(matches!(
            failed_update,
            DynamoDbError::ConditionalCheckFailed
        ));

        let stored = service
            .get_item(
                &scope,
                GetItemInput {
                    key: BTreeMap::from([("id".to_owned(), string("user-1"))]),
                    table_name: table_name("users"),
                },
            )
            .expect("get item should succeed");
        let stored = stored.item.expect("item should remain");
        assert_eq!(stored.get("version"), Some(&number("2")));
        assert_eq!(stored.get("name"), Some(&string("alice")));
    }

    #[test]
    fn dynamodb_core_batch_get_write_and_update_table_round_trip() {
        let service = service();
        let scope = scope();
        service
            .create_table(
                &scope,
                table_input(
                    "inventory",
                    vec![KeySchemaElement {
                        attribute_name: "id".to_owned(),
                        key_type: KeyType::Hash,
                    }],
                    vec![AttributeDefinition {
                        attribute_name: "id".to_owned(),
                        attribute_type: ScalarAttributeType::String,
                    }],
                ),
            )
            .expect("table should create");

        service
            .batch_write_item(
                &scope,
                BatchWriteItemInput {
                    request_items: table_map([(
                        "inventory",
                        vec![
                            BatchWriteRequest::Put {
                                item: BTreeMap::from([
                                    ("id".to_owned(), string("a")),
                                    ("stock".to_owned(), number("2")),
                                ]),
                            },
                            BatchWriteRequest::Put {
                                item: BTreeMap::from([
                                    ("id".to_owned(), string("b")),
                                    ("stock".to_owned(), number("3")),
                                ]),
                            },
                        ],
                    )]),
                },
            )
            .expect("batch write should succeed");

        let fetched = service
            .batch_get_item(
                &scope,
                BatchGetItemInput {
                    request_items: table_map([(
                        "inventory",
                        KeysAndAttributes {
                            keys: vec![
                                BTreeMap::from([(
                                    "id".to_owned(),
                                    string("a"),
                                )]),
                                BTreeMap::from([(
                                    "id".to_owned(),
                                    string("b"),
                                )]),
                                BTreeMap::from([(
                                    "id".to_owned(),
                                    string("missing"),
                                )]),
                            ],
                        },
                    )]),
                },
            )
            .expect("batch get should succeed");
        assert_eq!(fetched.responses["inventory"].len(), 2);

        let updated = service
            .update_table(
                &scope,
                UpdateTableInput {
                    billing_mode: Some(BillingMode::Provisioned),
                    provisioned_throughput: Some(ProvisionedThroughput {
                        read_capacity_units: 4,
                        write_capacity_units: 8,
                    }),
                    stream_specification: None,
                    table_name: table_name("inventory"),
                },
            )
            .expect("update table should succeed");
        assert_eq!(updated.billing_mode, BillingMode::Provisioned);
        assert_eq!(
            updated.provisioned_throughput,
            Some(ProvisionedThroughput {
                read_capacity_units: 4,
                write_capacity_units: 8,
            })
        );

        let duplicate_batch = service
            .batch_write_item(
                &scope,
                BatchWriteItemInput {
                    request_items: table_map([(
                        "inventory",
                        vec![
                            BatchWriteRequest::Delete {
                                key: BTreeMap::from([(
                                    "id".to_owned(),
                                    string("a"),
                                )]),
                            },
                            BatchWriteRequest::Put {
                                item: BTreeMap::from([
                                    ("id".to_owned(), string("a")),
                                    ("stock".to_owned(), number("9")),
                                ]),
                            },
                        ],
                    )]),
                },
            )
            .expect_err("duplicate batch key should fail");
        assert!(matches!(duplicate_batch, DynamoDbError::Validation { .. }));
    }

    #[test]
    fn dynamodb_core_return_values_and_expression_helpers_cover_supported_shapes()
     {
        assert_eq!(
            ReturnValues::parse(Some("UPDATED_NEW"))
                .expect("updated new should parse"),
            ReturnValues::UpdatedNew
        );
        assert!(
            split_expression_clauses(
                "tenant = :tenant AND order BETWEEN :low AND :high"
            )
            .iter()
            .any(|clause| clause.contains("BETWEEN"))
        );
        assert!(matches!(
            parse_expression_clause(
                "attribute_not_exists(id)",
                &BTreeMap::new(),
                &BTreeMap::new(),
            )
            .expect("clause should parse"),
            ExpressionClause::AttributeExists { negated: true, .. }
        ));
    }

    #[test]
    fn dynamodb_transactions_transact_write_commits_and_emits_stream_records()
    {
        let service = service();
        let scope = scope();
        service
            .create_table(
                &scope,
                table_input(
                    "orders",
                    vec![KeySchemaElement {
                        attribute_name: "id".to_owned(),
                        key_type: KeyType::Hash,
                    }],
                    vec![AttributeDefinition {
                        attribute_name: "id".to_owned(),
                        attribute_type: ScalarAttributeType::String,
                    }],
                ),
            )
            .expect("table should create");
        let table = service
            .update_table(
                &scope,
                UpdateTableInput {
                    billing_mode: None,
                    provisioned_throughput: None,
                    stream_specification: Some(StreamSpecification {
                        stream_enabled: true,
                        stream_view_type: Some(
                            StreamViewType::NewAndOldImages,
                        ),
                    }),
                    table_name: table_name("orders"),
                },
            )
            .expect("stream should enable");

        service
            .transact_write_items(
                &scope,
                TransactWriteItemsInput {
                    transact_items: vec![
                        TransactWriteItem::Put(TransactPutItem {
                            condition_expression: Some(
                                "attribute_not_exists(id)".to_owned(),
                            ),
                            expression_attribute_names: BTreeMap::new(),
                            expression_attribute_values: BTreeMap::new(),
                            item: BTreeMap::from([
                                ("id".to_owned(), string("order-1")),
                                ("status".to_owned(), string("queued")),
                            ]),
                            table_name: table_name("orders"),
                        }),
                        TransactWriteItem::Put(TransactPutItem {
                            condition_expression: None,
                            expression_attribute_names: BTreeMap::new(),
                            expression_attribute_values: BTreeMap::new(),
                            item: BTreeMap::from([
                                ("id".to_owned(), string("order-2")),
                                ("status".to_owned(), string("queued")),
                            ]),
                            table_name: table_name("orders"),
                        }),
                    ],
                },
            )
            .expect("transaction should commit");

        let fetched = service
            .transact_get_items(
                &scope,
                TransactGetItemsInput {
                    transact_items: vec![
                        TransactGetItem {
                            key: BTreeMap::from([(
                                "id".to_owned(),
                                string("order-1"),
                            )]),
                            table_name: table_name("orders"),
                        },
                        TransactGetItem {
                            key: BTreeMap::from([(
                                "id".to_owned(),
                                string("order-2"),
                            )]),
                            table_name: table_name("orders"),
                        },
                    ],
                },
            )
            .expect("transactional get should succeed");
        assert_eq!(fetched.responses.len(), 2);
        assert_eq!(
            fetched.responses[0]
                .item
                .as_ref()
                .and_then(|item| item.get("status")),
            Some(&string("queued"))
        );

        let streams = service
            .list_streams(
                &scope,
                ListStreamsInput {
                    exclusive_start_stream_arn: None,
                    limit: None,
                    table_name: Some(table_name("orders")),
                },
            )
            .expect("streams should list");
        assert_eq!(streams.streams.len(), 1);
        assert_eq!(
            streams.streams[0].stream_arn,
            table.latest_stream_arn.expect("stream ARN should be present")
        );

        let description = service
            .describe_stream(
                &scope,
                DescribeStreamInput {
                    exclusive_start_shard_id: None,
                    limit: None,
                    stream_arn: streams.streams[0].stream_arn.clone(),
                },
            )
            .expect("stream should describe");
        assert_eq!(description.shards.len(), 1);

        let iterator = service
            .get_shard_iterator(
                &scope,
                GetShardIteratorInput {
                    sequence_number: None,
                    shard_id: description.shards[0].shard_id.clone(),
                    shard_iterator_type: ShardIteratorType::TrimHorizon,
                    stream_arn: description.stream_arn.clone(),
                },
            )
            .expect("iterator should build");
        let records = service
            .get_records(
                &scope,
                GetRecordsInput {
                    limit: Some(10),
                    shard_iterator: iterator.shard_iterator,
                },
            )
            .expect("records should load");
        assert_eq!(records.records.len(), 2);
        assert_eq!(records.records[0].event_name, "INSERT");
        assert_eq!(
            records.records[0]
                .dynamodb
                .new_image
                .as_ref()
                .and_then(|item| item.get("id")),
            Some(&string("order-1"))
        );
    }

    #[test]
    fn dynamodb_transactions_failed_transaction_and_invalid_iterator_are_explicit()
     {
        let service = service();
        let scope = scope();
        service
            .create_table(
                &scope,
                table_input(
                    "users",
                    vec![KeySchemaElement {
                        attribute_name: "id".to_owned(),
                        key_type: KeyType::Hash,
                    }],
                    vec![AttributeDefinition {
                        attribute_name: "id".to_owned(),
                        attribute_type: ScalarAttributeType::String,
                    }],
                ),
            )
            .expect("table should create");
        service
            .put_item(
                &scope,
                PutItemInput {
                    condition_expression: None,
                    expression_attribute_names: BTreeMap::new(),
                    expression_attribute_values: BTreeMap::new(),
                    item: BTreeMap::from([
                        ("id".to_owned(), string("user-1")),
                        ("version".to_owned(), number("1")),
                    ]),
                    return_values: ReturnValues::None,
                    table_name: table_name("users"),
                },
            )
            .expect("seed item should write");

        let error = service
            .transact_write_items(
                &scope,
                TransactWriteItemsInput {
                    transact_items: vec![
                        TransactWriteItem::Put(TransactPutItem {
                            condition_expression: None,
                            expression_attribute_names: BTreeMap::new(),
                            expression_attribute_values: BTreeMap::new(),
                            item: BTreeMap::from([
                                ("id".to_owned(), string("user-2")),
                                ("version".to_owned(), number("1")),
                            ]),
                            table_name: table_name("users"),
                        }),
                        TransactWriteItem::ConditionCheck(
                            TransactConditionCheck {
                                condition_expression: "version = :expected"
                                    .to_owned(),
                                expression_attribute_names: BTreeMap::new(),
                                expression_attribute_values: BTreeMap::from([
                                    (":expected".to_owned(), number("7")),
                                ]),
                                key: BTreeMap::from([(
                                    "id".to_owned(),
                                    string("user-1"),
                                )]),
                                table_name: table_name("users"),
                            },
                        ),
                    ],
                },
            )
            .expect_err("transaction should cancel");
        let reasons = error
            .cancellation_reasons()
            .expect("cancellation reasons should be present");
        assert_eq!(reasons.len(), 2);
        assert_eq!(reasons[0].code, None);
        assert_eq!(reasons[1].code.as_deref(), Some("ConditionalCheckFailed"));

        let missing = service
            .get_item(
                &scope,
                GetItemInput {
                    key: BTreeMap::from([("id".to_owned(), string("user-2"))]),
                    table_name: table_name("users"),
                },
            )
            .expect("get item should succeed");
        assert!(missing.item.is_none());

        let invalid_iterator = service
            .get_records(
                &scope,
                GetRecordsInput {
                    limit: Some(1),
                    shard_iterator: "invalid".to_owned(),
                },
            )
            .expect_err("invalid iterators should fail");
        assert!(matches!(
            invalid_iterator,
            DynamoDbError::InvalidShardIterator
        ));
    }

    #[test]
    fn dynamodb_transactions_ttl_transitions_tags_and_expiry_are_clock_driven()
    {
        let time = Arc::new(AtomicU64::new(1_763_320_000));
        let service = service_with_time(Arc::clone(&time));
        let scope = scope();
        let table = service
            .create_table(
                &scope,
                table_input(
                    "sessions",
                    vec![KeySchemaElement {
                        attribute_name: "id".to_owned(),
                        key_type: KeyType::Hash,
                    }],
                    vec![AttributeDefinition {
                        attribute_name: "id".to_owned(),
                        attribute_type: ScalarAttributeType::String,
                    }],
                ),
            )
            .expect("table should create");
        service
            .update_table(
                &scope,
                UpdateTableInput {
                    billing_mode: None,
                    provisioned_throughput: None,
                    stream_specification: Some(StreamSpecification {
                        stream_enabled: true,
                        stream_view_type: Some(StreamViewType::OldImage),
                    }),
                    table_name: table_name("sessions"),
                },
            )
            .expect("stream should enable");

        let disabled = service
            .describe_time_to_live(&scope, "sessions")
            .expect("TTL should describe");
        assert_eq!(disabled.time_to_live_status, TimeToLiveStatus::Disabled);
        assert!(disabled.attribute_name.is_none());

        service
            .update_time_to_live(
                &scope,
                UpdateTimeToLiveInput {
                    table_name: "sessions".to_owned(),
                    time_to_live_specification: TimeToLiveSpecification {
                        attribute_name: "expires_at".to_owned(),
                        enabled: true,
                    },
                },
            )
            .expect("TTL should update");
        let enabling = service
            .describe_time_to_live(&scope, &table.table_arn)
            .expect("TTL should describe while enabling");
        assert_eq!(enabling.time_to_live_status, TimeToLiveStatus::Enabling);
        assert_eq!(enabling.attribute_name.as_deref(), Some("expires_at"));

        let pending = service
            .update_time_to_live(
                &scope,
                UpdateTimeToLiveInput {
                    table_name: "sessions".to_owned(),
                    time_to_live_specification: TimeToLiveSpecification {
                        attribute_name: "expires_at".to_owned(),
                        enabled: false,
                    },
                },
            )
            .expect_err("TTL transitions should be explicit");
        assert!(matches!(
            pending,
            DynamoDbError::TtlTransitionInProgress { .. }
        ));

        service
            .tag_resource(
                &scope,
                TagResourceInput {
                    resource_arn: table.table_arn.clone(),
                    tags: vec![
                        ResourceTag {
                            key: "env".to_owned(),
                            value: "test".to_owned(),
                        },
                        ResourceTag {
                            key: "team".to_owned(),
                            value: "ddb".to_owned(),
                        },
                    ],
                },
            )
            .expect("tags should apply");
        let tags = service
            .list_tags_of_resource(
                &scope,
                ListTagsOfResourceInput {
                    next_token: None,
                    resource_arn: table.table_arn.clone(),
                },
            )
            .expect("tags should list");
        assert_eq!(tags.tags.len(), 2);

        service
            .untag_resource(
                &scope,
                UntagResourceInput {
                    resource_arn: table.table_arn.clone(),
                    tag_keys: vec!["team".to_owned()],
                },
            )
            .expect("tags should remove");
        let tags = service
            .list_tags_of_resource(
                &scope,
                ListTagsOfResourceInput {
                    next_token: None,
                    resource_arn: table.table_arn,
                },
            )
            .expect("tags should list after removal");
        assert_eq!(tags.tags.len(), 1);
        assert_eq!(tags.tags[0].key, "env");

        time.store(
            1_763_320_000 + TTL_TRANSITION_SECONDS,
            AtomicOrdering::SeqCst,
        );
        let enabled = service
            .describe_time_to_live(&scope, "sessions")
            .expect("TTL should enable after the transition window");
        assert_eq!(enabled.time_to_live_status, TimeToLiveStatus::Enabled);

        service
            .put_item(
                &scope,
                PutItemInput {
                    condition_expression: None,
                    expression_attribute_names: BTreeMap::new(),
                    expression_attribute_values: BTreeMap::new(),
                    item: BTreeMap::from([
                        ("id".to_owned(), string("expired")),
                        (
                            "expires_at".to_owned(),
                            number(
                                &(time.load(AtomicOrdering::SeqCst) - 1)
                                    .to_string(),
                            ),
                        ),
                    ]),
                    return_values: ReturnValues::None,
                    table_name: table_name("sessions"),
                },
            )
            .expect("expired item should write");

        assert_eq!(
            service.purge_expired_items().expect("TTL purge should succeed"),
            1
        );
        let deleted = service
            .get_item(
                &scope,
                GetItemInput {
                    key: BTreeMap::from([(
                        "id".to_owned(),
                        string("expired"),
                    )]),
                    table_name: table_name("sessions"),
                },
            )
            .expect("deleted item should read as missing");
        assert!(deleted.item.is_none());

        let stream_arn = service
            .list_streams(
                &scope,
                ListStreamsInput {
                    exclusive_start_stream_arn: None,
                    limit: None,
                    table_name: Some(table_name("sessions")),
                },
            )
            .expect("stream should list")
            .streams[0]
            .stream_arn
            .clone();
        let iterator = service
            .get_shard_iterator(
                &scope,
                GetShardIteratorInput {
                    sequence_number: None,
                    shard_id: STREAM_SHARD_ID.to_owned(),
                    shard_iterator_type: ShardIteratorType::TrimHorizon,
                    stream_arn,
                },
            )
            .expect("stream iterator should build");
        let records = service
            .get_records(
                &scope,
                GetRecordsInput {
                    limit: Some(10),
                    shard_iterator: iterator.shard_iterator,
                },
            )
            .expect("stream records should load");
        assert_eq!(
            records
                .records
                .last()
                .expect("remove record should be present")
                .event_name,
            "REMOVE"
        );
    }
}
