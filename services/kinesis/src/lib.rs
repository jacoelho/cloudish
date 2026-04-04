mod errors;
mod identifiers;
mod iterators;
mod records;
mod scope;
mod shards;
mod streams;

pub use errors::KinesisError;
pub use identifiers::{
    KinesisStreamIdentifier, KinesisStreamName, validate_stream_name,
};
pub use iterators::{
    GetRecordsInput, GetRecordsOutput, GetShardIteratorInput,
    GetShardIteratorOutput, KinesisShardIteratorType,
};
pub use records::{
    KinesisRecord, PutRecordInput, PutRecordOutput, PutRecordsEntry,
    PutRecordsInput, PutRecordsOutput, PutRecordsResultEntry,
};
pub use scope::KinesisScope;
pub use shards::{
    KinesisHashKeyRange, KinesisSequenceNumberRange, KinesisShard,
    ListShardsInput, ListShardsOutput, MergeShardsInput, SplitShardInput,
};
pub use streams::{
    AddTagsToStreamInput, CreateStreamInput, DeleteStreamInput,
    DeregisterStreamConsumerInput, DescribeStreamConsumerInput,
    DescribeStreamInput, DescribeStreamSummaryInput, KinesisConsumer,
    KinesisConsumerStatus, KinesisEncryptionType, KinesisEnhancedMetrics,
    KinesisStreamDescription, KinesisStreamDescriptionSummary,
    KinesisStreamMode, KinesisStreamModeDetails, KinesisStreamStatus,
    KinesisTag, ListStreamConsumersInput, ListStreamConsumersOutput,
    ListStreamsInput, ListStreamsOutput, ListTagsForStreamInput,
    ListTagsForStreamOutput, RegisterStreamConsumerInput,
    RemoveTagsFromStreamInput, StartStreamEncryptionInput,
    StopStreamEncryptionInput,
};

use crate::errors::storage_error;
use crate::iterators::{
    IteratorToken, TOKEN_KIND_CONSUMERS, TOKEN_KIND_SHARDS,
    decode_index_token, decode_iterator_token, decode_scoped_token,
    encode_iterator_token, encode_scoped_token, iterator_position,
};
use crate::records::{effective_hash_key, validate_partition_key};
use crate::shards::{
    StoredShard, build_initial_shards, format_sequence_number,
    merge_hash_key_ranges, next_shard_id, public_shards,
    select_writable_shard, shard_start_index, split_hash_key_range,
};
use crate::streams::{
    ConsumerStorageKey, StoredStream, StreamStorageKey, stream_mode_details,
    stream_name_from_arn, validate_consumer_name,
};
use aws::{Arn, Clock};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::UNIX_EPOCH;
use storage::{StorageFactory, StorageHandle};

const DEFAULT_LIST_LIMIT: usize = 100;
const DEFAULT_LIST_SHARDS_LIMIT: usize = 1_000;
const DEFAULT_LIST_CONSUMERS_LIMIT: usize = 100;
const DEFAULT_LIST_TAGS_LIMIT: usize = 100;
const DEFAULT_GET_RECORDS_LIMIT: usize = 10_000;
const MAX_GET_RECORDS_LIMIT: usize = 10_000;
const MAX_PUT_RECORDS_COUNT: usize = 500;
const MAX_STREAM_TAGS: usize = 50;

#[derive(Clone)]
pub struct KinesisService {
    clock: Arc<dyn Clock>,
    consumer_store: StorageHandle<ConsumerStorageKey, KinesisConsumer>,
    mutation_lock: Arc<Mutex<()>>,
    stream_store: StorageHandle<StreamStorageKey, StoredStream>,
}

impl KinesisService {
    pub fn new(factory: &StorageFactory, clock: Arc<dyn Clock>) -> Self {
        Self {
            clock,
            consumer_store: factory.create("kinesis", "consumers"),
            mutation_lock: Arc::new(Mutex::new(())),
            stream_store: factory.create("kinesis", "streams"),
        }
    }

    /// # Errors
    ///
    /// Returns validation, conflict, shard-construction, or persistence errors
    /// when the stream request is invalid or cannot be stored.
    pub fn create_stream(
        &self,
        scope: &KinesisScope,
        input: CreateStreamInput,
    ) -> Result<(), KinesisError> {
        validate_positive("ShardCount", input.shard_count)?;

        let _guard = self.lock_state();
        let key = StreamStorageKey::new(scope, &input.stream_name);
        if self.stream_store.get(&key).is_some() {
            return Err(KinesisError::ResourceInUse {
                message: format!(
                    "Stream {} already exists.",
                    input.stream_name
                ),
            });
        }

        let next_sequence_number = 1;
        let starting_sequence_number =
            format_sequence_number(next_sequence_number);
        let shards = build_initial_shards(
            input.shard_count,
            starting_sequence_number.as_str(),
        )?;
        let stream = StoredStream {
            encryption_type: KinesisEncryptionType::None,
            key_id: None,
            next_sequence_number,
            next_shard_index: input.shard_count as u64,
            retention_period_hours: 24,
            shards,
            stream_creation_timestamp: current_epoch_seconds(&*self.clock),
            stream_name: input.stream_name,
            stream_status: KinesisStreamStatus::Active,
            tags: std::collections::BTreeMap::new(),
        };

        self.stream_store
            .put(key, stream)
            .map_err(|source| storage_error("writing stream state", source))
    }

    /// # Errors
    ///
    /// Returns not-found, in-use, or persistence errors when the stream does
    /// not exist or still has registered consumers.
    pub fn delete_stream(
        &self,
        scope: &KinesisScope,
        input: DeleteStreamInput,
    ) -> Result<(), KinesisError> {
        let _guard = self.lock_state();
        let key = self.stream_key_from_identifier(scope, &input.stream)?;
        let stream = self.load_stream(&key)?;
        let consumers = self.consumers_for_stream(scope, &key.stream_arn());
        if !consumers.is_empty()
            && !input.enforce_consumer_deletion.unwrap_or(false)
        {
            return Err(KinesisError::ResourceInUse {
                message: format!(
                    "Stream {} still has registered consumers.",
                    key.stream_name
                ),
            });
        }

        for consumer_key in
            self.consumer_store.keys().into_iter().filter(|consumer_key| {
                consumer_key.account_id == *scope.account_id()
                    && consumer_key.region == *scope.region()
            })
        {
            if self.consumer_store.get(&consumer_key).is_some_and(|consumer| {
                consumer.stream_arn.as_ref() == Some(&key.stream_arn())
            }) {
                self.consumer_store.delete(&consumer_key).map_err(
                    |source| storage_error("deleting consumer state", source),
                )?;
            }
        }

        let _ = stream;
        self.stream_store
            .delete(&key)
            .map_err(|source| storage_error("deleting stream state", source))
    }

    /// # Errors
    ///
    /// Returns validation or token errors when the requested page cannot be
    /// resolved.
    pub fn list_streams(
        &self,
        scope: &KinesisScope,
        input: ListStreamsInput,
    ) -> Result<ListStreamsOutput, KinesisError> {
        let stream_names = self.list_scoped_stream_names(scope);
        let page_size =
            normalize_page_size(input.limit, DEFAULT_LIST_LIMIT, "Limit")?;
        let start = if let Some(next_token) = input.next_token.as_deref() {
            if input.exclusive_start_stream_name.is_some() {
                return Err(KinesisError::InvalidArgument {
                    message:
                        "NextToken cannot be combined with ExclusiveStartStreamName."
                            .to_owned(),
                });
            }
            decode_index_token(next_token)?
        } else if let Some(exclusive_start) =
            input.exclusive_start_stream_name.as_ref()
        {
            stream_names
                .binary_search_by(|stream| {
                    stream.as_str().cmp(exclusive_start.as_str())
                })
                .map(|index| index.saturating_add(1))
                .unwrap_or_else(|index| index)
        } else {
            0
        };
        if start > stream_names.len() {
            return Err(KinesisError::ExpiredNextToken {
                message: "The provided NextToken is invalid.".to_owned(),
            });
        }

        let end = start.saturating_add(page_size).min(stream_names.len());
        let page = paged_items(&stream_names, start, end);

        Ok(ListStreamsOutput {
            has_more_streams: end < stream_names.len(),
            next_token: (end < stream_names.len()).then(|| end.to_string()),
            stream_names: page,
        })
    }

    /// # Errors
    ///
    /// Returns validation, not-found, or token errors when the requested shard
    /// page cannot be resolved.
    pub fn describe_stream(
        &self,
        scope: &KinesisScope,
        input: DescribeStreamInput,
    ) -> Result<KinesisStreamDescription, KinesisError> {
        let key = self.stream_key_from_identifier(scope, &input.stream)?;
        let stream = self.load_stream(&key)?;
        let shards = public_shards(&stream.shards);
        let page_size =
            normalize_page_size(input.limit, shards.len().max(1), "Limit")?;
        let start = shard_start_index(
            &shards,
            input.exclusive_start_shard_id.as_deref(),
        )?;
        let end = start.saturating_add(page_size).min(shards.len());

        Ok(KinesisStreamDescription {
            encryption_type: stream.encryption_type,
            enhanced_monitoring: Vec::new(),
            has_more_shards: end < shards.len(),
            key_id: stream.key_id,
            retention_period_hours: stream.retention_period_hours,
            shards: paged_items(&shards, start, end),
            stream_arn: key.stream_arn(),
            stream_creation_timestamp: stream.stream_creation_timestamp,
            stream_mode_details: stream_mode_details(),
            stream_name: stream.stream_name,
            stream_status: stream.stream_status,
        })
    }

    /// # Errors
    ///
    /// Returns validation or not-found errors when the stream identifier does
    /// not resolve to a stored stream.
    pub fn describe_stream_summary(
        &self,
        scope: &KinesisScope,
        input: DescribeStreamSummaryInput,
    ) -> Result<KinesisStreamDescriptionSummary, KinesisError> {
        let key = self.stream_key_from_identifier(scope, &input.stream)?;
        let stream = self.load_stream(&key)?;
        let consumers = self.consumers_for_stream(scope, &key.stream_arn());

        Ok(KinesisStreamDescriptionSummary {
            consumer_count: consumers.len() as u32,
            encryption_type: stream.encryption_type,
            enhanced_monitoring: Vec::new(),
            key_id: stream.key_id,
            open_shard_count: stream
                .shards
                .iter()
                .filter(|shard| !shard.closed)
                .count() as u32,
            retention_period_hours: stream.retention_period_hours,
            stream_arn: key.stream_arn(),
            stream_creation_timestamp: stream.stream_creation_timestamp,
            stream_mode_details: stream_mode_details(),
            stream_name: stream.stream_name,
            stream_status: stream.stream_status,
        })
    }

    /// # Errors
    ///
    /// Returns validation, not-found, or token errors when the shard page
    /// cannot be resolved.
    pub fn list_shards(
        &self,
        scope: &KinesisScope,
        input: ListShardsInput,
    ) -> Result<ListShardsOutput, KinesisError> {
        let (key, start, page_size) = if let Some(next_token) =
            input.next_token.as_deref()
        {
            if input.stream.is_some()
                || input.exclusive_start_shard_id.is_some()
            {
                return Err(KinesisError::InvalidArgument {
                    message:
                        "NextToken cannot be combined with stream identifiers or ExclusiveStartShardId."
                            .to_owned(),
                });
            }
            let (stream_arn, start) =
                decode_scoped_token(next_token, TOKEN_KIND_SHARDS)?;
            let stream_arn = stream_arn.parse::<Arn>().map_err(|error| {
                KinesisError::ExpiredNextToken {
                    message: format!(
                        "The provided NextToken is invalid: {error}."
                    ),
                }
            })?;
            let key = self.stream_key_from_identifier(
                scope,
                &KinesisStreamIdentifier::new(Some(stream_arn), None),
            )?;
            (
                key,
                start,
                normalize_page_size(
                    input.max_results,
                    DEFAULT_LIST_SHARDS_LIMIT,
                    "MaxResults",
                )?,
            )
        } else {
            let Some(stream) = input.stream.as_ref() else {
                return Err(KinesisError::InvalidArgument {
                    message:
                        "ListShards requires StreamName or StreamARN when NextToken is absent."
                            .to_owned(),
                });
            };
            let key = self.stream_key_from_identifier(scope, stream)?;
            let stream = self.load_stream(&key)?;
            let start = shard_start_index(
                &public_shards(&stream.shards),
                input.exclusive_start_shard_id.as_deref(),
            )?;
            (
                key,
                start,
                normalize_page_size(
                    input.max_results,
                    DEFAULT_LIST_SHARDS_LIMIT,
                    "MaxResults",
                )?,
            )
        };
        let stream = self.load_stream(&key)?;
        let shards = public_shards(&stream.shards);
        if start > shards.len() {
            return Err(KinesisError::ExpiredNextToken {
                message: "The provided NextToken is invalid.".to_owned(),
            });
        }

        let end = start.saturating_add(page_size).min(shards.len());
        Ok(ListShardsOutput {
            next_token: (end < shards.len()).then(|| {
                encode_scoped_token(TOKEN_KIND_SHARDS, &key.stream_arn(), end)
            }),
            shards: paged_items(&shards, start, end),
        })
    }

    /// # Errors
    ///
    /// Returns validation, not-found, or persistence errors when the record
    /// cannot be assigned to a writable shard and stored.
    pub fn put_record(
        &self,
        scope: &KinesisScope,
        input: PutRecordInput,
    ) -> Result<PutRecordOutput, KinesisError> {
        validate_partition_key(&input.partition_key)?;
        if input.sequence_number_for_ordering.is_some() {
            return Err(KinesisError::UnsupportedOperation {
                message:
                    "SequenceNumberForOrdering is not supported by Cloudish Kinesis."
                        .to_owned(),
            });
        }

        let _guard = self.lock_state();
        let key = self.stream_key_from_identifier(scope, &input.stream)?;
        let mut stream = self.load_stream(&key)?;
        let hash_key = effective_hash_key(
            &input.partition_key,
            input.explicit_hash_key.as_deref(),
        )?;
        let shard_index = select_writable_shard(&stream.shards, hash_key)?;
        let sequence_number =
            format_sequence_number(stream.next_sequence_number);
        stream.next_sequence_number =
            stream.next_sequence_number.saturating_add(1);
        let record = KinesisRecord {
            approximate_arrival_timestamp: current_epoch_seconds(&*self.clock),
            data: input.data,
            encryption_type: stream.encryption_type,
            partition_key: input.partition_key,
            sequence_number: sequence_number.clone(),
        };
        let shard = stream.shards.get_mut(shard_index).ok_or_else(|| {
            KinesisError::InternalFailure {
                message: format!(
                    "Selected shard index {shard_index} was missing."
                ),
            }
        })?;
        let shard_id = shard.shard_id.clone();
        shard.records.push(record);
        self.persist_stream(&key, stream)?;

        Ok(PutRecordOutput {
            encryption_type: self.load_stream(&key)?.encryption_type,
            sequence_number,
            shard_id,
        })
    }

    /// # Errors
    ///
    /// Returns validation, not-found, or persistence errors when any record
    /// cannot be assigned to a writable shard and stored.
    pub fn put_records(
        &self,
        scope: &KinesisScope,
        input: PutRecordsInput,
    ) -> Result<PutRecordsOutput, KinesisError> {
        if input.records.is_empty() {
            return Err(KinesisError::InvalidArgument {
                message: "Records must contain at least one entry.".to_owned(),
            });
        }
        if input.records.len() > MAX_PUT_RECORDS_COUNT {
            return Err(KinesisError::InvalidArgument {
                message: format!(
                    "PutRecords supports at most {MAX_PUT_RECORDS_COUNT} records."
                ),
            });
        }

        let _guard = self.lock_state();
        let key = self.stream_key_from_identifier(scope, &input.stream)?;
        let mut stream = self.load_stream(&key)?;
        let mut results = Vec::with_capacity(input.records.len());

        for record in input.records {
            validate_partition_key(&record.partition_key)?;
            let hash_key = effective_hash_key(
                &record.partition_key,
                record.explicit_hash_key.as_deref(),
            )?;
            let shard_index = select_writable_shard(&stream.shards, hash_key)?;
            let sequence_number =
                format_sequence_number(stream.next_sequence_number);
            stream.next_sequence_number =
                stream.next_sequence_number.saturating_add(1);
            let shard =
                stream.shards.get_mut(shard_index).ok_or_else(|| {
                    KinesisError::InternalFailure {
                        message: format!(
                            "Selected shard index {shard_index} was missing."
                        ),
                    }
                })?;
            shard.records.push(KinesisRecord {
                approximate_arrival_timestamp: current_epoch_seconds(
                    &*self.clock,
                ),
                data: record.data,
                encryption_type: stream.encryption_type,
                partition_key: record.partition_key,
                sequence_number: sequence_number.clone(),
            });
            results.push(PutRecordsResultEntry {
                error_code: None,
                error_message: None,
                sequence_number: Some(sequence_number),
                shard_id: Some(shard.shard_id.clone()),
            });
        }

        let encryption_type = stream.encryption_type;
        self.persist_stream(&key, stream)?;

        Ok(PutRecordsOutput {
            encryption_type,
            failed_record_count: 0,
            records: results,
        })
    }

    /// # Errors
    ///
    /// Returns validation or not-found errors when the iterator inputs do not
    /// resolve to a stored shard position.
    pub fn get_shard_iterator(
        &self,
        scope: &KinesisScope,
        input: GetShardIteratorInput,
    ) -> Result<GetShardIteratorOutput, KinesisError> {
        let key = self.stream_key_from_identifier(scope, &input.stream)?;
        let stream = self.load_stream(&key)?;
        let shard = stream
            .shards
            .iter()
            .find(|shard| shard.shard_id == input.shard_id)
            .ok_or_else(|| KinesisError::ResourceNotFound {
                message: format!("Shard {} was not found.", input.shard_id),
            })?;
        let position = iterator_position(
            &shard.records,
            input.shard_iterator_type,
            input.starting_sequence_number.as_deref(),
            input.timestamp,
        )?;

        Ok(GetShardIteratorOutput {
            shard_iterator: encode_iterator_token(&IteratorToken {
                position,
                shard_id: shard.shard_id.clone(),
                stream_arn: key.stream_arn(),
            }),
        })
    }

    /// # Errors
    ///
    /// Returns validation or not-found errors when the shard iterator does not
    /// resolve to a stored shard page.
    pub fn get_records(
        &self,
        scope: &KinesisScope,
        input: GetRecordsInput,
    ) -> Result<GetRecordsOutput, KinesisError> {
        let limit = normalize_page_size(
            input.limit,
            DEFAULT_GET_RECORDS_LIMIT,
            "Limit",
        )?;
        if limit > MAX_GET_RECORDS_LIMIT {
            return Err(KinesisError::InvalidArgument {
                message: format!(
                    "GetRecords Limit must be between 1 and {MAX_GET_RECORDS_LIMIT}."
                ),
            });
        }

        let token = decode_iterator_token(&input.shard_iterator)?;
        let key = self.stream_key_from_identifier(
            scope,
            &KinesisStreamIdentifier::new(
                Some(token.stream_arn.clone()),
                None,
            ),
        )?;
        let stream = self.load_stream(&key)?;
        let shard = stream
            .shards
            .iter()
            .find(|shard| shard.shard_id == token.shard_id)
            .ok_or_else(|| KinesisError::ResourceNotFound {
                message: format!("Shard {} was not found.", token.shard_id),
            })?;
        if token.position > shard.records.len() {
            return Err(KinesisError::InvalidArgument {
                message: "The provided ShardIterator is invalid.".to_owned(),
            });
        }

        let end = token
            .position
            .saturating_add(limit)
            .min(shard.records.len());
        Ok(GetRecordsOutput {
            millis_behind_latest: 0,
            next_shard_iterator: Some(encode_iterator_token(&IteratorToken {
                position: end,
                shard_id: token.shard_id,
                stream_arn: token.stream_arn,
            })),
            records: paged_items(&shard.records, token.position, end),
        })
    }

    /// # Errors
    ///
    /// Returns validation, not-found, conflict, or persistence errors when the
    /// consumer cannot be registered.
    pub fn register_stream_consumer(
        &self,
        scope: &KinesisScope,
        input: RegisterStreamConsumerInput,
    ) -> Result<KinesisConsumer, KinesisError> {
        validate_consumer_name(&input.consumer_name)?;

        let _guard = self.lock_state();
        let key = self.stream_key_from_identifier(
            scope,
            &KinesisStreamIdentifier::new(
                Some(input.stream_arn.clone()),
                None,
            ),
        )?;
        let _ = self.load_stream(&key)?;
        if self
            .consumers_for_stream(scope, &key.stream_arn())
            .into_iter()
            .any(|consumer| consumer.consumer_name == input.consumer_name)
        {
            return Err(KinesisError::ResourceInUse {
                message: format!(
                    "Consumer {} is already registered for stream {}.",
                    input.consumer_name, key.stream_name
                ),
            });
        }

        let created_at = current_epoch_seconds(&*self.clock);
        let consumer = KinesisConsumer {
            consumer_arn: format!(
                "{}/consumer/{}:{}",
                key.stream_arn(),
                input.consumer_name,
                created_at
            ),
            consumer_creation_timestamp: created_at,
            consumer_name: input.consumer_name,
            consumer_status: KinesisConsumerStatus::Active,
            stream_arn: Some(key.stream_arn()),
        };
        self.consumer_store
            .put(
                ConsumerStorageKey::new(scope, &consumer.consumer_arn),
                consumer.clone(),
            )
            .map_err(|source| {
                storage_error("writing consumer state", source)
            })?;

        Ok(consumer)
    }

    /// # Errors
    ///
    /// Returns validation or not-found errors when the consumer identifier does
    /// not resolve to a stored consumer.
    pub fn describe_stream_consumer(
        &self,
        scope: &KinesisScope,
        input: DescribeStreamConsumerInput,
    ) -> Result<KinesisConsumer, KinesisError> {
        if let Some(consumer_arn) = input.consumer_arn.as_deref() {
            return self
                .consumer_store
                .get(&ConsumerStorageKey::new(scope, consumer_arn))
                .ok_or_else(|| KinesisError::ResourceNotFound {
                    message: format!("Consumer {consumer_arn} was not found."),
                });
        }

        let stream_arn = input.stream_arn.as_ref().ok_or_else(|| {
            KinesisError::InvalidArgument {
                message:
                    "DescribeStreamConsumer requires ConsumerARN or StreamARN."
                        .to_owned(),
            }
        })?;
        let consumer_name = input.consumer_name.as_deref().ok_or_else(|| {
            KinesisError::InvalidArgument {
                message:
                    "DescribeStreamConsumer requires ConsumerName when ConsumerARN is absent."
                        .to_owned(),
            }
        })?;
        validate_consumer_name(consumer_name)?;
        let _ = self.stream_key_from_identifier(
            scope,
            &KinesisStreamIdentifier::new(Some(stream_arn.clone()), None),
        )?;

        self.consumers_for_stream(scope, stream_arn)
            .into_iter()
            .find(|consumer| consumer.consumer_name == consumer_name)
            .ok_or_else(|| KinesisError::ResourceNotFound {
                message: format!("Consumer {consumer_name} was not found."),
            })
    }

    /// # Errors
    ///
    /// Returns validation, not-found, or token errors when the consumer page
    /// cannot be resolved.
    pub fn list_stream_consumers(
        &self,
        scope: &KinesisScope,
        input: ListStreamConsumersInput,
    ) -> Result<ListStreamConsumersOutput, KinesisError> {
        let (stream_arn, start, page_size) = if let Some(next_token) =
            input.next_token.as_deref()
        {
            if input.stream_arn.is_some() {
                return Err(KinesisError::InvalidArgument {
                    message: "NextToken cannot be combined with StreamARN."
                        .to_owned(),
                });
            }
            let (stream_arn, start) =
                decode_scoped_token(next_token, TOKEN_KIND_CONSUMERS)?;
            let stream_arn = stream_arn.parse::<Arn>().map_err(|error| {
                KinesisError::ExpiredNextToken {
                    message: format!(
                        "The provided NextToken is invalid: {error}."
                    ),
                }
            })?;
            (
                stream_arn,
                start,
                normalize_page_size(
                    input.max_results,
                    DEFAULT_LIST_CONSUMERS_LIMIT,
                    "MaxResults",
                )?,
            )
        } else {
            let stream_arn = input.stream_arn.ok_or_else(|| {
                KinesisError::InvalidArgument {
                    message:
                        "ListStreamConsumers requires StreamARN when NextToken is absent."
                            .to_owned(),
                }
            })?;
            (
                stream_arn,
                0,
                normalize_page_size(
                    input.max_results,
                    DEFAULT_LIST_CONSUMERS_LIMIT,
                    "MaxResults",
                )?,
            )
        };
        let _ = self.stream_key_from_identifier(
            scope,
            &KinesisStreamIdentifier::new(Some(stream_arn.clone()), None),
        )?;
        let consumers = self.consumers_for_stream(scope, &stream_arn);
        if start > consumers.len() {
            return Err(KinesisError::ExpiredNextToken {
                message: "The provided NextToken is invalid.".to_owned(),
            });
        }

        let end = start.saturating_add(page_size).min(consumers.len());
        Ok(ListStreamConsumersOutput {
            consumers: paged_items(&consumers, start, end),
            next_token: (end < consumers.len()).then(|| {
                encode_scoped_token(TOKEN_KIND_CONSUMERS, &stream_arn, end)
            }),
        })
    }

    /// # Errors
    ///
    /// Returns validation, not-found, or persistence errors when the consumer
    /// cannot be removed.
    pub fn deregister_stream_consumer(
        &self,
        scope: &KinesisScope,
        input: DeregisterStreamConsumerInput,
    ) -> Result<(), KinesisError> {
        let _guard = self.lock_state();
        if let Some(consumer_arn) = input.consumer_arn.as_deref() {
            let key = ConsumerStorageKey::new(scope, consumer_arn);
            if self.consumer_store.get(&key).is_none() {
                return Err(KinesisError::ResourceNotFound {
                    message: format!("Consumer {consumer_arn} was not found."),
                });
            }
            return self.consumer_store.delete(&key).map_err(|source| {
                storage_error("deleting consumer state", source)
            });
        }

        let stream_arn = input.stream_arn.as_ref().ok_or_else(|| {
            KinesisError::InvalidArgument {
                message:
                    "DeregisterStreamConsumer requires ConsumerARN or StreamARN."
                        .to_owned(),
            }
        })?;
        let consumer_name = input.consumer_name.as_deref().ok_or_else(|| {
            KinesisError::InvalidArgument {
                message:
                    "DeregisterStreamConsumer requires ConsumerName when ConsumerARN is absent."
                        .to_owned(),
            }
        })?;
        let consumer = self.describe_stream_consumer(
            scope,
            DescribeStreamConsumerInput {
                consumer_arn: None,
                consumer_name: Some(consumer_name.to_owned()),
                stream_arn: Some(stream_arn.clone()),
            },
        )?;

        self.consumer_store
            .delete(&ConsumerStorageKey::new(scope, &consumer.consumer_arn))
            .map_err(|source| storage_error("deleting consumer state", source))
    }

    /// # Errors
    ///
    /// Returns validation, not-found, or persistence errors when the shard
    /// cannot be split.
    pub fn split_shard(
        &self,
        scope: &KinesisScope,
        input: SplitShardInput,
    ) -> Result<(), KinesisError> {
        let _guard = self.lock_state();
        let key = self.stream_key_from_identifier(scope, &input.stream)?;
        let mut stream = self.load_stream(&key)?;
        let parent_index = stream
            .shards
            .iter()
            .position(|shard| shard.shard_id == input.shard_to_split)
            .ok_or_else(|| KinesisError::ResourceNotFound {
                message: format!(
                    "Shard {} was not found.",
                    input.shard_to_split
                ),
            })?;
        let parent_shard =
            stream.shards.get_mut(parent_index).ok_or_else(|| {
                KinesisError::InternalFailure {
                    message: format!(
                        "Shard index {parent_index} disappeared during split."
                    ),
                }
            })?;
        if parent_shard.closed {
            return Err(KinesisError::InvalidArgument {
                message: format!(
                    "Shard {} is already closed.",
                    input.shard_to_split
                ),
            });
        }

        let (left_range, right_range) = split_hash_key_range(
            &parent_shard.hash_key_range,
            &input.new_starting_hash_key,
        )?;
        let boundary_sequence =
            format_sequence_number(stream.next_sequence_number);
        parent_shard.closed = true;
        parent_shard.sequence_number_range.ending_sequence_number =
            Some(boundary_sequence.clone());

        let left_child = StoredShard {
            adjacent_parent_shard_id: None,
            closed: false,
            hash_key_range: left_range,
            parent_shard_id: Some(input.shard_to_split.clone()),
            records: Vec::new(),
            sequence_number_range: KinesisSequenceNumberRange {
                ending_sequence_number: None,
                starting_sequence_number: boundary_sequence.clone(),
            },
            shard_id: next_shard_id(&mut stream.next_shard_index),
        };
        let right_child = StoredShard {
            adjacent_parent_shard_id: None,
            closed: false,
            hash_key_range: right_range,
            parent_shard_id: Some(input.shard_to_split),
            records: Vec::new(),
            sequence_number_range: KinesisSequenceNumberRange {
                ending_sequence_number: None,
                starting_sequence_number: boundary_sequence,
            },
            shard_id: next_shard_id(&mut stream.next_shard_index),
        };
        stream.shards.push(left_child);
        stream.shards.push(right_child);
        self.persist_stream(&key, stream)
    }

    /// # Errors
    ///
    /// Returns validation, not-found, or persistence errors when the shards
    /// cannot be merged.
    pub fn merge_shards(
        &self,
        scope: &KinesisScope,
        input: MergeShardsInput,
    ) -> Result<(), KinesisError> {
        if input.shard_to_merge == input.adjacent_shard_to_merge {
            return Err(KinesisError::InvalidArgument {
                message: "ShardToMerge and AdjacentShardToMerge must differ."
                    .to_owned(),
            });
        }

        let _guard = self.lock_state();
        let key = self.stream_key_from_identifier(scope, &input.stream)?;
        let mut stream = self.load_stream(&key)?;
        let left_index = stream
            .shards
            .iter()
            .position(|shard| shard.shard_id == input.shard_to_merge)
            .ok_or_else(|| KinesisError::ResourceNotFound {
                message: format!(
                    "Shard {} was not found.",
                    input.shard_to_merge
                ),
            })?;
        let right_index = stream
            .shards
            .iter()
            .position(|shard| shard.shard_id == input.adjacent_shard_to_merge)
            .ok_or_else(|| KinesisError::ResourceNotFound {
                message: format!(
                    "Shard {} was not found.",
                    input.adjacent_shard_to_merge
                ),
            })?;
        let (left_closed, left_range, right_closed, right_range) = {
            let left_shard = stream.shards.get(left_index).ok_or_else(|| {
                KinesisError::InternalFailure {
                    message: format!(
                        "Left shard index {left_index} disappeared during merge."
                    ),
                }
            })?;
            let right_shard =
                stream.shards.get(right_index).ok_or_else(|| {
                    KinesisError::InternalFailure {
                        message: format!(
                            "Right shard index {right_index} disappeared during merge."
                        ),
                    }
                })?;
            (
                left_shard.closed,
                left_shard.hash_key_range.clone(),
                right_shard.closed,
                right_shard.hash_key_range.clone(),
            )
        };
        if left_closed || right_closed {
            return Err(KinesisError::InvalidArgument {
                message: "Closed shards cannot be merged.".to_owned(),
            });
        }

        let merged_range = merge_hash_key_ranges(&left_range, &right_range)?;
        let boundary_sequence =
            format_sequence_number(stream.next_sequence_number);
        if left_index < right_index {
            let (left_slice, right_slice) =
                stream.shards.split_at_mut(right_index);
            let left_shard =
                left_slice.get_mut(left_index).ok_or_else(|| {
                    KinesisError::InternalFailure {
                        message: format!(
                            "Left shard index {left_index} disappeared during merge."
                        ),
                    }
                })?;
            let right_shard = right_slice.first_mut().ok_or_else(|| {
                KinesisError::InternalFailure {
                    message: format!(
                        "Right shard index {right_index} disappeared during merge."
                    ),
                }
            })?;
            left_shard.closed = true;
            left_shard.sequence_number_range.ending_sequence_number =
                Some(boundary_sequence.clone());
            right_shard.closed = true;
            right_shard.sequence_number_range.ending_sequence_number =
                Some(boundary_sequence.clone());
        } else {
            let (right_slice, left_slice) =
                stream.shards.split_at_mut(left_index);
            let right_shard =
                right_slice.get_mut(right_index).ok_or_else(|| {
                    KinesisError::InternalFailure {
                        message: format!(
                            "Right shard index {right_index} disappeared during merge."
                        ),
                    }
                })?;
            let left_shard = left_slice.first_mut().ok_or_else(|| {
                KinesisError::InternalFailure {
                    message: format!(
                        "Left shard index {left_index} disappeared during merge."
                    ),
                }
            })?;
            left_shard.closed = true;
            left_shard.sequence_number_range.ending_sequence_number =
                Some(boundary_sequence.clone());
            right_shard.closed = true;
            right_shard.sequence_number_range.ending_sequence_number =
                Some(boundary_sequence.clone());
        }
        let merged_shard_id = next_shard_id(&mut stream.next_shard_index);
        stream.shards.push(StoredShard {
            adjacent_parent_shard_id: Some(
                input.adjacent_shard_to_merge.clone(),
            ),
            closed: false,
            hash_key_range: merged_range,
            parent_shard_id: Some(input.shard_to_merge),
            records: Vec::new(),
            sequence_number_range: KinesisSequenceNumberRange {
                ending_sequence_number: None,
                starting_sequence_number: boundary_sequence,
            },
            shard_id: merged_shard_id,
        });
        self.persist_stream(&key, stream)
    }

    /// # Errors
    ///
    /// Returns validation, not-found, or persistence errors when the stream
    /// tags cannot be updated.
    pub fn add_tags_to_stream(
        &self,
        scope: &KinesisScope,
        input: AddTagsToStreamInput,
    ) -> Result<(), KinesisError> {
        let _guard = self.lock_state();
        let key = self.stream_key_from_identifier(scope, &input.stream)?;
        let mut stream = self.load_stream(&key)?;
        let mut updated = stream.tags.clone();
        updated.extend(input.tags);
        if updated.len() > MAX_STREAM_TAGS {
            return Err(KinesisError::InvalidArgument {
                message: format!(
                    "A stream supports at most {MAX_STREAM_TAGS} tags."
                ),
            });
        }
        stream.tags = updated;
        self.persist_stream(&key, stream)
    }

    /// # Errors
    ///
    /// Returns validation, not-found, or persistence errors when the stream
    /// tags cannot be updated.
    pub fn remove_tags_from_stream(
        &self,
        scope: &KinesisScope,
        input: RemoveTagsFromStreamInput,
    ) -> Result<(), KinesisError> {
        let _guard = self.lock_state();
        let key = self.stream_key_from_identifier(scope, &input.stream)?;
        let mut stream = self.load_stream(&key)?;
        for tag_key in input.tag_keys {
            stream.tags.remove(&tag_key);
        }
        self.persist_stream(&key, stream)
    }

    /// # Errors
    ///
    /// Returns validation or not-found errors when the tag page cannot be
    /// resolved.
    pub fn list_tags_for_stream(
        &self,
        scope: &KinesisScope,
        input: ListTagsForStreamInput,
    ) -> Result<ListTagsForStreamOutput, KinesisError> {
        let key = self.stream_key_from_identifier(scope, &input.stream)?;
        let stream = self.load_stream(&key)?;
        let tags = stream
            .tags
            .into_iter()
            .map(|(key, value)| KinesisTag { key, value })
            .collect::<Vec<_>>();
        let page_size = normalize_page_size(
            input.limit,
            DEFAULT_LIST_TAGS_LIMIT,
            "Limit",
        )?;
        let start = if let Some(exclusive_start) =
            input.exclusive_start_tag_key.as_deref()
        {
            tags.binary_search_by(|tag| tag.key.as_str().cmp(exclusive_start))
                .map(|index| index.saturating_add(1))
                .unwrap_or_else(|index| index)
        } else {
            0
        };
        let end = start.saturating_add(page_size).min(tags.len());

        Ok(ListTagsForStreamOutput {
            has_more_tags: end < tags.len(),
            tags: paged_items(&tags, start, end),
        })
    }

    /// # Errors
    ///
    /// Returns validation, not-found, in-use, or persistence errors when
    /// stream encryption cannot be enabled.
    pub fn start_stream_encryption(
        &self,
        scope: &KinesisScope,
        input: StartStreamEncryptionInput,
    ) -> Result<(), KinesisError> {
        let _guard = self.lock_state();
        let key = self.stream_key_from_identifier(scope, &input.stream)?;
        let mut stream = self.load_stream(&key)?;
        let encryption_type =
            KinesisEncryptionType::parse(&input.encryption_type)?;
        if encryption_type != KinesisEncryptionType::Kms {
            return Err(KinesisError::InvalidArgument {
                message: "StartStreamEncryption only supports KMS encryption."
                    .to_owned(),
            });
        }
        if stream.encryption_type == KinesisEncryptionType::Kms
            && stream.key_id.as_deref() == Some(input.key_id.as_str())
        {
            return Err(KinesisError::ResourceInUse {
                message: format!(
                    "Stream {} is already encrypted with key {}.",
                    key.stream_name, input.key_id
                ),
            });
        }

        stream.encryption_type = KinesisEncryptionType::Kms;
        stream.key_id = Some(input.key_id);
        self.persist_stream(&key, stream)
    }

    /// # Errors
    ///
    /// Returns validation, not-found, in-use, or persistence errors when
    /// stream encryption cannot be disabled.
    pub fn stop_stream_encryption(
        &self,
        scope: &KinesisScope,
        input: StopStreamEncryptionInput,
    ) -> Result<(), KinesisError> {
        let _guard = self.lock_state();
        let key = self.stream_key_from_identifier(scope, &input.stream)?;
        let mut stream = self.load_stream(&key)?;
        let encryption_type =
            KinesisEncryptionType::parse(&input.encryption_type)?;
        if encryption_type != KinesisEncryptionType::Kms {
            return Err(KinesisError::InvalidArgument {
                message: "StopStreamEncryption only supports KMS encryption."
                    .to_owned(),
            });
        }
        if stream.encryption_type == KinesisEncryptionType::None {
            return Err(KinesisError::ResourceInUse {
                message: format!(
                    "Stream {} is not encrypted.",
                    key.stream_name
                ),
            });
        }
        if stream.key_id.as_deref() != Some(input.key_id.as_str()) {
            return Err(KinesisError::InvalidArgument {
                message: format!(
                    "Stream {} is not encrypted with key {}.",
                    key.stream_name, input.key_id
                ),
            });
        }

        stream.encryption_type = KinesisEncryptionType::None;
        stream.key_id = None;
        self.persist_stream(&key, stream)
    }

    fn list_scoped_stream_names(
        &self,
        scope: &KinesisScope,
    ) -> Vec<KinesisStreamName> {
        let mut names = self
            .stream_store
            .keys()
            .into_iter()
            .filter(|key| {
                key.account_id == *scope.account_id()
                    && key.region == *scope.region()
            })
            .map(|key| key.stream_name)
            .collect::<Vec<_>>();
        names.sort();
        names
    }

    fn consumers_for_stream(
        &self,
        scope: &KinesisScope,
        stream_arn: &Arn,
    ) -> Vec<KinesisConsumer> {
        let mut consumers = self
            .consumer_store
            .keys()
            .into_iter()
            .filter(|key| {
                key.account_id == *scope.account_id()
                    && key.region == *scope.region()
            })
            .filter_map(|key| self.consumer_store.get(&key))
            .filter(|consumer| {
                consumer.stream_arn.as_ref() == Some(stream_arn)
            })
            .collect::<Vec<_>>();
        consumers.sort_by(|left, right| {
            left.consumer_name
                .cmp(&right.consumer_name)
                .then_with(|| left.consumer_arn.cmp(&right.consumer_arn))
        });
        consumers
    }

    fn stream_key_from_identifier(
        &self,
        scope: &KinesisScope,
        identifier: &KinesisStreamIdentifier,
    ) -> Result<StreamStorageKey, KinesisError> {
        let name_from_arn = identifier
            .stream_arn()
            .map(|stream_arn| stream_name_from_arn(scope, stream_arn))
            .transpose()?;
        let name_from_field = identifier.stream_name().cloned();

        match (name_from_arn, name_from_field) {
            (Some(from_arn), Some(from_field)) if from_arn != from_field => {
                Err(KinesisError::InvalidArgument {
                    message:
                        "StreamARN and StreamName must refer to the same stream."
                            .to_owned(),
                })
            }
            (Some(stream_name), _) => Ok(StreamStorageKey::new(scope, &stream_name)),
            (None, Some(stream_name)) => Ok(StreamStorageKey::new(scope, &stream_name)),
            (None, None) => Err(KinesisError::InvalidArgument {
                message: "StreamName or StreamARN is required.".to_owned(),
            }),
        }
    }

    fn load_stream(
        &self,
        key: &StreamStorageKey,
    ) -> Result<StoredStream, KinesisError> {
        self.stream_store.get(key).ok_or_else(|| {
            KinesisError::ResourceNotFound {
                message: format!("Stream {} was not found.", key.stream_name),
            }
        })
    }

    fn persist_stream(
        &self,
        key: &StreamStorageKey,
        stream: StoredStream,
    ) -> Result<(), KinesisError> {
        self.stream_store
            .put(key.clone(), stream)
            .map_err(|source| storage_error("writing stream state", source))
    }

    fn lock_state(&self) -> MutexGuard<'_, ()> {
        self.mutation_lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

fn validate_positive(
    field_name: &str,
    value: usize,
) -> Result<(), KinesisError> {
    if value == 0 {
        return Err(KinesisError::InvalidArgument {
            message: format!("{field_name} must be greater than zero."),
        });
    }

    Ok(())
}

fn normalize_page_size(
    value: Option<u32>,
    default_value: usize,
    field_name: &str,
) -> Result<usize, KinesisError> {
    let page_size = value
        .map(usize::try_from)
        .transpose()
        .map_err(|_| KinesisError::InvalidArgument {
            message: format!("{field_name} could not be represented."),
        })?
        .unwrap_or(default_value);
    if page_size == 0 {
        return Err(KinesisError::InvalidArgument {
            message: format!("{field_name} must be greater than zero."),
        });
    }

    Ok(page_size)
}

fn paged_items<T: Clone>(items: &[T], start: usize, end: usize) -> Vec<T> {
    match items.get(start..end) {
        Some(page) => page.to_vec(),
        None => Vec::new(),
    }
}

fn current_epoch_seconds(clock: &dyn Clock) -> u64 {
    clock.now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::iterators::{IteratorToken, encode_iterator_token};
    use crate::streams::stream_name_from_arn;
    use std::collections::BTreeMap;
    use std::time::{Duration, SystemTime};
    use storage::{StorageConfig, StorageMode};

    struct SequenceClock {
        times: Mutex<Vec<SystemTime>>,
    }

    impl SequenceClock {
        fn new(times: Vec<SystemTime>) -> Self {
            Self { times: Mutex::new(times) }
        }
    }

    impl Clock for SequenceClock {
        fn now(&self) -> SystemTime {
            self.times
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .pop()
                .unwrap_or(UNIX_EPOCH)
        }
    }

    fn seconds(value: u64) -> SystemTime {
        UNIX_EPOCH
            .checked_add(Duration::from_secs(value))
            .unwrap_or(UNIX_EPOCH)
    }

    fn scope(region: &str) -> KinesisScope {
        KinesisScope::new(
            "000000000000".parse().expect("account id should parse"),
            region.parse().expect("region should parse"),
        )
    }

    fn service(label: &str, times: Vec<SystemTime>) -> KinesisService {
        let factory = StorageFactory::new(StorageConfig::new(
            format!("/tmp/{label}"),
            StorageMode::Memory,
        ));

        KinesisService::new(&factory, Arc::new(SequenceClock::new(times)))
    }

    fn stream_name(value: &str) -> KinesisStreamName {
        KinesisStreamName::new(value)
            .expect("test stream names should be valid")
    }

    fn stream_arn(value: &str) -> Arn {
        value.parse().expect("test stream arns should parse")
    }

    fn identifier(name: &str) -> KinesisStreamIdentifier {
        KinesisStreamIdentifier::new(None, Some(stream_name(name)))
    }

    #[test]
    fn kinesis_stream_lifecycle_listing_and_describe_are_scoped() {
        let service = service(
            "services-kinesis-lifecycle",
            vec![seconds(30), seconds(20), seconds(10)],
        );
        let west = scope("eu-west-2");
        let east = scope("us-east-1");

        service
            .create_stream(
                &west,
                CreateStreamInput {
                    shard_count: 2,
                    stream_name: stream_name("events"),
                },
            )
            .expect("events stream should create");
        service
            .create_stream(
                &west,
                CreateStreamInput {
                    shard_count: 1,
                    stream_name: stream_name("audit"),
                },
            )
            .expect("audit stream should create");
        service
            .create_stream(
                &east,
                CreateStreamInput {
                    shard_count: 1,
                    stream_name: stream_name("events"),
                },
            )
            .expect("east events stream should create");

        let first_page = service
            .list_streams(
                &west,
                ListStreamsInput {
                    exclusive_start_stream_name: None,
                    limit: Some(1),
                    next_token: None,
                },
            )
            .expect("first page should list");
        let second_page = service
            .list_streams(
                &west,
                ListStreamsInput {
                    exclusive_start_stream_name: None,
                    limit: Some(10),
                    next_token: first_page.next_token.clone(),
                },
            )
            .expect("second page should list");
        let described = service
            .describe_stream(
                &west,
                DescribeStreamInput {
                    exclusive_start_shard_id: None,
                    limit: Some(1),
                    stream: identifier("events"),
                },
            )
            .expect("describe should succeed");
        let described_next = service
            .describe_stream(
                &west,
                DescribeStreamInput {
                    exclusive_start_shard_id: Some(
                        "shardId-000000000000".to_owned(),
                    ),
                    limit: Some(2),
                    stream: KinesisStreamIdentifier::new(
                        Some(described.stream_arn.clone()),
                        Some(stream_name("events")),
                    ),
                },
            )
            .expect("second describe page should succeed");
        let summary = service
            .describe_stream_summary(
                &west,
                DescribeStreamSummaryInput { stream: identifier("events") },
            )
            .expect("summary should succeed");

        assert_eq!(first_page.stream_names, vec!["audit".to_owned()]);
        assert!(first_page.has_more_streams);
        assert_eq!(second_page.stream_names, vec!["events".to_owned()]);
        assert_eq!(described.stream_name, "events");
        assert_eq!(described.shards.len(), 1);
        assert!(described.has_more_shards);
        assert_eq!(described_next.shards.len(), 1);
        assert!(!described_next.has_more_shards);
        assert_eq!(summary.open_shard_count, 2);
        assert_eq!(summary.consumer_count, 0);
        assert_eq!(
            service
                .list_streams(
                    &east,
                    ListStreamsInput {
                        exclusive_start_stream_name: None,
                        limit: None,
                        next_token: None,
                    },
                )
                .expect("east streams should list")
                .stream_names,
            vec!["events".to_owned()]
        );

        service
            .delete_stream(
                &west,
                DeleteStreamInput {
                    enforce_consumer_deletion: None,
                    stream: identifier("audit"),
                },
            )
            .expect("audit stream should delete");
        assert_eq!(
            service
                .list_streams(
                    &west,
                    ListStreamsInput {
                        exclusive_start_stream_name: None,
                        limit: None,
                        next_token: None,
                    },
                )
                .expect("remaining streams should list")
                .stream_names,
            vec!["events".to_owned()]
        );
    }

    #[test]
    fn kinesis_put_record_put_records_and_iterators_are_deterministic() {
        let service = service(
            "services-kinesis-records",
            vec![seconds(4), seconds(3), seconds(2), seconds(1)],
        );
        let scope = scope("eu-west-2");

        service
            .create_stream(
                &scope,
                CreateStreamInput {
                    shard_count: 1,
                    stream_name: stream_name("events"),
                },
            )
            .expect("stream should create");
        let first = service
            .put_record(
                &scope,
                PutRecordInput {
                    data: b"one".to_vec(),
                    explicit_hash_key: None,
                    partition_key: "alpha".to_owned(),
                    sequence_number_for_ordering: None,
                    stream: identifier("events"),
                },
            )
            .expect("first record should write");
        let batch = service
            .put_records(
                &scope,
                PutRecordsInput {
                    records: vec![
                        PutRecordsEntry {
                            data: b"two".to_vec(),
                            explicit_hash_key: None,
                            partition_key: "alpha".to_owned(),
                        },
                        PutRecordsEntry {
                            data: b"three".to_vec(),
                            explicit_hash_key: None,
                            partition_key: "alpha".to_owned(),
                        },
                    ],
                    stream: identifier("events"),
                },
            )
            .expect("batch records should write");
        let trim_horizon = service
            .get_shard_iterator(
                &scope,
                GetShardIteratorInput {
                    shard_id: first.shard_id.clone(),
                    shard_iterator_type: KinesisShardIteratorType::TrimHorizon,
                    starting_sequence_number: None,
                    stream: identifier("events"),
                    timestamp: None,
                },
            )
            .expect("trim horizon iterator should create");
        let first_page = service
            .get_records(
                &scope,
                GetRecordsInput {
                    limit: Some(2),
                    shard_iterator: trim_horizon.shard_iterator,
                },
            )
            .expect("first records page should read");
        let second_page = service
            .get_records(
                &scope,
                GetRecordsInput {
                    limit: Some(10),
                    shard_iterator: first_page
                        .next_shard_iterator
                        .clone()
                        .expect("next iterator should exist"),
                },
            )
            .expect("second records page should read");
        let at_sequence = service
            .get_shard_iterator(
                &scope,
                GetShardIteratorInput {
                    shard_id: first.shard_id.clone(),
                    shard_iterator_type:
                        KinesisShardIteratorType::AtSequenceNumber,
                    starting_sequence_number: batch.records[0]
                        .sequence_number
                        .clone(),
                    stream: identifier("events"),
                    timestamp: None,
                },
            )
            .expect("at-sequence iterator should create");
        let at_sequence_records = service
            .get_records(
                &scope,
                GetRecordsInput {
                    limit: Some(10),
                    shard_iterator: at_sequence.shard_iterator,
                },
            )
            .expect("at-sequence read should succeed");
        let after_sequence = service
            .get_shard_iterator(
                &scope,
                GetShardIteratorInput {
                    shard_id: first.shard_id.clone(),
                    shard_iterator_type:
                        KinesisShardIteratorType::AfterSequenceNumber,
                    starting_sequence_number: batch.records[0]
                        .sequence_number
                        .clone(),
                    stream: identifier("events"),
                    timestamp: None,
                },
            )
            .expect("after-sequence iterator should create");
        let latest = service
            .get_shard_iterator(
                &scope,
                GetShardIteratorInput {
                    shard_id: first.shard_id.clone(),
                    shard_iterator_type: KinesisShardIteratorType::Latest,
                    starting_sequence_number: None,
                    stream: identifier("events"),
                    timestamp: None,
                },
            )
            .expect("latest iterator should create");
        let latest_records = service
            .get_records(
                &scope,
                GetRecordsInput {
                    limit: Some(10),
                    shard_iterator: latest.shard_iterator,
                },
            )
            .expect("latest read should succeed");
        let at_timestamp = service
            .get_shard_iterator(
                &scope,
                GetShardIteratorInput {
                    shard_id: first.shard_id,
                    shard_iterator_type: KinesisShardIteratorType::AtTimestamp,
                    starting_sequence_number: None,
                    stream: identifier("events"),
                    timestamp: Some(3),
                },
            )
            .expect("timestamp iterator should create");
        let timestamp_records = service
            .get_records(
                &scope,
                GetRecordsInput {
                    limit: Some(10),
                    shard_iterator: at_timestamp.shard_iterator,
                },
            )
            .expect("timestamp read should succeed");

        assert_eq!(first.sequence_number, "00000000000000000001");
        assert_eq!(batch.failed_record_count, 0);
        assert_eq!(first_page.records.len(), 2);
        assert_eq!(first_page.records[0].data, b"one");
        assert_eq!(first_page.records[1].data, b"two");
        assert_eq!(second_page.records.len(), 1);
        assert_eq!(second_page.records[0].data, b"three");
        assert_eq!(
            at_sequence_records
                .records
                .iter()
                .map(|record| record.data.clone())
                .collect::<Vec<_>>(),
            vec![b"two".to_vec(), b"three".to_vec()]
        );
        assert_eq!(
            service
                .get_records(
                    &scope,
                    GetRecordsInput {
                        limit: Some(10),
                        shard_iterator: after_sequence.shard_iterator,
                    },
                )
                .expect("after-sequence read should succeed")
                .records
                .iter()
                .map(|record| record.data.clone())
                .collect::<Vec<_>>(),
            vec![b"three".to_vec()]
        );
        assert!(latest_records.records.is_empty());
        assert_eq!(
            timestamp_records
                .records
                .iter()
                .map(|record| record.data.clone())
                .collect::<Vec<_>>(),
            vec![b"two".to_vec(), b"three".to_vec()]
        );
    }

    #[test]
    fn kinesis_consumers_tags_and_encryption_round_trip() {
        let service = service(
            "services-kinesis-consumers",
            vec![seconds(20), seconds(10)],
        );
        let scope = scope("eu-west-2");

        service
            .create_stream(
                &scope,
                CreateStreamInput {
                    shard_count: 1,
                    stream_name: stream_name("events"),
                },
            )
            .expect("stream should create");
        let stream_arn = service
            .describe_stream_summary(
                &scope,
                DescribeStreamSummaryInput { stream: identifier("events") },
            )
            .expect("summary should succeed")
            .stream_arn;
        let consumer = service
            .register_stream_consumer(
                &scope,
                RegisterStreamConsumerInput {
                    consumer_name: "analytics".to_owned(),
                    stream_arn: stream_arn.clone(),
                },
            )
            .expect("consumer should register");
        let described_by_arn = service
            .describe_stream_consumer(
                &scope,
                DescribeStreamConsumerInput {
                    consumer_arn: Some(consumer.consumer_arn.clone()),
                    consumer_name: None,
                    stream_arn: None,
                },
            )
            .expect("consumer should describe by arn");
        let listed = service
            .list_stream_consumers(
                &scope,
                ListStreamConsumersInput {
                    max_results: Some(1),
                    next_token: None,
                    stream_arn: Some(stream_arn.clone()),
                },
            )
            .expect("consumer list should succeed");
        service
            .add_tags_to_stream(
                &scope,
                AddTagsToStreamInput {
                    stream: identifier("events"),
                    tags: BTreeMap::from([
                        ("env".to_owned(), "dev".to_owned()),
                        ("team".to_owned(), "platform".to_owned()),
                    ]),
                },
            )
            .expect("tags should attach");
        let tags_page = service
            .list_tags_for_stream(
                &scope,
                ListTagsForStreamInput {
                    exclusive_start_tag_key: None,
                    limit: Some(1),
                    stream: identifier("events"),
                },
            )
            .expect("tags should list");
        service
            .remove_tags_from_stream(
                &scope,
                RemoveTagsFromStreamInput {
                    stream: identifier("events"),
                    tag_keys: vec!["env".to_owned()],
                },
            )
            .expect("tag should remove");
        service
            .start_stream_encryption(
                &scope,
                StartStreamEncryptionInput {
                    encryption_type: "KMS".to_owned(),
                    key_id: "alias/cloudish".to_owned(),
                    stream: identifier("events"),
                },
            )
            .expect("encryption should start");
        let encrypted_summary = service
            .describe_stream_summary(
                &scope,
                DescribeStreamSummaryInput { stream: identifier("events") },
            )
            .expect("encrypted summary should succeed");
        let delete_error = service
            .delete_stream(
                &scope,
                DeleteStreamInput {
                    enforce_consumer_deletion: None,
                    stream: identifier("events"),
                },
            )
            .expect_err("stream delete should require consumer deletion");
        service
            .stop_stream_encryption(
                &scope,
                StopStreamEncryptionInput {
                    encryption_type: "KMS".to_owned(),
                    key_id: "alias/cloudish".to_owned(),
                    stream: identifier("events"),
                },
            )
            .expect("encryption should stop");
        service
            .deregister_stream_consumer(
                &scope,
                DeregisterStreamConsumerInput {
                    consumer_arn: None,
                    consumer_name: Some("analytics".to_owned()),
                    stream_arn: Some(stream_arn),
                },
            )
            .expect("consumer should deregister");

        assert_eq!(described_by_arn.consumer_name, "analytics");
        assert_eq!(listed.consumers, vec![consumer]);
        assert!(listed.next_token.is_none());
        assert_eq!(tags_page.tags.len(), 1);
        assert!(tags_page.has_more_tags);
        assert_eq!(
            service
                .list_tags_for_stream(
                    &scope,
                    ListTagsForStreamInput {
                        exclusive_start_tag_key: None,
                        limit: None,
                        stream: identifier("events"),
                    },
                )
                .expect("remaining tags should list")
                .tags,
            vec![KinesisTag {
                key: "team".to_owned(),
                value: "platform".to_owned(),
            }]
        );
        assert_eq!(
            encrypted_summary.encryption_type,
            KinesisEncryptionType::Kms
        );
        assert_eq!(
            encrypted_summary.key_id,
            Some("alias/cloudish".to_owned())
        );
        assert_eq!(
            delete_error.to_aws_error().code(),
            "ResourceInUseException"
        );
    }

    #[test]
    fn kinesis_split_merge_and_list_shards_update_topology() {
        let service = service("services-kinesis-topology", vec![seconds(10)]);
        let scope = scope("eu-west-2");

        service
            .create_stream(
                &scope,
                CreateStreamInput {
                    shard_count: 1,
                    stream_name: stream_name("events"),
                },
            )
            .expect("stream should create");
        service
            .split_shard(
                &scope,
                SplitShardInput {
                    new_starting_hash_key: (u128::MAX / 2).to_string(),
                    shard_to_split: "shardId-000000000000".to_owned(),
                    stream: identifier("events"),
                },
            )
            .expect("shard should split");
        let first_page = service
            .list_shards(
                &scope,
                ListShardsInput {
                    exclusive_start_shard_id: None,
                    max_results: Some(2),
                    next_token: None,
                    stream: Some(identifier("events")),
                },
            )
            .expect("first shard page should list");
        let second_page = service
            .list_shards(
                &scope,
                ListShardsInput {
                    exclusive_start_shard_id: None,
                    max_results: Some(10),
                    next_token: first_page.next_token.clone(),
                    stream: None,
                },
            )
            .expect("second shard page should list");
        let mut shards = first_page.shards;
        shards.extend(second_page.shards);
        let child_shards = shards
            .iter()
            .filter(|shard| shard.parent_shard_id.is_some())
            .cloned()
            .collect::<Vec<_>>();

        service
            .merge_shards(
                &scope,
                MergeShardsInput {
                    adjacent_shard_to_merge: child_shards[1].shard_id.clone(),
                    shard_to_merge: child_shards[0].shard_id.clone(),
                    stream: identifier("events"),
                },
            )
            .expect("child shards should merge");
        let merged = service
            .list_shards(
                &scope,
                ListShardsInput {
                    exclusive_start_shard_id: None,
                    max_results: None,
                    next_token: None,
                    stream: Some(identifier("events")),
                },
            )
            .expect("merged shards should list");
        let summary = service
            .describe_stream_summary(
                &scope,
                DescribeStreamSummaryInput { stream: identifier("events") },
            )
            .expect("summary should reflect topology");

        assert_eq!(shards.len(), 3);
        assert_eq!(summary.open_shard_count, 1);
        assert_eq!(merged.shards.len(), 4);
        assert!(
            merged
                .shards
                .iter()
                .any(|shard| shard.adjacent_parent_shard_id.is_some())
        );
    }

    #[test]
    fn kinesis_validation_and_gap_paths_are_explicit() {
        let service = service(
            "services-kinesis-errors",
            vec![seconds(5), seconds(4), seconds(3), seconds(2), seconds(1)],
        );
        let scope = scope("eu-west-2");

        let invalid_shard_count = service
            .create_stream(
                &scope,
                CreateStreamInput {
                    shard_count: 0,
                    stream_name: stream_name("invalid"),
                },
            )
            .expect_err("zero shard count should fail");
        service
            .create_stream(
                &scope,
                CreateStreamInput {
                    shard_count: 3,
                    stream_name: stream_name("events"),
                },
            )
            .expect("events stream should create");
        let duplicate = service
            .create_stream(
                &scope,
                CreateStreamInput {
                    shard_count: 1,
                    stream_name: stream_name("events"),
                },
            )
            .expect_err("duplicate stream should fail");
        let invalid_iterator = service
            .get_records(
                &scope,
                GetRecordsInput {
                    limit: Some(1),
                    shard_iterator: "not-base64".to_owned(),
                },
            )
            .expect_err("invalid iterator should fail");
        let invalid_limit = service
            .get_records(
                &scope,
                GetRecordsInput {
                    limit: Some(0),
                    shard_iterator: encode_iterator_token(&IteratorToken {
                        position: 0,
                        shard_id: "shardId-000000000000".to_owned(),
                        stream_arn: stream_arn(
                            "arn:aws:kinesis:eu-west-2:000000000000:stream/events",
                        ),
                    }),
                },
            )
            .expect_err("zero record limit should fail");
        let invalid_split = service
            .split_shard(
                &scope,
                SplitShardInput {
                    new_starting_hash_key: "0".to_owned(),
                    shard_to_split: "shardId-000000000000".to_owned(),
                    stream: identifier("events"),
                },
            )
            .expect_err("invalid split key should fail");
        let invalid_merge = service
            .merge_shards(
                &scope,
                MergeShardsInput {
                    adjacent_shard_to_merge: "shardId-000000000002".to_owned(),
                    shard_to_merge: "shardId-000000000000".to_owned(),
                    stream: identifier("events"),
                },
            )
            .expect_err("non-adjacent merge should fail");
        let invalid_token = service
            .list_streams(
                &scope,
                ListStreamsInput {
                    exclusive_start_stream_name: None,
                    limit: Some(1),
                    next_token: Some("bogus".to_owned()),
                },
            )
            .expect_err("invalid next token should fail");
        let unsupported =
            KinesisError::UnsupportedOperation { message: "gap".to_owned() };
        let wrong_scope = stream_name_from_arn(
            &KinesisScope::new(
                "000000000000".parse().expect("account should parse"),
                "us-east-1".parse().expect("region should parse"),
            ),
            &stream_arn(
                "arn:aws:kinesis:eu-west-2:000000000000:stream/events",
            ),
        )
        .expect_err("scope mismatch should fail");

        assert_eq!(
            invalid_shard_count.to_aws_error().code(),
            "InvalidArgumentException"
        );
        assert_eq!(duplicate.to_aws_error().code(), "ResourceInUseException");
        assert_eq!(
            invalid_iterator.to_aws_error().code(),
            "InvalidArgumentException"
        );
        assert_eq!(
            invalid_limit.to_aws_error().code(),
            "InvalidArgumentException"
        );
        assert_eq!(
            invalid_split.to_aws_error().code(),
            "InvalidArgumentException"
        );
        assert_eq!(
            invalid_merge.to_aws_error().code(),
            "InvalidArgumentException"
        );
        assert_eq!(
            invalid_token.to_aws_error().code(),
            "ExpiredNextTokenException"
        );
        assert_eq!(unsupported.to_aws_error().code(), "UnsupportedOperation");
        assert_eq!(
            wrong_scope.to_aws_error().code(),
            "ResourceNotFoundException"
        );
    }
}
