pub(crate) use crate::aws_error_shape::AwsErrorShape;
use crate::request::HttpRequest;
use aws::{Arn, AwsError, RequestContext};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use kinesis::KinesisStreamName;
use serde::Deserialize;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::json;
use services::{
    CreateStreamInput, DeleteKinesisStreamInput,
    DeregisterStreamConsumerInput, DescribeKinesisStreamInput,
    DescribeStreamConsumerInput, DescribeStreamSummaryInput,
    KinesisAddTagsToStreamInput, KinesisError, KinesisGetRecordsInput,
    KinesisGetShardIteratorInput, KinesisListShardsInput, KinesisScope,
    KinesisService, KinesisShardIteratorType, KinesisStreamIdentifier,
    ListStreamConsumersInput, ListStreamsInput, ListTagsForStreamInput,
    MergeShardsInput, PutRecordInput, PutRecordsEntry, PutRecordsInput,
    RegisterStreamConsumerInput, RemoveTagsFromStreamInput, SplitShardInput,
    StartStreamEncryptionInput, StopStreamEncryptionInput,
};

pub(crate) fn handle_json(
    kinesis: &KinesisService,
    request: &HttpRequest<'_>,
    context: &RequestContext,
) -> Result<Vec<u8>, AwsError> {
    let operation = operation_from_target(request.header("x-amz-target"))
        .map_err(|error| error.to_aws_error())?;
    let scope = KinesisScope::new(
        context.account_id().clone(),
        context.region().clone(),
    );

    match operation {
        KinesisOperation::CreateStream => {
            let input =
                parse_json_body::<CreateStreamRequest>(request.body())?;
            kinesis
                .create_stream(
                    &scope,
                    CreateStreamInput {
                        shard_count: usize::try_from(input.shard_count)
                            .map_err(|_| {
                                KinesisError::InvalidArgument {
                                    message:
                                        "ShardCount could not be represented."
                                            .to_owned(),
                                }
                                .to_aws_error()
                            })?,
                        stream_name: KinesisStreamName::new(input.stream_name)
                            .map_err(|error| error.to_aws_error())?,
                    },
                )
                .map_err(|error| error.to_aws_error())?;
            json_response(&json!({}))
        }
        KinesisOperation::DeleteStream => {
            let input =
                parse_json_body::<DeleteStreamRequest>(request.body())?;
            kinesis
                .delete_stream(
                    &scope,
                    DeleteKinesisStreamInput {
                        enforce_consumer_deletion: input
                            .enforce_consumer_deletion,
                        stream: stream_identifier(
                            input.stream_arn,
                            input.stream_name,
                        )?,
                    },
                )
                .map_err(|error| error.to_aws_error())?;
            json_response(&json!({}))
        }
        KinesisOperation::DescribeStream => {
            let input =
                parse_json_body::<DescribeStreamRequest>(request.body())?;
            let output = kinesis
                .describe_stream(
                    &scope,
                    DescribeKinesisStreamInput {
                        exclusive_start_shard_id: input
                            .exclusive_start_shard_id,
                        limit: input.limit,
                        stream: stream_identifier(
                            input.stream_arn,
                            input.stream_name,
                        )?,
                    },
                )
                .map_err(|error| error.to_aws_error())?;
            json_response(&json!({ "StreamDescription": output }))
        }
        KinesisOperation::DescribeStreamSummary => {
            let input =
                parse_json_body::<StreamReferenceRequest>(request.body())?;
            let output = kinesis
                .describe_stream_summary(
                    &scope,
                    DescribeStreamSummaryInput {
                        stream: stream_identifier(
                            input.stream_arn,
                            input.stream_name,
                        )?,
                    },
                )
                .map_err(|error| error.to_aws_error())?;
            json_response(&json!({ "StreamDescriptionSummary": output }))
        }
        KinesisOperation::ListStreams => {
            let input = parse_json_body::<ListStreamsRequest>(request.body())?;
            let output = kinesis
                .list_streams(
                    &scope,
                    ListStreamsInput {
                        exclusive_start_stream_name: input
                            .exclusive_start_stream_name
                            .map(KinesisStreamName::new)
                            .transpose()
                            .map_err(|error| error.to_aws_error())?,
                        limit: input.limit,
                        next_token: input.next_token,
                    },
                )
                .map_err(|error| error.to_aws_error())?;
            json_response(&output)
        }
        KinesisOperation::ListShards => {
            let input = parse_json_body::<ListShardsRequest>(request.body())?;
            if input.shard_filter.is_some() {
                return Err(KinesisError::UnsupportedOperation {
                    message:
                        "ListShards ShardFilter remains unsupported in Cloudish Kinesis."
                            .to_owned(),
                }
                .to_aws_error());
            }
            let output = kinesis
                .list_shards(
                    &scope,
                    KinesisListShardsInput {
                        exclusive_start_shard_id: input
                            .exclusive_start_shard_id,
                        max_results: input.max_results,
                        next_token: input.next_token,
                        stream: (input.stream_arn.is_some()
                            || input.stream_name.is_some())
                        .then_some((input.stream_arn, input.stream_name))
                        .map(|(stream_arn, stream_name)| {
                            stream_identifier(stream_arn, stream_name)
                        })
                        .transpose()?,
                    },
                )
                .map_err(|error| error.to_aws_error())?;
            json_response(&output)
        }
        KinesisOperation::PutRecord => {
            let input = parse_json_body::<PutRecordRequest>(request.body())?;
            let output = kinesis
                .put_record(
                    &scope,
                    PutRecordInput {
                        data: decode_blob(&input.data, "Data")?,
                        explicit_hash_key: input.explicit_hash_key,
                        partition_key: input.partition_key,
                        sequence_number_for_ordering: input
                            .sequence_number_for_ordering,
                        stream: stream_identifier(
                            input.stream_arn,
                            input.stream_name,
                        )?,
                    },
                )
                .map_err(|error| error.to_aws_error())?;
            json_response(&output)
        }
        KinesisOperation::PutRecords => {
            let input = parse_json_body::<PutRecordsRequest>(request.body())?;
            let output = kinesis
                .put_records(
                    &scope,
                    PutRecordsInput {
                        records: input
                            .records
                            .into_iter()
                            .map(|record| {
                                Ok(PutRecordsEntry {
                                    data: decode_blob(&record.data, "Data")?,
                                    explicit_hash_key: record
                                        .explicit_hash_key,
                                    partition_key: record.partition_key,
                                })
                            })
                            .collect::<Result<Vec<_>, AwsError>>()?,
                        stream: stream_identifier(
                            input.stream_arn,
                            input.stream_name,
                        )?,
                    },
                )
                .map_err(|error| error.to_aws_error())?;
            json_response(&output)
        }
        KinesisOperation::GetShardIterator => {
            let input =
                parse_json_body::<GetShardIteratorRequest>(request.body())?;
            let output = kinesis
                .get_shard_iterator(
                    &scope,
                    KinesisGetShardIteratorInput {
                        shard_id: input.shard_id,
                        shard_iterator_type: KinesisShardIteratorType::parse(
                            &input.shard_iterator_type,
                        )
                        .map_err(|error| error.to_aws_error())?,
                        starting_sequence_number: input
                            .starting_sequence_number,
                        stream: stream_identifier(
                            input.stream_arn,
                            input.stream_name,
                        )?,
                        timestamp: input.timestamp,
                    },
                )
                .map_err(|error| error.to_aws_error())?;
            json_response(&output)
        }
        KinesisOperation::GetRecords => {
            let input = parse_json_body::<GetRecordsRequest>(request.body())?;
            let output = kinesis
                .get_records(
                    &scope,
                    KinesisGetRecordsInput {
                        limit: input.limit,
                        shard_iterator: input.shard_iterator,
                    },
                )
                .map_err(|error| error.to_aws_error())?;
            json_response(&output)
        }
        KinesisOperation::RegisterStreamConsumer => {
            let input =
                parse_json_body::<RegisterConsumerRequest>(request.body())?;
            let consumer = kinesis
                .register_stream_consumer(
                    &scope,
                    RegisterStreamConsumerInput {
                        consumer_name: input.consumer_name,
                        stream_arn: parse_arn("StreamARN", &input.stream_arn)?,
                    },
                )
                .map_err(|error| error.to_aws_error())?;
            json_response(&json!({ "Consumer": consumer }))
        }
        KinesisOperation::DescribeStreamConsumer => {
            let input =
                parse_json_body::<DescribeConsumerRequest>(request.body())?;
            let consumer = kinesis
                .describe_stream_consumer(
                    &scope,
                    DescribeStreamConsumerInput {
                        consumer_arn: input.consumer_arn,
                        consumer_name: input.consumer_name,
                        stream_arn: input
                            .stream_arn
                            .map(|stream_arn| {
                                parse_arn("StreamARN", &stream_arn)
                            })
                            .transpose()?,
                    },
                )
                .map_err(|error| error.to_aws_error())?;
            json_response(&json!({ "ConsumerDescription": consumer }))
        }
        KinesisOperation::ListStreamConsumers => {
            let input =
                parse_json_body::<ListConsumersRequest>(request.body())?;
            let output = kinesis
                .list_stream_consumers(
                    &scope,
                    ListStreamConsumersInput {
                        max_results: input.max_results,
                        next_token: input.next_token,
                        stream_arn: input
                            .stream_arn
                            .map(|stream_arn| {
                                parse_arn("StreamARN", &stream_arn)
                            })
                            .transpose()?,
                    },
                )
                .map_err(|error| error.to_aws_error())?;
            json_response(&output)
        }
        KinesisOperation::DeregisterStreamConsumer => {
            let input =
                parse_json_body::<DeregisterConsumerRequest>(request.body())?;
            kinesis
                .deregister_stream_consumer(
                    &scope,
                    DeregisterStreamConsumerInput {
                        consumer_arn: input.consumer_arn,
                        consumer_name: input.consumer_name,
                        stream_arn: input
                            .stream_arn
                            .map(|stream_arn| {
                                parse_arn("StreamARN", &stream_arn)
                            })
                            .transpose()?,
                    },
                )
                .map_err(|error| error.to_aws_error())?;
            json_response(&json!({}))
        }
        KinesisOperation::SplitShard => {
            let input = parse_json_body::<SplitShardRequest>(request.body())?;
            kinesis
                .split_shard(
                    &scope,
                    SplitShardInput {
                        new_starting_hash_key: input.new_starting_hash_key,
                        shard_to_split: input.shard_to_split,
                        stream: stream_identifier(
                            input.stream_arn,
                            input.stream_name,
                        )?,
                    },
                )
                .map_err(|error| error.to_aws_error())?;
            json_response(&json!({}))
        }
        KinesisOperation::MergeShards => {
            let input = parse_json_body::<MergeShardsRequest>(request.body())?;
            kinesis
                .merge_shards(
                    &scope,
                    MergeShardsInput {
                        adjacent_shard_to_merge: input.adjacent_shard_to_merge,
                        shard_to_merge: input.shard_to_merge,
                        stream: stream_identifier(
                            input.stream_arn,
                            input.stream_name,
                        )?,
                    },
                )
                .map_err(|error| error.to_aws_error())?;
            json_response(&json!({}))
        }
        KinesisOperation::AddTagsToStream => {
            let input = parse_json_body::<TagsRequest>(request.body())?;
            kinesis
                .add_tags_to_stream(
                    &scope,
                    KinesisAddTagsToStreamInput {
                        stream: stream_identifier(
                            input.stream_arn,
                            input.stream_name,
                        )?,
                        tags: input.tags,
                    },
                )
                .map_err(|error| error.to_aws_error())?;
            json_response(&json!({}))
        }
        KinesisOperation::RemoveTagsFromStream => {
            let input = parse_json_body::<RemoveTagsRequest>(request.body())?;
            kinesis
                .remove_tags_from_stream(
                    &scope,
                    RemoveTagsFromStreamInput {
                        stream: stream_identifier(
                            input.stream_arn,
                            input.stream_name,
                        )?,
                        tag_keys: input.tag_keys,
                    },
                )
                .map_err(|error| error.to_aws_error())?;
            json_response(&json!({}))
        }
        KinesisOperation::ListTagsForStream => {
            let input = parse_json_body::<ListTagsRequest>(request.body())?;
            let output = kinesis
                .list_tags_for_stream(
                    &scope,
                    ListTagsForStreamInput {
                        exclusive_start_tag_key: input.exclusive_start_tag_key,
                        limit: input.limit,
                        stream: stream_identifier(
                            input.stream_arn,
                            input.stream_name,
                        )?,
                    },
                )
                .map_err(|error| error.to_aws_error())?;
            json_response(&output)
        }
        KinesisOperation::StartStreamEncryption => {
            let input = parse_json_body::<EncryptionRequest>(request.body())?;
            kinesis
                .start_stream_encryption(
                    &scope,
                    StartStreamEncryptionInput {
                        encryption_type: input.encryption_type,
                        key_id: input.key_id,
                        stream: stream_identifier(
                            input.stream_arn,
                            input.stream_name,
                        )?,
                    },
                )
                .map_err(|error| error.to_aws_error())?;
            json_response(&json!({}))
        }
        KinesisOperation::StopStreamEncryption => {
            let input = parse_json_body::<EncryptionRequest>(request.body())?;
            kinesis
                .stop_stream_encryption(
                    &scope,
                    StopStreamEncryptionInput {
                        encryption_type: input.encryption_type,
                        key_id: input.key_id,
                        stream: stream_identifier(
                            input.stream_arn,
                            input.stream_name,
                        )?,
                    },
                )
                .map_err(|error| error.to_aws_error())?;
            json_response(&json!({}))
        }
        KinesisOperation::SubscribeToShard => Err(unsupported_gap(
            "SubscribeToShard remains an explicit Cloudish gap until the event-stream contract is promoted.",
        )),
        KinesisOperation::DeleteResourcePolicy
        | KinesisOperation::GetResourcePolicy
        | KinesisOperation::PutResourcePolicy => Err(unsupported_gap(
            "Kinesis resource-policy APIs remain an explicit Cloudish gap.",
        )),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum KinesisOperation {
    CreateStream,
    DeleteResourcePolicy,
    DeleteStream,
    DeregisterStreamConsumer,
    DescribeStream,
    DescribeStreamConsumer,
    DescribeStreamSummary,
    GetRecords,
    GetResourcePolicy,
    GetShardIterator,
    ListShards,
    ListStreamConsumers,
    ListStreams,
    ListTagsForStream,
    MergeShards,
    PutRecord,
    PutRecords,
    PutResourcePolicy,
    RegisterStreamConsumer,
    RemoveTagsFromStream,
    SplitShard,
    StartStreamEncryption,
    StopStreamEncryption,
    SubscribeToShard,
    AddTagsToStream,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct CreateStreamRequest {
    shard_count: u32,
    stream_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct DeleteStreamRequest {
    enforce_consumer_deletion: Option<bool>,
    #[serde(rename = "StreamARN")]
    stream_arn: Option<String>,
    stream_name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct DescribeStreamRequest {
    exclusive_start_shard_id: Option<String>,
    limit: Option<u32>,
    #[serde(rename = "StreamARN")]
    stream_arn: Option<String>,
    stream_name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct StreamReferenceRequest {
    #[serde(rename = "StreamARN")]
    stream_arn: Option<String>,
    stream_name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct ListStreamsRequest {
    exclusive_start_stream_name: Option<String>,
    limit: Option<u32>,
    next_token: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct ListShardsRequest {
    exclusive_start_shard_id: Option<String>,
    max_results: Option<u32>,
    next_token: Option<String>,
    shard_filter: Option<serde_json::Value>,
    #[serde(rename = "StreamARN")]
    stream_arn: Option<String>,
    stream_name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct PutRecordRequest {
    data: String,
    explicit_hash_key: Option<String>,
    partition_key: String,
    sequence_number_for_ordering: Option<String>,
    #[serde(rename = "StreamARN")]
    stream_arn: Option<String>,
    stream_name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct PutRecordsRequest {
    records: Vec<PutRecordsEntryRequest>,
    #[serde(rename = "StreamARN")]
    stream_arn: Option<String>,
    stream_name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct PutRecordsEntryRequest {
    data: String,
    explicit_hash_key: Option<String>,
    partition_key: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct GetShardIteratorRequest {
    shard_id: String,
    shard_iterator_type: String,
    starting_sequence_number: Option<String>,
    #[serde(rename = "StreamARN")]
    stream_arn: Option<String>,
    stream_name: Option<String>,
    timestamp: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct GetRecordsRequest {
    limit: Option<u32>,
    shard_iterator: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct RegisterConsumerRequest {
    consumer_name: String,
    #[serde(rename = "StreamARN")]
    stream_arn: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct DescribeConsumerRequest {
    #[serde(rename = "ConsumerARN")]
    consumer_arn: Option<String>,
    consumer_name: Option<String>,
    #[serde(rename = "StreamARN")]
    stream_arn: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct ListConsumersRequest {
    max_results: Option<u32>,
    next_token: Option<String>,
    #[serde(rename = "StreamARN")]
    stream_arn: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct DeregisterConsumerRequest {
    #[serde(rename = "ConsumerARN")]
    consumer_arn: Option<String>,
    consumer_name: Option<String>,
    #[serde(rename = "StreamARN")]
    stream_arn: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct SplitShardRequest {
    new_starting_hash_key: String,
    shard_to_split: String,
    #[serde(rename = "StreamARN")]
    stream_arn: Option<String>,
    stream_name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct MergeShardsRequest {
    adjacent_shard_to_merge: String,
    shard_to_merge: String,
    #[serde(rename = "StreamARN")]
    stream_arn: Option<String>,
    stream_name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct TagsRequest {
    #[serde(rename = "StreamARN")]
    stream_arn: Option<String>,
    stream_name: Option<String>,
    #[serde(default)]
    tags: std::collections::BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct RemoveTagsRequest {
    #[serde(rename = "StreamARN")]
    stream_arn: Option<String>,
    stream_name: Option<String>,
    #[serde(default)]
    tag_keys: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct ListTagsRequest {
    exclusive_start_tag_key: Option<String>,
    limit: Option<u32>,
    #[serde(rename = "StreamARN")]
    stream_arn: Option<String>,
    stream_name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct EncryptionRequest {
    encryption_type: String,
    key_id: String,
    #[serde(rename = "StreamARN")]
    stream_arn: Option<String>,
    stream_name: Option<String>,
}

fn action_from_target(target: Option<&str>) -> Result<&str, KinesisError> {
    let target = target.ok_or_else(|| KinesisError::UnsupportedOperation {
        message: "missing X-Amz-Target".to_owned(),
    })?;
    let Some((prefix, operation)) = target.split_once('.') else {
        return Err(KinesisError::UnsupportedOperation {
            message: format!("Operation {target} is not supported."),
        });
    };
    if prefix != "Kinesis_20131202" {
        return Err(KinesisError::UnsupportedOperation {
            message: format!("Operation {target} is not supported."),
        });
    }

    Ok(operation)
}

fn operation_from_target(
    target: Option<&str>,
) -> Result<KinesisOperation, KinesisError> {
    let operation = action_from_target(target)?;

    match operation {
        "AddTagsToStream" => Ok(KinesisOperation::AddTagsToStream),
        "CreateStream" => Ok(KinesisOperation::CreateStream),
        "DeleteResourcePolicy" => Ok(KinesisOperation::DeleteResourcePolicy),
        "DeleteStream" => Ok(KinesisOperation::DeleteStream),
        "DeregisterStreamConsumer" => {
            Ok(KinesisOperation::DeregisterStreamConsumer)
        }
        "DescribeStream" => Ok(KinesisOperation::DescribeStream),
        "DescribeStreamConsumer" => {
            Ok(KinesisOperation::DescribeStreamConsumer)
        }
        "DescribeStreamSummary" => Ok(KinesisOperation::DescribeStreamSummary),
        "GetRecords" => Ok(KinesisOperation::GetRecords),
        "GetResourcePolicy" => Ok(KinesisOperation::GetResourcePolicy),
        "GetShardIterator" => Ok(KinesisOperation::GetShardIterator),
        "ListShards" => Ok(KinesisOperation::ListShards),
        "ListStreamConsumers" => Ok(KinesisOperation::ListStreamConsumers),
        "ListStreams" => Ok(KinesisOperation::ListStreams),
        "ListTagsForStream" => Ok(KinesisOperation::ListTagsForStream),
        "MergeShards" => Ok(KinesisOperation::MergeShards),
        "PutRecord" => Ok(KinesisOperation::PutRecord),
        "PutRecords" => Ok(KinesisOperation::PutRecords),
        "PutResourcePolicy" => Ok(KinesisOperation::PutResourcePolicy),
        "RegisterStreamConsumer" => {
            Ok(KinesisOperation::RegisterStreamConsumer)
        }
        "RemoveTagsFromStream" => Ok(KinesisOperation::RemoveTagsFromStream),
        "SplitShard" => Ok(KinesisOperation::SplitShard),
        "StartStreamEncryption" => Ok(KinesisOperation::StartStreamEncryption),
        "StopStreamEncryption" => Ok(KinesisOperation::StopStreamEncryption),
        "SubscribeToShard" => Ok(KinesisOperation::SubscribeToShard),
        _ => Err(KinesisError::UnsupportedOperation {
            message: format!("Operation {operation} is not supported."),
        }),
    }
}

fn parse_json_body<T>(body: &[u8]) -> Result<T, AwsError>
where
    T: DeserializeOwned,
{
    serde_json::from_slice(body).map_err(|error| {
        KinesisError::InvalidArgument {
            message: format!("The request body is not valid JSON: {error}"),
        }
        .to_aws_error()
    })
}

fn decode_blob(value: &str, field_name: &str) -> Result<Vec<u8>, AwsError> {
    BASE64_STANDARD.decode(value).map_err(|_| {
        KinesisError::InvalidArgument {
            message: format!("{field_name} must be base64-encoded."),
        }
        .to_aws_error()
    })
}

fn json_response<T>(value: &T) -> Result<Vec<u8>, AwsError>
where
    T: Serialize,
{
    serde_json::to_vec(value).map_err(|error| {
        KinesisError::InternalFailure {
            message: format!("Failed to serialize Kinesis response: {error}"),
        }
        .to_aws_error()
    })
}

fn stream_identifier(
    stream_arn: Option<String>,
    stream_name: Option<String>,
) -> Result<KinesisStreamIdentifier, AwsError> {
    Ok(KinesisStreamIdentifier::new(
        stream_arn
            .map(|stream_arn| parse_arn("StreamARN", &stream_arn))
            .transpose()?,
        stream_name
            .map(|stream_name| {
                KinesisStreamName::new(stream_name)
                    .map_err(|error| error.to_aws_error())
            })
            .transpose()?,
    ))
}

fn parse_arn(field_name: &str, value: &str) -> Result<Arn, AwsError> {
    value.parse::<Arn>().map_err(|error| {
        KinesisError::InvalidArgument {
            message: format!("{field_name} is not valid: {error}"),
        }
        .to_aws_error()
    })
}

fn unsupported_gap(message: &str) -> AwsError {
    KinesisError::UnsupportedOperation { message: message.to_owned() }
        .to_aws_error()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_runtime::{self, FixedClock};
    use aws::{ProtocolFamily, ServiceName};
    use serde_json::Value;
    use services::KinesisService;
    use std::sync::Arc;
    use std::time::UNIX_EPOCH;
    use storage::{StorageConfig, StorageFactory, StorageMode};

    fn service() -> KinesisService {
        let factory = StorageFactory::new(StorageConfig::new(
            "/tmp/http-kinesis-service",
            StorageMode::Memory,
        ));

        KinesisService::new(&factory, Arc::new(FixedClock::new(UNIX_EPOCH)))
    }

    fn context(operation: &str) -> RequestContext {
        RequestContext::try_new(
            "000000000000".parse().expect("account should parse"),
            "eu-west-2".parse().expect("region should parse"),
            ServiceName::Kinesis,
            ProtocolFamily::AwsJson11,
            operation,
            None,
            false,
        )
        .expect("request context should build")
    }

    fn request_bytes(target: &str, body: &str) -> Vec<u8> {
        format!(
            "POST / HTTP/1.1\r\nHost: localhost:4566\r\nContent-Type: application/x-amz-json-1.1\r\nX-Amz-Target: {target}\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        )
        .into_bytes()
    }

    fn json_call(
        service: &KinesisService,
        operation: &str,
        body: &str,
    ) -> Value {
        let request_bytes =
            request_bytes(&format!("Kinesis_20131202.{operation}"), body);
        let request =
            HttpRequest::parse(&request_bytes).expect("request should parse");
        let response = handle_json(service, &request, &context(operation))
            .expect("operation should succeed");

        serde_json::from_slice(&response)
            .expect("response should be valid JSON")
    }

    fn router() -> crate::runtime::EdgeRouter {
        test_runtime::router_with_http_forwarder("http-kinesis-router", None)
    }

    fn split_response(
        response: &[u8],
    ) -> (String, Vec<(String, String)>, String) {
        let header_end = response
            .windows(4)
            .position(|window| window == b"\r\n\r\n")
            .expect("response should contain a header terminator");
        let headers = std::str::from_utf8(&response[..header_end])
            .expect("response headers should be UTF-8");
        let body = std::str::from_utf8(&response[header_end + 4..])
            .expect("response body should be UTF-8");
        let mut lines = headers.split("\r\n");
        let status =
            lines.next().expect("status line should exist").to_owned();
        let parsed_headers = lines
            .map(|line| {
                line.split_once(':').expect("header should contain ':'")
            })
            .map(|(name, value)| (name.to_owned(), value.trim().to_owned()))
            .collect();

        (status, parsed_headers, body.to_owned())
    }

    fn header_value<'a>(
        headers: &'a [(String, String)],
        name: &str,
    ) -> Option<&'a str> {
        headers
            .iter()
            .find(|(header, _)| header.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }

    #[test]
    fn kinesis_json_direct_handler_round_trips_supported_operations() {
        let service = service();

        json_call(
            &service,
            "CreateStream",
            r#"{"StreamName":"events","ShardCount":1}"#,
        );
        let stream_description = json_call(
            &service,
            "DescribeStream",
            r#"{"StreamName":"events"}"#,
        );
        let stream_arn = stream_description["StreamDescription"]["StreamARN"]
            .as_str()
            .expect("stream arn should exist")
            .to_owned();
        let stream_summary = json_call(
            &service,
            "DescribeStreamSummary",
            r#"{"StreamARN":"arn:aws:kinesis:eu-west-2:000000000000:stream/events"}"#,
        );
        let list_streams =
            json_call(&service, "ListStreams", r#"{"Limit":10}"#);
        let put_record = json_call(
            &service,
            "PutRecord",
            r#"{"Data":"b25l","PartitionKey":"pk","StreamName":"events"}"#,
        );
        let put_records = json_call(
            &service,
            "PutRecords",
            r#"{"Records":[{"Data":"dHdv","PartitionKey":"pk"},{"Data":"dGhyZWU=","PartitionKey":"pk"}],"StreamName":"events"}"#,
        );
        let iterator = json_call(
            &service,
            "GetShardIterator",
            &format!(
                r#"{{"ShardId":"{}","ShardIteratorType":"TRIM_HORIZON","StreamARN":"{}"}}"#,
                put_record["ShardId"].as_str().expect("shard id should exist"),
                stream_arn,
            ),
        );
        let records = json_call(
            &service,
            "GetRecords",
            &format!(
                r#"{{"Limit":10,"ShardIterator":"{}"}}"#,
                iterator["ShardIterator"]
                    .as_str()
                    .expect("iterator should exist")
            ),
        );
        let registered = json_call(
            &service,
            "RegisterStreamConsumer",
            &format!(
                r#"{{"ConsumerName":"analytics","StreamARN":"{stream_arn}"}}"#
            ),
        );
        let described_consumer = json_call(
            &service,
            "DescribeStreamConsumer",
            &format!(
                r#"{{"ConsumerARN":"{}"}}"#,
                registered["Consumer"]["ConsumerARN"]
                    .as_str()
                    .expect("consumer arn should exist")
            ),
        );
        let listed_consumers = json_call(
            &service,
            "ListStreamConsumers",
            &format!(r#"{{"StreamARN":"{stream_arn}","MaxResults":10}}"#),
        );
        json_call(
            &service,
            "AddTagsToStream",
            r#"{"StreamName":"events","Tags":{"env":"dev","team":"core"}}"#,
        );
        let tags = json_call(
            &service,
            "ListTagsForStream",
            r#"{"StreamName":"events","Limit":10}"#,
        );
        json_call(
            &service,
            "RemoveTagsFromStream",
            r#"{"StreamName":"events","TagKeys":["env"]}"#,
        );
        json_call(
            &service,
            "StartStreamEncryption",
            r#"{"EncryptionType":"KMS","KeyId":"alias/cloudish","StreamName":"events"}"#,
        );
        let encrypted_summary = json_call(
            &service,
            "DescribeStreamSummary",
            r#"{"StreamName":"events"}"#,
        );
        json_call(
            &service,
            "StopStreamEncryption",
            r#"{"EncryptionType":"KMS","KeyId":"alias/cloudish","StreamName":"events"}"#,
        );
        json_call(
            &service,
            "SplitShard",
            &format!(
                r#"{{"NewStartingHashKey":"{}","ShardToSplit":"{}","StreamName":"events"}}"#,
                u128::MAX / 2,
                put_record["ShardId"].as_str().expect("shard id should exist"),
            ),
        );
        let listed_shards = json_call(
            &service,
            "ListShards",
            r#"{"StreamName":"events","MaxResults":10}"#,
        );
        let child_ids = listed_shards["Shards"]
            .as_array()
            .expect("shards should be an array")
            .iter()
            .filter(|shard| shard["ParentShardId"].is_string())
            .map(|shard| shard["ShardId"].as_str().expect("shard id"))
            .collect::<Vec<_>>();
        json_call(
            &service,
            "MergeShards",
            &format!(
                r#"{{"AdjacentShardToMerge":"{}","ShardToMerge":"{}","StreamName":"events"}}"#,
                child_ids[1], child_ids[0],
            ),
        );
        json_call(
            &service,
            "DeregisterStreamConsumer",
            &format!(
                r#"{{"ConsumerARN":"{}"}}"#,
                registered["Consumer"]["ConsumerARN"]
                    .as_str()
                    .expect("consumer arn should exist")
            ),
        );
        json_call(&service, "DeleteStream", r#"{"StreamName":"events"}"#);

        assert_eq!(
            stream_summary["StreamDescriptionSummary"]["OpenShardCount"],
            1
        );
        assert_eq!(list_streams["StreamNames"], json!(["events"]));
        assert_eq!(put_records["FailedRecordCount"], 0);
        assert_eq!(
            records["Records"]
                .as_array()
                .expect("records should be an array")
                .len(),
            3
        );
        assert_eq!(records["Records"][0]["Data"], "b25l");
        assert_eq!(
            described_consumer["ConsumerDescription"]["ConsumerName"],
            "analytics"
        );
        assert_eq!(
            listed_consumers["Consumers"]
                .as_array()
                .expect("consumers should be an array")
                .len(),
            1
        );
        assert_eq!(
            tags["Tags"].as_array().expect("tags should be an array").len(),
            2
        );
        assert_eq!(
            encrypted_summary["StreamDescriptionSummary"]["EncryptionType"],
            "KMS"
        );
        assert_eq!(
            listed_shards["Shards"]
                .as_array()
                .expect("shards should be an array")
                .len(),
            3
        );
    }

    #[test]
    fn kinesis_json_direct_handler_reports_explicit_errors_and_gap_paths() {
        let service = service();
        let missing_target = operation_from_target(None)
            .expect_err("missing target should fail");
        let wrong_prefix =
            operation_from_target(Some("AmazonSSM.GetParameter"))
                .expect_err("wrong prefix should fail");
        let invalid_data_request_bytes = request_bytes(
            "Kinesis_20131202.PutRecord",
            r#"{"Data":"%%%","PartitionKey":"pk","StreamName":"events"}"#,
        );
        let invalid_data_request =
            HttpRequest::parse(&invalid_data_request_bytes)
                .expect("request should parse");
        let invalid_data = handle_json(
            &service,
            &invalid_data_request,
            &context("PutRecord"),
        )
        .expect_err("invalid base64 should fail");
        let subscribe_request_bytes = request_bytes(
            "Kinesis_20131202.SubscribeToShard",
            r#"{"ConsumerARN":"arn","ShardId":"shardId-000000000000"}"#,
        );
        let subscribe_request = HttpRequest::parse(&subscribe_request_bytes)
            .expect("request should parse");
        let subscribe_error = handle_json(
            &service,
            &subscribe_request,
            &context("SubscribeToShard"),
        )
        .expect_err("subscribe gap should fail");
        let policy_request_bytes = request_bytes(
            "Kinesis_20131202.PutResourcePolicy",
            r#"{"Policy":"{}","ResourceARN":"arn"}"#,
        );
        let policy_request = HttpRequest::parse(&policy_request_bytes)
            .expect("request should parse");
        let policy_error = handle_json(
            &service,
            &policy_request,
            &context("PutResourcePolicy"),
        )
        .expect_err("policy gap should fail");

        assert_eq!(
            missing_target.to_aws_error().code(),
            "UnsupportedOperation"
        );
        assert_eq!(wrong_prefix.to_aws_error().code(), "UnsupportedOperation");
        assert_eq!(invalid_data.code(), "InvalidArgumentException");
        assert_eq!(subscribe_error.code(), "UnsupportedOperation");
        assert_eq!(policy_error.code(), "UnsupportedOperation");
    }

    #[test]
    fn kinesis_json_router_round_trips_aws_json_requests() {
        let router = router();
        let create = router.handle_bytes(&request_bytes(
            "Kinesis_20131202.CreateStream",
            r#"{"StreamName":"events","ShardCount":1}"#,
        ));
        let describe = router.handle_bytes(&request_bytes(
            "Kinesis_20131202.DescribeStream",
            r#"{"StreamName":"events"}"#,
        ));
        let put = router.handle_bytes(&request_bytes(
            "Kinesis_20131202.PutRecord",
            r#"{"Data":"b25l","PartitionKey":"pk","StreamName":"events"}"#,
        ));
        let describe_body: Value = {
            let (_, _, body) = split_response(&describe.to_http_bytes());
            serde_json::from_str(&body).expect("describe body should be JSON")
        };
        let shard_id =
            describe_body["StreamDescription"]["Shards"][0]["ShardId"]
                .as_str()
                .expect("shard id should exist");
        let iterator = router.handle_bytes(&request_bytes(
            "Kinesis_20131202.GetShardIterator",
            &format!(
                r#"{{"ShardId":"{shard_id}","ShardIteratorType":"TRIM_HORIZON","StreamName":"events"}}"#
            ),
        ));
        let iterator_body: Value = {
            let (_, _, body) = split_response(&iterator.to_http_bytes());
            serde_json::from_str(&body).expect("iterator body should be JSON")
        };
        let get_records = router.handle_bytes(&request_bytes(
            "Kinesis_20131202.GetRecords",
            &format!(
                r#"{{"ShardIterator":"{}","Limit":10}}"#,
                iterator_body["ShardIterator"]
                    .as_str()
                    .expect("iterator should exist")
            ),
        ));
        let invalid = router.handle_bytes(&request_bytes(
            "Kinesis_20131202.GetRecords",
            r#"{"ShardIterator":"bogus","Limit":10}"#,
        ));

        let (create_status, create_headers, _) =
            split_response(&create.to_http_bytes());
        let (put_status, _, _) = split_response(&put.to_http_bytes());
        let (records_status, records_headers, records_body) =
            split_response(&get_records.to_http_bytes());
        let (invalid_status, _, invalid_body) =
            split_response(&invalid.to_http_bytes());
        let records_json: Value =
            serde_json::from_str(&records_body).expect("records body JSON");
        let invalid_json: Value =
            serde_json::from_str(&invalid_body).expect("invalid body JSON");

        assert_eq!(create_status, "HTTP/1.1 200 OK");
        assert_eq!(put_status, "HTTP/1.1 200 OK");
        assert_eq!(records_status, "HTTP/1.1 200 OK");
        assert_eq!(
            header_value(&create_headers, "content-type"),
            Some("application/x-amz-json-1.1")
        );
        assert_eq!(
            header_value(&records_headers, "content-type"),
            Some("application/x-amz-json-1.1")
        );
        assert_eq!(
            records_json["Records"]
                .as_array()
                .expect("records array should exist")
                .len(),
            1
        );
        assert_eq!(records_json["Records"][0]["Data"], "b25l");
        assert_eq!(invalid_status, "HTTP/1.1 400 Bad Request");
        assert_eq!(invalid_json["__type"], "InvalidArgumentException");
    }
}
