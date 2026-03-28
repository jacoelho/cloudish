use crate::errors::KinesisError;
use crate::streams::KinesisEncryptionType;
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PutRecordInput {
    pub data: Vec<u8>,
    pub explicit_hash_key: Option<String>,
    pub partition_key: String,
    pub sequence_number_for_ordering: Option<String>,
    pub stream: crate::KinesisStreamIdentifier,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct PutRecordOutput {
    pub encryption_type: KinesisEncryptionType,
    pub sequence_number: String,
    pub shard_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PutRecordsInput {
    pub records: Vec<PutRecordsEntry>,
    pub stream: crate::KinesisStreamIdentifier,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PutRecordsEntry {
    pub data: Vec<u8>,
    pub explicit_hash_key: Option<String>,
    pub partition_key: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct PutRecordsResultEntry {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sequence_number: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub shard_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct PutRecordsOutput {
    pub encryption_type: KinesisEncryptionType,
    pub failed_record_count: u32,
    pub records: Vec<PutRecordsResultEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct KinesisRecord {
    pub approximate_arrival_timestamp: u64,
    #[serde(
        serialize_with = "serialize_blob",
        deserialize_with = "deserialize_blob"
    )]
    pub data: Vec<u8>,
    pub encryption_type: KinesisEncryptionType,
    pub partition_key: String,
    pub sequence_number: String,
}

pub(crate) fn validate_partition_key(value: &str) -> Result<(), KinesisError> {
    if value.is_empty() {
        return Err(KinesisError::InvalidArgument {
            message: "PartitionKey must not be empty.".to_owned(),
        });
    }

    Ok(())
}

pub(crate) fn effective_hash_key(
    partition_key: &str,
    explicit_hash_key: Option<&str>,
) -> Result<u128, KinesisError> {
    if let Some(explicit_hash_key) = explicit_hash_key {
        return crate::shards::parse_hash_key(explicit_hash_key);
    }

    let bytes = md5::compute(partition_key.as_bytes()).0;
    Ok(u128::from_be_bytes(bytes))
}

fn serialize_blob<S>(value: &[u8], serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(&BASE64_STANDARD.encode(value))
}

fn deserialize_blob<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
where
    D: Deserializer<'de>,
{
    let encoded = String::deserialize(deserializer)?;
    BASE64_STANDARD.decode(encoded).map_err(D::Error::custom)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kinesis_record_blob_and_explicit_hash_key_round_trip() {
        let record = KinesisRecord {
            approximate_arrival_timestamp: 1,
            data: b"hello".to_vec(),
            encryption_type: KinesisEncryptionType::None,
            partition_key: "pk".to_owned(),
            sequence_number: "00000000000000000001".to_owned(),
        };

        let serialized =
            serde_json::to_string(&record).expect("record should serialize");
        let round_trip = serde_json::from_str::<KinesisRecord>(&serialized)
            .expect("record should deserialize");

        assert_eq!(round_trip, record);
        assert!(serialized.contains("aGVsbG8="));
        assert_eq!(
            effective_hash_key("ignored", Some("17"))
                .expect("explicit hash key should parse"),
            17
        );
    }
}
