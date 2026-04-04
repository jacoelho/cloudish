use crate::errors::KinesisError;
use crate::records::KinesisRecord;
use aws::Arn;
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use serde::{Deserialize, Serialize};

pub(crate) const TOKEN_KIND_CONSUMERS: &str = "consumers";
pub(crate) const TOKEN_KIND_SHARDS: &str = "shards";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum KinesisShardIteratorType {
    #[serde(rename = "AFTER_SEQUENCE_NUMBER")]
    AfterSequenceNumber,
    #[serde(rename = "AT_SEQUENCE_NUMBER")]
    AtSequenceNumber,
    #[serde(rename = "AT_TIMESTAMP")]
    AtTimestamp,
    #[serde(rename = "LATEST")]
    Latest,
    #[serde(rename = "TRIM_HORIZON")]
    TrimHorizon,
}

impl KinesisShardIteratorType {
    /// # Errors
    ///
    /// Returns [`KinesisError::InvalidArgument`] when the iterator type is not
    /// supported by the API.
    pub fn parse(value: &str) -> Result<Self, KinesisError> {
        match value {
            "AFTER_SEQUENCE_NUMBER" => Ok(Self::AfterSequenceNumber),
            "AT_SEQUENCE_NUMBER" => Ok(Self::AtSequenceNumber),
            "AT_TIMESTAMP" => Ok(Self::AtTimestamp),
            "LATEST" => Ok(Self::Latest),
            "TRIM_HORIZON" => Ok(Self::TrimHorizon),
            _ => Err(KinesisError::InvalidArgument {
                message: format!(
                    "Unsupported ShardIteratorType value `{value}`."
                ),
            }),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetShardIteratorInput {
    pub shard_id: String,
    pub shard_iterator_type: KinesisShardIteratorType,
    pub starting_sequence_number: Option<String>,
    pub stream: crate::KinesisStreamIdentifier,
    pub timestamp: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct GetShardIteratorOutput {
    pub shard_iterator: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetRecordsInput {
    pub limit: Option<u32>,
    pub shard_iterator: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct GetRecordsOutput {
    pub millis_behind_latest: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_shard_iterator: Option<String>,
    pub records: Vec<KinesisRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct IteratorToken {
    pub position: usize,
    pub shard_id: String,
    pub stream_arn: Arn,
}

pub(crate) fn iterator_position(
    records: &[KinesisRecord],
    iterator_type: KinesisShardIteratorType,
    starting_sequence_number: Option<&str>,
    timestamp: Option<u64>,
) -> Result<usize, KinesisError> {
    match iterator_type {
        KinesisShardIteratorType::TrimHorizon => Ok(0),
        KinesisShardIteratorType::Latest => Ok(records.len()),
        KinesisShardIteratorType::AtSequenceNumber => {
            let Some(sequence_number) = starting_sequence_number else {
                return Err(KinesisError::InvalidArgument {
                    message:
                        "StartingSequenceNumber is required for AT_SEQUENCE_NUMBER iterators."
                            .to_owned(),
                });
            };
            find_sequence_position(records, sequence_number, false)
        }
        KinesisShardIteratorType::AfterSequenceNumber => {
            let Some(sequence_number) = starting_sequence_number else {
                return Err(KinesisError::InvalidArgument {
                    message:
                        "StartingSequenceNumber is required for AFTER_SEQUENCE_NUMBER iterators."
                            .to_owned(),
                });
            };
            find_sequence_position(records, sequence_number, true)
        }
        KinesisShardIteratorType::AtTimestamp => {
            let Some(timestamp) = timestamp else {
                return Err(KinesisError::InvalidArgument {
                    message:
                        "Timestamp is required for AT_TIMESTAMP iterators."
                            .to_owned(),
                });
            };
            Ok(records
                .iter()
                .position(|record| {
                    record.approximate_arrival_timestamp >= timestamp
                })
                .unwrap_or(records.len()))
        }
    }
}

fn find_sequence_position(
    records: &[KinesisRecord],
    sequence_number: &str,
    after: bool,
) -> Result<usize, KinesisError> {
    let Some(index) = records
        .iter()
        .position(|record| record.sequence_number == sequence_number)
    else {
        return Err(KinesisError::InvalidArgument {
            message: format!(
                "Sequence number {sequence_number} was not found in the shard."
            ),
        });
    };

    Ok(index.saturating_add(usize::from(after)))
}

pub(crate) fn encode_iterator_token(token: &IteratorToken) -> String {
    BASE64_STANDARD.encode(format!(
        "{}|{}|{}",
        token.stream_arn, token.shard_id, token.position
    ))
}

pub(crate) fn decode_iterator_token(
    value: &str,
) -> Result<IteratorToken, KinesisError> {
    let decoded = BASE64_STANDARD.decode(value).map_err(|_| {
        KinesisError::InvalidArgument {
            message: "The provided ShardIterator is invalid.".to_owned(),
        }
    })?;
    let decoded = String::from_utf8(decoded).map_err(|_| {
        KinesisError::InvalidArgument {
            message: "The provided ShardIterator is invalid.".to_owned(),
        }
    })?;
    let mut sections = decoded.splitn(3, '|');
    let stream_arn = sections.next().unwrap_or_default();
    let shard_id = sections.next().unwrap_or_default();
    let position = sections.next().unwrap_or_default();
    if stream_arn.is_empty() || shard_id.is_empty() || position.is_empty() {
        return Err(KinesisError::InvalidArgument {
            message: "The provided ShardIterator is invalid.".to_owned(),
        });
    }

    let position = position.parse::<usize>().map_err(|_| {
        KinesisError::InvalidArgument {
            message: "The provided ShardIterator is invalid.".to_owned(),
        }
    })?;
    let stream_arn = stream_arn.parse::<Arn>().map_err(|_| {
        KinesisError::InvalidArgument {
            message: "The provided ShardIterator is invalid.".to_owned(),
        }
    })?;

    Ok(IteratorToken { position, shard_id: shard_id.to_owned(), stream_arn })
}

pub(crate) fn encode_scoped_token(
    kind: &str,
    scope: &(impl std::fmt::Display + ?Sized),
    index: usize,
) -> String {
    BASE64_STANDARD.encode(format!("{kind}|{scope}|{index}"))
}

pub(crate) fn decode_scoped_token(
    token: &str,
    expected_kind: &str,
) -> Result<(String, usize), KinesisError> {
    let decoded = BASE64_STANDARD.decode(token).map_err(|_| {
        KinesisError::ExpiredNextToken {
            message: "The provided NextToken is invalid.".to_owned(),
        }
    })?;
    let decoded = String::from_utf8(decoded).map_err(|_| {
        KinesisError::ExpiredNextToken {
            message: "The provided NextToken is invalid.".to_owned(),
        }
    })?;
    let mut parts = decoded.splitn(3, '|');
    let kind = parts.next().unwrap_or_default();
    let scope = parts.next().unwrap_or_default();
    let index = parts.next().unwrap_or_default();
    if kind != expected_kind || scope.is_empty() || index.is_empty() {
        return Err(KinesisError::ExpiredNextToken {
            message: "The provided NextToken is invalid.".to_owned(),
        });
    }

    let index = index.parse::<usize>().map_err(|_| {
        KinesisError::ExpiredNextToken {
            message: "The provided NextToken is invalid.".to_owned(),
        }
    })?;

    Ok((scope.to_owned(), index))
}

pub(crate) fn decode_index_token(token: &str) -> Result<usize, KinesisError> {
    token.parse::<usize>().map_err(|_| KinesisError::ExpiredNextToken {
        message: "The provided NextToken is invalid.".to_owned(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stream_arn(value: &str) -> Arn {
        value.parse().expect("test stream arn should parse")
    }

    #[test]
    fn kinesis_iterator_and_pagination_tokens_round_trip() {
        let token = encode_iterator_token(&IteratorToken {
            position: 7,
            shard_id: "shardId-000000000001".to_owned(),
            stream_arn: stream_arn(
                "arn:aws:kinesis:eu-west-2:000000000000:stream/demo",
            ),
        });
        let decoded = decode_iterator_token(&token)
            .expect("iterator token should decode");
        let scoped = encode_scoped_token(TOKEN_KIND_SHARDS, "demo", 3);

        assert_eq!(decoded.position, 7);
        assert_eq!(
            decode_scoped_token(&scoped, TOKEN_KIND_SHARDS)
                .expect("scoped token should decode"),
            ("demo".to_owned(), 3)
        );
        assert_eq!(
            decode_index_token("bogus").unwrap_err().to_aws_error().code(),
            "ExpiredNextTokenException"
        );
        assert_eq!(
            decode_iterator_token("bogus")
                .expect_err("invalid iterator token should fail")
                .to_aws_error()
                .code(),
            "InvalidArgumentException"
        );
    }
}
