use crate::errors::KinesisError;
use crate::records::KinesisRecord;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListShardsInput {
    pub exclusive_start_shard_id: Option<String>,
    pub max_results: Option<u32>,
    pub next_token: Option<String>,
    pub stream: Option<crate::KinesisStreamIdentifier>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct ListShardsOutput {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_token: Option<String>,
    pub shards: Vec<KinesisShard>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SplitShardInput {
    pub new_starting_hash_key: String,
    pub shard_to_split: String,
    pub stream: crate::KinesisStreamIdentifier,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergeShardsInput {
    pub adjacent_shard_to_merge: String,
    pub shard_to_merge: String,
    pub stream: crate::KinesisStreamIdentifier,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct KinesisHashKeyRange {
    pub ending_hash_key: String,
    pub starting_hash_key: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct KinesisSequenceNumberRange {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ending_sequence_number: Option<String>,
    pub starting_sequence_number: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct KinesisShard {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub adjacent_parent_shard_id: Option<String>,
    pub hash_key_range: KinesisHashKeyRange,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_shard_id: Option<String>,
    pub sequence_number_range: KinesisSequenceNumberRange,
    pub shard_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct StoredShard {
    pub adjacent_parent_shard_id: Option<String>,
    pub closed: bool,
    pub hash_key_range: KinesisHashKeyRange,
    pub parent_shard_id: Option<String>,
    pub records: Vec<KinesisRecord>,
    pub sequence_number_range: KinesisSequenceNumberRange,
    pub shard_id: String,
}

pub(crate) fn public_shards(shards: &[StoredShard]) -> Vec<KinesisShard> {
    let mut shards = shards
        .iter()
        .map(|shard| KinesisShard {
            adjacent_parent_shard_id: shard.adjacent_parent_shard_id.clone(),
            hash_key_range: shard.hash_key_range.clone(),
            parent_shard_id: shard.parent_shard_id.clone(),
            sequence_number_range: shard.sequence_number_range.clone(),
            shard_id: shard.shard_id.clone(),
        })
        .collect::<Vec<_>>();
    shards.sort_by(|left, right| left.shard_id.cmp(&right.shard_id));
    shards
}

pub(crate) fn shard_start_index(
    shards: &[KinesisShard],
    exclusive_start_shard_id: Option<&str>,
) -> Result<usize, KinesisError> {
    match exclusive_start_shard_id {
        None => Ok(0),
        Some(shard_id) => shards
            .iter()
            .position(|shard| shard.shard_id == shard_id)
            .map(|index| index.saturating_add(1))
            .ok_or_else(|| KinesisError::InvalidArgument {
                message: format!(
                    "Shard {shard_id} was not found for pagination."
                ),
            }),
    }
}

pub(crate) fn build_initial_shards(
    shard_count: usize,
    starting_sequence_number: &str,
) -> Result<Vec<StoredShard>, KinesisError> {
    let ranges = split_hash_key_space(shard_count)?;

    Ok(ranges
        .into_iter()
        .enumerate()
        .map(|(index, hash_key_range)| StoredShard {
            adjacent_parent_shard_id: None,
            closed: false,
            hash_key_range,
            parent_shard_id: None,
            records: Vec::new(),
            sequence_number_range: KinesisSequenceNumberRange {
                ending_sequence_number: None,
                starting_sequence_number: starting_sequence_number.to_owned(),
            },
            shard_id: format_shard_id(index as u64),
        })
        .collect())
}

pub(crate) fn split_hash_key_space(
    shard_count: usize,
) -> Result<Vec<KinesisHashKeyRange>, KinesisError> {
    if shard_count == 0 {
        return Err(KinesisError::InvalidArgument {
            message: "ShardCount must be greater than zero.".to_owned(),
        });
    }
    if shard_count == 1 {
        return Ok(vec![KinesisHashKeyRange {
            ending_hash_key: u128::MAX.to_string(),
            starting_hash_key: "0".to_owned(),
        }]);
    }

    let divisor = shard_count as u128;
    let quotient = u128::MAX.checked_div(divisor).ok_or_else(|| {
        KinesisError::InvalidArgument {
            message: "ShardCount must be greater than zero.".to_owned(),
        }
    })?;
    let remainder = u128::MAX.checked_rem(divisor).ok_or_else(|| {
        KinesisError::InvalidArgument {
            message: "ShardCount must be greater than zero.".to_owned(),
        }
    })?;
    let remainder_plus_one = remainder.saturating_add(1);
    let (base_size, extra_segments) = if remainder_plus_one == divisor {
        (quotient.saturating_add(1), 0)
    } else {
        (quotient, remainder_plus_one)
    };
    let mut next_start = 0_u128;
    let mut ranges = Vec::with_capacity(shard_count);

    for index in 0..shard_count {
        let size = base_size.saturating_add(
            u128::from((index as u128) < extra_segments),
        );
        let end = if index.saturating_add(1) == shard_count {
            u128::MAX
        } else {
            next_start.saturating_add(size).saturating_sub(1)
        };
        ranges.push(KinesisHashKeyRange {
            ending_hash_key: end.to_string(),
            starting_hash_key: next_start.to_string(),
        });
        next_start = end.saturating_add(1);
    }

    Ok(ranges)
}

pub(crate) fn split_hash_key_range(
    range: &KinesisHashKeyRange,
    new_starting_hash_key: &str,
) -> Result<(KinesisHashKeyRange, KinesisHashKeyRange), KinesisError> {
    let start = parse_hash_key(&range.starting_hash_key)?;
    let end = parse_hash_key(&range.ending_hash_key)?;
    let split = parse_hash_key(new_starting_hash_key)?;
    if split <= start || split > end {
        return Err(KinesisError::InvalidArgument {
            message:
                "NewStartingHashKey must be greater than the current starting hash key and within the shard range."
                    .to_owned(),
        });
    }

    Ok((
        KinesisHashKeyRange {
            ending_hash_key: split.saturating_sub(1).to_string(),
            starting_hash_key: start.to_string(),
        },
        KinesisHashKeyRange {
            ending_hash_key: end.to_string(),
            starting_hash_key: split.to_string(),
        },
    ))
}

pub(crate) fn merge_hash_key_ranges(
    left: &KinesisHashKeyRange,
    right: &KinesisHashKeyRange,
) -> Result<KinesisHashKeyRange, KinesisError> {
    let left_start = parse_hash_key(&left.starting_hash_key)?;
    let left_end = parse_hash_key(&left.ending_hash_key)?;
    let right_start = parse_hash_key(&right.starting_hash_key)?;
    let right_end = parse_hash_key(&right.ending_hash_key)?;
    let adjacent = left_end
        .checked_add(1)
        .is_some_and(|value| value == right_start)
        || right_end.checked_add(1).is_some_and(|value| value == left_start);
    if !adjacent {
        return Err(KinesisError::InvalidArgument {
            message: "Shards must be adjacent to merge.".to_owned(),
        });
    }

    Ok(KinesisHashKeyRange {
        ending_hash_key: left_end.max(right_end).to_string(),
        starting_hash_key: left_start.min(right_start).to_string(),
    })
}

pub(crate) fn parse_hash_key(value: &str) -> Result<u128, KinesisError> {
    value.parse::<u128>().map_err(|_| KinesisError::InvalidArgument {
        message: format!("Hash key {value} is not valid."),
    })
}

pub(crate) fn select_writable_shard(
    shards: &[StoredShard],
    hash_key: u128,
) -> Result<usize, KinesisError> {
    shards
        .iter()
        .enumerate()
        .find(|(_, shard)| {
            !shard.closed
                && hash_key_in_range(hash_key, &shard.hash_key_range)
                    .unwrap_or(false)
        })
        .map(|(index, _)| index)
        .ok_or_else(|| KinesisError::InternalFailure {
            message:
                "No writable shard was available for the computed hash key."
                    .to_owned(),
        })
}

fn hash_key_in_range(
    hash_key: u128,
    range: &KinesisHashKeyRange,
) -> Result<bool, KinesisError> {
    let start = parse_hash_key(&range.starting_hash_key)?;
    let end = parse_hash_key(&range.ending_hash_key)?;

    Ok((start..=end).contains(&hash_key))
}

pub(crate) fn next_shard_id(next_shard_index: &mut u64) -> String {
    let shard_id = format_shard_id(*next_shard_index);
    *next_shard_index = next_shard_index.saturating_add(1);
    shard_id
}

pub(crate) fn format_shard_id(index: u64) -> String {
    format!("shardId-{index:012}")
}

pub(crate) fn format_sequence_number(value: u64) -> String {
    format!("{value:020}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kinesis_hash_ranges_are_contiguous_and_mergeable() {
        let ranges =
            split_hash_key_space(3).expect("hash key ranges should build");
        let left = ranges[0].clone();
        let middle = ranges[1].clone();

        assert_eq!(ranges.len(), 3);
        assert_eq!(left.starting_hash_key, "0");
        assert_eq!(
            parse_hash_key(&left.ending_hash_key)
                .expect("ending hash key should parse")
                .saturating_add(1),
            parse_hash_key(&middle.starting_hash_key)
                .expect("starting hash key should parse")
        );
        assert!(
            merge_hash_key_ranges(&left, &middle)
                .expect("adjacent ranges should merge")
                .ending_hash_key
                .parse::<u128>()
                .expect("merged hash key should parse")
                >= parse_hash_key(&middle.ending_hash_key)
                    .expect("ending hash key should parse")
        );
    }
}
