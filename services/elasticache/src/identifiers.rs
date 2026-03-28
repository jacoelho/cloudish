use crate::errors::ElastiCacheError;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(try_from = "String", into = "String")]
pub struct ElastiCacheReplicationGroupId(String);

impl ElastiCacheReplicationGroupId {
    /// # Errors
    ///
    /// Returns [`ElastiCacheError::InvalidParameterValue`] when the identifier
    /// is blank or contains unsupported characters.
    pub fn new(value: impl AsRef<str>) -> Result<Self, ElastiCacheError> {
        normalize_identifier("ReplicationGroupId", value.as_ref()).map(Self)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for ElastiCacheReplicationGroupId {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Display for ElastiCacheReplicationGroupId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl From<ElastiCacheReplicationGroupId> for String {
    fn from(value: ElastiCacheReplicationGroupId) -> Self {
        value.0
    }
}

impl TryFrom<String> for ElastiCacheReplicationGroupId {
    type Error = ElastiCacheError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl PartialEq<&str> for ElastiCacheReplicationGroupId {
    fn eq(&self, other: &&str) -> bool {
        self.as_str() == *other
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(try_from = "String", into = "String")]
pub struct ElastiCacheUserId(String);

impl ElastiCacheUserId {
    /// # Errors
    ///
    /// Returns [`ElastiCacheError::InvalidParameterValue`] when the identifier
    /// is blank or contains unsupported characters.
    pub fn new(value: impl AsRef<str>) -> Result<Self, ElastiCacheError> {
        normalize_identifier("UserId", value.as_ref()).map(Self)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for ElastiCacheUserId {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Display for ElastiCacheUserId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl From<ElastiCacheUserId> for String {
    fn from(value: ElastiCacheUserId) -> Self {
        value.0
    }
}

impl TryFrom<String> for ElastiCacheUserId {
    type Error = ElastiCacheError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl PartialEq<&str> for ElastiCacheUserId {
    fn eq(&self, other: &&str) -> bool {
        self.as_str() == *other
    }
}

fn normalize_identifier(
    name: &str,
    value: &str,
) -> Result<String, ElastiCacheError> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(ElastiCacheError::InvalidParameterValue {
            message: format!("{name} is required."),
        });
    }
    let normalized = trimmed.to_ascii_lowercase();
    let mut characters = normalized.chars();
    let Some(first) = characters.next() else {
        return Err(ElastiCacheError::InvalidParameterValue {
            message: format!("{name} is required."),
        });
    };
    if !first.is_ascii_alphabetic()
        || !characters.all(|character| {
            character.is_ascii_alphanumeric() || character == '-'
        })
    {
        return Err(ElastiCacheError::InvalidParameterValue {
            message: format!(
                "{name} must start with an ASCII letter and contain only ASCII \
                 letters, digits, or hyphens."
            ),
        });
    }

    Ok(normalized)
}

#[cfg(test)]
mod tests {
    use super::{ElastiCacheReplicationGroupId, ElastiCacheUserId};
    use crate::errors::ElastiCacheError;

    #[test]
    fn elasticache_identifiers_normalize_and_round_trip() {
        let replication_group =
            ElastiCacheReplicationGroupId::new(" Cache-A ")
                .expect("replication group id should normalize");
        let user_id = ElastiCacheUserId::try_from("User-01".to_owned())
            .expect("user id should convert from String");

        assert_eq!(replication_group, "cache-a");
        assert_eq!(replication_group.as_ref(), "cache-a");
        assert_eq!(replication_group.to_string(), "cache-a");
        assert_eq!(String::from(replication_group), "cache-a".to_owned());
        assert_eq!(user_id, "user-01");
    }

    #[test]
    fn elasticache_identifiers_reject_blank_and_invalid_values() {
        let blank = ElastiCacheReplicationGroupId::new("   ")
            .expect_err("replication group ids should reject blanks");
        let invalid = ElastiCacheUserId::new("1bad")
            .expect_err("user ids should reject non-letter prefixes");

        assert_eq!(
            blank,
            ElastiCacheError::InvalidParameterValue {
                message: "ReplicationGroupId is required.".to_owned(),
            }
        );
        assert_eq!(
            invalid,
            ElastiCacheError::InvalidParameterValue {
                message:
                    "UserId must start with an ASCII letter and contain only ASCII letters, digits, or hyphens."
                        .to_owned(),
            }
        );
    }
}
