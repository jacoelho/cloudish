use aws::{AccountId, RegionId};
use serde::{Deserialize, Serialize};
use std::borrow::Borrow;
use std::fmt;

use crate::errors::DynamoDbError;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct DynamoDbScope {
    account_id: AccountId,
    region: RegionId,
}

impl DynamoDbScope {
    pub fn new(account_id: AccountId, region: RegionId) -> Self {
        Self { account_id, region }
    }

    pub fn account_id(&self) -> &AccountId {
        &self.account_id
    }

    pub fn region(&self) -> &RegionId {
        &self.region
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
pub struct TableName(String);

impl TableName {
    /// # Errors
    ///
    /// Returns an error when the table name is blank.
    pub fn parse(value: impl AsRef<str>) -> Result<Self, DynamoDbError> {
        let value = value.as_ref();
        if value.trim().is_empty() {
            return Err(DynamoDbError::Validation {
                message: "TableName is required.".to_owned(),
            });
        }

        Ok(Self(value.to_owned()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for TableName {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl PartialEq<&str> for TableName {
    fn eq(&self, other: &&str) -> bool {
        self.as_str() == *other
    }
}

impl PartialEq<String> for TableName {
    fn eq(&self, other: &String) -> bool {
        self.as_str() == other
    }
}

impl Borrow<str> for TableName {
    fn borrow(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Display for TableName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}
