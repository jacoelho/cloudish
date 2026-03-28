use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;
use thiserror::Error;

const ACCOUNT_ID_LENGTH: usize = 12;

#[derive(
    Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(try_from = "String", into = "String")]
pub struct AccountId(String);

impl AccountId {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for AccountId {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Display for AccountId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for AccountId {
    type Err = AccountIdError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.len() != ACCOUNT_ID_LENGTH {
            return Err(AccountIdError::InvalidLength { actual: value.len() });
        }

        if let Some((index, character)) = value
            .char_indices()
            .find(|(_, character)| !character.is_ascii_digit())
        {
            return Err(AccountIdError::InvalidCharacter { index, character });
        }

        Ok(Self(value.to_owned()))
    }
}

impl TryFrom<String> for AccountId {
    type Error = AccountIdError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}

impl From<AccountId> for String {
    fn from(value: AccountId) -> Self {
        value.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum AccountIdError {
    #[error("account id must be 12 digits, got {actual} characters")]
    InvalidLength { actual: usize },
    #[error(
        "account id must contain only ASCII digits, found {character:?} at index {index}"
    )]
    InvalidCharacter { index: usize, character: char },
}

#[cfg(test)]
mod tests {
    use super::{AccountId, AccountIdError};

    #[test]
    fn parse_valid_account_id() {
        let account_id: AccountId =
            "123456789012".parse().expect("valid account id should parse");

        assert_eq!(account_id.as_str(), "123456789012");
        assert_eq!(account_id.as_ref(), "123456789012");
        assert_eq!(account_id.to_string(), "123456789012");
    }

    #[test]
    fn account_id_supports_string_conversions() {
        let account_id = AccountId::try_from("123456789012".to_owned())
            .expect("account id should convert from String");

        assert_eq!(account_id.as_ref(), "123456789012");
        assert_eq!(String::from(account_id), "123456789012");
    }

    #[test]
    fn reject_account_id_with_wrong_length() {
        let error = "1234"
            .parse::<AccountId>()
            .expect_err("short account id must fail");

        assert_eq!(error, AccountIdError::InvalidLength { actual: 4 });
        assert_eq!(
            error.to_string(),
            "account id must be 12 digits, got 4 characters"
        );
    }

    #[test]
    fn reject_account_id_with_non_digit_characters() {
        let error = "12345678901a"
            .parse::<AccountId>()
            .expect_err("non-digit account id must fail");

        assert_eq!(
            error,
            AccountIdError::InvalidCharacter { index: 11, character: 'a' }
        );
        assert_eq!(
            error.to_string(),
            "account id must contain only ASCII digits, found 'a' at index 11"
        );
    }
}
