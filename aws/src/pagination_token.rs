use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;
use thiserror::Error;

#[derive(
    Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(try_from = "String", into = "String")]
pub struct PaginationToken(String);

impl PaginationToken {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for PaginationToken {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Display for PaginationToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for PaginationToken {
    type Err = PaginationTokenError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.trim().is_empty() {
            return Err(PaginationTokenError::Empty);
        }

        if let Some((index, character)) =
            value.char_indices().find(|(_, character)| character.is_control())
        {
            return Err(PaginationTokenError::InvalidCharacter {
                index,
                character,
            });
        }

        Ok(Self(value.to_owned()))
    }
}

impl TryFrom<String> for PaginationToken {
    type Error = PaginationTokenError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}

impl From<PaginationToken> for String {
    fn from(value: PaginationToken) -> Self {
        value.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum PaginationTokenError {
    #[error("pagination token cannot be blank")]
    Empty,
    #[error(
        "pagination token cannot contain control characters, found {character:?} at index {index}"
    )]
    InvalidCharacter { index: usize, character: char },
}

#[cfg(test)]
mod tests {
    use super::{PaginationToken, PaginationTokenError};

    #[test]
    fn parse_pagination_token() {
        let token: PaginationToken =
            "AQICAHj/==".parse().expect("pagination token should parse");

        assert_eq!(token.as_str(), "AQICAHj/==");
        assert_eq!(token.as_ref(), "AQICAHj/==");
        assert_eq!(token.to_string(), "AQICAHj/==");
    }

    #[test]
    fn pagination_token_supports_string_conversions() {
        let token = PaginationToken::try_from("AQICAHj/==".to_owned())
            .expect("token should convert from String");

        assert_eq!(token.as_ref(), "AQICAHj/==");
        assert_eq!(String::from(token), "AQICAHj/==");
    }

    #[test]
    fn reject_blank_pagination_token() {
        let error = "   "
            .parse::<PaginationToken>()
            .expect_err("blank token must fail");

        assert_eq!(error, PaginationTokenError::Empty);
        assert_eq!(error.to_string(), "pagination token cannot be blank");
    }

    #[test]
    fn reject_pagination_token_with_control_characters() {
        let error = "abc\n"
            .parse::<PaginationToken>()
            .expect_err("token with control characters must fail");

        assert_eq!(
            error,
            PaginationTokenError::InvalidCharacter {
                index: 3,
                character: '\n',
            }
        );
        assert_eq!(
            error.to_string(),
            "pagination token cannot contain control characters, found '\\n' at index 3"
        );
    }
}
