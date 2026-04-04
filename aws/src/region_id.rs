use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;
use thiserror::Error;

/// AWS-style region identifiers. This stays intentionally permissive enough to
/// allow future and emulator-test region names such as `pluto-central-2a`.
#[derive(
    Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(try_from = "String", into = "String")]
pub struct RegionId(String);

impl RegionId {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for RegionId {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Display for RegionId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for RegionId {
    type Err = RegionIdError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.is_empty() {
            return Err(RegionIdError::Empty);
        }

        let Some((last_index, _)) = value.char_indices().last() else {
            return Err(RegionIdError::Empty);
        };
        let mut saw_separator = false;
        let mut previous_was_separator = false;

        for (index, character) in value.char_indices() {
            match character {
                'a'..='z' | '0'..='9' => previous_was_separator = false,
                '-' => {
                    if index == 0
                        || index == last_index
                        || previous_was_separator
                    {
                        return Err(RegionIdError::InvalidSeparator { index });
                    }

                    saw_separator = true;
                    previous_was_separator = true;
                }
                _ => {
                    return Err(RegionIdError::InvalidCharacter {
                        index,
                        character,
                    });
                }
            }
        }

        if !saw_separator {
            return Err(RegionIdError::MissingSeparator);
        }

        Ok(Self(value.to_owned()))
    }
}

impl TryFrom<String> for RegionId {
    type Error = RegionIdError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}

impl From<RegionId> for String {
    fn from(value: RegionId) -> Self {
        value.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum RegionIdError {
    #[error("region id cannot be empty")]
    Empty,
    #[error("region id must contain at least one '-' separator")]
    MissingSeparator,
    #[error("region id contains an invalid '-' separator at index {index}")]
    InvalidSeparator { index: usize },
    #[error(
        "region id must contain only lowercase ASCII letters, digits, and '-', found {character:?} at index {index}"
    )]
    InvalidCharacter { index: usize, character: char },
}

#[cfg(test)]
mod tests {
    use super::{RegionId, RegionIdError};

    #[test]
    fn parse_standard_and_nonstandard_region_ids() {
        let standard: RegionId =
            "eu-west-2".parse().expect("standard region should parse");
        let nonstandard: RegionId = "pluto-central-2a"
            .parse()
            .expect("nonstandard region should parse");

        assert_eq!(standard.as_str(), "eu-west-2");
        assert_eq!(standard.as_ref(), "eu-west-2");
        assert_eq!(nonstandard.to_string(), "pluto-central-2a");
    }

    #[test]
    fn region_id_supports_string_conversions() {
        let region_id = RegionId::try_from("eu-west-2".to_owned())
            .expect("region id should convert from String");

        assert_eq!(region_id.as_ref(), "eu-west-2");
        assert_eq!(String::from(region_id), "eu-west-2");
    }

    #[test]
    fn reject_empty_region_ids() {
        let error =
            "".parse::<RegionId>().expect_err("empty region must fail");

        assert_eq!(error, RegionIdError::Empty);
        assert_eq!(error.to_string(), "region id cannot be empty");
    }

    #[test]
    fn reject_region_ids_without_separators() {
        let error = "useast1"
            .parse::<RegionId>()
            .expect_err("region without separators must fail");

        assert_eq!(error, RegionIdError::MissingSeparator);
        assert_eq!(
            error.to_string(),
            "region id must contain at least one '-' separator"
        );
    }

    #[test]
    fn reject_region_ids_with_invalid_separators() {
        let error = "us--east-1"
            .parse::<RegionId>()
            .expect_err("region with repeated separators must fail");

        assert_eq!(error, RegionIdError::InvalidSeparator { index: 3 });
        assert_eq!(
            error.to_string(),
            "region id contains an invalid '-' separator at index 3"
        );
    }

    #[test]
    fn reject_region_ids_with_invalid_characters() {
        let error = "us East-1"
            .parse::<RegionId>()
            .expect_err("region with spaces must fail");

        assert_eq!(
            error,
            RegionIdError::InvalidCharacter { index: 2, character: ' ' }
        );
        assert_eq!(
            error.to_string(),
            "region id must contain only lowercase ASCII letters, digits, and '-', found ' ' at index 2"
        );
    }
}
