use crate::{AccountId, AccountIdError, RegionId, RegionIdError};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use thiserror::Error;

pub const DEFAULT_ACCOUNT_ENV: &str = "CLOUDISH_DEFAULT_ACCOUNT";
pub const DEFAULT_REGION_ENV: &str = "CLOUDISH_DEFAULT_REGION";
pub const STATE_DIRECTORY_ENV: &str = "CLOUDISH_STATE_DIR";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeDefaults {
    default_account: AccountId,
    default_region: RegionId,
    state_directory: PathBuf,
}

impl RuntimeDefaults {
    /// Builds runtime defaults from the required environment-derived values.
    ///
    /// # Errors
    ///
    /// Returns `RuntimeDefaultsError` when any required field is missing or
    /// blank, or when the default account or region are invalid.
    pub fn try_new(
        default_account: Option<String>,
        default_region: Option<String>,
        state_directory: Option<String>,
    ) -> Result<Self, RuntimeDefaultsError> {
        let mut missing = Vec::new();

        let default_account =
            required_value(DEFAULT_ACCOUNT_ENV, default_account, &mut missing);
        let default_region =
            required_value(DEFAULT_REGION_ENV, default_region, &mut missing);
        let state_directory =
            required_value(STATE_DIRECTORY_ENV, state_directory, &mut missing);

        if !missing.is_empty() {
            return Err(RuntimeDefaultsError::MissingFields {
                fields: missing,
            });
        }

        let (
            Some(default_account),
            Some(default_region),
            Some(state_directory),
        ) = (default_account, default_region, state_directory)
        else {
            return Err(RuntimeDefaultsError::MissingFields {
                fields: missing,
            });
        };

        Ok(Self {
            default_account: default_account.parse().map_err(|source| {
                RuntimeDefaultsError::invalid_account(
                    default_account.clone(),
                    source,
                )
            })?,
            default_region: default_region.parse().map_err(|source| {
                RuntimeDefaultsError::invalid_region(
                    default_region.clone(),
                    source,
                )
            })?,
            state_directory: PathBuf::from(state_directory),
        })
    }

    pub fn default_account(&self) -> &str {
        self.default_account.as_str()
    }

    pub fn default_account_id(&self) -> &AccountId {
        &self.default_account
    }

    pub fn default_region(&self) -> &str {
        self.default_region.as_str()
    }

    pub fn default_region_id(&self) -> &RegionId {
        &self.default_region
    }

    pub fn state_directory(&self) -> &Path {
        &self.state_directory
    }
}

fn required_value(
    name: &'static str,
    value: Option<String>,
    missing: &mut Vec<&'static str>,
) -> Option<String> {
    match value {
        Some(value) if !value.trim().is_empty() => Some(value),
        _ => {
            missing.push(name);
            None
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum RuntimeDefaultsError {
    #[error("missing required config values: {}", fields.join(", "))]
    MissingFields { fields: Vec<&'static str> },
    #[error("invalid default account in CLOUDISH_DEFAULT_ACCOUNT: {source}")]
    InvalidAccount {
        value: String,
        #[source]
        source: AccountIdError,
    },
    #[error("invalid default region in CLOUDISH_DEFAULT_REGION: {source}")]
    InvalidRegion {
        value: String,
        #[source]
        source: RegionIdError,
    },
}

impl RuntimeDefaultsError {
    fn invalid_account(value: String, source: AccountIdError) -> Self {
        Self::InvalidAccount { value, source }
    }

    fn invalid_region(value: String, source: RegionIdError) -> Self {
        Self::InvalidRegion { value, source }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        DEFAULT_ACCOUNT_ENV, DEFAULT_REGION_ENV, RuntimeDefaults,
        RuntimeDefaultsError, STATE_DIRECTORY_ENV,
    };
    use crate::{AccountIdError, RegionIdError};

    #[test]
    fn build_runtime_defaults_from_required_values() {
        let defaults = RuntimeDefaults::try_new(
            Some("000000000000".to_owned()),
            Some("eu-west-2".to_owned()),
            Some("/tmp/cloudish".to_owned()),
        )
        .expect("required values should build defaults");

        assert_eq!(defaults.default_account(), "000000000000");
        assert_eq!(defaults.default_account_id().as_str(), "000000000000");
        assert_eq!(defaults.default_region(), "eu-west-2");
        assert_eq!(defaults.default_region_id().as_str(), "eu-west-2");
        assert_eq!(
            defaults.state_directory().to_string_lossy(),
            "/tmp/cloudish"
        );
    }

    #[test]
    fn reject_missing_or_blank_required_values() {
        let error = RuntimeDefaults::try_new(
            Some(String::new()),
            None,
            Some("   ".to_owned()),
        )
        .expect_err("missing values must fail");

        assert_eq!(
            error.to_string(),
            format!(
                "missing required config values: {DEFAULT_ACCOUNT_ENV}, \
                 {DEFAULT_REGION_ENV}, {STATE_DIRECTORY_ENV}"
            )
        );
    }

    #[test]
    fn reject_invalid_default_account() {
        let error = RuntimeDefaults::try_new(
            Some("account".to_owned()),
            Some("eu-west-2".to_owned()),
            Some("/tmp/cloudish".to_owned()),
        )
        .expect_err("invalid account must fail");

        assert_eq!(
            error,
            RuntimeDefaultsError::invalid_account(
                "account".to_owned(),
                AccountIdError::InvalidLength { actual: 7 },
            )
        );
        assert_eq!(
            error.to_string(),
            "invalid default account in CLOUDISH_DEFAULT_ACCOUNT: account id must be 12 digits, got 7 characters"
        );
    }

    #[test]
    fn reject_invalid_default_region() {
        let error = RuntimeDefaults::try_new(
            Some("000000000000".to_owned()),
            Some("invalid".to_owned()),
            Some("/tmp/cloudish".to_owned()),
        )
        .expect_err("invalid region must fail");

        assert_eq!(
            error,
            RuntimeDefaultsError::invalid_region(
                "invalid".to_owned(),
                RegionIdError::MissingSeparator,
            )
        );
        assert_eq!(
            error.to_string(),
            "invalid default region in CLOUDISH_DEFAULT_REGION: region id must contain at least one '-' separator"
        );
    }
}
