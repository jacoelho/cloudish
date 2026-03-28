use crate::errors::ElastiCacheError;

use serde::{Deserialize, Serialize};
use std::fmt;

use crate::ElastiCacheReplicationGroupId;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ElastiCacheAuthenticationType {
    Password,
    NoPassword,
    Iam,
}

impl ElastiCacheAuthenticationType {
    pub fn as_input_str(self) -> &'static str {
        match self {
            Self::Password => "password",
            Self::NoPassword => "no-password-required",
            Self::Iam => "iam",
        }
    }

    pub fn as_output_str(self) -> &'static str {
        match self {
            Self::Password => "password",
            Self::NoPassword => "no-password",
            Self::Iam => "iam",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserAuthentication {
    pub password_count: usize,
    pub type_: ElastiCacheAuthenticationType,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthenticationModeInput {
    pub passwords: Vec<String>,
    pub type_: Option<String>,
}

pub trait ElastiCacheIamTokenValidator: Send + Sync + fmt::Debug {
    /// # Errors
    ///
    /// Returns an error string when the provided IAM token is rejected for the
    /// replication group.
    fn validate(
        &self,
        replication_group_id: &ElastiCacheReplicationGroupId,
        token: &str,
    ) -> Result<(), String>;
}

#[derive(Debug, Default, Clone, Copy)]
pub struct RejectAllElastiCacheIamTokenValidator;

impl ElastiCacheIamTokenValidator for RejectAllElastiCacheIamTokenValidator {
    fn validate(
        &self,
        _replication_group_id: &ElastiCacheReplicationGroupId,
        _token: &str,
    ) -> Result<(), String> {
        Err("IAM authentication is not configured.".to_owned())
    }
}

pub(crate) fn resolve_user_authentication(
    authentication_mode: Option<&AuthenticationModeInput>,
    no_password_required: Option<bool>,
    top_level_passwords: Vec<String>,
) -> Result<(ElastiCacheAuthenticationType, Vec<String>), ElastiCacheError> {
    let mode_type = authentication_mode
        .and_then(|mode| mode.type_.as_deref())
        .map(|value| value.trim().to_ascii_lowercase());
    let passwords = if let Some(authentication_mode) = authentication_mode {
        if !authentication_mode.passwords.is_empty() {
            authentication_mode.passwords.clone()
        } else {
            top_level_passwords
        }
    } else {
        top_level_passwords
    };
    let passwords = passwords
        .into_iter()
        .map(|password| password.trim().to_owned())
        .filter(|password| !password.is_empty())
        .collect::<Vec<_>>();
    let no_password_required = no_password_required.unwrap_or(false);

    match mode_type.as_deref() {
        Some("iam") => {
            if no_password_required || !passwords.is_empty() {
                return Err(ElastiCacheError::InvalidParameterCombination {
                    message: "IAM users cannot also set passwords or \
                              NoPasswordRequired."
                        .to_owned(),
                });
            }
            Ok((ElastiCacheAuthenticationType::Iam, Vec::new()))
        }
        Some("password") => {
            if no_password_required || passwords.is_empty() {
                return Err(ElastiCacheError::InvalidParameterCombination {
                    message: "Password users must provide at least one \
                              password and cannot set NoPasswordRequired."
                        .to_owned(),
                });
            }
            Ok((ElastiCacheAuthenticationType::Password, passwords))
        }
        Some("no-password-required") | Some("no-password") => {
            if !passwords.is_empty() {
                return Err(ElastiCacheError::InvalidParameterCombination {
                    message: "No-password users cannot also set passwords."
                        .to_owned(),
                });
            }
            Ok((ElastiCacheAuthenticationType::NoPassword, Vec::new()))
        }
        Some(other) => Err(ElastiCacheError::InvalidParameterValue {
            message: format!(
                "Unsupported AuthenticationMode.Type `{other}`. Supported \
                 values are iam, password, and no-password-required."
            ),
        }),
        None if no_password_required => {
            Ok((ElastiCacheAuthenticationType::NoPassword, Vec::new()))
        }
        None if passwords.is_empty() => {
            Ok((ElastiCacheAuthenticationType::NoPassword, Vec::new()))
        }
        None => Ok((ElastiCacheAuthenticationType::Password, passwords)),
    }
}
