use crate::{Arn, ArnError, ArnResource, ServiceName};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;
use std::str::FromStr;
use thiserror::Error;

#[derive(
    Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(try_from = "String", into = "String")]
pub struct KmsKeyId(String);

impl KmsKeyId {
    /// Builds a KMS key id wrapper.
    ///
    /// # Errors
    ///
    /// Returns `KmsKeyIdError` when the value is blank.
    pub fn new(value: impl Into<String>) -> Result<Self, KmsKeyIdError> {
        let value = value.into();
        if value.trim().is_empty() {
            return Err(KmsKeyIdError::Empty);
        }

        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for KmsKeyId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for KmsKeyId {
    type Err = KmsKeyIdError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value.to_owned())
    }
}

impl TryFrom<String> for KmsKeyId {
    type Error = KmsKeyIdError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<KmsKeyId> for String {
    fn from(value: KmsKeyId) -> Self {
        value.0
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(try_from = "String", into = "String")]
pub struct KmsAliasName(String);

impl KmsAliasName {
    /// Builds a KMS alias name wrapper.
    ///
    /// # Errors
    ///
    /// Returns `KmsAliasNameError` when the value does not start with
    /// `alias/`.
    pub fn new(value: impl Into<String>) -> Result<Self, KmsAliasNameError> {
        let value = value.into();
        if !value.starts_with("alias/") {
            return Err(KmsAliasNameError::MissingPrefix);
        }

        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for KmsAliasName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for KmsAliasName {
    type Err = KmsAliasNameError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value.to_owned())
    }
}

impl TryFrom<String> for KmsAliasName {
    type Error = KmsAliasNameError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<KmsAliasName> for String {
    fn from(value: KmsAliasName) -> Self {
        value.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum KmsKeyReference {
    AliasArn(Arn),
    AliasName(KmsAliasName),
    KeyArn(Arn),
    KeyId(KmsKeyId),
}

impl KmsKeyReference {
    pub fn as_alias_name(&self) -> Option<&KmsAliasName> {
        match self {
            Self::AliasName(alias_name) => Some(alias_name),
            _ => None,
        }
    }

    pub fn as_arn(&self) -> Option<&Arn> {
        match self {
            Self::AliasArn(arn) | Self::KeyArn(arn) => Some(arn),
            _ => None,
        }
    }

    pub fn as_key_id(&self) -> Option<&KmsKeyId> {
        match self {
            Self::KeyId(key_id) => Some(key_id),
            _ => None,
        }
    }

    /// Converts a typed ARN into a KMS key reference.
    ///
    /// # Errors
    ///
    /// Returns `KmsKeyReferenceError` when the ARN targets the wrong service,
    /// is missing account or region scope, or has an unsupported resource
    /// shape.
    pub fn from_arn(arn: Arn) -> Result<Self, KmsKeyReferenceError> {
        if arn.service() != ServiceName::Kms {
            return Err(KmsKeyReferenceError::InvalidService);
        }
        if arn.account_id().is_none() || arn.region().is_none() {
            return Err(KmsKeyReferenceError::MissingScope);
        }

        match arn.resource() {
            ArnResource::Generic(resource)
                if resource.starts_with("alias/") =>
            {
                resource.parse::<KmsAliasName>()?;
                Ok(Self::AliasArn(arn))
            }
            ArnResource::Generic(resource) if resource.starts_with("key/") => {
                let key_id = resource
                    .strip_prefix("key/")
                    .ok_or(KmsKeyReferenceError::InvalidResource)?;
                KmsKeyId::new(key_id.to_owned())?;
                Ok(Self::KeyArn(arn))
            }
            _ => Err(KmsKeyReferenceError::InvalidResource),
        }
    }
}

impl fmt::Display for KmsKeyReference {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AliasArn(arn) | Self::KeyArn(arn) => {
                write!(formatter, "{arn}")
            }
            Self::AliasName(alias_name) => write!(formatter, "{alias_name}"),
            Self::KeyId(key_id) => write!(formatter, "{key_id}"),
        }
    }
}

impl FromStr for KmsKeyReference {
    type Err = KmsKeyReferenceError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.trim().is_empty() {
            return Err(KmsKeyReferenceError::Empty);
        }

        if value.starts_with("arn:") {
            return Self::from_arn(value.parse()?);
        }
        if value.starts_with("alias/") {
            return Ok(Self::AliasName(value.parse()?));
        }

        Ok(Self::KeyId(value.parse()?))
    }
}

impl TryFrom<String> for KmsKeyReference {
    type Error = KmsKeyReferenceError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}

impl From<KmsKeyReference> for String {
    fn from(value: KmsKeyReference) -> Self {
        value.to_string()
    }
}

impl From<KmsKeyId> for KmsKeyReference {
    fn from(value: KmsKeyId) -> Self {
        Self::KeyId(value)
    }
}

impl From<KmsAliasName> for KmsKeyReference {
    fn from(value: KmsAliasName) -> Self {
        Self::AliasName(value)
    }
}

impl Serialize for KmsKeyReference {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for KmsKeyReference {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        value.parse().map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum KmsKeyIdError {
    #[error("KMS key identifiers must not be blank.")]
    Empty,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum KmsAliasNameError {
    #[error("KMS alias names must start with the prefix \"alias/\".")]
    MissingPrefix,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum KmsKeyReferenceError {
    #[error("KMS key references must not be blank.")]
    Empty,
    #[error(transparent)]
    InvalidAliasName(#[from] KmsAliasNameError),
    #[error(transparent)]
    InvalidArn(#[from] ArnError),
    #[error(transparent)]
    InvalidKeyId(#[from] KmsKeyIdError),
    #[error("KMS key ARN references must target the KMS service.")]
    InvalidService,
    #[error("KMS key ARN references must include an AWS account and region.")]
    MissingScope,
    #[error("KMS key ARN references must target a key or alias resource.")]
    InvalidResource,
}

#[cfg(test)]
mod tests {
    use super::{KmsAliasName, KmsKeyId, KmsKeyReference};

    #[test]
    fn key_id_rejects_blank_values() {
        assert!(matches!(
            KmsKeyId::new("   "),
            Err(super::KmsKeyIdError::Empty)
        ));
    }

    #[test]
    fn alias_name_requires_alias_prefix() {
        assert!(matches!(
            KmsAliasName::new("demo"),
            Err(super::KmsAliasNameError::MissingPrefix)
        ));
    }

    #[test]
    fn key_reference_parses_alias_arns() {
        let parsed = "arn:aws:kms:eu-west-2:123456789012:alias/demo"
            .parse::<KmsKeyReference>()
            .expect("alias arn should parse");

        assert!(matches!(parsed, KmsKeyReference::AliasArn(_)));
    }

    #[test]
    fn key_reference_parses_key_arns() {
        let parsed = "arn:aws:kms:eu-west-2:123456789012:key/demo-key-id"
            .parse::<KmsKeyReference>()
            .expect("key arn should parse");

        assert!(matches!(parsed, KmsKeyReference::KeyArn(_)));
    }

    #[test]
    fn key_reference_rejects_non_kms_arns() {
        let error = "arn:aws:sns:eu-west-2:123456789012:demo"
            .parse::<KmsKeyReference>()
            .expect_err("non-kms arns should fail");

        assert!(matches!(error, super::KmsKeyReferenceError::InvalidService));
    }
}
