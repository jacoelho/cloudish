use crate::{
    errors::{SecretNameError, SecretReferenceError, SecretsManagerError},
    lifecycle::serialize_optional_binary,
};
use aws::{Arn, KmsKeyReference, RegionId, ServiceName};
use serde::{Deserialize, Serialize};

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(try_from = "String", into = "String")]
pub struct SecretName(String);

impl SecretName {
    /// # Errors
    ///
    /// Returns [`SecretNameError`] when the secret name is empty, too long, or
    /// contains unsupported characters.
    pub fn new(value: impl Into<String>) -> Result<Self, SecretNameError> {
        let value = value.into();
        if value.is_empty() || value.len() > crate::MAX_SECRET_NAME_LENGTH {
            return Err(SecretNameError::Length);
        }
        if value.chars().any(|character| {
            !character.is_ascii_alphanumeric()
                && !matches!(
                    character,
                    '/' | '_' | '+' | '=' | '.' | '@' | '-'
                )
        }) {
            return Err(SecretNameError::InvalidCharacter);
        }

        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for SecretName {
    fn fmt(
        &self,
        formatter: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl std::str::FromStr for SecretName {
    type Err = SecretNameError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value.to_owned())
    }
}

impl TryFrom<String> for SecretName {
    type Error = SecretNameError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<SecretName> for String {
    fn from(value: SecretName) -> Self {
        value.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SecretReference {
    Arn(Arn),
    Name(SecretName),
}

impl SecretReference {
    /// # Errors
    ///
    /// Returns [`SecretReferenceError`] when the ARN is not a scoped Secrets
    /// Manager secret ARN.
    pub fn from_arn(arn: Arn) -> Result<Self, SecretReferenceError> {
        if arn.service() != ServiceName::SecretsManager {
            return Err(SecretReferenceError::InvalidService);
        }
        if arn.account_id().is_none() || arn.region().is_none() {
            return Err(SecretReferenceError::MissingScope);
        }
        match arn.resource() {
            aws::ArnResource::Generic(resource)
                if resource.starts_with("secret:") =>
            {
                Ok(Self::Arn(arn))
            }
            _ => Err(SecretReferenceError::InvalidResource),
        }
    }
}

impl std::fmt::Display for SecretReference {
    fn fmt(
        &self,
        formatter: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        match self {
            Self::Arn(arn) => write!(formatter, "{arn}"),
            Self::Name(name) => write!(formatter, "{name}"),
        }
    }
}

impl std::str::FromStr for SecretReference {
    type Err = SecretReferenceError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.starts_with("arn:") {
            return Self::from_arn(value.parse()?);
        }

        Ok(Self::Name(value.parse()?))
    }
}

impl From<SecretName> for SecretReference {
    fn from(value: SecretName) -> Self {
        Self::Name(value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct SecretsManagerTag {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct SecretsManagerExternalMetadataItem {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct SecretsManagerRotationRules {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub automatically_after_days: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schedule_expression: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateSecretInput {
    pub client_request_token: Option<String>,
    pub description: Option<String>,
    pub kms_key_id: Option<KmsKeyReference>,
    pub name: SecretName,
    pub secret_binary: Option<Vec<u8>>,
    pub secret_string: Option<String>,
    pub secret_type: Option<String>,
    pub tags: Vec<SecretsManagerTag>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetSecretValueInput {
    pub secret_id: SecretReference,
    pub version_id: Option<String>,
    pub version_stage: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PutSecretValueInput {
    pub client_request_token: Option<String>,
    pub secret_binary: Option<Vec<u8>>,
    pub secret_id: SecretReference,
    pub secret_string: Option<String>,
    pub version_stages: Option<Vec<String>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateSecretInput {
    pub client_request_token: Option<String>,
    pub description: Option<String>,
    pub kms_key_id: Option<KmsKeyReference>,
    pub secret_binary: Option<Vec<u8>>,
    pub secret_id: SecretReference,
    pub secret_string: Option<String>,
    pub secret_type: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeSecretInput {
    pub secret_id: SecretReference,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListSecretsInput {
    pub filters: Vec<ListSecretsFilter>,
    pub include_planned_deletion: bool,
    pub max_results: Option<u32>,
    pub next_token: Option<String>,
    pub sort_by: Option<ListSecretsSortBy>,
    pub sort_order: Option<ListSecretsSortOrder>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteSecretInput {
    pub force_delete_without_recovery: bool,
    pub recovery_window_in_days: Option<u32>,
    pub secret_id: SecretReference,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RotateSecretInput {
    pub client_request_token: Option<String>,
    pub external_secret_rotation_metadata:
        Vec<SecretsManagerExternalMetadataItem>,
    pub external_secret_rotation_role_arn: Option<String>,
    pub rotate_immediately: Option<bool>,
    pub rotation_lambda_arn: Option<Arn>,
    pub rotation_rules: Option<SecretsManagerRotationRules>,
    pub secret_id: SecretReference,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TagResourceInput {
    pub secret_id: SecretReference,
    pub tags: Vec<SecretsManagerTag>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UntagResourceInput {
    pub secret_id: SecretReference,
    pub tag_keys: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListSecretVersionIdsInput {
    pub include_deprecated: bool,
    pub max_results: Option<u32>,
    pub next_token: Option<String>,
    pub secret_id: SecretReference,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ListSecretsFilterKey {
    Description,
    Name,
    TagKey,
    TagValue,
    PrimaryRegion,
    OwningService,
    All,
}

impl ListSecretsFilterKey {
    /// # Errors
    ///
    /// Returns [`SecretsManagerError`] when the filter key is unsupported.
    pub fn parse(value: &str) -> Result<Self, SecretsManagerError> {
        match value {
            "description" => Ok(Self::Description),
            "name" => Ok(Self::Name),
            "tag-key" => Ok(Self::TagKey),
            "tag-value" => Ok(Self::TagValue),
            "primary-region" => Ok(Self::PrimaryRegion),
            "owning-service" => Ok(Self::OwningService),
            "all" => Ok(Self::All),
            _ => Err(SecretsManagerError::invalid_parameter(format!(
                "Filter key {value} is not supported."
            ))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListSecretsFilter {
    pub key: ListSecretsFilterKey,
    pub values: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ListSecretsSortBy {
    CreatedDate,
    LastAccessedDate,
    LastChangedDate,
    Name,
}

impl ListSecretsSortBy {
    /// # Errors
    ///
    /// Returns [`SecretsManagerError`] when the sort key is unsupported.
    pub fn parse(value: &str) -> Result<Self, SecretsManagerError> {
        match value {
            "created-date" => Ok(Self::CreatedDate),
            "last-accessed-date" => Ok(Self::LastAccessedDate),
            "last-changed-date" => Ok(Self::LastChangedDate),
            "name" => Ok(Self::Name),
            _ => Err(SecretsManagerError::invalid_parameter(format!(
                "SortBy value {value} is not supported."
            ))),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ListSecretsSortOrder {
    Asc,
    Desc,
}

impl ListSecretsSortOrder {
    /// # Errors
    ///
    /// Returns [`SecretsManagerError`] when the sort order is unsupported.
    pub fn parse(value: &str) -> Result<Self, SecretsManagerError> {
        match value {
            "asc" => Ok(Self::Asc),
            "desc" => Ok(Self::Desc),
            _ => Err(SecretsManagerError::invalid_parameter(format!(
                "SortOrder value {value} is not supported."
            ))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct CreateSecretOutput {
    #[serde(rename = "ARN")]
    pub arn: Arn,
    pub name: SecretName,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct GetSecretValueOutput {
    #[serde(rename = "ARN")]
    pub arn: Arn,
    pub created_date: u64,
    pub name: SecretName,
    #[serde(
        skip_serializing_if = "Option::is_none",
        serialize_with = "serialize_optional_binary"
    )]
    pub secret_binary: Option<Vec<u8>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub secret_string: Option<String>,
    pub version_id: String,
    pub version_stages: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct UpdateSecretOutput {
    #[serde(rename = "ARN")]
    pub arn: Arn,
    pub name: SecretName,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct DescribeSecretOutput {
    #[serde(rename = "ARN")]
    pub arn: Arn,
    pub created_date: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deleted_date: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub external_secret_rotation_metadata:
        Vec<SecretsManagerExternalMetadataItem>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub external_secret_rotation_role_arn: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kms_key_id: Option<KmsKeyReference>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_accessed_date: Option<u64>,
    pub last_changed_date: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_rotated_date: Option<u64>,
    pub name: SecretName,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_rotation_date: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub owning_service: Option<String>,
    pub primary_region: RegionId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rotation_enabled: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "RotationLambdaARN")]
    pub rotation_lambda_arn: Option<Arn>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rotation_rules: Option<SecretsManagerRotationRules>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub tags: Vec<SecretsManagerTag>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub r#type: Option<String>,
    #[serde(skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub version_ids_to_stages: std::collections::BTreeMap<String, Vec<String>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct SecretsManagerSecretListEntry {
    #[serde(rename = "ARN")]
    pub arn: Arn,
    pub created_date: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deleted_date: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub external_secret_rotation_metadata:
        Vec<SecretsManagerExternalMetadataItem>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub external_secret_rotation_role_arn: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kms_key_id: Option<KmsKeyReference>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_accessed_date: Option<u64>,
    pub last_changed_date: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_rotated_date: Option<u64>,
    pub name: SecretName,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_rotation_date: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub owning_service: Option<String>,
    pub primary_region: RegionId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rotation_enabled: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "RotationLambdaARN")]
    pub rotation_lambda_arn: Option<Arn>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rotation_rules: Option<SecretsManagerRotationRules>,
    #[serde(skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub secret_versions_to_stages:
        std::collections::BTreeMap<String, Vec<String>>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub tags: Vec<SecretsManagerTag>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub r#type: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct ListSecretsOutput {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_token: Option<String>,
    pub secret_list: Vec<SecretsManagerSecretListEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct DeleteSecretOutput {
    #[serde(rename = "ARN")]
    pub arn: Arn,
    pub deletion_date: u64,
    pub name: SecretName,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct RotateSecretOutput {
    #[serde(rename = "ARN")]
    pub arn: Arn,
    pub name: SecretName,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TagResourceOutput {}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct UntagResourceOutput {}
