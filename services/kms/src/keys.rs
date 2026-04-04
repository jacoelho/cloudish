use crate::{
    MAX_TAG_KEY_BYTES, MAX_TAG_VALUE_BYTES, MAX_TAGS,
    crypto::{hash_bytes, hex_encode},
    errors::KmsError,
    scope::KmsScope,
};
use aws::{AccountId, Arn, KmsKeyId, KmsKeyReference, RegionId};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct KmsTag {
    pub tag_key: String,
    pub tag_value: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateKmsKeyInput {
    pub description: Option<String>,
    pub key_spec: Option<String>,
    pub key_usage: Option<String>,
    pub tags: Vec<KmsTag>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct CreateKmsKeyOutput {
    pub key_metadata: KmsKeyMetadata,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct DescribeKmsKeyOutput {
    pub key_metadata: KmsKeyMetadata,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct ListKmsKeysOutput {
    pub keys: Vec<KmsKeyListEntry>,
    pub truncated: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct KmsKeyListEntry {
    pub key_id: KmsKeyId,
    #[serde(rename = "KeyArn")]
    pub key_arn: Arn,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScheduleKmsKeyDeletionInput {
    pub key_id: KmsKeyReference,
    pub pending_window_in_days: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct ScheduleKmsKeyDeletionOutput {
    pub deletion_date: u64,
    pub key_id: Arn,
    pub key_state: String,
    pub pending_window_in_days: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CancelKmsKeyDeletionInput {
    pub key_id: KmsKeyReference,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct CancelKmsKeyDeletionOutput {
    pub key_id: Arn,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KmsTagResourceInput {
    pub key_id: KmsKeyReference,
    pub tags: Vec<KmsTag>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KmsUntagResourceInput {
    pub key_id: KmsKeyReference,
    pub tag_keys: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KmsListResourceTagsInput {
    pub key_id: KmsKeyReference,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct KmsListResourceTagsOutput {
    pub tags: Vec<KmsTag>,
    pub truncated: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct KmsKeyMetadata {
    #[serde(rename = "AWSAccountId")]
    pub aws_account_id: AccountId,
    #[serde(rename = "Arn")]
    pub arn: Arn,
    pub creation_date: u64,
    pub current_key_material_id: String,
    pub customer_master_key_spec: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deletion_date: Option<u64>,
    pub description: String,
    pub enabled: bool,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub encryption_algorithms: Vec<String>,
    pub key_id: KmsKeyId,
    pub key_manager: String,
    pub key_spec: String,
    pub key_state: String,
    pub key_usage: String,
    pub multi_region: bool,
    pub origin: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pending_deletion_window_in_days: Option<u32>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub signing_algorithms: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum KmsKeySpec {
    SymmetricDefault,
    Rsa2048,
}

impl KmsKeySpec {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::SymmetricDefault => "SYMMETRIC_DEFAULT",
            Self::Rsa2048 => "RSA_2048",
        }
    }

    pub(crate) fn parse(value: &str) -> Result<Self, KmsError> {
        match value {
            "SYMMETRIC_DEFAULT" => Ok(Self::SymmetricDefault),
            "RSA_2048" => Ok(Self::Rsa2048),
            _ => Err(KmsError::ValidationException {
                message: format!("Key spec {value} is not supported."),
            }),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum KmsKeyUsage {
    EncryptDecrypt,
    SignVerify,
}

impl KmsKeyUsage {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::EncryptDecrypt => "ENCRYPT_DECRYPT",
            Self::SignVerify => "SIGN_VERIFY",
        }
    }

    pub(crate) fn parse(value: &str) -> Result<Self, KmsError> {
        match value {
            "ENCRYPT_DECRYPT" => Ok(Self::EncryptDecrypt),
            "SIGN_VERIFY" => Ok(Self::SignVerify),
            _ => Err(KmsError::ValidationException {
                message: format!("Key usage {value} is not supported."),
            }),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum KmsKeyState {
    Enabled,
    PendingDeletion,
}

impl KmsKeyState {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Enabled => "Enabled",
            Self::PendingDeletion => "PendingDeletion",
        }
    }

    pub(crate) fn enabled(self) -> bool {
        self == Self::Enabled
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
pub(crate) struct KmsKeyStorageKey {
    pub(crate) account_id: AccountId,
    pub(crate) key_id: String,
    pub(crate) region: RegionId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct StoredKmsKey {
    pub(crate) account_id: AccountId,
    pub(crate) arn: Arn,
    pub(crate) creation_date: u64,
    pub(crate) current_key_material_id: String,
    pub(crate) deletion_date: Option<u64>,
    pub(crate) description: String,
    pub(crate) key_id: KmsKeyId,
    pub(crate) key_spec: KmsKeySpec,
    pub(crate) key_state: KmsKeyState,
    pub(crate) key_usage: KmsKeyUsage,
    pub(crate) pending_deletion_window_in_days: Option<u32>,
    pub(crate) region: RegionId,
    pub(crate) tags: BTreeMap<String, String>,
}

impl StoredKmsKey {
    pub(crate) fn metadata(&self) -> KmsKeyMetadata {
        KmsKeyMetadata {
            aws_account_id: self.account_id.clone(),
            arn: self.arn.clone(),
            creation_date: self.creation_date,
            current_key_material_id: self.current_key_material_id.clone(),
            customer_master_key_spec: self.key_spec.as_str().to_owned(),
            deletion_date: self.deletion_date,
            description: self.description.clone(),
            enabled: self.key_state.enabled(),
            encryption_algorithms: if self.key_usage
                == KmsKeyUsage::EncryptDecrypt
            {
                vec!["SYMMETRIC_DEFAULT".to_owned()]
            } else {
                Vec::new()
            },
            key_id: self.key_id.clone(),
            key_manager: "CUSTOMER".to_owned(),
            key_spec: self.key_spec.as_str().to_owned(),
            key_state: self.key_state.as_str().to_owned(),
            key_usage: self.key_usage.as_str().to_owned(),
            multi_region: false,
            origin: "AWS_KMS".to_owned(),
            pending_deletion_window_in_days: self
                .pending_deletion_window_in_days,
            signing_algorithms: if self.key_usage == KmsKeyUsage::SignVerify {
                vec!["RSASSA_PSS_SHA_256".to_owned()]
            } else {
                Vec::new()
            },
        }
    }
}

pub(crate) fn validate_tags(
    tags: &[KmsTag],
) -> Result<BTreeMap<String, String>, KmsError> {
    if tags.len() > MAX_TAGS {
        return Err(KmsError::TagException {
            message: "Too many tags".to_owned(),
        });
    }

    let mut seen = BTreeSet::new();
    let mut validated = BTreeMap::new();

    for (index, tag) in tags.iter().enumerate() {
        if !seen.insert(tag.tag_key.clone()) {
            return Err(KmsError::TagException {
                message: "Duplicate tag keys".to_owned(),
            });
        }
        if tag.tag_key.to_ascii_lowercase().starts_with("aws:") {
            return Err(KmsError::TagException {
                message: "Tags beginning with aws: are reserved".to_owned(),
            });
        }
        if tag.tag_key.len() > MAX_TAG_KEY_BYTES {
            return Err(KmsError::ValidationException {
                message: format!(
                    "1 validation error detected: Value '{}' at \
                     'tags.{}.member.tagKey' failed to satisfy constraint: \
                     Member must have length less than or equal to {}",
                    tag.tag_key,
                    index.saturating_add(1),
                    MAX_TAG_KEY_BYTES,
                ),
            });
        }
        if tag.tag_value.len() > MAX_TAG_VALUE_BYTES {
            return Err(KmsError::ValidationException {
                message: format!(
                    "1 validation error detected: Value '{}' at \
                     'tags.{}.member.tagValue' failed to satisfy constraint: \
                     Member must have length less than or equal to {}",
                    tag.tag_value,
                    index.saturating_add(1),
                    MAX_TAG_VALUE_BYTES,
                ),
            });
        }

        validated.insert(tag.tag_key.clone(), tag.tag_value.clone());
    }

    Ok(validated)
}

pub(crate) fn storage_key(scope: &KmsScope, key_id: &str) -> KmsKeyStorageKey {
    KmsKeyStorageKey {
        account_id: scope.account_id().clone(),
        key_id: key_id.to_owned(),
        region: scope.region().clone(),
    }
}

pub(crate) fn scope_filter(
    scope: &KmsScope,
) -> impl Fn(&KmsKeyStorageKey) -> bool + Send + Sync + 'static {
    let account_id = scope.account_id().clone();
    let region = scope.region().clone();

    move |key| key.account_id == account_id && key.region == region
}

pub(crate) fn format_key_id(scope: &KmsScope, sequence: usize) -> String {
    let digest = hash_bytes(&[
        scope.account_id().as_str(),
        scope.region().as_str(),
        &sequence.to_string(),
    ]);
    let hex = hex_encode(&digest[..16]);

    format!(
        "{}-{}-{}-{}-{}",
        &hex[..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..32],
    )
}

pub(crate) fn current_key_material_id(
    scope: &KmsScope,
    key_id: &KmsKeyId,
    key_spec: KmsKeySpec,
    key_usage: KmsKeyUsage,
) -> String {
    hex_encode(&hash_bytes(&[
        scope.account_id().as_str(),
        scope.region().as_str(),
        key_id.as_str(),
        key_spec.as_str(),
        key_usage.as_str(),
    ]))
}
