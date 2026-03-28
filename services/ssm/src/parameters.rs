use crate::{errors::SsmError, scope::SsmScope};
use aws::{AccountId, RegionId};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt;

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
pub struct ParameterName(String);

impl ParameterName {
    /// # Errors
    ///
    /// Returns [`SsmError::ValidationException`] when the name is blank or
    /// contains spaces.
    pub fn parse(value: impl AsRef<str>) -> Result<Self, SsmError> {
        let trimmed = value.as_ref().trim();
        if trimmed.is_empty() {
            return Err(SsmError::ValidationException {
                message: "Parameter names must not be blank.".to_owned(),
            });
        }
        if trimmed.contains(' ') {
            return Err(SsmError::ValidationException {
                message:
                    "Parameter names can't contain spaces between characters."
                        .to_owned(),
            });
        }

        Ok(Self(trimmed.to_owned()))
    }

    fn from_validated(value: String) -> Self {
        Self(value)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    fn without_leading_slash(&self) -> &str {
        self.0.trim_start_matches('/')
    }
}

impl AsRef<str> for ParameterName {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl PartialEq<&str> for ParameterName {
    fn eq(&self, other: &&str) -> bool {
        self.as_str() == *other
    }
}

impl PartialEq<String> for ParameterName {
    fn eq(&self, other: &String) -> bool {
        self.as_str() == other
    }
}

impl fmt::Display for ParameterName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParameterPath(ParameterName);

impl ParameterPath {
    /// # Errors
    ///
    /// Returns any validation error produced while parsing the underlying
    /// parameter name.
    pub fn parse(value: impl AsRef<str>) -> Result<Self, SsmError> {
        let path = ParameterName::parse(value)?;
        if path.as_str() == "/" {
            return Ok(Self(path));
        }

        Ok(Self(ParameterName::from_validated(
            path.as_str().trim_end_matches('/').to_owned(),
        )))
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    pub(crate) fn matches(
        &self,
        name: &ParameterName,
        recursive: bool,
    ) -> bool {
        if self.as_str() == "/" {
            let remainder = name.as_str().trim_start_matches('/');
            return recursive || !remainder.contains('/');
        }

        let prefix = format!("{}/", self.as_str());
        if !name.as_str().starts_with(&prefix) {
            return false;
        }

        if recursive {
            return true;
        }

        !name.as_str()[prefix.len()..].contains('/')
    }
}

impl AsRef<str> for ParameterPath {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Display for ParameterPath {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParameterReference {
    name: ParameterName,
    selector: Option<ParameterSelector>,
}

impl ParameterReference {
    /// # Errors
    ///
    /// Returns [`SsmError::ValidationException`] when the selector is blank or
    /// malformed, or when the parameter name is invalid.
    pub fn parse(value: &str) -> Result<Self, SsmError> {
        let value = value.trim();
        if value.starts_with("arn:") {
            return Err(SsmError::ValidationException {
                message: "Parameter ARN reads are not yet supported."
                    .to_owned(),
            });
        }

        if let Some((name, selector)) = value.rsplit_once(':') {
            if selector.is_empty() {
                return Err(SsmError::ValidationException {
                    message: "Parameter selectors must not be blank."
                        .to_owned(),
                });
            }

            let name = ParameterName::parse(name)?;
            if selector.chars().all(|character| character.is_ascii_digit()) {
                let version = selector.parse::<u64>().map_err(|_| {
                    SsmError::ValidationException {
                        message:
                            "Parameter version selectors must be unsigned integers."
                                .to_owned(),
                    }
                })?;
                return Ok(Self {
                    name,
                    selector: Some(ParameterSelector::Version(version)),
                });
            }

            return Ok(Self {
                name,
                selector: Some(ParameterSelector::Label(selector.to_owned())),
            });
        }

        Ok(Self { name: ParameterName::parse(value)?, selector: None })
    }

    pub fn name(&self) -> &ParameterName {
        &self.name
    }

    pub(crate) fn selector(&self) -> Option<&ParameterSelector> {
        self.selector.as_ref()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ParameterSelector {
    Label(String),
    Version(u64),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SsmResourceType {
    Parameter,
}

impl SsmResourceType {
    /// # Errors
    ///
    /// Returns [`SsmError::InvalidResourceType`] when the value is not a
    /// supported SSM resource type.
    pub fn parse(value: &str) -> Result<Self, SsmError> {
        match value {
            "Parameter" => Ok(Self::Parameter),
            _ => Err(SsmError::InvalidResourceType),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Parameter => "Parameter",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SsmParameterType {
    #[serde(rename = "String")]
    String,
    #[serde(rename = "StringList")]
    StringList,
    #[serde(rename = "SecureString")]
    SecureString,
}

impl SsmParameterType {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::String => "String",
            Self::StringList => "StringList",
            Self::SecureString => "SecureString",
        }
    }

    /// # Errors
    ///
    /// Returns [`SsmError::UnsupportedParameterType`] when the value is not a
    /// supported SSM parameter type.
    pub fn parse(value: &str) -> Result<Self, SsmError> {
        match value {
            "String" => Ok(Self::String),
            "StringList" => Ok(Self::StringList),
            "SecureString" => Ok(Self::SecureString),
            _ => Err(SsmError::UnsupportedParameterType),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SsmPutParameterInput {
    pub description: Option<String>,
    pub name: ParameterName,
    pub overwrite: Option<bool>,
    pub parameter_type: Option<SsmParameterType>,
    pub value: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SsmPutParameterOutput {
    pub version: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SsmGetParameterInput {
    pub reference: ParameterReference,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SsmGetParametersInput {
    pub names: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SsmGetParametersOutput {
    pub invalid_parameters: Vec<String>,
    pub parameters: Vec<SsmParameter>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SsmGetParametersByPathInput {
    pub label_filter: Option<Vec<String>>,
    pub max_results: Option<u32>,
    pub next_token: Option<String>,
    pub path: ParameterPath,
    pub recursive: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SsmGetParametersByPathOutput {
    pub parameters: Vec<SsmParameter>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SsmDeleteParameterInput {
    pub name: ParameterName,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SsmDeleteParameterOutput {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SsmDeleteParametersInput {
    pub names: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SsmDeleteParametersOutput {
    pub deleted_parameters: Vec<String>,
    pub invalid_parameters: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SsmGetParameterHistoryInput {
    pub max_results: Option<u32>,
    pub name: ParameterName,
    pub next_token: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SsmGetParameterHistoryOutput {
    pub parameters: Vec<SsmParameterHistoryEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SsmDescribeFilter {
    NameEquals(Vec<ParameterName>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SsmDescribeParametersInput {
    pub filters: Vec<SsmDescribeFilter>,
    pub max_results: Option<u32>,
    pub next_token: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SsmDescribeParametersOutput {
    pub parameters: Vec<SsmParameterMetadata>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SsmLabelParameterVersionInput {
    pub labels: Vec<String>,
    pub name: ParameterName,
    pub parameter_version: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SsmLabelParameterVersionOutput {
    pub invalid_labels: Vec<String>,
    pub parameter_version: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SsmTag {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SsmAddTagsToResourceInput {
    pub resource_id: ParameterName,
    pub resource_type: SsmResourceType,
    pub tags: Vec<SsmTag>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SsmAddTagsToResourceOutput {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SsmListTagsForResourceInput {
    pub resource_id: ParameterName,
    pub resource_type: SsmResourceType,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SsmListTagsForResourceOutput {
    pub tag_list: Vec<SsmTag>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SsmRemoveTagsFromResourceInput {
    pub resource_id: ParameterName,
    pub resource_type: SsmResourceType,
    pub tag_keys: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SsmRemoveTagsFromResourceOutput {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SsmParameter {
    pub arn: String,
    pub data_type: String,
    pub last_modified_date: u64,
    pub name: ParameterName,
    pub selector: Option<String>,
    pub parameter_type: SsmParameterType,
    pub value: String,
    pub version: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SsmParameterHistoryEntry {
    pub data_type: String,
    pub description: Option<String>,
    pub labels: Vec<String>,
    pub last_modified_date: u64,
    pub name: ParameterName,
    pub parameter_type: SsmParameterType,
    pub value: String,
    pub version: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SsmParameterMetadata {
    pub arn: String,
    pub data_type: String,
    pub description: Option<String>,
    pub last_modified_date: u64,
    pub name: ParameterName,
    pub parameter_type: SsmParameterType,
    pub version: u64,
}

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
pub(crate) struct SsmParameterStorageKey {
    pub(crate) account_id: AccountId,
    pub(crate) name: ParameterName,
    pub(crate) region: RegionId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct StoredParameter {
    pub(crate) history: Vec<StoredParameterVersion>,
    pub(crate) name: ParameterName,
    pub(crate) tags: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct StoredParameterVersion {
    pub(crate) data_type: String,
    pub(crate) description: Option<String>,
    pub(crate) labels: Vec<String>,
    pub(crate) last_modified_epoch_millis: u64,
    pub(crate) parameter_type: SsmParameterType,
    pub(crate) value: String,
    pub(crate) version: u64,
}

pub(crate) fn storage_key(
    scope: &SsmScope,
    name: &ParameterName,
) -> SsmParameterStorageKey {
    SsmParameterStorageKey {
        account_id: scope.account_id().clone(),
        name: name.clone(),
        region: scope.region().clone(),
    }
}

pub(crate) fn scope_filter(
    scope: &SsmScope,
) -> impl Fn(&SsmParameterStorageKey) -> bool + Send + Sync + 'static {
    let account_id = scope.account_id().clone();
    let region = scope.region().clone();

    move |key| key.account_id == account_id && key.region == region
}

pub(crate) fn build_parameter(
    scope: &SsmScope,
    name: &ParameterName,
    version: &StoredParameterVersion,
    selector: Option<String>,
) -> SsmParameter {
    SsmParameter {
        arn: parameter_arn(scope, name),
        data_type: version.data_type.clone(),
        last_modified_date: version.last_modified_epoch_millis,
        name: name.clone(),
        selector,
        parameter_type: version.parameter_type,
        value: version.value.clone(),
        version: version.version,
    }
}

pub(crate) fn build_history_entry(
    name: &ParameterName,
    version: &StoredParameterVersion,
) -> SsmParameterHistoryEntry {
    SsmParameterHistoryEntry {
        data_type: version.data_type.clone(),
        description: version.description.clone(),
        labels: version.labels.clone(),
        last_modified_date: version.last_modified_epoch_millis,
        name: name.clone(),
        parameter_type: version.parameter_type,
        value: version.value.clone(),
        version: version.version,
    }
}

pub(crate) fn build_metadata(
    scope: &SsmScope,
    name: &ParameterName,
    version: &StoredParameterVersion,
) -> SsmParameterMetadata {
    SsmParameterMetadata {
        arn: parameter_arn(scope, name),
        data_type: version.data_type.clone(),
        description: version.description.clone(),
        last_modified_date: version.last_modified_epoch_millis,
        name: name.clone(),
        parameter_type: version.parameter_type,
        version: version.version,
    }
}

pub(crate) fn parameter_arn(scope: &SsmScope, name: &ParameterName) -> String {
    format!(
        "arn:aws:ssm:{}:{}:parameter/{}",
        scope.region().as_str(),
        scope.account_id().as_str(),
        name.without_leading_slash(),
    )
}

pub(crate) fn selector_string(
    selector: Option<&ParameterSelector>,
) -> Option<String> {
    match selector {
        Some(ParameterSelector::Label(label)) => Some(format!(":{label}")),
        Some(ParameterSelector::Version(version)) => {
            Some(format!(":{version}"))
        }
        None => None,
    }
}
