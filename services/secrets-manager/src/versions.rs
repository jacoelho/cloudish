use crate::{
    CURRENT_STAGE, PREVIOUS_STAGE, errors::SecretsManagerError,
    lifecycle::StoredSecret, secrets::SecretName,
};
use aws::{Arn, KmsKeyReference};
use serde::{Deserialize, Serialize, Serializer};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PutSecretValueOutput {
    pub arn: Arn,
    pub name: SecretName,
    pub version_id: String,
    pub version_stages: Vec<String>,
}

impl Serialize for PutSecretValueOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        #[derive(Serialize)]
        #[serde(rename_all = "PascalCase")]
        struct Output<'a> {
            #[serde(rename = "ARN")]
            arn: &'a Arn,
            name: &'a SecretName,
            version_id: &'a str,
            version_stages: &'a [String],
        }

        Output {
            arn: &self.arn,
            name: &self.name,
            version_id: &self.version_id,
            version_stages: &self.version_stages,
        }
        .serialize(serializer)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct SecretVersionListEntry {
    pub created_date: u64,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub kms_key_ids: Vec<KmsKeyReference>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_accessed_date: Option<u64>,
    pub version_id: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub version_stages: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct ListSecretVersionIdsOutput {
    #[serde(rename = "ARN")]
    pub arn: Arn,
    pub name: SecretName,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_token: Option<String>,
    pub versions: Vec<SecretVersionListEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct StoredSecretVersion {
    pub(crate) created_date: u64,
    pub(crate) kms_key_id: Option<KmsKeyReference>,
    pub(crate) last_accessed_date: Option<u64>,
    pub(crate) secret_binary: Option<Vec<u8>>,
    pub(crate) secret_string: Option<String>,
    pub(crate) version_stages: BTreeSet<String>,
}

pub(crate) fn resolve_new_version_id(
    secret: &StoredSecret,
    client_request_token: Option<&str>,
) -> Result<String, SecretsManagerError> {
    if let Some(client_request_token) = client_request_token {
        if !(32..=64).contains(&client_request_token.len()) {
            return Err(SecretsManagerError::invalid_parameter(
                "ClientRequestToken must be between 32 and 64 characters long."
                    .to_owned(),
            ));
        }
        if secret.versions.contains_key(client_request_token) {
            return Err(SecretsManagerError::invalid_request(
                "You can't modify an existing version, you can only create a \
                 new version."
                    .to_owned(),
            ));
        }
        return Ok(client_request_token.to_owned());
    }

    Ok(format!(
        "{:x}",
        md5::compute(format!("{}:{}", secret.arn, secret.next_version_number))
    ))
}

pub(crate) fn normalize_requested_stages(
    version_stages: Option<Vec<String>>,
) -> Result<BTreeSet<String>, SecretsManagerError> {
    let version_stages =
        version_stages.unwrap_or_else(|| vec![CURRENT_STAGE.to_owned()]);
    if version_stages.is_empty() {
        return Err(SecretsManagerError::invalid_parameter(
            "VersionStages must contain at least one staging label."
                .to_owned(),
        ));
    }
    let mut normalized = BTreeSet::new();
    for stage in version_stages {
        if stage.trim().is_empty() {
            return Err(SecretsManagerError::invalid_parameter(
                "Version stage labels must not be blank.".to_owned(),
            ));
        }
        normalized.insert(stage);
    }

    Ok(normalized)
}

pub(crate) fn apply_new_version(
    secret: &mut StoredSecret,
    version_id: String,
    secret_string: Option<String>,
    secret_binary: Option<Vec<u8>>,
    version_stages: BTreeSet<String>,
    created_date: u64,
) {
    let previous_current = if version_stages.contains(CURRENT_STAGE) {
        detach_stage(secret, CURRENT_STAGE)
    } else {
        None
    };
    for stage in &version_stages {
        if stage != CURRENT_STAGE {
            detach_stage(secret, stage);
        }
    }
    if version_stages.contains(CURRENT_STAGE)
        && !version_stages.contains(PREVIOUS_STAGE)
    {
        detach_stage(secret, PREVIOUS_STAGE);
        if let Some(previous_current) = previous_current
            && let Some(version) = secret.versions.get_mut(&previous_current)
        {
            version.version_stages.insert(PREVIOUS_STAGE.to_owned());
        }
    }

    secret.versions.insert(
        version_id,
        StoredSecretVersion {
            created_date,
            kms_key_id: secret.kms_key_id.clone(),
            last_accessed_date: None,
            secret_binary,
            secret_string,
            version_stages,
        },
    );
    secret.next_version_number += 1;
}

pub(crate) fn detach_stage(
    secret: &mut StoredSecret,
    stage: &str,
) -> Option<String> {
    let mut detached_from = None;
    for (version_id, version) in &mut secret.versions {
        if version.version_stages.remove(stage) {
            detached_from = Some(version_id.clone());
        }
    }

    detached_from
}

pub(crate) fn current_version_id(secret: &StoredSecret) -> Option<String> {
    find_version_id_for_stage(secret, CURRENT_STAGE)
}

pub(crate) fn find_version_id_for_stage(
    secret: &StoredSecret,
    stage: &str,
) -> Option<String> {
    secret.versions.iter().find_map(|(version_id, version)| {
        version.version_stages.contains(stage).then_some(version_id.clone())
    })
}

pub(crate) fn version_stages(version: &StoredSecretVersion) -> Vec<String> {
    version.version_stages.iter().cloned().collect()
}

pub(crate) fn secret_versions_to_stages(
    secret: &StoredSecret,
) -> BTreeMap<String, Vec<String>> {
    secret
        .versions
        .iter()
        .filter(|(_, version)| !version.version_stages.is_empty())
        .map(|(version_id, version)| {
            (version_id.clone(), version_stages(version))
        })
        .collect()
}
