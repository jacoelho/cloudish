use crate::{
    DEFAULT_MAX_RESULTS, MAX_RECOVERY_WINDOW_DAYS,
    MAX_SECRET_DESCRIPTION_LENGTH, MAX_SECRET_VALUE_BYTES,
    MIN_RECOVERY_WINDOW_DAYS,
    errors::SecretsManagerError,
    scope::SecretsManagerScope,
    secrets::{
        DescribeSecretOutput, ListSecretsFilter, ListSecretsFilterKey,
        ListSecretsSortBy, ListSecretsSortOrder, SecretName,
        SecretsManagerExternalMetadataItem, SecretsManagerRotationRules,
        SecretsManagerSecretListEntry, SecretsManagerTag,
    },
    versions::{StoredSecretVersion, secret_versions_to_stages},
};
use aws::{
    Arn, ArnResource, Clock, KmsKeyReference, Partition, RegionId, ServiceName,
};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use serde::{Deserialize, Serialize, Serializer};
use std::collections::BTreeMap;
use std::time::UNIX_EPOCH;

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
pub(crate) struct SecretsManagerStorageKey {
    pub(crate) account_id: aws::AccountId,
    pub(crate) name: String,
    pub(crate) region: RegionId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct StoredSecret {
    pub(crate) arn: Arn,
    pub(crate) created_date: u64,
    pub(crate) deleted_date: Option<u64>,
    pub(crate) description: Option<String>,
    pub(crate) external_secret_rotation_metadata:
        Vec<SecretsManagerExternalMetadataItem>,
    pub(crate) external_secret_rotation_role_arn: Option<String>,
    pub(crate) kms_key_id: Option<KmsKeyReference>,
    pub(crate) last_accessed_date: Option<u64>,
    pub(crate) last_changed_date: u64,
    pub(crate) last_rotated_date: Option<u64>,
    pub(crate) name: SecretName,
    pub(crate) next_rotation_date: Option<u64>,
    pub(crate) next_version_number: u64,
    pub(crate) owning_service: Option<String>,
    pub(crate) primary_region: RegionId,
    pub(crate) rotation_enabled: Option<bool>,
    pub(crate) rotation_lambda_arn: Option<Arn>,
    pub(crate) rotation_rules: Option<SecretsManagerRotationRules>,
    pub(crate) secret_type: Option<String>,
    pub(crate) tags: Vec<SecretsManagerTag>,
    pub(crate) versions: BTreeMap<String, StoredSecretVersion>,
}

pub(crate) fn storage_key(
    scope: &SecretsManagerScope,
    name: &str,
) -> SecretsManagerStorageKey {
    SecretsManagerStorageKey {
        account_id: scope.account_id().clone(),
        name: name.to_owned(),
        region: scope.region().clone(),
    }
}

pub(crate) fn scope_filter(
    scope: &SecretsManagerScope,
) -> impl Fn(&SecretsManagerStorageKey) -> bool + Send + Sync + 'static {
    let account_id = scope.account_id().clone();
    let region = scope.region().clone();

    move |key| key.account_id == account_id && key.region == region
}

pub(crate) fn current_epoch_seconds(
    clock: &dyn Clock,
) -> Result<u64, SecretsManagerError> {
    clock
        .now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .map_err(|_| {
            SecretsManagerError::internal(
                "Secrets Manager clock produced a timestamp before the Unix \
                 epoch.",
            )
        })
}

pub(crate) fn build_secret_arn(
    scope: &SecretsManagerScope,
    name: &SecretName,
) -> Arn {
    let suffix = format!(
        "{:x}",
        md5::compute(format!(
            "{}:{}:{}",
            scope.account_id().as_str(),
            scope.region().as_str(),
            name.as_str(),
        ))
    );
    Arn::trusted_new(
        Partition::aws(),
        ServiceName::SecretsManager,
        Some(scope.region().clone()),
        Some(scope.account_id().clone()),
        ArnResource::Generic(format!(
            "secret:{}-{}",
            name.as_str(),
            &suffix[..6]
        )),
    )
}

pub(crate) fn validate_description(
    description: Option<&str>,
) -> Result<(), SecretsManagerError> {
    if description.is_some_and(|description| {
        description.len() > MAX_SECRET_DESCRIPTION_LENGTH
    }) {
        return Err(SecretsManagerError::invalid_parameter(
            "Description exceeds the maximum supported length.".to_owned(),
        ));
    }

    Ok(())
}

pub(crate) fn validate_secret_value(
    secret_string: Option<&str>,
    secret_binary: Option<&[u8]>,
    allow_empty: bool,
) -> Result<(), SecretsManagerError> {
    if secret_string.is_some() && secret_binary.is_some() {
        return Err(SecretsManagerError::invalid_parameter(
            "You must provide either SecretString or SecretBinary, but not \
             both."
                .to_owned(),
        ));
    }
    if !allow_empty && secret_string.is_none() && secret_binary.is_none() {
        return Err(SecretsManagerError::invalid_parameter(
            "You must provide either SecretString or SecretBinary.".to_owned(),
        ));
    }
    if secret_string.is_some_and(|value| value.len() > MAX_SECRET_VALUE_BYTES)
    {
        return Err(SecretsManagerError::invalid_parameter(
            "SecretString exceeds the maximum supported length.".to_owned(),
        ));
    }
    if secret_binary.is_some_and(|value| value.len() > MAX_SECRET_VALUE_BYTES)
    {
        return Err(SecretsManagerError::invalid_parameter(
            "SecretBinary exceeds the maximum supported length.".to_owned(),
        ));
    }

    Ok(())
}

pub(crate) fn secret_not_found_error() -> SecretsManagerError {
    SecretsManagerError::resource_not_found(
        "Secrets Manager can't find the specified secret.".to_owned(),
    )
}

pub(crate) fn deduplicate_tags(
    tags: Vec<SecretsManagerTag>,
) -> Vec<SecretsManagerTag> {
    merge_tags(Vec::new(), tags)
}

pub(crate) fn merge_tags(
    existing: Vec<SecretsManagerTag>,
    new_tags: Vec<SecretsManagerTag>,
) -> Vec<SecretsManagerTag> {
    let mut merged = existing
        .into_iter()
        .map(|tag| (tag.key, tag.value))
        .collect::<BTreeMap<_, _>>();
    for tag in new_tags {
        merged.insert(tag.key, tag.value);
    }

    merged
        .into_iter()
        .map(|(key, value)| SecretsManagerTag { key, value })
        .collect()
}

pub(crate) fn ensure_secret_not_deleted(
    secret: &StoredSecret,
) -> Result<(), SecretsManagerError> {
    if secret.deleted_date.is_some() {
        return Err(SecretsManagerError::invalid_request(
            "You can't perform this operation on the secret because it was \
             marked for deletion."
                .to_owned(),
        ));
    }

    Ok(())
}

pub(crate) fn describe_output(secret: &StoredSecret) -> DescribeSecretOutput {
    DescribeSecretOutput {
        arn: secret.arn.clone(),
        created_date: secret.created_date,
        deleted_date: secret.deleted_date,
        description: secret.description.clone(),
        external_secret_rotation_metadata: secret
            .external_secret_rotation_metadata
            .clone(),
        external_secret_rotation_role_arn: secret
            .external_secret_rotation_role_arn
            .clone(),
        kms_key_id: secret.kms_key_id.clone(),
        last_accessed_date: secret.last_accessed_date,
        last_changed_date: secret.last_changed_date,
        last_rotated_date: secret.last_rotated_date,
        name: secret.name.clone(),
        next_rotation_date: secret.next_rotation_date,
        owning_service: secret.owning_service.clone(),
        primary_region: secret.primary_region.clone(),
        rotation_enabled: secret.rotation_enabled,
        rotation_lambda_arn: secret.rotation_lambda_arn.clone(),
        rotation_rules: secret.rotation_rules.clone(),
        tags: secret.tags.clone(),
        r#type: secret.secret_type.clone(),
        version_ids_to_stages: secret_versions_to_stages(secret),
    }
}

pub(crate) fn list_entry_output(
    secret: StoredSecret,
) -> SecretsManagerSecretListEntry {
    let secret_versions_to_stages = secret_versions_to_stages(&secret);

    SecretsManagerSecretListEntry {
        arn: secret.arn,
        created_date: secret.created_date,
        deleted_date: secret.deleted_date,
        description: secret.description,
        external_secret_rotation_metadata: secret
            .external_secret_rotation_metadata,
        external_secret_rotation_role_arn: secret
            .external_secret_rotation_role_arn,
        kms_key_id: secret.kms_key_id,
        last_accessed_date: secret.last_accessed_date,
        last_changed_date: secret.last_changed_date,
        last_rotated_date: secret.last_rotated_date,
        name: secret.name,
        next_rotation_date: secret.next_rotation_date,
        owning_service: secret.owning_service,
        primary_region: secret.primary_region,
        rotation_enabled: secret.rotation_enabled,
        rotation_lambda_arn: secret.rotation_lambda_arn,
        rotation_rules: secret.rotation_rules,
        secret_versions_to_stages,
        tags: secret.tags,
        r#type: secret.secret_type,
    }
}

pub(crate) fn validate_delete_input(
    recovery_window_in_days: Option<u32>,
    force_delete_without_recovery: bool,
) -> Result<(), SecretsManagerError> {
    if force_delete_without_recovery && recovery_window_in_days.is_some() {
        return Err(SecretsManagerError::invalid_parameter(
            "You can't specify RecoveryWindowInDays and \
             ForceDeleteWithoutRecovery in the same request."
                .to_owned(),
        ));
    }
    if let Some(recovery_window_in_days) = recovery_window_in_days
        && !(MIN_RECOVERY_WINDOW_DAYS..=MAX_RECOVERY_WINDOW_DAYS)
            .contains(&recovery_window_in_days)
    {
        return Err(SecretsManagerError::invalid_parameter(
            "RecoveryWindowInDays must be between 7 and 30.".to_owned(),
        ));
    }

    Ok(())
}

pub(crate) fn validate_rotation_lambda_arn(
    rotation_lambda_arn: &Arn,
) -> Result<(), SecretsManagerError> {
    if rotation_lambda_arn.service() != ServiceName::Lambda {
        return Err(SecretsManagerError::invalid_parameter(
            "RotationLambdaARN must be a valid Lambda function ARN."
                .to_owned(),
        ));
    }

    Ok(())
}

pub(crate) fn next_rotation_date(
    now: u64,
    rotation_rules: &SecretsManagerRotationRules,
) -> Option<u64> {
    rotation_rules
        .automatically_after_days
        .map(|days| {
            now.saturating_add(
                u64::from(days).saturating_mul(24 * 60 * 60),
            )
        })
}

pub(crate) fn matches_filters(
    secret: &StoredSecret,
    filters: &[ListSecretsFilter],
) -> bool {
    filters.iter().all(|filter| {
        let mut positive =
            filter.values.iter().filter(|value| !value.starts_with('!'));
        let mut negative =
            filter.values.iter().filter_map(|value| value.strip_prefix('!'));
        let positive_match = positive.next().is_none()
            || filter.values.iter().any(|value| {
                !value.starts_with('!')
                    && filter_value_matches(secret, &filter.key, value)
            });
        let negative_match = negative
            .any(|value| filter_value_matches(secret, &filter.key, value));

        positive_match && !negative_match
    })
}

pub(crate) fn filter_value_matches(
    secret: &StoredSecret,
    key: &ListSecretsFilterKey,
    value: &str,
) -> bool {
    match key {
        ListSecretsFilterKey::Description => {
            secret.description.as_deref().is_some_and(|candidate| {
                candidate
                    .to_ascii_lowercase()
                    .starts_with(&value.to_ascii_lowercase())
            })
        }
        ListSecretsFilterKey::Name => secret.name.as_str().starts_with(value),
        ListSecretsFilterKey::TagKey => {
            secret.tags.iter().any(|tag| tag.key.starts_with(value))
        }
        ListSecretsFilterKey::TagValue => {
            secret.tags.iter().any(|tag| tag.value.starts_with(value))
        }
        ListSecretsFilterKey::PrimaryRegion => {
            secret.primary_region.as_str().starts_with(value)
        }
        ListSecretsFilterKey::OwningService => secret
            .owning_service
            .as_deref()
            .is_some_and(|candidate| candidate.starts_with(value)),
        ListSecretsFilterKey::All => {
            let words = value
                .split_whitespace()
                .map(|word| word.to_ascii_lowercase())
                .collect::<Vec<_>>();
            let candidates = std::iter::once(secret.name.as_str())
                .chain(secret.description.iter().map(String::as_str))
                .chain(std::iter::once(secret.primary_region.as_str()))
                .chain(secret.owning_service.iter().map(String::as_str))
                .chain(secret.tags.iter().map(|tag| tag.key.as_str()))
                .chain(secret.tags.iter().map(|tag| tag.value.as_str()))
                .map(str::to_ascii_lowercase)
                .collect::<Vec<_>>();

            words.iter().all(|word| {
                candidates.iter().any(|candidate| candidate.contains(word))
            })
        }
    }
}

pub(crate) fn sort_list_entries(
    entries: &mut [SecretsManagerSecretListEntry],
    sort_by: ListSecretsSortBy,
    sort_order: ListSecretsSortOrder,
) {
    entries.sort_by(|left, right| {
        let ordering = match sort_by {
            ListSecretsSortBy::CreatedDate => {
                left.created_date.cmp(&right.created_date)
            }
            ListSecretsSortBy::LastAccessedDate => left
                .last_accessed_date
                .unwrap_or(0)
                .cmp(&right.last_accessed_date.unwrap_or(0)),
            ListSecretsSortBy::LastChangedDate => {
                left.last_changed_date.cmp(&right.last_changed_date)
            }
            ListSecretsSortBy::Name => left.name.cmp(&right.name),
        }
        .then_with(|| left.name.cmp(&right.name));

        match sort_order {
            ListSecretsSortOrder::Asc => ordering,
            ListSecretsSortOrder::Desc => ordering.reverse(),
        }
    });
}

pub(crate) fn paginate<T>(
    items: Vec<T>,
    max_results: Option<u32>,
    next_token: Option<&str>,
) -> Result<(Vec<T>, Option<String>), SecretsManagerError> {
    let start = if let Some(next_token) = next_token {
        next_token.parse::<usize>().map_err(|_| {
            SecretsManagerError::invalid_next_token(
                "The specified NextToken is invalid.".to_owned(),
            )
        })?
    } else {
        0
    };
    if start > items.len() {
        return Err(SecretsManagerError::invalid_next_token(
            "The specified NextToken is invalid.".to_owned(),
        ));
    }

    let max_results =
        usize::try_from(max_results.unwrap_or(DEFAULT_MAX_RESULTS))
            .unwrap_or(DEFAULT_MAX_RESULTS as usize);
    if max_results == 0 {
        return Err(SecretsManagerError::invalid_parameter(
            "MaxResults must be greater than 0.".to_owned(),
        ));
    }

    let total_items = items.len();
    let end = start.saturating_add(max_results).min(total_items);
    let paged =
        items.into_iter().skip(start).take(max_results).collect::<Vec<_>>();
    let next_token = (end < total_items).then(|| end.to_string());

    Ok((paged, next_token))
}

pub(crate) fn serialize_optional_binary<S>(
    value: &Option<Vec<u8>>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match value {
        Some(bytes) => {
            serializer.serialize_str(&BASE64_STANDARD.encode(bytes))
        }
        None => serializer.serialize_none(),
    }
}
