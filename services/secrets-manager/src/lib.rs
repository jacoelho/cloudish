#![cfg_attr(
    test,
    allow(clippy::arithmetic_side_effects, clippy::indexing_slicing)
)]

mod errors;
mod lifecycle;
mod scope;
mod secrets;
mod versions;

use self::lifecycle::{
    SecretsManagerStorageKey, StoredSecret, build_secret_arn,
    current_epoch_seconds, deduplicate_tags, describe_output,
    ensure_secret_not_deleted, list_entry_output, matches_filters, merge_tags,
    next_rotation_date, paginate, scope_filter, secret_not_found_error,
    storage_key, validate_delete_input, validate_description,
    validate_rotation_lambda_arn, validate_secret_value,
};
use self::versions::{
    StoredSecretVersion, apply_new_version, current_version_id,
    find_version_id_for_stage, normalize_requested_stages,
    resolve_new_version_id, version_stages,
};
#[cfg(test)]
use aws::{Arn, KmsKeyReference};
use aws::{Clock, ServiceName};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use storage::{StorageFactory, StorageHandle};

pub use self::errors::{
    SecretNameError, SecretReferenceError, SecretsManagerError,
};
pub use self::scope::SecretsManagerScope;
pub use self::secrets::PutSecretValueInput;
pub use self::secrets::{
    CreateSecretInput, CreateSecretOutput, DeleteSecretInput,
    DeleteSecretOutput, DescribeSecretInput, DescribeSecretOutput,
    GetSecretValueInput, GetSecretValueOutput, ListSecretVersionIdsInput,
    ListSecretsFilter, ListSecretsFilterKey, ListSecretsInput,
    ListSecretsOutput, ListSecretsSortBy, ListSecretsSortOrder,
    RotateSecretInput, RotateSecretOutput, SecretName, SecretReference,
    SecretsManagerExternalMetadataItem, SecretsManagerRotationRules,
    SecretsManagerSecretListEntry, SecretsManagerTag, TagResourceInput,
    TagResourceOutput, UntagResourceInput, UntagResourceOutput,
    UpdateSecretInput, UpdateSecretOutput,
};
pub use self::versions::{
    ListSecretVersionIdsOutput, PutSecretValueOutput, SecretVersionListEntry,
};

pub(crate) const CURRENT_STAGE: &str = "AWSCURRENT";
pub(crate) const PREVIOUS_STAGE: &str = "AWSPREVIOUS";
pub(crate) const DEFAULT_RECOVERY_WINDOW_DAYS: u32 = 30;
pub(crate) const MIN_RECOVERY_WINDOW_DAYS: u32 = 7;
pub(crate) const MAX_RECOVERY_WINDOW_DAYS: u32 = 30;
pub(crate) const DEFAULT_MAX_RESULTS: u32 = 100;
pub(crate) const MAX_SECRET_NAME_LENGTH: usize = 256;
pub(crate) const MAX_SECRET_DESCRIPTION_LENGTH: usize = 2_048;
pub(crate) const MAX_SECRET_VALUE_BYTES: usize = 65_536;

#[derive(Clone)]
pub struct SecretsManagerService {
    clock: Arc<dyn Clock>,
    secret_store: StorageHandle<SecretsManagerStorageKey, StoredSecret>,
}

impl SecretsManagerService {
    pub fn new(factory: &StorageFactory, clock: Arc<dyn Clock>) -> Self {
        Self {
            clock,
            secret_store: factory.create("secretsmanager", "secrets"),
        }
    }

    /// # Errors
    ///
    /// Returns validation, duplicate-name, clock, or persistence errors when
    /// the input is invalid or the new secret cannot be stored.
    pub fn create_secret(
        &self,
        scope: &SecretsManagerScope,
        input: CreateSecretInput,
    ) -> Result<CreateSecretOutput, SecretsManagerError> {
        validate_description(input.description.as_deref())?;
        validate_secret_value(
            input.secret_string.as_deref(),
            input.secret_binary.as_deref(),
            true,
        )?;

        if self
            .secret_store
            .get(&storage_key(scope, input.name.as_str()))
            .is_some()
        {
            return Err(SecretsManagerError::resource_exists(format!(
                "A secret with the name {} already exists.",
                input.name
            )));
        }

        let created_date = current_epoch_seconds(&*self.clock)?;
        let mut secret = StoredSecret {
            arn: build_secret_arn(scope, &input.name),
            created_date,
            deleted_date: None,
            description: input.description,
            external_secret_rotation_metadata: Vec::new(),
            external_secret_rotation_role_arn: None,
            kms_key_id: input.kms_key_id,
            last_accessed_date: None,
            last_changed_date: created_date,
            last_rotated_date: None,
            name: input.name,
            next_rotation_date: None,
            next_version_number: 1,
            owning_service: None,
            primary_region: scope.region().clone(),
            rotation_enabled: None,
            rotation_lambda_arn: None,
            rotation_rules: None,
            secret_type: input.secret_type,
            tags: deduplicate_tags(input.tags),
            versions: BTreeMap::new(),
        };

        let version_id = if input.secret_string.is_some()
            || input.secret_binary.is_some()
        {
            Some(self.create_initial_version(
                &mut secret,
                input.client_request_token.as_deref(),
                input.secret_string,
                input.secret_binary,
                created_date,
            )?)
        } else {
            None
        };

        self.persist_secret(scope, secret.clone())?;

        Ok(CreateSecretOutput {
            arn: secret.arn,
            name: secret.name,
            version_id,
        })
    }

    /// # Errors
    ///
    /// Returns not-found, invalid-request, clock, or persistence errors when
    /// the secret or requested version cannot be resolved.
    pub fn get_secret_value(
        &self,
        scope: &SecretsManagerScope,
        input: GetSecretValueInput,
    ) -> Result<GetSecretValueOutput, SecretsManagerError> {
        let mut secret = self.resolve_secret(scope, &input.secret_id)?;
        ensure_secret_not_deleted(&secret)?;

        let version_id = match (input.version_id, input.version_stage) {
            (Some(version_id), Some(stage)) => {
                let version =
                    secret.versions.get(&version_id).ok_or_else(|| {
                        SecretsManagerError::resource_not_found(
                            "Secrets Manager can't find the specified secret \
                         version."
                                .to_owned(),
                        )
                    })?;
                if !version.version_stages.contains(&stage) {
                    return Err(SecretsManagerError::invalid_request(
                        "You provided a VersionStage and VersionId that do not \
                         refer to the same secret version."
                            .to_owned(),
                    ));
                }
                version_id
            }
            (Some(version_id), None) => {
                if !secret.versions.contains_key(&version_id) {
                    return Err(SecretsManagerError::resource_not_found(
                        "Secrets Manager can't find the specified secret \
                         version."
                            .to_owned(),
                    ));
                }
                version_id
            }
            (None, Some(stage)) => find_version_id_for_stage(&secret, &stage)
                .ok_or_else(|| {
                SecretsManagerError::resource_not_found(format!(
                    "Secrets Manager can't find the specified secret value \
                         for staging label: {stage}"
                ))
            })?,
            (None, None) => find_version_id_for_stage(&secret, CURRENT_STAGE)
                .ok_or_else(|| {
                SecretsManagerError::resource_not_found(
                    "Secrets Manager can't find the specified secret \
                         version."
                        .to_owned(),
                )
            })?,
        };

        let accessed_at = current_epoch_seconds(&*self.clock)?;
        let version =
            secret.versions.get_mut(&version_id).ok_or_else(|| {
                SecretsManagerError::resource_not_found(
                    "Secrets Manager can't find the specified secret version."
                        .to_owned(),
                )
            })?;
        version.last_accessed_date = Some(accessed_at);
        secret.last_accessed_date = Some(accessed_at);
        let output = GetSecretValueOutput {
            arn: secret.arn.clone(),
            created_date: version.created_date,
            name: secret.name.clone(),
            secret_binary: version.secret_binary.clone(),
            secret_string: version.secret_string.clone(),
            version_id: version_id.clone(),
            version_stages: version_stages(version),
        };
        self.persist_secret(scope, secret)?;

        Ok(output)
    }

    /// # Errors
    ///
    /// Returns not-found, deleted-secret, validation, version-conflict, clock,
    /// or persistence errors when the new version cannot be created.
    pub fn put_secret_value(
        &self,
        scope: &SecretsManagerScope,
        input: PutSecretValueInput,
    ) -> Result<PutSecretValueOutput, SecretsManagerError> {
        let mut secret = self.resolve_secret(scope, &input.secret_id)?;
        ensure_secret_not_deleted(&secret)?;
        validate_secret_value(
            input.secret_string.as_deref(),
            input.secret_binary.as_deref(),
            false,
        )?;
        let version_id = resolve_new_version_id(
            &secret,
            input.client_request_token.as_deref(),
        )?;
        let version_stages = normalize_requested_stages(input.version_stages)?;
        let created_date = current_epoch_seconds(&*self.clock)?;

        apply_new_version(
            &mut secret,
            version_id.clone(),
            input.secret_string,
            input.secret_binary,
            version_stages.clone(),
            created_date,
        );
        secret.last_changed_date = created_date;
        self.persist_secret(scope, secret.clone())?;

        Ok(PutSecretValueOutput {
            arn: secret.arn,
            name: secret.name,
            version_id,
            version_stages: version_stages.into_iter().collect(),
        })
    }

    /// # Errors
    ///
    /// Returns not-found, deleted-secret, validation, version-conflict, clock,
    /// or persistence errors when the secret cannot be updated.
    pub fn update_secret(
        &self,
        scope: &SecretsManagerScope,
        input: UpdateSecretInput,
    ) -> Result<UpdateSecretOutput, SecretsManagerError> {
        let mut secret = self.resolve_secret(scope, &input.secret_id)?;
        ensure_secret_not_deleted(&secret)?;
        validate_description(input.description.as_deref())?;
        if input.secret_string.is_some() || input.secret_binary.is_some() {
            validate_secret_value(
                input.secret_string.as_deref(),
                input.secret_binary.as_deref(),
                false,
            )?;
        }

        let updated_at = current_epoch_seconds(&*self.clock)?;
        let has_new_secret_value =
            input.secret_string.is_some() || input.secret_binary.is_some();
        let changed = has_new_secret_value
            || input.description.is_some()
            || input.kms_key_id.is_some()
            || input.secret_type.is_some();
        let version_id = if has_new_secret_value {
            let version_id = resolve_new_version_id(
                &secret,
                input.client_request_token.as_deref(),
            )?;
            apply_new_version(
                &mut secret,
                version_id.clone(),
                input.secret_string,
                input.secret_binary,
                BTreeSet::from([CURRENT_STAGE.to_owned()]),
                updated_at,
            );
            Some(version_id)
        } else {
            None
        };

        if let Some(description) = input.description {
            secret.description = Some(description);
        }
        if let Some(secret_type) = input.secret_type {
            secret.secret_type = Some(secret_type);
        }
        if let Some(kms_key_id) = input.kms_key_id {
            secret.kms_key_id = Some(kms_key_id);
        }
        if changed {
            secret.last_changed_date = updated_at;
        }

        self.persist_secret(scope, secret.clone())?;

        Ok(UpdateSecretOutput {
            arn: secret.arn,
            name: secret.name,
            version_id,
        })
    }

    /// # Errors
    ///
    /// Returns [`SecretsManagerError`] when the referenced secret does not
    /// exist.
    pub fn describe_secret(
        &self,
        scope: &SecretsManagerScope,
        input: DescribeSecretInput,
    ) -> Result<DescribeSecretOutput, SecretsManagerError> {
        let secret = self.resolve_secret(scope, &input.secret_id)?;
        Ok(describe_output(&secret))
    }

    /// # Errors
    ///
    /// Returns pagination errors when the provided `MaxResults` or
    /// `NextToken` values are invalid.
    pub fn list_secrets(
        &self,
        scope: &SecretsManagerScope,
        input: ListSecretsInput,
    ) -> Result<ListSecretsOutput, SecretsManagerError> {
        let secrets = self
            .secret_store
            .scan(&scope_filter(scope))
            .into_iter()
            .filter(|secret| {
                input.include_planned_deletion || secret.deleted_date.is_none()
            })
            .filter(|secret| matches_filters(secret, &input.filters))
            .collect::<Vec<_>>();
        let mut entries =
            secrets.into_iter().map(list_entry_output).collect::<Vec<_>>();
        sort_list_entries(
            &mut entries,
            input.sort_by.unwrap_or(ListSecretsSortBy::Name),
            input.sort_order.unwrap_or(ListSecretsSortOrder::Asc),
        );
        let (secret_list, next_token) =
            paginate(entries, input.max_results, input.next_token.as_deref())?;

        Ok(ListSecretsOutput { next_token, secret_list })
    }

    /// # Errors
    ///
    /// Returns not-found, invalid-request, clock, or persistence errors when
    /// the secret cannot be scheduled for deletion or force-deleted.
    pub fn delete_secret(
        &self,
        scope: &SecretsManagerScope,
        input: DeleteSecretInput,
    ) -> Result<DeleteSecretOutput, SecretsManagerError> {
        let mut secret = self.resolve_secret(scope, &input.secret_id)?;
        validate_delete_input(
            input.recovery_window_in_days,
            input.force_delete_without_recovery,
        )?;
        let deletion_date = current_epoch_seconds(&*self.clock)?;

        if input.force_delete_without_recovery {
            self.secret_store
                .delete(&storage_key(scope, secret.name.as_str()))
                .map_err(SecretsManagerError::from)?;
            return Ok(DeleteSecretOutput {
                arn: secret.arn,
                deletion_date,
                name: secret.name,
            });
        }

        if secret.deleted_date.is_some() {
            return Err(SecretsManagerError::invalid_request(
                "You can't perform this operation on the secret because it was \
                 marked for deletion."
                    .to_owned(),
            ));
        }

        let recovery_window_in_days = input
            .recovery_window_in_days
            .unwrap_or(DEFAULT_RECOVERY_WINDOW_DAYS);
        secret.deleted_date = Some(
            deletion_date.saturating_add(
                u64::from(recovery_window_in_days)
                    .saturating_mul(24 * 60 * 60),
            ),
        );
        let scheduled_deletion_date =
            secret.deleted_date.ok_or_else(|| {
                SecretsManagerError::internal(
                    "Secrets Manager failed to record the scheduled deletion \
                     date.",
                )
            })?;
        self.persist_secret(scope, secret.clone())?;

        Ok(DeleteSecretOutput {
            arn: secret.arn,
            deletion_date: scheduled_deletion_date,
            name: secret.name,
        })
    }

    /// # Errors
    ///
    /// Returns not-found, deleted-secret, invalid-request, clock, or
    /// persistence errors when rotation cannot be configured.
    pub fn rotate_secret(
        &self,
        scope: &SecretsManagerScope,
        input: RotateSecretInput,
    ) -> Result<RotateSecretOutput, SecretsManagerError> {
        let mut secret = self.resolve_secret(scope, &input.secret_id)?;
        ensure_secret_not_deleted(&secret)?;
        if let Some(rotation_lambda_arn) = input.rotation_lambda_arn.as_ref() {
            validate_rotation_lambda_arn(rotation_lambda_arn)?;
        } else if secret.rotation_lambda_arn.is_none()
            && input.external_secret_rotation_metadata.is_empty()
            && input.external_secret_rotation_role_arn.is_none()
        {
            return Err(SecretsManagerError::invalid_request(
                "A rotation Lambda ARN or external rotation metadata is \
                 required to configure rotation."
                    .to_owned(),
            ));
        }

        let rotated_at = current_epoch_seconds(&*self.clock)?;
        if let Some(rotation_lambda_arn) = input.rotation_lambda_arn {
            secret.rotation_lambda_arn = Some(rotation_lambda_arn);
        }
        if let Some(rotation_rules) = input.rotation_rules {
            secret.next_rotation_date =
                next_rotation_date(rotated_at, &rotation_rules);
            secret.rotation_rules = Some(rotation_rules);
        }
        if !input.external_secret_rotation_metadata.is_empty() {
            secret.external_secret_rotation_metadata =
                input.external_secret_rotation_metadata;
        }
        if input.external_secret_rotation_role_arn.is_some() {
            secret.external_secret_rotation_role_arn =
                input.external_secret_rotation_role_arn;
        }
        if input.rotate_immediately.unwrap_or(false) {
            secret.last_rotated_date = None;
        }
        if input.client_request_token.is_some()
            && secret.versions.contains_key(
                input.client_request_token.as_deref().unwrap_or_default(),
            )
        {
            return Err(SecretsManagerError::invalid_request(
                "You can't modify an existing version, you can only create a \
                 new version."
                    .to_owned(),
            ));
        }
        secret.rotation_enabled = Some(true);
        secret.last_changed_date = rotated_at;
        self.persist_secret(scope, secret.clone())?;
        let version_id = current_version_id(&secret);

        Ok(RotateSecretOutput {
            arn: secret.arn,
            name: secret.name,
            version_id,
        })
    }

    /// # Errors
    ///
    /// Returns not-found, deleted-secret, clock, or persistence errors when
    /// the tags cannot be updated.
    pub fn tag_resource(
        &self,
        scope: &SecretsManagerScope,
        input: TagResourceInput,
    ) -> Result<TagResourceOutput, SecretsManagerError> {
        let mut secret = self.resolve_secret(scope, &input.secret_id)?;
        ensure_secret_not_deleted(&secret)?;
        secret.tags = merge_tags(secret.tags, input.tags);
        secret.last_changed_date = current_epoch_seconds(&*self.clock)?;
        self.persist_secret(scope, secret)?;
        Ok(TagResourceOutput {})
    }

    /// # Errors
    ///
    /// Returns not-found, deleted-secret, clock, or persistence errors when
    /// the tags cannot be updated.
    pub fn untag_resource(
        &self,
        scope: &SecretsManagerScope,
        input: UntagResourceInput,
    ) -> Result<UntagResourceOutput, SecretsManagerError> {
        let mut secret = self.resolve_secret(scope, &input.secret_id)?;
        ensure_secret_not_deleted(&secret)?;
        let tag_keys = input.tag_keys.into_iter().collect::<BTreeSet<_>>();
        secret.tags.retain(|tag| !tag_keys.contains(&tag.key));
        secret.last_changed_date = current_epoch_seconds(&*self.clock)?;
        self.persist_secret(scope, secret)?;
        Ok(UntagResourceOutput {})
    }

    /// # Errors
    ///
    /// Returns not-found or pagination errors when the secret does not exist or
    /// the provided paging inputs are invalid.
    pub fn list_secret_version_ids(
        &self,
        scope: &SecretsManagerScope,
        input: ListSecretVersionIdsInput,
    ) -> Result<ListSecretVersionIdsOutput, SecretsManagerError> {
        let secret = self.resolve_secret(scope, &input.secret_id)?;
        let mut versions = secret
            .versions
            .iter()
            .filter(|(_, version)| {
                input.include_deprecated || !version.version_stages.is_empty()
            })
            .map(|(version_id, version)| SecretVersionListEntry {
                created_date: version.created_date,
                kms_key_ids: version
                    .kms_key_id
                    .clone()
                    .map(|kms_key_id| vec![kms_key_id])
                    .unwrap_or_default(),
                last_accessed_date: version.last_accessed_date,
                version_id: version_id.clone(),
                version_stages: version_stages(version),
            })
            .collect::<Vec<_>>();
        versions.sort_by(|left, right| {
            right
                .created_date
                .cmp(&left.created_date)
                .then_with(|| left.version_id.cmp(&right.version_id))
        });
        let (versions, next_token) = paginate(
            versions,
            input.max_results,
            input.next_token.as_deref(),
        )?;

        Ok(ListSecretVersionIdsOutput {
            arn: secret.arn,
            name: secret.name,
            next_token,
            versions,
        })
    }

    fn create_initial_version(
        &self,
        secret: &mut StoredSecret,
        client_request_token: Option<&str>,
        secret_string: Option<String>,
        secret_binary: Option<Vec<u8>>,
        created_date: u64,
    ) -> Result<String, SecretsManagerError> {
        let version_id = resolve_new_version_id(secret, client_request_token)?;
        secret.versions.insert(
            version_id.clone(),
            StoredSecretVersion {
                created_date,
                kms_key_id: secret.kms_key_id.clone(),
                last_accessed_date: None,
                secret_binary,
                secret_string,
                version_stages: BTreeSet::from([CURRENT_STAGE.to_owned()]),
            },
        );
        secret.next_version_number = secret.next_version_number.saturating_add(1);

        Ok(version_id)
    }

    fn resolve_secret(
        &self,
        scope: &SecretsManagerScope,
        secret_id: &SecretReference,
    ) -> Result<StoredSecret, SecretsManagerError> {
        match secret_id {
            SecretReference::Arn(arn) => {
                if arn.service() != ServiceName::SecretsManager
                    || arn.account_id() != Some(scope.account_id())
                    || arn.region() != Some(scope.region())
                {
                    return Err(secret_not_found_error());
                }
                let matching = self
                    .secret_store
                    .scan(&scope_filter(scope))
                    .into_iter()
                    .find(|secret| secret.arn == *arn);

                matching.ok_or_else(secret_not_found_error)
            }
            SecretReference::Name(secret_name) => self
                .secret_store
                .get(&storage_key(scope, secret_name.as_str()))
                .ok_or_else(secret_not_found_error),
        }
    }

    fn persist_secret(
        &self,
        scope: &SecretsManagerScope,
        secret: StoredSecret,
    ) -> Result<(), SecretsManagerError> {
        self.secret_store
            .put(storage_key(scope, secret.name.as_str()), secret)
            .map_err(SecretsManagerError::from)
    }
}

use self::lifecycle::sort_list_entries;

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};
    use storage::{StorageConfig, StorageFactory, StorageMode};

    struct SequenceClock {
        times: Mutex<Vec<SystemTime>>,
    }

    impl SequenceClock {
        fn new(times: Vec<SystemTime>) -> Self {
            Self { times: Mutex::new(times) }
        }
    }

    impl Clock for SequenceClock {
        fn now(&self) -> SystemTime {
            let mut times =
                self.times.lock().unwrap_or_else(|poison| poison.into_inner());
            if times.is_empty() {
                return UNIX_EPOCH;
            }
            if times.len() == 1 {
                return times[0];
            }

            times.remove(0)
        }
    }

    fn service(label: &str, clock: Arc<dyn Clock>) -> SecretsManagerService {
        let factory = StorageFactory::new(StorageConfig::new(
            format!("/tmp/{label}"),
            StorageMode::Memory,
        ));

        SecretsManagerService::new(&factory, clock)
    }

    fn scope(region: &str) -> SecretsManagerScope {
        SecretsManagerScope::new(
            "000000000000".parse().expect("account should parse"),
            region.parse().expect("region should parse"),
        )
    }

    fn seconds(value: u64) -> SystemTime {
        UNIX_EPOCH + Duration::from_secs(value)
    }

    fn arn(value: &str) -> Arn {
        value.parse().expect("ARN should parse")
    }

    fn kms_key_ref(value: &str) -> KmsKeyReference {
        value.parse().expect("KMS key reference should parse")
    }

    fn secret_name(value: &str) -> SecretName {
        value.parse().expect("secret name should parse")
    }

    fn secret_ref(value: &str) -> SecretReference {
        value.parse().expect("secret reference should parse")
    }

    fn secret_ref_from_name(value: &SecretName) -> SecretReference {
        value.clone().into()
    }

    fn create_string(
        service: &SecretsManagerService,
        scope: &SecretsManagerScope,
        name: &str,
        value: &str,
    ) -> CreateSecretOutput {
        service
            .create_secret(
                scope,
                CreateSecretInput {
                    client_request_token: None,
                    description: Some("initial".to_owned()),
                    kms_key_id: None,
                    name: secret_name(name),
                    secret_binary: None,
                    secret_string: Some(value.to_owned()),
                    secret_type: None,
                    tags: vec![SecretsManagerTag {
                        key: "env".to_owned(),
                        value: "dev".to_owned(),
                    }],
                },
            )
            .expect("secret should create")
    }

    #[test]
    fn secrets_manager_lifecycle_stages_tags_and_rotation_are_deterministic() {
        let service = service(
            "services-secrets-manager-lifecycle",
            Arc::new(SequenceClock::new(vec![
                seconds(10),
                seconds(20),
                seconds(30),
                seconds(40),
                seconds(50),
                seconds(60),
                seconds(70),
                seconds(80),
                seconds(90),
                seconds(100),
            ])),
        );
        let scope = scope("eu-west-2");

        let created =
            create_string(&service, &scope, "app/db/password", "alpha");
        let initial = service
            .get_secret_value(
                &scope,
                GetSecretValueInput {
                    secret_id: secret_ref_from_name(&created.name),
                    version_id: None,
                    version_stage: None,
                },
            )
            .expect("current version should load");
        let pending = service
            .put_secret_value(
                &scope,
                PutSecretValueInput {
                    client_request_token: Some(
                        "1234567890abcdef1234567890abcdef".to_owned(),
                    ),
                    secret_binary: None,
                    secret_id: secret_ref_from_name(&created.name),
                    secret_string: Some("beta".to_owned()),
                    version_stages: Some(vec!["AWSPENDING".to_owned()]),
                },
            )
            .expect("pending version should create");
        let pending_value = service
            .get_secret_value(
                &scope,
                GetSecretValueInput {
                    secret_id: secret_ref_from_name(&created.name),
                    version_id: None,
                    version_stage: Some("AWSPENDING".to_owned()),
                },
            )
            .expect("pending version should load");
        let updated = service
            .update_secret(
                &scope,
                UpdateSecretInput {
                    client_request_token: None,
                    description: Some("updated".to_owned()),
                    kms_key_id: Some(kms_key_ref("alias/local")),
                    secret_binary: None,
                    secret_id: secret_ref_from_name(&created.name),
                    secret_string: Some("gamma".to_owned()),
                    secret_type: None,
                },
            )
            .expect("update should succeed");
        let current = service
            .get_secret_value(
                &scope,
                GetSecretValueInput {
                    secret_id: secret_ref_from_name(&created.name),
                    version_id: None,
                    version_stage: Some(CURRENT_STAGE.to_owned()),
                },
            )
            .expect("current version should load");
        let previous = service
            .get_secret_value(
                &scope,
                GetSecretValueInput {
                    secret_id: secret_ref_from_name(&created.name),
                    version_id: None,
                    version_stage: Some(PREVIOUS_STAGE.to_owned()),
                },
            )
            .expect("previous version should load");
        service
            .tag_resource(
                &scope,
                TagResourceInput {
                    secret_id: secret_ref_from_name(&created.name),
                    tags: vec![SecretsManagerTag {
                        key: "tier".to_owned(),
                        value: "backend".to_owned(),
                    }],
                },
            )
            .expect("tag should succeed");
        service
            .untag_resource(
                &scope,
                UntagResourceInput {
                    secret_id: secret_ref_from_name(&created.name),
                    tag_keys: vec!["env".to_owned()],
                },
            )
            .expect("untag should succeed");
        let rotated = service
            .rotate_secret(
                &scope,
                RotateSecretInput {
                    client_request_token: None,
                    external_secret_rotation_metadata: Vec::new(),
                    external_secret_rotation_role_arn: None,
                    rotate_immediately: Some(false),
                    rotation_lambda_arn: Some(arn(
                        "arn:aws:lambda:eu-west-2:000000000000:function:rotate",
                    )),
                    rotation_rules: Some(SecretsManagerRotationRules {
                        automatically_after_days: Some(10),
                        duration: None,
                        schedule_expression: None,
                    }),
                    secret_id: secret_ref_from_name(&created.name),
                },
            )
            .expect("rotate should update metadata");
        let described = service
            .describe_secret(
                &scope,
                DescribeSecretInput {
                    secret_id: secret_ref_from_name(&created.name),
                },
            )
            .expect("describe should succeed");
        let listed = service
            .list_secrets(
                &scope,
                ListSecretsInput {
                    filters: vec![ListSecretsFilter {
                        key: ListSecretsFilterKey::TagKey,
                        values: vec!["tier".to_owned()],
                    }],
                    include_planned_deletion: false,
                    max_results: None,
                    next_token: None,
                    sort_by: Some(ListSecretsSortBy::Name),
                    sort_order: Some(ListSecretsSortOrder::Asc),
                },
            )
            .expect("list should succeed");
        let versions = service
            .list_secret_version_ids(
                &scope,
                ListSecretVersionIdsInput {
                    include_deprecated: false,
                    max_results: None,
                    next_token: None,
                    secret_id: secret_ref_from_name(&created.name),
                },
            )
            .expect("versions should list");

        assert_eq!(initial.secret_string.as_deref(), Some("alpha"));
        assert_eq!(pending.version_stages, vec!["AWSPENDING"]);
        assert_eq!(pending_value.secret_string.as_deref(), Some("beta"));
        assert_eq!(
            updated.version_id.as_deref(),
            Some(current.version_id.as_str())
        );
        assert_eq!(current.secret_string.as_deref(), Some("gamma"));
        assert_eq!(current.version_stages, vec![CURRENT_STAGE.to_owned()]);
        assert_eq!(previous.secret_string.as_deref(), Some("alpha"));
        assert_eq!(previous.version_stages, vec![PREVIOUS_STAGE.to_owned()]);
        assert_eq!(rotated.version_id, updated.version_id);
        assert_eq!(described.description.as_deref(), Some("updated"));
        assert_eq!(described.kms_key_id, Some(kms_key_ref("alias/local")));
        assert_eq!(described.rotation_enabled, Some(true));
        assert_eq!(
            described.rotation_lambda_arn,
            Some(
                arn("arn:aws:lambda:eu-west-2:000000000000:function:rotate",)
            )
        );
        assert_eq!(described.next_rotation_date, Some(864_100));
        assert_eq!(described.tags.len(), 1);
        assert_eq!(described.tags[0].key, "tier");
        assert_eq!(described.tags[0].value, "backend");
        assert_eq!(listed.secret_list.len(), 1);
        assert_eq!(listed.secret_list[0].name, created.name);
        assert_eq!(versions.versions.len(), 3);
        assert_eq!(versions.versions[0].created_date, 50);
    }

    #[test]
    fn secrets_manager_delete_hides_normal_lists_and_blocks_writes() {
        let service = service(
            "services-secrets-manager-delete",
            Arc::new(SequenceClock::new(vec![
                seconds(10),
                seconds(20),
                seconds(30),
                seconds(40),
                seconds(50),
                seconds(60),
                seconds(70),
            ])),
        );
        let scope = scope("eu-west-2");
        let created = create_string(&service, &scope, "app/secret", "alpha");
        let deleted = service
            .delete_secret(
                &scope,
                DeleteSecretInput {
                    force_delete_without_recovery: false,
                    recovery_window_in_days: Some(7),
                    secret_id: secret_ref_from_name(&created.name),
                },
            )
            .expect("delete should succeed");
        let described = service
            .describe_secret(
                &scope,
                DescribeSecretInput {
                    secret_id: secret_ref_from_name(&created.name),
                },
            )
            .expect("describe should work for deleted secret");
        let hidden = service
            .list_secrets(
                &scope,
                ListSecretsInput {
                    filters: Vec::new(),
                    include_planned_deletion: false,
                    max_results: None,
                    next_token: None,
                    sort_by: None,
                    sort_order: None,
                },
            )
            .expect("list should succeed");
        let visible = service
            .list_secrets(
                &scope,
                ListSecretsInput {
                    filters: Vec::new(),
                    include_planned_deletion: true,
                    max_results: None,
                    next_token: None,
                    sort_by: None,
                    sort_order: None,
                },
            )
            .expect("planned deletion list should succeed");
        let put_error = service
            .put_secret_value(
                &scope,
                PutSecretValueInput {
                    client_request_token: None,
                    secret_binary: None,
                    secret_id: secret_ref_from_name(&created.name),
                    secret_string: Some("beta".to_owned()),
                    version_stages: None,
                },
            )
            .expect_err("writes to deleted secrets should fail");
        let tag_error = service
            .tag_resource(
                &scope,
                TagResourceInput {
                    secret_id: secret_ref_from_name(&created.name),
                    tags: vec![SecretsManagerTag {
                        key: "env".to_owned(),
                        value: "prod".to_owned(),
                    }],
                },
            )
            .expect_err("tagging a deleted secret should fail");

        assert_eq!(deleted.deletion_date, 20 + 7 * 24 * 60 * 60);
        assert_eq!(described.deleted_date, Some(deleted.deletion_date));
        assert!(hidden.secret_list.is_empty());
        assert_eq!(visible.secret_list.len(), 1);
        assert_eq!(put_error.to_aws_error().code(), "InvalidRequestException");
        assert_eq!(tag_error.to_aws_error().code(), "InvalidRequestException");
    }

    #[test]
    fn secrets_manager_duplicate_lookup_and_delete_validation_errors_are_explicit()
     {
        let service = service(
            "services-secrets-manager-errors",
            Arc::new(SequenceClock::new(vec![
                seconds(10),
                seconds(20),
                seconds(30),
            ])),
        );
        let scope = scope("eu-west-2");
        let created = create_string(&service, &scope, "app/secret", "alpha");

        let duplicate = service
            .create_secret(
                &scope,
                CreateSecretInput {
                    client_request_token: None,
                    description: None,
                    kms_key_id: None,
                    name: secret_name("app/secret"),
                    secret_binary: None,
                    secret_string: Some("beta".to_owned()),
                    secret_type: None,
                    tags: Vec::new(),
                },
            )
            .expect_err("duplicate names should fail");
        let missing_version = service
            .get_secret_value(
                &scope,
                GetSecretValueInput {
                    secret_id: secret_ref("app/secret"),
                    version_id: Some(
                        "ffffffffffffffffffffffffffffffff".to_owned(),
                    ),
                    version_stage: None,
                },
            )
            .expect_err("missing version should fail");
        let mismatched_stage = service
            .get_secret_value(
                &scope,
                GetSecretValueInput {
                    secret_id: secret_ref("app/secret"),
                    version_id: created.version_id,
                    version_stage: Some(PREVIOUS_STAGE.to_owned()),
                },
            )
            .expect_err("mismatched version and stage should fail");
        let invalid_delete = service
            .delete_secret(
                &scope,
                DeleteSecretInput {
                    force_delete_without_recovery: true,
                    recovery_window_in_days: Some(7),
                    secret_id: secret_ref("app/secret"),
                },
            )
            .expect_err("conflicting delete flags should fail");

        assert_eq!(duplicate.to_aws_error().code(), "ResourceExistsException");
        assert_eq!(
            missing_version.to_aws_error().code(),
            "ResourceNotFoundException"
        );
        assert_eq!(
            mismatched_stage.to_aws_error().code(),
            "InvalidRequestException"
        );
        assert_eq!(
            invalid_delete.to_aws_error().code(),
            "InvalidParameterException"
        );
    }

    #[test]
    fn secrets_manager_filters_pagination_and_binary_payloads_are_pure() {
        let service = service(
            "services-secrets-manager-filters",
            Arc::new(SequenceClock::new(vec![
                seconds(10),
                seconds(20),
                seconds(30),
                seconds(40),
                seconds(50),
                seconds(60),
            ])),
        );
        let scope = scope("eu-west-2");
        service
            .create_secret(
                &scope,
                CreateSecretInput {
                    client_request_token: Some(
                        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
                    ),
                    description: Some("database".to_owned()),
                    kms_key_id: None,
                    name: secret_name("db/one"),
                    secret_binary: None,
                    secret_string: Some("one".to_owned()),
                    secret_type: None,
                    tags: vec![SecretsManagerTag {
                        key: "team".to_owned(),
                        value: "payments".to_owned(),
                    }],
                },
            )
            .expect("first secret should create");
        service
            .create_secret(
                &scope,
                CreateSecretInput {
                    client_request_token: Some(
                        "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_owned(),
                    ),
                    description: Some("binary".to_owned()),
                    kms_key_id: None,
                    name: secret_name("db/two"),
                    secret_binary: Some(vec![1, 2, 3]),
                    secret_string: None,
                    secret_type: None,
                    tags: vec![SecretsManagerTag {
                        key: "team".to_owned(),
                        value: "platform".to_owned(),
                    }],
                },
            )
            .expect("second secret should create");

        let filtered = service
            .list_secrets(
                &scope,
                ListSecretsInput {
                    filters: vec![ListSecretsFilter {
                        key: ListSecretsFilterKey::Description,
                        values: vec!["bin".to_owned()],
                    }],
                    include_planned_deletion: false,
                    max_results: Some(1),
                    next_token: None,
                    sort_by: Some(ListSecretsSortBy::Name),
                    sort_order: Some(ListSecretsSortOrder::Asc),
                },
            )
            .expect("filtered list should succeed");
        let next_page = service
            .list_secrets(
                &scope,
                ListSecretsInput {
                    filters: Vec::new(),
                    include_planned_deletion: false,
                    max_results: Some(1),
                    next_token: Some("1".to_owned()),
                    sort_by: Some(ListSecretsSortBy::Name),
                    sort_order: Some(ListSecretsSortOrder::Asc),
                },
            )
            .expect("next page should succeed");
        let binary = service
            .get_secret_value(
                &scope,
                GetSecretValueInput {
                    secret_id: secret_ref("db/two"),
                    version_id: None,
                    version_stage: None,
                },
            )
            .expect("binary secret should load");
        let invalid_token = service
            .list_secret_version_ids(
                &scope,
                ListSecretVersionIdsInput {
                    include_deprecated: false,
                    max_results: Some(1),
                    next_token: Some("bogus".to_owned()),
                    secret_id: secret_ref("db/one"),
                },
            )
            .expect_err("invalid next token should fail");

        assert_eq!(filtered.secret_list.len(), 1);
        assert_eq!(filtered.secret_list[0].name.as_str(), "db/two");
        assert_eq!(next_page.secret_list.len(), 1);
        assert_eq!(next_page.secret_list[0].name.as_str(), "db/two");
        assert_eq!(binary.secret_binary, Some(vec![1, 2, 3]));
        assert_eq!(
            serde_json::to_value(&binary)
                .expect("binary output should serialize")["SecretBinary"],
            serde_json::Value::String("AQID".to_owned())
        );
        assert_eq!(
            invalid_token.to_aws_error().code(),
            "InvalidNextTokenException"
        );
    }

    #[test]
    fn secrets_manager_rotation_validation_and_force_delete_are_explicit() {
        let service = service(
            "services-secrets-manager-rotation",
            Arc::new(SequenceClock::new(vec![seconds(10), seconds(20)])),
        );
        let scope = scope("eu-west-2");
        create_string(&service, &scope, "app/secret", "alpha");

        let invalid_lambda = service
            .rotate_secret(
                &scope,
                RotateSecretInput {
                    client_request_token: None,
                    external_secret_rotation_metadata: Vec::new(),
                    external_secret_rotation_role_arn: None,
                    rotate_immediately: None,
                    rotation_lambda_arn: Some(arn(
                        "arn:aws:kms:eu-west-2:000000000000:key/demo",
                    )),
                    rotation_rules: None,
                    secret_id: secret_ref("app/secret"),
                },
            )
            .expect_err("invalid rotation arn should fail");
        let force_deleted = service
            .delete_secret(
                &scope,
                DeleteSecretInput {
                    force_delete_without_recovery: true,
                    recovery_window_in_days: None,
                    secret_id: secret_ref("app/secret"),
                },
            )
            .expect("force delete should succeed");
        let missing_after_force_delete = service
            .describe_secret(
                &scope,
                DescribeSecretInput { secret_id: secret_ref("app/secret") },
            )
            .expect_err("force deleted secret should be removed");

        assert_eq!(
            invalid_lambda.to_aws_error().code(),
            "InvalidParameterException"
        );
        assert_eq!(force_deleted.deletion_date, 20);
        assert_eq!(
            missing_after_force_delete.to_aws_error().code(),
            "ResourceNotFoundException"
        );
    }

    #[test]
    fn secrets_manager_typed_identifiers_round_trip_valid_names_and_arns() {
        let secret_name = "app/secret"
            .parse::<SecretName>()
            .expect("valid secret names should parse");
        let secret_reference =
            "arn:aws:secretsmanager:eu-west-2:000000000000:secret:app/secret"
                .parse::<SecretReference>()
                .expect("valid secret ARNs should parse");

        assert_eq!(secret_name.as_str(), "app/secret");
        assert_eq!(secret_name.to_string(), "app/secret");
        assert_eq!(
            secret_reference.to_string(),
            "arn:aws:secretsmanager:eu-west-2:000000000000:secret:app/secret"
        );
    }

    #[test]
    fn secrets_manager_typed_identifiers_validate_names_and_arns() {
        assert_eq!(
            "bad name"
                .parse::<SecretName>()
                .expect_err("secret names should reject spaces")
                .to_string(),
            "Secret name contains invalid characters."
        );
        assert_eq!(
            "arn:aws:s3:::bucket"
                .parse::<SecretReference>()
                .expect_err("non-secrets ARNs should fail")
                .to_string(),
            "Secret ARNs must reference the secretsmanager service."
        );
    }
}
