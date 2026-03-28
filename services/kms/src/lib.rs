mod aliases;
mod crypto;
mod errors;
mod keys;
mod scope;

use self::aliases::{
    KmsAliasStorageKey, StoredKmsAlias, alias_scope_filter, alias_storage_key,
};
use self::crypto::{data_key_byte_len, ensure_encrypt_key, ensure_sign_key};
use self::keys::{
    KmsKeySpec, KmsKeyState, KmsKeyStorageKey, StoredKmsKey,
    current_key_material_id, scope_filter, storage_key,
};
use aws::{Arn, Clock, KmsAliasName, KmsKeyId, KmsKeyReference, ServiceName};
use std::sync::Arc;
use std::time::UNIX_EPOCH;
use storage::{StorageFactory, StorageHandle};

pub use self::aliases::{
    CreateKmsAliasInput, DeleteKmsAliasInput, KmsAliasEntry,
    ListKmsAliasesInput, ListKmsAliasesOutput,
};
pub use self::crypto::{
    KmsDecryptInput, KmsDecryptOutput, KmsEncryptInput, KmsEncryptOutput,
    KmsGenerateDataKeyInput, KmsGenerateDataKeyOutput,
    KmsGenerateDataKeyWithoutPlaintextOutput, KmsReEncryptInput,
    KmsReEncryptOutput, KmsSignInput, KmsSignOutput, KmsVerifyInput,
    KmsVerifyOutput,
};
pub use self::errors::KmsError;
pub use self::keys::{
    CancelKmsKeyDeletionInput, CancelKmsKeyDeletionOutput, CreateKmsKeyInput,
    CreateKmsKeyOutput, DescribeKmsKeyOutput, KmsKeyListEntry, KmsKeyMetadata,
    KmsListResourceTagsInput, KmsListResourceTagsOutput, KmsTag,
    KmsTagResourceInput, KmsUntagResourceInput, ListKmsKeysOutput,
    ScheduleKmsKeyDeletionInput, ScheduleKmsKeyDeletionOutput,
};
pub use self::scope::KmsScope;

pub(crate) use self::crypto::{KmsMessageType, MockKmsCodec, derived_bytes};
pub(crate) use self::keys::{KmsKeyUsage, format_key_id, validate_tags};

pub(crate) const CIPHERTEXT_PREFIX: &str = "cloudish-kms-cipher-v1";
pub(crate) const SIGNATURE_PREFIX: &str = "cloudish-kms-signature-v1";
pub(crate) const DEFAULT_PENDING_WINDOW_DAYS: u32 = 30;
pub(crate) const MIN_PENDING_WINDOW_DAYS: u32 = 7;
pub(crate) const MAX_PENDING_WINDOW_DAYS: u32 = 30;
pub(crate) const MAX_DATA_KEY_BYTES: usize = 1024;
pub(crate) const MAX_TAGS: usize = 50;
pub(crate) const MAX_TAG_KEY_BYTES: usize = 128;
pub(crate) const MAX_TAG_VALUE_BYTES: usize = 256;

#[derive(Clone)]
pub struct KmsService {
    alias_store: StorageHandle<KmsAliasStorageKey, StoredKmsAlias>,
    clock: Arc<dyn Clock>,
    key_store: StorageHandle<KmsKeyStorageKey, StoredKmsKey>,
}

impl KmsService {
    pub fn new(factory: &StorageFactory, clock: Arc<dyn Clock>) -> Self {
        Self {
            alias_store: factory.create("kms", "aliases"),
            clock,
            key_store: factory.create("kms", "keys"),
        }
    }

    /// Creates a KMS key in the current scope.
    ///
    /// # Errors
    ///
    /// Returns `KmsError` when the requested key usage, key spec, generated
    /// identifiers, or tags are invalid.
    pub fn create_key(
        &self,
        scope: &KmsScope,
        input: CreateKmsKeyInput,
    ) -> Result<CreateKmsKeyOutput, KmsError> {
        let key_usage = KmsKeyUsage::parse(
            input.key_usage.as_deref().unwrap_or("ENCRYPT_DECRYPT"),
        )?;
        let key_spec = match input.key_spec.as_deref() {
            Some(value) => KmsKeySpec::parse(value)?,
            None => match key_usage {
                KmsKeyUsage::EncryptDecrypt => KmsKeySpec::SymmetricDefault,
                KmsKeyUsage::SignVerify => KmsKeySpec::Rsa2048,
            },
        };
        let tags = validate_tags(&input.tags)?;
        let sequence = self.key_store.scan(&scope_filter(scope)).len() + 1;
        let key_id = generated_key_id(scope, sequence)?;
        let now = current_epoch_seconds(&*self.clock);
        let stored = StoredKmsKey {
            account_id: scope.account_id().clone(),
            arn: kms_arn(scope, &format!("key/{}", key_id.as_str()))?,
            creation_date: now,
            current_key_material_id: current_key_material_id(
                scope, &key_id, key_spec, key_usage,
            ),
            deletion_date: None,
            description: input.description.unwrap_or_default(),
            key_id: key_id.clone(),
            key_spec,
            key_state: KmsKeyState::Enabled,
            key_usage,
            pending_deletion_window_in_days: None,
            region: scope.region().clone(),
            tags,
        };
        self.key_store
            .put(storage_key(scope, key_id.as_str()), stored.clone())?;

        Ok(CreateKmsKeyOutput { key_metadata: stored.metadata() })
    }

    /// Describes a key by key id or alias reference.
    ///
    /// # Errors
    ///
    /// Returns `KmsError` when the requested key reference is invalid or not
    /// found in the current scope.
    pub fn describe_key(
        &self,
        scope: &KmsScope,
        key_id: &KmsKeyReference,
    ) -> Result<DescribeKmsKeyOutput, KmsError> {
        let key = self.resolve_key(scope, key_id)?;

        Ok(DescribeKmsKeyOutput { key_metadata: key.metadata() })
    }

    /// Lists all keys in the current scope.
    ///
    /// # Errors
    ///
    /// Returns `KmsError` if the backing key store cannot be read.
    pub fn list_keys(
        &self,
        scope: &KmsScope,
    ) -> Result<ListKmsKeysOutput, KmsError> {
        let mut keys = self
            .key_store
            .scan(&scope_filter(scope))
            .into_iter()
            .map(|key| KmsKeyListEntry {
                key_id: key.key_id,
                key_arn: key.arn,
            })
            .collect::<Vec<_>>();
        keys.sort_by(|left, right| left.key_id.cmp(&right.key_id));

        Ok(ListKmsKeysOutput { keys, truncated: false })
    }

    /// Schedules a key for deletion.
    ///
    /// # Errors
    ///
    /// Returns `KmsError` when the key cannot be resolved, is already pending
    /// deletion, or the requested pending window is outside the supported
    /// range.
    pub fn schedule_key_deletion(
        &self,
        scope: &KmsScope,
        input: ScheduleKmsKeyDeletionInput,
    ) -> Result<ScheduleKmsKeyDeletionOutput, KmsError> {
        let mut key = self.resolve_key(scope, &input.key_id)?;
        if key.key_state == KmsKeyState::PendingDeletion {
            return Err(invalid_state_error(format!(
                "{} is pending deletion.",
                key.arn
            )));
        }

        let pending_window_in_days = input
            .pending_window_in_days
            .unwrap_or(DEFAULT_PENDING_WINDOW_DAYS);
        if !(MIN_PENDING_WINDOW_DAYS..=MAX_PENDING_WINDOW_DAYS)
            .contains(&pending_window_in_days)
        {
            return Err(KmsError::ValidationException {
                message: format!(
                    "1 validation error detected: Value '{pending_window_in_days}' at \
                     'pendingWindowInDays' failed to satisfy constraint: \
                     Member must have value less than or equal to {MAX_PENDING_WINDOW_DAYS} and \
                     greater than or equal to {MIN_PENDING_WINDOW_DAYS}",
                ),
            });
        }

        let deletion_date = current_epoch_seconds(&*self.clock)
            .saturating_add(u64::from(pending_window_in_days) * 86_400);
        key.key_state = KmsKeyState::PendingDeletion;
        key.deletion_date = Some(deletion_date);
        key.pending_deletion_window_in_days = Some(pending_window_in_days);
        self.key_store
            .put(storage_key(scope, key.key_id.as_str()), key.clone())?;

        Ok(ScheduleKmsKeyDeletionOutput {
            deletion_date,
            key_id: key.arn,
            key_state: KmsKeyState::PendingDeletion.as_str().to_owned(),
            pending_window_in_days,
        })
    }

    /// Cancels a previously scheduled key deletion.
    ///
    /// # Errors
    ///
    /// Returns `KmsError` when the key cannot be resolved or is not currently
    /// pending deletion.
    pub fn cancel_key_deletion(
        &self,
        scope: &KmsScope,
        input: CancelKmsKeyDeletionInput,
    ) -> Result<CancelKmsKeyDeletionOutput, KmsError> {
        let mut key = self.resolve_key(scope, &input.key_id)?;
        if key.key_state != KmsKeyState::PendingDeletion {
            return Err(invalid_state_error(format!(
                "{} is not pending deletion.",
                key.arn
            )));
        }

        key.key_state = KmsKeyState::Enabled;
        key.deletion_date = None;
        key.pending_deletion_window_in_days = None;
        self.key_store
            .put(storage_key(scope, key.key_id.as_str()), key.clone())?;

        Ok(CancelKmsKeyDeletionOutput { key_id: key.arn })
    }

    /// Creates an alias for an existing key.
    ///
    /// # Errors
    ///
    /// Returns `KmsError` when the target key cannot be resolved, the alias
    /// already exists, or the alias ARN cannot be shaped for this scope.
    pub fn create_alias(
        &self,
        scope: &KmsScope,
        input: CreateKmsAliasInput,
    ) -> Result<(), KmsError> {
        let target_key = self.resolve_key(scope, &input.target_key_id)?;
        let alias_key = alias_storage_key(scope, input.alias_name.as_str());
        if self.alias_store.get(&alias_key).is_some() {
            return Err(KmsError::AlreadyExistsException {
                message: format!("Alias {} already exists.", input.alias_name),
            });
        }

        let stored = StoredKmsAlias {
            alias_arn: kms_arn(scope, input.alias_name.as_str())?,
            alias_name: input.alias_name.clone(),
            creation_date: current_epoch_seconds(&*self.clock),
            target_key_id: target_key.key_id,
        };
        self.alias_store.put(alias_key, stored)?;

        Ok(())
    }

    /// Deletes an alias in the current scope.
    ///
    /// # Errors
    ///
    /// Returns `KmsError` when the alias does not exist or the backing store
    /// cannot be updated.
    pub fn delete_alias(
        &self,
        scope: &KmsScope,
        input: DeleteKmsAliasInput,
    ) -> Result<(), KmsError> {
        let alias_key = alias_storage_key(scope, input.alias_name.as_str());
        if self.alias_store.get(&alias_key).is_none() {
            return Err(KmsError::NotFoundException {
                message: format!(
                    "Alias '{}' does not exist",
                    input.alias_name
                ),
            });
        }
        self.alias_store.delete(&alias_key)?;

        Ok(())
    }

    /// Lists aliases, optionally filtered to a single key.
    ///
    /// # Errors
    ///
    /// Returns `KmsError` when the optional key filter cannot be resolved.
    pub fn list_aliases(
        &self,
        scope: &KmsScope,
        input: ListKmsAliasesInput,
    ) -> Result<ListKmsAliasesOutput, KmsError> {
        let key_id = input
            .key_id
            .as_ref()
            .map(|identifier| self.resolve_key(scope, identifier))
            .transpose()?
            .map(|key| key.key_id);
        let mut aliases = self
            .alias_store
            .scan(&alias_scope_filter(scope))
            .into_iter()
            .filter(|alias| {
                key_id
                    .as_ref()
                    .map(|key_id| key_id == &alias.target_key_id)
                    .unwrap_or(true)
            })
            .map(|alias| KmsAliasEntry {
                alias_arn: alias.alias_arn,
                alias_name: alias.alias_name,
                creation_date: alias.creation_date,
                target_key_id: alias.target_key_id,
            })
            .collect::<Vec<_>>();
        aliases.sort_by(|left, right| left.alias_name.cmp(&right.alias_name));

        Ok(ListKmsAliasesOutput { aliases, truncated: false })
    }

    /// Encrypts plaintext with the selected key.
    ///
    /// # Errors
    ///
    /// Returns `KmsError` when the key cannot be resolved or is not valid for
    /// encryption.
    pub fn encrypt(
        &self,
        scope: &KmsScope,
        input: KmsEncryptInput,
    ) -> Result<KmsEncryptOutput, KmsError> {
        let key = self.resolve_key(scope, &input.key_id)?;
        ensure_encrypt_key(&key, "Encrypt")?;

        Ok(KmsEncryptOutput {
            ciphertext_blob: MockKmsCodec::encrypt(
                &key.key_id,
                &input.plaintext,
            ),
            key_id: key.arn,
        })
    }

    /// Decrypts ciphertext produced by the local deterministic codec.
    ///
    /// # Errors
    ///
    /// Returns `KmsError` when the ciphertext is invalid, the optional key
    /// reference does not match the ciphertext key id, or the resolved key is
    /// not valid for decryption.
    pub fn decrypt(
        &self,
        scope: &KmsScope,
        input: KmsDecryptInput,
    ) -> Result<KmsDecryptOutput, KmsError> {
        let decoded = MockKmsCodec::decrypt(&input.ciphertext_blob)?;
        let key = match input.key_id.as_ref() {
            Some(identifier) => {
                let key = self.resolve_key(scope, identifier)?;
                if key.key_id != decoded.key_id {
                    return Err(KmsError::IncorrectKeyException {
                        message:
                            "The key ID in the request does not identify \
                                  a CMK that can perform this operation."
                                .to_owned(),
                    });
                }
                key
            }
            None => self.resolve_key(
                scope,
                &KmsKeyReference::KeyId(decoded.key_id.clone()),
            )?,
        };
        ensure_encrypt_key(&key, "Decrypt")?;

        Ok(KmsDecryptOutput { key_id: key.arn, plaintext: decoded.plaintext })
    }

    /// Decrypts with the source key and re-encrypts with the destination key.
    ///
    /// # Errors
    ///
    /// Returns `KmsError` when the ciphertext is invalid, either key cannot be
    /// resolved, the optional source key does not match, or either key has an
    /// incompatible usage or state.
    pub fn re_encrypt(
        &self,
        scope: &KmsScope,
        input: KmsReEncryptInput,
    ) -> Result<KmsReEncryptOutput, KmsError> {
        let decoded = MockKmsCodec::decrypt(&input.ciphertext_blob)?;
        let source_key = match input.source_key_id.as_ref() {
            Some(identifier) => {
                let key = self.resolve_key(scope, identifier)?;
                if key.key_id != decoded.key_id {
                    return Err(KmsError::IncorrectKeyException {
                        message:
                            "The key ID in the request does not identify \
                                  a CMK that can perform this operation."
                                .to_owned(),
                    });
                }
                key
            }
            None => self.resolve_key(
                scope,
                &KmsKeyReference::KeyId(decoded.key_id.clone()),
            )?,
        };
        ensure_encrypt_key(&source_key, "ReEncrypt")?;
        let destination_key =
            self.resolve_key(scope, &input.destination_key_id)?;
        ensure_encrypt_key(&destination_key, "ReEncrypt")?;

        Ok(KmsReEncryptOutput {
            ciphertext_blob: MockKmsCodec::encrypt(
                &destination_key.key_id,
                &decoded.plaintext,
            ),
            key_id: destination_key.arn,
            source_key_id: source_key.arn,
        })
    }

    /// Generates a deterministic data key and its ciphertext wrapper.
    ///
    /// # Errors
    ///
    /// Returns `KmsError` when the key cannot be resolved, the data-key size
    /// inputs are invalid, or the key is not valid for encryption.
    pub fn generate_data_key(
        &self,
        scope: &KmsScope,
        input: KmsGenerateDataKeyInput,
    ) -> Result<KmsGenerateDataKeyOutput, KmsError> {
        let key = self.resolve_key(scope, &input.key_id)?;
        ensure_encrypt_key(&key, "GenerateDataKey")?;
        let byte_len = data_key_byte_len(&input)?;
        let plaintext =
            derived_bytes(&["data-key", key.key_id.as_str()], byte_len);

        Ok(KmsGenerateDataKeyOutput {
            ciphertext_blob: MockKmsCodec::encrypt(&key.key_id, &plaintext),
            key_id: key.arn,
            plaintext,
        })
    }

    /// Generates a deterministic data key and returns only the ciphertext.
    ///
    /// # Errors
    ///
    /// Returns `KmsError` when data-key generation fails for the same reasons
    /// as [`KmsService::generate_data_key`].
    pub fn generate_data_key_without_plaintext(
        &self,
        scope: &KmsScope,
        input: KmsGenerateDataKeyInput,
    ) -> Result<KmsGenerateDataKeyWithoutPlaintextOutput, KmsError> {
        let output = self.generate_data_key(scope, input)?;

        Ok(KmsGenerateDataKeyWithoutPlaintextOutput {
            ciphertext_blob: output.ciphertext_blob,
            key_id: output.key_id,
        })
    }

    /// Produces a deterministic local signature for the supplied message.
    ///
    /// # Errors
    ///
    /// Returns `KmsError` when the key cannot be resolved, the message type is
    /// invalid, or the key cannot be used for signing.
    pub fn sign(
        &self,
        scope: &KmsScope,
        input: KmsSignInput,
    ) -> Result<KmsSignOutput, KmsError> {
        let key = self.resolve_key(scope, &input.key_id)?;
        ensure_sign_key(&key, "Sign")?;
        let message_type =
            KmsMessageType::parse(input.message_type.as_deref())?;

        Ok(KmsSignOutput {
            key_id: key.arn,
            signature: MockKmsCodec::sign(
                &key.key_id,
                &input.message,
                message_type,
                &input.signing_algorithm,
            ),
            signing_algorithm: input.signing_algorithm,
        })
    }

    /// Verifies a deterministic local signature.
    ///
    /// # Errors
    ///
    /// Returns `KmsError` when the key cannot be resolved, the message type or
    /// signature is invalid, or the key cannot be used for verification.
    pub fn verify(
        &self,
        scope: &KmsScope,
        input: KmsVerifyInput,
    ) -> Result<KmsVerifyOutput, KmsError> {
        let key = self.resolve_key(scope, &input.key_id)?;
        ensure_sign_key(&key, "Verify")?;
        let message_type =
            KmsMessageType::parse(input.message_type.as_deref())?;
        let decoded = MockKmsCodec::decode_signature(&input.signature)?;
        if decoded.key_id != key.key_id
            || decoded.signing_algorithm != input.signing_algorithm
            || decoded.message_type != message_type
            || decoded.message != input.message
        {
            return Err(KmsError::InvalidSignatureException {
                message: "The signature is invalid.".to_owned(),
            });
        }

        Ok(KmsVerifyOutput {
            key_id: key.arn,
            signature_valid: true,
            signing_algorithm: input.signing_algorithm,
        })
    }

    /// Adds or replaces tags on the selected key.
    ///
    /// # Errors
    ///
    /// Returns `KmsError` when the key cannot be resolved or the supplied tags
    /// fail validation.
    pub fn tag_resource(
        &self,
        scope: &KmsScope,
        input: KmsTagResourceInput,
    ) -> Result<(), KmsError> {
        let mut key = self.resolve_key(scope, &input.key_id)?;
        for (tag_key, tag_value) in validate_tags(&input.tags)? {
            key.tags.insert(tag_key, tag_value);
        }
        self.key_store.put(storage_key(scope, key.key_id.as_str()), key)?;

        Ok(())
    }

    /// Removes tags from the selected key.
    ///
    /// # Errors
    ///
    /// Returns `KmsError` when the key cannot be resolved.
    pub fn untag_resource(
        &self,
        scope: &KmsScope,
        input: KmsUntagResourceInput,
    ) -> Result<(), KmsError> {
        let mut key = self.resolve_key(scope, &input.key_id)?;
        for tag_key in input.tag_keys {
            key.tags.remove(&tag_key);
        }
        self.key_store.put(storage_key(scope, key.key_id.as_str()), key)?;

        Ok(())
    }

    /// Lists tags attached to the selected key.
    ///
    /// # Errors
    ///
    /// Returns `KmsError` when the key cannot be resolved.
    pub fn list_resource_tags(
        &self,
        scope: &KmsScope,
        input: KmsListResourceTagsInput,
    ) -> Result<KmsListResourceTagsOutput, KmsError> {
        let key = self.resolve_key(scope, &input.key_id)?;
        let tags = key
            .tags
            .into_iter()
            .map(|(tag_key, tag_value)| KmsTag { tag_key, tag_value })
            .collect();

        Ok(KmsListResourceTagsOutput { tags, truncated: false })
    }

    fn resolve_key(
        &self,
        scope: &KmsScope,
        identifier: &KmsKeyReference,
    ) -> Result<StoredKmsKey, KmsError> {
        let key_id = match identifier {
            KmsKeyReference::AliasArn(arn) => {
                ensure_key_reference_scope(scope, arn, identifier)?;
                let alias_name = parse_alias_name_from_arn(arn, identifier)?;
                self.alias_store
                    .get(&alias_storage_key(scope, alias_name.as_str()))
                    .map(|alias| alias.target_key_id)
                    .ok_or_else(|| alias_name_not_found(&alias_name))?
            }
            KmsKeyReference::AliasName(alias_name) => self
                .alias_store
                .get(&alias_storage_key(scope, alias_name.as_str()))
                .map(|alias| alias.target_key_id)
                .ok_or_else(|| alias_name_not_found(alias_name))?,
            KmsKeyReference::KeyArn(arn) => {
                ensure_key_reference_scope(scope, arn, identifier)?;
                parse_key_id_from_arn(arn, identifier)?
            }
            KmsKeyReference::KeyId(key_id) => key_id.clone(),
        };

        self.key_store.get(&storage_key(scope, key_id.as_str())).ok_or_else(
            || KmsError::NotFoundException {
                message: format!("Key '{identifier}' does not exist"),
            },
        )
    }
}

fn ensure_key_reference_scope(
    scope: &KmsScope,
    arn: &Arn,
    identifier: &KmsKeyReference,
) -> Result<(), KmsError> {
    if arn.service() != ServiceName::Kms {
        return Err(invalid_key_arn(identifier));
    }
    if arn.region() != Some(scope.region())
        || arn.account_id() != Some(scope.account_id())
    {
        return Err(KmsError::NotFoundException {
            message: format!("Key '{identifier}' does not exist"),
        });
    }

    Ok(())
}

fn parse_key_id_from_arn(
    arn: &Arn,
    identifier: &KmsKeyReference,
) -> Result<KmsKeyId, KmsError> {
    match arn.resource() {
        aws::ArnResource::Generic(resource) => resource
            .strip_prefix("key/")
            .ok_or_else(|| invalid_key_arn(identifier))
            .and_then(|key_id| {
                KmsKeyId::new(key_id.to_owned())
                    .map_err(|_| invalid_key_arn(identifier))
            }),
        _ => Err(invalid_key_arn(identifier)),
    }
}

fn parse_alias_name_from_arn(
    arn: &Arn,
    identifier: &KmsKeyReference,
) -> Result<KmsAliasName, KmsError> {
    match arn.resource() {
        aws::ArnResource::Generic(resource) => resource
            .parse::<KmsAliasName>()
            .map_err(|_| invalid_key_arn(identifier)),
        _ => Err(invalid_key_arn(identifier)),
    }
}

fn invalid_key_arn(identifier: &KmsKeyReference) -> KmsError {
    KmsError::InvalidArnException {
        message: format!("Invalid arn {identifier}"),
    }
}

fn alias_name_not_found(alias_name: &KmsAliasName) -> KmsError {
    KmsError::NotFoundException {
        message: format!("Alias '{alias_name}' does not exist"),
    }
}

pub(crate) fn invalid_state_error(message: String) -> KmsError {
    KmsError::KmsInvalidStateException { message }
}

fn current_epoch_seconds(clock: &dyn Clock) -> u64 {
    clock.now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs()
}

fn generated_key_id(
    scope: &KmsScope,
    sequence: usize,
) -> Result<KmsKeyId, KmsError> {
    KmsKeyId::new(format_key_id(scope, sequence)).map_err(|error| {
        KmsError::InternalFailure {
            message: format!(
                "failed to construct generated KMS key id: {error}"
            ),
        }
    })
}

fn kms_arn(scope: &KmsScope, resource: &str) -> Result<Arn, KmsError> {
    Arn::new(
        aws::Partition::aws(),
        ServiceName::Kms,
        Some(scope.region().clone()),
        Some(scope.account_id().clone()),
        aws::ArnResource::Generic(resource.to_owned()),
    )
    .map_err(|error| KmsError::InternalFailure {
        message: format!(
            "failed to construct KMS ARN for {resource}: {error}"
        ),
    })
}

#[cfg(test)]
mod tests {
    use super::{
        CancelKmsKeyDeletionInput, CreateKmsAliasInput, CreateKmsKeyInput,
        DeleteKmsAliasInput, KmsDecryptInput, KmsGenerateDataKeyInput,
        KmsKeyUsage, KmsListResourceTagsInput, KmsMessageType, KmsScope,
        KmsService, KmsSignInput, KmsTag, KmsTagResourceInput,
        KmsUntagResourceInput, ListKmsAliasesInput,
        ScheduleKmsKeyDeletionInput,
    };
    use aws::{Clock, KmsAliasName, KmsKeyId, KmsKeyReference};
    use std::sync::Arc;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};
    use storage::{StorageConfig, StorageFactory, StorageMode};

    #[derive(Debug)]
    struct FixedClock(SystemTime);

    impl Clock for FixedClock {
        fn now(&self) -> SystemTime {
            self.0
        }
    }

    fn scope() -> KmsScope {
        KmsScope::new(
            "000000000000".parse().expect("account should parse"),
            "eu-west-2".parse().expect("region should parse"),
        )
    }

    fn service() -> KmsService {
        let factory = StorageFactory::new(StorageConfig::new(
            "/tmp/cloudish-kms-tests",
            StorageMode::Memory,
        ));

        KmsService::new(
            &factory,
            Arc::new(FixedClock(UNIX_EPOCH + Duration::from_secs(60))),
        )
    }

    fn alias_name(value: &str) -> KmsAliasName {
        value.parse().expect("alias name should parse")
    }

    fn key_ref(value: &str) -> KmsKeyReference {
        value.parse().expect("key reference should parse")
    }

    fn key_ref_from_id(value: &KmsKeyId) -> KmsKeyReference {
        value.clone().into()
    }

    #[test]
    fn kms_lifecycle_alias_tags_and_crypto_round_trip_are_deterministic() {
        let scope = scope();
        let kms = service();
        let encrypt_key = kms
            .create_key(
                &scope,
                CreateKmsKeyInput {
                    description: Some("encrypt".to_owned()),
                    key_spec: None,
                    key_usage: None,
                    tags: vec![KmsTag {
                        tag_key: "env".to_owned(),
                        tag_value: "dev".to_owned(),
                    }],
                },
            )
            .expect("encrypt key should create");
        let sign_key = kms
            .create_key(
                &scope,
                CreateKmsKeyInput {
                    description: Some("sign".to_owned()),
                    key_spec: Some("RSA_2048".to_owned()),
                    key_usage: Some("SIGN_VERIFY".to_owned()),
                    tags: Vec::new(),
                },
            )
            .expect("sign key should create");

        assert_eq!(encrypt_key.key_metadata.key_usage, "ENCRYPT_DECRYPT");
        assert_eq!(sign_key.key_metadata.key_usage, "SIGN_VERIFY");
        assert_eq!(encrypt_key.key_metadata.creation_date, 60);
        assert_eq!(
            kms.describe_key(
                &scope,
                &key_ref_from_id(&encrypt_key.key_metadata.key_id),
            )
            .expect("describe should succeed")
            .key_metadata
            .arn,
            encrypt_key.key_metadata.arn
        );

        let keys = kms.list_keys(&scope).expect("list keys should succeed");
        assert_eq!(keys.keys.len(), 2);

        kms.create_alias(
            &scope,
            CreateKmsAliasInput {
                alias_name: alias_name("alias/demo"),
                target_key_id: key_ref_from_id(
                    &encrypt_key.key_metadata.key_id,
                ),
            },
        )
        .expect("alias should create");
        assert_eq!(
            kms.list_aliases(
                &scope,
                ListKmsAliasesInput {
                    key_id: Some(key_ref_from_id(
                        &encrypt_key.key_metadata.key_id,
                    )),
                },
            )
            .expect("aliases should list")
            .aliases
            .len(),
            1
        );

        let encrypt = kms
            .encrypt(
                &scope,
                super::KmsEncryptInput {
                    key_id: key_ref("alias/demo"),
                    plaintext: b"payload".to_vec(),
                },
            )
            .expect("encrypt should succeed");
        let decrypt = kms
            .decrypt(
                &scope,
                KmsDecryptInput {
                    ciphertext_blob: encrypt.ciphertext_blob.clone(),
                    key_id: None,
                },
            )
            .expect("decrypt should succeed");
        assert_eq!(decrypt.plaintext, b"payload");
        assert_eq!(decrypt.key_id, encrypt_key.key_metadata.arn);

        let re_encrypt = kms
            .re_encrypt(
                &scope,
                super::KmsReEncryptInput {
                    ciphertext_blob: encrypt.ciphertext_blob.clone(),
                    destination_key_id: key_ref_from_id(
                        &encrypt_key.key_metadata.key_id,
                    ),
                    source_key_id: Some(key_ref_from_id(
                        &encrypt_key.key_metadata.key_id,
                    )),
                },
            )
            .expect("re-encrypt should succeed");
        assert_eq!(re_encrypt.source_key_id, encrypt_key.key_metadata.arn);
        assert_eq!(re_encrypt.key_id, encrypt_key.key_metadata.arn);

        let data_key = kms
            .generate_data_key(
                &scope,
                KmsGenerateDataKeyInput {
                    key_id: key_ref_from_id(&encrypt_key.key_metadata.key_id),
                    key_spec: Some("AES_256".to_owned()),
                    number_of_bytes: None,
                },
            )
            .expect("data key should generate");
        assert_eq!(data_key.plaintext.len(), 32);
        let decrypted_data_key = kms
            .decrypt(
                &scope,
                KmsDecryptInput {
                    ciphertext_blob: data_key.ciphertext_blob.clone(),
                    key_id: Some(key_ref_from_id(
                        &encrypt_key.key_metadata.key_id,
                    )),
                },
            )
            .expect("generated data key should decrypt");
        assert_eq!(decrypted_data_key.plaintext, data_key.plaintext);
        let without_plaintext = kms
            .generate_data_key_without_plaintext(
                &scope,
                KmsGenerateDataKeyInput {
                    key_id: key_ref_from_id(&encrypt_key.key_metadata.key_id),
                    key_spec: None,
                    number_of_bytes: Some(24),
                },
            )
            .expect("data key without plaintext should generate");
        assert!(!without_plaintext.ciphertext_blob.is_empty());

        let signature = kms
            .sign(
                &scope,
                KmsSignInput {
                    key_id: key_ref_from_id(&sign_key.key_metadata.key_id),
                    message: b"message".to_vec(),
                    message_type: Some(
                        KmsMessageType::Raw.as_str().to_owned(),
                    ),
                    signing_algorithm: "RSASSA_PSS_SHA_256".to_owned(),
                },
            )
            .expect("sign should succeed");
        let verification = kms
            .verify(
                &scope,
                super::KmsVerifyInput {
                    key_id: key_ref_from_id(&sign_key.key_metadata.key_id),
                    message: b"message".to_vec(),
                    message_type: Some(
                        KmsMessageType::Raw.as_str().to_owned(),
                    ),
                    signature: signature.signature.clone(),
                    signing_algorithm: "RSASSA_PSS_SHA_256".to_owned(),
                },
            )
            .expect("verify should succeed");
        assert!(verification.signature_valid);

        kms.tag_resource(
            &scope,
            KmsTagResourceInput {
                key_id: key_ref_from_id(&encrypt_key.key_metadata.key_id),
                tags: vec![KmsTag {
                    tag_key: "team".to_owned(),
                    tag_value: "platform".to_owned(),
                }],
            },
        )
        .expect("tag resource should succeed");
        assert_eq!(
            kms.list_resource_tags(
                &scope,
                KmsListResourceTagsInput {
                    key_id: key_ref_from_id(&encrypt_key.key_metadata.key_id),
                },
            )
            .expect("tags should list")
            .tags
            .len(),
            2
        );
        kms.untag_resource(
            &scope,
            KmsUntagResourceInput {
                key_id: key_ref_from_id(&encrypt_key.key_metadata.key_id),
                tag_keys: vec!["team".to_owned()],
            },
        )
        .expect("untag should succeed");
        assert_eq!(
            kms.list_resource_tags(
                &scope,
                KmsListResourceTagsInput {
                    key_id: key_ref_from_id(&encrypt_key.key_metadata.key_id),
                },
            )
            .expect("tags should list")
            .tags
            .len(),
            1
        );

        let scheduled = kms
            .schedule_key_deletion(
                &scope,
                ScheduleKmsKeyDeletionInput {
                    key_id: key_ref_from_id(&encrypt_key.key_metadata.key_id),
                    pending_window_in_days: Some(7),
                },
            )
            .expect("schedule deletion should succeed");
        assert_eq!(scheduled.pending_window_in_days, 7);
        assert_eq!(scheduled.key_state, "PendingDeletion");
        let described = kms
            .describe_key(
                &scope,
                &key_ref_from_id(&encrypt_key.key_metadata.key_id),
            )
            .expect("describe after schedule should succeed");
        assert_eq!(described.key_metadata.key_state, "PendingDeletion");
        assert!(!described.key_metadata.enabled);
        kms.cancel_key_deletion(
            &scope,
            CancelKmsKeyDeletionInput {
                key_id: key_ref_from_id(&encrypt_key.key_metadata.key_id),
            },
        )
        .expect("cancel deletion should succeed");
        let described = kms
            .describe_key(
                &scope,
                &key_ref_from_id(&encrypt_key.key_metadata.key_id),
            )
            .expect("describe after cancel should succeed");
        assert_eq!(described.key_metadata.key_state, "Enabled");
        assert!(described.key_metadata.enabled);
        assert!(described.key_metadata.deletion_date.is_none());

        kms.delete_alias(
            &scope,
            DeleteKmsAliasInput { alias_name: alias_name("alias/demo") },
        )
        .expect("delete alias should succeed");
        assert!(
            kms.list_aliases(
                &scope,
                ListKmsAliasesInput {
                    key_id: Some(key_ref_from_id(
                        &encrypt_key.key_metadata.key_id,
                    )),
                },
            )
            .expect("aliases should list")
            .aliases
            .is_empty()
        );
    }

    #[test]
    fn kms_negative_paths_return_explicit_errors() {
        let scope = scope();
        let kms = service();
        let encrypt_key = kms
            .create_key(
                &scope,
                CreateKmsKeyInput {
                    description: None,
                    key_spec: None,
                    key_usage: None,
                    tags: Vec::new(),
                },
            )
            .expect("encrypt key should create");
        let sign_key = kms
            .create_key(
                &scope,
                CreateKmsKeyInput {
                    description: None,
                    key_spec: Some("RSA_2048".to_owned()),
                    key_usage: Some(
                        KmsKeyUsage::SignVerify.as_str().to_owned(),
                    ),
                    tags: Vec::new(),
                },
            )
            .expect("sign key should create");

        let missing_key = kms
            .describe_key(&scope, &key_ref("missing"))
            .expect_err("missing key must fail");
        assert_eq!(missing_key.to_aws_error().code(), "NotFoundException");

        let wrong_usage = kms
            .sign(
                &scope,
                KmsSignInput {
                    key_id: key_ref_from_id(&encrypt_key.key_metadata.key_id),
                    message: b"message".to_vec(),
                    message_type: None,
                    signing_algorithm: "RSASSA_PSS_SHA_256".to_owned(),
                },
            )
            .expect_err("encrypt key should not sign");
        assert_eq!(
            wrong_usage.to_aws_error().code(),
            "InvalidKeyUsageException"
        );

        kms.schedule_key_deletion(
            &scope,
            ScheduleKmsKeyDeletionInput {
                key_id: key_ref_from_id(&encrypt_key.key_metadata.key_id),
                pending_window_in_days: None,
            },
        )
        .expect("schedule deletion should succeed");
        let pending_encrypt = kms
            .encrypt(
                &scope,
                super::KmsEncryptInput {
                    key_id: key_ref_from_id(&encrypt_key.key_metadata.key_id),
                    plaintext: b"blocked".to_vec(),
                },
            )
            .expect_err("pending-deletion key should reject crypto");
        assert_eq!(
            pending_encrypt.to_aws_error().code(),
            "KMSInvalidStateException"
        );

        let bad_ciphertext = kms
            .decrypt(
                &scope,
                KmsDecryptInput {
                    ciphertext_blob: b"not-a-ciphertext".to_vec(),
                    key_id: None,
                },
            )
            .expect_err("malformed ciphertext should fail");
        assert_eq!(
            bad_ciphertext.to_aws_error().code(),
            "InvalidCiphertextException"
        );

        let signature = kms
            .sign(
                &scope,
                KmsSignInput {
                    key_id: key_ref_from_id(&sign_key.key_metadata.key_id),
                    message: b"message".to_vec(),
                    message_type: None,
                    signing_algorithm: "RSASSA_PSS_SHA_256".to_owned(),
                },
            )
            .expect("sign should succeed");
        let wrong_signature = kms
            .verify(
                &scope,
                super::KmsVerifyInput {
                    key_id: key_ref_from_id(&sign_key.key_metadata.key_id),
                    message: b"other".to_vec(),
                    message_type: None,
                    signature: signature.signature,
                    signing_algorithm: "RSASSA_PSS_SHA_256".to_owned(),
                },
            )
            .expect_err("wrong signature should fail");
        assert_eq!(
            wrong_signature.to_aws_error().code(),
            "KMSInvalidSignatureException"
        );

        let bad_data_key_spec = kms
            .generate_data_key(
                &scope,
                KmsGenerateDataKeyInput {
                    key_id: key_ref("missing"),
                    key_spec: Some("AES_512".to_owned()),
                    number_of_bytes: None,
                },
            )
            .expect_err("missing key must fail");
        assert_eq!(
            bad_data_key_spec.to_aws_error().code(),
            "NotFoundException"
        );

        let fresh_encrypt_key = kms
            .create_key(
                &scope,
                CreateKmsKeyInput {
                    description: None,
                    key_spec: None,
                    key_usage: None,
                    tags: Vec::new(),
                },
            )
            .expect("fresh encrypt key should create");
        let bad_data_key_spec = kms
            .generate_data_key(
                &scope,
                KmsGenerateDataKeyInput {
                    key_id: key_ref_from_id(
                        &fresh_encrypt_key.key_metadata.key_id,
                    ),
                    key_spec: Some("AES_512".to_owned()),
                    number_of_bytes: None,
                },
            )
            .expect_err("unsupported data key spec must fail");
        assert_eq!(
            bad_data_key_spec.to_aws_error().code(),
            "ValidationException"
        );
    }

    #[test]
    fn kms_helpers_cover_validation_and_codec_edges() {
        let scope = scope();
        assert!(
            super::validate_tags(&[])
                .expect("empty tags should work")
                .is_empty()
        );
        assert_eq!(
            super::format_key_id(&scope, 1).len(),
            36,
            "formatted key ids should use UUID-like formatting"
        );
        assert_eq!(
            super::derived_bytes(&["seed"], 48).len(),
            48,
            "derived bytes should fill requested length"
        );

        let duplicate_tags = vec![
            KmsTag { tag_key: "env".to_owned(), tag_value: "dev".to_owned() },
            KmsTag { tag_key: "env".to_owned(), tag_value: "prod".to_owned() },
        ];
        assert_eq!(
            super::validate_tags(&duplicate_tags)
                .expect_err("duplicate tag keys should fail")
                .to_aws_error()
                .code(),
            "TagException"
        );
        assert_eq!(
            super::MockKmsCodec::decrypt(b"bad")
                .expect_err("bad ciphertext should fail")
                .to_aws_error()
                .code(),
            "InvalidCiphertextException"
        );
        assert_eq!(
            super::MockKmsCodec::decode_signature(b"bad")
                .expect_err("bad signature should fail")
                .to_aws_error()
                .code(),
            "KMSInvalidSignatureException"
        );
        assert_eq!(
            key_ref("arn:aws:kms:eu-west-2:000000000000:alias/demo"),
            KmsKeyReference::AliasArn(
                "arn:aws:kms:eu-west-2:000000000000:alias/demo"
                    .parse()
                    .expect("alias arn should parse")
            )
        );
        assert_eq!(
            key_ref("arn:aws:kms:eu-west-2:000000000000:key/demo"),
            KmsKeyReference::KeyArn(
                "arn:aws:kms:eu-west-2:000000000000:key/demo"
                    .parse()
                    .expect("key arn should parse")
            )
        );
        assert_eq!(
            "arn:aws:s3:::bucket"
                .parse::<KmsKeyReference>()
                .expect_err("non-kms arn should fail")
                .to_string(),
            "KMS key ARN references must target the KMS service."
        );
        assert_eq!(
            "not-alias"
                .parse::<KmsAliasName>()
                .expect_err("plain key ids are invalid aliases")
                .to_string(),
            "KMS alias names must start with the prefix \"alias/\"."
        );
    }
}
