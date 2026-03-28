use crate::scope::KmsScope;
use aws::{AccountId, Arn, KmsAliasName, KmsKeyId, KmsKeyReference, RegionId};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateKmsAliasInput {
    pub alias_name: KmsAliasName,
    pub target_key_id: KmsKeyReference,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteKmsAliasInput {
    pub alias_name: KmsAliasName,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListKmsAliasesInput {
    pub key_id: Option<KmsKeyReference>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct ListKmsAliasesOutput {
    pub aliases: Vec<KmsAliasEntry>,
    pub truncated: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct KmsAliasEntry {
    #[serde(rename = "AliasArn")]
    pub alias_arn: Arn,
    pub alias_name: KmsAliasName,
    pub creation_date: u64,
    pub target_key_id: KmsKeyId,
}

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
pub(crate) struct KmsAliasStorageKey {
    pub account_id: AccountId,
    pub alias_name: String,
    pub region: RegionId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct StoredKmsAlias {
    pub alias_arn: Arn,
    pub alias_name: KmsAliasName,
    pub creation_date: u64,
    pub target_key_id: KmsKeyId,
}

pub(crate) fn alias_storage_key(
    scope: &KmsScope,
    alias_name: &str,
) -> KmsAliasStorageKey {
    KmsAliasStorageKey {
        account_id: scope.account_id().clone(),
        alias_name: alias_name.to_owned(),
        region: scope.region().clone(),
    }
}

pub(crate) fn alias_scope_filter(
    scope: &KmsScope,
) -> impl Fn(&KmsAliasStorageKey) -> bool + Send + Sync + 'static {
    let account_id = scope.account_id().clone();
    let region = scope.region().clone();

    move |key| key.account_id == account_id && key.region == region
}
