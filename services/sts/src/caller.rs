use crate::StsError;
use aws::{
    AccountId, Arn, ArnResource, CallerIdentity, Partition, ServiceName,
};
use iam::IamTag;
use std::collections::BTreeSet;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CallerIdentityOutput {
    pub account: AccountId,
    pub arn: Arn,
    pub user_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StsCaller {
    account_id: AccountId,
    identity: CallerIdentity,
    session_tags: Vec<IamTag>,
    transitive_tag_keys: BTreeSet<String>,
}

impl StsCaller {
    pub(crate) fn try_root(account_id: AccountId) -> Result<Self, StsError> {
        let identity = CallerIdentity::try_new(
            Arn::new(
                Partition::aws(),
                ServiceName::Iam,
                None,
                Some(account_id.clone()),
                ArnResource::Generic("root".to_owned()),
            )
            .map_err(|error| StsError::Validation {
                message: format!(
                    "failed to construct root caller identity: {error}"
                ),
            })?,
            account_id.to_string(),
        )
        .map_err(|error| StsError::Validation {
            message: format!(
                "failed to construct root caller identity: {error}"
            ),
        })?;

        Ok(Self {
            identity,
            account_id,
            session_tags: Vec::new(),
            transitive_tag_keys: BTreeSet::new(),
        })
    }

    pub fn new(
        account_id: AccountId,
        identity: CallerIdentity,
        session_tags: Vec<IamTag>,
        transitive_tag_keys: BTreeSet<String>,
    ) -> Self {
        Self { account_id, identity, session_tags, transitive_tag_keys }
    }

    pub fn account_id(&self) -> &AccountId {
        &self.account_id
    }

    pub fn arn(&self) -> &Arn {
        self.identity.arn()
    }

    pub fn principal_id(&self) -> &str {
        self.identity.principal_id()
    }

    pub fn identity(&self) -> &CallerIdentity {
        &self.identity
    }

    pub(crate) fn session_tags(&self) -> &[IamTag] {
        &self.session_tags
    }

    pub(crate) fn transitive_tag_keys(&self) -> &BTreeSet<String> {
        &self.transitive_tag_keys
    }
}
