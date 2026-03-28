use aws::{AccountId, Arn, CallerIdentity};
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
