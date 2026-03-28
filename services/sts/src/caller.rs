use aws::{
    AccountId, Arn, CallerCredentialKind, CallerIdentity, StableAwsPrincipal,
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
    credential_kind: CallerCredentialKind,
    identity: CallerIdentity,
    session_tags: Vec<IamTag>,
    transitive_tag_keys: BTreeSet<String>,
}

impl StsCaller {
    pub fn new(
        account_id: AccountId,
        credential_kind: CallerCredentialKind,
        identity: CallerIdentity,
        session_tags: Vec<IamTag>,
        transitive_tag_keys: BTreeSet<String>,
    ) -> Self {
        Self {
            account_id,
            credential_kind,
            identity,
            session_tags,
            transitive_tag_keys,
        }
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

    pub fn credential_kind(&self) -> &CallerCredentialKind {
        &self.credential_kind
    }

    pub fn uses_long_term_credentials(&self) -> bool {
        self.credential_kind.uses_long_term_credentials()
    }

    pub fn canonical_trust_principal(&self) -> StableAwsPrincipal {
        self.credential_kind.canonical_trust_principal()
    }

    pub fn role_session_name(&self) -> Option<&str> {
        self.credential_kind.role_session_name()
    }

    pub(crate) fn session_tags(&self) -> &[IamTag] {
        &self.session_tags
    }

    pub(crate) fn transitive_tag_keys(&self) -> &BTreeSet<String> {
        &self.transitive_tag_keys
    }
}
