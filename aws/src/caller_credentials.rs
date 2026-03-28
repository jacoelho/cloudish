use crate::Arn;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AwsPrincipalType {
    Account,
    FederatedUser,
    Role,
    User,
}

impl AwsPrincipalType {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Account => "Account",
            Self::FederatedUser => "FederatedUser",
            Self::Role => "Role",
            Self::User => "User",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StableAwsPrincipal {
    arn: Arn,
    principal_type: AwsPrincipalType,
    username: Option<String>,
}

impl StableAwsPrincipal {
    pub fn new(
        arn: Arn,
        principal_type: AwsPrincipalType,
        username: Option<String>,
    ) -> Self {
        Self { arn, principal_type, username }
    }

    pub fn arn(&self) -> &Arn {
        &self.arn
    }

    pub fn principal_type(&self) -> AwsPrincipalType {
        self.principal_type
    }

    pub fn username(&self) -> Option<&str> {
        self.username.as_deref()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CallerCredentialKind {
    LongTerm(StableAwsPrincipal),
    Temporary(TemporaryCredentialKind),
}

impl CallerCredentialKind {
    pub fn uses_long_term_credentials(&self) -> bool {
        matches!(self, Self::LongTerm(_))
    }

    pub fn canonical_trust_principal(&self) -> StableAwsPrincipal {
        match self {
            Self::LongTerm(principal) => principal.clone(),
            Self::Temporary(temporary) => {
                temporary.canonical_trust_principal()
            }
        }
    }

    pub fn role_session_name(&self) -> Option<&str> {
        match self {
            Self::LongTerm(_) => None,
            Self::Temporary(temporary) => temporary.role_session_name(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TemporaryCredentialKind {
    AssumedRole {
        role_arn: Arn,
        role_session_name: String,
    },
    AssumedRoleWithSaml {
        principal_arn: Arn,
        role_arn: Arn,
        role_session_name: String,
    },
    AssumedRoleWithWebIdentity {
        provider: String,
        role_arn: Arn,
        role_session_name: String,
    },
    FederationToken {
        federated_user_arn: Arn,
        federated_user_name: String,
    },
    SessionToken {
        source_principal: StableAwsPrincipal,
    },
}

impl TemporaryCredentialKind {
    pub fn canonical_trust_principal(&self) -> StableAwsPrincipal {
        match self {
            Self::AssumedRole { role_arn, .. }
            | Self::AssumedRoleWithSaml { role_arn, .. }
            | Self::AssumedRoleWithWebIdentity { role_arn, .. } => {
                StableAwsPrincipal::new(
                    role_arn.clone(),
                    AwsPrincipalType::Role,
                    None,
                )
            }
            Self::FederationToken {
                federated_user_arn,
                federated_user_name,
            } => StableAwsPrincipal::new(
                federated_user_arn.clone(),
                AwsPrincipalType::FederatedUser,
                Some(federated_user_name.clone()),
            ),
            Self::SessionToken { source_principal } => {
                source_principal.clone()
            }
        }
    }

    pub fn role_session_name(&self) -> Option<&str> {
        match self {
            Self::AssumedRole { role_session_name, .. }
            | Self::AssumedRoleWithSaml { role_session_name, .. }
            | Self::AssumedRoleWithWebIdentity { role_session_name, .. } => {
                Some(role_session_name)
            }
            Self::FederationToken { .. } | Self::SessionToken { .. } => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        AwsPrincipalType, CallerCredentialKind, StableAwsPrincipal,
        TemporaryCredentialKind,
    };
    use crate::Arn;

    fn user_principal() -> StableAwsPrincipal {
        StableAwsPrincipal::new(
            "arn:aws:iam::123456789012:user/alice"
                .parse::<Arn>()
                .expect("user ARN should parse"),
            AwsPrincipalType::User,
            Some("alice".to_owned()),
        )
    }

    #[test]
    fn long_term_credentials_report_their_canonical_principal() {
        let credentials = CallerCredentialKind::LongTerm(user_principal());

        assert!(credentials.uses_long_term_credentials());
        assert_eq!(
            credentials.canonical_trust_principal().arn().to_string(),
            "arn:aws:iam::123456789012:user/alice"
        );
        assert_eq!(
            credentials.canonical_trust_principal().principal_type().as_str(),
            "User"
        );
        assert_eq!(
            credentials.canonical_trust_principal().username(),
            Some("alice")
        );
    }

    #[test]
    fn temporary_credentials_map_assumed_roles_back_to_the_role_arn() {
        let credentials = CallerCredentialKind::Temporary(
            TemporaryCredentialKind::AssumedRole {
                role_arn: "arn:aws:iam::123456789012:role/demo"
                    .parse::<Arn>()
                    .expect("role ARN should parse"),
                role_session_name: "session".to_owned(),
            },
        );

        assert!(!credentials.uses_long_term_credentials());
        assert_eq!(
            credentials.canonical_trust_principal().arn().to_string(),
            "arn:aws:iam::123456789012:role/demo"
        );
        assert_eq!(credentials.role_session_name(), Some("session"));
    }

    #[test]
    fn session_tokens_reuse_the_source_principal_for_trust() {
        let credentials = CallerCredentialKind::Temporary(
            TemporaryCredentialKind::SessionToken {
                source_principal: user_principal(),
            },
        );

        assert_eq!(
            credentials.canonical_trust_principal().arn().to_string(),
            "arn:aws:iam::123456789012:user/alice"
        );
        assert_eq!(
            credentials.canonical_trust_principal().username(),
            Some("alice")
        );
        assert_eq!(credentials.role_session_name(), None);
    }
}
