mod assume_role;
mod caller;
mod credentials;
mod federation;
mod trust_policy;
mod validation;

#[cfg(test)]
mod tests;

use crate::assume_role::{access_denied, assumed_role_identity};
use crate::credentials::{
    SessionIssueInput, StsWorld, find_session_credential, issue_session,
};
use crate::federation::{
    default_web_identity_provider, extract_saml_role_session_name,
    federated_user_identity,
};
use crate::trust_policy::trust_policy_allows;
use crate::validation::{
    ASSUME_ROLE_DURATION, FEDERATION_TOKEN_DURATION, SESSION_TOKEN_DURATION,
    normalize_duration, parse_role_arn, session_state_for_assume_role,
    validate_federation_name, validate_saml_principal_arn,
    validate_session_name, validate_session_tags,
};
#[cfg(test)]
use aws::{AwsError, AwsErrorFamily};
use aws::{SessionCredentialLookup, SessionCredentialRecord};
use iam::{IamScope, IamService};
use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use std::time::SystemTime;
use thiserror::Error;

pub use crate::assume_role::{AssumeRoleInput, AssumeRoleOutput};
pub use crate::caller::{CallerIdentityOutput, StsCaller};
pub use crate::credentials::{
    AssumedRoleUser, FederatedUser, SessionCredentials,
};
pub use crate::federation::{
    AssumeRoleWithSamlInput, AssumeRoleWithSamlOutput,
    AssumeRoleWithWebIdentityInput, AssumeRoleWithWebIdentityOutput,
    GetFederationTokenInput, GetFederationTokenOutput, GetSessionTokenInput,
};

#[derive(Clone)]
pub struct StsService {
    iam: IamService,
    state: Arc<Mutex<StsWorld>>,
    time_source: Arc<dyn Fn() -> SystemTime + Send + Sync>,
}

impl StsService {
    /// Creates a new STS service backed by the supplied IAM service and the
    /// system clock.
    pub fn new(iam: IamService) -> Self {
        Self::with_time_source(iam, Arc::new(SystemTime::now))
    }

    pub fn with_time_source(
        iam: IamService,
        time_source: Arc<dyn Fn() -> SystemTime + Send + Sync>,
    ) -> Self {
        Self { iam, state: Arc::default(), time_source }
    }

    /// Issues credentials for a role session after validating the caller,
    /// role ARN, requested tags, and duration.
    ///
    /// # Errors
    ///
    /// Returns `StsError` when the request is invalid, the target role does
    /// not allow the caller to assume it, or Cloudish cannot shape the
    /// synthetic session identity.
    pub fn assume_role(
        &self,
        scope: &IamScope,
        caller: Option<&StsCaller>,
        input: AssumeRoleInput,
    ) -> Result<AssumeRoleOutput, StsError> {
        let caller = Self::caller_or_root(scope, caller)?;
        validate_session_name(&input.role_session_name)?;
        validate_session_tags(
            &input.tags,
            &input.transitive_tag_keys,
            &caller,
        )?;

        let role_ref = parse_role_arn(&input.role_arn)?;
        let role_scope =
            IamScope::new(role_ref.account_id.clone(), scope.region().clone());
        let role = self
            .iam
            .get_role(&role_scope, &role_ref.role_name)
            .map_err(|_| access_denied(&caller, &input.role_arn))?;
        if !trust_policy_allows(&role, &caller) {
            return Err(access_denied(&caller, &input.role_arn));
        }

        let duration_seconds = normalize_duration(
            input.duration_seconds,
            ASSUME_ROLE_DURATION.with_max(role.max_session_duration),
        )?;
        let (session_tags, transitive_tag_keys) =
            session_state_for_assume_role(
                &caller,
                &input.tags,
                &input.transitive_tag_keys,
            );
        let assumed_role = assumed_role_identity(
            &role_ref.account_id,
            &role.role_name,
            &role.role_id,
            &input.role_session_name,
        )?;
        let credentials = issue_session(
            &self.state,
            &*self.time_source,
            SessionIssueInput {
                account_id: role_ref.account_id,
                duration_seconds,
                principal_arn: assumed_role.user.arn.clone(),
                principal_id: assumed_role.principal_id.clone(),
                session_tags,
                transitive_tag_keys,
            },
        )?;

        Ok(AssumeRoleOutput {
            assumed_role_user: assumed_role.user,
            credentials,
            packed_policy_size: 0,
        })
    }

    /// Issues credentials for a web-identity assume-role request.
    ///
    /// # Errors
    ///
    /// Returns `StsError` when the session name, token, role ARN, requested
    /// duration, or synthetic session identity is invalid.
    pub fn assume_role_with_web_identity(
        &self,
        scope: &IamScope,
        input: AssumeRoleWithWebIdentityInput,
    ) -> Result<AssumeRoleWithWebIdentityOutput, StsError> {
        validate_session_name(&input.role_session_name)?;
        if input.web_identity_token.trim().is_empty() {
            return Err(StsError::Validation {
                message:
                    "The request must contain the parameter WebIdentityToken."
                        .to_owned(),
            });
        }

        let role_ref = parse_role_arn(&input.role_arn)?;
        let role_scope =
            IamScope::new(role_ref.account_id.clone(), scope.region().clone());
        let role = self
            .iam
            .get_role(&role_scope, &role_ref.role_name)
            .map_err(|_| StsError::Validation {
                message: format!("{} is invalid", input.role_arn),
            })?;
        let duration_seconds = normalize_duration(
            input.duration_seconds,
            ASSUME_ROLE_DURATION.with_max(role.max_session_duration),
        )?;
        let assumed_role = assumed_role_identity(
            &role_ref.account_id,
            &role.role_name,
            &role.role_id,
            &input.role_session_name,
        )?;
        let credentials = issue_session(
            &self.state,
            &*self.time_source,
            SessionIssueInput {
                account_id: role_ref.account_id,
                duration_seconds,
                principal_arn: assumed_role.user.arn.clone(),
                principal_id: assumed_role.principal_id.clone(),
                session_tags: Vec::new(),
                transitive_tag_keys: BTreeSet::new(),
            },
        )?;

        Ok(AssumeRoleWithWebIdentityOutput {
            assumed_role_user: assumed_role.user,
            audience: "sts.amazonaws.com".to_owned(),
            credentials,
            packed_policy_size: 0,
            provider: default_web_identity_provider(input.provider_id),
            subject_from_web_identity_token: "web-identity-subject".to_owned(),
        })
    }

    /// Issues credentials for a SAML-backed assume-role request.
    ///
    /// # Errors
    ///
    /// Returns `StsError` when the role ARN, SAML principal ARN, session
    /// name, requested duration, or synthetic session identity is invalid.
    pub fn assume_role_with_saml(
        &self,
        scope: &IamScope,
        input: AssumeRoleWithSamlInput,
    ) -> Result<AssumeRoleWithSamlOutput, StsError> {
        let role_ref = parse_role_arn(&input.role_arn)?;
        validate_saml_principal_arn(&input.principal_arn)?;
        let session_name =
            extract_saml_role_session_name(&input.saml_assertion)
                .unwrap_or_else(|| "saml-session".to_owned());
        validate_session_name(&session_name)?;

        let role_scope =
            IamScope::new(role_ref.account_id.clone(), scope.region().clone());
        let role = self
            .iam
            .get_role(&role_scope, &role_ref.role_name)
            .map_err(|_| StsError::Validation {
                message: format!("{} is invalid", input.role_arn),
            })?;
        let duration_seconds = normalize_duration(
            input.duration_seconds,
            ASSUME_ROLE_DURATION.with_max(role.max_session_duration),
        )?;
        let assumed_role = assumed_role_identity(
            &role_ref.account_id,
            &role.role_name,
            &role.role_id,
            &session_name,
        )?;
        let credentials = issue_session(
            &self.state,
            &*self.time_source,
            SessionIssueInput {
                account_id: role_ref.account_id,
                duration_seconds,
                principal_arn: assumed_role.user.arn.clone(),
                principal_id: assumed_role.principal_id.clone(),
                session_tags: Vec::new(),
                transitive_tag_keys: BTreeSet::new(),
            },
        )?;

        Ok(AssumeRoleWithSamlOutput {
            assumed_role_user: assumed_role.user,
            audience: "urn:amazon:webservices".to_owned(),
            credentials,
            issuer: "https://saml.example.com".to_owned(),
            name_qualifier: "saml-qualifier".to_owned(),
            packed_policy_size: 0,
            subject: "saml-subject".to_owned(),
            subject_type: "persistent".to_owned(),
        })
    }

    /// Returns the current caller identity or a synthetic root identity when
    /// the request is unsigned.
    ///
    /// # Errors
    ///
    /// Returns `StsError` if Cloudish cannot construct the synthetic root
    /// caller identity.
    pub fn get_caller_identity(
        &self,
        scope: &IamScope,
        caller: Option<&StsCaller>,
    ) -> Result<CallerIdentityOutput, StsError> {
        let caller = Self::caller_or_root(scope, caller)?;
        Ok(CallerIdentityOutput {
            account: caller.account_id().clone(),
            arn: caller.arn().clone(),
            user_id: caller.principal_id().to_owned(),
        })
    }

    /// Issues temporary session credentials for the current caller.
    ///
    /// # Errors
    ///
    /// Returns `StsError` when the requested duration is outside the allowed
    /// session-token bounds or the current time source cannot be represented.
    pub fn get_session_token(
        &self,
        scope: &IamScope,
        caller: Option<&StsCaller>,
        input: GetSessionTokenInput,
    ) -> Result<SessionCredentials, StsError> {
        let caller = Self::caller_or_root(scope, caller)?;
        let duration_seconds = normalize_duration(
            input.duration_seconds,
            SESSION_TOKEN_DURATION,
        )?;

        issue_session(
            &self.state,
            &*self.time_source,
            SessionIssueInput {
                account_id: caller.account_id().clone(),
                duration_seconds,
                principal_arn: caller.arn().clone(),
                principal_id: caller.principal_id().to_owned(),
                session_tags: Vec::new(),
                transitive_tag_keys: BTreeSet::new(),
            },
        )
    }

    /// Issues credentials for a federated user session.
    ///
    /// # Errors
    ///
    /// Returns `StsError` when the federated user name or requested duration
    /// is invalid, or when the session expiration cannot be represented.
    pub fn get_federation_token(
        &self,
        scope: &IamScope,
        caller: Option<&StsCaller>,
        input: GetFederationTokenInput,
    ) -> Result<GetFederationTokenOutput, StsError> {
        let caller = Self::caller_or_root(scope, caller)?;
        validate_federation_name(&input.name)?;
        let duration_seconds = normalize_duration(
            input.duration_seconds,
            FEDERATION_TOKEN_DURATION,
        )?;
        let federated_user =
            federated_user_identity(caller.account_id(), &input.name)?;
        let credentials = issue_session(
            &self.state,
            &*self.time_source,
            SessionIssueInput {
                account_id: caller.account_id().clone(),
                duration_seconds,
                principal_arn: federated_user.user.arn.clone(),
                principal_id: federated_user.principal_id.clone(),
                session_tags: Vec::new(),
                transitive_tag_keys: BTreeSet::new(),
            },
        )?;

        Ok(GetFederationTokenOutput {
            credentials,
            federated_user: federated_user.user,
            packed_policy_size: 0,
        })
    }

    fn caller_or_root(
        scope: &IamScope,
        caller: Option<&StsCaller>,
    ) -> Result<StsCaller, StsError> {
        caller
            .cloned()
            .map(Ok)
            .unwrap_or_else(|| StsCaller::try_root(scope.account_id().clone()))
    }
}

impl SessionCredentialLookup for StsService {
    fn find_session_credential(
        &self,
        access_key_id: &str,
    ) -> Option<SessionCredentialRecord> {
        find_session_credential(&self.state, access_key_id)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum StsError {
    #[error("{message}")]
    AccessDenied { message: String },
    #[error("{message}")]
    InvalidAuthorizationMessage { message: String },
    #[error("{message}")]
    InvalidParameterValue { message: String },
    #[error("{message}")]
    Validation { message: String },
}

impl StsError {
    #[cfg(test)]
    fn to_aws_error(&self) -> AwsError {
        match self {
            Self::AccessDenied { message } => AwsError::custom(
                AwsErrorFamily::AccessDenied,
                "AccessDenied",
                message,
                403,
                true,
            ),
            Self::InvalidAuthorizationMessage { message } => AwsError::custom(
                AwsErrorFamily::Validation,
                "InvalidAuthorizationMessageException",
                message,
                400,
                true,
            ),
            Self::InvalidParameterValue { message } => AwsError::custom(
                AwsErrorFamily::Validation,
                "InvalidParameterValue",
                message,
                400,
                true,
            ),
            Self::Validation { message } => AwsError::custom(
                AwsErrorFamily::Validation,
                "ValidationError",
                message,
                400,
                true,
            ),
        }
        .expect("STS error shapes must be valid")
    }
}
