use crate::{
    StsError,
    credentials::{FederatedUser, SessionCredentials},
};
use aws::{AccountId, Arn, ArnResource, Partition, ServiceName};
use base64::{Engine as _, engine::general_purpose::STANDARD};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssumeRoleWithWebIdentityInput {
    pub duration_seconds: Option<u32>,
    pub provider_id: Option<String>,
    pub role_arn: Arn,
    pub role_session_name: String,
    pub web_identity_token: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssumeRoleWithWebIdentityOutput {
    pub assumed_role_user: crate::AssumedRoleUser,
    pub audience: String,
    pub credentials: SessionCredentials,
    pub packed_policy_size: u32,
    pub provider: String,
    pub subject_from_web_identity_token: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssumeRoleWithSamlInput {
    pub duration_seconds: Option<u32>,
    pub principal_arn: Arn,
    pub role_arn: Arn,
    pub saml_assertion: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssumeRoleWithSamlOutput {
    pub assumed_role_user: crate::AssumedRoleUser,
    pub audience: String,
    pub credentials: SessionCredentials,
    pub issuer: String,
    pub name_qualifier: String,
    pub packed_policy_size: u32,
    pub subject: String,
    pub subject_type: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetSessionTokenInput {
    pub duration_seconds: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetFederationTokenInput {
    pub duration_seconds: Option<u32>,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetFederationTokenOutput {
    pub credentials: SessionCredentials,
    pub federated_user: FederatedUser,
    pub packed_policy_size: u32,
}

pub(crate) struct FederatedUserIdentity {
    pub(crate) user: FederatedUser,
    pub(crate) principal_id: String,
}

pub(crate) fn default_web_identity_provider(
    provider_id: Option<String>,
) -> String {
    provider_id
        .filter(|provider| !provider.trim().is_empty())
        .unwrap_or_else(|| "accounts.google.com".to_owned())
}

pub(crate) fn extract_saml_role_session_name(
    assertion: &str,
) -> Option<String> {
    let decoded = STANDARD.decode(assertion).ok()?;
    let decoded = String::from_utf8(decoded).ok()?;
    let marker = "https://aws.amazon.com/SAML/Attributes/RoleSessionName";
    let marker_index = decoded.find(marker)?;
    let value_start = decoded[marker_index..].find("<AttributeValue>")?
        + marker_index
        + "<AttributeValue>".len();
    let value_end =
        decoded[value_start..].find("</AttributeValue>")? + value_start;

    Some(decoded[value_start..value_end].trim().to_owned())
}

pub(crate) fn federated_user_identity(
    account_id: &AccountId,
    name: &str,
) -> Result<FederatedUserIdentity, StsError> {
    let federated_user_id = format!("{account_id}:{name}");
    let federated_user_arn = Arn::new(
        Partition::aws(),
        ServiceName::Sts,
        None,
        Some(account_id.clone()),
        ArnResource::Generic(format!("federated-user/{name}")),
    )
    .map_err(|error| StsError::Validation {
        message: format!(
            "failed to construct federated-user identity: {error}"
        ),
    })?;

    Ok(FederatedUserIdentity {
        user: FederatedUser {
            arn: federated_user_arn,
            federated_user_id: federated_user_id.clone(),
        },
        principal_id: federated_user_id,
    })
}
