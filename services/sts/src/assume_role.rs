use crate::{StsError, caller::StsCaller, credentials::AssumedRoleUser};
use aws::{AccountId, Arn, ArnResource, Partition, ServiceName};
use iam::IamTag;
use std::fmt::Display;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssumeRoleInput {
    pub duration_seconds: Option<u32>,
    pub role_arn: Arn,
    pub role_session_name: String,
    pub tags: Vec<IamTag>,
    pub transitive_tag_keys: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssumeRoleOutput {
    pub assumed_role_user: AssumedRoleUser,
    pub credentials: crate::SessionCredentials,
    pub packed_policy_size: u32,
}

pub(crate) struct AssumedRoleIdentity {
    pub(crate) user: AssumedRoleUser,
    pub(crate) principal_id: String,
}

pub(crate) fn assumed_role_identity(
    account_id: &AccountId,
    role_name: &str,
    role_id: &str,
    session_name: &str,
) -> Result<AssumedRoleIdentity, StsError> {
    let assumed_role_id = format!("{role_id}:{session_name}");
    let assumed_role_arn = Arn::new(
        Partition::aws(),
        ServiceName::Sts,
        None,
        Some(account_id.clone()),
        ArnResource::Generic(format!(
            "assumed-role/{role_name}/{session_name}"
        )),
    )
    .map_err(|error| StsError::Validation {
        message: format!("failed to construct assumed-role identity: {error}"),
    })?;

    Ok(AssumedRoleIdentity {
        user: AssumedRoleUser {
            arn: assumed_role_arn,
            assumed_role_id: assumed_role_id.clone(),
        },
        principal_id: assumed_role_id,
    })
}

pub(crate) fn access_denied(caller: &StsCaller, role_arn: &Arn) -> StsError {
    access_denied_for(caller.arn(), "sts:AssumeRole", role_arn)
}

pub(crate) fn access_denied_for(
    principal: impl Display,
    action: &str,
    role_arn: &Arn,
) -> StsError {
    StsError::AccessDenied {
        message: format!(
            "User: {principal} is not authorized to perform: {action} on resource: {role_arn}",
        ),
    }
}
