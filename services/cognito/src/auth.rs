use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use crate::{
    errors::CognitoError,
    identifiers::{CognitoUserPoolClientId, CognitoUserPoolId},
    users::{AttributeType, CognitoCodeDeliveryDetails},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum CognitoExplicitAuthFlow {
    AdminNoSrpAuth,
    CustomAuthFlowOnly,
    UserPasswordAuthLegacy,
    AllowAdminUserPasswordAuth,
    AllowCustomAuth,
    AllowRefreshTokenAuth,
    AllowUserAuth,
    AllowUserPasswordAuth,
    AllowUserSrpAuth,
}

impl CognitoExplicitAuthFlow {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::AdminNoSrpAuth => "ADMIN_NO_SRP_AUTH",
            Self::CustomAuthFlowOnly => "CUSTOM_AUTH_FLOW_ONLY",
            Self::UserPasswordAuthLegacy => "USER_PASSWORD_AUTH",
            Self::AllowAdminUserPasswordAuth => {
                "ALLOW_ADMIN_USER_PASSWORD_AUTH"
            }
            Self::AllowCustomAuth => "ALLOW_CUSTOM_AUTH",
            Self::AllowRefreshTokenAuth => "ALLOW_REFRESH_TOKEN_AUTH",
            Self::AllowUserAuth => "ALLOW_USER_AUTH",
            Self::AllowUserPasswordAuth => "ALLOW_USER_PASSWORD_AUTH",
            Self::AllowUserSrpAuth => "ALLOW_USER_SRP_AUTH",
        }
    }

    pub(crate) fn is_legacy(self) -> bool {
        matches!(
            self,
            Self::AdminNoSrpAuth
                | Self::CustomAuthFlowOnly
                | Self::UserPasswordAuthLegacy
        )
    }
}

impl Serialize for CognitoExplicitAuthFlow {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for CognitoExplicitAuthFlow {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;

        match value.as_str() {
            "ADMIN_NO_SRP_AUTH" => Ok(Self::AdminNoSrpAuth),
            "CUSTOM_AUTH_FLOW_ONLY" => Ok(Self::CustomAuthFlowOnly),
            "USER_PASSWORD_AUTH" => Ok(Self::UserPasswordAuthLegacy),
            "ALLOW_ADMIN_USER_PASSWORD_AUTH" => {
                Ok(Self::AllowAdminUserPasswordAuth)
            }
            "ALLOW_CUSTOM_AUTH" => Ok(Self::AllowCustomAuth),
            "ALLOW_REFRESH_TOKEN_AUTH" => Ok(Self::AllowRefreshTokenAuth),
            "ALLOW_USER_AUTH" => Ok(Self::AllowUserAuth),
            "ALLOW_USER_PASSWORD_AUTH" => Ok(Self::AllowUserPasswordAuth),
            "ALLOW_USER_SRP_AUTH" => Ok(Self::AllowUserSrpAuth),
            _ => Err(serde::de::Error::custom(format!(
                "Unsupported ExplicitAuthFlows value `{value}`."
            ))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InitiateAuthInput {
    pub auth_flow: String,
    pub auth_parameters: BTreeMap<String, String>,
    pub client_id: CognitoUserPoolClientId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdminInitiateAuthInput {
    pub auth_flow: String,
    pub auth_parameters: BTreeMap<String, String>,
    pub client_id: CognitoUserPoolClientId,
    pub user_pool_id: CognitoUserPoolId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RespondToAuthChallengeInput {
    pub challenge_name: String,
    pub challenge_responses: BTreeMap<String, String>,
    pub client_id: CognitoUserPoolClientId,
    pub session: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetUserInput {
    pub access_token: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateUserAttributesInput {
    pub access_token: String,
    pub user_attributes: Vec<AttributeType>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangePasswordInput {
    pub access_token: String,
    pub previous_password: String,
    pub proposed_password: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub enum CognitoChallengeName {
    #[serde(rename = "NEW_PASSWORD_REQUIRED")]
    NewPasswordRequired,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct CognitoAuthenticationResult {
    pub access_token: String,
    pub expires_in: u64,
    pub id_token: String,
    pub token_type: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct InitiateAuthOutput {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub authentication_result: Option<CognitoAuthenticationResult>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub challenge_name: Option<CognitoChallengeName>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub challenge_parameters: Option<BTreeMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session: Option<String>,
}

pub type AdminInitiateAuthOutput = InitiateAuthOutput;
pub type RespondToAuthChallengeOutput = InitiateAuthOutput;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct GetUserOutput {
    pub user_attributes: Vec<AttributeType>,
    pub username: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct UpdateUserAttributesOutput {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub code_delivery_details_list: Option<Vec<CognitoCodeDeliveryDetails>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ChangePasswordOutput {}

pub(crate) fn parse_explicit_auth_flows(
    flows: &[CognitoExplicitAuthFlow],
) -> Result<(), CognitoError> {
    let has_legacy =
        flows.iter().copied().any(CognitoExplicitAuthFlow::is_legacy);
    if has_legacy && flows.len() > 1 {
        return Err(CognitoError::invalid_parameter(
            "ExplicitAuthFlows must not mix legacy and ALLOW_* values.",
        ));
    }
    Ok(())
}
