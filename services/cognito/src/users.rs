use serde::{Deserialize, Serialize};

use crate::identifiers::{CognitoUserPoolClientId, CognitoUserPoolId};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct AttributeType {
    pub name: String,
    pub value: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdminCreateUserInput {
    pub temporary_password: Option<String>,
    pub user_attributes: Vec<AttributeType>,
    pub user_pool_id: CognitoUserPoolId,
    pub username: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct AdminGetUserInput {
    pub user_pool_id: CognitoUserPoolId,
    pub username: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListUsersInput {
    pub limit: Option<u32>,
    pub pagination_token: Option<String>,
    pub user_pool_id: CognitoUserPoolId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdminUpdateUserAttributesInput {
    pub user_attributes: Vec<AttributeType>,
    pub user_pool_id: CognitoUserPoolId,
    pub username: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct AdminDeleteUserInput {
    pub user_pool_id: CognitoUserPoolId,
    pub username: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct AdminSetUserPasswordInput {
    pub password: String,
    pub permanent: bool,
    pub user_pool_id: CognitoUserPoolId,
    pub username: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignUpInput {
    pub client_id: CognitoUserPoolClientId,
    pub password: String,
    pub user_attributes: Vec<AttributeType>,
    pub username: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfirmSignUpInput {
    pub client_id: CognitoUserPoolClientId,
    pub confirmation_code: String,
    pub username: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CognitoUserStatus {
    #[serde(rename = "CONFIRMED")]
    Confirmed,
    #[serde(rename = "FORCE_CHANGE_PASSWORD")]
    ForceChangePassword,
    #[serde(rename = "UNCONFIRMED")]
    Unconfirmed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct CognitoUser {
    pub attributes: Vec<AttributeType>,
    pub enabled: bool,
    pub user_create_date: u64,
    pub user_last_modified_date: u64,
    pub username: String,
    pub user_status: CognitoUserStatus,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct AdminGetUserOutput {
    pub enabled: bool,
    pub user_attributes: Vec<AttributeType>,
    pub user_create_date: u64,
    pub user_last_modified_date: u64,
    pub username: String,
    pub user_status: CognitoUserStatus,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct AdminCreateUserOutput {
    pub user: CognitoUser,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct ListUsersOutput {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pagination_token: Option<String>,
    pub users: Vec<CognitoUser>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AdminUpdateUserAttributesOutput {}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AdminDeleteUserOutput {}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AdminSetUserPasswordOutput {}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct CognitoCodeDeliveryDetails {
    pub attribute_name: String,
    pub delivery_medium: String,
    pub destination: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct SignUpOutput {
    pub code_delivery_details: CognitoCodeDeliveryDetails,
    pub user_confirmed: bool,
    pub user_sub: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ConfirmSignUpOutput {}
