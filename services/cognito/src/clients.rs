use serde::{Deserialize, Serialize};

use crate::{
    auth::CognitoExplicitAuthFlow,
    identifiers::{CognitoUserPoolClientId, CognitoUserPoolId},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateUserPoolClientInput {
    pub client_name: String,
    pub explicit_auth_flows: Option<Vec<CognitoExplicitAuthFlow>>,
    pub generate_secret: Option<bool>,
    pub user_pool_id: CognitoUserPoolId,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct DescribeUserPoolClientInput {
    pub client_id: CognitoUserPoolClientId,
    pub user_pool_id: CognitoUserPoolId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListUserPoolClientsInput {
    pub max_results: Option<u32>,
    pub next_token: Option<String>,
    pub user_pool_id: CognitoUserPoolId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateUserPoolClientInput {
    pub client_id: CognitoUserPoolClientId,
    pub client_name: Option<String>,
    pub explicit_auth_flows: Option<Vec<CognitoExplicitAuthFlow>>,
    pub user_pool_id: CognitoUserPoolId,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct DeleteUserPoolClientInput {
    pub client_id: CognitoUserPoolClientId,
    pub user_pool_id: CognitoUserPoolId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct CognitoUserPoolClient {
    pub client_id: CognitoUserPoolClientId,
    pub client_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_secret: Option<String>,
    pub creation_date: u64,
    pub explicit_auth_flows: Vec<CognitoExplicitAuthFlow>,
    pub last_modified_date: u64,
    pub user_pool_id: CognitoUserPoolId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct CognitoUserPoolClientDescription {
    pub client_id: CognitoUserPoolClientId,
    pub client_name: String,
    pub user_pool_id: CognitoUserPoolId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct CreateUserPoolClientOutput {
    pub user_pool_client: CognitoUserPoolClient,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct DescribeUserPoolClientOutput {
    pub user_pool_client: CognitoUserPoolClient,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct ListUserPoolClientsOutput {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_token: Option<String>,
    pub user_pool_clients: Vec<CognitoUserPoolClientDescription>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct UpdateUserPoolClientOutput {
    pub user_pool_client: CognitoUserPoolClient,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DeleteUserPoolClientOutput {}
