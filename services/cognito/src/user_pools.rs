use serde::{Deserialize, Serialize};

use crate::identifiers::CognitoUserPoolId;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateUserPoolInput {
    pub pool_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct DescribeUserPoolInput {
    pub user_pool_id: CognitoUserPoolId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListUserPoolsInput {
    pub max_results: Option<u32>,
    pub next_token: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateUserPoolInput {
    pub pool_name: Option<String>,
    pub user_pool_id: CognitoUserPoolId,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct DeleteUserPoolInput {
    pub user_pool_id: CognitoUserPoolId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CognitoUserPoolStatus {
    #[serde(rename = "Enabled")]
    Enabled,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct CognitoUserPool {
    #[serde(rename = "Arn")]
    pub arn: String,
    pub creation_date: u64,
    pub id: CognitoUserPoolId,
    pub last_modified_date: u64,
    pub name: String,
    pub status: CognitoUserPoolStatus,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct CognitoUserPoolSummary {
    pub creation_date: u64,
    pub id: CognitoUserPoolId,
    pub last_modified_date: u64,
    pub name: String,
    pub status: CognitoUserPoolStatus,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct CreateUserPoolOutput {
    pub user_pool: CognitoUserPool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct DescribeUserPoolOutput {
    pub user_pool: CognitoUserPool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct ListUserPoolsOutput {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_token: Option<String>,
    pub user_pools: Vec<CognitoUserPoolSummary>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct UpdateUserPoolOutput {
    pub user_pool: CognitoUserPool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DeleteUserPoolOutput {}
