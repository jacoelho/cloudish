use aws::Arn;

use crate::documents::IamTag;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IamRole {
    pub arn: Arn,
    pub assume_role_policy_document: String,
    pub create_date: String,
    pub description: String,
    pub max_session_duration: u32,
    pub path: String,
    pub role_id: String,
    pub role_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateRoleInput {
    pub assume_role_policy_document: String,
    pub description: String,
    pub max_session_duration: u32,
    pub path: String,
    pub role_name: String,
    pub tags: Vec<IamTag>,
}
