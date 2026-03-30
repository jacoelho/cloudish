use aws::Arn;

use crate::documents::IamTag;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IamPolicy {
    pub arn: Arn,
    pub attachment_count: usize,
    pub create_date: String,
    pub default_version_id: String,
    pub is_attachable: bool,
    pub path: String,
    pub policy_id: String,
    pub policy_name: String,
    pub update_date: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IamPolicyVersion {
    pub create_date: String,
    pub document: String,
    pub is_default_version: bool,
    pub version_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttachedPolicy {
    pub policy_arn: Arn,
    pub policy_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreatePolicyInput {
    pub description: String,
    pub path: String,
    pub policy_document: String,
    pub policy_name: String,
    pub tags: Vec<IamTag>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreatePolicyVersionInput {
    pub policy_arn: Arn,
    pub policy_document: String,
    pub set_as_default: bool,
}
