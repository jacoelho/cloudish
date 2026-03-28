use aws::Arn;

use crate::{documents::IamTag, roles::IamRole};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IamInstanceProfile {
    pub arn: Arn,
    pub create_date: String,
    pub instance_profile_id: String,
    pub instance_profile_name: String,
    pub path: String,
    pub roles: Vec<IamRole>,
    pub tags: Vec<IamTag>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateInstanceProfileInput {
    pub instance_profile_name: String,
    pub path: String,
    pub tags: Vec<IamTag>,
}
