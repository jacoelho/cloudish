use aws::Arn;

use crate::users::IamUser;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IamGroup {
    pub arn: Arn,
    pub create_date: String,
    pub group_id: String,
    pub group_name: String,
    pub path: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupDetails {
    pub group: IamGroup,
    pub users: Vec<IamUser>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateGroupInput {
    pub group_name: String,
    pub path: String,
}
