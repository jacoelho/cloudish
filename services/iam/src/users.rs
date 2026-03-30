use aws::Arn;

use crate::documents::IamTag;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IamUser {
    pub arn: Arn,
    pub create_date: String,
    pub path: String,
    pub user_id: String,
    pub user_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateUserInput {
    pub path: String,
    pub tags: Vec<IamTag>,
    pub user_name: String,
}
