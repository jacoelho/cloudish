use aws::IamAccessKeyStatus;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IamAccessKey {
    pub access_key_id: String,
    pub create_date: String,
    pub secret_access_key: String,
    pub status: IamAccessKeyStatus,
    pub user_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IamAccessKeyMetadata {
    pub access_key_id: String,
    pub create_date: String,
    pub status: IamAccessKeyStatus,
    pub user_name: String,
}
