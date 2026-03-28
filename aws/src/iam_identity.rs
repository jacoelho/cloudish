use crate::{AccountId, Arn, RegionId};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IamAccessKeyStatus {
    Active,
    Inactive,
    Expired,
}

impl IamAccessKeyStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Active => "Active",
            Self::Inactive => "Inactive",
            Self::Expired => "Expired",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IamAccessKeyRecord {
    pub access_key_id: String,
    pub account_id: AccountId,
    pub create_date: String,
    pub region: RegionId,
    pub secret_access_key: String,
    pub status: IamAccessKeyStatus,
    pub user_arn: Arn,
    pub user_id: String,
    pub user_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IamRoleRecord {
    pub arn: Arn,
    pub create_date: String,
    pub path: String,
    pub role_id: String,
    pub role_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IamResourceTag {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IamInstanceProfileRecord {
    pub account_id: AccountId,
    pub arn: Arn,
    pub create_date: String,
    pub instance_profile_id: String,
    pub instance_profile_name: String,
    pub path: String,
    pub region: RegionId,
    pub role: Option<IamRoleRecord>,
    pub tags: Vec<IamResourceTag>,
}

pub trait IamAccessKeyLookup {
    fn find_access_key(
        &self,
        access_key_id: &str,
    ) -> Option<IamAccessKeyRecord>;
}

pub trait IamInstanceProfileLookup {
    fn find_instance_profile(
        &self,
        account_id: &AccountId,
        region: &RegionId,
        instance_profile_name: &str,
    ) -> Option<IamInstanceProfileRecord>;

    fn find_instance_profiles_for_role(
        &self,
        account_id: &AccountId,
        region: &RegionId,
        role_name: &str,
    ) -> Vec<IamInstanceProfileRecord>;
}

#[cfg(test)]
mod tests {
    use super::{
        IamAccessKeyRecord, IamAccessKeyStatus, IamInstanceProfileRecord,
        IamResourceTag, IamRoleRecord,
    };
    use crate::{AccountId, Arn, RegionId};

    #[test]
    fn iam_access_key_status_strings_are_aws_canonical() {
        assert_eq!(IamAccessKeyStatus::Active.as_str(), "Active");
        assert_eq!(IamAccessKeyStatus::Inactive.as_str(), "Inactive");
        assert_eq!(IamAccessKeyStatus::Expired.as_str(), "Expired");
    }

    #[test]
    fn iam_lookup_records_expose_typed_arns() {
        let access_key = IamAccessKeyRecord {
            access_key_id: "AKIA0000000000000001".to_owned(),
            account_id: "123456789012"
                .parse::<AccountId>()
                .expect("account id should parse"),
            create_date: "2026-03-25T12:00:00Z".to_owned(),
            region: "eu-west-2"
                .parse::<RegionId>()
                .expect("region should parse"),
            secret_access_key: "secret".to_owned(),
            status: IamAccessKeyStatus::Active,
            user_arn: "arn:aws:iam::123456789012:user/alice"
                .parse::<Arn>()
                .expect("user ARN should parse"),
            user_id: "AIDA1234567890EXAMPLE".to_owned(),
            user_name: "alice".to_owned(),
        };
        let profile = IamInstanceProfileRecord {
            account_id: "123456789012"
                .parse::<AccountId>()
                .expect("account id should parse"),
            arn: "arn:aws:iam::123456789012:instance-profile/demo"
                .parse::<Arn>()
                .expect("instance profile ARN should parse"),
            create_date: "2026-03-25T12:00:00Z".to_owned(),
            instance_profile_id: "AIPA1234567890EXAMPLE".to_owned(),
            instance_profile_name: "demo".to_owned(),
            path: "/".to_owned(),
            region: "eu-west-2"
                .parse::<RegionId>()
                .expect("region should parse"),
            role: Some(IamRoleRecord {
                arn: "arn:aws:iam::123456789012:role/demo"
                    .parse::<Arn>()
                    .expect("role ARN should parse"),
                create_date: "2026-03-25T12:00:00Z".to_owned(),
                path: "/".to_owned(),
                role_id: "AROA1234567890EXAMPLE".to_owned(),
                role_name: "demo".to_owned(),
            }),
            tags: vec![IamResourceTag {
                key: "team".to_owned(),
                value: "platform".to_owned(),
            }],
        };

        assert_eq!(
            access_key.user_arn.to_string(),
            "arn:aws:iam::123456789012:user/alice"
        );
        assert_eq!(
            profile.arn.to_string(),
            "arn:aws:iam::123456789012:instance-profile/demo"
        );
        assert_eq!(
            profile.role.expect("role should be present").arn.to_string(),
            "arn:aws:iam::123456789012:role/demo"
        );
    }
}
