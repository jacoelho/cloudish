use crate::auth::{
    AuthenticationModeInput, ElastiCacheAuthenticationType, UserAuthentication,
};
use crate::scope::ElastiCacheScope;
use crate::{ElastiCacheEngine, ElastiCacheError, ElastiCacheUserId};

use serde::{Deserialize, Serialize};

const DEFAULT_USER_ACCESS_STRING: &str = "on ~* +@all";
const DEFAULT_USER_MINIMUM_ENGINE_VERSION: &str = "6.0";
pub(crate) const REQUESTED_STATUS_ACTIVE: &str = "active";
pub(crate) const REQUESTED_STATUS_DELETING: &str = "deleting";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ElastiCacheUser {
    pub access_string: String,
    pub arn: String,
    pub authentication: UserAuthentication,
    pub engine: ElastiCacheEngine,
    pub minimum_engine_version: String,
    pub status: String,
    pub user_group_ids: Vec<String>,
    pub user_id: ElastiCacheUserId,
    pub user_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateUserInput {
    pub access_string: Option<String>,
    pub authentication_mode: Option<AuthenticationModeInput>,
    pub engine: Option<String>,
    pub no_password_required: Option<bool>,
    pub passwords: Vec<String>,
    pub user_id: ElastiCacheUserId,
    pub user_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModifyUserInput {
    pub access_string: Option<String>,
    pub authentication_mode: Option<AuthenticationModeInput>,
    pub no_password_required: Option<bool>,
    pub passwords: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct StoredElastiCacheUser {
    pub access_string: String,
    pub auth_type: ElastiCacheAuthenticationType,
    pub engine: ElastiCacheEngine,
    pub passwords: Vec<String>,
    pub user_id: ElastiCacheUserId,
    pub user_name: String,
}

impl StoredElastiCacheUser {
    pub(crate) fn matches_password(
        &self,
        username: Option<&str>,
        password: &str,
    ) -> bool {
        if self.passwords.iter().all(|candidate| candidate != password) {
            return false;
        }

        match username {
            Some(username) => self.user_name == username,
            None => true,
        }
    }

    pub(crate) fn to_output(
        &self,
        scope: &ElastiCacheScope,
        status: &str,
    ) -> ElastiCacheUser {
        ElastiCacheUser {
            access_string: self.access_string.clone(),
            arn: format!(
                "arn:aws:elasticache:{}:{}:user:{}",
                scope.region().as_str(),
                scope.account_id().as_str(),
                self.user_id,
            ),
            authentication: UserAuthentication {
                password_count: self.passwords.len(),
                type_: self.auth_type,
            },
            engine: self.engine,
            minimum_engine_version: DEFAULT_USER_MINIMUM_ENGINE_VERSION
                .to_owned(),
            status: status.to_owned(),
            user_group_ids: Vec::new(),
            user_id: self.user_id.clone(),
            user_name: self.user_name.clone(),
        }
    }
}

pub(crate) fn normalized_access_string(value: Option<String>) -> String {
    value
        .filter(|candidate| !candidate.trim().is_empty())
        .unwrap_or_else(|| DEFAULT_USER_ACCESS_STRING.to_owned())
}

pub(crate) fn updated_access_string(
    value: Option<String>,
) -> Result<Option<String>, ElastiCacheError> {
    value
        .map(|access_string| {
            let access_string = access_string.trim();
            if access_string.is_empty() {
                return Err(ElastiCacheError::InvalidParameterValue {
                    message: "AccessString must not be blank.".to_owned(),
                });
            }

            Ok(access_string.to_owned())
        })
        .transpose()
}
