use crate::errors::RdsError;
use crate::scope::RdsScope;
use crate::{
    DbClusterIdentifier, DbInstanceIdentifier, DbParameterGroupName,
    StoredRdsState,
};
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RdsEngine {
    Postgres,
    AuroraPostgresql,
    Mysql,
    Aurora,
    AuroraMysql,
    MariaDb,
}

impl RdsEngine {
    /// # Errors
    ///
    /// Returns [`RdsError::InvalidParameterValue`] when the engine name is not
    /// supported by this emulator.
    pub fn parse(value: &str) -> Result<Self, RdsError> {
        match value.trim().to_ascii_lowercase().as_str() {
            "postgres" => Ok(Self::Postgres),
            "aurora-postgresql" => Ok(Self::AuroraPostgresql),
            "mysql" => Ok(Self::Mysql),
            "aurora" => Ok(Self::Aurora),
            "aurora-mysql" => Ok(Self::AuroraMysql),
            "mariadb" => Ok(Self::MariaDb),
            other => Err(RdsError::InvalidParameterValue {
                message: format!(
                    "Unsupported RDS engine `{other}`. Supported engines are \
                     postgres, aurora-postgresql, mysql, aurora, \
                     aurora-mysql, and mariadb."
                ),
            }),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Postgres => "postgres",
            Self::AuroraPostgresql => "aurora-postgresql",
            Self::Mysql => "mysql",
            Self::Aurora => "aurora",
            Self::AuroraMysql => "aurora-mysql",
            Self::MariaDb => "mariadb",
        }
    }

    pub(crate) fn default_engine_version(self) -> &'static str {
        match self {
            Self::Postgres | Self::AuroraPostgresql => "16.3",
            Self::Mysql | Self::Aurora | Self::AuroraMysql => "8.0",
            Self::MariaDb => "11.4",
        }
    }

    pub(crate) fn supports_cluster(self) -> bool {
        matches!(
            self,
            Self::Aurora | Self::AuroraMysql | Self::AuroraPostgresql
        )
    }

    fn supports_postgres_protocol(self) -> bool {
        matches!(self, Self::Postgres | Self::AuroraPostgresql)
    }

    fn supports_iam_auth(self) -> bool {
        matches!(self, Self::Postgres | Self::AuroraPostgresql)
    }

    pub(crate) fn parameter_group_matches(self, family: &str) -> bool {
        let family = family.trim().to_ascii_lowercase();

        match self {
            Self::Postgres => family.starts_with("postgres"),
            Self::AuroraPostgresql => family.starts_with("aurora-postgresql"),
            Self::Mysql => family.starts_with("mysql"),
            Self::Aurora | Self::AuroraMysql => {
                family.starts_with("aurora")
                    && !family.starts_with("aurora-postgresql")
            }
            Self::MariaDb => family.starts_with("mariadb"),
        }
    }

    pub(crate) fn protocol_family(self) -> RdsProtocolFamily {
        if self.supports_postgres_protocol() {
            RdsProtocolFamily::Postgres
        } else {
            RdsProtocolFamily::Mysql
        }
    }
}

impl fmt::Display for RdsEngine {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DbLifecycleStatus {
    Available,
    Deleting,
    Rebooting,
}

impl DbLifecycleStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Available => "available",
            Self::Deleting => "deleting",
            Self::Rebooting => "rebooting",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RdsEndpoint {
    pub address: String,
    pub port: u16,
}

impl RdsEndpoint {
    pub fn loopback(port: u16) -> Self {
        Self { address: "127.0.0.1".to_owned(), port }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DbInstance {
    pub allocated_storage: u32,
    pub db_cluster_identifier: Option<DbClusterIdentifier>,
    pub db_instance_arn: String,
    pub db_instance_class: String,
    pub db_instance_identifier: DbInstanceIdentifier,
    pub db_instance_status: DbLifecycleStatus,
    pub db_name: Option<String>,
    pub db_parameter_group_name: Option<DbParameterGroupName>,
    pub dbi_resource_id: String,
    pub endpoint: RdsEndpoint,
    pub engine: RdsEngine,
    pub engine_version: String,
    pub iam_database_authentication_enabled: bool,
    pub instance_create_time_epoch_seconds: u64,
    pub master_username: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateDbInstanceInput {
    pub allocated_storage: Option<u32>,
    pub db_cluster_identifier: Option<DbClusterIdentifier>,
    pub db_instance_class: Option<String>,
    pub db_instance_identifier: DbInstanceIdentifier,
    pub db_name: Option<String>,
    pub db_parameter_group_name: Option<DbParameterGroupName>,
    pub engine: Option<String>,
    pub engine_version: Option<String>,
    pub enable_iam_database_authentication: Option<bool>,
    pub master_user_password: Option<String>,
    pub master_username: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModifyDbInstanceInput {
    pub db_parameter_group_name: Option<DbParameterGroupName>,
    pub enable_iam_database_authentication: Option<bool>,
    pub master_user_password: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct StoredDbInstance {
    pub allocated_storage: u32,
    pub db_cluster_identifier: Option<DbClusterIdentifier>,
    pub db_instance_class: String,
    pub db_instance_identifier: DbInstanceIdentifier,
    pub db_instance_status: DbLifecycleStatus,
    pub db_name: Option<String>,
    pub db_parameter_group_name: Option<DbParameterGroupName>,
    pub dbi_resource_id: String,
    pub endpoint: RdsEndpoint,
    pub engine: RdsEngine,
    pub engine_version: String,
    pub iam_database_authentication_enabled: bool,
    pub instance_create_time_epoch_seconds: u64,
    pub master_password: String,
    pub master_username: String,
}

impl StoredDbInstance {
    pub(crate) fn to_output(&self, scope: &RdsScope) -> DbInstance {
        DbInstance {
            allocated_storage: self.allocated_storage,
            db_cluster_identifier: self.db_cluster_identifier.clone(),
            db_instance_arn: format!(
                "arn:aws:rds:{}:{}:db:{}",
                scope.region().as_str(),
                scope.account_id().as_str(),
                self.db_instance_identifier
            ),
            db_instance_class: self.db_instance_class.clone(),
            db_instance_identifier: self.db_instance_identifier.clone(),
            db_instance_status: self.db_instance_status,
            db_name: self.db_name.clone(),
            db_parameter_group_name: self.db_parameter_group_name.clone(),
            dbi_resource_id: self.dbi_resource_id.clone(),
            endpoint: self.endpoint.clone(),
            engine: self.engine,
            engine_version: self.engine_version.clone(),
            iam_database_authentication_enabled: self
                .iam_database_authentication_enabled,
            instance_create_time_epoch_seconds: self
                .instance_create_time_epoch_seconds,
            master_username: self.master_username.clone(),
        }
    }
}

pub(crate) fn validate_iam_setting(
    engine: RdsEngine,
    iam_enabled: bool,
) -> Result<(), RdsError> {
    if iam_enabled && !engine.supports_iam_auth() {
        return Err(RdsError::InvalidParameterCombination {
            message: format!(
                "IAM database authentication is only supported for postgres \
                 and aurora-postgresql within the current local boundary, not \
                 for engine {}.",
                engine.as_str()
            ),
        });
    }
    Ok(())
}

pub(crate) fn sorted_instances(
    state: &StoredRdsState,
) -> Vec<&StoredDbInstance> {
    let mut instances = state.instances.values().collect::<Vec<_>>();
    instances.sort_by(|left, right| {
        left.db_instance_identifier.cmp(&right.db_instance_identifier)
    });
    instances
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RdsProtocolFamily {
    Mysql,
    Postgres,
}
