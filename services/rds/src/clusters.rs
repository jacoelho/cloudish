use crate::instances::{DbLifecycleStatus, RdsEndpoint, RdsEngine};
use crate::scope::RdsScope;
use crate::{
    DbClusterIdentifier, DbInstanceIdentifier, DbParameterGroupName,
    StoredRdsState,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DbClusterMember {
    pub db_instance_identifier: DbInstanceIdentifier,
    pub is_cluster_writer: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DbCluster {
    pub database_name: Option<String>,
    pub db_cluster_arn: String,
    pub db_cluster_identifier: DbClusterIdentifier,
    pub db_cluster_members: Vec<DbClusterMember>,
    pub db_cluster_parameter_group: Option<DbParameterGroupName>,
    pub db_cluster_resource_id: String,
    pub endpoint: RdsEndpoint,
    pub engine: RdsEngine,
    pub engine_version: String,
    pub iam_database_authentication_enabled: bool,
    pub master_username: String,
    pub reader_endpoint: Option<RdsEndpoint>,
    pub status: DbLifecycleStatus,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateDbClusterInput {
    pub database_name: Option<String>,
    pub db_cluster_identifier: DbClusterIdentifier,
    pub db_cluster_parameter_group_name: Option<DbParameterGroupName>,
    pub engine: String,
    pub engine_version: Option<String>,
    pub enable_iam_database_authentication: Option<bool>,
    pub master_user_password: String,
    pub master_username: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModifyDbClusterInput {
    pub db_cluster_parameter_group_name: Option<DbParameterGroupName>,
    pub enable_iam_database_authentication: Option<bool>,
    pub master_user_password: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct StoredDbCluster {
    pub database_name: Option<String>,
    pub db_cluster_identifier: DbClusterIdentifier,
    pub db_cluster_members: Vec<DbClusterMember>,
    pub db_cluster_parameter_group: Option<DbParameterGroupName>,
    pub db_cluster_resource_id: String,
    pub endpoint: RdsEndpoint,
    pub engine: RdsEngine,
    pub engine_version: String,
    pub iam_database_authentication_enabled: bool,
    pub master_password: String,
    pub master_username: String,
    pub status: DbLifecycleStatus,
}

impl StoredDbCluster {
    pub(crate) fn to_output(&self, scope: &RdsScope) -> DbCluster {
        DbCluster {
            database_name: self.database_name.clone(),
            db_cluster_arn: format!(
                "arn:aws:rds:{}:{}:cluster:{}",
                scope.region().as_str(),
                scope.account_id().as_str(),
                self.db_cluster_identifier
            ),
            db_cluster_identifier: self.db_cluster_identifier.clone(),
            db_cluster_members: self.db_cluster_members.clone(),
            db_cluster_parameter_group: self
                .db_cluster_parameter_group
                .clone(),
            db_cluster_resource_id: self.db_cluster_resource_id.clone(),
            endpoint: self.endpoint.clone(),
            engine: self.engine,
            engine_version: self.engine_version.clone(),
            iam_database_authentication_enabled: self
                .iam_database_authentication_enabled,
            master_username: self.master_username.clone(),
            reader_endpoint: Some(self.endpoint.clone()),
            status: self.status,
        }
    }
}

pub(crate) fn sorted_clusters(
    state: &StoredRdsState,
) -> Vec<&StoredDbCluster> {
    let mut clusters = state.clusters.values().collect::<Vec<_>>();
    clusters.sort_by(|left, right| {
        left.db_cluster_identifier.cmp(&right.db_cluster_identifier)
    });
    clusters
}
