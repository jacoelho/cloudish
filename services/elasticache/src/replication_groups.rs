use crate::ElastiCacheReplicationGroupId;
use crate::auth::ElastiCacheAuthenticationType;
use crate::errors::ElastiCacheError;

use serde::{Deserialize, Serialize};
use std::fmt;

const DEFAULT_REDIS_ENGINE_VERSION: &str = "7.0";
const DEFAULT_VALKEY_ENGINE_VERSION: &str = "8.0";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ElastiCacheEngine {
    Redis,
    Valkey,
}

impl ElastiCacheEngine {
    /// # Errors
    ///
    /// Returns [`ElastiCacheError::InvalidParameterValue`] when the engine is
    /// not `redis` or `valkey`.
    pub fn parse(value: &str) -> Result<Self, ElastiCacheError> {
        match value.trim().to_ascii_lowercase().as_str() {
            "redis" => Ok(Self::Redis),
            "valkey" => Ok(Self::Valkey),
            other => Err(ElastiCacheError::InvalidParameterValue {
                message: format!(
                    "Unsupported ElastiCache engine `{other}`. Supported \
                     engines are redis and valkey."
                ),
            }),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Redis => "redis",
            Self::Valkey => "valkey",
        }
    }

    pub(crate) fn default_engine_version(self) -> &'static str {
        match self {
            Self::Redis => DEFAULT_REDIS_ENGINE_VERSION,
            Self::Valkey => DEFAULT_VALKEY_ENGINE_VERSION,
        }
    }
}

impl fmt::Display for ElastiCacheEngine {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReplicationGroupStatus {
    Available,
    Deleting,
}

impl ReplicationGroupStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Available => "available",
            Self::Deleting => "deleting",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ElastiCacheEndpoint {
    pub address: String,
    pub port: u16,
}

impl ElastiCacheEndpoint {
    pub(crate) fn loopback(port: u16) -> Self {
        Self { address: "127.0.0.1".to_owned(), port }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ElastiCacheNodeGroupMember {
    pub cache_cluster_id: String,
    pub cache_node_id: String,
    pub current_role: String,
    pub read_endpoint: ElastiCacheEndpoint,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ElastiCacheNodeGroup {
    pub node_group_id: String,
    pub primary_endpoint: ElastiCacheEndpoint,
    pub status: ReplicationGroupStatus,
    pub node_group_members: Vec<ElastiCacheNodeGroupMember>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicationGroup {
    pub auth_token_enabled: bool,
    pub automatic_failover: String,
    pub at_rest_encryption_enabled: bool,
    pub cluster_enabled: bool,
    pub configuration_endpoint: ElastiCacheEndpoint,
    pub description: String,
    pub engine: ElastiCacheEngine,
    pub engine_version: String,
    pub member_clusters: Vec<String>,
    pub multi_az: String,
    pub node_groups: Vec<ElastiCacheNodeGroup>,
    pub replication_group_id: ElastiCacheReplicationGroupId,
    pub snapshot_retention_limit: u32,
    pub status: ReplicationGroupStatus,
    pub transit_encryption_enabled: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateReplicationGroupInput {
    pub auth_token: Option<String>,
    pub cluster_mode: Option<String>,
    pub engine: Option<String>,
    pub engine_version: Option<String>,
    pub multi_az_enabled: Option<bool>,
    pub num_cache_clusters: Option<u32>,
    pub num_node_groups: Option<u32>,
    pub replicas_per_node_group: Option<u32>,
    pub replication_group_description: String,
    pub replication_group_id: ElastiCacheReplicationGroupId,
    pub transit_encryption_enabled: Option<bool>,
    pub has_node_group_configuration: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct StoredReplicationGroup {
    pub auth_mode: ElastiCacheAuthenticationType,
    pub auth_token: Option<String>,
    pub description: String,
    pub endpoint: ElastiCacheEndpoint,
    pub engine: ElastiCacheEngine,
    pub engine_version: String,
    pub replication_group_id: ElastiCacheReplicationGroupId,
    pub status: ReplicationGroupStatus,
    pub transit_encryption_enabled: bool,
    pub created_at_epoch_seconds: u64,
}

impl StoredReplicationGroup {
    pub(crate) fn to_output(&self) -> ReplicationGroup {
        let primary_endpoint = self.endpoint.clone();
        let member_cluster = self.replication_group_id.to_string();
        ReplicationGroup {
            auth_token_enabled: self.auth_mode
                == ElastiCacheAuthenticationType::Password,
            automatic_failover: "disabled".to_owned(),
            at_rest_encryption_enabled: false,
            cluster_enabled: false,
            configuration_endpoint: self.endpoint.clone(),
            description: self.description.clone(),
            engine: self.engine,
            engine_version: self.engine_version.clone(),
            member_clusters: vec![member_cluster.clone()],
            multi_az: "disabled".to_owned(),
            node_groups: vec![ElastiCacheNodeGroup {
                node_group_id: "0001".to_owned(),
                primary_endpoint: primary_endpoint.clone(),
                status: self.status,
                node_group_members: vec![ElastiCacheNodeGroupMember {
                    cache_cluster_id: member_cluster,
                    cache_node_id: "0001".to_owned(),
                    current_role: "primary".to_owned(),
                    read_endpoint: primary_endpoint,
                }],
            }],
            replication_group_id: self.replication_group_id.clone(),
            snapshot_retention_limit: 0,
            status: self.status,
            transit_encryption_enabled: self.transit_encryption_enabled,
        }
    }
}

pub(crate) fn reject_unsupported_replication_group_inputs(
    input: &CreateReplicationGroupInput,
) -> Result<(), ElastiCacheError> {
    if input
        .cluster_mode
        .as_deref()
        .is_some_and(|value| !value.eq_ignore_ascii_case("disabled"))
    {
        return Err(ElastiCacheError::InvalidParameterCombination {
            message: "Cluster mode is not supported by Cloudish \
                      ElastiCache."
                .to_owned(),
        });
    }
    if input.num_node_groups.unwrap_or(1) > 1
        || input.num_cache_clusters.unwrap_or(1) > 1
        || input.replicas_per_node_group.unwrap_or(0) > 0
        || input.has_node_group_configuration
    {
        return Err(ElastiCacheError::InvalidParameterCombination {
            message: "Multi-node replication groups are not supported by \
                      Cloudish ElastiCache."
                .to_owned(),
        });
    }
    if input.multi_az_enabled.unwrap_or(false) {
        return Err(ElastiCacheError::InvalidParameterCombination {
            message: "Multi-AZ ElastiCache replication groups are not \
                      supported by Cloudish."
                .to_owned(),
        });
    }

    Ok(())
}
