use crate::errors::RdsError;
use crate::instances::RdsEngine;
use crate::scope::RdsScope;
use crate::{DbParameterGroupFamily, DbParameterGroupName, StoredRdsState};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DbParameterGroup {
    pub db_parameter_group_arn: String,
    pub db_parameter_group_family: DbParameterGroupFamily,
    pub db_parameter_group_name: DbParameterGroupName,
    pub description: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DbParameter {
    pub apply_type: String,
    pub data_type: String,
    pub description: String,
    pub is_modifiable: bool,
    pub parameter_name: String,
    pub parameter_value: String,
    pub source: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateDbParameterGroupInput {
    pub db_parameter_group_family: DbParameterGroupFamily,
    pub db_parameter_group_name: DbParameterGroupName,
    pub description: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModifyDbParameterGroupInput {
    pub parameters: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct StoredDbParameterGroup {
    pub db_parameter_group_family: DbParameterGroupFamily,
    pub db_parameter_group_name: DbParameterGroupName,
    pub description: String,
    pub parameters: BTreeMap<String, String>,
}

impl StoredDbParameterGroup {
    pub(crate) fn to_output(&self, scope: &RdsScope) -> DbParameterGroup {
        DbParameterGroup {
            db_parameter_group_arn: format!(
                "arn:aws:rds:{}:{}:pg:{}",
                scope.region().as_str(),
                scope.account_id().as_str(),
                self.db_parameter_group_name
            ),
            db_parameter_group_family: self.db_parameter_group_family.clone(),
            db_parameter_group_name: self.db_parameter_group_name.clone(),
            description: self.description.clone(),
        }
    }
}

pub(crate) fn parameter_group_family_supported(
    family: &DbParameterGroupFamily,
) -> bool {
    let family = family.as_str();
    family.starts_with("postgres")
        || family.starts_with("aurora-postgresql")
        || family.starts_with("mysql")
        || family.starts_with("aurora")
        || family.starts_with("mariadb")
}

pub(crate) fn validate_parameter_group_engine(
    state: &StoredRdsState,
    engine: RdsEngine,
    parameter_group_name: &DbParameterGroupName,
) -> Result<(), RdsError> {
    let group =
        state.parameter_groups.get(parameter_group_name).ok_or_else(|| {
            RdsError::DbParameterGroupNotFound {
                message: format!(
                    "DB parameter group {parameter_group_name} not found."
                ),
            }
        })?;
    if !engine
        .parameter_group_matches(group.db_parameter_group_family.as_str())
    {
        return Err(RdsError::InvalidParameterCombination {
            message: format!(
                "DB parameter group {} with family {} is not compatible with \
                 engine {}.",
                group.db_parameter_group_name,
                group.db_parameter_group_family,
                engine.as_str()
            ),
        });
    }
    Ok(())
}

pub(crate) fn sorted_parameter_groups(
    state: &StoredRdsState,
) -> Vec<&StoredDbParameterGroup> {
    let mut groups = state.parameter_groups.values().collect::<Vec<_>>();
    groups.sort_by(|left, right| {
        left.db_parameter_group_name.cmp(&right.db_parameter_group_name)
    });
    groups
}

pub(crate) fn parameter_group_parameters(
    group: &StoredDbParameterGroup,
) -> Vec<DbParameter> {
    let mut parameters =
        default_parameters(group.db_parameter_group_family.as_str());
    for (name, value) in &group.parameters {
        if let Some(parameter) = parameters
            .iter_mut()
            .find(|parameter| parameter.parameter_name == *name)
        {
            parameter.parameter_value = value.clone();
            parameter.source = "user".to_owned();
        } else {
            parameters.push(DbParameter {
                apply_type: "dynamic".to_owned(),
                data_type: "string".to_owned(),
                description: "User-defined local parameter.".to_owned(),
                is_modifiable: true,
                parameter_name: name.clone(),
                parameter_value: value.clone(),
                source: "user".to_owned(),
            });
        }
    }
    parameters
        .sort_by(|left, right| left.parameter_name.cmp(&right.parameter_name));
    parameters
}

fn default_parameters(family: &str) -> Vec<DbParameter> {
    if family.starts_with("postgres")
        || family.starts_with("aurora-postgresql")
    {
        return vec![
            DbParameter {
                apply_type: "dynamic".to_owned(),
                data_type: "integer".to_owned(),
                description: "Maximum allowed concurrent connections."
                    .to_owned(),
                is_modifiable: true,
                parameter_name: "max_connections".to_owned(),
                parameter_value: "100".to_owned(),
                source: "engine-default".to_owned(),
            },
            DbParameter {
                apply_type: "dynamic".to_owned(),
                data_type: "bool".to_owned(),
                description: "Whether SSL is required.".to_owned(),
                is_modifiable: true,
                parameter_name: "rds.force_ssl".to_owned(),
                parameter_value: "0".to_owned(),
                source: "engine-default".to_owned(),
            },
        ];
    }

    vec![
        DbParameter {
            apply_type: "dynamic".to_owned(),
            data_type: "string".to_owned(),
            description: "Default character set.".to_owned(),
            is_modifiable: true,
            parameter_name: "character_set_server".to_owned(),
            parameter_value: "utf8mb4".to_owned(),
            source: "engine-default".to_owned(),
        },
        DbParameter {
            apply_type: "dynamic".to_owned(),
            data_type: "integer".to_owned(),
            description: "Maximum allowed concurrent connections.".to_owned(),
            is_modifiable: true,
            parameter_name: "max_connections".to_owned(),
            parameter_value: "151".to_owned(),
            source: "engine-default".to_owned(),
        },
    ]
}
