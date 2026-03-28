use serde::Serialize;
use std::collections::BTreeMap;

pub use crate::state::CloudFormationService;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CloudFormationStackOperationInput {
    pub capabilities: Vec<String>,
    pub parameters: BTreeMap<String, String>,
    pub stack_name: String,
    pub template_body: Option<String>,
    pub template_url: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CloudFormationStackOperationOutput {
    pub stack_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CloudFormationCreateChangeSetInput {
    pub capabilities: Vec<String>,
    pub change_set_name: String,
    pub change_set_type: Option<String>,
    pub parameters: BTreeMap<String, String>,
    pub stack_name: String,
    pub template_body: Option<String>,
    pub template_url: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CloudFormationCreateChangeSetOutput {
    pub id: String,
    pub stack_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct CloudFormationStackOutput {
    pub description: Option<String>,
    pub output_key: String,
    pub output_value: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CloudFormationStackDescription {
    pub capabilities: Vec<String>,
    pub creation_time_epoch_seconds: u64,
    pub description: Option<String>,
    pub last_updated_time_epoch_seconds: Option<u64>,
    pub outputs: Vec<CloudFormationStackOutput>,
    pub stack_id: String,
    pub stack_name: String,
    pub stack_status: String,
    pub stack_status_reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CloudFormationChangeSummary {
    pub action: String,
    pub logical_resource_id: String,
    pub replacement: Option<String>,
    pub resource_type: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CloudFormationChangeSetDescription {
    pub changes: Vec<CloudFormationChangeSummary>,
    pub change_set_id: String,
    pub change_set_name: String,
    pub change_set_type: String,
    pub creation_time_epoch_seconds: u64,
    pub execution_status: String,
    pub stack_id: String,
    pub stack_name: String,
    pub status: String,
    pub status_reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CloudFormationChangeSetSummary {
    pub change_set_id: String,
    pub change_set_name: String,
    pub creation_time_epoch_seconds: u64,
    pub execution_status: String,
    pub status: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CloudFormationStackResourceDescription {
    pub last_updated_timestamp_epoch_seconds: u64,
    pub logical_resource_id: String,
    pub physical_resource_id: String,
    pub resource_status: String,
    pub resource_status_reason: Option<String>,
    pub resource_type: String,
    pub stack_id: String,
    pub stack_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CloudFormationStackResourceSummary {
    pub last_updated_timestamp_epoch_seconds: u64,
    pub logical_resource_id: String,
    pub physical_resource_id: String,
    pub resource_status: String,
    pub resource_status_reason: Option<String>,
    pub resource_type: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CloudFormationStackEvent {
    pub event_id: String,
    pub logical_resource_id: String,
    pub physical_resource_id: Option<String>,
    pub resource_status: String,
    pub resource_status_reason: Option<String>,
    pub resource_type: String,
    pub stack_id: String,
    pub stack_name: String,
    pub timestamp_epoch_seconds: u64,
}
