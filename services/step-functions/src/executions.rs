use crate::errors::StepFunctionsError;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ExecutionStatus {
    Running,
    Succeeded,
    Failed,
    Aborted,
}

impl ExecutionStatus {
    pub(crate) fn parse(
        value: Option<&str>,
    ) -> Result<Self, StepFunctionsError> {
        match value {
            None => Ok(Self::Running),
            Some("RUNNING") => Ok(Self::Running),
            Some("SUCCEEDED") => Ok(Self::Succeeded),
            Some("FAILED") => Ok(Self::Failed),
            Some("ABORTED") => Ok(Self::Aborted),
            Some(other) => Err(StepFunctionsError::Validation {
                message: format!("Unsupported execution status `{other}`."),
            }),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StartExecutionInput {
    pub input: Option<String>,
    pub name: Option<String>,
    pub state_machine_arn: String,
    pub trace_header: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StartExecutionOutput {
    pub execution_arn: String,
    pub start_date: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DescribeExecutionOutput {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cause: Option<String>,
    pub execution_arn: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    pub input: String,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output: Option<String>,
    pub start_date: u64,
    pub state_machine_arn: String,
    pub status: ExecutionStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stop_date: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeExecutionInput {
    pub execution_arn: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListExecutionsInput {
    pub max_results: Option<u32>,
    pub next_token: Option<String>,
    pub state_machine_arn: String,
    pub status_filter: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ListExecutionsOutput {
    pub executions: Vec<ExecutionListItem>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecutionListItem {
    pub execution_arn: String,
    pub name: String,
    pub start_date: u64,
    pub state_machine_arn: String,
    pub status: ExecutionStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stop_date: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StopExecutionInput {
    pub cause: Option<String>,
    pub error: Option<String>,
    pub execution_arn: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StopExecutionOutput {
    pub stop_date: u64,
}
