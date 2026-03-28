use crate::errors::StepFunctionsError;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum StateMachineType {
    Standard,
    Express,
}

impl StateMachineType {
    pub(crate) fn parse(
        value: Option<&str>,
    ) -> Result<Self, StepFunctionsError> {
        match value.unwrap_or("STANDARD") {
            "STANDARD" => Ok(Self::Standard),
            "EXPRESS" => {
                Err(StepFunctionsError::UnsupportedStateMachineType {
                    message:
                        "Only STANDARD workflows are supported by Cloudish."
                            .to_owned(),
                })
            }
            other => Err(StepFunctionsError::Validation {
                message: format!("Unsupported state machine type `{other}`."),
            }),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum StateMachineStatus {
    Active,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateStateMachineInput {
    pub definition: String,
    pub name: String,
    pub role_arn: String,
    pub state_machine_type: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateStateMachineOutput {
    pub creation_date: u64,
    pub state_machine_arn: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListStateMachinesInput {
    pub max_results: Option<u32>,
    pub next_token: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ListStateMachinesOutput {
    pub state_machines: Vec<StateMachineListItem>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StateMachineListItem {
    pub creation_date: u64,
    pub name: String,
    pub state_machine_arn: String,
    pub state_machine_type: StateMachineType,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DescribeStateMachineOutput {
    pub creation_date: u64,
    pub definition: String,
    pub name: String,
    pub role_arn: String,
    pub state_machine_arn: String,
    pub state_machine_type: StateMachineType,
    pub status: StateMachineStatus,
}
