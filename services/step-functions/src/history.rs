use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetExecutionHistoryInput {
    pub execution_arn: String,
    pub include_execution_data: Option<bool>,
    pub max_results: Option<u32>,
    pub next_token: Option<String>,
    pub reverse_order: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GetExecutionHistoryOutput {
    pub events: Vec<HistoryEvent>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HistoryEvent {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub execution_aborted_event_details: Option<ExecutionAbortedEventDetails>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub execution_failed_event_details: Option<ExecutionFailedEventDetails>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub execution_started_event_details: Option<ExecutionStartedEventDetails>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub execution_succeeded_event_details:
        Option<ExecutionSucceededEventDetails>,
    pub id: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub previous_event_id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state_entered_event_details: Option<StateEnteredEventDetails>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state_exited_event_details: Option<StateExitedEventDetails>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task_failed_event_details: Option<TaskFailedEventDetails>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task_scheduled_event_details: Option<TaskScheduledEventDetails>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task_succeeded_event_details: Option<TaskSucceededEventDetails>,
    pub timestamp: u64,
    #[serde(rename = "type")]
    pub event_type: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecutionStartedEventDetails {
    pub input: String,
    pub role_arn: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecutionSucceededEventDetails {
    pub output: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecutionFailedEventDetails {
    pub cause: String,
    pub error: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecutionAbortedEventDetails {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cause: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StateEnteredEventDetails {
    pub input: String,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StateExitedEventDetails {
    pub name: String,
    pub output: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskScheduledEventDetails {
    pub resource: String,
    pub resource_type: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskSucceededEventDetails {
    pub output: String,
    pub resource: String,
    pub resource_type: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskFailedEventDetails {
    pub cause: String,
    pub error: String,
    pub resource: String,
    pub resource_type: String,
}
