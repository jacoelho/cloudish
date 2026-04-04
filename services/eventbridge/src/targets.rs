use crate::errors::EventBridgeError;
use aws::{Arn, ServiceName};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

const DEFAULT_TARGET_PAGE_SIZE: usize = 100;
const MAX_TARGETS_PER_MUTATION: usize = 10;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct EventBridgeInputTransformer {
    #[serde(default)]
    pub input_paths_map: BTreeMap<String, String>,
    pub input_template: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct EventBridgeTarget {
    pub arn: Arn,
    pub id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input_transformer: Option<EventBridgeInputTransformer>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub role_arn: Option<Arn>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct PutTargetsInput {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_bus_name: Option<String>,
    pub rule: String,
    pub targets: Vec<EventBridgeTarget>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct PutTargetsFailureEntry {
    pub error_code: String,
    pub error_message: String,
    pub target_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct PutTargetsOutput {
    pub failed_entry_count: i32,
    pub failed_entries: Vec<PutTargetsFailureEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ListTargetsByRuleInput {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_bus_name: Option<String>,
    pub limit: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_token: Option<String>,
    pub rule: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ListTargetsByRuleOutput {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_token: Option<String>,
    pub targets: Vec<EventBridgeTarget>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct RemoveTargetsInput {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_bus_name: Option<String>,
    pub force: Option<bool>,
    pub ids: Vec<String>,
    pub rule: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct RemoveTargetsFailureEntry {
    pub error_code: String,
    pub error_message: String,
    pub target_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct RemoveTargetsOutput {
    pub failed_entry_count: i32,
    pub failed_entries: Vec<RemoveTargetsFailureEntry>,
}

pub(crate) fn validate_put_targets_input(
    input: &PutTargetsInput,
) -> Result<(), EventBridgeError> {
    if input.targets.is_empty() {
        return Err(validation("Targets must not be empty."));
    }
    if input.targets.len() > MAX_TARGETS_PER_MUTATION {
        return Err(validation("Targets must contain at most 10 entries."));
    }

    for target in &input.targets {
        validate_target(target)?;
    }

    Ok(())
}

pub(crate) fn validate_target_scope(
    rule_arn: &Arn,
    target: &EventBridgeTarget,
) -> Result<(), EventBridgeError> {
    let Some(rule_account_id) = rule_arn.account_id() else {
        return Err(validation(
            "Rule Arn must include an account and region.",
        ));
    };
    let Some(rule_region) = rule_arn.region() else {
        return Err(validation(
            "Rule Arn must include an account and region.",
        ));
    };
    if target.arn.account_id() != Some(rule_account_id)
        || target.arn.region() != Some(rule_region)
    {
        return Err(validation(&format!(
            "Target Arn {} must match the rule account and region.",
            target.arn
        )));
    }

    Ok(())
}

pub(crate) fn validate_remove_targets_input(
    input: &RemoveTargetsInput,
) -> Result<(), EventBridgeError> {
    if input.force.unwrap_or(false) {
        return Err(EventBridgeError::UnsupportedOperation {
            message: "RemoveTargets Force is not supported by this Cloudish EventBridge subset.".to_owned(),
        });
    }
    if input.ids.is_empty() {
        return Err(validation("Ids must not be empty."));
    }
    if input.ids.len() > MAX_TARGETS_PER_MUTATION {
        return Err(validation("Ids must contain at most 10 entries."));
    }
    if input.ids.iter().any(|id| id.trim().is_empty()) {
        return Err(validation("Target ids must not be empty."));
    }

    Ok(())
}

pub(crate) fn normalize_target_limit(
    limit: Option<u32>,
) -> Result<usize, EventBridgeError> {
    let limit = limit.unwrap_or(DEFAULT_TARGET_PAGE_SIZE as u32);
    if limit == 0 {
        return Err(validation("Limit must be greater than zero."));
    }

    usize::try_from(limit)
        .map_err(|_| validation("Limit could not be represented."))
}

pub(crate) fn render_target_payload(
    target: &EventBridgeTarget,
    event: &Value,
) -> Result<Vec<u8>, EventBridgeError> {
    if let Some(input) = &target.input {
        return parse_json_text(input);
    }
    if let Some(input_path) = &target.input_path {
        let selected = select_json_path(event, input_path)?;
        return serde_json::to_vec(selected).map_err(|error| {
            EventBridgeError::InternalFailure { message: error.to_string() }
        });
    }
    if let Some(transformer) = &target.input_transformer {
        let rendered = render_input_template(transformer, event)?;
        return Ok(rendered.into_bytes());
    }

    serde_json::to_vec(event).map_err(|error| {
        EventBridgeError::InternalFailure { message: error.to_string() }
    })
}

pub(crate) fn put_targets_failure(
    target_id: &str,
    error: &EventBridgeError,
) -> PutTargetsFailureEntry {
    PutTargetsFailureEntry {
        error_code: error_code(error).to_owned(),
        error_message: error.to_string(),
        target_id: target_id.to_owned(),
    }
}

pub(crate) fn remove_targets_output() -> RemoveTargetsOutput {
    RemoveTargetsOutput { failed_entry_count: 0, failed_entries: Vec::new() }
}

pub(crate) fn put_targets_output(
    failed_entries: Vec<PutTargetsFailureEntry>,
) -> PutTargetsOutput {
    PutTargetsOutput {
        failed_entry_count: failed_entries.len() as i32,
        failed_entries,
    }
}

fn validate_target(
    target: &EventBridgeTarget,
) -> Result<(), EventBridgeError> {
    if target.id.trim().is_empty() {
        return Err(validation("Target Id must not be empty."));
    }
    if target.arn.account_id().is_none() || target.arn.region().is_none() {
        return Err(validation(
            "Target Arn must include an account and region.",
        ));
    }
    match target.arn.service() {
        ServiceName::Lambda | ServiceName::Sns | ServiceName::Sqs => {}
        _ => {
            return Err(EventBridgeError::UnsupportedOperation {
                message: format!(
                    "Target Arn service {} is not supported by this Cloudish EventBridge subset.",
                    target.arn.service().as_str()
                ),
            });
        }
    }
    if target.role_arn.is_some() {
        return Err(validation(
            "RoleArn is not supported for Lambda, SNS, or SQS EventBridge targets.",
        ));
    }

    let configured_payloads = [
        target.input.is_some(),
        target.input_path.is_some(),
        target.input_transformer.is_some(),
    ]
    .into_iter()
    .filter(|configured| *configured)
    .count();
    if configured_payloads > 1 {
        return Err(validation(
            "Input, InputPath, and InputTransformer are mutually exclusive.",
        ));
    }
    if let Some(input) = &target.input {
        let _ = parse_json_text(input)?;
    }
    if let Some(input_path) = &target.input_path {
        validate_json_path(input_path)?;
    }
    if let Some(transformer) = &target.input_transformer {
        validate_input_transformer(transformer)?;
    }

    Ok(())
}

fn validate_input_transformer(
    transformer: &EventBridgeInputTransformer,
) -> Result<(), EventBridgeError> {
    if transformer.input_template.trim().is_empty() {
        return Err(validation("InputTemplate must not be empty."));
    }
    for path in transformer.input_paths_map.values() {
        validate_json_path(path)?;
    }

    Ok(())
}

fn parse_json_text(text: &str) -> Result<Vec<u8>, EventBridgeError> {
    let value = serde_json::from_str::<Value>(text).map_err(|error| {
        validation(&format!("Target input must be valid JSON: {error}"))
    })?;

    serde_json::to_vec(&value).map_err(|error| {
        EventBridgeError::InternalFailure { message: error.to_string() }
    })
}

fn render_input_template(
    transformer: &EventBridgeInputTransformer,
    event: &Value,
) -> Result<String, EventBridgeError> {
    let mut rendered = transformer.input_template.clone();

    for (placeholder, path) in &transformer.input_paths_map {
        let selected = select_json_path(event, path)?;
        let replacement =
            serde_json::to_string(selected).map_err(|error| {
                EventBridgeError::InternalFailure {
                    message: error.to_string(),
                }
            })?;
        rendered = rendered.replace(&format!("<{placeholder}>"), &replacement);
    }

    Ok(rendered)
}

fn select_json_path<'a>(
    value: &'a Value,
    path: &str,
) -> Result<&'a Value, EventBridgeError> {
    validate_json_path(path)?;

    if path == "$" {
        return Ok(value);
    }

    let mut current = value;
    for segment in path.trim_start_matches("$.").split('.') {
        let Value::Object(map) = current else {
            return Err(validation(&format!(
                "InputPath {path} did not resolve to a value."
            )));
        };
        current = map.get(segment).ok_or_else(|| {
            validation(&format!(
                "InputPath {path} did not resolve to a value."
            ))
        })?;
    }

    Ok(current)
}

fn validate_json_path(path: &str) -> Result<(), EventBridgeError> {
    if path == "$" {
        return Ok(());
    }
    let Some(rest) = path.strip_prefix("$.") else {
        return Err(validation(
            "Only '$' and '$.field' style JSON paths are supported.",
        ));
    };
    if rest.is_empty() {
        return Err(validation(
            "Only '$' and '$.field' style JSON paths are supported.",
        ));
    }
    if rest.split('.').any(|segment| {
        segment.is_empty()
            || segment.chars().any(|character| {
                !character.is_ascii_alphanumeric()
                    && character != '_'
                    && character != '-'
            })
    }) {
        return Err(validation(
            "Only '$' and '$.field' style JSON paths are supported.",
        ));
    }

    Ok(())
}

fn validation(message: &str) -> EventBridgeError {
    EventBridgeError::Validation { message: message.to_owned() }
}

fn error_code(error: &EventBridgeError) -> &'static str {
    match error {
        EventBridgeError::ConcurrentModification { .. } => {
            "ConcurrentModificationException"
        }
        EventBridgeError::InternalFailure { .. } => "InternalException",
        EventBridgeError::ResourceAlreadyExists { .. } => {
            "ResourceAlreadyExistsException"
        }
        EventBridgeError::ResourceNotFound { .. } => {
            "ResourceNotFoundException"
        }
        EventBridgeError::UnsupportedOperation { .. } => {
            "UnsupportedOperationException"
        }
        EventBridgeError::Validation { .. } => "ValidationException",
    }
}

#[cfg(test)]
mod tests {
    use super::{
        EventBridgeInputTransformer, EventBridgeTarget, PutTargetsInput,
        render_target_payload, validate_put_targets_input,
    };
    use aws::Arn;
    use serde_json::json;
    use std::collections::BTreeMap;

    fn lambda_target() -> EventBridgeTarget {
        EventBridgeTarget {
            arn: "arn:aws:lambda:eu-west-2:000000000000:function:processor"
                .parse::<Arn>()
                .expect("lambda arn should parse"),
            id: "processor".to_owned(),
            input: None,
            input_path: None,
            input_transformer: None,
            role_arn: None,
        }
    }

    #[test]
    fn target_validation_rejects_unsupported_arns_and_payload_modes() {
        let mut unsupported = lambda_target();
        unsupported.arn =
            "arn:aws:events:eu-west-2:000000000000:event-bus/default"
                .parse::<Arn>()
                .expect("events arn should parse");
        assert!(
            validate_put_targets_input(&PutTargetsInput {
                event_bus_name: None,
                rule: "demo".to_owned(),
                targets: vec![unsupported],
            })
            .is_err()
        );

        let mut conflicting = lambda_target();
        conflicting.input = Some("{\"kind\":\"direct\"}".to_owned());
        conflicting.input_path = Some("$.detail".to_owned());
        assert!(
            validate_put_targets_input(&PutTargetsInput {
                event_bus_name: None,
                rule: "demo".to_owned(),
                targets: vec![conflicting],
            })
            .is_err()
        );
    }

    #[test]
    fn target_payload_rendering_supports_default_path_and_template_modes() {
        let event = json!({
            "source": "orders",
            "detail": {
                "id": "ord-1",
                "kind": "created"
            }
        });

        let mut direct = lambda_target();
        direct.input = Some("{\"kind\":\"fixed\"}".to_owned());
        assert_eq!(
            String::from_utf8(
                render_target_payload(&direct, &event)
                    .expect("direct input should render")
            )
            .expect("payload should be utf-8"),
            "{\"kind\":\"fixed\"}"
        );

        let mut path = lambda_target();
        path.input_path = Some("$.detail".to_owned());
        assert_eq!(
            String::from_utf8(
                render_target_payload(&path, &event)
                    .expect("path input should render")
            )
            .expect("payload should be utf-8"),
            "{\"id\":\"ord-1\",\"kind\":\"created\"}"
        );

        let mut templated = lambda_target();
        templated.input_transformer = Some(EventBridgeInputTransformer {
            input_paths_map: BTreeMap::from([
                ("source".to_owned(), "$.source".to_owned()),
                ("id".to_owned(), "$.detail.id".to_owned()),
            ]),
            input_template: "{\"source\":<source>,\"id\":<id>}".to_owned(),
        });
        assert_eq!(
            String::from_utf8(
                render_target_payload(&templated, &event)
                    .expect("templated input should render")
            )
            .expect("payload should be utf-8"),
            "{\"source\":\"orders\",\"id\":\"ord-1\"}"
        );
    }
}
