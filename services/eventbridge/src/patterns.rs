use crate::errors::EventBridgeError;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct EventPattern {
    root: Value,
}

impl EventPattern {
    pub(crate) fn original(&self) -> &Value {
        &self.root
    }
}

pub(crate) fn parse_event_pattern(
    pattern: &str,
) -> Result<EventPattern, EventBridgeError> {
    let root = serde_json::from_str::<Value>(pattern).map_err(|error| {
        EventBridgeError::Validation {
            message: format!("EventPattern is not valid JSON: {error}"),
        }
    })?;
    validate_root_pattern(&root)?;

    Ok(EventPattern { root })
}

pub(crate) fn event_matches(
    pattern: &EventPattern,
    source: &str,
    detail_type: &str,
    detail: &Value,
) -> bool {
    let Value::Object(root) = pattern.original() else {
        return false;
    };

    root.iter().all(|(field, candidate)| match field.as_str() {
        "source" => match_field(candidate, &Value::String(source.to_owned())),
        "detail-type" => {
            match_field(candidate, &Value::String(detail_type.to_owned()))
        }
        "detail" => match_field(candidate, detail),
        _ => false,
    })
}

fn validate_root_pattern(value: &Value) -> Result<(), EventBridgeError> {
    let Value::Object(root) = value else {
        return Err(validation("EventPattern must be a JSON object."));
    };

    for (field, candidate) in root {
        match field.as_str() {
            "source" | "detail-type" => validate_match_candidate(candidate)?,
            "detail" => validate_detail_pattern(candidate)?,
            _ => {
                return Err(EventBridgeError::UnsupportedOperation {
                    message: format!(
                        "EventPattern field {field} is not supported by this Cloudish EventBridge subset."
                    ),
                });
            }
        }
    }

    Ok(())
}

fn validate_detail_pattern(value: &Value) -> Result<(), EventBridgeError> {
    let Value::Object(map) = value else {
        return Err(validation("EventPattern detail must be a JSON object."));
    };

    validate_detail_map(map)
}

fn validate_detail_map(
    map: &Map<String, Value>,
) -> Result<(), EventBridgeError> {
    for candidate in map.values() {
        match candidate {
            Value::Object(child) => validate_detail_map(child)?,
            _ => validate_match_candidate(candidate)?,
        }
    }

    Ok(())
}

fn validate_match_candidate(value: &Value) -> Result<(), EventBridgeError> {
    match value {
        Value::Array(values) => {
            if values.is_empty() {
                return Err(validation(
                    "EventPattern arrays must not be empty.",
                ));
            }
            for entry in values {
                if matches!(entry, Value::Object(_)) {
                    return Err(EventBridgeError::UnsupportedOperation {
                        message: "EventPattern objects inside match arrays are not supported by this Cloudish EventBridge subset.".to_owned(),
                    });
                }
            }
            Ok(())
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {
            Ok(())
        }
        Value::Object(_) => Ok(()),
    }
}

fn match_field(pattern: &Value, candidate: &Value) -> bool {
    match pattern {
        Value::Array(expected) => expected
            .iter()
            .any(|value| scalar_or_array_membership_match(value, candidate)),
        Value::Object(expected) => {
            let Value::Object(actual) = candidate else {
                return false;
            };
            expected.iter().all(|(field, value)| {
                actual
                    .get(field)
                    .is_some_and(|candidate| match_field(value, candidate))
            })
        }
        _ => scalar_or_array_membership_match(pattern, candidate),
    }
}

fn scalar_or_array_membership_match(
    expected: &Value,
    candidate: &Value,
) -> bool {
    if candidate == expected {
        return true;
    }

    match candidate {
        Value::Array(values) => values.iter().any(|value| value == expected),
        _ => false,
    }
}

fn validation(message: &str) -> EventBridgeError {
    EventBridgeError::Validation { message: message.to_owned() }
}

#[cfg(test)]
mod tests {
    use super::{event_matches, parse_event_pattern};
    use serde_json::json;

    #[test]
    fn pattern_matching_supports_source_detail_type_and_nested_detail() {
        let pattern = parse_event_pattern(
            r#"{
                "source": ["orders"],
                "detail-type": ["Created"],
                "detail": {
                    "region": ["eu-west-2"],
                    "status": ["queued"],
                    "tags": ["priority"]
                }
            }"#,
        )
        .expect("pattern should parse");

        assert!(event_matches(
            &pattern,
            "orders",
            "Created",
            &json!({
                "region": "eu-west-2",
                "status": "queued",
                "tags": ["priority", "new"]
            }),
        ));
        assert!(!event_matches(
            &pattern,
            "orders",
            "Created",
            &json!({
                "region": "us-east-1",
                "status": "queued",
                "tags": ["priority", "new"]
            }),
        ));
    }

    #[test]
    fn pattern_validation_rejects_unsupported_fields_and_shapes() {
        assert!(parse_event_pattern(r#"{"account":["1"]}"#).is_err());
        assert!(parse_event_pattern(r#"[]"#).is_err());
        assert!(parse_event_pattern(r#"{"detail":"nope"}"#).is_err());
    }
}
