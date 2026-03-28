use crate::errors::EventBridgeError;
use crate::scope::EventBridgeScope;
use crate::targets::EventBridgeTarget;
use aws::{Arn, Partition, ServiceName};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

const MAX_EVENT_ENTRY_SIZE_BYTES: usize = 256 * 1024;
const MAX_PUT_EVENTS_ENTRIES: usize = 10;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct PutEventsRequestEntry {
    pub detail: String,
    pub detail_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_bus_name: Option<String>,
    #[serde(default)]
    pub resources: Vec<String>,
    pub source: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trace_header: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct PutEventsInput {
    pub entries: Vec<PutEventsRequestEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct PutEventsResultEntry {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct PutEventsOutput {
    pub entries: Vec<PutEventsResultEntry>,
    pub failed_entry_count: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventBridgePlannedDelivery {
    pub payload: Vec<u8>,
    pub rule_arn: Arn,
    pub target: EventBridgeTarget,
}

pub trait EventBridgeDeliveryDispatcher: Send + Sync {
    /// # Errors
    ///
    /// Returns an error when the configured target cannot be accepted for the
    /// given rule in the supplied scope.
    fn validate_target(
        &self,
        scope: &EventBridgeScope,
        rule_arn: &Arn,
        target: &EventBridgeTarget,
    ) -> Result<(), EventBridgeError>;

    fn dispatch(&self, deliveries: Vec<EventBridgePlannedDelivery>);
}

#[derive(Debug, Default)]
pub struct NoopEventBridgeDeliveryDispatcher;

impl EventBridgeDeliveryDispatcher for NoopEventBridgeDeliveryDispatcher {
    fn validate_target(
        &self,
        _scope: &EventBridgeScope,
        _rule_arn: &Arn,
        _target: &EventBridgeTarget,
    ) -> Result<(), EventBridgeError> {
        Ok(())
    }

    fn dispatch(&self, _deliveries: Vec<EventBridgePlannedDelivery>) {}
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ValidatedPutEventsEntry {
    pub(crate) detail: Value,
    pub(crate) detail_type: String,
    pub(crate) event_bus_name: Option<String>,
    pub(crate) resources: Vec<String>,
    pub(crate) source: String,
    pub(crate) time: Option<String>,
}

pub(crate) fn default_time(now: SystemTime) -> String {
    let seconds = now
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or_default();

    OffsetDateTime::from_unix_timestamp(seconds as i64)
        .unwrap_or(OffsetDateTime::UNIX_EPOCH)
        .format(&Rfc3339)
        .unwrap_or_else(|_| "1970-01-01T00:00:00Z".to_owned())
}

pub(crate) fn validate_put_events_input(
    input: &PutEventsInput,
) -> Result<(), EventBridgeError> {
    if input.entries.is_empty() {
        return Err(validation("Entries must not be empty."));
    }
    if input.entries.len() > MAX_PUT_EVENTS_ENTRIES {
        return Err(validation("Entries must contain at most 10 items."));
    }

    Ok(())
}

pub(crate) fn validate_put_events_entry(
    entry: PutEventsRequestEntry,
) -> Result<ValidatedPutEventsEntry, PutEventsResultEntry> {
    if entry.source.trim().is_empty() {
        return Err(put_events_failure(
            "ValidationException",
            "Source is required.",
        ));
    }
    if entry.detail_type.trim().is_empty() {
        return Err(put_events_failure(
            "ValidationException",
            "DetailType is required.",
        ));
    }
    if entry.detail.trim().is_empty() {
        return Err(put_events_failure(
            "ValidationException",
            "Detail is required.",
        ));
    }
    if entry.detail.len() > MAX_EVENT_ENTRY_SIZE_BYTES {
        return Err(put_events_failure(
            "ValidationException",
            "Detail exceeds the maximum supported size.",
        ));
    }

    let detail =
        serde_json::from_str::<Value>(&entry.detail).map_err(|error| {
            put_events_failure(
                "ValidationException",
                &format!("Detail is not valid JSON: {error}"),
            )
        })?;

    Ok(ValidatedPutEventsEntry {
        detail,
        detail_type: entry.detail_type,
        event_bus_name: entry.event_bus_name,
        resources: entry.resources,
        source: entry.source,
        time: entry.time,
    })
}

pub(crate) fn put_events_success(event_id: String) -> PutEventsResultEntry {
    PutEventsResultEntry {
        error_code: None,
        error_message: None,
        event_id: Some(event_id),
    }
}

pub(crate) fn put_events_failure(
    code: &str,
    message: &str,
) -> PutEventsResultEntry {
    PutEventsResultEntry {
        error_code: Some(code.to_owned()),
        error_message: Some(message.to_owned()),
        event_id: None,
    }
}

pub(crate) fn put_events_output(
    entries: Vec<PutEventsResultEntry>,
) -> PutEventsOutput {
    let failed_entry_count =
        entries.iter().filter(|entry| entry.event_id.is_none()).count() as i32;

    PutEventsOutput { entries, failed_entry_count }
}

pub(crate) fn event_payload(
    scope: &EventBridgeScope,
    event_id: &str,
    bus_name: &str,
    entry: &ValidatedPutEventsEntry,
    default_timestamp: &str,
) -> Value {
    json!({
        "version": "0",
        "id": event_id,
        "detail-type": entry.detail_type,
        "source": entry.source,
        "account": scope.account_id(),
        "time": entry.time.clone().unwrap_or_else(|| default_timestamp.to_owned()),
        "region": scope.region(),
        "resources": entry.resources,
        "detail": entry.detail,
        "eventBusName": bus_name,
    })
}

pub(crate) fn scheduled_event_payload(
    scope: &EventBridgeScope,
    event_id: &str,
    rule_arn: &Arn,
    default_timestamp: &str,
) -> Value {
    json!({
        "version": "0",
        "id": event_id,
        "detail-type": "Scheduled Event",
        "source": "aws.events",
        "account": scope.account_id(),
        "time": default_timestamp,
        "region": scope.region(),
        "resources": [rule_arn],
        "detail": {},
        "eventBusName": "default",
    })
}

pub(crate) fn rule_arn(
    scope: &EventBridgeScope,
    event_bus_name: &str,
    rule_name: &str,
) -> Arn {
    Arn::trusted_new(
        Partition::aws(),
        ServiceName::EventBridge,
        Some(scope.region().clone()),
        Some(scope.account_id().clone()),
        aws::ArnResource::Generic(format!(
            "rule/{event_bus_name}/{rule_name}"
        )),
    )
}

pub(crate) fn dispatcher(
    dispatcher: Arc<dyn EventBridgeDeliveryDispatcher>,
) -> Arc<dyn EventBridgeDeliveryDispatcher> {
    dispatcher
}

fn validation(message: &str) -> EventBridgeError {
    EventBridgeError::Validation { message: message.to_owned() }
}

#[cfg(test)]
mod tests {
    use super::{
        PutEventsInput, PutEventsRequestEntry, default_time, event_payload,
        put_events_output, put_events_success, scheduled_event_payload,
        validate_put_events_entry, validate_put_events_input,
    };
    use crate::scope::EventBridgeScope;
    use std::time::UNIX_EPOCH;

    fn scope() -> EventBridgeScope {
        EventBridgeScope::new(
            "000000000000".parse().expect("account should parse"),
            "eu-west-2".parse().expect("region should parse"),
        )
    }

    #[test]
    fn put_events_validation_rejects_invalid_request_sizes_and_entries() {
        assert!(
            validate_put_events_input(&PutEventsInput { entries: Vec::new() })
                .is_err()
        );
        assert!(
            validate_put_events_entry(PutEventsRequestEntry {
                detail: String::new(),
                detail_type: "Created".to_owned(),
                event_bus_name: None,
                resources: Vec::new(),
                source: "orders".to_owned(),
                time: None,
                trace_header: None,
            })
            .is_err()
        );
    }

    #[test]
    fn put_events_helpers_shape_payloads_and_fail_counts() {
        let entry = validate_put_events_entry(PutEventsRequestEntry {
            detail: "{\"kind\":\"created\"}".to_owned(),
            detail_type: "Created".to_owned(),
            event_bus_name: Some("custom".to_owned()),
            resources: vec![
                "arn:aws:sns:eu-west-2:000000000000:orders".to_owned(),
            ],
            source: "orders".to_owned(),
            time: Some("2024-01-01T00:00:00Z".to_owned()),
            trace_header: None,
        })
        .expect("entry should validate");
        let payload =
            event_payload(&scope(), "evt-1", "custom", &entry, "fallback");
        assert_eq!(payload["source"], "orders");
        assert_eq!(payload["eventBusName"], "custom");

        let scheduled = scheduled_event_payload(
            &scope(),
            "evt-2",
            &"arn:aws:events:eu-west-2:000000000000:rule/default/nightly"
                .parse()
                .expect("rule arn should parse"),
            "2024-01-01T00:00:00Z",
        );
        assert_eq!(scheduled["source"], "aws.events");

        let output = put_events_output(vec![
            put_events_success("evt-1".to_owned()),
            super::put_events_failure("ValidationException", "bad"),
        ]);
        assert_eq!(output.failed_entry_count, 1);
        assert_eq!(default_time(UNIX_EPOCH), "1970-01-01T00:00:00Z");
    }
}
