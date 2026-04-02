use crate::LambdaError;
use aws::RegionId;
use base64::Engine;
use serde::Serialize;
use sqs::{
    MessageAttributeValue, ReceivedMessage, SqsQueueIdentity, SqsService,
};

const DEFAULT_EVENT_SOURCE_MAPPING_BATCH_SIZE: u32 = 10;
const DEFAULT_EVENT_SOURCE_MAPPING_WINDOW_SECONDS: u32 = 0;
const MAX_EVENT_SOURCE_MAPPING_BATCH_SIZE: u32 = 10_000;
const MAX_EVENT_SOURCE_MAPPING_WINDOW_SECONDS: u32 = 300;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateEventSourceMappingInput {
    pub batch_size: Option<u32>,
    pub enabled: Option<bool>,
    pub event_source_arn: String,
    pub function_name: String,
    pub maximum_batching_window_in_seconds: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateEventSourceMappingInput {
    pub batch_size: Option<u32>,
    pub enabled: Option<bool>,
    pub maximum_batching_window_in_seconds: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct EventSourceMappingOutput {
    pub(crate) batch_size: u32,
    pub(crate) event_source_arn: String,
    pub(crate) event_source_mapping_arn: String,
    pub(crate) function_arn: String,
    pub(crate) last_modified: i64,
    pub(crate) maximum_batching_window_in_seconds: u32,
    pub(crate) state: String,
    pub(crate) state_transition_reason: String,
    #[serde(rename = "UUID")]
    pub(crate) uuid: String,
}

impl EventSourceMappingOutput {
    pub fn batch_size(&self) -> u32 {
        self.batch_size
    }

    pub fn event_source_arn(&self) -> &str {
        &self.event_source_arn
    }

    pub fn event_source_mapping_arn(&self) -> &str {
        &self.event_source_mapping_arn
    }

    pub fn function_arn(&self) -> &str {
        &self.function_arn
    }

    pub fn last_modified(&self) -> i64 {
        self.last_modified
    }

    pub fn maximum_batching_window_in_seconds(&self) -> u32 {
        self.maximum_batching_window_in_seconds
    }

    pub fn state(&self) -> &str {
        &self.state
    }

    pub fn state_transition_reason(&self) -> &str {
        &self.state_transition_reason
    }

    pub fn uuid(&self) -> &str {
        &self.uuid
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct ListEventSourceMappingsOutput {
    pub(crate) event_source_mappings: Vec<EventSourceMappingOutput>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) next_marker: Option<String>,
}

impl ListEventSourceMappingsOutput {
    pub fn event_source_mappings(&self) -> &[EventSourceMappingOutput] {
        &self.event_source_mappings
    }

    pub fn next_marker(&self) -> Option<&str> {
        self.next_marker.as_deref()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EventSourceMappingOutputInput {
    pub(crate) batch_size: u32,
    pub(crate) event_source_arn: String,
    pub(crate) event_source_mapping_arn: String,
    pub(crate) function_arn: String,
    pub(crate) last_modified: i64,
    pub(crate) maximum_batching_window_in_seconds: u32,
    pub(crate) enabled: bool,
    pub(crate) state_override: Option<String>,
    pub(crate) uuid: String,
}

pub(crate) fn event_source_mapping_output(
    input: EventSourceMappingOutputInput,
) -> EventSourceMappingOutput {
    let state = input.state_override.unwrap_or_else(|| {
        if input.enabled { "Enabled" } else { "Disabled" }.to_owned()
    });

    EventSourceMappingOutput {
        batch_size: input.batch_size,
        event_source_arn: input.event_source_arn,
        event_source_mapping_arn: input.event_source_mapping_arn,
        function_arn: input.function_arn,
        last_modified: input.last_modified,
        maximum_batching_window_in_seconds: input
            .maximum_batching_window_in_seconds,
        state,
        state_transition_reason: "USER_INITIATED".to_owned(),
        uuid: input.uuid,
    }
}

pub(crate) fn list_event_source_mappings_output(
    event_source_mappings: Vec<EventSourceMappingOutput>,
    next_marker: Option<String>,
) -> ListEventSourceMappingsOutput {
    ListEventSourceMappingsOutput { event_source_mappings, next_marker }
}

pub(crate) fn validate_event_source_mapping_batch_size(
    sqs: &SqsService,
    queue_identity: &SqsQueueIdentity,
    batch_size: Option<u32>,
) -> Result<u32, LambdaError> {
    let batch_size =
        batch_size.unwrap_or(DEFAULT_EVENT_SOURCE_MAPPING_BATCH_SIZE);
    if batch_size == 0 || batch_size > MAX_EVENT_SOURCE_MAPPING_BATCH_SIZE {
        return Err(LambdaError::InvalidParameterValue {
            message: format!(
                "BatchSize must be between 1 and {MAX_EVENT_SOURCE_MAPPING_BATCH_SIZE}."
            ),
        });
    }
    let attributes = sqs
        .get_queue_attributes(queue_identity, &["FifoQueue"])
        .map_err(|error| LambdaError::InvalidParameterValue {
            message: error.to_string(),
        })?;
    if attributes.get("FifoQueue").is_some_and(|value| value == "true")
        && batch_size > 10
    {
        return Err(LambdaError::InvalidParameterValue {
            message: "FIFO queue event source mappings support a maximum batch size of 10.".to_owned(),
        });
    }

    Ok(batch_size)
}

pub(crate) fn validate_event_source_mapping_window(
    sqs: &SqsService,
    queue_identity: &SqsQueueIdentity,
    window: Option<u32>,
    batch_size: u32,
) -> Result<u32, LambdaError> {
    let window = window.unwrap_or(DEFAULT_EVENT_SOURCE_MAPPING_WINDOW_SECONDS);
    if window > MAX_EVENT_SOURCE_MAPPING_WINDOW_SECONDS {
        return Err(LambdaError::InvalidParameterValue {
            message: format!(
                "MaximumBatchingWindowInSeconds must be between 0 and {MAX_EVENT_SOURCE_MAPPING_WINDOW_SECONDS}."
            ),
        });
    }
    let attributes = sqs
        .get_queue_attributes(queue_identity, &["FifoQueue"])
        .map_err(|error| LambdaError::InvalidParameterValue {
            message: error.to_string(),
        })?;
    let is_fifo =
        attributes.get("FifoQueue").is_some_and(|value| value == "true");
    if is_fifo && window != 0 {
        return Err(LambdaError::InvalidParameterValue {
            message:
                "FIFO queue event source mappings do not support batching windows."
                    .to_owned(),
        });
    }
    if !is_fifo && batch_size > 10 && window == 0 {
        return Err(LambdaError::InvalidParameterValue {
            message: "MaximumBatchingWindowInSeconds must be at least 1 when BatchSize is greater than 10.".to_owned(),
        });
    }

    Ok(window)
}

pub(crate) fn build_sqs_event(
    event_source_arn: &str,
    region: &RegionId,
    messages: &[ReceivedMessage],
) -> Result<Vec<u8>, LambdaError> {
    serde_json::to_vec(&serde_json::json!({
        "Records": messages.iter().map(|message| serde_json::json!({
            "messageId": message.message_id,
            "receiptHandle": message.receipt_handle,
            "body": message.body,
            "attributes": message.attributes,
            "messageAttributes": sqs_event_message_attributes(&message.message_attributes),
            "md5OfBody": message.md5_of_body,
            "eventSource": "aws:sqs",
            "eventSourceARN": event_source_arn,
            "awsRegion": region.as_str(),
        })).collect::<Vec<_>>(),
    }))
    .map_err(|error| LambdaError::Internal { message: error.to_string() })
}

fn sqs_event_message_attributes(
    attributes: &std::collections::BTreeMap<String, MessageAttributeValue>,
) -> serde_json::Value {
    serde_json::Value::Object(
        attributes
            .iter()
            .map(|(name, value)| {
                let mut attribute = serde_json::Map::from_iter([
                    (
                        "dataType".to_owned(),
                        serde_json::json!(&value.data_type),
                    ),
                    ("stringListValues".to_owned(), serde_json::json!([])),
                    ("binaryListValues".to_owned(), serde_json::json!([])),
                ]);
                if let Some(string_value) = value.string_value.as_deref() {
                    attribute.insert(
                        "stringValue".to_owned(),
                        serde_json::json!(string_value),
                    );
                }
                if let Some(binary_value) = value.binary_value.as_ref() {
                    attribute.insert(
                        "binaryValue".to_owned(),
                        serde_json::json!(
                            base64::engine::general_purpose::STANDARD
                                .encode(binary_value)
                        ),
                    );
                }

                (name.clone(), serde_json::Value::Object(attribute))
            })
            .collect(),
    )
}

#[cfg(test)]
mod tests {
    use super::{
        CreateEventSourceMappingInput, EventSourceMappingOutput,
        EventSourceMappingOutputInput, ListEventSourceMappingsOutput,
        UpdateEventSourceMappingInput, build_sqs_event,
        event_source_mapping_output, list_event_source_mappings_output,
        validate_event_source_mapping_batch_size,
        validate_event_source_mapping_window,
    };
    use crate::LambdaError;
    use aws::{AccountId, RegionId};
    use serde_json::json;
    use sqs::{
        CreateQueueInput, MessageAttributeValue, ReceivedMessage, SqsError,
        SqsQueueIdentity, SqsScope, SqsService,
    };
    use std::collections::BTreeMap;

    fn scope() -> SqsScope {
        SqsScope::new(
            "000000000000".parse::<AccountId>().unwrap(),
            "eu-west-2".parse::<RegionId>().unwrap(),
        )
    }

    fn standard_queue(
        sqs: &SqsService,
        queue_name: &str,
    ) -> Result<SqsQueueIdentity, SqsError> {
        sqs.create_queue(
            &scope(),
            CreateQueueInput {
                attributes: BTreeMap::new(),
                queue_name: queue_name.to_owned(),
            },
        )
    }

    fn fifo_queue(
        sqs: &SqsService,
        queue_name: &str,
    ) -> Result<SqsQueueIdentity, SqsError> {
        sqs.create_queue(
            &scope(),
            CreateQueueInput {
                attributes: BTreeMap::from([
                    ("FifoQueue".to_owned(), "true".to_owned()),
                    (
                        "ContentBasedDeduplication".to_owned(),
                        "true".to_owned(),
                    ),
                ]),
                queue_name: format!("{queue_name}.fifo"),
            },
        )
    }

    #[test]
    fn event_source_mapping_contract_inputs_round_trip_public_fields() {
        assert_eq!(
            CreateEventSourceMappingInput {
                batch_size: Some(5),
                enabled: Some(true),
                event_source_arn: "arn:aws:sqs:eu-west-2:000000000000:source"
                    .to_owned(),
                function_name: "demo".to_owned(),
                maximum_batching_window_in_seconds: Some(0),
            },
            CreateEventSourceMappingInput {
                batch_size: Some(5),
                enabled: Some(true),
                event_source_arn: "arn:aws:sqs:eu-west-2:000000000000:source"
                    .to_owned(),
                function_name: "demo".to_owned(),
                maximum_batching_window_in_seconds: Some(0),
            }
        );
        assert_eq!(
            UpdateEventSourceMappingInput {
                batch_size: Some(20),
                enabled: Some(false),
                maximum_batching_window_in_seconds: Some(1),
            },
            UpdateEventSourceMappingInput {
                batch_size: Some(20),
                enabled: Some(false),
                maximum_batching_window_in_seconds: Some(1),
            }
        );
    }

    #[test]
    fn event_source_mapping_output_builders_shape_aws_contract() {
        let mapping = event_source_mapping_output(
            EventSourceMappingOutputInput {
                batch_size: 5,
                event_source_arn:
                    "arn:aws:sqs:eu-west-2:000000000000:source".to_owned(),
                event_source_mapping_arn:
                    "arn:aws:lambda:eu-west-2:000000000000:event-source-mapping:uuid-1"
                        .to_owned(),
                function_arn:
                    "arn:aws:lambda:eu-west-2:000000000000:function:demo"
                        .to_owned(),
                last_modified: 60,
                maximum_batching_window_in_seconds: 0,
                enabled: true,
                state_override: Some("Creating".to_owned()),
                uuid: "uuid-1".to_owned(),
            },
        );
        let list = list_event_source_mappings_output(
            vec![mapping.clone()],
            Some("marker-1".to_owned()),
        );

        assert_eq!(mapping.batch_size(), 5);
        assert_eq!(mapping.state(), "Creating");
        assert_eq!(mapping.uuid(), "uuid-1");
        assert_eq!(
            list.event_source_mappings(),
            std::slice::from_ref(&mapping)
        );
        assert_eq!(list.next_marker(), Some("marker-1"));
        assert_eq!(
            serde_json::to_value(ListEventSourceMappingsOutput {
                event_source_mappings: vec![mapping],
                next_marker: Some("marker-1".to_owned()),
            })
            .unwrap(),
            json!({
                "EventSourceMappings": [{
                    "BatchSize": 5,
                    "EventSourceArn": "arn:aws:sqs:eu-west-2:000000000000:source",
                    "EventSourceMappingArn": "arn:aws:lambda:eu-west-2:000000000000:event-source-mapping:uuid-1",
                    "FunctionArn": "arn:aws:lambda:eu-west-2:000000000000:function:demo",
                    "LastModified": 60,
                    "MaximumBatchingWindowInSeconds": 0,
                    "State": "Creating",
                    "StateTransitionReason": "USER_INITIATED",
                    "UUID": "uuid-1"
                }],
                "NextMarker": "marker-1"
            })
        );
    }

    #[test]
    fn event_source_mapping_validation_enforces_standard_and_fifo_rules() {
        let sqs = SqsService::new();
        let standard = standard_queue(&sqs, "source").unwrap();
        let fifo = fifo_queue(&sqs, "source").unwrap();

        assert_eq!(
            validate_event_source_mapping_batch_size(&sqs, &standard, None)
                .unwrap(),
            10
        );
        assert_eq!(
            validate_event_source_mapping_window(&sqs, &standard, Some(1), 20)
                .unwrap(),
            1
        );

        let invalid_standard_batch =
            validate_event_source_mapping_batch_size(&sqs, &standard, Some(0))
                .expect_err("batch size must be at least one");
        match invalid_standard_batch {
            LambdaError::InvalidParameterValue { message } => {
                assert_eq!(message, "BatchSize must be between 1 and 10000.");
            }
            other => panic!("expected invalid parameter value, got {other:?}"),
        }

        let invalid_standard_window = validate_event_source_mapping_window(
            &sqs,
            &standard,
            Some(0),
            20,
        )
        .expect_err("standard queues need a batching window above zero when batch size exceeds ten");
        match invalid_standard_window {
            LambdaError::InvalidParameterValue { message } => {
                assert_eq!(
                    message,
                    "MaximumBatchingWindowInSeconds must be at least 1 when BatchSize is greater than 10."
                );
            }
            other => panic!("expected invalid parameter value, got {other:?}"),
        }

        let invalid_fifo_batch =
            validate_event_source_mapping_batch_size(&sqs, &fifo, Some(11))
                .expect_err("fifo queues cap batch size at ten");
        match invalid_fifo_batch {
            LambdaError::InvalidParameterValue { message } => {
                assert_eq!(
                    message,
                    "FIFO queue event source mappings support a maximum batch size of 10."
                );
            }
            other => panic!("expected invalid parameter value, got {other:?}"),
        }

        let invalid_fifo_window =
            validate_event_source_mapping_window(&sqs, &fifo, Some(1), 10)
                .expect_err("fifo queues do not support batching windows");
        match invalid_fifo_window {
            LambdaError::InvalidParameterValue { message } => {
                assert_eq!(
                    message,
                    "FIFO queue event source mappings do not support batching windows."
                );
            }
            other => panic!("expected invalid parameter value, got {other:?}"),
        }
    }

    #[test]
    fn build_sqs_event_shapes_lambda_sqs_records() {
        let event = build_sqs_event(
            "arn:aws:sqs:eu-west-2:000000000000:source",
            &"eu-west-2".parse::<RegionId>().unwrap(),
            &[ReceivedMessage {
                attributes: BTreeMap::from([(
                    "ApproximateReceiveCount".to_owned(),
                    "1".to_owned(),
                )]),
                body: r#"{"job":"run"}"#.to_owned(),
                md5_of_body: "abc".to_owned(),
                md5_of_message_attributes: None,
                message_attributes: BTreeMap::from([
                    (
                        "store".to_owned(),
                        MessageAttributeValue {
                            binary_list_values: Vec::new(),
                            binary_value: None,
                            data_type: "String".to_owned(),
                            string_list_values: Vec::new(),
                            string_value: Some("eu-west".to_owned()),
                        },
                    ),
                    (
                        "blob".to_owned(),
                        MessageAttributeValue {
                            binary_list_values: Vec::new(),
                            binary_value: Some(vec![0x01, 0x02]),
                            data_type: "Binary".to_owned(),
                            string_list_values: Vec::new(),
                            string_value: None,
                        },
                    ),
                ]),
                message_id: "m-1".to_owned(),
                receipt_handle: "r-1".to_owned(),
            }],
        )
        .unwrap();

        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&event).unwrap(),
            json!({
                "Records": [{
                    "messageId": "m-1",
                    "receiptHandle": "r-1",
                    "body": "{\"job\":\"run\"}",
                    "attributes": {
                        "ApproximateReceiveCount": "1"
                    },
                    "messageAttributes": {
                        "blob": {
                            "binaryListValues": [],
                            "binaryValue": "AQI=",
                            "dataType": "Binary",
                            "stringListValues": []
                        },
                        "store": {
                            "dataType": "String",
                            "stringListValues": [],
                            "binaryListValues": [],
                            "stringValue": "eu-west"
                        }
                    },
                    "md5OfBody": "abc",
                    "eventSource": "aws:sqs",
                    "eventSourceARN": "arn:aws:sqs:eu-west-2:000000000000:source",
                    "awsRegion": "eu-west-2"
                }]
            })
        );
        let _: EventSourceMappingOutput = event_source_mapping_output(
            EventSourceMappingOutputInput {
                batch_size: 1,
                event_source_arn:
                    "arn:aws:sqs:eu-west-2:000000000000:source".to_owned(),
                event_source_mapping_arn:
                    "arn:aws:lambda:eu-west-2:000000000000:event-source-mapping:uuid-2"
                        .to_owned(),
                function_arn:
                    "arn:aws:lambda:eu-west-2:000000000000:function:demo"
                        .to_owned(),
                last_modified: 61,
                maximum_batching_window_in_seconds: 0,
                enabled: false,
                state_override: None,
                uuid: "uuid-2".to_owned(),
            },
        );
    }
}
