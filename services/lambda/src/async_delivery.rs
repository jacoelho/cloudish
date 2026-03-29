use crate::LambdaError;
use serde::Serialize;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

pub(crate) const DEFAULT_ASYNC_MAX_EVENT_AGE_SECONDS: u32 = 21_600;
pub(crate) const DEFAULT_ASYNC_MAX_RETRY_ATTEMPTS: u32 = 2;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DestinationTargetInput {
    pub destination: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DestinationConfigInput {
    pub on_failure: Option<DestinationTargetInput>,
    pub on_success: Option<DestinationTargetInput>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PutFunctionEventInvokeConfigInput {
    pub destination_config: Option<DestinationConfigInput>,
    pub maximum_event_age_in_seconds: Option<u32>,
    pub maximum_retry_attempts: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateFunctionEventInvokeConfigInput {
    pub destination_config: Option<DestinationConfigInput>,
    pub maximum_event_age_in_seconds: Option<u32>,
    pub maximum_retry_attempts: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct FunctionEventInvokeConfigOutput {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) destination_config: Option<DestinationConfigOutput>,
    pub(crate) function_arn: String,
    pub(crate) last_modified: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) maximum_event_age_in_seconds: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) maximum_retry_attempts: Option<u32>,
}

impl FunctionEventInvokeConfigOutput {
    pub fn destination_config(&self) -> Option<&DestinationConfigOutput> {
        self.destination_config.as_ref()
    }

    pub fn function_arn(&self) -> &str {
        &self.function_arn
    }

    pub fn last_modified(&self) -> i64 {
        self.last_modified
    }

    pub fn maximum_event_age_in_seconds(&self) -> Option<u32> {
        self.maximum_event_age_in_seconds
    }

    pub fn maximum_retry_attempts(&self) -> Option<u32> {
        self.maximum_retry_attempts
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct ListFunctionEventInvokeConfigsOutput {
    pub(crate) function_event_invoke_configs:
        Vec<FunctionEventInvokeConfigOutput>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) next_marker: Option<String>,
}

impl ListFunctionEventInvokeConfigsOutput {
    pub fn function_event_invoke_configs(
        &self,
    ) -> &[FunctionEventInvokeConfigOutput] {
        &self.function_event_invoke_configs
    }

    pub fn next_marker(&self) -> Option<&str> {
        self.next_marker.as_deref()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct DestinationConfigOutput {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) on_failure: Option<DestinationTargetOutput>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) on_success: Option<DestinationTargetOutput>,
}

impl DestinationConfigOutput {
    pub fn on_failure(&self) -> Option<&DestinationTargetOutput> {
        self.on_failure.as_ref()
    }

    pub fn on_success(&self) -> Option<&DestinationTargetOutput> {
        self.on_success.as_ref()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct DestinationTargetOutput {
    pub(crate) destination: String,
}

impl DestinationTargetOutput {
    pub fn destination(&self) -> &str {
        &self.destination
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct AsyncDestinationBodyInput<'a> {
    pub(crate) request_id: &'a str,
    pub(crate) request_payload: &'a [u8],
    pub(crate) function_arn: &'a str,
    pub(crate) condition: &'a str,
    pub(crate) approximate_invoke_count: u32,
    pub(crate) status_code: u16,
    pub(crate) executed_version: &'a str,
    pub(crate) function_error: Option<&'a str>,
    pub(crate) response_payload: &'a [u8],
    pub(crate) now_epoch_seconds: i64,
}

pub(crate) fn validate_event_invoke_config_input(
    destination_config: &Option<DestinationConfigInput>,
    maximum_event_age_in_seconds: Option<u32>,
    maximum_retry_attempts: Option<u32>,
) -> Result<(), LambdaError> {
    if destination_config.is_none()
        && maximum_event_age_in_seconds.is_none()
        && maximum_retry_attempts.is_none()
    {
        return Err(LambdaError::InvalidParameterValue {
            message: "At least one async invoke configuration field must be specified.".to_owned(),
        });
    }

    Ok(())
}

pub(crate) fn validate_maximum_event_age(
    value: Option<u32>,
) -> Result<Option<u32>, LambdaError> {
    let Some(value) = value else {
        return Ok(None);
    };
    if !(60..=21_600).contains(&value) {
        return Err(LambdaError::InvalidParameterValue {
            message: "MaximumEventAgeInSeconds must be between 60 and 21600."
                .to_owned(),
        });
    }

    Ok(Some(value))
}

pub(crate) fn validate_maximum_retry_attempts(
    value: Option<u32>,
) -> Result<Option<u32>, LambdaError> {
    let Some(value) = value else {
        return Ok(None);
    };
    if value > 2 {
        return Err(LambdaError::InvalidParameterValue {
            message: "MaximumRetryAttempts must be between 0 and 2."
                .to_owned(),
        });
    }

    Ok(Some(value))
}

pub(crate) fn destination_config_output(
    on_failure: Option<&str>,
    on_success: Option<&str>,
) -> DestinationConfigOutput {
    DestinationConfigOutput {
        on_failure: on_failure.map(|destination| DestinationTargetOutput {
            destination: destination.to_owned(),
        }),
        on_success: on_success.map(|destination| DestinationTargetOutput {
            destination: destination.to_owned(),
        }),
    }
}

pub(crate) fn function_event_invoke_config_output(
    destination_config: Option<DestinationConfigOutput>,
    function_arn: String,
    last_modified: i64,
    maximum_event_age_in_seconds: Option<u32>,
    maximum_retry_attempts: Option<u32>,
) -> FunctionEventInvokeConfigOutput {
    FunctionEventInvokeConfigOutput {
        destination_config,
        function_arn,
        last_modified,
        maximum_event_age_in_seconds,
        maximum_retry_attempts,
    }
}

pub(crate) fn list_function_event_invoke_configs_output(
    function_event_invoke_configs: Vec<FunctionEventInvokeConfigOutput>,
    next_marker: Option<String>,
) -> ListFunctionEventInvokeConfigsOutput {
    ListFunctionEventInvokeConfigsOutput {
        function_event_invoke_configs,
        next_marker,
    }
}

pub(crate) fn build_async_destination_body(
    input: AsyncDestinationBodyInput<'_>,
) -> Result<serde_json::Value, LambdaError> {
    let timestamp =
        OffsetDateTime::from_unix_timestamp(input.now_epoch_seconds)
            .map_err(|error| LambdaError::Internal {
                message: error.to_string(),
            })?
            .format(&Rfc3339)
            .map_err(|error| LambdaError::Internal {
                message: error.to_string(),
            })?;

    Ok(serde_json::json!({
        "version": "1.0",
        "timestamp": timestamp,
        "requestContext": {
            "requestId": input.request_id,
            "functionArn": input.function_arn,
            "condition": input.condition,
            "approximateInvokeCount": input.approximate_invoke_count,
        },
        "requestPayload": json_or_string_value(input.request_payload),
        "responseContext": {
            "statusCode": input.status_code,
            "executedVersion": input.executed_version,
            "functionError": input.function_error,
        },
        "responsePayload": json_or_string_value(input.response_payload),
    }))
}

pub(crate) fn json_or_string_value(payload: &[u8]) -> serde_json::Value {
    serde_json::from_slice(payload).unwrap_or_else(|_| {
        serde_json::Value::String(
            String::from_utf8_lossy(payload).into_owned(),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::{
        AsyncDestinationBodyInput, DestinationConfigInput,
        DestinationTargetInput, ListFunctionEventInvokeConfigsOutput,
        PutFunctionEventInvokeConfigInput,
        UpdateFunctionEventInvokeConfigInput, build_async_destination_body,
        destination_config_output, function_event_invoke_config_output,
        json_or_string_value, list_function_event_invoke_configs_output,
        validate_event_invoke_config_input, validate_maximum_event_age,
        validate_maximum_retry_attempts,
    };
    use crate::LambdaError;
    use serde_json::json;

    #[test]
    fn async_delivery_contract_inputs_round_trip_public_fields() {
        let destination_config = DestinationConfigInput {
            on_failure: Some(DestinationTargetInput {
                destination: "arn:aws:sqs:eu-west-2:000000000000:failure"
                    .to_owned(),
            }),
            on_success: None,
        };

        assert_eq!(
            PutFunctionEventInvokeConfigInput {
                destination_config: Some(destination_config.clone()),
                maximum_event_age_in_seconds: Some(120),
                maximum_retry_attempts: Some(1),
            },
            PutFunctionEventInvokeConfigInput {
                destination_config: Some(destination_config.clone()),
                maximum_event_age_in_seconds: Some(120),
                maximum_retry_attempts: Some(1),
            }
        );
        assert_eq!(
            UpdateFunctionEventInvokeConfigInput {
                destination_config: Some(destination_config),
                maximum_event_age_in_seconds: Some(120),
                maximum_retry_attempts: Some(1),
            },
            UpdateFunctionEventInvokeConfigInput {
                destination_config: Some(DestinationConfigInput {
                    on_failure: Some(DestinationTargetInput {
                        destination:
                            "arn:aws:sqs:eu-west-2:000000000000:failure"
                                .to_owned(),
                    }),
                    on_success: None,
                }),
                maximum_event_age_in_seconds: Some(120),
                maximum_retry_attempts: Some(1),
            }
        );
    }

    #[test]
    fn async_delivery_validation_enforces_present_and_bounded_fields() {
        let missing = validate_event_invoke_config_input(&None, None, None)
            .expect_err("at least one async config field is required");
        match missing {
            LambdaError::InvalidParameterValue { message } => {
                assert_eq!(
                    message,
                    "At least one async invoke configuration field must be specified."
                );
            }
            other => panic!("expected invalid parameter value, got {other:?}"),
        }

        assert!(
            validate_event_invoke_config_input(
                &Some(DestinationConfigInput {
                    on_failure: None,
                    on_success: Some(DestinationTargetInput {
                        destination:
                            "arn:aws:sqs:eu-west-2:000000000000:success"
                                .to_owned(),
                    }),
                }),
                None,
                None,
            )
            .is_ok()
        );

        assert_eq!(validate_maximum_event_age(Some(60)).unwrap(), Some(60));
        assert_eq!(validate_maximum_retry_attempts(Some(2)).unwrap(), Some(2));

        let invalid_age = validate_maximum_event_age(Some(59))
            .expect_err("event age below 60 must be rejected");
        match invalid_age {
            LambdaError::InvalidParameterValue { message } => {
                assert_eq!(
                    message,
                    "MaximumEventAgeInSeconds must be between 60 and 21600."
                );
            }
            other => panic!("expected invalid parameter value, got {other:?}"),
        }

        let invalid_retries = validate_maximum_retry_attempts(Some(3))
            .expect_err("retry attempts above two must be rejected");
        match invalid_retries {
            LambdaError::InvalidParameterValue { message } => {
                assert_eq!(
                    message,
                    "MaximumRetryAttempts must be between 0 and 2."
                );
            }
            other => panic!("expected invalid parameter value, got {other:?}"),
        }
    }

    #[test]
    fn async_delivery_output_builders_shape_aws_contract() {
        let config = destination_config_output(
            Some("arn:aws:sqs:eu-west-2:000000000000:failure"),
            Some("arn:aws:sqs:eu-west-2:000000000000:success"),
        );
        let output = function_event_invoke_config_output(
            Some(config),
            "arn:aws:lambda:eu-west-2:000000000000:function:demo:$LATEST"
                .to_owned(),
            60,
            Some(120),
            Some(1),
        );
        let list = list_function_event_invoke_configs_output(
            vec![output.clone()],
            Some("marker-1".to_owned()),
        );

        assert_eq!(
            output
                .destination_config()
                .unwrap()
                .on_failure()
                .unwrap()
                .destination(),
            "arn:aws:sqs:eu-west-2:000000000000:failure"
        );
        assert_eq!(
            output.function_arn(),
            "arn:aws:lambda:eu-west-2:000000000000:function:demo:$LATEST"
        );
        assert_eq!(output.last_modified(), 60);
        assert_eq!(output.maximum_event_age_in_seconds(), Some(120));
        assert_eq!(output.maximum_retry_attempts(), Some(1));
        assert_eq!(
            list.function_event_invoke_configs(),
            std::slice::from_ref(&output)
        );
        assert_eq!(list.next_marker(), Some("marker-1"));
        assert_eq!(
            serde_json::to_value(ListFunctionEventInvokeConfigsOutput {
                function_event_invoke_configs: vec![output],
                next_marker: Some("marker-1".to_owned()),
            })
            .unwrap(),
            json!({
                "FunctionEventInvokeConfigs": [{
                    "DestinationConfig": {
                        "OnFailure": {
                            "Destination": "arn:aws:sqs:eu-west-2:000000000000:failure"
                        },
                        "OnSuccess": {
                            "Destination": "arn:aws:sqs:eu-west-2:000000000000:success"
                        }
                    },
                    "FunctionArn": "arn:aws:lambda:eu-west-2:000000000000:function:demo:$LATEST",
                    "LastModified": 60,
                    "MaximumEventAgeInSeconds": 120,
                    "MaximumRetryAttempts": 1
                }],
                "NextMarker": "marker-1"
            })
        );
    }

    #[test]
    fn async_destination_body_shapes_json_or_string_payloads() {
        let event = build_async_destination_body(AsyncDestinationBodyInput {
            request_id: "request-1",
            request_payload: br#"{"job":"run"}"#,
            function_arn:
                "arn:aws:lambda:eu-west-2:000000000000:function:demo:1",
            condition: "RetriesExhausted",
            approximate_invoke_count: 3,
            status_code: 500,
            executed_version: "1",
            function_error: Some("Unhandled"),
            response_payload: b"not-json",
            now_epoch_seconds: 60,
        })
        .unwrap();

        assert_eq!(
            event,
            json!({
                "version": "1.0",
                "timestamp": "1970-01-01T00:01:00Z",
                "requestContext": {
                    "requestId": "request-1",
                    "functionArn": "arn:aws:lambda:eu-west-2:000000000000:function:demo:1",
                    "condition": "RetriesExhausted",
                    "approximateInvokeCount": 3
                },
                "requestPayload": { "job": "run" },
                "responseContext": {
                    "statusCode": 500,
                    "executedVersion": "1",
                    "functionError": "Unhandled"
                },
                "responsePayload": "not-json"
            })
        );
        assert_eq!(json_or_string_value(b"oops"), json!("oops"));
    }
}
