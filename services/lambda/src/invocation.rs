use crate::LambdaError;
use aws::{ExecuteApiSourceArn, LambdaFunctionTarget};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiGatewayInvokeInput {
    pub payload: Vec<u8>,
    pub source_arn: ExecuteApiSourceArn,
    pub target: LambdaFunctionTarget,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LambdaInvocationType {
    DryRun,
    Event,
    RequestResponse,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvokeInput {
    pub invocation_type: LambdaInvocationType,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvokeOutput {
    executed_version: String,
    function_error: Option<String>,
    payload: Vec<u8>,
    status_code: u16,
}

impl InvokeOutput {
    pub fn executed_version(&self) -> &str {
        &self.executed_version
    }

    pub fn function_error(&self) -> Option<&str> {
        self.function_error.as_deref()
    }

    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    pub fn status_code(&self) -> u16 {
        self.status_code
    }
}

pub(crate) fn invoke_output(
    executed_version: String,
    function_error: Option<String>,
    payload: Vec<u8>,
    status_code: u16,
) -> InvokeOutput {
    InvokeOutput { executed_version, function_error, payload, status_code }
}

pub(crate) fn validate_invoke_payload(
    input: &InvokeInput,
    max_event_payload_bytes: usize,
    max_request_response_payload_bytes: usize,
) -> Result<(), LambdaError> {
    let max_payload = match input.invocation_type {
        LambdaInvocationType::DryRun
        | LambdaInvocationType::RequestResponse => {
            max_request_response_payload_bytes
        }
        LambdaInvocationType::Event => max_event_payload_bytes,
    };
    if input.payload.len() > max_payload {
        return Err(LambdaError::RequestTooLarge {
            message: "The request payload exceeded the Invoke request body JSON input quota.".to_owned(),
        });
    }

    if matches!(input.invocation_type, LambdaInvocationType::DryRun) {
        return Ok(());
    }

    if !input.payload.is_empty()
        && serde_json::from_slice::<serde_json::Value>(&input.payload).is_err()
    {
        return Err(LambdaError::InvalidRequestContent {
            message: "The request body could not be parsed as JSON."
                .to_owned(),
        });
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        InvokeInput, LambdaInvocationType, invoke_output,
        validate_invoke_payload,
    };
    use crate::LambdaError;

    #[test]
    fn invoke_output_getters_round_trip_response_shape() {
        let output = invoke_output(
            "1".to_owned(),
            Some("Unhandled".to_owned()),
            br#"{"ok":true}"#.to_vec(),
            200,
        );

        assert_eq!(output.executed_version(), "1");
        assert_eq!(output.function_error(), Some("Unhandled"));
        assert_eq!(output.payload(), br#"{"ok":true}"#);
        assert_eq!(output.status_code(), 200);
    }

    #[test]
    fn dry_run_payload_validation_skips_json_parsing_but_enforces_limit() {
        assert!(
            validate_invoke_payload(
                &InvokeInput {
                    invocation_type: LambdaInvocationType::DryRun,
                    payload: b"not-json".to_vec(),
                },
                4,
                16,
            )
            .is_ok()
        );

        let too_large = validate_invoke_payload(
            &InvokeInput {
                invocation_type: LambdaInvocationType::DryRun,
                payload: vec![0; 17],
            },
            4,
            16,
        )
        .expect_err(
            "dry run requests must still respect the request size limit",
        );
        match too_large {
            LambdaError::RequestTooLarge { message } => {
                assert_eq!(
                    message,
                    "The request payload exceeded the Invoke request body JSON input quota."
                );
            }
            other => panic!("expected request too large error, got {other:?}"),
        }
    }

    #[test]
    fn invoke_payload_validation_uses_mode_specific_limits_and_json_contract()
    {
        assert!(
            validate_invoke_payload(
                &InvokeInput {
                    invocation_type: LambdaInvocationType::Event,
                    payload: br#"{"async":true}"#.to_vec(),
                },
                32,
                64,
            )
            .is_ok()
        );

        let invalid_json = validate_invoke_payload(
            &InvokeInput {
                invocation_type: LambdaInvocationType::RequestResponse,
                payload: b"nope".to_vec(),
            },
            32,
            64,
        )
        .expect_err("non-dry-run payloads must be valid JSON");
        match invalid_json {
            LambdaError::InvalidRequestContent { message } => {
                assert_eq!(
                    message,
                    "The request body could not be parsed as JSON."
                );
            }
            other => panic!("expected invalid request content, got {other:?}"),
        }

        let event_too_large = validate_invoke_payload(
            &InvokeInput {
                invocation_type: LambdaInvocationType::Event,
                payload: vec![0; 33],
            },
            32,
            64,
        )
        .expect_err("event payloads must use the smaller async limit");
        match event_too_large {
            LambdaError::RequestTooLarge { message } => {
                assert_eq!(
                    message,
                    "The request payload exceeded the Invoke request body JSON input quota."
                );
            }
            other => panic!("expected request too large error, got {other:?}"),
        }
    }
}
