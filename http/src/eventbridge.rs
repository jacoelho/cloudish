pub(crate) use crate::aws_error_shape::AwsErrorShape;
use crate::request::HttpRequest;
use aws::{AwsError, RequestContext};
use serde::Deserialize;
use serde::Serialize;
use serde::de::DeserializeOwned;
use services::{
    CreateEventBusInput, EventBridgeError, EventBridgeScope,
    EventBridgeService, ListEventBusesInput, ListRulesInput,
    ListTargetsByRuleInput, PutEventsInput, PutRuleInput, PutTargetsInput,
    RemoveTargetsInput,
};

pub(crate) fn handle_json(
    eventbridge: &EventBridgeService,
    request: &HttpRequest<'_>,
    context: &RequestContext,
) -> Result<Vec<u8>, AwsError> {
    let operation = operation_from_target(request.header("x-amz-target"))
        .map_err(|error| error.to_aws_error())?;
    let scope = EventBridgeScope::new(
        context.account_id().clone(),
        context.region().clone(),
    );

    match operation {
        EventBridgeOperation::CreateEventBus => {
            let input =
                parse_json_body::<CreateEventBusInput>(request.body())?;
            let output = eventbridge
                .create_event_bus(&scope, input)
                .map_err(|error| error.to_aws_error())?;
            json_response(&serde_json::json!({ "EventBusArn": output.arn }))
        }
        EventBridgeOperation::DeleteEventBus => {
            let input = parse_json_body::<NamedBusRequest>(request.body())?;
            eventbridge
                .delete_event_bus(&scope, &input.name)
                .map_err(|error| error.to_aws_error())?;
            json_response(&serde_json::json!({}))
        }
        EventBridgeOperation::DescribeEventBus => {
            let input =
                parse_json_body::<OptionalNamedBusRequest>(request.body())?;
            let output = eventbridge
                .describe_event_bus(&scope, input.name.as_deref())
                .map_err(|error| error.to_aws_error())?;
            json_response(&output)
        }
        EventBridgeOperation::ListEventBuses => {
            let input =
                parse_json_body::<ListEventBusesInput>(request.body())?;
            let output = eventbridge
                .list_event_buses(&scope, input)
                .map_err(|error| error.to_aws_error())?;
            json_response(&output)
        }
        EventBridgeOperation::PutRule => {
            let input = parse_json_body::<PutRuleInput>(request.body())?;
            let output = eventbridge
                .put_rule(&scope, input)
                .map_err(|error| error.to_aws_error())?;
            json_response(&output)
        }
        EventBridgeOperation::DescribeRule => {
            let input =
                parse_json_body::<RuleIdentityRequest>(request.body())?;
            let output = eventbridge
                .describe_rule(
                    &scope,
                    &input.name,
                    input.event_bus_name.as_deref(),
                )
                .map_err(|error| error.to_aws_error())?;
            json_response(&output)
        }
        EventBridgeOperation::ListRules => {
            let input = parse_json_body::<ListRulesInput>(request.body())?;
            let output = eventbridge
                .list_rules(&scope, input)
                .map_err(|error| error.to_aws_error())?;
            json_response(&output)
        }
        EventBridgeOperation::EnableRule => {
            let input =
                parse_json_body::<RuleIdentityRequest>(request.body())?;
            eventbridge
                .enable_rule(
                    &scope,
                    &input.name,
                    input.event_bus_name.as_deref(),
                )
                .map_err(|error| error.to_aws_error())?;
            json_response(&serde_json::json!({}))
        }
        EventBridgeOperation::DisableRule => {
            let input =
                parse_json_body::<RuleIdentityRequest>(request.body())?;
            eventbridge
                .disable_rule(
                    &scope,
                    &input.name,
                    input.event_bus_name.as_deref(),
                )
                .map_err(|error| error.to_aws_error())?;
            json_response(&serde_json::json!({}))
        }
        EventBridgeOperation::DeleteRule => {
            let input =
                parse_json_body::<RuleIdentityRequest>(request.body())?;
            eventbridge
                .delete_rule(
                    &scope,
                    &input.name,
                    input.event_bus_name.as_deref(),
                )
                .map_err(|error| error.to_aws_error())?;
            json_response(&serde_json::json!({}))
        }
        EventBridgeOperation::PutTargets => {
            let input = parse_json_body::<PutTargetsInput>(request.body())?;
            let output = eventbridge
                .put_targets(&scope, input)
                .map_err(|error| error.to_aws_error())?;
            json_response(&output)
        }
        EventBridgeOperation::ListTargetsByRule => {
            let input =
                parse_json_body::<ListTargetsByRuleInput>(request.body())?;
            let output = eventbridge
                .list_targets_by_rule(&scope, input)
                .map_err(|error| error.to_aws_error())?;
            json_response(&output)
        }
        EventBridgeOperation::RemoveTargets => {
            let input = parse_json_body::<RemoveTargetsInput>(request.body())?;
            let output = eventbridge
                .remove_targets(&scope, input)
                .map_err(|error| error.to_aws_error())?;
            json_response(&output)
        }
        EventBridgeOperation::PutEvents => {
            let input = parse_json_body::<PutEventsInput>(request.body())?;
            let output = eventbridge
                .put_events(&scope, input)
                .map_err(|error| error.to_aws_error())?;
            json_response(&output)
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct NamedBusRequest {
    name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct OptionalNamedBusRequest {
    name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct RuleIdentityRequest {
    #[serde(default)]
    event_bus_name: Option<String>,
    name: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EventBridgeOperation {
    CreateEventBus,
    DeleteEventBus,
    DescribeEventBus,
    ListEventBuses,
    PutRule,
    DescribeRule,
    ListRules,
    EnableRule,
    DisableRule,
    DeleteRule,
    PutTargets,
    ListTargetsByRule,
    RemoveTargets,
    PutEvents,
}

fn operation_from_target(
    target: Option<&str>,
) -> Result<EventBridgeOperation, EventBridgeError> {
    let target =
        target.ok_or_else(|| EventBridgeError::UnsupportedOperation {
            message: "missing X-Amz-Target".to_owned(),
        })?;
    let Some((prefix, operation)) = target.split_once('.') else {
        return Err(EventBridgeError::UnsupportedOperation {
            message: format!("Operation {target} is not supported."),
        });
    };
    if prefix != "AWSEvents" {
        return Err(EventBridgeError::UnsupportedOperation {
            message: format!("Operation {target} is not supported."),
        });
    }

    match operation {
        "CreateEventBus" => Ok(EventBridgeOperation::CreateEventBus),
        "DeleteEventBus" => Ok(EventBridgeOperation::DeleteEventBus),
        "DescribeEventBus" => Ok(EventBridgeOperation::DescribeEventBus),
        "ListEventBuses" => Ok(EventBridgeOperation::ListEventBuses),
        "PutRule" => Ok(EventBridgeOperation::PutRule),
        "DescribeRule" => Ok(EventBridgeOperation::DescribeRule),
        "ListRules" => Ok(EventBridgeOperation::ListRules),
        "EnableRule" => Ok(EventBridgeOperation::EnableRule),
        "DisableRule" => Ok(EventBridgeOperation::DisableRule),
        "DeleteRule" => Ok(EventBridgeOperation::DeleteRule),
        "PutTargets" => Ok(EventBridgeOperation::PutTargets),
        "ListTargetsByRule" => Ok(EventBridgeOperation::ListTargetsByRule),
        "RemoveTargets" => Ok(EventBridgeOperation::RemoveTargets),
        "PutEvents" => Ok(EventBridgeOperation::PutEvents),
        _ => Err(EventBridgeError::UnsupportedOperation {
            message: format!("Operation {target} is not supported."),
        }),
    }
}

fn parse_json_body<T>(body: &[u8]) -> Result<T, AwsError>
where
    T: DeserializeOwned,
{
    serde_json::from_slice(body).map_err(|error| {
        EventBridgeError::Validation {
            message: format!("invalid EventBridge request body: {error}"),
        }
        .to_aws_error()
    })
}

fn json_response<T>(value: &T) -> Result<Vec<u8>, AwsError>
where
    T: Serialize,
{
    serde_json::to_vec(value).map_err(|error| {
        EventBridgeError::InternalFailure { message: error.to_string() }
            .to_aws_error()
    })
}

#[cfg(test)]
mod tests {
    use super::operation_from_target;
    use crate::EventBridgeError;
    use crate::aws_error_shape::AwsErrorShape;
    use crate::test_runtime::router_with_runtime;

    #[test]
    fn target_parser_accepts_supported_eventbridge_operations() {
        assert_eq!(
            operation_from_target(Some("AWSEvents.PutEvents"))
                .expect("put events should parse"),
            super::EventBridgeOperation::PutEvents
        );
        assert!(operation_from_target(Some("AWSEvents.Unknown")).is_err());
    }

    #[test]
    fn adapter_round_trips_bus_rule_and_put_events_flow() {
        let (router, runtime) = router_with_runtime("http-eventbridge");
        let response = router.handle_bytes(
            b"POST / HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/x-amz-json-1.1\r\nX-Amz-Target: AWSEvents.CreateEventBus\r\nContent-Length: 17\r\n\r\n{\"Name\":\"orders\"}",
        );
        assert!(
            response.to_http_bytes().starts_with(b"HTTP/1.1 200 "),
            "create event bus should return HTTP 200"
        );

        let response = router.handle_bytes(
            b"POST / HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/x-amz-json-1.1\r\nX-Amz-Target: AWSEvents.PutRule\r\nContent-Length: 126\r\n\r\n{\"Name\":\"orders-rule\",\"EventBusName\":\"orders\",\"EventPattern\":\"{\\\"source\\\":[\\\"orders\\\"],\\\"detail-type\\\":[\\\"Created\\\"]}\"}",
        );
        assert!(
            response.to_http_bytes().starts_with(b"HTTP/1.1 200 "),
            "put rule should return HTTP 200"
        );

        let response = router.handle_bytes(
            b"POST / HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/x-amz-json-1.1\r\nX-Amz-Target: AWSEvents.PutEvents\r\nContent-Length: 136\r\n\r\n{\"Entries\":[{\"Source\":\"orders\",\"DetailType\":\"Created\",\"Detail\":\"{}\",\"EventBusName\":\"orders\"}]}",
        );
        assert!(
            response.to_http_bytes().starts_with(b"HTTP/1.1 200 "),
            "put events should return HTTP 200"
        );

        let scope = services::EventBridgeScope::new(
            "000000000000".parse().expect("account should parse"),
            "eu-west-2".parse().expect("region should parse"),
        );
        assert_eq!(
            runtime
                .eventbridge()
                .list_rules(
                    &scope,
                    services::ListRulesInput {
                        event_bus_name: Some("orders".to_owned()),
                        limit: Some(10),
                        name_prefix: None,
                        next_token: None,
                    },
                )
                .expect("rules should list")
                .rules
                .len(),
            1
        );
    }

    #[test]
    fn aws_error_shape_maps_eventbridge_validation_errors() {
        let error =
            EventBridgeError::Validation { message: "bad input".to_owned() }
                .to_aws_error();

        assert_eq!(error.code(), "ValidationException");
        assert_eq!(error.status_code(), 400);
    }
}
