use crate::{LambdaError, LambdaScope, functions::function_not_found_error};
use aws::CallerIdentity;
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use time::OffsetDateTime;
use time::format_description::parse;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LambdaFunctionUrlAuthType {
    #[serde(rename = "AWS_IAM")]
    AwsIam,
    #[serde(rename = "NONE")]
    None,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LambdaFunctionUrlInvokeMode {
    #[serde(rename = "BUFFERED")]
    Buffered,
    #[serde(rename = "RESPONSE_STREAM")]
    ResponseStream,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateFunctionUrlConfigInput {
    pub auth_type: LambdaFunctionUrlAuthType,
    pub invoke_mode: LambdaFunctionUrlInvokeMode,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateFunctionUrlConfigInput {
    pub auth_type: Option<LambdaFunctionUrlAuthType>,
    pub invoke_mode: Option<LambdaFunctionUrlInvokeMode>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LambdaFunctionUrlConfig {
    auth_type: LambdaFunctionUrlAuthType,
    function_arn: String,
    invoke_mode: LambdaFunctionUrlInvokeMode,
    last_modified_epoch_seconds: i64,
    url_id: String,
}

impl LambdaFunctionUrlConfig {
    pub fn auth_type(&self) -> LambdaFunctionUrlAuthType {
        self.auth_type
    }

    pub fn function_arn(&self) -> &str {
        &self.function_arn
    }

    pub fn invoke_mode(&self) -> LambdaFunctionUrlInvokeMode {
        self.invoke_mode
    }

    pub fn last_modified_epoch_seconds(&self) -> i64 {
        self.last_modified_epoch_seconds
    }

    pub fn url_id(&self) -> &str {
        &self.url_id
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListFunctionUrlConfigsOutput {
    function_url_configs: Vec<LambdaFunctionUrlConfig>,
    next_marker: Option<String>,
}

impl ListFunctionUrlConfigsOutput {
    pub fn function_url_configs(&self) -> &[LambdaFunctionUrlConfig] {
        &self.function_url_configs
    }

    pub fn next_marker(&self) -> Option<&str> {
        self.next_marker.as_deref()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FunctionUrlInvocationInput {
    pub body: Vec<u8>,
    pub headers: Vec<(String, String)>,
    pub method: String,
    pub path: String,
    pub protocol: Option<String>,
    pub query_string: Option<String>,
    pub source_ip: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FunctionUrlInvocationOutput {
    body: Vec<u8>,
    headers: Vec<(String, String)>,
    status_code: u16,
}

impl FunctionUrlInvocationOutput {
    pub fn body(&self) -> &[u8] {
        &self.body
    }

    pub fn headers(&self) -> &[(String, String)] {
        &self.headers
    }

    pub fn status_code(&self) -> u16 {
        self.status_code
    }
}

pub(crate) fn lambda_function_url_config(
    auth_type: LambdaFunctionUrlAuthType,
    function_arn: String,
    invoke_mode: LambdaFunctionUrlInvokeMode,
    last_modified_epoch_seconds: i64,
    url_id: String,
) -> LambdaFunctionUrlConfig {
    LambdaFunctionUrlConfig {
        auth_type,
        function_arn,
        invoke_mode,
        last_modified_epoch_seconds,
        url_id,
    }
}

pub(crate) fn list_function_url_configs_output(
    function_url_configs: Vec<LambdaFunctionUrlConfig>,
    next_marker: Option<String>,
) -> ListFunctionUrlConfigsOutput {
    ListFunctionUrlConfigsOutput { function_url_configs, next_marker }
}

pub(crate) fn function_url_config_key(qualifier: Option<&str>) -> &str {
    qualifier.unwrap_or("")
}

pub(crate) fn function_url_config_qualifier(key: &str) -> Option<&str> {
    (!key.is_empty()).then_some(key)
}

pub(crate) fn function_url_target_label(
    function_name: &str,
    qualifier: Option<&str>,
) -> String {
    qualifier
        .map(|qualifier| format!("{function_name}:{qualifier}"))
        .unwrap_or_else(|| function_name.to_owned())
}

pub(crate) fn validate_function_url_invoke_mode(
    invoke_mode: LambdaFunctionUrlInvokeMode,
) -> Result<(), LambdaError> {
    if invoke_mode != LambdaFunctionUrlInvokeMode::Buffered {
        return Err(LambdaError::UnsupportedOperation {
            message: "Lambda function URLs only support BUFFERED invoke mode in this Cloudish build.".to_owned(),
        });
    }

    Ok(())
}

pub(crate) fn validate_function_url_qualifier_kind(
    qualifier: Option<&str>,
) -> Result<(), LambdaError> {
    let Some(qualifier) = qualifier else {
        return Ok(());
    };
    if qualifier == "$LATEST"
        || qualifier.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(LambdaError::InvalidParameterValue {
            message:
                "Function URLs support unqualified functions and aliases only."
                    .to_owned(),
        });
    }

    Ok(())
}

pub(crate) trait LambdaFunctionUrlQualifierStore {
    fn has_alias(&self, qualifier: &str) -> bool;
}

pub(crate) fn validate_function_url_qualifier<State>(
    scope: &LambdaScope,
    function_name: &str,
    state: &State,
    qualifier: Option<&str>,
) -> Result<(), LambdaError>
where
    State: LambdaFunctionUrlQualifierStore,
{
    validate_function_url_qualifier_kind(qualifier)?;
    let Some(qualifier) = qualifier else {
        return Ok(());
    };
    if !state.has_alias(qualifier) {
        return Err(function_not_found_error(
            scope,
            function_name,
            Some(qualifier),
        ));
    }

    Ok(())
}

pub(crate) fn function_url_exists_error(
    function_name: &str,
    qualifier: Option<&str>,
) -> LambdaError {
    LambdaError::ResourceConflict {
        message: format!(
            "Function URL already exists for {}.",
            function_url_target_label(function_name, qualifier)
        ),
    }
}

pub(crate) fn function_url_not_found_error(
    function_arn: String,
) -> LambdaError {
    LambdaError::ResourceNotFound {
        message: format!("Function URL not found: {function_arn}"),
    }
}

pub(crate) fn build_function_url_event(
    scope: &LambdaScope,
    url_id: &str,
    input: &FunctionUrlInvocationInput,
    caller_identity: Option<&CallerIdentity>,
    now_epoch_millis: u64,
    request_id: String,
) -> Result<Vec<u8>, LambdaError> {
    let headers = canonical_header_map(&input.headers);
    let query_string_parameters =
        query_string_parameters(input.query_string.as_deref());
    let cookies = extract_cookies(&headers);
    let body_is_utf8 = std::str::from_utf8(&input.body).ok();
    let (body, is_base64_encoded) = match body_is_utf8 {
        Some(body) => (serde_json::Value::String(body.to_owned()), false),
        None => (
            serde_json::Value::String(BASE64_STANDARD.encode(&input.body)),
            true,
        ),
    };
    let request_context = serde_json::json!({
        "accountId": scope.account_id().as_str(),
        "apiId": url_id,
        "authentication": serde_json::Value::Null,
        "authorizer": caller_identity.map(|caller_identity| serde_json::json!({
            "iam": {
                "accessKey": serde_json::Value::Null,
                "accountId": caller_identity
                    .arn()
                    .account_id()
                    .map(|account_id| account_id.as_str())
                    .unwrap_or(scope.account_id().as_str()),
                "callerId": caller_identity.principal_id(),
                "cognitoIdentity": serde_json::Value::Null,
                "principalOrgId": serde_json::Value::Null,
                "userArn": caller_identity.arn().to_string(),
                "userId": caller_identity.principal_id(),
            }
        })).unwrap_or(serde_json::Value::Null),
        "domainName": format!(
            "{}.lambda-url.{}.localhost",
            url_id,
            scope.region().as_str(),
        ),
        "domainPrefix": url_id,
        "http": {
            "method": input.method,
            "path": input.path,
            "protocol": input.protocol.as_deref().unwrap_or("HTTP/1.1"),
            "sourceIp": input.source_ip.as_deref().unwrap_or("127.0.0.1"),
            "userAgent": headers.get("user-agent").cloned().unwrap_or_default(),
        },
        "requestId": request_id,
        "routeKey": "$default",
        "stage": "$default",
        "time": format_request_context_time(now_epoch_millis)?,
        "timeEpoch": now_epoch_millis,
    });
    let event = serde_json::json!({
        "version": "2.0",
        "routeKey": "$default",
        "rawPath": input.path,
        "rawQueryString": input.query_string.clone().unwrap_or_default(),
        "cookies": cookies
            .map(|cookies| {
                serde_json::Value::Array(
                    cookies
                        .into_iter()
                        .map(serde_json::Value::String)
                        .collect(),
                )
            })
            .unwrap_or(serde_json::Value::Null),
        "headers": json_string_map(headers),
        "queryStringParameters": if query_string_parameters.is_empty() {
            serde_json::Value::Null
        } else {
            serde_json::Value::Object(json_string_map(query_string_parameters))
        },
        "requestContext": request_context,
        "body": body,
        "pathParameters": serde_json::Value::Null,
        "isBase64Encoded": is_base64_encoded,
        "stageVariables": serde_json::Value::Null,
    });

    serde_json::to_vec(&event)
        .map_err(|error| LambdaError::Internal { message: error.to_string() })
}

pub(crate) fn map_function_url_response(
    payload: &[u8],
) -> Result<FunctionUrlInvocationOutput, LambdaError> {
    let value = serde_json::from_slice::<serde_json::Value>(payload).ok();
    let Some(value) = value else {
        return Ok(default_json_response(payload));
    };
    let Some(object) = value.as_object() else {
        return Ok(default_json_response(payload));
    };
    if !object.contains_key("statusCode") {
        return Ok(FunctionUrlInvocationOutput {
            body: serde_json::to_vec(&value).map_err(|error| {
                LambdaError::Internal { message: error.to_string() }
            })?,
            headers: vec![(
                "content-type".to_owned(),
                "application/json".to_owned(),
            )],
            status_code: 200,
        });
    }

    let status_code = object
        .get("statusCode")
        .and_then(serde_json::Value::as_u64)
        .and_then(|value| u16::try_from(value).ok())
        .filter(|value| (100..=599).contains(value))
        .ok_or_else(|| LambdaError::InvalidRequestContent {
            message: "Lambda function URL responses must use an HTTP status code between 100 and 599.".to_owned(),
        })?;
    let mut headers = object
        .get("headers")
        .and_then(serde_json::Value::as_object)
        .map(|headers| {
            headers
                .iter()
                .filter_map(|(name, value)| {
                    Some((name.to_owned(), value.as_str()?.to_owned()))
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    if !headers
        .iter()
        .any(|(name, _)| name.eq_ignore_ascii_case("content-type"))
    {
        headers
            .push(("content-type".to_owned(), "application/json".to_owned()));
    }
    let mut cookie_headers = object
        .get("cookies")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|value| value.as_str())
        .map(|value| ("set-cookie".to_owned(), value.to_owned()))
        .collect::<Vec<_>>();
    let body = match object.get("body") {
        Some(serde_json::Value::String(body))
            if object
                .get("isBase64Encoded")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false) =>
        {
            BASE64_STANDARD.decode(body).map_err(|_| {
                LambdaError::InvalidRequestContent {
                    message: "Lambda function URL responses contained an invalid base64 body.".to_owned(),
                }
            })?
        }
        Some(serde_json::Value::String(body)) => body.as_bytes().to_vec(),
        Some(body) => serde_json::to_vec(body).map_err(|error| LambdaError::Internal {
            message: error.to_string(),
        })?,
        None => Vec::new(),
    };
    headers.append(&mut cookie_headers);

    Ok(FunctionUrlInvocationOutput { body, headers, status_code })
}

fn default_json_response(payload: &[u8]) -> FunctionUrlInvocationOutput {
    FunctionUrlInvocationOutput {
        body: payload.to_vec(),
        headers: vec![(
            "content-type".to_owned(),
            "application/json".to_owned(),
        )],
        status_code: 200,
    }
}

fn json_string_map(
    values: BTreeMap<String, String>,
) -> serde_json::Map<String, serde_json::Value> {
    values
        .into_iter()
        .map(|(name, value)| (name, serde_json::Value::String(value)))
        .collect()
}

fn canonical_header_map(
    headers: &[(String, String)],
) -> BTreeMap<String, String> {
    let mut combined = BTreeMap::<String, Vec<String>>::new();
    for (name, value) in headers {
        combined
            .entry(name.to_ascii_lowercase())
            .or_default()
            .push(value.clone());
    }

    combined
        .into_iter()
        .map(|(name, values)| (name, values.join(",")))
        .collect()
}

fn query_string_parameters(
    raw_query: Option<&str>,
) -> BTreeMap<String, String> {
    let mut parameters = BTreeMap::<String, Vec<String>>::new();
    for pair in raw_query
        .unwrap_or_default()
        .split('&')
        .filter(|pair| !pair.is_empty())
    {
        let (name, value) = pair.split_once('=').unwrap_or((pair, ""));
        parameters.entry(name.to_owned()).or_default().push(value.to_owned());
    }

    parameters
        .into_iter()
        .map(|(name, values)| (name, values.join(",")))
        .collect()
}

fn extract_cookies(headers: &BTreeMap<String, String>) -> Option<Vec<String>> {
    headers.get("cookie").map(|cookies| {
        cookies
            .split(';')
            .map(str::trim)
            .filter(|cookie| !cookie.is_empty())
            .map(str::to_owned)
            .collect()
    })
}

fn format_request_context_time(
    epoch_millis: u64,
) -> Result<String, LambdaError> {
    let timestamp = OffsetDateTime::from_unix_timestamp_nanos(
        i128::from(epoch_millis) * 1_000_000,
    )
    .map_err(|error| LambdaError::Internal { message: error.to_string() })?;
    let format = parse(
        "[day padding:none]/[month repr:short]/[year]:[hour]:[minute]:[second] +0000",
    )
    .map_err(|error| LambdaError::Internal {
        message: error.to_string(),
    })?;

    timestamp
        .format(&format)
        .map_err(|error| LambdaError::Internal { message: error.to_string() })
}

#[cfg(test)]
mod tests {
    use super::{
        CreateFunctionUrlConfigInput, FunctionUrlInvocationInput,
        LambdaFunctionUrlAuthType, LambdaFunctionUrlInvokeMode,
        LambdaFunctionUrlQualifierStore, build_function_url_event,
        function_url_config_key, function_url_config_qualifier,
        function_url_exists_error, function_url_not_found_error,
        lambda_function_url_config, list_function_url_configs_output,
        map_function_url_response, validate_function_url_invoke_mode,
        validate_function_url_qualifier, validate_function_url_qualifier_kind,
    };
    use crate::{LambdaError, LambdaScope};
    use aws::{AccountId, Arn, CallerIdentity, RegionId};
    use serde_json::json;

    fn scope() -> LambdaScope {
        LambdaScope::new(
            "000000000000".parse::<AccountId>().unwrap(),
            "eu-west-2".parse::<RegionId>().unwrap(),
        )
    }

    #[derive(Debug)]
    struct TestQualifierStore {
        aliases: Vec<&'static str>,
    }

    impl LambdaFunctionUrlQualifierStore for TestQualifierStore {
        fn has_alias(&self, qualifier: &str) -> bool {
            self.aliases.contains(&qualifier)
        }
    }

    #[test]
    fn function_url_contract_round_trips_public_fields() {
        let config = lambda_function_url_config(
            LambdaFunctionUrlAuthType::AwsIam,
            "arn:aws:lambda:eu-west-2:000000000000:function:demo:live"
                .to_owned(),
            LambdaFunctionUrlInvokeMode::Buffered,
            60,
            "abc123".to_owned(),
        );
        let output = list_function_url_configs_output(
            vec![config.clone()],
            Some("marker-1".to_owned()),
        );
        let input = CreateFunctionUrlConfigInput {
            auth_type: LambdaFunctionUrlAuthType::None,
            invoke_mode: LambdaFunctionUrlInvokeMode::ResponseStream,
        };

        assert_eq!(input.auth_type, LambdaFunctionUrlAuthType::None);
        assert_eq!(
            input.invoke_mode,
            LambdaFunctionUrlInvokeMode::ResponseStream
        );
        assert_eq!(config.auth_type(), LambdaFunctionUrlAuthType::AwsIam);
        assert_eq!(
            config.function_arn(),
            "arn:aws:lambda:eu-west-2:000000000000:function:demo:live"
        );
        assert_eq!(
            config.invoke_mode(),
            LambdaFunctionUrlInvokeMode::Buffered
        );
        assert_eq!(config.last_modified_epoch_seconds(), 60);
        assert_eq!(config.url_id(), "abc123");
        assert_eq!(output.function_url_configs(), &[config]);
        assert_eq!(output.next_marker(), Some("marker-1"));
    }

    #[test]
    fn function_url_helpers_validate_targeting_rules() {
        assert_eq!(function_url_config_key(None), "");
        assert_eq!(function_url_config_key(Some("live")), "live");
        assert_eq!(function_url_config_qualifier(""), None);
        assert_eq!(function_url_config_qualifier("live"), Some("live"));

        match function_url_exists_error("demo", Some("live")) {
            LambdaError::ResourceConflict { message } => {
                assert_eq!(
                    message,
                    "Function URL already exists for demo:live."
                );
            }
            other => panic!("expected conflict error, got {other:?}"),
        }

        match function_url_not_found_error(
            "arn:aws:lambda:eu-west-2:000000000000:function:demo:live"
                .to_owned(),
        ) {
            LambdaError::ResourceNotFound { message } => {
                assert_eq!(
                    message,
                    "Function URL not found: arn:aws:lambda:eu-west-2:000000000000:function:demo:live"
                );
            }
            other => panic!("expected not found error, got {other:?}"),
        }

        for qualifier in ["$LATEST", "7"] {
            match validate_function_url_qualifier_kind(Some(qualifier)) {
                Err(LambdaError::InvalidParameterValue { message }) => {
                    assert_eq!(
                        message,
                        "Function URLs support unqualified functions and aliases only."
                    );
                }
                other => {
                    panic!("expected invalid qualifier error, got {other:?}")
                }
            }
        }

        assert!(validate_function_url_qualifier_kind(None).is_ok());
        assert!(validate_function_url_qualifier_kind(Some("live")).is_ok());

        match validate_function_url_invoke_mode(
            LambdaFunctionUrlInvokeMode::ResponseStream,
        ) {
            Err(LambdaError::UnsupportedOperation { message }) => {
                assert_eq!(
                    message,
                    "Lambda function URLs only support BUFFERED invoke mode in this Cloudish build."
                );
            }
            other => {
                panic!("expected unsupported invoke mode error, got {other:?}")
            }
        }
        assert!(
            validate_function_url_invoke_mode(
                LambdaFunctionUrlInvokeMode::Buffered
            )
            .is_ok()
        );

        let store = TestQualifierStore { aliases: vec!["live"] };
        assert!(
            validate_function_url_qualifier(
                &scope(),
                "demo",
                &store,
                Some("live"),
            )
            .is_ok()
        );
        match validate_function_url_qualifier(
            &scope(),
            "demo",
            &store,
            Some("missing"),
        ) {
            Err(LambdaError::ResourceNotFound { message }) => {
                assert_eq!(
                    message,
                    "Function not found: arn:aws:lambda:eu-west-2:000000000000:function:demo:missing"
                );
            }
            other => panic!("expected missing alias error, got {other:?}"),
        }
    }

    #[test]
    fn build_function_url_event_shapes_payload_v2_request() {
        let event = build_function_url_event(
            &scope(),
            "abc123",
            &FunctionUrlInvocationInput {
                body: br#"{"hello":"world"}"#.to_vec(),
                headers: vec![
                    ("User-Agent".to_owned(), "cloudish-tests".to_owned()),
                    ("Cookie".to_owned(), "a=1; b=2".to_owned()),
                    ("X-Test".to_owned(), "one".to_owned()),
                    ("x-test".to_owned(), "two".to_owned()),
                ],
                method: "POST".to_owned(),
                path: "/custom/path".to_owned(),
                protocol: None,
                query_string: Some("mode=test&mode=prod&flag".to_owned()),
                source_ip: Some("10.0.0.1".to_owned()),
            },
            None,
            60_000,
            "request-1".to_owned(),
        )
        .unwrap();
        let event =
            serde_json::from_slice::<serde_json::Value>(&event).unwrap();

        assert_eq!(
            event,
            json!({
                "version": "2.0",
                "routeKey": "$default",
                "rawPath": "/custom/path",
                "rawQueryString": "mode=test&mode=prod&flag",
                "cookies": ["a=1", "b=2"],
                "headers": {
                    "cookie": "a=1; b=2",
                    "user-agent": "cloudish-tests",
                    "x-test": "one,two"
                },
                "queryStringParameters": {
                    "flag": "",
                    "mode": "test,prod"
                },
                "requestContext": {
                    "accountId": "000000000000",
                    "apiId": "abc123",
                    "authentication": serde_json::Value::Null,
                    "authorizer": serde_json::Value::Null,
                    "domainName": "abc123.lambda-url.eu-west-2.localhost",
                    "domainPrefix": "abc123",
                    "http": {
                        "method": "POST",
                        "path": "/custom/path",
                        "protocol": "HTTP/1.1",
                        "sourceIp": "10.0.0.1",
                        "userAgent": "cloudish-tests"
                    },
                    "requestId": "request-1",
                    "routeKey": "$default",
                    "stage": "$default",
                    "time": "1/Jan/1970:00:01:00 +0000",
                    "timeEpoch": 60_000
                },
                "body": "{\"hello\":\"world\"}",
                "pathParameters": serde_json::Value::Null,
                "isBase64Encoded": false,
                "stageVariables": serde_json::Value::Null
            })
        );
    }

    #[test]
    fn build_function_url_event_includes_iam_authorizer_and_binary_body() {
        let caller_identity = CallerIdentity::try_new(
            "arn:aws:iam::123456789012:user/demo".parse::<Arn>().unwrap(),
            "AIDAEXAMPLE",
        )
        .unwrap();

        let event = build_function_url_event(
            &scope(),
            "url-1",
            &FunctionUrlInvocationInput {
                body: vec![0, 159, 146, 150],
                headers: Vec::new(),
                method: "GET".to_owned(),
                path: "/".to_owned(),
                protocol: Some("HTTP/2".to_owned()),
                query_string: None,
                source_ip: None,
            },
            Some(&caller_identity),
            1,
            "request-2".to_owned(),
        )
        .unwrap();
        let event =
            serde_json::from_slice::<serde_json::Value>(&event).unwrap();

        assert_eq!(event["body"], "AJ+Slg==");
        assert_eq!(event["isBase64Encoded"], true);
        assert_eq!(event["cookies"], serde_json::Value::Null);
        assert_eq!(
            event["requestContext"]["authorizer"],
            json!({
                "iam": {
                    "accessKey": serde_json::Value::Null,
                    "accountId": "123456789012",
                    "callerId": "AIDAEXAMPLE",
                    "cognitoIdentity": serde_json::Value::Null,
                    "principalOrgId": serde_json::Value::Null,
                    "userArn": "arn:aws:iam::123456789012:user/demo",
                    "userId": "AIDAEXAMPLE"
                }
            })
        );
        assert_eq!(event["requestContext"]["http"]["protocol"], "HTTP/2");
        assert_eq!(event["requestContext"]["http"]["sourceIp"], "127.0.0.1");
    }

    #[test]
    fn map_function_url_response_defaults_plain_payload_to_json_200() {
        let response = map_function_url_response(br#"not-json"#).unwrap();

        assert_eq!(response.status_code(), 200);
        assert_eq!(response.body(), br#"not-json"#);
        assert_eq!(
            response.headers(),
            &[("content-type".to_owned(), "application/json".to_owned())]
        );
    }

    #[test]
    fn map_function_url_response_decodes_structured_payload() {
        let response = map_function_url_response(
            br#"{"statusCode":201,"headers":{"x-mode":"lambda-url"},"cookies":["a=1","b=2"],"isBase64Encoded":true,"body":"aGVsbG8="}"#,
        )
        .unwrap();

        assert_eq!(response.status_code(), 201);
        assert_eq!(response.body(), b"hello");
        assert_eq!(
            response.headers(),
            &[
                ("x-mode".to_owned(), "lambda-url".to_owned()),
                ("content-type".to_owned(), "application/json".to_owned()),
                ("set-cookie".to_owned(), "a=1".to_owned()),
                ("set-cookie".to_owned(), "b=2".to_owned()),
            ]
        );
    }

    #[test]
    fn map_function_url_response_rejects_invalid_http_status() {
        let error = map_function_url_response(br#"{"statusCode":99}"#)
            .expect_err("status codes below 100 must be rejected");

        match error {
            LambdaError::InvalidRequestContent { message } => {
                assert_eq!(
                    message,
                    "Lambda function URL responses must use an HTTP status code between 100 and 599."
                );
            }
            other => {
                panic!("expected invalid request content error, got {other:?}")
            }
        }
    }

    #[test]
    fn map_function_url_response_rejects_invalid_base64_body() {
        let error = map_function_url_response(
            br#"{"statusCode":200,"isBase64Encoded":true,"body":"***"}"#,
        )
        .expect_err("invalid base64 should be rejected");

        match error {
            LambdaError::InvalidRequestContent { message } => {
                assert_eq!(
                    message,
                    "Lambda function URL responses contained an invalid base64 body."
                );
            }
            other => {
                panic!("expected invalid request content error, got {other:?}")
            }
        }
    }
}
