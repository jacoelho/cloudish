pub(crate) use crate::aws_error_shape::AwsErrorShape;
use crate::request::HttpRequest;
use aws::{AwsError, RequestContext};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use serde::Deserialize;
use serde::Serialize;
use serde::de::DeserializeOwned;
use services::{
    KmsKeyReference, SecretsManagerCreateSecretInput,
    SecretsManagerDeleteSecretInput, SecretsManagerDescribeSecretInput,
    SecretsManagerError, SecretsManagerExternalMetadataItem,
    SecretsManagerGetSecretValueInput,
    SecretsManagerListSecretVersionIdsInput, SecretsManagerListSecretsFilter,
    SecretsManagerListSecretsFilterKey, SecretsManagerListSecretsInput,
    SecretsManagerListSecretsSortBy, SecretsManagerListSecretsSortOrder,
    SecretsManagerPutSecretValueInput, SecretsManagerRotateSecretInput,
    SecretsManagerRotationRules, SecretsManagerScope,
    SecretsManagerSecretName, SecretsManagerSecretReference,
    SecretsManagerService, SecretsManagerTag, SecretsManagerTagResourceInput,
    SecretsManagerUntagResourceInput, SecretsManagerUpdateSecretInput,
};

pub(crate) fn handle_json(
    secrets_manager: &SecretsManagerService,
    request: &HttpRequest<'_>,
    context: &RequestContext,
) -> Result<Vec<u8>, AwsError> {
    let operation = operation_from_target(request.header("x-amz-target"))
        .map_err(|error| error.to_aws_error())?;
    let scope = SecretsManagerScope::new(
        context.account_id().clone(),
        context.region().clone(),
    );

    match operation {
        SecretsManagerOperation::CreateSecret => json_response(
            &secrets_manager
                .create_secret(
                    &scope,
                    parse_create_secret_input(request.body())?,
                )
                .map_err(|error| error.to_aws_error())?,
        ),
        SecretsManagerOperation::GetSecretValue => json_response(
            &secrets_manager
                .get_secret_value(
                    &scope,
                    parse_get_secret_value_input(request.body())?,
                )
                .map_err(|error| error.to_aws_error())?,
        ),
        SecretsManagerOperation::PutSecretValue => json_response(
            &secrets_manager
                .put_secret_value(
                    &scope,
                    parse_put_secret_value_input(request.body())?,
                )
                .map_err(|error| error.to_aws_error())?,
        ),
        SecretsManagerOperation::UpdateSecret => json_response(
            &secrets_manager
                .update_secret(
                    &scope,
                    parse_update_secret_input(request.body())?,
                )
                .map_err(|error| error.to_aws_error())?,
        ),
        SecretsManagerOperation::DescribeSecret => json_response(
            &secrets_manager
                .describe_secret(
                    &scope,
                    parse_describe_secret_input(request.body())?,
                )
                .map_err(|error| error.to_aws_error())?,
        ),
        SecretsManagerOperation::ListSecrets => json_response(
            &secrets_manager
                .list_secrets(
                    &scope,
                    parse_list_secrets_input(request.body())?,
                )
                .map_err(|error| error.to_aws_error())?,
        ),
        SecretsManagerOperation::DeleteSecret => json_response(
            &secrets_manager
                .delete_secret(
                    &scope,
                    parse_delete_secret_input(request.body())?,
                )
                .map_err(|error| error.to_aws_error())?,
        ),
        SecretsManagerOperation::RotateSecret => json_response(
            &secrets_manager
                .rotate_secret(
                    &scope,
                    parse_rotate_secret_input(request.body())?,
                )
                .map_err(|error| error.to_aws_error())?,
        ),
        SecretsManagerOperation::TagResource => json_response(
            &secrets_manager
                .tag_resource(
                    &scope,
                    parse_tag_resource_input(request.body())?,
                )
                .map_err(|error| error.to_aws_error())?,
        ),
        SecretsManagerOperation::UntagResource => json_response(
            &secrets_manager
                .untag_resource(
                    &scope,
                    parse_untag_resource_input(request.body())?,
                )
                .map_err(|error| error.to_aws_error())?,
        ),
        SecretsManagerOperation::ListSecretVersionIds => json_response(
            &secrets_manager
                .list_secret_version_ids(
                    &scope,
                    parse_list_secret_version_ids_input(request.body())?,
                )
                .map_err(|error| error.to_aws_error())?,
        ),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct CreateSecretRequest {
    client_request_token: Option<String>,
    description: Option<String>,
    kms_key_id: Option<String>,
    name: String,
    secret_binary: Option<String>,
    secret_string: Option<String>,
    tags: Option<Vec<SecretsManagerTag>>,
    #[serde(rename = "Type")]
    secret_type: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct PutSecretValueRequest {
    client_request_token: Option<String>,
    secret_binary: Option<String>,
    secret_id: String,
    secret_string: Option<String>,
    version_stages: Option<Vec<String>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct GetSecretValueRequest {
    secret_id: String,
    version_id: Option<String>,
    version_stage: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct UpdateSecretRequest {
    client_request_token: Option<String>,
    description: Option<String>,
    kms_key_id: Option<String>,
    secret_binary: Option<String>,
    secret_id: String,
    secret_string: Option<String>,
    #[serde(rename = "Type")]
    secret_type: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct DescribeSecretRequest {
    secret_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct ListSecretsRequest {
    filters: Option<Vec<ListSecretsFilterRequest>>,
    include_planned_deletion: Option<bool>,
    max_results: Option<u32>,
    next_token: Option<String>,
    sort_by: Option<String>,
    sort_order: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct ListSecretsFilterRequest {
    key: String,
    values: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct RotateSecretRequest {
    client_request_token: Option<String>,
    external_secret_rotation_metadata:
        Option<Vec<SecretsManagerExternalMetadataItem>>,
    external_secret_rotation_role_arn: Option<String>,
    rotate_immediately: Option<bool>,
    #[serde(rename = "RotationLambdaARN")]
    rotation_lambda_arn: Option<String>,
    rotation_rules: Option<SecretsManagerRotationRules>,
    secret_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct DeleteSecretRequest {
    #[serde(default)]
    force_delete_without_recovery: bool,
    recovery_window_in_days: Option<u32>,
    secret_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct TagResourceRequest {
    secret_id: String,
    tags: Vec<SecretsManagerTag>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct UntagResourceRequest {
    secret_id: String,
    tag_keys: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct ListSecretVersionIdsRequest {
    #[serde(default)]
    include_deprecated: bool,
    max_results: Option<u32>,
    next_token: Option<String>,
    secret_id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SecretsManagerOperation {
    CreateSecret,
    GetSecretValue,
    PutSecretValue,
    UpdateSecret,
    DescribeSecret,
    ListSecrets,
    DeleteSecret,
    RotateSecret,
    TagResource,
    UntagResource,
    ListSecretVersionIds,
}

fn action_from_target(
    target: Option<&str>,
) -> Result<&str, SecretsManagerError> {
    let target =
        target.ok_or_else(|| SecretsManagerError::UnsupportedOperation {
            message: "missing X-Amz-Target".to_owned(),
        })?;
    let Some((prefix, operation)) = target.split_once('.') else {
        return Err(SecretsManagerError::UnsupportedOperation {
            message: format!("Operation {target} is not supported."),
        });
    };
    if prefix != "secretsmanager" {
        return Err(SecretsManagerError::UnsupportedOperation {
            message: format!("Operation {target} is not supported."),
        });
    }

    Ok(operation)
}

fn operation_from_target(
    target: Option<&str>,
) -> Result<SecretsManagerOperation, SecretsManagerError> {
    let operation = action_from_target(target)?;

    match operation {
        "CreateSecret" => Ok(SecretsManagerOperation::CreateSecret),
        "GetSecretValue" => Ok(SecretsManagerOperation::GetSecretValue),
        "PutSecretValue" => Ok(SecretsManagerOperation::PutSecretValue),
        "UpdateSecret" => Ok(SecretsManagerOperation::UpdateSecret),
        "DescribeSecret" => Ok(SecretsManagerOperation::DescribeSecret),
        "ListSecrets" => Ok(SecretsManagerOperation::ListSecrets),
        "DeleteSecret" => Ok(SecretsManagerOperation::DeleteSecret),
        "RotateSecret" => Ok(SecretsManagerOperation::RotateSecret),
        "TagResource" => Ok(SecretsManagerOperation::TagResource),
        "UntagResource" => Ok(SecretsManagerOperation::UntagResource),
        "ListSecretVersionIds" => {
            Ok(SecretsManagerOperation::ListSecretVersionIds)
        }
        _ => Err(SecretsManagerError::UnsupportedOperation {
            message: format!("Operation {operation} is not supported."),
        }),
    }
}

fn parse_json_body<T>(body: &[u8]) -> Result<T, AwsError>
where
    T: DeserializeOwned,
{
    serde_json::from_slice(body).map_err(|error| {
        SecretsManagerError::InvalidParameterException {
            message: format!("The request body is not valid JSON: {error}"),
        }
        .to_aws_error()
    })
}

fn json_response<T>(value: &T) -> Result<Vec<u8>, AwsError>
where
    T: Serialize,
{
    serde_json::to_vec(value).map_err(|error| {
        SecretsManagerError::InternalServiceError {
            message: format!(
                "Failed to serialize Secrets Manager response: {error}"
            ),
        }
        .to_aws_error()
    })
}

fn parse_create_secret_input(
    body: &[u8],
) -> Result<SecretsManagerCreateSecretInput, AwsError> {
    let request = parse_json_body::<CreateSecretRequest>(body)?;

    Ok(SecretsManagerCreateSecretInput {
        client_request_token: request.client_request_token,
        description: request.description,
        kms_key_id: parse_optional_kms_key_reference(request.kms_key_id)?,
        name: parse_secret_name(request.name)?,
        secret_binary: decode_optional_binary(
            request.secret_binary,
            "SecretBinary",
        )?,
        secret_string: request.secret_string,
        secret_type: request.secret_type,
        tags: request.tags.unwrap_or_default(),
    })
}

fn parse_put_secret_value_input(
    body: &[u8],
) -> Result<SecretsManagerPutSecretValueInput, AwsError> {
    let request = parse_json_body::<PutSecretValueRequest>(body)?;

    Ok(SecretsManagerPutSecretValueInput {
        client_request_token: request.client_request_token,
        secret_binary: decode_optional_binary(
            request.secret_binary,
            "SecretBinary",
        )?,
        secret_id: parse_secret_reference(request.secret_id)?,
        secret_string: request.secret_string,
        version_stages: request.version_stages,
    })
}

fn parse_get_secret_value_input(
    body: &[u8],
) -> Result<SecretsManagerGetSecretValueInput, AwsError> {
    let request = parse_json_body::<GetSecretValueRequest>(body)?;

    Ok(SecretsManagerGetSecretValueInput {
        secret_id: parse_secret_reference(request.secret_id)?,
        version_id: request.version_id,
        version_stage: request.version_stage,
    })
}

fn parse_update_secret_input(
    body: &[u8],
) -> Result<SecretsManagerUpdateSecretInput, AwsError> {
    let request = parse_json_body::<UpdateSecretRequest>(body)?;

    Ok(SecretsManagerUpdateSecretInput {
        client_request_token: request.client_request_token,
        description: request.description,
        kms_key_id: parse_optional_kms_key_reference(request.kms_key_id)?,
        secret_binary: decode_optional_binary(
            request.secret_binary,
            "SecretBinary",
        )?,
        secret_id: parse_secret_reference(request.secret_id)?,
        secret_string: request.secret_string,
        secret_type: request.secret_type,
    })
}

fn parse_describe_secret_input(
    body: &[u8],
) -> Result<SecretsManagerDescribeSecretInput, AwsError> {
    let request = parse_json_body::<DescribeSecretRequest>(body)?;

    Ok(SecretsManagerDescribeSecretInput {
        secret_id: parse_secret_reference(request.secret_id)?,
    })
}

fn parse_list_secrets_input(
    body: &[u8],
) -> Result<SecretsManagerListSecretsInput, AwsError> {
    let request = parse_json_body::<ListSecretsRequest>(body)?;
    let filters = request
        .filters
        .unwrap_or_default()
        .into_iter()
        .map(|filter| {
            Ok(SecretsManagerListSecretsFilter {
                key: SecretsManagerListSecretsFilterKey::parse(&filter.key)?,
                values: filter.values,
            })
        })
        .collect::<Result<Vec<_>, SecretsManagerError>>()
        .map_err(|error| error.to_aws_error())?;
    let sort_by = request
        .sort_by
        .as_deref()
        .map(SecretsManagerListSecretsSortBy::parse)
        .transpose()
        .map_err(|error| error.to_aws_error())?;
    let sort_order = request
        .sort_order
        .as_deref()
        .map(SecretsManagerListSecretsSortOrder::parse)
        .transpose()
        .map_err(|error| error.to_aws_error())?;

    Ok(SecretsManagerListSecretsInput {
        filters,
        include_planned_deletion: request
            .include_planned_deletion
            .unwrap_or(false),
        max_results: request.max_results,
        next_token: request.next_token,
        sort_by,
        sort_order,
    })
}

fn parse_rotate_secret_input(
    body: &[u8],
) -> Result<SecretsManagerRotateSecretInput, AwsError> {
    let request = parse_json_body::<RotateSecretRequest>(body)?;

    Ok(SecretsManagerRotateSecretInput {
        client_request_token: request.client_request_token,
        external_secret_rotation_metadata: request
            .external_secret_rotation_metadata
            .unwrap_or_default(),
        external_secret_rotation_role_arn: request
            .external_secret_rotation_role_arn,
        rotate_immediately: request.rotate_immediately,
        rotation_lambda_arn: parse_optional_arn(
            request.rotation_lambda_arn,
            "RotationLambdaARN",
        )?,
        rotation_rules: request.rotation_rules,
        secret_id: parse_secret_reference(request.secret_id)?,
    })
}

fn parse_delete_secret_input(
    body: &[u8],
) -> Result<SecretsManagerDeleteSecretInput, AwsError> {
    let request = parse_json_body::<DeleteSecretRequest>(body)?;

    Ok(SecretsManagerDeleteSecretInput {
        force_delete_without_recovery: request.force_delete_without_recovery,
        recovery_window_in_days: request.recovery_window_in_days,
        secret_id: parse_secret_reference(request.secret_id)?,
    })
}

fn parse_tag_resource_input(
    body: &[u8],
) -> Result<SecretsManagerTagResourceInput, AwsError> {
    let request = parse_json_body::<TagResourceRequest>(body)?;

    Ok(SecretsManagerTagResourceInput {
        secret_id: parse_secret_reference(request.secret_id)?,
        tags: request.tags,
    })
}

fn parse_untag_resource_input(
    body: &[u8],
) -> Result<SecretsManagerUntagResourceInput, AwsError> {
    let request = parse_json_body::<UntagResourceRequest>(body)?;

    Ok(SecretsManagerUntagResourceInput {
        secret_id: parse_secret_reference(request.secret_id)?,
        tag_keys: request.tag_keys,
    })
}

fn parse_list_secret_version_ids_input(
    body: &[u8],
) -> Result<SecretsManagerListSecretVersionIdsInput, AwsError> {
    let request = parse_json_body::<ListSecretVersionIdsRequest>(body)?;

    Ok(SecretsManagerListSecretVersionIdsInput {
        include_deprecated: request.include_deprecated,
        max_results: request.max_results,
        next_token: request.next_token,
        secret_id: parse_secret_reference(request.secret_id)?,
    })
}

fn decode_optional_binary(
    value: Option<String>,
    field_name: &str,
) -> Result<Option<Vec<u8>>, AwsError> {
    value
        .map(|value| {
            BASE64_STANDARD.decode(value).map_err(|error| {
                SecretsManagerError::InvalidParameterException {
                    message: format!(
                        "{field_name} must be base64 encoded: {error}"
                    ),
                }
                .to_aws_error()
            })
        })
        .transpose()
}

fn parse_optional_arn(
    value: Option<String>,
    field_name: &str,
) -> Result<Option<aws::Arn>, AwsError> {
    value
        .map(|value| {
            value.parse().map_err(|_| {
                SecretsManagerError::InvalidParameterException {
                    message: format!("{field_name} must be a valid ARN."),
                }
                .to_aws_error()
            })
        })
        .transpose()
}

fn parse_optional_kms_key_reference(
    value: Option<String>,
) -> Result<Option<KmsKeyReference>, AwsError> {
    value
        .filter(|value| !value.is_empty())
        .map(parse_kms_key_reference)
        .transpose()
}

fn parse_kms_key_reference(
    value: String,
) -> Result<KmsKeyReference, AwsError> {
    value.parse::<KmsKeyReference>().map_err(|error| {
        SecretsManagerError::InvalidParameterException {
            message: error.to_string(),
        }
        .to_aws_error()
    })
}

fn parse_secret_name(
    value: String,
) -> Result<SecretsManagerSecretName, AwsError> {
    value.parse::<SecretsManagerSecretName>().map_err(|error| {
        SecretsManagerError::InvalidParameterException {
            message: error.to_string(),
        }
        .to_aws_error()
    })
}

fn parse_secret_reference(
    value: String,
) -> Result<SecretsManagerSecretReference, AwsError> {
    value.parse::<SecretsManagerSecretReference>().map_err(|error| {
        SecretsManagerError::InvalidParameterException {
            message: error.to_string(),
        }
        .to_aws_error()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::request::HttpRequest;
    use crate::runtime::EdgeRouter;
    use crate::test_runtime;
    use aws::{ProtocolFamily, ServiceName};
    use serde::Serializer;
    use serde::ser::Error as _;
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};
    use storage::{StorageConfig, StorageFactory, StorageMode};

    #[derive(Debug)]
    struct FixedClock(SystemTime);

    impl FixedClock {
        fn new(time: SystemTime) -> Self {
            Self(time)
        }
    }

    impl services::Clock for FixedClock {
        fn now(&self) -> SystemTime {
            self.0
        }
    }

    fn service() -> SecretsManagerService {
        let factory = StorageFactory::new(StorageConfig::new(
            "/tmp/http-secrets-manager-tests",
            StorageMode::Memory,
        ));

        SecretsManagerService::new(
            &factory,
            Arc::new(FixedClock::new(UNIX_EPOCH)),
        )
    }

    fn context(operation: &str) -> RequestContext {
        RequestContext::try_new(
            "000000000000".parse().expect("account should parse"),
            "eu-west-2".parse().expect("region should parse"),
            ServiceName::SecretsManager,
            ProtocolFamily::AwsJson11,
            operation,
            None,
            false,
        )
        .expect("request context should build")
    }

    fn request_bytes(target: &str, body: &str) -> Vec<u8> {
        format!(
            "POST / HTTP/1.1\r\nHost: localhost:4566\r\nContent-Type: application/x-amz-json-1.1\r\nX-Amz-Target: {target}\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        )
        .into_bytes()
    }

    fn call(operation: &str, body: &str) -> Result<Vec<u8>, AwsError> {
        let target = format!("secretsmanager.{operation}");
        let request_bytes = request_bytes(&target, body);
        let request =
            HttpRequest::parse(&request_bytes).expect("request should parse");

        handle_json(&service(), &request, &context(operation))
    }

    fn call_with_service(
        service: &SecretsManagerService,
        operation: &str,
        body: &str,
    ) -> Result<Vec<u8>, AwsError> {
        let target = format!("secretsmanager.{operation}");
        let request_bytes = request_bytes(&target, body);
        let request =
            HttpRequest::parse(&request_bytes).expect("request should parse");

        handle_json(service, &request, &context(operation))
    }

    fn router() -> EdgeRouter {
        test_runtime::router_with_http_forwarder(
            "http-secrets-manager-router",
            None,
        )
    }

    fn split_response(
        response: &[u8],
    ) -> (String, Vec<(String, String)>, String) {
        let header_end = response
            .windows(4)
            .position(|window| window == b"\r\n\r\n")
            .expect("response should contain a header terminator");
        let headers = std::str::from_utf8(&response[..header_end])
            .expect("response headers should be UTF-8");
        let body = std::str::from_utf8(&response[header_end + 4..])
            .expect("response body should be UTF-8");
        let mut lines = headers.split("\r\n");
        let status =
            lines.next().expect("status line should exist").to_owned();
        let parsed_headers = lines
            .map(|line| {
                line.split_once(':').expect("header should contain ':'")
            })
            .map(|(name, value)| (name.to_owned(), value.trim().to_owned()))
            .collect();

        (status, parsed_headers, body.to_owned())
    }

    #[test]
    fn secrets_manager_json_direct_handler_round_trips_promoted_operations() {
        let service = service();
        let created = call_with_service(
            &service,
            "CreateSecret",
            r#"{"Name":"app/db/password","Description":"initial","SecretString":"alpha","Tags":[{"Key":"env","Value":"dev"}]}"#,
        )
            .expect("create should succeed");
        let get = call_with_service(
            &service,
            "GetSecretValue",
            r#"{"SecretId":"app/db/password"}"#,
        )
        .expect("get should succeed");
        let put = call_with_service(
            &service,
            "PutSecretValue",
            r#"{"SecretId":"app/db/password","ClientRequestToken":"1234567890abcdef1234567890abcdef","SecretString":"beta","VersionStages":["AWSPENDING"]}"#,
        )
            .expect("put should succeed");
        let get_pending = call_with_service(
            &service,
            "GetSecretValue",
            r#"{"SecretId":"app/db/password","VersionStage":"AWSPENDING"}"#,
        )
        .expect("pending get should succeed");
        let updated = call_with_service(
            &service,
            "UpdateSecret",
            r#"{"SecretId":"app/db/password","Description":"updated","SecretString":"gamma"}"#,
        )
            .expect("update should succeed");
        let rotated = call_with_service(
            &service,
            "RotateSecret",
            r#"{"SecretId":"app/db/password","RotationLambdaARN":"arn:aws:lambda:eu-west-2:000000000000:function:rotate","RotationRules":{"AutomaticallyAfterDays":7}}"#,
        )
            .expect("rotate should succeed");
        let describe = call_with_service(
            &service,
            "DescribeSecret",
            r#"{"SecretId":"app/db/password"}"#,
        )
        .expect("describe should succeed");
        let listed = call_with_service(
            &service,
            "ListSecrets",
            r#"{"Filters":[{"Key":"name","Values":["app/"]}]}"#,
        )
        .expect("list should succeed");
        let versions = call_with_service(
            &service,
            "ListSecretVersionIds",
            r#"{"SecretId":"app/db/password"}"#,
        )
        .expect("versions should succeed");
        let tag = call_with_service(
            &service,
            "TagResource",
            r#"{"SecretId":"app/db/password","Tags":[{"Key":"tier","Value":"backend"}]}"#,
        )
            .expect("tag should succeed");
        let untag = call_with_service(
            &service,
            "UntagResource",
            r#"{"SecretId":"app/db/password","TagKeys":["env"]}"#,
        )
        .expect("untag should succeed");
        let deleted = call_with_service(
            &service,
            "DeleteSecret",
            r#"{"SecretId":"app/db/password","RecoveryWindowInDays":7}"#,
        )
        .expect("delete should succeed");

        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&created)
                .expect("create response should decode")["Name"],
            "app/db/password"
        );
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&get)
                .expect("get response should decode")["SecretString"],
            "alpha"
        );
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&put)
                .expect("put response should decode")["VersionStages"],
            serde_json::json!(["AWSPENDING"])
        );
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&get_pending)
                .expect("pending get response should decode")["SecretString"],
            "beta"
        );
        assert!(
            serde_json::from_slice::<serde_json::Value>(&updated)
                .expect("update response should decode")["VersionId"]
                .as_str()
                .is_some()
        );
        assert!(
            serde_json::from_slice::<serde_json::Value>(&rotated)
                .expect("rotate response should decode")["VersionId"]
                .as_str()
                .is_some()
        );
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&describe)
                .expect("describe response should decode")["Description"],
            "updated"
        );
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&listed)
                .expect("list response should decode")["SecretList"][0]["Name"],
            "app/db/password"
        );
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&versions)
                .expect("versions response should decode")["Versions"]
                .as_array()
                .expect("versions should be an array")
                .len(),
            3
        );
        assert_eq!(tag, b"{}");
        assert_eq!(untag, b"{}");
        assert!(
            serde_json::from_slice::<serde_json::Value>(&deleted)
                .expect("delete response should decode")["DeletionDate"]
                .as_u64()
                .is_some()
        );
    }

    #[test]
    fn secrets_manager_json_direct_handlers_report_explicit_errors() {
        let service = service();
        let missing_target_request = HttpRequest::parse(
            b"POST / HTTP/1.1\r\nHost: localhost:4566\r\nContent-Type: application/x-amz-json-1.1\r\nContent-Length: 2\r\n\r\n{}",
        )
        .expect("request should parse");
        let missing_target = handle_json(
            &service,
            &missing_target_request,
            &context("CreateSecret"),
        )
        .expect_err("missing target should fail");
        let invalid_json_request = HttpRequest::parse(
            b"POST / HTTP/1.1\r\nHost: localhost:4566\r\nContent-Type: application/x-amz-json-1.1\r\nX-Amz-Target: secretsmanager.CreateSecret\r\nContent-Length: 1\r\n\r\n{",
        )
        .expect("request should parse");
        let invalid_json = handle_json(
            &service,
            &invalid_json_request,
            &context("CreateSecret"),
        )
        .expect_err("invalid JSON should fail");
        let invalid_binary = call(
            "CreateSecret",
            r#"{"Name":"app/binary","SecretBinary":"***"}"#,
        )
        .expect_err("invalid binary should fail");
        let unsupported_operation =
            call("Nope", "{}").expect_err("unsupported operation should fail");
        let invalid_delete = call(
            "DeleteSecret",
            r#"{"SecretId":"app/db/password","RecoveryWindowInDays":7,"ForceDeleteWithoutRecovery":true}"#,
        )
        .expect_err("conflicting delete flags should fail");
        let malformed_target = action_from_target(Some("secretsmanager"))
            .expect_err("targets without an operation should fail");
        let wrong_prefix = action_from_target(Some("AmazonSSM.GetParameter"))
            .expect_err("non-Secrets Manager targets should fail");

        assert_eq!(missing_target.code(), "UnsupportedOperation");
        assert_eq!(invalid_json.code(), "InvalidParameterException");
        assert_eq!(invalid_binary.code(), "InvalidParameterException");
        assert_eq!(unsupported_operation.code(), "UnsupportedOperation");
        assert_eq!(invalid_delete.code(), "ResourceNotFoundException");
        assert_eq!(
            malformed_target.to_aws_error().code(),
            "UnsupportedOperation"
        );
        assert_eq!(wrong_prefix.to_aws_error().code(), "UnsupportedOperation");
    }

    #[test]
    fn secrets_manager_json_helper_parsers_cover_filter_sort_and_serializer_failures()
     {
        #[derive(Debug)]
        struct FailingSerialize;

        impl Serialize for FailingSerialize {
            fn serialize<S>(&self, _serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                Err(S::Error::custom("boom"))
            }
        }

        let create = parse_create_secret_input(
            br#"{"Name":"app/binary","SecretBinary":"AQID"}"#,
        )
        .expect("create input should parse");
        let list = parse_list_secrets_input(
            br#"{"Filters":[{"Key":"name","Values":["app/"]}],"SortBy":"name","SortOrder":"asc"}"#,
        )
        .expect("list input should parse");
        let rotate = parse_rotate_secret_input(
            br#"{"SecretId":"app/db/password","RotationRules":{"AutomaticallyAfterDays":7}}"#,
        )
        .expect("rotate input should parse");
        let invalid_rotate_lambda = parse_rotate_secret_input(
            br#"{"SecretId":"app/db/password","RotationLambdaARN":"not-an-arn"}"#,
        )
        .expect_err("invalid rotation lambda ARN should fail");
        let serialize_error = json_response(&FailingSerialize)
            .expect_err("serializer failures should surface");

        assert_eq!(create.secret_binary, Some(vec![1, 2, 3]));
        assert_eq!(list.filters.len(), 1);
        assert_eq!(list.sort_by, Some(SecretsManagerListSecretsSortBy::Name));
        assert_eq!(
            list.sort_order,
            Some(SecretsManagerListSecretsSortOrder::Asc)
        );
        assert_eq!(
            rotate
                .rotation_rules
                .expect("rotation rules should exist")
                .automatically_after_days,
            Some(7)
        );
        assert_eq!(invalid_rotate_lambda.code(), "InvalidParameterException");
        assert_eq!(serialize_error.code(), "InternalServiceError");
    }

    #[test]
    fn secrets_manager_json_router_handles_aws_json_11_requests() {
        let router = router();
        let create_request = request_bytes(
            "secretsmanager.CreateSecret",
            r#"{"Name":"router/secret","SecretString":"alpha"}"#,
        );
        let get_request = request_bytes(
            "secretsmanager.GetSecretValue",
            r#"{"SecretId":"router/secret"}"#,
        );

        let create_response =
            router.handle_bytes(&create_request).to_http_bytes();
        let get_response = router.handle_bytes(&get_request).to_http_bytes();
        let (create_status, create_headers, _) =
            split_response(&create_response);
        let (get_status, get_headers, get_body) =
            split_response(&get_response);

        assert_eq!(create_status, "HTTP/1.1 200 OK");
        assert_eq!(get_status, "HTTP/1.1 200 OK");
        assert!(create_headers.iter().any(|(name, value)| {
            name.eq_ignore_ascii_case("content-type")
                && value == "application/x-amz-json-1.1"
        }));
        assert!(get_headers.iter().any(|(name, value)| {
            name.eq_ignore_ascii_case("content-type")
                && value == "application/x-amz-json-1.1"
        }));
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&get_body)
                .expect("response body should decode")["SecretString"],
            "alpha"
        );
    }
}
