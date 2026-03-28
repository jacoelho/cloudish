use crate::{
    LambdaError, LambdaScope, arns::function_arn_with_optional_latest,
    urls::LambdaFunctionUrlAuthType,
};
use aws::{
    Arn, ArnResource, CallerIdentity, ExecuteApiSourceArn,
    ExecuteApiSourceArnError, ServiceName,
};
use serde_json::{Map, Value};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AddPermissionInput {
    pub action: String,
    pub function_url_auth_type: Option<LambdaFunctionUrlAuthType>,
    pub principal: String,
    pub revision_id: Option<String>,
    pub source_arn: Option<String>,
    pub statement_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AddPermissionOutput {
    statement: String,
}

impl AddPermissionOutput {
    pub fn statement(&self) -> &str {
        &self.statement
    }
}

pub(crate) trait LambdaPermissionRecord {
    fn action(&self) -> &str;
    fn function_url_auth_type(&self) -> Option<LambdaFunctionUrlAuthType>;
    fn principal(&self) -> &str;
    fn source_arn(&self) -> Option<&str>;
    fn statement_id(&self) -> &str;
}

pub(crate) trait LambdaPermissionRevisionStore {
    fn latest_revision_id(&self) -> &str;
    fn alias_revision_id(&self, qualifier: &str) -> Option<&str>;
}

pub(crate) fn add_permission_output(statement: String) -> AddPermissionOutput {
    AddPermissionOutput { statement }
}

pub(crate) fn permission_revision_id<'a, State>(
    state: &'a State,
    qualifier: Option<&str>,
) -> Result<&'a str, LambdaError>
where
    State: LambdaPermissionRevisionStore,
{
    match qualifier {
        None => Ok(state.latest_revision_id()),
        Some(qualifier) => {
            state.alias_revision_id(qualifier).ok_or_else(|| {
                LambdaError::ResourceNotFound {
                    message: format!("Function alias not found: {qualifier}"),
                }
            })
        }
    }
}

pub(crate) fn validate_add_permission_input(
    input: &AddPermissionInput,
) -> Result<(), LambdaError> {
    if input.statement_id.trim().is_empty() {
        return Err(LambdaError::Validation {
            message: "StatementId is required.".to_owned(),
        });
    }
    if input.principal.trim().is_empty() {
        return Err(LambdaError::Validation {
            message: "Principal is required.".to_owned(),
        });
    }

    match input.action.as_str() {
        "lambda:InvokeFunctionUrl" => {
            if input.source_arn.is_some() {
                return Err(LambdaError::Validation {
                    message:
                        "SourceArn is not supported for lambda:InvokeFunctionUrl permissions."
                            .to_owned(),
                });
            }
        }
        "lambda:InvokeFunction" => {
            if input.function_url_auth_type.is_some() {
                return Err(LambdaError::Validation {
                    message:
                        "FunctionUrlAuthType is only valid for lambda:InvokeFunctionUrl permissions."
                            .to_owned(),
                });
            }
            let source_arn = input.source_arn.as_deref().ok_or_else(|| {
                LambdaError::Validation {
                    message:
                        "SourceArn is required for lambda:InvokeFunction permissions."
                            .to_owned(),
                }
            })?;
            match input.principal.as_str() {
                "apigateway.amazonaws.com" => {
                    validate_execute_api_source_arn(source_arn)?;
                }
                "events.amazonaws.com" => {
                    validate_eventbridge_source_arn(source_arn)?;
                }
                _ => {
                    return Err(LambdaError::UnsupportedOperation {
                        message: "Only API Gateway and EventBridge service-principal InvokeFunction permissions are supported by this Cloudish Lambda subset.".to_owned(),
                    });
                }
            }
        }
        _ => {
            return Err(LambdaError::UnsupportedOperation {
                message: "Only lambda:InvokeFunctionUrl and API Gateway or EventBridge lambda:InvokeFunction permissions are supported by this Cloudish Lambda subset.".to_owned(),
            });
        }
    }

    Ok(())
}

pub(crate) fn build_permission_statement<Permission>(
    scope: &LambdaScope,
    function_name: &str,
    qualifier: Option<&str>,
    permission: &Permission,
) -> Result<String, LambdaError>
where
    Permission: LambdaPermissionRecord,
{
    let mut statement = Map::from_iter([
        (
            "Sid".to_owned(),
            Value::String(permission.statement_id().to_owned()),
        ),
        ("Effect".to_owned(), Value::String("Allow".to_owned())),
        (
            "Principal".to_owned(),
            Value::String(permission.principal().to_owned()),
        ),
        ("Action".to_owned(), Value::String(permission.action().to_owned())),
        (
            "Resource".to_owned(),
            Value::String(function_arn_with_optional_latest(
                scope,
                function_name,
                qualifier,
            )),
        ),
    ]);
    let mut condition = Map::new();
    if let Some(function_url_auth_type) = permission.function_url_auth_type() {
        condition.insert(
            "StringEquals".to_owned(),
            serde_json::json!({
                "lambda:FunctionUrlAuthType":
                    function_url_auth_type_name(function_url_auth_type),
            }),
        );
    }
    if let Some(source_arn) = permission.source_arn() {
        condition.insert(
            "ArnLike".to_owned(),
            serde_json::json!({
                "AWS:SourceArn": source_arn,
            }),
        );
    }
    if !condition.is_empty() {
        statement.insert("Condition".to_owned(), Value::Object(condition));
    }

    serde_json::to_string(&statement)
        .map_err(|error| LambdaError::Internal { message: error.to_string() })
}

pub(crate) fn permission_allows_function_url_invoke<Permission>(
    permissions: Option<&BTreeMap<String, Permission>>,
    caller_identity: Option<&CallerIdentity>,
    auth_type: LambdaFunctionUrlAuthType,
) -> bool
where
    Permission: LambdaPermissionRecord,
{
    permissions.is_some_and(|permissions| {
        permissions.values().any(|permission| {
            if permission.action() != "lambda:InvokeFunctionUrl" {
                return false;
            }
            if permission
                .function_url_auth_type()
                .is_some_and(|allowed| allowed != auth_type)
            {
                return false;
            }
            if permission.principal() == "*" {
                return true;
            }
            let Some(caller_identity) = caller_identity else {
                return false;
            };
            permission.principal() == caller_identity.arn().to_string()
                || caller_identity.arn().account_id().is_some_and(
                    |account_id| permission.principal() == account_id.as_str(),
                )
        })
    })
}

pub(crate) fn permission_allows_service_invoke<Permission>(
    permissions: Option<&BTreeMap<String, Permission>>,
    principal: &str,
    source_arn: &str,
) -> bool
where
    Permission: LambdaPermissionRecord,
{
    permissions.is_some_and(|permissions| {
        permissions.values().any(|permission| {
            permission.action() == "lambda:InvokeFunction"
                && permission.principal() == principal
                && permission.source_arn().is_some_and(|pattern| {
                    wildcard_matches(pattern, source_arn)
                })
        })
    })
}

pub(crate) fn function_url_access_denied_error() -> LambdaError {
    LambdaError::AccessDenied {
        message: "Forbidden. For troubleshooting Function URL authorization issues, see: https://docs.aws.amazon.com/lambda/latest/dg/urls-auth.html".to_owned(),
    }
}

fn validate_execute_api_source_arn(
    source_arn: &str,
) -> Result<(), LambdaError> {
    ExecuteApiSourceArn::parse(source_arn)
        .map(|_| ())
        .map_err(execute_api_source_arn_error)
}

fn execute_api_source_arn_error(
    error: ExecuteApiSourceArnError,
) -> LambdaError {
    LambdaError::Validation {
        message: match error {
            ExecuteApiSourceArnError::InvalidArn(source) => {
                format!("SourceArn is invalid: {source}")
            }
            ExecuteApiSourceArnError::MissingScope
            | ExecuteApiSourceArnError::InvalidService => {
                "SourceArn is invalid.".to_owned()
            }
        },
    }
}

fn validate_eventbridge_source_arn(
    source_arn: &str,
) -> Result<(), LambdaError> {
    let parsed = source_arn.parse::<Arn>().map_err(|_| {
        LambdaError::Validation { message: "SourceArn is invalid.".to_owned() }
    })?;
    if parsed.service() != ServiceName::EventBridge {
        return Err(LambdaError::Validation {
            message: "SourceArn is invalid.".to_owned(),
        });
    }
    let Some(_region) = parsed.region() else {
        return Err(LambdaError::Validation {
            message: "SourceArn is invalid.".to_owned(),
        });
    };
    let Some(_account_id) = parsed.account_id() else {
        return Err(LambdaError::Validation {
            message: "SourceArn is invalid.".to_owned(),
        });
    };
    let ArnResource::Generic(resource) = parsed.resource() else {
        return Err(LambdaError::Validation {
            message: "SourceArn is invalid.".to_owned(),
        });
    };
    if !resource.starts_with("rule/") {
        return Err(LambdaError::Validation {
            message: "SourceArn is invalid.".to_owned(),
        });
    }

    Ok(())
}

fn function_url_auth_type_name(
    auth_type: LambdaFunctionUrlAuthType,
) -> &'static str {
    match auth_type {
        LambdaFunctionUrlAuthType::AwsIam => "AWS_IAM",
        LambdaFunctionUrlAuthType::None => "NONE",
    }
}

fn wildcard_matches(pattern: &str, candidate: &str) -> bool {
    let pattern = pattern.as_bytes();
    let candidate = candidate.as_bytes();
    let mut pattern_index = 0;
    let mut candidate_index = 0;
    let mut star_index = None;
    let mut match_index = 0;

    while candidate_index < candidate.len() {
        if pattern
            .get(pattern_index)
            .zip(candidate.get(candidate_index))
            .is_some_and(|(pattern_byte, candidate_byte)| {
                pattern_byte == candidate_byte
            })
        {
            pattern_index += 1;
            candidate_index += 1;
            continue;
        }

        if pattern
            .get(pattern_index)
            .is_some_and(|pattern_byte| *pattern_byte == b'*')
        {
            star_index = Some(pattern_index);
            pattern_index += 1;
            match_index = candidate_index;
            continue;
        }

        if let Some(star_index) = star_index {
            pattern_index = star_index + 1;
            match_index += 1;
            candidate_index = match_index;
            continue;
        }

        return false;
    }

    while pattern
        .get(pattern_index)
        .is_some_and(|pattern_byte| *pattern_byte == b'*')
    {
        pattern_index += 1;
    }

    pattern_index == pattern.len()
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws::Arn;

    #[derive(Debug, Clone)]
    struct TestPermission {
        action: String,
        function_url_auth_type: Option<LambdaFunctionUrlAuthType>,
        principal: String,
        source_arn: Option<String>,
        statement_id: String,
    }

    #[derive(Debug)]
    struct TestRevisionStore {
        alias_revisions: BTreeMap<String, String>,
        latest_revision_id: String,
    }

    impl LambdaPermissionRecord for TestPermission {
        fn action(&self) -> &str {
            &self.action
        }

        fn function_url_auth_type(&self) -> Option<LambdaFunctionUrlAuthType> {
            self.function_url_auth_type
        }

        fn principal(&self) -> &str {
            &self.principal
        }

        fn source_arn(&self) -> Option<&str> {
            self.source_arn.as_deref()
        }

        fn statement_id(&self) -> &str {
            &self.statement_id
        }
    }

    impl LambdaPermissionRevisionStore for TestRevisionStore {
        fn latest_revision_id(&self) -> &str {
            &self.latest_revision_id
        }

        fn alias_revision_id(&self, qualifier: &str) -> Option<&str> {
            self.alias_revisions.get(qualifier).map(String::as_str)
        }
    }

    #[test]
    fn add_permission_validation_tracks_supported_permission_families() {
        assert!(matches!(
            validate_add_permission_input(&AddPermissionInput {
                action: "lambda:InvokeFunction".to_owned(),
                function_url_auth_type: Some(LambdaFunctionUrlAuthType::None),
                principal: "apigateway.amazonaws.com".to_owned(),
                revision_id: None,
                source_arn: Some("arn:aws:execute-api:eu-west-2:000000000000:api/$default/GET/pets".to_owned()),
                statement_id: "sid".to_owned(),
            }),
            Err(LambdaError::Validation { message })
                if message.contains("FunctionUrlAuthType")
        ));
        assert!(matches!(
            validate_add_permission_input(&AddPermissionInput {
                action: "lambda:InvokeFunction".to_owned(),
                function_url_auth_type: None,
                principal: "sns.amazonaws.com".to_owned(),
                revision_id: None,
                source_arn: Some("arn:aws:execute-api:eu-west-2:000000000000:api/$default/GET/pets".to_owned()),
                statement_id: "sid".to_owned(),
            }),
            Err(LambdaError::UnsupportedOperation { .. })
        ));
        assert!(matches!(
            validate_add_permission_input(&AddPermissionInput {
                action: "lambda:InvokeFunction".to_owned(),
                function_url_auth_type: None,
                principal: "apigateway.amazonaws.com".to_owned(),
                revision_id: None,
                source_arn: Some(
                    "arn:aws:sns:eu-west-2:000000000000:topic".to_owned()
                ),
                statement_id: "sid".to_owned(),
            }),
            Err(LambdaError::Validation { .. })
        ));
        assert!(
            validate_add_permission_input(&AddPermissionInput {
                action: "lambda:InvokeFunction".to_owned(),
                function_url_auth_type: None,
                principal: "events.amazonaws.com".to_owned(),
                revision_id: None,
                source_arn: Some(
                    "arn:aws:events:eu-west-2:000000000000:rule/default/orders".to_owned()
                ),
                statement_id: "sid".to_owned(),
            })
            .is_ok()
        );
        assert!(
            validate_add_permission_input(&AddPermissionInput {
                action: "lambda:InvokeFunctionUrl".to_owned(),
                function_url_auth_type: Some(
                    LambdaFunctionUrlAuthType::AwsIam
                ),
                principal: "*".to_owned(),
                revision_id: None,
                source_arn: None,
                statement_id: "sid".to_owned(),
            })
            .is_ok()
        );
    }

    #[test]
    fn build_permission_statement_renders_auth_type_and_source_conditions() {
        let statement = build_permission_statement(
            &LambdaScope::new(
                "000000000000".parse::<aws::AccountId>().unwrap(),
                "eu-west-2".parse::<aws::RegionId>().unwrap(),
            ),
            "demo",
            Some("live"),
            &TestPermission {
                action: "lambda:InvokeFunction".to_owned(),
                function_url_auth_type: Some(LambdaFunctionUrlAuthType::AwsIam),
                principal: "apigateway.amazonaws.com".to_owned(),
                source_arn: Some(
                    "arn:aws:execute-api:eu-west-2:000000000000:api/$default/GET/pets"
                        .to_owned(),
                ),
                statement_id: "sid".to_owned(),
            },
        )
        .expect("statement should serialize");

        let value: Value =
            serde_json::from_str(&statement).expect("statement should decode");
        assert_eq!(value["Sid"], "sid");
        assert_eq!(
            value["Condition"]["StringEquals"]["lambda:FunctionUrlAuthType"],
            "AWS_IAM"
        );
        assert_eq!(
            value["Condition"]["ArnLike"]["AWS:SourceArn"],
            "arn:aws:execute-api:eu-west-2:000000000000:api/$default/GET/pets"
        );
    }

    #[test]
    fn permission_matching_handles_account_and_service_principals() {
        let caller_identity = CallerIdentity::try_new(
            "arn:aws:iam::000000000000:user/demo".parse::<Arn>().unwrap(),
            "AIDAEXAMPLE",
        )
        .unwrap();
        let permissions = BTreeMap::from([
            (
                "account".to_owned(),
                TestPermission {
                    action: "lambda:InvokeFunctionUrl".to_owned(),
                    function_url_auth_type: Some(
                        LambdaFunctionUrlAuthType::AwsIam,
                    ),
                    principal: "000000000000".to_owned(),
                    source_arn: None,
                    statement_id: "account".to_owned(),
                },
            ),
            (
                "service".to_owned(),
                TestPermission {
                    action: "lambda:InvokeFunction".to_owned(),
                    function_url_auth_type: None,
                    principal: "apigateway.amazonaws.com".to_owned(),
                    source_arn: Some(
                        "arn:aws:execute-api:eu-west-2:000000000000:api/*/GET/pets*"
                            .to_owned(),
                    ),
                    statement_id: "service".to_owned(),
                },
            ),
            (
                "events".to_owned(),
                TestPermission {
                    action: "lambda:InvokeFunction".to_owned(),
                    function_url_auth_type: None,
                    principal: "events.amazonaws.com".to_owned(),
                    source_arn: Some(
                        "arn:aws:events:eu-west-2:000000000000:rule/default/orders*"
                            .to_owned(),
                    ),
                    statement_id: "events".to_owned(),
                },
            ),
        ]);

        assert!(permission_allows_function_url_invoke(
            Some(&permissions),
            Some(&caller_identity),
            LambdaFunctionUrlAuthType::AwsIam,
        ));
        assert!(permission_allows_service_invoke(
            Some(&permissions),
            "apigateway.amazonaws.com",
            "arn:aws:execute-api:eu-west-2:000000000000:api/prod/GET/pets/1",
        ));
        assert!(!permission_allows_service_invoke(
            Some(&permissions),
            "apigateway.amazonaws.com",
            "arn:aws:execute-api:eu-west-2:000000000000:api/prod/POST/pets",
        ));
        assert!(permission_allows_service_invoke(
            Some(&permissions),
            "events.amazonaws.com",
            "arn:aws:events:eu-west-2:000000000000:rule/default/orders-created",
        ));
    }

    #[test]
    fn permission_revision_id_reads_latest_or_alias_revision() {
        let store = TestRevisionStore {
            alias_revisions: BTreeMap::from([(
                "live".to_owned(),
                "rev-live".to_owned(),
            )]),
            latest_revision_id: "rev-latest".to_owned(),
        };

        assert_eq!(
            permission_revision_id(&store, None).unwrap(),
            "rev-latest"
        );
        assert_eq!(
            permission_revision_id(&store, Some("live")).unwrap(),
            "rev-live"
        );

        match permission_revision_id(&store, Some("missing")) {
            Err(LambdaError::ResourceNotFound { message }) => {
                assert_eq!(message, "Function alias not found: missing");
            }
            other => panic!("expected missing alias error, got {other:?}"),
        }
    }
}
