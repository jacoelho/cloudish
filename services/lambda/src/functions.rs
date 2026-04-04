use crate::{
    LambdaError, LambdaScope,
    arns::{function_arn_for_version, function_arn_with_optional_latest},
    code::LambdaCodeInput,
    state::LambdaPackageType,
};
use aws::{Arn, LambdaFunctionTarget, LambdaFunctionTargetError, ServiceName};
use serde::Serialize;
use std::collections::BTreeMap;
use time::OffsetDateTime;
use time::format_description::parse;

pub use crate::state::{LambdaService, LambdaServiceDependencies};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FunctionLocator {
    pub(crate) function_name: String,
    pub(crate) qualifier: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateFunctionInput {
    pub code: LambdaCodeInput,
    pub dead_letter_target_arn: Option<String>,
    pub description: Option<String>,
    pub environment: BTreeMap<String, String>,
    pub function_name: String,
    pub handler: Option<String>,
    pub memory_size: Option<u32>,
    pub package_type: LambdaPackageType,
    pub publish: bool,
    pub role: String,
    pub runtime: Option<String>,
    pub timeout: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct LambdaEnvironment {
    pub(crate) variables: BTreeMap<String, String>,
}

impl LambdaEnvironment {
    pub(crate) fn new(variables: BTreeMap<String, String>) -> Self {
        Self { variables }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct LambdaFunctionConfiguration {
    pub(crate) architectures: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) code_sha256: Option<String>,
    pub(crate) code_size: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) dead_letter_config: Option<LambdaDeadLetterConfig>,
    pub(crate) description: String,
    pub(crate) environment: LambdaEnvironment,
    pub(crate) function_arn: String,
    pub(crate) function_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) handler: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) image_uri: Option<String>,
    pub(crate) last_modified: String,
    pub(crate) memory_size: u32,
    pub(crate) package_type: String,
    pub(crate) revision_id: String,
    pub(crate) role: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) runtime: Option<String>,
    pub(crate) state: String,
    pub(crate) timeout: u32,
    pub(crate) version: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct LambdaDeadLetterConfig {
    pub(crate) target_arn: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct LambdaFunctionCodeLocation {
    pub(crate) repository_type: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct LambdaGetFunctionOutput {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) code: Option<LambdaFunctionCodeLocation>,
    pub(crate) configuration: LambdaFunctionConfiguration,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct ListFunctionsOutput {
    pub(crate) functions: Vec<LambdaFunctionConfiguration>,
}

pub(crate) trait LambdaFunctionVersionRecord {
    fn code_sha256(&self) -> Option<&str>;
    fn code_size(&self) -> usize;
    fn dead_letter_target_arn(&self) -> Option<&str>;
    fn description(&self) -> &str;
    fn environment(&self) -> &BTreeMap<String, String>;
    fn handler(&self) -> Option<&str>;
    fn image_uri(&self) -> Option<String>;
    fn last_modified_epoch_millis(&self) -> u64;
    fn memory_size(&self) -> u32;
    fn package_type_name(&self) -> &str;
    fn revision_id(&self) -> &str;
    fn role(&self) -> &str;
    fn runtime(&self) -> Option<&str>;
    fn timeout(&self) -> u32;
    fn version(&self) -> &str;
}

pub(crate) fn lambda_function_configuration<Version>(
    scope: &LambdaScope,
    function_name: &str,
    version: &Version,
) -> LambdaFunctionConfiguration
where
    Version: LambdaFunctionVersionRecord,
{
    LambdaFunctionConfiguration {
        architectures: vec!["x86_64".to_owned()],
        code_sha256: version.code_sha256().map(str::to_owned),
        code_size: version.code_size(),
        dead_letter_config: version.dead_letter_target_arn().map(
            |target_arn| LambdaDeadLetterConfig {
                target_arn: target_arn.to_owned(),
            },
        ),
        description: version.description().to_owned(),
        environment: LambdaEnvironment::new(version.environment().clone()),
        function_arn: function_arn_for_version(
            scope,
            function_name,
            version.version(),
        ),
        function_name: function_name.to_owned(),
        handler: version.handler().map(str::to_owned),
        image_uri: version.image_uri(),
        last_modified: format_last_modified(
            version.last_modified_epoch_millis(),
        ),
        memory_size: version.memory_size(),
        package_type: version.package_type_name().to_owned(),
        revision_id: version.revision_id().to_owned(),
        role: version.role().to_owned(),
        runtime: version.runtime().map(str::to_owned),
        state: "Active".to_owned(),
        timeout: version.timeout(),
        version: version.version().to_owned(),
    }
}

pub(crate) fn function_not_found_error(
    scope: &LambdaScope,
    function_name: &str,
    qualifier: Option<&str>,
) -> LambdaError {
    let function_arn =
        function_arn_with_optional_latest(scope, function_name, qualifier);

    LambdaError::ResourceNotFound {
        message: format!("Function not found: {function_arn}"),
    }
}

pub(crate) fn format_last_modified(epoch_millis: u64) -> String {
    const FALLBACK_TIMESTAMP: &str = "1970-01-01T00:00:00.000+0000";

    let Ok(timestamp) = OffsetDateTime::from_unix_timestamp_nanos(
        i128::from(epoch_millis).saturating_mul(1_000_000),
    ) else {
        return FALLBACK_TIMESTAMP.to_owned();
    };
    let Ok(format) = parse(
        "[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond digits:3]+0000",
    ) else {
        return FALLBACK_TIMESTAMP.to_owned();
    };

    timestamp.format(&format).unwrap_or_else(|_| FALLBACK_TIMESTAMP.to_owned())
}

pub(crate) fn validate_create_function_name(
    function_name: &str,
) -> Result<(), LambdaError> {
    if function_name.trim().is_empty() {
        return Err(LambdaError::InvalidParameterValue {
            message: "FunctionName is required".to_owned(),
        });
    }
    if function_name.len() > 64 {
        return Err(LambdaError::Validation {
            message: "FunctionName must not exceed 64 characters.".to_owned(),
        });
    }
    if !function_name.bytes().all(|byte| {
        byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_')
    }) {
        return Err(LambdaError::Validation {
            message: "FunctionName may contain only letters, digits, hyphens, and underscores.".to_owned(),
        });
    }

    Ok(())
}

pub(crate) fn validate_dead_letter_target_arn(
    target_arn: Option<&str>,
) -> Result<Option<String>, LambdaError> {
    let Some(target_arn) = target_arn else {
        return Ok(None);
    };
    if target_arn.trim().is_empty() {
        return Ok(None);
    }
    let arn = target_arn.parse::<Arn>().map_err(|error| {
        LambdaError::Validation { message: error.to_string() }
    })?;
    if arn.service() != ServiceName::Sqs {
        return Err(LambdaError::UnsupportedOperation {
            message: format!(
                "Dead-letter target service {} is not supported by this Cloudish Lambda subset.",
                arn.service().as_str()
            ),
        });
    }

    Ok(Some(target_arn.to_owned()))
}

pub(crate) fn resolve_function_locator(
    scope: &LambdaScope,
    function_name: &str,
    qualifier: Option<&str>,
) -> Result<FunctionLocator, LambdaError> {
    let target = resolve_lambda_function_target(function_name, qualifier)?;

    resolve_function_target(scope, &target)
}

pub(crate) fn resolve_unqualified_function(
    scope: &LambdaScope,
    function_name: &str,
) -> Result<FunctionLocator, LambdaError> {
    let target = LambdaFunctionTarget::parse(function_name)
        .map_err(lambda_function_target_error)?;

    if target.qualifier().is_some() {
        return Err(LambdaError::InvalidParameterValue {
            message: "Qualified function identifiers are not supported for this operation.".to_owned(),
        });
    }

    resolve_function_target(scope, &target)
}

pub(crate) fn resolve_function_target(
    scope: &LambdaScope,
    target: &LambdaFunctionTarget,
) -> Result<FunctionLocator, LambdaError> {
    if target.region().is_some_and(|region| region != scope.region()) {
        return Err(function_not_found_error(
            scope,
            &target.to_string(),
            None,
        ));
    }
    if target
        .account_id()
        .is_some_and(|account_id| account_id != scope.account_id())
    {
        return Err(function_not_found_error(
            scope,
            &target.to_string(),
            None,
        ));
    }

    Ok(FunctionLocator {
        function_name: target.function_name().to_owned(),
        qualifier: target.qualifier().map(str::to_owned),
    })
}

pub(crate) fn resolve_lambda_function_target(
    function_name: &str,
    qualifier: Option<&str>,
) -> Result<LambdaFunctionTarget, LambdaError> {
    let target = LambdaFunctionTarget::parse(function_name)
        .map_err(lambda_function_target_error)?;

    match qualifier {
        Some(qualifier) => target
            .with_qualifier(qualifier)
            .map_err(lambda_function_target_error),
        None => Ok(target),
    }
}

fn lambda_function_target_error(
    error: LambdaFunctionTargetError,
) -> LambdaError {
    match error {
        LambdaFunctionTargetError::QualifierConflict
        | LambdaFunctionTargetError::MissingQualifier => {
            LambdaError::InvalidParameterValue { message: error.to_string() }
        }
        LambdaFunctionTargetError::MissingFunctionName
        | LambdaFunctionTargetError::InvalidArn(_)
        | LambdaFunctionTargetError::InvalidService
        | LambdaFunctionTargetError::InvalidResourceShape
        | LambdaFunctionTargetError::InvalidResourceType => {
            LambdaError::Validation { message: error.to_string() }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        CreateFunctionInput, LambdaDeadLetterConfig, LambdaEnvironment,
        LambdaFunctionCodeLocation, LambdaFunctionConfiguration,
        LambdaGetFunctionOutput, ListFunctionsOutput, format_last_modified,
        function_not_found_error, resolve_function_locator,
        resolve_lambda_function_target, resolve_unqualified_function,
        validate_create_function_name, validate_dead_letter_target_arn,
    };
    use crate::{
        LambdaCodeInput, LambdaError, LambdaPackageType, LambdaScope,
    };
    use aws::{AccountId, RegionId};
    use serde_json::json;
    use std::collections::BTreeMap;

    fn scope() -> LambdaScope {
        LambdaScope::new(
            "000000000000".parse::<AccountId>().unwrap(),
            "eu-west-2".parse::<RegionId>().unwrap(),
        )
    }

    fn configuration() -> LambdaFunctionConfiguration {
        LambdaFunctionConfiguration {
            architectures: vec!["x86_64".to_owned()],
            code_sha256: Some("digest".to_owned()),
            code_size: 9,
            dead_letter_config: Some(LambdaDeadLetterConfig {
                target_arn: "arn:aws:sqs:eu-west-2:000000000000:dead-letter"
                    .to_owned(),
            }),
            description: "demo".to_owned(),
            environment: LambdaEnvironment::new(BTreeMap::from([(
                "MODE".to_owned(),
                "test".to_owned(),
            )])),
            function_arn:
                "arn:aws:lambda:eu-west-2:000000000000:function:demo"
                    .to_owned(),
            function_name: "demo".to_owned(),
            handler: Some("bootstrap.handler".to_owned()),
            image_uri: None,
            last_modified: "2026-03-27T00:00:00.000+0000".to_owned(),
            memory_size: 128,
            package_type: "Zip".to_owned(),
            revision_id: "rev-1".to_owned(),
            role: "arn:aws:iam::000000000000:role/service-role/lambda-role"
                .to_owned(),
            runtime: Some("provided.al2".to_owned()),
            state: "Active".to_owned(),
            timeout: 3,
            version: "$LATEST".to_owned(),
        }
    }

    #[test]
    fn create_function_input_round_trips_code_contract() {
        assert_eq!(
            CreateFunctionInput {
                code: LambdaCodeInput::InlineZip {
                    archive: b"zip-bytes".to_vec(),
                },
                dead_letter_target_arn: None,
                description: Some("demo".to_owned()),
                environment: BTreeMap::from([(
                    "MODE".to_owned(),
                    "test".to_owned(),
                )]),
                function_name: "demo".to_owned(),
                handler: Some("bootstrap.handler".to_owned()),
                memory_size: Some(128),
                package_type: LambdaPackageType::Zip,
                publish: false,
                role:
                    "arn:aws:iam::000000000000:role/service-role/lambda-role"
                        .to_owned(),
                runtime: Some("provided.al2".to_owned()),
                timeout: Some(3),
            },
            CreateFunctionInput {
                code: LambdaCodeInput::InlineZip {
                    archive: b"zip-bytes".to_vec(),
                },
                dead_letter_target_arn: None,
                description: Some("demo".to_owned()),
                environment: BTreeMap::from([(
                    "MODE".to_owned(),
                    "test".to_owned(),
                )]),
                function_name: "demo".to_owned(),
                handler: Some("bootstrap.handler".to_owned()),
                memory_size: Some(128),
                package_type: LambdaPackageType::Zip,
                publish: false,
                role:
                    "arn:aws:iam::000000000000:role/service-role/lambda-role"
                        .to_owned(),
                runtime: Some("provided.al2".to_owned()),
                timeout: Some(3),
            }
        );
    }

    #[test]
    fn lambda_function_contract_serializes_aws_shaped_fields() {
        let output = LambdaGetFunctionOutput {
            code: Some(LambdaFunctionCodeLocation {
                repository_type: "S3".to_owned(),
            }),
            configuration: configuration(),
        };

        assert_eq!(
            serde_json::to_value(&output).unwrap(),
            json!({
                "Code": {
                    "RepositoryType": "S3"
                },
                "Configuration": {
                    "Architectures": ["x86_64"],
                    "CodeSha256": "digest",
                    "CodeSize": 9,
                    "DeadLetterConfig": {
                        "TargetArn": "arn:aws:sqs:eu-west-2:000000000000:dead-letter"
                    },
                    "Description": "demo",
                    "Environment": {
                        "Variables": {
                            "MODE": "test"
                        }
                    },
                    "FunctionArn": "arn:aws:lambda:eu-west-2:000000000000:function:demo",
                    "FunctionName": "demo",
                    "Handler": "bootstrap.handler",
                    "LastModified": "2026-03-27T00:00:00.000+0000",
                    "MemorySize": 128,
                    "PackageType": "Zip",
                    "RevisionId": "rev-1",
                    "Role": "arn:aws:iam::000000000000:role/service-role/lambda-role",
                    "Runtime": "provided.al2",
                    "State": "Active",
                    "Timeout": 3,
                    "Version": "$LATEST"
                }
            })
        );
    }

    #[test]
    fn list_functions_output_serializes_function_array() {
        assert_eq!(
            serde_json::to_value(ListFunctionsOutput {
                functions: vec![configuration()],
            })
            .unwrap(),
            json!({
                "Functions": [{
                    "Architectures": ["x86_64"],
                    "CodeSha256": "digest",
                    "CodeSize": 9,
                    "DeadLetterConfig": {
                        "TargetArn": "arn:aws:sqs:eu-west-2:000000000000:dead-letter"
                    },
                    "Description": "demo",
                    "Environment": {
                        "Variables": {
                            "MODE": "test"
                        }
                    },
                    "FunctionArn": "arn:aws:lambda:eu-west-2:000000000000:function:demo",
                    "FunctionName": "demo",
                    "Handler": "bootstrap.handler",
                    "LastModified": "2026-03-27T00:00:00.000+0000",
                    "MemorySize": 128,
                    "PackageType": "Zip",
                    "RevisionId": "rev-1",
                    "Role": "arn:aws:iam::000000000000:role/service-role/lambda-role",
                    "Runtime": "provided.al2",
                    "State": "Active",
                    "Timeout": 3,
                    "Version": "$LATEST"
                }]
            })
        );
    }

    #[test]
    fn function_target_helpers_resolve_lambda_identifiers() {
        let locator = resolve_function_locator(&scope(), "demo", Some("live"))
            .expect("qualified locator should resolve");
        assert_eq!(locator.function_name, "demo");
        assert_eq!(locator.qualifier.as_deref(), Some("live"));

        let arn_locator = resolve_function_locator(
            &scope(),
            "arn:aws:lambda:eu-west-2:000000000000:function:demo:3",
            None,
        )
        .expect("arn locator should resolve");
        assert_eq!(arn_locator.function_name, "demo");
        assert_eq!(arn_locator.qualifier.as_deref(), Some("3"));

        let target = resolve_lambda_function_target("demo", Some("live"))
            .expect("qualifier should attach to target");
        assert_eq!(target.function_name(), "demo");
        assert_eq!(target.qualifier(), Some("live"));

        let unqualified = resolve_unqualified_function(&scope(), "demo")
            .expect("unqualified operations should accept plain names");
        assert_eq!(unqualified.function_name, "demo");
        assert_eq!(unqualified.qualifier, None);
    }

    #[test]
    fn function_target_helpers_reject_invalid_or_cross_scope_targets() {
        match resolve_unqualified_function(
            &scope(),
            "arn:aws:lambda:eu-west-2:000000000000:function:demo:live",
        ) {
            Err(LambdaError::InvalidParameterValue { message }) => {
                assert_eq!(
                    message,
                    "Qualified function identifiers are not supported for this operation."
                );
            }
            other => {
                panic!("expected unqualified target error, got {other:?}")
            }
        }

        match resolve_function_locator(
            &scope(),
            "arn:aws:lambda:us-east-1:000000000000:function:demo",
            None,
        ) {
            Err(LambdaError::ResourceNotFound { message }) => {
                assert_eq!(
                    message,
                    "Function not found: arn:aws:lambda:eu-west-2:000000000000:function:arn:aws:lambda:us-east-1:000000000000:function:demo:$LATEST"
                );
            }
            other => {
                panic!("expected cross-scope not-found error, got {other:?}")
            }
        }

        match resolve_lambda_function_target("", None) {
            Err(LambdaError::Validation { .. }) => {}
            other => panic!("expected validation error, got {other:?}"),
        }

        match function_not_found_error(&scope(), "demo", None) {
            LambdaError::ResourceNotFound { message } => {
                assert_eq!(
                    message,
                    "Function not found: arn:aws:lambda:eu-west-2:000000000000:function:demo:$LATEST"
                );
            }
            other => panic!("expected not-found error, got {other:?}"),
        }
    }

    #[test]
    fn function_contract_helpers_validate_names_dead_letter_targets_and_timestamps()
     {
        assert!(validate_create_function_name("demo_1").is_ok());
        assert_eq!(format_last_modified(0), "1970-01-01T00:00:00.000+0000");
        assert_eq!(
            validate_dead_letter_target_arn(Some(
                "arn:aws:sqs:eu-west-2:000000000000:dead-letter"
            ))
            .unwrap(),
            Some("arn:aws:sqs:eu-west-2:000000000000:dead-letter".to_owned())
        );
        assert_eq!(validate_dead_letter_target_arn(Some("  ")).unwrap(), None);

        match validate_create_function_name(" ") {
            Err(LambdaError::InvalidParameterValue { message }) => {
                assert_eq!(message, "FunctionName is required");
            }
            other => panic!("expected required-name error, got {other:?}"),
        }

        match validate_create_function_name(&"a".repeat(65)) {
            Err(LambdaError::Validation { message }) => {
                assert_eq!(
                    message,
                    "FunctionName must not exceed 64 characters."
                );
            }
            other => panic!("expected length validation error, got {other:?}"),
        }

        match validate_create_function_name("demo.prod") {
            Err(LambdaError::Validation { message }) => {
                assert_eq!(
                    message,
                    "FunctionName may contain only letters, digits, hyphens, and underscores."
                );
            }
            other => {
                panic!("expected character validation error, got {other:?}")
            }
        }

        match validate_dead_letter_target_arn(Some(
            "arn:aws:sns:eu-west-2:000000000000:topic",
        )) {
            Err(LambdaError::UnsupportedOperation { message }) => {
                assert_eq!(
                    message,
                    "Dead-letter target service sns is not supported by this Cloudish Lambda subset."
                );
            }
            other => panic!(
                "expected unsupported dead-letter target, got {other:?}"
            ),
        }
    }
}
