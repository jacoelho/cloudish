use crate::{LambdaError, LambdaScope, arns::function_arn};
use serde::Serialize;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateAliasInput {
    pub description: Option<String>,
    pub function_version: String,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateAliasInput {
    pub description: Option<String>,
    pub function_version: Option<String>,
    pub revision_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct LambdaAliasConfiguration {
    pub(crate) alias_arn: String,
    pub(crate) description: String,
    pub(crate) function_version: String,
    pub(crate) name: String,
    pub(crate) revision_id: String,
}

impl LambdaAliasConfiguration {
    pub fn alias_arn(&self) -> &str {
        &self.alias_arn
    }

    pub fn description(&self) -> &str {
        &self.description
    }

    pub fn function_version(&self) -> &str {
        &self.function_version
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn revision_id(&self) -> &str {
        &self.revision_id
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct ListAliasesOutput {
    pub(crate) aliases: Vec<LambdaAliasConfiguration>,
}

impl ListAliasesOutput {
    pub fn aliases(&self) -> &[LambdaAliasConfiguration] {
        &self.aliases
    }
}

pub(crate) trait LambdaAliasRecord {
    fn description(&self) -> &str;
    fn function_version(&self) -> &str;
    fn revision_id(&self) -> &str;
}

pub(crate) fn validate_alias_name(name: &str) -> Result<(), LambdaError> {
    if name.trim().is_empty() {
        return Err(LambdaError::Validation {
            message: "Alias name cannot be blank.".to_owned(),
        });
    }
    if name.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(LambdaError::InvalidParameterValue {
            message: "Alias names cannot be entirely numeric.".to_owned(),
        });
    }
    if !name.bytes().all(|byte| {
        byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_')
    }) {
        return Err(LambdaError::Validation {
            message: "Alias names may contain only letters, digits, hyphens, and underscores.".to_owned(),
        });
    }

    Ok(())
}

pub(crate) fn lambda_alias_configuration<Alias>(
    scope: &LambdaScope,
    function_name: &str,
    alias_name: &str,
    alias: &Alias,
) -> LambdaAliasConfiguration
where
    Alias: LambdaAliasRecord,
{
    LambdaAliasConfiguration {
        alias_arn: format!(
            "{}:{}",
            function_arn(scope, function_name),
            alias_name
        ),
        description: alias.description().to_owned(),
        function_version: alias.function_version().to_owned(),
        name: alias_name.to_owned(),
        revision_id: alias.revision_id().to_owned(),
    }
}

pub(crate) fn list_aliases_output(
    aliases: Vec<LambdaAliasConfiguration>,
) -> ListAliasesOutput {
    ListAliasesOutput { aliases }
}

#[cfg(test)]
mod tests {
    use super::{
        CreateAliasInput, LambdaAliasRecord, ListAliasesOutput,
        UpdateAliasInput, lambda_alias_configuration, list_aliases_output,
        validate_alias_name,
    };
    use crate::{LambdaError, LambdaScope};
    use aws::{AccountId, RegionId};
    use serde_json::json;

    #[derive(Debug)]
    struct TestAlias {
        description: &'static str,
        function_version: &'static str,
        revision_id: &'static str,
    }

    impl LambdaAliasRecord for TestAlias {
        fn description(&self) -> &str {
            self.description
        }

        fn function_version(&self) -> &str {
            self.function_version
        }

        fn revision_id(&self) -> &str {
            self.revision_id
        }
    }

    fn scope() -> LambdaScope {
        LambdaScope::new(
            "000000000000".parse::<AccountId>().unwrap(),
            "eu-west-2".parse::<RegionId>().unwrap(),
        )
    }

    #[test]
    fn alias_contract_inputs_round_trip_public_fields() {
        assert_eq!(
            CreateAliasInput {
                description: Some("live".to_owned()),
                function_version: "1".to_owned(),
                name: "live".to_owned(),
            },
            CreateAliasInput {
                description: Some("live".to_owned()),
                function_version: "1".to_owned(),
                name: "live".to_owned(),
            }
        );
        assert_eq!(
            UpdateAliasInput {
                description: Some("blue".to_owned()),
                function_version: Some("2".to_owned()),
                revision_id: Some("rev-2".to_owned()),
            },
            UpdateAliasInput {
                description: Some("blue".to_owned()),
                function_version: Some("2".to_owned()),
                revision_id: Some("rev-2".to_owned()),
            }
        );
    }

    #[test]
    fn alias_validation_enforces_lambda_alias_contract() {
        assert!(validate_alias_name("live_1").is_ok());

        let blank = validate_alias_name("  ")
            .expect_err("blank aliases must be rejected");
        match blank {
            LambdaError::Validation { message } => {
                assert_eq!(message, "Alias name cannot be blank.");
            }
            other => panic!("expected validation error, got {other:?}"),
        }

        let numeric = validate_alias_name("123")
            .expect_err("numeric aliases must be rejected");
        match numeric {
            LambdaError::InvalidParameterValue { message } => {
                assert_eq!(message, "Alias names cannot be entirely numeric.");
            }
            other => {
                panic!("expected invalid parameter value, got {other:?}")
            }
        }

        let invalid = validate_alias_name("live.prod")
            .expect_err("invalid characters must be rejected");
        match invalid {
            LambdaError::Validation { message } => {
                assert_eq!(
                    message,
                    "Alias names may contain only letters, digits, hyphens, and underscores."
                );
            }
            other => panic!("expected validation error, got {other:?}"),
        }
    }

    #[test]
    fn alias_configuration_builder_and_list_output_shape_results() {
        let alias = lambda_alias_configuration(
            &scope(),
            "demo",
            "live",
            &TestAlias {
                description: "serves traffic",
                function_version: "3",
                revision_id: "rev-3",
            },
        );
        let output = list_aliases_output(vec![alias.clone()]);

        assert_eq!(
            alias.alias_arn(),
            "arn:aws:lambda:eu-west-2:000000000000:function:demo:live"
        );
        assert_eq!(alias.description(), "serves traffic");
        assert_eq!(alias.function_version(), "3");
        assert_eq!(alias.name(), "live");
        assert_eq!(alias.revision_id(), "rev-3");
        assert_eq!(output.aliases(), std::slice::from_ref(&alias));
        assert_eq!(
            serde_json::to_value(ListAliasesOutput { aliases: vec![alias] })
                .unwrap(),
            json!({
                "Aliases": [{
                    "AliasArn": "arn:aws:lambda:eu-west-2:000000000000:function:demo:live",
                    "Description": "serves traffic",
                    "FunctionVersion": "3",
                    "Name": "live",
                    "RevisionId": "rev-3"
                }]
            })
        );
    }
}
