use crate::{
    LambdaError, LambdaScope,
    functions::{LambdaFunctionConfiguration, function_not_found_error},
};
use serde::Serialize;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishVersionInput {
    pub description: Option<String>,
    pub revision_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct ListVersionsByFunctionOutput {
    pub(crate) versions: Vec<LambdaFunctionConfiguration>,
}

impl ListVersionsByFunctionOutput {
    pub fn versions(&self) -> &[LambdaFunctionConfiguration] {
        &self.versions
    }
}

pub(crate) fn list_versions_by_function_output(
    versions: Vec<LambdaFunctionConfiguration>,
) -> ListVersionsByFunctionOutput {
    ListVersionsByFunctionOutput { versions }
}

pub(crate) trait LambdaAliasVersionRecord {
    fn function_version(&self) -> &str;
}

pub(crate) trait LambdaVersionStore {
    type Alias: LambdaAliasVersionRecord;
    type Version;

    fn latest(&self) -> &Self::Version;
    fn alias(&self, name: &str) -> Option<&Self::Alias>;
    fn version(&self, version: &str) -> Option<&Self::Version>;
}

pub(crate) fn ensure_version_exists<State>(
    state: &State,
    version: &str,
) -> Result<(), LambdaError>
where
    State: LambdaVersionStore,
{
    if version == "$LATEST" || state.version(version).is_some() {
        return Ok(());
    }

    Err(LambdaError::ResourceNotFound {
        message: format!("Function version not found: {version}"),
    })
}

pub(crate) fn resolve_version_state<'a, State>(
    scope: &LambdaScope,
    function_name: &str,
    state: &'a State,
    qualifier: Option<&str>,
) -> Result<&'a State::Version, LambdaError>
where
    State: LambdaVersionStore,
{
    resolve_version_with_execution_target(
        scope,
        function_name,
        state,
        qualifier,
    )
    .map(|(version, _)| version)
}

pub(crate) fn resolve_version_with_execution_target<'a, State>(
    scope: &LambdaScope,
    function_name: &str,
    state: &'a State,
    qualifier: Option<&str>,
) -> Result<(&'a State::Version, String), LambdaError>
where
    State: LambdaVersionStore,
{
    let qualifier = qualifier.unwrap_or("$LATEST");

    if qualifier == "$LATEST" {
        return Ok((state.latest(), "$LATEST".to_owned()));
    }

    if let Some(alias) = state.alias(qualifier) {
        if alias.function_version() == "$LATEST" {
            return Ok((state.latest(), "$LATEST".to_owned()));
        }
        let version =
            state.version(alias.function_version()).ok_or_else(|| {
                function_not_found_error(scope, function_name, Some(qualifier))
            })?;

        return Ok((version, alias.function_version().to_owned()));
    }

    let version = state.version(qualifier).ok_or_else(|| {
        function_not_found_error(scope, function_name, Some(qualifier))
    })?;

    Ok((version, qualifier.to_owned()))
}

#[cfg(test)]
mod tests {
    use super::{
        LambdaAliasVersionRecord, LambdaVersionStore,
        ListVersionsByFunctionOutput, PublishVersionInput,
        ensure_version_exists, resolve_version_state,
        resolve_version_with_execution_target,
    };
    use crate::functions::{
        LambdaDeadLetterConfig, LambdaEnvironment, LambdaFunctionConfiguration,
    };
    use crate::{LambdaError, LambdaScope};
    use aws::{AccountId, RegionId};
    use serde_json::json;
    use std::collections::BTreeMap;

    #[derive(Debug)]
    struct TestAlias {
        function_version: &'static str,
    }

    impl LambdaAliasVersionRecord for TestAlias {
        fn function_version(&self) -> &str {
            self.function_version
        }
    }

    #[derive(Debug)]
    struct TestVersionStore {
        aliases: BTreeMap<&'static str, TestAlias>,
        latest: &'static str,
        versions: BTreeMap<&'static str, &'static str>,
    }

    impl LambdaVersionStore for TestVersionStore {
        type Alias = TestAlias;
        type Version = &'static str;

        fn latest(&self) -> &Self::Version {
            &self.latest
        }

        fn alias(&self, name: &str) -> Option<&Self::Alias> {
            self.aliases.get(name)
        }

        fn version(&self, version: &str) -> Option<&Self::Version> {
            self.versions.get(version)
        }
    }

    fn scope() -> LambdaScope {
        LambdaScope::new(
            "000000000000".parse::<AccountId>().unwrap(),
            "eu-west-2".parse::<RegionId>().unwrap(),
        )
    }

    fn configuration(version: &str) -> LambdaFunctionConfiguration {
        LambdaFunctionConfiguration {
            architectures: vec!["x86_64".to_owned()],
            code_sha256: Some("digest".to_owned()),
            code_size: 9,
            dead_letter_config: Some(LambdaDeadLetterConfig {
                target_arn: "arn:aws:sqs:eu-west-2:000000000000:dead-letter"
                    .to_owned(),
            }),
            description: format!("demo-{version}"),
            environment: LambdaEnvironment::new(BTreeMap::from([(
                "MODE".to_owned(),
                "test".to_owned(),
            )])),
            function_arn: format!(
                "arn:aws:lambda:eu-west-2:000000000000:function:demo:{version}"
            ),
            function_name: "demo".to_owned(),
            handler: Some("bootstrap.handler".to_owned()),
            image_uri: None,
            last_modified: "2026-03-27T00:00:00.000+0000".to_owned(),
            memory_size: 128,
            package_type: "Zip".to_owned(),
            revision_id: format!("rev-{version}"),
            role: "arn:aws:iam::000000000000:role/service-role/lambda-role"
                .to_owned(),
            runtime: Some("provided.al2".to_owned()),
            state: "Active".to_owned(),
            timeout: 3,
            version: version.to_owned(),
        }
    }

    #[test]
    fn publish_version_input_round_trips_optional_fields() {
        assert_eq!(
            PublishVersionInput {
                description: Some("v1".to_owned()),
                revision_id: Some("rev-1".to_owned()),
            },
            PublishVersionInput {
                description: Some("v1".to_owned()),
                revision_id: Some("rev-1".to_owned()),
            }
        );
    }

    #[test]
    fn list_versions_output_exposes_versions_and_serializes() {
        let output = ListVersionsByFunctionOutput {
            versions: vec![configuration("$LATEST"), configuration("1")],
        };

        assert_eq!(output.versions().len(), 2);
        assert_eq!(
            serde_json::to_value(&output).unwrap(),
            json!({
                "Versions": [
                    {
                        "Architectures": ["x86_64"],
                        "CodeSha256": "digest",
                        "CodeSize": 9,
                        "DeadLetterConfig": {
                            "TargetArn": "arn:aws:sqs:eu-west-2:000000000000:dead-letter"
                        },
                        "Description": "demo-$LATEST",
                        "Environment": {
                            "Variables": {
                                "MODE": "test"
                            }
                        },
                        "FunctionArn": "arn:aws:lambda:eu-west-2:000000000000:function:demo:$LATEST",
                        "FunctionName": "demo",
                        "Handler": "bootstrap.handler",
                        "LastModified": "2026-03-27T00:00:00.000+0000",
                        "MemorySize": 128,
                        "PackageType": "Zip",
                        "RevisionId": "rev-$LATEST",
                        "Role": "arn:aws:iam::000000000000:role/service-role/lambda-role",
                        "Runtime": "provided.al2",
                        "State": "Active",
                        "Timeout": 3,
                        "Version": "$LATEST"
                    },
                    {
                        "Architectures": ["x86_64"],
                        "CodeSha256": "digest",
                        "CodeSize": 9,
                        "DeadLetterConfig": {
                            "TargetArn": "arn:aws:sqs:eu-west-2:000000000000:dead-letter"
                        },
                        "Description": "demo-1",
                        "Environment": {
                            "Variables": {
                                "MODE": "test"
                            }
                        },
                        "FunctionArn": "arn:aws:lambda:eu-west-2:000000000000:function:demo:1",
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
                        "Version": "1"
                    }
                ]
            })
        );
    }

    #[test]
    fn version_resolution_helpers_handle_latest_aliases_and_published_versions()
     {
        let state = TestVersionStore {
            aliases: BTreeMap::from([
                ("live", TestAlias { function_version: "2" }),
                ("current", TestAlias { function_version: "$LATEST" }),
            ]),
            latest: "$LATEST",
            versions: BTreeMap::from([("1", "1"), ("2", "2")]),
        };

        assert!(ensure_version_exists(&state, "$LATEST").is_ok());
        assert!(ensure_version_exists(&state, "1").is_ok());
        assert_eq!(
            resolve_version_state(&scope(), "demo", &state, None).unwrap(),
            &"$LATEST"
        );
        assert_eq!(
            resolve_version_state(&scope(), "demo", &state, Some("1"))
                .unwrap(),
            &"1"
        );
        assert_eq!(
            resolve_version_with_execution_target(
                &scope(),
                "demo",
                &state,
                Some("live"),
            )
            .unwrap(),
            (&"2", "2".to_owned())
        );
        assert_eq!(
            resolve_version_with_execution_target(
                &scope(),
                "demo",
                &state,
                Some("current"),
            )
            .unwrap(),
            (&"$LATEST", "$LATEST".to_owned())
        );
    }

    #[test]
    fn version_resolution_helpers_report_missing_versions_and_qualifiers() {
        let state = TestVersionStore {
            aliases: BTreeMap::from([(
                "broken",
                TestAlias { function_version: "99" },
            )]),
            latest: "$LATEST",
            versions: BTreeMap::from([("1", "1")]),
        };

        match ensure_version_exists(&state, "2") {
            Err(LambdaError::ResourceNotFound { message }) => {
                assert_eq!(message, "Function version not found: 2");
            }
            other => panic!("expected missing version error, got {other:?}"),
        }

        match resolve_version_with_execution_target(
            &scope(),
            "demo",
            &state,
            Some("missing"),
        ) {
            Err(LambdaError::ResourceNotFound { message }) => {
                assert_eq!(
                    message,
                    "Function not found: arn:aws:lambda:eu-west-2:000000000000:function:demo:missing"
                );
            }
            other => panic!("expected missing qualifier error, got {other:?}"),
        }

        match resolve_version_with_execution_target(
            &scope(),
            "demo",
            &state,
            Some("broken"),
        ) {
            Err(LambdaError::ResourceNotFound { message }) => {
                assert_eq!(
                    message,
                    "Function not found: arn:aws:lambda:eu-west-2:000000000000:function:demo:broken"
                );
            }
            other => panic!("expected broken alias error, got {other:?}"),
        }
    }
}
