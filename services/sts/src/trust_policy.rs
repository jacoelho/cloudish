use crate::caller::StsCaller;
use aws::{AccountId, Arn};
use iam::IamRole;
use serde_json::Value;
use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TrustAction {
    Role,
    Saml,
    TagSession,
    WebIdentity,
}

impl TrustAction {
    fn iam_action(self) -> &'static str {
        match self {
            Self::Role => "sts:AssumeRole",
            Self::Saml => "sts:AssumeRoleWithSAML",
            Self::TagSession => "sts:TagSession",
            Self::WebIdentity => "sts:AssumeRoleWithWebIdentity",
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) enum TrustPrincipal {
    Aws {
        account_id: AccountId,
        canonical_principal_arn: Arn,
        presented_principal_arn: Arn,
    },
    Saml {
        principal_arn: Arn,
    },
    WebIdentity {
        trusted_federated_principals: Vec<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TrustConditionContext {
    values: BTreeMap<String, Vec<String>>,
}

impl TrustConditionContext {
    pub(crate) fn from_values(values: BTreeMap<String, Vec<String>>) -> Self {
        Self { values }
    }

    fn value(&self, key: &str) -> Option<&[String]> {
        self.values.get(key).map(Vec::as_slice)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct TrustEvaluationInput {
    pub(crate) action: TrustAction,
    pub(crate) condition_context: TrustConditionContext,
    pub(crate) principal: TrustPrincipal,
}

impl TrustEvaluationInput {
    pub(crate) fn aws(caller: &StsCaller, action: TrustAction) -> Self {
        let canonical_principal = caller.canonical_trust_principal();
        let mut values = BTreeMap::from([
            (
                "aws:PrincipalAccount".to_owned(),
                vec![caller.account_id().to_string()],
            ),
            (
                "aws:PrincipalArn".to_owned(),
                vec![
                    caller.arn().to_string(),
                    canonical_principal.arn().to_string(),
                ],
            ),
            (
                "aws:PrincipalType".to_owned(),
                vec![canonical_principal.principal_type().as_str().to_owned()],
            ),
        ]);
        if let Some(username) = canonical_principal.username() {
            values
                .insert("aws:username".to_owned(), vec![username.to_owned()]);
        }
        if let Some(role_session_name) = caller.role_session_name() {
            values.insert(
                "sts:RoleSessionName".to_owned(),
                vec![role_session_name.to_owned()],
            );
        }

        Self {
            action,
            condition_context: TrustConditionContext::from_values(values),
            principal: TrustPrincipal::Aws {
                account_id: caller.account_id().clone(),
                canonical_principal_arn: canonical_principal.arn().clone(),
                presented_principal_arn: caller.arn().clone(),
            },
        }
    }

    pub(crate) fn saml(
        principal_arn: &Arn,
        condition_values: &BTreeMap<String, Vec<String>>,
    ) -> Self {
        Self {
            action: TrustAction::Saml,
            condition_context: TrustConditionContext::from_values(
                condition_values.clone(),
            ),
            principal: TrustPrincipal::Saml {
                principal_arn: principal_arn.clone(),
            },
        }
    }

    pub(crate) fn web_identity(
        trusted_federated_principals: &[String],
        condition_values: &BTreeMap<String, Vec<String>>,
    ) -> Self {
        Self {
            action: TrustAction::WebIdentity,
            condition_context: TrustConditionContext::from_values(
                condition_values.clone(),
            ),
            principal: TrustPrincipal::WebIdentity {
                trusted_federated_principals: trusted_federated_principals
                    .to_vec(),
            },
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StatementEffect {
    Allow,
    Deny,
}

const AWS_SUPPORTED_CONDITION_KEYS: &[&str] = &[
    "aws:PrincipalArn",
    "aws:PrincipalAccount",
    "aws:PrincipalType",
    "aws:username",
    "sts:RoleSessionName",
];
const SAML_SUPPORTED_CONDITION_KEYS: &[&str] = &[
    "SAML:aud",
    "SAML:iss",
    "SAML:sub",
    "SAML:sub_type",
    "SAML:namequalifier",
    "sts:RoleSessionName",
];

pub(crate) fn trust_policy_allows(
    role: &IamRole,
    input: TrustEvaluationInput,
) -> bool {
    let Ok(document) =
        serde_json::from_str::<Value>(&role.assume_role_policy_document)
    else {
        return false;
    };
    let statements = match document.get("Statement") {
        Some(Value::Array(statements)) => statements.as_slice(),
        Some(statement) => std::slice::from_ref(statement),
        None => return false,
    };
    let mut allow_found = false;

    for statement in statements {
        if !action_matches(statement, input.action)
            || !principal_matches(statement, &input.principal)
        {
            continue;
        }

        let Some(effect) = effect(statement) else {
            continue;
        };

        let condition_result = statement
            .get("Condition")
            .map(|condition| {
                condition_matches(
                    condition,
                    &input.condition_context,
                    &input.principal,
                )
            })
            .unwrap_or(Ok(true));
        let Ok(condition_matches) = condition_result else {
            return false;
        };
        if !condition_matches {
            continue;
        }

        match effect {
            StatementEffect::Deny => return false,
            StatementEffect::Allow => allow_found = true,
        }
    }

    allow_found
}

fn effect(statement: &Value) -> Option<StatementEffect> {
    match statement.get("Effect").and_then(Value::as_str) {
        Some("Allow") => Some(StatementEffect::Allow),
        Some("Deny") => Some(StatementEffect::Deny),
        _ => None,
    }
}

fn action_matches(statement: &Value, expected_action: TrustAction) -> bool {
    string_or_array_values(statement.get("Action"))
        .any(|action| action_matches_value(action, expected_action))
}

fn action_matches_value(action: &str, expected_action: TrustAction) -> bool {
    action == expected_action.iam_action()
        || action == "sts:*"
        || action == "*"
}

fn principal_matches(statement: &Value, principal: &TrustPrincipal) -> bool {
    let Some(statement_principal) = statement.get("Principal") else {
        return false;
    };
    if statement_principal == &Value::String("*".to_owned()) {
        return true;
    }

    match principal {
        TrustPrincipal::Aws {
            account_id,
            canonical_principal_arn,
            presented_principal_arn,
        } => statement_principal.get("AWS").is_some_and(|value| {
            aws_principal_matches(
                value,
                account_id,
                canonical_principal_arn,
                presented_principal_arn,
            )
        }),
        TrustPrincipal::Saml { principal_arn } => {
            statement_principal.get("Federated").is_some_and(|value| {
                federated_principal_matches(
                    value,
                    &[principal_arn.to_string()],
                )
            })
        }
        TrustPrincipal::WebIdentity { trusted_federated_principals } => {
            statement_principal.get("Federated").is_some_and(|value| {
                federated_principal_matches(
                    value,
                    trusted_federated_principals,
                )
            })
        }
    }
}

fn aws_principal_matches(
    value: &Value,
    account_id: &AccountId,
    canonical_principal_arn: &Arn,
    presented_principal_arn: &Arn,
) -> bool {
    let root_arn = format!("arn:aws:iam::{account_id}:root");

    principal_values(value).into_iter().any(|candidate| {
        candidate == "*"
            || candidate == account_id.as_str()
            || candidate == root_arn
            || candidate == canonical_principal_arn.to_string()
            || candidate == presented_principal_arn.to_string()
    })
}

fn federated_principal_matches(
    value: &Value,
    expected_values: &[String],
) -> bool {
    principal_values(value).into_iter().any(|candidate| {
        candidate == "*"
            || expected_values.iter().any(|expected| expected == &candidate)
    })
}

fn principal_values(value: &Value) -> Vec<String> {
    match value {
        Value::String(value) => vec![value.clone()],
        Value::Array(values) => values
            .iter()
            .filter_map(Value::as_str)
            .map(str::to_owned)
            .collect(),
        _ => Vec::new(),
    }
}

fn condition_matches(
    condition: &Value,
    context: &TrustConditionContext,
    principal: &TrustPrincipal,
) -> Result<bool, ()> {
    let operators = condition.as_object().ok_or(())?;

    for (operator, clauses) in operators {
        let clauses = clauses.as_object().ok_or(())?;
        for (key, expected) in clauses {
            if !is_supported_condition_key(key, principal) {
                return Err(());
            }
            if !condition_clause_matches(operator, key, expected, context)? {
                return Ok(false);
            }
        }
    }

    Ok(true)
}

fn is_supported_condition_key(key: &str, principal: &TrustPrincipal) -> bool {
    supported_condition_keys(principal).contains(&key)
        || is_supported_web_identity_condition_key(principal, key)
}

fn condition_clause_matches(
    operator: &str,
    key: &str,
    expected: &Value,
    context: &TrustConditionContext,
) -> Result<bool, ()> {
    let actual = context.value(key).unwrap_or(&[]);
    match operator {
        "StringEquals" | "ForAnyValue:StringEquals" => {
            evaluate_any_string_condition(actual, expected, string_equals)
        }
        "StringLike" | "ForAnyValue:StringLike" => {
            evaluate_any_string_condition(actual, expected, glob_matches)
        }
        "ForAllValues:StringEquals" => {
            evaluate_all_string_condition(actual, expected, string_equals)
        }
        "ForAllValues:StringLike" => {
            evaluate_all_string_condition(actual, expected, glob_matches)
        }
        "Null" => match expected {
            Value::Bool(expected_is_null) => {
                Ok(actual.is_empty() == *expected_is_null)
            }
            Value::String(expected_is_null) => {
                let expected_is_null =
                    matches!(expected_is_null.as_str(), "true" | "True");
                Ok(actual.is_empty() == expected_is_null)
            }
            _ => Err(()),
        },
        _ => Err(()),
    }
}

fn string_condition_values(expected: &Value) -> Result<Vec<String>, ()> {
    match expected {
        Value::String(expected) => Ok(vec![expected.clone()]),
        Value::Array(values) => values
            .iter()
            .map(Value::as_str)
            .collect::<Option<Vec<_>>>()
            .map(|values| values.into_iter().map(str::to_owned).collect())
            .ok_or(()),
        _ => Err(()),
    }
}

fn any_value_matches(
    actual: &[String],
    expected: &[String],
    matcher: impl Fn(&str, &str) -> bool,
) -> bool {
    !actual.is_empty()
        && actual.iter().any(|actual| {
            expected.iter().any(|expected| matcher(actual, expected))
        })
}

fn all_value_matches(
    actual: &[String],
    expected: &[String],
    matcher: impl Fn(&str, &str) -> bool,
) -> bool {
    actual.iter().all(|actual| {
        expected.iter().any(|expected| matcher(actual, expected))
    })
}

fn evaluate_any_string_condition(
    actual: &[String],
    expected: &Value,
    matcher: impl Fn(&str, &str) -> bool,
) -> Result<bool, ()> {
    let expected = string_condition_values(expected)?;
    Ok(any_value_matches(actual, &expected, matcher))
}

fn evaluate_all_string_condition(
    actual: &[String],
    expected: &Value,
    matcher: impl Fn(&str, &str) -> bool,
) -> Result<bool, ()> {
    let expected = string_condition_values(expected)?;
    Ok(all_value_matches(actual, &expected, matcher))
}

fn string_equals(actual: &str, expected: &str) -> bool {
    actual == expected
}

fn supported_condition_keys(
    principal: &TrustPrincipal,
) -> &'static [&'static str] {
    match principal {
        TrustPrincipal::Aws { .. } => AWS_SUPPORTED_CONDITION_KEYS,
        TrustPrincipal::Saml { .. } => SAML_SUPPORTED_CONDITION_KEYS,
        TrustPrincipal::WebIdentity { .. } => &["sts:RoleSessionName"],
    }
}

fn is_supported_web_identity_condition_key(
    principal: &TrustPrincipal,
    key: &str,
) -> bool {
    matches!(principal, TrustPrincipal::WebIdentity { .. })
        && key
            .split_once(':')
            .is_some_and(|(_, suffix)| matches!(suffix, "aud" | "sub" | "amr"))
}

fn string_or_array_values(
    value: Option<&Value>,
) -> impl Iterator<Item = &str> {
    value.into_iter().flat_map(|value| match value {
        Value::String(value) => vec![value.as_str()],
        Value::Array(values) => {
            values.iter().filter_map(Value::as_str).collect::<Vec<_>>()
        }
        _ => Vec::new(),
    })
}

fn glob_matches(actual: &str, pattern: &str) -> bool {
    glob_matches_bytes(actual.as_bytes(), pattern.as_bytes())
}

fn glob_matches_bytes(actual: &[u8], pattern: &[u8]) -> bool {
    let (mut actual_index, mut pattern_index) = (0, 0);
    let mut star_index = None;
    let mut star_match_index = 0;

    while actual_index < actual.len() {
        match pattern.get(pattern_index) {
            Some(b'?') => {
                actual_index += 1;
                pattern_index += 1;
            }
            Some(b'*') => {
                star_index = Some(pattern_index);
                pattern_index += 1;
                star_match_index = actual_index;
            }
            Some(expected) if actual.get(actual_index) == Some(expected) => {
                actual_index += 1;
                pattern_index += 1;
            }
            _ => {
                let Some(star_index) = star_index else {
                    return false;
                };
                pattern_index = star_index + 1;
                star_match_index += 1;
                actual_index = star_match_index;
            }
        }
    }

    while matches!(pattern.get(pattern_index), Some(b'*')) {
        pattern_index += 1;
    }

    pattern_index == pattern.len()
}

#[cfg(test)]
mod tests {
    use super::{
        TrustAction, TrustConditionContext, TrustEvaluationInput,
        TrustPrincipal, glob_matches, trust_policy_allows,
    };
    use crate::caller::StsCaller;
    use aws::{
        Arn, AwsPrincipalType, CallerCredentialKind, CallerIdentity,
        StableAwsPrincipal,
    };
    use iam::IamRole;
    use std::collections::BTreeMap;
    use std::collections::BTreeSet;

    fn caller() -> StsCaller {
        StsCaller::new(
            "123456789012".parse().expect("account id should parse"),
            CallerCredentialKind::Temporary(
                aws::TemporaryCredentialKind::AssumedRole {
                    role_arn: "arn:aws:iam::123456789012:role/source"
                        .parse::<Arn>()
                        .expect("role ARN should parse"),
                    role_session_name: "build".to_owned(),
                },
            ),
            CallerIdentity::try_new(
                "arn:aws:sts::123456789012:assumed-role/source/build"
                    .parse::<Arn>()
                    .expect("session ARN should parse"),
                "AROA123EXAMPLE:build",
            )
            .expect("identity should build"),
            Vec::new(),
            BTreeSet::new(),
        )
    }

    fn role(document: &str) -> IamRole {
        IamRole {
            arn: "arn:aws:iam::123456789012:role/demo"
                .parse::<Arn>()
                .expect("role ARN should parse"),
            assume_role_policy_document: document.to_owned(),
            create_date: "2026-03-28T00:00:00Z".to_owned(),
            description: String::new(),
            max_session_duration: 3600,
            path: "/".to_owned(),
            role_id: "AROA1234567890EXAMPLE".to_owned(),
            role_name: "demo".to_owned(),
        }
    }

    #[test]
    fn aws_trust_matches_canonical_role_arn_for_chained_assume_role() {
        let role = role(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::123456789012:role/source"},"Action":"sts:AssumeRole"}]}"#,
        );

        assert!(trust_policy_allows(
            &role,
            TrustEvaluationInput::aws(&caller(), TrustAction::Role)
        ));
    }

    #[test]
    fn deny_conditions_only_apply_when_the_condition_matches() {
        let role = role(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"123456789012"},"Action":"sts:AssumeRole"},{"Effect":"Deny","Principal":{"AWS":"arn:aws:iam::123456789012:role/source"},"Action":"sts:AssumeRole","Condition":{"StringEquals":{"sts:RoleSessionName":"other"}}}]}"#,
        );

        assert!(trust_policy_allows(
            &role,
            TrustEvaluationInput::aws(&caller(), TrustAction::Role)
        ));
    }

    #[test]
    fn oidc_condition_keys_support_string_like_matching() {
        let role = role(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Federated":"arn:aws:iam::123456789012:oidc-provider/token.actions.githubusercontent.com"},"Action":"sts:AssumeRoleWithWebIdentity","Condition":{"StringLike":{"token.actions.githubusercontent.com:sub":"repo:cloudish:*"}}}]}"#,
        );

        assert!(trust_policy_allows(
            &role,
            TrustEvaluationInput::web_identity(
                &[
                    "token.actions.githubusercontent.com".to_owned(),
                    "arn:aws:iam::123456789012:oidc-provider/token.actions.githubusercontent.com"
                        .to_owned(),
                ],
                &BTreeMap::from([(
                    "token.actions.githubusercontent.com:sub".to_owned(),
                    vec!["repo:cloudish:ref:refs/heads/main".to_owned()],
                )]),
            )
        ));
    }

    #[test]
    fn glob_matches_supports_star_suffixes() {
        assert!(glob_matches("repo:cloudish:ref", "repo:cloudish:*"));
        assert!(!glob_matches("repo:other:ref", "repo:cloudish:*"));
    }

    #[test]
    fn glob_matches_supports_question_marks_and_middle_stars() {
        assert!(glob_matches(
            "repo:cloudish:prod:deploy",
            "repo:cloudish:*:dep?oy"
        ));
        assert!(glob_matches("abcz", "a*z"));
        assert!(!glob_matches(
            "repo:cloudish:prod:deploy",
            "repo:cloudish:*:dep??o"
        ));
    }

    #[test]
    fn for_all_values_string_like_matches_empty_actual_sets() {
        let role = role(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Federated":"arn:aws:iam::123456789012:oidc-provider/token.actions.githubusercontent.com"},"Action":"sts:AssumeRoleWithWebIdentity","Condition":{"ForAllValues:StringLike":{"token.actions.githubusercontent.com:amr":"auth*"}}}]}"#,
        );

        assert!(trust_policy_allows(
            &role,
            TrustEvaluationInput::web_identity(
                &[
                    "token.actions.githubusercontent.com".to_owned(),
                    "arn:aws:iam::123456789012:oidc-provider/token.actions.githubusercontent.com"
                        .to_owned(),
                ],
                &BTreeMap::new(),
            )
        ));
    }

    #[test]
    fn aws_condition_context_exposes_principal_type_and_username() {
        let caller = StsCaller::new(
            "123456789012".parse().expect("account id should parse"),
            CallerCredentialKind::LongTerm(StableAwsPrincipal::new(
                "arn:aws:iam::123456789012:user/alice"
                    .parse::<Arn>()
                    .expect("user ARN should parse"),
                AwsPrincipalType::User,
                Some("alice".to_owned()),
            )),
            CallerIdentity::try_new(
                "arn:aws:iam::123456789012:user/alice"
                    .parse::<Arn>()
                    .expect("user ARN should parse"),
                "AIDA1234567890EXAMPLE",
            )
            .expect("identity should build"),
            Vec::new(),
            BTreeSet::new(),
        );
        let input = TrustEvaluationInput::aws(&caller, TrustAction::Role);

        assert!(matches!(input.principal, TrustPrincipal::Aws { .. }));
        assert_eq!(
            input.condition_context.value("aws:PrincipalType"),
            Some(&["User".to_owned()][..])
        );
        assert_eq!(
            input.condition_context.value("aws:username"),
            Some(&["alice".to_owned()][..])
        );
    }

    #[test]
    fn unsupported_condition_keys_fail_closed_for_aws_principals() {
        let role = role(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"123456789012"},"Action":"sts:AssumeRole","Condition":{"StringEquals":{"aws:MultiFactorAuthPresent":"true"}}}]}"#,
        );
        let input = TrustEvaluationInput {
            action: TrustAction::Role,
            condition_context: TrustConditionContext::from_values(
                BTreeMap::from([(
                    "aws:MultiFactorAuthPresent".to_owned(),
                    vec!["true".to_owned()],
                )]),
            ),
            principal: TrustPrincipal::Aws {
                account_id: "123456789012"
                    .parse()
                    .expect("account id should parse"),
                canonical_principal_arn:
                    "arn:aws:iam::123456789012:role/source"
                        .parse()
                        .expect("principal ARN should parse"),
                presented_principal_arn:
                    "arn:aws:sts::123456789012:assumed-role/source/build"
                        .parse()
                        .expect("presented ARN should parse"),
            },
        };

        assert!(!trust_policy_allows(&role, input));
    }

    #[test]
    fn unsupported_condition_keys_fail_closed_for_saml_principals() {
        let role = role(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Federated":"arn:aws:iam::123456789012:saml-provider/example"},"Action":"sts:AssumeRoleWithSAML","Condition":{"StringEquals":{"SAML:department":"eng"}}}]}"#,
        );
        let input = TrustEvaluationInput {
            action: TrustAction::Saml,
            condition_context: TrustConditionContext::from_values(
                BTreeMap::from([(
                    "SAML:department".to_owned(),
                    vec!["eng".to_owned()],
                )]),
            ),
            principal: TrustPrincipal::Saml {
                principal_arn:
                    "arn:aws:iam::123456789012:saml-provider/example"
                        .parse()
                        .expect("principal ARN should parse"),
            },
        };

        assert!(!trust_policy_allows(&role, input));
    }

    #[test]
    fn unsupported_condition_keys_fail_closed_for_web_identity_principals() {
        let role = role(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Federated":"arn:aws:iam::123456789012:oidc-provider/token.actions.githubusercontent.com"},"Action":"sts:AssumeRoleWithWebIdentity","Condition":{"StringEquals":{"token.actions.githubusercontent.com:email":"dev@example.com"}}}]}"#,
        );
        let input = TrustEvaluationInput::web_identity(
            &[
                "token.actions.githubusercontent.com".to_owned(),
                "arn:aws:iam::123456789012:oidc-provider/token.actions.githubusercontent.com"
                    .to_owned(),
            ],
            &BTreeMap::from([(
                "token.actions.githubusercontent.com:email".to_owned(),
                vec!["dev@example.com".to_owned()],
            )]),
        );

        assert!(!trust_policy_allows(&role, input));
    }
}
