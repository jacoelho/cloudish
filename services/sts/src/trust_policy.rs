use crate::caller::StsCaller;
use aws::{AccountId, Arn};
use iam::IamRole;
use serde_json::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TrustAction {
    Role,
    WebIdentity,
    Saml,
}

impl TrustAction {
    fn iam_action(self) -> &'static str {
        match self {
            Self::Role => "sts:AssumeRole",
            Self::WebIdentity => "sts:AssumeRoleWithWebIdentity",
            Self::Saml => "sts:AssumeRoleWithSAML",
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum TrustPrincipal<'a> {
    Aws(&'a StsCaller),
    WebIdentity { provider: &'a str, account_id: &'a AccountId },
    Saml { principal_arn: &'a Arn },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StatementEffect {
    Allow,
    Deny,
}

pub(crate) fn trust_policy_allows(
    role: &IamRole,
    action: TrustAction,
    principal: TrustPrincipal<'_>,
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
        if !action_matches(statement, action)
            || !principal_matches(statement, principal)
        {
            continue;
        }

        match effect(statement) {
            Some(StatementEffect::Deny) => return false,
            Some(StatementEffect::Allow)
                if !statement_has_condition(statement) =>
            {
                allow_found = true;
            }
            Some(StatementEffect::Allow) | None => {}
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

fn statement_has_condition(statement: &Value) -> bool {
    statement.get("Condition").is_some()
}

fn action_matches(statement: &Value, expected_action: TrustAction) -> bool {
    match statement.get("Action") {
        Some(Value::String(action)) => {
            action_matches_value(action, expected_action)
        }
        Some(Value::Array(actions)) => actions.iter().any(|action| {
            action.as_str().is_some_and(|action| {
                action_matches_value(action, expected_action)
            })
        }),
        _ => false,
    }
}

fn action_matches_value(action: &str, expected_action: TrustAction) -> bool {
    action == expected_action.iam_action()
        || action == "sts:*"
        || action == "*"
}

fn principal_matches(
    statement: &Value,
    principal: TrustPrincipal<'_>,
) -> bool {
    let Some(statement_principal) = statement.get("Principal") else {
        return false;
    };
    if statement_principal == &Value::String("*".to_owned()) {
        return true;
    }

    match principal {
        TrustPrincipal::Aws(caller) => statement_principal
            .get("AWS")
            .is_some_and(|value| aws_principal_matches(value, caller)),
        TrustPrincipal::WebIdentity { provider, account_id } => {
            statement_principal.get("Federated").is_some_and(|value| {
                federated_principal_matches(
                    value,
                    &[
                        provider.to_owned(),
                        oidc_provider_arn(account_id, provider),
                    ],
                )
            })
        }
        TrustPrincipal::Saml { principal_arn } => {
            statement_principal.get("Federated").is_some_and(|value| {
                federated_principal_matches(
                    value,
                    &[principal_arn.to_string()],
                )
            })
        }
    }
}

fn aws_principal_matches(value: &Value, caller: &StsCaller) -> bool {
    principal_values(value).into_iter().any(|candidate| {
        candidate == "*"
            || candidate == caller.account_id().as_str()
            || candidate == caller.arn().to_string()
            || candidate
                == format!("arn:aws:iam::{}:root", caller.account_id())
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

fn oidc_provider_arn(account_id: &AccountId, provider: &str) -> String {
    format!("arn:aws:iam::{account_id}:oidc-provider/{provider}")
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
