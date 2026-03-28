use crate::caller::StsCaller;
use iam::IamRole;
use serde_json::Value;

pub(crate) fn trust_policy_allows(role: &IamRole, caller: &StsCaller) -> bool {
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

    statements.iter().any(|statement| {
        effect_is_allow(statement)
            && action_allows_assume_role(statement)
            && principal_matches(statement, caller)
    })
}

fn effect_is_allow(statement: &Value) -> bool {
    statement
        .get("Effect")
        .and_then(Value::as_str)
        .is_some_and(|effect| effect == "Allow")
}

fn action_allows_assume_role(statement: &Value) -> bool {
    match statement.get("Action") {
        Some(Value::String(action)) => {
            action == "sts:AssumeRole" || action == "*"
        }
        Some(Value::Array(actions)) => actions.iter().any(|action| {
            action.as_str().is_some_and(|action| {
                action == "sts:AssumeRole"
                    || action == "sts:*"
                    || action == "*"
            })
        }),
        _ => false,
    }
}

fn principal_matches(statement: &Value, caller: &StsCaller) -> bool {
    let Some(principal) = statement.get("Principal") else {
        return false;
    };
    if principal == &Value::String("*".to_owned()) {
        return true;
    }

    let Some(aws_principal) = principal.get("AWS") else {
        return false;
    };
    principal_values(aws_principal).into_iter().any(|candidate| {
        candidate == "*"
            || candidate == caller.account_id().as_str()
            || candidate == caller.arn().to_string()
            || candidate
                == format!("arn:aws:iam::{}:root", caller.account_id())
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
