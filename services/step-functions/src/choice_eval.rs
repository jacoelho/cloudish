use crate::{
    errors::StepFunctionsError,
    state::{JsonPath, PathLookup, parse_json_path, resolve_path},
};
use serde::Deserialize;
use serde_json::Value;

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum ChoiceRule {
    And { next: String, rules: Vec<ChoiceRule> },
    Or { next: String, rules: Vec<ChoiceRule> },
    Not { next: String, rule: Box<ChoiceRule> },
    Comparison { next: String, variable: JsonPath, predicate: ChoicePredicate },
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum ChoicePredicate {
    StringEquals(String),
    NumericEquals(f64),
    NumericLessThan(f64),
    NumericLessThanEquals(f64),
    NumericGreaterThan(f64),
    NumericGreaterThanEquals(f64),
    BooleanEquals(bool),
    IsNull(bool),
    IsPresent(bool),
    IsString(bool),
    IsNumeric(bool),
    IsBoolean(bool),
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "PascalCase", deny_unknown_fields)]
struct RawChoiceRule {
    and: Option<Vec<Value>>,
    boolean_equals: Option<bool>,
    is_boolean: Option<bool>,
    is_null: Option<bool>,
    is_numeric: Option<bool>,
    is_present: Option<bool>,
    is_string: Option<bool>,
    next: String,
    not: Option<Box<Value>>,
    numeric_equals: Option<f64>,
    numeric_greater_than: Option<f64>,
    numeric_greater_than_equals: Option<f64>,
    numeric_less_than: Option<f64>,
    numeric_less_than_equals: Option<f64>,
    or: Option<Vec<Value>>,
    string_equals: Option<String>,
    variable: Option<String>,
}

impl ChoiceRule {
    pub(crate) fn next(&self) -> &str {
        match self {
            Self::And { next, .. }
            | Self::Or { next, .. }
            | Self::Not { next, .. }
            | Self::Comparison { next, .. } => next,
        }
    }
}

pub(crate) fn parse_choice_rule(
    value: Value,
) -> Result<ChoiceRule, StepFunctionsError> {
    let raw: RawChoiceRule =
        serde_json::from_value(value).map_err(|error| {
            StepFunctionsError::InvalidDefinition {
                message: format!("Choice rule is invalid: {error}"),
            }
        })?;
    if raw.next.trim().is_empty() {
        return Err(StepFunctionsError::InvalidDefinition {
            message: "Choice rules must declare a non-empty Next target."
                .to_owned(),
        });
    }
    let operator_count = usize::from(raw.and.is_some())
        + usize::from(raw.or.is_some())
        + usize::from(raw.not.is_some())
        + usize::from(raw.string_equals.is_some())
        + usize::from(raw.numeric_equals.is_some())
        + usize::from(raw.numeric_less_than.is_some())
        + usize::from(raw.numeric_less_than_equals.is_some())
        + usize::from(raw.numeric_greater_than.is_some())
        + usize::from(raw.numeric_greater_than_equals.is_some())
        + usize::from(raw.boolean_equals.is_some())
        + usize::from(raw.is_null.is_some())
        + usize::from(raw.is_present.is_some())
        + usize::from(raw.is_string.is_some())
        + usize::from(raw.is_numeric.is_some())
        + usize::from(raw.is_boolean.is_some());
    if operator_count != 1 {
        return Err(StepFunctionsError::InvalidDefinition {
            message:
                "Choice rules must declare exactly one supported comparator or \
                 logical operator."
                    .to_owned(),
        });
    }

    if let Some(rules) = raw.and {
        return Ok(ChoiceRule::And {
            next: raw.next,
            rules: rules
                .into_iter()
                .map(parse_choice_rule)
                .collect::<Result<Vec<_>, _>>()?,
        });
    }
    if let Some(rules) = raw.or {
        return Ok(ChoiceRule::Or {
            next: raw.next,
            rules: rules
                .into_iter()
                .map(parse_choice_rule)
                .collect::<Result<Vec<_>, _>>()?,
        });
    }
    if let Some(rule) = raw.not {
        return Ok(ChoiceRule::Not {
            next: raw.next,
            rule: Box::new(parse_choice_rule(*rule)?),
        });
    }
    let variable =
        parse_json_path(raw.variable.as_deref().ok_or_else(|| {
            StepFunctionsError::InvalidDefinition {
                message:
                    "Choice comparison rules must declare a Variable path."
                        .to_owned(),
            }
        })?)?;
    let predicate = if let Some(value) = raw.string_equals {
        ChoicePredicate::StringEquals(value)
    } else if let Some(value) = raw.numeric_equals {
        ChoicePredicate::NumericEquals(value)
    } else if let Some(value) = raw.numeric_less_than {
        ChoicePredicate::NumericLessThan(value)
    } else if let Some(value) = raw.numeric_less_than_equals {
        ChoicePredicate::NumericLessThanEquals(value)
    } else if let Some(value) = raw.numeric_greater_than {
        ChoicePredicate::NumericGreaterThan(value)
    } else if let Some(value) = raw.numeric_greater_than_equals {
        ChoicePredicate::NumericGreaterThanEquals(value)
    } else if let Some(value) = raw.boolean_equals {
        ChoicePredicate::BooleanEquals(value)
    } else if let Some(value) = raw.is_null {
        ChoicePredicate::IsNull(value)
    } else if let Some(value) = raw.is_present {
        ChoicePredicate::IsPresent(value)
    } else if let Some(value) = raw.is_string {
        ChoicePredicate::IsString(value)
    } else if let Some(value) = raw.is_numeric {
        ChoicePredicate::IsNumeric(value)
    } else if let Some(value) = raw.is_boolean {
        ChoicePredicate::IsBoolean(value)
    } else {
        return Err(StepFunctionsError::InvalidDefinition {
            message: "Choice rule did not declare a supported comparator."
                .to_owned(),
        });
    };

    Ok(ChoiceRule::Comparison { next: raw.next, variable, predicate })
}

pub(crate) fn evaluate_choice_rules(
    choices: &[ChoiceRule],
    default: Option<&str>,
    input: &Value,
) -> Result<Option<String>, StepFunctionsError> {
    for choice in choices {
        if choice_matches(choice, input)? {
            return Ok(Some(choice.next().to_owned()));
        }
    }

    Ok(default.map(str::to_owned))
}

fn choice_matches(
    choice: &ChoiceRule,
    input: &Value,
) -> Result<bool, StepFunctionsError> {
    match choice {
        ChoiceRule::And { rules, .. } => {
            for rule in rules {
                if !choice_matches(rule, input)? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        ChoiceRule::Or { rules, .. } => {
            for rule in rules {
                if choice_matches(rule, input)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        ChoiceRule::Not { rule, .. } => Ok(!choice_matches(rule, input)?),
        ChoiceRule::Comparison { variable, predicate, .. } => {
            Ok(matches_predicate(resolve_path(input, variable), predicate))
        }
    }
}

fn matches_predicate(
    value: PathLookup<'_>,
    predicate: &ChoicePredicate,
) -> bool {
    match predicate {
        ChoicePredicate::StringEquals(expected) => value
            .present()
            .and_then(Value::as_str)
            .is_some_and(|actual| actual == expected),
        ChoicePredicate::NumericEquals(expected) => value
            .present()
            .and_then(Value::as_f64)
            .is_some_and(|actual| actual == *expected),
        ChoicePredicate::NumericLessThan(expected) => value
            .present()
            .and_then(Value::as_f64)
            .is_some_and(|actual| actual < *expected),
        ChoicePredicate::NumericLessThanEquals(expected) => value
            .present()
            .and_then(Value::as_f64)
            .is_some_and(|actual| actual <= *expected),
        ChoicePredicate::NumericGreaterThan(expected) => value
            .present()
            .and_then(Value::as_f64)
            .is_some_and(|actual| actual > *expected),
        ChoicePredicate::NumericGreaterThanEquals(expected) => value
            .present()
            .and_then(Value::as_f64)
            .is_some_and(|actual| actual >= *expected),
        ChoicePredicate::BooleanEquals(expected) => value
            .present()
            .and_then(Value::as_bool)
            .is_some_and(|actual| actual == *expected),
        ChoicePredicate::IsNull(expected) => match value {
            PathLookup::Missing => !expected,
            PathLookup::Present(value) => value.is_null() == *expected,
        },
        ChoicePredicate::IsPresent(expected) => {
            matches!(value, PathLookup::Present(_)) == *expected
        }
        ChoicePredicate::IsString(expected) => {
            value.present().is_some_and(|value| value.is_string() == *expected)
        }
        ChoicePredicate::IsNumeric(expected) => {
            value.present().is_some_and(|value| value.is_number() == *expected)
        }
        ChoicePredicate::IsBoolean(expected) => value
            .present()
            .is_some_and(|value| value.is_boolean() == *expected),
    }
}

impl<'a> PathLookup<'a> {
    fn present(&self) -> Option<&'a Value> {
        match self {
            Self::Missing => None,
            Self::Present(value) => Some(value),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{evaluate_choice_rules, parse_choice_rule};
    use serde_json::json;

    #[test]
    fn parse_choice_rule_rejects_multiple_operators() {
        let error = parse_choice_rule(json!({
            "Variable": "$.value",
            "BooleanEquals": true,
            "IsPresent": true,
            "Next": "Done"
        }))
        .expect_err("multiple operators should be rejected");

        assert_eq!(
            error.to_string(),
            "Choice rules must declare exactly one supported comparator or logical operator."
        );
    }

    #[test]
    fn evaluate_choice_rules_applies_logical_operators() {
        let first = parse_choice_rule(json!({
            "And": [
                {
                    "Variable": "$.route.invoke",
                    "BooleanEquals": true,
                    "Next": "ignored"
                },
                {
                    "Variable": "$.user.tier",
                    "StringEquals": "gold",
                    "Next": "ignored"
                }
            ],
            "Next": "Invoke"
        }))
        .expect("rule should parse");
        let second = parse_choice_rule(json!({
            "Variable": "$.route.invoke",
            "BooleanEquals": false,
            "Next": "Skip"
        }))
        .expect("rule should parse");

        let matched = evaluate_choice_rules(
            &[first, second],
            Some("Done"),
            &json!({
                "route": {"invoke": true},
                "user": {"tier": "gold"}
            }),
        )
        .expect("evaluation should succeed");

        assert_eq!(matched, Some("Invoke".to_owned()));
    }
}
