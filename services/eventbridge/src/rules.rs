use crate::buses::DEFAULT_EVENT_BUS_NAME;
use crate::delivery::rule_arn;
use crate::errors::EventBridgeError;
use crate::patterns::{EventPattern, parse_event_pattern};
use crate::schedules::{ParsedScheduleExpression, parse_schedule_expression};
use crate::scope::EventBridgeScope;
use crate::targets::EventBridgeTarget;
use aws::{AccountId, Arn, RegionId};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

const DEFAULT_RULE_PAGE_SIZE: usize = 100;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EventBridgeRuleState {
    #[serde(rename = "DISABLED")]
    Disabled,
    #[serde(rename = "ENABLED")]
    Enabled,
}

impl EventBridgeRuleState {
    pub(crate) fn is_enabled(self) -> bool {
        self == Self::Enabled
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct PutRuleInput {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_bus_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_pattern: Option<String>,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub role_arn: Option<Arn>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schedule_expression: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state: Option<EventBridgeRuleState>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct PutRuleOutput {
    pub rule_arn: Arn,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct DescribeRuleOutput {
    pub arn: Arn,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub event_bus_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_pattern: Option<String>,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub role_arn: Option<Arn>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schedule_expression: Option<String>,
    pub state: EventBridgeRuleState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ListRulesInput {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_bus_name: Option<String>,
    pub limit: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name_prefix: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_token: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct RuleSummary {
    pub arn: Arn,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub event_bus_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_pattern: Option<String>,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub role_arn: Option<Arn>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schedule_expression: Option<String>,
    pub state: EventBridgeRuleState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ListRulesOutput {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_token: Option<String>,
    pub rules: Vec<RuleSummary>,
}

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
pub(crate) struct EventBridgeRuleKey {
    pub(crate) account_id: AccountId,
    pub(crate) bus_name: String,
    pub(crate) name: String,
    pub(crate) region: RegionId,
}

impl EventBridgeRuleKey {
    pub(crate) fn new(
        scope: &EventBridgeScope,
        bus_name: &str,
        name: &str,
    ) -> Result<Self, EventBridgeError> {
        validate_rule_name(name)?;

        Ok(Self {
            account_id: scope.account_id().clone(),
            bus_name: bus_name.to_owned(),
            name: name.to_owned(),
            region: scope.region().clone(),
        })
    }

    pub(crate) fn scope(&self) -> EventBridgeScope {
        EventBridgeScope::new(self.account_id.clone(), self.region.clone())
    }

    pub(crate) fn rule_arn(&self) -> Arn {
        rule_arn(&self.scope(), &self.bus_name, &self.name)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct StoredRule {
    pub(crate) description: Option<String>,
    pub(crate) event_pattern: Option<EventPattern>,
    pub(crate) event_pattern_source: Option<String>,
    pub(crate) role_arn: Option<Arn>,
    pub(crate) schedule_expression: Option<ParsedScheduleExpression>,
    pub(crate) schedule_expression_source: Option<String>,
    pub(crate) state: EventBridgeRuleState,
    pub(crate) targets: BTreeMap<String, EventBridgeTarget>,
}

pub(crate) fn normalize_rule_bus_name(
    event_bus_name: Option<&str>,
) -> Result<String, EventBridgeError> {
    crate::buses::normalize_bus_name(event_bus_name)
}

pub(crate) fn validate_rule_name(name: &str) -> Result<(), EventBridgeError> {
    if name.is_empty()
        || name.len() > 64
        || name.chars().any(|character| {
            !character.is_ascii_alphanumeric()
                && character != '.'
                && character != '-'
                && character != '_'
        })
    {
        return Err(EventBridgeError::Validation {
            message: "Rule name must be 1-64 characters of letters, digits, '.', '-', or '_'.".to_owned(),
        });
    }

    Ok(())
}

pub(crate) fn validate_put_rule(
    input: &PutRuleInput,
    event_bus_name: &str,
) -> Result<ValidatedPutRule, EventBridgeError> {
    validate_rule_name(&input.name)?;
    if input.event_pattern.is_none() && input.schedule_expression.is_none() {
        return Err(EventBridgeError::Validation {
            message:
                "Either EventPattern or ScheduleExpression must be provided."
                    .to_owned(),
        });
    }
    if input.role_arn.is_some() {
        return Err(EventBridgeError::UnsupportedOperation {
            message: "Rule-level RoleArn is not supported by this Cloudish EventBridge subset.".to_owned(),
        });
    }

    let event_pattern =
        input.event_pattern.as_deref().map(parse_event_pattern).transpose()?;
    let schedule_expression = input
        .schedule_expression
        .as_deref()
        .map(parse_schedule_expression)
        .transpose()?;
    if schedule_expression.is_some()
        && event_bus_name != DEFAULT_EVENT_BUS_NAME
    {
        return Err(EventBridgeError::Validation {
            message:
                "Scheduled rules are supported only on the default event bus."
                    .to_owned(),
        });
    }

    Ok(ValidatedPutRule {
        description: input.description.clone(),
        event_pattern,
        event_pattern_source: input.event_pattern.clone(),
        role_arn: input.role_arn.clone(),
        schedule_expression,
        schedule_expression_source: input.schedule_expression.clone(),
        state: input.state.unwrap_or(EventBridgeRuleState::Enabled),
    })
}

pub(crate) fn normalize_rule_limit(
    limit: Option<u32>,
) -> Result<usize, EventBridgeError> {
    let limit = limit.unwrap_or(DEFAULT_RULE_PAGE_SIZE as u32);
    if limit == 0 {
        return Err(EventBridgeError::Validation {
            message: "Limit must be greater than zero.".to_owned(),
        });
    }

    usize::try_from(limit).map_err(|_| EventBridgeError::Validation {
        message: "Limit could not be represented.".to_owned(),
    })
}

pub(crate) fn describe_rule_output(
    key: &EventBridgeRuleKey,
    rule: &StoredRule,
) -> DescribeRuleOutput {
    DescribeRuleOutput {
        arn: key.rule_arn(),
        description: rule.description.clone(),
        event_bus_name: key.bus_name.clone(),
        event_pattern: rule.event_pattern_source.clone(),
        name: key.name.clone(),
        role_arn: rule.role_arn.clone(),
        schedule_expression: rule.schedule_expression_source.clone(),
        state: rule.state,
    }
}

pub(crate) fn rule_summary(
    key: &EventBridgeRuleKey,
    rule: &StoredRule,
) -> RuleSummary {
    RuleSummary {
        arn: key.rule_arn(),
        description: rule.description.clone(),
        event_bus_name: key.bus_name.clone(),
        event_pattern: rule.event_pattern_source.clone(),
        name: key.name.clone(),
        role_arn: rule.role_arn.clone(),
        schedule_expression: rule.schedule_expression_source.clone(),
        state: rule.state,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ValidatedPutRule {
    pub(crate) description: Option<String>,
    pub(crate) event_pattern: Option<EventPattern>,
    pub(crate) event_pattern_source: Option<String>,
    pub(crate) role_arn: Option<Arn>,
    pub(crate) schedule_expression: Option<ParsedScheduleExpression>,
    pub(crate) schedule_expression_source: Option<String>,
    pub(crate) state: EventBridgeRuleState,
}

#[cfg(test)]
mod tests {
    use super::{
        EventBridgeRuleState, PutRuleInput, normalize_rule_bus_name,
        validate_put_rule,
    };

    #[test]
    fn put_rule_requires_a_pattern_or_schedule_and_keeps_default_state() {
        assert!(
            validate_put_rule(
                &PutRuleInput {
                    description: None,
                    event_bus_name: None,
                    event_pattern: None,
                    name: "demo".to_owned(),
                    role_arn: None,
                    schedule_expression: None,
                    state: None,
                },
                "default",
            )
            .is_err()
        );

        let validated = validate_put_rule(
            &PutRuleInput {
                description: Some("demo".to_owned()),
                event_bus_name: None,
                event_pattern: Some("{\"source\":[\"orders\"]}".to_owned()),
                name: "demo".to_owned(),
                role_arn: None,
                schedule_expression: None,
                state: None,
            },
            &normalize_rule_bus_name(None)
                .expect("default bus should normalize"),
        )
        .expect("pattern rule should validate");

        assert_eq!(validated.state, EventBridgeRuleState::Enabled);
    }

    #[test]
    fn scheduled_rules_stay_on_default_bus() {
        let error = validate_put_rule(
            &PutRuleInput {
                description: None,
                event_bus_name: Some("custom".to_owned()),
                event_pattern: None,
                name: "nightly".to_owned(),
                role_arn: None,
                schedule_expression: Some("rate(1 hour)".to_owned()),
                state: Some(EventBridgeRuleState::Disabled),
            },
            "custom",
        )
        .expect_err("scheduled custom bus rule should fail");

        assert!(error.to_string().contains("default event bus"));
    }
}
