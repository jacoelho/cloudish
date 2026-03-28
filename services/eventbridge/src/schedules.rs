use crate::errors::EventBridgeError;
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ParsedScheduleExpression {
    interval_seconds: u64,
    original: String,
}

impl ParsedScheduleExpression {
    pub(crate) fn interval(&self) -> Duration {
        Duration::from_secs(self.interval_seconds)
    }
}

pub(crate) fn parse_schedule_expression(
    expression: &str,
) -> Result<ParsedScheduleExpression, EventBridgeError> {
    let trimmed = expression.trim();
    let Some(body) =
        trimmed.strip_prefix("rate(").and_then(|rest| rest.strip_suffix(')'))
    else {
        return Err(EventBridgeError::UnsupportedOperation {
            message: "Only rate(...) schedule expressions are supported by this Cloudish EventBridge subset.".to_owned(),
        });
    };

    let mut parts = body.split_whitespace();
    let Some(value_text) = parts.next() else {
        return Err(EventBridgeError::Validation {
            message: format!("ScheduleExpression {expression} is invalid."),
        });
    };
    let Some(unit) = parts.next() else {
        return Err(EventBridgeError::Validation {
            message: format!("ScheduleExpression {expression} is invalid."),
        });
    };
    if parts.next().is_some() {
        return Err(EventBridgeError::Validation {
            message: format!("ScheduleExpression {expression} is invalid."),
        });
    }

    let value = value_text.parse::<u64>().map_err(|_| {
        EventBridgeError::Validation {
            message: format!("ScheduleExpression {expression} is invalid."),
        }
    })?;
    if value == 0 {
        return Err(EventBridgeError::Validation {
            message: format!("ScheduleExpression {expression} is invalid."),
        });
    }

    let multiplier = match (value, unit) {
        (1, "minute") => 60,
        (_, "minutes") if value > 1 => 60,
        (1, "hour") => 60 * 60,
        (_, "hours") if value > 1 => 60 * 60,
        (1, "day") => 60 * 60 * 24,
        (_, "days") if value > 1 => 60 * 60 * 24,
        _ => {
            return Err(EventBridgeError::Validation {
                message: format!(
                    "ScheduleExpression {expression} is invalid."
                ),
            });
        }
    };

    Ok(ParsedScheduleExpression {
        interval_seconds: multiplier * value,
        original: expression.to_owned(),
    })
}

#[cfg(test)]
mod tests {
    use super::parse_schedule_expression;
    use std::time::Duration;

    #[test]
    fn rate_schedule_supports_minutes_hours_and_days() {
        assert_eq!(
            parse_schedule_expression("rate(1 minute)")
                .expect("minutes should parse")
                .interval(),
            Duration::from_secs(60)
        );
        assert_eq!(
            parse_schedule_expression("rate(2 hours)")
                .expect("hours should parse")
                .interval(),
            Duration::from_secs(7_200)
        );
        assert_eq!(
            parse_schedule_expression("rate(3 days)")
                .expect("days should parse")
                .interval(),
            Duration::from_secs(259_200)
        );
    }

    #[test]
    fn schedule_parser_rejects_invalid_and_unsupported_forms() {
        assert!(parse_schedule_expression("cron(0 * * * ? *)").is_err());
        assert!(parse_schedule_expression("rate(0 minutes)").is_err());
        assert!(parse_schedule_expression("rate(1 minutes)").is_err());
    }
}
