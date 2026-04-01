use crate::{SnsError, SnsScope};
use aws::Arn;
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use serde_json::{Value, json};

const PAGE_SIZE: usize = 100;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PaginatedList<T> {
    pub items: Vec<T>,
    pub next_token: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PaginationContext {
    Scope { operation: &'static str, scope: SnsScope },
    Topic { operation: &'static str, topic_arn: Arn },
}

impl PaginationContext {
    fn matches(&self, operation: &str, anchor: &Value) -> bool {
        match self {
            Self::Scope { operation: expected_operation, scope } => {
                *expected_operation == operation
                    && anchor
                        .get("account_id")
                        .and_then(Value::as_str)
                        .is_some_and(|value| {
                            value == scope.account_id().as_str()
                        })
                    && anchor
                        .get("region")
                        .and_then(Value::as_str)
                        .is_some_and(|value| value == scope.region().as_ref())
            }
            Self::Topic { operation: expected_operation, topic_arn } => {
                *expected_operation == operation
                    && anchor
                        .get("topic_arn")
                        .and_then(Value::as_str)
                        .is_some_and(|value| value == topic_arn.to_string())
            }
        }
    }

    fn token_anchor(&self) -> Value {
        match self {
            Self::Scope { scope, .. } => json!({
                "account_id": scope.account_id().as_str(),
                "region": scope.region().as_ref(),
            }),
            Self::Topic { topic_arn, .. } => {
                json!({ "topic_arn": topic_arn.to_string() })
            }
        }
    }

    fn operation(&self) -> &'static str {
        match self {
            Self::Scope { operation, .. } | Self::Topic { operation, .. } => {
                operation
            }
        }
    }
}

pub(crate) fn paginate<T: Clone>(
    items: &[T],
    context: &PaginationContext,
    next_token: Option<&str>,
) -> Result<PaginatedList<T>, SnsError> {
    let start = decode_next_token(next_token, context)?;
    if start > items.len() {
        return Err(invalid_next_token());
    }

    let end = start.saturating_add(PAGE_SIZE).min(items.len());
    let next_token = (end < items.len())
        .then(|| encode_next_token(context, end))
        .transpose()?;

    Ok(PaginatedList { items: items[start..end].to_vec(), next_token })
}

fn encode_next_token(
    context: &PaginationContext,
    start: usize,
) -> Result<String, SnsError> {
    let payload = json!({
        "operation": context.operation(),
        "anchor": context.token_anchor(),
        "start": start,
    });
    let serialized =
        serde_json::to_vec(&payload).map_err(|_| invalid_next_token())?;

    Ok(URL_SAFE_NO_PAD.encode(serialized))
}

fn decode_next_token(
    next_token: Option<&str>,
    context: &PaginationContext,
) -> Result<usize, SnsError> {
    let Some(next_token) = next_token else {
        return Ok(0);
    };
    let decoded = URL_SAFE_NO_PAD
        .decode(next_token)
        .map_err(|_| invalid_next_token())?;
    let payload: Value =
        serde_json::from_slice(&decoded).map_err(|_| invalid_next_token())?;
    let operation = payload
        .get("operation")
        .and_then(Value::as_str)
        .ok_or_else(invalid_next_token)?;
    let anchor = payload.get("anchor").ok_or_else(invalid_next_token)?;
    let start_u64 = payload
        .get("start")
        .and_then(Value::as_u64)
        .ok_or_else(invalid_next_token)?;
    let start =
        usize::try_from(start_u64).map_err(|_| invalid_next_token())?;

    if !context.matches(operation, anchor) {
        return Err(invalid_next_token());
    }

    Ok(start)
}

pub(crate) fn invalid_next_token() -> SnsError {
    SnsError::InvalidParameter {
        message: "Invalid parameter: NextToken".to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::{PaginationContext, paginate};
    use crate::SnsScope;
    use aws::Arn;

    fn scope() -> SnsScope {
        SnsScope::new(
            "000000000000".parse().expect("account should parse"),
            "eu-west-2".parse().expect("region should parse"),
        )
    }

    #[test]
    fn sns_pagination_round_trips_scope_tokens() {
        let items = (0..205).collect::<Vec<_>>();
        let context = PaginationContext::Scope {
            operation: "ListTopics",
            scope: scope(),
        };

        let first =
            paginate(&items, &context, None).expect("first page should build");
        let second = paginate(&items, &context, first.next_token.as_deref())
            .expect("second page should build");
        let third = paginate(&items, &context, second.next_token.as_deref())
            .expect("third page should build");

        assert_eq!(first.items.len(), 100);
        assert_eq!(second.items.len(), 100);
        assert_eq!(third.items.len(), 5);
        assert!(third.next_token.is_none());
    }

    #[test]
    fn sns_pagination_rejects_tokens_for_the_wrong_context() {
        let items = (0..101).collect::<Vec<_>>();
        let first_context = PaginationContext::Scope {
            operation: "ListTopics",
            scope: scope(),
        };
        let second_context = PaginationContext::Topic {
            operation: "ListSubscriptionsByTopic",
            topic_arn: "arn:aws:sns:eu-west-2:000000000000:orders"
                .parse::<Arn>()
                .expect("topic ARN should parse"),
        };
        let first = paginate(&items, &first_context, None)
            .expect("first page should build");
        assert!(first.next_token.is_some());
        let error =
            paginate(&items, &second_context, first.next_token.as_deref())
                .expect_err("token reuse on another context should fail");

        assert_eq!(error.code(), "InvalidParameter");
        assert_eq!(error.message(), "Invalid parameter: NextToken");
    }
}
