use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ObjectReadConditions {
    pub if_match: Option<String>,
    pub if_modified_since: Option<SystemTime>,
    pub if_none_match: Option<String>,
    pub if_unmodified_since: Option<SystemTime>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectReadPreconditionOutcome {
    NotModified,
    PreconditionFailed,
    Proceed,
}

pub fn evaluate_object_read_preconditions(
    etag: &str,
    last_modified_epoch_seconds: u64,
    conditions: &ObjectReadConditions,
) -> ObjectReadPreconditionOutcome {
    let object_time = UNIX_EPOCH
        .checked_add(std::time::Duration::from_secs(last_modified_epoch_seconds))
        .unwrap_or(UNIX_EPOCH);
    if let Some(value) = conditions.if_match.as_deref() {
        if !strong_etag_condition_matches(value, etag) {
            return ObjectReadPreconditionOutcome::PreconditionFailed;
        }
        return ObjectReadPreconditionOutcome::Proceed;
    }
    if let Some(value) = conditions.if_none_match.as_deref()
        && weak_etag_condition_matches(value, etag)
    {
        return ObjectReadPreconditionOutcome::NotModified;
    }
    if conditions.if_unmodified_since.is_some_and(|value| object_time > value)
    {
        return ObjectReadPreconditionOutcome::PreconditionFailed;
    }
    if conditions.if_modified_since.is_some_and(|value| object_time <= value) {
        return ObjectReadPreconditionOutcome::NotModified;
    }

    ObjectReadPreconditionOutcome::Proceed
}

pub(crate) fn strong_etag_condition_matches(
    condition: &str,
    etag: &str,
) -> bool {
    condition.split(',').map(str::trim).any(|candidate| {
        candidate == "*" || etags_match(candidate, etag, /*strong*/ true)
    })
}

pub(crate) fn weak_etag_condition_matches(
    condition: &str,
    etag: &str,
) -> bool {
    condition.split(',').map(str::trim).any(|candidate| {
        candidate == "*" || etags_match(candidate, etag, /*strong*/ false)
    })
}

fn etags_match(left: &str, right: &str, strong: bool) -> bool {
    let Some(left) = parse_entity_tag(left) else {
        return false;
    };
    let Some(right) = parse_entity_tag(right) else {
        return false;
    };
    if strong && (left.is_weak || right.is_weak) {
        return false;
    }

    left.opaque_tag == right.opaque_tag
}

fn parse_entity_tag(value: &str) -> Option<EntityTag<'_>> {
    let value = value.trim();
    let (is_weak, opaque_tag) =
        if let Some(opaque_tag) = value.strip_prefix("W/") {
            (true, opaque_tag)
        } else {
            (false, value)
        };
    if !(opaque_tag.starts_with('"') && opaque_tag.ends_with('"')) {
        return None;
    }

    Some(EntityTag { is_weak, opaque_tag })
}

struct EntityTag<'a> {
    is_weak: bool,
    opaque_tag: &'a str,
}

#[cfg(test)]
mod tests {
    use super::{
        ObjectReadConditions, ObjectReadPreconditionOutcome,
        evaluate_object_read_preconditions, strong_etag_condition_matches,
        weak_etag_condition_matches,
    };
    use std::time::{Duration, UNIX_EPOCH};

    #[test]
    fn object_reads_preconditions_follow_documented_s3_mixed_validator_rules()
    {
        let object = fixture_object();
        let stale_time = UNIX_EPOCH
            + Duration::from_secs(object.last_modified_epoch_seconds - 1);
        let future_time = UNIX_EPOCH
            + Duration::from_secs(object.last_modified_epoch_seconds + 1);

        assert_eq!(
            evaluate_object_read_preconditions(
                &object.etag,
                object.last_modified_epoch_seconds,
                &ObjectReadConditions {
                    if_match: Some(object.etag.clone()),
                    if_modified_since: None,
                    if_none_match: None,
                    if_unmodified_since: Some(stale_time),
                },
            ),
            ObjectReadPreconditionOutcome::Proceed
        );
        assert_eq!(
            evaluate_object_read_preconditions(
                &object.etag,
                object.last_modified_epoch_seconds,
                &ObjectReadConditions {
                    if_match: None,
                    if_modified_since: Some(future_time),
                    if_none_match: Some("\"stale\"".to_owned()),
                    if_unmodified_since: None,
                },
            ),
            ObjectReadPreconditionOutcome::NotModified
        );
    }

    #[test]
    fn object_reads_etag_conditions_use_weak_and_strong_matching_rules() {
        let object = fixture_object();

        assert!(weak_etag_condition_matches("W/\"etag\"", &object.etag));
        assert!(!strong_etag_condition_matches("W/\"etag\"", &object.etag));
    }

    fn fixture_object() -> FixtureObject {
        FixtureObject {
            etag: "\"etag\"".to_owned(),
            last_modified_epoch_seconds: 1_700_000_000,
        }
    }

    struct FixtureObject {
        etag: String,
        last_modified_epoch_seconds: u64,
    }
}
