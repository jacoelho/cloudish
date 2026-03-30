use crate::{StsError, caller::StsCaller};
use aws::{AccountId, Arn, AwsPrincipalType, IamResourceTag, ServiceName};
use iam::IamTag;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, Copy)]
pub(crate) struct DurationBounds {
    default: u32,
    min: u32,
    max: u32,
}

impl DurationBounds {
    const fn new(default: u32, min: u32, max: u32) -> Self {
        Self { default, min, max }
    }

    pub(crate) fn with_max(self, max: u32) -> Self {
        Self { max: self.max.min(max), ..self }
    }
}

pub(crate) const ASSUME_ROLE_DURATION: DurationBounds =
    DurationBounds::new(3_600, 900, 43_200);
pub(crate) const ROOT_SESSION_TOKEN_DURATION: DurationBounds =
    DurationBounds::new(3_600, 900, 3_600);
pub(crate) const IAM_USER_SESSION_TOKEN_DURATION: DurationBounds =
    DurationBounds::new(43_200, 900, 129_600);
pub(crate) const FEDERATION_TOKEN_DURATION: DurationBounds =
    DurationBounds::new(3_600, 900, 129_600);

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RoleReference {
    pub(crate) account_id: AccountId,
    pub(crate) role_name: String,
}

pub(crate) fn parse_role_arn(arn: &Arn) -> Result<RoleReference, StsError> {
    if arn.service() != ServiceName::Iam {
        return Err(StsError::Validation {
            message: format!("{arn} is invalid"),
        });
    }

    let account_id = arn.account_id().cloned().ok_or_else(|| {
        StsError::Validation { message: format!("{arn} is invalid") }
    })?;
    let resource = arn.resource().to_string();
    let role_name = resource
        .strip_prefix("role/")
        .and_then(|name| name.rsplit('/').next())
        .filter(|name| !name.is_empty())
        .ok_or_else(|| StsError::Validation {
            message: format!("{arn} is invalid"),
        })?;

    Ok(RoleReference { account_id, role_name: role_name.to_owned() })
}

pub(crate) fn validate_saml_principal_arn(arn: &Arn) -> Result<(), StsError> {
    let valid = arn.service() == ServiceName::Iam
        && arn.resource().to_string().starts_with("saml-provider/");
    valid.then_some(()).ok_or_else(|| StsError::Validation {
        message: format!("{arn} is invalid"),
    })
}

pub(crate) fn validate_session_name(value: &str) -> Result<(), StsError> {
    validate_name(value, "roleSessionName")
}

pub(crate) fn validate_federation_name(value: &str) -> Result<(), StsError> {
    validate_name(value, "name")
}

pub(crate) fn normalize_duration(
    requested: Option<u32>,
    bounds: DurationBounds,
) -> Result<u32, StsError> {
    let value = requested.unwrap_or(bounds.default);
    if !(bounds.min..=bounds.max).contains(&value) {
        return Err(StsError::Validation {
            message: format!(
                "1 validation error detected: Value '{value}' at 'durationSeconds' failed to satisfy constraint: Member must have value between {} and {}",
                bounds.min, bounds.max
            ),
        });
    }

    Ok(value)
}

pub(crate) fn assume_role_duration_bounds(
    caller: &StsCaller,
    role_max_session_duration: u32,
) -> DurationBounds {
    let bounds = ASSUME_ROLE_DURATION.with_max(role_max_session_duration);
    if caller.uses_long_term_credentials() {
        bounds
    } else {
        bounds.with_max(3_600)
    }
}

pub(crate) fn get_session_token_duration_bounds(
    caller: &StsCaller,
) -> DurationBounds {
    match caller.credential_kind().canonical_trust_principal().principal_type()
    {
        AwsPrincipalType::Account => ROOT_SESSION_TOKEN_DURATION,
        AwsPrincipalType::User => IAM_USER_SESSION_TOKEN_DURATION,
        AwsPrincipalType::FederatedUser | AwsPrincipalType::Role => {
            IAM_USER_SESSION_TOKEN_DURATION
        }
    }
}

pub(crate) fn validate_session_tags(
    tags: &[IamTag],
    transitive_tag_keys: &[String],
    caller: &StsCaller,
) -> Result<(), StsError> {
    let mut requested_tags = BTreeMap::new();
    for tag in tags {
        let normalized = tag.key.to_ascii_lowercase();
        if requested_tags.contains_key(&normalized) {
            return Err(StsError::InvalidParameterValue {
                message: "Duplicate tag keys found. Please note that Tag keys are case insensitive."
                    .to_owned(),
            });
        }
        requested_tags.insert(normalized, tag.value.clone());
    }

    for transitive_key in transitive_tag_keys {
        if !requested_tags.contains_key(&transitive_key.to_ascii_lowercase()) {
            return Err(StsError::InvalidParameterValue {
                message: "The specified transitive tag key must be included in the requested tags."
                    .to_owned(),
            });
        }
    }

    for tag in tags {
        if caller.transitive_tag_keys().contains(&tag.key.to_ascii_lowercase())
        {
            return Err(StsError::InvalidParameterValue {
                message: "One of the specified transitive tag keys can't be set because it conflicts with a transitive tag key from the calling session."
                    .to_owned(),
            });
        }
    }

    Ok(())
}

pub(crate) fn session_state_for_assume_role(
    caller: &StsCaller,
    tags: &[IamTag],
    transitive_tag_keys: &[String],
) -> (Vec<IamResourceTag>, BTreeSet<String>) {
    let inherited_transitive_tags = caller
        .session_tags()
        .iter()
        .filter(|tag| {
            caller
                .transitive_tag_keys()
                .contains(&tag.key.to_ascii_lowercase())
        })
        .map(|tag| IamResourceTag {
            key: tag.key.clone(),
            value: tag.value.clone(),
        });
    let requested_tags = tags.iter().map(|tag| IamResourceTag {
        key: tag.key.clone(),
        value: tag.value.clone(),
    });

    (
        inherited_transitive_tags.chain(requested_tags).collect(),
        caller
            .transitive_tag_keys()
            .iter()
            .cloned()
            .chain(
                transitive_tag_keys.iter().map(|key| key.to_ascii_lowercase()),
            )
            .collect(),
    )
}

fn validate_name(value: &str, field_name: &str) -> Result<(), StsError> {
    let valid = !value.trim().is_empty()
        && value.len() <= 64
        && value.chars().all(|character| {
            matches!(
                character,
                'A'..='Z'
                    | 'a'..='z'
                    | '0'..='9'
                    | '='
                    | ','
                    | '.'
                    | '@'
                    | '-'
                    | '_'
            )
        });
    valid.then_some(()).ok_or_else(|| StsError::Validation {
        message: format!(
            "1 validation error detected: Value '{value}' at '{field_name}' failed to satisfy constraint: Member must satisfy regular expression pattern: [\\w+=,.@-]*"
        ),
    })
}
