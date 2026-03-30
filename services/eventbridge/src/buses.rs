use crate::errors::EventBridgeError;
use crate::scope::EventBridgeScope;
use aws::{AccountId, Arn, ArnResource, Partition, RegionId, ServiceName};
use serde::{Deserialize, Serialize};

pub(crate) const DEFAULT_EVENT_BUS_NAME: &str = "default";
const DEFAULT_PAGE_SIZE: usize = 100;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct CreateEventBusInput {
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct EventBusDescription {
    pub arn: Arn,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ListEventBusesInput {
    pub limit: Option<u32>,
    pub name_prefix: Option<String>,
    pub next_token: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ListEventBusesOutput {
    pub event_buses: Vec<EventBusDescription>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_token: Option<String>,
}

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
pub(crate) struct EventBusKey {
    pub(crate) account_id: AccountId,
    pub(crate) name: String,
    pub(crate) region: RegionId,
}

impl EventBusKey {
    pub(crate) fn new(
        scope: &EventBridgeScope,
        name: &str,
    ) -> Result<Self, EventBridgeError> {
        validate_bus_name(name)?;

        Ok(Self {
            account_id: scope.account_id().clone(),
            name: name.to_owned(),
            region: scope.region().clone(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct EventBusRecord;

pub(crate) fn bus_arn(scope: &EventBridgeScope, name: &str) -> Arn {
    bus_arn_from_parts(scope.account_id(), scope.region(), name)
}

pub(crate) fn default_event_bus(
    scope: &EventBridgeScope,
) -> EventBusDescription {
    EventBusDescription {
        arn: bus_arn(scope, DEFAULT_EVENT_BUS_NAME),
        name: DEFAULT_EVENT_BUS_NAME.to_owned(),
    }
}

pub(crate) fn bus_description(
    scope: &EventBridgeScope,
    name: &str,
) -> EventBusDescription {
    EventBusDescription { arn: bus_arn(scope, name), name: name.to_owned() }
}

pub(crate) fn normalize_bus_name(
    event_bus_name: Option<&str>,
) -> Result<String, EventBridgeError> {
    match event_bus_name {
        None => Ok(DEFAULT_EVENT_BUS_NAME.to_owned()),
        Some(name) => {
            validate_bus_name(name)?;
            Ok(name.to_owned())
        }
    }
}

pub(crate) fn validate_bus_name(name: &str) -> Result<(), EventBridgeError> {
    if name.is_empty()
        || name.len() > 256
        || name.chars().any(|character| {
            !character.is_ascii_alphanumeric()
                && character != '.'
                && character != '-'
                && character != '_'
        })
    {
        return Err(EventBridgeError::Validation {
            message: "Event bus name must be 1-256 characters of letters, digits, '.', '-', or '_'.".to_owned(),
        });
    }

    Ok(())
}

pub(crate) fn validate_create_event_bus(
    input: &CreateEventBusInput,
) -> Result<(), EventBridgeError> {
    validate_bus_name(&input.name)?;

    if input.name == DEFAULT_EVENT_BUS_NAME {
        return Err(EventBridgeError::ResourceAlreadyExists {
            message: "Event bus default already exists.".to_owned(),
        });
    }

    Ok(())
}

pub(crate) fn validate_delete_event_bus_name(
    name: &str,
) -> Result<(), EventBridgeError> {
    validate_bus_name(name)?;

    if name == DEFAULT_EVENT_BUS_NAME {
        return Err(EventBridgeError::Validation {
            message: "Cannot delete the default event bus.".to_owned(),
        });
    }

    Ok(())
}

pub(crate) fn decode_next_token(
    next_token: Option<&str>,
) -> Result<usize, EventBridgeError> {
    match next_token {
        None => Ok(0),
        Some(token) => {
            token.parse::<usize>().map_err(|_| EventBridgeError::Validation {
                message: "NextToken is invalid.".to_owned(),
            })
        }
    }
}

pub(crate) fn normalize_limit(
    limit: Option<u32>,
    field_name: &str,
) -> Result<usize, EventBridgeError> {
    let limit = limit.unwrap_or(DEFAULT_PAGE_SIZE as u32);
    if limit == 0 {
        return Err(EventBridgeError::Validation {
            message: format!("{field_name} must be greater than zero."),
        });
    }

    usize::try_from(limit).map_err(|_| EventBridgeError::Validation {
        message: format!("{field_name} could not be represented."),
    })
}

fn bus_arn_from_parts(
    account_id: &AccountId,
    region: &RegionId,
    name: &str,
) -> Arn {
    Arn::trusted_new(
        Partition::aws(),
        ServiceName::EventBridge,
        Some(region.clone()),
        Some(account_id.clone()),
        ArnResource::Generic(format!("event-bus/{name}")),
    )
}

#[cfg(test)]
mod tests {
    use super::{
        CreateEventBusInput, DEFAULT_EVENT_BUS_NAME, decode_next_token,
        default_event_bus, normalize_bus_name, normalize_limit,
        validate_bus_name, validate_create_event_bus,
    };
    use crate::scope::EventBridgeScope;

    fn scope() -> EventBridgeScope {
        EventBridgeScope::new(
            "000000000000".parse().expect("account should parse"),
            "eu-west-2".parse().expect("region should parse"),
        )
    }

    #[test]
    fn default_event_bus_uses_canonical_name_and_arn() {
        let bus = default_event_bus(&scope());

        assert_eq!(bus.name, DEFAULT_EVENT_BUS_NAME);
        assert_eq!(
            bus.arn.to_string(),
            "arn:aws:events:eu-west-2:000000000000:event-bus/default"
        );
    }

    #[test]
    fn bus_validation_rejects_invalid_names() {
        assert!(validate_bus_name("").is_err());
        assert!(validate_bus_name("invalid name").is_err());
        assert!(validate_bus_name("valid-name_1").is_ok());
    }

    #[test]
    fn create_validation_reserves_default_bus_name() {
        let error = validate_create_event_bus(&CreateEventBusInput {
            name: DEFAULT_EVENT_BUS_NAME.to_owned(),
        })
        .expect_err("default bus should be reserved");

        assert!(error.to_string().contains("already exists"));
    }

    #[test]
    fn paging_helpers_validate_tokens_and_limits() {
        assert_eq!(
            normalize_bus_name(None).expect("missing bus should default"),
            DEFAULT_EVENT_BUS_NAME
        );
        assert_eq!(
            decode_next_token(Some("7")).expect("token should parse"),
            7
        );
        assert_eq!(
            normalize_limit(Some(25), "Limit").expect("limit should parse"),
            25
        );
        assert!(decode_next_token(Some("nope")).is_err());
        assert!(normalize_limit(Some(0), "Limit").is_err());
    }
}
