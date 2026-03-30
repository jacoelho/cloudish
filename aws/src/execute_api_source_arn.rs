use crate::{AccountId, Arn, RegionId, ServiceName};
use std::fmt;
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ExecuteApiSourceArn {
    arn: Arn,
    value: String,
}

impl ExecuteApiSourceArn {
    /// Parses and validates an execute-api source ARN.
    ///
    /// # Errors
    ///
    /// Returns `ExecuteApiSourceArnError` when the ARN is malformed, targets
    /// the wrong service, or is missing region/account scope.
    pub fn parse(
        value: impl Into<String>,
    ) -> Result<Self, ExecuteApiSourceArnError> {
        let value = value.into();
        let arn = value.parse::<Arn>()?;
        validate_execute_api_arn(&arn)?;

        Ok(Self { arn, value })
    }

    /// Constructs an execute-api source ARN from a typed scope and resource.
    ///
    /// # Errors
    ///
    /// Returns `ExecuteApiSourceArnError` when the formatted ARN fails the
    /// same validation as [`ExecuteApiSourceArn::parse`].
    pub fn from_resource(
        region: &RegionId,
        account_id: &AccountId,
        resource: impl Into<String>,
    ) -> Result<Self, ExecuteApiSourceArnError> {
        Self::parse(format!(
            "arn:aws:execute-api:{}:{}:{}",
            region.as_str(),
            account_id.as_str(),
            resource.into(),
        ))
    }

    pub fn arn(&self) -> &Arn {
        &self.arn
    }

    pub fn as_str(&self) -> &str {
        &self.value
    }
}

impl fmt::Display for ExecuteApiSourceArn {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.value)
    }
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ExecuteApiSourceArnError {
    #[error(transparent)]
    InvalidArn(#[from] crate::ArnError),
    #[error("SourceArn is invalid: missing execute-api region or account.")]
    MissingScope,
    #[error("SourceArn is invalid: must target execute-api.")]
    InvalidService,
}

fn validate_execute_api_arn(
    arn: &Arn,
) -> Result<(), ExecuteApiSourceArnError> {
    if arn.service() != ServiceName::ApiGateway {
        return Err(ExecuteApiSourceArnError::InvalidService);
    }
    if arn.region().is_none() || arn.account_id().is_none() {
        return Err(ExecuteApiSourceArnError::MissingScope);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{ExecuteApiSourceArn, ExecuteApiSourceArnError};
    use crate::{AccountId, ArnError, RegionId, ServiceName};

    #[test]
    fn execute_api_source_arn_preserves_execute_api_display() {
        let arn = ExecuteApiSourceArn::parse(
            "arn:aws:execute-api:eu-west-2:000000000000:api123/dev/GET/pets",
        )
        .expect("source ARN parses");

        assert_eq!(
            arn.arn().service().as_str(),
            "apigateway",
            "parsed service stays on the shared ServiceName enum"
        );
        assert_eq!(
            arn.as_str(),
            "arn:aws:execute-api:eu-west-2:000000000000:api123/dev/GET/pets"
        );
        assert_eq!(arn.to_string(), arn.as_str());
    }

    #[test]
    fn execute_api_source_arn_builds_from_typed_scope() {
        let arn = ExecuteApiSourceArn::from_resource(
            &"eu-west-2".parse::<RegionId>().expect("region should parse"),
            &"000000000000"
                .parse::<AccountId>()
                .expect("account should parse"),
            "api123/dev/GET/pets",
        )
        .expect("typed scope should build an execute-api source ARN");

        assert_eq!(
            arn.as_str(),
            "arn:aws:execute-api:eu-west-2:000000000000:api123/dev/GET/pets"
        );
        assert_eq!(arn.arn().service(), ServiceName::ApiGateway);
    }

    #[test]
    fn execute_api_source_arn_rejects_wrong_service_missing_scope_and_invalid_arns()
     {
        let wrong_service = ExecuteApiSourceArn::parse(
            "arn:aws:lambda:eu-west-2:000000000000:function:demo",
        )
        .expect_err("non execute-api ARNs should fail");
        let missing_scope = ExecuteApiSourceArn::parse(
            "arn:aws:execute-api::000000000000:api123/dev/GET/pets",
        )
        .expect_err("execute-api ARNs without region should fail");
        let invalid = ExecuteApiSourceArn::parse("not-an-arn")
            .expect_err("invalid ARN strings should fail");

        assert_eq!(wrong_service, ExecuteApiSourceArnError::InvalidService);
        assert_eq!(missing_scope, ExecuteApiSourceArnError::MissingScope);
        assert_eq!(
            invalid,
            ExecuteApiSourceArnError::InvalidArn(
                ArnError::InvalidSectionCount { actual: 1 }
            )
        );
    }
}
