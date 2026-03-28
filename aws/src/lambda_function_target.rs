use crate::{AccountId, Arn, ArnResource, RegionId, ServiceName};
use std::fmt;
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum LambdaFunctionIdentifier {
    Arn(Arn),
    AccountScopedName { account_id: AccountId, function_name: String },
    Name(String),
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LambdaFunctionTarget {
    function_name: String,
    identifier: LambdaFunctionIdentifier,
    qualifier: Option<String>,
}

impl LambdaFunctionTarget {
    /// Parses a Lambda function target from a name, account-scoped name, or
    /// function ARN.
    ///
    /// # Errors
    ///
    /// Returns `LambdaFunctionTargetError` when the target cannot be parsed or
    /// contains an invalid qualifier shape.
    pub fn parse(
        value: impl Into<String>,
    ) -> Result<Self, LambdaFunctionTargetError> {
        let value = value.into();
        if value.starts_with("arn:") {
            return parse_lambda_arn_target(&value);
        }

        if let Some((account_id, resource)) = value.split_once(":function:")
            && let Ok(account_id) = account_id.parse::<AccountId>()
        {
            let (function_name, qualifier) =
                split_name_and_qualifier(resource)?;
            return Ok(Self {
                function_name: function_name.clone(),
                identifier: LambdaFunctionIdentifier::AccountScopedName {
                    account_id,
                    function_name,
                },
                qualifier,
            });
        }

        let (function_name, qualifier) = split_name_and_qualifier(&value)?;
        Ok(Self {
            function_name: function_name.clone(),
            identifier: LambdaFunctionIdentifier::Name(function_name),
            qualifier,
        })
    }

    /// Attaches a qualifier to the parsed Lambda target.
    ///
    /// # Errors
    ///
    /// Returns `LambdaFunctionTargetError` when the qualifier is blank or
    /// conflicts with an embedded qualifier already present on the target.
    pub fn with_qualifier(
        mut self,
        qualifier: impl Into<String>,
    ) -> Result<Self, LambdaFunctionTargetError> {
        let qualifier = qualifier.into();
        if qualifier.trim().is_empty() {
            return Err(LambdaFunctionTargetError::MissingQualifier);
        }
        if self
            .qualifier
            .as_deref()
            .is_some_and(|embedded| embedded != qualifier)
        {
            return Err(LambdaFunctionTargetError::QualifierConflict);
        }
        self.qualifier = Some(qualifier);
        Ok(self)
    }

    pub fn account_id(&self) -> Option<&AccountId> {
        match &self.identifier {
            LambdaFunctionIdentifier::Arn(arn) => arn.account_id(),
            LambdaFunctionIdentifier::AccountScopedName {
                account_id, ..
            } => Some(account_id),
            LambdaFunctionIdentifier::Name(_) => None,
        }
    }

    pub fn function_name(&self) -> &str {
        &self.function_name
    }

    pub fn qualifier(&self) -> Option<&str> {
        self.qualifier.as_deref()
    }

    pub fn region(&self) -> Option<&RegionId> {
        match &self.identifier {
            LambdaFunctionIdentifier::Arn(arn) => arn.region(),
            LambdaFunctionIdentifier::AccountScopedName { .. }
            | LambdaFunctionIdentifier::Name(_) => None,
        }
    }
}

impl fmt::Display for LambdaFunctionTarget {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let append_qualifier = match &self.identifier {
            LambdaFunctionIdentifier::Arn(arn) => arn.fmt(formatter),
            LambdaFunctionIdentifier::AccountScopedName {
                account_id,
                function_name,
            } => write!(formatter, "{account_id}:function:{function_name}"),
            LambdaFunctionIdentifier::Name(function_name) => {
                formatter.write_str(function_name)
            }
        }
        .map(|_| {
            !matches!(self.identifier, LambdaFunctionIdentifier::Arn(_))
        })?;
        if append_qualifier && let Some(qualifier) = &self.qualifier {
            write!(formatter, ":{qualifier}")?;
        }

        Ok(())
    }
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum LambdaFunctionTargetError {
    #[error("FunctionName is required.")]
    MissingFunctionName,
    #[error("Qualifier is required.")]
    MissingQualifier,
    #[error(
        "The derived qualifier from the function name does not match the \
         specified qualifier."
    )]
    QualifierConflict,
    #[error(transparent)]
    InvalidArn(#[from] crate::ArnError),
    #[error("Lambda function identifiers must target the lambda service.")]
    InvalidService,
    #[error("Lambda function ARNs must use a generic function resource.")]
    InvalidResourceShape,
    #[error("Lambda function ARNs must use the function resource type.")]
    InvalidResourceType,
}

fn parse_lambda_arn_target(
    value: &str,
) -> Result<LambdaFunctionTarget, LambdaFunctionTargetError> {
    let arn = value.parse::<Arn>()?;
    if arn.service() != ServiceName::Lambda {
        return Err(LambdaFunctionTargetError::InvalidService);
    }
    let ArnResource::Generic(resource) = arn.resource() else {
        return Err(LambdaFunctionTargetError::InvalidResourceShape);
    };
    let Some(resource) = resource.strip_prefix("function:") else {
        return Err(LambdaFunctionTargetError::InvalidResourceType);
    };
    let (function_name, qualifier) = split_name_and_qualifier(resource)?;

    Ok(LambdaFunctionTarget {
        function_name,
        identifier: LambdaFunctionIdentifier::Arn(arn),
        qualifier,
    })
}

fn split_name_and_qualifier(
    identifier: &str,
) -> Result<(String, Option<String>), LambdaFunctionTargetError> {
    let (function_name, qualifier) = identifier
        .rsplit_once(':')
        .filter(|(function_name, _)| !function_name.contains("function:"))
        .map(|(function_name, qualifier)| {
            (function_name.to_owned(), Some(qualifier.to_owned()))
        })
        .unwrap_or_else(|| (identifier.to_owned(), None));

    if function_name.trim().is_empty() {
        return Err(LambdaFunctionTargetError::MissingFunctionName);
    }

    Ok((function_name, qualifier))
}

#[cfg(test)]
mod tests {
    use super::{LambdaFunctionTarget, LambdaFunctionTargetError};

    #[test]
    fn lambda_function_target_parses_name_and_qualifier() {
        let target =
            LambdaFunctionTarget::parse("demo:live").expect("target parses");

        assert_eq!(target.function_name(), "demo");
        assert_eq!(target.qualifier(), Some("live"));
        assert_eq!(target.to_string(), "demo:live");
    }

    #[test]
    fn lambda_function_target_parses_full_arn() {
        let target = LambdaFunctionTarget::parse(
            "arn:aws:lambda:eu-west-2:000000000000:function:demo:live",
        )
        .expect("ARN target parses");

        assert_eq!(target.function_name(), "demo");
        assert_eq!(target.qualifier(), Some("live"));
        assert_eq!(
            target.region().map(|region| region.as_str()),
            Some("eu-west-2")
        );
        assert_eq!(
            target.account_id().map(|account_id| account_id.as_str()),
            Some("000000000000")
        );
    }

    #[test]
    fn lambda_function_target_parses_account_scoped_identifier() {
        let target =
            LambdaFunctionTarget::parse("000000000000:function:demo:live")
                .expect("account-scoped target parses");

        assert_eq!(target.function_name(), "demo");
        assert_eq!(target.qualifier(), Some("live"));
        assert_eq!(
            target.account_id().map(|account_id| account_id.as_str()),
            Some("000000000000")
        );
        assert_eq!(target.region(), None);
        assert_eq!(target.to_string(), "000000000000:function:demo:live");
    }

    #[test]
    fn lambda_function_target_rejects_mismatched_explicit_qualifier() {
        let error = LambdaFunctionTarget::parse("demo:live")
            .expect("target parses")
            .with_qualifier("green")
            .expect_err("mismatched qualifier should fail");

        assert_eq!(error, LambdaFunctionTargetError::QualifierConflict);
    }

    #[test]
    fn lambda_function_target_rejects_non_lambda_arns() {
        let error = LambdaFunctionTarget::parse(
            "arn:aws:sqs:eu-west-2:000000000000:queue",
        )
        .expect_err("non-lambda ARN should fail");

        assert_eq!(error, LambdaFunctionTargetError::InvalidService);
    }
}
