use crate::{AccountId, Arn, RegionId, ServiceName};
use std::fmt;
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ProtocolFamily {
    Query,
    AwsJson10,
    AwsJson11,
    RestJson,
    RestXml,
    SmithyRpcV2Cbor,
}

impl ProtocolFamily {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Query => "query",
            Self::AwsJson10 => "aws-json-1.0",
            Self::AwsJson11 => "aws-json-1.1",
            Self::RestJson => "rest-json",
            Self::RestXml => "rest-xml",
            Self::SmithyRpcV2Cbor => "smithy-rpc-v2-cbor",
        }
    }
}

impl fmt::Display for ProtocolFamily {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CallerIdentity {
    arn: Arn,
    principal_id: String,
}

impl CallerIdentity {
    /// Builds a caller identity from a typed ARN and principal id.
    ///
    /// # Errors
    ///
    /// Returns `CallerIdentityError` when the principal id is blank or
    /// contains control characters.
    pub fn try_new(
        arn: Arn,
        principal_id: impl Into<String>,
    ) -> Result<Self, CallerIdentityError> {
        let principal_id = principal_id.into();

        if principal_id.trim().is_empty() {
            return Err(CallerIdentityError::BlankPrincipalId);
        }

        if let Some((index, character)) = principal_id
            .char_indices()
            .find(|(_, character)| character.is_control())
        {
            return Err(CallerIdentityError::InvalidPrincipalIdCharacter {
                index,
                character,
            });
        }

        Ok(Self { arn, principal_id })
    }

    pub fn arn(&self) -> &Arn {
        &self.arn
    }

    pub fn principal_id(&self) -> &str {
        &self.principal_id
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestContext {
    account_id: AccountId,
    region: RegionId,
    service: ServiceName,
    protocol: ProtocolFamily,
    operation: String,
    caller_identity: Option<CallerIdentity>,
    is_signed: bool,
}

impl RequestContext {
    /// Builds a normalized request context for signed or unsigned traffic.
    ///
    /// # Errors
    ///
    /// Returns `RequestContextError` when the operation name is blank or
    /// contains control characters.
    pub fn try_new(
        account_id: AccountId,
        region: RegionId,
        service: ServiceName,
        protocol: ProtocolFamily,
        operation: impl Into<String>,
        caller_identity: Option<CallerIdentity>,
        is_signed: bool,
    ) -> Result<Self, RequestContextError> {
        let operation = operation.into();

        if operation.trim().is_empty() {
            return Err(RequestContextError::BlankOperation);
        }

        if let Some((index, character)) = operation
            .char_indices()
            .find(|(_, character)| character.is_control())
        {
            return Err(RequestContextError::InvalidOperationCharacter {
                index,
                character,
            });
        }

        Ok(Self {
            account_id,
            region,
            service,
            protocol,
            operation,
            caller_identity,
            is_signed,
        })
    }

    /// Builds a request context from already-validated values.
    pub fn trusted_new(
        account_id: AccountId,
        region: RegionId,
        service: ServiceName,
        protocol: ProtocolFamily,
        operation: impl Into<String>,
        caller_identity: Option<CallerIdentity>,
        is_signed: bool,
    ) -> Self {
        Self {
            account_id,
            region,
            service,
            protocol,
            operation: operation.into(),
            caller_identity,
            is_signed,
        }
    }

    pub fn account_id(&self) -> &AccountId {
        &self.account_id
    }

    pub fn region(&self) -> &RegionId {
        &self.region
    }

    pub fn service(&self) -> ServiceName {
        self.service
    }

    pub fn protocol(&self) -> ProtocolFamily {
        self.protocol
    }

    pub fn operation(&self) -> &str {
        &self.operation
    }

    pub fn caller_identity(&self) -> Option<&CallerIdentity> {
        self.caller_identity.as_ref()
    }

    pub fn is_signed(&self) -> bool {
        self.is_signed
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum CallerIdentityError {
    #[error("caller principal id cannot be blank")]
    BlankPrincipalId,
    #[error(
        "caller principal id cannot contain control characters, found {character:?} at index {index}"
    )]
    InvalidPrincipalIdCharacter { index: usize, character: char },
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum RequestContextError {
    #[error("request operation cannot be blank")]
    BlankOperation,
    #[error(
        "request operation cannot contain control characters, found {character:?} at index {index}"
    )]
    InvalidOperationCharacter { index: usize, character: char },
}

#[cfg(test)]
mod tests {
    use super::{
        CallerIdentity, CallerIdentityError, ProtocolFamily, RequestContext,
        RequestContextError,
    };
    use crate::{AccountId, Arn, RegionId, ServiceName};

    fn sample_identity() -> CallerIdentity {
        CallerIdentity::try_new(
            "arn:aws:sts:eu-west-2:123456789012:assumed-role/demo/session"
                .parse::<Arn>()
                .expect("sample ARN should parse"),
            "ARO123EXAMPLE:session",
        )
        .expect("sample identity should be valid")
    }

    #[test]
    fn protocol_family_strings_are_canonical() {
        let cases = [
            (ProtocolFamily::Query, "query"),
            (ProtocolFamily::AwsJson10, "aws-json-1.0"),
            (ProtocolFamily::AwsJson11, "aws-json-1.1"),
            (ProtocolFamily::RestJson, "rest-json"),
            (ProtocolFamily::RestXml, "rest-xml"),
            (ProtocolFamily::SmithyRpcV2Cbor, "smithy-rpc-v2-cbor"),
        ];

        for (family, value) in cases {
            assert_eq!(family.as_str(), value);
            assert_eq!(family.to_string(), value);
        }
    }

    #[test]
    fn build_request_context() {
        let context = RequestContext::try_new(
            "123456789012"
                .parse::<AccountId>()
                .expect("sample account should parse"),
            "eu-west-2"
                .parse::<RegionId>()
                .expect("sample region should parse"),
            ServiceName::Sqs,
            ProtocolFamily::AwsJson10,
            "SendMessage",
            Some(sample_identity()),
            true,
        )
        .expect("request context should build");

        assert_eq!(context.account_id().as_str(), "123456789012");
        assert_eq!(context.region().as_str(), "eu-west-2");
        assert_eq!(context.service(), ServiceName::Sqs);
        assert_eq!(context.protocol(), ProtocolFamily::AwsJson10);
        assert_eq!(context.operation(), "SendMessage");
        assert!(context.is_signed());
        assert_eq!(
            context
                .caller_identity()
                .expect("caller identity should exist")
                .principal_id(),
            "ARO123EXAMPLE:session"
        );
        assert_eq!(
            context
                .caller_identity()
                .expect("caller identity should exist")
                .arn()
                .to_string(),
            "arn:aws:sts:eu-west-2:123456789012:assumed-role/demo/session"
        );
    }

    #[test]
    fn reject_blank_caller_identity_principal() {
        let error = CallerIdentity::try_new(
            "arn:aws:iam::123456789012:user/demo"
                .parse::<Arn>()
                .expect("sample ARN should parse"),
            " ",
        )
        .expect_err("blank principal should fail");

        assert_eq!(error, CallerIdentityError::BlankPrincipalId);
        assert_eq!(error.to_string(), "caller principal id cannot be blank");
    }

    #[test]
    fn reject_caller_identity_with_control_characters() {
        let error = CallerIdentity::try_new(
            "arn:aws:iam::123456789012:user/demo"
                .parse::<Arn>()
                .expect("sample ARN should parse"),
            "ARO123\n",
        )
        .expect_err("principal with control characters must fail");

        assert_eq!(
            error,
            CallerIdentityError::InvalidPrincipalIdCharacter {
                index: 6,
                character: '\n',
            }
        );
        assert_eq!(
            error.to_string(),
            "caller principal id cannot contain control characters, found '\\n' at index 6"
        );
    }

    #[test]
    fn reject_blank_request_operation() {
        let error = RequestContext::try_new(
            "123456789012"
                .parse::<AccountId>()
                .expect("sample account should parse"),
            "eu-west-2"
                .parse::<RegionId>()
                .expect("sample region should parse"),
            ServiceName::S3,
            ProtocolFamily::RestXml,
            "",
            None,
            false,
        )
        .expect_err("blank operation must fail");

        assert_eq!(error, RequestContextError::BlankOperation);
        assert_eq!(error.to_string(), "request operation cannot be blank");
    }

    #[test]
    fn reject_request_operation_with_control_characters() {
        let error = RequestContext::try_new(
            "123456789012"
                .parse::<AccountId>()
                .expect("sample account should parse"),
            "eu-west-2"
                .parse::<RegionId>()
                .expect("sample region should parse"),
            ServiceName::S3,
            ProtocolFamily::RestXml,
            "GetObject\t",
            None,
            false,
        )
        .expect_err("operation with control characters must fail");

        assert_eq!(
            error,
            RequestContextError::InvalidOperationCharacter {
                index: 9,
                character: '\t',
            }
        );
        assert_eq!(
            error.to_string(),
            "request operation cannot contain control characters, found '\\t' at index 9"
        );
    }
}
