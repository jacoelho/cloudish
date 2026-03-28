use std::fmt;
use thiserror::Error;

/// Cross-service AWS error categories. Services can use the family defaults or
/// override the wire code while keeping shared classification and metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AwsErrorFamily {
    Validation,
    MissingParameter,
    NotFound,
    AlreadyExists,
    UnsupportedOperation,
    AccessDenied,
    Conflict,
    Throttling,
    Internal,
}

impl AwsErrorFamily {
    pub const fn default_code(self) -> &'static str {
        match self {
            Self::Validation => "ValidationException",
            Self::MissingParameter => "MissingParameter",
            Self::NotFound => "ResourceNotFoundException",
            Self::AlreadyExists => "ResourceAlreadyExistsException",
            Self::UnsupportedOperation => "UnsupportedOperationException",
            Self::AccessDenied => "AccessDeniedException",
            Self::Conflict => "ConflictException",
            Self::Throttling => "ThrottlingException",
            Self::Internal => "InternalFailure",
        }
    }

    pub const fn default_status_code(self) -> u16 {
        match self {
            Self::Validation
            | Self::MissingParameter
            | Self::UnsupportedOperation
            | Self::Conflict => 400,
            Self::AccessDenied => 403,
            Self::NotFound => 404,
            Self::AlreadyExists => 409,
            Self::Throttling => 429,
            Self::Internal => 500,
        }
    }

    pub const fn sender_fault(self) -> bool {
        !matches!(self, Self::Throttling | Self::Internal)
    }
}

impl fmt::Display for AwsErrorFamily {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.default_code())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AwsError {
    family: AwsErrorFamily,
    code: String,
    message: String,
    status_code: u16,
    sender_fault: bool,
}

impl AwsError {
    /// Builds an AWS error using the defaults for the selected family.
    ///
    /// # Errors
    ///
    /// Returns `AwsErrorBuildError` when the derived code or message would be
    /// blank, or when the derived status code would be invalid.
    pub fn from_family(
        family: AwsErrorFamily,
        message: impl Into<String>,
    ) -> Result<Self, AwsErrorBuildError> {
        Self::custom(
            family,
            family.default_code(),
            message,
            family.default_status_code(),
            family.sender_fault(),
        )
    }

    /// Builds an AWS error with an explicit wire shape.
    ///
    /// # Errors
    ///
    /// Returns `AwsErrorBuildError` when the code or message are blank, or
    /// when the status code is outside the valid HTTP range.
    pub fn custom(
        family: AwsErrorFamily,
        code: impl Into<String>,
        message: impl Into<String>,
        status_code: u16,
        sender_fault: bool,
    ) -> Result<Self, AwsErrorBuildError> {
        let code = code.into();
        let message = message.into();

        if code.trim().is_empty() {
            return Err(AwsErrorBuildError::BlankCode);
        }

        if message.trim().is_empty() {
            return Err(AwsErrorBuildError::BlankMessage);
        }

        if !(100..=599).contains(&status_code) {
            return Err(AwsErrorBuildError::InvalidStatusCode { status_code });
        }

        Ok(Self { family, code, message, status_code, sender_fault })
    }

    /// Builds an AWS error from already-validated wire metadata.
    ///
    /// This constructor skips validation and is intended for internal call
    /// sites that already constrain the code, status code, and sender-fault
    /// values.
    pub fn trusted_custom(
        family: AwsErrorFamily,
        code: impl Into<String>,
        message: impl Into<String>,
        status_code: u16,
        sender_fault: bool,
    ) -> Self {
        Self {
            family,
            code: code.into(),
            message: message.into(),
            status_code,
            sender_fault,
        }
    }

    pub fn family(&self) -> AwsErrorFamily {
        self.family
    }

    pub fn code(&self) -> &str {
        &self.code
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub fn status_code(&self) -> u16 {
        self.status_code
    }

    pub fn sender_fault(&self) -> bool {
        self.sender_fault
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum AwsErrorBuildError {
    #[error("AWS error code cannot be blank")]
    BlankCode,
    #[error("AWS error message cannot be blank")]
    BlankMessage,
    #[error(
        "AWS error status code must be between 100 and 599, got {status_code}"
    )]
    InvalidStatusCode { status_code: u16 },
}

#[cfg(test)]
mod tests {
    use super::{AwsError, AwsErrorBuildError, AwsErrorFamily};

    #[test]
    fn shared_error_family_defaults_cover_all_variants() {
        let cases = [
            (AwsErrorFamily::Validation, "ValidationException", 400, true),
            (AwsErrorFamily::MissingParameter, "MissingParameter", 400, true),
            (AwsErrorFamily::NotFound, "ResourceNotFoundException", 404, true),
            (
                AwsErrorFamily::AlreadyExists,
                "ResourceAlreadyExistsException",
                409,
                true,
            ),
            (
                AwsErrorFamily::UnsupportedOperation,
                "UnsupportedOperationException",
                400,
                true,
            ),
            (AwsErrorFamily::AccessDenied, "AccessDeniedException", 403, true),
            (AwsErrorFamily::Conflict, "ConflictException", 400, true),
            (AwsErrorFamily::Throttling, "ThrottlingException", 429, false),
            (AwsErrorFamily::Internal, "InternalFailure", 500, false),
        ];

        for (family, code, status_code, sender_fault) in cases {
            assert_eq!(family.default_code(), code);
            assert_eq!(family.default_status_code(), status_code);
            assert_eq!(family.sender_fault(), sender_fault);
            assert_eq!(family.to_string(), code);
        }
    }

    #[test]
    fn build_aws_error_from_shared_family_defaults() {
        let error = AwsError::from_family(
            AwsErrorFamily::Validation,
            "queue name is invalid",
        )
        .expect("shared family should build an error");

        assert_eq!(error.family(), AwsErrorFamily::Validation);
        assert_eq!(error.code(), "ValidationException");
        assert_eq!(error.message(), "queue name is invalid");
        assert_eq!(error.status_code(), 400);
        assert!(error.sender_fault());
    }

    #[test]
    fn build_custom_aws_error_shape() {
        let error = AwsError::custom(
            AwsErrorFamily::UnsupportedOperation,
            "UnsupportedOperation",
            "The operation is not supported.",
            400,
            true,
        )
        .expect("custom error should build");

        assert_eq!(error.family(), AwsErrorFamily::UnsupportedOperation);
        assert_eq!(error.code(), "UnsupportedOperation");
        assert_eq!(error.message(), "The operation is not supported.");
        assert_eq!(error.status_code(), 400);
        assert!(error.sender_fault());
    }

    #[test]
    fn reject_blank_aws_error_fields() {
        let code_error = AwsError::custom(
            AwsErrorFamily::Internal,
            " ",
            "boom",
            500,
            false,
        )
        .expect_err("blank code must fail");
        let message_error = AwsError::custom(
            AwsErrorFamily::Internal,
            "InternalFailure",
            "",
            500,
            false,
        )
        .expect_err("blank message must fail");

        assert_eq!(code_error, AwsErrorBuildError::BlankCode);
        assert_eq!(message_error, AwsErrorBuildError::BlankMessage);
    }

    #[test]
    fn reject_invalid_status_codes() {
        let error = AwsError::custom(
            AwsErrorFamily::Internal,
            "InternalFailure",
            "boom",
            99,
            false,
        )
        .expect_err("invalid status code must fail");

        assert_eq!(
            error,
            AwsErrorBuildError::InvalidStatusCode { status_code: 99 }
        );
        assert_eq!(
            error.to_string(),
            "AWS error status code must be between 100 and 599, got 99"
        );
    }
}
