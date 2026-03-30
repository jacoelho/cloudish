use crate::{errors::LambdaError, state::LambdaPackageType};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use sha2::{Digest, Sha256};

const SUPPORTED_RUNTIMES: &[&str] = &[
    "provided",
    "provided.al2",
    "provided.al2023",
    "python3.8",
    "python3.9",
    "python3.10",
    "python3.11",
    "python3.12",
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LambdaCodeInput {
    ImageUri { image_uri: String },
    InlineZip { archive: Vec<u8> },
    S3Object { bucket: String, key: String, object_version: Option<String> },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateFunctionCodeInput {
    pub code: LambdaCodeInput,
    pub publish: bool,
    pub revision_id: Option<String>,
}

pub(crate) fn sha256_base64(body: &[u8]) -> String {
    let digest = Sha256::digest(body);
    BASE64_STANDARD.encode(digest)
}

pub(crate) fn validate_handler_for_package(
    package_type: LambdaPackageType,
    handler: Option<String>,
) -> Result<Option<String>, LambdaError> {
    if package_type == LambdaPackageType::Image {
        return Ok(handler.filter(|handler| !handler.trim().is_empty()));
    }

    let handler = handler.unwrap_or_default();
    if handler.trim().is_empty() {
        return Err(LambdaError::InvalidParameterValue {
            message: "Handler is required for Zip package type.".to_owned(),
        });
    }
    if handler.chars().any(char::is_whitespace) {
        return Err(LambdaError::Validation {
            message: "Handler cannot contain whitespace.".to_owned(),
        });
    }

    Ok(Some(handler))
}

pub(crate) fn validate_runtime_for_package(
    package_type: LambdaPackageType,
    runtime: Option<&str>,
) -> Result<Option<String>, LambdaError> {
    if package_type == LambdaPackageType::Image {
        return Ok(runtime.map(str::to_owned));
    }

    let runtime = runtime.unwrap_or_default();
    if runtime.trim().is_empty() {
        return Err(LambdaError::InvalidParameterValue {
            message: "Runtime is required for Zip package type.".to_owned(),
        });
    }
    if !SUPPORTED_RUNTIMES.contains(&runtime) {
        return Err(LambdaError::InvalidParameterValue {
            message: format!(
                "Value {runtime} at 'runtime' failed to satisfy constraint: Member must satisfy enum value set: [{}] or be a valid ARN",
                SUPPORTED_RUNTIMES.join(", ")
            ),
        });
    }

    Ok(Some(runtime.to_owned()))
}

#[cfg(test)]
mod tests {
    use super::{
        LambdaCodeInput, UpdateFunctionCodeInput, sha256_base64,
        validate_handler_for_package, validate_runtime_for_package,
    };
    use crate::{LambdaError, LambdaPackageType};

    #[test]
    fn lambda_code_contract_round_trips_public_fields() {
        let input = LambdaCodeInput::S3Object {
            bucket: "deployments".to_owned(),
            key: "demo.zip".to_owned(),
            object_version: Some("v1".to_owned()),
        };
        let update = UpdateFunctionCodeInput {
            code: input.clone(),
            publish: true,
            revision_id: Some("rev-1".to_owned()),
        };

        assert_eq!(
            update,
            UpdateFunctionCodeInput {
                code: input,
                publish: true,
                revision_id: Some("rev-1".to_owned()),
            }
        );
    }

    #[test]
    fn image_packages_accept_optional_runtime_and_nonblank_handler() {
        assert_eq!(
            validate_runtime_for_package(
                LambdaPackageType::Image,
                Some("any-runtime")
            )
            .unwrap(),
            Some("any-runtime".to_owned())
        );
        assert_eq!(
            validate_handler_for_package(
                LambdaPackageType::Image,
                Some("bootstrap.handler".to_owned())
            )
            .unwrap(),
            Some("bootstrap.handler".to_owned())
        );
        assert_eq!(
            validate_handler_for_package(
                LambdaPackageType::Image,
                Some("   ".to_owned())
            )
            .unwrap(),
            None
        );
    }

    #[test]
    fn zip_packages_require_runtime_from_supported_set() {
        assert_eq!(
            validate_runtime_for_package(
                LambdaPackageType::Zip,
                Some("provided.al2023")
            )
            .unwrap(),
            Some("provided.al2023".to_owned())
        );

        let missing_runtime =
            validate_runtime_for_package(LambdaPackageType::Zip, None)
                .expect_err("zip packages must require a runtime");
        match missing_runtime {
            LambdaError::InvalidParameterValue { message } => {
                assert_eq!(
                    message,
                    "Runtime is required for Zip package type."
                );
            }
            other => panic!("expected invalid parameter value, got {other:?}"),
        }

        let unsupported_runtime = validate_runtime_for_package(
            LambdaPackageType::Zip,
            Some("nodejs22.x"),
        )
        .expect_err("unsupported runtimes must be rejected");
        match unsupported_runtime {
            LambdaError::InvalidParameterValue { message } => {
                assert_eq!(
                    message,
                    "Value nodejs22.x at 'runtime' failed to satisfy constraint: Member must satisfy enum value set: [provided, provided.al2, provided.al2023, python3.8, python3.9, python3.10, python3.11, python3.12] or be a valid ARN"
                );
            }
            other => panic!("expected invalid parameter value, got {other:?}"),
        }
    }

    #[test]
    fn zip_packages_require_nonblank_handler_without_whitespace() {
        assert_eq!(
            validate_handler_for_package(
                LambdaPackageType::Zip,
                Some("bootstrap.handler".to_owned())
            )
            .unwrap(),
            Some("bootstrap.handler".to_owned())
        );

        let missing_handler =
            validate_handler_for_package(LambdaPackageType::Zip, None)
                .expect_err("zip packages must require a handler");
        match missing_handler {
            LambdaError::InvalidParameterValue { message } => {
                assert_eq!(
                    message,
                    "Handler is required for Zip package type."
                );
            }
            other => panic!("expected invalid parameter value, got {other:?}"),
        }

        let invalid_handler = validate_handler_for_package(
            LambdaPackageType::Zip,
            Some("bootstrap handler".to_owned()),
        )
        .expect_err("zip handlers cannot contain whitespace");
        match invalid_handler {
            LambdaError::Validation { message } => {
                assert_eq!(message, "Handler cannot contain whitespace.");
            }
            other => panic!("expected validation error, got {other:?}"),
        }
    }

    #[test]
    fn sha256_base64_matches_aws_digest_shape() {
        assert_eq!(
            sha256_base64(b"zip-bytes"),
            "S5pKxZ88OqMicyYN9s9L81jRxG+EFRJqo1tjgNCruPc="
        );
    }
}
