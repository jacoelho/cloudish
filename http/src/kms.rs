pub(crate) use crate::aws_error_shape::AwsErrorShape;
use crate::request::HttpRequest;
use aws::{AwsError, KmsAliasNameError, RequestContext};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use serde::Deserialize;
use serde_json::{Value, json};
use services::{
    CancelKmsKeyDeletionInput, CreateKmsAliasInput, CreateKmsKeyInput,
    DeleteKmsAliasInput, KmsAliasName, KmsDecryptInput, KmsEncryptInput,
    KmsError, KmsGenerateDataKeyInput, KmsKeyReference,
    KmsListResourceTagsInput, KmsScope, KmsService, KmsSignInput, KmsTag,
    KmsTagResourceInput, KmsUntagResourceInput, KmsVerifyInput,
    ListKmsAliasesInput, ScheduleKmsKeyDeletionInput,
};

pub(crate) fn handle_json(
    kms: &KmsService,
    request: &HttpRequest<'_>,
    context: &RequestContext,
) -> Result<Vec<u8>, AwsError> {
    let operation = operation_from_target(request.header("x-amz-target"))
        .map_err(|error| error.to_aws_error())?;
    let scope =
        KmsScope::new(context.account_id().clone(), context.region().clone());

    match operation {
        KmsOperation::CreateKey => {
            let request = parse_json_body::<CreateKeyRequest>(request.body())?;
            json_response(json!({
                "KeyMetadata": kms
                    .create_key(
                        &scope,
                        CreateKmsKeyInput {
                            description: request.description,
                            key_spec: request.key_spec,
                            key_usage: request.key_usage,
                            tags: request.tags.unwrap_or_default(),
                        },
                    )
                    .map_err(|error| error.to_aws_error())?
                    .key_metadata,
            }))
        }
        KmsOperation::DescribeKey => {
            let request = parse_json_body::<KeyIdRequest>(request.body())?;
            json_response(json!({
                "KeyMetadata": kms
                    .describe_key(&scope, &parse_key_reference(request.key_id)?)
                    .map_err(|error| error.to_aws_error())?
                    .key_metadata,
            }))
        }
        KmsOperation::ListKeys => {
            let request = parse_json_body::<ListKeysRequest>(request.body())?;
            reject_pagination("ListKeys", request.limit, request.marker)?;
            let output =
                kms.list_keys(&scope).map_err(|error| error.to_aws_error())?;

            json_response(json!({
                "Keys": output.keys,
                "Truncated": output.truncated,
            }))
        }
        KmsOperation::ScheduleKeyDeletion => {
            let request =
                parse_json_body::<ScheduleKeyDeletionRequest>(request.body())?;
            let output = kms
                .schedule_key_deletion(
                    &scope,
                    ScheduleKmsKeyDeletionInput {
                        key_id: parse_key_reference(request.key_id)?,
                        pending_window_in_days: request.pending_window_in_days,
                    },
                )
                .map_err(|error| error.to_aws_error())?;

            json_response(json!({
                "DeletionDate": output.deletion_date,
                "KeyId": output.key_id,
                "KeyState": output.key_state,
                "PendingWindowInDays": output.pending_window_in_days,
            }))
        }
        KmsOperation::CancelKeyDeletion => {
            let request = parse_json_body::<KeyIdRequest>(request.body())?;
            json_response(json!(
                kms.cancel_key_deletion(
                    &scope,
                    CancelKmsKeyDeletionInput {
                        key_id: parse_key_reference(request.key_id)?,
                    },
                )
                .map_err(|error| error.to_aws_error())?
            ))
        }
        KmsOperation::CreateAlias => {
            let request =
                parse_json_body::<CreateAliasRequest>(request.body())?;
            kms.create_alias(
                &scope,
                CreateKmsAliasInput {
                    alias_name: parse_alias_name(request.alias_name)?,
                    target_key_id: parse_key_reference(request.target_key_id)?,
                },
            )
            .map_err(|error| error.to_aws_error())?;
            json_response(json!({}))
        }
        KmsOperation::DeleteAlias => {
            let request =
                parse_json_body::<DeleteAliasRequest>(request.body())?;
            kms.delete_alias(
                &scope,
                DeleteKmsAliasInput {
                    alias_name: parse_alias_name(request.alias_name)?,
                },
            )
            .map_err(|error| error.to_aws_error())?;
            json_response(json!({}))
        }
        KmsOperation::ListAliases => {
            let request =
                parse_json_body::<ListAliasesRequest>(request.body())?;
            reject_pagination("ListAliases", request.limit, request.marker)?;
            let output = kms
                .list_aliases(
                    &scope,
                    ListKmsAliasesInput {
                        key_id: request
                            .key_id
                            .map(parse_key_reference)
                            .transpose()?,
                    },
                )
                .map_err(|error| error.to_aws_error())?;

            json_response(json!({
                "Aliases": output.aliases,
                "Truncated": output.truncated,
            }))
        }
        KmsOperation::Encrypt => {
            let request = parse_json_body::<EncryptRequest>(request.body())?;
            let output = kms
                .encrypt(
                    &scope,
                    KmsEncryptInput {
                        key_id: parse_key_reference(request.key_id)?,
                        plaintext: decode_request_blob(
                            request.plaintext,
                            BlobKind::Plaintext,
                        )?,
                    },
                )
                .map_err(|error| error.to_aws_error())?;

            json_response(json!({
                "CiphertextBlob": BASE64_STANDARD.encode(output.ciphertext_blob),
                "KeyId": output.key_id,
            }))
        }
        KmsOperation::Decrypt => {
            let request = parse_json_body::<DecryptRequest>(request.body())?;
            let output = kms
                .decrypt(
                    &scope,
                    KmsDecryptInput {
                        ciphertext_blob: decode_request_blob(
                            request.ciphertext_blob,
                            BlobKind::Ciphertext,
                        )?,
                        key_id: request
                            .key_id
                            .map(parse_key_reference)
                            .transpose()?,
                    },
                )
                .map_err(|error| error.to_aws_error())?;

            json_response(json!({
                "KeyId": output.key_id,
                "Plaintext": BASE64_STANDARD.encode(output.plaintext),
            }))
        }
        KmsOperation::ReEncrypt => {
            let request = parse_json_body::<ReEncryptRequest>(request.body())?;
            let output = kms
                .re_encrypt(
                    &scope,
                    services::KmsReEncryptInput {
                        ciphertext_blob: decode_request_blob(
                            request.ciphertext_blob,
                            BlobKind::Ciphertext,
                        )?,
                        destination_key_id: parse_key_reference(
                            request.destination_key_id,
                        )?,
                        source_key_id: request
                            .source_key_id
                            .map(parse_key_reference)
                            .transpose()?,
                    },
                )
                .map_err(|error| error.to_aws_error())?;

            json_response(json!({
                "CiphertextBlob": BASE64_STANDARD.encode(output.ciphertext_blob),
                "KeyId": output.key_id,
                "SourceKeyId": output.source_key_id,
            }))
        }
        KmsOperation::GenerateDataKey => {
            let request =
                parse_json_body::<GenerateDataKeyRequest>(request.body())?;
            let output = kms
                .generate_data_key(
                    &scope,
                    KmsGenerateDataKeyInput {
                        key_id: parse_key_reference(request.key_id)?,
                        key_spec: request.key_spec,
                        number_of_bytes: request.number_of_bytes,
                    },
                )
                .map_err(|error| error.to_aws_error())?;

            json_response(json!({
                "CiphertextBlob": BASE64_STANDARD.encode(output.ciphertext_blob),
                "KeyId": output.key_id,
                "Plaintext": BASE64_STANDARD.encode(output.plaintext),
            }))
        }
        KmsOperation::GenerateDataKeyWithoutPlaintext => {
            let request =
                parse_json_body::<GenerateDataKeyRequest>(request.body())?;
            let output = kms
                .generate_data_key_without_plaintext(
                    &scope,
                    KmsGenerateDataKeyInput {
                        key_id: parse_key_reference(request.key_id)?,
                        key_spec: request.key_spec,
                        number_of_bytes: request.number_of_bytes,
                    },
                )
                .map_err(|error| error.to_aws_error())?;

            json_response(json!({
                "CiphertextBlob": BASE64_STANDARD.encode(output.ciphertext_blob),
                "KeyId": output.key_id,
            }))
        }
        KmsOperation::Sign => {
            let request = parse_json_body::<SignRequest>(request.body())?;
            let output = kms
                .sign(
                    &scope,
                    KmsSignInput {
                        key_id: parse_key_reference(request.key_id)?,
                        message: decode_request_blob(
                            request.message,
                            BlobKind::Message,
                        )?,
                        message_type: request.message_type,
                        signing_algorithm: request.signing_algorithm,
                    },
                )
                .map_err(|error| error.to_aws_error())?;

            json_response(json!({
                "KeyId": output.key_id,
                "Signature": BASE64_STANDARD.encode(output.signature),
                "SigningAlgorithm": output.signing_algorithm,
            }))
        }
        KmsOperation::Verify => {
            let request = parse_json_body::<VerifyRequest>(request.body())?;
            let output = kms
                .verify(
                    &scope,
                    KmsVerifyInput {
                        key_id: parse_key_reference(request.key_id)?,
                        message: decode_request_blob(
                            request.message,
                            BlobKind::Message,
                        )?,
                        message_type: request.message_type,
                        signature: decode_request_blob(
                            request.signature,
                            BlobKind::Signature,
                        )?,
                        signing_algorithm: request.signing_algorithm,
                    },
                )
                .map_err(|error| error.to_aws_error())?;

            json_response(json!({
                "KeyId": output.key_id,
                "SignatureValid": output.signature_valid,
                "SigningAlgorithm": output.signing_algorithm,
            }))
        }
        KmsOperation::TagResource => {
            let request =
                parse_json_body::<TagResourceRequest>(request.body())?;
            kms.tag_resource(
                &scope,
                KmsTagResourceInput {
                    key_id: parse_key_reference(request.key_id)?,
                    tags: request.tags,
                },
            )
            .map_err(|error| error.to_aws_error())?;
            json_response(json!({}))
        }
        KmsOperation::UntagResource => {
            let request =
                parse_json_body::<UntagResourceRequest>(request.body())?;
            kms.untag_resource(
                &scope,
                KmsUntagResourceInput {
                    key_id: parse_key_reference(request.key_id)?,
                    tag_keys: request.tag_keys,
                },
            )
            .map_err(|error| error.to_aws_error())?;
            json_response(json!({}))
        }
        KmsOperation::ListResourceTags => {
            let request =
                parse_json_body::<ListResourceTagsRequest>(request.body())?;
            reject_pagination(
                "ListResourceTags",
                request.limit,
                request.marker,
            )?;
            let output = kms
                .list_resource_tags(
                    &scope,
                    KmsListResourceTagsInput {
                        key_id: parse_key_reference(request.key_id)?,
                    },
                )
                .map_err(|error| error.to_aws_error())?;

            json_response(json!({
                "Tags": output.tags,
                "Truncated": output.truncated,
            }))
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BlobKind {
    Ciphertext,
    Message,
    Plaintext,
    Signature,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum KmsOperation {
    CancelKeyDeletion,
    CreateAlias,
    CreateKey,
    Decrypt,
    DeleteAlias,
    DescribeKey,
    Encrypt,
    GenerateDataKey,
    GenerateDataKeyWithoutPlaintext,
    ListAliases,
    ListKeys,
    ListResourceTags,
    ReEncrypt,
    ScheduleKeyDeletion,
    Sign,
    TagResource,
    UntagResource,
    Verify,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase", deny_unknown_fields)]
struct CreateKeyRequest {
    description: Option<String>,
    key_spec: Option<String>,
    key_usage: Option<String>,
    tags: Option<Vec<KmsTag>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase", deny_unknown_fields)]
struct KeyIdRequest {
    key_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase", deny_unknown_fields)]
struct ListKeysRequest {
    limit: Option<u32>,
    marker: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase", deny_unknown_fields)]
struct ScheduleKeyDeletionRequest {
    key_id: String,
    pending_window_in_days: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase", deny_unknown_fields)]
struct CreateAliasRequest {
    alias_name: String,
    target_key_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase", deny_unknown_fields)]
struct DeleteAliasRequest {
    alias_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase", deny_unknown_fields)]
struct ListAliasesRequest {
    key_id: Option<String>,
    limit: Option<u32>,
    marker: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase", deny_unknown_fields)]
struct EncryptRequest {
    key_id: String,
    plaintext: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase", deny_unknown_fields)]
struct DecryptRequest {
    ciphertext_blob: String,
    key_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase", deny_unknown_fields)]
struct ReEncryptRequest {
    ciphertext_blob: String,
    destination_key_id: String,
    source_key_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase", deny_unknown_fields)]
struct GenerateDataKeyRequest {
    key_id: String,
    key_spec: Option<String>,
    number_of_bytes: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase", deny_unknown_fields)]
struct SignRequest {
    key_id: String,
    message: String,
    message_type: Option<String>,
    signing_algorithm: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase", deny_unknown_fields)]
struct VerifyRequest {
    key_id: String,
    message: String,
    message_type: Option<String>,
    signature: String,
    signing_algorithm: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase", deny_unknown_fields)]
struct TagResourceRequest {
    key_id: String,
    tags: Vec<KmsTag>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase", deny_unknown_fields)]
struct UntagResourceRequest {
    key_id: String,
    tag_keys: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase", deny_unknown_fields)]
struct ListResourceTagsRequest {
    key_id: String,
    limit: Option<u32>,
    marker: Option<String>,
}

fn operation_from_target(
    target: Option<&str>,
) -> Result<KmsOperation, KmsError> {
    let target = target.ok_or_else(|| KmsError::UnsupportedOperation {
        message: "missing X-Amz-Target".to_owned(),
    })?;
    let Some((prefix, operation)) = target.split_once('.') else {
        return Err(KmsError::UnsupportedOperation {
            message: format!("Operation {target} is not supported."),
        });
    };
    if prefix != "TrentService" {
        return Err(KmsError::UnsupportedOperation {
            message: format!("Operation {target} is not supported."),
        });
    }

    match operation {
        "CreateKey" => Ok(KmsOperation::CreateKey),
        "DescribeKey" => Ok(KmsOperation::DescribeKey),
        "ListKeys" => Ok(KmsOperation::ListKeys),
        "ScheduleKeyDeletion" => Ok(KmsOperation::ScheduleKeyDeletion),
        "CancelKeyDeletion" => Ok(KmsOperation::CancelKeyDeletion),
        "CreateAlias" => Ok(KmsOperation::CreateAlias),
        "DeleteAlias" => Ok(KmsOperation::DeleteAlias),
        "ListAliases" => Ok(KmsOperation::ListAliases),
        "Encrypt" => Ok(KmsOperation::Encrypt),
        "Decrypt" => Ok(KmsOperation::Decrypt),
        "ReEncrypt" => Ok(KmsOperation::ReEncrypt),
        "GenerateDataKey" => Ok(KmsOperation::GenerateDataKey),
        "GenerateDataKeyWithoutPlaintext" => {
            Ok(KmsOperation::GenerateDataKeyWithoutPlaintext)
        }
        "Sign" => Ok(KmsOperation::Sign),
        "Verify" => Ok(KmsOperation::Verify),
        "TagResource" => Ok(KmsOperation::TagResource),
        "UntagResource" => Ok(KmsOperation::UntagResource),
        "ListResourceTags" => Ok(KmsOperation::ListResourceTags),
        _ => Err(KmsError::UnsupportedOperation {
            message: format!("Operation {operation} is not supported."),
        }),
    }
}

fn parse_json_body<T>(body: &[u8]) -> Result<T, AwsError>
where
    T: for<'de> Deserialize<'de>,
{
    serde_json::from_slice(body).map_err(|error| {
        KmsError::ValidationException {
            message: format!("The request body is not valid JSON: {error}"),
        }
        .to_aws_error()
    })
}

fn decode_request_blob(
    encoded: String,
    kind: BlobKind,
) -> Result<Vec<u8>, AwsError> {
    BASE64_STANDARD.decode(encoded).map_err(|_| match kind {
        BlobKind::Ciphertext => KmsError::InvalidCiphertextException {
            message: "The ciphertext is invalid.".to_owned(),
        }
        .to_aws_error(),
        BlobKind::Signature => KmsError::InvalidSignatureException {
            message: "The signature is invalid.".to_owned(),
        }
        .to_aws_error(),
        BlobKind::Plaintext | BlobKind::Message => {
            KmsError::ValidationException {
                message: "The provided blob value is not valid base64."
                    .to_owned(),
            }
            .to_aws_error()
        }
    })
}

fn parse_alias_name(value: String) -> Result<KmsAliasName, AwsError> {
    value.parse::<KmsAliasName>().map_err(|error| match error {
        KmsAliasNameError::MissingPrefix => KmsError::ValidationException {
            message: "Alias must start with the prefix \"alias/\". Please see https://docs.aws.amazon.com/kms/latest/developerguide/kms-alias.html".to_owned(),
        }
        .to_aws_error(),
    })
}

fn parse_key_reference(value: String) -> Result<KmsKeyReference, AwsError> {
    value.parse::<KmsKeyReference>().map_err(|error| match error {
        services::KmsKeyReferenceError::InvalidArn(_) => {
            KmsError::InvalidArnException {
                message: format!("Invalid arn {value}"),
            }
            .to_aws_error()
        }
        _ => KmsError::ValidationException { message: error.to_string() }
            .to_aws_error(),
    })
}

fn reject_pagination(
    operation: &str,
    limit: Option<u32>,
    marker: Option<String>,
) -> Result<(), AwsError> {
    if limit.is_none() && marker.is_none() {
        return Ok(());
    }

    Err(KmsError::ValidationException {
        message: format!(
            "Pagination is not supported for {operation} in Cloudish KMS."
        ),
    }
    .to_aws_error())
}

fn json_response(body: Value) -> Result<Vec<u8>, AwsError> {
    serde_json::to_vec(&body).map_err(|error| {
        KmsError::InternalFailure {
            message: format!("Failed to serialize KMS response: {error}"),
        }
        .to_aws_error()
    })
}

#[cfg(test)]
mod tests {
    use super::{BlobKind, handle_json, operation_from_target};
    use crate::aws_error_shape::AwsErrorShape;
    use crate::request::HttpRequest;
    use crate::test_runtime::FixedClock;
    use aws::{ProtocolFamily, RequestContext, ServiceName};
    use serde_json::Value;
    use services::KmsService;
    use std::sync::Arc;
    use std::time::UNIX_EPOCH;
    use storage::{StorageConfig, StorageFactory, StorageMode};

    fn kms_service() -> KmsService {
        let factory = StorageFactory::new(StorageConfig::new(
            "/tmp/cloudish-http-kms",
            StorageMode::Memory,
        ));

        KmsService::new(&factory, Arc::new(FixedClock::new(UNIX_EPOCH)))
    }

    fn context(operation: &str) -> RequestContext {
        RequestContext::try_new(
            "000000000000".parse().expect("account should parse"),
            "eu-west-2".parse().expect("region should parse"),
            ServiceName::Kms,
            ProtocolFamily::AwsJson11,
            operation,
            None,
            false,
        )
        .expect("context should build")
    }

    fn request(target: &str, body: &str) -> HttpRequest<'static> {
        let raw = format!(
            "POST / HTTP/1.1\r\nHost: localhost\r\nX-Amz-Target: {target}\r\nContent-Type: application/x-amz-json-1.1\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        )
        .into_bytes();
        let raw = Box::leak(raw.into_boxed_slice());

        HttpRequest::parse(raw).expect("request should parse")
    }

    #[test]
    fn kms_json_direct_handler_round_trips_core_operations() {
        let kms = kms_service();

        let create = handle_json(
            &kms,
            &request("TrentService.CreateKey", r#"{"Description":"encrypt"}"#),
            &context("CreateKey"),
        )
        .expect("create key should succeed");
        let create: Value =
            serde_json::from_slice(&create).expect("create body should parse");
        let key_id = create["KeyMetadata"]["KeyId"]
            .as_str()
            .expect("key id should be present");

        handle_json(
            &kms,
            &request(
                "TrentService.CreateAlias",
                &format!(
                    r#"{{"AliasName":"alias/demo","TargetKeyId":"{key_id}"}}"#
                ),
            ),
            &context("CreateAlias"),
        )
        .expect("alias creation should succeed");

        let listed = handle_json(
            &kms,
            &request("TrentService.ListKeys", "{}"),
            &context("ListKeys"),
        )
        .expect("list keys should succeed");
        let listed: Value =
            serde_json::from_slice(&listed).expect("list body should parse");
        assert_eq!(listed["Keys"].as_array().map(Vec::len), Some(1));

        let encrypted = handle_json(
            &kms,
            &request(
                "TrentService.Encrypt",
                r#"{"KeyId":"alias/demo","Plaintext":"cGF5bG9hZA=="}"#,
            ),
            &context("Encrypt"),
        )
        .expect("encrypt should succeed");
        let encrypted: Value = serde_json::from_slice(&encrypted)
            .expect("encrypt body should parse");
        let ciphertext = encrypted["CiphertextBlob"]
            .as_str()
            .expect("ciphertext should be present");

        let decrypted = handle_json(
            &kms,
            &request(
                "TrentService.Decrypt",
                &format!(r#"{{"CiphertextBlob":"{ciphertext}"}}"#),
            ),
            &context("Decrypt"),
        )
        .expect("decrypt should succeed");
        let decrypted: Value = serde_json::from_slice(&decrypted)
            .expect("decrypt body should parse");
        assert_eq!(decrypted["Plaintext"].as_str(), Some("cGF5bG9hZA=="));

        let data_key = handle_json(
            &kms,
            &request(
                "TrentService.GenerateDataKey",
                &format!(r#"{{"KeyId":"{key_id}","KeySpec":"AES_256"}}"#),
            ),
            &context("GenerateDataKey"),
        )
        .expect("generate data key should succeed");
        let data_key: Value = serde_json::from_slice(&data_key)
            .expect("data-key body should parse");
        assert!(data_key["Plaintext"].as_str().is_some());
        assert!(data_key["CiphertextBlob"].as_str().is_some());

        let sign_key = handle_json(
            &kms,
            &request(
                "TrentService.CreateKey",
                r#"{"Description":"sign","KeySpec":"RSA_2048","KeyUsage":"SIGN_VERIFY"}"#,
            ),
            &context("CreateKey"),
        )
        .expect("signing key should create");
        let sign_key: Value = serde_json::from_slice(&sign_key)
            .expect("sign key body should parse");
        let sign_key_id = sign_key["KeyMetadata"]["KeyId"]
            .as_str()
            .expect("sign key id should be present");

        let signature = handle_json(
            &kms,
            &request(
                "TrentService.Sign",
                &format!(
                    r#"{{"KeyId":"{sign_key_id}","Message":"bWVzc2FnZQ==","SigningAlgorithm":"RSASSA_PSS_SHA_256"}}"#
                ),
            ),
            &context("Sign"),
        )
        .expect("sign should succeed");
        let signature: Value = serde_json::from_slice(&signature)
            .expect("sign body should parse");
        let signature = signature["Signature"]
            .as_str()
            .expect("signature should be present");

        let verification = handle_json(
            &kms,
            &request(
                "TrentService.Verify",
                &format!(
                    r#"{{"KeyId":"{sign_key_id}","Message":"bWVzc2FnZQ==","Signature":"{signature}","SigningAlgorithm":"RSASSA_PSS_SHA_256"}}"#
                ),
            ),
            &context("Verify"),
        )
        .expect("verify should succeed");
        let verification: Value = serde_json::from_slice(&verification)
            .expect("verify body should parse");
        assert_eq!(verification["SignatureValid"], true);

        handle_json(
            &kms,
            &request(
                "TrentService.TagResource",
                &format!(
                    r#"{{"KeyId":"{key_id}","Tags":[{{"TagKey":"env","TagValue":"dev"}}]}}"#
                ),
            ),
            &context("TagResource"),
        )
        .expect("tag resource should succeed");
        let tags = handle_json(
            &kms,
            &request(
                "TrentService.ListResourceTags",
                &format!(r#"{{"KeyId":"{key_id}"}}"#),
            ),
            &context("ListResourceTags"),
        )
        .expect("list tags should succeed");
        let tags: Value =
            serde_json::from_slice(&tags).expect("tags body should parse");
        assert_eq!(tags["Tags"].as_array().map(Vec::len), Some(1));
    }

    #[test]
    fn kms_json_direct_handlers_report_explicit_errors_and_gap_paths() {
        let kms = kms_service();

        let missing_target = operation_from_target(None)
            .expect_err("missing target should fail");
        let wrong_prefix =
            operation_from_target(Some("AmazonSSM.GetParameter"))
                .expect_err("wrong prefix should fail");
        let unsupported =
            operation_from_target(Some("TrentService.GetKeyPolicy"))
                .expect_err("unsupported operation should fail");
        let invalid_json = handle_json(
            &kms,
            &request("TrentService.CreateKey", "{"),
            &context("CreateKey"),
        )
        .expect_err("invalid json should fail");
        let invalid_alias = handle_json(
            &kms,
            &request(
                "TrentService.CreateAlias",
                r#"{"AliasName":"demo","TargetKeyId":"missing"}"#,
            ),
            &context("CreateAlias"),
        )
        .expect_err("alias without prefix should fail");
        let bad_ciphertext = handle_json(
            &kms,
            &request("TrentService.Decrypt", r#"{"CiphertextBlob":"@@@"}"#),
            &context("Decrypt"),
        )
        .expect_err("invalid ciphertext should fail");
        let bad_signature = handle_json(
            &kms,
            &request(
                "TrentService.Verify",
                r#"{"KeyId":"missing","Message":"bWVzc2FnZQ==","Signature":"@@@","SigningAlgorithm":"RSASSA_PSS_SHA_256"}"#,
            ),
            &context("Verify"),
        )
        .expect_err("invalid signature should fail");
        let pagination = handle_json(
            &kms,
            &request("TrentService.ListKeys", r#"{"Limit":1}"#),
            &context("ListKeys"),
        )
        .expect_err("pagination should fail");
        let invalid_plaintext =
            super::decode_request_blob("@@@".to_owned(), BlobKind::Plaintext)
                .expect_err("invalid plaintext should fail");

        assert_eq!(
            missing_target.to_aws_error().code(),
            "UnsupportedOperation"
        );
        assert_eq!(wrong_prefix.to_aws_error().code(), "UnsupportedOperation");
        assert_eq!(unsupported.to_aws_error().code(), "UnsupportedOperation");
        assert_eq!(invalid_json.code(), "ValidationException");
        assert_eq!(invalid_alias.code(), "ValidationException");
        assert_eq!(bad_ciphertext.code(), "InvalidCiphertextException");
        assert_eq!(bad_signature.code(), "KMSInvalidSignatureException");
        assert_eq!(pagination.code(), "ValidationException");
        assert_eq!(invalid_plaintext.code(), "ValidationException");
    }
}
