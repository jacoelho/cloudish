use crate::{
    CIPHERTEXT_PREFIX, MAX_DATA_KEY_BYTES, SIGNATURE_PREFIX,
    errors::KmsError,
    invalid_state_error,
    keys::{KmsKeyState, KmsKeyUsage, StoredKmsKey},
};
use aws::{Arn, KmsKeyId, KmsKeyReference};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use serde::Serialize;
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KmsEncryptInput {
    pub key_id: KmsKeyReference,
    pub plaintext: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct KmsEncryptOutput {
    pub ciphertext_blob: Vec<u8>,
    pub key_id: Arn,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KmsDecryptInput {
    pub ciphertext_blob: Vec<u8>,
    pub key_id: Option<KmsKeyReference>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct KmsDecryptOutput {
    pub key_id: Arn,
    pub plaintext: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KmsReEncryptInput {
    pub ciphertext_blob: Vec<u8>,
    pub destination_key_id: KmsKeyReference,
    pub source_key_id: Option<KmsKeyReference>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct KmsReEncryptOutput {
    pub ciphertext_blob: Vec<u8>,
    pub key_id: Arn,
    pub source_key_id: Arn,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KmsGenerateDataKeyInput {
    pub key_id: KmsKeyReference,
    pub key_spec: Option<String>,
    pub number_of_bytes: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct KmsGenerateDataKeyOutput {
    pub ciphertext_blob: Vec<u8>,
    pub key_id: Arn,
    pub plaintext: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct KmsGenerateDataKeyWithoutPlaintextOutput {
    pub ciphertext_blob: Vec<u8>,
    pub key_id: Arn,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KmsSignInput {
    pub key_id: KmsKeyReference,
    pub message: Vec<u8>,
    pub message_type: Option<String>,
    pub signing_algorithm: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct KmsSignOutput {
    pub key_id: Arn,
    pub signature: Vec<u8>,
    pub signing_algorithm: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KmsVerifyInput {
    pub key_id: KmsKeyReference,
    pub message: Vec<u8>,
    pub message_type: Option<String>,
    pub signature: Vec<u8>,
    pub signing_algorithm: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct KmsVerifyOutput {
    pub key_id: Arn,
    pub signature_valid: bool,
    pub signing_algorithm: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum KmsMessageType {
    Raw,
    Digest,
}

impl KmsMessageType {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Raw => "RAW",
            Self::Digest => "DIGEST",
        }
    }

    pub(crate) fn parse(value: Option<&str>) -> Result<Self, KmsError> {
        match value.unwrap_or("RAW") {
            "RAW" => Ok(Self::Raw),
            "DIGEST" => Ok(Self::Digest),
            other => Err(KmsError::ValidationException {
                message: format!("Message type {other} is not supported."),
            }),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DataKeySpec {
    Aes128,
    Aes256,
}

impl DataKeySpec {
    fn byte_len(self) -> usize {
        match self {
            Self::Aes128 => 16,
            Self::Aes256 => 32,
        }
    }

    fn parse(value: &str) -> Result<Self, KmsError> {
        match value {
            "AES_128" => Ok(Self::Aes128),
            "AES_256" => Ok(Self::Aes256),
            _ => Err(KmsError::ValidationException {
                message: format!("Data key spec {value} is not supported."),
            }),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DecodedCiphertext {
    pub(crate) key_id: KmsKeyId,
    pub(crate) plaintext: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DecodedSignature {
    pub(crate) key_id: KmsKeyId,
    pub(crate) message: Vec<u8>,
    pub(crate) message_type: KmsMessageType,
    pub(crate) signing_algorithm: String,
}

pub(crate) fn data_key_byte_len(
    input: &KmsGenerateDataKeyInput,
) -> Result<usize, KmsError> {
    match (input.key_spec.as_deref(), input.number_of_bytes) {
        (Some(spec), None) => Ok(DataKeySpec::parse(spec)?.byte_len()),
        (None, Some(number_of_bytes)) => {
            if number_of_bytes == 0 || number_of_bytes > MAX_DATA_KEY_BYTES {
                return Err(KmsError::ValidationException {
                    message: format!(
                        "1 validation error detected: Value '{number_of_bytes}' at \
                         'numberOfBytes' failed to satisfy constraint: Member \
                         must have value less than or equal to {MAX_DATA_KEY_BYTES} and greater \
                         than or equal to 1",
                    ),
                });
            }

            Ok(number_of_bytes)
        }
        (Some(_), Some(_)) => Err(KmsError::ValidationException {
            message: "Specify either KeySpec or NumberOfBytes, not both."
                .to_owned(),
        }),
        (None, None) => Err(KmsError::ValidationException {
            message: "Either KeySpec or NumberOfBytes is required.".to_owned(),
        }),
    }
}

pub(crate) fn ensure_encrypt_key(
    key: &StoredKmsKey,
    operation: &str,
) -> Result<(), KmsError> {
    if key.key_state == KmsKeyState::PendingDeletion {
        return Err(invalid_state_error(format!(
            "{} is pending deletion.",
            key.arn
        )));
    }
    if key.key_usage != KmsKeyUsage::EncryptDecrypt {
        return Err(KmsError::InvalidKeyUsageException {
            message: format!(
                "{} cannot perform {} because its KeyUsage is {}.",
                key.arn,
                operation,
                key.key_usage.as_str(),
            ),
        });
    }

    Ok(())
}

pub(crate) fn ensure_sign_key(
    key: &StoredKmsKey,
    operation: &str,
) -> Result<(), KmsError> {
    if key.key_state == KmsKeyState::PendingDeletion {
        return Err(invalid_state_error(format!(
            "{} is pending deletion.",
            key.arn
        )));
    }
    if key.key_usage != KmsKeyUsage::SignVerify {
        return Err(KmsError::InvalidKeyUsageException {
            message: format!(
                "{} cannot perform {} because its KeyUsage is {}.",
                key.arn,
                operation,
                key.key_usage.as_str(),
            ),
        });
    }

    Ok(())
}

pub(crate) fn derived_bytes(labels: &[&str], len: usize) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(len);
    let mut counter = 0_u64;

    while bytes.len() < len {
        let mut hasher = Sha256::new();
        for label in labels {
            hasher.update(label.as_bytes());
            hasher.update([0_u8]);
        }
        hasher.update(counter.to_be_bytes());
        bytes.extend_from_slice(&hasher.finalize());
        counter = counter.saturating_add(1);
    }

    bytes.truncate(len);
    bytes
}

pub(crate) fn hash_bytes(labels: &[&str]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    for label in labels {
        hasher.update(label.as_bytes());
        hasher.update([0_u8]);
    }

    hasher.finalize().into()
}

pub(crate) fn hex_encode(bytes: &[u8]) -> String {
    let mut hex = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        let _ =
            std::fmt::Write::write_fmt(&mut hex, format_args!("{byte:02x}"));
    }

    hex
}

pub(crate) struct MockKmsCodec;

impl MockKmsCodec {
    pub(crate) fn encrypt(key_id: &KmsKeyId, plaintext: &[u8]) -> Vec<u8> {
        format!(
            "{CIPHERTEXT_PREFIX}:{key_id}:{}",
            BASE64_STANDARD.encode(plaintext)
        )
        .into_bytes()
    }

    pub(crate) fn decrypt(
        ciphertext: &[u8],
    ) -> Result<DecodedCiphertext, KmsError> {
        let text = std::str::from_utf8(ciphertext).map_err(|_| {
            KmsError::InvalidCiphertextException {
                message: "The ciphertext is invalid.".to_owned(),
            }
        })?;
        let mut parts = text.splitn(3, ':');
        let prefix = parts.next().unwrap_or_default();
        let key_id = parts.next().unwrap_or_default();
        let payload = parts.next().unwrap_or_default();
        if prefix != CIPHERTEXT_PREFIX
            || key_id.is_empty()
            || payload.is_empty()
        {
            return Err(KmsError::InvalidCiphertextException {
                message: "The ciphertext is invalid.".to_owned(),
            });
        }

        let plaintext = BASE64_STANDARD.decode(payload).map_err(|_| {
            KmsError::InvalidCiphertextException {
                message: "The ciphertext is invalid.".to_owned(),
            }
        })?;

        Ok(DecodedCiphertext {
            key_id: KmsKeyId::new(key_id.to_owned()).map_err(|_| {
                KmsError::InvalidCiphertextException {
                    message: "The ciphertext is invalid.".to_owned(),
                }
            })?,
            plaintext,
        })
    }

    pub(crate) fn sign(
        key_id: &KmsKeyId,
        message: &[u8],
        message_type: KmsMessageType,
        signing_algorithm: &str,
    ) -> Vec<u8> {
        format!(
            "{SIGNATURE_PREFIX}:{key_id}:{signing_algorithm}:{}:{}",
            message_type.as_str(),
            BASE64_STANDARD.encode(message),
        )
        .into_bytes()
    }

    pub(crate) fn decode_signature(
        signature: &[u8],
    ) -> Result<DecodedSignature, KmsError> {
        let text = std::str::from_utf8(signature).map_err(|_| {
            KmsError::InvalidSignatureException {
                message: "The signature is invalid.".to_owned(),
            }
        })?;
        let mut parts = text.splitn(5, ':');
        let prefix = parts.next().unwrap_or_default();
        let key_id = parts.next().unwrap_or_default();
        let signing_algorithm = parts.next().unwrap_or_default();
        let message_type = parts.next().unwrap_or_default();
        let payload = parts.next().unwrap_or_default();
        if prefix != SIGNATURE_PREFIX
            || key_id.is_empty()
            || signing_algorithm.is_empty()
            || message_type.is_empty()
            || payload.is_empty()
        {
            return Err(KmsError::InvalidSignatureException {
                message: "The signature is invalid.".to_owned(),
            });
        }

        Ok(DecodedSignature {
            key_id: KmsKeyId::new(key_id.to_owned()).map_err(|_| {
                KmsError::InvalidSignatureException {
                    message: "The signature is invalid.".to_owned(),
                }
            })?,
            message: BASE64_STANDARD.decode(payload).map_err(|_| {
                KmsError::InvalidSignatureException {
                    message: "The signature is invalid.".to_owned(),
                }
            })?,
            message_type: KmsMessageType::parse(Some(message_type))?,
            signing_algorithm: signing_algorithm.to_owned(),
        })
    }
}
