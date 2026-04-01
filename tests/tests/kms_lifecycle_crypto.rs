#![allow(
    clippy::unreachable,
    clippy::assertions_on_constants,
    clippy::missing_panics_doc,
    clippy::missing_errors_doc,
    clippy::expect_used,
    clippy::panic
)]
use tests::common::runtime;
use tests::common::sdk;

use aws_sdk_kms::Client as KmsClient;
use aws_sdk_kms::error::ProvideErrorMetadata;
use aws_sdk_kms::primitives::Blob;
use aws_sdk_kms::types::{
    DataKeySpec, KeySpec, KeyUsageType, SigningAlgorithmSpec, Tag,
};
use runtime::SharedRuntimeLease;
use sdk::SdkSmokeTarget;

static SHARED_RUNTIME: runtime::SharedRuntime =
    runtime::SharedRuntime::new("kms_lifecycle_crypto");

async fn shared_runtime() -> SharedRuntimeLease<'static> {
    SHARED_RUNTIME.acquire().await
}

use std::sync::atomic::{AtomicUsize, Ordering};

static NEXT_ID: AtomicUsize = AtomicUsize::new(0);

fn unique_alias() -> String {
    let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    format!("alias/sdk-kms-{id}")
}

#[tokio::test]
async fn kms_lifecycle_and_crypto_round_trip() {
    let runtime = shared_runtime().await;
    let target = SdkSmokeTarget::new(
        format!("http://{}", runtime.address()),
        "eu-west-2",
    );
    let config = target.load().await;
    let client = KmsClient::new(&config);
    let alias_name = unique_alias();

    let created = client
        .create_key()
        .description("sdk encrypt key")
        .send()
        .await
        .expect("encrypt key should create");
    let key_metadata =
        created.key_metadata().expect("key metadata should exist");
    let key_id = key_metadata.key_id();
    assert_eq!(key_metadata.key_usage(), Some(&KeyUsageType::EncryptDecrypt));

    let described = client
        .describe_key()
        .key_id(key_id)
        .send()
        .await
        .expect("describe key should succeed");
    assert_eq!(
        described
            .key_metadata()
            .expect("described key metadata should exist")
            .key_state()
            .map(ToString::to_string),
        Some("Enabled".to_owned())
    );

    let listed =
        client.list_keys().send().await.expect("list keys should succeed");
    assert!(
        listed.keys().iter().any(|entry| entry.key_id() == Some(key_id)),
        "listed keys should include the created key"
    );

    client
        .create_alias()
        .alias_name(&alias_name)
        .target_key_id(key_id)
        .send()
        .await
        .expect("alias should create");
    let alias_describe = client
        .describe_key()
        .key_id(&alias_name)
        .send()
        .await
        .expect("alias should resolve through describe");
    assert_eq!(
        alias_describe
            .key_metadata()
            .expect("alias describe should include key metadata")
            .key_id(),
        key_id
    );

    let encrypted = client
        .encrypt()
        .key_id(&alias_name)
        .plaintext(Blob::new(b"payload"))
        .send()
        .await
        .expect("encrypt should succeed");
    let decrypted = client
        .decrypt()
        .ciphertext_blob(
            encrypted
                .ciphertext_blob()
                .expect("ciphertext should be present")
                .clone(),
        )
        .send()
        .await
        .expect("decrypt should succeed");
    assert_eq!(
        decrypted.plaintext().expect("plaintext should be present").as_ref(),
        b"payload"
    );

    let data_key = client
        .generate_data_key()
        .key_id(key_id)
        .key_spec(DataKeySpec::Aes256)
        .send()
        .await
        .expect("data key should generate");
    let decrypted_data_key = client
        .decrypt()
        .ciphertext_blob(
            data_key
                .ciphertext_blob()
                .expect("data-key ciphertext should be present")
                .clone(),
        )
        .key_id(key_id)
        .send()
        .await
        .expect("data-key ciphertext should decrypt");
    assert_eq!(
        decrypted_data_key
            .plaintext()
            .expect("data-key plaintext should be present")
            .as_ref(),
        data_key
            .plaintext()
            .expect("generated plaintext should be present")
            .as_ref()
    );

    let without_plaintext = client
        .generate_data_key_without_plaintext()
        .key_id(key_id)
        .number_of_bytes(24)
        .send()
        .await
        .expect("data key without plaintext should generate");
    assert!(without_plaintext.ciphertext_blob().is_some());

    let sign_key = client
        .create_key()
        .description("sdk signing key")
        .key_spec(KeySpec::Rsa2048)
        .key_usage(KeyUsageType::SignVerify)
        .send()
        .await
        .expect("sign key should create");
    let sign_key_id = sign_key
        .key_metadata()
        .map(|metadata| metadata.key_id())
        .expect("sign key id should be present");
    let signature = client
        .sign()
        .key_id(sign_key_id)
        .message(Blob::new(b"message"))
        .signing_algorithm(SigningAlgorithmSpec::RsassaPssSha256)
        .send()
        .await
        .expect("sign should succeed");
    let verification = client
        .verify()
        .key_id(sign_key_id)
        .message(Blob::new(b"message"))
        .signature(
            signature
                .signature()
                .expect("signature should be present")
                .clone(),
        )
        .signing_algorithm(SigningAlgorithmSpec::RsassaPssSha256)
        .send()
        .await
        .expect("verify should succeed");
    assert!(verification.signature_valid());

    client
        .tag_resource()
        .key_id(key_id)
        .tags(
            Tag::builder()
                .tag_key("env")
                .tag_value("dev")
                .build()
                .expect("tag should build"),
        )
        .send()
        .await
        .expect("tag resource should succeed");
    let tags = client
        .list_resource_tags()
        .key_id(key_id)
        .send()
        .await
        .expect("list tags should succeed");
    assert!(
        tags.tags()
            .iter()
            .any(|tag| tag.tag_key() == "env" && tag.tag_value() == "dev")
    );
    client
        .untag_resource()
        .key_id(key_id)
        .tag_keys("env")
        .send()
        .await
        .expect("untag resource should succeed");
    let tags = client
        .list_resource_tags()
        .key_id(key_id)
        .send()
        .await
        .expect("list tags after untag should succeed");
    assert!(tags.tags().is_empty());

    client
        .schedule_key_deletion()
        .key_id(key_id)
        .pending_window_in_days(7)
        .send()
        .await
        .expect("schedule deletion should succeed");
    client
        .cancel_key_deletion()
        .key_id(key_id)
        .send()
        .await
        .expect("cancel deletion should succeed");
    let described = client
        .describe_key()
        .key_id(key_id)
        .send()
        .await
        .expect("describe after cancel should succeed");
    assert_eq!(
        described
            .key_metadata()
            .expect("described key metadata should exist")
            .key_state()
            .map(ToString::to_string),
        Some("Enabled".to_owned())
    );

    assert!(runtime.state_directory().exists());
}

#[tokio::test]
async fn kms_reports_explicit_errors() {
    let runtime = shared_runtime().await;
    let target = SdkSmokeTarget::new(
        format!("http://{}", runtime.address()),
        "eu-west-2",
    );
    let config = target.load().await;
    let client = KmsClient::new(&config);

    let created = client
        .create_key()
        .description("sdk errors key")
        .send()
        .await
        .expect("error key should create");
    let key_id = created
        .key_metadata()
        .map(|metadata| metadata.key_id())
        .expect("key id should be present");

    let invalid_alias = client
        .create_alias()
        .alias_name("demo")
        .target_key_id(key_id)
        .send()
        .await
        .expect_err("alias without prefix should fail");
    assert_eq!(invalid_alias.code(), Some("ValidationException"));

    let invalid_ciphertext = client
        .decrypt()
        .ciphertext_blob(Blob::new(b"not-a-ciphertext"))
        .send()
        .await
        .expect_err("malformed ciphertext should fail");
    assert_eq!(invalid_ciphertext.code(), Some("InvalidCiphertextException"));

    client
        .schedule_key_deletion()
        .key_id(key_id)
        .pending_window_in_days(7)
        .send()
        .await
        .expect("schedule deletion should succeed");
    let pending_encrypt = client
        .encrypt()
        .key_id(key_id)
        .plaintext(Blob::new(b"payload"))
        .send()
        .await
        .expect_err("pending deletion key should reject encrypt");
    assert_eq!(pending_encrypt.code(), Some("KMSInvalidStateException"));

    let sign_key = client
        .create_key()
        .description("sdk error sign key")
        .key_spec(KeySpec::Rsa2048)
        .key_usage(KeyUsageType::SignVerify)
        .send()
        .await
        .expect("sign key should create");
    let sign_key_id = sign_key
        .key_metadata()
        .map(|metadata| metadata.key_id())
        .expect("sign key id should be present");
    let signature = client
        .sign()
        .key_id(sign_key_id)
        .message(Blob::new(b"message"))
        .signing_algorithm(SigningAlgorithmSpec::RsassaPssSha256)
        .send()
        .await
        .expect("sign should succeed");
    let wrong_signature = client
        .verify()
        .key_id(sign_key_id)
        .message(Blob::new(b"other"))
        .signature(
            signature
                .signature()
                .expect("signature should be present")
                .clone(),
        )
        .signing_algorithm(SigningAlgorithmSpec::RsassaPssSha256)
        .send()
        .await
        .expect_err("wrong signature should fail");
    assert_eq!(wrong_signature.code(), Some("KMSInvalidSignatureException"));

    assert!(runtime.state_directory().exists());
}
