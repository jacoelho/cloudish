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

use aws_sdk_secretsmanager::Client as SecretsManagerClient;
use aws_sdk_secretsmanager::error::ProvideErrorMetadata;
use aws_sdk_secretsmanager::types::{RotationRulesType, Tag};
use runtime::SharedRuntimeLease;
use sdk::SdkSmokeTarget;

static SHARED_RUNTIME: runtime::SharedRuntime =
    runtime::SharedRuntime::new("secrets_manager_lifecycle");

async fn shared_runtime() -> SharedRuntimeLease<'static> {
    SHARED_RUNTIME.acquire().await
}

use std::sync::atomic::{AtomicUsize, Ordering};

static NEXT_ID: AtomicUsize = AtomicUsize::new(0);

fn unique_name() -> String {
    let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    format!("sdk/secrets/{id}")
}

#[tokio::test]
async fn secrets_manager_lifecycle_versions_tags_and_delete_round_trip() {
    let runtime = shared_runtime().await;
    let target = SdkSmokeTarget::new(
        format!("http://{}", runtime.address()),
        "eu-west-2",
    );
    let config = target.load().await;
    let client = SecretsManagerClient::new(&config);

    let name = unique_name();
    let lambda_arn =
        "arn:aws:lambda:eu-west-2:000000000000:function:rotate-sdk";

    let created = client
        .create_secret()
        .name(&name)
        .description("initial")
        .secret_string("alpha")
        .tags(Tag::builder().key("env").value("dev").build())
        .send()
        .await
        .expect("secret should create");
    assert_eq!(created.name(), Some(name.as_str()));

    let current = client
        .get_secret_value()
        .secret_id(&name)
        .send()
        .await
        .expect("current version should load");
    assert_eq!(current.secret_string(), Some("alpha"));

    client
        .put_secret_value()
        .secret_id(&name)
        .secret_string("beta")
        .set_version_stages(Some(vec!["AWSPENDING".to_owned()]))
        .send()
        .await
        .expect("pending version should create");
    let pending = client
        .get_secret_value()
        .secret_id(&name)
        .version_stage("AWSPENDING")
        .send()
        .await
        .expect("pending version should load");
    assert_eq!(pending.secret_string(), Some("beta"));

    let updated = client
        .update_secret()
        .secret_id(&name)
        .description("updated")
        .secret_string("gamma")
        .send()
        .await
        .expect("secret should update");
    assert!(updated.version_id().is_some());

    client
        .tag_resource()
        .secret_id(&name)
        .tags(Tag::builder().key("tier").value("backend").build())
        .send()
        .await
        .expect("tag should attach");
    client
        .untag_resource()
        .secret_id(&name)
        .tag_keys("env")
        .send()
        .await
        .expect("tag should remove");

    let rotated = client
        .rotate_secret()
        .secret_id(&name)
        .rotation_lambda_arn(lambda_arn)
        .rotation_rules(
            RotationRulesType::builder().automatically_after_days(7).build(),
        )
        .send()
        .await
        .expect("rotation metadata should update");
    assert_eq!(rotated.name(), Some(name.as_str()));

    let described = client
        .describe_secret()
        .secret_id(&name)
        .send()
        .await
        .expect("secret should describe");
    assert_eq!(described.description(), Some("updated"));
    assert_eq!(described.rotation_enabled(), Some(true));
    assert_eq!(described.rotation_lambda_arn(), Some(lambda_arn));
    assert!(described.tags().iter().any(|tag| {
        tag.key() == Some("tier") && tag.value() == Some("backend")
    }));
    assert!(described.tags().iter().all(|tag| tag.key() != Some("env")));

    let listed =
        client.list_secrets().send().await.expect("secrets should list");
    assert!(
        listed
            .secret_list()
            .iter()
            .any(|secret| secret.name() == Some(name.as_str()))
    );

    let versions = client
        .list_secret_version_ids()
        .secret_id(&name)
        .send()
        .await
        .expect("versions should list");
    assert!(versions.versions().len() >= 3);
    assert!(versions.versions().iter().any(|version| {
        version.version_stages().iter().any(|stage| stage == "AWSPENDING")
    }));

    let deleted = client
        .delete_secret()
        .secret_id(&name)
        .recovery_window_in_days(7)
        .send()
        .await
        .expect("secret should delete");
    assert!(deleted.deletion_date().is_some());

    let described_deleted = client
        .describe_secret()
        .secret_id(&name)
        .send()
        .await
        .expect("deleted secret should still describe");
    assert!(described_deleted.deleted_date().is_some());

    assert!(runtime.state_directory().exists());
}

#[tokio::test]
async fn secrets_manager_reports_explicit_lifecycle_errors() {
    let runtime = shared_runtime().await;
    let target = SdkSmokeTarget::new(
        format!("http://{}", runtime.address()),
        "eu-west-2",
    );
    let config = target.load().await;
    let client = SecretsManagerClient::new(&config);

    let name = unique_name();

    client
        .create_secret()
        .name(&name)
        .secret_string("alpha")
        .send()
        .await
        .expect("seed secret should create");

    let duplicate = client
        .create_secret()
        .name(&name)
        .secret_string("beta")
        .send()
        .await
        .expect_err("duplicate names should fail");
    assert_eq!(duplicate.code(), Some("ResourceExistsException"));

    let missing_version = client
        .get_secret_value()
        .secret_id(&name)
        .version_id("ffffffffffffffffffffffffffffffff")
        .send()
        .await
        .expect_err("missing versions should fail");
    assert_eq!(missing_version.code(), Some("ResourceNotFoundException"));

    client
        .delete_secret()
        .secret_id(&name)
        .recovery_window_in_days(7)
        .send()
        .await
        .expect("secret should delete");

    let deleted_write = client
        .put_secret_value()
        .secret_id(&name)
        .secret_string("gamma")
        .send()
        .await
        .expect_err("writes to deleted secrets should fail");
    assert_eq!(deleted_write.code(), Some("InvalidRequestException"));

    assert!(runtime.state_directory().exists());
}
