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

use aws_sdk_iam::Client;
use aws_sdk_iam::error::ProvideErrorMetadata;
use aws_sdk_iam::types::Tag;
use runtime::SharedRuntimeLease;
use sdk::SdkSmokeTarget;

static SHARED_RUNTIME: runtime::SharedRuntime =
    runtime::SharedRuntime::new("iam_principals");

async fn shared_runtime() -> SharedRuntimeLease<'static> {
    SHARED_RUNTIME.acquire().await
}

fn trust_policy() -> &'static str {
    r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}"#
}

fn managed_policy_document() -> &'static str {
    r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]}"#
}

#[tokio::test]
async fn iam_principal_attachment_flow_round_trips() {
    let runtime = shared_runtime().await;
    let target = SdkSmokeTarget::new(
        format!("http://{}", runtime.address()),
        "eu-west-2",
    );
    assert_eq!(target.access_key_id(), "test");
    assert_eq!(target.secret_access_key(), "test");
    assert_eq!(target.region(), "eu-west-2");
    assert_eq!(target.endpoint_url(), format!("http://{}", runtime.address()));
    let config = target.load().await;
    let client = Client::new(&config);
    let tag = Tag::builder()
        .key("env")
        .value("dev")
        .build()
        .expect("tag should build");

    let user = client
        .create_user()
        .user_name("alice")
        .set_tags(Some(vec![tag]))
        .send()
        .await
        .expect("user should be created");
    client
        .create_group()
        .group_name("admins")
        .send()
        .await
        .expect("group should be created");
    client
        .add_user_to_group()
        .group_name("admins")
        .user_name("alice")
        .send()
        .await
        .expect("user should join the group");
    let role = client
        .create_role()
        .role_name("app-role")
        .assume_role_policy_document(trust_policy())
        .send()
        .await
        .expect("role should be created");
    let policy = client
        .create_policy()
        .policy_name("read-only")
        .policy_document(managed_policy_document())
        .send()
        .await
        .expect("policy should be created");
    let policy_arn = policy
        .policy()
        .and_then(|policy| policy.arn())
        .expect("policy ARN should be returned");
    client
        .attach_role_policy()
        .role_name("app-role")
        .policy_arn(policy_arn)
        .send()
        .await
        .expect("policy should attach to the role");

    let attached = client
        .list_attached_role_policies()
        .role_name("app-role")
        .send()
        .await
        .expect("role attachments should list");
    let groups = client
        .list_groups_for_user()
        .user_name("alice")
        .send()
        .await
        .expect("groups should list");
    let user_tags = client
        .list_user_tags()
        .user_name("alice")
        .send()
        .await
        .expect("user tags should list");

    assert_eq!(user.user().map(|user| user.user_name()), Some("alice"));
    assert_eq!(role.role().map(|role| role.role_name()), Some("app-role"));
    assert_eq!(groups.groups()[0].group_name(), "admins");
    assert_eq!(
        attached.attached_policies()[0].policy_name(),
        Some("read-only")
    );
    assert_eq!(attached.attached_policies()[0].policy_arn(), Some(policy_arn));
    assert_eq!(user_tags.tags()[0].key(), "env");
    assert_eq!(user_tags.tags()[0].value(), "dev");

    assert!(runtime.state_directory().exists());
}

#[tokio::test]
async fn iam_delete_user_with_attachment_returns_delete_conflict() {
    let runtime = shared_runtime().await;
    let target = SdkSmokeTarget::new(
        format!("http://{}", runtime.address()),
        "eu-west-2",
    );
    assert_eq!(target.access_key_id(), "test");
    assert_eq!(target.secret_access_key(), "test");
    assert_eq!(target.region(), "eu-west-2");
    assert_eq!(target.endpoint_url(), format!("http://{}", runtime.address()));
    let config = target.load().await;
    let client = Client::new(&config);

    client
        .create_user()
        .user_name("attached-user")
        .send()
        .await
        .expect("user should be created");
    let policy = client
        .create_policy()
        .policy_name("managed")
        .policy_document(managed_policy_document())
        .send()
        .await
        .expect("policy should be created");
    let policy_arn = policy
        .policy()
        .and_then(|policy| policy.arn())
        .expect("policy ARN should be returned");
    client
        .attach_user_policy()
        .user_name("attached-user")
        .policy_arn(policy_arn)
        .send()
        .await
        .expect("policy should attach");

    let error = client
        .delete_user()
        .user_name("attached-user")
        .send()
        .await
        .expect_err("delete should fail with DeleteConflict");

    assert_eq!(error.code(), Some("DeleteConflict"));
    assert!(
        error
            .message()
            .is_some_and(|message| message.contains("Cannot delete entity"))
    );

    assert!(runtime.state_directory().exists());
}
