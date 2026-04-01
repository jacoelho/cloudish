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
use aws_sdk_iam::types::{StatusType, Tag};
use runtime::SharedRuntimeLease;
use sdk::SdkSmokeTarget;

static SHARED_RUNTIME: runtime::SharedRuntime =
    runtime::SharedRuntime::new("iam_access_keys");

async fn shared_runtime() -> SharedRuntimeLease<'static> {
    SHARED_RUNTIME.acquire().await
}

fn trust_policy() -> &'static str {
    r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]}"#
}

#[tokio::test]
async fn iam_access_keys_and_instance_profiles_round_trip() {
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
    let profile_tag = Tag::builder()
        .key("env")
        .value("dev")
        .build()
        .expect("profile tag should build");

    client
        .create_user()
        .user_name("alice")
        .send()
        .await
        .expect("user should be created");
    let first_key = client
        .create_access_key()
        .user_name("alice")
        .send()
        .await
        .expect("first access key should be created");
    let second_key = client
        .create_access_key()
        .user_name("alice")
        .send()
        .await
        .expect("second access key should be created");
    let first_key_id = first_key
        .access_key()
        .map(|access_key| access_key.access_key_id())
        .expect("first access key id should be returned");
    let second_key_id = second_key
        .access_key()
        .map(|access_key| access_key.access_key_id())
        .expect("second access key id should be returned");
    client
        .update_access_key()
        .user_name("alice")
        .access_key_id(first_key_id)
        .status(StatusType::Inactive)
        .send()
        .await
        .expect("access key status should update");

    let listed_keys = client
        .list_access_keys()
        .user_name("alice")
        .send()
        .await
        .expect("access keys should list");
    assert_eq!(listed_keys.access_key_metadata().len(), 2);
    assert!(listed_keys.access_key_metadata().iter().any(|metadata| {
        metadata.access_key_id() == Some(first_key_id)
            && metadata.status().is_some_and(|status| {
                status.as_str() == StatusType::Inactive.as_str()
            })
    }));
    assert!(listed_keys.access_key_metadata().iter().any(|metadata| {
        metadata.access_key_id() == Some(second_key_id)
            && metadata
                .status()
                .is_some_and(|status| status.as_str() == "Active")
    }));

    client
        .create_role()
        .role_name("app-role")
        .assume_role_policy_document(trust_policy())
        .send()
        .await
        .expect("role should be created");
    let created_profile = client
        .create_instance_profile()
        .instance_profile_name("app-profile")
        .path("/team/")
        .tags(profile_tag)
        .send()
        .await
        .expect("instance profile should be created");
    assert!(created_profile.instance_profile().is_some_and(|profile| {
        profile.instance_profile_id().starts_with("AIPA")
    }));
    client
        .add_role_to_instance_profile()
        .instance_profile_name("app-profile")
        .role_name("app-role")
        .send()
        .await
        .expect("role should attach to the profile");

    let profile = client
        .get_instance_profile()
        .instance_profile_name("app-profile")
        .send()
        .await
        .expect("instance profile should fetch");
    let profile_tags = client
        .list_instance_profile_tags()
        .instance_profile_name("app-profile")
        .send()
        .await
        .expect("instance profile tags should list");
    let profiles_for_role = client
        .list_instance_profiles_for_role()
        .role_name("app-role")
        .send()
        .await
        .expect("instance profiles for role should list");

    assert_eq!(
        profile
            .instance_profile()
            .map(|profile| profile.instance_profile_name()),
        Some("app-profile")
    );
    assert_eq!(
        profile
            .instance_profile()
            .map(|profile| profile.roles()[0].role_name()),
        Some("app-role")
    );
    assert_eq!(profile_tags.tags()[0].key(), "env");
    assert_eq!(profile_tags.tags()[0].value(), "dev");
    assert_eq!(
        profiles_for_role.instance_profiles()[0].instance_profile_name(),
        "app-profile"
    );

    assert!(runtime.state_directory().exists());
}

#[tokio::test]
async fn iam_access_keys_limit_and_instance_profile_quota_errors_surface() {
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
        .user_name("alice")
        .send()
        .await
        .expect("user should be created");
    client
        .create_access_key()
        .user_name("alice")
        .send()
        .await
        .expect("first access key should be created");
    client
        .create_access_key()
        .user_name("alice")
        .send()
        .await
        .expect("second access key should be created");
    let access_key_limit = client
        .create_access_key()
        .user_name("alice")
        .send()
        .await
        .expect_err("third access key should fail");
    assert_eq!(access_key_limit.code(), Some("LimitExceeded"));

    client
        .create_role()
        .role_name("role-one")
        .assume_role_policy_document(trust_policy())
        .send()
        .await
        .expect("first role should be created");
    client
        .create_role()
        .role_name("role-two")
        .assume_role_policy_document(trust_policy())
        .send()
        .await
        .expect("second role should be created");
    client
        .create_instance_profile()
        .instance_profile_name("profile-a")
        .send()
        .await
        .expect("instance profile should be created");
    client
        .add_role_to_instance_profile()
        .instance_profile_name("profile-a")
        .role_name("role-one")
        .send()
        .await
        .expect("first role should attach");
    let role_limit = client
        .add_role_to_instance_profile()
        .instance_profile_name("profile-a")
        .role_name("role-two")
        .send()
        .await
        .expect_err("second role should fail");
    assert_eq!(role_limit.code(), Some("LimitExceeded"));

    assert!(runtime.state_directory().exists());
}
