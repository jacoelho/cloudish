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

use aws_sdk_cognitoidentityprovider::Client as CognitoClient;
use aws_sdk_cognitoidentityprovider::error::ProvideErrorMetadata;
use aws_sdk_cognitoidentityprovider::types::{
    AttributeType, ExplicitAuthFlowsType,
};
use runtime::SharedRuntimeLease;
use sdk::SdkSmokeTarget;

static SHARED_RUNTIME: runtime::SharedRuntime =
    runtime::SharedRuntime::new("cognito_control");

async fn shared_runtime() -> SharedRuntimeLease<'static> {
    SHARED_RUNTIME.acquire().await
}

use std::sync::atomic::{AtomicUsize, Ordering};

static NEXT_ID: AtomicUsize = AtomicUsize::new(0);

fn unique_name(prefix: &str) -> String {
    let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}-{id}")
}

#[tokio::test]
async fn cognito_control_pools_clients_and_admin_users_round_trip() {
    let runtime = shared_runtime().await;
    let target = SdkSmokeTarget::new(
        format!("http://{}", runtime.address()),
        "eu-west-2",
    );
    let config = target.load().await;
    let client = CognitoClient::new(&config);

    let pool_name = unique_name("sdk-cognito-pool");
    let updated_pool_name = unique_name("sdk-cognito-pool-updated");
    let client_name = unique_name("sdk-cognito-client");
    let updated_client_name = unique_name("sdk-cognito-client-updated");
    let username = unique_name("sdk-user");

    let created_pool = client
        .create_user_pool()
        .pool_name(&pool_name)
        .send()
        .await
        .expect("user pool should create");
    let pool_id = created_pool
        .user_pool()
        .and_then(|user_pool| user_pool.id())
        .expect("created pool should expose an id")
        .to_owned();

    let listed_pools = client
        .list_user_pools()
        .max_results(60)
        .send()
        .await
        .expect("user pools should list");
    assert!(
        listed_pools
            .user_pools()
            .iter()
            .any(|user_pool| user_pool.id() == Some(pool_id.as_str()))
    );

    client
        .update_user_pool()
        .user_pool_id(&pool_id)
        .pool_name(&updated_pool_name)
        .send()
        .await
        .expect("user pool should update");
    let described_pool = client
        .describe_user_pool()
        .user_pool_id(&pool_id)
        .send()
        .await
        .expect("user pool should describe");
    assert_eq!(
        described_pool.user_pool().and_then(|user_pool| user_pool.name()),
        Some(updated_pool_name.as_str())
    );

    let created_client = client
        .create_user_pool_client()
        .user_pool_id(&pool_id)
        .client_name(&client_name)
        .generate_secret(true)
        .explicit_auth_flows(ExplicitAuthFlowsType::AllowUserPasswordAuth)
        .explicit_auth_flows(ExplicitAuthFlowsType::AllowAdminUserPasswordAuth)
        .send()
        .await
        .expect("user pool client should create");
    let client_id = created_client
        .user_pool_client()
        .and_then(|user_pool_client| user_pool_client.client_id())
        .expect("created client should expose an id")
        .to_owned();
    assert!(
        created_client
            .user_pool_client()
            .and_then(|user_pool_client| user_pool_client.client_secret())
            .is_some()
    );

    let listed_clients = client
        .list_user_pool_clients()
        .user_pool_id(&pool_id)
        .max_results(60)
        .send()
        .await
        .expect("user pool clients should list");
    assert!(listed_clients.user_pool_clients().iter().any(
        |user_pool_client| {
            user_pool_client.client_id() == Some(client_id.as_str())
        }
    ));

    client
        .update_user_pool_client()
        .user_pool_id(&pool_id)
        .client_id(&client_id)
        .client_name(&updated_client_name)
        .explicit_auth_flows(ExplicitAuthFlowsType::AllowRefreshTokenAuth)
        .send()
        .await
        .expect("user pool client should update");
    let described_client = client
        .describe_user_pool_client()
        .user_pool_id(&pool_id)
        .client_id(&client_id)
        .send()
        .await
        .expect("user pool client should describe");
    assert_eq!(
        described_client
            .user_pool_client()
            .and_then(|user_pool_client| user_pool_client.client_name()),
        Some(updated_client_name.as_str())
    );

    client
        .admin_create_user()
        .user_pool_id(&pool_id)
        .username(&username)
        .temporary_password("Temp1234!")
        .user_attributes(
            AttributeType::builder()
                .name("email")
                .value("user@example.com")
                .build()
                .expect("email attribute should build"),
        )
        .send()
        .await
        .expect("admin user should create");
    client
        .admin_update_user_attributes()
        .user_pool_id(&pool_id)
        .username(&username)
        .user_attributes(
            AttributeType::builder()
                .name("name")
                .value("Alice")
                .build()
                .expect("name attribute should build"),
        )
        .send()
        .await
        .expect("user attributes should update");
    client
        .admin_set_user_password()
        .user_pool_id(&pool_id)
        .username(&username)
        .password("Perm1234!")
        .permanent(true)
        .send()
        .await
        .expect("user password should update");

    let fetched_user = client
        .admin_get_user()
        .user_pool_id(&pool_id)
        .username(&username)
        .send()
        .await
        .expect("user should fetch");
    assert_eq!(fetched_user.username(), username.as_str());
    assert_eq!(
        fetched_user.user_status().map(|status| status.as_str()),
        Some("CONFIRMED")
    );
    assert!(fetched_user.user_attributes().iter().any(|attribute| {
        attribute.name() == "name" && attribute.value() == Some("Alice")
    }));

    let listed_users = client
        .list_users()
        .user_pool_id(&pool_id)
        .limit(60)
        .send()
        .await
        .expect("users should list");
    assert!(
        listed_users
            .users()
            .iter()
            .any(|user| user.username() == Some(username.as_str()))
    );

    client
        .admin_delete_user()
        .user_pool_id(&pool_id)
        .username(&username)
        .send()
        .await
        .expect("user should delete");
    client
        .delete_user_pool_client()
        .user_pool_id(&pool_id)
        .client_id(&client_id)
        .send()
        .await
        .expect("client should delete");
    client
        .delete_user_pool()
        .user_pool_id(&pool_id)
        .send()
        .await
        .expect("pool should delete");

    assert!(runtime.state_directory().exists());
}

#[tokio::test]
async fn cognito_control_reports_duplicate_users_and_missing_users() {
    let runtime = shared_runtime().await;
    let target = SdkSmokeTarget::new(
        format!("http://{}", runtime.address()),
        "eu-west-2",
    );
    let config = target.load().await;
    let client = CognitoClient::new(&config);

    let pool_name = unique_name("sdk-cognito-errors-pool");
    let username = unique_name("sdk-errors-user");

    let created_pool = client
        .create_user_pool()
        .pool_name(&pool_name)
        .send()
        .await
        .expect("user pool should create");
    let pool_id = created_pool
        .user_pool()
        .and_then(|user_pool| user_pool.id())
        .expect("created pool should expose an id")
        .to_owned();

    client
        .admin_create_user()
        .user_pool_id(&pool_id)
        .username(&username)
        .temporary_password("Temp1234!")
        .send()
        .await
        .expect("seed user should create");

    let duplicate = client
        .admin_create_user()
        .user_pool_id(&pool_id)
        .username(&username)
        .temporary_password("Temp1234!")
        .send()
        .await
        .expect_err("duplicate usernames should fail");
    assert_eq!(duplicate.code(), Some("UsernameExistsException"));

    let missing = client
        .admin_get_user()
        .user_pool_id(&pool_id)
        .username("missing-user")
        .send()
        .await
        .expect_err("missing users should fail");
    assert_eq!(missing.code(), Some("UserNotFoundException"));

    assert!(runtime.state_directory().exists());
}
