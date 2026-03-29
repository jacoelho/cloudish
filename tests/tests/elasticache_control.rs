#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    clippy::unreachable,
    clippy::indexing_slicing,
    clippy::assertions_on_constants,
    clippy::missing_panics_doc,
    clippy::missing_errors_doc
)]
use tests::common::elasticache;
use tests::common::runtime;
use tests::common::sdk;

use aws_sdk_elasticache::Client as ElastiCacheClient;
use aws_sdk_elasticache::error::ProvideErrorMetadata;
use aws_sdk_elasticache::types::{
    AuthenticationMode, ClusterMode, InputAuthenticationType,
};
use runtime::SharedRuntimeLease;
use sdk::SdkSmokeTarget;
use test_support::temporary_directory;

static SHARED_RUNTIME: runtime::SharedRuntime =
    runtime::SharedRuntime::new("elasticache_control");

async fn shared_runtime() -> SharedRuntimeLease<'static> {
    SHARED_RUNTIME.acquire().await
}

fn assert_auth_failure(error: &str) {
    let normalized = error.to_ascii_lowercase();

    assert!(
        normalized.contains("auth")
            || normalized.contains("password")
            || normalized.contains("invalid")
            || normalized.contains("disabled")
            || normalized.contains("terminated")
            || normalized.contains("ioerror"),
        "expected an authentication failure, got: {error}"
    );
}

#[tokio::test]
async fn user_lifecycle_and_password_proxy_round_trip() {
    let runtime = shared_runtime().await;
    let target = SdkSmokeTarget::new(
        format!("http://{}", runtime.address()),
        "eu-west-2",
    );
    let config = target.load().await;
    let client = ElastiCacheClient::new(&config);

    let replication_group_id = elasticache::unique_name("sdk-rg");
    let user_id = elasticache::unique_name("sdk-user");
    let user_name = elasticache::unique_name("sdk-alice");
    let group_password = "GroupPass123!";
    let first_password = "UserPass123!";
    let rotated_password = "RotatedPass123!";

    let created_user = client
        .create_user()
        .user_id(&user_id)
        .user_name(&user_name)
        .engine("redis")
        .authentication_mode(
            AuthenticationMode::builder()
                .r#type(InputAuthenticationType::Password)
                .passwords(first_password)
                .build(),
        )
        .send()
        .await
        .expect("user should create");
    assert_eq!(created_user.user_id(), Some(user_id.as_str()));
    assert_eq!(
        created_user
            .authentication()
            .and_then(|authentication| authentication.r#type())
            .map(|type_| type_.as_str()),
        Some("password")
    );

    let described_user = client
        .describe_users()
        .user_id(&user_id)
        .send()
        .await
        .expect("user should describe");
    assert!(
        described_user
            .users()
            .iter()
            .any(|user| user.user_name() == Some(user_name.as_str()))
    );

    let modified_user = client
        .modify_user()
        .user_id(&user_id)
        .access_string("on ~* +@read")
        .authentication_mode(
            AuthenticationMode::builder()
                .r#type(InputAuthenticationType::Password)
                .passwords(rotated_password)
                .build(),
        )
        .send()
        .await
        .expect("user should modify");
    assert_eq!(modified_user.access_string(), Some("on ~* +@read"));

    let created_group = client
        .create_replication_group()
        .replication_group_id(&replication_group_id)
        .replication_group_description("demo")
        .engine("redis")
        .auth_token(group_password)
        .send()
        .await
        .expect("replication group should create");
    let group = created_group
        .replication_group()
        .expect("create output should include a replication group");
    let endpoint = group
        .configuration_endpoint()
        .expect("replication group should expose an endpoint");
    let host = endpoint
        .address()
        .expect("endpoint should include an address")
        .to_owned();
    let port = endpoint.port().expect("endpoint should include a port") as u16;

    let group_value = elasticache::redis_set_get(
        &host,
        port,
        None,
        Some(group_password),
        "sdk-group-key",
        "sdk-group-value",
    )
    .await
    .expect("group password auth should work");
    assert_eq!(group_value, "sdk-group-value");

    let user_value = elasticache::redis_set_get(
        &host,
        port,
        Some(user_name.as_str()),
        Some(rotated_password),
        "sdk-user-key",
        "sdk-user-value",
    )
    .await
    .expect("user password auth should work");
    assert_eq!(user_value, "sdk-user-value");

    let acl_error = elasticache::redis_acl_list_error(
        &host,
        port,
        Some(user_name.as_str()),
        Some(rotated_password),
    )
    .await;
    assert!(acl_error.contains("ACL command support is not implemented"));

    let invalid_password = elasticache::redis_connect_error(
        &host,
        port,
        Some(user_name.as_str()),
        Some("wrong-password"),
    )
    .await;
    assert_auth_failure(&invalid_password);

    client
        .delete_user()
        .user_id(&user_id)
        .send()
        .await
        .expect("user should delete");
    let deleted_user_error = elasticache::redis_connect_error(
        &host,
        port,
        Some(user_name.as_str()),
        Some(rotated_password),
    )
    .await;
    assert_auth_failure(&deleted_user_error);
    client
        .delete_replication_group()
        .replication_group_id(&replication_group_id)
        .send()
        .await
        .expect("replication group should delete");
    elasticache::wait_for_port_closed(&host, port).await;
}

#[tokio::test]
async fn iam_proxy_and_topology_failures_are_explicit() {
    let runtime = shared_runtime().await;
    let target = SdkSmokeTarget::new(
        format!("http://{}", runtime.address()),
        "eu-west-2",
    );
    let config = target.load().await;
    let client = ElastiCacheClient::new(&config);

    let replication_group_id = elasticache::unique_name("sdk-iam-rg");
    let unsupported_group_id = elasticache::unique_name("sdk-rg-bad");

    let created_group = client
        .create_replication_group()
        .replication_group_id(&replication_group_id)
        .replication_group_description("iam")
        .transit_encryption_enabled(true)
        .send()
        .await
        .expect("IAM replication group should create");
    let group = created_group
        .replication_group()
        .expect("create output should include a replication group");
    let endpoint = group
        .configuration_endpoint()
        .expect("replication group should expose an endpoint");
    let host = endpoint
        .address()
        .expect("endpoint should include an address")
        .to_owned();
    let port = endpoint.port().expect("endpoint should include a port") as u16;

    let valid_token = elasticache::elasticache_iam_token(
        &replication_group_id,
        "eu-west-2",
        "iam-user",
    );
    let value = elasticache::redis_set_get(
        &host,
        port,
        None,
        Some(valid_token.as_str()),
        "sdk-iam-key",
        "sdk-iam-value",
    )
    .await
    .expect("valid IAM auth token should work");
    assert_eq!(value, "sdk-iam-value");

    let invalid_token = format!("{valid_token}broken");
    let invalid_token = elasticache::redis_connect_error(
        &host,
        port,
        None,
        Some(invalid_token.as_str()),
    )
    .await;
    assert_auth_failure(&invalid_token);
    let wrong_group_token = elasticache::elasticache_iam_token(
        "different-group",
        "eu-west-2",
        "iam-user",
    );
    let wrong_group_error = elasticache::redis_connect_error(
        &host,
        port,
        None,
        Some(wrong_group_token.as_str()),
    )
    .await;
    assert_auth_failure(&wrong_group_error);

    let topology_error = client
        .create_replication_group()
        .replication_group_id(&unsupported_group_id)
        .replication_group_description("bad")
        .cluster_mode(ClusterMode::Enabled)
        .send()
        .await
        .expect_err("unsupported topology should fail");
    assert_eq!(topology_error.code(), Some("InvalidParameterCombination"));

    client
        .delete_replication_group()
        .replication_group_id(&replication_group_id)
        .send()
        .await
        .expect("replication group should delete");
    elasticache::wait_for_port_closed(&host, port).await;
}

#[tokio::test]
async fn runtimes_restore_persisted_elasticache_groups_after_restart() {
    let state_directory =
        temporary_directory("elasticache-runtime-restore").join("state");
    let runtime = runtime::RuntimeServer::spawn_with_state_directory(
        state_directory.clone(),
    )
    .await;
    let target = SdkSmokeTarget::new(
        format!("http://{}", runtime.address()),
        "eu-west-2",
    );
    let config = target.load().await;
    let client = ElastiCacheClient::new(&config);
    let replication_group_id = elasticache::unique_name("sdk-restore-rg");
    let password = "GroupPass123!";

    let created = client
        .create_replication_group()
        .replication_group_id(&replication_group_id)
        .replication_group_description("demo")
        .engine("redis")
        .auth_token(password)
        .send()
        .await
        .expect("replication group should create");
    let endpoint = created
        .replication_group()
        .and_then(|group| group.configuration_endpoint())
        .expect("replication group should expose an endpoint");
    let host = endpoint
        .address()
        .expect("endpoint should include an address")
        .to_owned();
    let port = endpoint.port().expect("endpoint should include a port") as u16;

    assert_eq!(
        elasticache::redis_set_get(
            &host,
            port,
            None,
            Some(password),
            "sdk-restore-key",
            "sdk-restore-value",
        )
        .await
        .expect("password auth should work before restart"),
        "sdk-restore-value"
    );
    assert!(runtime.state_directory().exists());
    runtime.shutdown().await;
    elasticache::wait_for_port_closed(&host, port).await;

    let restored_runtime =
        runtime::RuntimeServer::spawn_with_state_directory(state_directory)
            .await;
    let restored_target = SdkSmokeTarget::new(
        format!("http://{}", restored_runtime.address()),
        "eu-west-2",
    );
    let restored_config = restored_target.load().await;
    let restored_client = ElastiCacheClient::new(&restored_config);
    let described = restored_client
        .describe_replication_groups()
        .replication_group_id(&replication_group_id)
        .send()
        .await
        .expect("restored replication group should describe");
    let restored_endpoint = described.replication_groups()[0]
        .configuration_endpoint()
        .expect("restored replication group should expose an endpoint");

    assert_eq!(restored_endpoint.address(), Some(host.as_str()));
    assert_eq!(restored_endpoint.port(), Some(i32::from(port)));
    assert_eq!(
        elasticache::redis_set_get(
            &host,
            port,
            None,
            Some(password),
            "sdk-restore-key",
            "sdk-restore-value-2",
        )
        .await
        .expect("password auth should work after restart"),
        "sdk-restore-value-2"
    );

    restored_client
        .delete_replication_group()
        .replication_group_id(&replication_group_id)
        .send()
        .await
        .expect("restored replication group should delete");
    elasticache::wait_for_port_closed(&host, port).await;
    restored_runtime.shutdown().await;
}
