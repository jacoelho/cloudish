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
#[path = "common/cognito.rs"]
mod cognito;
#[path = "common/runtime.rs"]
mod runtime;
#[path = "common/sdk.rs"]
mod sdk;

use aws_sdk_cognitoidentityprovider::Client as CognitoClient;
use aws_sdk_cognitoidentityprovider::error::ProvideErrorMetadata;
use aws_sdk_cognitoidentityprovider::types::{
    AttributeType, AuthFlowType, ChallengeNameType, ExplicitAuthFlowsType,
};
use runtime::RuntimeServer;
use sdk::SdkSmokeTarget;
use std::sync::atomic::{AtomicUsize, Ordering};

static NEXT_ID: AtomicUsize = AtomicUsize::new(0);

fn unique_name(prefix: &str) -> String {
    let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}-{id}")
}

#[tokio::test]
async fn cognito_auth_round_trips_signup_password_and_oidc_metadata() {
    let runtime = RuntimeServer::spawn("sdk-cognito-auth").await;
    let target = SdkSmokeTarget::new(
        format!("http://{}", runtime.address()),
        "eu-west-2",
    );
    let config = target.load().await;
    let client = CognitoClient::new(&config);

    let pool_name = unique_name("sdk-cognito-auth-pool");
    let client_name = unique_name("sdk-cognito-auth-client");
    let username = unique_name("sdk-cognito-user");

    let created_pool = client
        .create_user_pool()
        .pool_name(&pool_name)
        .send()
        .await
        .expect("user pool should create");
    let pool_id = created_pool
        .user_pool()
        .and_then(|user_pool| user_pool.id())
        .expect("user pool should expose an id")
        .to_owned();

    let created_client = client
        .create_user_pool_client()
        .user_pool_id(&pool_id)
        .client_name(&client_name)
        .explicit_auth_flows(ExplicitAuthFlowsType::AllowUserPasswordAuth)
        .explicit_auth_flows(ExplicitAuthFlowsType::AllowAdminUserPasswordAuth)
        .explicit_auth_flows(ExplicitAuthFlowsType::AllowRefreshTokenAuth)
        .send()
        .await
        .expect("user pool client should create");
    let client_id = created_client
        .user_pool_client()
        .and_then(|user_pool_client| user_pool_client.client_id())
        .expect("user pool client should expose an id")
        .to_owned();

    client
        .sign_up()
        .client_id(&client_id)
        .username(&username)
        .password("Perm1234!")
        .user_attributes(
            AttributeType::builder()
                .name("email")
                .value("user@example.com")
                .build()
                .expect("email attribute should build"),
        )
        .send()
        .await
        .expect("sign up should succeed");
    client
        .confirm_sign_up()
        .client_id(&client_id)
        .username(&username)
        .confirmation_code("000000")
        .send()
        .await
        .expect("confirm sign up should succeed");

    let auth = client
        .initiate_auth()
        .client_id(&client_id)
        .auth_flow(AuthFlowType::UserPasswordAuth)
        .auth_parameters("USERNAME", &username)
        .auth_parameters("PASSWORD", "Perm1234!")
        .send()
        .await
        .expect("password auth should succeed");
    let authentication_result = auth
        .authentication_result()
        .expect("auth response should expose tokens");
    let access_token = authentication_result
        .access_token()
        .expect("auth response should expose an access token")
        .to_owned();
    let id_token = authentication_result
        .id_token()
        .expect("auth response should expose an id token")
        .to_owned();

    let user = client
        .get_user()
        .access_token(&access_token)
        .send()
        .await
        .expect("GetUser should succeed");
    assert_eq!(user.username(), username.as_str());

    client
        .update_user_attributes()
        .access_token(&access_token)
        .user_attributes(
            AttributeType::builder()
                .name("name")
                .value("Alice")
                .build()
                .expect("name attribute should build"),
        )
        .send()
        .await
        .expect("UpdateUserAttributes should succeed");
    client
        .change_password()
        .access_token(&access_token)
        .previous_password("Perm1234!")
        .proposed_password("Perm5678!")
        .send()
        .await
        .expect("ChangePassword should succeed");

    let updated_user = client
        .get_user()
        .access_token(&access_token)
        .send()
        .await
        .expect("GetUser should still succeed after profile updates");
    assert!(updated_user.user_attributes().iter().any(|attribute| {
        attribute.name() == "name" && attribute.value() == Some("Alice")
    }));

    let discovery: serde_json::Value = serde_json::from_str(
        &reqwest::get(format!(
            "{}/{pool_id}/.well-known/openid-configuration",
            target.endpoint_url(),
        ))
        .await
        .expect("openid configuration request should succeed")
        .text()
        .await
        .expect("openid configuration body should load"),
    )
    .expect("openid configuration should decode");
    let issuer = discovery["issuer"]
        .as_str()
        .expect("openid configuration should expose an issuer")
        .to_owned();
    let jwks: serde_json::Value = serde_json::from_str(
        &reqwest::get(
            discovery["jwks_uri"]
                .as_str()
                .expect("openid configuration should expose a jwks uri"),
        )
        .await
        .expect("jwks request should succeed")
        .text()
        .await
        .expect("jwks body should load"),
    )
    .expect("jwks should decode");

    let claims = cognito::verify_token_with_jwks(&id_token, &issuer, &jwks);
    assert_eq!(claims["token_use"], "id");
    assert_eq!(claims["cognito:username"], username);

    client
        .initiate_auth()
        .client_id(&client_id)
        .auth_flow(AuthFlowType::UserPasswordAuth)
        .auth_parameters("USERNAME", &username)
        .auth_parameters("PASSWORD", "Perm5678!")
        .send()
        .await
        .expect("updated password should authenticate");

    runtime.shutdown().await;
}

#[tokio::test]
async fn cognito_auth_handles_new_password_required_challenge() {
    let runtime = RuntimeServer::spawn("sdk-cognito-auth-challenge").await;
    let target = SdkSmokeTarget::new(
        format!("http://{}", runtime.address()),
        "eu-west-2",
    );
    let config = target.load().await;
    let client = CognitoClient::new(&config);

    let pool_name = unique_name("sdk-cognito-auth-challenge-pool");
    let client_name = unique_name("sdk-cognito-auth-challenge-client");
    let username = unique_name("sdk-challenge-user");

    let created_pool = client
        .create_user_pool()
        .pool_name(&pool_name)
        .send()
        .await
        .expect("user pool should create");
    let pool_id = created_pool
        .user_pool()
        .and_then(|user_pool| user_pool.id())
        .expect("user pool should expose an id")
        .to_owned();

    let created_client = client
        .create_user_pool_client()
        .user_pool_id(&pool_id)
        .client_name(&client_name)
        .explicit_auth_flows(ExplicitAuthFlowsType::AllowAdminUserPasswordAuth)
        .send()
        .await
        .expect("user pool client should create");
    let client_id = created_client
        .user_pool_client()
        .and_then(|user_pool_client| user_pool_client.client_id())
        .expect("user pool client should expose an id")
        .to_owned();

    client
        .admin_create_user()
        .user_pool_id(&pool_id)
        .username(&username)
        .temporary_password("Temp1234!")
        .send()
        .await
        .expect("admin user should create");

    let challenge = client
        .admin_initiate_auth()
        .user_pool_id(&pool_id)
        .client_id(&client_id)
        .auth_flow(AuthFlowType::AdminUserPasswordAuth)
        .auth_parameters("USERNAME", &username)
        .auth_parameters("PASSWORD", "Temp1234!")
        .send()
        .await
        .expect("admin auth should return a challenge");
    assert_eq!(
        challenge.challenge_name(),
        Some(&ChallengeNameType::NewPasswordRequired)
    );
    let session = challenge
        .session()
        .expect("challenge response should expose a session")
        .to_owned();

    let completed = client
        .respond_to_auth_challenge()
        .client_id(&client_id)
        .challenge_name(ChallengeNameType::NewPasswordRequired)
        .session(session)
        .challenge_responses("USERNAME", &username)
        .challenge_responses("NEW_PASSWORD", "Perm1234!")
        .send()
        .await
        .expect("challenge completion should succeed");
    assert!(
        completed
            .authentication_result()
            .and_then(|result| result.access_token())
            .is_some()
    );

    let fetched_user = client
        .admin_get_user()
        .user_pool_id(&pool_id)
        .username(&username)
        .send()
        .await
        .expect("completed challenge user should fetch");
    assert_eq!(
        fetched_user.user_status().map(|status| status.as_str()),
        Some("CONFIRMED")
    );

    runtime.shutdown().await;
}

#[tokio::test]
async fn cognito_auth_rejects_unsupported_refresh_flows_explicitly() {
    let runtime = RuntimeServer::spawn("sdk-cognito-auth-negative").await;
    let target = SdkSmokeTarget::new(
        format!("http://{}", runtime.address()),
        "eu-west-2",
    );
    let config = target.load().await;
    let client = CognitoClient::new(&config);

    let pool_name = unique_name("sdk-cognito-auth-negative-pool");
    let client_name = unique_name("sdk-cognito-auth-negative-client");

    let created_pool = client
        .create_user_pool()
        .pool_name(&pool_name)
        .send()
        .await
        .expect("user pool should create");
    let pool_id = created_pool
        .user_pool()
        .and_then(|user_pool| user_pool.id())
        .expect("user pool should expose an id")
        .to_owned();

    let created_client = client
        .create_user_pool_client()
        .user_pool_id(&pool_id)
        .client_name(&client_name)
        .explicit_auth_flows(ExplicitAuthFlowsType::AllowRefreshTokenAuth)
        .send()
        .await
        .expect("user pool client should create");
    let client_id = created_client
        .user_pool_client()
        .and_then(|user_pool_client| user_pool_client.client_id())
        .expect("user pool client should expose an id")
        .to_owned();

    let error = client
        .initiate_auth()
        .client_id(&client_id)
        .auth_flow(AuthFlowType::RefreshTokenAuth)
        .auth_parameters("REFRESH_TOKEN", "token")
        .send()
        .await
        .expect_err("refresh token auth should fail explicitly");
    assert_eq!(
        error
            .as_service_error()
            .and_then(|service_error| service_error.code()),
        Some("UnsupportedOperation")
    );

    runtime.shutdown().await;
}
