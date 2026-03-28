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
#[path = "common/runtime.rs"]
mod runtime;
#[path = "common/sdk.rs"]
mod sdk;

use aws_sdk_iam::Client as IamClient;
use aws_sdk_sts::Client as StsClient;
use aws_sdk_sts::error::ProvideErrorMetadata;
use aws_sdk_sts::types::Tag;
use runtime::RuntimeServer;
use sdk::SdkSmokeTarget;

fn assume_role_policy(account_id: &str) -> String {
    format!(
        r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Allow","Principal":{{"AWS":"{account_id}"}},"Action":["sts:AssumeRole","sts:TagSession"]}}]}}"#
    )
}

fn saml_assertion(session_name: &str) -> String {
    assert_eq!(
        session_name, "saml-session",
        "only the seeded SAML assertion is used in smoke tests",
    );
    "PEFzc2VydGlvbj48QXR0cmlidXRlIE5hbWU9Imh0dHBzOi8vYXdzLmFtYXpvbi5jb20vU0FNTC9BdHRyaWJ1dGVzL1JvbGVTZXNzaW9uTmFtZSI+PEF0dHJpYnV0ZVZhbHVlPnNhbWwtc2Vzc2lvbjwvQXR0cmlidXRlVmFsdWU+PC9BdHRyaWJ1dGU+PC9Bc3NlcnRpb24+".to_owned()
}

fn session_target(
    base: &SdkSmokeTarget,
    credentials: &aws_sdk_sts::types::Credentials,
) -> SdkSmokeTarget {
    SdkSmokeTarget::new(base.endpoint_url(), base.region()).with_credentials(
        credentials.access_key_id(),
        credentials.secret_access_key(),
        Some(credentials.session_token()),
    )
}

#[tokio::test]
async fn assume_role_credentials_sign_downstream_iam_requests() {
    let runtime = RuntimeServer::spawn("sdk-sts-assume-role").await;
    let target = SdkSmokeTarget::new(
        format!("http://{}", runtime.address()),
        "eu-west-2",
    );
    assert_eq!(target.access_key_id(), "test");
    assert_eq!(target.secret_access_key(), "test");
    assert_eq!(target.region(), "eu-west-2");
    assert_eq!(target.endpoint_url(), format!("http://{}", runtime.address()));
    let root_config = target.load().await;
    let iam = IamClient::new(&root_config);
    let sts = StsClient::new(&root_config);

    iam.create_role()
        .role_name("delegated")
        .assume_role_policy_document(assume_role_policy("000000000000"))
        .send()
        .await
        .expect("role should be created");

    let assumed = sts
        .assume_role()
        .role_arn("arn:aws:iam::000000000000:role/delegated")
        .role_session_name("demo-session")
        .tags(
            Tag::builder()
                .key("env")
                .value("dev")
                .build()
                .expect("STS tag should build"),
        )
        .transitive_tag_keys("env")
        .send()
        .await
        .expect("assume role should succeed");

    let assumed_credentials =
        assumed.credentials().expect("assume role should return credentials");
    let assumed_user = assumed
        .assumed_role_user()
        .expect("assume role should return assumed user");

    assert!(assumed_credentials.access_key_id().starts_with("ASIA"));
    assert!(
        assumed_user.arn().contains(":assumed-role/delegated/demo-session")
    );

    let delegated_config =
        session_target(&target, assumed_credentials).load().await;
    let delegated_iam = IamClient::new(&delegated_config);
    let delegated_sts = StsClient::new(&delegated_config);

    delegated_iam
        .create_user()
        .user_name("session-user")
        .send()
        .await
        .expect("temporary credentials should sign IAM requests");

    let caller = delegated_sts
        .get_caller_identity()
        .send()
        .await
        .expect("temporary credentials should resolve caller identity");
    assert_eq!(caller.account(), Some("000000000000"));
    assert_eq!(caller.arn(), Some(assumed_user.arn()));
    assert_eq!(caller.user_id(), Some(assumed_user.assumed_role_id()));

    let invalid_config =
        SdkSmokeTarget::new(target.endpoint_url(), target.region())
            .with_credentials(
                assumed_credentials.access_key_id(),
                assumed_credentials.secret_access_key(),
                Some("tampered-session-token"),
            )
            .load()
            .await;
    let invalid_sts = StsClient::new(&invalid_config);
    let error = invalid_sts
        .get_caller_identity()
        .send()
        .await
        .expect_err("tampered session token should fail authentication");
    assert_eq!(error.code(), Some("InvalidClientTokenId"));

    assert!(runtime.state_directory().exists());
    runtime.shutdown().await;
}

#[tokio::test]
async fn root_caller_identity_and_invalid_session_names_match_sts_rules() {
    let runtime = RuntimeServer::spawn("tests-sts-query").await;
    let target = SdkSmokeTarget::new(
        format!("http://{}", runtime.address()),
        "eu-west-2",
    );
    let config = target.load().await;
    let iam = IamClient::new(&config);
    let sts = StsClient::new(&config);

    let caller = sts
        .get_caller_identity()
        .send()
        .await
        .expect("root credentials should resolve caller identity");
    assert_eq!(caller.account(), Some("000000000000"));
    assert_eq!(caller.arn(), Some("arn:aws:iam::000000000000:root"));
    assert_eq!(caller.user_id(), Some("000000000000"));

    iam.create_role()
        .role_name("app-role")
        .assume_role_policy_document(assume_role_policy("000000000000"))
        .send()
        .await
        .expect("role should be created");

    let error = sts
        .assume_role()
        .role_arn("arn:aws:iam::000000000000:role/app-role")
        .role_session_name("bad session")
        .send()
        .await
        .expect_err("invalid role session names should fail");
    assert_eq!(error.code(), Some("ValidationError"));
    assert!(error.message().is_some_and(|message| {
        message.contains("roleSessionName") && message.contains("[\\w+=,.@-]*")
    }));

    assert!(runtime.state_directory().exists());
    runtime.shutdown().await;
}

#[tokio::test]
async fn extended_query_paths_round_trip() {
    let runtime = RuntimeServer::spawn("sdk-sts-extended").await;
    let target = SdkSmokeTarget::new(
        format!("http://{}", runtime.address()),
        "eu-west-2",
    );
    let config = target.load().await;
    let iam = IamClient::new(&config);
    let sts = StsClient::new(&config);

    iam.create_role()
        .role_name("external")
        .assume_role_policy_document(assume_role_policy("000000000000"))
        .send()
        .await
        .expect("role should be created");

    let session_token = sts
        .get_session_token()
        .duration_seconds(900)
        .send()
        .await
        .expect("get session token should succeed");
    let session_credentials = session_token
        .credentials()
        .expect("session token should return credentials");
    assert!(
        session_credentials.session_token().contains("cloudish-session-token")
    );

    let federation = sts
        .get_federation_token()
        .name("federated-demo")
        .duration_seconds(900)
        .send()
        .await
        .expect("get federation token should succeed");
    assert_eq!(
        federation.federated_user().map(|user| user.arn()),
        Some("arn:aws:sts::000000000000:federated-user/federated-demo")
    );

    let web_identity = sts
        .assume_role_with_web_identity()
        .role_arn("arn:aws:iam::000000000000:role/external")
        .role_session_name("web-session")
        .provider_id("accounts.google.com")
        .web_identity_token("opaque-token")
        .send()
        .await
        .expect("assume role with web identity should succeed");
    assert_eq!(web_identity.provider(), Some("accounts.google.com"));
    assert_eq!(web_identity.audience(), Some("sts.amazonaws.com"));
    assert_eq!(
        web_identity.assumed_role_user().map(|user| user.arn()),
        Some("arn:aws:sts::000000000000:assumed-role/external/web-session")
    );

    let saml = sts
        .assume_role_with_saml()
        .role_arn("arn:aws:iam::000000000000:role/external")
        .principal_arn("arn:aws:iam::000000000000:saml-provider/example")
        .saml_assertion(saml_assertion("saml-session"))
        .send()
        .await
        .expect("assume role with SAML should succeed");
    assert_eq!(saml.audience(), Some("urn:amazon:webservices"));
    assert_eq!(saml.issuer(), Some("https://saml.example.com"));
    assert_eq!(saml.name_qualifier(), Some("saml-qualifier"));
    assert_eq!(saml.subject(), Some("saml-subject"));
    assert_eq!(
        saml.assumed_role_user().map(|user| user.arn()),
        Some("arn:aws:sts::000000000000:assumed-role/external/saml-session")
    );

    let decoded = sts
        .decode_authorization_message()
        .encoded_message("eyJyZWFzb24iOiJkZW5pZWQifQ==")
        .send()
        .await
        .expect("decode authorization message should succeed");
    assert_eq!(decoded.decoded_message(), Some(r#"{"reason":"denied"}"#));

    assert!(runtime.state_directory().exists());
    runtime.shutdown().await;
}
