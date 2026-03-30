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
use tests::common::runtime;
use tests::common::sdk;

use aws_sdk_iam::Client as IamClient;
use aws_sdk_sts::Client as StsClient;
use aws_sdk_sts::error::ProvideErrorMetadata;
use aws_sdk_sts::types::Tag;
use base64::{
    Engine as _,
    engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD},
};
use runtime::SharedRuntimeLease;
use sdk::SdkSmokeTarget;
use test_support::send_http_request;

static SHARED_RUNTIME: runtime::SharedRuntime =
    runtime::SharedRuntime::new("sts");

async fn shared_runtime() -> SharedRuntimeLease<'static> {
    SHARED_RUNTIME.acquire().await
}

fn assume_role_policy(account_id: &str) -> String {
    format!(
        r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Allow","Principal":{{"AWS":"{account_id}"}},"Action":["sts:AssumeRole","sts:TagSession"]}}]}}"#
    )
}

fn federated_role_policy(account_id: &str) -> String {
    format!(
        concat!(
            r#"{{"Version":"2012-10-17","Statement":["#,
            r#"{{"Effect":"Allow","Principal":{{"AWS":"{account_id}"}},"Action":["sts:AssumeRole","sts:TagSession"]}},"#,
            r#"{{"Effect":"Allow","Principal":{{"Federated":"arn:aws:iam::{account_id}:oidc-provider/token.actions.githubusercontent.com"}},"Action":"sts:AssumeRoleWithWebIdentity","Condition":{{"StringEquals":{{"token.actions.githubusercontent.com:aud":"sts.amazonaws.com"}},"StringLike":{{"token.actions.githubusercontent.com:sub":"repo:cloudish:*"}}}}}},"#,
            r#"{{"Effect":"Allow","Principal":{{"Federated":"arn:aws:iam::{account_id}:saml-provider/example"}},"Action":"sts:AssumeRoleWithSAML","Condition":{{"StringEquals":{{"SAML:aud":"urn:amazon:webservices"}}}}}}"#,
            r#"]}}"#
        ),
        account_id = account_id,
    )
}

fn saml_assertion(session_name: &str) -> String {
    STANDARD.encode(format!(
        r#"<Assertion><Issuer>https://saml.example.com</Issuer><Audience>urn:amazon:webservices</Audience><Subject><NameID Format="urn:oasis:names:tc:SAML:2.0:nameid-format:persistent" NameQualifier="saml-qualifier">saml-subject</NameID></Subject><Attribute Name="https://aws.amazon.com/SAML/Attributes/RoleSessionName"><AttributeValue>{session_name}</AttributeValue></Attribute></Assertion>"#
    ))
}

fn github_jwt(subject: &str) -> String {
    let header = URL_SAFE_NO_PAD.encode(r#"{"alg":"none"}"#);
    let payload = URL_SAFE_NO_PAD.encode(format!(
        r#"{{"iss":"https://token.actions.githubusercontent.com","sub":"{subject}","aud":["sts.amazonaws.com"],"amr":["authenticated"]}}"#
    ));

    format!("{header}.{payload}.signature")
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

fn split_response(response: &str) -> (&str, Vec<(&str, &str)>, &str) {
    let header_end = response
        .find("\r\n\r\n")
        .expect("response should contain header terminator");
    let headers = &response[..header_end];
    let mut lines = headers.split("\r\n");
    let status = lines.next().expect("response should contain a status line");
    let mut parsed_headers = Vec::new();

    for line in lines {
        let (name, value) =
            line.split_once(':').expect("header should contain ':'");
        parsed_headers.push((name, value.trim()));
    }

    (status, parsed_headers, &response[header_end + 4..])
}

fn header_value<'a>(
    headers: &'a [(&'a str, &'a str)],
    name: &str,
) -> Option<&'a str> {
    headers
        .iter()
        .find(|(header, _)| header.eq_ignore_ascii_case(name))
        .map(|(_, value)| *value)
}

#[tokio::test]
async fn assume_role_credentials_sign_downstream_iam_requests() {
    let runtime = shared_runtime().await;
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
}

#[tokio::test]
async fn chained_assume_role_matches_trusted_role_arns() {
    let runtime = shared_runtime().await;
    let target = SdkSmokeTarget::new(
        format!("http://{}", runtime.address()),
        "eu-west-2",
    );
    let config = target.load().await;
    let iam = IamClient::new(&config);
    let sts = StsClient::new(&config);

    iam.create_role()
        .role_name("source")
        .assume_role_policy_document(assume_role_policy("000000000000"))
        .send()
        .await
        .expect("source role should be created");
    iam.create_role()
        .role_name("destination")
        .assume_role_policy_document(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::000000000000:role/source"},"Action":"sts:AssumeRole"}]}"#,
        )
        .send()
        .await
        .expect("destination role should be created");

    let source = sts
        .assume_role()
        .role_arn("arn:aws:iam::000000000000:role/source")
        .role_session_name("build")
        .send()
        .await
        .expect("source assume role should succeed");
    let source_credentials =
        source.credentials().expect("source credentials should be returned");

    let delegated_config =
        session_target(&target, source_credentials).load().await;
    let delegated_sts = StsClient::new(&delegated_config);
    let destination = delegated_sts
        .assume_role()
        .role_arn("arn:aws:iam::000000000000:role/destination")
        .role_session_name("deploy")
        .send()
        .await
        .expect("destination assume role should succeed");

    assert_eq!(
        destination.assumed_role_user().map(|user| user.arn()),
        Some("arn:aws:sts::000000000000:assumed-role/destination/deploy")
    );
}

#[tokio::test]
async fn temporary_credentials_cannot_mint_more_session_credentials() {
    let runtime = shared_runtime().await;
    let target = SdkSmokeTarget::new(
        format!("http://{}", runtime.address()),
        "eu-west-2",
    );
    let config = target.load().await;
    let iam = IamClient::new(&config);
    let sts = StsClient::new(&config);

    iam.create_role()
        .role_name("source")
        .assume_role_policy_document(assume_role_policy("000000000000"))
        .send()
        .await
        .expect("source role should be created");
    let source = sts
        .assume_role()
        .role_arn("arn:aws:iam::000000000000:role/source")
        .role_session_name("build")
        .send()
        .await
        .expect("source assume role should succeed");
    let source_credentials =
        source.credentials().expect("source credentials should be returned");

    let delegated_config =
        session_target(&target, source_credentials).load().await;
    let delegated_sts = StsClient::new(&delegated_config);

    let session_error = delegated_sts
        .get_session_token()
        .duration_seconds(900)
        .send()
        .await
        .expect_err("temporary credentials must not mint session tokens");
    assert_eq!(session_error.code(), Some("AccessDenied"));

    let federation_error = delegated_sts
        .get_federation_token()
        .name("nested-federation")
        .duration_seconds(900)
        .send()
        .await
        .expect_err("temporary credentials must not mint federation tokens");
    assert_eq!(federation_error.code(), Some("AccessDenied"));
}

#[tokio::test]
async fn root_caller_identity_and_invalid_session_names_match_sts_rules() {
    let runtime = shared_runtime().await;
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
}

#[tokio::test]
async fn extended_query_paths_round_trip() {
    let runtime = shared_runtime().await;
    let target = SdkSmokeTarget::new(
        format!("http://{}", runtime.address()),
        "eu-west-2",
    );
    let config = target.load().await;
    let iam = IamClient::new(&config);
    let sts = StsClient::new(&config);

    iam.create_role()
        .role_name("external")
        .assume_role_policy_document(federated_role_policy("000000000000"))
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
        .web_identity_token(github_jwt("repo:cloudish:ref:refs/heads/main"))
        .send()
        .await
        .expect("assume role with web identity should succeed");
    assert_eq!(
        web_identity.provider(),
        Some("https://token.actions.githubusercontent.com")
    );
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
}

#[tokio::test]
async fn unsigned_caller_bound_actions_require_authentication() {
    let runtime = shared_runtime().await;
    let address = runtime.address();

    for body in [
        "Action=AssumeRole&RoleArn=arn%3Aaws%3Aiam%3A%3A000000000000%3Arole%2Fmissing&RoleSessionName=demo-session",
        "Action=GetCallerIdentity",
        "Action=GetSessionToken",
        "Action=GetFederationToken&Name=federated-demo",
        "Action=DecodeAuthorizationMessage&EncodedMessage=eyJyZWFzb24iOiJkZW5pZWQifQ%3D%3D",
    ] {
        let response = tokio::task::spawn_blocking(move || {
            send_http_request(
                address,
                &format!(
                    "POST / HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/x-www-form-urlencoded\r\nContent-Length: {}\r\n\r\n{body}&Version=2011-06-15",
                    body.len()
                        + "&Version=2011-06-15".len()
                ),
            )
        })
        .await
        .expect("unsigned STS request task should complete")
        .expect("unsigned STS request should return a response");

        let (status, headers, response_body) = split_response(&response);

        assert_eq!(status, "HTTP/1.1 403 Forbidden");
        assert_eq!(header_value(&headers, "content-type"), Some("text/xml"));
        assert!(
            response_body.contains("<Code>MissingAuthenticationToken</Code>")
        );
        assert!(response_body.contains(
            "<Message>The request must contain either a valid (registered) AWS access key ID or X.509 certificate.</Message>"
        ));
    }
}

#[tokio::test]
async fn get_style_sts_query_requests_route_to_sts() {
    let runtime = shared_runtime().await;
    let address = runtime.address();

    let response = tokio::task::spawn_blocking(move || {
        send_http_request(
            address,
            "GET /?Action=GetCallerIdentity&Version=2011-06-15 HTTP/1.1\r\nHost: localhost\r\n\r\n",
        )
    })
    .await
    .expect("GET STS request task should complete")
    .expect("GET STS request should return a response");

    let (status, headers, response_body) = split_response(&response);

    assert_eq!(status, "HTTP/1.1 403 Forbidden");
    assert_eq!(header_value(&headers, "content-type"), Some("text/xml"));
    assert!(response_body.contains("<Code>MissingAuthenticationToken</Code>"));
    assert!(!response_body.contains("<ListAllMyBucketsResult"));
}

#[tokio::test]
async fn federated_trust_mismatches_fail_with_access_denied() {
    let runtime = shared_runtime().await;
    let target = SdkSmokeTarget::new(
        format!("http://{}", runtime.address()),
        "eu-west-2",
    );
    let config = target.load().await;
    let iam = IamClient::new(&config);
    let sts = StsClient::new(&config);

    iam.create_role()
        .role_name("web-denied")
        .assume_role_policy_document(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":{"Federated":"arn:aws:iam::000000000000:oidc-provider/token.actions.githubusercontent.com"},"Action":"sts:AssumeRoleWithWebIdentity","Condition":{"StringLike":{"token.actions.githubusercontent.com:sub":"repo:cloudish:*"}}}]}"#,
        )
        .send()
        .await
        .expect("web-denied role should be created");

    iam.create_role()
        .role_name("saml-mismatch")
        .assume_role_policy_document(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Federated":"arn:aws:iam::000000000000:saml-provider/other"},"Action":"sts:AssumeRoleWithSAML"}]}"#,
        )
        .send()
        .await
        .expect("saml-mismatch role should be created");

    let web_error = sts
        .assume_role_with_web_identity()
        .role_arn("arn:aws:iam::000000000000:role/web-denied")
        .role_session_name("web-session")
        .web_identity_token(github_jwt("repo:cloudish:ref:refs/heads/main"))
        .send()
        .await
        .expect_err("denied web identity trust should fail");
    assert_eq!(web_error.code(), Some("AccessDenied"));

    let saml_error = sts
        .assume_role_with_saml()
        .role_arn("arn:aws:iam::000000000000:role/saml-mismatch")
        .principal_arn("arn:aws:iam::000000000000:saml-provider/example")
        .saml_assertion(saml_assertion("saml-session"))
        .send()
        .await
        .expect_err("mismatched SAML trust should fail");
    assert_eq!(saml_error.code(), Some("AccessDenied"));

    assert!(runtime.state_directory().exists());
}
