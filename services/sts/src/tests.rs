use crate::{
    AssumeRoleInput, AssumeRoleWithSamlInput, AssumeRoleWithWebIdentityInput,
    GetFederationTokenInput, GetSessionTokenInput, StsCaller, StsError,
    StsService,
};
use aws::{CallerIdentity, SessionCredentialLookup};
use iam::{CreateRoleInput, IamScope, IamService, IamTag};
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

fn time_source(
    seconds: u64,
) -> Arc<dyn Fn() -> std::time::SystemTime + Send + Sync> {
    Arc::new(move || UNIX_EPOCH + Duration::from_secs(seconds))
}

fn scope(account_id: &str) -> IamScope {
    IamScope::new(
        account_id.parse().expect("account id should parse"),
        "eu-west-2".parse().expect("region should parse"),
    )
}

fn aws_trust_policy(account_id: &str) -> String {
    format!(
        r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Allow","Principal":{{"AWS":"{account_id}"}},"Action":["sts:AssumeRole","sts:TagSession"]}}]}}"#
    )
}

fn federated_trust_policy(
    account_id: &str,
    provider: &str,
    saml_principal_arn: &str,
) -> String {
    format!(
        concat!(
            r#"{{"Version":"2012-10-17","Statement":["#,
            r#"{{"Effect":"Allow","Principal":{{"AWS":"{account_id}"}},"Action":["sts:AssumeRole","sts:TagSession"]}},"#,
            r#"{{"Effect":"Allow","Principal":{{"Federated":["{provider}","arn:aws:iam::{account_id}:oidc-provider/{provider}"]}},"#,
            r#""Action":"sts:AssumeRoleWithWebIdentity"}},"#,
            r#"{{"Effect":"Allow","Principal":{{"Federated":"{saml_principal_arn}"}},"Action":"sts:AssumeRoleWithSAML"}}"#,
            r#"]}}"#
        ),
        account_id = account_id,
        provider = provider,
        saml_principal_arn = saml_principal_arn,
    )
}

fn create_role(
    iam: &IamService,
    account_id: &str,
    role_name: &str,
    assume_role_policy_document: String,
) {
    iam.create_role(
        &scope(account_id),
        CreateRoleInput {
            assume_role_policy_document,
            description: "demo".to_owned(),
            max_session_duration: 3_600,
            path: "/".to_owned(),
            role_name: role_name.to_owned(),
            tags: Vec::new(),
        },
    )
    .expect("role should create");
}

fn root_caller(account_id: &str) -> StsCaller {
    let account_id: aws::AccountId =
        account_id.parse().expect("account id should parse");
    let identity = CallerIdentity::try_new(
        format!("arn:aws:iam::{account_id}:root")
            .parse()
            .expect("root ARN should parse"),
        account_id.to_string(),
    )
    .expect("root caller identity should build");

    StsCaller::new(account_id, identity, Vec::new(), BTreeSet::new())
}

#[test]
fn sts_assume_role_issues_temporary_credentials_and_lookup_records() {
    let iam = IamService::new();
    create_role(
        &iam,
        "000000000000",
        "demo",
        aws_trust_policy("000000000000"),
    );
    let sts = StsService::with_time_source(iam, time_source(1_742_905_600));
    let caller = root_caller("000000000000");

    let assumed = sts
        .assume_role(
            &scope("000000000000"),
            &caller,
            AssumeRoleInput {
                duration_seconds: Some(900),
                role_arn: "arn:aws:iam::000000000000:role/demo"
                    .parse()
                    .expect("role ARN should parse"),
                role_session_name: "demo-session".to_owned(),
                tags: vec![IamTag {
                    key: "env".to_owned(),
                    value: "dev".to_owned(),
                }],
                transitive_tag_keys: vec!["env".to_owned()],
            },
        )
        .expect("assume role should succeed");

    assert_eq!(
        assumed.assumed_role_user.arn.to_string(),
        "arn:aws:sts::000000000000:assumed-role/demo/demo-session"
    );
    assert_eq!(
        assumed.assumed_role_user.assumed_role_id,
        "AROA0000000000000001:demo-session"
    );
    assert!(assumed.credentials.access_key_id.starts_with("ASIA"));
    assert!(assumed.credentials.expiration.starts_with("2025-03-25T"));

    let session = sts
        .find_session_credential(&assumed.credentials.access_key_id)
        .expect("issued credentials should be resolvable");
    assert_eq!(
        session.principal_arn.to_string(),
        "arn:aws:sts::000000000000:assumed-role/demo/demo-session"
    );
    assert_eq!(session.session_tags[0].key, "env");
    assert!(session.transitive_tag_keys.contains("env"));
}

#[test]
fn sts_get_caller_identity_reflects_the_current_principal() {
    let iam = IamService::new();
    let sts = StsService::with_time_source(iam, time_source(1_742_905_600));
    let caller = StsCaller::new(
        "123456789012".parse().expect("account id should parse"),
        CallerIdentity::try_new(
            "arn:aws:sts::123456789012:assumed-role/demo/session"
                .parse()
                .expect("ARN should parse"),
            "AROA1234567890EXAMPLE:session",
        )
        .expect("caller identity should build"),
        Vec::new(),
        BTreeSet::new(),
    );

    let identity = sts.get_caller_identity(&scope("123456789012"), &caller);

    assert_eq!(identity.account.as_str(), "123456789012");
    assert_eq!(
        identity.arn.to_string(),
        "arn:aws:sts::123456789012:assumed-role/demo/session"
    );
    assert_eq!(identity.user_id, "AROA1234567890EXAMPLE:session");
}

#[test]
fn sts_assume_role_rejects_invalid_session_names() {
    let iam = IamService::new();
    create_role(
        &iam,
        "000000000000",
        "demo",
        aws_trust_policy("000000000000"),
    );
    let sts = StsService::with_time_source(iam, time_source(1_742_905_600));
    let error = sts
        .assume_role(
            &scope("000000000000"),
            &root_caller("000000000000"),
            AssumeRoleInput {
                duration_seconds: None,
                role_arn: "arn:aws:iam::000000000000:role/demo"
                    .parse()
                    .expect("role ARN should parse"),
                role_session_name: "bad:session".to_owned(),
                tags: Vec::new(),
                transitive_tag_keys: Vec::new(),
            },
        )
        .expect_err("invalid session name should fail");

    assert!(matches!(error, StsError::Validation { .. }));
    assert!(error.to_aws_error().message().contains("roleSessionName"));
}

#[test]
fn sts_assume_role_rejects_duplicate_and_transitive_tag_errors() {
    let iam = IamService::new();
    create_role(
        &iam,
        "000000000000",
        "demo",
        aws_trust_policy("000000000000"),
    );
    let sts = StsService::with_time_source(iam, time_source(1_742_905_600));
    let root = root_caller("000000000000");

    let duplicate = sts
        .assume_role(
            &scope("000000000000"),
            &root,
            AssumeRoleInput {
                duration_seconds: None,
                role_arn: "arn:aws:iam::000000000000:role/demo"
                    .parse()
                    .expect("role ARN should parse"),
                role_session_name: "session".to_owned(),
                tags: vec![
                    IamTag { key: "Env".to_owned(), value: "dev".to_owned() },
                    IamTag { key: "env".to_owned(), value: "prod".to_owned() },
                ],
                transitive_tag_keys: vec!["env".to_owned()],
            },
        )
        .expect_err("duplicate tag keys should fail");
    assert!(matches!(duplicate, StsError::InvalidParameterValue { .. }));

    let transitive = sts
        .assume_role(
            &scope("000000000000"),
            &root,
            AssumeRoleInput {
                duration_seconds: None,
                role_arn: "arn:aws:iam::000000000000:role/demo"
                    .parse()
                    .expect("role ARN should parse"),
                role_session_name: "session".to_owned(),
                tags: vec![IamTag {
                    key: "Team".to_owned(),
                    value: "platform".to_owned(),
                }],
                transitive_tag_keys: vec!["missing".to_owned()],
            },
        )
        .expect_err("missing transitive tag key should fail");
    assert!(matches!(transitive, StsError::InvalidParameterValue { .. }));
}

#[test]
fn sts_assume_role_chaining_rejects_transitive_tag_overrides() {
    let iam = IamService::new();
    create_role(
        &iam,
        "000000000000",
        "demo",
        aws_trust_policy("000000000000"),
    );
    let sts = StsService::with_time_source(iam, time_source(1_742_905_600));
    let caller = StsCaller::new(
        "000000000000".parse().expect("account should parse"),
        CallerIdentity::try_new(
            "arn:aws:sts::000000000000:assumed-role/source/session"
                .parse()
                .expect("ARN should parse"),
            "AROA0000000000000000:session",
        )
        .expect("caller identity should build"),
        vec![IamTag {
            key: "SessionTag1".to_owned(),
            value: "one".to_owned(),
        }],
        BTreeSet::from(["sessiontag1".to_owned()]),
    );

    let error = sts
        .assume_role(
            &scope("000000000000"),
            &caller,
            AssumeRoleInput {
                duration_seconds: None,
                role_arn: "arn:aws:iam::000000000000:role/demo"
                    .parse()
                    .expect("role ARN should parse"),
                role_session_name: "next-session".to_owned(),
                tags: vec![IamTag {
                    key: "sessiontag1".to_owned(),
                    value: "two".to_owned(),
                }],
                transitive_tag_keys: Vec::new(),
            },
        )
        .expect_err("transitive tag override should fail");

    assert!(matches!(error, StsError::InvalidParameterValue { .. }));
}

#[test]
fn sts_supports_web_identity_saml_session_and_federation_flows() {
    let iam = IamService::new();
    create_role(
        &iam,
        "000000000000",
        "demo",
        federated_trust_policy(
            "000000000000",
            "accounts.google.com",
            "arn:aws:iam::000000000000:saml-provider/Test",
        ),
    );
    let sts = StsService::with_time_source(iam, time_source(1_742_905_600));

    let web = sts
        .assume_role_with_web_identity(
            &scope("000000000000"),
            AssumeRoleWithWebIdentityInput {
                duration_seconds: Some(900),
                provider_id: None,
                role_arn: "arn:aws:iam::000000000000:role/demo"
                    .parse()
                    .expect("role ARN should parse"),
                role_session_name: "web-session".to_owned(),
                web_identity_token: "token".to_owned(),
            },
        )
        .expect("web identity assume role should succeed");
    assert_eq!(web.provider, "accounts.google.com");
    assert_eq!(web.audience, "sts.amazonaws.com");

    let saml = sts
        .assume_role_with_saml(
            &scope("000000000000"),
            AssumeRoleWithSamlInput {
                duration_seconds: Some(900),
                principal_arn: "arn:aws:iam::000000000000:saml-provider/Test"
                    .parse()
                    .expect("principal ARN should parse"),
                role_arn: "arn:aws:iam::000000000000:role/demo"
                    .parse()
                    .expect("role ARN should parse"),
                saml_assertion: "PHNhbWxwOlJlc3BvbnNlPjxBdHRyaWJ1dGUgTmFtZT0iaHR0cHM6Ly9hd3MuYW1hem9uLmNvbS9TQU1ML0F0dHJpYnV0ZXMvUm9sZVNlc3Npb25OYW1lIj48QXR0cmlidXRlVmFsdWU+ZmVkLXVzZXI8L0F0dHJpYnV0ZVZhbHVlPjwvQXR0cmlidXRlPjwvc2FtbHA6UmVzcG9uc2U+".to_owned(),
            },
        )
        .expect("SAML assume role should succeed");
    assert!(saml.assumed_role_user.arn.to_string().ends_with("/fed-user"));

    let session = sts
        .get_session_token(
            &scope("000000000000"),
            &root_caller("000000000000"),
            GetSessionTokenInput { duration_seconds: Some(900) },
        )
        .expect("session token should succeed");
    assert!(session.access_key_id.starts_with("ASIA"));

    let federated = sts
        .get_federation_token(
            &scope("000000000000"),
            &root_caller("000000000000"),
            GetFederationTokenInput {
                duration_seconds: Some(900),
                name: "build-user".to_owned(),
            },
        )
        .expect("federation token should succeed");
    assert_eq!(
        federated.federated_user.federated_user_id,
        "000000000000:build-user"
    );
}

#[test]
fn sts_assume_role_respects_explicit_deny_over_allow() {
    let iam = IamService::new();
    create_role(
        &iam,
        "000000000000",
        "denied",
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"000000000000"},"Action":"sts:AssumeRole"},{"Effect":"Deny","Principal":{"AWS":"arn:aws:iam::000000000000:root"},"Action":"sts:AssumeRole"}]}"#
            .to_owned(),
    );
    let sts = StsService::with_time_source(iam, time_source(1_742_905_600));

    let error = sts
        .assume_role(
            &scope("000000000000"),
            &root_caller("000000000000"),
            AssumeRoleInput {
                duration_seconds: None,
                role_arn: "arn:aws:iam::000000000000:role/denied"
                    .parse()
                    .expect("role ARN should parse"),
                role_session_name: "session".to_owned(),
                tags: Vec::new(),
                transitive_tag_keys: Vec::new(),
            },
        )
        .expect_err("explicit deny should override allow");

    assert!(matches!(error, StsError::AccessDenied { .. }));
}

#[test]
fn sts_assume_role_with_web_identity_enforces_trust_policy() {
    let iam = IamService::new();
    create_role(
        &iam,
        "000000000000",
        "web-wrong-provider",
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Federated":"login.example.com"},"Action":"sts:AssumeRoleWithWebIdentity"}]}"#
            .to_owned(),
    );
    create_role(
        &iam,
        "000000000000",
        "web-denied",
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":{"Federated":"accounts.google.com"},"Action":"sts:AssumeRoleWithWebIdentity"}]}"#
            .to_owned(),
    );
    let sts = StsService::with_time_source(iam, time_source(1_742_905_600));

    let wrong_provider = sts
        .assume_role_with_web_identity(
            &scope("000000000000"),
            AssumeRoleWithWebIdentityInput {
                duration_seconds: Some(900),
                provider_id: None,
                role_arn: "arn:aws:iam::000000000000:role/web-wrong-provider"
                    .parse()
                    .expect("role ARN should parse"),
                role_session_name: "web-session".to_owned(),
                web_identity_token: "token".to_owned(),
            },
        )
        .expect_err("mismatched web identity provider should fail");
    assert!(matches!(wrong_provider, StsError::AccessDenied { .. }));

    let denied = sts
        .assume_role_with_web_identity(
            &scope("000000000000"),
            AssumeRoleWithWebIdentityInput {
                duration_seconds: Some(900),
                provider_id: None,
                role_arn: "arn:aws:iam::000000000000:role/web-denied"
                    .parse()
                    .expect("role ARN should parse"),
                role_session_name: "web-session".to_owned(),
                web_identity_token: "token".to_owned(),
            },
        )
        .expect_err("explicitly denied web identity provider should fail");
    assert!(matches!(denied, StsError::AccessDenied { .. }));
}

#[test]
fn sts_assume_role_with_saml_enforces_trust_policy() {
    let iam = IamService::new();
    create_role(
        &iam,
        "000000000000",
        "saml-wrong-principal",
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Federated":"arn:aws:iam::000000000000:saml-provider/Other"},"Action":"sts:AssumeRoleWithSAML"}]}"#
            .to_owned(),
    );
    create_role(
        &iam,
        "000000000000",
        "saml-denied",
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":{"Federated":"arn:aws:iam::000000000000:saml-provider/Test"},"Action":"sts:AssumeRoleWithSAML"}]}"#
            .to_owned(),
    );
    let sts = StsService::with_time_source(iam, time_source(1_742_905_600));
    let principal_arn: aws::Arn =
        "arn:aws:iam::000000000000:saml-provider/Test"
            .parse()
            .expect("principal ARN should parse");

    let wrong_principal = sts
        .assume_role_with_saml(
            &scope("000000000000"),
            AssumeRoleWithSamlInput {
                duration_seconds: Some(900),
                principal_arn: principal_arn.clone(),
                role_arn: "arn:aws:iam::000000000000:role/saml-wrong-principal"
                    .parse()
                    .expect("role ARN should parse"),
                saml_assertion: "PHNhbWxwOlJlc3BvbnNlPjxBdHRyaWJ1dGUgTmFtZT0iaHR0cHM6Ly9hd3MuYW1hem9uLmNvbS9TQU1ML0F0dHJpYnV0ZXMvUm9sZVNlc3Npb25OYW1lIj48QXR0cmlidXRlVmFsdWU+ZmVkLXVzZXI8L0F0dHJpYnV0ZVZhbHVlPjwvQXR0cmlidXRlPjwvc2FtbHA6UmVzcG9uc2U+".to_owned(),
            },
        )
        .expect_err("mismatched SAML principal should fail");
    assert!(matches!(wrong_principal, StsError::AccessDenied { .. }));

    let denied = sts
        .assume_role_with_saml(
            &scope("000000000000"),
            AssumeRoleWithSamlInput {
                duration_seconds: Some(900),
                principal_arn,
                role_arn: "arn:aws:iam::000000000000:role/saml-denied"
                    .parse()
                    .expect("role ARN should parse"),
                saml_assertion: "PHNhbWxwOlJlc3BvbnNlPjxBdHRyaWJ1dGUgTmFtZT0iaHR0cHM6Ly9hd3MuYW1hem9uLmNvbS9TQU1ML0F0dHJpYnV0ZXMvUm9sZVNlc3Npb25OYW1lIj48QXR0cmlidXRlVmFsdWU+ZmVkLXVzZXI8L0F0dHJpYnV0ZVZhbHVlPjwvQXR0cmlidXRlPjwvc2FtbHA6UmVzcG9uc2U+".to_owned(),
            },
        )
        .expect_err("explicitly denied SAML principal should fail");
    assert!(matches!(denied, StsError::AccessDenied { .. }));
}
