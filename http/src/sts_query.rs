pub(crate) use crate::aws_error_shape::AwsErrorShape;
use crate::query::{QueryParameters, missing_action_error};
use crate::xml::XmlBuilder;
use auth::{VerifiedRequest, decode_authorization_message};
use aws::{Arn, AwsError, AwsErrorFamily, RequestContext};
use services::{
    AssumeRoleInput, AssumeRoleWithSamlInput, AssumeRoleWithWebIdentityInput,
    CallerIdentityOutput, GetFederationTokenInput, GetSessionTokenInput,
    SessionCredentials, StsCaller, StsService,
};

const REQUEST_ID: &str = "0000000000000000";
const STS_XMLNS: &str = "https://sts.amazonaws.com/doc/2011-06-15/";

pub(crate) fn is_sts_action(action: &str) -> bool {
    matches!(
        action,
        "AssumeRole"
            | "AssumeRoleWithWebIdentity"
            | "AssumeRoleWithSAML"
            | "DecodeAuthorizationMessage"
            | "GetCallerIdentity"
            | "GetFederationToken"
            | "GetSessionToken"
    )
}

pub(crate) fn handle(
    sts: &StsService,
    body: &[u8],
    context: &RequestContext,
    verified_request: Option<&VerifiedRequest>,
) -> Result<String, AwsError> {
    let params = QueryParameters::parse(body)?;
    let Some(action) = params.action() else {
        return Err(missing_action_error());
    };
    let scope = services::IamScope::new(
        context.account_id().clone(),
        context.region().clone(),
    );
    let caller = verified_request.map(verified_caller);

    let body = match action {
        "AssumeRole" => {
            let caller =
                require_authenticated_caller(action, caller.as_ref())?;
            let assumed = sts
                .assume_role(
                    &scope,
                    caller,
                    AssumeRoleInput {
                        duration_seconds: parse_u32(
                            &params,
                            "DurationSeconds",
                        )?,
                        role_arn: parse_arn(&params, "RoleArn")?,
                        role_session_name: params
                            .required("RoleSessionName")?
                            .to_owned(),
                        tags: tags(&params),
                        transitive_tag_keys: transitive_tag_keys(&params),
                    },
                )
                .map_err(|error| error.to_aws_error())?;
            response_with_result(
                action,
                &XmlBuilder::new()
                    .raw(&credentials_xml(&assumed.credentials))
                    .raw(&assumed_role_user_xml(&assumed.assumed_role_user))
                    .elem(
                        "PackedPolicySize",
                        &assumed.packed_policy_size.to_string(),
                    )
                    .build(),
            )
        }
        "AssumeRoleWithWebIdentity" => {
            let assumed = sts
                .assume_role_with_web_identity(
                    &scope,
                    AssumeRoleWithWebIdentityInput {
                        duration_seconds: parse_u32(
                            &params,
                            "DurationSeconds",
                        )?,
                        provider_id: params
                            .optional("ProviderId")
                            .map(str::to_owned),
                        role_arn: parse_arn(&params, "RoleArn")?,
                        role_session_name: params
                            .required("RoleSessionName")?
                            .to_owned(),
                        web_identity_token: params
                            .required("WebIdentityToken")?
                            .to_owned(),
                    },
                )
                .map_err(|error| error.to_aws_error())?;
            response_with_result(
                action,
                &XmlBuilder::new()
                    .raw(&credentials_xml(&assumed.credentials))
                    .raw(&assumed_role_user_xml(&assumed.assumed_role_user))
                    .elem(
                        "PackedPolicySize",
                        &assumed.packed_policy_size.to_string(),
                    )
                    .elem("Provider", &assumed.provider)
                    .elem("Audience", &assumed.audience)
                    .elem(
                        "SubjectFromWebIdentityToken",
                        &assumed.subject_from_web_identity_token,
                    )
                    .build(),
            )
        }
        "AssumeRoleWithSAML" => {
            let assumed = sts
                .assume_role_with_saml(
                    &scope,
                    AssumeRoleWithSamlInput {
                        duration_seconds: parse_u32(
                            &params,
                            "DurationSeconds",
                        )?,
                        principal_arn: parse_arn(&params, "PrincipalArn")?,
                        role_arn: parse_arn(&params, "RoleArn")?,
                        saml_assertion: params
                            .required("SAMLAssertion")?
                            .to_owned(),
                    },
                )
                .map_err(|error| error.to_aws_error())?;
            response_with_result(
                action,
                &XmlBuilder::new()
                    .raw(&credentials_xml(&assumed.credentials))
                    .raw(&assumed_role_user_xml(&assumed.assumed_role_user))
                    .elem(
                        "PackedPolicySize",
                        &assumed.packed_policy_size.to_string(),
                    )
                    .elem("Subject", &assumed.subject)
                    .elem("SubjectType", &assumed.subject_type)
                    .elem("Issuer", &assumed.issuer)
                    .elem("Audience", &assumed.audience)
                    .elem("NameQualifier", &assumed.name_qualifier)
                    .build(),
            )
        }
        "DecodeAuthorizationMessage" => {
            require_authenticated_caller(action, caller.as_ref())?;
            let decoded = decode_authorization_message(
                params.required("EncodedMessage")?,
            )?;
            response_with_result(
                action,
                &XmlBuilder::new().elem("DecodedMessage", &decoded).build(),
            )
        }
        "GetCallerIdentity" => response_with_result(
            action,
            &caller_identity_xml(&sts.get_caller_identity(
                &scope,
                require_authenticated_caller(action, caller.as_ref())?,
            )),
        ),
        "GetFederationToken" => {
            let caller =
                require_authenticated_caller(action, caller.as_ref())?;
            let token = sts
                .get_federation_token(
                    &scope,
                    caller,
                    GetFederationTokenInput {
                        duration_seconds: parse_u32(
                            &params,
                            "DurationSeconds",
                        )?,
                        name: params.required("Name")?.to_owned(),
                    },
                )
                .map_err(|error| error.to_aws_error())?;
            response_with_result(
                action,
                &XmlBuilder::new()
                    .raw(&credentials_xml(&token.credentials))
                    .raw(&federated_user_xml(&token.federated_user))
                    .elem(
                        "PackedPolicySize",
                        &token.packed_policy_size.to_string(),
                    )
                    .build(),
            )
        }
        "GetSessionToken" => {
            let caller =
                require_authenticated_caller(action, caller.as_ref())?;
            let token = sts
                .get_session_token(
                    &scope,
                    caller,
                    GetSessionTokenInput {
                        duration_seconds: parse_u32(
                            &params,
                            "DurationSeconds",
                        )?,
                    },
                )
                .map_err(|error| error.to_aws_error())?;
            response_with_result(action, &credentials_xml(&token))
        }
        _ => return Err(invalid_action_error(action)),
    };

    Ok(body)
}

fn require_authenticated_caller<'a>(
    _action: &str,
    caller: Option<&'a StsCaller>,
) -> Result<&'a StsCaller, AwsError> {
    caller.ok_or_else(missing_authentication_token_error)
}

fn verified_caller(verified_request: &VerifiedRequest) -> StsCaller {
    StsCaller::new(
        verified_request.account_id().clone(),
        verified_request.caller_identity().clone(),
        verified_request
            .session()
            .map(|session| {
                session
                    .session_tags()
                    .iter()
                    .map(|tag| services::IamTag {
                        key: tag.key.clone(),
                        value: tag.value.clone(),
                    })
                    .collect()
            })
            .unwrap_or_default(),
        verified_request
            .session()
            .map(|session| session.transitive_tag_keys().clone())
            .unwrap_or_default(),
    )
}

fn parse_u32(
    params: &QueryParameters,
    name: &str,
) -> Result<Option<u32>, AwsError> {
    match params.optional(name) {
        Some(value) => value.parse::<u32>().map(Some).map_err(|_| {
            AwsError::trusted_custom(
                AwsErrorFamily::Validation,
                "ValidationError",
                format!("Parameter {name} must be an unsigned integer."),
                400,
                true,
            )
        }),
        None => Ok(None),
    }
}

fn parse_arn(params: &QueryParameters, name: &str) -> Result<Arn, AwsError> {
    let value = params.required(name)?;
    value.parse::<Arn>().map_err(|_| {
        AwsError::trusted_custom(
            AwsErrorFamily::Validation,
            "ValidationError",
            format!("{value} is invalid"),
            400,
            true,
        )
    })
}

fn tags(params: &QueryParameters) -> Vec<services::IamTag> {
    let mut tags = Vec::new();

    for index in 1.. {
        let key_name = format!("Tags.member.{index}.Key");
        let Some(key) = params.optional(&key_name) else {
            break;
        };
        let value_name = format!("Tags.member.{index}.Value");
        tags.push(services::IamTag {
            key: key.to_owned(),
            value: params.optional(&value_name).unwrap_or_default().to_owned(),
        });
    }

    tags
}

fn transitive_tag_keys(params: &QueryParameters) -> Vec<String> {
    let mut keys = Vec::new();

    for index in 1.. {
        let key_name = format!("TransitiveTagKeys.member.{index}");
        let Some(key) = params.optional(&key_name) else {
            break;
        };
        keys.push(key.to_owned());
    }

    keys
}

fn response_with_result(action: &str, result: &str) -> String {
    let response_name = format!("{action}Response");
    let result_name = format!("{action}Result");

    XmlBuilder::new()
        .start(&response_name, Some(STS_XMLNS))
        .start(&result_name, None)
        .raw(result)
        .end(&result_name)
        .raw(&response_metadata_xml())
        .end(&response_name)
        .build()
}

fn response_metadata_xml() -> String {
    XmlBuilder::new()
        .start("ResponseMetadata", None)
        .elem("RequestId", REQUEST_ID)
        .end("ResponseMetadata")
        .build()
}

fn credentials_xml(credentials: &SessionCredentials) -> String {
    XmlBuilder::new()
        .start("Credentials", None)
        .elem("AccessKeyId", &credentials.access_key_id)
        .elem("SecretAccessKey", &credentials.secret_access_key)
        .elem("SessionToken", &credentials.session_token)
        .elem("Expiration", &credentials.expiration)
        .end("Credentials")
        .build()
}

fn assumed_role_user_xml(user: &services::AssumedRoleUser) -> String {
    let arn = user.arn.to_string();
    XmlBuilder::new()
        .start("AssumedRoleUser", None)
        .elem("Arn", &arn)
        .elem("AssumedRoleId", &user.assumed_role_id)
        .end("AssumedRoleUser")
        .build()
}

fn federated_user_xml(user: &services::FederatedUser) -> String {
    let arn = user.arn.to_string();
    XmlBuilder::new()
        .start("FederatedUser", None)
        .elem("FederatedUserId", &user.federated_user_id)
        .elem("Arn", &arn)
        .end("FederatedUser")
        .build()
}

fn caller_identity_xml(identity: &CallerIdentityOutput) -> String {
    let account = identity.account.to_string();
    let arn = identity.arn.to_string();
    XmlBuilder::new()
        .elem("UserId", &identity.user_id)
        .elem("Account", &account)
        .elem("Arn", &arn)
        .build()
}

fn invalid_action_error(action: &str) -> AwsError {
    AwsError::trusted_custom(
        AwsErrorFamily::Validation,
        "InvalidAction",
        format!("Unknown action {action}."),
        400,
        true,
    )
}

fn missing_authentication_token_error() -> AwsError {
    AwsError::trusted_custom(
        AwsErrorFamily::AccessDenied,
        "MissingAuthenticationToken",
        "The request must contain either a valid (registered) AWS access key ID or X.509 certificate.",
        403,
        true,
    )
}

#[cfg(test)]
mod tests {
    use super::handle;
    use auth::{SessionAttributes, VerifiedRequest};
    use aws::{
        Arn, CallerIdentity, CredentialScope, IamResourceTag, ProtocolFamily,
        RequestContext, ServiceName,
    };
    use services::{CreateRoleInput, IamScope, IamService, StsService};
    use std::collections::BTreeSet;
    use std::sync::Arc;
    use std::time::{Duration, UNIX_EPOCH};

    fn sts() -> (IamService, StsService) {
        let iam = IamService::new();
        iam.create_role(
            &IamScope::new(
                "000000000000".parse().expect("account should parse"),
                "eu-west-2".parse().expect("region should parse"),
            ),
            CreateRoleInput {
                assume_role_policy_document: r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"000000000000"},"Action":["sts:AssumeRole","sts:TagSession"]}]}"#.to_owned(),
                description: "demo".to_owned(),
                max_session_duration: 3_600,
                path: "/".to_owned(),
                role_name: "demo".to_owned(),
                tags: Vec::new(),
            },
        )
        .expect("role should create");
        let sts = StsService::with_time_source(
            iam.clone(),
            Arc::new(|| UNIX_EPOCH + Duration::from_secs(1_742_905_600)),
        );

        (iam, sts)
    }

    fn context(
        action: &str,
        caller_identity: Option<CallerIdentity>,
    ) -> RequestContext {
        RequestContext::try_new(
            "000000000000".parse().expect("account should parse"),
            "eu-west-2".parse().expect("region should parse"),
            ServiceName::Sts,
            ProtocolFamily::Query,
            action,
            caller_identity,
            true,
        )
        .expect("context should build")
    }

    fn root_verified_request() -> (CallerIdentity, VerifiedRequest) {
        let caller_identity = CallerIdentity::try_new(
            "arn:aws:iam::000000000000:root"
                .parse::<Arn>()
                .expect("root ARN should parse"),
            "000000000000",
        )
        .expect("caller identity should build");
        let verified_request = VerifiedRequest::new(
            "000000000000".parse().expect("account should parse"),
            caller_identity.clone(),
            CredentialScope::new(
                "eu-west-2".parse().expect("region should parse"),
                ServiceName::Sts,
            ),
            None,
        );

        (caller_identity, verified_request)
    }

    #[test]
    fn sts_query_assume_role_serializes_the_query_response() {
        let (_, sts) = sts();
        let (caller_identity, verified_request) = root_verified_request();

        let response = handle(
            &sts,
            b"Action=AssumeRole&RoleArn=arn%3Aaws%3Aiam%3A%3A000000000000%3Arole%2Fdemo&RoleSessionName=demo-session&Tags.member.1.Key=env&Tags.member.1.Value=dev&TransitiveTagKeys.member.1=env",
            &context("AssumeRole", Some(caller_identity)),
            Some(&verified_request),
        )
        .expect("assume role should succeed");

        assert!(response.contains("<AssumeRoleResponse xmlns=\"https://sts.amazonaws.com/doc/2011-06-15/\">"));
        assert!(response.contains("<AssumedRoleUser>"));
        assert!(response.contains("<Credentials>"));
        assert!(response.contains("<PackedPolicySize>0</PackedPolicySize>"));
    }

    #[test]
    fn sts_query_get_caller_identity_uses_verified_principal() {
        let (_, sts) = sts();
        let caller_identity = CallerIdentity::try_new(
            "arn:aws:sts::123456789012:assumed-role/demo/session"
                .parse::<Arn>()
                .expect("STS ARN should parse"),
            "AROA1234567890EXAMPLE:session",
        )
        .expect("caller identity should build");
        let verified_request = VerifiedRequest::new(
            "123456789012".parse().expect("account should parse"),
            caller_identity.clone(),
            CredentialScope::new(
                "eu-west-2".parse().expect("region should parse"),
                ServiceName::Sts,
            ),
            Some(SessionAttributes::new(
                vec![IamResourceTag {
                    key: "env".to_owned(),
                    value: "dev".to_owned(),
                }],
                BTreeSet::from(["env".to_owned()]),
            )),
        );
        let context = RequestContext::try_new(
            "123456789012".parse().expect("account should parse"),
            "eu-west-2".parse().expect("region should parse"),
            ServiceName::Sts,
            ProtocolFamily::Query,
            "GetCallerIdentity",
            Some(caller_identity),
            true,
        )
        .expect("context should build");

        let response = handle(
            &sts,
            b"Action=GetCallerIdentity",
            &context,
            Some(&verified_request),
        )
        .expect("caller identity should succeed");

        assert!(response.contains("<Account>123456789012</Account>"));
        assert!(response.contains(
            "<Arn>arn:aws:sts::123456789012:assumed-role/demo/session</Arn>"
        ));
    }

    #[test]
    fn sts_query_rejects_invalid_role_arn_at_the_adapter_boundary() {
        let (_, sts) = sts();
        let (caller_identity, verified_request) = root_verified_request();
        let error = handle(
            &sts,
            b"Action=AssumeRole&RoleArn=not-an-arn&RoleSessionName=demo-session",
            &context("AssumeRole", Some(caller_identity)),
            Some(&verified_request),
        )
        .expect_err("invalid role ARN should fail before reaching STS");

        assert_eq!(error.code(), "ValidationError");
        assert_eq!(error.message(), "not-an-arn is invalid");
    }

    #[test]
    fn sts_query_decode_authorization_message_decodes_base64_payloads() {
        let (_, sts) = sts();
        let (caller_identity, verified_request) = root_verified_request();

        let response = handle(
            &sts,
            b"Action=DecodeAuthorizationMessage&EncodedMessage=eyJhbGxvd2VkIjpmYWxzZX0%3D",
            &context("DecodeAuthorizationMessage", Some(caller_identity)),
            Some(&verified_request),
        )
        .expect("decode authorization message should succeed");

        assert!(response.contains(
            "<DecodedMessage>{&quot;allowed&quot;:false}</DecodedMessage>"
        ));
    }

    #[test]
    fn sts_query_requires_authenticated_caller_for_caller_bound_actions() {
        let (_, sts) = sts();

        for (action, body) in [
            (
                "AssumeRole",
                "Action=AssumeRole&RoleArn=arn%3Aaws%3Aiam%3A%3A000000000000%3Arole%2Fdemo&RoleSessionName=demo-session",
            ),
            ("GetCallerIdentity", "Action=GetCallerIdentity"),
            ("GetSessionToken", "Action=GetSessionToken"),
            (
                "GetFederationToken",
                "Action=GetFederationToken&Name=federated-user",
            ),
            (
                "DecodeAuthorizationMessage",
                "Action=DecodeAuthorizationMessage&EncodedMessage=eyJhbGxvd2VkIjpmYWxzZX0%3D",
            ),
        ] {
            let error =
                handle(&sts, body.as_bytes(), &context(action, None), None)
                    .expect_err(
                        "unsigned caller-bound STS actions should fail",
                    );

            assert_eq!(error.code(), "MissingAuthenticationToken");
            assert_eq!(error.status_code(), 403);
            assert_eq!(
                error.message(),
                "The request must contain either a valid (registered) AWS access key ID or X.509 certificate."
            );
        }
    }
}
