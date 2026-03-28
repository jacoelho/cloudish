use crate::{
    StsError,
    credentials::{FederatedUser, SessionCredentials},
};
use aws::{AccountId, Arn, ArnResource, Partition, ServiceName};
use base64::{
    Engine as _,
    engine::general_purpose::{STANDARD, URL_SAFE, URL_SAFE_NO_PAD},
};
use serde_json::Value;
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssumeRoleWithWebIdentityInput {
    pub duration_seconds: Option<u32>,
    pub provider_id: Option<String>,
    pub role_arn: Arn,
    pub role_session_name: String,
    pub web_identity_token: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssumeRoleWithWebIdentityOutput {
    pub assumed_role_user: crate::AssumedRoleUser,
    pub audience: String,
    pub credentials: SessionCredentials,
    pub packed_policy_size: u32,
    pub provider: String,
    pub subject_from_web_identity_token: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssumeRoleWithSamlInput {
    pub duration_seconds: Option<u32>,
    pub principal_arn: Arn,
    pub role_arn: Arn,
    pub saml_assertion: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssumeRoleWithSamlOutput {
    pub assumed_role_user: crate::AssumedRoleUser,
    pub audience: String,
    pub credentials: SessionCredentials,
    pub issuer: String,
    pub name_qualifier: String,
    pub packed_policy_size: u32,
    pub subject: String,
    pub subject_type: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetSessionTokenInput {
    pub duration_seconds: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetFederationTokenInput {
    pub duration_seconds: Option<u32>,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetFederationTokenOutput {
    pub credentials: SessionCredentials,
    pub federated_user: FederatedUser,
    pub packed_policy_size: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WebIdentityContext {
    audience: String,
    condition_values: BTreeMap<String, Vec<String>>,
    provider_output: String,
    trusted_federated_principals: Vec<String>,
    subject: String,
}

impl WebIdentityContext {
    pub(crate) fn audience(&self) -> &str {
        &self.audience
    }

    pub(crate) fn condition_values(&self) -> &BTreeMap<String, Vec<String>> {
        &self.condition_values
    }

    pub(crate) fn provider_output(&self) -> &str {
        &self.provider_output
    }

    pub(crate) fn subject(&self) -> &str {
        &self.subject
    }

    pub(crate) fn trusted_federated_principals(&self) -> &[String] {
        &self.trusted_federated_principals
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SamlContext {
    audience: String,
    condition_values: BTreeMap<String, Vec<String>>,
    issuer: String,
    name_qualifier: String,
    principal_arn: Arn,
    role_session_name: String,
    subject: String,
    subject_type: String,
}

impl SamlContext {
    pub(crate) fn audience(&self) -> &str {
        &self.audience
    }

    pub(crate) fn condition_values(&self) -> &BTreeMap<String, Vec<String>> {
        &self.condition_values
    }

    pub(crate) fn issuer(&self) -> &str {
        &self.issuer
    }

    pub(crate) fn name_qualifier(&self) -> &str {
        &self.name_qualifier
    }

    pub(crate) fn principal_arn(&self) -> &Arn {
        &self.principal_arn
    }

    pub(crate) fn role_session_name(&self) -> &str {
        &self.role_session_name
    }

    pub(crate) fn subject(&self) -> &str {
        &self.subject
    }

    pub(crate) fn subject_type(&self) -> &str {
        &self.subject_type
    }
}

pub(crate) struct FederatedUserIdentity {
    pub(crate) user: FederatedUser,
    pub(crate) principal_id: String,
}

pub(crate) fn normalize_web_identity(
    input: &AssumeRoleWithWebIdentityInput,
    role_account_id: &AccountId,
) -> Result<WebIdentityContext, StsError> {
    if let Some(claims) = parse_jwt_claims(&input.web_identity_token)? {
        let issuer = claims
            .get("iss")
            .and_then(|values| values.first())
            .ok_or_else(|| StsError::Validation {
                message:
                    "The web identity token must contain an issuer claim."
                        .to_owned(),
            })?;
        let provider_key = oidc_provider_key(issuer)?;
        let provider_arn = format!(
            "arn:aws:iam::{role_account_id}:oidc-provider/{provider_key}"
        );
        let audience = claims
            .get("aud")
            .and_then(|values| values.first())
            .cloned()
            .ok_or_else(|| StsError::Validation {
                message:
                    "The web identity token must contain an audience claim."
                        .to_owned(),
            })?;
        let subject = claims
            .get("sub")
            .and_then(|values| values.first())
            .cloned()
            .ok_or_else(|| StsError::Validation {
                message:
                    "The web identity token must contain a subject claim."
                        .to_owned(),
            })?;
        let condition_values = oidc_condition_values(&provider_key, &claims);

        return Ok(WebIdentityContext {
            audience,
            condition_values,
            provider_output: issuer.clone(),
            trusted_federated_principals: vec![provider_key, provider_arn],
            subject,
        });
    }

    let provider_id = input
        .provider_id
        .as_deref()
        .map(str::trim)
        .filter(|provider| !provider.is_empty())
        .ok_or_else(|| StsError::Validation {
            message: "An OpenID Connect identity token must include an issuer claim, or an OAuth provider must be supplied with ProviderId."
                .to_owned(),
        })?;

    Ok(WebIdentityContext {
        audience: "sts.amazonaws.com".to_owned(),
        condition_values: BTreeMap::new(),
        provider_output: provider_id.to_owned(),
        trusted_federated_principals: vec![provider_id.to_owned()],
        subject: "web-identity-subject".to_owned(),
    })
}

pub(crate) fn normalize_saml(
    input: &AssumeRoleWithSamlInput,
) -> Result<SamlContext, StsError> {
    let decoded = decode_saml_assertion(&input.saml_assertion)?;
    let role_session_name = extract_attribute_value(
        &decoded,
        "https://aws.amazon.com/SAML/Attributes/RoleSessionName",
    )
    .unwrap_or_else(|| "saml-session".to_owned());
    let audience = extract_tag_text(&decoded, "Audience")
        .unwrap_or_else(|| "urn:amazon:webservices".to_owned());
    let issuer = extract_tag_text(&decoded, "Issuer")
        .unwrap_or_else(|| "https://saml.example.com".to_owned());
    let name_id = extract_name_id(&decoded);
    let subject = name_id
        .as_ref()
        .map(|name_id| name_id.value.clone())
        .unwrap_or_else(|| "saml-subject".to_owned());
    let subject_type = name_id
        .as_ref()
        .map(NameId::subject_type)
        .unwrap_or_else(|| "persistent".to_owned());
    let name_qualifier = name_id
        .as_ref()
        .and_then(|name_id| name_id.name_qualifier.clone())
        .unwrap_or_else(|| "saml-qualifier".to_owned());
    let condition_values = BTreeMap::from([
        ("SAML:aud".to_owned(), vec![audience.clone()]),
        ("SAML:iss".to_owned(), vec![issuer.clone()]),
        ("SAML:sub".to_owned(), vec![subject.clone()]),
        ("SAML:sub_type".to_owned(), vec![subject_type.clone()]),
        ("SAML:namequalifier".to_owned(), vec![name_qualifier.clone()]),
    ]);

    Ok(SamlContext {
        audience,
        condition_values,
        issuer,
        name_qualifier,
        principal_arn: input.principal_arn.clone(),
        role_session_name,
        subject,
        subject_type,
    })
}

pub(crate) fn federated_user_identity(
    account_id: &AccountId,
    name: &str,
) -> Result<FederatedUserIdentity, StsError> {
    let federated_user_id = format!("{account_id}:{name}");
    let federated_user_arn = Arn::new(
        Partition::aws(),
        ServiceName::Sts,
        None,
        Some(account_id.clone()),
        ArnResource::Generic(format!("federated-user/{name}")),
    )
    .map_err(|error| StsError::Validation {
        message: format!(
            "failed to construct federated-user identity: {error}"
        ),
    })?;

    Ok(FederatedUserIdentity {
        user: FederatedUser {
            arn: federated_user_arn,
            federated_user_id: federated_user_id.clone(),
        },
        principal_id: federated_user_id,
    })
}

fn parse_jwt_claims(
    token: &str,
) -> Result<Option<BTreeMap<String, Vec<String>>>, StsError> {
    let mut segments = token.split('.');
    let _header = segments.next();
    let Some(payload_segment) = segments.next() else {
        return Ok(None);
    };
    if segments.next().is_none() {
        return Ok(None);
    }

    let decoded = decode_url_safe(payload_segment)?;
    let value = serde_json::from_slice::<Value>(&decoded).map_err(|_| {
        StsError::Validation {
            message: "The web identity token payload is not valid JSON."
                .to_owned(),
        }
    })?;
    let claims = value.as_object().ok_or_else(|| StsError::Validation {
        message: "The web identity token payload must be a JSON object."
            .to_owned(),
    })?;
    let mut extracted = BTreeMap::new();

    for (claim_name, claim_value) in claims {
        if let Some(values) = claim_values(claim_value) {
            extracted.insert(claim_name.clone(), values);
        }
    }

    Ok(Some(extracted))
}

fn decode_url_safe(value: &str) -> Result<Vec<u8>, StsError> {
    URL_SAFE_NO_PAD.decode(value).or_else(|_| URL_SAFE.decode(value)).map_err(
        |_| StsError::Validation {
            message:
                "The web identity token payload is not valid base64url data."
                    .to_owned(),
        },
    )
}

fn claim_values(value: &Value) -> Option<Vec<String>> {
    match value {
        Value::String(value) => Some(vec![value.clone()]),
        Value::Array(values) => {
            let strings = values
                .iter()
                .map(Value::as_str)
                .collect::<Option<Vec<_>>>()?;
            Some(strings.into_iter().map(str::to_owned).collect())
        }
        _ => None,
    }
}

fn oidc_provider_key(issuer: &str) -> Result<String, StsError> {
    let issuer = issuer.trim();
    let issuer = issuer
        .strip_prefix("https://")
        .or_else(|| issuer.strip_prefix("http://"))
        .ok_or_else(|| StsError::Validation {
            message:
                "The web identity token issuer must be an HTTP or HTTPS URL."
                    .to_owned(),
        })?;
    let issuer = issuer
        .split(['?', '#'])
        .next()
        .unwrap_or_default()
        .trim_end_matches('/');

    if issuer.is_empty() {
        return Err(StsError::Validation {
            message: "The web identity token issuer must include a host name."
                .to_owned(),
        });
    }

    Ok(issuer.to_owned())
}

fn oidc_condition_values(
    provider_key: &str,
    claims: &BTreeMap<String, Vec<String>>,
) -> BTreeMap<String, Vec<String>> {
    claims
        .iter()
        .map(|(name, values)| {
            (format!("{provider_key}:{name}"), values.clone())
        })
        .collect()
}

fn decode_saml_assertion(assertion: &str) -> Result<String, StsError> {
    let decoded =
        STANDARD.decode(assertion).map_err(|_| StsError::Validation {
            message: "The request must contain a valid SAML assertion."
                .to_owned(),
        })?;

    String::from_utf8(decoded).map_err(|_| StsError::Validation {
        message: "The request must contain a valid SAML assertion.".to_owned(),
    })
}

fn extract_attribute_value(
    decoded_assertion: &str,
    name: &str,
) -> Option<String> {
    let attribute_start =
        decoded_assertion.find(&format!("Name=\"{name}\""))?;
    let value_start = decoded_assertion[attribute_start..]
        .find("<AttributeValue>")?
        + attribute_start
        + "<AttributeValue>".len();
    let value_end = decoded_assertion[value_start..]
        .find("</AttributeValue>")?
        + value_start;

    Some(decoded_assertion[value_start..value_end].trim().to_owned())
}

fn extract_tag_text(decoded_assertion: &str, tag: &str) -> Option<String> {
    let start_tag = format!("<{tag}>");
    let end_tag = format!("</{tag}>");
    let value_start = decoded_assertion.find(&start_tag)? + start_tag.len();
    let value_end =
        decoded_assertion[value_start..].find(&end_tag)? + value_start;

    Some(decoded_assertion[value_start..value_end].trim().to_owned())
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct NameId {
    format: Option<String>,
    name_qualifier: Option<String>,
    value: String,
}

impl NameId {
    fn subject_type(&self) -> String {
        let Some(format) = &self.format else {
            return "persistent".to_owned();
        };
        if format.ends_with(":persistent") {
            return "persistent".to_owned();
        }
        if format.ends_with(":transient") {
            return "transient".to_owned();
        }

        format.clone()
    }
}

fn extract_name_id(decoded_assertion: &str) -> Option<NameId> {
    let start = decoded_assertion.find("<NameID")?;
    let open_end = decoded_assertion[start..].find('>')? + start;
    let close_start =
        decoded_assertion[open_end + 1..].find("</NameID>")? + open_end + 1;
    let open_tag = &decoded_assertion[start..=open_end];

    Some(NameId {
        format: extract_attribute(open_tag, "Format"),
        name_qualifier: extract_attribute(open_tag, "NameQualifier"),
        value: decoded_assertion[open_end + 1..close_start].trim().to_owned(),
    })
}

fn extract_attribute(open_tag: &str, attribute_name: &str) -> Option<String> {
    let marker = format!("{attribute_name}=\"");
    let value_start = open_tag.find(&marker)? + marker.len();
    let value_end = open_tag[value_start..].find('"')? + value_start;

    Some(open_tag[value_start..value_end].to_owned())
}

#[cfg(test)]
mod tests {
    use super::{
        AssumeRoleWithSamlInput, AssumeRoleWithWebIdentityInput,
        normalize_saml, normalize_web_identity,
    };
    use aws::{AccountId, Arn};
    use base64::{
        Engine as _,
        engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD},
    };

    fn github_jwt() -> String {
        let header = URL_SAFE_NO_PAD.encode(r#"{"alg":"none"}"#);
        let payload = URL_SAFE_NO_PAD.encode(
            r#"{"iss":"https://token.actions.githubusercontent.com","sub":"repo:cloudish:ref:refs/heads/main","aud":["sts.amazonaws.com"],"amr":["authenticated"]}"#,
        );
        format!("{header}.{payload}.signature")
    }

    fn saml_assertion() -> String {
        STANDARD.encode(
            r#"<Assertion><Issuer>https://saml.example.com</Issuer><Audience>urn:amazon:webservices</Audience><Subject><NameID Format="urn:oasis:names:tc:SAML:2.0:nameid-format:persistent" NameQualifier="saml-qualifier">saml-subject</NameID></Subject><Attribute Name="https://aws.amazon.com/SAML/Attributes/RoleSessionName"><AttributeValue>saml-session</AttributeValue></Attribute></Assertion>"#,
        )
    }

    #[test]
    fn normalize_web_identity_derives_oidc_provider_from_issuer() {
        let context = normalize_web_identity(
            &AssumeRoleWithWebIdentityInput {
                duration_seconds: None,
                provider_id: None,
                role_arn: "arn:aws:iam::000000000000:role/demo"
                    .parse::<Arn>()
                    .expect("role ARN should parse"),
                role_session_name: "web-session".to_owned(),
                web_identity_token: github_jwt(),
            },
            &"000000000000"
                .parse::<AccountId>()
                .expect("account id should parse"),
        )
        .expect("OIDC token should normalize");

        assert_eq!(
            context.provider_output(),
            "https://token.actions.githubusercontent.com"
        );
        assert_eq!(
            context.trusted_federated_principals(),
            &[
                "token.actions.githubusercontent.com".to_owned(),
                "arn:aws:iam::000000000000:oidc-provider/token.actions.githubusercontent.com".to_owned(),
            ]
        );
        assert_eq!(
            context
                .condition_values()
                .get("token.actions.githubusercontent.com:sub"),
            Some(&vec!["repo:cloudish:ref:refs/heads/main".to_owned()])
        );
    }

    #[test]
    fn normalize_web_identity_requires_provider_or_oidc_issuer() {
        let error = normalize_web_identity(
            &AssumeRoleWithWebIdentityInput {
                duration_seconds: None,
                provider_id: None,
                role_arn: "arn:aws:iam::000000000000:role/demo"
                    .parse::<Arn>()
                    .expect("role ARN should parse"),
                role_session_name: "web-session".to_owned(),
                web_identity_token: "opaque-token".to_owned(),
            },
            &"000000000000"
                .parse::<AccountId>()
                .expect("account id should parse"),
        )
        .expect_err("opaque tokens require an explicit provider");

        assert!(matches!(error, crate::StsError::Validation { .. }));
    }

    #[test]
    fn normalize_saml_extracts_trust_condition_values() {
        let context = normalize_saml(&AssumeRoleWithSamlInput {
            duration_seconds: None,
            principal_arn: "arn:aws:iam::000000000000:saml-provider/example"
                .parse::<Arn>()
                .expect("principal ARN should parse"),
            role_arn: "arn:aws:iam::000000000000:role/demo"
                .parse::<Arn>()
                .expect("role ARN should parse"),
            saml_assertion: saml_assertion(),
        })
        .expect("SAML assertion should normalize");

        assert_eq!(context.role_session_name(), "saml-session");
        assert_eq!(context.subject(), "saml-subject");
        assert_eq!(
            context.condition_values().get("SAML:namequalifier"),
            Some(&vec!["saml-qualifier".to_owned()])
        );
    }
}
