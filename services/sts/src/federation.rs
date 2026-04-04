use crate::{
    StsError,
    credentials::{FederatedUser, SessionCredentials},
};
use aws::{AccountId, Arn, ArnResource, Partition, ServiceName};
use base64::{
    Engine as _,
    engine::general_purpose::{STANDARD, URL_SAFE, URL_SAFE_NO_PAD},
};
use quick_xml::Reader;
use quick_xml::events::{BytesStart, Event};
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
    expiration_epoch_seconds: Option<u64>,
    not_before_epoch_seconds: Option<u64>,
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

    pub(crate) fn expiration_epoch_seconds(&self) -> Option<u64> {
        self.expiration_epoch_seconds
    }

    pub(crate) fn not_before_epoch_seconds(&self) -> Option<u64> {
        self.not_before_epoch_seconds
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
            .values
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
            .values
            .get("aud")
            .and_then(|values| values.first())
            .cloned()
            .ok_or_else(|| StsError::Validation {
                message:
                    "The web identity token must contain an audience claim."
                        .to_owned(),
            })?;
        let subject = claims
            .values
            .get("sub")
            .and_then(|values| values.first())
            .cloned()
            .ok_or_else(|| StsError::Validation {
                message:
                    "The web identity token must contain a subject claim."
                        .to_owned(),
            })?;
        let condition_values =
            oidc_condition_values(&provider_key, &claims.values);

        return Ok(WebIdentityContext {
            audience,
            condition_values,
            expiration_epoch_seconds: claims.expiration_epoch_seconds,
            not_before_epoch_seconds: claims.not_before_epoch_seconds,
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
        expiration_epoch_seconds: None,
        not_before_epoch_seconds: None,
        provider_output: provider_id.to_owned(),
        trusted_federated_principals: vec![provider_id.to_owned()],
        subject: "web-identity-subject".to_owned(),
    })
}

pub(crate) fn normalize_saml(
    input: &AssumeRoleWithSamlInput,
) -> Result<SamlContext, StsError> {
    let decoded = decode_saml_assertion(&input.saml_assertion)?;
    let parsed = parse_saml_assertion(&decoded)?;
    let role_session_name =
        parsed.role_session_name.unwrap_or_else(|| "saml-session".to_owned());
    let audience =
        parsed.audience.unwrap_or_else(|| "urn:amazon:webservices".to_owned());
    let issuer =
        parsed.issuer.unwrap_or_else(|| "https://saml.example.com".to_owned());
    let name_id = parsed.name_id;
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

fn parse_jwt_claims(token: &str) -> Result<Option<ParsedJwtClaims>, StsError> {
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

    Ok(Some(ParsedJwtClaims {
        values: extracted,
        expiration_epoch_seconds: epoch_seconds_claim(claims.get("exp")),
        not_before_epoch_seconds: epoch_seconds_claim(claims.get("nbf")),
    }))
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ParsedJwtClaims {
    values: BTreeMap<String, Vec<String>>,
    expiration_epoch_seconds: Option<u64>,
    not_before_epoch_seconds: Option<u64>,
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

fn epoch_seconds_claim(value: Option<&Value>) -> Option<u64> {
    match value {
        Some(Value::Number(value)) => value.as_u64(),
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

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct ParsedSamlAssertion {
    audience: Option<String>,
    issuer: Option<String>,
    name_id: Option<NameId>,
    role_session_name: Option<String>,
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

fn parse_saml_assertion(
    decoded_assertion: &str,
) -> Result<ParsedSamlAssertion, StsError> {
    let mut reader = Reader::from_str(decoded_assertion);
    reader.config_mut().trim_text(true);
    let mut buf = Vec::new();
    let mut parsed = ParsedSamlAssertion::default();
    let mut current_attribute_name = None;
    let mut in_attribute_value = false;
    let mut in_audience = false;
    let mut in_issuer = false;
    let mut current_name_id = None;
    let mut depth = 0_usize;
    let mut saw_element = false;

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(event)) => {
                saw_element = true;
                depth = depth.saturating_add(1);
                match xml_local_name(event.name().as_ref()) {
                    b"Attribute" => {
                        current_attribute_name =
                            saml_attribute_value(&event, &reader, b"Name")?;
                    }
                    b"AttributeValue" => {
                        in_attribute_value = current_attribute_name.as_deref()
                            == Some(
                                "https://aws.amazon.com/SAML/Attributes/RoleSessionName",
                            );
                    }
                    b"Audience" => in_audience = true,
                    b"Issuer" => in_issuer = true,
                    b"NameID" => {
                        current_name_id = Some(NameId {
                            format: saml_attribute_value(
                                &event, &reader, b"Format",
                            )?,
                            name_qualifier: saml_attribute_value(
                                &event,
                                &reader,
                                b"NameQualifier",
                            )?,
                            value: String::new(),
                        });
                    }
                    _ => {}
                }
            }
            Ok(Event::End(event)) => {
                if depth == 0 {
                    return Err(invalid_saml_assertion_error());
                }
                depth = depth.saturating_sub(1);
                match xml_local_name(event.name().as_ref()) {
                    b"Attribute" => current_attribute_name = None,
                    b"AttributeValue" => in_attribute_value = false,
                    b"Audience" => in_audience = false,
                    b"Issuer" => in_issuer = false,
                    b"NameID" => {
                        if let Some(name_id) = current_name_id
                            .take()
                            .filter(|name_id| !name_id.value.is_empty())
                            && parsed.name_id.is_none()
                        {
                            parsed.name_id = Some(name_id);
                        }
                    }
                    _ => {}
                }
            }
            Ok(Event::Text(event)) => {
                let text = xml_text(event.as_ref())?;
                capture_saml_text(
                    &mut parsed,
                    current_name_id.as_mut(),
                    &text,
                    in_attribute_value,
                    in_audience,
                    in_issuer,
                );
            }
            Ok(Event::CData(event)) => {
                let text = xml_text(event.as_ref())?;
                capture_saml_text(
                    &mut parsed,
                    current_name_id.as_mut(),
                    &text,
                    in_attribute_value,
                    in_audience,
                    in_issuer,
                );
            }
            Ok(Event::Empty(_)) => saw_element = true,
            Ok(Event::Eof) => {
                return if saw_element && depth == 0 {
                    Ok(parsed)
                } else {
                    Err(invalid_saml_assertion_error())
                };
            }
            Ok(
                Event::Comment(_)
                | Event::Decl(_)
                | Event::DocType(_)
                | Event::PI(_),
            ) => {}
            Err(_) => return Err(invalid_saml_assertion_error()),
        }
        buf.clear();
    }
}

fn capture_saml_text(
    parsed: &mut ParsedSamlAssertion,
    current_name_id: Option<&mut NameId>,
    text: &str,
    in_attribute_value: bool,
    in_audience: bool,
    in_issuer: bool,
) {
    let text = text.trim();
    if text.is_empty() {
        return;
    }

    if in_attribute_value && parsed.role_session_name.is_none() {
        parsed.role_session_name = Some(text.to_owned());
    }
    if in_audience && parsed.audience.is_none() {
        parsed.audience = Some(text.to_owned());
    }
    if in_issuer && parsed.issuer.is_none() {
        parsed.issuer = Some(text.to_owned());
    }
    if let Some(name_id) = current_name_id {
        name_id.value.push_str(text);
    }
}

fn saml_attribute_value(
    event: &BytesStart<'_>,
    reader: &Reader<&[u8]>,
    attribute_name: &[u8],
) -> Result<Option<String>, StsError> {
    for attribute in event.attributes().with_checks(false) {
        let attribute =
            attribute.map_err(|_| invalid_saml_assertion_error())?;
        if xml_local_name(attribute.key.as_ref()) == attribute_name {
            return attribute
                .decode_and_unescape_value(reader.decoder())
                .map(|value| Some(value.into_owned()))
                .map_err(|_| invalid_saml_assertion_error());
        }
    }

    Ok(None)
}

fn xml_text(value: &[u8]) -> Result<String, StsError> {
    let value = std::str::from_utf8(value)
        .map_err(|_| invalid_saml_assertion_error())?;
    quick_xml::escape::unescape(value)
        .map(|value| value.into_owned())
        .map_err(|_| invalid_saml_assertion_error())
}

fn xml_local_name(name: &[u8]) -> &[u8] {
    name.rsplit(|byte| *byte == b':').next().unwrap_or(name)
}

fn invalid_saml_assertion_error() -> StsError {
    StsError::Validation {
        message: "The request must contain a valid SAML assertion.".to_owned(),
    }
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

    fn github_jwt_with_times(exp: u64, nbf: u64) -> String {
        let header = URL_SAFE_NO_PAD.encode(r#"{"alg":"none"}"#);
        let payload = URL_SAFE_NO_PAD.encode(format!(
            r#"{{"iss":"https://token.actions.githubusercontent.com","sub":"repo:cloudish:ref:refs/heads/main","aud":["sts.amazonaws.com"],"amr":["authenticated"],"exp":{exp},"nbf":{nbf}}}"#
        ));
        format!("{header}.{payload}.signature")
    }

    fn namespaced_saml_assertion() -> String {
        STANDARD.encode(
            r#"<saml2:Assertion xmlns:saml2="urn:oasis:names:tc:SAML:2.0:assertion"><saml2:Subject><saml2:NameID NameQualifier="saml-qualifier" Format="urn:oasis:names:tc:SAML:2.0:nameid-format:transient">saml-subject</saml2:NameID></saml2:Subject><saml2:AttributeStatement><saml2:Attribute Name="https://aws.amazon.com/SAML/Attributes/RoleSessionName"><saml2:AttributeValue>federated-session</saml2:AttributeValue></saml2:Attribute></saml2:AttributeStatement><saml2:Conditions><saml2:AudienceRestriction><saml2:Audience>urn:amazon:webservices</saml2:Audience></saml2:AudienceRestriction></saml2:Conditions><saml2:Issuer>https://saml.example.com</saml2:Issuer></saml2:Assertion>"#,
        )
    }

    fn fallback_saml_assertion() -> String {
        STANDARD.encode("<Assertion />")
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
        assert_eq!(context.expiration_epoch_seconds(), None);
        assert_eq!(context.not_before_epoch_seconds(), None);
    }

    #[test]
    fn normalize_web_identity_preserves_numeric_exp_and_nbf_claims() {
        let context = normalize_web_identity(
            &AssumeRoleWithWebIdentityInput {
                duration_seconds: None,
                provider_id: None,
                role_arn: "arn:aws:iam::000000000000:role/demo"
                    .parse::<Arn>()
                    .expect("role ARN should parse"),
                role_session_name: "web-session".to_owned(),
                web_identity_token: github_jwt_with_times(
                    1_742_905_900,
                    1_742_905_500,
                ),
            },
            &"000000000000"
                .parse::<AccountId>()
                .expect("account id should parse"),
        )
        .expect("OIDC token should normalize");

        assert_eq!(context.expiration_epoch_seconds(), Some(1_742_905_900));
        assert_eq!(context.not_before_epoch_seconds(), Some(1_742_905_500));
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
    fn normalize_web_identity_opaque_provider_tokens_keep_time_claims_empty() {
        let context = normalize_web_identity(
            &AssumeRoleWithWebIdentityInput {
                duration_seconds: None,
                provider_id: Some("graph.facebook.com".to_owned()),
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
        .expect("opaque provider tokens should normalize");

        assert_eq!(context.provider_output(), "graph.facebook.com");
        assert_eq!(context.expiration_epoch_seconds(), None);
        assert_eq!(context.not_before_epoch_seconds(), None);
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
            saml_assertion: namespaced_saml_assertion(),
        })
        .expect("SAML assertion should normalize");

        assert_eq!(context.role_session_name(), "federated-session");
        assert_eq!(context.subject(), "saml-subject");
        assert_eq!(context.subject_type(), "transient");
        assert_eq!(
            context.condition_values().get("SAML:namequalifier"),
            Some(&vec!["saml-qualifier".to_owned()])
        );
    }

    #[test]
    fn normalize_saml_preserves_fallbacks_and_rejects_invalid_xml() {
        let fallback = normalize_saml(&AssumeRoleWithSamlInput {
            duration_seconds: None,
            principal_arn: "arn:aws:iam::000000000000:saml-provider/example"
                .parse::<Arn>()
                .expect("principal ARN should parse"),
            role_arn: "arn:aws:iam::000000000000:role/demo"
                .parse::<Arn>()
                .expect("role ARN should parse"),
            saml_assertion: fallback_saml_assertion(),
        })
        .expect("minimal SAML assertion should normalize");
        let invalid = normalize_saml(&AssumeRoleWithSamlInput {
            duration_seconds: None,
            principal_arn: "arn:aws:iam::000000000000:saml-provider/example"
                .parse::<Arn>()
                .expect("principal ARN should parse"),
            role_arn: "arn:aws:iam::000000000000:role/demo"
                .parse::<Arn>()
                .expect("role ARN should parse"),
            saml_assertion: STANDARD.encode("<Assertion>"),
        })
        .expect_err("malformed XML should fail");

        assert_eq!(fallback.role_session_name(), "saml-session");
        assert_eq!(fallback.audience(), "urn:amazon:webservices");
        assert_eq!(fallback.issuer(), "https://saml.example.com");
        assert_eq!(fallback.subject(), "saml-subject");
        assert!(matches!(invalid, crate::StsError::Validation { .. }));
    }
}
