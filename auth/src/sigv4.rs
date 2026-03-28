use crate::aws_chunked::{
    AwsChunkedMode, AwsChunkedSigningContext,
    STREAMING_AWS4_HMAC_SHA256_PAYLOAD,
    STREAMING_AWS4_HMAC_SHA256_PAYLOAD_TRAILER,
    STREAMING_UNSIGNED_PAYLOAD_TRAILER,
};
use aws::{
    AccountId, Arn, AwsError, AwsErrorFamily, CallerIdentity, CredentialScope,
    IamAccessKeyLookup, IamAccessKeyStatus, IamResourceTag, Partition,
    RuntimeDefaults, ServiceName, SessionCredentialLookup,
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use sha2::{Digest, Sha256};
use std::borrow::Cow;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use time::{Date, Month, PrimitiveDateTime, Time};

pub const BOOTSTRAP_ACCESS_KEY_ID: &str = "test";
pub const BOOTSTRAP_SECRET_ACCESS_KEY: &str = "test";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RequestHeader<'a> {
    name: &'a str,
    value: &'a str,
}

impl<'a> RequestHeader<'a> {
    pub fn new(name: &'a str, value: &'a str) -> Self {
        Self { name, value }
    }

    fn name(&self) -> &str {
        self.name
    }

    fn value(&self) -> &str {
        self.value
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestAuth<'a> {
    method: &'a str,
    path: &'a str,
    headers: Vec<RequestHeader<'a>>,
    body: &'a [u8],
}

impl<'a> RequestAuth<'a> {
    pub fn new(
        method: &'a str,
        path: &'a str,
        headers: Vec<RequestHeader<'a>>,
        body: &'a [u8],
    ) -> Self {
        Self { method, path, headers, body }
    }

    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|header| header.name().eq_ignore_ascii_case(name))
            .map(RequestHeader::value)
    }

    fn header_values(&self, name: &str) -> Vec<&str> {
        self.headers
            .iter()
            .filter(|header| header.name().eq_ignore_ascii_case(name))
            .map(RequestHeader::value)
            .collect()
    }

    fn body_hash(&self) -> String {
        hash_hex(self.body)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionAttributes {
    session_tags: Vec<IamResourceTag>,
    transitive_tag_keys: BTreeSet<String>,
}

impl SessionAttributes {
    pub fn new(
        session_tags: Vec<IamResourceTag>,
        transitive_tag_keys: BTreeSet<String>,
    ) -> Self {
        Self { session_tags, transitive_tag_keys }
    }

    pub fn session_tags(&self) -> &[IamResourceTag] {
        &self.session_tags
    }

    pub fn transitive_tag_keys(&self) -> &BTreeSet<String> {
        &self.transitive_tag_keys
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedRequest {
    account_id: AccountId,
    caller_identity: CallerIdentity,
    scope: CredentialScope,
    session: Option<SessionAttributes>,
}

impl VerifiedRequest {
    pub fn new(
        account_id: AccountId,
        caller_identity: CallerIdentity,
        scope: CredentialScope,
        session: Option<SessionAttributes>,
    ) -> Self {
        Self { account_id, caller_identity, scope, session }
    }

    pub fn account_id(&self) -> &AccountId {
        &self.account_id
    }

    pub fn caller_identity(&self) -> &CallerIdentity {
        &self.caller_identity
    }

    pub fn scope(&self) -> &CredentialScope {
        &self.scope
    }

    pub fn session(&self) -> Option<&SessionAttributes> {
        self.session.as_ref()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VerifiedPayload {
    SignedBody,
    UnsignedPayload,
    AwsChunked(AwsChunkedSigningContext),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedSignature {
    request: VerifiedRequest,
    payload: VerifiedPayload,
}

impl VerifiedSignature {
    pub fn new(request: VerifiedRequest, payload: VerifiedPayload) -> Self {
        Self { request, payload }
    }

    pub fn verified_request(&self) -> &VerifiedRequest {
        &self.request
    }

    pub fn payload(&self) -> &VerifiedPayload {
        &self.payload
    }

    fn into_verified_request(self) -> VerifiedRequest {
        self.request
    }
}

#[derive(Clone)]
pub struct Authenticator {
    defaults: RuntimeDefaults,
    time_source: Arc<dyn Fn() -> SystemTime + Send + Sync>,
}

impl Authenticator {
    pub fn new(defaults: RuntimeDefaults) -> Self {
        Self::with_time_source(defaults, Arc::new(SystemTime::now))
    }

    pub fn with_time_source(
        defaults: RuntimeDefaults,
        time_source: Arc<dyn Fn() -> SystemTime + Send + Sync>,
    ) -> Self {
        Self { defaults, time_source }
    }

    pub fn fallback_account(&self) -> &str {
        self.defaults.default_account()
    }

    pub fn fallback_region(&self) -> &str {
        self.defaults.default_region()
    }

    /// Verifies a SigV4-authenticated request using either header-based or
    /// presigned-query credentials.
    ///
    /// # Errors
    ///
    /// Returns an AWS-compatible validation or access-denied error when the
    /// request signature, credential scope, timestamps, or credential material
    /// are invalid.
    pub fn verify<L, S>(
        &self,
        request: &RequestAuth<'_>,
        access_keys: &L,
        sessions: &S,
    ) -> Result<Option<VerifiedRequest>, AwsError>
    where
        L: IamAccessKeyLookup,
        S: SessionCredentialLookup,
    {
        self.verify_details(request, access_keys, sessions).map(|verified| {
            verified.map(VerifiedSignature::into_verified_request)
        })
    }

    /// Verifies a SigV4-authenticated request and retains any streaming
    /// payload-signing context needed for later payload verification.
    ///
    /// # Errors
    ///
    /// Returns an AWS-compatible validation or access-denied error when the
    /// request signature, credential scope, timestamps, or credential material
    /// are invalid.
    pub fn verify_details<L, S>(
        &self,
        request: &RequestAuth<'_>,
        access_keys: &L,
        sessions: &S,
    ) -> Result<Option<VerifiedSignature>, AwsError>
    where
        L: IamAccessKeyLookup,
        S: SessionCredentialLookup,
    {
        if let Some(authorization_header) = request.header("authorization") {
            return self.verify_authorization_header(
                request,
                access_keys,
                sessions,
                authorization_header,
            );
        }

        let Some(authorization) =
            PresignedAuthorization::parse_from_request(request)?
        else {
            return Ok(None);
        };

        let request_epoch =
            parse_amz_date_epoch_seconds(&authorization.amz_date)?;
        let now_epoch = epoch_seconds((self.time_source)())?;
        if now_epoch
            > request_epoch.saturating_add(authorization.expires_seconds)
        {
            return Err(access_denied_error("Request has expired."));
        }

        let material = self.resolve_credential_material(
            access_keys,
            sessions,
            &authorization.credential,
            authorization.security_token.as_deref(),
        )?;
        let canonical_request = build_presigned_canonical_request(
            request,
            &authorization.signed_headers,
        )?;
        let expected_signature = build_signature(
            &material.secret_access_key,
            &authorization.credential.date,
            authorization.credential.scope.region().as_str(),
            &authorization.credential.signing_service,
            &authorization.amz_date,
            &canonical_request,
        );

        if expected_signature != authorization.signature {
            return Err(signature_does_not_match_error(
                "The request signature we calculated does not match the signature you provided.",
            ));
        }

        Ok(Some(VerifiedSignature::new(
            VerifiedRequest {
                account_id: material.account_id,
                caller_identity: material.caller_identity,
                scope: authorization.credential.scope,
                session: material.session,
            },
            VerifiedPayload::UnsignedPayload,
        )))
    }

    fn verify_authorization_header<L, S>(
        &self,
        request: &RequestAuth<'_>,
        access_keys: &L,
        sessions: &S,
        authorization_header: &str,
    ) -> Result<Option<VerifiedSignature>, AwsError>
    where
        L: IamAccessKeyLookup,
        S: SessionCredentialLookup,
    {
        if authorization_header.trim().is_empty() {
            return Err(incomplete_signature_error(
                "Authorization header cannot be blank when request signing is present.",
            ));
        }

        let authorization = AuthorizationFields::parse(authorization_header)?;
        let amz_date = request.header("x-amz-date").ok_or_else(|| {
            incomplete_signature_error(
                "Signed requests must include an X-Amz-Date header.",
            )
        })?;
        validate_amz_date(amz_date, &authorization.credential.date)?;

        let material = self.resolve_credential_material(
            access_keys,
            sessions,
            &authorization.credential,
            request.header("x-amz-security-token"),
        )?;
        let payload =
            canonical_payload_mode(request.header("x-amz-content-sha256"))?;
        let canonical_request = build_canonical_request(
            request,
            &authorization.signed_headers,
            &payload,
        )?;
        let expected_signature = build_signature(
            &material.secret_access_key,
            &authorization.credential.date,
            authorization.credential.scope.region().as_str(),
            &authorization.credential.signing_service,
            amz_date,
            &canonical_request,
        );

        if expected_signature != authorization.signature {
            return Err(signature_does_not_match_error(
                "The request signature we calculated does not match the signature you provided.",
            ));
        }

        let payload = payload.verified_payload(
            &material.secret_access_key,
            &authorization.credential,
            amz_date,
            &authorization.signature,
        );

        Ok(Some(VerifiedSignature::new(
            VerifiedRequest {
                account_id: material.account_id,
                caller_identity: material.caller_identity,
                scope: authorization.credential.scope,
                session: material.session,
            },
            payload,
        )))
    }

    fn resolve_credential_material<L, S>(
        &self,
        access_keys: &L,
        sessions: &S,
        credential: &CredentialFields,
        security_token: Option<&str>,
    ) -> Result<CredentialMaterial, AwsError>
    where
        L: IamAccessKeyLookup,
        S: SessionCredentialLookup,
    {
        if credential.access_key_id == BOOTSTRAP_ACCESS_KEY_ID {
            if security_token.is_some() {
                return Err(invalid_client_token_error(
                    "The security token included in the request is invalid.",
                ));
            }

            let account_id = self.defaults.default_account_id().clone();
            return Ok(CredentialMaterial {
                account_id: account_id.clone(),
                caller_identity: caller_identity(
                    root_arn(&account_id),
                    account_id.as_str().to_owned(),
                )?,
                secret_access_key: BOOTSTRAP_SECRET_ACCESS_KEY.to_owned(),
                session: None,
            });
        }

        if let Some(session) =
            sessions.find_session_credential(&credential.access_key_id)
        {
            let token = security_token.ok_or_else(|| {
                invalid_client_token_error(
                    "The security token included in the request is invalid.",
                )
            })?;
            if token != session.session_token {
                return Err(invalid_client_token_error(
                    "The security token included in the request is invalid.",
                ));
            }

            if epoch_seconds((self.time_source)())?
                >= session.expires_at_epoch_seconds
            {
                return Err(expired_token_error(
                    "The security token included in the request is expired.",
                ));
            }

            return Ok(CredentialMaterial {
                account_id: session.account_id,
                caller_identity: caller_identity(
                    session.principal_arn,
                    session.principal_id,
                )?,
                secret_access_key: session.secret_access_key,
                session: Some(SessionAttributes {
                    session_tags: session.session_tags,
                    transitive_tag_keys: session.transitive_tag_keys,
                }),
            });
        }

        let access_key = access_keys
            .find_access_key(&credential.access_key_id)
            .ok_or_else(|| {
                invalid_client_token_error(
                    "The security token included in the request is invalid.",
                )
            })?;

        if security_token.is_some() {
            return Err(invalid_client_token_error(
                "The security token included in the request is invalid.",
            ));
        }

        match access_key.status {
            IamAccessKeyStatus::Active => Ok(CredentialMaterial {
                account_id: access_key.account_id,
                caller_identity: caller_identity(
                    access_key.user_arn,
                    access_key.user_id,
                )?,
                secret_access_key: access_key.secret_access_key,
                session: None,
            }),
            IamAccessKeyStatus::Inactive => Err(invalid_client_token_error(
                "The security token included in the request is invalid.",
            )),
            IamAccessKeyStatus::Expired => Err(expired_token_error(
                "The security token included in the request is expired.",
            )),
        }
    }
}

/// Decodes the base64-encoded authorization message returned by STS.
///
/// # Errors
///
/// Returns an AWS-compatible validation error when the message is not valid
/// base64 data or does not decode to UTF-8.
pub fn decode_authorization_message(
    encoded_message: &str,
) -> Result<String, AwsError> {
    let decoded = STANDARD.decode(encoded_message).map_err(|_| {
        invalid_authorization_message_error(
            "The encoded message is not valid base64 data.",
        )
    })?;

    String::from_utf8(decoded).map_err(|_| {
        invalid_authorization_message_error(
            "The encoded message did not decode to valid UTF-8.",
        )
    })
}

#[derive(Debug, Clone)]
struct CredentialMaterial {
    account_id: AccountId,
    caller_identity: CallerIdentity,
    secret_access_key: String,
    session: Option<SessionAttributes>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AuthorizationFields {
    credential: CredentialFields,
    signature: String,
    signed_headers: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PresignedAuthorization {
    credential: CredentialFields,
    signature: String,
    signed_headers: Vec<String>,
    amz_date: String,
    expires_seconds: u64,
    security_token: Option<String>,
}

impl PresignedAuthorization {
    fn parse_from_request(
        request: &RequestAuth<'_>,
    ) -> Result<Option<Self>, AwsError> {
        let query =
            request.path.split_once('?').map(|(_, query)| query).unwrap_or("");
        let Some(algorithm) =
            decoded_query_parameter(query, "X-Amz-Algorithm")?
        else {
            return Ok(None);
        };
        if algorithm != "AWS4-HMAC-SHA256" {
            return Err(incomplete_signature_error(
                "X-Amz-Algorithm must use AWS4-HMAC-SHA256.",
            ));
        }

        let credential = CredentialFields::parse(
            &required_decoded_query_parameter(query, "X-Amz-Credential")?,
        )?;
        let amz_date = required_decoded_query_parameter(query, "X-Amz-Date")?;
        validate_amz_date(&amz_date, &credential.date)?;
        let expires_seconds =
            required_decoded_query_parameter(query, "X-Amz-Expires")?
                .parse::<u64>()
                .map_err(|_| {
                    access_denied_error("Invalid X-Amz-Expires value.")
                })?;
        let signature =
            required_decoded_query_parameter(query, "X-Amz-Signature")?
                .to_ascii_lowercase();
        validate_hex_signature(&signature)?;
        let signed_headers =
            required_decoded_query_parameter(query, "X-Amz-SignedHeaders")?
                .split(';')
                .map(str::trim)
                .filter(|header| !header.is_empty())
                .map(str::to_owned)
                .collect::<Vec<_>>();
        if signed_headers.is_empty() {
            return Err(incomplete_signature_error(
                "X-Amz-SignedHeaders must list at least one signed header.",
            ));
        }

        Ok(Some(Self {
            credential,
            signature,
            signed_headers,
            amz_date,
            expires_seconds,
            security_token: decoded_query_parameter(
                query,
                "X-Amz-Security-Token",
            )?,
        }))
    }
}

impl AuthorizationFields {
    fn parse(value: &str) -> Result<Self, AwsError> {
        let header = value.trim();
        let (algorithm, fields) = header
            .find(char::is_whitespace)
            .map(|index| (&header[..index], header[index..].trim_start()))
            .ok_or_else(|| {
                incomplete_signature_error(
                    "Authorization header must start with the AWS4-HMAC-SHA256 algorithm identifier.",
                )
            })?;
        if algorithm != "AWS4-HMAC-SHA256" {
            return Err(incomplete_signature_error(
                "Authorization header must use AWS4-HMAC-SHA256.",
            ));
        }

        let mut credential = None;
        let mut signature = None;
        let mut signed_headers = None;

        for part in
            fields.split(',').map(str::trim).filter(|part| !part.is_empty())
        {
            let (name, value) = part.split_once('=').ok_or_else(|| {
                incomplete_signature_error(
                    "Authorization header fields must be NAME=VALUE pairs.",
                )
            })?;

            match name {
                "Credential" => {
                    credential = Some(CredentialFields::parse(value)?);
                }
                "Signature" => {
                    validate_hex_signature(value)?;
                    signature = Some(value.to_ascii_lowercase());
                }
                "SignedHeaders" => {
                    let headers = value
                        .split(';')
                        .map(str::trim)
                        .filter(|header| !header.is_empty())
                        .map(str::to_owned)
                        .collect::<Vec<_>>();
                    if headers.is_empty() {
                        return Err(incomplete_signature_error(
                            "Authorization header must list at least one signed header.",
                        ));
                    }
                    signed_headers = Some(headers);
                }
                _ => {}
            }
        }

        Ok(Self {
            credential: credential.ok_or_else(|| {
                incomplete_signature_error(
                    "Authorization header must contain a SigV4 Credential scope.",
                )
            })?,
            signature: signature.ok_or_else(|| {
                incomplete_signature_error(
                    "Authorization header must contain a SigV4 Signature.",
                )
            })?,
            signed_headers: signed_headers.ok_or_else(|| {
                incomplete_signature_error(
                    "Authorization header must contain SignedHeaders metadata.",
                )
            })?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CredentialFields {
    access_key_id: String,
    date: String,
    signing_service: String,
    scope: CredentialScope,
}

impl CredentialFields {
    fn parse(value: &str) -> Result<Self, AwsError> {
        let mut parts = value.split('/');
        let access_key_id = parts.next().unwrap_or_default();
        let date = parts.next().unwrap_or_default();
        let region = parts.next().unwrap_or_default();
        let signing_service = parts.next().unwrap_or_default();
        let terminator = parts.next().unwrap_or_default();

        if access_key_id.is_empty()
            || date.len() != 8
            || !date.bytes().all(|byte| byte.is_ascii_digit())
            || region.is_empty()
            || signing_service.is_empty()
            || terminator != "aws4_request"
            || parts.next().is_some()
        {
            return Err(incomplete_signature_error(
                "Authorization Credential scope must match access-key/date/region/service/aws4_request.",
            ));
        }

        let region = region.parse().map_err(|source| {
            incomplete_signature_error(format!(
                "Authorization Credential scope contains an invalid region: {source}"
            ))
        })?;
        let service = signing_service.parse().map_err(|_| {
            incomplete_signature_error(format!(
                "Authorization Credential scope contains an unsupported service: {signing_service}"
            ))
        })?;

        Ok(Self {
            access_key_id: access_key_id.to_owned(),
            date: date.to_owned(),
            signing_service: signing_service.to_owned(),
            scope: CredentialScope::new(region, service),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CanonicalPayloadMode<'a> {
    SignedBody,
    HeaderValue(&'a str),
    AwsChunked(AwsChunkedMode),
}

impl CanonicalPayloadMode<'_> {
    fn canonical_request_payload_hash<'a>(
        &'a self,
        request: &'a RequestAuth<'_>,
    ) -> Cow<'a, str> {
        match self {
            Self::SignedBody => Cow::Owned(request.body_hash()),
            Self::HeaderValue(value) => Cow::Borrowed(value),
            Self::AwsChunked(AwsChunkedMode::Payload) => {
                Cow::Borrowed(STREAMING_AWS4_HMAC_SHA256_PAYLOAD)
            }
            Self::AwsChunked(AwsChunkedMode::PayloadTrailer) => {
                Cow::Borrowed(STREAMING_AWS4_HMAC_SHA256_PAYLOAD_TRAILER)
            }
        }
    }

    fn verified_payload(
        self,
        secret_access_key: &str,
        credential: &CredentialFields,
        amz_date: &str,
        seed_signature: &str,
    ) -> VerifiedPayload {
        match self {
            Self::SignedBody => VerifiedPayload::SignedBody,
            Self::HeaderValue("UNSIGNED-PAYLOAD") => {
                VerifiedPayload::UnsignedPayload
            }
            Self::HeaderValue(_) => VerifiedPayload::SignedBody,
            Self::AwsChunked(mode) => {
                VerifiedPayload::AwsChunked(AwsChunkedSigningContext::new(
                    secret_access_key,
                    &credential.date,
                    credential.scope.region().as_str(),
                    &credential.signing_service,
                    amz_date,
                    seed_signature,
                    mode,
                ))
            }
        }
    }
}

fn canonical_payload_mode(
    x_amz_content_sha256: Option<&str>,
) -> Result<CanonicalPayloadMode<'_>, AwsError> {
    match x_amz_content_sha256 {
        None => Ok(CanonicalPayloadMode::SignedBody),
        Some(STREAMING_AWS4_HMAC_SHA256_PAYLOAD) => {
            Ok(CanonicalPayloadMode::AwsChunked(AwsChunkedMode::Payload))
        }
        Some(STREAMING_AWS4_HMAC_SHA256_PAYLOAD_TRAILER) => Ok(
            CanonicalPayloadMode::AwsChunked(AwsChunkedMode::PayloadTrailer),
        ),
        Some(STREAMING_UNSIGNED_PAYLOAD_TRAILER) => Err(access_denied_error(
            "X-Amz-Content-Sha256 payload mode STREAMING-UNSIGNED-PAYLOAD-TRAILER is not supported.",
        )),
        Some(value) => Ok(CanonicalPayloadMode::HeaderValue(value)),
    }
}

fn build_canonical_request(
    request: &RequestAuth<'_>,
    signed_headers: &[String],
    payload: &CanonicalPayloadMode<'_>,
) -> Result<String, AwsError> {
    build_canonical_request_with_payload(
        request,
        signed_headers,
        &payload.canonical_request_payload_hash(request),
        &[],
    )
}

fn build_presigned_canonical_request(
    request: &RequestAuth<'_>,
    signed_headers: &[String],
) -> Result<String, AwsError> {
    build_canonical_request_with_payload(
        request,
        signed_headers,
        "UNSIGNED-PAYLOAD",
        &["X-Amz-Signature"],
    )
}

fn build_canonical_request_with_payload(
    request: &RequestAuth<'_>,
    signed_headers: &[String],
    payload_hash: &str,
    excluded_query_parameters: &[&str],
) -> Result<String, AwsError> {
    let (path, query) =
        request.path.split_once('?').unwrap_or((request.path, ""));
    let canonical_headers = signed_headers
        .iter()
        .map(|name| canonical_header_line(request, name))
        .collect::<Result<Vec<_>, _>>()?
        .join("");
    let signed_headers = signed_headers.join(";");

    Ok(format!(
        "{}\n{}\n{}\n{}\n{}\n{}",
        request.method,
        canonical_uri(path),
        canonical_query_excluding(query, excluded_query_parameters),
        canonical_headers,
        signed_headers,
        payload_hash,
    ))
}

fn canonical_header_line(
    request: &RequestAuth<'_>,
    name: &str,
) -> Result<String, AwsError> {
    if name.chars().any(|character| {
        !matches!(
            character,
            'a'..='z' | '0'..='9' | '-'
        )
    }) {
        return Err(incomplete_signature_error(
            "Signed header names must be lowercase ASCII header tokens.",
        ));
    }

    let values = request.header_values(name);
    if values.is_empty() {
        return Err(incomplete_signature_error(format!(
            "Signed header {name} is missing from the request.",
        )));
    }

    let value = values
        .into_iter()
        .map(normalize_header_value)
        .collect::<Vec<_>>()
        .join(",");
    Ok(format!("{name}:{value}\n"))
}

fn canonical_uri(path: &str) -> String {
    let path = if path.is_empty() { "/" } else { path };
    let path = if path.starts_with('/') {
        path.to_owned()
    } else {
        format!("/{path}")
    };

    percent_encode(path.as_bytes(), true)
}

#[cfg(test)]
fn canonical_query(query: &str) -> String {
    canonical_query_excluding(query, &[])
}

fn canonical_query_excluding(query: &str, excluded_names: &[&str]) -> String {
    let mut pairs = query
        .split('&')
        .filter(|pair| !pair.is_empty())
        .map(|pair| {
            let (name, value) = pair.split_once('=').unwrap_or((pair, ""));
            (
                percent_encode(&percent_decode_query_component(name), false),
                percent_encode(&percent_decode_query_component(value), false),
            )
        })
        .filter(|(name, _)| {
            !excluded_names.iter().any(|excluded| name == excluded)
        })
        .collect::<Vec<_>>();
    pairs.sort();

    pairs
        .into_iter()
        .map(|(name, value)| format!("{name}={value}"))
        .collect::<Vec<_>>()
        .join("&")
}

fn percent_decode_query_component(value: &str) -> Vec<u8> {
    let bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;

    while let Some(&current) = bytes.get(index) {
        if let Some([b'%', high, low, ..]) = bytes.get(index..)
            && let Some(decoded_byte) = decode_hex_pair(*high, *low)
        {
            decoded.push(decoded_byte);
            index += 3;
            continue;
        }

        decoded.push(current);
        index += 1;
    }

    decoded
}

fn decoded_query_parameter(
    query: &str,
    name: &str,
) -> Result<Option<String>, AwsError> {
    for pair in query.split('&').filter(|pair| !pair.is_empty()) {
        let (raw_name, raw_value) = pair.split_once('=').unwrap_or((pair, ""));
        if decode_query_parameter_component(raw_name)? != name {
            continue;
        }

        return Ok(Some(decode_query_parameter_component(raw_value)?));
    }

    Ok(None)
}

fn required_decoded_query_parameter(
    query: &str,
    name: &str,
) -> Result<String, AwsError> {
    decoded_query_parameter(query, name)?.ok_or_else(|| {
        access_denied_error(format!(
            "Missing required pre-signed URL parameter {name}."
        ))
    })
}

fn decode_query_parameter_component(value: &str) -> Result<String, AwsError> {
    String::from_utf8(percent_decode_query_component(value)).map_err(|_| {
        incomplete_signature_error(
            "Pre-signed URL query parameters must decode to valid UTF-8.",
        )
    })
}

fn decode_hex_pair(high: u8, low: u8) -> Option<u8> {
    Some((decode_hex(high)? << 4) | decode_hex(low)?)
}

fn decode_hex(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        b'A'..=b'F' => Some(value - b'A' + 10),
        _ => None,
    }
}

fn normalize_header_value(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn percent_encode(bytes: &[u8], preserve_slash: bool) -> String {
    let mut encoded = String::with_capacity(bytes.len());

    for byte in bytes {
        match byte {
            b'A'..=b'Z'
            | b'a'..=b'z'
            | b'0'..=b'9'
            | b'-'
            | b'_'
            | b'.'
            | b'~' => encoded.push(char::from(*byte)),
            b'/' if preserve_slash => encoded.push('/'),
            _ => encoded.push_str(&format!("%{byte:02X}")),
        }
    }

    encoded
}

fn build_signature(
    secret_access_key: &str,
    date: &str,
    region: &str,
    service: &str,
    amz_date: &str,
    canonical_request: &str,
) -> String {
    let scope = format!("{date}/{region}/{service}/aws4_request");
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
        hash_hex(canonical_request.as_bytes())
    );
    let signing_key = signing_key(secret_access_key, date, region, service);

    hex_encode(&hmac_bytes(&signing_key, string_to_sign.as_bytes()))
}

pub(crate) fn signing_key(
    secret_access_key: &str,
    date: &str,
    region: &str,
    service: &str,
) -> [u8; 32] {
    let date_key = hmac_bytes(
        format!("AWS4{secret_access_key}").as_bytes(),
        date.as_bytes(),
    );
    let region_key = hmac_bytes(&date_key, region.as_bytes());
    let service_key = hmac_bytes(&region_key, service.as_bytes());
    hmac_bytes(&service_key, b"aws4_request")
}

pub(crate) fn hmac_bytes(key: &[u8], data: &[u8]) -> [u8; 32] {
    const BLOCK_SIZE: usize = 64;

    let mut normalized_key = [0_u8; BLOCK_SIZE];
    if key.len() > BLOCK_SIZE {
        let digest = Sha256::digest(key);
        for (slot, byte) in normalized_key.iter_mut().zip(digest) {
            *slot = byte;
        }
    } else {
        for (slot, byte) in normalized_key.iter_mut().zip(key.iter().copied())
        {
            *slot = byte;
        }
    }

    let mut inner_pad = [0x36_u8; BLOCK_SIZE];
    let mut outer_pad = [0x5c_u8; BLOCK_SIZE];
    for ((inner, outer), key_byte) in inner_pad
        .iter_mut()
        .zip(outer_pad.iter_mut())
        .zip(normalized_key.iter().copied())
    {
        *inner ^= key_byte;
        *outer ^= key_byte;
    }

    let mut inner = Sha256::new();
    inner.update(inner_pad);
    inner.update(data);
    let inner_hash = inner.finalize();

    let mut outer = Sha256::new();
    outer.update(outer_pad);
    outer.update(inner_hash);
    outer.finalize().into()
}

pub(crate) fn hash_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex_encode(&hasher.finalize())
}

pub(crate) fn hex_encode(bytes: &[u8]) -> String {
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push_str(&format!("{byte:02x}"));
    }
    encoded
}

pub(crate) fn validate_hex_signature(value: &str) -> Result<(), AwsError> {
    if value.len() != 64 || !value.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(incomplete_signature_error(
            "Authorization Signature must be a 64-character hexadecimal string.",
        ));
    }

    Ok(())
}

fn validate_amz_date(
    amz_date: &str,
    scope_date: &str,
) -> Result<(), AwsError> {
    let bytes = amz_date.as_bytes();
    let valid = matches!(
        bytes,
        [
            y1,
            y2,
            y3,
            y4,
            m1,
            m2,
            d1,
            d2,
            b'T',
            h1,
            h2,
            min1,
            min2,
            s1,
            s2,
            b'Z',
        ] if [
            y1, y2, y3, y4, m1, m2, d1, d2, h1, h2, min1, min2, s1, s2,
        ]
        .into_iter()
        .all(u8::is_ascii_digit)
    );
    if !valid {
        return Err(incomplete_signature_error(
            "X-Amz-Date must use the SigV4 timestamp format YYYYMMDD'T'HHMMSS'Z'.",
        ));
    }
    if amz_date.get(..8) != Some(scope_date) {
        return Err(incomplete_signature_error(
            "Authorization Credential scope date must match the X-Amz-Date header date.",
        ));
    }

    Ok(())
}

fn parse_amz_date_epoch_seconds(amz_date: &str) -> Result<u64, AwsError> {
    let year = amz_date[0..4].parse::<i32>().map_err(|_| {
        incomplete_signature_error("X-Amz-Date must be valid.")
    })?;
    let month = amz_date[4..6].parse::<u8>().map_err(|_| {
        incomplete_signature_error("X-Amz-Date must be valid.")
    })?;
    let day = amz_date[6..8].parse::<u8>().map_err(|_| {
        incomplete_signature_error("X-Amz-Date must be valid.")
    })?;
    let hour = amz_date[9..11].parse::<u8>().map_err(|_| {
        incomplete_signature_error("X-Amz-Date must be valid.")
    })?;
    let minute = amz_date[11..13].parse::<u8>().map_err(|_| {
        incomplete_signature_error("X-Amz-Date must be valid.")
    })?;
    let second = amz_date[13..15].parse::<u8>().map_err(|_| {
        incomplete_signature_error("X-Amz-Date must be valid.")
    })?;
    let date = Date::from_calendar_date(
        year,
        Month::try_from(month).map_err(|_| {
            incomplete_signature_error("X-Amz-Date must be valid.")
        })?,
        day,
    )
    .map_err(|_| incomplete_signature_error("X-Amz-Date must be valid."))?;
    let time = Time::from_hms(hour, minute, second).map_err(|_| {
        incomplete_signature_error("X-Amz-Date must be valid.")
    })?;
    let timestamp =
        PrimitiveDateTime::new(date, time).assume_utc().unix_timestamp();

    u64::try_from(timestamp)
        .map_err(|_| incomplete_signature_error("X-Amz-Date must be valid."))
}

fn epoch_seconds(time: SystemTime) -> Result<u64, AwsError> {
    time.duration_since(UNIX_EPOCH).map(|duration| duration.as_secs()).map_err(
        |_| {
            aws_error(
                AwsErrorFamily::Internal,
                "InternalFailure",
                "System time is earlier than the Unix epoch.",
                500,
                false,
            )
        },
    )
}

fn root_arn(account_id: &AccountId) -> Arn {
    Arn::trusted_new(
        Partition::aws(),
        ServiceName::Iam,
        None,
        Some(account_id.clone()),
        aws::ArnResource::Generic("root".to_owned()),
    )
}

fn caller_identity(
    arn: Arn,
    principal_id: String,
) -> Result<CallerIdentity, AwsError> {
    CallerIdentity::try_new(arn, principal_id).map_err(|_| {
        aws_error(
            AwsErrorFamily::Internal,
            "InternalFailure",
            "Stored caller identity is invalid.",
            500,
            false,
        )
    })
}

pub(crate) fn incomplete_signature_error(
    message: impl Into<String>,
) -> AwsError {
    aws_error(
        AwsErrorFamily::Validation,
        "IncompleteSignature",
        message,
        400,
        true,
    )
}

fn invalid_client_token_error(message: impl Into<String>) -> AwsError {
    aws_error(
        AwsErrorFamily::AccessDenied,
        "InvalidClientTokenId",
        message,
        403,
        true,
    )
}

fn expired_token_error(message: impl Into<String>) -> AwsError {
    aws_error(AwsErrorFamily::AccessDenied, "ExpiredToken", message, 403, true)
}

pub(crate) fn access_denied_error(message: impl Into<String>) -> AwsError {
    aws_error(AwsErrorFamily::AccessDenied, "AccessDenied", message, 403, true)
}

fn invalid_authorization_message_error(
    message: impl Into<String>,
) -> AwsError {
    aws_error(
        AwsErrorFamily::Validation,
        "InvalidAuthorizationMessageException",
        message,
        400,
        true,
    )
}

pub(crate) fn signature_does_not_match_error(
    message: impl Into<String>,
) -> AwsError {
    aws_error(
        AwsErrorFamily::AccessDenied,
        "SignatureDoesNotMatch",
        message,
        403,
        true,
    )
}

fn aws_error(
    family: AwsErrorFamily,
    code: impl Into<String>,
    message: impl Into<String>,
    status_code: u16,
    sender_fault: bool,
) -> AwsError {
    AwsError::trusted_custom(family, code, message, status_code, sender_fault)
}

#[cfg(test)]
mod tests {
    use super::{
        Authenticator, BOOTSTRAP_ACCESS_KEY_ID, BOOTSTRAP_SECRET_ACCESS_KEY,
        RequestAuth, RequestHeader, build_presigned_canonical_request,
        build_signature, canonical_query, decode_authorization_message,
    };
    use aws::{
        AccountId, Arn, IamAccessKeyLookup, IamAccessKeyRecord,
        IamAccessKeyStatus, IamResourceTag, RuntimeDefaults,
        SessionCredentialLookup, SessionCredentialRecord,
    };
    use std::collections::BTreeSet;
    use std::sync::Arc;
    use std::time::{Duration, UNIX_EPOCH};

    #[derive(Debug, Default)]
    struct AccessKeyLookup {
        record: Option<IamAccessKeyRecord>,
    }

    impl IamAccessKeyLookup for AccessKeyLookup {
        fn find_access_key(
            &self,
            access_key_id: &str,
        ) -> Option<IamAccessKeyRecord> {
            self.record
                .clone()
                .filter(|record| record.access_key_id == access_key_id)
        }
    }

    #[derive(Debug, Default)]
    struct SessionLookup {
        record: Option<SessionCredentialRecord>,
    }

    impl SessionCredentialLookup for SessionLookup {
        fn find_session_credential(
            &self,
            access_key_id: &str,
        ) -> Option<SessionCredentialRecord> {
            self.record
                .clone()
                .filter(|record| record.access_key_id == access_key_id)
        }
    }

    fn defaults() -> RuntimeDefaults {
        RuntimeDefaults::try_new(
            Some("000000000000".to_owned()),
            Some("eu-west-2".to_owned()),
            Some("/tmp/cloudish-auth".to_owned()),
        )
        .expect("test defaults should be valid")
    }

    fn authenticator(now: u64) -> Authenticator {
        Authenticator::with_time_source(
            defaults(),
            Arc::new(move || UNIX_EPOCH + Duration::from_secs(now)),
        )
    }

    fn authorization_header(
        access_key_id: &str,
        secret_access_key: &str,
        date: &str,
        amz_date: &str,
        service: &str,
        headers: &[&str],
        body: &[u8],
    ) -> String {
        let signed_headers = headers.join(";");
        let request = RequestAuth::new(
            "POST",
            "/",
            headers
                .iter()
                .map(|header| match *header {
                    "content-type" => RequestHeader::new(
                        "Content-Type",
                        "application/x-www-form-urlencoded",
                    ),
                    "host" => RequestHeader::new("Host", "localhost"),
                    "x-amz-date" => RequestHeader::new("X-Amz-Date", amz_date),
                    "x-amz-security-token" => {
                        RequestHeader::new("X-Amz-Security-Token", "token-1")
                    }
                    _ => panic!("unsupported signed header in test"),
                })
                .collect(),
            body,
        );
        let canonical = super::build_canonical_request(
            &request,
            &headers
                .iter()
                .map(|header| (*header).to_owned())
                .collect::<Vec<_>>(),
            &super::canonical_payload_mode(
                request.header("x-amz-content-sha256"),
            )
            .expect("test payload mode should be valid"),
        )
        .expect("test canonical request should build");
        let signature = build_signature(
            secret_access_key,
            date,
            "eu-west-2",
            service,
            amz_date,
            &canonical,
        );

        format!(
            "AWS4-HMAC-SHA256 Credential={access_key_id}/{date}/eu-west-2/{service}/aws4_request, SignedHeaders={signed_headers}, Signature={signature}"
        )
    }

    fn s3_presigned_get_path(path: &str, expires_seconds: u64) -> String {
        let method = "GET";
        let access_key_id = BOOTSTRAP_ACCESS_KEY_ID;
        let secret_access_key = BOOTSTRAP_SECRET_ACCESS_KEY;
        let amz_date = "20260325T120000Z";
        let service = "s3";
        let signed_headers = ["host"];
        let signed_headers = signed_headers.join(";");
        let mut request_path = format!(
            "{path}?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential={access_key_id}%2F20260325%2Feu-west-2%2F{service}%2Faws4_request&X-Amz-Date={amz_date}&X-Amz-Expires={expires_seconds}&X-Amz-SignedHeaders={signed_headers}",
        );
        let request = RequestAuth::new(
            method,
            &request_path,
            vec![RequestHeader::new("Host", "localhost")],
            b"",
        );
        let canonical = build_presigned_canonical_request(
            &request,
            &signed_headers.split(';').map(str::to_owned).collect::<Vec<_>>(),
        )
        .expect("pre-signed canonical request should build");
        let signature = build_signature(
            secret_access_key,
            "20260325",
            "eu-west-2",
            service,
            amz_date,
            &canonical,
        );
        request_path.push_str("&X-Amz-Signature=");
        request_path.push_str(&signature);
        request_path
    }

    #[test]
    fn canonical_request_uses_x_amz_content_sha256_header_value() {
        let request = RequestAuth::new(
            "PUT",
            "/examplebucket/chunkObject.txt",
            vec![
                RequestHeader::new("Content-Encoding", "aws-chunked"),
                RequestHeader::new("Content-Length", "66824"),
                RequestHeader::new("Host", "s3.amazonaws.com"),
                RequestHeader::new(
                    "X-Amz-Content-Sha256",
                    "STREAMING-AWS4-HMAC-SHA256-PAYLOAD",
                ),
                RequestHeader::new("X-Amz-Date", "20130524T000000Z"),
                RequestHeader::new("X-Amz-Decoded-Content-Length", "66560"),
                RequestHeader::new(
                    "X-Amz-Storage-Class",
                    "REDUCED_REDUNDANCY",
                ),
            ],
            b"not-the-canonical-payload",
        );

        let canonical = super::build_canonical_request(
            &request,
            &[
                "content-encoding",
                "content-length",
                "host",
                "x-amz-content-sha256",
                "x-amz-date",
                "x-amz-decoded-content-length",
                "x-amz-storage-class",
            ]
            .into_iter()
            .map(str::to_owned)
            .collect::<Vec<_>>(),
            &super::canonical_payload_mode(
                request.header("x-amz-content-sha256"),
            )
            .expect("payload mode should be valid"),
        )
        .expect("canonical request should build");

        assert!(canonical.ends_with("STREAMING-AWS4-HMAC-SHA256-PAYLOAD"));
        assert!(
            !canonical
                .ends_with(&super::hash_hex(b"not-the-canonical-payload"))
        );
    }

    #[test]
    fn reject_unsupported_unsigned_trailing_payload_mode() {
        let error = super::canonical_payload_mode(Some(
            "STREAMING-UNSIGNED-PAYLOAD-TRAILER",
        ))
        .expect_err("unsigned trailing payload mode must fail explicitly");

        assert_eq!(error.code(), "AccessDenied");
    }

    #[test]
    fn auth_verifies_bootstrap_root_credentials() {
        let amz_date = "20260325T120000Z";
        let body = b"Action=GetCallerIdentity&Version=2011-06-15";
        let authorization = authorization_header(
            BOOTSTRAP_ACCESS_KEY_ID,
            BOOTSTRAP_SECRET_ACCESS_KEY,
            "20260325",
            amz_date,
            "sts",
            &["content-type", "host", "x-amz-date"],
            body,
        );
        let request = RequestAuth::new(
            "POST",
            "/",
            vec![
                RequestHeader::new("Host", "localhost"),
                RequestHeader::new(
                    "Content-Type",
                    "application/x-www-form-urlencoded",
                ),
                RequestHeader::new("X-Amz-Date", amz_date),
                RequestHeader::new("Authorization", &authorization),
            ],
            body,
        );

        let verified = authenticator(1_742_905_600)
            .verify(
                &request,
                &AccessKeyLookup::default(),
                &SessionLookup::default(),
            )
            .expect("signature should verify")
            .expect("signed request should authenticate");

        assert_eq!(verified.account_id().as_str(), "000000000000");
        assert_eq!(
            verified.caller_identity().arn().to_string(),
            "arn:aws:iam::000000000000:root"
        );
        assert_eq!(verified.caller_identity().principal_id(), "000000000000");
        assert_eq!(verified.scope().service().as_str(), "sts");
        assert_eq!(verified.scope().region().as_str(), "eu-west-2");
    }

    #[test]
    fn auth_verifies_temporary_session_credentials() {
        let amz_date = "20260325T120000Z";
        let body = b"Action=ListUsers";
        let authorization = authorization_header(
            "ASIA0000000000000001",
            "temporary-secret",
            "20260325",
            amz_date,
            "iam",
            &["content-type", "host", "x-amz-date", "x-amz-security-token"],
            body,
        );
        let request = RequestAuth::new(
            "POST",
            "/",
            vec![
                RequestHeader::new("Host", "localhost"),
                RequestHeader::new(
                    "Content-Type",
                    "application/x-www-form-urlencoded",
                ),
                RequestHeader::new("X-Amz-Date", amz_date),
                RequestHeader::new("X-Amz-Security-Token", "token-1"),
                RequestHeader::new("Authorization", &authorization),
            ],
            body,
        );
        let session_lookup = SessionLookup {
            record: Some(SessionCredentialRecord {
                access_key_id: "ASIA0000000000000001".to_owned(),
                account_id: "123456789012"
                    .parse::<AccountId>()
                    .expect("account id should parse"),
                expires_at_epoch_seconds: 1_742_909_200,
                principal_arn:
                    "arn:aws:sts::123456789012:assumed-role/demo/session"
                        .parse::<Arn>()
                        .expect("assumed role ARN should parse"),
                principal_id: "AROA1234567890EXAMPLE:session".to_owned(),
                secret_access_key: "temporary-secret".to_owned(),
                session_tags: vec![IamResourceTag {
                    key: "env".to_owned(),
                    value: "dev".to_owned(),
                }],
                session_token: "token-1".to_owned(),
                transitive_tag_keys: BTreeSet::from(["env".to_owned()]),
            }),
        };

        let verified = authenticator(1_742_905_600)
            .verify(&request, &AccessKeyLookup::default(), &session_lookup)
            .expect("signature should verify")
            .expect("signed request should authenticate");

        assert_eq!(verified.account_id().as_str(), "123456789012");
        assert_eq!(
            verified.caller_identity().arn().to_string(),
            "arn:aws:sts::123456789012:assumed-role/demo/session"
        );
        assert_eq!(
            verified
                .session()
                .expect("temporary credential should expose session state")
                .session_tags()[0]
                .key,
            "env"
        );
    }

    #[test]
    fn auth_rejects_expired_session_credentials() {
        let amz_date = "20260325T120000Z";
        let body = b"Action=ListUsers";
        let authorization = authorization_header(
            "ASIA0000000000000001",
            "temporary-secret",
            "20260325",
            amz_date,
            "iam",
            &["content-type", "host", "x-amz-date", "x-amz-security-token"],
            body,
        );
        let request = RequestAuth::new(
            "POST",
            "/",
            vec![
                RequestHeader::new("Host", "localhost"),
                RequestHeader::new(
                    "Content-Type",
                    "application/x-www-form-urlencoded",
                ),
                RequestHeader::new("X-Amz-Date", amz_date),
                RequestHeader::new("X-Amz-Security-Token", "token-1"),
                RequestHeader::new("Authorization", &authorization),
            ],
            body,
        );
        let session_lookup = SessionLookup {
            record: Some(SessionCredentialRecord {
                access_key_id: "ASIA0000000000000001".to_owned(),
                account_id: "123456789012"
                    .parse::<AccountId>()
                    .expect("account id should parse"),
                expires_at_epoch_seconds: 1_742_905_599,
                principal_arn:
                    "arn:aws:sts::123456789012:assumed-role/demo/session"
                        .parse::<Arn>()
                        .expect("assumed role ARN should parse"),
                principal_id: "AROA1234567890EXAMPLE:session".to_owned(),
                secret_access_key: "temporary-secret".to_owned(),
                session_tags: Vec::new(),
                session_token: "token-1".to_owned(),
                transitive_tag_keys: BTreeSet::new(),
            }),
        };

        let error = authenticator(1_742_905_600)
            .verify(&request, &AccessKeyLookup::default(), &session_lookup)
            .expect_err("expired session token should fail");

        assert_eq!(error.code(), "ExpiredToken");
        assert_eq!(error.status_code(), 403);
    }

    #[test]
    fn auth_rejects_signature_mismatches() {
        let request = RequestAuth::new(
            "POST",
            "/",
            vec![
                RequestHeader::new("Host", "localhost"),
                RequestHeader::new(
                    "Content-Type",
                    "application/x-www-form-urlencoded",
                ),
                RequestHeader::new("X-Amz-Date", "20260325T120000Z"),
                RequestHeader::new(
                    "Authorization",
                    "AWS4-HMAC-SHA256 Credential=test/20260325/eu-west-2/sts/aws4_request, SignedHeaders=content-type;host;x-amz-date, Signature=0000000000000000000000000000000000000000000000000000000000000000",
                ),
            ],
            b"Action=GetCallerIdentity&Version=2011-06-15",
        );

        let error = authenticator(1_742_905_600)
            .verify(
                &request,
                &AccessKeyLookup::default(),
                &SessionLookup::default(),
            )
            .expect_err("bad signature should fail");

        assert_eq!(error.code(), "SignatureDoesNotMatch");
        assert_eq!(error.status_code(), 403);
    }

    #[test]
    fn auth_rejects_incomplete_authorization_headers() {
        let request = RequestAuth::new(
            "POST",
            "/",
            vec![
                RequestHeader::new("Host", "localhost"),
                RequestHeader::new("X-Amz-Date", "20260325T120000Z"),
                RequestHeader::new("Authorization", "AWS4-HMAC-SHA256"),
            ],
            b"",
        );

        let error = authenticator(1_742_905_600)
            .verify(
                &request,
                &AccessKeyLookup::default(),
                &SessionLookup::default(),
            )
            .expect_err("malformed authorization header should fail");

        assert_eq!(error.code(), "IncompleteSignature");
    }

    #[test]
    fn auth_rejects_credential_scope_date_mismatches() {
        let request = RequestAuth::new(
            "POST",
            "/",
            vec![
                RequestHeader::new("Host", "localhost"),
                RequestHeader::new("X-Amz-Date", "20260325T120000Z"),
                RequestHeader::new(
                    "Authorization",
                    "AWS4-HMAC-SHA256 Credential=test/20260324/eu-west-2/sts/aws4_request, SignedHeaders=host;x-amz-date, Signature=0000000000000000000000000000000000000000000000000000000000000000",
                ),
            ],
            b"",
        );

        let error = authenticator(1_742_905_600)
            .verify(
                &request,
                &AccessKeyLookup::default(),
                &SessionLookup::default(),
            )
            .expect_err("scope date mismatch should fail");

        assert_eq!(error.code(), "IncompleteSignature");
        assert!(error.message().contains("Credential scope date must match"));
    }

    #[test]
    fn auth_decodes_authorization_messages() {
        let decoded = decode_authorization_message("eyJhY2NvdW50IjoiMTIzNCJ9")
            .expect("valid base64 should decode");

        assert_eq!(decoded, "{\"account\":\"1234\"}");
    }

    #[test]
    fn auth_rejects_invalid_authorization_messages() {
        let error = decode_authorization_message("not base64!!!")
            .expect_err("invalid message should fail");

        assert_eq!(error.code(), "InvalidAuthorizationMessageException");
    }

    #[test]
    fn auth_canonical_query_normalizes_percent_encoded_components() {
        assert_eq!(
            canonical_query("prefix=reports%2F&list-type=2"),
            "list-type=2&prefix=reports%2F"
        );
    }

    #[test]
    fn auth_verifies_iam_user_access_keys() {
        let amz_date = "20260325T120000Z";
        let body = b"Action=ListUsers";
        let authorization = authorization_header(
            "AKIA0000000000000001",
            "user-secret-key",
            "20260325",
            amz_date,
            "iam",
            &["content-type", "host", "x-amz-date"],
            body,
        );
        let request = RequestAuth::new(
            "POST",
            "/",
            vec![
                RequestHeader::new("Host", "localhost"),
                RequestHeader::new(
                    "Content-Type",
                    "application/x-www-form-urlencoded",
                ),
                RequestHeader::new("X-Amz-Date", amz_date),
                RequestHeader::new("Authorization", &authorization),
            ],
            body,
        );
        let access_key_lookup = AccessKeyLookup {
            record: Some(IamAccessKeyRecord {
                access_key_id: "AKIA0000000000000001".to_owned(),
                account_id: "123456789012"
                    .parse::<AccountId>()
                    .expect("account id should parse"),
                create_date: "2026-03-25T12:00:00Z".to_owned(),
                region: "eu-west-2".parse().expect("region should parse"),
                secret_access_key: "user-secret-key".to_owned(),
                status: IamAccessKeyStatus::Active,
                user_arn: "arn:aws:iam::123456789012:user/alice"
                    .parse::<Arn>()
                    .expect("user ARN should parse"),
                user_id: "AIDA1234567890EXAMPLE".to_owned(),
                user_name: "alice".to_owned(),
            }),
        };

        let verified = authenticator(1_742_905_600)
            .verify(&request, &access_key_lookup, &SessionLookup::default())
            .expect("signature should verify")
            .expect("signed request should authenticate");

        assert_eq!(
            verified.caller_identity().arn().to_string(),
            "arn:aws:iam::123456789012:user/alice"
        );
        assert_eq!(
            verified.caller_identity().principal_id(),
            "AIDA1234567890EXAMPLE"
        );
    }

    #[test]
    fn auth_verifies_presigned_requests() {
        let path = s3_presigned_get_path("/demo/object.txt", 300);
        let request = RequestAuth::new(
            "GET",
            &path,
            vec![RequestHeader::new("Host", "localhost")],
            b"",
        );

        let verified = authenticator(1_742_905_750)
            .verify(
                &request,
                &AccessKeyLookup::default(),
                &SessionLookup::default(),
            )
            .expect("pre-signed signature should verify")
            .expect("pre-signed request should authenticate");

        assert_eq!(verified.account_id().as_str(), "000000000000");
        assert_eq!(verified.scope().service().as_str(), "s3");
    }

    #[test]
    fn auth_rejects_expired_presigned_requests() {
        let path = s3_presigned_get_path("/demo/object.txt", 2);
        let request = RequestAuth::new(
            "GET",
            &path,
            vec![RequestHeader::new("Host", "localhost")],
            b"",
        );

        let error = authenticator(1_800_000_000)
            .verify(
                &request,
                &AccessKeyLookup::default(),
                &SessionLookup::default(),
            )
            .expect_err("expired pre-signed request should fail");

        assert_eq!(error.code(), "AccessDenied");
        assert_eq!(error.status_code(), 403);
        assert_eq!(error.message(), "Request has expired.");
    }
}
