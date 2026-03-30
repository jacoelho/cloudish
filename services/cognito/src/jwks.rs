use serde::Serialize;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CognitoJwksDocument {
    pub keys: Vec<CognitoJwk>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CognitoJwk {
    pub alg: String,
    pub e: String,
    pub kid: String,
    pub kty: String,
    pub n: String,
    #[serde(rename = "use")]
    pub use_: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CognitoOpenIdConfiguration {
    pub claims_supported: Vec<String>,
    pub id_token_signing_alg_values_supported: Vec<String>,
    pub issuer: String,
    pub jwks_uri: String,
    pub subject_types_supported: Vec<String>,
}
