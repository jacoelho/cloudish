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
use jsonwebtoken::{
    Algorithm, DecodingKey, Validation, decode, decode_header,
};
use serde_json::Value;

pub fn verify_token_with_jwks(
    token: &str,
    issuer: &str,
    jwks: &Value,
) -> Value {
    let header = decode_header(token).expect("token header should decode");
    let kid = header.kid.expect("token should include a key id");
    let key = jwks["keys"]
        .as_array()
        .and_then(|keys| {
            keys.iter().find(|key| key["kid"].as_str() == Some(kid.as_str()))
        })
        .expect("jwks should expose the token key");
    let modulus =
        key["n"].as_str().expect("jwks key should include an RSA modulus");
    let exponent =
        key["e"].as_str().expect("jwks key should include an RSA exponent");
    let decoding_key = DecodingKey::from_rsa_components(modulus, exponent)
        .expect("jwks key should build a decoding key");
    let mut validation = Validation::new(Algorithm::RS256);
    validation.set_issuer(&[issuer]);
    validation.validate_aud = false;

    decode::<Value>(token, &decoding_key, &validation)
        .expect("token should validate against the jwks")
        .claims
}
