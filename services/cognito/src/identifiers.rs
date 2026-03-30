use crate::CognitoError;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(try_from = "String", into = "String")]
pub struct CognitoUserPoolId(String);

impl CognitoUserPoolId {
    /// # Errors
    ///
    /// Returns an error when the identifier is blank or exceeds the supported
    /// Cognito identifier length.
    pub fn new(value: impl AsRef<str>) -> Result<Self, CognitoError> {
        normalize_identifier("UserPoolId", value.as_ref()).map(Self)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for CognitoUserPoolId {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Display for CognitoUserPoolId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl PartialEq<&str> for CognitoUserPoolId {
    fn eq(&self, other: &&str) -> bool {
        self.as_str() == *other
    }
}

impl PartialEq<String> for CognitoUserPoolId {
    fn eq(&self, other: &String) -> bool {
        self.as_str() == other
    }
}

impl From<CognitoUserPoolId> for String {
    fn from(value: CognitoUserPoolId) -> Self {
        value.0
    }
}

impl TryFrom<String> for CognitoUserPoolId {
    type Error = CognitoError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(try_from = "String", into = "String")]
pub struct CognitoUserPoolClientId(String);

impl CognitoUserPoolClientId {
    /// # Errors
    ///
    /// Returns an error when the identifier is blank or exceeds the supported
    /// Cognito identifier length.
    pub fn new(value: impl AsRef<str>) -> Result<Self, CognitoError> {
        normalize_identifier("ClientId", value.as_ref()).map(Self)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for CognitoUserPoolClientId {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Display for CognitoUserPoolClientId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl PartialEq<&str> for CognitoUserPoolClientId {
    fn eq(&self, other: &&str) -> bool {
        self.as_str() == *other
    }
}

impl PartialEq<String> for CognitoUserPoolClientId {
    fn eq(&self, other: &String) -> bool {
        self.as_str() == other
    }
}

impl From<CognitoUserPoolClientId> for String {
    fn from(value: CognitoUserPoolClientId) -> Self {
        value.0
    }
}

impl TryFrom<String> for CognitoUserPoolClientId {
    type Error = CognitoError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

fn normalize_identifier(
    field_name: &str,
    value: &str,
) -> Result<String, CognitoError> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(CognitoError::invalid_parameter(format!(
            "{field_name} must not be blank."
        )));
    }

    Ok(trimmed.to_owned())
}

#[cfg(test)]
mod tests {
    use super::{CognitoUserPoolClientId, CognitoUserPoolId};

    #[test]
    fn cognito_identifiers_round_trip_through_typed_wrappers() {
        let user_pool_id = CognitoUserPoolId::new("eu-west-2_demo")
            .expect("user pool id should build");
        let client_id =
            CognitoUserPoolClientId::try_from("client123".to_owned())
                .expect("client id should convert from String");

        assert_eq!(user_pool_id, "eu-west-2_demo");
        assert_eq!(user_pool_id, "eu-west-2_demo".to_owned());
        assert_eq!(user_pool_id.as_ref(), "eu-west-2_demo");
        assert_eq!(user_pool_id.to_string(), "eu-west-2_demo");
        assert_eq!(String::from(user_pool_id), "eu-west-2_demo".to_owned());
        assert_eq!(client_id, "client123");
        assert_eq!(client_id, "client123".to_owned());
    }

    #[test]
    fn cognito_identifiers_reject_blank_values() {
        let user_pool_error = CognitoUserPoolId::new("   ")
            .expect_err("user pool ids should reject blanks");
        let client_error = CognitoUserPoolClientId::new("")
            .expect_err("client ids should reject blanks");

        assert_eq!(
            user_pool_error.to_string(),
            "UserPoolId must not be blank."
        );
        assert_eq!(client_error.to_string(), "ClientId must not be blank.");
    }
}
