use crate::errors::RdsError;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(try_from = "String", into = "String")]
pub struct DbInstanceIdentifier(String);

impl DbInstanceIdentifier {
    /// # Errors
    ///
    /// Returns [`RdsError::InvalidParameterValue`] when the identifier is
    /// blank, too long, or contains unsupported characters.
    pub fn new(value: impl AsRef<str>) -> Result<Self, RdsError> {
        normalize_identifier("DBInstanceIdentifier", value.as_ref()).map(Self)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for DbInstanceIdentifier {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Display for DbInstanceIdentifier {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl From<DbInstanceIdentifier> for String {
    fn from(value: DbInstanceIdentifier) -> Self {
        value.0
    }
}

impl TryFrom<String> for DbInstanceIdentifier {
    type Error = RdsError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl PartialEq<&str> for DbInstanceIdentifier {
    fn eq(&self, other: &&str) -> bool {
        self.as_str() == *other
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(try_from = "String", into = "String")]
pub struct DbClusterIdentifier(String);

impl DbClusterIdentifier {
    /// # Errors
    ///
    /// Returns [`RdsError::InvalidParameterValue`] when the identifier is
    /// blank, too long, or contains unsupported characters.
    pub fn new(value: impl AsRef<str>) -> Result<Self, RdsError> {
        normalize_identifier("DBClusterIdentifier", value.as_ref()).map(Self)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for DbClusterIdentifier {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Display for DbClusterIdentifier {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl From<DbClusterIdentifier> for String {
    fn from(value: DbClusterIdentifier) -> Self {
        value.0
    }
}

impl TryFrom<String> for DbClusterIdentifier {
    type Error = RdsError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl PartialEq<&str> for DbClusterIdentifier {
    fn eq(&self, other: &&str) -> bool {
        self.as_str() == *other
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(try_from = "String", into = "String")]
pub struct DbParameterGroupName(String);

impl DbParameterGroupName {
    /// # Errors
    ///
    /// Returns [`RdsError::InvalidParameterValue`] when the name is blank.
    pub fn new(value: impl AsRef<str>) -> Result<Self, RdsError> {
        normalize_non_empty(
            "DBParameterGroupName must not be blank.",
            value.as_ref(),
        )
        .map(Self)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for DbParameterGroupName {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Display for DbParameterGroupName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl From<DbParameterGroupName> for String {
    fn from(value: DbParameterGroupName) -> Self {
        value.0
    }
}

impl TryFrom<String> for DbParameterGroupName {
    type Error = RdsError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl PartialEq<&str> for DbParameterGroupName {
    fn eq(&self, other: &&str) -> bool {
        self.as_str() == *other
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(try_from = "String", into = "String")]
pub struct DbParameterGroupFamily(String);

impl DbParameterGroupFamily {
    /// # Errors
    ///
    /// Returns [`RdsError::InvalidParameterValue`] when the family is blank.
    pub fn new(value: impl AsRef<str>) -> Result<Self, RdsError> {
        normalize_non_empty(
            "DBParameterGroupFamily must not be blank.",
            value.as_ref(),
        )
        .map(Self)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for DbParameterGroupFamily {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Display for DbParameterGroupFamily {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl From<DbParameterGroupFamily> for String {
    fn from(value: DbParameterGroupFamily) -> Self {
        value.0
    }
}

impl TryFrom<String> for DbParameterGroupFamily {
    type Error = RdsError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl PartialEq<&str> for DbParameterGroupFamily {
    fn eq(&self, other: &&str) -> bool {
        self.as_str() == *other
    }
}

fn normalize_identifier(name: &str, value: &str) -> Result<String, RdsError> {
    let value = value.trim().to_ascii_lowercase();
    if value.is_empty() || value.len() > 63 {
        return Err(RdsError::InvalidParameterValue {
            message: format!("{name} must be between 1 and 63 characters."),
        });
    }
    if !value.chars().all(|character| {
        character.is_ascii_lowercase()
            || character.is_ascii_digit()
            || character == '-'
    }) {
        return Err(RdsError::InvalidParameterValue {
            message: format!(
                "{name} must contain only lowercase letters, digits, and hyphens."
            ),
        });
    }
    if value.starts_with('-')
        || value.ends_with('-')
        || value.contains("--")
        || !value
            .chars()
            .next()
            .is_some_and(|character| character.is_ascii_alphabetic())
    {
        return Err(RdsError::InvalidParameterValue {
            message: format!(
                "{name} must start with a letter, must not end with a hyphen, \
                 and must not contain consecutive hyphens."
            ),
        });
    }

    Ok(value)
}

fn normalize_non_empty(
    message: &str,
    value: &str,
) -> Result<String, RdsError> {
    let value = value.trim().to_ascii_lowercase();
    if value.is_empty() {
        return Err(RdsError::InvalidParameterValue {
            message: message.to_owned(),
        });
    }

    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::{
        DbClusterIdentifier, DbInstanceIdentifier, DbParameterGroupFamily,
        DbParameterGroupName,
    };
    use crate::RdsError;

    #[test]
    fn rds_identifiers_normalize_and_round_trip() {
        let instance = DbInstanceIdentifier::new(" Demo-01 ")
            .expect("instance identifier should normalize");
        let cluster = DbClusterIdentifier::try_from("Cluster-01".to_owned())
            .expect("cluster identifier should convert from String");
        let parameter_group = DbParameterGroupName::new(" POSTGRES16 ")
            .expect("parameter group name should normalize");
        let family = DbParameterGroupFamily::new(" MYSQL8.0 ")
            .expect("parameter group family should normalize");

        assert_eq!(instance, "demo-01");
        assert_eq!(instance.as_ref(), "demo-01");
        assert_eq!(instance.to_string(), "demo-01");
        assert_eq!(String::from(instance), "demo-01".to_owned());
        assert_eq!(cluster, "cluster-01");
        assert_eq!(parameter_group, "postgres16");
        assert_eq!(family, "mysql8.0");
    }

    #[test]
    fn rds_identifiers_reject_invalid_shapes_and_blank_group_values() {
        let invalid_instance = DbInstanceIdentifier::new("1demo")
            .expect_err("instance identifiers should start with letters");
        let invalid_cluster = DbClusterIdentifier::new("demo--cluster")
            .expect_err(
                "cluster identifiers should reject consecutive hyphens",
            );
        let blank_group = DbParameterGroupName::new("   ")
            .expect_err("parameter group names should reject blanks");
        let blank_family = DbParameterGroupFamily::new("")
            .expect_err("parameter group families should reject blanks");

        assert_eq!(
            invalid_instance,
            RdsError::InvalidParameterValue {
                message:
                    "DBInstanceIdentifier must start with a letter, must not end with a hyphen, and must not contain consecutive hyphens."
                        .to_owned(),
            }
        );
        assert_eq!(
            invalid_cluster,
            RdsError::InvalidParameterValue {
                message:
                    "DBClusterIdentifier must start with a letter, must not end with a hyphen, and must not contain consecutive hyphens."
                        .to_owned(),
            }
        );
        assert_eq!(
            blank_group,
            RdsError::InvalidParameterValue {
                message: "DBParameterGroupName must not be blank.".to_owned(),
            }
        );
        assert_eq!(
            blank_family,
            RdsError::InvalidParameterValue {
                message: "DBParameterGroupFamily must not be blank."
                    .to_owned(),
            }
        );
    }
}
