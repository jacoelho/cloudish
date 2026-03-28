use aws::{AwsError, AwsErrorFamily};
use std::collections::BTreeMap;

#[derive(Debug, Default)]
pub(crate) struct QueryParameters {
    values: BTreeMap<String, String>,
}

impl QueryParameters {
    pub(crate) fn parse(body: &[u8]) -> Result<Self, AwsError> {
        let body =
            std::str::from_utf8(body).map_err(|_| malformed_query_error())?;
        let mut values = BTreeMap::new();

        for pair in body.split('&').filter(|pair| !pair.is_empty()) {
            let (name, value) = pair.split_once('=').unwrap_or((pair, ""));
            values.insert(percent_decode(name)?, percent_decode(value)?);
        }

        Ok(Self { values })
    }

    pub(crate) fn action(&self) -> Option<&str> {
        self.optional("Action")
    }

    pub(crate) fn required(&self, name: &str) -> Result<&str, AwsError> {
        self.optional(name).ok_or_else(|| missing_parameter_error(name))
    }

    pub(crate) fn optional(&self, name: &str) -> Option<&str> {
        self.values.get(name).map(String::as_str)
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.values.iter().map(|(name, value)| (name.as_str(), value.as_str()))
    }
}

#[cfg(test)]
pub(crate) fn parse_action(body: &[u8]) -> Result<Option<String>, AwsError> {
    Ok(QueryParameters::parse(body)?.action().map(str::to_owned))
}

pub(crate) fn missing_action_error() -> AwsError {
    AwsError::trusted_custom(
        AwsErrorFamily::MissingParameter,
        "MissingAction",
        "Query requests must include an Action parameter.",
        400,
        true,
    )
}

pub(crate) fn malformed_query_error() -> AwsError {
    AwsError::trusted_custom(
        AwsErrorFamily::Validation,
        "MalformedQueryString",
        "Query request body is not valid application/x-www-form-urlencoded data.",
        400,
        true,
    )
}

pub(crate) fn missing_parameter_error(name: &str) -> AwsError {
    AwsError::trusted_custom(
        AwsErrorFamily::MissingParameter,
        "MissingParameter",
        format!("The request must contain the parameter {name}."),
        400,
        true,
    )
}

fn percent_decode(value: &str) -> Result<String, AwsError> {
    let mut decoded = Vec::with_capacity(value.len());
    let bytes = value.as_bytes();
    let mut index = 0;

    while let Some(&byte) = bytes.get(index) {
        match byte {
            b'+' => {
                decoded.push(b' ');
                index += 1;
            }
            b'%' => {
                let Some(&high_byte) = bytes.get(index + 1) else {
                    return Err(malformed_query_error());
                };
                let Some(&low_byte) = bytes.get(index + 2) else {
                    return Err(malformed_query_error());
                };
                let high = hex_value(high_byte)?;
                let low = hex_value(low_byte)?;
                decoded.push((high << 4) | low);
                index += 3;
            }
            other => {
                decoded.push(other);
                index += 1;
            }
        }
    }

    String::from_utf8(decoded).map_err(|_| malformed_query_error())
}

fn hex_value(byte: u8) -> Result<u8, AwsError> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(malformed_query_error()),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        QueryParameters, malformed_query_error, missing_action_error,
        parse_action,
    };

    #[test]
    fn query_parameters_decode_urlencoded_pairs() {
        let params = QueryParameters::parse(
            b"Action=GetCallerIdentity&RoleSessionName=team%2Fsession",
        )
        .expect("query parameters should parse");

        assert_eq!(params.action(), Some("GetCallerIdentity"));
        assert_eq!(params.optional("RoleSessionName"), Some("team/session"));
        assert_eq!(
            params.iter().collect::<Vec<_>>(),
            vec![
                ("Action", "GetCallerIdentity"),
                ("RoleSessionName", "team/session"),
            ]
        );
    }

    #[test]
    fn query_parameters_report_missing_fields() {
        let params = QueryParameters::parse(b"Action=GetCallerIdentity")
            .expect("query parameters should parse");
        let error =
            params.required("RoleArn").expect_err("missing field should fail");

        assert_eq!(error.code(), "MissingParameter");
        assert_eq!(
            error.message(),
            "The request must contain the parameter RoleArn."
        );
    }

    #[test]
    fn query_action_parser_reports_malformed_payloads() {
        let error = parse_action(b"Action=%zz")
            .expect_err("malformed query payload should fail");

        assert_eq!(error, malformed_query_error());
        assert_eq!(missing_action_error().code(), "MissingAction");
    }
}
