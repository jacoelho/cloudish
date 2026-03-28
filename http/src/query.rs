use crate::request::HttpRequest;
use aws::{AwsError, AwsErrorFamily};
use std::collections::BTreeMap;

#[derive(Debug, Default)]
pub(crate) struct QueryParameters {
    values: BTreeMap<String, String>,
}

#[derive(Debug)]
pub(crate) struct ParsedQueryRequest<'a> {
    raw_parameters: &'a [u8],
    parameters: QueryParameters,
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

impl<'a> ParsedQueryRequest<'a> {
    pub(crate) fn raw_parameters(&self) -> &'a [u8] {
        self.raw_parameters
    }

    pub(crate) fn parameters(&self) -> &QueryParameters {
        &self.parameters
    }
}

pub(crate) fn is_query_request(request: &HttpRequest<'_>) -> bool {
    query_parameter_source(request).is_some()
}

pub(crate) fn parse_request<'a>(
    request: &'a HttpRequest<'_>,
) -> Result<Option<ParsedQueryRequest<'a>>, AwsError> {
    let Some(raw_parameters) = query_parameter_source(request) else {
        return Ok(None);
    };

    Ok(Some(ParsedQueryRequest {
        raw_parameters,
        parameters: QueryParameters::parse(raw_parameters)?,
    }))
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

fn query_parameter_source<'a>(
    request: &'a HttpRequest<'_>,
) -> Option<&'a [u8]> {
    if request.method() == "POST"
        && content_type_is_form_urlencoded(
            request.header("content-type").unwrap_or_default(),
        )
        && !request.body().is_empty()
    {
        return Some(request.body());
    }

    if let Some(query_string) = request.query_string()
        && looks_like_query_parameters(query_string.as_bytes())
    {
        return Some(query_string.as_bytes());
    }

    if request.method() == "POST"
        && looks_like_query_parameters(request.body())
    {
        return Some(request.body());
    }

    None
}

fn content_type_is_form_urlencoded(content_type: &str) -> bool {
    content_type.split(';').next().is_some_and(|media_type| {
        media_type
            .trim()
            .eq_ignore_ascii_case("application/x-www-form-urlencoded")
    })
}

fn looks_like_query_parameters(parameters: &[u8]) -> bool {
    parameters.windows(7).any(|window| window == b"Action=")
        || parameters.windows(8).any(|window| window == b"Version=")
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
        QueryParameters, is_query_request, malformed_query_error,
        missing_action_error, parse_action, parse_request,
    };
    use crate::request::EdgeRequest;

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

    #[test]
    fn query_request_parser_uses_uri_parameters_for_get_requests() {
        let request = EdgeRequest::new(
            "GET",
            "/?Action=GetCallerIdentity&Version=2011-06-15",
            Vec::new(),
            Vec::new(),
        );
        let parsed = parse_request(&request)
            .expect("GET query parameters should parse")
            .expect("GET request should be recognized as Query");

        assert!(is_query_request(&request));
        assert_eq!(parsed.parameters().action(), Some("GetCallerIdentity"));
        assert_eq!(
            parsed
                .parameters()
                .required("Version")
                .expect("Version should exist"),
            "2011-06-15"
        );
        assert_eq!(
            parsed.raw_parameters(),
            b"Action=GetCallerIdentity&Version=2011-06-15"
        );
    }

    #[test]
    fn query_request_parser_prefers_form_body_over_uri_for_post_requests() {
        let request = EdgeRequest::new(
            "POST",
            "/?Action=GetCallerIdentity&Version=2011-06-15",
            vec![(
                "Content-Type".to_owned(),
                "application/x-www-form-urlencoded".to_owned(),
            )],
            b"Action=CreateQueue&QueueName=demo".to_vec(),
        );
        let parsed = parse_request(&request)
            .expect("POST query parameters should parse")
            .expect("POST request should be recognized as Query");

        assert_eq!(parsed.parameters().action(), Some("CreateQueue"));
        assert_eq!(parsed.parameters().optional("QueueName"), Some("demo"));
        assert_eq!(
            parsed.raw_parameters(),
            b"Action=CreateQueue&QueueName=demo"
        );
    }

    #[test]
    fn query_request_parser_reports_malformed_uri_payloads() {
        let request =
            EdgeRequest::new("GET", "/?Action=%zz", Vec::new(), Vec::new());
        let error = parse_request(&request)
            .expect_err("malformed URI query should fail");

        assert_eq!(error, malformed_query_error());
    }
}
