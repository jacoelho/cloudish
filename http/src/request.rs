use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct HttpRequest<'a> {
    method: &'a str,
    path: &'a str,
    headers: Vec<Header<'a>>,
    body: &'a [u8],
}

impl<'a> HttpRequest<'a> {
    pub(crate) fn parse(request: &'a [u8]) -> Result<Self, RequestParseError> {
        let header_end = request
            .windows(4)
            .position(|window| window == b"\r\n\r\n")
            .map(|index| index + 4)
            .unwrap_or(request.len());
        let (head, body) = request.split_at(header_end);
        let head = if head.ends_with(b"\r\n\r\n") {
            head.get(..head.len().saturating_sub(4)).unwrap_or_default()
        } else {
            head
        };
        let head = std::str::from_utf8(head)
            .map_err(|_| RequestParseError::InvalidEncoding)?;
        let mut lines = head.split("\r\n");
        let request_line = lines
            .next()
            .filter(|line| !line.trim().is_empty())
            .ok_or(RequestParseError::MissingRequestLine)?;
        let mut parts = request_line.split_whitespace();
        let method =
            parts.next().ok_or(RequestParseError::InvalidRequestLine)?;
        let path =
            parts.next().ok_or(RequestParseError::InvalidRequestLine)?;
        let version =
            parts.next().ok_or(RequestParseError::InvalidRequestLine)?;

        if parts.next().is_some() || !version.starts_with("HTTP/1.") {
            return Err(RequestParseError::InvalidRequestLine);
        }

        let mut parsed_headers = Vec::new();
        for line in lines.filter(|line| !line.is_empty()) {
            let (name, value) = line
                .split_once(':')
                .ok_or(RequestParseError::InvalidHeaderLine)?;
            parsed_headers
                .push(Header { name: name.trim(), value: value.trim() });
        }

        Ok(Self { method, path, headers: parsed_headers, body })
    }

    pub(crate) fn method(&self) -> &str {
        self.method
    }

    pub(crate) fn path_without_query(&self) -> &str {
        self.path.split('?').next().unwrap_or(self.path)
    }

    pub(crate) fn path(&self) -> &str {
        self.path
    }

    pub(crate) fn query_string(&self) -> Option<&str> {
        self.path.split_once('?').map(|(_, query)| query)
    }

    pub(crate) fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|header| header.name.eq_ignore_ascii_case(name))
            .map(|header| header.value)
    }

    pub(crate) fn body(&self) -> &[u8] {
        self.body
    }

    pub(crate) fn headers(&self) -> impl Iterator<Item = (&str, &str)> {
        self.headers.iter().map(|header| (header.name, header.value))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Header<'a> {
    name: &'a str,
    value: &'a str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RequestParseError {
    InvalidEncoding,
    InvalidHeaderLine,
    InvalidRequestLine,
    MissingRequestLine,
}

impl fmt::Display for RequestParseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidEncoding => {
                formatter.write_str("request headers are not valid UTF-8")
            }
            Self::InvalidHeaderLine => formatter
                .write_str("request headers must be NAME: VALUE pairs"),
            Self::InvalidRequestLine => formatter
                .write_str("request line must be METHOD PATH HTTP/1.x"),
            Self::MissingRequestLine => {
                formatter.write_str("request is empty")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{HttpRequest, RequestParseError};

    #[test]
    fn parse_http_request_with_headers_and_body() {
        let request = HttpRequest::parse(
            b"POST / HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/x-amz-json-1.0\r\n\r\n{}",
        )
        .expect("request should parse");

        assert_eq!(request.method(), "POST");
        assert_eq!(request.path_without_query(), "/");
        assert_eq!(
            request.header("content-type"),
            Some("application/x-amz-json-1.0")
        );
        assert_eq!(request.body(), b"{}");
    }

    #[test]
    fn parse_request_without_header_terminator() {
        let request = HttpRequest::parse(
            b"GET /__cloudish/health HTTP/1.1\r\nHost: localhost",
        )
        .expect("request should parse");

        assert_eq!(request.method(), "GET");
        assert_eq!(request.header("host"), Some("localhost"));
        assert!(request.body().is_empty());
    }

    #[test]
    fn reject_invalid_request_line() {
        let error = HttpRequest::parse(b"THIS IS NOT HTTP")
            .expect_err("invalid request line should fail");

        assert_eq!(error, RequestParseError::InvalidRequestLine);
        assert_eq!(
            error.to_string(),
            "request line must be METHOD PATH HTTP/1.x"
        );
    }

    #[test]
    fn reject_invalid_header_line() {
        let error = HttpRequest::parse(b"GET / HTTP/1.1\r\nInvalid\r\n\r\n")
            .expect_err("invalid header should fail");

        assert_eq!(error, RequestParseError::InvalidHeaderLine);
    }

    #[test]
    fn reject_invalid_header_encoding() {
        let error =
            HttpRequest::parse(b"GET / HTTP/1.1\r\nHost: \xff\r\n\r\n")
                .expect_err("invalid header encoding should fail");

        assert_eq!(error, RequestParseError::InvalidEncoding);
    }

    #[test]
    fn reject_empty_request() {
        let error =
            HttpRequest::parse(b"").expect_err("empty request should fail");

        assert_eq!(error, RequestParseError::MissingRequestLine);
        assert_eq!(error.to_string(), "request is empty");
    }
}
