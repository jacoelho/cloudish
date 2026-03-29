use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EdgeRequest {
    method: String,
    path: String,
    headers: Vec<Header>,
    body: Vec<u8>,
    source_ip: Option<String>,
}

pub(crate) type HttpRequest<'a> = EdgeRequest;

impl EdgeRequest {
    pub fn new(
        method: impl Into<String>,
        path: impl Into<String>,
        headers: Vec<(String, String)>,
        body: Vec<u8>,
    ) -> Self {
        Self {
            method: method.into(),
            path: path.into(),
            headers: headers
                .into_iter()
                .map(|(name, value)| Header { name, value })
                .collect(),
            body,
            source_ip: None,
        }
    }

    pub(crate) fn parse(request: &[u8]) -> Result<Self, RequestParseError> {
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
            parsed_headers.push(Header {
                name: name.trim().to_owned(),
                value: value.trim().to_owned(),
            });
        }

        Ok(Self {
            method: method.to_owned(),
            path: path.to_owned(),
            headers: parsed_headers,
            body: body.to_vec(),
            source_ip: None,
        })
    }

    pub fn method(&self) -> &str {
        &self.method
    }

    pub fn path_without_query(&self) -> &str {
        self.path.split('?').next().unwrap_or(&self.path)
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    pub fn query_string(&self) -> Option<&str> {
        self.path.split_once('?').map(|(_, query)| query)
    }

    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|header| header.name.eq_ignore_ascii_case(name))
            .map(|header| header.value.as_str())
    }

    pub fn body(&self) -> &[u8] {
        &self.body
    }

    pub fn source_ip(&self) -> Option<&str> {
        self.source_ip.as_deref()
    }

    pub fn set_body(&mut self, body: Vec<u8>) {
        self.body = body;
    }

    pub fn set_source_ip(&mut self, source_ip: Option<String>) {
        self.source_ip = source_ip;
    }

    pub fn with_source_ip(mut self, source_ip: impl Into<String>) -> Self {
        self.source_ip = Some(source_ip.into());
        self
    }

    pub fn headers(&self) -> impl Iterator<Item = (&str, &str)> {
        self.headers
            .iter()
            .map(|header| (header.name.as_str(), header.value.as_str()))
    }

    pub fn append_header(
        &mut self,
        name: impl Into<String>,
        value: impl Into<String>,
    ) {
        self.headers.push(Header { name: name.into(), value: value.into() });
    }

    pub fn set_header(
        &mut self,
        name: impl Into<String>,
        value: impl Into<String>,
    ) {
        let name = name.into();
        let value = value.into();

        if let Some(existing) = self
            .headers
            .iter_mut()
            .find(|header| header.name.eq_ignore_ascii_case(&name))
        {
            existing.name = name;
            existing.value = value;
            return;
        }

        self.headers.push(Header { name, value });
    }

    pub fn remove_header(&mut self, name: &str) {
        self.headers.retain(|header| !header.name.eq_ignore_ascii_case(name));
    }

    pub fn header_values(&self, name: &str) -> Vec<&str> {
        self.headers
            .iter()
            .filter(|header| header.name.eq_ignore_ascii_case(name))
            .map(|header| header.value.as_str())
            .collect()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Header {
    name: String,
    value: String,
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
    use super::{EdgeRequest, RequestParseError};

    #[test]
    fn parse_http_request_with_headers_and_body() {
        let request = EdgeRequest::parse(
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
        let request = EdgeRequest::parse(
            b"GET /__cloudish/health HTTP/1.1\r\nHost: localhost",
        )
        .expect("request should parse");

        assert_eq!(request.method(), "GET");
        assert_eq!(request.header("host"), Some("localhost"));
        assert!(request.body().is_empty());
    }

    #[test]
    fn reject_invalid_request_line() {
        let error = EdgeRequest::parse(b"THIS IS NOT HTTP")
            .expect_err("invalid request line should fail");

        assert_eq!(error, RequestParseError::InvalidRequestLine);
        assert_eq!(
            error.to_string(),
            "request line must be METHOD PATH HTTP/1.x"
        );
    }

    #[test]
    fn reject_invalid_header_line() {
        let error = EdgeRequest::parse(b"GET / HTTP/1.1\r\nInvalid\r\n\r\n")
            .expect_err("invalid header should fail");

        assert_eq!(error, RequestParseError::InvalidHeaderLine);
    }

    #[test]
    fn reject_invalid_header_encoding() {
        let error =
            EdgeRequest::parse(b"GET / HTTP/1.1\r\nHost: \xff\r\n\r\n")
                .expect_err("invalid header encoding should fail");

        assert_eq!(error, RequestParseError::InvalidEncoding);
    }

    #[test]
    fn reject_empty_request() {
        let error =
            EdgeRequest::parse(b"").expect_err("empty request should fail");

        assert_eq!(error, RequestParseError::MissingRequestLine);
        assert_eq!(error.to_string(), "request is empty");
    }

    #[test]
    fn update_headers_and_body() {
        let mut request = EdgeRequest::new(
            "PUT",
            "/bucket/key",
            vec![("Content-Encoding".to_owned(), "aws-chunked".to_owned())],
            b"encoded".to_vec(),
        );

        request.set_header("Content-Encoding", "gzip");
        request.append_header("x-amz-meta-trace", "abc123");
        request.set_body(b"decoded".to_vec());
        request.remove_header("x-amz-missing");

        assert_eq!(request.header("content-encoding"), Some("gzip"));
        assert_eq!(request.header_values("x-amz-meta-trace"), vec!["abc123"]);
        assert_eq!(request.body(), b"decoded");
    }

    #[test]
    fn edge_request_preserves_attached_source_ip_and_defaults_parsed_requests_to_none()
     {
        let attached = EdgeRequest::new(
            "GET",
            "/",
            vec![("Host".to_owned(), "localhost".to_owned())],
            Vec::new(),
        )
        .with_source_ip("203.0.113.10");
        let parsed =
            EdgeRequest::parse(b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
                .expect("request should parse");

        assert_eq!(attached.source_ip(), Some("203.0.113.10"));
        assert_eq!(parsed.source_ip(), None);
    }
}
