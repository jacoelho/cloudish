use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EdgeRequest {
    method: String,
    path: String,
    headers: Vec<Header>,
    body: Vec<u8>,
    source_ip: Option<String>,
}

pub type HttpRequest<'a> = EdgeRequest;

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

    /// Parses a raw HTTP/1.x request into the edge request representation.
    ///
    /// # Errors
    ///
    /// Returns `RequestParseError` when the request bytes are not valid UTF-8,
    /// the request line is missing or malformed, or any header line does not
    /// use the `name:value` form.
    pub fn parse(request: &[u8]) -> Result<Self, RequestParseError> {
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
pub enum RequestParseError {
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
