use httpdate::fmt_http_date;
use serde_json::Value;
use std::time::SystemTime;

#[derive(Debug, Clone)]
pub struct EdgeResponse {
    status_code: u16,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl EdgeResponse {
    pub fn bytes(status_code: u16, content_type: &str, body: Vec<u8>) -> Self {
        Self {
            status_code,
            headers: vec![
                ("Date".to_owned(), fmt_http_date(SystemTime::now())),
                ("Content-Type".to_owned(), content_type.to_owned()),
                ("Content-Length".to_owned(), body.len().to_string()),
                ("Connection".to_owned(), "close".to_owned()),
            ],
            body,
        }
    }

    pub fn json(status_code: u16, body: Value) -> Self {
        match serde_json::to_vec(&body) {
            Ok(body) => Self::bytes(status_code, "application/json", body),
            Err(error) => Self::bytes(
                500,
                "application/json",
                format!(
                    "{{\"message\":\"Failed to serialize JSON response: {error}\"}}"
                )
                .into_bytes(),
            ),
        }
    }

    pub fn with_header(mut self, name: &str, value: &str) -> Self {
        self.headers.push((name.to_owned(), value.to_owned()));
        self
    }

    pub fn set_header(
        mut self,
        name: impl Into<String>,
        value: impl Into<String>,
    ) -> Self {
        let name = name.into();
        let value = value.into();

        if let Some((_, existing_value)) =
            self.headers.iter_mut().find(|(existing_name, _)| {
                existing_name.eq_ignore_ascii_case(&name)
            })
        {
            *existing_value = value;
            return self;
        }

        self.headers.push((name, value));
        self
    }

    pub fn to_http_bytes(&self) -> Vec<u8> {
        let mut response = format!(
            "HTTP/1.1 {} {}\r\n",
            self.status_code,
            http_reason_phrase(self.status_code)
        )
        .into_bytes();

        for (name, value) in &self.headers {
            response.extend_from_slice(name.as_bytes());
            response.extend_from_slice(b": ");
            response.extend_from_slice(value.as_bytes());
            response.extend_from_slice(b"\r\n");
        }

        response.extend_from_slice(b"\r\n");
        response.extend_from_slice(&self.body);
        response
    }

    pub fn into_parts(self) -> (u16, Vec<(String, String)>, Vec<u8>) {
        (self.status_code, self.headers, self.body)
    }
}

pub fn http_reason_phrase(status_code: u16) -> &'static str {
    match status_code {
        200 => "OK",
        201 => "Created",
        202 => "Accepted",
        204 => "No Content",
        301 => "Moved Permanently",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        409 => "Conflict",
        412 => "Precondition Failed",
        413 => "Payload Too Large",
        415 => "Unsupported Media Type",
        429 => "Too Many Requests",
        500 => "Internal Server Error",
        501 => "Not Implemented",
        502 => "Bad Gateway",
        503 => "Service Unavailable",
        _ => "Unknown",
    }
}

#[cfg(test)]
mod tests {
    use super::EdgeResponse;

    fn status_line(response: &[u8]) -> &str {
        let line_end = response
            .windows(2)
            .position(|window| window == b"\r\n")
            .expect("response should contain a status line terminator");
        std::str::from_utf8(&response[..line_end])
            .expect("status line should be UTF-8")
    }

    #[test]
    fn edge_response_serializes_extended_reason_phrases() {
        let cases = [
            (412, "HTTP/1.1 412 Precondition Failed"),
            (413, "HTTP/1.1 413 Payload Too Large"),
            (415, "HTTP/1.1 415 Unsupported Media Type"),
            (429, "HTTP/1.1 429 Too Many Requests"),
            (502, "HTTP/1.1 502 Bad Gateway"),
        ];

        for (status_code, expected_status_line) in cases {
            let response =
                EdgeResponse::bytes(status_code, "text/plain", Vec::new());
            assert_eq!(
                status_line(&response.to_http_bytes()),
                expected_status_line
            );
        }
    }
}
