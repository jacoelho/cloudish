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
            reason_phrase(self.status_code)
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

fn reason_phrase(status_code: u16) -> &'static str {
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
        500 => "Internal Server Error",
        501 => "Not Implemented",
        503 => "Service Unavailable",
        _ => "Unknown",
    }
}
