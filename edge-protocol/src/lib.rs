#![cfg_attr(
    test,
    allow(
        clippy::unreachable,
        clippy::assertions_on_constants,
        clippy::missing_panics_doc,
        clippy::missing_errors_doc
    )
)]

mod request;
mod response;

pub use request::{EdgeRequest, HttpRequest, RequestParseError};
pub use response::{EdgeResponse, http_reason_phrase};
