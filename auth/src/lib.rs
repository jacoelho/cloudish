#![forbid(unsafe_code)]
#![cfg_attr(
    test,
    allow(
        clippy::expect_used,
        clippy::unwrap_used,
        clippy::panic,
        clippy::unreachable,
        clippy::indexing_slicing,
        clippy::assertions_on_constants,
        clippy::missing_panics_doc,
        clippy::missing_errors_doc
    )
)]

mod sigv4;

pub use sigv4::{
    Authenticator, BOOTSTRAP_ACCESS_KEY_ID, BOOTSTRAP_SECRET_ACCESS_KEY,
    RequestAuth, RequestHeader, SessionAttributes, VerifiedRequest,
    decode_authorization_message,
};
