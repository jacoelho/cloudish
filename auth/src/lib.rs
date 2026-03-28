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

mod aws_chunked;
mod sigv4;

pub use aws_chunked::{
    AwsChunkedDecoded, AwsChunkedMode, AwsChunkedSigningContext,
};
pub use sigv4::{
    Authenticator, BOOTSTRAP_ACCESS_KEY_ID, BOOTSTRAP_SECRET_ACCESS_KEY,
    RequestAuth, RequestHeader, SessionAttributes, VerifiedPayload,
    VerifiedRequest, VerifiedSignature, decode_authorization_message,
};
