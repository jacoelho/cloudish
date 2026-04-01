#![allow(
    clippy::unreachable,
    clippy::assertions_on_constants,
    clippy::missing_panics_doc,
    clippy::missing_errors_doc
)]

use aws_sdk_iam::Client as IamClient;
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use std::sync::atomic::{AtomicUsize, Ordering};

const PROVIDED_BOOTSTRAP_ZIP_BASE64: &str = "UEsDBBQAAAAAAAAAIQCEK9lNDgAAAA4AAAAJAAAAYm9vdHN0cmFwIyEvYmluL3NoCmNhdApQSwECFAMUAAAAAAAAACEAhCvZTQ4AAAAOAAAACQAAAAAAAAAAAAAA7QEAAAAAYm9vdHN0cmFwUEsFBgAAAAABAAEANwAAADUAAAAAAA==";
const FAILING_BOOTSTRAP_ZIP_BASE64: &str = "UEsDBAoAAAAAACIseVyjt17aEQAAABEAAAAJABwAYm9vdHN0cmFwVVQJAAOPc8Npj3PDaXV4CwABBPUBAAAEFAAAACMhL2Jpbi9zaApleGl0IDEKUEsBAh4DCgAAAAAAIix5XKO3XtoRAAAAEQAAAAkAGAAAAAAAAQAAAKSBAAAAAGJvb3RzdHJhcFVUBQADj3PDaXV4CwABBPUBAAAEFAAAAFBLBQYAAAAAAQABAE8AAABUAAAAAAA=";
const PROXY_BOOTSTRAP_ZIP_BASE64: &str = "UEsDBBQAAAAIAIm1eVwi/hYppwAAAMgAAAAJABwAYm9vdHN0cmFwVVQJAANBZcRpQWXEaXV4CwABBPUBAAAEFAAAAC2OwQrCMBBE7/2KuFaq0NAi4qHgRfEvvGybaCPtJiRbbKn9d6N4G2YeM7NeFbWhIrSJw6mzqE7ptkEWb1Fj0MdDFOyFVCK7UbZLnDfEd5HNEBh5CBerNFT7ssyh1ai0D1DN0FhiTSx5cjEFdK4zsdRYKp7BEuQwSnTm8ZJ+IDb9F+qwrxVK5+04wZJDbdUU7U2ItAnn35krNXFPQcV+0EsmIP2fhuQDUEsBAh4DFAAAAAgAibV5XCL+FimnAAAAyAAAAAkAGAAAAAAAAQAAAO2BAAAAAGJvb3RzdHJhcFVUBQADQWXEaXV4CwABBPUBAAAEFAAAAFBLBQYAAAAAAQABAE8AAADqAAAAAAA=";

pub fn provided_bootstrap_zip() -> Vec<u8> {
    BASE64_STANDARD
        .decode(PROVIDED_BOOTSTRAP_ZIP_BASE64)
        .expect("lambda fixture ZIP should decode")
}

pub fn failing_bootstrap_zip() -> Vec<u8> {
    BASE64_STANDARD
        .decode(FAILING_BOOTSTRAP_ZIP_BASE64)
        .expect("failing lambda fixture ZIP should decode")
}

pub fn proxy_bootstrap_zip() -> Vec<u8> {
    BASE64_STANDARD
        .decode(PROXY_BOOTSTRAP_ZIP_BASE64)
        .expect("proxy lambda fixture ZIP should decode")
}

pub fn unique_name(prefix: &str) -> String {
    static NEXT_ID: AtomicUsize = AtomicUsize::new(0);

    format!("{prefix}-{}", NEXT_ID.fetch_add(1, Ordering::Relaxed))
}

pub async fn create_lambda_role(iam: &IamClient, role_name: &str) -> String {
    iam.create_role()
        .role_name(role_name)
        .assume_role_policy_document(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}"#,
        )
        .send()
        .await
        .expect("lambda role should be created")
        .role()
        .map(|role| role.arn().to_owned())
        .expect("lambda role ARN should be returned")
}
