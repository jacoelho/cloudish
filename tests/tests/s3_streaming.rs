#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    clippy::unreachable,
    clippy::indexing_slicing,
    clippy::assertions_on_constants,
    clippy::missing_panics_doc,
    clippy::missing_errors_doc
)]
use auth::{BOOTSTRAP_ACCESS_KEY_ID, BOOTSTRAP_SECRET_ACCESS_KEY};
use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3::config::Builder as S3ConfigBuilder;
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};
use std::fmt::Write as _;
use std::io::Write as _;
use std::io::{ErrorKind, Read};
use std::net::TcpStream;
use tests::common::runtime;
use tests::common::sdk;
use time::OffsetDateTime;

type HmacSha256 = Hmac<Sha256>;
type SharedRuntimeLease = runtime::SharedRuntimeLease<'static>;

static SHARED_RUNTIME: runtime::SharedRuntime =
    runtime::SharedRuntime::new("s3_streaming");

async fn shared_runtime() -> SharedRuntimeLease {
    SHARED_RUNTIME.acquire().await
}

async fn s3_client(target: &sdk::SdkSmokeTarget) -> S3Client {
    let shared = target.load().await;
    let config = S3ConfigBuilder::from(&shared).force_path_style(true).build();
    S3Client::from_conf(config)
}

#[tokio::test]
async fn given_a_signed_aws_chunked_put_object_when_sent_over_raw_http_then_the_decoded_object_is_stored()
 {
    let runtime = shared_runtime().await;
    let target = sdk::SdkSmokeTarget::new(
        runtime.localhost_endpoint_url(),
        "us-east-1",
    );
    let client = s3_client(&target).await;
    client
        .create_bucket()
        .bucket("sdk-s3-streaming")
        .send()
        .await
        .expect("bucket should be created");

    let payload = b"hello aws chunked";
    let request = aws_chunked_put_object_request(
        "/sdk-s3-streaming/streamed.txt",
        "localhost",
        "us-east-1",
        &sigv4_timestamp_now(),
        payload,
    );
    let response = send_raw_request(runtime.address(), &request)
        .expect("signed aws-chunked request should receive a response");

    let object = client
        .get_object()
        .bucket("sdk-s3-streaming")
        .key("streamed.txt")
        .send()
        .await
        .expect("streamed object should be readable");
    let body = object
        .body
        .collect()
        .await
        .expect("streamed body should collect")
        .into_bytes();

    assert!(
        response.is_empty() || response.starts_with(b"HTTP/1.1 200 OK\r\n"),
        "expected an empty raw socket read or 200 OK, got {:?}",
        String::from_utf8_lossy(&response)
    );
    assert_eq!(body.as_ref(), payload);
}

fn send_raw_request(
    address: std::net::SocketAddr,
    request: &[u8],
) -> std::io::Result<Vec<u8>> {
    let mut stream = TcpStream::connect(address)?;
    stream.set_read_timeout(Some(std::time::Duration::from_millis(250)))?;
    stream.write_all(request)?;

    let mut response = Vec::new();
    let mut buffer = [0_u8; 4096];
    loop {
        match stream.read(&mut buffer) {
            Ok(0) => break,
            Ok(read) => response.extend_from_slice(&buffer[..read]),
            Err(error)
                if matches!(
                    error.kind(),
                    ErrorKind::TimedOut | ErrorKind::WouldBlock
                ) =>
            {
                break;
            }
            Err(error) => return Err(error),
        }
    }

    Ok(response)
}

fn aws_chunked_put_object_request(
    path: &str,
    host: &str,
    region: &str,
    amz_date: &str,
    payload: &[u8],
) -> Vec<u8> {
    let chunk_sizes = [5_usize, payload.len().saturating_sub(5), 0];
    let content_length = aws_chunked_body_length(&chunk_sizes);
    let date = &amz_date[..8];
    let signed_headers = "content-encoding;content-length;host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length";
    let canonical_request = format!(
        "PUT\n{path}\n\ncontent-encoding:aws-chunked\ncontent-length:{content_length}\nhost:{host}\nx-amz-content-sha256:STREAMING-AWS4-HMAC-SHA256-PAYLOAD\nx-amz-date:{amz_date}\nx-amz-decoded-content-length:{}\n\n{signed_headers}\nSTREAMING-AWS4-HMAC-SHA256-PAYLOAD",
        payload.len()
    );
    let seed_signature = build_signature(
        BOOTSTRAP_SECRET_ACCESS_KEY,
        date,
        region,
        "s3",
        amz_date,
        &canonical_request,
    );
    let body = aws_chunked_body(
        payload,
        &chunk_sizes,
        date,
        region,
        "s3",
        amz_date,
        &seed_signature,
    );

    let mut request = format!(
        "PUT {path} HTTP/1.1\r\nHost: {host}\r\nContent-Encoding: aws-chunked\r\nContent-Length: {content_length}\r\nX-Amz-Content-Sha256: STREAMING-AWS4-HMAC-SHA256-PAYLOAD\r\nX-Amz-Date: {amz_date}\r\nX-Amz-Decoded-Content-Length: {}\r\nAuthorization: AWS4-HMAC-SHA256 Credential={}/{date}/{region}/s3/aws4_request,SignedHeaders={signed_headers},Signature={seed_signature}\r\n\r\n",
        payload.len(),
        BOOTSTRAP_ACCESS_KEY_ID,
    )
    .into_bytes();
    request.extend_from_slice(&body);
    request
}

fn sigv4_timestamp_now() -> String {
    let now = OffsetDateTime::now_utc();
    format!(
        "{:04}{:02}{:02}T{:02}{:02}{:02}Z",
        now.year(),
        u8::from(now.month()),
        now.day(),
        now.hour(),
        now.minute(),
        now.second(),
    )
}

fn aws_chunked_body(
    payload: &[u8],
    chunk_sizes: &[usize],
    date: &str,
    region: &str,
    service: &str,
    amz_date: &str,
    seed_signature: &str,
) -> Vec<u8> {
    let signing_key =
        signing_key(BOOTSTRAP_SECRET_ACCESS_KEY, date, region, service);
    let mut previous_signature = seed_signature.to_owned();
    let mut payload_offset = 0;
    let mut encoded = Vec::new();

    for chunk_size in chunk_sizes {
        let next_offset = payload_offset + *chunk_size;
        let chunk_data = payload
            .get(payload_offset..next_offset)
            .expect("chunk size should stay within payload bounds");
        let signature = sign_chunk(
            &signing_key,
            amz_date,
            date,
            region,
            service,
            &previous_signature,
            chunk_data,
        );
        encoded.extend_from_slice(
            format!("{chunk_size:x};chunk-signature={signature}\r\n")
                .as_bytes(),
        );
        encoded.extend_from_slice(chunk_data);
        encoded.extend_from_slice(b"\r\n");
        previous_signature = signature;
        payload_offset = next_offset;
    }

    encoded
}

fn aws_chunked_body_length(chunk_sizes: &[usize]) -> usize {
    chunk_sizes
        .iter()
        .map(|chunk_size| {
            format!("{chunk_size:x};chunk-signature={}\r\n", "0".repeat(64))
                .len()
                + chunk_size
                + 2
        })
        .sum()
}

fn sign_chunk(
    signing_key: &[u8; 32],
    amz_date: &str,
    date: &str,
    region: &str,
    service: &str,
    previous_signature: &str,
    chunk_data: &[u8],
) -> String {
    let scope = format!("{date}/{region}/{service}/aws4_request");
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256-PAYLOAD\n{amz_date}\n{scope}\n{previous_signature}\n{}\n{}",
        hash_hex(b""),
        hash_hex(chunk_data),
    );

    hex_encode(&hmac_bytes(signing_key, string_to_sign.as_bytes()))
}

fn build_signature(
    secret_access_key: &str,
    date: &str,
    region: &str,
    service: &str,
    amz_date: &str,
    canonical_request: &str,
) -> String {
    let scope = format!("{date}/{region}/{service}/aws4_request");
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
        hash_hex(canonical_request.as_bytes())
    );
    let signing_key = signing_key(secret_access_key, date, region, service);

    hex_encode(&hmac_bytes(&signing_key, string_to_sign.as_bytes()))
}

fn signing_key(
    secret_access_key: &str,
    date: &str,
    region: &str,
    service: &str,
) -> [u8; 32] {
    let date_key = hmac_bytes(
        format!("AWS4{secret_access_key}").as_bytes(),
        date.as_bytes(),
    );
    let region_key = hmac_bytes(&date_key, region.as_bytes());
    let service_key = hmac_bytes(&region_key, service.as_bytes());
    hmac_bytes(&service_key, b"aws4_request")
}

fn hmac_bytes(key: &[u8], data: &[u8]) -> [u8; 32] {
    let mut mac =
        HmacSha256::new_from_slice(key).expect("HMAC-SHA256 accepts any key");
    mac.update(data);
    mac.finalize().into_bytes().into()
}

fn hash_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex_encode(&hasher.finalize())
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(&mut encoded, "{byte:02x}")
            .expect("hex encoding should write to String");
    }
    encoded
}
