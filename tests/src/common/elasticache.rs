#![allow(
    clippy::unreachable,
    clippy::assertions_on_constants,
    clippy::missing_panics_doc,
    clippy::missing_errors_doc
)]

use auth::{BOOTSTRAP_ACCESS_KEY_ID, BOOTSTRAP_SECRET_ACCESS_KEY};
use hmac::{Hmac, Mac};
use redis::{
    AsyncCommands, Client, ConnectionAddr, ConnectionInfo, ProtocolVersion,
    RedisConnectionInfo,
};
use sha2::{Digest, Sha256};
use std::fmt::Write as _;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use time::OffsetDateTime;
use time::format_description::parse;
use tokio::net::TcpStream;

type HmacSha256 = Hmac<Sha256>;

static NEXT_ID: AtomicUsize = AtomicUsize::new(0);

pub fn unique_name(prefix: &str) -> String {
    format!("{prefix}-{}", NEXT_ID.fetch_add(1, Ordering::Relaxed))
}

pub async fn redis_set_get(
    host: &str,
    port: u16,
    username: Option<&str>,
    password: Option<&str>,
    key: &str,
    value: &str,
) -> Result<String, String> {
    let mut connection = redis_connect(host, port, username, password).await?;
    tokio::time::timeout(
        Duration::from_secs(2),
        connection.set::<_, _, ()>(key, value),
    )
    .await
    .map_err(|_| "redis SET timed out".to_owned())?
    .map_err(|error| error.to_string())?;
    tokio::time::timeout(Duration::from_secs(2), connection.get(key))
        .await
        .map_err(|_| "redis GET timed out".to_owned())?
        .map_err(|error| error.to_string())
}

pub async fn redis_acl_list_error(
    host: &str,
    port: u16,
    username: Option<&str>,
    password: Option<&str>,
) -> String {
    let mut connection = redis_connect(host, port, username, password)
        .await
        .expect("redis connection should authenticate");
    let error = tokio::time::timeout(
        Duration::from_secs(2),
        redis::cmd("ACL").arg("LIST").query_async::<String>(&mut connection),
    )
    .await
    .expect("ACL command should return promptly");
    let failed = error.is_err();
    assert!(failed, "ACL LIST should fail explicitly");
    let error = match error {
        Ok(_) => std::process::abort(),
        Err(error) => error,
    };

    error.to_string()
}

pub async fn redis_connect_error(
    host: &str,
    port: u16,
    username: Option<&str>,
    password: Option<&str>,
) -> String {
    let connection = redis_connect(host, port, username, password).await;
    let failed = connection.is_err();
    assert!(failed, "redis connection should fail");
    match connection {
        Ok(_) => std::process::abort(),
        Err(error) => error,
    }
}

pub async fn wait_for_port_closed(host: &str, port: u16) {
    for _ in 0..30 {
        if TcpStream::connect((host, port)).await.is_err() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let still_open = TcpStream::connect((host, port)).await.is_ok();
    assert!(!still_open, "port {host}:{port} should close");
    std::process::abort();
}

pub fn elasticache_iam_token(
    replication_group_id: &str,
    region: &str,
    user: &str,
) -> String {
    let now = OffsetDateTime::now_utc();
    let date_format =
        parse("[year][month][day]").expect("date format should parse");
    let timestamp_format = parse("[year][month][day]T[hour][minute][second]Z")
        .expect("timestamp format should parse");
    let date = now.format(&date_format).expect("signing date should format");
    let amz_date = now
        .format(&timestamp_format)
        .expect("signing timestamp should format");

    let mut parameters = vec![
        ("Action".to_owned(), "connect".to_owned()),
        ("User".to_owned(), user.to_owned()),
        ("X-Amz-Algorithm".to_owned(), "AWS4-HMAC-SHA256".to_owned()),
        (
            "X-Amz-Credential".to_owned(),
            format!(
                "{BOOTSTRAP_ACCESS_KEY_ID}/{date}/{region}/elasticache/aws4_request"
            ),
        ),
        ("X-Amz-Date".to_owned(), amz_date.clone()),
        ("X-Amz-Expires".to_owned(), "900".to_owned()),
        ("X-Amz-SignedHeaders".to_owned(), "host".to_owned()),
    ];
    parameters.sort_by(|left, right| {
        canonical_pair(left).cmp(&canonical_pair(right))
    });

    let canonical_query =
        parameters.iter().map(canonical_pair).collect::<Vec<_>>().join("&");
    let canonical_request = format!(
        "GET\n/\n{canonical_query}\nhost:{replication_group_id}\n\nhost\nUNSIGNED-PAYLOAD"
    );
    let signature = build_signature(
        BOOTSTRAP_SECRET_ACCESS_KEY,
        &date,
        region,
        "elasticache",
        &amz_date,
        &canonical_request,
    );

    format!(
        "https://{replication_group_id}/?{canonical_query}&X-Amz-Signature={signature}"
    )
}

async fn redis_connect(
    host: &str,
    port: u16,
    username: Option<&str>,
    password: Option<&str>,
) -> Result<redis::aio::MultiplexedConnection, String> {
    let client = Client::open(ConnectionInfo {
        addr: ConnectionAddr::Tcp(host.to_owned(), port),
        redis: RedisConnectionInfo {
            db: 0,
            username: username.map(str::to_owned),
            password: password.map(str::to_owned),
            protocol: ProtocolVersion::RESP2,
        },
    })
    .map_err(|error| error.to_string())?;

    tokio::time::timeout(
        Duration::from_secs(2),
        client.get_multiplexed_async_connection(),
    )
    .await
    .map_err(|_| "redis connection timed out".to_owned())?
    .map_err(|error| error.to_string())
}

fn canonical_pair((name, value): &(String, String)) -> String {
    format!(
        "{}={}",
        percent_encode(name.as_bytes(), false),
        percent_encode(value.as_bytes(), false),
    )
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

fn percent_encode(bytes: &[u8], preserve_slash: bool) -> String {
    let mut encoded = String::with_capacity(bytes.len());

    for byte in bytes {
        match byte {
            b'A'..=b'Z'
            | b'a'..=b'z'
            | b'0'..=b'9'
            | b'-'
            | b'_'
            | b'.'
            | b'~' => encoded.push(char::from(*byte)),
            b'/' if preserve_slash => encoded.push('/'),
            _ => {
                write!(&mut encoded, "%{byte:02X}")
                    .expect("percent encoding should write to String");
            }
        }
    }

    encoded
}
