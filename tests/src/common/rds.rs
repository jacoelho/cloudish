#![allow(
    clippy::unreachable,
    clippy::assertions_on_constants,
    clippy::missing_panics_doc,
    clippy::missing_errors_doc
)]

use auth::{BOOTSTRAP_ACCESS_KEY_ID, BOOTSTRAP_SECRET_ACCESS_KEY};
use hmac::{Hmac, Mac};
use mysql_async::prelude::Queryable;
use mysql_async::{OptsBuilder, Pool};
use sha2::{Digest, Sha256};
use std::fmt::Write as _;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use time::OffsetDateTime;
use time::format_description::parse;
use tokio::net::TcpStream;
use tokio_postgres::NoTls;

type HmacSha256 = Hmac<Sha256>;

static NEXT_ID: AtomicUsize = AtomicUsize::new(0);

pub fn unique_name(prefix: &str) -> String {
    format!("{prefix}-{}", NEXT_ID.fetch_add(1, Ordering::Relaxed))
}

pub async fn postgres_query(
    host: &str,
    port: u16,
    username: &str,
    password: &str,
    database_name: Option<&str>,
) -> Result<String, String> {
    let mut config = tokio_postgres::Config::new();
    config.host(host).port(port).user(username).password(password);
    if let Some(database_name) = database_name {
        config.dbname(database_name);
    }
    let (client, connection) =
        tokio::time::timeout(Duration::from_secs(2), config.connect(NoTls))
            .await
            .map_err(|_| "postgres connection timed out".to_owned())?
            .map_err(|error| error.to_string())?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    let rows = tokio::time::timeout(
        Duration::from_secs(2),
        client.simple_query("SELECT 1"),
    )
    .await
    .map_err(|_| "postgres query timed out".to_owned())?
    .map_err(|error| error.to_string())?;
    let value = rows
        .into_iter()
        .find_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => {
                row.get(0).map(str::to_owned)
            }
            _ => None,
        })
        .unwrap_or_default();

    Ok(value)
}

pub async fn postgres_query_eventually(
    host: &str,
    port: u16,
    username: &str,
    password: &str,
    database_name: Option<&str>,
) -> String {
    let mut last_error = None;

    for _ in 0..5 {
        match postgres_query(host, port, username, password, database_name)
            .await
        {
            Ok(value) => return value,
            Err(error) => last_error = Some(error),
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    let exhausted_retries = last_error.is_some();
    assert!(
        !exhausted_retries,
        "postgres query should succeed: {:?}",
        last_error.expect("retry loop should capture an error")
    );
    std::process::abort();
}

pub async fn mysql_query(
    host: &str,
    port: u16,
    username: &str,
    password: &str,
    database_name: Option<&str>,
) -> Result<u8, String> {
    let options = OptsBuilder::default()
        .ip_or_hostname(host.to_owned())
        .tcp_port(port)
        .user(Some(username.to_owned()))
        .pass(Some(password.to_owned()))
        .db_name(database_name.map(str::to_owned));
    let pool = Pool::new(options);
    let mut connection =
        tokio::time::timeout(Duration::from_secs(2), pool.get_conn())
            .await
            .map_err(|_| "mysql connection timed out".to_owned())?
            .map_err(|error| error.to_string())?;
    let value = tokio::time::timeout(
        Duration::from_secs(2),
        connection.query_first::<u8, _>("SELECT 1"),
    )
    .await
    .map_err(|_| "mysql query timed out".to_owned())?
    .map_err(|error| error.to_string())?
    .expect("mysql query should return one row");
    connection.disconnect().await.map_err(|error| error.to_string())?;
    pool.disconnect().await.map_err(|error| error.to_string())?;

    Ok(value)
}

pub async fn mysql_query_eventually(
    host: &str,
    port: u16,
    username: &str,
    password: &str,
    database_name: Option<&str>,
) -> u8 {
    let mut last_error = None;

    for _ in 0..5 {
        match mysql_query(host, port, username, password, database_name).await
        {
            Ok(value) => return value,
            Err(error) => last_error = Some(error),
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    let exhausted_retries = last_error.is_some();
    assert!(
        !exhausted_retries,
        "mysql query should succeed: {:?}",
        last_error.expect("retry loop should capture an error")
    );
    std::process::abort();
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

pub fn postgres_iam_token(
    host: &str,
    port: u16,
    region: &str,
    username: &str,
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
        ("DBUser".to_owned(), username.to_owned()),
        ("X-Amz-Algorithm".to_owned(), "AWS4-HMAC-SHA256".to_owned()),
        (
            "X-Amz-Credential".to_owned(),
            format!(
                "{BOOTSTRAP_ACCESS_KEY_ID}/{date}/{region}/rds-db/aws4_request"
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
        "GET\n/\n{canonical_query}\nhost:{host}:{port}\n\nhost\nUNSIGNED-PAYLOAD"
    );
    let signature = build_signature(
        BOOTSTRAP_SECRET_ACCESS_KEY,
        &date,
        region,
        "rds-db",
        &amz_date,
        &canonical_request,
    );

    format!(
        "https://{host}:{port}/?{canonical_query}&X-Amz-Signature={signature}"
    )
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
    let mut encoded = String::with_capacity(bytes.len().saturating_mul(2));
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
