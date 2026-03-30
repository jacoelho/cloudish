use std::error::Error;
use std::fmt;
use std::net::{AddrParseError, IpAddr, Ipv4Addr, SocketAddr, SocketAddrV4};
use std::num::ParseIntError;
use std::path::PathBuf;

use aws::AdvertisedEdgeTemplate;

pub const EDGE_HOST_ENV: &str = "CLOUDISH_EDGE_HOST";
pub const EDGE_PORT_ENV: &str = "CLOUDISH_EDGE_PORT";
pub const EDGE_MAX_REQUEST_BYTES_ENV: &str = "CLOUDISH_EDGE_MAX_REQUEST_BYTES";
pub const EDGE_PUBLIC_HOST_ENV: &str = "CLOUDISH_EDGE_PUBLIC_HOST";
pub const EDGE_PUBLIC_PORT_ENV: &str = "CLOUDISH_EDGE_PUBLIC_PORT";
pub const EDGE_PUBLIC_SCHEME_ENV: &str = "CLOUDISH_EDGE_PUBLIC_SCHEME";
pub const EDGE_READY_FILE_ENV: &str = "CLOUDISH_EDGE_READY_FILE";
const DEFAULT_EDGE_PORT: u16 = 4566;
const DEFAULT_MAX_REQUEST_BYTES: usize = 64 * 1024 * 1024;

#[derive(Debug, Clone)]
pub(crate) struct HostingPlan {
    advertised_edge: AdvertisedEdgeTemplate,
    edge_address: SocketAddr,
    max_request_bytes: usize,
    ready_file: Option<PathBuf>,
}

impl HostingPlan {
    pub(crate) fn from_env<F>(
        mut read_env: F,
    ) -> Result<Self, HostingPlanError>
    where
        F: FnMut(&'static str) -> Option<String>,
    {
        let host = read_env(EDGE_HOST_ENV)
            .map(parse_edge_host)
            .transpose()?
            .unwrap_or(IpAddr::V4(Ipv4Addr::LOCALHOST));
        let port = read_env(EDGE_PORT_ENV)
            .map(parse_edge_port)
            .transpose()?
            .unwrap_or(DEFAULT_EDGE_PORT);
        let max_request_bytes = read_env(EDGE_MAX_REQUEST_BYTES_ENV)
            .map(parse_max_request_bytes)
            .transpose()?
            .unwrap_or(DEFAULT_MAX_REQUEST_BYTES);
        let public_host = read_env(EDGE_PUBLIC_HOST_ENV)
            .map(parse_public_host)
            .transpose()?
            .unwrap_or_else(|| default_public_host(host));
        let public_port = read_env(EDGE_PUBLIC_PORT_ENV)
            .map(parse_public_port)
            .transpose()?
            .or((port != 0).then_some(port));
        let public_scheme = read_env(EDGE_PUBLIC_SCHEME_ENV)
            .map(parse_public_scheme)
            .transpose()?
            .unwrap_or_else(|| "http".to_owned());
        let ready_file =
            read_env(EDGE_READY_FILE_ENV).map(parse_ready_file).transpose()?;

        Ok(Self {
            advertised_edge: AdvertisedEdgeTemplate::new(
                public_scheme,
                public_host,
                public_port,
            ),
            edge_address: SocketAddr::new(host, port),
            max_request_bytes,
            ready_file,
        })
    }

    pub(crate) fn local() -> Self {
        Self {
            advertised_edge: AdvertisedEdgeTemplate::localhost(None),
            edge_address: SocketAddr::V4(SocketAddrV4::new(
                Ipv4Addr::LOCALHOST,
                DEFAULT_EDGE_PORT,
            )),
            max_request_bytes: DEFAULT_MAX_REQUEST_BYTES,
            ready_file: None,
        }
    }

    pub(crate) fn advertised_edge(&self) -> &AdvertisedEdgeTemplate {
        &self.advertised_edge
    }

    pub(crate) fn edge_address(&self) -> SocketAddr {
        self.edge_address
    }

    pub(crate) fn max_request_bytes(&self) -> usize {
        self.max_request_bytes
    }

    pub(crate) fn ready_file(&self) -> Option<&PathBuf> {
        self.ready_file.as_ref()
    }
}

#[derive(Debug)]
pub enum HostingPlanError {
    BlankHost,
    BlankMaxRequestBytes,
    BlankPublicHost,
    BlankPublicScheme,
    BlankReadyFile,
    InvalidHost { source: AddrParseError },
    InvalidMaxRequestBytes { source: ParseIntError },
    BlankPort,
    BlankPublicPort,
    InvalidPort { source: ParseIntError },
    InvalidPublicPort { source: ParseIntError },
    ZeroMaxRequestBytes,
}

impl fmt::Display for HostingPlanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BlankHost => write!(
                formatter,
                "invalid edge host in {EDGE_HOST_ENV}: value must not be blank"
            ),
            Self::BlankMaxRequestBytes => write!(
                formatter,
                "invalid edge request size in {EDGE_MAX_REQUEST_BYTES_ENV}: value must not be blank"
            ),
            Self::BlankPublicHost => write!(
                formatter,
                "invalid public edge host in {EDGE_PUBLIC_HOST_ENV}: value must not be blank"
            ),
            Self::BlankPublicScheme => write!(
                formatter,
                "invalid public edge scheme in {EDGE_PUBLIC_SCHEME_ENV}: value must not be blank"
            ),
            Self::BlankReadyFile => write!(
                formatter,
                "invalid edge ready file in {EDGE_READY_FILE_ENV}: value must not be blank"
            ),
            Self::InvalidHost { source, .. } => write!(
                formatter,
                "invalid edge host in {EDGE_HOST_ENV}: {source}"
            ),
            Self::InvalidMaxRequestBytes { source, .. } => write!(
                formatter,
                "invalid edge request size in {EDGE_MAX_REQUEST_BYTES_ENV}: {source}"
            ),
            Self::BlankPort => write!(
                formatter,
                "invalid edge port in {EDGE_PORT_ENV}: value must not be blank"
            ),
            Self::BlankPublicPort => write!(
                formatter,
                "invalid public edge port in {EDGE_PUBLIC_PORT_ENV}: value must not be blank"
            ),
            Self::InvalidPort { source, .. } => write!(
                formatter,
                "invalid edge port in {EDGE_PORT_ENV}: {source}"
            ),
            Self::InvalidPublicPort { source, .. } => write!(
                formatter,
                "invalid public edge port in {EDGE_PUBLIC_PORT_ENV}: {source}"
            ),
            Self::ZeroMaxRequestBytes => write!(
                formatter,
                "invalid edge request size in {EDGE_MAX_REQUEST_BYTES_ENV}: value must be greater than zero"
            ),
        }
    }
}

impl Error for HostingPlanError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidHost { source, .. } => Some(source),
            Self::InvalidMaxRequestBytes { source, .. } => Some(source),
            Self::InvalidPort { source, .. } => Some(source),
            Self::InvalidPublicPort { source, .. } => Some(source),
            Self::BlankHost
            | Self::BlankMaxRequestBytes
            | Self::BlankPublicHost
            | Self::BlankPublicPort
            | Self::BlankPublicScheme
            | Self::BlankReadyFile
            | Self::BlankPort
            | Self::ZeroMaxRequestBytes => None,
        }
    }
}

fn parse_edge_host(value: String) -> Result<IpAddr, HostingPlanError> {
    let value = value.trim().to_owned();
    if value.is_empty() {
        return Err(HostingPlanError::BlankHost);
    }

    value.parse().map_err(|source| HostingPlanError::InvalidHost { source })
}

fn parse_edge_port(value: String) -> Result<u16, HostingPlanError> {
    let value = value.trim().to_owned();
    if value.is_empty() {
        return Err(HostingPlanError::BlankPort);
    }

    value.parse().map_err(|source| HostingPlanError::InvalidPort { source })
}

fn parse_max_request_bytes(value: String) -> Result<usize, HostingPlanError> {
    let value = value.trim().to_owned();
    if value.is_empty() {
        return Err(HostingPlanError::BlankMaxRequestBytes);
    }

    let parsed = value.parse().map_err(|source| {
        HostingPlanError::InvalidMaxRequestBytes { source }
    })?;
    if parsed == 0 {
        return Err(HostingPlanError::ZeroMaxRequestBytes);
    }

    Ok(parsed)
}

fn parse_public_host(value: String) -> Result<String, HostingPlanError> {
    let value = value.trim().to_owned();
    if value.is_empty() {
        return Err(HostingPlanError::BlankPublicHost);
    }

    Ok(value)
}

fn parse_public_port(value: String) -> Result<u16, HostingPlanError> {
    let value = value.trim().to_owned();
    if value.is_empty() {
        return Err(HostingPlanError::BlankPublicPort);
    }

    value
        .parse()
        .map_err(|source| HostingPlanError::InvalidPublicPort { source })
}

fn parse_public_scheme(value: String) -> Result<String, HostingPlanError> {
    let value = value.trim().to_owned();
    if value.is_empty() {
        return Err(HostingPlanError::BlankPublicScheme);
    }

    Ok(value)
}

fn parse_ready_file(value: String) -> Result<PathBuf, HostingPlanError> {
    let value = value.trim().to_owned();
    if value.is_empty() {
        return Err(HostingPlanError::BlankReadyFile);
    }

    Ok(PathBuf::from(value))
}

fn default_public_host(host: IpAddr) -> String {
    if host.is_loopback() || host.is_unspecified() {
        return "localhost".to_owned();
    }

    host.to_string()
}

#[cfg(test)]
mod tests {
    use super::{
        EDGE_HOST_ENV, EDGE_MAX_REQUEST_BYTES_ENV, EDGE_PORT_ENV,
        EDGE_PUBLIC_HOST_ENV, EDGE_PUBLIC_PORT_ENV, EDGE_PUBLIC_SCHEME_ENV,
        EDGE_READY_FILE_ENV, HostingPlan, HostingPlanError,
    };
    use std::error::Error;
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

    #[test]
    fn local_plan_exposes_default_edge_address() {
        let plan = HostingPlan::local();

        assert_eq!(
            plan.edge_address(),
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 4566))
        );
        assert_eq!(
            plan.advertised_edge().resolve(4566).origin(),
            "http://localhost:4566"
        );
        assert_eq!(plan.max_request_bytes(), 64 * 1024 * 1024);
    }

    #[test]
    fn env_plan_defaults_to_localhost_when_overrides_are_unset() {
        let plan = HostingPlan::from_env(|_| None)
            .expect("missing overrides should use the default listener");

        assert_eq!(
            plan.edge_address(),
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 4566))
        );
        assert_eq!(
            plan.advertised_edge().resolve(4566).origin(),
            "http://localhost:4566"
        );
        assert_eq!(plan.max_request_bytes(), 64 * 1024 * 1024);
    }

    #[test]
    fn env_plan_uses_configured_edge_settings() {
        let plan = HostingPlan::from_env(|name| match name {
            EDGE_HOST_ENV => Some("0.0.0.0".to_owned()),
            EDGE_PORT_ENV => Some("4570".to_owned()),
            EDGE_MAX_REQUEST_BYTES_ENV => Some("1048576".to_owned()),
            EDGE_PUBLIC_HOST_ENV => Some("edge.cloudish.test".to_owned()),
            EDGE_PUBLIC_PORT_ENV => Some("8080".to_owned()),
            EDGE_PUBLIC_SCHEME_ENV => Some("https".to_owned()),
            EDGE_READY_FILE_ENV => Some("/tmp/cloudish-ready".to_owned()),
            _ => None,
        })
        .expect("valid overrides should build a listener plan");

        assert_eq!(plan.edge_address().to_string(), "0.0.0.0:4570");
        assert_eq!(
            plan.advertised_edge().resolve(4570).origin(),
            "https://edge.cloudish.test:8080"
        );
        assert_eq!(plan.max_request_bytes(), 1_048_576);
        assert_eq!(
            plan.ready_file().expect("ready file should be set"),
            &std::path::PathBuf::from("/tmp/cloudish-ready")
        );
    }

    #[test]
    fn env_plan_rejects_blank_edge_host() {
        let error = HostingPlan::from_env(|name| match name {
            EDGE_HOST_ENV => Some("   ".to_owned()),
            _ => None,
        })
        .expect_err("blank edge host must fail");

        assert!(matches!(error, HostingPlanError::BlankHost));
        assert_eq!(
            error.to_string(),
            "invalid edge host in CLOUDISH_EDGE_HOST: value must not be blank"
        );
        assert!(Error::source(&error).is_none());
    }

    #[test]
    fn env_plan_rejects_invalid_edge_host() {
        let error = HostingPlan::from_env(|name| match name {
            EDGE_HOST_ENV => Some("localhost".to_owned()),
            _ => None,
        })
        .expect_err("invalid edge host must fail");

        assert!(matches!(error, HostingPlanError::InvalidHost { .. }));
        assert_eq!(
            error.to_string(),
            "invalid edge host in CLOUDISH_EDGE_HOST: invalid IP address syntax"
        );
        assert_eq!(
            Error::source(&error).map(ToString::to_string).as_deref(),
            Some("invalid IP address syntax")
        );
    }

    #[test]
    fn env_plan_rejects_blank_edge_port() {
        let error = HostingPlan::from_env(|name| match name {
            EDGE_PORT_ENV => Some("   ".to_owned()),
            _ => None,
        })
        .expect_err("blank edge port must fail");

        assert!(matches!(error, HostingPlanError::BlankPort));
        assert_eq!(
            error.to_string(),
            "invalid edge port in CLOUDISH_EDGE_PORT: value must not be blank"
        );
        assert!(Error::source(&error).is_none());
    }

    #[test]
    fn env_plan_rejects_invalid_edge_port() {
        let error = HostingPlan::from_env(|name| match name {
            EDGE_PORT_ENV => Some("not-a-port".to_owned()),
            _ => None,
        })
        .expect_err("invalid edge port must fail");

        assert!(matches!(error, HostingPlanError::InvalidPort { .. }));
        assert_eq!(
            error.to_string(),
            "invalid edge port in CLOUDISH_EDGE_PORT: invalid digit found in string"
        );
        assert_eq!(
            Error::source(&error).map(ToString::to_string).as_deref(),
            Some("invalid digit found in string")
        );
    }

    #[test]
    fn env_plan_rejects_blank_request_size() {
        let error = HostingPlan::from_env(|name| match name {
            EDGE_MAX_REQUEST_BYTES_ENV => Some("   ".to_owned()),
            _ => None,
        })
        .expect_err("blank request size must fail");

        assert!(matches!(error, HostingPlanError::BlankMaxRequestBytes));
        assert_eq!(
            error.to_string(),
            "invalid edge request size in CLOUDISH_EDGE_MAX_REQUEST_BYTES: value must not be blank"
        );
        assert!(Error::source(&error).is_none());
    }

    #[test]
    fn env_plan_rejects_zero_request_size() {
        let error = HostingPlan::from_env(|name| match name {
            EDGE_MAX_REQUEST_BYTES_ENV => Some("0".to_owned()),
            _ => None,
        })
        .expect_err("zero request size must fail");

        assert!(matches!(error, HostingPlanError::ZeroMaxRequestBytes));
        assert_eq!(
            error.to_string(),
            "invalid edge request size in CLOUDISH_EDGE_MAX_REQUEST_BYTES: value must be greater than zero"
        );
        assert!(Error::source(&error).is_none());
    }

    #[test]
    fn env_plan_rejects_invalid_request_size() {
        let error = HostingPlan::from_env(|name| match name {
            EDGE_MAX_REQUEST_BYTES_ENV => Some("not-a-number".to_owned()),
            _ => None,
        })
        .expect_err("invalid request size must fail");

        assert!(matches!(
            error,
            HostingPlanError::InvalidMaxRequestBytes { .. }
        ));
        assert_eq!(
            error.to_string(),
            "invalid edge request size in CLOUDISH_EDGE_MAX_REQUEST_BYTES: invalid digit found in string"
        );
        assert_eq!(
            Error::source(&error).map(ToString::to_string).as_deref(),
            Some("invalid digit found in string")
        );
    }

    #[test]
    fn env_plan_defaults_public_host_from_specific_bind_address() {
        let plan = HostingPlan::from_env(|name| match name {
            EDGE_HOST_ENV => Some("172.17.0.220".to_owned()),
            EDGE_PORT_ENV => Some("4566".to_owned()),
            _ => None,
        })
        .expect("specific bind address should infer a public host");

        assert_eq!(
            plan.advertised_edge().resolve(4566).origin(),
            "http://172.17.0.220:4566"
        );
    }
}
