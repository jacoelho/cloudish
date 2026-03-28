use std::error::Error;
use std::fmt;
use std::net::{AddrParseError, IpAddr, Ipv4Addr, SocketAddr, SocketAddrV4};
use std::num::ParseIntError;

pub const EDGE_HOST_ENV: &str = "CLOUDISH_EDGE_HOST";
pub const EDGE_PORT_ENV: &str = "CLOUDISH_EDGE_PORT";
const DEFAULT_EDGE_PORT: u16 = 4566;

#[derive(Debug, Clone)]
pub(crate) struct HostingPlan {
    edge_address: SocketAddr,
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

        Ok(Self { edge_address: SocketAddr::new(host, port) })
    }

    pub(crate) fn local() -> Self {
        Self {
            edge_address: SocketAddr::V4(SocketAddrV4::new(
                Ipv4Addr::LOCALHOST,
                DEFAULT_EDGE_PORT,
            )),
        }
    }

    pub(crate) fn edge_address(&self) -> SocketAddr {
        self.edge_address
    }
}

#[derive(Debug)]
pub enum HostingPlanError {
    BlankHost,
    InvalidHost { source: AddrParseError },
    BlankPort,
    InvalidPort { source: ParseIntError },
}

impl fmt::Display for HostingPlanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BlankHost => write!(
                formatter,
                "invalid edge host in {EDGE_HOST_ENV}: value must not be blank"
            ),
            Self::InvalidHost { source, .. } => write!(
                formatter,
                "invalid edge host in {EDGE_HOST_ENV}: {source}"
            ),
            Self::BlankPort => write!(
                formatter,
                "invalid edge port in {EDGE_PORT_ENV}: value must not be blank"
            ),
            Self::InvalidPort { source, .. } => write!(
                formatter,
                "invalid edge port in {EDGE_PORT_ENV}: {source}"
            ),
        }
    }
}

impl Error for HostingPlanError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidHost { source, .. } => Some(source),
            Self::InvalidPort { source, .. } => Some(source),
            Self::BlankHost | Self::BlankPort => None,
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

#[cfg(test)]
mod tests {
    use super::{EDGE_HOST_ENV, EDGE_PORT_ENV, HostingPlan, HostingPlanError};
    use std::error::Error;
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

    #[test]
    fn local_plan_exposes_default_edge_address() {
        let plan = HostingPlan::local();

        assert_eq!(
            plan.edge_address(),
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 4566))
        );
    }

    #[test]
    fn env_plan_defaults_to_localhost_when_overrides_are_unset() {
        let plan = HostingPlan::from_env(|_| None)
            .expect("missing overrides should use the default listener");

        assert_eq!(
            plan.edge_address(),
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 4566))
        );
    }

    #[test]
    fn env_plan_uses_configured_edge_host_and_port() {
        let plan = HostingPlan::from_env(|name| match name {
            EDGE_HOST_ENV => Some("0.0.0.0".to_owned()),
            EDGE_PORT_ENV => Some("4570".to_owned()),
            _ => None,
        })
        .expect("valid overrides should build a listener plan");

        assert_eq!(plan.edge_address().to_string(), "0.0.0.0:4570");
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
}
