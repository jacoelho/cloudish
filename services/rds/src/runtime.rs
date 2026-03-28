use crate::errors::{RdsError, infrastructure_error};
use crate::instances::RdsEngine;
use crate::scope::RdsScope;
use crate::{DbClusterIdentifier, DbInstanceIdentifier};
use aws::{
    AccountId, Clock, Endpoint, InfrastructureError, RegionId,
    RunningTcpProxy, TcpProxyRuntime, TcpProxySpec,
};
use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, LockResult, Mutex};

#[derive(Clone)]
pub struct RdsServiceDependencies {
    pub backend_runtime: Arc<dyn RdsBackendRuntime + Send + Sync>,
    pub clock: Arc<dyn Clock>,
    pub iam_token_validator: Arc<dyn RdsIamTokenValidator + Send + Sync>,
    pub proxy_runtime: Arc<dyn TcpProxyRuntime + Send + Sync>,
}

#[derive(Debug, Clone)]
pub struct RdsBackendSpec {
    pub auth_endpoint_source: Arc<dyn RdsAuthEndpointSource + Send + Sync>,
    pub database_name: Option<String>,
    pub engine: RdsEngine,
    pub iam_token_validator:
        Option<Arc<dyn RdsIamTokenValidator + Send + Sync>>,
    pub listen_endpoint: Endpoint,
    pub master_password: String,
    pub master_username: String,
}

pub trait RunningRdsBackend: Send + Sync {
    fn listen_endpoint(&self) -> Endpoint;

    /// # Errors
    ///
    /// Returns an infrastructure error when the backend process cannot be
    /// stopped cleanly.
    fn stop(&self) -> Result<(), InfrastructureError>;
}

pub trait RdsBackendRuntime: Send + Sync {
    /// # Errors
    ///
    /// Returns an infrastructure error when the backend process cannot be
    /// started.
    fn start(
        &self,
        spec: &RdsBackendSpec,
    ) -> Result<Box<dyn RunningRdsBackend>, InfrastructureError>;
}

pub trait RdsAuthEndpointSource: Send + Sync + fmt::Debug {
    fn endpoints(&self) -> Vec<Endpoint>;
}

pub trait RdsIamTokenValidator: Send + Sync + fmt::Debug {
    /// # Errors
    ///
    /// Returns an error string when the IAM database authentication token is
    /// rejected.
    fn validate(
        &self,
        endpoint: &Endpoint,
        username: &str,
        token: &str,
    ) -> Result<(), String>;
}

#[derive(Debug, Default, Clone, Copy)]
pub struct RejectAllRdsIamTokenValidator;

impl RdsIamTokenValidator for RejectAllRdsIamTokenValidator {
    fn validate(
        &self,
        _endpoint: &Endpoint,
        _username: &str,
        _token: &str,
    ) -> Result<(), String> {
        Err("IAM database authentication is not configured.".to_owned())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct InstanceRuntimeKey {
    account_id: AccountId,
    region: RegionId,
    pub db_instance_identifier: DbInstanceIdentifier,
}

impl InstanceRuntimeKey {
    pub(crate) fn new(
        scope: &RdsScope,
        db_instance_identifier: &DbInstanceIdentifier,
    ) -> Self {
        Self {
            account_id: scope.account_id().clone(),
            region: scope.region().clone(),
            db_instance_identifier: db_instance_identifier.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct ClusterRuntimeKey {
    account_id: AccountId,
    region: RegionId,
    pub db_cluster_identifier: DbClusterIdentifier,
}

impl ClusterRuntimeKey {
    pub(crate) fn new(
        scope: &RdsScope,
        db_cluster_identifier: &DbClusterIdentifier,
    ) -> Self {
        Self {
            account_id: scope.account_id().clone(),
            region: scope.region().clone(),
            db_cluster_identifier: db_cluster_identifier.clone(),
        }
    }
}

#[derive(Default)]
pub(crate) struct RdsRuntimeState {
    pub clusters: BTreeMap<ClusterRuntimeKey, ClusterRuntime>,
    pub instances: BTreeMap<InstanceRuntimeKey, InstanceRuntime>,
}

pub(crate) struct ClusterRuntime {
    pub auth_endpoints: SharedAuthEndpoints,
    pub backend: Box<dyn RunningRdsBackend>,
    pub proxy: Box<dyn RunningTcpProxy>,
}

impl ClusterRuntime {
    pub(crate) fn new(
        auth_endpoints: SharedAuthEndpoints,
        backend: Box<dyn RunningRdsBackend>,
        proxy: Box<dyn RunningTcpProxy>,
    ) -> Self {
        Self { auth_endpoints, backend, proxy }
    }

    pub(crate) fn stop(self) -> Result<(), RdsError> {
        self.proxy.stop().map_err(|source| {
            infrastructure_error("stopping RDS proxy", source)
        })?;
        self.backend.stop().map_err(|source| {
            infrastructure_error("stopping RDS backend", source)
        })
    }
}

pub(crate) enum InstanceRuntime {
    ClusterMember {
        proxy: Box<dyn RunningTcpProxy>,
    },
    Standalone {
        backend: Box<dyn RunningRdsBackend>,
        proxy: Box<dyn RunningTcpProxy>,
    },
}

impl InstanceRuntime {
    pub(crate) fn cluster_member(proxy: Box<dyn RunningTcpProxy>) -> Self {
        Self::ClusterMember { proxy }
    }

    pub(crate) fn standalone(started: StartedRuntime) -> Self {
        let StartedRuntime { auth_endpoints: _, backend, proxy } = started;
        Self::Standalone { backend, proxy }
    }

    pub(crate) fn stop(self) -> Result<(), RdsError> {
        match self {
            Self::ClusterMember { proxy } => proxy.stop().map_err(|source| {
                infrastructure_error("stopping RDS proxy", source)
            }),
            Self::Standalone { backend, proxy } => {
                proxy.stop().map_err(|source| {
                    infrastructure_error("stopping RDS proxy", source)
                })?;
                backend.stop().map_err(|source| {
                    infrastructure_error("stopping RDS backend", source)
                })
            }
        }
    }

    pub(crate) fn stop_proxy(self) -> Result<(), RdsError> {
        match self {
            Self::ClusterMember { proxy } | Self::Standalone { proxy, .. } => {
                proxy.stop().map_err(|source| {
                    infrastructure_error("stopping RDS proxy", source)
                })
            }
        }
    }
}

pub(crate) struct StartedRuntime {
    pub auth_endpoints: SharedAuthEndpoints,
    pub backend: Box<dyn RunningRdsBackend>,
    pub proxy: Box<dyn RunningTcpProxy>,
}

impl StartedRuntime {
    pub(crate) fn proxy_port(&self) -> u16 {
        self.proxy.listen_endpoint().port()
    }

    pub(crate) fn stop(self) -> Result<(), RdsError> {
        self.proxy.stop().map_err(|source| {
            infrastructure_error("stopping RDS proxy", source)
        })?;
        self.backend.stop().map_err(|source| {
            infrastructure_error("stopping RDS backend", source)
        })
    }
}

#[derive(Debug, Default, Clone)]
pub(crate) struct SharedAuthEndpoints {
    inner: Arc<Mutex<Vec<Endpoint>>>,
}

impl SharedAuthEndpoints {
    pub(crate) fn add_endpoint(&self, endpoint: Endpoint) {
        let mut endpoints = recover(self.inner.lock());
        if !endpoints.contains(&endpoint) {
            endpoints.push(endpoint);
        }
    }

    pub(crate) fn remove_endpoint(&self, endpoint: &Endpoint) {
        recover(self.inner.lock()).retain(|candidate| candidate != endpoint);
    }

    pub(crate) fn set_endpoints(&self, endpoints: Vec<Endpoint>) {
        *recover(self.inner.lock()) = endpoints;
    }
}

impl RdsAuthEndpointSource for SharedAuthEndpoints {
    fn endpoints(&self) -> Vec<Endpoint> {
        recover(self.inner.lock()).clone()
    }
}

pub(crate) fn recover<T>(result: LockResult<T>) -> T {
    result.unwrap_or_else(std::sync::PoisonError::into_inner)
}

pub(crate) fn start_proxy(
    proxy_runtime: &Arc<dyn TcpProxyRuntime + Send + Sync>,
    upstream: &Endpoint,
    listen_port: Option<u16>,
) -> Result<Box<dyn RunningTcpProxy>, RdsError> {
    proxy_runtime
        .start(&TcpProxySpec::new(
            Endpoint::localhost(listen_port.unwrap_or(0)),
            upstream.clone(),
        ))
        .map_err(|source| infrastructure_error("starting RDS proxy", source))
}
