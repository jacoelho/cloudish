use crate::auth::{
    ElastiCacheAuthenticationType, ElastiCacheIamTokenValidator,
};
use crate::errors::{ElastiCacheError, infrastructure_error};
use crate::scope::ElastiCacheScope;
use crate::{ElastiCacheEngine, ElastiCacheReplicationGroupId};

use aws::{AccountId, Clock, Endpoint, InfrastructureError, RegionId};
use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, LockResult};

pub trait RunningElastiCacheNode: Send + Sync {
    fn listen_endpoint(&self) -> Endpoint;

    /// # Errors
    ///
    /// Returns an infrastructure error when the backend process cannot be
    /// stopped cleanly.
    fn stop(&self) -> Result<(), InfrastructureError>;
}

pub trait ElastiCacheNodeRuntime: Send + Sync {
    /// # Errors
    ///
    /// Returns an infrastructure error when the backend process cannot be
    /// started.
    fn start(
        &self,
        spec: &ElastiCacheNodeSpec,
    ) -> Result<Box<dyn RunningElastiCacheNode>, InfrastructureError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ElastiCacheNodeSpec {
    engine: ElastiCacheEngine,
    group_id: ElastiCacheReplicationGroupId,
    listen_endpoint: Endpoint,
}

impl ElastiCacheNodeSpec {
    pub fn new(
        group_id: ElastiCacheReplicationGroupId,
        engine: ElastiCacheEngine,
        listen_endpoint: Endpoint,
    ) -> Self {
        Self { engine, group_id, listen_endpoint }
    }

    pub fn engine(&self) -> ElastiCacheEngine {
        self.engine
    }

    pub fn group_id(&self) -> &ElastiCacheReplicationGroupId {
        &self.group_id
    }

    pub fn listen_endpoint(&self) -> &Endpoint {
        &self.listen_endpoint
    }
}

pub trait RunningElastiCacheProxy: Send + Sync {
    fn listen_endpoint(&self) -> Endpoint;

    /// # Errors
    ///
    /// Returns an infrastructure error when the proxy process cannot be
    /// stopped cleanly.
    fn stop(&self) -> Result<(), InfrastructureError>;
}

pub trait ElastiCacheProxyRuntime: Send + Sync {
    /// # Errors
    ///
    /// Returns an infrastructure error when the proxy cannot be started.
    fn start(
        &self,
        spec: &ElastiCacheProxySpec,
    ) -> Result<Box<dyn RunningElastiCacheProxy>, InfrastructureError>;
}

pub trait ElastiCacheConnectionAuthenticator:
    Send + Sync + fmt::Debug
{
    /// # Errors
    ///
    /// Returns an error string when the IAM token is rejected.
    fn validate_iam_token(
        &self,
        scope: &ElastiCacheScope,
        replication_group_id: &ElastiCacheReplicationGroupId,
        token: &str,
    ) -> Result<(), String>;

    /// # Errors
    ///
    /// Returns an error string when the username/password pair is rejected.
    fn validate_password(
        &self,
        scope: &ElastiCacheScope,
        replication_group_id: &ElastiCacheReplicationGroupId,
        username: Option<&str>,
        password: &str,
    ) -> Result<(), String>;
}

#[derive(Clone)]
pub struct ElastiCacheProxySpec {
    auth_mode: ElastiCacheAuthenticationType,
    authenticator: Arc<dyn ElastiCacheConnectionAuthenticator + Send + Sync>,
    listen_endpoint: Endpoint,
    replication_group_id: ElastiCacheReplicationGroupId,
    scope: ElastiCacheScope,
    upstream: Endpoint,
}

impl ElastiCacheProxySpec {
    pub fn new(
        scope: ElastiCacheScope,
        replication_group_id: ElastiCacheReplicationGroupId,
        auth_mode: ElastiCacheAuthenticationType,
        listen_endpoint: Endpoint,
        upstream: Endpoint,
        authenticator: Arc<
            dyn ElastiCacheConnectionAuthenticator + Send + Sync,
        >,
    ) -> Self {
        Self {
            auth_mode,
            authenticator,
            listen_endpoint,
            replication_group_id,
            scope,
            upstream,
        }
    }

    pub fn auth_mode(&self) -> ElastiCacheAuthenticationType {
        self.auth_mode
    }

    pub fn authenticator(
        &self,
    ) -> &(dyn ElastiCacheConnectionAuthenticator + Send + Sync) {
        self.authenticator.as_ref()
    }

    pub fn listen_endpoint(&self) -> &Endpoint {
        &self.listen_endpoint
    }

    pub fn replication_group_id(&self) -> &ElastiCacheReplicationGroupId {
        &self.replication_group_id
    }

    pub fn scope(&self) -> &ElastiCacheScope {
        &self.scope
    }

    pub fn upstream(&self) -> &Endpoint {
        &self.upstream
    }
}

#[derive(Clone)]
pub struct ElastiCacheServiceDependencies {
    pub clock: Arc<dyn Clock>,
    pub iam_token_validator:
        Arc<dyn ElastiCacheIamTokenValidator + Send + Sync>,
    pub node_runtime: Arc<dyn ElastiCacheNodeRuntime + Send + Sync>,
    pub proxy_runtime: Arc<dyn ElastiCacheProxyRuntime + Send + Sync>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct ReplicationGroupRuntimeKey {
    account_id: AccountId,
    region: RegionId,
    replication_group_id: ElastiCacheReplicationGroupId,
}

impl ReplicationGroupRuntimeKey {
    pub(crate) fn new(
        scope: &ElastiCacheScope,
        replication_group_id: &ElastiCacheReplicationGroupId,
    ) -> Self {
        Self {
            account_id: scope.account_id().clone(),
            region: scope.region().clone(),
            replication_group_id: replication_group_id.clone(),
        }
    }
}

#[derive(Default)]
pub(crate) struct ElastiCacheRuntimeState {
    pub groups: BTreeMap<ReplicationGroupRuntimeKey, ReplicationGroupRuntime>,
}

pub(crate) struct ReplicationGroupRuntime {
    backend: Box<dyn RunningElastiCacheNode>,
    proxy: Box<dyn RunningElastiCacheProxy>,
}

impl ReplicationGroupRuntime {
    pub(crate) fn new(
        backend: Box<dyn RunningElastiCacheNode>,
        proxy: Box<dyn RunningElastiCacheProxy>,
    ) -> Self {
        Self { backend, proxy }
    }

    pub(crate) fn stop(self) -> Result<(), ElastiCacheError> {
        self.proxy.stop().map_err(|source| {
            infrastructure_error("stopping ElastiCache proxy", source)
        })?;
        self.backend.stop().map_err(|source| {
            infrastructure_error("stopping ElastiCache backend", source)
        })
    }
}

pub(crate) struct StartedRuntime {
    pub backend: Box<dyn RunningElastiCacheNode>,
    pub proxy: Box<dyn RunningElastiCacheProxy>,
}

pub(crate) struct RuntimeStartInput<'a> {
    auth_mode: ElastiCacheAuthenticationType,
    authenticator: Arc<dyn ElastiCacheConnectionAuthenticator + Send + Sync>,
    engine: ElastiCacheEngine,
    listen_port: Option<u16>,
    replication_group_id: &'a ElastiCacheReplicationGroupId,
    scope: &'a ElastiCacheScope,
}

impl<'a> RuntimeStartInput<'a> {
    pub(crate) fn new(
        scope: &'a ElastiCacheScope,
        replication_group_id: &'a ElastiCacheReplicationGroupId,
        engine: ElastiCacheEngine,
        auth_mode: ElastiCacheAuthenticationType,
        listen_port: Option<u16>,
        authenticator: Arc<
            dyn ElastiCacheConnectionAuthenticator + Send + Sync,
        >,
    ) -> Self {
        Self {
            auth_mode,
            authenticator,
            engine,
            listen_port,
            replication_group_id,
            scope,
        }
    }
}

impl StartedRuntime {
    pub(crate) fn proxy_port(&self) -> u16 {
        self.proxy.listen_endpoint().port()
    }

    pub(crate) fn stop(self) -> Result<(), ElastiCacheError> {
        self.proxy.stop().map_err(|source| {
            infrastructure_error("stopping ElastiCache proxy", source)
        })?;
        self.backend.stop().map_err(|source| {
            infrastructure_error("stopping ElastiCache backend", source)
        })
    }
}

pub(crate) fn start_runtime(
    node_runtime: &Arc<dyn ElastiCacheNodeRuntime + Send + Sync>,
    proxy_runtime: &Arc<dyn ElastiCacheProxyRuntime + Send + Sync>,
    input: RuntimeStartInput<'_>,
) -> Result<StartedRuntime, ElastiCacheError> {
    let RuntimeStartInput {
        auth_mode,
        authenticator,
        engine,
        listen_port,
        replication_group_id,
        scope,
    } = input;
    let backend = node_runtime
        .start(&ElastiCacheNodeSpec::new(
            replication_group_id.clone(),
            engine,
            Endpoint::localhost(0),
        ))
        .map_err(|source| {
            infrastructure_error("starting ElastiCache backend", source)
        })?;
    let upstream = backend.listen_endpoint();
    let proxy = match proxy_runtime.start(&ElastiCacheProxySpec::new(
        scope.clone(),
        replication_group_id.clone(),
        auth_mode,
        Endpoint::localhost(listen_port.unwrap_or(0)),
        upstream,
        authenticator,
    )) {
        Ok(proxy) => proxy,
        Err(source) => {
            let _ = backend.stop();
            return Err(infrastructure_error(
                "starting ElastiCache proxy",
                source,
            ));
        }
    };

    Ok(StartedRuntime { backend, proxy })
}

pub(crate) fn recover<T>(result: LockResult<T>) -> T {
    result.unwrap_or_else(std::sync::PoisonError::into_inner)
}
