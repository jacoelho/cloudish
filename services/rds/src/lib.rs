mod clusters;
mod errors;
mod identifiers;
mod instances;
mod parameter_groups;
mod runtime;
mod scope;

pub use clusters::{
    CreateDbClusterInput, DbCluster, DbClusterMember, ModifyDbClusterInput,
};
pub use errors::RdsError;
pub use identifiers::{
    DbClusterIdentifier, DbInstanceIdentifier, DbParameterGroupFamily,
    DbParameterGroupName,
};
pub use instances::{
    CreateDbInstanceInput, DbInstance, DbLifecycleStatus,
    ModifyDbInstanceInput, RdsEndpoint, RdsEngine,
};
pub use parameter_groups::{
    CreateDbParameterGroupInput, DbParameter, DbParameterGroup,
    ModifyDbParameterGroupInput,
};
pub use runtime::{
    RdsAuthEndpointSource, RdsBackendRuntime, RdsBackendSpec,
    RdsIamTokenValidator, RdsServiceDependencies,
    RejectAllRdsIamTokenValidator, RunningRdsBackend,
};
pub use scope::RdsScope;

use crate::clusters::{StoredDbCluster, sorted_clusters};
use crate::errors::{infrastructure_error, storage_error};
use crate::instances::{
    StoredDbInstance, sorted_instances, validate_iam_setting,
};
use crate::parameter_groups::{
    StoredDbParameterGroup, parameter_group_family_supported,
    parameter_group_parameters, sorted_parameter_groups,
    validate_parameter_group_engine,
};
use crate::runtime::{
    ClusterRuntime, ClusterRuntimeKey, InstanceRuntime, InstanceRuntimeKey,
    RdsRuntimeState, SharedAuthEndpoints, StartedRuntime, recover,
    start_proxy as start_runtime_proxy,
};
use aws::{
    AccountId, Clock, Endpoint, RegionId, RunningTcpProxy, TcpProxyRuntime,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::UNIX_EPOCH;
use storage::{StorageFactory, StorageHandle};

type RestoredClusterRuntimes = Vec<(ClusterRuntimeKey, ClusterRuntime)>;
type RestoredInstanceRuntimes = Vec<(InstanceRuntimeKey, InstanceRuntime)>;
type RestoredScopeRuntimes =
    (RestoredClusterRuntimes, RestoredInstanceRuntimes);

#[derive(Clone)]
pub struct RdsService {
    backend_runtime: Arc<dyn RdsBackendRuntime + Send + Sync>,
    clock: Arc<dyn Clock>,
    iam_token_validator: Arc<dyn RdsIamTokenValidator + Send + Sync>,
    proxy_runtime: Arc<dyn TcpProxyRuntime + Send + Sync>,
    runtime_state: Arc<Mutex<RdsRuntimeState>>,
    state_store: StorageHandle<RdsStateKey, StoredRdsState>,
}

impl RdsService {
    pub fn new(
        factory: &StorageFactory,
        dependencies: RdsServiceDependencies,
    ) -> Self {
        let RdsServiceDependencies {
            backend_runtime,
            clock,
            iam_token_validator,
            proxy_runtime,
        } = dependencies;

        Self {
            backend_runtime,
            clock,
            iam_token_validator,
            proxy_runtime,
            runtime_state: Arc::new(Mutex::new(RdsRuntimeState::default())),
            state_store: factory.create("rds", "control-plane"),
        }
    }

    /// Restores persisted backend and proxy runtimes for every loaded RDS
    /// scope.
    ///
    /// # Errors
    ///
    /// Returns an error when a persisted backend or proxy cannot be restarted.
    pub fn restore_runtimes(&self) -> Result<(), RdsError> {
        let mut restored_clusters = Vec::new();
        let mut restored_instances = Vec::new();

        for state_key in self.state_store.keys() {
            let scope = RdsScope::new(
                state_key.account_id.clone(),
                state_key.region.clone(),
            );
            let state = self.load_state(&scope);
            match self.restore_scope_runtimes(&scope, &state) {
                Ok((clusters, instances)) => {
                    restored_clusters.extend(clusters);
                    restored_instances.extend(instances);
                }
                Err(error) => {
                    self.stop_restored_runtimes(
                        restored_instances,
                        restored_clusters,
                    );
                    return Err(error);
                }
            }
        }

        let mut runtime = self.lock_runtime();
        if !runtime.clusters.is_empty() || !runtime.instances.is_empty() {
            drop(runtime);
            self.stop_restored_runtimes(restored_instances, restored_clusters);
            return Err(RdsError::internal(
                "RDS runtime restore requires an empty runtime state.",
            ));
        }
        runtime.clusters.extend(restored_clusters);
        runtime.instances.extend(restored_instances);

        Ok(())
    }

    /// Stops every active backend and proxy owned by this service.
    ///
    /// # Errors
    ///
    /// Returns an error when a backend or proxy cannot be stopped cleanly.
    pub fn shutdown(&self) -> Result<(), RdsError> {
        let (instances, clusters) = {
            let mut runtime = self.lock_runtime();
            (
                std::mem::take(&mut runtime.instances),
                std::mem::take(&mut runtime.clusters),
            )
        };

        for (_, runtime) in instances {
            runtime.stop()?;
        }
        for (_, runtime) in clusters {
            runtime.stop()?;
        }

        Ok(())
    }

    /// # Errors
    ///
    /// Returns validation, conflict, runtime, or persistence errors when the
    /// DB instance request is invalid or cannot be started and stored.
    pub fn create_db_instance(
        &self,
        scope: &RdsScope,
        input: CreateDbInstanceInput,
    ) -> Result<DbInstance, RdsError> {
        let identifier = input.db_instance_identifier;
        let mut state = self.load_state(scope);
        if state.instances.contains_key(&identifier) {
            return Err(RdsError::DbInstanceAlreadyExists {
                message: format!("DB instance {identifier} already exists."),
            });
        }

        if let Some(cluster_identifier) = input.db_cluster_identifier.as_ref()
        {
            let cluster =
                state.clusters.get(cluster_identifier).cloned().ok_or_else(
                    || RdsError::DbClusterNotFoundFault {
                        message: format!(
                            "DB cluster {cluster_identifier} not found."
                        ),
                    },
                )?;
            let engine = match input.engine.as_deref() {
                Some(engine) => {
                    let engine = RdsEngine::parse(engine)?;
                    if engine.protocol_family()
                        != cluster.engine.protocol_family()
                    {
                        return Err(RdsError::InvalidParameterCombination {
                            message: format!(
                                "DB instance engine {} is incompatible with DB \
                                 cluster engine {}.",
                                engine.as_str(),
                                cluster.engine.as_str()
                            ),
                        });
                    }
                    if !engine.supports_cluster() {
                        return Err(RdsError::InvalidParameterCombination {
                            message: format!(
                                "DB instance engine {} cannot attach to a DB \
                                 cluster.",
                                engine.as_str()
                            ),
                        });
                    }
                    engine
                }
                None => cluster.engine,
            };
            if let Some(username) = input
                .master_username
                .as_deref()
                .filter(|value| !value.is_empty())
                && username != cluster.master_username
            {
                return Err(RdsError::InvalidParameterCombination {
                    message: "Cluster-backed DB instances inherit the DB \
                              cluster master username."
                        .to_owned(),
                });
            }
            if let Some(password) = input
                .master_user_password
                .as_deref()
                .filter(|value| !value.is_empty())
                && password != cluster.master_password
            {
                return Err(RdsError::InvalidParameterCombination {
                    message: "Cluster-backed DB instances inherit the DB \
                              cluster master password."
                        .to_owned(),
                });
            }
            if let Some(iam_enabled) = input.enable_iam_database_authentication
                && iam_enabled != cluster.iam_database_authentication_enabled
            {
                return Err(RdsError::InvalidParameterCombination {
                    message: "Cluster-backed DB instances inherit the DB \
                              cluster IAM authentication setting."
                        .to_owned(),
                });
            }
            if let Some(database_name) = input.db_name.as_deref()
                && cluster
                    .database_name
                    .as_deref()
                    .is_some_and(|value| value != database_name)
            {
                return Err(RdsError::InvalidParameterCombination {
                    message: "Cluster-backed DB instances inherit the DB \
                              cluster database name."
                        .to_owned(),
                });
            }
            let parameter_group_name =
                input.db_parameter_group_name.as_ref().cloned();
            if let Some(parameter_group_name) = parameter_group_name.as_ref() {
                validate_parameter_group_engine(
                    &state,
                    engine,
                    parameter_group_name,
                )?;
            }

            let cluster_key =
                ClusterRuntimeKey::new(scope, cluster_identifier);
            let upstream = self
                .runtime_state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clusters
                .get(&cluster_key)
                .map(|runtime| runtime.backend.listen_endpoint())
                .ok_or_else(|| {
                    RdsError::internal(format!(
                        "Missing runtime for DB cluster {cluster_identifier}."
                    ))
                })?;
            let proxy = self.start_proxy(&upstream, None)?;
            let proxy_endpoint = proxy.listen_endpoint();
            let public_endpoint = RdsEndpoint::loopback(proxy_endpoint.port());
            let instance = StoredDbInstance {
                allocated_storage: input.allocated_storage.unwrap_or(20),
                db_cluster_identifier: Some(cluster_identifier.clone()),
                db_instance_class: input
                    .db_instance_class
                    .unwrap_or_else(|| "db.t3.micro".to_owned()),
                db_instance_identifier: identifier.clone(),
                db_instance_status: DbLifecycleStatus::Available,
                db_name: cluster.database_name.clone(),
                db_parameter_group_name: parameter_group_name,
                dbi_resource_id: state.next_instance_resource_id(),
                endpoint: public_endpoint,
                engine,
                engine_version: input
                    .engine_version
                    .unwrap_or_else(|| cluster.engine_version.clone()),
                iam_database_authentication_enabled: cluster
                    .iam_database_authentication_enabled,
                instance_create_time_epoch_seconds: self
                    .current_epoch_seconds(),
                master_password: cluster.master_password.clone(),
                master_username: cluster.master_username,
            };

            let runtime_key = InstanceRuntimeKey::new(scope, &identifier);
            let instance_output = instance.to_output(scope);
            if let Some(cluster) = state.clusters.get_mut(cluster_identifier) {
                cluster.db_cluster_members.push(DbClusterMember {
                    db_instance_identifier: identifier.clone(),
                    is_cluster_writer: false,
                });
            }
            state.instances.insert(identifier, instance);
            if let Err(error) = self.save_state(scope, state) {
                let _ = proxy.stop();
                return Err(error);
            }
            let mut runtime = self.lock_runtime();
            runtime
                .clusters
                .get_mut(&cluster_key)
                .ok_or_else(|| {
                    RdsError::internal(format!(
                        "Missing runtime for DB cluster {cluster_identifier}."
                    ))
                })?
                .auth_endpoints
                .add_endpoint(proxy_endpoint);
            runtime
                .instances
                .insert(runtime_key, InstanceRuntime::cluster_member(proxy));

            return Ok(instance_output);
        }

        let engine =
            RdsEngine::parse(input.engine.as_deref().ok_or_else(|| {
                RdsError::InvalidParameterValue {
                    message: "DB instance engine is required.".to_owned(),
                }
            })?)?;
        if engine.supports_cluster() {
            return Err(RdsError::InvalidParameterCombination {
                message: format!(
                    "Engine {} requires DB cluster lifecycle APIs.",
                    engine.as_str()
                ),
            });
        }
        let master_username = required_non_empty(
            "MasterUsername",
            input.master_username.as_deref(),
        )?;
        let master_password = required_non_empty(
            "MasterUserPassword",
            input.master_user_password.as_deref(),
        )?;
        let iam_enabled =
            input.enable_iam_database_authentication.unwrap_or(false);
        validate_iam_setting(engine, iam_enabled)?;
        let parameter_group_name =
            input.db_parameter_group_name.as_ref().cloned();
        if let Some(parameter_group_name) = parameter_group_name.as_ref() {
            validate_parameter_group_engine(
                &state,
                engine,
                parameter_group_name,
            )?;
        }

        let started = self.start_backend_and_proxy(
            engine,
            master_username.to_owned(),
            master_password.to_owned(),
            input.db_name.clone(),
            iam_enabled,
            None,
        )?;
        let instance = StoredDbInstance {
            allocated_storage: input.allocated_storage.unwrap_or(20),
            db_cluster_identifier: None,
            db_instance_class: input
                .db_instance_class
                .unwrap_or_else(|| "db.t3.micro".to_owned()),
            db_instance_identifier: identifier.clone(),
            db_instance_status: DbLifecycleStatus::Available,
            db_name: input.db_name,
            db_parameter_group_name: parameter_group_name,
            dbi_resource_id: state.next_instance_resource_id(),
            endpoint: RdsEndpoint::loopback(started.proxy_port()),
            engine,
            engine_version: input
                .engine_version
                .unwrap_or_else(|| engine.default_engine_version().to_owned()),
            iam_database_authentication_enabled: iam_enabled,
            instance_create_time_epoch_seconds: self.current_epoch_seconds(),
            master_password: master_password.to_owned(),
            master_username: master_username.to_owned(),
        };
        let output = instance.to_output(scope);
        let runtime_key = InstanceRuntimeKey::new(scope, &identifier);
        state.instances.insert(identifier, instance);
        if let Err(error) = self.save_state(scope, state) {
            let _ = started.stop();
            return Err(error);
        }
        self.lock_runtime()
            .instances
            .insert(runtime_key, InstanceRuntime::standalone(started));

        Ok(output)
    }

    /// # Errors
    ///
    /// Returns [`RdsError::DbInstanceNotFound`] when a specific DB instance was
    /// requested but does not exist.
    pub fn describe_db_instances(
        &self,
        scope: &RdsScope,
        db_instance_identifier: Option<&DbInstanceIdentifier>,
    ) -> Result<Vec<DbInstance>, RdsError> {
        let state = self.load_state(scope);

        if let Some(identifier) = db_instance_identifier {
            let instance =
                state.instances.get(identifier).ok_or_else(|| {
                    RdsError::DbInstanceNotFound {
                        message: format!(
                            "DB instance {identifier} not found."
                        ),
                    }
                })?;
            return Ok(vec![instance.to_output(scope)]);
        }

        Ok(sorted_instances(&state)
            .into_iter()
            .map(|instance| instance.to_output(scope))
            .collect())
    }

    /// # Errors
    ///
    /// Returns not-found, validation, runtime, or persistence errors when the
    /// DB instance does not exist or the requested update cannot be applied.
    pub fn modify_db_instance(
        &self,
        scope: &RdsScope,
        db_instance_identifier: &DbInstanceIdentifier,
        input: ModifyDbInstanceInput,
    ) -> Result<DbInstance, RdsError> {
        let identifier = db_instance_identifier.clone();
        let mut state = self.load_state(scope);
        let mut updated =
            state.instances.get(&identifier).cloned().ok_or_else(|| {
                RdsError::DbInstanceNotFound {
                    message: format!("DB instance {identifier} not found."),
                }
            })?;
        if let Some(parameter_group_name) =
            input.db_parameter_group_name.as_ref()
        {
            validate_parameter_group_engine(
                &state,
                updated.engine,
                parameter_group_name,
            )?;
            updated.db_parameter_group_name =
                Some(parameter_group_name.clone());
        }

        if updated.db_cluster_identifier.is_some()
            && (input.master_user_password.is_some()
                || input.enable_iam_database_authentication.is_some())
        {
            return Err(RdsError::InvalidParameterCombination {
                message:
                    "Cluster-backed DB instances inherit credentials and \
                          IAM settings from the DB cluster."
                        .to_owned(),
            });
        }

        let mut restart_needed = false;
        if let Some(password) = input.master_user_password {
            if password.is_empty() {
                return Err(RdsError::InvalidParameterValue {
                    message: "MasterUserPassword must not be blank."
                        .to_owned(),
                });
            }
            updated.master_password = password;
            restart_needed = true;
        }
        if let Some(iam_enabled) = input.enable_iam_database_authentication {
            validate_iam_setting(updated.engine, iam_enabled)?;
            updated.iam_database_authentication_enabled = iam_enabled;
            restart_needed = true;
        }

        if restart_needed && updated.db_cluster_identifier.is_none() {
            let runtime = self.replace_instance_runtime(scope, &updated)?;
            self.lock_runtime().instances.insert(
                InstanceRuntimeKey::new(scope, &identifier),
                InstanceRuntime::standalone(runtime),
            );
        }

        state.instances.insert(identifier, updated.clone());
        self.save_state(scope, state)?;
        Ok(updated.to_output(scope))
    }

    /// # Errors
    ///
    /// Returns not-found or runtime errors when the DB instance does not exist
    /// or its runtime cannot be restarted.
    pub fn reboot_db_instance(
        &self,
        scope: &RdsScope,
        db_instance_identifier: &DbInstanceIdentifier,
    ) -> Result<DbInstance, RdsError> {
        let identifier = db_instance_identifier.clone();
        let state = self.load_state(scope);
        let instance =
            state.instances.get(&identifier).cloned().ok_or_else(|| {
                RdsError::DbInstanceNotFound {
                    message: format!("DB instance {identifier} not found."),
                }
            })?;

        let runtime_key = InstanceRuntimeKey::new(scope, &identifier);
        if let Some(cluster_identifier) =
            instance.db_cluster_identifier.as_ref()
        {
            let cluster_runtime_key =
                ClusterRuntimeKey::new(scope, cluster_identifier);
            let upstream = self
                .lock_runtime()
                .clusters
                .get(&cluster_runtime_key)
                .map(|runtime| runtime.backend.listen_endpoint())
                .ok_or_else(|| {
                    RdsError::internal(format!(
                        "Missing runtime for DB cluster {cluster_identifier}."
                    ))
                })?;
            let old = self
                .lock_runtime()
                .instances
                .remove(&runtime_key)
                .ok_or_else(|| {
                    RdsError::internal(format!(
                        "Missing runtime for DB instance {identifier}."
                    ))
                })?;
            old.stop_proxy()?;
            let proxy =
                self.start_proxy(&upstream, Some(instance.endpoint.port))?;
            self.lock_runtime()
                .instances
                .insert(runtime_key, InstanceRuntime::cluster_member(proxy));
            return Ok(instance.to_output(scope));
        }

        let runtime = self.replace_instance_runtime(scope, &instance)?;
        self.lock_runtime()
            .instances
            .insert(runtime_key, InstanceRuntime::standalone(runtime));
        Ok(instance.to_output(scope))
    }

    /// # Errors
    ///
    /// Returns not-found, runtime, or persistence errors when the DB instance
    /// does not exist or cannot be removed cleanly.
    pub fn delete_db_instance(
        &self,
        scope: &RdsScope,
        db_instance_identifier: &DbInstanceIdentifier,
    ) -> Result<DbInstance, RdsError> {
        let identifier = db_instance_identifier.clone();
        let mut state = self.load_state(scope);
        let instance =
            state.instances.remove(&identifier).ok_or_else(|| {
                RdsError::DbInstanceNotFound {
                    message: format!("DB instance {identifier} not found."),
                }
            })?;
        if let Some(cluster_identifier) =
            instance.db_cluster_identifier.as_ref()
            && let Some(cluster) = state.clusters.get_mut(cluster_identifier)
        {
            cluster
                .db_cluster_members
                .retain(|member| member.db_instance_identifier != identifier);
        }
        self.save_state(scope, state)?;
        if let Some(cluster_identifier) =
            instance.db_cluster_identifier.as_ref()
            && let Some(runtime) = self
                .lock_runtime()
                .clusters
                .get_mut(&ClusterRuntimeKey::new(scope, cluster_identifier))
        {
            runtime
                .auth_endpoints
                .remove_endpoint(&Endpoint::localhost(instance.endpoint.port));
        }

        let runtime_key = InstanceRuntimeKey::new(scope, &identifier);
        if let Some(runtime) =
            self.lock_runtime().instances.remove(&runtime_key)
        {
            runtime.stop()?;
        }

        Ok(instance.to_output(scope))
    }

    /// # Errors
    ///
    /// Returns validation, conflict, runtime, or persistence errors when the
    /// DB cluster request is invalid or cannot be started and stored.
    pub fn create_db_cluster(
        &self,
        scope: &RdsScope,
        input: CreateDbClusterInput,
    ) -> Result<DbCluster, RdsError> {
        let identifier = input.db_cluster_identifier;
        let mut state = self.load_state(scope);
        if state.clusters.contains_key(&identifier) {
            return Err(RdsError::DbClusterAlreadyExistsFault {
                message: format!("DB cluster {identifier} already exists."),
            });
        }
        let engine = RdsEngine::parse(&input.engine)?;
        if !engine.supports_cluster() {
            return Err(RdsError::InvalidParameterCombination {
                message: format!(
                    "Engine {} does not support DB cluster lifecycle APIs.",
                    engine.as_str()
                ),
            });
        }
        let master_username = required_non_empty(
            "MasterUsername",
            Some(&input.master_username),
        )?;
        let master_password = required_non_empty(
            "MasterUserPassword",
            Some(&input.master_user_password),
        )?;
        let iam_enabled =
            input.enable_iam_database_authentication.unwrap_or(false);
        validate_iam_setting(engine, iam_enabled)?;
        let parameter_group_name =
            input.db_cluster_parameter_group_name.as_ref().cloned();
        if let Some(parameter_group_name) = parameter_group_name.as_ref() {
            validate_parameter_group_engine(
                &state,
                engine,
                parameter_group_name,
            )?;
        }

        let started = self.start_backend_and_proxy(
            engine,
            master_username.to_owned(),
            master_password.to_owned(),
            input.database_name.clone(),
            iam_enabled,
            None,
        )?;
        let cluster = StoredDbCluster {
            database_name: input.database_name,
            db_cluster_identifier: identifier.clone(),
            db_cluster_members: Vec::new(),
            db_cluster_parameter_group: parameter_group_name,
            db_cluster_resource_id: state.next_cluster_resource_id(),
            endpoint: RdsEndpoint::loopback(started.proxy_port()),
            engine,
            engine_version: input
                .engine_version
                .unwrap_or_else(|| engine.default_engine_version().to_owned()),
            iam_database_authentication_enabled: iam_enabled,
            master_password: master_password.to_owned(),
            master_username: master_username.to_owned(),
            status: DbLifecycleStatus::Available,
        };
        let output = cluster.to_output(scope);
        let runtime_key = ClusterRuntimeKey::new(scope, &identifier);
        state.clusters.insert(identifier, cluster);
        if let Err(error) = self.save_state(scope, state) {
            let _ = started.stop();
            return Err(error);
        }
        self.lock_runtime().clusters.insert(
            runtime_key,
            ClusterRuntime::new(
                started.auth_endpoints,
                started.backend,
                started.proxy,
            ),
        );

        Ok(output)
    }

    /// # Errors
    ///
    /// Returns [`RdsError::DbClusterNotFoundFault`] when a specific DB cluster
    /// was requested but does not exist.
    pub fn describe_db_clusters(
        &self,
        scope: &RdsScope,
        db_cluster_identifier: Option<&DbClusterIdentifier>,
    ) -> Result<Vec<DbCluster>, RdsError> {
        let state = self.load_state(scope);

        if let Some(identifier) = db_cluster_identifier {
            let cluster = state.clusters.get(identifier).ok_or_else(|| {
                RdsError::DbClusterNotFoundFault {
                    message: format!("DB cluster {identifier} not found."),
                }
            })?;
            return Ok(vec![cluster.to_output(scope)]);
        }

        Ok(sorted_clusters(&state)
            .into_iter()
            .map(|cluster| cluster.to_output(scope))
            .collect())
    }

    /// # Errors
    ///
    /// Returns not-found, validation, runtime, or persistence errors when the
    /// DB cluster does not exist or the requested update cannot be applied.
    pub fn modify_db_cluster(
        &self,
        scope: &RdsScope,
        db_cluster_identifier: &DbClusterIdentifier,
        input: ModifyDbClusterInput,
    ) -> Result<DbCluster, RdsError> {
        let identifier = db_cluster_identifier.clone();
        let mut state = self.load_state(scope);
        let mut updated =
            state.clusters.get(&identifier).cloned().ok_or_else(|| {
                RdsError::DbClusterNotFoundFault {
                    message: format!("DB cluster {identifier} not found."),
                }
            })?;
        if let Some(parameter_group_name) =
            input.db_cluster_parameter_group_name.as_ref()
        {
            validate_parameter_group_engine(
                &state,
                updated.engine,
                parameter_group_name,
            )?;
            updated.db_cluster_parameter_group =
                Some(parameter_group_name.clone());
        }

        let mut restart_needed = false;
        if let Some(password) = input.master_user_password {
            if password.is_empty() {
                return Err(RdsError::InvalidParameterValue {
                    message: "MasterUserPassword must not be blank."
                        .to_owned(),
                });
            }
            updated.master_password = password.clone();
            for instance in state.instances.values_mut().filter(|instance| {
                instance.db_cluster_identifier.as_ref() == Some(&identifier)
            }) {
                instance.master_password = password.clone();
            }
            restart_needed = true;
        }
        if let Some(iam_enabled) = input.enable_iam_database_authentication {
            validate_iam_setting(updated.engine, iam_enabled)?;
            updated.iam_database_authentication_enabled = iam_enabled;
            for instance in state.instances.values_mut().filter(|instance| {
                instance.db_cluster_identifier.as_ref() == Some(&identifier)
            }) {
                instance.iam_database_authentication_enabled = iam_enabled;
            }
            restart_needed = true;
        }

        if restart_needed {
            self.replace_cluster_runtime(scope, &updated, &state)?;
        }

        state.clusters.insert(identifier, updated.clone());
        self.save_state(scope, state)?;
        Ok(updated.to_output(scope))
    }

    /// # Errors
    ///
    /// Returns not-found, invalid-state, runtime, or persistence errors when
    /// the DB cluster cannot be removed cleanly.
    pub fn delete_db_cluster(
        &self,
        scope: &RdsScope,
        db_cluster_identifier: &DbClusterIdentifier,
    ) -> Result<DbCluster, RdsError> {
        let identifier = db_cluster_identifier.clone();
        let mut state = self.load_state(scope);
        let cluster =
            state.clusters.get(&identifier).cloned().ok_or_else(|| {
                RdsError::DbClusterNotFoundFault {
                    message: format!("DB cluster {identifier} not found."),
                }
            })?;
        if !cluster.db_cluster_members.is_empty() {
            return Err(RdsError::InvalidDbClusterStateFault {
                message: format!(
                    "DB cluster {identifier} still has DB instances."
                ),
            });
        }
        state.clusters.remove(&identifier);
        self.save_state(scope, state)?;

        let runtime_key = ClusterRuntimeKey::new(scope, &identifier);
        if let Some(runtime) =
            self.lock_runtime().clusters.remove(&runtime_key)
        {
            runtime.stop()?;
        }

        Ok(cluster.to_output(scope))
    }

    /// # Errors
    ///
    /// Returns validation, conflict, or persistence errors when the parameter
    /// group request is invalid or cannot be stored.
    pub fn create_db_parameter_group(
        &self,
        scope: &RdsScope,
        input: CreateDbParameterGroupInput,
    ) -> Result<DbParameterGroup, RdsError> {
        let name = input.db_parameter_group_name;
        let family = input.db_parameter_group_family;
        if !parameter_group_family_supported(&family) {
            return Err(RdsError::InvalidParameterValue {
                message: format!(
                    "Unsupported DB parameter group family `{family}`."
                ),
            });
        }

        let mut state = self.load_state(scope);
        if state.parameter_groups.contains_key(&name) {
            return Err(RdsError::DbParameterGroupAlreadyExists {
                message: format!("DB parameter group {name} already exists."),
            });
        }
        let group = StoredDbParameterGroup {
            db_parameter_group_family: family,
            db_parameter_group_name: name.clone(),
            description: input.description,
            parameters: BTreeMap::new(),
        };
        let output = group.to_output(scope);
        state.parameter_groups.insert(name, group);
        self.save_state(scope, state)?;

        Ok(output)
    }

    /// # Errors
    ///
    /// Returns [`RdsError::DbParameterGroupNotFound`] when a specific
    /// parameter group was requested but does not exist.
    pub fn describe_db_parameter_groups(
        &self,
        scope: &RdsScope,
        db_parameter_group_name: Option<&DbParameterGroupName>,
    ) -> Result<Vec<DbParameterGroup>, RdsError> {
        let state = self.load_state(scope);

        if let Some(name) = db_parameter_group_name {
            let group = state.parameter_groups.get(name).ok_or_else(|| {
                RdsError::DbParameterGroupNotFound {
                    message: format!("DB parameter group {name} not found."),
                }
            })?;
            return Ok(vec![group.to_output(scope)]);
        }

        Ok(sorted_parameter_groups(&state)
            .into_iter()
            .map(|group| group.to_output(scope))
            .collect())
    }

    /// # Errors
    ///
    /// Returns not-found, validation, or persistence errors when the
    /// parameter group does not exist or the update is invalid.
    pub fn modify_db_parameter_group(
        &self,
        scope: &RdsScope,
        db_parameter_group_name: &DbParameterGroupName,
        input: ModifyDbParameterGroupInput,
    ) -> Result<DbParameterGroup, RdsError> {
        let name = db_parameter_group_name.clone();
        let mut state = self.load_state(scope);
        let group =
            state.parameter_groups.get_mut(&name).ok_or_else(|| {
                RdsError::DbParameterGroupNotFound {
                    message: format!("DB parameter group {name} not found."),
                }
            })?;
        for (parameter_name, parameter_value) in input.parameters {
            let parameter_name = parameter_name.trim().to_ascii_lowercase();
            if parameter_name.is_empty() {
                return Err(RdsError::InvalidParameterValue {
                    message: "ParameterName must not be blank.".to_owned(),
                });
            }
            group.parameters.insert(parameter_name, parameter_value);
        }
        let output = group.to_output(scope);
        self.save_state(scope, state)?;
        Ok(output)
    }

    /// # Errors
    ///
    /// Returns not-found or persistence errors when the parameter group does
    /// not exist or cannot be removed.
    pub fn delete_db_parameter_group(
        &self,
        scope: &RdsScope,
        db_parameter_group_name: &DbParameterGroupName,
    ) -> Result<(), RdsError> {
        let name = db_parameter_group_name.clone();
        let mut state = self.load_state(scope);
        if state.parameter_groups.remove(&name).is_none() {
            return Err(RdsError::DbParameterGroupNotFound {
                message: format!("DB parameter group {name} not found."),
            });
        }
        self.save_state(scope, state)
    }

    /// # Errors
    ///
    /// Returns [`RdsError::DbParameterGroupNotFound`] when the parameter group
    /// does not exist.
    pub fn describe_db_parameters(
        &self,
        scope: &RdsScope,
        db_parameter_group_name: &DbParameterGroupName,
    ) -> Result<Vec<DbParameter>, RdsError> {
        let name = db_parameter_group_name.clone();
        let state = self.load_state(scope);
        let group = state.parameter_groups.get(&name).ok_or_else(|| {
            RdsError::DbParameterGroupNotFound {
                message: format!("DB parameter group {name} not found."),
            }
        })?;

        Ok(parameter_group_parameters(group))
    }

    fn load_state(&self, scope: &RdsScope) -> StoredRdsState {
        self.state_store.get(&RdsStateKey::new(scope)).unwrap_or_default()
    }

    fn save_state(
        &self,
        scope: &RdsScope,
        state: StoredRdsState,
    ) -> Result<(), RdsError> {
        self.state_store
            .put(RdsStateKey::new(scope), state)
            .map_err(|source| storage_error("writing RDS state", source))
    }

    fn current_epoch_seconds(&self) -> u64 {
        self.clock
            .now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
    }

    fn lock_runtime(&self) -> std::sync::MutexGuard<'_, RdsRuntimeState> {
        recover(self.runtime_state.lock())
    }

    fn restore_scope_runtimes(
        &self,
        scope: &RdsScope,
        state: &StoredRdsState,
    ) -> Result<RestoredScopeRuntimes, RdsError> {
        let mut restored_clusters = Vec::new();
        let mut restored_instances = Vec::new();
        let mut cluster_upstreams = BTreeMap::new();

        for cluster in sorted_clusters(state) {
            let started = match self.start_backend_and_proxy(
                cluster.engine,
                cluster.master_username.clone(),
                cluster.master_password.clone(),
                cluster.database_name.clone(),
                cluster.iam_database_authentication_enabled,
                Some(cluster.endpoint.port),
            ) {
                Ok(started) => started,
                Err(error) => {
                    self.stop_restored_runtimes(
                        restored_instances,
                        restored_clusters,
                    );
                    return Err(error);
                }
            };
            let upstream = started.backend.listen_endpoint();
            let auth_endpoints = started.auth_endpoints.clone();
            cluster_upstreams.insert(
                cluster.db_cluster_identifier.clone(),
                (upstream, auth_endpoints.clone()),
            );
            restored_clusters.push((
                ClusterRuntimeKey::new(scope, &cluster.db_cluster_identifier),
                ClusterRuntime::new(
                    auth_endpoints,
                    started.backend,
                    started.proxy,
                ),
            ));
        }

        for instance in sorted_instances(state)
            .into_iter()
            .filter(|instance| instance.db_cluster_identifier.is_none())
        {
            let started = match self.start_backend_and_proxy(
                instance.engine,
                instance.master_username.clone(),
                instance.master_password.clone(),
                instance.db_name.clone(),
                instance.iam_database_authentication_enabled,
                Some(instance.endpoint.port),
            ) {
                Ok(started) => started,
                Err(error) => {
                    self.stop_restored_runtimes(
                        restored_instances,
                        restored_clusters,
                    );
                    return Err(error);
                }
            };
            restored_instances.push((
                InstanceRuntimeKey::new(
                    scope,
                    &instance.db_instance_identifier,
                ),
                InstanceRuntime::standalone(started),
            ));
        }

        for instance in sorted_instances(state)
            .into_iter()
            .filter(|instance| instance.db_cluster_identifier.is_some())
        {
            let Some(cluster_identifier) =
                instance.db_cluster_identifier.as_ref()
            else {
                self.stop_restored_runtimes(
                    restored_instances,
                    restored_clusters,
                );
                return Err(RdsError::internal(format!(
                    "Missing persisted cluster identifier for DB instance {}.",
                    instance.db_instance_identifier
                )));
            };
            let Some((upstream, auth_endpoints)) =
                cluster_upstreams.get(cluster_identifier).cloned()
            else {
                self.stop_restored_runtimes(
                    restored_instances,
                    restored_clusters,
                );
                return Err(RdsError::internal(format!(
                    "Missing persisted runtime for DB cluster {cluster_identifier}."
                )));
            };
            let proxy = match self
                .start_proxy(&upstream, Some(instance.endpoint.port))
            {
                Ok(proxy) => proxy,
                Err(error) => {
                    self.stop_restored_runtimes(
                        restored_instances,
                        restored_clusters,
                    );
                    return Err(error);
                }
            };
            auth_endpoints.add_endpoint(proxy.listen_endpoint());
            restored_instances.push((
                InstanceRuntimeKey::new(
                    scope,
                    &instance.db_instance_identifier,
                ),
                InstanceRuntime::cluster_member(proxy),
            ));
        }

        Ok((restored_clusters, restored_instances))
    }

    fn stop_restored_runtimes(
        &self,
        restored_instances: Vec<(InstanceRuntimeKey, InstanceRuntime)>,
        restored_clusters: Vec<(ClusterRuntimeKey, ClusterRuntime)>,
    ) {
        for (_, runtime) in restored_instances {
            let _ = runtime.stop();
        }
        for (_, runtime) in restored_clusters {
            let _ = runtime.stop();
        }
    }

    fn start_backend_and_proxy(
        &self,
        engine: RdsEngine,
        master_username: String,
        master_password: String,
        database_name: Option<String>,
        iam_enabled: bool,
        listen_port: Option<u16>,
    ) -> Result<StartedRuntime, RdsError> {
        let auth_endpoints = SharedAuthEndpoints::default();
        let backend = self
            .backend_runtime
            .start(&RdsBackendSpec {
                auth_endpoint_source: Arc::new(auth_endpoints.clone()),
                database_name,
                engine,
                iam_token_validator: iam_enabled
                    .then(|| Arc::clone(&self.iam_token_validator)),
                listen_endpoint: Endpoint::localhost(0),
                master_password,
                master_username,
            })
            .map_err(|source| {
                infrastructure_error("starting RDS backend", source)
            })?;
        let upstream = backend.listen_endpoint();
        let proxy = match self.start_proxy(&upstream, listen_port) {
            Ok(proxy) => proxy,
            Err(error) => {
                let _ = backend.stop();
                return Err(error);
            }
        };
        auth_endpoints.set_endpoints(vec![proxy.listen_endpoint()]);

        Ok(StartedRuntime { auth_endpoints, backend, proxy })
    }

    fn start_proxy(
        &self,
        upstream: &Endpoint,
        listen_port: Option<u16>,
    ) -> Result<Box<dyn RunningTcpProxy>, RdsError> {
        start_runtime_proxy(&self.proxy_runtime, upstream, listen_port)
    }

    fn replace_instance_runtime(
        &self,
        scope: &RdsScope,
        instance: &StoredDbInstance,
    ) -> Result<StartedRuntime, RdsError> {
        let runtime_key =
            InstanceRuntimeKey::new(scope, &instance.db_instance_identifier);
        let old_runtime =
            self.lock_runtime().instances.remove(&runtime_key).ok_or_else(
                || {
                    RdsError::internal(format!(
                        "Missing runtime for DB instance {}.",
                        instance.db_instance_identifier
                    ))
                },
            )?;
        let old_proxy_port = instance.endpoint.port;
        old_runtime.stop()?;
        self.start_backend_and_proxy(
            instance.engine,
            instance.master_username.clone(),
            instance.master_password.clone(),
            instance.db_name.clone(),
            instance.iam_database_authentication_enabled,
            Some(old_proxy_port),
        )
    }

    fn replace_cluster_runtime(
        &self,
        scope: &RdsScope,
        cluster: &StoredDbCluster,
        state: &StoredRdsState,
    ) -> Result<(), RdsError> {
        let cluster_key =
            ClusterRuntimeKey::new(scope, &cluster.db_cluster_identifier);
        let old_cluster_runtime =
            self.lock_runtime().clusters.remove(&cluster_key).ok_or_else(
                || {
                    RdsError::internal(format!(
                        "Missing runtime for DB cluster {}.",
                        cluster.db_cluster_identifier
                    ))
                },
            )?;
        let member_keys = state
            .instances
            .values()
            .filter(|instance| {
                instance.db_cluster_identifier.as_ref()
                    == Some(&cluster.db_cluster_identifier)
            })
            .map(|instance| {
                (
                    InstanceRuntimeKey::new(
                        scope,
                        &instance.db_instance_identifier,
                    ),
                    instance.endpoint.port,
                )
            })
            .collect::<Vec<_>>();

        let mut old_member_runtimes = Vec::with_capacity(member_keys.len());
        for (member_key, port) in &member_keys {
            let member_runtime =
                self.lock_runtime().instances.remove(member_key).ok_or_else(
                    || {
                        RdsError::internal(format!(
                            "Missing runtime for DB instance {}.",
                            member_key.db_instance_identifier
                        ))
                    },
                )?;
            old_member_runtimes.push((
                member_key.clone(),
                *port,
                member_runtime,
            ));
        }

        for (_, _, member_runtime) in old_member_runtimes {
            member_runtime.stop_proxy()?;
        }

        old_cluster_runtime.stop()?;

        let started = self.start_backend_and_proxy(
            cluster.engine,
            cluster.master_username.clone(),
            cluster.master_password.clone(),
            cluster.database_name.clone(),
            cluster.iam_database_authentication_enabled,
            Some(cluster.endpoint.port),
        )?;
        let upstream = started.backend.listen_endpoint();
        let mut member_proxies = Vec::with_capacity(member_keys.len());
        for (member_key, port) in member_keys {
            match self.start_proxy(&upstream, Some(port)) {
                Ok(proxy) => member_proxies.push((member_key, proxy)),
                Err(error) => {
                    for (_, proxy) in member_proxies {
                        let _ = proxy.stop();
                    }
                    let _ = started.stop();
                    return Err(error);
                }
            }
        }

        for (member_key, proxy) in member_proxies {
            started.auth_endpoints.add_endpoint(proxy.listen_endpoint());
            self.lock_runtime()
                .instances
                .insert(member_key, InstanceRuntime::cluster_member(proxy));
        }

        self.lock_runtime().clusters.insert(
            cluster_key,
            ClusterRuntime::new(
                started.auth_endpoints,
                started.backend,
                started.proxy,
            ),
        );
        Ok(())
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
struct RdsStateKey {
    account_id: AccountId,
    region: RegionId,
}

impl RdsStateKey {
    fn new(scope: &RdsScope) -> Self {
        Self {
            account_id: scope.account_id().clone(),
            region: scope.region().clone(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub(crate) struct StoredRdsState {
    pub(crate) next_cluster_resource_id: u64,
    pub(crate) next_instance_resource_id: u64,
    pub(crate) clusters: BTreeMap<DbClusterIdentifier, StoredDbCluster>,
    pub(crate) instances: BTreeMap<DbInstanceIdentifier, StoredDbInstance>,
    pub(crate) parameter_groups:
        BTreeMap<DbParameterGroupName, StoredDbParameterGroup>,
}

impl StoredRdsState {
    pub(crate) fn next_cluster_resource_id(&mut self) -> String {
        self.next_cluster_resource_id += 1;
        format!("cluster-cloudish-{:08}", self.next_cluster_resource_id)
    }

    pub(crate) fn next_instance_resource_id(&mut self) -> String {
        self.next_instance_resource_id += 1;
        format!("dbi-cloudish-{:08}", self.next_instance_resource_id)
    }
}

fn required_non_empty<'a>(
    name: &str,
    value: Option<&'a str>,
) -> Result<&'a str, RdsError> {
    value.map(str::trim).filter(|value| !value.is_empty()).ok_or_else(|| {
        RdsError::InvalidParameterValue {
            message: format!("{name} is required."),
        }
    })
}

#[cfg(test)]
mod tests {
    use super::{
        CreateDbClusterInput, CreateDbInstanceInput,
        CreateDbParameterGroupInput, DbClusterIdentifier, DbClusterMember,
        DbInstanceIdentifier, DbParameterGroup, DbParameterGroupFamily,
        DbParameterGroupName, ModifyDbInstanceInput,
        ModifyDbParameterGroupInput, RdsBackendRuntime, RdsEngine,
        RdsIamTokenValidator, RdsScope, RdsService, RdsServiceDependencies,
        RejectAllRdsIamTokenValidator, RunningRdsBackend,
    };
    use aws::{
        Clock, Endpoint, InfrastructureError, RunningTcpProxy,
        TcpProxyRuntime, TcpProxySpec,
    };
    use std::collections::BTreeMap;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU16, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::UNIX_EPOCH;
    use storage::{StorageConfig, StorageFactory, StorageMode};

    fn temporary_directory(label: &str) -> PathBuf {
        static NEXT_ID: AtomicUsize = AtomicUsize::new(0);

        let path = std::env::temp_dir().join(format!(
            "cloudish-services-rds-{label}-{}-{}",
            std::process::id(),
            NEXT_ID.fetch_add(1, Ordering::Relaxed),
        ));
        if path.exists() {
            let _ = std::fs::remove_dir_all(&path);
        }
        std::fs::create_dir_all(&path)
            .expect("RDS service test directory should be creatable");
        path
    }

    #[derive(Debug, Clone, Copy)]
    struct FixedClock;

    impl Clock for FixedClock {
        fn now(&self) -> std::time::SystemTime {
            UNIX_EPOCH + std::time::Duration::from_secs(1_711_111_111)
        }
    }

    #[derive(Debug, Default)]
    struct FakeTokenValidator;

    impl RdsIamTokenValidator for FakeTokenValidator {
        fn validate(
            &self,
            _endpoint: &Endpoint,
            _username: &str,
            token: &str,
        ) -> Result<(), String> {
            if token == "valid-token" {
                Ok(())
            } else {
                Err("invalid token".to_owned())
            }
        }
    }

    #[derive(Debug, Default)]
    struct FakeBackendRuntime {
        next_port: AtomicU16,
        starts: Mutex<Vec<RdsEngine>>,
        stops: Arc<Mutex<Vec<u16>>>,
    }

    impl FakeBackendRuntime {
        fn new() -> Self {
            Self {
                next_port: AtomicU16::new(6_100),
                starts: Mutex::new(Vec::new()),
                stops: Arc::new(Mutex::new(Vec::new())),
            }
        }
    }

    impl RdsBackendRuntime for FakeBackendRuntime {
        fn start(
            &self,
            spec: &super::RdsBackendSpec,
        ) -> Result<Box<dyn RunningRdsBackend>, InfrastructureError> {
            self.starts.lock().expect("starts should lock").push(spec.engine);
            let port = self.next_port.fetch_add(1, Ordering::Relaxed) + 1;
            Ok(Box::new(FakeRunningBackend {
                endpoint: Endpoint::localhost(port),
                stops: Arc::clone(&self.stops),
            }))
        }
    }

    struct FakeRunningBackend {
        endpoint: Endpoint,
        stops: Arc<Mutex<Vec<u16>>>,
    }

    impl RunningRdsBackend for FakeRunningBackend {
        fn listen_endpoint(&self) -> Endpoint {
            self.endpoint.clone()
        }

        fn stop(&self) -> Result<(), InfrastructureError> {
            self.stops
                .lock()
                .expect("stops should lock")
                .push(self.endpoint.port());
            Ok(())
        }
    }

    #[derive(Debug, Default)]
    struct FakeProxyRuntime {
        next_port: AtomicU16,
        starts: Mutex<Vec<(u16, u16)>>,
        stops: Arc<Mutex<Vec<u16>>>,
    }

    impl FakeProxyRuntime {
        fn new() -> Self {
            Self {
                next_port: AtomicU16::new(7_000),
                starts: Mutex::new(Vec::new()),
                stops: Arc::new(Mutex::new(Vec::new())),
            }
        }
    }

    impl TcpProxyRuntime for FakeProxyRuntime {
        fn start(
            &self,
            spec: &TcpProxySpec,
        ) -> Result<Box<dyn RunningTcpProxy>, InfrastructureError> {
            let listen_port = if spec.listen().port() == 0 {
                self.next_port.fetch_add(1, Ordering::Relaxed) + 1
            } else {
                spec.listen().port()
            };
            self.starts
                .lock()
                .expect("starts should lock")
                .push((listen_port, spec.upstream().port()));
            Ok(Box::new(FakeRunningProxy {
                endpoint: Endpoint::localhost(listen_port),
                stops: Arc::clone(&self.stops),
            }))
        }
    }

    struct FakeRunningProxy {
        endpoint: Endpoint,
        stops: Arc<Mutex<Vec<u16>>>,
    }

    impl RunningTcpProxy for FakeRunningProxy {
        fn listen_endpoint(&self) -> Endpoint {
            self.endpoint.clone()
        }

        fn stop(&self) -> Result<(), InfrastructureError> {
            self.stops
                .lock()
                .expect("stops should lock")
                .push(self.endpoint.port());
            Ok(())
        }
    }

    fn service() -> (RdsService, Arc<FakeBackendRuntime>, Arc<FakeProxyRuntime>)
    {
        let backend = Arc::new(FakeBackendRuntime::new());
        let proxy = Arc::new(FakeProxyRuntime::new());
        let factory = StorageFactory::new(StorageConfig::new(
            temporary_directory("rds-service-tests"),
            StorageMode::Memory,
        ));

        (
            RdsService::new(
                &factory,
                RdsServiceDependencies {
                    backend_runtime: backend.clone(),
                    clock: Arc::new(FixedClock),
                    iam_token_validator: Arc::new(FakeTokenValidator),
                    proxy_runtime: proxy.clone(),
                },
            ),
            backend,
            proxy,
        )
    }

    fn persistent_service(
        directory: PathBuf,
        backend_runtime: Arc<FakeBackendRuntime>,
        proxy_runtime: Arc<FakeProxyRuntime>,
    ) -> RdsService {
        std::fs::create_dir_all(&directory)
            .expect("persistent RDS state directory should exist");

        RdsService::new(
            &StorageFactory::new(StorageConfig::new(
                directory,
                StorageMode::Persistent,
            )),
            RdsServiceDependencies {
                backend_runtime,
                clock: Arc::new(FixedClock),
                iam_token_validator: Arc::new(FakeTokenValidator),
                proxy_runtime,
            },
        )
    }

    fn scope() -> RdsScope {
        RdsScope::new(
            "000000000000".parse().expect("account should parse"),
            "eu-west-2".parse().expect("region should parse"),
        )
    }

    fn db_instance_identifier(value: &str) -> DbInstanceIdentifier {
        DbInstanceIdentifier::new(value)
            .expect("DB instance identifier should be valid")
    }

    fn db_cluster_identifier(value: &str) -> DbClusterIdentifier {
        DbClusterIdentifier::new(value)
            .expect("DB cluster identifier should be valid")
    }

    fn db_parameter_group_name(value: &str) -> DbParameterGroupName {
        DbParameterGroupName::new(value)
            .expect("DB parameter group name should be valid")
    }

    fn db_parameter_group_family(value: &str) -> DbParameterGroupFamily {
        DbParameterGroupFamily::new(value)
            .expect("DB parameter group family should be valid")
    }

    #[test]
    fn rds_instance_lifecycle_starts_and_restarts_runtime() {
        let (service, backend, proxy) = service();
        let scope = scope();

        let created = service
            .create_db_instance(
                &scope,
                CreateDbInstanceInput {
                    allocated_storage: Some(20),
                    db_cluster_identifier: None,
                    db_instance_class: None,
                    db_instance_identifier: db_instance_identifier("demo"),
                    db_name: Some("app".to_owned()),
                    db_parameter_group_name: None,
                    engine: Some("postgres".to_owned()),
                    engine_version: None,
                    enable_iam_database_authentication: Some(true),
                    master_user_password: Some("secret123".to_owned()),
                    master_username: Some("postgres".to_owned()),
                },
            )
            .expect("instance should create");

        assert_eq!(created.engine, RdsEngine::Postgres);
        assert!(created.endpoint.port >= 7_001);
        assert_eq!(
            service
                .describe_db_instances(
                    &scope,
                    Some(&db_instance_identifier("demo")),
                )
                .expect("instance should describe")[0]
                .endpoint
                .port,
            created.endpoint.port
        );

        let modified = service
            .modify_db_instance(
                &scope,
                &db_instance_identifier("demo"),
                ModifyDbInstanceInput {
                    db_parameter_group_name: None,
                    enable_iam_database_authentication: Some(false),
                    master_user_password: Some("newsecret".to_owned()),
                },
            )
            .expect("instance should modify");
        assert!(!modified.iam_database_authentication_enabled);

        let rebooted = service
            .reboot_db_instance(&scope, &db_instance_identifier("demo"))
            .expect("instance should reboot");
        assert_eq!(rebooted.endpoint.port, created.endpoint.port);

        let deleted = service
            .delete_db_instance(&scope, &db_instance_identifier("demo"))
            .expect("instance should delete");
        assert_eq!(deleted.db_instance_identifier, "demo");
        assert_eq!(
            backend.starts.lock().expect("starts should lock").len(),
            3
        );
        assert_eq!(proxy.stops.lock().expect("stops should lock").len(), 3);
    }

    #[test]
    fn rds_cluster_members_block_cluster_delete_and_share_backend() {
        let (service, backend, proxy) = service();
        let scope = scope();

        let cluster = service
            .create_db_cluster(
                &scope,
                CreateDbClusterInput {
                    database_name: Some("clusterdb".to_owned()),
                    db_cluster_identifier: db_cluster_identifier(
                        "cluster-demo",
                    ),
                    db_cluster_parameter_group_name: None,
                    engine: "aurora-postgresql".to_owned(),
                    engine_version: None,
                    enable_iam_database_authentication: Some(true),
                    master_user_password: "cluster-secret".to_owned(),
                    master_username: "postgres".to_owned(),
                },
            )
            .expect("cluster should create");
        assert_eq!(cluster.engine, RdsEngine::AuroraPostgresql);

        let member = service
            .create_db_instance(
                &scope,
                CreateDbInstanceInput {
                    allocated_storage: None,
                    db_cluster_identifier: Some(db_cluster_identifier(
                        "cluster-demo",
                    )),
                    db_instance_class: None,
                    db_instance_identifier: db_instance_identifier("member-1"),
                    db_name: None,
                    db_parameter_group_name: None,
                    engine: Some("aurora-postgresql".to_owned()),
                    engine_version: None,
                    enable_iam_database_authentication: Some(true),
                    master_user_password: Some("cluster-secret".to_owned()),
                    master_username: Some("postgres".to_owned()),
                },
            )
            .expect("member should create");
        assert_ne!(cluster.endpoint.port, member.endpoint.port);

        let delete_error = service
            .delete_db_cluster(&scope, &db_cluster_identifier("cluster-demo"))
            .expect_err("cluster with members should fail");
        assert_eq!(
            delete_error.to_aws_error().code(),
            "InvalidDBClusterStateFault"
        );

        service
            .delete_db_instance(&scope, &db_instance_identifier("member-1"))
            .expect("member should delete");
        service
            .delete_db_cluster(&scope, &db_cluster_identifier("cluster-demo"))
            .expect("cluster should delete after member removal");

        assert_eq!(
            backend.starts.lock().expect("starts should lock").len(),
            1
        );
        assert_eq!(proxy.starts.lock().expect("starts should lock").len(), 2);
    }

    #[test]
    fn rds_restore_runtimes_rehydrates_persisted_proxy_ports() {
        let directory = temporary_directory("rds-runtime-restore");
        let scope = scope();
        let backend = Arc::new(FakeBackendRuntime::new());
        let proxy = Arc::new(FakeProxyRuntime::new());
        let service = persistent_service(directory.clone(), backend, proxy);

        let cluster = service
            .create_db_cluster(
                &scope,
                CreateDbClusterInput {
                    database_name: Some("clusterdb".to_owned()),
                    db_cluster_identifier: db_cluster_identifier(
                        "cluster-demo",
                    ),
                    db_cluster_parameter_group_name: None,
                    engine: "aurora-postgresql".to_owned(),
                    engine_version: None,
                    enable_iam_database_authentication: Some(true),
                    master_user_password: "cluster-secret".to_owned(),
                    master_username: "postgres".to_owned(),
                },
            )
            .expect("cluster should create");
        let standalone = service
            .create_db_instance(
                &scope,
                CreateDbInstanceInput {
                    allocated_storage: Some(20),
                    db_cluster_identifier: None,
                    db_instance_class: None,
                    db_instance_identifier: db_instance_identifier("solo"),
                    db_name: Some("app".to_owned()),
                    db_parameter_group_name: None,
                    engine: Some("postgres".to_owned()),
                    engine_version: None,
                    enable_iam_database_authentication: Some(true),
                    master_user_password: Some("secret123".to_owned()),
                    master_username: Some("postgres".to_owned()),
                },
            )
            .expect("standalone instance should create");
        let member = service
            .create_db_instance(
                &scope,
                CreateDbInstanceInput {
                    allocated_storage: None,
                    db_cluster_identifier: Some(db_cluster_identifier(
                        "cluster-demo",
                    )),
                    db_instance_class: None,
                    db_instance_identifier: db_instance_identifier("member"),
                    db_name: None,
                    db_parameter_group_name: None,
                    engine: Some("aurora-postgresql".to_owned()),
                    engine_version: None,
                    enable_iam_database_authentication: Some(true),
                    master_user_password: Some("cluster-secret".to_owned()),
                    master_username: Some("postgres".to_owned()),
                },
            )
            .expect("cluster member should create");

        let restored_backend = Arc::new(FakeBackendRuntime::new());
        let restored_proxy = Arc::new(FakeProxyRuntime::new());
        let restored_service = persistent_service(
            directory,
            restored_backend.clone(),
            restored_proxy.clone(),
        );
        restored_service.state_store.load().expect("RDS state should load");
        restored_service
            .restore_runtimes()
            .expect("RDS runtimes should restore");

        assert_eq!(
            restored_service
                .describe_db_clusters(
                    &scope,
                    Some(&db_cluster_identifier("cluster-demo")),
                )
                .expect("cluster should describe")[0]
                .endpoint
                .port,
            cluster.endpoint.port
        );
        assert_eq!(
            restored_service
                .describe_db_instances(
                    &scope,
                    Some(&db_instance_identifier("solo"))
                )
                .expect("standalone instance should describe")[0]
                .endpoint
                .port,
            standalone.endpoint.port
        );
        assert_eq!(
            restored_service
                .describe_db_instances(
                    &scope,
                    Some(&db_instance_identifier("member")),
                )
                .expect("cluster member should describe")[0]
                .endpoint
                .port,
            member.endpoint.port
        );
        assert_eq!(
            restored_backend
                .starts
                .lock()
                .expect("starts should lock")
                .clone(),
            vec![RdsEngine::AuroraPostgresql, RdsEngine::Postgres]
        );

        let started_proxies =
            restored_proxy.starts.lock().expect("starts should lock").clone();
        let cluster_start = started_proxies
            .iter()
            .find(|(listen_port, _)| *listen_port == cluster.endpoint.port)
            .expect("cluster proxy should restart");
        let standalone_start = started_proxies
            .iter()
            .find(|(listen_port, _)| *listen_port == standalone.endpoint.port)
            .expect("standalone proxy should restart");
        let member_start = started_proxies
            .iter()
            .find(|(listen_port, _)| *listen_port == member.endpoint.port)
            .expect("member proxy should restart");

        assert_eq!(cluster_start.1, 6_101);
        assert_eq!(standalone_start.1, 6_102);
        assert_eq!(member_start.1, 6_101);
    }

    #[test]
    fn rds_parameter_groups_validate_engine_families() {
        let (service, _, _) = service();
        let scope = scope();

        let created = service
            .create_db_parameter_group(
                &scope,
                CreateDbParameterGroupInput {
                    db_parameter_group_family: db_parameter_group_family(
                        "postgres16",
                    ),
                    db_parameter_group_name: db_parameter_group_name(
                        "pg-demo",
                    ),
                    description: "Postgres parameters".to_owned(),
                },
            )
            .expect("parameter group should create");
        assert_eq!(created.db_parameter_group_family, "postgres16");

        let mismatch = service
            .create_db_instance(
                &scope,
                CreateDbInstanceInput {
                    allocated_storage: None,
                    db_cluster_identifier: None,
                    db_instance_class: None,
                    db_instance_identifier: db_instance_identifier(
                        "mysql-demo",
                    ),
                    db_name: None,
                    db_parameter_group_name: Some(db_parameter_group_name(
                        "pg-demo",
                    )),
                    engine: Some("mysql".to_owned()),
                    engine_version: None,
                    enable_iam_database_authentication: Some(false),
                    master_user_password: Some("secret123".to_owned()),
                    master_username: Some("admin".to_owned()),
                },
            )
            .expect_err("mismatched parameter group should fail");
        assert_eq!(
            mismatch.to_aws_error().code(),
            "InvalidParameterCombination"
        );

        let modified = service
            .modify_db_parameter_group(
                &scope,
                &db_parameter_group_name("pg-demo"),
                ModifyDbParameterGroupInput {
                    parameters: BTreeMap::from([(
                        "max_connections".to_owned(),
                        "200".to_owned(),
                    )]),
                },
            )
            .expect("parameter group should modify");
        assert_eq!(modified.db_parameter_group_name, "pg-demo");

        let parameters = service
            .describe_db_parameters(
                &scope,
                &db_parameter_group_name("pg-demo"),
            )
            .expect("parameters should describe");
        assert!(parameters.iter().any(|parameter| {
            parameter.parameter_name == "max_connections"
                && parameter.parameter_value == "200"
                && parameter.source == "user"
        }));
    }

    #[test]
    fn rds_rejects_mysql_iam_enablement_and_unknown_engines() {
        let (service, _, _) = service();
        let scope = scope();

        let unsupported_engine = service
            .create_db_instance(
                &scope,
                CreateDbInstanceInput {
                    allocated_storage: None,
                    db_cluster_identifier: None,
                    db_instance_class: None,
                    db_instance_identifier: db_instance_identifier("bad"),
                    db_name: None,
                    db_parameter_group_name: None,
                    engine: Some("oracle-ee".to_owned()),
                    engine_version: None,
                    enable_iam_database_authentication: Some(false),
                    master_user_password: Some("secret123".to_owned()),
                    master_username: Some("admin".to_owned()),
                },
            )
            .expect_err("unsupported engine should fail");
        assert_eq!(
            unsupported_engine.to_aws_error().code(),
            "InvalidParameterValue"
        );

        let mysql_iam = service
            .create_db_instance(
                &scope,
                CreateDbInstanceInput {
                    allocated_storage: None,
                    db_cluster_identifier: None,
                    db_instance_class: None,
                    db_instance_identifier: db_instance_identifier(
                        "mysql-iam",
                    ),
                    db_name: None,
                    db_parameter_group_name: None,
                    engine: Some("mysql".to_owned()),
                    engine_version: None,
                    enable_iam_database_authentication: Some(true),
                    master_user_password: Some("secret123".to_owned()),
                    master_username: Some("admin".to_owned()),
                },
            )
            .expect_err("mysql IAM should fail");
        assert_eq!(
            mysql_iam.to_aws_error().code(),
            "InvalidParameterCombination"
        );
    }

    #[test]
    fn rds_delete_missing_parameter_group_reports_not_found() {
        let (service, _, _) = service();
        let scope = scope();
        let error = service
            .delete_db_parameter_group(
                &scope,
                &db_parameter_group_name("missing"),
            )
            .expect_err("missing parameter group should fail");
        assert_eq!(error.to_aws_error().code(), "DBParameterGroupNotFound");
        assert!(
            RejectAllRdsIamTokenValidator
                .validate(&Endpoint::localhost(5432), "postgres", "token")
                .is_err()
        );
        assert_eq!(
            DbParameterGroup {
                db_parameter_group_arn: "arn".to_owned(),
                db_parameter_group_family: db_parameter_group_family(
                    "postgres16",
                ),
                db_parameter_group_name: db_parameter_group_name("group"),
                description: "desc".to_owned(),
            }
            .db_parameter_group_name,
            "group"
        );
        assert_eq!(
            DbClusterMember {
                db_instance_identifier: db_instance_identifier("member"),
                is_cluster_writer: false,
            }
            .db_instance_identifier,
            "member"
        );
    }
}
