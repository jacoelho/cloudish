mod auth;
mod errors;
mod identifiers;
mod replication_groups;
mod runtime;
mod scope;
mod users;

pub use auth::{
    AuthenticationModeInput, ElastiCacheAuthenticationType,
    ElastiCacheIamTokenValidator, RejectAllElastiCacheIamTokenValidator,
    UserAuthentication,
};
pub use errors::ElastiCacheError;
pub use identifiers::{ElastiCacheReplicationGroupId, ElastiCacheUserId};
pub use replication_groups::{
    CreateReplicationGroupInput, ElastiCacheEndpoint, ElastiCacheEngine,
    ElastiCacheNodeGroup, ElastiCacheNodeGroupMember, ReplicationGroup,
    ReplicationGroupStatus,
};
pub use runtime::{
    ElastiCacheConnectionAuthenticator, ElastiCacheNodeRuntime,
    ElastiCacheNodeSpec, ElastiCacheProxyRuntime, ElastiCacheProxySpec,
    ElastiCacheServiceDependencies, RunningElastiCacheNode,
    RunningElastiCacheProxy,
};
pub use scope::ElastiCacheScope;
pub use users::{CreateUserInput, ElastiCacheUser, ModifyUserInput};

use crate::auth::resolve_user_authentication;
#[cfg(test)]
use crate::errors::infrastructure_error;
use crate::errors::storage_error;
use crate::replication_groups::{
    StoredReplicationGroup, reject_unsupported_replication_group_inputs,
};
use crate::runtime::{
    ElastiCacheRuntimeState, ReplicationGroupRuntime,
    ReplicationGroupRuntimeKey, RuntimeStartInput, recover, start_runtime,
};
use crate::users::{
    REQUESTED_STATUS_ACTIVE, REQUESTED_STATUS_DELETING, StoredElastiCacheUser,
    normalized_access_string, updated_access_string,
};
use aws::{AccountId, Clock, RegionId};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::UNIX_EPOCH;
use storage::{StorageFactory, StorageHandle};

#[derive(Clone)]
pub struct ElastiCacheService {
    clock: Arc<dyn Clock>,
    iam_token_validator: Arc<dyn ElastiCacheIamTokenValidator + Send + Sync>,
    node_runtime: Arc<dyn ElastiCacheNodeRuntime + Send + Sync>,
    proxy_runtime: Arc<dyn ElastiCacheProxyRuntime + Send + Sync>,
    runtime_state: Arc<Mutex<ElastiCacheRuntimeState>>,
    state_store: StorageHandle<ElastiCacheStateKey, StoredElastiCacheState>,
}

impl fmt::Debug for ElastiCacheService {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ElastiCacheService")
    }
}

impl ElastiCacheService {
    pub fn new(
        factory: &StorageFactory,
        dependencies: ElastiCacheServiceDependencies,
    ) -> Self {
        let ElastiCacheServiceDependencies {
            clock,
            iam_token_validator,
            node_runtime,
            proxy_runtime,
        } = dependencies;

        Self {
            clock,
            iam_token_validator,
            node_runtime,
            proxy_runtime,
            runtime_state: Arc::new(Mutex::new(
                ElastiCacheRuntimeState::default(),
            )),
            state_store: factory.create("elasticache", "control-plane"),
        }
    }

    /// Restores persisted backend and proxy runtimes for every loaded
    /// replication group.
    ///
    /// # Errors
    ///
    /// Returns an error when a persisted backend or proxy cannot be restarted.
    pub fn restore_runtimes(&self) -> Result<(), ElastiCacheError> {
        let mut restored_groups = Vec::new();

        for state_key in self.state_store.keys() {
            let scope = ElastiCacheScope::new(
                state_key.account_id.clone(),
                state_key.region.clone(),
            );
            let state = self.load_state(&scope);
            match self.restore_scope_runtimes(&scope, &state) {
                Ok(groups) => restored_groups.extend(groups),
                Err(error) => {
                    self.stop_restored_runtimes(restored_groups);
                    return Err(error);
                }
            }
        }

        let mut runtime = self.lock_runtime();
        if !runtime.groups.is_empty() {
            drop(runtime);
            self.stop_restored_runtimes(restored_groups);
            return Err(ElastiCacheError::internal(
                "ElastiCache runtime restore requires an empty runtime state.",
            ));
        }
        runtime.groups.extend(restored_groups);

        Ok(())
    }

    /// Stops every active backend and proxy owned by this service.
    ///
    /// # Errors
    ///
    /// Returns an error when a backend or proxy cannot be stopped cleanly.
    pub fn shutdown(&self) -> Result<(), ElastiCacheError> {
        let groups = {
            let mut runtime = self.lock_runtime();
            std::mem::take(&mut runtime.groups)
        };

        for (_, runtime) in groups {
            runtime.stop()?;
        }

        Ok(())
    }

    /// # Errors
    ///
    /// Returns validation, conflict, runtime-start, or persistence errors when
    /// the requested replication group is invalid, already exists, or cannot be
    /// started and saved.
    pub fn create_replication_group(
        &self,
        scope: &ElastiCacheScope,
        input: CreateReplicationGroupInput,
    ) -> Result<ReplicationGroup, ElastiCacheError> {
        reject_unsupported_replication_group_inputs(&input)?;
        let replication_group_id = input.replication_group_id;
        let description = required_non_empty(
            "ReplicationGroupDescription",
            Some(&input.replication_group_description),
        )?
        .to_owned();

        let mut state = self.load_state(scope);
        if state.groups.contains_key(&replication_group_id) {
            return Err(ElastiCacheError::ReplicationGroupAlreadyExists {
                message: format!(
                    "Replication group {replication_group_id} already exists."
                ),
            });
        }

        let engine = input
            .engine
            .as_deref()
            .map(ElastiCacheEngine::parse)
            .transpose()?
            .unwrap_or(ElastiCacheEngine::Redis);
        let engine_version = input
            .engine_version
            .unwrap_or_else(|| engine.default_engine_version().to_owned());
        let auth_token = optional_non_empty(input.auth_token);
        let auth_mode = if auth_token.is_some() {
            ElastiCacheAuthenticationType::Password
        } else if input.transit_encryption_enabled.unwrap_or(false) {
            ElastiCacheAuthenticationType::Iam
        } else {
            ElastiCacheAuthenticationType::NoPassword
        };
        let transit_encryption_enabled =
            auth_mode != ElastiCacheAuthenticationType::NoPassword;

        let started = start_runtime(
            &self.node_runtime,
            &self.proxy_runtime,
            RuntimeStartInput::new(
                scope.clone(),
                replication_group_id.clone(),
                engine,
                auth_mode,
                None,
                Arc::new(self.clone()),
            ),
        )?;
        let group = StoredReplicationGroup {
            auth_mode,
            auth_token,
            description,
            endpoint: ElastiCacheEndpoint::loopback(started.proxy_port()),
            engine,
            engine_version,
            replication_group_id: replication_group_id.clone(),
            status: ReplicationGroupStatus::Available,
            transit_encryption_enabled,
            created_at_epoch_seconds: self.current_epoch_seconds(),
        };
        let output = group.to_output();
        state.groups.insert(replication_group_id.clone(), group);
        if let Err(error) = self.save_state(scope, state) {
            let _ = started.stop();
            return Err(error);
        }

        self.lock_runtime().groups.insert(
            ReplicationGroupRuntimeKey::new(scope, &replication_group_id),
            ReplicationGroupRuntime::new(started.backend, started.proxy),
        );

        Ok(output)
    }

    /// # Errors
    ///
    /// Returns [`ElastiCacheError::ReplicationGroupNotFound`] when a specific
    /// replication group was requested but does not exist.
    pub fn describe_replication_groups(
        &self,
        scope: &ElastiCacheScope,
        replication_group_id: Option<&ElastiCacheReplicationGroupId>,
    ) -> Result<Vec<ReplicationGroup>, ElastiCacheError> {
        let state = self.load_state(scope);

        if let Some(replication_group_id) = replication_group_id {
            let group = state.groups.get(replication_group_id).ok_or_else(
                || ElastiCacheError::ReplicationGroupNotFound {
                    message: format!(
                        "Replication group {replication_group_id} not found."
                    ),
                },
            )?;
            return Ok(vec![group.to_output()]);
        }

        Ok(state.groups.values().map(|group| group.to_output()).collect())
    }

    /// # Errors
    ///
    /// Returns not-found, persistence, or runtime-stop errors when the target
    /// replication group does not exist or its runtime cannot be torn down.
    pub fn delete_replication_group(
        &self,
        scope: &ElastiCacheScope,
        replication_group_id: &ElastiCacheReplicationGroupId,
    ) -> Result<ReplicationGroup, ElastiCacheError> {
        let replication_group_id = replication_group_id.clone();
        let mut state = self.load_state(scope);
        let mut group = state
            .groups
            .remove(&replication_group_id)
            .ok_or_else(|| ElastiCacheError::ReplicationGroupNotFound {
                message: format!(
                    "Replication group {replication_group_id} not found."
                ),
            })?;
        group.status = ReplicationGroupStatus::Deleting;
        self.save_state(scope, state)?;

        if let Some(runtime) = self.lock_runtime().groups.remove(
            &ReplicationGroupRuntimeKey::new(scope, &replication_group_id),
        ) {
            runtime.stop()?;
        }

        Ok(group.to_output())
    }

    /// # Errors
    ///
    /// Returns validation, duplicate-name, or persistence errors when the user
    /// request is invalid or conflicts with existing state.
    pub fn create_user(
        &self,
        scope: &ElastiCacheScope,
        input: CreateUserInput,
    ) -> Result<ElastiCacheUser, ElastiCacheError> {
        let user_id = input.user_id;
        let user_name =
            required_non_empty("UserName", Some(&input.user_name))?.to_owned();
        let engine = input
            .engine
            .as_deref()
            .map(ElastiCacheEngine::parse)
            .transpose()?
            .unwrap_or(ElastiCacheEngine::Redis);
        let access_string = normalized_access_string(input.access_string);
        let (auth_type, passwords) = resolve_user_authentication(
            input.authentication_mode.as_ref(),
            input.no_password_required,
            input.passwords,
        )?;
        let mut state = self.load_state(scope);
        if state.users.contains_key(&user_id) {
            return Err(ElastiCacheError::UserAlreadyExists {
                message: format!("User {user_id} already exists."),
            });
        }
        if state
            .users
            .values()
            .any(|candidate| candidate.user_name == user_name)
        {
            return Err(ElastiCacheError::DuplicateUserName {
                message: format!("User name {user_name} already exists."),
            });
        }

        let user = StoredElastiCacheUser {
            access_string,
            auth_type,
            engine,
            passwords,
            user_id: user_id.clone(),
            user_name,
        };
        let output = user.to_output(scope, REQUESTED_STATUS_ACTIVE);
        state.users.insert(user_id, user);
        self.save_state(scope, state)?;

        Ok(output)
    }

    /// # Errors
    ///
    /// Returns [`ElastiCacheError::UserNotFound`] when a specific user was
    /// requested but does not exist.
    pub fn describe_users(
        &self,
        scope: &ElastiCacheScope,
        user_id: Option<&ElastiCacheUserId>,
    ) -> Result<Vec<ElastiCacheUser>, ElastiCacheError> {
        let state = self.load_state(scope);

        if let Some(user_id) = user_id {
            let user = state.users.get(user_id).ok_or_else(|| {
                ElastiCacheError::UserNotFound {
                    message: format!("User {user_id} not found."),
                }
            })?;
            return Ok(vec![user.to_output(scope, REQUESTED_STATUS_ACTIVE)]);
        }

        Ok(state
            .users
            .values()
            .map(|user| user.to_output(scope, REQUESTED_STATUS_ACTIVE))
            .collect())
    }

    /// # Errors
    ///
    /// Returns not-found, validation, or persistence errors when the user does
    /// not exist or the update is invalid.
    pub fn modify_user(
        &self,
        scope: &ElastiCacheScope,
        user_id: &ElastiCacheUserId,
        input: ModifyUserInput,
    ) -> Result<ElastiCacheUser, ElastiCacheError> {
        let user_id = user_id.clone();
        let mut state = self.load_state(scope);
        let current = state.users.get(&user_id).cloned().ok_or_else(|| {
            ElastiCacheError::UserNotFound {
                message: format!("User {user_id} not found."),
            }
        })?;
        let mut updated = current;
        if let Some(access_string) =
            updated_access_string(input.access_string)?
        {
            updated.access_string = access_string;
        }
        let (auth_type, passwords) = resolve_user_authentication(
            input.authentication_mode.as_ref(),
            input.no_password_required,
            input.passwords,
        )?;
        updated.auth_type = auth_type;
        updated.passwords = passwords;
        let output = updated.to_output(scope, REQUESTED_STATUS_ACTIVE);
        state.users.insert(user_id, updated);
        self.save_state(scope, state)?;

        Ok(output)
    }

    /// # Errors
    ///
    /// Returns not-found or persistence errors when the user does not exist or
    /// the deletion cannot be saved.
    pub fn delete_user(
        &self,
        scope: &ElastiCacheScope,
        user_id: &ElastiCacheUserId,
    ) -> Result<ElastiCacheUser, ElastiCacheError> {
        let user_id = user_id.clone();
        let mut state = self.load_state(scope);
        let user = state.users.remove(&user_id).ok_or_else(|| {
            ElastiCacheError::UserNotFound {
                message: format!("User {user_id} not found."),
            }
        })?;
        self.save_state(scope, state)?;

        Ok(user.to_output(scope, REQUESTED_STATUS_DELETING))
    }

    fn current_epoch_seconds(&self) -> u64 {
        self.clock
            .now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
    }

    fn load_state(&self, scope: &ElastiCacheScope) -> StoredElastiCacheState {
        self.state_store
            .get(&ElastiCacheStateKey::new(scope))
            .unwrap_or_default()
    }

    fn save_state(
        &self,
        scope: &ElastiCacheScope,
        state: StoredElastiCacheState,
    ) -> Result<(), ElastiCacheError> {
        self.state_store.put(ElastiCacheStateKey::new(scope), state).map_err(
            |source| storage_error("writing ElastiCache state", source),
        )
    }

    fn lock_runtime(
        &self,
    ) -> std::sync::MutexGuard<'_, ElastiCacheRuntimeState> {
        recover(self.runtime_state.lock())
    }

    fn restore_scope_runtimes(
        &self,
        scope: &ElastiCacheScope,
        state: &StoredElastiCacheState,
    ) -> Result<
        Vec<(ReplicationGroupRuntimeKey, ReplicationGroupRuntime)>,
        ElastiCacheError,
    > {
        let mut restored_groups = Vec::new();

        for group in state.groups.values().cloned() {
            let started = match start_runtime(
                &self.node_runtime,
                &self.proxy_runtime,
                RuntimeStartInput::new(
                    scope.clone(),
                    group.replication_group_id.clone(),
                    group.engine,
                    group.auth_mode,
                    Some(group.endpoint.port),
                    Arc::new(self.clone()),
                ),
            ) {
                Ok(started) => started,
                Err(error) => {
                    self.stop_restored_runtimes(restored_groups);
                    return Err(error);
                }
            };
            restored_groups.push((
                ReplicationGroupRuntimeKey::new(
                    scope,
                    &group.replication_group_id,
                ),
                ReplicationGroupRuntime::new(started.backend, started.proxy),
            ));
        }

        Ok(restored_groups)
    }

    fn stop_restored_runtimes(
        &self,
        restored_groups: Vec<(
            ReplicationGroupRuntimeKey,
            ReplicationGroupRuntime,
        )>,
    ) {
        for (_, runtime) in restored_groups {
            let _ = runtime.stop();
        }
    }
}

impl ElastiCacheConnectionAuthenticator for ElastiCacheService {
    fn validate_iam_token(
        &self,
        _scope: &ElastiCacheScope,
        replication_group_id: &ElastiCacheReplicationGroupId,
        token: &str,
    ) -> Result<(), String> {
        self.iam_token_validator.validate(replication_group_id, token)
    }

    fn validate_password(
        &self,
        scope: &ElastiCacheScope,
        replication_group_id: &ElastiCacheReplicationGroupId,
        username: Option<&str>,
        password: &str,
    ) -> Result<(), String> {
        let state = self.load_state(scope);
        let group =
            state.groups.get(replication_group_id).ok_or_else(|| {
                format!(
                    "Replication group {replication_group_id} is not available."
                )
            })?;

        if username.is_none() && group.auth_token.as_deref() == Some(password)
        {
            return Ok(());
        }

        if state.users.values().any(|user| {
            user.auth_type == ElastiCacheAuthenticationType::Password
                && user.matches_password(username, password)
        }) {
            Ok(())
        } else {
            Err("invalid username-password pair or user is disabled."
                .to_owned())
        }
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
struct ElastiCacheStateKey {
    account_id: AccountId,
    region: RegionId,
}

impl ElastiCacheStateKey {
    fn new(scope: &ElastiCacheScope) -> Self {
        Self {
            account_id: scope.account_id().clone(),
            region: scope.region().clone(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct StoredElastiCacheState {
    groups: BTreeMap<ElastiCacheReplicationGroupId, StoredReplicationGroup>,
    users: BTreeMap<ElastiCacheUserId, StoredElastiCacheUser>,
}

fn required_non_empty<'a>(
    name: &str,
    value: Option<&'a str>,
) -> Result<&'a str, ElastiCacheError> {
    let value =
        value.ok_or_else(|| ElastiCacheError::InvalidParameterValue {
            message: format!("{name} is required."),
        })?;
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(ElastiCacheError::InvalidParameterValue {
            message: format!("{name} must not be blank."),
        });
    }

    Ok(trimmed)
}

fn optional_non_empty(value: Option<String>) -> Option<String> {
    value.and_then(|value| {
        let trimmed = value.trim();
        (!trimmed.is_empty()).then(|| trimmed.to_owned())
    })
}

#[cfg(test)]
mod tests {
    use super::{
        AuthenticationModeInput, CreateReplicationGroupInput, CreateUserInput,
        ElastiCacheAuthenticationType, ElastiCacheConnectionAuthenticator,
        ElastiCacheEngine, ElastiCacheError, ElastiCacheIamTokenValidator,
        ElastiCacheNodeRuntime, ElastiCacheNodeSpec, ElastiCacheProxyRuntime,
        ElastiCacheProxySpec, ElastiCacheReplicationGroupId, ElastiCacheScope,
        ElastiCacheService, ElastiCacheServiceDependencies, ElastiCacheUserId,
        ModifyUserInput, RejectAllElastiCacheIamTokenValidator,
        ReplicationGroupStatus, RunningElastiCacheNode,
        RunningElastiCacheProxy, StoredElastiCacheUser, infrastructure_error,
        optional_non_empty, required_non_empty, resolve_user_authentication,
        storage_error,
    };
    use aws::{Clock, Endpoint, InfrastructureError};
    use std::io;
    use std::sync::atomic::{AtomicU16, Ordering};
    use std::sync::{Arc, LockResult, Mutex};
    use std::time::{Duration, SystemTime, UNIX_EPOCH};
    use storage::{StorageConfig, StorageFactory, StorageMode};

    #[derive(Debug, Clone)]
    struct StaticClock {
        now: SystemTime,
    }

    impl Clock for StaticClock {
        fn now(&self) -> SystemTime {
            self.now
        }
    }

    #[derive(Default)]
    struct FakeNodeRuntime {
        next_port: AtomicU16,
        started: Mutex<Vec<(ElastiCacheReplicationGroupId, u16)>>,
        stopped: Mutex<Vec<u16>>,
        fail_start: Mutex<bool>,
    }

    impl FakeNodeRuntime {
        fn with_port(start: u16) -> Self {
            Self {
                next_port: AtomicU16::new(start),
                started: Mutex::default(),
                stopped: Mutex::default(),
                fail_start: Mutex::default(),
            }
        }
    }

    impl ElastiCacheNodeRuntime for FakeNodeRuntime {
        fn start(
            &self,
            spec: &ElastiCacheNodeSpec,
        ) -> Result<Box<dyn RunningElastiCacheNode>, InfrastructureError>
        {
            if *recover(self.fail_start.lock()) {
                return Err(InfrastructureError::tcp_proxy(
                    "start-node",
                    spec.listen_endpoint(),
                    io::Error::other("boom"),
                ));
            }
            let port = self.next_port.fetch_add(1, Ordering::Relaxed);
            recover(self.started.lock())
                .push((spec.group_id().to_owned(), port));
            Ok(Box::new(FakeRunningNode {
                endpoint: Endpoint::localhost(port),
                stopped: Arc::new(Mutex::new(self.stopped.lock().is_err())),
                stops: Arc::new(Mutex::new(Vec::new())),
            }))
        }
    }

    struct FakeRunningNode {
        endpoint: Endpoint,
        stopped: Arc<Mutex<bool>>,
        stops: Arc<Mutex<Vec<u16>>>,
    }

    impl RunningElastiCacheNode for FakeRunningNode {
        fn listen_endpoint(&self) -> Endpoint {
            self.endpoint.clone()
        }

        fn stop(&self) -> Result<(), InfrastructureError> {
            *recover(self.stopped.lock()) = true;
            recover(self.stops.lock()).push(self.endpoint.port());
            Ok(())
        }
    }

    #[derive(Default)]
    struct FakeProxyRuntime {
        next_port: AtomicU16,
        started: Mutex<
            Vec<(
                ElastiCacheReplicationGroupId,
                u16,
                u16,
                ElastiCacheAuthenticationType,
            )>,
        >,
        fail_start: Mutex<bool>,
    }

    impl FakeProxyRuntime {
        fn with_port(start: u16) -> Self {
            Self {
                next_port: AtomicU16::new(start),
                started: Mutex::default(),
                fail_start: Mutex::default(),
            }
        }
    }

    impl ElastiCacheProxyRuntime for FakeProxyRuntime {
        fn start(
            &self,
            spec: &ElastiCacheProxySpec,
        ) -> Result<Box<dyn RunningElastiCacheProxy>, InfrastructureError>
        {
            if *recover(self.fail_start.lock()) {
                return Err(InfrastructureError::tcp_proxy(
                    "start-proxy",
                    spec.listen_endpoint(),
                    io::Error::other("boom"),
                ));
            }
            let port = if spec.listen_endpoint().port() == 0 {
                self.next_port.fetch_add(1, Ordering::Relaxed)
            } else {
                spec.listen_endpoint().port()
            };
            recover(self.started.lock()).push((
                spec.replication_group_id().to_owned(),
                port,
                spec.upstream().port(),
                spec.auth_mode(),
            ));
            Ok(Box::new(FakeRunningProxy {
                endpoint: Endpoint::localhost(port),
                stopped: Arc::new(Mutex::new(false)),
                stops: Arc::new(Mutex::new(Vec::new())),
            }))
        }
    }

    struct FakeRunningProxy {
        endpoint: Endpoint,
        stopped: Arc<Mutex<bool>>,
        stops: Arc<Mutex<Vec<u16>>>,
    }

    impl RunningElastiCacheProxy for FakeRunningProxy {
        fn listen_endpoint(&self) -> Endpoint {
            self.endpoint.clone()
        }

        fn stop(&self) -> Result<(), InfrastructureError> {
            *recover(self.stopped.lock()) = true;
            recover(self.stops.lock()).push(self.endpoint.port());
            Ok(())
        }
    }

    #[derive(Debug, Default)]
    struct RecordingIamTokenValidator {
        accepted: Mutex<Vec<String>>,
        rejected: Mutex<Vec<String>>,
    }

    impl ElastiCacheIamTokenValidator for RecordingIamTokenValidator {
        fn validate(
            &self,
            replication_group_id: &ElastiCacheReplicationGroupId,
            token: &str,
        ) -> Result<(), String> {
            let key = format!("{replication_group_id}:{token}");
            if recover(self.accepted.lock()).contains(&key) {
                Ok(())
            } else {
                recover(self.rejected.lock()).push(key);
                Err("bad token".to_owned())
            }
        }
    }

    #[derive(Debug, Default)]
    struct AllowAllAuthenticator;

    impl ElastiCacheConnectionAuthenticator for AllowAllAuthenticator {
        fn validate_iam_token(
            &self,
            _scope: &ElastiCacheScope,
            _replication_group_id: &ElastiCacheReplicationGroupId,
            _token: &str,
        ) -> Result<(), String> {
            Ok(())
        }

        fn validate_password(
            &self,
            _scope: &ElastiCacheScope,
            _replication_group_id: &ElastiCacheReplicationGroupId,
            _username: Option<&str>,
            _password: &str,
        ) -> Result<(), String> {
            Ok(())
        }
    }

    struct FailingStopNodeRuntime;

    impl ElastiCacheNodeRuntime for FailingStopNodeRuntime {
        fn start(
            &self,
            _spec: &ElastiCacheNodeSpec,
        ) -> Result<Box<dyn RunningElastiCacheNode>, InfrastructureError>
        {
            Ok(Box::new(FailingStopNode {
                endpoint: Endpoint::localhost(7101),
            }))
        }
    }

    struct FailingStopNode {
        endpoint: Endpoint,
    }

    impl RunningElastiCacheNode for FailingStopNode {
        fn listen_endpoint(&self) -> Endpoint {
            self.endpoint.clone()
        }

        fn stop(&self) -> Result<(), InfrastructureError> {
            Err(InfrastructureError::tcp_proxy(
                "stop-node",
                &self.endpoint,
                io::Error::other("stop failed"),
            ))
        }
    }

    struct FailingStopProxyRuntime;

    impl ElastiCacheProxyRuntime for FailingStopProxyRuntime {
        fn start(
            &self,
            _spec: &ElastiCacheProxySpec,
        ) -> Result<Box<dyn RunningElastiCacheProxy>, InfrastructureError>
        {
            Ok(Box::new(FailingStopProxy {
                endpoint: Endpoint::localhost(8101),
            }))
        }
    }

    struct FailingStopProxy {
        endpoint: Endpoint,
    }

    impl RunningElastiCacheProxy for FailingStopProxy {
        fn listen_endpoint(&self) -> Endpoint {
            self.endpoint.clone()
        }

        fn stop(&self) -> Result<(), InfrastructureError> {
            Err(InfrastructureError::tcp_proxy(
                "stop-proxy",
                &self.endpoint,
                io::Error::other("stop failed"),
            ))
        }
    }

    fn factory(label: &str) -> StorageFactory {
        let path = std::env::temp_dir()
            .join(format!("cloudish-elasticache-tests-{label}"));
        if path.exists() {
            let _ = std::fs::remove_dir_all(&path);
        }
        std::fs::create_dir_all(&path)
            .expect("test state directory should exist");

        StorageFactory::new(StorageConfig::new(path, StorageMode::Memory))
    }

    fn scope() -> ElastiCacheScope {
        ElastiCacheScope::new(
            "000000000000".parse().expect("account id should parse"),
            "eu-west-2".parse().expect("region should parse"),
        )
    }

    fn replication_group_id(value: &str) -> ElastiCacheReplicationGroupId {
        ElastiCacheReplicationGroupId::new(value)
            .expect("replication group identifier should be valid")
    }

    fn user_id(value: &str) -> ElastiCacheUserId {
        ElastiCacheUserId::new(value).expect("user identifier should be valid")
    }

    fn service(
        label: &str,
        node_runtime: Arc<dyn ElastiCacheNodeRuntime + Send + Sync>,
        proxy_runtime: Arc<dyn ElastiCacheProxyRuntime + Send + Sync>,
        iam_token_validator: Arc<
            dyn ElastiCacheIamTokenValidator + Send + Sync,
        >,
    ) -> ElastiCacheService {
        ElastiCacheService::new(
            &factory(label),
            ElastiCacheServiceDependencies {
                clock: Arc::new(StaticClock {
                    now: UNIX_EPOCH + Duration::from_secs(42),
                }),
                iam_token_validator,
                node_runtime,
                proxy_runtime,
            },
        )
    }

    fn persistent_service(
        directory: std::path::PathBuf,
        node_runtime: Arc<dyn ElastiCacheNodeRuntime + Send + Sync>,
        proxy_runtime: Arc<dyn ElastiCacheProxyRuntime + Send + Sync>,
    ) -> ElastiCacheService {
        std::fs::create_dir_all(&directory)
            .expect("persistent ElastiCache state directory should exist");

        ElastiCacheService::new(
            &StorageFactory::new(StorageConfig::new(
                directory,
                StorageMode::Persistent,
            )),
            ElastiCacheServiceDependencies {
                clock: Arc::new(StaticClock {
                    now: UNIX_EPOCH + Duration::from_secs(42),
                }),
                iam_token_validator: Arc::new(
                    RejectAllElastiCacheIamTokenValidator,
                ),
                node_runtime,
                proxy_runtime,
            },
        )
    }

    fn blocked_persistent_service(
        label: &str,
        node_runtime: Arc<dyn ElastiCacheNodeRuntime + Send + Sync>,
        proxy_runtime: Arc<dyn ElastiCacheProxyRuntime + Send + Sync>,
    ) -> ElastiCacheService {
        let path = std::env::temp_dir()
            .join(format!("cloudish-elasticache-persistent-tests-{label}"));
        if path.exists() {
            let _ = std::fs::remove_dir_all(&path);
        }
        std::fs::create_dir_all(path.join("elasticache"))
            .expect("persistent test state directory should exist");
        std::fs::create_dir_all(
            path.join("elasticache").join("control-plane.json.tmp"),
        )
        .expect("snapshot temp blocker should exist");

        ElastiCacheService::new(
            &StorageFactory::new(StorageConfig::new(
                path,
                StorageMode::Persistent,
            )),
            ElastiCacheServiceDependencies {
                clock: Arc::new(StaticClock {
                    now: UNIX_EPOCH + Duration::from_secs(7),
                }),
                iam_token_validator: Arc::new(
                    RejectAllElastiCacheIamTokenValidator,
                ),
                node_runtime,
                proxy_runtime,
            },
        )
    }

    #[test]
    fn elasticache_engine_and_auth_strings_cover_supported_variants() {
        assert_eq!(
            ElastiCacheEngine::parse(" valkey ")
                .expect("valkey should parse")
                .to_string(),
            "valkey"
        );
        let engine_error = ElastiCacheEngine::parse("memcached")
            .expect_err("unsupported engine should fail");
        assert_eq!(engine_error.code(), "InvalidParameterValue");

        assert_eq!(
            ElastiCacheAuthenticationType::Password.as_input_str(),
            "password"
        );
        assert_eq!(
            ElastiCacheAuthenticationType::NoPassword.as_input_str(),
            "no-password-required"
        );
        assert_eq!(ElastiCacheAuthenticationType::Iam.as_input_str(), "iam");
        assert_eq!(
            ElastiCacheAuthenticationType::NoPassword.as_output_str(),
            "no-password"
        );
        assert_eq!(
            ElastiCacheAuthenticationType::Password.as_output_str(),
            "password"
        );
        assert_eq!(ElastiCacheAuthenticationType::Iam.as_output_str(), "iam");
        assert_eq!(ReplicationGroupStatus::Available.as_str(), "available");
        assert_eq!(ReplicationGroupStatus::Deleting.as_str(), "deleting");
    }

    #[test]
    fn elasticache_specs_expose_expected_runtime_fields() {
        let elasticache_scope = scope();
        let node_spec = ElastiCacheNodeSpec::new(
            replication_group_id("cache-z"),
            ElastiCacheEngine::Valkey,
            Endpoint::localhost(4567),
        );
        assert_eq!(node_spec.engine(), ElastiCacheEngine::Valkey);
        assert_eq!(node_spec.group_id().as_str(), "cache-z");
        assert_eq!(node_spec.listen_endpoint().port(), 4567);

        let proxy_spec = ElastiCacheProxySpec::new(
            elasticache_scope.clone(),
            replication_group_id("cache-z"),
            ElastiCacheAuthenticationType::NoPassword,
            Endpoint::localhost(4568),
            Endpoint::localhost(4569),
            Arc::new(AllowAllAuthenticator),
        );
        assert_eq!(
            proxy_spec.auth_mode(),
            ElastiCacheAuthenticationType::NoPassword
        );
        assert_eq!(proxy_spec.listen_endpoint().port(), 4568);
        assert_eq!(proxy_spec.replication_group_id().as_str(), "cache-z");
        assert_eq!(proxy_spec.scope(), &elasticache_scope);
        assert_eq!(proxy_spec.upstream().port(), 4569);
        proxy_spec
            .authenticator()
            .validate_password(
                &elasticache_scope,
                &replication_group_id("cache-z"),
                None,
                "ignored",
            )
            .expect("test authenticator should accept passwords");
        proxy_spec
            .authenticator()
            .validate_iam_token(
                &elasticache_scope,
                &replication_group_id("cache-z"),
                "ignored",
            )
            .expect("test authenticator should accept IAM tokens");
    }

    #[test]
    fn elasticache_error_and_identifier_helpers_remain_stable() {
        assert_eq!(
            ElastiCacheError::DuplicateUserName { message: "dup".to_owned() }
                .message(),
            "dup"
        );
        assert_eq!(
            ElastiCacheError::ReplicationGroupAlreadyExists {
                message: "group".to_owned(),
            }
            .message(),
            "group"
        );
        assert_eq!(
            ElastiCacheError::ReplicationGroupNotFound {
                message: "missing".to_owned(),
            }
            .message(),
            "missing"
        );
        assert_eq!(
            ElastiCacheError::UserAlreadyExists { message: "user".to_owned() }
                .message(),
            "user"
        );
        assert_eq!(
            required_non_empty("Name", Some("  value  "))
                .expect("trimmed value should be accepted"),
            "value"
        );
        assert_eq!(
            required_non_empty("Name", None)
                .expect_err("missing values should fail")
                .message(),
            "Name is required."
        );
        assert_eq!(
            required_non_empty("Name", Some("   "))
                .expect_err("blank values should fail")
                .message(),
            "Name must not be blank."
        );
        assert_eq!(
            optional_non_empty(Some("  kept  ".to_owned())),
            Some("kept".to_owned())
        );
        assert_eq!(optional_non_empty(Some("   ".to_owned())), None);
        assert_eq!(replication_group_id("Cache-A"), "cache-a");
        assert_eq!(
            ElastiCacheReplicationGroupId::new("1bad")
                .expect_err("invalid identifiers should fail")
                .code(),
            "InvalidParameterValue"
        );
    }

    #[test]
    fn elasticache_storage_and_auth_helper_paths_are_stable() {
        let validation_error = storage_error(
            "writing state",
            storage::StorageError::WriteSnapshot {
                path: std::path::PathBuf::from("/tmp/example"),
                source: io::Error::other("boom"),
            },
        );
        assert_eq!(validation_error.code(), "InternalFailure");
        assert_eq!(validation_error.status_code(), 500);
        assert_eq!(validation_error.to_aws_error().status_code(), 500);
        let infra_error = infrastructure_error(
            "starting backend",
            InfrastructureError::tcp_proxy(
                "bind",
                &Endpoint::localhost(9000),
                io::Error::other("boom"),
            ),
        );
        assert_eq!(infra_error.code(), "InternalFailure");
        let stored_user = StoredElastiCacheUser {
            access_string: "on ~* +@all".to_owned(),
            auth_type: ElastiCacheAuthenticationType::Password,
            engine: ElastiCacheEngine::Redis,
            passwords: vec!["secret".to_owned()],
            user_id: user_id("user-z"),
            user_name: "alice".to_owned(),
        };
        assert!(stored_user.matches_password(None, "secret"));

        let elasticache_scope = scope();
        let service = service(
            "debug-helper",
            Arc::new(FakeNodeRuntime::with_port(7001)),
            Arc::new(FakeProxyRuntime::with_port(8001)),
            Arc::new(RejectAllElastiCacheIamTokenValidator),
        );
        assert_eq!(format!("{service:?}"), "ElastiCacheService");
        assert_eq!(
            service
                .validate_password(
                    &elasticache_scope,
                    &replication_group_id("missing-group"),
                    None,
                    "secret",
                )
                .expect_err("missing groups should fail"),
            "Replication group missing-group is not available."
        );
        assert_eq!(
            RejectAllElastiCacheIamTokenValidator
                .validate(&replication_group_id("cache-a"), "token")
                .expect_err("default validator should reject"),
            "IAM authentication is not configured."
        );
    }

    #[test]
    fn elasticache_replication_group_lists_and_runtime_failures_are_explicit()
    {
        let list_service = service(
            "groups-listing",
            Arc::new(FakeNodeRuntime::with_port(7100)),
            Arc::new(FakeProxyRuntime::with_port(8100)),
            Arc::new(RejectAllElastiCacheIamTokenValidator),
        );
        let created = list_service
            .create_replication_group(
                &scope(),
                CreateReplicationGroupInput {
                    auth_token: Some("   ".to_owned()),
                    cluster_mode: None,
                    engine: Some("valkey".to_owned()),
                    engine_version: None,
                    multi_az_enabled: None,
                    num_cache_clusters: None,
                    num_node_groups: None,
                    replicas_per_node_group: None,
                    replication_group_description: " demo ".to_owned(),
                    replication_group_id: replication_group_id("Cache-B"),
                    transit_encryption_enabled: None,
                    has_node_group_configuration: false,
                },
            )
            .expect("replication group should create");
        assert_eq!(created.engine, ElastiCacheEngine::Valkey);
        assert_eq!(created.engine_version, "8.0");
        assert!(!created.auth_token_enabled);
        assert!(!created.transit_encryption_enabled);

        let listed = list_service
            .describe_replication_groups(&scope(), None)
            .expect("list should succeed");
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].replication_group_id, "cache-b");

        let duplicate_error = list_service
            .create_replication_group(
                &scope(),
                CreateReplicationGroupInput {
                    auth_token: None,
                    cluster_mode: None,
                    engine: Some("redis".to_owned()),
                    engine_version: None,
                    multi_az_enabled: None,
                    num_cache_clusters: None,
                    num_node_groups: None,
                    replicas_per_node_group: None,
                    replication_group_description: "demo".to_owned(),
                    replication_group_id: replication_group_id("cache-b"),
                    transit_encryption_enabled: None,
                    has_node_group_configuration: false,
                },
            )
            .expect_err("duplicate groups should fail");
        assert_eq!(duplicate_error.code(), "ReplicationGroupAlreadyExists");

        let not_found = list_service
            .describe_replication_groups(
                &scope(),
                Some(&replication_group_id("missing")),
            )
            .expect_err("missing groups should fail");
        assert_eq!(not_found.code(), "ReplicationGroupNotFoundFault");
        assert_eq!(not_found.status_code(), 404);

        let persistent_directory = std::env::temp_dir().join(format!(
            "cloudish-elasticache-restore-{}",
            std::process::id()
        ));
        if persistent_directory.exists() {
            let _ = std::fs::remove_dir_all(&persistent_directory);
        }
        let created_service = persistent_service(
            persistent_directory.clone(),
            Arc::new(FakeNodeRuntime::with_port(7_100)),
            Arc::new(FakeProxyRuntime::with_port(8_100)),
        );
        let created_group = created_service
            .create_replication_group(
                &scope(),
                CreateReplicationGroupInput {
                    auth_token: Some("secret".to_owned()),
                    cluster_mode: None,
                    engine: Some("redis".to_owned()),
                    engine_version: None,
                    multi_az_enabled: None,
                    num_cache_clusters: None,
                    num_node_groups: None,
                    replicas_per_node_group: None,
                    replication_group_description: "demo".to_owned(),
                    replication_group_id: replication_group_id(
                        "cache-restore",
                    ),
                    transit_encryption_enabled: None,
                    has_node_group_configuration: false,
                },
            )
            .expect("replication group should create");
        let restored_node_runtime =
            Arc::new(FakeNodeRuntime::with_port(7_200));
        let restored_proxy_runtime =
            Arc::new(FakeProxyRuntime::with_port(8_200));
        let restored_service = persistent_service(
            persistent_directory,
            restored_node_runtime.clone(),
            restored_proxy_runtime.clone(),
        );
        restored_service
            .state_store
            .load()
            .expect("ElastiCache state should load");
        restored_service
            .restore_runtimes()
            .expect("ElastiCache runtimes should restore");
        assert_eq!(
            restored_service
                .describe_replication_groups(
                    &scope(),
                    Some(&replication_group_id("cache-restore")),
                )
                .expect("replication group should describe")[0]
                .configuration_endpoint
                .port,
            created_group.configuration_endpoint.port
        );
        assert_eq!(
            recover(restored_node_runtime.started.lock()).clone(),
            vec![(replication_group_id("cache-restore"), 7_200)]
        );
        assert_eq!(
            recover(restored_proxy_runtime.started.lock()).clone(),
            vec![(
                replication_group_id("cache-restore"),
                created_group.configuration_endpoint.port,
                7_200,
                ElastiCacheAuthenticationType::Password,
            )]
        );

        let delete_missing = list_service
            .delete_replication_group(
                &scope(),
                &replication_group_id("missing"),
            )
            .expect_err("missing deletes should fail");
        assert_eq!(delete_missing.code(), "ReplicationGroupNotFoundFault");

        let node_fail_runtime = Arc::new(FakeNodeRuntime::with_port(7200));
        *recover(node_fail_runtime.fail_start.lock()) = true;
        let node_fail_service = service(
            "groups-node-fail",
            node_fail_runtime,
            Arc::new(FakeProxyRuntime::with_port(8200)),
            Arc::new(RejectAllElastiCacheIamTokenValidator),
        );
        let node_fail = node_fail_service
            .create_replication_group(
                &scope(),
                CreateReplicationGroupInput {
                    auth_token: None,
                    cluster_mode: None,
                    engine: Some("redis".to_owned()),
                    engine_version: None,
                    multi_az_enabled: None,
                    num_cache_clusters: None,
                    num_node_groups: None,
                    replicas_per_node_group: None,
                    replication_group_description: "demo".to_owned(),
                    replication_group_id: replication_group_id("cache-c"),
                    transit_encryption_enabled: None,
                    has_node_group_configuration: false,
                },
            )
            .expect_err("node start failures should surface");
        assert_eq!(node_fail.code(), "InternalFailure");
        assert!(node_fail.message().contains("starting ElastiCache backend"));

        let proxy_fail_runtime = Arc::new(FakeProxyRuntime::with_port(8300));
        *recover(proxy_fail_runtime.fail_start.lock()) = true;
        let proxy_fail_service = service(
            "groups-proxy-fail",
            Arc::new(FakeNodeRuntime::with_port(7300)),
            proxy_fail_runtime,
            Arc::new(RejectAllElastiCacheIamTokenValidator),
        );
        let proxy_fail = proxy_fail_service
            .create_replication_group(
                &scope(),
                CreateReplicationGroupInput {
                    auth_token: None,
                    cluster_mode: None,
                    engine: Some("redis".to_owned()),
                    engine_version: None,
                    multi_az_enabled: None,
                    num_cache_clusters: None,
                    num_node_groups: None,
                    replicas_per_node_group: None,
                    replication_group_description: "demo".to_owned(),
                    replication_group_id: replication_group_id("cache-d"),
                    transit_encryption_enabled: None,
                    has_node_group_configuration: false,
                },
            )
            .expect_err("proxy start failures should surface");
        assert_eq!(proxy_fail.code(), "InternalFailure");
        assert!(proxy_fail.message().contains("starting ElastiCache proxy"));

        let proxy_stop_service = service(
            "groups-proxy-stop-fail",
            Arc::new(FakeNodeRuntime::with_port(7400)),
            Arc::new(FailingStopProxyRuntime),
            Arc::new(RejectAllElastiCacheIamTokenValidator),
        );
        proxy_stop_service
            .create_replication_group(
                &scope(),
                CreateReplicationGroupInput {
                    auth_token: None,
                    cluster_mode: None,
                    engine: Some("redis".to_owned()),
                    engine_version: None,
                    multi_az_enabled: None,
                    num_cache_clusters: None,
                    num_node_groups: None,
                    replicas_per_node_group: None,
                    replication_group_description: "demo".to_owned(),
                    replication_group_id: replication_group_id("cache-e"),
                    transit_encryption_enabled: None,
                    has_node_group_configuration: false,
                },
            )
            .expect("replication group should create");
        let proxy_stop_error = proxy_stop_service
            .delete_replication_group(
                &scope(),
                &replication_group_id("cache-e"),
            )
            .expect_err("proxy stop failures should surface");
        assert_eq!(proxy_stop_error.code(), "InternalFailure");
        assert!(
            proxy_stop_error.message().contains("stopping ElastiCache proxy")
        );

        let backend_stop_service = service(
            "groups-backend-stop-fail",
            Arc::new(FailingStopNodeRuntime),
            Arc::new(FakeProxyRuntime::with_port(8400)),
            Arc::new(RejectAllElastiCacheIamTokenValidator),
        );
        backend_stop_service
            .create_replication_group(
                &scope(),
                CreateReplicationGroupInput {
                    auth_token: None,
                    cluster_mode: None,
                    engine: Some("redis".to_owned()),
                    engine_version: None,
                    multi_az_enabled: None,
                    num_cache_clusters: None,
                    num_node_groups: None,
                    replicas_per_node_group: None,
                    replication_group_description: "demo".to_owned(),
                    replication_group_id: replication_group_id("cache-f"),
                    transit_encryption_enabled: None,
                    has_node_group_configuration: false,
                },
            )
            .expect("replication group should create");
        let backend_stop_error = backend_stop_service
            .delete_replication_group(
                &scope(),
                &replication_group_id("cache-f"),
            )
            .expect_err("backend stop failures should surface");
        assert_eq!(backend_stop_error.code(), "InternalFailure");
        assert!(
            backend_stop_error
                .message()
                .contains("stopping ElastiCache backend")
        );
    }

    #[test]
    fn elasticache_user_lists_and_error_paths_are_explicit() {
        let service = service(
            "users-errors",
            Arc::new(FakeNodeRuntime::with_port(7500)),
            Arc::new(FakeProxyRuntime::with_port(8500)),
            Arc::new(RejectAllElastiCacheIamTokenValidator),
        );

        let first = service
            .create_user(
                &scope(),
                CreateUserInput {
                    access_string: Some("   ".to_owned()),
                    authentication_mode: None,
                    engine: Some("redis".to_owned()),
                    no_password_required: None,
                    passwords: vec!["secret-1".to_owned()],
                    user_id: user_id("user-b"),
                    user_name: "bob".to_owned(),
                },
            )
            .expect("user should create");
        assert_eq!(first.access_string, "on ~* +@all");
        assert_eq!(
            first.authentication.type_,
            ElastiCacheAuthenticationType::Password
        );

        let listed = service
            .describe_users(&scope(), None)
            .expect("user listing should succeed");
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].user_name, "bob");

        let duplicate_id = service
            .create_user(
                &scope(),
                CreateUserInput {
                    access_string: None,
                    authentication_mode: None,
                    engine: Some("redis".to_owned()),
                    no_password_required: Some(true),
                    passwords: Vec::new(),
                    user_id: user_id("user-b"),
                    user_name: "carol".to_owned(),
                },
            )
            .expect_err("duplicate ids should fail");
        assert_eq!(duplicate_id.code(), "UserAlreadyExists");

        let duplicate_name = service
            .create_user(
                &scope(),
                CreateUserInput {
                    access_string: None,
                    authentication_mode: None,
                    engine: Some("redis".to_owned()),
                    no_password_required: Some(true),
                    passwords: Vec::new(),
                    user_id: user_id("user-c"),
                    user_name: "bob".to_owned(),
                },
            )
            .expect_err("duplicate names should fail");
        assert_eq!(duplicate_name.code(), "DuplicateUserName");

        let invalid_engine = service
            .create_user(
                &scope(),
                CreateUserInput {
                    access_string: None,
                    authentication_mode: None,
                    engine: Some("memcached".to_owned()),
                    no_password_required: Some(true),
                    passwords: Vec::new(),
                    user_id: user_id("user-d"),
                    user_name: "dave".to_owned(),
                },
            )
            .expect_err("unsupported engines should fail");
        assert_eq!(invalid_engine.code(), "InvalidParameterValue");

        let modify_missing = service
            .modify_user(
                &scope(),
                &user_id("missing"),
                ModifyUserInput {
                    access_string: None,
                    authentication_mode: None,
                    no_password_required: Some(true),
                    passwords: Vec::new(),
                },
            )
            .expect_err("missing users should fail");
        assert_eq!(modify_missing.code(), "UserNotFound");

        let blank_access = service
            .modify_user(
                &scope(),
                &user_id("user-b"),
                ModifyUserInput {
                    access_string: Some("   ".to_owned()),
                    authentication_mode: None,
                    no_password_required: Some(true),
                    passwords: Vec::new(),
                },
            )
            .expect_err("blank access strings should fail");
        assert_eq!(blank_access.code(), "InvalidParameterValue");

        let describe_missing = service
            .describe_users(&scope(), Some(&user_id("missing")))
            .expect_err("missing user descriptions should fail");
        assert_eq!(describe_missing.code(), "UserNotFound");
        assert_eq!(describe_missing.status_code(), 404);

        let deleted = service
            .delete_user(&scope(), &user_id("user-b"))
            .expect("user delete should succeed");
        assert_eq!(deleted.status, "deleting");
        let delete_missing = service
            .delete_user(&scope(), &user_id("user-b"))
            .expect_err("deleted users should stay missing");
        assert_eq!(delete_missing.code(), "UserNotFound");
    }

    #[test]
    fn elasticache_storage_failures_and_auth_resolution_cover_remaining_paths()
    {
        let save_fail_service = blocked_persistent_service(
            "save-failure",
            Arc::new(FakeNodeRuntime::with_port(7600)),
            Arc::new(FakeProxyRuntime::with_port(8600)),
        );
        let save_error = save_fail_service
            .create_replication_group(
                &scope(),
                CreateReplicationGroupInput {
                    auth_token: Some("secret".to_owned()),
                    cluster_mode: None,
                    engine: Some("redis".to_owned()),
                    engine_version: None,
                    multi_az_enabled: None,
                    num_cache_clusters: None,
                    num_node_groups: None,
                    replicas_per_node_group: None,
                    replication_group_description: "demo".to_owned(),
                    replication_group_id: replication_group_id("cache-save"),
                    transit_encryption_enabled: Some(true),
                    has_node_group_configuration: false,
                },
            )
            .expect_err("blocked snapshot writes should fail");
        assert_eq!(save_error.code(), "InternalFailure");
        assert!(save_error.message().contains("writing ElastiCache state"));

        let save_fail_proxy_stop_service = blocked_persistent_service(
            "save-failure-proxy-stop",
            Arc::new(FakeNodeRuntime::with_port(7601)),
            Arc::new(FailingStopProxyRuntime),
        );
        let save_fail_proxy_stop_error = save_fail_proxy_stop_service
            .create_replication_group(
                &scope(),
                CreateReplicationGroupInput {
                    auth_token: Some("secret".to_owned()),
                    cluster_mode: None,
                    engine: Some("redis".to_owned()),
                    engine_version: None,
                    multi_az_enabled: None,
                    num_cache_clusters: None,
                    num_node_groups: None,
                    replicas_per_node_group: None,
                    replication_group_description: "demo".to_owned(),
                    replication_group_id: replication_group_id(
                        "cache-save-proxy",
                    ),
                    transit_encryption_enabled: Some(true),
                    has_node_group_configuration: false,
                },
            )
            .expect_err("blocked writes should still clean up proxy stops");
        assert_eq!(save_fail_proxy_stop_error.code(), "InternalFailure");

        let save_fail_backend_stop_service = blocked_persistent_service(
            "save-failure-backend-stop",
            Arc::new(FailingStopNodeRuntime),
            Arc::new(FakeProxyRuntime::with_port(8601)),
        );
        let save_fail_backend_stop_error = save_fail_backend_stop_service
            .create_replication_group(
                &scope(),
                CreateReplicationGroupInput {
                    auth_token: Some("secret".to_owned()),
                    cluster_mode: None,
                    engine: Some("redis".to_owned()),
                    engine_version: None,
                    multi_az_enabled: None,
                    num_cache_clusters: None,
                    num_node_groups: None,
                    replicas_per_node_group: None,
                    replication_group_description: "demo".to_owned(),
                    replication_group_id: replication_group_id(
                        "cache-save-backend",
                    ),
                    transit_encryption_enabled: Some(true),
                    has_node_group_configuration: false,
                },
            )
            .expect_err("blocked writes should still clean up backend stops");
        assert_eq!(save_fail_backend_stop_error.code(), "InternalFailure");

        let no_password_mode = resolve_user_authentication(
            Some(&AuthenticationModeInput {
                passwords: Vec::new(),
                type_: Some("no-password".to_owned()),
            }),
            None,
            Vec::new(),
        )
        .expect("explicit no-password mode should resolve");
        assert_eq!(
            no_password_mode.0,
            ElastiCacheAuthenticationType::NoPassword
        );

        let no_password_required_error = resolve_user_authentication(
            Some(&AuthenticationModeInput {
                passwords: vec!["secret".to_owned()],
                type_: Some("no-password-required".to_owned()),
            }),
            None,
            Vec::new(),
        )
        .expect_err("no-password users cannot also set passwords");
        assert_eq!(
            no_password_required_error.code(),
            "InvalidParameterCombination"
        );

        let unsupported_type = resolve_user_authentication(
            Some(&AuthenticationModeInput {
                passwords: Vec::new(),
                type_: Some("kerberos".to_owned()),
            }),
            None,
            Vec::new(),
        )
        .expect_err("unsupported auth types should fail");
        assert_eq!(unsupported_type.code(), "InvalidParameterValue");

        let implicit_no_password =
            resolve_user_authentication(None, None, Vec::new())
                .expect("empty inputs should default to no-password");
        assert_eq!(
            implicit_no_password.0,
            ElastiCacheAuthenticationType::NoPassword
        );

        let implicit_password = resolve_user_authentication(
            None,
            None,
            vec!["  trimmed-secret  ".to_owned()],
        )
        .expect("top-level passwords should imply password auth");
        assert_eq!(
            implicit_password,
            (
                ElastiCacheAuthenticationType::Password,
                vec!["trimmed-secret".to_owned()]
            )
        );

        let password_and_flag_error = resolve_user_authentication(
            Some(&AuthenticationModeInput {
                passwords: vec!["secret".to_owned()],
                type_: Some("password".to_owned()),
            }),
            Some(true),
            Vec::new(),
        )
        .expect_err("password users cannot also set NoPasswordRequired");
        assert_eq!(
            password_and_flag_error.code(),
            "InvalidParameterCombination"
        );

        let aws_error =
            ElastiCacheError::Internal { message: "internal".to_owned() }
                .to_aws_error();
        assert_eq!(aws_error.code(), "InternalFailure");
        assert_eq!(aws_error.status_code(), 500);
    }

    #[test]
    fn elasticache_iam_modes_and_explicit_multi_az_errors_are_supported() {
        let proxy_runtime = Arc::new(FakeProxyRuntime::with_port(8700));
        let service = service(
            "iam-modes",
            Arc::new(FakeNodeRuntime::with_port(7700)),
            proxy_runtime.clone(),
            Arc::new(RejectAllElastiCacheIamTokenValidator),
        );

        let iam_group = service
            .create_replication_group(
                &scope(),
                CreateReplicationGroupInput {
                    auth_token: None,
                    cluster_mode: None,
                    engine: Some("redis".to_owned()),
                    engine_version: None,
                    multi_az_enabled: None,
                    num_cache_clusters: None,
                    num_node_groups: None,
                    replicas_per_node_group: None,
                    replication_group_description: "demo".to_owned(),
                    replication_group_id: replication_group_id("cache-iam"),
                    transit_encryption_enabled: Some(true),
                    has_node_group_configuration: false,
                },
            )
            .expect("IAM-backed replication groups should create");
        assert!(!iam_group.auth_token_enabled);
        assert!(iam_group.transit_encryption_enabled);
        assert_eq!(
            recover(proxy_runtime.started.lock())[0].3,
            ElastiCacheAuthenticationType::Iam
        );

        let iam_user = service
            .create_user(
                &scope(),
                CreateUserInput {
                    access_string: None,
                    authentication_mode: Some(AuthenticationModeInput {
                        passwords: Vec::new(),
                        type_: Some("iam".to_owned()),
                    }),
                    engine: Some("redis".to_owned()),
                    no_password_required: None,
                    passwords: Vec::new(),
                    user_id: user_id("user-iam"),
                    user_name: "iris".to_owned(),
                },
            )
            .expect("IAM users should create");
        assert_eq!(
            iam_user.authentication.type_,
            ElastiCacheAuthenticationType::Iam
        );
        assert_eq!(iam_user.authentication.password_count, 0);

        let multi_az_error = service
            .create_replication_group(
                &scope(),
                CreateReplicationGroupInput {
                    auth_token: None,
                    cluster_mode: None,
                    engine: Some("redis".to_owned()),
                    engine_version: None,
                    multi_az_enabled: Some(true),
                    num_cache_clusters: None,
                    num_node_groups: None,
                    replicas_per_node_group: None,
                    replication_group_description: "demo".to_owned(),
                    replication_group_id: replication_group_id(
                        "cache-multi-az",
                    ),
                    transit_encryption_enabled: None,
                    has_node_group_configuration: false,
                },
            )
            .expect_err("multi-AZ should fail explicitly");
        assert_eq!(multi_az_error.code(), "InvalidParameterCombination");
        assert!(multi_az_error.message().contains("Multi-AZ"));
    }

    #[test]
    fn elasticache_replication_groups_round_trip_and_cleanup_runtime_handles()
    {
        let node_runtime = Arc::new(FakeNodeRuntime::with_port(7001));
        let proxy_runtime = Arc::new(FakeProxyRuntime::with_port(8001));
        let service = service(
            "groups-round-trip",
            node_runtime,
            proxy_runtime.clone(),
            Arc::new(RejectAllElastiCacheIamTokenValidator),
        );

        let created = service
            .create_replication_group(
                &scope(),
                CreateReplicationGroupInput {
                    auth_token: Some("secret123".to_owned()),
                    cluster_mode: None,
                    engine: Some("redis".to_owned()),
                    engine_version: None,
                    multi_az_enabled: None,
                    num_cache_clusters: None,
                    num_node_groups: None,
                    replicas_per_node_group: None,
                    replication_group_description: "demo".to_owned(),
                    replication_group_id: replication_group_id("cache-a"),
                    transit_encryption_enabled: Some(true),
                    has_node_group_configuration: false,
                },
            )
            .expect("replication group should create");

        assert_eq!(created.replication_group_id, "cache-a");
        assert_eq!(created.status, ReplicationGroupStatus::Available);
        assert_eq!(created.configuration_endpoint.port, 8001);
        assert_eq!(
            recover(proxy_runtime.started.lock()).as_slice(),
            &[(
                replication_group_id("cache-a"),
                8001,
                7001,
                ElastiCacheAuthenticationType::Password,
            )]
        );

        let described = service
            .describe_replication_groups(
                &scope(),
                Some(&replication_group_id("cache-a")),
            )
            .expect("replication group should describe");
        assert_eq!(described.len(), 1);
        assert_eq!(
            described[0].node_groups[0].primary_endpoint.port,
            created.configuration_endpoint.port
        );

        let deleted = service
            .delete_replication_group(
                &scope(),
                &replication_group_id("cache-a"),
            )
            .expect("replication group should delete");
        assert_eq!(deleted.status, ReplicationGroupStatus::Deleting);
    }

    #[test]
    fn elasticache_replication_group_rejects_unsupported_topology_inputs() {
        let service = service(
            "groups-unsupported",
            Arc::new(FakeNodeRuntime::with_port(7001)),
            Arc::new(FakeProxyRuntime::with_port(8001)),
            Arc::new(RejectAllElastiCacheIamTokenValidator),
        );

        let error = service
            .create_replication_group(
                &scope(),
                CreateReplicationGroupInput {
                    auth_token: None,
                    cluster_mode: Some("enabled".to_owned()),
                    engine: Some("redis".to_owned()),
                    engine_version: None,
                    multi_az_enabled: None,
                    num_cache_clusters: None,
                    num_node_groups: None,
                    replicas_per_node_group: None,
                    replication_group_description: "demo".to_owned(),
                    replication_group_id: replication_group_id("cache-a"),
                    transit_encryption_enabled: None,
                    has_node_group_configuration: false,
                },
            )
            .expect_err("cluster mode should fail");
        assert_eq!(error.code(), "InvalidParameterCombination");

        let topology_error = service
            .create_replication_group(
                &scope(),
                CreateReplicationGroupInput {
                    auth_token: None,
                    cluster_mode: None,
                    engine: Some("redis".to_owned()),
                    engine_version: None,
                    multi_az_enabled: Some(true),
                    num_cache_clusters: Some(2),
                    num_node_groups: None,
                    replicas_per_node_group: None,
                    replication_group_description: "demo".to_owned(),
                    replication_group_id: replication_group_id("cache-b"),
                    transit_encryption_enabled: None,
                    has_node_group_configuration: false,
                },
            )
            .expect_err("multi node should fail");
        assert_eq!(topology_error.code(), "InvalidParameterCombination");
    }

    #[test]
    fn elasticache_users_round_trip_and_password_validation_follow_current_state()
     {
        let service = service(
            "users-round-trip",
            Arc::new(FakeNodeRuntime::with_port(7001)),
            Arc::new(FakeProxyRuntime::with_port(8001)),
            Arc::new(RejectAllElastiCacheIamTokenValidator),
        );
        service
            .create_replication_group(
                &scope(),
                CreateReplicationGroupInput {
                    auth_token: Some("group-secret".to_owned()),
                    cluster_mode: None,
                    engine: Some("redis".to_owned()),
                    engine_version: None,
                    multi_az_enabled: None,
                    num_cache_clusters: None,
                    num_node_groups: None,
                    replicas_per_node_group: None,
                    replication_group_description: "demo".to_owned(),
                    replication_group_id: replication_group_id("cache-a"),
                    transit_encryption_enabled: Some(true),
                    has_node_group_configuration: false,
                },
            )
            .expect("replication group should create");

        let created = service
            .create_user(
                &scope(),
                CreateUserInput {
                    access_string: None,
                    authentication_mode: Some(AuthenticationModeInput {
                        passwords: vec!["user-secret".to_owned()],
                        type_: Some("password".to_owned()),
                    }),
                    engine: Some("redis".to_owned()),
                    no_password_required: None,
                    passwords: Vec::new(),
                    user_id: user_id("user-a"),
                    user_name: "alice".to_owned(),
                },
            )
            .expect("user should create");
        assert_eq!(created.authentication.password_count, 1);
        assert_eq!(
            created.authentication.type_,
            ElastiCacheAuthenticationType::Password
        );
        assert_eq!(created.status, "active");

        service
            .validate_password(
                &scope(),
                &replication_group_id("cache-a"),
                None,
                "group-secret",
            )
            .expect("group auth token should validate");
        service
            .validate_password(
                &scope(),
                &replication_group_id("cache-a"),
                Some("alice"),
                "user-secret",
            )
            .expect("user password should validate");
        assert!(
            service
                .validate_password(
                    &scope(),
                    &replication_group_id("cache-a"),
                    Some("alice"),
                    "wrong"
                )
                .is_err()
        );

        let modified = service
            .modify_user(
                &scope(),
                &user_id("user-a"),
                ModifyUserInput {
                    access_string: Some("on ~* +@read".to_owned()),
                    authentication_mode: Some(AuthenticationModeInput {
                        passwords: vec!["rotated-secret".to_owned()],
                        type_: Some("password".to_owned()),
                    }),
                    no_password_required: None,
                    passwords: Vec::new(),
                },
            )
            .expect("user should modify");
        assert_eq!(modified.access_string, "on ~* +@read");
        assert_eq!(modified.authentication.password_count, 1);
        assert!(
            service
                .validate_password(
                    &scope(),
                    &replication_group_id("cache-a"),
                    Some("alice"),
                    "user-secret",
                )
                .is_err()
        );
        service
            .validate_password(
                &scope(),
                &replication_group_id("cache-a"),
                Some("alice"),
                "rotated-secret",
            )
            .expect("rotated password should validate");

        let deleted = service
            .delete_user(&scope(), &user_id("user-a"))
            .expect("user should delete");
        assert_eq!(deleted.status, "deleting");
        assert!(
            service
                .validate_password(
                    &scope(),
                    &replication_group_id("cache-a"),
                    Some("alice"),
                    "rotated-secret",
                )
                .is_err()
        );
    }

    #[test]
    fn elasticache_iam_token_validation_delegates_to_the_runtime_validator() {
        let validator = Arc::new(RecordingIamTokenValidator::default());
        recover(validator.accepted.lock()).push("cache-a:token-ok".to_owned());
        let service = service(
            "iam-token",
            Arc::new(FakeNodeRuntime::with_port(7001)),
            Arc::new(FakeProxyRuntime::with_port(8001)),
            validator.clone(),
        );

        service
            .validate_iam_token(
                &scope(),
                &replication_group_id("cache-a"),
                "token-ok",
            )
            .expect("accepted token should validate");
        assert!(
            service
                .validate_iam_token(
                    &scope(),
                    &replication_group_id("cache-a"),
                    "token-bad"
                )
                .is_err()
        );
        assert_eq!(
            recover(validator.rejected.lock()).as_slice(),
            &["cache-a:token-bad".to_owned()]
        );
    }

    #[test]
    fn elasticache_user_authentication_resolution_rejects_invalid_combinations()
     {
        let password_error = resolve_user_authentication(
            Some(&AuthenticationModeInput {
                passwords: Vec::new(),
                type_: Some("password".to_owned()),
            }),
            None,
            Vec::new(),
        )
        .expect_err("password mode without passwords should fail");
        assert_eq!(password_error.code(), "InvalidParameterCombination");

        let iam_error = resolve_user_authentication(
            Some(&AuthenticationModeInput {
                passwords: vec!["secret".to_owned()],
                type_: Some("iam".to_owned()),
            }),
            None,
            Vec::new(),
        )
        .expect_err("iam mode with passwords should fail");
        assert_eq!(iam_error.code(), "InvalidParameterCombination");

        let no_password =
            resolve_user_authentication(None, Some(true), Vec::new())
                .expect("no password users should resolve");
        assert_eq!(no_password.0, ElastiCacheAuthenticationType::NoPassword);
    }

    #[test]
    fn elasticache_unsupported_validator_and_error_mapping_are_explicit() {
        let error = RejectAllElastiCacheIamTokenValidator
            .validate(&replication_group_id("cache-a"), "token")
            .expect_err("default validator should reject");
        assert!(error.contains("not configured"));

        let aws_error = ElastiCacheError::UnsupportedOperation {
            message: "unsupported".to_owned(),
        }
        .to_aws_error();
        assert_eq!(aws_error.code(), "UnsupportedOperation");
        assert_eq!(aws_error.status_code(), 400);
    }

    fn recover<T>(result: LockResult<T>) -> T {
        result.unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}
