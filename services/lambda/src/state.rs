mod hosting {
    pub(crate) use aws::{
        LambdaExecutionPackage, LambdaExecutor, LambdaInvocationRequest,
    };
}

use self::hosting::{
    LambdaExecutionPackage, LambdaExecutor, LambdaInvocationRequest,
};
use aws::{
    Arn, ArnResource, BlobKey, BlobStore, CallerIdentity, Clock,
    LambdaFunctionTarget, RandomSource, RegionId, ServiceName,
};
use iam::{IamScope, IamService};
use s3::{S3Scope, S3Service};
use serde::{Deserialize, Serialize};
use sqs::{
    ReceiveMessageInput, ReceivedMessage, SendMessageInput, SqsQueueIdentity,
    SqsService,
};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::num::TryFromIntError;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::UNIX_EPOCH;
use storage::{ScopedStorage, StorageFactory};

use crate::{
    aliases::{
        CreateAliasInput, LambdaAliasConfiguration, LambdaAliasRecord,
        ListAliasesOutput, UpdateAliasInput, lambda_alias_configuration,
        list_aliases_output, validate_alias_name,
    },
    arns::{
        event_source_mapping_arn, function_arn_with_optional_latest,
        function_arn_with_optional_qualifier, function_arn_with_qualifier,
    },
    async_delivery::{
        AsyncDestinationBodyInput, DEFAULT_ASYNC_MAX_EVENT_AGE_SECONDS,
        DEFAULT_ASYNC_MAX_RETRY_ATTEMPTS, DestinationConfigInput,
        DestinationTargetInput, FunctionEventInvokeConfigOutput,
        ListFunctionEventInvokeConfigsOutput,
        PutFunctionEventInvokeConfigInput,
        UpdateFunctionEventInvokeConfigInput, build_async_destination_body,
        destination_config_output, function_event_invoke_config_output,
        json_or_string_value, list_function_event_invoke_configs_output,
        validate_event_invoke_config_input, validate_maximum_event_age,
        validate_maximum_retry_attempts,
    },
    code::{
        LambdaCodeInput, UpdateFunctionCodeInput, sha256_base64,
        validate_handler_for_package, validate_runtime_for_package,
    },
    errors::{LambdaError, LambdaInitError},
    event_source_mappings::{
        CreateEventSourceMappingInput, EventSourceMappingOutput,
        EventSourceMappingOutputInput, ListEventSourceMappingsOutput,
        UpdateEventSourceMappingInput, build_sqs_event,
        event_source_mapping_output as build_event_source_mapping_output,
        list_event_source_mappings_output,
        validate_event_source_mapping_batch_size,
        validate_event_source_mapping_window,
    },
    functions::{
        CreateFunctionInput, LambdaFunctionCodeLocation,
        LambdaFunctionConfiguration, LambdaFunctionVersionRecord,
        LambdaGetFunctionOutput, ListFunctionsOutput,
        function_not_found_error, lambda_function_configuration,
        resolve_function_locator, resolve_function_target,
        resolve_lambda_function_target, resolve_unqualified_function,
        validate_create_function_name, validate_dead_letter_target_arn,
    },
    invocation::{
        ApiGatewayInvokeInput, InvokeInput, InvokeOutput,
        LambdaInvocationType, invoke_output, validate_invoke_payload,
    },
    permissions::{
        AddPermissionInput, AddPermissionOutput, LambdaPermissionRecord,
        LambdaPermissionRevisionStore, add_permission_output,
        build_permission_statement, function_url_access_denied_error,
        permission_allows_function_url_invoke,
        permission_allows_service_invoke, permission_revision_id,
        validate_add_permission_input,
    },
    scope::LambdaScope,
    urls::{
        CreateFunctionUrlConfigInput, FunctionUrlInvocationInput,
        FunctionUrlInvocationOutput, LambdaFunctionUrlAuthType,
        LambdaFunctionUrlConfig, LambdaFunctionUrlInvokeMode,
        LambdaFunctionUrlQualifierStore, ListFunctionUrlConfigsOutput,
        ResolvedFunctionUrlTarget, UpdateFunctionUrlConfigInput,
        build_function_url_event, function_url_config_key,
        function_url_config_qualifier, function_url_exists_error,
        function_url_not_found_error, lambda_function_url_config,
        list_function_url_configs_output, map_function_url_response,
        validate_function_url_invoke_mode, validate_function_url_qualifier,
    },
    versions::{
        LambdaAliasVersionRecord, LambdaVersionStore,
        ListVersionsByFunctionOutput, PublishVersionInput,
        ensure_version_exists, list_versions_by_function_output,
        resolve_version_state, resolve_version_with_execution_target,
    },
};

const DEFAULT_MEMORY_SIZE_MB: u32 = 128;
const DEFAULT_TIMEOUT_SECONDS: u32 = 3;
const MAX_EVENT_PAYLOAD_BYTES: usize = 1_048_576;
const MAX_REQUEST_RESPONSE_PAYLOAD_BYTES: usize = 6 * 1_048_576;
const PRECONDITION_FAILED_MESSAGE: &str = "The Revision Id provided does not match the latest Revision Id. Call the \
     GetFunction/GetAlias API to retrieve the latest Revision Id";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LambdaPackageType {
    Image,
    Zip,
}

impl LambdaPackageType {
    fn as_str(self) -> &'static str {
        match self {
            Self::Image => "Image",
            Self::Zip => "Zip",
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct CodeResolutionInput<'a> {
    code: &'a LambdaCodeInput,
    function_name: &'a str,
    handler: Option<&'a str>,
    package_type: LambdaPackageType,
    revision_id: &'a str,
    runtime: Option<&'a str>,
    scope: &'a LambdaScope,
}

pub struct LambdaServiceDependencies {
    pub blob_store: Arc<dyn BlobStore>,
    pub executor: Arc<dyn LambdaExecutor>,
    pub iam: IamService,
    pub s3: S3Service,
    pub sqs: SqsService,
    pub clock: Arc<dyn Clock>,
    pub random: Arc<dyn RandomSource>,
}

struct AsyncFailureDelivery<'a> {
    function_name: &'a str,
    executed_version: &'a str,
    destination_config: Option<&'a LambdaDestinationConfigState>,
    dead_letter_target_arn: Option<&'a str>,
    condition: &'a str,
    approximate_invoke_count: u32,
    status_code: u16,
    function_error: Option<&'a str>,
    response_payload: serde_json::Value,
}

#[derive(Clone)]
pub struct LambdaService {
    background_state: Arc<Mutex<LambdaBackgroundState>>,
    blob_store: Arc<dyn BlobStore>,
    clock: Arc<dyn Clock>,
    executor: Arc<dyn LambdaExecutor>,
    function_store: ScopedStorage<LambdaFunctionKey, LambdaFunctionState>,
    iam: IamService,
    mutation_lock: Arc<Mutex<()>>,
    random: Arc<dyn RandomSource>,
    s3: S3Service,
    sqs: SqsService,
}

impl LambdaService {
    /// # Errors
    ///
    /// Returns an error when the scoped Lambda storage cannot be loaded.
    pub fn new(
        factory: &StorageFactory,
        dependencies: LambdaServiceDependencies,
    ) -> Result<Self, LambdaInitError> {
        Self::with_store(
            factory.create_scoped("lambda", "functions"),
            dependencies,
        )
    }

    fn with_store(
        function_store: ScopedStorage<LambdaFunctionKey, LambdaFunctionState>,
        dependencies: LambdaServiceDependencies,
    ) -> Result<Self, LambdaInitError> {
        function_store.load().map_err(LambdaInitError::Store)?;
        let LambdaServiceDependencies {
            blob_store,
            executor,
            iam,
            s3,
            sqs,
            clock,
            random,
        } = dependencies;

        Ok(Self {
            background_state: Arc::new(Mutex::new(
                LambdaBackgroundState::default(),
            )),
            blob_store,
            clock,
            executor,
            function_store,
            iam,
            mutation_lock: Arc::new(Mutex::new(())),
            random,
            s3,
            sqs,
        })
    }

    /// # Errors
    ///
    /// Returns an error when validation fails, required IAM or dependency
    /// state is missing, or storage/runtime adapters reject the request.
    pub fn create_function(
        &self,
        scope: &LambdaScope,
        input: CreateFunctionInput,
    ) -> Result<LambdaFunctionConfiguration, LambdaError> {
        validate_create_function_name(&input.function_name)?;
        validate_execution_role(&self.iam, scope, &input.role)?;

        let runtime = validate_runtime_for_package(
            input.package_type,
            input.runtime.as_deref(),
        )?;
        let handler =
            validate_handler_for_package(input.package_type, input.handler)?;

        let revision_id = self.next_revision_id()?;
        let latest = LambdaVersionState {
            code: self.resolve_code_input(CodeResolutionInput {
                code: &input.code,
                function_name: &input.function_name,
                handler: handler.as_deref(),
                package_type: input.package_type,
                revision_id: &revision_id,
                runtime: runtime.as_deref(),
                scope,
            })?,
            dead_letter_target_arn: validate_dead_letter_target_arn(
                input.dead_letter_target_arn.as_deref(),
            )?,
            description: input.description.unwrap_or_default(),
            environment: input.environment,
            handler,
            last_modified_epoch_millis: self.now_epoch_millis()?,
            memory_size: input.memory_size.unwrap_or(DEFAULT_MEMORY_SIZE_MB),
            package_type: input.package_type,
            revision_id: revision_id.clone(),
            role: input.role,
            runtime,
            timeout: input.timeout.unwrap_or(DEFAULT_TIMEOUT_SECONDS),
            version: "$LATEST".to_owned(),
        };
        let mut state = LambdaFunctionState {
            aliases: BTreeMap::new(),
            event_invoke_configs: BTreeMap::new(),
            event_source_mappings: BTreeMap::new(),
            function_name: input.function_name.clone(),
            function_url_configs: BTreeMap::new(),
            latest,
            next_version: 1,
            permissions: BTreeMap::new(),
            versions: BTreeMap::new(),
        };
        let key = LambdaFunctionKey::new(scope, &input.function_name);
        let _guard = self.lock_state();

        if self.function_store.get(scope, &key).is_some() {
            return Err(LambdaError::ResourceConflict {
                message: format!(
                    "Function already exist: {}",
                    input.function_name
                ),
            });
        }

        let output = if input.publish {
            let published = self.publish_latest_version(&mut state, None)?;
            self.function_store
                .put(scope, key, state)
                .map_err(LambdaError::Store)?;
            lambda_function_configuration(
                scope,
                &input.function_name,
                &published,
            )
        } else {
            let output = lambda_function_configuration(
                scope,
                &input.function_name,
                &state.latest,
            );
            self.function_store
                .put(scope, key, state)
                .map_err(LambdaError::Store)?;
            output
        };

        Ok(output)
    }

    /// # Errors
    ///
    /// Returns an error when the target function or qualifier cannot be
    /// resolved or when persisted Lambda state cannot satisfy the request.
    pub fn get_function(
        &self,
        scope: &LambdaScope,
        function_name: &str,
        qualifier: Option<&str>,
    ) -> Result<LambdaGetFunctionOutput, LambdaError> {
        let locator =
            resolve_function_locator(scope, function_name, qualifier)?;
        let key = LambdaFunctionKey::new(scope, &locator.function_name);
        let state = self.function_store.get(scope, &key).ok_or_else(|| {
            function_not_found_error(
                scope,
                &locator.function_name,
                locator.qualifier.as_deref(),
            )
        })?;
        let version = resolve_version_state(
            scope,
            &locator.function_name,
            &state,
            locator.qualifier.as_deref(),
        )?;

        Ok(LambdaGetFunctionOutput {
            code: Some(LambdaFunctionCodeLocation {
                repository_type: version.code.repository_type().to_owned(),
            }),
            configuration: lambda_function_configuration(
                scope,
                &locator.function_name,
                version,
            ),
        })
    }

    pub fn list_functions(&self, scope: &LambdaScope) -> ListFunctionsOutput {
        let functions = self
            .function_store
            .scan(scope, |_| true)
            .into_iter()
            .map(|state| {
                lambda_function_configuration(
                    scope,
                    state.function_name(),
                    &state.latest,
                )
            })
            .collect();

        ListFunctionsOutput { functions }
    }

    /// # Errors
    ///
    /// Returns an error when the target function cannot be resolved, revision
    /// validation fails, or storage/blob dependencies reject the update.
    pub fn update_function_code(
        &self,
        scope: &LambdaScope,
        function_name: &str,
        input: UpdateFunctionCodeInput,
    ) -> Result<LambdaFunctionConfiguration, LambdaError> {
        let locator = resolve_unqualified_function(scope, function_name)?;
        let key = LambdaFunctionKey::new(scope, &locator.function_name);
        let _guard = self.lock_state();
        let mut state =
            self.function_store.get(scope, &key).ok_or_else(|| {
                function_not_found_error(scope, &locator.function_name, None)
            })?;

        ensure_revision_matches(
            state.latest.revision_id.as_str(),
            input.revision_id.as_deref(),
        )?;

        let revision_id = self.next_revision_id()?;
        let code = self.resolve_code_input(CodeResolutionInput {
            code: &input.code,
            function_name: &locator.function_name,
            handler: state.latest.handler.as_deref(),
            package_type: state.latest.package_type,
            revision_id: &revision_id,
            runtime: state.latest.runtime.as_deref(),
            scope,
        })?;
        state.latest.code = code;
        state.latest.last_modified_epoch_millis = self.now_epoch_millis()?;
        state.latest.revision_id = revision_id;

        let output = if input.publish {
            let published = self.publish_latest_version(&mut state, None)?;
            self.function_store
                .put(scope, key, state)
                .map_err(LambdaError::Store)?;
            lambda_function_configuration(
                scope,
                &locator.function_name,
                &published,
            )
        } else {
            let output = lambda_function_configuration(
                scope,
                &locator.function_name,
                &state.latest,
            );
            self.function_store
                .put(scope, key, state)
                .map_err(LambdaError::Store)?;
            output
        };

        Ok(output)
    }

    /// # Errors
    ///
    /// Returns an error when the function cannot be resolved or when blob or
    /// storage dependencies fail while removing the function state.
    pub fn delete_function(
        &self,
        scope: &LambdaScope,
        function_name: &str,
    ) -> Result<(), LambdaError> {
        let locator = resolve_unqualified_function(scope, function_name)?;
        let key = LambdaFunctionKey::new(scope, &locator.function_name);
        let _guard = self.lock_state();
        let state = self.function_store.get(scope, &key).ok_or_else(|| {
            function_not_found_error(scope, &locator.function_name, None)
        })?;

        for blob_key in state.referenced_zip_blobs() {
            self.blob_store.delete(&blob_key).map_err(LambdaError::Blob)?;
        }

        self.function_store.delete(scope, &key).map_err(LambdaError::Store)
    }

    /// # Errors
    ///
    /// Returns an error when the function cannot be resolved, revision
    /// validation fails, or the updated state cannot be persisted.
    pub fn publish_version(
        &self,
        scope: &LambdaScope,
        function_name: &str,
        input: PublishVersionInput,
    ) -> Result<LambdaFunctionConfiguration, LambdaError> {
        let locator = resolve_unqualified_function(scope, function_name)?;
        let key = LambdaFunctionKey::new(scope, &locator.function_name);
        let _guard = self.lock_state();
        let mut state =
            self.function_store.get(scope, &key).ok_or_else(|| {
                function_not_found_error(scope, &locator.function_name, None)
            })?;

        ensure_revision_matches(
            state.latest.revision_id.as_str(),
            input.revision_id.as_deref(),
        )?;
        let published =
            self.publish_latest_version(&mut state, input.description)?;
        self.function_store
            .put(scope, key, state)
            .map_err(LambdaError::Store)?;

        Ok(lambda_function_configuration(
            scope,
            &locator.function_name,
            &published,
        ))
    }

    /// # Errors
    ///
    /// Returns an error when the function cannot be resolved or its stored
    /// versions cannot be read.
    pub fn list_versions_by_function(
        &self,
        scope: &LambdaScope,
        function_name: &str,
    ) -> Result<ListVersionsByFunctionOutput, LambdaError> {
        let locator = resolve_unqualified_function(scope, function_name)?;
        let key = LambdaFunctionKey::new(scope, &locator.function_name);
        let state = self.function_store.get(scope, &key).ok_or_else(|| {
            function_not_found_error(scope, &locator.function_name, None)
        })?;

        let mut versions = vec![lambda_function_configuration(
            scope,
            &locator.function_name,
            &state.latest,
        )];
        let mut published =
            state.versions.values().cloned().collect::<Vec<_>>();
        published.sort_by_key(|version| {
            version.version.parse::<u64>().unwrap_or_default()
        });
        versions.extend(published.into_iter().map(|version| {
            lambda_function_configuration(
                scope,
                &locator.function_name,
                &version,
            )
        }));

        Ok(list_versions_by_function_output(versions))
    }

    /// # Errors
    ///
    /// Returns an error when validation fails, the function or version cannot
    /// be resolved, or the updated state cannot be persisted.
    pub fn create_alias(
        &self,
        scope: &LambdaScope,
        function_name: &str,
        input: CreateAliasInput,
    ) -> Result<LambdaAliasConfiguration, LambdaError> {
        validate_alias_name(&input.name)?;
        let locator = resolve_unqualified_function(scope, function_name)?;
        let key = LambdaFunctionKey::new(scope, &locator.function_name);
        let _guard = self.lock_state();
        let mut state =
            self.function_store.get(scope, &key).ok_or_else(|| {
                function_not_found_error(scope, &locator.function_name, None)
            })?;

        if state.aliases.contains_key(&input.name) {
            return Err(LambdaError::ResourceConflict {
                message: format!("Alias already exist: {}", input.name),
            });
        }

        ensure_version_exists(&state, &input.function_version)?;
        let alias = LambdaAliasState {
            description: input.description.unwrap_or_default(),
            function_version: input.function_version,
            revision_id: self.next_revision_id()?,
        };
        let output = lambda_alias_configuration(
            scope,
            &locator.function_name,
            &input.name,
            &alias,
        );
        state.aliases.insert(input.name, alias);
        self.function_store
            .put(scope, key, state)
            .map_err(LambdaError::Store)?;

        Ok(output)
    }

    /// # Errors
    ///
    /// Returns an error when the function or alias cannot be resolved from the
    /// scoped Lambda state.
    pub fn get_alias(
        &self,
        scope: &LambdaScope,
        function_name: &str,
        alias_name: &str,
    ) -> Result<LambdaAliasConfiguration, LambdaError> {
        let locator = resolve_unqualified_function(scope, function_name)?;
        let key = LambdaFunctionKey::new(scope, &locator.function_name);
        let state = self.function_store.get(scope, &key).ok_or_else(|| {
            function_not_found_error(scope, &locator.function_name, None)
        })?;
        let alias = state.aliases.get(alias_name).ok_or_else(|| {
            function_not_found_error(
                scope,
                &locator.function_name,
                Some(alias_name),
            )
        })?;

        Ok(lambda_alias_configuration(
            scope,
            &locator.function_name,
            alias_name,
            alias,
        ))
    }

    /// # Errors
    ///
    /// Returns an error when the function cannot be resolved or its aliases
    /// cannot be read from storage.
    pub fn list_aliases(
        &self,
        scope: &LambdaScope,
        function_name: &str,
    ) -> Result<ListAliasesOutput, LambdaError> {
        let locator = resolve_unqualified_function(scope, function_name)?;
        let key = LambdaFunctionKey::new(scope, &locator.function_name);
        let state = self.function_store.get(scope, &key).ok_or_else(|| {
            function_not_found_error(scope, &locator.function_name, None)
        })?;
        let aliases = state
            .aliases
            .iter()
            .map(|(name, alias)| {
                lambda_alias_configuration(
                    scope,
                    &locator.function_name,
                    name,
                    alias,
                )
            })
            .collect();

        Ok(list_aliases_output(aliases))
    }

    /// # Errors
    ///
    /// Returns an error when validation fails, the function or alias cannot be
    /// resolved, or the updated state cannot be persisted.
    pub fn update_alias(
        &self,
        scope: &LambdaScope,
        function_name: &str,
        alias_name: &str,
        input: UpdateAliasInput,
    ) -> Result<LambdaAliasConfiguration, LambdaError> {
        let locator = resolve_unqualified_function(scope, function_name)?;
        let key = LambdaFunctionKey::new(scope, &locator.function_name);
        let _guard = self.lock_state();
        let mut state =
            self.function_store.get(scope, &key).ok_or_else(|| {
                function_not_found_error(scope, &locator.function_name, None)
            })?;

        if let Some(function_version) = input.function_version.as_deref() {
            ensure_version_exists(&state, function_version)?;
        }

        let Some(alias) = state.aliases.get_mut(alias_name) else {
            return Err(function_not_found_error(
                scope,
                &locator.function_name,
                Some(alias_name),
            ));
        };

        ensure_revision_matches(
            alias.revision_id.as_str(),
            input.revision_id.as_deref(),
        )?;

        if let Some(function_version) = input.function_version {
            alias.function_version = function_version;
        }
        if let Some(description) = input.description {
            alias.description = description;
        }
        alias.revision_id = self.next_revision_id()?;
        let output = lambda_alias_configuration(
            scope,
            &locator.function_name,
            alias_name,
            alias,
        );
        self.function_store
            .put(scope, key, state)
            .map_err(LambdaError::Store)?;

        Ok(output)
    }

    /// # Errors
    ///
    /// Returns an error when the function or alias cannot be resolved or when
    /// the updated state cannot be persisted.
    pub fn delete_alias(
        &self,
        scope: &LambdaScope,
        function_name: &str,
        alias_name: &str,
    ) -> Result<(), LambdaError> {
        let locator = resolve_unqualified_function(scope, function_name)?;
        let key = LambdaFunctionKey::new(scope, &locator.function_name);
        let _guard = self.lock_state();
        let mut state =
            self.function_store.get(scope, &key).ok_or_else(|| {
                function_not_found_error(scope, &locator.function_name, None)
            })?;

        if state.aliases.remove(alias_name).is_none() {
            return Err(function_not_found_error(
                scope,
                &locator.function_name,
                Some(alias_name),
            ));
        }

        self.function_store.put(scope, key, state).map_err(LambdaError::Store)
    }

    /// # Errors
    ///
    /// Returns an error when asynchronous invocation delivery cannot complete.
    pub fn run_async_invocation_cycle(&self) -> Result<(), LambdaError> {
        self.process_async_invocations()
    }

    /// # Errors
    ///
    /// Returns an error when polling or invoking SQS-backed event source
    /// mappings cannot complete.
    pub fn run_sqs_event_source_mapping_cycle(
        &self,
    ) -> Result<(), LambdaError> {
        self.process_sqs_event_source_mappings()
    }

    /// # Errors
    ///
    /// Returns an error when either asynchronous delivery or event-source
    /// processing fails during the background cycle.
    pub fn run_background_cycle(&self) -> Result<(), LambdaError> {
        self.run_async_invocation_cycle()?;
        self.run_sqs_event_source_mapping_cycle()
    }

    /// # Errors
    ///
    /// Returns an error when validation fails, the target function or
    /// qualifier cannot be resolved, or the updated state cannot be persisted.
    pub fn create_function_url_config(
        &self,
        scope: &LambdaScope,
        function_name: &str,
        qualifier: Option<&str>,
        input: CreateFunctionUrlConfigInput,
    ) -> Result<LambdaFunctionUrlConfig, LambdaError> {
        validate_function_url_invoke_mode(input.invoke_mode)?;
        let locator =
            resolve_function_locator(scope, function_name, qualifier)?;
        let key = LambdaFunctionKey::new(scope, &locator.function_name);
        let _guard = self.lock_state();
        let mut state =
            self.function_store.get(scope, &key).ok_or_else(|| {
                function_not_found_error(
                    scope,
                    &locator.function_name,
                    locator.qualifier.as_deref(),
                )
            })?;
        validate_function_url_qualifier(
            scope,
            &locator.function_name,
            &state,
            locator.qualifier.as_deref(),
        )?;
        let config_key = function_url_config_key(locator.qualifier.as_deref());
        if state.function_url_configs.contains_key(config_key) {
            return Err(function_url_exists_error(
                &locator.function_name,
                locator.qualifier.as_deref(),
            ));
        }

        let output = {
            let url_state = LambdaFunctionUrlState {
                auth_type: input.auth_type,
                invoke_mode: input.invoke_mode,
                last_modified_epoch_seconds: self.now_epoch_seconds()?,
                url_id: self.next_unique_url_id(scope.region())?,
            };
            let output = self.function_url_config(
                scope,
                &locator.function_name,
                locator.qualifier.as_deref(),
                &url_state,
            );
            state
                .function_url_configs
                .insert(config_key.to_owned(), url_state);
            output
        };
        self.function_store
            .put(scope, key, state)
            .map_err(LambdaError::Store)?;

        Ok(output)
    }

    /// # Errors
    ///
    /// Returns an error when the function or qualifier cannot be resolved or
    /// when no stored URL configuration exists for the request.
    pub fn get_function_url_config(
        &self,
        scope: &LambdaScope,
        function_name: &str,
        qualifier: Option<&str>,
    ) -> Result<LambdaFunctionUrlConfig, LambdaError> {
        let locator =
            resolve_function_locator(scope, function_name, qualifier)?;
        let key = LambdaFunctionKey::new(scope, &locator.function_name);
        let state = self.function_store.get(scope, &key).ok_or_else(|| {
            function_not_found_error(
                scope,
                &locator.function_name,
                locator.qualifier.as_deref(),
            )
        })?;
        validate_function_url_qualifier(
            scope,
            &locator.function_name,
            &state,
            locator.qualifier.as_deref(),
        )?;
        let config = state
            .function_url_configs
            .get(function_url_config_key(locator.qualifier.as_deref()))
            .ok_or_else(|| {
                function_url_not_found_error(
                    function_arn_with_optional_qualifier(
                        scope,
                        &locator.function_name,
                        locator.qualifier.as_deref(),
                    ),
                )
            })?;

        Ok(self.function_url_config(
            scope,
            &locator.function_name,
            locator.qualifier.as_deref(),
            config,
        ))
    }

    /// # Errors
    ///
    /// Returns an error when the function cannot be resolved or when marker
    /// validation rejects the requested page.
    pub fn list_function_url_configs(
        &self,
        scope: &LambdaScope,
        function_name: &str,
        marker: Option<&str>,
        max_items: Option<usize>,
    ) -> Result<ListFunctionUrlConfigsOutput, LambdaError> {
        let locator = resolve_unqualified_function(scope, function_name)?;
        let key = LambdaFunctionKey::new(scope, &locator.function_name);
        let state = self.function_store.get(scope, &key).ok_or_else(|| {
            function_not_found_error(scope, &locator.function_name, None)
        })?;
        let mut configs = state
            .function_url_configs
            .iter()
            .map(|(qualifier_key, config)| {
                self.function_url_config(
                    scope,
                    &locator.function_name,
                    function_url_config_qualifier(qualifier_key),
                    config,
                )
            })
            .collect::<Vec<_>>();
        configs.sort_by(|left, right| {
            left.function_arn().cmp(right.function_arn())
        });
        let (function_url_configs, next_marker) =
            paginate_items(configs, marker, max_items)?;

        Ok(list_function_url_configs_output(function_url_configs, next_marker))
    }

    /// # Errors
    ///
    /// Returns an error when validation fails, the target function or
    /// qualifier cannot be resolved, or the updated state cannot be persisted.
    pub fn update_function_url_config(
        &self,
        scope: &LambdaScope,
        function_name: &str,
        qualifier: Option<&str>,
        input: UpdateFunctionUrlConfigInput,
    ) -> Result<LambdaFunctionUrlConfig, LambdaError> {
        if let Some(invoke_mode) = input.invoke_mode {
            validate_function_url_invoke_mode(invoke_mode)?;
        }
        let locator =
            resolve_function_locator(scope, function_name, qualifier)?;
        let key = LambdaFunctionKey::new(scope, &locator.function_name);
        let _guard = self.lock_state();
        let mut state =
            self.function_store.get(scope, &key).ok_or_else(|| {
                function_not_found_error(
                    scope,
                    &locator.function_name,
                    locator.qualifier.as_deref(),
                )
            })?;
        validate_function_url_qualifier(
            scope,
            &locator.function_name,
            &state,
            locator.qualifier.as_deref(),
        )?;
        let config = state
            .function_url_configs
            .get_mut(function_url_config_key(locator.qualifier.as_deref()))
            .ok_or_else(|| {
                function_url_not_found_error(
                    function_arn_with_optional_qualifier(
                        scope,
                        &locator.function_name,
                        locator.qualifier.as_deref(),
                    ),
                )
            })?;
        if let Some(auth_type) = input.auth_type {
            config.auth_type = auth_type;
        }
        if let Some(invoke_mode) = input.invoke_mode {
            config.invoke_mode = invoke_mode;
        }
        config.last_modified_epoch_seconds = self.now_epoch_seconds()?;
        let output = self.function_url_config(
            scope,
            &locator.function_name,
            locator.qualifier.as_deref(),
            config,
        );
        self.function_store
            .put(scope, key, state)
            .map_err(LambdaError::Store)?;

        Ok(output)
    }

    /// # Errors
    ///
    /// Returns an error when the function or qualifier cannot be resolved,
    /// when no stored URL configuration exists, or when persistence fails.
    pub fn delete_function_url_config(
        &self,
        scope: &LambdaScope,
        function_name: &str,
        qualifier: Option<&str>,
    ) -> Result<(), LambdaError> {
        let locator =
            resolve_function_locator(scope, function_name, qualifier)?;
        let key = LambdaFunctionKey::new(scope, &locator.function_name);
        let _guard = self.lock_state();
        let mut state =
            self.function_store.get(scope, &key).ok_or_else(|| {
                function_not_found_error(
                    scope,
                    &locator.function_name,
                    locator.qualifier.as_deref(),
                )
            })?;
        validate_function_url_qualifier(
            scope,
            &locator.function_name,
            &state,
            locator.qualifier.as_deref(),
        )?;
        if state
            .function_url_configs
            .remove(function_url_config_key(locator.qualifier.as_deref()))
            .is_none()
        {
            return Err(function_url_not_found_error(
                function_arn_with_optional_qualifier(
                    scope,
                    &locator.function_name,
                    locator.qualifier.as_deref(),
                ),
            ));
        }
        self.function_store.put(scope, key, state).map_err(LambdaError::Store)
    }

    /// # Errors
    ///
    /// Returns an error when validation fails, the function or qualifier
    /// cannot be resolved, or the updated permission state cannot be
    /// persisted.
    pub fn add_permission(
        &self,
        scope: &LambdaScope,
        function_name: &str,
        qualifier: Option<&str>,
        input: AddPermissionInput,
    ) -> Result<AddPermissionOutput, LambdaError> {
        validate_add_permission_input(&input)?;
        let locator =
            resolve_function_locator(scope, function_name, qualifier)?;
        let key = LambdaFunctionKey::new(scope, &locator.function_name);
        let _guard = self.lock_state();
        let mut state =
            self.function_store.get(scope, &key).ok_or_else(|| {
                function_not_found_error(
                    scope,
                    &locator.function_name,
                    locator.qualifier.as_deref(),
                )
            })?;
        validate_function_url_qualifier(
            scope,
            &locator.function_name,
            &state,
            locator.qualifier.as_deref(),
        )?;
        ensure_revision_matches(
            permission_revision_id(&state, locator.qualifier.as_deref())?,
            input.revision_id.as_deref(),
        )?;

        let permissions = state
            .permissions
            .entry(permission_key(locator.qualifier.as_deref()))
            .or_default();
        if permissions.contains_key(&input.statement_id) {
            return Err(LambdaError::ResourceConflict {
                message: format!(
                    "The statement id ({}) provided already exists. Please provide a new statement id, or remove the existing statement.",
                    input.statement_id
                ),
            });
        }

        let permission = LambdaPermissionState {
            action: input.action,
            function_url_auth_type: input.function_url_auth_type,
            principal: input.principal,
            source_arn: input.source_arn,
            statement_id: input.statement_id.clone(),
        };
        let statement = build_permission_statement(
            scope,
            &locator.function_name,
            locator.qualifier.as_deref(),
            &permission,
        )?;
        permissions.insert(input.statement_id, permission);
        self.function_store
            .put(scope, key, state)
            .map_err(LambdaError::Store)?;

        Ok(add_permission_output(statement))
    }

    /// # Errors
    ///
    /// Returns an error when validation fails, the async target cannot be
    /// resolved, destination validation fails, or persistence fails.
    pub fn put_function_event_invoke_config(
        &self,
        scope: &LambdaScope,
        function_name: &str,
        qualifier: Option<&str>,
        input: PutFunctionEventInvokeConfigInput,
    ) -> Result<FunctionEventInvokeConfigOutput, LambdaError> {
        validate_event_invoke_config_input(
            &input.destination_config,
            input.maximum_event_age_in_seconds,
            input.maximum_retry_attempts,
        )?;
        let locator =
            resolve_function_locator(scope, function_name, qualifier)?;
        let key = LambdaFunctionKey::new(scope, &locator.function_name);
        let _guard = self.lock_state();
        let mut state =
            self.function_store.get(scope, &key).ok_or_else(|| {
                function_not_found_error(
                    scope,
                    &locator.function_name,
                    locator.qualifier.as_deref(),
                )
            })?;
        ensure_async_target_exists(
            scope,
            &locator.function_name,
            &state,
            locator.qualifier.as_deref(),
        )?;
        let config = LambdaEventInvokeConfigState {
            destination_config: self.validate_destination_config(
                scope,
                &locator.function_name,
                locator.qualifier.as_deref(),
                input.destination_config,
            )?,
            last_modified_epoch_seconds: self.now_epoch_seconds()?,
            maximum_event_age_in_seconds: validate_maximum_event_age(
                input.maximum_event_age_in_seconds,
            )?,
            maximum_retry_attempts: validate_maximum_retry_attempts(
                input.maximum_retry_attempts,
            )?,
        };
        let output = self.function_event_invoke_config(
            scope,
            &locator.function_name,
            locator.qualifier.as_deref(),
            &config,
        );
        state.event_invoke_configs.insert(
            event_invoke_config_key(locator.qualifier.as_deref()).to_owned(),
            config,
        );
        self.function_store
            .put(scope, key, state)
            .map_err(LambdaError::Store)?;

        Ok(output)
    }

    /// # Errors
    ///
    /// Returns an error when validation fails, the async target cannot be
    /// resolved, destination validation fails, or persistence fails.
    pub fn update_function_event_invoke_config(
        &self,
        scope: &LambdaScope,
        function_name: &str,
        qualifier: Option<&str>,
        input: UpdateFunctionEventInvokeConfigInput,
    ) -> Result<FunctionEventInvokeConfigOutput, LambdaError> {
        validate_event_invoke_config_input(
            &input.destination_config,
            input.maximum_event_age_in_seconds,
            input.maximum_retry_attempts,
        )?;
        let locator =
            resolve_function_locator(scope, function_name, qualifier)?;
        let key = LambdaFunctionKey::new(scope, &locator.function_name);
        let _guard = self.lock_state();
        let mut state =
            self.function_store.get(scope, &key).ok_or_else(|| {
                function_not_found_error(
                    scope,
                    &locator.function_name,
                    locator.qualifier.as_deref(),
                )
            })?;
        ensure_async_target_exists(
            scope,
            &locator.function_name,
            &state,
            locator.qualifier.as_deref(),
        )?;
        let config_key = event_invoke_config_key(locator.qualifier.as_deref());
        let mut config =
            state.event_invoke_configs.get(config_key).cloned().unwrap_or(
                LambdaEventInvokeConfigState {
                    destination_config: None,
                    last_modified_epoch_seconds: self.now_epoch_seconds()?,
                    maximum_event_age_in_seconds: None,
                    maximum_retry_attempts: None,
                },
            );
        if let Some(destination_config) = input.destination_config {
            config.destination_config = self.validate_destination_config(
                scope,
                &locator.function_name,
                locator.qualifier.as_deref(),
                Some(destination_config),
            )?;
        }
        if input.maximum_event_age_in_seconds.is_some() {
            config.maximum_event_age_in_seconds = validate_maximum_event_age(
                input.maximum_event_age_in_seconds,
            )?;
        }
        if input.maximum_retry_attempts.is_some() {
            config.maximum_retry_attempts =
                validate_maximum_retry_attempts(input.maximum_retry_attempts)?;
        }
        config.last_modified_epoch_seconds = self.now_epoch_seconds()?;
        let output = self.function_event_invoke_config(
            scope,
            &locator.function_name,
            locator.qualifier.as_deref(),
            &config,
        );
        state.event_invoke_configs.insert(config_key.to_owned(), config);
        self.function_store
            .put(scope, key, state)
            .map_err(LambdaError::Store)?;

        Ok(output)
    }

    /// # Errors
    ///
    /// Returns an error when the async target cannot be resolved or when no
    /// stored invoke configuration exists for the requested qualifier.
    pub fn get_function_event_invoke_config(
        &self,
        scope: &LambdaScope,
        function_name: &str,
        qualifier: Option<&str>,
    ) -> Result<FunctionEventInvokeConfigOutput, LambdaError> {
        let locator =
            resolve_function_locator(scope, function_name, qualifier)?;
        let key = LambdaFunctionKey::new(scope, &locator.function_name);
        let state = self.function_store.get(scope, &key).ok_or_else(|| {
            function_not_found_error(
                scope,
                &locator.function_name,
                locator.qualifier.as_deref(),
            )
        })?;
        ensure_async_target_exists(
            scope,
            &locator.function_name,
            &state,
            locator.qualifier.as_deref(),
        )?;
        let config = state
            .event_invoke_configs
            .get(event_invoke_config_key(locator.qualifier.as_deref()))
            .ok_or_else(|| {
                function_event_invoke_config_not_found_error(
                    scope,
                    &locator.function_name,
                    locator.qualifier.as_deref(),
                )
            })?;

        Ok(self.function_event_invoke_config(
            scope,
            &locator.function_name,
            locator.qualifier.as_deref(),
            config,
        ))
    }

    /// # Errors
    ///
    /// Returns an error when the function cannot be resolved or when marker
    /// validation rejects the requested page.
    pub fn list_function_event_invoke_configs(
        &self,
        scope: &LambdaScope,
        function_name: &str,
        marker: Option<&str>,
        max_items: Option<usize>,
    ) -> Result<ListFunctionEventInvokeConfigsOutput, LambdaError> {
        let locator = resolve_unqualified_function(scope, function_name)?;
        let key = LambdaFunctionKey::new(scope, &locator.function_name);
        let state = self.function_store.get(scope, &key).ok_or_else(|| {
            function_not_found_error(scope, &locator.function_name, None)
        })?;
        let mut configs = state
            .event_invoke_configs
            .iter()
            .map(|(qualifier_key, config)| {
                self.function_event_invoke_config(
                    scope,
                    &locator.function_name,
                    Some(qualifier_key.as_str()),
                    config,
                )
            })
            .collect::<Vec<_>>();
        configs
            .sort_by(|left, right| left.function_arn.cmp(&right.function_arn));
        let (function_event_invoke_configs, next_marker) =
            paginate_items(configs, marker, max_items)?;

        Ok(list_function_event_invoke_configs_output(
            function_event_invoke_configs,
            next_marker,
        ))
    }

    /// # Errors
    ///
    /// Returns an error when the async target cannot be resolved, when no
    /// stored invoke configuration exists, or when persistence fails.
    pub fn delete_function_event_invoke_config(
        &self,
        scope: &LambdaScope,
        function_name: &str,
        qualifier: Option<&str>,
    ) -> Result<(), LambdaError> {
        let locator =
            resolve_function_locator(scope, function_name, qualifier)?;
        let key = LambdaFunctionKey::new(scope, &locator.function_name);
        let _guard = self.lock_state();
        let mut state =
            self.function_store.get(scope, &key).ok_or_else(|| {
                function_not_found_error(
                    scope,
                    &locator.function_name,
                    locator.qualifier.as_deref(),
                )
            })?;
        ensure_async_target_exists(
            scope,
            &locator.function_name,
            &state,
            locator.qualifier.as_deref(),
        )?;
        if state
            .event_invoke_configs
            .remove(event_invoke_config_key(locator.qualifier.as_deref()))
            .is_none()
        {
            return Err(function_event_invoke_config_not_found_error(
                scope,
                &locator.function_name,
                locator.qualifier.as_deref(),
            ));
        }
        self.function_store.put(scope, key, state).map_err(LambdaError::Store)
    }

    /// # Errors
    ///
    /// Returns an error when validation fails, the target function or event
    /// source cannot be resolved, or the updated state cannot be persisted.
    pub fn create_event_source_mapping(
        &self,
        scope: &LambdaScope,
        input: CreateEventSourceMappingInput,
    ) -> Result<EventSourceMappingOutput, LambdaError> {
        let locator =
            resolve_function_locator(scope, &input.function_name, None)?;
        let key = LambdaFunctionKey::new(scope, &locator.function_name);
        let _guard = self.lock_state();
        let mut state =
            self.function_store.get(scope, &key).ok_or_else(|| {
                function_not_found_error(
                    scope,
                    &locator.function_name,
                    locator.qualifier.as_deref(),
                )
            })?;
        ensure_async_target_exists(
            scope,
            &locator.function_name,
            &state,
            locator.qualifier.as_deref(),
        )?;
        let queue_identity =
            self.validate_sqs_event_source_arn(&input.event_source_arn)?;
        let batch_size = validate_event_source_mapping_batch_size(
            &self.sqs,
            &queue_identity,
            input.batch_size,
        )?;
        let maximum_batching_window_in_seconds =
            validate_event_source_mapping_window(
                &self.sqs,
                &queue_identity,
                input.maximum_batching_window_in_seconds,
                batch_size,
            )?;
        let target_arn = function_arn_with_optional_qualifier(
            scope,
            &locator.function_name,
            locator.qualifier.as_deref(),
        );
        if state.event_source_mappings.values().any(|mapping| {
            mapping.event_source_arn == input.event_source_arn
                && function_arn_with_optional_qualifier(
                    scope,
                    &mapping.function_name,
                    mapping.qualifier.as_deref(),
                ) == target_arn
        }) {
            return Err(LambdaError::ResourceConflict {
                message: format!(
                    "An event source mapping with SQS arn (\" {} \") and function (\" {} \") already exists.",
                    input.event_source_arn, target_arn
                ),
            });
        }

        let mapping = LambdaEventSourceMappingState {
            batch_size,
            enabled: input.enabled.unwrap_or(true),
            event_source_arn: input.event_source_arn,
            function_name: locator.function_name.clone(),
            qualifier: locator.qualifier.clone(),
            last_modified_epoch_seconds: self.now_epoch_seconds()?,
            maximum_batching_window_in_seconds,
            uuid: self.next_revision_id()?,
        };
        let output = self.event_source_mapping_output(
            scope,
            &mapping,
            Some("Creating"),
        );
        state.event_source_mappings.insert(mapping.uuid.clone(), mapping);
        self.function_store
            .put(scope, key, state)
            .map_err(LambdaError::Store)?;

        Ok(output)
    }

    /// # Errors
    ///
    /// Returns an error when no event source mapping exists for the requested
    /// UUID within the scoped Lambda state.
    pub fn get_event_source_mapping(
        &self,
        scope: &LambdaScope,
        uuid: &str,
    ) -> Result<EventSourceMappingOutput, LambdaError> {
        let (_, _, mapping) = self
            .find_event_source_mapping(scope, uuid)
            .ok_or_else(|| event_source_mapping_not_found_error(uuid))?;

        Ok(self.event_source_mapping_output(scope, &mapping, None))
    }

    /// # Errors
    ///
    /// Returns an error when function filters cannot be resolved or when
    /// marker validation rejects the requested page.
    pub fn list_event_source_mappings(
        &self,
        scope: &LambdaScope,
        function_name: Option<&str>,
        event_source_arn: Option<&str>,
        marker: Option<&str>,
        max_items: Option<usize>,
    ) -> Result<ListEventSourceMappingsOutput, LambdaError> {
        let function_filter = function_name
            .map(|function_name| {
                resolve_function_locator(scope, function_name, None)
            })
            .transpose()?;
        let mut mappings = self
            .function_store
            .scan(scope, |_| true)
            .into_iter()
            .flat_map(|state| state.event_source_mappings.into_values())
            .filter(|mapping| {
                function_filter.as_ref().is_none_or(|filter| {
                    mapping.function_name == filter.function_name
                        && mapping.qualifier == filter.qualifier
                })
            })
            .filter(|mapping| {
                event_source_arn
                    .is_none_or(|target| mapping.event_source_arn == target)
            })
            .map(|mapping| {
                self.event_source_mapping_output(scope, &mapping, None)
            })
            .collect::<Vec<_>>();
        mappings.sort_by(|left, right| left.uuid.cmp(&right.uuid));
        let (event_source_mappings, next_marker) =
            paginate_items(mappings, marker, max_items)?;

        Ok(list_event_source_mappings_output(
            event_source_mappings,
            next_marker,
        ))
    }

    /// # Errors
    ///
    /// Returns an error when validation fails, the event source mapping cannot
    /// be resolved, or the updated state cannot be persisted.
    pub fn update_event_source_mapping(
        &self,
        scope: &LambdaScope,
        uuid: &str,
        input: UpdateEventSourceMappingInput,
    ) -> Result<EventSourceMappingOutput, LambdaError> {
        self.with_event_source_mapping_mut(scope, uuid, |service, mapping| {
            let queue_identity = service
                .validate_sqs_event_source_arn(&mapping.event_source_arn)?;
            if let Some(batch_size) = input.batch_size {
                mapping.batch_size = validate_event_source_mapping_batch_size(
                    &service.sqs,
                    &queue_identity,
                    Some(batch_size),
                )?;
            }
            if let Some(enabled) = input.enabled {
                mapping.enabled = enabled;
            }
            if input.maximum_batching_window_in_seconds.is_some() {
                mapping.maximum_batching_window_in_seconds =
                    validate_event_source_mapping_window(
                        &service.sqs,
                        &queue_identity,
                        input.maximum_batching_window_in_seconds,
                        mapping.batch_size,
                    )?;
            }
            mapping.last_modified_epoch_seconds =
                service.now_epoch_seconds()?;

            Ok(service.event_source_mapping_output(scope, mapping, None))
        })
    }

    /// # Errors
    ///
    /// Returns an error when the event source mapping cannot be resolved or
    /// when the updated state cannot be persisted.
    pub fn delete_event_source_mapping(
        &self,
        scope: &LambdaScope,
        uuid: &str,
    ) -> Result<EventSourceMappingOutput, LambdaError> {
        let _guard = self.lock_state();
        let (key, mut state, mapping) = self
            .find_event_source_mapping(scope, uuid)
            .ok_or_else(|| event_source_mapping_not_found_error(uuid))?;
        let mapping = state
            .event_source_mappings
            .remove(&mapping.uuid)
            .ok_or_else(|| {
                internal_error(format!(
                    "missing event source mapping `{}` while deleting it",
                    mapping.uuid
                ))
            })?;
        let output = self.event_source_mapping_output(
            scope,
            &mapping,
            Some("Deleting"),
        );
        self.function_store
            .put(scope, key, state)
            .map_err(LambdaError::Store)?;

        Ok(output)
    }

    /// # Errors
    ///
    /// Returns an error when the function URL cannot be resolved uniquely in
    /// the requested region.
    pub fn resolve_function_url(
        &self,
        region: &RegionId,
        url_id: &str,
    ) -> Result<ResolvedFunctionUrlTarget, LambdaError> {
        let mut matches =
            self.matching_function_url_targets(region, url_id).into_iter();
        let Some(target) = matches.next() else {
            return Err(LambdaError::ResourceNotFound {
                message: format!("Function URL not found: {url_id}"),
            });
        };
        if matches.next().is_some() {
            return Err(internal_error(format!(
                "multiple function URLs matched region {} and id {}",
                region.as_str(),
                url_id,
            )));
        }

        Ok(target)
    }

    /// # Errors
    ///
    /// Returns an error when the resolved function URL target is missing,
    /// permissions reject the caller, or Lambda execution fails.
    pub fn invoke_resolved_function_url(
        &self,
        target: &ResolvedFunctionUrlTarget,
        input: FunctionUrlInvocationInput,
        caller_identity: Option<&CallerIdentity>,
    ) -> Result<FunctionUrlInvocationOutput, LambdaError> {
        let key =
            LambdaFunctionKey::new(target.scope(), target.function_name());
        let state = self.function_store.get(target.scope(), &key).ok_or_else(
            || {
                function_not_found_error(
                    target.scope(),
                    target.function_name(),
                    target.qualifier(),
                )
            },
        )?;
        let config_key = function_url_config_key(target.qualifier());
        let config =
            state.function_url_configs.get(config_key).ok_or_else(|| {
                function_url_not_found_error(
                    function_arn_with_optional_qualifier(
                        target.scope(),
                        target.function_name(),
                        target.qualifier(),
                    ),
                )
            })?;
        if config.url_id != target.url_id() {
            return Err(internal_error(format!(
                "resolved function URL target {} no longer matches stored URL {}",
                target.url_id(),
                config.url_id,
            )));
        }
        if config.auth_type == LambdaFunctionUrlAuthType::AwsIam
            && caller_identity.is_none()
        {
            return Err(function_url_access_denied_error());
        }
        if !permission_allows_function_url_invoke(
            state.permissions.get(&permission_key(target.qualifier())),
            caller_identity,
            config.auth_type,
        ) {
            return Err(function_url_access_denied_error());
        }

        let payload = build_function_url_event(
            target.scope(),
            &config.url_id,
            &input,
            caller_identity,
            self.now_epoch_millis()?,
            self.next_revision_id()?,
        )?;
        let output = self.execute_request_response(
            target.scope(),
            target.function_name(),
            target.qualifier(),
            payload,
        )?;

        map_function_url_response(output.payload())
    }

    /// # Errors
    ///
    /// Returns an error when the function URL cannot be resolved, URL
    /// permissions reject the caller, or Lambda execution fails.
    pub fn invoke_function_url(
        &self,
        scope: &LambdaScope,
        url_id: &str,
        input: FunctionUrlInvocationInput,
        caller_identity: Option<&CallerIdentity>,
    ) -> Result<FunctionUrlInvocationOutput, LambdaError> {
        let state = self
            .function_store
            .scan(scope, |_| true)
            .into_iter()
            .find(|state| {
                state
                    .function_url_configs
                    .values()
                    .any(|config| config.url_id == url_id)
            })
            .ok_or_else(|| LambdaError::ResourceNotFound {
                message: format!("Function URL not found: {url_id}"),
            })?;
        let (qualifier_key, config) = state
            .function_url_configs
            .iter()
            .find(|(_, config)| config.url_id == url_id)
            .ok_or_else(|| {
                internal_error(format!(
                    "located function URL parent state without config `{url_id}`"
                ))
            })?;
        let target = ResolvedFunctionUrlTarget::new(
            scope.clone(),
            state.function_name.clone(),
            function_url_config_qualifier(qualifier_key).map(str::to_owned),
            config.auth_type,
            config.url_id.clone(),
        );

        self.invoke_resolved_function_url(&target, input, caller_identity)
    }

    /// # Errors
    ///
    /// Returns an error when the Lambda target cannot be resolved, API
    /// Gateway lacks invoke permission, or execution fails.
    pub fn invoke_apigateway(
        &self,
        scope: &LambdaScope,
        input: &ApiGatewayInvokeInput,
    ) -> Result<InvokeOutput, LambdaError> {
        let locator = resolve_function_target(scope, &input.target)?;
        let key = LambdaFunctionKey::new(scope, &locator.function_name);
        let state = self.function_store.get(scope, &key).ok_or_else(|| {
            function_not_found_error(
                scope,
                &locator.function_name,
                locator.qualifier.as_deref(),
            )
        })?;
        let (version, executed_version) =
            resolve_version_with_execution_target(
                scope,
                &locator.function_name,
                &state,
                locator.qualifier.as_deref(),
            )?;

        if !permission_allows_service_invoke(
            state
                .permissions
                .get(&permission_key(locator.qualifier.as_deref())),
            "apigateway.amazonaws.com",
            input.source_arn.as_str(),
        ) {
            return Err(LambdaError::AccessDenied {
                message:
                    "Invalid permissions on Lambda function for API Gateway."
                        .to_owned(),
            });
        }

        let request = self.build_execution_request(
            &locator.function_name,
            &executed_version,
            version,
            input.payload.clone(),
        )?;
        let result = self
            .executor
            .invoke(&request)
            .map_err(LambdaError::InvokeBackend)?;

        Ok(invoke_output(
            executed_version,
            result.function_error().map(str::to_owned),
            result.payload().to_vec(),
            200,
        ))
    }

    /// # Errors
    ///
    /// Returns an error when the Lambda target cannot be resolved,
    /// `EventBridge` lacks invoke permission, or the async invocation cannot be
    /// enqueued.
    pub fn invoke_eventbridge(
        &self,
        scope: &LambdaScope,
        function_name: &str,
        source_arn: &str,
        payload: Vec<u8>,
    ) -> Result<InvokeOutput, LambdaError> {
        let target =
            LambdaFunctionTarget::parse(function_name).map_err(|error| {
                LambdaError::Validation { message: error.to_string() }
            })?;
        let locator = resolve_function_target(scope, &target)?;
        let key = LambdaFunctionKey::new(scope, &locator.function_name);
        let state = self.function_store.get(scope, &key).ok_or_else(|| {
            function_not_found_error(
                scope,
                &locator.function_name,
                locator.qualifier.as_deref(),
            )
        })?;

        if !permission_allows_service_invoke(
            state
                .permissions
                .get(&permission_key(locator.qualifier.as_deref())),
            "events.amazonaws.com",
            source_arn,
        ) {
            return Err(LambdaError::AccessDenied {
                message:
                    "Invalid permissions on Lambda function for EventBridge."
                        .to_owned(),
            });
        }

        self.invoke_target(
            scope,
            &target,
            InvokeInput {
                invocation_type: LambdaInvocationType::Event,
                payload,
            },
        )
    }

    /// # Errors
    ///
    /// Returns an error when the Lambda target cannot be resolved, payload
    /// validation fails, or execution/enqueue dependencies reject the request.
    pub fn invoke(
        &self,
        scope: &LambdaScope,
        function_name: &str,
        qualifier: Option<&str>,
        input: InvokeInput,
    ) -> Result<InvokeOutput, LambdaError> {
        let target = resolve_lambda_function_target(function_name, qualifier)?;

        self.invoke_target(scope, &target, input)
    }

    /// # Errors
    ///
    /// Returns an error when the Lambda target cannot be resolved, payload
    /// validation fails, or execution/enqueue dependencies reject the request.
    pub fn invoke_target(
        &self,
        scope: &LambdaScope,
        target: &LambdaFunctionTarget,
        input: InvokeInput,
    ) -> Result<InvokeOutput, LambdaError> {
        let locator = resolve_function_target(scope, target)?;
        let key = LambdaFunctionKey::new(scope, &locator.function_name);
        let state = self.function_store.get(scope, &key).ok_or_else(|| {
            function_not_found_error(
                scope,
                &locator.function_name,
                locator.qualifier.as_deref(),
            )
        })?;
        let (_, executed_version) = resolve_version_with_execution_target(
            scope,
            &locator.function_name,
            &state,
            locator.qualifier.as_deref(),
        )?;

        validate_invoke_payload(
            &input,
            MAX_EVENT_PAYLOAD_BYTES,
            MAX_REQUEST_RESPONSE_PAYLOAD_BYTES,
        )?;
        match input.invocation_type {
            LambdaInvocationType::DryRun => {
                Ok(invoke_output(executed_version, None, Vec::new(), 204))
            }
            LambdaInvocationType::RequestResponse => self
                .execute_request_response(
                    scope,
                    &locator.function_name,
                    locator.qualifier.as_deref(),
                    input.payload,
                ),
            LambdaInvocationType::Event => {
                self.enqueue_async_invocation(
                    scope,
                    &locator.function_name,
                    locator.qualifier.as_deref(),
                    input.payload,
                )?;

                Ok(invoke_output(executed_version, None, Vec::new(), 202))
            }
        }
    }

    fn lock_state(&self) -> MutexGuard<'_, ()> {
        self.mutation_lock.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn now_epoch_millis(&self) -> Result<u64, LambdaError> {
        self.clock
            .now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| LambdaError::Internal {
                message: error.to_string(),
            })?
            .as_millis()
            .try_into()
            .map_err(|error: TryFromIntError| LambdaError::Internal {
                message: error.to_string(),
            })
    }

    fn now_epoch_seconds(&self) -> Result<i64, LambdaError> {
        self.clock
            .now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| LambdaError::Internal {
                message: error.to_string(),
            })?
            .as_secs()
            .try_into()
            .map_err(|error: TryFromIntError| LambdaError::Internal {
                message: error.to_string(),
            })
    }

    fn next_revision_id(&self) -> Result<String, LambdaError> {
        let mut bytes = [0_u8; 16];
        self.random.fill_bytes(&mut bytes).map_err(LambdaError::Random)?;
        let [
            b0,
            b1,
            b2,
            b3,
            b4,
            b5,
            b6,
            b7,
            b8,
            b9,
            b10,
            b11,
            b12,
            b13,
            b14,
            b15,
        ] = bytes;

        Ok(format!(
            "{b0:02x}{b1:02x}{b2:02x}{b3:02x}{b4:02x}{b5:02x}{b6:02x}{b7:02x}-\
             {b8:02x}{b9:02x}-{b10:02x}{b11:02x}-{b12:02x}{b13:02x}-\
             {b14:02x}{b15:02x}{b0:02x}{b1:02x}{b2:02x}{b3:02x}",
        ))
    }

    fn publish_latest_version(
        &self,
        state: &mut LambdaFunctionState,
        description_override: Option<String>,
    ) -> Result<LambdaVersionState, LambdaError> {
        let version = state.next_version.to_string();
        state.next_version += 1;
        let mut published = state.latest.clone();
        published.version = version.clone();
        if let Some(description) = description_override {
            published.description = description;
        }
        published.last_modified_epoch_millis = self.now_epoch_millis()?;
        published.revision_id = self.next_revision_id()?;
        state.versions.insert(version, published.clone());
        state.latest.last_modified_epoch_millis = self.now_epoch_millis()?;
        state.latest.revision_id = self.next_revision_id()?;

        Ok(published)
    }

    fn execute_request_response(
        &self,
        scope: &LambdaScope,
        function_name: &str,
        qualifier: Option<&str>,
        payload: Vec<u8>,
    ) -> Result<InvokeOutput, LambdaError> {
        let locator =
            resolve_function_locator(scope, function_name, qualifier)?;
        let key = LambdaFunctionKey::new(scope, &locator.function_name);
        let state = self.function_store.get(scope, &key).ok_or_else(|| {
            function_not_found_error(
                scope,
                &locator.function_name,
                locator.qualifier.as_deref(),
            )
        })?;
        let (version, executed_version) =
            resolve_version_with_execution_target(
                scope,
                &locator.function_name,
                &state,
                locator.qualifier.as_deref(),
            )?;
        let request = self.build_execution_request(
            &locator.function_name,
            &executed_version,
            version,
            payload,
        )?;
        let result = self
            .executor
            .invoke(&request)
            .map_err(LambdaError::InvokeBackend)?;

        Ok(invoke_output(
            executed_version,
            result.function_error().map(str::to_owned),
            result.payload().to_vec(),
            200,
        ))
    }

    fn build_execution_request(
        &self,
        function_name: &str,
        executed_version: &str,
        version: &LambdaVersionState,
        payload: Vec<u8>,
    ) -> Result<LambdaInvocationRequest, LambdaError> {
        let runtime = version.runtime.clone().ok_or_else(|| {
            LambdaError::UnsupportedOperation {
                message: "Image package invocations are not supported by this Cloudish build.".to_owned(),
            }
        })?;
        let handler = version.handler.clone().ok_or_else(|| {
            LambdaError::UnsupportedOperation {
                message: "Image package invocations are not supported by this Cloudish build.".to_owned(),
            }
        })?;
        let package = match &version.code.package {
            LambdaCodePackage::Image { .. } => {
                return Err(LambdaError::UnsupportedOperation {
                    message: "Image package invocations are not supported by this Cloudish build.".to_owned(),
                });
            }
            LambdaCodePackage::Zip { blob_key } => {
                let archive = self
                    .blob_store
                    .get(blob_key)
                    .map_err(LambdaError::Blob)?
                    .ok_or_else(|| LambdaError::Internal {
                        message: format!(
                            "missing Lambda archive for `{function_name}`"
                        ),
                    })?;
                LambdaExecutionPackage::ZipArchive(archive)
            }
        };

        Ok(LambdaInvocationRequest::new(
            function_name.to_owned(),
            executed_version.to_owned(),
            runtime,
            handler,
            package,
            payload,
        )
        .with_environment(version.environment.clone()))
    }

    fn enqueue_async_invocation(
        &self,
        scope: &LambdaScope,
        function_name: &str,
        qualifier: Option<&str>,
        payload: Vec<u8>,
    ) -> Result<(), LambdaError> {
        let mut background_state = self
            .background_state
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        background_state.async_invocations.push_back(PendingAsyncInvocation {
            attempt_count: 0,
            enqueued_at_epoch_seconds: self.now_epoch_seconds()?,
            function_name: function_name.to_owned(),
            payload,
            qualifier: qualifier.map(str::to_owned),
            request_id: self.next_revision_id()?,
            scope: scope.clone(),
        });

        Ok(())
    }

    fn next_url_id(&self) -> Result<String, LambdaError> {
        Ok(self
            .next_revision_id()?
            .chars()
            .filter(|character| *character != '-')
            .take(10)
            .collect())
    }

    fn next_unique_url_id(
        &self,
        region: &RegionId,
    ) -> Result<String, LambdaError> {
        for _ in 0..128 {
            let url_id = self.next_url_id()?;
            if !self.function_url_exists_in_region(region, &url_id) {
                return Ok(url_id);
            }
        }

        Err(internal_error(format!(
            "failed to allocate a unique function URL id in region {}",
            region.as_str(),
        )))
    }

    fn function_url_exists_in_region(
        &self,
        region: &RegionId,
        url_id: &str,
    ) -> bool {
        !self.matching_function_url_targets(region, url_id).is_empty()
    }

    fn matching_function_url_targets(
        &self,
        region: &RegionId,
        url_id: &str,
    ) -> Vec<ResolvedFunctionUrlTarget> {
        self.function_store
            .scan_all(|_| true)
            .into_iter()
            .filter(|(scope, _)| scope.region() == region)
            .flat_map(|(scope, state)| {
                state
                    .function_url_configs
                    .iter()
                    .filter(|(_, config)| config.url_id == url_id)
                    .map(move |(qualifier_key, config)| {
                        ResolvedFunctionUrlTarget::new(
                            LambdaScope::new(
                                scope.account_id().clone(),
                                scope.region().clone(),
                            ),
                            state.function_name.clone(),
                            function_url_config_qualifier(qualifier_key)
                                .map(str::to_owned),
                            config.auth_type,
                            config.url_id.clone(),
                        )
                    })
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    fn function_url_config(
        &self,
        scope: &LambdaScope,
        function_name: &str,
        qualifier: Option<&str>,
        config: &LambdaFunctionUrlState,
    ) -> LambdaFunctionUrlConfig {
        lambda_function_url_config(
            config.auth_type,
            function_arn_with_optional_qualifier(
                scope,
                function_name,
                qualifier,
            ),
            config.invoke_mode,
            config.last_modified_epoch_seconds,
            config.url_id.clone(),
        )
    }

    fn function_event_invoke_config(
        &self,
        scope: &LambdaScope,
        function_name: &str,
        qualifier: Option<&str>,
        config: &LambdaEventInvokeConfigState,
    ) -> FunctionEventInvokeConfigOutput {
        function_event_invoke_config_output(
            config.destination_config.as_ref().map(|config| {
                destination_config_output(
                    config
                        .on_failure
                        .as_ref()
                        .map(|target| target.destination.as_str()),
                    config
                        .on_success
                        .as_ref()
                        .map(|target| target.destination.as_str()),
                )
            }),
            function_arn_with_optional_latest(scope, function_name, qualifier),
            config.last_modified_epoch_seconds,
            config.maximum_event_age_in_seconds,
            config.maximum_retry_attempts,
        )
    }

    fn event_source_mapping_output(
        &self,
        scope: &LambdaScope,
        mapping: &LambdaEventSourceMappingState,
        state_override: Option<&str>,
    ) -> EventSourceMappingOutput {
        build_event_source_mapping_output(EventSourceMappingOutputInput {
            batch_size: mapping.batch_size,
            event_source_arn: mapping.event_source_arn.clone(),
            event_source_mapping_arn: event_source_mapping_arn(
                scope,
                &mapping.uuid,
            ),
            function_arn: function_arn_with_optional_qualifier(
                scope,
                &mapping.function_name,
                mapping.qualifier.as_deref(),
            ),
            last_modified: mapping.last_modified_epoch_seconds,
            maximum_batching_window_in_seconds: mapping
                .maximum_batching_window_in_seconds,
            enabled: mapping.enabled,
            state_override: state_override.map(str::to_owned),
            uuid: mapping.uuid.clone(),
        })
    }

    fn validate_destination_config(
        &self,
        scope: &LambdaScope,
        function_name: &str,
        qualifier: Option<&str>,
        input: Option<DestinationConfigInput>,
    ) -> Result<Option<LambdaDestinationConfigState>, LambdaError> {
        let Some(input) = input else {
            return Ok(None);
        };
        let on_failure = self.validate_destination_target(
            scope,
            function_name,
            qualifier,
            input.on_failure,
        )?;
        let on_success = self.validate_destination_target(
            scope,
            function_name,
            qualifier,
            input.on_success,
        )?;
        if on_failure.is_none() && on_success.is_none() {
            return Ok(None);
        }

        Ok(Some(LambdaDestinationConfigState { on_failure, on_success }))
    }

    fn validate_destination_target(
        &self,
        scope: &LambdaScope,
        function_name: &str,
        qualifier: Option<&str>,
        input: Option<DestinationTargetInput>,
    ) -> Result<Option<LambdaDestinationTargetState>, LambdaError> {
        let Some(input) = input else {
            return Ok(None);
        };
        if input.destination.trim().is_empty() {
            return Ok(None);
        }
        let arn = input.destination.parse::<Arn>().map_err(|error| {
            LambdaError::Validation { message: error.to_string() }
        })?;
        if arn.service() != ServiceName::Sqs {
            return Err(LambdaError::UnsupportedOperation {
                message: format!(
                    "Destination service {} is not supported by this Cloudish Lambda subset.",
                    arn.service().as_str()
                ),
            });
        }
        let queue_identity = parse_sqs_queue_identity_from_arn(
            &input.destination,
            "Destination",
        )?;
        ensure_destination_queue_exists(&self.sqs, &queue_identity)?;
        let target_arn = function_arn_with_optional_qualifier(
            scope,
            function_name,
            qualifier,
        );
        if input.destination == target_arn {
            return Err(LambdaError::InvalidParameterValue {
                message: "Destination cannot target the same function."
                    .to_owned(),
            });
        }

        Ok(Some(LambdaDestinationTargetState {
            destination: input.destination,
        }))
    }

    fn validate_sqs_event_source_arn(
        &self,
        event_source_arn: &str,
    ) -> Result<SqsQueueIdentity, LambdaError> {
        let queue_identity = parse_sqs_queue_identity_from_arn(
            event_source_arn,
            "EventSourceArn",
        )?;
        ensure_destination_queue_exists(&self.sqs, &queue_identity)?;

        Ok(queue_identity)
    }

    fn find_event_source_mapping(
        &self,
        scope: &LambdaScope,
        uuid: &str,
    ) -> Option<(
        LambdaFunctionKey,
        LambdaFunctionState,
        LambdaEventSourceMappingState,
    )> {
        self.function_store.scan(scope, |_| true).into_iter().find_map(
            |state| {
                let mapping = state.event_source_mappings.get(uuid)?.clone();
                Some((
                    LambdaFunctionKey::new(scope, &state.function_name),
                    state,
                    mapping,
                ))
            },
        )
    }

    fn with_event_source_mapping_mut<T, F>(
        &self,
        scope: &LambdaScope,
        uuid: &str,
        mut update: F,
    ) -> Result<T, LambdaError>
    where
        F: FnMut(
            &Self,
            &mut LambdaEventSourceMappingState,
        ) -> Result<T, LambdaError>,
    {
        let _guard = self.lock_state();
        let (key, mut state, mapping) = self
            .find_event_source_mapping(scope, uuid)
            .ok_or_else(|| event_source_mapping_not_found_error(uuid))?;
        let mapping = state
            .event_source_mappings
            .get_mut(&mapping.uuid)
            .ok_or_else(|| {
                internal_error(format!(
                    "missing event source mapping `{}` during mutation",
                    mapping.uuid
                ))
            })?;
        let output = update(self, mapping)?;
        self.function_store
            .put(scope, key, state)
            .map_err(LambdaError::Store)?;

        Ok(output)
    }

    fn process_async_invocations(&self) -> Result<(), LambdaError> {
        let pending = {
            let mut background_state = self
                .background_state
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            background_state.async_invocations.drain(..).collect::<Vec<_>>()
        };

        for pending_invocation in pending {
            self.process_pending_async_invocation(pending_invocation)?;
        }

        Ok(())
    }

    fn process_pending_async_invocation(
        &self,
        pending: PendingAsyncInvocation,
    ) -> Result<(), LambdaError> {
        let locator = resolve_function_locator(
            &pending.scope,
            &pending.function_name,
            pending.qualifier.as_deref(),
        )?;
        let key =
            LambdaFunctionKey::new(&pending.scope, &locator.function_name);
        let Some(state) = self.function_store.get(&pending.scope, &key) else {
            return Ok(());
        };
        let (version, executed_version) =
            resolve_version_with_execution_target(
                &pending.scope,
                &locator.function_name,
                &state,
                locator.qualifier.as_deref(),
            )?;
        let config = state
            .event_invoke_configs
            .get(event_invoke_config_key(locator.qualifier.as_deref()))
            .cloned();
        let maximum_event_age_in_seconds = config
            .as_ref()
            .and_then(|config| config.maximum_event_age_in_seconds)
            .unwrap_or(DEFAULT_ASYNC_MAX_EVENT_AGE_SECONDS);
        let maximum_retry_attempts = config
            .as_ref()
            .and_then(|config| config.maximum_retry_attempts)
            .unwrap_or(DEFAULT_ASYNC_MAX_RETRY_ATTEMPTS);
        let approximate_invoke_count = pending.attempt_count + 1;
        let now_epoch_seconds = self.now_epoch_seconds()?;
        if now_epoch_seconds.saturating_sub(pending.enqueued_at_epoch_seconds)
            > i64::from(maximum_event_age_in_seconds)
        {
            self.deliver_async_failure(
                &pending,
                AsyncFailureDelivery {
                    function_name: &locator.function_name,
                    executed_version: &executed_version,
                    destination_config: config
                        .as_ref()
                        .and_then(|config| config.destination_config.as_ref()),
                    dead_letter_target_arn: version
                        .dead_letter_target_arn
                        .as_deref(),
                    condition: "EventAgeExceeded",
                    approximate_invoke_count,
                    status_code: 500,
                    function_error: Some("Unhandled"),
                    response_payload: serde_json::json!({
                        "errorMessage": "Event age exceeded the configured maximum."
                    }),
                },
            )?;
            return Ok(());
        }

        let output = self.execute_request_response(
            &pending.scope,
            &locator.function_name,
            locator.qualifier.as_deref(),
            pending.payload.clone(),
        );
        match output {
            Ok(output) if output.function_error().is_none() => {
                if let Some(destination_config) = config
                    .as_ref()
                    .and_then(|config| config.destination_config.as_ref())
                    && let Some(target) =
                        destination_config.on_success.as_ref()
                {
                    let function_arn = function_arn_with_qualifier(
                        &pending.scope,
                        &locator.function_name,
                        &executed_version,
                    );
                    self.send_async_destination_message(
                        &target.destination,
                        build_async_destination_body(
                            AsyncDestinationBodyInput {
                                request_id: &pending.request_id,
                                request_payload: &pending.payload,
                                function_arn: &function_arn,
                                condition: "Success",
                                approximate_invoke_count,
                                status_code: output.status_code(),
                                executed_version: &executed_version,
                                function_error: output.function_error(),
                                response_payload: output.payload(),
                                now_epoch_seconds,
                            },
                        )?,
                    )?;
                }
            }
            Ok(output) => {
                if approximate_invoke_count <= maximum_retry_attempts {
                    self.requeue_async_invocation(
                        pending,
                        approximate_invoke_count,
                    );
                } else {
                    self.deliver_async_failure(
                        &pending,
                        AsyncFailureDelivery {
                            function_name: &locator.function_name,
                            executed_version: &executed_version,
                            destination_config: config.as_ref().and_then(
                                |config| config.destination_config.as_ref(),
                            ),
                            dead_letter_target_arn: version
                                .dead_letter_target_arn
                                .as_deref(),
                            condition: "RetriesExhausted",
                            approximate_invoke_count,
                            status_code: output.status_code(),
                            function_error: output.function_error(),
                            response_payload: json_or_string_value(
                                output.payload(),
                            ),
                        },
                    )?;
                }
            }
            Err(error) => {
                if approximate_invoke_count <= maximum_retry_attempts {
                    self.requeue_async_invocation(
                        pending,
                        approximate_invoke_count,
                    );
                } else {
                    self.deliver_async_failure(
                        &pending,
                        AsyncFailureDelivery {
                            function_name: &locator.function_name,
                            executed_version: &executed_version,
                            destination_config: config.as_ref().and_then(
                                |config| config.destination_config.as_ref(),
                            ),
                            dead_letter_target_arn: version
                                .dead_letter_target_arn
                                .as_deref(),
                            condition: "RetriesExhausted",
                            approximate_invoke_count,
                            status_code: 500,
                            function_error: Some("Unhandled"),
                            response_payload: serde_json::json!({
                                "errorMessage": error.to_string()
                            }),
                        },
                    )?;
                }
            }
        }

        Ok(())
    }

    fn deliver_async_failure(
        &self,
        pending: &PendingAsyncInvocation,
        delivery: AsyncFailureDelivery<'_>,
    ) -> Result<(), LambdaError> {
        if let Some(target) = delivery
            .destination_config
            .and_then(|config| config.on_failure.as_ref())
        {
            let response_payload = serde_json::to_vec(
                &delivery.response_payload,
            )
            .map_err(|error| LambdaError::Internal {
                message: error.to_string(),
            })?;
            let function_arn = function_arn_with_qualifier(
                &pending.scope,
                delivery.function_name,
                delivery.executed_version,
            );
            self.send_async_destination_message(
                &target.destination,
                build_async_destination_body(AsyncDestinationBodyInput {
                    request_id: &pending.request_id,
                    request_payload: &pending.payload,
                    function_arn: &function_arn,
                    condition: delivery.condition,
                    approximate_invoke_count: delivery
                        .approximate_invoke_count,
                    status_code: delivery.status_code,
                    executed_version: delivery.executed_version,
                    function_error: delivery.function_error,
                    response_payload: &response_payload,
                    now_epoch_seconds: self.now_epoch_seconds()?,
                })?,
            )?;
        }
        if let Some(dead_letter_target_arn) = delivery.dead_letter_target_arn {
            self.send_dead_letter_message(
                dead_letter_target_arn,
                &pending.payload,
            )?;
        }

        Ok(())
    }

    fn requeue_async_invocation(
        &self,
        pending: PendingAsyncInvocation,
        attempt_count: u32,
    ) {
        let mut background_state = self
            .background_state
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        background_state
            .async_invocations
            .push_back(PendingAsyncInvocation { attempt_count, ..pending });
    }

    fn send_async_destination_message(
        &self,
        destination_arn: &str,
        body: serde_json::Value,
    ) -> Result<(), LambdaError> {
        let queue_identity =
            parse_sqs_queue_identity_from_arn(destination_arn, "Destination")?;
        self.sqs
            .send_message(
                &queue_identity,
                SendMessageInput {
                    body: serde_json::to_string(&body).map_err(|error| {
                        LambdaError::Internal { message: error.to_string() }
                    })?,
                    delay_seconds: None,
                    message_deduplication_id: None,
                    message_group_id: None,
                },
            )
            .map_err(|error| LambdaError::InvalidParameterValue {
                message: error.to_string(),
            })?;

        Ok(())
    }

    fn send_dead_letter_message(
        &self,
        destination_arn: &str,
        payload: &[u8],
    ) -> Result<(), LambdaError> {
        let queue_identity = parse_sqs_queue_identity_from_arn(
            destination_arn,
            "DeadLetterConfig",
        )?;
        self.sqs
            .send_message(
                &queue_identity,
                SendMessageInput {
                    body: String::from_utf8_lossy(payload).into_owned(),
                    delay_seconds: None,
                    message_deduplication_id: None,
                    message_group_id: None,
                },
            )
            .map_err(|error| LambdaError::InvalidParameterValue {
                message: error.to_string(),
            })?;

        Ok(())
    }

    fn process_sqs_event_source_mappings(&self) -> Result<(), LambdaError> {
        let mappings = self
            .function_store
            .scan_all(|_| true)
            .into_iter()
            .flat_map(|(scope, state)| {
                let scope = LambdaScope::new(
                    scope.account_id().clone(),
                    scope.region().clone(),
                );
                state
                    .event_source_mappings
                    .into_values()
                    .map(move |mapping| (scope.clone(), mapping))
            })
            .collect::<Vec<_>>();

        for (scope, mapping) in mappings {
            if !mapping.enabled {
                continue;
            }
            self.process_sqs_event_source_mapping(&scope, &mapping)?;
        }

        Ok(())
    }

    fn process_sqs_event_source_mapping(
        &self,
        scope: &LambdaScope,
        mapping: &LambdaEventSourceMappingState,
    ) -> Result<(), LambdaError> {
        let queue_identity =
            self.validate_sqs_event_source_arn(&mapping.event_source_arn)?;
        let messages =
            self.receive_sqs_batch(&queue_identity, mapping.batch_size)?;
        if messages.is_empty() {
            return Ok(());
        }

        let payload = build_sqs_event(
            &mapping.event_source_arn,
            queue_identity.region(),
            &messages,
        )?;
        let output = self.execute_request_response(
            scope,
            &mapping.function_name,
            mapping.qualifier.as_deref(),
            payload,
        )?;
        if output.function_error().is_none() {
            for message in &messages {
                let _ = self
                    .sqs
                    .delete_message(&queue_identity, &message.receipt_handle);
            }
        }

        Ok(())
    }

    fn receive_sqs_batch(
        &self,
        queue_identity: &SqsQueueIdentity,
        requested_batch_size: u32,
    ) -> Result<Vec<ReceivedMessage>, LambdaError> {
        let mut messages = Vec::new();

        while (messages.len() as u32) < requested_batch_size {
            let remaining = requested_batch_size - messages.len() as u32;
            let batch = self
                .sqs
                .receive_message(
                    queue_identity,
                    ReceiveMessageInput {
                        max_number_of_messages: Some(remaining.min(10)),
                        visibility_timeout: None,
                        wait_time_seconds: Some(0),
                    },
                )
                .map_err(|error| LambdaError::InvalidParameterValue {
                    message: error.to_string(),
                })?;
            if batch.is_empty() {
                break;
            }
            messages.extend(batch);
        }

        Ok(messages)
    }

    fn resolve_code_input(
        &self,
        input: CodeResolutionInput<'_>,
    ) -> Result<LambdaCodeRecord, LambdaError> {
        match (input.package_type, input.code) {
            (
                LambdaPackageType::Image,
                LambdaCodeInput::ImageUri { image_uri },
            ) => {
                if image_uri.trim().is_empty() {
                    return Err(LambdaError::InvalidParameterValue {
                        message:
                            "ImageUri is required for Image package type."
                                .to_owned(),
                    });
                }

                Ok(LambdaCodeRecord {
                    code_sha256: None,
                    code_size: 0,
                    package: LambdaCodePackage::Image {
                        image_uri: image_uri.clone(),
                    },
                })
            }
            (LambdaPackageType::Image, _) => {
                Err(LambdaError::InvalidParameterValue {
                    message: "Image package type requires ImageUri."
                        .to_owned(),
                })
            }
            (LambdaPackageType::Zip, LambdaCodeInput::ImageUri { .. }) => {
                Err(LambdaError::InvalidParameterValue {
                    message: "Zip package type requires ZipFile or S3 source."
                        .to_owned(),
                })
            }
            (
                LambdaPackageType::Zip,
                LambdaCodeInput::InlineZip { archive },
            ) => self.persist_zip_package(
                input.scope,
                input.function_name,
                input.revision_id,
                required_zip_runtime(input.runtime)?,
                required_zip_handler(input.handler)?,
                archive,
            ),
            (
                LambdaPackageType::Zip,
                LambdaCodeInput::S3Object { bucket, key, object_version },
            ) => {
                let object = self
                    .s3
                    .get_object(
                        &S3Scope::new(
                            input.scope.account_id().clone(),
                            input.scope.region().clone(),
                        ),
                        bucket,
                        key,
                        object_version.as_deref(),
                    )
                    .map_err(|_| LambdaError::InvalidParameterValue {
                        message: "Could not load deployment package from S3. Please verify the bucket and key, then try again.".to_owned(),
                    })?;

                self.persist_zip_package(
                    input.scope,
                    input.function_name,
                    input.revision_id,
                    required_zip_runtime(input.runtime)?,
                    required_zip_handler(input.handler)?,
                    &object.body,
                )
            }
        }
    }

    fn persist_zip_package(
        &self,
        scope: &LambdaScope,
        function_name: &str,
        revision_id: &str,
        runtime: &str,
        handler: &str,
        archive: &[u8],
    ) -> Result<LambdaCodeRecord, LambdaError> {
        self.executor
            .validate_zip(runtime, handler, archive)
            .map_err(|_| LambdaError::InvalidParameterValue {
                message: "Could not unzip uploaded file. Please check your file, then try to upload again.".to_owned(),
            })?;

        let blob_key = BlobKey::new(
            "lambda-code",
            format!(
                "{}/{}/{}/{}",
                scope.account_id().as_str(),
                scope.region().as_str(),
                function_name,
                revision_id
            ),
        );
        self.blob_store.put(&blob_key, archive).map_err(LambdaError::Blob)?;

        Ok(LambdaCodeRecord {
            code_sha256: Some(sha256_base64(archive)),
            code_size: archive.len(),
            package: LambdaCodePackage::Zip { blob_key },
        })
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
struct LambdaFunctionKey {
    function_name: String,
}

impl LambdaFunctionKey {
    fn new(_scope: &LambdaScope, function_name: &str) -> Self {
        Self { function_name: function_name.to_owned() }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct LambdaFunctionState {
    aliases: BTreeMap<String, LambdaAliasState>,
    #[serde(default)]
    event_invoke_configs: BTreeMap<String, LambdaEventInvokeConfigState>,
    #[serde(default)]
    event_source_mappings: BTreeMap<String, LambdaEventSourceMappingState>,
    function_name: String,
    #[serde(default)]
    function_url_configs: BTreeMap<String, LambdaFunctionUrlState>,
    latest: LambdaVersionState,
    next_version: u64,
    #[serde(default)]
    permissions: BTreeMap<String, BTreeMap<String, LambdaPermissionState>>,
    versions: BTreeMap<String, LambdaVersionState>,
}

impl LambdaFunctionState {
    fn function_name(&self) -> &str {
        &self.function_name
    }

    fn referenced_zip_blobs(&self) -> Vec<BlobKey> {
        let mut keys = BTreeSet::new();
        self.latest.insert_blob_key(&mut keys);
        for version in self.versions.values() {
            version.insert_blob_key(&mut keys);
        }

        keys.into_iter().collect()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct LambdaVersionState {
    code: LambdaCodeRecord,
    #[serde(default)]
    dead_letter_target_arn: Option<String>,
    description: String,
    environment: BTreeMap<String, String>,
    handler: Option<String>,
    last_modified_epoch_millis: u64,
    memory_size: u32,
    package_type: LambdaPackageType,
    revision_id: String,
    role: String,
    runtime: Option<String>,
    timeout: u32,
    version: String,
}

impl LambdaVersionState {
    fn insert_blob_key(&self, keys: &mut BTreeSet<BlobKey>) {
        if let LambdaCodePackage::Zip { blob_key } = &self.code.package {
            keys.insert(blob_key.clone());
        }
    }
}

impl LambdaFunctionVersionRecord for LambdaVersionState {
    fn code_sha256(&self) -> Option<&str> {
        self.code.code_sha256.as_deref()
    }

    fn code_size(&self) -> usize {
        self.code.code_size
    }

    fn dead_letter_target_arn(&self) -> Option<&str> {
        self.dead_letter_target_arn.as_deref()
    }

    fn description(&self) -> &str {
        &self.description
    }

    fn environment(&self) -> &BTreeMap<String, String> {
        &self.environment
    }

    fn handler(&self) -> Option<&str> {
        self.handler.as_deref()
    }

    fn image_uri(&self) -> Option<String> {
        self.code.image_uri()
    }

    fn last_modified_epoch_millis(&self) -> u64 {
        self.last_modified_epoch_millis
    }

    fn memory_size(&self) -> u32 {
        self.memory_size
    }

    fn package_type_name(&self) -> &str {
        self.package_type.as_str()
    }

    fn revision_id(&self) -> &str {
        &self.revision_id
    }

    fn role(&self) -> &str {
        &self.role
    }

    fn runtime(&self) -> Option<&str> {
        self.runtime.as_deref()
    }

    fn timeout(&self) -> u32 {
        self.timeout
    }

    fn version(&self) -> &str {
        &self.version
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct LambdaAliasState {
    description: String,
    function_version: String,
    revision_id: String,
}

impl LambdaAliasRecord for LambdaAliasState {
    fn description(&self) -> &str {
        &self.description
    }

    fn function_version(&self) -> &str {
        &self.function_version
    }

    fn revision_id(&self) -> &str {
        &self.revision_id
    }
}

impl LambdaAliasVersionRecord for LambdaAliasState {
    fn function_version(&self) -> &str {
        &self.function_version
    }
}

impl LambdaFunctionUrlQualifierStore for LambdaFunctionState {
    fn has_alias(&self, qualifier: &str) -> bool {
        self.aliases.contains_key(qualifier)
    }
}

impl LambdaPermissionRevisionStore for LambdaFunctionState {
    fn latest_revision_id(&self) -> &str {
        self.latest.revision_id.as_str()
    }

    fn alias_revision_id(&self, qualifier: &str) -> Option<&str> {
        self.aliases.get(qualifier).map(|alias| alias.revision_id.as_str())
    }
}

impl LambdaVersionStore for LambdaFunctionState {
    type Alias = LambdaAliasState;
    type Version = LambdaVersionState;

    fn latest(&self) -> &Self::Version {
        &self.latest
    }

    fn alias(&self, name: &str) -> Option<&Self::Alias> {
        self.aliases.get(name)
    }

    fn version(&self, version: &str) -> Option<&Self::Version> {
        self.versions.get(version)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct LambdaFunctionUrlState {
    auth_type: LambdaFunctionUrlAuthType,
    invoke_mode: LambdaFunctionUrlInvokeMode,
    last_modified_epoch_seconds: i64,
    url_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct LambdaPermissionState {
    action: String,
    function_url_auth_type: Option<LambdaFunctionUrlAuthType>,
    principal: String,
    source_arn: Option<String>,
    statement_id: String,
}

impl LambdaPermissionRecord for LambdaPermissionState {
    fn action(&self) -> &str {
        &self.action
    }

    fn function_url_auth_type(&self) -> Option<LambdaFunctionUrlAuthType> {
        self.function_url_auth_type
    }

    fn principal(&self) -> &str {
        &self.principal
    }

    fn source_arn(&self) -> Option<&str> {
        self.source_arn.as_deref()
    }

    fn statement_id(&self) -> &str {
        &self.statement_id
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct LambdaEventInvokeConfigState {
    destination_config: Option<LambdaDestinationConfigState>,
    last_modified_epoch_seconds: i64,
    maximum_event_age_in_seconds: Option<u32>,
    maximum_retry_attempts: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct LambdaDestinationConfigState {
    on_failure: Option<LambdaDestinationTargetState>,
    on_success: Option<LambdaDestinationTargetState>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct LambdaDestinationTargetState {
    destination: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct LambdaEventSourceMappingState {
    batch_size: u32,
    enabled: bool,
    event_source_arn: String,
    function_name: String,
    qualifier: Option<String>,
    last_modified_epoch_seconds: i64,
    maximum_batching_window_in_seconds: u32,
    uuid: String,
}

#[derive(Debug, Default)]
struct LambdaBackgroundState {
    async_invocations: VecDeque<PendingAsyncInvocation>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PendingAsyncInvocation {
    attempt_count: u32,
    enqueued_at_epoch_seconds: i64,
    function_name: String,
    payload: Vec<u8>,
    qualifier: Option<String>,
    request_id: String,
    scope: LambdaScope,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct LambdaCodeRecord {
    code_sha256: Option<String>,
    code_size: usize,
    package: LambdaCodePackage,
}

impl LambdaCodeRecord {
    fn image_uri(&self) -> Option<String> {
        match &self.package {
            LambdaCodePackage::Image { image_uri } => Some(image_uri.clone()),
            LambdaCodePackage::Zip { .. } => None,
        }
    }

    fn repository_type(&self) -> &'static str {
        match self.package {
            LambdaCodePackage::Image { .. } => "ECR",
            LambdaCodePackage::Zip { .. } => "S3",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
enum LambdaCodePackage {
    Image { image_uri: String },
    Zip { blob_key: BlobKey },
}

fn ensure_revision_matches(
    current: &str,
    expected: Option<&str>,
) -> Result<(), LambdaError> {
    if expected.is_some_and(|expected| expected != current) {
        return Err(LambdaError::PreconditionFailed {
            message: PRECONDITION_FAILED_MESSAGE.to_owned(),
        });
    }

    Ok(())
}

fn permission_key(qualifier: Option<&str>) -> String {
    qualifier.unwrap_or("").to_owned()
}

fn event_invoke_config_key(qualifier: Option<&str>) -> &str {
    qualifier.unwrap_or("$LATEST")
}

fn ensure_async_target_exists(
    scope: &LambdaScope,
    function_name: &str,
    state: &LambdaFunctionState,
    qualifier: Option<&str>,
) -> Result<(), LambdaError> {
    let _ = resolve_version_with_execution_target(
        scope,
        function_name,
        state,
        qualifier,
    )?;

    Ok(())
}

fn function_event_invoke_config_not_found_error(
    scope: &LambdaScope,
    function_name: &str,
    qualifier: Option<&str>,
) -> LambdaError {
    LambdaError::ResourceNotFound {
        message: format!(
            "Function event invoke config not found: {}",
            function_arn_with_optional_latest(scope, function_name, qualifier)
        ),
    }
}

fn event_source_mapping_not_found_error(uuid: &str) -> LambdaError {
    LambdaError::ResourceNotFound {
        message: format!("Event source mapping not found: {uuid}"),
    }
}

fn internal_error(message: String) -> LambdaError {
    LambdaError::Internal { message }
}

fn required_zip_runtime(runtime: Option<&str>) -> Result<&str, LambdaError> {
    runtime.ok_or_else(|| {
        internal_error(
            "validated zip package request is missing a runtime".to_owned(),
        )
    })
}

fn required_zip_handler(handler: Option<&str>) -> Result<&str, LambdaError> {
    handler.ok_or_else(|| {
        internal_error(
            "validated zip package request is missing a handler".to_owned(),
        )
    })
}

fn parse_sqs_queue_identity_from_arn(
    arn: &str,
    parameter_name: &str,
) -> Result<SqsQueueIdentity, LambdaError> {
    let parsed = arn.parse::<Arn>().map_err(|error| {
        LambdaError::Validation { message: error.to_string() }
    })?;
    if parsed.service() != ServiceName::Sqs {
        return Err(LambdaError::InvalidParameterValue {
            message: format!(
                "Value {arn} for parameter {parameter_name} is invalid."
            ),
        });
    }
    let account_id = parsed.account_id().cloned().ok_or_else(|| {
        LambdaError::InvalidParameterValue {
            message: format!(
                "Value {arn} for parameter {parameter_name} is invalid."
            ),
        }
    })?;
    let region = parsed.region().cloned().ok_or_else(|| {
        LambdaError::InvalidParameterValue {
            message: format!(
                "Value {arn} for parameter {parameter_name} is invalid."
            ),
        }
    })?;
    let ArnResource::Sqs(resource) = parsed.resource() else {
        return Err(LambdaError::InvalidParameterValue {
            message: format!(
                "Value {arn} for parameter {parameter_name} is invalid."
            ),
        });
    };

    SqsQueueIdentity::new(account_id, region, resource.queue_name()).map_err(
        |_| LambdaError::InvalidParameterValue {
            message: format!(
                "Value {arn} for parameter {parameter_name} is invalid."
            ),
        },
    )
}

fn ensure_destination_queue_exists(
    sqs: &SqsService,
    queue_identity: &SqsQueueIdentity,
) -> Result<(), LambdaError> {
    sqs.get_queue_attributes(queue_identity, &[]).map_err(|error| {
        LambdaError::InvalidParameterValue { message: error.to_string() }
    })?;

    Ok(())
}

fn paginate_items<T>(
    items: Vec<T>,
    marker: Option<&str>,
    max_items: Option<usize>,
) -> Result<(Vec<T>, Option<String>), LambdaError> {
    let start = marker
        .map(|marker| {
            marker.parse::<usize>().map_err(|_| LambdaError::Validation {
                message: "Invalid pagination marker.".to_owned(),
            })
        })
        .transpose()?
        .unwrap_or(0);
    let max_items = max_items.unwrap_or(items.len());
    let end = start.saturating_add(max_items).min(items.len());
    let next_marker = (end < items.len()).then(|| end.to_string());

    Ok((items.into_iter().skip(start).take(max_items).collect(), next_marker))
}

fn validate_execution_role(
    iam: &IamService,
    scope: &LambdaScope,
    role_arn: &str,
) -> Result<(), LambdaError> {
    let arn = role_arn.parse::<Arn>().map_err(|_| LambdaError::Validation {
        message: format!(
            "1 validation error detected: Value '{role_arn}' at 'role' failed to satisfy constraint: Member must satisfy regular expression pattern: arn:(aws[a-zA-Z-]*)?:iam::\\d{{12}}:role/?[a-zA-Z_0-9+=,.@\\-_/]+"
        ),
    })?;
    if arn.service() != ServiceName::Iam {
        return Err(LambdaError::Validation {
            message: format!(
                "1 validation error detected: Value '{role_arn}' at 'role' failed to satisfy constraint: Member must satisfy regular expression pattern: arn:(aws[a-zA-Z-]*)?:iam::\\d{{12}}:role/?[a-zA-Z_0-9+=,.@\\-_/]+"
            ),
        });
    }

    let Some(account_id) = arn.account_id() else {
        return Err(LambdaError::Validation {
            message: format!(
                "1 validation error detected: Value '{role_arn}' at 'role' failed to satisfy constraint: Member must satisfy regular expression pattern: arn:(aws[a-zA-Z-]*)?:iam::\\d{{12}}:role/?[a-zA-Z_0-9+=,.@\\-_/]+"
            ),
        });
    };
    if account_id != scope.account_id() {
        return Err(LambdaError::InvalidParameterValue {
            message:
                "The role defined for the function cannot be assumed by Lambda."
                    .to_owned(),
        });
    }

    let ArnResource::Generic(resource) = arn.resource() else {
        return Err(LambdaError::Validation {
            message: format!(
                "1 validation error detected: Value '{role_arn}' at 'role' failed to satisfy constraint: Member must satisfy regular expression pattern: arn:(aws[a-zA-Z-]*)?:iam::\\d{{12}}:role/?[a-zA-Z_0-9+=,.@\\-_/]+"
            ),
        });
    };
    let Some(role_path) = resource.strip_prefix("role/") else {
        return Err(LambdaError::Validation {
            message: format!(
                "1 validation error detected: Value '{role_arn}' at 'role' failed to satisfy constraint: Member must satisfy regular expression pattern: arn:(aws[a-zA-Z-]*)?:iam::\\d{{12}}:role/?[a-zA-Z_0-9+=,.@\\-_/]+"
            ),
        });
    };
    let role_name = role_path.rsplit('/').next().unwrap_or_default();
    if role_name.is_empty() {
        return Err(LambdaError::Validation {
            message: format!(
                "1 validation error detected: Value '{role_arn}' at 'role' failed to satisfy constraint: Member must satisfy regular expression pattern: arn:(aws[a-zA-Z-]*)?:iam::\\d{{12}}:role/?[a-zA-Z_0-9+=,.@\\-_/]+"
            ),
        });
    }

    iam.get_role(
        &IamScope::new(scope.account_id().clone(), scope.region().clone()),
        role_name,
    )
    .map_err(|_| LambdaError::InvalidParameterValue {
        message:
            "The role defined for the function cannot be assumed by Lambda."
                .to_owned(),
    })?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::hosting::{LambdaExecutor, LambdaInvocationRequest};
    use super::{
        AddPermissionInput, CreateAliasInput, CreateEventSourceMappingInput,
        CreateFunctionInput, CreateFunctionUrlConfigInput,
        DestinationConfigInput, DestinationTargetInput,
        FunctionUrlInvocationInput, InvokeInput, LambdaCodeInput, LambdaError,
        LambdaFunctionKey, LambdaFunctionState, LambdaFunctionUrlAuthType,
        LambdaFunctionUrlInvokeMode, LambdaInvocationType, LambdaPackageType,
        LambdaScope, LambdaService, LambdaServiceDependencies,
        PublishVersionInput, PutFunctionEventInvokeConfigInput,
        UpdateAliasInput, UpdateEventSourceMappingInput,
        UpdateFunctionCodeInput, UpdateFunctionUrlConfigInput,
    };
    use aws::{
        AccountId, Arn, BlobKey, BlobMetadata, BlobStore, CallerIdentity,
        Clock, InfrastructureError, LambdaInvocationResult, RandomSource,
        RegionId,
    };
    use iam::CreateRoleInput;
    use iam::IamService;
    use s3::S3Service;
    use serde_json::json;
    use sqs::{
        CreateQueueInput, ReceiveMessageInput, SendMessageInput,
        SqsQueueIdentity, SqsScope, SqsService,
    };
    use std::collections::BTreeMap;
    use std::io;
    use std::sync::atomic::{AtomicU8, Ordering};
    use std::sync::{Arc, Mutex, PoisonError};
    use std::time::{Duration, SystemTime, UNIX_EPOCH};
    use storage::{InMemoryStorage, ScopedStorage, ScopedStorageKey};

    #[derive(Default)]
    struct MemoryBlobStore {
        blobs: Mutex<BTreeMap<BlobKey, Vec<u8>>>,
    }

    impl BlobStore for MemoryBlobStore {
        fn put(
            &self,
            key: &BlobKey,
            body: &[u8],
        ) -> Result<BlobMetadata, InfrastructureError> {
            recover(self.blobs.lock()).insert(key.clone(), body.to_vec());
            Ok(BlobMetadata::new(key.clone(), body.len()))
        }

        fn get(
            &self,
            key: &BlobKey,
        ) -> Result<Option<Vec<u8>>, InfrastructureError> {
            Ok(recover(self.blobs.lock()).get(key).cloned())
        }

        fn delete(&self, key: &BlobKey) -> Result<(), InfrastructureError> {
            recover(self.blobs.lock()).remove(key);
            Ok(())
        }
    }

    #[derive(Clone)]
    struct FixedClock(SystemTime);

    impl Clock for FixedClock {
        fn now(&self) -> SystemTime {
            self.0
        }
    }

    #[derive(Clone)]
    struct SequenceRandom {
        cursor: Arc<AtomicU8>,
    }

    impl SequenceRandom {
        fn new() -> Self {
            Self { cursor: Arc::new(AtomicU8::new(0)) }
        }
    }

    impl RandomSource for SequenceRandom {
        fn fill_bytes(
            &self,
            bytes: &mut [u8],
        ) -> Result<(), InfrastructureError> {
            let start =
                self.cursor.fetch_add(bytes.len() as u8, Ordering::Relaxed);
            for (offset, byte) in bytes.iter_mut().enumerate() {
                *byte = start.wrapping_add(offset as u8);
            }
            Ok(())
        }
    }

    struct RecordingExecutor {
        async_invocations: Mutex<Vec<LambdaInvocationRequest>>,
        next_result: Mutex<LambdaInvocationResult>,
        sync_invocations: Mutex<Vec<LambdaInvocationRequest>>,
        validate_error: Mutex<Option<InfrastructureError>>,
        validate_inputs: Mutex<Vec<(String, String, Vec<u8>)>>,
    }

    impl Default for RecordingExecutor {
        fn default() -> Self {
            Self {
                async_invocations: Mutex::new(Vec::new()),
                next_result: Mutex::new(LambdaInvocationResult::new(
                    Vec::new(),
                    Option::<String>::None,
                )),
                sync_invocations: Mutex::new(Vec::new()),
                validate_error: Mutex::new(None),
                validate_inputs: Mutex::new(Vec::new()),
            }
        }
    }

    impl RecordingExecutor {
        fn set_next_result(&self, result: LambdaInvocationResult) {
            *recover(self.next_result.lock()) = result;
        }

        fn set_validate_error(&self, error: InfrastructureError) {
            *recover(self.validate_error.lock()) = Some(error);
        }
    }

    impl LambdaExecutor for RecordingExecutor {
        fn invoke(
            &self,
            request: &LambdaInvocationRequest,
        ) -> Result<LambdaInvocationResult, InfrastructureError> {
            recover(self.sync_invocations.lock()).push(request.clone());
            Ok(recover(self.next_result.lock()).clone())
        }

        fn invoke_async(
            &self,
            request: LambdaInvocationRequest,
        ) -> Result<(), InfrastructureError> {
            recover(self.async_invocations.lock()).push(request);
            Ok(())
        }

        fn validate_zip(
            &self,
            runtime: &str,
            handler: &str,
            archive: &[u8],
        ) -> Result<(), InfrastructureError> {
            recover(self.validate_inputs.lock()).push((
                runtime.to_owned(),
                handler.to_owned(),
                archive.to_vec(),
            ));
            if let Some(error) = recover(self.validate_error.lock()).take() {
                return Err(error);
            }
            Ok(())
        }
    }

    fn recover<T>(result: Result<T, PoisonError<T>>) -> T {
        result.unwrap_or_else(PoisonError::into_inner)
    }

    fn scope() -> LambdaScope {
        LambdaScope::new(
            "000000000000".parse::<AccountId>().unwrap(),
            "eu-west-2".parse::<RegionId>().unwrap(),
        )
    }

    fn alternate_scope() -> LambdaScope {
        LambdaScope::new(
            "111111111111".parse::<AccountId>().unwrap(),
            "us-east-1".parse::<RegionId>().unwrap(),
        )
    }

    fn sqs_scope() -> SqsScope {
        SqsScope::new(scope().account_id().clone(), scope().region().clone())
    }

    fn service_with_sqs(
        executor: Arc<RecordingExecutor>,
        blob_store: Arc<MemoryBlobStore>,
    ) -> (LambdaService, SqsService) {
        let iam = IamService::new();
        for scope in [scope(), alternate_scope()] {
            iam.create_role(
                &iam::IamScope::new(
                    scope.account_id().clone(),
                    scope.region().clone(),
                ),
                CreateRoleInput {
                    assume_role_policy_document: r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}"#.to_owned(),
                    description: String::new(),
                    max_session_duration: 3_600,
                    path: "/".to_owned(),
                    role_name: "lambda-role".to_owned(),
                    tags: Vec::new(),
                },
            )
            .unwrap();
        }
        let s3 = S3Service::new(
            &storage::StorageFactory::new(storage::StorageConfig::new(
                "/tmp/cloudish-lambda-s3-tests",
                storage::StorageMode::Memory,
            )),
            blob_store.clone(),
            Arc::new(FixedClock(UNIX_EPOCH + Duration::from_secs(60))),
        )
        .unwrap();
        let sqs = SqsService::new();
        let store = ScopedStorage::new(Arc::new(InMemoryStorage::<
            ScopedStorageKey<LambdaFunctionKey>,
            LambdaFunctionState,
        >::new()));

        (
            LambdaService::with_store(
                store,
                LambdaServiceDependencies {
                    blob_store,
                    executor,
                    iam,
                    s3,
                    sqs: sqs.clone(),
                    clock: Arc::new(FixedClock(
                        UNIX_EPOCH + Duration::from_secs(60),
                    )),
                    random: Arc::new(SequenceRandom::new()),
                },
            )
            .unwrap(),
            sqs,
        )
    }

    fn service(
        executor: Arc<RecordingExecutor>,
        blob_store: Arc<MemoryBlobStore>,
    ) -> LambdaService {
        service_with_sqs(executor, blob_store).0
    }

    fn inline_zip() -> LambdaCodeInput {
        LambdaCodeInput::InlineZip { archive: b"zip-bytes".to_vec() }
    }

    fn role_arn() -> &'static str {
        "arn:aws:iam::000000000000:role/service-role/lambda-role"
    }

    fn role_arn_for(scope: &LambdaScope) -> String {
        format!(
            "arn:aws:iam::{}:role/service-role/lambda-role",
            scope.account_id()
        )
    }

    fn create_queue(sqs: &SqsService, queue_name: &str) -> SqsQueueIdentity {
        sqs.create_queue(
            &sqs_scope(),
            CreateQueueInput {
                attributes: BTreeMap::new(),
                queue_name: queue_name.to_owned(),
            },
        )
        .unwrap()
    }

    fn queue_arn(sqs: &SqsService, queue: &SqsQueueIdentity) -> String {
        sqs.get_queue_attributes(queue, &[String::from("QueueArn")])
            .unwrap()
            .remove("QueueArn")
            .expect("queue ARN should be present")
    }

    #[test]
    fn lambda_core_create_get_list_and_delete_function_round_trips_metadata() {
        let executor = Arc::new(RecordingExecutor::default());
        let blob_store = Arc::new(MemoryBlobStore::default());
        let service = service(executor.clone(), blob_store.clone());

        let created = service
            .create_function(
                &scope(),
                CreateFunctionInput {
                    code: inline_zip(),
                    dead_letter_target_arn: None,
                    description: Some("demo".to_owned()),
                    environment: BTreeMap::from([(
                        "MODE".to_owned(),
                        "test".to_owned(),
                    )]),
                    function_name: "demo".to_owned(),
                    handler: Some("bootstrap.handler".to_owned()),
                    memory_size: None,
                    package_type: LambdaPackageType::Zip,
                    publish: false,
                    role: role_arn().to_owned(),
                    runtime: Some("provided.al2".to_owned()),
                    timeout: None,
                },
            )
            .unwrap();

        assert_eq!(created.function_name, "demo");
        assert_eq!(created.version, "$LATEST");
        assert_eq!(created.package_type, "Zip");
        assert!(created.code_sha256.is_some());
        assert_eq!(recover(executor.validate_inputs.lock()).len(), 1);

        let fetched = service.get_function(&scope(), "demo", None).unwrap();
        assert_eq!(fetched.configuration.function_name, "demo");
        assert_eq!(service.list_functions(&scope()).functions.len(), 1);

        service.delete_function(&scope(), "demo").unwrap();
        assert!(matches!(
            service.get_function(&scope(), "demo", None),
            Err(LambdaError::ResourceNotFound { .. })
        ));
        assert!(blob_store
            .get(&BlobKey::new("lambda-code", "000000000000/eu-west-2/demo/0001020304050607-0809-0a0b-0c0d-0e0f00010203"))
            .unwrap()
            .is_none());
    }

    #[test]
    fn lambda_core_same_function_name_is_isolated_per_scope() {
        let executor = Arc::new(RecordingExecutor::default());
        let blob_store = Arc::new(MemoryBlobStore::default());
        let service = service(executor, blob_store);
        let primary_scope = scope();
        let secondary_scope = alternate_scope();

        for (scope, description) in
            [(&primary_scope, "primary"), (&secondary_scope, "secondary")]
        {
            service
                .create_function(
                    scope,
                    CreateFunctionInput {
                        code: inline_zip(),
                        dead_letter_target_arn: None,
                        description: Some(description.to_owned()),
                        environment: BTreeMap::new(),
                        function_name: "demo".to_owned(),
                        handler: Some("bootstrap.handler".to_owned()),
                        memory_size: None,
                        package_type: LambdaPackageType::Zip,
                        publish: false,
                        role: role_arn_for(scope),
                        runtime: Some("provided.al2".to_owned()),
                        timeout: None,
                    },
                )
                .unwrap();
        }

        let primary = service
            .get_function(&primary_scope, "demo", None)
            .expect("primary scope should resolve its function");
        let secondary = service
            .get_function(&secondary_scope, "demo", None)
            .expect("secondary scope should resolve its function");

        assert_eq!(primary.configuration.description, "primary");
        assert_eq!(secondary.configuration.description, "secondary");
        assert_eq!(service.list_functions(&primary_scope).functions.len(), 1);
        assert_eq!(
            service.list_functions(&secondary_scope).functions.len(),
            1
        );

        service.delete_function(&primary_scope, "demo").unwrap();
        assert!(matches!(
            service.get_function(&primary_scope, "demo", None),
            Err(LambdaError::ResourceNotFound { .. })
        ));
        assert_eq!(
            service
                .get_function(&secondary_scope, "demo", None)
                .unwrap()
                .configuration
                .description,
            "secondary"
        );
    }

    #[test]
    fn lambda_core_publish_version_and_alias_flow_resolves_invoke_targets() {
        let executor = Arc::new(RecordingExecutor::default());
        executor.set_next_result(LambdaInvocationResult::new(
            br#"{"ok":true}"#.to_vec(),
            Option::<String>::None,
        ));
        let lambda =
            service(executor.clone(), Arc::new(MemoryBlobStore::default()));

        lambda
            .create_function(
                &scope(),
                CreateFunctionInput {
                    code: inline_zip(),
                    dead_letter_target_arn: None,
                    description: None,
                    environment: BTreeMap::new(),
                    function_name: "demo".to_owned(),
                    handler: Some("bootstrap.handler".to_owned()),
                    memory_size: None,
                    package_type: LambdaPackageType::Zip,
                    publish: false,
                    role: role_arn().to_owned(),
                    runtime: Some("provided.al2".to_owned()),
                    timeout: None,
                },
            )
            .unwrap();
        let version = lambda
            .publish_version(
                &scope(),
                "demo",
                PublishVersionInput {
                    description: Some("v1".to_owned()),
                    revision_id: None,
                },
            )
            .unwrap();
        assert_eq!(version.version, "1");

        let alias = lambda
            .create_alias(
                &scope(),
                "demo",
                CreateAliasInput {
                    description: Some("live".to_owned()),
                    function_version: "1".to_owned(),
                    name: "live".to_owned(),
                },
            )
            .unwrap();
        assert_eq!(alias.function_version, "1");

        let invoked = lambda
            .invoke(
                &scope(),
                "demo",
                Some("live"),
                InvokeInput {
                    invocation_type: LambdaInvocationType::RequestResponse,
                    payload: br#"{"hello":"world"}"#.to_vec(),
                },
            )
            .unwrap();
        assert_eq!(invoked.status_code(), 200);
        assert_eq!(invoked.executed_version(), "1");
        assert_eq!(invoked.payload(), br#"{"ok":true}"#);
        assert_eq!(recover(executor.sync_invocations.lock()).len(), 1);
        assert_eq!(
            lambda
                .list_versions_by_function(&scope(), "demo")
                .unwrap()
                .versions
                .len(),
            2
        );
        assert_eq!(
            lambda.list_aliases(&scope(), "demo").unwrap().aliases.len(),
            1
        );
    }

    #[test]
    fn lambda_core_invoke_modes_keep_execution_boundaries_explicit() {
        let executor = Arc::new(RecordingExecutor::default());
        executor.set_next_result(LambdaInvocationResult::new(
            br#"{"ok":true}"#.to_vec(),
            Option::<String>::None,
        ));
        let service =
            service(executor.clone(), Arc::new(MemoryBlobStore::default()));

        service
            .create_function(
                &scope(),
                CreateFunctionInput {
                    code: inline_zip(),
                    dead_letter_target_arn: None,
                    description: None,
                    environment: BTreeMap::new(),
                    function_name: "demo".to_owned(),
                    handler: Some("bootstrap.handler".to_owned()),
                    memory_size: None,
                    package_type: LambdaPackageType::Zip,
                    publish: false,
                    role: role_arn().to_owned(),
                    runtime: Some("provided.al2".to_owned()),
                    timeout: None,
                },
            )
            .unwrap();

        let dry_run = service
            .invoke(
                &scope(),
                "demo",
                None,
                InvokeInput {
                    invocation_type: LambdaInvocationType::DryRun,
                    payload: br#"{"noop":true}"#.to_vec(),
                },
            )
            .unwrap();
        assert_eq!(dry_run.status_code(), 204);
        assert!(recover(executor.sync_invocations.lock()).is_empty());

        let event = service
            .invoke(
                &scope(),
                "demo",
                None,
                InvokeInput {
                    invocation_type: LambdaInvocationType::Event,
                    payload: br#"{"async":true}"#.to_vec(),
                },
            )
            .unwrap();
        assert_eq!(event.status_code(), 202);
        assert!(recover(executor.async_invocations.lock()).is_empty());
        assert!(recover(executor.sync_invocations.lock()).is_empty());
        assert_eq!(
            recover(service.background_state.lock()).async_invocations.len(),
            1
        );

        service.run_background_cycle().unwrap();

        assert!(
            recover(service.background_state.lock())
                .async_invocations
                .is_empty()
        );
        assert_eq!(recover(executor.sync_invocations.lock()).len(), 1);
    }

    #[test]
    fn lambda_core_invalid_role_runtime_zip_and_qualifier_fail_before_execution()
     {
        let executor = Arc::new(RecordingExecutor::default());
        executor.set_validate_error(InfrastructureError::container(
            "validate",
            "lambda-zip",
            io::Error::other("invalid zip"),
        ));
        let lambda =
            service(executor.clone(), Arc::new(MemoryBlobStore::default()));

        let missing_role = lambda.create_function(
            &scope(),
            CreateFunctionInput {
                code: inline_zip(),
                dead_letter_target_arn: None,
                description: None,
                environment: BTreeMap::new(),
                function_name: "demo".to_owned(),
                handler: Some("bootstrap.handler".to_owned()),
                memory_size: None,
                package_type: LambdaPackageType::Zip,
                publish: false,
                role: "arn:aws:iam::000000000000:role/service-role/missing"
                    .to_owned(),
                runtime: Some("provided.al2".to_owned()),
                timeout: None,
            },
        );
        assert!(matches!(
            missing_role,
            Err(LambdaError::InvalidParameterValue { .. })
        ));
        assert!(recover(executor.validate_inputs.lock()).is_empty());

        let unsupported_runtime = lambda.create_function(
            &scope(),
            CreateFunctionInput {
                code: inline_zip(),
                dead_letter_target_arn: None,
                description: None,
                environment: BTreeMap::new(),
                function_name: "demo".to_owned(),
                handler: Some("bootstrap.handler".to_owned()),
                memory_size: None,
                package_type: LambdaPackageType::Zip,
                publish: false,
                role: role_arn().to_owned(),
                runtime: Some("nodejs22.x".to_owned()),
                timeout: None,
            },
        );
        assert!(matches!(
            unsupported_runtime,
            Err(LambdaError::InvalidParameterValue { .. })
        ));
        assert!(recover(executor.validate_inputs.lock()).is_empty());

        let invalid_zip = lambda.create_function(
            &scope(),
            CreateFunctionInput {
                code: inline_zip(),
                dead_letter_target_arn: None,
                description: None,
                environment: BTreeMap::new(),
                function_name: "demo".to_owned(),
                handler: Some("bootstrap.handler".to_owned()),
                memory_size: None,
                package_type: LambdaPackageType::Zip,
                publish: false,
                role: role_arn().to_owned(),
                runtime: Some("provided.al2".to_owned()),
                timeout: None,
            },
        );
        assert!(matches!(
            invalid_zip,
            Err(LambdaError::InvalidParameterValue { .. })
        ));
        assert_eq!(recover(executor.validate_inputs.lock()).len(), 1);

        let valid_service = service(
            Arc::new(RecordingExecutor::default()),
            Arc::new(MemoryBlobStore::default()),
        );
        valid_service
            .create_function(
                &scope(),
                CreateFunctionInput {
                    code: inline_zip(),
                    dead_letter_target_arn: None,
                    description: None,
                    environment: BTreeMap::new(),
                    function_name: "demo".to_owned(),
                    handler: Some("bootstrap.handler".to_owned()),
                    memory_size: None,
                    package_type: LambdaPackageType::Zip,
                    publish: false,
                    role: role_arn().to_owned(),
                    runtime: Some("provided.al2".to_owned()),
                    timeout: None,
                },
            )
            .unwrap();
        let missing_qualifier = valid_service.invoke(
            &scope(),
            "demo",
            Some("missing"),
            InvokeInput {
                invocation_type: LambdaInvocationType::RequestResponse,
                payload: br#"{"hello":"world"}"#.to_vec(),
            },
        );
        assert!(matches!(
            missing_qualifier,
            Err(LambdaError::ResourceNotFound { .. })
        ));
    }

    #[test]
    fn lambda_core_update_alias_and_update_code_require_matching_revision_ids()
    {
        let executor = Arc::new(RecordingExecutor::default());
        let lambda =
            service(executor.clone(), Arc::new(MemoryBlobStore::default()));

        let created = lambda
            .create_function(
                &scope(),
                CreateFunctionInput {
                    code: inline_zip(),
                    dead_letter_target_arn: None,
                    description: None,
                    environment: BTreeMap::new(),
                    function_name: "demo".to_owned(),
                    handler: Some("bootstrap.handler".to_owned()),
                    memory_size: None,
                    package_type: LambdaPackageType::Zip,
                    publish: false,
                    role: role_arn().to_owned(),
                    runtime: Some("provided.al2".to_owned()),
                    timeout: None,
                },
            )
            .unwrap();
        let version = lambda
            .publish_version(
                &scope(),
                "demo",
                PublishVersionInput {
                    description: None,
                    revision_id: Some(created.revision_id.clone()),
                },
            )
            .unwrap();
        let alias = lambda
            .create_alias(
                &scope(),
                "demo",
                CreateAliasInput {
                    description: None,
                    function_version: version.version.clone(),
                    name: "live".to_owned(),
                },
            )
            .unwrap();

        let update_alias = lambda.update_alias(
            &scope(),
            "demo",
            "live",
            UpdateAliasInput {
                description: Some("new".to_owned()),
                function_version: None,
                revision_id: Some("wrong".to_owned()),
            },
        );
        assert!(matches!(
            update_alias,
            Err(LambdaError::PreconditionFailed { .. })
        ));

        let update_code = lambda.update_function_code(
            &scope(),
            "demo",
            UpdateFunctionCodeInput {
                code: inline_zip(),
                publish: false,
                revision_id: Some("wrong".to_owned()),
            },
        );
        assert!(matches!(
            update_code,
            Err(LambdaError::PreconditionFailed { .. })
        ));

        let updated_alias = lambda
            .update_alias(
                &scope(),
                "demo",
                "live",
                UpdateAliasInput {
                    description: Some("new".to_owned()),
                    function_version: Some("1".to_owned()),
                    revision_id: Some(alias.revision_id.clone()),
                },
            )
            .unwrap();
        assert_eq!(updated_alias.description, "new");
    }

    #[test]
    fn lambda_url_function_url_crud_and_iam_context_are_explicit() {
        let executor = Arc::new(RecordingExecutor::default());
        executor.set_next_result(LambdaInvocationResult::new(
            br#"{"statusCode":201,"headers":{"content-type":"text/plain","x-mode":"lambda-url"},"body":"ok"}"#.to_vec(),
            Option::<String>::None,
        ));
        let lambda =
            service(executor.clone(), Arc::new(MemoryBlobStore::default()));

        lambda
            .create_function(
                &scope(),
                CreateFunctionInput {
                    code: inline_zip(),
                    dead_letter_target_arn: None,
                    description: None,
                    environment: BTreeMap::new(),
                    function_name: "demo".to_owned(),
                    handler: Some("bootstrap.handler".to_owned()),
                    memory_size: None,
                    package_type: LambdaPackageType::Zip,
                    publish: false,
                    role: role_arn().to_owned(),
                    runtime: Some("provided.al2".to_owned()),
                    timeout: None,
                },
            )
            .unwrap();
        let version = lambda
            .publish_version(
                &scope(),
                "demo",
                PublishVersionInput { description: None, revision_id: None },
            )
            .unwrap();
        lambda
            .create_alias(
                &scope(),
                "demo",
                CreateAliasInput {
                    description: None,
                    function_version: version.version,
                    name: "live".to_owned(),
                },
            )
            .unwrap();
        let url_config = lambda
            .create_function_url_config(
                &scope(),
                "demo",
                Some("live"),
                CreateFunctionUrlConfigInput {
                    auth_type: LambdaFunctionUrlAuthType::None,
                    invoke_mode: LambdaFunctionUrlInvokeMode::Buffered,
                },
            )
            .unwrap();
        let fetched = lambda
            .get_function_url_config(&scope(), "demo", Some("live"))
            .unwrap();
        assert_eq!(fetched.url_id(), url_config.url_id());
        assert_eq!(
            lambda
                .list_function_url_configs(&scope(), "demo", None, None)
                .unwrap()
                .function_url_configs()
                .len(),
            1
        );
        assert!(matches!(
            lambda.invoke_function_url(
                &scope(),
                url_config.url_id(),
                FunctionUrlInvocationInput {
                    body: br#"{"hello":"world"}"#.to_vec(),
                    domain_name: "localhost:4566".to_owned(),
                    headers: vec![(
                        "user-agent".to_owned(),
                        "cloudish-tests".to_owned(),
                    )],
                    method: "POST".to_owned(),
                    path: "/custom/path".to_owned(),
                    protocol: None,
                    query_string: Some("mode=test".to_owned()),
                    source_ip: Some("127.0.0.1".to_owned()),
                },
                None,
            ),
            Err(LambdaError::AccessDenied { .. })
        ));

        lambda
            .add_permission(
                &scope(),
                "demo",
                Some("live"),
                AddPermissionInput {
                    action: "lambda:InvokeFunctionUrl".to_owned(),
                    function_url_auth_type: Some(
                        LambdaFunctionUrlAuthType::None,
                    ),
                    principal: "*".to_owned(),
                    revision_id: None,
                    source_arn: None,
                    statement_id: "allow-public".to_owned(),
                },
            )
            .unwrap();
        let none_auth_response = lambda
            .invoke_function_url(
                &scope(),
                url_config.url_id(),
                FunctionUrlInvocationInput {
                    body: br#"{"hello":"world"}"#.to_vec(),
                    domain_name: "localhost:4566".to_owned(),
                    headers: vec![(
                        "cookie".to_owned(),
                        "theme=light".to_owned(),
                    )],
                    method: "POST".to_owned(),
                    path: "/custom/path".to_owned(),
                    protocol: None,
                    query_string: Some("mode=test".to_owned()),
                    source_ip: Some("127.0.0.1".to_owned()),
                },
                None,
            )
            .unwrap();
        assert_eq!(none_auth_response.status_code(), 201);
        assert_eq!(none_auth_response.body(), b"ok");

        let updated = lambda
            .update_function_url_config(
                &scope(),
                "demo",
                Some("live"),
                UpdateFunctionUrlConfigInput {
                    auth_type: Some(LambdaFunctionUrlAuthType::AwsIam),
                    invoke_mode: Some(LambdaFunctionUrlInvokeMode::Buffered),
                },
            )
            .unwrap();
        assert_eq!(updated.auth_type(), LambdaFunctionUrlAuthType::AwsIam);
        assert!(matches!(
            lambda.invoke_function_url(
                &scope(),
                url_config.url_id(),
                FunctionUrlInvocationInput {
                    body: br#"{"hello":"world"}"#.to_vec(),
                    domain_name: "localhost:4566".to_owned(),
                    headers: Vec::new(),
                    method: "POST".to_owned(),
                    path: "/custom/path".to_owned(),
                    protocol: None,
                    query_string: None,
                    source_ip: None,
                },
                None,
            ),
            Err(LambdaError::AccessDenied { .. })
        ));

        let permission = lambda
            .add_permission(
                &scope(),
                "demo",
                Some("live"),
                AddPermissionInput {
                    action: "lambda:InvokeFunctionUrl".to_owned(),
                    function_url_auth_type: Some(
                        LambdaFunctionUrlAuthType::AwsIam,
                    ),
                    principal: "000000000000".to_owned(),
                    revision_id: None,
                    source_arn: None,
                    statement_id: "allow-account".to_owned(),
                },
            )
            .unwrap();
        assert!(permission.statement().contains("AWS_IAM"));

        let caller_identity = CallerIdentity::try_new(
            "arn:aws:iam::000000000000:user/demo".parse::<Arn>().unwrap(),
            "AIDAEXAMPLE",
        )
        .unwrap();
        let iam_response = lambda
            .invoke_function_url(
                &scope(),
                url_config.url_id(),
                FunctionUrlInvocationInput {
                    body: br#"{"hello":"world"}"#.to_vec(),
                    domain_name: "localhost:4566".to_owned(),
                    headers: vec![(
                        "user-agent".to_owned(),
                        "cloudish-tests".to_owned(),
                    )],
                    method: "POST".to_owned(),
                    path: "/custom/path".to_owned(),
                    protocol: Some("HTTP/1.1".to_owned()),
                    query_string: Some("mode=test".to_owned()),
                    source_ip: Some("10.0.0.1".to_owned()),
                },
                Some(&caller_identity),
            )
            .unwrap();
        assert_eq!(iam_response.status_code(), 201);
        assert_eq!(iam_response.body(), b"ok");
        assert!(iam_response.headers().iter().any(|(name, value)| {
            name.eq_ignore_ascii_case("x-mode") && value == "lambda-url"
        }));

        let recorded = recover(executor.sync_invocations.lock());
        let event: serde_json::Value =
            serde_json::from_slice(recorded.last().unwrap().payload())
                .unwrap();
        assert_eq!(event["rawPath"], "/custom/path");
        assert_eq!(event["rawQueryString"], "mode=test");
        assert_eq!(
            event["requestContext"]["authorizer"]["iam"]["userArn"],
            caller_identity.arn().to_string()
        );

        lambda
            .delete_function_url_config(&scope(), "demo", Some("live"))
            .unwrap();
        assert!(matches!(
            lambda.get_function_url_config(&scope(), "demo", Some("live")),
            Err(LambdaError::ResourceNotFound { .. })
        ));
    }

    #[test]
    fn lambda_url_resolution_uses_the_owning_scope() {
        let executor = Arc::new(RecordingExecutor::default());
        executor.set_next_result(LambdaInvocationResult::new(
            br#"{"statusCode":200,"headers":{"content-type":"text/plain"},"body":"ok"}"#
                .to_vec(),
            Option::<String>::None,
        ));
        let lambda =
            service(executor.clone(), Arc::new(MemoryBlobStore::default()));
        let alternate_scope = alternate_scope();

        lambda
            .create_function(
                &alternate_scope,
                CreateFunctionInput {
                    code: inline_zip(),
                    dead_letter_target_arn: None,
                    description: None,
                    environment: BTreeMap::new(),
                    function_name: "alternate-demo".to_owned(),
                    handler: Some("bootstrap.handler".to_owned()),
                    memory_size: None,
                    package_type: LambdaPackageType::Zip,
                    publish: false,
                    role: role_arn_for(&alternate_scope),
                    runtime: Some("provided.al2".to_owned()),
                    timeout: None,
                },
            )
            .unwrap();
        let url_config = lambda
            .create_function_url_config(
                &alternate_scope,
                "alternate-demo",
                None,
                CreateFunctionUrlConfigInput {
                    auth_type: LambdaFunctionUrlAuthType::None,
                    invoke_mode: LambdaFunctionUrlInvokeMode::Buffered,
                },
            )
            .unwrap();
        lambda
            .add_permission(
                &alternate_scope,
                "alternate-demo",
                None,
                AddPermissionInput {
                    action: "lambda:InvokeFunctionUrl".to_owned(),
                    function_url_auth_type: Some(
                        LambdaFunctionUrlAuthType::None,
                    ),
                    principal: "*".to_owned(),
                    revision_id: None,
                    source_arn: None,
                    statement_id: "allow-public".to_owned(),
                },
            )
            .unwrap();

        let target = lambda
            .resolve_function_url(
                alternate_scope.region(),
                url_config.url_id(),
            )
            .unwrap();
        assert_eq!(target.scope(), &alternate_scope);
        assert_eq!(target.function_name(), "alternate-demo");
        assert_eq!(target.qualifier(), None);

        let response = lambda
            .invoke_resolved_function_url(
                &target,
                FunctionUrlInvocationInput {
                    body: br#"{"hello":"world"}"#.to_vec(),
                    domain_name: "localhost:4566".to_owned(),
                    headers: Vec::new(),
                    method: "POST".to_owned(),
                    path: "/custom/path".to_owned(),
                    protocol: None,
                    query_string: Some("mode=test".to_owned()),
                    source_ip: None,
                },
                None,
            )
            .unwrap();

        assert_eq!(response.status_code(), 200);
        assert_eq!(response.body(), b"ok");
    }

    #[test]
    fn lambda_url_async_destinations_and_dead_letters_use_explicit_sqs_targets()
     {
        let executor = Arc::new(RecordingExecutor::default());
        executor.set_next_result(LambdaInvocationResult::new(
            br#"{"errorMessage":"boom"}"#.to_vec(),
            Some("Unhandled"),
        ));
        let (lambda, sqs) = service_with_sqs(
            executor.clone(),
            Arc::new(MemoryBlobStore::default()),
        );
        let failure_queue = create_queue(&sqs, "lambda-failure");
        let dead_letter_queue = create_queue(&sqs, "lambda-dlq");
        let failure_queue_arn = queue_arn(&sqs, &failure_queue);
        let dead_letter_queue_arn = queue_arn(&sqs, &dead_letter_queue);

        lambda
            .create_function(
                &scope(),
                CreateFunctionInput {
                    code: inline_zip(),
                    dead_letter_target_arn: Some(
                        dead_letter_queue_arn.clone(),
                    ),
                    description: None,
                    environment: BTreeMap::new(),
                    function_name: "async-demo".to_owned(),
                    handler: Some("bootstrap.handler".to_owned()),
                    memory_size: None,
                    package_type: LambdaPackageType::Zip,
                    publish: false,
                    role: role_arn().to_owned(),
                    runtime: Some("provided.al2".to_owned()),
                    timeout: None,
                },
            )
            .unwrap();
        lambda
            .put_function_event_invoke_config(
                &scope(),
                "async-demo",
                None,
                PutFunctionEventInvokeConfigInput {
                    destination_config: Some(DestinationConfigInput {
                        on_failure: Some(DestinationTargetInput {
                            destination: failure_queue_arn.clone(),
                        }),
                        on_success: None,
                    }),
                    maximum_event_age_in_seconds: None,
                    maximum_retry_attempts: Some(0),
                },
            )
            .unwrap();
        lambda
            .invoke(
                &scope(),
                "async-demo",
                None,
                InvokeInput {
                    invocation_type: LambdaInvocationType::Event,
                    payload: br#"{"job":"process"}"#.to_vec(),
                },
            )
            .unwrap();

        lambda.run_background_cycle().unwrap();

        let failure_messages = sqs
            .receive_message(
                &failure_queue,
                ReceiveMessageInput {
                    max_number_of_messages: Some(1),
                    visibility_timeout: Some(0),
                    wait_time_seconds: Some(0),
                },
            )
            .unwrap();
        assert_eq!(failure_messages.len(), 1);
        let failure_event: serde_json::Value =
            serde_json::from_str(&failure_messages[0].body).unwrap();
        assert_eq!(
            failure_event["requestContext"]["condition"],
            "RetriesExhausted"
        );
        assert_eq!(
            failure_event["requestPayload"],
            json!({ "job": "process" })
        );
        assert_eq!(
            failure_event["responseContext"]["functionError"],
            "Unhandled"
        );

        let dead_letter_messages = sqs
            .receive_message(
                &dead_letter_queue,
                ReceiveMessageInput {
                    max_number_of_messages: Some(1),
                    visibility_timeout: Some(0),
                    wait_time_seconds: Some(0),
                },
            )
            .unwrap();
        assert_eq!(dead_letter_messages.len(), 1);
        assert_eq!(dead_letter_messages[0].body, r#"{"job":"process"}"#);

        let unsupported_destination = lambda.put_function_event_invoke_config(
            &scope(),
            "async-demo",
            None,
            PutFunctionEventInvokeConfigInput {
                destination_config: Some(DestinationConfigInput {
                    on_failure: Some(DestinationTargetInput {
                        destination:
                            "arn:aws:sns:eu-west-2:000000000000:topic"
                                .to_owned(),
                    }),
                    on_success: None,
                }),
                maximum_event_age_in_seconds: None,
                maximum_retry_attempts: Some(0),
            },
        );
        assert!(matches!(
            unsupported_destination,
            Err(LambdaError::UnsupportedOperation { .. })
        ));
    }

    #[test]
    fn lambda_url_sqs_event_source_mapping_invokes_functions_and_rejects_invalid_batching()
     {
        let executor = Arc::new(RecordingExecutor::default());
        executor.set_next_result(LambdaInvocationResult::new(
            br#"{"ok":true}"#.to_vec(),
            Option::<String>::None,
        ));
        let (lambda, sqs) = service_with_sqs(
            executor.clone(),
            Arc::new(MemoryBlobStore::default()),
        );
        let source_queue = create_queue(&sqs, "lambda-source");
        let source_queue_arn = queue_arn(&sqs, &source_queue);

        lambda
            .create_function(
                &scope(),
                CreateFunctionInput {
                    code: inline_zip(),
                    dead_letter_target_arn: None,
                    description: None,
                    environment: BTreeMap::new(),
                    function_name: "mapping-demo".to_owned(),
                    handler: Some("bootstrap.handler".to_owned()),
                    memory_size: None,
                    package_type: LambdaPackageType::Zip,
                    publish: false,
                    role: role_arn().to_owned(),
                    runtime: Some("provided.al2".to_owned()),
                    timeout: None,
                },
            )
            .unwrap();

        let mapping = lambda
            .create_event_source_mapping(
                &scope(),
                CreateEventSourceMappingInput {
                    batch_size: Some(5),
                    enabled: Some(true),
                    event_source_arn: source_queue_arn.clone(),
                    function_name: "mapping-demo".to_owned(),
                    maximum_batching_window_in_seconds: Some(0),
                },
            )
            .unwrap();
        assert_eq!(mapping.state, "Creating");
        assert_eq!(
            lambda
                .list_event_source_mappings(&scope(), None, None, None, None)
                .unwrap()
                .event_source_mappings
                .len(),
            1
        );

        sqs.send_message(
            &source_queue,
            SendMessageInput {
                body: r#"{"job":"run"}"#.to_owned(),
                delay_seconds: None,
                message_deduplication_id: None,
                message_group_id: None,
            },
        )
        .unwrap();
        lambda.run_background_cycle().unwrap();

        let recorded = recover(executor.sync_invocations.lock());
        let event: serde_json::Value =
            serde_json::from_slice(recorded.last().unwrap().payload())
                .unwrap();
        assert_eq!(event["Records"][0]["eventSource"], "aws:sqs");
        assert_eq!(event["Records"][0]["body"], r#"{"job":"run"}"#);
        assert!(
            sqs.receive_message(
                &source_queue,
                ReceiveMessageInput {
                    max_number_of_messages: Some(1),
                    visibility_timeout: Some(0),
                    wait_time_seconds: Some(0),
                },
            )
            .unwrap()
            .is_empty()
        );

        let updated = lambda
            .update_event_source_mapping(
                &scope(),
                &mapping.uuid,
                UpdateEventSourceMappingInput {
                    batch_size: Some(20),
                    enabled: Some(false),
                    maximum_batching_window_in_seconds: Some(1),
                },
            )
            .unwrap();
        assert_eq!(updated.state, "Disabled");
        assert_eq!(updated.batch_size, 20);

        let deleted = lambda
            .delete_event_source_mapping(&scope(), &mapping.uuid)
            .unwrap();
        assert_eq!(deleted.state, "Deleting");

        let invalid_mapping = lambda.create_event_source_mapping(
            &scope(),
            CreateEventSourceMappingInput {
                batch_size: Some(11),
                enabled: Some(true),
                event_source_arn: source_queue_arn,
                function_name: "mapping-demo".to_owned(),
                maximum_batching_window_in_seconds: Some(0),
            },
        );
        assert!(matches!(
            invalid_mapping,
            Err(LambdaError::InvalidParameterValue { .. })
        ));
    }
}
