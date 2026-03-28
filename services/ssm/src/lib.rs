mod errors;
mod filters;
mod history;
mod labels;
mod parameters;
mod scope;

use self::filters::{
    find_label_filtered_version, merge_describe_name_filters,
    reject_pagination, select_parameter_version,
};
use self::history::{
    current_epoch_millis, current_version, next_parameter_version,
    upsert_parameter,
};
use self::labels::apply_labels_to_history;
use self::parameters::{
    SsmParameterStorageKey, StoredParameter, StoredParameterVersion,
    build_history_entry, build_metadata, build_parameter, scope_filter,
    selector_string, storage_key,
};
use aws::Clock;
use std::sync::Arc;
#[cfg(test)]
use storage::StorageError;
use storage::{StorageFactory, StorageHandle};

pub use self::errors::SsmError;
pub use self::parameters::{
    ParameterName, ParameterPath, ParameterReference,
    SsmAddTagsToResourceInput, SsmAddTagsToResourceOutput,
    SsmDeleteParameterInput, SsmDeleteParameterOutput,
    SsmDeleteParametersInput, SsmDeleteParametersOutput, SsmDescribeFilter,
    SsmDescribeParametersInput, SsmDescribeParametersOutput,
    SsmGetParameterHistoryInput, SsmGetParameterHistoryOutput,
    SsmGetParameterInput, SsmGetParametersByPathInput,
    SsmGetParametersByPathOutput, SsmGetParametersInput,
    SsmGetParametersOutput, SsmLabelParameterVersionInput,
    SsmLabelParameterVersionOutput, SsmListTagsForResourceInput,
    SsmListTagsForResourceOutput, SsmParameter, SsmParameterHistoryEntry,
    SsmParameterMetadata, SsmParameterType, SsmPutParameterInput,
    SsmPutParameterOutput, SsmRemoveTagsFromResourceInput,
    SsmRemoveTagsFromResourceOutput, SsmResourceType, SsmTag,
};
pub use self::scope::SsmScope;

#[cfg(test)]
pub(crate) use self::labels::is_valid_label;
#[cfg(test)]
pub(crate) use self::parameters::parameter_arn;

const DEFAULT_DATA_TYPE: &str = "text";
const DEFAULT_MAX_PARAMETER_HISTORY: usize = 100;

#[derive(Clone)]
pub struct SsmService {
    clock: Arc<dyn Clock>,
    max_history: usize,
    parameter_store: StorageHandle<SsmParameterStorageKey, StoredParameter>,
}

impl SsmService {
    pub fn new(factory: &StorageFactory, clock: Arc<dyn Clock>) -> Self {
        Self::with_max_history(factory, clock, DEFAULT_MAX_PARAMETER_HISTORY)
    }

    pub fn with_max_history(
        factory: &StorageFactory,
        clock: Arc<dyn Clock>,
        max_history: usize,
    ) -> Self {
        Self {
            clock,
            max_history: max_history.max(1),
            parameter_store: factory.create("ssm", "parameters"),
        }
    }

    /// # Errors
    ///
    /// Returns validation, conflict, or storage errors when the request is
    /// invalid, overwrite is disabled for an existing parameter, or the state
    /// update fails.
    pub fn put_parameter(
        &self,
        scope: &SsmScope,
        input: SsmPutParameterInput,
    ) -> Result<SsmPutParameterOutput, SsmError> {
        let key = storage_key(scope, &input.name);
        let existing = self.parameter_store.get(&key);
        if existing.is_some() && !input.overwrite.unwrap_or(false) {
            return Err(SsmError::ParameterAlreadyExists);
        }
        let parameter_type =
            input.parameter_type.unwrap_or(SsmParameterType::String);

        let stored_version = StoredParameterVersion {
            data_type: DEFAULT_DATA_TYPE.to_owned(),
            description: input.description,
            labels: Vec::new(),
            last_modified_epoch_millis: current_epoch_millis(&*self.clock),
            parameter_type,
            value: input.value,
            version: next_parameter_version(existing.as_ref())?,
        };
        let stored = upsert_parameter(
            existing,
            input.name,
            stored_version,
            self.max_history,
        );
        let version = current_version(&stored)?.version;

        self.parameter_store.put(key, stored)?;

        Ok(SsmPutParameterOutput { version })
    }

    /// # Errors
    ///
    /// Returns not-found, selector, or storage-backed state errors when the
    /// referenced parameter or version does not exist.
    pub fn get_parameter(
        &self,
        scope: &SsmScope,
        input: SsmGetParameterInput,
    ) -> Result<SsmParameter, SsmError> {
        let stored = self.load_parameter(scope, input.reference.name())?;
        let version =
            select_parameter_version(&stored, input.reference.selector())?;

        Ok(build_parameter(
            scope,
            &stored.name,
            version,
            selector_string(input.reference.selector()),
        ))
    }

    /// # Errors
    ///
    /// Returns any unexpected state error encountered while loading individual
    /// parameters. Invalid names are reported in the output instead of failing
    /// the operation.
    pub fn get_parameters(
        &self,
        scope: &SsmScope,
        input: SsmGetParametersInput,
    ) -> Result<SsmGetParametersOutput, SsmError> {
        let mut parameters = Vec::new();
        let mut invalid_parameters = Vec::new();

        for name in input.names {
            let reference = match ParameterReference::parse(&name) {
                Ok(reference) => reference,
                Err(_) => {
                    invalid_parameters.push(name);
                    continue;
                }
            };

            match self.get_parameter(scope, SsmGetParameterInput { reference })
            {
                Ok(parameter) => parameters.push(parameter),
                Err(_) => invalid_parameters.push(name),
            }
        }

        parameters.sort_by(|left, right| {
            left.name
                .cmp(&right.name)
                .then_with(|| left.version.cmp(&right.version))
        });
        invalid_parameters.sort();

        Ok(SsmGetParametersOutput { invalid_parameters, parameters })
    }

    /// # Errors
    ///
    /// Returns pagination gap errors, storage failures, or unexpected state
    /// errors when a stored parameter is missing its current version.
    pub fn get_parameters_by_path(
        &self,
        scope: &SsmScope,
        input: SsmGetParametersByPathInput,
    ) -> Result<SsmGetParametersByPathOutput, SsmError> {
        reject_pagination(
            "GetParametersByPath",
            input.max_results,
            input.next_token.as_deref(),
        )?;

        let mut parameters = self
            .parameter_store
            .scan(&scope_filter(scope))
            .into_iter()
            .filter(|stored| input.path.matches(&stored.name, input.recursive))
            .map(|stored| {
                let version = match input.label_filter.as_ref() {
                    Some(labels) => {
                        find_label_filtered_version(&stored, labels)
                    }
                    None => Some(current_version(&stored)?),
                };

                Ok(version.map(|selected| {
                    build_parameter(scope, &stored.name, selected, None)
                }))
            })
            .collect::<Result<Vec<_>, SsmError>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();

        parameters.sort_by(|left, right| left.name.cmp(&right.name));

        Ok(SsmGetParametersByPathOutput { parameters })
    }

    /// # Errors
    ///
    /// Returns not-found or storage errors when the target parameter does not
    /// exist or deletion fails.
    pub fn delete_parameter(
        &self,
        scope: &SsmScope,
        input: SsmDeleteParameterInput,
    ) -> Result<SsmDeleteParameterOutput, SsmError> {
        let key = storage_key(scope, &input.name);
        if self.parameter_store.get(&key).is_none() {
            return Err(SsmError::ParameterNotFound);
        }

        self.parameter_store.delete(&key)?;

        Ok(SsmDeleteParameterOutput {})
    }

    /// # Errors
    ///
    /// Returns storage errors if any existing parameter delete fails. Invalid
    /// or missing names are reported in the output instead of failing the
    /// operation.
    pub fn delete_parameters(
        &self,
        scope: &SsmScope,
        input: SsmDeleteParametersInput,
    ) -> Result<SsmDeleteParametersOutput, SsmError> {
        let mut deleted_parameters = Vec::new();
        let mut invalid_parameters = Vec::new();

        for name in input.names {
            match ParameterName::parse(&name) {
                Ok(valid_name) => {
                    let key = storage_key(scope, &valid_name);
                    if self.parameter_store.get(&key).is_some() {
                        self.parameter_store.delete(&key)?;
                        deleted_parameters.push(valid_name.to_string());
                    } else {
                        invalid_parameters.push(valid_name.to_string());
                    }
                }
                Err(_) => invalid_parameters.push(name),
            }
        }

        deleted_parameters.sort();
        invalid_parameters.sort();

        Ok(SsmDeleteParametersOutput {
            deleted_parameters,
            invalid_parameters,
        })
    }

    /// # Errors
    ///
    /// Returns not-found, pagination, or storage-backed state errors when the
    /// parameter does not exist or stored state is invalid.
    pub fn get_parameter_history(
        &self,
        scope: &SsmScope,
        input: SsmGetParameterHistoryInput,
    ) -> Result<SsmGetParameterHistoryOutput, SsmError> {
        reject_pagination(
            "GetParameterHistory",
            input.max_results,
            input.next_token.as_deref(),
        )?;

        let stored = self.load_parameter(scope, &input.name)?;
        let parameters = stored
            .history
            .iter()
            .map(|version| build_history_entry(&stored.name, version))
            .collect();

        Ok(SsmGetParameterHistoryOutput { parameters })
    }

    /// # Errors
    ///
    /// Returns pagination, storage, or unexpected state errors when the
    /// parameter metadata cannot be loaded consistently.
    pub fn describe_parameters(
        &self,
        scope: &SsmScope,
        input: SsmDescribeParametersInput,
    ) -> Result<SsmDescribeParametersOutput, SsmError> {
        reject_pagination(
            "DescribeParameters",
            input.max_results,
            input.next_token.as_deref(),
        )?;

        let name_filter = merge_describe_name_filters(&input.filters);
        let mut parameters = self
            .parameter_store
            .scan(&scope_filter(scope))
            .into_iter()
            .filter(|stored| {
                name_filter
                    .as_ref()
                    .map(|names| names.iter().any(|name| name == &stored.name))
                    .unwrap_or(true)
            })
            .map(|stored| {
                current_version(&stored).map(|version| {
                    build_metadata(scope, &stored.name, version)
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        parameters.sort_by(|left, right| left.name.cmp(&right.name));

        Ok(SsmDescribeParametersOutput { parameters })
    }

    /// # Errors
    ///
    /// Returns not-found, version, label-limit, or storage errors when the
    /// requested version cannot be updated.
    pub fn label_parameter_version(
        &self,
        scope: &SsmScope,
        input: SsmLabelParameterVersionInput,
    ) -> Result<SsmLabelParameterVersionOutput, SsmError> {
        let key = storage_key(scope, &input.name);
        let mut stored = self
            .parameter_store
            .get(&key)
            .ok_or(SsmError::ParameterNotFound)?;
        let parameter_version =
            input.parameter_version.map(Ok).unwrap_or_else(|| {
                current_version(&stored).map(|version| version.version)
            })?;
        let result = apply_labels_to_history(
            &stored.history,
            parameter_version,
            &input.labels,
        )?;
        stored.history = result.history;

        self.parameter_store.put(key, stored)?;

        Ok(SsmLabelParameterVersionOutput {
            invalid_labels: result.invalid_labels,
            parameter_version,
        })
    }

    /// # Errors
    ///
    /// Returns invalid-resource or storage errors when the target parameter
    /// does not exist or tag persistence fails.
    pub fn add_tags_to_resource(
        &self,
        scope: &SsmScope,
        input: SsmAddTagsToResourceInput,
    ) -> Result<SsmAddTagsToResourceOutput, SsmError> {
        let key = storage_key(scope, &input.resource_id);
        let mut stored = self
            .parameter_store
            .get(&key)
            .ok_or(SsmError::InvalidResourceId)?;

        for tag in input.tags {
            stored.tags.insert(tag.key, tag.value);
        }

        self.parameter_store.put(key, stored)?;

        Ok(SsmAddTagsToResourceOutput {})
    }

    /// # Errors
    ///
    /// Returns [`SsmError::InvalidResourceId`] when the target parameter does
    /// not exist.
    pub fn list_tags_for_resource(
        &self,
        scope: &SsmScope,
        input: SsmListTagsForResourceInput,
    ) -> Result<SsmListTagsForResourceOutput, SsmError> {
        let stored = self
            .parameter_store
            .get(&storage_key(scope, &input.resource_id))
            .ok_or(SsmError::InvalidResourceId)?;
        let tag_list = stored
            .tags
            .into_iter()
            .map(|(key, value)| SsmTag { key, value })
            .collect();

        Ok(SsmListTagsForResourceOutput { tag_list })
    }

    /// # Errors
    ///
    /// Returns invalid-resource or storage errors when the target parameter
    /// does not exist or tag persistence fails.
    pub fn remove_tags_from_resource(
        &self,
        scope: &SsmScope,
        input: SsmRemoveTagsFromResourceInput,
    ) -> Result<SsmRemoveTagsFromResourceOutput, SsmError> {
        let key = storage_key(scope, &input.resource_id);
        let mut stored = self
            .parameter_store
            .get(&key)
            .ok_or(SsmError::InvalidResourceId)?;

        for tag_key in input.tag_keys {
            stored.tags.remove(&tag_key);
        }

        self.parameter_store.put(key, stored)?;

        Ok(SsmRemoveTagsFromResourceOutput {})
    }

    fn load_parameter(
        &self,
        scope: &SsmScope,
        name: &ParameterName,
    ) -> Result<StoredParameter, SsmError> {
        self.parameter_store
            .get(&storage_key(scope, name))
            .ok_or(SsmError::ParameterNotFound)
    }
}

#[cfg(test)]
fn validate_parameter_name(name: &str) -> Result<ParameterName, SsmError> {
    ParameterName::parse(name)
}

#[cfg(test)]
fn normalize_parameter_path(path: &str) -> Result<ParameterPath, SsmError> {
    ParameterPath::parse(path)
}

#[cfg(test)]
fn parameter_matches_path(name: &str, path: &str, recursive: bool) -> bool {
    let Ok(name) = ParameterName::parse(name) else {
        return false;
    };
    let Ok(path) = ParameterPath::parse(path) else {
        return false;
    };

    path.matches(&name, recursive)
}

#[cfg(test)]
fn parse_parameter_reference(
    value: &str,
) -> Result<ParameterReference, SsmError> {
    ParameterReference::parse(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::sync::Mutex;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};
    use storage::{StorageConfig, StorageFactory, StorageMode};

    struct SequenceClock {
        times: Mutex<Vec<SystemTime>>,
    }

    impl SequenceClock {
        fn new(times: Vec<SystemTime>) -> Self {
            Self { times: Mutex::new(times) }
        }
    }

    impl Clock for SequenceClock {
        fn now(&self) -> SystemTime {
            let mut times =
                self.times.lock().unwrap_or_else(|poison| poison.into_inner());
            if times.is_empty() {
                return UNIX_EPOCH;
            }
            if times.len() == 1 {
                return times[0];
            }

            times.remove(0)
        }
    }

    fn service_with_max_history(
        label: &str,
        max_history: usize,
        clock: Arc<dyn Clock>,
    ) -> SsmService {
        let factory = StorageFactory::new(StorageConfig::new(
            format!("/tmp/{label}"),
            StorageMode::Memory,
        ));

        SsmService::with_max_history(&factory, clock, max_history)
    }

    fn scope(region: &str) -> SsmScope {
        SsmScope::new(
            "000000000000".parse().expect("account should parse"),
            region.parse().expect("region should parse"),
        )
    }

    fn parameter_name(name: &str) -> ParameterName {
        ParameterName::parse(name).expect("parameter name should parse")
    }

    fn parameter_path(path: &str) -> ParameterPath {
        ParameterPath::parse(path).expect("parameter path should parse")
    }

    fn parameter_reference(value: &str) -> ParameterReference {
        ParameterReference::parse(value)
            .expect("parameter reference should parse")
    }

    fn resource_type_parameter() -> SsmResourceType {
        SsmResourceType::Parameter
    }

    fn put_input(
        name: &str,
        value: &str,
        overwrite: bool,
    ) -> SsmPutParameterInput {
        SsmPutParameterInput {
            description: Some(format!("description-{value}")),
            name: parameter_name(name),
            overwrite: Some(overwrite),
            parameter_type: Some(SsmParameterType::String),
            value: value.to_owned(),
        }
    }

    #[test]
    fn ssm_put_parameter_overwrite_and_history_trim_are_deterministic() {
        let service = service_with_max_history(
            "services-ssm-history",
            2,
            Arc::new(SequenceClock::new(vec![
                UNIX_EPOCH + Duration::from_secs(1),
                UNIX_EPOCH + Duration::from_secs(2),
                UNIX_EPOCH + Duration::from_secs(3),
            ])),
        );
        let scope = scope("eu-west-2");

        let created = service
            .put_parameter(&scope, put_input("/app/db/host", "one", false))
            .expect("parameter should create");
        let duplicate = service
            .put_parameter(&scope, put_input("/app/db/host", "one", false))
            .expect_err("duplicate put should fail");
        let updated = service
            .put_parameter(&scope, put_input("/app/db/host", "two", true))
            .expect("parameter should update");
        let updated_again = service
            .put_parameter(&scope, put_input("/app/db/host", "three", true))
            .expect("parameter should update again");
        let current = service
            .get_parameter(
                &scope,
                SsmGetParameterInput {
                    reference: parameter_reference("/app/db/host"),
                },
            )
            .expect("current parameter should load");
        let history = service
            .get_parameter_history(
                &scope,
                SsmGetParameterHistoryInput {
                    max_results: None,
                    name: parameter_name("/app/db/host"),
                    next_token: None,
                },
            )
            .expect("history should load");

        assert_eq!(created.version, 1);
        assert_eq!(duplicate.to_aws_error().code(), "ParameterAlreadyExists");
        assert_eq!(updated.version, 2);
        assert_eq!(updated_again.version, 3);
        assert_eq!(current.value, "three");
        assert_eq!(current.version, 3);
        assert_eq!(
            history
                .parameters
                .iter()
                .map(|entry| entry.version)
                .collect::<Vec<_>>(),
            vec![2, 3]
        );
        assert_eq!(
            history
                .parameters
                .iter()
                .map(|entry| entry.last_modified_date)
                .collect::<Vec<_>>(),
            vec![2_000, 3_000]
        );
    }

    #[test]
    fn ssm_get_parameters_sort_results_and_report_invalid_references() {
        let service = service_with_max_history(
            "services-ssm-get-parameters",
            5,
            Arc::new(SequenceClock::new(vec![
                UNIX_EPOCH + Duration::from_secs(1),
                UNIX_EPOCH + Duration::from_secs(2),
            ])),
        );
        let scope = scope("eu-west-2");

        service
            .put_parameter(&scope, put_input("/b", "beta", false))
            .expect("parameter should create");
        service
            .put_parameter(&scope, put_input("/a", "alpha", false))
            .expect("parameter should create");

        let output = service
            .get_parameters(
                &scope,
                SsmGetParametersInput {
                    names: vec![
                        "/b".to_owned(),
                        "/missing".to_owned(),
                        "/a:99".to_owned(),
                        "/a".to_owned(),
                    ],
                },
            )
            .expect("batch get should succeed");

        assert_eq!(
            output
                .parameters
                .iter()
                .map(|parameter| parameter.name.clone())
                .collect::<Vec<_>>(),
            vec!["/a".to_owned(), "/b".to_owned()]
        );
        assert_eq!(
            output.invalid_parameters,
            vec!["/a:99".to_owned(), "/missing".to_owned()]
        );
    }

    #[test]
    fn ssm_path_filters_respect_recursive_and_label_matching() {
        let service = service_with_max_history(
            "services-ssm-path",
            5,
            Arc::new(SequenceClock::new(vec![
                UNIX_EPOCH + Duration::from_secs(1),
                UNIX_EPOCH + Duration::from_secs(2),
                UNIX_EPOCH + Duration::from_secs(3),
                UNIX_EPOCH + Duration::from_secs(4),
            ])),
        );
        let scope = scope("eu-west-2");

        service
            .put_parameter(&scope, put_input("/app/db/host", "old", false))
            .expect("parameter should create");
        service
            .put_parameter(&scope, put_input("/app/db/host", "new", true))
            .expect("parameter should overwrite");
        service
            .label_parameter_version(
                &scope,
                SsmLabelParameterVersionInput {
                    labels: vec!["stable".to_owned()],
                    name: parameter_name("/app/db/host"),
                    parameter_version: Some(1),
                },
            )
            .expect("label should attach");
        service
            .put_parameter(
                &scope,
                put_input("/app/db/readonly/user", "ro", false),
            )
            .expect("nested parameter should create");

        let non_recursive = service
            .get_parameters_by_path(
                &scope,
                SsmGetParametersByPathInput {
                    label_filter: None,
                    max_results: None,
                    next_token: None,
                    path: parameter_path("/app/db"),
                    recursive: false,
                },
            )
            .expect("path query should succeed");
        let recursive = service
            .get_parameters_by_path(
                &scope,
                SsmGetParametersByPathInput {
                    label_filter: None,
                    max_results: None,
                    next_token: None,
                    path: parameter_path("/app/db"),
                    recursive: true,
                },
            )
            .expect("recursive path query should succeed");
        let by_label = service
            .get_parameters_by_path(
                &scope,
                SsmGetParametersByPathInput {
                    label_filter: Some(vec!["stable".to_owned()]),
                    max_results: None,
                    next_token: None,
                    path: parameter_path("/app"),
                    recursive: true,
                },
            )
            .expect("label path query should succeed");

        assert_eq!(
            non_recursive
                .parameters
                .iter()
                .map(|parameter| parameter.name.clone())
                .collect::<Vec<_>>(),
            vec!["/app/db/host".to_owned()]
        );
        assert_eq!(
            recursive
                .parameters
                .iter()
                .map(|parameter| parameter.name.clone())
                .collect::<Vec<_>>(),
            vec![
                "/app/db/host".to_owned(),
                "/app/db/readonly/user".to_owned()
            ]
        );
        assert_eq!(by_label.parameters.len(), 1);
        assert_eq!(by_label.parameters[0].name, "/app/db/host");
        assert_eq!(by_label.parameters[0].value, "old");
        assert_eq!(by_label.parameters[0].version, 1);
    }

    #[test]
    fn ssm_reference_selectors_support_version_and_label_reads() {
        let service = service_with_max_history(
            "services-ssm-selectors",
            5,
            Arc::new(SequenceClock::new(vec![
                UNIX_EPOCH + Duration::from_secs(1),
                UNIX_EPOCH + Duration::from_secs(2),
            ])),
        );
        let scope = scope("eu-west-2");

        service
            .put_parameter(&scope, put_input("/config/api", "v1", false))
            .expect("parameter should create");
        service
            .put_parameter(&scope, put_input("/config/api", "v2", true))
            .expect("parameter should overwrite");
        service
            .label_parameter_version(
                &scope,
                SsmLabelParameterVersionInput {
                    labels: vec!["live".to_owned()],
                    name: parameter_name("/config/api"),
                    parameter_version: Some(1),
                },
            )
            .expect("label should attach");

        let by_version = service
            .get_parameter(
                &scope,
                SsmGetParameterInput {
                    reference: parameter_reference("/config/api:1"),
                },
            )
            .expect("version selector should resolve");
        let by_label = service
            .get_parameter(
                &scope,
                SsmGetParameterInput {
                    reference: parameter_reference("/config/api:live"),
                },
            )
            .expect("label selector should resolve");
        let missing = service
            .get_parameter(
                &scope,
                SsmGetParameterInput {
                    reference: parameter_reference("/config/api:missing"),
                },
            )
            .expect_err("missing label should fail");

        assert_eq!(by_version.value, "v1");
        assert_eq!(by_version.selector.as_deref(), Some(":1"));
        assert_eq!(by_label.value, "v1");
        assert_eq!(by_label.selector.as_deref(), Some(":live"));
        assert_eq!(missing.to_aws_error().code(), "ParameterVersionNotFound");
    }

    #[test]
    fn ssm_label_operations_move_labels_validate_names_and_enforce_limits() {
        let service = service_with_max_history(
            "services-ssm-labels",
            5,
            Arc::new(SequenceClock::new(vec![
                UNIX_EPOCH + Duration::from_secs(1),
                UNIX_EPOCH + Duration::from_secs(2),
            ])),
        );
        let scope = scope("eu-west-2");

        service
            .put_parameter(&scope, put_input("/labels/demo", "v1", false))
            .expect("parameter should create");
        service
            .put_parameter(&scope, put_input("/labels/demo", "v2", true))
            .expect("parameter should overwrite");
        service
            .label_parameter_version(
                &scope,
                SsmLabelParameterVersionInput {
                    labels: vec!["prod".to_owned()],
                    name: parameter_name("/labels/demo"),
                    parameter_version: Some(1),
                },
            )
            .expect("label should attach");
        let moved = service
            .label_parameter_version(
                &scope,
                SsmLabelParameterVersionInput {
                    labels: vec![
                        "prod".to_owned(),
                        "1bad".to_owned(),
                        "blue".to_owned(),
                    ],
                    name: parameter_name("/labels/demo"),
                    parameter_version: Some(2),
                },
            )
            .expect("labels should move");
        let history = service
            .get_parameter_history(
                &scope,
                SsmGetParameterHistoryInput {
                    max_results: None,
                    name: parameter_name("/labels/demo"),
                    next_token: None,
                },
            )
            .expect("history should load");
        let limit_error = service
            .label_parameter_version(
                &scope,
                SsmLabelParameterVersionInput {
                    labels: vec![
                        "a".to_owned(),
                        "b".to_owned(),
                        "c".to_owned(),
                        "d".to_owned(),
                        "e".to_owned(),
                        "f".to_owned(),
                        "g".to_owned(),
                        "h".to_owned(),
                        "i".to_owned(),
                        "j".to_owned(),
                        "k".to_owned(),
                    ],
                    name: parameter_name("/labels/demo"),
                    parameter_version: Some(2),
                },
            )
            .expect_err("too many labels should fail");
        let missing = service
            .label_parameter_version(
                &scope,
                SsmLabelParameterVersionInput {
                    labels: vec!["ghost".to_owned()],
                    name: parameter_name("/labels/demo"),
                    parameter_version: Some(99),
                },
            )
            .expect_err("missing version should fail");

        assert_eq!(moved.invalid_labels, vec!["1bad".to_owned()]);
        assert_eq!(history.parameters[0].labels, Vec::<String>::new());
        assert_eq!(
            history.parameters[1].labels,
            vec!["prod".to_owned(), "blue".to_owned()]
        );
        assert_eq!(
            limit_error.to_aws_error().code(),
            "ParameterVersionLabelLimitExceeded"
        );
        assert_eq!(missing.to_aws_error().code(), "ParameterVersionNotFound");
    }

    #[test]
    fn ssm_describe_tags_delete_and_scope_isolation_behave_as_expected() {
        let service = service_with_max_history(
            "services-ssm-scope",
            5,
            Arc::new(SequenceClock::new(vec![
                UNIX_EPOCH + Duration::from_secs(1),
                UNIX_EPOCH + Duration::from_secs(2),
            ])),
        );
        let west = scope("eu-west-2");
        let east = scope("us-east-1");

        service
            .put_parameter(&west, put_input("/shared/name", "west", false))
            .expect("west parameter should create");
        service
            .put_parameter(&east, put_input("/shared/name", "east", false))
            .expect("east parameter should create");
        service
            .add_tags_to_resource(
                &west,
                SsmAddTagsToResourceInput {
                    resource_id: parameter_name("/shared/name"),
                    resource_type: resource_type_parameter(),
                    tags: vec![
                        SsmTag {
                            key: "env".to_owned(),
                            value: "dev".to_owned(),
                        },
                        SsmTag {
                            key: "team".to_owned(),
                            value: "platform".to_owned(),
                        },
                    ],
                },
            )
            .expect("tags should add");
        let described = service
            .describe_parameters(
                &west,
                SsmDescribeParametersInput {
                    filters: vec![SsmDescribeFilter::NameEquals(vec![
                        parameter_name("/shared/name"),
                    ])],
                    max_results: None,
                    next_token: None,
                },
            )
            .expect("describe should succeed");
        let tags = service
            .list_tags_for_resource(
                &west,
                SsmListTagsForResourceInput {
                    resource_id: parameter_name("/shared/name"),
                    resource_type: resource_type_parameter(),
                },
            )
            .expect("tag listing should succeed");
        service
            .remove_tags_from_resource(
                &west,
                SsmRemoveTagsFromResourceInput {
                    resource_id: parameter_name("/shared/name"),
                    resource_type: resource_type_parameter(),
                    tag_keys: vec!["env".to_owned()],
                },
            )
            .expect("tag removal should succeed");
        let delete_output = service
            .delete_parameters(
                &west,
                SsmDeleteParametersInput {
                    names: vec![
                        "/shared/name".to_owned(),
                        "/missing".to_owned(),
                    ],
                },
            )
            .expect("batch delete should succeed");
        let east_value = service
            .get_parameter(
                &east,
                SsmGetParameterInput {
                    reference: parameter_reference("/shared/name"),
                },
            )
            .expect("east parameter should remain visible");
        let invalid_type = SsmResourceType::parse("PatchBaseline")
            .expect_err("invalid resource type should fail");

        assert_eq!(described.parameters.len(), 1);
        assert_eq!(described.parameters[0].name, "/shared/name");
        assert_eq!(
            tags.tag_list,
            vec![
                SsmTag { key: "env".to_owned(), value: "dev".to_owned() },
                SsmTag {
                    key: "team".to_owned(),
                    value: "platform".to_owned()
                },
            ]
        );
        assert_eq!(
            delete_output.deleted_parameters,
            vec!["/shared/name".to_owned()]
        );
        assert_eq!(
            delete_output.invalid_parameters,
            vec!["/missing".to_owned()]
        );
        assert_eq!(east_value.value, "east");
        assert_eq!(invalid_type.to_aws_error().code(), "InvalidResourceType");
    }

    #[test]
    fn ssm_gap_paths_and_validation_rules_return_explicit_errors() {
        let service = service_with_max_history(
            "services-ssm-gaps",
            5,
            Arc::new(SequenceClock::new(vec![
                UNIX_EPOCH + Duration::from_secs(1),
            ])),
        );
        let scope = scope("eu-west-2");

        service
            .put_parameter(&scope, put_input("/demo", "value", false))
            .expect("parameter should create");

        let pagination_history = service
            .get_parameter_history(
                &scope,
                SsmGetParameterHistoryInput {
                    max_results: Some(1),
                    name: parameter_name("/demo"),
                    next_token: None,
                },
            )
            .expect_err("history pagination should fail");
        let pagination_path = service
            .get_parameters_by_path(
                &scope,
                SsmGetParametersByPathInput {
                    label_filter: None,
                    max_results: None,
                    next_token: Some("next".to_owned()),
                    path: parameter_path("/"),
                    recursive: true,
                },
            )
            .expect_err("path pagination should fail");
        let pagination_describe = service
            .describe_parameters(
                &scope,
                SsmDescribeParametersInput {
                    filters: Vec::new(),
                    max_results: None,
                    next_token: Some("next".to_owned()),
                },
            )
            .expect_err("describe pagination should fail");
        let bad_name = ParameterReference::parse("bad name")
            .expect_err("names with spaces should fail");
        let bad_arn = ParameterReference::parse(
            "arn:aws:ssm:eu-west-2:000000000000:parameter/demo",
        )
        .expect_err("arn reads should be explicit gap paths");

        assert_eq!(
            pagination_history.to_aws_error().code(),
            "ValidationException"
        );
        assert_eq!(
            pagination_path.to_aws_error().code(),
            "ValidationException"
        );
        assert_eq!(
            pagination_describe.to_aws_error().code(),
            "ValidationException"
        );
        assert_eq!(bad_name.to_aws_error().code(), "ValidationException");
        assert_eq!(bad_arn.to_aws_error().code(), "ValidationException");
    }

    #[test]
    fn ssm_parameter_type_parser_and_error_mapping_cover_remaining_variants() {
        assert_eq!(SsmParameterType::String.as_str(), "String");
        assert_eq!(SsmParameterType::StringList.as_str(), "StringList");
        assert_eq!(
            SsmParameterType::parse("StringList").expect("type should parse"),
            SsmParameterType::StringList
        );
        assert_eq!(
            SsmParameterType::parse("SecureString")
                .expect("type should parse")
                .as_str(),
            "SecureString"
        );
        assert_eq!(
            SsmParameterType::parse("Binary")
                .expect_err("unsupported type should fail")
                .to_aws_error()
                .code(),
            "UnsupportedParameterType"
        );
        assert_eq!(
            SsmError::UnsupportedOperation {
                message: "Operation Unknown is not supported.".to_owned(),
            }
            .to_aws_error()
            .code(),
            "UnsupportedOperation"
        );
        assert_eq!(
            SsmError::InternalFailure { message: "boom".to_owned() }
                .to_aws_error()
                .status_code(),
            500
        );
    }

    #[test]
    fn ssm_private_helpers_cover_validation_path_and_conversion_edges() {
        let service = service_with_max_history(
            "services-ssm-helpers",
            5,
            Arc::new(SequenceClock::new(vec![])),
        );
        let scope = scope("eu-west-2");
        let blank_name = validate_parameter_name("  ")
            .expect_err("blank names should fail");
        let blank_selector = parse_parameter_reference("/demo:")
            .expect_err("blank selectors should fail");
        let missing_delete = service
            .delete_parameter(
                &scope,
                SsmDeleteParameterInput { name: parameter_name("/missing") },
            )
            .expect_err("missing delete should fail");
        let batch_delete = service
            .delete_parameters(
                &scope,
                SsmDeleteParametersInput {
                    names: vec!["bad name".to_owned()],
                },
            )
            .expect("batch delete should classify invalid names");
        let storage_error = SsmError::from(StorageError::DecodeSnapshot {
            path: PathBuf::from("/tmp/ssm.json"),
            details: "boom".to_owned(),
        });
        let empty_clock = SequenceClock::new(vec![]);

        assert_eq!(blank_name.to_aws_error().code(), "ValidationException");
        assert_eq!(
            normalize_parameter_path("/")
                .expect("root path should normalize")
                .as_str(),
            "/"
        );
        assert!(parameter_matches_path("/demo", "/", true));
        assert!(parameter_matches_path("/demo", "/", false));
        assert!(!parameter_matches_path("/demo/child", "/", false));
        assert!(!parameter_matches_path("/demo", "/other", true));
        assert_eq!(
            blank_selector.to_aws_error().code(),
            "ValidationException"
        );
        assert_eq!(missing_delete.to_aws_error().code(), "ParameterNotFound");
        assert_eq!(
            batch_delete.invalid_parameters,
            vec!["bad name".to_owned()]
        );
        assert_eq!(storage_error.to_aws_error().code(), "InternalServerError");
        assert_eq!(empty_clock.now(), UNIX_EPOCH);
    }

    #[test]
    fn ssm_helper_paths_cover_constructor_sorting_and_label_edges() {
        let factory = StorageFactory::new(StorageConfig::new(
            "/tmp/services-ssm-helper-coverage",
            StorageMode::Memory,
        ));
        let service = SsmService::new(
            &factory,
            Arc::new(SequenceClock::new(vec![
                UNIX_EPOCH + Duration::from_secs(1),
                UNIX_EPOCH + Duration::from_secs(2),
                UNIX_EPOCH + Duration::from_secs(3),
                UNIX_EPOCH + Duration::from_secs(4),
            ])),
        );
        let scope = scope("eu-west-2");

        service
            .put_parameter(&scope, put_input("/config/api", "v1", false))
            .expect("parameter should create");
        service
            .put_parameter(&scope, put_input("/config/api", "v2", true))
            .expect("parameter should overwrite");
        service
            .put_parameter(&scope, put_input("/config/zeta", "z", false))
            .expect("parameter should create");
        service
            .put_parameter(&scope, put_input("/config/alpha", "a", false))
            .expect("parameter should create");

        let current_label = service
            .label_parameter_version(
                &scope,
                SsmLabelParameterVersionInput {
                    labels: vec!["live".to_owned()],
                    name: parameter_name("/config/api"),
                    parameter_version: None,
                },
            )
            .expect("labels should default to the current version");
        let sorted_parameters = service
            .get_parameters(
                &scope,
                SsmGetParametersInput {
                    names: vec![
                        "/config/api:2".to_owned(),
                        "/config/api:1".to_owned(),
                    ],
                },
            )
            .expect("batch get should sort by name and version");
        let described = service
            .describe_parameters(
                &scope,
                SsmDescribeParametersInput {
                    filters: Vec::new(),
                    max_results: None,
                    next_token: None,
                },
            )
            .expect("describe should sort by name");
        let stored = service
            .load_parameter(&scope, &parameter_name("/config/api"))
            .expect("stored parameter should exist");
        let version_reference = parse_parameter_reference("/config/api:1")
            .expect("version selector should parse");
        let label_reference = parse_parameter_reference("/config/api:live")
            .expect("label selector should parse");
        let label_application = apply_labels_to_history(
            &stored.history,
            1,
            &[
                "live".to_owned(),
                "qa".to_owned(),
                "1bad".to_owned(),
                "qa".to_owned(),
            ],
        )
        .expect("label application should succeed");
        let parameter = build_parameter(
            &scope,
            &parameter_name("/config/api"),
            &StoredParameterVersion {
                data_type: DEFAULT_DATA_TYPE.to_owned(),
                description: None,
                labels: Vec::new(),
                last_modified_epoch_millis: 1_500,
                parameter_type: SsmParameterType::SecureString,
                value: "secret".to_owned(),
                version: 2,
            },
            Some(":live".to_owned()),
        );
        let storage_error = SsmError::from(StorageError::DecodeSnapshot {
            path: PathBuf::from("/tmp/ssm.json"),
            details: "boom".to_owned(),
        });

        assert_eq!(current_label.parameter_version, 2);
        assert_eq!(
            sorted_parameters
                .parameters
                .iter()
                .map(|parameter| parameter.version)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert_eq!(
            described
                .parameters
                .iter()
                .map(|parameter| parameter.name.clone())
                .collect::<Vec<_>>(),
            vec![
                "/config/alpha".to_owned(),
                "/config/api".to_owned(),
                "/config/zeta".to_owned(),
            ]
        );
        assert_eq!(
            select_parameter_version(&stored, version_reference.selector())
                .expect("version selector should resolve")
                .version,
            1
        );
        assert_eq!(
            select_parameter_version(&stored, label_reference.selector())
                .expect("label selector should resolve")
                .version,
            2
        );
        assert_eq!(
            find_label_filtered_version(&stored, &["live".to_owned()])
                .expect("label lookup should find the current version")
                .version,
            2
        );
        assert_eq!(label_application.invalid_labels, vec!["1bad".to_owned()]);
        assert_eq!(
            label_application.history[0].labels,
            vec!["live".to_owned(), "qa".to_owned()]
        );
        assert!(label_application.history[1].labels.is_empty());
        assert_eq!(
            parameter_arn(&scope, &parameter.name),
            "arn:aws:ssm:eu-west-2:000000000000:parameter/config/api"
        );
        assert_eq!(parameter.last_modified_date, 1_500);
        assert_eq!(parameter.parameter_type.as_str(), "SecureString");
        assert_eq!(storage_error.to_aws_error().code(), "InternalServerError");
    }

    #[test]
    fn ssm_label_validation_covers_reserved_prefixes_and_allowed_characters() {
        assert!(!is_valid_label(""));
        assert!(!is_valid_label(&"x".repeat(101)));
        assert!(!is_valid_label("aws-prod"));
        assert!(!is_valid_label("ssm-prod"));
        assert!(is_valid_label("Abc-9_."));
    }
}
