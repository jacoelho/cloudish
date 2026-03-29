use crate::{
    errors::CloudFormationError,
    planning::{
        CloudFormationPlanInput, CloudFormationPlannedOutput,
        CloudFormationPlannedResource, CloudFormationStackPlan,
        ExistingStackResource, ExistingStackSnapshot,
        PlannedResourceDefinition, build_change_summaries,
        ensure_capabilities_satisfied, topological_resource_order,
    },
    providers::{CloudFormationDependencies, CloudFormationIamPort},
    scope::CloudFormationScope,
    stacks::{
        CloudFormationChangeSetDescription, CloudFormationChangeSetSummary,
        CloudFormationChangeSummary, CloudFormationCreateChangeSetInput,
        CloudFormationCreateChangeSetOutput, CloudFormationStackDescription,
        CloudFormationStackEvent, CloudFormationStackOperationInput,
        CloudFormationStackOperationOutput, CloudFormationStackOutput,
        CloudFormationStackResourceDescription,
        CloudFormationStackResourceSummary,
    },
    template::{
        CloudFormationTemplateParameter, ValidateTemplateInput,
        ValidateTemplateOutput,
    },
};
use dynamodb::{
    AttributeDefinition as DynamoDbAttributeDefinition,
    BillingMode as DynamoDbBillingMode,
    CreateTableInput as CreateDynamoDbTableInput, DynamoDbError,
    DynamoDbScope,
    GlobalSecondaryIndexDefinition as DynamoDbGlobalSecondaryIndexDefinition,
    KeySchemaElement as DynamoDbKeySchemaElement, KeyType as DynamoDbKeyType,
    LocalSecondaryIndexDefinition as DynamoDbLocalSecondaryIndexDefinition,
    Projection as DynamoDbProjection,
    ProjectionType as DynamoDbProjectionType,
    ProvisionedThroughput as DynamoDbProvisionedThroughput,
    ResourceTag as DynamoDbResourceTag,
    ScalarAttributeType as DynamoDbScalarAttributeType,
    TableName as DynamoDbTableName,
    TagResourceInput as DynamoDbTagResourceInput,
};
use iam::{
    CreateInstanceProfileInput, CreatePolicyInput, CreatePolicyVersionInput,
    CreateRoleInput, CreateUserInput, IamError, IamScope, IamTag,
    InlinePolicyInput,
};
use kms::{
    CreateKmsAliasInput, CreateKmsKeyInput, DeleteKmsAliasInput, KmsError,
    KmsScope, KmsTag, ScheduleKmsKeyDeletionInput,
};
use lambda::{
    CreateFunctionInput, LambdaCodeInput, LambdaError, LambdaPackageType,
    LambdaScope,
};
use s3::{CreateBucketInput, S3Error, S3Scope};
use secrets_manager::{
    CreateSecretInput, DeleteSecretInput,
    SecretName as SecretsManagerSecretName,
    SecretReference as SecretsManagerSecretReference, SecretsManagerError,
    SecretsManagerScope, SecretsManagerTag, UpdateSecretInput,
};
use sns::{CreateTopicInput, SnsError, SnsScope};
use sqs::{CreateQueueInput, SqsError, SqsQueueIdentity, SqsScope};
use ssm::{
    ParameterName as SsmParameterName, SsmAddTagsToResourceInput,
    SsmDeleteParameterInput, SsmError, SsmParameterType, SsmPutParameterInput,
    SsmResourceType, SsmScope, SsmTag,
};

use aws::{
    AccountId, Arn, IamAccessKeyStatus, KmsAliasName, KmsKeyReference,
    RegionId,
};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use serde::Deserialize;
use serde_json::{Map, Value};
use serde_yaml::Value as YamlValue;
use serde_yaml::value::TaggedValue;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::io::Write as _;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use zip::CompressionMethod;
use zip::ZipWriter;
use zip::write::SimpleFileOptions;

const SUPPORTED_RESOURCE_TYPES: &[&str] = &[
    "AWS::S3::Bucket",
    "AWS::S3::BucketPolicy",
    "AWS::SQS::Queue",
    "AWS::SQS::QueuePolicy",
    "AWS::SNS::Topic",
    "AWS::DynamoDB::Table",
    "AWS::Lambda::Function",
    "AWS::IAM::Role",
    "AWS::IAM::User",
    "AWS::IAM::AccessKey",
    "AWS::IAM::Policy",
    "AWS::IAM::ManagedPolicy",
    "AWS::IAM::InstanceProfile",
    "AWS::SSM::Parameter",
    "AWS::KMS::Key",
    "AWS::KMS::Alias",
    "AWS::SecretsManager::Secret",
];
const IAM_RESOURCE_TYPES: &[&str] = &[
    "AWS::IAM::Role",
    "AWS::IAM::User",
    "AWS::IAM::AccessKey",
    "AWS::IAM::Policy",
    "AWS::IAM::ManagedPolicy",
    "AWS::IAM::InstanceProfile",
];

#[derive(Clone)]
struct CloudFormationProviderEngine {
    dependencies: CloudFormationDependencies,
}

#[derive(Clone)]
pub struct CloudFormationService {
    clock: Arc<dyn Fn() -> SystemTime + Send + Sync>,
    providers: CloudFormationProviderEngine,
    state: Arc<Mutex<CloudFormationState>>,
}

impl std::fmt::Debug for CloudFormationService {
    fn fmt(
        &self,
        formatter: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        formatter
            .debug_struct("CloudFormationService")
            .field("has_s3", &self.providers.dependencies.s3.is_some())
            .field("has_sqs", &self.providers.dependencies.sqs.is_some())
            .field("has_sns", &self.providers.dependencies.sns.is_some())
            .field("has_lambda", &self.providers.dependencies.lambda.is_some())
            .finish_non_exhaustive()
    }
}

impl Default for CloudFormationService {
    fn default() -> Self {
        Self::new()
    }
}

impl CloudFormationProviderEngine {
    fn new(dependencies: CloudFormationDependencies) -> Self {
        Self { dependencies }
    }
}

impl CloudFormationService {
    pub fn new() -> Self {
        Self::with_dependencies(
            Arc::new(SystemTime::now),
            CloudFormationDependencies::default(),
        )
    }

    pub fn with_dependencies(
        clock: Arc<dyn Fn() -> SystemTime + Send + Sync>,
        dependencies: CloudFormationDependencies,
    ) -> Self {
        Self {
            clock,
            providers: CloudFormationProviderEngine::new(dependencies),
            state: Arc::default(),
        }
    }

    #[doc = "# Errors\n\nReturns `CloudFormationError` when the template cannot be parsed, validated, or planned against the current `CloudFormation` dependencies."]
    pub fn validate_template(
        &self,
        _scope: &CloudFormationScope,
        input: ValidateTemplateInput,
    ) -> Result<ValidateTemplateOutput, CloudFormationError> {
        let document = parse_template_document(&input.template_body)?;
        validate_template_document(&document)?;
        let parameters = template_parameters(&document.parameters)?;
        let (capabilities, capabilities_reason) =
            required_capabilities(&document.resources);

        Ok(ValidateTemplateOutput {
            capabilities,
            capabilities_reason,
            declared_transforms: Vec::new(),
            description: document.description,
            parameters,
        })
    }

    #[doc = "# Errors\n\nReturns `CloudFormationError` when the external template source cannot be resolved or the fetched template fails `CloudFormation` validation."]
    pub fn validate_template_source(
        &self,
        scope: &CloudFormationScope,
        template_body: Option<String>,
        template_url: Option<String>,
    ) -> Result<ValidateTemplateOutput, CloudFormationError> {
        let template_body =
            self.template_body(scope, template_body, template_url)?;
        self.validate_template(scope, ValidateTemplateInput { template_body })
    }

    #[doc = "# Errors\n\nReturns `CloudFormationError` when the template, parameters, or calculated resource graph are invalid."]
    pub fn plan_stack(
        &self,
        scope: &CloudFormationScope,
        input: CloudFormationPlanInput,
    ) -> Result<CloudFormationStackPlan, CloudFormationError> {
        if input.stack_name.trim().is_empty() {
            return Err(template_validation_error(
                "stack planning requires a non-empty stack name",
            ));
        }

        let document = parse_template_document(&input.template_body)?;
        validate_template_document(&document)?;
        let parameter_values =
            planned_parameter_values(&document.parameters, &input.parameters)?;
        let stack_id = planned_stack_id(scope, &input.stack_name);
        let base_context = PlanningContext {
            account_id: scope.account_id(),
            region: scope.region(),
            resources: &document.resources,
            stack_id: &stack_id,
            stack_name: &input.stack_name,
            parameters: &parameter_values,
            conditions: None,
        };
        let conditions =
            evaluate_conditions(&document.conditions, &base_context)?;
        let context =
            PlanningContext { conditions: Some(&conditions), ..base_context };

        let mut resources = BTreeMap::new();
        for (logical_id, resource) in &document.resources {
            if !resource_enabled(resource.condition.as_deref(), &conditions)? {
                continue;
            }

            let properties = evaluate_value(
                &resource.properties,
                &context,
                &resource_path(logical_id, "Properties"),
            )?
            .into_json()
            .unwrap_or(Value::Object(Map::new()));

            resources.insert(
                logical_id.clone(),
                CloudFormationPlannedResource {
                    properties,
                    resource_type: resource.resource_type.clone(),
                },
            );
        }

        let mut outputs = BTreeMap::new();
        for (logical_id, output) in &document.outputs {
            if !resource_enabled(output.condition.as_deref(), &conditions)? {
                continue;
            }

            let value = evaluate_value(
                &output.value,
                &context,
                &output_path(logical_id),
            )?
            .into_json()
            .unwrap_or(Value::Null);

            outputs.insert(
                logical_id.clone(),
                CloudFormationPlannedOutput {
                    description: output.description.clone(),
                    value,
                },
            );
        }

        Ok(CloudFormationStackPlan {
            conditions,
            description: document.description,
            outputs,
            resources,
        })
    }

    #[doc = "# Errors\n\nReturns `CloudFormationError` when stack creation fails validation, violates capabilities, or a provider operation cannot be completed."]
    pub fn create_stack(
        &self,
        scope: &CloudFormationScope,
        input: CloudFormationStackOperationInput,
    ) -> Result<CloudFormationStackOperationOutput, CloudFormationError> {
        let prepared = self.prepare_operation(
            scope,
            input.stack_name.clone(),
            input.template_body,
            input.template_url,
            input.parameters,
            input.capabilities,
        )?;
        let stack = self.execute_prepared(scope, prepared, false)?;

        Ok(CloudFormationStackOperationOutput { stack_id: stack.stack_id })
    }

    #[doc = "# Errors\n\nReturns `CloudFormationError` when the update request is invalid or a provider operation fails while applying the change set."]
    pub fn update_stack(
        &self,
        scope: &CloudFormationScope,
        input: CloudFormationStackOperationInput,
    ) -> Result<CloudFormationStackOperationOutput, CloudFormationError> {
        let prepared = self.prepare_operation(
            scope,
            input.stack_name.clone(),
            input.template_body,
            input.template_url,
            input.parameters,
            input.capabilities,
        )?;
        let stack = self.execute_prepared(scope, prepared, true)?;

        Ok(CloudFormationStackOperationOutput { stack_id: stack.stack_id })
    }

    #[doc = "# Errors\n\nReturns `CloudFormationError` when the named stack does not exist or a provider operation fails while deleting resources."]
    pub fn delete_stack(
        &self,
        scope: &CloudFormationScope,
        stack_name: &str,
    ) -> Result<(), CloudFormationError> {
        self.delete_stack_internal(scope, stack_name)
    }

    #[doc = "# Errors\n\nReturns `CloudFormationError` when the requested stack cannot be found."]
    pub fn describe_stacks(
        &self,
        scope: &CloudFormationScope,
        stack_name: Option<&str>,
    ) -> Result<Vec<CloudFormationStackDescription>, CloudFormationError> {
        self.describe_stacks_internal(scope, stack_name)
    }

    #[doc = "# Errors\n\nReturns `CloudFormationError` when stack state cannot be enumerated."]
    pub fn list_stacks(
        &self,
        scope: &CloudFormationScope,
    ) -> Vec<CloudFormationStackDescription> {
        self.list_stacks_internal(scope)
    }

    #[doc = "# Errors\n\nReturns `CloudFormationError` when the stack template is invalid or the requested change set cannot be prepared."]
    pub fn create_change_set(
        &self,
        scope: &CloudFormationScope,
        input: CloudFormationCreateChangeSetInput,
    ) -> Result<CloudFormationCreateChangeSetOutput, CloudFormationError> {
        self.create_change_set_internal(scope, input)
    }

    #[doc = "# Errors\n\nReturns `CloudFormationError` when the stack or change set cannot be found."]
    pub fn describe_change_set(
        &self,
        scope: &CloudFormationScope,
        stack_name: &str,
        change_set_name: &str,
    ) -> Result<CloudFormationChangeSetDescription, CloudFormationError> {
        self.describe_change_set_internal(scope, stack_name, change_set_name)
    }

    #[doc = "# Errors\n\nReturns `CloudFormationError` when the stack or change set cannot be found or provider application fails."]
    pub fn execute_change_set(
        &self,
        scope: &CloudFormationScope,
        stack_name: &str,
        change_set_name: &str,
    ) -> Result<(), CloudFormationError> {
        self.execute_change_set_internal(scope, stack_name, change_set_name)
    }

    #[doc = "# Errors\n\nReturns `CloudFormationError` when the requested stack cannot be found."]
    pub fn list_change_sets(
        &self,
        scope: &CloudFormationScope,
        stack_name: &str,
    ) -> Result<Vec<CloudFormationChangeSetSummary>, CloudFormationError> {
        self.list_change_sets_internal(scope, stack_name)
    }

    #[doc = "# Errors\n\nReturns `CloudFormationError` when the requested stack cannot be found."]
    pub fn describe_stack_events(
        &self,
        scope: &CloudFormationScope,
        stack_name: &str,
    ) -> Result<Vec<CloudFormationStackEvent>, CloudFormationError> {
        self.describe_stack_events_internal(scope, stack_name)
    }

    #[doc = "# Errors\n\nReturns `CloudFormationError` when the requested stack cannot be found."]
    pub fn describe_stack_resources(
        &self,
        scope: &CloudFormationScope,
        stack_name: &str,
    ) -> Result<Vec<CloudFormationStackResourceDescription>, CloudFormationError>
    {
        self.describe_stack_resources_internal(scope, stack_name)
    }

    #[doc = "# Errors\n\nReturns `CloudFormationError` when the requested stack or logical resource cannot be found."]
    pub fn describe_stack_resource(
        &self,
        scope: &CloudFormationScope,
        stack_name: &str,
        logical_resource_id: &str,
    ) -> Result<CloudFormationStackResourceDescription, CloudFormationError>
    {
        self.describe_stack_resource_internal(
            scope,
            stack_name,
            logical_resource_id,
        )
    }

    #[doc = "# Errors\n\nReturns `CloudFormationError` when the requested stack cannot be found."]
    pub fn list_stack_resources(
        &self,
        scope: &CloudFormationScope,
        stack_name: &str,
    ) -> Result<Vec<CloudFormationStackResourceSummary>, CloudFormationError>
    {
        self.list_stack_resources_internal(scope, stack_name)
    }

    #[doc = "# Errors\n\nReturns `CloudFormationError` when the requested stack cannot be found."]
    pub fn get_template(
        &self,
        scope: &CloudFormationScope,
        stack_name: &str,
    ) -> Result<String, CloudFormationError> {
        self.get_template_internal(scope, stack_name)
    }
}

#[derive(Debug, Clone, Default)]
struct CloudFormationState {
    stacks: BTreeMap<CloudFormationStackKey, StoredStack>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct CloudFormationStackKey {
    account_id: AccountId,
    region: RegionId,
    stack_name: String,
}

impl CloudFormationStackKey {
    fn new(scope: &CloudFormationScope, stack_name: &str) -> Self {
        Self {
            account_id: scope.account_id().clone(),
            region: scope.region().clone(),
            stack_name: stack_name.to_owned(),
        }
    }
}

#[derive(Debug, Clone)]
struct StoredStack {
    capabilities: Vec<String>,
    creation_time_epoch_seconds: u64,
    description: Option<String>,
    event_counter: u64,
    events: Vec<StoredStackEvent>,
    change_sets: BTreeMap<String, StoredChangeSet>,
    last_updated_time_epoch_seconds: Option<u64>,
    outputs: BTreeMap<String, StoredStackOutput>,
    parameters: BTreeMap<String, String>,
    resource_order: Vec<String>,
    resources: BTreeMap<String, StoredStackResource>,
    stack_id: String,
    stack_name: String,
    stack_status: String,
    stack_status_reason: Option<String>,
    template_body: String,
}

impl ExistingStackSnapshot for StoredStack {
    type Resource = StoredStackResource;

    fn resource(&self, logical_id: &str) -> Option<&Self::Resource> {
        self.resources.get(logical_id)
    }

    fn resource_order(&self) -> &[String] {
        &self.resource_order
    }
}

#[derive(Debug, Clone)]
struct StoredStackOutput {
    description: Option<String>,
    value: String,
}

#[derive(Debug, Clone)]
struct StoredStackResource {
    attributes: BTreeMap<String, String>,
    last_updated_timestamp_epoch_seconds: u64,
    logical_resource_id: String,
    physical_resource_id: String,
    properties: Value,
    ref_value: String,
    resource_status: String,
    resource_status_reason: Option<String>,
    resource_type: String,
    template_properties: Value,
}

impl ExistingStackResource for StoredStackResource {
    fn resource_type(&self) -> &str {
        &self.resource_type
    }

    fn template_properties(&self) -> &Value {
        &self.template_properties
    }
}

#[derive(Debug, Clone)]
struct StoredStackEvent {
    event_id: String,
    logical_resource_id: String,
    physical_resource_id: Option<String>,
    resource_status: String,
    resource_status_reason: Option<String>,
    resource_type: String,
    timestamp_epoch_seconds: u64,
}

#[derive(Debug, Clone)]
struct StoredChangeSet {
    changes: Vec<CloudFormationChangeSummary>,
    change_set_id: String,
    change_set_name: String,
    change_set_type: String,
    creation_time_epoch_seconds: u64,
    execution_status: String,
    prepared: PreparedOperation,
    stack_id: String,
    stack_name: String,
    status: String,
    status_reason: Option<String>,
}

#[derive(Debug, Clone)]
struct PreparedOperation {
    capabilities: Vec<String>,
    conditions: BTreeMap<String, bool>,
    changes: Vec<CloudFormationChangeSummary>,
    description: Option<String>,
    document: TemplateDocument,
    parameters: BTreeMap<String, String>,
    resource_order: Vec<String>,
    stack_id: String,
    stack_name: String,
    template_body: String,
}

#[derive(Debug, Clone)]
struct ResolvedResourceValues {
    attributes: BTreeMap<String, String>,
    ref_value: String,
    resource_type: String,
}

struct ApplyResourceInput<'a> {
    existing: Option<&'a StoredStackResource>,
    logical_id: &'a str,
    properties: &'a Value,
    resource_type: &'a str,
    scope: &'a CloudFormationScope,
    stack_name: &'a str,
    template_properties: &'a Value,
}

#[derive(Debug, Clone, Copy)]
struct ResourceContract {
    attributes: &'static [&'static str],
    ref_supported: bool,
}

fn resource_contract(resource_type: &str) -> Option<ResourceContract> {
    Some(match resource_type {
        "AWS::S3::Bucket" => ResourceContract {
            ref_supported: true,
            attributes: &[
                "Arn",
                "DomainName",
                "DualStackDomainName",
                "RegionalDomainName",
            ],
        },
        "AWS::S3::BucketPolicy" => {
            ResourceContract { ref_supported: false, attributes: &[] }
        }
        "AWS::SQS::Queue" => ResourceContract {
            ref_supported: true,
            attributes: &["Arn", "QueueName", "QueueUrl"],
        },
        "AWS::SQS::QueuePolicy" => {
            ResourceContract { ref_supported: false, attributes: &["Id"] }
        }
        "AWS::SNS::Topic" => ResourceContract {
            ref_supported: true,
            attributes: &["TopicArn", "TopicName"],
        },
        "AWS::DynamoDB::Table" => ResourceContract {
            ref_supported: true,
            attributes: &["Arn", "StreamArn"],
        },
        "AWS::Lambda::Function" => {
            ResourceContract { ref_supported: true, attributes: &["Arn"] }
        }
        "AWS::IAM::Role" => ResourceContract {
            ref_supported: true,
            attributes: &["Arn", "RoleId"],
        },
        "AWS::IAM::User" => {
            ResourceContract { ref_supported: true, attributes: &["Arn"] }
        }
        "AWS::IAM::AccessKey" => ResourceContract {
            ref_supported: true,
            attributes: &["SecretAccessKey"],
        },
        "AWS::IAM::Policy" => {
            ResourceContract { ref_supported: true, attributes: &["Id"] }
        }
        "AWS::IAM::ManagedPolicy" => {
            ResourceContract { ref_supported: true, attributes: &["Arn"] }
        }
        "AWS::IAM::InstanceProfile" => {
            ResourceContract { ref_supported: true, attributes: &["Arn"] }
        }
        "AWS::SSM::Parameter" => ResourceContract {
            ref_supported: true,
            attributes: &["Type", "Value"],
        },
        "AWS::KMS::Key" => {
            ResourceContract { ref_supported: true, attributes: &["Arn"] }
        }
        "AWS::KMS::Alias" => ResourceContract {
            ref_supported: true,
            attributes: &["AliasArn", "AliasName", "TargetKeyId"],
        },
        "AWS::SecretsManager::Secret" => ResourceContract {
            ref_supported: true,
            attributes: &["Arn", "Name"],
        },
        _ => return None,
    })
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct TemplateDocument {
    #[serde(default)]
    conditions: BTreeMap<String, Value>,
    description: Option<String>,
    #[serde(default)]
    outputs: BTreeMap<String, OutputDefinition>,
    #[serde(default)]
    parameters: BTreeMap<String, ParameterDefinition>,
    #[serde(default)]
    resources: BTreeMap<String, ResourceDefinition>,
    #[serde(default)]
    transform: Option<Value>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct ParameterDefinition {
    default: Option<Value>,
    description: Option<String>,
    no_echo: Option<bool>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct ResourceDefinition {
    condition: Option<String>,
    #[serde(default)]
    depends_on: DependsOn,
    #[serde(default = "empty_object")]
    properties: Value,
    #[serde(rename = "Type")]
    resource_type: String,
}

impl PlannedResourceDefinition for ResourceDefinition {
    fn condition(&self) -> Option<&str> {
        self.condition.as_deref()
    }

    fn depends_on(&self) -> Box<dyn Iterator<Item = &str> + '_> {
        self.depends_on.iter()
    }

    fn properties(&self) -> &Value {
        &self.properties
    }

    fn resource_type(&self) -> &str {
        &self.resource_type
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct OutputDefinition {
    condition: Option<String>,
    description: Option<String>,
    export: Option<Value>,
    value: Value,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(untagged)]
enum DependsOn {
    #[default]
    None,
    One(String),
    Many(Vec<String>),
}

impl DependsOn {
    fn iter(&self) -> Box<dyn Iterator<Item = &str> + '_> {
        match self {
            Self::None => Box::new(std::iter::empty()),
            Self::One(value) => Box::new(std::iter::once(value.as_str())),
            Self::Many(values) => Box::new(values.iter().map(String::as_str)),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
enum ResolvedValue {
    Array(Vec<ResolvedValue>),
    Bool(bool),
    Null,
    Number(serde_json::Number),
    NoValue,
    Object(Map<String, Value>),
    String(String),
}

impl ResolvedValue {
    fn into_json(self) -> Option<Value> {
        match self {
            Self::Array(items) => Some(Value::Array(
                items
                    .into_iter()
                    .filter_map(ResolvedValue::into_json)
                    .collect(),
            )),
            Self::Bool(value) => Some(Value::Bool(value)),
            Self::Null => Some(Value::Null),
            Self::Number(value) => Some(Value::Number(value)),
            Self::NoValue => None,
            Self::Object(value) => Some(Value::Object(value)),
            Self::String(value) => Some(Value::String(value)),
        }
    }

    fn to_string_value(
        &self,
        path: &str,
    ) -> Result<String, CloudFormationError> {
        match self {
            Self::String(value) => Ok(value.clone()),
            Self::Bool(value) => Ok(value.to_string()),
            Self::Number(value) => Ok(value.to_string()),
            Self::Null | Self::NoValue => Ok(String::new()),
            Self::Array(_) | Self::Object(_) => {
                Err(template_validation_error(format!(
                    "expected a scalar value at {path}"
                )))
            }
        }
    }
}

struct ValidationContext<'a> {
    conditions: &'a BTreeSet<String>,
    parameters: &'a BTreeSet<String>,
    resources: &'a BTreeMap<String, String>,
}

#[derive(Clone, Copy)]
struct PlanningContext<'a> {
    account_id: &'a AccountId,
    conditions: Option<&'a BTreeMap<String, bool>>,
    parameters: &'a BTreeMap<String, String>,
    region: &'a RegionId,
    resources: &'a BTreeMap<String, ResourceDefinition>,
    stack_id: &'a str,
    stack_name: &'a str,
}

fn parse_template_document(
    body: &str,
) -> Result<TemplateDocument, CloudFormationError> {
    let yaml_value: YamlValue =
        serde_yaml::from_str(body).map_err(|error| {
            template_validation_error(format!(
                "failed to parse template: {error}"
            ))
        })?;
    let json_value = yaml_value_to_json(yaml_value)?;

    serde_json::from_value(json_value).map_err(|error| {
        template_validation_error(format!(
            "template structure is invalid: {error}"
        ))
    })
}

fn validate_template_document(
    document: &TemplateDocument,
) -> Result<(), CloudFormationError> {
    if let Some(transform) = &document.transform {
        return Err(template_validation_error(format!(
            "unsupported transform declaration: {}",
            summarize_value(transform)
        )));
    }

    let resources = document
        .resources
        .iter()
        .map(|(logical_id, resource)| {
            validate_resource_type(logical_id, &resource.resource_type)?;
            Ok((logical_id.clone(), resource.resource_type.clone()))
        })
        .collect::<Result<BTreeMap<_, _>, CloudFormationError>>()?;
    let parameters =
        document.parameters.keys().cloned().collect::<BTreeSet<_>>();
    let conditions =
        document.conditions.keys().cloned().collect::<BTreeSet<_>>();
    let context = ValidationContext {
        conditions: &conditions,
        parameters: &parameters,
        resources: &resources,
    };

    for (name, parameter) in &document.parameters {
        if let Some(default) = &parameter.default {
            scalar_parameter_value(default, &parameter_path(name, "Default"))?;
        }
    }

    for (name, condition) in &document.conditions {
        validate_condition_expression(
            condition,
            &context,
            &condition_path(name),
        )?;
    }

    for (logical_id, resource) in &document.resources {
        if let Some(condition) = &resource.condition {
            ensure_condition_exists(condition, &conditions, logical_id)?;
        }
        for dependency in resource.depends_on.iter() {
            if !resources.contains_key(dependency) {
                return Err(template_validation_error(format!(
                    "resource {logical_id} depends on unknown resource {dependency}"
                )));
            }
        }
        validate_value(
            &resource.properties,
            &context,
            &resource_path(logical_id, "Properties"),
        )?;
    }

    for (logical_id, output) in &document.outputs {
        if let Some(condition) = &output.condition {
            ensure_condition_exists(condition, &conditions, logical_id)?;
        }
        if output.export.is_some() {
            return Err(template_validation_error(format!(
                "output {logical_id} uses unsupported export behavior"
            )));
        }
        validate_value(&output.value, &context, &output_path(logical_id))?;
    }

    Ok(())
}

fn validate_resource_type(
    logical_id: &str,
    resource_type: &str,
) -> Result<(), CloudFormationError> {
    if SUPPORTED_RESOURCE_TYPES.contains(&resource_type) {
        return Ok(());
    }

    Err(template_validation_error(format!(
        "resource {logical_id} uses unsupported type {resource_type}"
    )))
}

fn validate_condition_expression(
    value: &Value,
    context: &ValidationContext<'_>,
    path: &str,
) -> Result<(), CloudFormationError> {
    match value {
        Value::Bool(_) => Ok(()),
        Value::Object(map) if map.len() == 1 => {
            let (key, inner) =
                single_entry_object(map, path, "condition expression")?;
            match key {
                "Condition" => {
                    let name = inner.as_str().ok_or_else(|| {
                        template_validation_error(format!(
                            "Condition at {path} must reference a named condition"
                        ))
                    })?;
                    ensure_condition_name_known(name, context.conditions, path)
                }
                "Ref" => validate_ref_target(inner, context, path, true),
                "Fn::Equals" => {
                    let items = array_items(inner, 2, 2, path, "Fn::Equals")?;
                    validate_value(
                        required_item(items, 0, path, "Fn::Equals")?,
                        context,
                        &indexed_path(path, 0),
                    )?;
                    validate_value(
                        required_item(items, 1, path, "Fn::Equals")?,
                        context,
                        &indexed_path(path, 1),
                    )
                }
                "Fn::Not" => {
                    let items = array_items(inner, 1, 1, path, "Fn::Not")?;
                    validate_condition_expression(
                        required_item(items, 0, path, "Fn::Not")?,
                        context,
                        &indexed_path(path, 0),
                    )
                }
                "Fn::And" | "Fn::Or" => {
                    let items = array_items(inner, 2, 10, path, key)?;
                    for (index, item) in items.iter().enumerate() {
                        validate_condition_expression(
                            item,
                            context,
                            &indexed_path(path, index),
                        )?;
                    }
                    Ok(())
                }
                other => Err(unsupported_intrinsic_error(other, path)),
            }
        }
        _ => Err(template_validation_error(format!(
            "condition expression at {path} must be a supported condition intrinsic"
        ))),
    }
}

fn validate_value(
    value: &Value,
    context: &ValidationContext<'_>,
    path: &str,
) -> Result<(), CloudFormationError> {
    match value {
        Value::Array(items) => {
            for (index, item) in items.iter().enumerate() {
                validate_value(item, context, &indexed_path(path, index))?;
            }
            Ok(())
        }
        Value::Object(map) if map.len() == 1 => {
            let (key, inner) =
                single_entry_object(map, path, "intrinsic value")?;
            match key {
                "Ref" => validate_ref_target(inner, context, path, false),
                "Fn::Base64" => validate_value(
                    inner,
                    context,
                    &child_path(path, "Fn::Base64"),
                ),
                "Fn::If" => {
                    let items = array_items(inner, 3, 3, path, "Fn::If")?;
                    let condition_name = required_string_item(
                        items,
                        0,
                        path,
                        "Fn::If",
                        format!(
                            "Fn::If at {path} must reference a named condition"
                        ),
                    )?;
                    ensure_condition_name_known(
                        condition_name,
                        context.conditions,
                        path,
                    )?;
                    validate_value(
                        required_item(items, 1, path, "Fn::If")?,
                        context,
                        &indexed_path(path, 1),
                    )?;
                    validate_value(
                        required_item(items, 2, path, "Fn::If")?,
                        context,
                        &indexed_path(path, 2),
                    )
                }
                "Fn::Join" => {
                    let items = array_items(inner, 2, 2, path, "Fn::Join")?;
                    let delimiter_path = indexed_path(path, 0);
                    required_string_item(
                        items,
                        0,
                        path,
                        "Fn::Join",
                        format!(
                            "Fn::Join delimiter at {delimiter_path} must be a string"
                        ),
                    )?;
                    let values = required_array_item(
                        items,
                        1,
                        path,
                        "Fn::Join",
                        format!(
                            "Fn::Join list at {} must be an array",
                            indexed_path(path, 1)
                        ),
                    )?;
                    for (index, item) in values.iter().enumerate() {
                        validate_value(
                            item,
                            context,
                            &indexed_path(&indexed_path(path, 1), index),
                        )?;
                    }
                    Ok(())
                }
                "Fn::Select" => {
                    let items = array_items(inner, 2, 2, path, "Fn::Select")?;
                    validate_value(
                        required_item(items, 0, path, "Fn::Select")?,
                        context,
                        &indexed_path(path, 0),
                    )?;
                    validate_value(
                        required_item(items, 1, path, "Fn::Select")?,
                        context,
                        &indexed_path(path, 1),
                    )
                }
                "Fn::Sub" => validate_sub(inner, context, path),
                "Fn::GetAtt" => validate_get_att(inner, context, path),
                other if other.starts_with("Fn::") => {
                    Err(unsupported_intrinsic_error(other, path))
                }
                _ => {
                    let mut object_path = path.to_owned();
                    if object_path.is_empty() {
                        object_path = "<root>".to_owned();
                    }
                    for (field, field_value) in map {
                        validate_value(
                            field_value,
                            context,
                            &child_path(&object_path, field),
                        )?;
                    }
                    Ok(())
                }
            }
        }
        Value::Object(map) => {
            for (field, field_value) in map {
                validate_value(
                    field_value,
                    context,
                    &child_path(path, field),
                )?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn validate_ref_target(
    value: &Value,
    context: &ValidationContext<'_>,
    path: &str,
    allow_condition: bool,
) -> Result<(), CloudFormationError> {
    let target = value.as_str().ok_or_else(|| {
        template_validation_error(format!("Ref at {path} must be a string"))
    })?;

    if is_supported_pseudo_parameter(target)
        || context.parameters.contains(target)
        || (allow_condition && context.conditions.contains(target))
    {
        return Ok(());
    }

    if let Some(resource_type) = context.resources.get(target) {
        return if resource_contract(resource_type)
            .is_some_and(|contract| contract.ref_supported)
        {
            Ok(())
        } else {
            Err(unsupported_resource_reference_error(
                target,
                resource_type,
                path,
            ))
        };
    }

    Err(template_validation_error(format!(
        "Ref at {path} targets unknown parameter or pseudo-parameter {target}"
    )))
}

fn validate_sub(
    value: &Value,
    context: &ValidationContext<'_>,
    path: &str,
) -> Result<(), CloudFormationError> {
    let (template, variables) = match value {
        Value::String(template) => (template.as_str(), None),
        Value::Array(items) if items.len() == 2 => {
            let template = required_string_item(
                items,
                0,
                path,
                "Fn::Sub",
                format!("Fn::Sub at {path} must start with a string template"),
            )?;
            let variables = required_object_item(
                items,
                1,
                path,
                "Fn::Sub",
                format!(
                    "Fn::Sub variable map at {} must be an object",
                    indexed_path(path, 1)
                ),
            )?;
            for (name, variable) in variables {
                validate_value(
                    variable,
                    context,
                    &child_path(&indexed_path(path, 1), name),
                )?;
            }
            (template, Some(variables))
        }
        _ => {
            return Err(template_validation_error(format!(
                "Fn::Sub at {path} must be a string or [template, variables] tuple"
            )));
        }
    };

    for token in sub_tokens(template) {
        if variables.and_then(|vars| vars.get(token)).is_some() {
            continue;
        }
        if token.contains('.') {
            let (logical_id, attribute) =
                token.split_once('.').ok_or_else(|| {
                    template_validation_error(format!(
                        "Fn::Sub token {token} at {path} must use Resource.Attribute"
                    ))
                })?;
            validate_get_att(
                &Value::Array(vec![
                    Value::String(logical_id.to_owned()),
                    Value::String(attribute.to_owned()),
                ]),
                context,
                path,
            )?;
            continue;
        }
        validate_ref_target(
            &Value::String(token.to_owned()),
            context,
            path,
            false,
        )?;
    }

    Ok(())
}

fn validate_get_att(
    value: &Value,
    context: &ValidationContext<'_>,
    path: &str,
) -> Result<(), CloudFormationError> {
    let (logical_id, attribute) = get_att_parts(value, path)?;
    let resource_type =
        context.resources.get(logical_id).ok_or_else(|| {
            template_validation_error(format!(
                "Fn::GetAtt at {path} references unknown resource {logical_id}"
            ))
        })?;
    let Some(contract) = resource_contract(resource_type) else {
        return Err(template_validation_error(format!(
            "Fn::GetAtt attribute {attribute} for resource {logical_id} ({resource_type}) is not supported"
        )));
    };
    if contract.attributes.contains(&attribute) {
        return Ok(());
    }

    Err(template_validation_error(format!(
        "Fn::GetAtt attribute {attribute} for resource {logical_id} ({resource_type}) is not supported"
    )))
}

fn planned_parameter_values(
    definitions: &BTreeMap<String, ParameterDefinition>,
    overrides: &BTreeMap<String, String>,
) -> Result<BTreeMap<String, String>, CloudFormationError> {
    let mut values = BTreeMap::new();

    for (name, definition) in definitions {
        if let Some(value) = overrides.get(name) {
            values.insert(name.clone(), value.clone());
            continue;
        }

        if let Some(default) = &definition.default {
            values.insert(
                name.clone(),
                scalar_parameter_value(
                    default,
                    &parameter_path(name, "Default"),
                )?,
            );
            continue;
        }

        return Err(template_validation_error(format!(
            "parameter {name} must be supplied"
        )));
    }

    Ok(values)
}

fn evaluate_conditions(
    conditions: &BTreeMap<String, Value>,
    context: &PlanningContext<'_>,
) -> Result<BTreeMap<String, bool>, CloudFormationError> {
    let mut resolved = BTreeMap::new();
    let mut visiting = BTreeSet::new();

    for name in conditions.keys() {
        resolve_condition(
            name,
            conditions,
            context,
            &mut resolved,
            &mut visiting,
        )?;
    }

    Ok(resolved)
}

fn resolve_condition(
    name: &str,
    conditions: &BTreeMap<String, Value>,
    context: &PlanningContext<'_>,
    resolved: &mut BTreeMap<String, bool>,
    visiting: &mut BTreeSet<String>,
) -> Result<bool, CloudFormationError> {
    if let Some(value) = resolved.get(name) {
        return Ok(*value);
    }
    if !visiting.insert(name.to_owned()) {
        return Err(template_validation_error(format!(
            "condition {name} contains a cycle"
        )));
    }

    let expression = conditions.get(name).ok_or_else(|| {
        template_validation_error(format!("unknown condition {name}"))
    })?;
    let value = evaluate_condition_expression(
        expression, conditions, context, resolved, visiting,
    )?;
    visiting.remove(name);
    resolved.insert(name.to_owned(), value);

    Ok(value)
}

fn evaluate_condition_expression(
    value: &Value,
    conditions: &BTreeMap<String, Value>,
    context: &PlanningContext<'_>,
    resolved: &mut BTreeMap<String, bool>,
    visiting: &mut BTreeSet<String>,
) -> Result<bool, CloudFormationError> {
    match value {
        Value::Bool(value) => Ok(*value),
        Value::Object(map) if map.len() == 1 => {
            let (key, inner) =
                single_entry_object(map, "Conditions", "condition")?;
            match key {
                "Condition" => {
                    let name = inner.as_str().ok_or_else(|| {
                        template_validation_error(
                            "Condition references must be strings",
                        )
                    })?;
                    resolve_condition(
                        name, conditions, context, resolved, visiting,
                    )
                }
                "Ref" => {
                    let resolved =
                        evaluate_ref(inner, context, "Conditions.Ref")?;
                    match resolved {
                        ResolvedValue::Bool(value) => Ok(value),
                        ResolvedValue::String(value) => Ok(matches!(
                            value.as_str(),
                            "true" | "True" | "TRUE"
                        )),
                        _ => Err(template_validation_error(
                            "condition Refs must resolve to scalar values",
                        )),
                    }
                }
                "Fn::Equals" => {
                    let items =
                        array_items(inner, 2, 2, "Conditions", "Fn::Equals")?;
                    Ok(evaluate_value(
                        required_item(items, 0, "Conditions", "Fn::Equals")?,
                        context,
                        "Conditions[0]",
                    )? == evaluate_value(
                        required_item(items, 1, "Conditions", "Fn::Equals")?,
                        context,
                        "Conditions[1]",
                    )?)
                }
                "Fn::Not" => {
                    let items =
                        array_items(inner, 1, 1, "Conditions", "Fn::Not")?;
                    Ok(!evaluate_condition_expression(
                        required_item(items, 0, "Conditions", "Fn::Not")?,
                        conditions,
                        context,
                        resolved,
                        visiting,
                    )?)
                }
                "Fn::And" => {
                    let items =
                        array_items(inner, 2, 10, "Conditions", "Fn::And")?;
                    for item in items {
                        if !evaluate_condition_expression(
                            item, conditions, context, resolved, visiting,
                        )? {
                            return Ok(false);
                        }
                    }
                    Ok(true)
                }
                "Fn::Or" => {
                    let items =
                        array_items(inner, 2, 10, "Conditions", "Fn::Or")?;
                    for item in items {
                        if evaluate_condition_expression(
                            item, conditions, context, resolved, visiting,
                        )? {
                            return Ok(true);
                        }
                    }
                    Ok(false)
                }
                other => Err(unsupported_intrinsic_error(other, "Conditions")),
            }
        }
        _ => Err(template_validation_error(
            "condition expressions must use supported condition intrinsics",
        )),
    }
}

fn evaluate_value(
    value: &Value,
    context: &PlanningContext<'_>,
    path: &str,
) -> Result<ResolvedValue, CloudFormationError> {
    match value {
        Value::Null => Ok(ResolvedValue::Null),
        Value::Bool(value) => Ok(ResolvedValue::Bool(*value)),
        Value::Number(value) => Ok(ResolvedValue::Number(value.clone())),
        Value::String(value) => Ok(ResolvedValue::String(value.clone())),
        Value::Array(items) => {
            let mut resolved = Vec::new();
            for (index, item) in items.iter().enumerate() {
                let value =
                    evaluate_value(item, context, &indexed_path(path, index))?;
                if value != ResolvedValue::NoValue {
                    resolved.push(value);
                }
            }
            Ok(ResolvedValue::Array(resolved))
        }
        Value::Object(map) if map.len() == 1 => {
            let (key, inner) =
                single_entry_object(map, path, "intrinsic value")?;
            match key {
                "Ref" => evaluate_ref(inner, context, path),
                "Fn::Base64" => Ok(ResolvedValue::String(
                    BASE64_STANDARD.encode(
                        evaluate_value(
                            inner,
                            context,
                            &child_path(path, "Fn::Base64"),
                        )?
                        .to_string_value(path)?,
                    ),
                )),
                "Fn::If" => {
                    let items = array_items(inner, 3, 3, path, "Fn::If")?;
                    let condition_name = required_string_item(
                        items,
                        0,
                        path,
                        "Fn::If",
                        format!(
                            "Fn::If at {path} must reference a named condition"
                        ),
                    )?;
                    let active = context
                        .conditions
                        .and_then(|conditions| conditions.get(condition_name))
                        .copied()
                        .ok_or_else(|| {
                            template_validation_error(format!(
                                "Fn::If at {path} references unknown condition {condition_name}"
                            ))
                        })?;

                    evaluate_value(
                        if active {
                            required_item(items, 1, path, "Fn::If")?
                        } else {
                            required_item(items, 2, path, "Fn::If")?
                        },
                        context,
                        path,
                    )
                }
                "Fn::Join" => evaluate_join(inner, context, path),
                "Fn::Select" => evaluate_select(inner, context, path),
                "Fn::Sub" => evaluate_sub(inner, context, path),
                "Fn::GetAtt" => {
                    let (logical_id, attribute) = get_att_parts(inner, path)?;
                    let resource_type = context
                        .resources
                        .get(logical_id)
                        .map(|resource| resource.resource_type.as_str())
                        .unwrap_or("unknown");
                    Err(template_validation_error(format!(
                        "Fn::GetAtt attribute {attribute} for resource {logical_id} ({resource_type}) is not supported before provider contracts are implemented"
                    )))
                }
                other if other.starts_with("Fn::") => {
                    Err(unsupported_intrinsic_error(other, path))
                }
                _ => {
                    let mut object = Map::new();
                    for (field, field_value) in map {
                        if let Some(json_value) = evaluate_value(
                            field_value,
                            context,
                            &child_path(path, field),
                        )?
                        .into_json()
                        {
                            object.insert(field.clone(), json_value);
                        }
                    }
                    Ok(ResolvedValue::Object(object))
                }
            }
        }
        Value::Object(map) => {
            let mut object = Map::new();
            for (field, field_value) in map {
                if let Some(json_value) = evaluate_value(
                    field_value,
                    context,
                    &child_path(path, field),
                )?
                .into_json()
                {
                    object.insert(field.clone(), json_value);
                }
            }
            Ok(ResolvedValue::Object(object))
        }
    }
}

fn evaluate_ref(
    value: &Value,
    context: &PlanningContext<'_>,
    path: &str,
) -> Result<ResolvedValue, CloudFormationError> {
    let target = value.as_str().ok_or_else(|| {
        template_validation_error(format!("Ref at {path} must be a string"))
    })?;

    match target {
        "AWS::AccountId" => {
            Ok(ResolvedValue::String(context.account_id.to_string()))
        }
        "AWS::Region" => Ok(ResolvedValue::String(context.region.to_string())),
        "AWS::StackName" => {
            Ok(ResolvedValue::String(context.stack_name.to_owned()))
        }
        "AWS::StackId" => {
            Ok(ResolvedValue::String(context.stack_id.to_owned()))
        }
        "AWS::Partition" => Ok(ResolvedValue::String("aws".to_owned())),
        "AWS::URLSuffix" => {
            Ok(ResolvedValue::String("amazonaws.com".to_owned()))
        }
        "AWS::NoValue" => Ok(ResolvedValue::NoValue),
        _ => {
            if let Some(value) = context.parameters.get(target) {
                return Ok(ResolvedValue::String(value.clone()));
            }
            if let Some(resource) = context.resources.get(target) {
                return Err(unsupported_resource_reference_error(
                    target,
                    &resource.resource_type,
                    path,
                ));
            }

            Err(template_validation_error(format!(
                "Ref at {path} targets unknown parameter or pseudo-parameter {target}"
            )))
        }
    }
}

fn evaluate_join(
    value: &Value,
    context: &PlanningContext<'_>,
    path: &str,
) -> Result<ResolvedValue, CloudFormationError> {
    let items = array_items(value, 2, 2, path, "Fn::Join")?;
    let delimiter = required_string_item(
        items,
        0,
        path,
        "Fn::Join",
        format!(
            "Fn::Join delimiter at {} must be a string",
            indexed_path(path, 0)
        ),
    )?;
    let values = required_array_item(
        items,
        1,
        path,
        "Fn::Join",
        format!("Fn::Join list at {} must be an array", indexed_path(path, 1)),
    )?;
    let mut rendered = Vec::new();
    for (index, item) in values.iter().enumerate() {
        rendered.push(
            evaluate_value(item, context, &indexed_path(path, index))?
                .to_string_value(path)?,
        );
    }

    Ok(ResolvedValue::String(rendered.join(delimiter)))
}

fn evaluate_select(
    value: &Value,
    context: &PlanningContext<'_>,
    path: &str,
) -> Result<ResolvedValue, CloudFormationError> {
    let items = array_items(value, 2, 2, path, "Fn::Select")?;
    let index_value = evaluate_value(
        required_item(items, 0, path, "Fn::Select")?,
        context,
        &indexed_path(path, 0),
    )?;
    let index =
        index_value.to_string_value(path)?.parse::<usize>().map_err(|_| {
            template_validation_error(format!(
                "Fn::Select index at {} must be an unsigned integer",
                indexed_path(path, 0)
            ))
        })?;
    let values = evaluate_value(
        required_item(items, 1, path, "Fn::Select")?,
        context,
        &indexed_path(path, 1),
    )?;
    let items = match values {
        ResolvedValue::Array(items) => items,
        _ => {
            return Err(template_validation_error(format!(
                "Fn::Select source at {} must resolve to an array",
                indexed_path(path, 1)
            )));
        }
    };

    items.into_iter().nth(index).ok_or_else(|| {
        template_validation_error(format!(
            "Fn::Select index {index} is out of bounds at {path}"
        ))
    })
}

fn evaluate_sub(
    value: &Value,
    context: &PlanningContext<'_>,
    path: &str,
) -> Result<ResolvedValue, CloudFormationError> {
    let (template, variables) = match value {
        Value::String(template) => (template.as_str(), None),
        Value::Array(items) if items.len() == 2 => {
            let template = required_string_item(
                items,
                0,
                path,
                "Fn::Sub",
                format!("Fn::Sub at {path} must start with a string template"),
            )?;
            let variables = required_object_item(
                items,
                1,
                path,
                "Fn::Sub",
                format!(
                    "Fn::Sub variable map at {} must be an object",
                    indexed_path(path, 1)
                ),
            )?;
            (template, Some(variables))
        }
        _ => {
            return Err(template_validation_error(format!(
                "Fn::Sub at {path} must be a string or [template, variables] tuple"
            )));
        }
    };

    let mut rendered = String::new();
    let mut remaining = template;

    while let Some(start) = remaining.find("${") {
        rendered.push_str(&remaining[..start]);
        let token_start = start + 2;
        let Some(end) = remaining[token_start..].find('}') else {
            return Err(template_validation_error(format!(
                "Fn::Sub at {path} contains an unterminated token"
            )));
        };
        let token = &remaining[token_start..token_start + end];
        let value = if let Some(variable) =
            variables.and_then(|variables| variables.get(token))
        {
            evaluate_value(variable, context, &child_path(path, token))?
                .to_string_value(path)?
        } else if token.contains('.') {
            return Err(template_validation_error(format!(
                "Fn::Sub token {token} at {path} uses unsupported resource attribute shorthand"
            )));
        } else {
            evaluate_ref(&Value::String(token.to_owned()), context, path)?
                .to_string_value(path)?
        };
        rendered.push_str(&value);
        remaining = &remaining[token_start + end + 1..];
    }

    rendered.push_str(remaining);

    Ok(ResolvedValue::String(rendered))
}

fn template_parameters(
    definitions: &BTreeMap<String, ParameterDefinition>,
) -> Result<Vec<CloudFormationTemplateParameter>, CloudFormationError> {
    definitions
        .iter()
        .map(|(name, definition)| {
            Ok(CloudFormationTemplateParameter {
                default_value: definition
                    .default
                    .as_ref()
                    .map(|value| {
                        scalar_parameter_value(
                            value,
                            &parameter_path(name, "Default"),
                        )
                    })
                    .transpose()?,
                description: definition.description.clone(),
                no_echo: definition.no_echo.unwrap_or(false),
                parameter_key: name.clone(),
            })
        })
        .collect()
}

fn required_capabilities(
    resources: &BTreeMap<String, ResourceDefinition>,
) -> (Vec<String>, Option<String>) {
    let mut iam_resources = Vec::new();
    let mut named_iam = false;

    for (logical_id, resource) in resources {
        if !IAM_RESOURCE_TYPES.contains(&resource.resource_type.as_str()) {
            continue;
        }

        iam_resources.push(logical_id.clone());
        named_iam |= resource_has_named_iam_property(resource);
    }

    if iam_resources.is_empty() {
        return (Vec::new(), None);
    }

    let capabilities = if named_iam {
        vec!["CAPABILITY_NAMED_IAM".to_owned()]
    } else {
        vec!["CAPABILITY_IAM".to_owned()]
    };

    (
        capabilities,
        Some(format!(
            "The following resource(s) require capabilities: {}",
            iam_resources.join(", ")
        )),
    )
}

fn resource_has_named_iam_property(resource: &ResourceDefinition) -> bool {
    let Some(properties) = resource.properties.as_object() else {
        return false;
    };

    let name_key = match resource.resource_type.as_str() {
        "AWS::IAM::Role" => "RoleName",
        "AWS::IAM::User" => "UserName",
        "AWS::IAM::Policy" => "PolicyName",
        "AWS::IAM::ManagedPolicy" => "ManagedPolicyName",
        "AWS::IAM::InstanceProfile" => "InstanceProfileName",
        _ => return false,
    };

    properties.contains_key(name_key)
}

fn yaml_value_to_json(value: YamlValue) -> Result<Value, CloudFormationError> {
    match value {
        YamlValue::Null => Ok(Value::Null),
        YamlValue::Bool(value) => Ok(Value::Bool(value)),
        YamlValue::Number(number) => {
            serde_json::to_value(number).map_err(|error| {
                template_validation_error(format!(
                    "failed to convert YAML number to JSON: {error}"
                ))
            })
        }
        YamlValue::String(value) => Ok(Value::String(value)),
        YamlValue::Sequence(items) => Ok(Value::Array(
            items
                .into_iter()
                .map(yaml_value_to_json)
                .collect::<Result<Vec<_>, _>>()?,
        )),
        YamlValue::Mapping(entries) => {
            let mut object = Map::new();
            for (key, value) in entries {
                object.insert(
                    yaml_key_to_string(key)?,
                    yaml_value_to_json(value)?,
                );
            }
            Ok(Value::Object(object))
        }
        YamlValue::Tagged(tagged) => tagged_yaml_value_to_json(*tagged),
    }
}

fn tagged_yaml_value_to_json(
    tagged: TaggedValue,
) -> Result<Value, CloudFormationError> {
    let key = match tagged.tag.to_string().trim_start_matches('!') {
        "Ref" => "Ref",
        "Sub" => "Fn::Sub",
        "GetAtt" => "Fn::GetAtt",
        "If" => "Fn::If",
        "Join" => "Fn::Join",
        "Select" => "Fn::Select",
        "Base64" => "Fn::Base64",
        "Equals" => "Fn::Equals",
        "Not" => "Fn::Not",
        "And" => "Fn::And",
        "Or" => "Fn::Or",
        "Split" => "Fn::Split",
        "FindInMap" => "Fn::FindInMap",
        "ImportValue" => "Fn::ImportValue",
        "Transform" => "Fn::Transform",
        "Condition" => "Condition",
        other => {
            return Err(template_validation_error(format!(
                "unsupported YAML tag !{other}"
            )));
        }
    };

    let value = if key == "Fn::GetAtt" {
        normalize_get_att_yaml_value(tagged.value)?
    } else {
        yaml_value_to_json(tagged.value)?
    };

    let mut object = Map::new();
    object.insert(key.to_owned(), value);
    Ok(Value::Object(object))
}

fn normalize_get_att_yaml_value(
    value: YamlValue,
) -> Result<Value, CloudFormationError> {
    match value {
        YamlValue::String(value) => {
            let mut parts = value.splitn(2, '.');
            let logical_id = parts.next().unwrap_or_default();
            let attribute = parts.next().unwrap_or_default();

            Ok(Value::Array(vec![
                Value::String(logical_id.to_owned()),
                Value::String(attribute.to_owned()),
            ]))
        }
        other => yaml_value_to_json(other),
    }
}

fn yaml_key_to_string(
    value: YamlValue,
) -> Result<String, CloudFormationError> {
    match value {
        YamlValue::String(value) => Ok(value),
        YamlValue::Bool(value) => Ok(value.to_string()),
        YamlValue::Number(number) => {
            serde_json::to_string(&number).map_err(|error| {
                template_validation_error(format!(
                    "failed to convert YAML map key to string: {error}"
                ))
            })
        }
        YamlValue::Tagged(tagged) => {
            let value = tagged_yaml_value_to_json(*tagged)?;
            Err(template_validation_error(format!(
                "template map keys must be scalars, found tagged key {}",
                summarize_value(&value)
            )))
        }
        _ => Err(template_validation_error(
            "template map keys must be scalar values",
        )),
    }
}

pub(crate) fn get_att_parts<'a>(
    value: &'a Value,
    path: &str,
) -> Result<(&'a str, &'a str), CloudFormationError> {
    match value {
        Value::Array(items) if items.len() == 2 => {
            let logical_id = required_string_item(
                items,
                0,
                path,
                "Fn::GetAtt",
                format!("Fn::GetAtt at {path} must use string logical ids"),
            )?;
            let attribute = required_string_item(
                items,
                1,
                path,
                "Fn::GetAtt",
                format!("Fn::GetAtt at {path} must use string attributes"),
            )?;
            Ok((logical_id, attribute))
        }
        Value::String(value) => {
            let Some((logical_id, attribute)) = value.split_once('.') else {
                return Err(template_validation_error(format!(
                    "Fn::GetAtt at {path} must use Resource.Attribute or [Resource, Attribute]"
                )));
            };
            Ok((logical_id, attribute))
        }
        _ => Err(template_validation_error(format!(
            "Fn::GetAtt at {path} must use Resource.Attribute or [Resource, Attribute]"
        ))),
    }
}

fn scalar_parameter_value(
    value: &Value,
    path: &str,
) -> Result<String, CloudFormationError> {
    match value {
        Value::String(value) => Ok(value.clone()),
        Value::Bool(value) => Ok(value.to_string()),
        Value::Number(value) => Ok(value.to_string()),
        Value::Null => Ok(String::new()),
        _ => Err(template_validation_error(format!(
            "parameter default at {path} must be a scalar value"
        ))),
    }
}

fn is_supported_pseudo_parameter(value: &str) -> bool {
    matches!(
        value,
        "AWS::AccountId"
            | "AWS::Region"
            | "AWS::StackName"
            | "AWS::StackId"
            | "AWS::Partition"
            | "AWS::URLSuffix"
            | "AWS::NoValue"
    )
}

fn unsupported_intrinsic_error(
    intrinsic: &str,
    path: &str,
) -> CloudFormationError {
    template_validation_error(format!(
        "unsupported intrinsic {intrinsic} at {path}"
    ))
}

fn unsupported_resource_reference_error(
    logical_id: &str,
    resource_type: &str,
    path: &str,
) -> CloudFormationError {
    template_validation_error(format!(
        "resource reference {logical_id} ({resource_type}) at {path} is not supported"
    ))
}

pub(crate) fn template_validation_error(
    detail: impl Into<String>,
) -> CloudFormationError {
    CloudFormationError::Validation {
        message: format!("Template format error: {}", detail.into()),
    }
}

fn ensure_condition_exists(
    condition: &str,
    conditions: &BTreeSet<String>,
    logical_id: &str,
) -> Result<(), CloudFormationError> {
    ensure_condition_name_known(
        condition,
        conditions,
        &format!("Condition on {logical_id}"),
    )
}

fn ensure_condition_name_known(
    condition: &str,
    conditions: &BTreeSet<String>,
    path: &str,
) -> Result<(), CloudFormationError> {
    if conditions.contains(condition) {
        return Ok(());
    }

    Err(template_validation_error(format!(
        "unknown condition {condition} referenced at {path}"
    )))
}

pub(crate) fn resource_enabled(
    condition: Option<&str>,
    conditions: &BTreeMap<String, bool>,
) -> Result<bool, CloudFormationError> {
    match condition {
        Some(name) => conditions.get(name).copied().ok_or_else(|| {
            template_validation_error(format!("unknown condition {name}"))
        }),
        None => Ok(true),
    }
}

fn array_items<'a>(
    value: &'a Value,
    min: usize,
    max: usize,
    path: &str,
    intrinsic: &str,
) -> Result<&'a Vec<Value>, CloudFormationError> {
    let items = value.as_array().ok_or_else(|| {
        template_validation_error(format!(
            "{intrinsic} at {path} must be an array"
        ))
    })?;
    if !(min..=max).contains(&items.len()) {
        return Err(template_validation_error(format!(
            "{intrinsic} at {path} must contain between {min} and {max} entries"
        )));
    }

    Ok(items)
}

fn single_entry_object<'a>(
    map: &'a Map<String, Value>,
    path: &str,
    context: &str,
) -> Result<(&'a str, &'a Value), CloudFormationError> {
    map.iter().next().map(|(key, value)| (key.as_str(), value)).ok_or_else(
        || {
            template_validation_error(format!(
                "{context} at {path} must contain exactly one entry"
            ))
        },
    )
}

fn required_item<'a>(
    items: &'a [Value],
    index: usize,
    path: &str,
    context: &str,
) -> Result<&'a Value, CloudFormationError> {
    items.get(index).ok_or_else(|| {
        template_validation_error(format!(
            "{context} at {path} is missing item {index}"
        ))
    })
}

fn required_string_item<'a>(
    items: &'a [Value],
    index: usize,
    path: &str,
    context: &str,
    detail: String,
) -> Result<&'a str, CloudFormationError> {
    required_item(items, index, path, context)?
        .as_str()
        .ok_or_else(|| template_validation_error(detail))
}

fn required_array_item<'a>(
    items: &'a [Value],
    index: usize,
    path: &str,
    context: &str,
    detail: String,
) -> Result<&'a Vec<Value>, CloudFormationError> {
    required_item(items, index, path, context)?
        .as_array()
        .ok_or_else(|| template_validation_error(detail))
}

fn required_object_item<'a>(
    items: &'a [Value],
    index: usize,
    path: &str,
    context: &str,
    detail: String,
) -> Result<&'a Map<String, Value>, CloudFormationError> {
    required_item(items, index, path, context)?
        .as_object()
        .ok_or_else(|| template_validation_error(detail))
}

fn child_path(path: &str, child: &str) -> String {
    if path.is_empty() { child.to_owned() } else { format!("{path}.{child}") }
}

fn indexed_path(path: &str, index: usize) -> String {
    format!("{path}[{index}]")
}

fn parameter_path(name: &str, field: &str) -> String {
    format!("Parameters.{name}.{field}")
}

fn resource_path(logical_id: &str, field: &str) -> String {
    format!("Resources.{logical_id}.{field}")
}

fn condition_path(name: &str) -> String {
    format!("Conditions.{name}")
}

fn output_path(name: &str) -> String {
    format!("Outputs.{name}.Value")
}

fn summarize_value(value: &Value) -> String {
    match value {
        Value::String(value) => value.clone(),
        _ => serde_json::to_string(value)
            .unwrap_or_else(|_| "<unserializable>".to_owned()),
    }
}

pub(crate) fn sub_tokens(template: &str) -> Vec<&str> {
    let mut tokens = Vec::new();
    let mut remaining = template;

    while let Some(start) = remaining.find("${") {
        let token_start = start + 2;
        let Some(end) = remaining[token_start..].find('}') else {
            break;
        };
        tokens.push(&remaining[token_start..token_start + end]);
        remaining = &remaining[token_start + end + 1..];
    }

    tokens
}

fn planned_stack_id(scope: &CloudFormationScope, stack_name: &str) -> String {
    let digest = Sha256::digest(format!(
        "{}:{}:{stack_name}",
        scope.account_id(),
        scope.region(),
    ));
    let hex = format!("{digest:x}");

    format!(
        "arn:aws:cloudformation:{}:{}:stack/{stack_name}/{}-{}-{}-{}-{}",
        scope.region(),
        scope.account_id(),
        &hex[0..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..32],
    )
}

fn empty_object() -> Value {
    Value::Object(Map::new())
}

impl CloudFormationService {
    fn template_body(
        &self,
        scope: &CloudFormationScope,
        template_body: Option<String>,
        template_url: Option<String>,
    ) -> Result<String, CloudFormationError> {
        if let Some(template_body) =
            template_body.filter(|template| !template.trim().is_empty())
        {
            return Ok(template_body);
        }

        let template_url = template_url.ok_or_else(|| {
            template_validation_error(
                "the request must contain either TemplateBody or TemplateURL",
            )
        })?;
        self.providers.fetch_template_from_url(scope, &template_url)
    }
}

impl CloudFormationProviderEngine {
    fn fetch_template_from_url(
        &self,
        scope: &CloudFormationScope,
        template_url: &str,
    ) -> Result<String, CloudFormationError> {
        let s3 = self.dependencies.s3.as_ref().ok_or_else(|| {
            CloudFormationError::UnsupportedOperation {
                message:
                    "TemplateURL requires the S3 provider dependencies to be configured."
                        .to_owned(),
            }
        })?;
        let without_scheme = template_url
            .split_once("://")
            .map(|(_, remainder)| remainder)
            .unwrap_or(template_url);
        let (host, path) =
            without_scheme.split_once('/').ok_or_else(|| {
                template_validation_error(format!(
                    "TemplateURL {template_url} is not a valid S3 URL"
                ))
            })?;
        let (bucket, key) = if host.contains(".s3.") {
            (
                host.split('.').next().unwrap_or_default().to_owned(),
                path.to_owned(),
            )
        } else {
            let (bucket, key) = path.split_once('/').ok_or_else(|| {
                template_validation_error(format!(
                    "TemplateURL {template_url} must contain a bucket and object key"
                ))
            })?;
            (bucket.to_owned(), key.to_owned())
        };
        let object = s3
            .get_object(
                &S3Scope::new(
                    scope.account_id().clone(),
                    scope.region().clone(),
                ),
                &bucket,
                &key,
                None,
            )
            .map_err(|error| CloudFormationError::Validation {
                message: format!(
                    "Template format error: failed to fetch template from TemplateURL {template_url}: {error}"
                ),
            })?;

        String::from_utf8(object.body).map_err(|_| {
            template_validation_error(format!(
                "template fetched from TemplateURL {template_url} is not valid UTF-8"
            ))
        })
    }
}

impl CloudFormationService {
    fn prepare_operation(
        &self,
        scope: &CloudFormationScope,
        stack_name: String,
        template_body: Option<String>,
        template_url: Option<String>,
        parameters: BTreeMap<String, String>,
        capabilities: Vec<String>,
    ) -> Result<PreparedOperation, CloudFormationError> {
        if stack_name.trim().is_empty() {
            return Err(template_validation_error(
                "stack operations require a non-empty stack name",
            ));
        }

        let template_body =
            self.template_body(scope, template_body, template_url)?;
        let document = parse_template_document(&template_body)?;
        validate_template_document(&document)?;
        let existing_stack = self
            .state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .stacks
            .get(&CloudFormationStackKey::new(scope, &stack_name))
            .cloned();
        let mut effective_parameters = existing_stack
            .as_ref()
            .map(|stack| stack.parameters.clone())
            .unwrap_or_default();
        effective_parameters.extend(parameters);
        let parameter_values = planned_parameter_values(
            &document.parameters,
            &effective_parameters,
        )?;
        let stack_id = existing_stack
            .as_ref()
            .map(|stack| stack.stack_id.clone())
            .unwrap_or_else(|| planned_stack_id(scope, &stack_name));
        let base_context = PlanningContext {
            account_id: scope.account_id(),
            region: scope.region(),
            resources: &document.resources,
            stack_id: &stack_id,
            stack_name: &stack_name,
            parameters: &parameter_values,
            conditions: None,
        };
        let conditions =
            evaluate_conditions(&document.conditions, &base_context)?;
        let required = required_capabilities(&document.resources);
        ensure_capabilities_satisfied(
            &required.0,
            required.1.as_deref(),
            &capabilities,
        )?;
        let resource_order =
            topological_resource_order(&document.resources, &conditions)?;
        let changes = build_change_summaries(
            existing_stack.as_ref(),
            &document.resources,
            &conditions,
            &resource_order,
        )?;

        Ok(PreparedOperation {
            capabilities: required.0,
            conditions,
            changes,
            description: document.description.clone(),
            document,
            parameters: parameter_values,
            resource_order,
            stack_id,
            stack_name,
            template_body,
        })
    }

    fn create_change_set_internal(
        &self,
        scope: &CloudFormationScope,
        input: CloudFormationCreateChangeSetInput,
    ) -> Result<CloudFormationCreateChangeSetOutput, CloudFormationError> {
        let stack_key = CloudFormationStackKey::new(scope, &input.stack_name);
        let existing_stack = self
            .state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .stacks
            .get(&stack_key)
            .cloned();
        let change_set_type = input
            .change_set_type
            .unwrap_or_else(|| {
                if existing_stack.is_some() {
                    "UPDATE".to_owned()
                } else {
                    "CREATE".to_owned()
                }
            })
            .to_ascii_uppercase();
        match (change_set_type.as_str(), existing_stack.as_ref()) {
            ("CREATE", Some(stack))
                if stack.stack_status != "REVIEW_IN_PROGRESS"
                    || !stack.resources.is_empty() =>
            {
                return Err(CloudFormationError::AlreadyExists {
                    message: format!(
                        "Stack [{}] already exists",
                        input.stack_name
                    ),
                });
            }
            ("UPDATE", None) => {
                return Err(stack_not_found_error(&input.stack_name));
            }
            ("IMPORT", _) => {
                return Err(CloudFormationError::UnsupportedOperation {
                    message:
                        "CloudFormation change set type IMPORT is not supported."
                            .to_owned(),
                });
            }
            _ => {}
        }

        let prepared = self.prepare_operation(
            scope,
            input.stack_name.clone(),
            input.template_body,
            input.template_url,
            input.parameters,
            input.capabilities,
        )?;
        let now = self.now_epoch_seconds()?;
        let change_set_id = format!(
            "arn:aws:cloudformation:{}:{}:changeSet/{}/{}",
            scope.region(),
            scope.account_id(),
            input.change_set_name,
            short_hash(&format!(
                "{}:{}:{}",
                scope.account_id(),
                prepared.stack_name,
                input.change_set_name,
            )),
        );
        let mut guard =
            self.state.lock().unwrap_or_else(|poison| poison.into_inner());
        let stack =
            guard.stacks.entry(stack_key).or_insert_with(|| StoredStack {
                capabilities: Vec::new(),
                creation_time_epoch_seconds: now,
                description: None,
                event_counter: 0,
                events: Vec::new(),
                change_sets: BTreeMap::new(),
                last_updated_time_epoch_seconds: None,
                outputs: BTreeMap::new(),
                parameters: BTreeMap::new(),
                resource_order: Vec::new(),
                resources: BTreeMap::new(),
                stack_id: prepared.stack_id.clone(),
                stack_name: prepared.stack_name.clone(),
                stack_status: "REVIEW_IN_PROGRESS".to_owned(),
                stack_status_reason: None,
                template_body: prepared.template_body.clone(),
            });
        if stack.change_sets.contains_key(&input.change_set_name) {
            return Err(CloudFormationError::AlreadyExists {
                message: format!(
                    "ChangeSet [{}] already exists",
                    input.change_set_name
                ),
            });
        }
        stack.change_sets.insert(
            input.change_set_name.clone(),
            StoredChangeSet {
                changes: prepared.changes.clone(),
                change_set_id: change_set_id.clone(),
                change_set_name: input.change_set_name.clone(),
                change_set_type,
                creation_time_epoch_seconds: now,
                execution_status: "AVAILABLE".to_owned(),
                prepared: prepared.clone(),
                stack_id: prepared.stack_id.clone(),
                stack_name: prepared.stack_name.clone(),
                status: "CREATE_COMPLETE".to_owned(),
                status_reason: None,
            },
        );

        Ok(CloudFormationCreateChangeSetOutput {
            id: change_set_id,
            stack_id: prepared.stack_id,
        })
    }

    fn describe_change_set_internal(
        &self,
        scope: &CloudFormationScope,
        stack_name: &str,
        change_set_name: &str,
    ) -> Result<CloudFormationChangeSetDescription, CloudFormationError> {
        let guard =
            self.state.lock().unwrap_or_else(|poison| poison.into_inner());
        let stack = guard
            .stacks
            .get(&CloudFormationStackKey::new(scope, stack_name))
            .ok_or_else(|| stack_not_found_error(stack_name))?;
        let change_set =
            stack.change_sets.get(change_set_name).ok_or_else(|| {
                CloudFormationError::ChangeSetNotFound {
                    message: format!(
                        "ChangeSet [{change_set_name}] does not exist"
                    ),
                }
            })?;

        Ok(change_set_description(change_set))
    }

    fn execute_change_set_internal(
        &self,
        scope: &CloudFormationScope,
        stack_name: &str,
        change_set_name: &str,
    ) -> Result<(), CloudFormationError> {
        let change_set = {
            let guard =
                self.state.lock().unwrap_or_else(|poison| poison.into_inner());
            let stack = guard
                .stacks
                .get(&CloudFormationStackKey::new(scope, stack_name))
                .ok_or_else(|| stack_not_found_error(stack_name))?;
            stack.change_sets.get(change_set_name).cloned().ok_or_else(
                || CloudFormationError::ChangeSetNotFound {
                    message: format!(
                        "ChangeSet [{change_set_name}] does not exist"
                    ),
                },
            )?
        };

        let result = self.execute_prepared(
            scope,
            change_set.prepared.clone(),
            change_set.change_set_type == "UPDATE",
        );
        let mut guard =
            self.state.lock().unwrap_or_else(|poison| poison.into_inner());
        if let Some(stack) = guard
            .stacks
            .get_mut(&CloudFormationStackKey::new(scope, stack_name))
            && let Some(stored_change_set) =
                stack.change_sets.get_mut(change_set_name)
        {
            match &result {
                Ok(_) => {
                    stored_change_set.execution_status =
                        "EXECUTE_COMPLETE".to_owned();
                    stored_change_set.status = "EXECUTE_COMPLETE".to_owned();
                    stored_change_set.status_reason = None;
                }
                Err(error) => {
                    stored_change_set.execution_status =
                        "EXECUTE_FAILED".to_owned();
                    stored_change_set.status = "FAILED".to_owned();
                    stored_change_set.status_reason = Some(error.to_string());
                }
            }
        }

        result.map(|_| ())
    }

    fn list_change_sets_internal(
        &self,
        scope: &CloudFormationScope,
        stack_name: &str,
    ) -> Result<Vec<CloudFormationChangeSetSummary>, CloudFormationError> {
        let guard =
            self.state.lock().unwrap_or_else(|poison| poison.into_inner());
        let stack = guard
            .stacks
            .get(&CloudFormationStackKey::new(scope, stack_name))
            .ok_or_else(|| stack_not_found_error(stack_name))?;
        let mut change_sets = stack
            .change_sets
            .values()
            .map(change_set_summary)
            .collect::<Vec<_>>();
        change_sets.sort_by(|left, right| {
            left.creation_time_epoch_seconds
                .cmp(&right.creation_time_epoch_seconds)
        });

        Ok(change_sets)
    }

    fn execute_prepared(
        &self,
        scope: &CloudFormationScope,
        prepared: PreparedOperation,
        update: bool,
    ) -> Result<StoredStack, CloudFormationError> {
        let key = CloudFormationStackKey::new(scope, &prepared.stack_name);
        let existing_stack = self
            .state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .stacks
            .get(&key)
            .cloned();
        let now = self.now_epoch_seconds()?;
        let mut stack = match (update, existing_stack) {
            (true, Some(stack)) => stack,
            (true, None) => {
                return Err(stack_not_found_error(&prepared.stack_name));
            }
            (false, Some(stack))
                if stack.stack_status == "REVIEW_IN_PROGRESS"
                    && stack.resources.is_empty() =>
            {
                stack
            }
            (false, Some(_)) => {
                return Err(CloudFormationError::AlreadyExists {
                    message: format!(
                        "Stack [{}] already exists",
                        prepared.stack_name
                    ),
                });
            }
            (false, None) => StoredStack {
                capabilities: Vec::new(),
                creation_time_epoch_seconds: now,
                description: None,
                event_counter: 0,
                events: Vec::new(),
                change_sets: BTreeMap::new(),
                last_updated_time_epoch_seconds: None,
                outputs: BTreeMap::new(),
                parameters: BTreeMap::new(),
                resource_order: Vec::new(),
                resources: BTreeMap::new(),
                stack_id: prepared.stack_id.clone(),
                stack_name: prepared.stack_name.clone(),
                stack_status: "REVIEW_IN_PROGRESS".to_owned(),
                stack_status_reason: None,
                template_body: prepared.template_body.clone(),
            },
        };
        let previous_resources = stack.resources.clone();
        let mut resolved_resources = previous_resources
            .values()
            .map(|resource| {
                (
                    resource.logical_resource_id.clone(),
                    ResolvedResourceValues {
                        attributes: resource.attributes.clone(),
                        ref_value: resource.ref_value.clone(),
                        resource_type: resource.resource_type.clone(),
                    },
                )
            })
            .collect::<BTreeMap<_, _>>();
        let mut next_resources = BTreeMap::new();
        let in_progress_status =
            if update { "UPDATE_IN_PROGRESS" } else { "CREATE_IN_PROGRESS" };
        stack.stack_status = in_progress_status.to_owned();
        stack.stack_status_reason = None;
        stack.last_updated_time_epoch_seconds = Some(now);
        push_stack_event(
            &mut stack,
            &prepared.stack_name,
            Some(prepared.stack_id.clone()),
            "AWS::CloudFormation::Stack",
            in_progress_status,
            None,
            now,
        );

        let apply_result = (|| {
            for logical_id in &prepared.resource_order {
                let resource = prepared
                    .document
                    .resources
                    .get(logical_id)
                    .ok_or_else(|| {
                        template_validation_error(format!(
                            "planned resource order references unknown resource {logical_id}"
                        ))
                    })?;
                if !resource_enabled(
                    resource.condition.as_deref(),
                    &prepared.conditions,
                )? {
                    continue;
                }
                let runtime_context = RuntimeContext {
                    account_id: scope.account_id(),
                    conditions: &prepared.conditions,
                    parameters: &prepared.parameters,
                    region: scope.region(),
                    resources: &resolved_resources,
                    stack_id: &prepared.stack_id,
                    stack_name: &prepared.stack_name,
                };
                let properties = evaluate_runtime_value(
                    &resource.properties,
                    &runtime_context,
                    &resource_path(logical_id, "Properties"),
                )?
                .into_json()
                .unwrap_or(Value::Object(Map::new()));
                push_stack_event(
                    &mut stack,
                    logical_id,
                    None,
                    &resource.resource_type,
                    if update && previous_resources.contains_key(logical_id) {
                        "UPDATE_IN_PROGRESS"
                    } else {
                        "CREATE_IN_PROGRESS"
                    },
                    None,
                    now,
                );
                let mut stored =
                    self.providers.apply_resource(ApplyResourceInput {
                        existing: previous_resources.get(logical_id),
                        logical_id,
                        properties: &properties,
                        resource_type: &resource.resource_type,
                        scope,
                        stack_name: &prepared.stack_name,
                        template_properties: &resource.properties,
                    })?;
                stored.last_updated_timestamp_epoch_seconds = now;
                stored.resource_status =
                    if update && previous_resources.contains_key(logical_id) {
                        "UPDATE_COMPLETE".to_owned()
                    } else {
                        "CREATE_COMPLETE".to_owned()
                    };
                push_stack_event(
                    &mut stack,
                    logical_id,
                    Some(stored.physical_resource_id.clone()),
                    &stored.resource_type,
                    &stored.resource_status,
                    stored.resource_status_reason.clone(),
                    now,
                );
                resolved_resources.insert(
                    logical_id.clone(),
                    ResolvedResourceValues {
                        attributes: stored.attributes.clone(),
                        ref_value: stored.ref_value.clone(),
                        resource_type: stored.resource_type.clone(),
                    },
                );
                next_resources.insert(logical_id.clone(), stored);
            }

            let desired_resources =
                next_resources.keys().cloned().collect::<BTreeSet<_>>();
            let previous_order = stack.resource_order.clone();
            for logical_id in previous_order.iter().rev() {
                if desired_resources.contains(logical_id) {
                    continue;
                }
                if let Some(resource) = previous_resources.get(logical_id) {
                    push_stack_event(
                        &mut stack,
                        logical_id,
                        Some(resource.physical_resource_id.clone()),
                        &resource.resource_type,
                        "DELETE_IN_PROGRESS",
                        None,
                        now,
                    );
                    self.providers.delete_resource(scope, resource)?;
                    push_stack_event(
                        &mut stack,
                        logical_id,
                        Some(resource.physical_resource_id.clone()),
                        &resource.resource_type,
                        "DELETE_COMPLETE",
                        None,
                        now,
                    );
                }
            }

            let runtime_context = RuntimeContext {
                account_id: scope.account_id(),
                conditions: &prepared.conditions,
                parameters: &prepared.parameters,
                region: scope.region(),
                resources: &resolved_resources,
                stack_id: &prepared.stack_id,
                stack_name: &prepared.stack_name,
            };
            let mut outputs = BTreeMap::new();
            for (logical_id, output) in &prepared.document.outputs {
                if !resource_enabled(
                    output.condition.as_deref(),
                    &prepared.conditions,
                )? {
                    continue;
                }
                let value = evaluate_runtime_value(
                    &output.value,
                    &runtime_context,
                    &output_path(logical_id),
                )?
                .to_string_value(&output_path(logical_id))?;
                outputs.insert(
                    logical_id.clone(),
                    StoredStackOutput {
                        description: output.description.clone(),
                        value,
                    },
                );
            }
            stack.capabilities = prepared.capabilities.clone();
            stack.description = prepared.description.clone();
            stack.outputs = outputs;
            stack.parameters = prepared.parameters.clone();
            stack.resource_order = prepared
                .resource_order
                .iter()
                .filter(|logical_id| next_resources.contains_key(*logical_id))
                .cloned()
                .collect();
            stack.resources = next_resources;
            stack.stack_status = if update {
                "UPDATE_COMPLETE".to_owned()
            } else {
                "CREATE_COMPLETE".to_owned()
            };
            stack.template_body = prepared.template_body.clone();
            let stack_status = stack.stack_status.clone();
            push_stack_event(
                &mut stack,
                &prepared.stack_name,
                Some(prepared.stack_id.clone()),
                "AWS::CloudFormation::Stack",
                &stack_status,
                None,
                now,
            );

            Ok::<(), CloudFormationError>(())
        })();

        if let Err(error) = apply_result {
            let status =
                if update { "UPDATE_FAILED" } else { "CREATE_FAILED" };
            stack.stack_status = status.to_owned();
            stack.stack_status_reason = Some(error.to_string());
            stack.outputs.clear();
            for (logical_id, resource) in previous_resources {
                stack.resources.entry(logical_id).or_insert(resource);
            }
            push_stack_event(
                &mut stack,
                &prepared.stack_name,
                Some(prepared.stack_id.clone()),
                "AWS::CloudFormation::Stack",
                status,
                Some(error.to_string()),
                now,
            );
            self.state
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .stacks
                .insert(key, stack);
            return Err(error);
        }

        self.state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .stacks
            .insert(key, stack.clone());

        Ok(stack)
    }

    fn delete_stack_internal(
        &self,
        scope: &CloudFormationScope,
        stack_name: &str,
    ) -> Result<(), CloudFormationError> {
        let key = CloudFormationStackKey::new(scope, stack_name);
        let existing = self
            .state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .stacks
            .get(&key)
            .cloned();
        let Some(mut stack) = existing else {
            return Ok(());
        };
        let now = self.now_epoch_seconds()?;
        stack.stack_status = "DELETE_IN_PROGRESS".to_owned();
        stack.stack_status_reason = None;
        let stack_id = stack.stack_id.clone();
        push_stack_event(
            &mut stack,
            stack_name,
            Some(stack_id),
            "AWS::CloudFormation::Stack",
            "DELETE_IN_PROGRESS",
            None,
            now,
        );
        let result = (|| {
            let resource_order = stack.resource_order.clone();
            for logical_id in resource_order.iter().rev() {
                let Some(resource) = stack.resources.get(logical_id).cloned()
                else {
                    continue;
                };
                let physical_resource_id =
                    resource.physical_resource_id.clone();
                let resource_type = resource.resource_type.clone();
                push_stack_event(
                    &mut stack,
                    logical_id,
                    Some(physical_resource_id.clone()),
                    &resource_type,
                    "DELETE_IN_PROGRESS",
                    None,
                    now,
                );
                self.providers.delete_resource(scope, &resource)?;
                push_stack_event(
                    &mut stack,
                    logical_id,
                    Some(physical_resource_id),
                    &resource_type,
                    "DELETE_COMPLETE",
                    None,
                    now,
                );
            }

            Ok::<(), CloudFormationError>(())
        })();

        match result {
            Ok(()) => {
                stack.stack_status = "DELETE_COMPLETE".to_owned();
                let stack_id = stack.stack_id.clone();
                push_stack_event(
                    &mut stack,
                    stack_name,
                    Some(stack_id),
                    "AWS::CloudFormation::Stack",
                    "DELETE_COMPLETE",
                    None,
                    now,
                );
                self.state
                    .lock()
                    .unwrap_or_else(|poison| poison.into_inner())
                    .stacks
                    .remove(&key);
                Ok(())
            }
            Err(error) => {
                stack.stack_status = "DELETE_FAILED".to_owned();
                stack.stack_status_reason = Some(error.to_string());
                self.state
                    .lock()
                    .unwrap_or_else(|poison| poison.into_inner())
                    .stacks
                    .insert(key, stack);
                Err(error)
            }
        }
    }

    fn describe_stacks_internal(
        &self,
        scope: &CloudFormationScope,
        stack_name: Option<&str>,
    ) -> Result<Vec<CloudFormationStackDescription>, CloudFormationError> {
        let guard =
            self.state.lock().unwrap_or_else(|poison| poison.into_inner());
        if let Some(stack_name) = stack_name {
            let stack = guard
                .stacks
                .get(&CloudFormationStackKey::new(scope, stack_name))
                .ok_or_else(|| stack_not_found_error(stack_name))?;
            return Ok(vec![stack_description(stack)]);
        }
        let mut stacks = guard
            .stacks
            .iter()
            .filter(|(key, _)| {
                key.account_id == *scope.account_id()
                    && key.region == *scope.region()
            })
            .map(|(_, stack)| stack_description(stack))
            .collect::<Vec<_>>();
        stacks.sort_by(|left, right| {
            left.creation_time_epoch_seconds
                .cmp(&right.creation_time_epoch_seconds)
        });
        Ok(stacks)
    }

    fn list_stacks_internal(
        &self,
        scope: &CloudFormationScope,
    ) -> Vec<CloudFormationStackDescription> {
        self.describe_stacks_internal(scope, None)
            .unwrap_or_else(|_| Vec::new())
    }

    fn describe_stack_events_internal(
        &self,
        scope: &CloudFormationScope,
        stack_name: &str,
    ) -> Result<Vec<CloudFormationStackEvent>, CloudFormationError> {
        let guard =
            self.state.lock().unwrap_or_else(|poison| poison.into_inner());
        let stack = guard
            .stacks
            .get(&CloudFormationStackKey::new(scope, stack_name))
            .ok_or_else(|| stack_not_found_error(stack_name))?;
        Ok(stack
            .events
            .iter()
            .rev()
            .map(|event| CloudFormationStackEvent {
                event_id: event.event_id.clone(),
                logical_resource_id: event.logical_resource_id.clone(),
                physical_resource_id: event.physical_resource_id.clone(),
                resource_status: event.resource_status.clone(),
                resource_status_reason: event.resource_status_reason.clone(),
                resource_type: event.resource_type.clone(),
                stack_id: stack.stack_id.clone(),
                stack_name: stack.stack_name.clone(),
                timestamp_epoch_seconds: event.timestamp_epoch_seconds,
            })
            .collect())
    }

    fn describe_stack_resources_internal(
        &self,
        scope: &CloudFormationScope,
        stack_name: &str,
    ) -> Result<Vec<CloudFormationStackResourceDescription>, CloudFormationError>
    {
        let guard =
            self.state.lock().unwrap_or_else(|poison| poison.into_inner());
        let stack = guard
            .stacks
            .get(&CloudFormationStackKey::new(scope, stack_name))
            .ok_or_else(|| stack_not_found_error(stack_name))?;
        Ok(stack
            .resource_order
            .iter()
            .filter_map(|logical_id| {
                stack
                    .resources
                    .get(logical_id)
                    .map(|resource| resource_description(stack, resource))
            })
            .collect())
    }

    fn describe_stack_resource_internal(
        &self,
        scope: &CloudFormationScope,
        stack_name: &str,
        logical_resource_id: &str,
    ) -> Result<CloudFormationStackResourceDescription, CloudFormationError>
    {
        let guard =
            self.state.lock().unwrap_or_else(|poison| poison.into_inner());
        let stack = guard
            .stacks
            .get(&CloudFormationStackKey::new(scope, stack_name))
            .ok_or_else(|| stack_not_found_error(stack_name))?;
        let resource =
            stack.resources.get(logical_resource_id).ok_or_else(|| {
                CloudFormationError::Validation {
                    message: format!(
                        "Stack resource {logical_resource_id} does not exist for stack {stack_name}"
                    ),
                }
            })?;

        Ok(resource_description(stack, resource))
    }

    fn list_stack_resources_internal(
        &self,
        scope: &CloudFormationScope,
        stack_name: &str,
    ) -> Result<Vec<CloudFormationStackResourceSummary>, CloudFormationError>
    {
        let resources =
            self.describe_stack_resources_internal(scope, stack_name)?;
        Ok(resources
            .into_iter()
            .map(|resource| CloudFormationStackResourceSummary {
                last_updated_timestamp_epoch_seconds: resource
                    .last_updated_timestamp_epoch_seconds,
                logical_resource_id: resource.logical_resource_id,
                physical_resource_id: resource.physical_resource_id,
                resource_status: resource.resource_status,
                resource_status_reason: resource.resource_status_reason,
                resource_type: resource.resource_type,
            })
            .collect())
    }

    fn get_template_internal(
        &self,
        scope: &CloudFormationScope,
        stack_name: &str,
    ) -> Result<String, CloudFormationError> {
        let guard =
            self.state.lock().unwrap_or_else(|poison| poison.into_inner());
        let stack = guard
            .stacks
            .get(&CloudFormationStackKey::new(scope, stack_name))
            .ok_or_else(|| stack_not_found_error(stack_name))?;
        Ok(stack.template_body.clone())
    }

    fn now_epoch_seconds(&self) -> Result<u64, CloudFormationError> {
        (self.clock)()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_secs())
            .map_err(|error| CloudFormationError::Validation {
                message: format!("time moved backwards: {error}"),
            })
    }
}

impl CloudFormationProviderEngine {
    fn apply_resource(
        &self,
        input: ApplyResourceInput<'_>,
    ) -> Result<StoredStackResource, CloudFormationError> {
        let properties_map =
            input.properties.as_object().ok_or_else(|| {
                template_validation_error(format!(
                    "resolved properties for {} must be an object",
                    input.logical_id
                ))
            })?;
        match input.resource_type {
            "AWS::S3::Bucket" => self.apply_s3_bucket(
                input.scope,
                input.stack_name,
                input.logical_id,
                properties_map,
                input.template_properties,
                input.existing,
            ),
            "AWS::S3::BucketPolicy" => self.apply_s3_bucket_policy(
                input.scope,
                input.logical_id,
                properties_map,
                input.template_properties,
            ),
            "AWS::SQS::Queue" => self.apply_sqs_queue(
                input.scope,
                input.stack_name,
                input.logical_id,
                properties_map,
                input.template_properties,
                input.existing,
            ),
            "AWS::SQS::QueuePolicy" => self.apply_sqs_queue_policy(
                input.scope,
                input.logical_id,
                properties_map,
                input.template_properties,
            ),
            "AWS::SNS::Topic" => self.apply_sns_topic(
                input.scope,
                input.stack_name,
                input.logical_id,
                properties_map,
                input.template_properties,
            ),
            "AWS::DynamoDB::Table" => self.apply_dynamodb_table(
                input.scope,
                input.stack_name,
                input.logical_id,
                properties_map,
                input.template_properties,
                input.existing,
            ),
            "AWS::Lambda::Function" => self.apply_lambda_function(
                input.scope,
                input.stack_name,
                input.logical_id,
                properties_map,
                input.template_properties,
                input.existing,
            ),
            "AWS::IAM::Role" => self.apply_iam_role(
                input.scope,
                input.stack_name,
                input.logical_id,
                properties_map,
                input.template_properties,
                input.existing,
            ),
            "AWS::IAM::User" => self.apply_iam_user(
                input.scope,
                input.stack_name,
                input.logical_id,
                properties_map,
                input.template_properties,
            ),
            "AWS::IAM::AccessKey" => self.apply_iam_access_key(
                input.scope,
                input.logical_id,
                properties_map,
                input.template_properties,
                input.existing,
            ),
            "AWS::IAM::Policy" => self.apply_iam_inline_policy(
                input.scope,
                input.stack_name,
                input.logical_id,
                properties_map,
                input.template_properties,
                input.existing,
            ),
            "AWS::IAM::ManagedPolicy" => self.apply_iam_managed_policy(
                input.scope,
                input.stack_name,
                input.logical_id,
                properties_map,
                input.template_properties,
                input.existing,
            ),
            "AWS::IAM::InstanceProfile" => self.apply_iam_instance_profile(
                input.scope,
                input.stack_name,
                input.logical_id,
                properties_map,
                input.template_properties,
                input.existing,
            ),
            "AWS::SSM::Parameter" => self.apply_ssm_parameter(
                input.scope,
                input.stack_name,
                input.logical_id,
                properties_map,
                input.template_properties,
            ),
            "AWS::KMS::Key" => self.apply_kms_key(
                input.scope,
                input.logical_id,
                properties_map,
                input.template_properties,
                input.existing,
            ),
            "AWS::KMS::Alias" => self.apply_kms_alias(
                input.scope,
                input.logical_id,
                properties_map,
                input.template_properties,
                input.existing,
            ),
            "AWS::SecretsManager::Secret" => self.apply_secret(
                input.scope,
                input.stack_name,
                input.logical_id,
                properties_map,
                input.template_properties,
                input.existing,
            ),
            _ => Err(template_validation_error(format!(
                "resource {} uses unsupported type {}",
                input.logical_id, input.resource_type
            ))),
        }
    }

    fn delete_resource(
        &self,
        scope: &CloudFormationScope,
        resource: &StoredStackResource,
    ) -> Result<(), CloudFormationError> {
        match resource.resource_type.as_str() {
            "AWS::S3::Bucket" => {
                let s3 = require_dependency(&self.dependencies.s3, "S3")?;
                let scope = S3Scope::new(
                    scope.account_id().clone(),
                    scope.region().clone(),
                );
                let _ = s3.delete_bucket_policy(
                    &scope,
                    &resource.physical_resource_id,
                );
                s3.delete_bucket(&scope, &resource.physical_resource_id)
                    .map_err(map_s3_error)
            }
            "AWS::S3::BucketPolicy" => {
                let s3 = require_dependency(&self.dependencies.s3, "S3")?;
                let scope = S3Scope::new(
                    scope.account_id().clone(),
                    scope.region().clone(),
                );
                let bucket = required_string_field_from_value(
                    &resource.properties,
                    "Bucket",
                )?;
                let _ = s3.delete_bucket_policy(&scope, &bucket);
                Ok(())
            }
            "AWS::SQS::Queue" => {
                let sqs = require_dependency(&self.dependencies.sqs, "SQS")?;
                sqs.delete_queue(&queue_identity_from_ref(
                    scope,
                    &resource.ref_value,
                )?)
                .map_err(map_sqs_error)
            }
            "AWS::SQS::QueuePolicy" => {
                let sqs = require_dependency(&self.dependencies.sqs, "SQS")?;
                let queues = string_list_field_from_value(
                    &resource.properties,
                    "Queues",
                )?;
                for queue in queues {
                    let _ = sqs.set_queue_attributes(
                        &queue_identity_from_ref(scope, &queue)?,
                        BTreeMap::from([(
                            String::from("Policy"),
                            String::new(),
                        )]),
                    );
                }
                Ok(())
            }
            "AWS::SNS::Topic" => {
                let sns = require_dependency(&self.dependencies.sns, "SNS")?;
                sns.delete_topic(
                    &resource
                        .physical_resource_id
                        .parse()
                        .map_err(|_| CloudFormationError::Validation {
                            message: format!(
                                "Stored SNS physical resource id {} is not a valid ARN.",
                                resource.physical_resource_id
                            ),
                        })?,
                )
                    .map_err(map_sns_error)
            }
            "AWS::DynamoDB::Table" => {
                let dynamodb = require_dependency(
                    &self.dependencies.dynamodb,
                    "DynamoDB",
                )?;
                dynamodb
                    .delete_table(
                        &DynamoDbScope::new(
                            scope.account_id().clone(),
                            scope.region().clone(),
                        ),
                        &resource.physical_resource_id,
                    )
                    .map(|_| ())
                    .map_err(map_dynamodb_error)
            }
            "AWS::Lambda::Function" => {
                let lambda =
                    require_dependency(&self.dependencies.lambda, "Lambda")?;
                lambda
                    .delete_function(
                        &LambdaScope::new(
                            scope.account_id().clone(),
                            scope.region().clone(),
                        ),
                        &resource.physical_resource_id,
                    )
                    .map_err(map_lambda_error)
            }
            "AWS::IAM::Role" => self.delete_iam_role(scope, resource),
            "AWS::IAM::User" => self.delete_iam_user(scope, resource),
            "AWS::IAM::AccessKey" => {
                self.delete_iam_access_key(scope, resource)
            }
            "AWS::IAM::Policy" => {
                self.delete_iam_inline_policy(scope, resource)
            }
            "AWS::IAM::ManagedPolicy" => {
                self.delete_iam_managed_policy(scope, resource)
            }
            "AWS::IAM::InstanceProfile" => {
                self.delete_iam_instance_profile(scope, resource)
            }
            "AWS::SSM::Parameter" => {
                let ssm = require_dependency(&self.dependencies.ssm, "SSM")?;
                ssm.delete_parameter(
                    &SsmScope::new(
                        scope.account_id().clone(),
                        scope.region().clone(),
                    ),
                    SsmDeleteParameterInput {
                        name: SsmParameterName::parse(
                            &resource.physical_resource_id,
                        )
                        .map_err(map_ssm_error)?,
                    },
                )
                .map(|_| ())
                .map_err(map_ssm_error)
            }
            "AWS::KMS::Key" => {
                let kms = require_dependency(&self.dependencies.kms, "KMS")?;
                kms.schedule_key_deletion(
                    &KmsScope::new(
                        scope.account_id().clone(),
                        scope.region().clone(),
                    ),
                    ScheduleKmsKeyDeletionInput {
                        key_id: resource
                            .physical_resource_id
                            .parse::<KmsKeyReference>()
                            .map_err(|error| {
                                CloudFormationError::Validation {
                                    message: error.to_string(),
                                }
                            })?,
                        pending_window_in_days: None,
                    },
                )
                .map(|_| ())
                .map_err(map_kms_error)
            }
            "AWS::KMS::Alias" => {
                let kms = require_dependency(&self.dependencies.kms, "KMS")?;
                kms.delete_alias(
                    &KmsScope::new(
                        scope.account_id().clone(),
                        scope.region().clone(),
                    ),
                    DeleteKmsAliasInput {
                        alias_name: resource
                            .physical_resource_id
                            .parse::<KmsAliasName>()
                            .map_err(|error| {
                                CloudFormationError::Validation {
                                    message: error.to_string(),
                                }
                            })?,
                    },
                )
                .map_err(map_kms_error)
            }
            "AWS::SecretsManager::Secret" => {
                let secrets_manager = require_dependency(
                    &self.dependencies.secrets_manager,
                    "Secrets Manager",
                )?;
                secrets_manager
                    .delete_secret(
                        &SecretsManagerScope::new(
                            scope.account_id().clone(),
                            scope.region().clone(),
                        ),
                        DeleteSecretInput {
                            force_delete_without_recovery: true,
                            recovery_window_in_days: None,
                            secret_id: resource
                                .physical_resource_id
                                .parse::<SecretsManagerSecretReference>()
                                .map_err(|error| {
                                    CloudFormationError::Validation {
                                        message: error.to_string(),
                                    }
                                })?,
                        },
                    )
                    .map(|_| ())
                    .map_err(map_secrets_manager_error)
            }
            other => Err(CloudFormationError::UnsupportedOperation {
                message: format!(
                    "CloudFormation provider {other} cannot delete resources."
                ),
            }),
        }
    }

    fn apply_s3_bucket(
        &self,
        scope: &CloudFormationScope,
        stack_name: &str,
        logical_id: &str,
        properties: &Map<String, Value>,
        template_properties: &Value,
        existing: Option<&StoredStackResource>,
    ) -> Result<StoredStackResource, CloudFormationError> {
        let bucket_name = optional_string_field(properties, "BucketName")?
            .unwrap_or_else(|| generated_bucket_name(stack_name, logical_id));
        let object_lock_enabled =
            optional_bool_field(properties, "ObjectLockEnabled")?
                .unwrap_or(false);
        let s3 = require_dependency(&self.dependencies.s3, "S3")?;
        let scope =
            S3Scope::new(scope.account_id().clone(), scope.region().clone());
        if let Some(existing_resource) = existing
            .filter(|resource| resource.physical_resource_id != bucket_name)
        {
            self.delete_resource(
                &CloudFormationScope::new(
                    scope.account_id().clone(),
                    scope.region().clone(),
                ),
                existing_resource,
            )?;
        }
        if existing.is_none()
            || existing.is_some_and(|resource| {
                resource.physical_resource_id != bucket_name
            })
        {
            s3.create_bucket(
                &scope,
                CreateBucketInput {
                    name: bucket_name.clone(),
                    object_lock_enabled,
                    region: scope.region().clone(),
                },
            )
            .map_err(map_s3_error)?;
        }
        let mut attributes = BTreeMap::new();
        attributes
            .insert("Arn".to_owned(), format!("arn:aws:s3:::{bucket_name}"));
        attributes.insert(
            "DomainName".to_owned(),
            format!("{bucket_name}.s3.amazonaws.com"),
        );
        attributes.insert(
            "DualStackDomainName".to_owned(),
            format!(
                "{bucket_name}.s3.dualstack.{}.amazonaws.com",
                scope.region()
            ),
        );
        attributes.insert(
            "RegionalDomainName".to_owned(),
            format!("{bucket_name}.s3.{}.amazonaws.com", scope.region()),
        );
        Ok(stored_resource(
            logical_id,
            "AWS::S3::Bucket",
            bucket_name.clone(),
            bucket_name,
            attributes,
            Value::Object(properties.clone()),
            template_properties.clone(),
        ))
    }

    fn apply_s3_bucket_policy(
        &self,
        scope: &CloudFormationScope,
        logical_id: &str,
        properties: &Map<String, Value>,
        template_properties: &Value,
    ) -> Result<StoredStackResource, CloudFormationError> {
        let bucket = required_string_field(properties, "Bucket")?;
        let policy = properties
            .get("PolicyDocument")
            .map(compact_json)
            .ok_or_else(|| {
                template_validation_error(format!(
                    "resource {logical_id} requires PolicyDocument"
                ))
            })?;
        let s3 = require_dependency(&self.dependencies.s3, "S3")?;
        s3.put_bucket_policy(
            &S3Scope::new(scope.account_id().clone(), scope.region().clone()),
            &bucket,
            policy,
        )
        .map_err(map_s3_error)?;
        Ok(stored_resource(
            logical_id,
            "AWS::S3::BucketPolicy",
            format!("bucket-policy:{bucket}"),
            format!("bucket-policy:{bucket}"),
            BTreeMap::new(),
            Value::Object(properties.clone()),
            template_properties.clone(),
        ))
    }

    fn apply_sqs_queue(
        &self,
        scope: &CloudFormationScope,
        stack_name: &str,
        logical_id: &str,
        properties: &Map<String, Value>,
        template_properties: &Value,
        existing: Option<&StoredStackResource>,
    ) -> Result<StoredStackResource, CloudFormationError> {
        let queue_name = optional_string_field(properties, "QueueName")?
            .unwrap_or_else(|| generated_queue_name(stack_name, logical_id));
        let queue_url = queue_ref(
            &self.dependencies.advertised_edge.current(),
            scope.account_id(),
            &queue_name,
        );
        let sqs = require_dependency(&self.dependencies.sqs, "SQS")?;
        let scope =
            SqsScope::new(scope.account_id().clone(), scope.region().clone());
        if let Some(existing_resource) = existing
            .filter(|resource| resource.physical_resource_id != queue_url)
        {
            self.delete_resource(
                &CloudFormationScope::new(
                    scope.account_id().clone(),
                    scope.region().clone(),
                ),
                existing_resource,
            )?;
        }
        let attributes = sqs_queue_attributes(properties)?;
        let queue = if existing.is_some()
            && existing.is_some_and(|resource| {
                resource.physical_resource_id == queue_url
            }) {
            let queue = queue_identity_from_ref(
                &CloudFormationScope::new(
                    scope.account_id().clone(),
                    scope.region().clone(),
                ),
                &queue_url,
            )?;
            sqs.set_queue_attributes(&queue, attributes.clone())
                .map_err(map_sqs_error)?;
            queue
        } else {
            sqs.create_queue(
                &scope,
                CreateQueueInput {
                    attributes: attributes.clone(),
                    queue_name: queue_name.clone(),
                },
            )
            .map_err(map_sqs_error)?
        };
        let queue_url = queue_ref(
            &self.dependencies.advertised_edge.current(),
            scope.account_id(),
            queue.queue_name(),
        );
        let queue_arn = format!(
            "arn:aws:sqs:{}:{}:{}",
            scope.region(),
            scope.account_id(),
            queue.queue_name()
        );
        Ok(stored_resource(
            logical_id,
            "AWS::SQS::Queue",
            queue_url.clone(),
            queue_url.clone(),
            BTreeMap::from([
                ("Arn".to_owned(), queue_arn),
                ("QueueName".to_owned(), queue.queue_name().to_owned()),
                ("QueueUrl".to_owned(), queue_url.clone()),
            ]),
            Value::Object(properties.clone()),
            template_properties.clone(),
        ))
    }

    fn apply_sqs_queue_policy(
        &self,
        scope: &CloudFormationScope,
        logical_id: &str,
        properties: &Map<String, Value>,
        template_properties: &Value,
    ) -> Result<StoredStackResource, CloudFormationError> {
        let queues = string_list_field(properties, "Queues")?;
        let policy = properties
            .get("PolicyDocument")
            .map(compact_json)
            .ok_or_else(|| {
                template_validation_error(format!(
                    "resource {logical_id} requires PolicyDocument"
                ))
            })?;
        let sqs = require_dependency(&self.dependencies.sqs, "SQS")?;
        for queue in &queues {
            sqs.set_queue_attributes(
                &queue_identity_from_ref(scope, queue)?,
                BTreeMap::from([(String::from("Policy"), policy.clone())]),
            )
            .map_err(map_sqs_error)?;
        }
        let id = format!("queue-policy-{}", short_hash(&queues.join(",")));
        Ok(stored_resource(
            logical_id,
            "AWS::SQS::QueuePolicy",
            id.clone(),
            id.clone(),
            BTreeMap::from([("Id".to_owned(), id.clone())]),
            Value::Object(properties.clone()),
            template_properties.clone(),
        ))
    }

    fn apply_sns_topic(
        &self,
        scope: &CloudFormationScope,
        stack_name: &str,
        logical_id: &str,
        properties: &Map<String, Value>,
        template_properties: &Value,
    ) -> Result<StoredStackResource, CloudFormationError> {
        let topic_name = optional_string_field(properties, "TopicName")?
            .unwrap_or_else(|| generated_topic_name(stack_name, logical_id));
        let sns = require_dependency(&self.dependencies.sns, "SNS")?;
        let topic_arn = sns
            .create_topic(
                &SnsScope::new(
                    scope.account_id().clone(),
                    scope.region().clone(),
                ),
                CreateTopicInput {
                    attributes: sns_topic_attributes(properties)?,
                    name: topic_name.clone(),
                },
            )
            .map_err(map_sns_error)?;
        Ok(stored_resource(
            logical_id,
            "AWS::SNS::Topic",
            topic_arn.to_string(),
            topic_arn.to_string(),
            BTreeMap::from([
                ("TopicArn".to_owned(), topic_arn.to_string()),
                ("TopicName".to_owned(), topic_name),
            ]),
            Value::Object(properties.clone()),
            template_properties.clone(),
        ))
    }

    fn apply_dynamodb_table(
        &self,
        scope: &CloudFormationScope,
        stack_name: &str,
        logical_id: &str,
        properties: &Map<String, Value>,
        template_properties: &Value,
        existing: Option<&StoredStackResource>,
    ) -> Result<StoredStackResource, CloudFormationError> {
        let table_name = optional_string_field(properties, "TableName")?
            .unwrap_or_else(|| generated_table_name(stack_name, logical_id));
        let dynamodb =
            require_dependency(&self.dependencies.dynamodb, "DynamoDB")?;
        let scope = DynamoDbScope::new(
            scope.account_id().clone(),
            scope.region().clone(),
        );
        if let Some(existing_resource) = existing
            .filter(|resource| resource.physical_resource_id != table_name)
        {
            self.delete_resource(
                &CloudFormationScope::new(
                    scope.account_id().clone(),
                    scope.region().clone(),
                ),
                existing_resource,
            )?;
        }
        let description = if existing.is_some()
            && existing.is_some_and(|resource| {
                resource.physical_resource_id == table_name
            }) {
            dynamodb
                .describe_table(&scope, &table_name)
                .map_err(map_dynamodb_error)?
        } else {
            let description = dynamodb
                .create_table(
                    &scope,
                    CreateDynamoDbTableInput {
                        attribute_definitions: dynamodb_attribute_definitions(
                            properties,
                        )?,
                        billing_mode: dynamodb_billing_mode(properties)?,
                        global_secondary_indexes:
                            dynamodb_global_secondary_indexes(properties)?,
                        key_schema: dynamodb_key_schema(properties)?,
                        local_secondary_indexes:
                            dynamodb_local_secondary_indexes(properties)?,
                        provisioned_throughput:
                            dynamodb_provisioned_throughput(properties)?,
                        table_name: DynamoDbTableName::parse(&table_name)
                            .map_err(map_dynamodb_error)?,
                    },
                )
                .map_err(map_dynamodb_error)?;
            if let Some(tags) = dynamodb_tags(properties)? {
                dynamodb
                    .tag_resource(
                        &scope,
                        DynamoDbTagResourceInput {
                            resource_arn: description.table_arn.clone(),
                            tags,
                        },
                    )
                    .map_err(map_dynamodb_error)?;
            }
            description
        };
        let mut attributes = BTreeMap::from([(
            "Arn".to_owned(),
            description.table_arn.clone(),
        )]);
        if let Some(stream_arn) = description.latest_stream_arn.clone() {
            attributes.insert("StreamArn".to_owned(), stream_arn);
        }
        Ok(stored_resource(
            logical_id,
            "AWS::DynamoDB::Table",
            table_name.clone(),
            table_name,
            attributes,
            Value::Object(properties.clone()),
            template_properties.clone(),
        ))
    }

    fn apply_lambda_function(
        &self,
        scope: &CloudFormationScope,
        stack_name: &str,
        logical_id: &str,
        properties: &Map<String, Value>,
        template_properties: &Value,
        existing: Option<&StoredStackResource>,
    ) -> Result<StoredStackResource, CloudFormationError> {
        let function_name = optional_string_field(properties, "FunctionName")?
            .unwrap_or_else(|| {
                generated_function_name(stack_name, logical_id)
            });
        let runtime = optional_string_field(properties, "Runtime")?
            .unwrap_or_else(|| "python3.11".to_owned());
        let handler = optional_string_field(properties, "Handler")?
            .unwrap_or_else(|| "index.handler".to_owned());
        if let Some(existing_resource) = existing
            .filter(|resource| resource.physical_resource_id != function_name)
        {
            self.delete_resource(scope, existing_resource)?;
        }
        let lambda = require_dependency(&self.dependencies.lambda, "Lambda")?;
        if existing.is_some()
            && existing.is_some_and(|resource| {
                resource.physical_resource_id == function_name
            })
        {
            let existing_resource = existing.ok_or_else(|| {
                template_validation_error(format!(
                    "resource {logical_id} is missing an existing Lambda function"
                ))
            })?;
            self.delete_resource(scope, existing_resource)?;
            lambda
                .create_function(
                    &LambdaScope::new(
                        scope.account_id().clone(),
                        scope.region().clone(),
                    ),
                    lambda_create_input(
                        scope,
                        &function_name,
                        &runtime,
                        &handler,
                        properties,
                    )?,
                )
                .map_err(map_lambda_error)?;
        } else {
            lambda
                .create_function(
                    &LambdaScope::new(
                        scope.account_id().clone(),
                        scope.region().clone(),
                    ),
                    lambda_create_input(
                        scope,
                        &function_name,
                        &runtime,
                        &handler,
                        properties,
                    )?,
                )
                .map_err(map_lambda_error)?;
        }
        Ok(stored_resource(
            logical_id,
            "AWS::Lambda::Function",
            function_name.clone(),
            function_name.clone(),
            BTreeMap::from([(
                "Arn".to_owned(),
                format!(
                    "arn:aws:lambda:{}:{}:function:{}",
                    scope.region(),
                    scope.account_id(),
                    function_name
                ),
            )]),
            Value::Object(properties.clone()),
            template_properties.clone(),
        ))
    }

    fn apply_iam_role(
        &self,
        scope: &CloudFormationScope,
        stack_name: &str,
        logical_id: &str,
        properties: &Map<String, Value>,
        template_properties: &Value,
        existing: Option<&StoredStackResource>,
    ) -> Result<StoredStackResource, CloudFormationError> {
        let role_name = optional_string_field(properties, "RoleName")?
            .unwrap_or_else(|| generated_iam_name(stack_name, logical_id));
        let iam = require_dependency(&self.dependencies.iam, "IAM")?;
        let scope =
            IamScope::new(scope.account_id().clone(), scope.region().clone());
        if let Some(existing_resource) = existing
            .filter(|resource| resource.physical_resource_id != role_name)
        {
            self.delete_resource(
                &CloudFormationScope::new(
                    scope.account_id().clone(),
                    scope.region().clone(),
                ),
                existing_resource,
            )?;
        }
        let role = match iam.create_role(
            &scope,
            CreateRoleInput {
                assume_role_policy_document: properties
                    .get("AssumeRolePolicyDocument")
                    .map(compact_json)
                    .unwrap_or_else(|| {
                        "{\"Version\":\"2012-10-17\",\"Statement\":[]}"
                            .to_owned()
                    }),
                description: optional_string_field(properties, "Description")?
                    .unwrap_or_default(),
                max_session_duration: optional_u32_field(
                    properties,
                    "MaxSessionDuration",
                )?
                .unwrap_or(3_600),
                path: optional_string_field(properties, "Path")?
                    .unwrap_or_else(|| "/".to_owned()),
                role_name: role_name.clone(),
                tags: iam_tags(properties)?,
            },
        ) {
            Ok(role) => role,
            Err(IamError::EntityAlreadyExists { .. }) => {
                iam.get_role(&scope, &role_name).map_err(map_iam_error)?
            }
            Err(error) => return Err(map_iam_error(error)),
        };
        sync_role_managed_policies(iam, &scope, &role_name, properties)?;
        sync_role_inline_policies(iam, &scope, &role_name, properties)?;
        let role_arn = role.arn.to_string();
        Ok(stored_resource(
            logical_id,
            "AWS::IAM::Role",
            role_name.clone(),
            role_name,
            BTreeMap::from([
                ("Arn".to_owned(), role_arn),
                ("RoleId".to_owned(), role.role_id),
            ]),
            Value::Object(properties.clone()),
            template_properties.clone(),
        ))
    }

    fn apply_iam_user(
        &self,
        scope: &CloudFormationScope,
        stack_name: &str,
        logical_id: &str,
        properties: &Map<String, Value>,
        template_properties: &Value,
    ) -> Result<StoredStackResource, CloudFormationError> {
        let user_name = optional_string_field(properties, "UserName")?
            .unwrap_or_else(|| generated_iam_name(stack_name, logical_id));
        let iam = require_dependency(&self.dependencies.iam, "IAM")?;
        let scope =
            IamScope::new(scope.account_id().clone(), scope.region().clone());
        let user = match iam.create_user(
            &scope,
            CreateUserInput {
                path: optional_string_field(properties, "Path")?
                    .unwrap_or_else(|| "/".to_owned()),
                tags: iam_tags(properties)?,
                user_name: user_name.clone(),
            },
        ) {
            Ok(user) => user,
            Err(IamError::EntityAlreadyExists { .. }) => {
                iam.get_user(&scope, &user_name).map_err(map_iam_error)?
            }
            Err(error) => return Err(map_iam_error(error)),
        };
        let user_arn = user.arn.to_string();
        Ok(stored_resource(
            logical_id,
            "AWS::IAM::User",
            user_name.clone(),
            user_name,
            BTreeMap::from([("Arn".to_owned(), user_arn)]),
            Value::Object(properties.clone()),
            template_properties.clone(),
        ))
    }

    fn apply_iam_access_key(
        &self,
        scope: &CloudFormationScope,
        logical_id: &str,
        properties: &Map<String, Value>,
        template_properties: &Value,
        existing: Option<&StoredStackResource>,
    ) -> Result<StoredStackResource, CloudFormationError> {
        let user_name = required_string_field(properties, "UserName")?;
        let iam = require_dependency(&self.dependencies.iam, "IAM")?;
        let scope =
            IamScope::new(scope.account_id().clone(), scope.region().clone());
        let access_key = if let Some(existing) = existing {
            if existing
                .attributes
                .get("UserName")
                .is_some_and(|existing_user| existing_user == &user_name)
            {
                if let Some(status) =
                    optional_string_field(properties, "Status")?
                {
                    iam.update_access_key(
                        &scope,
                        &user_name,
                        &existing.physical_resource_id,
                        match status.as_str() {
                            "Inactive" => IamAccessKeyStatus::Inactive,
                            _ => IamAccessKeyStatus::Active,
                        },
                    )
                    .map_err(map_iam_error)?;
                }
                let metadata = iam
                    .list_access_keys(&scope, &user_name)
                    .map_err(map_iam_error)?
                    .into_iter()
                    .find(|candidate| {
                        candidate.access_key_id
                            == existing.physical_resource_id
                    })
                    .ok_or_else(|| CloudFormationError::Validation {
                        message: format!(
                            "Access key {} is missing for user {}",
                            existing.physical_resource_id, user_name
                        ),
                    })?;
                StoredStackResource {
                    attributes: BTreeMap::from([
                        (
                            "SecretAccessKey".to_owned(),
                            existing
                                .attributes
                                .get("SecretAccessKey")
                                .cloned()
                                .unwrap_or_default(),
                        ),
                        ("UserName".to_owned(), user_name.clone()),
                    ]),
                    last_updated_timestamp_epoch_seconds: 0,
                    logical_resource_id: logical_id.to_owned(),
                    physical_resource_id: metadata.access_key_id.clone(),
                    properties: Value::Object(properties.clone()),
                    ref_value: metadata.access_key_id,
                    resource_status: String::new(),
                    resource_status_reason: None,
                    resource_type: "AWS::IAM::AccessKey".to_owned(),
                    template_properties: template_properties.clone(),
                }
            } else {
                self.delete_resource(
                    &CloudFormationScope::new(
                        scope.account_id().clone(),
                        scope.region().clone(),
                    ),
                    existing,
                )?;
                let access_key = iam
                    .create_access_key(&scope, &user_name)
                    .map_err(map_iam_error)?;
                stored_resource(
                    logical_id,
                    "AWS::IAM::AccessKey",
                    access_key.access_key_id.clone(),
                    access_key.access_key_id.clone(),
                    BTreeMap::from([
                        (
                            "SecretAccessKey".to_owned(),
                            access_key.secret_access_key,
                        ),
                        ("UserName".to_owned(), user_name),
                    ]),
                    Value::Object(properties.clone()),
                    template_properties.clone(),
                )
            }
        } else {
            let access_key = iam
                .create_access_key(&scope, &user_name)
                .map_err(map_iam_error)?;
            stored_resource(
                logical_id,
                "AWS::IAM::AccessKey",
                access_key.access_key_id.clone(),
                access_key.access_key_id.clone(),
                BTreeMap::from([
                    (
                        "SecretAccessKey".to_owned(),
                        access_key.secret_access_key,
                    ),
                    ("UserName".to_owned(), user_name),
                ]),
                Value::Object(properties.clone()),
                template_properties.clone(),
            )
        };
        Ok(access_key)
    }

    fn apply_iam_inline_policy(
        &self,
        scope: &CloudFormationScope,
        stack_name: &str,
        logical_id: &str,
        properties: &Map<String, Value>,
        template_properties: &Value,
        existing: Option<&StoredStackResource>,
    ) -> Result<StoredStackResource, CloudFormationError> {
        if let Some(existing) = existing {
            self.delete_resource(scope, existing)?;
        }
        let policy_name = optional_string_field(properties, "PolicyName")?
            .unwrap_or_else(|| generated_iam_name(stack_name, logical_id));
        let document = properties
            .get("PolicyDocument")
            .map(compact_json)
            .ok_or_else(|| {
                template_validation_error(format!(
                    "resource {logical_id} requires PolicyDocument"
                ))
            })?;
        let iam = require_dependency(&self.dependencies.iam, "IAM")?;
        let scope =
            IamScope::new(scope.account_id().clone(), scope.region().clone());
        for role_name in optional_string_list_field(properties, "Roles")?
            .unwrap_or_default()
        {
            iam.put_role_policy(
                &scope,
                &role_name,
                InlinePolicyInput {
                    document: document.clone(),
                    policy_name: policy_name.clone(),
                },
            )
            .map_err(map_iam_error)?;
        }
        for user_name in optional_string_list_field(properties, "Users")?
            .unwrap_or_default()
        {
            iam.put_user_policy(
                &scope,
                &user_name,
                InlinePolicyInput {
                    document: document.clone(),
                    policy_name: policy_name.clone(),
                },
            )
            .map_err(map_iam_error)?;
        }
        let id = format!("inline-policy-{}", short_hash(&policy_name));
        Ok(stored_resource(
            logical_id,
            "AWS::IAM::Policy",
            policy_name.clone(),
            policy_name,
            BTreeMap::from([("Id".to_owned(), id)]),
            Value::Object(properties.clone()),
            template_properties.clone(),
        ))
    }

    fn apply_iam_managed_policy(
        &self,
        scope: &CloudFormationScope,
        stack_name: &str,
        logical_id: &str,
        properties: &Map<String, Value>,
        template_properties: &Value,
        existing: Option<&StoredStackResource>,
    ) -> Result<StoredStackResource, CloudFormationError> {
        let policy_name =
            optional_string_field(properties, "ManagedPolicyName")?
                .unwrap_or_else(|| generated_iam_name(stack_name, logical_id));
        let document = properties
            .get("PolicyDocument")
            .map(compact_json)
            .ok_or_else(|| {
                template_validation_error(format!(
                    "resource {logical_id} requires PolicyDocument"
                ))
            })?;
        let iam = require_dependency(&self.dependencies.iam, "IAM")?;
        let scope =
            IamScope::new(scope.account_id().clone(), scope.region().clone());
        let policy = if let Some(existing) = existing {
            let policy_arn =
                parse_managed_policy_arn(&existing.physical_resource_id)?;
            iam.create_policy_version(
                &scope,
                CreatePolicyVersionInput {
                    policy_arn: policy_arn.clone(),
                    policy_document: document,
                    set_as_default: true,
                },
            )
            .map_err(map_iam_error)?;
            iam.get_policy(&scope, &policy_arn).map_err(map_iam_error)?
        } else {
            iam.create_policy(
                &scope,
                CreatePolicyInput {
                    description: optional_string_field(
                        properties,
                        "Description",
                    )?
                    .unwrap_or_default(),
                    path: optional_string_field(properties, "Path")?
                        .unwrap_or_else(|| "/".to_owned()),
                    policy_document: document,
                    policy_name,
                    tags: iam_tags(properties)?,
                },
            )
            .map_err(map_iam_error)?
        };
        sync_managed_policy_attachments(iam, &scope, &policy.arn, properties)?;
        let policy_arn = policy.arn.to_string();
        Ok(stored_resource(
            logical_id,
            "AWS::IAM::ManagedPolicy",
            policy_arn.clone(),
            policy_arn.clone(),
            BTreeMap::from([("Arn".to_owned(), policy_arn)]),
            Value::Object(properties.clone()),
            template_properties.clone(),
        ))
    }

    fn apply_iam_instance_profile(
        &self,
        scope: &CloudFormationScope,
        stack_name: &str,
        logical_id: &str,
        properties: &Map<String, Value>,
        template_properties: &Value,
        existing: Option<&StoredStackResource>,
    ) -> Result<StoredStackResource, CloudFormationError> {
        let profile_name =
            optional_string_field(properties, "InstanceProfileName")?
                .unwrap_or_else(|| generated_iam_name(stack_name, logical_id));
        let iam = require_dependency(&self.dependencies.iam, "IAM")?;
        let scope =
            IamScope::new(scope.account_id().clone(), scope.region().clone());
        let profile = match iam.create_instance_profile(
            &scope,
            CreateInstanceProfileInput {
                instance_profile_name: profile_name.clone(),
                path: optional_string_field(properties, "Path")?
                    .unwrap_or_else(|| "/".to_owned()),
                tags: iam_tags(properties)?,
            },
        ) {
            Ok(profile) => profile,
            Err(IamError::EntityAlreadyExists { .. }) => iam
                .get_instance_profile(&scope, &profile_name)
                .map_err(map_iam_error)?,
            Err(error) => return Err(map_iam_error(error)),
        };
        if let Some(existing) = existing {
            for role in
                string_list_field_from_value(&existing.properties, "Roles")?
            {
                let _ = iam.remove_role_from_instance_profile(
                    &scope,
                    &profile_name,
                    &role,
                );
            }
        }
        for role in optional_string_list_field(properties, "Roles")?
            .unwrap_or_default()
        {
            let _ =
                iam.add_role_to_instance_profile(&scope, &profile_name, &role);
        }
        let profile_arn = profile.arn.to_string();
        Ok(stored_resource(
            logical_id,
            "AWS::IAM::InstanceProfile",
            profile_name.clone(),
            profile_name,
            BTreeMap::from([("Arn".to_owned(), profile_arn)]),
            Value::Object(properties.clone()),
            template_properties.clone(),
        ))
    }

    fn apply_ssm_parameter(
        &self,
        scope: &CloudFormationScope,
        stack_name: &str,
        logical_id: &str,
        properties: &Map<String, Value>,
        template_properties: &Value,
    ) -> Result<StoredStackResource, CloudFormationError> {
        let name =
            optional_string_field(properties, "Name")?.unwrap_or_else(|| {
                generated_parameter_name(stack_name, logical_id)
            });
        let parameter_type = optional_string_field(properties, "Type")?
            .unwrap_or_else(|| "String".to_owned());
        let value = required_string_field(properties, "Value")?;
        let ssm = require_dependency(&self.dependencies.ssm, "SSM")?;
        let scope =
            SsmScope::new(scope.account_id().clone(), scope.region().clone());
        ssm.put_parameter(
            &scope,
            SsmPutParameterInput {
                description: optional_string_field(properties, "Description")?,
                name: SsmParameterName::parse(&name).map_err(map_ssm_error)?,
                overwrite: Some(true),
                parameter_type: Some(
                    SsmParameterType::parse(&parameter_type)
                        .map_err(map_ssm_error)?,
                ),
                value: value.clone(),
            },
        )
        .map_err(map_ssm_error)?;
        let tags = ssm_parameter_tags(properties)?;
        if !tags.is_empty() {
            ssm.add_tags_to_resource(
                &scope,
                SsmAddTagsToResourceInput {
                    resource_id: SsmParameterName::parse(&name)
                        .map_err(map_ssm_error)?,
                    resource_type: SsmResourceType::Parameter,
                    tags,
                },
            )
            .map_err(map_ssm_error)?;
        }
        Ok(stored_resource(
            logical_id,
            "AWS::SSM::Parameter",
            name.clone(),
            name.clone(),
            BTreeMap::from([
                ("Type".to_owned(), parameter_type),
                ("Value".to_owned(), value),
            ]),
            Value::Object(properties.clone()),
            template_properties.clone(),
        ))
    }

    fn apply_kms_key(
        &self,
        scope: &CloudFormationScope,
        logical_id: &str,
        properties: &Map<String, Value>,
        template_properties: &Value,
        existing: Option<&StoredStackResource>,
    ) -> Result<StoredStackResource, CloudFormationError> {
        if let Some(existing) = existing {
            return Ok(existing.clone());
        }
        let kms = require_dependency(&self.dependencies.kms, "KMS")?;
        let output = kms
            .create_key(
                &KmsScope::new(
                    scope.account_id().clone(),
                    scope.region().clone(),
                ),
                CreateKmsKeyInput {
                    description: optional_string_field(
                        properties,
                        "Description",
                    )?,
                    key_spec: optional_string_field(properties, "KeySpec")?,
                    key_usage: optional_string_field(properties, "KeyUsage")?,
                    tags: kms_tags(properties)?,
                },
            )
            .map_err(map_kms_error)?;
        let key_id = output.key_metadata.key_id.to_string();
        let arn = output.key_metadata.arn.to_string();
        Ok(stored_resource(
            logical_id,
            "AWS::KMS::Key",
            key_id.clone(),
            key_id,
            BTreeMap::from([("Arn".to_owned(), arn)]),
            Value::Object(properties.clone()),
            template_properties.clone(),
        ))
    }

    fn apply_kms_alias(
        &self,
        scope: &CloudFormationScope,
        logical_id: &str,
        properties: &Map<String, Value>,
        template_properties: &Value,
        existing: Option<&StoredStackResource>,
    ) -> Result<StoredStackResource, CloudFormationError> {
        let alias_name = required_string_field(properties, "AliasName")?;
        let target_key_id = required_string_field(properties, "TargetKeyId")?;
        if let Some(existing) = existing
            && existing.physical_resource_id != alias_name
        {
            self.delete_resource(scope, existing)?;
        }
        let kms = require_dependency(&self.dependencies.kms, "KMS")?;
        let scope =
            KmsScope::new(scope.account_id().clone(), scope.region().clone());
        if existing.is_none()
            || existing.is_some_and(|resource| {
                resource.physical_resource_id != alias_name
            })
        {
            kms.create_alias(
                &scope,
                CreateKmsAliasInput {
                    alias_name: alias_name.parse::<KmsAliasName>().map_err(
                        |error| CloudFormationError::Validation {
                            message: error.to_string(),
                        },
                    )?,
                    target_key_id: target_key_id
                        .parse::<KmsKeyReference>()
                        .map_err(|error| CloudFormationError::Validation {
                            message: error.to_string(),
                        })?,
                },
            )
            .map_err(map_kms_error)?;
        }
        let alias_arn = format!(
            "arn:aws:kms:{}:{}:{}",
            scope.region(),
            scope.account_id(),
            alias_name
        );
        Ok(stored_resource(
            logical_id,
            "AWS::KMS::Alias",
            alias_name.clone(),
            alias_name.clone(),
            BTreeMap::from([
                ("AliasArn".to_owned(), alias_arn),
                ("AliasName".to_owned(), alias_name.clone()),
                ("TargetKeyId".to_owned(), target_key_id),
            ]),
            Value::Object(properties.clone()),
            template_properties.clone(),
        ))
    }

    fn apply_secret(
        &self,
        scope: &CloudFormationScope,
        stack_name: &str,
        logical_id: &str,
        properties: &Map<String, Value>,
        template_properties: &Value,
        existing: Option<&StoredStackResource>,
    ) -> Result<StoredStackResource, CloudFormationError> {
        let name = optional_string_field(properties, "Name")?
            .unwrap_or_else(|| generated_secret_name(stack_name, logical_id));
        let secrets_manager = require_dependency(
            &self.dependencies.secrets_manager,
            "Secrets Manager",
        )?;
        let scope = SecretsManagerScope::new(
            scope.account_id().clone(),
            scope.region().clone(),
        );
        let arn = if existing.is_some() {
            secrets_manager
                .update_secret(
                    &scope,
                    UpdateSecretInput {
                        client_request_token: None,
                        description: optional_string_field(
                            properties,
                            "Description",
                        )?,
                        kms_key_id: optional_string_field(
                            properties, "KmsKeyId",
                        )?
                        .filter(|value| !value.is_empty())
                        .map(|value| value.parse::<KmsKeyReference>())
                        .transpose()
                        .map_err(|error| CloudFormationError::Validation {
                            message: error.to_string(),
                        })?,
                        secret_binary: None,
                        secret_id: existing
                            .ok_or_else(|| {
                                template_validation_error(format!(
                                    "resource {logical_id} is missing an existing secret"
                                ))
                            })?
                            .physical_resource_id
                            .parse::<SecretsManagerSecretReference>()
                            .map_err(|error| {
                                CloudFormationError::Validation {
                                    message: error.to_string(),
                                }
                            })?,
                        secret_string: optional_string_field(
                            properties,
                            "SecretString",
                        )?,
                        secret_type: optional_string_field(
                            properties, "Type",
                        )?,
                    },
                )
                .map_err(map_secrets_manager_error)?
                .arn
        } else {
            secrets_manager
                .create_secret(
                    &scope,
                    CreateSecretInput {
                        client_request_token: None,
                        description: optional_string_field(
                            properties,
                            "Description",
                        )?,
                        kms_key_id: optional_string_field(
                            properties, "KmsKeyId",
                        )?
                        .filter(|value| !value.is_empty())
                        .map(|value| value.parse::<KmsKeyReference>())
                        .transpose()
                        .map_err(|error| CloudFormationError::Validation {
                            message: error.to_string(),
                        })?,
                        name: name
                            .parse::<SecretsManagerSecretName>()
                            .map_err(|error| {
                                CloudFormationError::Validation {
                                    message: error.to_string(),
                                }
                            })?,
                        secret_binary: None,
                        secret_string: optional_string_field(
                            properties,
                            "SecretString",
                        )?,
                        secret_type: optional_string_field(
                            properties, "Type",
                        )?,
                        tags: secrets_manager_tags(properties)?,
                    },
                )
                .map_err(map_secrets_manager_error)?
                .arn
        };
        let arn = arn.to_string();
        Ok(stored_resource(
            logical_id,
            "AWS::SecretsManager::Secret",
            arn.clone(),
            arn.clone(),
            BTreeMap::from([
                ("Arn".to_owned(), arn.clone()),
                ("Name".to_owned(), name),
            ]),
            Value::Object(properties.clone()),
            template_properties.clone(),
        ))
    }

    fn delete_iam_role(
        &self,
        scope: &CloudFormationScope,
        resource: &StoredStackResource,
    ) -> Result<(), CloudFormationError> {
        let iam = require_dependency(&self.dependencies.iam, "IAM")?;
        let scope =
            IamScope::new(scope.account_id().clone(), scope.region().clone());
        for policy in iam
            .list_attached_role_policies(
                &scope,
                &resource.physical_resource_id,
                None,
            )
            .map_err(map_iam_error)?
        {
            let _ = iam.detach_role_policy(
                &scope,
                &resource.physical_resource_id,
                &policy.policy_arn,
            );
        }
        for policy_name in iam
            .list_role_policies(&scope, &resource.physical_resource_id)
            .map_err(map_iam_error)?
        {
            let _ = iam.delete_role_policy(
                &scope,
                &resource.physical_resource_id,
                &policy_name,
            );
        }
        for profile in iam
            .list_instance_profiles_for_role(
                &scope,
                &resource.physical_resource_id,
            )
            .map_err(map_iam_error)?
        {
            let _ = iam.remove_role_from_instance_profile(
                &scope,
                &profile.instance_profile_name,
                &resource.physical_resource_id,
            );
        }
        iam.delete_role(&scope, &resource.physical_resource_id)
            .map_err(map_iam_error)
    }

    fn delete_iam_user(
        &self,
        scope: &CloudFormationScope,
        resource: &StoredStackResource,
    ) -> Result<(), CloudFormationError> {
        let iam = require_dependency(&self.dependencies.iam, "IAM")?;
        let scope =
            IamScope::new(scope.account_id().clone(), scope.region().clone());
        for access_key in iam
            .list_access_keys(&scope, &resource.physical_resource_id)
            .map_err(map_iam_error)?
        {
            let _ = iam.delete_access_key(
                &scope,
                &resource.physical_resource_id,
                &access_key.access_key_id,
            );
        }
        for policy in iam
            .list_attached_user_policies(
                &scope,
                &resource.physical_resource_id,
                None,
            )
            .map_err(map_iam_error)?
        {
            let _ = iam.detach_user_policy(
                &scope,
                &resource.physical_resource_id,
                &policy.policy_arn,
            );
        }
        for policy_name in iam
            .list_user_policies(&scope, &resource.physical_resource_id)
            .map_err(map_iam_error)?
        {
            let _ = iam.delete_user_policy(
                &scope,
                &resource.physical_resource_id,
                &policy_name,
            );
        }
        iam.delete_user(&scope, &resource.physical_resource_id)
            .map_err(map_iam_error)
    }

    fn delete_iam_access_key(
        &self,
        scope: &CloudFormationScope,
        resource: &StoredStackResource,
    ) -> Result<(), CloudFormationError> {
        let iam = require_dependency(&self.dependencies.iam, "IAM")?;
        iam.delete_access_key(
            &IamScope::new(scope.account_id().clone(), scope.region().clone()),
            resource
                .attributes
                .get("UserName")
                .map(String::as_str)
                .unwrap_or_default(),
            &resource.physical_resource_id,
        )
        .map_err(map_iam_error)
    }

    fn delete_iam_inline_policy(
        &self,
        scope: &CloudFormationScope,
        resource: &StoredStackResource,
    ) -> Result<(), CloudFormationError> {
        let iam = require_dependency(&self.dependencies.iam, "IAM")?;
        let scope =
            IamScope::new(scope.account_id().clone(), scope.region().clone());
        let policy_name = resource.physical_resource_id.clone();
        for role_name in optional_string_list_field_from_value(
            &resource.properties,
            "Roles",
        )? {
            let _ = iam.delete_role_policy(&scope, &role_name, &policy_name);
        }
        for user_name in optional_string_list_field_from_value(
            &resource.properties,
            "Users",
        )? {
            let _ = iam.delete_user_policy(&scope, &user_name, &policy_name);
        }
        Ok(())
    }

    fn delete_iam_managed_policy(
        &self,
        scope: &CloudFormationScope,
        resource: &StoredStackResource,
    ) -> Result<(), CloudFormationError> {
        let iam = require_dependency(&self.dependencies.iam, "IAM")?;
        let scope =
            IamScope::new(scope.account_id().clone(), scope.region().clone());
        let policy_arn =
            parse_managed_policy_arn(&resource.physical_resource_id)?;
        for role_name in optional_string_list_field_from_value(
            &resource.properties,
            "Roles",
        )? {
            let _ = iam.detach_role_policy(&scope, &role_name, &policy_arn);
        }
        for user_name in optional_string_list_field_from_value(
            &resource.properties,
            "Users",
        )? {
            let _ = iam.detach_user_policy(&scope, &user_name, &policy_arn);
        }
        iam.delete_policy(&scope, &policy_arn).map_err(map_iam_error)
    }

    fn delete_iam_instance_profile(
        &self,
        scope: &CloudFormationScope,
        resource: &StoredStackResource,
    ) -> Result<(), CloudFormationError> {
        let iam = require_dependency(&self.dependencies.iam, "IAM")?;
        let scope =
            IamScope::new(scope.account_id().clone(), scope.region().clone());
        for role_name in optional_string_list_field_from_value(
            &resource.properties,
            "Roles",
        )? {
            let _ = iam.remove_role_from_instance_profile(
                &scope,
                &resource.physical_resource_id,
                &role_name,
            );
        }
        iam.delete_instance_profile(&scope, &resource.physical_resource_id)
            .map_err(map_iam_error)
    }
}

#[derive(Clone, Copy)]
struct RuntimeContext<'a> {
    account_id: &'a AccountId,
    conditions: &'a BTreeMap<String, bool>,
    parameters: &'a BTreeMap<String, String>,
    region: &'a RegionId,
    resources: &'a BTreeMap<String, ResolvedResourceValues>,
    stack_id: &'a str,
    stack_name: &'a str,
}

fn evaluate_runtime_value(
    value: &Value,
    context: &RuntimeContext<'_>,
    path: &str,
) -> Result<ResolvedValue, CloudFormationError> {
    match value {
        Value::Null => Ok(ResolvedValue::Null),
        Value::Bool(value) => Ok(ResolvedValue::Bool(*value)),
        Value::Number(value) => Ok(ResolvedValue::Number(value.clone())),
        Value::String(value) => Ok(ResolvedValue::String(value.clone())),
        Value::Array(items) => Ok(ResolvedValue::Array(
            items
                .iter()
                .enumerate()
                .map(|(index, item)| {
                    evaluate_runtime_value(
                        item,
                        context,
                        &indexed_path(path, index),
                    )
                })
                .filter_map(|value| match value {
                    Ok(ResolvedValue::NoValue) => None,
                    other => Some(other),
                })
                .collect::<Result<Vec<_>, _>>()?,
        )),
        Value::Object(map) if map.len() == 1 => {
            let (key, inner) =
                single_entry_object(map, path, "runtime intrinsic")?;
            match key {
                "Ref" => evaluate_runtime_ref(inner, context, path),
                "Fn::Base64" => Ok(ResolvedValue::String(
                    BASE64_STANDARD.encode(
                        evaluate_runtime_value(
                            inner,
                            context,
                            &child_path(path, "Fn::Base64"),
                        )?
                        .to_string_value(path)?,
                    ),
                )),
                "Fn::If" => {
                    let items = array_items(inner, 3, 3, path, "Fn::If")?;
                    let condition_name = required_string_item(
                        items,
                        0,
                        path,
                        "Fn::If",
                        format!(
                            "Fn::If at {path} must reference a named condition"
                        ),
                    )?;
                    let active = context.conditions.get(condition_name).copied().ok_or_else(|| {
                        template_validation_error(format!(
                            "Fn::If at {path} references unknown condition {condition_name}"
                        ))
                    })?;
                    evaluate_runtime_value(
                        if active {
                            required_item(items, 1, path, "Fn::If")?
                        } else {
                            required_item(items, 2, path, "Fn::If")?
                        },
                        context,
                        path,
                    )
                }
                "Fn::Join" => evaluate_runtime_join(inner, context, path),
                "Fn::Select" => evaluate_runtime_select(inner, context, path),
                "Fn::Sub" => evaluate_runtime_sub(inner, context, path),
                "Fn::GetAtt" => evaluate_runtime_get_att(inner, context, path),
                other if other.starts_with("Fn::") => {
                    Err(unsupported_intrinsic_error(other, path))
                }
                _ => {
                    let mut object = Map::new();
                    for (field, value) in map {
                        if let Some(value) = evaluate_runtime_value(
                            value,
                            context,
                            &child_path(path, field),
                        )?
                        .into_json()
                        {
                            object.insert(field.clone(), value);
                        }
                    }
                    Ok(ResolvedValue::Object(object))
                }
            }
        }
        Value::Object(map) => {
            let mut object = Map::new();
            for (field, value) in map {
                if let Some(value) = evaluate_runtime_value(
                    value,
                    context,
                    &child_path(path, field),
                )?
                .into_json()
                {
                    object.insert(field.clone(), value);
                }
            }
            Ok(ResolvedValue::Object(object))
        }
    }
}

fn evaluate_runtime_ref(
    value: &Value,
    context: &RuntimeContext<'_>,
    path: &str,
) -> Result<ResolvedValue, CloudFormationError> {
    let target = value.as_str().ok_or_else(|| {
        template_validation_error(format!("Ref at {path} must be a string"))
    })?;
    match target {
        "AWS::AccountId" => {
            Ok(ResolvedValue::String(context.account_id.to_string()))
        }
        "AWS::Region" => Ok(ResolvedValue::String(context.region.to_string())),
        "AWS::StackName" => {
            Ok(ResolvedValue::String(context.stack_name.to_owned()))
        }
        "AWS::StackId" => {
            Ok(ResolvedValue::String(context.stack_id.to_owned()))
        }
        "AWS::Partition" => Ok(ResolvedValue::String("aws".to_owned())),
        "AWS::URLSuffix" => {
            Ok(ResolvedValue::String("amazonaws.com".to_owned()))
        }
        "AWS::NoValue" => Ok(ResolvedValue::NoValue),
        _ => {
            if let Some(value) = context.parameters.get(target) {
                return Ok(ResolvedValue::String(value.clone()));
            }
            if let Some(resource) = context.resources.get(target) {
                return Ok(ResolvedValue::String(resource.ref_value.clone()));
            }
            Err(template_validation_error(format!(
                "Ref at {path} targets unknown parameter or resource {target}"
            )))
        }
    }
}

fn evaluate_runtime_join(
    value: &Value,
    context: &RuntimeContext<'_>,
    path: &str,
) -> Result<ResolvedValue, CloudFormationError> {
    let items = array_items(value, 2, 2, path, "Fn::Join")?;
    let delimiter = required_string_item(
        items,
        0,
        path,
        "Fn::Join",
        format!(
            "Fn::Join delimiter at {} must be a string",
            indexed_path(path, 0)
        ),
    )?;
    let values = required_array_item(
        items,
        1,
        path,
        "Fn::Join",
        format!("Fn::Join list at {} must be an array", indexed_path(path, 1)),
    )?;
    let rendered = values
        .iter()
        .enumerate()
        .map(|(index, item)| {
            evaluate_runtime_value(item, context, &indexed_path(path, index))?
                .to_string_value(path)
        })
        .collect::<Result<Vec<_>, _>>()?;

    Ok(ResolvedValue::String(rendered.join(delimiter)))
}

fn evaluate_runtime_select(
    value: &Value,
    context: &RuntimeContext<'_>,
    path: &str,
) -> Result<ResolvedValue, CloudFormationError> {
    let items = array_items(value, 2, 2, path, "Fn::Select")?;
    let index = evaluate_runtime_value(
        required_item(items, 0, path, "Fn::Select")?,
        context,
        &indexed_path(path, 0),
    )?
    .to_string_value(path)?
    .parse::<usize>()
    .map_err(|_| {
        template_validation_error(format!(
            "Fn::Select index at {} must be an unsigned integer",
            indexed_path(path, 0)
        ))
    })?;
    let items = match evaluate_runtime_value(
        required_item(items, 1, path, "Fn::Select")?,
        context,
        &indexed_path(path, 1),
    )? {
        ResolvedValue::Array(items) => items,
        _ => {
            return Err(template_validation_error(format!(
                "Fn::Select source at {} must resolve to an array",
                indexed_path(path, 1)
            )));
        }
    };
    items.into_iter().nth(index).ok_or_else(|| {
        template_validation_error(format!(
            "Fn::Select index {index} is out of bounds at {path}"
        ))
    })
}

fn evaluate_runtime_sub(
    value: &Value,
    context: &RuntimeContext<'_>,
    path: &str,
) -> Result<ResolvedValue, CloudFormationError> {
    let (template, variables) = match value {
        Value::String(template) => (template.as_str(), None),
        Value::Array(items) if items.len() == 2 => {
            let template = required_string_item(
                items,
                0,
                path,
                "Fn::Sub",
                format!("Fn::Sub at {path} must start with a string template"),
            )?;
            let variables = required_object_item(
                items,
                1,
                path,
                "Fn::Sub",
                format!(
                    "Fn::Sub variable map at {} must be an object",
                    indexed_path(path, 1)
                ),
            )?;
            (template, Some(variables))
        }
        _ => {
            return Err(template_validation_error(format!(
                "Fn::Sub at {path} must be a string or [template, variables] tuple"
            )));
        }
    };
    let mut rendered = String::new();
    let mut remaining = template;
    while let Some(start) = remaining.find("${") {
        rendered.push_str(&remaining[..start]);
        let token_start = start + 2;
        let Some(end) = remaining[token_start..].find('}') else {
            return Err(template_validation_error(format!(
                "Fn::Sub at {path} contains an unterminated token"
            )));
        };
        let token = &remaining[token_start..token_start + end];
        let value = if let Some(variable) =
            variables.and_then(|variables| variables.get(token))
        {
            evaluate_runtime_value(
                variable,
                context,
                &child_path(path, token),
            )?
            .to_string_value(path)?
        } else if let Some((logical_id, attribute)) = token.split_once('.') {
            evaluate_runtime_get_att(
                &Value::Array(vec![
                    Value::String(logical_id.to_owned()),
                    Value::String(attribute.to_owned()),
                ]),
                context,
                path,
            )?
            .to_string_value(path)?
        } else {
            evaluate_runtime_ref(
                &Value::String(token.to_owned()),
                context,
                path,
            )?
            .to_string_value(path)?
        };
        rendered.push_str(&value);
        remaining = &remaining[token_start + end + 1..];
    }
    rendered.push_str(remaining);
    Ok(ResolvedValue::String(rendered))
}

fn evaluate_runtime_get_att(
    value: &Value,
    context: &RuntimeContext<'_>,
    path: &str,
) -> Result<ResolvedValue, CloudFormationError> {
    let (logical_id, attribute) = get_att_parts(value, path)?;
    let resource = context.resources.get(logical_id).ok_or_else(|| {
        template_validation_error(format!(
            "Fn::GetAtt at {path} references unknown resource {logical_id}"
        ))
    })?;
    let value = resource.attributes.get(attribute).ok_or_else(|| {
        template_validation_error(format!(
            "Fn::GetAtt attribute {attribute} for resource {logical_id} ({}) is not supported",
            resource.resource_type
        ))
    })?;
    Ok(ResolvedValue::String(value.clone()))
}

fn push_stack_event(
    stack: &mut StoredStack,
    logical_resource_id: &str,
    physical_resource_id: Option<String>,
    resource_type: &str,
    resource_status: &str,
    resource_status_reason: Option<String>,
    timestamp_epoch_seconds: u64,
) {
    stack.event_counter += 1;
    stack.events.push(StoredStackEvent {
        event_id: format!(
            "{}-{:08}",
            logical_resource_id, stack.event_counter
        ),
        logical_resource_id: logical_resource_id.to_owned(),
        physical_resource_id,
        resource_status: resource_status.to_owned(),
        resource_status_reason,
        resource_type: resource_type.to_owned(),
        timestamp_epoch_seconds,
    });
}

fn stack_description(stack: &StoredStack) -> CloudFormationStackDescription {
    let mut outputs = stack
        .outputs
        .iter()
        .map(|(output_key, output)| CloudFormationStackOutput {
            description: output.description.clone(),
            output_key: output_key.clone(),
            output_value: output.value.clone(),
        })
        .collect::<Vec<_>>();
    outputs.sort_by(|left, right| left.output_key.cmp(&right.output_key));
    CloudFormationStackDescription {
        capabilities: stack.capabilities.clone(),
        creation_time_epoch_seconds: stack.creation_time_epoch_seconds,
        description: stack.description.clone(),
        last_updated_time_epoch_seconds: stack.last_updated_time_epoch_seconds,
        outputs,
        stack_id: stack.stack_id.clone(),
        stack_name: stack.stack_name.clone(),
        stack_status: stack.stack_status.clone(),
        stack_status_reason: stack.stack_status_reason.clone(),
    }
}

fn change_set_description(
    change_set: &StoredChangeSet,
) -> CloudFormationChangeSetDescription {
    CloudFormationChangeSetDescription {
        changes: change_set.changes.clone(),
        change_set_id: change_set.change_set_id.clone(),
        change_set_name: change_set.change_set_name.clone(),
        change_set_type: change_set.change_set_type.clone(),
        creation_time_epoch_seconds: change_set.creation_time_epoch_seconds,
        execution_status: change_set.execution_status.clone(),
        stack_id: change_set.stack_id.clone(),
        stack_name: change_set.stack_name.clone(),
        status: change_set.status.clone(),
        status_reason: change_set.status_reason.clone(),
    }
}

fn change_set_summary(
    change_set: &StoredChangeSet,
) -> CloudFormationChangeSetSummary {
    CloudFormationChangeSetSummary {
        change_set_id: change_set.change_set_id.clone(),
        change_set_name: change_set.change_set_name.clone(),
        creation_time_epoch_seconds: change_set.creation_time_epoch_seconds,
        execution_status: change_set.execution_status.clone(),
        status: change_set.status.clone(),
    }
}

fn resource_description(
    stack: &StoredStack,
    resource: &StoredStackResource,
) -> CloudFormationStackResourceDescription {
    CloudFormationStackResourceDescription {
        last_updated_timestamp_epoch_seconds: resource
            .last_updated_timestamp_epoch_seconds,
        logical_resource_id: resource.logical_resource_id.clone(),
        physical_resource_id: resource.physical_resource_id.clone(),
        resource_status: resource.resource_status.clone(),
        resource_status_reason: resource.resource_status_reason.clone(),
        resource_type: resource.resource_type.clone(),
        stack_id: stack.stack_id.clone(),
        stack_name: stack.stack_name.clone(),
    }
}

fn stored_resource(
    logical_id: &str,
    resource_type: &str,
    physical_resource_id: String,
    ref_value: String,
    attributes: BTreeMap<String, String>,
    properties: Value,
    template_properties: Value,
) -> StoredStackResource {
    StoredStackResource {
        attributes,
        last_updated_timestamp_epoch_seconds: 0,
        logical_resource_id: logical_id.to_owned(),
        physical_resource_id,
        properties,
        ref_value,
        resource_status: String::new(),
        resource_status_reason: None,
        resource_type: resource_type.to_owned(),
        template_properties,
    }
}

fn short_hash(value: &str) -> String {
    format!("{:x}", Sha256::digest(value.as_bytes()))[0..12].to_owned()
}

fn generated_bucket_name(stack_name: &str, logical_id: &str) -> String {
    format!(
        "{}-{}-{}",
        stack_name.to_ascii_lowercase(),
        logical_id.to_ascii_lowercase(),
        short_hash(&format!("{stack_name}:{logical_id}:bucket"))
    )
}

fn generated_queue_name(stack_name: &str, logical_id: &str) -> String {
    format!("{stack_name}-{logical_id}")
}

fn generated_topic_name(stack_name: &str, logical_id: &str) -> String {
    format!("{stack_name}-{logical_id}")
}

fn generated_function_name(stack_name: &str, logical_id: &str) -> String {
    format!("{stack_name}-{logical_id}")
}

fn generated_table_name(stack_name: &str, logical_id: &str) -> String {
    format!("{stack_name}-{logical_id}")
}

fn generated_iam_name(stack_name: &str, logical_id: &str) -> String {
    format!("{stack_name}-{logical_id}")
}

fn generated_parameter_name(stack_name: &str, logical_id: &str) -> String {
    format!("/{stack_name}/{logical_id}")
}

fn generated_secret_name(stack_name: &str, logical_id: &str) -> String {
    format!("{stack_name}/{logical_id}")
}

fn queue_ref(
    advertised_edge: &aws::AdvertisedEdge,
    account_id: &AccountId,
    queue_name: &str,
) -> String {
    advertised_edge.sqs_queue_url(account_id, queue_name)
}

fn queue_identity_from_ref(
    scope: &CloudFormationScope,
    value: &str,
) -> Result<SqsQueueIdentity, CloudFormationError> {
    let path = value
        .split_once("://")
        .map(|(_, remainder)| remainder)
        .unwrap_or(value);
    let queue_name = path.rsplit('/').next().ok_or_else(|| {
        template_validation_error("queue reference is invalid")
    })?;
    SqsQueueIdentity::new(
        scope.account_id().clone(),
        scope.region().clone(),
        queue_name,
    )
    .map_err(map_sqs_error)
}

fn optional_string_field(
    properties: &Map<String, Value>,
    name: &str,
) -> Result<Option<String>, CloudFormationError> {
    properties.get(name).map(value_to_string).transpose()
}

fn required_string_field(
    properties: &Map<String, Value>,
    name: &str,
) -> Result<String, CloudFormationError> {
    optional_string_field(properties, name)?.ok_or_else(|| {
        template_validation_error(format!("property {name} is required"))
    })
}

fn required_string_field_from_value(
    value: &Value,
    name: &str,
) -> Result<String, CloudFormationError> {
    value
        .as_object()
        .ok_or_else(|| {
            template_validation_error("resource properties must be an object")
        })
        .and_then(|properties| required_string_field(properties, name))
}

fn optional_bool_field(
    properties: &Map<String, Value>,
    name: &str,
) -> Result<Option<bool>, CloudFormationError> {
    properties
        .get(name)
        .map(|value| match value {
            Value::Bool(value) => Ok(*value),
            Value::String(value) => {
                Ok(matches!(value.as_str(), "true" | "True" | "TRUE"))
            }
            _ => Err(template_validation_error(format!(
                "property {name} must be a boolean"
            ))),
        })
        .transpose()
}

fn optional_u32_field(
    properties: &Map<String, Value>,
    name: &str,
) -> Result<Option<u32>, CloudFormationError> {
    properties
        .get(name)
        .map(|value| {
            value_to_string(value)?.parse::<u32>().map_err(|_| {
                template_validation_error(format!(
                    "property {name} must be an unsigned integer"
                ))
            })
        })
        .transpose()
}

fn value_to_string(value: &Value) -> Result<String, CloudFormationError> {
    match value {
        Value::String(value) => Ok(value.clone()),
        Value::Bool(value) => Ok(value.to_string()),
        Value::Number(value) => Ok(value.to_string()),
        Value::Null => Ok(String::new()),
        _ => Err(template_validation_error(
            "property value must be a scalar".to_owned(),
        )),
    }
}

fn compact_json(value: &Value) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| "null".to_owned())
}

fn string_list_field(
    properties: &Map<String, Value>,
    name: &str,
) -> Result<Vec<String>, CloudFormationError> {
    optional_string_list_field(properties, name)?.ok_or_else(|| {
        template_validation_error(format!("property {name} is required"))
    })
}

fn optional_string_list_field(
    properties: &Map<String, Value>,
    name: &str,
) -> Result<Option<Vec<String>>, CloudFormationError> {
    properties
        .get(name)
        .map(|value| string_list_from_value(value, name))
        .transpose()
}

fn optional_string_list_field_from_value(
    value: &Value,
    name: &str,
) -> Result<Vec<String>, CloudFormationError> {
    Ok(value
        .as_object()
        .map(|properties| optional_string_list_field(properties, name))
        .transpose()?
        .flatten()
        .unwrap_or_default())
}

fn string_list_field_from_value(
    value: &Value,
    name: &str,
) -> Result<Vec<String>, CloudFormationError> {
    value
        .as_object()
        .ok_or_else(|| {
            template_validation_error("resource properties must be an object")
        })
        .and_then(|properties| string_list_field(properties, name))
}

fn string_list_from_value(
    value: &Value,
    name: &str,
) -> Result<Vec<String>, CloudFormationError> {
    match value {
        Value::Array(items) => {
            items.iter().map(value_to_string).collect::<Result<Vec<_>, _>>()
        }
        Value::String(value) => Ok(vec![value.clone()]),
        _ => Err(template_validation_error(format!(
            "property {name} must be a string list"
        ))),
    }
}

fn cloudformation_tags(
    properties: &Map<String, Value>,
) -> Result<Vec<(String, String)>, CloudFormationError> {
    let Some(tags) = properties.get("Tags") else {
        return Ok(Vec::new());
    };
    let items = tags.as_array().ok_or_else(|| {
        template_validation_error("Tags must be an array of Key/Value objects")
    })?;
    items
        .iter()
        .map(|item| {
            let item = item.as_object().ok_or_else(|| {
                template_validation_error("tag entries must be objects")
            })?;
            Ok((
                required_string_field(item, "Key")?,
                required_string_field(item, "Value")?,
            ))
        })
        .collect()
}

fn iam_tags(
    properties: &Map<String, Value>,
) -> Result<Vec<IamTag>, CloudFormationError> {
    cloudformation_tags(properties).map(|tags| {
        tags.into_iter().map(|(key, value)| IamTag { key, value }).collect()
    })
}

fn kms_tags(
    properties: &Map<String, Value>,
) -> Result<Vec<KmsTag>, CloudFormationError> {
    cloudformation_tags(properties).map(|tags| {
        tags.into_iter()
            .map(|(key, value)| KmsTag { tag_key: key, tag_value: value })
            .collect()
    })
}

fn secrets_manager_tags(
    properties: &Map<String, Value>,
) -> Result<Vec<SecretsManagerTag>, CloudFormationError> {
    cloudformation_tags(properties).map(|tags| {
        tags.into_iter()
            .map(|(key, value)| SecretsManagerTag { key, value })
            .collect()
    })
}

fn ssm_parameter_tags(
    properties: &Map<String, Value>,
) -> Result<Vec<SsmTag>, CloudFormationError> {
    let Some(tags) = properties.get("Tags") else {
        return Ok(Vec::new());
    };
    let object = tags.as_object().ok_or_else(|| {
        template_validation_error("SSM Parameter Tags must be an object")
    })?;
    object
        .iter()
        .map(|(key, value)| {
            Ok(SsmTag { key: key.clone(), value: value_to_string(value)? })
        })
        .collect()
}

fn dynamodb_tags(
    properties: &Map<String, Value>,
) -> Result<Option<Vec<DynamoDbResourceTag>>, CloudFormationError> {
    let tags = cloudformation_tags(properties)?;
    if tags.is_empty() {
        return Ok(None);
    }
    Ok(Some(
        tags.into_iter()
            .map(|(key, value)| DynamoDbResourceTag { key, value })
            .collect(),
    ))
}

fn sqs_queue_attributes(
    properties: &Map<String, Value>,
) -> Result<BTreeMap<String, String>, CloudFormationError> {
    let mut attributes = BTreeMap::new();
    for name in [
        "ContentBasedDeduplication",
        "DelaySeconds",
        "FifoQueue",
        "MaximumMessageSize",
        "MessageRetentionPeriod",
        "Policy",
        "ReceiveMessageWaitTimeSeconds",
        "RedrivePolicy",
        "VisibilityTimeout",
    ] {
        if let Some(value) = properties.get(name) {
            attributes.insert(
                name.to_owned(),
                if name == "RedrivePolicy" {
                    compact_json(value)
                } else {
                    value_to_string(value)?
                },
            );
        }
    }
    Ok(attributes)
}

fn sns_topic_attributes(
    properties: &Map<String, Value>,
) -> Result<BTreeMap<String, String>, CloudFormationError> {
    let mut attributes = BTreeMap::new();
    for name in ["DisplayName", "FifoTopic", "KmsMasterKeyId"] {
        if let Some(value) = properties.get(name) {
            attributes.insert(name.to_owned(), value_to_string(value)?);
        }
    }
    Ok(attributes)
}

fn dynamodb_attribute_definitions(
    properties: &Map<String, Value>,
) -> Result<Vec<DynamoDbAttributeDefinition>, CloudFormationError> {
    string_map_array(properties, "AttributeDefinitions", |item| {
        Ok(DynamoDbAttributeDefinition {
            attribute_name: required_string_field(item, "AttributeName")?,
            attribute_type: match required_string_field(item, "AttributeType")?
                .as_str()
            {
                "B" => DynamoDbScalarAttributeType::Binary,
                "N" => DynamoDbScalarAttributeType::Number,
                _ => DynamoDbScalarAttributeType::String,
            },
        })
    })
}

fn dynamodb_key_schema(
    properties: &Map<String, Value>,
) -> Result<Vec<DynamoDbKeySchemaElement>, CloudFormationError> {
    string_map_array(properties, "KeySchema", |item| {
        Ok(DynamoDbKeySchemaElement {
            attribute_name: required_string_field(item, "AttributeName")?,
            key_type: match required_string_field(item, "KeyType")?.as_str() {
                "RANGE" => DynamoDbKeyType::Range,
                _ => DynamoDbKeyType::Hash,
            },
        })
    })
}

fn dynamodb_billing_mode(
    properties: &Map<String, Value>,
) -> Result<Option<DynamoDbBillingMode>, CloudFormationError> {
    optional_string_field(properties, "BillingMode").map(|value| {
        value.map(|value| {
            if value == "PAY_PER_REQUEST" {
                DynamoDbBillingMode::PayPerRequest
            } else {
                DynamoDbBillingMode::Provisioned
            }
        })
    })
}

fn dynamodb_provisioned_throughput(
    properties: &Map<String, Value>,
) -> Result<Option<DynamoDbProvisionedThroughput>, CloudFormationError> {
    let Some(value) = properties.get("ProvisionedThroughput") else {
        return Ok(None);
    };
    let value = value.as_object().ok_or_else(|| {
        template_validation_error(
            "ProvisionedThroughput must be an object".to_owned(),
        )
    })?;
    Ok(Some(DynamoDbProvisionedThroughput {
        read_capacity_units: required_string_field(
            value,
            "ReadCapacityUnits",
        )?
        .parse()
        .map_err(|_| {
            template_validation_error("ReadCapacityUnits must be numeric")
        })?,
        write_capacity_units: required_string_field(
            value,
            "WriteCapacityUnits",
        )?
        .parse()
        .map_err(|_| {
            template_validation_error("WriteCapacityUnits must be numeric")
        })?,
    }))
}

fn dynamodb_projection(
    item: &Map<String, Value>,
) -> Result<DynamoDbProjection, CloudFormationError> {
    Ok(DynamoDbProjection {
        non_key_attributes: optional_string_list_field(
            item,
            "NonKeyAttributes",
        )?
        .unwrap_or_default(),
        projection_type: match required_string_field(item, "ProjectionType")?
            .as_str()
        {
            "INCLUDE" => DynamoDbProjectionType::Include,
            "KEYS_ONLY" => DynamoDbProjectionType::KeysOnly,
            _ => DynamoDbProjectionType::All,
        },
    })
}

fn dynamodb_global_secondary_indexes(
    properties: &Map<String, Value>,
) -> Result<Vec<DynamoDbGlobalSecondaryIndexDefinition>, CloudFormationError> {
    optional_map_array(properties, "GlobalSecondaryIndexes", |item| {
        Ok(DynamoDbGlobalSecondaryIndexDefinition {
            index_name: required_string_field(item, "IndexName")?,
            key_schema: dynamodb_key_schema(item)?,
            projection: {
                let projection = item
                    .get("Projection")
                    .and_then(Value::as_object)
                    .ok_or_else(|| {
                        template_validation_error(
                            "GlobalSecondaryIndexes entries require Projection"
                                .to_owned(),
                        )
                    })?;
                dynamodb_projection(projection)?
            },
            provisioned_throughput: dynamodb_provisioned_throughput(item)?,
        })
    })
    .map(|value| value.unwrap_or_default())
}

fn dynamodb_local_secondary_indexes(
    properties: &Map<String, Value>,
) -> Result<Vec<DynamoDbLocalSecondaryIndexDefinition>, CloudFormationError> {
    optional_map_array(properties, "LocalSecondaryIndexes", |item| {
        Ok(DynamoDbLocalSecondaryIndexDefinition {
            index_name: required_string_field(item, "IndexName")?,
            key_schema: dynamodb_key_schema(item)?,
            projection: {
                let projection = item
                    .get("Projection")
                    .and_then(Value::as_object)
                    .ok_or_else(|| {
                        template_validation_error(
                            "LocalSecondaryIndexes entries require Projection"
                                .to_owned(),
                        )
                    })?;
                dynamodb_projection(projection)?
            },
        })
    })
    .map(|value| value.unwrap_or_default())
}

fn optional_map_array<T>(
    properties: &Map<String, Value>,
    name: &str,
    map_item: impl Fn(&Map<String, Value>) -> Result<T, CloudFormationError>,
) -> Result<Option<Vec<T>>, CloudFormationError> {
    properties
        .get(name)
        .map(|value| map_array_value(value, name, &map_item))
        .transpose()
}

fn string_map_array<T>(
    properties: &Map<String, Value>,
    name: &str,
    map_item: impl Fn(&Map<String, Value>) -> Result<T, CloudFormationError>,
) -> Result<Vec<T>, CloudFormationError> {
    properties
        .get(name)
        .ok_or_else(|| {
            template_validation_error(format!("property {name} is required"))
        })
        .and_then(|value| map_array_value(value, name, &map_item))
}

fn map_array_value<T>(
    value: &Value,
    name: &str,
    map_item: &impl Fn(&Map<String, Value>) -> Result<T, CloudFormationError>,
) -> Result<Vec<T>, CloudFormationError> {
    let items = value.as_array().ok_or_else(|| {
        template_validation_error(format!("property {name} must be an array"))
    })?;
    items
        .iter()
        .map(|item| {
            item.as_object()
                .ok_or_else(|| {
                    template_validation_error(format!(
                        "entries in property {name} must be objects"
                    ))
                })
                .and_then(map_item)
        })
        .collect()
}

fn lambda_create_input(
    scope: &CloudFormationScope,
    function_name: &str,
    runtime: &str,
    handler: &str,
    properties: &Map<String, Value>,
) -> Result<CreateFunctionInput, CloudFormationError> {
    Ok(CreateFunctionInput {
        code: lambda_code_input(runtime, handler, properties)?,
        dead_letter_target_arn: properties
            .get("DeadLetterConfig")
            .and_then(Value::as_object)
            .map(|config| required_string_field(config, "TargetArn"))
            .transpose()?,
        description: optional_string_field(properties, "Description")?,
        environment: properties
            .get("Environment")
            .and_then(Value::as_object)
            .and_then(|environment| environment.get("Variables"))
            .and_then(Value::as_object)
            .map(|variables| {
                variables
                    .iter()
                    .map(|(name, value)| {
                        Ok((name.clone(), value_to_string(value)?))
                    })
                    .collect::<Result<BTreeMap<_, _>, CloudFormationError>>()
            })
            .transpose()?
            .unwrap_or_default(),
        function_name: function_name.to_owned(),
        handler: Some(handler.to_owned()),
        memory_size: optional_u32_field(properties, "MemorySize")?,
        package_type: if optional_string_field(properties, "PackageType")?
            .as_deref()
            == Some("Image")
        {
            LambdaPackageType::Image
        } else {
            LambdaPackageType::Zip
        },
        publish: optional_bool_field(properties, "Publish")?.unwrap_or(false),
        role: required_string_field(properties, "Role").or_else(|_| {
            Ok(format!("arn:aws:iam::{}:role/default", scope.account_id()))
        })?,
        runtime: Some(runtime.to_owned()),
        timeout: optional_u32_field(properties, "Timeout")?,
    })
}

fn lambda_code_input(
    runtime: &str,
    handler: &str,
    properties: &Map<String, Value>,
) -> Result<LambdaCodeInput, CloudFormationError> {
    let code = properties.get("Code").and_then(Value::as_object);
    if let Some(code) = code {
        if let Some(zip_file) = code.get("ZipFile") {
            return Ok(LambdaCodeInput::InlineZip {
                archive: inline_zip_archive(
                    runtime,
                    handler,
                    &value_to_string(zip_file)?,
                )?,
            });
        }
        if let (Some(bucket), Some(key)) =
            (code.get("S3Bucket"), code.get("S3Key"))
        {
            return Ok(LambdaCodeInput::S3Object {
                bucket: value_to_string(bucket)?,
                key: value_to_string(key)?,
                object_version: code
                    .get("S3ObjectVersion")
                    .map(value_to_string)
                    .transpose()?,
            });
        }
    }
    Ok(LambdaCodeInput::InlineZip {
        archive: inline_zip_archive(
            runtime,
            handler,
            "def handler(event, context):\n    return {'statusCode': 200, 'body': 'ok'}\n",
        )?,
    })
}

fn inline_zip_archive(
    runtime: &str,
    handler: &str,
    source: &str,
) -> Result<Vec<u8>, CloudFormationError> {
    if !runtime.starts_with("python") {
        return Err(CloudFormationError::UnsupportedOperation {
            message: format!(
                "CloudFormation Lambda ZipFile only supports Python runtimes in Cloudish today; got {runtime}."
            ),
        });
    }
    let module = handler.split('.').next().unwrap_or("index");
    let mut writer = ZipWriter::new(std::io::Cursor::new(Vec::new()));
    writer
        .start_file(
            format!("{module}.py"),
            SimpleFileOptions::default()
                .compression_method(CompressionMethod::Stored),
        )
        .map_err(|error| {
            template_validation_error(format!(
                "failed to build Lambda inline ZipFile archive: {error}"
            ))
        })?;
    writer.write_all(source.as_bytes()).map_err(|error| {
        template_validation_error(format!(
            "failed to write Lambda inline ZipFile archive: {error}"
        ))
    })?;
    writer.finish().map(|cursor| cursor.into_inner()).map_err(|error| {
        template_validation_error(format!(
            "failed to finish Lambda inline ZipFile archive: {error}"
        ))
    })
}

fn sync_role_managed_policies(
    iam: &dyn CloudFormationIamPort,
    scope: &IamScope,
    role_name: &str,
    properties: &Map<String, Value>,
) -> Result<(), CloudFormationError> {
    let desired = optional_string_list_field(properties, "ManagedPolicyArns")?
        .unwrap_or_default()
        .into_iter()
        .map(|policy_arn| parse_managed_policy_arn(&policy_arn))
        .collect::<Result<BTreeSet<_>, _>>()?;
    let existing = iam
        .list_attached_role_policies(scope, role_name, None)
        .map_err(map_iam_error)?
        .into_iter()
        .map(|policy| policy.policy_arn)
        .collect::<BTreeSet<_>>();
    for policy_arn in existing.difference(&desired) {
        let _ = iam.detach_role_policy(scope, role_name, policy_arn);
    }
    for policy_arn in desired.difference(&existing) {
        iam.attach_role_policy(scope, role_name, policy_arn)
            .map_err(map_iam_error)?;
    }
    Ok(())
}

fn sync_role_inline_policies(
    iam: &dyn CloudFormationIamPort,
    scope: &IamScope,
    role_name: &str,
    properties: &Map<String, Value>,
) -> Result<(), CloudFormationError> {
    let mut desired = BTreeMap::new();
    if let Some(policies) = properties.get("Policies") {
        let policies = policies.as_array().ok_or_else(|| {
            template_validation_error("Role Policies must be an array")
        })?;
        for policy in policies {
            let policy = policy.as_object().ok_or_else(|| {
                template_validation_error("Role Policies must be objects")
            })?;
            desired.insert(
                required_string_field(policy, "PolicyName")?,
                compact_json(policy.get("PolicyDocument").ok_or_else(
                    || {
                        template_validation_error(
                            "Role policy requires PolicyDocument".to_owned(),
                        )
                    },
                )?),
            );
        }
    }
    let existing = iam
        .list_role_policies(scope, role_name)
        .map_err(map_iam_error)?
        .into_iter()
        .collect::<BTreeSet<_>>();
    for policy_name in existing.difference(&desired.keys().cloned().collect())
    {
        let _ = iam.delete_role_policy(scope, role_name, policy_name);
    }
    for (policy_name, document) in desired {
        iam.put_role_policy(
            scope,
            role_name,
            InlinePolicyInput { document, policy_name },
        )
        .map_err(map_iam_error)?;
    }
    Ok(())
}

fn sync_managed_policy_attachments(
    iam: &dyn CloudFormationIamPort,
    scope: &IamScope,
    policy_arn: &Arn,
    properties: &Map<String, Value>,
) -> Result<(), CloudFormationError> {
    let desired_roles = optional_string_list_field(properties, "Roles")?
        .unwrap_or_default()
        .into_iter()
        .collect::<BTreeSet<_>>();
    let current_roles = desired_roles.clone();
    for role_name in current_roles.difference(&desired_roles) {
        let _ = iam.detach_role_policy(scope, role_name, policy_arn);
    }
    for role_name in desired_roles {
        let _ = iam.attach_role_policy(scope, &role_name, policy_arn);
    }
    let desired_users =
        optional_string_list_field(properties, "Users")?.unwrap_or_default();
    for user_name in desired_users {
        let _ = iam.attach_user_policy(scope, &user_name, policy_arn);
    }
    Ok(())
}

fn parse_managed_policy_arn(value: &str) -> Result<Arn, CloudFormationError> {
    value.parse().map_err(|_| {
        template_validation_error(format!(
            "{value} must be a valid IAM policy ARN"
        ))
    })
}

fn require_dependency<'a, T: ?Sized>(
    dependency: &'a Option<Arc<T>>,
    name: &str,
) -> Result<&'a T, CloudFormationError> {
    dependency.as_deref().ok_or_else(|| CloudFormationError::UnsupportedOperation {
        message: format!(
            "CloudFormation provider {name} is not configured in this Cloudish runtime."
        ),
    })
}

fn map_s3_error(error: S3Error) -> CloudFormationError {
    CloudFormationError::Validation {
        message: format!("CloudFormation S3 provider failed: {error}"),
    }
}

fn map_sqs_error(error: SqsError) -> CloudFormationError {
    CloudFormationError::Validation {
        message: format!("CloudFormation SQS provider failed: {error}"),
    }
}

fn map_sns_error(error: SnsError) -> CloudFormationError {
    CloudFormationError::Validation {
        message: format!("CloudFormation SNS provider failed: {error}"),
    }
}

fn map_dynamodb_error(error: DynamoDbError) -> CloudFormationError {
    CloudFormationError::Validation {
        message: format!("CloudFormation DynamoDB provider failed: {error}"),
    }
}

fn map_lambda_error(error: LambdaError) -> CloudFormationError {
    CloudFormationError::Validation {
        message: format!("CloudFormation Lambda provider failed: {error}"),
    }
}

fn map_iam_error(error: IamError) -> CloudFormationError {
    CloudFormationError::Validation {
        message: format!("CloudFormation IAM provider failed: {error}"),
    }
}

fn map_ssm_error(error: SsmError) -> CloudFormationError {
    CloudFormationError::Validation {
        message: format!("CloudFormation SSM provider failed: {error}"),
    }
}

fn map_kms_error(error: KmsError) -> CloudFormationError {
    CloudFormationError::Validation {
        message: format!("CloudFormation KMS provider failed: {error}"),
    }
}

fn map_secrets_manager_error(
    error: SecretsManagerError,
) -> CloudFormationError {
    CloudFormationError::Validation {
        message: format!(
            "CloudFormation Secrets Manager provider failed: {error}"
        ),
    }
}

fn stack_not_found_error(stack_name: &str) -> CloudFormationError {
    CloudFormationError::NotFound {
        message: format!("Stack with id {stack_name} does not exist"),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        CloudFormationCreateChangeSetInput, CloudFormationDependencies,
        CloudFormationPlanInput, CloudFormationScope, CloudFormationService,
        CloudFormationStackOperationInput, CloudFormationTemplateParameter,
        ValidateTemplateInput,
    };
    use crate::CloudFormationSqsPort;
    use aws::{AccountId, RegionId};
    use serde_json::json;
    use sqs::{
        CreateQueueInput, SqsError, SqsQueueIdentity, SqsScope, SqsService,
    };
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex};

    fn scope() -> CloudFormationScope {
        CloudFormationScope::new(
            "000000000000".parse::<AccountId>().expect("account should parse"),
            "eu-west-2".parse::<RegionId>().expect("region should parse"),
        )
    }

    fn provider_service() -> (CloudFormationService, SqsService) {
        let sqs = SqsService::new();
        (
            CloudFormationService::with_dependencies(
                Arc::new(std::time::SystemTime::now),
                CloudFormationDependencies {
                    sqs: Some(Arc::new(sqs.clone())),
                    ..CloudFormationDependencies::default()
                },
            ),
            sqs,
        )
    }

    #[derive(Clone, Default)]
    struct FakeSqsPort {
        created_queues: Arc<Mutex<Vec<String>>>,
    }

    impl CloudFormationSqsPort for FakeSqsPort {
        fn create_queue(
            &self,
            scope: &SqsScope,
            input: CreateQueueInput,
        ) -> Result<SqsQueueIdentity, SqsError> {
            self.created_queues
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .push(input.queue_name.clone());
            SqsQueueIdentity::new(
                scope.account_id().clone(),
                scope.region().clone(),
                &input.queue_name,
            )
        }

        fn delete_queue(
            &self,
            _queue: &SqsQueueIdentity,
        ) -> Result<(), SqsError> {
            Ok(())
        }

        fn set_queue_attributes(
            &self,
            _queue: &SqsQueueIdentity,
            _attributes: BTreeMap<String, String>,
        ) -> Result<(), SqsError> {
            Ok(())
        }
    }

    #[test]
    fn cloudformation_parser_validate_template_reports_description_parameters_and_capabilities()
     {
        let service = CloudFormationService::new();
        let template = r#"
Description: parser story
Parameters:
  Env:
    Type: String
    Default: dev
    Description: deployment environment
Resources:
  Role:
    Type: AWS::IAM::Role
    Properties:
      AssumeRolePolicyDocument: {}
      RoleName: named-role
"#;

        let output = service
            .validate_template(
                &scope(),
                ValidateTemplateInput { template_body: template.to_owned() },
            )
            .expect("template should validate");

        assert_eq!(output.description.as_deref(), Some("parser story"));
        assert_eq!(
            output.parameters,
            vec![CloudFormationTemplateParameter {
                default_value: Some("dev".to_owned()),
                description: Some("deployment environment".to_owned()),
                no_echo: false,
                parameter_key: "Env".to_owned(),
            }]
        );
        assert_eq!(output.capabilities, vec!["CAPABILITY_NAMED_IAM"]);
        assert!(
            output
                .capabilities_reason
                .as_deref()
                .expect("IAM resources should explain capabilities")
                .contains("Role")
        );
        assert!(output.declared_transforms.is_empty());
    }

    #[test]
    fn cloudformation_parser_plan_stack_resolves_supported_intrinsics_and_pseudo_parameters()
     {
        let service = CloudFormationService::new();
        let template = r#"
Parameters:
  Prefix:
    Type: String
    Default: demo
  Env:
    Type: String
Conditions:
  UseStackSuffix: !Equals [!Ref Env, prod]
Resources:
  Queue:
    Type: AWS::SQS::Queue
    Properties:
      QueueName: !Join
        - "-"
        - - !Ref Prefix
          - !If
            - UseStackSuffix
            - !Sub "${AWS::StackName}-${AWS::Region}"
            - !Sub "${Prefix}-${AWS::AccountId}"
Outputs:
  Summary:
    Value: !Sub "stack=${AWS::StackName};region=${AWS::Region};account=${AWS::AccountId};prefix=${Prefix}"
"#;
        let mut parameters = BTreeMap::new();
        parameters.insert("Env".to_owned(), "prod".to_owned());
        let plan = service
            .plan_stack(
                &scope(),
                CloudFormationPlanInput {
                    parameters,
                    stack_name: "demo-stack".to_owned(),
                    template_body: template.to_owned(),
                },
            )
            .expect("stack planning should succeed");

        assert_eq!(plan.conditions.get("UseStackSuffix"), Some(&true));
        assert_eq!(
            plan.resources["Queue"].properties["QueueName"],
            json!("demo-demo-stack-eu-west-2")
        );
        assert_eq!(
            plan.outputs["Summary"].value,
            json!(
                "stack=demo-stack;region=eu-west-2;account=000000000000;prefix=demo"
            )
        );
    }

    #[test]
    fn cloudformation_parser_plan_stack_drops_no_value_properties() {
        let service = CloudFormationService::new();
        let template = r#"
Parameters:
  EnableDisplayName:
    Type: String
Conditions:
  IncludeDisplayName: !Equals [!Ref EnableDisplayName, yes]
Resources:
  Topic:
    Type: AWS::SNS::Topic
    Properties:
      TopicName: demo-topic
      DisplayName: !If
        - IncludeDisplayName
        - visible
        - !Ref AWS::NoValue
"#;
        let mut parameters = BTreeMap::new();
        parameters.insert("EnableDisplayName".to_owned(), "no".to_owned());
        let plan = service
            .plan_stack(
                &scope(),
                CloudFormationPlanInput {
                    parameters,
                    stack_name: "demo-stack".to_owned(),
                    template_body: template.to_owned(),
                },
            )
            .expect("stack planning should succeed");

        assert_eq!(
            plan.resources["Topic"].properties,
            json!({ "TopicName": "demo-topic" })
        );
    }

    #[test]
    fn cloudformation_parser_validate_template_rejects_unsupported_transform()
    {
        let service = CloudFormationService::new();
        let error = service
            .validate_template(
                &scope(),
                ValidateTemplateInput {
                    template_body: r#"
Transform: AWS::Serverless-2016-10-31
Resources: {}
"#
                    .to_owned(),
                },
            )
            .expect_err("transforms should fail");

        assert!(error.to_string().contains("unsupported transform"));
    }

    #[test]
    fn cloudformation_parser_validate_template_rejects_unsupported_intrinsic()
    {
        let service = CloudFormationService::new();
        let error = service
            .validate_template(
                &scope(),
                ValidateTemplateInput {
                    template_body: r#"
Resources:
  Queue:
    Type: AWS::SQS::Queue
    Properties:
      QueueName:
        Fn::Split:
          - "-"
          - "a-b"
"#
                    .to_owned(),
                },
            )
            .expect_err("unsupported intrinsic should fail");

        assert!(error.to_string().contains("unsupported intrinsic Fn::Split"));
    }

    #[test]
    fn cloudformation_parser_validate_template_rejects_unsupported_resource_type()
     {
        let service = CloudFormationService::new();
        let error = service
            .validate_template(
                &scope(),
                ValidateTemplateInput {
                    template_body: r#"
Resources:
  Machine:
    Type: AWS::StepFunctions::StateMachine
    Properties: {}
"#
                    .to_owned(),
                },
            )
            .expect_err("unsupported resource type should fail");

        assert!(
            error
                .to_string()
                .contains("unsupported type AWS::StepFunctions::StateMachine")
        );
    }

    #[test]
    fn cloudformation_parser_validate_template_accepts_supported_resource_refs()
     {
        let service = CloudFormationService::new();
        let output = service
            .validate_template(
                &scope(),
                ValidateTemplateInput {
                    template_body: r#"
Resources:
  Bucket:
    Type: AWS::S3::Bucket
Outputs:
  BucketRef:
    Value: !Ref Bucket
"#
                    .to_owned(),
                },
            )
            .expect("supported resource refs should validate");

        assert!(output.parameters.is_empty());
    }

    #[test]
    fn cloudformation_parser_validate_template_accepts_supported_get_att_attributes()
     {
        let service = CloudFormationService::new();
        let output = service
            .validate_template(
                &scope(),
                ValidateTemplateInput {
                    template_body: r#"
Resources:
  Queue:
    Type: AWS::SQS::Queue
Outputs:
  QueueArn:
    Value: !GetAtt Queue.Arn
"#
                    .to_owned(),
                },
            )
            .expect("supported Fn::GetAtt should validate");

        assert!(output.parameters.is_empty());
    }

    #[test]
    fn cloudformation_parser_validate_template_rejects_unsupported_get_att_attributes()
     {
        let service = CloudFormationService::new();
        let error = service
            .validate_template(
                &scope(),
                ValidateTemplateInput {
                    template_body: r#"
Resources:
  Queue:
    Type: AWS::SQS::Queue
Outputs:
  QueueBogus:
    Value: !GetAtt Queue.Bogus
"#
                    .to_owned(),
                },
            )
            .expect_err("unsupported Fn::GetAtt attribute should fail");

        assert!(error.to_string().contains("Fn::GetAtt attribute Bogus"));
    }

    #[test]
    fn cloudformation_providers_create_stack_provisions_supported_resources() {
        let (service, sqs) = provider_service();
        let output = service
            .create_stack(
                &scope(),
                CloudFormationStackOperationInput {
                    capabilities: Vec::new(),
                    parameters: BTreeMap::new(),
                    stack_name: "provider-stack".to_owned(),
                    template_body: Some(
                        r#"
Resources:
  Queue:
    Type: AWS::SQS::Queue
    Properties:
      QueueName: provider-queue
Outputs:
  QueueArn:
    Value: !GetAtt Queue.Arn
"#
                        .to_owned(),
                    ),
                    template_url: None,
                },
            )
            .expect("supported stack should create");

        assert!(output.stack_id.contains(":stack/provider-stack/"));

        let description = service
            .describe_stacks(&scope(), Some("provider-stack"))
            .expect("stack should describe")
            .pop()
            .expect("stack should exist");
        assert_eq!(description.stack_status, "CREATE_COMPLETE");
        assert_eq!(description.outputs.len(), 1);
        assert_eq!(description.outputs[0].output_key, "QueueArn");
        assert_eq!(
            description.outputs[0].output_value,
            "arn:aws:sqs:eu-west-2:000000000000:provider-queue"
        );

        let resources = service
            .list_stack_resources(&scope(), "provider-stack")
            .expect("stack resources should list");
        assert_eq!(resources.len(), 1);
        let queue_name = resources[0]
            .physical_resource_id
            .rsplit('/')
            .next()
            .expect("queue url should include a queue name");
        let queue_attributes = sqs
            .get_queue_attributes(
                &SqsQueueIdentity::new(
                    scope().account_id().clone(),
                    scope().region().clone(),
                    queue_name,
                )
                .expect("queue identity should build"),
                &[],
            )
            .expect("queue should exist downstream");
        assert_eq!(
            queue_attributes.get("QueueArn"),
            Some(
                &"arn:aws:sqs:eu-west-2:000000000000:provider-queue"
                    .to_owned()
            )
        );
    }

    #[test]
    fn cloudformation_providers_execute_change_set_updates_supported_resources()
     {
        let (service, sqs) = provider_service();
        service
            .create_stack(
                &scope(),
                CloudFormationStackOperationInput {
                    capabilities: Vec::new(),
                    parameters: BTreeMap::new(),
                    stack_name: "provider-change-set".to_owned(),
                    template_body: Some(
                        r#"
Resources:
  Queue:
    Type: AWS::SQS::Queue
    Properties:
      QueueName: provider-before
"#
                        .to_owned(),
                    ),
                    template_url: None,
                },
            )
            .expect("base stack should create");

        let output = service
            .create_change_set(
                &scope(),
                CloudFormationCreateChangeSetInput {
                    capabilities: Vec::new(),
                    change_set_name: "rename".to_owned(),
                    change_set_type: Some("UPDATE".to_owned()),
                    parameters: BTreeMap::new(),
                    stack_name: "provider-change-set".to_owned(),
                    template_body: Some(
                        r#"
Resources:
  Queue:
    Type: AWS::SQS::Queue
    Properties:
      QueueName: provider-after
"#
                        .to_owned(),
                    ),
                    template_url: None,
                },
            )
            .expect("change set should create");
        assert!(output.id.contains(":changeSet/rename/"));

        let change_set = service
            .describe_change_set(&scope(), "provider-change-set", "rename")
            .expect("change set should describe");
        assert_eq!(change_set.changes.len(), 1);
        assert_eq!(change_set.changes[0].action, "Modify");

        service
            .execute_change_set(&scope(), "provider-change-set", "rename")
            .expect("change set should execute");

        let resources = service
            .list_stack_resources(&scope(), "provider-change-set")
            .expect("stack resources should list");
        let queue_name = resources[0]
            .physical_resource_id
            .rsplit('/')
            .next()
            .expect("queue url should include a queue name");
        let queue_attributes = sqs
            .get_queue_attributes(
                &SqsQueueIdentity::new(
                    scope().account_id().clone(),
                    scope().region().clone(),
                    queue_name,
                )
                .expect("queue identity should build"),
                &[],
            )
            .expect("queue should exist downstream");
        assert_eq!(
            queue_attributes.get("QueueArn"),
            Some(
                &"arn:aws:sqs:eu-west-2:000000000000:provider-after"
                    .to_owned()
            )
        );
    }

    #[test]
    fn cloudformation_providers_accept_typed_sqs_ports_without_concrete_service()
     {
        let sqs = FakeSqsPort::default();
        let service = CloudFormationService::with_dependencies(
            Arc::new(std::time::SystemTime::now),
            CloudFormationDependencies {
                sqs: Some(Arc::new(sqs.clone())),
                ..CloudFormationDependencies::default()
            },
        );

        service
            .create_stack(
                &scope(),
                CloudFormationStackOperationInput {
                    capabilities: Vec::new(),
                    parameters: BTreeMap::new(),
                    stack_name: "typed-port-stack".to_owned(),
                    template_body: Some(
                        r#"
Resources:
  Queue:
    Type: AWS::SQS::Queue
    Properties:
      QueueName: typed-port-queue
"#
                        .to_owned(),
                    ),
                    template_url: None,
                },
            )
            .expect("stack should provision through the typed SQS port");

        assert_eq!(
            sqs.created_queues
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .as_slice(),
            ["typed-port-queue"]
        );
    }
}
