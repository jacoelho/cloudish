use aws::{Arn, IamAccessKeyStatus};
use dynamodb::{
    CreateTableInput as CreateDynamoDbTableInput, DynamoDbError,
    DynamoDbScope, DynamoDbService, TableName as DynamoDbTableName,
    TagResourceInput as DynamoDbTagResourceInput,
};
use iam::{
    CreateInstanceProfileInput, CreatePolicyInput, CreatePolicyVersionInput,
    CreateRoleInput, CreateUserInput, IamError, IamScope, IamService,
    InlinePolicyInput,
};
use kms::{
    CreateKmsAliasInput, CreateKmsKeyInput, DeleteKmsAliasInput, KmsError,
    KmsScope, KmsService, ScheduleKmsKeyDeletionInput,
};
use lambda::{CreateFunctionInput, LambdaError, LambdaScope, LambdaService};
use s3::{CreateBucketInput, S3Error, S3Scope, S3Service};
use secrets_manager::{
    CreateSecretInput, DeleteSecretInput, SecretsManagerError,
    SecretsManagerScope, SecretsManagerService, UpdateSecretInput,
};
use sns::{CreateTopicInput, SnsError, SnsScope, SnsService};
use sqs::{
    CreateQueueInput, SqsError, SqsQueueIdentity, SqsScope, SqsService,
};
use ssm::{
    SsmAddTagsToResourceInput, SsmDeleteParameterInput, SsmError,
    SsmPutParameterInput, SsmScope, SsmService,
};
use std::collections::BTreeMap;
use std::sync::Arc;

pub trait CloudFormationS3Port: Send + Sync {
    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn get_object(
        &self,
        scope: &S3Scope,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
    ) -> Result<s3::GetObjectOutput, S3Error>;

    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn create_bucket(
        &self,
        scope: &S3Scope,
        input: CreateBucketInput,
    ) -> Result<(), S3Error>;

    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn delete_bucket(
        &self,
        scope: &S3Scope,
        bucket: &str,
    ) -> Result<(), S3Error>;

    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn put_bucket_policy(
        &self,
        scope: &S3Scope,
        bucket: &str,
        policy: String,
    ) -> Result<(), S3Error>;

    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn delete_bucket_policy(
        &self,
        scope: &S3Scope,
        bucket: &str,
    ) -> Result<(), S3Error>;
}

impl CloudFormationS3Port for S3Service {
    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn get_object(
        &self,
        scope: &S3Scope,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
    ) -> Result<s3::GetObjectOutput, S3Error> {
        self.get_object(scope, bucket, key, version_id)
    }

    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn create_bucket(
        &self,
        scope: &S3Scope,
        input: CreateBucketInput,
    ) -> Result<(), S3Error> {
        self.create_bucket(scope, input).map(|_| ())
    }

    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn delete_bucket(
        &self,
        scope: &S3Scope,
        bucket: &str,
    ) -> Result<(), S3Error> {
        self.delete_bucket(scope, bucket)
    }

    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn put_bucket_policy(
        &self,
        scope: &S3Scope,
        bucket: &str,
        policy: String,
    ) -> Result<(), S3Error> {
        self.put_bucket_policy(scope, bucket, policy)
    }

    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn delete_bucket_policy(
        &self,
        scope: &S3Scope,
        bucket: &str,
    ) -> Result<(), S3Error> {
        self.delete_bucket_policy(scope, bucket)
    }
}

pub trait CloudFormationSqsPort: Send + Sync {
    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn create_queue(
        &self,
        scope: &SqsScope,
        input: CreateQueueInput,
    ) -> Result<SqsQueueIdentity, SqsError>;

    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn delete_queue(&self, queue: &SqsQueueIdentity) -> Result<(), SqsError>;

    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn set_queue_attributes(
        &self,
        queue: &SqsQueueIdentity,
        attributes: BTreeMap<String, String>,
    ) -> Result<(), SqsError>;
}

impl CloudFormationSqsPort for SqsService {
    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn create_queue(
        &self,
        scope: &SqsScope,
        input: CreateQueueInput,
    ) -> Result<SqsQueueIdentity, SqsError> {
        self.create_queue(scope, input)
    }

    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn delete_queue(&self, queue: &SqsQueueIdentity) -> Result<(), SqsError> {
        self.delete_queue(queue)
    }

    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn set_queue_attributes(
        &self,
        queue: &SqsQueueIdentity,
        attributes: BTreeMap<String, String>,
    ) -> Result<(), SqsError> {
        self.set_queue_attributes(queue, attributes)
    }
}

pub trait CloudFormationSnsPort: Send + Sync {
    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn create_topic(
        &self,
        scope: &SnsScope,
        input: CreateTopicInput,
    ) -> Result<Arn, SnsError>;

    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn delete_topic(&self, topic_arn: &Arn) -> Result<(), SnsError>;
}

impl CloudFormationSnsPort for SnsService {
    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn create_topic(
        &self,
        scope: &SnsScope,
        input: CreateTopicInput,
    ) -> Result<Arn, SnsError> {
        self.create_topic(scope, input)
    }

    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn delete_topic(&self, topic_arn: &Arn) -> Result<(), SnsError> {
        self.delete_topic(topic_arn)
    }
}

pub trait CloudFormationDynamoDbPort: Send + Sync {
    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn create_table(
        &self,
        scope: &DynamoDbScope,
        input: CreateDynamoDbTableInput,
    ) -> Result<dynamodb::TableDescription, DynamoDbError>;

    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn describe_table(
        &self,
        scope: &DynamoDbScope,
        table_name: &str,
    ) -> Result<dynamodb::TableDescription, DynamoDbError>;

    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn tag_resource(
        &self,
        scope: &DynamoDbScope,
        input: DynamoDbTagResourceInput,
    ) -> Result<(), DynamoDbError>;

    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn delete_table(
        &self,
        scope: &DynamoDbScope,
        table_name: &str,
    ) -> Result<(), DynamoDbError>;
}

impl CloudFormationDynamoDbPort for DynamoDbService {
    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn create_table(
        &self,
        scope: &DynamoDbScope,
        input: CreateDynamoDbTableInput,
    ) -> Result<dynamodb::TableDescription, DynamoDbError> {
        self.create_table(scope, input)
    }

    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn describe_table(
        &self,
        scope: &DynamoDbScope,
        table_name: &str,
    ) -> Result<dynamodb::TableDescription, DynamoDbError> {
        self.describe_table(scope, &DynamoDbTableName::parse(table_name)?)
    }

    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn tag_resource(
        &self,
        scope: &DynamoDbScope,
        input: DynamoDbTagResourceInput,
    ) -> Result<(), DynamoDbError> {
        self.tag_resource(scope, input)
    }

    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn delete_table(
        &self,
        scope: &DynamoDbScope,
        table_name: &str,
    ) -> Result<(), DynamoDbError> {
        self.delete_table(scope, &DynamoDbTableName::parse(table_name)?)
            .map(|_| ())
    }
}

pub trait CloudFormationLambdaPort: Send + Sync {
    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn create_function(
        &self,
        scope: &LambdaScope,
        input: CreateFunctionInput,
    ) -> Result<(), LambdaError>;

    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn delete_function(
        &self,
        scope: &LambdaScope,
        function_name: &str,
    ) -> Result<(), LambdaError>;
}

impl CloudFormationLambdaPort for LambdaService {
    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn create_function(
        &self,
        scope: &LambdaScope,
        input: CreateFunctionInput,
    ) -> Result<(), LambdaError> {
        self.create_function(scope, input).map(|_| ())
    }

    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn delete_function(
        &self,
        scope: &LambdaScope,
        function_name: &str,
    ) -> Result<(), LambdaError> {
        self.delete_function(scope, function_name)
    }
}

pub trait CloudFormationIamPort: Send + Sync {
    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn create_role(
        &self,
        scope: &IamScope,
        input: CreateRoleInput,
    ) -> Result<iam::IamRole, IamError>;

    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn get_role(
        &self,
        scope: &IamScope,
        role_name: &str,
    ) -> Result<iam::IamRole, IamError>;

    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn delete_role(
        &self,
        scope: &IamScope,
        role_name: &str,
    ) -> Result<(), IamError>;

    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn create_user(
        &self,
        scope: &IamScope,
        input: CreateUserInput,
    ) -> Result<iam::IamUser, IamError>;

    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn get_user(
        &self,
        scope: &IamScope,
        user_name: &str,
    ) -> Result<iam::IamUser, IamError>;

    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn delete_user(
        &self,
        scope: &IamScope,
        user_name: &str,
    ) -> Result<(), IamError>;

    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn create_access_key(
        &self,
        scope: &IamScope,
        user_name: &str,
    ) -> Result<iam::IamAccessKey, IamError>;

    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn update_access_key(
        &self,
        scope: &IamScope,
        user_name: &str,
        access_key_id: &str,
        status: IamAccessKeyStatus,
    ) -> Result<(), IamError>;

    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn list_access_keys(
        &self,
        scope: &IamScope,
        user_name: &str,
    ) -> Result<Vec<iam::IamAccessKeyMetadata>, IamError>;

    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn delete_access_key(
        &self,
        scope: &IamScope,
        user_name: &str,
        access_key_id: &str,
    ) -> Result<(), IamError>;

    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn create_policy(
        &self,
        scope: &IamScope,
        input: CreatePolicyInput,
    ) -> Result<iam::IamPolicy, IamError>;

    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn get_policy(
        &self,
        scope: &IamScope,
        policy_arn: &Arn,
    ) -> Result<iam::IamPolicy, IamError>;

    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn create_policy_version(
        &self,
        scope: &IamScope,
        input: CreatePolicyVersionInput,
    ) -> Result<iam::IamPolicyVersion, IamError>;

    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn delete_policy(
        &self,
        scope: &IamScope,
        policy_arn: &Arn,
    ) -> Result<(), IamError>;

    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn attach_user_policy(
        &self,
        scope: &IamScope,
        user_name: &str,
        policy_arn: &Arn,
    ) -> Result<(), IamError>;

    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn detach_user_policy(
        &self,
        scope: &IamScope,
        user_name: &str,
        policy_arn: &Arn,
    ) -> Result<(), IamError>;

    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn list_attached_user_policies(
        &self,
        scope: &IamScope,
        user_name: &str,
        path_prefix: Option<&str>,
    ) -> Result<Vec<iam::AttachedPolicy>, IamError>;

    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn attach_role_policy(
        &self,
        scope: &IamScope,
        role_name: &str,
        policy_arn: &Arn,
    ) -> Result<(), IamError>;

    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn detach_role_policy(
        &self,
        scope: &IamScope,
        role_name: &str,
        policy_arn: &Arn,
    ) -> Result<(), IamError>;

    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn list_attached_role_policies(
        &self,
        scope: &IamScope,
        role_name: &str,
        path_prefix: Option<&str>,
    ) -> Result<Vec<iam::AttachedPolicy>, IamError>;

    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn put_user_policy(
        &self,
        scope: &IamScope,
        user_name: &str,
        input: InlinePolicyInput,
    ) -> Result<(), IamError>;

    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn delete_user_policy(
        &self,
        scope: &IamScope,
        user_name: &str,
        policy_name: &str,
    ) -> Result<(), IamError>;

    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn list_user_policies(
        &self,
        scope: &IamScope,
        user_name: &str,
    ) -> Result<Vec<String>, IamError>;

    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn put_role_policy(
        &self,
        scope: &IamScope,
        role_name: &str,
        input: InlinePolicyInput,
    ) -> Result<(), IamError>;

    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn delete_role_policy(
        &self,
        scope: &IamScope,
        role_name: &str,
        policy_name: &str,
    ) -> Result<(), IamError>;

    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn list_role_policies(
        &self,
        scope: &IamScope,
        role_name: &str,
    ) -> Result<Vec<String>, IamError>;

    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn create_instance_profile(
        &self,
        scope: &IamScope,
        input: CreateInstanceProfileInput,
    ) -> Result<iam::IamInstanceProfile, IamError>;

    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn get_instance_profile(
        &self,
        scope: &IamScope,
        instance_profile_name: &str,
    ) -> Result<iam::IamInstanceProfile, IamError>;

    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn delete_instance_profile(
        &self,
        scope: &IamScope,
        instance_profile_name: &str,
    ) -> Result<(), IamError>;

    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn add_role_to_instance_profile(
        &self,
        scope: &IamScope,
        instance_profile_name: &str,
        role_name: &str,
    ) -> Result<(), IamError>;

    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn remove_role_from_instance_profile(
        &self,
        scope: &IamScope,
        instance_profile_name: &str,
        role_name: &str,
    ) -> Result<(), IamError>;

    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn list_instance_profiles_for_role(
        &self,
        scope: &IamScope,
        role_name: &str,
    ) -> Result<Vec<iam::IamInstanceProfile>, IamError>;
}

impl CloudFormationIamPort for IamService {
    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn create_role(
        &self,
        scope: &IamScope,
        input: CreateRoleInput,
    ) -> Result<iam::IamRole, IamError> {
        self.create_role(scope, input)
    }

    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn get_role(
        &self,
        scope: &IamScope,
        role_name: &str,
    ) -> Result<iam::IamRole, IamError> {
        self.get_role(scope, role_name)
    }

    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn delete_role(
        &self,
        scope: &IamScope,
        role_name: &str,
    ) -> Result<(), IamError> {
        self.delete_role(scope, role_name)
    }

    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn create_user(
        &self,
        scope: &IamScope,
        input: CreateUserInput,
    ) -> Result<iam::IamUser, IamError> {
        self.create_user(scope, input)
    }

    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn get_user(
        &self,
        scope: &IamScope,
        user_name: &str,
    ) -> Result<iam::IamUser, IamError> {
        self.get_user(scope, user_name)
    }

    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn delete_user(
        &self,
        scope: &IamScope,
        user_name: &str,
    ) -> Result<(), IamError> {
        self.delete_user(scope, user_name)
    }

    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn create_access_key(
        &self,
        scope: &IamScope,
        user_name: &str,
    ) -> Result<iam::IamAccessKey, IamError> {
        self.create_access_key(scope, user_name)
    }

    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn update_access_key(
        &self,
        scope: &IamScope,
        user_name: &str,
        access_key_id: &str,
        status: IamAccessKeyStatus,
    ) -> Result<(), IamError> {
        self.update_access_key(scope, user_name, access_key_id, status)
    }

    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn list_access_keys(
        &self,
        scope: &IamScope,
        user_name: &str,
    ) -> Result<Vec<iam::IamAccessKeyMetadata>, IamError> {
        self.list_access_keys(scope, user_name)
    }

    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn delete_access_key(
        &self,
        scope: &IamScope,
        user_name: &str,
        access_key_id: &str,
    ) -> Result<(), IamError> {
        self.delete_access_key(scope, user_name, access_key_id)
    }

    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn create_policy(
        &self,
        scope: &IamScope,
        input: CreatePolicyInput,
    ) -> Result<iam::IamPolicy, IamError> {
        self.create_policy(scope, input)
    }

    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn get_policy(
        &self,
        scope: &IamScope,
        policy_arn: &Arn,
    ) -> Result<iam::IamPolicy, IamError> {
        self.get_policy(scope, policy_arn)
    }

    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn create_policy_version(
        &self,
        scope: &IamScope,
        input: CreatePolicyVersionInput,
    ) -> Result<iam::IamPolicyVersion, IamError> {
        self.create_policy_version(scope, input)
    }

    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn delete_policy(
        &self,
        scope: &IamScope,
        policy_arn: &Arn,
    ) -> Result<(), IamError> {
        self.delete_policy(scope, policy_arn)
    }

    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn attach_user_policy(
        &self,
        scope: &IamScope,
        user_name: &str,
        policy_arn: &Arn,
    ) -> Result<(), IamError> {
        self.attach_user_policy(scope, user_name, policy_arn)
    }

    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn detach_user_policy(
        &self,
        scope: &IamScope,
        user_name: &str,
        policy_arn: &Arn,
    ) -> Result<(), IamError> {
        self.detach_user_policy(scope, user_name, policy_arn)
    }

    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn list_attached_user_policies(
        &self,
        scope: &IamScope,
        user_name: &str,
        path_prefix: Option<&str>,
    ) -> Result<Vec<iam::AttachedPolicy>, IamError> {
        self.list_attached_user_policies(scope, user_name, path_prefix)
    }

    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn attach_role_policy(
        &self,
        scope: &IamScope,
        role_name: &str,
        policy_arn: &Arn,
    ) -> Result<(), IamError> {
        self.attach_role_policy(scope, role_name, policy_arn)
    }

    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn detach_role_policy(
        &self,
        scope: &IamScope,
        role_name: &str,
        policy_arn: &Arn,
    ) -> Result<(), IamError> {
        self.detach_role_policy(scope, role_name, policy_arn)
    }

    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn list_attached_role_policies(
        &self,
        scope: &IamScope,
        role_name: &str,
        path_prefix: Option<&str>,
    ) -> Result<Vec<iam::AttachedPolicy>, IamError> {
        self.list_attached_role_policies(scope, role_name, path_prefix)
    }

    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn put_user_policy(
        &self,
        scope: &IamScope,
        user_name: &str,
        input: InlinePolicyInput,
    ) -> Result<(), IamError> {
        self.put_user_policy(scope, user_name, input)
    }

    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn delete_user_policy(
        &self,
        scope: &IamScope,
        user_name: &str,
        policy_name: &str,
    ) -> Result<(), IamError> {
        self.delete_user_policy(scope, user_name, policy_name)
    }

    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn list_user_policies(
        &self,
        scope: &IamScope,
        user_name: &str,
    ) -> Result<Vec<String>, IamError> {
        self.list_user_policies(scope, user_name)
    }

    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn put_role_policy(
        &self,
        scope: &IamScope,
        role_name: &str,
        input: InlinePolicyInput,
    ) -> Result<(), IamError> {
        self.put_role_policy(scope, role_name, input)
    }

    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn delete_role_policy(
        &self,
        scope: &IamScope,
        role_name: &str,
        policy_name: &str,
    ) -> Result<(), IamError> {
        self.delete_role_policy(scope, role_name, policy_name)
    }

    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn list_role_policies(
        &self,
        scope: &IamScope,
        role_name: &str,
    ) -> Result<Vec<String>, IamError> {
        self.list_role_policies(scope, role_name)
    }

    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn create_instance_profile(
        &self,
        scope: &IamScope,
        input: CreateInstanceProfileInput,
    ) -> Result<iam::IamInstanceProfile, IamError> {
        self.create_instance_profile(scope, input)
    }

    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn get_instance_profile(
        &self,
        scope: &IamScope,
        instance_profile_name: &str,
    ) -> Result<iam::IamInstanceProfile, IamError> {
        self.get_instance_profile(scope, instance_profile_name)
    }

    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn delete_instance_profile(
        &self,
        scope: &IamScope,
        instance_profile_name: &str,
    ) -> Result<(), IamError> {
        self.delete_instance_profile(scope, instance_profile_name)
    }

    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn add_role_to_instance_profile(
        &self,
        scope: &IamScope,
        instance_profile_name: &str,
        role_name: &str,
    ) -> Result<(), IamError> {
        self.add_role_to_instance_profile(
            scope,
            instance_profile_name,
            role_name,
        )
    }

    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn remove_role_from_instance_profile(
        &self,
        scope: &IamScope,
        instance_profile_name: &str,
        role_name: &str,
    ) -> Result<(), IamError> {
        self.remove_role_from_instance_profile(
            scope,
            instance_profile_name,
            role_name,
        )
    }

    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn list_instance_profiles_for_role(
        &self,
        scope: &IamScope,
        role_name: &str,
    ) -> Result<Vec<iam::IamInstanceProfile>, IamError> {
        self.list_instance_profiles_for_role(scope, role_name)
    }
}

pub trait CloudFormationSsmPort: Send + Sync {
    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn put_parameter(
        &self,
        scope: &SsmScope,
        input: SsmPutParameterInput,
    ) -> Result<(), SsmError>;

    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn add_tags_to_resource(
        &self,
        scope: &SsmScope,
        input: SsmAddTagsToResourceInput,
    ) -> Result<(), SsmError>;

    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn delete_parameter(
        &self,
        scope: &SsmScope,
        input: SsmDeleteParameterInput,
    ) -> Result<(), SsmError>;
}

impl CloudFormationSsmPort for SsmService {
    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn put_parameter(
        &self,
        scope: &SsmScope,
        input: SsmPutParameterInput,
    ) -> Result<(), SsmError> {
        self.put_parameter(scope, input).map(|_| ())
    }

    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn add_tags_to_resource(
        &self,
        scope: &SsmScope,
        input: SsmAddTagsToResourceInput,
    ) -> Result<(), SsmError> {
        self.add_tags_to_resource(scope, input).map(|_| ())
    }

    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn delete_parameter(
        &self,
        scope: &SsmScope,
        input: SsmDeleteParameterInput,
    ) -> Result<(), SsmError> {
        self.delete_parameter(scope, input).map(|_| ())
    }
}

pub trait CloudFormationKmsPort: Send + Sync {
    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn create_key(
        &self,
        scope: &KmsScope,
        input: CreateKmsKeyInput,
    ) -> Result<kms::CreateKmsKeyOutput, KmsError>;

    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn schedule_key_deletion(
        &self,
        scope: &KmsScope,
        input: ScheduleKmsKeyDeletionInput,
    ) -> Result<(), KmsError>;

    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn create_alias(
        &self,
        scope: &KmsScope,
        input: CreateKmsAliasInput,
    ) -> Result<(), KmsError>;

    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn delete_alias(
        &self,
        scope: &KmsScope,
        input: DeleteKmsAliasInput,
    ) -> Result<(), KmsError>;
}

impl CloudFormationKmsPort for KmsService {
    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn create_key(
        &self,
        scope: &KmsScope,
        input: CreateKmsKeyInput,
    ) -> Result<kms::CreateKmsKeyOutput, KmsError> {
        self.create_key(scope, input)
    }

    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn schedule_key_deletion(
        &self,
        scope: &KmsScope,
        input: ScheduleKmsKeyDeletionInput,
    ) -> Result<(), KmsError> {
        self.schedule_key_deletion(scope, input).map(|_| ())
    }

    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn create_alias(
        &self,
        scope: &KmsScope,
        input: CreateKmsAliasInput,
    ) -> Result<(), KmsError> {
        self.create_alias(scope, input)
    }

    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn delete_alias(
        &self,
        scope: &KmsScope,
        input: DeleteKmsAliasInput,
    ) -> Result<(), KmsError> {
        self.delete_alias(scope, input)
    }
}

pub trait CloudFormationSecretsManagerPort: Send + Sync {
    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn create_secret(
        &self,
        scope: &SecretsManagerScope,
        input: CreateSecretInput,
    ) -> Result<secrets_manager::CreateSecretOutput, SecretsManagerError>;

    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn update_secret(
        &self,
        scope: &SecretsManagerScope,
        input: UpdateSecretInput,
    ) -> Result<secrets_manager::UpdateSecretOutput, SecretsManagerError>;

    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn delete_secret(
        &self,
        scope: &SecretsManagerScope,
        input: DeleteSecretInput,
    ) -> Result<(), SecretsManagerError>;
}

impl CloudFormationSecretsManagerPort for SecretsManagerService {
    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn create_secret(
        &self,
        scope: &SecretsManagerScope,
        input: CreateSecretInput,
    ) -> Result<secrets_manager::CreateSecretOutput, SecretsManagerError> {
        self.create_secret(scope, input)
    }

    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn update_secret(
        &self,
        scope: &SecretsManagerScope,
        input: UpdateSecretInput,
    ) -> Result<secrets_manager::UpdateSecretOutput, SecretsManagerError> {
        self.update_secret(scope, input)
    }

    #[doc = "# Errors\n\nReturns the downstream service error when the provider operation fails or the referenced resource identifiers are invalid."]
    fn delete_secret(
        &self,
        scope: &SecretsManagerScope,
        input: DeleteSecretInput,
    ) -> Result<(), SecretsManagerError> {
        self.delete_secret(scope, input).map(|_| ())
    }
}

/// CloudFormation-owned cross-service ports for `TemplateURL` fetches and
/// supported resource-provider operations.
#[derive(Clone, Default)]
pub struct CloudFormationDependencies {
    pub dynamodb: Option<Arc<dyn CloudFormationDynamoDbPort>>,
    pub iam: Option<Arc<dyn CloudFormationIamPort>>,
    pub kms: Option<Arc<dyn CloudFormationKmsPort>>,
    pub lambda: Option<Arc<dyn CloudFormationLambdaPort>>,
    pub s3: Option<Arc<dyn CloudFormationS3Port>>,
    pub secrets_manager: Option<Arc<dyn CloudFormationSecretsManagerPort>>,
    pub sns: Option<Arc<dyn CloudFormationSnsPort>>,
    pub sqs: Option<Arc<dyn CloudFormationSqsPort>>,
    pub ssm: Option<Arc<dyn CloudFormationSsmPort>>,
}
