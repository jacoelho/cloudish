use crate::{
    access_keys::{IamAccessKey, IamAccessKeyMetadata},
    documents::{IamTag, InlinePolicy, InlinePolicyInput},
    errors::IamError,
    groups::{CreateGroupInput, GroupDetails, IamGroup},
    instance_profiles::{CreateInstanceProfileInput, IamInstanceProfile},
    policies::{
        AttachedPolicy, CreatePolicyInput, CreatePolicyVersionInput,
        IamPolicy, IamPolicyVersion,
    },
    roles::{CreateRoleInput, IamRole},
    scope::IamScope,
    users::{CreateUserInput, IamUser},
};
use aws::{
    AccountId, Arn, ArnResource, IamAccessKeyLookup, IamAccessKeyRecord,
    IamAccessKeyStatus, IamInstanceProfileLookup, IamInstanceProfileRecord,
    IamResourceTag, IamRoleRecord, RegionId,
};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

const DEFAULT_PATH: &str = "/";
const MAX_ACCESS_KEYS_PER_USER: usize = 2;
const IAM_PATH_MAX: usize = 512;
const MAX_POLICY_VERSIONS: usize = 5;
const MAX_SESSION_DURATION: u32 = 43_200;
const MAX_TAGS: usize = 50;
const MIN_SESSION_DURATION: u32 = 3_600;
const REQUEST_DATE_PREFIX: &str = "2026-03-24";
const USER_NAME_MAX: usize = 64;
const ENTITY_NAME_MAX: usize = 128;

#[derive(Debug, Clone, Default)]
pub struct IamService {
    state: Arc<Mutex<IamWorld>>,
}

impl IamService {
    pub fn new() -> Self {
        Self::default()
    }

    /// Performs the requested IAM operation for the provided scoped state.
    ///
    /// # Errors
    ///
    /// Returns IAM validation, lookup, conflict, or state-mutation errors when the request cannot be applied.
    pub fn create_user(
        &self,
        scope: &IamScope,
        input: CreateUserInput,
    ) -> Result<IamUser, IamError> {
        self.with_scope_mut(scope, |state| {
            state.create_user(scope.account_id(), input)
        })
    }

    /// Performs the requested IAM operation for the provided scoped state.
    ///
    /// # Errors
    ///
    /// Returns IAM validation, lookup, conflict, or state-mutation errors when the request cannot be applied.
    pub fn get_user(
        &self,
        scope: &IamScope,
        user_name: &str,
    ) -> Result<IamUser, IamError> {
        self.with_scope(scope, |state| state.get_user(user_name))
    }

    pub fn list_users(
        &self,
        scope: &IamScope,
        path_prefix: Option<&str>,
    ) -> Vec<IamUser> {
        self.with_scope(scope, |state| state.list_users(path_prefix))
    }

    /// Performs the requested IAM operation for the provided scoped state.
    ///
    /// # Errors
    ///
    /// Returns IAM validation, lookup, conflict, or state-mutation errors when the request cannot be applied.
    pub fn delete_user(
        &self,
        scope: &IamScope,
        user_name: &str,
    ) -> Result<(), IamError> {
        self.with_scope_mut(scope, |state| state.delete_user(user_name))
    }

    /// Performs the requested IAM operation for the provided scoped state.
    ///
    /// # Errors
    ///
    /// Returns IAM validation, lookup, conflict, or state-mutation errors when the request cannot be applied.
    pub fn tag_user(
        &self,
        scope: &IamScope,
        user_name: &str,
        tags: Vec<IamTag>,
    ) -> Result<(), IamError> {
        self.with_scope_mut(scope, |state| state.tag_user(user_name, tags))
    }

    /// Performs the requested IAM operation for the provided scoped state.
    ///
    /// # Errors
    ///
    /// Returns IAM validation, lookup, conflict, or state-mutation errors when the request cannot be applied.
    pub fn untag_user(
        &self,
        scope: &IamScope,
        user_name: &str,
        tag_keys: Vec<String>,
    ) -> Result<(), IamError> {
        self.with_scope_mut(scope, |state| {
            state.untag_user(user_name, tag_keys)
        })
    }

    /// Performs the requested IAM operation for the provided scoped state.
    ///
    /// # Errors
    ///
    /// Returns IAM validation, lookup, conflict, or state-mutation errors when the request cannot be applied.
    pub fn list_user_tags(
        &self,
        scope: &IamScope,
        user_name: &str,
    ) -> Result<Vec<IamTag>, IamError> {
        self.with_scope(scope, |state| state.list_user_tags(user_name))
    }

    /// Performs the requested IAM operation for the provided scoped state.
    ///
    /// # Errors
    ///
    /// Returns IAM validation, lookup, conflict, or state-mutation errors when the request cannot be applied.
    pub fn create_access_key(
        &self,
        scope: &IamScope,
        user_name: &str,
    ) -> Result<IamAccessKey, IamError> {
        let mut guard =
            self.state.lock().unwrap_or_else(|poison| poison.into_inner());
        let (access_key_id, secret_access_key) =
            guard.next_access_key_material();
        let state = guard.scopes.entry(scope_key(scope)).or_default();
        state.create_access_key(access_key_id, secret_access_key, user_name)
    }

    /// Performs the requested IAM operation for the provided scoped state.
    ///
    /// # Errors
    ///
    /// Returns IAM validation, lookup, conflict, or state-mutation errors when the request cannot be applied.
    pub fn delete_access_key(
        &self,
        scope: &IamScope,
        user_name: &str,
        access_key_id: &str,
    ) -> Result<(), IamError> {
        self.with_scope_mut(scope, |state| {
            state.delete_access_key(user_name, access_key_id)
        })
    }

    /// Performs the requested IAM operation for the provided scoped state.
    ///
    /// # Errors
    ///
    /// Returns IAM validation, lookup, conflict, or state-mutation errors when the request cannot be applied.
    pub fn list_access_keys(
        &self,
        scope: &IamScope,
        user_name: &str,
    ) -> Result<Vec<IamAccessKeyMetadata>, IamError> {
        self.with_scope(scope, |state| state.list_access_keys(user_name))
    }

    /// Performs the requested IAM operation for the provided scoped state.
    ///
    /// # Errors
    ///
    /// Returns IAM validation, lookup, conflict, or state-mutation errors when the request cannot be applied.
    pub fn update_access_key(
        &self,
        scope: &IamScope,
        user_name: &str,
        access_key_id: &str,
        status: IamAccessKeyStatus,
    ) -> Result<(), IamError> {
        self.with_scope_mut(scope, |state| {
            state.update_access_key(user_name, access_key_id, status)
        })
    }

    /// Performs the requested IAM operation for the provided scoped state.
    ///
    /// # Errors
    ///
    /// Returns IAM validation, lookup, conflict, or state-mutation errors when the request cannot be applied.
    pub fn create_group(
        &self,
        scope: &IamScope,
        input: CreateGroupInput,
    ) -> Result<IamGroup, IamError> {
        self.with_scope_mut(scope, |state| {
            state.create_group(scope.account_id(), input)
        })
    }

    /// Performs the requested IAM operation for the provided scoped state.
    ///
    /// # Errors
    ///
    /// Returns IAM validation, lookup, conflict, or state-mutation errors when the request cannot be applied.
    pub fn get_group(
        &self,
        scope: &IamScope,
        group_name: &str,
    ) -> Result<GroupDetails, IamError> {
        self.with_scope(scope, |state| state.get_group(group_name))
    }

    pub fn list_groups(
        &self,
        scope: &IamScope,
        path_prefix: Option<&str>,
    ) -> Vec<IamGroup> {
        self.with_scope(scope, |state| state.list_groups(path_prefix))
    }

    /// Performs the requested IAM operation for the provided scoped state.
    ///
    /// # Errors
    ///
    /// Returns IAM validation, lookup, conflict, or state-mutation errors when the request cannot be applied.
    pub fn delete_group(
        &self,
        scope: &IamScope,
        group_name: &str,
    ) -> Result<(), IamError> {
        self.with_scope_mut(scope, |state| state.delete_group(group_name))
    }

    /// Performs the requested IAM operation for the provided scoped state.
    ///
    /// # Errors
    ///
    /// Returns IAM validation, lookup, conflict, or state-mutation errors when the request cannot be applied.
    pub fn add_user_to_group(
        &self,
        scope: &IamScope,
        group_name: &str,
        user_name: &str,
    ) -> Result<(), IamError> {
        self.with_scope_mut(scope, |state| {
            state.add_user_to_group(group_name, user_name)
        })
    }

    /// Performs the requested IAM operation for the provided scoped state.
    ///
    /// # Errors
    ///
    /// Returns IAM validation, lookup, conflict, or state-mutation errors when the request cannot be applied.
    pub fn remove_user_from_group(
        &self,
        scope: &IamScope,
        group_name: &str,
        user_name: &str,
    ) -> Result<(), IamError> {
        self.with_scope_mut(scope, |state| {
            state.remove_user_from_group(group_name, user_name)
        })
    }

    /// Performs the requested IAM operation for the provided scoped state.
    ///
    /// # Errors
    ///
    /// Returns IAM validation, lookup, conflict, or state-mutation errors when the request cannot be applied.
    pub fn list_groups_for_user(
        &self,
        scope: &IamScope,
        user_name: &str,
    ) -> Result<Vec<IamGroup>, IamError> {
        self.with_scope(scope, |state| state.list_groups_for_user(user_name))
    }

    /// Performs the requested IAM operation for the provided scoped state.
    ///
    /// # Errors
    ///
    /// Returns IAM validation, lookup, conflict, or state-mutation errors when the request cannot be applied.
    pub fn create_role(
        &self,
        scope: &IamScope,
        input: CreateRoleInput,
    ) -> Result<IamRole, IamError> {
        self.with_scope_mut(scope, |state| {
            state.create_role(scope.account_id(), input)
        })
    }

    /// Performs the requested IAM operation for the provided scoped state.
    ///
    /// # Errors
    ///
    /// Returns IAM validation, lookup, conflict, or state-mutation errors when the request cannot be applied.
    pub fn get_role(
        &self,
        scope: &IamScope,
        role_name: &str,
    ) -> Result<IamRole, IamError> {
        self.with_scope(scope, |state| state.get_role(role_name))
    }

    pub fn list_roles(
        &self,
        scope: &IamScope,
        path_prefix: Option<&str>,
    ) -> Vec<IamRole> {
        self.with_scope(scope, |state| state.list_roles(path_prefix))
    }

    /// Performs the requested IAM operation for the provided scoped state.
    ///
    /// # Errors
    ///
    /// Returns IAM validation, lookup, conflict, or state-mutation errors when the request cannot be applied.
    pub fn delete_role(
        &self,
        scope: &IamScope,
        role_name: &str,
    ) -> Result<(), IamError> {
        self.with_scope_mut(scope, |state| state.delete_role(role_name))
    }

    /// Performs the requested IAM operation for the provided scoped state.
    ///
    /// # Errors
    ///
    /// Returns IAM validation, lookup, conflict, or state-mutation errors when the request cannot be applied.
    pub fn tag_role(
        &self,
        scope: &IamScope,
        role_name: &str,
        tags: Vec<IamTag>,
    ) -> Result<(), IamError> {
        self.with_scope_mut(scope, |state| state.tag_role(role_name, tags))
    }

    /// Performs the requested IAM operation for the provided scoped state.
    ///
    /// # Errors
    ///
    /// Returns IAM validation, lookup, conflict, or state-mutation errors when the request cannot be applied.
    pub fn untag_role(
        &self,
        scope: &IamScope,
        role_name: &str,
        tag_keys: Vec<String>,
    ) -> Result<(), IamError> {
        self.with_scope_mut(scope, |state| {
            state.untag_role(role_name, tag_keys)
        })
    }

    /// Performs the requested IAM operation for the provided scoped state.
    ///
    /// # Errors
    ///
    /// Returns IAM validation, lookup, conflict, or state-mutation errors when the request cannot be applied.
    pub fn list_role_tags(
        &self,
        scope: &IamScope,
        role_name: &str,
    ) -> Result<Vec<IamTag>, IamError> {
        self.with_scope(scope, |state| state.list_role_tags(role_name))
    }

    /// Performs the requested IAM operation for the provided scoped state.
    ///
    /// # Errors
    ///
    /// Returns IAM validation, lookup, conflict, or state-mutation errors when the request cannot be applied.
    pub fn create_instance_profile(
        &self,
        scope: &IamScope,
        input: CreateInstanceProfileInput,
    ) -> Result<IamInstanceProfile, IamError> {
        self.with_scope_mut(scope, |state| {
            state.create_instance_profile(scope.account_id(), input)
        })
    }

    /// Performs the requested IAM operation for the provided scoped state.
    ///
    /// # Errors
    ///
    /// Returns IAM validation, lookup, conflict, or state-mutation errors when the request cannot be applied.
    pub fn get_instance_profile(
        &self,
        scope: &IamScope,
        instance_profile_name: &str,
    ) -> Result<IamInstanceProfile, IamError> {
        self.with_scope(scope, |state| {
            state.get_instance_profile(instance_profile_name)
        })
    }

    pub fn list_instance_profiles(
        &self,
        scope: &IamScope,
        path_prefix: Option<&str>,
    ) -> Vec<IamInstanceProfile> {
        self.with_scope(scope, |state| {
            state.list_instance_profiles(path_prefix)
        })
    }

    /// Performs the requested IAM operation for the provided scoped state.
    ///
    /// # Errors
    ///
    /// Returns IAM validation, lookup, conflict, or state-mutation errors when the request cannot be applied.
    pub fn delete_instance_profile(
        &self,
        scope: &IamScope,
        instance_profile_name: &str,
    ) -> Result<(), IamError> {
        self.with_scope_mut(scope, |state| {
            state.delete_instance_profile(instance_profile_name)
        })
    }

    /// Performs the requested IAM operation for the provided scoped state.
    ///
    /// # Errors
    ///
    /// Returns IAM validation, lookup, conflict, or state-mutation errors when the request cannot be applied.
    pub fn add_role_to_instance_profile(
        &self,
        scope: &IamScope,
        instance_profile_name: &str,
        role_name: &str,
    ) -> Result<(), IamError> {
        self.with_scope_mut(scope, |state| {
            state
                .add_role_to_instance_profile(instance_profile_name, role_name)
        })
    }

    /// Performs the requested IAM operation for the provided scoped state.
    ///
    /// # Errors
    ///
    /// Returns IAM validation, lookup, conflict, or state-mutation errors when the request cannot be applied.
    pub fn remove_role_from_instance_profile(
        &self,
        scope: &IamScope,
        instance_profile_name: &str,
        role_name: &str,
    ) -> Result<(), IamError> {
        self.with_scope_mut(scope, |state| {
            state.remove_role_from_instance_profile(
                instance_profile_name,
                role_name,
            )
        })
    }

    /// Performs the requested IAM operation for the provided scoped state.
    ///
    /// # Errors
    ///
    /// Returns IAM validation, lookup, conflict, or state-mutation errors when the request cannot be applied.
    pub fn list_instance_profiles_for_role(
        &self,
        scope: &IamScope,
        role_name: &str,
    ) -> Result<Vec<IamInstanceProfile>, IamError> {
        self.with_scope(scope, |state| {
            state.list_instance_profiles_for_role(role_name)
        })
    }

    /// Performs the requested IAM operation for the provided scoped state.
    ///
    /// # Errors
    ///
    /// Returns IAM validation, lookup, conflict, or state-mutation errors when the request cannot be applied.
    pub fn tag_instance_profile(
        &self,
        scope: &IamScope,
        instance_profile_name: &str,
        tags: Vec<IamTag>,
    ) -> Result<(), IamError> {
        self.with_scope_mut(scope, |state| {
            state.tag_instance_profile(instance_profile_name, tags)
        })
    }

    /// Performs the requested IAM operation for the provided scoped state.
    ///
    /// # Errors
    ///
    /// Returns IAM validation, lookup, conflict, or state-mutation errors when the request cannot be applied.
    pub fn untag_instance_profile(
        &self,
        scope: &IamScope,
        instance_profile_name: &str,
        tag_keys: Vec<String>,
    ) -> Result<(), IamError> {
        self.with_scope_mut(scope, |state| {
            state.untag_instance_profile(instance_profile_name, tag_keys)
        })
    }

    /// Performs the requested IAM operation for the provided scoped state.
    ///
    /// # Errors
    ///
    /// Returns IAM validation, lookup, conflict, or state-mutation errors when the request cannot be applied.
    pub fn list_instance_profile_tags(
        &self,
        scope: &IamScope,
        instance_profile_name: &str,
    ) -> Result<Vec<IamTag>, IamError> {
        self.with_scope(scope, |state| {
            state.list_instance_profile_tags(instance_profile_name)
        })
    }

    /// Performs the requested IAM operation for the provided scoped state.
    ///
    /// # Errors
    ///
    /// Returns IAM validation, lookup, conflict, or state-mutation errors when the request cannot be applied.
    pub fn create_policy(
        &self,
        scope: &IamScope,
        input: CreatePolicyInput,
    ) -> Result<IamPolicy, IamError> {
        self.with_scope_mut(scope, |state| {
            state.create_policy(scope.account_id(), input)
        })
    }

    /// Performs the requested IAM operation for the provided scoped state.
    ///
    /// # Errors
    ///
    /// Returns IAM validation, lookup, conflict, or state-mutation errors when the request cannot be applied.
    pub fn get_policy(
        &self,
        scope: &IamScope,
        policy_arn: &Arn,
    ) -> Result<IamPolicy, IamError> {
        let policy_arn = policy_arn.to_string();
        self.with_scope(scope, |state| state.get_policy(&policy_arn))
    }

    /// Performs the requested IAM operation for the provided scoped state.
    ///
    /// # Errors
    ///
    /// Returns IAM validation, lookup, conflict, or state-mutation errors when the request cannot be applied.
    pub fn list_policies(
        &self,
        scope: &IamScope,
        scope_filter: Option<&str>,
        path_prefix: Option<&str>,
    ) -> Result<Vec<IamPolicy>, IamError> {
        self.with_scope(scope, |state| {
            state.list_policies(scope_filter, path_prefix)
        })
    }

    /// Performs the requested IAM operation for the provided scoped state.
    ///
    /// # Errors
    ///
    /// Returns IAM validation, lookup, conflict, or state-mutation errors when the request cannot be applied.
    pub fn delete_policy(
        &self,
        scope: &IamScope,
        policy_arn: &Arn,
    ) -> Result<(), IamError> {
        let policy_arn = policy_arn.to_string();
        self.with_scope_mut(scope, |state| state.delete_policy(&policy_arn))
    }

    /// Performs the requested IAM operation for the provided scoped state.
    ///
    /// # Errors
    ///
    /// Returns IAM validation, lookup, conflict, or state-mutation errors when the request cannot be applied.
    pub fn create_policy_version(
        &self,
        scope: &IamScope,
        input: CreatePolicyVersionInput,
    ) -> Result<IamPolicyVersion, IamError> {
        self.with_scope_mut(scope, |state| state.create_policy_version(input))
    }

    /// Performs the requested IAM operation for the provided scoped state.
    ///
    /// # Errors
    ///
    /// Returns IAM validation, lookup, conflict, or state-mutation errors when the request cannot be applied.
    pub fn get_policy_version(
        &self,
        scope: &IamScope,
        policy_arn: &Arn,
        version_id: &str,
    ) -> Result<IamPolicyVersion, IamError> {
        let policy_arn = policy_arn.to_string();
        self.with_scope(scope, |state| {
            state.get_policy_version(&policy_arn, version_id)
        })
    }

    /// Performs the requested IAM operation for the provided scoped state.
    ///
    /// # Errors
    ///
    /// Returns IAM validation, lookup, conflict, or state-mutation errors when the request cannot be applied.
    pub fn delete_policy_version(
        &self,
        scope: &IamScope,
        policy_arn: &Arn,
        version_id: &str,
    ) -> Result<(), IamError> {
        let policy_arn = policy_arn.to_string();
        self.with_scope_mut(scope, |state| {
            state.delete_policy_version(&policy_arn, version_id)
        })
    }

    /// Performs the requested IAM operation for the provided scoped state.
    ///
    /// # Errors
    ///
    /// Returns IAM validation, lookup, conflict, or state-mutation errors when the request cannot be applied.
    pub fn list_policy_versions(
        &self,
        scope: &IamScope,
        policy_arn: &Arn,
    ) -> Result<Vec<IamPolicyVersion>, IamError> {
        let policy_arn = policy_arn.to_string();
        self.with_scope(scope, |state| state.list_policy_versions(&policy_arn))
    }

    /// Performs the requested IAM operation for the provided scoped state.
    ///
    /// # Errors
    ///
    /// Returns IAM validation, lookup, conflict, or state-mutation errors when the request cannot be applied.
    pub fn set_default_policy_version(
        &self,
        scope: &IamScope,
        policy_arn: &Arn,
        version_id: &str,
    ) -> Result<(), IamError> {
        let policy_arn = policy_arn.to_string();
        self.with_scope_mut(scope, |state| {
            state.set_default_policy_version(&policy_arn, version_id)
        })
    }

    /// Performs the requested IAM operation for the provided scoped state.
    ///
    /// # Errors
    ///
    /// Returns IAM validation, lookup, conflict, or state-mutation errors when the request cannot be applied.
    pub fn tag_policy(
        &self,
        scope: &IamScope,
        policy_arn: &Arn,
        tags: Vec<IamTag>,
    ) -> Result<(), IamError> {
        let policy_arn = policy_arn.to_string();
        self.with_scope_mut(scope, |state| state.tag_policy(&policy_arn, tags))
    }

    /// Performs the requested IAM operation for the provided scoped state.
    ///
    /// # Errors
    ///
    /// Returns IAM validation, lookup, conflict, or state-mutation errors when the request cannot be applied.
    pub fn untag_policy(
        &self,
        scope: &IamScope,
        policy_arn: &Arn,
        tag_keys: Vec<String>,
    ) -> Result<(), IamError> {
        let policy_arn = policy_arn.to_string();
        self.with_scope_mut(scope, |state| {
            state.untag_policy(&policy_arn, tag_keys)
        })
    }

    /// Performs the requested IAM operation for the provided scoped state.
    ///
    /// # Errors
    ///
    /// Returns IAM validation, lookup, conflict, or state-mutation errors when the request cannot be applied.
    pub fn list_policy_tags(
        &self,
        scope: &IamScope,
        policy_arn: &Arn,
    ) -> Result<Vec<IamTag>, IamError> {
        let policy_arn = policy_arn.to_string();
        self.with_scope(scope, |state| state.list_policy_tags(&policy_arn))
    }

    /// Performs the requested IAM operation for the provided scoped state.
    ///
    /// # Errors
    ///
    /// Returns IAM validation, lookup, conflict, or state-mutation errors when the request cannot be applied.
    pub fn attach_user_policy(
        &self,
        scope: &IamScope,
        user_name: &str,
        policy_arn: &Arn,
    ) -> Result<(), IamError> {
        let policy_arn = policy_arn.to_string();
        self.with_scope_mut(scope, |state| {
            state.attach_user_policy(user_name, &policy_arn)
        })
    }

    /// Performs the requested IAM operation for the provided scoped state.
    ///
    /// # Errors
    ///
    /// Returns IAM validation, lookup, conflict, or state-mutation errors when the request cannot be applied.
    pub fn detach_user_policy(
        &self,
        scope: &IamScope,
        user_name: &str,
        policy_arn: &Arn,
    ) -> Result<(), IamError> {
        let policy_arn = policy_arn.to_string();
        self.with_scope_mut(scope, |state| {
            state.detach_user_policy(user_name, &policy_arn)
        })
    }

    /// Performs the requested IAM operation for the provided scoped state.
    ///
    /// # Errors
    ///
    /// Returns IAM validation, lookup, conflict, or state-mutation errors when the request cannot be applied.
    pub fn list_attached_user_policies(
        &self,
        scope: &IamScope,
        user_name: &str,
        path_prefix: Option<&str>,
    ) -> Result<Vec<AttachedPolicy>, IamError> {
        self.with_scope(scope, |state| {
            state.list_attached_user_policies(user_name, path_prefix)
        })
    }

    /// Performs the requested IAM operation for the provided scoped state.
    ///
    /// # Errors
    ///
    /// Returns IAM validation, lookup, conflict, or state-mutation errors when the request cannot be applied.
    pub fn attach_group_policy(
        &self,
        scope: &IamScope,
        group_name: &str,
        policy_arn: &Arn,
    ) -> Result<(), IamError> {
        let policy_arn = policy_arn.to_string();
        self.with_scope_mut(scope, |state| {
            state.attach_group_policy(group_name, &policy_arn)
        })
    }

    /// Performs the requested IAM operation for the provided scoped state.
    ///
    /// # Errors
    ///
    /// Returns IAM validation, lookup, conflict, or state-mutation errors when the request cannot be applied.
    pub fn detach_group_policy(
        &self,
        scope: &IamScope,
        group_name: &str,
        policy_arn: &Arn,
    ) -> Result<(), IamError> {
        let policy_arn = policy_arn.to_string();
        self.with_scope_mut(scope, |state| {
            state.detach_group_policy(group_name, &policy_arn)
        })
    }

    /// Performs the requested IAM operation for the provided scoped state.
    ///
    /// # Errors
    ///
    /// Returns IAM validation, lookup, conflict, or state-mutation errors when the request cannot be applied.
    pub fn list_attached_group_policies(
        &self,
        scope: &IamScope,
        group_name: &str,
        path_prefix: Option<&str>,
    ) -> Result<Vec<AttachedPolicy>, IamError> {
        self.with_scope(scope, |state| {
            state.list_attached_group_policies(group_name, path_prefix)
        })
    }

    /// Performs the requested IAM operation for the provided scoped state.
    ///
    /// # Errors
    ///
    /// Returns IAM validation, lookup, conflict, or state-mutation errors when the request cannot be applied.
    pub fn attach_role_policy(
        &self,
        scope: &IamScope,
        role_name: &str,
        policy_arn: &Arn,
    ) -> Result<(), IamError> {
        let policy_arn = policy_arn.to_string();
        self.with_scope_mut(scope, |state| {
            state.attach_role_policy(role_name, &policy_arn)
        })
    }

    /// Performs the requested IAM operation for the provided scoped state.
    ///
    /// # Errors
    ///
    /// Returns IAM validation, lookup, conflict, or state-mutation errors when the request cannot be applied.
    pub fn detach_role_policy(
        &self,
        scope: &IamScope,
        role_name: &str,
        policy_arn: &Arn,
    ) -> Result<(), IamError> {
        let policy_arn = policy_arn.to_string();
        self.with_scope_mut(scope, |state| {
            state.detach_role_policy(role_name, &policy_arn)
        })
    }

    /// Performs the requested IAM operation for the provided scoped state.
    ///
    /// # Errors
    ///
    /// Returns IAM validation, lookup, conflict, or state-mutation errors when the request cannot be applied.
    pub fn list_attached_role_policies(
        &self,
        scope: &IamScope,
        role_name: &str,
        path_prefix: Option<&str>,
    ) -> Result<Vec<AttachedPolicy>, IamError> {
        self.with_scope(scope, |state| {
            state.list_attached_role_policies(role_name, path_prefix)
        })
    }

    /// Performs the requested IAM operation for the provided scoped state.
    ///
    /// # Errors
    ///
    /// Returns IAM validation, lookup, conflict, or state-mutation errors when the request cannot be applied.
    pub fn put_user_policy(
        &self,
        scope: &IamScope,
        user_name: &str,
        input: InlinePolicyInput,
    ) -> Result<(), IamError> {
        self.with_scope_mut(scope, |state| {
            state.put_user_policy(user_name, input)
        })
    }

    /// Performs the requested IAM operation for the provided scoped state.
    ///
    /// # Errors
    ///
    /// Returns IAM validation, lookup, conflict, or state-mutation errors when the request cannot be applied.
    pub fn get_user_policy(
        &self,
        scope: &IamScope,
        user_name: &str,
        policy_name: &str,
    ) -> Result<InlinePolicy, IamError> {
        self.with_scope(scope, |state| {
            state.get_user_policy(user_name, policy_name)
        })
    }

    /// Performs the requested IAM operation for the provided scoped state.
    ///
    /// # Errors
    ///
    /// Returns IAM validation, lookup, conflict, or state-mutation errors when the request cannot be applied.
    pub fn delete_user_policy(
        &self,
        scope: &IamScope,
        user_name: &str,
        policy_name: &str,
    ) -> Result<(), IamError> {
        self.with_scope_mut(scope, |state| {
            state.delete_user_policy(user_name, policy_name)
        })
    }

    /// Performs the requested IAM operation for the provided scoped state.
    ///
    /// # Errors
    ///
    /// Returns IAM validation, lookup, conflict, or state-mutation errors when the request cannot be applied.
    pub fn list_user_policies(
        &self,
        scope: &IamScope,
        user_name: &str,
    ) -> Result<Vec<String>, IamError> {
        self.with_scope(scope, |state| state.list_user_policies(user_name))
    }

    /// Performs the requested IAM operation for the provided scoped state.
    ///
    /// # Errors
    ///
    /// Returns IAM validation, lookup, conflict, or state-mutation errors when the request cannot be applied.
    pub fn put_group_policy(
        &self,
        scope: &IamScope,
        group_name: &str,
        input: InlinePolicyInput,
    ) -> Result<(), IamError> {
        self.with_scope_mut(scope, |state| {
            state.put_group_policy(group_name, input)
        })
    }

    /// Performs the requested IAM operation for the provided scoped state.
    ///
    /// # Errors
    ///
    /// Returns IAM validation, lookup, conflict, or state-mutation errors when the request cannot be applied.
    pub fn get_group_policy(
        &self,
        scope: &IamScope,
        group_name: &str,
        policy_name: &str,
    ) -> Result<InlinePolicy, IamError> {
        self.with_scope(scope, |state| {
            state.get_group_policy(group_name, policy_name)
        })
    }

    /// Performs the requested IAM operation for the provided scoped state.
    ///
    /// # Errors
    ///
    /// Returns IAM validation, lookup, conflict, or state-mutation errors when the request cannot be applied.
    pub fn delete_group_policy(
        &self,
        scope: &IamScope,
        group_name: &str,
        policy_name: &str,
    ) -> Result<(), IamError> {
        self.with_scope_mut(scope, |state| {
            state.delete_group_policy(group_name, policy_name)
        })
    }

    /// Performs the requested IAM operation for the provided scoped state.
    ///
    /// # Errors
    ///
    /// Returns IAM validation, lookup, conflict, or state-mutation errors when the request cannot be applied.
    pub fn list_group_policies(
        &self,
        scope: &IamScope,
        group_name: &str,
    ) -> Result<Vec<String>, IamError> {
        self.with_scope(scope, |state| state.list_group_policies(group_name))
    }

    /// Performs the requested IAM operation for the provided scoped state.
    ///
    /// # Errors
    ///
    /// Returns IAM validation, lookup, conflict, or state-mutation errors when the request cannot be applied.
    pub fn put_role_policy(
        &self,
        scope: &IamScope,
        role_name: &str,
        input: InlinePolicyInput,
    ) -> Result<(), IamError> {
        self.with_scope_mut(scope, |state| {
            state.put_role_policy(role_name, input)
        })
    }

    /// Performs the requested IAM operation for the provided scoped state.
    ///
    /// # Errors
    ///
    /// Returns IAM validation, lookup, conflict, or state-mutation errors when the request cannot be applied.
    pub fn get_role_policy(
        &self,
        scope: &IamScope,
        role_name: &str,
        policy_name: &str,
    ) -> Result<InlinePolicy, IamError> {
        self.with_scope(scope, |state| {
            state.get_role_policy(role_name, policy_name)
        })
    }

    /// Performs the requested IAM operation for the provided scoped state.
    ///
    /// # Errors
    ///
    /// Returns IAM validation, lookup, conflict, or state-mutation errors when the request cannot be applied.
    pub fn delete_role_policy(
        &self,
        scope: &IamScope,
        role_name: &str,
        policy_name: &str,
    ) -> Result<(), IamError> {
        self.with_scope_mut(scope, |state| {
            state.delete_role_policy(role_name, policy_name)
        })
    }

    /// Performs the requested IAM operation for the provided scoped state.
    ///
    /// # Errors
    ///
    /// Returns IAM validation, lookup, conflict, or state-mutation errors when the request cannot be applied.
    pub fn list_role_policies(
        &self,
        scope: &IamScope,
        role_name: &str,
    ) -> Result<Vec<String>, IamError> {
        self.with_scope(scope, |state| state.list_role_policies(role_name))
    }

    fn with_scope<T>(
        &self,
        scope: &IamScope,
        action: impl FnOnce(&ScopedIamState) -> T,
    ) -> T {
        let guard =
            self.state.lock().unwrap_or_else(|poison| poison.into_inner());
        let key = scope_key(scope);
        action(guard.scopes.get(&key).unwrap_or(&EMPTY_SCOPE))
    }

    fn with_scope_mut<T>(
        &self,
        scope: &IamScope,
        action: impl FnOnce(&mut ScopedIamState) -> T,
    ) -> T {
        let mut guard =
            self.state.lock().unwrap_or_else(|poison| poison.into_inner());
        let state = guard.scopes.entry(scope_key(scope)).or_default();
        action(state)
    }
}

impl IamAccessKeyLookup for IamService {
    fn find_access_key(
        &self,
        access_key_id: &str,
    ) -> Option<IamAccessKeyRecord> {
        let guard =
            self.state.lock().unwrap_or_else(|poison| poison.into_inner());

        guard.scopes.iter().find_map(|(scope, state)| {
            let access_key = state.access_keys.get(access_key_id)?;
            let user = state.users.get(&access_key.user_name)?;
            Some(IamAccessKeyRecord {
                access_key_id: access_key_id.to_owned(),
                account_id: scope.account_id.clone(),
                create_date: access_key.create_date.clone(),
                region: scope.region.clone(),
                secret_access_key: access_key.secret_access_key.clone(),
                status: access_key.status,
                user_arn: user.entity.arn.clone(),
                user_id: user.entity.user_id.clone(),
                user_name: access_key.user_name.clone(),
            })
        })
    }
}

impl IamInstanceProfileLookup for IamService {
    fn find_instance_profile(
        &self,
        account_id: &AccountId,
        region: &RegionId,
        instance_profile_name: &str,
    ) -> Option<IamInstanceProfileRecord> {
        let guard =
            self.state.lock().unwrap_or_else(|poison| poison.into_inner());
        let scope = ScopeKey {
            account_id: account_id.clone(),
            region: region.clone(),
        };
        let state = guard.scopes.get(&scope)?;
        let profile = state.instance_profiles.get(instance_profile_name)?;
        Some(state.instance_profile_lookup_record(&scope, profile))
    }

    fn find_instance_profiles_for_role(
        &self,
        account_id: &AccountId,
        region: &RegionId,
        role_name: &str,
    ) -> Vec<IamInstanceProfileRecord> {
        let guard =
            self.state.lock().unwrap_or_else(|poison| poison.into_inner());
        let scope = ScopeKey {
            account_id: account_id.clone(),
            region: region.clone(),
        };
        let Some(state) = guard.scopes.get(&scope) else {
            return Vec::new();
        };

        state
            .instance_profiles
            .values()
            .filter(|profile| profile.role_name.as_deref() == Some(role_name))
            .map(|profile| {
                state.instance_profile_lookup_record(&scope, profile)
            })
            .collect()
    }
}

#[derive(Debug)]
struct IamWorld {
    next_access_key_sequence: u64,
    scopes: BTreeMap<ScopeKey, ScopedIamState>,
}

impl IamWorld {
    fn new() -> Self {
        Self { next_access_key_sequence: 1, scopes: BTreeMap::new() }
    }

    fn next_access_key_material(&mut self) -> (String, String) {
        let value = self.next_access_key_sequence;
        self.next_access_key_sequence += 1;
        (format!("AKIA{value:016X}"), format!("cloudishsecret{value:024X}"))
    }
}

impl Default for IamWorld {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct ScopeKey {
    account_id: AccountId,
    region: RegionId,
}

fn scope_key(scope: &IamScope) -> ScopeKey {
    ScopeKey {
        account_id: scope.account_id().clone(),
        region: scope.region().clone(),
    }
}

static EMPTY_SCOPE: ScopedIamState = ScopedIamState::new_const();

#[derive(Debug)]
struct ScopedIamState {
    access_keys: BTreeMap<String, StoredAccessKey>,
    event_counter: u64,
    next_identifier: u64,
    groups: BTreeMap<String, StoredGroup>,
    instance_profiles: BTreeMap<String, StoredInstanceProfile>,
    policies: BTreeMap<String, StoredPolicy>,
    roles: BTreeMap<String, StoredRole>,
    users: BTreeMap<String, StoredUser>,
}

impl Default for ScopedIamState {
    fn default() -> Self {
        Self {
            access_keys: BTreeMap::new(),
            event_counter: 0,
            next_identifier: 1,
            groups: BTreeMap::new(),
            instance_profiles: BTreeMap::new(),
            policies: BTreeMap::new(),
            roles: BTreeMap::new(),
            users: BTreeMap::new(),
        }
    }
}

impl ScopedIamState {
    const fn new_const() -> Self {
        Self {
            access_keys: BTreeMap::new(),
            event_counter: 0,
            next_identifier: 1,
            groups: BTreeMap::new(),
            instance_profiles: BTreeMap::new(),
            policies: BTreeMap::new(),
            roles: BTreeMap::new(),
            users: BTreeMap::new(),
        }
    }

    fn create_user(
        &mut self,
        account_id: &AccountId,
        input: CreateUserInput,
    ) -> Result<IamUser, IamError> {
        validate_entity_name(&input.user_name, "user", USER_NAME_MAX)?;
        let path = normalize_path(&input.path)?;
        validate_tags(&input.tags)?;
        ensure_name_is_unique(self.users.keys(), &input.user_name, "User")?;
        let user = IamUser {
            arn: iam_arn(
                account_id,
                &format!("user{}", path_resource(&path, &input.user_name)),
            ),
            create_date: self.next_timestamp(),
            path,
            user_id: self.next_identifier("AIDA"),
            user_name: input.user_name.clone(),
        };
        self.users.insert(
            input.user_name,
            StoredUser {
                attached_policies: BTreeSet::new(),
                entity: user.clone(),
                group_names: BTreeSet::new(),
                inline_policies: BTreeMap::new(),
                tags: tags_map(&input.tags),
            },
        );
        Ok(user)
    }

    fn get_user(&self, user_name: &str) -> Result<IamUser, IamError> {
        Ok(self.user(user_name)?.entity.clone())
    }

    fn list_users(&self, path_prefix: Option<&str>) -> Vec<IamUser> {
        self.users
            .values()
            .filter(|user| path_matches(&user.entity.path, path_prefix))
            .map(|user| user.entity.clone())
            .collect()
    }

    fn delete_user(&mut self, user_name: &str) -> Result<(), IamError> {
        let user = self.user(user_name)?;
        if !user.attached_policies.is_empty()
            || !user.inline_policies.is_empty()
            || !user.group_names.is_empty()
            || self
                .access_keys
                .values()
                .any(|access_key| access_key.user_name == user_name)
        {
            return Err(delete_conflict());
        }

        self.users.remove(user_name);
        Ok(())
    }

    fn tag_user(
        &mut self,
        user_name: &str,
        tags: Vec<IamTag>,
    ) -> Result<(), IamError> {
        validate_tags(&tags)?;
        let user = self.user_mut(user_name)?;
        merge_tags(&mut user.tags, tags)
    }

    fn untag_user(
        &mut self,
        user_name: &str,
        tag_keys: Vec<String>,
    ) -> Result<(), IamError> {
        let user = self.user_mut(user_name)?;
        remove_tags(&mut user.tags, &tag_keys);
        Ok(())
    }

    fn list_user_tags(
        &self,
        user_name: &str,
    ) -> Result<Vec<IamTag>, IamError> {
        Ok(tags_vec(&self.user(user_name)?.tags))
    }

    fn create_access_key(
        &mut self,
        access_key_id: String,
        secret_access_key: String,
        user_name: &str,
    ) -> Result<IamAccessKey, IamError> {
        self.user(user_name)?;
        if self
            .access_keys
            .values()
            .filter(|access_key| access_key.user_name == user_name)
            .count()
            >= MAX_ACCESS_KEYS_PER_USER
        {
            return Err(IamError::LimitExceeded {
                message: format!(
                    "Cannot exceed quota for AccessKeysPerUser: {MAX_ACCESS_KEYS_PER_USER}."
                ),
            });
        }

        let access_key = IamAccessKey {
            access_key_id: access_key_id.clone(),
            create_date: self.next_timestamp(),
            secret_access_key: secret_access_key.clone(),
            status: IamAccessKeyStatus::Active,
            user_name: user_name.to_owned(),
        };
        self.access_keys.insert(
            access_key_id,
            StoredAccessKey {
                create_date: access_key.create_date.clone(),
                secret_access_key,
                status: access_key.status,
                user_name: user_name.to_owned(),
            },
        );
        Ok(access_key)
    }

    fn delete_access_key(
        &mut self,
        user_name: &str,
        access_key_id: &str,
    ) -> Result<(), IamError> {
        let access_key = self.access_key(access_key_id)?;
        if access_key.user_name != user_name {
            return Err(IamError::NoSuchEntity {
                message: format!(
                    "The access key with id {access_key_id} cannot be found."
                ),
            });
        }

        self.access_keys.remove(access_key_id);
        Ok(())
    }

    fn list_access_keys(
        &self,
        user_name: &str,
    ) -> Result<Vec<IamAccessKeyMetadata>, IamError> {
        self.user(user_name)?;
        Ok(self
            .access_keys
            .iter()
            .filter(|(_, access_key)| access_key.user_name == user_name)
            .map(|(access_key_id, access_key)| IamAccessKeyMetadata {
                access_key_id: access_key_id.clone(),
                create_date: access_key.create_date.clone(),
                status: access_key.status,
                user_name: access_key.user_name.clone(),
            })
            .collect())
    }

    fn update_access_key(
        &mut self,
        user_name: &str,
        access_key_id: &str,
        status: IamAccessKeyStatus,
    ) -> Result<(), IamError> {
        let access_key = self.access_key_mut(access_key_id)?;
        if access_key.user_name != user_name {
            return Err(IamError::NoSuchEntity {
                message: format!(
                    "The access key with id {access_key_id} cannot be found."
                ),
            });
        }

        access_key.status = status;
        Ok(())
    }

    fn create_group(
        &mut self,
        account_id: &AccountId,
        input: CreateGroupInput,
    ) -> Result<IamGroup, IamError> {
        validate_entity_name(&input.group_name, "group", ENTITY_NAME_MAX)?;
        let path = normalize_path(&input.path)?;
        ensure_name_is_unique(self.groups.keys(), &input.group_name, "Group")?;
        let group = IamGroup {
            arn: iam_arn(
                account_id,
                &format!("group{}", path_resource(&path, &input.group_name)),
            ),
            create_date: self.next_timestamp(),
            group_id: self.next_identifier("AGPA"),
            group_name: input.group_name.clone(),
            path,
        };
        self.groups.insert(
            input.group_name,
            StoredGroup {
                attached_policies: BTreeSet::new(),
                entity: group.clone(),
                inline_policies: BTreeMap::new(),
                user_names: BTreeSet::new(),
            },
        );
        Ok(group)
    }

    fn get_group(&self, group_name: &str) -> Result<GroupDetails, IamError> {
        let group = self.group(group_name)?;
        let users = group
            .user_names
            .iter()
            .filter_map(|user_name| self.users.get(user_name))
            .map(|user| user.entity.clone())
            .collect();
        Ok(GroupDetails { group: group.entity.clone(), users })
    }

    fn list_groups(&self, path_prefix: Option<&str>) -> Vec<IamGroup> {
        self.groups
            .values()
            .filter(|group| path_matches(&group.entity.path, path_prefix))
            .map(|group| group.entity.clone())
            .collect()
    }

    fn delete_group(&mut self, group_name: &str) -> Result<(), IamError> {
        let group = self.group(group_name)?;
        if !group.user_names.is_empty()
            || !group.attached_policies.is_empty()
            || !group.inline_policies.is_empty()
        {
            return Err(delete_conflict());
        }

        self.groups.remove(group_name);
        Ok(())
    }

    fn add_user_to_group(
        &mut self,
        group_name: &str,
        user_name: &str,
    ) -> Result<(), IamError> {
        self.user(user_name)?;
        self.group(group_name)?;
        self.user_mut(user_name)?.group_names.insert(group_name.to_owned());
        self.group_mut(group_name)?.user_names.insert(user_name.to_owned());
        Ok(())
    }

    fn remove_user_from_group(
        &mut self,
        group_name: &str,
        user_name: &str,
    ) -> Result<(), IamError> {
        self.user(user_name)?;
        self.group(group_name)?;
        self.user_mut(user_name)?.group_names.remove(group_name);
        self.group_mut(group_name)?.user_names.remove(user_name);
        Ok(())
    }

    fn list_groups_for_user(
        &self,
        user_name: &str,
    ) -> Result<Vec<IamGroup>, IamError> {
        let user = self.user(user_name)?;
        Ok(user
            .group_names
            .iter()
            .filter_map(|group_name| self.groups.get(group_name))
            .map(|group| group.entity.clone())
            .collect())
    }

    fn create_role(
        &mut self,
        account_id: &AccountId,
        input: CreateRoleInput,
    ) -> Result<IamRole, IamError> {
        validate_entity_name(&input.role_name, "role", ENTITY_NAME_MAX)?;
        let path = normalize_path(&input.path)?;
        validate_policy_document(&input.assume_role_policy_document)?;
        validate_tags(&input.tags)?;
        if !(MIN_SESSION_DURATION..=MAX_SESSION_DURATION)
            .contains(&input.max_session_duration)
        {
            return Err(IamError::InvalidInput {
                message: format!(
                    "MaxSessionDuration must be between {MIN_SESSION_DURATION} and {MAX_SESSION_DURATION} seconds."
                ),
            });
        }
        ensure_name_is_unique(self.roles.keys(), &input.role_name, "Role")?;
        let role = IamRole {
            arn: iam_arn(
                account_id,
                &format!("role{}", path_resource(&path, &input.role_name)),
            ),
            assume_role_policy_document: input.assume_role_policy_document,
            create_date: self.next_timestamp(),
            description: input.description,
            max_session_duration: input.max_session_duration,
            path,
            role_id: self.next_identifier("AROA"),
            role_name: input.role_name.clone(),
        };
        self.roles.insert(
            input.role_name,
            StoredRole {
                attached_policies: BTreeSet::new(),
                entity: role.clone(),
                inline_policies: BTreeMap::new(),
                tags: tags_map(&input.tags),
            },
        );
        Ok(role)
    }

    fn get_role(&self, role_name: &str) -> Result<IamRole, IamError> {
        Ok(self.role(role_name)?.entity.clone())
    }

    fn list_roles(&self, path_prefix: Option<&str>) -> Vec<IamRole> {
        self.roles
            .values()
            .filter(|role| path_matches(&role.entity.path, path_prefix))
            .map(|role| role.entity.clone())
            .collect()
    }

    fn delete_role(&mut self, role_name: &str) -> Result<(), IamError> {
        let role = self.role(role_name)?;
        if !role.attached_policies.is_empty()
            || !role.inline_policies.is_empty()
            || self
                .instance_profiles
                .values()
                .any(|profile| profile.role_name.as_deref() == Some(role_name))
        {
            return Err(delete_conflict());
        }

        self.roles.remove(role_name);
        Ok(())
    }

    fn tag_role(
        &mut self,
        role_name: &str,
        tags: Vec<IamTag>,
    ) -> Result<(), IamError> {
        validate_tags(&tags)?;
        let role = self.role_mut(role_name)?;
        merge_tags(&mut role.tags, tags)
    }

    fn untag_role(
        &mut self,
        role_name: &str,
        tag_keys: Vec<String>,
    ) -> Result<(), IamError> {
        let role = self.role_mut(role_name)?;
        remove_tags(&mut role.tags, &tag_keys);
        Ok(())
    }

    fn list_role_tags(
        &self,
        role_name: &str,
    ) -> Result<Vec<IamTag>, IamError> {
        Ok(tags_vec(&self.role(role_name)?.tags))
    }

    fn create_instance_profile(
        &mut self,
        account_id: &AccountId,
        input: CreateInstanceProfileInput,
    ) -> Result<IamInstanceProfile, IamError> {
        validate_entity_name(
            &input.instance_profile_name,
            "instance profile",
            ENTITY_NAME_MAX,
        )?;
        let path = normalize_path(&input.path)?;
        validate_tags(&input.tags)?;
        ensure_name_is_unique(
            self.instance_profiles.keys(),
            &input.instance_profile_name,
            "InstanceProfile",
        )?;
        let profile = IamInstanceProfile {
            arn: iam_arn(
                account_id,
                &format!(
                    "instance-profile{}",
                    path_resource(&path, &input.instance_profile_name)
                ),
            ),
            create_date: self.next_timestamp(),
            instance_profile_id: self.next_identifier("AIPA"),
            instance_profile_name: input.instance_profile_name.clone(),
            path,
            roles: Vec::new(),
            tags: input.tags.clone(),
        };
        self.instance_profiles.insert(
            input.instance_profile_name,
            StoredInstanceProfile {
                entity: InstanceProfileEntity {
                    arn: profile.arn.clone(),
                    create_date: profile.create_date.clone(),
                    instance_profile_id: profile.instance_profile_id.clone(),
                    instance_profile_name: profile
                        .instance_profile_name
                        .clone(),
                    path: profile.path.clone(),
                },
                role_name: None,
                tags: tags_map(&input.tags),
            },
        );
        Ok(profile)
    }

    fn get_instance_profile(
        &self,
        instance_profile_name: &str,
    ) -> Result<IamInstanceProfile, IamError> {
        let profile = self.instance_profile(instance_profile_name)?;
        Ok(self.instance_profile_view(profile))
    }

    fn list_instance_profiles(
        &self,
        path_prefix: Option<&str>,
    ) -> Vec<IamInstanceProfile> {
        self.instance_profiles
            .values()
            .filter(|profile| path_matches(&profile.entity.path, path_prefix))
            .map(|profile| self.instance_profile_view(profile))
            .collect()
    }

    fn delete_instance_profile(
        &mut self,
        instance_profile_name: &str,
    ) -> Result<(), IamError> {
        let profile = self.instance_profile(instance_profile_name)?;
        if profile.role_name.is_some() {
            return Err(delete_conflict());
        }

        self.instance_profiles.remove(instance_profile_name);
        Ok(())
    }

    fn add_role_to_instance_profile(
        &mut self,
        instance_profile_name: &str,
        role_name: &str,
    ) -> Result<(), IamError> {
        self.role(role_name)?;
        let profile = self.instance_profile_mut(instance_profile_name)?;
        if profile.role_name.as_deref() == Some(role_name) {
            return Err(IamError::EntityAlreadyExists {
                message: format!(
                    "Role {role_name} already exists in instance profile {instance_profile_name}."
                ),
            });
        }
        if profile.role_name.is_some() {
            return Err(IamError::LimitExceeded {
                message: "Cannot exceed quota for RolesPerInstanceProfile: 1."
                    .to_owned(),
            });
        }

        profile.role_name = Some(role_name.to_owned());
        Ok(())
    }

    fn remove_role_from_instance_profile(
        &mut self,
        instance_profile_name: &str,
        role_name: &str,
    ) -> Result<(), IamError> {
        self.role(role_name)?;
        let profile = self.instance_profile_mut(instance_profile_name)?;
        if profile.role_name.as_deref() == Some(role_name) {
            profile.role_name = None;
        }
        Ok(())
    }

    fn list_instance_profiles_for_role(
        &self,
        role_name: &str,
    ) -> Result<Vec<IamInstanceProfile>, IamError> {
        self.role(role_name)?;
        Ok(self
            .instance_profiles
            .values()
            .filter(|profile| profile.role_name.as_deref() == Some(role_name))
            .map(|profile| self.instance_profile_view(profile))
            .collect())
    }

    fn tag_instance_profile(
        &mut self,
        instance_profile_name: &str,
        tags: Vec<IamTag>,
    ) -> Result<(), IamError> {
        validate_tags(&tags)?;
        let profile = self.instance_profile_mut(instance_profile_name)?;
        merge_tags(&mut profile.tags, tags)
    }

    fn untag_instance_profile(
        &mut self,
        instance_profile_name: &str,
        tag_keys: Vec<String>,
    ) -> Result<(), IamError> {
        let profile = self.instance_profile_mut(instance_profile_name)?;
        remove_tags(&mut profile.tags, &tag_keys);
        Ok(())
    }

    fn list_instance_profile_tags(
        &self,
        instance_profile_name: &str,
    ) -> Result<Vec<IamTag>, IamError> {
        Ok(tags_vec(&self.instance_profile(instance_profile_name)?.tags))
    }

    fn create_policy(
        &mut self,
        account_id: &AccountId,
        input: CreatePolicyInput,
    ) -> Result<IamPolicy, IamError> {
        validate_entity_name(&input.policy_name, "policy", ENTITY_NAME_MAX)?;
        let path = normalize_path(&input.path)?;
        validate_policy_document(&input.policy_document)?;
        validate_tags(&input.tags)?;
        ensure_name_is_unique(
            self.policies
                .values()
                .map(|policy| policy.entity.policy_name.as_str()),
            &input.policy_name,
            "Policy",
        )?;
        let created_at = self.next_timestamp();
        let version_id = "v1".to_owned();
        let policy_arn = iam_arn(
            account_id,
            &format!("policy{}", path_resource(&path, &input.policy_name)),
        );
        let stored = StoredPolicy {
            default_version_id: version_id.clone(),
            entity: PolicyEntity {
                arn: policy_arn.clone(),
                create_date: created_at.clone(),
                path,
                policy_id: self.next_identifier("ANPA"),
                policy_name: input.policy_name,
                update_date: created_at.clone(),
            },
            next_version_number: 2,
            tags: tags_map(&input.tags),
            versions: BTreeMap::from([(
                version_id.clone(),
                StoredPolicyVersion {
                    create_date: created_at,
                    document: PolicyDocument::new(input.policy_document)?,
                    version_id: version_id.clone(),
                },
            )]),
        };
        let view = self.policy_view(&stored);
        self.policies.insert(policy_arn.to_string(), stored);
        Ok(view)
    }

    fn get_policy(&self, policy_arn: &str) -> Result<IamPolicy, IamError> {
        let policy = self.policy(policy_arn)?;
        Ok(self.policy_view(policy))
    }

    fn list_policies(
        &self,
        scope_filter: Option<&str>,
        path_prefix: Option<&str>,
    ) -> Result<Vec<IamPolicy>, IamError> {
        match scope_filter {
            Some("AWS") => Ok(Vec::new()),
            Some("All") | Some("Local") | None => Ok(self
                .policies
                .values()
                .filter(|policy| {
                    path_matches(&policy.entity.path, path_prefix)
                })
                .map(|policy| self.policy_view(policy))
                .collect()),
            Some(other) => Err(IamError::InvalidInput {
                message: format!(
                    "Scope must be one of All, AWS, or Local; received {other}."
                ),
            }),
        }
    }

    fn delete_policy(&mut self, policy_arn: &str) -> Result<(), IamError> {
        let policy = self.policy(policy_arn)?;
        if self.attachment_count(policy_arn) > 0 || policy.versions.len() > 1 {
            return Err(delete_conflict());
        }

        self.policies.remove(policy_arn);
        Ok(())
    }

    fn create_policy_version(
        &mut self,
        input: CreatePolicyVersionInput,
    ) -> Result<IamPolicyVersion, IamError> {
        validate_policy_document(&input.policy_document)?;
        let policy_arn = input.policy_arn.to_string();
        let created_at = self.next_timestamp();
        let policy = self.policy_mut(&policy_arn)?;
        if policy.versions.len() >= MAX_POLICY_VERSIONS {
            return Err(IamError::LimitExceeded {
                message: format!(
                    "Cannot exceed quota for PolicyVersionsPerPolicy: {MAX_POLICY_VERSIONS}."
                ),
            });
        }
        let version_id = format!("v{}", policy.next_version_number);
        policy.next_version_number += 1;
        if input.set_as_default {
            policy.default_version_id = version_id.clone();
        }
        policy.entity.update_date = created_at.clone();
        let version = StoredPolicyVersion {
            create_date: created_at,
            document: PolicyDocument::new(input.policy_document)?,
            version_id: version_id.clone(),
        };
        let view = policy.policy_version(&version);
        policy.versions.insert(version_id, version);
        Ok(view)
    }

    fn get_policy_version(
        &self,
        policy_arn: &str,
        version_id: &str,
    ) -> Result<IamPolicyVersion, IamError> {
        let policy = self.policy(policy_arn)?;
        let version = policy.version(version_id)?;
        Ok(policy.policy_version(version))
    }

    fn delete_policy_version(
        &mut self,
        policy_arn: &str,
        version_id: &str,
    ) -> Result<(), IamError> {
        let updated_at = self.next_timestamp();
        let policy = self.policy_mut(policy_arn)?;
        policy.version(version_id)?;
        if policy.default_version_id == version_id {
            return Err(delete_conflict());
        }
        policy.versions.remove(version_id);
        policy.entity.update_date = updated_at;
        Ok(())
    }

    fn list_policy_versions(
        &self,
        policy_arn: &str,
    ) -> Result<Vec<IamPolicyVersion>, IamError> {
        let policy = self.policy(policy_arn)?;
        Ok(policy
            .versions
            .values()
            .map(|version| policy.policy_version(version))
            .collect())
    }

    fn set_default_policy_version(
        &mut self,
        policy_arn: &str,
        version_id: &str,
    ) -> Result<(), IamError> {
        let updated_at = self.next_timestamp();
        let policy = self.policy_mut(policy_arn)?;
        policy.version(version_id)?;
        policy.default_version_id = version_id.to_owned();
        policy.entity.update_date = updated_at;
        Ok(())
    }

    fn tag_policy(
        &mut self,
        policy_arn: &str,
        tags: Vec<IamTag>,
    ) -> Result<(), IamError> {
        validate_tags(&tags)?;
        let policy = self.policy_mut(policy_arn)?;
        merge_tags(&mut policy.tags, tags)
    }

    fn untag_policy(
        &mut self,
        policy_arn: &str,
        tag_keys: Vec<String>,
    ) -> Result<(), IamError> {
        let policy = self.policy_mut(policy_arn)?;
        remove_tags(&mut policy.tags, &tag_keys);
        Ok(())
    }

    fn list_policy_tags(
        &self,
        policy_arn: &str,
    ) -> Result<Vec<IamTag>, IamError> {
        Ok(tags_vec(&self.policy(policy_arn)?.tags))
    }

    fn attach_user_policy(
        &mut self,
        user_name: &str,
        policy_arn: &str,
    ) -> Result<(), IamError> {
        self.policy(policy_arn)?;
        self.user_mut(user_name)?
            .attached_policies
            .insert(policy_arn.to_owned());
        Ok(())
    }

    fn detach_user_policy(
        &mut self,
        user_name: &str,
        policy_arn: &str,
    ) -> Result<(), IamError> {
        self.user_mut(user_name)?.attached_policies.remove(policy_arn);
        Ok(())
    }

    fn list_attached_user_policies(
        &self,
        user_name: &str,
        path_prefix: Option<&str>,
    ) -> Result<Vec<AttachedPolicy>, IamError> {
        Ok(self.attached_policies(
            &self.user(user_name)?.attached_policies,
            path_prefix,
        ))
    }

    fn attach_group_policy(
        &mut self,
        group_name: &str,
        policy_arn: &str,
    ) -> Result<(), IamError> {
        self.policy(policy_arn)?;
        self.group_mut(group_name)?
            .attached_policies
            .insert(policy_arn.to_owned());
        Ok(())
    }

    fn detach_group_policy(
        &mut self,
        group_name: &str,
        policy_arn: &str,
    ) -> Result<(), IamError> {
        self.group_mut(group_name)?.attached_policies.remove(policy_arn);
        Ok(())
    }

    fn list_attached_group_policies(
        &self,
        group_name: &str,
        path_prefix: Option<&str>,
    ) -> Result<Vec<AttachedPolicy>, IamError> {
        Ok(self.attached_policies(
            &self.group(group_name)?.attached_policies,
            path_prefix,
        ))
    }

    fn attach_role_policy(
        &mut self,
        role_name: &str,
        policy_arn: &str,
    ) -> Result<(), IamError> {
        self.policy(policy_arn)?;
        self.role_mut(role_name)?
            .attached_policies
            .insert(policy_arn.to_owned());
        Ok(())
    }

    fn detach_role_policy(
        &mut self,
        role_name: &str,
        policy_arn: &str,
    ) -> Result<(), IamError> {
        self.role_mut(role_name)?.attached_policies.remove(policy_arn);
        Ok(())
    }

    fn list_attached_role_policies(
        &self,
        role_name: &str,
        path_prefix: Option<&str>,
    ) -> Result<Vec<AttachedPolicy>, IamError> {
        Ok(self.attached_policies(
            &self.role(role_name)?.attached_policies,
            path_prefix,
        ))
    }

    fn put_user_policy(
        &mut self,
        user_name: &str,
        input: InlinePolicyInput,
    ) -> Result<(), IamError> {
        let user = self.user_mut(user_name)?;
        put_inline_policy(&mut user.inline_policies, input)
    }

    fn get_user_policy(
        &self,
        user_name: &str,
        policy_name: &str,
    ) -> Result<InlinePolicy, IamError> {
        let user = self.user(user_name)?;
        get_inline_policy(&user.inline_policies, user_name, policy_name)
    }

    fn delete_user_policy(
        &mut self,
        user_name: &str,
        policy_name: &str,
    ) -> Result<(), IamError> {
        let user = self.user_mut(user_name)?;
        delete_inline_policy(&mut user.inline_policies, policy_name)
    }

    fn list_user_policies(
        &self,
        user_name: &str,
    ) -> Result<Vec<String>, IamError> {
        Ok(list_inline_policies(&self.user(user_name)?.inline_policies))
    }

    fn put_group_policy(
        &mut self,
        group_name: &str,
        input: InlinePolicyInput,
    ) -> Result<(), IamError> {
        let group = self.group_mut(group_name)?;
        put_inline_policy(&mut group.inline_policies, input)
    }

    fn get_group_policy(
        &self,
        group_name: &str,
        policy_name: &str,
    ) -> Result<InlinePolicy, IamError> {
        let group = self.group(group_name)?;
        get_inline_policy(&group.inline_policies, group_name, policy_name)
    }

    fn delete_group_policy(
        &mut self,
        group_name: &str,
        policy_name: &str,
    ) -> Result<(), IamError> {
        let group = self.group_mut(group_name)?;
        delete_inline_policy(&mut group.inline_policies, policy_name)
    }

    fn list_group_policies(
        &self,
        group_name: &str,
    ) -> Result<Vec<String>, IamError> {
        Ok(list_inline_policies(&self.group(group_name)?.inline_policies))
    }

    fn put_role_policy(
        &mut self,
        role_name: &str,
        input: InlinePolicyInput,
    ) -> Result<(), IamError> {
        let role = self.role_mut(role_name)?;
        put_inline_policy(&mut role.inline_policies, input)
    }

    fn get_role_policy(
        &self,
        role_name: &str,
        policy_name: &str,
    ) -> Result<InlinePolicy, IamError> {
        let role = self.role(role_name)?;
        get_inline_policy(&role.inline_policies, role_name, policy_name)
    }

    fn delete_role_policy(
        &mut self,
        role_name: &str,
        policy_name: &str,
    ) -> Result<(), IamError> {
        let role = self.role_mut(role_name)?;
        delete_inline_policy(&mut role.inline_policies, policy_name)
    }

    fn list_role_policies(
        &self,
        role_name: &str,
    ) -> Result<Vec<String>, IamError> {
        Ok(list_inline_policies(&self.role(role_name)?.inline_policies))
    }

    fn user(&self, user_name: &str) -> Result<&StoredUser, IamError> {
        self.users.get(user_name).ok_or_else(|| IamError::NoSuchEntity {
            message: format!(
                "The user with name {user_name} cannot be found."
            ),
        })
    }

    fn user_mut(
        &mut self,
        user_name: &str,
    ) -> Result<&mut StoredUser, IamError> {
        self.users.get_mut(user_name).ok_or_else(|| IamError::NoSuchEntity {
            message: format!(
                "The user with name {user_name} cannot be found."
            ),
        })
    }

    fn access_key(
        &self,
        access_key_id: &str,
    ) -> Result<&StoredAccessKey, IamError> {
        self.access_keys.get(access_key_id).ok_or_else(|| {
            IamError::NoSuchEntity {
                message: format!(
                    "The access key with id {access_key_id} cannot be found."
                ),
            }
        })
    }

    fn access_key_mut(
        &mut self,
        access_key_id: &str,
    ) -> Result<&mut StoredAccessKey, IamError> {
        self.access_keys.get_mut(access_key_id).ok_or_else(|| {
            IamError::NoSuchEntity {
                message: format!(
                    "The access key with id {access_key_id} cannot be found."
                ),
            }
        })
    }

    fn group(&self, group_name: &str) -> Result<&StoredGroup, IamError> {
        self.groups.get(group_name).ok_or_else(|| IamError::NoSuchEntity {
            message: format!(
                "The group with name {group_name} cannot be found."
            ),
        })
    }

    fn group_mut(
        &mut self,
        group_name: &str,
    ) -> Result<&mut StoredGroup, IamError> {
        self.groups.get_mut(group_name).ok_or_else(|| IamError::NoSuchEntity {
            message: format!(
                "The group with name {group_name} cannot be found."
            ),
        })
    }

    fn role(&self, role_name: &str) -> Result<&StoredRole, IamError> {
        self.roles.get(role_name).ok_or_else(|| IamError::NoSuchEntity {
            message: format!(
                "The role with name {role_name} cannot be found."
            ),
        })
    }

    fn role_mut(
        &mut self,
        role_name: &str,
    ) -> Result<&mut StoredRole, IamError> {
        self.roles.get_mut(role_name).ok_or_else(|| IamError::NoSuchEntity {
            message: format!(
                "The role with name {role_name} cannot be found."
            ),
        })
    }

    fn instance_profile(
        &self,
        instance_profile_name: &str,
    ) -> Result<&StoredInstanceProfile, IamError> {
        self.instance_profiles.get(instance_profile_name).ok_or_else(|| {
            IamError::NoSuchEntity {
                message: format!(
                    "The instance profile with name {instance_profile_name} cannot be found."
                ),
            }
        })
    }

    fn instance_profile_mut(
        &mut self,
        instance_profile_name: &str,
    ) -> Result<&mut StoredInstanceProfile, IamError> {
        self.instance_profiles
            .get_mut(instance_profile_name)
            .ok_or_else(|| IamError::NoSuchEntity {
                message: format!(
                    "The instance profile with name {instance_profile_name} cannot be found."
                ),
            })
    }

    fn policy(&self, policy_arn: &str) -> Result<&StoredPolicy, IamError> {
        self.policies.get(policy_arn).ok_or_else(|| IamError::NoSuchEntity {
            message: format!("Policy {policy_arn} was not found."),
        })
    }

    fn policy_mut(
        &mut self,
        policy_arn: &str,
    ) -> Result<&mut StoredPolicy, IamError> {
        self.policies.get_mut(policy_arn).ok_or_else(|| {
            IamError::NoSuchEntity {
                message: format!("Policy {policy_arn} was not found."),
            }
        })
    }

    fn attachment_count(&self, policy_arn: &str) -> usize {
        self.users
            .values()
            .filter(|user| user.attached_policies.contains(policy_arn))
            .count()
            + self
                .groups
                .values()
                .filter(|group| group.attached_policies.contains(policy_arn))
                .count()
            + self
                .roles
                .values()
                .filter(|role| role.attached_policies.contains(policy_arn))
                .count()
    }

    fn policy_view(&self, policy: &StoredPolicy) -> IamPolicy {
        let policy_key = policy.entity.arn.to_string();
        IamPolicy {
            arn: policy.entity.arn.clone(),
            attachment_count: self.attachment_count(&policy_key),
            create_date: policy.entity.create_date.clone(),
            default_version_id: policy.default_version_id.clone(),
            is_attachable: true,
            path: policy.entity.path.clone(),
            policy_id: policy.entity.policy_id.clone(),
            policy_name: policy.entity.policy_name.clone(),
            update_date: policy.entity.update_date.clone(),
        }
    }

    fn instance_profile_view(
        &self,
        profile: &StoredInstanceProfile,
    ) -> IamInstanceProfile {
        IamInstanceProfile {
            arn: profile.entity.arn.clone(),
            create_date: profile.entity.create_date.clone(),
            instance_profile_id: profile.entity.instance_profile_id.clone(),
            instance_profile_name: profile
                .entity
                .instance_profile_name
                .clone(),
            path: profile.entity.path.clone(),
            roles: profile
                .role_name
                .as_deref()
                .and_then(|role_name| self.roles.get(role_name))
                .map(|role| vec![role.entity.clone()])
                .unwrap_or_default(),
            tags: tags_vec(&profile.tags),
        }
    }

    fn instance_profile_lookup_record(
        &self,
        scope: &ScopeKey,
        profile: &StoredInstanceProfile,
    ) -> IamInstanceProfileRecord {
        IamInstanceProfileRecord {
            account_id: scope.account_id.clone(),
            arn: profile.entity.arn.clone(),
            create_date: profile.entity.create_date.clone(),
            instance_profile_id: profile.entity.instance_profile_id.clone(),
            instance_profile_name: profile
                .entity
                .instance_profile_name
                .clone(),
            path: profile.entity.path.clone(),
            region: scope.region.clone(),
            role: profile
                .role_name
                .as_deref()
                .and_then(|role_name| self.roles.get(role_name))
                .map(|role| IamRoleRecord {
                    arn: role.entity.arn.clone(),
                    create_date: role.entity.create_date.clone(),
                    path: role.entity.path.clone(),
                    role_id: role.entity.role_id.clone(),
                    role_name: role.entity.role_name.clone(),
                }),
            tags: profile
                .tags
                .iter()
                .map(|(key, value)| IamResourceTag {
                    key: key.clone(),
                    value: value.clone(),
                })
                .collect(),
        }
    }

    fn attached_policies(
        &self,
        attachments: &BTreeSet<String>,
        path_prefix: Option<&str>,
    ) -> Vec<AttachedPolicy> {
        attachments
            .iter()
            .filter_map(|policy_arn| self.policies.get(policy_arn))
            .filter(|policy| path_matches(&policy.entity.path, path_prefix))
            .map(|policy| AttachedPolicy {
                policy_arn: policy.entity.arn.clone(),
                policy_name: policy.entity.policy_name.clone(),
            })
            .collect()
    }

    fn next_timestamp(&mut self) -> String {
        let value = self.event_counter;
        self.event_counter += 1;
        let hours = value / 3_600;
        let minutes = (value / 60) % 60;
        let seconds = value % 60;
        format!(
            "{REQUEST_DATE_PREFIX}T{:02}:{minutes:02}:{seconds:02}Z",
            hours % 24
        )
    }

    fn next_identifier(&mut self, prefix: &str) -> String {
        let value = self.next_identifier;
        self.next_identifier += 1;
        format!("{prefix}{value:016X}")
    }
}

#[derive(Debug, Clone)]
struct StoredUser {
    attached_policies: BTreeSet<String>,
    entity: IamUser,
    group_names: BTreeSet<String>,
    inline_policies: BTreeMap<String, PolicyDocument>,
    tags: BTreeMap<String, String>,
}

#[derive(Debug, Clone)]
struct StoredAccessKey {
    create_date: String,
    secret_access_key: String,
    status: IamAccessKeyStatus,
    user_name: String,
}

#[derive(Debug, Clone)]
struct StoredGroup {
    attached_policies: BTreeSet<String>,
    entity: IamGroup,
    inline_policies: BTreeMap<String, PolicyDocument>,
    user_names: BTreeSet<String>,
}

#[derive(Debug, Clone)]
struct StoredRole {
    attached_policies: BTreeSet<String>,
    entity: IamRole,
    inline_policies: BTreeMap<String, PolicyDocument>,
    tags: BTreeMap<String, String>,
}

#[derive(Debug, Clone)]
struct InstanceProfileEntity {
    arn: Arn,
    create_date: String,
    instance_profile_id: String,
    instance_profile_name: String,
    path: String,
}

#[derive(Debug, Clone)]
struct StoredInstanceProfile {
    entity: InstanceProfileEntity,
    role_name: Option<String>,
    tags: BTreeMap<String, String>,
}

#[derive(Debug, Clone)]
struct PolicyEntity {
    arn: Arn,
    create_date: String,
    path: String,
    policy_id: String,
    policy_name: String,
    update_date: String,
}

#[derive(Debug, Clone)]
struct StoredPolicy {
    default_version_id: String,
    entity: PolicyEntity,
    next_version_number: u32,
    tags: BTreeMap<String, String>,
    versions: BTreeMap<String, StoredPolicyVersion>,
}

impl StoredPolicy {
    fn version(
        &self,
        version_id: &str,
    ) -> Result<&StoredPolicyVersion, IamError> {
        self.versions.get(version_id).ok_or_else(|| IamError::NoSuchEntity {
            message: format!(
                "Policy version {version_id} for {} was not found.",
                self.entity.arn
            ),
        })
    }

    fn policy_version(
        &self,
        version: &StoredPolicyVersion,
    ) -> IamPolicyVersion {
        IamPolicyVersion {
            create_date: version.create_date.clone(),
            document: version.document.as_str().to_owned(),
            is_default_version: self.default_version_id == version.version_id,
            version_id: version.version_id.clone(),
        }
    }
}

#[derive(Debug, Clone)]
struct StoredPolicyVersion {
    create_date: String,
    document: PolicyDocument,
    version_id: String,
}

#[derive(Debug, Clone)]
struct PolicyDocument(String);

impl PolicyDocument {
    fn new(document: String) -> Result<Self, IamError> {
        validate_policy_document(&document)?;
        Ok(Self(document))
    }

    fn as_str(&self) -> &str {
        &self.0
    }
}

fn validate_policy_document(document: &str) -> Result<(), IamError> {
    let parsed = serde_json::from_str::<Value>(document).map_err(|_| {
        IamError::MalformedPolicyDocument {
            message: "This policy contains invalid Json".to_owned(),
        }
    })?;
    if !parsed.is_object() {
        return Err(IamError::MalformedPolicyDocument {
            message: "This policy contains invalid Json".to_owned(),
        });
    }
    Ok(())
}

fn validate_tags(tags: &[IamTag]) -> Result<(), IamError> {
    if tags.len() > MAX_TAGS {
        return Err(IamError::LimitExceeded {
            message: format!(
                "Cannot exceed quota for TagsPerResource: {MAX_TAGS}."
            ),
        });
    }

    for tag in tags {
        if tag.key.trim().is_empty() {
            return Err(IamError::InvalidInput {
                message: "Tag keys must not be blank.".to_owned(),
            });
        }
    }

    Ok(())
}

fn validate_entity_name(
    name: &str,
    entity_kind: &str,
    max_len: usize,
) -> Result<(), IamError> {
    if name.is_empty() || name.len() > max_len {
        return Err(IamError::InvalidInput {
            message: format!(
                "{entity_kind} name must be between 1 and {max_len} characters."
            ),
        });
    }
    if name
        .chars()
        .any(|ch| !(ch.is_ascii_alphanumeric() || "_+=,.@-".contains(ch)))
    {
        return Err(IamError::InvalidInput {
            message: format!(
                "{entity_kind} name contains invalid characters."
            ),
        });
    }
    Ok(())
}

fn ensure_name_is_unique<T>(
    existing_names: impl IntoIterator<Item = T>,
    requested_name: &str,
    entity_kind: &str,
) -> Result<(), IamError>
where
    T: AsRef<str>,
{
    let requested = requested_name.to_ascii_lowercase();
    if existing_names
        .into_iter()
        .any(|name| name.as_ref().to_ascii_lowercase() == requested)
    {
        return Err(IamError::EntityAlreadyExists {
            message: format!("{entity_kind} {requested_name} already exists."),
        });
    }
    Ok(())
}

fn normalize_path(path: &str) -> Result<String, IamError> {
    let normalized = if path.is_empty() { DEFAULT_PATH } else { path };

    if normalized.len() > IAM_PATH_MAX
        || !normalized.starts_with('/')
        || !normalized.ends_with('/')
    {
        return Err(IamError::InvalidInput {
            message: "Path must begin and end with '/' and be at most 512 characters.".to_owned(),
        });
    }

    if normalized.chars().any(|character| character.is_ascii_control()) {
        return Err(IamError::InvalidInput {
            message: "Path cannot contain control characters.".to_owned(),
        });
    }

    Ok(normalized.to_owned())
}

fn path_resource(path: &str, name: &str) -> String {
    if path == DEFAULT_PATH {
        format!("/{name}")
    } else {
        format!("{path}{name}")
    }
}

fn iam_arn(account_id: &AccountId, resource: &str) -> Arn {
    Arn::trusted_new(
        aws::Partition::aws(),
        aws::ServiceName::Iam,
        None,
        Some(account_id.clone()),
        ArnResource::Generic(resource.to_owned()),
    )
}

fn path_matches(path: &str, path_prefix: Option<&str>) -> bool {
    path_prefix.is_none_or(|prefix| path.starts_with(prefix))
}

fn tags_map(tags: &[IamTag]) -> BTreeMap<String, String> {
    tags.iter().map(|tag| (tag.key.clone(), tag.value.clone())).collect()
}

fn tags_vec(tags: &BTreeMap<String, String>) -> Vec<IamTag> {
    tags.iter()
        .map(|(key, value)| IamTag { key: key.clone(), value: value.clone() })
        .collect()
}

fn merge_tags(
    target: &mut BTreeMap<String, String>,
    tags: Vec<IamTag>,
) -> Result<(), IamError> {
    let keys: BTreeSet<_> = tags.iter().map(|tag| tag.key.as_str()).collect();
    let retained =
        target.keys().filter(|key| !keys.contains(key.as_str())).count();
    if retained + tags.len() > MAX_TAGS {
        return Err(IamError::LimitExceeded {
            message: format!(
                "Cannot exceed quota for TagsPerResource: {MAX_TAGS}."
            ),
        });
    }

    for tag in tags {
        target.insert(tag.key, tag.value);
    }
    Ok(())
}

fn remove_tags(target: &mut BTreeMap<String, String>, tag_keys: &[String]) {
    for key in tag_keys {
        target.remove(key);
    }
}

fn put_inline_policy(
    target: &mut BTreeMap<String, PolicyDocument>,
    input: InlinePolicyInput,
) -> Result<(), IamError> {
    validate_entity_name(&input.policy_name, "policy", ENTITY_NAME_MAX)?;
    target.insert(input.policy_name, PolicyDocument::new(input.document)?);
    Ok(())
}

fn get_inline_policy(
    target: &BTreeMap<String, PolicyDocument>,
    principal_name: &str,
    policy_name: &str,
) -> Result<InlinePolicy, IamError> {
    let document =
        target.get(policy_name).ok_or_else(|| IamError::NoSuchEntity {
            message: format!(
                "Inline policy {policy_name} for {principal_name} was not found."
            ),
        })?;
    Ok(InlinePolicy {
        document: document.as_str().to_owned(),
        policy_name: policy_name.to_owned(),
        principal_name: principal_name.to_owned(),
    })
}

fn delete_inline_policy(
    target: &mut BTreeMap<String, PolicyDocument>,
    policy_name: &str,
) -> Result<(), IamError> {
    if target.remove(policy_name).is_none() {
        return Err(IamError::NoSuchEntity {
            message: format!("Inline policy {policy_name} was not found."),
        });
    }
    Ok(())
}

fn list_inline_policies(
    target: &BTreeMap<String, PolicyDocument>,
) -> Vec<String> {
    target.keys().cloned().collect()
}

fn delete_conflict() -> IamError {
    IamError::DeleteConflict {
        message: "Cannot delete entity, must remove referenced objects first."
            .to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        CreateGroupInput, CreateInstanceProfileInput, CreatePolicyInput,
        CreatePolicyVersionInput, CreateRoleInput, CreateUserInput, IamScope,
        IamService, IamTag, InlinePolicyInput,
    };
    use aws::{
        AccountId, IamAccessKeyLookup, IamAccessKeyStatus,
        IamInstanceProfileLookup, RegionId,
    };

    fn scope() -> IamScope {
        IamScope::new(
            "000000000000".parse::<AccountId>().expect("account should parse"),
            "eu-west-2".parse::<RegionId>().expect("region should parse"),
        )
    }

    fn trust_policy() -> String {
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}"#.to_owned()
    }

    fn managed_policy_document() -> String {
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]}"#.to_owned()
    }

    #[test]
    fn iam_access_key_lifecycle_exposes_lookup_port_and_limits() {
        let service = IamService::new();
        let scope = scope();
        service
            .create_user(
                &scope,
                CreateUserInput {
                    path: "/".to_owned(),
                    tags: Vec::new(),
                    user_name: "alice".to_owned(),
                },
            )
            .expect("user should exist");

        let first_key = service
            .create_access_key(&scope, "alice")
            .expect("first access key should be created");
        let second_key = service
            .create_access_key(&scope, "alice")
            .expect("second access key should be created");

        let listed = service
            .list_access_keys(&scope, "alice")
            .expect("access keys should list");
        assert_eq!(listed.len(), 2);
        assert_eq!(listed[0].status, IamAccessKeyStatus::Active);

        service
            .update_access_key(
                &scope,
                "alice",
                &first_key.access_key_id,
                IamAccessKeyStatus::Expired,
            )
            .expect("status update should succeed");
        assert_eq!(
            service
                .list_access_keys(&scope, "alice")
                .expect("access keys should list after update")[0]
                .status,
            IamAccessKeyStatus::Expired
        );

        let lookup = IamAccessKeyLookup::find_access_key(
            &service,
            &first_key.access_key_id,
        )
        .expect("lookup port should resolve the access key");
        assert_eq!(lookup.account_id.as_str(), "000000000000");
        assert_eq!(lookup.region.as_str(), "eu-west-2");
        assert_eq!(lookup.secret_access_key, first_key.secret_access_key);
        assert_eq!(
            lookup.user_arn.to_string(),
            "arn:aws:iam::000000000000:user/alice"
        );
        assert_eq!(lookup.user_name, "alice");

        assert_eq!(
            service
                .create_access_key(&scope, "alice")
                .expect_err("third access key should exceed the quota")
                .to_aws_error()
                .code(),
            "LimitExceeded"
        );
        assert_eq!(
            service
                .delete_user(&scope, "alice")
                .expect_err("user deletion should fail while keys exist")
                .to_aws_error()
                .code(),
            "DeleteConflict"
        );

        service
            .delete_access_key(&scope, "alice", &first_key.access_key_id)
            .expect("first access key should delete");
        service
            .delete_access_key(&scope, "alice", &second_key.access_key_id)
            .expect("second access key should delete");
        assert!(
            IamAccessKeyLookup::find_access_key(
                &service,
                &first_key.access_key_id
            )
            .is_none()
        );
        service.delete_user(&scope, "alice").expect("user should delete");
    }

    #[test]
    fn iam_access_key_instance_profile_membership_rules_and_lookup_port() {
        let service = IamService::new();
        let scope = scope();
        service
            .create_role(
                &scope,
                CreateRoleInput {
                    assume_role_policy_document: trust_policy(),
                    description: "role".to_owned(),
                    max_session_duration: 3_600,
                    path: "/team/".to_owned(),
                    role_name: "primary-role".to_owned(),
                    tags: Vec::new(),
                },
            )
            .expect("primary role should exist");
        service
            .create_role(
                &scope,
                CreateRoleInput {
                    assume_role_policy_document: trust_policy(),
                    description: "role".to_owned(),
                    max_session_duration: 3_600,
                    path: "/team/".to_owned(),
                    role_name: "secondary-role".to_owned(),
                    tags: Vec::new(),
                },
            )
            .expect("secondary role should exist");
        let profile = service
            .create_instance_profile(
                &scope,
                CreateInstanceProfileInput {
                    instance_profile_name: "profile-a".to_owned(),
                    path: "/team/".to_owned(),
                    tags: vec![IamTag {
                        key: "env".to_owned(),
                        value: "dev".to_owned(),
                    }],
                },
            )
            .expect("instance profile should be created");
        assert!(profile.instance_profile_id.starts_with("AIPA"));

        service
            .add_role_to_instance_profile(&scope, "profile-a", "primary-role")
            .expect("first role should attach");
        assert_eq!(
            service
                .add_role_to_instance_profile(
                    &scope,
                    "profile-a",
                    "primary-role",
                )
                .expect_err("duplicate role attachment should fail")
                .to_aws_error()
                .code(),
            "EntityAlreadyExists"
        );
        assert_eq!(
            service
                .add_role_to_instance_profile(
                    &scope,
                    "profile-a",
                    "secondary-role",
                )
                .expect_err("second distinct role should exceed the quota")
                .to_aws_error()
                .code(),
            "LimitExceeded"
        );

        service
            .tag_instance_profile(
                &scope,
                "profile-a",
                vec![IamTag {
                    key: "owner".to_owned(),
                    value: "platform".to_owned(),
                }],
            )
            .expect("profile tags should update");
        service
            .untag_instance_profile(
                &scope,
                "profile-a",
                vec!["env".to_owned()],
            )
            .expect("profile tag removal should succeed");

        let fetched = service
            .get_instance_profile(&scope, "profile-a")
            .expect("profile should fetch");
        let role_profiles = service
            .list_instance_profiles_for_role(&scope, "primary-role")
            .expect("role profiles should list");
        let lookup = IamInstanceProfileLookup::find_instance_profile(
            &service,
            scope.account_id(),
            scope.region(),
            "profile-a",
        )
        .expect("lookup port should resolve the profile");

        assert_eq!(fetched.roles.len(), 1);
        assert_eq!(fetched.roles[0].role_name, "primary-role");
        assert_eq!(fetched.tags[0].key, "owner");
        assert_eq!(role_profiles[0].instance_profile_name, "profile-a");
        assert_eq!(lookup.account_id.as_str(), "000000000000");
        assert_eq!(lookup.region.as_str(), "eu-west-2");
        assert_eq!(
            lookup.arn.to_string(),
            "arn:aws:iam::000000000000:instance-profile/team/profile-a"
        );
        assert_eq!(
            lookup.role.expect("lookup should include the role").role_name,
            "primary-role"
        );

        assert_eq!(
            service
                .delete_role(&scope, "primary-role")
                .expect_err("attached role should not delete")
                .to_aws_error()
                .code(),
            "DeleteConflict"
        );
        assert_eq!(
            service
                .delete_instance_profile(&scope, "profile-a")
                .expect_err("profile with a role should not delete")
                .to_aws_error()
                .code(),
            "DeleteConflict"
        );

        service
            .remove_role_from_instance_profile(
                &scope,
                "profile-a",
                "primary-role",
            )
            .expect("role should detach");
        assert!(
            IamInstanceProfileLookup::find_instance_profiles_for_role(
                &service,
                scope.account_id(),
                scope.region(),
                "primary-role",
            )
            .is_empty()
        );
        service
            .delete_instance_profile(&scope, "profile-a")
            .expect("profile should delete");
        service
            .delete_role(&scope, "primary-role")
            .expect("primary role should delete");
        service
            .delete_role(&scope, "secondary-role")
            .expect("secondary role should delete");
    }

    #[test]
    fn iam_principal_policy_round_trip_stays_scoped() {
        let service = IamService::new();
        let scope = scope();

        let user = service
            .create_user(
                &scope,
                CreateUserInput {
                    path: "/team/".to_owned(),
                    tags: vec![IamTag {
                        key: "env".to_owned(),
                        value: "dev".to_owned(),
                    }],
                    user_name: "alice".to_owned(),
                },
            )
            .expect("user should be created");
        let group = service
            .create_group(
                &scope,
                CreateGroupInput {
                    group_name: "admins".to_owned(),
                    path: "/team/".to_owned(),
                },
            )
            .expect("group should be created");
        let role = service
            .create_role(
                &scope,
                CreateRoleInput {
                    assume_role_policy_document: trust_policy(),
                    description: "role".to_owned(),
                    max_session_duration: 3_600,
                    path: "/team/".to_owned(),
                    role_name: "app-role".to_owned(),
                    tags: vec![IamTag {
                        key: "tier".to_owned(),
                        value: "backend".to_owned(),
                    }],
                },
            )
            .expect("role should be created");
        let policy = service
            .create_policy(
                &scope,
                CreatePolicyInput {
                    description: "managed".to_owned(),
                    path: "/team/".to_owned(),
                    policy_document: managed_policy_document(),
                    policy_name: "read-only".to_owned(),
                    tags: vec![IamTag {
                        key: "owner".to_owned(),
                        value: "security".to_owned(),
                    }],
                },
            )
            .expect("policy should be created");

        service
            .add_user_to_group(&scope, "admins", "alice")
            .expect("membership should be added");
        service
            .attach_user_policy(&scope, "alice", &policy.arn)
            .expect("user attachment should succeed");
        service
            .attach_group_policy(&scope, "admins", &policy.arn)
            .expect("group attachment should succeed");
        service
            .attach_role_policy(&scope, "app-role", &policy.arn)
            .expect("role attachment should succeed");
        service
            .put_user_policy(
                &scope,
                "alice",
                InlinePolicyInput {
                    document: managed_policy_document(),
                    policy_name: "inline-user".to_owned(),
                },
            )
            .expect("user inline policy should be stored");
        service
            .put_group_policy(
                &scope,
                "admins",
                InlinePolicyInput {
                    document: managed_policy_document(),
                    policy_name: "inline-group".to_owned(),
                },
            )
            .expect("group inline policy should be stored");
        service
            .put_role_policy(
                &scope,
                "app-role",
                InlinePolicyInput {
                    document: managed_policy_document(),
                    policy_name: "inline-role".to_owned(),
                },
            )
            .expect("role inline policy should be stored");

        let user_tags = service
            .list_user_tags(&scope, "alice")
            .expect("user tags should list");
        let group_details = service
            .get_group(&scope, "admins")
            .expect("group should be fetchable");
        let groups_for_user = service
            .list_groups_for_user(&scope, "alice")
            .expect("groups should list");
        let role_tags = service
            .list_role_tags(&scope, "app-role")
            .expect("role tags should list");
        let policy_tags = service
            .list_policy_tags(&scope, &policy.arn)
            .expect("policy tags should list");
        let user_inline = service
            .get_user_policy(&scope, "alice", "inline-user")
            .expect("user inline policy should fetch");
        let group_inline = service
            .get_group_policy(&scope, "admins", "inline-group")
            .expect("group inline policy should fetch");
        let role_inline = service
            .get_role_policy(&scope, "app-role", "inline-role")
            .expect("role inline policy should fetch");
        let user_attached = service
            .list_attached_user_policies(&scope, "alice", Some("/team/"))
            .expect("user attachments should list");
        let group_attached = service
            .list_attached_group_policies(&scope, "admins", Some("/team/"))
            .expect("group attachments should list");
        let role_attached = service
            .list_attached_role_policies(&scope, "app-role", Some("/team/"))
            .expect("role attachments should list");
        let policies = service
            .list_policies(&scope, Some("Local"), Some("/team/"))
            .expect("policies should list");

        assert_eq!(user.user_name, "alice");
        assert_eq!(group.group_name, "admins");
        assert_eq!(role.role_name, "app-role");
        assert_eq!(user_tags[0].key, "env");
        assert_eq!(group_details.users[0].user_name, "alice");
        assert_eq!(groups_for_user[0].group_name, "admins");
        assert_eq!(role_tags[0].key, "tier");
        assert_eq!(policy_tags[0].key, "owner");
        assert!(user_inline.document.contains("s3:GetObject"));
        assert!(group_inline.document.contains("s3:GetObject"));
        assert!(role_inline.document.contains("s3:GetObject"));
        assert_eq!(user_attached[0].policy_arn, policy.arn);
        assert_eq!(group_attached[0].policy_name, "read-only");
        assert_eq!(role_attached[0].policy_name, "read-only");
        assert_eq!(policies[0].attachment_count, 3);
    }

    #[test]
    fn iam_policy_versions_enforce_limits_and_default_rules() {
        let service = IamService::new();
        let scope = scope();
        let policy = service
            .create_policy(
                &scope,
                CreatePolicyInput {
                    description: "managed".to_owned(),
                    path: "/".to_owned(),
                    policy_document: managed_policy_document(),
                    policy_name: "versions".to_owned(),
                    tags: Vec::new(),
                },
            )
            .expect("policy should be created");

        for index in 0..4 {
            let version = service
                .create_policy_version(
                    &scope,
                    CreatePolicyVersionInput {
                        policy_arn: policy.arn.clone(),
                        policy_document: format!(
                            "{{\"Version\":\"2012-10-17\",\"Statement\":[{{\"Effect\":\"Allow\",\"Action\":\"service:{index}\",\"Resource\":\"*\"}}]}}"
                        ),
                        set_as_default: index == 3,
                    },
                )
                .expect("version should be created");
            if index == 3 {
                assert!(version.is_default_version);
            }
        }

        let limit_error = service
            .create_policy_version(
                &scope,
                CreatePolicyVersionInput {
                    policy_arn: policy.arn.clone(),
                    policy_document: managed_policy_document(),
                    set_as_default: false,
                },
            )
            .expect_err("sixth version should fail");
        assert!(limit_error.to_aws_error().code() == "LimitExceeded");

        let versions = service
            .list_policy_versions(&scope, &policy.arn)
            .expect("versions should list");
        assert_eq!(versions.len(), 5);
        assert_eq!(
            service
                .get_policy_version(&scope, &policy.arn, "v5")
                .expect("latest version should load")
                .version_id,
            "v5"
        );

        let delete_default = service
            .delete_policy_version(&scope, &policy.arn, "v5")
            .expect_err("default version should not delete");
        assert_eq!(delete_default.to_aws_error().code(), "DeleteConflict");

        service
            .set_default_policy_version(&scope, &policy.arn, "v2")
            .expect("new default should be accepted");
        service
            .delete_policy_version(&scope, &policy.arn, "v5")
            .expect("non-default version should delete");
        assert_eq!(
            service
                .get_policy(&scope, &policy.arn)
                .expect("policy should still exist")
                .default_version_id,
            "v2"
        );
    }

    #[test]
    fn iam_delete_conflicts_cover_user_group_role_and_policy_relationships() {
        let service = IamService::new();
        let scope = scope();
        let policy = service
            .create_policy(
                &scope,
                CreatePolicyInput {
                    description: "managed".to_owned(),
                    path: "/".to_owned(),
                    policy_document: managed_policy_document(),
                    policy_name: "attached".to_owned(),
                    tags: Vec::new(),
                },
            )
            .expect("policy should be created");
        service
            .create_user(
                &scope,
                CreateUserInput {
                    path: "/".to_owned(),
                    tags: Vec::new(),
                    user_name: "alice".to_owned(),
                },
            )
            .expect("user should exist");
        service
            .create_group(
                &scope,
                CreateGroupInput {
                    group_name: "admins".to_owned(),
                    path: "/".to_owned(),
                },
            )
            .expect("group should exist");
        service
            .create_role(
                &scope,
                CreateRoleInput {
                    assume_role_policy_document: trust_policy(),
                    description: "role".to_owned(),
                    max_session_duration: 3_600,
                    path: "/".to_owned(),
                    role_name: "app-role".to_owned(),
                    tags: Vec::new(),
                },
            )
            .expect("role should exist");

        service
            .attach_user_policy(&scope, "alice", &policy.arn)
            .expect("user policy should attach");
        service
            .add_user_to_group(&scope, "admins", "alice")
            .expect("membership should attach");
        service
            .attach_role_policy(&scope, "app-role", &policy.arn)
            .expect("role policy should attach");

        assert_eq!(
            service
                .delete_user(&scope, "alice")
                .expect_err("attached user should fail")
                .to_aws_error()
                .code(),
            "DeleteConflict"
        );
        assert_eq!(
            service
                .delete_group(&scope, "admins")
                .expect_err("group with members should fail")
                .to_aws_error()
                .code(),
            "DeleteConflict"
        );
        assert_eq!(
            service
                .delete_role(&scope, "app-role")
                .expect_err("attached role should fail")
                .to_aws_error()
                .code(),
            "DeleteConflict"
        );
        assert_eq!(
            service
                .delete_policy(&scope, &policy.arn)
                .expect_err("attached policy should fail")
                .to_aws_error()
                .code(),
            "DeleteConflict"
        );

        service
            .detach_user_policy(&scope, "alice", &policy.arn)
            .expect("user policy should detach");
        service
            .remove_user_from_group(&scope, "admins", "alice")
            .expect("membership should detach");
        service
            .detach_role_policy(&scope, "app-role", &policy.arn)
            .expect("role policy should detach");
        service.delete_user(&scope, "alice").expect("user should delete");
        service.delete_group(&scope, "admins").expect("group should delete");
        service.delete_role(&scope, "app-role").expect("role should delete");
        service
            .delete_policy(&scope, &policy.arn)
            .expect("policy should delete");
    }

    #[test]
    fn iam_tag_updates_replace_and_remove_keys() {
        let service = IamService::new();
        let scope = scope();
        let policy = service
            .create_policy(
                &scope,
                CreatePolicyInput {
                    description: "managed".to_owned(),
                    path: "/".to_owned(),
                    policy_document: managed_policy_document(),
                    policy_name: "tagged".to_owned(),
                    tags: vec![IamTag {
                        key: "env".to_owned(),
                        value: "dev".to_owned(),
                    }],
                },
            )
            .expect("policy should be created");
        service
            .create_user(
                &scope,
                CreateUserInput {
                    path: "/".to_owned(),
                    tags: vec![IamTag {
                        key: "env".to_owned(),
                        value: "dev".to_owned(),
                    }],
                    user_name: "alice".to_owned(),
                },
            )
            .expect("user should exist");
        service
            .create_role(
                &scope,
                CreateRoleInput {
                    assume_role_policy_document: trust_policy(),
                    description: "role".to_owned(),
                    max_session_duration: 3_600,
                    path: "/".to_owned(),
                    role_name: "app-role".to_owned(),
                    tags: vec![IamTag {
                        key: "env".to_owned(),
                        value: "dev".to_owned(),
                    }],
                },
            )
            .expect("role should exist");

        service
            .tag_user(
                &scope,
                "alice",
                vec![IamTag {
                    key: "env".to_owned(),
                    value: "prod".to_owned(),
                }],
            )
            .expect("user tags should update");
        service
            .tag_role(
                &scope,
                "app-role",
                vec![IamTag {
                    key: "env".to_owned(),
                    value: "prod".to_owned(),
                }],
            )
            .expect("role tags should update");
        service
            .tag_policy(
                &scope,
                &policy.arn,
                vec![IamTag {
                    key: "env".to_owned(),
                    value: "prod".to_owned(),
                }],
            )
            .expect("policy tags should update");
        service
            .untag_user(&scope, "alice", vec!["env".to_owned()])
            .expect("user tag should be removed");
        service
            .untag_role(&scope, "app-role", vec!["env".to_owned()])
            .expect("role tag should be removed");
        service
            .untag_policy(&scope, &policy.arn, vec!["env".to_owned()])
            .expect("policy tag should be removed");

        assert!(
            service
                .list_user_tags(&scope, "alice")
                .expect("user tags should list")
                .is_empty()
        );
        assert!(
            service
                .list_role_tags(&scope, "app-role")
                .expect("role tags should list")
                .is_empty()
        );
        assert!(
            service
                .list_policy_tags(&scope, &policy.arn)
                .expect("policy tags should list")
                .is_empty()
        );
    }

    #[test]
    fn iam_rejects_invalid_documents_and_duplicate_names_case_insensitively() {
        let service = IamService::new();
        let scope = scope();

        let policy_error = service
            .create_policy(
                &scope,
                CreatePolicyInput {
                    description: "managed".to_owned(),
                    path: "/".to_owned(),
                    policy_document: "not-json".to_owned(),
                    policy_name: "bad".to_owned(),
                    tags: Vec::new(),
                },
            )
            .expect_err("invalid policy should fail");
        assert_eq!(
            policy_error.to_aws_error().code(),
            "MalformedPolicyDocument"
        );

        let role_error = service
            .create_role(
                &scope,
                CreateRoleInput {
                    assume_role_policy_document: "not-json".to_owned(),
                    description: "role".to_owned(),
                    max_session_duration: 3_600,
                    path: "/".to_owned(),
                    role_name: "bad-role".to_owned(),
                    tags: Vec::new(),
                },
            )
            .expect_err("invalid trust policy should fail");
        assert_eq!(
            role_error.to_aws_error().code(),
            "MalformedPolicyDocument"
        );

        service
            .create_user(
                &scope,
                CreateUserInput {
                    path: "/".to_owned(),
                    tags: Vec::new(),
                    user_name: "Alice".to_owned(),
                },
            )
            .expect("first user should succeed");
        let duplicate = service
            .create_user(
                &scope,
                CreateUserInput {
                    path: "/".to_owned(),
                    tags: Vec::new(),
                    user_name: "alice".to_owned(),
                },
            )
            .expect_err("case-insensitive duplicate should fail");
        assert_eq!(duplicate.to_aws_error().code(), "EntityAlreadyExists");
    }
}
