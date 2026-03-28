pub(crate) use crate::aws_error_shape::AwsErrorShape;
use crate::query::{QueryParameters, missing_action_error};
use crate::xml::XmlBuilder;
use aws::{Arn, AwsError, AwsErrorFamily, IamAccessKeyStatus, RequestContext};
use services::{
    AttachedPolicy, CreateGroupInput, CreateInstanceProfileInput,
    CreatePolicyInput, CreatePolicyVersionInput, CreateRoleInput,
    CreateUserInput, GroupDetails, IamAccessKey, IamAccessKeyMetadata,
    IamError, IamGroup, IamInstanceProfile, IamPolicy, IamPolicyVersion,
    IamRole, IamScope, IamService, IamTag, IamUser, InlinePolicyInput,
};

const IAM_XMLNS: &str = "https://iam.amazonaws.com/doc/2010-05-08/";
const REQUEST_ID: &str = "0000000000000000";

pub(crate) fn is_iam_action(action: &str) -> bool {
    matches!(
        action,
        "CreateUser"
            | "GetUser"
            | "DeleteUser"
            | "ListUsers"
            | "TagUser"
            | "UntagUser"
            | "ListUserTags"
            | "CreateGroup"
            | "GetGroup"
            | "DeleteGroup"
            | "ListGroups"
            | "AddUserToGroup"
            | "RemoveUserFromGroup"
            | "ListGroupsForUser"
            | "CreateRole"
            | "GetRole"
            | "DeleteRole"
            | "ListRoles"
            | "TagRole"
            | "UntagRole"
            | "ListRoleTags"
            | "CreatePolicy"
            | "GetPolicy"
            | "DeletePolicy"
            | "ListPolicies"
            | "CreatePolicyVersion"
            | "GetPolicyVersion"
            | "DeletePolicyVersion"
            | "ListPolicyVersions"
            | "SetDefaultPolicyVersion"
            | "TagPolicy"
            | "UntagPolicy"
            | "ListPolicyTags"
            | "AttachUserPolicy"
            | "DetachUserPolicy"
            | "ListAttachedUserPolicies"
            | "AttachGroupPolicy"
            | "DetachGroupPolicy"
            | "ListAttachedGroupPolicies"
            | "AttachRolePolicy"
            | "DetachRolePolicy"
            | "ListAttachedRolePolicies"
            | "PutUserPolicy"
            | "GetUserPolicy"
            | "DeleteUserPolicy"
            | "ListUserPolicies"
            | "CreateAccessKey"
            | "DeleteAccessKey"
            | "ListAccessKeys"
            | "UpdateAccessKey"
            | "CreateInstanceProfile"
            | "GetInstanceProfile"
            | "DeleteInstanceProfile"
            | "ListInstanceProfiles"
            | "AddRoleToInstanceProfile"
            | "RemoveRoleFromInstanceProfile"
            | "ListInstanceProfilesForRole"
            | "TagInstanceProfile"
            | "UntagInstanceProfile"
            | "ListInstanceProfileTags"
            | "PutGroupPolicy"
            | "GetGroupPolicy"
            | "DeleteGroupPolicy"
            | "ListGroupPolicies"
            | "PutRolePolicy"
            | "GetRolePolicy"
            | "DeleteRolePolicy"
            | "ListRolePolicies"
    )
}

pub(crate) fn handle(
    iam: &IamService,
    body: &[u8],
    context: &RequestContext,
) -> Result<String, AwsError> {
    let params = QueryParameters::parse(body)?;
    let Some(action) = params.action() else {
        return Err(missing_action_error());
    };
    let scope =
        IamScope::new(context.account_id().clone(), context.region().clone());

    let body = match action {
        "CreateUser" => {
            let user = iam
                .create_user(
                    &scope,
                    CreateUserInput {
                        path: params
                            .optional("Path")
                            .unwrap_or("/")
                            .to_owned(),
                        tags: params.tags()?,
                        user_name: params.required("UserName")?.to_owned(),
                    },
                )
                .map_err(|error| error.to_aws_error())?;
            response_with_result(action, &user_xml(&user))
        }
        "GetUser" => {
            let user = iam
                .get_user(&scope, params.required("UserName")?)
                .map_err(|error| error.to_aws_error())?;
            response_with_result(action, &user_xml(&user))
        }
        "DeleteUser" => {
            iam.delete_user(&scope, params.required("UserName")?)
                .map_err(|error| error.to_aws_error())?;
            response_without_result(action)
        }
        "ListUsers" => {
            let users = iam.list_users(&scope, params.optional("PathPrefix"));
            response_with_result(action, &list_users_xml(&users))
        }
        "TagUser" => {
            iam.tag_user(&scope, params.required("UserName")?, params.tags()?)
                .map_err(|error| error.to_aws_error())?;
            response_without_result(action)
        }
        "UntagUser" => {
            iam.untag_user(
                &scope,
                params.required("UserName")?,
                params.tag_keys(),
            )
            .map_err(|error| error.to_aws_error())?;
            response_without_result(action)
        }
        "ListUserTags" => {
            let tags = iam
                .list_user_tags(&scope, params.required("UserName")?)
                .map_err(|error| error.to_aws_error())?;
            response_with_result(action, &tags_result_xml(&tags))
        }
        "CreateAccessKey" => {
            let access_key = iam
                .create_access_key(&scope, params.required("UserName")?)
                .map_err(|error| error.to_aws_error())?;
            response_with_result(action, &access_key_xml(&access_key))
        }
        "DeleteAccessKey" => {
            iam.delete_access_key(
                &scope,
                params.required("UserName")?,
                params.required("AccessKeyId")?,
            )
            .map_err(|error| error.to_aws_error())?;
            response_without_result(action)
        }
        "ListAccessKeys" => {
            let access_keys = iam
                .list_access_keys(&scope, params.required("UserName")?)
                .map_err(|error| error.to_aws_error())?;
            response_with_result(action, &list_access_keys_xml(&access_keys))
        }
        "UpdateAccessKey" => {
            iam.update_access_key(
                &scope,
                params.required("UserName")?,
                params.required("AccessKeyId")?,
                parse_access_key_status(params.required("Status")?)?,
            )
            .map_err(|error| error.to_aws_error())?;
            response_without_result(action)
        }
        "CreateGroup" => {
            let group = iam
                .create_group(
                    &scope,
                    CreateGroupInput {
                        group_name: params.required("GroupName")?.to_owned(),
                        path: params
                            .optional("Path")
                            .unwrap_or("/")
                            .to_owned(),
                    },
                )
                .map_err(|error| error.to_aws_error())?;
            response_with_result(action, &group_xml(&group))
        }
        "GetGroup" => {
            let details = iam
                .get_group(&scope, params.required("GroupName")?)
                .map_err(|error| error.to_aws_error())?;
            response_with_result(action, &group_details_xml(&details))
        }
        "DeleteGroup" => {
            iam.delete_group(&scope, params.required("GroupName")?)
                .map_err(|error| error.to_aws_error())?;
            response_without_result(action)
        }
        "ListGroups" => {
            let groups =
                iam.list_groups(&scope, params.optional("PathPrefix"));
            response_with_result(action, &list_groups_xml(&groups))
        }
        "AddUserToGroup" => {
            iam.add_user_to_group(
                &scope,
                params.required("GroupName")?,
                params.required("UserName")?,
            )
            .map_err(|error| error.to_aws_error())?;
            response_without_result(action)
        }
        "RemoveUserFromGroup" => {
            iam.remove_user_from_group(
                &scope,
                params.required("GroupName")?,
                params.required("UserName")?,
            )
            .map_err(|error| error.to_aws_error())?;
            response_without_result(action)
        }
        "ListGroupsForUser" => {
            let groups = iam
                .list_groups_for_user(&scope, params.required("UserName")?)
                .map_err(|error| error.to_aws_error())?;
            response_with_result(action, &list_groups_xml(&groups))
        }
        "CreateRole" => {
            let role = iam
                .create_role(
                    &scope,
                    CreateRoleInput {
                        assume_role_policy_document: params
                            .required("AssumeRolePolicyDocument")?
                            .to_owned(),
                        description: params
                            .optional("Description")
                            .unwrap_or_default()
                            .to_owned(),
                        max_session_duration: params
                            .parse_u32("MaxSessionDuration")?
                            .unwrap_or(3_600),
                        path: params
                            .optional("Path")
                            .unwrap_or("/")
                            .to_owned(),
                        role_name: params.required("RoleName")?.to_owned(),
                        tags: params.tags()?,
                    },
                )
                .map_err(|error| error.to_aws_error())?;
            response_with_result(action, &role_xml(&role))
        }
        "GetRole" => {
            let role = iam
                .get_role(&scope, params.required("RoleName")?)
                .map_err(|error| error.to_aws_error())?;
            response_with_result(action, &role_xml(&role))
        }
        "DeleteRole" => {
            iam.delete_role(&scope, params.required("RoleName")?)
                .map_err(|error| error.to_aws_error())?;
            response_without_result(action)
        }
        "ListRoles" => {
            let roles = iam.list_roles(&scope, params.optional("PathPrefix"));
            response_with_result(action, &list_roles_xml(&roles))
        }
        "TagRole" => {
            iam.tag_role(&scope, params.required("RoleName")?, params.tags()?)
                .map_err(|error| error.to_aws_error())?;
            response_without_result(action)
        }
        "UntagRole" => {
            iam.untag_role(
                &scope,
                params.required("RoleName")?,
                params.tag_keys(),
            )
            .map_err(|error| error.to_aws_error())?;
            response_without_result(action)
        }
        "ListRoleTags" => {
            let tags = iam
                .list_role_tags(&scope, params.required("RoleName")?)
                .map_err(|error| error.to_aws_error())?;
            response_with_result(action, &tags_result_xml(&tags))
        }
        "CreateInstanceProfile" => {
            let profile = iam
                .create_instance_profile(
                    &scope,
                    CreateInstanceProfileInput {
                        instance_profile_name: params
                            .required("InstanceProfileName")?
                            .to_owned(),
                        path: params
                            .optional("Path")
                            .unwrap_or("/")
                            .to_owned(),
                        tags: params.tags()?,
                    },
                )
                .map_err(|error| error.to_aws_error())?;
            response_with_result(action, &instance_profile_xml(&profile))
        }
        "GetInstanceProfile" => {
            let profile = iam
                .get_instance_profile(
                    &scope,
                    params.required("InstanceProfileName")?,
                )
                .map_err(|error| error.to_aws_error())?;
            response_with_result(action, &instance_profile_xml(&profile))
        }
        "DeleteInstanceProfile" => {
            iam.delete_instance_profile(
                &scope,
                params.required("InstanceProfileName")?,
            )
            .map_err(|error| error.to_aws_error())?;
            response_without_result(action)
        }
        "ListInstanceProfiles" => {
            let profiles = iam
                .list_instance_profiles(&scope, params.optional("PathPrefix"));
            response_with_result(
                action,
                &list_instance_profiles_xml(&profiles),
            )
        }
        "AddRoleToInstanceProfile" => {
            iam.add_role_to_instance_profile(
                &scope,
                params.required("InstanceProfileName")?,
                params.required("RoleName")?,
            )
            .map_err(|error| error.to_aws_error())?;
            response_without_result(action)
        }
        "RemoveRoleFromInstanceProfile" => {
            iam.remove_role_from_instance_profile(
                &scope,
                params.required("InstanceProfileName")?,
                params.required("RoleName")?,
            )
            .map_err(|error| error.to_aws_error())?;
            response_without_result(action)
        }
        "ListInstanceProfilesForRole" => {
            let profiles = iam
                .list_instance_profiles_for_role(
                    &scope,
                    params.required("RoleName")?,
                )
                .map_err(|error| error.to_aws_error())?;
            response_with_result(
                action,
                &list_instance_profiles_xml(&profiles),
            )
        }
        "TagInstanceProfile" => {
            iam.tag_instance_profile(
                &scope,
                params.required("InstanceProfileName")?,
                params.tags()?,
            )
            .map_err(|error| error.to_aws_error())?;
            response_without_result(action)
        }
        "UntagInstanceProfile" => {
            iam.untag_instance_profile(
                &scope,
                params.required("InstanceProfileName")?,
                params.tag_keys(),
            )
            .map_err(|error| error.to_aws_error())?;
            response_without_result(action)
        }
        "ListInstanceProfileTags" => {
            let tags = iam
                .list_instance_profile_tags(
                    &scope,
                    params.required("InstanceProfileName")?,
                )
                .map_err(|error| error.to_aws_error())?;
            response_with_result(action, &tags_result_xml(&tags))
        }
        "CreatePolicy" => {
            let policy = iam
                .create_policy(
                    &scope,
                    CreatePolicyInput {
                        description: params
                            .optional("Description")
                            .unwrap_or_default()
                            .to_owned(),
                        path: params
                            .optional("Path")
                            .unwrap_or("/")
                            .to_owned(),
                        policy_document: params
                            .required("PolicyDocument")?
                            .to_owned(),
                        policy_name: params.required("PolicyName")?.to_owned(),
                        tags: params.tags()?,
                    },
                )
                .map_err(|error| error.to_aws_error())?;
            response_with_result(action, &policy_xml(&policy))
        }
        "GetPolicy" => {
            let policy_arn = parse_arn(&params, "PolicyArn")?;
            let policy = iam
                .get_policy(&scope, &policy_arn)
                .map_err(|error| error.to_aws_error())?;
            response_with_result(action, &policy_xml(&policy))
        }
        "DeletePolicy" => {
            let policy_arn = parse_arn(&params, "PolicyArn")?;
            iam.delete_policy(&scope, &policy_arn)
                .map_err(|error| error.to_aws_error())?;
            response_without_result(action)
        }
        "ListPolicies" => {
            let policies = iam
                .list_policies(
                    &scope,
                    params.optional("Scope"),
                    params.optional("PathPrefix"),
                )
                .map_err(|error| error.to_aws_error())?;
            response_with_result(action, &list_policies_xml(&policies))
        }
        "CreatePolicyVersion" => {
            let version = iam
                .create_policy_version(
                    &scope,
                    CreatePolicyVersionInput {
                        policy_arn: parse_arn(&params, "PolicyArn")?,
                        policy_document: params
                            .required("PolicyDocument")?
                            .to_owned(),
                        set_as_default: params
                            .optional("SetAsDefault")
                            .is_some_and(|value| {
                                value.eq_ignore_ascii_case("true")
                            }),
                    },
                )
                .map_err(|error| error.to_aws_error())?;
            response_with_result(
                action,
                &policy_version_xml("PolicyVersion", &version),
            )
        }
        "GetPolicyVersion" => {
            let policy_arn = parse_arn(&params, "PolicyArn")?;
            let version = iam
                .get_policy_version(
                    &scope,
                    &policy_arn,
                    params.required("VersionId")?,
                )
                .map_err(|error| error.to_aws_error())?;
            response_with_result(
                action,
                &policy_version_xml("PolicyVersion", &version),
            )
        }
        "DeletePolicyVersion" => {
            let policy_arn = parse_arn(&params, "PolicyArn")?;
            iam.delete_policy_version(
                &scope,
                &policy_arn,
                params.required("VersionId")?,
            )
            .map_err(|error| error.to_aws_error())?;
            response_without_result(action)
        }
        "ListPolicyVersions" => {
            let policy_arn = parse_arn(&params, "PolicyArn")?;
            let versions = iam
                .list_policy_versions(&scope, &policy_arn)
                .map_err(|error| error.to_aws_error())?;
            response_with_result(action, &list_policy_versions_xml(&versions))
        }
        "SetDefaultPolicyVersion" => {
            let policy_arn = parse_arn(&params, "PolicyArn")?;
            iam.set_default_policy_version(
                &scope,
                &policy_arn,
                params.required("VersionId")?,
            )
            .map_err(|error| error.to_aws_error())?;
            response_without_result(action)
        }
        "TagPolicy" => {
            let policy_arn = parse_arn(&params, "PolicyArn")?;
            iam.tag_policy(&scope, &policy_arn, params.tags()?)
                .map_err(|error| error.to_aws_error())?;
            response_without_result(action)
        }
        "UntagPolicy" => {
            let policy_arn = parse_arn(&params, "PolicyArn")?;
            iam.untag_policy(&scope, &policy_arn, params.tag_keys())
                .map_err(|error| error.to_aws_error())?;
            response_without_result(action)
        }
        "ListPolicyTags" => {
            let policy_arn = parse_arn(&params, "PolicyArn")?;
            let tags = iam
                .list_policy_tags(&scope, &policy_arn)
                .map_err(|error| error.to_aws_error())?;
            response_with_result(action, &tags_result_xml(&tags))
        }
        "AttachUserPolicy" => {
            let policy_arn = parse_arn(&params, "PolicyArn")?;
            iam.attach_user_policy(
                &scope,
                params.required("UserName")?,
                &policy_arn,
            )
            .map_err(|error| error.to_aws_error())?;
            response_without_result(action)
        }
        "DetachUserPolicy" => {
            let policy_arn = parse_arn(&params, "PolicyArn")?;
            iam.detach_user_policy(
                &scope,
                params.required("UserName")?,
                &policy_arn,
            )
            .map_err(|error| error.to_aws_error())?;
            response_without_result(action)
        }
        "ListAttachedUserPolicies" => {
            let policies = iam
                .list_attached_user_policies(
                    &scope,
                    params.required("UserName")?,
                    params.optional("PathPrefix"),
                )
                .map_err(|error| error.to_aws_error())?;
            response_with_result(action, &attached_policies_xml(&policies))
        }
        "AttachGroupPolicy" => {
            let policy_arn = parse_arn(&params, "PolicyArn")?;
            iam.attach_group_policy(
                &scope,
                params.required("GroupName")?,
                &policy_arn,
            )
            .map_err(|error| error.to_aws_error())?;
            response_without_result(action)
        }
        "DetachGroupPolicy" => {
            let policy_arn = parse_arn(&params, "PolicyArn")?;
            iam.detach_group_policy(
                &scope,
                params.required("GroupName")?,
                &policy_arn,
            )
            .map_err(|error| error.to_aws_error())?;
            response_without_result(action)
        }
        "ListAttachedGroupPolicies" => {
            let policies = iam
                .list_attached_group_policies(
                    &scope,
                    params.required("GroupName")?,
                    params.optional("PathPrefix"),
                )
                .map_err(|error| error.to_aws_error())?;
            response_with_result(action, &attached_policies_xml(&policies))
        }
        "AttachRolePolicy" => {
            let policy_arn = parse_arn(&params, "PolicyArn")?;
            iam.attach_role_policy(
                &scope,
                params.required("RoleName")?,
                &policy_arn,
            )
            .map_err(|error| error.to_aws_error())?;
            response_without_result(action)
        }
        "DetachRolePolicy" => {
            let policy_arn = parse_arn(&params, "PolicyArn")?;
            iam.detach_role_policy(
                &scope,
                params.required("RoleName")?,
                &policy_arn,
            )
            .map_err(|error| error.to_aws_error())?;
            response_without_result(action)
        }
        "ListAttachedRolePolicies" => {
            let policies = iam
                .list_attached_role_policies(
                    &scope,
                    params.required("RoleName")?,
                    params.optional("PathPrefix"),
                )
                .map_err(|error| error.to_aws_error())?;
            response_with_result(action, &attached_policies_xml(&policies))
        }
        "PutUserPolicy" => {
            iam.put_user_policy(
                &scope,
                params.required("UserName")?,
                InlinePolicyInput {
                    document: params.required("PolicyDocument")?.to_owned(),
                    policy_name: params.required("PolicyName")?.to_owned(),
                },
            )
            .map_err(|error| error.to_aws_error())?;
            response_without_result(action)
        }
        "GetUserPolicy" => {
            let principal = params.required("UserName")?;
            let policy_name = params.required("PolicyName")?;
            let policy = iam
                .get_user_policy(&scope, principal, policy_name)
                .map_err(|error| error.to_aws_error())?;
            response_with_result(
                action,
                &inline_policy_xml("UserName", principal, &policy),
            )
        }
        "DeleteUserPolicy" => {
            iam.delete_user_policy(
                &scope,
                params.required("UserName")?,
                params.required("PolicyName")?,
            )
            .map_err(|error| error.to_aws_error())?;
            response_without_result(action)
        }
        "ListUserPolicies" => {
            let names = iam
                .list_user_policies(&scope, params.required("UserName")?)
                .map_err(|error| error.to_aws_error())?;
            response_with_result(action, &inline_policy_names_xml(&names))
        }
        "PutGroupPolicy" => {
            iam.put_group_policy(
                &scope,
                params.required("GroupName")?,
                InlinePolicyInput {
                    document: params.required("PolicyDocument")?.to_owned(),
                    policy_name: params.required("PolicyName")?.to_owned(),
                },
            )
            .map_err(|error| error.to_aws_error())?;
            response_without_result(action)
        }
        "GetGroupPolicy" => {
            let principal = params.required("GroupName")?;
            let policy_name = params.required("PolicyName")?;
            let policy = iam
                .get_group_policy(&scope, principal, policy_name)
                .map_err(|error| error.to_aws_error())?;
            response_with_result(
                action,
                &inline_policy_xml("GroupName", principal, &policy),
            )
        }
        "DeleteGroupPolicy" => {
            iam.delete_group_policy(
                &scope,
                params.required("GroupName")?,
                params.required("PolicyName")?,
            )
            .map_err(|error| error.to_aws_error())?;
            response_without_result(action)
        }
        "ListGroupPolicies" => {
            let names = iam
                .list_group_policies(&scope, params.required("GroupName")?)
                .map_err(|error| error.to_aws_error())?;
            response_with_result(action, &inline_policy_names_xml(&names))
        }
        "PutRolePolicy" => {
            iam.put_role_policy(
                &scope,
                params.required("RoleName")?,
                InlinePolicyInput {
                    document: params.required("PolicyDocument")?.to_owned(),
                    policy_name: params.required("PolicyName")?.to_owned(),
                },
            )
            .map_err(|error| error.to_aws_error())?;
            response_without_result(action)
        }
        "GetRolePolicy" => {
            let principal = params.required("RoleName")?;
            let policy_name = params.required("PolicyName")?;
            let policy = iam
                .get_role_policy(&scope, principal, policy_name)
                .map_err(|error| error.to_aws_error())?;
            response_with_result(
                action,
                &inline_policy_xml("RoleName", principal, &policy),
            )
        }
        "DeleteRolePolicy" => {
            iam.delete_role_policy(
                &scope,
                params.required("RoleName")?,
                params.required("PolicyName")?,
            )
            .map_err(|error| error.to_aws_error())?;
            response_without_result(action)
        }
        "ListRolePolicies" => {
            let names = iam
                .list_role_policies(&scope, params.required("RoleName")?)
                .map_err(|error| error.to_aws_error())?;
            response_with_result(action, &inline_policy_names_xml(&names))
        }
        _ => return Err(invalid_action_error(action)),
    };

    Ok(body)
}

fn response_with_result(action: &str, result: &str) -> String {
    let response_name = format!("{action}Response");
    let result_name = format!("{action}Result");

    XmlBuilder::new()
        .start(&response_name, Some(IAM_XMLNS))
        .start(&result_name, None)
        .raw(result)
        .end(&result_name)
        .raw(&response_metadata_xml())
        .end(&response_name)
        .build()
}

fn response_without_result(action: &str) -> String {
    let response_name = format!("{action}Response");

    XmlBuilder::new()
        .start(&response_name, Some(IAM_XMLNS))
        .raw(&response_metadata_xml())
        .end(&response_name)
        .build()
}

fn response_metadata_xml() -> String {
    XmlBuilder::new()
        .start("ResponseMetadata", None)
        .elem("RequestId", REQUEST_ID)
        .end("ResponseMetadata")
        .build()
}

fn user_xml(user: &IamUser) -> String {
    XmlBuilder::new()
        .start("User", None)
        .raw(&user_fields_xml(user))
        .end("User")
        .build()
}

fn group_xml(group: &IamGroup) -> String {
    XmlBuilder::new()
        .start("Group", None)
        .raw(&group_fields_xml(group))
        .end("Group")
        .build()
}

fn role_xml(role: &IamRole) -> String {
    XmlBuilder::new()
        .start("Role", None)
        .raw(&role_fields_xml(role))
        .end("Role")
        .build()
}

fn access_key_xml(access_key: &IamAccessKey) -> String {
    XmlBuilder::new()
        .start("AccessKey", None)
        .raw(&access_key_fields_xml(access_key))
        .end("AccessKey")
        .build()
}

fn instance_profile_xml(profile: &IamInstanceProfile) -> String {
    XmlBuilder::new()
        .start("InstanceProfile", None)
        .raw(&instance_profile_fields_xml(profile))
        .end("InstanceProfile")
        .build()
}

fn policy_xml(policy: &IamPolicy) -> String {
    XmlBuilder::new()
        .start("Policy", None)
        .raw(&policy_fields_xml(policy))
        .end("Policy")
        .build()
}

fn policy_version_xml(
    element_name: &str,
    version: &IamPolicyVersion,
) -> String {
    XmlBuilder::new()
        .start(element_name, None)
        .elem("Document", &version.document)
        .elem("VersionId", &version.version_id)
        .elem(
            "IsDefaultVersion",
            if version.is_default_version { "true" } else { "false" },
        )
        .elem("CreateDate", &version.create_date)
        .end(element_name)
        .build()
}

fn list_users_xml(users: &[IamUser]) -> String {
    let mut xml = XmlBuilder::new().start("Users", None);
    for user in users {
        xml = xml
            .start("member", None)
            .raw(&user_fields_xml(user))
            .end("member");
    }
    xml.end("Users").elem("IsTruncated", "false").build()
}

fn list_groups_xml(groups: &[IamGroup]) -> String {
    let mut xml = XmlBuilder::new().start("Groups", None);
    for group in groups {
        xml = xml
            .start("member", None)
            .raw(&group_fields_xml(group))
            .end("member");
    }
    xml.end("Groups").elem("IsTruncated", "false").build()
}

fn list_access_keys_xml(access_keys: &[IamAccessKeyMetadata]) -> String {
    let mut xml = XmlBuilder::new().start("AccessKeyMetadata", None);
    for access_key in access_keys {
        xml = xml
            .start("member", None)
            .raw(&access_key_metadata_fields_xml(access_key))
            .end("member");
    }
    xml.end("AccessKeyMetadata").elem("IsTruncated", "false").build()
}

fn group_details_xml(details: &GroupDetails) -> String {
    let mut xml =
        XmlBuilder::new().raw(&group_xml(&details.group)).start("Users", None);
    for user in &details.users {
        xml = xml
            .start("member", None)
            .raw(&user_fields_xml(user))
            .end("member");
    }
    xml.end("Users").elem("IsTruncated", "false").build()
}

fn list_roles_xml(roles: &[IamRole]) -> String {
    let mut xml = XmlBuilder::new().start("Roles", None);
    for role in roles {
        xml = xml
            .start("member", None)
            .raw(&role_fields_xml(role))
            .end("member");
    }
    xml.end("Roles").elem("IsTruncated", "false").build()
}

fn list_instance_profiles_xml(profiles: &[IamInstanceProfile]) -> String {
    let mut xml = XmlBuilder::new().start("InstanceProfiles", None);
    for profile in profiles {
        xml = xml
            .start("member", None)
            .raw(&instance_profile_fields_xml(profile))
            .end("member");
    }
    xml.end("InstanceProfiles").elem("IsTruncated", "false").build()
}

fn list_policies_xml(policies: &[IamPolicy]) -> String {
    let mut xml = XmlBuilder::new().start("Policies", None);
    for policy in policies {
        xml = xml
            .start("member", None)
            .raw(&policy_fields_xml(policy))
            .end("member");
    }
    xml.end("Policies").elem("IsTruncated", "false").build()
}

fn list_policy_versions_xml(versions: &[IamPolicyVersion]) -> String {
    let mut xml = XmlBuilder::new().start("Versions", None);
    for version in versions {
        xml = xml
            .start("member", None)
            .raw(&policy_version_fields_xml(version))
            .end("member");
    }
    xml.end("Versions").elem("IsTruncated", "false").build()
}

fn tags_result_xml(tags: &[IamTag]) -> String {
    let mut xml = XmlBuilder::new().start("Tags", None);
    for tag in tags {
        xml = xml
            .start("member", None)
            .elem("Key", &tag.key)
            .elem("Value", &tag.value)
            .end("member");
    }
    xml.end("Tags").elem("IsTruncated", "false").build()
}

fn attached_policies_xml(policies: &[AttachedPolicy]) -> String {
    let mut xml = XmlBuilder::new().start("AttachedPolicies", None);
    for policy in policies {
        let policy_arn = policy.policy_arn.to_string();
        xml = xml
            .start("member", None)
            .elem("PolicyName", &policy.policy_name)
            .elem("PolicyArn", &policy_arn)
            .end("member");
    }
    xml.end("AttachedPolicies").elem("IsTruncated", "false").build()
}

fn inline_policy_xml(
    principal_element: &str,
    principal_name: &str,
    policy: &services::InlinePolicy,
) -> String {
    XmlBuilder::new()
        .elem(principal_element, principal_name)
        .elem("PolicyName", &policy.policy_name)
        .elem("PolicyDocument", &policy.document)
        .build()
}

fn inline_policy_names_xml(names: &[String]) -> String {
    let mut xml = XmlBuilder::new().start("PolicyNames", None);
    for name in names {
        xml = xml.elem("member", name);
    }
    xml.end("PolicyNames").elem("IsTruncated", "false").build()
}

fn user_fields_xml(user: &IamUser) -> String {
    let arn = user.arn.to_string();
    XmlBuilder::new()
        .elem("Path", &user.path)
        .elem("UserName", &user.user_name)
        .elem("UserId", &user.user_id)
        .elem("Arn", &arn)
        .elem("CreateDate", &user.create_date)
        .build()
}

fn group_fields_xml(group: &IamGroup) -> String {
    let arn = group.arn.to_string();
    XmlBuilder::new()
        .elem("Path", &group.path)
        .elem("GroupName", &group.group_name)
        .elem("GroupId", &group.group_id)
        .elem("Arn", &arn)
        .elem("CreateDate", &group.create_date)
        .build()
}

fn role_fields_xml(role: &IamRole) -> String {
    let arn = role.arn.to_string();
    XmlBuilder::new()
        .elem("Path", &role.path)
        .elem("RoleName", &role.role_name)
        .elem("RoleId", &role.role_id)
        .elem("Arn", &arn)
        .elem("CreateDate", &role.create_date)
        .elem("AssumeRolePolicyDocument", &role.assume_role_policy_document)
        .elem("Description", &role.description)
        .elem("MaxSessionDuration", &role.max_session_duration.to_string())
        .build()
}

fn access_key_fields_xml(access_key: &IamAccessKey) -> String {
    XmlBuilder::new()
        .elem("UserName", &access_key.user_name)
        .elem("AccessKeyId", &access_key.access_key_id)
        .elem("Status", access_key.status.as_str())
        .elem("SecretAccessKey", &access_key.secret_access_key)
        .elem("CreateDate", &access_key.create_date)
        .build()
}

fn access_key_metadata_fields_xml(
    access_key: &IamAccessKeyMetadata,
) -> String {
    XmlBuilder::new()
        .elem("UserName", &access_key.user_name)
        .elem("AccessKeyId", &access_key.access_key_id)
        .elem("Status", access_key.status.as_str())
        .elem("CreateDate", &access_key.create_date)
        .build()
}

fn instance_profile_fields_xml(profile: &IamInstanceProfile) -> String {
    let arn = profile.arn.to_string();
    let mut xml = XmlBuilder::new()
        .elem("Path", &profile.path)
        .elem("InstanceProfileName", &profile.instance_profile_name)
        .elem("InstanceProfileId", &profile.instance_profile_id)
        .elem("Arn", &arn)
        .elem("CreateDate", &profile.create_date)
        .start("Roles", None);
    for role in &profile.roles {
        xml = xml
            .start("member", None)
            .raw(&role_fields_xml(role))
            .end("member");
    }
    xml = xml.end("Roles").start("Tags", None);
    for tag in &profile.tags {
        xml = xml
            .start("member", None)
            .elem("Key", &tag.key)
            .elem("Value", &tag.value)
            .end("member");
    }
    xml.end("Tags").build()
}

fn policy_fields_xml(policy: &IamPolicy) -> String {
    let arn = policy.arn.to_string();
    XmlBuilder::new()
        .elem("PolicyName", &policy.policy_name)
        .elem("PolicyId", &policy.policy_id)
        .elem("Arn", &arn)
        .elem("Path", &policy.path)
        .elem("DefaultVersionId", &policy.default_version_id)
        .elem("AttachmentCount", &policy.attachment_count.to_string())
        .elem("IsAttachable", "true")
        .elem("CreateDate", &policy.create_date)
        .elem("UpdateDate", &policy.update_date)
        .build()
}

fn policy_version_fields_xml(version: &IamPolicyVersion) -> String {
    XmlBuilder::new()
        .elem("Document", &version.document)
        .elem("VersionId", &version.version_id)
        .elem(
            "IsDefaultVersion",
            if version.is_default_version { "true" } else { "false" },
        )
        .elem("CreateDate", &version.create_date)
        .build()
}

fn parse_access_key_status(
    value: &str,
) -> Result<IamAccessKeyStatus, AwsError> {
    match value {
        "Active" => Ok(IamAccessKeyStatus::Active),
        "Inactive" => Ok(IamAccessKeyStatus::Inactive),
        "Expired" => Ok(IamAccessKeyStatus::Expired),
        _ => Err(IamError::InvalidInput {
            message: "Status must be one of Active, Inactive, or Expired."
                .to_owned(),
        }
        .to_aws_error()),
    }
}

fn parse_arn(params: &QueryParameters, name: &str) -> Result<Arn, AwsError> {
    let value = params.required(name)?;
    value.parse::<Arn>().map_err(|_| {
        IamError::InvalidInput {
            message: format!("Parameter {name} must be a valid ARN."),
        }
        .to_aws_error()
    })
}

fn invalid_action_error(action: &str) -> AwsError {
    AwsError::trusted_custom(
        AwsErrorFamily::Validation,
        "InvalidAction",
        format!("Unknown action {action}."),
        400,
        true,
    )
}

impl QueryParameters {
    fn parse_u32(&self, name: &str) -> Result<Option<u32>, AwsError> {
        match self.optional(name) {
            Some(value) => value.parse::<u32>().map(Some).map_err(|_| {
                IamError::InvalidInput {
                    message: format!(
                        "Parameter {name} must be an unsigned integer."
                    ),
                }
                .to_aws_error()
            }),
            None => Ok(None),
        }
    }

    fn tags(&self) -> Result<Vec<IamTag>, AwsError> {
        let mut tags = Vec::new();

        for index in 1.. {
            let key_name = format!("Tags.member.{index}.Key");
            let Some(key) = self.optional(&key_name) else {
                break;
            };
            let value_name = format!("Tags.member.{index}.Value");
            tags.push(IamTag {
                key: key.to_owned(),
                value: self
                    .optional(&value_name)
                    .unwrap_or_default()
                    .to_owned(),
            });
        }

        Ok(tags)
    }

    fn tag_keys(&self) -> Vec<String> {
        let mut keys = Vec::new();

        for index in 1.. {
            let key_name = format!("TagKeys.member.{index}");
            let Some(key) = self.optional(&key_name) else {
                break;
            };
            keys.push(key.to_owned());
        }

        keys
    }
}

#[cfg(test)]
mod tests {
    use super::handle;
    use aws::{ProtocolFamily, RequestContext, ServiceName};
    use services::IamService;

    fn context(action: &str) -> RequestContext {
        RequestContext::try_new(
            "000000000000".parse().expect("account should parse"),
            "eu-west-2".parse().expect("region should parse"),
            ServiceName::Iam,
            ProtocolFamily::Query,
            action,
            None,
            true,
        )
        .expect("context should build")
    }

    fn call(iam: &IamService, action: &str, body: &str) -> String {
        handle(iam, body.as_bytes(), &context(action))
            .expect("IAM query action should succeed")
    }

    #[test]
    fn iam_query_create_and_fetch_user_namespace_round_trips() {
        let iam = IamService::new();
        let create = handle(
            &iam,
            b"Action=CreateUser&UserName=alice&Path=%2Fteam%2F&Tags.member.1.Key=env&Tags.member.1.Value=dev",
            &context("CreateUser"),
        )
        .expect("create should succeed");
        let get = handle(
            &iam,
            b"Action=GetUser&UserName=alice",
            &context("GetUser"),
        )
        .expect("get should succeed");

        assert!(create.contains("<CreateUserResponse xmlns=\"https://iam.amazonaws.com/doc/2010-05-08/\">"));
        assert!(create.contains("<UserName>alice</UserName>"));
        assert!(create.contains("<Path>/team/</Path>"));
        assert!(create.contains("<RequestId>0000000000000000</RequestId>"));
        assert!(get.contains("<GetUserResponse"));
        assert!(
            get.contains(
                "<Arn>arn:aws:iam::000000000000:user/team/alice</Arn>"
            )
        );
    }

    #[test]
    fn iam_query_reports_delete_conflict_for_attached_user() {
        let iam = IamService::new();
        handle(
            &iam,
            b"Action=CreateUser&UserName=alice",
            &context("CreateUser"),
        )
        .expect("user should create");
        handle(
            &iam,
            b"Action=CreatePolicy&PolicyName=managed&PolicyDocument=%7B%22Version%22%3A%222012-10-17%22%2C%22Statement%22%3A%5B%7B%22Effect%22%3A%22Allow%22%2C%22Action%22%3A%22s3%3AGetObject%22%2C%22Resource%22%3A%22*%22%7D%5D%7D",
            &context("CreatePolicy"),
        )
        .expect("policy should create");
        handle(
            &iam,
            b"Action=AttachUserPolicy&UserName=alice&PolicyArn=arn%3Aaws%3Aiam%3A%3A000000000000%3Apolicy%2Fmanaged",
            &context("AttachUserPolicy"),
        )
        .expect("attachment should succeed");

        let error = handle(
            &iam,
            b"Action=DeleteUser&UserName=alice",
            &context("DeleteUser"),
        )
        .expect_err("delete should fail");

        assert_eq!(error.code(), "DeleteConflict");
        assert_eq!(error.status_code(), 409);
    }

    #[test]
    fn iam_query_rejects_invalid_policy_arn_at_the_adapter_boundary() {
        let iam = IamService::new();
        let error = handle(
            &iam,
            b"Action=GetPolicy&PolicyArn=not-an-arn",
            &context("GetPolicy"),
        )
        .expect_err("invalid policy ARN should fail before reaching IAM");

        assert_eq!(error.code(), "InvalidInput");
        assert_eq!(
            error.message(),
            "Parameter PolicyArn must be a valid ARN."
        );
    }

    #[test]
    fn iam_query_access_key_and_instance_profile_lifecycle_round_trips() {
        let iam = IamService::new();
        let trust_policy = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]}"#;

        call(&iam, "CreateUser", "Action=CreateUser&UserName=alice");
        let create_access_key = call(
            &iam,
            "CreateAccessKey",
            "Action=CreateAccessKey&UserName=alice",
        );
        assert!(create_access_key.contains("<AccessKeyId>AKIA"));
        assert!(create_access_key.contains("<Status>Active</Status>"));

        assert!(
            call(
                &iam,
                "ListAccessKeys",
                "Action=ListAccessKeys&UserName=alice"
            )
            .contains("<AccessKeyMetadata><member><UserName>alice</UserName>")
        );
        assert!(call(
            &iam,
            "UpdateAccessKey",
            "Action=UpdateAccessKey&UserName=alice&AccessKeyId=AKIA0000000000000001&Status=Inactive",
        )
        .contains("<UpdateAccessKeyResponse"));

        call(
            &iam,
            "CreateRole",
            &format!(
                "Action=CreateRole&RoleName=app-role&AssumeRolePolicyDocument={trust_policy}"
            ),
        );
        let create_profile = call(
            &iam,
            "CreateInstanceProfile",
            "Action=CreateInstanceProfile&InstanceProfileName=app-profile&Path=/team/&Tags.member.1.Key=env&Tags.member.1.Value=dev",
        );
        assert!(create_profile.contains(
            "<InstanceProfileName>app-profile</InstanceProfileName>"
        ));
        assert!(create_profile.contains("<InstanceProfileId>AIPA"));
        call(
            &iam,
            "AddRoleToInstanceProfile",
            "Action=AddRoleToInstanceProfile&InstanceProfileName=app-profile&RoleName=app-role",
        );
        call(
            &iam,
            "TagInstanceProfile",
            "Action=TagInstanceProfile&InstanceProfileName=app-profile&Tags.member.1.Key=owner&Tags.member.1.Value=platform",
        );
        call(
            &iam,
            "UntagInstanceProfile",
            "Action=UntagInstanceProfile&InstanceProfileName=app-profile&TagKeys.member.1=env",
        );

        let get_profile = call(
            &iam,
            "GetInstanceProfile",
            "Action=GetInstanceProfile&InstanceProfileName=app-profile",
        );
        assert!(get_profile.contains("<RoleName>app-role</RoleName>"));
        assert!(
            get_profile.contains("<Key>owner</Key><Value>platform</Value>")
        );
        assert!(
            call(
                &iam,
                "ListInstanceProfilesForRole",
                "Action=ListInstanceProfilesForRole&RoleName=app-role"
            )
            .contains(
                "<InstanceProfileName>app-profile</InstanceProfileName>"
            )
        );
        assert!(
            call(
                &iam,
                "ListInstanceProfileTags",
                "Action=ListInstanceProfileTags&InstanceProfileName=app-profile"
            )
            .contains("<Key>owner</Key><Value>platform</Value>")
        );
    }

    #[test]
    fn iam_query_access_key_and_instance_profile_limit_errors_are_aws_shaped()
    {
        let iam = IamService::new();
        let trust_policy = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]}"#;

        call(&iam, "CreateUser", "Action=CreateUser&UserName=alice");
        call(&iam, "CreateAccessKey", "Action=CreateAccessKey&UserName=alice");
        call(&iam, "CreateAccessKey", "Action=CreateAccessKey&UserName=alice");
        let access_key_limit = handle(
            &iam,
            b"Action=CreateAccessKey&UserName=alice",
            &context("CreateAccessKey"),
        )
        .expect_err("third access key should fail");
        assert_eq!(access_key_limit.code(), "LimitExceeded");
        assert_eq!(access_key_limit.status_code(), 409);

        call(
            &iam,
            "CreateRole",
            &format!(
                "Action=CreateRole&RoleName=role-one&AssumeRolePolicyDocument={trust_policy}"
            ),
        );
        call(
            &iam,
            "CreateRole",
            &format!(
                "Action=CreateRole&RoleName=role-two&AssumeRolePolicyDocument={trust_policy}"
            ),
        );
        call(
            &iam,
            "CreateInstanceProfile",
            "Action=CreateInstanceProfile&InstanceProfileName=profile-a",
        );
        call(
            &iam,
            "AddRoleToInstanceProfile",
            "Action=AddRoleToInstanceProfile&InstanceProfileName=profile-a&RoleName=role-one",
        );

        let duplicate_role = handle(
            &iam,
            b"Action=AddRoleToInstanceProfile&InstanceProfileName=profile-a&RoleName=role-one",
            &context("AddRoleToInstanceProfile"),
        )
        .expect_err("duplicate role attachment should fail");
        assert_eq!(duplicate_role.code(), "EntityAlreadyExists");

        let role_limit = handle(
            &iam,
            b"Action=AddRoleToInstanceProfile&InstanceProfileName=profile-a&RoleName=role-two",
            &context("AddRoleToInstanceProfile"),
        )
        .expect_err("second distinct role should fail");
        assert_eq!(role_limit.code(), "LimitExceeded");
        assert_eq!(role_limit.status_code(), 409);
    }

    fn create_team_user(iam: &IamService) {
        assert!(call(
            iam,
            "CreateUser",
            "Action=CreateUser&UserName=alice&Path=/team/&Tags.member.1.Key=env&Tags.member.1.Value=dev",
        )
        .contains("<UserName>alice</UserName>"));
    }

    fn create_team_group(iam: &IamService) {
        assert!(
            call(
                iam,
                "CreateGroup",
                "Action=CreateGroup&GroupName=admins&Path=/team/",
            )
            .contains("<GroupName>admins</GroupName>")
        );
    }

    fn create_team_role(iam: &IamService, trust_policy: &str) {
        assert!(call(
            iam,
            "CreateRole",
            &format!(
                "Action=CreateRole&RoleName=app-role&Path=/team/&AssumeRolePolicyDocument={trust_policy}&Description=application&MaxSessionDuration=3600&Tags.member.1.Key=tier&Tags.member.1.Value=backend"
            ),
        )
        .contains("<RoleName>app-role</RoleName>"));
    }

    fn create_team_managed_policy(iam: &IamService, managed_policy: &str) {
        assert!(call(
            iam,
            "CreatePolicy",
            &format!(
                "Action=CreatePolicy&PolicyName=managed&Path=/team/&PolicyDocument={managed_policy}&Description=managed&Tags.member.1.Key=env&Tags.member.1.Value=dev"
            ),
        )
        .contains("<PolicyName>managed</PolicyName>"));
    }

    #[test]
    fn iam_query_user_and_group_lifecycle_round_trip() {
        let iam = IamService::new();
        create_team_user(&iam);
        assert!(call(&iam, "ListUsers", "Action=ListUsers").contains(
            "<Users><member><Path>/team/</Path><UserName>alice</UserName>"
        ));
        assert!(
            call(&iam, "GetUser", "Action=GetUser&UserName=alice").contains(
                "<Arn>arn:aws:iam::000000000000:user/team/alice</Arn>"
            )
        );
        assert!(
            call(&iam, "ListUserTags", "Action=ListUserTags&UserName=alice")
                .contains("<Key>env</Key><Value>dev</Value>")
        );
        call(
            &iam,
            "TagUser",
            "Action=TagUser&UserName=alice&Tags.member.1.Key=owner&Tags.member.1.Value=platform",
        );
        assert!(
            call(
                &iam,
                "UntagUser",
                "Action=UntagUser&UserName=alice&TagKeys.member.1=env"
            )
            .contains("<UntagUserResponse")
        );

        create_team_group(&iam);
        assert!(call(&iam, "ListGroups", "Action=ListGroups").contains(
            "<Groups><member><Path>/team/</Path><GroupName>admins</GroupName>"
        ));
        call(
            &iam,
            "AddUserToGroup",
            "Action=AddUserToGroup&GroupName=admins&UserName=alice",
        );
        assert!(call(
            &iam,
            "GetGroup",
            "Action=GetGroup&GroupName=admins"
        )
        .contains("<Users><member><Path>/team/</Path><UserName>alice</UserName>"));
        assert!(
            call(
                &iam,
                "ListGroupsForUser",
                "Action=ListGroupsForUser&UserName=alice"
            )
            .contains("<GroupName>admins</GroupName>")
        );

        call(
            &iam,
            "RemoveUserFromGroup",
            "Action=RemoveUserFromGroup&GroupName=admins&UserName=alice",
        );
        call(&iam, "DeleteGroup", "Action=DeleteGroup&GroupName=admins");
        call(&iam, "DeleteUser", "Action=DeleteUser&UserName=alice");
    }

    #[test]
    fn iam_query_role_and_managed_policy_version_lifecycle_round_trip() {
        let iam = IamService::new();
        let trust_policy = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}"#;
        let managed_policy = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]}"#;
        let managed_policy_v2 = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:PutObject","Resource":"*"}]}"#;

        create_team_role(&iam, trust_policy);
        assert!(call(&iam, "ListRoles", "Action=ListRoles").contains(
            "<Roles><member><Path>/team/</Path><RoleName>app-role</RoleName>"
        ));
        assert!(
            call(&iam, "GetRole", "Action=GetRole&RoleName=app-role")
                .contains("<Description>application</Description>")
        );
        assert!(
            call(
                &iam,
                "ListRoleTags",
                "Action=ListRoleTags&RoleName=app-role"
            )
            .contains("<Key>tier</Key><Value>backend</Value>")
        );
        call(
            &iam,
            "TagRole",
            "Action=TagRole&RoleName=app-role&Tags.member.1.Key=owner&Tags.member.1.Value=security",
        );
        call(
            &iam,
            "UntagRole",
            "Action=UntagRole&RoleName=app-role&TagKeys.member.1=tier",
        );

        create_team_managed_policy(&iam, managed_policy);
        assert!(call(
            &iam,
            "GetPolicy",
            "Action=GetPolicy&PolicyArn=arn:aws:iam::000000000000:policy/team/managed"
        )
        .contains("<DefaultVersionId>v1</DefaultVersionId>"));
        assert!(
            call(
                &iam,
                "ListPolicies",
                "Action=ListPolicies&Scope=Local&PathPrefix=/team/"
            )
            .contains("<PolicyName>managed</PolicyName>")
        );
        assert!(call(
            &iam,
            "ListPolicyTags",
            "Action=ListPolicyTags&PolicyArn=arn:aws:iam::000000000000:policy/team/managed"
        )
        .contains("<Key>env</Key><Value>dev</Value>"));
        call(
            &iam,
            "TagPolicy",
            "Action=TagPolicy&PolicyArn=arn:aws:iam::000000000000:policy/team/managed&Tags.member.1.Key=owner&Tags.member.1.Value=security",
        );
        call(
            &iam,
            "UntagPolicy",
            "Action=UntagPolicy&PolicyArn=arn:aws:iam::000000000000:policy/team/managed&TagKeys.member.1=env",
        );
        assert!(call(
            &iam,
            "CreatePolicyVersion",
            &format!(
                "Action=CreatePolicyVersion&PolicyArn=arn:aws:iam::000000000000:policy/team/managed&PolicyDocument={managed_policy_v2}&SetAsDefault=true"
            ),
        )
        .contains("<VersionId>v2</VersionId>"));
        assert!(call(
            &iam,
            "GetPolicyVersion",
            "Action=GetPolicyVersion&PolicyArn=arn:aws:iam::000000000000:policy/team/managed&VersionId=v2"
        )
        .contains("<IsDefaultVersion>true</IsDefaultVersion>"));
        assert!(call(
            &iam,
            "ListPolicyVersions",
            "Action=ListPolicyVersions&PolicyArn=arn:aws:iam::000000000000:policy/team/managed"
        )
        .contains("<VersionId>v1</VersionId>"));
        call(
            &iam,
            "SetDefaultPolicyVersion",
            "Action=SetDefaultPolicyVersion&PolicyArn=arn:aws:iam::000000000000:policy/team/managed&VersionId=v1",
        );
        call(
            &iam,
            "DeletePolicyVersion",
            "Action=DeletePolicyVersion&PolicyArn=arn:aws:iam::000000000000:policy/team/managed&VersionId=v2",
        );
        call(&iam, "DeleteRole", "Action=DeleteRole&RoleName=app-role");
        call(
            &iam,
            "DeletePolicy",
            "Action=DeletePolicy&PolicyArn=arn:aws:iam::000000000000:policy/team/managed",
        );
    }

    #[test]
    fn iam_query_managed_policy_attachments_round_trip() {
        let iam = IamService::new();
        let trust_policy = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}"#;
        let managed_policy = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]}"#;

        create_team_user(&iam);
        create_team_group(&iam);
        create_team_role(&iam, trust_policy);
        create_team_managed_policy(&iam, managed_policy);

        call(
            &iam,
            "AttachUserPolicy",
            "Action=AttachUserPolicy&UserName=alice&PolicyArn=arn:aws:iam::000000000000:policy/team/managed",
        );
        call(
            &iam,
            "AttachGroupPolicy",
            "Action=AttachGroupPolicy&GroupName=admins&PolicyArn=arn:aws:iam::000000000000:policy/team/managed",
        );
        call(
            &iam,
            "AttachRolePolicy",
            "Action=AttachRolePolicy&RoleName=app-role&PolicyArn=arn:aws:iam::000000000000:policy/team/managed",
        );
        assert!(call(
            &iam,
            "ListAttachedUserPolicies",
            "Action=ListAttachedUserPolicies&UserName=alice&PathPrefix=/team/"
        )
        .contains("<PolicyArn>arn:aws:iam::000000000000:policy/team/managed</PolicyArn>"));
        assert!(call(
            &iam,
            "ListAttachedGroupPolicies",
            "Action=ListAttachedGroupPolicies&GroupName=admins&PathPrefix=/team/"
        )
        .contains("<PolicyName>managed</PolicyName>"));
        assert!(call(
            &iam,
            "ListAttachedRolePolicies",
            "Action=ListAttachedRolePolicies&RoleName=app-role&PathPrefix=/team/"
        )
        .contains("<PolicyName>managed</PolicyName>"));
        call(
            &iam,
            "DetachUserPolicy",
            "Action=DetachUserPolicy&UserName=alice&PolicyArn=arn:aws:iam::000000000000:policy/team/managed",
        );
        call(
            &iam,
            "DetachGroupPolicy",
            "Action=DetachGroupPolicy&GroupName=admins&PolicyArn=arn:aws:iam::000000000000:policy/team/managed",
        );
        call(
            &iam,
            "DetachRolePolicy",
            "Action=DetachRolePolicy&RoleName=app-role&PolicyArn=arn:aws:iam::000000000000:policy/team/managed",
        );
        assert!(
            !call(
                &iam,
                "ListAttachedUserPolicies",
                "Action=ListAttachedUserPolicies&UserName=alice&PathPrefix=/team/"
            )
            .contains("policy/team/managed")
        );
        assert!(
            !call(
                &iam,
                "ListAttachedGroupPolicies",
                "Action=ListAttachedGroupPolicies&GroupName=admins&PathPrefix=/team/"
            )
            .contains("<PolicyName>managed</PolicyName>")
        );
        assert!(
            !call(
                &iam,
                "ListAttachedRolePolicies",
                "Action=ListAttachedRolePolicies&RoleName=app-role&PathPrefix=/team/"
            )
            .contains("<PolicyName>managed</PolicyName>")
        );
    }

    #[test]
    fn iam_query_inline_principal_policies_round_trip() {
        let iam = IamService::new();
        let trust_policy = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}"#;
        let managed_policy = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]}"#;

        create_team_user(&iam);
        create_team_group(&iam);
        create_team_role(&iam, trust_policy);

        call(
            &iam,
            "PutUserPolicy",
            &format!(
                "Action=PutUserPolicy&UserName=alice&PolicyName=user-inline&PolicyDocument={managed_policy}"
            ),
        );
        call(
            &iam,
            "PutGroupPolicy",
            &format!(
                "Action=PutGroupPolicy&GroupName=admins&PolicyName=group-inline&PolicyDocument={managed_policy}"
            ),
        );
        call(
            &iam,
            "PutRolePolicy",
            &format!(
                "Action=PutRolePolicy&RoleName=app-role&PolicyName=role-inline&PolicyDocument={managed_policy}"
            ),
        );
        assert!(
            call(
                &iam,
                "GetUserPolicy",
                "Action=GetUserPolicy&UserName=alice&PolicyName=user-inline"
            )
            .contains("<PolicyName>user-inline</PolicyName>")
        );
        assert!(
            call(
                &iam,
                "GetGroupPolicy",
                "Action=GetGroupPolicy&GroupName=admins&PolicyName=group-inline"
            )
            .contains("<PolicyName>group-inline</PolicyName>")
        );
        assert!(
            call(
                &iam,
                "GetRolePolicy",
                "Action=GetRolePolicy&RoleName=app-role&PolicyName=role-inline"
            )
            .contains("<PolicyName>role-inline</PolicyName>")
        );
        assert!(
            call(
                &iam,
                "ListUserPolicies",
                "Action=ListUserPolicies&UserName=alice"
            )
            .contains("<member>user-inline</member>")
        );
        assert!(
            call(
                &iam,
                "ListGroupPolicies",
                "Action=ListGroupPolicies&GroupName=admins"
            )
            .contains("<member>group-inline</member>")
        );
        assert!(
            call(
                &iam,
                "ListRolePolicies",
                "Action=ListRolePolicies&RoleName=app-role"
            )
            .contains("<member>role-inline</member>")
        );
        call(
            &iam,
            "DeleteUserPolicy",
            "Action=DeleteUserPolicy&UserName=alice&PolicyName=user-inline",
        );
        call(
            &iam,
            "DeleteGroupPolicy",
            "Action=DeleteGroupPolicy&GroupName=admins&PolicyName=group-inline",
        );
        call(
            &iam,
            "DeleteRolePolicy",
            "Action=DeleteRolePolicy&RoleName=app-role&PolicyName=role-inline",
        );
        assert!(
            !call(
                &iam,
                "ListUserPolicies",
                "Action=ListUserPolicies&UserName=alice"
            )
            .contains("<member>user-inline</member>")
        );
        assert!(
            !call(
                &iam,
                "ListGroupPolicies",
                "Action=ListGroupPolicies&GroupName=admins"
            )
            .contains("<member>group-inline</member>")
        );
        assert!(
            !call(
                &iam,
                "ListRolePolicies",
                "Action=ListRolePolicies&RoleName=app-role"
            )
            .contains("<member>role-inline</member>")
        );
    }
}
