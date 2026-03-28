pub(crate) use crate::aws_error_shape::AwsErrorShape;
use crate::request::HttpRequest;
use aws::{AwsError, AwsErrorFamily, RequestContext};
use cognito::{CognitoUserPoolClientId, CognitoUserPoolId};
use serde::Deserialize;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use services::{
    CognitoAdminCreateUserInput, CognitoAdminDeleteUserInput,
    CognitoAdminGetUserInput, CognitoAdminInitiateAuthInput,
    CognitoAdminSetUserPasswordInput, CognitoAdminUpdateUserAttributesInput,
    CognitoAttributeType, CognitoChangePasswordInput,
    CognitoConfirmSignUpInput, CognitoError, CognitoExplicitAuthFlow,
    CognitoGetUserInput, CognitoInitiateAuthInput, CognitoListUsersInput,
    CognitoRespondToAuthChallengeInput, CognitoScope, CognitoService,
    CognitoSignUpInput, CognitoUpdateUserAttributesInput,
    CreateCognitoUserPoolClientInput, CreateCognitoUserPoolInput,
    DeleteCognitoUserPoolClientInput, DeleteCognitoUserPoolInput,
    DescribeCognitoUserPoolClientInput, DescribeCognitoUserPoolInput,
    ListCognitoUserPoolClientsInput, ListCognitoUserPoolsInput,
    UpdateCognitoUserPoolClientInput, UpdateCognitoUserPoolInput,
};
use std::collections::BTreeMap;

pub(crate) fn handle_json(
    cognito: &CognitoService,
    request: &HttpRequest<'_>,
    context: &RequestContext,
) -> Result<Vec<u8>, AwsError> {
    let operation = operation_from_target(request.header("x-amz-target"))
        .map_err(|error| error.to_aws_error())?;
    let request_host = request.header("host").unwrap_or("localhost:4566");
    let scope = CognitoScope::new(
        context.account_id().clone(),
        context.region().clone(),
    );

    match operation {
        CognitoOperation::CreateUserPool => {
            let request =
                parse_json_body::<CreateUserPoolRequest>(request.body())?;
            reject_extra_fields("CreateUserPool", request.extra)?;
            json_response(
                &cognito
                    .create_user_pool(
                        &scope,
                        CreateCognitoUserPoolInput {
                            pool_name: request.pool_name,
                        },
                    )
                    .map_err(|error| error.to_aws_error())?,
            )
        }
        CognitoOperation::DescribeUserPool => {
            let request =
                parse_json_body::<DescribeUserPoolRequest>(request.body())?;
            reject_extra_fields("DescribeUserPool", request.extra)?;
            json_response(
                &cognito
                    .describe_user_pool(
                        &scope,
                        DescribeCognitoUserPoolInput {
                            user_pool_id: user_pool_id(request.user_pool_id)?,
                        },
                    )
                    .map_err(|error| error.to_aws_error())?,
            )
        }
        CognitoOperation::ListUserPools => {
            let request =
                parse_json_body::<ListUserPoolsRequest>(request.body())?;
            reject_extra_fields("ListUserPools", request.extra)?;
            json_response(
                &cognito
                    .list_user_pools(
                        &scope,
                        ListCognitoUserPoolsInput {
                            max_results: request.max_results,
                            next_token: request.next_token,
                        },
                    )
                    .map_err(|error| error.to_aws_error())?,
            )
        }
        CognitoOperation::UpdateUserPool => {
            let request =
                parse_json_body::<UpdateUserPoolRequest>(request.body())?;
            reject_extra_fields("UpdateUserPool", request.extra)?;
            json_response(
                &cognito
                    .update_user_pool(
                        &scope,
                        UpdateCognitoUserPoolInput {
                            pool_name: request.pool_name,
                            user_pool_id: user_pool_id(request.user_pool_id)?,
                        },
                    )
                    .map_err(|error| error.to_aws_error())?,
            )
        }
        CognitoOperation::DeleteUserPool => {
            let request =
                parse_json_body::<DeleteUserPoolRequest>(request.body())?;
            reject_extra_fields("DeleteUserPool", request.extra)?;
            json_response(
                &cognito
                    .delete_user_pool(
                        &scope,
                        DeleteCognitoUserPoolInput {
                            user_pool_id: user_pool_id(request.user_pool_id)?,
                        },
                    )
                    .map_err(|error| error.to_aws_error())?,
            )
        }
        CognitoOperation::CreateUserPoolClient => {
            let request = parse_json_body::<CreateUserPoolClientRequest>(
                request.body(),
            )?;
            reject_extra_fields("CreateUserPoolClient", request.extra)?;
            json_response(
                &cognito
                    .create_user_pool_client(
                        &scope,
                        CreateCognitoUserPoolClientInput {
                            client_name: request.client_name,
                            explicit_auth_flows: request.explicit_auth_flows,
                            generate_secret: request.generate_secret,
                            user_pool_id: user_pool_id(request.user_pool_id)?,
                        },
                    )
                    .map_err(|error| error.to_aws_error())?,
            )
        }
        CognitoOperation::DescribeUserPoolClient => {
            let request = parse_json_body::<DescribeUserPoolClientRequest>(
                request.body(),
            )?;
            reject_extra_fields("DescribeUserPoolClient", request.extra)?;
            json_response(
                &cognito
                    .describe_user_pool_client(
                        &scope,
                        DescribeCognitoUserPoolClientInput {
                            client_id: client_id(request.client_id)?,
                            user_pool_id: user_pool_id(request.user_pool_id)?,
                        },
                    )
                    .map_err(|error| error.to_aws_error())?,
            )
        }
        CognitoOperation::ListUserPoolClients => {
            let request =
                parse_json_body::<ListUserPoolClientsRequest>(request.body())?;
            reject_extra_fields("ListUserPoolClients", request.extra)?;
            json_response(
                &cognito
                    .list_user_pool_clients(
                        &scope,
                        ListCognitoUserPoolClientsInput {
                            max_results: request.max_results,
                            next_token: request.next_token,
                            user_pool_id: user_pool_id(request.user_pool_id)?,
                        },
                    )
                    .map_err(|error| error.to_aws_error())?,
            )
        }
        CognitoOperation::UpdateUserPoolClient => {
            let request = parse_json_body::<UpdateUserPoolClientRequest>(
                request.body(),
            )?;
            reject_extra_fields("UpdateUserPoolClient", request.extra)?;
            json_response(
                &cognito
                    .update_user_pool_client(
                        &scope,
                        UpdateCognitoUserPoolClientInput {
                            client_id: client_id(request.client_id)?,
                            client_name: request.client_name,
                            explicit_auth_flows: request.explicit_auth_flows,
                            user_pool_id: user_pool_id(request.user_pool_id)?,
                        },
                    )
                    .map_err(|error| error.to_aws_error())?,
            )
        }
        CognitoOperation::DeleteUserPoolClient => {
            let request = parse_json_body::<DeleteUserPoolClientRequest>(
                request.body(),
            )?;
            reject_extra_fields("DeleteUserPoolClient", request.extra)?;
            json_response(
                &cognito
                    .delete_user_pool_client(
                        &scope,
                        DeleteCognitoUserPoolClientInput {
                            client_id: client_id(request.client_id)?,
                            user_pool_id: user_pool_id(request.user_pool_id)?,
                        },
                    )
                    .map_err(|error| error.to_aws_error())?,
            )
        }
        CognitoOperation::AdminCreateUser => {
            let request =
                parse_json_body::<AdminCreateUserRequest>(request.body())?;
            reject_extra_fields("AdminCreateUser", request.extra)?;
            json_response(
                &cognito
                    .admin_create_user(
                        &scope,
                        CognitoAdminCreateUserInput {
                            temporary_password: request.temporary_password,
                            user_attributes: request
                                .user_attributes
                                .unwrap_or_default(),
                            user_pool_id: user_pool_id(request.user_pool_id)?,
                            username: request.username,
                        },
                    )
                    .map_err(|error| error.to_aws_error())?,
            )
        }
        CognitoOperation::AdminGetUser => {
            let request =
                parse_json_body::<AdminGetUserRequest>(request.body())?;
            reject_extra_fields("AdminGetUser", request.extra)?;
            json_response(
                &cognito
                    .admin_get_user(
                        &scope,
                        CognitoAdminGetUserInput {
                            user_pool_id: user_pool_id(request.user_pool_id)?,
                            username: request.username,
                        },
                    )
                    .map_err(|error| error.to_aws_error())?,
            )
        }
        CognitoOperation::ListUsers => {
            let request = parse_json_body::<ListUsersRequest>(request.body())?;
            reject_extra_fields("ListUsers", request.extra)?;
            json_response(
                &cognito
                    .list_users(
                        &scope,
                        CognitoListUsersInput {
                            limit: request.limit,
                            pagination_token: request.pagination_token,
                            user_pool_id: user_pool_id(request.user_pool_id)?,
                        },
                    )
                    .map_err(|error| error.to_aws_error())?,
            )
        }
        CognitoOperation::AdminUpdateUserAttributes => {
            let request = parse_json_body::<AdminUpdateUserAttributesRequest>(
                request.body(),
            )?;
            reject_extra_fields("AdminUpdateUserAttributes", request.extra)?;
            json_response(
                &cognito
                    .admin_update_user_attributes(
                        &scope,
                        CognitoAdminUpdateUserAttributesInput {
                            user_attributes: request
                                .user_attributes
                                .unwrap_or_default(),
                            user_pool_id: user_pool_id(request.user_pool_id)?,
                            username: request.username,
                        },
                    )
                    .map_err(|error| error.to_aws_error())?,
            )
        }
        CognitoOperation::AdminDeleteUser => {
            let request =
                parse_json_body::<AdminDeleteUserRequest>(request.body())?;
            reject_extra_fields("AdminDeleteUser", request.extra)?;
            json_response(
                &cognito
                    .admin_delete_user(
                        &scope,
                        CognitoAdminDeleteUserInput {
                            user_pool_id: user_pool_id(request.user_pool_id)?,
                            username: request.username,
                        },
                    )
                    .map_err(|error| error.to_aws_error())?,
            )
        }
        CognitoOperation::AdminSetUserPassword => {
            let request = parse_json_body::<AdminSetUserPasswordRequest>(
                request.body(),
            )?;
            reject_extra_fields("AdminSetUserPassword", request.extra)?;
            json_response(
                &cognito
                    .admin_set_user_password(
                        &scope,
                        CognitoAdminSetUserPasswordInput {
                            password: request.password,
                            permanent: request.permanent.unwrap_or(false),
                            user_pool_id: user_pool_id(request.user_pool_id)?,
                            username: request.username,
                        },
                    )
                    .map_err(|error| error.to_aws_error())?,
            )
        }
        CognitoOperation::SignUp => {
            let request = parse_json_body::<SignUpRequest>(request.body())?;
            reject_extra_fields("SignUp", request.extra)?;
            json_response(
                &cognito
                    .sign_up(
                        &scope,
                        CognitoSignUpInput {
                            client_id: client_id(request.client_id)?,
                            password: request.password,
                            user_attributes: request
                                .user_attributes
                                .unwrap_or_default(),
                            username: request.username,
                        },
                    )
                    .map_err(|error| error.to_aws_error())?,
            )
        }
        CognitoOperation::ConfirmSignUp => {
            let request =
                parse_json_body::<ConfirmSignUpRequest>(request.body())?;
            reject_extra_fields("ConfirmSignUp", request.extra)?;
            json_response(
                &cognito
                    .confirm_sign_up(
                        &scope,
                        CognitoConfirmSignUpInput {
                            client_id: client_id(request.client_id)?,
                            confirmation_code: request.confirmation_code,
                            username: request.username,
                        },
                    )
                    .map_err(|error| error.to_aws_error())?,
            )
        }
        CognitoOperation::InitiateAuth => {
            let request =
                parse_json_body::<InitiateAuthRequest>(request.body())?;
            reject_extra_fields("InitiateAuth", request.extra)?;
            json_response(
                &cognito
                    .initiate_auth(
                        &scope,
                        request_host,
                        CognitoInitiateAuthInput {
                            auth_flow: request.auth_flow,
                            auth_parameters: request
                                .auth_parameters
                                .unwrap_or_default(),
                            client_id: client_id(request.client_id)?,
                        },
                    )
                    .map_err(|error| error.to_aws_error())?,
            )
        }
        CognitoOperation::AdminInitiateAuth => {
            let request =
                parse_json_body::<AdminInitiateAuthRequest>(request.body())?;
            reject_extra_fields("AdminInitiateAuth", request.extra)?;
            json_response(
                &cognito
                    .admin_initiate_auth(
                        &scope,
                        request_host,
                        CognitoAdminInitiateAuthInput {
                            auth_flow: request.auth_flow,
                            auth_parameters: request
                                .auth_parameters
                                .unwrap_or_default(),
                            client_id: client_id(request.client_id)?,
                            user_pool_id: user_pool_id(request.user_pool_id)?,
                        },
                    )
                    .map_err(|error| error.to_aws_error())?,
            )
        }
        CognitoOperation::RespondToAuthChallenge => {
            let request = parse_json_body::<RespondToAuthChallengeRequest>(
                request.body(),
            )?;
            reject_extra_fields("RespondToAuthChallenge", request.extra)?;
            json_response(
                &cognito
                    .respond_to_auth_challenge(
                        &scope,
                        request_host,
                        CognitoRespondToAuthChallengeInput {
                            challenge_name: request.challenge_name,
                            challenge_responses: request
                                .challenge_responses
                                .unwrap_or_default(),
                            client_id: client_id(request.client_id)?,
                            session: request.session,
                        },
                    )
                    .map_err(|error| error.to_aws_error())?,
            )
        }
        CognitoOperation::GetUser => {
            let request = parse_json_body::<GetUserRequest>(request.body())?;
            reject_extra_fields("GetUser", request.extra)?;
            json_response(
                &cognito
                    .get_user(CognitoGetUserInput {
                        access_token: request.access_token,
                    })
                    .map_err(|error| error.to_aws_error())?,
            )
        }
        CognitoOperation::UpdateUserAttributes => {
            let request = parse_json_body::<UpdateUserAttributesRequest>(
                request.body(),
            )?;
            reject_extra_fields("UpdateUserAttributes", request.extra)?;
            json_response(
                &cognito
                    .update_user_attributes(CognitoUpdateUserAttributesInput {
                        access_token: request.access_token,
                        user_attributes: request
                            .user_attributes
                            .unwrap_or_default(),
                    })
                    .map_err(|error| error.to_aws_error())?,
            )
        }
        CognitoOperation::ChangePassword => {
            let request =
                parse_json_body::<ChangePasswordRequest>(request.body())?;
            reject_extra_fields("ChangePassword", request.extra)?;
            json_response(
                &cognito
                    .change_password(CognitoChangePasswordInput {
                        access_token: request.access_token,
                        previous_password: request.previous_password,
                        proposed_password: request.proposed_password,
                    })
                    .map_err(|error| error.to_aws_error())?,
            )
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct CreateUserPoolRequest {
    pool_name: String,
    #[serde(flatten)]
    extra: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct DescribeUserPoolRequest {
    user_pool_id: String,
    #[serde(flatten)]
    extra: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct ListUserPoolsRequest {
    max_results: Option<u32>,
    next_token: Option<String>,
    #[serde(flatten)]
    extra: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct UpdateUserPoolRequest {
    pool_name: Option<String>,
    user_pool_id: String,
    #[serde(flatten)]
    extra: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct DeleteUserPoolRequest {
    user_pool_id: String,
    #[serde(flatten)]
    extra: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct CreateUserPoolClientRequest {
    client_name: String,
    explicit_auth_flows: Option<Vec<CognitoExplicitAuthFlow>>,
    generate_secret: Option<bool>,
    user_pool_id: String,
    #[serde(flatten)]
    extra: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct DescribeUserPoolClientRequest {
    client_id: String,
    user_pool_id: String,
    #[serde(flatten)]
    extra: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct ListUserPoolClientsRequest {
    max_results: Option<u32>,
    next_token: Option<String>,
    user_pool_id: String,
    #[serde(flatten)]
    extra: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct UpdateUserPoolClientRequest {
    client_id: String,
    client_name: Option<String>,
    explicit_auth_flows: Option<Vec<CognitoExplicitAuthFlow>>,
    user_pool_id: String,
    #[serde(flatten)]
    extra: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct DeleteUserPoolClientRequest {
    client_id: String,
    user_pool_id: String,
    #[serde(flatten)]
    extra: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct AdminCreateUserRequest {
    temporary_password: Option<String>,
    user_attributes: Option<Vec<CognitoAttributeType>>,
    user_pool_id: String,
    username: String,
    #[serde(flatten)]
    extra: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct AdminGetUserRequest {
    user_pool_id: String,
    username: String,
    #[serde(flatten)]
    extra: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct ListUsersRequest {
    limit: Option<u32>,
    pagination_token: Option<String>,
    user_pool_id: String,
    #[serde(flatten)]
    extra: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct AdminUpdateUserAttributesRequest {
    user_attributes: Option<Vec<CognitoAttributeType>>,
    user_pool_id: String,
    username: String,
    #[serde(flatten)]
    extra: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct AdminDeleteUserRequest {
    user_pool_id: String,
    username: String,
    #[serde(flatten)]
    extra: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct AdminSetUserPasswordRequest {
    password: String,
    permanent: Option<bool>,
    user_pool_id: String,
    username: String,
    #[serde(flatten)]
    extra: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct SignUpRequest {
    client_id: String,
    password: String,
    user_attributes: Option<Vec<CognitoAttributeType>>,
    username: String,
    #[serde(flatten)]
    extra: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct ConfirmSignUpRequest {
    client_id: String,
    confirmation_code: String,
    username: String,
    #[serde(flatten)]
    extra: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct InitiateAuthRequest {
    auth_flow: String,
    auth_parameters: Option<BTreeMap<String, String>>,
    client_id: String,
    #[serde(flatten)]
    extra: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct AdminInitiateAuthRequest {
    auth_flow: String,
    auth_parameters: Option<BTreeMap<String, String>>,
    client_id: String,
    user_pool_id: String,
    #[serde(flatten)]
    extra: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct RespondToAuthChallengeRequest {
    challenge_name: String,
    challenge_responses: Option<BTreeMap<String, String>>,
    client_id: String,
    session: String,
    #[serde(flatten)]
    extra: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct GetUserRequest {
    access_token: String,
    #[serde(flatten)]
    extra: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct UpdateUserAttributesRequest {
    access_token: String,
    user_attributes: Option<Vec<CognitoAttributeType>>,
    #[serde(flatten)]
    extra: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct ChangePasswordRequest {
    access_token: String,
    previous_password: String,
    proposed_password: String,
    #[serde(flatten)]
    extra: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CognitoOperation {
    CreateUserPool,
    DescribeUserPool,
    ListUserPools,
    UpdateUserPool,
    DeleteUserPool,
    CreateUserPoolClient,
    DescribeUserPoolClient,
    ListUserPoolClients,
    UpdateUserPoolClient,
    DeleteUserPoolClient,
    AdminCreateUser,
    AdminGetUser,
    ListUsers,
    AdminUpdateUserAttributes,
    AdminDeleteUser,
    AdminSetUserPassword,
    SignUp,
    ConfirmSignUp,
    InitiateAuth,
    AdminInitiateAuth,
    RespondToAuthChallenge,
    GetUser,
    UpdateUserAttributes,
    ChangePassword,
}

fn action_from_target(target: Option<&str>) -> Result<&str, CognitoError> {
    let target = target.ok_or_else(|| {
        CognitoError::unsupported_operation("missing X-Amz-Target")
    })?;
    let Some((prefix, operation)) = target.split_once('.') else {
        return Err(CognitoError::unsupported_operation(format!(
            "Operation {target} is not supported."
        )));
    };
    if prefix != "AWSCognitoIdentityProviderService" {
        return Err(CognitoError::unsupported_operation(format!(
            "Operation {target} is not supported."
        )));
    }

    Ok(operation)
}

fn operation_from_target(
    target: Option<&str>,
) -> Result<CognitoOperation, CognitoError> {
    match action_from_target(target)? {
        "CreateUserPool" => Ok(CognitoOperation::CreateUserPool),
        "DescribeUserPool" => Ok(CognitoOperation::DescribeUserPool),
        "ListUserPools" => Ok(CognitoOperation::ListUserPools),
        "UpdateUserPool" => Ok(CognitoOperation::UpdateUserPool),
        "DeleteUserPool" => Ok(CognitoOperation::DeleteUserPool),
        "CreateUserPoolClient" => Ok(CognitoOperation::CreateUserPoolClient),
        "DescribeUserPoolClient" => {
            Ok(CognitoOperation::DescribeUserPoolClient)
        }
        "ListUserPoolClients" => Ok(CognitoOperation::ListUserPoolClients),
        "UpdateUserPoolClient" => Ok(CognitoOperation::UpdateUserPoolClient),
        "DeleteUserPoolClient" => Ok(CognitoOperation::DeleteUserPoolClient),
        "AdminCreateUser" => Ok(CognitoOperation::AdminCreateUser),
        "AdminGetUser" => Ok(CognitoOperation::AdminGetUser),
        "ListUsers" => Ok(CognitoOperation::ListUsers),
        "AdminUpdateUserAttributes" => {
            Ok(CognitoOperation::AdminUpdateUserAttributes)
        }
        "AdminDeleteUser" => Ok(CognitoOperation::AdminDeleteUser),
        "AdminSetUserPassword" => Ok(CognitoOperation::AdminSetUserPassword),
        "SignUp" => Ok(CognitoOperation::SignUp),
        "ConfirmSignUp" => Ok(CognitoOperation::ConfirmSignUp),
        "InitiateAuth" => Ok(CognitoOperation::InitiateAuth),
        "AdminInitiateAuth" => Ok(CognitoOperation::AdminInitiateAuth),
        "RespondToAuthChallenge" => {
            Ok(CognitoOperation::RespondToAuthChallenge)
        }
        "GetUser" => Ok(CognitoOperation::GetUser),
        "UpdateUserAttributes" => Ok(CognitoOperation::UpdateUserAttributes),
        "ChangePassword" => Ok(CognitoOperation::ChangePassword),
        operation => Err(CognitoError::unsupported_operation(format!(
            "Operation {operation} is not supported."
        ))),
    }
}

fn reject_extra_fields(
    operation: &str,
    extra: BTreeMap<String, Value>,
) -> Result<(), AwsError> {
    if extra.is_empty() {
        return Ok(());
    }

    let unsupported = extra.into_keys().collect::<Vec<_>>().join(", ");
    Err(CognitoError::invalid_parameter(format!(
        "{operation} does not support these fields yet: {unsupported}."
    ))
    .to_aws_error())
}

fn parse_json_body<T>(body: &[u8]) -> Result<T, AwsError>
where
    T: DeserializeOwned,
{
    serde_json::from_slice(body).map_err(|error| {
        CognitoError::invalid_parameter(format!(
            "The request body is not valid JSON: {error}"
        ))
        .to_aws_error()
    })
}

fn json_response<T>(value: &T) -> Result<Vec<u8>, AwsError>
where
    T: Serialize,
{
    serde_json::to_vec(value).map_err(|error| {
        AwsError::trusted_custom(
            AwsErrorFamily::Internal,
            "InternalErrorException",
            format!("Failed to serialize Cognito response: {error}"),
            500,
            false,
        )
    })
}

pub(crate) fn open_id_configuration(
    cognito: &CognitoService,
    host: &str,
    user_pool_id: &str,
) -> Result<Vec<u8>, AwsError> {
    let user_pool_id = CognitoUserPoolId::new(user_pool_id)
        .map_err(|error| error.to_aws_error())?;
    json_response(
        &cognito
            .open_id_configuration(host, &user_pool_id)
            .map_err(|error| error.to_aws_error())?,
    )
}

pub(crate) fn jwks_document(
    cognito: &CognitoService,
    host: &str,
    user_pool_id: &str,
) -> Result<Vec<u8>, AwsError> {
    let user_pool_id = CognitoUserPoolId::new(user_pool_id)
        .map_err(|error| error.to_aws_error())?;
    json_response(
        &cognito
            .jwks_document(host, &user_pool_id)
            .map_err(|error| error.to_aws_error())?,
    )
}

fn user_pool_id(value: String) -> Result<CognitoUserPoolId, AwsError> {
    CognitoUserPoolId::new(value).map_err(|error| error.to_aws_error())
}

fn client_id(value: String) -> Result<CognitoUserPoolClientId, AwsError> {
    CognitoUserPoolClientId::new(value).map_err(|error| error.to_aws_error())
}

#[cfg(test)]
mod tests {
    use super::{
        CognitoOperation, handle_json, json_response, jwks_document,
        open_id_configuration, operation_from_target,
    };
    use crate::aws_error_shape::AwsErrorShape;
    use crate::request::HttpRequest;
    use aws::{
        AccountId, ProtocolFamily, RegionId, RequestContext, ServiceName,
    };
    use services::CognitoService;
    use std::sync::Arc;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};
    use storage::{StorageConfig, StorageFactory, StorageMode};

    struct FixedClock {
        now: SystemTime,
    }

    impl FixedClock {
        fn new(seconds: u64) -> Self {
            Self { now: UNIX_EPOCH + Duration::from_secs(seconds) }
        }
    }

    impl services::Clock for FixedClock {
        fn now(&self) -> SystemTime {
            self.now
        }
    }

    fn service() -> CognitoService {
        let factory = StorageFactory::new(StorageConfig::new(
            "/tmp/http-cognito",
            StorageMode::Memory,
        ));
        CognitoService::new(&factory, Arc::new(FixedClock::new(100)))
    }

    fn context(operation: &str) -> RequestContext {
        RequestContext::try_new(
            "000000000000".parse::<AccountId>().expect("account should parse"),
            "eu-west-2".parse::<RegionId>().expect("region should parse"),
            ServiceName::CognitoIdentityProvider,
            ProtocolFamily::AwsJson11,
            operation,
            None,
            true,
        )
        .expect("request context should build")
    }

    fn request(target: &str, body: &str) -> HttpRequest<'static> {
        let raw = format!(
            "POST / HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/x-amz-json-1.1\r\nX-Amz-Target: {target}\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        );
        let bytes = raw.into_bytes().into_boxed_slice();
        let leaked = Box::leak(bytes);

        HttpRequest::parse(leaked).expect("request should parse")
    }

    #[test]
    fn cognito_control_adapter_round_trips_json_requests() {
        let cognito = service();
        let create_pool = handle_json(
            &cognito,
            &request(
                "AWSCognitoIdentityProviderService.CreateUserPool",
                r#"{"PoolName":"demo"}"#,
            ),
            &context("CreateUserPool"),
        )
        .expect("pool should create");
        let pool: serde_json::Value = serde_json::from_slice(&create_pool)
            .expect("pool JSON should decode");
        let pool_id = pool["UserPool"]["Id"]
            .as_str()
            .expect("pool id should exist")
            .to_owned();

        let create_client = handle_json(
            &cognito,
            &request(
                "AWSCognitoIdentityProviderService.CreateUserPoolClient",
                &format!(
                    r#"{{"UserPoolId":"{pool_id}","ClientName":"app","GenerateSecret":true,"ExplicitAuthFlows":["ALLOW_USER_PASSWORD_AUTH"]}}"#
                ),
            ),
            &context("CreateUserPoolClient"),
        )
        .expect("client should create");
        let client: serde_json::Value = serde_json::from_slice(&create_client)
            .expect("client JSON should decode");
        assert_eq!(
            client["UserPoolClient"]["ClientName"],
            serde_json::Value::String("app".to_owned())
        );

        let create_user = handle_json(
            &cognito,
            &request(
                "AWSCognitoIdentityProviderService.AdminCreateUser",
                &format!(
                    r#"{{"UserPoolId":"{pool_id}","Username":"alice","TemporaryPassword":"Temp1234!","UserAttributes":[{{"Name":"email","Value":"user@example.com"}}]}}"#
                ),
            ),
            &context("AdminCreateUser"),
        )
        .expect("user should create");
        let user: serde_json::Value = serde_json::from_slice(&create_user)
            .expect("user JSON should decode");
        assert_eq!(user["User"]["Username"], "alice");

        let list_users = handle_json(
            &cognito,
            &request(
                "AWSCognitoIdentityProviderService.ListUsers",
                &format!(r#"{{"UserPoolId":"{pool_id}","Limit":60}}"#),
            ),
            &context("ListUsers"),
        )
        .expect("users should list");
        let listed: serde_json::Value = serde_json::from_slice(&list_users)
            .expect("listed JSON should decode");
        assert_eq!(listed["Users"].as_array().map(Vec::len), Some(1));
    }

    #[test]
    fn cognito_control_adapter_rejects_unsupported_fields_and_invalid_json() {
        let cognito = service();
        let unsupported = handle_json(
            &cognito,
            &request(
                "AWSCognitoIdentityProviderService.CreateUserPool",
                r#"{"PoolName":"demo","LambdaConfig":{}}"#,
            ),
            &context("CreateUserPool"),
        )
        .expect_err("unsupported fields should fail");
        assert_eq!(unsupported.code(), "InvalidParameterException");

        let invalid_json = handle_json(
            &cognito,
            &request(
                "AWSCognitoIdentityProviderService.CreateUserPool",
                r#"{"PoolName":"demo""#,
            ),
            &context("CreateUserPool"),
        )
        .expect_err("invalid JSON should fail");
        assert_eq!(invalid_json.code(), "InvalidParameterException");
    }

    #[test]
    fn cognito_control_adapter_covers_remaining_error_paths() {
        let cognito = service();
        let create_pool = handle_json(
            &cognito,
            &request(
                "AWSCognitoIdentityProviderService.CreateUserPool",
                r#"{"PoolName":"demo"}"#,
            ),
            &context("CreateUserPool"),
        )
        .expect("pool should create");
        let pool: serde_json::Value = serde_json::from_slice(&create_pool)
            .expect("pool JSON should decode");
        let pool_id = pool["UserPool"]["Id"]
            .as_str()
            .expect("pool id should exist")
            .to_owned();

        let create_client = handle_json(
            &cognito,
            &request(
                "AWSCognitoIdentityProviderService.CreateUserPoolClient",
                &format!(r#"{{"UserPoolId":"{pool_id}","ClientName":"app"}}"#),
            ),
            &context("CreateUserPoolClient"),
        )
        .expect("client should create");
        let client: serde_json::Value = serde_json::from_slice(&create_client)
            .expect("client JSON should decode");
        let client_id = client["UserPoolClient"]["ClientId"]
            .as_str()
            .expect("client id should exist")
            .to_owned();

        handle_json(
            &cognito,
            &request(
                "AWSCognitoIdentityProviderService.AdminCreateUser",
                &format!(r#"{{"UserPoolId":"{pool_id}","Username":"alice"}}"#),
            ),
            &context("AdminCreateUser"),
        )
        .expect("user should create");

        let describe_client = handle_json(
            &cognito,
            &request(
                "AWSCognitoIdentityProviderService.DescribeUserPoolClient",
                &format!(
                    r#"{{"UserPoolId":"{pool_id}","ClientId":"{client_id}"}}"#
                ),
            ),
            &context("DescribeUserPoolClient"),
        )
        .expect("client should describe");
        let described: serde_json::Value =
            serde_json::from_slice(&describe_client)
                .expect("describe JSON should decode");
        assert_eq!(described["UserPoolClient"]["ClientId"], client_id);

        handle_json(
            &cognito,
            &request(
                "AWSCognitoIdentityProviderService.AdminUpdateUserAttributes",
                &format!(
                    r#"{{"UserPoolId":"{pool_id}","Username":"alice","UserAttributes":[{{"Name":"name","Value":"Alice"}}]}}"#
                ),
            ),
            &context("AdminUpdateUserAttributes"),
        )
        .expect("user attributes should update");

        let invalid_describe = handle_json(
            &cognito,
            &request(
                "AWSCognitoIdentityProviderService.DescribeUserPoolClient",
                "{",
            ),
            &context("DescribeUserPoolClient"),
        )
        .expect_err("invalid describe client JSON should fail");
        assert_eq!(invalid_describe.code(), "InvalidParameterException");

        let invalid_update = handle_json(
            &cognito,
            &request(
                "AWSCognitoIdentityProviderService.AdminUpdateUserAttributes",
                "{",
            ),
            &context("AdminUpdateUserAttributes"),
        )
        .expect_err("invalid admin update JSON should fail");
        assert_eq!(invalid_update.code(), "InvalidParameterException");

        let malformed_target = operation_from_target(Some("BadTarget"))
            .expect_err("targets without separators should fail");
        assert_eq!(
            malformed_target.to_aws_error().code(),
            "UnsupportedOperation"
        );

        struct FailingSerialize;

        impl serde::Serialize for FailingSerialize {
            fn serialize<S>(&self, _serializer: S) -> Result<S::Ok, S::Error>
            where
                S: serde::Serializer,
            {
                Err(serde::ser::Error::custom("boom"))
            }
        }

        let internal = json_response(&FailingSerialize)
            .expect_err("serialization should fail");
        assert_eq!(internal.code(), "InternalErrorException");
        assert_eq!(internal.status_code(), 500);
        assert!(!internal.sender_fault());
    }

    #[test]
    fn cognito_auth_adapter_round_trips_password_flows_and_well_known_docs() {
        let cognito = service();
        let create_pool = handle_json(
            &cognito,
            &request(
                "AWSCognitoIdentityProviderService.CreateUserPool",
                r#"{"PoolName":"auth-demo"}"#,
            ),
            &context("CreateUserPool"),
        )
        .expect("pool should create");
        let pool: serde_json::Value = serde_json::from_slice(&create_pool)
            .expect("pool JSON should decode");
        let pool_id = pool["UserPool"]["Id"]
            .as_str()
            .expect("pool id should exist")
            .to_owned();

        let create_client = handle_json(
            &cognito,
            &request(
                "AWSCognitoIdentityProviderService.CreateUserPoolClient",
                &format!(
                    r#"{{"UserPoolId":"{pool_id}","ClientName":"app","ExplicitAuthFlows":["ALLOW_USER_PASSWORD_AUTH"]}}"#
                ),
            ),
            &context("CreateUserPoolClient"),
        )
        .expect("client should create");
        let client: serde_json::Value = serde_json::from_slice(&create_client)
            .expect("client JSON should decode");
        let client_id = client["UserPoolClient"]["ClientId"]
            .as_str()
            .expect("client id should exist")
            .to_owned();

        handle_json(
            &cognito,
            &request(
                "AWSCognitoIdentityProviderService.SignUp",
                &format!(
                    r#"{{"ClientId":"{client_id}","Password":"Perm1234!","Username":"alice","UserAttributes":[{{"Name":"email","Value":"user@example.com"}}]}}"#
                ),
            ),
            &context("SignUp"),
        )
        .expect("sign up should succeed");
        handle_json(
            &cognito,
            &request(
                "AWSCognitoIdentityProviderService.ConfirmSignUp",
                &format!(
                    r#"{{"ClientId":"{client_id}","ConfirmationCode":"000000","Username":"alice"}}"#
                ),
            ),
            &context("ConfirmSignUp"),
        )
        .expect("confirm sign up should succeed");

        let auth = handle_json(
            &cognito,
            &request(
                "AWSCognitoIdentityProviderService.InitiateAuth",
                &format!(
                    r#"{{"AuthFlow":"USER_PASSWORD_AUTH","AuthParameters":{{"USERNAME":"alice","PASSWORD":"Perm1234!"}},"ClientId":"{client_id}"}}"#
                ),
            ),
            &context("InitiateAuth"),
        )
        .expect("password auth should succeed");
        let auth_json: serde_json::Value =
            serde_json::from_slice(&auth).expect("auth JSON should decode");
        let access_token = auth_json["AuthenticationResult"]["AccessToken"]
            .as_str()
            .expect("access token should exist")
            .to_owned();

        let get_user = handle_json(
            &cognito,
            &request(
                "AWSCognitoIdentityProviderService.GetUser",
                &format!(r#"{{"AccessToken":"{access_token}"}}"#),
            ),
            &context("GetUser"),
        )
        .expect("get user should succeed");
        let get_user_json: serde_json::Value =
            serde_json::from_slice(&get_user)
                .expect("user JSON should decode");
        assert_eq!(get_user_json["Username"], "alice");

        handle_json(
            &cognito,
            &request(
                "AWSCognitoIdentityProviderService.UpdateUserAttributes",
                &format!(
                    r#"{{"AccessToken":"{access_token}","UserAttributes":[{{"Name":"name","Value":"Alice"}}]}}"#
                ),
            ),
            &context("UpdateUserAttributes"),
        )
        .expect("update user attributes should succeed");
        handle_json(
            &cognito,
            &request(
                "AWSCognitoIdentityProviderService.ChangePassword",
                &format!(
                    r#"{{"AccessToken":"{access_token}","PreviousPassword":"Perm1234!","ProposedPassword":"Perm5678!"}}"#
                ),
            ),
            &context("ChangePassword"),
        )
        .expect("change password should succeed");

        let open_id = open_id_configuration(&cognito, "localhost", &pool_id)
            .expect("openid configuration should render");
        let open_id_json: serde_json::Value = serde_json::from_slice(&open_id)
            .expect("openid JSON should decode");
        assert_eq!(
            open_id_json["issuer"],
            serde_json::Value::String(format!("http://localhost/{pool_id}"))
        );
        let jwks = jwks_document(&cognito, "localhost", &pool_id)
            .expect("jwks should render");
        let jwks_json: serde_json::Value =
            serde_json::from_slice(&jwks).expect("jwks JSON should decode");
        assert_eq!(jwks_json["keys"][0]["kid"], pool_id);
    }

    #[test]
    fn cognito_auth_adapter_handles_challenges_and_explicit_failures() {
        let cognito = service();
        let create_pool = handle_json(
            &cognito,
            &request(
                "AWSCognitoIdentityProviderService.CreateUserPool",
                r#"{"PoolName":"auth-demo"}"#,
            ),
            &context("CreateUserPool"),
        )
        .expect("pool should create");
        let pool: serde_json::Value = serde_json::from_slice(&create_pool)
            .expect("pool JSON should decode");
        let pool_id = pool["UserPool"]["Id"]
            .as_str()
            .expect("pool id should exist")
            .to_owned();

        let create_client = handle_json(
            &cognito,
            &request(
                "AWSCognitoIdentityProviderService.CreateUserPoolClient",
                &format!(
                    r#"{{"UserPoolId":"{pool_id}","ClientName":"app","ExplicitAuthFlows":["ALLOW_ADMIN_USER_PASSWORD_AUTH","ALLOW_USER_PASSWORD_AUTH"]}}"#
                ),
            ),
            &context("CreateUserPoolClient"),
        )
        .expect("client should create");
        let client: serde_json::Value = serde_json::from_slice(&create_client)
            .expect("client JSON should decode");
        let client_id = client["UserPoolClient"]["ClientId"]
            .as_str()
            .expect("client id should exist")
            .to_owned();

        handle_json(
            &cognito,
            &request(
                "AWSCognitoIdentityProviderService.AdminCreateUser",
                &format!(
                    r#"{{"UserPoolId":"{pool_id}","Username":"alice","TemporaryPassword":"Temp1234!"}}"#
                ),
            ),
            &context("AdminCreateUser"),
        )
        .expect("admin user should create");

        let challenge = handle_json(
            &cognito,
            &request(
                "AWSCognitoIdentityProviderService.AdminInitiateAuth",
                &format!(
                    r#"{{"UserPoolId":"{pool_id}","ClientId":"{client_id}","AuthFlow":"ADMIN_USER_PASSWORD_AUTH","AuthParameters":{{"USERNAME":"alice","PASSWORD":"Temp1234!"}}}}"#
                ),
            ),
            &context("AdminInitiateAuth"),
        )
        .expect("admin auth should challenge");
        let challenge_json: serde_json::Value =
            serde_json::from_slice(&challenge)
                .expect("challenge JSON should decode");
        assert_eq!(
            challenge_json["ChallengeName"],
            serde_json::Value::String("NEW_PASSWORD_REQUIRED".to_owned())
        );
        let session = challenge_json["Session"]
            .as_str()
            .expect("session should exist")
            .to_owned();

        let completed = handle_json(
            &cognito,
            &request(
                "AWSCognitoIdentityProviderService.RespondToAuthChallenge",
                &format!(
                    r#"{{"ClientId":"{client_id}","ChallengeName":"NEW_PASSWORD_REQUIRED","ChallengeResponses":{{"USERNAME":"alice","NEW_PASSWORD":"Perm1234!"}},"Session":"{session}"}}"#
                ),
            ),
            &context("RespondToAuthChallenge"),
        )
        .expect("challenge should complete");
        let completed_json: serde_json::Value =
            serde_json::from_slice(&completed)
                .expect("completed auth JSON should decode");
        assert!(
            completed_json["AuthenticationResult"]["AccessToken"]
                .as_str()
                .is_some()
        );

        let unsupported = handle_json(
            &cognito,
            &request(
                "AWSCognitoIdentityProviderService.InitiateAuth",
                &format!(
                    r#"{{"AuthFlow":"USER_SRP_AUTH","AuthParameters":{{"USERNAME":"alice","SRP_A":"value"}},"ClientId":"{client_id}"}}"#
                ),
            ),
            &context("InitiateAuth"),
        )
        .expect_err("unsupported auth flows should fail explicitly");
        assert_eq!(unsupported.code(), "UnsupportedOperation");
    }

    #[test]
    fn cognito_control_adapter_maps_targets_to_explicit_operations() {
        assert_eq!(
            operation_from_target(Some(
                "AWSCognitoIdentityProviderService.AdminGetUser",
            ))
            .expect("target should parse"),
            CognitoOperation::AdminGetUser
        );

        let missing = operation_from_target(None)
            .expect_err("missing target should fail");
        let wrong_prefix =
            operation_from_target(Some("AmazonSSM.GetParameter"))
                .expect_err("wrong target prefix should fail");
        let unsupported = operation_from_target(Some(
            "AWSCognitoIdentityProviderService.ForgotPassword",
        ))
        .expect_err("unsupported operations should fail");

        assert_eq!(missing.to_aws_error().code(), "UnsupportedOperation");
        assert_eq!(wrong_prefix.to_aws_error().code(), "UnsupportedOperation");
        assert_eq!(unsupported.to_aws_error().code(), "UnsupportedOperation");
    }
}
