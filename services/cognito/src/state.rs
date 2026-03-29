use crate::{
    auth::{
        AdminInitiateAuthInput, AdminInitiateAuthOutput, ChangePasswordInput,
        ChangePasswordOutput, CognitoAuthenticationResult,
        CognitoChallengeName, CognitoExplicitAuthFlow, GetUserInput,
        GetUserOutput, InitiateAuthInput, InitiateAuthOutput,
        RespondToAuthChallengeInput, RespondToAuthChallengeOutput,
        UpdateUserAttributesInput, UpdateUserAttributesOutput,
        parse_explicit_auth_flows,
    },
    clients::{
        CognitoUserPoolClient, CognitoUserPoolClientDescription,
        CreateUserPoolClientInput, CreateUserPoolClientOutput,
        DeleteUserPoolClientInput, DeleteUserPoolClientOutput,
        DescribeUserPoolClientInput, DescribeUserPoolClientOutput,
        ListUserPoolClientsInput, ListUserPoolClientsOutput,
        UpdateUserPoolClientInput, UpdateUserPoolClientOutput,
    },
    errors::CognitoError,
    identifiers::{CognitoUserPoolClientId, CognitoUserPoolId},
    jwks::{CognitoJwk, CognitoJwksDocument, CognitoOpenIdConfiguration},
    scope::CognitoScope,
    user_pools::{
        CognitoUserPool, CognitoUserPoolStatus, CognitoUserPoolSummary,
        CreateUserPoolInput, CreateUserPoolOutput, DeleteUserPoolInput,
        DeleteUserPoolOutput, DescribeUserPoolInput, DescribeUserPoolOutput,
        ListUserPoolsInput, ListUserPoolsOutput, UpdateUserPoolInput,
        UpdateUserPoolOutput,
    },
    users::{
        AdminCreateUserInput, AdminCreateUserOutput, AdminDeleteUserInput,
        AdminDeleteUserOutput, AdminGetUserInput, AdminGetUserOutput,
        AdminSetUserPasswordInput, AdminSetUserPasswordOutput,
        AdminUpdateUserAttributesInput, AdminUpdateUserAttributesOutput,
        AttributeType, CognitoCodeDeliveryDetails, CognitoUser,
        CognitoUserStatus, ConfirmSignUpInput, ConfirmSignUpOutput,
        ListUsersInput, ListUsersOutput, SignUpInput, SignUpOutput,
    },
};
use aws::{AccountId, Clock, RegionId, SharedAdvertisedEdge};
use base64::Engine as _;
use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
use ring::{rand::SystemRandom, rsa, signature};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, OnceLock};
use std::time::UNIX_EPOCH;
use storage::{StorageFactory, StorageHandle};

const ACCESS_TOKEN_LIFETIME_SECONDS: u64 = 3600;
const DEFAULT_CLIENT_AUTH_FLOWS: [CognitoExplicitAuthFlow; 3] = [
    CognitoExplicitAuthFlow::AllowRefreshTokenAuth,
    CognitoExplicitAuthFlow::AllowUserSrpAuth,
    CognitoExplicitAuthFlow::AllowCustomAuth,
];
const DEFAULT_CONFIRMATION_CODE: &str = "000000";
const MAX_LIST_RESULTS: u32 = 60;
const SESSION_LIFETIME_SECONDS: u64 = 300;
const SIGNING_KEY_DER_B64: &str = "MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDNQ8kC8gneCbXBmuFjUYQ6AkH8TCx2A12UFR/675ywnR/LQP0uOgagLCNomLepAsI7hTkSFt8xY6D7zA8z6A3bF2/um8GtEaAzzZAQJDHYPRHW9pwXYbzRzkjpcy02ExtkYlqmmqjs1Q57ZXEDkrZNtl1h+J6Dznc2g9Mb05EU9B1N4+30v72OTVs8sukwOQ20uw4qgB50AS44oiywGzJWraUEeiR7egyDXLPGojGpXeV+9x5lt5k98CgXu8hlv+MRAQGInov1lJWYHdSY2CwAnvFrDSrR23yOXKSZfF1thjcKarVRBRXcFxFMtonbOToc0wyaxLbxcoWLQF0weOyJAgMBAAECggEAEJCjOtF/Xhu8vylxfb9E+1z2CAc6MdJq9YHAbVLzu1WMzK17mSTN5tR1DZKAhNtQAzdowAbYP389rUBeLDdSke8p5XDMsBu7aN7SnNjc7GleRJGtjl4A+9II1I0q1YG0fNUKoYYa8pTtmKP6uzn2bB5W2iqTGUo62OuJyIW6AD2flXPQNvDCaogREUhP1jx5dKAnvzqcbB8BFsyWS14/Rd72z0z4b5pfvDG0Fne1kahTo31UQqqanf8V5epUmzSZQbEYkNb0HVgmAwKE3IbI7+bkdH5LNX3UiH/GO2oIfVenNRfk88smpdYZU73R02EvDN5dzFLhBuHGT31jJA+KsQKBgQDmMsl3Nmi6ITVpW0DPwBuQ3lzdqrWiovBl8rEczQhByCkDIERcuCaGFLY2njmTkRm1ga3BszgK3kfUCRjIi0erdA/bTQw29o2BhrjjpeLmkE6g4YefmzkwW6ZTqSYN4gW77iVFcOJbV1JpGLkpg86aRGTxfFqL2UQaQdee6EOqGQKBgQDkRZBzjHeoCEaks9lyd8EA5TjxbdJtNjbY+EaZPXi2mZQ0GXoGGma4CVUs8F4D8r8hTUG5+JMuoG+P7RsyQ45F3a07myANuEFL7hUcp7tjc7K4QmF0i/k2Ad7J13NyvxAV6TgCRl71HJX8+KuTxctNOWRyidTmMpbHoLgva/OD8QKBgQDS6UlpbnJ5xx92zmMNdchL7VBM0LHmtz9nSPs5limwi8H32UKJaTfytVtVo5bBO7rAcHZQ+PGqE4rgHQ2WAPbDgm1c0rUUTsXMHTGdckn4UdOY18o5VDELu40jLeFt6t6yFzoS66CZa+JX5I0SGm/bLDsIfpU/eOtK23TBZbFJUQKBgG5B54bYV0dzT+Pn9uTiUPgEv99lY1la8V0Vdsw3s+HHBp2vnI/sGqdT4q9FHrQbgRQtw8x282h3F8vWA+fgV40JiM4cnHvj/q4VPl9L6SgdTzrO3VG09leOybSGe332KWfb8TCwGtebyhqZZg6HYC5ZxVnJl2rlDo035R8KrmGRAoGBAOOqzmChtWW6auOj/KEiWM0RPOciR1vTP9hqAszH/e4YRUKiawOfET0QjTSHKit1xCsvw2QhFWuGUCeMikG8Tun17QnWK1Cu/+C11lMhpMY6Mpe69HzrNTCuxtJlHeSl30CuEQMGe/3HeMWnV50znCJwrxlfN81NFCX2XazgzpgY";
const SIGNING_PUBLIC_KEY_EXPONENT_B64URL: &str = "AQAB";
const SIGNING_PUBLIC_KEY_MODULUS_B64URL: &str = "zUPJAvIJ3gm1wZrhY1GEOgJB_EwsdgNdlBUf-u-csJ0fy0D9LjoGoCwjaJi3qQLCO4U5EhbfMWOg-8wPM-gN2xdv7pvBrRGgM82QECQx2D0R1vacF2G80c5I6XMtNhMbZGJappqo7NUOe2VxA5K2TbZdYfieg853NoPTG9ORFPQdTePt9L-9jk1bPLLpMDkNtLsOKoAedAEuOKIssBsyVq2lBHoke3oMg1yzxqIxqV3lfvceZbeZPfAoF7vIZb_jEQEBiJ6L9ZSVmB3UmNgsAJ7xaw0q0dt8jlykmXxdbYY3Cmq1UQUV3BcRTLaJ2zk6HNMMmsS28XKFi0BdMHjsiQ";
static SIGNING_KEY_PAIR: OnceLock<Result<rsa::KeyPair, String>> =
    OnceLock::new();
static SIGNING_PUBLIC_KEY_EXPONENT: OnceLock<Result<Vec<u8>, String>> =
    OnceLock::new();
static SIGNING_PUBLIC_KEY_MODULUS: OnceLock<Result<Vec<u8>, String>> =
    OnceLock::new();

#[derive(Clone)]
pub struct CognitoService {
    advertised_edge: SharedAdvertisedEdge,
    clock: Arc<dyn Clock>,
    state_store: StorageHandle<CognitoStateKey, StoredCognitoState>,
}

impl CognitoService {
    pub fn new(factory: &StorageFactory, clock: Arc<dyn Clock>) -> Self {
        Self::with_advertised_edge(
            factory,
            clock,
            SharedAdvertisedEdge::default(),
        )
    }

    pub fn with_advertised_edge(
        factory: &StorageFactory,
        clock: Arc<dyn Clock>,
        advertised_edge: SharedAdvertisedEdge,
    ) -> Self {
        Self::with_store_and_advertised_edge(
            clock,
            factory.create("cognito", "control-plane"),
            advertised_edge,
        )
    }

    #[cfg(test)]
    fn with_store(
        clock: Arc<dyn Clock>,
        state_store: StorageHandle<CognitoStateKey, StoredCognitoState>,
    ) -> Self {
        Self::with_store_and_advertised_edge(
            clock,
            state_store,
            SharedAdvertisedEdge::default(),
        )
    }

    fn with_store_and_advertised_edge(
        clock: Arc<dyn Clock>,
        state_store: StorageHandle<CognitoStateKey, StoredCognitoState>,
        advertised_edge: SharedAdvertisedEdge,
    ) -> Self {
        Self { advertised_edge, clock, state_store }
    }

    /// # Errors
    ///
    /// Returns an error when validation fails, identifier allocation fails, or
    /// the updated Cognito state cannot be persisted.
    pub fn create_user_pool(
        &self,
        scope: &CognitoScope,
        input: CreateUserPoolInput,
    ) -> Result<CreateUserPoolOutput, CognitoError> {
        validate_non_empty("PoolName", &input.pool_name)?;

        let mut state = self.load_state(scope);
        let pool_id = self.next_user_pool_id(scope, &mut state)?;
        let now = self.now_epoch_seconds();
        let pool = StoredUserPool {
            arn: user_pool_arn(scope, &pool_id),
            creation_date: now,
            id: pool_id.clone(),
            issuer_origin: None,
            last_modified_date: now,
            name: input.pool_name,
            status: CognitoUserPoolStatus::Enabled,
        };
        let response = CreateUserPoolOutput { user_pool: pool.to_user_pool() };

        state.user_pools.insert(pool_id, pool);
        self.save_state(scope, state)?;

        Ok(response)
    }

    /// # Errors
    ///
    /// Returns an error when the requested user pool does not exist.
    pub fn describe_user_pool(
        &self,
        scope: &CognitoScope,
        input: DescribeUserPoolInput,
    ) -> Result<DescribeUserPoolOutput, CognitoError> {
        let state = self.load_state(scope);
        let pool = user_pool(&state, &input.user_pool_id)?;

        Ok(DescribeUserPoolOutput { user_pool: pool.to_user_pool() })
    }

    /// # Errors
    ///
    /// Returns an error when pagination inputs are invalid.
    pub fn list_user_pools(
        &self,
        scope: &CognitoScope,
        input: ListUserPoolsInput,
    ) -> Result<ListUserPoolsOutput, CognitoError> {
        let state = self.load_state(scope);
        let mut user_pools = state
            .user_pools
            .values()
            .map(StoredUserPool::to_summary)
            .collect::<Vec<_>>();
        user_pools.sort_by(|left, right| left.id.cmp(&right.id));
        let (user_pools, next_token) = paginate(
            user_pools,
            input.max_results,
            input.next_token.as_deref(),
        )?;

        Ok(ListUserPoolsOutput { next_token, user_pools })
    }

    /// # Errors
    ///
    /// Returns an error when validation fails, the target user pool does not
    /// exist, or the updated state cannot be persisted.
    pub fn update_user_pool(
        &self,
        scope: &CognitoScope,
        input: UpdateUserPoolInput,
    ) -> Result<UpdateUserPoolOutput, CognitoError> {
        let mut state = self.load_state(scope);
        let pool = user_pool_mut(&mut state, &input.user_pool_id)?;

        if let Some(pool_name) = input.pool_name {
            validate_non_empty("PoolName", &pool_name)?;
            pool.name = pool_name;
            pool.last_modified_date = self.now_epoch_seconds();
        }

        let response = UpdateUserPoolOutput { user_pool: pool.to_user_pool() };
        self.save_state(scope, state)?;

        Ok(response)
    }

    /// # Errors
    ///
    /// Returns an error when the target user pool does not exist or the
    /// updated state cannot be persisted.
    pub fn delete_user_pool(
        &self,
        scope: &CognitoScope,
        input: DeleteUserPoolInput,
    ) -> Result<DeleteUserPoolOutput, CognitoError> {
        let mut state = self.load_state(scope);
        if state.user_pools.remove(&input.user_pool_id).is_none() {
            return Err(CognitoError::resource_not_found(format!(
                "User pool {} does not exist.",
                input.user_pool_id
            )));
        }

        state
            .user_pool_clients
            .retain(|_, client| client.user_pool_id != input.user_pool_id);
        state
            .auth_sessions
            .retain(|_, session| session.user_pool_id != input.user_pool_id);
        state
            .issued_tokens
            .retain(|_, token| token.user_pool_id != input.user_pool_id);
        state
            .users
            .retain(|identity, _| identity.user_pool_id != input.user_pool_id);
        self.save_state(scope, state)?;

        Ok(DeleteUserPoolOutput {})
    }

    /// # Errors
    ///
    /// Returns an error when validation fails, the target user pool does not
    /// exist, or the updated state cannot be persisted.
    pub fn create_user_pool_client(
        &self,
        scope: &CognitoScope,
        input: CreateUserPoolClientInput,
    ) -> Result<CreateUserPoolClientOutput, CognitoError> {
        validate_non_empty("ClientName", &input.client_name)?;

        let mut state = self.load_state(scope);
        let _ = user_pool(&state, &input.user_pool_id)?;
        let client_id = next_user_pool_client_id(&mut state)?;
        let explicit_auth_flows =
            normalize_auth_flows(input.explicit_auth_flows, true)?;
        let now = self.now_epoch_seconds();
        let client = StoredUserPoolClient {
            client_id: client_id.clone(),
            client_name: input.client_name,
            client_secret: input
                .generate_secret
                .unwrap_or(false)
                .then(|| derived_secret(&client_id)),
            creation_date: now,
            explicit_auth_flows,
            last_modified_date: now,
            user_pool_id: input.user_pool_id,
        };
        let response = CreateUserPoolClientOutput {
            user_pool_client: client.to_user_pool_client(),
        };

        state.user_pool_clients.insert(client_id, client);
        self.save_state(scope, state)?;

        Ok(response)
    }

    /// # Errors
    ///
    /// Returns an error when the target user pool or client does not exist.
    pub fn describe_user_pool_client(
        &self,
        scope: &CognitoScope,
        input: DescribeUserPoolClientInput,
    ) -> Result<DescribeUserPoolClientOutput, CognitoError> {
        let state = self.load_state(scope);
        let client =
            user_pool_client(&state, &input.user_pool_id, &input.client_id)?;

        Ok(DescribeUserPoolClientOutput {
            user_pool_client: client.to_user_pool_client(),
        })
    }

    /// # Errors
    ///
    /// Returns an error when the target user pool does not exist or pagination
    /// inputs are invalid.
    pub fn list_user_pool_clients(
        &self,
        scope: &CognitoScope,
        input: ListUserPoolClientsInput,
    ) -> Result<ListUserPoolClientsOutput, CognitoError> {
        let state = self.load_state(scope);
        let _ = user_pool(&state, &input.user_pool_id)?;
        let mut user_pool_clients = state
            .user_pool_clients
            .values()
            .filter(|client| client.user_pool_id == input.user_pool_id)
            .map(StoredUserPoolClient::to_description)
            .collect::<Vec<_>>();
        user_pool_clients
            .sort_by(|left, right| left.client_id.cmp(&right.client_id));
        let (user_pool_clients, next_token) = paginate(
            user_pool_clients,
            input.max_results,
            input.next_token.as_deref(),
        )?;

        Ok(ListUserPoolClientsOutput { next_token, user_pool_clients })
    }

    /// # Errors
    ///
    /// Returns an error when validation fails, the target user pool client
    /// does not exist, or the updated state cannot be persisted.
    pub fn update_user_pool_client(
        &self,
        scope: &CognitoScope,
        input: UpdateUserPoolClientInput,
    ) -> Result<UpdateUserPoolClientOutput, CognitoError> {
        let mut state = self.load_state(scope);
        let client = user_pool_client_mut(
            &mut state,
            &input.user_pool_id,
            &input.client_id,
        )?;

        if let Some(client_name) = input.client_name {
            validate_non_empty("ClientName", &client_name)?;
            client.client_name = client_name;
            client.last_modified_date = self.now_epoch_seconds();
        }
        if let Some(explicit_auth_flows) = input.explicit_auth_flows {
            client.explicit_auth_flows =
                normalize_auth_flows(Some(explicit_auth_flows), false)?;
            client.last_modified_date = self.now_epoch_seconds();
        }

        let response = UpdateUserPoolClientOutput {
            user_pool_client: client.to_user_pool_client(),
        };
        self.save_state(scope, state)?;

        Ok(response)
    }

    /// # Errors
    ///
    /// Returns an error when the target user pool client does not exist or the
    /// updated state cannot be persisted.
    pub fn delete_user_pool_client(
        &self,
        scope: &CognitoScope,
        input: DeleteUserPoolClientInput,
    ) -> Result<DeleteUserPoolClientOutput, CognitoError> {
        let mut state = self.load_state(scope);
        let _ =
            user_pool_client(&state, &input.user_pool_id, &input.client_id)?;
        state.user_pool_clients.remove(&input.client_id);
        self.save_state(scope, state)?;

        Ok(DeleteUserPoolClientOutput {})
    }

    /// # Errors
    ///
    /// Returns an error when validation fails, the target user pool does not
    /// exist, the username already exists, or the updated state cannot be
    /// persisted.
    pub fn admin_create_user(
        &self,
        scope: &CognitoScope,
        input: AdminCreateUserInput,
    ) -> Result<AdminCreateUserOutput, CognitoError> {
        validate_non_empty("Username", &input.username)?;
        if let Some(temporary_password) = input.temporary_password.as_deref() {
            validate_non_empty("TemporaryPassword", temporary_password)?;
        }
        let attributes = attribute_map(&input.user_attributes)?;

        let mut state = self.load_state(scope);
        let _ = user_pool(&state, &input.user_pool_id)?;
        let identity = StoredUserIdentity {
            user_pool_id: input.user_pool_id.clone(),
            username: input.username.clone(),
        };
        if state.users.contains_key(&identity) {
            return Err(CognitoError::username_exists(format!(
                "User {} already exists.",
                input.username
            )));
        }

        let sub = next_user_sub(&mut state)?;
        let now = self.now_epoch_seconds();
        let mut user_attributes = attributes;
        user_attributes.insert("sub".to_owned(), sub);
        let temporary_password =
            input.temporary_password.as_deref().map(hash_password);
        let user_status = if temporary_password.is_some() {
            CognitoUserStatus::ForceChangePassword
        } else {
            CognitoUserStatus::Unconfirmed
        };
        let user = StoredUser {
            attributes: user_attributes,
            confirmation_code_hash: None,
            enabled: true,
            password_hash: temporary_password,
            temporary_password: input.temporary_password.is_some(),
            user_create_date: now,
            user_last_modified_date: now,
            user_status,
            username: input.username,
        };
        let response = AdminCreateUserOutput { user: user.to_user() };

        state.users.insert(identity, user);
        self.save_state(scope, state)?;

        Ok(response)
    }

    /// # Errors
    ///
    /// Returns an error when the target user pool or user does not exist.
    pub fn admin_get_user(
        &self,
        scope: &CognitoScope,
        input: AdminGetUserInput,
    ) -> Result<AdminGetUserOutput, CognitoError> {
        let state = self.load_state(scope);
        let _ = user_pool(&state, &input.user_pool_id)?;
        let user = user(&state, &input.user_pool_id, &input.username)?;

        Ok(user.to_admin_get_user_output())
    }

    /// # Errors
    ///
    /// Returns an error when the target user pool does not exist or pagination
    /// inputs are invalid.
    pub fn list_users(
        &self,
        scope: &CognitoScope,
        input: ListUsersInput,
    ) -> Result<ListUsersOutput, CognitoError> {
        let state = self.load_state(scope);
        let _ = user_pool(&state, &input.user_pool_id)?;
        let mut users = state
            .users
            .iter()
            .filter(|(identity, _)| {
                identity.user_pool_id == input.user_pool_id
            })
            .map(|(_, user)| user.to_user())
            .collect::<Vec<_>>();
        users.sort_by(|left, right| left.username.cmp(&right.username));
        let (users, pagination_token) =
            paginate(users, input.limit, input.pagination_token.as_deref())?;

        Ok(ListUsersOutput { pagination_token, users })
    }

    /// # Errors
    ///
    /// Returns an error when validation fails, the target user does not exist,
    /// or the updated state cannot be persisted.
    pub fn admin_update_user_attributes(
        &self,
        scope: &CognitoScope,
        input: AdminUpdateUserAttributesInput,
    ) -> Result<AdminUpdateUserAttributesOutput, CognitoError> {
        let attributes = attribute_map(&input.user_attributes)?;
        let mut state = self.load_state(scope);
        let user = user_mut(&mut state, &input.user_pool_id, &input.username)?;
        for (name, value) in attributes {
            user.attributes.insert(name, value);
        }
        user.user_last_modified_date = self.now_epoch_seconds();
        self.save_state(scope, state)?;

        Ok(AdminUpdateUserAttributesOutput {})
    }

    /// # Errors
    ///
    /// Returns an error when the target user does not exist or the updated
    /// state cannot be persisted.
    pub fn admin_delete_user(
        &self,
        scope: &CognitoScope,
        input: AdminDeleteUserInput,
    ) -> Result<AdminDeleteUserOutput, CognitoError> {
        let mut state = self.load_state(scope);
        let _ = user(&state, &input.user_pool_id, &input.username)?;
        let identity = StoredUserIdentity {
            user_pool_id: input.user_pool_id,
            username: input.username,
        };
        remove_user_artifacts(&mut state, &identity);
        self.save_state(scope, state)?;

        Ok(AdminDeleteUserOutput {})
    }

    /// # Errors
    ///
    /// Returns an error when validation fails, the target user does not exist,
    /// or the updated state cannot be persisted.
    pub fn admin_set_user_password(
        &self,
        scope: &CognitoScope,
        input: AdminSetUserPasswordInput,
    ) -> Result<AdminSetUserPasswordOutput, CognitoError> {
        validate_non_empty("Password", &input.password)?;

        let mut state = self.load_state(scope);
        let user = user_mut(&mut state, &input.user_pool_id, &input.username)?;
        user.password_hash = Some(hash_password(&input.password));
        user.temporary_password = !input.permanent;
        user.user_last_modified_date = self.now_epoch_seconds();
        user.user_status = if input.permanent {
            CognitoUserStatus::Confirmed
        } else {
            CognitoUserStatus::ForceChangePassword
        };
        self.save_state(scope, state)?;

        Ok(AdminSetUserPasswordOutput {})
    }

    /// # Errors
    ///
    /// Returns an error when validation fails, the target client is invalid,
    /// the username already exists, or the updated state cannot be persisted.
    pub fn sign_up(
        &self,
        scope: &CognitoScope,
        input: SignUpInput,
    ) -> Result<SignUpOutput, CognitoError> {
        validate_non_empty("Password", &input.password)?;
        validate_non_empty("Username", &input.username)?;
        let attributes = attribute_map(&input.user_attributes)?;

        let mut state = self.load_state(scope);
        let client = user_pool_client_by_id(&state, &input.client_id)?;
        let _ = user_pool(&state, &client.user_pool_id)?;
        let identity = StoredUserIdentity {
            user_pool_id: client.user_pool_id.clone(),
            username: input.username.clone(),
        };
        if state.users.contains_key(&identity) {
            return Err(CognitoError::username_exists(format!(
                "User {} already exists.",
                input.username
            )));
        }

        let sub = next_user_sub(&mut state)?;
        let now = self.now_epoch_seconds();
        let mut user_attributes = attributes;
        user_attributes.insert("sub".to_owned(), sub.clone());
        let user = StoredUser {
            attributes: user_attributes.clone(),
            confirmation_code_hash: Some(hash_password(
                DEFAULT_CONFIRMATION_CODE,
            )),
            enabled: true,
            password_hash: Some(hash_password(&input.password)),
            temporary_password: false,
            user_create_date: now,
            user_last_modified_date: now,
            user_status: CognitoUserStatus::Unconfirmed,
            username: input.username.clone(),
        };
        let response = SignUpOutput {
            code_delivery_details: confirmation_delivery_details(
                &user_attributes,
            ),
            user_confirmed: false,
            user_sub: sub,
        };

        state.users.insert(identity, user);
        self.save_state(scope, state)?;

        Ok(response)
    }

    /// # Errors
    ///
    /// Returns an error when validation fails, the target user cannot be
    /// confirmed, or the updated state cannot be persisted.
    pub fn confirm_sign_up(
        &self,
        scope: &CognitoScope,
        input: ConfirmSignUpInput,
    ) -> Result<ConfirmSignUpOutput, CognitoError> {
        validate_non_empty("ConfirmationCode", &input.confirmation_code)?;
        validate_non_empty("Username", &input.username)?;

        let mut state = self.load_state(scope);
        let user_pool_id = user_pool_client_by_id(&state, &input.client_id)?
            .user_pool_id
            .clone();
        let user = user_mut(&mut state, &user_pool_id, &input.username)?;
        let Some(stored_confirmation_code) =
            user.confirmation_code_hash.as_ref()
        else {
            return Err(CognitoError::not_authorized(format!(
                "User {} cannot be confirmed.",
                input.username
            )));
        };
        if *stored_confirmation_code != hash_password(&input.confirmation_code)
        {
            return Err(CognitoError::code_mismatch(
                "Invalid confirmation code provided.",
            ));
        }

        user.confirmation_code_hash = None;
        user.user_last_modified_date = self.now_epoch_seconds();
        user.user_status = CognitoUserStatus::Confirmed;
        self.save_state(scope, state)?;

        Ok(ConfirmSignUpOutput {})
    }

    /// # Errors
    ///
    /// Returns an error when the auth flow is unsupported, credentials are
    /// invalid, or the updated state cannot be persisted.
    pub fn initiate_auth(
        &self,
        scope: &CognitoScope,
        input: InitiateAuthInput,
    ) -> Result<InitiateAuthOutput, CognitoError> {
        let auth_flow = parse_auth_flow(&input.auth_flow)?;

        let mut state = self.load_state(scope);
        let transition = {
            let client = user_pool_client_by_id(&state, &input.client_id)?;
            let user_pool_id = client.user_pool_id.clone();
            ensure_client_supports_flow(client, auth_flow)?;
            authenticate_with_password(
                &state,
                &user_pool_id,
                &input.client_id,
                auth_flow,
                &input.auth_parameters,
            )?
        };
        let output = self.finalize_auth_transition(
            scope,
            &mut state,
            &input.client_id,
            transition,
        )?;
        self.save_state(scope, state)?;

        Ok(output)
    }

    /// # Errors
    ///
    /// Returns an error when the auth flow is unsupported, credentials are
    /// invalid, or the updated state cannot be persisted.
    pub fn admin_initiate_auth(
        &self,
        scope: &CognitoScope,
        input: AdminInitiateAuthInput,
    ) -> Result<AdminInitiateAuthOutput, CognitoError> {
        let auth_flow = parse_auth_flow(&input.auth_flow)?;

        let mut state = self.load_state(scope);
        let transition = {
            let client = user_pool_client(
                &state,
                &input.user_pool_id,
                &input.client_id,
            )?;
            ensure_client_supports_flow(client, auth_flow)?;
            authenticate_with_password(
                &state,
                &input.user_pool_id,
                &input.client_id,
                auth_flow,
                &input.auth_parameters,
            )?
        };
        let output = self.finalize_auth_transition(
            scope,
            &mut state,
            &input.client_id,
            transition,
        )?;
        self.save_state(scope, state)?;

        Ok(output)
    }

    /// # Errors
    ///
    /// Returns an error when the challenge request is invalid, the session is
    /// invalid or expired, or the updated state cannot be persisted.
    pub fn respond_to_auth_challenge(
        &self,
        scope: &CognitoScope,
        input: RespondToAuthChallengeInput,
    ) -> Result<RespondToAuthChallengeOutput, CognitoError> {
        validate_non_empty("Session", &input.session)?;
        let challenge_name = parse_challenge_name(&input.challenge_name)?;
        let mut state = self.load_state(scope);
        let transition = match challenge_name {
            SupportedChallengeName::NewPasswordRequired => {
                validate_challenge_responses(
                    &input.challenge_responses,
                    &["USERNAME", "NEW_PASSWORD"],
                )?;
                let session = {
                    let session =
                        auth_session_mut(&mut state, &input.session)?;
                    if session.challenge
                        != StoredChallengeName::NewPasswordRequired
                    {
                        return Err(CognitoError::not_authorized(
                            "The provided session is not valid.",
                        ));
                    }
                    if session.client_id != input.client_id {
                        return Err(CognitoError::not_authorized(
                            "The provided session is not valid for this client.",
                        ));
                    }
                    if session.expires_at < self.now_epoch_seconds() {
                        return Err(CognitoError::not_authorized(
                            "The provided session has expired.",
                        ));
                    }

                    session.clone()
                };
                let username = required_parameter(
                    &input.challenge_responses,
                    "USERNAME",
                )?;
                if username != session.username {
                    return Err(CognitoError::not_authorized(
                        "The provided session is not valid for this user.",
                    ));
                }
                let new_password = required_parameter(
                    &input.challenge_responses,
                    "NEW_PASSWORD",
                )?;
                validate_non_empty("NEW_PASSWORD", new_password)?;

                let user = user_mut(
                    &mut state,
                    &session.user_pool_id,
                    &session.username,
                )?;
                user.confirmation_code_hash = None;
                user.password_hash = Some(hash_password(new_password));
                user.temporary_password = false;
                user.user_last_modified_date = self.now_epoch_seconds();
                user.user_status = CognitoUserStatus::Confirmed;
                let transition = AuthTransition::Authenticated {
                    user_pool_id: session.user_pool_id.clone(),
                    username: session.username.clone(),
                };
                state.auth_sessions.remove(&input.session);
                transition
            }
        };
        let output = self.finalize_auth_transition(
            scope,
            &mut state,
            &input.client_id,
            transition,
        )?;
        self.save_state(scope, state)?;

        Ok(output)
    }

    /// # Errors
    ///
    /// Returns an error when the access token is invalid or the referenced
    /// user no longer exists.
    pub fn get_user(
        &self,
        input: GetUserInput,
    ) -> Result<GetUserOutput, CognitoError> {
        let verified = self.verify_access_token(&input.access_token)?;
        let state =
            self.state_store.get(&verified.scope).ok_or_else(|| {
                CognitoError::not_authorized("Invalid access token")
            })?;
        let user = user(&state, &verified.user_pool_id, &verified.username)?;

        Ok(GetUserOutput {
            user_attributes: user.attribute_list(),
            username: user.username.clone(),
        })
    }

    /// # Errors
    ///
    /// Returns an error when the access token is invalid, attribute
    /// validation fails, or the updated state cannot be persisted.
    pub fn update_user_attributes(
        &self,
        input: UpdateUserAttributesInput,
    ) -> Result<UpdateUserAttributesOutput, CognitoError> {
        let verified = self.verify_access_token(&input.access_token)?;
        let attributes = attribute_map(&input.user_attributes)?;
        let mut state =
            self.state_store.get(&verified.scope).ok_or_else(|| {
                CognitoError::not_authorized("Invalid access token")
            })?;
        let user =
            user_mut(&mut state, &verified.user_pool_id, &verified.username)?;
        for (name, value) in attributes {
            user.attributes.insert(name, value);
        }
        user.user_last_modified_date = self.now_epoch_seconds();
        self.state_store
            .put(verified.scope, state)
            .map_err(CognitoError::from)?;

        Ok(UpdateUserAttributesOutput { code_delivery_details_list: None })
    }

    /// # Errors
    ///
    /// Returns an error when validation fails, the access token is invalid, or
    /// the updated state cannot be persisted.
    pub fn change_password(
        &self,
        input: ChangePasswordInput,
    ) -> Result<ChangePasswordOutput, CognitoError> {
        validate_non_empty("PreviousPassword", &input.previous_password)?;
        validate_non_empty("ProposedPassword", &input.proposed_password)?;

        let verified = self.verify_access_token(&input.access_token)?;
        let mut state =
            self.state_store.get(&verified.scope).ok_or_else(|| {
                CognitoError::not_authorized("Invalid access token")
            })?;
        let user =
            user_mut(&mut state, &verified.user_pool_id, &verified.username)?;
        let Some(password_hash) = user.password_hash.as_ref() else {
            return Err(CognitoError::not_authorized(
                "Incorrect username or password.",
            ));
        };
        if *password_hash != hash_password(&input.previous_password) {
            return Err(CognitoError::not_authorized(
                "Incorrect username or password.",
            ));
        }

        user.confirmation_code_hash = None;
        user.password_hash = Some(hash_password(&input.proposed_password));
        user.temporary_password = false;
        user.user_last_modified_date = self.now_epoch_seconds();
        user.user_status = CognitoUserStatus::Confirmed;
        self.state_store
            .put(verified.scope, state)
            .map_err(CognitoError::from)?;

        Ok(ChangePasswordOutput {})
    }

    /// # Errors
    ///
    /// Returns an error when the referenced user pool cannot be resolved or
    /// its issuer metadata cannot be persisted.
    pub fn open_id_configuration(
        &self,
        user_pool_id: &CognitoUserPoolId,
    ) -> Result<CognitoOpenIdConfiguration, CognitoError> {
        let (scope, mut state) = self.load_state_for_pool_id(user_pool_id)?;
        let issuer = {
            let pool = user_pool_mut(&mut state, user_pool_id)?;
            ensure_pool_issuer(pool, &self.advertised_edge.current())?
        };
        let configuration = CognitoOpenIdConfiguration {
            claims_supported: vec![
                "auth_time".to_owned(),
                "client_id".to_owned(),
                "cognito:username".to_owned(),
                "email".to_owned(),
                "exp".to_owned(),
                "iat".to_owned(),
                "iss".to_owned(),
                "jti".to_owned(),
                "sub".to_owned(),
                "token_use".to_owned(),
                "username".to_owned(),
            ],
            id_token_signing_alg_values_supported: vec!["RS256".to_owned()],
            issuer: issuer.clone(),
            jwks_uri: self
                .advertised_edge
                .current()
                .cognito_jwks_uri(user_pool_id.as_str()),
            subject_types_supported: vec!["public".to_owned()],
        };
        self.state_store.put(scope, state).map_err(CognitoError::from)?;

        Ok(configuration)
    }

    /// # Errors
    ///
    /// Returns an error when the referenced user pool cannot be resolved or
    /// its issuer metadata cannot be persisted.
    pub fn jwks_document(
        &self,
        user_pool_id: &CognitoUserPoolId,
    ) -> Result<CognitoJwksDocument, CognitoError> {
        let (scope, mut state) = self.load_state_for_pool_id(user_pool_id)?;
        {
            let pool = user_pool_mut(&mut state, user_pool_id)?;
            let _ = ensure_pool_issuer(pool, &self.advertised_edge.current())?;
        }
        let document = CognitoJwksDocument {
            keys: vec![CognitoJwk {
                alg: "RS256".to_owned(),
                e: SIGNING_PUBLIC_KEY_EXPONENT_B64URL.to_owned(),
                kid: signing_key_id(user_pool_id),
                kty: "RSA".to_owned(),
                n: SIGNING_PUBLIC_KEY_MODULUS_B64URL.to_owned(),
                use_: "sig".to_owned(),
            }],
        };
        self.state_store.put(scope, state).map_err(CognitoError::from)?;

        Ok(document)
    }

    fn load_state(&self, scope: &CognitoScope) -> StoredCognitoState {
        self.state_store.get(&scope_key(scope)).unwrap_or_default()
    }

    fn save_state(
        &self,
        scope: &CognitoScope,
        state: StoredCognitoState,
    ) -> Result<(), CognitoError> {
        self.state_store
            .put(scope_key(scope), state)
            .map_err(CognitoError::from)
    }

    fn now_epoch_seconds(&self) -> u64 {
        self.clock
            .now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
    }

    fn next_user_pool_id(
        &self,
        scope: &CognitoScope,
        state: &mut StoredCognitoState,
    ) -> Result<CognitoUserPoolId, CognitoError> {
        let mut next = state.next_user_pool_sequence + 1;
        loop {
            let user_pool_id = CognitoUserPoolId::new(format!(
                "{}_{next:09x}",
                scope.region().as_str()
            ))?;
            if !self.user_pool_id_exists(&user_pool_id) {
                state.next_user_pool_sequence = next;
                return Ok(user_pool_id);
            }

            next += 1;
        }
    }

    fn user_pool_id_exists(&self, user_pool_id: &CognitoUserPoolId) -> bool {
        self.state_store.keys().into_iter().any(|key| {
            self.state_store.get(&key).is_some_and(|state| {
                state.user_pools.contains_key(user_pool_id)
            })
        })
    }

    fn load_state_for_pool_id(
        &self,
        user_pool_id: &CognitoUserPoolId,
    ) -> Result<(CognitoStateKey, StoredCognitoState), CognitoError> {
        let mut matched = self
            .state_store
            .keys()
            .into_iter()
            .filter_map(|key| {
                self.state_store.get(&key).and_then(|state| {
                    state
                        .user_pools
                        .contains_key(user_pool_id)
                        .then_some((key, state))
                })
            })
            .collect::<Vec<_>>();
        match matched.len() {
            0 => Err(CognitoError::resource_not_found(format!(
                "User pool {user_pool_id} does not exist."
            ))),
            1 => matched.pop().ok_or_else(|| {
                CognitoError::invalid_parameter(format!(
                    "User pool {user_pool_id} disappeared during lookup."
                ))
            }),
            _ => Err(CognitoError::invalid_parameter(format!(
                "User pool {user_pool_id} is ambiguous across scopes."
            ))),
        }
    }

    fn finalize_auth_transition(
        &self,
        scope: &CognitoScope,
        state: &mut StoredCognitoState,
        client_id: &CognitoUserPoolClientId,
        transition: AuthTransition,
    ) -> Result<InitiateAuthOutput, CognitoError> {
        match transition {
            AuthTransition::ChallengeNewPasswordRequired {
                user_pool_id,
                username,
            } => {
                let challenge_username = username.clone();
                let session = build_session_id(
                    &user_pool_id,
                    client_id,
                    challenge_username.as_str(),
                    next_auth_session_sequence(state),
                );
                state.auth_sessions.insert(
                    session.clone(),
                    StoredAuthSession {
                        challenge: StoredChallengeName::NewPasswordRequired,
                        client_id: client_id.to_owned(),
                        expires_at: self.now_epoch_seconds()
                            + SESSION_LIFETIME_SECONDS,
                        user_pool_id,
                        username,
                    },
                );

                Ok(InitiateAuthOutput {
                    authentication_result: None,
                    challenge_name: Some(
                        CognitoChallengeName::NewPasswordRequired,
                    ),
                    challenge_parameters: Some(BTreeMap::from([
                        ("USER_ID_FOR_SRP".to_owned(), challenge_username),
                        ("requiredAttributes".to_owned(), "[]".to_owned()),
                        ("userAttributes".to_owned(), "{}".to_owned()),
                    ])),
                    session: Some(session),
                })
            }
            AuthTransition::Authenticated { user_pool_id, username } => {
                let issuer = {
                    let pool = user_pool_mut(state, &user_pool_id)?;
                    ensure_pool_issuer(pool, &self.advertised_edge.current())?
                };
                let user = user(state, &user_pool_id, &username)?.clone();
                let tokens = self.issue_auth_tokens(
                    scope,
                    state,
                    &issuer,
                    client_id,
                    &user_pool_id,
                    &user,
                )?;

                Ok(InitiateAuthOutput {
                    authentication_result: Some(tokens),
                    challenge_name: None,
                    challenge_parameters: None,
                    session: None,
                })
            }
        }
    }

    fn issue_auth_tokens(
        &self,
        scope: &CognitoScope,
        state: &mut StoredCognitoState,
        issuer: &str,
        client_id: &CognitoUserPoolClientId,
        user_pool_id: &CognitoUserPoolId,
        user: &StoredUser,
    ) -> Result<CognitoAuthenticationResult, CognitoError> {
        let now = self.now_epoch_seconds();
        let expires_at = now + ACCESS_TOKEN_LIFETIME_SECONDS;
        let key_id = signing_key_id(user_pool_id);
        let sub = user.attributes.get("sub").cloned().ok_or_else(|| {
            CognitoError::invalid_parameter(format!(
                "User {} is missing a subject identifier.",
                user.username
            ))
        })?;

        let access_jti = next_token_jti(state)?;
        let access_token = encode_jwt(
            &JwtHeader {
                alg: "RS256".to_owned(),
                kid: key_id.clone(),
                typ: "JWT".to_owned(),
            },
            &AccessTokenClaims {
                auth_time: now,
                client_id: client_id.to_string(),
                exp: expires_at,
                iat: now,
                iss: issuer.to_owned(),
                jti: access_jti.clone(),
                sub: sub.clone(),
                token_use: "access".to_owned(),
                username: user.username.clone(),
            },
        )?;
        state.issued_tokens.insert(
            access_jti,
            StoredIssuedToken {
                client_id: client_id.clone(),
                expires_at,
                issued_at: now,
                key_id: key_id.clone(),
                scope: scope_key(scope),
                token_use: StoredTokenUse::Access,
                user_pool_id: user_pool_id.clone(),
                username: user.username.clone(),
            },
        );

        let id_jti = next_token_jti(state)?;
        let id_token = encode_jwt(
            &JwtHeader {
                alg: "RS256".to_owned(),
                kid: key_id,
                typ: "JWT".to_owned(),
            },
            &IdTokenClaims {
                aud: client_id.to_string(),
                auth_time: now,
                custom_claims: user.id_token_claims(),
                exp: expires_at,
                iat: now,
                iss: issuer.to_owned(),
                jti: id_jti.clone(),
                sub,
                token_use: "id".to_owned(),
                username: user.username.clone(),
            },
        )?;
        state.issued_tokens.insert(
            id_jti,
            StoredIssuedToken {
                client_id: client_id.clone(),
                expires_at,
                issued_at: now,
                key_id: signing_key_id(user_pool_id),
                scope: scope_key(scope),
                token_use: StoredTokenUse::Id,
                user_pool_id: user_pool_id.clone(),
                username: user.username.clone(),
            },
        );

        Ok(CognitoAuthenticationResult {
            access_token,
            expires_in: ACCESS_TOKEN_LIFETIME_SECONDS,
            id_token,
            token_type: "Bearer".to_owned(),
        })
    }

    fn verify_access_token(
        &self,
        access_token: &str,
    ) -> Result<VerifiedAccessToken, CognitoError> {
        let claims =
            decode_and_verify_token::<AccessTokenClaims>(access_token)?;
        if claims.token_use != "access" {
            return Err(CognitoError::not_authorized("Invalid access token."));
        }
        let user_pool_id = pool_id_from_issuer(&claims.iss)?;
        let (scope, state) = self.load_state_for_pool_id(&user_pool_id)?;
        let issued_token =
            state.issued_tokens.get(&claims.jti).ok_or_else(|| {
                CognitoError::not_authorized("Invalid access token.")
            })?;
        if issued_token.token_use != StoredTokenUse::Access
            || issued_token.user_pool_id != user_pool_id
            || issued_token.username != claims.username
            || issued_token.client_id != claims.client_id
            || issued_token.key_id != signing_key_id(&user_pool_id)
            || issued_token.scope != scope
            || issued_token.expires_at < self.now_epoch_seconds()
        {
            return Err(CognitoError::not_authorized("Invalid access token."));
        }

        Ok(VerifiedAccessToken {
            scope,
            user_pool_id,
            username: claims.username,
        })
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
pub(crate) struct CognitoStateKey {
    account_id: AccountId,
    region: RegionId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub(crate) struct StoredCognitoState {
    auth_sessions: BTreeMap<String, StoredAuthSession>,
    issued_tokens: BTreeMap<String, StoredIssuedToken>,
    next_auth_session_sequence: u64,
    next_token_sequence: u64,
    next_user_pool_sequence: u64,
    next_user_pool_client_sequence: u64,
    next_user_sub_sequence: u64,
    user_pool_clients: BTreeMap<CognitoUserPoolClientId, StoredUserPoolClient>,
    user_pools: BTreeMap<CognitoUserPoolId, StoredUserPool>,
    users: BTreeMap<StoredUserIdentity, StoredUser>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct StoredUserPool {
    arn: String,
    creation_date: u64,
    id: CognitoUserPoolId,
    issuer_origin: Option<String>,
    last_modified_date: u64,
    name: String,
    status: CognitoUserPoolStatus,
}

impl StoredUserPool {
    fn to_summary(&self) -> CognitoUserPoolSummary {
        CognitoUserPoolSummary {
            creation_date: self.creation_date,
            id: self.id.clone(),
            last_modified_date: self.last_modified_date,
            name: self.name.clone(),
            status: self.status.clone(),
        }
    }

    fn to_user_pool(&self) -> CognitoUserPool {
        CognitoUserPool {
            arn: self.arn.clone(),
            creation_date: self.creation_date,
            id: self.id.clone(),
            last_modified_date: self.last_modified_date,
            name: self.name.clone(),
            status: self.status.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct StoredUserPoolClient {
    client_id: CognitoUserPoolClientId,
    client_name: String,
    client_secret: Option<String>,
    creation_date: u64,
    explicit_auth_flows: Vec<CognitoExplicitAuthFlow>,
    last_modified_date: u64,
    user_pool_id: CognitoUserPoolId,
}

impl StoredUserPoolClient {
    fn to_description(&self) -> CognitoUserPoolClientDescription {
        CognitoUserPoolClientDescription {
            client_id: self.client_id.clone(),
            client_name: self.client_name.clone(),
            user_pool_id: self.user_pool_id.clone(),
        }
    }

    fn to_user_pool_client(&self) -> CognitoUserPoolClient {
        CognitoUserPoolClient {
            client_id: self.client_id.clone(),
            client_name: self.client_name.clone(),
            client_secret: self.client_secret.clone(),
            creation_date: self.creation_date,
            explicit_auth_flows: self.explicit_auth_flows.clone(),
            last_modified_date: self.last_modified_date,
            user_pool_id: self.user_pool_id.clone(),
        }
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
struct StoredUserIdentity {
    user_pool_id: CognitoUserPoolId,
    username: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct StoredUser {
    attributes: BTreeMap<String, String>,
    confirmation_code_hash: Option<String>,
    enabled: bool,
    password_hash: Option<String>,
    temporary_password: bool,
    user_create_date: u64,
    user_last_modified_date: u64,
    user_status: CognitoUserStatus,
    username: String,
}

impl StoredUser {
    fn to_admin_get_user_output(&self) -> AdminGetUserOutput {
        AdminGetUserOutput {
            enabled: self.enabled,
            user_attributes: self.attribute_list(),
            user_create_date: self.user_create_date,
            user_last_modified_date: self.user_last_modified_date,
            username: self.username.clone(),
            user_status: self.user_status.clone(),
        }
    }

    fn to_user(&self) -> CognitoUser {
        CognitoUser {
            attributes: self.attribute_list(),
            enabled: self.enabled,
            user_create_date: self.user_create_date,
            user_last_modified_date: self.user_last_modified_date,
            username: self.username.clone(),
            user_status: self.user_status.clone(),
        }
    }

    fn attribute_list(&self) -> Vec<AttributeType> {
        self.attributes
            .iter()
            .map(|(name, value)| AttributeType {
                name: name.clone(),
                value: value.clone(),
            })
            .collect()
    }

    fn id_token_claims(&self) -> BTreeMap<String, String> {
        let mut claims = self
            .attributes
            .iter()
            .filter(|(name, _)| {
                !matches!(
                    name.as_str(),
                    "aud"
                        | "auth_time"
                        | "cognito:username"
                        | "exp"
                        | "iat"
                        | "iss"
                        | "jti"
                        | "sub"
                        | "token_use"
                )
            })
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect::<BTreeMap<_, _>>();
        claims.insert("cognito:username".to_owned(), self.username.clone());
        claims
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct StoredAuthSession {
    challenge: StoredChallengeName,
    client_id: CognitoUserPoolClientId,
    expires_at: u64,
    user_pool_id: CognitoUserPoolId,
    username: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum StoredChallengeName {
    NewPasswordRequired,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct StoredIssuedToken {
    client_id: CognitoUserPoolClientId,
    expires_at: u64,
    issued_at: u64,
    key_id: String,
    scope: CognitoStateKey,
    token_use: StoredTokenUse,
    user_pool_id: CognitoUserPoolId,
    username: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum StoredTokenUse {
    Access,
    Id,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct JwtHeader {
    alg: String,
    kid: String,
    typ: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct AccessTokenClaims {
    auth_time: u64,
    client_id: String,
    exp: u64,
    iat: u64,
    iss: String,
    jti: String,
    sub: String,
    token_use: String,
    username: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct IdTokenClaims {
    aud: String,
    auth_time: u64,
    #[serde(flatten)]
    custom_claims: BTreeMap<String, String>,
    exp: u64,
    iat: u64,
    iss: String,
    jti: String,
    sub: String,
    #[serde(rename = "cognito:username")]
    username: String,
    token_use: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum AuthTransition {
    Authenticated {
        user_pool_id: CognitoUserPoolId,
        username: String,
    },
    ChallengeNewPasswordRequired {
        user_pool_id: CognitoUserPoolId,
        username: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SupportedAuthFlow {
    AdminUserPasswordAuth,
    RefreshToken,
    RefreshTokenAuth,
    UserAuth,
    UserPasswordAuth,
    UserSrpAuth,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SupportedChallengeName {
    NewPasswordRequired,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct VerifiedAccessToken {
    scope: CognitoStateKey,
    user_pool_id: CognitoUserPoolId,
    username: String,
}

fn scope_key(scope: &CognitoScope) -> CognitoStateKey {
    CognitoStateKey {
        account_id: scope.account_id().clone(),
        region: scope.region().clone(),
    }
}

fn validate_non_empty(
    field_name: &str,
    value: &str,
) -> Result<(), CognitoError> {
    if value.trim().is_empty() {
        return Err(CognitoError::invalid_parameter(format!(
            "{field_name} must not be blank."
        )));
    }

    Ok(())
}

fn user_pool_arn(
    scope: &CognitoScope,
    user_pool_id: &CognitoUserPoolId,
) -> String {
    format!(
        "arn:aws:cognito-idp:{}:{}:userpool/{}",
        scope.region().as_str(),
        scope.account_id().as_str(),
        user_pool_id
    )
}

fn confirmation_delivery_details(
    attributes: &BTreeMap<String, String>,
) -> CognitoCodeDeliveryDetails {
    CognitoCodeDeliveryDetails {
        attribute_name: "email".to_owned(),
        delivery_medium: "EMAIL".to_owned(),
        destination: attributes
            .get("email")
            .cloned()
            .unwrap_or_else(|| "local-000000".to_owned()),
    }
}

fn parse_auth_flow(
    auth_flow: &str,
) -> Result<SupportedAuthFlow, CognitoError> {
    match auth_flow {
        "ADMIN_USER_PASSWORD_AUTH" => {
            Ok(SupportedAuthFlow::AdminUserPasswordAuth)
        }
        "REFRESH_TOKEN" => Ok(SupportedAuthFlow::RefreshToken),
        "REFRESH_TOKEN_AUTH" => Ok(SupportedAuthFlow::RefreshTokenAuth),
        "USER_AUTH" => Ok(SupportedAuthFlow::UserAuth),
        "USER_PASSWORD_AUTH" => Ok(SupportedAuthFlow::UserPasswordAuth),
        "USER_SRP_AUTH" => Ok(SupportedAuthFlow::UserSrpAuth),
        other => Err(CognitoError::unsupported_operation(format!(
            "Auth flow {other} is not supported."
        ))),
    }
}

fn parse_challenge_name(
    challenge_name: &str,
) -> Result<SupportedChallengeName, CognitoError> {
    match challenge_name {
        "NEW_PASSWORD_REQUIRED" => {
            Ok(SupportedChallengeName::NewPasswordRequired)
        }
        other => Err(CognitoError::unsupported_operation(format!(
            "Challenge {other} is not supported."
        ))),
    }
}

fn ensure_client_supports_flow(
    client: &StoredUserPoolClient,
    auth_flow: SupportedAuthFlow,
) -> Result<(), CognitoError> {
    let supported = match auth_flow {
        SupportedAuthFlow::AdminUserPasswordAuth => client
            .explicit_auth_flows
            .contains(&CognitoExplicitAuthFlow::AllowAdminUserPasswordAuth),
        SupportedAuthFlow::UserPasswordAuth => client
            .explicit_auth_flows
            .contains(&CognitoExplicitAuthFlow::AllowUserPasswordAuth),
        SupportedAuthFlow::RefreshToken
        | SupportedAuthFlow::RefreshTokenAuth
        | SupportedAuthFlow::UserAuth
        | SupportedAuthFlow::UserSrpAuth => false,
    };
    if !supported {
        return Err(CognitoError::unsupported_operation(format!(
            "Auth flow {} is not enabled for this client.",
            auth_flow_name(auth_flow)
        )));
    }

    Ok(())
}

fn authenticate_with_password(
    state: &StoredCognitoState,
    user_pool_id: &CognitoUserPoolId,
    _client_id: &CognitoUserPoolClientId,
    auth_flow: SupportedAuthFlow,
    auth_parameters: &BTreeMap<String, String>,
) -> Result<AuthTransition, CognitoError> {
    match auth_flow {
        SupportedAuthFlow::AdminUserPasswordAuth
        | SupportedAuthFlow::UserPasswordAuth => {}
        SupportedAuthFlow::RefreshToken
        | SupportedAuthFlow::RefreshTokenAuth
        | SupportedAuthFlow::UserAuth
        | SupportedAuthFlow::UserSrpAuth => {
            return Err(CognitoError::unsupported_operation(format!(
                "Auth flow {} is not supported.",
                auth_flow_name(auth_flow)
            )));
        }
    }

    validate_auth_parameters(auth_parameters, &["PASSWORD", "USERNAME"])?;
    let username = required_parameter(auth_parameters, "USERNAME")?;
    let password = required_parameter(auth_parameters, "PASSWORD")?;
    let user = user(state, user_pool_id, username)?;

    if !user.enabled {
        return Err(CognitoError::not_authorized("User is disabled."));
    }
    if user.user_status == CognitoUserStatus::Unconfirmed {
        return Err(CognitoError::user_not_confirmed(format!(
            "User {username} is not confirmed."
        )));
    }
    let Some(password_hash) = user.password_hash.as_ref() else {
        return Err(CognitoError::not_authorized(
            "Incorrect username or password.",
        ));
    };
    if *password_hash != hash_password(password) {
        return Err(CognitoError::not_authorized(
            "Incorrect username or password.",
        ));
    }

    if user.temporary_password
        || user.user_status == CognitoUserStatus::ForceChangePassword
    {
        return Ok(AuthTransition::ChallengeNewPasswordRequired {
            user_pool_id: user_pool_id.to_owned(),
            username: username.to_owned(),
        });
    }

    Ok(AuthTransition::Authenticated {
        user_pool_id: user_pool_id.to_owned(),
        username: username.to_owned(),
    })
}

fn auth_flow_name(auth_flow: SupportedAuthFlow) -> &'static str {
    match auth_flow {
        SupportedAuthFlow::AdminUserPasswordAuth => "ADMIN_USER_PASSWORD_AUTH",
        SupportedAuthFlow::RefreshToken => "REFRESH_TOKEN",
        SupportedAuthFlow::RefreshTokenAuth => "REFRESH_TOKEN_AUTH",
        SupportedAuthFlow::UserAuth => "USER_AUTH",
        SupportedAuthFlow::UserPasswordAuth => "USER_PASSWORD_AUTH",
        SupportedAuthFlow::UserSrpAuth => "USER_SRP_AUTH",
    }
}

fn validate_auth_parameters(
    auth_parameters: &BTreeMap<String, String>,
    supported_names: &[&str],
) -> Result<(), CognitoError> {
    reject_unsupported_map_keys(
        "AuthParameters",
        auth_parameters,
        supported_names,
    )
}

fn validate_challenge_responses(
    challenge_responses: &BTreeMap<String, String>,
    supported_names: &[&str],
) -> Result<(), CognitoError> {
    reject_unsupported_map_keys(
        "ChallengeResponses",
        challenge_responses,
        supported_names,
    )
}

fn reject_unsupported_map_keys(
    field_name: &str,
    values: &BTreeMap<String, String>,
    supported_names: &[&str],
) -> Result<(), CognitoError> {
    let unsupported = values
        .keys()
        .filter(|name| !supported_names.contains(&name.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    if !unsupported.is_empty() {
        return Err(CognitoError::invalid_parameter(format!(
            "{field_name} does not support these entries: {}.",
            unsupported.join(", ")
        )));
    }

    Ok(())
}

fn required_parameter<'a>(
    parameters: &'a BTreeMap<String, String>,
    name: &str,
) -> Result<&'a str, CognitoError> {
    let value = parameters
        .get(name)
        .ok_or_else(|| {
            CognitoError::invalid_parameter(format!("{name} is required."))
        })?
        .as_str();
    validate_non_empty(name, value)?;

    Ok(value)
}

fn ensure_pool_issuer(
    pool: &mut StoredUserPool,
    advertised_edge: &aws::AdvertisedEdge,
) -> Result<String, CognitoError> {
    if let Some(issuer_origin) = pool.issuer_origin.clone() {
        return Ok(format!("{issuer_origin}/{}", pool.id));
    }

    let origin = advertised_edge.origin();
    pool.issuer_origin = Some(origin.clone());

    Ok(advertised_edge.cognito_issuer(pool.id.as_str()))
}

fn build_session_id(
    user_pool_id: &CognitoUserPoolId,
    client_id: &CognitoUserPoolClientId,
    username: &str,
    sequence: u64,
) -> String {
    let digest = Sha256::digest(
        format!("session:{user_pool_id}:{client_id}:{username}:{sequence}")
            .as_bytes(),
    );
    hex_encode(&digest)
}

fn next_auth_session_sequence(state: &mut StoredCognitoState) -> u64 {
    state.next_auth_session_sequence += 1;
    state.next_auth_session_sequence
}

fn next_token_jti(
    state: &mut StoredCognitoState,
) -> Result<String, CognitoError> {
    let next = state.next_token_sequence + 1;
    let jti = format!("00000000-0000-0000-0000-{next:012}");
    if state.issued_tokens.contains_key(&jti) {
        return Err(CognitoError::invalid_parameter(format!(
            "Generated duplicate token identifier {jti}."
        )));
    }
    state.next_token_sequence = next;

    Ok(jti)
}

fn encode_jwt<T>(
    header: &JwtHeader,
    claims: &T,
) -> Result<String, CognitoError>
where
    T: Serialize,
{
    let header_json = serde_json::to_vec(header).map_err(|error| {
        CognitoError::invalid_parameter(format!(
            "Failed to serialize JWT header: {error}"
        ))
    })?;
    let claims_json = serde_json::to_vec(claims).map_err(|error| {
        CognitoError::invalid_parameter(format!(
            "Failed to serialize JWT claims: {error}"
        ))
    })?;
    let header_segment = URL_SAFE_NO_PAD.encode(header_json);
    let claims_segment = URL_SAFE_NO_PAD.encode(claims_json);
    let signing_input = format!("{header_segment}.{claims_segment}");
    let key_pair = signing_key_pair()?;
    let mut signature_bytes = vec![0; key_pair.public().modulus_len()];
    key_pair
        .sign(
            &signature::RSA_PKCS1_SHA256,
            &SystemRandom::new(),
            signing_input.as_bytes(),
            &mut signature_bytes,
        )
        .map_err(|_| CognitoError::invalid_parameter("Failed to sign JWT."))?;
    let signature_segment = URL_SAFE_NO_PAD.encode(signature_bytes);

    Ok(format!("{signing_input}.{signature_segment}"))
}

fn decode_and_verify_token<T>(token: &str) -> Result<T, CognitoError>
where
    T: for<'de> Deserialize<'de>,
{
    let (header_segment, claims_segment, signature_segment) =
        jwt_segments(token)?;
    let signing_input = format!("{header_segment}.{claims_segment}");
    let signature = URL_SAFE_NO_PAD
        .decode(signature_segment)
        .map_err(|_| CognitoError::not_authorized("Invalid access token."))?;
    signature::RsaPublicKeyComponents {
        e: signing_public_key_exponent()?,
        n: signing_public_key_modulus()?,
    }
    .verify(
        &signature::RSA_PKCS1_2048_8192_SHA256,
        signing_input.as_bytes(),
        &signature,
    )
    .map_err(|_| CognitoError::not_authorized("Invalid access token."))?;

    let header: JwtHeader = serde_json::from_slice(
        &URL_SAFE_NO_PAD.decode(header_segment).map_err(|_| {
            CognitoError::not_authorized("Invalid access token.")
        })?,
    )
    .map_err(|_| CognitoError::not_authorized("Invalid access token."))?;
    if header.alg != "RS256" {
        return Err(CognitoError::not_authorized("Invalid access token."));
    }

    serde_json::from_slice(
        &URL_SAFE_NO_PAD.decode(claims_segment).map_err(|_| {
            CognitoError::not_authorized("Invalid access token.")
        })?,
    )
    .map_err(|_| CognitoError::not_authorized("Invalid access token."))
}

fn jwt_segments(token: &str) -> Result<(&str, &str, &str), CognitoError> {
    let mut segments = token.split('.');
    let header = segments.next().ok_or_else(|| {
        CognitoError::not_authorized("Invalid access token.")
    })?;
    let claims = segments.next().ok_or_else(|| {
        CognitoError::not_authorized("Invalid access token.")
    })?;
    let signature = segments.next().ok_or_else(|| {
        CognitoError::not_authorized("Invalid access token.")
    })?;
    if segments.next().is_some() {
        return Err(CognitoError::not_authorized("Invalid access token."));
    }

    Ok((header, claims, signature))
}

fn signing_key_id(user_pool_id: &CognitoUserPoolId) -> String {
    user_pool_id.to_string()
}

fn pool_id_from_issuer(
    issuer: &str,
) -> Result<CognitoUserPoolId, CognitoError> {
    issuer
        .rsplit('/')
        .next()
        .filter(|segment| !segment.trim().is_empty())
        .ok_or_else(|| CognitoError::not_authorized("Invalid access token."))
        .and_then(CognitoUserPoolId::new)
}

fn signing_key_pair() -> Result<&'static rsa::KeyPair, CognitoError> {
    match SIGNING_KEY_PAIR.get_or_init(|| {
        let private_key_der = STANDARD
            .decode(SIGNING_KEY_DER_B64)
            .map_err(|error| error.to_string())?;
        rsa::KeyPair::from_pkcs8(&private_key_der)
            .map_err(|_| "static Cognito signing key should parse".to_owned())
    }) {
        Ok(key_pair) => Ok(key_pair),
        Err(message) => Err(CognitoError::invalid_parameter(message.clone())),
    }
}

fn signing_public_key_exponent() -> Result<&'static [u8], CognitoError> {
    match SIGNING_PUBLIC_KEY_EXPONENT.get_or_init(|| {
        URL_SAFE_NO_PAD
            .decode(SIGNING_PUBLIC_KEY_EXPONENT_B64URL)
            .map_err(|error| error.to_string())
    }) {
        Ok(exponent) => Ok(exponent.as_slice()),
        Err(message) => Err(CognitoError::invalid_parameter(message.clone())),
    }
}

fn signing_public_key_modulus() -> Result<&'static [u8], CognitoError> {
    match SIGNING_PUBLIC_KEY_MODULUS.get_or_init(|| {
        URL_SAFE_NO_PAD
            .decode(SIGNING_PUBLIC_KEY_MODULUS_B64URL)
            .map_err(|error| error.to_string())
    }) {
        Ok(modulus) => Ok(modulus.as_slice()),
        Err(message) => Err(CognitoError::invalid_parameter(message.clone())),
    }
}

fn next_user_pool_client_id(
    state: &mut StoredCognitoState,
) -> Result<CognitoUserPoolClientId, CognitoError> {
    let next = state.next_user_pool_client_sequence + 1;
    let client_id = CognitoUserPoolClientId::new(format!("{next:026x}"))?;
    if state.user_pool_clients.contains_key(&client_id) {
        return Err(CognitoError::invalid_parameter(format!(
            "Generated duplicate user pool client identifier {client_id}."
        )));
    }

    state.next_user_pool_client_sequence = next;

    Ok(client_id)
}

fn next_user_sub(
    state: &mut StoredCognitoState,
) -> Result<String, CognitoError> {
    let next = state.next_user_sub_sequence + 1;
    let sub = format!("00000000-0000-0000-0000-{next:012}");
    let exists = state
        .users
        .values()
        .any(|user| user.attributes.get("sub") == Some(&sub));
    if exists {
        return Err(CognitoError::invalid_parameter(format!(
            "Generated duplicate user identifier {sub}."
        )));
    }

    state.next_user_sub_sequence = next;

    Ok(sub)
}

fn normalize_auth_flows(
    explicit_auth_flows: Option<Vec<CognitoExplicitAuthFlow>>,
    use_default: bool,
) -> Result<Vec<CognitoExplicitAuthFlow>, CognitoError> {
    let flows = explicit_auth_flows.unwrap_or_else(|| {
        if use_default {
            DEFAULT_CLIENT_AUTH_FLOWS.to_vec()
        } else {
            Vec::new()
        }
    });
    parse_explicit_auth_flows(&flows)?;

    Ok(flows.into_iter().collect::<BTreeSet<_>>().into_iter().collect())
}

fn paginate<T>(
    items: Vec<T>,
    limit: Option<u32>,
    next_token: Option<&str>,
) -> Result<(Vec<T>, Option<String>), CognitoError> {
    let limit = limit.unwrap_or(MAX_LIST_RESULTS);
    if limit == 0 || limit > MAX_LIST_RESULTS {
        return Err(CognitoError::invalid_parameter(format!(
            "List limits must be between 1 and {MAX_LIST_RESULTS}."
        )));
    }

    let total_items = items.len();
    let start = if let Some(next_token) = next_token {
        next_token.parse::<usize>().map_err(|_| {
            CognitoError::invalid_parameter(
                "The provided pagination token is invalid.",
            )
        })?
    } else {
        0
    };
    if start > total_items || (start == total_items && total_items != 0) {
        return Err(CognitoError::invalid_parameter(
            "The provided pagination token is invalid.",
        ));
    }

    let page_size =
        usize::try_from(limit).unwrap_or(MAX_LIST_RESULTS as usize);
    let end = (start + page_size).min(total_items);
    let paged =
        items.into_iter().skip(start).take(page_size).collect::<Vec<_>>();
    let next_token = (end < total_items).then(|| end.to_string());

    Ok((paged, next_token))
}

fn attribute_map(
    attributes: &[AttributeType],
) -> Result<BTreeMap<String, String>, CognitoError> {
    let mut mapped = BTreeMap::new();

    for attribute in attributes {
        validate_non_empty("Attribute name", &attribute.name)?;
        if attribute.name == "sub" {
            return Err(CognitoError::invalid_parameter(
                "The `sub` attribute is managed by Cognito.",
            ));
        }
        if mapped
            .insert(attribute.name.clone(), attribute.value.clone())
            .is_some()
        {
            return Err(CognitoError::invalid_parameter(format!(
                "Duplicate user attribute {} is not allowed.",
                attribute.name
            )));
        }
    }

    Ok(mapped)
}

fn derived_secret(client_id: &CognitoUserPoolClientId) -> String {
    let digest =
        Sha256::digest(format!("client-secret:{client_id}").as_bytes());
    hex_encode(&digest)
}

fn hash_password(password: &str) -> String {
    let digest = Sha256::digest(password.as_bytes());
    hex_encode(&digest)
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded
            .push(char::from_digit(u32::from(byte >> 4), 16).unwrap_or('0'));
        encoded
            .push(char::from_digit(u32::from(byte & 0x0f), 16).unwrap_or('0'));
    }

    encoded
}

fn user_pool<'a>(
    state: &'a StoredCognitoState,
    user_pool_id: &CognitoUserPoolId,
) -> Result<&'a StoredUserPool, CognitoError> {
    state.user_pools.get(user_pool_id).ok_or_else(|| {
        CognitoError::resource_not_found(format!(
            "User pool {user_pool_id} does not exist."
        ))
    })
}

fn user_pool_mut<'a>(
    state: &'a mut StoredCognitoState,
    user_pool_id: &CognitoUserPoolId,
) -> Result<&'a mut StoredUserPool, CognitoError> {
    state.user_pools.get_mut(user_pool_id).ok_or_else(|| {
        CognitoError::resource_not_found(format!(
            "User pool {user_pool_id} does not exist."
        ))
    })
}

fn user_pool_client<'a>(
    state: &'a StoredCognitoState,
    user_pool_id: &CognitoUserPoolId,
    client_id: &CognitoUserPoolClientId,
) -> Result<&'a StoredUserPoolClient, CognitoError> {
    let client = state.user_pool_clients.get(client_id).ok_or_else(|| {
        CognitoError::resource_not_found(format!(
            "User pool client {client_id} does not exist."
        ))
    })?;
    if &client.user_pool_id != user_pool_id {
        return Err(CognitoError::resource_not_found(format!(
            "User pool client {client_id} does not exist."
        )));
    }

    Ok(client)
}

fn user_pool_client_by_id<'a>(
    state: &'a StoredCognitoState,
    client_id: &CognitoUserPoolClientId,
) -> Result<&'a StoredUserPoolClient, CognitoError> {
    state.user_pool_clients.get(client_id).ok_or_else(|| {
        CognitoError::resource_not_found(format!(
            "User pool client {client_id} does not exist."
        ))
    })
}

fn user_pool_client_mut<'a>(
    state: &'a mut StoredCognitoState,
    user_pool_id: &CognitoUserPoolId,
    client_id: &CognitoUserPoolClientId,
) -> Result<&'a mut StoredUserPoolClient, CognitoError> {
    let client =
        state.user_pool_clients.get_mut(client_id).ok_or_else(|| {
            CognitoError::resource_not_found(format!(
                "User pool client {client_id} does not exist."
            ))
        })?;
    if &client.user_pool_id != user_pool_id {
        return Err(CognitoError::resource_not_found(format!(
            "User pool client {client_id} does not exist."
        )));
    }

    Ok(client)
}

fn user<'a>(
    state: &'a StoredCognitoState,
    user_pool_id: &CognitoUserPoolId,
    username: &str,
) -> Result<&'a StoredUser, CognitoError> {
    let _ = user_pool(state, user_pool_id)?;
    state
        .users
        .get(&StoredUserIdentity {
            user_pool_id: user_pool_id.to_owned(),
            username: username.to_owned(),
        })
        .ok_or_else(|| {
            CognitoError::user_not_found(format!(
                "User {username} does not exist."
            ))
        })
}

fn user_mut<'a>(
    state: &'a mut StoredCognitoState,
    user_pool_id: &CognitoUserPoolId,
    username: &str,
) -> Result<&'a mut StoredUser, CognitoError> {
    let _ = user_pool(state, user_pool_id)?;
    state
        .users
        .get_mut(&StoredUserIdentity {
            user_pool_id: user_pool_id.to_owned(),
            username: username.to_owned(),
        })
        .ok_or_else(|| {
            CognitoError::user_not_found(format!(
                "User {username} does not exist."
            ))
        })
}

fn auth_session_mut<'a>(
    state: &'a mut StoredCognitoState,
    session: &str,
) -> Result<&'a mut StoredAuthSession, CognitoError> {
    state.auth_sessions.get_mut(session).ok_or_else(|| {
        CognitoError::not_authorized("The provided session is not valid.")
    })
}

fn remove_user_artifacts(
    state: &mut StoredCognitoState,
    identity: &StoredUserIdentity,
) {
    state.users.remove(identity);
    state.auth_sessions.retain(|_, session| {
        !(session.user_pool_id == identity.user_pool_id
            && session.username == identity.username)
    });
    state.issued_tokens.retain(|_, token| {
        !(token.user_pool_id == identity.user_pool_id
            && token.username == identity.username)
    });
}

#[cfg(test)]
mod tests {
    use super::{
        AdminCreateUserInput, AdminGetUserInput, AdminInitiateAuthInput,
        AdminSetUserPasswordInput, AdminUpdateUserAttributesInput,
        AttributeType, ChangePasswordInput, CognitoExplicitAuthFlow,
        CognitoScope, CognitoService, CognitoUserPoolClientId,
        CognitoUserPoolId, ConfirmSignUpInput, CreateUserPoolClientInput,
        CreateUserPoolInput, CreateUserPoolOutput, DeleteUserPoolInput,
        DescribeUserPoolInput, DescribeUserPoolOutput, GetUserInput,
        InitiateAuthInput, ListUserPoolClientsInput, ListUserPoolsInput,
        ListUsersInput, RespondToAuthChallengeInput, SignUpInput,
        StoredCognitoState, StoredUserPool, UpdateUserAttributesInput,
        UpdateUserPoolClientInput, UpdateUserPoolInput,
    };
    use aws::{AccountId, Clock, RegionId};
    use std::collections::BTreeMap;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};
    use storage::{InMemoryStorage, StorageError, StorageHandle};

    #[derive(Debug)]
    struct FixedClock {
        now: SystemTime,
    }

    impl FixedClock {
        fn new(seconds: u64) -> Self {
            Self { now: UNIX_EPOCH + Duration::from_secs(seconds) }
        }
    }

    impl Clock for FixedClock {
        fn now(&self) -> SystemTime {
            self.now
        }
    }

    fn scope(region: &str) -> CognitoScope {
        CognitoScope::new(
            "000000000000"
                .parse::<AccountId>()
                .expect("account id should parse"),
            region.parse::<RegionId>().expect("region should parse"),
        )
    }

    fn service() -> CognitoService {
        let store: StorageHandle<_, _> =
            Arc::new(InMemoryStorage::<_, StoredCognitoState>::new());
        CognitoService::with_store(Arc::new(FixedClock::new(100)), store)
    }

    fn user_pool_id(value: &str) -> CognitoUserPoolId {
        CognitoUserPoolId::new(value)
            .expect("test user pool ids should be valid")
    }

    fn client_id(value: &str) -> CognitoUserPoolClientId {
        CognitoUserPoolClientId::new(value)
            .expect("test client ids should be valid")
    }

    fn stored_user_pool(scope: &CognitoScope, id: &str) -> StoredUserPool {
        let id = user_pool_id(id);
        let name = format!("pool-{id}");
        StoredUserPool {
            arn: super::user_pool_arn(scope, &id),
            creation_date: 100,
            id,
            issuer_origin: None,
            last_modified_date: 100,
            name,
            status: super::CognitoUserPoolStatus::Enabled,
        }
    }

    fn stored_user_pool_client(
        user_pool_id: &CognitoUserPoolId,
        client_identifier: &str,
    ) -> super::StoredUserPoolClient {
        let client_id = client_id(client_identifier);
        super::StoredUserPoolClient {
            client_id: client_id.clone(),
            client_name: format!("client-{client_id}"),
            client_secret: None,
            creation_date: 100,
            explicit_auth_flows: vec![
                CognitoExplicitAuthFlow::AllowUserPasswordAuth,
            ],
            last_modified_date: 100,
            user_pool_id: user_pool_id.clone(),
        }
    }

    fn stored_user(username: &str, sub: &str) -> super::StoredUser {
        super::StoredUser {
            attributes: BTreeMap::from([("sub".to_owned(), sub.to_owned())]),
            confirmation_code_hash: None,
            enabled: true,
            password_hash: None,
            temporary_password: false,
            user_create_date: 100,
            user_last_modified_date: 100,
            user_status: super::CognitoUserStatus::Confirmed,
            username: username.to_owned(),
        }
    }

    #[test]
    fn cognito_auth_round_trips_password_tokens_and_oidc_metadata() {
        let service = service();
        let scope = scope("eu-west-2");
        let pool_id = service
            .create_user_pool(
                &scope,
                CreateUserPoolInput { pool_name: "auth-demo".to_owned() },
            )
            .expect("pool should create")
            .user_pool
            .id;
        let client_id = service
            .create_user_pool_client(
                &scope,
                CreateUserPoolClientInput {
                    client_name: "app".to_owned(),
                    explicit_auth_flows: Some(vec![
                        CognitoExplicitAuthFlow::AllowUserPasswordAuth,
                    ]),
                    generate_secret: Some(false),
                    user_pool_id: pool_id.clone(),
                },
            )
            .expect("client should create")
            .user_pool_client
            .client_id;

        let sign_up = service
            .sign_up(
                &scope,
                SignUpInput {
                    client_id: client_id.clone(),
                    password: "Perm1234!".to_owned(),
                    user_attributes: vec![AttributeType {
                        name: "email".to_owned(),
                        value: "user@example.com".to_owned(),
                    }],
                    username: "alice".to_owned(),
                },
            )
            .expect("sign up should succeed");
        assert!(!sign_up.user_confirmed);
        assert!(!sign_up.user_sub.is_empty());

        service
            .confirm_sign_up(
                &scope,
                ConfirmSignUpInput {
                    client_id: client_id.clone(),
                    confirmation_code: "000000".to_owned(),
                    username: "alice".to_owned(),
                },
            )
            .expect("confirm sign up should succeed");

        let auth = service
            .initiate_auth(
                &scope,
                InitiateAuthInput {
                    auth_flow: "USER_PASSWORD_AUTH".to_owned(),
                    auth_parameters: BTreeMap::from([
                        ("PASSWORD".to_owned(), "Perm1234!".to_owned()),
                        ("USERNAME".to_owned(), "alice".to_owned()),
                    ]),
                    client_id: client_id.clone(),
                },
            )
            .expect("password auth should succeed");
        let result = auth
            .authentication_result
            .expect("auth result should issue tokens");
        assert_eq!(result.token_type, "Bearer");

        let user = service
            .get_user(GetUserInput {
                access_token: result.access_token.clone(),
            })
            .expect("access token should resolve the user");
        assert_eq!(user.username, "alice");

        service
            .update_user_attributes(UpdateUserAttributesInput {
                access_token: result.access_token.clone(),
                user_attributes: vec![AttributeType {
                    name: "name".to_owned(),
                    value: "Alice".to_owned(),
                }],
            })
            .expect("token-backed attribute update should succeed");
        service
            .change_password(ChangePasswordInput {
                access_token: result.access_token.clone(),
                previous_password: "Perm1234!".to_owned(),
                proposed_password: "Perm5678!".to_owned(),
            })
            .expect("change password should succeed");

        let open_id = service
            .open_id_configuration(&pool_id)
            .expect("openid configuration should resolve");
        assert_eq!(open_id.issuer, format!("http://localhost:4566/{pool_id}"));
        assert_eq!(
            open_id.jwks_uri,
            format!("http://localhost:4566/{pool_id}/.well-known/jwks.json")
        );
        let jwks = service
            .jwks_document(&pool_id)
            .expect("jwks document should resolve");
        assert_eq!(jwks.keys.len(), 1);
        assert_eq!(pool_id.to_string(), jwks.keys[0].kid);

        let old_password_error = service
            .initiate_auth(
                &scope,
                InitiateAuthInput {
                    auth_flow: "USER_PASSWORD_AUTH".to_owned(),
                    auth_parameters: BTreeMap::from([
                        ("PASSWORD".to_owned(), "Perm1234!".to_owned()),
                        ("USERNAME".to_owned(), "alice".to_owned()),
                    ]),
                    client_id: client_id.clone(),
                },
            )
            .expect_err("old passwords should no longer work");
        assert_eq!(
            old_password_error.to_aws_error().code(),
            "NotAuthorizedException"
        );
        let updated_password = service
            .initiate_auth(
                &scope,
                InitiateAuthInput {
                    auth_flow: "USER_PASSWORD_AUTH".to_owned(),
                    auth_parameters: BTreeMap::from([
                        ("PASSWORD".to_owned(), "Perm5678!".to_owned()),
                        ("USERNAME".to_owned(), "alice".to_owned()),
                    ]),
                    client_id,
                },
            )
            .expect("new password should authenticate");
        assert!(updated_password.authentication_result.is_some());
    }

    #[test]
    fn cognito_auth_requires_new_password_challenge_until_completed() {
        let service = service();
        let scope = scope("eu-west-2");
        let pool_id = service
            .create_user_pool(
                &scope,
                CreateUserPoolInput { pool_name: "auth-demo".to_owned() },
            )
            .expect("pool should create")
            .user_pool
            .id;
        let client_id = service
            .create_user_pool_client(
                &scope,
                CreateUserPoolClientInput {
                    client_name: "admin-app".to_owned(),
                    explicit_auth_flows: Some(vec![
                        CognitoExplicitAuthFlow::AllowAdminUserPasswordAuth,
                    ]),
                    generate_secret: Some(false),
                    user_pool_id: pool_id.clone(),
                },
            )
            .expect("client should create")
            .user_pool_client
            .client_id;

        service
            .admin_create_user(
                &scope,
                AdminCreateUserInput {
                    temporary_password: Some("Temp1234!".to_owned()),
                    user_attributes: Vec::new(),
                    user_pool_id: pool_id.clone(),
                    username: "alice".to_owned(),
                },
            )
            .expect("admin user should create");

        let challenge = service
            .admin_initiate_auth(
                &scope,
                AdminInitiateAuthInput {
                    auth_flow: "ADMIN_USER_PASSWORD_AUTH".to_owned(),
                    auth_parameters: BTreeMap::from([
                        ("PASSWORD".to_owned(), "Temp1234!".to_owned()),
                        ("USERNAME".to_owned(), "alice".to_owned()),
                    ]),
                    client_id: client_id.clone(),
                    user_pool_id: pool_id.clone(),
                },
            )
            .expect("temporary password auth should challenge");
        assert_eq!(
            challenge.challenge_name,
            Some(super::CognitoChallengeName::NewPasswordRequired)
        );
        let session =
            challenge.session.expect("challenge should return session");

        let completed = service
            .respond_to_auth_challenge(
                &scope,
                RespondToAuthChallengeInput {
                    challenge_name: "NEW_PASSWORD_REQUIRED".to_owned(),
                    challenge_responses: BTreeMap::from([
                        ("NEW_PASSWORD".to_owned(), "Perm1234!".to_owned()),
                        ("USERNAME".to_owned(), "alice".to_owned()),
                    ]),
                    client_id,
                    session,
                },
            )
            .expect("challenge should complete");
        assert!(completed.authentication_result.is_some());
        let user = service
            .admin_get_user(
                &scope,
                AdminGetUserInput {
                    user_pool_id: pool_id,
                    username: "alice".to_owned(),
                },
            )
            .expect("user should load after challenge");
        assert_eq!(user.user_status, super::CognitoUserStatus::Confirmed);
    }

    #[test]
    fn cognito_auth_rejects_invalid_confirmation_codes_and_unsupported_flows()
    {
        let service = service();
        let scope = scope("eu-west-2");
        let pool_id = service
            .create_user_pool(
                &scope,
                CreateUserPoolInput { pool_name: "auth-demo".to_owned() },
            )
            .expect("pool should create")
            .user_pool
            .id;
        let client_id = service
            .create_user_pool_client(
                &scope,
                CreateUserPoolClientInput {
                    client_name: "app".to_owned(),
                    explicit_auth_flows: Some(vec![
                        CognitoExplicitAuthFlow::AllowUserPasswordAuth,
                        CognitoExplicitAuthFlow::AllowRefreshTokenAuth,
                    ]),
                    generate_secret: Some(false),
                    user_pool_id: pool_id,
                },
            )
            .expect("client should create")
            .user_pool_client
            .client_id;

        service
            .sign_up(
                &scope,
                SignUpInput {
                    client_id: client_id.clone(),
                    password: "Perm1234!".to_owned(),
                    user_attributes: Vec::new(),
                    username: "alice".to_owned(),
                },
            )
            .expect("sign up should succeed");
        let wrong_code = service
            .confirm_sign_up(
                &scope,
                ConfirmSignUpInput {
                    client_id: client_id.clone(),
                    confirmation_code: "111111".to_owned(),
                    username: "alice".to_owned(),
                },
            )
            .expect_err("wrong confirmation codes should fail");
        assert_eq!(wrong_code.to_aws_error().code(), "CodeMismatchException");

        let unsupported = service
            .initiate_auth(
                &scope,
                InitiateAuthInput {
                    auth_flow: "REFRESH_TOKEN_AUTH".to_owned(),
                    auth_parameters: BTreeMap::from([(
                        "REFRESH_TOKEN".to_owned(),
                        "anything".to_owned(),
                    )]),
                    client_id,
                },
            )
            .expect_err("refresh-token auth should fail explicitly");
        assert_eq!(unsupported.to_aws_error().code(), "UnsupportedOperation");
    }

    #[test]
    fn cognito_control_round_trips_pool_client_and_admin_user_state() {
        let service = service();
        let scope = scope("eu-west-2");

        let CreateUserPoolOutput { user_pool } = service
            .create_user_pool(
                &scope,
                CreateUserPoolInput { pool_name: "demo".to_owned() },
            )
            .expect("pool should create");
        let DescribeUserPoolOutput { user_pool: described_pool } = service
            .describe_user_pool(
                &scope,
                DescribeUserPoolInput { user_pool_id: user_pool.id.clone() },
            )
            .expect("pool should describe");

        assert_eq!(described_pool.name, "demo");

        let updated_pool = service
            .update_user_pool(
                &scope,
                UpdateUserPoolInput {
                    pool_name: Some("demo-updated".to_owned()),
                    user_pool_id: user_pool.id.clone(),
                },
            )
            .expect("pool should update");
        assert_eq!(updated_pool.user_pool.name, "demo-updated");

        let created_client = service
            .create_user_pool_client(
                &scope,
                CreateUserPoolClientInput {
                    client_name: "app".to_owned(),
                    explicit_auth_flows: Some(vec![
                        CognitoExplicitAuthFlow::AllowAdminUserPasswordAuth,
                        CognitoExplicitAuthFlow::AllowUserPasswordAuth,
                    ]),
                    generate_secret: Some(true),
                    user_pool_id: user_pool.id.clone(),
                },
            )
            .expect("client should create");
        assert!(created_client.user_pool_client.client_secret.is_some());

        let listed_clients = service
            .list_user_pool_clients(
                &scope,
                ListUserPoolClientsInput {
                    max_results: Some(60),
                    next_token: None,
                    user_pool_id: user_pool.id.clone(),
                },
            )
            .expect("clients should list");
        assert_eq!(listed_clients.user_pool_clients.len(), 1);

        let updated_client = service
            .update_user_pool_client(
                &scope,
                UpdateUserPoolClientInput {
                    client_id: created_client
                        .user_pool_client
                        .client_id
                        .clone(),
                    client_name: Some("app-updated".to_owned()),
                    explicit_auth_flows: Some(vec![
                        CognitoExplicitAuthFlow::AllowRefreshTokenAuth,
                    ]),
                    user_pool_id: user_pool.id.clone(),
                },
            )
            .expect("client should update");
        assert_eq!(updated_client.user_pool_client.client_name, "app-updated");
        assert_eq!(
            updated_client.user_pool_client.explicit_auth_flows,
            vec![CognitoExplicitAuthFlow::AllowRefreshTokenAuth]
        );

        let created_user = service
            .admin_create_user(
                &scope,
                AdminCreateUserInput {
                    temporary_password: Some("Temp1234!".to_owned()),
                    user_attributes: vec![AttributeType {
                        name: "email".to_owned(),
                        value: "user@example.com".to_owned(),
                    }],
                    user_pool_id: user_pool.id.clone(),
                    username: "alice".to_owned(),
                },
            )
            .expect("user should create");
        assert_eq!(
            created_user.user.user_status,
            super::CognitoUserStatus::ForceChangePassword
        );
        assert!(
            created_user
                .user
                .attributes
                .iter()
                .any(|attribute| attribute.name == "sub")
        );

        let fetched_user = service
            .admin_get_user(
                &scope,
                AdminGetUserInput {
                    user_pool_id: user_pool.id.clone(),
                    username: "alice".to_owned(),
                },
            )
            .expect("user should load");
        assert_eq!(fetched_user.username, "alice");

        service
            .admin_update_user_attributes(
                &scope,
                AdminUpdateUserAttributesInput {
                    user_attributes: vec![AttributeType {
                        name: "name".to_owned(),
                        value: "Alice".to_owned(),
                    }],
                    user_pool_id: user_pool.id.clone(),
                    username: "alice".to_owned(),
                },
            )
            .expect("attributes should update");
        service
            .admin_set_user_password(
                &scope,
                AdminSetUserPasswordInput {
                    password: "Perm1234!".to_owned(),
                    permanent: true,
                    user_pool_id: user_pool.id.clone(),
                    username: "alice".to_owned(),
                },
            )
            .expect("password should update");

        let listed_users = service
            .list_users(
                &scope,
                ListUsersInput {
                    limit: Some(60),
                    pagination_token: None,
                    user_pool_id: user_pool.id.clone(),
                },
            )
            .expect("users should list");
        assert_eq!(listed_users.users.len(), 1);
        assert_eq!(listed_users.users[0].username, "alice");
        assert_eq!(
            listed_users.users[0].user_status,
            super::CognitoUserStatus::Confirmed
        );
        assert!(
            listed_users.users[0]
                .attributes
                .iter()
                .any(|attribute| attribute.name == "name")
        );

        service
            .delete_user_pool(
                &scope,
                DeleteUserPoolInput { user_pool_id: user_pool.id.clone() },
            )
            .expect("pool should delete");
        let pools = service
            .list_user_pools(
                &scope,
                ListUserPoolsInput { max_results: Some(60), next_token: None },
            )
            .expect("pools should list");
        assert!(pools.user_pools.is_empty());
    }

    #[test]
    fn cognito_control_is_account_and_region_scoped() {
        let service = service();
        let west = scope("eu-west-2");
        let east = scope("us-east-1");

        let pool = service
            .create_user_pool(
                &west,
                CreateUserPoolInput { pool_name: "demo".to_owned() },
            )
            .expect("pool should create")
            .user_pool;

        let error = service
            .describe_user_pool(
                &east,
                DescribeUserPoolInput { user_pool_id: pool.id.clone() },
            )
            .expect_err("cross-region access should fail");
        assert_eq!(error.to_aws_error().code(), "ResourceNotFoundException");
    }

    #[test]
    fn cognito_control_rejects_duplicate_usernames_and_reserved_attributes() {
        let service = service();
        let scope = scope("eu-west-2");
        let pool_id = service
            .create_user_pool(
                &scope,
                CreateUserPoolInput { pool_name: "demo".to_owned() },
            )
            .expect("pool should create")
            .user_pool
            .id;

        let reserved = service
            .admin_create_user(
                &scope,
                AdminCreateUserInput {
                    temporary_password: None,
                    user_attributes: vec![AttributeType {
                        name: "sub".to_owned(),
                        value: "custom".to_owned(),
                    }],
                    user_pool_id: pool_id.clone(),
                    username: "alice".to_owned(),
                },
            )
            .expect_err("reserved attributes should fail");
        assert_eq!(
            reserved.to_aws_error().code(),
            "InvalidParameterException"
        );

        service
            .admin_create_user(
                &scope,
                AdminCreateUserInput {
                    temporary_password: None,
                    user_attributes: Vec::new(),
                    user_pool_id: pool_id.clone(),
                    username: "alice".to_owned(),
                },
            )
            .expect("user should create");
        let duplicate = service
            .admin_create_user(
                &scope,
                AdminCreateUserInput {
                    temporary_password: None,
                    user_attributes: Vec::new(),
                    user_pool_id: pool_id,
                    username: "alice".to_owned(),
                },
            )
            .expect_err("duplicate usernames should fail");
        assert_eq!(duplicate.to_aws_error().code(), "UsernameExistsException");
    }

    #[test]
    fn cognito_control_rejects_invalid_pagination_and_auth_flow_mixes() {
        let service = service();
        let scope = scope("eu-west-2");
        let pool_id = service
            .create_user_pool(
                &scope,
                CreateUserPoolInput { pool_name: "demo".to_owned() },
            )
            .expect("pool should create")
            .user_pool
            .id;

        let invalid_page = service
            .list_users(
                &scope,
                ListUsersInput {
                    limit: Some(0),
                    pagination_token: None,
                    user_pool_id: pool_id.clone(),
                },
            )
            .expect_err("zero limits should fail");
        assert_eq!(
            invalid_page.to_aws_error().code(),
            "InvalidParameterException"
        );

        let mixed_flows = service
            .create_user_pool_client(
                &scope,
                CreateUserPoolClientInput {
                    client_name: "app".to_owned(),
                    explicit_auth_flows: Some(vec![
                        CognitoExplicitAuthFlow::AllowUserPasswordAuth,
                        CognitoExplicitAuthFlow::UserPasswordAuthLegacy,
                    ]),
                    generate_secret: Some(false),
                    user_pool_id: pool_id,
                },
            )
            .expect_err("mixed auth flow families should fail");
        assert_eq!(
            mixed_flows.to_aws_error().code(),
            "InvalidParameterException"
        );
    }

    #[test]
    fn cognito_control_covers_public_edge_paths_and_defaults() {
        let service = service();
        let scope = scope("eu-west-2");
        let pool_id = service
            .create_user_pool(
                &scope,
                CreateUserPoolInput { pool_name: "demo".to_owned() },
            )
            .expect("pool should create")
            .user_pool
            .id;

        let unchanged_pool = service
            .update_user_pool(
                &scope,
                UpdateUserPoolInput {
                    pool_name: None,
                    user_pool_id: pool_id.clone(),
                },
            )
            .expect("pool should support empty updates");
        assert_eq!(unchanged_pool.user_pool.name, "demo");

        let default_client = service
            .create_user_pool_client(
                &scope,
                CreateUserPoolClientInput {
                    client_name: "default".to_owned(),
                    explicit_auth_flows: None,
                    generate_secret: None,
                    user_pool_id: pool_id.clone(),
                },
            )
            .expect("client should create with default auth flows");
        assert_eq!(
            default_client.user_pool_client.explicit_auth_flows,
            vec![
                CognitoExplicitAuthFlow::AllowCustomAuth,
                CognitoExplicitAuthFlow::AllowRefreshTokenAuth,
                CognitoExplicitAuthFlow::AllowUserSrpAuth,
            ]
        );
        assert!(default_client.user_pool_client.client_secret.is_none());

        let invalid_pool_token = service
            .list_user_pools(
                &scope,
                ListUserPoolsInput {
                    max_results: Some(1),
                    next_token: Some("bad-token".to_owned()),
                },
            )
            .expect_err("invalid pool tokens should fail");
        assert_eq!(
            invalid_pool_token.to_aws_error().code(),
            "InvalidParameterException"
        );

        let out_of_range_pool_token = service
            .list_user_pools(
                &scope,
                ListUserPoolsInput {
                    max_results: Some(1),
                    next_token: Some("1".to_owned()),
                },
            )
            .expect_err("out-of-range pool tokens should fail");
        assert_eq!(
            out_of_range_pool_token.to_aws_error().code(),
            "InvalidParameterException"
        );

        let invalid_client_token = service
            .list_user_pool_clients(
                &scope,
                ListUserPoolClientsInput {
                    max_results: Some(1),
                    next_token: Some("bad-token".to_owned()),
                    user_pool_id: pool_id.clone(),
                },
            )
            .expect_err("invalid client tokens should fail");
        assert_eq!(
            invalid_client_token.to_aws_error().code(),
            "InvalidParameterException"
        );

        let unchanged_client = service
            .update_user_pool_client(
                &scope,
                UpdateUserPoolClientInput {
                    client_id: default_client
                        .user_pool_client
                        .client_id
                        .clone(),
                    client_name: None,
                    explicit_auth_flows: None,
                    user_pool_id: pool_id.clone(),
                },
            )
            .expect("client should support empty updates");
        assert_eq!(unchanged_client.user_pool_client.client_name, "default");
        assert_eq!(
            unchanged_client.user_pool_client.explicit_auth_flows,
            default_client.user_pool_client.explicit_auth_flows
        );

        let missing_client = service
            .update_user_pool_client(
                &scope,
                UpdateUserPoolClientInput {
                    client_id: client_id("missing-client"),
                    client_name: Some("ignored".to_owned()),
                    explicit_auth_flows: None,
                    user_pool_id: pool_id.clone(),
                },
            )
            .expect_err("missing clients should fail");
        assert_eq!(
            missing_client.to_aws_error().code(),
            "ResourceNotFoundException"
        );

        let missing_pool = service
            .delete_user_pool(
                &scope,
                DeleteUserPoolInput {
                    user_pool_id: user_pool_id("missing-pool"),
                },
            )
            .expect_err("missing pools should fail");
        assert_eq!(
            missing_pool.to_aws_error().code(),
            "ResourceNotFoundException"
        );

        service
            .admin_create_user(
                &scope,
                AdminCreateUserInput {
                    temporary_password: None,
                    user_attributes: Vec::new(),
                    user_pool_id: pool_id.clone(),
                    username: "alice".to_owned(),
                },
            )
            .expect("user should create");
        service
            .admin_set_user_password(
                &scope,
                AdminSetUserPasswordInput {
                    password: "Temp1234!".to_owned(),
                    permanent: false,
                    user_pool_id: pool_id.clone(),
                    username: "alice".to_owned(),
                },
            )
            .expect("temporary passwords should be allowed");
        let updated_user = service
            .admin_get_user(
                &scope,
                AdminGetUserInput {
                    user_pool_id: pool_id,
                    username: "alice".to_owned(),
                },
            )
            .expect("user should load");
        assert_eq!(
            updated_user.user_status,
            super::CognitoUserStatus::ForceChangePassword
        );
    }

    #[test]
    fn cognito_control_reports_duplicate_generated_pool_identifiers() {
        let store: StorageHandle<_, _> =
            Arc::new(InMemoryStorage::<_, StoredCognitoState>::new());
        let scope = scope("eu-west-2");
        store
            .put(
                super::scope_key(&scope),
                StoredCognitoState {
                    auth_sessions: BTreeMap::new(),
                    issued_tokens: BTreeMap::new(),
                    next_auth_session_sequence: 0,
                    next_token_sequence: 0,
                    next_user_pool_sequence: 0,
                    next_user_pool_client_sequence: 0,
                    next_user_sub_sequence: 0,
                    user_pool_clients: BTreeMap::new(),
                    user_pools: BTreeMap::from([(
                        user_pool_id("eu-west-2_000000001"),
                        StoredUserPool {
                            arn: "arn:aws:cognito-idp:eu-west-2:000000000000:userpool/eu-west-2_000000001".to_owned(),
                            creation_date: 100,
                            id: user_pool_id("eu-west-2_000000001"),
                            issuer_origin: None,
                            last_modified_date: 100,
                            name: "seed".to_owned(),
                            status: super::CognitoUserPoolStatus::Enabled,
                        },
                    )]),
                    users: BTreeMap::new(),
                },
            )
            .expect("state should seed");
        let service =
            CognitoService::with_store(Arc::new(FixedClock::new(100)), store);

        let created = service
            .create_user_pool(
                &scope,
                CreateUserPoolInput { pool_name: "dup".to_owned() },
            )
            .expect("pool id generation should skip duplicate identifiers");
        assert_eq!(created.user_pool.id, "eu-west-2_000000002");
    }

    #[test]
    fn cognito_control_private_helpers_cover_remaining_edges() {
        let auth_flow_cases = [
            (CognitoExplicitAuthFlow::AdminNoSrpAuth, "ADMIN_NO_SRP_AUTH"),
            (
                CognitoExplicitAuthFlow::CustomAuthFlowOnly,
                "CUSTOM_AUTH_FLOW_ONLY",
            ),
            (
                CognitoExplicitAuthFlow::UserPasswordAuthLegacy,
                "USER_PASSWORD_AUTH",
            ),
            (
                CognitoExplicitAuthFlow::AllowAdminUserPasswordAuth,
                "ALLOW_ADMIN_USER_PASSWORD_AUTH",
            ),
            (CognitoExplicitAuthFlow::AllowCustomAuth, "ALLOW_CUSTOM_AUTH"),
            (
                CognitoExplicitAuthFlow::AllowRefreshTokenAuth,
                "ALLOW_REFRESH_TOKEN_AUTH",
            ),
            (CognitoExplicitAuthFlow::AllowUserAuth, "ALLOW_USER_AUTH"),
            (
                CognitoExplicitAuthFlow::AllowUserPasswordAuth,
                "ALLOW_USER_PASSWORD_AUTH",
            ),
            (CognitoExplicitAuthFlow::AllowUserSrpAuth, "ALLOW_USER_SRP_AUTH"),
        ];
        for (flow, expected) in auth_flow_cases {
            assert_eq!(flow.as_str(), expected);
            let json = serde_json::to_string(&flow)
                .expect("auth flow should serialize");
            assert_eq!(json, format!("\"{expected}\""));
            let decoded =
                serde_json::from_str::<CognitoExplicitAuthFlow>(&json)
                    .expect("auth flow should deserialize");
            assert_eq!(decoded, flow);
        }
        let invalid_flow =
            serde_json::from_str::<CognitoExplicitAuthFlow>("\"UNSUPPORTED\"")
                .expect_err("unknown auth flows should fail");
        assert!(
            invalid_flow
                .to_string()
                .contains("Unsupported ExplicitAuthFlows value")
        );

        let blank = super::validate_non_empty("PoolName", "   ")
            .expect_err("blank values should fail");
        assert_eq!(blank.to_aws_error().code(), "InvalidParameterException");

        let duplicate_attributes = super::attribute_map(&[
            AttributeType {
                name: "email".to_owned(),
                value: "one@example.com".to_owned(),
            },
            AttributeType {
                name: "email".to_owned(),
                value: "two@example.com".to_owned(),
            },
        ])
        .expect_err("duplicate attributes should fail");
        assert_eq!(
            duplicate_attributes.to_aws_error().code(),
            "InvalidParameterException"
        );

        let scope = scope("eu-west-2");
        let mut duplicate_state = StoredCognitoState {
            next_user_pool_client_sequence: 0,
            next_user_sub_sequence: 0,
            user_pool_clients: BTreeMap::from([(
                client_id(&format!("{:026x}", 1)),
                stored_user_pool_client(
                    &user_pool_id("pool-a"),
                    &format!("{:026x}", 1),
                ),
            )]),
            users: BTreeMap::from([(
                super::StoredUserIdentity {
                    user_pool_id: user_pool_id("pool-a"),
                    username: "seed".to_owned(),
                },
                stored_user("seed", "00000000-0000-0000-0000-000000000001"),
            )]),
            ..Default::default()
        };
        let duplicate_client =
            super::next_user_pool_client_id(&mut duplicate_state)
                .expect_err("duplicate generated clients should fail");
        assert_eq!(
            duplicate_client.to_aws_error().code(),
            "InvalidParameterException"
        );
        let duplicate_user = super::next_user_sub(&mut duplicate_state)
            .expect_err("duplicate generated users should fail");
        assert_eq!(
            duplicate_user.to_aws_error().code(),
            "InvalidParameterException"
        );

        let pool_a = stored_user_pool(&scope, "eu-west-2_pool-a");
        let pool_b = stored_user_pool(&scope, "eu-west-2_pool-b");
        let client = stored_user_pool_client(&pool_a.id, "client-a");
        let mut lookup_state = StoredCognitoState {
            user_pools: BTreeMap::from([
                (pool_a.id.clone(), pool_a.clone()),
                (pool_b.id.clone(), pool_b.clone()),
            ]),
            user_pool_clients: BTreeMap::from([(
                client.client_id.clone(),
                client.clone(),
            )]),
            ..Default::default()
        };

        let missing_pool = super::user_pool_mut(
            &mut lookup_state,
            &user_pool_id("missing-pool"),
        )
        .expect_err("missing pools should fail");
        assert_eq!(
            missing_pool.to_aws_error().code(),
            "ResourceNotFoundException"
        );

        let missing_client = super::user_pool_client(
            &lookup_state,
            &pool_a.id,
            &client_id("missing-client"),
        )
        .expect_err("missing clients should fail");
        assert_eq!(
            missing_client.to_aws_error().code(),
            "ResourceNotFoundException"
        );

        let wrong_pool_client = super::user_pool_client(
            &lookup_state,
            &pool_b.id,
            &client.client_id,
        )
        .expect_err("cross-pool clients should fail");
        assert_eq!(
            wrong_pool_client.to_aws_error().code(),
            "ResourceNotFoundException"
        );

        let missing_mut_client = super::user_pool_client_mut(
            &mut lookup_state,
            &pool_a.id,
            &client_id("missing-client"),
        )
        .expect_err("missing mutable clients should fail");
        assert_eq!(
            missing_mut_client.to_aws_error().code(),
            "ResourceNotFoundException"
        );

        let wrong_pool_mut_client = super::user_pool_client_mut(
            &mut lookup_state,
            &pool_b.id,
            &client.client_id,
        )
        .expect_err("cross-pool mutable clients should fail");
        assert_eq!(
            wrong_pool_mut_client.to_aws_error().code(),
            "ResourceNotFoundException"
        );

        let missing_user =
            super::user_mut(&mut lookup_state, &pool_a.id, "ghost")
                .expect_err("missing mutable users should fail");
        assert_eq!(
            missing_user.to_aws_error().code(),
            "UserNotFoundException"
        );
    }

    #[test]
    fn cognito_control_maps_errors_to_explicit_aws_shapes() {
        let not_found = super::CognitoError::resource_not_found("missing");
        let unsupported = super::CognitoError::unsupported_operation("gap");
        let store = super::CognitoError::from(StorageError::DecodeSnapshot {
            path: PathBuf::from("state/cognito.json"),
            details: "boom".to_owned(),
        });

        assert_eq!(
            not_found.to_aws_error().code(),
            "ResourceNotFoundException"
        );
        assert_eq!(unsupported.to_aws_error().code(), "UnsupportedOperation");
        assert_eq!(store.to_aws_error().code(), "InternalErrorException");
    }
}
