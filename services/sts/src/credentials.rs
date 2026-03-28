use crate::StsError;
use aws::{AccountId, Arn, IamResourceTag, SessionCredentialRecord};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionCredentials {
    pub access_key_id: String,
    pub expiration: String,
    pub secret_access_key: String,
    pub session_token: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssumedRoleUser {
    pub arn: Arn,
    pub assumed_role_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FederatedUser {
    pub arn: Arn,
    pub federated_user_id: String,
}

#[derive(Debug, Default)]
pub(crate) struct StsWorld {
    next_session_id: u64,
    sessions: BTreeMap<String, StoredSessionCredential>,
}

pub(crate) struct SessionIssueInput {
    pub(crate) account_id: AccountId,
    pub(crate) duration_seconds: u32,
    pub(crate) principal_arn: Arn,
    pub(crate) principal_id: String,
    pub(crate) session_tags: Vec<IamResourceTag>,
    pub(crate) transitive_tag_keys: BTreeSet<String>,
}

#[derive(Debug, Clone)]
struct StoredSessionCredential {
    access_key_id: String,
    account_id: AccountId,
    expires_at_epoch_seconds: u64,
    principal_arn: Arn,
    principal_id: String,
    secret_access_key: String,
    session_tags: Vec<IamResourceTag>,
    session_token: String,
    transitive_tag_keys: BTreeSet<String>,
}

pub(crate) fn find_session_credential(
    state: &Mutex<StsWorld>,
    access_key_id: &str,
) -> Option<SessionCredentialRecord> {
    let guard = state.lock().unwrap_or_else(|poison| poison.into_inner());
    let record = guard.sessions.get(access_key_id)?;

    Some(SessionCredentialRecord {
        access_key_id: record.access_key_id.clone(),
        account_id: record.account_id.clone(),
        expires_at_epoch_seconds: record.expires_at_epoch_seconds,
        principal_arn: record.principal_arn.clone(),
        principal_id: record.principal_id.clone(),
        secret_access_key: record.secret_access_key.clone(),
        session_tags: record.session_tags.clone(),
        session_token: record.session_token.clone(),
        transitive_tag_keys: record.transitive_tag_keys.clone(),
    })
}

pub(crate) fn issue_session(
    state: &Mutex<StsWorld>,
    time_source: &(dyn Fn() -> SystemTime + Send + Sync),
    input: SessionIssueInput,
) -> Result<SessionCredentials, StsError> {
    let expires_at_epoch_seconds = now_epoch_seconds(time_source())?
        .saturating_add(u64::from(input.duration_seconds));
    let expiration = format_timestamp(expires_at_epoch_seconds)?;
    let mut guard = state.lock().unwrap_or_else(|poison| poison.into_inner());
    guard.next_session_id += 1;
    let session_id = guard.next_session_id;
    let access_key_id = format!("ASIA{session_id:016}");
    let secret_access_key = format!("cloudishsessionsecret{session_id:020}");
    let session_token = format!("cloudish-session-token-{session_id:020}");

    guard.sessions.insert(
        access_key_id.clone(),
        StoredSessionCredential {
            access_key_id: access_key_id.clone(),
            account_id: input.account_id,
            expires_at_epoch_seconds,
            principal_arn: input.principal_arn,
            principal_id: input.principal_id,
            secret_access_key: secret_access_key.clone(),
            session_tags: input.session_tags,
            session_token: session_token.clone(),
            transitive_tag_keys: input.transitive_tag_keys,
        },
    );

    Ok(SessionCredentials {
        access_key_id,
        expiration,
        secret_access_key,
        session_token,
    })
}

fn format_timestamp(epoch_seconds: u64) -> Result<String, StsError> {
    let timestamp = OffsetDateTime::from_unix_timestamp(epoch_seconds as i64)
        .map_err(|_| StsError::Validation {
            message:
                "The requested session expiration could not be represented."
                    .to_owned(),
        })?;
    timestamp.format(&Rfc3339).map_err(|_| StsError::Validation {
        message: "The requested session expiration could not be formatted."
            .to_owned(),
    })
}

fn now_epoch_seconds(time: SystemTime) -> Result<u64, StsError> {
    time.duration_since(UNIX_EPOCH).map(|duration| duration.as_secs()).map_err(
        |_| StsError::Validation {
            message: "System time is earlier than the Unix epoch.".to_owned(),
        },
    )
}
