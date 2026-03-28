use crate::{AccountId, Arn, IamResourceTag};
use std::collections::BTreeSet;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionCredentialRecord {
    pub access_key_id: String,
    pub account_id: AccountId,
    pub expires_at_epoch_seconds: u64,
    pub principal_arn: Arn,
    pub principal_id: String,
    pub secret_access_key: String,
    pub session_tags: Vec<IamResourceTag>,
    pub session_token: String,
    pub transitive_tag_keys: BTreeSet<String>,
}

pub trait SessionCredentialLookup {
    fn find_session_credential(
        &self,
        access_key_id: &str,
    ) -> Option<SessionCredentialRecord>;
}

#[cfg(test)]
mod tests {
    use super::{SessionCredentialLookup, SessionCredentialRecord};
    use crate::{AccountId, Arn, IamResourceTag};
    use std::collections::BTreeSet;

    #[derive(Debug)]
    struct StaticLookup {
        record: SessionCredentialRecord,
    }

    impl SessionCredentialLookup for StaticLookup {
        fn find_session_credential(
            &self,
            access_key_id: &str,
        ) -> Option<SessionCredentialRecord> {
            (self.record.access_key_id == access_key_id)
                .then_some(self.record.clone())
        }
    }

    fn sample_record() -> SessionCredentialRecord {
        SessionCredentialRecord {
            access_key_id: "ASIA0000000000000001".to_owned(),
            account_id: "123456789012"
                .parse::<AccountId>()
                .expect("account id should parse"),
            expires_at_epoch_seconds: 1_700_000_000,
            principal_arn:
                "arn:aws:sts::123456789012:assumed-role/demo/session"
                    .parse::<Arn>()
                    .expect("STS ARN should parse"),
            principal_id: "AROA1234567890EXAMPLE:session".to_owned(),
            secret_access_key: "temporary-secret-key".to_owned(),
            session_tags: vec![IamResourceTag {
                key: "team".to_owned(),
                value: "platform".to_owned(),
            }],
            session_token: "temporary-session-token".to_owned(),
            transitive_tag_keys: BTreeSet::from(["team".to_owned()]),
        }
    }

    #[test]
    fn session_credential_lookup_returns_matching_records() {
        let lookup = StaticLookup { record: sample_record() };

        let record = lookup
            .find_session_credential("ASIA0000000000000001")
            .expect("known session credential should resolve");

        assert_eq!(record.account_id.as_str(), "123456789012");
        assert_eq!(
            record.principal_arn.to_string(),
            "arn:aws:sts::123456789012:assumed-role/demo/session"
        );
        assert_eq!(record.principal_id, "AROA1234567890EXAMPLE:session");
        assert_eq!(record.session_tags.len(), 1);
        assert!(record.transitive_tag_keys.contains("team"));
    }

    #[test]
    fn session_credential_lookup_rejects_unknown_access_keys() {
        let lookup = StaticLookup { record: sample_record() };

        assert!(
            lookup.find_session_credential("ASIA0000000000009999").is_none()
        );
    }
}
