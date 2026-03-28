use aws::{AccountId, RegionId};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct SsmScope {
    account_id: AccountId,
    region: RegionId,
}

impl SsmScope {
    pub fn new(account_id: AccountId, region: RegionId) -> Self {
        Self { account_id, region }
    }

    pub fn account_id(&self) -> &AccountId {
        &self.account_id
    }

    pub fn region(&self) -> &RegionId {
        &self.region
    }
}
