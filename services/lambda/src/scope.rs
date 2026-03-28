use aws::{AccountId, RegionId};
use storage::{StorageScope, StorageScopeProvider};

pub use crate::{functions::LambdaEnvironment, state::LambdaPackageType};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct LambdaScope {
    account_id: AccountId,
    region: RegionId,
}

impl LambdaScope {
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

impl StorageScopeProvider for LambdaScope {
    fn storage_scope(&self) -> StorageScope {
        StorageScope::new(self.account_id.clone(), self.region.clone())
    }
}
