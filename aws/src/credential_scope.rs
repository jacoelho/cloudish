use crate::{RegionId, ServiceName};

/// Shared AWS `SigV4` credential scope used across auth and edge routing.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CredentialScope {
    region: RegionId,
    service: ServiceName,
}

impl CredentialScope {
    pub fn new(region: RegionId, service: ServiceName) -> Self {
        Self { region, service }
    }

    pub fn region(&self) -> &RegionId {
        &self.region
    }

    pub fn service(&self) -> ServiceName {
        self.service
    }
}

#[cfg(test)]
mod tests {
    use super::CredentialScope;
    use crate::ServiceName;

    #[test]
    fn build_credential_scope_from_typed_region_and_service() {
        let scope = CredentialScope::new(
            "eu-west-2".parse().expect("region should parse"),
            ServiceName::Sqs,
        );

        assert_eq!(scope.region().as_str(), "eu-west-2");
        assert_eq!(scope.service(), ServiceName::Sqs);
    }
}
