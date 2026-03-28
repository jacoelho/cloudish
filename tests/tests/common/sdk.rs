#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    clippy::unreachable,
    clippy::indexing_slicing,
    clippy::assertions_on_constants,
    clippy::missing_panics_doc,
    clippy::missing_errors_doc
)]
use auth::{BOOTSTRAP_ACCESS_KEY_ID, BOOTSTRAP_SECRET_ACCESS_KEY};
use aws_config::{BehaviorVersion, Region, SdkConfig};
use aws_credential_types::Credentials;
use aws_credential_types::provider::SharedCredentialsProvider;
use aws_smithy_runtime::client::http::hyper_014::HyperClientBuilder;
use hyper::client::HttpConnector;

pub(crate) struct SdkSmokeTarget {
    access_key_id: String,
    endpoint_url: String,
    region: String,
    secret_access_key: String,
    session_token: Option<String>,
}

#[allow(dead_code)]
impl SdkSmokeTarget {
    pub(crate) fn new(
        endpoint_url: impl Into<String>,
        region: impl Into<String>,
    ) -> Self {
        Self {
            access_key_id: BOOTSTRAP_ACCESS_KEY_ID.to_owned(),
            endpoint_url: endpoint_url.into(),
            region: region.into(),
            secret_access_key: BOOTSTRAP_SECRET_ACCESS_KEY.to_owned(),
            session_token: None,
        }
    }

    pub(crate) fn access_key_id(&self) -> &str {
        &self.access_key_id
    }

    pub(crate) fn endpoint_url(&self) -> &str {
        &self.endpoint_url
    }

    pub(crate) fn region(&self) -> &str {
        &self.region
    }

    pub(crate) fn secret_access_key(&self) -> &str {
        &self.secret_access_key
    }

    #[allow(dead_code)]
    pub(crate) fn with_credentials(
        mut self,
        access_key_id: impl Into<String>,
        secret_access_key: impl Into<String>,
        session_token: Option<impl Into<String>>,
    ) -> Self {
        self.access_key_id = access_key_id.into();
        self.secret_access_key = secret_access_key.into();
        self.session_token = session_token.map(Into::into);
        self
    }

    pub(crate) async fn load(&self) -> SdkConfig {
        let credentials = Credentials::new(
            self.access_key_id.clone(),
            self.secret_access_key.clone(),
            self.session_token.clone(),
            None,
            "cloudish-sdk-smoke",
        );
        let mut connector = HttpConnector::new();
        connector.enforce_http(false);
        let http_client = HyperClientBuilder::new().build(connector);

        aws_config::defaults(BehaviorVersion::v2024_03_28())
            .credentials_provider(SharedCredentialsProvider::new(credentials))
            .http_client(http_client)
            .endpoint_url(self.endpoint_url.clone())
            .region(Region::new(self.region.clone()))
            .load()
            .await
    }
}
