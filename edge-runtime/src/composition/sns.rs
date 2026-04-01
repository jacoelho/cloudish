#[cfg(feature = "sns")]
use aws::{HttpForwardRequest, HttpForwarder, SharedAdvertisedEdge};
#[cfg(feature = "sns")]
use base64::{Engine as _, engine::general_purpose::STANDARD};
#[cfg(feature = "sns")]
use lambda::{InvokeInput, LambdaInvocationType, LambdaScope, LambdaService};
#[cfg(feature = "sns")]
use rsa::{RsaPrivateKey, pkcs1v15::Pkcs1v15Sign, pkcs8::DecodePrivateKey};
#[cfg(feature = "sns")]
use sha1::{Digest, Sha1};
#[cfg(feature = "sns")]
use sns::{
    ConfirmationDelivery, DeliveryEndpoint, PlannedDelivery,
    SnsDeliveryTransport, SnsHttpRequest, SnsHttpSigner, SnsIdentifierSource,
    SnsService,
};
#[cfg(feature = "sns")]
use sqs::{SendMessageInput, SqsService};
#[cfg(feature = "sns")]
use std::sync::{Arc, OnceLock};
#[cfg(feature = "sns")]
use std::time::SystemTime;

#[cfg(feature = "sns")]
#[derive(Clone, Default)]
pub struct SnsServiceDependencies {
    pub advertised_edge: SharedAdvertisedEdge,
    pub http_forwarder: Option<Arc<dyn HttpForwarder + Send + Sync>>,
    pub http_signer: Option<Arc<dyn SnsHttpSigner + Send + Sync>>,
    pub lambda: Option<LambdaService>,
    pub sqs: Option<SqsService>,
}

#[cfg(feature = "sns")]
pub fn build_sns_service(
    advertised_edge: SharedAdvertisedEdge,
    time_source: Arc<dyn Fn() -> SystemTime + Send + Sync>,
    identifier_source: Arc<dyn SnsIdentifierSource + Send + Sync>,
    mut dependencies: SnsServiceDependencies,
) -> SnsService {
    dependencies.advertised_edge = advertised_edge;
    if dependencies.http_signer.is_none() {
        dependencies.http_signer = Some(Arc::new(CloudishSnsHttpSigner::new(
            dependencies.advertised_edge.clone(),
        )));
    }
    SnsService::with_transport(
        time_source,
        identifier_source,
        Arc::new(SnsDeliveryDispatcher { dependencies }),
    )
}

#[cfg(feature = "sns")]
const CLOUDISH_SNS_SIGNING_PRIVATE_KEY_PEM: &str = r#"-----BEGIN PRIVATE KEY-----
MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCzlgpV4C+kMkWm
fHDmvMVXB29yPqVC96A3pnUFGOwFnwqR4CLPtjTf5u8iFAc3UwQiHfK6LPBJN+Mg
DOwHGJS3i19O993Ca9HKYZqjf6MqrNJPUQe0fUmjPafw6EGFiDC22CrdDrzTCDYE
R2Th6i8hc7mr4bfQAibdQMoUUbJNqic2amiZhd4BFQXozXBQ2aja08kZZzlR83rg
cL6HyQ9cNQss8O3fO1zIUXl7ffMLL3TQ7El9GJlSg/KTttfYcPt1kclYLSwT90CE
0hBnN8RpJOyDwHPhtunhjzvcdHGDLb2F1FBUlZIxtpMjtxapf3KeYhAGvpWV7gS1
q6GY5agFAgMBAAECggEALVMee6sPyxynCIxawFl/YuYtAgP+mMa/qJv559Xw58BK
niOYFZ1yfdoem5a7dYKdxfCSDNv/rzMMP1ATl/zjt+lUni0fyoyEz9PPgBlcOI6S
q9MTI0IFvk32323273k+dj9bnhw0mvx1CaJtOzlsOMCo6VEYH8cTQP8zoWo3GrN9
VEJkYYMBwAhB5Mi5e2vhKLN4XUcFrK8tFfbabohdCuP6oOrXfldOaCumjXO3tJtQ
HW3CWcOlR74HWTPkw4/QxUMg2puRlZcSHiOzjlVfyFvDprxozbhKdlFsFsSiziLO
d3m0zN09KdZKFwnVxl+2mL6oUwK82k3eh55gnzslVwKBgQDds+s4Laf5aSOj5pIe
19HOLr+uEi2GrqXRjFiTYB4EfBJrqPKZu/hUk9sqfBw3zI5kLfdKAlWazhL9ESxQ
vx1w54t8O5TdRW0NkPs7Yl8cN7Th5fhMKc8OHlE3FuczymI9ddsAhACnt9Tx3J8E
NZZ66+9UhjykEnrZLZ3UWXaY+wKBgQDPXi0ei/Xc7oWjxUHyM2qizGKW2XQUEjkG
tTD65iAiP/Y2ClP1Guo+VDR1Ibit/7jRqfpVUMWvwXxzDKHiHu2Gry2idu+0U6gR
FdB5x9AudDKazy8bzoQwwPoFazVCkdpHgyEHlunG2q8bHdk63j79doREWTPPH2D8
qLhLYJLy/wKBgCE5+8C5pvkMNtkzjyasNbdu7i9KbiRHPHbBT+0WdKk7Zw9XjLRZ
pYgXeLtPSnNaZuTAttUSsH248MOYtUmMuv7W1OLTkyXuZ7+mwOBPh+2Us7k/XA0e
HvgAty9IcXIjnMGVTjMvlWGNfY6aAAMDfQADKCVE0QXN9zdhTMwsdEfNAoGATuHC
RBZ1pl9Nkujclyeb7uXUsxFxKJlt+/E8+pRDsQOnwxLWsSxV4vPhKJV1TSszwP3p
7j5VlPADSTiK9BtTu6Izt9OKh4wzKJylu02ZEbK99UnO38MFYg5mjV0k23fkEsP8
8ogj0bMqXSRTmCMmzwAgfGd6X9XN7Q65XGMWQz0CgYEAk+nyf5oyYThIDmvhkE8W
m9BL7zkiFKUZ6S6jChN6H+H6E1woD08Li5mH98mVE8NWUVqKoXnROApBWnYlkaeS
yRp+ZEUsuGEnfYvlG3XV4sRRFBNTxpj4QsNLQCGM+UyL7L7W5zDX39izaydjKsmU
u1wVSdLMaZ0bh9EzaTJKtVI=
-----END PRIVATE KEY-----
"#;

#[cfg(feature = "sns")]
const CLOUDISH_SNS_SIGNING_CERT_PEM: &str = r#"-----BEGIN CERTIFICATE-----
MIIDGzCCAgOgAwIBAgIUY6l51sYMiDRC4gv5mXbgxtLw6YEwDQYJKoZIhvcNAQEL
BQAwHTEbMBkGA1UEAwwSY2xvdWRpc2gtc25zLmxvY2FsMB4XDTI2MDMzMDEzMzA0
MloXDTM2MDMyNzEzMzA0MlowHTEbMBkGA1UEAwwSY2xvdWRpc2gtc25zLmxvY2Fs
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAs5YKVeAvpDJFpnxw5rzF
Vwdvcj6lQvegN6Z1BRjsBZ8KkeAiz7Y03+bvIhQHN1MEIh3yuizwSTfjIAzsBxiU
t4tfTvfdwmvRymGao3+jKqzST1EHtH1Joz2n8OhBhYgwttgq3Q680wg2BEdk4eov
IXO5q+G30AIm3UDKFFGyTaonNmpomYXeARUF6M1wUNmo2tPJGWc5UfN64HC+h8kP
XDULLPDt3ztcyFF5e33zCy900OxJfRiZUoPyk7bX2HD7dZHJWC0sE/dAhNIQZzfE
aSTsg8Bz4bbp4Y873HRxgy29hdRQVJWSMbaTI7cWqX9ynmIQBr6Vle4EtauhmOWo
BQIDAQABo1MwUTAdBgNVHQ4EFgQUnAPQcNWbNSTQUQ3JOe839WgYbUMwHwYDVR0j
BBgwFoAUnAPQcNWbNSTQUQ3JOe839WgYbUMwDwYDVR0TAQH/BAUwAwEB/zANBgkq
hkiG9w0BAQsFAAOCAQEAPBeSNojBJqpJF6+5H0+L/MzpojgeBH88vvV5WXYBoNUO
4mQYIjZpLJjkUXCoEFO6RxB6OpHkbQn+xbaiKHhOaP71ED9yZnlGW1BMDjrUP9qQ
jSqPKXfO1yypci4hlFMJMouPve5IeYkQlmXDbmCg3SAxcmTK80yGaU3KUOZCrSBQ
hSKjQhXg++tMRuRKD9EkD+kfdQ0ZAvMPnap8IRfD2A0lS/oIKz2OMzWnqu2XQazI
OXREKq3QG7ltAClZxBQUxpNL1Inpd/HbOs5333ddLBAN2BXP99a/fLgLjenueHGh
G/lNp5qXpLwhj+zMgs4QwyMmkLTyLy9hnwg0BS7XFg==
-----END CERTIFICATE-----
"#;

#[cfg(feature = "sns")]
pub fn cloudish_sns_signing_cert_pem() -> &'static [u8] {
    CLOUDISH_SNS_SIGNING_CERT_PEM.as_bytes()
}

#[cfg(feature = "sns")]
pub(crate) fn cloudish_sns_signing_key() -> &'static RsaPrivateKey {
    static KEY: OnceLock<RsaPrivateKey> = OnceLock::new();

    KEY.get_or_init(|| {
        RsaPrivateKey::from_pkcs8_pem(CLOUDISH_SNS_SIGNING_PRIVATE_KEY_PEM)
            .expect("embedded SNS signing key should parse")
    })
}

#[cfg(feature = "sns")]
#[derive(Clone)]
pub(crate) struct CloudishSnsHttpSigner {
    advertised_edge: SharedAdvertisedEdge,
}

#[cfg(feature = "sns")]
impl CloudishSnsHttpSigner {
    pub(crate) fn new(advertised_edge: SharedAdvertisedEdge) -> Self {
        Self { advertised_edge }
    }
}

#[cfg(feature = "sns")]
impl SnsHttpSigner for CloudishSnsHttpSigner {
    fn sign(&self, string_to_sign: &str) -> String {
        let digest = Sha1::digest(string_to_sign.as_bytes());
        let signature = cloudish_sns_signing_key()
            .sign(Pkcs1v15Sign::new::<Sha1>(), &digest)
            .expect("embedded SNS signing key should sign");

        STANDARD.encode(signature)
    }

    fn signing_cert_url(&self) -> String {
        self.advertised_edge.current().sns_signing_cert_url()
    }
}

#[cfg(feature = "sns")]
#[derive(Clone)]
struct SnsDeliveryDispatcher {
    dependencies: SnsServiceDependencies,
}

#[cfg(feature = "sns")]
impl SnsDeliveryTransport for SnsDeliveryDispatcher {
    fn deliver_confirmation(
        &self,
        delivery: &ConfirmationDelivery,
        message_id: String,
        timestamp: String,
    ) {
        let Some(forwarder) = self.dependencies.http_forwarder.as_ref() else {
            return;
        };
        let advertised_edge = self.dependencies.advertised_edge.current();
        let request = delivery.http_request(
            &message_id,
            &timestamp,
            &advertised_edge,
            self.http_signer().as_ref(),
        );
        let _ = forwarder.forward(&self.http_forward_request(
            delivery.endpoint.endpoint.clone(),
            delivery.endpoint.path.clone(),
            &request,
        ));
    }

    fn deliver_notification(&self, delivery: &PlannedDelivery) {
        let advertised_edge = self.dependencies.advertised_edge.current();
        match &delivery.endpoint {
            DeliveryEndpoint::Http(parsed) => {
                let Some(forwarder) =
                    self.dependencies.http_forwarder.as_ref()
                else {
                    return;
                };
                let request = delivery.payload.http_request(
                    delivery.raw_message_delivery,
                    &advertised_edge,
                    self.http_signer().as_ref(),
                );
                let _ = forwarder.forward(&self.http_forward_request(
                    parsed.endpoint.clone(),
                    parsed.path.clone(),
                    &request,
                ));
            }
            DeliveryEndpoint::Lambda(arn) => {
                let Some(lambda) = self.dependencies.lambda.as_ref() else {
                    return;
                };
                let Some(account_id) = arn.account_id().cloned() else {
                    return;
                };
                let Some(region) = arn.region().cloned() else {
                    return;
                };
                let _ = lambda.invoke(
                    &LambdaScope::new(account_id, region),
                    &arn.to_string(),
                    None,
                    InvokeInput {
                        invocation_type: LambdaInvocationType::Event,
                        payload: delivery.payload.lambda_event(
                            &advertised_edge,
                            self.http_signer().as_ref(),
                        ),
                    },
                );
            }
            DeliveryEndpoint::Sqs(queue_arn) => {
                let Some(sqs) = self.dependencies.sqs.as_ref() else {
                    return;
                };
                let Ok(queue) = sqs::SqsQueueIdentity::from_arn(queue_arn)
                else {
                    return;
                };
                let _ = sqs.send_message(
                    &queue,
                    SendMessageInput {
                        body: delivery.payload.sqs_body(
                            delivery.raw_message_delivery,
                            &advertised_edge,
                            self.http_signer().as_ref(),
                        ),
                        delay_seconds: None,
                        message_deduplication_id: delivery
                            .payload
                            .message_deduplication_id
                            .clone(),
                        message_group_id: delivery
                            .payload
                            .message_group_id
                            .clone(),
                    },
                );
            }
        }
    }
}

#[cfg(feature = "sns")]
impl SnsDeliveryDispatcher {
    fn http_forward_request(
        &self,
        endpoint: aws::Endpoint,
        path: String,
        request: &SnsHttpRequest,
    ) -> HttpForwardRequest {
        let mut forward = HttpForwardRequest::new(endpoint, "POST", path)
            .with_body(request.body().to_vec());
        for (name, value) in request.headers() {
            forward = forward.with_header(name.clone(), value.clone());
        }

        forward
    }

    fn http_signer(&self) -> Arc<dyn SnsHttpSigner + Send + Sync> {
        self.dependencies.http_signer.clone().unwrap_or_else(|| {
            Arc::new(CloudishSnsHttpSigner::new(
                self.dependencies.advertised_edge.clone(),
            ))
        })
    }
}
