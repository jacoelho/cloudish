mod catalog;
mod dispatch;

pub(crate) use catalog::{
    detect_generic_protocol, json_target, query_action_matches_version,
    rest_json_service, service_from_query_action, service_from_smithy_id,
    supports_protocol,
};
pub(crate) use dispatch::{
    dispatch_json, dispatch_query, dispatch_rest_json, dispatch_smithy,
};
