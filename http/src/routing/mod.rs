mod catalog;
mod dispatch;

pub(crate) use catalog::{
    ScopedQueryValidation, detect_generic_protocol, json_target,
    query_action_matches_version, resolve_unsigned_query_service,
    rest_json_service, service_from_smithy_id, supports_protocol,
    validate_query_for_service,
};
pub(crate) use dispatch::{
    dispatch_json, dispatch_query, dispatch_rest_json, dispatch_smithy,
};
