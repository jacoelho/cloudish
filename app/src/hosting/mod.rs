#![forbid(unsafe_code)]

mod plan;
pub(crate) use plan::HostingPlan;
pub use plan::{EDGE_HOST_ENV, EDGE_PORT_ENV, HostingPlanError};
