pub mod access_keys;
pub mod documents;
pub mod errors;
pub mod groups;
pub mod instance_profiles;
pub mod policies;
pub mod roles;
pub mod scope;
mod state;
pub mod users;

pub use access_keys::{IamAccessKey, IamAccessKeyMetadata};
pub use documents::{IamTag, InlinePolicy, InlinePolicyInput};
pub use errors::IamError;
pub use groups::{CreateGroupInput, GroupDetails, IamGroup};
pub use instance_profiles::{CreateInstanceProfileInput, IamInstanceProfile};
pub use policies::{
    AttachedPolicy, CreatePolicyInput, CreatePolicyVersionInput, IamPolicy,
    IamPolicyVersion,
};
pub use roles::{CreateRoleInput, IamRole};
pub use scope::IamScope;
pub use state::IamService;
pub use users::{CreateUserInput, IamUser};
