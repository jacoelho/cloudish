#![cfg_attr(
    test,
    allow(clippy::arithmetic_side_effects, clippy::indexing_slicing)
)]

pub mod errors;
pub mod planning;
pub mod providers;
pub mod scope;
pub mod stacks;
mod state;
pub mod template;

pub use errors::CloudFormationError;
pub use planning::{
    CloudFormationPlanInput, CloudFormationPlannedOutput,
    CloudFormationPlannedResource, CloudFormationStackPlan,
};
pub use providers::{
    CloudFormationDependencies, CloudFormationDynamoDbPort,
    CloudFormationIamPort, CloudFormationKmsPort, CloudFormationLambdaPort,
    CloudFormationS3Port, CloudFormationSecretsManagerPort,
    CloudFormationSnsPort, CloudFormationSqsPort, CloudFormationSsmPort,
};
pub use scope::CloudFormationScope;
pub use stacks::{
    CloudFormationChangeSetDescription, CloudFormationChangeSetSummary,
    CloudFormationChangeSummary, CloudFormationCreateChangeSetInput,
    CloudFormationCreateChangeSetOutput, CloudFormationService,
    CloudFormationStackDescription, CloudFormationStackEvent,
    CloudFormationStackOperationInput, CloudFormationStackOperationOutput,
    CloudFormationStackOutput, CloudFormationStackResourceDescription,
    CloudFormationStackResourceSummary,
};
pub use template::{
    CloudFormationTemplateParameter, ValidateTemplateInput,
    ValidateTemplateOutput,
};
