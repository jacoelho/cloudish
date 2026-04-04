#![deny(clippy::arithmetic_side_effects, clippy::indexing_slicing)]
mod attributes;
mod errors;
mod fifo;
mod message_attributes;
mod messages;
mod queues;
mod redrive;
#[doc(hidden)]
pub mod request_runtime;
mod scope;

#[cfg(test)]
#[allow(clippy::arithmetic_side_effects, clippy::indexing_slicing)]
mod tests;

pub use attributes::{AddPermissionInput, RemovePermissionInput};
pub use errors::SqsError;
pub use message_attributes::MessageAttributeValue;
pub use messages::{
    BatchFailure, ChangeMessageVisibilityBatchEntryInput,
    ChangeMessageVisibilityBatchOutput, ChangeMessageVisibilityBatchSuccess,
    DeleteMessageBatchEntryInput, DeleteMessageBatchOutput,
    DeleteMessageBatchSuccess, ListDeadLetterSourceQueuesInput,
    ListQueuesInput, PaginatedDeadLetterSourceQueues, PaginatedQueues,
    ReceiveMessageInput, ReceivedMessage, SendMessageBatchEntryInput,
    SendMessageBatchOutput, SendMessageBatchSuccess, SendMessageInput,
    SendMessageOutput,
};
pub use queues::{
    CreateQueueInput, SequentialSqsIdentifierSource, SqsIdentifierSource,
    SqsService,
};
pub use redrive::{
    CancelMessageMoveTaskInput, CancelMessageMoveTaskOutput,
    ListMessageMoveTasksInput, ListMessageMoveTasksOutput,
    ListMessageMoveTasksResultEntry, StartMessageMoveTaskInput,
    StartMessageMoveTaskOutput,
};
pub use scope::{SqsQueueIdentity, SqsScope};
