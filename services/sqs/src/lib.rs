mod attributes;
mod errors;
mod fifo;
mod messages;
mod queues;
mod redrive;
#[doc(hidden)]
pub mod request_runtime;
mod scope;

#[cfg(test)]
mod tests;

pub use errors::SqsError;
pub use messages::{
    BatchFailure, ChangeMessageVisibilityBatchEntryInput,
    ChangeMessageVisibilityBatchOutput, ChangeMessageVisibilityBatchSuccess,
    DeleteMessageBatchEntryInput, DeleteMessageBatchOutput,
    DeleteMessageBatchSuccess, ReceiveMessageInput, ReceivedMessage,
    SendMessageBatchEntryInput, SendMessageBatchOutput,
    SendMessageBatchSuccess, SendMessageInput, SendMessageOutput,
};
pub use queues::{
    CreateQueueInput, SequentialSqsIdentifierSource, SqsIdentifierSource,
    SqsService,
};
pub use redrive::{StartMessageMoveTaskInput, StartMessageMoveTaskOutput};
pub use scope::{SqsQueueIdentity, SqsScope};
