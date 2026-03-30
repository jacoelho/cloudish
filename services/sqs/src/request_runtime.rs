use crate::{
    ReceiveMessageInput, ReceivedMessage, SqsError, SqsQueueIdentity,
    SqsService,
};

#[derive(Clone)]
pub struct SqsRequestRuntime {
    service: SqsService,
}

impl SqsRequestRuntime {
    pub fn new(service: SqsService) -> Self {
        Self { service }
    }

    /// # Errors
    ///
    /// Returns [`SqsError`] when the queue does not exist or the receive
    /// parameters are invalid.
    pub fn receive_message(
        &self,
        queue: &SqsQueueIdentity,
        input: ReceiveMessageInput,
        is_cancelled: &(dyn Fn() -> bool + Send + Sync),
    ) -> Result<Vec<ReceivedMessage>, SqsError> {
        self.service.receive_message_with_cancellation(
            queue,
            input,
            is_cancelled,
        )
    }
}
