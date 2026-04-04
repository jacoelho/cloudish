use crate::errors::SqsError;
use crate::messages::{
    MessageRecord, PendingQueueMove, ReceiveOutcome, ReceiveSelectors,
    SendMessageInput, SendMessageOutput,
};
use crate::queues::QueueRecord;
use std::collections::BTreeSet;

pub(crate) const DEDUPLICATION_WINDOW_MILLIS: u64 = 5 * 60 * 1_000;
const MAX_FIFO_IDENTIFIER_LENGTH: usize = 128;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DeduplicationRecord {
    pub(crate) expires_at_millis: u64,
    pub(crate) message_id: String,
    pub(crate) sequence_number: Option<String>,
}

impl QueueRecord {
    pub(crate) fn send_fifo_message(
        &mut self,
        input: SendMessageInput,
        now_millis: u64,
        message_id: String,
        md5_of_message_body: String,
        md5_of_message_attributes: Option<String>,
        md5_of_message_system_attributes: Option<String>,
    ) -> Result<SendMessageOutput, SqsError> {
        if input.delay_seconds.unwrap_or(0) > 0 {
            return Err(SqsError::InvalidParameterValue {
                message: format!(
                    "Value {} for parameter DelaySeconds is invalid. Reason: The request include parameter that is not valid for this queue type.",
                    input.delay_seconds.unwrap_or(0)
                ),
            });
        }

        let message_group_id = input.message_group_id.ok_or_else(|| {
            SqsError::MissingParameter {
                message:
                    "The request must contain the parameter MessageGroupId."
                        .to_owned(),
            }
        })?;
        validate_fifo_identifier(&message_group_id, "MessageGroupId")?;
        self.prune_expired_deduplication_records(now_millis);
        let deduplication_id = if let Some(deduplication_id) =
            input.message_deduplication_id
        {
            validate_fifo_identifier(
                &deduplication_id,
                "MessageDeduplicationId",
            )?;
            deduplication_id
        } else if self.content_based_deduplication_enabled() {
            sha256_hex(&input.body)
        } else {
            return Err(SqsError::InvalidParameterValue {
                    message: "The queue should either have ContentBasedDeduplication enabled or MessageDeduplicationId provided explicitly".to_owned(),
                });
        };

        if let Some(existing) =
            self.deduplication_records.get(&deduplication_id)
            && existing.expires_at_millis > now_millis
        {
            return Ok(SendMessageOutput {
                md5_of_message_body,
                md5_of_message_attributes,
                md5_of_message_system_attributes,
                message_id: existing.message_id.clone(),
                sequence_number: existing.sequence_number.clone(),
            });
        }

        let sequence_number = Some(self.next_sequence_number());
        let visible_at_millis = now_millis.saturating_add(
            u64::from(self.queue_delay_seconds()).saturating_mul(1_000),
        );
        self.messages.push_back(MessageRecord {
            body: input.body,
            dead_letter_source_arn: None,
            deleted: false,
            deduplication_id: Some(deduplication_id.clone()),
            first_receive_timestamp_millis: None,
            issued_receipt_handles: BTreeSet::new(),
            latest_receipt_handle: None,
            md5_of_body: md5_of_message_body.clone(),
            md5_of_message_attributes: md5_of_message_attributes.clone(),
            message_attributes: input.message_attributes,
            message_group_id: Some(message_group_id),
            message_id: message_id.clone(),
            message_system_attributes: input.message_system_attributes,
            receive_count: 0,
            sent_timestamp_millis: now_millis,
            sequence_number: sequence_number.clone(),
            visible_at_millis,
        });
        self.deduplication_records.insert(
            deduplication_id,
            DeduplicationRecord {
                expires_at_millis: now_millis
                    .saturating_add(DEDUPLICATION_WINDOW_MILLIS),
                message_id: message_id.clone(),
                sequence_number: sequence_number.clone(),
            },
        );

        Ok(SendMessageOutput {
            md5_of_message_body,
            md5_of_message_attributes,
            md5_of_message_system_attributes,
            message_id,
            sequence_number,
        })
    }

    pub(crate) fn receive_fifo_messages(
        &mut self,
        max_number_of_messages: u32,
        now_millis: u64,
        selectors: &ReceiveSelectors,
        visibility_timeout: u32,
        next_receipt_handle: &mut dyn FnMut() -> String,
    ) -> ReceiveOutcome {
        let mut outcome = ReceiveOutcome::default();
        let blocked_groups = self.blocked_fifo_groups(now_millis);
        let queue_arn = self.queue_arn();
        let queue_identity = self.identity.clone();
        let max_receive_count = self
            .redrive_policy_document()
            .map(|policy| policy.max_receive_count);
        let mut ordered_groups = Vec::new();

        for message in &self.messages {
            let Some(group_id) = message.message_group_id.as_ref() else {
                continue;
            };
            if !message.is_receivable(now_millis)
                || blocked_groups.contains(group_id)
                || ordered_groups.contains(group_id)
            {
                continue;
            }
            ordered_groups.push(group_id.clone());
        }

        for group_id in ordered_groups {
            let candidate_indexes = self
                .messages
                .iter()
                .enumerate()
                .filter_map(|(index, message)| {
                    (message.message_group_id.as_deref()
                        == Some(group_id.as_str()))
                    .then_some(index)
                })
                .collect::<Vec<_>>();

            for index in candidate_indexes {
                if outcome.received.len() == max_number_of_messages as usize {
                    return outcome;
                }

                let Some(message) = self.messages.get_mut(index) else {
                    continue;
                };
                if !message.is_receivable(now_millis) {
                    continue;
                }

                let would_exceed_receive_limit = max_receive_count
                    .is_some_and(|limit| {
                        message
                            .receive_count
                            .checked_add(1)
                            .is_none_or(|count| count > limit)
                    });
                if would_exceed_receive_limit {
                    outcome.dead_letter_messages.push(PendingQueueMove {
                        message: message
                            .staged_for_queue_move(Some(queue_arn.clone())),
                        source_message_index: index,
                    });
                    continue;
                }

                outcome.received.push(message.receive(
                    now_millis,
                    selectors,
                    &queue_identity,
                    visibility_timeout,
                    next_receipt_handle(),
                ));
            }
        }

        outcome
    }

    pub(crate) fn blocked_fifo_groups(
        &self,
        now_millis: u64,
    ) -> BTreeSet<String> {
        self.messages
            .iter()
            .filter(|message| {
                !message.deleted
                    && message.visible_at_millis > now_millis
                    && message.receive_count > 0
            })
            .filter_map(|message| message.message_group_id.clone())
            .collect()
    }

    pub(crate) fn prune_expired_deduplication_records(
        &mut self,
        now_millis: u64,
    ) {
        self.deduplication_records
            .retain(|_, record| record.expires_at_millis > now_millis);
    }

    pub(crate) fn is_fifo(&self) -> bool {
        self.attributes.get("FifoQueue").is_some_and(|value| value == "true")
    }

    pub(crate) fn content_based_deduplication_enabled(&self) -> bool {
        self.attributes
            .get("ContentBasedDeduplication")
            .is_some_and(|value| value == "true")
    }

    pub(crate) fn next_sequence_number(&mut self) -> String {
        self.next_sequence_number =
            self.next_sequence_number.saturating_add(1);
        self.next_sequence_number.to_string()
    }
}

pub(crate) fn validate_fifo_identifier(
    value: &str,
    parameter_name: &str,
) -> Result<(), SqsError> {
    if value.is_empty()
        || value.len() > MAX_FIFO_IDENTIFIER_LENGTH
        || !value.chars().all(|character| {
            character.is_ascii_alphanumeric()
                || matches!(
                    character,
                    '!' | '"'
                        | '#'
                        | '$'
                        | '%'
                        | '&'
                        | '\''
                        | '('
                        | ')'
                        | '*'
                        | '+'
                        | ','
                        | '-'
                        | '.'
                        | '/'
                        | ':'
                        | ';'
                        | '<'
                        | '='
                        | '>'
                        | '?'
                        | '@'
                        | '['
                        | '\\'
                        | ']'
                        | '^'
                        | '_'
                        | '`'
                        | '{'
                        | '|'
                        | '}'
                        | '~'
                )
        })
    {
        return Err(SqsError::InvalidParameterValue {
            message: format!(
                "Value {value} for parameter {parameter_name} is invalid."
            ),
        });
    }

    Ok(())
}

fn sha256_hex(value: &str) -> String {
    use sha2::Digest;

    let digest = sha2::Sha256::digest(value.as_bytes());
    format!("{digest:x}")
}
