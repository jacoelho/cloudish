use crate::SsmError;
use crate::parameters::{
    ParameterName, StoredParameter, StoredParameterVersion,
};
use aws::Clock;
use std::collections::BTreeMap;
use std::time::UNIX_EPOCH;

pub(crate) fn current_epoch_millis(clock: &dyn Clock) -> u64 {
    clock
        .now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

pub(crate) fn next_parameter_version(
    existing: Option<&StoredParameter>,
) -> Result<u64, SsmError> {
    existing
        .map(|stored| Ok(current_version(stored)?.version + 1))
        .unwrap_or(Ok(1))
}

pub(crate) fn upsert_parameter(
    existing: Option<StoredParameter>,
    name: ParameterName,
    stored_version: StoredParameterVersion,
    max_history: usize,
) -> StoredParameter {
    let mut stored = existing.unwrap_or(StoredParameter {
        history: Vec::new(),
        name,
        tags: BTreeMap::new(),
    });
    stored.history.push(stored_version);
    trim_parameter_history(&mut stored.history, max_history);
    stored
}

pub(crate) fn trim_parameter_history(
    history: &mut Vec<StoredParameterVersion>,
    max_history: usize,
) {
    if history.len() > max_history {
        history.drain(..history.len() - max_history);
    }
}

pub(crate) fn current_version(
    stored: &StoredParameter,
) -> Result<&StoredParameterVersion, SsmError> {
    stored.history.last().ok_or_else(|| SsmError::InternalFailure {
        message: "SSM parameter state is corrupted: missing latest version."
            .to_owned(),
    })
}
