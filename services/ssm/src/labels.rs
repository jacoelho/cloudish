use crate::{errors::SsmError, parameters::StoredParameterVersion};

const MAX_LABELS_PER_VERSION: usize = 10;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LabelApplication {
    pub(crate) history: Vec<StoredParameterVersion>,
    pub(crate) invalid_labels: Vec<String>,
}

pub(crate) fn apply_labels_to_history(
    history: &[StoredParameterVersion],
    target_version: u64,
    labels: &[String],
) -> Result<LabelApplication, SsmError> {
    let Some(target_index) =
        history.iter().position(|version| version.version == target_version)
    else {
        return Err(SsmError::ParameterVersionNotFound);
    };

    let mut updated_history = history.to_vec();
    let mut invalid_labels = Vec::new();
    let mut labels_to_apply = Vec::new();

    for label in labels {
        if !is_valid_label(label) {
            invalid_labels.push(label.clone());
            continue;
        }
        if !labels_to_apply.contains(label) {
            labels_to_apply.push(label.clone());
        }
    }

    for version in &mut updated_history {
        version.labels.retain(|label| {
            !labels_to_apply.iter().any(|expected| expected == label)
        });
    }

    let Some(target) = updated_history.get_mut(target_index) else {
        return Err(SsmError::InternalFailure {
            message:
                "SSM parameter state changed while labels were being applied."
                    .to_owned(),
        });
    };
    for label in labels_to_apply {
        if !target.labels.iter().any(|existing| existing == &label) {
            target.labels.push(label);
        }
    }

    if target.labels.len() > MAX_LABELS_PER_VERSION {
        return Err(SsmError::ParameterVersionLabelLimitExceeded);
    }

    Ok(LabelApplication { history: updated_history, invalid_labels })
}

pub(crate) fn is_valid_label(label: &str) -> bool {
    if label.is_empty() || label.len() > 100 {
        return false;
    }
    if label.chars().next().is_some_and(|character| character.is_ascii_digit())
    {
        return false;
    }

    let lowercase = label.to_ascii_lowercase();
    if lowercase.starts_with("aws") || lowercase.starts_with("ssm") {
        return false;
    }

    label.chars().all(|character| {
        character.is_ascii_alphanumeric()
            || matches!(character, '.' | '-' | '_')
    })
}
