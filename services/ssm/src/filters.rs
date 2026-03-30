use crate::{
    errors::SsmError,
    history::current_version,
    parameters::{
        ParameterName, ParameterSelector, SsmDescribeFilter, StoredParameter,
        StoredParameterVersion,
    },
};

pub(crate) fn select_parameter_version<'a>(
    stored: &'a StoredParameter,
    selector: Option<&ParameterSelector>,
) -> Result<&'a StoredParameterVersion, SsmError> {
    match selector {
        Some(ParameterSelector::Version(version)) => stored
            .history
            .iter()
            .find(|stored_version| stored_version.version == *version)
            .ok_or(SsmError::ParameterVersionNotFound),
        Some(ParameterSelector::Label(label)) => stored
            .history
            .iter()
            .find(|stored_version| {
                stored_version
                    .labels
                    .iter()
                    .any(|stored_label| stored_label == label)
            })
            .ok_or(SsmError::ParameterVersionNotFound),
        None => current_version(stored),
    }
}

pub(crate) fn find_label_filtered_version<'a>(
    stored: &'a StoredParameter,
    labels: &[String],
) -> Option<&'a StoredParameterVersion> {
    stored
        .history
        .iter()
        .filter(|version| {
            version
                .labels
                .iter()
                .any(|label| labels.iter().any(|expected| expected == label))
        })
        .max_by_key(|version| version.version)
}

pub(crate) fn merge_describe_name_filters(
    filters: &[SsmDescribeFilter],
) -> Option<Vec<ParameterName>> {
    let mut names = Vec::new();

    for filter in filters {
        match filter {
            SsmDescribeFilter::NameEquals(values) => {
                for value in values {
                    if !names.contains(value) {
                        names.push(value.clone());
                    }
                }
            }
        }
    }

    (!names.is_empty()).then_some(names)
}

pub(crate) fn reject_pagination(
    operation: &str,
    max_results: Option<u32>,
    next_token: Option<&str>,
) -> Result<(), SsmError> {
    if max_results.is_some() || next_token.is_some() {
        return Err(SsmError::ValidationException {
            message: format!(
                "{operation} pagination is not implemented yet; MaxResults and NextToken remain explicit gap paths."
            ),
        });
    }

    Ok(())
}
