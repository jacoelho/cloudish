use serde::Serialize;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidateTemplateInput {
    pub template_body: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidateTemplateOutput {
    pub capabilities: Vec<String>,
    pub capabilities_reason: Option<String>,
    pub declared_transforms: Vec<String>,
    pub description: Option<String>,
    pub parameters: Vec<CloudFormationTemplateParameter>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct CloudFormationTemplateParameter {
    pub default_value: Option<String>,
    pub description: Option<String>,
    pub no_echo: bool,
    pub parameter_key: String,
}
