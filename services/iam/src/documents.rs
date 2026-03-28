#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IamTag {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InlinePolicy {
    pub document: String,
    pub policy_name: String,
    pub principal_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InlinePolicyInput {
    pub document: String,
    pub policy_name: String,
}
