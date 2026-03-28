use crate::{
    errors::CloudFormationError,
    stacks::CloudFormationChangeSummary,
    state::{
        get_att_parts, resource_enabled, sub_tokens, template_validation_error,
    },
};
use serde::Serialize;
use serde_json::{Map, Value};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CloudFormationPlanInput {
    pub parameters: BTreeMap<String, String>,
    pub stack_name: String,
    pub template_body: String,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct CloudFormationStackPlan {
    pub conditions: BTreeMap<String, bool>,
    pub description: Option<String>,
    pub outputs: BTreeMap<String, CloudFormationPlannedOutput>,
    pub resources: BTreeMap<String, CloudFormationPlannedResource>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct CloudFormationPlannedResource {
    pub properties: Value,
    pub resource_type: String,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct CloudFormationPlannedOutput {
    pub description: Option<String>,
    pub value: Value,
}

pub(crate) trait PlannedResourceDefinition {
    fn condition(&self) -> Option<&str>;
    fn depends_on(&self) -> Box<dyn Iterator<Item = &str> + '_>;
    fn properties(&self) -> &Value;
    fn resource_type(&self) -> &str;
}

pub(crate) trait ExistingStackResource {
    fn resource_type(&self) -> &str;
    fn template_properties(&self) -> &Value;
}

pub(crate) trait ExistingStackSnapshot {
    type Resource: ExistingStackResource;

    fn resource(&self, logical_id: &str) -> Option<&Self::Resource>;
    fn resource_order(&self) -> &[String];
}

fn resource_by_logical_id<'a, Resource>(
    resources: &'a BTreeMap<String, Resource>,
    logical_id: &str,
    context: &str,
) -> Result<&'a Resource, CloudFormationError> {
    resources.get(logical_id).ok_or_else(|| {
        template_validation_error(format!(
            "{context} references unknown resource {logical_id}"
        ))
    })
}

fn map_single_entry<'a>(
    map: &'a Map<String, Value>,
    path: &str,
) -> Result<(&'a str, &'a Value), CloudFormationError> {
    map.iter().next().map(|(key, value)| (key.as_str(), value)).ok_or_else(
        || {
            template_validation_error(format!(
                "intrinsic object at {path} must contain exactly one entry"
            ))
        },
    )
}

fn required_item<'a>(
    items: &'a [Value],
    index: usize,
    path: &str,
    intrinsic: &str,
) -> Result<&'a Value, CloudFormationError> {
    items.get(index).ok_or_else(|| {
        template_validation_error(format!(
            "{intrinsic} at {path} is missing item {index}"
        ))
    })
}

pub(crate) fn ensure_capabilities_satisfied(
    required: &[String],
    reason: Option<&str>,
    provided: &[String],
) -> Result<(), CloudFormationError> {
    if required.is_empty() {
        return Ok(());
    }
    let provided = provided.iter().collect::<BTreeSet<_>>();
    let satisfied = required.iter().all(|required| {
        provided.contains(required)
            || (required == "CAPABILITY_IAM"
                && provided.contains(&"CAPABILITY_NAMED_IAM".to_owned()))
    });
    if satisfied {
        return Ok(());
    }

    Err(CloudFormationError::InsufficientCapabilities {
        message: reason
            .unwrap_or(
                "The template contains IAM resources that require explicit capabilities.",
            )
            .to_owned(),
    })
}

pub(crate) fn build_change_summaries<Stack, Resource>(
    existing_stack: Option<&Stack>,
    resources: &BTreeMap<String, Resource>,
    conditions: &BTreeMap<String, bool>,
    resource_order: &[String],
) -> Result<Vec<CloudFormationChangeSummary>, CloudFormationError>
where
    Stack: ExistingStackSnapshot,
    Resource: PlannedResourceDefinition,
{
    let mut changes = Vec::new();
    let mut active_resources = BTreeSet::new();
    for logical_id in resource_order {
        let resource =
            resource_by_logical_id(resources, logical_id, "resource order")?;
        if !resource_enabled(resource.condition(), conditions)? {
            continue;
        }
        active_resources.insert(logical_id.clone());
        match existing_stack.and_then(|stack| stack.resource(logical_id)) {
            None => changes.push(CloudFormationChangeSummary {
                action: "Add".to_owned(),
                logical_resource_id: logical_id.clone(),
                replacement: None,
                resource_type: resource.resource_type().to_owned(),
            }),
            Some(existing)
                if existing.resource_type() != resource.resource_type()
                    || existing.template_properties()
                        != resource.properties() =>
            {
                changes.push(CloudFormationChangeSummary {
                    action: "Modify".to_owned(),
                    logical_resource_id: logical_id.clone(),
                    replacement: None,
                    resource_type: resource.resource_type().to_owned(),
                });
            }
            Some(_) => {}
        }
    }

    if let Some(stack) = existing_stack {
        for logical_id in stack.resource_order().iter().rev() {
            if active_resources.contains(logical_id) {
                continue;
            }
            if let Some(resource) = stack.resource(logical_id) {
                changes.push(CloudFormationChangeSummary {
                    action: "Remove".to_owned(),
                    logical_resource_id: logical_id.clone(),
                    replacement: None,
                    resource_type: resource.resource_type().to_owned(),
                });
            }
        }
    }

    Ok(changes)
}

pub(crate) fn topological_resource_order<Resource>(
    resources: &BTreeMap<String, Resource>,
    conditions: &BTreeMap<String, bool>,
) -> Result<Vec<String>, CloudFormationError>
where
    Resource: PlannedResourceDefinition,
{
    let active = resources
        .iter()
        .filter_map(|(logical_id, resource)| {
            resource_enabled(resource.condition(), conditions)
                .ok()
                .and_then(|enabled| enabled.then_some(logical_id.clone()))
        })
        .collect::<BTreeSet<_>>();
    let mut dependencies = BTreeMap::new();
    for logical_id in &active {
        let resource =
            resource_by_logical_id(resources, logical_id, "active set")?;
        let mut deps = BTreeSet::new();
        collect_resource_dependencies(
            resource.properties(),
            &active,
            &mut deps,
        )?;
        for explicit in resource.depends_on() {
            if active.contains(explicit) {
                deps.insert(explicit.to_owned());
            }
        }
        dependencies.insert(logical_id.clone(), deps);
    }
    let mut indegree = dependencies
        .iter()
        .map(|(logical_id, deps)| (logical_id.clone(), deps.len()))
        .collect::<BTreeMap<_, _>>();
    let mut ready = indegree
        .iter()
        .filter(|(_, degree)| **degree == 0)
        .map(|(logical_id, _)| logical_id.clone())
        .collect::<BTreeSet<_>>();
    let mut order = Vec::new();
    while let Some(logical_id) = ready.pop_first() {
        order.push(logical_id.clone());
        for (candidate, deps) in &dependencies {
            if !deps.contains(&logical_id) {
                continue;
            }
            let degree = indegree.get_mut(candidate).ok_or_else(|| {
                template_validation_error(format!(
                    "dependency graph is missing indegree for {candidate}"
                ))
            })?;
            *degree -= 1;
            if *degree == 0 {
                ready.insert(candidate.clone());
            }
        }
    }
    if order.len() != active.len() {
        return Err(template_validation_error(
            "resource dependency graph contains a cycle",
        ));
    }

    Ok(order)
}

fn collect_resource_dependencies(
    value: &Value,
    resource_names: &BTreeSet<String>,
    dependencies: &mut BTreeSet<String>,
) -> Result<(), CloudFormationError> {
    match value {
        Value::Array(items) => {
            for item in items {
                collect_resource_dependencies(
                    item,
                    resource_names,
                    dependencies,
                )?;
            }
            Ok(())
        }
        Value::Object(map) if map.len() == 1 => {
            let (key, inner) = map_single_entry(map, "dependencies")?;
            match key {
                "Ref" => {
                    if let Some(target) = inner.as_str()
                        && resource_names.contains(target)
                    {
                        dependencies.insert(target.to_owned());
                    }
                    Ok(())
                }
                "Fn::GetAtt" => {
                    let (logical_id, _) =
                        get_att_parts(inner, "dependencies")?;
                    if resource_names.contains(logical_id) {
                        dependencies.insert(logical_id.to_owned());
                    }
                    Ok(())
                }
                "Fn::Sub" => {
                    let (template, variables) = match inner {
                        Value::String(template) => (template.as_str(), None),
                        Value::Array(items) if items.len() == 2 => {
                            let variables = required_item(
                                items,
                                1,
                                "dependencies",
                                "Fn::Sub",
                            )?
                            .as_object()
                            .ok_or_else(|| {
                                template_validation_error(
                                    "Fn::Sub variable map must be an object",
                                )
                            })?;
                            for value in variables.values() {
                                collect_resource_dependencies(
                                    value,
                                    resource_names,
                                    dependencies,
                                )?;
                            }
                            (
                                required_item(
                                    items,
                                    0,
                                    "dependencies",
                                    "Fn::Sub",
                                )?
                                .as_str()
                                .unwrap_or_default(),
                                Some(variables),
                            )
                        }
                        _ => return Ok(()),
                    };
                    for token in sub_tokens(template) {
                        if variables.and_then(|vars| vars.get(token)).is_some()
                        {
                            continue;
                        }
                        let logical_id = token
                            .split_once('.')
                            .map(|(logical_id, _)| logical_id)
                            .unwrap_or(token);
                        if resource_names.contains(logical_id) {
                            dependencies.insert(logical_id.to_owned());
                        }
                    }
                    Ok(())
                }
                _ => collect_resource_dependencies(
                    inner,
                    resource_names,
                    dependencies,
                ),
            }
        }
        Value::Object(map) => {
            for value in map.values() {
                collect_resource_dependencies(
                    value,
                    resource_names,
                    dependencies,
                )?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[derive(Debug, Clone)]
    struct TestResource {
        condition: Option<String>,
        depends_on: Vec<String>,
        properties: Value,
        resource_type: String,
    }

    impl PlannedResourceDefinition for TestResource {
        fn condition(&self) -> Option<&str> {
            self.condition.as_deref()
        }

        fn depends_on(&self) -> Box<dyn Iterator<Item = &str> + '_> {
            Box::new(self.depends_on.iter().map(String::as_str))
        }

        fn properties(&self) -> &Value {
            &self.properties
        }

        fn resource_type(&self) -> &str {
            &self.resource_type
        }
    }

    #[derive(Debug, Clone)]
    struct TestExistingResource {
        resource_type: String,
        template_properties: Value,
    }

    impl ExistingStackResource for TestExistingResource {
        fn resource_type(&self) -> &str {
            &self.resource_type
        }

        fn template_properties(&self) -> &Value {
            &self.template_properties
        }
    }

    #[derive(Debug, Clone)]
    struct TestExistingStack {
        resource_order: Vec<String>,
        resources: BTreeMap<String, TestExistingResource>,
    }

    impl ExistingStackSnapshot for TestExistingStack {
        type Resource = TestExistingResource;

        fn resource(&self, logical_id: &str) -> Option<&Self::Resource> {
            self.resources.get(logical_id)
        }

        fn resource_order(&self) -> &[String] {
            &self.resource_order
        }
    }

    #[test]
    fn capabilities_accept_named_iam_for_iam_requirements() {
        assert!(
            ensure_capabilities_satisfied(
                &[String::from("CAPABILITY_IAM")],
                None,
                &[String::from("CAPABILITY_NAMED_IAM")],
            )
            .is_ok()
        );
    }

    #[test]
    fn change_summaries_track_add_modify_and_remove_actions() {
        let existing = TestExistingStack {
            resource_order: vec![
                "Same".to_owned(),
                "Changed".to_owned(),
                "Removed".to_owned(),
            ],
            resources: BTreeMap::from([
                (
                    "Same".to_owned(),
                    TestExistingResource {
                        resource_type: "AWS::SQS::Queue".to_owned(),
                        template_properties: json!({"QueueName": "same"}),
                    },
                ),
                (
                    "Changed".to_owned(),
                    TestExistingResource {
                        resource_type: "AWS::SQS::Queue".to_owned(),
                        template_properties: json!({"QueueName": "before"}),
                    },
                ),
                (
                    "Removed".to_owned(),
                    TestExistingResource {
                        resource_type: "AWS::SNS::Topic".to_owned(),
                        template_properties: json!({"TopicName": "gone"}),
                    },
                ),
            ]),
        };
        let resources = BTreeMap::from([
            (
                "Same".to_owned(),
                TestResource {
                    condition: None,
                    depends_on: Vec::new(),
                    properties: json!({"QueueName": "same"}),
                    resource_type: "AWS::SQS::Queue".to_owned(),
                },
            ),
            (
                "Changed".to_owned(),
                TestResource {
                    condition: None,
                    depends_on: Vec::new(),
                    properties: json!({"QueueName": "after"}),
                    resource_type: "AWS::SQS::Queue".to_owned(),
                },
            ),
            (
                "Added".to_owned(),
                TestResource {
                    condition: None,
                    depends_on: Vec::new(),
                    properties: json!({"TopicName": "new"}),
                    resource_type: "AWS::SNS::Topic".to_owned(),
                },
            ),
        ]);

        let summaries = build_change_summaries(
            Some(&existing),
            &resources,
            &BTreeMap::new(),
            &["Same".to_owned(), "Changed".to_owned(), "Added".to_owned()],
        )
        .expect("change summaries should build");

        assert_eq!(
            summaries,
            vec![
                CloudFormationChangeSummary {
                    action: "Modify".to_owned(),
                    logical_resource_id: "Changed".to_owned(),
                    replacement: None,
                    resource_type: "AWS::SQS::Queue".to_owned(),
                },
                CloudFormationChangeSummary {
                    action: "Add".to_owned(),
                    logical_resource_id: "Added".to_owned(),
                    replacement: None,
                    resource_type: "AWS::SNS::Topic".to_owned(),
                },
                CloudFormationChangeSummary {
                    action: "Remove".to_owned(),
                    logical_resource_id: "Removed".to_owned(),
                    replacement: None,
                    resource_type: "AWS::SNS::Topic".to_owned(),
                },
            ]
        );
    }

    #[test]
    fn resource_order_tracks_intrinsic_and_explicit_dependencies() {
        let resources = BTreeMap::from([
            (
                "Base".to_owned(),
                TestResource {
                    condition: None,
                    depends_on: Vec::new(),
                    properties: json!({}),
                    resource_type: "AWS::SQS::Queue".to_owned(),
                },
            ),
            (
                "Derived".to_owned(),
                TestResource {
                    condition: None,
                    depends_on: vec!["Base".to_owned()],
                    properties: json!({
                        "Target": { "Fn::GetAtt": "Base.Arn" }
                    }),
                    resource_type: "AWS::SNS::Topic".to_owned(),
                },
            ),
            (
                "Rendered".to_owned(),
                TestResource {
                    condition: None,
                    depends_on: Vec::new(),
                    properties: json!({
                        "Name": { "Fn::Sub": "${Derived}-${Base}" }
                    }),
                    resource_type: "AWS::SSM::Parameter".to_owned(),
                },
            ),
            (
                "Disabled".to_owned(),
                TestResource {
                    condition: Some("Disabled".to_owned()),
                    depends_on: vec!["Rendered".to_owned()],
                    properties: json!({}),
                    resource_type: "AWS::S3::Bucket".to_owned(),
                },
            ),
        ]);

        let order = topological_resource_order(
            &resources,
            &BTreeMap::from([("Disabled".to_owned(), false)]),
        )
        .expect("resource order should resolve");

        assert_eq!(
            order,
            vec![
                "Base".to_owned(),
                "Derived".to_owned(),
                "Rendered".to_owned(),
            ]
        );
    }
}
