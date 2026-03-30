#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    clippy::unreachable,
    clippy::indexing_slicing,
    clippy::assertions_on_constants,
    clippy::missing_panics_doc,
    clippy::missing_errors_doc
)]
use serde::Deserialize;
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::process::Command;

#[derive(Debug, Deserialize)]
struct Metadata {
    packages: Vec<Package>,
}

#[derive(Debug, Deserialize)]
struct Package {
    features: BTreeMap<String, Vec<String>>,
    name: String,
    manifest_path: String,
    dependencies: Vec<Dependency>,
}

#[derive(Debug, Deserialize)]
struct Dependency {
    optional: bool,
    name: String,
    kind: Option<String>,
    path: Option<String>,
}

#[derive(Debug)]
struct PackageContract {
    normal: &'static [&'static str],
    dev: &'static [&'static str],
}

macro_rules! assert_ok {
    ($expr:expr, $($arg:tt)*) => {{
        match $expr {
            Ok(value) => value,
            Err(error) => {
                assert!(false, "{}: {error}", format!($($arg)*));
                return;
            }
        }
    }};
}

macro_rules! assert_some {
    ($expr:expr, $($arg:tt)*) => {{
        match $expr {
            Some(value) => value,
            None => {
                assert!(false, $($arg)*);
                return;
            }
        }
    }};
}

#[test]
fn workspace_dependency_contract_matches_spec() {
    let root = assert_ok!(workspace_root(), "workspace root should resolve");
    let packages = assert_ok!(
        workspace_packages(&root),
        "workspace packages should load from cargo metadata"
    );
    let contracts = dependency_contracts();

    let package_names = packages.keys().cloned().collect::<BTreeSet<_>>();
    let contract_names = contracts
        .keys()
        .map(|name| (*name).to_owned())
        .collect::<BTreeSet<_>>();
    assert_eq!(
        package_names, contract_names,
        "spec and regression contract must enumerate the same workspace crates"
    );

    for (name, contract) in contracts {
        let package = assert_some!(
            packages.get(name),
            "workspace package `{name}` must exist"
        );
        assert_eq!(
            dependency_names(package, &root, None),
            expected_names(contract.normal),
            "unexpected normal project dependencies for `{name}`"
        );
        assert_eq!(
            dependency_names(package, &root, Some("dev")),
            expected_names(contract.dev),
            "unexpected dev project dependencies for `{name}`"
        );
        assert!(
            dependency_names(package, &root, Some("build")).is_empty(),
            "workspace package `{name}` must not add build-time project dependencies"
        );
    }
}

#[test]
fn workspace_avoids_generic_bucket_crate_names() {
    let root = assert_ok!(workspace_root(), "workspace root should resolve");
    let packages = assert_ok!(
        workspace_packages(&root),
        "workspace packages should load from cargo metadata"
    );
    let banned_names = ["common", "helpers", "platform", "shared", "utils"];

    for name in banned_names {
        assert!(
            !packages.contains_key(name),
            "workspace crate `{name}` is too generic for the architecture contract"
        );
    }
}

#[test]
fn mandatory_runtime_services_are_not_feature_gated() {
    let root = assert_ok!(workspace_root(), "workspace root should resolve");
    let packages = assert_ok!(
        workspace_packages(&root),
        "workspace packages should load from cargo metadata"
    );

    for package_name in ["app", "http"] {
        let package = assert_some!(
            packages.get(package_name),
            "workspace package `{package_name}` must exist"
        );

        for feature in ["iam", "sts"] {
            assert!(
                !package.features.contains_key(feature),
                "workspace package `{package_name}` must not model mandatory service `{feature}` as an optional feature"
            );
        }

        for dependency_name in ["iam", "sts"] {
            let dependency = assert_some!(
                workspace_dependency(package, &root, dependency_name, None),
                "workspace package `{package_name}` must depend on `{dependency_name}`"
            );
            assert!(
                !dependency.optional,
                "workspace package `{package_name}` must keep `{dependency_name}` as a mandatory dependency"
            );
        }
    }
}

fn dependency_contracts() -> BTreeMap<&'static str, PackageContract> {
    BTreeMap::from([
        (
            "apigateway",
            PackageContract { normal: &["aws", "storage"], dev: &[] },
        ),
        (
            "app",
            PackageContract {
                normal: &[
                    "apigateway",
                    "auth",
                    "aws",
                    "cloudformation",
                    "cloudwatch",
                    "cognito",
                    "dynamodb",
                    "edge-runtime",
                    "elasticache",
                    "eventbridge",
                    "http",
                    "iam",
                    "kinesis",
                    "kms",
                    "lambda",
                    "rds",
                    "s3",
                    "secrets-manager",
                    "sns",
                    "sqs",
                    "ssm",
                    "step-functions",
                    "storage",
                    "sts",
                ],
                dev: &["test-support"],
            },
        ),
        ("auth", PackageContract { normal: &["aws"], dev: &[] }),
        ("aws", PackageContract { normal: &[], dev: &[] }),
        (
            "cloudformation",
            PackageContract {
                normal: &[
                    "aws",
                    "dynamodb",
                    "iam",
                    "kms",
                    "lambda",
                    "s3",
                    "secrets-manager",
                    "sns",
                    "sqs",
                    "ssm",
                ],
                dev: &[],
            },
        ),
        (
            "cloudwatch",
            PackageContract { normal: &["aws", "storage"], dev: &[] },
        ),
        ("cognito", PackageContract { normal: &["aws", "storage"], dev: &[] }),
        (
            "dynamodb",
            PackageContract { normal: &["aws", "storage"], dev: &[] },
        ),
        (
            "edge-runtime",
            PackageContract {
                normal: &[
                    "apigateway",
                    "auth",
                    "aws",
                    "cloudformation",
                    "cloudwatch",
                    "cognito",
                    "dynamodb",
                    "elasticache",
                    "eventbridge",
                    "iam",
                    "kinesis",
                    "kms",
                    "lambda",
                    "rds",
                    "s3",
                    "secrets-manager",
                    "sns",
                    "sqs",
                    "ssm",
                    "step-functions",
                    "storage",
                    "sts",
                ],
                dev: &["test-support"],
            },
        ),
        (
            "eventbridge",
            PackageContract { normal: &["aws", "storage"], dev: &[] },
        ),
        (
            "elasticache",
            PackageContract { normal: &["aws", "storage"], dev: &[] },
        ),
        (
            "http",
            PackageContract {
                normal: &[
                    "apigateway",
                    "auth",
                    "aws",
                    "cloudformation",
                    "cloudwatch",
                    "cognito",
                    "dynamodb",
                    "edge-runtime",
                    "elasticache",
                    "eventbridge",
                    "iam",
                    "kinesis",
                    "kms",
                    "lambda",
                    "rds",
                    "s3",
                    "secrets-manager",
                    "sns",
                    "sqs",
                    "ssm",
                    "step-functions",
                    "sts",
                ],
                dev: &["storage"],
            },
        ),
        ("iam", PackageContract { normal: &["aws"], dev: &[] }),
        ("kinesis", PackageContract { normal: &["aws", "storage"], dev: &[] }),
        ("kms", PackageContract { normal: &["aws", "storage"], dev: &[] }),
        (
            "lambda",
            PackageContract {
                normal: &["aws", "iam", "s3", "sqs", "storage"],
                dev: &[],
            },
        ),
        ("rds", PackageContract { normal: &["aws", "storage"], dev: &[] }),
        ("s3", PackageContract { normal: &["aws", "storage"], dev: &[] }),
        (
            "secrets-manager",
            PackageContract { normal: &["aws", "storage"], dev: &[] },
        ),
        ("sns", PackageContract { normal: &["aws"], dev: &[] }),
        ("sqs", PackageContract { normal: &["aws"], dev: &[] }),
        ("ssm", PackageContract { normal: &["aws", "storage"], dev: &[] }),
        (
            "step-functions",
            PackageContract { normal: &["aws", "storage"], dev: &[] },
        ),
        (
            "storage",
            PackageContract { normal: &["aws"], dev: &["test-support"] },
        ),
        ("sts", PackageContract { normal: &["aws", "iam"], dev: &[] }),
        ("test-support", PackageContract { normal: &[], dev: &[] }),
        (
            "tests",
            PackageContract {
                normal: &["app", "auth", "aws", "storage", "test-support"],
                dev: &[],
            },
        ),
    ])
}

fn expected_names(names: &[&str]) -> BTreeSet<String> {
    names.iter().map(|name| (*name).to_owned()).collect()
}

fn dependency_names(
    package: &Package,
    workspace_root: &Path,
    kind: Option<&str>,
) -> BTreeSet<String> {
    let workspace_root = workspace_root.to_string_lossy();

    package
        .dependencies
        .iter()
        .filter(|dependency| dependency.kind.as_deref() == kind)
        .filter_map(|dependency| {
            let path = dependency.path.as_deref()?;
            path.starts_with(workspace_root.as_ref())
                .then(|| dependency.name.clone())
        })
        .collect()
}

fn workspace_dependency<'a>(
    package: &'a Package,
    workspace_root: &Path,
    dependency_name: &str,
    kind: Option<&str>,
) -> Option<&'a Dependency> {
    let workspace_root = workspace_root.to_string_lossy();

    package.dependencies.iter().find(|dependency| {
        dependency.name == dependency_name
            && dependency.kind.as_deref() == kind
            && dependency
                .path
                .as_deref()
                .is_some_and(|path| path.starts_with(workspace_root.as_ref()))
    })
}

fn workspace_packages(
    workspace_root: &Path,
) -> Result<BTreeMap<String, Package>, String> {
    let root = workspace_root.to_string_lossy();
    let metadata = cargo_metadata(workspace_root)?;

    Ok(metadata
        .packages
        .into_iter()
        .filter(|package| package.manifest_path.starts_with(root.as_ref()))
        .map(|package| (package.name.clone(), package))
        .collect())
}

fn cargo_metadata(workspace_root: &Path) -> Result<Metadata, String> {
    let output = Command::new("cargo")
        .args(["metadata", "--format-version", "1", "--no-deps"])
        .current_dir(workspace_root)
        .output()
        .map_err(|error| format!("cargo metadata should run: {error}"))?;

    if !output.status.success() {
        return Err(format!(
            "cargo metadata failed: {}",
            String::from_utf8_lossy(&output.stderr)
        ));
    }

    serde_json::from_slice(&output.stdout).map_err(|error| {
        format!("cargo metadata output should be valid JSON: {error}")
    })
}

fn workspace_root() -> Result<PathBuf, String> {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .canonicalize()
        .map_err(|error| format!("workspace root should resolve: {error}"))
}
