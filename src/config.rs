use crate::constants::RULE_STRING_MAX_BYTES;
use crate::digest::sha256_hex;
use crate::profile::ProfileSnapshot;
use crate::rule::{RuleString, RuleType, parse_rule};
use serde::Deserialize;
use serde_yaml_ng::{Mapping, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::path::{Component, Path, PathBuf};
use url::Url;

const BUNDLED_CATALOG: &str = include_str!("../fixtures/mihomo/v1.19.28/config-schema.yaml");
const BUNDLED_CORE_VERSION: &str = "v1.19.28";
const COMPILER_POLICY_REVISION: &str = "hopash-config-policy-v1";

#[derive(Clone, Eq, PartialEq)]
pub struct AuthoritativeConfig {
    controller_unix: String,
    secret: String,
}

impl fmt::Debug for AuthoritativeConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AuthoritativeConfig")
            .field("controller_unix", &"[REDACTED]")
            .field("secret", &"[REDACTED]")
            .finish()
    }
}

impl AuthoritativeConfig {
    #[must_use]
    pub fn new(controller_unix: impl Into<String>, secret: impl Into<String>) -> Self {
        Self {
            controller_unix: controller_unix.into(),
            secret: secret.into(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum ProviderSection {
    Proxy,
    Rule,
}

impl ProviderSection {
    fn directory(self) -> &'static str {
        match self {
            Self::Proxy => "proxy",
            Self::Rule => "rule",
        }
    }
}

#[derive(Clone, Eq, PartialEq)]
pub enum ProviderKind {
    Remote { url: String },
    Local { source: PathBuf },
}

impl fmt::Debug for ProviderKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Remote { .. } => formatter
                .debug_struct("Remote")
                .field("url", &"[REDACTED]")
                .finish(),
            Self::Local { .. } => formatter
                .debug_struct("Local")
                .field("source", &"[REDACTED]")
                .finish(),
        }
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct ProviderRecord {
    pub section: ProviderSection,
    pub name: String,
    pub relative_path: PathBuf,
    pub kind: ProviderKind,
}

impl fmt::Debug for ProviderRecord {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProviderRecord")
            .field("section", &self.section)
            .field("name", &"[REDACTED]")
            .field("relative_path", &"[REDACTED]")
            .field("kind", &self.kind)
            .finish()
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct EffectiveConfiguration {
    yaml: String,
    core_version: String,
    compiler_policy_sha256: String,
    providers: Vec<ProviderRecord>,
}

impl fmt::Debug for EffectiveConfiguration {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EffectiveConfiguration")
            .field("yaml_bytes", &self.yaml.len())
            .field("core_version", &self.core_version)
            .field("compiler_policy_sha256", &self.compiler_policy_sha256)
            .field("provider_count", &self.providers.len())
            .finish()
    }
}

impl EffectiveConfiguration {
    #[must_use]
    pub fn yaml(&self) -> &str {
        &self.yaml
    }

    #[must_use]
    pub fn core_version(&self) -> &str {
        &self.core_version
    }

    #[must_use]
    pub fn compiler_policy_sha256(&self) -> &str {
        &self.compiler_policy_sha256
    }

    #[must_use]
    pub fn providers(&self) -> &[ProviderRecord] {
        &self.providers
    }
}

pub trait CoreConfigValidator {
    fn validate(
        &self,
        configuration: &EffectiveConfiguration,
        staging_root: &Path,
    ) -> Result<(), CoreValidationError>;
}

#[derive(Clone, Eq, PartialEq)]
pub struct CoreValidationError;

impl fmt::Debug for CoreValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CoreValidationError")
            .field("diagnostic", &"[REDACTED]")
            .finish()
    }
}

impl CoreValidationError {
    #[must_use]
    pub fn new(message: impl Into<String>) -> Self {
        drop(message.into());
        Self
    }

    #[must_use]
    pub const fn message(&self) -> &'static str {
        "Mihomo configuration validation failed"
    }
}

impl fmt::Display for CoreValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.message())
    }
}

impl std::error::Error for CoreValidationError {}

#[derive(Clone, Eq, PartialEq)]
pub enum ConfigError {
    CatalogInvalid,
    CatalogVersionMismatch,
    UnsupportedField {
        path: String,
    },
    UnsupportedVariant {
        path: String,
        value: String,
    },
    InvalidShape {
        path: String,
        expected: &'static str,
    },
    MissingDiscriminator {
        path: String,
        field: String,
    },
    MissingField {
        path: String,
    },
    EmptyName {
        path: String,
    },
    DuplicateName {
        path: String,
        name: String,
    },
    InvalidRoutingRule {
        path: String,
        reason: String,
    },
    UnavailableReference {
        path: String,
        reference_kind: &'static str,
        name: String,
    },
    InvalidAuthoritativeValue {
        field: &'static str,
    },
    StagingRootUnavailable {
        path: PathBuf,
    },
    ProviderPathRequired {
        path: String,
    },
    ProviderUrlRequired {
        path: String,
    },
    ProviderUrlInvalid {
        path: String,
    },
    ProviderPathTraversal {
        path: PathBuf,
    },
    ProviderPathOutsideStagingRoot {
        path: PathBuf,
    },
    ProviderFileUnavailable {
        path: PathBuf,
    },
    CoreValidationFailed(CoreValidationError),
    SerializationFailed,
}

impl fmt::Debug for ConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let kind = match self {
            Self::CatalogInvalid => "CatalogInvalid",
            Self::CatalogVersionMismatch => "CatalogVersionMismatch",
            Self::UnsupportedField { .. } => "UnsupportedField",
            Self::UnsupportedVariant { .. } => "UnsupportedVariant",
            Self::InvalidShape { .. } => "InvalidShape",
            Self::MissingDiscriminator { .. } => "MissingDiscriminator",
            Self::MissingField { .. } => "MissingField",
            Self::EmptyName { .. } => "EmptyName",
            Self::DuplicateName { .. } => "DuplicateName",
            Self::InvalidRoutingRule { .. } => "InvalidRoutingRule",
            Self::UnavailableReference { .. } => "UnavailableReference",
            Self::InvalidAuthoritativeValue { .. } => "InvalidAuthoritativeValue",
            Self::StagingRootUnavailable { .. } => "StagingRootUnavailable",
            Self::ProviderPathRequired { .. } => "ProviderPathRequired",
            Self::ProviderUrlRequired { .. } => "ProviderUrlRequired",
            Self::ProviderUrlInvalid { .. } => "ProviderUrlInvalid",
            Self::ProviderPathTraversal { .. } => "ProviderPathTraversal",
            Self::ProviderPathOutsideStagingRoot { .. } => "ProviderPathOutsideStagingRoot",
            Self::ProviderFileUnavailable { .. } => "ProviderFileUnavailable",
            Self::CoreValidationFailed(_) => "CoreValidationFailed",
            Self::SerializationFailed => "SerializationFailed",
        };
        formatter.write_str(kind)
    }
}

impl fmt::Display for ConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CatalogInvalid => {
                formatter.write_str("the bundled Mihomo field catalog is invalid")
            }
            Self::CatalogVersionMismatch => formatter
                .write_str("the bundled Mihomo field catalog version does not match the Core"),
            Self::UnsupportedField { .. } => {
                formatter.write_str("the Profile Snapshot contains an unsupported field")
            }
            Self::UnsupportedVariant { .. } => {
                formatter.write_str("the Profile Snapshot contains an unsupported variant")
            }
            Self::InvalidShape { .. } => {
                formatter.write_str("the Profile Snapshot contains an invalid field shape")
            }
            Self::MissingDiscriminator { .. } => {
                formatter.write_str("a Profile Snapshot record is missing its type discriminator")
            }
            Self::MissingField { .. } => {
                formatter.write_str("the Profile Snapshot is missing a required field")
            }
            Self::EmptyName { .. } => {
                formatter.write_str("the Profile Snapshot contains an empty record name")
            }
            Self::DuplicateName { .. } => {
                formatter.write_str("the Profile Snapshot contains a duplicate record name")
            }
            Self::InvalidRoutingRule { .. } => {
                formatter.write_str("the Local Rule Set contains an invalid Routing Rule")
            }
            Self::UnavailableReference { .. } => {
                formatter.write_str("the configuration contains an unavailable named reference")
            }
            Self::InvalidAuthoritativeValue { field } => {
                write!(
                    formatter,
                    "authoritative configuration field {field} is invalid"
                )
            }
            Self::StagingRootUnavailable { .. } => {
                formatter.write_str("the Profile staging root is unavailable")
            }
            Self::ProviderPathRequired { .. } => {
                formatter.write_str("a local provider requires a path")
            }
            Self::ProviderUrlRequired { .. } => {
                formatter.write_str("a remote provider requires a URL")
            }
            Self::ProviderUrlInvalid { .. } => {
                formatter.write_str("a remote provider requires an HTTP(S) URL")
            }
            Self::ProviderPathTraversal { .. } => {
                formatter.write_str("a provider path traverses outside its staging root")
            }
            Self::ProviderPathOutsideStagingRoot { .. } => {
                formatter.write_str("a provider path resolves outside its staging root")
            }
            Self::ProviderFileUnavailable { .. } => {
                formatter.write_str("a local provider file is unavailable")
            }
            Self::CoreValidationFailed(error) => {
                write!(
                    formatter,
                    "Mihomo rejected the effective configuration: {error}"
                )
            }
            Self::SerializationFailed => {
                formatter.write_str("effective configuration serialization failed")
            }
        }
    }
}

impl std::error::Error for ConfigError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::CoreValidationFailed(error) => Some(error),
            _ => None,
        }
    }
}

pub struct ConfigCompiler {
    catalog: FieldCatalog,
    compiler_policy_sha256: String,
}

impl ConfigCompiler {
    pub fn bundled() -> Result<Self, ConfigError> {
        let catalog: FieldCatalog =
            serde_yaml_ng::from_str(BUNDLED_CATALOG).map_err(|_| ConfigError::CatalogInvalid)?;
        if catalog.schema_version != 1 || catalog.core_version != BUNDLED_CORE_VERSION {
            return Err(ConfigError::CatalogVersionMismatch);
        }
        let compiler_policy_sha256 = policy_sha256();
        Ok(Self {
            catalog,
            compiler_policy_sha256,
        })
    }

    pub fn compile(
        &self,
        snapshot: &ProfileSnapshot,
        rules: &[String],
        authoritative: &AuthoritativeConfig,
        staging_root: &Path,
    ) -> Result<EffectiveConfiguration, ConfigError> {
        validate_authoritative(authoritative)?;
        self.catalog.validate(snapshot.document())?;
        validate_references(snapshot.document(), rules)?;
        let canonical_root = canonical_staging_root(staging_root)?;
        let mut document = snapshot.document().clone();

        strip_managed_mapping(&mut document, &self.catalog.top_level);

        document.insert(
            "rules".into(),
            rules.iter().cloned().map(Value::String).collect(),
        );
        document.insert("mode".into(), "rule".into());
        document.insert("allow-lan".into(), false.into());
        document.insert(
            "external-controller-unix".into(),
            authoritative.controller_unix.clone().into(),
        );
        document.insert("secret".into(), authoritative.secret.clone().into());

        let tun = document
            .entry("tun".into())
            .or_insert_with(|| Value::Mapping(Mapping::new()));
        let Value::Mapping(tun) = tun else {
            return Err(ConfigError::InvalidShape {
                path: "tun".to_owned(),
                expected: "a mapping",
            });
        };
        tun.insert("enable".into(), true.into());

        let mut providers = Vec::new();
        classify_provider_section(
            &mut document,
            "proxy-providers",
            ProviderSection::Proxy,
            &canonical_root,
            &mut providers,
        )?;
        classify_provider_section(
            &mut document,
            "rule-providers",
            ProviderSection::Rule,
            &canonical_root,
            &mut providers,
        )?;
        providers.sort_by(|left, right| {
            left.section
                .cmp(&right.section)
                .then_with(|| left.name.cmp(&right.name))
        });

        self.catalog.validate(&document)?;
        let canonical = canonicalize(Value::Mapping(document));
        let yaml =
            serde_yaml_ng::to_string(&canonical).map_err(|_| ConfigError::SerializationFailed)?;

        Ok(EffectiveConfiguration {
            yaml,
            core_version: self.catalog.core_version.clone(),
            compiler_policy_sha256: self.compiler_policy_sha256.clone(),
            providers,
        })
    }

    pub fn compile_validated(
        &self,
        snapshot: &ProfileSnapshot,
        rules: &[String],
        authoritative: &AuthoritativeConfig,
        staging_root: &Path,
        validator: &impl CoreConfigValidator,
    ) -> Result<EffectiveConfiguration, ConfigError> {
        let configuration = self.compile(snapshot, rules, authoritative, staging_root)?;
        validator
            .validate(&configuration, &canonical_staging_root(staging_root)?)
            .map_err(ConfigError::CoreValidationFailed)?;
        Ok(configuration)
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "kebab-case")]
struct FieldCatalog {
    schema_version: u16,
    core_version: String,
    top_level: BTreeMap<String, SchemaNode>,
}

impl FieldCatalog {
    fn validate(&self, document: &Mapping) -> Result<(), ConfigError> {
        validate_known_mapping(document, &self.top_level, "")
    }
}

fn validate_authoritative(authoritative: &AuthoritativeConfig) -> Result<(), ConfigError> {
    if !Path::new(&authoritative.controller_unix).is_absolute()
        || authoritative.controller_unix.is_empty()
    {
        return Err(ConfigError::InvalidAuthoritativeValue {
            field: "external-controller-unix",
        });
    }
    if authoritative.secret.is_empty() {
        return Err(ConfigError::InvalidAuthoritativeValue { field: "secret" });
    }
    Ok(())
}

fn canonical_staging_root(staging_root: &Path) -> Result<PathBuf, ConfigError> {
    let canonical_root =
        std::fs::canonicalize(staging_root).map_err(|_| ConfigError::StagingRootUnavailable {
            path: staging_root.to_path_buf(),
        })?;
    if !canonical_root.is_dir() {
        return Err(ConfigError::StagingRootUnavailable {
            path: staging_root.to_path_buf(),
        });
    }
    Ok(canonical_root)
}

fn validate_references(document: &Mapping, rules: &[String]) -> Result<(), ConfigError> {
    let proxy_names = collect_record_names(document, "proxies")?;
    let group_names = collect_record_names(document, "proxy-groups")?;
    validate_unique_names(&proxy_names, &group_names)?;

    let proxy_provider_names = collect_map_names(document, "proxy-providers")?;
    let rule_provider_names = collect_map_names(document, "rule-providers")?;
    let sub_rule_names = collect_map_names(document, "sub-rules")?;

    let mut policy_targets = builtin_policy_targets();
    policy_targets.extend(proxy_names.iter().map(|(_, name)| name.clone()));
    policy_targets.extend(group_names.iter().map(|(_, name)| name.clone()));

    validate_group_references(document, &policy_targets, &proxy_provider_names)?;
    validate_rule_strings(
        rules.iter().map(String::as_str),
        "rules",
        &policy_targets,
        &rule_provider_names,
        &sub_rule_names,
    )?;

    if let Some(Value::Mapping(sub_rules)) = document.get("sub-rules") {
        for (name, rules) in sub_rules {
            let name = name
                .as_str()
                .expect("the field catalog requires string sub-rule names");
            let rules = rules
                .as_sequence()
                .expect("the field catalog requires sub-rule sequences");
            validate_rule_strings(
                rules.iter().map(|rule| {
                    rule.as_str()
                        .expect("the field catalog requires rule strings")
                }),
                &child_path("sub-rules", name),
                &policy_targets,
                &rule_provider_names,
                &sub_rule_names,
            )?;
        }
    }

    Ok(())
}

fn collect_record_names(
    document: &Mapping,
    section: &str,
) -> Result<Vec<(usize, String)>, ConfigError> {
    let Some(Value::Sequence(records)) = document.get(section) else {
        return Ok(Vec::new());
    };
    records
        .iter()
        .enumerate()
        .map(|(index, record)| {
            let path = format!("{section}[{index}].name");
            let name = record
                .as_mapping()
                .and_then(|record| record.get("name"))
                .and_then(Value::as_str)
                .expect("the field catalog requires string record names");
            if name.is_empty() {
                return Err(ConfigError::EmptyName { path });
            }
            Ok((index, name.to_owned()))
        })
        .collect()
}

fn collect_map_names(document: &Mapping, section: &str) -> Result<BTreeSet<String>, ConfigError> {
    let Some(Value::Mapping(records)) = document.get(section) else {
        return Ok(BTreeSet::new());
    };
    records
        .keys()
        .map(|name| {
            let name = name
                .as_str()
                .expect("the field catalog requires string map keys");
            if name.is_empty() {
                return Err(ConfigError::EmptyName {
                    path: section.to_owned(),
                });
            }
            Ok(name.to_owned())
        })
        .collect()
}

fn validate_unique_names(
    proxies: &[(usize, String)],
    groups: &[(usize, String)],
) -> Result<(), ConfigError> {
    let mut names = reserved_proxy_names();
    for (index, name) in proxies {
        if name == "GLOBAL" || !names.insert(name.clone()) {
            return Err(ConfigError::DuplicateName {
                path: format!("proxies[{index}].name"),
                name: name.clone(),
            });
        }
    }
    for (index, name) in groups {
        if !names.insert(name.clone()) {
            return Err(ConfigError::DuplicateName {
                path: format!("proxy-groups[{index}].name"),
                name: name.clone(),
            });
        }
    }
    Ok(())
}

fn validate_group_references(
    document: &Mapping,
    policy_targets: &BTreeSet<String>,
    proxy_provider_names: &BTreeSet<String>,
) -> Result<(), ConfigError> {
    let Some(Value::Sequence(groups)) = document.get("proxy-groups") else {
        return Ok(());
    };
    for (index, group) in groups.iter().enumerate() {
        let group = group
            .as_mapping()
            .expect("the field catalog requires proxy group mappings");
        for (field, available, reference_kind) in [
            ("proxies", policy_targets, "proxy or Proxy Group"),
            ("use", proxy_provider_names, "proxy provider"),
        ] {
            let Some(Value::Sequence(references)) = group.get(field) else {
                continue;
            };
            for (reference_index, reference) in references.iter().enumerate() {
                let name = reference
                    .as_str()
                    .expect("the field catalog requires string references");
                if !available.contains(name) {
                    return Err(ConfigError::UnavailableReference {
                        path: format!("proxy-groups[{index}].{field}[{reference_index}]"),
                        reference_kind,
                        name: name.to_owned(),
                    });
                }
            }
        }
    }
    Ok(())
}

fn validate_rule_strings<'a>(
    rules: impl IntoIterator<Item = &'a str>,
    path: &str,
    policy_targets: &BTreeSet<String>,
    rule_provider_names: &BTreeSet<String>,
    sub_rule_names: &BTreeSet<String>,
) -> Result<(), ConfigError> {
    for (index, rule) in rules.into_iter().enumerate() {
        let rule_path = format!("{path}[{index}]");
        let rule = RuleString::new(rule, RULE_STRING_MAX_BYTES).map_err(|error| {
            ConfigError::InvalidRoutingRule {
                path: rule_path.clone(),
                reason: error.to_string(),
            }
        })?;
        let parsed = parse_rule(&rule).map_err(|error| ConfigError::InvalidRoutingRule {
            path: rule_path.clone(),
            reason: error.to_string(),
        })?;
        if parsed.params.iter().any(|parameter| parameter.is_empty()) {
            return Err(ConfigError::InvalidRoutingRule {
                path: rule_path,
                reason: "empty rule parameters are unsupported".to_owned(),
            });
        }

        match parsed.rule_type {
            RuleType::SubRule => {
                validate_reference(&rule_path, "sub-rule", parsed.policy_target, sub_rule_names)?
            }
            _ => validate_reference(
                &rule_path,
                "Policy Target",
                parsed.policy_target,
                policy_targets,
            )?,
        }
        if parsed.rule_type == RuleType::RuleSet {
            let provider = parsed.payload.expect("RULE-SET rules have a payload");
            validate_reference(&rule_path, "rule provider", provider, rule_provider_names)?;
        }
    }
    Ok(())
}

fn validate_reference(
    path: &str,
    reference_kind: &'static str,
    name: &str,
    available: &BTreeSet<String>,
) -> Result<(), ConfigError> {
    if available.contains(name) {
        Ok(())
    } else {
        Err(ConfigError::UnavailableReference {
            path: path.to_owned(),
            reference_kind,
            name: name.to_owned(),
        })
    }
}

fn builtin_policy_targets() -> BTreeSet<String> {
    let mut targets = reserved_proxy_names();
    targets.insert("GLOBAL".to_owned());
    targets
}

fn reserved_proxy_names() -> BTreeSet<String> {
    [
        "COMPATIBLE",
        "DIRECT",
        "PASS",
        "PASS-RULE",
        "REJECT",
        "REJECT-DROP",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect()
}

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
enum SchemaNode {
    Scalar,
    String,
    Boolean,
    Integer,
    ManagedDrop,
    OneOf {
        options: Vec<SchemaNode>,
    },
    Sequence {
        item: Box<SchemaNode>,
    },
    Mapping {
        fields: BTreeMap<String, SchemaNode>,
        #[serde(default)]
        required: Vec<String>,
    },
    NamedMap {
        value: Box<SchemaNode>,
    },
    Discriminated {
        discriminator: String,
        common: BTreeMap<String, SchemaNode>,
        variants: BTreeMap<String, BTreeMap<String, SchemaNode>>,
        #[serde(default)]
        required: Vec<String>,
        #[serde(default, rename = "variant-required")]
        variant_required: BTreeMap<String, Vec<String>>,
    },
}

fn validate_node(value: &Value, schema: &SchemaNode, path: &str) -> Result<(), ConfigError> {
    match schema {
        SchemaNode::ManagedDrop => Ok(()),
        SchemaNode::Scalar if is_scalar(value) => Ok(()),
        SchemaNode::Scalar => Err(invalid_shape(path, "a scalar")),
        SchemaNode::String if value.is_string() => Ok(()),
        SchemaNode::String => Err(invalid_shape(path, "a string")),
        SchemaNode::Boolean if value.is_bool() => Ok(()),
        SchemaNode::Boolean => Err(invalid_shape(path, "a boolean")),
        SchemaNode::Integer if is_integer(value) => Ok(()),
        SchemaNode::Integer => Err(invalid_shape(path, "an integer")),
        SchemaNode::OneOf { options } => {
            if options
                .iter()
                .any(|option| validate_node(value, option, path).is_ok())
            {
                Ok(())
            } else {
                Err(invalid_shape(path, "one of the catalog shapes"))
            }
        }
        SchemaNode::Sequence { item } => {
            let Value::Sequence(values) = value else {
                return Err(invalid_shape(path, "a sequence"));
            };
            for (index, value) in values.iter().enumerate() {
                validate_node(value, item, &format!("{path}[{index}]"))?;
            }
            Ok(())
        }
        SchemaNode::Mapping { fields, required } => {
            let Value::Mapping(mapping) = value else {
                return Err(invalid_shape(path, "a mapping"));
            };
            validate_required_fields(mapping, required, path)?;
            validate_known_mapping(mapping, fields, path)
        }
        SchemaNode::NamedMap {
            value: value_schema,
        } => {
            let Value::Mapping(mapping) = value else {
                return Err(invalid_shape(path, "a mapping"));
            };
            for (name, value) in mapping {
                let Some(name) = name.as_str() else {
                    return Err(invalid_shape(path, "a string-keyed mapping"));
                };
                if name.is_empty() {
                    return Err(ConfigError::EmptyName {
                        path: path.to_owned(),
                    });
                }
                validate_node(value, value_schema, &child_path(path, name))?;
            }
            Ok(())
        }
        SchemaNode::Discriminated {
            discriminator,
            common,
            variants,
            required,
            variant_required,
        } => {
            let Value::Mapping(mapping) = value else {
                return Err(invalid_shape(path, "a mapping"));
            };
            let variant = mapping
                .get(discriminator.as_str())
                .and_then(Value::as_str)
                .ok_or_else(|| ConfigError::MissingDiscriminator {
                    path: path.to_owned(),
                    field: discriminator.clone(),
                })?;
            let variant_fields =
                variants
                    .get(variant)
                    .ok_or_else(|| ConfigError::UnsupportedVariant {
                        path: child_path(path, discriminator),
                        value: variant.to_owned(),
                    })?;
            validate_required_fields(mapping, required, path)?;
            if let Some(required) = variant_required.get(variant) {
                validate_required_fields(mapping, required, path)?;
            }
            for (key, value) in mapping {
                let Some(key) = key.as_str() else {
                    return Err(invalid_shape(path, "a string-keyed mapping"));
                };
                let field_schema = common
                    .get(key)
                    .or_else(|| variant_fields.get(key))
                    .ok_or_else(|| ConfigError::UnsupportedField {
                        path: child_path(path, key),
                    })?;
                validate_node(value, field_schema, &child_path(path, key))?;
            }
            Ok(())
        }
    }
}

fn validate_known_mapping(
    mapping: &Mapping,
    fields: &BTreeMap<String, SchemaNode>,
    path: &str,
) -> Result<(), ConfigError> {
    for (key, value) in mapping {
        let Some(key) = key.as_str() else {
            return Err(invalid_shape(path, "a string-keyed mapping"));
        };
        let field_path = child_path(path, key);
        let schema = fields
            .get(key)
            .ok_or_else(|| ConfigError::UnsupportedField {
                path: field_path.clone(),
            })?;
        validate_node(value, schema, &field_path)?;
    }
    Ok(())
}

fn validate_required_fields(
    mapping: &Mapping,
    required: &[String],
    path: &str,
) -> Result<(), ConfigError> {
    for field in required {
        if !mapping.contains_key(field.as_str()) {
            return Err(ConfigError::MissingField {
                path: child_path(path, field),
            });
        }
    }
    Ok(())
}

fn strip_managed_mapping(mapping: &mut Mapping, fields: &BTreeMap<String, SchemaNode>) {
    for (field, schema) in fields {
        if matches!(schema, SchemaNode::ManagedDrop) {
            mapping.remove(field.as_str());
        } else if let Some(value) = mapping.get_mut(field.as_str()) {
            strip_managed_node(value, schema);
        }
    }
}

fn strip_managed_node(value: &mut Value, schema: &SchemaNode) {
    match (value, schema) {
        (Value::Sequence(values), SchemaNode::Sequence { item }) => {
            for value in values {
                strip_managed_node(value, item);
            }
        }
        (Value::Mapping(mapping), SchemaNode::Mapping { fields, .. }) => {
            strip_managed_mapping(mapping, fields);
        }
        (
            Value::Mapping(mapping),
            SchemaNode::NamedMap {
                value: value_schema,
            },
        ) => {
            for value in mapping.values_mut() {
                strip_managed_node(value, value_schema);
            }
        }
        (
            Value::Mapping(mapping),
            SchemaNode::Discriminated {
                discriminator,
                common,
                variants,
                ..
            },
        ) => {
            let Some(variant) = mapping
                .get(discriminator.as_str())
                .and_then(Value::as_str)
                .and_then(|variant| variants.get(variant))
            else {
                return;
            };
            for (field, schema) in common.iter().chain(variant) {
                if matches!(schema, SchemaNode::ManagedDrop) {
                    mapping.remove(field.as_str());
                } else if let Some(value) = mapping.get_mut(field.as_str()) {
                    strip_managed_node(value, schema);
                }
            }
        }
        _ => {}
    }
}

fn classify_provider_section(
    document: &mut Mapping,
    section_name: &str,
    section: ProviderSection,
    staging_root: &Path,
    records: &mut Vec<ProviderRecord>,
) -> Result<(), ConfigError> {
    let Some(Value::Mapping(providers)) = document.get_mut(section_name) else {
        return Ok(());
    };
    for (name, provider) in providers {
        let name = name
            .as_str()
            .expect("the field catalog already requires string provider names")
            .to_owned();
        let Value::Mapping(provider) = provider else {
            return Err(invalid_shape(&child_path(section_name, &name), "a mapping"));
        };
        let provider_path = child_path(section_name, &name);
        match provider.get("type").and_then(Value::as_str) {
            Some("http") => {
                let url = provider
                    .get("url")
                    .and_then(Value::as_str)
                    .ok_or_else(|| ConfigError::ProviderUrlRequired {
                        path: provider_path.clone(),
                    })?
                    .to_owned();
                let parsed_url = Url::parse(&url).map_err(|_| ConfigError::ProviderUrlInvalid {
                    path: provider_path.clone(),
                })?;
                if !matches!(parsed_url.scheme(), "http" | "https")
                    || parsed_url.host_str().is_none()
                {
                    return Err(ConfigError::ProviderUrlInvalid {
                        path: provider_path,
                    });
                }
                let relative_path = controlled_remote_path(section, &name, &url);
                provider.insert("path".into(), path_string(&relative_path).into());
                records.push(ProviderRecord {
                    section,
                    name,
                    relative_path,
                    kind: ProviderKind::Remote { url },
                });
            }
            Some("file") => {
                let raw_path = provider
                    .get("path")
                    .and_then(Value::as_str)
                    .ok_or_else(|| ConfigError::ProviderPathRequired {
                        path: provider_path.clone(),
                    })?;
                let (source, relative_path) = resolve_local_provider(staging_root, raw_path)?;
                provider.insert("path".into(), path_string(&relative_path).into());
                records.push(ProviderRecord {
                    section,
                    name,
                    relative_path,
                    kind: ProviderKind::Local { source },
                });
            }
            Some("inline") => {
                provider.remove("path");
            }
            _ => unreachable!("the field catalog already validates provider types"),
        }
    }
    Ok(())
}

fn resolve_local_provider(
    staging_root: &Path,
    raw_path: &str,
) -> Result<(PathBuf, PathBuf), ConfigError> {
    let path = Path::new(raw_path);
    if path
        .components()
        .any(|component| matches!(component, Component::ParentDir))
    {
        return Err(ConfigError::ProviderPathTraversal {
            path: path.to_path_buf(),
        });
    }
    let candidate = if path.is_absolute() {
        path.to_path_buf()
    } else {
        staging_root.join(path)
    };
    let source =
        std::fs::canonicalize(&candidate).map_err(|_| ConfigError::ProviderFileUnavailable {
            path: candidate.clone(),
        })?;
    let relative_path = source
        .strip_prefix(staging_root)
        .map_err(|_| ConfigError::ProviderPathOutsideStagingRoot {
            path: source.clone(),
        })?
        .to_path_buf();
    if relative_path.as_os_str().is_empty() {
        return Err(ConfigError::ProviderPathOutsideStagingRoot { path: source });
    }
    if !source.is_file() {
        return Err(ConfigError::ProviderFileUnavailable { path: source });
    }
    Ok((source, relative_path))
}

fn controlled_remote_path(section: ProviderSection, name: &str, url: &str) -> PathBuf {
    let mut identity = Vec::with_capacity(section.directory().len() + name.len() + url.len() + 2);
    identity.extend_from_slice(section.directory().as_bytes());
    identity.push(0);
    identity.extend_from_slice(name.as_bytes());
    identity.push(0);
    identity.extend_from_slice(url.as_bytes());
    let digest = sha256_hex(&identity);
    PathBuf::from("providers")
        .join(section.directory())
        .join(format!("{digest}.yaml"))
}

fn canonicalize(value: Value) -> Value {
    match value {
        Value::Sequence(values) => Value::Sequence(values.into_iter().map(canonicalize).collect()),
        Value::Mapping(mapping) => {
            let mut entries = mapping.into_iter().collect::<Vec<_>>();
            entries.sort_by_key(|(key, _)| yaml_key(key));
            let mut sorted = Mapping::new();
            for (key, value) in entries {
                sorted.insert(key, canonicalize(value));
            }
            Value::Mapping(sorted)
        }
        other => other,
    }
}

fn yaml_key(value: &Value) -> String {
    value
        .as_str()
        .map(str::to_owned)
        .unwrap_or_else(|| serde_yaml_ng::to_string(value).unwrap_or_else(|_| format!("{value:?}")))
}

fn child_path(parent: &str, child: &str) -> String {
    if parent.is_empty() {
        child.to_owned()
    } else {
        format!("{parent}.{child}")
    }
}

fn invalid_shape(path: &str, expected: &'static str) -> ConfigError {
    ConfigError::InvalidShape {
        path: path.to_owned(),
        expected,
    }
}

fn is_scalar(value: &Value) -> bool {
    matches!(value, Value::Bool(_) | Value::Number(_) | Value::String(_))
}

fn is_integer(value: &Value) -> bool {
    match value {
        Value::Number(number) => number.as_i64().is_some() || number.as_u64().is_some(),
        _ => false,
    }
}

fn policy_sha256() -> String {
    let managed = b"rules=local\ntun.enable=true\nmode=rule\nallow-lan=false";
    let mut policy = Vec::with_capacity(
        COMPILER_POLICY_REVISION.len() + BUNDLED_CATALOG.len() + managed.len() + 2,
    );
    policy.extend_from_slice(COMPILER_POLICY_REVISION.as_bytes());
    policy.push(0);
    policy.extend_from_slice(BUNDLED_CATALOG.as_bytes());
    policy.push(0);
    policy.extend_from_slice(managed);
    sha256_hex(&policy)
}

fn path_string(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}
