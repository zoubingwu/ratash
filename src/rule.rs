use std::fmt;

use serde::{Deserialize, Serialize};

use crate::domain::LocalRuleSetRevision;

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct RuleString(String);

impl RuleString {
    pub fn new(value: impl Into<String>, max_bytes: usize) -> Result<Self, RuleStringError> {
        let value = value.into();
        let actual_bytes = value.len();
        if actual_bytes > max_bytes {
            return Err(RuleStringError::TooLarge {
                actual_bytes,
                max_bytes,
            });
        }

        Ok(Self(value))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for RuleString {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RuleStringError {
    TooLarge {
        actual_bytes: usize,
        max_bytes: usize,
    },
}

impl fmt::Display for RuleStringError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TooLarge {
                actual_bytes,
                max_bytes,
            } => write!(
                formatter,
                "rule string is {actual_bytes} bytes and exceeds the {max_bytes}-byte limit"
            ),
        }
    }
}

impl std::error::Error for RuleStringError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuleType {
    And,
    Domain,
    DomainKeyword,
    DomainRegex,
    DomainSuffix,
    DomainWildcard,
    Dscp,
    DstPort,
    GeoIp,
    Geosite,
    InName,
    InPort,
    InType,
    InUser,
    IpAsn,
    IpCidr,
    IpCidr6,
    IpSuffix,
    Match,
    Network,
    Not,
    Or,
    ProcessName,
    ProcessNameRegex,
    ProcessNameWildcard,
    ProcessPath,
    ProcessPathRegex,
    ProcessPathWildcard,
    RematchName,
    RuleSet,
    SrcGeoIp,
    SrcIpAsn,
    SrcIpCidr,
    SrcIpSuffix,
    SrcPort,
    SubRule,
    Uid,
}

impl RuleType {
    fn parse(value: &str) -> Result<Self, RuleParseError> {
        match value {
            "AND" => Ok(Self::And),
            "DOMAIN" => Ok(Self::Domain),
            "DOMAIN-KEYWORD" => Ok(Self::DomainKeyword),
            "DOMAIN-REGEX" => Ok(Self::DomainRegex),
            "DOMAIN-SUFFIX" => Ok(Self::DomainSuffix),
            "DOMAIN-WILDCARD" => Ok(Self::DomainWildcard),
            "DSCP" => Ok(Self::Dscp),
            "DST-PORT" => Ok(Self::DstPort),
            "GEOIP" => Ok(Self::GeoIp),
            "GEOSITE" => Ok(Self::Geosite),
            "IN-NAME" => Ok(Self::InName),
            "IN-PORT" => Ok(Self::InPort),
            "IN-TYPE" => Ok(Self::InType),
            "IN-USER" => Ok(Self::InUser),
            "IP-ASN" => Ok(Self::IpAsn),
            "IP-CIDR" => Ok(Self::IpCidr),
            "IP-CIDR6" => Ok(Self::IpCidr6),
            "IP-SUFFIX" => Ok(Self::IpSuffix),
            "MATCH" => Ok(Self::Match),
            "NETWORK" => Ok(Self::Network),
            "NOT" => Ok(Self::Not),
            "OR" => Ok(Self::Or),
            "PROCESS-NAME" => Ok(Self::ProcessName),
            "PROCESS-NAME-REGEX" => Ok(Self::ProcessNameRegex),
            "PROCESS-NAME-WILDCARD" => Ok(Self::ProcessNameWildcard),
            "PROCESS-PATH" => Ok(Self::ProcessPath),
            "PROCESS-PATH-REGEX" => Ok(Self::ProcessPathRegex),
            "PROCESS-PATH-WILDCARD" => Ok(Self::ProcessPathWildcard),
            "REMATCH-NAME" => Ok(Self::RematchName),
            "RULE-SET" => Ok(Self::RuleSet),
            "SRC-GEOIP" => Ok(Self::SrcGeoIp),
            "SRC-IP-ASN" => Ok(Self::SrcIpAsn),
            "SRC-IP-CIDR" => Ok(Self::SrcIpCidr),
            "SRC-IP-SUFFIX" => Ok(Self::SrcIpSuffix),
            "SRC-PORT" => Ok(Self::SrcPort),
            "SUB-RULE" => Ok(Self::SubRule),
            "UID" => Ok(Self::Uid),
            "" => Err(RuleParseError::MissingRuleType),
            other => Err(RuleParseError::UnsupportedRuleType(other.to_owned())),
        }
    }

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::And => "AND",
            Self::Domain => "DOMAIN",
            Self::DomainKeyword => "DOMAIN-KEYWORD",
            Self::DomainRegex => "DOMAIN-REGEX",
            Self::DomainSuffix => "DOMAIN-SUFFIX",
            Self::DomainWildcard => "DOMAIN-WILDCARD",
            Self::Dscp => "DSCP",
            Self::DstPort => "DST-PORT",
            Self::GeoIp => "GEOIP",
            Self::Geosite => "GEOSITE",
            Self::InName => "IN-NAME",
            Self::InPort => "IN-PORT",
            Self::InType => "IN-TYPE",
            Self::InUser => "IN-USER",
            Self::IpAsn => "IP-ASN",
            Self::IpCidr => "IP-CIDR",
            Self::IpCidr6 => "IP-CIDR6",
            Self::IpSuffix => "IP-SUFFIX",
            Self::Match => "MATCH",
            Self::Network => "NETWORK",
            Self::Not => "NOT",
            Self::Or => "OR",
            Self::ProcessName => "PROCESS-NAME",
            Self::ProcessNameRegex => "PROCESS-NAME-REGEX",
            Self::ProcessNameWildcard => "PROCESS-NAME-WILDCARD",
            Self::ProcessPath => "PROCESS-PATH",
            Self::ProcessPathRegex => "PROCESS-PATH-REGEX",
            Self::ProcessPathWildcard => "PROCESS-PATH-WILDCARD",
            Self::RematchName => "REMATCH-NAME",
            Self::RuleSet => "RULE-SET",
            Self::SrcGeoIp => "SRC-GEOIP",
            Self::SrcIpAsn => "SRC-IP-ASN",
            Self::SrcIpCidr => "SRC-IP-CIDR",
            Self::SrcIpSuffix => "SRC-IP-SUFFIX",
            Self::SrcPort => "SRC-PORT",
            Self::SubRule => "SUB-RULE",
            Self::Uid => "UID",
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct ParsedRule<'a> {
    pub original: &'a RuleString,
    pub rule_type: RuleType,
    pub payload: Option<&'a str>,
    pub policy_target: &'a str,
    pub params: Vec<&'a str>,
}

pub fn parse_rule(rule: &RuleString) -> Result<ParsedRule<'_>, RuleParseError> {
    let (rule_type, remainder) = match rule.as_str().split_once(',') {
        Some((rule_type, remainder)) => (RuleType::parse(rule_type.trim())?, remainder),
        None => (RuleType::parse(rule.as_str().trim())?, ""),
    };

    match rule_type {
        RuleType::Match => parse_rule_without_payload(rule, rule_type, remainder),
        RuleType::And | RuleType::Not | RuleType::Or | RuleType::SubRule => {
            parse_logical_rule(rule, rule_type, remainder)
        }
        RuleType::DomainRegex | RuleType::ProcessNameRegex | RuleType::ProcessPathRegex => {
            parse_regex_rule(rule, rule_type, remainder)
        }
        _ => parse_standard_rule(rule, rule_type, remainder),
    }
}

fn parse_regex_rule<'a>(
    rule: &'a RuleString,
    rule_type: RuleType,
    remainder: &'a str,
) -> Result<ParsedRule<'a>, RuleParseError> {
    let mut rule_and_target = remainder;
    let mut params = Vec::new();
    while let Some((head, tail)) = rule_and_target.rsplit_once(',') {
        let parameter = tail.trim();
        if !is_rule_parameter(parameter) || !head.contains(',') {
            break;
        }
        params.push(parameter);
        rule_and_target = head;
    }
    params.reverse();

    let (payload, policy_target) = rule_and_target
        .rsplit_once(',')
        .ok_or(RuleParseError::MissingPolicyTarget(rule_type))?;
    let payload = required_field(
        Some(payload.trim()),
        RuleParseError::MissingPayload(rule_type),
    )?;
    let policy_target = required_field(
        Some(policy_target.trim()),
        RuleParseError::MissingPolicyTarget(rule_type),
    )?;

    Ok(ParsedRule {
        original: rule,
        rule_type,
        payload: Some(payload),
        policy_target,
        params,
    })
}

fn is_rule_parameter(value: &str) -> bool {
    matches!(value, "no-resolve" | "src")
}

fn parse_rule_without_payload<'a>(
    rule: &'a RuleString,
    rule_type: RuleType,
    remainder: &'a str,
) -> Result<ParsedRule<'a>, RuleParseError> {
    let mut fields = remainder.split(',').map(str::trim);
    let policy_target = required_field(
        fields.next(),
        RuleParseError::MissingPolicyTarget(rule_type),
    )?;
    Ok(ParsedRule {
        original: rule,
        rule_type,
        payload: None,
        policy_target,
        params: fields.collect(),
    })
}

fn parse_standard_rule<'a>(
    rule: &'a RuleString,
    rule_type: RuleType,
    remainder: &'a str,
) -> Result<ParsedRule<'a>, RuleParseError> {
    let mut fields = remainder.split(',').map(str::trim);

    let payload = required_field(fields.next(), RuleParseError::MissingPayload(rule_type))?;
    let policy_target = required_field(
        fields.next(),
        RuleParseError::MissingPolicyTarget(rule_type),
    )?;

    Ok(ParsedRule {
        original: rule,
        rule_type,
        payload: Some(payload),
        policy_target,
        params: fields.collect(),
    })
}

fn parse_logical_rule<'a>(
    rule: &'a RuleString,
    rule_type: RuleType,
    remainder: &'a str,
) -> Result<ParsedRule<'a>, RuleParseError> {
    let remainder = remainder.trim_start();
    let payload_end =
        logical_payload_end(remainder).ok_or(RuleParseError::InvalidLogicalPayload(rule_type))?;
    let payload = remainder[..payload_end].trim();
    let tail = remainder[payload_end..]
        .trim_start()
        .strip_prefix(',')
        .ok_or(RuleParseError::MissingPolicyTarget(rule_type))?;
    let mut fields = tail.split(',').map(str::trim);
    let policy_target = required_field(
        fields.next(),
        RuleParseError::MissingPolicyTarget(rule_type),
    )?;

    Ok(ParsedRule {
        original: rule,
        rule_type,
        payload: Some(payload),
        policy_target,
        params: fields.collect(),
    })
}

fn logical_payload_end(value: &str) -> Option<usize> {
    let mut depth = 0_usize;
    let mut escaped = false;
    let mut in_character_class = false;

    for (index, character) in value.char_indices() {
        if escaped {
            escaped = false;
            continue;
        }
        if character == '\\' {
            escaped = true;
            continue;
        }
        if character == '[' {
            in_character_class = true;
            continue;
        }
        if character == ']' {
            in_character_class = false;
            continue;
        }
        if in_character_class {
            continue;
        }

        match character {
            '(' => depth += 1,
            ')' if depth == 0 => return None,
            ')' => {
                depth -= 1;
                if depth == 0 {
                    return Some(index + character.len_utf8());
                }
            }
            _ if depth == 0 && !character.is_whitespace() => return None,
            _ => {}
        }
    }

    None
}

fn required_field(value: Option<&str>, error: RuleParseError) -> Result<&str, RuleParseError> {
    match value {
        Some(value) if !value.is_empty() => Ok(value),
        _ => Err(error),
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RuleParseError {
    MissingRuleType,
    UnsupportedRuleType(String),
    MissingPayload(RuleType),
    MissingPolicyTarget(RuleType),
    InvalidLogicalPayload(RuleType),
}

impl fmt::Display for RuleParseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingRuleType => formatter.write_str("rule type is required"),
            Self::UnsupportedRuleType(rule_type) => {
                write!(formatter, "unsupported rule type: {rule_type}")
            }
            Self::MissingPayload(rule_type) => {
                write!(formatter, "{} rule payload is required", rule_type.as_str())
            }
            Self::MissingPolicyTarget(rule_type) => write!(
                formatter,
                "{} rule policy target is required",
                rule_type.as_str()
            ),
            Self::InvalidLogicalPayload(rule_type) => write!(
                formatter,
                "{} rule payload must be a balanced logical expression",
                rule_type.as_str()
            ),
        }
    }
}

impl std::error::Error for RuleParseError {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocalRuleSet {
    revision: LocalRuleSetRevision,
    rules: Option<Vec<RuleString>>,
}

impl LocalRuleSet {
    #[must_use]
    pub const fn uninitialized() -> Self {
        Self {
            revision: LocalRuleSetRevision(0),
            rules: None,
        }
    }

    #[must_use]
    pub fn initialized(rules: Vec<RuleString>) -> Self {
        Self::initialized_at(rules, LocalRuleSetRevision(1))
    }

    #[must_use]
    pub const fn initialized_at(rules: Vec<RuleString>, revision: LocalRuleSetRevision) -> Self {
        Self {
            revision,
            rules: Some(rules),
        }
    }

    #[must_use]
    pub const fn is_initialized(&self) -> bool {
        self.rules.is_some()
    }

    #[must_use]
    pub const fn revision(&self) -> LocalRuleSetRevision {
        self.revision
    }

    pub fn list(&self) -> Result<RuleList<'_>, RuleParseError> {
        match &self.rules {
            None => Ok(RuleList {
                initialized: false,
                entries: Vec::new(),
            }),
            Some(rules) => {
                let entries = rules
                    .iter()
                    .enumerate()
                    .map(|(index, rule)| {
                        Ok(RuleListEntry {
                            index,
                            rule,
                            parsed: parse_rule(rule)?,
                        })
                    })
                    .collect::<Result<Vec<_>, RuleParseError>>()?;
                Ok(RuleList {
                    initialized: true,
                    entries,
                })
            }
        }
    }

    pub fn from_yaml(
        document: &str,
        revision: LocalRuleSetRevision,
        limits: RuleSetLimits,
    ) -> Result<Self, RuleDocumentError> {
        if document.len() > limits.max_document_bytes {
            return Err(RuleDocumentError::DocumentTooLarge {
                actual_bytes: document.len(),
                max_bytes: limits.max_document_bytes,
            });
        }

        let document: RuleDocument = serde_yaml_ng::from_str(document)
            .map_err(|error| RuleDocumentError::InvalidYaml(error.to_string()))?;
        if document.rules.len() > limits.max_rule_count {
            return Err(RuleDocumentError::TooManyRules {
                actual_rules: document.rules.len(),
                max_rules: limits.max_rule_count,
            });
        }

        let rules = document
            .rules
            .into_iter()
            .enumerate()
            .map(|(index, value)| {
                let rule = RuleString::new(value, limits.max_rule_bytes)
                    .map_err(|source| RuleDocumentError::InvalidRuleString { index, source })?;
                parse_rule(&rule)
                    .map_err(|source| RuleDocumentError::InvalidRule { index, source })?;
                Ok(rule)
            })
            .collect::<Result<Vec<_>, RuleDocumentError>>()?;

        Ok(Self::initialized_at(rules, revision))
    }

    pub fn to_yaml(&self) -> Result<String, RuleDocumentError> {
        let rules = self
            .rules
            .as_deref()
            .ok_or(RuleDocumentError::RulesUninitialized)?;
        let document = RuleDocumentRef { rules };
        serde_yaml_ng::to_string(&document)
            .map_err(|error| RuleDocumentError::SerializationFailed(error.to_string()))
    }

    pub fn add(
        &mut self,
        rule: RuleString,
        placement: RulePlacement,
    ) -> Result<usize, RuleSetError> {
        match &mut self.rules {
            None => Err(RuleSetError::RulesUninitialized),
            Some(rules) => {
                parse_rule(&rule).map_err(RuleSetError::InvalidRule)?;
                let index = match placement {
                    RulePlacement::Prepend => 0,
                    RulePlacement::Append => rules.len(),
                    RulePlacement::Before(anchor) => exact_rule_index(rules, &anchor)?,
                    RulePlacement::After(anchor) => exact_rule_index(rules, &anchor)? + 1,
                };
                let matching_indexes = matching_rule_indexes(rules, &rule);
                if !matching_indexes.is_empty() {
                    return Err(RuleSetError::RuleAlreadyExists { matching_indexes });
                }
                let next_revision = next_revision(self.revision)?;
                rules.insert(index, rule);
                self.revision = next_revision;
                Ok(index)
            }
        }
    }

    pub fn replace(
        &mut self,
        old_rule: &RuleString,
        new_rule: RuleString,
    ) -> Result<usize, RuleSetError> {
        match &mut self.rules {
            None => Err(RuleSetError::RulesUninitialized),
            Some(rules) => {
                parse_rule(&new_rule).map_err(RuleSetError::InvalidRule)?;
                let index = exact_rule_index(rules, old_rule)?;
                let matching_indexes = matching_rule_indexes(rules, &new_rule)
                    .into_iter()
                    .filter(|matching_index| *matching_index != index)
                    .collect::<Vec<_>>();
                if !matching_indexes.is_empty() {
                    return Err(RuleSetError::RuleAlreadyExists { matching_indexes });
                }
                if rules[index] == new_rule {
                    return Ok(index);
                }
                let next_revision = next_revision(self.revision)?;
                rules[index] = new_rule;
                self.revision = next_revision;
                Ok(index)
            }
        }
    }

    pub fn remove(&mut self, rule: &RuleString) -> Result<RuleString, RuleSetError> {
        match &mut self.rules {
            None => Err(RuleSetError::RulesUninitialized),
            Some(rules) => {
                let index = exact_rule_index(rules, rule)?;
                let next_revision = next_revision(self.revision)?;
                let removed = rules.remove(index);
                self.revision = next_revision;
                Ok(removed)
            }
        }
    }
}

impl Default for LocalRuleSet {
    fn default() -> Self {
        Self::uninitialized()
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct RuleList<'a> {
    pub initialized: bool,
    pub entries: Vec<RuleListEntry<'a>>,
}

#[derive(Debug, Eq, PartialEq)]
pub struct RuleListEntry<'a> {
    pub index: usize,
    pub rule: &'a RuleString,
    pub parsed: ParsedRule<'a>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RulePlacement {
    Prepend,
    Append,
    Before(RuleString),
    After(RuleString),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RuleSetError {
    RulesUninitialized,
    RevisionExhausted,
    RuleNotFound,
    RuleAmbiguous { matching_indexes: Vec<usize> },
    RuleAlreadyExists { matching_indexes: Vec<usize> },
    InvalidRule(RuleParseError),
}

impl fmt::Display for RuleSetError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RulesUninitialized => formatter.write_str("local rule set is uninitialized"),
            Self::RevisionExhausted => formatter.write_str("local rule set revision is exhausted"),
            Self::RuleNotFound => formatter.write_str("rule string has no exact match"),
            Self::RuleAmbiguous { matching_indexes } => write!(
                formatter,
                "rule string matches multiple entries at indexes {matching_indexes:?}"
            ),
            Self::RuleAlreadyExists { matching_indexes } => write!(
                formatter,
                "rule string already exists at indexes {matching_indexes:?}"
            ),
            Self::InvalidRule(error) => write!(formatter, "invalid rule string: {error}"),
        }
    }
}

fn next_revision(revision: LocalRuleSetRevision) -> Result<LocalRuleSetRevision, RuleSetError> {
    revision
        .0
        .checked_add(1)
        .map(LocalRuleSetRevision)
        .ok_or(RuleSetError::RevisionExhausted)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RuleSetLimits {
    pub max_document_bytes: usize,
    pub max_rule_bytes: usize,
    pub max_rule_count: usize,
}

impl RuleSetLimits {
    #[must_use]
    pub const fn product() -> Self {
        Self {
            max_document_bytes: crate::constants::LOCAL_RULE_SET_MAX_BYTES,
            max_rule_bytes: crate::constants::RULE_STRING_MAX_BYTES,
            max_rule_count: crate::constants::LOCAL_RULE_COUNT_MAX,
        }
    }
}

impl Default for RuleSetLimits {
    fn default() -> Self {
        Self::product()
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RuleDocument {
    rules: Vec<String>,
}

#[derive(Serialize)]
struct RuleDocumentRef<'a> {
    rules: &'a [RuleString],
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RuleDocumentError {
    RulesUninitialized,
    DocumentTooLarge {
        actual_bytes: usize,
        max_bytes: usize,
    },
    TooManyRules {
        actual_rules: usize,
        max_rules: usize,
    },
    InvalidYaml(String),
    InvalidRuleString {
        index: usize,
        source: RuleStringError,
    },
    InvalidRule {
        index: usize,
        source: RuleParseError,
    },
    SerializationFailed(String),
}

impl fmt::Display for RuleDocumentError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RulesUninitialized => formatter.write_str("local rule set is uninitialized"),
            Self::DocumentTooLarge {
                actual_bytes,
                max_bytes,
            } => write!(
                formatter,
                "rules document is {actual_bytes} bytes and exceeds the {max_bytes}-byte limit"
            ),
            Self::TooManyRules {
                actual_rules,
                max_rules,
            } => write!(
                formatter,
                "rules document has {actual_rules} entries and exceeds the {max_rules}-entry limit"
            ),
            Self::InvalidYaml(error) => write!(formatter, "invalid rules YAML: {error}"),
            Self::InvalidRuleString { index, source } => {
                write!(formatter, "invalid rule string at index {index}: {source}")
            }
            Self::InvalidRule { index, source } => {
                write!(formatter, "invalid rule at index {index}: {source}")
            }
            Self::SerializationFailed(error) => {
                write!(formatter, "failed to serialize rules YAML: {error}")
            }
        }
    }
}

impl std::error::Error for RuleDocumentError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::InvalidRuleString { source, .. } => Some(source),
            Self::InvalidRule { source, .. } => Some(source),
            _ => None,
        }
    }
}

impl std::error::Error for RuleSetError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::InvalidRule(error) => Some(error),
            _ => None,
        }
    }
}

fn exact_rule_index(rules: &[RuleString], target: &RuleString) -> Result<usize, RuleSetError> {
    let matching_indexes = matching_rule_indexes(rules, target);

    match matching_indexes.as_slice() {
        [] => Err(RuleSetError::RuleNotFound),
        [index] => Ok(*index),
        _ => Err(RuleSetError::RuleAmbiguous { matching_indexes }),
    }
}

fn matching_rule_indexes(rules: &[RuleString], target: &RuleString) -> Vec<usize> {
    rules
        .iter()
        .enumerate()
        .filter_map(|(index, rule)| (rule == target).then_some(index))
        .collect()
}
