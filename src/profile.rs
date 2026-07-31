use crate::domain::{NodeRecordId, ProfileId, SubscriptionUrl, is_token_like_path_segment};
use serde_yaml_ng::{Mapping, Value};
use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SnapshotLimits {
    pub max_bytes: usize,
    pub max_depth: usize,
}

impl SnapshotLimits {
    #[must_use]
    pub const fn new(max_bytes: usize, max_depth: usize) -> Self {
        Self {
            max_bytes,
            max_depth,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SnapshotError {
    BodyTooLarge { limit: usize },
    InvalidYaml,
    TopLevelMappingRequired,
    DepthExceeded { limit: usize },
    RulesSequenceRequired,
    RuleStringRequired { index: usize },
}

#[derive(Clone, PartialEq)]
pub struct ProfileSnapshot {
    raw: Arc<[u8]>,
    document: Mapping,
    rule_strings: Vec<String>,
    content_sha256: String,
}

impl ProfileSnapshot {
    pub fn parse(raw: &[u8], limits: SnapshotLimits) -> Result<Self, SnapshotError> {
        if raw.len() > limits.max_bytes {
            return Err(SnapshotError::BodyTooLarge {
                limit: limits.max_bytes,
            });
        }

        let parse_bytes = raw.strip_prefix(&[0xef, 0xbb, 0xbf]).unwrap_or(raw);
        let value: Value =
            serde_yaml_ng::from_slice(parse_bytes).map_err(|_| SnapshotError::InvalidYaml)?;
        if yaml_depth(&value) > limits.max_depth {
            return Err(SnapshotError::DepthExceeded {
                limit: limits.max_depth,
            });
        }
        let Value::Mapping(document) = value else {
            return Err(SnapshotError::TopLevelMappingRequired);
        };
        let rule_strings = extract_rules(&document)?;
        let content_sha256 = crate::digest::sha256_hex(raw);

        Ok(Self {
            raw: Arc::from(raw),
            document,
            rule_strings,
            content_sha256,
        })
    }

    #[must_use]
    pub fn raw(&self) -> &[u8] {
        &self.raw
    }

    #[must_use]
    pub fn rule_strings(&self) -> &[String] {
        &self.rule_strings
    }

    #[must_use]
    pub fn content_sha256(&self) -> &str {
        &self.content_sha256
    }

    #[must_use]
    pub fn document(&self) -> &Mapping {
        &self.document
    }
}

impl fmt::Debug for ProfileSnapshot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProfileSnapshot")
            .field("raw_bytes", &self.raw.len())
            .field("rule_count", &self.rule_strings.len())
            .field("content_sha256", &self.content_sha256)
            .finish()
    }
}

fn extract_rules(document: &Mapping) -> Result<Vec<String>, SnapshotError> {
    let Some(rules) = document.get(Value::String("rules".to_owned())) else {
        return Ok(Vec::new());
    };
    let Value::Sequence(rules) = rules else {
        return Err(SnapshotError::RulesSequenceRequired);
    };

    rules
        .iter()
        .enumerate()
        .map(|(index, rule)| match rule {
            Value::String(rule) => Ok(rule.clone()),
            _ => Err(SnapshotError::RuleStringRequired { index }),
        })
        .collect()
}

fn yaml_depth(value: &Value) -> usize {
    match value {
        Value::Sequence(values) => 1 + values.iter().map(yaml_depth).max().unwrap_or_default(),
        Value::Mapping(values) => {
            1 + values
                .iter()
                .map(|(key, value)| yaml_depth(key).max(yaml_depth(value)))
                .max()
                .unwrap_or_default()
        }
        Value::Tagged(tagged) => 1 + yaml_depth(&tagged.value),
        _ => 1,
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProfileNameSource {
    Metadata,
    Filename,
    HostAndShortId,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DerivedProfileName {
    pub value: String,
    pub source: ProfileNameSource,
}

#[must_use]
pub fn derive_profile_name(
    metadata_name: Option<&str>,
    subscription_url: &SubscriptionUrl,
    profile_id: ProfileId,
) -> DerivedProfileName {
    if let Some(value) = metadata_name.and_then(normalize_name) {
        return DerivedProfileName {
            value,
            source: ProfileNameSource::Metadata,
        };
    }

    if let Some(value) = filename_name(subscription_url) {
        return DerivedProfileName {
            value,
            source: ProfileNameSource::Filename,
        };
    }

    let host = subscription_url.expose().host_str().unwrap_or("profile");
    let id = profile_id.to_string();
    DerivedProfileName {
        value: format!("{host}-{}", &id[..8]),
        source: ProfileNameSource::HostAndShortId,
    }
}

fn filename_name(subscription_url: &SubscriptionUrl) -> Option<String> {
    let filename = subscription_url.expose().path_segments()?.next_back()?;
    if is_token_like_path_segment(filename) {
        return None;
    }
    let stem = filename
        .strip_suffix(".yaml")
        .or_else(|| filename.strip_suffix(".yml"))
        .unwrap_or(filename);
    normalize_name(stem)
}

fn normalize_name(value: &str) -> Option<String> {
    let value = value.trim();
    if value.is_empty() || value.chars().any(char::is_control) {
        return None;
    }
    Some(value.chars().take(80).collect())
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ProfileRevision(pub u64);

impl ProfileRevision {
    fn next(self) -> Option<Self> {
        self.0.checked_add(1).map(Self)
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd)]
pub struct ActiveProfileRevision(pub u64);

impl ActiveProfileRevision {
    fn next(self) -> Option<Self> {
        self.0.checked_add(1).map(Self)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RefreshContext {
    pub profile_revision: ProfileRevision,
    pub active_revision: Option<ActiveProfileRevision>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RefreshStage {
    Download,
    Parse,
    Validate,
    Apply,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RefreshFailure {
    pub stage: RefreshStage,
    pub safe_message: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Profile {
    pub id: ProfileId,
    pub name: String,
    pub subscription_url: SubscriptionUrl,
    pub snapshot: ProfileSnapshot,
    pub revision: ProfileRevision,
    pub last_success_at_unix_ms: u64,
    pub last_error: Option<RefreshFailure>,
    pub next_refresh_at_unix_ms: u64,
    pub selections: BTreeMap<String, NodeRecordId>,
}

impl Profile {
    #[must_use]
    pub fn new(
        id: ProfileId,
        name: String,
        subscription_url: SubscriptionUrl,
        snapshot: ProfileSnapshot,
        last_success_at_unix_ms: u64,
        next_refresh_at_unix_ms: u64,
    ) -> Self {
        Self {
            id,
            name,
            subscription_url,
            snapshot,
            revision: ProfileRevision(1),
            last_success_at_unix_ms,
            last_error: None,
            next_refresh_at_unix_ms,
            selections: BTreeMap::new(),
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct ProfileCatalog {
    profiles: BTreeMap<ProfileId, Profile>,
    active_profile_id: Option<ProfileId>,
    active_revision: ActiveProfileRevision,
}

impl ProfileCatalog {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, profile: Profile) -> Result<(), ProfileInsertError> {
        if self.profiles.contains_key(&profile.id) {
            return Err(ProfileInsertError::DuplicateId(profile.id));
        }
        self.profiles.insert(profile.id, profile);
        Ok(())
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.profiles.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.profiles.is_empty()
    }

    #[must_use]
    pub fn active_profile_id(&self) -> Option<ProfileId> {
        self.active_profile_id
    }

    #[must_use]
    pub fn active_revision(&self) -> ActiveProfileRevision {
        self.active_revision
    }

    #[must_use]
    pub fn get(&self, id: ProfileId) -> Option<&Profile> {
        self.profiles.get(&id)
    }

    pub fn get_mut(&mut self, id: ProfileId) -> Option<&mut Profile> {
        self.profiles.get_mut(&id)
    }

    pub fn profiles(&self) -> impl ExactSizeIterator<Item = &Profile> {
        self.profiles.values()
    }

    pub fn restore(
        profiles: Vec<Profile>,
        active_profile_id: ProfileId,
        active_revision: ActiveProfileRevision,
    ) -> Result<Self, ProfileRestoreError> {
        if profiles.is_empty() || profiles.len() > crate::constants::PROFILE_COUNT_MAX {
            return Err(ProfileRestoreError::InvalidProfileCount);
        }
        if active_revision.0 == 0 {
            return Err(ProfileRestoreError::InvalidActiveRevision);
        }
        let mut catalog = Self::new();
        for profile in profiles {
            catalog
                .insert(profile)
                .map_err(|_| ProfileRestoreError::DuplicateProfileId)?;
        }
        if !catalog.profiles.contains_key(&active_profile_id) {
            return Err(ProfileRestoreError::ActiveProfileMissing);
        }
        catalog.active_profile_id = Some(active_profile_id);
        catalog.active_revision = active_revision;
        Ok(catalog)
    }

    pub fn resolve(&self, selector: &str) -> Result<ProfileId, ProfileSelectorError> {
        if let Ok(id) = ProfileId::parse(selector) {
            return self
                .profiles
                .contains_key(&id)
                .then_some(id)
                .ok_or(ProfileSelectorError::NotFound);
        }

        let candidate_ids = self
            .profiles
            .values()
            .filter(|profile| profile.name == selector)
            .map(|profile| profile.id)
            .collect::<Vec<_>>();
        match candidate_ids.as_slice() {
            [] => Err(ProfileSelectorError::NotFound),
            [id] => Ok(*id),
            _ => Err(ProfileSelectorError::Ambiguous { candidate_ids }),
        }
    }

    pub fn activate(&mut self, selector: &str) -> Result<ProfileId, ProfileSelectorError> {
        let id = self.resolve(selector)?;
        if self.active_profile_id == Some(id) {
            return Ok(id);
        }
        self.active_revision = self
            .active_revision
            .next()
            .ok_or(ProfileSelectorError::ActiveRevisionExhausted)?;
        self.active_profile_id = Some(id);
        Ok(id)
    }

    pub fn refresh_context(&self, id: ProfileId) -> Result<RefreshContext, RefreshCommitError> {
        let profile = self
            .profiles
            .get(&id)
            .ok_or(RefreshCommitError::ProfileRemoved)?;
        Ok(RefreshContext {
            profile_revision: profile.revision,
            active_revision: (self.active_profile_id == Some(id)).then_some(self.active_revision),
        })
    }

    pub fn remove(&mut self, selector: &str) -> Result<Profile, ProfileSelectorError> {
        let id = self.resolve(selector)?;
        if self.active_profile_id == Some(id) {
            return Err(ProfileSelectorError::Active);
        }
        self.profiles
            .remove(&id)
            .ok_or(ProfileSelectorError::NotFound)
    }

    pub fn commit_refresh(
        &mut self,
        id: ProfileId,
        context: RefreshContext,
        snapshot: ProfileSnapshot,
        success_at_unix_ms: u64,
        next_refresh_at_unix_ms: u64,
    ) -> Result<ProfileRevision, RefreshCommitError> {
        let profile = self
            .profiles
            .get_mut(&id)
            .ok_or(RefreshCommitError::ProfileRemoved)?;
        if profile.revision != context.profile_revision {
            return Err(RefreshCommitError::StaleRevision {
                expected: context.profile_revision,
                actual: profile.revision,
            });
        }
        if self.active_profile_id == Some(id)
            && context.active_revision != Some(self.active_revision)
        {
            return Err(RefreshCommitError::ActiveRevisionChanged {
                expected: context.active_revision,
                actual: self.active_revision,
            });
        }
        let revision = profile
            .revision
            .next()
            .ok_or(RefreshCommitError::RevisionExhausted)?;
        profile.snapshot = snapshot;
        profile.revision = revision;
        profile.last_success_at_unix_ms = success_at_unix_ms;
        profile.last_error = None;
        profile.next_refresh_at_unix_ms = next_refresh_at_unix_ms;
        Ok(revision)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProfileSelectorError {
    NotFound,
    Ambiguous { candidate_ids: Vec<ProfileId> },
    Active,
    ActiveRevisionExhausted,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProfileInsertError {
    DuplicateId(ProfileId),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProfileRestoreError {
    InvalidProfileCount,
    DuplicateProfileId,
    ActiveProfileMissing,
    InvalidActiveRevision,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RefreshCommitError {
    ProfileRemoved,
    StaleRevision {
        expected: ProfileRevision,
        actual: ProfileRevision,
    },
    ActiveRevisionChanged {
        expected: Option<ActiveProfileRevision>,
        actual: ActiveProfileRevision,
    },
    RevisionExhausted,
}
