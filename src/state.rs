use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::io;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::constants::{
    EFFECTIVE_CONFIGURATION_MAX_BYTES, MAX_ACTIVE_NODES, PROFILE_COUNT_MAX,
    PROFILE_METADATA_NAME_MAX_BYTES, SUPERVISOR_STATE_MAX_BYTES,
};
use crate::domain::{
    LocalRuleSetRevision, NodeRecordId, ProfileId, RuntimeGeneration, SubscriptionUrl,
};
use crate::persistence::{ObjectId, PersistenceStore, TransactionBundle};
use crate::profile::{
    ActiveProfileRevision, Profile, ProfileCatalog, ProfileRestoreError, ProfileRevision,
    ProfileSnapshot, RefreshFailure, RefreshStage, SnapshotLimits,
};
use crate::rule::{LocalRuleSet, RuleSetLimits};

const STATE_SCHEMA_VERSION: u16 = 1;
const REFRESH_ERROR_MAX_BYTES: usize = 1_024;

pub struct AuthoritativeState<'a> {
    pub profiles: &'a ProfileCatalog,
    pub local_rules: &'a LocalRuleSet,
    pub effective_configuration: &'a [u8],
    pub runtime_generation: RuntimeGeneration,
}

#[derive(Clone, Debug, PartialEq)]
pub struct HydratedState {
    pub profiles: ProfileCatalog,
    pub local_rules: LocalRuleSet,
    pub effective_configuration: Vec<u8>,
    pub runtime_generation: RuntimeGeneration,
}

pub struct AuthoritativeStateStore {
    persistence: PersistenceStore,
}

impl AuthoritativeStateStore {
    pub fn open(root: impl AsRef<Path>) -> Result<Self, StateStoreError> {
        Ok(Self {
            persistence: PersistenceStore::open(root).map_err(StateStoreError::io)?,
        })
    }

    #[must_use]
    pub fn persistence(&self) -> &PersistenceStore {
        &self.persistence
    }

    pub fn stage_candidate(
        &self,
        candidate: AuthoritativeState<'_>,
    ) -> Result<TransactionBundle, StateStoreError> {
        validate_candidate(&candidate)?;
        if candidate.effective_configuration.len() > EFFECTIVE_CONFIGURATION_MAX_BYTES {
            return Err(StateStoreError::too_large());
        }

        let local_rule_set = candidate
            .local_rules
            .to_yaml()
            .map_err(|_| StateStoreError::invalid())?;
        let local_rule_set = self
            .persistence
            .put_object(local_rule_set.as_bytes())
            .map_err(StateStoreError::io)?;
        let effective_configuration = self
            .persistence
            .put_object(candidate.effective_configuration)
            .map_err(StateStoreError::io)?;

        let active_profile_id = candidate
            .profiles
            .active_profile_id()
            .ok_or_else(StateStoreError::invalid)?;
        let mut profile_records = Vec::with_capacity(candidate.profiles.len());
        let mut active_profile_snapshot = None;
        let mut active_profile_revision = None;
        for profile in candidate.profiles.profiles() {
            validate_profile(profile)?;
            let snapshot = self
                .persistence
                .put_object(profile.snapshot.raw())
                .map_err(StateStoreError::io)?;
            if profile.id == active_profile_id {
                active_profile_snapshot = Some(snapshot.clone());
                active_profile_revision = Some(profile.revision);
            }
            profile_records.push(PersistedProfile::from_profile(profile, snapshot));
        }

        let state = PersistedSupervisorState {
            schema_version: STATE_SCHEMA_VERSION,
            active_profile_id: active_profile_id.to_string(),
            active_revision: candidate.profiles.active_revision().0,
            local_rule_set: local_rule_set.clone(),
            local_rule_set_revision: candidate.local_rules.revision().0,
            effective_configuration: effective_configuration.clone(),
            runtime_generation: candidate.runtime_generation.0,
            profiles: profile_records,
        };
        let state = serde_json::to_vec(&state).map_err(|_| StateStoreError::invalid())?;
        if state.len() > SUPERVISOR_STATE_MAX_BYTES {
            return Err(StateStoreError::too_large());
        }
        let supervisor_state = self
            .persistence
            .put_object(&state)
            .map_err(StateStoreError::io)?;

        Ok(TransactionBundle {
            supervisor_state,
            profile_snapshot: active_profile_snapshot.ok_or_else(StateStoreError::invalid)?,
            local_rule_set,
            effective_configuration,
            profile_revision: active_profile_revision.ok_or_else(StateStoreError::invalid)?,
            local_rule_set_revision: candidate.local_rules.revision(),
            active_profile_id,
            runtime_generation: candidate.runtime_generation,
        })
    }

    pub fn load_committed(
        &self,
        snapshot_limits: SnapshotLimits,
        rule_limits: RuleSetLimits,
    ) -> Result<Option<HydratedState>, StateStoreError> {
        let recovery = self.persistence.recover().map_err(StateStoreError::io)?;
        let Some(manifest) = recovery.committed else {
            return Ok(None);
        };
        let bundle = self
            .persistence
            .load_transaction(&manifest.current)
            .map_err(StateStoreError::io)?;
        let raw_state = self
            .persistence
            .read_object_limited(&bundle.supervisor_state, SUPERVISOR_STATE_MAX_BYTES)
            .map_err(StateStoreError::io)?;
        if raw_state.len() > SUPERVISOR_STATE_MAX_BYTES {
            return Err(StateStoreError::too_large());
        }
        let state: PersistedSupervisorState =
            serde_json::from_slice(&raw_state).map_err(|_| StateStoreError::invalid())?;
        validate_state_header(&state, &bundle)?;

        let active_profile_id =
            ProfileId::parse(&state.active_profile_id).map_err(|_| StateStoreError::invalid())?;
        let mut profiles = Vec::with_capacity(state.profiles.len());
        let mut seen_ids = BTreeSet::new();
        let mut active_snapshot = None;
        let mut active_profile_revision = None;
        for record in state.profiles {
            let id = ProfileId::parse(&record.id).map_err(|_| StateStoreError::invalid())?;
            if !seen_ids.insert(id) {
                return Err(StateStoreError::invalid());
            }
            if id == active_profile_id {
                active_snapshot = Some(record.snapshot.clone());
                active_profile_revision = Some(record.revision);
            }
            profiles.push(record.hydrate(&self.persistence, snapshot_limits)?);
        }
        if active_snapshot.as_ref() != Some(&bundle.profile_snapshot)
            || active_profile_revision != Some(bundle.profile_revision.0)
        {
            return Err(StateStoreError::invalid());
        }

        let profiles = ProfileCatalog::restore(
            profiles,
            active_profile_id,
            ActiveProfileRevision(state.active_revision),
        )
        .map_err(|_| StateStoreError::invalid())?;
        let rules = self
            .persistence
            .read_object_limited(&state.local_rule_set, rule_limits.max_document_bytes)
            .map_err(StateStoreError::io)?;
        let rules = std::str::from_utf8(&rules).map_err(|_| StateStoreError::invalid())?;
        let local_rules = LocalRuleSet::from_yaml(
            rules,
            LocalRuleSetRevision(state.local_rule_set_revision),
            rule_limits,
        )
        .map_err(|_| StateStoreError::invalid())?;
        let effective_configuration = self
            .persistence
            .read_object_limited(
                &state.effective_configuration,
                EFFECTIVE_CONFIGURATION_MAX_BYTES,
            )
            .map_err(StateStoreError::io)?;

        Ok(Some(HydratedState {
            profiles,
            local_rules,
            effective_configuration,
            runtime_generation: RuntimeGeneration(state.runtime_generation),
        }))
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct PersistedSupervisorState {
    schema_version: u16,
    active_profile_id: String,
    active_revision: u64,
    local_rule_set: ObjectId,
    local_rule_set_revision: u64,
    effective_configuration: ObjectId,
    runtime_generation: u64,
    profiles: Vec<PersistedProfile>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct PersistedProfile {
    id: String,
    name: String,
    subscription_url: String,
    snapshot: ObjectId,
    revision: u64,
    last_success_at_unix_ms: u64,
    last_error: Option<PersistedRefreshFailure>,
    next_refresh_at_unix_ms: u64,
    selections: Vec<PersistedSelection>,
}

impl PersistedProfile {
    fn from_profile(profile: &Profile, snapshot: ObjectId) -> Self {
        Self {
            id: profile.id.to_string(),
            name: profile.name.clone(),
            subscription_url: profile.subscription_url.expose().as_str().to_owned(),
            snapshot,
            revision: profile.revision.0,
            last_success_at_unix_ms: profile.last_success_at_unix_ms,
            last_error: profile.last_error.as_ref().map(Into::into),
            next_refresh_at_unix_ms: profile.next_refresh_at_unix_ms,
            selections: profile
                .selections
                .iter()
                .map(|(group, node)| PersistedSelection {
                    group: group.clone(),
                    node_id: node.as_str().to_owned(),
                })
                .collect(),
        }
    }

    fn hydrate(
        self,
        persistence: &PersistenceStore,
        limits: SnapshotLimits,
    ) -> Result<Profile, StateStoreError> {
        let id = ProfileId::parse(&self.id).map_err(|_| StateStoreError::invalid())?;
        validate_name(&self.name)?;
        if self.revision == 0 || self.selections.len() > MAX_ACTIVE_NODES {
            return Err(StateStoreError::invalid());
        }
        let subscription_url = SubscriptionUrl::parse(&self.subscription_url)
            .map_err(|_| StateStoreError::invalid())?;
        let snapshot = persistence
            .read_object_limited(&self.snapshot, limits.max_bytes)
            .map_err(StateStoreError::io)?;
        let snapshot =
            ProfileSnapshot::parse(&snapshot, limits).map_err(|_| StateStoreError::invalid())?;
        let mut selections = BTreeMap::new();
        for selection in self.selections {
            let node =
                NodeRecordId::parse(&selection.node_id).map_err(|_| StateStoreError::invalid())?;
            if selections.insert(selection.group, node).is_some() {
                return Err(StateStoreError::invalid());
            }
        }
        let last_error = self
            .last_error
            .map(PersistedRefreshFailure::hydrate)
            .transpose()?;

        Ok(Profile {
            id,
            name: self.name,
            subscription_url,
            snapshot,
            revision: ProfileRevision(self.revision),
            last_success_at_unix_ms: self.last_success_at_unix_ms,
            last_error,
            next_refresh_at_unix_ms: self.next_refresh_at_unix_ms,
            selections,
        })
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct PersistedSelection {
    group: String,
    node_id: String,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct PersistedRefreshFailure {
    stage: String,
    safe_message: String,
}

impl From<&RefreshFailure> for PersistedRefreshFailure {
    fn from(failure: &RefreshFailure) -> Self {
        Self {
            stage: refresh_stage_name(failure.stage).to_owned(),
            safe_message: failure.safe_message.clone(),
        }
    }
}

impl PersistedRefreshFailure {
    fn hydrate(self) -> Result<RefreshFailure, StateStoreError> {
        if self.safe_message.len() > REFRESH_ERROR_MAX_BYTES
            || self.safe_message.chars().any(char::is_control)
        {
            return Err(StateStoreError::invalid());
        }
        Ok(RefreshFailure {
            stage: parse_refresh_stage(&self.stage)?,
            safe_message: self.safe_message,
        })
    }
}

fn validate_candidate(candidate: &AuthoritativeState<'_>) -> Result<(), StateStoreError> {
    if candidate.profiles.is_empty()
        || candidate.profiles.len() > PROFILE_COUNT_MAX
        || candidate.profiles.active_profile_id().is_none()
        || candidate.profiles.active_revision().0 == 0
        || !candidate.local_rules.is_initialized()
        || candidate.local_rules.revision().0 == 0
        || candidate.runtime_generation.0 == 0
    {
        return Err(StateStoreError::invalid());
    }
    Ok(())
}

fn validate_profile(profile: &Profile) -> Result<(), StateStoreError> {
    validate_name(&profile.name)?;
    if profile.revision.0 == 0
        || profile.selections.len() > MAX_ACTIVE_NODES
        || profile.last_error.as_ref().is_some_and(|error| {
            error.safe_message.len() > REFRESH_ERROR_MAX_BYTES
                || error.safe_message.chars().any(char::is_control)
        })
    {
        return Err(StateStoreError::invalid());
    }
    Ok(())
}

fn validate_name(name: &str) -> Result<(), StateStoreError> {
    if name.is_empty()
        || name.len() > PROFILE_METADATA_NAME_MAX_BYTES
        || name.chars().any(char::is_control)
    {
        Err(StateStoreError::invalid())
    } else {
        Ok(())
    }
}

fn validate_state_header(
    state: &PersistedSupervisorState,
    bundle: &TransactionBundle,
) -> Result<(), StateStoreError> {
    if state.schema_version != STATE_SCHEMA_VERSION
        || state.profiles.is_empty()
        || state.profiles.len() > PROFILE_COUNT_MAX
        || state.active_profile_id != bundle.active_profile_id.to_string()
        || state.local_rule_set != bundle.local_rule_set
        || state.local_rule_set_revision != bundle.local_rule_set_revision.0
        || state.effective_configuration != bundle.effective_configuration
        || state.runtime_generation != bundle.runtime_generation.0
        || state.runtime_generation == 0
    {
        return Err(StateStoreError::invalid());
    }
    Ok(())
}

fn refresh_stage_name(stage: RefreshStage) -> &'static str {
    match stage {
        RefreshStage::Download => "download",
        RefreshStage::Parse => "parse",
        RefreshStage::Validate => "validate",
        RefreshStage::Apply => "apply",
    }
}

fn parse_refresh_stage(value: &str) -> Result<RefreshStage, StateStoreError> {
    match value {
        "download" => Ok(RefreshStage::Download),
        "parse" => Ok(RefreshStage::Parse),
        "validate" => Ok(RefreshStage::Validate),
        "apply" => Ok(RefreshStage::Apply),
        _ => Err(StateStoreError::invalid()),
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StateStoreErrorKind {
    Io,
    StateTooLarge,
    InvalidState,
}

pub struct StateStoreError {
    kind: StateStoreErrorKind,
    source: Option<io::Error>,
}

impl StateStoreError {
    #[must_use]
    pub const fn kind(&self) -> StateStoreErrorKind {
        self.kind
    }

    fn io(source: io::Error) -> Self {
        Self {
            kind: StateStoreErrorKind::Io,
            source: Some(source),
        }
    }

    const fn invalid() -> Self {
        Self {
            kind: StateStoreErrorKind::InvalidState,
            source: None,
        }
    }

    const fn too_large() -> Self {
        Self {
            kind: StateStoreErrorKind::StateTooLarge,
            source: None,
        }
    }
}

impl fmt::Debug for StateStoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StateStoreError")
            .field("kind", &self.kind)
            .finish()
    }
}

impl fmt::Display for StateStoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self.kind {
            StateStoreErrorKind::Io => "authoritative state storage failed",
            StateStoreErrorKind::StateTooLarge => "authoritative state exceeds its size limit",
            StateStoreErrorKind::InvalidState => "authoritative state is invalid",
        })
    }
}

impl std::error::Error for StateStoreError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source
            .as_ref()
            .map(|source| source as &(dyn std::error::Error + 'static))
    }
}

impl From<ProfileRestoreError> for StateStoreError {
    fn from(_: ProfileRestoreError) -> Self {
        Self::invalid()
    }
}
