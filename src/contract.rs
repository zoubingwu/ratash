use serde::Serialize;

pub use crate::error::{ErrorCode, ProcessExitCode};

use crate::application::{self, ApplicationError, ApplicationErrorDetails, ApplicationOutput};
use crate::domain::{
    ActiveProfileSummary, ApplyState, CoreLifecycle, CoreStatus, LatencySample, SampleState,
    SelectedNodeSummary, StatusSnapshot, StreamHealthSet, StreamState, SupervisorLifecycle,
    SupervisorStatus, TrafficSample, TunReason, TunStatus,
};

pub const SCHEMA_VERSION: u16 = 1;

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct JsonEnvelope<T> {
    pub schema_version: u16,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<T>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<ApiError>,
}

impl<T> JsonEnvelope<T> {
    #[must_use]
    pub fn success(data: T) -> Self {
        Self {
            schema_version: SCHEMA_VERSION,
            data: Some(data),
            error: None,
        }
    }

    #[must_use]
    pub fn failure(error: ApiError) -> Self {
        Self {
            schema_version: SCHEMA_VERSION,
            data: None,
            error: Some(error),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ApiError {
    pub code: ErrorCode,
    pub message: String,
    pub retryable: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<serde_json::Value>,
}

impl ApiError {
    #[must_use]
    pub fn new(code: ErrorCode, message: impl Into<String>, retryable: bool) -> Self {
        Self {
            code,
            message: message.into(),
            retryable,
            details: None,
        }
    }

    #[must_use]
    pub fn with_details(mut self, details: serde_json::Value) -> Self {
        self.details = Some(details);
        self
    }
}

impl From<ApplicationError> for ApiError {
    fn from(error: ApplicationError) -> Self {
        let mut details = serde_json::Map::new();
        if let Some(ApplicationErrorDetails::CandidateIds { candidate_ids }) = error.details {
            details.insert("candidate_ids".to_owned(), serde_json::json!(candidate_ids));
        }
        if let Some(selector_details) = error.selector_candidates {
            let candidates = selector_details
                .candidates
                .iter()
                .map(|candidate| {
                    serde_json::json!({
                        "id": candidate.id,
                        "name": candidate.name,
                    })
                })
                .collect::<Vec<_>>();
            details
                .entry("candidate_ids".to_owned())
                .or_insert_with(|| {
                    serde_json::json!(
                        selector_details
                            .candidates
                            .iter()
                            .map(|candidate| &candidate.id)
                            .collect::<Vec<_>>()
                    )
                });
            details.insert(
                "selector".to_owned(),
                serde_json::json!(selector_kind_name(selector_details.selector)),
            );
            details.insert("candidates".to_owned(), serde_json::json!(candidates));
        }
        Self {
            code: error.code,
            message: error.message,
            retryable: error.retryable,
            details: (!details.is_empty()).then_some(serde_json::Value::Object(details)),
        }
    }
}

fn selector_kind_name(kind: application::SelectorKind) -> &'static str {
    match kind {
        application::SelectorKind::Profile => "profile",
        application::SelectorKind::ProxyGroup => "proxy_group",
        application::SelectorKind::Node => "node",
        application::SelectorKind::Rule => "rule",
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(untagged)]
pub enum ApplicationOutputViewV1 {
    Status(StatusViewV1),
    Lifecycle(LifecycleOutcomeViewV1),
    Profiles(ProfileListViewV1),
    ProfileMutation(ProfileMutationViewV1),
    Proxies(ProxyListViewV1),
    ProxySelection(ProxySelectionViewV1),
    Latencies(LatencyListViewV1),
    Latency(LatencyShowViewV1),
    Rules(RuleListViewV1),
    RuleMutation(RuleMutationViewV1),
    LogMetadata(LogMetadataViewV1),
}

impl From<ApplicationOutput> for ApplicationOutputViewV1 {
    fn from(output: ApplicationOutput) -> Self {
        match output {
            ApplicationOutput::Status(status) => Self::Status(status.into()),
            ApplicationOutput::Lifecycle(outcome) => Self::Lifecycle(outcome.into()),
            ApplicationOutput::Profiles(outcome) => Self::Profiles(outcome.into()),
            ApplicationOutput::ProfileMutation(outcome) => Self::ProfileMutation(outcome.into()),
            ApplicationOutput::Proxies(outcome) => Self::Proxies(outcome.into()),
            ApplicationOutput::ProxySelection(outcome) => Self::ProxySelection(outcome.into()),
            ApplicationOutput::Latencies(outcome) => Self::Latencies(outcome.into()),
            ApplicationOutput::Latency(outcome) => Self::Latency(outcome.into()),
            ApplicationOutput::Rules(outcome) => Self::Rules(outcome.into()),
            ApplicationOutput::RuleMutation(outcome) => Self::RuleMutation(outcome.into()),
            ApplicationOutput::LogMetadata(metadata) => Self::LogMetadata(metadata.into()),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct LifecycleOutcomeViewV1 {
    pub action: LifecycleActionViewV1,
    pub changed: bool,
    pub status: StatusViewV1,
}

impl From<application::LifecycleOutcome> for LifecycleOutcomeViewV1 {
    fn from(outcome: application::LifecycleOutcome) -> Self {
        Self {
            action: outcome.action.into(),
            changed: outcome.changed,
            status: outcome.status.into(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LifecycleActionViewV1 {
    Start,
    Stop,
    Restart,
}

impl From<application::LifecycleAction> for LifecycleActionViewV1 {
    fn from(action: application::LifecycleAction) -> Self {
        match action {
            application::LifecycleAction::Start => Self::Start,
            application::LifecycleAction::Stop => Self::Stop,
            application::LifecycleAction::Restart => Self::Restart,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ProfileListViewV1 {
    pub profiles: Vec<ProfileViewV1>,
}

impl From<application::ProfileListOutcome> for ProfileListViewV1 {
    fn from(outcome: application::ProfileListOutcome) -> Self {
        Self {
            profiles: outcome.profiles.into_iter().map(Into::into).collect(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ProfileViewV1 {
    pub id: String,
    pub name: String,
    pub subscription_url: String,
    pub active: bool,
    pub refresh_state: ProfileRefreshStateViewV1,
    pub last_success_at_unix_ms: String,
    pub next_refresh_at_unix_ms: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<ProfileRefreshFailureViewV1>,
}

impl From<application::ProfileSummary> for ProfileViewV1 {
    fn from(profile: application::ProfileSummary) -> Self {
        Self {
            id: profile.id.to_string(),
            name: profile.name,
            subscription_url: profile.subscription_url.redacted(),
            active: profile.active,
            refresh_state: profile.refresh_state.into(),
            last_success_at_unix_ms: profile.last_success_at_unix_ms.to_string(),
            next_refresh_at_unix_ms: profile.next_refresh_at_unix_ms.to_string(),
            last_error: profile.last_error.map(Into::into),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProfileRefreshStateViewV1 {
    Fresh,
    Error,
}

impl From<application::ProfileRefreshState> for ProfileRefreshStateViewV1 {
    fn from(state: application::ProfileRefreshState) -> Self {
        match state {
            application::ProfileRefreshState::Fresh => Self::Fresh,
            application::ProfileRefreshState::Error => Self::Error,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ProfileRefreshFailureViewV1 {
    pub stage: ProfileRefreshStageViewV1,
    pub message: String,
}

impl From<application::ProfileRefreshFailure> for ProfileRefreshFailureViewV1 {
    fn from(failure: application::ProfileRefreshFailure) -> Self {
        Self {
            stage: failure.stage.into(),
            message: failure.message,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProfileRefreshStageViewV1 {
    Download,
    Parse,
    Validate,
    Apply,
}

impl From<application::ProfileRefreshStage> for ProfileRefreshStageViewV1 {
    fn from(stage: application::ProfileRefreshStage) -> Self {
        match stage {
            application::ProfileRefreshStage::Download => Self::Download,
            application::ProfileRefreshStage::Parse => Self::Parse,
            application::ProfileRefreshStage::Validate => Self::Validate,
            application::ProfileRefreshStage::Apply => Self::Apply,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ProfileMutationViewV1 {
    pub action: ProfileMutationActionViewV1,
    pub profile: ProfileViewV1,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub runtime_apply: Option<RuntimeApplyViewV1>,
}

impl From<application::ProfileMutationOutcome> for ProfileMutationViewV1 {
    fn from(outcome: application::ProfileMutationOutcome) -> Self {
        Self {
            action: outcome.action.into(),
            profile: outcome.profile.into(),
            runtime_apply: outcome.runtime_apply.map(Into::into),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProfileMutationActionViewV1 {
    Added,
    Activated,
    Removed,
}

impl From<application::ProfileMutationAction> for ProfileMutationActionViewV1 {
    fn from(action: application::ProfileMutationAction) -> Self {
        match action {
            application::ProfileMutationAction::Added => Self::Added,
            application::ProfileMutationAction::Activated => Self::Activated,
            application::ProfileMutationAction::Removed => Self::Removed,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ProxyListViewV1 {
    pub group: ProxyGroupViewV1,
    pub nodes: Vec<ProxyNodeViewV1>,
}

impl From<application::ProxyListOutcome> for ProxyListViewV1 {
    fn from(outcome: application::ProxyListOutcome) -> Self {
        Self {
            group: outcome.group.into(),
            nodes: outcome.nodes.into_iter().map(Into::into).collect(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ProxyGroupViewV1 {
    pub name: String,
    pub proxy_type: String,
    pub selectable: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub selected_node: Option<SelectorIdentityViewV1>,
}

impl From<application::ProxyGroupSummary> for ProxyGroupViewV1 {
    fn from(group: application::ProxyGroupSummary) -> Self {
        Self {
            name: group.name,
            proxy_type: group.proxy_type,
            selectable: group.selectable,
            selected_node: group.selected_node.map(Into::into),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ProxyNodeViewV1 {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    pub name: String,
    pub member_kind: ProxyMemberKindViewV1,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<ProxyNodeSourceViewV1>,
    pub candidate_ids: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub proxy_type: Option<String>,
    pub availability: ProxyAvailabilityViewV1,
    pub selected: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delay_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sampled_at_unix_ms: Option<String>,
    pub freshness: LatencyFreshnessViewV1,
    pub probe_status: LatencyProbeStatusViewV1,
}

impl From<application::ProxyNodeRow> for ProxyNodeViewV1 {
    fn from(node: application::ProxyNodeRow) -> Self {
        Self {
            id: node.id.map(|id| id.as_str().to_owned()),
            name: node.name,
            member_kind: node.member_kind.into(),
            source: node.source.map(Into::into),
            candidate_ids: node
                .candidate_ids
                .into_iter()
                .map(|id| id.as_str().to_owned())
                .collect(),
            proxy_type: node.proxy_type,
            availability: node.availability.into(),
            selected: node.selected,
            delay_ms: node.delay_ms,
            sampled_at_unix_ms: node.sampled_at_unix_ms.map(|value| value.to_string()),
            freshness: node.freshness.into(),
            probe_status: node.probe_status.into(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProxyMemberKindViewV1 {
    Node,
    Group,
    Missing,
    Ambiguous,
    ProviderUnavailable,
}

impl From<application::ProxyMemberKind> for ProxyMemberKindViewV1 {
    fn from(kind: application::ProxyMemberKind) -> Self {
        match kind {
            application::ProxyMemberKind::Node => Self::Node,
            application::ProxyMemberKind::Group => Self::Group,
            application::ProxyMemberKind::Missing => Self::Missing,
            application::ProxyMemberKind::Ambiguous => Self::Ambiguous,
            application::ProxyMemberKind::ProviderUnavailable => Self::ProviderUnavailable,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ProxyNodeSourceViewV1 {
    Core,
    Provider { provider_name: String },
}

impl From<application::ProxyNodeSource> for ProxyNodeSourceViewV1 {
    fn from(source: application::ProxyNodeSource) -> Self {
        match source {
            application::ProxyNodeSource::Core => Self::Core,
            application::ProxyNodeSource::Provider { provider_name } => {
                Self::Provider { provider_name }
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProxyAvailabilityViewV1 {
    Available,
    Unavailable,
}

impl From<application::ProxyAvailability> for ProxyAvailabilityViewV1 {
    fn from(availability: application::ProxyAvailability) -> Self {
        match availability {
            application::ProxyAvailability::Available => Self::Available,
            application::ProxyAvailability::Unavailable => Self::Unavailable,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct SelectorIdentityViewV1 {
    pub id: String,
    pub name: String,
}

impl From<application::SelectorIdentity> for SelectorIdentityViewV1 {
    fn from(identity: application::SelectorIdentity) -> Self {
        Self {
            id: identity.id,
            name: identity.name,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ProxySelectionViewV1 {
    pub group: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub previous_node: Option<SelectorIdentityViewV1>,
    pub selected_node: SelectorIdentityViewV1,
    pub persisted: bool,
    pub recovery: RecoveryViewV1,
}

impl From<application::ProxySelectionOutcome> for ProxySelectionViewV1 {
    fn from(outcome: application::ProxySelectionOutcome) -> Self {
        Self {
            group: outcome.group,
            previous_node: outcome.previous_node.map(Into::into),
            selected_node: outcome.selected_node.into(),
            persisted: outcome.persisted,
            recovery: outcome.recovery.into(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct LatencyListViewV1 {
    pub samples: Vec<LatencyViewV1>,
}

impl From<application::LatencyListOutcome> for LatencyListViewV1 {
    fn from(outcome: application::LatencyListOutcome) -> Self {
        Self {
            samples: outcome.samples.into_iter().map(Into::into).collect(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct LatencyShowViewV1 {
    pub sample: LatencyViewV1,
}

impl From<application::LatencyShowOutcome> for LatencyShowViewV1 {
    fn from(outcome: application::LatencyShowOutcome) -> Self {
        Self {
            sample: outcome.sample.into(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct LatencyViewV1 {
    pub node_id: String,
    pub node_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delay_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sampled_at_unix_ms: Option<String>,
    pub freshness: LatencyFreshnessViewV1,
    pub probe_status: LatencyProbeStatusViewV1,
    pub probe_generation: String,
}

impl From<application::LatencySummary> for LatencyViewV1 {
    fn from(sample: application::LatencySummary) -> Self {
        Self {
            node_id: sample.node_id.as_str().to_owned(),
            node_name: sample.node_name,
            delay_ms: sample.delay_ms,
            sampled_at_unix_ms: sample.sampled_at_unix_ms.map(|value| value.to_string()),
            freshness: sample.freshness.into(),
            probe_status: sample.probe_status.into(),
            probe_generation: sample.probe_generation.0.to_string(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LatencyFreshnessViewV1 {
    NotSampled,
    Fresh,
    Stale,
    Unavailable,
}

impl From<application::LatencyFreshness> for LatencyFreshnessViewV1 {
    fn from(freshness: application::LatencyFreshness) -> Self {
        match freshness {
            application::LatencyFreshness::NotSampled => Self::NotSampled,
            application::LatencyFreshness::Fresh => Self::Fresh,
            application::LatencyFreshness::Stale => Self::Stale,
            application::LatencyFreshness::Unavailable => Self::Unavailable,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LatencyProbeStatusViewV1 {
    NotSampled,
    Queued,
    InFlight,
    Succeeded,
    Failed,
}

impl From<application::LatencyProbeStatus> for LatencyProbeStatusViewV1 {
    fn from(status: application::LatencyProbeStatus) -> Self {
        match status {
            application::LatencyProbeStatus::NotSampled => Self::NotSampled,
            application::LatencyProbeStatus::Queued => Self::Queued,
            application::LatencyProbeStatus::InFlight => Self::InFlight,
            application::LatencyProbeStatus::Succeeded => Self::Succeeded,
            application::LatencyProbeStatus::Failed => Self::Failed,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct RuleListViewV1 {
    pub initialized: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub revision: Option<String>,
    pub rules: Vec<RuleViewV1>,
}

impl From<application::RuleListOutcome> for RuleListViewV1 {
    fn from(outcome: application::RuleListOutcome) -> Self {
        Self {
            initialized: outcome.initialized,
            revision: outcome.revision.map(|revision| revision.0.to_string()),
            rules: outcome.rules.into_iter().map(Into::into).collect(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct RuleViewV1 {
    pub index: usize,
    pub rule_string: String,
    pub rule_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload: Option<String>,
    pub policy_target: String,
    pub params: Vec<String>,
    pub policy_target_validation: PolicyTargetValidationViewV1,
}

impl From<application::RuleSummary> for RuleViewV1 {
    fn from(rule: application::RuleSummary) -> Self {
        Self {
            index: rule.index,
            rule_string: rule.rule_string,
            rule_type: rule.rule_type,
            payload: rule.payload,
            policy_target: rule.policy_target,
            params: rule.params,
            policy_target_validation: rule.policy_target_validation.into(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PolicyTargetValidationViewV1 {
    Valid,
    Missing,
    Unavailable,
}

impl From<application::PolicyTargetValidation> for PolicyTargetValidationViewV1 {
    fn from(validation: application::PolicyTargetValidation) -> Self {
        match validation {
            application::PolicyTargetValidation::Valid => Self::Valid,
            application::PolicyTargetValidation::Missing => Self::Missing,
            application::PolicyTargetValidation::Unavailable => Self::Unavailable,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct RuleMutationViewV1 {
    pub action: RuleMutationActionViewV1,
    pub changed_rule: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub previous_rule: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resulting_position: Option<usize>,
    pub runtime_apply: RuntimeApplyViewV1,
}

impl From<application::RuleMutationOutcome> for RuleMutationViewV1 {
    fn from(outcome: application::RuleMutationOutcome) -> Self {
        Self {
            action: outcome.action.into(),
            changed_rule: outcome.changed_rule,
            previous_rule: outcome.previous_rule,
            resulting_position: outcome.resulting_position,
            runtime_apply: outcome.runtime_apply.into(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RuleMutationActionViewV1 {
    Added,
    Replaced,
    Removed,
}

impl From<application::RuleMutationAction> for RuleMutationActionViewV1 {
    fn from(action: application::RuleMutationAction) -> Self {
        match action {
            application::RuleMutationAction::Added => Self::Added,
            application::RuleMutationAction::Replaced => Self::Replaced,
            application::RuleMutationAction::Removed => Self::Removed,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct RuntimeApplyViewV1 {
    pub status: RuntimeApplyStatusViewV1,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub candidate_generation: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub committed_generation: Option<String>,
    pub recovery: RecoveryViewV1,
}

impl From<application::RuntimeApplyOutcome> for RuntimeApplyViewV1 {
    fn from(outcome: application::RuntimeApplyOutcome) -> Self {
        Self {
            status: outcome.status.into(),
            candidate_generation: outcome
                .candidate_generation
                .map(|generation| generation.0.to_string()),
            committed_generation: outcome
                .committed_generation
                .map(|generation| generation.0.to_string()),
            recovery: outcome.recovery.into(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeApplyStatusViewV1 {
    NotRequired,
    Applied,
    Recovered,
    Failed,
}

impl From<application::RuntimeApplyStatus> for RuntimeApplyStatusViewV1 {
    fn from(status: application::RuntimeApplyStatus) -> Self {
        match status {
            application::RuntimeApplyStatus::NotRequired => Self::NotRequired,
            application::RuntimeApplyStatus::Applied => Self::Applied,
            application::RuntimeApplyStatus::Recovered => Self::Recovered,
            application::RuntimeApplyStatus::Failed => Self::Failed,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct RecoveryViewV1 {
    pub status: RecoveryStatusViewV1,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub restored_generation: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

impl From<application::RecoveryOutcome> for RecoveryViewV1 {
    fn from(outcome: application::RecoveryOutcome) -> Self {
        Self {
            status: outcome.status.into(),
            restored_generation: outcome
                .restored_generation
                .map(|generation| generation.0.to_string()),
            message: outcome.message,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RecoveryStatusViewV1 {
    NotRequired,
    Succeeded,
    Failed,
}

impl From<application::RecoveryStatus> for RecoveryStatusViewV1 {
    fn from(status: application::RecoveryStatus) -> Self {
        match status {
            application::RecoveryStatus::NotRequired => Self::NotRequired,
            application::RecoveryStatus::Succeeded => Self::Succeeded,
            application::RecoveryStatus::Failed => Self::Failed,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct LogMetadataViewV1 {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub first_sequence: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_sequence: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_sequence: Option<String>,
    pub dropped_total: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gap: Option<LogGapViewV1>,
}

impl From<application::LogMetadata> for LogMetadataViewV1 {
    fn from(metadata: application::LogMetadata) -> Self {
        Self {
            first_sequence: metadata.first_sequence.map(|sequence| sequence.to_string()),
            last_sequence: metadata.last_sequence.map(|sequence| sequence.to_string()),
            next_sequence: metadata.next_sequence.map(|sequence| sequence.to_string()),
            dropped_total: metadata.dropped_total,
            gap: metadata.gap.map(Into::into),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct LogGapViewV1 {
    pub requested_after_sequence: String,
    pub first_available_sequence: String,
    pub dropped_count: u64,
}

impl From<application::LogGap> for LogGapViewV1 {
    fn from(gap: application::LogGap) -> Self {
        Self {
            requested_after_sequence: gap.requested_after_sequence.to_string(),
            first_available_sequence: gap.first_available_sequence.to_string(),
            dropped_count: gap.dropped_count,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct StatusViewV1 {
    pub supervisor: SupervisorViewV1,
    pub core: CoreViewV1,
    pub tun: TunViewV1,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub active_profile: Option<ActiveProfileViewV1>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub primary_proxy_group: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub selected_node: Option<SelectedNodeViewV1>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latency: Option<LatencySampleViewV1>,
    pub traffic: TrafficSampleViewV1,
    pub connection_count: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub runtime_generation: Option<String>,
    pub apply_state: ApplyStateViewV1,
    pub stream_health: StreamHealthViewV1,
}

impl From<StatusSnapshot> for StatusViewV1 {
    fn from(snapshot: StatusSnapshot) -> Self {
        Self {
            supervisor: snapshot.supervisor.into(),
            core: snapshot.core.into(),
            tun: snapshot.tun.into(),
            active_profile: snapshot.active_profile.map(Into::into),
            primary_proxy_group: snapshot.primary_proxy_group,
            selected_node: snapshot.selected_node.map(Into::into),
            latency: snapshot.latency.map(Into::into),
            traffic: snapshot.traffic.into(),
            connection_count: snapshot.connection_count,
            runtime_generation: snapshot
                .runtime_generation
                .map(|generation| generation.0.to_string()),
            apply_state: snapshot.apply_state.into(),
            stream_health: snapshot.stream_health.into(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct SupervisorViewV1 {
    pub lifecycle: SupervisorLifecycleViewV1,
    pub started_at_unix_ms: String,
    pub uptime_seconds: u64,
}

impl From<SupervisorStatus> for SupervisorViewV1 {
    fn from(status: SupervisorStatus) -> Self {
        Self {
            lifecycle: status.lifecycle.into(),
            started_at_unix_ms: status.started_at_unix_ms.to_string(),
            uptime_seconds: status.uptime_seconds,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SupervisorLifecycleViewV1 {
    Starting,
    Ready,
    Stopping,
    Degraded,
}

impl From<SupervisorLifecycle> for SupervisorLifecycleViewV1 {
    fn from(lifecycle: SupervisorLifecycle) -> Self {
        match lifecycle {
            SupervisorLifecycle::Starting => Self::Starting,
            SupervisorLifecycle::Ready => Self::Ready,
            SupervisorLifecycle::Stopping => Self::Stopping,
            SupervisorLifecycle::Degraded => Self::Degraded,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct CoreViewV1 {
    pub lifecycle: CoreLifecycleViewV1,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pid: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub instance_generation: Option<String>,
}

impl From<CoreStatus> for CoreViewV1 {
    fn from(status: CoreStatus) -> Self {
        Self {
            lifecycle: status.lifecycle.into(),
            pid: status.pid,
            instance_generation: status
                .instance_generation
                .map(|generation| generation.0.to_string()),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CoreLifecycleViewV1 {
    Unconfigured,
    Stopped,
    Starting,
    Ready,
    Reloading,
    Stopping,
    Degraded,
}

impl From<CoreLifecycle> for CoreLifecycleViewV1 {
    fn from(lifecycle: CoreLifecycle) -> Self {
        match lifecycle {
            CoreLifecycle::Unconfigured => Self::Unconfigured,
            CoreLifecycle::Stopped => Self::Stopped,
            CoreLifecycle::Starting => Self::Starting,
            CoreLifecycle::Ready => Self::Ready,
            CoreLifecycle::Reloading => Self::Reloading,
            CoreLifecycle::Stopping => Self::Stopping,
            CoreLifecycle::Degraded => Self::Degraded,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct TunViewV1 {
    pub requested: bool,
    pub capable: bool,
    pub effective: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<TunReasonViewV1>,
}

impl From<TunStatus> for TunViewV1 {
    fn from(status: TunStatus) -> Self {
        Self {
            requested: status.requested,
            capable: status.capable,
            effective: status.effective,
            reason: status.reason.map(Into::into),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TunReasonViewV1 {
    NoActiveProfile,
    PermissionDenied,
    Unsupported,
    CoreUnavailable,
}

impl From<TunReason> for TunReasonViewV1 {
    fn from(reason: TunReason) -> Self {
        match reason {
            TunReason::NoActiveProfile => Self::NoActiveProfile,
            TunReason::PermissionDenied => Self::PermissionDenied,
            TunReason::Unsupported => Self::Unsupported,
            TunReason::CoreUnavailable => Self::CoreUnavailable,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ActiveProfileViewV1 {
    pub id: String,
    pub name: String,
}

impl From<ActiveProfileSummary> for ActiveProfileViewV1 {
    fn from(profile: ActiveProfileSummary) -> Self {
        Self {
            id: profile.id.to_string(),
            name: profile.name,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct SelectedNodeViewV1 {
    pub id: String,
    pub name: String,
}

impl From<SelectedNodeSummary> for SelectedNodeViewV1 {
    fn from(node: SelectedNodeSummary) -> Self {
        Self {
            id: node.id.as_str().to_owned(),
            name: node.name,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct LatencySampleViewV1 {
    pub node_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delay_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sampled_at_unix_ms: Option<String>,
    pub state: SampleStateViewV1,
    pub probe_generation: String,
}

impl From<LatencySample> for LatencySampleViewV1 {
    fn from(sample: LatencySample) -> Self {
        Self {
            node_id: sample.node_id.as_str().to_owned(),
            delay_ms: sample.delay_ms,
            sampled_at_unix_ms: sample.sampled_at_unix_ms.map(|value| value.to_string()),
            state: sample.state.into(),
            probe_generation: sample.probe_generation.0.to_string(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct TrafficSampleViewV1 {
    pub upload_bytes_per_second: u64,
    pub download_bytes_per_second: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sampled_at_unix_ms: Option<String>,
    pub state: SampleStateViewV1,
}

impl From<TrafficSample> for TrafficSampleViewV1 {
    fn from(sample: TrafficSample) -> Self {
        Self {
            upload_bytes_per_second: sample.upload_bytes_per_second,
            download_bytes_per_second: sample.download_bytes_per_second,
            sampled_at_unix_ms: sample.sampled_at_unix_ms.map(|value| value.to_string()),
            state: sample.state.into(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SampleStateViewV1 {
    Fresh,
    Stale,
    Unavailable,
}

impl From<SampleState> for SampleStateViewV1 {
    fn from(state: SampleState) -> Self {
        match state {
            SampleState::Fresh => Self::Fresh,
            SampleState::Stale => Self::Stale,
            SampleState::Unavailable => Self::Unavailable,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ApplyStateViewV1 {
    Idle,
    Applying,
    Recovering,
    Failed,
}

impl From<ApplyState> for ApplyStateViewV1 {
    fn from(state: ApplyState) -> Self {
        match state {
            ApplyState::Idle => Self::Idle,
            ApplyState::Applying => Self::Applying,
            ApplyState::Recovering => Self::Recovering,
            ApplyState::Failed => Self::Failed,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct StreamHealthViewV1 {
    pub traffic: StreamStateViewV1,
    pub connections: StreamStateViewV1,
    pub logs: StreamStateViewV1,
}

impl From<StreamHealthSet> for StreamHealthViewV1 {
    fn from(health: StreamHealthSet) -> Self {
        Self {
            traffic: health.traffic.into(),
            connections: health.connections.into(),
            logs: health.logs.into(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StreamStateViewV1 {
    Disconnected,
    Connecting,
    Healthy,
    Stale,
    Degraded,
}

impl From<StreamState> for StreamStateViewV1 {
    fn from(state: StreamState) -> Self {
        match state {
            StreamState::Disconnected => Self::Disconnected,
            StreamState::Connecting => Self::Connecting,
            StreamState::Healthy => Self::Healthy,
            StreamState::Stale => Self::Stale,
            StreamState::Degraded => Self::Degraded,
        }
    }
}
