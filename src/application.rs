use crate::cancellation::CancellationToken;
use crate::domain::{
    ApplyState, CoreLifecycle, CoreStatus, LocalRuleSetRevision, NodeRecordId, ProbeGeneration,
    ProbeQueueStatus, ProfileId, ProxyGroupId, RuntimeApplySnapshot, RuntimeGeneration,
    SampleState, StatusSnapshot, StreamHealthSet, StreamState, SubscriptionUrl,
    SupervisorLifecycle, SupervisorStatus, TrafficSample, TunReason, TunStatus,
};
use crate::error::ErrorCode;
use std::fmt;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

pub trait Clock: Send + Sync {
    fn now_unix_ms(&self) -> u64;
}

#[derive(Debug, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_unix_ms(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
            .try_into()
            .unwrap_or(u64::MAX)
    }
}

pub struct ApplicationService {
    clock: Arc<dyn Clock>,
    started_at_unix_ms: u64,
}

pub trait ApplicationClient {
    fn execute(
        &self,
        operation: ApplicationOperation,
    ) -> Result<ApplicationOutput, ApplicationError>;

    fn execute_cancellable(
        &self,
        operation: ApplicationOperation,
        cancellation: &CancellationToken,
    ) -> Result<ApplicationOutput, ApplicationError> {
        if cancellation.is_cancelled() {
            return Err(ApplicationError::new(
                ErrorCode::OperationUnavailable,
                "The application operation was cancelled",
                true,
            ));
        }
        self.execute(operation)
    }
}

impl Default for ApplicationService {
    fn default() -> Self {
        Self::new()
    }
}

impl ApplicationService {
    #[must_use]
    pub fn new() -> Self {
        Self::with_clock(Arc::new(SystemClock))
    }

    #[must_use]
    pub fn with_clock(clock: Arc<dyn Clock>) -> Self {
        let started_at_unix_ms = clock.now_unix_ms();
        Self {
            clock,
            started_at_unix_ms,
        }
    }

    #[must_use]
    pub fn status(&self) -> StatusSnapshot {
        let uptime_seconds = self
            .clock
            .now_unix_ms()
            .saturating_sub(self.started_at_unix_ms)
            / 1_000;
        StatusSnapshot {
            supervisor: SupervisorStatus {
                lifecycle: SupervisorLifecycle::Ready,
                started_at_unix_ms: self.started_at_unix_ms,
                uptime_seconds,
                health_reasons: Vec::new(),
            },
            core: CoreStatus {
                lifecycle: CoreLifecycle::Unconfigured,
                pid: None,
                instance_generation: None,
                restart: crate::domain::CoreRestartStatus::default(),
            },
            tun: TunStatus {
                requested: true,
                capable: false,
                effective: false,
                reason: Some(TunReason::NoActiveProfile),
            },
            active_profile: None,
            primary_proxy_group: None,
            selected_node: None,
            latency: None,
            traffic: TrafficSample {
                upload_bytes_per_second: 0,
                download_bytes_per_second: 0,
                sampled_at_unix_ms: None,
                state: SampleState::Unavailable,
            },
            connection_count: 0,
            upload_total_bytes: 0,
            download_total_bytes: 0,
            memory_bytes: None,
            connections: Vec::new(),
            runtime_generation: None,
            apply_state: ApplyState::Idle,
            runtime_apply: RuntimeApplySnapshot::default(),
            selection_restore_pending: false,
            probe_queue: ProbeQueueStatus::default(),
            stream_health: StreamHealthSet {
                traffic: StreamState::Disconnected,
                connections: StreamState::Disconnected,
                logs: StreamState::Disconnected,
            },
        }
    }

    pub fn execute(
        &self,
        operation: ApplicationOperation,
    ) -> Result<ApplicationOutput, ApplicationError> {
        match operation {
            ApplicationOperation::GetStatus => Ok(ApplicationOutput::Status(self.status())),
            _ => Err(ApplicationError::new(
                ErrorCode::OperationUnavailable,
                "The lifecycle service is not connected",
                true,
            )),
        }
    }
}

impl ApplicationClient for ApplicationService {
    fn execute(
        &self,
        operation: ApplicationOperation,
    ) -> Result<ApplicationOutput, ApplicationError> {
        ApplicationService::execute(self, operation)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ApplicationOperation {
    Start,
    Stop,
    Restart,
    GetStatus,
    ProfileAdd {
        subscription_url: SubscriptionUrl,
    },
    ProfileList,
    #[doc(hidden)]
    ProfileListPage {
        offset: usize,
    },
    ProfileUse {
        profile: String,
    },
    ProfileRemove {
        profile: String,
    },
    ProxyList {
        group: String,
    },
    #[doc(hidden)]
    ProxyListPage {
        group: String,
        groups_offset: usize,
        nodes_offset: usize,
    },
    ProxySelect {
        group: String,
        node: String,
    },
    LatencyList,
    LatencyShow {
        node: String,
    },
    RuleList,
    #[doc(hidden)]
    RuleListPage {
        offset: usize,
    },
    RuleAdd {
        rule: String,
        placement: RulePlacement,
    },
    RuleReplace {
        old_rule: String,
        new_rule: String,
    },
    RuleRemove {
        rule: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RulePlacement {
    Prepend,
    Append,
    Before(String),
    After(String),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LifecycleAction {
    Start,
    Stop,
    Restart,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LifecycleOutcome {
    pub action: LifecycleAction,
    pub changed: bool,
    pub status: StatusSnapshot,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProfileRefreshState {
    Fresh,
    Error,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProfileRefreshStage {
    Download,
    Parse,
    Validate,
    Apply,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProfileRefreshFailure {
    pub stage: ProfileRefreshStage,
    pub message: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProfileSummary {
    pub id: ProfileId,
    pub name: String,
    pub subscription_url: SubscriptionUrl,
    pub active: bool,
    pub refresh_state: ProfileRefreshState,
    pub last_success_at_unix_ms: u64,
    pub next_refresh_at_unix_ms: u64,
    pub last_error: Option<ProfileRefreshFailure>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProfileListOutcome {
    pub profiles: Vec<ProfileSummary>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[doc(hidden)]
pub struct ProfileListPageOutcome {
    pub snapshot_id: u64,
    pub total: usize,
    pub offset: usize,
    pub profiles: Vec<ProfileSummary>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProfileMutationAction {
    Added,
    Activated,
    Removed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProfileMutationOutcome {
    pub action: ProfileMutationAction,
    pub profile: ProfileSummary,
    pub runtime_apply: Option<RuntimeApplyOutcome>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProxyAvailability {
    Available,
    Unavailable,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProxyMemberKind {
    Node,
    Group,
    Missing,
    Ambiguous,
    ProviderUnavailable,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProxyNodeSource {
    Core,
    Provider { provider_name: String },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProxyNodeRow {
    pub id: Option<NodeRecordId>,
    pub name: String,
    pub member_kind: ProxyMemberKind,
    pub source: Option<ProxyNodeSource>,
    pub candidate_ids: Vec<NodeRecordId>,
    pub proxy_type: Option<String>,
    pub availability: ProxyAvailability,
    pub selected: bool,
    pub delay_ms: Option<u64>,
    pub sampled_at_unix_ms: Option<u64>,
    pub freshness: LatencyFreshness,
    pub probe_status: LatencyProbeStatus,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProxyGroupSummary {
    pub id: ProxyGroupId,
    pub name: String,
    pub proxy_type: String,
    pub selectable: bool,
    pub selected_node: Option<SelectorIdentity>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProxyListOutcome {
    pub group: ProxyGroupSummary,
    pub groups: Vec<ProxyGroupSummary>,
    pub nodes: Vec<ProxyNodeRow>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[doc(hidden)]
pub struct ProxyListPageOutcome {
    pub snapshot_id: u64,
    pub group: ProxyGroupSummary,
    pub groups_total: usize,
    pub groups_offset: usize,
    pub groups: Vec<ProxyGroupSummary>,
    pub nodes_total: usize,
    pub nodes_offset: usize,
    pub nodes: Vec<ProxyNodeRow>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SelectorIdentity {
    pub id: String,
    pub name: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProxySelectionOutcome {
    pub group_id: ProxyGroupId,
    pub group: String,
    pub previous_node: Option<SelectorIdentity>,
    pub selected_node: SelectorIdentity,
    pub persisted: bool,
    pub recovery: RecoveryOutcome,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LatencyFreshness {
    NotSampled,
    Fresh,
    Stale,
    Unavailable,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LatencyProbeStatus {
    NotSampled,
    Queued,
    InFlight,
    Succeeded,
    Failed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LatencySummary {
    pub node_id: NodeRecordId,
    pub node_name: String,
    pub delay_ms: Option<u64>,
    pub sampled_at_unix_ms: Option<u64>,
    pub freshness: LatencyFreshness,
    pub probe_status: LatencyProbeStatus,
    pub probe_generation: ProbeGeneration,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LatencyListOutcome {
    pub samples: Vec<LatencySummary>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LatencyShowOutcome {
    pub sample: LatencySummary,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PolicyTargetValidation {
    Valid,
    Missing,
    Unavailable,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuleSummary {
    pub index: usize,
    pub rule_string: String,
    pub rule_type: String,
    pub payload: Option<String>,
    pub policy_target: String,
    pub params: Vec<String>,
    pub policy_target_validation: PolicyTargetValidation,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuleListOutcome {
    pub initialized: bool,
    pub revision: Option<LocalRuleSetRevision>,
    pub rules: Vec<RuleSummary>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[doc(hidden)]
pub struct RuleListPageOutcome {
    pub initialized: bool,
    pub revision: Option<LocalRuleSetRevision>,
    pub total: usize,
    pub offset: usize,
    pub rules: Vec<RuleSummary>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuleMutationAction {
    Added,
    Replaced,
    Removed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuleMutationOutcome {
    pub action: RuleMutationAction,
    pub changed_rule: String,
    pub previous_rule: Option<String>,
    pub resulting_position: Option<usize>,
    pub runtime_apply: RuntimeApplyOutcome,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeApplyStatus {
    NotRequired,
    Applied,
    Recovered,
    Failed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecoveryStatus {
    NotRequired,
    Succeeded,
    Pending,
    Failed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecoveryOutcome {
    pub status: RecoveryStatus,
    pub restored_generation: Option<RuntimeGeneration>,
    pub message: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeApplyOutcome {
    pub status: RuntimeApplyStatus,
    pub candidate_generation: Option<RuntimeGeneration>,
    pub committed_generation: Option<RuntimeGeneration>,
    pub recovery: RecoveryOutcome,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeApplyFailureStage {
    State,
    Bundle,
    Lock,
    RecoveryRequired,
    StaleCandidate,
    InvalidCandidate,
    Validation,
    Prepare,
    Apply,
    IndeterminateApply,
    Health,
    Commit,
    Cleanup,
    Recovery,
}

impl RuntimeApplyFailureStage {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::State => "state",
            Self::Bundle => "bundle",
            Self::Lock => "lock",
            Self::RecoveryRequired => "recovery_required",
            Self::StaleCandidate => "stale_candidate",
            Self::InvalidCandidate => "invalid_candidate",
            Self::Validation => "validation",
            Self::Prepare => "prepare",
            Self::Apply => "apply",
            Self::IndeterminateApply => "indeterminate_apply",
            Self::Health => "health",
            Self::Commit => "commit",
            Self::Cleanup => "cleanup",
            Self::Recovery => "recovery",
        }
    }

    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        Some(match value {
            "state" => Self::State,
            "bundle" => Self::Bundle,
            "lock" => Self::Lock,
            "recovery_required" => Self::RecoveryRequired,
            "stale_candidate" => Self::StaleCandidate,
            "invalid_candidate" => Self::InvalidCandidate,
            "validation" => Self::Validation,
            "prepare" => Self::Prepare,
            "apply" => Self::Apply,
            "indeterminate_apply" => Self::IndeterminateApply,
            "health" => Self::Health,
            "commit" => Self::Commit,
            "cleanup" => Self::Cleanup,
            "recovery" => Self::Recovery,
            _ => return None,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogGap {
    pub requested_after_sequence: u64,
    pub first_available_sequence: u64,
    pub dropped_count: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogMetadata {
    pub first_sequence: Option<u64>,
    pub last_sequence: Option<u64>,
    pub next_sequence: Option<u64>,
    pub dropped_total: u64,
    pub gap: Option<LogGap>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ApplicationOutput {
    Status(StatusSnapshot),
    Lifecycle(LifecycleOutcome),
    Profiles(ProfileListOutcome),
    #[doc(hidden)]
    ProfilePage(ProfileListPageOutcome),
    ProfileMutation(ProfileMutationOutcome),
    Proxies(ProxyListOutcome),
    #[doc(hidden)]
    ProxyPage(ProxyListPageOutcome),
    ProxySelection(ProxySelectionOutcome),
    Latencies(LatencyListOutcome),
    Latency(LatencyShowOutcome),
    Rules(RuleListOutcome),
    #[doc(hidden)]
    RulePage(RuleListPageOutcome),
    RuleMutation(RuleMutationOutcome),
    LogMetadata(LogMetadata),
}

#[derive(Clone, Eq, PartialEq)]
pub struct ApplicationError {
    pub code: ErrorCode,
    pub message: String,
    pub retryable: bool,
    pub details: Option<ApplicationErrorDetails>,
    pub selector_candidates: Option<SelectorCandidateDetails>,
}

impl ApplicationError {
    #[must_use]
    pub fn new(code: ErrorCode, message: impl Into<String>, retryable: bool) -> Self {
        Self {
            code,
            message: message.into(),
            retryable,
            details: None,
            selector_candidates: None,
        }
    }

    #[must_use]
    pub fn with_details(mut self, details: ApplicationErrorDetails) -> Self {
        self.details = Some(details);
        self
    }

    #[must_use]
    pub fn with_selector_candidates(
        mut self,
        selector: SelectorKind,
        candidates: Vec<SelectorCandidate>,
    ) -> Self {
        self.selector_candidates = Some(SelectorCandidateDetails {
            selector,
            candidates,
        });
        self
    }
}

impl fmt::Debug for ApplicationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ApplicationError")
            .field("code", &self.code)
            .field("message_bytes", &self.message.len())
            .field("retryable", &self.retryable)
            .field("has_details", &self.details.is_some())
            .field(
                "selector_candidate_count",
                &self
                    .selector_candidates
                    .as_ref()
                    .map_or(0, |details| details.candidates.len()),
            )
            .finish()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ApplicationErrorDetails {
    CandidateIds { candidate_ids: Vec<String> },
    RuntimeApplyFailure(Box<RuntimeApplyFailureDetails>),
    LifecycleFailure(Box<LifecycleFailureDetails>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LifecycleFailureDetails {
    pub stage: String,
    pub category: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeApplyFailureDetails {
    pub candidate_generation: Option<RuntimeGeneration>,
    pub committed_generation: Option<RuntimeGeneration>,
    pub stage: RuntimeApplyFailureStage,
    pub recovery: RecoveryOutcome,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SelectorKind {
    Profile,
    ProxyGroup,
    Node,
    Rule,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SelectorCandidate {
    pub id: String,
    pub name: String,
}

impl SelectorCandidate {
    #[must_use]
    pub fn new(id: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            name: name.into(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SelectorCandidateDetails {
    pub selector: SelectorKind,
    pub candidates: Vec<SelectorCandidate>,
}
