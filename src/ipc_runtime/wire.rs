//! Private wire projections between IPC JSON and application values.

use serde::{Deserialize, Serialize};

use crate::application::{
    ApplicationOperation, ApplicationOutput, LatencyFreshness, LatencyListOutcome,
    LatencyProbeStatus, LatencyShowOutcome, LatencySummary, LifecycleAction, LifecycleOutcome,
    PolicyTargetValidation, ProfileListOutcome, ProfileListPageOutcome, ProfileMutationAction,
    ProfileMutationOutcome, ProfileRefreshFailure, ProfileRefreshStage, ProfileRefreshState,
    ProfileSummary, ProxyAvailability, ProxyGroupSummary, ProxyListOutcome, ProxyListPageOutcome,
    ProxyMemberKind, ProxyNodeRow, ProxyNodeSource, ProxySelectionOutcome, RecoveryOutcome,
    RecoveryStatus, RuleListOutcome, RuleListPageOutcome, RuleMutationAction, RuleMutationOutcome,
    RuleSummary, RuntimeApplyOutcome, RuntimeApplyStatus, SelectorIdentity,
};
use crate::domain::{
    LocalRuleSetRevision, NodeRecordId, ProbeGeneration, ProfileId, ProxyGroupId,
    RuntimeGeneration, StatusSnapshot, SubscriptionUrl,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ExpectedOutput {
    Status,
    Lifecycle,
    Profiles,
    ProfilePage,
    ProfileMutation,
    Proxies,
    ProxyPage,
    ProxySelection,
    Latencies,
    Latency,
    Rules,
    RulePage,
    RuleMutation,
}

impl ExpectedOutput {
    pub(super) fn for_operation(operation: &ApplicationOperation) -> Self {
        match operation {
            ApplicationOperation::Start
            | ApplicationOperation::Stop
            | ApplicationOperation::Restart => Self::Lifecycle,
            ApplicationOperation::GetStatus => Self::Status,
            ApplicationOperation::ProfileAdd { .. }
            | ApplicationOperation::ProfileUse { .. }
            | ApplicationOperation::ProfileRemove { .. } => Self::ProfileMutation,
            ApplicationOperation::ProfileList => Self::Profiles,
            ApplicationOperation::ProfileListPage { .. } => Self::ProfilePage,
            ApplicationOperation::ProxyList { .. } => Self::Proxies,
            ApplicationOperation::ProxyListPage { .. } => Self::ProxyPage,
            ApplicationOperation::ProxySelect { .. } => Self::ProxySelection,
            ApplicationOperation::LatencyList => Self::Latencies,
            ApplicationOperation::LatencyShow { .. } => Self::Latency,
            ApplicationOperation::RuleList => Self::Rules,
            ApplicationOperation::RuleListPage { .. } => Self::RulePage,
            ApplicationOperation::RuleAdd { .. }
            | ApplicationOperation::RuleReplace { .. }
            | ApplicationOperation::RuleRemove { .. } => Self::RuleMutation,
        }
    }

    pub(super) fn matches(self, output: &ApplicationOutput) -> bool {
        matches!(
            (self, output),
            (Self::Status, ApplicationOutput::Status(_))
                | (Self::Lifecycle, ApplicationOutput::Lifecycle(_))
                | (Self::Profiles, ApplicationOutput::Profiles(_))
                | (Self::ProfilePage, ApplicationOutput::ProfilePage(_))
                | (Self::ProfilePage, ApplicationOutput::Profiles(_))
                | (Self::ProfileMutation, ApplicationOutput::ProfileMutation(_))
                | (Self::Proxies, ApplicationOutput::Proxies(_))
                | (Self::ProxyPage, ApplicationOutput::ProxyPage(_))
                | (Self::ProxyPage, ApplicationOutput::Proxies(_))
                | (Self::ProxySelection, ApplicationOutput::ProxySelection(_))
                | (Self::Latencies, ApplicationOutput::Latencies(_))
                | (Self::Latency, ApplicationOutput::Latency(_))
                | (Self::Rules, ApplicationOutput::Rules(_))
                | (Self::RulePage, ApplicationOutput::RulePage(_))
                | (Self::RulePage, ApplicationOutput::Rules(_))
                | (Self::RuleMutation, ApplicationOutput::RuleMutation(_))
        )
    }
}

#[derive(Debug)]
pub(super) struct WireConversionError;

macro_rules! wire_enum {
    ($wire:ident, $domain:ident, [$($variant:ident),+ $(,)?]) => {
        #[derive(Debug, Deserialize, Serialize)]
        #[serde(rename_all = "snake_case")]
        enum $wire {
            $($variant),+
        }

        impl From<$domain> for $wire {
            fn from(value: $domain) -> Self {
                match value {
                    $($domain::$variant => Self::$variant),+
                }
            }
        }

        impl From<$wire> for $domain {
            fn from(value: $wire) -> Self {
                match value {
                    $($wire::$variant => Self::$variant),+
                }
            }
        }
    };
}

mod status;

use status::{WireLogMetadata, WireStatusSnapshot};

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "output", content = "data", rename_all = "snake_case")]
enum WireApplicationOutput {
    Status(WireStatusSnapshot),
    Lifecycle(WireLifecycleOutcome),
    Profiles(WireProfileListOutcome),
    ProfilePage(WireProfileListPageOutcome),
    ProfileMutation(WireProfileMutationOutcome),
    Proxies(WireProxyListOutcome),
    ProxyPage(WireProxyListPageOutcome),
    ProxySelection(WireProxySelectionOutcome),
    Latencies(WireLatencyListOutcome),
    Latency(WireLatencyShowOutcome),
    Rules(WireRuleListOutcome),
    RulePage(WireRuleListPageOutcome),
    RuleMutation(WireRuleMutationOutcome),
    LogMetadata(WireLogMetadata),
}

impl TryFrom<ApplicationOutput> for WireApplicationOutput {
    type Error = WireConversionError;

    fn try_from(output: ApplicationOutput) -> Result<Self, Self::Error> {
        match output {
            ApplicationOutput::Status(status) => Ok(Self::Status(status.into())),
            ApplicationOutput::Lifecycle(outcome) => Ok(Self::Lifecycle(outcome.into())),
            ApplicationOutput::Profiles(outcome) => Ok(Self::Profiles(outcome.into())),
            ApplicationOutput::ProfilePage(outcome) => Ok(Self::ProfilePage(outcome.into())),
            ApplicationOutput::ProfileMutation(outcome) => {
                Ok(Self::ProfileMutation(outcome.into()))
            }
            ApplicationOutput::Proxies(outcome) => Ok(Self::Proxies(outcome.into())),
            ApplicationOutput::ProxyPage(outcome) => Ok(Self::ProxyPage(outcome.into())),
            ApplicationOutput::ProxySelection(outcome) => Ok(Self::ProxySelection(outcome.into())),
            ApplicationOutput::Latencies(outcome) => Ok(Self::Latencies(outcome.into())),
            ApplicationOutput::Latency(outcome) => Ok(Self::Latency(outcome.into())),
            ApplicationOutput::Rules(outcome) => Ok(Self::Rules(outcome.into())),
            ApplicationOutput::RulePage(outcome) => Ok(Self::RulePage(outcome.into())),
            ApplicationOutput::RuleMutation(outcome) => Ok(Self::RuleMutation(outcome.into())),
            ApplicationOutput::LogMetadata(metadata) => Ok(Self::LogMetadata(metadata.into())),
        }
    }
}

impl TryFrom<WireApplicationOutput> for ApplicationOutput {
    type Error = WireConversionError;

    fn try_from(output: WireApplicationOutput) -> Result<Self, Self::Error> {
        match output {
            WireApplicationOutput::Status(status) => {
                Ok(Self::Status(StatusSnapshot::try_from(status)?))
            }
            WireApplicationOutput::Lifecycle(outcome) => {
                Ok(Self::Lifecycle(LifecycleOutcome::try_from(outcome)?))
            }
            WireApplicationOutput::Profiles(outcome) => {
                Ok(Self::Profiles(ProfileListOutcome::try_from(outcome)?))
            }
            WireApplicationOutput::ProfilePage(outcome) => Ok(Self::ProfilePage(
                ProfileListPageOutcome::try_from(outcome)?,
            )),
            WireApplicationOutput::ProfileMutation(outcome) => Ok(Self::ProfileMutation(
                ProfileMutationOutcome::try_from(outcome)?,
            )),
            WireApplicationOutput::Proxies(outcome) => {
                Ok(Self::Proxies(ProxyListOutcome::try_from(outcome)?))
            }
            WireApplicationOutput::ProxyPage(outcome) => {
                Ok(Self::ProxyPage(ProxyListPageOutcome::try_from(outcome)?))
            }
            WireApplicationOutput::ProxySelection(outcome) => {
                Ok(Self::ProxySelection(outcome.try_into()?))
            }
            WireApplicationOutput::Latencies(outcome) => {
                Ok(Self::Latencies(LatencyListOutcome::try_from(outcome)?))
            }
            WireApplicationOutput::Latency(outcome) => {
                Ok(Self::Latency(LatencyShowOutcome::try_from(outcome)?))
            }
            WireApplicationOutput::Rules(outcome) => Ok(Self::Rules(outcome.into())),
            WireApplicationOutput::RulePage(outcome) => Ok(Self::RulePage(outcome.into())),
            WireApplicationOutput::RuleMutation(outcome) => Ok(Self::RuleMutation(outcome.into())),
            WireApplicationOutput::LogMetadata(metadata) => Ok(Self::LogMetadata(metadata.into())),
        }
    }
}

pub(super) fn encode_application_output(
    output: ApplicationOutput,
) -> Result<serde_json::Value, WireConversionError> {
    let output = WireApplicationOutput::try_from(output)?;
    serde_json::to_value(output).map_err(|_| WireConversionError)
}

pub(super) fn decode_application_output(
    value: serde_json::Value,
) -> Result<ApplicationOutput, WireConversionError> {
    serde_json::from_value::<WireApplicationOutput>(value)
        .map_err(|_| WireConversionError)?
        .try_into()
}

pub(super) fn encode_status_snapshot(
    status: StatusSnapshot,
) -> Result<serde_json::Value, WireConversionError> {
    serde_json::to_value(WireStatusSnapshot::from(status)).map_err(|_| WireConversionError)
}

pub(super) fn decode_status_snapshot(
    value: serde_json::Value,
) -> Result<StatusSnapshot, WireConversionError> {
    serde_json::from_value::<WireStatusSnapshot>(value)
        .map_err(|_| WireConversionError)?
        .try_into()
}

wire_enum!(WireLifecycleAction, LifecycleAction, [Start, Stop, Restart]);

#[derive(Debug, Deserialize, Serialize)]
struct WireLifecycleOutcome {
    action: WireLifecycleAction,
    changed: bool,
    status: WireStatusSnapshot,
}

impl From<LifecycleOutcome> for WireLifecycleOutcome {
    fn from(value: LifecycleOutcome) -> Self {
        Self {
            action: value.action.into(),
            changed: value.changed,
            status: value.status.into(),
        }
    }
}

impl TryFrom<WireLifecycleOutcome> for LifecycleOutcome {
    type Error = WireConversionError;

    fn try_from(value: WireLifecycleOutcome) -> Result<Self, Self::Error> {
        Ok(Self {
            action: value.action.into(),
            changed: value.changed,
            status: value.status.try_into()?,
        })
    }
}

wire_enum!(WireProfileRefreshState, ProfileRefreshState, [Fresh, Error]);
wire_enum!(
    WireProfileRefreshStage,
    ProfileRefreshStage,
    [Download, Parse, Validate, Apply]
);

#[derive(Debug, Deserialize, Serialize)]
struct WireProfileRefreshFailure {
    stage: WireProfileRefreshStage,
    message: String,
}

impl From<ProfileRefreshFailure> for WireProfileRefreshFailure {
    fn from(value: ProfileRefreshFailure) -> Self {
        Self {
            stage: value.stage.into(),
            message: value.message,
        }
    }
}

impl From<WireProfileRefreshFailure> for ProfileRefreshFailure {
    fn from(value: WireProfileRefreshFailure) -> Self {
        Self {
            stage: value.stage.into(),
            message: value.message,
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct WireProfileSummary {
    id: String,
    name: String,
    subscription_url: String,
    active: bool,
    refresh_state: WireProfileRefreshState,
    last_success_at_unix_ms: u64,
    next_refresh_at_unix_ms: u64,
    last_error: Option<WireProfileRefreshFailure>,
}

impl From<ProfileSummary> for WireProfileSummary {
    fn from(value: ProfileSummary) -> Self {
        Self {
            id: value.id.to_string(),
            name: value.name,
            subscription_url: value.subscription_url.redacted(),
            active: value.active,
            refresh_state: value.refresh_state.into(),
            last_success_at_unix_ms: value.last_success_at_unix_ms,
            next_refresh_at_unix_ms: value.next_refresh_at_unix_ms,
            last_error: value.last_error.map(Into::into),
        }
    }
}

impl TryFrom<WireProfileSummary> for ProfileSummary {
    type Error = WireConversionError;

    fn try_from(value: WireProfileSummary) -> Result<Self, Self::Error> {
        Ok(Self {
            id: ProfileId::parse(&value.id).map_err(|_| WireConversionError)?,
            name: value.name,
            subscription_url: SubscriptionUrl::parse(&value.subscription_url)
                .map_err(|_| WireConversionError)?,
            active: value.active,
            refresh_state: value.refresh_state.into(),
            last_success_at_unix_ms: value.last_success_at_unix_ms,
            next_refresh_at_unix_ms: value.next_refresh_at_unix_ms,
            last_error: value.last_error.map(Into::into),
        })
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct WireProfileListOutcome {
    profiles: Vec<WireProfileSummary>,
}

impl From<ProfileListOutcome> for WireProfileListOutcome {
    fn from(value: ProfileListOutcome) -> Self {
        Self {
            profiles: value.profiles.into_iter().map(Into::into).collect(),
        }
    }
}

impl TryFrom<WireProfileListOutcome> for ProfileListOutcome {
    type Error = WireConversionError;

    fn try_from(value: WireProfileListOutcome) -> Result<Self, Self::Error> {
        Ok(Self {
            profiles: value
                .profiles
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_, _>>()?,
        })
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct WireProfileListPageOutcome {
    snapshot_id: u64,
    total: usize,
    offset: usize,
    profiles: Vec<WireProfileSummary>,
}

impl From<ProfileListPageOutcome> for WireProfileListPageOutcome {
    fn from(value: ProfileListPageOutcome) -> Self {
        Self {
            snapshot_id: value.snapshot_id,
            total: value.total,
            offset: value.offset,
            profiles: value.profiles.into_iter().map(Into::into).collect(),
        }
    }
}

impl TryFrom<WireProfileListPageOutcome> for ProfileListPageOutcome {
    type Error = WireConversionError;

    fn try_from(value: WireProfileListPageOutcome) -> Result<Self, Self::Error> {
        Ok(Self {
            snapshot_id: value.snapshot_id,
            total: value.total,
            offset: value.offset,
            profiles: value
                .profiles
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_, _>>()?,
        })
    }
}

wire_enum!(
    WireProfileMutationAction,
    ProfileMutationAction,
    [Added, Activated, Removed]
);

#[derive(Debug, Deserialize, Serialize)]
struct WireProfileMutationOutcome {
    action: WireProfileMutationAction,
    profile: WireProfileSummary,
    runtime_apply: Option<WireRuntimeApplyOutcome>,
}

impl From<ProfileMutationOutcome> for WireProfileMutationOutcome {
    fn from(value: ProfileMutationOutcome) -> Self {
        Self {
            action: value.action.into(),
            profile: value.profile.into(),
            runtime_apply: value.runtime_apply.map(Into::into),
        }
    }
}

impl TryFrom<WireProfileMutationOutcome> for ProfileMutationOutcome {
    type Error = WireConversionError;

    fn try_from(value: WireProfileMutationOutcome) -> Result<Self, Self::Error> {
        Ok(Self {
            action: value.action.into(),
            profile: value.profile.try_into()?,
            runtime_apply: value.runtime_apply.map(Into::into),
        })
    }
}

wire_enum!(
    WireProxyAvailability,
    ProxyAvailability,
    [Available, Unavailable]
);
wire_enum!(
    WireProxyMemberKind,
    ProxyMemberKind,
    [Node, Group, Missing, Ambiguous, ProviderUnavailable]
);

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum WireProxyNodeSource {
    Core,
    Provider { provider_name: String },
}

impl From<ProxyNodeSource> for WireProxyNodeSource {
    fn from(value: ProxyNodeSource) -> Self {
        match value {
            ProxyNodeSource::Core => Self::Core,
            ProxyNodeSource::Provider { provider_name } => Self::Provider { provider_name },
        }
    }
}

impl From<WireProxyNodeSource> for ProxyNodeSource {
    fn from(value: WireProxyNodeSource) -> Self {
        match value {
            WireProxyNodeSource::Core => Self::Core,
            WireProxyNodeSource::Provider { provider_name } => Self::Provider { provider_name },
        }
    }
}

wire_enum!(
    WireLatencyFreshness,
    LatencyFreshness,
    [NotSampled, Fresh, Stale, Unavailable]
);
wire_enum!(
    WireLatencyProbeStatus,
    LatencyProbeStatus,
    [NotSampled, Queued, InFlight, Succeeded, Failed]
);

#[derive(Debug, Deserialize, Serialize)]
struct WireProxyNodeRow {
    id: Option<String>,
    name: String,
    member_kind: WireProxyMemberKind,
    source: Option<WireProxyNodeSource>,
    candidate_ids: Vec<String>,
    proxy_type: Option<String>,
    availability: WireProxyAvailability,
    selected: bool,
    delay_ms: Option<u64>,
    sampled_at_unix_ms: Option<u64>,
    freshness: WireLatencyFreshness,
    probe_status: WireLatencyProbeStatus,
}

impl From<ProxyNodeRow> for WireProxyNodeRow {
    fn from(value: ProxyNodeRow) -> Self {
        Self {
            id: value.id.map(|id| id.as_str().to_owned()),
            name: value.name,
            member_kind: value.member_kind.into(),
            source: value.source.map(Into::into),
            candidate_ids: value
                .candidate_ids
                .into_iter()
                .map(|id| id.as_str().to_owned())
                .collect(),
            proxy_type: value.proxy_type,
            availability: value.availability.into(),
            selected: value.selected,
            delay_ms: value.delay_ms,
            sampled_at_unix_ms: value.sampled_at_unix_ms,
            freshness: value.freshness.into(),
            probe_status: value.probe_status.into(),
        }
    }
}

impl TryFrom<WireProxyNodeRow> for ProxyNodeRow {
    type Error = WireConversionError;

    fn try_from(value: WireProxyNodeRow) -> Result<Self, Self::Error> {
        Ok(Self {
            id: value
                .id
                .map(|id| NodeRecordId::parse(&id).map_err(|_| WireConversionError))
                .transpose()?,
            name: value.name,
            member_kind: value.member_kind.into(),
            source: value.source.map(Into::into),
            candidate_ids: value
                .candidate_ids
                .into_iter()
                .map(|id| NodeRecordId::parse(&id).map_err(|_| WireConversionError))
                .collect::<Result<_, _>>()?,
            proxy_type: value.proxy_type,
            availability: value.availability.into(),
            selected: value.selected,
            delay_ms: value.delay_ms,
            sampled_at_unix_ms: value.sampled_at_unix_ms,
            freshness: value.freshness.into(),
            probe_status: value.probe_status.into(),
        })
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct WireSelectorIdentity {
    id: String,
    name: String,
}

impl From<SelectorIdentity> for WireSelectorIdentity {
    fn from(value: SelectorIdentity) -> Self {
        Self {
            id: value.id,
            name: value.name,
        }
    }
}

impl From<WireSelectorIdentity> for SelectorIdentity {
    fn from(value: WireSelectorIdentity) -> Self {
        Self {
            id: value.id,
            name: value.name,
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct WireProxyGroupSummary {
    id: String,
    name: String,
    proxy_type: String,
    selectable: bool,
    selected_node: Option<WireSelectorIdentity>,
}

impl From<ProxyGroupSummary> for WireProxyGroupSummary {
    fn from(value: ProxyGroupSummary) -> Self {
        Self {
            id: value.id.as_str().to_owned(),
            name: value.name,
            proxy_type: value.proxy_type,
            selectable: value.selectable,
            selected_node: value.selected_node.map(Into::into),
        }
    }
}

impl TryFrom<WireProxyGroupSummary> for ProxyGroupSummary {
    type Error = WireConversionError;

    fn try_from(value: WireProxyGroupSummary) -> Result<Self, Self::Error> {
        Ok(Self {
            id: ProxyGroupId::parse(&value.id).map_err(|_| WireConversionError)?,
            name: value.name,
            proxy_type: value.proxy_type,
            selectable: value.selectable,
            selected_node: value.selected_node.map(Into::into),
        })
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct WireProxyListOutcome {
    group: WireProxyGroupSummary,
    #[serde(default)]
    groups: Vec<WireProxyGroupSummary>,
    nodes: Vec<WireProxyNodeRow>,
}

impl From<ProxyListOutcome> for WireProxyListOutcome {
    fn from(value: ProxyListOutcome) -> Self {
        Self {
            group: value.group.into(),
            groups: value.groups.into_iter().map(Into::into).collect(),
            nodes: value.nodes.into_iter().map(Into::into).collect(),
        }
    }
}

impl TryFrom<WireProxyListOutcome> for ProxyListOutcome {
    type Error = WireConversionError;

    fn try_from(value: WireProxyListOutcome) -> Result<Self, Self::Error> {
        Ok(Self {
            group: value.group.try_into()?,
            groups: value
                .groups
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_, _>>()?,
            nodes: value
                .nodes
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_, _>>()?,
        })
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct WireProxyListPageOutcome {
    snapshot_id: u64,
    group: WireProxyGroupSummary,
    groups_total: usize,
    groups_offset: usize,
    groups: Vec<WireProxyGroupSummary>,
    nodes_total: usize,
    nodes_offset: usize,
    nodes: Vec<WireProxyNodeRow>,
}

impl From<ProxyListPageOutcome> for WireProxyListPageOutcome {
    fn from(value: ProxyListPageOutcome) -> Self {
        Self {
            snapshot_id: value.snapshot_id,
            group: value.group.into(),
            groups_total: value.groups_total,
            groups_offset: value.groups_offset,
            groups: value.groups.into_iter().map(Into::into).collect(),
            nodes_total: value.nodes_total,
            nodes_offset: value.nodes_offset,
            nodes: value.nodes.into_iter().map(Into::into).collect(),
        }
    }
}

impl TryFrom<WireProxyListPageOutcome> for ProxyListPageOutcome {
    type Error = WireConversionError;

    fn try_from(value: WireProxyListPageOutcome) -> Result<Self, Self::Error> {
        Ok(Self {
            snapshot_id: value.snapshot_id,
            group: value.group.try_into()?,
            groups_total: value.groups_total,
            groups_offset: value.groups_offset,
            groups: value
                .groups
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_, _>>()?,
            nodes_total: value.nodes_total,
            nodes_offset: value.nodes_offset,
            nodes: value
                .nodes
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_, _>>()?,
        })
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct WireProxySelectionOutcome {
    group_id: String,
    group: String,
    previous_node: Option<WireSelectorIdentity>,
    selected_node: WireSelectorIdentity,
    persisted: bool,
    recovery: WireRecoveryOutcome,
}

impl From<ProxySelectionOutcome> for WireProxySelectionOutcome {
    fn from(value: ProxySelectionOutcome) -> Self {
        Self {
            group_id: value.group_id.as_str().to_owned(),
            group: value.group,
            previous_node: value.previous_node.map(Into::into),
            selected_node: value.selected_node.into(),
            persisted: value.persisted,
            recovery: value.recovery.into(),
        }
    }
}

impl TryFrom<WireProxySelectionOutcome> for ProxySelectionOutcome {
    type Error = WireConversionError;

    fn try_from(value: WireProxySelectionOutcome) -> Result<Self, Self::Error> {
        Ok(Self {
            group_id: ProxyGroupId::parse(&value.group_id).map_err(|_| WireConversionError)?,
            group: value.group,
            previous_node: value.previous_node.map(Into::into),
            selected_node: value.selected_node.into(),
            persisted: value.persisted,
            recovery: value.recovery.into(),
        })
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct WireLatencySummary {
    node_id: String,
    node_name: String,
    delay_ms: Option<u64>,
    sampled_at_unix_ms: Option<u64>,
    freshness: WireLatencyFreshness,
    probe_status: WireLatencyProbeStatus,
    probe_generation: u64,
}

impl From<LatencySummary> for WireLatencySummary {
    fn from(value: LatencySummary) -> Self {
        Self {
            node_id: value.node_id.as_str().to_owned(),
            node_name: value.node_name,
            delay_ms: value.delay_ms,
            sampled_at_unix_ms: value.sampled_at_unix_ms,
            freshness: value.freshness.into(),
            probe_status: value.probe_status.into(),
            probe_generation: value.probe_generation.0,
        }
    }
}

impl TryFrom<WireLatencySummary> for LatencySummary {
    type Error = WireConversionError;

    fn try_from(value: WireLatencySummary) -> Result<Self, Self::Error> {
        Ok(Self {
            node_id: NodeRecordId::parse(&value.node_id).map_err(|_| WireConversionError)?,
            node_name: value.node_name,
            delay_ms: value.delay_ms,
            sampled_at_unix_ms: value.sampled_at_unix_ms,
            freshness: value.freshness.into(),
            probe_status: value.probe_status.into(),
            probe_generation: ProbeGeneration(value.probe_generation),
        })
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct WireLatencyListOutcome {
    samples: Vec<WireLatencySummary>,
}

impl From<LatencyListOutcome> for WireLatencyListOutcome {
    fn from(value: LatencyListOutcome) -> Self {
        Self {
            samples: value.samples.into_iter().map(Into::into).collect(),
        }
    }
}

impl TryFrom<WireLatencyListOutcome> for LatencyListOutcome {
    type Error = WireConversionError;

    fn try_from(value: WireLatencyListOutcome) -> Result<Self, Self::Error> {
        Ok(Self {
            samples: value
                .samples
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_, _>>()?,
        })
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct WireLatencyShowOutcome {
    sample: WireLatencySummary,
}

impl From<LatencyShowOutcome> for WireLatencyShowOutcome {
    fn from(value: LatencyShowOutcome) -> Self {
        Self {
            sample: value.sample.into(),
        }
    }
}

impl TryFrom<WireLatencyShowOutcome> for LatencyShowOutcome {
    type Error = WireConversionError;

    fn try_from(value: WireLatencyShowOutcome) -> Result<Self, Self::Error> {
        Ok(Self {
            sample: value.sample.try_into()?,
        })
    }
}

wire_enum!(
    WirePolicyTargetValidation,
    PolicyTargetValidation,
    [Valid, Missing, Unavailable]
);

#[derive(Debug, Deserialize, Serialize)]
struct WireRuleSummary {
    index: usize,
    rule_string: String,
    rule_type: String,
    payload: Option<String>,
    policy_target: String,
    params: Vec<String>,
    policy_target_validation: WirePolicyTargetValidation,
}

impl From<RuleSummary> for WireRuleSummary {
    fn from(value: RuleSummary) -> Self {
        Self {
            index: value.index,
            rule_string: value.rule_string,
            rule_type: value.rule_type,
            payload: value.payload,
            policy_target: value.policy_target,
            params: value.params,
            policy_target_validation: value.policy_target_validation.into(),
        }
    }
}

impl From<WireRuleSummary> for RuleSummary {
    fn from(value: WireRuleSummary) -> Self {
        Self {
            index: value.index,
            rule_string: value.rule_string,
            rule_type: value.rule_type,
            payload: value.payload,
            policy_target: value.policy_target,
            params: value.params,
            policy_target_validation: value.policy_target_validation.into(),
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct WireRuleListOutcome {
    initialized: bool,
    revision: Option<u64>,
    rules: Vec<WireRuleSummary>,
}

impl From<RuleListOutcome> for WireRuleListOutcome {
    fn from(value: RuleListOutcome) -> Self {
        Self {
            initialized: value.initialized,
            revision: value.revision.map(|revision| revision.0),
            rules: value.rules.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<WireRuleListOutcome> for RuleListOutcome {
    fn from(value: WireRuleListOutcome) -> Self {
        Self {
            initialized: value.initialized,
            revision: value.revision.map(LocalRuleSetRevision),
            rules: value.rules.into_iter().map(Into::into).collect(),
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct WireRuleListPageOutcome {
    initialized: bool,
    revision: Option<u64>,
    total: usize,
    offset: usize,
    rules: Vec<WireRuleSummary>,
}

impl From<RuleListPageOutcome> for WireRuleListPageOutcome {
    fn from(value: RuleListPageOutcome) -> Self {
        Self {
            initialized: value.initialized,
            revision: value.revision.map(|revision| revision.0),
            total: value.total,
            offset: value.offset,
            rules: value.rules.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<WireRuleListPageOutcome> for RuleListPageOutcome {
    fn from(value: WireRuleListPageOutcome) -> Self {
        Self {
            initialized: value.initialized,
            revision: value.revision.map(LocalRuleSetRevision),
            total: value.total,
            offset: value.offset,
            rules: value.rules.into_iter().map(Into::into).collect(),
        }
    }
}

wire_enum!(
    WireRuleMutationAction,
    RuleMutationAction,
    [Added, Replaced, Removed]
);

#[derive(Debug, Deserialize, Serialize)]
struct WireRuleMutationOutcome {
    action: WireRuleMutationAction,
    changed_rule: String,
    previous_rule: Option<String>,
    resulting_position: Option<usize>,
    runtime_apply: WireRuntimeApplyOutcome,
}

impl From<RuleMutationOutcome> for WireRuleMutationOutcome {
    fn from(value: RuleMutationOutcome) -> Self {
        Self {
            action: value.action.into(),
            changed_rule: value.changed_rule,
            previous_rule: value.previous_rule,
            resulting_position: value.resulting_position,
            runtime_apply: value.runtime_apply.into(),
        }
    }
}

impl From<WireRuleMutationOutcome> for RuleMutationOutcome {
    fn from(value: WireRuleMutationOutcome) -> Self {
        Self {
            action: value.action.into(),
            changed_rule: value.changed_rule,
            previous_rule: value.previous_rule,
            resulting_position: value.resulting_position,
            runtime_apply: value.runtime_apply.into(),
        }
    }
}

wire_enum!(
    WireRuntimeApplyStatus,
    RuntimeApplyStatus,
    [NotRequired, Applied, Recovered, Failed]
);
wire_enum!(
    WireRecoveryStatus,
    RecoveryStatus,
    [NotRequired, Succeeded, Pending, Failed]
);

#[derive(Debug, Deserialize, Serialize)]
struct WireRecoveryOutcome {
    status: WireRecoveryStatus,
    restored_generation: Option<u64>,
    message: Option<String>,
}

impl From<RecoveryOutcome> for WireRecoveryOutcome {
    fn from(value: RecoveryOutcome) -> Self {
        Self {
            status: value.status.into(),
            restored_generation: value.restored_generation.map(|generation| generation.0),
            message: value.message,
        }
    }
}

impl From<WireRecoveryOutcome> for RecoveryOutcome {
    fn from(value: WireRecoveryOutcome) -> Self {
        Self {
            status: value.status.into(),
            restored_generation: value.restored_generation.map(RuntimeGeneration),
            message: value.message,
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct WireRuntimeApplyOutcome {
    status: WireRuntimeApplyStatus,
    candidate_generation: Option<u64>,
    committed_generation: Option<u64>,
    recovery: WireRecoveryOutcome,
}

impl From<RuntimeApplyOutcome> for WireRuntimeApplyOutcome {
    fn from(value: RuntimeApplyOutcome) -> Self {
        Self {
            status: value.status.into(),
            candidate_generation: value.candidate_generation.map(|generation| generation.0),
            committed_generation: value.committed_generation.map(|generation| generation.0),
            recovery: value.recovery.into(),
        }
    }
}

impl From<WireRuntimeApplyOutcome> for RuntimeApplyOutcome {
    fn from(value: WireRuntimeApplyOutcome) -> Self {
        Self {
            status: value.status.into(),
            candidate_generation: value.candidate_generation.map(RuntimeGeneration),
            committed_generation: value.committed_generation.map(RuntimeGeneration),
            recovery: value.recovery.into(),
        }
    }
}
