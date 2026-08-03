//! Runtime status, health, and telemetry wire projections.

use serde::{Deserialize, Serialize};

use crate::application::{LogGap, LogMetadata};
use crate::constants::{
    CONNECTION_CHAIN_CAPACITY, CONNECTION_FIELD_MAX_BYTES, CONNECTION_RECORD_CAPACITY,
};
use crate::domain::{
    ActiveProfileSummary, ApplyState, ConnectionRecord, CoreDiagnosticCategory,
    CoreInstanceGeneration, CoreLifecycle, CoreRestartStatus, CoreStatus, LatencySample,
    NodeRecordId, ProbeGeneration, ProbeQueueStatus, ProfileId, RuntimeApplyPhase,
    RuntimeApplySnapshot, RuntimeGeneration, RuntimeRecoverySnapshot, RuntimeRecoveryStatus,
    SampleState, SelectedNodeSummary, StatusSnapshot, StreamHealthSet, StreamState,
    SupervisorHealthReason, SupervisorLifecycle, SupervisorStatus, TrafficSample, TunReason,
    TunStatus,
};

use super::WireConversionError;

#[derive(Debug, Deserialize, Serialize)]
struct WireLogGap {
    requested_after_sequence: u64,
    first_available_sequence: u64,
    dropped_count: u64,
}

impl From<LogGap> for WireLogGap {
    fn from(value: LogGap) -> Self {
        Self {
            requested_after_sequence: value.requested_after_sequence,
            first_available_sequence: value.first_available_sequence,
            dropped_count: value.dropped_count,
        }
    }
}

impl From<WireLogGap> for LogGap {
    fn from(value: WireLogGap) -> Self {
        Self {
            requested_after_sequence: value.requested_after_sequence,
            first_available_sequence: value.first_available_sequence,
            dropped_count: value.dropped_count,
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub(super) struct WireLogMetadata {
    first_sequence: Option<u64>,
    last_sequence: Option<u64>,
    next_sequence: Option<u64>,
    dropped_total: u64,
    gap: Option<WireLogGap>,
}

impl From<LogMetadata> for WireLogMetadata {
    fn from(value: LogMetadata) -> Self {
        Self {
            first_sequence: value.first_sequence,
            last_sequence: value.last_sequence,
            next_sequence: value.next_sequence,
            dropped_total: value.dropped_total,
            gap: value.gap.map(Into::into),
        }
    }
}

impl From<WireLogMetadata> for LogMetadata {
    fn from(value: WireLogMetadata) -> Self {
        Self {
            first_sequence: value.first_sequence,
            last_sequence: value.last_sequence,
            next_sequence: value.next_sequence,
            dropped_total: value.dropped_total,
            gap: value.gap.map(Into::into),
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub(super) struct WireStatusSnapshot {
    supervisor: WireSupervisorStatus,
    core: WireCoreStatus,
    tun: WireTunStatus,
    active_profile: Option<WireActiveProfileSummary>,
    primary_proxy_group: Option<String>,
    selected_node: Option<WireSelectedNodeSummary>,
    latency: Option<WireLatencySample>,
    traffic: WireTrafficSample,
    connection_count: u64,
    #[serde(default)]
    upload_total_bytes: u64,
    #[serde(default)]
    download_total_bytes: u64,
    #[serde(default)]
    memory_bytes: Option<u64>,
    #[serde(default)]
    connections: Vec<WireConnectionRecord>,
    runtime_generation: Option<u64>,
    apply_state: WireApplyState,
    #[serde(default)]
    runtime_apply: Option<WireRuntimeApplySnapshot>,
    #[serde(default)]
    selection_restore_pending: bool,
    #[serde(default)]
    probe_queue: WireProbeQueueStatus,
    stream_health: WireStreamHealthSet,
}

impl From<StatusSnapshot> for WireStatusSnapshot {
    fn from(status: StatusSnapshot) -> Self {
        Self {
            supervisor: status.supervisor.into(),
            core: status.core.into(),
            tun: status.tun.into(),
            active_profile: status.active_profile.map(Into::into),
            primary_proxy_group: status.primary_proxy_group,
            selected_node: status.selected_node.map(Into::into),
            latency: status.latency.map(Into::into),
            traffic: status.traffic.into(),
            connection_count: status.connection_count,
            upload_total_bytes: status.upload_total_bytes,
            download_total_bytes: status.download_total_bytes,
            memory_bytes: status.memory_bytes,
            connections: status.connections.into_iter().map(Into::into).collect(),
            runtime_generation: status.runtime_generation.map(|generation| generation.0),
            apply_state: status.apply_state.into(),
            runtime_apply: Some(status.runtime_apply.into()),
            selection_restore_pending: status.selection_restore_pending,
            probe_queue: status.probe_queue.into(),
            stream_health: status.stream_health.into(),
        }
    }
}

impl TryFrom<WireStatusSnapshot> for StatusSnapshot {
    type Error = WireConversionError;

    fn try_from(status: WireStatusSnapshot) -> Result<Self, Self::Error> {
        if status.connections.len() > CONNECTION_RECORD_CAPACITY {
            return Err(WireConversionError);
        }
        let connections = status
            .connections
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<Vec<_>, _>>()?;
        let runtime_generation = status.runtime_generation.map(RuntimeGeneration);
        let apply_state: ApplyState = status.apply_state.into();
        let runtime_apply = status.runtime_apply.map_or_else(
            || RuntimeApplySnapshot {
                candidate_generation: None,
                committed_generation: runtime_generation,
                phase: match apply_state {
                    ApplyState::Idle => RuntimeApplyPhase::Idle,
                    ApplyState::Applying => RuntimeApplyPhase::Applying,
                    ApplyState::Recovering => RuntimeApplyPhase::Recovering,
                    ApplyState::Failed => RuntimeApplyPhase::Failed,
                },
                recovery: RuntimeRecoverySnapshot::default(),
            },
            Into::into,
        );
        Ok(Self {
            supervisor: status.supervisor.try_into()?,
            core: status.core.into(),
            tun: status.tun.into(),
            active_profile: status.active_profile.map(TryInto::try_into).transpose()?,
            primary_proxy_group: status.primary_proxy_group,
            selected_node: status.selected_node.map(TryInto::try_into).transpose()?,
            latency: status.latency.map(TryInto::try_into).transpose()?,
            traffic: status.traffic.into(),
            connection_count: status.connection_count,
            upload_total_bytes: status.upload_total_bytes,
            download_total_bytes: status.download_total_bytes,
            memory_bytes: status.memory_bytes,
            connections,
            runtime_generation,
            apply_state,
            runtime_apply,
            selection_restore_pending: status.selection_restore_pending,
            probe_queue: status.probe_queue.try_into()?,
            stream_health: status.stream_health.into(),
        })
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct WireConnectionRecord {
    id: String,
    network: String,
    host: Option<String>,
    destination_ip: Option<String>,
    destination_port: Option<String>,
    chains: Vec<String>,
    rule: String,
    rule_payload: Option<String>,
    upload_bytes: u64,
    download_bytes: u64,
}

impl From<ConnectionRecord> for WireConnectionRecord {
    fn from(connection: ConnectionRecord) -> Self {
        Self {
            id: connection.id,
            network: connection.network,
            host: connection.host,
            destination_ip: connection.destination_ip,
            destination_port: connection.destination_port,
            chains: connection.chains,
            rule: connection.rule,
            rule_payload: connection.rule_payload,
            upload_bytes: connection.upload_bytes,
            download_bytes: connection.download_bytes,
        }
    }
}

impl TryFrom<WireConnectionRecord> for ConnectionRecord {
    type Error = WireConversionError;

    fn try_from(connection: WireConnectionRecord) -> Result<Self, Self::Error> {
        let fields = [
            connection.id.as_str(),
            connection.network.as_str(),
            connection.host.as_deref().unwrap_or_default(),
            connection.destination_ip.as_deref().unwrap_or_default(),
            connection.destination_port.as_deref().unwrap_or_default(),
            connection.rule.as_str(),
            connection.rule_payload.as_deref().unwrap_or_default(),
        ];
        if connection.chains.len() > CONNECTION_CHAIN_CAPACITY
            || fields
                .into_iter()
                .chain(connection.chains.iter().map(String::as_str))
                .any(|field| field.len() > CONNECTION_FIELD_MAX_BYTES)
        {
            return Err(WireConversionError);
        }
        Ok(Self {
            id: connection.id,
            network: connection.network,
            host: connection.host,
            destination_ip: connection.destination_ip,
            destination_port: connection.destination_port,
            chains: connection.chains,
            rule: connection.rule,
            rule_payload: connection.rule_payload,
            upload_bytes: connection.upload_bytes,
            download_bytes: connection.download_bytes,
        })
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct WireRuntimeApplySnapshot {
    candidate_generation: Option<u64>,
    committed_generation: Option<u64>,
    phase: WireRuntimeApplyPhase,
    recovery: WireRuntimeRecoverySnapshot,
}

impl From<RuntimeApplySnapshot> for WireRuntimeApplySnapshot {
    fn from(snapshot: RuntimeApplySnapshot) -> Self {
        Self {
            candidate_generation: snapshot.candidate_generation.map(|generation| generation.0),
            committed_generation: snapshot.committed_generation.map(|generation| generation.0),
            phase: snapshot.phase.into(),
            recovery: snapshot.recovery.into(),
        }
    }
}

impl From<WireRuntimeApplySnapshot> for RuntimeApplySnapshot {
    fn from(snapshot: WireRuntimeApplySnapshot) -> Self {
        Self {
            candidate_generation: snapshot.candidate_generation.map(RuntimeGeneration),
            committed_generation: snapshot.committed_generation.map(RuntimeGeneration),
            phase: snapshot.phase.into(),
            recovery: snapshot.recovery.into(),
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct WireRuntimeRecoverySnapshot {
    status: WireRuntimeRecoveryStatus,
    restored_generation: Option<u64>,
    message: Option<String>,
}

impl From<RuntimeRecoverySnapshot> for WireRuntimeRecoverySnapshot {
    fn from(snapshot: RuntimeRecoverySnapshot) -> Self {
        Self {
            status: snapshot.status.into(),
            restored_generation: snapshot.restored_generation.map(|generation| generation.0),
            message: snapshot.message,
        }
    }
}

impl From<WireRuntimeRecoverySnapshot> for RuntimeRecoverySnapshot {
    fn from(snapshot: WireRuntimeRecoverySnapshot) -> Self {
        Self {
            status: snapshot.status.into(),
            restored_generation: snapshot.restored_generation.map(RuntimeGeneration),
            message: snapshot.message,
        }
    }
}

#[derive(Debug, Default, Deserialize, Serialize)]
struct WireProbeQueueStatus {
    active_node_count: u64,
    queue_depth: u64,
    in_flight_count: u64,
    overloaded: bool,
    oldest_due_age_ms: Option<u64>,
    estimated_full_pass_duration_ms: u64,
    stale_node_count: u64,
}

impl From<ProbeQueueStatus> for WireProbeQueueStatus {
    fn from(status: ProbeQueueStatus) -> Self {
        Self {
            active_node_count: status.active_node_count,
            queue_depth: status.queue_depth,
            in_flight_count: status.in_flight_count,
            overloaded: status.overloaded,
            oldest_due_age_ms: status.oldest_due_age_ms,
            estimated_full_pass_duration_ms: status.estimated_full_pass_duration_ms,
            stale_node_count: status.stale_node_count,
        }
    }
}

impl TryFrom<WireProbeQueueStatus> for ProbeQueueStatus {
    type Error = WireConversionError;

    fn try_from(status: WireProbeQueueStatus) -> Result<Self, Self::Error> {
        let scheduled = status
            .queue_depth
            .checked_add(status.in_flight_count)
            .ok_or(WireConversionError)?;
        if status.stale_node_count > status.active_node_count
            || scheduled > status.active_node_count
            || status.oldest_due_age_ms.is_some() != (status.queue_depth > 0)
        {
            return Err(WireConversionError);
        }
        Ok(Self {
            active_node_count: status.active_node_count,
            queue_depth: status.queue_depth,
            in_flight_count: status.in_flight_count,
            overloaded: status.overloaded,
            oldest_due_age_ms: status.oldest_due_age_ms,
            estimated_full_pass_duration_ms: status.estimated_full_pass_duration_ms,
            stale_node_count: status.stale_node_count,
        })
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct WireSupervisorStatus {
    lifecycle: WireSupervisorLifecycle,
    started_at_unix_ms: u64,
    uptime_seconds: u64,
    #[serde(default)]
    health_reasons: Vec<WireSupervisorHealthReason>,
}

impl From<SupervisorStatus> for WireSupervisorStatus {
    fn from(status: SupervisorStatus) -> Self {
        let mut health_reasons = status.health_reasons;
        health_reasons.sort_unstable();
        health_reasons.dedup();
        Self {
            lifecycle: status.lifecycle.into(),
            started_at_unix_ms: status.started_at_unix_ms,
            uptime_seconds: status.uptime_seconds,
            health_reasons: health_reasons.into_iter().map(Into::into).collect(),
        }
    }
}

impl TryFrom<WireSupervisorStatus> for SupervisorStatus {
    type Error = WireConversionError;

    fn try_from(status: WireSupervisorStatus) -> Result<Self, Self::Error> {
        let health_reasons = status
            .health_reasons
            .into_iter()
            .map(Into::into)
            .collect::<Vec<SupervisorHealthReason>>();
        if health_reasons.len() > 5
            || !health_reasons
                .windows(2)
                .all(|window| window[0] < window[1])
        {
            return Err(WireConversionError);
        }
        Ok(Self {
            lifecycle: status.lifecycle.into(),
            started_at_unix_ms: status.started_at_unix_ms,
            uptime_seconds: status.uptime_seconds,
            health_reasons,
        })
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum WireSupervisorHealthReason {
    RuntimeRecovery,
    SelectionCompensation,
    ConfigurationProjection,
    ProbeScheduler,
    SelectionRestoration,
}

impl From<SupervisorHealthReason> for WireSupervisorHealthReason {
    fn from(value: SupervisorHealthReason) -> Self {
        match value {
            SupervisorHealthReason::RuntimeRecovery => Self::RuntimeRecovery,
            SupervisorHealthReason::SelectionCompensation => Self::SelectionCompensation,
            SupervisorHealthReason::ConfigurationProjection => Self::ConfigurationProjection,
            SupervisorHealthReason::ProbeScheduler => Self::ProbeScheduler,
            SupervisorHealthReason::SelectionRestoration => Self::SelectionRestoration,
        }
    }
}

impl From<WireSupervisorHealthReason> for SupervisorHealthReason {
    fn from(value: WireSupervisorHealthReason) -> Self {
        match value {
            WireSupervisorHealthReason::RuntimeRecovery => Self::RuntimeRecovery,
            WireSupervisorHealthReason::SelectionCompensation => Self::SelectionCompensation,
            WireSupervisorHealthReason::ConfigurationProjection => Self::ConfigurationProjection,
            WireSupervisorHealthReason::ProbeScheduler => Self::ProbeScheduler,
            WireSupervisorHealthReason::SelectionRestoration => Self::SelectionRestoration,
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum WireSupervisorLifecycle {
    Starting,
    Ready,
    Stopping,
    Stopped,
    Degraded,
}

impl From<SupervisorLifecycle> for WireSupervisorLifecycle {
    fn from(value: SupervisorLifecycle) -> Self {
        match value {
            SupervisorLifecycle::Starting => Self::Starting,
            SupervisorLifecycle::Ready => Self::Ready,
            SupervisorLifecycle::Stopping => Self::Stopping,
            SupervisorLifecycle::Stopped => Self::Stopped,
            SupervisorLifecycle::Degraded => Self::Degraded,
        }
    }
}

impl From<WireSupervisorLifecycle> for SupervisorLifecycle {
    fn from(value: WireSupervisorLifecycle) -> Self {
        match value {
            WireSupervisorLifecycle::Starting => Self::Starting,
            WireSupervisorLifecycle::Ready => Self::Ready,
            WireSupervisorLifecycle::Stopping => Self::Stopping,
            WireSupervisorLifecycle::Stopped => Self::Stopped,
            WireSupervisorLifecycle::Degraded => Self::Degraded,
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct WireCoreStatus {
    lifecycle: WireCoreLifecycle,
    pid: Option<u32>,
    instance_generation: Option<u64>,
    #[serde(default)]
    restart: WireCoreRestartStatus,
}

impl From<CoreStatus> for WireCoreStatus {
    fn from(status: CoreStatus) -> Self {
        Self {
            lifecycle: status.lifecycle.into(),
            pid: status.pid,
            instance_generation: status.instance_generation.map(|generation| generation.0),
            restart: status.restart.into(),
        }
    }
}

impl From<WireCoreStatus> for CoreStatus {
    fn from(status: WireCoreStatus) -> Self {
        Self {
            lifecycle: status.lifecycle.into(),
            pid: status.pid,
            instance_generation: status.instance_generation.map(CoreInstanceGeneration),
            restart: status.restart.into(),
        }
    }
}

#[derive(Debug, Default, Deserialize, Serialize)]
struct WireCoreRestartStatus {
    pending: bool,
    attempts: u64,
    backoff_ms: Option<u64>,
    diagnostic: Option<WireCoreDiagnosticCategory>,
}

impl From<CoreRestartStatus> for WireCoreRestartStatus {
    fn from(status: CoreRestartStatus) -> Self {
        Self {
            pending: status.pending,
            attempts: status.attempts,
            backoff_ms: status.backoff_ms,
            diagnostic: status.diagnostic.map(Into::into),
        }
    }
}

impl From<WireCoreRestartStatus> for CoreRestartStatus {
    fn from(status: WireCoreRestartStatus) -> Self {
        Self {
            pending: status.pending,
            attempts: status.attempts,
            backoff_ms: status.backoff_ms,
            diagnostic: status.diagnostic.map(Into::into),
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum WireCoreDiagnosticCategory {
    RestartLimitReached,
}

impl From<CoreDiagnosticCategory> for WireCoreDiagnosticCategory {
    fn from(category: CoreDiagnosticCategory) -> Self {
        match category {
            CoreDiagnosticCategory::RestartLimitReached => Self::RestartLimitReached,
        }
    }
}

impl From<WireCoreDiagnosticCategory> for CoreDiagnosticCategory {
    fn from(category: WireCoreDiagnosticCategory) -> Self {
        match category {
            WireCoreDiagnosticCategory::RestartLimitReached => Self::RestartLimitReached,
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum WireCoreLifecycle {
    Unconfigured,
    Stopped,
    Starting,
    Ready,
    Reloading,
    Stopping,
    Degraded,
}

impl From<CoreLifecycle> for WireCoreLifecycle {
    fn from(value: CoreLifecycle) -> Self {
        match value {
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

impl From<WireCoreLifecycle> for CoreLifecycle {
    fn from(value: WireCoreLifecycle) -> Self {
        match value {
            WireCoreLifecycle::Unconfigured => Self::Unconfigured,
            WireCoreLifecycle::Stopped => Self::Stopped,
            WireCoreLifecycle::Starting => Self::Starting,
            WireCoreLifecycle::Ready => Self::Ready,
            WireCoreLifecycle::Reloading => Self::Reloading,
            WireCoreLifecycle::Stopping => Self::Stopping,
            WireCoreLifecycle::Degraded => Self::Degraded,
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct WireTunStatus {
    requested: bool,
    capable: bool,
    effective: bool,
    reason: Option<WireTunReason>,
}

impl From<TunStatus> for WireTunStatus {
    fn from(status: TunStatus) -> Self {
        Self {
            requested: status.requested,
            capable: status.capable,
            effective: status.effective,
            reason: status.reason.map(Into::into),
        }
    }
}

impl From<WireTunStatus> for TunStatus {
    fn from(status: WireTunStatus) -> Self {
        Self {
            requested: status.requested,
            capable: status.capable,
            effective: status.effective,
            reason: status.reason.map(Into::into),
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum WireTunReason {
    NoActiveProfile,
    PermissionDenied,
    Unsupported,
    CoreUnavailable,
}

impl From<TunReason> for WireTunReason {
    fn from(value: TunReason) -> Self {
        match value {
            TunReason::NoActiveProfile => Self::NoActiveProfile,
            TunReason::PermissionDenied => Self::PermissionDenied,
            TunReason::Unsupported => Self::Unsupported,
            TunReason::CoreUnavailable => Self::CoreUnavailable,
        }
    }
}

impl From<WireTunReason> for TunReason {
    fn from(value: WireTunReason) -> Self {
        match value {
            WireTunReason::NoActiveProfile => Self::NoActiveProfile,
            WireTunReason::PermissionDenied => Self::PermissionDenied,
            WireTunReason::Unsupported => Self::Unsupported,
            WireTunReason::CoreUnavailable => Self::CoreUnavailable,
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct WireActiveProfileSummary {
    id: String,
    name: String,
}

impl From<ActiveProfileSummary> for WireActiveProfileSummary {
    fn from(value: ActiveProfileSummary) -> Self {
        Self {
            id: value.id.to_string(),
            name: value.name,
        }
    }
}

impl TryFrom<WireActiveProfileSummary> for ActiveProfileSummary {
    type Error = WireConversionError;

    fn try_from(value: WireActiveProfileSummary) -> Result<Self, Self::Error> {
        Ok(Self {
            id: ProfileId::parse(&value.id).map_err(|_| WireConversionError)?,
            name: value.name,
        })
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct WireSelectedNodeSummary {
    id: String,
    name: String,
}

impl From<SelectedNodeSummary> for WireSelectedNodeSummary {
    fn from(value: SelectedNodeSummary) -> Self {
        Self {
            id: value.id.as_str().to_owned(),
            name: value.name,
        }
    }
}

impl TryFrom<WireSelectedNodeSummary> for SelectedNodeSummary {
    type Error = WireConversionError;

    fn try_from(value: WireSelectedNodeSummary) -> Result<Self, Self::Error> {
        Ok(Self {
            id: NodeRecordId::parse(&value.id).map_err(|_| WireConversionError)?,
            name: value.name,
        })
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct WireLatencySample {
    node_id: String,
    delay_ms: Option<u64>,
    sampled_at_unix_ms: Option<u64>,
    state: WireSampleState,
    probe_generation: u64,
}

impl From<LatencySample> for WireLatencySample {
    fn from(value: LatencySample) -> Self {
        Self {
            node_id: value.node_id.as_str().to_owned(),
            delay_ms: value.delay_ms,
            sampled_at_unix_ms: value.sampled_at_unix_ms,
            state: value.state.into(),
            probe_generation: value.probe_generation.0,
        }
    }
}

impl TryFrom<WireLatencySample> for LatencySample {
    type Error = WireConversionError;

    fn try_from(value: WireLatencySample) -> Result<Self, Self::Error> {
        Ok(Self {
            node_id: NodeRecordId::parse(&value.node_id).map_err(|_| WireConversionError)?,
            delay_ms: value.delay_ms,
            sampled_at_unix_ms: value.sampled_at_unix_ms,
            state: value.state.into(),
            probe_generation: ProbeGeneration(value.probe_generation),
        })
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct WireTrafficSample {
    upload_bytes_per_second: u64,
    download_bytes_per_second: u64,
    sampled_at_unix_ms: Option<u64>,
    state: WireSampleState,
}

impl From<TrafficSample> for WireTrafficSample {
    fn from(value: TrafficSample) -> Self {
        Self {
            upload_bytes_per_second: value.upload_bytes_per_second,
            download_bytes_per_second: value.download_bytes_per_second,
            sampled_at_unix_ms: value.sampled_at_unix_ms,
            state: value.state.into(),
        }
    }
}

impl From<WireTrafficSample> for TrafficSample {
    fn from(value: WireTrafficSample) -> Self {
        Self {
            upload_bytes_per_second: value.upload_bytes_per_second,
            download_bytes_per_second: value.download_bytes_per_second,
            sampled_at_unix_ms: value.sampled_at_unix_ms,
            state: value.state.into(),
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum WireSampleState {
    Fresh,
    Stale,
    Unavailable,
}

impl From<SampleState> for WireSampleState {
    fn from(value: SampleState) -> Self {
        match value {
            SampleState::Fresh => Self::Fresh,
            SampleState::Stale => Self::Stale,
            SampleState::Unavailable => Self::Unavailable,
        }
    }
}

impl From<WireSampleState> for SampleState {
    fn from(value: WireSampleState) -> Self {
        match value {
            WireSampleState::Fresh => Self::Fresh,
            WireSampleState::Stale => Self::Stale,
            WireSampleState::Unavailable => Self::Unavailable,
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum WireApplyState {
    Idle,
    Applying,
    Recovering,
    Failed,
}

impl From<ApplyState> for WireApplyState {
    fn from(value: ApplyState) -> Self {
        match value {
            ApplyState::Idle => Self::Idle,
            ApplyState::Applying => Self::Applying,
            ApplyState::Recovering => Self::Recovering,
            ApplyState::Failed => Self::Failed,
        }
    }
}

impl From<WireApplyState> for ApplyState {
    fn from(value: WireApplyState) -> Self {
        match value {
            WireApplyState::Idle => Self::Idle,
            WireApplyState::Applying => Self::Applying,
            WireApplyState::Recovering => Self::Recovering,
            WireApplyState::Failed => Self::Failed,
        }
    }
}

wire_enum!(
    WireRuntimeApplyPhase,
    RuntimeApplyPhase,
    [Idle, Applying, Succeeded, Recovering, Failed]
);
wire_enum!(
    WireRuntimeRecoveryStatus,
    RuntimeRecoveryStatus,
    [NotRequired, Succeeded, Pending, Failed]
);

#[derive(Debug, Deserialize, Serialize)]
struct WireStreamHealthSet {
    traffic: WireStreamState,
    connections: WireStreamState,
    logs: WireStreamState,
}

impl From<StreamHealthSet> for WireStreamHealthSet {
    fn from(value: StreamHealthSet) -> Self {
        Self {
            traffic: value.traffic.into(),
            connections: value.connections.into(),
            logs: value.logs.into(),
        }
    }
}

impl From<WireStreamHealthSet> for StreamHealthSet {
    fn from(value: WireStreamHealthSet) -> Self {
        Self {
            traffic: value.traffic.into(),
            connections: value.connections.into(),
            logs: value.logs.into(),
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum WireStreamState {
    Disconnected,
    Connecting,
    Healthy,
    Stale,
    Degraded,
}

impl From<StreamState> for WireStreamState {
    fn from(value: StreamState) -> Self {
        match value {
            StreamState::Disconnected => Self::Disconnected,
            StreamState::Connecting => Self::Connecting,
            StreamState::Healthy => Self::Healthy,
            StreamState::Stale => Self::Stale,
            StreamState::Degraded => Self::Degraded,
        }
    }
}

impl From<WireStreamState> for StreamState {
    fn from(value: WireStreamState) -> Self {
        match value {
            WireStreamState::Disconnected => Self::Disconnected,
            WireStreamState::Connecting => Self::Connecting,
            WireStreamState::Healthy => Self::Healthy,
            WireStreamState::Stale => Self::Stale,
            WireStreamState::Degraded => Self::Degraded,
        }
    }
}
