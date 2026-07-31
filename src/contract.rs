use serde::Serialize;

pub use crate::error::{ErrorCode, ProcessExitCode};

use crate::application::{ApplicationError, ApplicationErrorDetails};
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
        let details = error.details.map(|details| match details {
            ApplicationErrorDetails::CandidateIds { candidate_ids } => {
                serde_json::json!({ "candidate_ids": candidate_ids })
            }
        });
        Self {
            code: error.code,
            message: error.message,
            retryable: error.retryable,
            details,
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
