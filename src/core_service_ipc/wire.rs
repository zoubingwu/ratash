//! Versioned privileged CoreRuntime wire contract.

use std::path::PathBuf;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::constants::IPC_FRAME_MAX_BYTES;
use crate::core::{
    ApplyCandidateResult, ApplyDisposition, CoreControlEndpoint, CoreRuntimeDiagnosticCategory,
    CoreRuntimeError, CoreRuntimeErrorKind, CoreRuntimeLifecycle, CoreRuntimeRestartStatus,
    CoreRuntimeStatus, CoreRuntimeTunReason, CoreRuntimeTunStatus, ForwardedCoreLog,
    ForwardedCoreLogBatch, ManagedCoreHandle, OwnerSession, OwnerSessionProof, OwnerSessionRequest,
    ProcessOutputSource, RuntimeBundle, StopCoreResult,
};
use crate::domain::{CoreInstanceGeneration, RuntimeGeneration};

use super::CORE_SERVICE_IPC_PROTOCOL_VERSION;

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct WireRequest {
    pub(super) protocol_version: u16,
    pub(super) request_id: u64,
    pub(super) operation: WireOperation,
}

#[derive(Deserialize, Serialize)]
#[serde(
    tag = "operation",
    content = "payload",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub(super) enum WireOperation {
    OpenOwnerSession(WireOwnerSessionRequest),
    ApplyCandidate(WireApplyRequest),
    Status(WireProofRequest),
    Logs(WireLogsRequest),
    Stop(WireProofRequest),
    CloseOwnerSession(WireProofRequest),
    CancelPendingApply(WireProofRequest),
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct WireResponse {
    pub(super) protocol_version: u16,
    pub(super) request_id: u64,
    pub(super) outcome: WireOutcome,
}

impl WireResponse {
    pub(super) fn success(request_id: u64, success: WireSuccess) -> Self {
        let response = Self {
            protocol_version: CORE_SERVICE_IPC_PROTOCOL_VERSION,
            request_id,
            outcome: WireOutcome::Success(success),
        };
        if serde_json::to_vec(&response).is_ok_and(|encoded| encoded.len() <= IPC_FRAME_MAX_BYTES) {
            response
        } else {
            Self::failure(request_id, CoreRuntimeErrorKind::Unavailable)
        }
    }

    pub(super) fn failure(request_id: u64, kind: CoreRuntimeErrorKind) -> Self {
        Self {
            protocol_version: CORE_SERVICE_IPC_PROTOCOL_VERSION,
            request_id,
            outcome: WireOutcome::Failure(WireCoreRuntimeError { kind: kind.into() }),
        }
    }
}

#[derive(Deserialize, Serialize)]
#[serde(
    tag = "outcome",
    content = "payload",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub(super) enum WireOutcome {
    Success(WireSuccess),
    Failure(WireCoreRuntimeError),
}

#[derive(Deserialize, Serialize)]
#[serde(
    tag = "operation",
    content = "payload",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub(super) enum WireSuccess {
    OwnerSession(WireOwnerSession),
    ApplyCandidate(WireApplyCandidateResult),
    Status(WireCoreRuntimeStatus),
    Logs(WireForwardedCoreLogBatch),
    Stop(WireStopCoreResult),
    CloseOwnerSession(WireEmpty),
    CancelPendingApply(WireEmpty),
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct WireEmpty {}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct WireOwnerSessionRequest {
    owner_uid: u32,
    supervisor_pid: u32,
    supervisor_start_identity: String,
    instance_token: String,
    protocol_version: u16,
}

impl From<&OwnerSessionRequest> for WireOwnerSessionRequest {
    fn from(request: &OwnerSessionRequest) -> Self {
        Self {
            owner_uid: request.owner_uid,
            supervisor_pid: request.supervisor_pid,
            supervisor_start_identity: request.supervisor_start_identity.clone(),
            instance_token: request.instance_token.clone(),
            protocol_version: request.protocol_version,
        }
    }
}

impl WireOwnerSessionRequest {
    pub(super) fn into_core(self) -> OwnerSessionRequest {
        OwnerSessionRequest {
            owner_uid: self.owner_uid,
            supervisor_pid: self.supervisor_pid,
            supervisor_start_identity: self.supervisor_start_identity,
            instance_token: self.instance_token,
            protocol_version: self.protocol_version,
        }
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct WireOwnerSessionProof {
    session_id: String,
    session_token: String,
}

impl From<&OwnerSessionProof> for WireOwnerSessionProof {
    fn from(proof: &OwnerSessionProof) -> Self {
        Self {
            session_id: proof.session_id().to_owned(),
            session_token: proof.session_token().to_owned(),
        }
    }
}

impl WireOwnerSessionProof {
    pub(super) fn into_core(self) -> OwnerSessionProof {
        OwnerSessionProof::new(self.session_id, self.session_token)
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct WireOwnerSession {
    proof: WireOwnerSessionProof,
    protocol_version: u16,
    owner_generation: u64,
    endpoint: WireCoreControlEndpoint,
}

impl From<&OwnerSession> for WireOwnerSession {
    fn from(session: &OwnerSession) -> Self {
        Self {
            proof: (&session.proof).into(),
            protocol_version: session.protocol_version,
            owner_generation: session.owner_generation,
            endpoint: (&session.endpoint).into(),
        }
    }
}

impl WireOwnerSession {
    pub(super) fn into_core(self) -> OwnerSession {
        OwnerSession {
            proof: self.proof.into_core(),
            protocol_version: self.protocol_version,
            owner_generation: self.owner_generation,
            endpoint: self.endpoint.into_core(),
        }
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct WireProofRequest {
    pub(super) owner: WireOwnerSessionProof,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct WireApplyRequest {
    pub(super) owner: WireOwnerSessionProof,
    pub(super) bundle: WireRuntimeBundle,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct WireLogsRequest {
    pub(super) owner: WireOwnerSessionProof,
    pub(super) after_sequence: Option<u64>,
    pub(super) limit: u64,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct WireRuntimeBundle {
    generation: u64,
    generation_root: PathBuf,
    manifest_sha256: String,
    compiler_policy_sha256: String,
    mihomo_binary_sha256: String,
}

impl From<&RuntimeBundle> for WireRuntimeBundle {
    fn from(bundle: &RuntimeBundle) -> Self {
        Self {
            generation: bundle.generation.0,
            generation_root: bundle.generation_root.clone(),
            manifest_sha256: bundle.manifest_sha256.clone(),
            compiler_policy_sha256: bundle.compiler_policy_sha256.clone(),
            mihomo_binary_sha256: bundle.mihomo_binary_sha256.clone(),
        }
    }
}

impl WireRuntimeBundle {
    pub(super) fn into_core(self) -> RuntimeBundle {
        RuntimeBundle {
            generation: RuntimeGeneration(self.generation),
            generation_root: self.generation_root,
            manifest_sha256: self.manifest_sha256,
            compiler_policy_sha256: self.compiler_policy_sha256,
            mihomo_binary_sha256: self.mihomo_binary_sha256,
        }
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct WireCoreControlEndpoint {
    socket_path: PathBuf,
    secret: String,
}

impl From<&CoreControlEndpoint> for WireCoreControlEndpoint {
    fn from(endpoint: &CoreControlEndpoint) -> Self {
        Self {
            socket_path: endpoint.socket_path.clone(),
            secret: endpoint.secret().to_owned(),
        }
    }
}

impl WireCoreControlEndpoint {
    fn into_core(self) -> CoreControlEndpoint {
        CoreControlEndpoint::new(self.socket_path, self.secret)
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct WireManagedCoreHandle {
    pid: u32,
    process_start_identity: String,
    endpoint: WireCoreControlEndpoint,
    instance_generation: u64,
    runtime_generation: u64,
}

impl From<&ManagedCoreHandle> for WireManagedCoreHandle {
    fn from(handle: &ManagedCoreHandle) -> Self {
        Self {
            pid: handle.pid,
            process_start_identity: handle.process_start_identity.clone(),
            endpoint: (&handle.endpoint).into(),
            instance_generation: handle.instance_generation.0,
            runtime_generation: handle.runtime_generation.0,
        }
    }
}

impl WireManagedCoreHandle {
    fn into_core(self) -> ManagedCoreHandle {
        ManagedCoreHandle {
            pid: self.pid,
            process_start_identity: self.process_start_identity,
            endpoint: self.endpoint.into_core(),
            instance_generation: CoreInstanceGeneration(self.instance_generation),
            runtime_generation: RuntimeGeneration(self.runtime_generation),
        }
    }
}

#[derive(Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum WireApplyDisposition {
    Spawned,
    Reloaded,
}

impl From<ApplyDisposition> for WireApplyDisposition {
    fn from(disposition: ApplyDisposition) -> Self {
        match disposition {
            ApplyDisposition::Spawned => Self::Spawned,
            ApplyDisposition::Reloaded => Self::Reloaded,
        }
    }
}

impl WireApplyDisposition {
    fn into_core(self) -> ApplyDisposition {
        match self {
            Self::Spawned => ApplyDisposition::Spawned,
            Self::Reloaded => ApplyDisposition::Reloaded,
        }
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct WireApplyCandidateResult {
    disposition: WireApplyDisposition,
    managed_core: WireManagedCoreHandle,
}

impl From<&ApplyCandidateResult> for WireApplyCandidateResult {
    fn from(result: &ApplyCandidateResult) -> Self {
        Self {
            disposition: result.disposition.into(),
            managed_core: (&result.managed_core).into(),
        }
    }
}

impl WireApplyCandidateResult {
    pub(super) fn into_core(self) -> ApplyCandidateResult {
        ApplyCandidateResult {
            disposition: self.disposition.into_core(),
            managed_core: self.managed_core.into_core(),
        }
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct WireCoreRuntimeStatus {
    managed_core: Option<WireManagedCoreHandle>,
    lifecycle: WireCoreRuntimeLifecycle,
    restart: WireCoreRuntimeRestartStatus,
    tun: WireCoreRuntimeTunStatus,
}

impl From<&CoreRuntimeStatus> for WireCoreRuntimeStatus {
    fn from(status: &CoreRuntimeStatus) -> Self {
        Self {
            managed_core: status.managed_core.as_ref().map(Into::into),
            lifecycle: status.lifecycle.into(),
            restart: (&status.restart).into(),
            tun: status.tun.into(),
        }
    }
}

impl WireCoreRuntimeStatus {
    pub(super) fn into_core(self) -> CoreRuntimeStatus {
        CoreRuntimeStatus {
            managed_core: self.managed_core.map(WireManagedCoreHandle::into_core),
            lifecycle: self.lifecycle.into_core(),
            restart: self.restart.into_core(),
            tun: self.tun.into_core(),
        }
    }
}

#[derive(Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum WireCoreRuntimeLifecycle {
    Owned,
    Running,
    RestartPending,
    Degraded,
}

impl From<CoreRuntimeLifecycle> for WireCoreRuntimeLifecycle {
    fn from(lifecycle: CoreRuntimeLifecycle) -> Self {
        match lifecycle {
            CoreRuntimeLifecycle::Owned => Self::Owned,
            CoreRuntimeLifecycle::Running => Self::Running,
            CoreRuntimeLifecycle::RestartPending => Self::RestartPending,
            CoreRuntimeLifecycle::Degraded => Self::Degraded,
        }
    }
}

impl WireCoreRuntimeLifecycle {
    fn into_core(self) -> CoreRuntimeLifecycle {
        match self {
            Self::Owned => CoreRuntimeLifecycle::Owned,
            Self::Running => CoreRuntimeLifecycle::Running,
            Self::RestartPending => CoreRuntimeLifecycle::RestartPending,
            Self::Degraded => CoreRuntimeLifecycle::Degraded,
        }
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct WireCoreRuntimeRestartStatus {
    pending: bool,
    attempts: u64,
    backoff_ms: Option<u64>,
    diagnostic: Option<WireCoreRuntimeDiagnosticCategory>,
}

impl From<&CoreRuntimeRestartStatus> for WireCoreRuntimeRestartStatus {
    fn from(status: &CoreRuntimeRestartStatus) -> Self {
        Self {
            pending: status.pending,
            attempts: u64::try_from(status.attempts).unwrap_or(u64::MAX),
            backoff_ms: status
                .backoff
                .map(|backoff| u64::try_from(backoff.as_millis()).unwrap_or(u64::MAX)),
            diagnostic: status.diagnostic.map(Into::into),
        }
    }
}

impl WireCoreRuntimeRestartStatus {
    fn into_core(self) -> CoreRuntimeRestartStatus {
        CoreRuntimeRestartStatus {
            pending: self.pending,
            attempts: usize::try_from(self.attempts).unwrap_or(usize::MAX),
            backoff: self.backoff_ms.map(Duration::from_millis),
            diagnostic: self
                .diagnostic
                .map(WireCoreRuntimeDiagnosticCategory::into_core),
        }
    }
}

#[derive(Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum WireCoreRuntimeDiagnosticCategory {
    CoreRestartLimitReached,
}

impl From<CoreRuntimeDiagnosticCategory> for WireCoreRuntimeDiagnosticCategory {
    fn from(category: CoreRuntimeDiagnosticCategory) -> Self {
        match category {
            CoreRuntimeDiagnosticCategory::CoreRestartLimitReached => Self::CoreRestartLimitReached,
        }
    }
}

impl WireCoreRuntimeDiagnosticCategory {
    fn into_core(self) -> CoreRuntimeDiagnosticCategory {
        match self {
            Self::CoreRestartLimitReached => CoreRuntimeDiagnosticCategory::CoreRestartLimitReached,
        }
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct WireCoreRuntimeTunStatus {
    capable: bool,
    reason: Option<WireCoreRuntimeTunReason>,
}

impl From<CoreRuntimeTunStatus> for WireCoreRuntimeTunStatus {
    fn from(status: CoreRuntimeTunStatus) -> Self {
        Self {
            capable: status.capable,
            reason: status.reason.map(Into::into),
        }
    }
}

impl WireCoreRuntimeTunStatus {
    fn into_core(self) -> CoreRuntimeTunStatus {
        CoreRuntimeTunStatus {
            capable: self.capable,
            reason: self.reason.map(WireCoreRuntimeTunReason::into_core),
        }
    }
}

#[derive(Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum WireCoreRuntimeTunReason {
    PermissionDenied,
    Unsupported,
}

impl From<CoreRuntimeTunReason> for WireCoreRuntimeTunReason {
    fn from(reason: CoreRuntimeTunReason) -> Self {
        match reason {
            CoreRuntimeTunReason::PermissionDenied => Self::PermissionDenied,
            CoreRuntimeTunReason::Unsupported => Self::Unsupported,
        }
    }
}

impl WireCoreRuntimeTunReason {
    fn into_core(self) -> CoreRuntimeTunReason {
        match self {
            Self::PermissionDenied => CoreRuntimeTunReason::PermissionDenied,
            Self::Unsupported => CoreRuntimeTunReason::Unsupported,
        }
    }
}

#[derive(Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum WireProcessOutputSource {
    Stdout,
    Stderr,
}

impl From<ProcessOutputSource> for WireProcessOutputSource {
    fn from(source: ProcessOutputSource) -> Self {
        match source {
            ProcessOutputSource::Stdout => Self::Stdout,
            ProcessOutputSource::Stderr => Self::Stderr,
        }
    }
}

impl WireProcessOutputSource {
    fn into_core(self) -> ProcessOutputSource {
        match self {
            Self::Stdout => ProcessOutputSource::Stdout,
            Self::Stderr => ProcessOutputSource::Stderr,
        }
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct WireForwardedCoreLog {
    sequence: u64,
    timestamp_unix_ms: u64,
    source: WireProcessOutputSource,
    message: String,
    instance_generation: u64,
}

impl From<&ForwardedCoreLog> for WireForwardedCoreLog {
    fn from(log: &ForwardedCoreLog) -> Self {
        Self {
            sequence: log.sequence,
            timestamp_unix_ms: log.timestamp_unix_ms,
            source: log.source.into(),
            message: log.message.clone(),
            instance_generation: log.instance_generation.0,
        }
    }
}

impl WireForwardedCoreLog {
    fn into_core(self) -> ForwardedCoreLog {
        ForwardedCoreLog {
            sequence: self.sequence,
            timestamp_unix_ms: self.timestamp_unix_ms,
            source: self.source.into_core(),
            message: self.message,
            instance_generation: CoreInstanceGeneration(self.instance_generation),
        }
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct WireForwardedCoreLogBatch {
    records: Vec<WireForwardedCoreLog>,
    next_sequence: Option<u64>,
    dropped_before: u64,
    dropped_since_after: u64,
}

impl From<&ForwardedCoreLogBatch> for WireForwardedCoreLogBatch {
    fn from(batch: &ForwardedCoreLogBatch) -> Self {
        Self {
            records: batch.records.iter().map(Into::into).collect(),
            next_sequence: batch.next_sequence,
            dropped_before: batch.dropped_before,
            dropped_since_after: batch.dropped_since_after,
        }
    }
}

impl WireForwardedCoreLogBatch {
    pub(super) fn into_core(self) -> ForwardedCoreLogBatch {
        ForwardedCoreLogBatch {
            records: self
                .records
                .into_iter()
                .map(WireForwardedCoreLog::into_core)
                .collect(),
            next_sequence: self.next_sequence,
            dropped_before: self.dropped_before,
            dropped_since_after: self.dropped_since_after,
        }
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct WireStopCoreResult {
    stopped: bool,
    instance_generation: Option<u64>,
}

impl From<&StopCoreResult> for WireStopCoreResult {
    fn from(result: &StopCoreResult) -> Self {
        Self {
            stopped: result.stopped,
            instance_generation: result.instance_generation.map(|generation| generation.0),
        }
    }
}

impl WireStopCoreResult {
    pub(super) fn into_core(self) -> StopCoreResult {
        StopCoreResult {
            stopped: self.stopped,
            instance_generation: self.instance_generation.map(CoreInstanceGeneration),
        }
    }
}

#[derive(Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum WireCoreRuntimeErrorKind {
    Authentication,
    ProtocolMismatch,
    TunPermissionDenied,
    TunUnsupported,
    InvalidBundle,
    ProcessIdentityMismatch,
    Apply,
    ReloadTimeout,
    Readiness,
    Unavailable,
}

impl From<CoreRuntimeErrorKind> for WireCoreRuntimeErrorKind {
    fn from(kind: CoreRuntimeErrorKind) -> Self {
        match kind {
            CoreRuntimeErrorKind::Authentication => Self::Authentication,
            CoreRuntimeErrorKind::ProtocolMismatch => Self::ProtocolMismatch,
            CoreRuntimeErrorKind::TunPermissionDenied => Self::TunPermissionDenied,
            CoreRuntimeErrorKind::TunUnsupported => Self::TunUnsupported,
            CoreRuntimeErrorKind::InvalidBundle => Self::InvalidBundle,
            CoreRuntimeErrorKind::ProcessIdentityMismatch => Self::ProcessIdentityMismatch,
            CoreRuntimeErrorKind::Apply => Self::Apply,
            CoreRuntimeErrorKind::ReloadTimeout => Self::ReloadTimeout,
            CoreRuntimeErrorKind::Readiness => Self::Readiness,
            CoreRuntimeErrorKind::Unavailable => Self::Unavailable,
        }
    }
}

impl WireCoreRuntimeErrorKind {
    fn into_core(self) -> CoreRuntimeErrorKind {
        match self {
            Self::Authentication => CoreRuntimeErrorKind::Authentication,
            Self::ProtocolMismatch => CoreRuntimeErrorKind::ProtocolMismatch,
            Self::TunPermissionDenied => CoreRuntimeErrorKind::TunPermissionDenied,
            Self::TunUnsupported => CoreRuntimeErrorKind::TunUnsupported,
            Self::InvalidBundle => CoreRuntimeErrorKind::InvalidBundle,
            Self::ProcessIdentityMismatch => CoreRuntimeErrorKind::ProcessIdentityMismatch,
            Self::Apply => CoreRuntimeErrorKind::Apply,
            Self::ReloadTimeout => CoreRuntimeErrorKind::ReloadTimeout,
            Self::Readiness => CoreRuntimeErrorKind::Readiness,
            Self::Unavailable => CoreRuntimeErrorKind::Unavailable,
        }
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct WireCoreRuntimeError {
    kind: WireCoreRuntimeErrorKind,
}

impl WireCoreRuntimeError {
    pub(super) fn into_core(self) -> CoreRuntimeError {
        CoreRuntimeError::new(
            self.kind.into_core(),
            "remote Core runtime operation failed",
        )
    }
}
