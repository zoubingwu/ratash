use crate::constants::{
    CORE_LOG_LINE_MAX_BYTES, CORE_RESTART_INITIAL_BACKOFF, CORE_RESTART_LIMIT,
    CORE_RESTART_MAX_BACKOFF, CORE_SERVICE_LIVENESS_INTERVAL, EFFECTIVE_CONFIGURATION_MAX_BYTES,
    LOG_CAPACITY, MIHOMO_BINARY_MAX_BYTES, PROFILE_RESPONSE_MAX_BYTES,
};
use crate::core::{
    ApplyCandidateResult, ApplyDisposition, CoreControlEndpoint, CoreRuntime,
    CoreRuntimeDiagnosticCategory, CoreRuntimeError, CoreRuntimeErrorKind, CoreRuntimeLifecycle,
    CoreRuntimeRestartStatus, CoreRuntimeStatus, CoreRuntimeTunReason, CoreRuntimeTunStatus,
    ForwardedCoreLog, ForwardedCoreLogBatch, ManagedCoreHandle, OwnerSession, OwnerSessionProof,
    OwnerSessionRequest, ProcessOutputSource, RuntimeBundle, StopCoreResult,
};
use crate::domain::{CoreInstanceGeneration, RuntimeGeneration};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::fmt;
use std::fs::{self, OpenOptions};
use std::io::{self, Read, Write};
#[cfg(unix)]
use std::os::unix::fs::{FileTypeExt, MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Component, Path, PathBuf};
use std::sync::Mutex;
use std::time::Duration;

pub const CORE_RUNTIME_PROTOCOL_VERSION: u16 = 1;
const RUNTIME_MANIFEST_SCHEMA_VERSION: u16 = 1;
const RUNTIME_MANIFEST_MAX_BYTES: usize = 64 * 1_024;
const RUNTIME_PROVIDER_FILE_MAX: usize = 1_024;
const SERVICE_GENERATION_STATE_SCHEMA_VERSION: u16 = 1;
const SERVICE_GENERATION_STATE_FILE: &str = "generation-state-v1.json";
#[cfg(unix)]
const SERVICE_GENERATION_LOCK_FILE: &str = "generation-state-v1.lock";
const SERVICE_GENERATION_STATE_MAX_BYTES: usize = 1_024;
#[cfg(unix)]
const SERVICE_DIRECTORY_MODE: u32 = 0o711;
#[cfg(unix)]
const SERVICE_GENERATION_STATE_MODE: u32 = 0o600;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct ServiceGenerationStateV1 {
    schema_version: u16,
    owner_generation: u64,
    core_instance_generation: u64,
}

impl ServiceGenerationStateV1 {
    const fn initial() -> Self {
        Self {
            schema_version: SERVICE_GENERATION_STATE_SCHEMA_VERSION,
            owner_generation: 0,
            core_instance_generation: 0,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeManifestFileV1 {
    pub path: String,
    pub sha256: String,
    pub size: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeManifestV1 {
    schema_version: u16,
    pub runtime_generation: u64,
    pub compiler_policy_sha256: String,
    pub mihomo_binary_sha256: String,
    pub configuration_sha256: String,
    pub executable: String,
    pub configuration: String,
    pub provider_files: Vec<RuntimeManifestFileV1>,
}

impl RuntimeManifestV1 {
    #[must_use]
    pub fn new(
        runtime_generation: RuntimeGeneration,
        compiler_policy_sha256: impl Into<String>,
        mihomo_binary_sha256: impl Into<String>,
        configuration_sha256: impl Into<String>,
    ) -> Self {
        Self {
            schema_version: RUNTIME_MANIFEST_SCHEMA_VERSION,
            runtime_generation: runtime_generation.0,
            compiler_policy_sha256: compiler_policy_sha256.into(),
            mihomo_binary_sha256: mihomo_binary_sha256.into(),
            configuration_sha256: configuration_sha256.into(),
            executable: "mihomo".to_owned(),
            configuration: "config.yaml".to_owned(),
            provider_files: Vec::new(),
        }
    }

    #[must_use]
    pub fn with_provider_files(mut self, mut provider_files: Vec<RuntimeManifestFileV1>) -> Self {
        provider_files.sort_by(|left, right| left.path.cmp(&right.path));
        self.provider_files = provider_files;
        self
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ServicePlatformErrorKind {
    Credential,
    ProcessInspection,
    Spawn,
    Reload,
    ReloadTimeout,
    Stop,
    Readiness,
    ReadinessTimeout,
    Logs,
    TunUnavailable,
    TunUnsupported,
    Randomness,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ServicePlatformError {
    pub kind: ServicePlatformErrorKind,
}

impl ServicePlatformError {
    #[must_use]
    pub const fn new(kind: ServicePlatformErrorKind) -> Self {
        Self { kind }
    }
}

pub trait CallerCredentialValidator: Send + Sync {
    fn validate(&self, request: &OwnerSessionRequest) -> Result<(), ServicePlatformError>;
}

pub trait ProcessIdentityProbe: Send + Sync {
    fn start_identity(&self, pid: u32) -> Result<Option<String>, ServicePlatformError>;
}

pub trait TunCapabilityPreflight: Send + Sync {
    fn check(&self, owner_uid: u32) -> Result<(), ServicePlatformError>;
}

pub trait SecretGenerator: Send + Sync {
    fn generate(&self) -> Result<String, ServicePlatformError>;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct UuidSecretGenerator;

impl SecretGenerator for UuidSecretGenerator {
    fn generate(&self) -> Result<String, ServicePlatformError> {
        Ok(uuid::Uuid::new_v4().to_string())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OwnedProcessIdentity {
    pub pid: u32,
    pub process_start_identity: String,
    pub instance_generation: CoreInstanceGeneration,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SpawnedCoreProcess {
    pub pid: u32,
    pub process_start_identity: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CoreProcessLog {
    pub timestamp_unix_ms: u64,
    pub source: ProcessOutputSource,
    pub message: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CoreProcessLogBatch {
    pub records: Vec<CoreProcessLog>,
    pub dropped: u64,
}

#[derive(Clone, Eq, PartialEq)]
pub struct VerifiedRuntimeBundle {
    bundle: RuntimeBundle,
    manifest_path: PathBuf,
    executable_path: PathBuf,
    configuration_path: PathBuf,
}

impl VerifiedRuntimeBundle {
    #[must_use]
    pub fn bundle(&self) -> &RuntimeBundle {
        &self.bundle
    }

    #[must_use]
    pub fn manifest_path(&self) -> &Path {
        &self.manifest_path
    }

    #[must_use]
    pub fn executable_path(&self) -> &Path {
        &self.executable_path
    }

    #[must_use]
    pub fn configuration_path(&self) -> &Path {
        &self.configuration_path
    }
}

impl fmt::Debug for VerifiedRuntimeBundle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("VerifiedRuntimeBundle")
            .field("generation", &self.bundle.generation)
            .field("manifest_sha256", &self.bundle.manifest_sha256)
            .field(
                "compiler_policy_sha256",
                &self.bundle.compiler_policy_sha256,
            )
            .field("mihomo_binary_sha256", &self.bundle.mihomo_binary_sha256)
            .finish()
    }
}

pub trait CoreProcessController: Send + Sync {
    fn spawn(
        &self,
        bundle: &VerifiedRuntimeBundle,
        endpoint: &CoreControlEndpoint,
        instance_generation: CoreInstanceGeneration,
    ) -> Result<SpawnedCoreProcess, ServicePlatformError>;

    fn reload(
        &self,
        process: &OwnedProcessIdentity,
        bundle: &VerifiedRuntimeBundle,
    ) -> Result<(), ServicePlatformError>;

    fn stop(&self, process: &OwnedProcessIdentity) -> Result<(), ServicePlatformError>;

    fn readiness(
        &self,
        process: &OwnedProcessIdentity,
        endpoint: &CoreControlEndpoint,
    ) -> Result<(), ServicePlatformError>;

    fn grant_endpoint_access(
        &self,
        endpoint: &CoreControlEndpoint,
        owner_uid: u32,
    ) -> Result<(), ServicePlatformError>;

    fn reap_if_exited(&self, process: &OwnedProcessIdentity) -> Result<bool, ServicePlatformError>;

    fn take_logs(
        &self,
        process: &OwnedProcessIdentity,
        limit: usize,
    ) -> Result<CoreProcessLogBatch, ServicePlatformError>;
}

pub struct PrivilegedServiceConfig {
    pub protocol_version: u16,
    pub service_owned_root: PathBuf,
    pub compiler_policy_sha256: String,
    pub mihomo_binary_sha256: String,
    pub restart_limit: usize,
    pub log_capacity: usize,
    pub max_log_line_bytes: usize,
}

impl PrivilegedServiceConfig {
    #[must_use]
    pub fn product_defaults(
        service_owned_root: PathBuf,
        compiler_policy_sha256: String,
        mihomo_binary_sha256: String,
    ) -> Self {
        Self {
            protocol_version: CORE_RUNTIME_PROTOCOL_VERSION,
            service_owned_root,
            compiler_policy_sha256,
            mihomo_binary_sha256,
            restart_limit: CORE_RESTART_LIMIT,
            log_capacity: LOG_CAPACITY,
            max_log_line_bytes: CORE_LOG_LINE_MAX_BYTES,
        }
    }
}

pub struct PrivilegedServiceDependencies {
    pub credentials: Box<dyn CallerCredentialValidator>,
    pub identities: Box<dyn ProcessIdentityProbe>,
    pub tun: Box<dyn TunCapabilityPreflight>,
    pub secrets: Box<dyn SecretGenerator>,
    pub processes: Box<dyn CoreProcessController>,
}

#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ServiceGenerationStateCommitFault {
    Write,
    FileSync,
    Rename,
    DirectorySync,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PrivilegedServiceLifecycle {
    Idle,
    Owned,
    Running,
    Degraded,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OwnerSessionMetadata {
    pub owner_generation: u64,
    pub endpoint: CoreControlEndpoint,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PrivilegedServiceSnapshot {
    pub lifecycle: PrivilegedServiceLifecycle,
    pub owner_generation: u64,
    pub owner_pid: u32,
    pub endpoint: CoreControlEndpoint,
    pub managed_core: Option<ManagedCoreHandle>,
    pub consecutive_restart_failures: usize,
    pub diagnostic: Option<CoreRuntimeDiagnosticCategory>,
    pub dropped_log_sequence: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CoreExitIdentity {
    pub pid: u32,
    pub process_start_identity: String,
    pub instance_generation: CoreInstanceGeneration,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum UnexpectedExitOutcome {
    Pending {
        attempts: usize,
        next_attempt_at: Duration,
    },
    Restarted {
        attempts: usize,
        managed_core: ManagedCoreHandle,
    },
    Degraded {
        attempts: usize,
        diagnostic: CoreRuntimeDiagnosticCategory,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ServiceMaintenanceOutcome {
    Unchanged(PrivilegedServiceLifecycle),
    OwnerRevoked,
    UnexpectedExit(UnexpectedExitOutcome),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServiceMaintenanceStep {
    pub outcome: ServiceMaintenanceOutcome,
    pub next_deadline: Duration,
}

#[derive(Clone)]
struct ServiceOwner {
    owner_uid: u32,
    supervisor_pid: u32,
    supervisor_start_identity: String,
    instance_token: String,
    generation: u64,
    proof: OwnerSessionProof,
    endpoint: CoreControlEndpoint,
}

#[derive(Clone)]
struct ManagedCoreRecord {
    handle: ManagedCoreHandle,
    owned_identity: OwnedProcessIdentity,
    endpoint_identity: Option<OwnedEndpointIdentity>,
}

#[derive(Clone)]
struct OwnedEndpointIdentity {
    device: u64,
    inode: u64,
}

struct ServiceState {
    owner_generation: u64,
    owner: Option<ServiceOwner>,
    core_instance_generation: u64,
    managed_core: Option<ManagedCoreRecord>,
    last_bundle: Option<RuntimeBundle>,
    degraded: bool,
    consecutive_restart_failures: usize,
    diagnostic: Option<CoreRuntimeDiagnosticCategory>,
    restart_due_at: Option<Duration>,
    restart_backoff: Option<Duration>,
    next_liveness_at: Option<Duration>,
    logs: VecDeque<ForwardedCoreLog>,
    next_log_sequence: u64,
    dropped_log_sequence: u64,
}

/// Implements the privileged side of the CoreRuntime boundary.
///
/// `RuntimeBundle::generation_root` must name a bundle already staged below
/// `PrivilegedServiceConfig::service_owned_root`. The transport adapter owns the
/// copy into that root before it calls this state machine.
pub struct PrivilegedCoreRuntimeService {
    protocol_version: u16,
    service_owned_root: PathBuf,
    control_root: PathBuf,
    compiler_policy_sha256: String,
    mihomo_binary_sha256: String,
    restart_limit: usize,
    log_capacity: usize,
    max_log_line_bytes: usize,
    dependencies: PrivilegedServiceDependencies,
    generation_state_commit_fault: Mutex<Option<ServiceGenerationStateCommitFault>>,
    state: Mutex<ServiceState>,
}

impl PrivilegedCoreRuntimeService {
    pub fn new(
        config: PrivilegedServiceConfig,
        dependencies: PrivilegedServiceDependencies,
    ) -> Result<Self, CoreRuntimeError> {
        if config.protocol_version == 0
            || config.restart_limit == 0
            || config.log_capacity == 0
            || config.max_log_line_bytes == 0
            || !valid_digest(&config.compiler_policy_sha256)
            || !valid_digest(&config.mihomo_binary_sha256)
        {
            return Err(service_error(
                CoreRuntimeErrorKind::InvalidBundle,
                "invalid privileged service configuration",
            ));
        }
        let service_owned_root = prepare_service_owned_root(&config.service_owned_root)?;
        let control_root = prepare_control_root(&service_owned_root)?;
        let generation_state = with_generation_state_lock(&control_root, || {
            cleanup_pending_generation_states(&control_root)?;
            load_or_initialize_generation_state(&control_root)
        })?;

        Ok(Self {
            protocol_version: config.protocol_version,
            service_owned_root,
            control_root,
            compiler_policy_sha256: config.compiler_policy_sha256,
            mihomo_binary_sha256: config.mihomo_binary_sha256,
            restart_limit: config.restart_limit,
            log_capacity: config.log_capacity,
            max_log_line_bytes: config.max_log_line_bytes,
            dependencies,
            generation_state_commit_fault: Mutex::new(None),
            state: Mutex::new(ServiceState {
                owner_generation: generation_state.owner_generation,
                owner: None,
                core_instance_generation: generation_state.core_instance_generation,
                managed_core: None,
                last_bundle: None,
                degraded: false,
                consecutive_restart_failures: 0,
                diagnostic: None,
                restart_due_at: None,
                restart_backoff: None,
                next_liveness_at: None,
                logs: VecDeque::with_capacity(config.log_capacity),
                next_log_sequence: 1,
                dropped_log_sequence: 0,
            }),
        })
    }

    pub fn owner_metadata(
        &self,
        proof: &OwnerSessionProof,
    ) -> Result<OwnerSessionMetadata, CoreRuntimeError> {
        let state = self.lock_state()?;
        let owner = authenticated_owner(&state, proof)?;
        Ok(OwnerSessionMetadata {
            owner_generation: owner.generation,
            endpoint: owner.endpoint.clone(),
        })
    }

    pub fn snapshot(
        &self,
        proof: &OwnerSessionProof,
    ) -> Result<PrivilegedServiceSnapshot, CoreRuntimeError> {
        let state = self.lock_state()?;
        let owner = authenticated_owner(&state, proof)?;
        let lifecycle = service_lifecycle(&state);
        Ok(PrivilegedServiceSnapshot {
            lifecycle,
            owner_generation: owner.generation,
            owner_pid: owner.supervisor_pid,
            endpoint: owner.endpoint.clone(),
            managed_core: state
                .managed_core
                .as_ref()
                .map(|record| record.handle.clone()),
            consecutive_restart_failures: state.consecutive_restart_failures,
            diagnostic: state.diagnostic,
            dropped_log_sequence: state.dropped_log_sequence,
        })
    }

    pub fn revoke_owner(&self, proof: &OwnerSessionProof) -> Result<(), CoreRuntimeError> {
        let mut state = self.lock_state()?;
        authenticated_owner(&state, proof)?;
        self.cleanup_owner(&mut state)
    }

    pub fn maintenance_step(
        &self,
        now: Duration,
    ) -> Result<ServiceMaintenanceStep, CoreRuntimeError> {
        let mut state = self.lock_state()?;
        if state.next_liveness_at.is_none() {
            state.next_liveness_at = Some(now);
        }
        let Some(owner) = state.owner.clone() else {
            state.next_liveness_at = Some(deadline_after(now, CORE_SERVICE_LIVENESS_INTERVAL));
            return Ok(maintenance_result(
                &state,
                ServiceMaintenanceOutcome::Unchanged(PrivilegedServiceLifecycle::Idle),
            ));
        };

        let restart_is_due = state.restart_due_at.is_some_and(|deadline| deadline <= now);
        let liveness_is_due = state
            .next_liveness_at
            .is_some_and(|deadline| deadline <= now);
        if restart_is_due || liveness_is_due {
            let owner_start_identity = self
                .dependencies
                .identities
                .start_identity(owner.supervisor_pid)
                .map_err(|_| {
                    service_error(
                        CoreRuntimeErrorKind::Unavailable,
                        "owner process identity inspection failed",
                    )
                })?;
            if owner_start_identity.as_deref() != Some(owner.supervisor_start_identity.as_str()) {
                self.cleanup_owner(&mut state)?;
                state.next_liveness_at = Some(deadline_after(now, CORE_SERVICE_LIVENESS_INTERVAL));
                return Ok(maintenance_result(
                    &state,
                    ServiceMaintenanceOutcome::OwnerRevoked,
                ));
            }
        }

        if liveness_is_due {
            state.next_liveness_at = Some(deadline_after(now, CORE_SERVICE_LIVENESS_INTERVAL));
        }

        if restart_is_due && !state.degraded {
            let outcome = self.attempt_owned_core_restart(&mut state, &owner, now)?;
            return Ok(maintenance_result(
                &state,
                ServiceMaintenanceOutcome::UnexpectedExit(outcome),
            ));
        }

        if liveness_is_due
            && !state.degraded
            && let Some(record) = state.managed_core.clone()
            && !self.owned_process_is_live(&record.owned_identity)?
        {
            self.drain_logs(&mut state, &record)?;
            self.remove_owned_endpoint(&record)?;
            state.managed_core = None;
            let outcome = schedule_initial_restart(&mut state, now)?;
            return Ok(maintenance_result(
                &state,
                ServiceMaintenanceOutcome::UnexpectedExit(outcome),
            ));
        }

        let outcome = match state.restart_due_at {
            Some(next_attempt_at) if !state.degraded => {
                ServiceMaintenanceOutcome::UnexpectedExit(UnexpectedExitOutcome::Pending {
                    attempts: state.consecutive_restart_failures,
                    next_attempt_at,
                })
            }
            _ => ServiceMaintenanceOutcome::Unchanged(service_lifecycle(&state)),
        };
        Ok(maintenance_result(&state, outcome))
    }

    pub fn shutdown_service(&self) -> Result<(), CoreRuntimeError> {
        let mut state = self.lock_state()?;
        self.cleanup_owner(&mut state)
    }

    pub fn handle_unexpected_exit(
        &self,
        proof: &OwnerSessionProof,
        exited: &CoreExitIdentity,
    ) -> Result<UnexpectedExitOutcome, CoreRuntimeError> {
        self.handle_unexpected_exit_at(proof, exited, Duration::ZERO)
    }

    pub fn handle_unexpected_exit_at(
        &self,
        proof: &OwnerSessionProof,
        exited: &CoreExitIdentity,
        now: Duration,
    ) -> Result<UnexpectedExitOutcome, CoreRuntimeError> {
        let mut state = self.lock_state()?;
        authenticated_owner(&state, proof)?;
        let record = state.managed_core.clone().ok_or_else(|| {
            service_error(
                CoreRuntimeErrorKind::ProcessIdentityMismatch,
                "unexpected exit has no owned Core",
            )
        })?;
        if record.owned_identity.pid != exited.pid
            || record.owned_identity.process_start_identity != exited.process_start_identity
            || record.owned_identity.instance_generation != exited.instance_generation
        {
            return Err(service_error(
                CoreRuntimeErrorKind::ProcessIdentityMismatch,
                "unexpected exit identity mismatch",
            ));
        }
        if self.owned_process_is_live(&record.owned_identity)? {
            return Err(service_error(
                CoreRuntimeErrorKind::ProcessIdentityMismatch,
                "unexpected exit process remains live",
            ));
        }
        self.drain_logs(&mut state, &record)?;
        self.remove_owned_endpoint(&record)?;
        state.managed_core = None;
        schedule_initial_restart(&mut state, now)
    }

    fn lock_state(&self) -> Result<std::sync::MutexGuard<'_, ServiceState>, CoreRuntimeError> {
        self.state.lock().map_err(|_| {
            service_error(
                CoreRuntimeErrorKind::Unavailable,
                "service state lock unavailable",
            )
        })
    }

    fn reserve_owner_generation(&self, state: &mut ServiceState) -> Result<u64, CoreRuntimeError> {
        with_generation_state_lock(&self.control_root, || {
            let persisted = load_generation_state(&self.control_root)?;
            state.owner_generation = state
                .owner_generation
                .max(persisted.owner_generation)
                .checked_add(1)
                .ok_or_else(|| {
                    service_error(
                        CoreRuntimeErrorKind::Unavailable,
                        "owner generation exhausted",
                    )
                })?;
            state.core_instance_generation = state
                .core_instance_generation
                .max(persisted.core_instance_generation);
            self.persist_generation_state(state)?;
            Ok(state.owner_generation)
        })
    }

    fn reserve_core_instance_generation(
        &self,
        state: &mut ServiceState,
    ) -> Result<CoreInstanceGeneration, CoreRuntimeError> {
        with_generation_state_lock(&self.control_root, || {
            let persisted = load_generation_state(&self.control_root)?;
            state.owner_generation = state.owner_generation.max(persisted.owner_generation);
            state.core_instance_generation = state
                .core_instance_generation
                .max(persisted.core_instance_generation)
                .checked_add(1)
                .ok_or_else(|| {
                    service_error(
                        CoreRuntimeErrorKind::Unavailable,
                        "Core Instance Generation exhausted",
                    )
                })?;
            self.persist_generation_state(state)?;
            Ok(CoreInstanceGeneration(state.core_instance_generation))
        })
    }

    fn persist_generation_state(&self, state: &ServiceState) -> Result<(), CoreRuntimeError> {
        let fault = self
            .generation_state_commit_fault
            .lock()
            .map_err(|_| {
                service_error(
                    CoreRuntimeErrorKind::Unavailable,
                    "service generation state fault seam is unavailable",
                )
            })?
            .take();
        persist_generation_state_with_fault(
            &self.control_root,
            ServiceGenerationStateV1 {
                schema_version: SERVICE_GENERATION_STATE_SCHEMA_VERSION,
                owner_generation: state.owner_generation,
                core_instance_generation: state.core_instance_generation,
            },
            fault,
        )
    }

    #[doc(hidden)]
    pub fn arm_generation_state_commit_fault(
        &self,
        fault: ServiceGenerationStateCommitFault,
    ) -> Result<(), CoreRuntimeError> {
        let mut armed = self.generation_state_commit_fault.lock().map_err(|_| {
            service_error(
                CoreRuntimeErrorKind::Unavailable,
                "service generation state fault seam is unavailable",
            )
        })?;
        if armed.is_some() {
            return Err(service_error(
                CoreRuntimeErrorKind::Unavailable,
                "service generation state fault seam is already armed",
            ));
        }
        *armed = Some(fault);
        Ok(())
    }

    fn cleanup_owner(&self, state: &mut ServiceState) -> Result<(), CoreRuntimeError> {
        if let Some(record) = state.managed_core.clone() {
            if self.owned_process_is_live(&record.owned_identity)? {
                self.dependencies
                    .processes
                    .stop(&record.owned_identity)
                    .map_err(map_stop_error)?;
            }
            self.drain_logs(state, &record)?;
            self.remove_owned_endpoint(&record)?;
        }
        state.managed_core = None;
        state.owner = None;
        state.last_bundle = None;
        reset_restart_state(state);
        state.next_liveness_at = None;
        state.logs.clear();
        state.dropped_log_sequence = state.next_log_sequence.saturating_sub(1);
        Ok(())
    }

    fn attempt_owned_core_restart(
        &self,
        state: &mut ServiceState,
        owner: &ServiceOwner,
        now: Duration,
    ) -> Result<UnexpectedExitOutcome, CoreRuntimeError> {
        let bundle = state.last_bundle.clone().ok_or_else(|| {
            service_error(
                CoreRuntimeErrorKind::Unavailable,
                "unexpected exit has no runtime bundle",
            )
        })?;
        let attempt = state.consecutive_restart_failures.saturating_add(1);
        state.consecutive_restart_failures = attempt;

        let restarted = self.verify_bundle(&bundle).and_then(|verified| {
            self.dependencies.tun.check(owner.owner_uid).map_err(|_| {
                service_error(
                    CoreRuntimeErrorKind::TunPermissionDenied,
                    "TUN capability preflight failed",
                )
            })?;
            self.spawn_verified(state, owner, &verified)
        });
        if let Ok(record) = restarted {
            let managed_core = record.handle.clone();
            state.managed_core = Some(record);
            reset_restart_state(state);
            state.next_liveness_at = Some(deadline_after(now, CORE_SERVICE_LIVENESS_INTERVAL));
            return Ok(UnexpectedExitOutcome::Restarted {
                attempts: attempt,
                managed_core,
            });
        }

        if attempt >= self.restart_limit {
            let diagnostic = CoreRuntimeDiagnosticCategory::CoreRestartLimitReached;
            state.degraded = true;
            state.diagnostic = Some(diagnostic);
            state.restart_due_at = None;
            state.restart_backoff = None;
            return Ok(UnexpectedExitOutcome::Degraded {
                attempts: attempt,
                diagnostic,
            });
        }

        let backoff = restart_backoff_after(attempt);
        let next_attempt_at = deadline_after(now, backoff);
        state.restart_due_at = Some(next_attempt_at);
        state.restart_backoff = Some(backoff);
        Ok(UnexpectedExitOutcome::Pending {
            attempts: attempt,
            next_attempt_at,
        })
    }

    fn remove_owned_endpoint(&self, record: &ManagedCoreRecord) -> Result<(), CoreRuntimeError> {
        remove_matching_endpoint(
            &record.handle.endpoint.socket_path,
            record.endpoint_identity.as_ref(),
        )
    }

    fn owned_process_is_live(
        &self,
        identity: &OwnedProcessIdentity,
    ) -> Result<bool, CoreRuntimeError> {
        if self
            .dependencies
            .processes
            .reap_if_exited(identity)
            .map_err(|_| {
                service_error(
                    CoreRuntimeErrorKind::Unavailable,
                    "Core process inspection failed",
                )
            })?
        {
            return Ok(false);
        }
        self.dependencies
            .identities
            .start_identity(identity.pid)
            .map(|actual| actual.as_deref() == Some(identity.process_start_identity.as_str()))
            .map_err(|_| {
                service_error(
                    CoreRuntimeErrorKind::Unavailable,
                    "process identity inspection failed",
                )
            })
    }

    fn require_owned_process(&self, record: &ManagedCoreRecord) -> Result<(), CoreRuntimeError> {
        if self.owned_process_is_live(&record.owned_identity)? {
            Ok(())
        } else {
            Err(service_error(
                CoreRuntimeErrorKind::ProcessIdentityMismatch,
                "owned Core identity mismatch",
            ))
        }
    }

    fn verify_bundle(
        &self,
        bundle: &RuntimeBundle,
    ) -> Result<VerifiedRuntimeBundle, CoreRuntimeError> {
        if !valid_digest(&bundle.manifest_sha256)
            || bundle.compiler_policy_sha256 != self.compiler_policy_sha256
            || bundle.mihomo_binary_sha256 != self.mihomo_binary_sha256
        {
            return Err(invalid_bundle("runtime identity mismatch"));
        }
        let generation_root = fs::canonicalize(&bundle.generation_root)
            .map_err(|_| invalid_bundle("runtime root unavailable"))?;
        if generation_root == self.service_owned_root
            || !generation_root.starts_with(&self.service_owned_root)
        {
            return Err(invalid_bundle("runtime root escaped service root"));
        }
        let manifest_path = generation_root.join("manifest.json");
        let manifest_bytes =
            read_bounded_regular(&generation_root, &manifest_path, RUNTIME_MANIFEST_MAX_BYTES)?;
        if crate::digest::sha256_hex(&manifest_bytes) != bundle.manifest_sha256 {
            return Err(invalid_bundle("runtime manifest digest mismatch"));
        }
        let manifest: RuntimeManifestV1 = serde_json::from_slice(&manifest_bytes)
            .map_err(|_| invalid_bundle("runtime manifest is invalid"))?;
        if manifest.schema_version != RUNTIME_MANIFEST_SCHEMA_VERSION
            || manifest.runtime_generation != bundle.generation.0
            || manifest.compiler_policy_sha256 != bundle.compiler_policy_sha256
            || manifest.mihomo_binary_sha256 != bundle.mihomo_binary_sha256
            || !valid_digest(&manifest.configuration_sha256)
            || manifest.executable != "mihomo"
            || manifest.configuration != "config.yaml"
        {
            return Err(invalid_bundle("runtime manifest fields mismatch"));
        }

        let executable_path = generation_root.join("mihomo");
        let binary =
            read_bounded_executable(&generation_root, &executable_path, MIHOMO_BINARY_MAX_BYTES)?;
        if crate::digest::sha256_hex(&binary) != bundle.mihomo_binary_sha256 {
            return Err(invalid_bundle("Mihomo binary identity mismatch"));
        }
        let configuration_path = generation_root.join("config.yaml");
        let configuration = read_bounded_regular(
            &generation_root,
            &configuration_path,
            EFFECTIVE_CONFIGURATION_MAX_BYTES,
        )?;
        if crate::digest::sha256_hex(&configuration) != manifest.configuration_sha256 {
            return Err(invalid_bundle("runtime configuration identity mismatch"));
        }
        verify_provider_files(&generation_root, &manifest.provider_files)?;

        Ok(VerifiedRuntimeBundle {
            bundle: RuntimeBundle {
                generation: bundle.generation,
                generation_root,
                manifest_sha256: bundle.manifest_sha256.clone(),
                compiler_policy_sha256: bundle.compiler_policy_sha256.clone(),
                mihomo_binary_sha256: bundle.mihomo_binary_sha256.clone(),
            },
            manifest_path,
            executable_path,
            configuration_path,
        })
    }

    fn spawn_verified(
        &self,
        state: &mut ServiceState,
        owner: &ServiceOwner,
        bundle: &VerifiedRuntimeBundle,
    ) -> Result<ManagedCoreRecord, CoreRuntimeError> {
        let instance_generation = self.reserve_core_instance_generation(state)?;
        let spawned = self
            .dependencies
            .processes
            .spawn(bundle, &owner.endpoint, instance_generation)
            .map_err(map_spawn_error)?;
        if spawned.pid == 0 || spawned.process_start_identity.is_empty() {
            return Err(service_error(
                CoreRuntimeErrorKind::ProcessIdentityMismatch,
                "spawned Core identity is invalid",
            ));
        }
        let owned_identity = OwnedProcessIdentity {
            pid: spawned.pid,
            process_start_identity: spawned.process_start_identity.clone(),
            instance_generation,
        };
        if !self.owned_process_is_live(&owned_identity)? {
            return Err(service_error(
                CoreRuntimeErrorKind::ProcessIdentityMismatch,
                "spawned Core identity could not be confirmed",
            ));
        }
        if let Err(error) = self
            .dependencies
            .processes
            .readiness(&owned_identity, &owner.endpoint)
        {
            let endpoint_identity = capture_endpoint_identity(&owner.endpoint.socket_path);
            if self.dependencies.processes.stop(&owned_identity).is_ok() {
                let _ = remove_matching_endpoint(
                    &owner.endpoint.socket_path,
                    endpoint_identity.as_ref(),
                );
            }
            return Err(map_readiness_error(error));
        }
        if self
            .dependencies
            .processes
            .grant_endpoint_access(&owner.endpoint, owner.owner_uid)
            .is_err()
        {
            let endpoint_identity = capture_endpoint_identity(&owner.endpoint.socket_path);
            if self.dependencies.processes.stop(&owned_identity).is_ok() {
                let _ = remove_matching_endpoint(
                    &owner.endpoint.socket_path,
                    endpoint_identity.as_ref(),
                );
            }
            return Err(service_error(
                CoreRuntimeErrorKind::Apply,
                "Core control endpoint access setup failed",
            ));
        }
        Ok(ManagedCoreRecord {
            handle: ManagedCoreHandle {
                pid: spawned.pid,
                process_start_identity: spawned.process_start_identity,
                endpoint: owner.endpoint.clone(),
                instance_generation,
                runtime_generation: bundle.bundle.generation,
            },
            owned_identity,
            endpoint_identity: capture_endpoint_identity(&owner.endpoint.socket_path),
        })
    }

    fn drain_logs(
        &self,
        state: &mut ServiceState,
        record: &ManagedCoreRecord,
    ) -> Result<(), CoreRuntimeError> {
        let incoming = self
            .dependencies
            .processes
            .take_logs(&record.owned_identity, self.log_capacity)
            .map_err(|_| {
                service_error(
                    CoreRuntimeErrorKind::Unavailable,
                    "Core log forwarding failed",
                )
            })?;
        if incoming.dropped > 0 {
            let last_dropped = state
                .next_log_sequence
                .saturating_add(incoming.dropped.saturating_sub(1));
            state.dropped_log_sequence = state.dropped_log_sequence.max(last_dropped);
            state.next_log_sequence = last_dropped.saturating_add(1);
        }
        for log in incoming.records {
            let Some(sequence) = next_sequence(&mut state.next_log_sequence) else {
                break;
            };
            let message = truncate_utf8(log.message, self.max_log_line_bytes);
            if state.logs.len() == self.log_capacity
                && let Some(dropped) = state.logs.pop_front()
            {
                state.dropped_log_sequence = state.dropped_log_sequence.max(dropped.sequence);
            }
            state.logs.push_back(ForwardedCoreLog {
                sequence,
                timestamp_unix_ms: log.timestamp_unix_ms,
                source: log.source,
                message,
                instance_generation: record.owned_identity.instance_generation,
            });
        }
        Ok(())
    }
}

fn schedule_initial_restart(
    state: &mut ServiceState,
    now: Duration,
) -> Result<UnexpectedExitOutcome, CoreRuntimeError> {
    if state.last_bundle.is_none() {
        return Err(service_error(
            CoreRuntimeErrorKind::Unavailable,
            "unexpected exit has no runtime bundle",
        ));
    }
    state.degraded = false;
    state.consecutive_restart_failures = 0;
    state.diagnostic = None;
    let next_attempt_at = deadline_after(now, CORE_RESTART_INITIAL_BACKOFF);
    state.restart_due_at = Some(next_attempt_at);
    state.restart_backoff = Some(CORE_RESTART_INITIAL_BACKOFF);
    Ok(UnexpectedExitOutcome::Pending {
        attempts: 0,
        next_attempt_at,
    })
}

fn reset_restart_state(state: &mut ServiceState) {
    state.degraded = false;
    state.consecutive_restart_failures = 0;
    state.diagnostic = None;
    state.restart_due_at = None;
    state.restart_backoff = None;
}

fn restart_backoff_after(failed_attempts: usize) -> Duration {
    let exponent = u32::try_from(failed_attempts).unwrap_or(u32::MAX);
    let multiplier = 2_u32.checked_pow(exponent).unwrap_or(u32::MAX);
    CORE_RESTART_INITIAL_BACKOFF
        .checked_mul(multiplier)
        .unwrap_or(CORE_RESTART_MAX_BACKOFF)
        .min(CORE_RESTART_MAX_BACKOFF)
}

fn deadline_after(now: Duration, delay: Duration) -> Duration {
    now.checked_add(delay).unwrap_or(Duration::MAX)
}

fn service_lifecycle(state: &ServiceState) -> PrivilegedServiceLifecycle {
    if state.degraded {
        PrivilegedServiceLifecycle::Degraded
    } else if state.managed_core.is_some() {
        PrivilegedServiceLifecycle::Running
    } else if state.owner.is_some() {
        PrivilegedServiceLifecycle::Owned
    } else {
        PrivilegedServiceLifecycle::Idle
    }
}

fn maintenance_result(
    state: &ServiceState,
    outcome: ServiceMaintenanceOutcome,
) -> ServiceMaintenanceStep {
    let next_deadline = match (state.restart_due_at, state.next_liveness_at) {
        (Some(restart), Some(liveness)) => restart.min(liveness),
        (Some(restart), None) => restart,
        (None, Some(liveness)) => liveness,
        (None, None) => Duration::MAX,
    };
    ServiceMaintenanceStep {
        outcome,
        next_deadline,
    }
}

fn capture_endpoint_identity(path: &Path) -> Option<OwnedEndpointIdentity> {
    let metadata = fs::symlink_metadata(path).ok()?;
    if !metadata.file_type().is_socket() {
        return None;
    }
    Some(OwnedEndpointIdentity {
        device: metadata.dev(),
        inode: metadata.ino(),
    })
}

fn remove_matching_endpoint(
    path: &Path,
    expected: Option<&OwnedEndpointIdentity>,
) -> Result<(), CoreRuntimeError> {
    let Some(expected) = expected else {
        return Ok(());
    };
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(_) => {
            return Err(service_error(
                CoreRuntimeErrorKind::Unavailable,
                "owned Core endpoint inspection failed",
            ));
        }
    };
    if !metadata.file_type().is_socket()
        || metadata.dev() != expected.device
        || metadata.ino() != expected.inode
    {
        return Ok(());
    }
    fs::remove_file(path).map_err(|_| {
        service_error(
            CoreRuntimeErrorKind::Unavailable,
            "owned Core endpoint cleanup failed",
        )
    })
}

impl CoreRuntime for PrivilegedCoreRuntimeService {
    fn open_owner_session(
        &self,
        request: &OwnerSessionRequest,
    ) -> Result<OwnerSession, CoreRuntimeError> {
        if request.protocol_version != self.protocol_version {
            return Err(service_error(
                CoreRuntimeErrorKind::ProtocolMismatch,
                "Core runtime protocol mismatch",
            ));
        }
        if request.supervisor_pid == 0
            || request.supervisor_start_identity.is_empty()
            || request.instance_token.is_empty()
        {
            return Err(service_error(
                CoreRuntimeErrorKind::Authentication,
                "owner identity is incomplete",
            ));
        }
        self.dependencies
            .credentials
            .validate(request)
            .map_err(|_| {
                service_error(
                    CoreRuntimeErrorKind::Authentication,
                    "caller credential validation failed",
                )
            })?;
        let mut state = self.lock_state()?;

        if let Some(owner) = state.owner.as_ref() {
            if owner.owner_uid == request.owner_uid
                && owner.supervisor_pid == request.supervisor_pid
                && owner.supervisor_start_identity == request.supervisor_start_identity
                && owner.instance_token == request.instance_token
            {
                return Ok(OwnerSession {
                    proof: owner.proof.clone(),
                    protocol_version: self.protocol_version,
                    owner_generation: owner.generation,
                    endpoint: owner.endpoint.clone(),
                });
            }
            let identity = self
                .dependencies
                .identities
                .start_identity(owner.supervisor_pid)
                .map_err(|_| {
                    service_error(
                        CoreRuntimeErrorKind::Unavailable,
                        "owner process identity inspection failed",
                    )
                })?;
            if identity.as_deref() == Some(owner.supervisor_start_identity.as_str()) {
                return Err(service_error(
                    CoreRuntimeErrorKind::Authentication,
                    "a live owner session already exists",
                ));
            }
            self.cleanup_owner(&mut state)?;
        }

        let owner_generation = self.reserve_owner_generation(&mut state)?;
        let session_id_secret = self.dependencies.secrets.generate().map_err(|_| {
            service_error(
                CoreRuntimeErrorKind::Unavailable,
                "session ID generation failed",
            )
        })?;
        let session_token = self.dependencies.secrets.generate().map_err(|_| {
            service_error(
                CoreRuntimeErrorKind::Unavailable,
                "session token generation failed",
            )
        })?;
        let endpoint_secret = self.dependencies.secrets.generate().map_err(|_| {
            service_error(
                CoreRuntimeErrorKind::Unavailable,
                "Core endpoint secret generation failed",
            )
        })?;
        if session_id_secret.is_empty() || session_token.is_empty() || endpoint_secret.is_empty() {
            return Err(service_error(
                CoreRuntimeErrorKind::Unavailable,
                "random secret generation returned an empty value",
            ));
        }
        let proof = OwnerSessionProof::new(
            format!("owner-{owner_generation}-{session_id_secret}"),
            session_token,
        );
        let endpoint = CoreControlEndpoint::new(
            self.control_root
                .join(format!("owner-{owner_generation}.sock")),
            endpoint_secret,
        );
        state.owner = Some(ServiceOwner {
            owner_uid: request.owner_uid,
            supervisor_pid: request.supervisor_pid,
            supervisor_start_identity: request.supervisor_start_identity.clone(),
            instance_token: request.instance_token.clone(),
            generation: owner_generation,
            proof: proof.clone(),
            endpoint: endpoint.clone(),
        });
        reset_restart_state(&mut state);
        state.next_liveness_at = None;

        Ok(OwnerSession {
            proof,
            protocol_version: self.protocol_version,
            owner_generation,
            endpoint,
        })
    }

    fn apply_candidate(
        &self,
        owner: &OwnerSessionProof,
        bundle: &RuntimeBundle,
    ) -> Result<ApplyCandidateResult, CoreRuntimeError> {
        let mut state = self.lock_state()?;
        let authenticated = authenticated_owner(&state, owner)?.clone();
        let verified = self.verify_bundle(bundle)?;
        self.dependencies
            .tun
            .check(authenticated.owner_uid)
            .map_err(|_| {
                service_error(
                    CoreRuntimeErrorKind::TunPermissionDenied,
                    "TUN capability preflight failed",
                )
            })?;

        if let Some(mut record) = state.managed_core.clone() {
            self.require_owned_process(&record)?;
            let reload = self
                .dependencies
                .processes
                .reload(&record.owned_identity, &verified);
            if let Err(error) = reload {
                return Err(map_reload_error(error));
            }
            if let Err(error) = self
                .dependencies
                .processes
                .readiness(&record.owned_identity, &record.handle.endpoint)
            {
                return Err(map_readiness_error(error));
            }
            record.handle.runtime_generation = bundle.generation;
            let handle = record.handle.clone();
            state.managed_core = Some(record);
            state.last_bundle = Some(bundle.clone());
            reset_restart_state(&mut state);
            state.next_liveness_at = None;
            return Ok(ApplyCandidateResult {
                disposition: ApplyDisposition::Reloaded,
                managed_core: handle,
            });
        }

        let record = self.spawn_verified(&mut state, &authenticated, &verified)?;
        let handle = record.handle.clone();
        state.managed_core = Some(record);
        state.last_bundle = Some(bundle.clone());
        reset_restart_state(&mut state);
        state.next_liveness_at = None;
        Ok(ApplyCandidateResult {
            disposition: ApplyDisposition::Spawned,
            managed_core: handle,
        })
    }

    fn status(&self, owner: &OwnerSessionProof) -> Result<CoreRuntimeStatus, CoreRuntimeError> {
        let state = self.lock_state()?;
        let authenticated = authenticated_owner(&state, owner)?;
        let (managed_core, observed_exit) = match state.managed_core.as_ref() {
            Some(record) if self.owned_process_is_live(&record.owned_identity)? => {
                (Some(record.handle.clone()), false)
            }
            Some(_) => (None, true),
            None => (None, false),
        };
        let tun = match self.dependencies.tun.check(authenticated.owner_uid) {
            Ok(()) => CoreRuntimeTunStatus::available(),
            Err(error) => match error.kind {
                ServicePlatformErrorKind::TunUnavailable => CoreRuntimeTunStatus {
                    capable: false,
                    reason: Some(CoreRuntimeTunReason::PermissionDenied),
                },
                ServicePlatformErrorKind::TunUnsupported => CoreRuntimeTunStatus {
                    capable: false,
                    reason: Some(CoreRuntimeTunReason::Unsupported),
                },
                _ => {
                    return Err(service_error(
                        CoreRuntimeErrorKind::Unavailable,
                        "TUN capability inspection failed",
                    ));
                }
            },
        };
        let core_is_running = managed_core.is_some();
        Ok(CoreRuntimeStatus {
            managed_core,
            lifecycle: if state.degraded {
                CoreRuntimeLifecycle::Degraded
            } else if state.restart_due_at.is_some() || observed_exit {
                CoreRuntimeLifecycle::RestartPending
            } else if core_is_running {
                CoreRuntimeLifecycle::Running
            } else {
                CoreRuntimeLifecycle::Owned
            },
            restart: CoreRuntimeRestartStatus {
                pending: (state.restart_due_at.is_some() || observed_exit) && !state.degraded,
                attempts: state.consecutive_restart_failures,
                backoff: state.restart_backoff.or_else(|| {
                    (observed_exit && !state.degraded).then_some(CORE_RESTART_INITIAL_BACKOFF)
                }),
                diagnostic: state.diagnostic,
            },
            tun,
        })
    }

    fn logs(
        &self,
        owner: &OwnerSessionProof,
        after_sequence: Option<u64>,
        limit: usize,
    ) -> Result<ForwardedCoreLogBatch, CoreRuntimeError> {
        let mut state = self.lock_state()?;
        authenticated_owner(&state, owner)?;
        if let Some(record) = state.managed_core.clone() {
            self.require_owned_process(&record)?;
            self.drain_logs(&mut state, &record)?;
        }
        let limit = limit.min(self.log_capacity);
        let records = state
            .logs
            .iter()
            .filter(|record| after_sequence.is_none_or(|after| record.sequence > after))
            .take(limit)
            .cloned()
            .collect::<Vec<_>>();
        let next_sequence = records
            .last()
            .map(|record| record.sequence)
            .or(after_sequence);
        Ok(ForwardedCoreLogBatch {
            records,
            next_sequence,
            dropped_before: state.dropped_log_sequence,
        })
    }

    fn stop(&self, owner: &OwnerSessionProof) -> Result<StopCoreResult, CoreRuntimeError> {
        let mut state = self.lock_state()?;
        authenticated_owner(&state, owner)?;
        let Some(record) = state.managed_core.clone() else {
            return Ok(StopCoreResult {
                stopped: false,
                instance_generation: None,
            });
        };
        self.require_owned_process(&record)?;
        self.dependencies
            .processes
            .stop(&record.owned_identity)
            .map_err(map_stop_error)?;
        self.drain_logs(&mut state, &record)?;
        self.remove_owned_endpoint(&record)?;
        let instance_generation = record.owned_identity.instance_generation;
        state.managed_core = None;
        state.last_bundle = None;
        reset_restart_state(&mut state);
        state.next_liveness_at = None;
        Ok(StopCoreResult {
            stopped: true,
            instance_generation: Some(instance_generation),
        })
    }

    fn close_owner_session(&self, owner: &OwnerSessionProof) -> Result<(), CoreRuntimeError> {
        let mut state = self.lock_state()?;
        authenticated_owner(&state, owner)?;
        self.cleanup_owner(&mut state)
    }
}

fn verify_provider_files(
    generation_root: &Path,
    files: &[RuntimeManifestFileV1],
) -> Result<(), CoreRuntimeError> {
    if files.len() > RUNTIME_PROVIDER_FILE_MAX {
        return Err(invalid_bundle("runtime provider file count exceeded"));
    }
    let mut previous_path: Option<&str> = None;
    for file in files {
        if previous_path.is_some_and(|previous| previous >= file.path.as_str())
            || !valid_manifest_relative_path(&file.path)
            || !valid_digest(&file.sha256)
            || file.size > PROFILE_RESPONSE_MAX_BYTES as u64
        {
            return Err(invalid_bundle("runtime provider manifest entry is invalid"));
        }
        let bytes = read_bounded_regular(
            generation_root,
            &generation_root.join(&file.path),
            PROFILE_RESPONSE_MAX_BYTES,
        )?;
        if bytes.len() as u64 != file.size || crate::digest::sha256_hex(&bytes) != file.sha256 {
            return Err(invalid_bundle("runtime provider identity mismatch"));
        }
        previous_path = Some(&file.path);
    }
    Ok(())
}

fn valid_manifest_relative_path(value: &str) -> bool {
    let path = Path::new(value);
    !value.is_empty()
        && !path.is_absolute()
        && !matches!(value, "manifest.json" | "config.yaml" | "mihomo")
        && path
            .components()
            .all(|component| matches!(component, Component::Normal(_)))
}

fn authenticated_owner<'a>(
    state: &'a ServiceState,
    proof: &OwnerSessionProof,
) -> Result<&'a ServiceOwner, CoreRuntimeError> {
    state
        .owner
        .as_ref()
        .filter(|owner| owner.proof == *proof)
        .ok_or_else(|| {
            service_error(
                CoreRuntimeErrorKind::Authentication,
                "owner session proof mismatch",
            )
        })
}

fn prepare_control_root(service_owned_root: &Path) -> Result<PathBuf, CoreRuntimeError> {
    let control_root = service_owned_root.join("control");
    match fs::symlink_metadata(&control_root) {
        Ok(metadata) => validate_control_root_metadata(&metadata)?,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            fs::create_dir(&control_root).map_err(|_| {
                service_error(
                    CoreRuntimeErrorKind::Unavailable,
                    "control root creation failed",
                )
            })?;
            #[cfg(unix)]
            fs::set_permissions(
                &control_root,
                fs::Permissions::from_mode(SERVICE_DIRECTORY_MODE),
            )
            .map_err(|_| {
                service_error(
                    CoreRuntimeErrorKind::Unavailable,
                    "control root permission update failed",
                )
            })?;
            let metadata = fs::symlink_metadata(&control_root).map_err(|_| {
                service_error(
                    CoreRuntimeErrorKind::Unavailable,
                    "control root validation failed",
                )
            })?;
            validate_control_root_metadata(&metadata)?;
            sync_private_service_directory(service_owned_root)?;
        }
        Err(_) => {
            return Err(service_error(
                CoreRuntimeErrorKind::Unavailable,
                "control root validation failed",
            ));
        }
    }
    Ok(control_root)
}

fn prepare_service_owned_root(configured_root: &Path) -> Result<PathBuf, CoreRuntimeError> {
    validate_service_root_parent(configured_root)?;
    match fs::symlink_metadata(configured_root) {
        Ok(metadata) => validate_service_directory_metadata(
            &metadata,
            "service root ownership, permissions, or type are invalid",
        )?,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            fs::create_dir(configured_root).map_err(|_| {
                service_error(
                    CoreRuntimeErrorKind::Unavailable,
                    "service root creation failed",
                )
            })?;
            #[cfg(unix)]
            fs::set_permissions(
                configured_root,
                fs::Permissions::from_mode(SERVICE_DIRECTORY_MODE),
            )
            .map_err(|_| {
                service_error(
                    CoreRuntimeErrorKind::Unavailable,
                    "service root permission update failed",
                )
            })?;
            let metadata = fs::symlink_metadata(configured_root).map_err(|_| {
                service_error(
                    CoreRuntimeErrorKind::Unavailable,
                    "service root validation failed",
                )
            })?;
            validate_service_directory_metadata(
                &metadata,
                "service root ownership, permissions, or type are invalid",
            )?;
            sync_service_root_parent(configured_root)?;
        }
        Err(_) => {
            return Err(service_error(
                CoreRuntimeErrorKind::Unavailable,
                "service root validation failed",
            ));
        }
    }
    let canonical = fs::canonicalize(configured_root).map_err(|_| {
        service_error(
            CoreRuntimeErrorKind::Unavailable,
            "service root canonicalization failed",
        )
    })?;
    let metadata = fs::symlink_metadata(&canonical).map_err(|_| {
        service_error(
            CoreRuntimeErrorKind::Unavailable,
            "service root validation failed",
        )
    })?;
    validate_service_directory_metadata(
        &metadata,
        "service root ownership, permissions, or type are invalid",
    )?;
    Ok(canonical)
}

fn validate_service_root_parent(service_owned_root: &Path) -> Result<(), CoreRuntimeError> {
    let parent = service_owned_root.parent().ok_or_else(|| {
        service_error(
            CoreRuntimeErrorKind::Unavailable,
            "service root parent is unavailable",
        )
    })?;
    let metadata = fs::symlink_metadata(parent).map_err(|_| {
        service_error(
            CoreRuntimeErrorKind::Unavailable,
            "service root parent validation failed",
        )
    })?;
    if !metadata.file_type().is_dir() {
        return Err(service_error(
            CoreRuntimeErrorKind::Unavailable,
            "service root parent type is invalid",
        ));
    }
    #[cfg(unix)]
    if metadata.uid() != nix::unistd::geteuid().as_raw()
        || metadata.permissions().mode() & 0o022 != 0
    {
        return Err(service_error(
            CoreRuntimeErrorKind::Unavailable,
            "service root parent ownership or permissions are invalid",
        ));
    }
    Ok(())
}

fn sync_service_root_parent(service_owned_root: &Path) -> Result<(), CoreRuntimeError> {
    let parent = service_owned_root.parent().ok_or_else(|| {
        service_error(
            CoreRuntimeErrorKind::Unavailable,
            "service root parent is unavailable",
        )
    })?;
    let path_metadata = fs::symlink_metadata(parent).map_err(|_| {
        service_error(
            CoreRuntimeErrorKind::Unavailable,
            "service root parent validation failed",
        )
    })?;
    if !path_metadata.file_type().is_dir() {
        return Err(service_error(
            CoreRuntimeErrorKind::Unavailable,
            "service root parent type is invalid",
        ));
    }
    #[cfg(unix)]
    if path_metadata.uid() != nix::unistd::geteuid().as_raw() {
        return Err(service_error(
            CoreRuntimeErrorKind::Unavailable,
            "service root parent ownership is invalid",
        ));
    }
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    options.custom_flags(nix::libc::O_CLOEXEC | nix::libc::O_DIRECTORY | nix::libc::O_NOFOLLOW);
    let directory = options.open(parent).map_err(|_| {
        service_error(
            CoreRuntimeErrorKind::Unavailable,
            "service root parent sync open failed",
        )
    })?;
    let opened_metadata = directory.metadata().map_err(|_| {
        service_error(
            CoreRuntimeErrorKind::Unavailable,
            "service root parent validation failed",
        )
    })?;
    if !opened_metadata.file_type().is_dir() {
        return Err(service_error(
            CoreRuntimeErrorKind::Unavailable,
            "service root parent type is invalid",
        ));
    }
    #[cfg(unix)]
    if path_metadata.dev() != opened_metadata.dev()
        || path_metadata.ino() != opened_metadata.ino()
        || opened_metadata.uid() != nix::unistd::geteuid().as_raw()
        || path_metadata.permissions().mode() & 0o022 != 0
        || opened_metadata.permissions().mode() & 0o022 != 0
    {
        return Err(service_error(
            CoreRuntimeErrorKind::Unavailable,
            "service root parent identity changed",
        ));
    }
    directory.sync_all().map_err(|_| {
        service_error(
            CoreRuntimeErrorKind::Unavailable,
            "service root parent sync failed",
        )
    })
}

fn validate_control_root(control_root: &Path) -> Result<(), CoreRuntimeError> {
    let metadata = fs::symlink_metadata(control_root).map_err(|_| {
        service_error(
            CoreRuntimeErrorKind::Unavailable,
            "control root validation failed",
        )
    })?;
    validate_control_root_metadata(&metadata)
}

fn validate_control_root_metadata(metadata: &fs::Metadata) -> Result<(), CoreRuntimeError> {
    validate_service_directory_metadata(
        metadata,
        "control root ownership, permissions, or type are invalid",
    )
}

fn validate_service_directory_metadata(
    metadata: &fs::Metadata,
    diagnostic: &'static str,
) -> Result<(), CoreRuntimeError> {
    if !metadata.file_type().is_dir() {
        return Err(service_error(CoreRuntimeErrorKind::Unavailable, diagnostic));
    }
    #[cfg(unix)]
    if metadata.uid() != nix::unistd::geteuid().as_raw()
        || metadata.permissions().mode() & 0o7777 != SERVICE_DIRECTORY_MODE
    {
        return Err(service_error(CoreRuntimeErrorKind::Unavailable, diagnostic));
    }
    Ok(())
}

fn load_or_initialize_generation_state(
    control_root: &Path,
) -> Result<ServiceGenerationStateV1, CoreRuntimeError> {
    validate_control_root(control_root)?;
    let state_path = control_root.join(SERVICE_GENERATION_STATE_FILE);
    match fs::symlink_metadata(&state_path) {
        Ok(metadata) => read_generation_state(&state_path, &metadata),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            let state = ServiceGenerationStateV1::initial();
            persist_generation_state(control_root, state)?;
            Ok(state)
        }
        Err(_) => Err(service_error(
            CoreRuntimeErrorKind::Unavailable,
            "service generation state validation failed",
        )),
    }
}

fn load_generation_state(
    control_root: &Path,
) -> Result<ServiceGenerationStateV1, CoreRuntimeError> {
    validate_control_root(control_root)?;
    let state_path = control_root.join(SERVICE_GENERATION_STATE_FILE);
    let metadata = fs::symlink_metadata(&state_path).map_err(|_| {
        service_error(
            CoreRuntimeErrorKind::Unavailable,
            "service generation state validation failed",
        )
    })?;
    read_generation_state(&state_path, &metadata)
}

#[cfg(unix)]
fn with_generation_state_lock<T>(
    control_root: &Path,
    operation: impl FnOnce() -> Result<T, CoreRuntimeError>,
) -> Result<T, CoreRuntimeError> {
    let file = open_generation_state_lock(control_root)?;
    let _lock = nix::fcntl::Flock::lock(file, nix::fcntl::FlockArg::LockExclusiveNonblock)
        .map_err(|_| {
            service_error(
                CoreRuntimeErrorKind::Unavailable,
                "service generation state lock is unavailable",
            )
        })?;
    operation()
}

#[cfg(not(unix))]
fn with_generation_state_lock<T>(
    _control_root: &Path,
    operation: impl FnOnce() -> Result<T, CoreRuntimeError>,
) -> Result<T, CoreRuntimeError> {
    operation()
}

#[cfg(unix)]
fn open_generation_state_lock(control_root: &Path) -> Result<fs::File, CoreRuntimeError> {
    validate_control_root(control_root)?;
    let lock_path = control_root.join(SERVICE_GENERATION_LOCK_FILE);
    for _ in 0..2 {
        match fs::symlink_metadata(&lock_path) {
            Ok(path_metadata) => {
                validate_generation_lock_metadata(&path_metadata)?;
                let mut options = OpenOptions::new();
                options
                    .read(true)
                    .write(true)
                    .custom_flags(nix::libc::O_CLOEXEC | nix::libc::O_NOFOLLOW);
                let file = options.open(&lock_path).map_err(|_| {
                    service_error(
                        CoreRuntimeErrorKind::Unavailable,
                        "service generation state lock open failed",
                    )
                })?;
                let opened_metadata = file.metadata().map_err(|_| {
                    service_error(
                        CoreRuntimeErrorKind::Unavailable,
                        "service generation state lock validation failed",
                    )
                })?;
                validate_generation_lock_metadata(&opened_metadata)?;
                if path_metadata.dev() != opened_metadata.dev()
                    || path_metadata.ino() != opened_metadata.ino()
                {
                    return Err(service_error(
                        CoreRuntimeErrorKind::Unavailable,
                        "service generation state lock identity changed",
                    ));
                }
                return Ok(file);
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                let mut options = OpenOptions::new();
                options
                    .read(true)
                    .write(true)
                    .create_new(true)
                    .mode(SERVICE_GENERATION_STATE_MODE)
                    .custom_flags(nix::libc::O_CLOEXEC | nix::libc::O_NOFOLLOW);
                match options.open(&lock_path) {
                    Ok(file) => {
                        file.set_permissions(fs::Permissions::from_mode(
                            SERVICE_GENERATION_STATE_MODE,
                        ))
                        .map_err(|_| {
                            service_error(
                                CoreRuntimeErrorKind::Unavailable,
                                "service generation state lock permission update failed",
                            )
                        })?;
                        validate_generation_lock_metadata(&file.metadata().map_err(|_| {
                            service_error(
                                CoreRuntimeErrorKind::Unavailable,
                                "service generation state lock validation failed",
                            )
                        })?)?;
                        file.sync_all().map_err(|_| {
                            service_error(
                                CoreRuntimeErrorKind::Unavailable,
                                "service generation state lock sync failed",
                            )
                        })?;
                        sync_private_service_directory(control_root)?;
                        return Ok(file);
                    }
                    Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
                    Err(_) => {
                        return Err(service_error(
                            CoreRuntimeErrorKind::Unavailable,
                            "service generation state lock creation failed",
                        ));
                    }
                }
            }
            Err(_) => {
                return Err(service_error(
                    CoreRuntimeErrorKind::Unavailable,
                    "service generation state lock validation failed",
                ));
            }
        }
    }
    Err(service_error(
        CoreRuntimeErrorKind::Unavailable,
        "service generation state lock creation raced",
    ))
}

#[cfg(unix)]
fn validate_generation_lock_metadata(metadata: &fs::Metadata) -> Result<(), CoreRuntimeError> {
    if !metadata.file_type().is_file()
        || metadata.len() != 0
        || metadata.uid() != nix::unistd::geteuid().as_raw()
        || metadata.permissions().mode() & 0o7777 != SERVICE_GENERATION_STATE_MODE
        || metadata.nlink() != 1
    {
        return Err(service_error(
            CoreRuntimeErrorKind::Unavailable,
            "service generation state lock ownership, permissions, or type are invalid",
        ));
    }
    Ok(())
}

fn cleanup_pending_generation_states(control_root: &Path) -> Result<(), CoreRuntimeError> {
    validate_control_root(control_root)?;
    let entries = fs::read_dir(control_root).map_err(|_| {
        service_error(
            CoreRuntimeErrorKind::Unavailable,
            "control root cleanup scan failed",
        )
    })?;
    let mut removed = false;
    for entry in entries {
        let entry = entry.map_err(|_| {
            service_error(
                CoreRuntimeErrorKind::Unavailable,
                "control root cleanup scan failed",
            )
        })?;
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        if !is_pending_generation_state_name(&name) {
            continue;
        }
        let path = entry.path();
        let metadata = fs::symlink_metadata(&path).map_err(|_| {
            service_error(
                CoreRuntimeErrorKind::Unavailable,
                "pending generation state validation failed",
            )
        })?;
        if !is_private_pending_generation_state(&metadata) {
            continue;
        }
        fs::remove_file(path).map_err(|_| {
            service_error(
                CoreRuntimeErrorKind::Unavailable,
                "pending generation state cleanup failed",
            )
        })?;
        removed = true;
    }
    if removed {
        sync_private_service_directory(control_root)?;
    }
    Ok(())
}

fn is_pending_generation_state_name(name: &str) -> bool {
    let prefix = format!(".{SERVICE_GENERATION_STATE_FILE}.");
    let Some(identifier) = name
        .strip_prefix(&prefix)
        .and_then(|value| value.strip_suffix(".pending"))
    else {
        return false;
    };
    uuid::Uuid::parse_str(identifier)
        .is_ok_and(|parsed| parsed.hyphenated().to_string() == identifier)
}

fn is_private_pending_generation_state(metadata: &fs::Metadata) -> bool {
    if !metadata.file_type().is_file() || metadata.len() > SERVICE_GENERATION_STATE_MAX_BYTES as u64
    {
        return false;
    }
    #[cfg(unix)]
    if metadata.uid() != nix::unistd::geteuid().as_raw()
        || metadata.permissions().mode() & 0o7777 & !SERVICE_GENERATION_STATE_MODE != 0
        || metadata.nlink() != 1
    {
        return false;
    }
    true
}

fn read_generation_state(
    state_path: &Path,
    path_metadata: &fs::Metadata,
) -> Result<ServiceGenerationStateV1, CoreRuntimeError> {
    validate_generation_state_metadata(path_metadata)?;
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    options.custom_flags(nix::libc::O_CLOEXEC | nix::libc::O_NOFOLLOW);
    let mut file = options.open(state_path).map_err(|_| {
        service_error(
            CoreRuntimeErrorKind::Unavailable,
            "service generation state open failed",
        )
    })?;
    let opened_metadata = file.metadata().map_err(|_| {
        service_error(
            CoreRuntimeErrorKind::Unavailable,
            "service generation state validation failed",
        )
    })?;
    validate_generation_state_metadata(&opened_metadata)?;
    #[cfg(unix)]
    if path_metadata.dev() != opened_metadata.dev() || path_metadata.ino() != opened_metadata.ino()
    {
        return Err(service_error(
            CoreRuntimeErrorKind::Unavailable,
            "service generation state identity changed",
        ));
    }

    let mut bytes = Vec::with_capacity(opened_metadata.len() as usize);
    (&mut file)
        .take((SERVICE_GENERATION_STATE_MAX_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|_| {
            service_error(
                CoreRuntimeErrorKind::Unavailable,
                "service generation state read failed",
            )
        })?;
    if bytes.is_empty() || bytes.len() > SERVICE_GENERATION_STATE_MAX_BYTES {
        return Err(service_error(
            CoreRuntimeErrorKind::Unavailable,
            "service generation state size is invalid",
        ));
    }
    let state: ServiceGenerationStateV1 = serde_json::from_slice(&bytes).map_err(|_| {
        service_error(
            CoreRuntimeErrorKind::Unavailable,
            "service generation state is invalid",
        )
    })?;
    if state.schema_version != SERVICE_GENERATION_STATE_SCHEMA_VERSION {
        return Err(service_error(
            CoreRuntimeErrorKind::Unavailable,
            "service generation state version is unsupported",
        ));
    }
    Ok(state)
}

fn validate_generation_state_metadata(metadata: &fs::Metadata) -> Result<(), CoreRuntimeError> {
    if !metadata.file_type().is_file()
        || metadata.len() == 0
        || metadata.len() > SERVICE_GENERATION_STATE_MAX_BYTES as u64
    {
        return Err(service_error(
            CoreRuntimeErrorKind::Unavailable,
            "service generation state shape is invalid",
        ));
    }
    #[cfg(unix)]
    if metadata.uid() != nix::unistd::geteuid().as_raw()
        || metadata.permissions().mode() & 0o7777 != SERVICE_GENERATION_STATE_MODE
        || metadata.nlink() != 1
    {
        return Err(service_error(
            CoreRuntimeErrorKind::Unavailable,
            "service generation state ownership or permissions are invalid",
        ));
    }
    Ok(())
}

fn persist_generation_state(
    control_root: &Path,
    state: ServiceGenerationStateV1,
) -> Result<(), CoreRuntimeError> {
    persist_generation_state_with_fault(control_root, state, None)
}

fn persist_generation_state_with_fault(
    control_root: &Path,
    state: ServiceGenerationStateV1,
    fault: Option<ServiceGenerationStateCommitFault>,
) -> Result<(), CoreRuntimeError> {
    validate_control_root(control_root)?;
    let bytes = serde_json::to_vec(&state).map_err(|_| {
        service_error(
            CoreRuntimeErrorKind::Unavailable,
            "service generation state serialization failed",
        )
    })?;
    if bytes.is_empty() || bytes.len() > SERVICE_GENERATION_STATE_MAX_BYTES {
        return Err(service_error(
            CoreRuntimeErrorKind::Unavailable,
            "service generation state serialization is invalid",
        ));
    }

    let state_path = control_root.join(SERVICE_GENERATION_STATE_FILE);
    match fs::symlink_metadata(&state_path) {
        Ok(metadata) => validate_generation_state_metadata(&metadata)?,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(_) => {
            return Err(service_error(
                CoreRuntimeErrorKind::Unavailable,
                "service generation state validation failed",
            ));
        }
    }

    let temporary_path = control_root.join(format!(
        ".{SERVICE_GENERATION_STATE_FILE}.{}.pending",
        uuid::Uuid::new_v4()
    ));
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    options
        .mode(SERVICE_GENERATION_STATE_MODE)
        .custom_flags(nix::libc::O_CLOEXEC | nix::libc::O_NOFOLLOW);
    let mut file = options.open(&temporary_path).map_err(|_| {
        service_error(
            CoreRuntimeErrorKind::Unavailable,
            "service generation state staging failed",
        )
    })?;
    let staged_identity = pending_file_identity(&file.metadata().map_err(|_| {
        service_error(
            CoreRuntimeErrorKind::Unavailable,
            "service generation state staging validation failed",
        )
    })?);
    let result = (|| {
        #[cfg(unix)]
        file.set_permissions(fs::Permissions::from_mode(SERVICE_GENERATION_STATE_MODE))
            .map_err(|_| {
                service_error(
                    CoreRuntimeErrorKind::Unavailable,
                    "service generation state staging permission update failed",
                )
            })?;
        inject_generation_state_commit_fault(fault, ServiceGenerationStateCommitFault::Write)?;
        file.write_all(&bytes).map_err(|_| {
            service_error(
                CoreRuntimeErrorKind::Unavailable,
                "service generation state write failed",
            )
        })?;
        inject_generation_state_commit_fault(fault, ServiceGenerationStateCommitFault::FileSync)?;
        file.sync_all().map_err(|_| {
            service_error(
                CoreRuntimeErrorKind::Unavailable,
                "service generation state sync failed",
            )
        })?;
        inject_generation_state_commit_fault(fault, ServiceGenerationStateCommitFault::Rename)?;
        fs::rename(&temporary_path, &state_path).map_err(|_| {
            service_error(
                CoreRuntimeErrorKind::Unavailable,
                "service generation state commit failed",
            )
        })?;
        inject_generation_state_commit_fault(
            fault,
            ServiceGenerationStateCommitFault::DirectorySync,
        )?;
        sync_private_service_directory(control_root)
    })();
    if result.is_err() {
        remove_created_pending_generation_state(&temporary_path, staged_identity);
    }
    result
}

fn inject_generation_state_commit_fault(
    armed: Option<ServiceGenerationStateCommitFault>,
    stage: ServiceGenerationStateCommitFault,
) -> Result<(), CoreRuntimeError> {
    if armed != Some(stage) {
        return Ok(());
    }
    let diagnostic = match stage {
        ServiceGenerationStateCommitFault::Write => {
            "injected service generation state write failure"
        }
        ServiceGenerationStateCommitFault::FileSync => {
            "injected service generation state file sync failure"
        }
        ServiceGenerationStateCommitFault::Rename => {
            "injected service generation state rename failure"
        }
        ServiceGenerationStateCommitFault::DirectorySync => {
            "injected service generation state directory sync failure"
        }
    };
    Err(service_error(CoreRuntimeErrorKind::Unavailable, diagnostic))
}

#[cfg(unix)]
#[derive(Clone, Copy)]
struct PendingFileIdentity {
    device: u64,
    inode: u64,
}

#[cfg(not(unix))]
#[derive(Clone, Copy)]
struct PendingFileIdentity;

fn pending_file_identity(metadata: &fs::Metadata) -> PendingFileIdentity {
    #[cfg(unix)]
    {
        PendingFileIdentity {
            device: metadata.dev(),
            inode: metadata.ino(),
        }
    }
    #[cfg(not(unix))]
    {
        let _ = metadata;
        PendingFileIdentity
    }
}

fn remove_created_pending_generation_state(path: &Path, identity: PendingFileIdentity) {
    let Ok(metadata) = fs::symlink_metadata(path) else {
        return;
    };
    #[cfg(unix)]
    let matches = metadata.file_type().is_file()
        && metadata.dev() == identity.device
        && metadata.ino() == identity.inode;
    #[cfg(not(unix))]
    let matches = false;
    if matches {
        let _ = fs::remove_file(path);
    }
}

fn sync_private_service_directory(directory_path: &Path) -> Result<(), CoreRuntimeError> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    options.custom_flags(nix::libc::O_CLOEXEC | nix::libc::O_DIRECTORY | nix::libc::O_NOFOLLOW);
    let directory = options.open(directory_path).map_err(|_| {
        service_error(
            CoreRuntimeErrorKind::Unavailable,
            "service directory sync open failed",
        )
    })?;
    validate_service_directory_metadata(
        &directory.metadata().map_err(|_| {
            service_error(
                CoreRuntimeErrorKind::Unavailable,
                "service directory validation failed",
            )
        })?,
        "service directory ownership, permissions, or type are invalid",
    )?;
    directory.sync_all().map_err(|_| {
        service_error(
            CoreRuntimeErrorKind::Unavailable,
            "service directory sync failed",
        )
    })
}

fn read_bounded_regular(
    root: &Path,
    path: &Path,
    max_bytes: usize,
) -> Result<Vec<u8>, CoreRuntimeError> {
    let metadata =
        fs::symlink_metadata(path).map_err(|_| invalid_bundle("runtime file is unavailable"))?;
    if !metadata.file_type().is_file() || metadata.len() > max_bytes as u64 {
        return Err(invalid_bundle("runtime file shape or size is invalid"));
    }
    let canonical = fs::canonicalize(path)
        .map_err(|_| invalid_bundle("runtime file canonicalization failed"))?;
    if !canonical.starts_with(root) {
        return Err(invalid_bundle("runtime file escaped generation root"));
    }
    let bytes = fs::read(canonical).map_err(|_| invalid_bundle("runtime file read failed"))?;
    if bytes.len() > max_bytes {
        return Err(invalid_bundle("runtime file exceeded its size limit"));
    }
    Ok(bytes)
}

fn read_bounded_executable(
    root: &Path,
    path: &Path,
    max_bytes: usize,
) -> Result<Vec<u8>, CoreRuntimeError> {
    let metadata =
        fs::symlink_metadata(path).map_err(|_| invalid_bundle("runtime file is unavailable"))?;
    #[cfg(unix)]
    if metadata.permissions().mode() & 0o111 == 0 {
        return Err(invalid_bundle("Mihomo binary is not executable"));
    }
    read_bounded_regular(root, path, max_bytes)
}

fn truncate_utf8(mut value: String, max_bytes: usize) -> String {
    if value.len() <= max_bytes {
        return value;
    }
    let mut boundary = max_bytes;
    while !value.is_char_boundary(boundary) {
        boundary -= 1;
    }
    value.truncate(boundary);
    value
}

fn next_sequence(sequence: &mut u64) -> Option<u64> {
    let current = *sequence;
    *sequence = sequence.checked_add(1)?;
    Some(current)
}

fn valid_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
}

fn invalid_bundle(message: &'static str) -> CoreRuntimeError {
    service_error(CoreRuntimeErrorKind::InvalidBundle, message)
}

fn map_spawn_error(_error: ServicePlatformError) -> CoreRuntimeError {
    service_error(CoreRuntimeErrorKind::Apply, "Core spawn failed")
}

fn map_reload_error(error: ServicePlatformError) -> CoreRuntimeError {
    match error.kind {
        ServicePlatformErrorKind::ReloadTimeout => {
            service_error(CoreRuntimeErrorKind::ReloadTimeout, "Core reload timed out")
        }
        _ => service_error(CoreRuntimeErrorKind::Apply, "Core reload failed"),
    }
}

fn map_readiness_error(_error: ServicePlatformError) -> CoreRuntimeError {
    service_error(
        CoreRuntimeErrorKind::Readiness,
        "Core readiness confirmation failed",
    )
}

fn map_stop_error(_error: ServicePlatformError) -> CoreRuntimeError {
    service_error(CoreRuntimeErrorKind::Apply, "Core stop failed")
}

fn service_error(kind: CoreRuntimeErrorKind, message: &'static str) -> CoreRuntimeError {
    CoreRuntimeError::new(kind, message)
}
