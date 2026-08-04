//! Privileged CoreRuntime service state machine.

mod bundle;
mod error;
mod generation_state;
mod platform;

pub use bundle::{
    RuntimeConfigurationPolicy, RuntimeManifestFileV1, RuntimeManifestV1, VerifiedRuntimeBundle,
};
pub use error::{ServicePlatformError, ServicePlatformErrorKind};
pub use generation_state::ServiceGenerationStateCommitFault;
pub use platform::{
    CallerCredentialValidator, CoreProcessController, CoreProcessLog, CoreProcessLogBatch,
    OwnedProcessIdentity, PrivilegedServiceDependencies, ProcessIdentityProbe, SecretGenerator,
    SpawnedCoreProcess, TunCapabilityPreflight, UuidSecretGenerator,
};

use std::collections::VecDeque;
use std::fs;
use std::io;
#[cfg(unix)]
use std::os::unix::fs::{FileTypeExt, MetadataExt};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::Duration;

use crate::constants::{
    CORE_LOG_FORWARD_BATCH_MAX_BYTES, CORE_LOG_FORWARD_CAPACITY, CORE_LOG_FORWARD_MAX_BYTES,
    CORE_LOG_LINE_MAX_BYTES, CORE_RESTART_INITIAL_BACKOFF, CORE_RESTART_LIMIT,
    CORE_RESTART_MAX_BACKOFF, CORE_SERVICE_LIVENESS_INTERVAL,
};
use crate::core::{
    ApplyCandidateResult, ApplyDisposition, CoreControlEndpoint, CoreRuntime,
    CoreRuntimeDiagnosticCategory, CoreRuntimeError, CoreRuntimeErrorKind, CoreRuntimeLifecycle,
    CoreRuntimeRestartStatus, CoreRuntimeStatus, CoreRuntimeTunReason, CoreRuntimeTunStatus,
    ForwardedCoreLog, ForwardedCoreLogBatch, ManagedCoreHandle, OwnerSession, OwnerSessionProof,
    OwnerSessionRequest, RuntimeBundle, StopCoreResult,
};
use crate::digest::is_lower_sha256_hex;
use crate::domain::CoreInstanceGeneration;

use bundle::verify_runtime_bundle;
use error::{
    map_readiness_error, map_spawn_error, map_stop_error, map_tun_preflight_error, service_error,
};
use generation_state::{
    ServiceGenerationStateV1, cleanup_pending_generation_states, load_generation_state,
    load_or_initialize_generation_state, persist_generation_state_with_fault, prepare_control_root,
    prepare_service_owned_root, with_generation_state_lock,
};

pub const CORE_RUNTIME_PROTOCOL_VERSION: u16 = 1;

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
            log_capacity: CORE_LOG_FORWARD_CAPACITY,
            max_log_line_bytes: CORE_LOG_LINE_MAX_BYTES,
        }
    }
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
enum OwnedCoreState {
    Active(ManagedCoreRecord),
    CleanupPending(ManagedCoreRecord),
}

impl OwnedCoreState {
    fn active(&self) -> Option<&ManagedCoreRecord> {
        match self {
            Self::Active(record) => Some(record),
            Self::CleanupPending(_) => None,
        }
    }

    fn record(&self) -> &ManagedCoreRecord {
        match self {
            Self::Active(record) | Self::CleanupPending(record) => record,
        }
    }

    fn record_mut(&mut self) -> &mut ManagedCoreRecord {
        match self {
            Self::Active(record) | Self::CleanupPending(record) => record,
        }
    }

    fn is_cleanup_pending(&self) -> bool {
        matches!(self, Self::CleanupPending(_))
    }
}

#[derive(Clone)]
struct OwnedEndpointIdentity {
    device: u64,
    inode: u64,
    change_time_seconds: i64,
    change_time_nanoseconds: i64,
}

struct ServiceState {
    owner_generation: u64,
    owner: Option<ServiceOwner>,
    core_instance_generation: u64,
    managed_core: Option<OwnedCoreState>,
    last_bundle: Option<RuntimeBundle>,
    degraded: bool,
    consecutive_restart_failures: usize,
    diagnostic: Option<CoreRuntimeDiagnosticCategory>,
    restart_due_at: Option<Duration>,
    restart_backoff: Option<Duration>,
    next_liveness_at: Option<Duration>,
    logs: VecDeque<ForwardedCoreLog>,
    log_bytes: usize,
    next_log_sequence: u64,
    dropped_log_sequence: u64,
}

struct ApplyCancellationState {
    owner: Option<OwnerSessionProof>,
    owner_generation: Option<u64>,
    requested: bool,
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
    apply_cancellation: Mutex<ApplyCancellationState>,
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
            || !is_lower_sha256_hex(&config.compiler_policy_sha256)
            || !is_lower_sha256_hex(&config.mihomo_binary_sha256)
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
            apply_cancellation: Mutex::new(ApplyCancellationState {
                owner: None,
                owner_generation: None,
                requested: false,
            }),
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
                log_bytes: 0,
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
                .and_then(OwnedCoreState::active)
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

        if (restart_is_due || liveness_is_due)
            && state
                .managed_core
                .as_ref()
                .is_some_and(OwnedCoreState::is_cleanup_pending)
        {
            self.cleanup_pending_core(&mut state)?;
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
            && let Some(OwnedCoreState::Active(record)) = state.managed_core.clone()
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
        let record = match state.managed_core.clone() {
            Some(OwnedCoreState::Active(record)) => record,
            Some(OwnedCoreState::CleanupPending(_)) => {
                return Err(service_error(
                    CoreRuntimeErrorKind::Unavailable,
                    "uncommitted Core cleanup is pending",
                ));
            }
            None => {
                return Err(service_error(
                    CoreRuntimeErrorKind::ProcessIdentityMismatch,
                    "unexpected exit has no owned Core",
                ));
            }
        };
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

    fn reset_apply_cancellation(
        &self,
        owner: &OwnerSessionProof,
        owner_generation: u64,
    ) -> Result<(), CoreRuntimeError> {
        let mut cancellation = self.apply_cancellation.lock().map_err(|_| {
            service_error(
                CoreRuntimeErrorKind::Unavailable,
                "Runtime Apply cancellation state is unavailable",
            )
        })?;
        cancellation.owner = Some(owner.clone());
        cancellation.owner_generation = Some(owner_generation);
        cancellation.requested = false;
        drop(cancellation);
        self.dependencies
            .processes
            .reset_apply_cancellation(owner_generation);
        Ok(())
    }

    fn clear_apply_cancellation(&self, owner: &ServiceOwner) -> Result<(), CoreRuntimeError> {
        let mut cancellation = self.apply_cancellation.lock().map_err(|_| {
            service_error(
                CoreRuntimeErrorKind::Unavailable,
                "Runtime Apply cancellation state is unavailable",
            )
        })?;
        if cancellation.owner.as_ref() != Some(&owner.proof)
            || cancellation.owner_generation != Some(owner.generation)
        {
            return Err(service_error(
                CoreRuntimeErrorKind::Authentication,
                "Runtime Apply cancellation owner mismatch",
            ));
        }
        cancellation.owner = None;
        cancellation.owner_generation = None;
        cancellation.requested = true;
        Ok(())
    }

    fn ensure_apply_active(&self, owner: &OwnerSessionProof) -> Result<(), CoreRuntimeError> {
        self.active_apply_guard(owner).map(drop)
    }

    fn active_apply_guard(
        &self,
        owner: &OwnerSessionProof,
    ) -> Result<std::sync::MutexGuard<'_, ApplyCancellationState>, CoreRuntimeError> {
        let cancellation = self.apply_cancellation.lock().map_err(|_| {
            service_error(
                CoreRuntimeErrorKind::Unavailable,
                "Runtime Apply cancellation state is unavailable",
            )
        })?;
        if cancellation.owner.as_ref() != Some(owner) {
            return Err(service_error(
                CoreRuntimeErrorKind::Authentication,
                "Runtime Apply cancellation owner mismatch",
            ));
        }
        if cancellation.requested {
            Err(service_error(
                CoreRuntimeErrorKind::ReloadTimeout,
                "Runtime Apply was cancelled during Supervisor shutdown",
            ))
        } else {
            Ok(cancellation)
        }
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
            ServiceGenerationStateV1::new(state.owner_generation, state.core_instance_generation),
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
        if let Some(core) = state.managed_core.clone() {
            self.stop_owned_record(state, core.record().clone())?;
        }
        if let Some(owner) = state.owner.as_ref() {
            self.clear_apply_cancellation(owner)?;
        }
        state.managed_core = None;
        state.owner = None;
        state.last_bundle = None;
        reset_restart_state(state);
        state.next_liveness_at = None;
        state.logs.clear();
        state.log_bytes = 0;
        state.next_log_sequence = 1;
        state.dropped_log_sequence = 0;
        Ok(())
    }

    fn stop_owned_record(
        &self,
        state: &mut ServiceState,
        mut record: ManagedCoreRecord,
    ) -> Result<(), CoreRuntimeError> {
        if record.endpoint_identity.is_none() {
            record.endpoint_identity =
                capture_endpoint_identity(&record.handle.endpoint.socket_path);
            if let Some(retained) = state.managed_core.as_mut()
                && retained.record().owned_identity == record.owned_identity
            {
                retained.record_mut().endpoint_identity = record.endpoint_identity.clone();
            }
        }
        if self.owned_process_is_live(&record.owned_identity)? {
            self.dependencies
                .processes
                .stop(&record.owned_identity)
                .map_err(map_stop_error)?;
        }
        self.drain_logs(state, &record)?;
        self.remove_owned_endpoint(&record)
    }

    fn cleanup_pending_core(&self, state: &mut ServiceState) -> Result<(), CoreRuntimeError> {
        let Some(OwnedCoreState::CleanupPending(record)) = state.managed_core.clone() else {
            return Ok(());
        };
        self.stop_owned_record(state, record)?;
        state.managed_core = None;
        Ok(())
    }

    fn fail_after_uncommitted_spawn(
        &self,
        state: &mut ServiceState,
        record: ManagedCoreRecord,
        primary: CoreRuntimeError,
    ) -> CoreRuntimeError {
        state.managed_core = Some(OwnedCoreState::CleanupPending(record));
        match self.cleanup_pending_core(state) {
            Ok(()) => primary,
            Err(_) => service_error(
                CoreRuntimeErrorKind::Unavailable,
                "uncommitted Core cleanup is pending",
            ),
        }
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

        let restarted = self
            .verify_bundle(&bundle, &owner.endpoint)
            .and_then(|verified| {
                self.dependencies
                    .tun
                    .check(owner.owner_uid)
                    .map_err(map_tun_preflight_error)?;
                self.spawn_verified(state, owner, &verified)
            });
        if let Err(error) = &restarted
            && (error.kind == CoreRuntimeErrorKind::InvalidBundle
                || state
                    .managed_core
                    .as_ref()
                    .is_some_and(OwnedCoreState::is_cleanup_pending))
        {
            return Err(error.clone());
        }
        if let Ok(record) = restarted {
            let managed_core = record.handle.clone();
            state.managed_core = Some(OwnedCoreState::Active(record));
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
        endpoint: &CoreControlEndpoint,
    ) -> Result<VerifiedRuntimeBundle, CoreRuntimeError> {
        verify_runtime_bundle(
            &self.service_owned_root,
            &self.compiler_policy_sha256,
            &self.mihomo_binary_sha256,
            self.dependencies.configuration_policy.as_ref(),
            bundle,
            endpoint,
        )
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
        let mut record = ManagedCoreRecord {
            handle: ManagedCoreHandle {
                pid: spawned.pid,
                process_start_identity: spawned.process_start_identity,
                endpoint: owner.endpoint.clone(),
                instance_generation,
                runtime_generation: bundle.bundle().generation,
            },
            owned_identity,
            endpoint_identity: None,
        };
        if let Err(error) = self.ensure_apply_active(&owner.proof) {
            return Err(self.fail_after_uncommitted_spawn(state, record, error));
        }
        match self.owned_process_is_live(&record.owned_identity) {
            Ok(true) => {}
            Ok(false) => {
                let error = service_error(
                    CoreRuntimeErrorKind::ProcessIdentityMismatch,
                    "spawned Core identity could not be confirmed",
                );
                return Err(self.fail_after_uncommitted_spawn(state, record, error));
            }
            Err(error) => {
                return Err(self.fail_after_uncommitted_spawn(state, record, error));
            }
        }
        let readiness = self
            .dependencies
            .processes
            .readiness(&record.owned_identity, &owner.endpoint);
        record.endpoint_identity = capture_endpoint_identity(&owner.endpoint.socket_path);
        if let Err(error) = readiness {
            let error = map_readiness_error(error);
            return Err(self.fail_after_uncommitted_spawn(state, record, error));
        }
        if let Err(error) = self.ensure_apply_active(&owner.proof) {
            return Err(self.fail_after_uncommitted_spawn(state, record, error));
        }
        let endpoint_access = self
            .dependencies
            .processes
            .grant_endpoint_access(&owner.endpoint, owner.owner_uid);
        if endpoint_access.is_ok() {
            record.endpoint_identity = capture_endpoint_identity(&owner.endpoint.socket_path);
        }
        if endpoint_access.is_err() {
            let error = service_error(
                CoreRuntimeErrorKind::Apply,
                "Core control endpoint access setup failed",
            );
            return Err(self.fail_after_uncommitted_spawn(state, record, error));
        }
        if let Err(error) = self.ensure_apply_active(&owner.proof) {
            return Err(self.fail_after_uncommitted_spawn(state, record, error));
        }
        Ok(record)
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
            let message = truncate_utf8(
                log.message,
                self.max_log_line_bytes.min(CORE_LOG_FORWARD_MAX_BYTES),
            );
            while state.logs.len() == self.log_capacity
                || state.log_bytes.saturating_add(message.len()) > CORE_LOG_FORWARD_MAX_BYTES
            {
                let Some(dropped) = state.logs.pop_front() else {
                    break;
                };
                state.log_bytes = state.log_bytes.saturating_sub(dropped.message.len());
                state.dropped_log_sequence = state.dropped_log_sequence.max(dropped.sequence);
            }
            state.log_bytes = state.log_bytes.saturating_add(message.len());
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
    if state.degraded
        || state
            .managed_core
            .as_ref()
            .is_some_and(OwnedCoreState::is_cleanup_pending)
    {
        PrivilegedServiceLifecycle::Degraded
    } else if state
        .managed_core
        .as_ref()
        .is_some_and(|core| core.active().is_some())
    {
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
        change_time_seconds: metadata.ctime(),
        change_time_nanoseconds: metadata.ctime_nsec(),
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
        || metadata.ctime() != expected.change_time_seconds
        || metadata.ctime_nsec() != expected.change_time_nanoseconds
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
        self.reset_apply_cancellation(&proof, owner_generation)?;
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
        self.cleanup_pending_core(&mut state)?;
        self.ensure_apply_active(owner)?;
        let verified = self.verify_bundle(bundle, &authenticated.endpoint)?;
        self.ensure_apply_active(owner)?;
        self.dependencies
            .tun
            .check(authenticated.owner_uid)
            .map_err(map_tun_preflight_error)?;
        self.ensure_apply_active(owner)?;

        if let Some(OwnedCoreState::Active(record)) = state.managed_core.clone() {
            self.require_owned_process(&record)?;
            state.managed_core = Some(OwnedCoreState::CleanupPending(record));
            self.cleanup_pending_core(&mut state)?;
            self.ensure_apply_active(owner)?;
        }

        let record = self.spawn_verified(&mut state, &authenticated, &verified)?;
        let _active_apply = match self.active_apply_guard(owner) {
            Ok(active) => active,
            Err(error) => {
                return Err(self.fail_after_uncommitted_spawn(&mut state, record, error));
            }
        };
        let handle = record.handle.clone();
        state.managed_core = Some(OwnedCoreState::Active(record));
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
        let cleanup_pending = state
            .managed_core
            .as_ref()
            .is_some_and(OwnedCoreState::is_cleanup_pending);
        let (managed_core, observed_exit) = match state.managed_core.as_ref() {
            Some(OwnedCoreState::Active(record))
                if self.owned_process_is_live(&record.owned_identity)? =>
            {
                (Some(record.handle.clone()), false)
            }
            Some(OwnedCoreState::Active(_)) => (None, true),
            Some(OwnedCoreState::CleanupPending(_)) => (None, false),
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
            lifecycle: if state.degraded || cleanup_pending {
                CoreRuntimeLifecycle::Degraded
            } else if state.restart_due_at.is_some() || observed_exit {
                CoreRuntimeLifecycle::RestartPending
            } else if core_is_running {
                CoreRuntimeLifecycle::Running
            } else {
                CoreRuntimeLifecycle::Owned
            },
            restart: CoreRuntimeRestartStatus {
                pending: (state.restart_due_at.is_some() || observed_exit)
                    && !state.degraded
                    && !cleanup_pending,
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
        if let Some(OwnedCoreState::Active(record)) = state.managed_core.clone() {
            self.require_owned_process(&record)?;
            self.drain_logs(&mut state, &record)?;
        }
        let limit = limit.min(self.log_capacity);
        let mut records = Vec::with_capacity(limit);
        let mut message_bytes = 0_usize;
        for record in state
            .logs
            .iter()
            .filter(|record| after_sequence.is_none_or(|after| record.sequence > after))
            .take(limit)
        {
            if message_bytes.saturating_add(record.message.len()) > CORE_LOG_FORWARD_BATCH_MAX_BYTES
            {
                break;
            }
            message_bytes = message_bytes.saturating_add(record.message.len());
            records.push(record.clone());
        }
        let page_cursor = records
            .last()
            .map(|record| record.sequence)
            .or(after_sequence);
        let has_more_records = state
            .logs
            .iter()
            .any(|record| page_cursor.is_none_or(|cursor| record.sequence > cursor));
        let sequence_horizon = (state.next_log_sequence > 1).then(|| state.next_log_sequence - 1);
        let next_sequence = if has_more_records {
            page_cursor
        } else {
            match (page_cursor, sequence_horizon) {
                (Some(cursor), Some(horizon)) => Some(cursor.max(horizon)),
                (cursor, horizon) => cursor.or(horizon),
            }
        };
        let delivered = u64::try_from(records.len()).unwrap_or(u64::MAX);
        let dropped_since_after = next_sequence
            .unwrap_or(0)
            .saturating_sub(after_sequence.unwrap_or(0))
            .saturating_sub(delivered);
        Ok(ForwardedCoreLogBatch {
            records,
            next_sequence,
            dropped_before: state.dropped_log_sequence,
            dropped_since_after,
        })
    }

    fn stop(&self, owner: &OwnerSessionProof) -> Result<StopCoreResult, CoreRuntimeError> {
        let mut state = self.lock_state()?;
        authenticated_owner(&state, owner)?;
        let Some(core) = state.managed_core.clone() else {
            return Ok(StopCoreResult {
                stopped: false,
                instance_generation: None,
            });
        };
        let record = core.record().clone();
        if matches!(core, OwnedCoreState::Active(_)) {
            self.require_owned_process(&record)?;
        }
        let instance_generation = record.owned_identity.instance_generation;
        self.stop_owned_record(&mut state, record)?;
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

    fn cancel_pending_apply(&self, owner: &OwnerSessionProof) -> Result<(), CoreRuntimeError> {
        let mut cancellation = self.apply_cancellation.lock().map_err(|_| {
            service_error(
                CoreRuntimeErrorKind::Unavailable,
                "Runtime Apply cancellation state is unavailable",
            )
        })?;
        if cancellation.owner.as_ref() != Some(owner) {
            return Err(service_error(
                CoreRuntimeErrorKind::Authentication,
                "Runtime Apply cancellation owner mismatch",
            ));
        }
        let owner_generation = cancellation.owner_generation.ok_or_else(|| {
            service_error(
                CoreRuntimeErrorKind::Authentication,
                "Runtime Apply cancellation owner generation is unavailable",
            )
        })?;
        cancellation.requested = true;
        drop(cancellation);
        self.dependencies
            .processes
            .cancel_pending_apply(owner_generation);
        Ok(())
    }
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

fn truncate_utf8(mut value: String, max_bytes: usize) -> String {
    if value.len() <= max_bytes {
        return value.into_boxed_str().into_string();
    }
    let mut boundary = max_bytes;
    while !value.is_char_boundary(boundary) {
        boundary -= 1;
    }
    value.truncate(boundary);
    value.into_boxed_str().into_string()
}

fn next_sequence(sequence: &mut u64) -> Option<u64> {
    let current = *sequence;
    *sequence = sequence.checked_add(1)?;
    Some(current)
}
