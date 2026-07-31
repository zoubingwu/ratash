use crate::constants::{
    CORE_LOG_LINE_MAX_BYTES, CORE_RESTART_LIMIT, EFFECTIVE_CONFIGURATION_MAX_BYTES, LOG_CAPACITY,
    MIHOMO_BINARY_MAX_BYTES, PROFILE_RESPONSE_MAX_BYTES,
};
use crate::core::{
    ApplyCandidateResult, ApplyDisposition, CoreControlEndpoint, CoreRuntime, CoreRuntimeError,
    CoreRuntimeErrorKind, CoreRuntimeStatus, ForwardedCoreLog, ForwardedCoreLogBatch,
    ManagedCoreHandle, OwnerSession, OwnerSessionProof, OwnerSessionRequest, ProcessOutputSource,
    RuntimeBundle, StopCoreResult,
};
use crate::domain::{CoreInstanceGeneration, RuntimeGeneration};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::fmt;
use std::fs;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::{Component, Path, PathBuf};
use std::sync::Mutex;

pub const CORE_RUNTIME_PROTOCOL_VERSION: u16 = 1;
const RUNTIME_MANIFEST_SCHEMA_VERSION: u16 = 1;
const RUNTIME_MANIFEST_MAX_BYTES: usize = 64 * 1_024;
const RUNTIME_PROVIDER_FILE_MAX: usize = 1_024;

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
    ) -> Result<Vec<CoreProcessLog>, ServicePlatformError>;
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
    Restarted {
        attempts: usize,
        managed_core: ManagedCoreHandle,
    },
    Degraded {
        attempts: usize,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ServiceMaintenanceOutcome {
    Unchanged(PrivilegedServiceLifecycle),
    OwnerRevoked,
    UnexpectedExit(UnexpectedExitOutcome),
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
}

struct ServiceState {
    owner_generation: u64,
    owner: Option<ServiceOwner>,
    core_instance_generation: u64,
    managed_core: Option<ManagedCoreRecord>,
    last_bundle: Option<RuntimeBundle>,
    degraded: bool,
    consecutive_restart_failures: usize,
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
    compiler_policy_sha256: String,
    mihomo_binary_sha256: String,
    restart_limit: usize,
    log_capacity: usize,
    max_log_line_bytes: usize,
    dependencies: PrivilegedServiceDependencies,
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
        fs::create_dir_all(&config.service_owned_root).map_err(|_| {
            service_error(
                CoreRuntimeErrorKind::Unavailable,
                "service root creation failed",
            )
        })?;
        #[cfg(unix)]
        fs::set_permissions(
            &config.service_owned_root,
            fs::Permissions::from_mode(0o711),
        )
        .map_err(|_| {
            service_error(
                CoreRuntimeErrorKind::Unavailable,
                "service root permission update failed",
            )
        })?;
        let service_owned_root = fs::canonicalize(&config.service_owned_root).map_err(|_| {
            service_error(
                CoreRuntimeErrorKind::Unavailable,
                "service root canonicalization failed",
            )
        })?;
        let control_root = service_owned_root.join("control");
        fs::create_dir_all(&control_root).map_err(|_| {
            service_error(
                CoreRuntimeErrorKind::Unavailable,
                "control root creation failed",
            )
        })?;
        #[cfg(unix)]
        fs::set_permissions(&control_root, fs::Permissions::from_mode(0o711)).map_err(|_| {
            service_error(
                CoreRuntimeErrorKind::Unavailable,
                "control root permission update failed",
            )
        })?;

        Ok(Self {
            protocol_version: config.protocol_version,
            service_owned_root,
            compiler_policy_sha256: config.compiler_policy_sha256,
            mihomo_binary_sha256: config.mihomo_binary_sha256,
            restart_limit: config.restart_limit,
            log_capacity: config.log_capacity,
            max_log_line_bytes: config.max_log_line_bytes,
            dependencies,
            state: Mutex::new(ServiceState {
                owner_generation: 0,
                owner: None,
                core_instance_generation: 0,
                managed_core: None,
                last_bundle: None,
                degraded: false,
                consecutive_restart_failures: 0,
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
        let lifecycle = if state.degraded {
            PrivilegedServiceLifecycle::Degraded
        } else if state.managed_core.is_some() {
            PrivilegedServiceLifecycle::Running
        } else {
            PrivilegedServiceLifecycle::Owned
        };
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
            dropped_log_sequence: state.dropped_log_sequence,
        })
    }

    pub fn revoke_owner(&self, proof: &OwnerSessionProof) -> Result<(), CoreRuntimeError> {
        let mut state = self.lock_state()?;
        authenticated_owner(&state, proof)?;
        self.cleanup_owner(&mut state)
    }

    pub fn maintenance_tick(&self) -> Result<ServiceMaintenanceOutcome, CoreRuntimeError> {
        let mut state = self.lock_state()?;
        let Some(owner) = state.owner.clone() else {
            return Ok(ServiceMaintenanceOutcome::Unchanged(
                PrivilegedServiceLifecycle::Idle,
            ));
        };
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
            return Ok(ServiceMaintenanceOutcome::OwnerRevoked);
        }
        if state.degraded {
            return Ok(ServiceMaintenanceOutcome::Unchanged(
                PrivilegedServiceLifecycle::Degraded,
            ));
        }
        let Some(record) = state.managed_core.as_ref() else {
            return Ok(ServiceMaintenanceOutcome::Unchanged(
                PrivilegedServiceLifecycle::Owned,
            ));
        };
        if self.owned_process_is_live(&record.owned_identity)? {
            return Ok(ServiceMaintenanceOutcome::Unchanged(
                PrivilegedServiceLifecycle::Running,
            ));
        }
        let outcome = self.restart_owned_core(&mut state, &owner)?;
        Ok(ServiceMaintenanceOutcome::UnexpectedExit(outcome))
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
        let mut state = self.lock_state()?;
        let owner = authenticated_owner(&state, proof)?.clone();
        let record = state.managed_core.as_ref().ok_or_else(|| {
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
        self.restart_owned_core(&mut state, &owner)
    }

    fn lock_state(&self) -> Result<std::sync::MutexGuard<'_, ServiceState>, CoreRuntimeError> {
        self.state.lock().map_err(|_| {
            service_error(
                CoreRuntimeErrorKind::Unavailable,
                "service state lock unavailable",
            )
        })
    }

    fn cleanup_owner(&self, state: &mut ServiceState) -> Result<(), CoreRuntimeError> {
        if let Some(record) = state.managed_core.as_ref()
            && self.owned_process_is_live(&record.owned_identity)?
        {
            self.dependencies
                .processes
                .stop(&record.owned_identity)
                .map_err(map_stop_error)?;
        }
        state.managed_core = None;
        state.owner = None;
        state.last_bundle = None;
        state.degraded = false;
        state.consecutive_restart_failures = 0;
        state.logs.clear();
        state.dropped_log_sequence = state.next_log_sequence.saturating_sub(1);
        Ok(())
    }

    fn restart_owned_core(
        &self,
        state: &mut ServiceState,
        owner: &ServiceOwner,
    ) -> Result<UnexpectedExitOutcome, CoreRuntimeError> {
        let bundle = state.last_bundle.clone().ok_or_else(|| {
            service_error(
                CoreRuntimeErrorKind::Unavailable,
                "unexpected exit has no runtime bundle",
            )
        })?;
        state.managed_core = None;

        for attempt in 1..=self.restart_limit {
            state.consecutive_restart_failures = attempt;
            let verified = match self.verify_bundle(&bundle) {
                Ok(verified) => verified,
                Err(_) => continue,
            };
            if self.dependencies.tun.check(owner.owner_uid).is_err() {
                continue;
            }
            match self.spawn_verified(state, owner, &verified) {
                Ok(record) => {
                    let managed_core = record.handle.clone();
                    state.managed_core = Some(record);
                    state.degraded = false;
                    state.consecutive_restart_failures = 0;
                    return Ok(UnexpectedExitOutcome::Restarted {
                        attempts: attempt,
                        managed_core,
                    });
                }
                Err(_) => continue,
            }
        }

        state.degraded = true;
        Ok(UnexpectedExitOutcome::Degraded {
            attempts: self.restart_limit,
        })
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
        let generation = state
            .core_instance_generation
            .checked_add(1)
            .ok_or_else(|| {
                service_error(
                    CoreRuntimeErrorKind::Unavailable,
                    "Core Instance Generation exhausted",
                )
            })?;
        state.core_instance_generation = generation;
        let instance_generation = CoreInstanceGeneration(generation);
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
            let _ = self.dependencies.processes.stop(&owned_identity);
            return Err(map_readiness_error(error));
        }
        if self
            .dependencies
            .processes
            .grant_endpoint_access(&owner.endpoint, owner.owner_uid)
            .is_err()
        {
            let _ = self.dependencies.processes.stop(&owned_identity);
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
        })
    }

    fn drain_logs(
        &self,
        state: &mut ServiceState,
        record: &ManagedCoreRecord,
    ) -> Result<(), CoreRuntimeError> {
        self.require_owned_process(record)?;
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
        for log in incoming.into_iter().take(self.log_capacity) {
            let Some(sequence) = next_sequence(&mut state.next_log_sequence) else {
                break;
            };
            let message = truncate_utf8(log.message, self.max_log_line_bytes);
            if state.logs.len() == self.log_capacity
                && let Some(dropped) = state.logs.pop_front()
            {
                state.dropped_log_sequence = dropped.sequence;
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

        let owner_generation = state.owner_generation.checked_add(1).ok_or_else(|| {
            service_error(
                CoreRuntimeErrorKind::Unavailable,
                "owner generation exhausted",
            )
        })?;
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
            self.service_owned_root
                .join("control")
                .join(format!("owner-{owner_generation}.sock")),
            endpoint_secret,
        );
        state.owner_generation = owner_generation;
        state.owner = Some(ServiceOwner {
            owner_uid: request.owner_uid,
            supervisor_pid: request.supervisor_pid,
            supervisor_start_identity: request.supervisor_start_identity.clone(),
            instance_token: request.instance_token.clone(),
            generation: owner_generation,
            proof: proof.clone(),
            endpoint: endpoint.clone(),
        });
        state.degraded = false;

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
            state.degraded = false;
            state.consecutive_restart_failures = 0;
            return Ok(ApplyCandidateResult {
                disposition: ApplyDisposition::Reloaded,
                managed_core: handle,
            });
        }

        let record = self.spawn_verified(&mut state, &authenticated, &verified)?;
        let handle = record.handle.clone();
        state.managed_core = Some(record);
        state.last_bundle = Some(bundle.clone());
        state.degraded = false;
        state.consecutive_restart_failures = 0;
        Ok(ApplyCandidateResult {
            disposition: ApplyDisposition::Spawned,
            managed_core: handle,
        })
    }

    fn status(&self, owner: &OwnerSessionProof) -> Result<CoreRuntimeStatus, CoreRuntimeError> {
        let state = self.lock_state()?;
        authenticated_owner(&state, owner)?;
        if let Some(record) = state.managed_core.as_ref() {
            self.require_owned_process(record)?;
        }
        Ok(CoreRuntimeStatus {
            managed_core: state
                .managed_core
                .as_ref()
                .map(|record| record.handle.clone()),
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
        let Some(record) = state.managed_core.as_ref() else {
            return Ok(StopCoreResult {
                stopped: false,
                instance_generation: None,
            });
        };
        self.require_owned_process(record)?;
        self.dependencies
            .processes
            .stop(&record.owned_identity)
            .map_err(map_stop_error)?;
        let instance_generation = record.owned_identity.instance_generation;
        state.managed_core = None;
        state.last_bundle = None;
        state.degraded = false;
        state.consecutive_restart_failures = 0;
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
