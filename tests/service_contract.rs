use hopash::constants::{CORE_RESTART_INITIAL_BACKOFF, CORE_RESTART_MAX_BACKOFF};
use hopash::core::{
    ApplyDisposition, CoreControlEndpoint, CoreRuntime, CoreRuntimeDiagnosticCategory,
    CoreRuntimeError, CoreRuntimeErrorKind, CoreRuntimeLifecycle, CoreRuntimeTunReason,
    OwnerSessionProof, OwnerSessionRequest, ProcessOutputSource, RuntimeBundle,
};
use hopash::domain::{CoreInstanceGeneration, RuntimeGeneration};
use hopash::service::{
    CORE_RUNTIME_PROTOCOL_VERSION, CallerCredentialValidator, CoreExitIdentity,
    CoreProcessController, CoreProcessLog, CoreProcessLogBatch, OwnedProcessIdentity,
    PrivilegedCoreRuntimeService, PrivilegedServiceConfig, PrivilegedServiceDependencies,
    PrivilegedServiceLifecycle, ProcessIdentityProbe, RuntimeManifestFileV1, RuntimeManifestV1,
    SecretGenerator, ServiceGenerationStateCommitFault, ServiceMaintenanceOutcome,
    ServicePlatformError, ServicePlatformErrorKind, SpawnedCoreProcess, TunCapabilityPreflight,
    UnexpectedExitOutcome, VerifiedRuntimeBundle,
};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, VecDeque};
use std::fs;
#[cfg(unix)]
use std::os::unix::fs::{PermissionsExt, symlink};
#[cfg(unix)]
use std::os::unix::net::UnixListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);

struct TestDirectory {
    path: PathBuf,
}

impl TestDirectory {
    fn new() -> Self {
        let sequence = NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed);
        let path = PathBuf::from("/tmp").join(format!(
            "hopash-service-contract-{}-{sequence}",
            std::process::id()
        ));
        fs::create_dir(&path).expect("the fixture root should be created");
        Self { path }
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.path).expect("the fixture root should be removed");
    }
}

#[derive(Clone, Default)]
struct IdentityRegistry {
    identities: Arc<Mutex<BTreeMap<u32, String>>>,
}

impl IdentityRegistry {
    fn set(&self, pid: u32, identity: impl Into<String>) {
        self.identities
            .lock()
            .expect("identity lock")
            .insert(pid, identity.into());
    }

    fn remove(&self, pid: u32) {
        self.identities.lock().expect("identity lock").remove(&pid);
    }
}

impl ProcessIdentityProbe for IdentityRegistry {
    fn start_identity(&self, pid: u32) -> Result<Option<String>, ServicePlatformError> {
        Ok(self
            .identities
            .lock()
            .expect("identity lock")
            .get(&pid)
            .cloned())
    }
}

#[derive(Clone, Default)]
struct FakeCredentials {
    denied: Arc<AtomicBool>,
}

impl FakeCredentials {
    fn deny(&self, denied: bool) {
        self.denied.store(denied, Ordering::Release);
    }
}

impl CallerCredentialValidator for FakeCredentials {
    fn validate(&self, _request: &OwnerSessionRequest) -> Result<(), ServicePlatformError> {
        if self.denied.load(Ordering::Acquire) {
            Err(ServicePlatformError::new(
                ServicePlatformErrorKind::Credential,
            ))
        } else {
            Ok(())
        }
    }
}

#[derive(Clone, Default)]
struct FakeTun {
    error: Arc<Mutex<Option<ServicePlatformErrorKind>>>,
}

impl FakeTun {
    fn deny(&self, denied: bool) {
        *self.error.lock().expect("TUN error lock") =
            denied.then_some(ServicePlatformErrorKind::TunUnavailable);
    }

    fn unsupported(&self) {
        *self.error.lock().expect("TUN error lock") =
            Some(ServicePlatformErrorKind::TunUnsupported);
    }
}

impl TunCapabilityPreflight for FakeTun {
    fn check(&self, _owner_uid: u32) -> Result<(), ServicePlatformError> {
        match *self.error.lock().expect("TUN error lock") {
            Some(kind) => Err(ServicePlatformError::new(kind)),
            None => Ok(()),
        }
    }
}

#[derive(Clone, Default)]
struct CountingSecrets {
    next: Arc<AtomicU64>,
}

impl SecretGenerator for CountingSecrets {
    fn generate(&self) -> Result<String, ServicePlatformError> {
        let value = self.next.fetch_add(1, Ordering::Relaxed) + 1;
        Ok(format!("random-secret-{value}"))
    }
}

#[derive(Clone, Copy, Default)]
struct FailingSecrets;

impl SecretGenerator for FailingSecrets {
    fn generate(&self) -> Result<String, ServicePlatformError> {
        Err(ServicePlatformError::new(
            ServicePlatformErrorKind::Randomness,
        ))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SpawnScript {
    Success,
    Failure,
}

#[derive(Default)]
struct FakeProcessState {
    scripts: VecDeque<SpawnScript>,
    processes: BTreeMap<u32, String>,
    logs: VecDeque<CoreProcessLog>,
    dropped_logs: u64,
    reload_error: Option<ServicePlatformErrorKind>,
    readiness_error: Option<ServicePlatformErrorKind>,
}

#[derive(Clone)]
struct FakeProcesses {
    state: Arc<Mutex<FakeProcessState>>,
    identities: IdentityRegistry,
    next_pid: Arc<AtomicU32>,
    spawn_count: Arc<AtomicUsize>,
    reload_count: Arc<AtomicUsize>,
    stop_count: Arc<AtomicUsize>,
}

impl FakeProcesses {
    fn new(identities: IdentityRegistry) -> Self {
        Self {
            state: Arc::new(Mutex::new(FakeProcessState::default())),
            identities,
            next_pid: Arc::new(AtomicU32::new(2_000)),
            spawn_count: Arc::new(AtomicUsize::new(0)),
            reload_count: Arc::new(AtomicUsize::new(0)),
            stop_count: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn script_spawns(&self, scripts: impl IntoIterator<Item = SpawnScript>) {
        self.state
            .lock()
            .expect("process lock")
            .scripts
            .extend(scripts);
    }

    fn fail_reload(&self, kind: ServicePlatformErrorKind) {
        self.state.lock().expect("process lock").reload_error = Some(kind);
    }

    fn fail_readiness(&self, kind: ServicePlatformErrorKind) {
        self.state.lock().expect("process lock").readiness_error = Some(kind);
    }

    fn push_logs(&self, logs: impl IntoIterator<Item = CoreProcessLog>) {
        self.state.lock().expect("process lock").logs.extend(logs);
    }

    fn drop_logs(&self, count: u64) {
        let mut state = self.state.lock().expect("process lock");
        state.dropped_logs = state.dropped_logs.saturating_add(count);
    }

    fn mark_exited(&self, pid: u32) {
        self.state
            .lock()
            .expect("process lock")
            .processes
            .remove(&pid);
        self.identities.remove(pid);
    }
}

impl CoreProcessController for FakeProcesses {
    fn spawn(
        &self,
        _bundle: &VerifiedRuntimeBundle,
        _endpoint: &CoreControlEndpoint,
        instance_generation: CoreInstanceGeneration,
    ) -> Result<SpawnedCoreProcess, ServicePlatformError> {
        self.spawn_count.fetch_add(1, Ordering::Relaxed);
        let mut state = self.state.lock().expect("process lock");
        if state.scripts.pop_front() == Some(SpawnScript::Failure) {
            return Err(ServicePlatformError::new(ServicePlatformErrorKind::Spawn));
        }
        let pid = self.next_pid.fetch_add(1, Ordering::Relaxed);
        let identity = format!("core-{pid}-{}", instance_generation.0);
        state.processes.insert(pid, identity.clone());
        self.identities.set(pid, identity.clone());
        Ok(SpawnedCoreProcess {
            pid,
            process_start_identity: identity,
        })
    }

    fn reload(
        &self,
        process: &OwnedProcessIdentity,
        _bundle: &VerifiedRuntimeBundle,
    ) -> Result<(), ServicePlatformError> {
        self.reload_count.fetch_add(1, Ordering::Relaxed);
        let mut state = self.state.lock().expect("process lock");
        if let Some(kind) = state.reload_error.take() {
            return Err(ServicePlatformError::new(kind));
        }
        match state.processes.get(&process.pid) {
            Some(identity) if identity == &process.process_start_identity => Ok(()),
            _ => Err(ServicePlatformError::new(ServicePlatformErrorKind::Reload)),
        }
    }

    fn stop(&self, process: &OwnedProcessIdentity) -> Result<(), ServicePlatformError> {
        self.stop_count.fetch_add(1, Ordering::Relaxed);
        let mut state = self.state.lock().expect("process lock");
        match state.processes.get(&process.pid) {
            Some(identity) if identity == &process.process_start_identity => {
                state.processes.remove(&process.pid);
                self.identities.remove(process.pid);
                Ok(())
            }
            _ => Err(ServicePlatformError::new(ServicePlatformErrorKind::Stop)),
        }
    }

    fn readiness(
        &self,
        _process: &OwnedProcessIdentity,
        _endpoint: &CoreControlEndpoint,
    ) -> Result<(), ServicePlatformError> {
        let mut state = self.state.lock().expect("process lock");
        match state.readiness_error.take() {
            Some(kind) => Err(ServicePlatformError::new(kind)),
            None => Ok(()),
        }
    }

    fn grant_endpoint_access(
        &self,
        _endpoint: &CoreControlEndpoint,
        _owner_uid: u32,
    ) -> Result<(), ServicePlatformError> {
        Ok(())
    }

    fn reap_if_exited(&self, process: &OwnedProcessIdentity) -> Result<bool, ServicePlatformError> {
        let state = self.state.lock().expect("process lock");
        match state.processes.get(&process.pid) {
            Some(identity) if identity == &process.process_start_identity => Ok(false),
            Some(_) => Err(ServicePlatformError::new(
                ServicePlatformErrorKind::ProcessInspection,
            )),
            None => Ok(true),
        }
    }

    fn take_logs(
        &self,
        _process: &OwnedProcessIdentity,
        limit: usize,
    ) -> Result<CoreProcessLogBatch, ServicePlatformError> {
        let mut state = self.state.lock().expect("process lock");
        let take = limit.min(state.logs.len());
        let dropped = std::mem::take(&mut state.dropped_logs);
        Ok(CoreProcessLogBatch {
            records: state.logs.drain(..take).collect(),
            dropped,
        })
    }
}

struct Harness {
    directory: TestDirectory,
    service_root: PathBuf,
    policy_sha256: String,
    binary: Vec<u8>,
    binary_sha256: String,
    identities: IdentityRegistry,
    credentials: FakeCredentials,
    tun: FakeTun,
    processes: FakeProcesses,
    restart_limit: usize,
    log_capacity: usize,
    max_log_line_bytes: usize,
    service: PrivilegedCoreRuntimeService,
}

impl Harness {
    fn new() -> Self {
        Self::with_limits(3, 4, 8)
    }

    fn with_limits(restart_limit: usize, log_capacity: usize, max_log_line_bytes: usize) -> Self {
        let directory = TestDirectory::new();
        let service_root = directory.path.join("service-owned");
        let policy_sha256 = sha256(b"compiler-policy");
        let binary = b"fixture-mihomo-binary".to_vec();
        let binary_sha256 = sha256(&binary);
        let identities = IdentityRegistry::default();
        let credentials = FakeCredentials::default();
        let tun = FakeTun::default();
        let processes = FakeProcesses::new(identities.clone());
        let service = PrivilegedCoreRuntimeService::new(
            PrivilegedServiceConfig {
                protocol_version: CORE_RUNTIME_PROTOCOL_VERSION,
                service_owned_root: service_root.clone(),
                compiler_policy_sha256: policy_sha256.clone(),
                mihomo_binary_sha256: binary_sha256.clone(),
                restart_limit,
                log_capacity,
                max_log_line_bytes,
            },
            PrivilegedServiceDependencies {
                credentials: Box::new(credentials.clone()),
                identities: Box::new(identities.clone()),
                tun: Box::new(tun.clone()),
                secrets: Box::new(CountingSecrets::default()),
                processes: Box::new(processes.clone()),
            },
        )
        .expect("the fixture service should initialize");
        let service_root =
            fs::canonicalize(service_root).expect("the fixture service root should canonicalize");
        Self {
            directory,
            service_root,
            policy_sha256,
            binary,
            binary_sha256,
            identities,
            credentials,
            tun,
            processes,
            restart_limit,
            log_capacity,
            max_log_line_bytes,
            service,
        }
    }

    fn request(&self, pid: u32, identity: &str, instance_token: &str) -> OwnerSessionRequest {
        self.identities.set(pid, identity);
        OwnerSessionRequest {
            owner_uid: 501,
            supervisor_pid: pid,
            supervisor_start_identity: identity.to_owned(),
            instance_token: instance_token.to_owned(),
            protocol_version: CORE_RUNTIME_PROTOCOL_VERSION,
        }
    }

    fn open(&self) -> hopash::core::OwnerSession {
        self.service
            .open_owner_session(&self.request(100, "supervisor-100", "instance-100"))
            .expect("the fixture owner should open")
    }

    fn bundle(&self, generation: u64) -> RuntimeBundle {
        write_bundle(
            &self.service_root.join(format!("generation-{generation}")),
            RuntimeGeneration(generation),
            &self.policy_sha256,
            &self.binary,
            &self.binary_sha256,
        )
    }

    fn reopen(&self) -> PrivilegedCoreRuntimeService {
        self.try_reopen()
            .expect("the fixture service should reopen")
    }

    fn try_reopen(&self) -> Result<PrivilegedCoreRuntimeService, CoreRuntimeError> {
        self.try_reopen_with_secrets(Box::new(CountingSecrets::default()))
    }

    fn try_reopen_with_secrets(
        &self,
        secrets: Box<dyn SecretGenerator>,
    ) -> Result<PrivilegedCoreRuntimeService, CoreRuntimeError> {
        self.try_open_at_root(self.service_root.clone(), secrets)
    }

    fn try_open_at_root(
        &self,
        service_owned_root: PathBuf,
        secrets: Box<dyn SecretGenerator>,
    ) -> Result<PrivilegedCoreRuntimeService, CoreRuntimeError> {
        PrivilegedCoreRuntimeService::new(
            PrivilegedServiceConfig {
                protocol_version: CORE_RUNTIME_PROTOCOL_VERSION,
                service_owned_root,
                compiler_policy_sha256: self.policy_sha256.clone(),
                mihomo_binary_sha256: self.binary_sha256.clone(),
                restart_limit: self.restart_limit,
                log_capacity: self.log_capacity,
                max_log_line_bytes: self.max_log_line_bytes,
            },
            PrivilegedServiceDependencies {
                credentials: Box::new(self.credentials.clone()),
                identities: Box::new(self.identities.clone()),
                tun: Box::new(self.tun.clone()),
                secrets,
                processes: Box::new(self.processes.clone()),
            },
        )
    }

    fn generation_state_path(&self) -> PathBuf {
        self.service_root
            .join("control")
            .join("generation-state-v1.json")
    }

    fn generation_lock_path(&self) -> PathBuf {
        self.service_root
            .join("control")
            .join("generation-state-v1.lock")
    }

    fn reopen_error(&self) -> CoreRuntimeError {
        match self.try_reopen() {
            Ok(_) => panic!("the fixture service reopen should fail"),
            Err(error) => error,
        }
    }
}

#[test]
fn session_bootstrap_negotiates_protocol_credentials_random_proof_and_generation() {
    let harness = Harness::new();
    let mut incompatible = harness.request(100, "supervisor-100", "instance-100");
    incompatible.protocol_version += 1;
    let protocol_error = harness
        .service
        .open_owner_session(&incompatible)
        .expect_err("the incompatible protocol should be rejected");
    assert_eq!(protocol_error.kind, CoreRuntimeErrorKind::ProtocolMismatch);

    harness.credentials.deny(true);
    let credential_error = harness
        .service
        .open_owner_session(&harness.request(100, "supervisor-100", "instance-100"))
        .expect_err("invalid caller credentials should be rejected");
    assert_eq!(credential_error.kind, CoreRuntimeErrorKind::Authentication);
    harness.credentials.deny(false);

    let request = harness.request(100, "supervisor-100", "instance-100");
    let first = harness
        .service
        .open_owner_session(&request)
        .expect("the first owner should open");
    let duplicate = harness
        .service
        .open_owner_session(&request)
        .expect("the same owner bootstrap should be idempotent");
    let metadata = harness
        .service
        .owner_metadata(&first.proof)
        .expect("owner metadata should be available");

    assert_eq!(first, duplicate);
    assert_eq!(metadata.owner_generation, 1);
    let canonical_service_root =
        fs::canonicalize(&harness.service_root).expect("the service root should canonicalize");
    assert!(
        metadata
            .endpoint
            .socket_path
            .starts_with(canonical_service_root)
    );
    assert!(first.proof.session_id().contains("random-secret-1"));
    assert_eq!(first.proof.session_token(), "random-secret-2");
    assert_eq!(metadata.endpoint.secret(), "random-secret-3");
    assert!(!format!("{:?}", first.proof).contains("random-secret-2"));
    let service_mode = fs::symlink_metadata(&harness.service_root)
        .expect("service root metadata should load")
        .permissions()
        .mode()
        & 0o777;
    let control_mode = fs::symlink_metadata(harness.service_root.join("control"))
        .expect("control root metadata should load")
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(service_mode, 0o711);
    assert_eq!(control_mode, 0o711);
    let generation_state_metadata = fs::symlink_metadata(harness.generation_state_path())
        .expect("generation state metadata should load");
    assert!(generation_state_metadata.file_type().is_file());
    assert_eq!(
        generation_state_metadata.permissions().mode() & 0o777,
        0o600
    );
    let generation_lock_metadata = fs::symlink_metadata(harness.generation_lock_path())
        .expect("generation lock metadata should load");
    assert!(generation_lock_metadata.file_type().is_file());
    assert_eq!(generation_lock_metadata.len(), 0);
    assert_eq!(generation_lock_metadata.permissions().mode() & 0o777, 0o600);
}

#[test]
fn owner_and_core_generations_continue_after_service_reopen() {
    let harness = Harness::new();
    let first_session = harness.open();
    let first_apply = harness
        .service
        .apply_candidate(&first_session.proof, &harness.bundle(1))
        .expect("the first Core should spawn");
    harness
        .service
        .close_owner_session(&first_session.proof)
        .expect("the first owner should close");

    let reopened = harness.reopen();
    let second_session = reopened
        .open_owner_session(&harness.request(200, "supervisor-200", "instance-200"))
        .expect("the reopened service should admit a new owner");
    let second_apply = reopened
        .apply_candidate(&second_session.proof, &harness.bundle(2))
        .expect("the reopened service should spawn a new Core");

    assert_eq!(first_session.owner_generation, 1);
    assert_eq!(second_session.owner_generation, 2);
    assert_eq!(
        first_apply.managed_core.instance_generation,
        CoreInstanceGeneration(1)
    );
    assert_eq!(
        second_apply.managed_core.instance_generation,
        CoreInstanceGeneration(2)
    );
}

#[test]
fn stale_service_instances_reread_locked_generation_high_water_marks() {
    let harness = Harness::new();
    let first_service = harness.reopen();
    let stale_service = harness.reopen();
    let first_session = first_service
        .open_owner_session(&harness.request(100, "supervisor-100", "instance-100"))
        .expect("the first service should admit an owner");
    let first_apply = first_service
        .apply_candidate(&first_session.proof, &harness.bundle(1))
        .expect("the first service should spawn a Core");
    first_service
        .close_owner_session(&first_session.proof)
        .expect("the first service owner should close");

    let stale_session = stale_service
        .open_owner_session(&harness.request(200, "supervisor-200", "instance-200"))
        .expect("the stale service should refresh the owner high-water mark");
    let stale_apply = stale_service
        .apply_candidate(&stale_session.proof, &harness.bundle(2))
        .expect("the stale service should refresh the Core high-water mark");

    assert_eq!(first_session.owner_generation, 1);
    assert_eq!(stale_session.owner_generation, 2);
    assert_eq!(
        first_apply.managed_core.instance_generation,
        CoreInstanceGeneration(1)
    );
    assert_eq!(
        stale_apply.managed_core.instance_generation,
        CoreInstanceGeneration(2)
    );
}

#[test]
fn failed_core_spawn_reserves_a_durable_generation_gap() {
    let harness = Harness::new();
    let first_session = harness.open();
    harness.processes.script_spawns([SpawnScript::Failure]);
    let error = harness
        .service
        .apply_candidate(&first_session.proof, &harness.bundle(1))
        .expect_err("the scripted Core spawn should fail");
    assert_eq!(error.kind, CoreRuntimeErrorKind::Apply);
    harness
        .service
        .close_owner_session(&first_session.proof)
        .expect("the failed owner should close");

    let reopened = harness.reopen();
    let second_session = reopened
        .open_owner_session(&harness.request(200, "supervisor-200", "instance-200"))
        .expect("the reopened service should admit a new owner");
    let applied = reopened
        .apply_candidate(&second_session.proof, &harness.bundle(2))
        .expect("the second Core spawn should succeed");

    assert_eq!(second_session.owner_generation, 2);
    assert_eq!(
        applied.managed_core.instance_generation,
        CoreInstanceGeneration(2)
    );
}

#[test]
fn core_spawn_waits_for_a_durable_private_generation_reservation() {
    let harness = Harness::new();
    let session = harness.open();
    let state_path = harness.generation_state_path();
    fs::set_permissions(&state_path, fs::Permissions::from_mode(0o644))
        .expect("the generation state permissions should change");

    let error = harness
        .service
        .apply_candidate(&session.proof, &harness.bundle(1))
        .expect_err("a non-private generation state should block Core spawn");

    assert_eq!(error.kind, CoreRuntimeErrorKind::Unavailable);
    assert_eq!(harness.processes.spawn_count.load(Ordering::Relaxed), 0);

    fs::set_permissions(&state_path, fs::Permissions::from_mode(0o600))
        .expect("the generation state privacy should be restored");
    let applied = harness
        .service
        .apply_candidate(&session.proof, &harness.bundle(1))
        .expect("the durable Core generation reservation should succeed");

    assert_eq!(
        applied.managed_core.instance_generation,
        CoreInstanceGeneration(1)
    );
    assert_eq!(harness.processes.spawn_count.load(Ordering::Relaxed), 1);
}

#[test]
fn every_atomic_generation_commit_failure_prevents_spawn_and_recovers_high_water() {
    for (fault, recovered_generation) in [
        (ServiceGenerationStateCommitFault::Write, 1),
        (ServiceGenerationStateCommitFault::FileSync, 1),
        (ServiceGenerationStateCommitFault::Rename, 1),
        (ServiceGenerationStateCommitFault::DirectorySync, 2),
    ] {
        let harness = Harness::new();
        let session = harness.open();
        harness
            .service
            .arm_generation_state_commit_fault(fault)
            .expect("the generation commit fault should arm");

        let error = harness
            .service
            .apply_candidate(&session.proof, &harness.bundle(1))
            .expect_err("the injected generation commit failure should block Core spawn");

        assert_eq!(error.kind, CoreRuntimeErrorKind::Unavailable);
        assert_eq!(harness.processes.spawn_count.load(Ordering::Relaxed), 0);

        let reopened = harness.reopen();
        let recovery_session = reopened
            .open_owner_session(&harness.request(200, "supervisor-200", "instance-200"))
            .expect("the persisted high-water state should remain recoverable");
        let recovered = reopened
            .apply_candidate(&recovery_session.proof, &harness.bundle(2))
            .expect("the recovered service should spawn a Core");

        assert_eq!(
            recovered.managed_core.instance_generation,
            CoreInstanceGeneration(recovered_generation)
        );
        assert_eq!(harness.processes.spawn_count.load(Ordering::Relaxed), 1);
    }
}

#[test]
fn failed_owner_secret_generation_reserves_a_durable_generation_gap() {
    let harness = Harness::new();
    let failing_service = harness
        .try_reopen_with_secrets(Box::new(FailingSecrets))
        .expect("the failing fixture service should initialize");
    let error = failing_service
        .open_owner_session(&harness.request(100, "supervisor-100", "instance-100"))
        .expect_err("the scripted secret generation should fail");
    assert_eq!(error.kind, CoreRuntimeErrorKind::Unavailable);
    drop(failing_service);

    let reopened = harness.reopen();
    let session = reopened
        .open_owner_session(&harness.request(200, "supervisor-200", "instance-200"))
        .expect("the reopened service should admit a new owner");

    assert_eq!(session.owner_generation, 2);
}

#[test]
fn service_reopen_rejects_corrupt_generation_state_without_exposing_paths() {
    let harness = Harness::new();
    fs::write(harness.generation_state_path(), b"{broken")
        .expect("the generation state should be corrupted");

    let error = harness.reopen_error();
    let rendered = format!("{error} {error:?}");

    assert_eq!(error.kind, CoreRuntimeErrorKind::Unavailable);
    assert!(!rendered.contains(&harness.directory.path.display().to_string()));
    assert!(!rendered.contains("generation-state-v1.json"));
}

#[test]
fn service_reopen_rejects_non_private_generation_state_permissions() {
    let harness = Harness::new();
    let state_path = harness.generation_state_path();
    fs::set_permissions(&state_path, fs::Permissions::from_mode(0o644))
        .expect("the generation state permissions should change");

    let error = harness.reopen_error();

    assert_eq!(error.kind, CoreRuntimeErrorKind::Unavailable);
}

#[test]
fn service_reopen_rejects_non_private_control_root_permissions() {
    let harness = Harness::new();
    let control_root = harness.service_root.join("control");
    fs::set_permissions(&control_root, fs::Permissions::from_mode(0o700))
        .expect("the control root permissions should change");

    let error = harness.reopen_error();

    assert_eq!(error.kind, CoreRuntimeErrorKind::Unavailable);
}

#[test]
fn service_reopen_rejects_non_private_service_root_permissions() {
    let harness = Harness::new();
    fs::set_permissions(&harness.service_root, fs::Permissions::from_mode(0o700))
        .expect("the service root permissions should change");

    let error = harness.reopen_error();

    assert_eq!(error.kind, CoreRuntimeErrorKind::Unavailable);
}

#[test]
fn service_reopen_rejects_generation_state_symlinks() {
    let harness = Harness::new();
    let state_path = harness.generation_state_path();
    let external_state = harness
        .directory
        .path
        .join("external-generation-state.json");
    fs::write(
        &external_state,
        br#"{"schema_version":1,"owner_generation":20,"core_instance_generation":30}"#,
    )
    .expect("the external generation state should be written");
    fs::set_permissions(&external_state, fs::Permissions::from_mode(0o600))
        .expect("the external generation state should be private");
    fs::remove_file(&state_path).expect("the managed generation state should be removed");
    symlink(&external_state, &state_path).expect("the generation state symlink should be created");

    let error = harness.reopen_error();

    assert_eq!(error.kind, CoreRuntimeErrorKind::Unavailable);
}

#[test]
fn service_reopen_rejects_control_root_symlinks() {
    let harness = Harness::new();
    let control_root = harness.service_root.join("control");
    fs::remove_file(harness.generation_state_path())
        .expect("the managed generation state should be removed");
    fs::remove_file(harness.generation_lock_path())
        .expect("the managed generation lock should be removed");
    fs::remove_dir(&control_root).expect("the managed control root should be removed");
    let external_control_root = harness.directory.path.join("external-control");
    fs::create_dir(&external_control_root).expect("the external control root should be created");
    fs::set_permissions(&external_control_root, fs::Permissions::from_mode(0o711))
        .expect("the external control root permissions should be set");
    symlink(&external_control_root, &control_root).expect("the control root symlink should exist");

    let error = harness.reopen_error();

    assert_eq!(error.kind, CoreRuntimeErrorKind::Unavailable);
}

#[test]
fn service_reopen_rejects_service_root_symlinks() {
    let harness = Harness::new();
    let control_root = harness.service_root.join("control");
    fs::remove_file(harness.generation_state_path())
        .expect("the managed generation state should be removed");
    fs::remove_file(harness.generation_lock_path())
        .expect("the managed generation lock should be removed");
    fs::remove_dir(control_root).expect("the managed control root should be removed");
    fs::remove_dir(&harness.service_root).expect("the managed service root should be removed");
    let external_service_root = harness.directory.path.join("external-service");
    fs::create_dir(&external_service_root).expect("the external service root should be created");
    fs::set_permissions(&external_service_root, fs::Permissions::from_mode(0o711))
        .expect("the external service root permissions should be set");
    symlink(&external_service_root, &harness.service_root)
        .expect("the service root symlink should exist");

    let error = harness.reopen_error();

    assert_eq!(error.kind, CoreRuntimeErrorKind::Unavailable);
}

#[test]
fn service_reopen_rejects_service_root_parent_symlinks() {
    let harness = Harness::new();
    let external_parent = harness.directory.path.join("external-parent");
    fs::create_dir(&external_parent).expect("the external parent should be created");
    let linked_parent = harness.directory.path.join("linked-parent");
    symlink(&external_parent, &linked_parent).expect("the parent symlink should exist");

    let result = harness.try_open_at_root(
        linked_parent.join("service-owned"),
        Box::new(CountingSecrets::default()),
    );
    let error = match result {
        Ok(_) => panic!("a service root below a symlinked parent should be rejected"),
        Err(error) => error,
    };

    assert_eq!(error.kind, CoreRuntimeErrorKind::Unavailable);
}

#[test]
fn service_reopen_rejects_group_or_other_writable_service_root_parents() {
    let harness = Harness::new();
    let writable_parent = harness.directory.path.join("writable-parent");
    fs::create_dir(&writable_parent).expect("the writable parent should be created");
    fs::set_permissions(&writable_parent, fs::Permissions::from_mode(0o777))
        .expect("the writable parent permissions should change");

    let result = harness.try_open_at_root(
        writable_parent.join("service-owned"),
        Box::new(CountingSecrets::default()),
    );
    let error = match result {
        Ok(_) => panic!("a service root below a writable parent should be rejected"),
        Err(error) => error,
    };

    assert_eq!(error.kind, CoreRuntimeErrorKind::Unavailable);
}

#[test]
fn service_reopen_rejects_non_file_generation_state() {
    let harness = Harness::new();
    let state_path = harness.generation_state_path();
    fs::remove_file(&state_path).expect("the managed generation state should be removed");
    fs::create_dir(&state_path).expect("a directory should replace the generation state");

    let error = harness.reopen_error();

    assert_eq!(error.kind, CoreRuntimeErrorKind::Unavailable);
}

#[test]
fn service_reopen_cleans_only_strict_pending_generation_states() {
    let harness = Harness::new();
    let control_root = harness.service_root.join("control");
    let mut private_pending = Vec::new();
    for sequence in 1..=16 {
        let identifier = uuid::Uuid::from_u128(sequence);
        let path = control_root.join(format!(".generation-state-v1.json.{identifier}.pending"));
        fs::write(&path, b"partial").expect("the private pending state should be written");
        let mode = if sequence == 1 { 0o000 } else { 0o600 };
        fs::set_permissions(&path, fs::Permissions::from_mode(mode))
            .expect("the private pending state should be private");
        private_pending.push(path);
    }
    let unknown = control_root.join("unrelated.pending");
    fs::write(&unknown, b"preserve").expect("the unknown entry should be written");
    let noncanonical_uuid =
        control_root.join(".generation-state-v1.json.00000000000000000000000000000016.pending");
    fs::write(&noncanonical_uuid, b"preserve")
        .expect("the noncanonical pending entry should be written");
    fs::set_permissions(&noncanonical_uuid, fs::Permissions::from_mode(0o600))
        .expect("the noncanonical pending entry should be private");
    let non_private = control_root.join(format!(
        ".generation-state-v1.json.{}.pending",
        uuid::Uuid::from_u128(20)
    ));
    fs::write(&non_private, b"preserve").expect("the non-private entry should be written");
    fs::set_permissions(&non_private, fs::Permissions::from_mode(0o644))
        .expect("the non-private entry permissions should change");
    let special_mode = control_root.join(format!(
        ".generation-state-v1.json.{}.pending",
        uuid::Uuid::from_u128(22)
    ));
    fs::write(&special_mode, b"preserve").expect("the special-mode entry should be written");
    fs::set_permissions(&special_mode, fs::Permissions::from_mode(0o1600))
        .expect("the special-mode entry permissions should change");
    let symlink_target = harness.directory.path.join("pending-symlink-target");
    fs::write(&symlink_target, b"preserve").expect("the symlink target should be written");
    let pending_symlink = control_root.join(format!(
        ".generation-state-v1.json.{}.pending",
        uuid::Uuid::from_u128(21)
    ));
    symlink(&symlink_target, &pending_symlink).expect("the pending symlink should be created");

    let _reopened = harness.reopen();

    assert!(private_pending.iter().all(|path| !path.exists()));
    assert!(unknown.exists());
    assert!(noncanonical_uuid.exists());
    assert!(non_private.exists());
    assert!(special_mode.exists());
    assert!(fs::symlink_metadata(pending_symlink).is_ok());
}

#[test]
fn live_owner_blocks_takeover_and_stale_owner_cleanup_advances_generation() {
    let harness = Harness::new();
    let first = harness.open();
    harness
        .service
        .apply_candidate(&first.proof, &harness.bundle(1))
        .expect("the first owner should spawn a Core");
    let second_request = harness.request(200, "supervisor-200", "instance-200");

    let live_error = harness
        .service
        .open_owner_session(&second_request)
        .expect_err("a live owner should retain ownership");
    assert_eq!(live_error.kind, CoreRuntimeErrorKind::Authentication);
    assert_eq!(harness.processes.stop_count.load(Ordering::Relaxed), 0);

    harness.identities.remove(100);
    let second = harness
        .service
        .open_owner_session(&second_request)
        .expect("a stale owner should be replaced after cleanup");
    let metadata = harness
        .service
        .owner_metadata(&second.proof)
        .expect("new owner metadata should be available");
    assert_eq!(metadata.owner_generation, 2);
    assert_eq!(harness.processes.stop_count.load(Ordering::Relaxed), 1);
    assert_eq!(
        harness
            .service
            .status(&first.proof)
            .expect_err("the stale proof should be revoked")
            .kind,
        CoreRuntimeErrorKind::Authentication
    );
}

#[test]
fn every_core_request_requires_the_exact_session_proof_and_revoke_cleans_up() {
    let harness = Harness::new();
    let session = harness.open();
    harness
        .service
        .apply_candidate(&session.proof, &harness.bundle(1))
        .expect("the Core should spawn");
    let wrong = OwnerSessionProof::new(session.proof.session_id(), "wrong-token");

    assert_eq!(
        harness
            .service
            .apply_candidate(&wrong, &harness.bundle(2))
            .expect_err("apply should authenticate")
            .kind,
        CoreRuntimeErrorKind::Authentication
    );
    assert_eq!(
        harness
            .service
            .status(&wrong)
            .expect_err("status should authenticate")
            .kind,
        CoreRuntimeErrorKind::Authentication
    );
    assert_eq!(
        harness
            .service
            .logs(&wrong, None, 1)
            .expect_err("logs should authenticate")
            .kind,
        CoreRuntimeErrorKind::Authentication
    );
    assert_eq!(
        harness
            .service
            .stop(&wrong)
            .expect_err("stop should authenticate")
            .kind,
        CoreRuntimeErrorKind::Authentication
    );
    assert_eq!(
        harness
            .service
            .close_owner_session(&wrong)
            .expect_err("close should authenticate")
            .kind,
        CoreRuntimeErrorKind::Authentication
    );

    harness
        .service
        .revoke_owner(&session.proof)
        .expect("revoke should stop the owned Core");
    assert_eq!(harness.processes.stop_count.load(Ordering::Relaxed), 1);
    assert_eq!(
        harness
            .service
            .status(&session.proof)
            .expect_err("the revoked proof should expire")
            .kind,
        CoreRuntimeErrorKind::Authentication
    );
}

#[test]
fn maintenance_revokes_a_dead_or_replaced_owner_and_stops_its_owned_core() {
    for replacement_identity in [None, Some("replacement-process")] {
        let harness = Harness::new();
        let session = harness.open();
        harness
            .service
            .apply_candidate(&session.proof, &harness.bundle(1))
            .expect("the Core should spawn");
        match replacement_identity {
            Some(identity) => harness.identities.set(100, identity),
            None => harness.identities.remove(100),
        }

        let outcome = harness
            .service
            .maintenance_step(Duration::ZERO)
            .expect("maintenance should revoke the stale owner")
            .outcome;

        assert_eq!(outcome, ServiceMaintenanceOutcome::OwnerRevoked);
        assert_eq!(harness.processes.stop_count.load(Ordering::Relaxed), 1);
        assert_eq!(
            harness
                .service
                .status(&session.proof)
                .expect_err("the stale owner proof should expire")
                .kind,
            CoreRuntimeErrorKind::Authentication
        );
        assert_eq!(
            harness
                .service
                .maintenance_step(Duration::ZERO)
                .expect("idle maintenance should remain available")
                .outcome,
            ServiceMaintenanceOutcome::Unchanged(PrivilegedServiceLifecycle::Idle)
        );
    }
}

#[test]
fn maintenance_restarts_an_unexpected_core_exit_with_the_bounded_policy() {
    let harness = Harness::with_limits(3, 4, 8);
    let session = harness.open();
    let first = harness
        .service
        .apply_candidate(&session.proof, &harness.bundle(1))
        .expect("the initial Core should spawn");
    harness.processes.mark_exited(first.managed_core.pid);
    harness
        .processes
        .script_spawns([SpawnScript::Failure, SpawnScript::Success]);

    let scheduled = harness
        .service
        .maintenance_step(Duration::ZERO)
        .expect("maintenance should schedule recovery");
    assert_eq!(
        scheduled.outcome,
        ServiceMaintenanceOutcome::UnexpectedExit(UnexpectedExitOutcome::Pending {
            attempts: 0,
            next_attempt_at: CORE_RESTART_INITIAL_BACKOFF,
        })
    );
    assert_eq!(scheduled.next_deadline, CORE_RESTART_INITIAL_BACKOFF);
    assert_eq!(harness.processes.spawn_count.load(Ordering::Relaxed), 1);
    let status = harness
        .service
        .status(&session.proof)
        .expect("pending restart status should load");
    assert_eq!(status.lifecycle, CoreRuntimeLifecycle::RestartPending);
    assert!(status.restart.pending);
    assert_eq!(status.restart.attempts, 0);
    assert_eq!(status.restart.backoff, Some(CORE_RESTART_INITIAL_BACKOFF));
    assert_eq!(status.restart.diagnostic, None);

    let early = harness
        .service
        .maintenance_step(CORE_RESTART_INITIAL_BACKOFF - Duration::from_millis(1))
        .expect("early maintenance should retain the deadline");
    assert_eq!(early, scheduled);
    assert_eq!(harness.processes.spawn_count.load(Ordering::Relaxed), 1);

    let first_attempt = harness
        .service
        .maintenance_step(CORE_RESTART_INITIAL_BACKOFF)
        .expect("the first due step should make one attempt");
    let second_deadline = CORE_RESTART_INITIAL_BACKOFF + (CORE_RESTART_INITIAL_BACKOFF * 2);
    assert_eq!(
        first_attempt.outcome,
        ServiceMaintenanceOutcome::UnexpectedExit(UnexpectedExitOutcome::Pending {
            attempts: 1,
            next_attempt_at: second_deadline,
        })
    );
    assert_eq!(first_attempt.next_deadline, second_deadline);
    assert_eq!(harness.processes.spawn_count.load(Ordering::Relaxed), 2);
    let retry_status = harness
        .service
        .status(&session.proof)
        .expect("retry status should load");
    assert_eq!(retry_status.lifecycle, CoreRuntimeLifecycle::RestartPending);
    assert_eq!(retry_status.restart.attempts, 1);
    assert_eq!(
        retry_status.restart.backoff,
        Some(CORE_RESTART_INITIAL_BACKOFF * 2)
    );

    let repeated = harness
        .service
        .maintenance_step(CORE_RESTART_INITIAL_BACKOFF)
        .expect("a repeated step before the deadline should preserve retry state");
    assert_eq!(repeated, first_attempt);
    assert_eq!(harness.processes.spawn_count.load(Ordering::Relaxed), 2);

    let outcome = harness
        .service
        .maintenance_step(second_deadline)
        .expect("the second due step should recover the unexpected exit")
        .outcome;

    let ServiceMaintenanceOutcome::UnexpectedExit(UnexpectedExitOutcome::Restarted {
        attempts,
        managed_core,
    }) = outcome
    else {
        panic!("maintenance should report the restarted Core");
    };
    assert_eq!(attempts, 2);
    assert_eq!(managed_core.instance_generation, CoreInstanceGeneration(3));
    assert_eq!(managed_core.runtime_generation, RuntimeGeneration(1));
    assert_eq!(
        harness
            .service
            .maintenance_step(second_deadline)
            .expect("the restarted Core should remain healthy"),
        hopash::service::ServiceMaintenanceStep {
            outcome: ServiceMaintenanceOutcome::Unchanged(PrivilegedServiceLifecycle::Running),
            next_deadline: second_deadline + hopash::constants::CORE_SERVICE_LIVENESS_INTERVAL,
        }
    );
}

#[test]
fn runtime_status_observes_an_exit_before_maintenance_schedules_recovery() {
    let harness = Harness::new();
    let session = harness.open();
    let first = harness
        .service
        .apply_candidate(&session.proof, &harness.bundle(1))
        .expect("the initial Core should spawn");
    harness.processes.mark_exited(first.managed_core.pid);

    let status = harness
        .service
        .status(&session.proof)
        .expect("status should observe the exited Core");

    assert_eq!(status.lifecycle, CoreRuntimeLifecycle::RestartPending);
    assert!(status.managed_core.is_none());
    assert!(status.restart.pending);
    assert_eq!(status.restart.attempts, 0);
    assert_eq!(status.restart.backoff, Some(CORE_RESTART_INITIAL_BACKOFF));
    assert!(matches!(
        harness
            .service
            .maintenance_step(Duration::ZERO)
            .expect("maintenance should schedule recovery after status observation")
            .outcome,
        ServiceMaintenanceOutcome::UnexpectedExit(UnexpectedExitOutcome::Pending {
            attempts: 0,
            ..
        })
    ));
}

#[test]
fn maintenance_preserves_the_degraded_restart_bound() {
    let harness = Harness::with_limits(2, 4, 8);
    let session = harness.open();
    let first = harness
        .service
        .apply_candidate(&session.proof, &harness.bundle(1))
        .expect("the initial Core should spawn");
    harness.processes.mark_exited(first.managed_core.pid);
    harness
        .processes
        .script_spawns([SpawnScript::Failure, SpawnScript::Failure]);

    let scheduled = harness
        .service
        .maintenance_step(Duration::ZERO)
        .expect("maintenance should schedule recovery");
    let first_deadline = scheduled.next_deadline;
    let retry = harness
        .service
        .maintenance_step(first_deadline)
        .expect("the first due step should retain retry state");
    let second_deadline = retry.next_deadline;
    assert_eq!(harness.processes.spawn_count.load(Ordering::Relaxed), 2);
    assert_eq!(
        harness
            .service
            .maintenance_step(second_deadline)
            .expect("the second due step should reach the restart bound")
            .outcome,
        ServiceMaintenanceOutcome::UnexpectedExit(UnexpectedExitOutcome::Degraded {
            attempts: 2,
            diagnostic: CoreRuntimeDiagnosticCategory::CoreRestartLimitReached,
        })
    );
    assert_eq!(
        harness
            .service
            .maintenance_step(second_deadline)
            .expect("degraded maintenance should remain bounded"),
        hopash::service::ServiceMaintenanceStep {
            outcome: ServiceMaintenanceOutcome::Unchanged(PrivilegedServiceLifecycle::Degraded),
            next_deadline: hopash::constants::CORE_SERVICE_LIVENESS_INTERVAL,
        }
    );
    assert_eq!(harness.processes.spawn_count.load(Ordering::Relaxed), 3);
    let status = harness
        .service
        .status(&session.proof)
        .expect("degraded runtime status should load");
    assert_eq!(status.lifecycle, CoreRuntimeLifecycle::Degraded);
    assert!(!status.restart.pending);
    assert_eq!(status.restart.attempts, 2);
    assert_eq!(status.restart.backoff, None);
    assert_eq!(
        status.restart.diagnostic,
        Some(CoreRuntimeDiagnosticCategory::CoreRestartLimitReached)
    );
}

#[test]
fn runtime_status_projects_tun_permission_and_platform_support() {
    let harness = Harness::new();
    let session = harness.open();
    harness.tun.deny(true);

    let denied = harness
        .service
        .status(&session.proof)
        .expect("permission status should load");
    assert!(!denied.tun.capable);
    assert_eq!(
        denied.tun.reason,
        Some(CoreRuntimeTunReason::PermissionDenied)
    );

    harness.tun.unsupported();
    let unsupported = harness
        .service
        .status(&session.proof)
        .expect("platform support status should load");
    assert!(!unsupported.tun.capable);
    assert_eq!(
        unsupported.tun.reason,
        Some(CoreRuntimeTunReason::Unsupported)
    );
}

#[test]
fn restart_backoff_grows_exponentially_and_stays_at_the_versioned_cap() {
    let harness = Harness::with_limits(8, 4, 8);
    let session = harness.open();
    let first = harness
        .service
        .apply_candidate(&session.proof, &harness.bundle(1))
        .expect("the initial Core should spawn");
    harness.processes.mark_exited(first.managed_core.pid);
    harness
        .processes
        .script_spawns(std::iter::repeat_n(SpawnScript::Failure, 6));

    let mut deadline = pending_attempt_deadline(
        harness
            .service
            .maintenance_step(Duration::ZERO)
            .expect("maintenance should schedule recovery")
            .outcome,
    );
    let expected_delays = [
        Duration::from_secs(2),
        Duration::from_secs(4),
        Duration::from_secs(8),
        Duration::from_secs(16),
        CORE_RESTART_MAX_BACKOFF,
        CORE_RESTART_MAX_BACKOFF,
    ];
    for (index, expected_delay) in expected_delays.into_iter().enumerate() {
        let step = harness
            .service
            .maintenance_step(deadline)
            .expect("a due maintenance step should make one restart attempt");
        let next_deadline = pending_attempt_deadline(step.outcome);
        assert_eq!(next_deadline - deadline, expected_delay);
        deadline = next_deadline;
        assert_eq!(
            harness.processes.spawn_count.load(Ordering::Relaxed),
            index + 2,
            "each due maintenance step should make one spawn attempt"
        );
    }
}

#[test]
fn successful_restart_resets_retry_state_for_the_next_exit() {
    let harness = Harness::with_limits(3, 4, 8);
    let session = harness.open();
    let first = harness
        .service
        .apply_candidate(&session.proof, &harness.bundle(1))
        .expect("the initial Core should spawn");
    harness.processes.mark_exited(first.managed_core.pid);
    harness.processes.script_spawns([
        SpawnScript::Failure,
        SpawnScript::Success,
        SpawnScript::Success,
    ]);

    let first_deadline = pending_attempt_deadline(ServiceMaintenanceOutcome::UnexpectedExit(
        harness
            .service
            .handle_unexpected_exit_at(
                &session.proof,
                &exit_identity(&first.managed_core),
                Duration::ZERO,
            )
            .expect("the first exit should schedule recovery"),
    ));
    let retry = harness
        .service
        .maintenance_step(first_deadline)
        .expect("the first restart attempt should fail");
    let recovered = harness
        .service
        .maintenance_step(pending_attempt_deadline(retry.outcome))
        .expect("the second restart attempt should recover");
    let ServiceMaintenanceOutcome::UnexpectedExit(UnexpectedExitOutcome::Restarted {
        managed_core,
        attempts: 2,
    }) = recovered.outcome
    else {
        panic!("the second restart attempt should succeed");
    };
    let snapshot = harness
        .service
        .snapshot(&session.proof)
        .expect("the recovered snapshot should load");
    assert_eq!(snapshot.consecutive_restart_failures, 0);
    assert_eq!(snapshot.diagnostic, None);

    harness.processes.mark_exited(managed_core.pid);
    let second_exit_at = Duration::from_secs(10);
    let next_deadline = match harness
        .service
        .handle_unexpected_exit_at(
            &session.proof,
            &exit_identity(&managed_core),
            second_exit_at,
        )
        .expect("the second exit should schedule recovery")
    {
        UnexpectedExitOutcome::Pending {
            attempts: 0,
            next_attempt_at,
        } => next_attempt_at,
        outcome => panic!("the second exit should start a fresh policy: {outcome:?}"),
    };
    assert_eq!(next_deadline, second_exit_at + CORE_RESTART_INITIAL_BACKOFF);
    assert!(matches!(
        harness
            .service
            .maintenance_step(next_deadline)
            .expect("the fresh first attempt should recover")
            .outcome,
        ServiceMaintenanceOutcome::UnexpectedExit(UnexpectedExitOutcome::Restarted {
            attempts: 1,
            ..
        })
    ));
}

#[test]
fn successful_apply_clears_a_degraded_restart_policy() {
    let harness = Harness::with_limits(1, 4, 8);
    let session = harness.open();
    let first = harness
        .service
        .apply_candidate(&session.proof, &harness.bundle(1))
        .expect("the initial Core should spawn");
    harness.processes.mark_exited(first.managed_core.pid);
    harness.processes.script_spawns([SpawnScript::Failure]);
    let first_deadline = pending_attempt_deadline(
        harness
            .service
            .maintenance_step(Duration::ZERO)
            .expect("maintenance should schedule recovery")
            .outcome,
    );
    assert!(matches!(
        harness
            .service
            .maintenance_step(first_deadline)
            .expect("the restart attempt should reach the bound")
            .outcome,
        ServiceMaintenanceOutcome::UnexpectedExit(UnexpectedExitOutcome::Degraded { .. })
    ));

    let applied = harness
        .service
        .apply_candidate(&session.proof, &harness.bundle(2))
        .expect("a valid apply should recover the service");
    let snapshot = harness
        .service
        .snapshot(&session.proof)
        .expect("the recovered snapshot should load");
    assert_eq!(applied.disposition, ApplyDisposition::Spawned);
    assert_eq!(snapshot.lifecycle, PrivilegedServiceLifecycle::Running);
    assert_eq!(snapshot.consecutive_restart_failures, 0);
    assert_eq!(snapshot.diagnostic, None);
}

#[test]
fn a_new_owner_starts_with_fresh_restart_state() {
    let harness = Harness::with_limits(1, 4, 8);
    let first_session = harness.open();
    let first = harness
        .service
        .apply_candidate(&first_session.proof, &harness.bundle(1))
        .expect("the initial Core should spawn");
    harness.processes.mark_exited(first.managed_core.pid);
    harness.processes.script_spawns([SpawnScript::Failure]);
    let first_deadline = pending_attempt_deadline(
        harness
            .service
            .maintenance_step(Duration::ZERO)
            .expect("maintenance should schedule recovery")
            .outcome,
    );
    harness
        .service
        .maintenance_step(first_deadline)
        .expect("the restart attempt should reach the bound");
    harness.identities.remove(100);

    let second_session = harness
        .service
        .open_owner_session(&harness.request(200, "supervisor-200", "instance-200"))
        .expect("a new owner should replace the stale owner");
    let snapshot = harness
        .service
        .snapshot(&second_session.proof)
        .expect("the new owner snapshot should load");
    assert_eq!(snapshot.lifecycle, PrivilegedServiceLifecycle::Owned);
    assert_eq!(snapshot.consecutive_restart_failures, 0);
    assert_eq!(snapshot.diagnostic, None);
}

#[cfg(unix)]
#[test]
fn owned_core_stop_removes_the_exact_recorded_endpoint() {
    let harness = Harness::new();
    let session = harness.open();
    let endpoint = session.endpoint.socket_path.clone();
    let listener = UnixListener::bind(&endpoint).expect("the endpoint fixture should bind");
    harness
        .service
        .apply_candidate(&session.proof, &harness.bundle(1))
        .expect("the Core should spawn");

    harness
        .service
        .stop(&session.proof)
        .expect("the owned Core should stop");

    assert!(!endpoint.exists());
    drop(listener);
}

#[cfg(unix)]
#[test]
fn owner_cleanup_preserves_a_replacement_endpoint() {
    let harness = Harness::new();
    let session = harness.open();
    let endpoint = session.endpoint.socket_path.clone();
    let original = UnixListener::bind(&endpoint).expect("the original endpoint should bind");
    harness
        .service
        .apply_candidate(&session.proof, &harness.bundle(1))
        .expect("the Core should spawn");
    drop(original);
    fs::remove_file(&endpoint).expect("the original endpoint should be removed");
    let replacement = UnixListener::bind(&endpoint).expect("the replacement endpoint should bind");

    harness
        .service
        .close_owner_session(&session.proof)
        .expect("owner cleanup should stop the Core");

    assert!(endpoint.exists());
    drop(replacement);
}

#[test]
fn service_shutdown_is_idempotent_and_clears_the_owner() {
    let harness = Harness::new();
    let session = harness.open();
    harness
        .service
        .apply_candidate(&session.proof, &harness.bundle(1))
        .expect("the Core should spawn");

    harness
        .service
        .shutdown_service()
        .expect("service shutdown should stop the owned Core");
    harness
        .service
        .shutdown_service()
        .expect("repeated service shutdown should be idempotent");

    assert_eq!(harness.processes.stop_count.load(Ordering::Relaxed), 1);
    assert_eq!(
        harness
            .service
            .status(&session.proof)
            .expect_err("shutdown should clear the owner proof")
            .kind,
        CoreRuntimeErrorKind::Authentication
    );
    assert_eq!(
        harness
            .service
            .maintenance_step(Duration::ZERO)
            .expect("maintenance should observe an idle service")
            .outcome,
        ServiceMaintenanceOutcome::Unchanged(PrivilegedServiceLifecycle::Idle)
    );
}

#[test]
fn exited_owner_cleanup_consumes_the_final_controller_log_batch() {
    let harness = Harness::new();
    let session = harness.open();
    let applied = harness
        .service
        .apply_candidate(&session.proof, &harness.bundle(1))
        .expect("the Core should spawn");
    harness.processes.push_logs([process_log(1, "final log")]);
    harness.processes.drop_logs(2);
    harness.processes.mark_exited(applied.managed_core.pid);

    harness
        .service
        .close_owner_session(&session.proof)
        .expect("owner cleanup should consume the exited Core log batch");

    let process_state = harness.processes.state.lock().expect("process lock");
    assert!(process_state.logs.is_empty());
    assert_eq!(process_state.dropped_logs, 0);
    assert_eq!(harness.processes.stop_count.load(Ordering::Relaxed), 0);
}

#[test]
fn tun_preflight_failure_prevents_process_changes() {
    let harness = Harness::new();
    let session = harness.open();
    harness.tun.deny(true);

    let error = harness
        .service
        .apply_candidate(&session.proof, &harness.bundle(1))
        .expect_err("TUN capability should be required");

    assert_eq!(error.kind, CoreRuntimeErrorKind::TunPermissionDenied);
    assert_eq!(harness.processes.spawn_count.load(Ordering::Relaxed), 0);
}

#[test]
fn runtime_bundle_checks_root_manifest_policy_binary_and_configuration_identity() {
    let harness = Harness::new();
    let session = harness.open();

    let mut bad_policy = harness.bundle(1);
    bad_policy.compiler_policy_sha256 = sha256(b"different-policy");
    assert_invalid_bundle(&harness.service, &session.proof, &bad_policy);

    let mut bad_manifest = harness.bundle(2);
    bad_manifest.manifest_sha256 = sha256(b"different-manifest");
    assert_invalid_bundle(&harness.service, &session.proof, &bad_manifest);

    let changed_binary = harness.bundle(3);
    fs::write(changed_binary.generation_root.join("mihomo"), b"changed")
        .expect("the binary fixture should change");
    assert_invalid_bundle(&harness.service, &session.proof, &changed_binary);

    let changed_config = harness.bundle(4);
    fs::write(
        changed_config.generation_root.join("config.yaml"),
        b"changed: true\n",
    )
    .expect("the configuration fixture should change");
    assert_invalid_bundle(&harness.service, &session.proof, &changed_config);

    let outside = write_bundle(
        &harness.directory.path.join("outside-generation"),
        RuntimeGeneration(5),
        &harness.policy_sha256,
        &harness.binary,
        &harness.binary_sha256,
    );
    assert_invalid_bundle(&harness.service, &session.proof, &outside);
    assert_eq!(harness.processes.spawn_count.load(Ordering::Relaxed), 0);
}

#[test]
fn runtime_bundle_rejects_changed_local_provider_content() {
    let harness = Harness::new();
    let session = harness.open();
    let mut bundle = harness.bundle(1);
    let provider_path = bundle.generation_root.join("providers/local.yaml");
    fs::create_dir_all(
        provider_path
            .parent()
            .expect("the provider fixture should have a parent"),
    )
    .expect("the provider fixture directory should be created");
    let original_provider = b"payload: []\n";
    fs::write(&provider_path, original_provider).expect("the provider fixture should be written");
    let configuration = fs::read(bundle.generation_root.join("config.yaml"))
        .expect("the configuration fixture should be readable");
    let manifest = RuntimeManifestV1::new(
        bundle.generation,
        &bundle.compiler_policy_sha256,
        &bundle.mihomo_binary_sha256,
        sha256(&configuration),
    )
    .with_provider_files(vec![RuntimeManifestFileV1 {
        path: "providers/local.yaml".to_owned(),
        sha256: sha256(original_provider),
        size: original_provider.len() as u64,
    }]);
    let manifest_bytes = serde_json::to_vec(&manifest).expect("the manifest should serialize");
    fs::write(
        bundle.generation_root.join("manifest.json"),
        &manifest_bytes,
    )
    .expect("the manifest fixture should be updated");
    bundle.manifest_sha256 = sha256(&manifest_bytes);
    fs::write(provider_path, b"changed: true\n").expect("the provider fixture should be changed");

    assert_invalid_bundle(&harness.service, &session.proof, &bundle);
    assert_eq!(harness.processes.spawn_count.load(Ordering::Relaxed), 0);
}

#[cfg(unix)]
#[test]
fn runtime_bundle_requires_an_executable_mihomo_binary() {
    use std::os::unix::fs::PermissionsExt as _;

    let harness = Harness::new();
    let session = harness.open();
    let bundle = harness.bundle(1);
    let executable = bundle.generation_root.join("mihomo");
    let mut permissions = fs::metadata(&executable)
        .expect("the Mihomo fixture metadata should load")
        .permissions();
    permissions.set_mode(0o600);
    fs::set_permissions(executable, permissions)
        .expect("the Mihomo fixture permissions should change");

    assert_invalid_bundle(&harness.service, &session.proof, &bundle);
    assert_eq!(harness.processes.spawn_count.load(Ordering::Relaxed), 0);
}

#[cfg(unix)]
#[test]
fn runtime_bundle_rejects_symlink_escape() {
    let harness = Harness::new();
    let session = harness.open();
    let bundle = harness.bundle(1);
    let external = harness.directory.path.join("external-config.yaml");
    fs::write(&external, b"mode: rule\n").expect("the external fixture should be written");
    fs::remove_file(bundle.generation_root.join("config.yaml"))
        .expect("the contained fixture should be removed");
    symlink(&external, bundle.generation_root.join("config.yaml"))
        .expect("the escape symlink should be created");

    assert_invalid_bundle(&harness.service, &session.proof, &bundle);
}

#[test]
fn owned_process_identity_is_checked_before_stop() {
    let harness = Harness::new();
    let session = harness.open();
    let applied = harness
        .service
        .apply_candidate(&session.proof, &harness.bundle(1))
        .expect("the Core should spawn");
    harness.identities.set(
        applied.managed_core.pid,
        "reused-pid-with-different-start-identity",
    );

    let error = harness
        .service
        .stop(&session.proof)
        .expect_err("a reused PID should be rejected");

    assert_eq!(error.kind, CoreRuntimeErrorKind::ProcessIdentityMismatch);
    assert_eq!(harness.processes.stop_count.load(Ordering::Relaxed), 0);
}

#[test]
fn bounded_log_forwarding_evicts_old_records_and_truncates_utf8_safely() {
    let harness = Harness::with_limits(3, 2, 5);
    let session = harness.open();
    harness
        .service
        .apply_candidate(&session.proof, &harness.bundle(1))
        .expect("the Core should spawn");
    harness
        .processes
        .push_logs([process_log(1, "first"), process_log(2, "second-long")]);
    let first = harness
        .service
        .logs(&session.proof, None, usize::MAX)
        .expect("the first log batch should load");
    assert_eq!(first.records.len(), 2);
    assert_eq!(first.records[1].message, "secon");

    harness
        .processes
        .push_logs([process_log(3, "ééé"), process_log(4, "fourth")]);
    harness.processes.drop_logs(3);
    let second = harness
        .service
        .logs(&session.proof, Some(0), usize::MAX)
        .expect("the bounded log tail should load");

    assert_eq!(second.records.len(), 2);
    assert_eq!(second.records[0].sequence, 6);
    assert_eq!(second.records[0].message, "éé");
    assert_eq!(second.records[1].message, "fourt");
    assert_eq!(second.dropped_before, 5);
    assert_eq!(second.next_sequence, Some(7));
}

#[test]
fn spawn_failure_is_distinct_from_readiness_failure() {
    let harness = Harness::new();
    let session = harness.open();
    harness.processes.script_spawns([SpawnScript::Failure]);

    let error = harness
        .service
        .apply_candidate(&session.proof, &harness.bundle(1))
        .expect_err("spawn should fail");

    assert_eq!(error.kind, CoreRuntimeErrorKind::Apply);
    assert_eq!(harness.processes.stop_count.load(Ordering::Relaxed), 0);
}

#[test]
fn spawn_readiness_failure_stops_the_fixture_and_keeps_service_owned_only() {
    let harness = Harness::new();
    let session = harness.open();
    harness
        .processes
        .fail_readiness(ServicePlatformErrorKind::ReadinessTimeout);

    let error = harness
        .service
        .apply_candidate(&session.proof, &harness.bundle(1))
        .expect_err("spawn readiness should fail");

    assert_eq!(error.kind, CoreRuntimeErrorKind::Readiness);
    assert_eq!(harness.processes.stop_count.load(Ordering::Relaxed), 1);
    assert!(
        harness
            .service
            .status(&session.proof)
            .expect("status should remain available")
            .managed_core
            .is_none()
    );
}

#[test]
fn reload_timeout_preserves_the_recorded_previous_generation() {
    let harness = Harness::new();
    let session = harness.open();
    let first = harness
        .service
        .apply_candidate(&session.proof, &harness.bundle(1))
        .expect("the initial Core should spawn");
    harness
        .processes
        .fail_reload(ServicePlatformErrorKind::ReloadTimeout);

    let error = harness
        .service
        .apply_candidate(&session.proof, &harness.bundle(2))
        .expect_err("the reload should time out");

    assert_eq!(error.kind, CoreRuntimeErrorKind::ReloadTimeout);
    let status = harness
        .service
        .status(&session.proof)
        .expect("status should retain the previous record")
        .managed_core
        .expect("the previous Core should remain recorded");
    assert_eq!(status.runtime_generation, RuntimeGeneration(1));
    assert_eq!(
        status.instance_generation,
        first.managed_core.instance_generation
    );
}

#[test]
fn unexpected_exit_restarts_with_monotonic_instance_generation() {
    let harness = Harness::with_limits(3, 4, 8);
    let session = harness.open();
    let first = harness
        .service
        .apply_candidate(&session.proof, &harness.bundle(1))
        .expect("the initial Core should spawn");
    let exit = exit_identity(&first.managed_core);
    harness.processes.mark_exited(first.managed_core.pid);
    harness
        .processes
        .script_spawns([SpawnScript::Failure, SpawnScript::Success]);

    let outcome = harness
        .service
        .handle_unexpected_exit_at(&session.proof, &exit, Duration::ZERO)
        .expect("the unexpected exit should schedule recovery");
    assert_eq!(
        outcome,
        UnexpectedExitOutcome::Pending {
            attempts: 0,
            next_attempt_at: CORE_RESTART_INITIAL_BACKOFF,
        }
    );
    let retry = harness
        .service
        .maintenance_step(CORE_RESTART_INITIAL_BACKOFF)
        .expect("the first restart attempt should retain retry state");
    let next_deadline = retry.next_deadline;
    let outcome = harness
        .service
        .maintenance_step(next_deadline)
        .expect("the second restart attempt should recover")
        .outcome;

    let ServiceMaintenanceOutcome::UnexpectedExit(UnexpectedExitOutcome::Restarted {
        attempts,
        managed_core,
    }) = outcome
    else {
        panic!("the fixture should restart");
    };
    assert_eq!(attempts, 2);
    assert_eq!(managed_core.instance_generation, CoreInstanceGeneration(3));
    assert_eq!(managed_core.runtime_generation, RuntimeGeneration(1));
}

#[test]
fn repeated_restart_failure_enters_degraded_state_at_the_bound() {
    let harness = Harness::with_limits(2, 4, 8);
    let session = harness.open();
    let first = harness
        .service
        .apply_candidate(&session.proof, &harness.bundle(1))
        .expect("the initial Core should spawn");
    let exit = exit_identity(&first.managed_core);
    harness.processes.mark_exited(first.managed_core.pid);
    harness
        .processes
        .script_spawns([SpawnScript::Failure, SpawnScript::Failure]);

    let outcome = harness
        .service
        .handle_unexpected_exit_at(&session.proof, &exit, Duration::ZERO)
        .expect("the restart policy should schedule recovery");
    let first_deadline = match outcome {
        UnexpectedExitOutcome::Pending {
            next_attempt_at, ..
        } => next_attempt_at,
        outcome => panic!("the restart should be pending: {outcome:?}"),
    };
    let retry = harness
        .service
        .maintenance_step(first_deadline)
        .expect("the first restart attempt should retain retry state");
    let outcome = harness
        .service
        .maintenance_step(retry.next_deadline)
        .expect("the restart policy should reach its bound")
        .outcome;

    assert_eq!(
        outcome,
        ServiceMaintenanceOutcome::UnexpectedExit(UnexpectedExitOutcome::Degraded {
            attempts: 2,
            diagnostic: CoreRuntimeDiagnosticCategory::CoreRestartLimitReached,
        })
    );
    let snapshot = harness
        .service
        .snapshot(&session.proof)
        .expect("the degraded snapshot should be visible");
    assert_eq!(snapshot.lifecycle, PrivilegedServiceLifecycle::Degraded);
    assert_eq!(snapshot.consecutive_restart_failures, 2);
    assert_eq!(
        snapshot.diagnostic,
        Some(CoreRuntimeDiagnosticCategory::CoreRestartLimitReached)
    );
    assert_eq!(
        snapshot
            .diagnostic
            .map(CoreRuntimeDiagnosticCategory::as_str),
        Some("core_restart_limit_reached")
    );
    assert!(snapshot.managed_core.is_none());
    assert_eq!(harness.processes.spawn_count.load(Ordering::Relaxed), 3);
}

#[test]
fn unexpected_exit_requires_the_exact_owned_identity() {
    let harness = Harness::new();
    let session = harness.open();
    let applied = harness
        .service
        .apply_candidate(&session.proof, &harness.bundle(1))
        .expect("the Core should spawn");
    let mut wrong = exit_identity(&applied.managed_core);
    wrong.instance_generation = CoreInstanceGeneration(999);

    let error = harness
        .service
        .handle_unexpected_exit(&session.proof, &wrong)
        .expect_err("a stale exit notification should be rejected");

    assert_eq!(error.kind, CoreRuntimeErrorKind::ProcessIdentityMismatch);
    assert_eq!(harness.processes.spawn_count.load(Ordering::Relaxed), 1);
}

#[test]
fn unexpected_exit_requires_the_owned_process_to_have_exited() {
    let harness = Harness::new();
    let session = harness.open();
    let applied = harness
        .service
        .apply_candidate(&session.proof, &harness.bundle(1))
        .expect("the Core should spawn");
    let exit = exit_identity(&applied.managed_core);

    let error = harness
        .service
        .handle_unexpected_exit(&session.proof, &exit)
        .expect_err("a live process should reject an exit notification");

    assert_eq!(error.kind, CoreRuntimeErrorKind::ProcessIdentityMismatch);
    assert_eq!(harness.processes.spawn_count.load(Ordering::Relaxed), 1);
}

#[test]
fn fixture_subprocess_supports_spawn_reload_readiness_and_owned_stop() {
    let directory = TestDirectory::new();
    let service_root = directory.path.join("fixture-service");
    let policy_sha256 = sha256(b"fixture-policy");
    let binary = b"fixture-binary".to_vec();
    let binary_sha256 = sha256(&binary);
    let identities = IdentityRegistry::default();
    identities.set(700, "fixture-supervisor");
    let processes = FixtureProcesses::new(identities.clone());
    let service = PrivilegedCoreRuntimeService::new(
        PrivilegedServiceConfig {
            protocol_version: CORE_RUNTIME_PROTOCOL_VERSION,
            service_owned_root: service_root.clone(),
            compiler_policy_sha256: policy_sha256.clone(),
            mihomo_binary_sha256: binary_sha256.clone(),
            restart_limit: 2,
            log_capacity: 4,
            max_log_line_bytes: 64,
        },
        PrivilegedServiceDependencies {
            credentials: Box::new(FakeCredentials::default()),
            identities: Box::new(identities),
            tun: Box::new(FakeTun::default()),
            secrets: Box::new(CountingSecrets::default()),
            processes: Box::new(processes.clone()),
        },
    )
    .expect("the fixture service should initialize");
    let session = service
        .open_owner_session(&OwnerSessionRequest {
            owner_uid: 501,
            supervisor_pid: 700,
            supervisor_start_identity: "fixture-supervisor".to_owned(),
            instance_token: "fixture-instance".to_owned(),
            protocol_version: CORE_RUNTIME_PROTOCOL_VERSION,
        })
        .expect("the fixture owner should open");
    let first_bundle = write_bundle(
        &service_root.join("generation-1"),
        RuntimeGeneration(1),
        &policy_sha256,
        &binary,
        &binary_sha256,
    );
    let second_bundle = write_bundle(
        &service_root.join("generation-2"),
        RuntimeGeneration(2),
        &policy_sha256,
        &binary,
        &binary_sha256,
    );

    let spawned = service
        .apply_candidate(&session.proof, &first_bundle)
        .expect("the fixture subprocess should spawn");
    let reloaded = service
        .apply_candidate(&session.proof, &second_bundle)
        .expect("the fixture subprocess should reload");
    let stopped = service
        .stop(&session.proof)
        .expect("the fixture subprocess should stop");

    assert_eq!(spawned.disposition, ApplyDisposition::Spawned);
    assert_eq!(reloaded.disposition, ApplyDisposition::Reloaded);
    assert_eq!(spawned.managed_core.pid, reloaded.managed_core.pid);
    assert_eq!(
        spawned.managed_core.instance_generation,
        reloaded.managed_core.instance_generation
    );
    assert!(stopped.stopped);
    assert_eq!(processes.child_count(), 0);
}

fn write_bundle(
    root: &Path,
    generation: RuntimeGeneration,
    policy_sha256: &str,
    binary: &[u8],
    binary_sha256: &str,
) -> RuntimeBundle {
    fs::create_dir_all(root).expect("the generation root should be created");
    let configuration = format!("mode: rule\ngeneration: {}\n", generation.0);
    fs::write(root.join("config.yaml"), configuration.as_bytes())
        .expect("the fixture configuration should be written");
    let executable = root.join("mihomo");
    fs::write(&executable, binary).expect("the fixture binary should be written");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;

        let mut permissions = fs::metadata(&executable)
            .expect("the fixture binary metadata should load")
            .permissions();
        permissions.set_mode(0o700);
        fs::set_permissions(&executable, permissions)
            .expect("the fixture binary should be executable");
    }
    let manifest = RuntimeManifestV1::new(
        generation,
        policy_sha256,
        binary_sha256,
        sha256(configuration.as_bytes()),
    );
    let manifest_bytes = serde_json::to_vec(&manifest).expect("the manifest should serialize");
    fs::write(root.join("manifest.json"), &manifest_bytes)
        .expect("the fixture manifest should be written");
    RuntimeBundle {
        generation,
        generation_root: root.to_owned(),
        manifest_sha256: sha256(&manifest_bytes),
        compiler_policy_sha256: policy_sha256.to_owned(),
        mihomo_binary_sha256: binary_sha256.to_owned(),
    }
}

fn assert_invalid_bundle(
    service: &PrivilegedCoreRuntimeService,
    proof: &OwnerSessionProof,
    bundle: &RuntimeBundle,
) {
    assert_eq!(
        service
            .apply_candidate(proof, bundle)
            .expect_err("the runtime bundle should be rejected")
            .kind,
        CoreRuntimeErrorKind::InvalidBundle
    );
}

fn process_log(timestamp_unix_ms: u64, message: &str) -> CoreProcessLog {
    CoreProcessLog {
        timestamp_unix_ms,
        source: ProcessOutputSource::Stdout,
        message: message.to_owned(),
    }
}

fn exit_identity(handle: &hopash::core::ManagedCoreHandle) -> CoreExitIdentity {
    CoreExitIdentity {
        pid: handle.pid,
        process_start_identity: handle.process_start_identity.clone(),
        instance_generation: handle.instance_generation,
    }
}

fn pending_attempt_deadline(outcome: ServiceMaintenanceOutcome) -> Duration {
    match outcome {
        ServiceMaintenanceOutcome::UnexpectedExit(UnexpectedExitOutcome::Pending {
            next_attempt_at,
            ..
        }) => next_attempt_at,
        outcome => panic!("the restart should remain pending: {outcome:?}"),
    }
}

fn sha256(content: &[u8]) -> String {
    Sha256::digest(content)
        .iter()
        .fold(String::with_capacity(64), |mut output, byte| {
            use std::fmt::Write as _;
            write!(output, "{byte:02x}").expect("writing to a String should succeed");
            output
        })
}

#[derive(Clone)]
struct FixtureProcesses {
    inner: Arc<FixtureProcessState>,
    identities: IdentityRegistry,
}

struct FixtureProcessState {
    children: Mutex<BTreeMap<u32, Child>>,
}

impl Drop for FixtureProcessState {
    fn drop(&mut self) {
        for child in self
            .children
            .get_mut()
            .expect("fixture process lock")
            .values_mut()
        {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

impl FixtureProcesses {
    fn new(identities: IdentityRegistry) -> Self {
        Self {
            inner: Arc::new(FixtureProcessState {
                children: Mutex::new(BTreeMap::new()),
            }),
            identities,
        }
    }

    fn child_count(&self) -> usize {
        self.inner
            .children
            .lock()
            .expect("fixture process lock")
            .len()
    }
}

impl CoreProcessController for FixtureProcesses {
    fn spawn(
        &self,
        _bundle: &VerifiedRuntimeBundle,
        _endpoint: &CoreControlEndpoint,
        instance_generation: CoreInstanceGeneration,
    ) -> Result<SpawnedCoreProcess, ServicePlatformError> {
        let child = Command::new("/usr/bin/tail")
            .args(["-f", "/dev/null"])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .map_err(|_| ServicePlatformError::new(ServicePlatformErrorKind::Spawn))?;
        let pid = child.id();
        let identity = format!("fixture-{pid}-{}", instance_generation.0);
        self.identities.set(pid, identity.clone());
        self.inner
            .children
            .lock()
            .expect("fixture process lock")
            .insert(pid, child);
        Ok(SpawnedCoreProcess {
            pid,
            process_start_identity: identity,
        })
    }

    fn reload(
        &self,
        process: &OwnedProcessIdentity,
        _bundle: &VerifiedRuntimeBundle,
    ) -> Result<(), ServicePlatformError> {
        let mut children = self.inner.children.lock().expect("fixture process lock");
        let child = children
            .get_mut(&process.pid)
            .ok_or_else(|| ServicePlatformError::new(ServicePlatformErrorKind::Reload))?;
        if child
            .try_wait()
            .map_err(|_| ServicePlatformError::new(ServicePlatformErrorKind::Reload))?
            .is_none()
        {
            Ok(())
        } else {
            Err(ServicePlatformError::new(ServicePlatformErrorKind::Reload))
        }
    }

    fn stop(&self, process: &OwnedProcessIdentity) -> Result<(), ServicePlatformError> {
        let mut child = self
            .inner
            .children
            .lock()
            .expect("fixture process lock")
            .remove(&process.pid)
            .ok_or_else(|| ServicePlatformError::new(ServicePlatformErrorKind::Stop))?;
        child
            .kill()
            .map_err(|_| ServicePlatformError::new(ServicePlatformErrorKind::Stop))?;
        child
            .wait()
            .map_err(|_| ServicePlatformError::new(ServicePlatformErrorKind::Stop))?;
        self.identities.remove(process.pid);
        Ok(())
    }

    fn readiness(
        &self,
        process: &OwnedProcessIdentity,
        _endpoint: &CoreControlEndpoint,
    ) -> Result<(), ServicePlatformError> {
        let mut children = self.inner.children.lock().expect("fixture process lock");
        let child = children
            .get_mut(&process.pid)
            .ok_or_else(|| ServicePlatformError::new(ServicePlatformErrorKind::Readiness))?;
        if child
            .try_wait()
            .map_err(|_| ServicePlatformError::new(ServicePlatformErrorKind::Readiness))?
            .is_none()
        {
            Ok(())
        } else {
            Err(ServicePlatformError::new(
                ServicePlatformErrorKind::Readiness,
            ))
        }
    }

    fn grant_endpoint_access(
        &self,
        _endpoint: &CoreControlEndpoint,
        _owner_uid: u32,
    ) -> Result<(), ServicePlatformError> {
        Ok(())
    }

    fn reap_if_exited(&self, process: &OwnedProcessIdentity) -> Result<bool, ServicePlatformError> {
        let mut children = self.inner.children.lock().expect("fixture process lock");
        let child = children.get_mut(&process.pid).ok_or_else(|| {
            ServicePlatformError::new(ServicePlatformErrorKind::ProcessInspection)
        })?;
        let exited = child
            .try_wait()
            .map_err(|_| ServicePlatformError::new(ServicePlatformErrorKind::ProcessInspection))?
            .is_some();
        if exited {
            children.remove(&process.pid);
            self.identities.remove(process.pid);
        }
        Ok(exited)
    }

    fn take_logs(
        &self,
        _process: &OwnedProcessIdentity,
        _limit: usize,
    ) -> Result<CoreProcessLogBatch, ServicePlatformError> {
        Ok(CoreProcessLogBatch {
            records: Vec::new(),
            dropped: 0,
        })
    }
}
