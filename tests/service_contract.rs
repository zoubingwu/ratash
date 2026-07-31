use hopash::core::{
    ApplyDisposition, CoreControlEndpoint, CoreRuntime, CoreRuntimeErrorKind, OwnerSessionProof,
    OwnerSessionRequest, ProcessOutputSource, RuntimeBundle,
};
use hopash::domain::{CoreInstanceGeneration, RuntimeGeneration};
use hopash::service::{
    CORE_RUNTIME_PROTOCOL_VERSION, CallerCredentialValidator, CoreExitIdentity,
    CoreProcessController, CoreProcessLog, OwnedProcessIdentity, PrivilegedCoreRuntimeService,
    PrivilegedServiceConfig, PrivilegedServiceDependencies, PrivilegedServiceLifecycle,
    ProcessIdentityProbe, RuntimeManifestV1, SecretGenerator, ServicePlatformError,
    ServicePlatformErrorKind, SpawnedCoreProcess, TunCapabilityPreflight, UnexpectedExitOutcome,
    VerifiedRuntimeBundle,
};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, VecDeque};
use std::fs;
#[cfg(unix)]
use std::os::unix::fs::symlink;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);

struct TestDirectory {
    path: PathBuf,
}

impl TestDirectory {
    fn new() -> Self {
        let sequence = NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
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
    denied: Arc<AtomicBool>,
}

impl FakeTun {
    fn deny(&self, denied: bool) {
        self.denied.store(denied, Ordering::Release);
    }
}

impl TunCapabilityPreflight for FakeTun {
    fn check(&self, _owner_uid: u32) -> Result<(), ServicePlatformError> {
        if self.denied.load(Ordering::Acquire) {
            Err(ServicePlatformError::new(
                ServicePlatformErrorKind::TunUnavailable,
            ))
        } else {
            Ok(())
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

    fn take_logs(
        &self,
        _process: &OwnedProcessIdentity,
        limit: usize,
    ) -> Result<Vec<CoreProcessLog>, ServicePlatformError> {
        let mut state = self.state.lock().expect("process lock");
        let take = limit.min(state.logs.len());
        Ok(state.logs.drain(..take).collect())
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
    let second = harness
        .service
        .logs(&session.proof, Some(0), usize::MAX)
        .expect("the bounded log tail should load");

    assert_eq!(second.records.len(), 2);
    assert_eq!(second.records[0].sequence, 3);
    assert_eq!(second.records[0].message, "éé");
    assert_eq!(second.records[1].message, "fourt");
    assert_eq!(second.dropped_before, 2);
    assert_eq!(second.next_sequence, Some(4));
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
        .handle_unexpected_exit(&session.proof, &exit)
        .expect("the unexpected exit should recover");

    let UnexpectedExitOutcome::Restarted {
        attempts,
        managed_core,
    } = outcome
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
        .handle_unexpected_exit(&session.proof, &exit)
        .expect("the restart policy should settle");

    assert_eq!(outcome, UnexpectedExitOutcome::Degraded { attempts: 2 });
    let snapshot = harness
        .service
        .snapshot(&session.proof)
        .expect("the degraded snapshot should be visible");
    assert_eq!(snapshot.lifecycle, PrivilegedServiceLifecycle::Degraded);
    assert_eq!(snapshot.consecutive_restart_failures, 2);
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

    fn take_logs(
        &self,
        _process: &OwnedProcessIdentity,
        _limit: usize,
    ) -> Result<Vec<CoreProcessLog>, ServicePlatformError> {
        Ok(Vec::new())
    }
}
