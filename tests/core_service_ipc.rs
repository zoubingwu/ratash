use std::collections::{BTreeMap, VecDeque};
use std::fs;
use std::io::{self, Read};
use std::os::unix::fs::{MetadataExt, PermissionsExt, symlink};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use hopash::core::{
    ApplyCandidateResult, ApplyDisposition, CoreControlEndpoint, CoreRuntime,
    CoreRuntimeDiagnosticCategory, CoreRuntimeError, CoreRuntimeErrorKind, CoreRuntimeLifecycle,
    CoreRuntimeRestartStatus, CoreRuntimeStatus, CoreRuntimeTunReason, CoreRuntimeTunStatus,
    ForwardedCoreLog, ForwardedCoreLogBatch, ManagedCoreHandle, OwnerSession, OwnerSessionProof,
    OwnerSessionRequest, ProcessOutputSource, RuntimeBundle, StopCoreResult,
};
use hopash::core_service_ipc::{
    CoreServiceClient, CoreServicePeerAuthorizer, CoreServicePeerIdentity, CoreServiceServer,
    CoreServiceServerConfig,
};
use hopash::domain::{CoreInstanceGeneration, RuntimeGeneration};
use hopash::ipc::{bind_private_listener, read_frame, write_frame};
use sha2::{Digest, Sha256};

static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);

struct RejectPeer {
    calls: Arc<AtomicUsize>,
}

impl CoreServicePeerAuthorizer for RejectPeer {
    fn authorize(&self, peer: &CoreServicePeerIdentity) -> io::Result<()> {
        assert_eq!(peer.uid(), nix::unistd::Uid::effective().as_raw());
        assert_eq!(peer.pid(), std::process::id());
        self.calls.fetch_add(1, Ordering::Relaxed);
        Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "fixture peer authorization rejection",
        ))
    }
}

struct TestDirectory {
    path: PathBuf,
}

impl TestDirectory {
    fn new() -> Self {
        let sequence = NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed);
        let path = Path::new("/private/tmp").join(format!("hcs-{}-{sequence}", std::process::id()));
        fs::create_dir(&path).expect("the fixture root should be created");
        Self { path }
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.path).expect("the fixture root should be removed");
    }
}

#[derive(Default)]
struct FakeRuntimeState {
    open_count: usize,
    apply_count: usize,
    status_count: usize,
    logs_count: usize,
    stop_count: usize,
    close_count: usize,
    managed_core: Option<ManagedCoreHandle>,
    staged_bundles: BTreeMap<u64, RuntimeBundle>,
    apply_delay: Option<Duration>,
    status_delay: Option<Duration>,
    status_diagnostic: Option<String>,
    runtime_status: Option<CoreRuntimeStatus>,
    oversized_logs: bool,
    apply_failures: VecDeque<CoreRuntimeErrorKind>,
}

struct FakeRuntime {
    runtime_root: PathBuf,
    session: OwnerSession,
    state: Mutex<FakeRuntimeState>,
    apply_started: AtomicUsize,
    apply_cancelled: AtomicBool,
    apply_wake: Condvar,
    cancel_count: AtomicUsize,
}

impl FakeRuntime {
    fn new(runtime_root: PathBuf, endpoint_root: &Path) -> Self {
        Self {
            runtime_root,
            session: OwnerSession {
                proof: OwnerSessionProof::new("owner-session-id", "owner-session-token"),
                protocol_version: 1,
                owner_generation: 7,
                endpoint: CoreControlEndpoint::new(
                    endpoint_root.join("core-control.sock"),
                    "core-control-secret",
                ),
            },
            state: Mutex::new(FakeRuntimeState::default()),
            apply_started: AtomicUsize::new(0),
            apply_cancelled: AtomicBool::new(false),
            apply_wake: Condvar::new(),
            cancel_count: AtomicUsize::new(0),
        }
    }

    fn require_owner(&self, owner: &OwnerSessionProof) -> Result<(), CoreRuntimeError> {
        if owner == &self.session.proof {
            Ok(())
        } else {
            Err(CoreRuntimeError::new(
                CoreRuntimeErrorKind::Authentication,
                "fixture owner proof mismatch",
            ))
        }
    }

    fn state(&self) -> std::sync::MutexGuard<'_, FakeRuntimeState> {
        self.state
            .lock()
            .expect("the fake runtime lock should work")
    }

    fn fail_next_apply(&self, kind: CoreRuntimeErrorKind) {
        self.state().apply_failures.push_back(kind);
    }

    fn set_runtime_status(&self, status: CoreRuntimeStatus) {
        self.state().runtime_status = Some(status);
    }
}

impl CoreRuntime for FakeRuntime {
    fn open_owner_session(
        &self,
        request: &OwnerSessionRequest,
    ) -> Result<OwnerSession, CoreRuntimeError> {
        self.apply_cancelled.store(false, Ordering::Release);
        self.state().open_count += 1;
        if request.protocol_version != 1 {
            return Err(CoreRuntimeError::new(
                CoreRuntimeErrorKind::ProtocolMismatch,
                "fixture protocol mismatch",
            ));
        }
        Ok(self.session.clone())
    }

    fn apply_candidate(
        &self,
        owner: &OwnerSessionProof,
        bundle: &RuntimeBundle,
    ) -> Result<ApplyCandidateResult, CoreRuntimeError> {
        self.require_owner(owner)?;
        assert!(bundle.generation_root.starts_with(&self.runtime_root));
        assert_eq!(
            fs::read(bundle.generation_root.join("config.yaml"))
                .expect("the staged configuration should be readable"),
            b"mode: rule\n"
        );
        assert_eq!(
            fs::read(bundle.generation_root.join("providers/local.yaml"))
                .expect("the staged provider should be readable"),
            b"payload: []\n"
        );
        self.apply_started.fetch_add(1, Ordering::Release);
        let delay = self.state().apply_delay;
        if let Some(delay) = delay {
            let state = self.state();
            let (_state, _wait) = self
                .apply_wake
                .wait_timeout_while(state, delay, |_| {
                    !self.apply_cancelled.load(Ordering::Acquire)
                })
                .expect("the fake Runtime Apply wait should remain available");
        }
        if self.apply_cancelled.load(Ordering::Acquire) {
            return Err(CoreRuntimeError::new(
                CoreRuntimeErrorKind::ReloadTimeout,
                "fixture Runtime Apply cancellation",
            ));
        }
        let mut state = self.state();
        let managed_core = ManagedCoreHandle {
            pid: 4_242,
            process_start_identity: "fixture-core-start".to_owned(),
            endpoint: self.session.endpoint.clone(),
            instance_generation: CoreInstanceGeneration(9),
            runtime_generation: bundle.generation,
        };
        state.apply_count += 1;
        if let Some(kind) = state.apply_failures.pop_front() {
            return Err(CoreRuntimeError::new(kind, "fixture Runtime Apply failure"));
        }
        state
            .staged_bundles
            .insert(bundle.generation.0, bundle.clone());
        state.managed_core = Some(managed_core.clone());
        Ok(ApplyCandidateResult {
            disposition: ApplyDisposition::Spawned,
            managed_core,
        })
    }

    fn status(&self, owner: &OwnerSessionProof) -> Result<CoreRuntimeStatus, CoreRuntimeError> {
        self.require_owner(owner)?;
        let (delay, diagnostic, runtime_status, managed_core) = {
            let mut state = self.state();
            state.status_count += 1;
            (
                state.status_delay,
                state.status_diagnostic.clone(),
                state.runtime_status.clone(),
                state.managed_core.clone(),
            )
        };
        if let Some(delay) = delay {
            std::thread::sleep(delay);
        }
        if let Some(diagnostic) = diagnostic {
            return Err(CoreRuntimeError::new(
                CoreRuntimeErrorKind::Unavailable,
                diagnostic,
            ));
        }
        Ok(runtime_status.unwrap_or_else(|| CoreRuntimeStatus::from_managed_core(managed_core)))
    }

    fn logs(
        &self,
        owner: &OwnerSessionProof,
        after_sequence: Option<u64>,
        limit: usize,
    ) -> Result<ForwardedCoreLogBatch, CoreRuntimeError> {
        self.require_owner(owner)?;
        let mut state = self.state();
        state.logs_count += 1;
        if state.oversized_logs {
            return Ok(ForwardedCoreLogBatch {
                records: (1..=32)
                    .map(|sequence| ForwardedCoreLog {
                        sequence,
                        timestamp_unix_ms: 123_456,
                        source: ProcessOutputSource::Stdout,
                        message: "\0".repeat(64 * 1_024 + 1),
                        instance_generation: CoreInstanceGeneration(9),
                    })
                    .collect(),
                next_sequence: Some(32),
                dropped_before: 0,
                dropped_since_after: 0,
            });
        }
        drop(state);
        assert_eq!(after_sequence, Some(40));
        assert_eq!(limit, 2);
        Ok(ForwardedCoreLogBatch {
            records: vec![ForwardedCoreLog {
                sequence: 41,
                timestamp_unix_ms: 123_456,
                source: ProcessOutputSource::Stderr,
                message: "fixture log".to_owned(),
                instance_generation: CoreInstanceGeneration(9),
            }],
            next_sequence: Some(41),
            dropped_before: 3,
            dropped_since_after: 0,
        })
    }

    fn stop(&self, owner: &OwnerSessionProof) -> Result<StopCoreResult, CoreRuntimeError> {
        self.require_owner(owner)?;
        let mut state = self.state();
        state.stop_count += 1;
        state.managed_core = None;
        Ok(StopCoreResult {
            stopped: true,
            instance_generation: Some(CoreInstanceGeneration(9)),
        })
    }

    fn close_owner_session(&self, owner: &OwnerSessionProof) -> Result<(), CoreRuntimeError> {
        self.require_owner(owner)?;
        self.state().close_count += 1;
        Ok(())
    }

    fn cancel_pending_apply(&self, owner: &OwnerSessionProof) -> Result<(), CoreRuntimeError> {
        self.require_owner(owner)?;
        self.cancel_count.fetch_add(1, Ordering::Relaxed);
        self.apply_cancelled.store(true, Ordering::Release);
        self.apply_wake.notify_all();
        Ok(())
    }
}

struct Harness {
    directory: TestDirectory,
    socket_path: PathBuf,
    runtime_root: PathBuf,
    runtime: Arc<FakeRuntime>,
    server: CoreServiceServer,
    client: CoreServiceClient,
}

impl Harness {
    fn new() -> Self {
        let directory = TestDirectory::new();
        let service_root = directory.path.join("service-owned");
        fs::create_dir(&service_root).expect("the service root should be created");
        let runtime_root = service_root.join("runtime");
        let socket_path = directory.path.join("ipc/core-runtime.sock");
        let runtime = Arc::new(FakeRuntime::new(runtime_root.clone(), &service_root));
        let owner_uid = nix::unistd::geteuid().as_raw();
        let server = CoreServiceServer::start(
            &socket_path,
            Arc::clone(&runtime),
            CoreServiceServerConfig::new(&runtime_root, owner_uid),
        )
        .expect("the Core service IPC server should start");
        let client = CoreServiceClient::for_service_uid(
            &socket_path,
            nix::unistd::Uid::effective().as_raw(),
        );
        Self {
            directory,
            socket_path,
            runtime_root,
            runtime,
            server,
            client,
        }
    }

    fn owner_request(&self) -> OwnerSessionRequest {
        OwnerSessionRequest {
            owner_uid: nix::unistd::geteuid().as_raw(),
            supervisor_pid: std::process::id(),
            supervisor_start_identity: "fixture-supervisor-start".to_owned(),
            instance_token: "fixture-instance-token".to_owned(),
            protocol_version: 1,
        }
    }

    fn source_bundle(&self, generation: u64) -> RuntimeBundle {
        write_bundle(
            &self.directory.path.join(format!("source-{generation}")),
            RuntimeGeneration(generation),
        )
    }
}

fn server_fixture(
    directory: &TestDirectory,
    label: &str,
) -> (PathBuf, Arc<FakeRuntime>, CoreServiceServerConfig) {
    let service_root = directory.path.join(format!("service-{label}"));
    fs::create_dir(&service_root).expect("the service fixture root should be created");
    let runtime_root = service_root.join("runtime");
    let socket_path = directory.path.join(format!("ipc-{label}/core.sock"));
    let runtime = Arc::new(FakeRuntime::new(runtime_root.clone(), &service_root));
    let config = CoreServiceServerConfig::new(runtime_root, nix::unistd::geteuid().as_raw());
    (socket_path, runtime, config)
}

#[test]
fn all_core_runtime_operations_round_trip_through_staged_service_owned_state() {
    let mut harness = Harness::new();
    let session = harness
        .client
        .open_owner_session(&harness.owner_request())
        .expect("the owner session should open");
    assert_eq!(session, harness.runtime.session);

    let source = harness.source_bundle(11);
    let applied = harness
        .client
        .apply_candidate(&session.proof, &source)
        .expect("the candidate should apply");
    assert_eq!(applied.disposition, ApplyDisposition::Spawned);
    assert_eq!(
        applied.managed_core.runtime_generation,
        RuntimeGeneration(11)
    );
    let staged_root = harness
        .runtime
        .state()
        .staged_bundles
        .get(&11)
        .expect("the staged bundle should be recorded")
        .generation_root
        .clone();
    assert!(staged_root.starts_with(&harness.runtime_root));
    assert_ne!(staged_root, source.generation_root);
    assert_eq!(
        fs::symlink_metadata(&harness.runtime_root)
            .expect("service runtime root metadata should load")
            .mode()
            & 0o777,
        0o711
    );
    assert_eq!(
        fs::symlink_metadata(&staged_root)
            .expect("staged generation metadata should load")
            .mode()
            & 0o777,
        0o700
    );
    assert_eq!(
        fs::symlink_metadata(staged_root.join("config.yaml"))
            .expect("staged configuration metadata should load")
            .mode()
            & 0o777,
        0o400
    );

    let status = harness
        .client
        .status(&session.proof)
        .expect("status should load");
    assert_eq!(status.managed_core, Some(applied.managed_core));
    let logs = harness
        .client
        .logs(&session.proof, Some(40), 2)
        .expect("logs should load");
    assert_eq!(logs.records[0].message, "fixture log");
    assert_eq!(logs.next_sequence, Some(41));
    assert_eq!(logs.dropped_before, 3);
    assert_eq!(logs.dropped_since_after, 0);
    let stopped = harness
        .client
        .stop(&session.proof)
        .expect("the Managed Core should stop");
    assert_eq!(stopped.instance_generation, Some(CoreInstanceGeneration(9)));
    harness
        .client
        .close_owner_session(&session.proof)
        .expect("the owner session should close");

    let error = harness
        .client
        .status(&session.proof)
        .expect_err("the closed session should be rejected by the transport binding");
    assert_eq!(error.kind, CoreRuntimeErrorKind::Authentication);
    let state = harness.runtime.state();
    assert_eq!(state.open_count, 1);
    assert_eq!(state.apply_count, 1);
    assert_eq!(state.status_count, 1);
    assert_eq!(state.logs_count, 1);
    assert_eq!(state.stop_count, 1);
    assert_eq!(state.close_count, 1);
    drop(state);
    let socket_metadata = fs::symlink_metadata(&harness.socket_path)
        .expect("the service socket metadata should load");
    let parent_metadata = fs::symlink_metadata(
        harness
            .socket_path
            .parent()
            .expect("the socket should have a parent"),
    )
    .expect("the service socket parent metadata should load");
    assert_eq!(socket_metadata.uid(), nix::unistd::geteuid().as_raw());
    assert_eq!(socket_metadata.mode() & 0o777, 0o600);
    assert_eq!(parent_metadata.uid(), nix::unistd::geteuid().as_raw());
    assert_eq!(parent_metadata.mode() & 0o777, 0o711);

    harness
        .server
        .shutdown()
        .expect("the server should shut down cleanly");
    assert!(!harness.socket_path.exists());
}

#[test]
fn runtime_status_round_trips_restart_and_tun_capability_details() {
    let harness = Harness::new();
    let session = harness
        .client
        .open_owner_session(&harness.owner_request())
        .expect("the owner session should open");
    let pending = CoreRuntimeStatus {
        managed_core: None,
        lifecycle: CoreRuntimeLifecycle::RestartPending,
        restart: CoreRuntimeRestartStatus {
            pending: true,
            attempts: 1,
            backoff: Some(Duration::from_secs(2)),
            diagnostic: None,
        },
        tun: CoreRuntimeTunStatus {
            capable: false,
            reason: Some(CoreRuntimeTunReason::PermissionDenied),
        },
    };
    harness.runtime.set_runtime_status(pending.clone());

    assert_eq!(
        harness
            .client
            .status(&session.proof)
            .expect("pending status should round trip"),
        pending
    );

    let degraded = CoreRuntimeStatus {
        managed_core: None,
        lifecycle: CoreRuntimeLifecycle::Degraded,
        restart: CoreRuntimeRestartStatus {
            pending: false,
            attempts: 3,
            backoff: None,
            diagnostic: Some(CoreRuntimeDiagnosticCategory::CoreRestartLimitReached),
        },
        tun: CoreRuntimeTunStatus {
            capable: false,
            reason: Some(CoreRuntimeTunReason::Unsupported),
        },
    };
    harness.runtime.set_runtime_status(degraded.clone());

    assert_eq!(
        harness
            .client
            .status(&session.proof)
            .expect("degraded status should round trip"),
        degraded
    );
}

#[test]
fn service_ingress_retains_only_two_successful_generations_and_one_candidate() {
    let harness = Harness::new();
    let session = harness
        .client
        .open_owner_session(&harness.owner_request())
        .expect("the owner session should open");

    for generation in 1..=5 {
        harness
            .client
            .apply_candidate(&session.proof, &harness.source_bundle(generation))
            .expect("the candidate should apply");
    }

    assert_eq!(
        service_generation_names(&harness.runtime_root),
        vec![3, 4, 5]
    );
}

#[test]
fn definite_apply_failure_discards_the_service_ingress_candidate() {
    let harness = Harness::new();
    let session = harness
        .client
        .open_owner_session(&harness.owner_request())
        .expect("the owner session should open");
    for generation in 1..=2 {
        harness
            .client
            .apply_candidate(&session.proof, &harness.source_bundle(generation))
            .expect("the candidate should apply");
    }
    harness.runtime.fail_next_apply(CoreRuntimeErrorKind::Apply);

    let error = harness
        .client
        .apply_candidate(&session.proof, &harness.source_bundle(3))
        .expect_err("the candidate should fail");

    assert_eq!(error.kind, CoreRuntimeErrorKind::Apply);
    assert_eq!(service_generation_names(&harness.runtime_root), vec![1, 2]);
}

#[test]
fn unsupported_tun_error_round_trips_through_the_service_wire() {
    let harness = Harness::new();
    let session = harness
        .client
        .open_owner_session(&harness.owner_request())
        .expect("the owner session should open");
    harness
        .runtime
        .fail_next_apply(CoreRuntimeErrorKind::TunUnsupported);

    let error = harness
        .client
        .apply_candidate(&session.proof, &harness.source_bundle(1))
        .expect_err("the unsupported TUN platform should reject Runtime Apply");

    assert_eq!(error.kind, CoreRuntimeErrorKind::TunUnsupported);
    assert!(service_generation_names(&harness.runtime_root).is_empty());
}

#[test]
fn service_startup_recovers_to_the_three_newest_strict_generations() {
    let directory = TestDirectory::new();
    let service_root = directory.path.join("service-owned");
    let runtime_root = service_root.join("runtime");
    fs::create_dir_all(&runtime_root).expect("the service runtime root should be created");
    fs::set_permissions(&runtime_root, fs::Permissions::from_mode(0o711))
        .expect("the service runtime root should be traversable");
    for generation in 1..=5 {
        let path = runtime_root.join(format!("generation-{generation:020}"));
        fs::create_dir(&path).expect("the service generation should be created");
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))
            .expect("the service generation should be private");
    }
    let socket_path = directory.path.join("ipc/core-runtime.sock");
    let runtime = Arc::new(FakeRuntime::new(runtime_root.clone(), &service_root));
    let mut server = CoreServiceServer::start(
        &socket_path,
        runtime,
        CoreServiceServerConfig::new(&runtime_root, nix::unistd::geteuid().as_raw()),
    )
    .expect("the Core service should recover strict historical generations");

    assert_eq!(service_generation_names(&runtime_root), vec![3, 4, 5]);
    server
        .shutdown()
        .expect("the recovered Core service should stop");
}

#[test]
fn unsafe_service_ingress_entry_blocks_startup_without_deleting_state() {
    let directory = TestDirectory::new();
    let service_root = directory.path.join("service-owned");
    let runtime_root = service_root.join("runtime");
    fs::create_dir_all(&runtime_root).expect("the service runtime root should be created");
    let generation = runtime_root.join("generation-00000000000000000001");
    fs::create_dir(&generation).expect("the service generation should be created");
    fs::set_permissions(&generation, fs::Permissions::from_mode(0o700))
        .expect("the service generation should be private");
    let unknown = runtime_root.join("unexpected-entry");
    fs::write(&unknown, b"preserve").expect("the unknown service entry should be written");
    let socket_path = directory.path.join("ipc/core-runtime.sock");
    let runtime = Arc::new(FakeRuntime::new(runtime_root.clone(), &service_root));

    let error = CoreServiceServer::start(
        &socket_path,
        runtime,
        CoreServiceServerConfig::new(&runtime_root, nix::unistd::geteuid().as_raw()),
    )
    .expect_err("an unsafe service Runtime Generation root should block startup");

    assert_eq!(error.kind(), std::io::ErrorKind::Other);
    assert!(generation.exists());
    assert_eq!(
        fs::read(unknown).expect("the unknown entry should remain"),
        b"preserve"
    );
    assert!(
        !error
            .to_string()
            .contains(&directory.path.display().to_string())
    );
    assert!(!socket_path.exists());
}

#[test]
fn peer_uid_must_match_the_claimed_owner_before_session_bootstrap() {
    let harness = Harness::new();
    let mut request = harness.owner_request();
    request.owner_uid = request.owner_uid.wrapping_add(1);

    let error = harness
        .client
        .open_owner_session(&request)
        .expect_err("a mismatched owner UID should be rejected");

    assert_eq!(error.kind, CoreRuntimeErrorKind::Authentication);
    assert_eq!(harness.runtime.state().open_count, 0);
}

#[test]
fn peer_pid_must_match_the_claimed_supervisor_before_session_bootstrap() {
    let harness = Harness::new();
    let mut request = harness.owner_request();
    request.supervisor_pid = request.supervisor_pid.wrapping_add(1);

    let error = harness
        .client
        .open_owner_session(&request)
        .expect_err("a mismatched Supervisor PID should be rejected");

    assert_eq!(error.kind, CoreRuntimeErrorKind::Authentication);
    assert_eq!(harness.runtime.state().open_count, 0);
}

#[test]
fn peer_authorization_rejection_precedes_request_dispatch() {
    let directory = TestDirectory::new();
    let (socket_path, runtime, config) = server_fixture(&directory, "peer-authorization");
    let calls = Arc::new(AtomicUsize::new(0));
    let _server = CoreServiceServer::start_with_peer_authorizer(
        &socket_path,
        Arc::clone(&runtime),
        config,
        Arc::new(RejectPeer {
            calls: Arc::clone(&calls),
        }),
    )
    .expect("the authorized service fixture should start");
    let client =
        CoreServiceClient::for_service_uid(socket_path, nix::unistd::Uid::effective().as_raw());

    let error = client
        .open_owner_session(&OwnerSessionRequest {
            owner_uid: nix::unistd::Uid::effective().as_raw(),
            supervisor_pid: std::process::id(),
            supervisor_start_identity: "fixture-start".to_owned(),
            instance_token: "fixture-instance".to_owned(),
            protocol_version: 1,
        })
        .expect_err("the rejected peer should receive a transport error");

    assert_eq!(error.kind, CoreRuntimeErrorKind::Unavailable);
    assert_eq!(calls.load(Ordering::Relaxed), 1);
    assert_eq!(runtime.state().open_count, 0);
}

#[test]
fn bound_session_proof_is_rejected_across_processes() {
    let harness = Harness::new();
    let session = harness
        .client
        .open_owner_session(&harness.owner_request())
        .expect("the owner session should open");
    let output = Command::new(std::env::current_exe().expect("the test executable should resolve"))
        .args(["--exact", "bound_session_child_attempt", "--nocapture"])
        .env("HOPASH_TEST_CORE_SERVICE_SOCKET", &harness.socket_path)
        .env(
            "HOPASH_TEST_CORE_SERVICE_UID",
            nix::unistd::Uid::effective().as_raw().to_string(),
        )
        .env(
            "HOPASH_TEST_CORE_SERVICE_SESSION_ID",
            session.proof.session_id(),
        )
        .env(
            "HOPASH_TEST_CORE_SERVICE_SESSION_TOKEN",
            session.proof.session_token(),
        )
        .output()
        .expect("the peer-process fixture should run");

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(harness.runtime.state().status_count, 0);
}

#[test]
fn bound_session_child_attempt() {
    let Ok(socket_path) = std::env::var("HOPASH_TEST_CORE_SERVICE_SOCKET") else {
        return;
    };
    let uid = std::env::var("HOPASH_TEST_CORE_SERVICE_UID")
        .expect("the service UID should be provided")
        .parse()
        .expect("the service UID should be numeric");
    let session_id = std::env::var("HOPASH_TEST_CORE_SERVICE_SESSION_ID")
        .expect("the session ID should be provided");
    let session_token = std::env::var("HOPASH_TEST_CORE_SERVICE_SESSION_TOKEN")
        .expect("the session token should be provided");
    let client = CoreServiceClient::for_service_uid(socket_path, uid);

    let error = client
        .status(&OwnerSessionProof::new(session_id, session_token))
        .expect_err("a different peer process should receive an authentication error");
    assert_eq!(error.kind, CoreRuntimeErrorKind::Authentication);
}

#[test]
fn client_rejects_a_core_service_owned_by_an_unexpected_uid() {
    let harness = Harness::new();
    let unexpected_uid = nix::unistd::Uid::effective().as_raw() ^ 1;
    let client = CoreServiceClient::for_service_uid(&harness.socket_path, unexpected_uid);

    let error = client
        .open_owner_session(&harness.owner_request())
        .expect_err("an unexpected service owner must fail before request delivery");

    assert_eq!(error.kind, CoreRuntimeErrorKind::Unavailable);
    assert_eq!(harness.runtime.state().open_count, 0);
}

#[test]
fn session_id_is_peer_bound_and_the_runtime_still_checks_the_secret_token() {
    let harness = Harness::new();
    let session = harness
        .client
        .open_owner_session(&harness.owner_request())
        .expect("the owner session should open");
    let unknown = OwnerSessionProof::new("unknown-session", session.proof.session_token());
    let unknown_error = harness
        .client
        .status(&unknown)
        .expect_err("an unknown session ID should be rejected by the transport");
    assert_eq!(unknown_error.kind, CoreRuntimeErrorKind::Authentication);
    assert_eq!(harness.runtime.state().status_count, 0);

    let wrong_token = OwnerSessionProof::new(session.proof.session_id(), "wrong-token");
    let token_error = harness
        .client
        .status(&wrong_token)
        .expect_err("the runtime should reject the wrong session token");
    assert_eq!(token_error.kind, CoreRuntimeErrorKind::Authentication);
    assert_eq!(harness.runtime.state().status_count, 0);
}

#[test]
fn pending_apply_cancellation_requires_the_full_owner_proof() {
    let harness = Harness::new();
    let session = harness
        .client
        .open_owner_session(&harness.owner_request())
        .expect("the owner session should open");
    let client = CoreServiceClient::for_service_uid(
        &harness.socket_path,
        nix::unistd::Uid::effective().as_raw(),
    );
    let wrong_token = OwnerSessionProof::new(session.proof.session_id(), "wrong-token");

    let error = client
        .cancel_pending_apply(&wrong_token)
        .expect_err("the wrong session token must not cancel Runtime Apply");

    assert_eq!(error.kind, CoreRuntimeErrorKind::Authentication);
    assert_eq!(harness.runtime.cancel_count.load(Ordering::Relaxed), 0);
}

#[test]
fn bundle_ingress_rejects_symlinked_provider_files_before_runtime_apply() {
    let harness = Harness::new();
    let session = harness
        .client
        .open_owner_session(&harness.owner_request())
        .expect("the owner session should open");
    let bundle = harness.source_bundle(12);
    let provider = bundle.generation_root.join("providers/local.yaml");
    let outside = harness.directory.path.join("outside-provider.yaml");
    fs::write(&outside, b"payload: [escaped]\n").expect("the outside provider should be written");
    fs::remove_file(&provider).expect("the provider fixture should be removed");
    symlink(&outside, &provider).expect("the provider symlink should be created");

    let error = harness
        .client
        .apply_candidate(&session.proof, &bundle)
        .expect_err("a symlinked provider should be rejected");

    assert_eq!(error.kind, CoreRuntimeErrorKind::InvalidBundle);
    assert_eq!(harness.runtime.state().apply_count, 0);
}

#[test]
fn absolute_response_deadline_bounds_a_stalled_runtime_operation() {
    let harness = Harness::new();
    let session = harness
        .client
        .open_owner_session(&harness.owner_request())
        .expect("the owner session should open");
    harness.runtime.state().status_delay = Some(Duration::from_millis(250));
    let client = CoreServiceClient::with_service_uid_and_timeouts(
        &harness.socket_path,
        nix::unistd::Uid::effective().as_raw(),
        Duration::from_secs(1),
        Duration::from_millis(40),
    );
    let started = Instant::now();

    let error = client
        .status(&session.proof)
        .expect_err("the stalled response should time out");

    assert_eq!(error.kind, CoreRuntimeErrorKind::Unavailable);
    assert!(started.elapsed() < Duration::from_millis(200));
}

#[test]
fn runtime_mutations_use_the_extended_response_budget() {
    let harness = Harness::new();
    let session = harness
        .client
        .open_owner_session(&harness.owner_request())
        .expect("the owner session should open");
    harness.runtime.state().apply_delay = Some(Duration::from_millis(80));
    let client = CoreServiceClient::with_service_uid_and_operation_timeouts(
        &harness.socket_path,
        nix::unistd::Uid::effective().as_raw(),
        Duration::from_secs(1),
        Duration::from_millis(40),
        Duration::from_secs(1),
    );

    let result = client
        .apply_candidate(&session.proof, &harness.source_bundle(13))
        .expect("the Runtime Apply should use the mutation response budget");

    assert_eq!(
        result.managed_core.runtime_generation,
        RuntimeGeneration(13)
    );
}

#[test]
fn authenticated_cancellation_interrupts_a_stalled_service_runtime_apply() {
    let harness = Harness::new();
    let session = harness
        .client
        .open_owner_session(&harness.owner_request())
        .expect("the owner session should open");
    harness.runtime.state().apply_delay = Some(Duration::from_secs(30));
    let client = Arc::new(CoreServiceClient::with_service_uid_and_operation_timeouts(
        &harness.socket_path,
        nix::unistd::Uid::effective().as_raw(),
        Duration::from_secs(1),
        Duration::from_secs(1),
        Duration::from_secs(1),
    ));
    let worker_client = Arc::clone(&client);
    let proof = session.proof.clone();
    let bundle = harness.source_bundle(14);
    let worker = std::thread::spawn(move || worker_client.apply_candidate(&proof, &bundle));
    let entered_deadline = Instant::now() + Duration::from_secs(1);
    while harness.runtime.apply_started.load(Ordering::Acquire) == 0 {
        assert!(
            Instant::now() < entered_deadline,
            "Runtime Apply should reach the fixture service"
        );
        std::thread::yield_now();
    }
    let started = Instant::now();

    client
        .cancel_pending_apply(&session.proof)
        .expect("the authenticated Runtime Apply cancellation should succeed");

    let error = worker
        .join()
        .expect("the Runtime Apply client should finish")
        .expect_err("the service Runtime Apply should be cancelled");
    assert_eq!(error.kind, CoreRuntimeErrorKind::ReloadTimeout);
    assert!(started.elapsed() < Duration::from_millis(200));

    let stop_started = Instant::now();
    client
        .stop_with_timeout(&session.proof, Duration::from_secs(1))
        .expect("Managed Core cleanup should follow cancellation promptly");
    assert!(stop_started.elapsed() < Duration::from_millis(200));

    let close_started = Instant::now();
    client
        .close_owner_session_with_timeout(&session.proof, Duration::from_secs(1))
        .expect("owner cleanup should follow the cancelled Runtime Apply promptly");
    assert!(close_started.elapsed() < Duration::from_millis(200));
    let state = harness.runtime.state();
    assert_eq!(state.apply_count, 0);
    assert_eq!(state.stop_count, 1);
    assert_eq!(state.close_count, 1);
    assert_eq!(harness.runtime.cancel_count.load(Ordering::Relaxed), 1);
    assert!(
        harness
            .runtime_root
            .join("generation-00000000000000000014")
            .exists(),
        "an indeterminate cancelled Runtime Apply must retain its staged generation"
    );
}

#[test]
fn debug_and_remote_errors_redact_paths_tokens_and_service_diagnostics() {
    let harness = Harness::new();
    let session = harness
        .client
        .open_owner_session(&harness.owner_request())
        .expect("the owner session should open");
    let sensitive_path = harness.directory.path.display().to_string();
    let diagnostic = format!(
        "secret={} path={sensitive_path}",
        session.proof.session_token()
    );
    harness.runtime.state().status_diagnostic = Some(diagnostic.clone());

    let error = harness
        .client
        .status(&session.proof)
        .expect_err("the fixture status should fail");
    let client_debug = format!("{:?}", harness.client);
    let server_debug = format!("{:?}", harness.server);
    let error_debug = format!("{error:?}");
    let error_display = error.to_string();

    for rendered in [client_debug, server_debug, error_debug, error_display] {
        assert!(!rendered.contains(&sensitive_path));
        assert!(!rendered.contains(session.proof.session_token()));
        assert!(!rendered.contains(&diagnostic));
    }
}

#[test]
fn encoded_responses_remain_inside_the_shared_frame_limit() {
    let harness = Harness::new();
    let session = harness
        .client
        .open_owner_session(&harness.owner_request())
        .expect("the owner session should open");
    harness.runtime.state().oversized_logs = true;

    let error = harness
        .client
        .logs(&session.proof, None, usize::MAX)
        .expect_err("an oversized encoded log response should be rejected");

    assert_eq!(error.kind, CoreRuntimeErrorKind::Unavailable);
}

#[test]
fn invalid_server_limits_fail_before_binding_a_socket() {
    let directory = TestDirectory::new();
    let service_root = directory.path.join("service-owned");
    fs::create_dir(&service_root).expect("the service root should be created");
    let runtime_root = service_root.join("runtime");
    let socket_path = directory.path.join("ipc/core-runtime.sock");
    let runtime = Arc::new(FakeRuntime::new(runtime_root.clone(), &service_root));
    let mut config = CoreServiceServerConfig::new(runtime_root, nix::unistd::geteuid().as_raw());
    config.worker_count = 0;

    let error = CoreServiceServer::start(&socket_path, runtime, config)
        .expect_err("zero workers should be rejected");

    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
    assert!(!socket_path.exists());
}

#[test]
fn server_rejects_protocol_versions_with_the_original_request_id() {
    let mut harness = Harness::new();
    let mut stream =
        UnixStream::connect(&harness.socket_path).expect("the raw protocol fixture should connect");
    let request = serde_json::json!({
        "protocol_version": 999,
        "request_id": 73,
        "operation": {
            "operation": "status",
            "payload": {
                "owner": {
                    "session_id": "unknown",
                    "session_token": "unknown"
                }
            }
        }
    });

    write_frame(&mut stream, &request).expect("the raw request should be written");
    let response: serde_json::Value =
        read_frame(&mut stream).expect("the protocol response should be read");

    assert_eq!(response["protocol_version"], 1);
    assert_eq!(response["request_id"], 73);
    assert_eq!(response["outcome"]["outcome"], "failure");
    assert_eq!(response["outcome"]["payload"]["kind"], "protocol_mismatch");
    harness
        .server
        .shutdown()
        .expect("the protocol fixture server should stop");
}

#[test]
fn client_rejects_a_response_with_a_different_request_id() {
    let directory = TestDirectory::new();
    let socket_path = directory.path.join("raw/correlation.sock");
    let listener =
        bind_private_listener(&socket_path).expect("the raw correlation listener should bind");
    let responder = std::thread::spawn(move || {
        let (mut stream, _) = listener
            .accept()
            .expect("the raw correlation request should connect");
        let request: serde_json::Value =
            read_frame(&mut stream).expect("the raw correlation request should be read");
        let response = serde_json::json!({
            "protocol_version": 1,
            "request_id": request["request_id"].as_u64().expect("request ID") + 1,
            "outcome": {
                "outcome": "success",
                "payload": {
                    "operation": "status",
                    "payload": { "managed_core": null }
                }
            }
        });
        write_frame(&mut stream, &response).expect("the raw correlation response should be sent");
    });
    let client =
        CoreServiceClient::for_service_uid(&socket_path, nix::unistd::Uid::effective().as_raw());

    let error = client
        .status(&OwnerSessionProof::new("fixture", "fixture"))
        .expect_err("the mismatched response should be rejected");

    assert_eq!(error.kind, CoreRuntimeErrorKind::ProtocolMismatch);
    responder
        .join()
        .expect("the raw correlation responder should finish");
    fs::remove_file(socket_path).expect("the raw correlation socket should be removed");
}

#[test]
fn verified_stale_service_socket_is_recovered_before_bind() {
    let directory = TestDirectory::new();
    let (socket_path, runtime, config) = server_fixture(&directory, "stale");
    fs::create_dir_all(socket_path.parent().expect("socket parent should exist"))
        .expect("socket parent should be created");
    let stale = UnixListener::bind(&socket_path).expect("stale socket fixture should bind");
    fs::set_permissions(&socket_path, fs::Permissions::from_mode(0o600))
        .expect("stale socket access should match the service policy");
    drop(stale);

    let mut server = CoreServiceServer::start(&socket_path, runtime, config)
        .expect("a verified stale socket should be recovered");

    let client =
        CoreServiceClient::for_service_uid(&socket_path, nix::unistd::Uid::effective().as_raw());
    let error = client
        .status(&OwnerSessionProof::new("stale", "stale"))
        .expect_err("the replacement service should answer requests");
    assert_eq!(error.kind, CoreRuntimeErrorKind::Authentication);
    server
        .shutdown()
        .expect("the replacement service should shut down cleanly");
}

#[test]
fn active_service_socket_is_preserved() {
    let directory = TestDirectory::new();
    let (socket_path, runtime, config) = server_fixture(&directory, "active");
    fs::create_dir_all(socket_path.parent().expect("socket parent should exist"))
        .expect("socket parent should be created");
    let active = UnixListener::bind(&socket_path).expect("active socket fixture should bind");
    fs::set_permissions(&socket_path, fs::Permissions::from_mode(0o600))
        .expect("active socket access should match the service policy");
    let before = fs::symlink_metadata(&socket_path).expect("active socket metadata should load");

    let error = CoreServiceServer::start(&socket_path, runtime, config)
        .expect_err("an active service socket should retain ownership");

    assert_eq!(error.kind(), std::io::ErrorKind::AddrInUse);
    let after = fs::symlink_metadata(&socket_path).expect("active socket should remain present");
    assert_eq!((after.dev(), after.ino()), (before.dev(), before.ino()));
    UnixStream::connect(&socket_path).expect("the active listener should still accept peers");
    active
        .accept()
        .expect("the active listener should retain ownership");
}

#[test]
fn unverified_service_paths_are_preserved() {
    let directory = TestDirectory::new();
    let (file_path, file_runtime, file_config) = server_fixture(&directory, "file");
    fs::create_dir_all(file_path.parent().expect("file parent should exist"))
        .expect("file parent should be created");
    fs::write(&file_path, b"preserve").expect("file fixture should be written");

    CoreServiceServer::start(&file_path, file_runtime, file_config)
        .expect_err("a non-socket service path should be preserved");

    assert_eq!(
        fs::read(&file_path).expect("file fixture should remain readable"),
        b"preserve"
    );

    let (link_path, link_runtime, link_config) = server_fixture(&directory, "link");
    fs::create_dir_all(link_path.parent().expect("link parent should exist"))
        .expect("link parent should be created");
    let target = directory.path.join("link-target");
    fs::write(&target, b"target").expect("symlink target should be written");
    symlink(&target, &link_path).expect("service path symlink should be created");

    CoreServiceServer::start(&link_path, link_runtime, link_config)
        .expect_err("a service path symlink should be preserved");

    assert!(
        fs::symlink_metadata(&link_path)
            .expect("service path symlink should remain")
            .file_type()
            .is_symlink()
    );
    assert_eq!(
        fs::read(&target).expect("symlink target should remain readable"),
        b"target"
    );

    let (wide_path, wide_runtime, wide_config) = server_fixture(&directory, "wide");
    fs::create_dir_all(wide_path.parent().expect("wide socket parent should exist"))
        .expect("wide socket parent should be created");
    let wide = UnixListener::bind(&wide_path).expect("wide socket fixture should bind");
    fs::set_permissions(&wide_path, fs::Permissions::from_mode(0o666))
        .expect("wide socket fixture permissions should be configured");
    let before = fs::symlink_metadata(&wide_path).expect("wide socket metadata should load");
    drop(wide);

    let error = CoreServiceServer::start(&wide_path, wide_runtime, wide_config)
        .expect_err("a stale socket outside the service access policy should be preserved");

    assert_eq!(error.kind(), std::io::ErrorKind::PermissionDenied);
    let after = fs::symlink_metadata(&wide_path).expect("wide stale socket should remain");
    assert_eq!((after.dev(), after.ino()), (before.dev(), before.ino()));
}

#[test]
fn pending_connection_capacity_rejects_excess_clients_and_shutdown_stays_bounded() {
    let directory = TestDirectory::new();
    let service_root = directory.path.join("service-owned");
    fs::create_dir(&service_root).expect("the service root should be created");
    let runtime_root = service_root.join("runtime");
    let socket_path = directory.path.join("ipc/core-runtime.sock");
    let runtime = Arc::new(FakeRuntime::new(runtime_root.clone(), &service_root));
    let mut config = CoreServiceServerConfig::new(runtime_root, nix::unistd::geteuid().as_raw());
    config.worker_count = 1;
    config.pending_connection_capacity = 1;
    config.io_timeout = Duration::from_secs(1);
    let mut server = CoreServiceServer::start(&socket_path, runtime, config)
        .expect("the bounded fixture server should start");

    let first = UnixStream::connect(&socket_path).expect("the active client should connect");
    std::thread::sleep(Duration::from_millis(40));
    let second = UnixStream::connect(&socket_path).expect("the queued client should connect");
    std::thread::sleep(Duration::from_millis(40));
    let mut excess = UnixStream::connect(&socket_path).expect("the excess client should connect");
    excess
        .set_read_timeout(Some(Duration::from_millis(200)))
        .expect("the excess client deadline should be configured");
    let mut byte = [0_u8; 1];

    let rejected = match excess.read(&mut byte) {
        Ok(0) => true,
        Err(error) => matches!(
            error.kind(),
            std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::BrokenPipe
        ),
        Ok(_) => false,
    };

    assert!(rejected);
    drop(excess);
    drop(second);
    drop(first);
    let started = Instant::now();
    server
        .shutdown()
        .expect("the bounded fixture server should stop");
    assert!(started.elapsed() < Duration::from_millis(300));
}

#[test]
fn idle_server_shutdown_wakes_accept_without_dispatching_a_request() {
    let mut harness = Harness::new();
    std::thread::sleep(Duration::from_millis(30));
    let started = Instant::now();

    harness
        .server
        .shutdown()
        .expect("the idle server should stop through its wake connection");

    assert!(started.elapsed() < Duration::from_millis(200));
    let state = harness.runtime.state();
    assert_eq!(state.open_count, 0);
    assert_eq!(state.apply_count, 0);
    assert_eq!(state.status_count, 0);
    assert_eq!(state.logs_count, 0);
    assert_eq!(state.stop_count, 0);
    assert_eq!(state.close_count, 0);
    assert!(!harness.socket_path.exists());
}

fn write_bundle(root: &Path, generation: RuntimeGeneration) -> RuntimeBundle {
    fs::create_dir_all(root.join("providers"))
        .expect("the bundle fixture directories should be created");
    let binary = b"fixture-mihomo-binary";
    let configuration = b"mode: rule\n";
    let provider = b"payload: []\n";
    let binary_sha256 = sha256(binary);
    let configuration_sha256 = sha256(configuration);
    let provider_sha256 = sha256(provider);
    let policy_sha256 = sha256(b"fixture-compiler-policy");
    fs::write(root.join("mihomo"), binary).expect("the fixture binary should be written");
    fs::set_permissions(root.join("mihomo"), fs::Permissions::from_mode(0o500))
        .expect("the fixture binary should be executable");
    fs::write(root.join("config.yaml"), configuration)
        .expect("the fixture configuration should be written");
    fs::write(root.join("providers/local.yaml"), provider)
        .expect("the fixture provider should be written");
    let manifest = serde_json::json!({
        "schema_version": 1,
        "runtime_generation": generation.0,
        "compiler_policy_sha256": policy_sha256,
        "mihomo_binary_sha256": binary_sha256,
        "configuration_sha256": configuration_sha256,
        "executable": "mihomo",
        "configuration": "config.yaml",
        "provider_files": [{
            "path": "providers/local.yaml",
            "sha256": provider_sha256,
            "size": provider.len(),
        }],
    });
    let manifest_bytes = serde_json::to_vec(&manifest).expect("the manifest should serialize");
    fs::write(root.join("manifest.json"), &manifest_bytes)
        .expect("the fixture manifest should be written");
    RuntimeBundle {
        generation,
        generation_root: root.to_path_buf(),
        manifest_sha256: sha256(&manifest_bytes),
        compiler_policy_sha256: manifest["compiler_policy_sha256"]
            .as_str()
            .expect("the policy digest should be a string")
            .to_owned(),
        mihomo_binary_sha256: manifest["mihomo_binary_sha256"]
            .as_str()
            .expect("the binary digest should be a string")
            .to_owned(),
    }
}

fn service_generation_names(root: &Path) -> Vec<u64> {
    let mut generations = fs::read_dir(root)
        .expect("the service runtime root should be readable")
        .filter_map(Result::ok)
        .filter_map(|entry| {
            entry
                .file_name()
                .to_str()
                .and_then(|name| name.strip_prefix("generation-"))
                .and_then(|generation| generation.parse::<u64>().ok())
        })
        .collect::<Vec<_>>();
    generations.sort_unstable();
    generations
}

fn sha256(content: &[u8]) -> String {
    let digest = Sha256::digest(content);
    let mut encoded = String::with_capacity(digest.len() * 2);
    for byte in digest {
        use std::fmt::Write as _;
        write!(&mut encoded, "{byte:02x}").expect("writing to a String should work");
    }
    encoded
}
