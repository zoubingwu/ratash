#![cfg(debug_assertions)]

use std::fs;
use std::io;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use hopash::core::{
    ApplyCandidateResult, CoreControlEndpoint, CoreRuntime, CoreRuntimeError, CoreRuntimeErrorKind,
    CoreRuntimeStatus, ForwardedCoreLogBatch, OwnerSession, OwnerSessionProof, OwnerSessionRequest,
    RuntimeBundle, StopCoreResult,
};
use hopash::core_service_ipc::{CoreServiceServer, CoreServiceServerConfig};
use hopash::lifecycle::{InstanceRecord, StatePaths};

const CORE_SERVICE_SOCKET_ENV: &str = "HOPASH_CORE_SERVICE_SOCKET";
const COMMAND_TIMEOUT: Duration = Duration::from_secs(30);

static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);

struct TestDirectory {
    path: PathBuf,
}

impl TestDirectory {
    fn new() -> Self {
        let sequence = NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed);
        let path = Path::new("/tmp").join(format!(
            "hopash-production-lifecycle-{}-{sequence}",
            std::process::id()
        ));
        fs::create_dir(&path).expect("the lifecycle fixture root should be created");
        fs::set_permissions(&path, fs::Permissions::from_mode(0o700))
            .expect("the lifecycle fixture root should be private");
        Self { path }
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct RuntimeSnapshot {
    opened_supervisor_pids: Vec<u32>,
    opened_owner_uids: Vec<u32>,
    apply_count: usize,
    stop_count: usize,
    close_count: usize,
}

#[derive(Default)]
struct RuntimeState {
    snapshot: RuntimeSnapshot,
    owner: Option<OwnerSessionProof>,
}

struct LifecycleRuntime {
    endpoint: CoreControlEndpoint,
    state: Mutex<RuntimeState>,
}

impl LifecycleRuntime {
    fn new(endpoint: CoreControlEndpoint) -> Self {
        Self {
            endpoint,
            state: Mutex::new(RuntimeState::default()),
        }
    }

    fn snapshot(&self) -> RuntimeSnapshot {
        self.state
            .lock()
            .expect("the lifecycle runtime state should lock")
            .snapshot
            .clone()
    }

    fn require_owner(&self, owner: &OwnerSessionProof) -> Result<(), CoreRuntimeError> {
        let state = self
            .state
            .lock()
            .expect("the lifecycle runtime state should lock");
        if state.owner.as_ref() == Some(owner) {
            Ok(())
        } else {
            Err(CoreRuntimeError::new(
                CoreRuntimeErrorKind::Authentication,
                "fixture owner proof mismatch",
            ))
        }
    }
}

impl CoreRuntime for LifecycleRuntime {
    fn open_owner_session(
        &self,
        request: &OwnerSessionRequest,
    ) -> Result<OwnerSession, CoreRuntimeError> {
        let mut state = self
            .state
            .lock()
            .expect("the lifecycle runtime state should lock");
        let owner_generation = state.snapshot.opened_supervisor_pids.len() as u64 + 1;
        let proof = OwnerSessionProof::new(
            format!("fixture-session-{owner_generation}"),
            format!("fixture-token-{owner_generation}"),
        );
        state
            .snapshot
            .opened_supervisor_pids
            .push(request.supervisor_pid);
        state.snapshot.opened_owner_uids.push(request.owner_uid);
        state.owner = Some(proof.clone());
        Ok(OwnerSession {
            proof,
            protocol_version: request.protocol_version,
            owner_generation,
            endpoint: self.endpoint.clone(),
        })
    }

    fn apply_candidate(
        &self,
        owner: &OwnerSessionProof,
        _bundle: &RuntimeBundle,
    ) -> Result<ApplyCandidateResult, CoreRuntimeError> {
        self.require_owner(owner)?;
        self.state
            .lock()
            .expect("the lifecycle runtime state should lock")
            .snapshot
            .apply_count += 1;
        Err(CoreRuntimeError::new(
            CoreRuntimeErrorKind::Apply,
            "the zero-Profile lifecycle fixture does not apply a runtime",
        ))
    }

    fn status(&self, owner: &OwnerSessionProof) -> Result<CoreRuntimeStatus, CoreRuntimeError> {
        self.require_owner(owner)?;
        Ok(CoreRuntimeStatus::from_managed_core(None))
    }

    fn logs(
        &self,
        owner: &OwnerSessionProof,
        _after_sequence: Option<u64>,
        _limit: usize,
    ) -> Result<ForwardedCoreLogBatch, CoreRuntimeError> {
        self.require_owner(owner)?;
        Ok(ForwardedCoreLogBatch {
            records: Vec::new(),
            next_sequence: None,
            dropped_before: 0,
        })
    }

    fn stop(&self, owner: &OwnerSessionProof) -> Result<StopCoreResult, CoreRuntimeError> {
        self.require_owner(owner)?;
        self.state
            .lock()
            .expect("the lifecycle runtime state should lock")
            .snapshot
            .stop_count += 1;
        Ok(StopCoreResult {
            stopped: false,
            instance_generation: None,
        })
    }

    fn close_owner_session(&self, owner: &OwnerSessionProof) -> Result<(), CoreRuntimeError> {
        self.require_owner(owner)?;
        let mut state = self
            .state
            .lock()
            .expect("the lifecycle runtime state should lock");
        state.snapshot.close_count += 1;
        state.owner = None;
        Ok(())
    }
}

#[derive(Clone)]
struct LifecycleCommands {
    binary: PathBuf,
    state_root: PathBuf,
    core_service_socket: PathBuf,
    mihomo_fixture: PathBuf,
}

impl LifecycleCommands {
    fn run(&self, arguments: &[&str]) -> io::Result<Output> {
        let mut command = Command::new(&self.binary);
        command
            .args(arguments)
            .env("HOPASH_STATE_DIR", &self.state_root)
            .env(CORE_SERVICE_SOCKET_ENV, &self.core_service_socket)
            .env("HOPASH_MIHOMO_PATH", &self.mihomo_fixture)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut child = command.spawn()?;
        let deadline = Instant::now() + COMMAND_TIMEOUT;
        loop {
            if child.try_wait()?.is_some() {
                return child.wait_with_output();
            }
            if Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "the lifecycle command exceeded its fixture deadline",
                ));
            }
            thread::sleep(Duration::from_millis(10));
        }
    }
}

struct SupervisorDropGuard {
    commands: LifecycleCommands,
    armed: bool,
}

impl SupervisorDropGuard {
    fn new(commands: LifecycleCommands) -> Self {
        Self {
            commands,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for SupervisorDropGuard {
    fn drop(&mut self) {
        if self.armed {
            let _ = self.commands.run(&["stop", "--json"]);
        }
    }
}

#[test]
fn production_binary_lifecycle_is_ready_idempotent_persistent_and_replaces_exactly() {
    let directory = TestDirectory::new();
    let state_root = directory.path.join("state-root");
    let service_runtime_root = directory.path.join("service-runtime");
    let core_service_socket = directory.path.join("core-service.sock");
    let mihomo_fixture = directory.path.join("mihomo-fixture");
    fs::write(&mihomo_fixture, b"#!/bin/sh\nexit 0\n")
        .expect("the harmless Mihomo fixture should be written");
    fs::set_permissions(&mihomo_fixture, fs::Permissions::from_mode(0o700))
        .expect("the harmless Mihomo fixture should be executable");

    let runtime = Arc::new(LifecycleRuntime::new(CoreControlEndpoint::new(
        directory.path.join("unused-core-control.sock"),
        "fixture-core-secret",
    )));
    let owner_uid = nix::unistd::Uid::effective().as_raw();
    let mut server = CoreServiceServer::start(
        &core_service_socket,
        Arc::clone(&runtime),
        CoreServiceServerConfig::new(&service_runtime_root, owner_uid),
    )
    .expect("the fake Core service should start");
    let commands = LifecycleCommands {
        binary: PathBuf::from(env!("CARGO_BIN_EXE_hopash")),
        state_root: state_root.clone(),
        core_service_socket: core_service_socket.clone(),
        mihomo_fixture,
    };
    let mut supervisor_guard = SupervisorDropGuard::new(commands.clone());
    let paths = StatePaths::for_root(&state_root);

    let start = success_json(commands.run(&["start", "--json"]));
    assert_eq!(start["data"]["action"], "start");
    assert_eq!(start["data"]["changed"], true);
    assert_ready(&start["data"]["status"]);
    let first_record = live_instance(&paths);
    let persistence_inode = fs::metadata(&paths.persistence)
        .expect("the persistence directory should exist")
        .ino();

    let status = success_json(commands.run(&["status", "--json"]));
    assert_ready(&status["data"]);

    let repeated_start = success_json(commands.run(&["start", "--json"]));
    assert_eq!(repeated_start["data"]["action"], "start");
    assert_eq!(repeated_start["data"]["changed"], false);
    assert_ready(&repeated_start["data"]["status"]);
    assert_eq!(live_instance(&paths).supervisor, first_record.supervisor);
    assert_eq!(runtime.snapshot().opened_supervisor_pids.len(), 1);

    let restart = success_json(commands.run(&["restart", "--json"]));
    assert_eq!(restart["data"]["action"], "restart");
    assert_eq!(restart["data"]["changed"], true);
    assert_ready(&restart["data"]["status"]);
    let replacement_record = live_instance(&paths);
    assert_ne!(replacement_record.supervisor, first_record.supervisor);
    assert_eq!(
        runtime.snapshot().opened_supervisor_pids,
        vec![
            first_record.supervisor.pid,
            replacement_record.supervisor.pid
        ]
    );
    assert_eq!(runtime.snapshot().opened_owner_uids, vec![owner_uid; 2]);
    assert_eq!(
        fs::metadata(&paths.persistence)
            .expect("the persistence directory should survive restart")
            .ino(),
        persistence_inode
    );
    assert_ready(&success_json(commands.run(&["status", "--json"]))["data"]);

    let stop = success_json(commands.run(&["stop", "--json"]));
    assert_eq!(stop["data"]["action"], "stop");
    assert_eq!(stop["data"]["changed"], true);
    assert_eq!(stop["data"]["status"]["supervisor"]["lifecycle"], "stopped");
    assert_eq!(stop["data"]["status"]["core"]["lifecycle"], "stopped");
    assert!(
        InstanceRecord::read_private(&paths.instance_record)
            .expect("the stopped instance record should be readable")
            .is_none()
    );
    assert!(!paths.ipc_socket.exists());
    assert!(!paths.shutdown_socket.exists());

    let repeated_stop = success_json(commands.run(&["stop", "--json"]));
    assert_eq!(repeated_stop["data"]["action"], "stop");
    assert_eq!(repeated_stop["data"]["changed"], false);
    assert_eq!(
        repeated_stop["data"]["status"]["supervisor"]["lifecycle"],
        "stopped"
    );
    assert_eq!(
        runtime.snapshot(),
        RuntimeSnapshot {
            opened_supervisor_pids: vec![
                first_record.supervisor.pid,
                replacement_record.supervisor.pid,
            ],
            opened_owner_uids: vec![owner_uid; 2],
            apply_count: 0,
            stop_count: 2,
            close_count: 2,
        }
    );

    supervisor_guard.disarm();
    server
        .shutdown()
        .expect("the fake Core service should stop cleanly");
    assert!(!core_service_socket.exists());
}

fn success_json(output: io::Result<Output>) -> serde_json::Value {
    let output = output.expect("the lifecycle command should finish within its deadline");
    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        output.stderr.is_empty(),
        "successful JSON commands should keep stderr empty: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).expect("stdout should contain one JSON document")
}

fn assert_ready(status: &serde_json::Value) {
    assert_eq!(status["supervisor"]["lifecycle"], "ready");
    assert_eq!(status["core"]["lifecycle"], "unconfigured");
}

fn live_instance(paths: &StatePaths) -> InstanceRecord {
    InstanceRecord::read_private(&paths.instance_record)
        .expect("the live instance record should be readable")
        .expect("the live instance record should exist")
}
