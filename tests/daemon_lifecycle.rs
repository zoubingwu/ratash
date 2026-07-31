use std::collections::{BTreeMap, VecDeque};
use std::fs;
use std::io;
use std::os::unix::fs::{PermissionsExt, symlink};
use std::os::unix::net::UnixListener;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use hopash::daemon::{
    DaemonAction, DaemonClock, DaemonErrorKind, DaemonLifecycle, DaemonProcessControl,
    DaemonTimeouts, DetachedSupervisorLaunch, ReadinessFailure, ShutdownAcknowledgement,
    ShutdownIntent, ShutdownPort, StartupFailureCategory, StartupStage, SupervisorOwnership,
};
use hopash::lifecycle::{
    DirectoryLease, LeaseAcquisition, ProcessIdentity, ProcessInspector, StatePaths,
};

static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);

struct TestDirectory {
    path: PathBuf,
}

impl TestDirectory {
    fn new() -> Self {
        let sequence = NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed);
        let path = PathBuf::from("/tmp").join(format!("hd-{}-{sequence}", std::process::id()));
        fs::create_dir(&path).expect("test directory should be created");
        Self { path }
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.path).expect("test directory should be removed");
    }
}

#[derive(Clone)]
enum SpawnBehavior {
    Ready,
    Silent,
    Failure(ReadinessFailure),
    FailureThenExit(ReadinessFailure),
    WrongProcess,
}

struct FixtureRuntime {
    ownership: SupervisorOwnership,
    listener: Option<UnixListener>,
}

struct FakeProcessControl {
    executable: PathBuf,
    identities: Mutex<BTreeMap<u32, String>>,
    runtimes: Mutex<BTreeMap<u32, FixtureRuntime>>,
    behaviors: Mutex<VecDeque<SpawnBehavior>>,
    launches: Mutex<Vec<Vec<std::ffi::OsString>>>,
    next_pid: AtomicU32,
    spawn_count: AtomicUsize,
    terminate_count: AtomicUsize,
}

impl FakeProcessControl {
    fn new() -> Self {
        let mut identities = BTreeMap::new();
        identities.insert(std::process::id(), "launcher-start".to_owned());
        Self {
            executable: PathBuf::from("/fixture/hopash"),
            identities: Mutex::new(identities),
            runtimes: Mutex::new(BTreeMap::new()),
            behaviors: Mutex::new(VecDeque::new()),
            launches: Mutex::new(Vec::new()),
            next_pid: AtomicU32::new(20_000),
            spawn_count: AtomicUsize::new(0),
            terminate_count: AtomicUsize::new(0),
        }
    }

    fn queue(&self, behavior: SpawnBehavior) {
        self.behaviors
            .lock()
            .expect("behavior lock should work")
            .push_back(behavior);
    }

    fn set_identity(&self, pid: u32, identity: Option<&str>) {
        let mut identities = self.identities.lock().expect("identity lock should work");
        match identity {
            Some(identity) => {
                identities.insert(pid, identity.to_owned());
            }
            None => {
                identities.remove(&pid);
            }
        }
    }

    fn stop_clean(&self, process: &ProcessIdentity) -> io::Result<()> {
        let runtime = self
            .runtimes
            .lock()
            .expect("runtime lock should work")
            .remove(&process.pid)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "fixture runtime missing"))?;
        let FixtureRuntime {
            ownership,
            listener,
        } = runtime;
        drop(listener);
        ownership
            .release()
            .map_err(|error| io::Error::other(error.to_string()))?;
        self.set_identity(process.pid, None);
        Ok(())
    }

    fn crash(&self, process: &ProcessIdentity) {
        self.set_identity(process.pid, None);
        self.runtimes
            .lock()
            .expect("runtime lock should work")
            .remove(&process.pid);
    }

    fn spawn_count(&self) -> usize {
        self.spawn_count.load(Ordering::Relaxed)
    }

    fn terminate_count(&self) -> usize {
        self.terminate_count.load(Ordering::Relaxed)
    }
}

impl ProcessInspector for FakeProcessControl {
    fn identity(&self, pid: u32) -> io::Result<Option<String>> {
        Ok(self
            .identities
            .lock()
            .expect("identity lock should work")
            .get(&pid)
            .cloned())
    }
}

impl DaemonProcessControl for FakeProcessControl {
    fn executable(&self) -> io::Result<PathBuf> {
        Ok(self.executable.clone())
    }

    fn spawn_detached(&self, launch: &DetachedSupervisorLaunch) -> io::Result<u32> {
        self.spawn_count.fetch_add(1, Ordering::Relaxed);
        self.launches
            .lock()
            .expect("launch lock should work")
            .push(launch.arguments());
        assert_eq!(launch.executable(), self.executable);
        let pid = self.next_pid.fetch_add(1, Ordering::Relaxed);
        let start_identity = format!("fixture-start-{pid}");
        self.set_identity(pid, Some(&start_identity));
        let process = ProcessIdentity {
            pid,
            start_identity,
        };
        let behavior = self
            .behaviors
            .lock()
            .expect("behavior lock should work")
            .pop_front()
            .unwrap_or(SpawnBehavior::Ready);
        match behavior {
            SpawnBehavior::Ready | SpawnBehavior::WrongProcess => {
                let paths = StatePaths::for_root(launch.state_root());
                let ownership = SupervisorOwnership::acquire(
                    paths.clone(),
                    process.clone(),
                    u64::from(pid),
                    self,
                )
                .map_err(|error| io::Error::other(error.to_string()))?;
                let listener = UnixListener::bind(&paths.ipc_socket)?;
                if matches!(behavior, SpawnBehavior::WrongProcess) {
                    launch
                        .readiness()
                        .publish_ready(
                            ProcessIdentity {
                                pid: pid + 1,
                                start_identity: "wrong-process".to_owned(),
                            },
                            ownership.record().instance_token().to_owned(),
                        )
                        .map_err(|error| io::Error::other(error.to_string()))?;
                } else {
                    ownership
                        .publish_ready(launch.readiness())
                        .map_err(|error| io::Error::other(error.to_string()))?;
                }
                self.runtimes
                    .lock()
                    .expect("runtime lock should work")
                    .insert(
                        pid,
                        FixtureRuntime {
                            ownership,
                            listener: Some(listener),
                        },
                    );
            }
            SpawnBehavior::Silent => {}
            SpawnBehavior::Failure(failure) => {
                launch
                    .readiness()
                    .publish_failure(process, failure)
                    .map_err(|error| io::Error::other(error.to_string()))?;
            }
            SpawnBehavior::FailureThenExit(failure) => {
                launch
                    .readiness()
                    .publish_failure(process.clone(), failure)
                    .map_err(|error| io::Error::other(error.to_string()))?;
                self.set_identity(process.pid, None);
            }
        }
        Ok(pid)
    }

    fn terminate_exact(&self, process: &ProcessIdentity) -> io::Result<bool> {
        if self.identity(process.pid)?.as_deref() != Some(process.start_identity.as_str()) {
            return Ok(false);
        }
        self.terminate_count.fetch_add(1, Ordering::Relaxed);
        self.crash(process);
        Ok(true)
    }
}

#[derive(Default)]
struct FakeClock {
    millis: AtomicU64,
}

impl DaemonClock for FakeClock {
    fn monotonic_now(&self) -> Duration {
        Duration::from_millis(self.millis.load(Ordering::Relaxed))
    }

    fn sleep(&self, duration: Duration) {
        self.millis.fetch_add(
            u64::try_from(duration.as_millis())
                .unwrap_or(u64::MAX)
                .max(1),
            Ordering::Relaxed,
        );
    }
}

#[derive(Clone, Copy)]
enum ShutdownBehavior {
    Stop,
    AcknowledgeOnly,
    WrongAcknowledgement,
}

struct FakeShutdownPort {
    process: Arc<FakeProcessControl>,
    behavior: Mutex<ShutdownBehavior>,
}

impl FakeShutdownPort {
    fn new(process: Arc<FakeProcessControl>) -> Self {
        Self {
            process,
            behavior: Mutex::new(ShutdownBehavior::Stop),
        }
    }

    fn set_behavior(&self, behavior: ShutdownBehavior) {
        *self.behavior.lock().expect("shutdown lock should work") = behavior;
    }
}

impl ShutdownPort for FakeShutdownPort {
    fn request_shutdown(
        &self,
        intent: &ShutdownIntent,
        _timeout: Duration,
    ) -> io::Result<ShutdownAcknowledgement> {
        let behavior = *self.behavior.lock().expect("shutdown lock should work");
        match behavior {
            ShutdownBehavior::Stop => {
                self.process.stop_clean(&intent.process)?;
                Ok(ShutdownAcknowledgement {
                    process: intent.process.clone(),
                    instance_token: intent.instance_token.clone(),
                })
            }
            ShutdownBehavior::AcknowledgeOnly => Ok(ShutdownAcknowledgement {
                process: intent.process.clone(),
                instance_token: intent.instance_token.clone(),
            }),
            ShutdownBehavior::WrongAcknowledgement => Ok(ShutdownAcknowledgement {
                process: intent.process.clone(),
                instance_token: "another-instance".to_owned(),
            }),
        }
    }
}

type FixtureLifecycle = DaemonLifecycle<FakeProcessControl, FakeShutdownPort, FakeClock>;

fn fixture(
    directory: &TestDirectory,
) -> (
    FixtureLifecycle,
    Arc<FakeProcessControl>,
    Arc<FakeShutdownPort>,
) {
    let process = Arc::new(FakeProcessControl::new());
    let shutdown = Arc::new(FakeShutdownPort::new(Arc::clone(&process)));
    let lifecycle = DaemonLifecycle::new(
        StatePaths::for_root(directory.path.join("state")),
        Arc::clone(&process),
        Arc::clone(&shutdown),
        Arc::new(FakeClock::default()),
        DaemonTimeouts {
            startup: Duration::from_millis(12),
            shutdown: Duration::from_millis(12),
            poll_interval: Duration::from_millis(2),
        },
    );
    (lifecycle, process, shutdown)
}

#[test]
fn start_and_stop_are_idempotent_and_restart_replaces_the_exact_instance() {
    let directory = TestDirectory::new();
    let (lifecycle, process, _) = fixture(&directory);

    let first = lifecycle.start().expect("first start should succeed");
    let repeated = lifecycle.start().expect("repeated start should succeed");
    assert_eq!(first.action, DaemonAction::Start);
    assert!(first.changed);
    assert!(!repeated.changed);
    assert_eq!(first.instance, repeated.instance);
    assert_eq!(process.spawn_count(), 1);

    let stopped = lifecycle.stop().expect("first stop should succeed");
    let repeated_stop = lifecycle.stop().expect("repeated stop should succeed");
    assert!(stopped.changed);
    assert!(!repeated_stop.changed);
    assert!(repeated_stop.instance.is_none());

    let running = lifecycle.start().expect("second start should succeed");
    let restarted = lifecycle.restart().expect("restart should succeed");
    assert_eq!(restarted.action, DaemonAction::Restart);
    assert!(restarted.changed);
    assert_ne!(
        running
            .instance
            .as_ref()
            .expect("running instance should exist")
            .instance_token(),
        restarted
            .instance
            .as_ref()
            .expect("restarted instance should exist")
            .instance_token()
    );
    assert_eq!(process.spawn_count(), 3);
}

#[test]
fn launch_uses_the_same_executable_and_the_hidden_internal_mode() {
    let directory = TestDirectory::new();
    let (lifecycle, process, _) = fixture(&directory);

    lifecycle.start().expect("start should succeed");
    let launches = process.launches.lock().expect("launch lock should work");
    let arguments = &launches[0];

    assert_eq!(arguments[0], "__supervisor");
    assert_eq!(arguments[1], "--state-root");
    assert_eq!(arguments[2], directory.path.join("state"));
    assert_eq!(arguments[3], "--readiness-path");
    assert!(PathBuf::from(&arguments[4]).starts_with(directory.path.join("state")));
}

#[test]
fn lifecycle_operation_lease_serializes_foreground_launchers() {
    let directory = TestDirectory::new();
    let (lifecycle, process, _) = fixture(&directory);
    let paths = StatePaths::for_root(directory.path.join("state"));
    paths.prepare().expect("state paths should prepare");
    let lease = match DirectoryLease::acquire(
        &paths.root,
        "lifecycle-operation",
        ProcessIdentity {
            pid: std::process::id(),
            start_identity: "launcher-start".to_owned(),
        },
        process.as_ref(),
    )
    .expect("fixture lease should acquire")
    {
        LeaseAcquisition::Acquired(lease) => lease,
        LeaseAcquisition::HeldByLiveProcess(_) => panic!("fixture lease should be new"),
    };

    let error = lifecycle.start().expect_err("contended start should fail");

    assert_eq!(error.kind(), DaemonErrorKind::LifecycleOperationBusy);
    assert_eq!(process.spawn_count(), 0);
    lease.release().expect("fixture lease should release");
}

#[test]
fn a_valid_stale_record_and_socket_are_cleaned_before_start() {
    let directory = TestDirectory::new();
    let (lifecycle, process, _) = fixture(&directory);
    let paths = StatePaths::for_root(directory.path.join("state"));
    let stale = ProcessIdentity {
        pid: 9_001,
        start_identity: "stale-start".to_owned(),
    };
    process.set_identity(stale.pid, Some(&stale.start_identity));
    let ownership = SupervisorOwnership::acquire(paths.clone(), stale.clone(), 1, process.as_ref())
        .expect("stale owner should acquire");
    let listener = UnixListener::bind(&paths.ipc_socket).expect("stale socket should bind");
    let control_listener =
        UnixListener::bind(&paths.shutdown_socket).expect("stale control socket should bind");
    std::mem::forget(ownership);
    drop(listener);
    drop(control_listener);
    process.set_identity(stale.pid, None);

    let outcome = lifecycle.start().expect("stale state should recover");

    assert!(outcome.changed);
    assert_eq!(process.spawn_count(), 1);
    assert!(!paths.shutdown_socket.exists());
    assert_eq!(
        outcome
            .instance
            .expect("new instance should exist")
            .supervisor
            .pid,
        20_000
    );
}

#[test]
fn stale_cleanup_preserves_a_regular_file_at_the_socket_path() {
    let directory = TestDirectory::new();
    let (lifecycle, process, _) = fixture(&directory);
    let paths = StatePaths::for_root(directory.path.join("state"));
    let stale = ProcessIdentity {
        pid: 9_002,
        start_identity: "stale-start".to_owned(),
    };
    process.set_identity(stale.pid, Some(&stale.start_identity));
    let ownership = SupervisorOwnership::acquire(paths.clone(), stale.clone(), 1, process.as_ref())
        .expect("stale owner should acquire");
    fs::write(&paths.ipc_socket, b"preserve me").expect("fixture file should write");
    std::mem::forget(ownership);
    process.set_identity(stale.pid, None);

    let error = lifecycle.start().expect_err("unsafe cleanup should fail");

    assert_eq!(error.kind(), DaemonErrorKind::UnsafeStaleState);
    assert_eq!(error.stage(), Some(StartupStage::StaleCleanup));
    assert_eq!(
        fs::read(&paths.ipc_socket).expect("regular file should remain"),
        b"preserve me"
    );
    assert_eq!(process.spawn_count(), 0);
}

#[test]
fn stale_cleanup_preserves_a_regular_file_at_the_control_socket_path() {
    let directory = TestDirectory::new();
    let (lifecycle, process, _) = fixture(&directory);
    let paths = StatePaths::for_root(directory.path.join("state"));
    let stale = ProcessIdentity {
        pid: 9_102,
        start_identity: "stale-control-start".to_owned(),
    };
    process.set_identity(stale.pid, Some(&stale.start_identity));
    let ownership = SupervisorOwnership::acquire(paths.clone(), stale.clone(), 1, process.as_ref())
        .expect("stale owner should acquire");
    fs::write(&paths.shutdown_socket, b"preserve control")
        .expect("fixture control file should write");
    std::mem::forget(ownership);
    process.set_identity(stale.pid, None);

    let error = lifecycle.start().expect_err("unsafe cleanup should fail");

    assert_eq!(error.kind(), DaemonErrorKind::UnsafeStaleState);
    assert_eq!(error.stage(), Some(StartupStage::StaleCleanup));
    assert_eq!(
        fs::read(&paths.shutdown_socket).expect("regular control file should remain"),
        b"preserve control"
    );
    assert_eq!(process.spawn_count(), 0);
}

#[test]
fn stale_cleanup_preserves_a_symlinked_instance_record_and_its_target() {
    let directory = TestDirectory::new();
    let (lifecycle, process, _) = fixture(&directory);
    let paths = StatePaths::for_root(directory.path.join("state"));
    paths.prepare().expect("state paths should prepare");
    let target = directory.path.join("valuable.json");
    fs::write(&target, b"valuable state").expect("target file should write");
    symlink(&target, &paths.instance_record).expect("fixture symlink should be created");

    let error = lifecycle
        .start()
        .expect_err("symlinked record should be preserved");

    assert_eq!(error.kind(), DaemonErrorKind::UnsafeStaleState);
    assert_eq!(error.stage(), Some(StartupStage::StaleCleanup));
    assert_eq!(
        fs::read(&target).expect("target should remain"),
        b"valuable state"
    );
    assert!(
        fs::symlink_metadata(&paths.instance_record)
            .expect("symlink should remain")
            .file_type()
            .is_symlink()
    );
    assert_eq!(process.spawn_count(), 0);
}

#[test]
fn stale_cleanup_requires_the_lease_and_instance_tokens_to_match() {
    let directory = TestDirectory::new();
    let (lifecycle, process, _) = fixture(&directory);
    let paths = StatePaths::for_root(directory.path.join("state"));
    let other_paths = StatePaths::for_root(directory.path.join("other"));
    let first_process = ProcessIdentity {
        pid: 9_003,
        start_identity: "first-stale-start".to_owned(),
    };
    let second_process = ProcessIdentity {
        pid: 9_004,
        start_identity: "second-stale-start".to_owned(),
    };
    process.set_identity(first_process.pid, Some(&first_process.start_identity));
    process.set_identity(second_process.pid, Some(&second_process.start_identity));
    let first =
        SupervisorOwnership::acquire(paths.clone(), first_process.clone(), 1, process.as_ref())
            .expect("first owner should acquire");
    let second = SupervisorOwnership::acquire(
        other_paths.clone(),
        second_process.clone(),
        2,
        process.as_ref(),
    )
    .expect("second owner should acquire");
    let listener = UnixListener::bind(&paths.ipc_socket).expect("stale socket should bind");
    fs::copy(&other_paths.instance_record, &paths.instance_record)
        .expect("foreign instance record should replace the fixture record");
    fs::set_permissions(&paths.instance_record, fs::Permissions::from_mode(0o600))
        .expect("instance permissions should be private");
    std::mem::forget(first);
    std::mem::forget(second);
    drop(listener);
    process.set_identity(first_process.pid, None);
    process.set_identity(second_process.pid, None);

    let error = lifecycle
        .start()
        .expect_err("token mismatch should block cleanup");

    assert_eq!(error.kind(), DaemonErrorKind::UnsafeStaleState);
    assert_eq!(error.stage(), Some(StartupStage::StaleCleanup));
    assert!(paths.instance_record.exists());
    assert!(paths.ipc_socket.exists());
    assert_eq!(process.spawn_count(), 0);
}

#[test]
fn readiness_timeout_terminates_only_the_spawned_process() {
    let directory = TestDirectory::new();
    let (lifecycle, process, _) = fixture(&directory);
    process.queue(SpawnBehavior::Silent);

    let error = lifecycle.start().expect_err("silent child should time out");

    assert_eq!(error.kind(), DaemonErrorKind::StartupTimedOut);
    assert_eq!(error.stage(), Some(StartupStage::Readiness));
    assert_eq!(process.terminate_count(), 1);
    assert!(
        process
            .identity(20_000)
            .expect("identity lookup should work")
            .is_none()
    );
}

#[test]
fn child_startup_failure_preserves_stage_category_and_safe_message() {
    let directory = TestDirectory::new();
    let (lifecycle, process, _) = fixture(&directory);
    process.queue(SpawnBehavior::Failure(ReadinessFailure {
        stage: StartupStage::CoreReadiness,
        category: StartupFailureCategory::Configuration,
        message: "the committed generation failed validation".to_owned(),
    }));

    let error = lifecycle
        .start()
        .expect_err("reported failure should surface");

    assert_eq!(error.kind(), DaemonErrorKind::StartupRejected);
    assert_eq!(error.stage(), Some(StartupStage::CoreReadiness));
    assert_eq!(
        error.category(),
        Some(StartupFailureCategory::Configuration)
    );
    assert_eq!(
        error.detail(),
        Some("the committed generation failed validation")
    );
    assert_eq!(process.terminate_count(), 1);
}

#[test]
fn child_failure_is_preserved_when_the_process_exits_before_identity_observation() {
    let directory = TestDirectory::new();
    let (lifecycle, process, _) = fixture(&directory);
    process.queue(SpawnBehavior::FailureThenExit(ReadinessFailure {
        stage: StartupStage::SupervisorInitialization,
        category: StartupFailureCategory::Permission,
        message: "the state root is unavailable".to_owned(),
    }));

    let error = lifecycle
        .start()
        .expect_err("early child failure should surface");

    assert_eq!(error.kind(), DaemonErrorKind::StartupRejected);
    assert_eq!(error.stage(), Some(StartupStage::SupervisorInitialization));
    assert_eq!(error.category(), Some(StartupFailureCategory::Permission));
    assert_eq!(error.detail(), Some("the state root is unavailable"));
}

#[test]
fn readiness_from_another_process_is_rejected_and_cleaned() {
    let directory = TestDirectory::new();
    let (lifecycle, process, _) = fixture(&directory);
    process.queue(SpawnBehavior::WrongProcess);

    let error = lifecycle
        .start()
        .expect_err("wrong process readiness should fail");

    assert_eq!(error.kind(), DaemonErrorKind::InvalidReadiness);
    assert_eq!(process.terminate_count(), 1);
    assert!(!directory.path.join("state/instance.json").exists());
    assert!(!directory.path.join("state/supervisor.sock").exists());
}

#[test]
fn mismatched_shutdown_acknowledgement_leaves_the_live_instance_untouched() {
    let directory = TestDirectory::new();
    let (lifecycle, process, shutdown) = fixture(&directory);
    let running = lifecycle.start().expect("start should succeed");
    shutdown.set_behavior(ShutdownBehavior::WrongAcknowledgement);

    let error = lifecycle
        .stop()
        .expect_err("mismatched acknowledgement should fail");

    assert_eq!(
        error.kind(),
        DaemonErrorKind::InvalidShutdownAcknowledgement
    );
    let record = running.instance.expect("running record should exist");
    assert_eq!(
        process
            .identity(record.supervisor.pid)
            .expect("identity lookup should work")
            .as_deref(),
        Some(record.supervisor.start_identity.as_str())
    );
    assert!(directory.path.join("state/instance.json").exists());
    assert!(directory.path.join("state/supervisor.sock").exists());
}

#[test]
fn shutdown_acknowledgement_requires_observed_process_exit() {
    let directory = TestDirectory::new();
    let (lifecycle, process, shutdown) = fixture(&directory);
    let running = lifecycle.start().expect("start should succeed");
    shutdown.set_behavior(ShutdownBehavior::AcknowledgeOnly);

    let error = lifecycle
        .stop()
        .expect_err("live process should reach the deadline");

    assert_eq!(error.kind(), DaemonErrorKind::ShutdownTimedOut);
    let record = running.instance.expect("running record should exist");
    assert!(
        process
            .identity(record.supervisor.pid)
            .expect("identity lookup should work")
            .is_some()
    );
    assert!(directory.path.join("state/instance.json").exists());
}
