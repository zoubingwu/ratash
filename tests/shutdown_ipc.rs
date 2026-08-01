use std::fs;
use std::io;
use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use hopash::application::{
    ApplicationClient, ApplicationError, ApplicationOperation, ApplicationOutput,
    ApplicationService,
};
use hopash::daemon::{ShutdownAcknowledgement, ShutdownIntent};
use hopash::ipc::{IPC_PROTOCOL_VERSION, PeerAuthorizationError, PeerAuthorizer};
use hopash::ipc_runtime::{IpcClient, IpcServer, IpcServerConfig};
use hopash::lifecycle::ProcessIdentity;
use hopash::shutdown_ipc::{
    ShutdownControlError, ShutdownControlHandler, ShutdownIpcServer, request_shutdown,
};

struct TestDirectory {
    path: PathBuf,
}

impl TestDirectory {
    fn new(label: &str) -> Self {
        let unique = uuid::Uuid::new_v4().simple().to_string();
        let path = PathBuf::from("/tmp").join(format!(
            "hsi-{label}-{}-{}",
            std::process::id(),
            &unique[..8]
        ));
        fs::create_dir_all(&path).expect("the fixture directory should be created");
        Self { path }
    }

    fn socket(&self, name: &str) -> PathBuf {
        self.path.join(name)
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

struct AllowPeer;

impl PeerAuthorizer for AllowPeer {
    fn authorize(&self, _peer: &UnixStream) -> Result<(), PeerAuthorizationError> {
        Ok(())
    }
}

struct ExactHandler {
    expected: ShutdownIntent,
    requested: AtomicBool,
}

impl ShutdownControlHandler for ExactHandler {
    fn request_shutdown(
        &self,
        intent: &ShutdownIntent,
    ) -> Result<ShutdownAcknowledgement, ShutdownControlError> {
        if intent != &self.expected {
            return Err(ShutdownControlError::Rejected);
        }
        self.requested.store(true, Ordering::Release);
        Ok(ShutdownAcknowledgement {
            process: intent.process.clone(),
            instance_token: intent.instance_token.clone(),
        })
    }
}

#[derive(Default)]
struct CountingHandler {
    calls: AtomicUsize,
}

impl ShutdownControlHandler for CountingHandler {
    fn request_shutdown(
        &self,
        _intent: &ShutdownIntent,
    ) -> Result<ShutdownAcknowledgement, ShutdownControlError> {
        self.calls.fetch_add(1, Ordering::AcqRel);
        Err(ShutdownControlError::Internal)
    }
}

struct BlockingShutdownHandler {
    entered: AtomicBool,
    gate: (Mutex<bool>, Condvar),
}

impl BlockingShutdownHandler {
    fn new() -> Self {
        Self {
            entered: AtomicBool::new(false),
            gate: (Mutex::new(false), Condvar::new()),
        }
    }

    fn wait_until_entered(&self) {
        let deadline = Instant::now() + Duration::from_secs(2);
        while !self.entered.load(Ordering::Acquire) {
            assert!(
                Instant::now() < deadline,
                "the shutdown handler should enter"
            );
            thread::yield_now();
        }
    }

    fn release(&self) {
        *self.gate.0.lock().expect("the fixture gate should lock") = true;
        self.gate.1.notify_all();
    }
}

impl ShutdownControlHandler for BlockingShutdownHandler {
    fn request_shutdown(
        &self,
        intent: &ShutdownIntent,
    ) -> Result<ShutdownAcknowledgement, ShutdownControlError> {
        self.entered.store(true, Ordering::Release);
        let guard = self.gate.0.lock().expect("the fixture gate should lock");
        let _guard = self
            .gate
            .1
            .wait_while(guard, |released| !*released)
            .expect("the fixture gate should remain available");
        Ok(ShutdownAcknowledgement {
            process: intent.process.clone(),
            instance_token: intent.instance_token.clone(),
        })
    }
}

struct BlockingApplication {
    entered: AtomicBool,
    gate: (Mutex<bool>, Condvar),
}

impl BlockingApplication {
    fn new() -> Self {
        Self {
            entered: AtomicBool::new(false),
            gate: (Mutex::new(false), Condvar::new()),
        }
    }

    fn wait_until_entered(&self) {
        let deadline = Instant::now() + Duration::from_secs(2);
        while !self.entered.load(Ordering::Acquire) {
            assert!(
                Instant::now() < deadline,
                "the main IPC request should enter"
            );
            thread::yield_now();
        }
    }

    fn release(&self) {
        *self.gate.0.lock().expect("the fixture gate should lock") = true;
        self.gate.1.notify_all();
    }
}

impl ApplicationClient for BlockingApplication {
    fn execute(
        &self,
        _operation: ApplicationOperation,
    ) -> Result<ApplicationOutput, ApplicationError> {
        self.entered.store(true, Ordering::Release);
        let guard = self.gate.0.lock().expect("the fixture gate should lock");
        let _guard = self
            .gate
            .1
            .wait_while(guard, |released| !*released)
            .expect("the fixture gate should remain available");
        Ok(ApplicationOutput::Status(
            ApplicationService::new().status(),
        ))
    }
}

#[test]
fn shutdown_identity_round_trips_on_the_dedicated_channel() {
    let directory = TestDirectory::new("round-trip");
    let socket = directory.socket("control.sock");
    let intent = fixture_intent();
    let handler = Arc::new(ExactHandler {
        expected: intent.clone(),
        requested: AtomicBool::new(false),
    });
    let mut server = ShutdownIpcServer::start(
        &socket,
        Arc::clone(&handler),
        Arc::new(AllowPeer),
        Duration::from_secs(1),
    )
    .expect("the shutdown IPC server should start");

    let acknowledgement = request_shutdown(&socket, &intent, Duration::from_secs(1))
        .expect("the exact shutdown identity should be acknowledged");

    assert_eq!(acknowledgement.process, intent.process);
    assert_eq!(acknowledgement.instance_token, intent.instance_token);
    assert!(handler.requested.load(Ordering::Acquire));
    server.shutdown().expect("the shutdown server should stop");
    assert!(!socket.exists());
}

#[test]
fn rejected_identity_cannot_request_shutdown() {
    let directory = TestDirectory::new("rejected");
    let socket = directory.socket("control.sock");
    let intent = fixture_intent();
    let handler = Arc::new(ExactHandler {
        expected: intent.clone(),
        requested: AtomicBool::new(false),
    });
    let mut server = ShutdownIpcServer::start(
        &socket,
        Arc::clone(&handler),
        Arc::new(AllowPeer),
        Duration::from_secs(1),
    )
    .expect("the shutdown IPC server should start");
    let mut wrong = intent;
    wrong.instance_token = "wrong-token".to_owned();

    let error = request_shutdown(&socket, &wrong, Duration::from_secs(1))
        .expect_err("the wrong shutdown identity should be rejected");

    assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
    assert!(!handler.requested.load(Ordering::Acquire));
    server.shutdown().expect("the shutdown server should stop");
}

#[test]
fn blocked_main_ipc_work_cannot_starve_shutdown_control() {
    let directory = TestDirectory::new("independent");
    let main_socket = directory.socket("main.sock");
    let control_socket = directory.socket("control.sock");
    let application = Arc::new(BlockingApplication::new());
    let mut main_server = IpcServer::start(
        &main_socket,
        Arc::clone(&application),
        Arc::new(AllowPeer),
        IpcServerConfig {
            io_timeout: Duration::from_secs(1),
            worker_count: 1,
            pending_connection_capacity: 1,
        },
    )
    .expect("the main IPC server should start");
    let main_client =
        IpcClient::with_timeouts(&main_socket, Duration::from_secs(1), Duration::from_secs(1));
    let blocked = thread::spawn(move || main_client.execute(ApplicationOperation::GetStatus));
    application.wait_until_entered();

    let intent = fixture_intent();
    let handler = Arc::new(ExactHandler {
        expected: intent.clone(),
        requested: AtomicBool::new(false),
    });
    let mut control_server = ShutdownIpcServer::start(
        &control_socket,
        Arc::clone(&handler),
        Arc::new(AllowPeer),
        Duration::from_secs(1),
    )
    .expect("the control IPC server should start");
    let started = Instant::now();

    request_shutdown(&control_socket, &intent, Duration::from_secs(1))
        .expect("shutdown control should remain responsive");

    assert!(started.elapsed() < Duration::from_millis(250));
    assert!(handler.requested.load(Ordering::Acquire));
    application.release();
    blocked
        .join()
        .expect("the main IPC client should finish")
        .expect("the main IPC response should complete");
    control_server
        .shutdown()
        .expect("the control IPC server should stop");
    main_server
        .shutdown()
        .expect("the main IPC server should stop");
}

#[test]
fn main_ipc_shutdown_deadline_does_not_join_a_blocked_handler() {
    let directory = TestDirectory::new("main-deadline");
    let main_socket = directory.socket("main.sock");
    let application = Arc::new(BlockingApplication::new());
    let mut main_server = IpcServer::start(
        &main_socket,
        Arc::clone(&application),
        Arc::new(AllowPeer),
        IpcServerConfig {
            io_timeout: Duration::from_secs(30),
            worker_count: 1,
            pending_connection_capacity: 1,
        },
    )
    .expect("the main IPC server should start");
    let main_client = IpcClient::with_timeouts(
        &main_socket,
        Duration::from_secs(1),
        Duration::from_secs(30),
    );
    let blocked = thread::spawn(move || main_client.execute(ApplicationOperation::GetStatus));
    application.wait_until_entered();
    let started = Instant::now();

    let error = main_server
        .shutdown_until(Instant::now() + Duration::from_millis(25))
        .expect_err("the blocked handler should reach the absolute shutdown deadline");

    assert_eq!(error.kind(), io::ErrorKind::TimedOut);
    assert!(started.elapsed() < Duration::from_millis(500));
    assert!(!main_socket.exists());
    application.release();
    let _ = blocked.join().expect("the main IPC client should finish");
}

#[test]
fn idle_control_server_uses_a_handler_free_fast_shutdown_wake() {
    let directory = TestDirectory::new("idle-shutdown");
    let socket = directory.socket("control.sock");
    let handler = Arc::new(CountingHandler::default());
    let mut server = ShutdownIpcServer::start(
        &socket,
        Arc::clone(&handler),
        Arc::new(AllowPeer),
        Duration::from_secs(1),
    )
    .expect("the shutdown IPC server should start");

    thread::sleep(Duration::from_millis(100));
    assert_eq!(handler.calls.load(Ordering::Acquire), 0);
    let started = Instant::now();
    server.shutdown().expect("the idle server should stop");

    assert!(started.elapsed() < Duration::from_millis(250));
    assert_eq!(handler.calls.load(Ordering::Acquire), 0);
    assert!(!socket.exists());
}

#[test]
fn control_ipc_shutdown_deadline_does_not_join_a_blocked_handler() {
    let directory = TestDirectory::new("control-deadline");
    let socket = directory.socket("control.sock");
    let intent = fixture_intent();
    let handler = Arc::new(BlockingShutdownHandler::new());
    let mut server = ShutdownIpcServer::start(
        &socket,
        Arc::clone(&handler),
        Arc::new(AllowPeer),
        Duration::from_secs(30),
    )
    .expect("the control IPC server should start");
    let client_socket = socket.clone();
    let client_intent = intent.clone();
    let blocked = thread::spawn(move || {
        request_shutdown(&client_socket, &client_intent, Duration::from_secs(30))
    });
    handler.wait_until_entered();
    let started = Instant::now();

    let error = server
        .shutdown_until(Instant::now() + Duration::from_millis(25))
        .expect_err("the blocked handler should reach the absolute shutdown deadline");

    assert_eq!(error.kind(), io::ErrorKind::TimedOut);
    assert!(started.elapsed() < Duration::from_millis(500));
    assert!(!socket.exists());
    handler.release();
    let _ = blocked
        .join()
        .expect("the shutdown IPC client should finish");
}

#[test]
fn main_ipc_shutdown_closes_an_idle_accepted_connection() {
    let directory = TestDirectory::new("main-idle-connection");
    let socket = directory.socket("main.sock");
    let mut server = IpcServer::start(
        &socket,
        Arc::new(ApplicationService::new()),
        Arc::new(AllowPeer),
        IpcServerConfig {
            io_timeout: Duration::from_secs(30),
            worker_count: 1,
            pending_connection_capacity: 1,
        },
    )
    .expect("the main IPC server should start");
    let _idle = UnixStream::connect(&socket).expect("the idle client should connect");
    thread::sleep(Duration::from_millis(50));
    let started = Instant::now();

    server
        .shutdown()
        .expect("main IPC shutdown should cancel the idle connection");

    assert!(started.elapsed() < Duration::from_millis(250));
    assert!(!socket.exists());
}

#[test]
fn control_ipc_shutdown_closes_an_idle_accepted_connection() {
    let directory = TestDirectory::new("control-idle-connection");
    let socket = directory.socket("control.sock");
    let handler = Arc::new(CountingHandler::default());
    let mut server = ShutdownIpcServer::start(
        &socket,
        Arc::clone(&handler),
        Arc::new(AllowPeer),
        Duration::from_secs(30),
    )
    .expect("the control IPC server should start");
    let _idle = UnixStream::connect(&socket).expect("the idle client should connect");
    thread::sleep(Duration::from_millis(50));
    let started = Instant::now();

    server
        .shutdown()
        .expect("control IPC shutdown should cancel the idle connection");

    assert!(started.elapsed() < Duration::from_millis(250));
    assert_eq!(handler.calls.load(Ordering::Acquire), 0);
    assert!(!socket.exists());
}

fn fixture_intent() -> ShutdownIntent {
    ShutdownIntent {
        process: ProcessIdentity {
            pid: 42,
            start_identity: "fixture-start".to_owned(),
        },
        instance_token: "fixture-instance-token".to_owned(),
        protocol_version: IPC_PROTOCOL_VERSION,
    }
}
