use super::*;
use std::sync::atomic::AtomicUsize;

#[test]
fn core_process_controller_startup_errors_preserve_kind_and_hide_diagnostics() {
    let error = core_process_controller_startup_error(io::Error::new(
        io::ErrorKind::PermissionDenied,
        "/private/sensitive/controller-path",
    ));

    assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
    assert_eq!(
        error.to_string(),
        "the Core process controller could not initialize"
    );
}

#[cfg(all(target_os = "macos", feature = "local-unsigned"))]
#[test]
fn local_unsigned_validation_flags_keep_static_options_off_running_code() {
    assert!(!dynamic_code_validation_flags().contains(CodeSigningFlags::CHECK_ALL_ARCHITECTURES));
    assert!(static_code_validation_flags().contains(CodeSigningFlags::CHECK_ALL_ARCHITECTURES));
}

#[cfg(all(
    target_os = "macos",
    target_arch = "aarch64",
    feature = "local-unsigned"
))]
#[test]
fn local_unsigned_dynamic_code_policy_accepts_running_code() {
    let code = SecCode::for_self(CodeSigningFlags::NONE)
        .expect("the test process should have a dynamic code object");
    let requirement: SecRequirement = "true"
        .parse()
        .expect("the unconditional code requirement should parse");

    code.check_validity(dynamic_code_validation_flags(), &requirement)
        .expect("the local unsigned dynamic code policy should be valid for running code");
}

#[derive(Default)]
struct TestShutdownSignal {
    requested: AtomicBool,
    waker: Mutex<Option<RuntimeWaker>>,
}

impl TestShutdownSignal {
    fn request(&self) {
        self.requested.store(true, Ordering::Release);
        if let Some(waker) = self
            .waker
            .lock()
            .expect("the signal waker should lock")
            .as_ref()
        {
            waker.wake();
        }
    }
}

impl ProcessShutdownSignal for TestShutdownSignal {
    fn shutdown_requested(&self) -> bool {
        self.requested.load(Ordering::Acquire)
    }

    fn install_waker(&self, waker: RuntimeWaker) {
        *self.waker.lock().expect("the signal waker should lock") = Some(waker);
    }
}

struct FixedMaintenanceClock;

impl RuntimeClock for FixedMaintenanceClock {
    fn now(&self) -> Duration {
        Duration::ZERO
    }

    fn now_unix_ms(&self) -> u64 {
        0
    }
}

struct RequestingWaiter {
    signal: Arc<TestShutdownSignal>,
    timeouts: Mutex<Vec<Option<Duration>>>,
}

impl RuntimeWaiter for RequestingWaiter {
    fn checkpoint(&self) -> u64 {
        0
    }

    fn wait(&self, _checkpoint: u64, timeout: Option<Duration>) {
        self.timeouts
            .lock()
            .expect("the timeout log should lock")
            .push(timeout);
        self.signal.request();
    }
}

#[test]
fn core_service_maintenance_waits_for_the_reported_deadline() {
    let signal = Arc::new(TestShutdownSignal::default());
    let waiter = RequestingWaiter {
        signal: Arc::clone(&signal),
        timeouts: Mutex::new(Vec::new()),
    };
    let calls = AtomicUsize::new(0);
    run_core_service_maintenance_loop(
        signal.as_ref(),
        &FixedMaintenanceClock,
        &waiter,
        RuntimeWaker::default(),
        |now| {
            calls.fetch_add(1, Ordering::Relaxed);
            now + CORE_SERVICE_LIVENESS_INTERVAL
        },
    );

    assert_eq!(calls.load(Ordering::Relaxed), 1);
    assert_eq!(
        waiter
            .timeouts
            .lock()
            .expect("the timeout log should lock")
            .as_slice(),
        &[Some(CORE_SERVICE_LIVENESS_INTERVAL)]
    );
}

#[test]
fn supervisor_shutdown_wait_has_no_periodic_deadline() {
    let signal = Arc::new(TestShutdownSignal::default());
    let waiter = RequestingWaiter {
        signal: Arc::clone(&signal),
        timeouts: Mutex::new(Vec::new()),
    };
    let drain = DrainController::default();

    wait_for_supervisor_shutdown(&drain, signal.as_ref(), &waiter, RuntimeWaker::default());

    assert!(drain.is_requested());
    assert_eq!(
        waiter
            .timeouts
            .lock()
            .expect("the timeout log should lock")
            .as_slice(),
        &[None]
    );
}

#[test]
fn drain_request_wakes_an_idle_supervisor_wait() {
    let signal = Arc::new(TestShutdownSignal::default());
    let drain = Arc::new(DrainController::default());
    let worker_signal = Arc::clone(&signal);
    let worker_drain = Arc::clone(&drain);
    let (done_sender, done_receiver) = mpsc::sync_channel(1);
    let worker = thread::spawn(move || {
        let wake = RuntimeWaker::default();
        wait_for_supervisor_shutdown(
            worker_drain.as_ref(),
            worker_signal.as_ref(),
            &wake,
            wake.clone(),
        );
        done_sender
            .send(())
            .expect("the fixture should report shutdown");
    });
    let install_deadline = std::time::Instant::now() + Duration::from_secs(1);
    loop {
        if drain
            .waker
            .lock()
            .expect("the drain waker should lock")
            .is_some()
        {
            break;
        }
        assert!(
            std::time::Instant::now() < install_deadline,
            "the shutdown wait should install its drain waker"
        );
        thread::yield_now();
    }

    drain.request();

    done_receiver
        .recv_timeout(Duration::from_secs(1))
        .expect("the drain request should wake the idle wait");
    worker.join().expect("the shutdown waiter should join");
}

#[test]
fn core_service_signal_wakes_a_long_deadline_immediately() {
    let signal = Arc::new(TestShutdownSignal::default());
    let worker_signal = Arc::clone(&signal);
    let (started_sender, started_receiver) = mpsc::sync_channel(1);
    let (done_sender, done_receiver) = mpsc::sync_channel(1);
    let worker = thread::spawn(move || {
        let wake = RuntimeWaker::default();
        run_core_service_maintenance_loop(
            worker_signal.as_ref(),
            &FixedMaintenanceClock,
            &wake,
            wake.clone(),
            |_| {
                started_sender
                    .send(())
                    .expect("the fixture should report maintenance");
                Duration::from_secs(60 * 60)
            },
        );
        done_sender
            .send(())
            .expect("the fixture should report shutdown");
    });
    started_receiver
        .recv_timeout(Duration::from_secs(1))
        .expect("maintenance should reach the long wait");

    signal.request();

    done_receiver
        .recv_timeout(Duration::from_secs(1))
        .expect("the signal should wake the long wait");
    worker.join().expect("the maintenance worker should join");
}

struct TestDirectory {
    path: PathBuf,
}

impl TestDirectory {
    fn new(label: &str) -> Self {
        let unique = uuid::Uuid::new_v4().simple().to_string();
        let path = PathBuf::from("/tmp").join(format!(
            "hopash-production-{label}-{}-{}",
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

fn fixture_shutdown_intent() -> ShutdownIntent {
    ShutdownIntent {
        process: ProcessIdentity {
            pid: 42,
            start_identity: "fixture-start".to_owned(),
        },
        instance_token: "fixture-instance-token".to_owned(),
        protocol_version: crate::ipc::IPC_PROTOCOL_VERSION,
    }
}

fn record_stage(stages: &Mutex<Vec<&'static str>>, stage: &'static str) -> io::Result<()> {
    stages
        .lock()
        .expect("the shutdown stage log should lock")
        .push(stage);
    Ok(())
}

struct MismatchedAcknowledgementHandler;

impl ShutdownControlHandler for MismatchedAcknowledgementHandler {
    fn request_shutdown(
        &self,
        intent: &ShutdownIntent,
    ) -> Result<ShutdownAcknowledgement, ShutdownControlError> {
        Ok(ShutdownAcknowledgement {
            process: intent.process.clone(),
            instance_token: "mismatched-instance-token".to_owned(),
        })
    }
}

struct BlockingMutationApplication {
    entered: AtomicBool,
    gate: (Mutex<bool>, Condvar),
}

impl BlockingMutationApplication {
    fn new() -> Self {
        Self {
            entered: AtomicBool::new(false),
            gate: (Mutex::new(false), Condvar::new()),
        }
    }

    fn wait_until_entered(&self) {
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while !self.entered.load(Ordering::Acquire) {
            assert!(
                std::time::Instant::now() < deadline,
                "the fixture mutation should enter"
            );
            thread::yield_now();
        }
    }

    fn release(&self) {
        *self.gate.0.lock().expect("the fixture gate should lock") = true;
        self.gate.1.notify_all();
    }
}

impl ApplicationClient for BlockingMutationApplication {
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

fn arguments(values: &[&str]) -> Vec<OsString> {
    values.iter().map(OsString::from).collect()
}

#[test]
fn core_service_mode_requires_exact_absolute_arguments() {
    let invocation = CoreServiceInvocation::parse_process_arguments(&arguments(&[
        "hopash",
        INTERNAL_CORE_SERVICE_MODE,
        "--owner-uid",
        "501",
        "--socket",
        "/private/var/run/hopash-rs/core.sock",
        "--runtime-root",
        "/private/var/db/hopash-rs/runtime",
        "--mihomo",
        "/Library/Application Support/Hopash RS/bin/mihomo",
    ]))
    .expect("the fixture invocation should parse")
    .expect("the Core service mode should be detected");

    assert_eq!(invocation.owner_uid, 501);
    assert!(invocation.socket_path.is_absolute());
    assert!(invocation.runtime_root.is_absolute());
    assert!(invocation.mihomo_binary.is_absolute());
}

#[test]
fn public_arguments_are_ignored_by_the_core_service_parser() {
    assert_eq!(
        CoreServiceInvocation::parse_process_arguments(&arguments(&["hopash", "status"]))
            .expect("public arguments should be valid"),
        None
    );
}

#[test]
fn internal_core_service_arguments_reject_relative_paths() {
    let error = CoreServiceInvocation::parse_process_arguments(&arguments(&[
        "hopash",
        INTERNAL_CORE_SERVICE_MODE,
        "--owner-uid",
        "501",
        "--socket",
        "core.sock",
        "--runtime-root",
        "/private/var/db/hopash-rs/runtime",
        "--mihomo",
        "/Library/Application Support/Hopash RS/bin/mihomo",
    ]))
    .expect_err("relative service paths must fail closed");
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
}

#[test]
fn production_shutdown_control_drains_through_both_ipc_servers_in_order() {
    let directory = TestDirectory::new("shutdown");
    let main_socket = directory.socket("main.sock");
    let control_socket = directory.socket("control.sock");
    let drain = Arc::new(DrainController::default());
    let application = Arc::new(SupervisorApplication {
        supervisor: Arc::new(ApplicationService::new()),
        drain: Arc::clone(&drain),
    });
    let mut main_server = IpcServer::start(
        &main_socket,
        application,
        Arc::new(SameUserPeerAuthorizer::current()),
        IpcServerConfig::default(),
    )
    .expect("the fixture main IPC server should start");
    let intent = fixture_shutdown_intent();
    let handler = Arc::new(ProductionShutdownHandler {
        process: intent.process.clone(),
        instance_token: intent.instance_token.clone(),
        drain: Arc::clone(&drain),
    });
    let mut control_server = ShutdownIpcServer::start(
        &control_socket,
        handler,
        Arc::new(SameUserPeerAuthorizer::current()),
        Duration::from_secs(1),
    )
    .expect("the fixture control IPC server should start");
    let main_client =
        IpcClient::with_timeouts(&main_socket, Duration::from_secs(1), Duration::from_secs(1));

    let lifecycle_error = main_client
        .execute(ApplicationOperation::Stop)
        .expect_err("main IPC lifecycle control should be unavailable");
    assert_eq!(lifecycle_error.code, ErrorCode::OperationUnavailable);
    assert!(!drain.is_requested());

    let acknowledgement =
        request_shutdown_over_control(&control_socket, &intent, Duration::from_secs(1))
            .expect("the dedicated control request should be acknowledged");
    assert_eq!(acknowledgement.process, intent.process);
    assert_eq!(acknowledgement.instance_token, intent.instance_token);
    assert!(drain.is_requested());

    let mutation_error = main_client
        .execute(ApplicationOperation::ProfileUse {
            profile: "fixture".to_owned(),
        })
        .expect_err("mutations should stop entering during drain");
    assert_eq!(mutation_error.code, ErrorCode::OperationUnavailable);
    assert_eq!(mutation_error.message, "The Supervisor is shutting down");
    let status = main_client
        .execute(ApplicationOperation::GetStatus)
        .and_then(expect_status)
        .expect("reads should remain available during drain");
    assert_eq!(status.supervisor.lifecycle, SupervisorLifecycle::Stopping);

    let stages = Arc::new(Mutex::new(Vec::new()));
    let background_stages = Arc::clone(&stages);
    let observer_stages = Arc::clone(&stages);
    let core_stages = Arc::clone(&stages);
    let main_stages = Arc::clone(&stages);
    let control_stages = Arc::clone(&stages);
    run_shutdown_sequence(
        || record_stage(&background_stages, "background"),
        || record_stage(&observer_stages, "observer"),
        || record_stage(&core_stages, "core_owner"),
        || {
            record_stage(&main_stages, "main_ipc")?;
            main_server.shutdown()
        },
        || {
            record_stage(&control_stages, "control_ipc")?;
            control_server.shutdown()
        },
    )
    .expect("the production shutdown sequence should complete");

    assert_eq!(
        *stages.lock().expect("the shutdown stage log should lock"),
        [
            "background",
            "observer",
            "core_owner",
            "main_ipc",
            "control_ipc"
        ]
    );
    assert!(!main_socket.exists());
    assert!(!control_socket.exists());
}

#[test]
fn foreground_shutdown_port_rejects_a_mismatched_acknowledgement() {
    let directory = TestDirectory::new("shutdown-ack");
    let socket = directory.socket("control.sock");
    let mut server = ShutdownIpcServer::start(
        &socket,
        Arc::new(MismatchedAcknowledgementHandler),
        Arc::new(SameUserPeerAuthorizer::current()),
        Duration::from_secs(1),
    )
    .expect("the fixture control IPC server should start");
    let port = ShutdownControlPort::new(socket);

    let error = port
        .request_shutdown(&fixture_shutdown_intent(), Duration::from_secs(1))
        .expect_err("a mismatched acknowledgement should fail closed");

    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    server
        .shutdown()
        .expect("the fixture control IPC server should stop");
}

#[test]
fn drain_rejects_new_mutations_and_waits_for_an_active_mutation() {
    let drain = Arc::new(DrainController::default());
    let delegate = Arc::new(BlockingMutationApplication::new());
    let application = Arc::new(SupervisorApplication {
        supervisor: Arc::clone(&delegate),
        drain: Arc::clone(&drain),
    });
    let active_application = Arc::clone(&application);
    let active = thread::spawn(move || {
        active_application.execute(ApplicationOperation::ProfileUse {
            profile: "active".to_owned(),
        })
    });
    delegate.wait_until_entered();
    drain.request();

    let error = application
        .execute(ApplicationOperation::ProfileUse {
            profile: "rejected".to_owned(),
        })
        .expect_err("a new mutation should be rejected during drain");
    assert_eq!(error.code, ErrorCode::OperationUnavailable);
    let (finished_sender, finished_receiver) = mpsc::sync_channel(1);
    let waiting_drain = Arc::clone(&drain);
    let waiter = thread::spawn(move || {
        let drained = waiting_drain.wait_for_mutations(Duration::from_secs(1));
        finished_sender
            .send(drained)
            .expect("the fixture completion should send");
    });
    assert!(
        finished_receiver
            .recv_timeout(Duration::from_millis(50))
            .is_err()
    );

    delegate.release();
    active
        .join()
        .expect("the active mutation thread should finish")
        .expect("the active mutation should complete");
    finished_receiver
        .recv_timeout(Duration::from_secs(1))
        .expect("drain should finish after the active mutation");
    waiter.join().expect("the drain waiter should finish");
}

#[test]
fn shutdown_cancels_a_stalled_profile_add_and_reaches_stopping_state() {
    let drain = Arc::new(DrainController::default());
    let delegate = Arc::new(BlockingMutationApplication::new());
    let release = Arc::clone(&delegate);
    drain.install_cancellation(Arc::new(move || release.release()));
    let application = Arc::new(SupervisorApplication {
        supervisor: Arc::clone(&delegate),
        drain: Arc::clone(&drain),
    });
    let active_application = Arc::clone(&application);
    let active = thread::spawn(move || {
        active_application.execute(ApplicationOperation::ProfileAdd {
            subscription_url: crate::domain::SubscriptionUrl::parse(
                "https://fixture.invalid/profile.yaml",
            )
            .expect("the fixture URL should parse"),
        })
    });
    delegate.wait_until_entered();

    drain.request();

    assert!(drain.wait_for_mutations(Duration::from_secs(1)));
    let output = active
        .join()
        .expect("the Profile Add worker should finish")
        .expect("the cancelled fixture should return its final status");
    let ApplicationOutput::Status(status) = output else {
        panic!("the fixture should return status");
    };
    assert_eq!(status.supervisor.lifecycle, SupervisorLifecycle::Stopping);
}

#[test]
fn drain_timeout_skips_core_teardown_until_the_mutation_finishes() {
    let drain = Arc::new(DrainController::default());
    let _mutation = drain
        .begin_mutation()
        .expect("the fixture mutation should enter");
    drain.request();
    let teardown_called = AtomicBool::new(false);

    let error = run_after_mutation_drain(&drain, Duration::from_millis(20), || {
        teardown_called.store(true, Ordering::Release);
        Ok(())
    })
    .expect_err("a stalled mutation should reach the drain deadline");

    assert_eq!(error.kind(), io::ErrorKind::TimedOut);
    assert!(!teardown_called.load(Ordering::Acquire));
}

#[test]
fn observer_shutdown_releases_a_stalled_thread_at_the_absolute_deadline() {
    let (release, blocked) = std::sync::mpsc::channel::<()>();
    let finished = Arc::new(AtomicBool::new(false));
    let worker_finished = Arc::clone(&finished);
    let mut observer = SupervisorObserver {
        shutdown: Arc::new(WakeSignal::default()),
        thread: Some(thread::spawn(move || {
            let _ = blocked.recv();
            worker_finished.store(true, Ordering::Release);
        })),
    };
    let started = Instant::now();

    let error = observer
        .shutdown_until(Instant::now() + Duration::from_millis(10))
        .expect_err("the stalled observer should reach the shutdown deadline");

    assert_eq!(error.kind(), io::ErrorKind::TimedOut);
    assert!(started.elapsed() < Duration::from_secs(1));
    assert!(observer.thread.is_none());
    release
        .send(())
        .expect("the fixture observer should release");
    let finish_deadline = Instant::now() + Duration::from_secs(1);
    while !finished.load(Ordering::Acquire) {
        assert!(
            Instant::now() < finish_deadline,
            "the detached fixture observer should finish"
        );
        thread::yield_now();
    }
}
