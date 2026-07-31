use std::collections::VecDeque;
use std::fs;
use std::io::{self, Read, Write};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use hopash::application::{
    ApplicationClient, ApplicationError, ApplicationErrorDetails, ApplicationOperation,
    ApplicationOutput, ApplicationService, LatencyFreshness, LatencyListOutcome,
    LatencyProbeStatus, LatencyShowOutcome, LatencySummary, LifecycleAction, LifecycleOutcome,
    LogGap, LogMetadata, PolicyTargetValidation, ProfileListOutcome, ProfileMutationAction,
    ProfileMutationOutcome, ProfileRefreshState, ProfileSummary, ProxyAvailability,
    ProxyGroupSummary, ProxyListOutcome, ProxyMemberKind, ProxyNodeRow, ProxyNodeSource,
    ProxySelectionOutcome, RecoveryOutcome, RecoveryStatus, RuleListOutcome, RuleMutationAction,
    RuleMutationOutcome, RulePlacement, RuleSummary, RuntimeApplyFailureStage, RuntimeApplyOutcome,
    RuntimeApplyStatus, SelectorCandidate, SelectorIdentity, SelectorKind,
};
use hopash::constants::IPC_FRAME_MAX_BYTES;
use hopash::domain::{
    LocalRuleSetRevision, NodeRecordId, ProbeGeneration, ProfileId, RuntimeGeneration,
    SubscriptionUrl,
};
use hopash::error::ErrorCode;
use hopash::ipc::{
    EmptyPayload, IPC_PROTOCOL_VERSION, IpcRequest, IpcResponse, LogSubscriptionPayload,
    PeerAuthorizationError, PeerAuthorizer, RequestId, RequestOperation, bind_private_listener,
    read_frame, write_frame,
};
use hopash::ipc_runtime::{IpcClient, IpcServer, IpcServerConfig, SameUserPeerAuthorizer};

static NEXT_TEMP_ID: AtomicU64 = AtomicU64::new(1);

struct TempSocket {
    directory: PathBuf,
    path: PathBuf,
}

impl TempSocket {
    fn new(label: &str) -> Self {
        let id = NEXT_TEMP_ID.fetch_add(1, Ordering::Relaxed);
        let directory =
            PathBuf::from("/tmp").join(format!("hopash-ipc-{label}-{}-{id}", std::process::id()));
        let path = directory.join("supervisor.sock");
        Self { directory, path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempSocket {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
        let _ = std::fs::remove_dir(&self.directory);
    }
}

#[derive(Debug)]
struct AllowPeer;

impl PeerAuthorizer for AllowPeer {
    fn authorize(&self, _peer: &UnixStream) -> Result<(), PeerAuthorizationError> {
        Ok(())
    }
}

#[derive(Debug)]
struct DenyPeer;

impl PeerAuthorizer for DenyPeer {
    fn authorize(&self, _peer: &UnixStream) -> Result<(), PeerAuthorizationError> {
        Err(PeerAuthorizationError::new("fixture peer rejected"))
    }
}

struct QueuedClient {
    results: Mutex<VecDeque<Result<ApplicationOutput, ApplicationError>>>,
}

impl QueuedClient {
    fn new(results: Vec<Result<ApplicationOutput, ApplicationError>>) -> Self {
        Self {
            results: Mutex::new(results.into()),
        }
    }
}

impl ApplicationClient for QueuedClient {
    fn execute(
        &self,
        _operation: ApplicationOperation,
    ) -> Result<ApplicationOutput, ApplicationError> {
        self.results
            .lock()
            .expect("fixture result queue should remain available")
            .pop_front()
            .expect("fixture should provide one result per request")
    }
}

struct CountingClient {
    calls: AtomicUsize,
}

impl CountingClient {
    fn new() -> Self {
        Self {
            calls: AtomicUsize::new(0),
        }
    }
}

impl ApplicationClient for CountingClient {
    fn execute(
        &self,
        _operation: ApplicationOperation,
    ) -> Result<ApplicationOutput, ApplicationError> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        Ok(ApplicationOutput::Status(
            ApplicationService::new().status(),
        ))
    }
}

struct BlockingClient {
    entered: AtomicUsize,
    gate: (Mutex<bool>, Condvar),
}

impl BlockingClient {
    fn new() -> Self {
        Self {
            entered: AtomicUsize::new(0),
            gate: (Mutex::new(false), Condvar::new()),
        }
    }

    fn wait_until_entered(&self, expected: usize) {
        let deadline = Instant::now() + Duration::from_secs(2);
        while self.entered.load(Ordering::Acquire) < expected {
            assert!(Instant::now() < deadline, "fixture request should enter");
            thread::yield_now();
        }
    }

    fn release(&self) {
        *self.gate.0.lock().expect("fixture gate should lock") = true;
        self.gate.1.notify_all();
    }
}

impl ApplicationClient for BlockingClient {
    fn execute(
        &self,
        _operation: ApplicationOperation,
    ) -> Result<ApplicationOutput, ApplicationError> {
        self.entered.fetch_add(1, Ordering::Release);
        let guard = self.gate.0.lock().expect("fixture gate should lock");
        let _guard = self
            .gate
            .1
            .wait_while(guard, |released| !*released)
            .expect("fixture gate should remain available");
        Ok(ApplicationOutput::Status(
            ApplicationService::new().status(),
        ))
    }
}

fn test_server_config() -> IpcServerConfig {
    IpcServerConfig {
        io_timeout: Duration::from_millis(500),
        worker_count: 4,
        pending_connection_capacity: 16,
    }
}

fn raw_request(path: &Path, request: &IpcRequest) -> UnixStream {
    let mut stream = UnixStream::connect(path).expect("fixture client should connect");
    stream
        .set_read_timeout(Some(Duration::from_secs(1)))
        .expect("fixture read timeout should configure");
    write_frame(&mut stream, request).expect("fixture request should write");
    stream
}

#[test]
fn one_shot_client_executes_through_the_authenticated_server() {
    let socket = TempSocket::new("round-trip");
    let mut server = IpcServer::start(
        socket.path(),
        Arc::new(ApplicationService::new()),
        Arc::new(SameUserPeerAuthorizer::current()),
        IpcServerConfig::default(),
    )
    .expect("server should start");

    let client = IpcClient::new(socket.path());
    let output = client
        .execute(ApplicationOperation::GetStatus)
        .expect("status should round trip");

    let ApplicationOutput::Status(status) = output else {
        panic!("status operation should return status output");
    };
    assert_eq!(
        status.core.lifecycle,
        hopash::domain::CoreLifecycle::Unconfigured
    );

    server.shutdown().expect("server should stop cleanly");
}

#[test]
fn every_one_shot_application_output_round_trips_with_typed_values() {
    let socket = TempSocket::new("all-outputs");
    let cases = application_output_cases();
    let results = cases.iter().map(|(_, output)| Ok(output.clone())).collect();
    let mut server = IpcServer::start(
        socket.path(),
        Arc::new(QueuedClient::new(results)),
        Arc::new(AllowPeer),
        test_server_config(),
    )
    .expect("server should start");
    let client = IpcClient::new(socket.path());

    for (operation, expected) in cases {
        assert_eq!(
            client.execute(operation).expect("output should round trip"),
            expected
        );
    }

    server.shutdown().expect("server should stop cleanly");
}

#[test]
fn log_metadata_is_encoded_and_decoded_before_operation_shape_validation() {
    let socket = TempSocket::new("log-metadata");
    let metadata = ApplicationOutput::LogMetadata(LogMetadata {
        first_sequence: Some(7),
        last_sequence: Some(9),
        next_sequence: Some(10),
        dropped_total: 3,
        gap: Some(LogGap {
            requested_after_sequence: 2,
            first_available_sequence: 7,
            dropped_count: 4,
        }),
    });
    let mut server = IpcServer::start(
        socket.path(),
        Arc::new(QueuedClient::new(vec![Ok(metadata)])),
        Arc::new(AllowPeer),
        test_server_config(),
    )
    .expect("server should start");

    let error = IpcClient::new(socket.path())
        .execute(ApplicationOperation::GetStatus)
        .expect_err("operation and output shape should be validated");
    assert_eq!(error.code, ErrorCode::ProtocolMismatch);
    assert_eq!(
        error.message,
        "The IPC response output does not match the request"
    );

    server.shutdown().expect("server should stop cleanly");
}

#[test]
fn structured_application_errors_preserve_selector_candidates() {
    let socket = TempSocket::new("typed-error");
    let source_error = ApplicationError::new(
        ErrorCode::ProfileAmbiguous,
        "The Profile selector is ambiguous",
        false,
    )
    .with_selector_candidates(
        SelectorKind::Profile,
        vec![
            SelectorCandidate::new("profile-1", "Work"),
            SelectorCandidate::new("profile-2", "Work"),
        ],
    );
    let mut server = IpcServer::start(
        socket.path(),
        Arc::new(QueuedClient::new(vec![Err(source_error.clone())])),
        Arc::new(AllowPeer),
        test_server_config(),
    )
    .expect("server should start");

    let error = IpcClient::new(socket.path())
        .execute(ApplicationOperation::ProfileList)
        .expect_err("application error should cross the transport");
    assert_eq!(error, source_error);

    server.shutdown().expect("server should stop cleanly");
}

#[test]
fn profile_field_unsupported_error_round_trips_over_ipc() {
    let socket = TempSocket::new("unsupported-profile-field");
    let source_error = ApplicationError::new(
        ErrorCode::ProfileFieldUnsupported,
        "The Profile contains a field unsupported by the bundled Mihomo version",
        false,
    );
    let mut server = IpcServer::start(
        socket.path(),
        Arc::new(QueuedClient::new(vec![Err(source_error.clone())])),
        Arc::new(AllowPeer),
        test_server_config(),
    )
    .expect("server should start");

    let error = IpcClient::new(socket.path())
        .execute(ApplicationOperation::ProfileList)
        .expect_err("the stable Profile error should cross the transport");

    assert_eq!(error, source_error);
    server.shutdown().expect("server should stop cleanly");
}

#[test]
fn runtime_apply_failure_details_round_trip_over_ipc() {
    let socket = TempSocket::new("runtime-apply-error");
    let source_error = ApplicationError::new(
        ErrorCode::ExternalOperationFailed,
        "Runtime Apply failed and the committed configuration was retained",
        false,
    )
    .with_details(ApplicationErrorDetails::RuntimeApplyFailure(Box::new(
        hopash::application::RuntimeApplyFailureDetails {
            candidate_generation: Some(RuntimeGeneration(9)),
            committed_generation: Some(RuntimeGeneration(8)),
            stage: RuntimeApplyFailureStage::Health,
            recovery: RecoveryOutcome {
                status: RecoveryStatus::Failed,
                restored_generation: Some(RuntimeGeneration(8)),
                message: Some("Committed state recovery failed".to_owned()),
            },
        },
    )));
    let mut server = IpcServer::start(
        socket.path(),
        Arc::new(QueuedClient::new(vec![Err(source_error.clone())])),
        Arc::new(AllowPeer),
        test_server_config(),
    )
    .expect("server should start");

    let error = IpcClient::new(socket.path())
        .execute(ApplicationOperation::RuleList)
        .expect_err("Runtime Apply details should cross the transport");
    assert_eq!(error, source_error);

    server.shutdown().expect("server should stop cleanly");
}

fn application_output_cases() -> Vec<(ApplicationOperation, ApplicationOutput)> {
    let status = ApplicationService::new().status();
    let profile = ProfileSummary {
        id: ProfileId::new(),
        name: "Work".to_owned(),
        subscription_url: SubscriptionUrl::parse("https://example.com/profile.yaml")
            .expect("fixture URL should be valid"),
        active: true,
        refresh_state: ProfileRefreshState::Fresh,
        last_success_at_unix_ms: 10,
        next_refresh_at_unix_ms: 20,
        last_error: None,
    };
    let node_id = NodeRecordId::for_provider("provider", "Node A");
    let latency = LatencySummary {
        node_id: node_id.clone(),
        node_name: "Node A".to_owned(),
        delay_ms: Some(42),
        sampled_at_unix_ms: Some(30),
        freshness: LatencyFreshness::Fresh,
        probe_status: LatencyProbeStatus::Succeeded,
        probe_generation: ProbeGeneration(4),
    };
    let recovery = RecoveryOutcome {
        status: RecoveryStatus::Succeeded,
        restored_generation: Some(RuntimeGeneration(2)),
        message: Some("Recovered the previous runtime".to_owned()),
    };
    let runtime_apply = RuntimeApplyOutcome {
        status: RuntimeApplyStatus::Applied,
        candidate_generation: Some(RuntimeGeneration(3)),
        committed_generation: Some(RuntimeGeneration(3)),
        recovery: recovery.clone(),
    };
    let identity = SelectorIdentity {
        id: node_id.as_str().to_owned(),
        name: "Node A".to_owned(),
    };

    vec![
        (
            ApplicationOperation::Start,
            ApplicationOutput::Lifecycle(LifecycleOutcome {
                action: LifecycleAction::Start,
                changed: true,
                status,
            }),
        ),
        (
            ApplicationOperation::ProfileList,
            ApplicationOutput::Profiles(ProfileListOutcome {
                profiles: vec![profile.clone()],
            }),
        ),
        (
            ApplicationOperation::ProfileUse {
                profile: profile.id.to_string(),
            },
            ApplicationOutput::ProfileMutation(ProfileMutationOutcome {
                action: ProfileMutationAction::Activated,
                profile,
                runtime_apply: Some(runtime_apply.clone()),
            }),
        ),
        (
            ApplicationOperation::ProxyList {
                group: "Main".to_owned(),
            },
            ApplicationOutput::Proxies(ProxyListOutcome {
                group: ProxyGroupSummary {
                    name: "Main".to_owned(),
                    proxy_type: "Selector".to_owned(),
                    selectable: true,
                    selected_node: Some(identity.clone()),
                },
                groups: vec![ProxyGroupSummary {
                    name: "Main".to_owned(),
                    proxy_type: "Selector".to_owned(),
                    selectable: true,
                    selected_node: Some(identity.clone()),
                }],
                nodes: vec![ProxyNodeRow {
                    id: Some(node_id.clone()),
                    name: "Node A".to_owned(),
                    member_kind: ProxyMemberKind::Node,
                    source: Some(ProxyNodeSource::Provider {
                        provider_name: "provider".to_owned(),
                    }),
                    candidate_ids: vec![node_id.clone()],
                    proxy_type: Some("Shadowsocks".to_owned()),
                    availability: ProxyAvailability::Available,
                    selected: true,
                    delay_ms: Some(42),
                    sampled_at_unix_ms: Some(30),
                    freshness: LatencyFreshness::Fresh,
                    probe_status: LatencyProbeStatus::Succeeded,
                }],
            }),
        ),
        (
            ApplicationOperation::ProxySelect {
                group: "Main".to_owned(),
                node: identity.id.clone(),
            },
            ApplicationOutput::ProxySelection(ProxySelectionOutcome {
                group: "Main".to_owned(),
                previous_node: None,
                selected_node: identity,
                persisted: true,
                recovery,
            }),
        ),
        (
            ApplicationOperation::LatencyList,
            ApplicationOutput::Latencies(LatencyListOutcome {
                samples: vec![latency.clone()],
            }),
        ),
        (
            ApplicationOperation::LatencyShow {
                node: node_id.as_str().to_owned(),
            },
            ApplicationOutput::Latency(LatencyShowOutcome { sample: latency }),
        ),
        (
            ApplicationOperation::RuleList,
            ApplicationOutput::Rules(RuleListOutcome {
                initialized: true,
                revision: Some(LocalRuleSetRevision(8)),
                rules: vec![RuleSummary {
                    index: 0,
                    rule_string: "MATCH,DIRECT".to_owned(),
                    rule_type: "MATCH".to_owned(),
                    payload: None,
                    policy_target: "DIRECT".to_owned(),
                    params: Vec::new(),
                    policy_target_validation: PolicyTargetValidation::Valid,
                }],
            }),
        ),
        (
            ApplicationOperation::RuleRemove {
                rule: "MATCH,DIRECT".to_owned(),
            },
            ApplicationOutput::RuleMutation(RuleMutationOutcome {
                action: RuleMutationAction::Removed,
                changed_rule: "MATCH,DIRECT".to_owned(),
                previous_rule: Some("MATCH,DIRECT".to_owned()),
                resulting_position: None,
                runtime_apply,
            }),
        ),
    ]
}

#[test]
fn bounded_worker_pool_serves_concurrent_clients() {
    let socket = TempSocket::new("concurrent");
    let application = Arc::new(CountingClient::new());
    let mut server = IpcServer::start(
        socket.path(),
        Arc::clone(&application),
        Arc::new(AllowPeer),
        IpcServerConfig {
            pending_connection_capacity: 32,
            ..test_server_config()
        },
    )
    .expect("server should start");
    let client = Arc::new(IpcClient::new(socket.path()));

    let clients = (0..24)
        .map(|_| {
            let client = Arc::clone(&client);
            thread::spawn(move || client.execute(ApplicationOperation::GetStatus))
        })
        .collect::<Vec<_>>();
    for client in clients {
        assert!(
            matches!(
                client.join().expect("fixture client should finish"),
                Ok(ApplicationOutput::Status(_))
            ),
            "every accepted client should receive its correlated response"
        );
    }
    assert_eq!(application.calls.load(Ordering::Relaxed), 24);

    server.shutdown().expect("server should stop cleanly");
}

#[test]
fn malformed_and_oversized_frames_receive_bounded_protocol_failures() {
    let socket = TempSocket::new("invalid-frames");
    let application = Arc::new(CountingClient::new());
    let mut server = IpcServer::start(
        socket.path(),
        Arc::clone(&application),
        Arc::new(AllowPeer),
        test_server_config(),
    )
    .expect("server should start");

    let mut malformed = UnixStream::connect(socket.path()).expect("fixture should connect");
    malformed
        .set_read_timeout(Some(Duration::from_secs(1)))
        .expect("fixture timeout should configure");
    malformed
        .write_all(&1_u32.to_be_bytes())
        .expect("malformed length should write");
    malformed
        .write_all(b"{")
        .expect("malformed payload should write");
    let malformed_response: IpcResponse =
        read_frame(&mut malformed).expect("server should reject malformed JSON");
    assert_eq!(malformed_response.request_id, RequestId(0));
    assert_eq!(
        malformed_response
            .error()
            .expect("failure should be present")
            .code,
        "protocol_mismatch"
    );

    let mut oversized = UnixStream::connect(socket.path()).expect("fixture should connect");
    oversized
        .set_read_timeout(Some(Duration::from_secs(1)))
        .expect("fixture timeout should configure");
    let length = u32::try_from(IPC_FRAME_MAX_BYTES + 1).expect("frame limit should fit u32");
    oversized
        .write_all(&length.to_be_bytes())
        .expect("oversized header should write");
    let oversized_response: IpcResponse =
        read_frame(&mut oversized).expect("server should reject oversized frame");
    assert_eq!(oversized_response.request_id, RequestId(0));
    assert_eq!(
        oversized_response
            .error()
            .expect("failure should be present")
            .code,
        "protocol_mismatch"
    );
    assert_eq!(application.calls.load(Ordering::Relaxed), 0);

    server.shutdown().expect("server should stop cleanly");
}

#[test]
fn unauthorized_peers_are_closed_before_application_dispatch() {
    let socket = TempSocket::new("unauthorized");
    let application = Arc::new(CountingClient::new());
    let mut server = IpcServer::start(
        socket.path(),
        Arc::clone(&application),
        Arc::new(DenyPeer),
        test_server_config(),
    )
    .expect("server should start");

    let mut stream = UnixStream::connect(socket.path()).expect("fixture should connect");
    stream
        .set_read_timeout(Some(Duration::from_secs(1)))
        .expect("fixture timeout should configure");
    let request = IpcRequest::new(RequestId(1), RequestOperation::GetStatus(EmptyPayload {}));
    write_frame(&mut stream, &request).expect("fixture request should write");
    let mut byte = [0_u8; 1];
    assert_eq!(
        stream.read(&mut byte).expect("rejected peer should close"),
        0
    );
    assert_eq!(application.calls.load(Ordering::Relaxed), 0);

    server.shutdown().expect("server should stop cleanly");
}

#[test]
fn client_read_deadline_returns_a_retryable_safe_error() {
    let socket = TempSocket::new("deadline-token-secret");
    let listener = bind_private_listener(socket.path()).expect("fixture listener should bind");
    let fixture = thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("fixture should accept");
        let _: IpcRequest = read_frame(&mut stream).expect("fixture should read request");
        thread::sleep(Duration::from_millis(200));
    });
    let client = IpcClient::with_timeouts(
        socket.path(),
        Duration::from_millis(100),
        Duration::from_millis(40),
    );
    let started = Instant::now();

    let error = client
        .execute(ApplicationOperation::GetStatus)
        .expect_err("silent server should reach the read deadline");
    assert_eq!(error.code, ErrorCode::SupervisorUnavailable);
    assert!(error.retryable);
    assert!(started.elapsed() < Duration::from_millis(180));
    assert!(!error.message.contains("token-secret"));
    assert!(!format!("{client:?}").contains("token-secret"));

    fixture.join().expect("fixture server should stop");
}

#[test]
fn client_write_deadline_bounds_a_server_that_does_not_read() {
    let socket = TempSocket::new("write-deadline");
    let listener = bind_private_listener(socket.path()).expect("fixture listener should bind");
    let fixture = thread::spawn(move || {
        let (_stream, _) = listener.accept().expect("fixture should accept");
        thread::sleep(Duration::from_millis(200));
    });
    let client = IpcClient::with_timeouts(
        socket.path(),
        Duration::from_millis(100),
        Duration::from_millis(40),
    );
    let operation = ApplicationOperation::RuleAdd {
        rule: "x".repeat(2 * 1024 * 1024),
        placement: RulePlacement::Append,
    };
    let started = Instant::now();

    let error = client
        .execute(operation)
        .expect_err("non-reading server should reach the write deadline");
    assert_eq!(error.code, ErrorCode::SupervisorUnavailable);
    assert!(error.retryable);
    assert_eq!(
        error.message,
        "Timed out sending the Supervisor IPC request"
    );
    assert!(started.elapsed() < Duration::from_millis(180));

    fixture.join().expect("fixture server should stop");
}

#[test]
fn client_read_deadline_is_absolute_across_trickled_bytes() {
    let socket = TempSocket::new("absolute-read-deadline");
    let listener = bind_private_listener(socket.path()).expect("fixture listener should bind");
    let fixture = thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("fixture should accept");
        let _: IpcRequest = read_frame(&mut stream).expect("fixture should read request");
        let payload = serde_json::to_vec(&serde_json::json!({
            "protocol_version": IPC_PROTOCOL_VERSION,
            "request_id": 1,
            "data": {}
        }))
        .expect("fixture response should encode");
        let mut frame = u32::try_from(payload.len())
            .expect("fixture response should fit")
            .to_be_bytes()
            .to_vec();
        frame.extend_from_slice(&payload);
        for byte in frame {
            if stream.write_all(&[byte]).is_err() {
                break;
            }
            thread::sleep(Duration::from_millis(15));
        }
    });
    let client = IpcClient::with_timeouts(
        socket.path(),
        Duration::from_millis(100),
        Duration::from_millis(35),
    );
    let started = Instant::now();

    let error = client
        .execute(ApplicationOperation::GetStatus)
        .expect_err("trickled response bytes should reach the absolute deadline");
    assert_eq!(error.code, ErrorCode::SupervisorUnavailable);
    assert!(error.retryable);
    assert!(started.elapsed() < Duration::from_millis(120));

    fixture.join().expect("fixture server should stop");
}

#[test]
fn server_request_deadline_is_absolute_across_trickled_bytes() {
    let socket = TempSocket::new("absolute-server-read-deadline");
    let application = Arc::new(CountingClient::new());
    let mut server = IpcServer::start(
        socket.path(),
        Arc::clone(&application),
        Arc::new(AllowPeer),
        IpcServerConfig {
            io_timeout: Duration::from_millis(35),
            worker_count: 1,
            pending_connection_capacity: 1,
        },
    )
    .expect("server should start");
    let request = IpcRequest::new(RequestId(5), RequestOperation::GetStatus(EmptyPayload {}));
    let payload = serde_json::to_vec(&request).expect("fixture request should encode");
    let mut frame = u32::try_from(payload.len())
        .expect("fixture request should fit")
        .to_be_bytes()
        .to_vec();
    frame.extend_from_slice(&payload);
    let mut stream = UnixStream::connect(socket.path()).expect("fixture should connect");
    stream
        .set_read_timeout(Some(Duration::from_millis(200)))
        .expect("fixture read timeout should configure");
    for byte in frame {
        if stream.write_all(&[byte]).is_err() {
            break;
        }
        thread::sleep(Duration::from_millis(15));
    }

    let response: IpcResponse = read_frame(&mut stream).expect("deadline failure should be framed");
    assert_eq!(response.request_id, RequestId(0));
    assert_eq!(
        response.error().expect("failure should be present").code,
        "protocol_mismatch"
    );
    assert_eq!(application.calls.load(Ordering::Relaxed), 0);

    server.shutdown().expect("server should stop cleanly");
}

#[test]
fn response_correlation_and_protocol_version_are_validated_before_data() {
    for (label, response_version, response_id) in [
        ("correlation", IPC_PROTOCOL_VERSION, RequestId(99)),
        ("version", IPC_PROTOCOL_VERSION + 1, RequestId(1)),
    ] {
        let socket = TempSocket::new(label);
        let listener = bind_private_listener(socket.path()).expect("fixture listener should bind");
        let fixture = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("fixture should accept");
            let request: IpcRequest = read_frame(&mut stream).expect("fixture should read request");
            let response = serde_json::json!({
                "protocol_version": response_version,
                "request_id": if label == "correlation" { response_id.0 } else { request.request_id.0 },
                "data": {},
            });
            write_frame(&mut stream, &response).expect("fixture response should write");
        });

        let error = IpcClient::new(socket.path())
            .execute(ApplicationOperation::GetStatus)
            .expect_err("invalid response identity should fail");
        assert_eq!(error.code, ErrorCode::ProtocolMismatch);
        assert_eq!(error.message, "The IPC response did not match the request");
        fixture.join().expect("fixture server should stop");
    }
}

#[test]
fn unknown_error_codes_are_rejected_without_echoing_the_remote_message() {
    let socket = TempSocket::new("unknown-error");
    let listener = bind_private_listener(socket.path()).expect("fixture listener should bind");
    let fixture = thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("fixture should accept");
        let request: IpcRequest = read_frame(&mut stream).expect("fixture should read request");
        let response = serde_json::json!({
            "protocol_version": IPC_PROTOCOL_VERSION,
            "request_id": request.request_id.0,
            "error": {
                "code": "future_error",
                "message": "subscription-secret-value",
                "retryable": false,
            },
        });
        write_frame(&mut stream, &response).expect("fixture response should write");
    });

    let error = IpcClient::new(socket.path())
        .execute(ApplicationOperation::GetStatus)
        .expect_err("unknown error code should fail protocol validation");
    assert_eq!(error.code, ErrorCode::ProtocolMismatch);
    assert_eq!(error.message, "The IPC response error code is unknown");
    assert!(!format!("{error:?}").contains("subscription-secret-value"));

    fixture.join().expect("fixture server should stop");
}

#[test]
fn request_protocol_and_streaming_operations_return_correlated_errors() {
    let socket = TempSocket::new("request-validation");
    let application = Arc::new(CountingClient::new());
    let mut server = IpcServer::start(
        socket.path(),
        Arc::clone(&application),
        Arc::new(AllowPeer),
        test_server_config(),
    )
    .expect("server should start");

    let mut incompatible =
        IpcRequest::new(RequestId(7), RequestOperation::GetStatus(EmptyPayload {}));
    incompatible.protocol_version += 1;
    let mut stream = raw_request(socket.path(), &incompatible);
    let response: IpcResponse = read_frame(&mut stream).expect("failure should be framed");
    assert_eq!(response.request_id, RequestId(7));
    assert_eq!(
        response.error().expect("failure should be present").code,
        "protocol_mismatch"
    );

    let streaming = IpcRequest::new(
        RequestId(8),
        RequestOperation::FollowLogs(LogSubscriptionPayload {
            after_sequence: Some(4),
        }),
    );
    let mut stream = raw_request(socket.path(), &streaming);
    let response: IpcResponse = read_frame(&mut stream).expect("failure should be framed");
    assert_eq!(response.request_id, RequestId(8));
    assert_eq!(
        response.error().expect("failure should be present").code,
        "operation_unavailable"
    );
    assert_eq!(application.calls.load(Ordering::Relaxed), 0);

    server.shutdown().expect("server should stop cleanly");
}

#[test]
fn pending_connection_queue_rejects_excess_work_without_growing() {
    let socket = TempSocket::new("bounded-pending");
    let application = Arc::new(BlockingClient::new());
    let mut server = IpcServer::start(
        socket.path(),
        Arc::clone(&application),
        Arc::new(AllowPeer),
        IpcServerConfig {
            io_timeout: Duration::from_secs(1),
            worker_count: 1,
            pending_connection_capacity: 1,
        },
    )
    .expect("server should start");

    let client_path = socket.path().to_path_buf();
    let first =
        thread::spawn(move || IpcClient::new(client_path).execute(ApplicationOperation::GetStatus));
    application.wait_until_entered(1);

    let second_request =
        IpcRequest::new(RequestId(2), RequestOperation::GetStatus(EmptyPayload {}));
    let mut second = raw_request(socket.path(), &second_request);
    second
        .set_read_timeout(Some(Duration::from_millis(300)))
        .expect("fixture timeout should configure");
    let third_request = IpcRequest::new(RequestId(3), RequestOperation::GetStatus(EmptyPayload {}));
    let mut third = raw_request(socket.path(), &third_request);
    third
        .set_read_timeout(Some(Duration::from_millis(300)))
        .expect("fixture timeout should configure");
    let second_closed = connection_closed_or_pending(&mut second);
    let third_closed = connection_closed_or_pending(&mut third);

    application.release();
    let second_closed = second_closed.expect("second admission state should be observable");
    let third_closed = third_closed.expect("third admission state should be observable");
    assert_ne!(
        second_closed, third_closed,
        "exactly one request should queue"
    );
    assert!(matches!(
        first.join().expect("first client should finish"),
        Ok(ApplicationOutput::Status(_))
    ));
    let (queued, expected_request_id) = if second_closed {
        (&mut third, RequestId(3))
    } else {
        (&mut second, RequestId(2))
    };
    let response: IpcResponse = read_frame(queued).expect("queued connection should complete");
    assert_eq!(response.request_id, expected_request_id);
    assert!(response.data().is_some());
    assert_eq!(application.entered.load(Ordering::Acquire), 2);

    server.shutdown().expect("server should stop cleanly");
}

fn connection_closed_or_pending(stream: &mut UnixStream) -> io::Result<bool> {
    let mut byte = [0_u8; 1];
    match stream.read(&mut byte) {
        Ok(0) => Ok(true),
        Err(error)
            if matches!(
                error.kind(),
                io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
            ) =>
        {
            Ok(false)
        }
        Ok(_) => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "a blocked fixture request received an early response",
        )),
        Err(error) => Err(error),
    }
}

#[test]
fn shutdown_stops_acceptance_and_removes_only_its_own_socket_identity() {
    let socket = TempSocket::new("shutdown");
    let mut server = IpcServer::start(
        socket.path(),
        Arc::new(ApplicationService::new()),
        Arc::new(AllowPeer),
        test_server_config(),
    )
    .expect("server should start");
    assert!(socket.path().exists());

    server.shutdown().expect("server should stop cleanly");
    assert!(!socket.path().exists());
    server
        .shutdown()
        .expect("repeated shutdown should be idempotent");

    let mut replacement_owner = IpcServer::start(
        socket.path(),
        Arc::new(ApplicationService::new()),
        Arc::new(AllowPeer),
        test_server_config(),
    )
    .expect("replacement owner should start");
    fs::remove_file(socket.path()).expect("fixture should unlink original endpoint");
    let replacement_listener =
        bind_private_listener(socket.path()).expect("fixture replacement should bind");

    replacement_owner
        .shutdown()
        .expect("original owner should stop cleanly");
    assert!(
        socket.path().exists(),
        "identity-aware cleanup should preserve a replacement endpoint"
    );
    drop(replacement_listener);
}

#[test]
fn server_shutdown_is_bounded_while_an_incomplete_client_is_connected() {
    let socket = TempSocket::new("shutdown-deadline");
    let mut server = IpcServer::start(
        socket.path(),
        Arc::new(ApplicationService::new()),
        Arc::new(AllowPeer),
        IpcServerConfig {
            io_timeout: Duration::from_millis(80),
            worker_count: 1,
            pending_connection_capacity: 1,
        },
    )
    .expect("server should start");
    let _incomplete = UnixStream::connect(socket.path()).expect("fixture should connect");
    thread::sleep(Duration::from_millis(15));

    let started = Instant::now();
    server
        .shutdown()
        .expect("server should stop after I/O deadline");
    assert!(started.elapsed() < Duration::from_millis(250));
    assert!(!socket.path().exists());
}

#[test]
fn client_diagnostics_never_echo_subscription_credentials_or_socket_paths() {
    let socket = TempSocket::new("secret-token-path");
    let listener = bind_private_listener(socket.path()).expect("fixture listener should bind");
    let request_seen = Arc::new(AtomicBool::new(false));
    let fixture_seen = Arc::clone(&request_seen);
    let fixture = thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("fixture should accept");
        let _request: IpcRequest = read_frame(&mut stream).expect("fixture should read request");
        fixture_seen.store(true, Ordering::Release);
        write_frame(&mut stream, &serde_json::json!({ "unexpected": true }))
            .expect("fixture response should write");
    });
    let client = IpcClient::new(socket.path());
    let secret = "subscription-secret-value";
    let operation = ApplicationOperation::ProfileAdd {
        subscription_url: SubscriptionUrl::parse(&format!(
            "https://user:password@example.com/profile.yaml?token={secret}"
        ))
        .expect("fixture URL should be valid"),
    };

    let error = client
        .execute(operation)
        .expect_err("malformed response should fail safely");
    let diagnostics = format!("{client:?} {error:?} {}", error.message);
    assert!(request_seen.load(Ordering::Acquire));
    assert!(!diagnostics.contains(secret));
    assert!(!diagnostics.contains("user:password"));
    assert!(!diagnostics.contains("secret-token-path"));

    fixture.join().expect("fixture server should stop");
}
