use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use hopash::application::{
    ApplicationClient, ApplicationOperation, ApplicationOutput, ApplicationService,
};
use hopash::error::ErrorCode;
use hopash::ipc::{
    IpcRequest, IpcStreamFrame, IpcStreamPayload, LogStreamItem, RequestId, bind_private_listener,
    read_frame,
};
use hopash::ipc_runtime::{
    IpcClient, IpcServer, IpcServerConfig, IpcStreamBroker, SameUserPeerAuthorizer,
    StatusStreamUpdate,
};
use hopash::telemetry::{CoreLogRecord, LogLevel, LogSource};

static NEXT_TEMP_ID: AtomicU64 = AtomicU64::new(1);

struct TempSocket {
    directory: PathBuf,
    path: PathBuf,
}

impl TempSocket {
    fn new(label: &str) -> Self {
        let id = NEXT_TEMP_ID.fetch_add(1, Ordering::Relaxed);
        let directory = PathBuf::from("/tmp").join(format!(
            "hopash-ipc-stream-{label}-{}-{id}",
            std::process::id()
        ));
        let path = directory.join("supervisor.sock");
        Self { directory, path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempSocket {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
        let _ = fs::remove_dir(&self.directory);
    }
}

fn server_config() -> IpcServerConfig {
    IpcServerConfig {
        io_timeout: Duration::from_secs(2),
        worker_count: 4,
        pending_connection_capacity: 16,
    }
}

fn broker(log_capacity: usize) -> Arc<IpcStreamBroker> {
    Arc::new(
        IpcStreamBroker::with_log_capacity(
            10,
            100,
            ApplicationService::new().status(),
            log_capacity,
        )
        .expect("fixture stream broker should be valid"),
    )
}

fn start_server(socket: &TempSocket, streams: Arc<IpcStreamBroker>) -> IpcServer {
    IpcServer::start_with_streams(
        socket.path(),
        Arc::new(ApplicationService::new()),
        Arc::new(SameUserPeerAuthorizer::current()),
        streams,
        server_config(),
    )
    .expect("fixture IPC server should start")
}

fn log_record(sequence: u64, message: &str) -> CoreLogRecord {
    CoreLogRecord::new(
        sequence,
        sequence * 10,
        LogLevel::Info,
        LogSource::CoreApi,
        message,
    )
}

#[test]
fn status_stream_reconstructs_deltas_and_tags_the_connection_generation() {
    let socket = TempSocket::new("status-round-trip");
    let streams = broker(8);
    let mut server = start_server(&socket, Arc::clone(&streams));
    let client = IpcClient::with_timeouts(
        socket.path(),
        Duration::from_secs(1),
        Duration::from_secs(2),
    );
    let mut status = client
        .subscribe_status(None, 7)
        .expect("status stream should connect");

    let initial = status
        .next_item()
        .expect("initial frame should be valid")
        .expect("initial frame should exist");
    assert_eq!(initial.connection_generation, 7);
    assert!(matches!(
        initial.item,
        StatusStreamUpdate::Snapshot { sequence: 10, .. }
    ));

    let mut changed = ApplicationService::new().status();
    changed.connection_count = 42;
    streams
        .publish_status(11, 110, changed)
        .expect("contiguous status should publish");

    let delta = status
        .next_item()
        .expect("delta frame should be valid")
        .expect("delta frame should exist");
    let StatusStreamUpdate::Delta {
        sequence,
        timestamp_unix_ms,
        patch,
        snapshot,
    } = delta.item
    else {
        panic!("second status item should be a delta");
    };
    assert_eq!(sequence, 11);
    assert_eq!(timestamp_unix_ms, 110);
    assert_eq!(patch["connection_count"], 42);
    assert_eq!(snapshot.connection_count, 42);
    assert_eq!(status.resume_after_sequence(), Some(11));

    drop(status);
    server.shutdown().expect("server should stop cleanly");
}

#[test]
fn reconnect_starts_with_the_latest_complete_status_snapshot() {
    let socket = TempSocket::new("status-reconnect");
    let streams = broker(8);
    let mut changed = ApplicationService::new().status();
    changed.connection_count = 9;
    streams
        .publish_status(11, 110, changed)
        .expect("status should publish");
    let mut server = start_server(&socket, Arc::clone(&streams));
    let client = IpcClient::new(socket.path());

    let mut status = client
        .subscribe_status(Some(10), 8)
        .expect("reconnected stream should open");
    let item = status
        .next_item()
        .expect("snapshot should be valid")
        .expect("snapshot should exist");
    let StatusStreamUpdate::Snapshot {
        sequence, snapshot, ..
    } = item.item
    else {
        panic!("reconnected status should start with a snapshot");
    };
    assert_eq!(item.connection_generation, 8);
    assert_eq!(sequence, 11);
    assert_eq!(snapshot.connection_count, 9);

    drop(status);
    server.shutdown().expect("server should stop cleanly");
}

#[test]
fn status_queue_overflow_delivers_one_terminal_resync_marker() {
    let socket = TempSocket::new("status-overflow");
    let streams = broker(8);
    let mut server = start_server(&socket, Arc::clone(&streams));
    let client = IpcClient::with_timeouts(
        socket.path(),
        Duration::from_secs(1),
        Duration::from_secs(3),
    );
    let mut status = client
        .subscribe_status(None, 3)
        .expect("status stream should connect");
    assert!(matches!(
        status
            .next_item()
            .expect("snapshot should be valid")
            .expect("snapshot should exist")
            .item,
        StatusStreamUpdate::Snapshot { sequence: 10, .. }
    ));

    let large_value = "x".repeat(256 * 1024);
    for sequence in 11..=90 {
        let mut snapshot = ApplicationService::new().status();
        snapshot.primary_proxy_group = Some(format!("{sequence}-{large_value}"));
        streams
            .publish_status(sequence, sequence * 10, snapshot)
            .expect("bounded status should publish");
    }

    let mut resync_count = 0;
    for _ in 0..16 {
        let Some(item) = status
            .next_item()
            .expect("queued status frame should be valid")
        else {
            break;
        };
        if matches!(item.item, StatusStreamUpdate::ResyncRequired { .. }) {
            resync_count += 1;
        }
    }
    assert_eq!(resync_count, 1);
    assert!(
        status
            .next_item()
            .expect("terminal stream should remain valid")
            .is_none()
    );

    drop(status);
    server.shutdown().expect("server should stop cleanly");
}

#[test]
fn log_gap_recovers_through_tail_then_live_following_resumes() {
    let socket = TempSocket::new("log-recovery");
    let streams = broker(3);
    for sequence in 1..=5 {
        streams
            .publish_log(log_record(sequence, &format!("log-{sequence}")))
            .expect("fixture log should publish");
    }
    let mut server = start_server(&socket, Arc::clone(&streams));
    let client = IpcClient::new(socket.path());

    let mut stale = client
        .follow_logs(Some(0), 4)
        .expect("log stream should connect");
    let gap = stale
        .next_item()
        .expect("gap frame should be valid")
        .expect("gap frame should exist");
    assert_eq!(gap.connection_generation, 4);
    assert_eq!(
        gap.item,
        LogStreamItem::Gap {
            after_sequence: Some(0),
            latest_sequence: 5,
        }
    );
    assert!(
        stale
            .next_item()
            .expect("gap should terminate the stream")
            .is_none()
    );

    let tail = client
        .log_tail(Some(0))
        .expect("retained log tail should load");
    assert!(tail.gap);
    assert_eq!(tail.dropped_total, 2);
    assert_eq!(
        tail.records
            .iter()
            .map(|record| record.sequence)
            .collect::<Vec<_>>(),
        [3, 4, 5]
    );

    let mut resumed = client
        .follow_logs(tail.latest_sequence, 5)
        .expect("recovered log stream should connect");
    streams
        .publish_log(log_record(6, "log-6"))
        .expect("next log should publish");
    let live = resumed
        .next_item()
        .expect("live frame should be valid")
        .expect("live frame should exist");
    assert_eq!(live.connection_generation, 5);
    assert!(matches!(
        live.item,
        LogStreamItem::Record { record } if record.sequence == 6
    ));

    drop(stale);
    drop(resumed);
    server.shutdown().expect("server should stop cleanly");
}

#[test]
fn cancellation_interrupts_an_idle_log_reader() {
    let socket = TempSocket::new("cancel");
    let streams = broker(8);
    let mut server = start_server(&socket, streams);
    let client = IpcClient::with_timeouts(
        socket.path(),
        Duration::from_secs(1),
        Duration::from_secs(2),
    );
    let mut logs = client
        .follow_logs(None, 6)
        .expect("log stream should connect");
    let cancellation = logs.cancellation();
    let canceller = thread::spawn(move || {
        thread::sleep(Duration::from_millis(20));
        cancellation.cancel();
    });

    let started = Instant::now();
    assert!(
        logs.next_item()
            .expect("cancelled stream should close cleanly")
            .is_none()
    );
    assert!(started.elapsed() < Duration::from_millis(500));

    canceller.join().expect("canceller should finish");
    drop(logs);
    server.shutdown().expect("server should stop cleanly");
}

#[test]
fn stream_client_rejects_a_mismatched_request_id() {
    let socket = TempSocket::new("correlation");
    let listener = bind_private_listener(socket.path()).expect("fixture listener should bind");
    let fixture = thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("fixture should accept");
        let request: IpcRequest = read_frame(&mut stream).expect("fixture should read request");
        let frame = IpcStreamFrame::new(
            RequestId(request.request_id.0 + 1),
            IpcStreamPayload::Heartbeat,
        );
        hopash::ipc::write_frame(&mut stream, &frame).expect("fixture frame should write");
    });
    let client = IpcClient::new(socket.path());
    let mut status = client
        .subscribe_status(None, 1)
        .expect("fixture stream should connect");

    let error = status
        .next_item()
        .expect_err("mismatched request ID should fail");
    assert_eq!(error.code, ErrorCode::ProtocolMismatch);
    assert_eq!(
        error.message,
        "The IPC stream frame did not match the request"
    );

    fixture.join().expect("fixture should stop");
}

#[test]
fn stream_read_deadline_is_absolute_across_trickled_bytes() {
    let socket = TempSocket::new("absolute-deadline");
    let listener = bind_private_listener(socket.path()).expect("fixture listener should bind");
    let fixture = thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("fixture should accept");
        let request: IpcRequest = read_frame(&mut stream).expect("fixture should read request");
        let frame = IpcStreamFrame::new(request.request_id, IpcStreamPayload::Heartbeat);
        let payload = serde_json::to_vec(&frame).expect("fixture frame should encode");
        let mut bytes = Vec::with_capacity(payload.len() + 4);
        bytes.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        bytes.extend_from_slice(&payload);
        for byte in bytes {
            if stream.write_all(&[byte]).is_err() {
                break;
            }
            thread::sleep(Duration::from_millis(15));
        }
    });
    let client = IpcClient::with_timeouts(
        socket.path(),
        Duration::from_secs(1),
        Duration::from_millis(40),
    );
    let mut logs = client
        .follow_logs(None, 1)
        .expect("fixture stream should connect");

    let started = Instant::now();
    let error = logs
        .next_item()
        .expect_err("trickled frame should exceed one absolute deadline");
    assert_eq!(error.code, ErrorCode::SupervisorUnavailable);
    assert!(started.elapsed() < Duration::from_millis(150));

    fixture.join().expect("fixture should stop");
}

#[test]
fn server_shutdown_wakes_idle_stream_workers() {
    let socket = TempSocket::new("shutdown");
    let streams = broker(8);
    let mut server = start_server(&socket, streams);
    let client = IpcClient::new(socket.path());
    let logs = client
        .follow_logs(None, 1)
        .expect("idle log stream should connect");

    let started = Instant::now();
    server.shutdown().expect("server should stop cleanly");
    assert!(started.elapsed() < Duration::from_millis(500));

    drop(logs);
}

#[test]
fn bounded_stream_slots_preserve_one_shot_request_capacity() {
    let socket = TempSocket::new("reserved-one-shot");
    let streams = broker(8);
    let mut server = start_server(&socket, streams);
    let client = IpcClient::new(socket.path());
    let mut held = Vec::new();

    for generation in 1..=3 {
        let mut stream = client
            .subscribe_status(None, generation)
            .expect("reserved status stream should connect");
        assert!(matches!(
            stream
                .next_item()
                .expect("snapshot should be valid")
                .expect("snapshot should exist")
                .item,
            StatusStreamUpdate::Snapshot { .. }
        ));
        held.push(stream);
    }

    let mut excess = client
        .subscribe_status(None, 4)
        .expect("excess stream request should reach the server");
    let error = excess
        .next_item()
        .expect_err("excess stream should receive a bounded capacity error");
    assert_eq!(error.code, ErrorCode::OperationUnavailable);
    assert!(error.retryable);

    let output = client
        .execute(ApplicationOperation::GetStatus)
        .expect("reserved worker should serve a one-shot request");
    assert!(matches!(output, ApplicationOutput::Status(_)));

    drop(excess);
    drop(held);
    server.shutdown().expect("server should stop cleanly");
}

#[test]
fn single_worker_server_reserves_its_only_worker_for_one_shot_requests() {
    let socket = TempSocket::new("single-worker-reservation");
    let streams = broker(8);
    let mut server = IpcServer::start_with_streams(
        socket.path(),
        Arc::new(ApplicationService::new()),
        Arc::new(SameUserPeerAuthorizer::current()),
        streams,
        IpcServerConfig {
            io_timeout: Duration::from_secs(2),
            worker_count: 1,
            pending_connection_capacity: 4,
        },
    )
    .expect("single-worker IPC server should start");
    let client = IpcClient::new(socket.path());

    let mut status = client
        .subscribe_status(None, 1)
        .expect("stream request should reach the server");
    let error = status
        .next_item()
        .expect_err("the reserved worker should reject streaming work");
    assert_eq!(error.code, ErrorCode::OperationUnavailable);

    assert!(matches!(
        client
            .execute(ApplicationOperation::GetStatus)
            .expect("the reserved worker should serve a one-shot request"),
        ApplicationOutput::Status(_)
    ));

    server.shutdown().expect("server should stop cleanly");
}
