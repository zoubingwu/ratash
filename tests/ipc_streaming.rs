use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use ratash::application::{
    ApplicationClient, ApplicationOperation, ApplicationOutput, ApplicationService,
};
use ratash::constants::{
    CORE_LOG_LINE_MAX_BYTES, LOG_BROKER_RECOVERY_CAPACITY, LOG_BROKER_RECOVERY_MAX_BYTES,
    LOG_CAPACITY, LOG_TAIL_MAX_BYTES, LOG_TAIL_MAX_RECORDS,
};
use ratash::error::ErrorCode;
use ratash::ipc::{
    IpcRequest, IpcStreamFrame, IpcStreamPayload, LogStreamItem, RequestId, bind_private_listener,
    read_frame,
};
use ratash::ipc_runtime::{
    IpcClient, IpcServer, IpcServerConfig, IpcStreamBroker, SameUserPeerAuthorizer,
    StatusStreamUpdate,
};
use ratash::telemetry::{CoreLogRecord, LogLevel, LogSource, LogTail};

static NEXT_TEMP_ID: AtomicU64 = AtomicU64::new(1);

struct TempSocket {
    directory: PathBuf,
    path: PathBuf,
}

impl TempSocket {
    fn new(label: &str) -> Self {
        let id = NEXT_TEMP_ID.fetch_add(1, Ordering::Relaxed);
        let directory = PathBuf::from("/tmp").join(format!(
            "ratash-ipc-stream-{label}-{}-{id}",
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
fn default_broker_bounds_recovery_storage_and_encoded_tails_at_release_scale() {
    let socket = TempSocket::new("bounded-log-tail");
    let streams = Arc::new(
        IpcStreamBroker::new(10, 100, ApplicationService::new().status())
            .expect("fixture stream broker should be valid"),
    );
    let message = "x".repeat(CORE_LOG_LINE_MAX_BYTES);
    for sequence in 1..=u64::try_from(LOG_CAPACITY).expect("release capacity should fit") {
        streams
            .publish_log(log_record(sequence, &message))
            .expect("maximum-size fixture log should publish");
    }
    let mut server = start_server(&socket, streams);
    let client = IpcClient::new(socket.path());

    let tail = client
        .log_tail(None)
        .expect("bounded recovery tail should load");
    let encoded = serde_json::to_vec(&tail).expect("bounded recovery tail should encode");

    assert!(tail.gap);
    assert!(tail.records.len() <= LOG_TAIL_MAX_RECORDS);
    assert!(tail.records.len() <= LOG_BROKER_RECOVERY_CAPACITY);
    assert!(encoded.len() <= LOG_TAIL_MAX_BYTES);
    assert_eq!(
        tail.latest_sequence,
        Some(u64::try_from(LOG_CAPACITY).expect("release capacity should fit"))
    );
    assert_eq!(
        tail.dropped_total,
        u64::try_from(
            LOG_CAPACITY
                - (LOG_BROKER_RECOVERY_MAX_BYTES / CORE_LOG_LINE_MAX_BYTES)
                    .min(LOG_BROKER_RECOVERY_CAPACITY)
        )
        .expect("drop count should fit")
    );

    server.shutdown().expect("server should stop cleanly");
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
fn authoritative_log_tail_resynchronizes_broker_history_and_followers() {
    let socket = TempSocket::new("authoritative-log-resync");
    let streams = broker(8);
    streams
        .publish_log(log_record(1, "old-1"))
        .expect("the initial log should publish");
    streams
        .publish_log(log_record(2, "old-2"))
        .expect("the initial log should publish");
    let mut server = start_server(&socket, Arc::clone(&streams));
    let client = IpcClient::new(socket.path());
    let mut follower = client
        .follow_logs(Some(2), 12)
        .expect("the live follower should connect");

    streams
        .synchronize_log_tail(LogTail {
            records: vec![log_record(10, "new-10"), log_record(11, "new-11")],
            dropped_total: 8,
            gap: true,
            earliest_sequence: Some(10),
            latest_sequence: Some(11),
            sequence_horizon: Some(11),
        })
        .expect("the authoritative tail should replace broker history");

    assert_eq!(
        follower
            .next_item()
            .expect("the gap frame should be valid")
            .expect("the gap frame should exist")
            .item,
        LogStreamItem::Gap {
            after_sequence: Some(2),
            latest_sequence: 11,
        }
    );
    let tail = client
        .log_tail(Some(2))
        .expect("the replacement tail should load");
    assert_eq!(tail.dropped_total, 8);
    assert!(tail.gap);
    assert_eq!(
        tail.records
            .iter()
            .map(|record| record.sequence)
            .collect::<Vec<_>>(),
        [10, 11]
    );

    drop(follower);
    server.shutdown().expect("server should stop cleanly");
}

#[test]
fn source_drop_without_a_new_record_preserves_broker_history_and_reports_the_gap() {
    let socket = TempSocket::new("source-drop-without-record");
    let streams = broker(8);
    streams
        .publish_log(log_record(1, "retained"))
        .expect("the retained log should publish");
    let mut server = start_server(&socket, Arc::clone(&streams));
    let client = IpcClient::new(socket.path());
    let mut follower = client
        .follow_logs(Some(0), 19)
        .expect("the live follower should connect");
    assert!(matches!(
        follower
            .next_item()
            .expect("the retained frame should be valid")
            .expect("the retained frame should exist")
            .item,
        LogStreamItem::Record { record } if record.sequence == 1
    ));
    streams
        .synchronize_log_tail(LogTail {
            records: Vec::new(),
            dropped_total: 3,
            gap: true,
            earliest_sequence: Some(1),
            latest_sequence: Some(1),
            sequence_horizon: Some(4),
        })
        .expect("the source gap should synchronize");

    assert_eq!(
        follower
            .next_item()
            .expect("the dropped-only gap frame should be valid")
            .expect("the dropped-only gap frame should exist")
            .item,
        LogStreamItem::Gap {
            after_sequence: Some(1),
            latest_sequence: 4,
        }
    );

    let tail = client
        .log_tail(Some(1))
        .expect("the retained tail should load");

    assert!(tail.gap);
    assert_eq!(tail.dropped_total, 3);
    assert_eq!(tail.latest_sequence, Some(1));
    assert_eq!(tail.sequence_horizon, Some(4));
    assert!(tail.records.is_empty());

    let mut late_follower = client
        .follow_logs(Some(1), 20)
        .expect("a late follower should connect at the stale cursor");
    assert_eq!(
        late_follower
            .next_item()
            .expect("the late dropped-only gap should be valid")
            .expect("the late dropped-only gap should exist")
            .item,
        LogStreamItem::Gap {
            after_sequence: Some(1),
            latest_sequence: 4,
        }
    );

    streams
        .publish_log(log_record(5, "after-gap"))
        .expect("the next record should follow the dropped horizon");
    let mut recovered = client
        .follow_logs(tail.sequence_horizon, 21)
        .expect("the recovered follower should connect");
    assert_eq!(
        recovered
            .next_item()
            .expect("the recovered frame should be valid")
            .expect("the recovered frame should exist")
            .item,
        LogStreamItem::Record {
            record: (&log_record(5, "after-gap")).into(),
        }
    );
    drop(follower);
    drop(late_follower);
    drop(recovered);
    server.shutdown().expect("server should stop cleanly");
}

#[test]
fn fresh_empty_log_tail_synchronizes_and_round_trips_without_a_horizon() {
    let socket = TempSocket::new("fresh-empty-log-tail");
    let streams = broker(8);
    streams
        .synchronize_log_tail(LogTail {
            records: Vec::new(),
            dropped_total: 0,
            gap: false,
            earliest_sequence: None,
            latest_sequence: None,
            sequence_horizon: None,
        })
        .expect("the fresh empty tail should synchronize");
    let mut server = start_server(&socket, streams);
    let client = IpcClient::new(socket.path());

    let tail = client
        .log_tail(None)
        .expect("the fresh empty tail should round trip");

    assert!(tail.records.is_empty());
    assert_eq!(tail.latest_sequence, None);
    assert_eq!(tail.sequence_horizon, None);
    assert!(!tail.gap);
    server.shutdown().expect("server should stop cleanly");
}

#[test]
fn authoritative_synchronization_keeps_the_drop_counter_monotonic() {
    let socket = TempSocket::new("monotonic-drop-counter");
    let streams = broker(2);
    for sequence in 1..=4 {
        streams
            .publish_log(log_record(sequence, "locally-retained"))
            .expect("fixture logs should publish");
    }
    let mut server = start_server(&socket, Arc::clone(&streams));
    let client = IpcClient::new(socket.path());
    assert_eq!(
        client
            .log_tail(None)
            .expect("the initial tail should load")
            .dropped_total,
        2
    );

    streams
        .synchronize_log_tail(LogTail {
            records: vec![log_record(4, "authoritative")],
            dropped_total: 1,
            gap: true,
            earliest_sequence: Some(4),
            latest_sequence: Some(4),
            sequence_horizon: Some(4),
        })
        .expect("the authoritative tail should synchronize");
    assert_eq!(
        client
            .log_tail(None)
            .expect("the synchronized tail should load")
            .dropped_total,
        2
    );

    streams
        .synchronize_log_tail(LogTail {
            records: Vec::new(),
            dropped_total: 0,
            gap: true,
            earliest_sequence: Some(4),
            latest_sequence: Some(4),
            sequence_horizon: Some(4),
        })
        .expect("an empty authoritative update should synchronize");
    assert_eq!(
        client
            .log_tail(None)
            .expect("the empty synchronized tail should load")
            .dropped_total,
        2
    );
    server.shutdown().expect("server should stop cleanly");
}

#[test]
fn source_sequence_gaps_remain_visible_in_a_recovery_tail() {
    let socket = TempSocket::new("source-sequence-gap");
    let streams = broker(8);
    streams
        .synchronize_log_tail(LogTail {
            records: vec![log_record(1, "before"), log_record(5, "after")],
            dropped_total: 3,
            gap: true,
            earliest_sequence: Some(1),
            latest_sequence: Some(5),
            sequence_horizon: Some(5),
        })
        .expect("the discontinuous source tail should synchronize");
    let mut server = start_server(&socket, streams);
    let client = IpcClient::new(socket.path());

    let tail = client
        .log_tail(None)
        .expect("the recovery tail should load");

    assert!(tail.gap);
    assert_eq!(tail.dropped_total, 3);
    assert_eq!(
        tail.records
            .iter()
            .map(|record| record.sequence)
            .collect::<Vec<_>>(),
        [1, 5]
    );
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
        ratash::ipc::write_frame(&mut stream, &frame).expect("fixture frame should write");
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
