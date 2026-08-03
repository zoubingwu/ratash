use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use ratash::application::{ApplicationService, ApplicationService as FixtureApplication};
use ratash::error::ProcessExitCode;
use ratash::frontend_ipc::{
    ForegroundLogFollower, IpcStatusLogEventSource, LogFollowCancellation, LogFollowFormat,
};
use ratash::ipc_runtime::{
    IpcClient, IpcServer, IpcServerConfig, IpcStreamBroker, SameUserPeerAuthorizer,
};
use ratash::telemetry::{CoreLogRecord, LogLevel, LogSource};
use ratash::tui_runtime::{CancellationToken, StatusLogEvent, StatusLogEventSource};

static NEXT_TEMP_ID: AtomicU64 = AtomicU64::new(1);

struct TempSocket {
    directory: PathBuf,
    path: PathBuf,
}

impl TempSocket {
    fn new(label: &str) -> Self {
        let id = NEXT_TEMP_ID.fetch_add(1, Ordering::Relaxed);
        let directory = PathBuf::from("/tmp").join(format!(
            "ratash-frontend-ipc-{label}-{}-{id}",
            std::process::id()
        ));
        Self {
            path: directory.join("supervisor.sock"),
            directory,
        }
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

fn broker(log_capacity: usize) -> Arc<IpcStreamBroker> {
    Arc::new(
        IpcStreamBroker::with_log_capacity(
            10,
            100,
            FixtureApplication::new().status(),
            log_capacity,
        )
        .expect("fixture broker should be valid"),
    )
}

fn start_server(socket: &TempSocket, streams: Arc<IpcStreamBroker>) -> IpcServer {
    IpcServer::start_with_streams(
        socket.path(),
        Arc::new(ApplicationService::new()),
        Arc::new(SameUserPeerAuthorizer::current()),
        streams,
        IpcServerConfig {
            io_timeout: Duration::from_secs(2),
            worker_count: 5,
            pending_connection_capacity: 16,
        },
    )
    .expect("fixture server should start")
}

fn client(socket: &TempSocket) -> Arc<IpcClient> {
    Arc::new(IpcClient::with_timeouts(
        socket.path(),
        Duration::from_secs(1),
        Duration::from_secs(2),
    ))
}

fn log_record(sequence: u64, level: LogLevel, source: LogSource, message: &str) -> CoreLogRecord {
    CoreLogRecord::new(sequence, sequence * 1_000, level, source, message)
}

fn next_event(
    source: &IpcStatusLogEventSource,
    predicate: impl Fn(&StatusLogEvent) -> bool,
) -> StatusLogEvent {
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        if let Some(event) = source.try_next().expect("event source should stay valid")
            && predicate(&event)
        {
            return event;
        }
        assert!(Instant::now() < deadline, "timed out waiting for IPC event");
        thread::sleep(Duration::from_millis(2));
    }
}

#[test]
fn status_updates_are_full_snapshots_tagged_with_the_current_generation() {
    let socket = TempSocket::new("status");
    let streams = broker(8);
    let mut server = start_server(&socket, Arc::clone(&streams));
    let source = IpcStatusLogEventSource::new(client(&socket));
    source
        .connect(7, &CancellationToken::default())
        .expect("event source should connect");

    let initial = next_event(&source, |event| {
        matches!(event, StatusLogEvent::Status { .. })
    });
    assert!(matches!(
        initial,
        StatusLogEvent::Status {
            connection_generation: 7,
            ..
        }
    ));

    let mut first = ApplicationService::new().status();
    first.connection_count = 41;
    streams
        .publish_status(11, 110, first)
        .expect("first status should publish");
    let mut latest = ApplicationService::new().status();
    latest.connection_count = 42;
    streams
        .publish_status(12, 120, latest)
        .expect("latest status should publish");

    let changed = next_event(&source, |event| {
        matches!(
            event,
            StatusLogEvent::Status { status, .. } if status.connection_count == 42
        )
    });
    assert!(matches!(
        changed,
        StatusLogEvent::Status {
            connection_generation: 7,
            status,
        } if status.connection_count == 42
    ));

    source.disconnect(7);
    drop(source);
    server.shutdown().expect("fixture server should stop");
}

#[test]
fn reconnect_recovers_a_log_gap_from_the_bounded_authoritative_tail() {
    let socket = TempSocket::new("gap");
    let streams = broker(3);
    let mut server = start_server(&socket, Arc::clone(&streams));
    let source = IpcStatusLogEventSource::new(client(&socket));
    source
        .connect(1, &CancellationToken::default())
        .expect("first generation should connect");
    let _ = next_event(&source, |event| {
        matches!(event, StatusLogEvent::Status { .. })
    });

    streams
        .publish_log(log_record(1, LogLevel::Info, LogSource::CoreApi, "one"))
        .expect("first log should publish");
    let first = next_event(&source, |event| {
        matches!(event, StatusLogEvent::Logs { .. })
    });
    assert!(matches!(
        first,
        StatusLogEvent::Logs { records, .. } if records.iter().map(|record| record.sequence).collect::<Vec<_>>() == [1]
    ));
    source.disconnect(1);

    for sequence in 2..=6 {
        streams
            .publish_log(log_record(
                sequence,
                LogLevel::Info,
                LogSource::CoreApi,
                &format!("log-{sequence}"),
            ))
            .expect("fixture log should publish");
    }
    source
        .connect(2, &CancellationToken::default())
        .expect("second generation should connect");
    let recovered = next_event(&source, |event| {
        matches!(event, StatusLogEvent::Logs { .. })
    });
    let StatusLogEvent::Logs {
        connection_generation,
        records,
        gap,
        dropped_total,
    } = recovered
    else {
        unreachable!("filtered event should contain logs");
    };
    assert_eq!(connection_generation, 2);
    assert_eq!(
        records
            .iter()
            .map(|record| record.sequence)
            .collect::<Vec<_>>(),
        [4, 5, 6]
    );
    assert!(gap);
    assert_eq!(dropped_total, 3);

    source.disconnect(2);
    drop(source);
    server.shutdown().expect("fixture server should stop");
}

#[test]
fn reconnect_recovers_when_a_restarted_supervisor_resets_log_sequences() {
    let socket = TempSocket::new("sequence-reset");
    let old_streams = broker(8);
    let mut old_server = start_server(&socket, Arc::clone(&old_streams));
    let source = IpcStatusLogEventSource::new(client(&socket));
    source
        .connect(1, &CancellationToken::default())
        .expect("first generation should connect");
    let _ = next_event(&source, |event| {
        matches!(event, StatusLogEvent::Status { .. })
    });
    for sequence in 1..=5 {
        old_streams
            .publish_log(log_record(
                sequence,
                LogLevel::Info,
                LogSource::CoreApi,
                &format!("old-{sequence}"),
            ))
            .expect("old fixture log should publish");
    }
    let old_logs = next_event(&source, |event| {
        matches!(
            event,
            StatusLogEvent::Logs { records, .. }
                if records.last().is_some_and(|record| record.sequence == 5)
        )
    });
    assert!(matches!(
        old_logs,
        StatusLogEvent::Logs { records, .. } if records.last().is_some_and(|record| record.sequence == 5)
    ));
    source.disconnect(1);
    old_server.shutdown().expect("old server should stop");

    let new_streams = broker(8);
    new_streams
        .publish_log(log_record(1, LogLevel::Info, LogSource::CoreApi, "new-1"))
        .expect("new initial log should publish");
    let mut new_server = start_server(&socket, Arc::clone(&new_streams));
    source
        .connect(2, &CancellationToken::default())
        .expect("replacement generation should connect");
    let _ = next_event(&source, |event| {
        matches!(event, StatusLogEvent::Status { .. })
    });
    new_streams
        .publish_log(log_record(2, LogLevel::Info, LogSource::CoreApi, "new-2"))
        .expect("new live log should publish");

    let recovered = next_event(&source, |event| {
        matches!(event, StatusLogEvent::Logs { .. })
    });
    assert!(matches!(
        recovered,
        StatusLogEvent::Logs { records, gap: true, .. }
            if records.iter().map(|record| record.sequence).collect::<Vec<_>>() == [1, 2]
    ));

    source.disconnect(2);
    drop(source);
    new_server
        .shutdown()
        .expect("replacement server should stop");
}

#[test]
fn snapshot_tail_coverage_suppresses_live_duplicates() {
    let socket = TempSocket::new("tail-coverage");
    let streams = broker(8);
    let mut server = start_server(&socket, Arc::clone(&streams));
    let source = IpcStatusLogEventSource::new(client(&socket));
    source
        .connect(3, &CancellationToken::default())
        .expect("event source should connect");
    let _ = next_event(&source, |event| {
        matches!(event, StatusLogEvent::Status { .. })
    });
    streams
        .publish_log(log_record(1, LogLevel::Info, LogSource::CoreApi, "one"))
        .expect("fixture log should publish");

    let tail = source
        .fetch_log_tail(3, None, &CancellationToken::default())
        .expect("authoritative tail should load");
    assert_eq!(
        tail.records
            .iter()
            .map(|record| record.sequence)
            .collect::<Vec<_>>(),
        [1]
    );
    thread::sleep(Duration::from_millis(30));
    for _ in 0..8 {
        assert!(!matches!(
            source.try_next().expect("event source should stay valid"),
            Some(StatusLogEvent::Logs { .. })
        ));
    }

    source.disconnect(3);
    drop(source);
    server.shutdown().expect("fixture server should stop");
}

#[test]
fn disconnect_cancels_idle_readers_without_blocking_the_caller() {
    let socket = TempSocket::new("disconnect");
    let streams = broker(8);
    let mut server = start_server(&socket, streams);
    let source = IpcStatusLogEventSource::new(client(&socket));
    source
        .connect(4, &CancellationToken::default())
        .expect("event source should connect");
    let started = Instant::now();
    source.disconnect(4);
    assert!(started.elapsed() < Duration::from_millis(100));

    drop(source);
    server.shutdown().expect("fixture server should stop");
}

#[test]
fn foreground_human_follow_is_terminal_safe_and_returns_the_interrupt_exit() {
    let socket = TempSocket::new("human-follow");
    let streams = broker(8);
    let mut server = start_server(&socket, Arc::clone(&streams));
    let follower = ForegroundLogFollower::new(client(&socket));
    let cancellation = LogFollowCancellation::default();
    let canceller = cancellation.clone();
    let publisher = thread::spawn(move || {
        thread::sleep(Duration::from_millis(50));
        streams
            .publish_log(log_record(
                1,
                LogLevel::Warn,
                LogSource::Stderr,
                "unsafe\u{1b}[2J\nline",
            ))
            .expect("fixture log should publish");
        thread::sleep(Duration::from_millis(50));
        canceller.cancel();
    });
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();

    let exit = follower.run(
        LogFollowFormat::Human,
        &mut stdout,
        &mut stderr,
        &cancellation,
    );
    publisher.join().expect("fixture publisher should stop");
    assert_eq!(exit, ProcessExitCode::Interrupted);
    let output = String::from_utf8(stdout).expect("human output should be UTF-8");
    assert!(output.contains("WARN"));
    assert!(output.contains("stderr"));
    assert!(output.contains("unsafe [2J line"));
    assert!(!output.contains('\u{1b}'));
    assert!(stderr.is_empty());

    server.shutdown().expect("fixture server should stop");
}

#[test]
fn foreground_ndjson_follow_writes_one_versioned_event_per_line() {
    let socket = TempSocket::new("json-follow");
    let streams = broker(8);
    let mut server = start_server(&socket, Arc::clone(&streams));
    let follower = ForegroundLogFollower::new(client(&socket));
    let cancellation = LogFollowCancellation::default();
    let canceller = cancellation.clone();
    let publisher = thread::spawn(move || {
        thread::sleep(Duration::from_millis(50));
        streams
            .publish_log(log_record(1, LogLevel::Error, LogSource::CoreApi, "failed"))
            .expect("fixture log should publish");
        thread::sleep(Duration::from_millis(50));
        canceller.cancel();
    });
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();

    let exit = follower.run(
        LogFollowFormat::Ndjson,
        &mut stdout,
        &mut stderr,
        &cancellation,
    );
    publisher.join().expect("fixture publisher should stop");
    assert_eq!(exit, ProcessExitCode::Interrupted);
    let lines = String::from_utf8(stdout)
        .expect("NDJSON should be UTF-8")
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).expect("line should be JSON"))
        .collect::<Vec<_>>();
    assert_eq!(lines.len(), 1);
    assert_eq!(lines[0]["schema_version"], 1);
    assert_eq!(lines[0]["event"]["sequence"], 1);
    assert_eq!(lines[0]["event"]["level"], "error");
    assert_eq!(lines[0]["event"]["message"], "failed");
    assert!(stderr.is_empty());

    server.shutdown().expect("fixture server should stop");
}

#[test]
fn foreground_follow_reports_a_stable_error_without_exposing_the_socket_path() {
    let socket = TempSocket::new("missing");
    let follower = ForegroundLogFollower::new(client(&socket));
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let exit = follower.run(
        LogFollowFormat::Human,
        &mut stdout,
        &mut stderr,
        &LogFollowCancellation::default(),
    );

    assert_eq!(exit, ProcessExitCode::SupervisorUnavailable);
    assert!(stdout.is_empty());
    let diagnostic = String::from_utf8(stderr).expect("diagnostic should be UTF-8");
    assert!(diagnostic.contains("Supervisor IPC endpoint is unavailable"));
    assert!(!diagnostic.contains(socket.path().to_string_lossy().as_ref()));
}
