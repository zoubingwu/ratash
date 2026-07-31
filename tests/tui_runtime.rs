use std::collections::VecDeque;
use std::io;
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use hopash::application::{
    ApplicationClient, ApplicationError, ApplicationOperation, ApplicationOutput,
    ProfileListOutcome,
};
use hopash::constants::LOG_CAPACITY;
use hopash::domain::{
    ActiveProfileSummary, ApplyState, CoreLifecycle, CoreStatus, NodeRecordId, ProfileId,
    SampleState, StatusSnapshot, StreamHealthSet, StreamState, SupervisorLifecycle,
    SupervisorStatus, TrafficSample, TunStatus,
};
use hopash::ipc::RequestId;
use hopash::tui::{
    Command, FullViewSnapshot, KeyInput, TerminalAction, TerminalControl, TerminalInput, UiEvent,
    ViewLogRecord,
};
use hopash::tui_runtime::{
    ApplicationSnapshotSource, BackgroundCommandDispatcher, BoundedReconnectTimer,
    CancellationToken, CommandDispatchError, CommandDispatcher, DispatchedEvent,
    FullSnapshotSource, LogTail, NoShutdownSignal, RatatuiStatusRenderer, ReconnectTiming,
    RenderedFrame, RuntimeClock, ShutdownSignal, StatusInterfaceError, StatusInterfaceErrorKind,
    StatusInterfacePorts, StatusInterfaceRuntime, StatusInterfaceSources, StatusLogEvent,
    StatusLogEventSource, StatusRenderer, TerminalEventSource, UiCommandExecutor,
    bootstrap_status_interface, run_with_terminal_session,
};
use ratatui::backend::TestBackend;

#[test]
fn reconnect_backoff_grows_and_stays_within_the_product_bound() {
    let mut timer = BoundedReconnectTimer::new(Duration::from_millis(250), Duration::from_secs(10))
        .expect("product reconnect bounds should be valid");

    timer.schedule(4, Duration::from_secs(1));
    assert_eq!(timer.deadline(), Some(Duration::from_millis(1_250)));
    assert_eq!(timer.take_due(Duration::from_millis(1_249)), None);
    assert_eq!(timer.take_due(Duration::from_millis(1_250)), Some(4));

    for generation in 5..=12 {
        timer.schedule(generation, Duration::from_secs(2));
        assert!(
            timer
                .deadline()
                .expect("scheduled reconnect has a deadline")
                <= Duration::from_secs(12)
        );
        let _ = timer.take_due(Duration::from_secs(12));
    }

    timer.reset();
    timer.schedule(13, Duration::from_secs(20));
    assert_eq!(timer.deadline(), Some(Duration::from_millis(20_250)));
}

#[test]
fn bootstrap_connects_and_fetches_before_terminal_takeover() {
    let order = Arc::new(Mutex::new(Vec::new()));
    let events = Arc::new(FakeEvents::with_order(Arc::clone(&order)));
    let snapshots = Arc::new(FakeSnapshots {
        snapshot: snapshot(10),
        order: Arc::clone(&order),
        fail: false,
    });
    let sources = StatusInterfaceSources {
        snapshots,
        events: events.clone(),
        commands: Arc::new(ImmediateCommands),
    };

    let initial = bootstrap_status_interface(&sources, 3)
        .expect("fixture bootstrap should complete before terminal setup");

    assert_eq!(initial.status.traffic.upload_bytes_per_second, 10);
    assert_eq!(
        order
            .lock()
            .expect("order lock should be available")
            .as_slice(),
        ["connect", "snapshot"]
    );
    assert!(events.disconnects().is_empty());
}

#[test]
fn failed_bootstrap_disconnects_without_entering_the_terminal() {
    let order = Arc::new(Mutex::new(Vec::new()));
    let events = Arc::new(FakeEvents::with_order(Arc::clone(&order)));
    let sources = StatusInterfaceSources {
        snapshots: Arc::new(FakeSnapshots {
            snapshot: snapshot(0),
            order,
            fail: true,
        }),
        events: events.clone(),
        commands: Arc::new(ImmediateCommands),
    };

    let error = bootstrap_status_interface(&sources, 7)
        .expect_err("injected snapshot failure should stop bootstrap");

    assert_eq!(error.kind, StatusInterfaceErrorKind::Snapshot);
    assert_eq!(events.disconnects(), vec![7]);
}

#[test]
fn application_snapshot_adapter_reads_the_complete_initial_view() {
    let client = Arc::new(SnapshotClient::default());
    let events = Arc::new(FakeEvents::default());
    let source = ApplicationSnapshotSource::new(client.clone(), events);

    let full = source
        .fetch_full_snapshot(8, &CancellationToken::default())
        .expect("application outputs should form a complete snapshot");

    assert_eq!(full.status.traffic.upload_bytes_per_second, 55);
    assert!(full.profiles.is_empty());
    assert!(full.proxies.is_empty());
    assert_eq!(
        client
            .operations
            .lock()
            .expect("operation lock should be available")
            .as_slice(),
        [
            ApplicationOperation::GetStatus,
            ApplicationOperation::ProfileList
        ]
    );
}

#[test]
fn event_loop_coalesces_status_updates_into_one_frame() {
    let events = Arc::new(FakeEvents::default());
    for upload in [20, 30, 40] {
        events.push(StatusLogEvent::Status {
            connection_generation: 1,
            status: Box::new(status(upload)),
        });
    }
    let mut dispatcher = RecordingDispatcher::default();
    let mut reconnect = PassiveReconnect;
    let mut input = ScriptedInput::quit_on_poll(2);
    let clock = FixedClock(Duration::from_secs(1));
    let signal = NoShutdownSignal;
    let mut renderer = RecordingRenderer::new();

    {
        let mut runtime = StatusInterfaceRuntime::new(
            1,
            snapshot(10),
            StatusInterfacePorts {
                events,
                dispatcher: &mut dispatcher,
                reconnect: &mut reconnect,
                input: &mut input,
                clock: &clock,
                signal: &signal,
                renderer: &mut renderer,
            },
        );
        runtime.run().expect("scripted TUI run should exit cleanly");
        assert_eq!(
            runtime
                .state()
                .status
                .as_ref()
                .expect("runtime should retain the latest status")
                .traffic
                .upload_bytes_per_second,
            40
        );
    }

    assert_eq!(renderer.uploads, vec![10, 40]);
    assert!(dispatcher.cancelled_all);
}

#[test]
fn reconnect_deadline_dispatches_the_next_connection_generation() {
    let events = Arc::new(FakeEvents::default());
    events.push(StatusLogEvent::Disconnected {
        connection_generation: 4,
    });
    let mut dispatcher = RecordingDispatcher::default();
    let mut reconnect = ImmediateReconnect::default();
    let mut input = ScriptedInput::quit_on_poll(3);
    let clock = FixedClock(Duration::from_secs(5));
    let signal = NoShutdownSignal;
    let mut renderer = RecordingRenderer::new();

    {
        let mut runtime = StatusInterfaceRuntime::new(
            4,
            snapshot(1),
            StatusInterfacePorts {
                events,
                dispatcher: &mut dispatcher,
                reconnect: &mut reconnect,
                input: &mut input,
                clock: &clock,
                signal: &signal,
                renderer: &mut renderer,
            },
        );
        runtime
            .run()
            .expect("scripted reconnect should remain bounded");
    }

    assert!(matches!(
        dispatcher.submitted.as_slice(),
        [Command::Connect {
            connection_generation: 5
        }]
    ));
}

#[test]
fn live_log_batches_keep_only_the_bounded_tail() {
    let events = Arc::new(FakeEvents::default());
    events.push(StatusLogEvent::Logs {
        connection_generation: 1,
        records: (0..LOG_CAPACITY + 7)
            .map(|sequence| ViewLogRecord {
                sequence: sequence as u64,
                timestamp_unix_ms: sequence as u64,
                level: hopash::telemetry::LogLevel::Info,
                source: hopash::telemetry::LogSource::CoreApi,
                message: "fixture".to_owned(),
            })
            .collect(),
        gap: false,
        dropped_total: 0,
    });
    let mut dispatcher = RecordingDispatcher::default();
    let mut reconnect = PassiveReconnect;
    let mut input = ScriptedInput::quit_on_poll(2);
    let clock = FixedClock(Duration::ZERO);
    let signal = NoShutdownSignal;
    let mut renderer = RecordingRenderer::new();

    {
        let mut runtime = StatusInterfaceRuntime::new(
            1,
            snapshot(0),
            StatusInterfacePorts {
                events,
                dispatcher: &mut dispatcher,
                reconnect: &mut reconnect,
                input: &mut input,
                clock: &clock,
                signal: &signal,
                renderer: &mut renderer,
            },
        );
        runtime
            .run()
            .expect("bounded log intake should remain live");
        assert_eq!(runtime.state().logs.records.len(), LOG_CAPACITY);
        assert_eq!(
            runtime
                .state()
                .logs
                .records
                .front()
                .expect("bounded tail should retain records")
                .sequence,
            7
        );
    }
}

#[test]
fn background_dispatcher_runs_commands_off_the_render_thread_and_cancels_results() {
    let (started_sender, started_receiver) = mpsc::sync_channel(1);
    let (release_sender, release_receiver) = mpsc::sync_channel(1);
    let (finished_sender, finished_receiver) = mpsc::sync_channel(1);
    let commands = Arc::new(BlockingCommands {
        started: started_sender,
        release: Mutex::new(release_receiver),
        finished: finished_sender,
    });
    let events = Arc::new(FakeEvents::default());
    let sources = StatusInterfaceSources {
        snapshots: Arc::new(FakeSnapshots::successful(snapshot(0))),
        events,
        commands,
    };
    let mut dispatcher =
        BackgroundCommandDispatcher::new(sources).expect("fixture command workers should start");
    let request_id = RequestId(91);
    let profile_id = ProfileId::new();

    dispatcher
        .submit(Command::ActivateProfile {
            request_id,
            connection_generation: 2,
            profile_id,
        })
        .expect("fixture command should enter the bounded queue");
    let (worker_thread, operation) = started_receiver
        .recv_timeout(Duration::from_secs(1))
        .expect("worker should begin the command");
    assert_ne!(worker_thread, thread::current().id());
    assert_eq!(
        operation,
        ApplicationOperation::ProfileUse {
            profile: profile_id.to_string()
        }
    );
    dispatcher.cancel(request_id);
    release_sender
        .send(())
        .expect("fixture command should be released");
    finished_receiver
        .recv_timeout(Duration::from_secs(1))
        .expect("fixture command should finish");

    assert!(
        dispatcher
            .try_next()
            .expect("result queue should remain open")
            .is_none()
    );
    dispatcher.shutdown();
}

#[test]
fn successful_mutation_dispatches_a_refreshed_full_snapshot() {
    let order = Arc::new(Mutex::new(Vec::new()));
    let sources = StatusInterfaceSources {
        snapshots: Arc::new(FakeSnapshots {
            snapshot: snapshot(73),
            order: Arc::clone(&order),
            fail: false,
        }),
        events: Arc::new(FakeEvents::default()),
        commands: Arc::new(OrderedCommands {
            order: Arc::clone(&order),
        }),
    };
    let mut dispatcher =
        BackgroundCommandDispatcher::new(sources).expect("fixture command workers should start");

    dispatcher
        .submit(Command::SelectNode {
            request_id: RequestId(92),
            connection_generation: 6,
            group: "Automatic".to_owned(),
            node_id: NodeRecordId::for_core("Berlin"),
        })
        .expect("fixture command should enter the bounded queue");

    let dispatched = (0..100)
        .find_map(|_| {
            let event = dispatcher
                .try_next()
                .expect("result queue should remain open");
            if event.is_none() {
                thread::sleep(Duration::from_millis(5));
            }
            event
        })
        .expect("successful mutation should dispatch a bounded result");

    match dispatched.event {
        UiEvent::CommandResult {
            request_id,
            connection_generation,
            result: Ok(success),
        } => {
            assert_eq!(request_id, RequestId(92));
            assert_eq!(connection_generation, 6);
            assert_eq!(success.message, "done");
            assert_eq!(success.snapshot.status.traffic.upload_bytes_per_second, 73);
        }
        other => panic!("unexpected mutation event: {other:?}"),
    }
    assert_eq!(
        order
            .lock()
            .expect("order lock should be available")
            .as_slice(),
        ["command", "snapshot"]
    );
    dispatcher.shutdown();
}

#[test]
fn shutdown_signal_exits_and_restores_every_terminal_mode() {
    let events = Arc::new(FakeEvents::default());
    let mut dispatcher = RecordingDispatcher::default();
    let mut reconnect = PassiveReconnect;
    let mut input = ScriptedInput::never();
    let clock = FixedClock(Duration::ZERO);
    let signal = ImmediateShutdown;
    let mut renderer = RecordingRenderer::new();
    let mut terminal = RecordingTerminal::default();

    run_with_terminal_session(&mut terminal, || {
        let mut runtime = StatusInterfaceRuntime::new(
            1,
            snapshot(0),
            StatusInterfacePorts {
                events,
                dispatcher: &mut dispatcher,
                reconnect: &mut reconnect,
                input: &mut input,
                clock: &clock,
                signal: &signal,
                renderer: &mut renderer,
            },
        );
        runtime.run()
    })
    .expect("shutdown signal should produce a clean exit");

    assert_eq!(
        terminal.actions.last(),
        Some(&TerminalAction::DisableRawMode)
    );
    assert!(terminal.actions.contains(&TerminalAction::ShowCursor));
    assert!(
        terminal
            .actions
            .contains(&TerminalAction::LeaveAlternateScreen)
    );
}

#[test]
fn terminal_session_restores_modes_after_errors_and_panics() {
    let mut error_terminal = RecordingTerminal::default();
    let error = run_with_terminal_session(&mut error_terminal, || -> Result<(), _> {
        Err(StatusInterfaceError::new(
            StatusInterfaceErrorKind::Render,
            "injected render failure",
        ))
    })
    .expect_err("injected runtime error should be returned");
    assert_eq!(error.kind, StatusInterfaceErrorKind::Render);
    assert_eq!(
        error_terminal.actions.last(),
        Some(&TerminalAction::DisableRawMode)
    );

    let mut panic_terminal = RecordingTerminal::default();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = run_with_terminal_session(&mut panic_terminal, || -> Result<(), _> {
            panic!("injected runtime panic");
        });
    }));
    assert!(result.is_err());
    assert_eq!(
        panic_terminal.actions.last(),
        Some(&TerminalAction::DisableRawMode)
    );
}

fn snapshot(upload: u64) -> FullViewSnapshot {
    FullViewSnapshot {
        status: status(upload),
        proxies: Vec::new(),
        profiles: Vec::new(),
        logs: Vec::new(),
        dropped_logs: 0,
    }
}

fn status(upload: u64) -> StatusSnapshot {
    StatusSnapshot {
        supervisor: SupervisorStatus {
            lifecycle: SupervisorLifecycle::Ready,
            started_at_unix_ms: 1,
            uptime_seconds: 2,
        },
        core: CoreStatus {
            lifecycle: CoreLifecycle::Ready,
            pid: Some(42),
            instance_generation: None,
        },
        tun: TunStatus {
            requested: true,
            capable: true,
            effective: true,
            reason: None,
        },
        active_profile: Some(ActiveProfileSummary {
            id: ProfileId::new(),
            name: "Fixture".to_owned(),
        }),
        primary_proxy_group: None,
        selected_node: None,
        latency: None,
        traffic: TrafficSample {
            upload_bytes_per_second: upload,
            download_bytes_per_second: upload.saturating_mul(2),
            sampled_at_unix_ms: Some(3),
            state: SampleState::Fresh,
        },
        connection_count: 0,
        runtime_generation: None,
        apply_state: ApplyState::Idle,
        stream_health: StreamHealthSet {
            traffic: StreamState::Healthy,
            connections: StreamState::Healthy,
            logs: StreamState::Healthy,
        },
    }
}

struct FakeSnapshots {
    snapshot: FullViewSnapshot,
    order: Arc<Mutex<Vec<&'static str>>>,
    fail: bool,
}

impl FakeSnapshots {
    fn successful(snapshot: FullViewSnapshot) -> Self {
        Self {
            snapshot,
            order: Arc::new(Mutex::new(Vec::new())),
            fail: false,
        }
    }
}

impl FullSnapshotSource for FakeSnapshots {
    fn fetch_full_snapshot(
        &self,
        _connection_generation: u64,
        _cancellation: &CancellationToken,
    ) -> Result<FullViewSnapshot, StatusInterfaceError> {
        self.order
            .lock()
            .expect("order lock should be available")
            .push("snapshot");
        if self.fail {
            Err(StatusInterfaceError::new(
                StatusInterfaceErrorKind::Snapshot,
                "injected snapshot failure",
            ))
        } else {
            Ok(self.snapshot.clone())
        }
    }
}

#[derive(Default)]
struct FakeEvents {
    order: Option<Arc<Mutex<Vec<&'static str>>>>,
    events: Mutex<VecDeque<StatusLogEvent>>,
    disconnected: Mutex<Vec<u64>>,
}

impl FakeEvents {
    fn with_order(order: Arc<Mutex<Vec<&'static str>>>) -> Self {
        Self {
            order: Some(order),
            ..Self::default()
        }
    }

    fn push(&self, event: StatusLogEvent) {
        self.events
            .lock()
            .expect("event lock should be available")
            .push_back(event);
    }

    fn disconnects(&self) -> Vec<u64> {
        self.disconnected
            .lock()
            .expect("disconnect lock should be available")
            .clone()
    }
}

impl StatusLogEventSource for FakeEvents {
    fn connect(
        &self,
        _connection_generation: u64,
        _cancellation: &CancellationToken,
    ) -> Result<(), StatusInterfaceError> {
        if let Some(order) = &self.order {
            order
                .lock()
                .expect("order lock should be available")
                .push("connect");
        }
        Ok(())
    }

    fn try_next(&self) -> Result<Option<StatusLogEvent>, StatusInterfaceError> {
        Ok(self
            .events
            .lock()
            .expect("event lock should be available")
            .pop_front())
    }

    fn fetch_log_tail(
        &self,
        _connection_generation: u64,
        _after_sequence: Option<u64>,
        _cancellation: &CancellationToken,
    ) -> Result<LogTail, StatusInterfaceError> {
        Ok(LogTail {
            records: Vec::new(),
            gap: false,
            dropped_total: 0,
        })
    }

    fn disconnect(&self, connection_generation: u64) {
        self.disconnected
            .lock()
            .expect("disconnect lock should be available")
            .push(connection_generation);
    }
}

struct ImmediateCommands;

impl UiCommandExecutor for ImmediateCommands {
    fn execute(
        &self,
        _operation: ApplicationOperation,
        _cancellation: &CancellationToken,
    ) -> Result<String, StatusInterfaceError> {
        Ok("done".to_owned())
    }
}

struct OrderedCommands {
    order: Arc<Mutex<Vec<&'static str>>>,
}

impl UiCommandExecutor for OrderedCommands {
    fn execute(
        &self,
        _operation: ApplicationOperation,
        _cancellation: &CancellationToken,
    ) -> Result<String, StatusInterfaceError> {
        self.order
            .lock()
            .expect("order lock should be available")
            .push("command");
        Ok("done".to_owned())
    }
}

#[derive(Default)]
struct SnapshotClient {
    operations: Mutex<Vec<ApplicationOperation>>,
}

impl ApplicationClient for SnapshotClient {
    fn execute(
        &self,
        operation: ApplicationOperation,
    ) -> Result<ApplicationOutput, ApplicationError> {
        self.operations
            .lock()
            .expect("operation lock should be available")
            .push(operation.clone());
        match operation {
            ApplicationOperation::GetStatus => Ok(ApplicationOutput::Status(status(55))),
            ApplicationOperation::ProfileList => {
                Ok(ApplicationOutput::Profiles(ProfileListOutcome {
                    profiles: Vec::new(),
                }))
            }
            _ => panic!("snapshot fixture received an unexpected operation"),
        }
    }
}

struct BlockingCommands {
    started: mpsc::SyncSender<(thread::ThreadId, ApplicationOperation)>,
    release: Mutex<mpsc::Receiver<()>>,
    finished: mpsc::SyncSender<()>,
}

impl UiCommandExecutor for BlockingCommands {
    fn execute(
        &self,
        operation: ApplicationOperation,
        cancellation: &CancellationToken,
    ) -> Result<String, StatusInterfaceError> {
        self.started
            .send((thread::current().id(), operation))
            .expect("test should receive worker identity");
        self.release
            .lock()
            .expect("release lock should be available")
            .recv()
            .expect("test should release worker");
        self.finished
            .send(())
            .expect("test should receive completion");
        if cancellation.is_cancelled() {
            Err(StatusInterfaceError::new(
                StatusInterfaceErrorKind::Command,
                "cancelled",
            ))
        } else {
            Ok("done".to_owned())
        }
    }
}

#[derive(Default)]
struct RecordingDispatcher {
    submitted: Vec<Command>,
    results: VecDeque<DispatchedEvent>,
    cancelled: Vec<RequestId>,
    cancelled_all: bool,
    shutdown: bool,
}

impl CommandDispatcher for RecordingDispatcher {
    fn submit(&mut self, command: Command) -> Result<(), CommandDispatchError> {
        self.submitted.push(command);
        Ok(())
    }

    fn cancel(&mut self, request_id: RequestId) {
        self.cancelled.push(request_id);
    }

    fn cancel_all(&mut self) {
        self.cancelled_all = true;
    }

    fn try_next(&mut self) -> Result<Option<DispatchedEvent>, CommandDispatchError> {
        Ok(self.results.pop_front())
    }

    fn shutdown(&mut self) {
        self.shutdown = true;
    }
}

#[derive(Default)]
struct PassiveReconnect;

impl ReconnectTiming for PassiveReconnect {
    fn schedule(&mut self, _connection_generation: u64, _now: Duration) {}

    fn take_due(&mut self, _now: Duration) -> Option<u64> {
        None
    }

    fn reset(&mut self) {}
}

#[derive(Default)]
struct ImmediateReconnect {
    generation: Option<u64>,
}

impl ReconnectTiming for ImmediateReconnect {
    fn schedule(&mut self, connection_generation: u64, _now: Duration) {
        self.generation = Some(connection_generation);
    }

    fn take_due(&mut self, _now: Duration) -> Option<u64> {
        self.generation.take()
    }

    fn reset(&mut self) {
        self.generation = None;
    }
}

struct FixedClock(Duration);

impl RuntimeClock for FixedClock {
    fn now(&self) -> Duration {
        self.0
    }
}

struct ImmediateShutdown;

impl ShutdownSignal for ImmediateShutdown {
    fn shutdown_requested(&self) -> bool {
        true
    }
}

struct ScriptedInput {
    polls: usize,
    quit_on: Option<usize>,
}

impl ScriptedInput {
    fn quit_on_poll(poll: usize) -> Self {
        Self {
            polls: 0,
            quit_on: Some(poll),
        }
    }

    fn never() -> Self {
        Self {
            polls: 0,
            quit_on: None,
        }
    }
}

impl TerminalEventSource for ScriptedInput {
    fn poll_event(&mut self, _timeout: Duration) -> Result<Option<UiEvent>, StatusInterfaceError> {
        self.polls += 1;
        Ok(
            (self.quit_on == Some(self.polls)).then_some(UiEvent::Terminal(TerminalInput::Key(
                KeyInput::Character('q'),
            ))),
        )
    }
}

struct RecordingRenderer {
    renderer: RatatuiStatusRenderer<TestBackend>,
    uploads: Vec<u64>,
}

impl RecordingRenderer {
    fn new() -> Self {
        Self {
            renderer: RatatuiStatusRenderer::new(TestBackend::new(100, 30))
                .expect("TestBackend should initialize"),
            uploads: Vec::new(),
        }
    }
}

impl StatusRenderer for RecordingRenderer {
    fn draw(
        &mut self,
        state: &hopash::tui::AppState,
    ) -> Result<RenderedFrame, StatusInterfaceError> {
        self.uploads.push(
            state
                .status
                .as_ref()
                .map_or(0, |status| status.traffic.upload_bytes_per_second),
        );
        self.renderer.draw(state)
    }
}

#[derive(Default)]
struct RecordingTerminal {
    actions: Vec<TerminalAction>,
}

impl TerminalControl for RecordingTerminal {
    fn apply(&mut self, action: TerminalAction) -> io::Result<()> {
        self.actions.push(action);
        Ok(())
    }
}
