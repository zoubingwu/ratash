#![cfg(unix)]

use std::io::{self, Read, Write};
use std::panic::{self, AssertUnwindSafe};
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use hopash::application::{ApplicationOperation, LatencyFreshness, LatencyProbeStatus};
use hopash::domain::{
    ActiveProfileSummary, ApplyState, CoreLifecycle, CoreStatus, NodeRecordId, ProbeQueueStatus,
    ProfileId, ProxyGroupId, SampleState, StatusSnapshot, StreamHealthSet, StreamState,
    SupervisorLifecycle, SupervisorStatus, TrafficSample, TunStatus,
};
use hopash::tui::{
    CrosstermControl, FullViewSnapshot, ProfileRow, ProxyGroupRow, ProxyGroupSnapshot, ProxyRow,
    TerminalSession, ViewLogRecord,
};
use hopash::tui_runtime::{
    CancellationToken, FullSnapshotSource, LogTail, StatusInterfaceError, StatusInterfaceSources,
    StatusLogEvent, StatusLogEventSource, UiCommandExecutor, run_crossterm_status_interface,
    run_crossterm_status_interface_with_render_writer, run_with_terminal_session,
};
use nix::sys::signal::{Signal, kill};
use nix::unistd::Pid;
use portable_pty::{CommandBuilder, PtySize, native_pty_system};

const CHILD_SCENARIO_ENV: &str = "HOPASH_TUI_PTY_CHILD_SCENARIO";
const PTY_TIMEOUT: Duration = Duration::from_secs(8);
const INITIAL_PTY_SIZE: PtySize = PtySize {
    rows: 24,
    cols: 80,
    pixel_width: 0,
    pixel_height: 0,
};
const SMALL_PTY_SIZE: PtySize = PtySize {
    rows: 20,
    cols: 70,
    pixel_width: 0,
    pixel_height: 0,
};
const RESIZED_PTY_SIZE: PtySize = PtySize {
    rows: 30,
    cols: 100,
    pixel_width: 0,
    pixel_height: 0,
};

#[test]
fn pty_child_entry() {
    let Ok(scenario) = std::env::var(CHILD_SCENARIO_ENV) else {
        return;
    };

    match scenario.as_str() {
        "interactive" | "sigint" | "sigterm" => {
            run_crossterm_status_interface(status_sources(false))
                .expect("the real terminal runner should exit cleanly");
        }
        "stalled_q" | "stalled_sigint" | "stalled_sigterm" => {
            run_crossterm_status_interface(stalled_status_sources())
                .expect("the cancelled command runner should exit cleanly");
        }
        "panic" => {
            let result = panic::catch_unwind(AssertUnwindSafe(|| {
                run_crossterm_status_interface(status_sources(true))
            }));
            assert!(result.is_err(), "the injected runtime panic should unwind");
        }
        "partial" => exercise_partial_terminal_initialization(),
        "render" => exercise_render_failure(),
        "repeat" => exercise_repeated_terminal_cleanup(),
        other => panic!("unknown PTY child scenario: {other}"),
    }

    emit_terminal_result(&scenario);
}

#[test]
fn real_pty_interaction_covers_resize_keyboard_mouse_and_restoration() {
    let mut interactive = PtySession::spawn_with_size("interactive", SMALL_PTY_SIZE);
    interactive.wait_for_text("Required:");
    interactive.resize(RESIZED_PTY_SIZE);
    interactive.wait_for_text("CONNECTED");
    interactive.write_input(b"p");
    interactive.wait_for_text("PROFILES (2)");
    interactive.write_input(b"\r");
    interactive.wait_for_text("HOPASH_PTY_COMMAND profile_use");
    interactive.wait_for_text("Success: done");
    interactive.write_input(b"2");
    interactive.wait_for_text("Nodes (2)");
    interactive.write_input(b"\x1b[<0;25;7M\x1b[<0;25;7m");
    interactive.wait_for_text("HOPASH_PTY_COMMAND proxy_select");
    interactive.write_input(b"5");
    interactive.wait_for_text("LOGS");
    interactive.write_input(b"\x1b[<0;52;4M\x1b[<0;52;4m");
    interactive.wait_for_text("following");
    interactive.write_input(b"q");
    let interactive_output = interactive.finish();
    assert_terminal_restored(&interactive_output, "interactive");
    assert_output_contains(&interactive_output, b"size=100x30", "resized terminal size");
    let separator = "─".repeat(100);
    assert_output_contains(
        &interactive_output,
        separator.as_bytes(),
        "100-column Status Interface frame",
    );
}

#[test]
fn real_pty_sigint_restores_terminal_modes() {
    assert_signal_restoration("sigint", Signal::SIGINT);
}

#[test]
fn real_pty_sigterm_restores_terminal_modes() {
    assert_signal_restoration("sigterm", Signal::SIGTERM);
}

#[test]
fn real_pty_q_interrupts_a_stalled_foreground_command() {
    assert_stalled_command_restoration("stalled_q", None);
}

#[test]
fn real_pty_sigint_interrupts_a_stalled_foreground_command() {
    assert_stalled_command_restoration("stalled_sigint", Some(Signal::SIGINT));
}

#[test]
fn real_pty_sigterm_interrupts_a_stalled_foreground_command() {
    assert_stalled_command_restoration("stalled_sigterm", Some(Signal::SIGTERM));
}

#[test]
fn real_pty_panic_restores_terminal_modes() {
    let mut panic_session = PtySession::spawn("panic");
    panic_session.wait_for_text("UP");
    let panic_output = panic_session.finish();
    assert_terminal_restored(&panic_output, "panic");
}

#[test]
fn real_pty_partial_initialization_restores_raw_mode() {
    let partial_output = PtySession::spawn("partial").finish();
    for (enabled, restored, mode) in terminal_mode_sequences()
        .into_iter()
        .filter(|(_, _, mode)| *mode != "cursor visibility")
    {
        let enabled_at = rfind_bytes(&partial_output, enabled).unwrap_or_else(|| {
            panic!(
                "partial setup did not enable {mode}: {}",
                output_tail(&partial_output)
            )
        });
        let restored_at = rfind_bytes(&partial_output, restored).unwrap_or_else(|| {
            panic!(
                "partial setup did not restore {mode}: {}",
                output_tail(&partial_output)
            )
        });
        assert!(
            restored_at > enabled_at,
            "partial setup left {mode} enabled: {}",
            output_tail(&partial_output)
        );
    }
    assert_result_marker(&partial_output, "partial");
}

#[test]
fn real_pty_render_error_restores_terminal_modes() {
    let render_output = PtySession::spawn("render").finish();
    assert_terminal_restored(&render_output, "render");
}

#[test]
fn real_pty_repeated_cleanup_emits_each_restoration_once() {
    let repeat_output = PtySession::spawn("repeat").finish();
    assert_terminal_restored(&repeat_output, "repeat");
    for (_, restored, mode) in terminal_mode_sequences() {
        assert_eq!(
            count_occurrences(&repeat_output, restored),
            1,
            "repeated cleanup should restore {mode} exactly once: {}",
            output_tail(&repeat_output)
        );
    }
}

fn assert_signal_restoration(scenario: &str, signal: Signal) {
    let mut session = PtySession::spawn(scenario);
    session.wait_for_text("UP");
    session.signal(signal);
    let output = session.finish();
    assert_terminal_restored(&output, scenario);
}

fn assert_stalled_command_restoration(scenario: &str, signal: Option<Signal>) {
    let mut session = PtySession::spawn(scenario);
    session.wait_for_text("UP");
    session.write_input(b"p\r");
    session.wait_for_text("HOPASH_PTY_COMMAND stalled");
    let started = Instant::now();
    if let Some(signal) = signal {
        session.signal(signal);
    } else {
        session.write_input(b"qq");
    }
    let output = session.finish();
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "{scenario} should interrupt the foreground wait within one second"
    );
    assert_terminal_restored(&output, scenario);
}

fn exercise_partial_terminal_initialization() {
    let mut control = CrosstermControl::new(FailingWriter::new());
    let error = run_with_terminal_session(&mut control, || Ok::<(), StatusInterfaceError>(()))
        .expect_err("the injected terminal writer should reject terminal setup");
    assert_eq!(
        error.kind,
        hopash::tui_runtime::StatusInterfaceErrorKind::TerminalSetup
    );
    assert!(
        control.into_inner().failed,
        "the terminal writer should fail while hiding the cursor"
    );
    assert!(
        !crossterm::terminal::is_raw_mode_enabled()
            .expect("the PTY raw-mode state should remain readable")
    );
}

fn exercise_repeated_terminal_cleanup() {
    let mut control = CrosstermControl::new(io::stdout());
    let mut session = TerminalSession::enter(&mut control)
        .expect("the real PTY should support every terminal mode");
    session
        .cleanup()
        .expect("the first terminal cleanup should succeed");
    session
        .cleanup()
        .expect("the repeated terminal cleanup should be idempotent");
}

fn exercise_render_failure() {
    let error = run_crossterm_status_interface_with_render_writer(
        status_sources(false),
        RenderFailingWriter,
    )
    .expect_err("the injected frame writer should reject rendering");
    assert_eq!(
        error.kind,
        hopash::tui_runtime::StatusInterfaceErrorKind::Render
    );
}

fn emit_terminal_result(scenario: &str) {
    let raw = crossterm::terminal::is_raw_mode_enabled()
        .expect("the PTY raw-mode state should remain readable");
    let (cols, rows) = crossterm::terminal::size()
        .expect("the PTY dimensions should remain readable after cleanup");
    println!("HOPASH_PTY_RESULT scenario={scenario} raw={raw} size={cols}x{rows}");
    io::stdout()
        .flush()
        .expect("the PTY result marker should flush");
}

fn status_sources(panic_on_event: bool) -> StatusInterfaceSources {
    StatusInterfaceSources {
        snapshots: Arc::new(StaticSnapshots),
        events: Arc::new(StaticEvents { panic_on_event }),
        commands: Arc::new(ImmediateCommands),
    }
}

fn stalled_status_sources() -> StatusInterfaceSources {
    StatusInterfaceSources {
        snapshots: Arc::new(StaticSnapshots),
        events: Arc::new(StaticEvents {
            panic_on_event: false,
        }),
        commands: Arc::new(StalledCommands),
    }
}

struct StaticSnapshots;

impl FullSnapshotSource for StaticSnapshots {
    fn fetch_full_snapshot(
        &self,
        _connection_generation: u64,
        _cancellation: &CancellationToken,
    ) -> Result<FullViewSnapshot, StatusInterfaceError> {
        Ok(full_snapshot())
    }

    fn fetch_proxy_group(
        &self,
        _group: &str,
        _connection_generation: u64,
        _cancellation: &CancellationToken,
    ) -> Result<ProxyGroupSnapshot, StatusInterfaceError> {
        Ok(ProxyGroupSnapshot {
            group: proxy_group(),
            groups: vec![proxy_group()],
            proxies: proxy_rows(),
        })
    }
}

struct StaticEvents {
    panic_on_event: bool,
}

impl StatusLogEventSource for StaticEvents {
    fn connect(
        &self,
        _connection_generation: u64,
        _cancellation: &CancellationToken,
    ) -> Result<(), StatusInterfaceError> {
        Ok(())
    }

    fn try_next(&self) -> Result<Option<StatusLogEvent>, StatusInterfaceError> {
        assert!(!self.panic_on_event, "injected Status Interface panic");
        Ok(None)
    }

    fn fetch_log_tail(
        &self,
        _connection_generation: u64,
        _after_sequence: Option<u64>,
        _cancellation: &CancellationToken,
    ) -> Result<LogTail, StatusInterfaceError> {
        Ok(LogTail {
            records: Vec::<ViewLogRecord>::new(),
            gap: false,
            dropped_total: 0,
        })
    }

    fn disconnect(&self, _connection_generation: u64) {}
}

struct ImmediateCommands;

struct StalledCommands;

impl UiCommandExecutor for StalledCommands {
    fn execute(
        &self,
        _operation: ApplicationOperation,
        cancellation: &CancellationToken,
    ) -> Result<String, StatusInterfaceError> {
        eprintln!("HOPASH_PTY_COMMAND stalled");
        let (sender, receiver) = mpsc::sync_channel(1);
        let _registration = cancellation.register_interrupt(move || {
            let _ = sender.send(());
        });
        receiver.recv_timeout(Duration::from_secs(4)).map_err(|_| {
            StatusInterfaceError::new(
                hopash::tui_runtime::StatusInterfaceErrorKind::Command,
                "The stalled fixture did not receive cancellation",
            )
        })?;
        Err(StatusInterfaceError::new(
            hopash::tui_runtime::StatusInterfaceErrorKind::Command,
            "The foreground command was cancelled",
        ))
    }
}

impl UiCommandExecutor for ImmediateCommands {
    fn execute(
        &self,
        operation: ApplicationOperation,
        _cancellation: &CancellationToken,
    ) -> Result<String, StatusInterfaceError> {
        match operation {
            ApplicationOperation::ProfileUse { .. } => {
                eprintln!("HOPASH_PTY_COMMAND profile_use");
            }
            ApplicationOperation::ProxySelect { .. } => {
                eprintln!("HOPASH_PTY_COMMAND proxy_select");
            }
            other => panic!("unexpected PTY command: {other:?}"),
        }
        Ok("done".to_owned())
    }
}

fn full_snapshot() -> FullViewSnapshot {
    FullViewSnapshot {
        status: StatusSnapshot {
            supervisor: SupervisorStatus {
                lifecycle: SupervisorLifecycle::Ready,
                started_at_unix_ms: 1,
                uptime_seconds: 2,
                health_reasons: Vec::new(),
            },
            core: CoreStatus {
                lifecycle: CoreLifecycle::Ready,
                pid: Some(42),
                instance_generation: None,
                restart: hopash::domain::CoreRestartStatus::default(),
            },
            tun: TunStatus {
                requested: true,
                capable: true,
                effective: true,
                reason: None,
            },
            active_profile: Some(ActiveProfileSummary {
                id: ProfileId::new(),
                name: "PTY Fixture".to_owned(),
            }),
            primary_proxy_group: None,
            selected_node: None,
            latency: None,
            traffic: TrafficSample {
                upload_bytes_per_second: 128,
                download_bytes_per_second: 256,
                sampled_at_unix_ms: Some(3),
                state: SampleState::Fresh,
            },
            connection_count: 1,
            runtime_generation: None,
            apply_state: ApplyState::Idle,
            runtime_apply: Default::default(),
            selection_restore_pending: false,
            probe_queue: ProbeQueueStatus::default(),
            stream_health: StreamHealthSet {
                traffic: StreamState::Healthy,
                connections: StreamState::Healthy,
                logs: StreamState::Healthy,
            },
        },
        proxy_groups: vec![proxy_group()],
        proxies: proxy_rows(),
        profiles: vec![profile("Primary", true), profile("Fallback", false)],
        logs: Vec::new(),
        dropped_logs: 0,
    }
}

fn proxy_group() -> ProxyGroupRow {
    ProxyGroupRow {
        id: ProxyGroupId::for_name("Automatic"),
        name: "Automatic".to_owned(),
        proxy_type: "Selector".to_owned(),
        selected_node: Some("Tokyo".to_owned()),
    }
}

fn proxy_rows() -> Vec<ProxyRow> {
    ["Tokyo", "Berlin"]
        .into_iter()
        .enumerate()
        .map(|(index, name)| ProxyRow {
            group_id: ProxyGroupId::for_name("Automatic"),
            group: "Automatic".to_owned(),
            node_id: Some(NodeRecordId::for_core(name)),
            name: name.to_owned(),
            node_type: "Shadowsocks".to_owned(),
            available: true,
            selected: index == 0,
            delay_ms: Some(20 + index as u64),
            sampled_at_unix_ms: Some(3),
            freshness: LatencyFreshness::Fresh,
            probe_status: LatencyProbeStatus::Succeeded,
        })
        .collect()
}

fn profile(name: &str, active: bool) -> ProfileRow {
    ProfileRow {
        id: ProfileId::new(),
        name: name.to_owned(),
        active,
        fresh: true,
        last_success_at_unix_ms: 2,
        next_refresh_at_unix_ms: 4,
        error: None,
    }
}

struct FailingWriter {
    stdout: io::Stdout,
    failed: bool,
}

impl FailingWriter {
    fn new() -> Self {
        Self {
            stdout: io::stdout(),
            failed: false,
        }
    }
}

impl Write for FailingWriter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        if !self.failed && contains_bytes(buffer, b"\x1b[?25l") {
            self.failed = true;
            return Err(io::Error::other("injected cursor setup failure"));
        }
        self.stdout.write(buffer)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.stdout.flush()
    }
}

struct RenderFailingWriter;

impl Write for RenderFailingWriter {
    fn write(&mut self, _buffer: &[u8]) -> io::Result<usize> {
        Err(io::Error::other("injected frame write failure"))
    }

    fn flush(&mut self) -> io::Result<()> {
        Err(io::Error::other("injected frame flush failure"))
    }
}

struct PtySession {
    child: Box<dyn portable_pty::Child + Send>,
    process_id: u32,
    writer: Box<dyn Write + Send>,
    reader: mpsc::Receiver<Vec<u8>>,
    reader_thread: Option<thread::JoinHandle<()>>,
    output: Vec<u8>,
    master: Box<dyn portable_pty::MasterPty + Send>,
    exited: bool,
}

impl PtySession {
    fn spawn(scenario: &str) -> Self {
        Self::spawn_with_size(scenario, INITIAL_PTY_SIZE)
    }

    fn spawn_with_size(scenario: &str, size: PtySize) -> Self {
        let pair = native_pty_system()
            .openpty(size)
            .expect("the test PTY should open");
        let executable = std::env::current_exe().expect("the test executable should resolve");
        let mut command = CommandBuilder::new(executable);
        command.arg("--exact");
        command.arg("pty_child_entry");
        command.arg("--nocapture");
        command.env(CHILD_SCENARIO_ENV, scenario);
        command.env("TERM", "xterm-256color");
        command.env("RUST_BACKTRACE", "0");

        let child = pair
            .slave
            .spawn_command(command)
            .expect("the PTY child should spawn");
        let process_id = child
            .process_id()
            .expect("the PTY child should expose its process ID");
        assert_ne!(process_id, std::process::id());
        drop(pair.slave);

        let reader = pair
            .master
            .try_clone_reader()
            .expect("the PTY reader should clone");
        let writer = pair
            .master
            .take_writer()
            .expect("the PTY writer should open");
        let (reader, reader_thread) = spawn_pty_reader(reader);

        Self {
            child,
            process_id,
            writer,
            reader,
            reader_thread: Some(reader_thread),
            output: Vec::new(),
            master: pair.master,
            exited: false,
        }
    }

    fn wait_for_text(&mut self, text: &str) {
        let deadline = Instant::now() + PTY_TIMEOUT;
        while !contains_bytes(&self.output, text.as_bytes()) {
            let remaining = deadline.saturating_duration_since(Instant::now());
            assert!(
                !remaining.is_zero(),
                "PTY child did not render {text:?}: {}",
                output_tail(&self.output)
            );
            match self.reader.recv_timeout(remaining) {
                Ok(chunk) => self.output.extend_from_slice(&chunk),
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    panic!(
                        "PTY child did not render {text:?}: {}",
                        output_tail(&self.output)
                    );
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    panic!(
                        "PTY child closed before rendering {text:?}: {}",
                        output_tail(&self.output)
                    );
                }
            }
        }
    }

    fn write_input(&mut self, input: &[u8]) {
        self.writer
            .write_all(input)
            .expect("the PTY input should write");
        self.writer.flush().expect("the PTY input should flush");
    }

    fn resize(&self, size: PtySize) {
        self.master.resize(size).expect("the PTY should resize");
    }

    fn signal(&self, signal: Signal) {
        let process_id =
            i32::try_from(self.process_id).expect("the PTY process ID should fit pid_t");
        kill(Pid::from_raw(process_id), signal).expect("the PTY child signal should send");
    }

    fn finish(mut self) -> Vec<u8> {
        let deadline = Instant::now() + PTY_TIMEOUT;
        let exit_code = loop {
            self.drain_ready_output();
            if let Some(status) = self
                .child
                .try_wait()
                .expect("the PTY child status should remain readable")
            {
                self.exited = true;
                break status.exit_code();
            }
            assert!(
                Instant::now() < deadline,
                "PTY child did not exit: {}",
                output_tail(&self.output)
            );
            thread::sleep(Duration::from_millis(10));
        };
        self.drain_final_output(deadline);
        self.reader_thread
            .take()
            .expect("the PTY reader thread should remain owned")
            .join()
            .expect("the PTY reader thread should exit cleanly");
        assert_eq!(
            exit_code,
            0,
            "PTY child exited with {exit_code}: {}",
            output_tail(&self.output)
        );
        std::mem::take(&mut self.output)
    }

    fn drain_ready_output(&mut self) {
        while let Ok(chunk) = self.reader.try_recv() {
            self.output.extend_from_slice(&chunk);
        }
    }

    fn drain_final_output(&mut self, deadline: Instant) {
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            assert!(
                !remaining.is_zero(),
                "PTY reader did not reach EOF: {}",
                output_tail(&self.output)
            );
            match self.reader.recv_timeout(remaining) {
                Ok(chunk) => self.output.extend_from_slice(&chunk),
                Err(mpsc::RecvTimeoutError::Disconnected) => break,
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    panic!(
                        "PTY reader did not reach EOF: {}",
                        output_tail(&self.output)
                    );
                }
            }
        }
    }
}

impl Drop for PtySession {
    fn drop(&mut self) {
        if self.exited {
            return;
        }
        match self.child.try_wait() {
            Ok(Some(_)) => {
                self.exited = true;
                return;
            }
            Ok(None) => {}
            Err(error) => {
                eprintln!("PTY child status cleanup failed: {error}");
                return;
            }
        }
        if let Err(error) = self.child.kill() {
            eprintln!("PTY child termination failed: {error}");
            return;
        }
        let deadline = Instant::now() + Duration::from_secs(1);
        loop {
            match self.child.try_wait() {
                Ok(Some(_)) => {
                    self.exited = true;
                    return;
                }
                Ok(None) if Instant::now() < deadline => {
                    thread::sleep(Duration::from_millis(10));
                }
                Ok(None) => {
                    eprintln!("PTY child did not exit during bounded cleanup");
                    return;
                }
                Err(error) => {
                    eprintln!("PTY child reap failed: {error}");
                    return;
                }
            }
        }
    }
}

fn spawn_pty_reader(
    mut reader: Box<dyn Read + Send>,
) -> (mpsc::Receiver<Vec<u8>>, thread::JoinHandle<()>) {
    let (sender, receiver) = mpsc::channel();
    let thread = thread::Builder::new()
        .name("hopash-tui-pty-reader".to_owned())
        .spawn(move || {
            let mut buffer = [0_u8; 4096];
            loop {
                match reader.read(&mut buffer) {
                    Ok(0) => break,
                    Ok(read) => {
                        if sender.send(buffer[..read].to_vec()).is_err() {
                            break;
                        }
                    }
                    Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                    Err(_) => break,
                }
            }
        })
        .expect("the PTY reader thread should spawn");
    (receiver, thread)
}

fn assert_terminal_restored(output: &[u8], scenario: &str) {
    for (enabled, restored, mode) in terminal_mode_sequences() {
        let enabled_at = rfind_bytes(output, enabled)
            .unwrap_or_else(|| panic!("{scenario} did not enable {mode}: {}", output_tail(output)));
        let restored_at = rfind_bytes(output, restored).unwrap_or_else(|| {
            panic!("{scenario} did not restore {mode}: {}", output_tail(output))
        });
        assert!(
            restored_at > enabled_at,
            "{scenario} left {mode} enabled: {}",
            output_tail(output)
        );
    }
    assert_result_marker(output, scenario);
}

fn terminal_mode_sequences() -> [(&'static [u8], &'static [u8], &'static str); 9] {
    [
        (
            b"\x1b[?1049h".as_slice(),
            b"\x1b[?1049l".as_slice(),
            "alternate screen",
        ),
        (
            b"\x1b[?1000h".as_slice(),
            b"\x1b[?1000l".as_slice(),
            "mouse normal tracking",
        ),
        (
            b"\x1b[?1002h".as_slice(),
            b"\x1b[?1002l".as_slice(),
            "mouse button tracking",
        ),
        (
            b"\x1b[?1003h".as_slice(),
            b"\x1b[?1003l".as_slice(),
            "mouse any-event tracking",
        ),
        (
            b"\x1b[?1015h".as_slice(),
            b"\x1b[?1015l".as_slice(),
            "mouse RXVT coordinates",
        ),
        (
            b"\x1b[?1006h".as_slice(),
            b"\x1b[?1006l".as_slice(),
            "mouse SGR coordinates",
        ),
        (
            b"\x1b[?1004h".as_slice(),
            b"\x1b[?1004l".as_slice(),
            "focus reporting",
        ),
        (
            b"\x1b[?2004h".as_slice(),
            b"\x1b[?2004l".as_slice(),
            "bracketed paste",
        ),
        (
            b"\x1b[?25l".as_slice(),
            b"\x1b[?25h".as_slice(),
            "cursor visibility",
        ),
    ]
}

fn assert_result_marker(output: &[u8], scenario: &str) {
    let marker = format!("HOPASH_PTY_RESULT scenario={scenario} raw=false");
    assert_output_contains(output, marker.as_bytes(), "terminal result marker");
}

fn assert_output_contains(output: &[u8], needle: &[u8], label: &str) {
    assert!(
        contains_bytes(output, needle),
        "PTY output omitted {label}: {}",
        output_tail(output)
    );
}

fn contains_bytes(haystack: &[u8], needle: &[u8]) -> bool {
    find_bytes(haystack, needle).is_some()
}

fn find_bytes(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

fn rfind_bytes(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .rposition(|window| window == needle)
}

fn count_occurrences(haystack: &[u8], needle: &[u8]) -> usize {
    haystack
        .windows(needle.len())
        .filter(|window| *window == needle)
        .count()
}

fn output_tail(output: &[u8]) -> String {
    let start = output.len().saturating_sub(4_000);
    String::from_utf8_lossy(&output[start..]).into_owned()
}
