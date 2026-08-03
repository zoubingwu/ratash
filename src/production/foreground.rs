//! Composes the public CLI client and its foreground runners.

use std::io::{self, Write};
use std::path::PathBuf;
use std::sync::{Arc, mpsc};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crate::application::{
    ApplicationClient, ApplicationError, ApplicationErrorDetails, ApplicationOperation,
    ApplicationOutput, ApplicationService, Clock, LifecycleAction, LifecycleFailureDetails,
    LifecycleOutcome, SystemClock,
};
use crate::cli::{
    ForegroundRunner, Invocation, OutputMode, run_invocation, run_invocation_with_frontend,
};
use crate::daemon::{
    DaemonAction, DaemonError, DaemonErrorKind, DaemonLifecycle, DaemonTimeouts,
    ShutdownAcknowledgement, ShutdownIntent, ShutdownPort, StartupFailureCategory, StartupStage,
    StdDaemonProcessControl, SystemDaemonClock,
};
use crate::domain::{
    ApplyState, CoreLifecycle, SampleState, StreamHealthSet, StreamState, SupervisorLifecycle,
    TrafficSample, TunReason,
};
use crate::error::{ErrorCode, ProcessExitCode};
use crate::frontend_ipc::{
    ForegroundLogFollower, IpcStatusLogEventSource, LogFollowCancellation, LogFollowFormat,
};
use crate::ipc_runtime::IpcClient;
use crate::lifecycle::StatePaths;
use crate::shutdown_ipc::request_shutdown as request_shutdown_over_control;
use crate::tui_runtime::{
    StatusInterfaceErrorKind, StatusInterfaceSources, run_crossterm_status_interface,
};

pub struct ProductionApplicationClient {
    ipc: Arc<IpcClient>,
    lifecycle: DaemonLifecycle<StdDaemonProcessControl, ShutdownControlPort, SystemDaemonClock>,
}

impl ProductionApplicationClient {
    pub fn discover() -> Result<Self, ApplicationError> {
        let paths = StatePaths::discover().map_err(|_| bootstrap_error())?;
        let process = Arc::new(StdDaemonProcessControl::current().map_err(|_| bootstrap_error())?);
        let shutdown = Arc::new(ShutdownControlPort::new(paths.shutdown_socket.clone()));
        let lifecycle = DaemonLifecycle::new(
            paths.clone(),
            process,
            shutdown,
            Arc::new(SystemDaemonClock),
            DaemonTimeouts::default(),
        );
        Ok(Self {
            ipc: Arc::new(IpcClient::new(paths.ipc_socket)),
            lifecycle,
        })
    }

    #[must_use]
    pub fn ipc(&self) -> Arc<IpcClient> {
        Arc::clone(&self.ipc)
    }

    fn execute_lifecycle(
        &self,
        operation: ApplicationOperation,
    ) -> Result<ApplicationOutput, ApplicationError> {
        let previous_status = self
            .ipc
            .execute(ApplicationOperation::GetStatus)
            .ok()
            .and_then(status_output);
        let outcome = match operation {
            ApplicationOperation::Start => self.lifecycle.start(),
            ApplicationOperation::Stop => self.lifecycle.stop(),
            ApplicationOperation::Restart => self.lifecycle.restart(),
            _ => unreachable!("lifecycle dispatch only accepts lifecycle operations"),
        }
        .map_err(map_daemon_error)?;

        let action = match outcome.action {
            DaemonAction::Start => LifecycleAction::Start,
            DaemonAction::Stop => LifecycleAction::Stop,
            DaemonAction::Restart => LifecycleAction::Restart,
        };
        let status = if action == LifecycleAction::Stop {
            stopped_status(previous_status)
        } else {
            self.ipc
                .execute(ApplicationOperation::GetStatus)
                .and_then(expect_status)?
        };
        Ok(ApplicationOutput::Lifecycle(LifecycleOutcome {
            action,
            changed: outcome.changed,
            status,
        }))
    }
}

impl ApplicationClient for ProductionApplicationClient {
    fn execute(
        &self,
        operation: ApplicationOperation,
    ) -> Result<ApplicationOutput, ApplicationError> {
        match operation {
            ApplicationOperation::Start
            | ApplicationOperation::Stop
            | ApplicationOperation::Restart => self.execute_lifecycle(operation),
            _ => self.ipc.execute(operation),
        }
    }
}

pub(super) struct ShutdownControlPort {
    socket_path: PathBuf,
}

impl ShutdownControlPort {
    pub(super) fn new(socket_path: PathBuf) -> Self {
        Self { socket_path }
    }
}

impl ShutdownPort for ShutdownControlPort {
    fn request_shutdown(
        &self,
        intent: &ShutdownIntent,
        timeout: Duration,
    ) -> io::Result<ShutdownAcknowledgement> {
        let acknowledgement = request_shutdown_over_control(&self.socket_path, intent, timeout)?;
        if acknowledgement.process != intent.process
            || acknowledgement.instance_token != intent.instance_token
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "the Supervisor returned a mismatched shutdown acknowledgement",
            ));
        }
        Ok(acknowledgement)
    }
}

pub struct IpcForegroundRunner {
    client: Arc<IpcClient>,
}

impl IpcForegroundRunner {
    #[must_use]
    pub fn new(client: Arc<IpcClient>) -> Self {
        Self { client }
    }
}

impl ForegroundRunner for IpcForegroundRunner {
    fn run_status_interface(&self, stderr: &mut dyn Write) -> ProcessExitCode {
        let events = Arc::new(IpcStatusLogEventSource::new(Arc::clone(&self.client)));
        let sources = StatusInterfaceSources::from_application(Arc::clone(&self.client), events);
        match run_crossterm_status_interface(sources) {
            Ok(()) => ProcessExitCode::Success,
            Err(error) => {
                let exit = match error.kind {
                    StatusInterfaceErrorKind::Snapshot | StatusInterfaceErrorKind::Stream => {
                        ProcessExitCode::SupervisorUnavailable
                    }
                    StatusInterfaceErrorKind::InvalidConfiguration
                    | StatusInterfaceErrorKind::Command
                    | StatusInterfaceErrorKind::CommandQueue
                    | StatusInterfaceErrorKind::TerminalInput
                    | StatusInterfaceErrorKind::TerminalSetup
                    | StatusInterfaceErrorKind::Render
                    | StatusInterfaceErrorKind::Signal => ProcessExitCode::InternalFailure,
                };
                if writeln!(stderr, "{error}").is_ok() {
                    exit
                } else {
                    ProcessExitCode::InternalFailure
                }
            }
        }
    }

    fn follow_logs(
        &self,
        output: OutputMode,
        stdout: &mut dyn Write,
        stderr: &mut dyn Write,
    ) -> ProcessExitCode {
        let cancellation = LogFollowCancellation::default();
        let signal_bridge = match LogSignalBridge::start(cancellation.clone()) {
            Ok(bridge) => bridge,
            Err(_) => {
                let _ = writeln!(stderr, "The log signal listener could not start");
                return ProcessExitCode::InternalFailure;
            }
        };
        let format = match output {
            OutputMode::Human => LogFollowFormat::Human,
            OutputMode::Json => LogFollowFormat::Ndjson,
        };
        let exit = ForegroundLogFollower::new(Arc::clone(&self.client)).run(
            format,
            stdout,
            stderr,
            &cancellation,
        );
        drop(signal_bridge);
        exit
    }
}

pub fn run_public_invocation(
    invocation: Invocation,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> ProcessExitCode {
    if matches!(
        &invocation,
        Invocation::PrintGeneralHelp | Invocation::PrintAgentHelp
    ) {
        return run_invocation(invocation, &ApplicationService::new(), stdout, stderr);
    }
    let mode = invocation_output_mode(&invocation);
    let client = match ProductionApplicationClient::discover() {
        Ok(client) => client,
        Err(error) => return crate::cli::write_application_error(error, mode, stderr),
    };
    let foreground = IpcForegroundRunner::new(client.ipc());
    run_invocation_with_frontend(invocation, &client, &foreground, stdout, stderr)
}

fn invocation_output_mode(invocation: &Invocation) -> OutputMode {
    match invocation {
        Invocation::Application { output, .. } | Invocation::FollowLogs { output } => *output,
        Invocation::PrintGeneralHelp
        | Invocation::PrintAgentHelp
        | Invocation::LaunchStatusInterface => OutputMode::Human,
    }
}

fn status_output(output: ApplicationOutput) -> Option<crate::domain::StatusSnapshot> {
    match output {
        ApplicationOutput::Status(status) => Some(status),
        _ => None,
    }
}

pub(super) fn expect_status(
    output: ApplicationOutput,
) -> Result<crate::domain::StatusSnapshot, ApplicationError> {
    status_output(output).ok_or_else(|| {
        ApplicationError::new(
            ErrorCode::ProtocolMismatch,
            "The Supervisor returned an unexpected lifecycle status",
            false,
        )
    })
}

fn stopped_status(
    previous: Option<crate::domain::StatusSnapshot>,
) -> crate::domain::StatusSnapshot {
    let now = SystemClock.now_unix_ms();
    let mut status = match previous {
        Some(status) => status,
        None => crate::domain::StatusSnapshot {
            supervisor: crate::domain::SupervisorStatus {
                lifecycle: SupervisorLifecycle::Stopped,
                started_at_unix_ms: now,
                uptime_seconds: 0,
                health_reasons: Vec::new(),
            },
            core: crate::domain::CoreStatus {
                lifecycle: CoreLifecycle::Stopped,
                pid: None,
                instance_generation: None,
                restart: crate::domain::CoreRestartStatus::default(),
            },
            tun: crate::domain::TunStatus {
                requested: true,
                capable: false,
                effective: false,
                reason: Some(TunReason::CoreUnavailable),
            },
            active_profile: None,
            primary_proxy_group: None,
            selected_node: None,
            latency: None,
            traffic: TrafficSample {
                upload_bytes_per_second: 0,
                download_bytes_per_second: 0,
                sampled_at_unix_ms: None,
                state: SampleState::Unavailable,
            },
            connection_count: 0,
            runtime_generation: None,
            apply_state: ApplyState::Idle,
            runtime_apply: crate::domain::RuntimeApplySnapshot::default(),
            selection_restore_pending: false,
            probe_queue: crate::domain::ProbeQueueStatus::default(),
            stream_health: StreamHealthSet {
                traffic: StreamState::Disconnected,
                connections: StreamState::Disconnected,
                logs: StreamState::Disconnected,
            },
        },
    };
    status.supervisor.lifecycle = SupervisorLifecycle::Stopped;
    status.core.lifecycle = CoreLifecycle::Stopped;
    status.core.pid = None;
    status.core.instance_generation = None;
    status.tun.capable = false;
    status.tun.effective = false;
    status.tun.reason = Some(TunReason::CoreUnavailable);
    status.stream_health = StreamHealthSet {
        traffic: StreamState::Disconnected,
        connections: StreamState::Disconnected,
        logs: StreamState::Disconnected,
    };
    status
}

fn bootstrap_error() -> ApplicationError {
    ApplicationError::new(
        ErrorCode::Internal,
        "The Ratash process environment could not be initialized",
        false,
    )
}

fn map_daemon_error(error: DaemonError) -> ApplicationError {
    let code = match (error.kind(), error.category()) {
        (DaemonErrorKind::StartupRejected, Some(StartupFailureCategory::Permission)) => {
            ErrorCode::TunPermissionDenied
        }
        (DaemonErrorKind::StartupRejected, Some(StartupFailureCategory::Unsupported)) => {
            ErrorCode::TunUnsupported
        }
        (DaemonErrorKind::LifecycleOperationBusy | DaemonErrorKind::SupervisorOwnershipBusy, _) => {
            ErrorCode::OperationUnavailable
        }
        (
            DaemonErrorKind::ShutdownRequestFailed
            | DaemonErrorKind::ShutdownTimedOut
            | DaemonErrorKind::InvalidShutdownAcknowledgement,
            _,
        ) => ErrorCode::SupervisorUnavailable,
        (
            DaemonErrorKind::SpawnFailed
            | DaemonErrorKind::StartupTimedOut
            | DaemonErrorKind::StartupProcessExited
            | DaemonErrorKind::StartupRejected
            | DaemonErrorKind::InvalidReadiness,
            _,
        ) => ErrorCode::CoreUnavailable,
        (
            DaemonErrorKind::InvalidLiveInstance
            | DaemonErrorKind::UnsafeStaleState
            | DaemonErrorKind::InvalidInternalInvocation
            | DaemonErrorKind::ProcessControlFailed
            | DaemonErrorKind::StateOperationFailed,
            _,
        ) => ErrorCode::Internal,
    };
    let retryable = matches!(
        error.kind(),
        DaemonErrorKind::LifecycleOperationBusy
            | DaemonErrorKind::SupervisorOwnershipBusy
            | DaemonErrorKind::StartupTimedOut
            | DaemonErrorKind::StartupProcessExited
            | DaemonErrorKind::ShutdownRequestFailed
            | DaemonErrorKind::ShutdownTimedOut
    );
    let mut message = error.to_string();
    if let Some(stage) = error.stage() {
        message.push_str(" [stage=");
        message.push_str(startup_stage_name(stage));
        message.push(']');
    }
    if let Some(category) = error.category() {
        message.push_str(" [category=");
        message.push_str(startup_category_name(category));
        message.push(']');
    }
    let application_error = ApplicationError::new(code, message, retryable);
    match error.stage() {
        Some(stage) => {
            let category = error
                .category()
                .unwrap_or_else(|| inferred_startup_category(stage));
            application_error.with_details(ApplicationErrorDetails::LifecycleFailure(Box::new(
                LifecycleFailureDetails {
                    stage: startup_stage_name(stage).to_owned(),
                    category: startup_category_name(category).to_owned(),
                },
            )))
        }
        None => application_error,
    }
}

fn inferred_startup_category(stage: StartupStage) -> StartupFailureCategory {
    match stage {
        StartupStage::StatePreparation
        | StartupStage::SingletonOwnership
        | StartupStage::StaleCleanup
        | StartupStage::SupervisorInitialization => StartupFailureCategory::Internal,
        StartupStage::ProcessSpawn | StartupStage::ProcessIdentity => {
            StartupFailureCategory::Process
        }
        StartupStage::CoreReadiness | StartupStage::Readiness => StartupFailureCategory::Readiness,
    }
}

fn startup_stage_name(stage: StartupStage) -> &'static str {
    match stage {
        StartupStage::StatePreparation => "state_preparation",
        StartupStage::SingletonOwnership => "singleton_ownership",
        StartupStage::StaleCleanup => "stale_cleanup",
        StartupStage::ProcessSpawn => "process_spawn",
        StartupStage::ProcessIdentity => "process_identity",
        StartupStage::SupervisorInitialization => "supervisor_initialization",
        StartupStage::CoreReadiness => "core_readiness",
        StartupStage::Readiness => "readiness",
    }
}

fn startup_category_name(category: StartupFailureCategory) -> &'static str {
    match category {
        StartupFailureCategory::Permission => "permission",
        StartupFailureCategory::Unsupported => "unsupported",
        StartupFailureCategory::Configuration => "configuration",
        StartupFailureCategory::Process => "process",
        StartupFailureCategory::Readiness => "readiness",
        StartupFailureCategory::Internal => "internal",
    }
}
// -----------------------------------------------------------------------------
// Foreground signal bridge
// -----------------------------------------------------------------------------

struct LogSignalBridge {
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    worker: Option<JoinHandle<()>>,
}

impl LogSignalBridge {
    fn start(cancellation: LogFollowCancellation) -> io::Result<Self> {
        let (stop, stop_receiver) = tokio::sync::oneshot::channel();
        let (ready_sender, ready_receiver) = mpsc::sync_channel(1);
        let worker = thread::Builder::new()
            .name("ratash-log-signals".to_owned())
            .spawn(move || run_log_signal_worker(cancellation, stop_receiver, ready_sender))?;
        match ready_receiver.recv() {
            Ok(Ok(())) => Ok(Self {
                stop: Some(stop),
                worker: Some(worker),
            }),
            Ok(Err(error)) => {
                let _ = worker.join();
                Err(error)
            }
            Err(_) => {
                let _ = worker.join();
                Err(io::Error::other("the log signal listener stopped early"))
            }
        }
    }
}

impl Drop for LogSignalBridge {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

fn run_log_signal_worker(
    cancellation: LogFollowCancellation,
    mut stop: tokio::sync::oneshot::Receiver<()>,
    ready: mpsc::SyncSender<io::Result<()>>,
) {
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .enable_time()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            let _ = ready.send(Err(error));
            return;
        }
    };
    runtime.block_on(async move {
        let mut interrupt =
            match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt()) {
                Ok(signal) => signal,
                Err(error) => {
                    let _ = ready.send(Err(error));
                    return;
                }
            };
        let mut terminate =
            match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
                Ok(signal) => signal,
                Err(error) => {
                    let _ = ready.send(Err(error));
                    return;
                }
            };
        if ready.send(Ok(())).is_err() {
            return;
        }
        tokio::select! {
            _ = interrupt.recv() => cancellation.cancel(),
            _ = terminate.recv() => cancellation.cancel(),
            _ = &mut stop => {}
        }
    });
}
