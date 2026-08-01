//! Production process composition for the public CLI and hidden service modes.

use std::ffi::OsString;
use std::fmt;
use std::fs;
use std::io::{self, Read, Write};
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, mpsc};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crate::application::{
    ApplicationClient, ApplicationError, ApplicationErrorDetails, ApplicationOperation,
    ApplicationOutput, ApplicationService, Clock, LifecycleAction, LifecycleFailureDetails,
    LifecycleOutcome, SystemClock,
};
use crate::background::{BackgroundApplication, BackgroundRuntime, MihomoBackgroundCorePort};
use crate::cli::{
    ForegroundRunner, Invocation, OutputMode, run_invocation, run_invocation_with_frontend,
};
use crate::config::{
    AuthoritativeConfig, BUNDLED_CORE_VERSION, ConfigCompiler, CoreConfigValidator,
};
use crate::constants::{
    CORE_SERVICE_LIVENESS_INTERVAL, IPC_REQUEST_TIMEOUT, MIHOMO_BINARY_MAX_BYTES,
    STATUS_SAMPLE_INTERVAL,
};
use crate::core::{
    CoreRuntime, MihomoAdapter, OwnerSessionProof, OwnerSessionRequest, ProcessOutputSource,
};
use crate::core_service_ipc::{CoreServiceClient, CoreServiceServer, CoreServiceServerConfig};
use crate::daemon::{
    DaemonAction, DaemonError, DaemonErrorKind, DaemonLifecycle, DaemonTimeouts,
    InternalSupervisorInvocation, ReadinessFailure, ShutdownAcknowledgement, ShutdownIntent,
    ShutdownPort, StartupFailureCategory, StartupStage, StdDaemonProcessControl,
    SupervisorOwnership, SystemDaemonClock,
};
use crate::domain::{
    ApplyState, CoreLifecycle, LocalRuleSetRevision, SampleState, StreamHealthSet, StreamState,
    SupervisorLifecycle, TrafficSample, TunReason,
};
use crate::error::{ErrorCode, ProcessExitCode};
use crate::frontend_ipc::{
    ForegroundLogFollower, IpcStatusLogEventSource, LogFollowCancellation, LogFollowFormat,
};
use crate::ipc_runtime::{
    IpcClient, IpcServer, IpcServerConfig, IpcStreamBroker, SameUserPeerAuthorizer,
};
use crate::lifecycle::{
    ManagedCoreIdentityRecord, ProcessIdentity, PsProcessInspector, StatePaths,
    current_process_identity,
};
use crate::mihomo::{MihomoAdapterConfig, UnixMihomoAdapter};
use crate::persistence::PersistenceStore;
use crate::process_controller::{
    NativeCoreProcessController, RootTunCapabilityPreflight, SystemProcessIdentityProbe,
};
use crate::profile::{ActiveProfileRevision, ProfileRevision};
use crate::profile_source::{ProfileSourcePolicy, ReqwestProfileSource};
use crate::runtime_adapters::{
    MihomoRuntimeHealthProbe, StagedRuntimeBundleResolver, classify_runtime_apply_error,
};
use crate::runtime_bundle::RuntimeBundleStager;
use crate::service::{
    CORE_RUNTIME_PROTOCOL_VERSION, CallerCredentialValidator, PrivilegedCoreRuntimeService,
    PrivilegedServiceConfig, PrivilegedServiceDependencies, ServicePlatformError,
    ServicePlatformErrorKind, UuidSecretGenerator,
};
use crate::shutdown_ipc::{
    ShutdownControlError, ShutdownControlHandler, ShutdownIpcServer,
    request_shutdown as request_shutdown_over_control,
};
use crate::state::AuthoritativeStateStore;
use crate::supervisor::{
    BlockingProfileFetchPort, CoordinatedSupervisorTransactions, DirectSupervisorCorePort,
    Supervisor, SupervisorDependencies, SupervisorRevisionAuthority, SupervisorTransactionPort,
};
use crate::telemetry::{LogLevel, LogSource};
use crate::transaction::{
    CandidateRevisionSource, CandidateRevisions, ConfigTransactionCoordinator,
    ConfigTransactionDependencies, CoreRuntimeApplyAdapter, RuntimeApplyPort,
    RuntimeBundleResolver, TransactionStore,
};
use crate::tui_runtime::{
    MonotonicClock, ProcessSignalSource, RuntimeClock, RuntimeWaiter, RuntimeWaker,
    ShutdownSignal as ProcessShutdownSignal, StatusInterfaceErrorKind, StatusInterfaceSources,
    run_crossterm_status_interface,
};
use crate::validator::MihomoCommandValidator;

pub const INTERNAL_CORE_SERVICE_MODE: &str = "__core-service";
pub const CORE_SERVICE_SOCKET_PATH: &str = "/var/run/hopash-rs/core-service.sock";
pub const BUNDLED_MIHOMO_PATH: &str = "/Library/Application Support/Hopash RS/bin/mihomo";

#[cfg(debug_assertions)]
const DEBUG_CORE_SERVICE_SOCKET_ENV: &str = "HOPASH_CORE_SERVICE_SOCKET";

const OBSERVER_LOG_BATCH: usize = 256;

// -----------------------------------------------------------------------------
// Public foreground composition
// -----------------------------------------------------------------------------

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

struct ShutdownControlPort {
    socket_path: PathBuf,
}

impl ShutdownControlPort {
    fn new(socket_path: PathBuf) -> Self {
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

fn expect_status(
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
        "The Hopash process environment could not be initialized",
        false,
    )
}

fn map_daemon_error(error: DaemonError) -> ApplicationError {
    let code = match (error.kind(), error.category()) {
        (DaemonErrorKind::StartupRejected, Some(StartupFailureCategory::Permission)) => {
            ErrorCode::TunPermissionDenied
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
        StartupFailureCategory::Configuration => "configuration",
        StartupFailureCategory::Process => "process",
        StartupFailureCategory::Readiness => "readiness",
        StartupFailureCategory::Internal => "internal",
    }
}

// -----------------------------------------------------------------------------
// Hidden privileged service mode
// -----------------------------------------------------------------------------

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CoreServiceInvocation {
    pub owner_uid: u32,
    pub socket_path: PathBuf,
    pub runtime_root: PathBuf,
    pub mihomo_binary: PathBuf,
}

impl CoreServiceInvocation {
    pub fn parse_process_arguments(args: &[OsString]) -> io::Result<Option<Self>> {
        if args.get(1).and_then(|value| value.to_str()) != Some(INTERNAL_CORE_SERVICE_MODE) {
            return Ok(None);
        }
        if args.len() != 10
            || args.get(2).and_then(|value| value.to_str()) != Some("--owner-uid")
            || args.get(4).and_then(|value| value.to_str()) != Some("--socket")
            || args.get(6).and_then(|value| value.to_str()) != Some("--runtime-root")
            || args.get(8).and_then(|value| value.to_str()) != Some("--mihomo")
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "the internal Core service invocation is invalid",
            ));
        }
        let owner_uid = args[3]
            .to_str()
            .and_then(|value| value.parse::<u32>().ok())
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "the internal Core service owner is invalid",
                )
            })?;
        let invocation = Self {
            owner_uid,
            socket_path: PathBuf::from(&args[5]),
            runtime_root: PathBuf::from(&args[7]),
            mihomo_binary: PathBuf::from(&args[9]),
        };
        if !invocation.socket_path.is_absolute()
            || !invocation.runtime_root.is_absolute()
            || !invocation.mihomo_binary.is_absolute()
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "the internal Core service paths must be absolute",
            ));
        }
        Ok(Some(invocation))
    }
}

struct ExactCallerCredentials {
    owner_uid: u32,
}

impl CallerCredentialValidator for ExactCallerCredentials {
    fn validate(&self, request: &OwnerSessionRequest) -> Result<(), ServicePlatformError> {
        if request.owner_uid != self.owner_uid
            || request.supervisor_pid == 0
            || request.supervisor_start_identity.is_empty()
            || request.instance_token.is_empty()
            || request.protocol_version != CORE_RUNTIME_PROTOCOL_VERSION
        {
            return Err(ServicePlatformError::new(
                ServicePlatformErrorKind::Credential,
            ));
        }
        let actual = crate::lifecycle::ProcessInspector::identity(
            &PsProcessInspector,
            request.supervisor_pid,
        )
        .map_err(|_| ServicePlatformError::new(ServicePlatformErrorKind::Credential))?;
        if actual.as_deref() == Some(request.supervisor_start_identity.as_str()) {
            Ok(())
        } else {
            Err(ServicePlatformError::new(
                ServicePlatformErrorKind::Credential,
            ))
        }
    }
}

pub fn run_core_service(invocation: CoreServiceInvocation) -> io::Result<()> {
    if !nix::unistd::Uid::effective().is_root() {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "the privileged Core service requires root privileges",
        ));
    }
    let compiler = ConfigCompiler::bundled().map_err(invalid_product_configuration)?;
    let compiler_policy_sha256 = compiler.compiler_policy_sha256().to_owned();
    let mihomo_binary_sha256 = verified_binary_sha256(&invocation.mihomo_binary)?;
    let runtime = Arc::new(
        PrivilegedCoreRuntimeService::new(
            PrivilegedServiceConfig::product_defaults(
                invocation.runtime_root.clone(),
                compiler_policy_sha256,
                mihomo_binary_sha256,
            ),
            PrivilegedServiceDependencies {
                credentials: Box::new(ExactCallerCredentials {
                    owner_uid: invocation.owner_uid,
                }),
                identities: Box::new(SystemProcessIdentityProbe),
                tun: Box::new(RootTunCapabilityPreflight),
                secrets: Box::new(UuidSecretGenerator),
                processes: Box::new(NativeCoreProcessController::product_defaults()),
            },
        )
        .map_err(|_| io::Error::other("the privileged Core service could not initialize"))?,
    );
    let mut server = CoreServiceServer::start(
        &invocation.socket_path,
        Arc::clone(&runtime),
        CoreServiceServerConfig::new(invocation.runtime_root, invocation.owner_uid),
    )?;
    let signal = ProcessSignalSource::new()
        .map_err(|_| io::Error::other("the Core service signal listener could not start"))?;
    let clock = MonotonicClock::default();
    let wake = RuntimeWaker::default();
    run_core_service_maintenance_loop(&signal, &clock, &wake, wake.clone(), |now| {
        runtime
            .maintenance_step(now)
            .map(|step| step.next_deadline)
            .unwrap_or_else(|_| now.saturating_add(CORE_SERVICE_LIVENESS_INTERVAL))
    });
    let server_result = server.shutdown();
    let service_result = runtime
        .shutdown_service()
        .map_err(|_| io::Error::other("the Core service shutdown failed"));
    server_result.and(service_result)
}

fn run_core_service_maintenance_loop(
    signal: &dyn ProcessShutdownSignal,
    clock: &dyn RuntimeClock,
    waiter: &dyn RuntimeWaiter,
    waker: RuntimeWaker,
    mut maintenance: impl FnMut(Duration) -> Duration,
) {
    signal.install_waker(waker);
    loop {
        let checkpoint = waiter.checkpoint();
        if signal.shutdown_requested() {
            break;
        }
        let next_deadline = maintenance(clock.now());
        if signal.shutdown_requested() {
            break;
        }
        let timeout = next_deadline.saturating_sub(clock.now());
        waiter.wait(checkpoint, Some(timeout));
    }
}

// -----------------------------------------------------------------------------
// Hidden Supervisor mode
// -----------------------------------------------------------------------------

pub fn run_internal_supervisor(invocation: InternalSupervisorInvocation) -> io::Result<()> {
    let readiness = invocation
        .readiness_channel()
        .map_err(|_| io::Error::other("the Supervisor readiness channel is invalid"))?;
    let inspector = PsProcessInspector;
    let process = current_process_identity(&inspector)
        .map_err(|_| io::Error::other("the Supervisor process identity is unavailable"))?;
    let paths = StatePaths::for_root(invocation.state_root);
    let started_at_unix_ms = SystemClock.now_unix_ms();
    let ownership = match SupervisorOwnership::acquire(
        paths.clone(),
        process.clone(),
        started_at_unix_ms,
        &inspector,
    ) {
        Ok(ownership) => ownership,
        Err(error) => {
            let failure = readiness_failure_from_daemon(&error);
            let _ = readiness.publish_failure(process, failure);
            return Err(io::Error::other(error.to_string()));
        }
    };
    let ownership = Arc::new(Mutex::new(Some(ownership)));
    let result = run_owned_supervisor(paths, process.clone(), Arc::clone(&ownership), &readiness);
    if let Err(error) = &result {
        let _ = readiness.publish_failure(process, error.failure.clone());
    }
    release_ownership(&ownership)?;
    result.map_err(|error| io::Error::other(error.failure.message))
}

#[derive(Clone, Debug)]
struct StartupError {
    failure: ReadinessFailure,
}

impl StartupError {
    fn new(
        stage: StartupStage,
        category: StartupFailureCategory,
        message: impl Into<String>,
    ) -> Self {
        Self {
            failure: ReadinessFailure {
                stage,
                category,
                message: message.into(),
            },
        }
    }
}

fn run_owned_supervisor(
    paths: StatePaths,
    process: ProcessIdentity,
    ownership: Arc<Mutex<Option<SupervisorOwnership>>>,
    readiness: &crate::daemon::ReadinessChannel,
) -> Result<(), StartupError> {
    let compiler = ConfigCompiler::bundled().map_err(|_| {
        StartupError::new(
            StartupStage::SupervisorInitialization,
            StartupFailureCategory::Configuration,
            "The bundled configuration policy is invalid",
        )
    })?;
    let compiler_policy_sha256 = compiler.compiler_policy_sha256().to_owned();
    let mihomo_binary = bundled_mihomo_path().map_err(|_| {
        StartupError::new(
            StartupStage::SupervisorInitialization,
            StartupFailureCategory::Configuration,
            "The bundled Mihomo path is unavailable",
        )
    })?;
    let mihomo_binary_sha256 = verified_binary_sha256(&mihomo_binary).map_err(|_| {
        StartupError::new(
            StartupStage::SupervisorInitialization,
            StartupFailureCategory::Configuration,
            "The bundled Mihomo executable is unavailable or invalid",
        )
    })?;

    let core_runtime: Arc<dyn CoreRuntime> = Arc::new(core_service_client().map_err(|_| {
        StartupError::new(
            StartupStage::SupervisorInitialization,
            StartupFailureCategory::Configuration,
            "The Core service endpoint configuration is invalid",
        )
    })?);
    let instance_token = ownership
        .lock()
        .map_err(|_| startup_internal("The Supervisor ownership state is unavailable"))?
        .as_ref()
        .map(|ownership| ownership.record().instance_token().to_owned())
        .ok_or_else(|| startup_internal("The Supervisor ownership state is unavailable"))?;
    let session = core_runtime
        .open_owner_session(&OwnerSessionRequest {
            owner_uid: nix::unistd::Uid::effective().as_raw(),
            supervisor_pid: process.pid,
            supervisor_start_identity: process.start_identity.clone(),
            instance_token: instance_token.clone(),
            protocol_version: CORE_RUNTIME_PROTOCOL_VERSION,
        })
        .map_err(map_core_session_startup)?;
    let owner = session.proof;
    let owner_guard = OwnerSessionGuard::new(Arc::clone(&core_runtime), owner.clone());
    let authoritative = AuthoritativeConfig::new(
        session.endpoint.socket_path.to_string_lossy().into_owned(),
        session.endpoint.secret().to_owned(),
    );

    let validator: Arc<dyn CoreConfigValidator + Send + Sync> = Arc::new(
        MihomoCommandValidator::bundled(&mihomo_binary, &mihomo_binary_sha256).map_err(|_| {
            StartupError::new(
                StartupStage::SupervisorInitialization,
                StartupFailureCategory::Configuration,
                "The Mihomo validation policy is invalid",
            )
        })?,
    );
    let bundle_root = paths.runtime.join("generations");
    let stager = Arc::new(
        RuntimeBundleStager::new(
            &bundle_root,
            &mihomo_binary,
            &mihomo_binary_sha256,
            &compiler_policy_sha256,
        )
        .map_err(|_| startup_internal("The Runtime Bundle staging root is unavailable"))?,
    );
    let resolver: Arc<dyn RuntimeBundleResolver> = Arc::new(
        StagedRuntimeBundleResolver::new(
            &bundle_root,
            &compiler_policy_sha256,
            &mihomo_binary_sha256,
        )
        .map_err(|_| startup_internal("The Runtime Bundle resolver is unavailable"))?,
    );
    let state_store = Arc::new(
        AuthoritativeStateStore::open(&paths.persistence)
            .map_err(|_| startup_internal("The authoritative state store is unavailable"))?,
    );
    let transaction_store: Arc<dyn TransactionStore> = Arc::new(
        PersistenceStore::open(&paths.persistence)
            .map_err(|_| startup_internal("The transaction store is unavailable"))?,
    );
    let revisions = Arc::new(SupervisorRevisionAuthority::new(CandidateRevisions {
        profile: ProfileRevision(0),
        active_profile: ActiveProfileRevision(0),
        local_rule_set: LocalRuleSetRevision(0),
        compiler_policy_sha256: compiler_policy_sha256.clone(),
        core_version: BUNDLED_CORE_VERSION.to_owned(),
    }));
    let revision_source: Arc<dyn CandidateRevisionSource> = revisions.clone();
    let runtime_apply: Arc<dyn RuntimeApplyPort> = Arc::new(CoreRuntimeApplyAdapter::new(
        Arc::clone(&core_runtime),
        classify_runtime_apply_error,
    ));
    let mihomo = Arc::new(
        UnixMihomoAdapter::new(MihomoAdapterConfig::default())
            .map_err(|_| startup_internal("The Mihomo adapter policy is invalid"))?,
    );
    let mihomo_port: Arc<dyn MihomoAdapter> = mihomo.clone();
    let lifecycle_lock = Arc::new(Mutex::new(()));
    let coordinator = Arc::new(ConfigTransactionCoordinator::new(
        ConfigTransactionDependencies {
            store: transaction_store,
            runtime: runtime_apply,
            validator: Arc::clone(&validator),
            health: Arc::new(MihomoRuntimeHealthProbe::bundled(Arc::clone(&mihomo_port))),
            revisions: revision_source,
            bundles: resolver,
            lifecycle_lock: Arc::clone(&lifecycle_lock),
        },
        owner.clone(),
    ));
    let transactions: Arc<dyn SupervisorTransactionPort> =
        Arc::new(CoordinatedSupervisorTransactions::new(
            Arc::clone(&state_store),
            coordinator,
            stager,
            revisions,
        ));
    let source = ReqwestProfileSource::new(ProfileSourcePolicy::product()).map_err(|_| {
        StartupError::new(
            StartupStage::SupervisorInitialization,
            StartupFailureCategory::Configuration,
            "The Profile source policy is invalid",
        )
    })?;
    let source = Arc::new(
        BlockingProfileFetchPort::new(Arc::new(source))
            .map_err(|_| startup_internal("The Profile source runtime could not start"))?,
    );
    let core = Arc::new(DirectSupervisorCorePort::new(
        Arc::clone(&core_runtime),
        Arc::clone(&mihomo_port),
        owner.clone(),
    ));
    let supervisor = Arc::new(
        Supervisor::open(SupervisorDependencies {
            clock: Arc::new(SystemClock),
            source,
            compiler,
            validator,
            transactions,
            state_store,
            core,
            authoritative,
            staging_root: paths.runtime.join("profiles"),
        })
        .map_err(|_| startup_internal("The Supervisor state could not be opened"))?,
    );
    let initial_status = supervisor
        .execute(ApplicationOperation::GetStatus)
        .and_then(expect_status)
        .map_err(|_| {
            StartupError::new(
                StartupStage::CoreReadiness,
                StartupFailureCategory::Readiness,
                "The initial Supervisor status is unavailable",
            )
        })?;
    update_instance_record(
        &ownership,
        &initial_status,
        core_runtime.status(&owner).ok(),
    )
    .map_err(|_| startup_internal("The Supervisor instance record could not be updated"))?;

    let broker = Arc::new(
        IpcStreamBroker::new(0, SystemClock.now_unix_ms(), initial_status.clone())
            .map_err(|_| startup_internal("The IPC stream broker could not start"))?,
    );
    let drain = Arc::new(DrainController::default());
    let application = Arc::new(SupervisorApplication {
        supervisor: Arc::clone(&supervisor),
        drain: Arc::clone(&drain),
    });
    let mut ipc_server = IpcServer::start_with_streams(
        &paths.ipc_socket,
        application,
        Arc::new(SameUserPeerAuthorizer::current()),
        Arc::clone(&broker),
        IpcServerConfig::default(),
    )
    .map_err(|_| startup_internal("The Supervisor IPC server could not start"))?;
    let shutdown_handler = Arc::new(ProductionShutdownHandler {
        process: process.clone(),
        instance_token,
        drain: Arc::clone(&drain),
    });
    let mut shutdown_server = ShutdownIpcServer::start(
        &paths.shutdown_socket,
        shutdown_handler,
        Arc::new(SameUserPeerAuthorizer::current()),
        IPC_REQUEST_TIMEOUT,
    )
    .map_err(|_| startup_internal("The Supervisor shutdown IPC server could not start"))?;
    let background_application: Arc<dyn BackgroundApplication> = supervisor.clone();
    let background_mihomo: Arc<dyn MihomoAdapter> = Arc::new(
        UnixMihomoAdapter::new(MihomoAdapterConfig::default())
            .map_err(|_| startup_internal("The background Mihomo adapter policy is invalid"))?,
    );
    let background_core = Arc::new(MihomoBackgroundCorePort::new(background_mihomo));
    let mut background = BackgroundRuntime::start(
        background_application,
        background_core,
        Arc::new(SystemClock),
    )
    .map_err(|_| startup_internal("The Supervisor background runtime could not start"))?;
    let mut observer = SupervisorObserver::start(ObserverDependencies {
        supervisor: Arc::clone(&supervisor),
        core_runtime: Arc::clone(&core_runtime),
        owner: owner.clone(),
        broker,
        ownership: Arc::clone(&ownership),
    })
    .map_err(|_| startup_internal("The Supervisor observer could not start"))?;

    {
        let guard = ownership
            .lock()
            .map_err(|_| startup_internal("The Supervisor ownership state is unavailable"))?;
        guard
            .as_ref()
            .ok_or_else(|| startup_internal("The Supervisor ownership state is unavailable"))?
            .publish_ready(readiness)
            .map_err(|_| startup_internal("The Supervisor readiness signal could not publish"))?;
    }

    let process_signal = ProcessSignalSource::new()
        .map_err(|_| startup_internal("The Supervisor signal listener could not start"))?;
    let shutdown_waker = RuntimeWaker::default();
    wait_for_supervisor_shutdown(
        drain.as_ref(),
        &process_signal,
        &shutdown_waker,
        shutdown_waker.clone(),
    );

    run_shutdown_sequence(
        || background.shutdown().map_err(io::Error::other),
        || observer.shutdown(),
        || {
            drain.wait_for_mutations();
            let runtime_result = lifecycle_lock
                .lock()
                .map_err(|_| io::Error::other("the Core lifecycle lock is unavailable"))
                .and_then(|_guard| {
                    core_runtime
                        .stop(&owner)
                        .map(|_| ())
                        .map_err(|_| io::Error::other("the Managed Core could not stop"))
                });
            let close_result = owner_guard.close();
            runtime_result.and(close_result)
        },
        || ipc_server.shutdown(),
        || shutdown_server.shutdown(),
    )
    .map_err(|_| {
        StartupError::new(
            StartupStage::SupervisorInitialization,
            StartupFailureCategory::Internal,
            "The Supervisor shutdown did not complete cleanly",
        )
    })
}

fn wait_for_supervisor_shutdown(
    drain: &DrainController,
    signal: &dyn ProcessShutdownSignal,
    waiter: &dyn RuntimeWaiter,
    waker: RuntimeWaker,
) {
    signal.install_waker(waker.clone());
    drain.install_waker(waker);
    loop {
        let checkpoint = waiter.checkpoint();
        if signal.shutdown_requested() {
            drain.request();
        }
        if drain.is_requested() {
            return;
        }
        waiter.wait(checkpoint, None);
    }
}

struct OwnerSessionGuard {
    runtime: Arc<dyn CoreRuntime>,
    owner: OwnerSessionProof,
    closed: AtomicBool,
}

impl OwnerSessionGuard {
    fn new(runtime: Arc<dyn CoreRuntime>, owner: OwnerSessionProof) -> Self {
        Self {
            runtime,
            owner,
            closed: AtomicBool::new(false),
        }
    }

    fn close(&self) -> io::Result<()> {
        if self.closed.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        self.runtime
            .close_owner_session(&self.owner)
            .map_err(|_| io::Error::other("the Core owner session could not close"))
    }
}

impl Drop for OwnerSessionGuard {
    fn drop(&mut self) {
        if !self.closed.swap(true, Ordering::AcqRel) {
            let _ = self.runtime.stop(&self.owner);
            let _ = self.runtime.close_owner_session(&self.owner);
        }
    }
}

fn run_shutdown_sequence<B, O, C, M, S>(
    background: B,
    observer: O,
    core_and_owner: C,
    main_ipc: M,
    control_ipc: S,
) -> io::Result<()>
where
    B: FnOnce() -> io::Result<()>,
    O: FnOnce() -> io::Result<()>,
    C: FnOnce() -> io::Result<()>,
    M: FnOnce() -> io::Result<()>,
    S: FnOnce() -> io::Result<()>,
{
    let background_result = background();
    let observer_result = observer();
    let core_result = core_and_owner();
    let main_ipc_result = main_ipc();
    let control_ipc_result = control_ipc();
    background_result
        .and(observer_result)
        .and(core_result)
        .and(main_ipc_result)
        .and(control_ipc_result)
}

#[derive(Default)]
struct DrainState {
    requested: bool,
    active_mutations: usize,
}

#[derive(Default)]
struct DrainController {
    state: Mutex<DrainState>,
    ready: Condvar,
    waker: Mutex<Option<RuntimeWaker>>,
}

impl DrainController {
    fn request(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.requested = true;
        self.ready.notify_all();
        drop(state);
        if let Some(waker) = self
            .waker
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
        {
            waker.wake();
        }
    }

    fn install_waker(&self, waker: RuntimeWaker) {
        *self
            .waker
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(waker.clone());
        if self.is_requested() {
            waker.wake();
        }
    }

    fn is_requested(&self) -> bool {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .requested
    }

    fn begin_mutation(self: &Arc<Self>) -> Option<MutationGuard> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.requested {
            return None;
        }
        state.active_mutations += 1;
        Some(MutationGuard {
            drain: Arc::clone(self),
        })
    }

    fn wait_for_mutations(&self) {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let _guard = self
            .ready
            .wait_while(state, |state| state.active_mutations > 0)
            .unwrap_or_else(std::sync::PoisonError::into_inner);
    }
}

struct MutationGuard {
    drain: Arc<DrainController>,
}

impl Drop for MutationGuard {
    fn drop(&mut self) {
        let mut state = self
            .drain
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.active_mutations -= 1;
        if state.active_mutations == 0 {
            self.drain.ready.notify_all();
        }
    }
}

struct ProductionShutdownHandler {
    process: ProcessIdentity,
    instance_token: String,
    drain: Arc<DrainController>,
}

impl ShutdownControlHandler for ProductionShutdownHandler {
    fn request_shutdown(
        &self,
        intent: &ShutdownIntent,
    ) -> Result<ShutdownAcknowledgement, ShutdownControlError> {
        if intent.protocol_version != crate::ipc::IPC_PROTOCOL_VERSION
            || intent.process != self.process
            || intent.instance_token != self.instance_token
        {
            return Err(ShutdownControlError::Rejected);
        }
        self.drain.request();
        Ok(ShutdownAcknowledgement {
            process: self.process.clone(),
            instance_token: self.instance_token.clone(),
        })
    }
}

struct SupervisorApplication<A> {
    supervisor: Arc<A>,
    drain: Arc<DrainController>,
}

impl<A: ApplicationClient> ApplicationClient for SupervisorApplication<A> {
    fn execute(
        &self,
        operation: ApplicationOperation,
    ) -> Result<ApplicationOutput, ApplicationError> {
        if is_lifecycle_operation(&operation) {
            return Err(ApplicationError::new(
                ErrorCode::OperationUnavailable,
                "Supervisor lifecycle operations require the dedicated control channel",
                false,
            ));
        }
        let _mutation = if is_mutation(&operation) {
            Some(self.drain.begin_mutation().ok_or_else(|| {
                ApplicationError::new(
                    ErrorCode::OperationUnavailable,
                    "The Supervisor is shutting down",
                    true,
                )
            })?)
        } else {
            None
        };
        let output = self.supervisor.execute(operation)?;
        Ok(if self.drain.is_requested() {
            stopping_output(output)
        } else {
            output
        })
    }
}

fn is_lifecycle_operation(operation: &ApplicationOperation) -> bool {
    matches!(
        operation,
        ApplicationOperation::Start | ApplicationOperation::Stop | ApplicationOperation::Restart
    )
}

fn is_mutation(operation: &ApplicationOperation) -> bool {
    matches!(
        operation,
        ApplicationOperation::ProfileAdd { .. }
            | ApplicationOperation::ProfileUse { .. }
            | ApplicationOperation::ProfileRemove { .. }
            | ApplicationOperation::ProxySelect { .. }
            | ApplicationOperation::RuleAdd { .. }
            | ApplicationOperation::RuleReplace { .. }
            | ApplicationOperation::RuleRemove { .. }
    )
}

fn stopping_output(output: ApplicationOutput) -> ApplicationOutput {
    match output {
        ApplicationOutput::Status(mut status) => {
            status.supervisor.lifecycle = SupervisorLifecycle::Stopping;
            if status.core.lifecycle != CoreLifecycle::Unconfigured
                && status.core.lifecycle != CoreLifecycle::Stopped
            {
                status.core.lifecycle = CoreLifecycle::Stopping;
            }
            ApplicationOutput::Status(status)
        }
        output => output,
    }
}

#[derive(Default)]
struct WakeSignal {
    requested: AtomicBool,
    mutex: Mutex<()>,
    ready: Condvar,
}

impl WakeSignal {
    fn request(&self) {
        self.requested.store(true, Ordering::Release);
        self.ready.notify_all();
    }

    fn is_requested(&self) -> bool {
        self.requested.load(Ordering::Acquire)
    }

    fn wait(&self, timeout: Duration) {
        if self.is_requested() {
            return;
        }
        let guard = self
            .mutex
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let _ = self.ready.wait_timeout(guard, timeout);
    }
}

struct ObserverDependencies {
    supervisor: Arc<Supervisor>,
    core_runtime: Arc<dyn CoreRuntime>,
    owner: OwnerSessionProof,
    broker: Arc<IpcStreamBroker>,
    ownership: Arc<Mutex<Option<SupervisorOwnership>>>,
}

struct SupervisorObserver {
    shutdown: Arc<WakeSignal>,
    thread: Option<JoinHandle<()>>,
}

impl SupervisorObserver {
    fn start(dependencies: ObserverDependencies) -> io::Result<Self> {
        let shutdown = Arc::new(WakeSignal::default());
        let worker_shutdown = Arc::clone(&shutdown);
        let thread = thread::Builder::new()
            .name("hopash-observer".to_owned())
            .spawn(move || observer_loop(dependencies, worker_shutdown))?;
        Ok(Self {
            shutdown,
            thread: Some(thread),
        })
    }

    fn shutdown(&mut self) -> io::Result<()> {
        self.shutdown.request();
        self.thread.take().map_or(Ok(()), |thread| {
            thread
                .join()
                .map_err(|_| io::Error::other("the Supervisor observer panicked"))
        })
    }
}

impl Drop for SupervisorObserver {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}

fn observer_loop(dependencies: ObserverDependencies, shutdown: Arc<WakeSignal>) {
    let mut status_sequence = 0_u64;
    let mut previous_status = None;
    let mut supervisor_log_sequence = None;
    let mut broker_logs_seeded = false;
    let mut service_log_sequence = None;
    let mut service_dropped_before = 0_u64;
    let mut pending_service_drops = 0_u64;

    while !shutdown.is_requested() {
        if let Ok(batch) = dependencies.core_runtime.logs(
            &dependencies.owner,
            service_log_sequence,
            OBSERVER_LOG_BATCH,
        ) {
            let current_core = dependencies
                .core_runtime
                .status(&dependencies.owner)
                .ok()
                .and_then(|status| status.managed_core);
            if !batch.records.is_empty() {
                let _ = dependencies
                    .supervisor
                    .execute(ApplicationOperation::GetStatus);
            }
            pending_service_drops = pending_service_drops.saturating_add(dropped_sequence_delta(
                &mut service_dropped_before,
                batch.dropped_before,
            ));
            if pending_service_drops > 0
                && let Some(core) = &current_core
                && dependencies
                    .supervisor
                    .record_core_log_drop(core.instance_generation, pending_service_drops)
                    .is_ok_and(|accepted| accepted)
            {
                pending_service_drops = 0;
                let _ = dependencies.supervisor.publish_core_log(
                    core.instance_generation,
                    SystemClock.now_unix_ms(),
                    LogLevel::Warn,
                    LogSource::Stderr,
                    "Privileged Core output was dropped before forwarding",
                );
            }
            let mut consumed_batch = true;
            for record in batch.records {
                if current_core
                    .as_ref()
                    .is_none_or(|core| core.instance_generation != record.instance_generation)
                {
                    service_log_sequence = Some(record.sequence);
                    continue;
                }
                let level = match record.source {
                    ProcessOutputSource::Stdout => LogLevel::Info,
                    ProcessOutputSource::Stderr => LogLevel::Warn,
                };
                let source = match record.source {
                    ProcessOutputSource::Stdout => LogSource::Stdout,
                    ProcessOutputSource::Stderr => LogSource::Stderr,
                };
                match dependencies.supervisor.publish_core_log(
                    record.instance_generation,
                    record.timestamp_unix_ms,
                    level,
                    source,
                    record.message,
                ) {
                    Ok(true) => service_log_sequence = Some(record.sequence),
                    Ok(false) | Err(_) => {
                        consumed_batch = false;
                        break;
                    }
                }
            }
            if consumed_batch {
                service_log_sequence = batch.next_sequence.or(service_log_sequence);
            }
        }

        if let Ok(tail) = dependencies
            .supervisor
            .core_log_tail(supervisor_log_sequence)
        {
            if !broker_logs_seeded || tail.gap {
                let latest_sequence = tail.latest_sequence;
                if dependencies.broker.synchronize_log_tail(tail).is_ok() {
                    broker_logs_seeded = true;
                    supervisor_log_sequence = latest_sequence.or(supervisor_log_sequence);
                }
            } else {
                let latest_sequence = tail.latest_sequence;
                let mut synchronized = true;
                for record in tail.records {
                    let sequence = record.sequence();
                    if dependencies.broker.publish_log(record).is_ok() {
                        supervisor_log_sequence = Some(sequence);
                    } else {
                        synchronized = false;
                        broker_logs_seeded = false;
                        break;
                    }
                }
                if synchronized {
                    supervisor_log_sequence = latest_sequence.or(supervisor_log_sequence);
                }
            }
        }

        if let Ok(status) = dependencies
            .supervisor
            .execute(ApplicationOperation::GetStatus)
            .and_then(expect_status)
        {
            let core_status = dependencies.core_runtime.status(&dependencies.owner).ok();
            let _ = update_instance_record(&dependencies.ownership, &status, core_status);
            if previous_status.as_ref() != Some(&status)
                && let Some(next) = status_sequence.checked_add(1)
                && dependencies
                    .broker
                    .publish_status(next, SystemClock.now_unix_ms(), status.clone())
                    .is_ok()
            {
                status_sequence = next;
                previous_status = Some(status);
            }
        }
        shutdown.wait(STATUS_SAMPLE_INTERVAL);
    }
}

fn dropped_sequence_delta(previous: &mut u64, current: u64) -> u64 {
    let delta = current.saturating_sub(*previous);
    *previous = current;
    delta
}

fn update_instance_record(
    ownership: &Mutex<Option<SupervisorOwnership>>,
    status: &crate::domain::StatusSnapshot,
    core_status: Option<crate::core::CoreRuntimeStatus>,
) -> Result<(), ()> {
    let active_profile_id = status
        .active_profile
        .as_ref()
        .map(|profile| profile.id.to_string());
    let managed_core = core_status
        .and_then(|status| status.managed_core)
        .map(|core| ManagedCoreIdentityRecord {
            pid: core.pid,
            process_start_identity: core.process_start_identity,
            control_endpoint: core.endpoint.socket_path,
            runtime_generation: core.runtime_generation.0,
            core_instance_generation: core.instance_generation.0,
        });
    let mut ownership = ownership.lock().map_err(|_| ())?;
    let ownership = ownership.as_mut().ok_or(())?;
    if ownership.record().active_profile_id == active_profile_id
        && ownership.record().managed_core == managed_core
    {
        return Ok(());
    }
    ownership
        .update_record(|record| {
            record.active_profile_id = active_profile_id;
            record.managed_core = managed_core;
        })
        .map_err(|_| ())
}

fn release_ownership(ownership: &Mutex<Option<SupervisorOwnership>>) -> io::Result<()> {
    let ownership = ownership
        .lock()
        .map_err(|_| io::Error::other("the Supervisor ownership state is unavailable"))?
        .take();
    ownership.map_or(Ok(()), |ownership| {
        ownership
            .release()
            .map_err(|_| io::Error::other("the Supervisor ownership state could not release"))
    })
}

fn map_core_session_startup(error: crate::core::CoreRuntimeError) -> StartupError {
    let (category, message) = match error.kind {
        crate::core::CoreRuntimeErrorKind::TunPermissionDenied => (
            StartupFailureCategory::Permission,
            "TUN capability is unavailable",
        ),
        crate::core::CoreRuntimeErrorKind::ProtocolMismatch => (
            StartupFailureCategory::Configuration,
            "The Core service protocol is incompatible",
        ),
        crate::core::CoreRuntimeErrorKind::Authentication => (
            StartupFailureCategory::Permission,
            "The Core service rejected the Supervisor identity",
        ),
        _ => (
            StartupFailureCategory::Readiness,
            "The privileged Core service is unavailable",
        ),
    };
    StartupError::new(StartupStage::CoreReadiness, category, message)
}

fn startup_internal(message: &'static str) -> StartupError {
    StartupError::new(
        StartupStage::SupervisorInitialization,
        StartupFailureCategory::Internal,
        message,
    )
}

fn readiness_failure_from_daemon(error: &DaemonError) -> ReadinessFailure {
    ReadinessFailure {
        stage: error.stage().unwrap_or(StartupStage::SingletonOwnership),
        category: error.category().unwrap_or(StartupFailureCategory::Internal),
        message: error.to_string(),
    }
}

fn bundled_mihomo_path() -> io::Result<PathBuf> {
    let path = std::env::var_os("HOPASH_MIHOMO_PATH")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(BUNDLED_MIHOMO_PATH));
    if path.is_absolute() {
        Ok(path)
    } else {
        Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "the Mihomo executable path must be absolute",
        ))
    }
}

fn core_service_client() -> io::Result<CoreServiceClient> {
    #[cfg(debug_assertions)]
    if let Some(socket_path) = std::env::var_os(DEBUG_CORE_SERVICE_SOCKET_ENV) {
        let socket_path = PathBuf::from(socket_path);
        if !socket_path.is_absolute() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "the Core service socket path must be absolute",
            ));
        }
        return Ok(CoreServiceClient::for_service_uid(
            socket_path,
            nix::unistd::Uid::effective().as_raw(),
        ));
    }
    Ok(CoreServiceClient::new(CORE_SERVICE_SOCKET_PATH))
}

fn verified_binary_sha256(path: &Path) -> io::Result<String> {
    if !path.is_absolute() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "the Mihomo path must be absolute",
        ));
    }
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink()
        || !metadata.is_file()
        || metadata.permissions().mode() & 0o111 == 0
        || metadata.len() > MIHOMO_BINARY_MAX_BYTES as u64
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "the Mihomo executable is invalid",
        ));
    }
    let file = fs::File::open(path)?;
    let opened = file.metadata()?;
    if opened.dev() != metadata.dev() || opened.ino() != metadata.ino() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "the Mihomo executable changed while opening",
        ));
    }
    let mut content = Vec::with_capacity(metadata.len() as usize);
    file.take((MIHOMO_BINARY_MAX_BYTES as u64).saturating_add(1))
        .read_to_end(&mut content)?;
    if content.len() > MIHOMO_BINARY_MAX_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "the Mihomo executable is too large",
        ));
    }
    Ok(crate::digest::sha256_hex(&content))
}

fn invalid_product_configuration(_error: impl fmt::Display) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        "the bundled product configuration is invalid",
    )
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
            .name("hopash-log-signals".to_owned())
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    #[test]
    fn forwarded_log_drop_sequence_reports_only_new_loss() {
        let mut previous = 0;

        assert_eq!(dropped_sequence_delta(&mut previous, 7), 7);
        assert_eq!(dropped_sequence_delta(&mut previous, 7), 0);
        assert_eq!(dropped_sequence_delta(&mut previous, 11), 4);
        assert_eq!(dropped_sequence_delta(&mut previous, 3), 0);
        assert_eq!(previous, 3);
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
            waiting_drain.wait_for_mutations();
            finished_sender
                .send(())
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
}
