use std::ffi::{OsStr, OsString};
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::os::unix::fs::{FileTypeExt, OpenOptionsExt, PermissionsExt};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use crate::constants::{DAEMON_POLL_INTERVAL, DAEMON_SHUTDOWN_TIMEOUT, DAEMON_STARTUP_TIMEOUT};
use crate::ipc::IPC_PROTOCOL_VERSION;
use crate::lifecycle::{
    DirectoryLease, InstanceRecord, LeaseAcquisition, LeaseOwner, LifecycleError, ProcessIdentity,
    ProcessInspector, PsProcessInspector, StatePaths, remove_verified_stale_socket,
};

pub const INTERNAL_SUPERVISOR_MODE: &str = "__supervisor";
pub const READINESS_TOKEN_ENV: &str = "HOPASH_SUPERVISOR_READINESS_TOKEN";

const LIFECYCLE_LEASE_NAME: &str = "lifecycle-operation";
const SUPERVISOR_LEASE_NAME: &str = "supervisor";
const READINESS_SCHEMA_VERSION: u16 = 1;
const READINESS_MAX_BYTES: usize = 64 * 1024;
const READINESS_FILE_PREFIX: &str = ".supervisor-readiness-";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DaemonAction {
    Start,
    Stop,
    Restart,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StartupStage {
    StatePreparation,
    SingletonOwnership,
    StaleCleanup,
    ProcessSpawn,
    ProcessIdentity,
    SupervisorInitialization,
    CoreReadiness,
    Readiness,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StartupFailureCategory {
    Permission,
    Configuration,
    Process,
    Readiness,
    Internal,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DaemonErrorKind {
    LifecycleOperationBusy,
    SupervisorOwnershipBusy,
    InvalidLiveInstance,
    UnsafeStaleState,
    InvalidInternalInvocation,
    SpawnFailed,
    StartupTimedOut,
    StartupProcessExited,
    StartupRejected,
    InvalidReadiness,
    ShutdownRequestFailed,
    InvalidShutdownAcknowledgement,
    ShutdownTimedOut,
    ProcessControlFailed,
    StateOperationFailed,
}

pub struct DaemonError {
    kind: DaemonErrorKind,
    stage: Option<StartupStage>,
    category: Option<StartupFailureCategory>,
    detail: Option<String>,
    source: Option<Box<dyn std::error::Error + Send + Sync>>,
}

impl DaemonError {
    #[must_use]
    pub const fn kind(&self) -> DaemonErrorKind {
        self.kind
    }

    #[must_use]
    pub const fn stage(&self) -> Option<StartupStage> {
        self.stage
    }

    #[must_use]
    pub const fn category(&self) -> Option<StartupFailureCategory> {
        self.category
    }

    #[must_use]
    pub fn detail(&self) -> Option<&str> {
        self.detail.as_deref()
    }

    fn new(kind: DaemonErrorKind) -> Self {
        Self {
            kind,
            stage: None,
            category: None,
            detail: None,
            source: None,
        }
    }

    fn startup(kind: DaemonErrorKind, stage: StartupStage) -> Self {
        Self {
            kind,
            stage: Some(stage),
            category: None,
            detail: None,
            source: None,
        }
    }

    fn rejected(failure: ReadinessFailure) -> Self {
        Self {
            kind: DaemonErrorKind::StartupRejected,
            stage: Some(failure.stage),
            category: Some(failure.category),
            detail: Some(failure.message),
            source: None,
        }
    }

    fn with_source(mut self, source: impl std::error::Error + Send + Sync + 'static) -> Self {
        self.source = Some(Box::new(source));
        self
    }
}

impl fmt::Debug for DaemonError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DaemonError")
            .field("kind", &self.kind)
            .field("stage", &self.stage)
            .field("category", &self.category)
            .field("detail", &self.detail)
            .finish()
    }
}

impl fmt::Display for DaemonError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self.kind {
            DaemonErrorKind::LifecycleOperationBusy => {
                "another Supervisor lifecycle operation is running"
            }
            DaemonErrorKind::SupervisorOwnershipBusy => {
                "another Supervisor owns the singleton lease"
            }
            DaemonErrorKind::InvalidLiveInstance => {
                "the live Supervisor instance record is inconsistent"
            }
            DaemonErrorKind::UnsafeStaleState => {
                "the stale Supervisor state cannot be cleaned safely"
            }
            DaemonErrorKind::InvalidInternalInvocation => {
                "the internal Supervisor invocation is invalid"
            }
            DaemonErrorKind::SpawnFailed => "the Supervisor process could not be started",
            DaemonErrorKind::StartupTimedOut => "the Supervisor startup timed out",
            DaemonErrorKind::StartupProcessExited => {
                "the Supervisor exited before reporting readiness"
            }
            DaemonErrorKind::StartupRejected => "the Supervisor reported a startup failure",
            DaemonErrorKind::InvalidReadiness => {
                "the Supervisor readiness acknowledgement is invalid"
            }
            DaemonErrorKind::ShutdownRequestFailed => "the Supervisor shutdown request failed",
            DaemonErrorKind::InvalidShutdownAcknowledgement => {
                "the Supervisor shutdown acknowledgement is invalid"
            }
            DaemonErrorKind::ShutdownTimedOut => "the Supervisor shutdown timed out",
            DaemonErrorKind::ProcessControlFailed => "the Supervisor process control failed",
            DaemonErrorKind::StateOperationFailed => "the Supervisor lifecycle state failed",
        })?;
        if let Some(detail) = &self.detail {
            write!(formatter, ": {detail}")?;
        }
        Ok(())
    }
}

impl std::error::Error for DaemonError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source
            .as_deref()
            .map(|source| source as &(dyn std::error::Error + 'static))
    }
}

#[derive(Clone, Copy, Debug)]
pub struct DaemonTimeouts {
    pub startup: Duration,
    pub shutdown: Duration,
    pub poll_interval: Duration,
}

impl Default for DaemonTimeouts {
    fn default() -> Self {
        Self {
            startup: DAEMON_STARTUP_TIMEOUT,
            shutdown: DAEMON_SHUTDOWN_TIMEOUT,
            poll_interval: DAEMON_POLL_INTERVAL,
        }
    }
}

impl DaemonTimeouts {
    fn validated(self) -> Self {
        Self {
            startup: self.startup.max(Duration::from_millis(1)),
            shutdown: self.shutdown.max(Duration::from_millis(1)),
            poll_interval: self.poll_interval.max(Duration::from_millis(1)),
        }
    }
}

pub trait DaemonClock: Send + Sync {
    fn monotonic_now(&self) -> Duration;
    fn sleep(&self, duration: Duration);
}

#[derive(Clone, Copy, Debug, Default)]
pub struct SystemDaemonClock;

impl DaemonClock for SystemDaemonClock {
    fn monotonic_now(&self) -> Duration {
        static EPOCH: OnceLock<Instant> = OnceLock::new();
        EPOCH.get_or_init(Instant::now).elapsed()
    }

    fn sleep(&self, duration: Duration) {
        std::thread::sleep(duration);
    }
}

pub trait DaemonProcessControl: ProcessInspector + Send + Sync {
    fn executable(&self) -> io::Result<PathBuf>;
    fn spawn_detached(&self, launch: &DetachedSupervisorLaunch) -> io::Result<u32>;
    fn terminate_exact(&self, process: &ProcessIdentity) -> io::Result<bool>;
}

#[derive(Clone, Debug)]
pub struct StdDaemonProcessControl {
    executable: PathBuf,
    inspector: PsProcessInspector,
    children: Arc<Mutex<Vec<Child>>>,
}

impl StdDaemonProcessControl {
    pub fn current() -> io::Result<Self> {
        Ok(Self {
            executable: std::env::current_exe()?,
            inspector: PsProcessInspector,
            children: Arc::new(Mutex::new(Vec::new())),
        })
    }

    pub fn for_executable(executable: PathBuf) -> io::Result<Self> {
        if !executable.is_absolute() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "the Supervisor executable path must be absolute",
            ));
        }
        Ok(Self {
            executable,
            inspector: PsProcessInspector,
            children: Arc::new(Mutex::new(Vec::new())),
        })
    }
}

impl ProcessInspector for StdDaemonProcessControl {
    fn identity(&self, pid: u32) -> io::Result<Option<String>> {
        let mut children = self
            .children
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(index) = children.iter().position(|child| child.id() == pid) {
            let mut child = children.swap_remove(index);
            match child.try_wait() {
                Ok(Some(_)) => {
                    child.wait()?;
                    return Ok(None);
                }
                Ok(None) => children.push(child),
                Err(error) => {
                    children.push(child);
                    return Err(error);
                }
            }
        }
        drop(children);
        self.inspector.identity(pid)
    }
}

impl DaemonProcessControl for StdDaemonProcessControl {
    fn executable(&self) -> io::Result<PathBuf> {
        Ok(self.executable.clone())
    }

    fn spawn_detached(&self, launch: &DetachedSupervisorLaunch) -> io::Result<u32> {
        if launch.executable != self.executable {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "the launch executable does not match the configured executable",
            ));
        }
        let child = Command::new(&launch.executable)
            .args(launch.arguments())
            .env(READINESS_TOKEN_ENV, launch.readiness.token())
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .process_group(0)
            .spawn()?;
        Ok(retain_launched_child(&self.children, child))
    }

    fn terminate_exact(&self, process: &ProcessIdentity) -> io::Result<bool> {
        if self.identity(process.pid)?.as_deref() != Some(process.start_identity.as_str()) {
            return Ok(false);
        }
        let mut children = self
            .children
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(child) = children.iter_mut().find(|child| child.id() == process.pid) else {
            return Ok(false);
        };
        match child.kill() {
            Ok(()) => Ok(true),
            Err(_) if child.try_wait()?.is_some() => Ok(false),
            Err(error) => Err(error),
        }
    }
}

#[derive(Clone)]
pub struct ReadinessChannel {
    path: PathBuf,
    token: String,
}

impl ReadinessChannel {
    pub fn create(state_root: &Path) -> Result<Self, DaemonError> {
        if !state_root.is_absolute() {
            return Err(DaemonError::new(DaemonErrorKind::InvalidInternalInvocation));
        }
        for _ in 0..4 {
            let id = uuid::Uuid::new_v4();
            let path = state_root.join(format!("{READINESS_FILE_PREFIX}{id}.json"));
            match fs::symlink_metadata(&path) {
                Err(error) if error.kind() == io::ErrorKind::NotFound => {
                    return Ok(Self {
                        path,
                        token: uuid::Uuid::new_v4().to_string(),
                    });
                }
                Ok(_) => {}
                Err(error) => {
                    return Err(
                        DaemonError::new(DaemonErrorKind::StateOperationFailed).with_source(error)
                    );
                }
            }
        }
        Err(DaemonError::new(DaemonErrorKind::StateOperationFailed))
    }

    pub fn from_internal_parts(
        state_root: &Path,
        path: PathBuf,
        token: String,
    ) -> Result<Self, DaemonError> {
        let valid_name = path
            .file_name()
            .and_then(OsStr::to_str)
            .is_some_and(|name| name.starts_with(READINESS_FILE_PREFIX) && name.ends_with(".json"));
        if !state_root.is_absolute()
            || !path.is_absolute()
            || path.parent() != Some(state_root)
            || !valid_name
            || token.is_empty()
            || token.len() > 128
            || !token.bytes().all(|byte| byte.is_ascii_graphic())
            || uuid::Uuid::parse_str(&token).is_err()
        {
            return Err(DaemonError::new(DaemonErrorKind::InvalidInternalInvocation));
        }
        Ok(Self { path, token })
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    fn token(&self) -> &str {
        &self.token
    }

    pub fn publish_ready(
        &self,
        process: ProcessIdentity,
        instance_token: String,
    ) -> Result<(), DaemonError> {
        self.publish(ReadinessOutcome::Ready { instance_token }, process)
    }

    pub fn publish_failure(
        &self,
        process: ProcessIdentity,
        failure: ReadinessFailure,
    ) -> Result<(), DaemonError> {
        self.publish(ReadinessOutcome::Failed(failure), process)
    }

    fn publish(
        &self,
        outcome: ReadinessOutcome,
        process: ProcessIdentity,
    ) -> Result<(), DaemonError> {
        validate_readiness_payload(&process, &outcome)?;
        let envelope = ReadinessEnvelope {
            schema_version: READINESS_SCHEMA_VERSION,
            token: self.token.clone(),
            process,
            outcome,
        };
        let content = serde_json::to_vec(&envelope).map_err(|error| {
            DaemonError::new(DaemonErrorKind::InvalidReadiness).with_source(error)
        })?;
        if content.len() > READINESS_MAX_BYTES {
            return Err(DaemonError::new(DaemonErrorKind::InvalidReadiness));
        }
        write_private_once(&self.path, &content).map_err(|error| {
            DaemonError::new(DaemonErrorKind::StateOperationFailed).with_source(error)
        })
    }

    fn try_receive(&self) -> Result<Option<ReadinessEnvelope>, DaemonError> {
        let metadata = match fs::symlink_metadata(&self.path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => {
                return Err(
                    DaemonError::new(DaemonErrorKind::StateOperationFailed).with_source(error)
                );
            }
        };
        if !metadata.is_file()
            || metadata.file_type().is_symlink()
            || metadata.permissions().mode() & 0o077 != 0
        {
            return Err(DaemonError::new(DaemonErrorKind::InvalidReadiness));
        }
        let content = read_limited(&self.path, READINESS_MAX_BYTES).map_err(|error| {
            DaemonError::new(DaemonErrorKind::InvalidReadiness).with_source(error)
        })?;
        let envelope: ReadinessEnvelope = serde_json::from_slice(&content).map_err(|error| {
            DaemonError::new(DaemonErrorKind::InvalidReadiness).with_source(error)
        })?;
        if envelope.schema_version != READINESS_SCHEMA_VERSION || envelope.token != self.token {
            return Err(DaemonError::new(DaemonErrorKind::InvalidReadiness));
        }
        validate_readiness_payload(&envelope.process, &envelope.outcome)?;
        remove_regular_file(&self.path)?;
        Ok(Some(envelope))
    }

    fn cleanup(&self) -> Result<(), DaemonError> {
        match fs::symlink_metadata(&self.path) {
            Ok(metadata) if metadata.is_file() && !metadata.file_type().is_symlink() => {
                let content = read_limited(&self.path, READINESS_MAX_BYTES).map_err(|error| {
                    DaemonError::new(DaemonErrorKind::InvalidReadiness).with_source(error)
                })?;
                let envelope: ReadinessEnvelope =
                    serde_json::from_slice(&content).map_err(|error| {
                        DaemonError::new(DaemonErrorKind::InvalidReadiness).with_source(error)
                    })?;
                if envelope.schema_version != READINESS_SCHEMA_VERSION
                    || envelope.token != self.token
                {
                    return Err(DaemonError::new(DaemonErrorKind::InvalidReadiness));
                }
                fs::remove_file(&self.path).map_err(|error| {
                    DaemonError::new(DaemonErrorKind::StateOperationFailed).with_source(error)
                })
            }
            Ok(_) => Err(DaemonError::new(DaemonErrorKind::InvalidReadiness)),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(error) => {
                Err(DaemonError::new(DaemonErrorKind::StateOperationFailed).with_source(error))
            }
        }
    }
}

impl fmt::Debug for ReadinessChannel {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ReadinessChannel")
            .field("path", &self.path)
            .field("token", &"[REDACTED]")
            .finish()
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReadinessFailure {
    pub stage: StartupStage,
    pub category: StartupFailureCategory,
    pub message: String,
}

#[derive(Clone, Debug)]
pub struct DetachedSupervisorLaunch {
    executable: PathBuf,
    state_root: PathBuf,
    readiness: ReadinessChannel,
}

impl DetachedSupervisorLaunch {
    #[must_use]
    pub fn executable(&self) -> &Path {
        &self.executable
    }

    #[must_use]
    pub fn state_root(&self) -> &Path {
        &self.state_root
    }

    #[must_use]
    pub fn readiness(&self) -> &ReadinessChannel {
        &self.readiness
    }

    #[must_use]
    pub fn arguments(&self) -> Vec<OsString> {
        vec![
            OsString::from(INTERNAL_SUPERVISOR_MODE),
            OsString::from("--state-root"),
            self.state_root.as_os_str().to_owned(),
            OsString::from("--readiness-path"),
            self.readiness.path.as_os_str().to_owned(),
        ]
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InternalSupervisorInvocation {
    pub state_root: PathBuf,
    pub readiness_path: PathBuf,
}

impl InternalSupervisorInvocation {
    pub fn parse_process_arguments(args: &[OsString]) -> Result<Option<Self>, DaemonError> {
        if args.get(1).and_then(|value| value.to_str()) != Some(INTERNAL_SUPERVISOR_MODE) {
            return Ok(None);
        }
        if args.len() != 6
            || args.get(2).and_then(|value| value.to_str()) != Some("--state-root")
            || args.get(4).and_then(|value| value.to_str()) != Some("--readiness-path")
        {
            return Err(DaemonError::new(DaemonErrorKind::InvalidInternalInvocation));
        }
        let state_root = PathBuf::from(&args[3]);
        let readiness_path = PathBuf::from(&args[5]);
        let token = std::env::var(READINESS_TOKEN_ENV)
            .map_err(|_| DaemonError::new(DaemonErrorKind::InvalidInternalInvocation))?;
        ReadinessChannel::from_internal_parts(&state_root, readiness_path.clone(), token)?;
        Ok(Some(Self {
            state_root,
            readiness_path,
        }))
    }

    pub fn readiness_channel(&self) -> Result<ReadinessChannel, DaemonError> {
        let token = std::env::var(READINESS_TOKEN_ENV)
            .map_err(|_| DaemonError::new(DaemonErrorKind::InvalidInternalInvocation))?;
        ReadinessChannel::from_internal_parts(&self.state_root, self.readiness_path.clone(), token)
    }
}

#[derive(Debug)]
pub struct SupervisorOwnership {
    paths: StatePaths,
    lease: Option<DirectoryLease>,
    record: InstanceRecord,
}

impl SupervisorOwnership {
    pub fn acquire(
        paths: StatePaths,
        process: ProcessIdentity,
        started_at_unix_ms: u64,
        inspector: &dyn ProcessInspector,
    ) -> Result<Self, DaemonError> {
        paths.prepare().map_err(state_error)?;
        let lease =
            match DirectoryLease::acquire(&paths.root, SUPERVISOR_LEASE_NAME, process, inspector)
                .map_err(state_error)?
            {
                LeaseAcquisition::Acquired(lease) => lease,
                LeaseAcquisition::HeldByLiveProcess(_) => {
                    return Err(DaemonError::startup(
                        DaemonErrorKind::SupervisorOwnershipBusy,
                        StartupStage::SingletonOwnership,
                    ));
                }
            };
        let record =
            InstanceRecord::new(lease.owner(), started_at_unix_ms, paths.ipc_socket.clone());
        record
            .write_private(&paths.instance_record)
            .map_err(state_error)?;
        Ok(Self {
            paths,
            lease: Some(lease),
            record,
        })
    }

    #[must_use]
    pub fn record(&self) -> &InstanceRecord {
        &self.record
    }

    pub fn update_record(
        &mut self,
        update: impl FnOnce(&mut InstanceRecord),
    ) -> Result<(), DaemonError> {
        let mut candidate = self.record.clone();
        update(&mut candidate);
        candidate
            .write_private(&self.paths.instance_record)
            .map_err(state_error)?;
        self.record = candidate;
        Ok(())
    }

    pub fn publish_ready(&self, readiness: &ReadinessChannel) -> Result<(), DaemonError> {
        readiness.publish_ready(
            self.record.supervisor.clone(),
            self.record.instance_token().to_owned(),
        )
    }

    pub fn release(mut self) -> Result<(), DaemonError> {
        remove_instance_artifacts(&self.paths, &self.record)?;
        let Some(lease) = self.lease.take() else {
            return Err(DaemonError::new(DaemonErrorKind::UnsafeStaleState));
        };
        lease.release().map_err(state_error)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ShutdownIntent {
    pub process: ProcessIdentity,
    pub instance_token: String,
    pub protocol_version: u16,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ShutdownAcknowledgement {
    pub process: ProcessIdentity,
    pub instance_token: String,
}

pub trait ShutdownPort: Send + Sync {
    fn request_shutdown(
        &self,
        intent: &ShutdownIntent,
        timeout: Duration,
    ) -> io::Result<ShutdownAcknowledgement>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DaemonLifecycleOutcome {
    pub action: DaemonAction,
    pub changed: bool,
    pub instance: Option<InstanceRecord>,
}

pub struct DaemonLifecycle<P, S, C>
where
    P: DaemonProcessControl,
    S: ShutdownPort,
    C: DaemonClock,
{
    paths: StatePaths,
    process: Arc<P>,
    shutdown: Arc<S>,
    clock: Arc<C>,
    timeouts: DaemonTimeouts,
}

impl<P, S, C> DaemonLifecycle<P, S, C>
where
    P: DaemonProcessControl,
    S: ShutdownPort,
    C: DaemonClock,
{
    #[must_use]
    pub fn new(
        paths: StatePaths,
        process: Arc<P>,
        shutdown: Arc<S>,
        clock: Arc<C>,
        timeouts: DaemonTimeouts,
    ) -> Self {
        Self {
            paths,
            process,
            shutdown,
            clock,
            timeouts: timeouts.validated(),
        }
    }

    pub fn start(&self) -> Result<DaemonLifecycleOutcome, DaemonError> {
        self.with_operation_lease(|| self.start_locked(DaemonAction::Start))
    }

    pub fn stop(&self) -> Result<DaemonLifecycleOutcome, DaemonError> {
        self.with_operation_lease(|| self.stop_locked(DaemonAction::Stop))
    }

    pub fn restart(&self) -> Result<DaemonLifecycleOutcome, DaemonError> {
        self.with_operation_lease(|| {
            let stopped = self.stop_locked(DaemonAction::Restart)?;
            let mut started = self.start_locked(DaemonAction::Restart)?;
            started.changed |= stopped.changed;
            Ok(started)
        })
    }

    fn with_operation_lease<T>(
        &self,
        operation: impl FnOnce() -> Result<T, DaemonError>,
    ) -> Result<T, DaemonError> {
        let lease = self.acquire_operation_lease()?;
        let result = operation();
        let release = lease.release().map_err(state_error);
        match (result, release) {
            (Ok(value), Ok(())) => Ok(value),
            (Ok(_), Err(error)) | (Err(error), _) => Err(error),
        }
    }

    fn acquire_operation_lease(&self) -> Result<DirectoryLease, DaemonError> {
        self.paths.prepare().map_err(state_error)?;
        let identity = self.current_process_identity()?;
        match DirectoryLease::acquire(
            &self.paths.root,
            LIFECYCLE_LEASE_NAME,
            identity,
            self.process.as_ref(),
        )
        .map_err(state_error)?
        {
            LeaseAcquisition::Acquired(lease) => Ok(lease),
            LeaseAcquisition::HeldByLiveProcess(_) => {
                Err(DaemonError::new(DaemonErrorKind::LifecycleOperationBusy))
            }
        }
    }

    fn current_process_identity(&self) -> Result<ProcessIdentity, DaemonError> {
        let pid = std::process::id();
        let start_identity = self
            .process
            .identity(pid)
            .map_err(|error| {
                DaemonError::new(DaemonErrorKind::ProcessControlFailed).with_source(error)
            })?
            .ok_or_else(|| DaemonError::new(DaemonErrorKind::ProcessControlFailed))?;
        Ok(ProcessIdentity {
            pid,
            start_identity,
        })
    }

    fn start_locked(&self, action: DaemonAction) -> Result<DaemonLifecycleOutcome, DaemonError> {
        match self.probe_supervisor()? {
            SupervisorProbe::Live(record) => Ok(DaemonLifecycleOutcome {
                action,
                changed: false,
                instance: Some(record),
            }),
            SupervisorProbe::Vacant { lease, stale_owner } => {
                self.cleanup_stale_state(stale_owner.as_ref())?;
                lease.release().map_err(state_error)?;
                self.launch_and_wait(action)
            }
        }
    }

    fn stop_locked(&self, action: DaemonAction) -> Result<DaemonLifecycleOutcome, DaemonError> {
        let record = match self.probe_supervisor()? {
            SupervisorProbe::Live(record) => record,
            SupervisorProbe::Vacant { lease, stale_owner } => {
                self.cleanup_stale_state(stale_owner.as_ref())?;
                lease.release().map_err(state_error)?;
                return Ok(DaemonLifecycleOutcome {
                    action,
                    changed: false,
                    instance: None,
                });
            }
        };
        let intent = ShutdownIntent {
            process: record.supervisor.clone(),
            instance_token: record.instance_token().to_owned(),
            protocol_version: IPC_PROTOCOL_VERSION,
        };
        let acknowledgement = match self
            .shutdown
            .request_shutdown(&intent, self.timeouts.shutdown)
        {
            Ok(acknowledgement) => acknowledgement,
            Err(error) => {
                if !self.process_matches(&record.supervisor)? {
                    self.cleanup_after_exit()?;
                    return Ok(DaemonLifecycleOutcome {
                        action,
                        changed: true,
                        instance: None,
                    });
                }
                return Err(
                    DaemonError::new(DaemonErrorKind::ShutdownRequestFailed).with_source(error)
                );
            }
        };
        if acknowledgement.process != record.supervisor
            || acknowledgement.instance_token != record.instance_token()
        {
            return Err(DaemonError::new(
                DaemonErrorKind::InvalidShutdownAcknowledgement,
            ));
        }
        if !self.wait_for_exit(&record.supervisor, self.timeouts.shutdown)? {
            return Err(DaemonError::new(DaemonErrorKind::ShutdownTimedOut));
        }
        self.cleanup_after_exit()?;
        Ok(DaemonLifecycleOutcome {
            action,
            changed: true,
            instance: None,
        })
    }

    fn launch_and_wait(&self, action: DaemonAction) -> Result<DaemonLifecycleOutcome, DaemonError> {
        let readiness = ReadinessChannel::create(&self.paths.root)?;
        let executable = self.process.executable().map_err(|error| {
            DaemonError::startup(DaemonErrorKind::SpawnFailed, StartupStage::ProcessSpawn)
                .with_source(error)
        })?;
        let launch = DetachedSupervisorLaunch {
            executable,
            state_root: self.paths.root.clone(),
            readiness: readiness.clone(),
        };
        let pid = self.process.spawn_detached(&launch).map_err(|error| {
            DaemonError::startup(DaemonErrorKind::SpawnFailed, StartupStage::ProcessSpawn)
                .with_source(error)
        })?;
        let deadline = deadline_after(self.clock.monotonic_now(), self.timeouts.startup);
        let initial = match self.wait_for_initial_startup(pid, &readiness, deadline) {
            Ok(initial) => initial,
            Err(error) => {
                let _ = readiness.cleanup();
                return Err(error);
            }
        };
        let (process, envelope) = match initial {
            InitialStartup::Process(process) => {
                let envelope = match self.wait_for_readiness(&readiness, &process, deadline) {
                    Ok(envelope) => envelope,
                    Err(error) => {
                        self.abort_failed_start(&process, &readiness)?;
                        return Err(error);
                    }
                };
                (process, envelope)
            }
            InitialStartup::Readiness(envelope) => {
                let reported = envelope.process.clone();
                let actual = self.process.identity(pid).map_err(|error| {
                    DaemonError::startup(
                        DaemonErrorKind::ProcessControlFailed,
                        StartupStage::ProcessIdentity,
                    )
                    .with_source(error)
                })?;
                if reported.pid != pid
                    || actual
                        .as_deref()
                        .is_some_and(|identity| identity != reported.start_identity.as_str())
                {
                    if let Some(start_identity) = actual {
                        self.abort_failed_start(
                            &ProcessIdentity {
                                pid,
                                start_identity,
                            },
                            &readiness,
                        )?;
                    }
                    return Err(DaemonError::new(DaemonErrorKind::InvalidReadiness));
                }
                if actual.is_none() && matches!(&envelope.outcome, ReadinessOutcome::Ready { .. }) {
                    self.cleanup_after_exit()?;
                    return Err(DaemonError::startup(
                        DaemonErrorKind::StartupProcessExited,
                        StartupStage::Readiness,
                    ));
                }
                (reported, envelope)
            }
        };
        match envelope.outcome {
            ReadinessOutcome::Failed(failure) => {
                self.abort_failed_start(&process, &readiness)?;
                Err(DaemonError::rejected(failure))
            }
            ReadinessOutcome::Ready { instance_token } => {
                let record = match self.load_live_instance() {
                    Ok(record) => record,
                    Err(error) => {
                        self.abort_failed_start(&process, &readiness)?;
                        return Err(error);
                    }
                };
                if record.supervisor != process || record.instance_token() != instance_token {
                    self.abort_failed_start(&process, &readiness)?;
                    return Err(DaemonError::new(DaemonErrorKind::InvalidReadiness));
                }
                Ok(DaemonLifecycleOutcome {
                    action,
                    changed: true,
                    instance: Some(record),
                })
            }
        }
    }

    fn wait_for_initial_startup(
        &self,
        pid: u32,
        readiness: &ReadinessChannel,
        deadline: Duration,
    ) -> Result<InitialStartup, DaemonError> {
        loop {
            if let Some(envelope) = readiness.try_receive()? {
                return Ok(InitialStartup::Readiness(envelope));
            }
            if let Some(start_identity) = self.process.identity(pid).map_err(|error| {
                DaemonError::startup(
                    DaemonErrorKind::ProcessControlFailed,
                    StartupStage::ProcessIdentity,
                )
                .with_source(error)
            })? {
                return Ok(InitialStartup::Process(ProcessIdentity {
                    pid,
                    start_identity,
                }));
            }
            if self.clock.monotonic_now() >= deadline {
                return Err(DaemonError::startup(
                    DaemonErrorKind::StartupProcessExited,
                    StartupStage::ProcessIdentity,
                ));
            }
            self.sleep_until_poll(deadline);
        }
    }

    fn wait_for_readiness(
        &self,
        readiness: &ReadinessChannel,
        process: &ProcessIdentity,
        deadline: Duration,
    ) -> Result<ReadinessEnvelope, DaemonError> {
        loop {
            if let Some(envelope) = readiness.try_receive()? {
                if envelope.process != *process {
                    return Err(DaemonError::new(DaemonErrorKind::InvalidReadiness));
                }
                return Ok(envelope);
            }
            if !self.process_matches(process)? {
                return Err(DaemonError::startup(
                    DaemonErrorKind::StartupProcessExited,
                    StartupStage::Readiness,
                ));
            }
            if self.clock.monotonic_now() >= deadline {
                return Err(DaemonError::startup(
                    DaemonErrorKind::StartupTimedOut,
                    StartupStage::Readiness,
                ));
            }
            self.sleep_until_poll(deadline);
        }
    }

    fn abort_failed_start(
        &self,
        process: &ProcessIdentity,
        readiness: &ReadinessChannel,
    ) -> Result<(), DaemonError> {
        let _ = readiness.cleanup();
        self.process.terminate_exact(process).map_err(|error| {
            DaemonError::new(DaemonErrorKind::ProcessControlFailed).with_source(error)
        })?;
        if !self.wait_for_exit(process, self.timeouts.shutdown)? {
            return Err(DaemonError::new(DaemonErrorKind::ProcessControlFailed));
        }
        self.cleanup_after_exit()
    }

    fn probe_supervisor(&self) -> Result<SupervisorProbe, DaemonError> {
        let identity = self.current_process_identity()?;
        for _ in 0..2 {
            let observed_owner = read_supervisor_lease_owner(&self.paths.root)?;
            match DirectoryLease::acquire(
                &self.paths.root,
                SUPERVISOR_LEASE_NAME,
                identity.clone(),
                self.process.as_ref(),
            )
            .map_err(state_error)?
            {
                LeaseAcquisition::Acquired(lease) => {
                    return Ok(SupervisorProbe::Vacant {
                        lease,
                        stale_owner: observed_owner,
                    });
                }
                LeaseAcquisition::HeldByLiveProcess(owner) => {
                    if self.owner_is_live(&owner)? {
                        return self.validate_live_record(&owner).map(SupervisorProbe::Live);
                    }
                }
            }
        }
        Err(DaemonError::new(DaemonErrorKind::InvalidLiveInstance))
    }

    fn load_live_instance(&self) -> Result<InstanceRecord, DaemonError> {
        match self.probe_supervisor()? {
            SupervisorProbe::Live(record) => Ok(record),
            SupervisorProbe::Vacant { lease, .. } => {
                lease.release().map_err(state_error)?;
                Err(DaemonError::new(DaemonErrorKind::InvalidReadiness))
            }
        }
    }

    fn owner_is_live(&self, owner: &LeaseOwner) -> Result<bool, DaemonError> {
        Ok(self
            .process
            .identity(owner.process.pid)
            .map_err(|error| {
                DaemonError::new(DaemonErrorKind::ProcessControlFailed).with_source(error)
            })?
            .as_deref()
            == Some(owner.process.start_identity.as_str()))
    }

    fn validate_live_record(&self, owner: &LeaseOwner) -> Result<InstanceRecord, DaemonError> {
        let record = read_instance_record_safe(
            &self.paths.instance_record,
            DaemonErrorKind::InvalidLiveInstance,
        )?
        .ok_or_else(|| DaemonError::new(DaemonErrorKind::InvalidLiveInstance))?;
        if record.supervisor != owner.process
            || record.instance_token() != owner.instance_token()
            || record.socket_path != self.paths.ipc_socket
            || record.protocol_version != IPC_PROTOCOL_VERSION
            || !self.process_matches(&record.supervisor)?
        {
            return Err(DaemonError::new(DaemonErrorKind::InvalidLiveInstance));
        }
        Ok(record)
    }

    fn cleanup_stale_state(&self, stale_owner: Option<&LeaseOwner>) -> Result<(), DaemonError> {
        let record = read_instance_record_safe(
            &self.paths.instance_record,
            DaemonErrorKind::UnsafeStaleState,
        )
        .map_err(with_stale_cleanup_stage)?;
        let Some(record) = record else {
            let main_exists = path_exists(&self.paths.ipc_socket).map_err(|error| {
                DaemonError::startup(
                    DaemonErrorKind::UnsafeStaleState,
                    StartupStage::StaleCleanup,
                )
                .with_source(error)
            })?;
            let control_exists = path_exists(&self.paths.shutdown_socket).map_err(|error| {
                DaemonError::startup(
                    DaemonErrorKind::UnsafeStaleState,
                    StartupStage::StaleCleanup,
                )
                .with_source(error)
            })?;
            if !main_exists && !control_exists {
                return Ok(());
            }
            return Err(DaemonError::startup(
                DaemonErrorKind::UnsafeStaleState,
                StartupStage::StaleCleanup,
            ));
        };
        if record.socket_path != self.paths.ipc_socket
            || record.protocol_version != IPC_PROTOCOL_VERSION
            || self.process_matches(&record.supervisor)?
            || stale_owner.is_some_and(|owner| {
                owner.process != record.supervisor
                    || owner.instance_token() != record.instance_token()
            })
        {
            return Err(DaemonError::startup(
                DaemonErrorKind::UnsafeStaleState,
                StartupStage::StaleCleanup,
            ));
        }
        remove_instance_artifacts(&self.paths, &record).map_err(with_stale_cleanup_stage)
    }

    fn cleanup_after_exit(&self) -> Result<(), DaemonError> {
        match self.probe_supervisor()? {
            SupervisorProbe::Live(_) => Err(DaemonError::new(DaemonErrorKind::ShutdownTimedOut)),
            SupervisorProbe::Vacant { lease, stale_owner } => {
                let result = self.cleanup_stale_state(stale_owner.as_ref());
                lease.release().map_err(state_error)?;
                result
            }
        }
    }

    fn wait_for_exit(
        &self,
        process: &ProcessIdentity,
        timeout: Duration,
    ) -> Result<bool, DaemonError> {
        let deadline = deadline_after(self.clock.monotonic_now(), timeout);
        loop {
            if !self.process_matches(process)? {
                return Ok(true);
            }
            if self.clock.monotonic_now() >= deadline {
                return Ok(false);
            }
            self.sleep_until_poll(deadline);
        }
    }

    fn process_matches(&self, process: &ProcessIdentity) -> Result<bool, DaemonError> {
        Ok(self
            .process
            .identity(process.pid)
            .map_err(|error| {
                DaemonError::new(DaemonErrorKind::ProcessControlFailed).with_source(error)
            })?
            .as_deref()
            == Some(process.start_identity.as_str()))
    }

    fn sleep_until_poll(&self, deadline: Duration) {
        let remaining = deadline.saturating_sub(self.clock.monotonic_now());
        self.clock.sleep(self.timeouts.poll_interval.min(remaining));
    }
}

enum SupervisorProbe {
    Live(InstanceRecord),
    Vacant {
        lease: DirectoryLease,
        stale_owner: Option<LeaseOwner>,
    },
}

enum InitialStartup {
    Process(ProcessIdentity),
    Readiness(ReadinessEnvelope),
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ReadinessEnvelope {
    schema_version: u16,
    token: String,
    process: ProcessIdentity,
    outcome: ReadinessOutcome,
}

#[derive(Deserialize, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
enum ReadinessOutcome {
    Ready { instance_token: String },
    Failed(ReadinessFailure),
}

fn validate_readiness_payload(
    process: &ProcessIdentity,
    outcome: &ReadinessOutcome,
) -> Result<(), DaemonError> {
    if process.pid == 0 || process.start_identity.is_empty() || process.start_identity.len() > 512 {
        return Err(DaemonError::new(DaemonErrorKind::InvalidReadiness));
    }
    match outcome {
        ReadinessOutcome::Ready { instance_token } => {
            if instance_token.is_empty()
                || instance_token.len() > 128
                || !instance_token.bytes().all(|byte| byte.is_ascii_graphic())
                || uuid::Uuid::parse_str(instance_token).is_err()
            {
                return Err(DaemonError::new(DaemonErrorKind::InvalidReadiness));
            }
        }
        ReadinessOutcome::Failed(failure) => {
            if failure.message.is_empty()
                || failure.message.len() > 4 * 1024
                || failure.message.chars().any(|character| {
                    character.is_control() && character != '\n' && character != '\t'
                })
            {
                return Err(DaemonError::new(DaemonErrorKind::InvalidReadiness));
            }
        }
    }
    Ok(())
}

fn deadline_after(now: Duration, timeout: Duration) -> Duration {
    now.saturating_add(timeout)
}

fn retain_launched_child(children: &Mutex<Vec<Child>>, child: Child) -> u32 {
    let pid = child.id();
    children
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .push(child);
    pid
}

fn state_error(error: LifecycleError) -> DaemonError {
    DaemonError::new(DaemonErrorKind::StateOperationFailed).with_source(error)
}

fn write_private_once(path: &Path, content: &[u8]) -> io::Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "missing parent directory"))?;
    let temporary = parent.join(format!(".readiness.{}.tmp", uuid::Uuid::new_v4()));
    let mut options = OpenOptions::new();
    options.write(true).create_new(true).mode(0o600);
    let result = (|| {
        let mut file = options.open(&temporary)?;
        file.write_all(content)?;
        file.sync_all()?;
        drop(file);
        fs::hard_link(&temporary, path)
    })();
    match result {
        Ok(()) => {
            let _ = fs::remove_file(&temporary);
            let _ = File::open(parent).and_then(|directory| directory.sync_all());
            Ok(())
        }
        Err(error) => {
            let _ = fs::remove_file(&temporary);
            Err(error)
        }
    }
}

fn read_limited(path: &Path, limit: usize) -> io::Result<Vec<u8>> {
    let file = File::open(path)?;
    if file.metadata()?.len() > limit as u64 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "the readiness record exceeds its size limit",
        ));
    }
    let mut content = Vec::new();
    file.take((limit as u64).saturating_add(1))
        .read_to_end(&mut content)?;
    if content.len() > limit {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "the readiness record exceeds its size limit",
        ));
    }
    Ok(content)
}

fn remove_regular_file(path: &Path) -> Result<(), DaemonError> {
    let metadata = fs::symlink_metadata(path).map_err(|error| {
        DaemonError::new(DaemonErrorKind::StateOperationFailed).with_source(error)
    })?;
    if !metadata.is_file() || metadata.file_type().is_symlink() {
        return Err(DaemonError::new(DaemonErrorKind::UnsafeStaleState));
    }
    fs::remove_file(path)
        .map_err(|error| DaemonError::new(DaemonErrorKind::StateOperationFailed).with_source(error))
}

fn read_instance_record_safe(
    path: &Path,
    error_kind: DaemonErrorKind,
) -> Result<Option<InstanceRecord>, DaemonError> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(DaemonError::new(error_kind).with_source(error)),
    };
    if !metadata.is_file()
        || metadata.file_type().is_symlink()
        || metadata.permissions().mode() & 0o077 != 0
    {
        return Err(DaemonError::new(error_kind));
    }
    let record = InstanceRecord::read_private(path)
        .map_err(|error| DaemonError::new(error_kind).with_source(error))?;
    if record.as_ref().is_some_and(|record| {
        record.supervisor.pid == 0
            || record.supervisor.start_identity.is_empty()
            || uuid::Uuid::parse_str(record.instance_token()).is_err()
    }) {
        return Err(DaemonError::new(error_kind));
    }
    Ok(record)
}

fn read_supervisor_lease_owner(root: &Path) -> Result<Option<LeaseOwner>, DaemonError> {
    let lock_path = root.join(format!("{SUPERVISOR_LEASE_NAME}.lock"));
    let metadata = match fs::symlink_metadata(&lock_path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(DaemonError::new(DaemonErrorKind::StateOperationFailed).with_source(error));
        }
    };
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(DaemonError::new(DaemonErrorKind::UnsafeStaleState));
    }
    let owner_path = lock_path.join("owner.json");
    let owner_metadata = fs::symlink_metadata(&owner_path)
        .map_err(|error| DaemonError::new(DaemonErrorKind::UnsafeStaleState).with_source(error))?;
    if !owner_metadata.is_file()
        || owner_metadata.file_type().is_symlink()
        || owner_metadata.permissions().mode() & 0o077 != 0
    {
        return Err(DaemonError::new(DaemonErrorKind::UnsafeStaleState));
    }
    let content = read_limited(&owner_path, READINESS_MAX_BYTES)
        .map_err(|error| DaemonError::new(DaemonErrorKind::UnsafeStaleState).with_source(error))?;
    let owner: LeaseOwner = serde_json::from_slice(&content)
        .map_err(|error| DaemonError::new(DaemonErrorKind::UnsafeStaleState).with_source(error))?;
    if owner.process.pid == 0
        || owner.process.start_identity.is_empty()
        || owner.instance_token().is_empty()
        || uuid::Uuid::parse_str(owner.instance_token()).is_err()
    {
        return Err(DaemonError::new(DaemonErrorKind::UnsafeStaleState));
    }
    Ok(Some(owner))
}

fn remove_instance_artifacts(
    paths: &StatePaths,
    expected: &InstanceRecord,
) -> Result<(), DaemonError> {
    let current =
        read_instance_record_safe(&paths.instance_record, DaemonErrorKind::UnsafeStaleState)?;
    if current.as_ref() != Some(expected) || expected.socket_path != paths.ipc_socket {
        return Err(DaemonError::new(DaemonErrorKind::UnsafeStaleState));
    }
    let socket_exists = verified_socket_exists(&paths.ipc_socket)?;
    let shutdown_socket_exists = verified_socket_exists(&paths.shutdown_socket)?;
    let quarantine = paths
        .root
        .join(format!(".instance.{}.stale", uuid::Uuid::new_v4()));
    fs::rename(&paths.instance_record, &quarantine).map_err(|error| {
        DaemonError::new(DaemonErrorKind::StateOperationFailed).with_source(error)
    })?;
    let quarantined = read_instance_record_safe(&quarantine, DaemonErrorKind::UnsafeStaleState);
    if !matches!(quarantined, Ok(Some(ref record)) if record == expected) {
        let _ = restore_quarantined_record(&quarantine, &paths.instance_record);
        return Err(DaemonError::new(DaemonErrorKind::UnsafeStaleState));
    }
    if socket_exists && let Err(error) = remove_verified_stale_socket(&paths.ipc_socket) {
        let _ = restore_quarantined_record(&quarantine, &paths.instance_record);
        return Err(DaemonError::new(DaemonErrorKind::UnsafeStaleState).with_source(error));
    }
    if shutdown_socket_exists
        && let Err(error) = remove_verified_stale_socket(&paths.shutdown_socket)
    {
        let _ = restore_quarantined_record(&quarantine, &paths.instance_record);
        return Err(DaemonError::new(DaemonErrorKind::UnsafeStaleState).with_source(error));
    }
    if let Err(error) = remove_regular_file(&quarantine) {
        let _ = restore_quarantined_record(&quarantine, &paths.instance_record);
        return Err(error);
    }
    File::open(&paths.root)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| DaemonError::new(DaemonErrorKind::StateOperationFailed).with_source(error))
}

fn path_exists(path: &Path) -> io::Result<bool> {
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error),
    }
}

fn verified_socket_exists(path: &Path) -> Result<bool, DaemonError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_socket() => Ok(true),
        Ok(_) => Err(DaemonError::new(DaemonErrorKind::UnsafeStaleState)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => {
            Err(DaemonError::new(DaemonErrorKind::StateOperationFailed).with_source(error))
        }
    }
}

fn restore_quarantined_record(quarantine: &Path, destination: &Path) -> io::Result<()> {
    match fs::symlink_metadata(destination) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            fs::rename(quarantine, destination)
        }
        Ok(_) => Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "the instance record path was replaced",
        )),
        Err(error) => Err(error),
    }
}

fn with_stale_cleanup_stage(mut error: DaemonError) -> DaemonError {
    error.kind = DaemonErrorKind::UnsafeStaleState;
    error.stage = Some(StartupStage::StaleCleanup);
    error
}
