use std::collections::VecDeque;
use std::fs;
use std::io::{self, Read};
use std::os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt, chown};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStderr, ChildStdin, ChildStdout, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use nix::sys::signal::{Signal, kill};
use nix::unistd::Pid;
#[cfg(target_os = "macos")]
use socket2::{Domain, Protocol, Socket, Type};

use crate::constants::{
    CORE_LOG_FORWARD_CAPACITY, CORE_LOG_FORWARD_MAX_BYTES, CORE_LOG_LINE_MAX_BYTES,
    CORE_PROCESS_STOP_TIMEOUT, CORE_READINESS_TIMEOUT,
};
use crate::core::{CoreControlEndpoint, MihomoAdapter, MihomoReadiness, ProcessOutputSource};
use crate::core_guardian::{CoreGuardianHandshake, CoreGuardianInvocation, read_handshake};
use crate::lifecycle::{ProcessInspector, PsProcessInspector};
use crate::mihomo::UnixMihomoAdapter;
use crate::mihomo_command::enforce_managed_runtime;
use crate::service::{
    CoreProcessController, CoreProcessLog, CoreProcessLogBatch, OwnedProcessIdentity,
    ProcessIdentityProbe, ServicePlatformError, ServicePlatformErrorKind, SpawnedCoreProcess,
    TunCapabilityPreflight, VerifiedRuntimeBundle,
};

const PROCESS_IDENTITY_ATTEMPTS: usize = 20;
const PROCESS_IDENTITY_RETRY: Duration = Duration::from_millis(10);
const PROCESS_WAIT_POLL_INTERVAL: Duration = Duration::from_millis(10);
const LOG_TRUNCATION_MARKER: &str = " [truncated]";

pub trait CoreControlClient: Send + Sync {
    fn readiness(
        &self,
        endpoint: &CoreControlEndpoint,
    ) -> Result<MihomoReadiness, ServicePlatformError>;

    fn reload(
        &self,
        endpoint: &CoreControlEndpoint,
        configuration_path: &Path,
    ) -> Result<(), ServicePlatformError>;

    fn cancel_pending(&self, _owner_generation: u64) {}

    fn reset_cancellation(&self, _owner_generation: u64) {}
}

#[derive(Clone, Debug, Default)]
pub struct UnixCoreControlClient {
    adapter: UnixMihomoAdapter,
}

impl UnixCoreControlClient {
    #[must_use]
    pub fn new(adapter: UnixMihomoAdapter) -> Self {
        Self { adapter }
    }
}

impl CoreControlClient for UnixCoreControlClient {
    fn readiness(
        &self,
        endpoint: &CoreControlEndpoint,
    ) -> Result<MihomoReadiness, ServicePlatformError> {
        self.adapter
            .readiness(endpoint)
            .map_err(|_| platform_error(ServicePlatformErrorKind::Readiness))
    }

    fn reload(
        &self,
        endpoint: &CoreControlEndpoint,
        configuration_path: &Path,
    ) -> Result<(), ServicePlatformError> {
        self.adapter
            .reload_configuration(endpoint, configuration_path)
            .map_err(|_| platform_error(ServicePlatformErrorKind::Reload))
    }

    fn cancel_pending(&self, owner_generation: u64) {
        self.adapter.cancel_pending_for(owner_generation);
    }

    fn reset_cancellation(&self, owner_generation: u64) {
        self.adapter.reset_cancellation_for(owner_generation);
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NativeCoreProcessConfig {
    pub readiness_timeout: Duration,
    pub readiness_poll_interval: Duration,
    pub stop_timeout: Duration,
    pub log_capacity: usize,
    pub max_log_line_bytes: usize,
}

impl Default for NativeCoreProcessConfig {
    fn default() -> Self {
        Self {
            readiness_timeout: CORE_READINESS_TIMEOUT,
            readiness_poll_interval: Duration::from_millis(50),
            stop_timeout: CORE_PROCESS_STOP_TIMEOUT,
            log_capacity: CORE_LOG_FORWARD_CAPACITY,
            max_log_line_bytes: CORE_LOG_LINE_MAX_BYTES,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NativeCoreProcessConfigError {
    field: &'static str,
}

impl std::fmt::Display for NativeCoreProcessConfigError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "Core process controller setting '{}' must be positive",
            self.field
        )
    }
}

impl std::error::Error for NativeCoreProcessConfigError {}

struct ManagedChild {
    child: Child,
    ownership: ChildOwnership,
    identity: OwnedProcessIdentity,
    endpoint: CoreControlEndpoint,
    exited: bool,
    log_readers: Option<LogReaders>,
}

struct LogReaders {
    stdout: thread::JoinHandle<()>,
    stderr: thread::JoinHandle<()>,
}

enum ChildOwnership {
    Direct,
    Guardian { control: Option<ChildStdin> },
}

struct SpawnedManagedChild {
    child: Child,
    ownership: ChildOwnership,
    pid: u32,
    process_start_identity: String,
    stdout: ChildStdout,
    stderr: ChildStderr,
}

enum CoreLaunchMode {
    Direct,
    Guardian { executable: PathBuf },
}

#[derive(Default)]
struct ControllerState {
    child: Option<ManagedChild>,
}

#[derive(Default)]
struct ApplyCancellationEpoch {
    latest_generation: u64,
    owner_generation: Option<u64>,
}

struct LogQueue {
    generation: Option<crate::domain::CoreInstanceGeneration>,
    capacity: usize,
    retained_bytes: usize,
    records: VecDeque<CoreProcessLog>,
    dropped: u64,
}

impl LogQueue {
    fn new(capacity: usize) -> Self {
        Self {
            generation: None,
            capacity,
            retained_bytes: 0,
            records: VecDeque::with_capacity(capacity),
            dropped: 0,
        }
    }

    fn begin_generation(&mut self, generation: crate::domain::CoreInstanceGeneration) {
        self.generation = Some(generation);
        self.records.clear();
        self.retained_bytes = 0;
        self.dropped = 0;
    }

    fn finish_generation(&mut self, generation: crate::domain::CoreInstanceGeneration) {
        if self.generation == Some(generation) {
            self.generation = None;
        }
    }

    fn push(
        &mut self,
        generation: crate::domain::CoreInstanceGeneration,
        source: ProcessOutputSource,
        message: String,
    ) {
        if self.generation != Some(generation) {
            return;
        }
        let message = message.into_boxed_str().into_string();
        if message.len() > CORE_LOG_FORWARD_MAX_BYTES {
            self.dropped = self.dropped.saturating_add(1);
            return;
        }
        while self.records.len() == self.capacity
            || self.retained_bytes.saturating_add(message.len()) > CORE_LOG_FORWARD_MAX_BYTES
        {
            let Some(dropped) = self.records.pop_front() else {
                break;
            };
            self.retained_bytes = self.retained_bytes.saturating_sub(dropped.message.len());
            self.dropped = self.dropped.saturating_add(1);
        }
        self.retained_bytes = self.retained_bytes.saturating_add(message.len());
        self.records.push_back(CoreProcessLog {
            timestamp_unix_ms: now_unix_ms(),
            source,
            message,
        });
    }

    fn take(&mut self, limit: usize) -> CoreProcessLogBatch {
        if limit == 0 {
            return CoreProcessLogBatch {
                records: Vec::new(),
                dropped: 0,
            };
        }
        let retain = limit.min(self.capacity);
        let excess = self.records.len().saturating_sub(retain);
        if excess > 0 {
            let dropped_bytes = self
                .records
                .drain(..excess)
                .map(|record| record.message.len())
                .sum::<usize>();
            self.retained_bytes = self.retained_bytes.saturating_sub(dropped_bytes);
            self.dropped = self
                .dropped
                .saturating_add(u64::try_from(excess).unwrap_or(u64::MAX));
        }
        let records = self.records.drain(..).collect();
        self.retained_bytes = 0;
        CoreProcessLogBatch {
            records,
            dropped: std::mem::take(&mut self.dropped),
        }
    }
}

pub struct NativeCoreProcessController {
    config: NativeCoreProcessConfig,
    control: Arc<dyn CoreControlClient>,
    inspector: Arc<dyn ProcessInspector>,
    launch_mode: CoreLaunchMode,
    state: Mutex<ControllerState>,
    logs: Arc<Mutex<LogQueue>>,
    apply_cancelled: AtomicBool,
    apply_cancellation_epoch: Mutex<ApplyCancellationEpoch>,
}

impl NativeCoreProcessController {
    pub fn new(
        config: NativeCoreProcessConfig,
        control: Arc<dyn CoreControlClient>,
        inspector: Arc<dyn ProcessInspector>,
    ) -> Result<Self, NativeCoreProcessConfigError> {
        for (field, value) in [
            ("readiness_timeout", config.readiness_timeout),
            ("readiness_poll_interval", config.readiness_poll_interval),
            ("stop_timeout", config.stop_timeout),
        ] {
            if value.is_zero() {
                return Err(NativeCoreProcessConfigError { field });
            }
        }
        for (field, value) in [
            ("log_capacity", config.log_capacity),
            ("max_log_line_bytes", config.max_log_line_bytes),
        ] {
            if value == 0 {
                return Err(NativeCoreProcessConfigError { field });
            }
        }
        Ok(Self {
            config,
            control,
            inspector,
            launch_mode: CoreLaunchMode::Direct,
            state: Mutex::new(ControllerState::default()),
            logs: Arc::new(Mutex::new(LogQueue::new(config.log_capacity))),
            apply_cancelled: AtomicBool::new(false),
            apply_cancellation_epoch: Mutex::new(ApplyCancellationEpoch::default()),
        })
    }

    pub fn new_guarded(
        config: NativeCoreProcessConfig,
        control: Arc<dyn CoreControlClient>,
        inspector: Arc<dyn ProcessInspector>,
        guardian_executable: PathBuf,
    ) -> Result<Self, NativeCoreProcessConfigError> {
        if !guardian_executable.is_absolute() {
            return Err(NativeCoreProcessConfigError {
                field: "guardian_executable",
            });
        }
        let mut controller = Self::new(config, control, inspector)?;
        controller.launch_mode = CoreLaunchMode::Guardian {
            executable: guardian_executable,
        };
        Ok(controller)
    }

    pub fn product_defaults() -> io::Result<Self> {
        Self::product_defaults_with(NativeCoreProcessConfig::default(), std::env::current_exe)
    }

    fn product_defaults_with(
        config: NativeCoreProcessConfig,
        current_executable: impl FnOnce() -> io::Result<PathBuf>,
    ) -> io::Result<Self> {
        let guardian_executable = current_executable()?;
        Self::new_guarded(
            config,
            Arc::new(UnixCoreControlClient::default()),
            Arc::new(PsProcessInspector),
            guardian_executable,
        )
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))
    }

    fn lock_state(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, ControllerState>, ServicePlatformError> {
        self.state
            .lock()
            .map_err(|_| platform_error(ServicePlatformErrorKind::ProcessInspection))
    }

    fn ensure_apply_active(&self) -> Result<(), ServicePlatformError> {
        if self.apply_cancelled.load(Ordering::Acquire) {
            Err(platform_error(ServicePlatformErrorKind::ApplyCancelled))
        } else {
            Ok(())
        }
    }

    fn verify_managed<'a>(
        &self,
        state: &'a mut ControllerState,
        expected: &OwnedProcessIdentity,
    ) -> Result<&'a mut ManagedChild, ServicePlatformError> {
        let managed = state
            .child
            .as_mut()
            .ok_or_else(|| platform_error(ServicePlatformErrorKind::ProcessInspection))?;
        if managed.identity != *expected {
            return Err(platform_error(ServicePlatformErrorKind::ProcessInspection));
        }
        if self.observe_exit(managed)? {
            return Err(platform_error(ServicePlatformErrorKind::ProcessInspection));
        }
        if matches!(managed.ownership, ChildOwnership::Guardian { .. })
            && self
                .inspector
                .identity(managed.identity.pid)
                .map_err(|_| platform_error(ServicePlatformErrorKind::ProcessInspection))?
                .as_deref()
                != Some(managed.identity.process_start_identity.as_str())
        {
            return Err(platform_error(ServicePlatformErrorKind::ProcessInspection));
        }
        Ok(managed)
    }

    fn observe_exit(&self, managed: &mut ManagedChild) -> Result<bool, ServicePlatformError> {
        if !managed.exited {
            if managed
                .child
                .try_wait()
                .map_err(|_| platform_error(ServicePlatformErrorKind::ProcessInspection))?
                .is_none()
            {
                return Ok(false);
            }
            managed.exited = true;
        }
        if matches!(managed.ownership, ChildOwnership::Guardian { .. }) {
            contain_exact_process_after_guardian_exit(
                self.inspector.as_ref(),
                &managed.identity,
                self.config.stop_timeout,
            )?;
        }
        self.finish_managed_logs(managed)?;
        Ok(true)
    }

    fn finish_managed_logs(&self, managed: &mut ManagedChild) -> Result<(), ServicePlatformError> {
        let Some(readers) = managed.log_readers.as_ref() else {
            return Ok(());
        };
        let deadline = Instant::now()
            .checked_add(self.config.stop_timeout)
            .ok_or_else(|| platform_error(ServicePlatformErrorKind::Logs))?;
        while !readers.stdout.is_finished() || !readers.stderr.is_finished() {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(platform_error(ServicePlatformErrorKind::Logs));
            }
            thread::sleep(remaining.min(PROCESS_WAIT_POLL_INTERVAL));
        }
        let readers = managed
            .log_readers
            .take()
            .ok_or_else(|| platform_error(ServicePlatformErrorKind::Logs))?;
        let stdout_result = readers.stdout.join();
        let stderr_result = readers.stderr.join();
        self.logs
            .lock()
            .map_err(|_| platform_error(ServicePlatformErrorKind::Logs))?
            .finish_generation(managed.identity.instance_generation);
        if stdout_result.is_err() || stderr_result.is_err() {
            Err(platform_error(ServicePlatformErrorKind::Logs))
        } else {
            Ok(())
        }
    }

    fn discover_identity(&self, pid: u32) -> Result<String, ServicePlatformError> {
        for attempt in 0..PROCESS_IDENTITY_ATTEMPTS {
            match self.inspector.identity(pid) {
                Ok(Some(identity)) if !identity.is_empty() => return Ok(identity),
                Ok(_) if attempt + 1 < PROCESS_IDENTITY_ATTEMPTS => {
                    thread::sleep(PROCESS_IDENTITY_RETRY);
                }
                Ok(_) => break,
                Err(_) => {
                    return Err(platform_error(ServicePlatformErrorKind::ProcessInspection));
                }
            }
        }
        Err(platform_error(ServicePlatformErrorKind::ProcessInspection))
    }

    fn start_log_reader<R: Read + Send + 'static>(
        &self,
        reader: R,
        source: ProcessOutputSource,
        generation: crate::domain::CoreInstanceGeneration,
    ) -> thread::JoinHandle<()> {
        let logs = Arc::clone(&self.logs);
        let max_line_bytes = self.config.max_log_line_bytes;
        thread::spawn(move || {
            collect_log_stream(reader, source, generation, logs, max_line_bytes);
        })
    }

    fn launch_direct(
        &self,
        bundle: &VerifiedRuntimeBundle,
        endpoint: &CoreControlEndpoint,
    ) -> Result<SpawnedManagedChild, ServicePlatformError> {
        let generation_root = bundle.bundle().generation_root.as_path();
        let mut command = Command::new(bundle.executable_path());
        command
            .arg("-d")
            .arg(generation_root)
            .arg("-f")
            .arg(bundle.configuration_path())
            .arg("-ext-ctl-unix")
            .arg(&endpoint.socket_path)
            .current_dir(generation_root)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        enforce_managed_runtime(&mut command);
        let mut child = command
            .spawn()
            .map_err(|_| platform_error(ServicePlatformErrorKind::Spawn))?;
        let pid = child.id();
        let process_start_identity = match self.discover_identity(pid) {
            Ok(identity) => identity,
            Err(error) => {
                terminate_owned_child(&mut child, self.config.stop_timeout);
                return Err(error);
            }
        };
        let (stdout, stderr) = match (child.stdout.take(), child.stderr.take()) {
            (Some(stdout), Some(stderr)) => (stdout, stderr),
            _ => {
                terminate_owned_child(&mut child, self.config.stop_timeout);
                return Err(platform_error(ServicePlatformErrorKind::Spawn));
            }
        };
        Ok(SpawnedManagedChild {
            child,
            ownership: ChildOwnership::Direct,
            pid,
            process_start_identity,
            stdout,
            stderr,
        })
    }

    fn launch_guarded(
        &self,
        guardian_executable: &Path,
        bundle: &VerifiedRuntimeBundle,
        endpoint: &CoreControlEndpoint,
    ) -> Result<SpawnedManagedChild, ServicePlatformError> {
        let generation_root = bundle.bundle().generation_root.as_path();
        let invocation = CoreGuardianInvocation::new(
            bundle.executable_path().to_path_buf(),
            bundle.bundle().mihomo_binary_sha256.clone(),
            generation_root.to_path_buf(),
            bundle.configuration_path().to_path_buf(),
            endpoint.socket_path.clone(),
        )
        .map_err(|_| platform_error(ServicePlatformErrorKind::Spawn))?;
        let mut command = Command::new(guardian_executable);
        invocation.configure_command(&mut command);
        let mut child = command
            .current_dir(generation_root)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|_| platform_error(ServicePlatformErrorKind::Spawn))?;
        let (control, stdout, stderr) =
            match (child.stdin.take(), child.stdout.take(), child.stderr.take()) {
                (Some(control), Some(stdout), Some(stderr)) => (control, stdout, stderr),
                (control, stdout, stderr) => {
                    drop(control);
                    drop(stdout);
                    drop(stderr);
                    stop_unidentified_guardian(&mut child, self.config.stop_timeout);
                    return Err(platform_error(ServicePlatformErrorKind::Spawn));
                }
            };
        let (handshake, stdout) = match read_guardian_handshake(
            stdout,
            self.config.readiness_timeout,
            &self.apply_cancelled,
        ) {
            Ok(result) => result,
            Err(error) => {
                drop(control);
                stop_unidentified_guardian(&mut child, self.config.stop_timeout);
                return Err(error);
            }
        };
        let pid = handshake.pid();
        let claimed_identity = handshake.process_start_identity().to_owned();
        let process_start_identity = match self.discover_identity(pid) {
            Ok(identity) if identity == claimed_identity => identity,
            Ok(_) | Err(_) => {
                let mut managed = ManagedChild {
                    child,
                    ownership: ChildOwnership::Guardian {
                        control: Some(control),
                    },
                    identity: OwnedProcessIdentity {
                        pid,
                        process_start_identity: claimed_identity,
                        instance_generation: crate::domain::CoreInstanceGeneration(0),
                    },
                    endpoint: endpoint.clone(),
                    exited: false,
                    log_readers: None,
                };
                if stop_managed_child(
                    &mut managed,
                    self.inspector.as_ref(),
                    self.config.stop_timeout,
                )
                .is_err()
                {
                    stop_unidentified_guardian(&mut managed.child, self.config.stop_timeout);
                }
                return Err(platform_error(ServicePlatformErrorKind::ProcessInspection));
            }
        };
        Ok(SpawnedManagedChild {
            child,
            ownership: ChildOwnership::Guardian {
                control: Some(control),
            },
            pid,
            process_start_identity,
            stdout,
            stderr,
        })
    }
}

fn read_guardian_handshake(
    mut stdout: ChildStdout,
    timeout: Duration,
    cancellation: &AtomicBool,
) -> Result<(CoreGuardianHandshake, ChildStdout), ServicePlatformError> {
    let (sender, receiver) = mpsc::sync_channel(1);
    thread::Builder::new()
        .name("ratash-guardian-handshake".to_owned())
        .spawn(move || {
            let result = read_handshake(&mut stdout);
            let _ = sender.send((result, stdout));
        })
        .map_err(|_| platform_error(ServicePlatformErrorKind::Spawn))?;
    let deadline = Instant::now()
        .checked_add(timeout)
        .ok_or_else(|| platform_error(ServicePlatformErrorKind::Spawn))?;
    loop {
        if cancellation.load(Ordering::Acquire) {
            return Err(platform_error(ServicePlatformErrorKind::ApplyCancelled));
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(platform_error(ServicePlatformErrorKind::Spawn));
        }
        match receiver.recv_timeout(remaining.min(Duration::from_millis(50))) {
            Ok((Ok(handshake), stdout)) => return Ok((handshake, stdout)),
            Ok((Err(_), _)) | Err(mpsc::RecvTimeoutError::Disconnected) => {
                return Err(platform_error(ServicePlatformErrorKind::Spawn));
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {}
        }
    }
}

fn terminate_owned_child(child: &mut Child, timeout: Duration) {
    if let Ok(pid) = i32::try_from(child.id()) {
        let _ = kill(Pid::from_raw(pid), Signal::SIGTERM);
    }
    let grace = timeout.min(Duration::from_millis(250));
    if let Some(deadline) = Instant::now().checked_add(grace)
        && wait_for_child_exit(child, deadline).unwrap_or(false)
    {
        return;
    }
    let _ = child.kill();
    if let Some(deadline) = Instant::now().checked_add(grace) {
        let _ = wait_for_child_exit(child, deadline);
    }
}

fn stop_unidentified_guardian(child: &mut Child, timeout: Duration) {
    if let Some(deadline) = Instant::now().checked_add(timeout)
        && wait_for_child_exit(child, deadline).unwrap_or(false)
    {
        return;
    }
    terminate_owned_child(child, timeout);
}

fn stop_managed_child(
    managed: &mut ManagedChild,
    inspector: &dyn ProcessInspector,
    timeout: Duration,
) -> Result<(), ServicePlatformError> {
    let started = Instant::now();
    let deadline = started
        .checked_add(timeout)
        .ok_or_else(|| platform_error(ServicePlatformErrorKind::Stop))?;
    let guardian = matches!(managed.ownership, ChildOwnership::Guardian { .. });
    match &mut managed.ownership {
        ChildOwnership::Direct => {
            managed
                .child
                .kill()
                .map_err(|_| platform_error(ServicePlatformErrorKind::Stop))?;
        }
        ChildOwnership::Guardian { control } => {
            drop(control.take());
            let fallback_delay = timeout.checked_div(2).filter(|delay| !delay.is_zero());
            let fallback_deadline = started
                .checked_add(fallback_delay.unwrap_or(timeout))
                .ok_or_else(|| platform_error(ServicePlatformErrorKind::Stop))?;
            if wait_for_child_exit(&mut managed.child, fallback_deadline)? {
                return contain_exact_process_after_guardian_exit(
                    inspector,
                    &managed.identity,
                    timeout,
                );
            }
            terminate_exact_process(inspector, &managed.identity)?;
        }
    }
    if wait_for_child_exit(&mut managed.child, deadline)? {
        return if guardian {
            contain_exact_process_after_guardian_exit(inspector, &managed.identity, timeout)
        } else {
            Ok(())
        };
    }
    managed
        .child
        .kill()
        .map_err(|_| platform_error(ServicePlatformErrorKind::Stop))?;
    let forced_deadline = Instant::now()
        .checked_add(timeout.min(Duration::from_millis(250)))
        .ok_or_else(|| platform_error(ServicePlatformErrorKind::Stop))?;
    if wait_for_child_exit(&mut managed.child, forced_deadline)? {
        if guardian {
            contain_exact_process_after_guardian_exit(inspector, &managed.identity, timeout)
        } else {
            Ok(())
        }
    } else {
        Err(platform_error(ServicePlatformErrorKind::Stop))
    }
}

fn wait_for_child_exit(child: &mut Child, deadline: Instant) -> Result<bool, ServicePlatformError> {
    loop {
        if child
            .try_wait()
            .map_err(|_| platform_error(ServicePlatformErrorKind::Stop))?
            .is_some()
        {
            return Ok(true);
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Ok(false);
        }
        thread::sleep(remaining.min(PROCESS_WAIT_POLL_INTERVAL));
    }
}

fn terminate_exact_process(
    inspector: &dyn ProcessInspector,
    identity: &OwnedProcessIdentity,
) -> Result<(), ServicePlatformError> {
    match inspector
        .identity(identity.pid)
        .map_err(|_| platform_error(ServicePlatformErrorKind::ProcessInspection))?
    {
        None => return Ok(()),
        Some(actual) if actual == identity.process_start_identity => {}
        Some(_) => return Err(platform_error(ServicePlatformErrorKind::ProcessInspection)),
    }
    let pid = i32::try_from(identity.pid)
        .map_err(|_| platform_error(ServicePlatformErrorKind::ProcessInspection))?;
    match kill(Pid::from_raw(pid), Signal::SIGKILL) {
        Ok(()) | Err(nix::errno::Errno::ESRCH) => Ok(()),
        Err(_) => Err(platform_error(ServicePlatformErrorKind::Stop)),
    }
}

fn contain_exact_process_after_guardian_exit(
    inspector: &dyn ProcessInspector,
    identity: &OwnedProcessIdentity,
    timeout: Duration,
) -> Result<(), ServicePlatformError> {
    match inspector
        .identity(identity.pid)
        .map_err(|_| platform_error(ServicePlatformErrorKind::ProcessInspection))?
    {
        None => return Ok(()),
        Some(actual) if actual != identity.process_start_identity => return Ok(()),
        Some(_) => {}
    }
    terminate_exact_process(inspector, identity)?;
    let deadline = Instant::now()
        .checked_add(timeout)
        .ok_or_else(|| platform_error(ServicePlatformErrorKind::Stop))?;
    loop {
        match inspector
            .identity(identity.pid)
            .map_err(|_| platform_error(ServicePlatformErrorKind::ProcessInspection))?
        {
            None => return Ok(()),
            Some(actual) if actual != identity.process_start_identity => return Ok(()),
            Some(_) => {}
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(platform_error(ServicePlatformErrorKind::Stop));
        }
        thread::sleep(remaining.min(PROCESS_WAIT_POLL_INTERVAL));
    }
}

impl CoreProcessController for NativeCoreProcessController {
    fn spawn(
        &self,
        bundle: &VerifiedRuntimeBundle,
        endpoint: &CoreControlEndpoint,
        instance_generation: crate::domain::CoreInstanceGeneration,
    ) -> Result<SpawnedCoreProcess, ServicePlatformError> {
        self.ensure_apply_active()?;
        let mut state = self.lock_state()?;
        if let Some(existing) = state.child.as_mut() {
            if !self.observe_exit(existing)? {
                return Err(platform_error(ServicePlatformErrorKind::Spawn));
            }
            state.child = None;
        }

        let spawned = match &self.launch_mode {
            CoreLaunchMode::Direct => self.launch_direct(bundle, endpoint)?,
            CoreLaunchMode::Guardian { executable } => {
                self.launch_guarded(executable, bundle, endpoint)?
            }
        };
        self.logs
            .lock()
            .map_err(|_| platform_error(ServicePlatformErrorKind::Logs))?
            .begin_generation(instance_generation);
        let stdout_reader = self.start_log_reader(
            spawned.stdout,
            ProcessOutputSource::Stdout,
            instance_generation,
        );
        let stderr_reader = self.start_log_reader(
            spawned.stderr,
            ProcessOutputSource::Stderr,
            instance_generation,
        );
        let identity = OwnedProcessIdentity {
            pid: spawned.pid,
            process_start_identity: spawned.process_start_identity.clone(),
            instance_generation,
        };
        state.child = Some(ManagedChild {
            child: spawned.child,
            ownership: spawned.ownership,
            identity,
            endpoint: endpoint.clone(),
            exited: false,
            log_readers: Some(LogReaders {
                stdout: stdout_reader,
                stderr: stderr_reader,
            }),
        });
        Ok(SpawnedCoreProcess {
            pid: spawned.pid,
            process_start_identity: spawned.process_start_identity,
        })
    }

    fn reload(
        &self,
        process: &OwnedProcessIdentity,
        bundle: &VerifiedRuntimeBundle,
    ) -> Result<(), ServicePlatformError> {
        self.ensure_apply_active()?;
        let mut state = self.lock_state()?;
        let managed = self.verify_managed(&mut state, process)?;
        let result = self
            .control
            .reload(&managed.endpoint, bundle.configuration_path());
        self.ensure_apply_active()?;
        result
    }

    fn stop(&self, process: &OwnedProcessIdentity) -> Result<(), ServicePlatformError> {
        let mut state = self.lock_state()?;
        let managed = self.verify_managed(&mut state, process)?;
        stop_managed_child(managed, self.inspector.as_ref(), self.config.stop_timeout)?;
        managed.exited = true;
        self.finish_managed_logs(managed)?;
        Ok(())
    }

    fn readiness(
        &self,
        process: &OwnedProcessIdentity,
        endpoint: &CoreControlEndpoint,
    ) -> Result<(), ServicePlatformError> {
        self.ensure_apply_active()?;
        let mut state = self.lock_state()?;
        let managed = self.verify_managed(&mut state, process)?;
        if managed.endpoint != *endpoint {
            return Err(platform_error(ServicePlatformErrorKind::Readiness));
        }
        let deadline = Instant::now()
            .checked_add(self.config.readiness_timeout)
            .ok_or_else(|| platform_error(ServicePlatformErrorKind::ReadinessTimeout))?;
        loop {
            self.ensure_apply_active()?;
            if self.observe_exit(managed)? {
                return Err(platform_error(ServicePlatformErrorKind::Readiness));
            }
            match self.control.readiness(endpoint) {
                Ok(MihomoReadiness::Ready) => return Ok(()),
                Ok(MihomoReadiness::Starting) | Err(_) => {}
            }
            self.ensure_apply_active()?;
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(platform_error(ServicePlatformErrorKind::ReadinessTimeout));
            }
            thread::sleep(remaining.min(self.config.readiness_poll_interval));
        }
    }

    fn grant_endpoint_access(
        &self,
        endpoint: &CoreControlEndpoint,
        owner_uid: u32,
    ) -> Result<(), ServicePlatformError> {
        let before = fs::symlink_metadata(&endpoint.socket_path)
            .map_err(|_| platform_error(ServicePlatformErrorKind::ProcessInspection))?;
        let service_uid = nix::unistd::Uid::effective().as_raw();
        if !before.file_type().is_socket()
            || (before.uid() != service_uid && before.uid() != owner_uid)
        {
            return Err(platform_error(ServicePlatformErrorKind::ProcessInspection));
        }
        fs::set_permissions(&endpoint.socket_path, fs::Permissions::from_mode(0o600))
            .map_err(|_| platform_error(ServicePlatformErrorKind::ProcessInspection))?;
        if before.uid() != owner_uid {
            chown(&endpoint.socket_path, Some(owner_uid), None)
                .map_err(|_| platform_error(ServicePlatformErrorKind::ProcessInspection))?;
        }
        fs::set_permissions(&endpoint.socket_path, fs::Permissions::from_mode(0o600))
            .map_err(|_| platform_error(ServicePlatformErrorKind::ProcessInspection))?;
        let after = fs::symlink_metadata(&endpoint.socket_path)
            .map_err(|_| platform_error(ServicePlatformErrorKind::ProcessInspection))?;
        if !after.file_type().is_socket()
            || after.uid() != owner_uid
            || after.mode() & 0o777 != 0o600
            || after.dev() != before.dev()
            || after.ino() != before.ino()
        {
            return Err(platform_error(ServicePlatformErrorKind::ProcessInspection));
        }
        Ok(())
    }

    fn reap_if_exited(&self, process: &OwnedProcessIdentity) -> Result<bool, ServicePlatformError> {
        let mut state = self.lock_state()?;
        let managed = state
            .child
            .as_mut()
            .ok_or_else(|| platform_error(ServicePlatformErrorKind::ProcessInspection))?;
        if managed.identity != *process {
            return Err(platform_error(ServicePlatformErrorKind::ProcessInspection));
        }
        self.observe_exit(managed)
    }

    fn take_logs(
        &self,
        process: &OwnedProcessIdentity,
        limit: usize,
    ) -> Result<CoreProcessLogBatch, ServicePlatformError> {
        if limit == 0 {
            return Ok(CoreProcessLogBatch {
                records: Vec::new(),
                dropped: 0,
            });
        }
        let mut state = self.lock_state()?;
        let managed = state
            .child
            .as_mut()
            .ok_or_else(|| platform_error(ServicePlatformErrorKind::ProcessInspection))?;
        if managed.identity != *process {
            return Err(platform_error(ServicePlatformErrorKind::ProcessInspection));
        }
        let _ = self.observe_exit(managed)?;
        if !managed.exited
            && matches!(managed.ownership, ChildOwnership::Guardian { .. })
            && self
                .inspector
                .identity(managed.identity.pid)
                .map_err(|_| platform_error(ServicePlatformErrorKind::ProcessInspection))?
                .as_deref()
                != Some(managed.identity.process_start_identity.as_str())
        {
            return Err(platform_error(ServicePlatformErrorKind::ProcessInspection));
        }
        let mut logs = self
            .logs
            .lock()
            .map_err(|_| platform_error(ServicePlatformErrorKind::Logs))?;
        Ok(logs.take(limit))
    }

    fn reset_apply_cancellation(&self, owner_generation: u64) {
        let accepted = {
            let mut epoch = self
                .apply_cancellation_epoch
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if owner_generation <= epoch.latest_generation {
                false
            } else {
                epoch.latest_generation = owner_generation;
                epoch.owner_generation = Some(owner_generation);
                self.apply_cancelled.store(false, Ordering::Release);
                true
            }
        };
        if accepted {
            self.control.reset_cancellation(owner_generation);
        }
    }

    fn cancel_pending_apply(&self, owner_generation: u64) {
        let accepted = {
            let epoch = self
                .apply_cancellation_epoch
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if epoch.owner_generation == Some(owner_generation) {
                self.apply_cancelled.store(true, Ordering::Release);
                true
            } else {
                false
            }
        };
        if accepted {
            self.control.cancel_pending(owner_generation);
        }
    }
}

impl Drop for NativeCoreProcessController {
    fn drop(&mut self) {
        let Ok(state) = self.state.get_mut() else {
            return;
        };
        let Some(mut managed) = state.child.take() else {
            return;
        };
        let guardian = matches!(managed.ownership, ChildOwnership::Guardian { .. });
        if managed.exited {
            if guardian {
                let _ = contain_exact_process_after_guardian_exit(
                    self.inspector.as_ref(),
                    &managed.identity,
                    self.config.stop_timeout,
                );
            }
            let _ = self.finish_managed_logs(&mut managed);
            return;
        }
        if stop_managed_child(
            &mut managed,
            self.inspector.as_ref(),
            self.config.stop_timeout,
        )
        .is_err()
        {
            if guardian {
                stop_unidentified_guardian(&mut managed.child, self.config.stop_timeout);
            } else {
                terminate_owned_child(&mut managed.child, self.config.stop_timeout);
            }
        }
        if guardian {
            let _ = contain_exact_process_after_guardian_exit(
                self.inspector.as_ref(),
                &managed.identity,
                self.config.stop_timeout,
            );
        }
        managed.exited = true;
        let _ = self.finish_managed_logs(&mut managed);
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct SystemProcessIdentityProbe;

impl ProcessIdentityProbe for SystemProcessIdentityProbe {
    fn start_identity(&self, pid: u32) -> Result<Option<String>, ServicePlatformError> {
        PsProcessInspector
            .identity(pid)
            .map_err(|_| platform_error(ServicePlatformErrorKind::ProcessInspection))
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct MacOsTunCapabilityPreflight;

impl TunCapabilityPreflight for MacOsTunCapabilityPreflight {
    fn check(&self, _owner_uid: u32) -> Result<(), ServicePlatformError> {
        check_macos_tun_control_socket()
    }
}

#[cfg(target_os = "macos")]
fn check_macos_tun_control_socket() -> Result<(), ServicePlatformError> {
    Socket::new(
        Domain::from(nix::libc::PF_SYSTEM),
        Type::DGRAM,
        Some(Protocol::from(nix::libc::SYSPROTO_CONTROL)),
    )
    .map(drop)
    .map_err(map_tun_socket_error)
}

fn map_tun_socket_error(error: io::Error) -> ServicePlatformError {
    let kind = match error.raw_os_error() {
        Some(nix::libc::EAFNOSUPPORT | nix::libc::EPROTONOSUPPORT | nix::libc::ENOPROTOOPT) => {
            ServicePlatformErrorKind::TunUnsupported
        }
        Some(nix::libc::EACCES | nix::libc::EPERM) => ServicePlatformErrorKind::TunUnavailable,
        _ if error.kind() == io::ErrorKind::Unsupported => ServicePlatformErrorKind::TunUnsupported,
        _ => ServicePlatformErrorKind::TunUnavailable,
    };
    platform_error(kind)
}

#[cfg(not(target_os = "macos"))]
fn check_macos_tun_control_socket() -> Result<(), ServicePlatformError> {
    Err(platform_error(ServicePlatformErrorKind::TunUnsupported))
}

#[cfg(test)]
mod tun_preflight_tests {
    use super::*;

    #[test]
    fn tun_socket_errors_keep_permission_and_platform_support_distinct() {
        assert_eq!(
            map_tun_socket_error(io::Error::from_raw_os_error(nix::libc::EPERM)).kind,
            ServicePlatformErrorKind::TunUnavailable
        );
        assert_eq!(
            map_tun_socket_error(io::Error::from_raw_os_error(nix::libc::EAFNOSUPPORT)).kind,
            ServicePlatformErrorKind::TunUnsupported
        );
    }
}

fn collect_log_stream<R: Read>(
    mut reader: R,
    source: ProcessOutputSource,
    generation: crate::domain::CoreInstanceGeneration,
    logs: Arc<Mutex<LogQueue>>,
    max_line_bytes: usize,
) {
    let mut chunk = [0_u8; 4 * 1024];
    let mut line = Vec::with_capacity(max_line_bytes.min(chunk.len()));
    let mut discarded_bytes = 0_usize;
    let mut last_discarded_byte = None;
    loop {
        let read = match reader.read(&mut chunk) {
            Ok(0) | Err(_) => break,
            Ok(read) => read,
        };
        for byte in &chunk[..read] {
            if *byte == b'\n' {
                let trailing_cr_is_discarded =
                    discarded_bytes > 0 && last_discarded_byte == Some(b'\r');
                let discarded_content_bytes =
                    discarded_bytes.saturating_sub(usize::from(trailing_cr_is_discarded));
                let strip_trailing_cr = discarded_bytes == 0 && line.last() == Some(&b'\r');
                publish_log_line(
                    &logs,
                    generation,
                    source,
                    &mut line,
                    strip_trailing_cr,
                    discarded_content_bytes > 0,
                    max_line_bytes,
                );
                discarded_bytes = 0;
                last_discarded_byte = None;
            } else if line.len() < max_line_bytes {
                line.push(*byte);
            } else {
                discarded_bytes = discarded_bytes.saturating_add(1);
                last_discarded_byte = Some(*byte);
            }
        }
    }
    if !line.is_empty() || discarded_bytes > 0 {
        let trailing_cr_is_discarded = discarded_bytes > 0 && last_discarded_byte == Some(b'\r');
        let strip_trailing_cr = discarded_bytes == 0 && line.last() == Some(&b'\r');
        publish_log_line(
            &logs,
            generation,
            source,
            &mut line,
            strip_trailing_cr,
            discarded_bytes.saturating_sub(usize::from(trailing_cr_is_discarded)) > 0,
            max_line_bytes,
        );
    }
}

fn publish_log_line(
    logs: &Mutex<LogQueue>,
    generation: crate::domain::CoreInstanceGeneration,
    source: ProcessOutputSource,
    line: &mut Vec<u8>,
    strip_trailing_cr: bool,
    truncated: bool,
    max_line_bytes: usize,
) {
    if strip_trailing_cr {
        line.pop();
    }
    let message = bound_log_message(
        String::from_utf8_lossy(line).into_owned(),
        truncated,
        max_line_bytes,
    );
    line.clear();
    if let Ok(mut logs) = logs.lock() {
        logs.push(generation, source, message);
    }
}

fn bound_log_message(mut message: String, truncated: bool, max_line_bytes: usize) -> String {
    if !truncated && message.len() <= max_line_bytes {
        return message;
    }
    if max_line_bytes < LOG_TRUNCATION_MARKER.len() {
        return LOG_TRUNCATION_MARKER[..max_line_bytes].to_owned();
    }
    let mut end = (max_line_bytes - LOG_TRUNCATION_MARKER.len()).min(message.len());
    while !message.is_char_boundary(end) {
        end -= 1;
    }
    message.truncate(end);
    message.push_str(LOG_TRUNCATION_MARKER);
    message
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

fn platform_error(kind: ServicePlatformErrorKind) -> ServicePlatformError {
    ServicePlatformError::new(kind)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::CoreInstanceGeneration;
    use std::io::{BufRead, BufReader, Cursor};
    use std::sync::Condvar;

    #[test]
    fn product_defaults_propagates_executable_and_configuration_failures() {
        let executable_error = NativeCoreProcessController::product_defaults_with(
            NativeCoreProcessConfig::default(),
            || {
                Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "fixture executable discovery failed",
                ))
            },
        )
        .err()
        .expect("executable discovery should fail");
        assert_eq!(executable_error.kind(), io::ErrorKind::PermissionDenied);

        let config_error = NativeCoreProcessController::product_defaults_with(
            NativeCoreProcessConfig {
                readiness_timeout: Duration::ZERO,
                ..NativeCoreProcessConfig::default()
            },
            || Ok(PathBuf::from("/private/tmp/ratash-fixture")),
        )
        .err()
        .expect("invalid product settings should fail");
        assert_eq!(config_error.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn product_defaults_builds_a_guarded_controller() {
        let expected_executable =
            std::env::current_exe().expect("the test executable should be discoverable");
        let controller = NativeCoreProcessController::product_defaults()
            .expect("the test executable should configure the product controller");

        assert!(matches!(
            &controller.launch_mode,
            CoreLaunchMode::Guardian { executable }
                if executable == &expected_executable
        ));
    }

    struct StartingControl;

    struct GuardianExitFixture {
        guardian: Option<Child>,
        core_identity: OwnedProcessIdentity,
    }

    impl Drop for GuardianExitFixture {
        fn drop(&mut self) {
            if let Some(guardian) = self.guardian.as_mut() {
                terminate_owned_child(guardian, Duration::from_millis(250));
            }
            let _ = terminate_exact_process(&PsProcessInspector, &self.core_identity);
        }
    }

    impl CoreControlClient for StartingControl {
        fn readiness(
            &self,
            _endpoint: &CoreControlEndpoint,
        ) -> Result<MihomoReadiness, ServicePlatformError> {
            Ok(MihomoReadiness::Starting)
        }

        fn reload(
            &self,
            _endpoint: &CoreControlEndpoint,
            _configuration_path: &Path,
        ) -> Result<(), ServicePlatformError> {
            Ok(())
        }
    }

    #[derive(Default)]
    struct DelayedCancellationControl {
        state: Mutex<(bool, bool)>,
        wake: Condvar,
    }

    impl DelayedCancellationControl {
        fn wait_until_cancel_entered(&self) {
            let state = self.state.lock().expect("cancellation fixture lock");
            let _state = self
                .wake
                .wait_while(state, |(entered, _)| !*entered)
                .expect("cancellation fixture lock");
        }

        fn release_cancel(&self) {
            let mut state = self.state.lock().expect("cancellation fixture lock");
            state.1 = true;
            self.wake.notify_all();
        }
    }

    impl CoreControlClient for DelayedCancellationControl {
        fn readiness(
            &self,
            _endpoint: &CoreControlEndpoint,
        ) -> Result<MihomoReadiness, ServicePlatformError> {
            Ok(MihomoReadiness::Ready)
        }

        fn reload(
            &self,
            _endpoint: &CoreControlEndpoint,
            _configuration_path: &Path,
        ) -> Result<(), ServicePlatformError> {
            Ok(())
        }

        fn cancel_pending(&self, _owner_generation: u64) {
            let mut state = self.state.lock().expect("cancellation fixture lock");
            state.0 = true;
            self.wake.notify_all();
            let _state = self
                .wake
                .wait_while(state, |(_, released)| !*released)
                .expect("cancellation fixture lock");
        }
    }

    fn collect_fixture_logs(input: Vec<u8>, max_line_bytes: usize) -> CoreProcessLogBatch {
        let generation = CoreInstanceGeneration(11);
        let logs = Arc::new(Mutex::new(LogQueue::new(16)));
        logs.lock()
            .expect("fixture log queue")
            .begin_generation(generation);
        collect_log_stream(
            Cursor::new(input),
            ProcessOutputSource::Stdout,
            generation,
            Arc::clone(&logs),
            max_line_bytes,
        );
        logs.lock().expect("fixture log queue").take(16)
    }

    #[test]
    fn high_volume_log_queue_reports_each_eviction_once() {
        let generation = CoreInstanceGeneration(7);
        let mut logs = LogQueue::new(4);
        logs.begin_generation(generation);
        for index in 0..10_000 {
            logs.push(
                generation,
                ProcessOutputSource::Stdout,
                format!("line-{index}"),
            );
        }

        let first = logs.take(2);
        assert_eq!(first.dropped, 9_998);
        assert_eq!(first.records.len(), 2);
        assert_eq!(first.records[0].message, "line-9998");
        assert_eq!(first.records[1].message, "line-9999");

        let second = logs.take(usize::MAX);
        assert_eq!(second.dropped, 0);
        assert!(second.records.is_empty());
    }

    #[test]
    fn high_volume_log_queue_evicts_by_aggregate_message_bytes() {
        let generation = CoreInstanceGeneration(8);
        let mut logs = LogQueue::new(CORE_LOG_FORWARD_CAPACITY);
        logs.begin_generation(generation);
        let message = "x".repeat(CORE_LOG_LINE_MAX_BYTES);
        let retained_records = CORE_LOG_FORWARD_MAX_BYTES / CORE_LOG_LINE_MAX_BYTES;
        for _ in 0..=retained_records {
            logs.push(generation, ProcessOutputSource::Stdout, message.clone());
        }

        let batch = logs.take(usize::MAX);

        assert_eq!(batch.records.len(), retained_records);
        assert_eq!(batch.dropped, 1);
        assert_eq!(
            batch
                .records
                .iter()
                .map(|record| record.message.len())
                .sum::<usize>(),
            CORE_LOG_FORWARD_MAX_BYTES
        );
    }

    #[test]
    fn oversized_ascii_process_line_ends_with_the_stable_marker() {
        let max_line_bytes = 64;
        let mut input = vec![b'a'; max_line_bytes + 100];
        input.push(b'\n');

        let batch = collect_fixture_logs(input, max_line_bytes);

        assert_eq!(batch.records.len(), 1);
        assert_eq!(batch.records[0].message.len(), max_line_bytes);
        assert!(batch.records[0].message.ends_with(LOG_TRUNCATION_MARKER));
    }

    #[test]
    fn oversized_multibyte_process_line_keeps_valid_utf8_and_the_marker() {
        let max_line_bytes = 32;
        let mut input = "a".repeat(max_line_bytes - LOG_TRUNCATION_MARKER.len() - 1);
        input.push('\u{754c}');
        input.push_str(&"b".repeat(max_line_bytes));
        input.push('\n');

        let batch = collect_fixture_logs(input.into_bytes(), max_line_bytes);

        assert_eq!(batch.records.len(), 1);
        assert!(batch.records[0].message.len() <= max_line_bytes);
        assert!(
            batch.records[0]
                .message
                .is_char_boundary(batch.records[0].message.len())
        );
        assert!(batch.records[0].message.ends_with(LOG_TRUNCATION_MARKER));
        assert_eq!(
            &batch.records[0].message
                [..batch.records[0].message.len() - LOG_TRUNCATION_MARKER.len()],
            "a".repeat(max_line_bytes - LOG_TRUNCATION_MARKER.len() - 1)
        );
    }

    #[test]
    fn crlf_is_removed_without_marking_an_exact_bound_line_as_truncated() {
        let max_line_bytes = 64;
        let mut input = vec![b'a'; max_line_bytes];
        input.extend_from_slice(b"\r\nsecond\r\n");

        let batch = collect_fixture_logs(input, max_line_bytes);

        assert_eq!(batch.records.len(), 2);
        assert_eq!(batch.records[0].message, "a".repeat(max_line_bytes));
        assert_eq!(batch.records[1].message, "second");
    }

    #[test]
    fn chunk_boundaries_preserve_one_record_per_physical_line() {
        let max_line_bytes = 4 * 1024 + 64;
        let mut input = vec![b'a'; 4 * 1024 - 1];
        input.push(b'\n');
        input.extend(std::iter::repeat_n(b'b', 4 * 1024));
        input.extend_from_slice(b"\r\n");

        let batch = collect_fixture_logs(input, max_line_bytes);

        assert_eq!(batch.records.len(), 2);
        assert_eq!(batch.records[0].message, "a".repeat(4 * 1024 - 1));
        assert_eq!(batch.records[1].message, "b".repeat(4 * 1024));
    }

    #[test]
    fn huge_unterminated_line_is_consumed_as_one_bounded_record() {
        let max_line_bytes = 64;
        let input = vec![b'x'; 2 * 1024 * 1024];

        let batch = collect_fixture_logs(input, max_line_bytes);

        assert_eq!(batch.records.len(), 1);
        assert_eq!(batch.records[0].message.len(), max_line_bytes);
        assert!(batch.records[0].message.ends_with(LOG_TRUNCATION_MARKER));
    }

    #[test]
    fn delayed_old_epoch_cancellation_cannot_cancel_the_new_epoch() {
        let control = Arc::new(DelayedCancellationControl::default());
        let controller = Arc::new(
            NativeCoreProcessController::new(
                NativeCoreProcessConfig {
                    readiness_timeout: Duration::from_secs(1),
                    readiness_poll_interval: Duration::from_millis(10),
                    stop_timeout: Duration::from_secs(1),
                    log_capacity: 1,
                    max_log_line_bytes: 64,
                },
                Arc::clone(&control) as Arc<dyn CoreControlClient>,
                Arc::new(PsProcessInspector),
            )
            .expect("the cancellation fixture controller should initialize"),
        );
        controller.reset_apply_cancellation(1);

        std::thread::scope(|scope| {
            let cancelling_controller = Arc::clone(&controller);
            let cancellation = scope.spawn(move || {
                cancelling_controller.cancel_pending_apply(1);
            });
            control.wait_until_cancel_entered();

            controller.reset_apply_cancellation(2);
            control.release_cancel();
            cancellation
                .join()
                .expect("the delayed cancellation should finish");
        });

        assert!(!controller.apply_cancelled.load(Ordering::Acquire));
        controller.cancel_pending_apply(1);
        assert!(!controller.apply_cancelled.load(Ordering::Acquire));
    }

    #[test]
    fn readiness_poll_exits_promptly_after_apply_cancellation() {
        let child = Command::new("/bin/sleep")
            .arg("30")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("the readiness fixture child should start");
        let pid = child.id();
        let process_start_identity = PsProcessInspector
            .identity(pid)
            .expect("the readiness fixture identity should load")
            .expect("the readiness fixture should be live");
        let identity = OwnedProcessIdentity {
            pid,
            process_start_identity,
            instance_generation: CoreInstanceGeneration(1),
        };
        let endpoint = CoreControlEndpoint::new(
            PathBuf::from("/private/tmp/ratash-readiness-cancel-fixture.sock"),
            "fixture-secret",
        );
        let controller = NativeCoreProcessController::new(
            NativeCoreProcessConfig {
                readiness_timeout: Duration::from_secs(30),
                readiness_poll_interval: Duration::from_millis(50),
                stop_timeout: Duration::from_secs(1),
                log_capacity: 1,
                max_log_line_bytes: 64,
            },
            Arc::new(StartingControl),
            Arc::new(PsProcessInspector),
        )
        .expect("the readiness fixture controller should initialize");
        controller
            .state
            .lock()
            .expect("the controller state should lock")
            .child = Some(ManagedChild {
            child,
            ownership: ChildOwnership::Direct,
            identity: identity.clone(),
            endpoint: endpoint.clone(),
            exited: false,
            log_readers: None,
        });
        let controller = Arc::new(controller);
        controller.reset_apply_cancellation(1);

        std::thread::scope(|scope| {
            let worker_controller = Arc::clone(&controller);
            let worker_identity = identity.clone();
            let worker_endpoint = endpoint.clone();
            let worker = scope
                .spawn(move || worker_controller.readiness(&worker_identity, &worker_endpoint));
            std::thread::sleep(Duration::from_millis(20));
            let started = Instant::now();

            controller.cancel_pending_apply(1);

            let error = worker
                .join()
                .expect("the readiness fixture worker should finish")
                .expect_err("cancelled readiness should return an error");
            assert_eq!(error.kind, ServicePlatformErrorKind::ApplyCancelled);
            assert!(started.elapsed() < Duration::from_millis(200));
        });
    }

    #[test]
    fn exited_guardian_contains_its_still_running_exact_core() {
        let mut guardian = Command::new("/bin/sh")
            .arg("-c")
            .arg(
                "nohup /bin/sleep 30 >/dev/null 2>&1 & core_pid=$!; printf '%s\\n' \"$core_pid\"; wait \"$core_pid\"",
            )
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .expect("the guardian-exit fixture should start");
        let guardian_pid = guardian.id();
        let mut core_pid = String::new();
        BufReader::new(
            guardian
                .stdout
                .take()
                .expect("the guardian-exit fixture should publish its Core PID"),
        )
        .read_line(&mut core_pid)
        .expect("the guardian-exit Core PID should be readable");
        let core_pid = core_pid
            .trim()
            .parse::<u32>()
            .expect("the guardian-exit Core PID should parse");
        let process_start_identity = PsProcessInspector
            .identity(core_pid)
            .expect("the guardian-exit Core identity should load")
            .expect("the guardian-exit Core should be live");
        let identity = OwnedProcessIdentity {
            pid: core_pid,
            process_start_identity,
            instance_generation: CoreInstanceGeneration(1),
        };
        let mut fixture = GuardianExitFixture {
            guardian: Some(guardian),
            core_identity: identity.clone(),
        };
        let controller = NativeCoreProcessController::new(
            NativeCoreProcessConfig {
                stop_timeout: Duration::from_secs(1),
                ..NativeCoreProcessConfig::default()
            },
            Arc::new(StartingControl),
            Arc::new(PsProcessInspector),
        )
        .expect("the guardian-exit controller should initialize");
        controller
            .state
            .lock()
            .expect("the controller state should lock")
            .child = Some(ManagedChild {
            child: fixture
                .guardian
                .take()
                .expect("the guardian-exit child should transfer to the controller"),
            ownership: ChildOwnership::Guardian { control: None },
            identity: identity.clone(),
            endpoint: CoreControlEndpoint::new(
                PathBuf::from("/private/tmp/ratash-guardian-exit-fixture.sock"),
                "fixture-secret",
            ),
            exited: false,
            log_readers: None,
        });
        kill(
            Pid::from_raw(i32::try_from(guardian_pid).expect("guardian PID should fit i32")),
            Signal::SIGKILL,
        )
        .expect("the guardian-exit fixture should kill only the guardian");

        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            if controller
                .reap_if_exited(&identity)
                .expect("guardian exit inspection should succeed")
            {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "the guardian exit should become observable"
            );
            thread::sleep(Duration::from_millis(10));
        }
        while PsProcessInspector
            .identity(core_pid)
            .expect("the contained Core identity should remain inspectable")
            .as_deref()
            == Some(identity.process_start_identity.as_str())
        {
            assert!(
                Instant::now() < deadline,
                "the exited guardian's exact Core should be contained"
            );
            thread::sleep(Duration::from_millis(10));
        }
    }

    #[test]
    fn guardian_handshake_wait_exits_promptly_after_apply_cancellation() {
        let mut child = Command::new("/bin/sleep")
            .arg("30")
            .stdout(Stdio::piped())
            .spawn()
            .expect("the guardian handshake fixture child should start");
        let stdout = child
            .stdout
            .take()
            .expect("the guardian handshake fixture should expose stdout");
        let cancellation = AtomicBool::new(false);

        std::thread::scope(|scope| {
            let worker = scope
                .spawn(|| read_guardian_handshake(stdout, Duration::from_secs(30), &cancellation));
            std::thread::sleep(Duration::from_millis(20));
            let started = Instant::now();

            cancellation.store(true, Ordering::Release);

            let error = worker
                .join()
                .expect("the guardian handshake fixture worker should finish")
                .expect_err("the cancelled guardian handshake should return an error");
            assert_eq!(error.kind, ServicePlatformErrorKind::ApplyCancelled);
            assert!(started.elapsed() < Duration::from_millis(200));
        });

        child
            .kill()
            .expect("the guardian handshake fixture child should stop");
        child
            .wait()
            .expect("the guardian handshake fixture child should be reaped");
    }
}
