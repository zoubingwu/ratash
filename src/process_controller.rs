use std::collections::VecDeque;
use std::fs;
use std::io::Read;
use std::os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt, chown};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStderr, ChildStdin, ChildStdout, Command, Stdio};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use nix::sys::signal::{Signal, kill};
use nix::unistd::Pid;

use crate::constants::{CORE_LOG_LINE_MAX_BYTES, CORE_READINESS_TIMEOUT, LOG_CAPACITY};
use crate::core::{CoreControlEndpoint, MihomoAdapter, MihomoReadiness, ProcessOutputSource};
use crate::core_guardian::{CoreGuardianHandshake, CoreGuardianInvocation, read_handshake};
use crate::lifecycle::{ProcessInspector, PsProcessInspector};
use crate::mihomo::UnixMihomoAdapter;
use crate::service::{
    CoreProcessController, CoreProcessLog, CoreProcessLogBatch, OwnedProcessIdentity,
    ProcessIdentityProbe, ServicePlatformError, ServicePlatformErrorKind, SpawnedCoreProcess,
    TunCapabilityPreflight, VerifiedRuntimeBundle,
};

const PROCESS_IDENTITY_ATTEMPTS: usize = 20;
const PROCESS_IDENTITY_RETRY: Duration = Duration::from_millis(10);
const PROCESS_WAIT_POLL_INTERVAL: Duration = Duration::from_millis(10);

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
            stop_timeout: Duration::from_secs(5),
            log_capacity: LOG_CAPACITY,
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

struct LogQueue {
    generation: Option<crate::domain::CoreInstanceGeneration>,
    capacity: usize,
    records: VecDeque<CoreProcessLog>,
    dropped: u64,
}

impl LogQueue {
    fn new(capacity: usize) -> Self {
        Self {
            generation: None,
            capacity,
            records: VecDeque::with_capacity(capacity),
            dropped: 0,
        }
    }

    fn begin_generation(&mut self, generation: crate::domain::CoreInstanceGeneration) {
        self.generation = Some(generation);
        self.records.clear();
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
        if self.records.len() == self.capacity {
            self.records.pop_front();
            self.dropped = self.dropped.saturating_add(1);
        }
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
            self.records.drain(..excess);
            self.dropped = self
                .dropped
                .saturating_add(u64::try_from(excess).unwrap_or(u64::MAX));
        }
        CoreProcessLogBatch {
            records: self.records.drain(..).collect(),
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

    #[must_use]
    pub fn product_defaults() -> Self {
        Self::new_guarded(
            NativeCoreProcessConfig::default(),
            Arc::new(UnixCoreControlClient::default()),
            Arc::new(PsProcessInspector),
            std::env::current_exe().expect("product Core guardian executable"),
        )
        .expect("product Core process controller settings")
    }

    fn lock_state(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, ControllerState>, ServicePlatformError> {
        self.state
            .lock()
            .map_err(|_| platform_error(ServicePlatformErrorKind::ProcessInspection))
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
        let mut child = Command::new(bundle.executable_path())
            .arg("-d")
            .arg(generation_root)
            .arg("-f")
            .arg(bundle.configuration_path())
            .arg("-ext-ctl-unix")
            .arg(&endpoint.socket_path)
            .current_dir(generation_root)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
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
        let (handshake, stdout) =
            match read_guardian_handshake(stdout, self.config.readiness_timeout) {
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
) -> Result<(CoreGuardianHandshake, ChildStdout), ServicePlatformError> {
    let (sender, receiver) = mpsc::sync_channel(1);
    thread::Builder::new()
        .name("hopash-guardian-handshake".to_owned())
        .spawn(move || {
            let result = read_handshake(&mut stdout);
            let _ = sender.send((result, stdout));
        })
        .map_err(|_| platform_error(ServicePlatformErrorKind::Spawn))?;
    match receiver.recv_timeout(timeout) {
        Ok((Ok(handshake), stdout)) => Ok((handshake, stdout)),
        Ok((Err(_), _))
        | Err(mpsc::RecvTimeoutError::Timeout | mpsc::RecvTimeoutError::Disconnected) => {
            Err(platform_error(ServicePlatformErrorKind::Spawn))
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
                return Ok(());
            }
            terminate_exact_process(inspector, &managed.identity)?;
        }
    }
    if wait_for_child_exit(&mut managed.child, deadline)? {
        return Ok(());
    }
    managed
        .child
        .kill()
        .map_err(|_| platform_error(ServicePlatformErrorKind::Stop))?;
    let forced_deadline = Instant::now()
        .checked_add(timeout.min(Duration::from_millis(250)))
        .ok_or_else(|| platform_error(ServicePlatformErrorKind::Stop))?;
    if wait_for_child_exit(&mut managed.child, forced_deadline)? {
        Ok(())
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

impl CoreProcessController for NativeCoreProcessController {
    fn spawn(
        &self,
        bundle: &VerifiedRuntimeBundle,
        endpoint: &CoreControlEndpoint,
        instance_generation: crate::domain::CoreInstanceGeneration,
    ) -> Result<SpawnedCoreProcess, ServicePlatformError> {
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
        let mut state = self.lock_state()?;
        let managed = self.verify_managed(&mut state, process)?;
        self.control
            .reload(&managed.endpoint, bundle.configuration_path())
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
        let mut state = self.lock_state()?;
        let managed = self.verify_managed(&mut state, process)?;
        if managed.endpoint != *endpoint {
            return Err(platform_error(ServicePlatformErrorKind::Readiness));
        }
        let deadline = Instant::now()
            .checked_add(self.config.readiness_timeout)
            .ok_or_else(|| platform_error(ServicePlatformErrorKind::ReadinessTimeout))?;
        loop {
            if self.observe_exit(managed)? {
                return Err(platform_error(ServicePlatformErrorKind::Readiness));
            }
            match self.control.readiness(endpoint) {
                Ok(MihomoReadiness::Ready) => return Ok(()),
                Ok(MihomoReadiness::Starting) | Err(_) => {}
            }
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
}

impl Drop for NativeCoreProcessController {
    fn drop(&mut self) {
        let Ok(state) = self.state.get_mut() else {
            return;
        };
        let Some(mut managed) = state.child.take() else {
            return;
        };
        if managed.exited {
            let _ = self.finish_managed_logs(&mut managed);
            return;
        }
        let guardian = matches!(managed.ownership, ChildOwnership::Guardian { .. });
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
pub struct RootTunCapabilityPreflight;

impl TunCapabilityPreflight for RootTunCapabilityPreflight {
    fn check(&self, _owner_uid: u32) -> Result<(), ServicePlatformError> {
        if nix::unistd::Uid::effective().is_root() {
            Ok(())
        } else {
            Err(platform_error(ServicePlatformErrorKind::TunUnavailable))
        }
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
    loop {
        let read = match reader.read(&mut chunk) {
            Ok(0) | Err(_) => break,
            Ok(read) => read,
        };
        for byte in &chunk[..read] {
            if *byte == b'\n' {
                publish_log_line(&logs, generation, source, &mut line);
            } else if line.len() < max_line_bytes {
                line.push(*byte);
            }
        }
    }
    if !line.is_empty() {
        publish_log_line(&logs, generation, source, &mut line);
    }
}

fn publish_log_line(
    logs: &Mutex<LogQueue>,
    generation: crate::domain::CoreInstanceGeneration,
    source: ProcessOutputSource,
    line: &mut Vec<u8>,
) {
    if line.last() == Some(&b'\r') {
        line.pop();
    }
    let message = String::from_utf8_lossy(line).into_owned();
    line.clear();
    if let Ok(mut logs) = logs.lock() {
        logs.push(generation, source, message);
    }
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
}
