use std::collections::VecDeque;
use std::io::Read;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::constants::{CORE_LOG_LINE_MAX_BYTES, CORE_READINESS_TIMEOUT, LOG_CAPACITY};
use crate::core::{CoreControlEndpoint, MihomoAdapter, MihomoReadiness, ProcessOutputSource};
use crate::lifecycle::{ProcessInspector, PsProcessInspector};
use crate::mihomo::UnixMihomoAdapter;
use crate::service::{
    CoreProcessController, CoreProcessLog, OwnedProcessIdentity, ProcessIdentityProbe,
    ServicePlatformError, ServicePlatformErrorKind, SpawnedCoreProcess, TunCapabilityPreflight,
    VerifiedRuntimeBundle,
};

const PROCESS_IDENTITY_ATTEMPTS: usize = 20;
const PROCESS_IDENTITY_RETRY: Duration = Duration::from_millis(10);

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

#[derive(Clone, Copy, Debug, Default)]
pub struct UnixCoreControlClient {
    adapter: UnixMihomoAdapter,
}

impl UnixCoreControlClient {
    #[must_use]
    pub const fn new(adapter: UnixMihomoAdapter) -> Self {
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
    identity: OwnedProcessIdentity,
    endpoint: CoreControlEndpoint,
}

#[derive(Default)]
struct ControllerState {
    child: Option<ManagedChild>,
}

struct LogQueue {
    generation: Option<crate::domain::CoreInstanceGeneration>,
    capacity: usize,
    records: VecDeque<CoreProcessLog>,
}

impl LogQueue {
    fn new(capacity: usize) -> Self {
        Self {
            generation: None,
            capacity,
            records: VecDeque::with_capacity(capacity),
        }
    }

    fn begin_generation(&mut self, generation: crate::domain::CoreInstanceGeneration) {
        self.generation = Some(generation);
        self.records.clear();
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
        }
        self.records.push_back(CoreProcessLog {
            timestamp_unix_ms: now_unix_ms(),
            source,
            message,
        });
    }
}

pub struct NativeCoreProcessController {
    config: NativeCoreProcessConfig,
    control: Arc<dyn CoreControlClient>,
    inspector: Arc<dyn ProcessInspector>,
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
            state: Mutex::new(ControllerState::default()),
            logs: Arc::new(Mutex::new(LogQueue::new(config.log_capacity))),
        })
    }

    #[must_use]
    pub fn product_defaults() -> Self {
        Self::new(
            NativeCoreProcessConfig::default(),
            Arc::new(UnixCoreControlClient::default()),
            Arc::new(PsProcessInspector),
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
        if managed
            .child
            .try_wait()
            .map_err(|_| platform_error(ServicePlatformErrorKind::ProcessInspection))?
            .is_some()
        {
            return Err(platform_error(ServicePlatformErrorKind::ProcessInspection));
        }
        Ok(managed)
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
    ) {
        let logs = Arc::clone(&self.logs);
        let max_line_bytes = self.config.max_log_line_bytes;
        thread::spawn(move || {
            collect_log_stream(reader, source, generation, logs, max_line_bytes);
        });
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
            let status = existing
                .child
                .try_wait()
                .map_err(|_| platform_error(ServicePlatformErrorKind::ProcessInspection))?;
            if status.is_none() {
                return Err(platform_error(ServicePlatformErrorKind::Spawn));
            }
            state.child = None;
        }

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
        let mut child = command
            .spawn()
            .map_err(|_| platform_error(ServicePlatformErrorKind::Spawn))?;
        let pid = child.id();
        let process_start_identity = match self.discover_identity(pid) {
            Ok(identity) => identity,
            Err(error) => {
                let _ = child.kill();
                let _ = child.wait();
                return Err(error);
            }
        };
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| platform_error(ServicePlatformErrorKind::Spawn))?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| platform_error(ServicePlatformErrorKind::Spawn))?;
        self.logs
            .lock()
            .map_err(|_| platform_error(ServicePlatformErrorKind::Logs))?
            .begin_generation(instance_generation);
        self.start_log_reader(stdout, ProcessOutputSource::Stdout, instance_generation);
        self.start_log_reader(stderr, ProcessOutputSource::Stderr, instance_generation);
        let identity = OwnedProcessIdentity {
            pid,
            process_start_identity: process_start_identity.clone(),
            instance_generation,
        };
        state.child = Some(ManagedChild {
            child,
            identity,
            endpoint: endpoint.clone(),
        });
        Ok(SpawnedCoreProcess {
            pid,
            process_start_identity,
        })
    }

    fn reload(
        &self,
        process: &OwnedProcessIdentity,
        bundle: &VerifiedRuntimeBundle,
    ) -> Result<(), ServicePlatformError> {
        let mut state = self.lock_state()?;
        let managed = Self::verify_managed(&mut state, process)?;
        self.control
            .reload(&managed.endpoint, bundle.configuration_path())
    }

    fn stop(&self, process: &OwnedProcessIdentity) -> Result<(), ServicePlatformError> {
        let mut state = self.lock_state()?;
        let managed = Self::verify_managed(&mut state, process)?;
        managed
            .child
            .kill()
            .map_err(|_| platform_error(ServicePlatformErrorKind::Stop))?;
        let deadline = Instant::now()
            .checked_add(self.config.stop_timeout)
            .ok_or_else(|| platform_error(ServicePlatformErrorKind::Stop))?;
        loop {
            if managed
                .child
                .try_wait()
                .map_err(|_| platform_error(ServicePlatformErrorKind::Stop))?
                .is_some()
            {
                break;
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(platform_error(ServicePlatformErrorKind::Stop));
            }
            thread::sleep(remaining.min(Duration::from_millis(10)));
        }
        self.logs
            .lock()
            .map_err(|_| platform_error(ServicePlatformErrorKind::Logs))?
            .finish_generation(process.instance_generation);
        state.child = None;
        Ok(())
    }

    fn readiness(
        &self,
        process: &OwnedProcessIdentity,
        endpoint: &CoreControlEndpoint,
    ) -> Result<(), ServicePlatformError> {
        let mut state = self.lock_state()?;
        let managed = Self::verify_managed(&mut state, process)?;
        if managed.endpoint != *endpoint {
            return Err(platform_error(ServicePlatformErrorKind::Readiness));
        }
        let deadline = Instant::now()
            .checked_add(self.config.readiness_timeout)
            .ok_or_else(|| platform_error(ServicePlatformErrorKind::ReadinessTimeout))?;
        loop {
            if managed
                .child
                .try_wait()
                .map_err(|_| platform_error(ServicePlatformErrorKind::ProcessInspection))?
                .is_some()
            {
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

    fn take_logs(
        &self,
        process: &OwnedProcessIdentity,
        limit: usize,
    ) -> Result<Vec<CoreProcessLog>, ServicePlatformError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let mut state = self.lock_state()?;
        Self::verify_managed(&mut state, process)?;
        let mut logs = self
            .logs
            .lock()
            .map_err(|_| platform_error(ServicePlatformErrorKind::Logs))?;
        let take = limit.min(logs.capacity).min(logs.records.len());
        Ok(logs.records.drain(..take).collect())
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
        let _ = managed.child.kill();
        let _ = managed.child.wait();
        if let Ok(mut logs) = self.logs.lock() {
            logs.finish_generation(managed.identity.instance_generation);
        }
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
