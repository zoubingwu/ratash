//! Owns bounded child-process and PTY measurement lifecycles.

use std::error::Error;
use std::io::{Read, Write};
use std::path::Path;
use std::process::{Child, Command, ExitStatus, Output, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use nix::sys::signal::{Signal, kill};
use nix::unistd::Pid;
use portable_pty::{CommandBuilder, PtySize, native_pty_system};
use serde_json::Value;

use hopash::tui_runtime::ShutdownSignal;

use super::CHILD_CLEANUP_TIMEOUT;
use super::reporting::{child_metric, elapsed_ms, invalid, resource_metric};
use super::runtime_support::{LifecycleGuard, curve_point, ensure_collection_running};

pub(super) struct ProcessChildGuard {
    child: Option<Child>,
}

impl ProcessChildGuard {
    pub(super) fn new(child: Child) -> Self {
        Self { child: Some(child) }
    }

    pub(super) fn id(&self) -> Result<u32, Box<dyn Error>> {
        self.child
            .as_ref()
            .map(Child::id)
            .ok_or_else(|| invalid("benchmark child is unavailable"))
    }

    #[cfg(test)]
    pub(super) fn into_child(mut self) -> Child {
        self.child
            .take()
            .expect("benchmark fixture child should be available")
    }

    pub(super) fn wait_with_output(
        mut self,
        timeout: Duration,
        signal: &dyn ShutdownSignal,
    ) -> Result<Output, Box<dyn Error>> {
        let child = self
            .child
            .as_mut()
            .ok_or_else(|| invalid("benchmark child is unavailable"))?;
        let deadline = Instant::now() + timeout;
        let status = loop {
            ensure_collection_running(signal)?;
            if let Some(status) = child.try_wait()? {
                break status;
            }
            if Instant::now() >= deadline {
                return Err(invalid("benchmark child exceeded its completion deadline"));
            }
            thread::sleep(Duration::from_millis(25));
        };
        let mut stdout = Vec::new();
        if let Some(mut pipe) = child.stdout.take() {
            pipe.read_to_end(&mut stdout)?;
        }
        let mut stderr = Vec::new();
        if let Some(mut pipe) = child.stderr.take() {
            pipe.read_to_end(&mut stderr)?;
        }
        self.child.take();
        Ok(Output {
            status,
            stdout,
            stderr,
        })
    }

    fn terminate_and_reap(&mut self) -> Result<Option<ExitStatus>, Box<dyn Error>> {
        self.terminate_and_reap_with_timeout(CHILD_CLEANUP_TIMEOUT)
    }

    pub(super) fn terminate_and_reap_with_timeout(
        &mut self,
        timeout: Duration,
    ) -> Result<Option<ExitStatus>, Box<dyn Error>> {
        let Some(child) = self.child.as_mut() else {
            return Ok(None);
        };
        let status = terminate_process_child(child, timeout)?;
        self.child.take();
        Ok(Some(status))
    }

    pub(super) fn terminate_and_collect_stderr(
        mut self,
    ) -> Result<(ExitStatus, String), Box<dyn Error>> {
        let child = self
            .child
            .as_mut()
            .ok_or_else(|| invalid("benchmark child is unavailable"))?;
        let status = terminate_process_child(child, CHILD_CLEANUP_TIMEOUT)?;
        let mut diagnostic = String::new();
        if let Some(mut stderr) = child.stderr.take() {
            stderr.read_to_string(&mut diagnostic)?;
        }
        self.child.take();
        Ok((status, diagnostic.trim().to_owned()))
    }
}

impl Drop for ProcessChildGuard {
    fn drop(&mut self) {
        let _ = self.terminate_and_reap();
    }
}

fn terminate_process_child(
    child: &mut Child,
    timeout: Duration,
) -> Result<ExitStatus, Box<dyn Error>> {
    if let Some(status) = child.try_wait()? {
        return Ok(status);
    }
    let deadline = Instant::now() + timeout;
    if let Ok(pid) = i32::try_from(child.id()) {
        let _ = kill(Pid::from_raw(pid), Signal::SIGTERM);
    }
    let graceful_deadline = Instant::now() + timeout / 2;
    loop {
        if let Some(status) = child.try_wait()? {
            return Ok(status);
        }
        if Instant::now() >= graceful_deadline {
            break;
        }
        thread::sleep(Duration::from_millis(25));
    }
    let _ = child.kill();
    loop {
        if let Some(status) = child.try_wait()? {
            return Ok(status);
        }
        if Instant::now() >= deadline {
            return Err(invalid("benchmark child did not exit after termination"));
        }
        thread::sleep(Duration::from_millis(25));
    }
}

pub(super) struct BackgroundProcessMetrics {
    pub(super) supervisor_idle_rss: f64,
    pub(super) service_idle_rss: f64,
    pub(super) combined_idle_rss: f64,
    pub(super) wakeups_per_second: f64,
    pub(super) supervisor_curve: Vec<Value>,
    pub(super) service_curve: Vec<Value>,
    pub(super) combined_curve: Vec<Value>,
}

pub(super) fn collect_background_process_metrics(
    resource_probe: &Path,
    supervisor_pid: u32,
    service_pid: u32,
    observation_seconds: u64,
    signal: &dyn ShutdownSignal,
) -> Result<BackgroundProcessMetrics, Box<dyn Error>> {
    let supervisor_pid = supervisor_pid.to_string();
    let service_pid = service_pid.to_string();
    let duration = observation_seconds.to_string();
    let wakeup_probe = ProcessChildGuard::new(
        Command::new(resource_probe)
            .args(["wakeups", &duration, &supervisor_pid, &service_pid])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?,
    );
    let started = Instant::now();
    let deadline = started + Duration::from_secs(observation_seconds);
    let supervisor_idle_rss =
        resource_metric(resource_probe, "rss", std::slice::from_ref(&supervisor_pid))?;
    let service_idle_rss =
        resource_metric(resource_probe, "rss", std::slice::from_ref(&service_pid))?;
    let combined_idle_rss = supervisor_idle_rss + service_idle_rss;
    let mut supervisor_curve = vec![curve_point(0, supervisor_idle_rss)];
    let mut service_curve = vec![curve_point(0, service_idle_rss)];
    let mut combined_curve = vec![curve_point(0, combined_idle_rss)];
    while Instant::now() < deadline {
        ensure_collection_running(signal)?;
        let supervisor_rss =
            resource_metric(resource_probe, "rss", std::slice::from_ref(&supervisor_pid))?;
        let service_rss =
            resource_metric(resource_probe, "rss", std::slice::from_ref(&service_pid))?;
        let elapsed = u64::try_from(started.elapsed().as_millis())?;
        supervisor_curve.push(curve_point(elapsed, supervisor_rss));
        service_curve.push(curve_point(elapsed, service_rss));
        combined_curve.push(curve_point(elapsed, supervisor_rss + service_rss));
        thread::sleep(
            Duration::from_secs(1).min(deadline.saturating_duration_since(Instant::now())),
        );
    }
    let wakeups_per_second = child_metric(wakeup_probe, "background wakeup probe", signal)?;
    Ok(BackgroundProcessMetrics {
        supervisor_idle_rss,
        service_idle_rss,
        combined_idle_rss,
        wakeups_per_second,
        supervisor_curve,
        service_curve,
        combined_curve,
    })
}

pub(super) struct TuiProcessMetrics {
    pub(super) cold_start_ms: f64,
    pub(super) idle_rss: f64,
    pub(super) peak_memory: f64,
    pub(super) rss_curve: Vec<Value>,
}

pub(super) struct PtyChildGuard {
    child: Option<Box<dyn portable_pty::Child + Send + Sync>>,
}

impl PtyChildGuard {
    pub(super) fn new(child: Box<dyn portable_pty::Child + Send + Sync>) -> Self {
        Self { child: Some(child) }
    }

    pub(super) fn process_id(&self) -> Option<u32> {
        self.child.as_ref().and_then(|child| child.process_id())
    }

    fn terminate_and_reap(
        &mut self,
        timeout: Duration,
    ) -> Result<Option<portable_pty::ExitStatus>, Box<dyn Error>> {
        let Some(child) = self.child.as_mut() else {
            return Ok(None);
        };
        if let Some(status) = child.try_wait()? {
            self.child.take();
            return Ok(Some(status));
        }
        let _ = child.kill();
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(status) = child.try_wait()? {
                self.child.take();
                return Ok(Some(status));
            }
            if Instant::now() >= deadline {
                return Err(invalid("Status Interface did not exit after termination"));
            }
            thread::sleep(Duration::from_millis(25));
        }
    }

    fn wait_bounded(
        &mut self,
        graceful_timeout: Duration,
    ) -> Result<portable_pty::ExitStatus, Box<dyn Error>> {
        let deadline = Instant::now() + graceful_timeout;
        loop {
            let child = self
                .child
                .as_mut()
                .ok_or_else(|| invalid("Status Interface child is unavailable"))?;
            if let Some(status) = child.try_wait()? {
                self.child.take();
                return Ok(status);
            }
            if Instant::now() >= deadline {
                break;
            }
            thread::sleep(Duration::from_millis(25));
        }

        self.terminate_and_reap(CHILD_CLEANUP_TIMEOUT)?
            .ok_or_else(|| invalid("Status Interface child is unavailable"))
    }
}

impl Drop for PtyChildGuard {
    fn drop(&mut self) {
        let _ = self.terminate_and_reap(CHILD_CLEANUP_TIMEOUT);
    }
}

pub(super) fn collect_tui_process_metrics(
    lifecycle: &LifecycleGuard,
    resource_probe: &Path,
    observation_seconds: u64,
    signal: &dyn ShutdownSignal,
) -> Result<TuiProcessMetrics, Box<dyn Error>> {
    let pair = native_pty_system().openpty(PtySize {
        rows: 40,
        cols: 120,
        pixel_width: 0,
        pixel_height: 0,
    })?;
    let mut command = CommandBuilder::new(&lifecycle.release_binary);
    command.arg("status");
    command.env("HOPASH_STATE_DIR", &lifecycle.state_root);
    command.env("HOPASH_CORE_SERVICE_SOCKET", &lifecycle.service_socket);
    command.env("HOPASH_MIHOMO_PATH", &lifecycle.mihomo);
    command.env("TERM", "xterm-256color");
    let start = Instant::now();
    let mut child = PtyChildGuard::new(pair.slave.spawn_command(command)?);
    let pid = child
        .process_id()
        .ok_or_else(|| invalid("Status Interface PTY did not expose a process ID"))?;
    drop(pair.slave);
    let mut reader = pair.master.try_clone_reader()?;
    let mut writer = pair.master.take_writer()?;
    let (sender, receiver) = mpsc::channel();
    let reader_thread = thread::spawn(move || {
        let mut buffer = [0_u8; 8 * 1_024];
        loop {
            match reader.read(&mut buffer) {
                Ok(0) | Err(_) => break,
                Ok(read) => {
                    if sender.send(buffer[..read].to_vec()).is_err() {
                        break;
                    }
                }
            }
        }
    });
    let result = (|| -> Result<TuiProcessMetrics, Box<dyn Error>> {
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut output = Vec::new();
        while !output
            .windows("Overview".len())
            .any(|bytes| bytes == b"Overview")
        {
            ensure_collection_running(signal)?;
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                let tail = &output[output.len().saturating_sub(1_024)..];
                return Err(invalid(format!(
                    "Status Interface did not render its first frame: {}",
                    String::from_utf8_lossy(tail).escape_default()
                )));
            }
            match receiver.recv_timeout(remaining.min(Duration::from_millis(100))) {
                Ok(chunk) => output.extend_from_slice(&chunk),
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    let tail = &output[output.len().saturating_sub(1_024)..];
                    return Err(invalid(format!(
                        "Status Interface PTY closed before its first frame: {}",
                        String::from_utf8_lossy(tail).escape_default()
                    )));
                }
            }
        }
        let cold_start_ms = elapsed_ms(start);
        let idle_rss = resource_metric(resource_probe, "rss", &[pid.to_string()])?;
        let observation_deadline = Instant::now() + Duration::from_secs(observation_seconds);
        let observation_start = Instant::now();
        let mut peak_rss = idle_rss;
        let mut rss_curve = vec![curve_point(0, idle_rss)];
        while Instant::now() < observation_deadline {
            ensure_collection_running(signal)?;
            while receiver.try_recv().is_ok() {}
            let current_rss = resource_metric(resource_probe, "rss", &[pid.to_string()])?;
            peak_rss = peak_rss.max(current_rss);
            rss_curve.push(curve_point(
                u64::try_from(observation_start.elapsed().as_millis())?,
                current_rss,
            ));
            thread::sleep(
                Duration::from_secs(1)
                    .min(observation_deadline.saturating_duration_since(Instant::now())),
            );
        }
        writer.write_all(b"q")?;
        writer.flush()?;
        let status = child.wait_bounded(Duration::from_secs(5))?;
        if status.exit_code() != 0 {
            return Err(invalid("Status Interface exited unsuccessfully"));
        }
        Ok(TuiProcessMetrics {
            cold_start_ms,
            idle_rss,
            peak_memory: peak_rss,
            rss_curve,
        })
    })();
    let cleanup = if result.is_err() {
        child.terminate_and_reap(CHILD_CLEANUP_TIMEOUT)
    } else {
        Ok(None)
    };
    drop(writer);
    drop(pair.master);
    let cleanup_failed = cleanup.is_err();
    drop(child);
    if cleanup_failed {
        drop(reader_thread);
    } else if reader_thread.join().is_err() {
        return Err(invalid("Status Interface PTY reader failed"));
    }
    match (result, cleanup) {
        (Ok(metrics), Ok(_)) => Ok(metrics),
        (Err(error), Ok(_)) => Err(error),
        (Err(error), Err(cleanup_error)) => Err(invalid(format!(
            "{error}; Status Interface cleanup failed: {cleanup_error}"
        ))),
        (Ok(_), Err(cleanup_error)) => Err(cleanup_error),
    }
}
