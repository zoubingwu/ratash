//! Collects background-process and Status Interface measurements.

use std::error::Error;
use std::io::{Read, Write};
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use portable_pty::{CommandBuilder, PtySize, native_pty_system};
use serde_json::Value;

use ratash::tui_runtime::ShutdownSignal;

use super::CHILD_CLEANUP_TIMEOUT;
use super::process_support::{ProcessChildGuard, PtyChildGuard};
use super::reporting::{child_metric, resource_metric};
use super::runtime_support::{LifecycleGuard, curve_point};
use super::support::{elapsed_ms, ensure_collection_running, invalid};

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
    command.env("RATASH_STATE_DIR", &lifecycle.state_root);
    command.env("RATASH_CORE_SERVICE_SOCKET", &lifecycle.service_socket);
    command.env("RATASH_MIHOMO_PATH", &lifecycle.mihomo);
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
