//! Provides exact-process cleanup and fixture command support.

use std::collections::VecDeque;
use std::env;
use std::error::Error;
use std::io;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use nix::sys::signal::{Signal, kill};
use nix::unistd::Pid;
use serde_json::{Value, json};

use hopash::constants::TRAFFIC_SERIES_CAPACITY;
use hopash::lifecycle::{ProcessIdentity, ProcessInspector, PsProcessInspector};
use hopash::telemetry::LogLevel;
use hopash::tui::Page;
use hopash::tui_runtime::ShutdownSignal;

use super::process_metrics::ProcessChildGuard;
use super::profile_server::wait_for_socket;
use super::reporting::{elapsed_ms, invalid};
use super::{CHILD_CLEANUP_TIMEOUT, SERVICE_CLEANUP_TIMEOUT, WorkloadScale};

pub(super) fn curve_point(elapsed_ms: u64, value: f64) -> Value {
    json!({
        "elapsed_ms": elapsed_ms,
        "value": value
    })
}

pub(super) fn ensure_collection_running(signal: &dyn ShutdownSignal) -> Result<(), Box<dyn Error>> {
    if signal.shutdown_requested() {
        Err(invalid("release benchmark collection was interrupted"))
    } else {
        Ok(())
    }
}

pub(super) fn parse_log_level(value: &str) -> Result<LogLevel, Box<dyn Error>> {
    match value {
        "debug" => Ok(LogLevel::Debug),
        "info" => Ok(LogLevel::Info),
        "warn" => Ok(LogLevel::Warn),
        "error" => Ok(LogLevel::Error),
        _ => Err(invalid("Core Log level is unsupported")),
    }
}

pub(super) fn parse_page(value: &str) -> Result<Page, Box<dyn Error>> {
    match value {
        "overview" => Ok(Page::Overview),
        "proxies" => Ok(Page::Proxies),
        "profiles" => Ok(Page::Profiles),
        "logs" => Ok(Page::Logs),
        _ => Err(invalid("TUI event page is unsupported")),
    }
}

pub(super) fn push_bounded_series(series: &mut VecDeque<u64>, value: u64) {
    if series.len() == TRAFFIC_SERIES_CAPACITY {
        series.pop_front();
    }
    series.push_back(value);
}

pub(super) struct LifecycleGuard {
    pub(super) release_binary: PathBuf,
    pub(super) fixture_binary: PathBuf,
    pub(super) state_root: PathBuf,
    pub(super) service_socket: PathBuf,
    pub(super) mihomo: PathBuf,
    pub(super) supervisor: Option<ProcessIdentity>,
    pub(super) service: Option<ProcessChildGuard>,
}

impl LifecycleGuard {
    pub(super) fn run(&self, arguments: &[&str]) -> Result<std::process::Output, io::Error> {
        self.run_with(&self.release_binary, arguments)
    }

    pub(super) fn run_fixture(
        &self,
        arguments: &[&str],
    ) -> Result<std::process::Output, io::Error> {
        self.run_with(&self.fixture_binary, arguments)
    }

    fn run_with(
        &self,
        binary: &Path,
        arguments: &[&str],
    ) -> Result<std::process::Output, io::Error> {
        Command::new(binary)
            .args(arguments)
            .env("HOPASH_STATE_DIR", &self.state_root)
            .env("HOPASH_CORE_SERVICE_SOCKET", &self.service_socket)
            .env("HOPASH_MIHOMO_PATH", &self.mihomo)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
    }

    pub(super) fn shutdown(&mut self) -> Result<(), Box<dyn Error>> {
        let mut failures = Vec::new();
        if let Some(supervisor) = self.supervisor.clone() {
            match self.run(&["stop", "--json"]) {
                Ok(output) if output.status.success() => {}
                Ok(output) => failures.push(format!(
                    "fixture Supervisor stop failed: {}",
                    String::from_utf8_lossy(&output.stderr).trim()
                )),
                Err(error) => failures.push(format!("fixture Supervisor stop failed: {error}")),
            }
            if let Err(error) = terminate_exact_process(&supervisor) {
                failures.push(format!("fixture Supervisor cleanup failed: {error}"));
            } else {
                self.supervisor = None;
            }
        }
        if let Some(mut service) = self.service.take()
            && let Err(error) = service.terminate_and_reap_with_timeout(SERVICE_CLEANUP_TIMEOUT)
        {
            failures.push(format!("fixture Core service cleanup failed: {error}"));
        }
        if failures.is_empty() {
            Ok(())
        } else {
            Err(invalid(failures.join("; ")))
        }
    }
}

impl Drop for LifecycleGuard {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}

fn terminate_exact_process(process: &ProcessIdentity) -> Result<(), Box<dyn Error>> {
    if wait_for_exact_process_exit(process, Duration::from_millis(250))? {
        return Ok(());
    }
    signal_exact_process(process, Signal::SIGTERM)?;
    if wait_for_exact_process_exit(process, CHILD_CLEANUP_TIMEOUT / 2)? {
        return Ok(());
    }
    signal_exact_process(process, Signal::SIGKILL)?;
    if wait_for_exact_process_exit(process, CHILD_CLEANUP_TIMEOUT / 2)? {
        Ok(())
    } else {
        Err(invalid("fixture Supervisor did not exit after termination"))
    }
}

fn signal_exact_process(process: &ProcessIdentity, signal: Signal) -> Result<(), Box<dyn Error>> {
    if PsProcessInspector.identity(process.pid)?.as_deref() != Some(process.start_identity.as_str())
    {
        return Ok(());
    }
    let pid = i32::try_from(process.pid)?;
    if let Err(error) = kill(Pid::from_raw(pid), signal) {
        if PsProcessInspector.identity(process.pid)?.as_deref()
            != Some(process.start_identity.as_str())
        {
            return Ok(());
        }
        return Err(Box::new(error));
    }
    Ok(())
}

fn wait_for_exact_process_exit(
    process: &ProcessIdentity,
    timeout: Duration,
) -> Result<bool, Box<dyn Error>> {
    let deadline = Instant::now() + timeout;
    loop {
        if PsProcessInspector.identity(process.pid)?.as_deref()
            != Some(process.start_identity.as_str())
        {
            return Ok(true);
        }
        if Instant::now() >= deadline {
            return Ok(false);
        }
        thread::sleep(Duration::from_millis(25));
    }
}

pub(super) fn spawn_fixture_service(
    socket: &Path,
    runtime_root: &Path,
    owner_uid: u32,
    mihomo: &Path,
    guardian: &Path,
    active_nodes: u64,
) -> Result<ProcessChildGuard, Box<dyn Error>> {
    let service = Command::new(env::current_exe()?)
        .arg("fixture-core-service")
        .arg(socket)
        .arg(runtime_root)
        .arg(owner_uid.to_string())
        .arg(mihomo)
        .arg(guardian)
        .env("HOPASH_BENCHMARK_ACTIVE_NODES", active_nodes.to_string())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()?;
    let service = ProcessChildGuard::new(service);
    if let Err(wait_error) = wait_for_socket(socket, Duration::from_secs(10)) {
        let (status, diagnostic) = service.terminate_and_collect_stderr()?;
        return Err(invalid(format!(
            "{wait_error}; fixture Core service status={status:?} stderr={}",
            diagnostic
        )));
    }
    Ok(service)
}

pub(super) fn wait_for_active_status(
    lifecycle: &LifecycleGuard,
    timeout: Duration,
    signal: &dyn ShutdownSignal,
) -> Result<(), Box<dyn Error>> {
    let deadline = Instant::now() + timeout;
    let mut latest = Value::Null;
    loop {
        ensure_collection_running(signal)?;
        let status = lifecycle.run(&["status", "--json"])?;
        if status.status.success() {
            latest = serde_json::from_slice(&status.stdout)?;
            if !latest["data"]["active_profile"].is_null()
                && latest["data"]["core"]["lifecycle"] == "ready"
            {
                return Ok(());
            }
        }
        if Instant::now() >= deadline {
            return Err(invalid(format!(
                "fixture cold start did not expose its Active Profile and Managed Core: {latest}"
            )));
        }
        thread::sleep(Duration::from_millis(50));
    }
}

pub(super) fn validate_release_application_scale(
    lifecycle: &LifecycleGuard,
    scale: WorkloadScale,
    signal: &dyn ShutdownSignal,
) -> Result<f64, Box<dyn Error>> {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        ensure_collection_running(signal)?;
        let status = command_json(
            lifecycle.run(&["status", "--json"])?,
            "release-scale status",
        )?;
        if status["data"]["probe_queue"]["active_node_count"] == scale.active_nodes {
            break;
        }
        if Instant::now() >= deadline {
            return Err(invalid(
                "release-scale Active Nodes did not reach the background Probe Queue",
            ));
        }
        thread::sleep(Duration::from_millis(50));
    }

    let profiles = command_json(
        lifecycle.run(&["profile", "list", "--json"])?,
        "release-scale Profile List",
    )?;
    if profiles["data"]["profiles"].as_array().map(Vec::len)
        != Some(usize::try_from(scale.profiles)?)
    {
        return Err(invalid(
            "release-scale Profiles did not traverse the public IPC application seam",
        ));
    }
    let proxies = wait_for_proxy_list(lifecycle, "PROXY", Duration::from_secs(10), signal)?;
    if proxies["data"]["nodes"].as_array().map(Vec::len)
        != Some(usize::try_from(scale.active_nodes)?)
    {
        return Err(invalid(
            "release-scale Active Nodes did not traverse the public IPC application seam",
        ));
    }
    let rules = command_json(
        lifecycle.run(&["rule", "list", "--json"])?,
        "release-scale Local Rule Set",
    )?;
    if rules["data"]["rules"].as_array().map(Vec::len) != Some(usize::try_from(scale.local_rules)?)
    {
        return Err(invalid(
            "release-scale Local Rules did not traverse the public IPC application seam",
        ));
    }

    let last_rule = scale
        .local_rules
        .checked_sub(1)
        .ok_or_else(|| invalid("release-scale Local Rule Set must be non-empty"))?;
    let old_rule = format!("DOMAIN-SUFFIX,rule-{last_rule:05}.example.invalid,PROXY");
    let new_rule = format!("DOMAIN-SUFFIX,rule-{last_rule:05}.example.invalid,DIRECT");
    let mutation_ms = measure_rule_mutation(lifecycle, &old_rule, &new_rule, signal)?;
    let _ = wait_for_proxy_list(lifecycle, "PROXY", Duration::from_secs(10), signal)?;
    Ok(mutation_ms)
}

fn measure_rule_mutation(
    lifecycle: &LifecycleGuard,
    old_rule: &str,
    new_rule: &str,
    signal: &dyn ShutdownSignal,
) -> Result<f64, Box<dyn Error>> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        ensure_collection_running(signal)?;
        let mutation_start = Instant::now();
        let output = lifecycle.run(&["rule", "replace", old_rule, new_rule, "--json"])?;
        if output.status.success() {
            let _: Value = serde_json::from_slice(&output.stdout)?;
            return Ok(elapsed_ms(mutation_start));
        }
        let diagnostic = command_diagnostic(&output);
        if !is_retryable_error(&output.stderr, "rule_busy") || Instant::now() >= deadline {
            return Err(invalid(format!(
                "release-scale Rule Mutation failed: {diagnostic}"
            )));
        }
        thread::sleep(Duration::from_millis(50));
    }
}

fn wait_for_proxy_list(
    lifecycle: &LifecycleGuard,
    group: &str,
    timeout: Duration,
    signal: &dyn ShutdownSignal,
) -> Result<Value, Box<dyn Error>> {
    let deadline = Instant::now() + timeout;
    loop {
        ensure_collection_running(signal)?;
        let output = lifecycle.run(&["proxy", "list", group, "--json"])?;
        if output.status.success() {
            return Ok(serde_json::from_slice(&output.stdout)?);
        }
        let latest_error = command_diagnostic(&output);
        let retryable = is_retryable_error(&output.stderr, "core_unavailable");
        if !retryable || Instant::now() >= deadline {
            let status = lifecycle.run(&["status", "--json"])?;
            return Err(invalid(format!(
                "release-scale Proxy List failed: {latest_error}; fixture status after proxy failure: {}",
                command_diagnostic(&status)
            )));
        }
        thread::sleep(Duration::from_millis(50));
    }
}

pub(super) fn is_retryable_error(stderr: &[u8], code: &str) -> bool {
    serde_json::from_slice::<Value>(stderr)
        .ok()
        .is_some_and(|error| error["error"]["code"] == code && error["error"]["retryable"] == true)
}

pub(super) fn command_json(
    output: std::process::Output,
    label: &str,
) -> Result<Value, Box<dyn Error>> {
    if !output.status.success() {
        return Err(invalid(format!(
            "{label} failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }
    Ok(serde_json::from_slice(&output.stdout)?)
}

fn command_diagnostic(output: &Output) -> String {
    let bytes = if output.stdout.is_empty() {
        &output.stderr
    } else {
        &output.stdout
    };
    String::from_utf8_lossy(bytes)
        .trim()
        .chars()
        .flat_map(char::escape_default)
        .collect()
}
