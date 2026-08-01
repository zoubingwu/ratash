use hopash::config::ConfigCompiler;
use hopash::constants::{
    CORE_LOG_LINE_MAX_BYTES, LOG_CAPACITY, PROBE_WORKER_COUNT, TRAFFIC_SERIES_CAPACITY,
};
use hopash::core::{CoreControlEndpoint, OwnerSessionRequest};
use hopash::core_service_ipc::{CoreServiceServer, CoreServiceServerConfig};
use hopash::domain::{
    CoreInstanceGeneration, LocalRuleSetRevision, NodeRecordId, ProbeGeneration, ProfileId,
    SampleState, TrafficSample,
};
use hopash::lifecycle::{
    InstanceRecord, ProcessIdentity, ProcessInspector, PsProcessInspector, StatePaths,
};
use hopash::process_controller::{
    NativeCoreProcessConfig, NativeCoreProcessController, UnixCoreControlClient,
};
use hopash::rule::{LocalRuleSet, RuleSetLimits};
use hopash::scheduler::{ProbeCompletion, ProbeOutcome, ProbeScheduler};
use hopash::service::{
    CallerCredentialValidator, PrivilegedCoreRuntimeService, PrivilegedServiceConfig,
    PrivilegedServiceDependencies, ProcessIdentityProbe, RuntimeConfigurationPolicy,
    RuntimeManifestFileV1, SecretGenerator, ServicePlatformError, ServicePlatformErrorKind,
    TunCapabilityPreflight,
};
use hopash::telemetry::{LogLevel, LogSource, TelemetryStore};
use hopash::tui::{AppState, Page, ProfileRow, ViewLogRecord, render_buffer};
use hopash::tui_runtime::{ProcessSignalSource, ShutdownSignal};
use nix::sys::signal::{Signal, kill};
use nix::unistd::Pid;
use portable_pty::{CommandBuilder, PtySize, native_pty_system};
use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use std::collections::VecDeque;
use std::env;
use std::error::Error;
use std::fmt::Write as FmtWrite;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, BufWriter, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::os::unix::fs::{FileTypeExt, PermissionsExt};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Output, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const CAPTURE_TOOL_VERSION: u64 = 1;
const WORKLOAD_GENERATOR_VERSION: u64 = 1;
const WORKLOAD_SEED: u64 = 0x484f_5041_5348_5253;
const CHILD_CLEANUP_TIMEOUT: Duration = Duration::from_secs(5);
const SERVICE_CLEANUP_TIMEOUT: Duration = Duration::from_secs(10);
const PROBE_COMPLETION_TIMEOUT: Duration = Duration::from_secs(10);
const CORE_FIXTURE_WORKER_COUNT: usize = PROBE_WORKER_COUNT + 8;
const MEASUREMENT_KEYS: [&str; 21] = [
    "wrapper_binary_bytes",
    "one_shot_cli_cold_start_ms",
    "supervisor_cold_start_ms",
    "privileged_service_cold_start_ms",
    "combined_cold_start_ms",
    "supervisor_idle_rss_bytes",
    "privileged_service_idle_rss_bytes",
    "combined_idle_rss_bytes",
    "idle_wakeups_per_second",
    "probe_peak_memory_bytes",
    "probe_peak_concurrency",
    "probe_first_pass_ms",
    "probe_stale_ratio",
    "rule_parse_20000_ms",
    "rule_filter_20000_ms",
    "rule_single_mutation_20000_ms",
    "telemetry_sustained_cpu_percent",
    "telemetry_peak_memory_bytes",
    "tui_cold_start_ms",
    "tui_idle_rss_bytes",
    "tui_peak_memory_bytes",
];
const CURVE_KEYS: [&str; 5] = [
    "supervisor_rss_bytes",
    "privileged_service_rss_bytes",
    "combined_background_rss_bytes",
    "telemetry_rss_bytes",
    "tui_rss_bytes",
];

#[derive(Clone, Copy)]
struct ObservationDurations {
    background_seconds: u64,
    telemetry_seconds: u64,
    tui_seconds: u64,
}

struct CollectionControl<'a> {
    observations: ObservationDurations,
    signal: &'a dyn ShutdownSignal,
}

const SMOKE_OBSERVATIONS: ObservationDurations = ObservationDurations {
    background_seconds: 1,
    telemetry_seconds: 1,
    tui_seconds: 2,
};

#[derive(Clone, Copy)]
struct WorkloadScale {
    profiles: u64,
    active_nodes: u64,
    local_rules: u64,
    telemetry_seconds: u64,
    core_logs_per_second: u64,
    tui_frames_per_second: u64,
}

const RELEASE_SCALE: WorkloadScale = WorkloadScale {
    profiles: 100,
    active_nodes: 10_000,
    local_rules: 20_000,
    telemetry_seconds: 1_800,
    core_logs_per_second: 4,
    tui_frames_per_second: 4,
};

const SMOKE_SCALE: WorkloadScale = WorkloadScale {
    profiles: 5,
    active_nodes: 100,
    local_rules: 200,
    telemetry_seconds: 5,
    core_logs_per_second: 2,
    tui_frames_per_second: 2,
};

fn main() -> Result<(), Box<dyn Error>> {
    let arguments = env::args().collect::<Vec<_>>();
    if arguments.iter().any(|argument| argument == "-t") {
        return Ok(());
    }
    if let Some(socket) = argument_value(&arguments, "-ext-ctl-unix") {
        return run_fixture_core(Path::new(socket));
    }
    match arguments.as_slice() {
        [_, command, metadata] if command == "validate" => {
            validate_metadata_file(Path::new(metadata), false)?;
        }
        [_, command, metadata, flag] if command == "validate" && flag == "--require-approved" => {
            validate_metadata_file(Path::new(metadata), true)?;
        }
        [_, command, output] if command == "generate" => {
            let manifest = generate_workload(Path::new(output), RELEASE_SCALE)?;
            println!("{}", manifest.display());
        }
        [_, command, output, flag] if command == "generate" && flag == "--smoke" => {
            let manifest = generate_workload(Path::new(output), SMOKE_SCALE)?;
            println!("{}", manifest.display());
        }
        [_, command, metadata, manifest, samples, output] if command == "capture" => {
            capture_results(
                Path::new(metadata),
                Path::new(manifest),
                Path::new(samples),
                Path::new(output),
            )?;
        }
        [
            _,
            command,
            metadata,
            manifest,
            release,
            fixture,
            resource_probe,
            output,
        ] if command == "collect" => {
            collect_sample(
                Path::new(metadata),
                Path::new(manifest),
                Path::new(release),
                Path::new(fixture),
                Path::new(resource_probe),
                Path::new(output),
                false,
            )?;
        }
        [
            _,
            command,
            metadata,
            manifest,
            release,
            fixture,
            resource_probe,
            output,
            flag,
        ] if command == "collect" && flag == "--smoke" => {
            collect_sample(
                Path::new(metadata),
                Path::new(manifest),
                Path::new(release),
                Path::new(fixture),
                Path::new(resource_probe),
                Path::new(output),
                true,
            )?;
        }
        [_, command, metadata, release, fixture] if command == "smoke" => {
            run_smoke(Path::new(metadata), Path::new(release), Path::new(fixture))?;
        }
        [
            _,
            command,
            socket,
            runtime_root,
            owner_uid,
            mihomo,
            guardian,
        ] if command == "fixture-core-service" => {
            run_fixture_core_service(
                Path::new(socket),
                Path::new(runtime_root),
                owner_uid.parse()?,
                Path::new(mihomo),
                Path::new(guardian),
            )?;
        }
        _ => return Err(usage().into()),
    }
    Ok(())
}

fn usage() -> &'static str {
    "usage:\n  cargo run --release --example release-benchmark -- validate <metadata> [--require-approved]\n  cargo run --release --example release-benchmark -- generate <output-directory> [--smoke]\n  cargo run --release --example release-benchmark -- collect <metadata> <workload-manifest> <release-binary> <fixture-binary> <resource-probe> <sample> [--smoke]\n  cargo run --release --example release-benchmark -- capture <metadata> <workload-manifest> <sample-directory> <report>\n  cargo run --release --example release-benchmark -- smoke <metadata> <release-binary> <fixture-binary>"
}

#[derive(Default)]
struct FixtureOwnerIdentity {
    pid: AtomicU64,
    value: Mutex<String>,
}

struct FixtureCredentials {
    owner_uid: u32,
    identity: Arc<FixtureOwnerIdentity>,
}

impl CallerCredentialValidator for FixtureCredentials {
    fn validate(&self, request: &OwnerSessionRequest) -> Result<(), ServicePlatformError> {
        if request.owner_uid != self.owner_uid {
            return Err(ServicePlatformError::new(
                ServicePlatformErrorKind::Credential,
            ));
        }
        self.identity
            .pid
            .store(u64::from(request.supervisor_pid), Ordering::Release);
        *self
            .identity
            .value
            .lock()
            .map_err(|_| ServicePlatformError::new(ServicePlatformErrorKind::Credential))? =
            request.supervisor_start_identity.clone();
        Ok(())
    }
}

struct FixtureIdentityProbe(Arc<FixtureOwnerIdentity>);

impl ProcessIdentityProbe for FixtureIdentityProbe {
    fn start_identity(&self, pid: u32) -> Result<Option<String>, ServicePlatformError> {
        if self.0.pid.load(Ordering::Acquire) == u64::from(pid) {
            self.0
                .value
                .lock()
                .map(|value| Some(value.clone()))
                .map_err(|_| ServicePlatformError::new(ServicePlatformErrorKind::ProcessInspection))
        } else {
            PsProcessInspector
                .identity(pid)
                .map_err(|_| ServicePlatformError::new(ServicePlatformErrorKind::ProcessInspection))
        }
    }
}

struct FixtureTun;

impl TunCapabilityPreflight for FixtureTun {
    fn check(&self, _owner_uid: u32) -> Result<(), ServicePlatformError> {
        Ok(())
    }
}

struct AllowConfigurationPolicy;

impl RuntimeConfigurationPolicy for AllowConfigurationPolicy {
    fn validate(
        &self,
        _configuration: &[u8],
        _endpoint: &CoreControlEndpoint,
        _provider_files: &[RuntimeManifestFileV1],
    ) -> Result<(), ServicePlatformError> {
        Ok(())
    }
}

#[derive(Default)]
struct FixtureSecrets(AtomicU64);

impl SecretGenerator for FixtureSecrets {
    fn generate(&self) -> Result<String, ServicePlatformError> {
        Ok(format!(
            "benchmark-secret-{}",
            self.0.fetch_add(1, Ordering::Relaxed)
        ))
    }
}

fn run_fixture_core_service(
    socket: &Path,
    runtime_root: &Path,
    owner_uid: u32,
    mihomo: &Path,
    guardian: &Path,
) -> Result<(), Box<dyn Error>> {
    let compiler = ConfigCompiler::bundled()?;
    let identity = Arc::new(FixtureOwnerIdentity::default());
    let processes = NativeCoreProcessController::new_guarded(
        NativeCoreProcessConfig::default(),
        Arc::new(UnixCoreControlClient::default()),
        Arc::new(PsProcessInspector),
        guardian.to_owned(),
    )?;
    let runtime = Arc::new(PrivilegedCoreRuntimeService::new(
        PrivilegedServiceConfig::product_defaults(
            runtime_root.to_owned(),
            compiler.compiler_policy_sha256().to_owned(),
            sha256_file(mihomo)?,
        ),
        PrivilegedServiceDependencies {
            credentials: Box::new(FixtureCredentials {
                owner_uid,
                identity: Arc::clone(&identity),
            }),
            identities: Box::new(FixtureIdentityProbe(identity)),
            tun: Box::new(FixtureTun),
            configuration_policy: Box::new(AllowConfigurationPolicy),
            secrets: Box::new(FixtureSecrets::default()),
            processes: Box::new(processes),
        },
    )?);
    let _server = CoreServiceServer::start(
        socket,
        runtime,
        CoreServiceServerConfig::new(runtime_root, owner_uid),
    )?;
    let signal = ProcessSignalSource::new()
        .map_err(|_| invalid("fixture Core service signal handling could not start"))?;
    while !signal.shutdown_requested() {
        thread::sleep(Duration::from_millis(25));
    }
    Ok(())
}

fn argument_value<'a>(arguments: &'a [String], name: &str) -> Option<&'a str> {
    arguments
        .windows(2)
        .find(|pair| pair[0] == name)
        .map(|pair| pair[1].as_str())
}

fn run_fixture_core(socket: &Path) -> Result<(), Box<dyn Error>> {
    let active_nodes = env::var("HOPASH_BENCHMARK_ACTIVE_NODES")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(1);
    let proxies = Arc::new(fixture_proxy_response(active_nodes)?);
    let listener = UnixListener::bind(socket)?;
    fs::set_permissions(socket, fs::Permissions::from_mode(0o600))?;
    let (sender, receiver) = mpsc::sync_channel(CORE_FIXTURE_WORKER_COUNT);
    let receiver = Arc::new(Mutex::new(receiver));
    let _workers = (0..CORE_FIXTURE_WORKER_COUNT)
        .map(|_| {
            let receiver = Arc::clone(&receiver);
            let proxies = Arc::clone(&proxies);
            thread::spawn(move || {
                loop {
                    let stream = receiver
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .recv();
                    let Ok(stream) = stream else {
                        return;
                    };
                    let _ = serve_core_request(stream, &proxies);
                }
            })
        })
        .collect::<Vec<_>>();
    loop {
        match listener.accept() {
            Ok((stream, _)) => {
                sender
                    .send(stream)
                    .map_err(|_| invalid("fixture Core worker pool stopped"))?;
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(Box::new(error)),
        }
    }
}

fn fixture_proxy_response(active_nodes: u64) -> Result<Vec<u8>, Box<dyn Error>> {
    let names = (0..active_nodes)
        .map(|index| Value::from(format!("Release Node {index:05}")))
        .collect::<Vec<_>>();
    let mut proxies = Map::new();
    proxies.insert(
        "GLOBAL".to_owned(),
        json!({
            "alive": true, "all": ["PROXY", "DIRECT"], "history": [],
            "name": "GLOBAL", "now": "PROXY", "type": "Selector",
            "udp": true, "xudp": false
        }),
    );
    proxies.insert(
        "PROXY".to_owned(),
        json!({
            "alive": true, "all": names, "history": [],
            "name": "PROXY", "now": "Release Node 00000", "type": "Selector",
            "udp": true, "xudp": false
        }),
    );
    proxies.insert(
        "DIRECT".to_owned(),
        json!({
            "alive": true, "history": [], "name": "DIRECT",
            "type": "Direct", "udp": true, "xudp": false
        }),
    );
    for index in 0..active_nodes {
        let name = format!("Release Node {index:05}");
        proxies.insert(
            name.clone(),
            json!({
                "alive": true, "history": [], "name": name,
                "type": "Shadowsocks", "udp": true, "xudp": false
            }),
        );
    }
    Ok(serde_json::to_vec(&json!({ "proxies": proxies }))?)
}

fn serve_core_request(mut stream: UnixStream, proxies: &[u8]) -> Result<(), Box<dyn Error>> {
    stream.set_read_timeout(Some(Duration::from_secs(5)))?;
    stream.set_write_timeout(Some(Duration::from_secs(5)))?;
    let mut request = [0_u8; 8 * 1_024];
    let mut request_bytes = 0_usize;
    let request_length = loop {
        if request_bytes == request.len() {
            return Err(invalid("fixture Core request exceeds its bounded buffer"));
        }
        let read = stream.read(&mut request[request_bytes..])?;
        if read == 0 {
            return Err(invalid("fixture Core request closed before completion"));
        }
        request_bytes += read;
        let Some(header_start) = request[..request_bytes]
            .windows(4)
            .position(|window| window == b"\r\n\r\n")
        else {
            continue;
        };
        let header_end = header_start + 4;
        let headers = std::str::from_utf8(&request[..header_start])?;
        let content_length = headers
            .split("\r\n")
            .skip(1)
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("content-length")
                    .then_some(value.trim())
            })
            .map(str::parse::<usize>)
            .transpose()?
            .unwrap_or(0);
        let complete = header_end
            .checked_add(content_length)
            .ok_or_else(|| invalid("fixture Core request length overflowed"))?;
        if complete > request.len() {
            return Err(invalid(
                "fixture Core request body exceeds its bounded buffer",
            ));
        }
        if request_bytes >= complete {
            break complete;
        }
    };
    let request = String::from_utf8_lossy(&request[..request_length]);
    let (status, body): (&str, &[u8]) = if request.starts_with("PUT /configs?force=true ") {
        ("204 No Content", b"")
    } else if request.starts_with("GET /providers/proxies ") {
        ("200 OK", br#"{"providers":{}}"#)
    } else if request.starts_with("GET /proxies ") {
        ("200 OK", proxies)
    } else if request.contains("/delay?") {
        thread::sleep(Duration::from_millis(5));
        ("200 OK", br#"{"delay":5}"#)
    } else if request.starts_with("GET /connections ") {
        (
            "200 OK",
            br#"{"downloadTotal":0,"uploadTotal":0,"connections":[]}"#,
        )
    } else {
        (
            "200 OK",
            br#"{"meta":true,"premium":false,"version":"v1.19.28"}"#,
        )
    };
    write!(
        stream,
        "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    )?;
    stream.write_all(body)?;
    stream.flush()?;
    thread::sleep(Duration::from_millis(1));
    Ok(())
}

fn validate_metadata_file(path: &Path, require_approved: bool) -> Result<(), Box<dyn Error>> {
    let metadata = read_json(path)?;
    validate_metadata(&metadata, require_approved)?;
    println!("benchmark metadata is valid");
    Ok(())
}

fn validate_metadata(metadata: &Value, require_approved: bool) -> Result<(), Box<dyn Error>> {
    require_u64(metadata, "schema_version", 1)?;
    let status = require_string(metadata, "status")?;
    if status != "capture_required" && status != "approved" {
        return Err(invalid("status must be capture_required or approved"));
    }
    if require_approved && status != "approved" {
        return Err(invalid("release benchmarks require explicit approval"));
    }
    let derived_target = match metadata.get("target") {
        Some(Value::String(target))
            if matches!(
                target.as_str(),
                "aarch64-apple-darwin" | "x86_64-apple-darwin"
            ) =>
        {
            true
        }
        Some(_) => return Err(invalid("target must name a supported macOS release target")),
        None => false,
    };

    let environment = require_object(metadata, "measurement_environment")?;
    require_non_empty_string(environment, "runner")?;
    require_u64_object(environment, "samples", 10)?;
    require_u64_object(environment, "warmup_samples", 2)?;
    require_string_object(environment, "summary_statistic", "median")?;
    require_u64_object(environment, "idle_observation_seconds", 60)?;
    require_u64_object(environment, "telemetry_observation_seconds", 1_800)?;
    require_u64_object(environment, "tui_observation_seconds", 1_800)?;
    require_u64_object(environment, "rss_sample_interval_seconds", 1)?;
    if environment["rss_curve_names"] != json!(CURVE_KEYS) {
        return Err(invalid(
            "measurement_environment.rss_curve_names must match the versioned curve set",
        ));
    }
    let runner_profile = environment
        .get("runner_profile")
        .and_then(Value::as_object)
        .ok_or_else(|| invalid("measurement_environment.runner_profile must be an object"))?;
    validate_runner_profile(runner_profile, status == "approved")?;

    let workloads = require_object(metadata, "workloads")?;
    require_u64_object(workloads, "profiles", RELEASE_SCALE.profiles)?;
    require_u64_object(workloads, "active_nodes", RELEASE_SCALE.active_nodes)?;
    require_u64_object(workloads, "local_rules", RELEASE_SCALE.local_rules)?;
    require_bool_object(workloads, "application_seam_scale", true)?;
    require_u64_object(
        workloads,
        "tui_duration_seconds",
        RELEASE_SCALE.telemetry_seconds,
    )?;
    require_bool_object(workloads, "sustained_core_logs", true)?;
    require_bool_object(workloads, "sustained_traffic_samples", true)?;

    let generator = require_object(metadata, "workload_generator")?;
    require_string_object(generator, "name", "hopash-release-workload")?;
    require_u64_object(generator, "version", WORKLOAD_GENERATOR_VERSION)?;
    require_u64_object(generator, "seed", WORKLOAD_SEED)?;

    let measurements = exact_measurement_map(metadata, "measurements")?;
    let baselines = exact_measurement_map(metadata, "baseline_measurements")?;
    let thresholds = exact_measurement_map(metadata, "thresholds")?;
    let budgets = exact_measurement_map(metadata, "regression_budgets_percent")?;
    let approved_capture = metadata
        .get("approved_capture")
        .ok_or_else(|| invalid("approved_capture is missing"))?;

    if status == "capture_required" {
        if derived_target {
            return Err(invalid(
                "target-specific benchmark metadata requires approved capture data",
            ));
        }
        for (label, values) in [
            ("measurements", measurements),
            ("baseline_measurements", baselines),
            ("thresholds", thresholds),
            ("regression_budgets_percent", budgets),
        ] {
            if values.values().any(|value| !value.is_null()) {
                return Err(invalid(format!(
                    "{label} must remain null until benchmark approval"
                )));
            }
        }
        if !approved_capture.is_null() {
            return Err(invalid(
                "approved_capture must remain null until benchmark approval",
            ));
        }
        return Ok(());
    }

    validate_approved_capture(approved_capture, environment, measurements, derived_target)?;
    for key in MEASUREMENT_KEYS {
        let measurement = non_negative_number(&measurements[key], key)?;
        let baseline = non_negative_number(&baselines[key], key)?;
        let threshold = non_negative_number(&thresholds[key], key)?;
        let budget = non_negative_number(&budgets[key], key)?;
        if budget > 100.0 {
            return Err(invalid(format!(
                "regression budget for {key} must be at most 100 percent"
            )));
        }
        if measurement > threshold {
            return Err(invalid(format!(
                "measurement {key} exceeds its approved threshold"
            )));
        }
        let regression_limit = baseline * (1.0 + budget / 100.0);
        if measurement > regression_limit {
            return Err(invalid(format!(
                "measurement {key} exceeds its approved regression budget"
            )));
        }
    }
    Ok(())
}

fn validate_approved_capture(
    approved_capture: &Value,
    declared_environment: &Map<String, Value>,
    approved_measurements: &Map<String, Value>,
    derived_target: bool,
) -> Result<(), Box<dyn Error>> {
    let capture = approved_capture
        .as_object()
        .ok_or_else(|| invalid("approved_capture must be an object when approved"))?;
    for field in [
        "runner",
        "rustc_version",
        "cargo_version",
        "git_revision",
        "source_tree_sha256",
        "cargo_lock_sha256",
        "collector_sha256",
        "workload_manifest_sha256",
        "reviewed_report_sha256",
    ] {
        require_non_empty_string(capture, field)?;
    }
    if capture["runner"] != declared_environment["runner"] {
        return Err(invalid(
            "approved capture runner must match measurement_environment.runner",
        ));
    }
    let capture_profile = capture
        .get("runner_profile")
        .and_then(Value::as_object)
        .ok_or_else(|| invalid("approved_capture.runner_profile must be an object"))?;
    let declared_profile = declared_environment["runner_profile"]
        .as_object()
        .ok_or_else(|| invalid("measurement runner profile must be an object"))?;
    validate_runner_profile(capture_profile, true)?;
    if capture_profile != declared_profile {
        return Err(invalid(
            "approved capture runner profile must match measurement_environment.runner_profile",
        ));
    }
    require_positive_u64_object(capture, "captured_at_unix_seconds")?;
    require_u64_object(
        capture,
        "samples",
        declared_environment["samples"]
            .as_u64()
            .ok_or_else(|| invalid("measurement_environment.samples must be an integer"))?,
    )?;
    require_string_object(capture, "summary_statistic", "median")?;
    validate_sha256_string(
        &capture["workload_manifest_sha256"],
        "workload_manifest_sha256",
    )?;
    validate_sha256_string(&capture["reviewed_report_sha256"], "reviewed_report_sha256")?;
    let current_rustc = command_output("rustc", &["--version"])?;
    let current_cargo = command_output("cargo", &["--version"])?;
    if capture["rustc_version"] != current_rustc || capture["cargo_version"] != current_cargo {
        return Err(invalid(
            "approved capture toolchain differs from the release toolchain",
        ));
    }
    let project_root = project_root()?;
    if capture["source_tree_sha256"] != source_tree_sha256()?
        || capture["cargo_lock_sha256"] != sha256_file(&project_root.join("Cargo.lock"))?
        || capture["collector_sha256"]
            != sha256_file(&project_root.join("examples/release-benchmark.rs"))?
    {
        return Err(invalid(
            "approved capture source inputs differ from the release tree",
        ));
    }
    let workload = TemporaryDirectory::new()?;
    let manifest = generate_workload(&workload.path.join("workload"), RELEASE_SCALE)?;
    if capture["workload_manifest_sha256"] != sha256_file(&manifest)? {
        return Err(invalid("approved capture workload digest is stale"));
    }
    let reviewed_report = capture
        .get("reviewed_report")
        .ok_or_else(|| invalid("approved_capture.reviewed_report is missing"))?;
    validate_reviewed_report(
        reviewed_report,
        capture,
        approved_measurements,
        derived_target,
    )?;
    if capture["reviewed_report_sha256"] != sha256_pretty_json(reviewed_report)? {
        return Err(invalid(
            "approved capture report digest differs from the reviewed report",
        ));
    }
    Ok(())
}

fn validate_reviewed_report(
    report: &Value,
    capture: &Map<String, Value>,
    approved_measurements: &Map<String, Value>,
    derived_target: bool,
) -> Result<(), Box<dyn Error>> {
    require_u64(report, "schema_version", 1)?;
    if report["status"] != "review_required" {
        return Err(invalid("reviewed report status must be review_required"));
    }
    let tool = require_object(report, "capture_tool")?;
    require_string_object(tool, "name", "hopash-release-benchmark")?;
    require_u64_object(tool, "version", CAPTURE_TOOL_VERSION)?;

    let report_environment = require_object(report, "environment")?;
    for field in [
        "runner",
        "runner_profile",
        "rustc_version",
        "cargo_version",
        "git_revision",
        "source_tree_sha256",
        "cargo_lock_sha256",
        "collector_sha256",
        "captured_at_unix_seconds",
    ] {
        if report_environment.get(field) != capture.get(field) {
            return Err(invalid(format!(
                "reviewed report environment field {field} differs from approved capture"
            )));
        }
    }

    let inputs = require_object(report, "inputs")?;
    for field in [
        "release_binary_sha256",
        "fixture_binary_sha256",
        "resource_probe_sha256",
    ] {
        validate_sha256_string(
            inputs
                .get(field)
                .ok_or_else(|| invalid(format!("reviewed report input {field} is missing")))?,
            field,
        )?;
    }

    let workload = require_object(report, "workload")?;
    if workload["manifest_sha256"] != capture["workload_manifest_sha256"] {
        return Err(invalid(
            "reviewed report workload differs from approved capture",
        ));
    }
    require_u64_object(
        report
            .as_object()
            .ok_or_else(|| invalid("reviewed report must be an object"))?,
        "samples",
        capture["samples"]
            .as_u64()
            .ok_or_else(|| invalid("approved capture samples must be an integer"))?,
    )?;
    if report["summary_statistic"] != capture["summary_statistic"] {
        return Err(invalid(
            "reviewed report summary statistic differs from approved capture",
        ));
    }
    let report_measurements = exact_measurement_map(report, "measurements")?;
    exact_numeric_measurements(report_measurements)?;
    for key in MEASUREMENT_KEYS {
        if derived_target && key == "wrapper_binary_bytes" {
            continue;
        }
        if report_measurements.get(key) != approved_measurements.get(key) {
            return Err(invalid(
                "approved measurements differ from the reviewed report median projection",
            ));
        }
    }

    let raw_samples = report["raw_samples"]
        .as_array()
        .ok_or_else(|| invalid("reviewed report raw_samples must be an array"))?;
    let expected_samples = usize::try_from(
        capture["samples"]
            .as_u64()
            .ok_or_else(|| invalid("approved capture samples must be an integer"))?,
    )?;
    if raw_samples.len() != expected_samples {
        return Err(invalid(
            "reviewed report raw sample digest set is incomplete",
        ));
    }
    let mut raw_measurements = MEASUREMENT_KEYS
        .iter()
        .map(|key| ((*key).to_owned(), Vec::with_capacity(expected_samples)))
        .collect::<std::collections::BTreeMap<_, _>>();
    for (index, sample) in raw_samples.iter().enumerate() {
        let sample = sample
            .as_object()
            .ok_or_else(|| invalid("reviewed report raw sample must be an object"))?;
        let expected_path = format!("samples/sample-{:02}.json", index + 1);
        require_string_object(sample, "path", &expected_path)?;
        validate_sha256_string(
            sample
                .get("sha256")
                .ok_or_else(|| invalid("reviewed report raw sample digest is missing"))?,
            "raw sample sha256",
        )?;
        let measurements = sample
            .get("measurements")
            .and_then(Value::as_object)
            .ok_or_else(|| invalid("reviewed report raw sample measurements are missing"))?;
        exact_numeric_measurements(measurements)?;
        for key in MEASUREMENT_KEYS {
            raw_measurements
                .get_mut(key)
                .expect("measurement keys are initialized")
                .push(non_negative_number(&measurements[key], key)?);
        }
    }
    if &median_measurements(raw_measurements)? != report_measurements {
        return Err(invalid(
            "reviewed report median projection does not match its raw sample measurements",
        ));
    }
    Ok(())
}

fn validate_runner_profile(
    profile: &Map<String, Value>,
    approved: bool,
) -> Result<(), Box<dyn Error>> {
    require_string_object(profile, "architecture", "aarch64")?;
    for field in ["hardware_model", "cpu_model", "os_version"] {
        let value = profile
            .get(field)
            .ok_or_else(|| invalid(format!("runner profile {field} is missing")))?;
        if approved {
            value
                .as_str()
                .filter(|value| !value.trim().is_empty())
                .ok_or_else(|| invalid(format!("runner profile {field} must be recorded")))?;
        } else if !value.is_null() {
            return Err(invalid(format!(
                "runner profile {field} must remain null until benchmark approval"
            )));
        }
    }
    for field in ["logical_cpu_count", "memory_bytes"] {
        let value = profile
            .get(field)
            .ok_or_else(|| invalid(format!("runner profile {field} is missing")))?;
        if approved {
            value
                .as_u64()
                .filter(|value| *value > 0)
                .ok_or_else(|| invalid(format!("runner profile {field} must be positive")))?;
        } else if !value.is_null() {
            return Err(invalid(format!(
                "runner profile {field} must remain null until benchmark approval"
            )));
        }
    }
    Ok(())
}

fn generate_workload(root: &Path, scale: WorkloadScale) -> Result<PathBuf, Box<dyn Error>> {
    fs::create_dir(root)?;
    let profiles = root.join("profiles.ndjson");
    let nodes = root.join("active-nodes.ndjson");
    let rules = root.join("rules.yaml");
    let telemetry = root.join("telemetry.ndjson");
    let tui_events = root.join("tui-events.ndjson");

    write_profiles(&profiles, scale.profiles)?;
    write_nodes(&nodes, scale.active_nodes)?;
    write_rules(&rules, scale.local_rules)?;
    write_telemetry(&telemetry, scale)?;
    write_tui_events(&tui_events, scale)?;

    let manifest = json!({
        "schema_version": 1,
        "generator": {
            "name": "hopash-release-workload",
            "version": WORKLOAD_GENERATOR_VERSION,
            "seed": WORKLOAD_SEED
        },
        "scale": {
            "profiles": scale.profiles,
            "active_nodes": scale.active_nodes,
            "local_rules": scale.local_rules,
            "telemetry_duration_seconds": scale.telemetry_seconds,
            "core_log_records": scale.telemetry_seconds * scale.core_logs_per_second,
            "traffic_sample_records": scale.telemetry_seconds,
            "tui_duration_seconds": scale.telemetry_seconds,
            "tui_frame_records": scale.telemetry_seconds * scale.tui_frames_per_second
        },
        "artifacts": {
            "profiles.ndjson": artifact_metadata(&profiles, scale.profiles)?,
            "active-nodes.ndjson": artifact_metadata(&nodes, scale.active_nodes)?,
            "rules.yaml": artifact_metadata(&rules, scale.local_rules + 1)?,
            "telemetry.ndjson": artifact_metadata(
                &telemetry,
                scale.telemetry_seconds * (scale.core_logs_per_second + 1)
            )?,
            "tui-events.ndjson": artifact_metadata(
                &tui_events,
                scale.telemetry_seconds * scale.tui_frames_per_second
            )?
        }
    });
    let manifest_path = root.join("workload-manifest-v1.json");
    write_json_new(&manifest_path, &manifest)?;
    Ok(manifest_path)
}

fn write_profiles(path: &Path, profiles: u64) -> Result<(), Box<dyn Error>> {
    let mut output = new_buffered_file(path)?;
    for index in 0..profiles {
        let profile_id = format!("00000000-0000-4000-8000-{index:012x}");
        let profile_id = ProfileId::parse(&profile_id)?;
        write_json_line(
            &mut output,
            &json!({
                "active": index == 0,
                "id": profile_id.to_string(),
                "name": format!("Release Profile {index:03}"),
                "subscription_url": format!("https://profile-{index:03}.example.invalid/subscription")
            }),
        )?;
    }
    output.flush()?;
    Ok(())
}

fn write_nodes(path: &Path, nodes: u64) -> Result<(), Box<dyn Error>> {
    let mut output = new_buffered_file(path)?;
    for index in 0..nodes {
        let name = format!("Release Node {index:05}");
        let node_id = NodeRecordId::for_core(&name);
        write_json_line(
            &mut output,
            &json!({
                "available": true,
                "id": node_id.as_str(),
                "name": name,
                "proxy_type": "socks5",
                "source": "core"
            }),
        )?;
    }
    output.flush()?;
    Ok(())
}

fn write_rules(path: &Path, rules: u64) -> Result<(), Box<dyn Error>> {
    let mut output = new_buffered_file(path)?;
    writeln!(output, "rules:")?;
    for index in 0..rules {
        writeln!(
            output,
            "- DOMAIN-SUFFIX,rule-{index:05}.example.invalid,PROXY"
        )?;
    }
    output.flush()?;
    Ok(())
}

fn write_telemetry(path: &Path, scale: WorkloadScale) -> Result<(), Box<dyn Error>> {
    let mut output = new_buffered_file(path)?;
    for second in 0..scale.telemetry_seconds {
        write_json_line(
            &mut output,
            &json!({
                "download_bytes_per_second": (second * 7_919 + WORKLOAD_SEED) % 8_000_000,
                "second": second,
                "type": "traffic",
                "upload_bytes_per_second": (second * 3_571 + WORKLOAD_SEED) % 2_000_000
            }),
        )?;
        for sequence in 0..scale.core_logs_per_second {
            let level =
                ["debug", "info", "warn", "error"][usize::try_from((second + sequence) % 4)?];
            write_json_line(
                &mut output,
                &json!({
                    "level": level,
                    "message": format!("release telemetry {second:04}-{sequence:02}"),
                    "second": second,
                    "sequence": sequence,
                    "type": "core_log"
                }),
            )?;
        }
    }
    output.flush()?;
    Ok(())
}

fn write_tui_events(path: &Path, scale: WorkloadScale) -> Result<(), Box<dyn Error>> {
    let mut output = new_buffered_file(path)?;
    let frames = scale.telemetry_seconds * scale.tui_frames_per_second;
    for frame in 0..frames {
        let page = ["overview", "proxies", "profiles", "logs"][usize::try_from((frame / 240) % 4)?];
        write_json_line(
            &mut output,
            &json!({
                "frame": frame,
                "page": page,
                "timestamp_ms": frame * 1_000 / scale.tui_frames_per_second,
                "type": "render_tick"
            }),
        )?;
    }
    output.flush()?;
    Ok(())
}

fn collect_sample(
    metadata_path: &Path,
    manifest_path: &Path,
    release_binary: &Path,
    fixture_binary: &Path,
    resource_probe: &Path,
    output: &Path,
    smoke: bool,
) -> Result<(), Box<dyn Error>> {
    let signal = ProcessSignalSource::new()
        .map_err(|_| invalid("release benchmark signal handling could not start"))?;
    ensure_collection_running(&signal)?;
    let metadata = read_json(metadata_path)?;
    validate_metadata(&metadata, false)?;
    if metadata["status"] != "capture_required" {
        return Err(invalid("collection requires capture_required metadata"));
    }
    let manifest = read_json(manifest_path)?;
    if smoke {
        validate_manifest_scale(&manifest, SMOKE_SCALE)?;
    } else {
        validate_release_manifest(&manifest, &metadata)?;
    }
    validate_manifest_artifacts(manifest_path, &manifest)?;
    for (label, path) in [
        ("release binary", release_binary),
        ("fixture binary", fixture_binary),
        ("resource probe", resource_probe),
    ] {
        let file = fs::metadata(path)?;
        if !path.is_absolute() || !file.is_file() || file.permissions().mode() & 0o111 == 0 {
            return Err(invalid(format!(
                "{label} must be an absolute executable file"
            )));
        }
    }
    let inputs = json!({
        "release_binary_sha256": sha256_file(release_binary)?,
        "fixture_binary_sha256": sha256_file(fixture_binary)?,
        "resource_probe_sha256": sha256_file(resource_probe)?
    });

    let mut measurements = Map::new();
    let mut curves = Map::new();
    insert_measurement(
        &mut measurements,
        "wrapper_binary_bytes",
        fs::metadata(release_binary)?.len() as f64,
    );
    let cli_start = Instant::now();
    let version = Command::new(release_binary).arg("--version").output()?;
    if !version.status.success() {
        return Err(invalid("the release One-shot CLI failed to start"));
    }
    insert_measurement(
        &mut measurements,
        "one_shot_cli_cold_start_ms",
        elapsed_ms(cli_start),
    );

    let observations = if smoke {
        SMOKE_OBSERVATIONS
    } else {
        ObservationDurations {
            background_seconds: metadata["measurement_environment"]["idle_observation_seconds"]
                .as_u64()
                .ok_or_else(|| invalid("idle observation must be an integer"))?,
            telemetry_seconds: metadata["measurement_environment"]["telemetry_observation_seconds"]
                .as_u64()
                .ok_or_else(|| invalid("telemetry observation must be an integer"))?,
            tui_seconds: metadata["measurement_environment"]["tui_observation_seconds"]
                .as_u64()
                .ok_or_else(|| invalid("TUI observation must be an integer"))?,
        }
    };
    let control = CollectionControl {
        observations,
        signal: &signal,
    };
    ensure_collection_running(&signal)?;
    measurements.extend(collect_lifecycle_metrics(
        release_binary,
        fixture_binary,
        manifest_path,
        if smoke { SMOKE_SCALE } else { RELEASE_SCALE },
        resource_probe,
        &mut curves,
        &control,
    )?);
    ensure_collection_running(&signal)?;
    measurements.extend(collect_product_metrics(
        manifest_path,
        resource_probe,
        &mut curves,
        &control,
    )?);
    exact_numeric_measurements(&measurements)?;
    validate_curves(&curves, observations)?;

    write_json_new(
        output,
        &json!({
            "schema_version": 1,
            "workload_manifest_sha256": sha256_file(manifest_path)?,
            "collector": {
                "name": "hopash-release-benchmark",
                "version": CAPTURE_TOOL_VERSION,
                "smoke": smoke
            },
            "environment": capture_environment(&metadata)?,
            "inputs": inputs,
            "observation_seconds": {
                "background": observations.background_seconds,
                "telemetry": observations.telemetry_seconds,
                "tui": observations.tui_seconds
            },
            "measurements": measurements,
            "curves": curves
        }),
    )?;
    Ok(())
}

fn collect_lifecycle_metrics(
    release_binary: &Path,
    fixture_binary: &Path,
    manifest_path: &Path,
    scale: WorkloadScale,
    resource_probe: &Path,
    curves: &mut Map<String, Value>,
    control: &CollectionControl<'_>,
) -> Result<Map<String, Value>, Box<dyn Error>> {
    let observations = control.observations;
    let signal = control.signal;
    let root = TemporaryDirectory::new()?;
    let state_root = root.path.join("state");
    let runtime_root = root.path.join("service-runtime");
    let service_socket = root.path.join("core-service.sock");
    let mihomo = env::current_exe()?;
    let profile_server = ProfileServer::start(manifest_path, scale)?;

    let owner_uid = nix::unistd::Uid::effective().as_raw();
    let combined_start = Instant::now();
    let service_start = Instant::now();
    let service = spawn_fixture_service(
        &service_socket,
        &runtime_root,
        owner_uid,
        &mihomo,
        fixture_binary,
        scale.active_nodes,
    )?;
    let service_cold_start_ms = elapsed_ms(service_start);
    let mut guard = LifecycleGuard {
        release_binary: release_binary.to_owned(),
        fixture_binary: fixture_binary.to_owned(),
        state_root: state_root.clone(),
        service_socket: service_socket.clone(),
        mihomo: mihomo.clone(),
        supervisor: None,
        service: Some(service),
    };
    let result = (|| -> Result<Map<String, Value>, Box<dyn Error>> {
        let supervisor_start = Instant::now();
        let start_output = guard.run_fixture(&["start", "--json"])?;
        let instance =
            InstanceRecord::read_private(&StatePaths::for_root(&state_root).instance_record)?;
        if let Some(instance) = instance.as_ref() {
            guard.supervisor = Some(instance.supervisor.clone());
        }
        command_json(start_output, "fixture Supervisor seed")?;
        let instance = instance
            .ok_or_else(|| invalid("fixture Supervisor did not write an instance record"))?;
        ensure_collection_running(signal)?;
        let active_profile_url = profile_server.url(0);
        let add_profile = guard.run(&["profile", "add", &active_profile_url, "--json"])?;
        if !add_profile.status.success() {
            return Err(invalid(format!(
                "fixture Profile seed failed: {}",
                String::from_utf8_lossy(&add_profile.stderr).trim(),
            )));
        }
        wait_for_active_status(&guard, Duration::from_secs(10), signal)?;
        let supervisor_cold_start_ms = elapsed_ms(supervisor_start);
        let combined_cold_start_ms = elapsed_ms(combined_start);
        for index in 1..scale.profiles {
            ensure_collection_running(signal)?;
            let profile_url = profile_server.url(index);
            command_json(
                guard.run(&["profile", "add", &profile_url, "--json"])?,
                "release-scale Inactive Profile seed",
            )?;
        }
        let rule_mutation_ms = validate_release_application_scale(&guard, scale, signal)?;
        drop(profile_server);
        let supervisor_pid = instance.supervisor.pid;
        let service_pid = guard
            .service
            .as_ref()
            .ok_or_else(|| invalid("fixture Core service is missing"))?
            .id()?;
        let background = collect_background_process_metrics(
            resource_probe,
            supervisor_pid,
            service_pid,
            observations.background_seconds,
            signal,
        )?;
        let tui =
            collect_tui_process_metrics(&guard, resource_probe, observations.tui_seconds, signal)?;
        curves.insert(
            "supervisor_rss_bytes".to_owned(),
            Value::Array(background.supervisor_curve),
        );
        curves.insert(
            "privileged_service_rss_bytes".to_owned(),
            Value::Array(background.service_curve),
        );
        curves.insert(
            "combined_background_rss_bytes".to_owned(),
            Value::Array(background.combined_curve),
        );
        curves.insert("tui_rss_bytes".to_owned(), Value::Array(tui.rss_curve));

        let mut measurements = Map::new();
        insert_measurement(
            &mut measurements,
            "supervisor_cold_start_ms",
            supervisor_cold_start_ms,
        );
        insert_measurement(
            &mut measurements,
            "privileged_service_cold_start_ms",
            service_cold_start_ms,
        );
        insert_measurement(
            &mut measurements,
            "combined_cold_start_ms",
            combined_cold_start_ms,
        );
        insert_measurement(
            &mut measurements,
            "supervisor_idle_rss_bytes",
            background.supervisor_idle_rss,
        );
        insert_measurement(
            &mut measurements,
            "privileged_service_idle_rss_bytes",
            background.service_idle_rss,
        );
        insert_measurement(
            &mut measurements,
            "combined_idle_rss_bytes",
            background.combined_idle_rss,
        );
        insert_measurement(
            &mut measurements,
            "idle_wakeups_per_second",
            background.wakeups_per_second,
        );
        insert_measurement(
            &mut measurements,
            "rule_single_mutation_20000_ms",
            rule_mutation_ms,
        );
        insert_measurement(&mut measurements, "tui_cold_start_ms", tui.cold_start_ms);
        insert_measurement(&mut measurements, "tui_idle_rss_bytes", tui.idle_rss);
        insert_measurement(&mut measurements, "tui_peak_memory_bytes", tui.peak_memory);
        Ok(measurements)
    })();
    let cleanup = guard.shutdown();
    match (result, cleanup) {
        (Ok(measurements), Ok(())) => Ok(measurements),
        (Err(error), Ok(())) => Err(error),
        (Ok(_), Err(cleanup_error)) => Err(cleanup_error),
        (Err(error), Err(cleanup_error)) => {
            Err(invalid(format!("{error}; cleanup failed: {cleanup_error}")))
        }
    }
}

fn collect_product_metrics(
    manifest_path: &Path,
    resource_probe: &Path,
    curves: &mut Map<String, Value>,
    control: &CollectionControl<'_>,
) -> Result<Map<String, Value>, Box<dyn Error>> {
    let observation_seconds = control.observations.telemetry_seconds;
    let signal = control.signal;
    let root = manifest_path
        .parent()
        .ok_or_else(|| invalid("workload manifest must have a parent directory"))?;
    let workload_manifest = read_json(manifest_path)?;
    let last_rule = workload_manifest["scale"]["local_rules"]
        .as_u64()
        .and_then(|count| count.checked_sub(1))
        .ok_or_else(|| invalid("workload must contain at least one Local Rule"))?;
    let rule_needle = format!("rule-{last_rule:05}");
    let mut measurements = Map::new();

    let profile_records = read_ndjson(&root.join("profiles.ndjson"))?;
    let mut profile_rows = Vec::with_capacity(profile_records.len());
    for profile in profile_records {
        let profile_id = ProfileId::parse(
            profile["id"]
                .as_str()
                .ok_or_else(|| invalid("workload Profile ID must be a string"))?,
        )?;
        let name = profile["name"]
            .as_str()
            .ok_or_else(|| invalid("workload Profile name must be a string"))?;
        profile_rows.push(ProfileRow {
            id: profile_id,
            name: name.to_owned(),
            active: profile["active"]
                .as_bool()
                .ok_or_else(|| invalid("workload Profile active state must be a boolean"))?,
            fresh: true,
            last_success_at_unix_ms: 0,
            next_refresh_at_unix_ms: 0,
            error: None,
        });
    }
    let node_ids = read_ndjson(&root.join("active-nodes.ndjson"))?
        .into_iter()
        .map(|node| {
            NodeRecordId::parse(
                node["id"]
                    .as_str()
                    .ok_or_else(|| invalid("workload Node ID must be a string"))?,
            )
            .map_err(|error| Box::new(error) as Box<dyn Error>)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let probe_rss_before = current_rss(resource_probe)?;
    let probe_start = Instant::now();
    let mut scheduler = ProbeScheduler::new();
    scheduler
        .reset(ProbeGeneration(1), node_ids, 0)
        .map_err(|_| {
            invalid("release Probe Scheduler rejected the generated Active Node workload")
        })?;
    let mut peak_concurrency = 0_usize;
    let mut completed = 0_usize;
    while completed < scheduler.active_node_count() {
        ensure_collection_running(signal)?;
        let tasks = scheduler.take_due(0);
        if tasks.is_empty() {
            return Err(invalid("release Probe Queue stopped before its first pass"));
        }
        peak_concurrency = peak_concurrency.max(tasks.len());
        completed += tasks.len();
        for task in tasks {
            let _ = scheduler.complete(ProbeCompletion {
                task,
                outcome: ProbeOutcome::Success { delay_ms: 1 },
                completed_at_unix_ms: 0,
            });
        }
    }
    let first_pass_ms = elapsed_ms(probe_start);
    let probe_metrics = scheduler.metrics(0);
    let probe_peak_memory = current_rss(resource_probe)?.max(probe_rss_before);
    if peak_concurrency > PROBE_WORKER_COUNT || probe_metrics.stale_ratio != 0.0 {
        return Err(invalid("release Probe Queue product bounds changed"));
    }
    insert_measurement(
        &mut measurements,
        "probe_peak_memory_bytes",
        probe_peak_memory,
    );
    insert_measurement(
        &mut measurements,
        "probe_peak_concurrency",
        peak_concurrency as f64,
    );
    insert_measurement(&mut measurements, "probe_first_pass_ms", first_pass_ms);
    insert_measurement(
        &mut measurements,
        "probe_stale_ratio",
        probe_metrics.stale_ratio,
    );
    drop(scheduler);

    let rule_document = fs::read_to_string(root.join("rules.yaml"))?;
    let rule_parse_start = Instant::now();
    let rules = LocalRuleSet::from_yaml(
        &rule_document,
        LocalRuleSetRevision(1),
        RuleSetLimits::product(),
    )?;
    let rule_parse_ms = elapsed_ms(rule_parse_start);
    let rule_filter_start = Instant::now();
    let matching_rules = rules
        .list()?
        .entries
        .into_iter()
        .filter(|entry| entry.rule.as_str().contains(&rule_needle))
        .count();
    let rule_filter_ms = elapsed_ms(rule_filter_start);
    if matching_rules != 1 {
        return Err(invalid(
            "release rule filter did not find its exact fixture",
        ));
    }
    insert_measurement(&mut measurements, "rule_parse_20000_ms", rule_parse_ms);
    insert_measurement(&mut measurements, "rule_filter_20000_ms", rule_filter_ms);
    drop(rules);

    let generation = CoreInstanceGeneration(1);
    let mut telemetry = TelemetryStore::new(
        generation,
        LOG_CAPACITY,
        CORE_LOG_LINE_MAX_BYTES,
        TRAFFIC_SERIES_CAPACITY,
    )?;
    let mut state = AppState::new();
    state.profiles.rows = profile_rows;
    let area = Rect::new(0, 0, 120, 40);
    let mut buffer = Buffer::empty(area);
    let _ = render_buffer(&state, area, &mut buffer);
    let telemetry_records = read_ndjson(&root.join("telemetry.ndjson"))?;
    let tui_events = read_ndjson(&root.join("tui-events.ndjson"))?;
    if telemetry_records.is_empty() || tui_events.is_empty() {
        return Err(invalid(
            "release telemetry and TUI schedules must be non-empty",
        ));
    }
    let initial_rss = current_rss(resource_probe)?;
    let cpu_pid = std::process::id().to_string();
    let duration = observation_seconds.to_string();
    let cpu_probe = ProcessChildGuard::new(
        Command::new(resource_probe)
            .args(["cpu", &cpu_pid, &duration])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?,
    );
    let observation_start = Instant::now();
    let observation = Duration::from_secs(observation_seconds);
    let mut telemetry_index = 0_usize;
    let mut log_sequence = 0_u64;
    let mut peak_memory = initial_rss;
    let mut telemetry_rss_curve = vec![curve_point(0, initial_rss)];
    for (frame_index, event) in tui_events.iter().enumerate() {
        ensure_collection_running(signal)?;
        let event_timestamp = event["timestamp_ms"]
            .as_u64()
            .ok_or_else(|| invalid("TUI event timestamp must be an integer"))?;
        while telemetry_index < telemetry_records.len()
            && telemetry_records[telemetry_index]["second"]
                .as_u64()
                .ok_or_else(|| invalid("telemetry second must be an integer"))?
                .saturating_mul(1_000)
                <= event_timestamp
        {
            let record = &telemetry_records[telemetry_index];
            match record["type"]
                .as_str()
                .ok_or_else(|| invalid("telemetry type must be a string"))?
            {
                "core_log" => {
                    let level = parse_log_level(
                        record["level"]
                            .as_str()
                            .ok_or_else(|| invalid("Core Log level must be a string"))?,
                    )?;
                    let timestamp_unix_ms = record["second"]
                        .as_u64()
                        .ok_or_else(|| invalid("Core Log second must be an integer"))?
                        .saturating_mul(1_000)
                        .saturating_add(
                            record["sequence"]
                                .as_u64()
                                .ok_or_else(|| invalid("Core Log sequence must be an integer"))?,
                        );
                    let message = record["message"]
                        .as_str()
                        .ok_or_else(|| invalid("Core Log message must be a string"))?;
                    telemetry.publish_log(
                        generation,
                        timestamp_unix_ms,
                        level,
                        LogSource::CoreApi,
                        message,
                    )?;
                    log_sequence = log_sequence.saturating_add(1);
                    if state.logs.records.len() == LOG_CAPACITY {
                        state.logs.records.pop_front();
                    }
                    state.logs.records.push_back(ViewLogRecord {
                        sequence: log_sequence,
                        timestamp_unix_ms,
                        level,
                        source: LogSource::CoreApi,
                        message: message.to_owned(),
                    });
                }
                "traffic" => {
                    let upload = record["upload_bytes_per_second"]
                        .as_u64()
                        .ok_or_else(|| invalid("traffic upload rate must be an integer"))?;
                    let download = record["download_bytes_per_second"]
                        .as_u64()
                        .ok_or_else(|| invalid("traffic download rate must be an integer"))?;
                    let sampled_at = record["second"]
                        .as_u64()
                        .ok_or_else(|| invalid("traffic second must be an integer"))?
                        .saturating_mul(1_000);
                    telemetry.publish_traffic(
                        generation,
                        TrafficSample {
                            upload_bytes_per_second: upload,
                            download_bytes_per_second: download,
                            sampled_at_unix_ms: Some(sampled_at),
                            state: SampleState::Fresh,
                        },
                    );
                    push_bounded_series(&mut state.upload_series, upload);
                    push_bounded_series(&mut state.download_series, download);
                }
                _ => return Err(invalid("telemetry record type is unsupported")),
            }
            telemetry_index += 1;
        }
        state.page = parse_page(
            event["page"]
                .as_str()
                .ok_or_else(|| invalid("TUI event page must be a string"))?,
        )?;
        let _ = render_buffer(&state, area, &mut buffer);
        if frame_index % 4 == 0 {
            let current = current_rss(resource_probe)?;
            peak_memory = peak_memory.max(current);
            telemetry_rss_curve.push(curve_point(
                u64::try_from(observation_start.elapsed().as_millis())?,
                current,
            ));
        }
        let completed_frames = u32::try_from(frame_index.saturating_add(1))?;
        let total_frames = u32::try_from(tui_events.len())?;
        let next_frame =
            observation_start + observation.saturating_mul(completed_frames) / total_frames;
        if let Some(remaining) = next_frame.checked_duration_since(Instant::now()) {
            thread::sleep(remaining);
        }
    }
    if telemetry_index != telemetry_records.len() {
        return Err(invalid("telemetry schedule was not consumed completely"));
    }
    let cpu_percent = child_metric(cpu_probe, "telemetry CPU probe", signal)?;
    if telemetry.logs().len() > LOG_CAPACITY
        || telemetry.traffic_history().len() > TRAFFIC_SERIES_CAPACITY
    {
        return Err(invalid(
            "release telemetry buffers exceeded product capacity",
        ));
    }
    insert_measurement(
        &mut measurements,
        "telemetry_sustained_cpu_percent",
        cpu_percent,
    );
    insert_measurement(
        &mut measurements,
        "telemetry_peak_memory_bytes",
        peak_memory,
    );
    curves.insert(
        "telemetry_rss_bytes".to_owned(),
        Value::Array(telemetry_rss_curve),
    );
    Ok(measurements)
}

struct ProcessChildGuard {
    child: Option<Child>,
}

impl ProcessChildGuard {
    fn new(child: Child) -> Self {
        Self { child: Some(child) }
    }

    fn id(&self) -> Result<u32, Box<dyn Error>> {
        self.child
            .as_ref()
            .map(Child::id)
            .ok_or_else(|| invalid("benchmark child is unavailable"))
    }

    #[cfg(test)]
    fn into_child(mut self) -> Child {
        self.child
            .take()
            .expect("benchmark fixture child should be available")
    }

    fn wait_with_output(
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

    fn terminate_and_reap_with_timeout(
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

    fn terminate_and_collect_stderr(mut self) -> Result<(ExitStatus, String), Box<dyn Error>> {
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

struct BackgroundProcessMetrics {
    supervisor_idle_rss: f64,
    service_idle_rss: f64,
    combined_idle_rss: f64,
    wakeups_per_second: f64,
    supervisor_curve: Vec<Value>,
    service_curve: Vec<Value>,
    combined_curve: Vec<Value>,
}

fn collect_background_process_metrics(
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

struct TuiProcessMetrics {
    cold_start_ms: f64,
    idle_rss: f64,
    peak_memory: f64,
    rss_curve: Vec<Value>,
}

struct PtyChildGuard {
    child: Option<Box<dyn portable_pty::Child + Send + Sync>>,
}

impl PtyChildGuard {
    fn new(child: Box<dyn portable_pty::Child + Send + Sync>) -> Self {
        Self { child: Some(child) }
    }

    fn process_id(&self) -> Option<u32> {
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

fn collect_tui_process_metrics(
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

fn curve_point(elapsed_ms: u64, value: f64) -> Value {
    json!({
        "elapsed_ms": elapsed_ms,
        "value": value
    })
}

fn ensure_collection_running(signal: &dyn ShutdownSignal) -> Result<(), Box<dyn Error>> {
    if signal.shutdown_requested() {
        Err(invalid("release benchmark collection was interrupted"))
    } else {
        Ok(())
    }
}

fn parse_log_level(value: &str) -> Result<LogLevel, Box<dyn Error>> {
    match value {
        "debug" => Ok(LogLevel::Debug),
        "info" => Ok(LogLevel::Info),
        "warn" => Ok(LogLevel::Warn),
        "error" => Ok(LogLevel::Error),
        _ => Err(invalid("Core Log level is unsupported")),
    }
}

fn parse_page(value: &str) -> Result<Page, Box<dyn Error>> {
    match value {
        "overview" => Ok(Page::Overview),
        "proxies" => Ok(Page::Proxies),
        "profiles" => Ok(Page::Profiles),
        "logs" => Ok(Page::Logs),
        _ => Err(invalid("TUI event page is unsupported")),
    }
}

fn push_bounded_series(series: &mut VecDeque<u64>, value: u64) {
    if series.len() == TRAFFIC_SERIES_CAPACITY {
        series.pop_front();
    }
    series.push_back(value);
}

struct LifecycleGuard {
    release_binary: PathBuf,
    fixture_binary: PathBuf,
    state_root: PathBuf,
    service_socket: PathBuf,
    mihomo: PathBuf,
    supervisor: Option<ProcessIdentity>,
    service: Option<ProcessChildGuard>,
}

impl LifecycleGuard {
    fn run(&self, arguments: &[&str]) -> Result<std::process::Output, io::Error> {
        self.run_with(&self.release_binary, arguments)
    }

    fn run_fixture(&self, arguments: &[&str]) -> Result<std::process::Output, io::Error> {
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

    fn shutdown(&mut self) -> Result<(), Box<dyn Error>> {
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

fn spawn_fixture_service(
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

fn wait_for_active_status(
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

fn validate_release_application_scale(
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

fn is_retryable_error(stderr: &[u8], code: &str) -> bool {
    serde_json::from_slice::<Value>(stderr)
        .ok()
        .is_some_and(|error| error["error"]["code"] == code && error["error"]["retryable"] == true)
}

fn command_json(output: std::process::Output, label: &str) -> Result<Value, Box<dyn Error>> {
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

struct ProfileServer {
    base_url: String,
    shutdown: Arc<AtomicBool>,
    thread: Option<thread::JoinHandle<()>>,
}

impl ProfileServer {
    fn start(manifest_path: &Path, scale: WorkloadScale) -> Result<Self, Box<dyn Error>> {
        let listener = TcpListener::bind(("127.0.0.1", 0))?;
        listener.set_nonblocking(true)?;
        let address = listener.local_addr()?;
        let active_body = Arc::new(release_profile_document(manifest_path, scale)?);
        let inactive_body = Arc::new(
            concat!(
                "proxies:\n",
                "  - name: benchmark-node\n",
                "    type: ss\n",
                "    server: 127.0.0.1\n",
                "    port: 443\n",
                "    cipher: aes-128-gcm\n",
                "    password: fixture-password\n",
                "proxy-groups:\n",
                "  - name: Main\n",
                "    type: select\n",
                "    proxies: [benchmark-node, DIRECT]\n",
                "rules:\n",
                "  - MATCH,Main\n"
            )
            .as_bytes()
            .to_vec(),
        );
        let shutdown = Arc::new(AtomicBool::new(false));
        let thread_shutdown = Arc::clone(&shutdown);
        let thread = thread::spawn(move || {
            while !thread_shutdown.load(Ordering::Acquire) {
                match listener.accept() {
                    Ok((stream, _)) => {
                        let _ = serve_profile(stream, &active_body, &inactive_body);
                    }
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(10));
                    }
                    Err(_) => return,
                }
            }
        });
        Ok(Self {
            base_url: format!("http://{address}"),
            shutdown,
            thread: Some(thread),
        })
    }

    fn url(&self, index: u64) -> String {
        format!("{}/profile-{index:03}.yaml", self.base_url)
    }
}

impl Drop for ProfileServer {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Release);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

fn release_profile_document(
    manifest_path: &Path,
    scale: WorkloadScale,
) -> Result<Vec<u8>, Box<dyn Error>> {
    let root = manifest_path
        .parent()
        .ok_or_else(|| invalid("workload manifest must have a parent directory"))?;
    let nodes = read_ndjson(&root.join("active-nodes.ndjson"))?;
    if nodes.len() != usize::try_from(scale.active_nodes)? {
        return Err(invalid(
            "release Profile document Active Node count differs from its workload",
        ));
    }
    let mut body = String::with_capacity(
        usize::try_from(scale.active_nodes.saturating_mul(180))?
            .saturating_add(usize::try_from(scale.local_rules.saturating_mul(64))?),
    );
    body.push_str("proxies:\n");
    for node in &nodes {
        let name = node["name"]
            .as_str()
            .ok_or_else(|| invalid("workload Node name must be a string"))?;
        writeln!(body, "  - name: {name}")?;
        body.push_str(
            "    type: ss\n    server: 127.0.0.1\n    port: 443\n    cipher: aes-128-gcm\n    password: fixture-password\n",
        );
    }
    body.push_str("proxy-groups:\n  - name: PROXY\n    type: select\n    proxies:\n");
    for node in &nodes {
        let name = node["name"]
            .as_str()
            .ok_or_else(|| invalid("workload Node name must be a string"))?;
        writeln!(body, "      - {name}")?;
    }
    body.push_str("      - DIRECT\n");
    let rules = fs::read_to_string(root.join("rules.yaml"))?;
    if rules.lines().count() != usize::try_from(scale.local_rules.saturating_add(1))? {
        return Err(invalid(
            "release Profile document Local Rule count differs from its workload",
        ));
    }
    body.push_str(&rules);
    Ok(body.into_bytes())
}

fn serve_profile(
    mut stream: TcpStream,
    active_body: &[u8],
    inactive_body: &[u8],
) -> Result<(), io::Error> {
    stream.set_nonblocking(false)?;
    stream.set_read_timeout(Some(Duration::from_secs(5)))?;
    stream.set_write_timeout(Some(Duration::from_secs(5)))?;
    let mut request = [0_u8; 4 * 1_024];
    let read = stream.read(&mut request)?;
    let active = String::from_utf8_lossy(&request[..read]).starts_with("GET /profile-000.yaml ");
    let body = if active { active_body } else { inactive_body };
    let header = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/yaml\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(header.as_bytes())?;
    stream.write_all(body)?;
    stream.flush()
}

fn wait_for_socket(path: &Path, timeout: Duration) -> Result<(), Box<dyn Error>> {
    let deadline = Instant::now() + timeout;
    loop {
        match fs::symlink_metadata(path) {
            Ok(metadata) if metadata.file_type().is_socket() => return Ok(()),
            Ok(_) => return Err(invalid("fixture Core service endpoint is not a socket")),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(Box::new(error)),
        }
        if Instant::now() >= deadline {
            return Err(invalid("fixture Core service did not become ready"));
        }
        thread::sleep(Duration::from_millis(10));
    }
}

fn read_ndjson(path: &Path) -> Result<Vec<Value>, Box<dyn Error>> {
    BufReader::new(File::open(path)?)
        .lines()
        .map(|line| Ok(serde_json::from_str(&line?)?))
        .collect()
}

fn current_rss(resource_probe: &Path) -> Result<f64, Box<dyn Error>> {
    resource_metric(resource_probe, "rss", &[std::process::id().to_string()])
}

fn resource_metric(
    resource_probe: &Path,
    mode: &str,
    arguments: &[String],
) -> Result<f64, Box<dyn Error>> {
    let output = Command::new(resource_probe)
        .arg(mode)
        .args(arguments)
        .output()?;
    if !output.status.success() {
        return Err(invalid(format!(
            "resource probe {mode} failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }
    let value = String::from_utf8(output.stdout)?.trim().parse::<f64>()?;
    if value.is_finite() && value >= 0.0 {
        Ok(value)
    } else {
        Err(invalid(format!(
            "resource probe {mode} returned an invalid value"
        )))
    }
}

fn child_metric(
    child: ProcessChildGuard,
    label: &str,
    signal: &dyn ShutdownSignal,
) -> Result<f64, Box<dyn Error>> {
    let output = child.wait_with_output(PROBE_COMPLETION_TIMEOUT, signal)?;
    if !output.status.success() {
        return Err(invalid(format!(
            "{label} failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }
    let value = String::from_utf8(output.stdout)?.trim().parse::<f64>()?;
    if value.is_finite() && value >= 0.0 {
        Ok(value)
    } else {
        Err(invalid(format!("{label} returned an invalid value")))
    }
}

fn insert_measurement(measurements: &mut Map<String, Value>, key: &str, value: f64) {
    measurements.insert(key.to_owned(), Value::from(value));
}

fn elapsed_ms(start: Instant) -> f64 {
    start.elapsed().as_secs_f64() * 1_000.0
}

fn capture_results(
    metadata_path: &Path,
    manifest_path: &Path,
    samples_directory: &Path,
    output: &Path,
) -> Result<(), Box<dyn Error>> {
    let metadata = read_json(metadata_path)?;
    let environment = capture_environment(&metadata)?;
    capture_results_with_environment(
        metadata_path,
        manifest_path,
        samples_directory,
        output,
        environment,
    )
}

fn capture_results_with_environment(
    metadata_path: &Path,
    manifest_path: &Path,
    samples_directory: &Path,
    output: &Path,
    environment: Value,
) -> Result<(), Box<dyn Error>> {
    let metadata = read_json(metadata_path)?;
    validate_metadata(&metadata, false)?;
    if metadata["status"] != "capture_required" {
        return Err(invalid("capture requires capture_required metadata"));
    }
    let manifest = read_json(manifest_path)?;
    validate_release_manifest(&manifest, &metadata)?;
    validate_manifest_artifacts(manifest_path, &manifest)?;
    let workload_manifest_sha256 = sha256_file(manifest_path)?;
    let expected_samples = metadata["measurement_environment"]["samples"]
        .as_u64()
        .ok_or_else(|| invalid("measurement_environment.samples must be an integer"))?;
    let sample_capacity = usize::try_from(expected_samples)?;
    let expected_observations = ObservationDurations {
        background_seconds: metadata["measurement_environment"]["idle_observation_seconds"]
            .as_u64()
            .ok_or_else(|| invalid("idle observation must be an integer"))?,
        telemetry_seconds: metadata["measurement_environment"]["telemetry_observation_seconds"]
            .as_u64()
            .ok_or_else(|| invalid("telemetry observation must be an integer"))?,
        tui_seconds: metadata["measurement_environment"]["tui_observation_seconds"]
            .as_u64()
            .ok_or_else(|| invalid("TUI observation must be an integer"))?,
    };
    let mut values = MEASUREMENT_KEYS
        .iter()
        .map(|key| ((*key).to_owned(), Vec::with_capacity(sample_capacity)))
        .collect::<std::collections::BTreeMap<_, _>>();
    let mut raw_samples = Vec::with_capacity(sample_capacity);
    let mut report_environment = None;
    let mut report_inputs = None;
    for sample in 1..=expected_samples {
        let path = samples_directory.join(format!("sample-{sample:02}.json"));
        let sample = read_json(&path)?;
        require_u64(&sample, "schema_version", 1)?;
        validate_sample_collector(&sample, false)?;
        validate_observation_durations(&sample, expected_observations)?;
        let sample_environment = sample
            .get("environment")
            .ok_or_else(|| invalid(format!("{} environment is missing", path.display())))?;
        validate_sample_environment(sample_environment, &environment)?;
        if let Some(first) = &report_environment {
            validate_sample_environment(sample_environment, first)?;
        } else {
            report_environment = Some(sample_environment.clone());
        }
        let sample_inputs = sample
            .get("inputs")
            .ok_or_else(|| invalid(format!("{} inputs are missing", path.display())))?;
        validate_sample_inputs(sample_inputs)?;
        if let Some(first) = &report_inputs {
            if sample_inputs != first {
                return Err(invalid("release samples use different executable inputs"));
            }
        } else {
            report_inputs = Some(sample_inputs.clone());
        }
        if sample["workload_manifest_sha256"] != workload_manifest_sha256 {
            return Err(invalid(format!(
                "{} is bound to a different workload manifest",
                path.display()
            )));
        }
        let measurements = sample["measurements"]
            .as_object()
            .ok_or_else(|| invalid(format!("{} measurements must be an object", path.display())))?;
        exact_numeric_measurements(measurements)?;
        validate_curves(
            sample["curves"]
                .as_object()
                .ok_or_else(|| invalid(format!("{} curves must be an object", path.display())))?,
            expected_observations,
        )?;
        raw_samples.push(json!({
            "path": format!("samples/sample-{sample:02}.json"),
            "sha256": sha256_file(&path)?,
            "measurements": measurements
        }));
        for key in MEASUREMENT_KEYS {
            values
                .get_mut(key)
                .expect("measurement keys are initialized")
                .push(non_negative_number(&measurements[key], key)?);
        }
    }
    let measurements = median_measurements(values)?;
    let report = json!({
        "schema_version": 1,
        "status": "review_required",
        "capture_tool": {
            "name": "hopash-release-benchmark",
            "version": CAPTURE_TOOL_VERSION
        },
        "environment": report_environment
            .ok_or_else(|| invalid("release report has no sample environment"))?,
        "inputs": report_inputs
            .ok_or_else(|| invalid("release report has no sample inputs"))?,
        "workload": {
            "manifest_sha256": workload_manifest_sha256,
            "generator": manifest["generator"].clone(),
            "scale": manifest["scale"].clone()
        },
        "samples": expected_samples,
        "summary_statistic": "median",
        "measurements": measurements,
        "raw_samples": raw_samples
    });
    write_json_new(output, &report)?;
    Ok(())
}

fn run_smoke(
    metadata_path: &Path,
    release_binary: &Path,
    fixture_binary: &Path,
) -> Result<(), Box<dyn Error>> {
    validate_metadata_file(metadata_path, false)?;
    let root = TemporaryDirectory::new()?;
    let manifest = generate_workload(&root.path.join("workload"), SMOKE_SCALE)?;
    let generated = read_json(&manifest)?;
    validate_manifest_scale(&generated, SMOKE_SCALE)?;
    let resource_probe = root.path.join("resource-probe");
    fs::write(
        &resource_probe,
        b"#!/bin/sh\ncase \"$1\" in rss) echo 1048576 ;; cpu) echo 0.5 ;; wakeups) echo 0.25 ;; *) exit 1 ;; esac\n",
    )?;
    fs::set_permissions(&resource_probe, fs::Permissions::from_mode(0o700))?;
    let sample = root.path.join("sample.json");
    collect_sample(
        metadata_path,
        &manifest,
        release_binary,
        fixture_binary,
        &resource_probe,
        &sample,
        true,
    )?;
    let sample = read_json(&sample)?;
    if sample["workload_manifest_sha256"] != sha256_file(&manifest)? {
        return Err(invalid("smoke sample workload binding changed"));
    }
    exact_numeric_measurements(
        sample["measurements"]
            .as_object()
            .ok_or_else(|| invalid("smoke sample measurements must be an object"))?,
    )?;
    validate_curves(
        sample["curves"]
            .as_object()
            .ok_or_else(|| invalid("smoke sample curves must be an object"))?,
        SMOKE_OBSERVATIONS,
    )?;
    println!("release benchmark smoke passed");
    Ok(())
}

fn validate_release_manifest(manifest: &Value, metadata: &Value) -> Result<(), Box<dyn Error>> {
    validate_manifest_scale(manifest, RELEASE_SCALE)?;
    for key in [
        "profiles",
        "active_nodes",
        "local_rules",
        "tui_duration_seconds",
    ] {
        if manifest["scale"][key] != metadata["workloads"][key] {
            return Err(invalid(format!(
                "workload manifest {key} differs from benchmark metadata"
            )));
        }
    }
    Ok(())
}

fn validate_manifest_artifacts(
    manifest_path: &Path,
    manifest: &Value,
) -> Result<(), Box<dyn Error>> {
    let root = manifest_path
        .parent()
        .ok_or_else(|| invalid("workload manifest must have a parent directory"))?;
    let artifacts = require_object(manifest, "artifacts")?;
    for name in [
        "profiles.ndjson",
        "active-nodes.ndjson",
        "rules.yaml",
        "telemetry.ndjson",
        "tui-events.ndjson",
    ] {
        let expected = artifacts
            .get(name)
            .and_then(Value::as_object)
            .ok_or_else(|| invalid(format!("workload artifact {name} is missing")))?;
        let path = root.join(name);
        let expected_bytes = expected["bytes"]
            .as_u64()
            .ok_or_else(|| invalid(format!("workload artifact {name} bytes are invalid")))?;
        if fs::metadata(&path)?.len() != expected_bytes {
            return Err(invalid(format!(
                "workload artifact {name} byte count changed"
            )));
        }
        let expected_records = expected["records"]
            .as_u64()
            .ok_or_else(|| invalid(format!("workload artifact {name} records are invalid")))?;
        let mut records = 0_u64;
        for line in BufReader::new(File::open(&path)?).lines() {
            line?;
            records += 1;
        }
        if records != expected_records {
            return Err(invalid(format!(
                "workload artifact {name} record count changed"
            )));
        }
        let digest = sha256_file(&path)?;
        if expected["sha256"].as_str() != Some(digest.as_str()) {
            return Err(invalid(format!("workload artifact {name} digest changed")));
        }
    }
    Ok(())
}

fn validate_manifest_scale(
    manifest: &Value,
    expected: WorkloadScale,
) -> Result<(), Box<dyn Error>> {
    require_u64(manifest, "schema_version", 1)?;
    let generator = require_object(manifest, "generator")?;
    require_string_object(generator, "name", "hopash-release-workload")?;
    require_u64_object(generator, "version", WORKLOAD_GENERATOR_VERSION)?;
    require_u64_object(generator, "seed", WORKLOAD_SEED)?;
    let scale = require_object(manifest, "scale")?;
    for (key, value) in [
        ("profiles", expected.profiles),
        ("active_nodes", expected.active_nodes),
        ("local_rules", expected.local_rules),
        ("telemetry_duration_seconds", expected.telemetry_seconds),
        ("tui_duration_seconds", expected.telemetry_seconds),
    ] {
        require_u64_object(scale, key, value)?;
    }
    Ok(())
}

fn capture_environment(metadata: &Value) -> Result<Value, Box<dyn Error>> {
    let runner = metadata["measurement_environment"]["runner"]
        .as_str()
        .ok_or_else(|| invalid("measurement_environment.runner must be a string"))?;
    let project_root = project_root()?;
    Ok(json!({
        "runner": runner,
        "runner_profile": capture_runner_profile()?,
        "rustc_version": command_output("rustc", &["--version"])?,
        "cargo_version": command_output("cargo", &["--version"])?,
        "git_revision": command_output("git", &["rev-parse", "HEAD"])?,
        "source_tree_sha256": source_tree_sha256()?,
        "cargo_lock_sha256": sha256_file(&project_root.join("Cargo.lock"))?,
        "collector_sha256": sha256_file(&project_root.join("examples/release-benchmark.rs"))?,
        "captured_at_unix_seconds": unix_seconds()?
    }))
}

fn validate_sample_environment(sample: &Value, expected: &Value) -> Result<(), Box<dyn Error>> {
    let sample = sample
        .as_object()
        .ok_or_else(|| invalid("sample environment must be an object"))?;
    let expected = expected
        .as_object()
        .ok_or_else(|| invalid("expected sample environment must be an object"))?;
    for field in [
        "runner",
        "runner_profile",
        "rustc_version",
        "cargo_version",
        "git_revision",
        "source_tree_sha256",
        "cargo_lock_sha256",
        "collector_sha256",
    ] {
        if sample.get(field) != expected.get(field) {
            return Err(invalid(format!(
                "sample environment field {field} differs across the capture"
            )));
        }
    }
    require_positive_u64_object(sample, "captured_at_unix_seconds")
}

fn validate_sample_collector(sample: &Value, expected_smoke: bool) -> Result<(), Box<dyn Error>> {
    let collector = require_object(sample, "collector")?;
    require_string_object(collector, "name", "hopash-release-benchmark")?;
    require_u64_object(collector, "version", CAPTURE_TOOL_VERSION)?;
    require_bool_object(collector, "smoke", expected_smoke)
}

fn validate_sample_inputs(inputs: &Value) -> Result<(), Box<dyn Error>> {
    let inputs = inputs
        .as_object()
        .ok_or_else(|| invalid("sample inputs must be an object"))?;
    if inputs.len() != 3 {
        return Err(invalid(
            "sample inputs must contain exactly three executable digests",
        ));
    }
    for field in [
        "release_binary_sha256",
        "fixture_binary_sha256",
        "resource_probe_sha256",
    ] {
        validate_sha256_string(
            inputs
                .get(field)
                .ok_or_else(|| invalid(format!("sample input {field} is missing")))?,
            field,
        )?;
    }
    Ok(())
}

fn project_root() -> Result<PathBuf, Box<dyn Error>> {
    Ok(PathBuf::from(command_output(
        "git",
        &["rev-parse", "--show-toplevel"],
    )?))
}

fn source_tree_sha256() -> Result<String, Box<dyn Error>> {
    let tree = command_output("git", &["ls-tree", "-r", "--full-tree", "HEAD"])?;
    let mut digest = Sha256::new();
    for line in tree.lines() {
        let path = line
            .split_once('\t')
            .map(|(_, path)| path)
            .ok_or_else(|| invalid("Git tree output is invalid"))?;
        if path == "fixtures/release/benchmark-metadata-v1.json" {
            continue;
        }
        digest.update(line.as_bytes());
        digest.update(b"\n");
    }
    Ok(digest
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}

fn capture_runner_profile() -> Result<Value, Box<dyn Error>> {
    if cfg!(target_os = "macos") {
        return Ok(json!({
            "architecture": env::consts::ARCH,
            "hardware_model": command_output("sysctl", &["-n", "hw.model"] ).ok(),
            "cpu_model": command_output("sysctl", &["-n", "machdep.cpu.brand_string"] ).ok(),
            "logical_cpu_count": command_u64("sysctl", &["-n", "hw.logicalcpu"] ).ok(),
            "memory_bytes": command_u64("sysctl", &["-n", "hw.memsize"] ).ok(),
            "os_version": os_version()?
        }));
    }

    Ok(json!({
        "architecture": env::consts::ARCH,
        "hardware_model": read_trimmed("/sys/devices/virtual/dmi/id/product_name"),
        "cpu_model": linux_cpu_model(),
        "logical_cpu_count": std::thread::available_parallelism().ok().map(|count| count.get()),
        "memory_bytes": linux_memory_bytes(),
        "os_version": os_version()?
    }))
}

fn os_version() -> Result<String, Box<dyn Error>> {
    if cfg!(target_os = "macos") {
        command_output("sw_vers", &["-productVersion"])
    } else {
        command_output("uname", &["-sr"])
    }
}

fn command_output(program: &str, arguments: &[&str]) -> Result<String, Box<dyn Error>> {
    let output = Command::new(program).args(arguments).output()?;
    if !output.status.success() {
        return Err(invalid(format!(
            "{program} failed while recording metadata"
        )));
    }
    let value = String::from_utf8(output.stdout)?.trim().to_owned();
    if value.is_empty() {
        return Err(invalid(format!("{program} returned empty metadata")));
    }
    Ok(value)
}

fn command_u64(program: &str, arguments: &[&str]) -> Result<u64, Box<dyn Error>> {
    Ok(command_output(program, arguments)?.parse()?)
}

fn read_trimmed(path: &str) -> Option<String> {
    fs::read_to_string(path)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

fn linux_cpu_model() -> Option<String> {
    fs::read_to_string("/proc/cpuinfo")
        .ok()
        .and_then(|content| {
            content.lines().find_map(|line| {
                line.strip_prefix("model name")
                    .and_then(|value| value.split_once(':'))
                    .map(|(_, value)| value.trim().to_owned())
            })
        })
}

fn linux_memory_bytes() -> Option<u64> {
    fs::read_to_string("/proc/meminfo")
        .ok()
        .and_then(|content| {
            content.lines().find_map(|line| {
                let kibibytes = line.strip_prefix("MemTotal:")?.split_whitespace().next()?;
                kibibytes.parse::<u64>().ok()?.checked_mul(1_024)
            })
        })
}

fn artifact_metadata(path: &Path, records: u64) -> Result<Value, Box<dyn Error>> {
    Ok(json!({
        "bytes": fs::metadata(path)?.len(),
        "records": records,
        "sha256": sha256_file(path)?
    }))
}

fn sha256_file(path: &Path) -> Result<String, Box<dyn Error>> {
    let content = fs::read(path)?;
    Ok(Sha256::digest(content)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}

fn sha256_pretty_json(value: &Value) -> Result<String, Box<dyn Error>> {
    let mut content = serde_json::to_vec_pretty(value)?;
    content.push(b'\n');
    Ok(Sha256::digest(content)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}

fn validate_sha256_string(value: &Value, label: &str) -> Result<(), Box<dyn Error>> {
    let digest = value
        .as_str()
        .ok_or_else(|| invalid(format!("{label} must be a string")))?;
    if digest.len() == 64 && digest.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        Ok(())
    } else {
        Err(invalid(format!(
            "{label} must be a hexadecimal SHA-256 digest"
        )))
    }
}

fn new_buffered_file(path: &Path) -> Result<BufWriter<File>, io::Error> {
    Ok(BufWriter::new(
        OpenOptions::new().write(true).create_new(true).open(path)?,
    ))
}

fn write_json_line(output: &mut impl Write, value: &Value) -> Result<(), Box<dyn Error>> {
    serde_json::to_writer(&mut *output, value)?;
    output.write_all(b"\n")?;
    Ok(())
}

fn read_json(path: &Path) -> Result<Value, Box<dyn Error>> {
    Ok(serde_json::from_slice(&fs::read(path)?)?)
}

fn write_json_new(path: &Path, value: &Value) -> Result<(), Box<dyn Error>> {
    let mut output = new_buffered_file(path)?;
    serde_json::to_writer_pretty(&mut output, value)?;
    output.write_all(b"\n")?;
    output.flush()?;
    Ok(())
}

fn exact_measurement_map<'a>(
    parent: &'a Value,
    field: &str,
) -> Result<&'a Map<String, Value>, Box<dyn Error>> {
    let values = require_object(parent, field)?;
    exact_measurement_keys(values, field)?;
    Ok(values)
}

fn exact_numeric_measurements(values: &Map<String, Value>) -> Result<(), Box<dyn Error>> {
    exact_measurement_keys(values, "measurements")?;
    for key in MEASUREMENT_KEYS {
        non_negative_number(&values[key], key)?;
    }
    Ok(())
}

fn median_measurements(
    values: std::collections::BTreeMap<String, Vec<f64>>,
) -> Result<Map<String, Value>, Box<dyn Error>> {
    values
        .into_iter()
        .map(|(key, mut samples)| {
            if samples.is_empty() {
                return Err(invalid(format!("measurement {key} has no samples")));
            }
            samples.sort_by(f64::total_cmp);
            let middle = samples.len() / 2;
            let median = if samples.len() % 2 == 0 {
                (samples[middle - 1] + samples[middle]) / 2.0
            } else {
                samples[middle]
            };
            Ok((key, Value::from(median)))
        })
        .collect()
}

fn validate_curves(
    curves: &Map<String, Value>,
    observations: ObservationDurations,
) -> Result<(), Box<dyn Error>> {
    if curves.len() != CURVE_KEYS.len() || CURVE_KEYS.iter().any(|name| !curves.contains_key(*name))
    {
        return Err(invalid(
            "sample curves must contain exactly the five versioned RSS series",
        ));
    }
    for (name, curve) in curves {
        let points = curve
            .as_array()
            .filter(|points| !points.is_empty())
            .ok_or_else(|| invalid(format!("{name} curve must be non-empty")))?;
        let expected_seconds = match name.as_str() {
            "supervisor_rss_bytes"
            | "privileged_service_rss_bytes"
            | "combined_background_rss_bytes" => observations.background_seconds,
            "telemetry_rss_bytes" => observations.telemetry_seconds,
            "tui_rss_bytes" => observations.tui_seconds,
            _ => unreachable!("curve keys are validated above"),
        };
        let minimum_points = usize::try_from(expected_seconds.max(1))?;
        if points.len() < minimum_points {
            return Err(invalid(format!(
                "{name} curve does not contain the required sampling coverage"
            )));
        }
        let mut previous = None;
        for point in points {
            let elapsed = point["elapsed_ms"]
                .as_u64()
                .ok_or_else(|| invalid(format!("{name} elapsed time must be an integer")))?;
            non_negative_number(&point["value"], name)?;
            if previous.is_some_and(|value| elapsed < value) {
                return Err(invalid(format!("{name} elapsed time must be monotonic")));
            }
            previous = Some(elapsed);
        }
        if points[0]["elapsed_ms"] != 0
            || previous.unwrap_or_default()
                < expected_seconds.saturating_sub(1).saturating_mul(1_000)
        {
            return Err(invalid(format!(
                "{name} curve does not cover its declared observation duration"
            )));
        }
    }
    Ok(())
}

fn validate_observation_durations(
    sample: &Value,
    expected: ObservationDurations,
) -> Result<(), Box<dyn Error>> {
    let observations = require_object(sample, "observation_seconds")?;
    require_u64_object(observations, "background", expected.background_seconds)?;
    require_u64_object(observations, "telemetry", expected.telemetry_seconds)?;
    require_u64_object(observations, "tui", expected.tui_seconds)
}

fn exact_measurement_keys(values: &Map<String, Value>, field: &str) -> Result<(), Box<dyn Error>> {
    if values.len() != MEASUREMENT_KEYS.len()
        || MEASUREMENT_KEYS
            .iter()
            .any(|key| !values.contains_key(*key))
    {
        return Err(invalid(format!(
            "{field} must contain exactly the 21 versioned measurement keys"
        )));
    }
    Ok(())
}

fn non_negative_number(value: &Value, label: &str) -> Result<f64, Box<dyn Error>> {
    value
        .as_f64()
        .filter(|value| value.is_finite() && *value >= 0.0)
        .ok_or_else(|| invalid(format!("{label} must be a finite non-negative number")))
}

fn require_object<'a>(
    parent: &'a Value,
    field: &str,
) -> Result<&'a Map<String, Value>, Box<dyn Error>> {
    parent
        .get(field)
        .and_then(Value::as_object)
        .ok_or_else(|| invalid(format!("{field} must be an object")))
}

fn require_string<'a>(parent: &'a Value, field: &str) -> Result<&'a str, Box<dyn Error>> {
    parent
        .get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| invalid(format!("{field} must be a string")))
}

fn require_non_empty_string(
    parent: &Map<String, Value>,
    field: &str,
) -> Result<(), Box<dyn Error>> {
    parent
        .get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(|_| ())
        .ok_or_else(|| invalid(format!("{field} must be a non-empty string")))
}

fn require_u64(parent: &Value, field: &str, expected: u64) -> Result<(), Box<dyn Error>> {
    parent
        .get(field)
        .and_then(Value::as_u64)
        .filter(|value| *value == expected)
        .map(|_| ())
        .ok_or_else(|| invalid(format!("{field} must equal {expected}")))
}

fn require_u64_object(
    parent: &Map<String, Value>,
    field: &str,
    expected: u64,
) -> Result<(), Box<dyn Error>> {
    parent
        .get(field)
        .and_then(Value::as_u64)
        .filter(|value| *value == expected)
        .map(|_| ())
        .ok_or_else(|| invalid(format!("{field} must equal {expected}")))
}

fn require_positive_u64_object(
    parent: &Map<String, Value>,
    field: &str,
) -> Result<(), Box<dyn Error>> {
    parent
        .get(field)
        .and_then(Value::as_u64)
        .filter(|value| *value > 0)
        .map(|_| ())
        .ok_or_else(|| invalid(format!("{field} must be positive")))
}

fn require_string_object(
    parent: &Map<String, Value>,
    field: &str,
    expected: &str,
) -> Result<(), Box<dyn Error>> {
    parent
        .get(field)
        .and_then(Value::as_str)
        .filter(|value| *value == expected)
        .map(|_| ())
        .ok_or_else(|| invalid(format!("{field} must equal {expected}")))
}

fn require_bool_object(
    parent: &Map<String, Value>,
    field: &str,
    expected: bool,
) -> Result<(), Box<dyn Error>> {
    parent
        .get(field)
        .and_then(Value::as_bool)
        .filter(|value| *value == expected)
        .map(|_| ())
        .ok_or_else(|| invalid(format!("{field} must equal {expected}")))
}

fn unix_seconds() -> Result<u64, Box<dyn Error>> {
    Ok(SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs())
}

fn invalid(message: impl Into<String>) -> Box<dyn Error> {
    Box::new(io::Error::new(io::ErrorKind::InvalidData, message.into()))
}

struct TemporaryDirectory {
    path: PathBuf,
}

impl TemporaryDirectory {
    fn new() -> Result<Self, io::Error> {
        let identifier = uuid::Uuid::new_v4().simple().to_string();
        let path = env::temp_dir().join(format!("h{}", &identifier[..6]));
        fs::create_dir(&path)?;
        fs::set_permissions(&path, fs::Permissions::from_mode(0o700))?;
        Ok(Self { path })
    }
}

impl Drop for TemporaryDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct RequestedShutdown;

    impl ShutdownSignal for RequestedShutdown {
        fn shutdown_requested(&self) -> bool {
            true
        }
    }

    #[test]
    fn release_workload_is_deterministic_and_complete() {
        let first = TemporaryDirectory::new().expect("fixture should exist");
        let second = TemporaryDirectory::new().expect("fixture should exist");
        let first_manifest = generate_workload(&first.path.join("workload"), RELEASE_SCALE)
            .expect("release workload should generate");
        let second_manifest = generate_workload(&second.path.join("workload"), RELEASE_SCALE)
            .expect("release workload should generate");
        let first = read_json(&first_manifest).expect("first manifest should parse");
        let second = read_json(&second_manifest).expect("second manifest should parse");

        assert_eq!(first, second);
        validate_manifest_scale(&first, RELEASE_SCALE).expect("release scale should validate");
        assert_eq!(first["scale"]["core_log_records"], 7_200);
        assert_eq!(first["scale"]["traffic_sample_records"], 1_800);
        assert_eq!(first["scale"]["tui_frame_records"], 7_200);
        let workload_root = first_manifest
            .parent()
            .expect("workload manifest should have a parent");
        let profile = read_ndjson(&workload_root.join("profiles.ndjson"))
            .expect("Profiles should parse")
            .remove(0);
        ProfileId::parse(
            profile["id"]
                .as_str()
                .expect("Profile ID should be a string"),
        )
        .expect("generated Profile ID should use the product format");
        let node = read_ndjson(&workload_root.join("active-nodes.ndjson"))
            .expect("Nodes should parse")
            .remove(0);
        NodeRecordId::parse(node["id"].as_str().expect("Node ID should be a string"))
            .expect("generated Node ID should use the product format");
    }

    #[test]
    fn capture_required_metadata_keeps_every_gate_value_empty() {
        let metadata = read_json(&project_path("fixtures/release/benchmark-metadata-v1.json"))
            .expect("benchmark metadata should parse");

        validate_metadata(&metadata, false).expect("capture metadata should validate");
        assert!(validate_metadata(&metadata, true).is_err());
    }

    #[test]
    fn approved_metadata_enforces_thresholds_and_regression_budgets() {
        let mut metadata = read_json(&project_path("fixtures/release/benchmark-metadata-v1.json"))
            .expect("benchmark metadata should parse");
        metadata["status"] = Value::from("approved");
        for field in [
            "measurements",
            "baseline_measurements",
            "thresholds",
            "regression_budgets_percent",
        ] {
            let values = metadata[field]
                .as_object_mut()
                .expect("gate values should be objects");
            for key in MEASUREMENT_KEYS {
                values[key] = Value::from(match field {
                    "measurements" | "baseline_measurements" => 100.0,
                    "thresholds" => 110.0,
                    "regression_budgets_percent" => 10.0,
                    _ => unreachable!(),
                });
            }
        }
        let runner_profile = json!({
            "architecture": "aarch64",
            "hardware_model": "MacFixture1,1",
            "cpu_model": "Apple Fixture",
            "logical_cpu_count": 8,
            "memory_bytes": 17_179_869_184_u64,
            "os_version": "15.6"
        });
        metadata["measurement_environment"]["runner_profile"] = runner_profile.clone();
        let approval_workload =
            TemporaryDirectory::new().expect("approval workload root should exist");
        let approval_manifest =
            generate_workload(&approval_workload.path.join("workload"), RELEASE_SCALE)
                .expect("approval workload should generate");
        let root = project_root().expect("project root should resolve");
        let capture_environment = json!({
            "runner": "dedicated macOS 15 arm64 release runner",
            "runner_profile": runner_profile,
            "rustc_version": command_output("rustc", &["--version"]).expect("rustc version"),
            "cargo_version": command_output("cargo", &["--version"]).expect("cargo version"),
            "git_revision": command_output("git", &["rev-parse", "HEAD"]).expect("Git revision"),
            "source_tree_sha256": source_tree_sha256().expect("source tree digest"),
            "cargo_lock_sha256": sha256_file(&root.join("Cargo.lock")).expect("Cargo lock digest"),
            "collector_sha256": sha256_file(&root.join("examples/release-benchmark.rs")).expect("collector digest"),
            "captured_at_unix_seconds": 1_700_000_000_u64
        });
        let workload_digest = sha256_file(&approval_manifest).expect("workload digest");
        let reviewed_report = json!({
            "schema_version": 1,
            "status": "review_required",
            "capture_tool": {
                "name": "hopash-release-benchmark",
                "version": CAPTURE_TOOL_VERSION
            },
            "environment": capture_environment,
            "inputs": {
                "release_binary_sha256": "1".repeat(64),
                "fixture_binary_sha256": "2".repeat(64),
                "resource_probe_sha256": "3".repeat(64)
            },
            "workload": {
                "manifest_sha256": workload_digest,
                "generator": {
                    "name": "hopash-release-workload",
                    "version": WORKLOAD_GENERATOR_VERSION,
                    "seed": WORKLOAD_SEED
                },
                "scale": {
                    "profiles": RELEASE_SCALE.profiles,
                    "active_nodes": RELEASE_SCALE.active_nodes,
                    "local_rules": RELEASE_SCALE.local_rules
                }
            },
            "samples": 10,
            "summary_statistic": "median",
            "measurements": metadata["measurements"].clone(),
            "raw_samples": (1..=10).map(|sample| json!({
                "path": format!("samples/sample-{sample:02}.json"),
                "sha256": format!("{sample:064x}"),
                "measurements": metadata["measurements"]
            })).collect::<Vec<_>>()
        });
        let mut approved_capture = reviewed_report["environment"].clone();
        approved_capture["workload_manifest_sha256"] = Value::from(workload_digest);
        approved_capture["samples"] = Value::from(10);
        approved_capture["summary_statistic"] = Value::from("median");
        approved_capture["reviewed_report_sha256"] =
            Value::from(sha256_pretty_json(&reviewed_report).expect("reviewed report digest"));
        approved_capture["reviewed_report"] = reviewed_report;
        metadata["approved_capture"] = approved_capture;

        validate_metadata(&metadata, true).expect("approved metadata should validate");
        metadata["measurements"]["wrapper_binary_bytes"] = Value::from(101.0);
        assert!(validate_metadata(&metadata, true).is_err());
        metadata["target"] = Value::from("x86_64-apple-darwin");
        validate_metadata(&metadata, true)
            .expect("derived target metadata may replace only executable size");
        metadata["measurements"][MEASUREMENT_KEYS[0]] = Value::from(111.0);
        assert!(validate_metadata(&metadata, true).is_err());
    }

    #[test]
    fn capture_rejects_an_incomplete_measurement_set() {
        let mut measurements = MEASUREMENT_KEYS
            .iter()
            .map(|key| ((*key).to_owned(), Value::from(1)))
            .collect::<Map<_, _>>();
        measurements.remove(MEASUREMENT_KEYS[0]);

        assert!(exact_numeric_measurements(&measurements).is_err());
    }

    #[test]
    fn release_aggregation_rejects_smoke_samples_and_short_curves() {
        let smoke = json!({
            "collector": {
                "name": "hopash-release-benchmark",
                "version": CAPTURE_TOOL_VERSION,
                "smoke": true
            }
        });
        assert!(validate_sample_collector(&smoke, false).is_err());

        let one_point = json!([curve_point(0, 1.0)]);
        let curves = CURVE_KEYS
            .into_iter()
            .map(|name| (name.to_owned(), one_point.clone()))
            .collect::<Map<_, _>>();
        assert!(
            validate_curves(
                &curves,
                ObservationDurations {
                    background_seconds: 60,
                    telemetry_seconds: 1_800,
                    tui_seconds: 1_800,
                },
            )
            .is_err()
        );
    }

    #[test]
    fn summary_requires_all_twenty_one_measurements_and_uses_the_median() {
        let fixture = TemporaryDirectory::new().expect("fixture should exist");
        let metadata_path = project_path("fixtures/release/benchmark-metadata-v1.json");
        let workload = generate_workload(&fixture.path.join("workload"), RELEASE_SCALE)
            .expect("release workload should generate");
        let workload_digest = sha256_file(&workload).expect("workload digest should compute");
        let samples = fixture.path.join("samples");
        fs::create_dir(&samples).expect("sample directory should exist");
        let expected_environment = json!({
            "runner": "dedicated macOS 15 arm64 release runner",
            "runner_profile": {
                "architecture": "aarch64",
                "hardware_model": "MacFixture1,1",
                "cpu_model": "Apple Fixture",
                "logical_cpu_count": 8,
                "memory_bytes": 17_179_869_184_u64,
                "os_version": "15.6"
            },
            "rustc_version": "rustc fixture",
            "cargo_version": "cargo fixture",
            "git_revision": "0123456789abcdef",
            "source_tree_sha256": "a".repeat(64),
            "cargo_lock_sha256": "b".repeat(64),
            "collector_sha256": "c".repeat(64),
            "captured_at_unix_seconds": 1_700_000_000_u64
        });
        let inputs = json!({
            "release_binary_sha256": "1".repeat(64),
            "fixture_binary_sha256": "2".repeat(64),
            "resource_probe_sha256": "3".repeat(64)
        });
        let curve = (0..1_800_u64)
            .map(|second| curve_point(second * 1_000, 1.0))
            .collect::<Vec<_>>();
        for sample in 1..=10 {
            let measurements = Value::Object(
                MEASUREMENT_KEYS
                    .iter()
                    .enumerate()
                    .map(|(index, key)| {
                        ((*key).to_owned(), Value::from(sample as f64 + index as f64))
                    })
                    .collect(),
            );
            write_json_new(
                &samples.join(format!("sample-{sample:02}.json")),
                &json!({
                    "schema_version": 1,
                    "workload_manifest_sha256": workload_digest,
                    "collector": {
                        "name": "hopash-release-benchmark",
                        "version": CAPTURE_TOOL_VERSION,
                        "smoke": false
                    },
                    "environment": expected_environment,
                    "inputs": inputs,
                    "observation_seconds": {
                        "background": 60,
                        "telemetry": 1_800,
                        "tui": 1_800
                    },
                    "measurements": measurements,
                    "curves": {
                        "supervisor_rss_bytes": curve,
                        "privileged_service_rss_bytes": curve,
                        "combined_background_rss_bytes": curve,
                        "telemetry_rss_bytes": curve,
                        "tui_rss_bytes": curve
                    }
                }),
            )
            .expect("sample should be written");
        }
        let report_path = fixture.path.join("report.json");
        capture_results_with_environment(
            &metadata_path,
            &workload,
            &samples,
            &report_path,
            expected_environment,
        )
        .expect("capture should summarize");
        let report = read_json(&report_path).expect("report should parse");

        assert_eq!(report["status"], "review_required");
        assert_eq!(report["samples"], 10);
        assert_eq!(report["measurements"][MEASUREMENT_KEYS[0]], 5.5);
        assert_eq!(
            report["measurements"]
                .as_object()
                .expect("measurements should be an object")
                .len(),
            21
        );
    }

    #[test]
    fn interrupted_collection_scope_reaps_resource_probe_children() {
        let telemetry = ProcessChildGuard::new(
            Command::new("/bin/sleep")
                .arg("30")
                .spawn()
                .expect("telemetry probe fixture should start"),
        );
        let wakeup = ProcessChildGuard::new(
            Command::new("/bin/sleep")
                .arg("30")
                .spawn()
                .expect("wakeup probe fixture should start"),
        );
        let telemetry_pid = telemetry.id().expect("telemetry fixture PID should exist");
        let wakeup_pid = wakeup.id().expect("wakeup fixture PID should exist");

        let telemetry_error = telemetry
            .wait_with_output(Duration::from_secs(30), &RequestedShutdown)
            .expect_err("telemetry wait should observe the interrupt");
        let wakeup_error = wakeup
            .wait_with_output(Duration::from_secs(30), &RequestedShutdown)
            .expect_err("wakeup wait should observe the interrupt");

        assert!(telemetry_error.to_string().contains("interrupted"));
        assert!(wakeup_error.to_string().contains("interrupted"));
        assert_process_exited(telemetry_pid);
        assert_process_exited(wakeup_pid);
    }

    #[test]
    fn retryable_error_matching_requires_the_exact_code_and_retryable_flag() {
        let error = br#"{"error":{"code":"rule_busy","retryable":true}}"#;

        assert!(is_retryable_error(error, "rule_busy"));
        assert!(!is_retryable_error(error, "core_unavailable"));
        assert!(!is_retryable_error(
            br#"{"error":{"code":"rule_busy","retryable":false}}"#,
            "rule_busy"
        ));
    }

    #[test]
    fn pty_guard_reaps_a_stalled_status_interface_child() {
        let pair = native_pty_system()
            .openpty(PtySize {
                rows: 24,
                cols: 80,
                pixel_width: 0,
                pixel_height: 0,
            })
            .expect("fixture PTY should open");
        let mut command = CommandBuilder::new("/bin/sleep");
        command.arg("30");
        let child = PtyChildGuard::new(
            pair.slave
                .spawn_command(command)
                .expect("fixture PTY child should start"),
        );
        let pid = child.process_id().expect("fixture PTY PID should exist");

        drop(child);
        drop(pair.slave);
        drop(pair.master);

        assert_process_exited(pid);
    }

    #[test]
    fn supervisor_stop_failure_is_reported_after_exact_process_cleanup() {
        let child = ProcessChildGuard::new(
            Command::new("/bin/sleep")
                .arg("30")
                .spawn()
                .expect("fixture Supervisor should start"),
        );
        let pid = child.id().expect("fixture Supervisor PID should exist");
        let identity = wait_for_process_identity(pid);
        let mut child = child.into_child();
        let reaper = thread::spawn(move || child.wait());
        let service = ProcessChildGuard::new(
            Command::new("/bin/sleep")
                .arg("30")
                .spawn()
                .expect("fixture Core service should start"),
        );
        let service_pid = service.id().expect("fixture Core service PID should exist");
        let root = TemporaryDirectory::new().expect("fixture state root should exist");
        let mut lifecycle = LifecycleGuard {
            release_binary: PathBuf::from("/usr/bin/false"),
            fixture_binary: PathBuf::from("/usr/bin/false"),
            state_root: root.path.join("state"),
            service_socket: root.path.join("service.sock"),
            mihomo: PathBuf::from("/usr/bin/false"),
            supervisor: Some(identity),
            service: Some(service),
        };

        let error = lifecycle
            .shutdown()
            .expect_err("the failed stop command should be reported");

        assert!(error.to_string().contains("fixture Supervisor stop failed"));
        reaper
            .join()
            .expect("fixture reaper should join")
            .expect("fixture Supervisor should be reaped");
        assert_process_exited(pid);
        assert_process_exited(service_pid);
    }

    fn wait_for_process_identity(pid: u32) -> ProcessIdentity {
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            if let Some(start_identity) = PsProcessInspector
                .identity(pid)
                .expect("fixture process inspection should succeed")
            {
                return ProcessIdentity {
                    pid,
                    start_identity,
                };
            }
            assert!(
                Instant::now() < deadline,
                "fixture process identity should appear"
            );
            thread::sleep(Duration::from_millis(10));
        }
    }

    fn assert_process_exited(pid: u32) {
        let pid = Pid::from_raw(i32::try_from(pid).expect("fixture PID should fit i32"));
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            match kill(pid, None) {
                Err(nix::errno::Errno::ESRCH) => return,
                Ok(()) => {}
                Err(error) => panic!("fixture process inspection should succeed: {error}"),
            }
            assert!(Instant::now() < deadline, "fixture process should exit");
            thread::sleep(Duration::from_millis(10));
        }
    }

    fn project_path(relative: &str) -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join(relative)
    }
}
