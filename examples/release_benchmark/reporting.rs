//! Captures environment provenance and validates benchmark artifacts.

use std::collections::BTreeMap;
use std::env;
use std::error::Error;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};

use ratash::tui_runtime::ShutdownSignal;

use super::process_support::ProcessChildGuard;
use super::support::invalid;
use super::{
    CAPTURE_TOOL_VERSION, COLLECTOR_SOURCE_FILES, CURVE_KEYS, MEASUREMENT_KEYS,
    ObservationDurations, PROBE_COMPLETION_TIMEOUT, RELEASE_SCALE, WORKLOAD_GENERATOR_VERSION,
    WORKLOAD_SEED, WorkloadScale,
};

pub(super) fn current_rss(resource_probe: &Path) -> Result<f64, Box<dyn Error>> {
    resource_metric(resource_probe, "rss", &[std::process::id().to_string()])
}

pub(super) fn resource_metric(
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

pub(super) fn child_metric(
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

pub(super) fn insert_measurement(measurements: &mut Map<String, Value>, key: &str, value: f64) {
    measurements.insert(key.to_owned(), Value::from(value));
}

pub(super) fn validate_release_manifest(
    manifest: &Value,
    metadata: &Value,
) -> Result<(), Box<dyn Error>> {
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

pub(super) fn validate_manifest_artifacts(
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

pub(super) fn validate_manifest_scale(
    manifest: &Value,
    expected: WorkloadScale,
) -> Result<(), Box<dyn Error>> {
    require_u64(manifest, "schema_version", 1)?;
    let generator = require_object(manifest, "generator")?;
    require_string_object(generator, "name", "ratash-release-workload")?;
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

pub(super) fn capture_environment(metadata: &Value) -> Result<Value, Box<dyn Error>> {
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
        "collector_sha256": collector_source_sha256(&project_root)?,
        "captured_at_unix_seconds": unix_seconds()?
    }))
}

pub(super) fn validate_sample_environment(
    sample: &Value,
    expected: &Value,
) -> Result<(), Box<dyn Error>> {
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

pub(super) fn validate_sample_collector(
    sample: &Value,
    expected_smoke: bool,
) -> Result<(), Box<dyn Error>> {
    let collector = require_object(sample, "collector")?;
    require_string_object(collector, "name", "ratash-release-benchmark")?;
    require_u64_object(collector, "version", CAPTURE_TOOL_VERSION)?;
    require_bool_object(collector, "smoke", expected_smoke)
}

pub(super) fn validate_sample_inputs(inputs: &Value) -> Result<(), Box<dyn Error>> {
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

pub(super) fn project_root() -> Result<PathBuf, Box<dyn Error>> {
    Ok(PathBuf::from(command_output(
        "git",
        &["rev-parse", "--show-toplevel"],
    )?))
}

pub(super) fn source_tree_sha256() -> Result<String, Box<dyn Error>> {
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

pub(super) fn command_output(program: &str, arguments: &[&str]) -> Result<String, Box<dyn Error>> {
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

pub(super) fn artifact_metadata(path: &Path, records: u64) -> Result<Value, Box<dyn Error>> {
    Ok(json!({
        "bytes": fs::metadata(path)?.len(),
        "records": records,
        "sha256": sha256_file(path)?
    }))
}

pub(super) fn sha256_file(path: &Path) -> Result<String, Box<dyn Error>> {
    let content = fs::read(path)?;
    Ok(Sha256::digest(content)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}

pub(super) fn collector_source_sha256(root: &Path) -> Result<String, Box<dyn Error>> {
    let mut hasher = Sha256::new();
    for relative in COLLECTOR_SOURCE_FILES {
        let contents = fs::read(root.join(relative))?;
        hasher.update(u64::try_from(relative.len())?.to_le_bytes());
        hasher.update(relative.as_bytes());
        hasher.update(u64::try_from(contents.len())?.to_le_bytes());
        hasher.update(contents);
    }
    Ok(hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}

pub(super) fn sha256_pretty_json(value: &Value) -> Result<String, Box<dyn Error>> {
    let mut content = serde_json::to_vec_pretty(value)?;
    content.push(b'\n');
    Ok(Sha256::digest(content)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}

pub(super) fn validate_sha256_string(value: &Value, label: &str) -> Result<(), Box<dyn Error>> {
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

pub(super) fn new_buffered_file(path: &Path) -> Result<BufWriter<File>, io::Error> {
    Ok(BufWriter::new(
        OpenOptions::new().write(true).create_new(true).open(path)?,
    ))
}

pub(super) fn write_json_line(
    output: &mut impl Write,
    value: &Value,
) -> Result<(), Box<dyn Error>> {
    serde_json::to_writer(&mut *output, value)?;
    output.write_all(b"\n")?;
    Ok(())
}

pub(super) fn read_json(path: &Path) -> Result<Value, Box<dyn Error>> {
    Ok(serde_json::from_slice(&fs::read(path)?)?)
}

pub(super) fn write_json_new(path: &Path, value: &Value) -> Result<(), Box<dyn Error>> {
    let mut output = new_buffered_file(path)?;
    serde_json::to_writer_pretty(&mut output, value)?;
    output.write_all(b"\n")?;
    output.flush()?;
    Ok(())
}

pub(super) fn exact_measurement_map<'a>(
    parent: &'a Value,
    field: &str,
) -> Result<&'a Map<String, Value>, Box<dyn Error>> {
    let values = require_object(parent, field)?;
    exact_measurement_keys(values, field)?;
    Ok(values)
}

pub(super) fn exact_numeric_measurements(
    values: &Map<String, Value>,
) -> Result<(), Box<dyn Error>> {
    exact_measurement_keys(values, "measurements")?;
    for key in MEASUREMENT_KEYS {
        non_negative_number(&values[key], key)?;
    }
    Ok(())
}

pub(super) fn median_measurements(
    values: BTreeMap<String, Vec<f64>>,
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

pub(super) fn validate_curves(
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

pub(super) fn validate_observation_durations(
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

pub(super) fn non_negative_number(value: &Value, label: &str) -> Result<f64, Box<dyn Error>> {
    value
        .as_f64()
        .filter(|value| value.is_finite() && *value >= 0.0)
        .ok_or_else(|| invalid(format!("{label} must be a finite non-negative number")))
}

pub(super) fn require_object<'a>(
    parent: &'a Value,
    field: &str,
) -> Result<&'a Map<String, Value>, Box<dyn Error>> {
    parent
        .get(field)
        .and_then(Value::as_object)
        .ok_or_else(|| invalid(format!("{field} must be an object")))
}

pub(super) fn require_string<'a>(
    parent: &'a Value,
    field: &str,
) -> Result<&'a str, Box<dyn Error>> {
    parent
        .get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| invalid(format!("{field} must be a string")))
}

pub(super) fn require_non_empty_string(
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

pub(super) fn require_u64(
    parent: &Value,
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

pub(super) fn require_u64_object(
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

pub(super) fn require_positive_u64_object(
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

pub(super) fn require_string_object(
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

pub(super) fn require_bool_object(
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

pub(super) struct TemporaryDirectory {
    pub(super) path: PathBuf,
}

impl TemporaryDirectory {
    pub(super) fn new() -> Result<Self, io::Error> {
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
