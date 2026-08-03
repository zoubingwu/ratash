//! Validates capture metadata and reviewed benchmark reports.

use std::error::Error;
use std::path::Path;

use serde_json::{Map, Value, json};

use super::reporting::{
    TemporaryDirectory, collector_source_sha256, command_output, exact_measurement_map,
    exact_numeric_measurements, median_measurements, non_negative_number, project_root, read_json,
    require_bool_object, require_non_empty_string, require_object, require_positive_u64_object,
    require_string, require_string_object, require_u64, require_u64_object, sha256_file,
    sha256_pretty_json, source_tree_sha256, validate_sha256_string,
};
use super::support::invalid;
use super::workload::generate_workload;
use super::{
    CAPTURE_TOOL_VERSION, CURVE_KEYS, MEASUREMENT_KEYS, RELEASE_SCALE, WORKLOAD_GENERATOR_VERSION,
    WORKLOAD_SEED,
};

pub(super) fn validate_metadata_file(
    path: &Path,
    require_approved: bool,
) -> Result<(), Box<dyn Error>> {
    let metadata = read_json(path)?;
    validate_metadata(&metadata, require_approved)?;
    println!("benchmark metadata is valid");
    Ok(())
}

pub(super) fn validate_metadata(
    metadata: &Value,
    require_approved: bool,
) -> Result<(), Box<dyn Error>> {
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
    require_string_object(generator, "name", "ratash-release-workload")?;
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
        || capture["collector_sha256"] != collector_source_sha256(&project_root)?
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
    require_string_object(tool, "name", "ratash-release-benchmark")?;
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
                .ok_or_else(|| invalid("reviewed report measurement key is missing"))?
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
