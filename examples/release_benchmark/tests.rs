//! Verifies deterministic workloads, report gates, and bounded fixture cleanup.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::thread;
use std::time::{Duration, Instant};

use nix::sys::signal::kill;
use nix::unistd::Pid;
use portable_pty::{CommandBuilder, PtySize, native_pty_system};
use serde_json::{Map, Value, json};

use hopash::domain::{NodeRecordId, ProfileId};
use hopash::lifecycle::{ProcessIdentity, ProcessInspector, PsProcessInspector};
use hopash::tui_runtime::ShutdownSignal;

use super::metadata::validate_metadata;
use super::process_metrics::{ProcessChildGuard, PtyChildGuard};
use super::profile_server::read_ndjson;
use super::reporting::{
    TemporaryDirectory, capture_results_with_environment, collector_source_sha256, command_output,
    exact_numeric_measurements, project_root, read_json, sha256_file, sha256_pretty_json,
    source_tree_sha256, validate_curves, validate_manifest_scale, validate_sample_collector,
    write_json_new,
};
use super::runtime_support::{LifecycleGuard, curve_point, is_retryable_error};
use super::workload::generate_workload;
use super::{
    CAPTURE_TOOL_VERSION, COLLECTOR_SOURCE_FILES, CURVE_KEYS, MEASUREMENT_KEYS,
    ObservationDurations, RELEASE_SCALE, WORKLOAD_GENERATOR_VERSION, WORKLOAD_SEED,
};

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
    let approval_workload = TemporaryDirectory::new().expect("approval workload root should exist");
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
        "collector_sha256": collector_source_sha256(&root).expect("collector digest"),
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
fn collector_source_list_covers_every_benchmark_module() {
    let root = project_root().expect("project root should resolve");
    let module_root = root.join("examples/release_benchmark");
    let mut actual = fs::read_dir(module_root)
        .expect("collector module directory should exist")
        .map(|entry| {
            let entry = entry.expect("collector module entry should be readable");
            format!(
                "examples/release_benchmark/{}",
                entry.file_name().to_string_lossy()
            )
        })
        .filter(|path| path.ends_with(".rs"))
        .collect::<Vec<_>>();
    actual.push("examples/release-benchmark.rs".to_owned());
    actual.sort();
    let mut expected = COLLECTOR_SOURCE_FILES.map(str::to_owned).to_vec();
    expected.sort();

    assert_eq!(actual, expected);
}

#[test]
fn collector_digest_changes_when_a_module_changes() {
    let root = TemporaryDirectory::new().expect("collector fixture root should exist");
    for relative in COLLECTOR_SOURCE_FILES {
        let path = root.path.join(relative);
        fs::create_dir_all(
            path.parent()
                .expect("collector source should have a parent"),
        )
        .expect("collector fixture directory should exist");
        fs::write(&path, relative).expect("collector fixture source should be written");
    }
    let before = collector_source_sha256(&root.path).expect("collector digest should compute");
    fs::write(
        root.path.join("examples/release_benchmark/metadata.rs"),
        "changed",
    )
    .expect("collector fixture module should change");
    let after = collector_source_sha256(&root.path).expect("collector digest should recompute");

    assert_ne!(before, after);
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
                .map(|(index, key)| ((*key).to_owned(), Value::from(sample as f64 + index as f64)))
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
