//! Collects one sample and orchestrates report capture and smoke validation.

use std::collections::BTreeMap;
use std::env;
use std::error::Error;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use serde_json::{Map, Value, json};

use ratash::application::PolicyTargetValidation;
use ratash::constants::{
    CORE_LOG_LINE_MAX_BYTES, LOG_CAPACITY, PROBE_WORKER_COUNT, TRAFFIC_SERIES_CAPACITY,
};
use ratash::domain::{
    CoreInstanceGeneration, LocalRuleSetRevision, NodeRecordId, ProbeGeneration, ProfileId,
    SampleState, TrafficSample,
};
use ratash::lifecycle::{InstanceRecord, StatePaths};
use ratash::rule::{LocalRuleSet, RuleSetLimits};
use ratash::scheduler::{ProbeCompletion, ProbeOutcome, ProbeScheduler};
use ratash::telemetry::{LogSource, TelemetryStore};
use ratash::tui::{AppState, ProfileRow, RuleRow, ViewLogRecord, render_buffer};
use ratash::tui_runtime::ProcessSignalSource;

use super::metadata::{validate_metadata, validate_metadata_file};
use super::process_metrics::{collect_background_process_metrics, collect_tui_process_metrics};
use super::process_support::ProcessChildGuard;
use super::profile_server::{ProfileServer, read_ndjson};
use super::reporting::{
    TemporaryDirectory, capture_environment, child_metric, current_rss, exact_numeric_measurements,
    insert_measurement, median_measurements, non_negative_number, read_json, require_u64,
    sha256_file, validate_curves, validate_manifest_artifacts, validate_manifest_scale,
    validate_observation_durations, validate_release_manifest, validate_sample_collector,
    validate_sample_environment, validate_sample_inputs, write_json_new,
};
use super::runtime_support::{
    LifecycleGuard, command_json, curve_point, parse_log_level, parse_page, spawn_fixture_service,
    validate_release_application_scale, wait_for_active_status,
};
use super::support::{elapsed_ms, ensure_collection_running, invalid};
use super::workload::generate_workload;
use super::{
    CAPTURE_TOOL_VERSION, CollectionControl, MEASUREMENT_KEYS, ObservationDurations, RELEASE_SCALE,
    SMOKE_OBSERVATIONS, SMOKE_SCALE, WorkloadScale,
};

pub(super) fn collect_sample(
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
                "name": "ratash-release-benchmark",
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
        service_socket,
        mihomo,
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
    let listed_rules = rules.list()?;
    let matching_rules = listed_rules
        .entries
        .iter()
        .filter(|entry| entry.rule.as_str().contains(&rule_needle))
        .count();
    let rule_rows = listed_rules
        .entries
        .into_iter()
        .map(|entry| RuleRow {
            index: entry.index,
            rule_string: entry.rule.as_str().to_owned(),
            rule_type: entry.parsed.rule_type.as_str().to_owned(),
            payload: entry.parsed.payload.map(str::to_owned),
            policy_target: entry.parsed.policy_target.to_owned(),
            policy_target_validation: PolicyTargetValidation::Valid,
        })
        .collect();
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
    state.rules.initialized = true;
    state.rules.revision = Some(LocalRuleSetRevision(1));
    state.rules.rows = rule_rows;
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

pub(super) fn capture_results(
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

pub(super) fn capture_results_with_environment(
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
        .collect::<BTreeMap<_, _>>();
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
                .ok_or_else(|| invalid("report measurement key is missing"))?
                .push(non_negative_number(&measurements[key], key)?);
        }
    }
    let measurements = median_measurements(values)?;
    let report = json!({
        "schema_version": 1,
        "status": "review_required",
        "capture_tool": {
            "name": "ratash-release-benchmark",
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

pub(super) fn run_smoke(
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
