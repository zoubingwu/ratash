use std::env;
use std::error::Error;
use std::path::Path;
use std::time::Duration;

use ratash::constants::PROBE_WORKER_COUNT;
use ratash::tui_runtime::ShutdownSignal;

#[path = "release_benchmark/collection.rs"]
mod collection;
#[path = "release_benchmark/fixture.rs"]
mod fixture;
#[path = "release_benchmark/metadata.rs"]
mod metadata;
#[path = "release_benchmark/process_metrics.rs"]
mod process_metrics;
#[path = "release_benchmark/process_support.rs"]
mod process_support;
#[path = "release_benchmark/profile_server.rs"]
mod profile_server;
#[path = "release_benchmark/reporting.rs"]
mod reporting;
#[path = "release_benchmark/runtime_support.rs"]
mod runtime_support;
#[path = "release_benchmark/support.rs"]
mod support;
#[cfg(test)]
#[path = "release_benchmark/tests.rs"]
mod tests;
#[path = "release_benchmark/workload.rs"]
mod workload;

use collection::{capture_results, collect_sample, run_smoke};
use fixture::{argument_value, run_fixture_core, run_fixture_core_service};
use metadata::validate_metadata_file;
use workload::generate_workload;

const CAPTURE_TOOL_VERSION: u64 = 1;
const WORKLOAD_GENERATOR_VERSION: u64 = 2;
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
const COLLECTOR_SOURCE_FILES: [&str; 12] = [
    "examples/release-benchmark.rs",
    "examples/release_benchmark/collection.rs",
    "examples/release_benchmark/fixture.rs",
    "examples/release_benchmark/metadata.rs",
    "examples/release_benchmark/process_metrics.rs",
    "examples/release_benchmark/process_support.rs",
    "examples/release_benchmark/profile_server.rs",
    "examples/release_benchmark/reporting.rs",
    "examples/release_benchmark/runtime_support.rs",
    "examples/release_benchmark/support.rs",
    "examples/release_benchmark/tests.rs",
    "examples/release_benchmark/workload.rs",
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
