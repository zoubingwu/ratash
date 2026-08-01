//! Generates deterministic release-scale workload artifacts.

use std::error::Error;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use serde_json::json;

use hopash::domain::{NodeRecordId, ProfileId};

use super::reporting::{artifact_metadata, new_buffered_file, write_json_line, write_json_new};
use super::{WORKLOAD_GENERATOR_VERSION, WORKLOAD_SEED, WorkloadScale};

pub(super) fn generate_workload(
    root: &Path,
    scale: WorkloadScale,
) -> Result<PathBuf, Box<dyn Error>> {
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
        let page = ["overview", "proxies", "connections", "rules", "logs"]
            [usize::try_from((frame / 240) % 5)?];
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
