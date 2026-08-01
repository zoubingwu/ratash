use hopash::constants::{
    CORE_RESTART_INITIAL_BACKOFF, CORE_RESTART_LIMIT, CORE_RESTART_MAX_BACKOFF,
    CORE_SERVICE_LIVENESS_INTERVAL,
};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Command;

const PRODUCT_CONTRACT: &str = include_str!("../fixtures/release/product-contract-v1.json");
const PACKAGE_CONTRACT: &str = include_str!("../packaging/macos/package-contract-v1.json");
const BENCHMARK_METADATA: &str = include_str!("../fixtures/release/benchmark-metadata-v1.json");

#[test]
fn release_contract_pins_both_macos_installers_and_core_artifacts() {
    let product: serde_json::Value =
        serde_json::from_str(PRODUCT_CONTRACT).expect("product contract should be valid JSON");
    let package: serde_json::Value =
        serde_json::from_str(PACKAGE_CONTRACT).expect("package contract should be valid JSON");

    assert_eq!(package["schema_version"], 1);
    assert_eq!(package["package_identifier"], "io.hopash.rs");
    assert_eq!(package["service_label"], "io.hopash.core-runtime");
    assert_eq!(package["mihomo_version"], product["mihomo_version"]);
    assert_eq!(
        package["paths"]["mihomo"],
        "/Library/Application Support/Hopash RS/bin/mihomo"
    );
    assert_eq!(
        package["paths"]["service_socket"],
        "/var/run/hopash-rs/core-service.sock"
    );
    assert_eq!(package["internal_service"]["mode"], "__core-service");
    assert_eq!(
        package["targets"]["aarch64-apple-darwin"]["sha256"],
        "40cdae2fab4b18df15f40eaa9dc3af70ab3d8be7f77164ae1e5f1af3a2a4fb44"
    );
    assert_eq!(
        package["targets"]["x86_64-apple-darwin"]["sha256"],
        "03e0ce01921f1bcc75e51e6505853330e2956e4dac123564a37620e2a68f823f"
    );

    let product_targets = product["supported_targets"]
        .as_array()
        .expect("supported targets should be an array")
        .iter()
        .map(|target| target.as_str().expect("target should be a string"))
        .collect::<BTreeSet<_>>();
    let package_targets = package["targets"]
        .as_object()
        .expect("package targets should be an object")
        .keys()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    assert_eq!(package_targets, product_targets);

    for target in product_targets {
        let artifact = &package["targets"][target];
        assert!(artifact["url"].as_str().is_some_and(|url| {
            url.starts_with("https://github.com/MetaCubeX/mihomo/releases/download/v1.19.28/")
        }));
        assert!(artifact["sha256"].as_str().is_some_and(
            |digest| digest.len() == 64 && digest.bytes().all(|byte| byte.is_ascii_hexdigit())
        ));
    }
}

#[test]
fn package_staging_contains_the_complete_installation_contract() {
    let fixture = TempDirectory::new("package-stage");
    let hopash = fixture.path.join("hopash");
    let mihomo = fixture.path.join("mihomo");
    let mihomo_license = fixture.path.join("Mihomo-GPL-3.0.txt");
    fs::write(&hopash, b"fixture hopash executable")
        .expect("fixture Hopash executable should be written");
    fs::write(&mihomo, b"fixture Mihomo executable")
        .expect("fixture Mihomo executable should be written");
    fs::write(&mihomo_license, b"fixture GPL-3.0 license")
        .expect("fixture Mihomo license should be written");
    set_mode(&hopash, 0o755);
    set_mode(&mihomo, 0o755);
    let stage = fixture.path.join("stage");
    let digest = hex_digest(&fs::read(&mihomo).expect("fixture Mihomo should be readable"));

    let output = Command::new("sh")
        .arg(project_path("scripts/package-macos.sh"))
        .args(["--version", "0.1.0"])
        .args(["--target", "aarch64-apple-darwin"])
        .arg("--hopash")
        .arg(&hopash)
        .arg("--mihomo")
        .arg(&mihomo)
        .args(["--mihomo-sha256", &digest])
        .arg("--mihomo-license")
        .arg(&mihomo_license)
        .arg("--stage-only")
        .arg(&stage)
        .output()
        .expect("package staging command should run");
    assert!(
        output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    for executable in [
        "payload/usr/local/bin/hopash",
        "payload/Library/PrivilegedHelperTools/io.hopash.core-runtime",
        "payload/Library/Application Support/Hopash RS/bin/mihomo",
        "payload/usr/local/share/hopash/uninstall.sh",
    ] {
        let path = stage.join(executable);
        assert!(path.is_file(), "missing {}", path.display());
        assert_eq!(mode(&path), 0o755, "unexpected mode for {}", path.display());
    }

    for asset in [
        "payload/usr/local/share/man/man1/hopash.1",
        "payload/usr/local/share/man/man1/hopash-profile-add.1",
        "payload/usr/local/share/bash-completion/completions/hopash",
        "payload/usr/local/share/zsh/site-functions/_hopash",
        "payload/usr/local/share/fish/vendor_completions.d/hopash.fish",
        "payload/usr/local/share/hopash/skills/hopash/SKILL.md",
        "payload/usr/local/share/hopash/release/product-contract-v1.json",
        "payload/usr/local/share/hopash/release/benchmark-metadata-v1.json",
        "payload/usr/local/share/hopash/release/package-contract-v1.json",
        "payload/usr/local/share/hopash/licenses/Mihomo-GPL-3.0.txt",
        "payload/usr/local/share/hopash/licenses/Mihomo-NOTICE.txt",
        "payload/Library/LaunchDaemons/io.hopash.core-runtime.plist",
        "scripts/postinstall",
    ] {
        let path = stage.join(asset);
        assert!(path.is_file(), "missing {}", path.display());
    }

    let plist = fs::read_to_string(
        stage.join("payload/Library/LaunchDaemons/io.hopash.core-runtime.plist"),
    )
    .expect("staged LaunchDaemon should be readable");
    for required in [
        "__core-service",
        "--owner-uid",
        "@OWNER_UID@",
        "--socket",
        "/var/run/hopash-rs/core-service.sock",
        "--runtime-root",
        "/Library/Application Support/Hopash RS/runtime",
        "--mihomo",
        "/Library/Application Support/Hopash RS/bin/mihomo",
        "<key>ExitTimeOut</key>",
        "<integer>50</integer>",
    ] {
        assert!(plist.contains(required), "plist is missing {required}");
    }
}

#[test]
fn postinstall_waits_for_a_booted_out_service_before_bootstrap() {
    let script = fs::read_to_string(project_path("packaging/macos/scripts/postinstall"))
        .expect("postinstall should be readable");
    let bootout = script
        .find("/bin/launchctl bootout")
        .expect("postinstall should boot out the previous service");
    let wait = script
        .find("while /bin/launchctl print")
        .expect("postinstall should wait for launchd to remove the previous service");
    let bootstrap = script
        .find("/bin/launchctl bootstrap")
        .expect("postinstall should bootstrap the replacement service");

    assert!(bootout < wait && wait < bootstrap);
    assert!(script.contains("SERVICE_REMOVAL_ATTEMPTS=600"));
    assert!(script.contains("/bin/sleep 0.1"));
    assert!(script.contains("timed out waiting for the previous Core service to stop"));
}

#[cfg(target_os = "macos")]
#[test]
fn package_builder_emits_a_verifiable_macos_installer_without_installing_it() {
    let fixture = TempDirectory::new("package-build");
    let hopash = fixture.path.join("hopash");
    let mihomo = fixture.path.join("mihomo");
    let mihomo_license = fixture.path.join("Mihomo-GPL-3.0.txt");
    fs::write(&hopash, b"#!/bin/sh\nexit 0\n").expect("fixture Hopash should be written");
    fs::write(&mihomo, b"#!/bin/sh\nexit 0\n").expect("fixture Mihomo should be written");
    fs::write(&mihomo_license, b"fixture GPL-3.0 license")
        .expect("fixture Mihomo license should be written");
    set_mode(&hopash, 0o755);
    set_mode(&mihomo, 0o755);
    let digest = hex_digest(&fs::read(&mihomo).expect("fixture Mihomo should be readable"));
    let output_directory = fixture.path.join("dist");

    let output = Command::new("sh")
        .arg(project_path("scripts/package-macos.sh"))
        .args(["--version", "0.1.0"])
        .args(["--target", "aarch64-apple-darwin"])
        .arg("--hopash")
        .arg(&hopash)
        .arg("--mihomo")
        .arg(&mihomo)
        .args(["--mihomo-sha256", &digest])
        .arg("--mihomo-license")
        .arg(&mihomo_license)
        .arg("--output")
        .arg(&output_directory)
        .output()
        .expect("package build command should run");
    assert!(
        output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let package = output_directory.join("hopash-0.1.0-aarch64-apple-darwin.pkg");
    let checksum = output_directory.join("hopash-0.1.0-aarch64-apple-darwin.pkg.sha256");
    assert!(package.is_file());
    assert!(checksum.is_file());
    let checksum_result = Command::new("/usr/bin/shasum")
        .current_dir(&output_directory)
        .args(["-a", "256", "-c"])
        .arg(checksum.file_name().expect("checksum should have a name"))
        .output()
        .expect("checksum verification should run");
    assert!(checksum_result.status.success());

    let payload = Command::new("/usr/sbin/pkgutil")
        .arg("--payload-files")
        .arg(package)
        .output()
        .expect("package payload inspection should run");
    assert!(payload.status.success());
    let payload = String::from_utf8_lossy(&payload.stdout);
    for required in [
        "usr/local/bin/hopash",
        "Library/PrivilegedHelperTools/io.hopash.core-runtime",
        "Library/Application Support/Hopash RS/bin/mihomo",
        "usr/local/share/man/man1/hopash-profile-add.1",
    ] {
        assert!(
            payload.contains(required),
            "package payload is missing {required}"
        );
    }
}

#[test]
fn generated_shell_and_manual_assets_cover_only_the_public_command_surface() {
    let assets = [
        project_path("packaging/generated/completions/hopash.bash"),
        project_path("packaging/generated/completions/_hopash"),
        project_path("packaging/generated/completions/hopash.fish"),
        project_path("packaging/generated/man/man1/hopash.1"),
    ];
    for path in assets {
        let content = fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("{} should be readable: {error}", path.display()));
        for command in [
            "start", "stop", "restart", "status", "profile", "proxy", "latency", "logs", "rule",
        ] {
            assert!(
                content.contains(command),
                "{} is missing {command}",
                path.display()
            );
        }
        assert!(
            !content.contains("__supervisor"),
            "{} exposes the Supervisor mode",
            path.display()
        );
        assert!(
            !content.contains("__core-service"),
            "{} exposes the Core service mode",
            path.display()
        );
        assert!(
            content.contains("agent"),
            "{} is missing Agent Help",
            path.display()
        );
    }
}

#[test]
fn release_metadata_names_every_required_resource_measurement() {
    let metadata: serde_json::Value =
        serde_json::from_str(BENCHMARK_METADATA).expect("benchmark metadata should be valid JSON");

    assert_eq!(metadata["schema_version"], 1);
    assert_eq!(
        metadata["measurement_environment"]["runner"],
        "dedicated macOS 15 arm64 release runner"
    );
    assert_eq!(
        metadata["measurement_environment"]["runner_profile"]["architecture"],
        "aarch64"
    );
    assert_eq!(metadata["workloads"]["profiles"], 100);
    assert_eq!(metadata["workloads"]["active_nodes"], 10_000);
    assert_eq!(metadata["workloads"]["local_rules"], 20_000);
    assert_eq!(metadata["workloads"]["application_seam_scale"], true);
    assert_eq!(metadata["workloads"]["tui_duration_seconds"], 1_800);
    assert_eq!(
        metadata["measurement_environment"]["idle_observation_seconds"],
        60
    );
    assert_eq!(
        metadata["measurement_environment"]["telemetry_observation_seconds"],
        1_800
    );
    assert_eq!(
        metadata["measurement_environment"]["tui_observation_seconds"],
        1_800
    );
    assert_eq!(
        metadata["measurement_environment"]["rss_sample_interval_seconds"],
        1
    );
    assert_eq!(
        metadata["measurement_environment"]["rss_curve_names"],
        serde_json::json!([
            "supervisor_rss_bytes",
            "privileged_service_rss_bytes",
            "combined_background_rss_bytes",
            "telemetry_rss_bytes",
            "tui_rss_bytes"
        ])
    );
    assert_eq!(
        metadata["workload_generator"]["name"],
        "hopash-release-workload"
    );
    assert_eq!(metadata["workload_generator"]["version"], 2);
    assert_eq!(
        metadata["workload_generator"]["seed"],
        5_210_471_535_391_298_131_u64
    );
    assert_eq!(
        metadata["versioned_defaults"]["core_restart_limit"],
        CORE_RESTART_LIMIT
    );
    for (name, value) in [
        (
            "core_restart_initial_backoff_ms",
            CORE_RESTART_INITIAL_BACKOFF,
        ),
        ("core_restart_max_backoff_ms", CORE_RESTART_MAX_BACKOFF),
        (
            "core_service_liveness_interval_ms",
            CORE_SERVICE_LIVENESS_INTERVAL,
        ),
    ] {
        assert_eq!(
            metadata["versioned_defaults"][name],
            u64::try_from(value.as_millis()).expect("the default duration should fit in u64"),
            "{name} drifted"
        );
    }
    let status = metadata["status"]
        .as_str()
        .expect("benchmark status should be a string");
    assert!(matches!(status, "capture_required" | "approved"));
    let measurement_names = [
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
    for field in [
        "measurements",
        "baseline_measurements",
        "thresholds",
        "regression_budgets_percent",
    ] {
        let values = metadata[field]
            .as_object()
            .unwrap_or_else(|| panic!("{field} should be an object"));
        assert_eq!(values.len(), measurement_names.len());
        for measurement in measurement_names {
            assert!(
                values.get(measurement).is_some(),
                "{field} is missing {measurement}"
            );
        }
        match status {
            "capture_required" => {
                assert!(values.values().all(serde_json::Value::is_null));
            }
            "approved" => {
                assert!(values.values().all(|value| {
                    value
                        .as_f64()
                        .is_some_and(|measurement| measurement.is_finite() && measurement >= 0.0)
                }));
            }
            _ => unreachable!("the benchmark status was validated above"),
        }
    }
    match status {
        "capture_required" => assert!(metadata["approved_capture"].is_null()),
        "approved" => assert!(metadata["approved_capture"].is_object()),
        _ => unreachable!("the benchmark status was validated above"),
    }
}

#[test]
fn package_scripts_are_valid_posix_shell() {
    for script in [
        "scripts/package-macos.sh",
        "scripts/package-local-macos.sh",
        "scripts/capture-release-benchmarks-macos.sh",
        "scripts/macos-release-resource-probe.sh",
        "packaging/macos/scripts/postinstall",
        "packaging/macos/uninstall.sh",
    ] {
        let output = Command::new("sh")
            .args(["-n"])
            .arg(project_path(script))
            .output()
            .expect("shell syntax check should run");
        assert!(
            output.status.success(),
            "{script}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

#[test]
fn benchmark_capture_scripts_enforce_provenance_and_complete_wakeup_samples() {
    let capture = fs::read_to_string(project_path("scripts/capture-release-benchmarks-macos.sh"))
        .expect("benchmark capture script should be readable");
    assert!(capture.contains("git status --porcelain --untracked-files=all"));
    assert!(capture.contains("clean Git working tree and index"));
    assert!(capture.contains("kill -TERM \"$active_collector\""));
    assert!(capture.contains("wait \"$active_collector\""));
    assert!(capture.contains("trap 'interrupt_and_exit 130' INT"));

    let probe = fs::read_to_string(project_path("scripts/macos-release-resource-probe.sh"))
        .expect("resource probe should be readable");
    assert!(probe.contains("if (!(pid in seen)) missing = 1"));
    assert!(probe.contains("did not report every requested process"));

    let ci = fs::read_to_string(project_path(".github/workflows/ci.yml"))
        .expect("CI workflow should be readable");
    assert!(ci.contains("Rust 1.88.0 macOS compatibility"));
    assert!(ci.contains("runs-on: macos-15"));
}

#[test]
fn package_builder_rejects_partial_signing_configuration_before_staging() {
    let fixture = TempDirectory::new("partial-signing");
    let hopash = fixture.path.join("hopash");
    let mihomo = fixture.path.join("mihomo");
    let mihomo_license = fixture.path.join("Mihomo-GPL-3.0.txt");
    fs::write(&hopash, b"fixture hopash").expect("fixture Hopash should be written");
    fs::write(&mihomo, b"fixture mihomo").expect("fixture Mihomo should be written");
    fs::write(&mihomo_license, b"fixture license")
        .expect("fixture Mihomo license should be written");
    let digest = hex_digest(&fs::read(&mihomo).expect("fixture Mihomo should be readable"));

    let output = Command::new("sh")
        .arg(project_path("scripts/package-macos.sh"))
        .args(["--version", "0.1.0"])
        .args(["--target", "aarch64-apple-darwin"])
        .arg("--hopash")
        .arg(&hopash)
        .arg("--mihomo")
        .arg(&mihomo)
        .args(["--mihomo-sha256", &digest])
        .arg("--mihomo-license")
        .arg(&mihomo_license)
        .args(["--application-identity", "fixture identity"])
        .arg("--stage-only")
        .arg(fixture.path.join("stage"))
        .output()
        .expect("package validation command should run");

    assert_eq!(output.status.code(), Some(1));
    assert!(
        String::from_utf8_lossy(&output.stderr)
            .contains("Application and installer signing identities must be provided together.")
    );
}

#[test]
fn readme_stays_user_facing_and_documents_the_installed_workflow() {
    let readme = include_str!("../README.md");
    for required in [
        "## Installation",
        "## Project Status",
        "HOPASH_OWNER_UID",
        "shasum -a 256",
        "## Shell Completion",
        "## Uninstall",
        "hopash start",
        "hopash status",
        "hopash help agent",
        "package-local-macos.sh",
        "local-unsigned",
    ] {
        assert!(readme.contains(required), "README is missing {required}");
    }
    for internal in [
        "Architecture",
        "Runtime Generation",
        "Config Transaction Coordinator",
        "SPEC.md",
        "USER_STORIES.md",
        "src/",
    ] {
        assert!(
            !readme.contains(internal),
            "README exposes internal detail {internal}"
        );
    }
}

fn project_path(relative: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join(relative)
}

fn set_mode(path: &Path, value: u32) {
    let mut permissions = fs::metadata(path)
        .expect("fixture metadata should be available")
        .permissions();
    permissions.set_mode(value);
    fs::set_permissions(path, permissions).expect("fixture mode should be set");
}

fn mode(path: &Path) -> u32 {
    fs::metadata(path)
        .expect("staged metadata should be available")
        .permissions()
        .mode()
        & 0o777
}

fn hex_digest(content: &[u8]) -> String {
    Sha256::digest(content)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

struct TempDirectory {
    path: PathBuf,
}

impl TempDirectory {
    fn new(label: &str) -> Self {
        let path = std::env::temp_dir().join(format!("hopash-{label}-{}", uuid::Uuid::new_v4()));
        fs::create_dir(&path).expect("temporary directory should be created");
        Self { path }
    }
}

impl Drop for TempDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}
