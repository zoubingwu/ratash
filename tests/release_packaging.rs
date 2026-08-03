use ratash::constants::{
    CORE_RESTART_INITIAL_BACKOFF, CORE_RESTART_LIMIT, CORE_RESTART_MAX_BACKOFF,
    CORE_SERVICE_LIVENESS_INTERVAL,
};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::fs;
use std::os::unix::fs::{PermissionsExt, symlink};
use std::path::{Path, PathBuf};
use std::process::Command;

const PRODUCT_CONTRACT: &str = include_str!("../fixtures/release/product-contract-v1.json");
const PACKAGE_CONTRACT: &str = include_str!("../packaging/macos/package-contract-v1.json");
const BENCHMARK_METADATA: &str = include_str!("../fixtures/release/benchmark-metadata-v1.json");

#[test]
fn geodata_manifest_pins_immutable_core_assets_and_source() {
    let manifest: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(project_path(
            "fixtures/mihomo/v1.19.28/geodata-manifest.json",
        ))
        .expect("Geo data manifest should be readable"),
    )
    .expect("Geo data manifest should be valid JSON");

    assert_eq!(manifest["schema_version"], 1);
    assert_eq!(manifest["core_version"], "v1.19.28");
    assert_eq!(
        manifest["repository"],
        "https://github.com/MetaCubeX/meta-rules-dat"
    );
    assert_eq!(
        manifest["asset_commit"],
        "1567448176a3b6b56661a93b96d1e1c4c10bf2f9"
    );
    assert_eq!(
        manifest["source_commit"],
        "4178770badecb1b349fbcd62c737e0d7a2079729"
    );
    assert_eq!(manifest["repository_license"], "GPL-3.0-only");
    assert_eq!(
        manifest["assets"],
        serde_json::json!([
            {
                "file_name": "ASN.mmdb",
                "source_name": "GeoLite2-ASN.mmdb",
                "url": "https://raw.githubusercontent.com/MetaCubeX/meta-rules-dat/1567448176a3b6b56661a93b96d1e1c4c10bf2f9/GeoLite2-ASN.mmdb",
                "size": 12_059_794,
                "sha256": "0f7e30ae5c234389ff3d0221ab311b4a6688817dd62d43d2f8cccd5935118124"
            },
            {
                "file_name": "Country.mmdb",
                "source_name": "country.mmdb",
                "url": "https://raw.githubusercontent.com/MetaCubeX/meta-rules-dat/1567448176a3b6b56661a93b96d1e1c4c10bf2f9/country.mmdb",
                "size": 7_903_639,
                "sha256": "80a846466123b76373bb2e9da1d27708a0c87e795d4650f60f82c7d179c8c9d0"
            },
            {
                "file_name": "GeoIP.dat",
                "source_name": "geoip.dat",
                "url": "https://raw.githubusercontent.com/MetaCubeX/meta-rules-dat/1567448176a3b6b56661a93b96d1e1c4c10bf2f9/geoip.dat",
                "size": 17_483_060,
                "sha256": "6ba63d75f307d16a81ae09406ddcf2779fa75cb642d4aae59613370d62d33509"
            },
            {
                "file_name": "GeoSite.dat",
                "source_name": "geosite.dat",
                "url": "https://raw.githubusercontent.com/MetaCubeX/meta-rules-dat/1567448176a3b6b56661a93b96d1e1c4c10bf2f9/geosite.dat",
                "size": 4_238_038,
                "sha256": "ebe025201883b095a62c4b2d72e1477ec4f14a5afba132f1effe7651cb4921cf"
            }
        ])
    );

    let notice = fs::read_to_string(project_path("packaging/macos/GeoData-NOTICE.txt"))
        .expect("Geo data notice should be readable");
    for required in [
        "GPL-3.0-only",
        "1567448176a3b6b56661a93b96d1e1c4c10bf2f9",
        "4178770badecb1b349fbcd62c737e0d7a2079729",
        "MetaCubeX-meta-rules-dat-GPL-3.0.txt",
        "This product includes GeoLite Data created by MaxMind",
        "https://www.maxmind.com/en/geolite/eula",
    ] {
        assert!(
            notice.contains(required),
            "Geo data notice is missing {required}"
        );
    }
}

#[test]
fn release_contract_pins_both_macos_installers_and_core_artifacts() {
    let product: serde_json::Value =
        serde_json::from_str(PRODUCT_CONTRACT).expect("product contract should be valid JSON");
    let package: serde_json::Value =
        serde_json::from_str(PACKAGE_CONTRACT).expect("package contract should be valid JSON");

    assert_eq!(package["schema_version"], 1);
    assert_eq!(package["package_identifier"], "io.ratash");
    assert_eq!(package["service_label"], "io.ratash.core-runtime");
    assert_eq!(package["mihomo_version"], product["mihomo_version"]);
    assert_eq!(
        package["paths"]["mihomo"],
        "/Library/Application Support/ratash/bin/mihomo"
    );
    assert_eq!(
        package["paths"]["service_socket"],
        "/var/run/ratash/core-service.sock"
    );
    assert_eq!(
        package["paths"]["geodata"],
        "/Library/Application Support/ratash/share/geodata"
    );
    assert_eq!(
        package["geodata"]["manifest"],
        "/usr/local/share/ratash/release/geodata-manifest.json"
    );
    assert_eq!(
        package["geodata"]["files"],
        serde_json::json!(["ASN.mmdb", "Country.mmdb", "GeoIP.dat", "GeoSite.dat"])
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
    let ratash = fixture.path.join("ratash");
    let mihomo = fixture.path.join("mihomo");
    let mihomo_license = fixture.path.join("Mihomo-GPL-3.0.txt");
    let geodata = write_geodata_fixture(&fixture.path);
    fs::write(&ratash, b"fixture ratash executable")
        .expect("fixture Ratash executable should be written");
    fs::write(&mihomo, b"fixture Mihomo executable")
        .expect("fixture Mihomo executable should be written");
    fs::write(&mihomo_license, b"fixture GPL-3.0 license")
        .expect("fixture Mihomo license should be written");
    set_mode(&ratash, 0o755);
    set_mode(&mihomo, 0o755);
    let stage = fixture.path.join("stage");
    let digest = hex_digest(&fs::read(&mihomo).expect("fixture Mihomo should be readable"));

    let mut command = Command::new("sh");
    command
        .arg(project_path("scripts/package-macos.sh"))
        .args(["--version", "0.1.0"])
        .args(["--target", "aarch64-apple-darwin"])
        .arg("--ratash")
        .arg(&ratash)
        .arg("--mihomo")
        .arg(&mihomo)
        .args(["--mihomo-sha256", &digest])
        .arg("--mihomo-license")
        .arg(&mihomo_license);
    add_geodata_arguments(&mut command, &geodata);
    let output = command
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
        "payload/usr/local/bin/ratash",
        "payload/Library/PrivilegedHelperTools/io.ratash.core-runtime",
        "payload/Library/Application Support/ratash/bin/mihomo",
        "payload/usr/local/share/ratash/uninstall.sh",
    ] {
        let path = stage.join(executable);
        assert!(path.is_file(), "missing {}", path.display());
        assert_eq!(mode(&path), 0o755, "unexpected mode for {}", path.display());
    }

    for asset in [
        "payload/usr/local/share/man/man1/ratash.1",
        "payload/usr/local/share/man/man1/ratash-profile-add.1",
        "payload/usr/local/share/bash-completion/completions/ratash",
        "payload/usr/local/share/zsh/site-functions/_ratash",
        "payload/usr/local/share/fish/vendor_completions.d/ratash.fish",
        "payload/usr/local/share/ratash/skills/ratash/SKILL.md",
        "payload/usr/local/share/ratash/release/product-contract-v1.json",
        "payload/usr/local/share/ratash/release/benchmark-metadata-v1.json",
        "payload/usr/local/share/ratash/release/package-contract-v1.json",
        "payload/usr/local/share/ratash/release/geodata-manifest.json",
        "payload/usr/local/share/ratash/licenses/Mihomo-GPL-3.0.txt",
        "payload/usr/local/share/ratash/licenses/Mihomo-NOTICE.txt",
        "payload/usr/local/share/ratash/licenses/MetaCubeX-meta-rules-dat-GPL-3.0.txt",
        "payload/usr/local/share/ratash/licenses/GeoData-NOTICE.txt",
        "payload/Library/Application Support/ratash/share/geodata/ASN.mmdb",
        "payload/Library/Application Support/ratash/share/geodata/Country.mmdb",
        "payload/Library/Application Support/ratash/share/geodata/GeoIP.dat",
        "payload/Library/Application Support/ratash/share/geodata/GeoSite.dat",
        "payload/Library/LaunchDaemons/io.ratash.core-runtime.plist",
        "scripts/postinstall",
    ] {
        let path = stage.join(asset);
        assert!(path.is_file(), "missing {}", path.display());
        let expected_mode = if asset == "scripts/postinstall" {
            0o755
        } else {
            0o644
        };
        assert_eq!(
            mode(&path),
            expected_mode,
            "unexpected mode for {}",
            path.display()
        );
    }

    let plist = fs::read_to_string(
        stage.join("payload/Library/LaunchDaemons/io.ratash.core-runtime.plist"),
    )
    .expect("staged LaunchDaemon should be readable");
    for required in [
        "__core-service",
        "--owner-uid",
        "@OWNER_UID@",
        "--socket",
        "/var/run/ratash/core-service.sock",
        "--runtime-root",
        "/Library/Application Support/ratash/runtime",
        "--mihomo",
        "/Library/Application Support/ratash/bin/mihomo",
        "<key>ExitTimeOut</key>",
        "<integer>50</integer>",
    ] {
        assert!(plist.contains(required), "plist is missing {required}");
    }
}

#[test]
fn package_builder_rejects_missing_symlinked_and_changed_geodata_assets() {
    let fixture = TempDirectory::new("package-geodata-verification");
    let ratash = fixture.path.join("ratash");
    let mihomo = fixture.path.join("mihomo");
    let mihomo_license = fixture.path.join("Mihomo-GPL-3.0.txt");
    let geodata = write_geodata_fixture(&fixture.path);
    fs::write(&ratash, b"fixture ratash executable")
        .expect("fixture Ratash executable should be written");
    fs::write(&mihomo, b"fixture Mihomo executable")
        .expect("fixture Mihomo executable should be written");
    fs::write(&mihomo_license, b"fixture GPL-3.0 license")
        .expect("fixture Mihomo license should be written");
    let mihomo_digest = hex_digest(&fs::read(&mihomo).expect("fixture Mihomo should be readable"));
    let run_builder = |allow_custom_manifest: bool| {
        let mut command = Command::new("sh");
        command
            .arg(project_path("scripts/package-macos.sh"))
            .args(["--version", "0.1.0"])
            .args(["--target", "aarch64-apple-darwin"])
            .arg("--ratash")
            .arg(&ratash)
            .arg("--mihomo")
            .arg(&mihomo)
            .args(["--mihomo-sha256", &mihomo_digest])
            .arg("--mihomo-license")
            .arg(&mihomo_license);
        add_geodata_arguments(&mut command, &geodata);
        if !allow_custom_manifest {
            command.env_remove("RATASH_TEST_ALLOW_CUSTOM_GEODATA_MANIFEST");
        }
        command
            .arg("--stage-only")
            .arg(fixture.path.join("rejected-stage"))
            .output()
            .expect("package validation command should run")
    };

    let mismatched_manifest = run_builder(false);
    assert_eq!(mismatched_manifest.status.code(), Some(1));
    assert!(
        String::from_utf8_lossy(&mismatched_manifest.stderr)
            .contains("Geo data manifest identity does not match the bundled catalog")
    );

    let country = geodata.directory.join("Country.mmdb");
    let original_country = fs::read(&country).expect("fixture Country database should be readable");
    let mut changed_country = original_country.clone();
    changed_country[0] ^= 0xff;
    fs::write(&country, changed_country).expect("changed Country database should be written");
    let changed = run_builder(true);
    assert_eq!(changed.status.code(), Some(1));
    assert!(
        String::from_utf8_lossy(&changed.stderr)
            .contains("Country.mmdb failed SHA-256 verification")
    );

    fs::write(&country, original_country).expect("fixture Country database should be restored");
    let geoip = geodata.directory.join("GeoIP.dat");
    let geoip_target = fixture.path.join("GeoIP.dat.target");
    fs::rename(&geoip, &geoip_target).expect("fixture GeoIP database should move");
    symlink(&geoip_target, &geoip).expect("fixture GeoIP symlink should be created");
    let symlinked = run_builder(true);
    assert_eq!(symlinked.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&symlinked.stderr).contains("GeoIP.dat is unavailable"));

    fs::remove_file(&geoip).expect("fixture GeoIP symlink should be removed");
    let missing = run_builder(true);
    assert_eq!(missing.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&missing.stderr).contains("GeoIP.dat is unavailable"));
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
    assert!(script.contains("GEODATA_ROOT=\"$SERVICE_ROOT/share/geodata\""));
    assert!(script.contains(
        "/bin/chmod 0644 \"$GEODATA_ROOT/ASN.mmdb\" \"$GEODATA_ROOT/Country.mmdb\" \"$GEODATA_ROOT/GeoIP.dat\" \"$GEODATA_ROOT/GeoSite.dat\""
    ));

    let uninstaller = fs::read_to_string(project_path("packaging/macos/uninstall.sh"))
        .expect("uninstaller should be readable");
    assert!(uninstaller.contains("'/Library/Application Support/ratash'"));
}

#[test]
fn local_installer_restarts_ratash_around_the_package_upgrade() {
    let installer = fs::read_to_string(project_path("packaging/macos/install-local.sh"))
        .expect("local installer should be readable");
    let checksum = installer
        .find("/usr/bin/shasum -a 256 -c")
        .expect("local installer should verify the package");
    let stop = installer
        .find("/usr/local/bin/ratash stop --json")
        .expect("local installer should stop the existing Supervisor");
    let package = installer
        .find("/usr/sbin/installer")
        .expect("local installer should install the package");
    let start = installer
        .find("/usr/local/bin/ratash start --json")
        .expect("local installer should start the replacement Supervisor");

    assert!(checksum < stop && stop < package && package < start);
    assert!(installer.contains("package_name='@PACKAGE_NAME@'"));

    let builder = fs::read_to_string(project_path("scripts/package-local-macos.sh"))
        .expect("local package builder should be readable");
    assert!(builder.contains("packaging/macos/install-local.sh"));
    assert!(builder.contains("install-ratash.sh"));
}

#[test]
fn package_builder_rejects_a_custom_manifest_for_installable_output() {
    let fixture = TempDirectory::new("package-build");
    let ratash = fixture.path.join("ratash");
    let mihomo = fixture.path.join("mihomo");
    let mihomo_license = fixture.path.join("Mihomo-GPL-3.0.txt");
    let geodata = write_geodata_fixture(&fixture.path);
    fs::write(&ratash, b"#!/bin/sh\nexit 0\n").expect("fixture Ratash should be written");
    fs::write(&mihomo, b"#!/bin/sh\nexit 0\n").expect("fixture Mihomo should be written");
    fs::write(&mihomo_license, b"fixture GPL-3.0 license")
        .expect("fixture Mihomo license should be written");
    set_mode(&ratash, 0o755);
    set_mode(&mihomo, 0o755);
    let digest = hex_digest(&fs::read(&mihomo).expect("fixture Mihomo should be readable"));
    let output_directory = fixture.path.join("dist");

    let mut command = Command::new("sh");
    command
        .arg(project_path("scripts/package-macos.sh"))
        .args(["--version", "0.1.0"])
        .args(["--target", "aarch64-apple-darwin"])
        .arg("--ratash")
        .arg(&ratash)
        .arg("--mihomo")
        .arg(&mihomo)
        .args(["--mihomo-sha256", &digest])
        .arg("--mihomo-license")
        .arg(&mihomo_license);
    add_geodata_arguments(&mut command, &geodata);
    let output = command
        .arg("--output")
        .arg(&output_directory)
        .output()
        .expect("package build command should run");
    assert_eq!(output.status.code(), Some(1));
    assert!(
        String::from_utf8_lossy(&output.stderr)
            .contains("Custom Geo data manifests are restricted to unsigned test staging")
    );
    assert!(!output_directory.exists());
}

#[test]
fn generated_shell_and_manual_assets_cover_only_the_public_command_surface() {
    let assets = [
        project_path("packaging/generated/completions/ratash.bash"),
        project_path("packaging/generated/completions/_ratash"),
        project_path("packaging/generated/completions/ratash.fish"),
        project_path("packaging/generated/man/man1/ratash.1"),
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
        "ratash-release-workload"
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
        "scripts/validate-pinned-mihomo-geodata.sh",
        "scripts/capture-release-benchmarks-macos.sh",
        "scripts/macos-release-resource-probe.sh",
        "packaging/macos/install-local.sh",
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
fn local_and_release_builds_verify_and_pass_pinned_geodata() {
    let local = fs::read_to_string(project_path("scripts/package-local-macos.sh"))
        .expect("local package script should be readable");
    for required in [
        "fixtures/mihomo/v1.19.28/geodata-manifest.json",
        ".assets[$index].url",
        ".assets[$index].size",
        ".assets[$index].sha256",
        "failed size verification",
        "failed SHA-256 verification",
        "validate-pinned-mihomo-geodata.sh",
        "--geodata-directory",
        "--geodata-manifest",
        "--geodata-license",
    ] {
        assert!(
            local.contains(required),
            "local package script is missing {required}"
        );
    }

    let release = fs::read_to_string(project_path(".github/workflows/release.yml"))
        .expect("release workflow should be readable");
    for required in [
        "Download and verify the pinned Geo data",
        "Validate the pinned Geo data with Mihomo",
        "./scripts/validate-pinned-mihomo-geodata.sh",
        "fixtures/mihomo/v1.19.28/geodata-manifest.json",
        "--geodata-directory",
        "--geodata-manifest",
        "--geodata-license",
        "meta-rules-dat-$geodata_source_commit-source.tar.gz",
        "dist/geodata-manifest.json",
    ] {
        assert!(
            release.contains(required),
            "release workflow is missing {required}"
        );
    }
}

#[test]
fn pinned_geodata_acceptance_parses_both_mihomo_data_modes_from_symlinks() {
    let script = fs::read_to_string(project_path("scripts/validate-pinned-mihomo-geodata.sh"))
        .expect("Geo data acceptance script should be readable");

    for required in [
        "for asset in ASN.mmdb Country.mmdb GeoIP.dat GeoSite.dat",
        "/bin/ln -s",
        "geodata-mode: false",
        "geodata-mode: true",
        "GEOIP,CN,DIRECT",
        "GEOSITE,CN,DIRECT",
        "IP-ASN,13335,DIRECT",
        "unset CLASH_AGE_SECRET_KEY",
        "CLASH_CONFIG_STRING",
        "SKIP_SAFE_PATH_CHECK",
    ] {
        assert!(
            script.contains(required),
            "Geo data acceptance script is missing {required}"
        );
    }
    assert_eq!(script.matches("\"$mihomo\" -t").count(), 2);
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
    let ratash = fixture.path.join("ratash");
    let mihomo = fixture.path.join("mihomo");
    let mihomo_license = fixture.path.join("Mihomo-GPL-3.0.txt");
    let geodata = write_geodata_fixture(&fixture.path);
    fs::write(&ratash, b"fixture ratash").expect("fixture Ratash should be written");
    fs::write(&mihomo, b"fixture mihomo").expect("fixture Mihomo should be written");
    fs::write(&mihomo_license, b"fixture license")
        .expect("fixture Mihomo license should be written");
    let digest = hex_digest(&fs::read(&mihomo).expect("fixture Mihomo should be readable"));

    let mut command = Command::new("sh");
    command
        .arg(project_path("scripts/package-macos.sh"))
        .args(["--version", "0.1.0"])
        .args(["--target", "aarch64-apple-darwin"])
        .arg("--ratash")
        .arg(&ratash)
        .arg("--mihomo")
        .arg(&mihomo)
        .args(["--mihomo-sha256", &digest])
        .arg("--mihomo-license")
        .arg(&mihomo_license);
    add_geodata_arguments(&mut command, &geodata);
    let output = command
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
        "RATASH_OWNER_UID",
        "shasum -a 256",
        "## Shell Completion",
        "## Uninstall",
        "ratash start",
        "ratash status",
        "ratash help agent",
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

struct GeodataFixture {
    directory: PathBuf,
    manifest: PathBuf,
    license: PathBuf,
}

fn write_geodata_fixture(root: &Path) -> GeodataFixture {
    let directory = root.join("geodata");
    fs::create_dir(&directory).expect("fixture Geo data directory should be created");
    let assets = [
        (
            "ASN.mmdb",
            "GeoLite2-ASN.mmdb",
            b"fixture ASN database".as_slice(),
        ),
        (
            "Country.mmdb",
            "country.mmdb",
            b"fixture Country database".as_slice(),
        ),
        (
            "GeoIP.dat",
            "geoip.dat",
            b"fixture GeoIP database".as_slice(),
        ),
        (
            "GeoSite.dat",
            "geosite.dat",
            b"fixture GeoSite database".as_slice(),
        ),
    ];
    let manifest_assets = assets
        .into_iter()
        .map(|(file_name, source_name, content)| {
            fs::write(directory.join(file_name), content)
                .expect("fixture Geo data asset should be written");
            serde_json::json!({
                "file_name": file_name,
                "source_name": source_name,
                "url": format!(
                    "https://raw.githubusercontent.com/MetaCubeX/meta-rules-dat/1567448176a3b6b56661a93b96d1e1c4c10bf2f9/{source_name}"
                ),
                "size": content.len(),
                "sha256": hex_digest(content)
            })
        })
        .collect::<Vec<_>>();
    let manifest = root.join("geodata-manifest.json");
    fs::write(
        &manifest,
        serde_json::to_vec_pretty(&serde_json::json!({
            "schema_version": 1,
            "core_version": "v1.19.28",
            "repository": "https://github.com/MetaCubeX/meta-rules-dat",
            "asset_commit": "1567448176a3b6b56661a93b96d1e1c4c10bf2f9",
            "source_commit": "4178770badecb1b349fbcd62c737e0d7a2079729",
            "repository_license": "GPL-3.0-only",
            "assets": manifest_assets
        }))
        .expect("fixture Geo data manifest should serialize"),
    )
    .expect("fixture Geo data manifest should be written");
    let license = root.join("MetaCubeX-meta-rules-dat-GPL-3.0.txt");
    fs::write(&license, b"fixture GPL-3.0-only license")
        .expect("fixture Geo data license should be written");
    GeodataFixture {
        directory,
        manifest,
        license,
    }
}

fn add_geodata_arguments(command: &mut Command, fixture: &GeodataFixture) {
    command
        .env("RATASH_TEST_ALLOW_CUSTOM_GEODATA_MANIFEST", "1")
        .arg("--geodata-directory")
        .arg(&fixture.directory)
        .arg("--geodata-manifest")
        .arg(&fixture.manifest)
        .arg("--geodata-license")
        .arg(&fixture.license);
}

struct TempDirectory {
    path: PathBuf,
}

impl TempDirectory {
    fn new(label: &str) -> Self {
        let path = std::env::temp_dir().join(format!("ratash-{label}-{}", uuid::Uuid::new_v4()));
        fs::create_dir(&path).expect("temporary directory should be created");
        Self { path }
    }
}

impl Drop for TempDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}
