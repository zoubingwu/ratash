use std::fs;
use std::os::unix::fs::{PermissionsExt, symlink};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use ratash::config::{AuthoritativeConfig, ConfigCompiler, CoreConfigValidator};
use ratash::geodata::GeoDataCatalog;
use ratash::profile::{ProfileSnapshot, SnapshotLimits};
use ratash::validator::{MihomoCommandValidator, MihomoValidationErrorKind};
use sha2::{Digest, Sha256};

static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);

struct TestDirectory {
    path: PathBuf,
}

impl TestDirectory {
    fn new(name: &str) -> Self {
        let sequence = NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "ratash-validator-{name}-{}-{sequence}",
            std::process::id()
        ));
        fs::create_dir(&path).expect("test directory should be created");
        Self { path }
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.path).expect("test directory should be removed");
    }
}

#[test]
fn pinned_fixture_validates_the_effective_configuration_and_cleans_staging_files() {
    let directory = TestDirectory::new("success");
    let binary = fixture_binary(
        &directory.path,
        "validator",
        r#"#!/bin/sh
test "$1" = "-t" || exit 64
test "$2" = "-d" || exit 64
test "$4" = "-f" || exit 64
test "$3" = "$(dirname "$5")" || exit 64
grep -q "mode: rule" "$5" || exit 65
grep -q "enable: true" "$5" || exit 65
exit 0
"#,
    );
    let staging = directory.path.join("staging");
    fs::create_dir(&staging).expect("staging directory should be created");
    let effective = effective_configuration(&staging, "MATCH,DIRECT");
    let validator = MihomoCommandValidator::new(&binary, sha256(&binary), Duration::from_secs(1))
        .expect("validator policy should be valid");

    validator
        .validate_detailed(&effective, &staging)
        .expect("fixture should accept the configuration");

    assert!(
        fs::read_dir(&staging)
            .expect("staging directory should read")
            .next()
            .is_none(),
        "validation files should be removed"
    );
    assert_eq!(
        fs::metadata(&staging)
            .expect("staging metadata should exist")
            .permissions()
            .mode()
            & 0o777,
        0o700
    );
}

#[test]
fn configured_geodata_is_verified_and_staged_before_core_validation() {
    let directory = TestDirectory::new("geodata");
    let binary = fixture_binary(
        &directory.path,
        "validator",
        r#"#!/bin/sh
test -f "$3/Country.mmdb" || exit 65
test -f "$3/GeoIP.dat" || exit 65
test -f "$3/GeoSite.dat" || exit 65
test -f "$3/ASN.mmdb" || exit 65
exit 0
"#,
    );
    let source = directory.path.join("source");
    let staging = directory.path.join("staging");
    fs::create_dir(&source).expect("Geo-data source should be created");
    fs::create_dir(&staging).expect("staging directory should be created");
    let catalog = fixture_geodata_catalog(&source);
    let interrupted = staging.join(".ratash-geodata-ASN.mmdb.pending");
    symlink(source.join("ASN.mmdb"), &interrupted)
        .expect("an interrupted Geo-data link should be staged");
    let effective = effective_configuration(&staging, "MATCH,DIRECT");
    let validator = validator(&binary, Duration::from_secs(5))
        .with_geodata(&source, catalog)
        .expect("the Geo-data policy should be valid");

    validator
        .validate_detailed(&effective, &staging)
        .expect("the staged Geo data should reach Core validation");

    for file_name in ["ASN.mmdb", "Country.mmdb", "GeoIP.dat", "GeoSite.dat"] {
        let path = staging.join(file_name);
        let metadata = fs::symlink_metadata(&path).expect("the staged Geo-data asset should exist");
        assert!(metadata.file_type().is_symlink());
        assert!(
            fs::metadata(path)
                .expect("the link target should exist")
                .is_file()
        );
    }
    assert!(fs::symlink_metadata(interrupted).is_err());
}

#[test]
fn configured_geodata_preserves_an_unrecognized_pending_entry() {
    let directory = TestDirectory::new("unsafe-geodata-pending");
    let binary = fixture_binary(&directory.path, "validator", "#!/bin/sh\nexit 0\n");
    let source = directory.path.join("source");
    let staging = directory.path.join("staging");
    fs::create_dir(&source).expect("Geo-data source should be created");
    fs::create_dir(&staging).expect("staging directory should be created");
    let catalog = fixture_geodata_catalog(&source);
    let pending = staging.join(".ratash-geodata-ASN.mmdb.pending");
    fs::write(&pending, b"preserve").expect("the unrecognized entry should be written");
    let effective = effective_configuration(&staging, "MATCH,DIRECT");
    let validator = validator(&binary, Duration::from_secs(5))
        .with_geodata(&source, catalog)
        .expect("the Geo-data policy should be valid");

    let error = validator
        .validate_detailed(&effective, &staging)
        .expect_err("an unrecognized pending entry should block staging");

    assert_eq!(error.kind(), MihomoValidationErrorKind::GeoDataUnavailable);
    assert_eq!(
        fs::read(pending).expect("the unrecognized entry should be preserved"),
        b"preserve"
    );
}

#[test]
fn zero_exit_with_fatal_output_is_rejected() {
    let directory = TestDirectory::new("fatal-output");
    let binary = fixture_binary(
        &directory.path,
        "validator",
        "#!/bin/sh\necho 'level=fatal parse config error' >&2\nexit 0\n",
    );
    let staging = directory.path.join("staging");
    fs::create_dir(&staging).expect("staging directory should be created");
    let effective = effective_configuration(&staging, "MATCH,DIRECT");
    let validator = validator(&binary, Duration::from_secs(1));

    let error = validator
        .validate_detailed(&effective, &staging)
        .expect_err("fatal output should reject the configuration");

    assert_eq!(
        error.kind(),
        MihomoValidationErrorKind::ConfigurationRejected
    );
}

#[test]
fn validator_enforces_the_process_deadline_and_removes_sensitive_config() {
    let directory = TestDirectory::new("timeout");
    let binary = fixture_binary(&directory.path, "validator", "#!/bin/sh\nsleep 1\nexit 0\n");
    let staging = directory.path.join("staging");
    fs::create_dir(&staging).expect("staging directory should be created");
    let effective = effective_configuration(&staging, "DOMAIN,secret.example,DIRECT");
    let validator = validator(&binary, Duration::from_millis(20));

    let error = validator
        .validate_detailed(&effective, &staging)
        .expect_err("slow validator should time out");

    assert_eq!(error.kind(), MihomoValidationErrorKind::TimedOut);
    assert!(
        fs::read_dir(&staging)
            .expect("staging directory should read")
            .next()
            .is_none(),
        "timed-out validation should remove temporary files"
    );
}

#[test]
fn cancellation_terminates_a_stalled_validator_and_removes_sensitive_config() {
    let directory = TestDirectory::new("cancel");
    let binary = fixture_binary(
        &directory.path,
        "validator",
        "#!/bin/sh\n: > \"$3/started\"\nexec /bin/sleep 30\n",
    );
    let staging = directory.path.join("staging");
    fs::create_dir(&staging).expect("staging directory should be created");
    let effective = effective_configuration(&staging, "DOMAIN,secret.example,DIRECT");
    let validator = Arc::new(validator(&binary, Duration::from_secs(30)));
    let worker_validator = Arc::clone(&validator);
    let worker_staging = staging.clone();
    let worker =
        std::thread::spawn(move || worker_validator.validate_detailed(&effective, &worker_staging));
    let started_marker = staging.join("started");
    let startup_deadline = Instant::now() + Duration::from_secs(1);
    while !started_marker.exists() {
        assert!(
            Instant::now() < startup_deadline,
            "the fixture validator should start"
        );
        std::thread::yield_now();
    }
    let started = Instant::now();

    CoreConfigValidator::cancel_pending(validator.as_ref());

    let error = worker
        .join()
        .expect("the validator worker should finish")
        .expect_err("the stalled validator should be cancelled");
    assert_eq!(error.kind(), MihomoValidationErrorKind::Cancelled);
    assert!(started.elapsed() < Duration::from_millis(200));
    let entries = fs::read_dir(&staging)
        .expect("staging directory should read")
        .map(|entry| entry.expect("staging entry should load").file_name())
        .collect::<Vec<_>>();
    assert_eq!(entries, ["started"]);
}

#[test]
fn changed_or_symlinked_binaries_fail_identity_validation_before_spawn() {
    let directory = TestDirectory::new("identity");
    let binary = fixture_binary(&directory.path, "validator", "#!/bin/sh\nexit 0\n");
    let expected_hash = sha256(&binary);
    let staging = directory.path.join("staging");
    fs::create_dir(&staging).expect("staging directory should be created");
    let effective = effective_configuration(&staging, "MATCH,DIRECT");
    fs::write(&binary, "#!/bin/sh\nexit 9\n").expect("fixture binary should change");
    fs::set_permissions(&binary, fs::Permissions::from_mode(0o700))
        .expect("fixture binary should remain executable");
    let changed = MihomoCommandValidator::new(&binary, expected_hash, Duration::from_secs(1))
        .expect("validator policy should be valid");

    assert_eq!(
        changed
            .validate_detailed(&effective, &staging)
            .expect_err("changed binary should fail")
            .kind(),
        MihomoValidationErrorKind::BinaryIdentityMismatch
    );

    let target = fixture_binary(&directory.path, "target", "#!/bin/sh\nexit 0\n");
    let link = directory.path.join("mihomo-link");
    symlink(&target, &link).expect("fixture symlink should be created");
    let linked = validator(&link, Duration::from_secs(1));
    assert_eq!(
        linked
            .validate_detailed(&effective, &staging)
            .expect_err("symlinked binary should fail")
            .kind(),
        MihomoValidationErrorKind::InvalidBinary
    );
}

#[test]
fn public_config_validator_seam_returns_a_safe_core_error() {
    let directory = TestDirectory::new("public-seam");
    let binary = fixture_binary(&directory.path, "validator", "#!/bin/sh\nexit 7\n");
    let staging = directory.path.join("staging");
    fs::create_dir(&staging).expect("staging directory should be created");
    let effective = effective_configuration(&staging, "DOMAIN,private.example,DIRECT");
    let validator = validator(&binary, Duration::from_secs(1));

    let error = CoreConfigValidator::validate(&validator, &effective, &staging)
        .expect_err("fixture should reject the configuration");
    let diagnostic = format!("{error:?} {error}");

    assert_eq!(error.message(), "Mihomo configuration validation failed");
    assert!(!diagnostic.contains("private.example"));
    assert!(!diagnostic.contains(binary.to_string_lossy().as_ref()));
}

fn effective_configuration(
    staging_root: &Path,
    rule: &str,
) -> ratash::config::EffectiveConfiguration {
    let snapshot = ProfileSnapshot::parse(
        b"proxies: []\nproxy-groups: []\nrules: []\n",
        SnapshotLimits::new(4_096, 16),
    )
    .expect("fixture snapshot should parse");
    ConfigCompiler::bundled()
        .expect("compiler should initialize")
        .compile(
            &snapshot,
            &[rule.to_owned()],
            &AuthoritativeConfig::new("/private/tmp/ratash-core.sock", "fixture-secret"),
            staging_root,
        )
        .expect("fixture configuration should compile")
}

fn validator(binary: &Path, timeout: Duration) -> MihomoCommandValidator {
    MihomoCommandValidator::new(binary, sha256(binary), timeout)
        .expect("validator policy should be valid")
}

fn fixture_geodata_catalog(root: &Path) -> GeoDataCatalog {
    let commit = "a".repeat(40);
    let assets = [
        ("ASN.mmdb", "GeoLite2-ASN.mmdb", b"asn".as_slice()),
        ("Country.mmdb", "country.mmdb", b"country".as_slice()),
        ("GeoIP.dat", "geoip.dat", b"geoip".as_slice()),
        ("GeoSite.dat", "geosite.dat", b"geosite".as_slice()),
    ]
    .into_iter()
    .map(|(file_name, source_name, content)| {
        fs::write(root.join(file_name), content).expect("the Geo-data fixture should be written");
        serde_json::json!({
            "file_name": file_name,
            "source_name": source_name,
            "url": format!(
                "https://raw.githubusercontent.com/MetaCubeX/meta-rules-dat/{commit}/{source_name}"
            ),
            "size": content.len(),
            "sha256": sha256_bytes(content),
        })
    })
    .collect::<Vec<_>>();
    let manifest = serde_json::json!({
        "schema_version": 1,
        "core_version": "v1.19.28",
        "repository": "https://github.com/MetaCubeX/meta-rules-dat",
        "asset_commit": commit,
        "source_commit": "b".repeat(40),
        "repository_license": "GPL-3.0-only",
        "assets": assets,
    });
    GeoDataCatalog::from_manifest(&manifest.to_string())
        .expect("the Geo-data fixture manifest should be valid")
}

fn sha256_bytes(content: &[u8]) -> String {
    Sha256::digest(content)
        .iter()
        .fold(String::with_capacity(64), |mut output, byte| {
            use std::fmt::Write as _;
            write!(output, "{byte:02x}").expect("writing to a String should succeed");
            output
        })
}

fn fixture_binary(root: &Path, name: &str, script: &str) -> PathBuf {
    let path = root.join(name);
    fs::write(&path, script).expect("fixture binary should be written");
    fs::set_permissions(&path, fs::Permissions::from_mode(0o700))
        .expect("fixture binary should be executable");
    path
}

fn sha256(path: &Path) -> String {
    let bytes = fs::read(path).expect("fixture binary should be readable");
    Sha256::digest(bytes)
        .iter()
        .fold(String::with_capacity(64), |mut output, byte| {
            use std::fmt::Write as _;
            write!(output, "{byte:02x}").expect("writing to a String should succeed");
            output
        })
}
