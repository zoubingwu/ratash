use std::fs;
use std::os::unix::fs::{PermissionsExt, symlink};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use hopash::config::{AuthoritativeConfig, ConfigCompiler, CoreConfigValidator};
use hopash::profile::{ProfileSnapshot, SnapshotLimits};
use hopash::validator::{MihomoCommandValidator, MihomoValidationErrorKind};
use sha2::{Digest, Sha256};

static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);

struct TestDirectory {
    path: PathBuf,
}

impl TestDirectory {
    fn new(name: &str) -> Self {
        let sequence = NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "hopash-validator-{name}-{}-{sequence}",
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
) -> hopash::config::EffectiveConfiguration {
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
            &AuthoritativeConfig::new("/private/tmp/hopash-core.sock", "fixture-secret"),
            staging_root,
        )
        .expect("fixture configuration should compile")
}

fn validator(binary: &Path, timeout: Duration) -> MihomoCommandValidator {
    MihomoCommandValidator::new(binary, sha256(binary), timeout)
        .expect("validator policy should be valid")
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
