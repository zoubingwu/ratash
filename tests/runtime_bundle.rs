use hopash::config::{AuthoritativeConfig, ConfigCompiler, EffectiveConfiguration};
use hopash::domain::RuntimeGeneration;
use hopash::profile::{ProfileSnapshot, SnapshotLimits};
use hopash::runtime_bundle::{RuntimeBundleStageErrorKind, RuntimeBundleStager};
use hopash::service::RuntimeManifestV1;
use sha2::{Digest, Sha256};
use std::fs;
use std::os::unix::fs::{PermissionsExt, symlink};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);

struct TestDirectory {
    path: PathBuf,
}

impl TestDirectory {
    fn new(name: &str) -> Self {
        let sequence = NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "hopash-runtime-bundle-{name}-{}-{sequence}",
            std::process::id()
        ));
        fs::create_dir_all(&path).expect("the fixture directory should be created");
        Self { path }
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

#[test]
fn stages_a_private_complete_and_idempotent_runtime_generation() {
    let directory = TestDirectory::new("complete");
    let effective = effective_configuration(&directory.path);
    let binary = Path::new("/bin/sh");
    let binary_sha256 = sha256(&fs::read(binary).expect("the fixture binary should be readable"));
    let runtime_root = directory.path.join("runtime");
    let stager = RuntimeBundleStager::new(
        &runtime_root,
        binary,
        &binary_sha256,
        effective.compiler_policy_sha256(),
    )
    .expect("the staging policy should be valid");

    let first = stager
        .stage(RuntimeGeneration(7), &effective)
        .expect("the Runtime Generation should be staged");
    let second = stager
        .stage(RuntimeGeneration(7), &effective)
        .expect("identical staging should be idempotent");

    assert_eq!(first, second);
    assert_eq!(mode(&runtime_root), 0o700);
    assert_eq!(mode(&first.generation_root), 0o700);
    assert_eq!(mode(&first.generation_root.join("mihomo")), 0o500);
    assert_eq!(mode(&first.generation_root.join("config.yaml")), 0o400);
    assert_eq!(
        mode(&first.generation_root.join("providers/nested/local.yaml")),
        0o400
    );
    assert_eq!(
        fs::read(first.generation_root.join("providers/nested/local.yaml"))
            .expect("the provider should be staged"),
        b"payload:\n  - example.com\n"
    );
    let manifest_bytes =
        fs::read(first.generation_root.join("manifest.json")).expect("the manifest should exist");
    assert_eq!(first.manifest_sha256, sha256(&manifest_bytes));
    let manifest: RuntimeManifestV1 =
        serde_json::from_slice(&manifest_bytes).expect("the manifest should decode");
    assert_eq!(manifest.provider_files.len(), 1);
    assert_eq!(
        manifest.provider_files[0].path,
        "providers/nested/local.yaml"
    );
    assert_eq!(manifest.provider_files[0].size, 25);
    assert_eq!(
        manifest.provider_files[0].sha256,
        sha256(b"payload:\n  - example.com\n")
    );
    assert!(
        fs::read_dir(&runtime_root)
            .expect("the runtime root should be readable")
            .all(|entry| !entry
                .expect("the runtime entry should be readable")
                .file_name()
                .to_string_lossy()
                .ends_with(".pending"))
    );
}

#[test]
fn rejects_changed_existing_provider_content() {
    let directory = TestDirectory::new("tampered-provider");
    let effective = effective_configuration(&directory.path);
    let binary = Path::new("/bin/sh");
    let binary_sha256 = sha256(&fs::read(binary).expect("the fixture binary should be readable"));
    let stager = RuntimeBundleStager::new(
        directory.path.join("runtime"),
        binary,
        binary_sha256,
        effective.compiler_policy_sha256(),
    )
    .expect("the staging policy should be valid");
    let bundle = stager
        .stage(RuntimeGeneration(1), &effective)
        .expect("the Runtime Generation should be staged");
    let staged_provider = bundle.generation_root.join("providers/nested/local.yaml");
    fs::set_permissions(&staged_provider, fs::Permissions::from_mode(0o600))
        .expect("the staged provider permissions should be changed");
    fs::write(staged_provider, b"changed: true\n").expect("the staged provider should be changed");

    assert_eq!(
        stager
            .stage(RuntimeGeneration(1), &effective)
            .expect_err("changed content should be rejected")
            .kind(),
        RuntimeBundleStageErrorKind::ExistingGenerationMismatch
    );
}

#[test]
fn rejects_binary_symlinks_and_identity_changes() {
    let directory = TestDirectory::new("binary-identity");
    let effective = effective_configuration(&directory.path);
    let binary_link = directory.path.join("mihomo-link");
    symlink("/bin/sh", &binary_link).expect("the binary symlink should be created");
    let real_binary = fs::read("/bin/sh").expect("the fixture binary should be readable");
    let runtime_root = directory.path.join("runtime");
    let symlink_stager = RuntimeBundleStager::new(
        &runtime_root,
        &binary_link,
        sha256(&real_binary),
        effective.compiler_policy_sha256(),
    )
    .expect("the staging policy should be valid");
    assert_eq!(
        symlink_stager
            .stage(RuntimeGeneration(1), &effective)
            .expect_err("a binary symlink should be rejected")
            .kind(),
        RuntimeBundleStageErrorKind::InvalidBinary
    );

    let mismatch_stager = RuntimeBundleStager::new(
        &runtime_root,
        "/bin/sh",
        sha256(b"different binary"),
        effective.compiler_policy_sha256(),
    )
    .expect("the staging policy should be valid");
    assert_eq!(
        mismatch_stager
            .stage(RuntimeGeneration(2), &effective)
            .expect_err("a changed binary should be rejected")
            .kind(),
        RuntimeBundleStageErrorKind::BinaryIdentityMismatch
    );
}

#[test]
fn rejects_a_different_compiler_policy() {
    let directory = TestDirectory::new("compiler-policy");
    let effective = effective_configuration(&directory.path);
    let binary = fs::read("/bin/sh").expect("the fixture binary should be readable");
    let stager = RuntimeBundleStager::new(
        directory.path.join("runtime"),
        "/bin/sh",
        sha256(&binary),
        sha256(b"different compiler policy"),
    )
    .expect("the staging policy should be valid");

    assert_eq!(
        stager
            .stage(RuntimeGeneration(1), &effective)
            .expect_err("a different compiler policy should be rejected")
            .kind(),
        RuntimeBundleStageErrorKind::CompilerPolicyMismatch
    );
}

fn effective_configuration(root: &Path) -> EffectiveConfiguration {
    let profile_root = root.join("profile");
    fs::create_dir_all(profile_root.join("providers/nested"))
        .expect("the profile provider directory should be created");
    fs::write(
        profile_root.join("providers/nested/local.yaml"),
        b"payload:\n  - example.com\n",
    )
    .expect("the local provider fixture should be written");
    let snapshot = ProfileSnapshot::parse(
        br#"
proxy-groups:
  - name: Main
    type: select
    proxies: [DIRECT]
rule-providers:
  local:
    type: file
    behavior: domain
    format: yaml
    path: providers/nested/local.yaml
rules:
  - MATCH,DIRECT
"#,
        SnapshotLimits::new(128 * 1_024, 32),
    )
    .expect("the profile fixture should parse");
    ConfigCompiler::bundled()
        .expect("the bundled compiler should load")
        .compile(
            &snapshot,
            &[],
            &AuthoritativeConfig::new("/tmp/hopash-core.sock", "fixture-secret"),
            &profile_root,
        )
        .expect("the profile fixture should compile")
}

fn mode(path: &Path) -> u32 {
    fs::metadata(path)
        .expect("the staged path should have metadata")
        .permissions()
        .mode()
        & 0o777
}

fn sha256(content: &[u8]) -> String {
    Sha256::digest(content)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}
