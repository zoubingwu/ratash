use ratash::config::{AuthoritativeConfig, ConfigCompiler, EffectiveConfiguration};
use ratash::domain::RuntimeGeneration;
use ratash::profile::{ProfileSnapshot, SnapshotLimits};
use ratash::runtime_bundle::{
    RuntimeBundleStageErrorKind, RuntimeBundleStager, RuntimeGenerationPruneErrorKind,
    RuntimeGenerationRetention, prune_runtime_generations,
};
use ratash::service::RuntimeManifestV1;
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
            "ratash-runtime-bundle-{name}-{}-{sequence}",
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
    let binary = fixture_binary();
    let binary_sha256 = sha256(&fs::read(&binary).expect("the fixture binary should be readable"));
    let runtime_root = directory.path.join("runtime");
    let stager = RuntimeBundleStager::new(
        &runtime_root,
        &binary,
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
    let binary = fixture_binary();
    let binary_sha256 = sha256(&fs::read(&binary).expect("the fixture binary should be readable"));
    let stager = RuntimeBundleStager::new(
        directory.path.join("runtime"),
        &binary,
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
    let binary = fixture_binary();
    let binary_link = directory.path.join("mihomo-link");
    symlink(&binary, &binary_link).expect("the binary symlink should be created");
    let real_binary = fs::read(&binary).expect("the fixture binary should be readable");
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
        &binary,
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
    let binary_path = fixture_binary();
    let binary = fs::read(&binary_path).expect("the fixture binary should be readable");
    let stager = RuntimeBundleStager::new(
        directory.path.join("runtime"),
        binary_path,
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

fn fixture_binary() -> PathBuf {
    std::env::current_exe().expect("the fixture executable path should resolve")
}

#[test]
fn prunes_runtime_generations_to_current_previous_and_prepared() {
    let directory = TestDirectory::new("bounded-retention");
    let runtime_root = directory.path.join("runtime");
    fs::create_dir(&runtime_root).expect("the runtime root should be created");
    fs::set_permissions(&runtime_root, fs::Permissions::from_mode(0o700))
        .expect("the runtime root should be private");
    for generation in 1..=6 {
        create_generation_directory(&runtime_root, generation);
    }

    let result = prune_runtime_generations(
        &runtime_root,
        RuntimeGenerationRetention::new(
            Some(RuntimeGeneration(5)),
            Some(RuntimeGeneration(4)),
            Some(RuntimeGeneration(6)),
        ),
    )
    .expect("strict stale generations should be removed");

    assert_eq!(result.scanned_entries, 6);
    assert_eq!(result.removed_generations, 3);
    assert_eq!(generation_names(&runtime_root), vec![4, 5, 6]);
}

#[test]
fn unsafe_runtime_entries_preserve_every_generation_and_redact_paths() {
    for unsafe_kind in ["unknown", "symlink", "non-strict"] {
        let directory = TestDirectory::new(unsafe_kind);
        let runtime_root = directory.path.join("runtime");
        fs::create_dir(&runtime_root).expect("the runtime root should be created");
        fs::set_permissions(&runtime_root, fs::Permissions::from_mode(0o700))
            .expect("the runtime root should be private");
        create_generation_directory(&runtime_root, 1);
        create_generation_directory(&runtime_root, 2);
        match unsafe_kind {
            "unknown" => {
                fs::write(runtime_root.join("unexpected.txt"), b"preserve")
                    .expect("the unknown entry should be written");
            }
            "symlink" => {
                symlink(
                    runtime_root.join("generation-00000000000000000002"),
                    runtime_root.join("generation-00000000000000000003"),
                )
                .expect("the generation symlink should be created");
            }
            "non-strict" => {
                fs::create_dir(runtime_root.join("generation-3"))
                    .expect("the non-strict generation should be created");
            }
            _ => unreachable!(),
        }

        let error = prune_runtime_generations(
            &runtime_root,
            RuntimeGenerationRetention::new(Some(RuntimeGeneration(2)), None, None),
        )
        .expect_err("an unsafe entry should stop cleanup");

        assert_eq!(error.kind(), RuntimeGenerationPruneErrorKind::UnsafeEntry);
        assert!(
            runtime_root
                .join("generation-00000000000000000001")
                .exists()
        );
        assert!(
            runtime_root
                .join("generation-00000000000000000002")
                .exists()
        );
        let diagnostic = format!("{error:?} {error}");
        assert!(!diagnostic.contains(&directory.path.display().to_string()));
        assert!(!diagnostic.contains(unsafe_kind));
    }
}

#[test]
fn recovers_a_strict_crash_left_pruning_quarantine() {
    let directory = TestDirectory::new("pruning-recovery");
    let runtime_root = directory.path.join("runtime");
    fs::create_dir(&runtime_root).expect("the runtime root should be created");
    fs::set_permissions(&runtime_root, fs::Permissions::from_mode(0o700))
        .expect("the runtime root should be private");
    create_generation_directory(&runtime_root, 2);
    let quarantine = runtime_root
        .join(".generation-00000000000000000001-00000000-0000-4000-8000-000000000000.pruning");
    fs::create_dir(&quarantine).expect("the pruning quarantine should be created");
    fs::set_permissions(&quarantine, fs::Permissions::from_mode(0o700))
        .expect("the pruning quarantine should be private");

    let result = prune_runtime_generations(
        &runtime_root,
        RuntimeGenerationRetention::new(Some(RuntimeGeneration(2)), None, None),
    )
    .expect("a strict pruning quarantine should be recovered");

    assert_eq!(result.removed_generations, 1);
    assert!(!quarantine.exists());
    assert_eq!(generation_names(&runtime_root), vec![2]);
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
            &AuthoritativeConfig::new("/tmp/ratash-core.sock", "fixture-secret"),
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

fn create_generation_directory(root: &Path, generation: u64) {
    let path = root.join(format!("generation-{generation:020}"));
    fs::create_dir(&path).expect("the generation directory should be created");
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
        .expect("the generation directory should be private");
}

fn generation_names(root: &Path) -> Vec<u64> {
    let mut generations = fs::read_dir(root)
        .expect("the runtime root should be readable")
        .map(|entry| {
            entry
                .expect("the runtime entry should be readable")
                .file_name()
                .to_string_lossy()
                .strip_prefix("generation-")
                .expect("the entry should be a generation")
                .parse::<u64>()
                .expect("the generation should parse")
        })
        .collect::<Vec<_>>();
    generations.sort_unstable();
    generations
}

fn sha256(content: &[u8]) -> String {
    Sha256::digest(content)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}
