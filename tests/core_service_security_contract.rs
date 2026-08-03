use std::fs;
use std::path::PathBuf;

use ratash::production::RATASH_CODE_IDENTIFIER;

fn project_path(path: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(path)
}

#[test]
fn package_signing_uses_the_runtime_peer_identifier() {
    let script = fs::read_to_string(project_path("scripts/package-macos.sh"))
        .expect("package script should be readable");
    let signing_argument = format!("--identifier '{RATASH_CODE_IDENTIFIER}'");

    assert!(script.contains(&signing_argument));
    assert!(script.contains("--options runtime"));
}

#[test]
fn personal_package_uses_the_local_unsigned_runtime_identity() {
    let script = fs::read_to_string(project_path("scripts/package-local-macos.sh"))
        .expect("personal package script should be readable");

    assert!(script.contains("--no-default-features"));
    assert!(script.contains("--features local-unsigned"));
    assert!(script.contains("--identifier 'ratash'"));
    assert!(script.contains("--options runtime"));
    assert!(script.contains("--sign -"));
    assert!(script.contains("codesign --verify --strict"));
}

#[test]
fn release_workflow_keeps_the_developer_id_trust_policy() {
    let workflow = fs::read_to_string(project_path(".github/workflows/release.yml"))
        .expect("release workflow should be readable");

    assert!(workflow.contains("MACOS_APPLICATION_IDENTITY"));
    assert!(workflow.contains("MACOS_INSTALLER_IDENTITY"));
    assert!(workflow.contains("--no-default-features"));
    assert!(!workflow.contains("--features local-unsigned"));
}

#[test]
fn macos_ci_exercises_the_local_unsigned_policy() {
    let workflow = fs::read_to_string(project_path(".github/workflows/ci.yml"))
        .expect("CI workflow should be readable");

    assert!(workflow.contains(
        "cargo test --locked --all-targets --no-default-features --features local-unsigned"
    ));
}
