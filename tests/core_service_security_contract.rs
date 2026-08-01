use std::fs;
use std::path::PathBuf;

use hopash::production::HOPASH_CODE_IDENTIFIER;

fn project_path(path: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(path)
}

#[test]
fn package_signing_uses_the_runtime_peer_identifier() {
    let script = fs::read_to_string(project_path("scripts/package-macos.sh"))
        .expect("package script should be readable");
    let signing_argument = format!("--identifier '{HOPASH_CODE_IDENTIFIER}'");

    assert!(script.contains(&signing_argument));
    assert!(script.contains("--options runtime"));
}
