use std::process::Command;

#[test]
fn bare_binary_prints_help_and_succeeds() {
    let output = Command::new(env!("CARGO_BIN_EXE_hopash"))
        .output()
        .expect("hopash should start");

    assert!(output.status.success());
    assert!(String::from_utf8_lossy(&output.stdout).contains("Usage: hopash [COMMAND]"));
    assert!(output.stderr.is_empty());
}

#[test]
fn version_flag_prints_the_package_version() {
    let output = Command::new(env!("CARGO_BIN_EXE_hopash"))
        .arg("--version")
        .output()
        .expect("hopash --version should start");

    assert!(output.status.success());
    assert_eq!(String::from_utf8_lossy(&output.stdout), "hopash 0.1.0\n");
    assert!(output.stderr.is_empty());
}

#[test]
fn help_points_to_the_agent_contract() {
    let output = Command::new(env!("CARGO_BIN_EXE_hopash"))
        .arg("help")
        .output()
        .expect("hopash help should start");

    assert!(output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stdout).contains("hopash help agent"),
        "{}",
        String::from_utf8_lossy(&output.stdout)
    );
    assert!(output.stderr.is_empty());
}

#[test]
fn agent_help_contains_the_safe_rule_workflow() {
    let output = Command::new(env!("CARGO_BIN_EXE_hopash"))
        .args(["help", "agent"])
        .output()
        .expect("hopash help agent should start");
    let stdout = String::from_utf8_lossy(&output.stdout);

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    for required_text in [
        "hopash profile add",
        "hopash proxy select",
        "hopash latency show",
        "hopash rule replace",
        "hopash rule list --json",
        "complete, case-sensitive Rule String",
        "rule_busy",
        "rule_not_found",
        "rule_ambiguous",
        "rule_already_exists",
        "Read the current rule list before retrying",
        "hopash status --json",
        "last committed Runtime Generation remains active",
        "hopash start --json",
    ] {
        assert!(
            stdout.contains(required_text),
            "missing {required_text}:\n{stdout}"
        );
    }
    assert!(output.stderr.is_empty());
}

#[test]
fn json_operation_failure_uses_stderr_and_the_supervisor_exit_code() {
    let output = Command::new(env!("CARGO_BIN_EXE_hopash"))
        .args(["status", "--json"])
        .output()
        .expect("hopash status --json should start");

    assert_eq!(output.status.code(), Some(3));
    assert!(output.stdout.is_empty());
    let error: serde_json::Value =
        serde_json::from_slice(&output.stderr).expect("stderr should contain one JSON document");
    assert_eq!(
        error,
        serde_json::json!({
            "schema_version": 1,
            "error": {
                "code": "supervisor_unavailable",
                "message": "The Hopash Supervisor IPC endpoint is unavailable",
                "retryable": true
            }
        })
    );
}

#[test]
fn invalid_json_invocation_returns_a_json_usage_error() {
    let output = Command::new(env!("CARGO_BIN_EXE_hopash"))
        .args(["rule", "add", "MATCH,DIRECT", "--json"])
        .output()
        .expect("invalid JSON invocation should start");

    assert_eq!(output.status.code(), Some(2));
    assert!(output.stdout.is_empty());
    let error: serde_json::Value =
        serde_json::from_slice(&output.stderr).expect("stderr should contain one JSON document");
    assert_eq!(error["error"]["code"], "usage");
    assert_eq!(error["error"]["retryable"], false);
    assert!(
        error["error"]["message"]
            .as_str()
            .is_some_and(|message| message.contains("placement"))
    );
}

#[test]
fn invalid_subscription_url_is_redacted_from_human_and_json_errors() {
    let subscription_url =
        "ftp://alice:password@example.test/access-token-value?token=query-secret-value";

    let human = Command::new(env!("CARGO_BIN_EXE_hopash"))
        .args(["profile", "add", subscription_url])
        .output()
        .expect("invalid human invocation should start");

    assert_eq!(human.status.code(), Some(2));
    assert!(human.stdout.is_empty());
    let human_error = String::from_utf8_lossy(&human.stderr);
    assert!(human_error.contains("[REDACTED]"), "{human_error}");
    assert!(!human_error.contains("password"), "{human_error}");
    assert!(!human_error.contains("access-token-value"), "{human_error}");
    assert!(!human_error.contains("query-secret-value"), "{human_error}");

    let json = Command::new(env!("CARGO_BIN_EXE_hopash"))
        .args(["profile", "add", subscription_url, "--json"])
        .output()
        .expect("invalid JSON invocation should start");

    assert_eq!(json.status.code(), Some(2));
    assert!(json.stdout.is_empty());
    let json_error: serde_json::Value =
        serde_json::from_slice(&json.stderr).expect("stderr should contain one JSON document");
    let message = json_error["error"]["message"]
        .as_str()
        .expect("usage error should include a message");
    assert!(message.contains("[REDACTED]"), "{message}");
    assert!(!message.contains("password"), "{message}");
    assert!(!message.contains("access-token-value"), "{message}");
    assert!(!message.contains("query-secret-value"), "{message}");
}
