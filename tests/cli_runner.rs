use hopash::application::{
    ApplicationClient, ApplicationError, ApplicationErrorDetails, ApplicationOperation,
    ApplicationOutput, ApplicationService, ProfileListOutcome, ProfileRefreshState, ProfileSummary,
    SelectorCandidate, SelectorKind,
};
use hopash::cli::{Invocation, OutputMode, run_invocation};
use hopash::contract::ProcessExitCode;
use hopash::domain::{ProfileId, SubscriptionUrl};
use hopash::error::ErrorCode;
use std::cell::RefCell;

struct RecordingClient {
    calls: RefCell<Vec<ApplicationOperation>>,
    result: Result<ApplicationOutput, ApplicationError>,
}

impl ApplicationClient for RecordingClient {
    fn execute(
        &self,
        operation: ApplicationOperation,
    ) -> Result<ApplicationOutput, ApplicationError> {
        self.calls.borrow_mut().push(operation);
        self.result.clone()
    }
}

#[test]
fn json_status_invokes_one_use_case_and_writes_stdout_only() {
    let client = RecordingClient {
        calls: RefCell::new(Vec::new()),
        result: Ok(ApplicationOutput::Status(
            ApplicationService::new().status(),
        )),
    };
    let invocation = Invocation::Application {
        operation: ApplicationOperation::GetStatus,
        output: OutputMode::Json,
    };
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();

    let exit = run_invocation(invocation, &client, &mut stdout, &mut stderr);

    assert_eq!(exit, ProcessExitCode::Success);
    assert_eq!(
        client.calls.into_inner(),
        vec![ApplicationOperation::GetStatus]
    );
    assert!(stderr.is_empty());
    let value: serde_json::Value =
        serde_json::from_slice(&stdout).expect("stdout should contain one JSON document");
    assert_eq!(value["schema_version"], 1);
    assert_eq!(value["data"]["supervisor"]["lifecycle"], "ready");
    assert_eq!(value["data"]["core"]["lifecycle"], "unconfigured");
}

#[test]
fn json_application_error_writes_stderr_and_uses_the_error_registry() {
    let client = RecordingClient {
        calls: RefCell::new(Vec::new()),
        result: Err(ApplicationError::new(
            ErrorCode::RuleBusy,
            "Another configuration change is in progress",
            true,
        )),
    };
    let invocation = Invocation::Application {
        operation: ApplicationOperation::RuleList,
        output: OutputMode::Json,
    };
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();

    let exit = run_invocation(invocation, &client, &mut stdout, &mut stderr);

    assert_eq!(exit, ProcessExitCode::DomainConflict);
    assert!(stdout.is_empty());
    let value: serde_json::Value =
        serde_json::from_slice(&stderr).expect("stderr should contain one JSON document");
    assert_eq!(value["error"]["code"], "rule_busy");
    assert_eq!(value["error"]["retryable"], true);
}

#[test]
fn application_error_details_reach_the_json_contract() {
    let client = RecordingClient {
        calls: RefCell::new(Vec::new()),
        result: Err(ApplicationError::new(
            ErrorCode::ProfileAmbiguous,
            "Profile name is ambiguous",
            false,
        )
        .with_details(ApplicationErrorDetails::CandidateIds {
            candidate_ids: vec!["profile-a".to_owned(), "profile-b".to_owned()],
        })),
    };
    let invocation = Invocation::Application {
        operation: ApplicationOperation::ProfileUse {
            profile: "Shared Name".to_owned(),
        },
        output: OutputMode::Json,
    };
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();

    let exit = run_invocation(invocation, &client, &mut stdout, &mut stderr);

    assert_eq!(exit, ProcessExitCode::DomainConflict);
    assert!(stdout.is_empty());
    let value: serde_json::Value =
        serde_json::from_slice(&stderr).expect("stderr should contain one JSON document");
    assert_eq!(
        value["error"]["details"],
        serde_json::json!({ "candidate_ids": ["profile-a", "profile-b"] })
    );
}

#[test]
fn profile_candidates_reach_human_diagnostics() {
    let client = RecordingClient {
        calls: RefCell::new(Vec::new()),
        result: Err(ApplicationError::new(
            ErrorCode::ProfileAmbiguous,
            "Profile name is ambiguous",
            false,
        )
        .with_details(ApplicationErrorDetails::CandidateIds {
            candidate_ids: vec!["profile-a".to_owned(), "profile-b".to_owned()],
        })),
    };
    let invocation = Invocation::Application {
        operation: ApplicationOperation::ProfileUse {
            profile: "Shared Name".to_owned(),
        },
        output: OutputMode::Human,
    };
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();

    let exit = run_invocation(invocation, &client, &mut stdout, &mut stderr);

    assert_eq!(exit, ProcessExitCode::DomainConflict);
    assert!(stdout.is_empty());
    let diagnostic = String::from_utf8(stderr).expect("diagnostic should be UTF-8");
    assert!(diagnostic.contains("Profile name is ambiguous"));
    assert!(diagnostic.contains("Candidate profile IDs:"));
    assert!(diagnostic.contains("profile-a"));
    assert!(diagnostic.contains("profile-b"));
}

#[test]
fn typed_selector_candidates_reach_json_and_human_diagnostics() {
    let error = ApplicationError::new(
        ErrorCode::ProfileAmbiguous,
        "Profile name is ambiguous",
        false,
    )
    .with_selector_candidates(
        SelectorKind::Profile,
        vec![
            SelectorCandidate::new("profile-a", "Shared"),
            SelectorCandidate::new("profile-b", "Shared"),
        ],
    );

    let json_client = RecordingClient {
        calls: RefCell::new(Vec::new()),
        result: Err(error.clone()),
    };
    let mut json_stdout = Vec::new();
    let mut json_stderr = Vec::new();
    let json_exit = run_invocation(
        Invocation::Application {
            operation: ApplicationOperation::ProfileUse {
                profile: "Shared".to_owned(),
            },
            output: OutputMode::Json,
        },
        &json_client,
        &mut json_stdout,
        &mut json_stderr,
    );

    assert_eq!(json_exit, ProcessExitCode::DomainConflict);
    assert!(json_stdout.is_empty());
    let value: serde_json::Value =
        serde_json::from_slice(&json_stderr).expect("stderr should contain one JSON document");
    assert_eq!(value["error"]["details"]["selector"], "profile");
    assert_eq!(
        value["error"]["details"]["candidate_ids"],
        serde_json::json!(["profile-a", "profile-b"])
    );
    assert_eq!(
        value["error"]["details"]["candidates"][0],
        serde_json::json!({"id": "profile-a", "name": "Shared"})
    );

    let human_client = RecordingClient {
        calls: RefCell::new(Vec::new()),
        result: Err(error),
    };
    let mut human_stdout = Vec::new();
    let mut human_stderr = Vec::new();
    let human_exit = run_invocation(
        Invocation::Application {
            operation: ApplicationOperation::ProfileUse {
                profile: "Shared".to_owned(),
            },
            output: OutputMode::Human,
        },
        &human_client,
        &mut human_stdout,
        &mut human_stderr,
    );

    assert_eq!(human_exit, ProcessExitCode::DomainConflict);
    assert!(human_stdout.is_empty());
    let diagnostic = String::from_utf8(human_stderr).expect("diagnostic should be UTF-8");
    assert!(diagnostic.contains("Profile candidates:"));
    assert!(diagnostic.contains("Shared (profile-a)"));
}

#[test]
fn human_profile_output_redacts_urls_and_escapes_terminal_controls() {
    let client = RecordingClient {
        calls: RefCell::new(Vec::new()),
        result: Ok(ApplicationOutput::Profiles(ProfileListOutcome {
            profiles: vec![ProfileSummary {
                id: ProfileId::parse("550e8400-e29b-41d4-a716-446655440000")
                    .expect("fixture Profile ID should parse"),
                name: "Primary\u{1b}[31m\nInjected".to_owned(),
                subscription_url: SubscriptionUrl::parse(
                    "https://user:password@example.com/subscription-secret.yaml?token=value",
                )
                .expect("fixture Subscription URL should parse"),
                active: true,
                refresh_state: ProfileRefreshState::Fresh,
                last_success_at_unix_ms: 1_700_000_000_000,
                next_refresh_at_unix_ms: 1_700_021_600_000,
                last_error: None,
            }],
        })),
    };
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();

    let exit = run_invocation(
        Invocation::Application {
            operation: ApplicationOperation::ProfileList,
            output: OutputMode::Human,
        },
        &client,
        &mut stdout,
        &mut stderr,
    );

    assert_eq!(exit, ProcessExitCode::Success);
    assert!(stderr.is_empty());
    let output = String::from_utf8(stdout).expect("output should be UTF-8");
    assert!(!output.contains('\u{1b}'));
    assert!(output.contains("Primary\\u{1b}[31m\\nInjected"));
    assert!(!output.contains("subscription-secret"));
    assert!(!output.contains("password"));
    assert!(output.contains("[redacted]"));
}
