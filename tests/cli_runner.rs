use hopash::application::{
    ApplicationClient, ApplicationError, ApplicationErrorDetails, ApplicationOperation,
    ApplicationOutput, ApplicationService, LifecycleFailureDetails, ProfileListOutcome,
    ProfileRefreshState, ProfileSummary, SelectorCandidate, SelectorKind,
};
use hopash::cli::{
    ForegroundRunner, Invocation, OutputMode, run_invocation, run_invocation_with_frontend,
};
use hopash::contract::ProcessExitCode;
use hopash::domain::{ProfileId, SubscriptionUrl};
use hopash::error::ErrorCode;
use std::cell::RefCell;
use std::io::Write;

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

#[derive(Default)]
struct RecordingForeground {
    status_calls: RefCell<usize>,
    log_formats: RefCell<Vec<OutputMode>>,
}

impl ForegroundRunner for RecordingForeground {
    fn run_status_interface(&self, stderr: &mut dyn Write) -> ProcessExitCode {
        *self.status_calls.borrow_mut() += 1;
        writeln!(stderr, "status interface").expect("fixture output should succeed");
        ProcessExitCode::Success
    }

    fn follow_logs(
        &self,
        output: OutputMode,
        stdout: &mut dyn Write,
        _stderr: &mut dyn Write,
    ) -> ProcessExitCode {
        self.log_formats.borrow_mut().push(output);
        writeln!(stdout, "log stream").expect("fixture output should succeed");
        ProcessExitCode::Interrupted
    }
}

#[test]
fn foreground_status_and_logs_are_dispatched_to_the_injected_runner() {
    let client = RecordingClient {
        calls: RefCell::new(Vec::new()),
        result: Ok(ApplicationOutput::Status(
            ApplicationService::new().status(),
        )),
    };
    let foreground = RecordingForeground::default();
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();

    let status_exit = run_invocation_with_frontend(
        Invocation::LaunchStatusInterface,
        &client,
        &foreground,
        &mut stdout,
        &mut stderr,
    );
    assert_eq!(status_exit, ProcessExitCode::Success);
    assert_eq!(*foreground.status_calls.borrow(), 1);
    assert_eq!(stderr, b"status interface\n");

    stderr.clear();
    let logs_exit = run_invocation_with_frontend(
        Invocation::FollowLogs {
            output: OutputMode::Json,
        },
        &client,
        &foreground,
        &mut stdout,
        &mut stderr,
    );
    assert_eq!(logs_exit, ProcessExitCode::Interrupted);
    assert_eq!(foreground.log_formats.into_inner(), vec![OutputMode::Json]);
    assert_eq!(stdout, b"log stream\n");
    assert!(client.calls.into_inner().is_empty());
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
fn runtime_apply_failures_expose_stage_generations_and_recovery() {
    let error = ApplicationError::new(
        ErrorCode::ExternalOperationFailed,
        "Runtime Apply failed and the committed configuration was retained",
        false,
    )
    .with_details(ApplicationErrorDetails::RuntimeApplyFailure(Box::new(
        hopash::application::RuntimeApplyFailureDetails {
            candidate_generation: Some(hopash::domain::RuntimeGeneration(12)),
            committed_generation: Some(hopash::domain::RuntimeGeneration(11)),
            stage: hopash::application::RuntimeApplyFailureStage::IndeterminateApply,
            recovery: hopash::application::RecoveryOutcome {
                status: hopash::application::RecoveryStatus::Succeeded,
                restored_generation: Some(hopash::domain::RuntimeGeneration(11)),
                message: Some("The committed Runtime Generation was confirmed".to_owned()),
            },
        },
    )));

    let json_client = RecordingClient {
        calls: RefCell::new(Vec::new()),
        result: Err(error.clone()),
    };
    let mut json_stdout = Vec::new();
    let mut json_stderr = Vec::new();
    let json_exit = run_invocation(
        Invocation::Application {
            operation: ApplicationOperation::RuleList,
            output: OutputMode::Json,
        },
        &json_client,
        &mut json_stdout,
        &mut json_stderr,
    );

    assert_eq!(json_exit, ProcessExitCode::ExternalOperationFailure);
    assert!(json_stdout.is_empty());
    let value: serde_json::Value =
        serde_json::from_slice(&json_stderr).expect("stderr should contain one JSON document");
    let details = &value["error"]["details"];
    assert_eq!(details["stage"], "indeterminate_apply");
    assert_eq!(details["candidate_generation"], "12");
    assert_eq!(details["committed_generation"], "11");
    assert_eq!(details["recovery"]["status"], "succeeded");
    assert_eq!(details["recovery"]["restored_generation"], "11");

    let human_client = RecordingClient {
        calls: RefCell::new(Vec::new()),
        result: Err(error),
    };
    let mut human_stdout = Vec::new();
    let mut human_stderr = Vec::new();
    let human_exit = run_invocation(
        Invocation::Application {
            operation: ApplicationOperation::RuleList,
            output: OutputMode::Human,
        },
        &human_client,
        &mut human_stdout,
        &mut human_stderr,
    );

    assert_eq!(human_exit, ProcessExitCode::ExternalOperationFailure);
    assert!(human_stdout.is_empty());
    let diagnostic = String::from_utf8(human_stderr).expect("diagnostic should be UTF-8");
    assert!(diagnostic.contains("Runtime Apply Stage: indeterminate_apply"));
    assert!(diagnostic.contains("Candidate Generation: 12"));
    assert!(diagnostic.contains("Committed Generation: 11"));
    assert!(diagnostic.contains("Recovery: succeeded"));
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

#[test]
fn lifecycle_failures_expose_stable_stage_and_category_details() {
    let error = ApplicationError::new(
        ErrorCode::CoreUnavailable,
        "The Supervisor reported a startup failure",
        true,
    )
    .with_details(ApplicationErrorDetails::LifecycleFailure(Box::new(
        LifecycleFailureDetails {
            stage: "core_readiness".to_owned(),
            category: "readiness".to_owned(),
        },
    )));
    let json_client = RecordingClient {
        calls: RefCell::new(Vec::new()),
        result: Err(error.clone()),
    };
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();

    let exit = run_invocation(
        Invocation::Application {
            operation: ApplicationOperation::Start,
            output: OutputMode::Json,
        },
        &json_client,
        &mut stdout,
        &mut stderr,
    );

    assert_eq!(exit, ProcessExitCode::ExternalOperationFailure);
    let value: serde_json::Value =
        serde_json::from_slice(&stderr).expect("stderr should contain one JSON document");
    assert_eq!(
        value["error"]["details"]["lifecycle_stage"],
        "core_readiness"
    );
    assert_eq!(value["error"]["details"]["failure_category"], "readiness");

    let human_client = RecordingClient {
        calls: RefCell::new(Vec::new()),
        result: Err(error),
    };
    stdout.clear();
    stderr.clear();
    let exit = run_invocation(
        Invocation::Application {
            operation: ApplicationOperation::Start,
            output: OutputMode::Human,
        },
        &human_client,
        &mut stdout,
        &mut stderr,
    );
    assert_eq!(exit, ProcessExitCode::ExternalOperationFailure);
    let diagnostic = String::from_utf8(stderr).expect("diagnostic should be UTF-8");
    assert!(diagnostic.contains("Lifecycle Stage: core_readiness"));
    assert!(diagnostic.contains("Failure Category: readiness"));
}
