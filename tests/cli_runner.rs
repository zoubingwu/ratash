use hopash::application::{
    ApplicationClient, ApplicationError, ApplicationErrorDetails, ApplicationOperation,
    ApplicationOutput, ApplicationService,
};
use hopash::cli::{Invocation, OutputMode, run_invocation};
use hopash::contract::ProcessExitCode;
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
