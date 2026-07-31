use hopash::contract::{ErrorCode, ProcessExitCode};

#[test]
fn public_error_codes_map_to_stable_process_exit_classes() {
    let cases = [
        (ErrorCode::Usage, ProcessExitCode::Usage),
        (
            ErrorCode::SupervisorUnavailable,
            ProcessExitCode::SupervisorUnavailable,
        ),
        (
            ErrorCode::ProtocolMismatch,
            ProcessExitCode::SupervisorUnavailable,
        ),
        (ErrorCode::ProfileAmbiguous, ProcessExitCode::DomainConflict),
        (ErrorCode::ProfileActive, ProcessExitCode::DomainConflict),
        (
            ErrorCode::RulesUninitialized,
            ProcessExitCode::DomainConflict,
        ),
        (ErrorCode::RuleBusy, ProcessExitCode::DomainConflict),
        (ErrorCode::RuleNotFound, ProcessExitCode::DomainConflict),
        (ErrorCode::RuleAmbiguous, ProcessExitCode::DomainConflict),
        (
            ErrorCode::RuleAlreadyExists,
            ProcessExitCode::DomainConflict,
        ),
        (
            ErrorCode::ExternalOperationFailed,
            ProcessExitCode::ExternalOperationFailure,
        ),
        (ErrorCode::Internal, ProcessExitCode::InternalFailure),
    ];

    for (error, expected) in cases {
        assert_eq!(error.process_exit_code(), expected, "{error:?}");
    }

    assert_eq!(ProcessExitCode::Success.as_u8(), 0);
    assert_eq!(ProcessExitCode::Usage.as_u8(), 2);
    assert_eq!(ProcessExitCode::SupervisorUnavailable.as_u8(), 3);
    assert_eq!(ProcessExitCode::DomainConflict.as_u8(), 4);
    assert_eq!(ProcessExitCode::ExternalOperationFailure.as_u8(), 5);
    assert_eq!(ProcessExitCode::InternalFailure.as_u8(), 70);
    assert_eq!(ProcessExitCode::Interrupted.as_u8(), 130);
}
