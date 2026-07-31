use serde::Serialize;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    Usage,
    SupervisorUnavailable,
    ProtocolMismatch,
    ProfileAmbiguous,
    ProfileActive,
    ProfileNotFound,
    ProxyGroupNotFound,
    NodeNotFound,
    NodeAmbiguous,
    InvalidSubscriptionUrl,
    RulesUninitialized,
    RuleBusy,
    RuleNotFound,
    RuleAmbiguous,
    RuleAlreadyExists,
    PolicyTargetNotFound,
    ProfileFieldUnsupported,
    TunPermissionDenied,
    CoreUnavailable,
    ExternalOperationFailed,
    Internal,
    OperationUnavailable,
}

impl ErrorCode {
    #[must_use]
    pub const fn process_exit_code(self) -> ProcessExitCode {
        match self {
            Self::Usage | Self::InvalidSubscriptionUrl => ProcessExitCode::Usage,
            Self::SupervisorUnavailable | Self::ProtocolMismatch => {
                ProcessExitCode::SupervisorUnavailable
            }
            Self::ProfileAmbiguous
            | Self::ProfileActive
            | Self::ProfileNotFound
            | Self::ProxyGroupNotFound
            | Self::NodeNotFound
            | Self::NodeAmbiguous
            | Self::RulesUninitialized
            | Self::RuleBusy
            | Self::RuleNotFound
            | Self::RuleAmbiguous
            | Self::RuleAlreadyExists
            | Self::PolicyTargetNotFound
            | Self::ProfileFieldUnsupported => ProcessExitCode::DomainConflict,
            Self::TunPermissionDenied
            | Self::CoreUnavailable
            | Self::ExternalOperationFailed
            | Self::OperationUnavailable => ProcessExitCode::ExternalOperationFailure,
            Self::Internal => ProcessExitCode::InternalFailure,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum ProcessExitCode {
    Success = 0,
    Usage = 2,
    SupervisorUnavailable = 3,
    DomainConflict = 4,
    ExternalOperationFailure = 5,
    InternalFailure = 70,
    Interrupted = 130,
}

impl ProcessExitCode {
    #[must_use]
    pub const fn as_u8(self) -> u8 {
        self as u8
    }
}
