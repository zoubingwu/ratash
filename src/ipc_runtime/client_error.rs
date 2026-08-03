//! Client-side transport and protocol error translation.

use std::io;
use std::time::Duration;

use mio::net::UnixStream as MioUnixStream;
use mio::{Events, Poll};

use crate::application::{
    ApplicationError, ApplicationErrorDetails, ApplicationOperation, RecoveryOutcome,
    RecoveryStatus, RuntimeApplyFailureDetails, RuntimeApplyFailureStage, SelectorCandidate,
    SelectorKind,
};
use crate::cancellation::CancellationToken;
use crate::domain::RuntimeGeneration;
use crate::error::ErrorCode;
use crate::ipc::IpcError;

pub(super) fn connect_error(_error: io::Error) -> ApplicationError {
    ApplicationError::new(
        ErrorCode::SupervisorUnavailable,
        "The Ratash Supervisor IPC endpoint is unavailable",
        true,
    )
}

pub(super) fn write_error(error: crate::ipc::FrameError) -> ApplicationError {
    match error {
        crate::ipc::FrameError::Io(error) if is_timeout(&error) => ApplicationError::new(
            ErrorCode::SupervisorUnavailable,
            "Timed out sending the Supervisor IPC request",
            true,
        ),
        crate::ipc::FrameError::Io(_) => ApplicationError::new(
            ErrorCode::SupervisorUnavailable,
            "The Supervisor IPC request could not be sent",
            true,
        ),
        crate::ipc::FrameError::Json(_) | crate::ipc::FrameError::FrameTooLarge { .. } => {
            ApplicationError::new(
                ErrorCode::Internal,
                "The application request could not be encoded",
                false,
            )
        }
    }
}

pub(super) fn operation_write_error(
    error: crate::ipc::FrameError,
    may_commit: bool,
) -> ApplicationError {
    if may_commit && matches!(&error, crate::ipc::FrameError::Io(_)) {
        return unknown_mutation_outcome(
            "The IPC mutation request was interrupted; query current state before retrying",
        );
    }
    write_error(error)
}

pub(super) fn operation_read_setup_error(error: io::Error, may_commit: bool) -> ApplicationError {
    if may_commit {
        unknown_mutation_outcome(
            "The IPC mutation response could not be read; query current state before retrying",
        )
    } else {
        connect_error(error)
    }
}

pub(super) fn operation_read_error(
    error: crate::ipc::FrameError,
    may_commit: bool,
) -> ApplicationError {
    if may_commit
        && matches!(
            &error,
            crate::ipc::FrameError::Io(error)
                if is_timeout(error)
                    || matches!(
                        error.kind(),
                        io::ErrorKind::UnexpectedEof
                            | io::ErrorKind::ConnectionReset
                            | io::ErrorKind::BrokenPipe
                    )
        )
    {
        return unknown_mutation_outcome(
            "The IPC mutation outcome is unknown; query current state before retrying",
        );
    }
    read_error(error)
}

fn unknown_mutation_outcome(message: &'static str) -> ApplicationError {
    ApplicationError::new(ErrorCode::ExternalOperationFailed, message, false)
}

pub(super) fn cancelled_operation(may_commit: bool) -> ApplicationError {
    if may_commit {
        unknown_mutation_outcome(
            "The IPC mutation wait was cancelled; query current state before retrying",
        )
    } else {
        ApplicationError::new(
            ErrorCode::OperationUnavailable,
            "The IPC operation was cancelled",
            true,
        )
    }
}

pub(super) fn operation_may_commit(operation: &ApplicationOperation) -> bool {
    matches!(
        operation,
        ApplicationOperation::Start
            | ApplicationOperation::Stop
            | ApplicationOperation::Restart
            | ApplicationOperation::ProfileAdd { .. }
            | ApplicationOperation::ProfileUse { .. }
            | ApplicationOperation::ProfileRemove { .. }
            | ApplicationOperation::ProxySelect { .. }
            | ApplicationOperation::RuleAdd { .. }
            | ApplicationOperation::RuleReplace { .. }
            | ApplicationOperation::RuleRemove { .. }
    )
}

pub(super) fn read_error(error: crate::ipc::FrameError) -> ApplicationError {
    match error {
        crate::ipc::FrameError::Io(error) if is_timeout(&error) => ApplicationError::new(
            ErrorCode::SupervisorUnavailable,
            "Timed out waiting for the Supervisor IPC response",
            true,
        ),
        crate::ipc::FrameError::Io(error)
            if matches!(
                error.kind(),
                io::ErrorKind::UnexpectedEof
                    | io::ErrorKind::ConnectionReset
                    | io::ErrorKind::BrokenPipe
            ) =>
        {
            ApplicationError::new(
                ErrorCode::SupervisorUnavailable,
                "The Supervisor IPC connection closed before responding",
                true,
            )
        }
        crate::ipc::FrameError::Io(_)
        | crate::ipc::FrameError::Json(_)
        | crate::ipc::FrameError::FrameTooLarge { .. } => {
            protocol_error("The Supervisor IPC response frame is invalid")
        }
    }
}

fn is_timeout(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock
    )
}

pub(super) fn ipc_connection_is_ready(stream: &MioUnixStream) -> io::Result<bool> {
    if let Some(error) = stream.take_error()? {
        return Err(error);
    }
    match stream.peer_addr() {
        Ok(_) => Ok(true),
        Err(error)
            if matches!(
                error.kind(),
                io::ErrorKind::NotConnected | io::ErrorKind::WouldBlock
            ) =>
        {
            Ok(false)
        }
        Err(error) => Err(error),
    }
}

pub(super) fn poll_for_ipc_connect(
    poll: &mut Poll,
    events: &mut Events,
    remaining: Duration,
    cancellation: &CancellationToken,
) -> io::Result<()> {
    events.clear();
    poll.poll(events, Some(remaining))?;
    if cancellation.is_cancelled() {
        return Err(io::Error::new(
            io::ErrorKind::Interrupted,
            "IPC connect was cancelled",
        ));
    }
    if events.is_empty() {
        Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "IPC connect timed out",
        ))
    } else {
        Ok(())
    }
}

pub(super) fn protocol_error(message: &'static str) -> ApplicationError {
    ApplicationError::new(ErrorCode::ProtocolMismatch, message, false)
}

pub(super) fn application_error(error: &IpcError) -> ApplicationError {
    let Some(code) = parse_error_code(&error.code) else {
        return protocol_error("The IPC response error code is unknown");
    };
    let mut result = ApplicationError::new(code, error.message.clone(), error.retryable);
    if let Some(details) = decode_runtime_apply_failure(error.details.as_ref()) {
        result = result.with_details(details);
    }
    if let Some(application_candidate_ids) = error
        .details
        .as_ref()
        .and_then(|details| details.get("application_candidate_ids"))
        .and_then(decode_candidate_ids)
    {
        result = result.with_details(ApplicationErrorDetails::CandidateIds {
            candidate_ids: application_candidate_ids,
        });
    }
    let selector_candidates = decode_selector_candidates(error.details.as_ref());
    if result.details.is_none()
        && selector_candidates.is_none()
        && let Some(candidate_ids) = error
            .details
            .as_ref()
            .and_then(|details| details.get("candidate_ids"))
            .and_then(decode_candidate_ids)
    {
        result = result.with_details(ApplicationErrorDetails::CandidateIds { candidate_ids });
    }
    if let Some((selector, candidates)) = selector_candidates {
        result = result.with_selector_candidates(selector, candidates);
    }
    result
}

fn decode_candidate_ids(value: &serde_json::Value) -> Option<Vec<String>> {
    value
        .as_array()?
        .iter()
        .map(|value| value.as_str().map(str::to_owned))
        .collect()
}

fn decode_runtime_apply_failure(
    details: Option<&serde_json::Value>,
) -> Option<ApplicationErrorDetails> {
    let details = details?.as_object()?;
    let stage = RuntimeApplyFailureStage::parse(details.get("stage")?.as_str()?)?;
    let candidate_generation = decode_optional_generation(details.get("candidate_generation"))?;
    let committed_generation = decode_optional_generation(details.get("committed_generation"))?;
    let recovery = details.get("recovery")?.as_object()?;
    let status = match recovery.get("status")?.as_str()? {
        "not_required" => RecoveryStatus::NotRequired,
        "succeeded" => RecoveryStatus::Succeeded,
        "pending" => RecoveryStatus::Pending,
        "failed" => RecoveryStatus::Failed,
        _ => return None,
    };
    let restored_generation = decode_optional_generation(recovery.get("restored_generation"))?;
    let message = match recovery.get("message") {
        None | Some(serde_json::Value::Null) => None,
        Some(message) => Some(message.as_str()?.to_owned()),
    };
    Some(ApplicationErrorDetails::RuntimeApplyFailure(Box::new(
        RuntimeApplyFailureDetails {
            candidate_generation,
            committed_generation,
            stage,
            recovery: RecoveryOutcome {
                status,
                restored_generation,
                message,
            },
        },
    )))
}

fn decode_optional_generation(
    value: Option<&serde_json::Value>,
) -> Option<Option<RuntimeGeneration>> {
    match value {
        None | Some(serde_json::Value::Null) => Some(None),
        Some(value) => value
            .as_str()?
            .parse::<u64>()
            .ok()
            .map(RuntimeGeneration)
            .map(Some),
    }
}

fn decode_selector_candidates(
    details: Option<&serde_json::Value>,
) -> Option<(SelectorKind, Vec<SelectorCandidate>)> {
    let details = details?.as_object()?;
    let selector = match details.get("selector")?.as_str()? {
        "profile" => SelectorKind::Profile,
        "proxy_group" => SelectorKind::ProxyGroup,
        "node" => SelectorKind::Node,
        "rule" => SelectorKind::Rule,
        _ => return None,
    };
    let candidates = details
        .get("candidates")?
        .as_array()?
        .iter()
        .map(|candidate| {
            let candidate = candidate.as_object()?;
            Some(SelectorCandidate::new(
                candidate.get("id")?.as_str()?,
                candidate.get("name")?.as_str()?,
            ))
        })
        .collect::<Option<Vec<_>>>()?;
    Some((selector, candidates))
}

fn parse_error_code(code: &str) -> Option<ErrorCode> {
    Some(match code {
        "usage" => ErrorCode::Usage,
        "supervisor_unavailable" => ErrorCode::SupervisorUnavailable,
        "protocol_mismatch" => ErrorCode::ProtocolMismatch,
        "profile_ambiguous" => ErrorCode::ProfileAmbiguous,
        "profile_active" => ErrorCode::ProfileActive,
        "profile_not_found" => ErrorCode::ProfileNotFound,
        "proxy_group_not_found" => ErrorCode::ProxyGroupNotFound,
        "node_not_found" => ErrorCode::NodeNotFound,
        "node_ambiguous" => ErrorCode::NodeAmbiguous,
        "invalid_subscription_url" => ErrorCode::InvalidSubscriptionUrl,
        "rules_uninitialized" => ErrorCode::RulesUninitialized,
        "rule_busy" => ErrorCode::RuleBusy,
        "rule_not_found" => ErrorCode::RuleNotFound,
        "rule_ambiguous" => ErrorCode::RuleAmbiguous,
        "rule_already_exists" => ErrorCode::RuleAlreadyExists,
        "policy_target_not_found" => ErrorCode::PolicyTargetNotFound,
        "profile_field_unsupported" => ErrorCode::ProfileFieldUnsupported,
        "tun_permission_denied" => ErrorCode::TunPermissionDenied,
        "tun_unsupported" => ErrorCode::TunUnsupported,
        "core_unavailable" => ErrorCode::CoreUnavailable,
        "external_operation_failed" => ErrorCode::ExternalOperationFailed,
        "internal" => ErrorCode::Internal,
        "operation_unavailable" => ErrorCode::OperationUnavailable,
        _ => return None,
    })
}
