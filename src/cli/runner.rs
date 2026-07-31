use super::{Cli, Invocation, OutputMode, render_agent_help};
use crate::application::{
    ApplicationClient, ApplicationError, ApplicationErrorDetails, ApplicationOutput,
};
use crate::contract::{ApiError, JsonEnvelope, StatusViewV1};
use crate::error::{ErrorCode, ProcessExitCode};
use clap::CommandFactory;
use std::io::Write;

pub fn run_invocation(
    invocation: Invocation,
    client: &dyn ApplicationClient,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> ProcessExitCode {
    match invocation {
        Invocation::Application { operation, output } => match client.execute(operation) {
            Ok(result) => write_application_output(result, output, stdout),
            Err(error) => write_application_error(error, output, stderr),
        },
        Invocation::PrintGeneralHelp => {
            let mut command = Cli::command();
            let help = command.render_long_help();
            if writeln!(stdout, "{help}").is_err() {
                ProcessExitCode::InternalFailure
            } else {
                ProcessExitCode::Success
            }
        }
        Invocation::PrintAgentHelp => {
            if write!(stdout, "{}", render_agent_help()).is_err() {
                ProcessExitCode::InternalFailure
            } else {
                ProcessExitCode::Success
            }
        }
        Invocation::LaunchStatusInterface => {
            write_application_error(supervisor_unavailable(), OutputMode::Human, stderr)
        }
        Invocation::FollowLogs { output } => {
            write_application_error(supervisor_unavailable(), output, stderr)
        }
    }
}

fn supervisor_unavailable() -> ApplicationError {
    ApplicationError::new(
        ErrorCode::SupervisorUnavailable,
        "The Hopash Supervisor is unavailable",
        true,
    )
}

fn write_application_output(
    output: ApplicationOutput,
    mode: OutputMode,
    stdout: &mut dyn Write,
) -> ProcessExitCode {
    match (output, mode) {
        (ApplicationOutput::Status(status), OutputMode::Json) => {
            let envelope = JsonEnvelope::success(StatusViewV1::from(status));
            if serde_json::to_writer(&mut *stdout, &envelope).is_err() || writeln!(stdout).is_err()
            {
                ProcessExitCode::InternalFailure
            } else {
                ProcessExitCode::Success
            }
        }
        (ApplicationOutput::Status(status), OutputMode::Human) => {
            if writeln!(
                stdout,
                "Supervisor: {:?}\nCore: {:?}\nUptime: {}s",
                status.supervisor.lifecycle,
                status.core.lifecycle,
                status.supervisor.uptime_seconds
            )
            .is_err()
            {
                ProcessExitCode::InternalFailure
            } else {
                ProcessExitCode::Success
            }
        }
    }
}

fn write_application_error(
    error: ApplicationError,
    mode: OutputMode,
    stderr: &mut dyn Write,
) -> ProcessExitCode {
    let exit = error.code.process_exit_code();
    match mode {
        OutputMode::Human => write_human_application_error(error, exit, stderr),
        OutputMode::Json => {
            let envelope = JsonEnvelope::<serde_json::Value>::failure(ApiError::from(error));
            if serde_json::to_writer(&mut *stderr, &envelope).is_err() || writeln!(stderr).is_err()
            {
                ProcessExitCode::InternalFailure
            } else {
                exit
            }
        }
    }
}

fn write_human_application_error(
    error: ApplicationError,
    exit: ProcessExitCode,
    stderr: &mut dyn Write,
) -> ProcessExitCode {
    if writeln!(stderr, "{}", error.message).is_err() {
        return ProcessExitCode::InternalFailure;
    }

    if let Some(ApplicationErrorDetails::CandidateIds { candidate_ids }) = error.details {
        if writeln!(stderr, "Candidate profile IDs:").is_err() {
            return ProcessExitCode::InternalFailure;
        }
        for candidate_id in candidate_ids {
            if writeln!(stderr, "- {candidate_id}").is_err() {
                return ProcessExitCode::InternalFailure;
            }
        }
    }

    exit
}
