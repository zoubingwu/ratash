use hopash::cli::parse_process_invocation;
use hopash::daemon::InternalSupervisorInvocation;
use hopash::error::ProcessExitCode;
use hopash::production::{
    CoreServiceInvocation, run_core_service, run_internal_supervisor, run_public_invocation,
};
use std::io::{self, Write};
use std::process::ExitCode;

fn main() -> ExitCode {
    let args = std::env::args_os().collect::<Vec<_>>();
    let stderr = io::stderr();
    if let Some(exit) = run_internal_mode(&args, &mut stderr.lock()) {
        return ExitCode::from(exit.as_u8());
    }
    let invocation = match parse_process_invocation(&args, &mut stderr.lock()) {
        Ok(invocation) => invocation,
        Err(exit) => return ExitCode::from(exit.as_u8()),
    };
    let stdout = io::stdout();
    let exit = run_public_invocation(invocation, &mut stdout.lock(), &mut stderr.lock());
    ExitCode::from(exit.as_u8())
}

fn run_internal_mode(
    args: &[std::ffi::OsString],
    stderr: &mut dyn Write,
) -> Option<ProcessExitCode> {
    match CoreServiceInvocation::parse_process_arguments(args) {
        Ok(Some(invocation)) => {
            return Some(match run_core_service(invocation) {
                Ok(()) => ProcessExitCode::Success,
                Err(_) => {
                    let _ = writeln!(stderr, "The privileged Core service stopped with an error");
                    ProcessExitCode::InternalFailure
                }
            });
        }
        Ok(None) => {}
        Err(_) => {
            let _ = writeln!(stderr, "The internal Core service invocation is invalid");
            return Some(ProcessExitCode::Usage);
        }
    }
    match InternalSupervisorInvocation::parse_process_arguments(args) {
        Ok(Some(invocation)) => Some(match run_internal_supervisor(invocation) {
            Ok(()) => ProcessExitCode::Success,
            Err(_) => ProcessExitCode::InternalFailure,
        }),
        Ok(None) => None,
        Err(_) => {
            let _ = writeln!(stderr, "The internal Supervisor invocation is invalid");
            Some(ProcessExitCode::Usage)
        }
    }
}
