use hopash::application::{
    ApplicationClient, ApplicationError, ApplicationOperation, ApplicationOutput,
};
use hopash::cli::{parse_process_invocation, run_invocation};
use hopash::error::ErrorCode;
use std::io;
use std::process::ExitCode;

struct UnavailableClient;

impl ApplicationClient for UnavailableClient {
    fn execute(
        &self,
        _operation: ApplicationOperation,
    ) -> Result<ApplicationOutput, ApplicationError> {
        Err(ApplicationError::new(
            ErrorCode::SupervisorUnavailable,
            "The Hopash Supervisor is unavailable",
            true,
        ))
    }
}

fn main() -> ExitCode {
    let args = std::env::args_os().collect::<Vec<_>>();
    let stderr = io::stderr();
    let invocation = match parse_process_invocation(&args, &mut stderr.lock()) {
        Ok(invocation) => invocation,
        Err(exit) => return ExitCode::from(exit.as_u8()),
    };
    let client = UnavailableClient;
    let stdout = io::stdout();
    let exit = run_invocation(invocation, &client, &mut stdout.lock(), &mut stderr.lock());
    ExitCode::from(exit.as_u8())
}
