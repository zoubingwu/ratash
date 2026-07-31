use super::{Cli, Invocation};
use crate::contract::{ApiError, JsonEnvelope};
use crate::error::{ErrorCode, ProcessExitCode};
use clap::{Parser, error::ErrorKind};
use std::ffi::{OsStr, OsString};
use std::io::Write;

pub fn parse_process_invocation(
    args: &[OsString],
    stderr: &mut dyn Write,
) -> Result<Invocation, ProcessExitCode> {
    let wants_json = args.iter().any(|argument| argument == "--json");
    match Cli::try_parse_from(args) {
        Ok(cli) => Ok(cli.into_invocation()),
        Err(error) if error.exit_code() != 0 => {
            write_usage_error(&error, args, wants_json, stderr);
            Err(ProcessExitCode::Usage)
        }
        Err(error) => error.exit(),
    }
}

fn write_usage_error(
    error: &clap::Error,
    args: &[OsString],
    wants_json: bool,
    stderr: &mut dyn Write,
) {
    let message = usage_error_message(error, args);
    if wants_json {
        let envelope = JsonEnvelope::<serde_json::Value>::failure(ApiError::new(
            ErrorCode::Usage,
            message,
            false,
        ));
        if serde_json::to_writer(&mut *stderr, &envelope).is_ok() {
            let _ = writeln!(stderr);
        }
    } else {
        let _ = writeln!(stderr, "{message}");
    }
}

fn usage_error_message(error: &clap::Error, args: &[OsString]) -> String {
    let mut message = error.to_string();
    if let Some(subscription_url) = subscription_url_argument(args) {
        message = message.replace(subscription_url.to_string_lossy().as_ref(), "[REDACTED]");
    }
    let message = message.trim();

    if error.kind() == ErrorKind::MissingRequiredArgument && is_rule_add(args) {
        return format!(
            "A rule placement is required. Choose one of --prepend, --append, --before, or \
             --after.\n\n{message}"
        );
    }

    message.to_owned()
}

fn is_rule_add(args: &[OsString]) -> bool {
    args.get(1)
        .is_some_and(|argument| argument == OsStr::new("rule"))
        && args
            .get(2)
            .is_some_and(|argument| argument == OsStr::new("add"))
}

fn subscription_url_argument(args: &[OsString]) -> Option<&OsStr> {
    let is_profile_add = args
        .get(1)
        .is_some_and(|argument| argument == OsStr::new("profile"))
        && args
            .get(2)
            .is_some_and(|argument| argument == OsStr::new("add"));

    is_profile_add
        .then(|| {
            args.iter()
                .skip(3)
                .find(|argument| argument != &OsStr::new("--json"))
                .map(OsString::as_os_str)
        })
        .flatten()
}
