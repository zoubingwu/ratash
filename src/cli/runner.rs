use super::{Cli, Invocation, OutputMode, render_agent_help};
use crate::application::{
    self, ApplicationClient, ApplicationError, ApplicationErrorDetails, ApplicationOutput,
};
use crate::constants::JSON_OUTPUT_MAX_BYTES;
use crate::contract::{ApiError, ApplicationOutputViewV1, JsonEnvelope};
use crate::error::{ErrorCode, ProcessExitCode};
use clap::CommandFactory;
use serde::Serialize;
use std::fmt::Write as _;
use std::io::{self, Write};

pub trait ForegroundRunner {
    fn run_status_interface(&self, stderr: &mut dyn Write) -> ProcessExitCode;

    fn follow_logs(
        &self,
        output: OutputMode,
        stdout: &mut dyn Write,
        stderr: &mut dyn Write,
    ) -> ProcessExitCode;
}

struct UnavailableForeground;

impl ForegroundRunner for UnavailableForeground {
    fn run_status_interface(&self, stderr: &mut dyn Write) -> ProcessExitCode {
        write_application_error(supervisor_unavailable(), OutputMode::Human, stderr)
    }

    fn follow_logs(
        &self,
        output: OutputMode,
        _stdout: &mut dyn Write,
        stderr: &mut dyn Write,
    ) -> ProcessExitCode {
        write_application_error(supervisor_unavailable(), output, stderr)
    }
}

pub fn run_invocation(
    invocation: Invocation,
    client: &dyn ApplicationClient,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> ProcessExitCode {
    run_invocation_with_frontend(invocation, client, &UnavailableForeground, stdout, stderr)
}

pub fn run_invocation_with_frontend(
    invocation: Invocation,
    client: &dyn ApplicationClient,
    foreground: &dyn ForegroundRunner,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> ProcessExitCode {
    match invocation {
        Invocation::Application { operation, output } => match client.execute(operation) {
            Ok(result) => write_application_output(result, output, stdout, stderr),
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
        Invocation::LaunchStatusInterface => foreground.run_status_interface(stderr),
        Invocation::FollowLogs { output } => foreground.follow_logs(output, stdout, stderr),
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
    stderr: &mut dyn Write,
) -> ProcessExitCode {
    match mode {
        OutputMode::Json => {
            let envelope = JsonEnvelope::success(ApplicationOutputViewV1::from(output));
            write_bounded_json_success(&envelope, stdout, stderr, JSON_OUTPUT_MAX_BYTES)
        }
        OutputMode::Human => {
            if write_human_application_output(output, stdout).is_err() {
                ProcessExitCode::InternalFailure
            } else {
                ProcessExitCode::Success
            }
        }
    }
}

fn write_bounded_json_success<T: Serialize>(
    value: &T,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
    max_bytes: usize,
) -> ProcessExitCode {
    match encode_json_line(value, max_bytes) {
        Ok(bytes) => {
            if stdout.write_all(&bytes).is_err() {
                ProcessExitCode::InternalFailure
            } else {
                ProcessExitCode::Success
            }
        }
        Err(JsonEncodingError::LimitExceeded) => write_application_error(
            ApplicationError::new(
                ErrorCode::ExternalOperationFailed,
                format!("JSON output exceeds the {max_bytes}-byte limit"),
                false,
            ),
            OutputMode::Json,
            stderr,
        ),
        Err(JsonEncodingError::Serialization) => write_application_error(
            ApplicationError::new(
                ErrorCode::Internal,
                "The JSON response could not be encoded",
                false,
            ),
            OutputMode::Json,
            stderr,
        ),
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum JsonEncodingError {
    LimitExceeded,
    Serialization,
}

fn encode_json_line<T: Serialize>(
    value: &T,
    max_bytes: usize,
) -> Result<Vec<u8>, JsonEncodingError> {
    let document_limit = max_bytes
        .checked_sub(1)
        .ok_or(JsonEncodingError::LimitExceeded)?;
    let mut writer = BoundedBuffer::new(document_limit);
    if serde_json::to_writer(&mut writer, value).is_err() {
        return if writer.exceeded {
            Err(JsonEncodingError::LimitExceeded)
        } else {
            Err(JsonEncodingError::Serialization)
        };
    }
    writer.bytes.push(b'\n');
    Ok(writer.bytes)
}

struct BoundedBuffer {
    bytes: Vec<u8>,
    limit: usize,
    exceeded: bool,
}

impl BoundedBuffer {
    fn new(limit: usize) -> Self {
        Self {
            bytes: Vec::with_capacity(limit.min(8 * 1024)),
            limit,
            exceeded: false,
        }
    }
}

impl Write for BoundedBuffer {
    fn write(&mut self, input: &[u8]) -> io::Result<usize> {
        let remaining = self.limit.saturating_sub(self.bytes.len());
        if input.len() > remaining {
            self.exceeded = true;
            return Err(io::Error::other("JSON output limit exceeded"));
        }
        self.bytes.extend_from_slice(input);
        Ok(input.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn write_human_application_output(
    output: ApplicationOutput,
    stdout: &mut dyn Write,
) -> io::Result<()> {
    match output {
        ApplicationOutput::Status(status) => write_status(&status, stdout),
        ApplicationOutput::Lifecycle(outcome) => {
            writeln!(stdout, "Action: {}", lifecycle_action(outcome.action))?;
            writeln!(stdout, "Changed: {}", yes_no(outcome.changed))?;
            write_status(&outcome.status, stdout)
        }
        ApplicationOutput::Profiles(outcome) => write_profiles(&outcome.profiles, stdout),
        ApplicationOutput::ProfileMutation(outcome) => {
            writeln!(
                stdout,
                "Profile {}: {} ({})",
                profile_mutation_action(outcome.action),
                terminal_safe(&outcome.profile.name),
                outcome.profile.id
            )?;
            writeln!(
                stdout,
                "Subscription URL: {}",
                terminal_safe(&outcome.profile.subscription_url.redacted())
            )?;
            if let Some(runtime_apply) = outcome.runtime_apply {
                write_runtime_apply(&runtime_apply, stdout)?;
            }
            Ok(())
        }
        ApplicationOutput::Proxies(outcome) => write_proxies(&outcome, stdout),
        ApplicationOutput::ProxySelection(outcome) => {
            writeln!(stdout, "Proxy Group: {}", terminal_safe(&outcome.group))?;
            writeln!(
                stdout,
                "Selected Node: {} ({})",
                terminal_safe(&outcome.selected_node.name),
                terminal_safe(&outcome.selected_node.id)
            )?;
            match outcome.previous_node {
                Some(previous) => writeln!(
                    stdout,
                    "Previous Node: {} ({})",
                    terminal_safe(&previous.name),
                    terminal_safe(&previous.id)
                )?,
                None => writeln!(stdout, "Previous Node: none")?,
            }
            writeln!(stdout, "Persisted: {}", yes_no(outcome.persisted))?;
            write_recovery(&outcome.recovery, stdout)
        }
        ApplicationOutput::Latencies(outcome) => {
            writeln!(stdout, "Latency Samples: {}", outcome.samples.len())?;
            for sample in &outcome.samples {
                write_latency(sample, stdout)?;
            }
            Ok(())
        }
        ApplicationOutput::Latency(outcome) => write_latency(&outcome.sample, stdout),
        ApplicationOutput::Rules(outcome) => write_rules(&outcome, stdout),
        ApplicationOutput::RuleMutation(outcome) => {
            writeln!(
                stdout,
                "Rule {}: {}",
                rule_mutation_action(outcome.action),
                terminal_safe(&outcome.changed_rule)
            )?;
            if let Some(previous_rule) = outcome.previous_rule {
                writeln!(stdout, "Previous Rule: {}", terminal_safe(&previous_rule))?;
            }
            match outcome.resulting_position {
                Some(position) => writeln!(stdout, "Resulting Position: {position}")?,
                None => writeln!(stdout, "Resulting Position: none")?,
            }
            write_runtime_apply(&outcome.runtime_apply, stdout)
        }
        ApplicationOutput::LogMetadata(metadata) => write_log_metadata(&metadata, stdout),
    }
}

fn write_status(status: &crate::domain::StatusSnapshot, output: &mut dyn Write) -> io::Result<()> {
    writeln!(
        output,
        "Supervisor: {}",
        supervisor_lifecycle(status.supervisor.lifecycle)
    )?;
    writeln!(output, "Core: {}", core_lifecycle(status.core.lifecycle))?;
    writeln!(output, "Uptime: {}s", status.supervisor.uptime_seconds)
}

fn write_profiles(
    profiles: &[application::ProfileSummary],
    output: &mut dyn Write,
) -> io::Result<()> {
    writeln!(output, "Profiles: {}", profiles.len())?;
    for profile in profiles {
        let marker = if profile.active { "*" } else { "-" };
        writeln!(
            output,
            "{marker} {} ({})",
            terminal_safe(&profile.name),
            profile.id
        )?;
        writeln!(
            output,
            "  Subscription URL: {}",
            terminal_safe(&profile.subscription_url.redacted())
        )?;
        writeln!(
            output,
            "  Refresh: {}",
            profile_refresh_state(profile.refresh_state)
        )?;
        writeln!(
            output,
            "  Last Success: {}",
            profile.last_success_at_unix_ms
        )?;
        writeln!(
            output,
            "  Next Refresh: {}",
            profile.next_refresh_at_unix_ms
        )?;
        if let Some(error) = &profile.last_error {
            writeln!(
                output,
                "  Last Error: {}: {}",
                profile_refresh_stage(error.stage),
                terminal_safe(&error.message)
            )?;
        }
    }
    Ok(())
}

fn write_proxies(
    outcome: &application::ProxyListOutcome,
    output: &mut dyn Write,
) -> io::Result<()> {
    writeln!(
        output,
        "Proxy Group: {}",
        terminal_safe(&outcome.group.name)
    )?;
    writeln!(output, "Type: {}", terminal_safe(&outcome.group.proxy_type))?;
    writeln!(output, "Selectable: {}", yes_no(outcome.group.selectable))?;
    match &outcome.group.selected_node {
        Some(selected) => writeln!(
            output,
            "Selected Node: {} ({})",
            terminal_safe(&selected.name),
            terminal_safe(&selected.id)
        )?,
        None => writeln!(output, "Selected Node: none")?,
    }
    writeln!(output, "Nodes: {}", outcome.nodes.len())?;
    for node in &outcome.nodes {
        let marker = if node.selected { "*" } else { "-" };
        let id = node
            .id
            .as_ref()
            .map_or_else(|| "none".to_owned(), |id| id.as_str().to_owned());
        let proxy_type = node.proxy_type.as_deref().unwrap_or("unknown");
        let delay = node
            .delay_ms
            .map_or_else(|| "none".to_owned(), |delay| format!("{delay}ms"));
        writeln!(
            output,
            "{marker} {} ({}) type={} member={} availability={} delay={} freshness={} probe={}",
            terminal_safe(&node.name),
            terminal_safe(&id),
            terminal_safe(proxy_type),
            proxy_member_kind(node.member_kind),
            proxy_availability(node.availability),
            delay,
            latency_freshness(node.freshness),
            latency_probe_status(node.probe_status)
        )?;
        if let Some(source) = &node.source {
            writeln!(output, "  Source: {}", proxy_node_source(source))?;
        }
        if !node.candidate_ids.is_empty() {
            writeln!(output, "  Candidate IDs:")?;
            for candidate_id in &node.candidate_ids {
                writeln!(output, "  - {}", candidate_id.as_str())?;
            }
        }
    }
    Ok(())
}

fn write_latency(sample: &application::LatencySummary, output: &mut dyn Write) -> io::Result<()> {
    let delay = sample
        .delay_ms
        .map_or_else(|| "none".to_owned(), |delay| format!("{delay}ms"));
    let sampled_at = sample
        .sampled_at_unix_ms
        .map_or_else(|| "none".to_owned(), |timestamp| timestamp.to_string());
    writeln!(
        output,
        "- {} ({}) delay={} sampled_at={} freshness={} probe={} generation={}",
        terminal_safe(&sample.node_name),
        sample.node_id.as_str(),
        delay,
        sampled_at,
        latency_freshness(sample.freshness),
        latency_probe_status(sample.probe_status),
        sample.probe_generation.0
    )
}

fn write_rules(outcome: &application::RuleListOutcome, output: &mut dyn Write) -> io::Result<()> {
    writeln!(output, "Initialized: {}", yes_no(outcome.initialized))?;
    match outcome.revision {
        Some(revision) => writeln!(output, "Revision: {}", revision.0)?,
        None => writeln!(output, "Revision: none")?,
    }
    writeln!(output, "Rules: {}", outcome.rules.len())?;
    for rule in &outcome.rules {
        writeln!(
            output,
            "- {}: {}",
            rule.index,
            terminal_safe(&rule.rule_string)
        )?;
        writeln!(
            output,
            "  Type: {} | Policy Target: {} | Validation: {}",
            terminal_safe(&rule.rule_type),
            terminal_safe(&rule.policy_target),
            policy_target_validation(rule.policy_target_validation)
        )?;
        if let Some(payload) = &rule.payload {
            writeln!(output, "  Payload: {}", terminal_safe(payload))?;
        }
        if !rule.params.is_empty() {
            let params = rule
                .params
                .iter()
                .map(|param| terminal_safe(param))
                .collect::<Vec<_>>()
                .join(", ");
            writeln!(output, "  Params: {params}")?;
        }
    }
    Ok(())
}

fn write_runtime_apply(
    outcome: &application::RuntimeApplyOutcome,
    output: &mut dyn Write,
) -> io::Result<()> {
    writeln!(
        output,
        "Runtime Apply: {}",
        runtime_apply_status(outcome.status)
    )?;
    match outcome.candidate_generation {
        Some(generation) => writeln!(output, "Candidate Generation: {}", generation.0)?,
        None => writeln!(output, "Candidate Generation: none")?,
    }
    match outcome.committed_generation {
        Some(generation) => writeln!(output, "Committed Generation: {}", generation.0)?,
        None => writeln!(output, "Committed Generation: none")?,
    }
    write_recovery(&outcome.recovery, output)
}

fn write_recovery(
    outcome: &application::RecoveryOutcome,
    output: &mut dyn Write,
) -> io::Result<()> {
    writeln!(output, "Recovery: {}", recovery_status(outcome.status))?;
    if let Some(generation) = outcome.restored_generation {
        writeln!(output, "Restored Generation: {}", generation.0)?;
    }
    if let Some(message) = &outcome.message {
        writeln!(output, "Recovery Message: {}", terminal_safe(message))?;
    }
    Ok(())
}

fn write_log_metadata(
    metadata: &application::LogMetadata,
    output: &mut dyn Write,
) -> io::Result<()> {
    writeln!(
        output,
        "First Sequence: {}",
        optional_sequence(metadata.first_sequence)
    )?;
    writeln!(
        output,
        "Last Sequence: {}",
        optional_sequence(metadata.last_sequence)
    )?;
    writeln!(
        output,
        "Next Sequence: {}",
        optional_sequence(metadata.next_sequence)
    )?;
    writeln!(output, "Dropped Total: {}", metadata.dropped_total)?;
    if let Some(gap) = &metadata.gap {
        writeln!(
            output,
            "Gap: requested_after={} first_available={} dropped={}",
            gap.requested_after_sequence, gap.first_available_sequence, gap.dropped_count
        )?;
    } else {
        writeln!(output, "Gap: none")?;
    }
    Ok(())
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
    if writeln!(stderr, "{}", terminal_safe(&error.message)).is_err() {
        return ProcessExitCode::InternalFailure;
    }

    if let Some(selector_details) = error.selector_candidates {
        if writeln!(
            stderr,
            "{} candidates:",
            selector_kind(selector_details.selector)
        )
        .is_err()
        {
            return ProcessExitCode::InternalFailure;
        }
        for candidate in selector_details.candidates {
            if writeln!(
                stderr,
                "- {} ({})",
                terminal_safe(&candidate.name),
                terminal_safe(&candidate.id)
            )
            .is_err()
            {
                return ProcessExitCode::InternalFailure;
            }
        }
    } else if let Some(details) = error.details {
        match details {
            ApplicationErrorDetails::CandidateIds { candidate_ids } => {
                if writeln!(stderr, "Candidate profile IDs:").is_err() {
                    return ProcessExitCode::InternalFailure;
                }
                for candidate_id in candidate_ids {
                    if writeln!(stderr, "- {}", terminal_safe(&candidate_id)).is_err() {
                        return ProcessExitCode::InternalFailure;
                    }
                }
            }
            ApplicationErrorDetails::RuntimeApplyFailure(details) => {
                if writeln!(stderr, "Runtime Apply Stage: {}", details.stage.as_str()).is_err()
                    || writeln!(
                        stderr,
                        "Candidate Generation: {}",
                        optional_generation(details.candidate_generation)
                    )
                    .is_err()
                    || writeln!(
                        stderr,
                        "Committed Generation: {}",
                        optional_generation(details.committed_generation)
                    )
                    .is_err()
                    || write_recovery(&details.recovery, stderr).is_err()
                {
                    return ProcessExitCode::InternalFailure;
                }
            }
        }
    }

    exit
}

fn optional_generation(generation: Option<crate::domain::RuntimeGeneration>) -> String {
    match generation {
        Some(generation) => generation.0.to_string(),
        None => "none".to_owned(),
    }
}

fn terminal_safe(value: &str) -> String {
    let mut safe = String::with_capacity(value.len());
    for character in value.chars() {
        match character {
            '\n' => safe.push_str("\\n"),
            '\r' => safe.push_str("\\r"),
            '\t' => safe.push_str("\\t"),
            character if character.is_control() || is_bidirectional_control(character) => {
                let _ = write!(safe, "\\u{{{:x}}}", character as u32);
            }
            character => safe.push(character),
        }
    }
    safe
}

fn is_bidirectional_control(character: char) -> bool {
    matches!(
        character,
        '\u{061c}'
            | '\u{200e}'
            | '\u{200f}'
            | '\u{202a}'..='\u{202e}'
            | '\u{2066}'..='\u{2069}'
    )
}

fn yes_no(value: bool) -> &'static str {
    if value { "yes" } else { "no" }
}

fn optional_sequence(sequence: Option<u64>) -> String {
    sequence.map_or_else(|| "none".to_owned(), |value| value.to_string())
}

fn lifecycle_action(action: application::LifecycleAction) -> &'static str {
    match action {
        application::LifecycleAction::Start => "start",
        application::LifecycleAction::Stop => "stop",
        application::LifecycleAction::Restart => "restart",
    }
}

fn supervisor_lifecycle(lifecycle: crate::domain::SupervisorLifecycle) -> &'static str {
    match lifecycle {
        crate::domain::SupervisorLifecycle::Starting => "starting",
        crate::domain::SupervisorLifecycle::Ready => "ready",
        crate::domain::SupervisorLifecycle::Stopping => "stopping",
        crate::domain::SupervisorLifecycle::Degraded => "degraded",
    }
}

fn core_lifecycle(lifecycle: crate::domain::CoreLifecycle) -> &'static str {
    match lifecycle {
        crate::domain::CoreLifecycle::Unconfigured => "unconfigured",
        crate::domain::CoreLifecycle::Stopped => "stopped",
        crate::domain::CoreLifecycle::Starting => "starting",
        crate::domain::CoreLifecycle::Ready => "ready",
        crate::domain::CoreLifecycle::Reloading => "reloading",
        crate::domain::CoreLifecycle::Stopping => "stopping",
        crate::domain::CoreLifecycle::Degraded => "degraded",
    }
}

fn profile_refresh_state(state: application::ProfileRefreshState) -> &'static str {
    match state {
        application::ProfileRefreshState::Fresh => "fresh",
        application::ProfileRefreshState::Error => "error",
    }
}

fn profile_refresh_stage(stage: application::ProfileRefreshStage) -> &'static str {
    match stage {
        application::ProfileRefreshStage::Download => "download",
        application::ProfileRefreshStage::Parse => "parse",
        application::ProfileRefreshStage::Validate => "validate",
        application::ProfileRefreshStage::Apply => "apply",
    }
}

fn profile_mutation_action(action: application::ProfileMutationAction) -> &'static str {
    match action {
        application::ProfileMutationAction::Added => "added",
        application::ProfileMutationAction::Activated => "activated",
        application::ProfileMutationAction::Removed => "removed",
    }
}

fn proxy_member_kind(kind: application::ProxyMemberKind) -> &'static str {
    match kind {
        application::ProxyMemberKind::Node => "node",
        application::ProxyMemberKind::Group => "group",
        application::ProxyMemberKind::Missing => "missing",
        application::ProxyMemberKind::Ambiguous => "ambiguous",
        application::ProxyMemberKind::ProviderUnavailable => "provider_unavailable",
    }
}

fn proxy_availability(availability: application::ProxyAvailability) -> &'static str {
    match availability {
        application::ProxyAvailability::Available => "available",
        application::ProxyAvailability::Unavailable => "unavailable",
    }
}

fn proxy_node_source(source: &application::ProxyNodeSource) -> String {
    match source {
        application::ProxyNodeSource::Core => "core".to_owned(),
        application::ProxyNodeSource::Provider { provider_name } => {
            format!("provider:{}", terminal_safe(provider_name))
        }
    }
}

fn latency_freshness(freshness: application::LatencyFreshness) -> &'static str {
    match freshness {
        application::LatencyFreshness::NotSampled => "not_sampled",
        application::LatencyFreshness::Fresh => "fresh",
        application::LatencyFreshness::Stale => "stale",
        application::LatencyFreshness::Unavailable => "unavailable",
    }
}

fn latency_probe_status(status: application::LatencyProbeStatus) -> &'static str {
    match status {
        application::LatencyProbeStatus::NotSampled => "not_sampled",
        application::LatencyProbeStatus::Queued => "queued",
        application::LatencyProbeStatus::InFlight => "in_flight",
        application::LatencyProbeStatus::Succeeded => "succeeded",
        application::LatencyProbeStatus::Failed => "failed",
    }
}

fn policy_target_validation(validation: application::PolicyTargetValidation) -> &'static str {
    match validation {
        application::PolicyTargetValidation::Valid => "valid",
        application::PolicyTargetValidation::Missing => "missing",
        application::PolicyTargetValidation::Unavailable => "unavailable",
    }
}

fn rule_mutation_action(action: application::RuleMutationAction) -> &'static str {
    match action {
        application::RuleMutationAction::Added => "added",
        application::RuleMutationAction::Replaced => "replaced",
        application::RuleMutationAction::Removed => "removed",
    }
}

fn runtime_apply_status(status: application::RuntimeApplyStatus) -> &'static str {
    match status {
        application::RuntimeApplyStatus::NotRequired => "not_required",
        application::RuntimeApplyStatus::Applied => "applied",
        application::RuntimeApplyStatus::Recovered => "recovered",
        application::RuntimeApplyStatus::Failed => "failed",
    }
}

fn recovery_status(status: application::RecoveryStatus) -> &'static str {
    match status {
        application::RecoveryStatus::NotRequired => "not_required",
        application::RecoveryStatus::Succeeded => "succeeded",
        application::RecoveryStatus::Failed => "failed",
    }
}

fn selector_kind(kind: application::SelectorKind) -> &'static str {
    match kind {
        application::SelectorKind::Profile => "Profile",
        application::SelectorKind::ProxyGroup => "Proxy Group",
        application::SelectorKind::Node => "Node",
        application::SelectorKind::Rule => "Rule",
    }
}

#[cfg(test)]
mod tests {
    use super::{JsonEncodingError, encode_json_line, terminal_safe, write_bounded_json_success};
    use crate::error::ProcessExitCode;

    #[test]
    fn bounded_json_encoder_counts_the_trailing_newline() {
        let value = serde_json::json!({"value": "abc"});
        let encoded = serde_json::to_vec(&value).expect("fixture should serialize");

        assert_eq!(
            encode_json_line(&value, encoded.len()),
            Err(JsonEncodingError::LimitExceeded)
        );
        let line = encode_json_line(&value, encoded.len() + 1).expect("line should fit exactly");
        assert_eq!(line.len(), encoded.len() + 1);
        assert_eq!(line.last(), Some(&b'\n'));
    }

    #[test]
    fn human_text_escapes_terminal_and_bidirectional_controls() {
        assert_eq!(
            terminal_safe("name\u{1b}[31m\n\u{202e}tail"),
            "name\\u{1b}[31m\\n\\u{202e}tail"
        );
    }

    #[test]
    fn oversized_json_never_writes_a_partial_success_document() {
        let value = serde_json::json!({"value": "a response larger than the test limit"});
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();

        let exit = write_bounded_json_success(&value, &mut stdout, &mut stderr, 24);

        assert_eq!(exit, ProcessExitCode::ExternalOperationFailure);
        assert!(stdout.is_empty());
        let error: serde_json::Value =
            serde_json::from_slice(&stderr).expect("stderr should contain one error envelope");
        assert_eq!(error["error"]["code"], "external_operation_failed");
        assert_eq!(
            error["error"]["message"],
            "JSON output exceeds the 24-byte limit"
        );
    }
}
