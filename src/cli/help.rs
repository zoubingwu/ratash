use super::Cli;
use clap::CommandFactory;

#[must_use]
pub fn render_agent_help() -> String {
    let mut command_surface = String::new();
    render_command_help(&Cli::command(), "ratash", &mut command_surface);
    format!(
        "Ratash Agent Help\n\nCurrent command surface:\n\n{command_surface}\
Safe rule workflow:\n\
1. Run `ratash rule list --json`.\n\
2. Copy the complete, case-sensitive Rule String for the target or anchor.\n\
3. Change one rule with exactly one placement option.\n\
4. Read the current rule list before retrying after `rule_busy`, `rule_not_found`, \
`rule_ambiguous`, or `rule_already_exists`.\n\
5. Inspect the Runtime Apply result before continuing.\n\n\
Failure recovery:\n\
- For `supervisor_unavailable`, run `ratash start --json`, then `ratash status --json`.\n\
- After a Runtime Apply failure, the last committed Runtime Generation remains active. Run \
`ratash status --json` and reread the affected resource before the next mutation.\n\
- After a mutation response deadline or transport failure, query status and the affected resource \
before retrying.\n\
- Treat `retryable: true` as permission to refresh state and retry the complete operation.\n"
    )
}

fn render_command_help(command: &clap::Command, path: &str, output: &mut String) {
    let mut rendered = command.clone().bin_name(path);
    output.push_str("$ ");
    output.push_str(path);
    output.push('\n');
    output.push_str(&rendered.render_long_help().to_string());
    output.push_str("\n\n");

    for subcommand in command.get_subcommands() {
        let subcommand_path = format!("{path} {}", subcommand.get_name());
        render_command_help(subcommand, &subcommand_path, output);
    }
}
