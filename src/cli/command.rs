use crate::application::{ApplicationOperation, RulePlacement};
use crate::domain::SubscriptionUrl;
use clap::{ArgGroup, Args, Parser, Subcommand, ValueEnum};

#[derive(Debug, Parser)]
#[command(
    name = "hopash",
    version,
    about = "Manage Mihomo from the command line and a terminal interface",
    disable_help_subcommand = true,
    after_help = "For automation guidance, run: hopash help agent"
)]
pub struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
}

impl Cli {
    #[must_use]
    pub fn into_invocation(self) -> Invocation {
        match self.command {
            None => Invocation::PrintGeneralHelp,
            Some(Command::Start(args)) => {
                Invocation::application(ApplicationOperation::Start, args)
            }
            Some(Command::Stop(args)) => Invocation::application(ApplicationOperation::Stop, args),
            Some(Command::Restart(args)) => {
                Invocation::application(ApplicationOperation::Restart, args)
            }
            Some(Command::Status(args)) => {
                if args.json {
                    Invocation::application(ApplicationOperation::GetStatus, args)
                } else {
                    Invocation::LaunchStatusInterface
                }
            }
            Some(Command::Profile(args)) => match args.command {
                ProfileCommand::Add(args) => Invocation::Application {
                    operation: ApplicationOperation::ProfileAdd {
                        subscription_url: args.subscription_url,
                    },
                    output: args.output.into(),
                },
                ProfileCommand::List(args) => {
                    Invocation::application(ApplicationOperation::ProfileList, args)
                }
                ProfileCommand::Use(args) => Invocation::Application {
                    operation: ApplicationOperation::ProfileUse {
                        profile: args.profile,
                    },
                    output: args.output.into(),
                },
                ProfileCommand::Remove(args) => Invocation::Application {
                    operation: ApplicationOperation::ProfileRemove {
                        profile: args.profile,
                    },
                    output: args.output.into(),
                },
            },
            Some(Command::Proxy(args)) => match args.command {
                ProxyCommand::List(args) => Invocation::Application {
                    operation: ApplicationOperation::ProxyList { group: args.group },
                    output: args.output.into(),
                },
                ProxyCommand::Select(args) => Invocation::Application {
                    operation: ApplicationOperation::ProxySelect {
                        group: args.group,
                        node: args.node,
                    },
                    output: args.output.into(),
                },
            },
            Some(Command::Latency(args)) => match args.command {
                LatencyCommand::List(args) => {
                    Invocation::application(ApplicationOperation::LatencyList, args)
                }
                LatencyCommand::Show(args) => Invocation::Application {
                    operation: ApplicationOperation::LatencyShow { node: args.node },
                    output: args.output.into(),
                },
            },
            Some(Command::Rule(args)) => match args.command {
                RuleCommand::List(args) => {
                    Invocation::application(ApplicationOperation::RuleList, args)
                }
                RuleCommand::Add(args) => {
                    let placement = args.placement();
                    let output = args.output.into();
                    Invocation::Application {
                        operation: ApplicationOperation::RuleAdd {
                            rule: args.rule,
                            placement,
                        },
                        output,
                    }
                }
                RuleCommand::Replace(args) => Invocation::Application {
                    operation: ApplicationOperation::RuleReplace {
                        old_rule: args.old_rule,
                        new_rule: args.new_rule,
                    },
                    output: args.output.into(),
                },
                RuleCommand::Remove(args) => Invocation::Application {
                    operation: ApplicationOperation::RuleRemove { rule: args.rule },
                    output: args.output.into(),
                },
            },
            Some(Command::Logs(args)) => Invocation::FollowLogs {
                output: args.output.into(),
            },
            Some(Command::Help(args)) => match args.topic {
                Some(HelpTopic::Agent) => Invocation::PrintAgentHelp,
                None => Invocation::PrintGeneralHelp,
            },
        }
    }
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Start the background Supervisor and wait until it is ready.
    Start(OutputArgs),
    /// Stop the Supervisor and its Managed Core.
    Stop(OutputArgs),
    /// Restart the Supervisor and restore its committed runtime.
    Restart(OutputArgs),
    /// Open the Status Interface or print one JSON status snapshot.
    Status(OutputArgs),
    /// Add, list, activate, and remove subscription Profiles.
    Profile(ProfileArgs),
    /// List Proxy Groups and select Nodes.
    Proxy(ProxyArgs),
    /// Inspect latency for Active Profile Nodes.
    Latency(LatencyArgs),
    /// List and atomically mutate the Local Rule Set.
    Rule(RuleArgs),
    /// Follow the live Core Log stream.
    Logs(LogsArgs),
    /// Show command help or the AI Agent operation contract.
    Help(HelpArgs),
}

#[derive(Debug, Args)]
struct ProfileArgs {
    #[command(subcommand)]
    command: ProfileCommand,
}

#[derive(Debug, Subcommand)]
enum ProfileCommand {
    /// Download, validate, and save an HTTP(S) subscription.
    Add(ProfileAddArgs),
    /// List saved Profiles and their refresh state.
    List(OutputArgs),
    /// Activate a validated Profile Snapshot.
    Use(ProfileSelectorArgs),
    /// Remove an Inactive Profile.
    Remove(ProfileSelectorArgs),
}

#[derive(Debug, Args)]
struct ProfileAddArgs {
    /// HTTP(S) subscription URL.
    #[arg(value_parser = parse_http_subscription_url)]
    subscription_url: SubscriptionUrl,
    #[command(flatten)]
    output: OutputArgs,
}

#[derive(Debug, Args)]
struct ProfileSelectorArgs {
    /// Opaque Profile ID or unique case-sensitive display name.
    profile: String,
    #[command(flatten)]
    output: OutputArgs,
}

fn parse_http_subscription_url(value: &str) -> Result<SubscriptionUrl, String> {
    SubscriptionUrl::parse(value).map_err(|error| error.to_string())
}

#[derive(Debug, Args)]
struct ProxyArgs {
    #[command(subcommand)]
    command: ProxyCommand,
}

#[derive(Debug, Subcommand)]
enum ProxyCommand {
    /// List the Nodes exposed by one Proxy Group.
    List(GroupSelectorArgs),
    /// Select one Node in a Proxy Group.
    Select(ProxySelectArgs),
}

#[derive(Debug, Args)]
struct GroupSelectorArgs {
    /// Opaque Proxy Group ID or unique case-sensitive display name.
    group: String,
    #[command(flatten)]
    output: OutputArgs,
}

#[derive(Debug, Args)]
struct ProxySelectArgs {
    /// Opaque Proxy Group ID or unique case-sensitive display name.
    group: String,
    /// Source-aware Node ID or unique case-sensitive display name.
    node: String,
    #[command(flatten)]
    output: OutputArgs,
}

#[derive(Debug, Args)]
struct LatencyArgs {
    #[command(subcommand)]
    command: LatencyCommand,
}

#[derive(Debug, Subcommand)]
enum LatencyCommand {
    /// List latency samples for all Active Profile Nodes.
    List(OutputArgs),
    /// Show the latency sample for one Active Profile Node.
    Show(NodeSelectorArgs),
}

#[derive(Debug, Args)]
struct NodeSelectorArgs {
    /// Source-aware Node ID or unique case-sensitive display name.
    node: String,
    #[command(flatten)]
    output: OutputArgs,
}

#[derive(Debug, Args)]
struct RuleArgs {
    #[command(subcommand)]
    command: RuleCommand,
}

#[derive(Debug, Subcommand)]
enum RuleCommand {
    /// List Local Rule Set entries in effective order.
    List(OutputArgs),
    /// Insert one complete Rule String at an explicit position.
    Add(RuleAddArgs),
    /// Replace one exact, complete Rule String.
    Replace(RuleReplaceArgs),
    /// Remove one exact, complete Rule String.
    Remove(RuleRemoveArgs),
}

#[derive(Debug, Args)]
#[command(group(
    ArgGroup::new("placement")
        .required(true)
        .multiple(false)
        .args(["prepend", "append", "before", "after"])
))]
struct RuleAddArgs {
    /// Complete, case-sensitive Mihomo Rule String to insert.
    rule: String,
    /// Insert before every existing rule.
    #[arg(long)]
    prepend: bool,
    /// Insert after every existing rule.
    #[arg(long)]
    append: bool,
    /// Insert before this exact, complete Rule String.
    #[arg(long)]
    before: Option<String>,
    /// Insert after this exact, complete Rule String.
    #[arg(long)]
    after: Option<String>,
    #[command(flatten)]
    output: OutputArgs,
}

impl RuleAddArgs {
    fn placement(&self) -> RulePlacement {
        if self.prepend {
            RulePlacement::Prepend
        } else if self.append {
            RulePlacement::Append
        } else if let Some(anchor) = &self.before {
            RulePlacement::Before(anchor.clone())
        } else if let Some(anchor) = &self.after {
            RulePlacement::After(anchor.clone())
        } else {
            unreachable!("Clap validates the required placement group")
        }
    }
}

#[derive(Debug, Args)]
struct RuleReplaceArgs {
    /// Exact, complete Rule String to replace.
    old_rule: String,
    /// Complete Rule String to insert in its place.
    new_rule: String,
    #[command(flatten)]
    output: OutputArgs,
}

#[derive(Debug, Args)]
struct RuleRemoveArgs {
    /// Exact, complete Rule String to remove.
    rule: String,
    #[command(flatten)]
    output: OutputArgs,
}

#[derive(Debug, Args)]
struct LogsArgs {
    /// Continue streaming until interrupted.
    #[arg(long, required = true)]
    follow: bool,
    #[command(flatten)]
    output: OutputArgs,
}

#[derive(Debug, Args)]
struct HelpArgs {
    /// Optional audience-specific help topic.
    #[arg(value_enum)]
    topic: Option<HelpTopic>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum HelpTopic {
    /// Stable operation guidance for AI Agents and scripts.
    Agent,
}

#[derive(Clone, Copy, Debug, Default, Args)]
struct OutputArgs {
    /// Emit a versioned JSON document or NDJSON stream.
    #[arg(long)]
    json: bool,
}

impl From<OutputArgs> for OutputMode {
    fn from(args: OutputArgs) -> Self {
        if args.json { Self::Json } else { Self::Human }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OutputMode {
    Human,
    Json,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Invocation {
    PrintGeneralHelp,
    PrintAgentHelp,
    LaunchStatusInterface,
    FollowLogs {
        output: OutputMode,
    },
    Application {
        operation: ApplicationOperation,
        output: OutputMode,
    },
}

impl Invocation {
    fn application(operation: ApplicationOperation, output: OutputArgs) -> Self {
        Self::Application {
            operation,
            output: output.into(),
        }
    }
}
