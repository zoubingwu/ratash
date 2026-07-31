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
    Start(OutputArgs),
    Stop(OutputArgs),
    Restart(OutputArgs),
    Status(OutputArgs),
    Profile(ProfileArgs),
    Proxy(ProxyArgs),
    Latency(LatencyArgs),
    Rule(RuleArgs),
    Logs(LogsArgs),
    Help(HelpArgs),
}

#[derive(Debug, Args)]
struct ProfileArgs {
    #[command(subcommand)]
    command: ProfileCommand,
}

#[derive(Debug, Subcommand)]
enum ProfileCommand {
    Add(ProfileAddArgs),
    List(OutputArgs),
    Use(ProfileSelectorArgs),
    Remove(ProfileSelectorArgs),
}

#[derive(Debug, Args)]
struct ProfileAddArgs {
    #[arg(value_parser = parse_http_subscription_url)]
    subscription_url: SubscriptionUrl,
    #[command(flatten)]
    output: OutputArgs,
}

#[derive(Debug, Args)]
struct ProfileSelectorArgs {
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
    List(GroupSelectorArgs),
    Select(ProxySelectArgs),
}

#[derive(Debug, Args)]
struct GroupSelectorArgs {
    group: String,
    #[command(flatten)]
    output: OutputArgs,
}

#[derive(Debug, Args)]
struct ProxySelectArgs {
    group: String,
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
    List(OutputArgs),
    Show(NodeSelectorArgs),
}

#[derive(Debug, Args)]
struct NodeSelectorArgs {
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
    List(OutputArgs),
    Add(RuleAddArgs),
    Replace(RuleReplaceArgs),
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
    rule: String,
    #[arg(long)]
    prepend: bool,
    #[arg(long)]
    append: bool,
    #[arg(long)]
    before: Option<String>,
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
    old_rule: String,
    new_rule: String,
    #[command(flatten)]
    output: OutputArgs,
}

#[derive(Debug, Args)]
struct RuleRemoveArgs {
    rule: String,
    #[command(flatten)]
    output: OutputArgs,
}

#[derive(Debug, Args)]
struct LogsArgs {
    #[arg(long, required = true)]
    follow: bool,
    #[command(flatten)]
    output: OutputArgs,
}

#[derive(Debug, Args)]
struct HelpArgs {
    #[arg(value_enum)]
    topic: Option<HelpTopic>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum HelpTopic {
    Agent,
}

#[derive(Clone, Copy, Debug, Default, Args)]
struct OutputArgs {
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
