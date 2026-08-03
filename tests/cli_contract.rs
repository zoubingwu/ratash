use clap::{CommandFactory, Parser};
use ratash::application::{ApplicationOperation, RulePlacement};
use ratash::cli::{Cli, Invocation, OutputMode};
use ratash::domain::SubscriptionUrl;

#[test]
fn bare_command_routes_to_general_help() {
    let cli = Cli::try_parse_from(["ratash"]).expect("bare command should parse");

    assert_eq!(cli.into_invocation(), Invocation::PrintGeneralHelp);
}

#[test]
fn lifecycle_commands_preserve_the_output_mode() {
    let cases = [
        (
            vec!["ratash", "start"],
            ApplicationOperation::Start,
            OutputMode::Human,
        ),
        (
            vec!["ratash", "stop", "--json"],
            ApplicationOperation::Stop,
            OutputMode::Json,
        ),
        (
            vec!["ratash", "restart"],
            ApplicationOperation::Restart,
            OutputMode::Human,
        ),
    ];

    for (arguments, operation, output) in cases {
        let cli = Cli::try_parse_from(arguments).expect("lifecycle command should parse");

        assert_eq!(
            cli.into_invocation(),
            Invocation::Application { operation, output }
        );
    }
}

#[test]
fn status_selects_the_terminal_interface_or_json_query() {
    let interactive = Cli::try_parse_from(["ratash", "status"])
        .expect("interactive status should parse")
        .into_invocation();
    let json = Cli::try_parse_from(["ratash", "status", "--json"])
        .expect("JSON status should parse")
        .into_invocation();

    assert_eq!(interactive, Invocation::LaunchStatusInterface);
    assert_eq!(
        json,
        Invocation::Application {
            operation: ApplicationOperation::GetStatus,
            output: OutputMode::Json,
        }
    );
}

#[test]
fn profile_commands_preserve_urls_selectors_and_output_modes() {
    let cases = [
        (
            vec![
                "ratash",
                "profile",
                "add",
                "https://example.com/sub?token=secret",
                "--json",
            ],
            ApplicationOperation::ProfileAdd {
                subscription_url: SubscriptionUrl::parse("https://example.com/sub?token=secret")
                    .expect("fixture URL should be valid"),
            },
            OutputMode::Json,
        ),
        (
            vec!["ratash", "profile", "list"],
            ApplicationOperation::ProfileList,
            OutputMode::Human,
        ),
        (
            vec!["ratash", "profile", "use", "Office Profile"],
            ApplicationOperation::ProfileUse {
                profile: "Office Profile".to_owned(),
            },
            OutputMode::Human,
        ),
        (
            vec!["ratash", "profile", "remove", "profile-01", "--json"],
            ApplicationOperation::ProfileRemove {
                profile: "profile-01".to_owned(),
            },
            OutputMode::Json,
        ),
    ];

    for (arguments, operation, output) in cases {
        let cli = Cli::try_parse_from(arguments).expect("profile command should parse");
        assert_eq!(
            cli.into_invocation(),
            Invocation::Application { operation, output }
        );
    }

    assert!(Cli::try_parse_from(["ratash", "profile", "add", "file:///tmp/profile.yaml"]).is_err());
}

#[test]
fn proxy_and_latency_commands_preserve_case_sensitive_selectors() {
    let cases = [
        (
            vec!["ratash", "proxy", "list", "Auto Select", "--json"],
            ApplicationOperation::ProxyList {
                group: "Auto Select".to_owned(),
            },
            OutputMode::Json,
        ),
        (
            vec!["ratash", "proxy", "select", "Auto Select", "HK Node 01"],
            ApplicationOperation::ProxySelect {
                group: "Auto Select".to_owned(),
                node: "HK Node 01".to_owned(),
            },
            OutputMode::Human,
        ),
        (
            vec!["ratash", "latency", "list", "--json"],
            ApplicationOperation::LatencyList,
            OutputMode::Json,
        ),
        (
            vec!["ratash", "latency", "show", "Provider/HK Node 01"],
            ApplicationOperation::LatencyShow {
                node: "Provider/HK Node 01".to_owned(),
            },
            OutputMode::Human,
        ),
    ];

    for (arguments, operation, output) in cases {
        let cli = Cli::try_parse_from(arguments).expect("query command should parse");
        assert_eq!(
            cli.into_invocation(),
            Invocation::Application { operation, output }
        );
    }
}

#[test]
fn rule_commands_preserve_complete_strings_and_require_one_placement() {
    let logical_rule = "AND,((DOMAIN,api.example.com),(NETWORK,TCP)),DIRECT";
    let anchor = "MATCH,PROXY";
    let cases = [
        (
            vec!["ratash", "rule", "list", "--json"],
            ApplicationOperation::RuleList,
            OutputMode::Json,
        ),
        (
            vec!["ratash", "rule", "add", logical_rule, "--before", anchor],
            ApplicationOperation::RuleAdd {
                rule: logical_rule.to_owned(),
                placement: RulePlacement::Before(anchor.to_owned()),
            },
            OutputMode::Human,
        ),
        (
            vec![
                "ratash",
                "rule",
                "replace",
                "DOMAIN-SUFFIX,Example.com,PROXY",
                "DOMAIN-SUFFIX,Example.com,DIRECT",
                "--json",
            ],
            ApplicationOperation::RuleReplace {
                old_rule: "DOMAIN-SUFFIX,Example.com,PROXY".to_owned(),
                new_rule: "DOMAIN-SUFFIX,Example.com,DIRECT".to_owned(),
            },
            OutputMode::Json,
        ),
        (
            vec![
                "ratash",
                "rule",
                "remove",
                "DOMAIN-SUFFIX,Example.com,DIRECT",
            ],
            ApplicationOperation::RuleRemove {
                rule: "DOMAIN-SUFFIX,Example.com,DIRECT".to_owned(),
            },
            OutputMode::Human,
        ),
    ];

    for (arguments, operation, output) in cases {
        let cli = Cli::try_parse_from(arguments).expect("rule command should parse");
        assert_eq!(
            cli.into_invocation(),
            Invocation::Application { operation, output }
        );
    }

    assert!(Cli::try_parse_from(["ratash", "rule", "add", "MATCH,DIRECT"]).is_err());
    assert!(
        Cli::try_parse_from([
            "ratash",
            "rule",
            "add",
            "MATCH,DIRECT",
            "--prepend",
            "--append",
        ])
        .is_err()
    );
}

#[test]
fn logs_and_help_route_to_dedicated_local_invocations() {
    let human_logs = Cli::try_parse_from(["ratash", "logs", "--follow"])
        .expect("human log follow should parse")
        .into_invocation();
    let json_logs = Cli::try_parse_from(["ratash", "logs", "--follow", "--json"])
        .expect("JSON log follow should parse")
        .into_invocation();
    let help = Cli::try_parse_from(["ratash", "help"])
        .expect("general help should parse")
        .into_invocation();
    let agent_help = Cli::try_parse_from(["ratash", "help", "agent"])
        .expect("agent help should parse")
        .into_invocation();

    assert_eq!(
        human_logs,
        Invocation::FollowLogs {
            output: OutputMode::Human
        }
    );
    assert_eq!(
        json_logs,
        Invocation::FollowLogs {
            output: OutputMode::Json
        }
    );
    assert_eq!(help, Invocation::PrintGeneralHelp);
    assert_eq!(agent_help, Invocation::PrintAgentHelp);

    assert!(Cli::try_parse_from(["ratash", "logs"]).is_err());
}

#[test]
fn root_help_lists_the_public_surface_only() {
    let mut command = Cli::command();
    let mut output = Vec::new();
    command
        .write_long_help(&mut output)
        .expect("help should render");
    let help = String::from_utf8(output).expect("help should be UTF-8");

    for public_command in [
        "start", "stop", "restart", "profile", "proxy", "latency", "status", "logs", "rule", "help",
    ] {
        assert!(
            help.contains(public_command),
            "missing {public_command}:\n{help}"
        );
    }
    assert!(!help.contains("supervisor-internal"));
    assert!(!help.contains("service-internal"));
}

#[test]
fn every_public_command_argument_and_flag_has_help_text() {
    let mut command = Cli::command();
    command.build();
    assert_help_tree(&command, "ratash");
}

fn assert_help_tree(command: &clap::Command, path: &str) {
    for argument in command.get_arguments() {
        assert!(
            argument
                .get_help()
                .is_some_and(|help| !help.to_string().trim().is_empty()),
            "{path} argument {} has no help text",
            argument.get_id()
        );
    }
    for subcommand in command.get_subcommands() {
        let subcommand_path = format!("{path} {}", subcommand.get_name());
        assert!(
            subcommand
                .get_about()
                .is_some_and(|about| !about.to_string().trim().is_empty()),
            "{subcommand_path} has no description"
        );
        assert_help_tree(subcommand, &subcommand_path);
    }
}
