use ratash::cli::render_agent_help;

const SKILL: &str = include_str!("../skills/ratash/SKILL.md");

#[test]
fn packaged_skill_uses_the_live_agent_help_as_authority() {
    assert!(SKILL.contains("ratash help agent"));
    assert!(SKILL.contains("Use the installed `ratash` executable as the only product interface"));
}

#[test]
fn packaged_skill_and_agent_help_share_the_atomic_rule_workflow() {
    let help = render_agent_help();
    for contract_term in [
        "ratash rule list --json",
        "--prepend",
        "--append",
        "--before",
        "--after",
        "rule_busy",
        "rule_not_found",
        "rule_ambiguous",
        "rule_already_exists",
        "Runtime Apply",
    ] {
        assert!(
            help.contains(contract_term),
            "Agent Help is missing {contract_term}"
        );
        assert!(
            SKILL.contains(contract_term),
            "packaged Skill is missing {contract_term}"
        );
    }
}

#[test]
fn packaged_skill_prefers_json_and_protects_subscription_credentials() {
    assert!(SKILL.contains("Use `--json`"));
    assert!(SKILL.contains("Subscription URL credentials"));
    assert!(SKILL.contains("versioned NDJSON event"));
}
