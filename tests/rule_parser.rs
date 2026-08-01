use hopash::domain::LocalRuleSetRevision;
use hopash::rule::{
    LocalRuleSet, ParsedRule, RuleDocumentError, RulePlacement, RuleSetError, RuleSetLimits,
    RuleString, RuleStringError, RuleType, parse_rule,
};

const TEST_LIMITS: RuleSetLimits = RuleSetLimits {
    max_document_bytes: 2 * 1024 * 1024,
    max_rule_bytes: 1024,
    max_rule_count: 20_000,
};

#[test]
fn product_limits_are_sourced_from_the_shared_release_contract() {
    let limits = RuleSetLimits::product();

    assert_eq!(
        limits.max_document_bytes,
        hopash::constants::LOCAL_RULE_SET_MAX_BYTES
    );
    assert_eq!(
        limits.max_rule_bytes,
        hopash::constants::RULE_STRING_MAX_BYTES
    );
    assert_eq!(
        limits.max_rule_count,
        hopash::constants::LOCAL_RULE_COUNT_MAX
    );
}

#[test]
fn rule_string_preserves_the_complete_original_with_an_explicit_size_limit() {
    let original = "DOMAIN-REGEX,^api,(v1|v2)\\.example\\.com$,Proxy A";

    let rule = RuleString::new(original, original.len()).expect("rule should fit");

    assert_eq!(rule.as_str(), original);
    assert_eq!(
        RuleString::new(original, original.len() - 1),
        Err(RuleStringError::TooLarge {
            actual_bytes: original.len(),
            max_bytes: original.len() - 1,
        })
    );
}

#[test]
fn rule_string_size_limit_counts_utf8_bytes_at_the_exact_boundary() {
    let original = "DOMAIN,例子.test,DIRECT";

    assert!(RuleString::new(original, original.len()).is_ok());
    assert_eq!(
        RuleString::new(original, original.len() - 1),
        Err(RuleStringError::TooLarge {
            actual_bytes: original.len(),
            max_bytes: original.len() - 1,
        })
    );
}

#[test]
fn parser_returns_type_payload_policy_target_and_params_for_a_standard_rule() {
    let rule = RuleString::new("DOMAIN,Example.COM,Proxy A,no-resolve", 1024).unwrap();

    let parsed = parse_rule(&rule).expect("rule should parse");

    assert_eq!(
        parsed,
        ParsedRule {
            original: &rule,
            rule_type: RuleType::Domain,
            payload: Some("Example.COM"),
            policy_target: "Proxy A",
            params: vec!["no-resolve"],
        }
    );
}

#[test]
fn parser_handles_a_rule_without_a_payload() {
    let rule = RuleString::new("MATCH,DIRECT", 1024).unwrap();

    let parsed = parse_rule(&rule).expect("rule should parse");

    assert_eq!(
        parsed,
        ParsedRule {
            original: &rule,
            rule_type: RuleType::Match,
            payload: None,
            policy_target: "DIRECT",
            params: vec![],
        }
    );
}

#[test]
fn parser_preserves_commas_inside_a_logical_rule_payload() {
    let rule = RuleString::new(
        "AND,((DOMAIN,example.com),(NETWORK,UDP)),Proxy A,no-resolve",
        1024,
    )
    .unwrap();

    let parsed = parse_rule(&rule).expect("logical rule should parse");

    assert_eq!(
        parsed,
        ParsedRule {
            original: &rule,
            rule_type: RuleType::And,
            payload: Some("((DOMAIN,example.com),(NETWORK,UDP))"),
            policy_target: "Proxy A",
            params: vec!["no-resolve"],
        }
    );
}

#[test]
fn parser_preserves_commas_inside_a_regex_payload() {
    let rule = RuleString::new(
        "DOMAIN-REGEX,^(api|cdn),v[0-9]+\\.example\\.com$,Proxy A,no-resolve",
        1024,
    )
    .unwrap();

    let parsed = parse_rule(&rule).expect("regex rule should parse");

    assert_eq!(
        parsed,
        ParsedRule {
            original: &rule,
            rule_type: RuleType::DomainRegex,
            payload: Some("^(api|cdn),v[0-9]+\\.example\\.com$"),
            policy_target: "Proxy A",
            params: vec!["no-resolve"],
        }
    );
}

#[test]
fn parser_preserves_regex_commas_with_multiple_documented_trailing_params() {
    let rule = RuleString::new(
        "PROCESS-PATH-REGEX,^/Applications/(Foo, Bar)/.*,Proxy A,no-resolve,src",
        1024,
    )
    .unwrap();

    let parsed = parse_rule(&rule).expect("regex rule should parse");

    assert_eq!(parsed.payload, Some("^/Applications/(Foo, Bar)/.*"));
    assert_eq!(parsed.policy_target, "Proxy A");
    assert_eq!(parsed.params, vec!["no-resolve", "src"]);
}

#[test]
fn parser_preserves_commas_inside_a_sub_rule_payload() {
    let rule = RuleString::new("SUB-RULE,(NETWORK,tcp),private-routing", 1024).unwrap();

    let parsed = parse_rule(&rule).expect("sub-rule should parse");

    assert_eq!(parsed.rule_type, RuleType::SubRule);
    assert_eq!(parsed.payload, Some("(NETWORK,tcp)"));
    assert_eq!(parsed.policy_target, "private-routing");
    assert!(parsed.params.is_empty());
}

#[test]
fn parser_recognizes_the_supported_mihomo_rule_type_catalog() {
    let cases = [
        ("DOMAIN-SUFFIX", RuleType::DomainSuffix),
        ("DOMAIN-KEYWORD", RuleType::DomainKeyword),
        ("DOMAIN-WILDCARD", RuleType::DomainWildcard),
        ("GEOSITE", RuleType::Geosite),
        ("GEOIP", RuleType::GeoIp),
        ("SRC-GEOIP", RuleType::SrcGeoIp),
        ("IP-ASN", RuleType::IpAsn),
        ("SRC-IP-ASN", RuleType::SrcIpAsn),
        ("IP-CIDR", RuleType::IpCidr),
        ("IP-CIDR6", RuleType::IpCidr6),
        ("SRC-IP-CIDR", RuleType::SrcIpCidr),
        ("IP-SUFFIX", RuleType::IpSuffix),
        ("SRC-IP-SUFFIX", RuleType::SrcIpSuffix),
        ("SRC-PORT", RuleType::SrcPort),
        ("DST-PORT", RuleType::DstPort),
        ("IN-PORT", RuleType::InPort),
        ("DSCP", RuleType::Dscp),
        ("PROCESS-NAME", RuleType::ProcessName),
        ("PROCESS-PATH", RuleType::ProcessPath),
        ("NETWORK", RuleType::Network),
        ("UID", RuleType::Uid),
        ("IN-TYPE", RuleType::InType),
        ("IN-USER", RuleType::InUser),
        ("IN-NAME", RuleType::InName),
        ("REMATCH-NAME", RuleType::RematchName),
        ("RULE-SET", RuleType::RuleSet),
        ("PROCESS-NAME-WILDCARD", RuleType::ProcessNameWildcard),
        ("PROCESS-PATH-WILDCARD", RuleType::ProcessPathWildcard),
    ];

    for (name, expected) in cases {
        let rule = RuleString::new(format!("{name},value,DIRECT"), 1024).unwrap();
        assert_eq!(parse_rule(&rule).unwrap().rule_type, expected, "{name}");
    }

    for (name, expected) in [
        ("PROCESS-NAME-REGEX", RuleType::ProcessNameRegex),
        ("PROCESS-PATH-REGEX", RuleType::ProcessPathRegex),
    ] {
        let rule = RuleString::new(format!("{name},^foo,bar$,DIRECT"), 1024).unwrap();
        assert_eq!(parse_rule(&rule).unwrap().rule_type, expected, "{name}");
    }

    for (name, expected) in [("OR", RuleType::Or), ("NOT", RuleType::Not)] {
        let rule = RuleString::new(format!("{name},((DOMAIN,example.com)),DIRECT"), 1024).unwrap();
        assert_eq!(parse_rule(&rule).unwrap().rule_type, expected, "{name}");
    }
}

#[test]
fn uninitialized_rule_set_lists_an_explicit_empty_state() {
    let rules = LocalRuleSet::uninitialized();

    let listed = rules.list().unwrap();

    assert!(!rules.is_initialized());
    assert!(!listed.initialized);
    assert!(listed.entries.is_empty());
}

#[test]
fn initialized_rule_set_lists_rules_in_effective_order_with_zero_based_indexes() {
    let rules = LocalRuleSet::initialized(vec![
        RuleString::new("DOMAIN,example.com,Proxy A", 1024).unwrap(),
        RuleString::new("MATCH,DIRECT", 1024).unwrap(),
    ]);

    let listed = rules.list().unwrap();
    let entries = listed
        .entries
        .iter()
        .map(|entry| {
            (
                entry.index,
                entry.rule.as_str(),
                entry.parsed.rule_type,
                entry.parsed.policy_target,
            )
        })
        .collect::<Vec<_>>();

    assert!(rules.is_initialized());
    assert!(listed.initialized);
    assert_eq!(
        entries,
        vec![
            (0, "DOMAIN,example.com,Proxy A", RuleType::Domain, "Proxy A"),
            (1, "MATCH,DIRECT", RuleType::Match, "DIRECT"),
        ]
    );
}

#[test]
fn initialized_rule_set_parses_only_the_requested_page() {
    let rules = LocalRuleSet::initialized(vec![
        RuleString::new("DOMAIN,first.example,DIRECT", 1024).unwrap(),
        RuleString::new("BROKEN", 1024).unwrap(),
        RuleString::new("MATCH,DIRECT", 1024).unwrap(),
    ]);

    let page = rules.list_page(2, 1).unwrap();

    assert!(page.initialized);
    assert_eq!(page.total, 3);
    assert_eq!(page.offset, 2);
    assert_eq!(page.entries.len(), 1);
    assert_eq!(page.entries[0].index, 2);
    assert_eq!(page.entries[0].rule.as_str(), "MATCH,DIRECT");
}

#[test]
fn mutation_rejects_an_uninitialized_rule_set() {
    let mut rules = LocalRuleSet::uninitialized();
    let rule = RuleString::new("MATCH,DIRECT", 1024).unwrap();

    let add = rules.add(rule.clone(), RulePlacement::Append);
    let replace = rules.replace(&rule, rule.clone());
    let remove = rules.remove(&rule);

    assert_eq!(
        (add, replace, remove),
        (
            Err(RuleSetError::RulesUninitialized),
            Err(RuleSetError::RulesUninitialized),
            Err(RuleSetError::RulesUninitialized),
        )
    );
}

#[test]
fn add_supports_all_four_explicit_placements() {
    let first = RuleString::new("DOMAIN,first.example,DIRECT", 1024).unwrap();
    let second = RuleString::new("DOMAIN,second.example,DIRECT", 1024).unwrap();
    let mut rules = LocalRuleSet::initialized(vec![first.clone(), second.clone()]);

    let appended = rules
        .add(
            RuleString::new("DOMAIN,appended.example,DIRECT", 1024).unwrap(),
            RulePlacement::Append,
        )
        .unwrap();
    let prepended = rules
        .add(
            RuleString::new("DOMAIN,prepended.example,DIRECT", 1024).unwrap(),
            RulePlacement::Prepend,
        )
        .unwrap();
    let before = rules
        .add(
            RuleString::new("DOMAIN,before.example,DIRECT", 1024).unwrap(),
            RulePlacement::Before(second),
        )
        .unwrap();
    let after = rules
        .add(
            RuleString::new("DOMAIN,after.example,DIRECT", 1024).unwrap(),
            RulePlacement::After(first),
        )
        .unwrap();

    let final_rules = rules
        .list()
        .unwrap()
        .entries
        .iter()
        .map(|entry| entry.rule.as_str())
        .collect::<Vec<_>>();
    assert_eq!((appended, prepended, before, after), (2, 0, 2, 2));
    assert_eq!(
        final_rules,
        vec![
            "DOMAIN,prepended.example,DIRECT",
            "DOMAIN,first.example,DIRECT",
            "DOMAIN,after.example,DIRECT",
            "DOMAIN,before.example,DIRECT",
            "DOMAIN,second.example,DIRECT",
            "DOMAIN,appended.example,DIRECT",
        ]
    );
}

#[test]
fn add_rejects_a_duplicate_result_without_changing_the_rule_set() {
    let existing = RuleString::new("DOMAIN,example.com,DIRECT", 1024).unwrap();
    let mut rules = LocalRuleSet::initialized(vec![existing.clone()]);

    let result = rules.add(existing, RulePlacement::Append);

    assert_eq!(
        result,
        Err(RuleSetError::RuleAlreadyExists {
            matching_indexes: vec![0]
        })
    );
    assert_eq!(rules.list().unwrap().entries.len(), 1);
}

#[test]
fn replace_changes_exactly_one_rule_in_place() {
    let old = RuleString::new("DOMAIN,old.example,DIRECT", 1024).unwrap();
    let replacement = RuleString::new("DOMAIN,new.example,Proxy A", 1024).unwrap();
    let mut rules = LocalRuleSet::initialized(vec![
        old.clone(),
        RuleString::new("MATCH,DIRECT", 1024).unwrap(),
    ]);

    let index = rules.replace(&old, replacement).unwrap();

    assert_eq!(index, 0);
    assert_eq!(
        rules
            .list()
            .unwrap()
            .entries
            .iter()
            .map(|entry| entry.rule.as_str())
            .collect::<Vec<_>>(),
        vec!["DOMAIN,new.example,Proxy A", "MATCH,DIRECT"]
    );
}

#[test]
fn remove_deletes_exactly_one_matching_rule() {
    let removed_rule = RuleString::new("DOMAIN,remove.example,DIRECT", 1024).unwrap();
    let mut rules = LocalRuleSet::initialized(vec![
        removed_rule.clone(),
        RuleString::new("MATCH,DIRECT", 1024).unwrap(),
    ]);

    let removed = rules.remove(&removed_rule).unwrap();

    assert_eq!(removed, removed_rule);
    assert_eq!(
        rules
            .list()
            .unwrap()
            .entries
            .iter()
            .map(|entry| entry.rule.as_str())
            .collect::<Vec<_>>(),
        vec!["MATCH,DIRECT"]
    );
}

#[test]
fn exact_matching_is_case_sensitive_and_reports_zero_or_multiple_matches() {
    let existing = RuleString::new("DOMAIN,Example.com,DIRECT", 1024).unwrap();
    let different_case = RuleString::new("DOMAIN,example.com,DIRECT", 1024).unwrap();
    let mut unique_rules = LocalRuleSet::initialized(vec![existing.clone()]);
    let mut duplicate_fixture =
        LocalRuleSet::initialized(vec![existing.clone(), different_case, existing.clone()]);

    let zero_matches =
        unique_rules.remove(&RuleString::new("DOMAIN,example.com,DIRECT", 1024).unwrap());
    let multiple_matches = duplicate_fixture.remove(&existing);

    assert_eq!(zero_matches, Err(RuleSetError::RuleNotFound));
    assert_eq!(
        multiple_matches,
        Err(RuleSetError::RuleAmbiguous {
            matching_indexes: vec![0, 2]
        })
    );
}

#[test]
fn replace_rejects_a_rule_that_would_duplicate_another_entry() {
    let first = RuleString::new("DOMAIN,first.example,DIRECT", 1024).unwrap();
    let second = RuleString::new("DOMAIN,second.example,DIRECT", 1024).unwrap();
    let mut rules = LocalRuleSet::initialized(vec![first.clone(), second.clone()]);

    let result = rules.replace(&first, second);

    assert_eq!(
        result,
        Err(RuleSetError::RuleAlreadyExists {
            matching_indexes: vec![1]
        })
    );
    assert_eq!(rules.list().unwrap().entries[0].rule, &first);
}

#[test]
fn mutation_rejects_an_unparseable_candidate_before_changing_state() {
    let existing = RuleString::new("MATCH,DIRECT", 1024).unwrap();
    let invalid = RuleString::new("UNKNOWN,value,DIRECT", 1024).unwrap();
    let mut rules = LocalRuleSet::initialized(vec![existing.clone()]);

    let result = rules.add(invalid, RulePlacement::Append);

    assert_eq!(
        result,
        Err(RuleSetError::InvalidRule(
            hopash::rule::RuleParseError::UnsupportedRuleType("UNKNOWN".to_owned())
        ))
    );
    assert_eq!(rules.list().unwrap().entries[0].rule, &existing);
}

#[test]
fn successful_content_mutations_increment_the_revision_once() {
    let first = RuleString::new("DOMAIN,first.example,DIRECT", 1024).unwrap();
    let replacement = RuleString::new("DOMAIN,replaced.example,DIRECT", 1024).unwrap();
    let appended = RuleString::new("DOMAIN,appended.example,DIRECT", 1024).unwrap();
    let mut rules = LocalRuleSet::initialized_at(vec![first.clone()], LocalRuleSetRevision(41));

    rules.add(appended.clone(), RulePlacement::Append).unwrap();
    assert_eq!(rules.revision(), LocalRuleSetRevision(42));

    rules.replace(&first, replacement.clone()).unwrap();
    assert_eq!(rules.revision(), LocalRuleSetRevision(43));

    rules.remove(&appended).unwrap();
    assert_eq!(rules.revision(), LocalRuleSetRevision(44));
    assert_eq!(
        rules.list().unwrap().entries[0].rule.as_str(),
        replacement.as_str()
    );
}

#[test]
fn failed_and_idempotent_mutations_preserve_the_revision_and_rules() {
    let existing = RuleString::new("DOMAIN,Example.com,DIRECT", 1024).unwrap();
    let missing = RuleString::new("DOMAIN,missing.example,DIRECT", 1024).unwrap();
    let invalid = RuleString::new("UNKNOWN,value,DIRECT", 1024).unwrap();
    let mut rules = LocalRuleSet::initialized_at(
        vec![existing.clone(), existing.clone()],
        LocalRuleSetRevision(9),
    );

    assert_eq!(
        rules.remove(&existing),
        Err(RuleSetError::RuleAmbiguous {
            matching_indexes: vec![0, 1]
        })
    );
    assert_eq!(
        rules.add(invalid, RulePlacement::Append),
        Err(RuleSetError::InvalidRule(
            hopash::rule::RuleParseError::UnsupportedRuleType("UNKNOWN".to_owned())
        ))
    );
    assert_eq!(rules.remove(&missing), Err(RuleSetError::RuleNotFound));
    assert_eq!(rules.revision(), LocalRuleSetRevision(9));
    assert_eq!(rules.list().unwrap().entries.len(), 2);

    let mut unique = LocalRuleSet::initialized_at(vec![existing.clone()], LocalRuleSetRevision(12));
    assert_eq!(unique.replace(&existing, existing.clone()), Ok(0));
    assert_eq!(unique.revision(), LocalRuleSetRevision(12));
}

#[test]
fn revision_exhaustion_preserves_the_committed_rule_set() {
    let existing = RuleString::new("MATCH,DIRECT", 1024).unwrap();
    let added = RuleString::new("DOMAIN,example.com,DIRECT", 1024).unwrap();
    let mut rules =
        LocalRuleSet::initialized_at(vec![existing.clone()], LocalRuleSetRevision(u64::MAX));

    assert_eq!(
        rules.add(added, RulePlacement::Append),
        Err(RuleSetError::RevisionExhausted)
    );
    assert_eq!(rules.revision(), LocalRuleSetRevision(u64::MAX));
    assert_eq!(rules.list().unwrap().entries[0].rule, &existing);
}

#[test]
fn duplicate_anchor_is_ambiguous_and_preserves_state() {
    let anchor = RuleString::new("DOMAIN,anchor.example,DIRECT", 1024).unwrap();
    let candidate = RuleString::new("DOMAIN,new.example,DIRECT", 1024).unwrap();
    let mut rules = LocalRuleSet::initialized_at(
        vec![anchor.clone(), anchor.clone()],
        LocalRuleSetRevision(5),
    );

    let result = rules.add(candidate, RulePlacement::Before(anchor));

    assert_eq!(
        result,
        Err(RuleSetError::RuleAmbiguous {
            matching_indexes: vec![0, 1]
        })
    );
    assert_eq!(rules.revision(), LocalRuleSetRevision(5));
    assert_eq!(rules.list().unwrap().entries.len(), 2);
}

#[test]
fn rules_yaml_is_deterministic_and_round_trips_exact_rule_strings() {
    let rules = LocalRuleSet::initialized_at(
        vec![
            RuleString::new("DOMAIN,example.com,Proxy A", 1024).unwrap(),
            RuleString::new("DOMAIN-REGEX,^api,(v1|v2)\\.example\\.com$,Proxy #1", 1024).unwrap(),
            RuleString::new("MATCH,DIRECT", 1024).unwrap(),
        ],
        LocalRuleSetRevision(7),
    );

    let first = rules.to_yaml().unwrap();
    let second = rules.to_yaml().unwrap();
    let restored = LocalRuleSet::from_yaml(&first, LocalRuleSetRevision(7), TEST_LIMITS).unwrap();

    assert_eq!(first, second);
    assert_eq!(restored, rules);
    assert_eq!(restored.revision(), LocalRuleSetRevision(7));
    assert_eq!(
        restored
            .list()
            .unwrap()
            .entries
            .iter()
            .map(|entry| entry.rule.as_str())
            .collect::<Vec<_>>(),
        vec![
            "DOMAIN,example.com,Proxy A",
            "DOMAIN-REGEX,^api,(v1|v2)\\.example\\.com$,Proxy #1",
            "MATCH,DIRECT",
        ]
    );
}

#[test]
fn rules_yaml_requires_the_closed_rules_document_shape() {
    let unknown_field = "rules:\n- MATCH,DIRECT\nmetadata: hidden\n";
    let non_string_rule = "rules:\n- type: MATCH\n";

    assert!(matches!(
        LocalRuleSet::from_yaml(unknown_field, LocalRuleSetRevision(1), TEST_LIMITS),
        Err(RuleDocumentError::InvalidYaml(_))
    ));
    assert!(matches!(
        LocalRuleSet::from_yaml(non_string_rule, LocalRuleSetRevision(1), TEST_LIMITS),
        Err(RuleDocumentError::InvalidYaml(_))
    ));
}

#[test]
fn rules_yaml_enforces_document_rule_count_and_per_rule_byte_limits() {
    let oversized_document = "rules:\n- MATCH,DIRECT\n";
    let document_limit = RuleSetLimits {
        max_document_bytes: oversized_document.len() - 1,
        ..TEST_LIMITS
    };
    assert_eq!(
        LocalRuleSet::from_yaml(oversized_document, LocalRuleSetRevision(1), document_limit),
        Err(RuleDocumentError::DocumentTooLarge {
            actual_bytes: oversized_document.len(),
            max_bytes: oversized_document.len() - 1,
        })
    );

    let too_many = "rules:\n- MATCH,DIRECT\n- MATCH,REJECT\n";
    let count_limit = RuleSetLimits {
        max_rule_count: 1,
        ..TEST_LIMITS
    };
    assert_eq!(
        LocalRuleSet::from_yaml(too_many, LocalRuleSetRevision(1), count_limit),
        Err(RuleDocumentError::TooManyRules {
            actual_rules: 2,
            max_rules: 1,
        })
    );

    let unicode_rule = "DOMAIN,例子.test,DIRECT";
    let unicode_yaml = format!("rules:\n- {unicode_rule}\n");
    let rule_limit = RuleSetLimits {
        max_rule_bytes: unicode_rule.len() - 1,
        ..TEST_LIMITS
    };
    assert_eq!(
        LocalRuleSet::from_yaml(&unicode_yaml, LocalRuleSetRevision(1), rule_limit),
        Err(RuleDocumentError::InvalidRuleString {
            index: 0,
            source: RuleStringError::TooLarge {
                actual_bytes: unicode_rule.len(),
                max_bytes: unicode_rule.len() - 1,
            },
        })
    );
}

#[test]
fn rules_yaml_accepts_the_twenty_thousand_rule_regression_boundary() {
    let document = format!(
        "rules:\n{}",
        (0..20_000)
            .map(|index| format!("- DOMAIN,node-{index}.example,DIRECT\n"))
            .collect::<String>()
    );

    let mut rules =
        LocalRuleSet::from_yaml(&document, LocalRuleSetRevision(3), TEST_LIMITS).unwrap();
    let target = RuleString::new("DOMAIN,node-19999.example,DIRECT", 1024).unwrap();
    let matching_indexes = rules
        .list()
        .unwrap()
        .entries
        .iter()
        .filter_map(|entry| (entry.rule == &target).then_some(entry.index))
        .collect::<Vec<_>>();
    let replacement = RuleString::new("DOMAIN,node-19999.example,REJECT", 1024).unwrap();
    let position = rules.replace(&target, replacement.clone()).unwrap();
    let serialized = rules.to_yaml().unwrap();

    assert_eq!(rules.list().unwrap().entries.len(), 20_000);
    assert_eq!(matching_indexes, vec![19_999]);
    assert_eq!(position, 19_999);
    assert_eq!(rules.revision(), LocalRuleSetRevision(4));
    assert!(serialized.contains(replacement.as_str()));
    assert!(!serialized.contains(target.as_str()));
}

#[test]
fn uninitialized_rules_have_revision_zero_and_no_yaml_document() {
    let rules = LocalRuleSet::uninitialized();

    assert_eq!(rules.revision(), LocalRuleSetRevision(0));
    assert_eq!(rules.to_yaml(), Err(RuleDocumentError::RulesUninitialized));
}
