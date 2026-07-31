use hopash::application::{
    ApplicationError, ApplicationOutput, LatencyFreshness, LatencyListOutcome, LatencyProbeStatus,
    LatencyShowOutcome, LatencySummary, LifecycleAction, LifecycleOutcome, LogGap, LogMetadata,
    PolicyTargetValidation, ProfileListOutcome, ProfileMutationAction, ProfileMutationOutcome,
    ProfileRefreshFailure, ProfileRefreshStage, ProfileRefreshState, ProfileSummary,
    ProxyAvailability, ProxyGroupSummary, ProxyListOutcome, ProxyMemberKind, ProxyNodeRow,
    ProxySelectionOutcome, RecoveryOutcome, RecoveryStatus, RuleListOutcome, RuleMutationAction,
    RuleMutationOutcome, RuleSummary, RuntimeApplyOutcome, RuntimeApplyStatus, SelectorCandidate,
    SelectorIdentity, SelectorKind,
};
use hopash::contract::{ApiError, ApplicationOutputViewV1, JsonEnvelope};
use hopash::domain::{
    LocalRuleSetRevision, NodeRecordId, ProbeGeneration, ProfileId, RuntimeGeneration,
    SubscriptionUrl,
};
use hopash::error::ErrorCode;

#[test]
fn lifecycle_and_profile_outputs_use_versioned_safe_json() {
    let lifecycle = JsonEnvelope::success(ApplicationOutputViewV1::from(
        ApplicationOutput::Lifecycle(LifecycleOutcome {
            action: LifecycleAction::Start,
            changed: true,
            status: hopash::application::ApplicationService::new().status(),
        }),
    ));
    let lifecycle_json =
        serde_json::to_value(lifecycle).expect("lifecycle output should serialize");
    assert_eq!(lifecycle_json["schema_version"], 1);
    assert_eq!(lifecycle_json["data"]["action"], "start");
    assert_eq!(lifecycle_json["data"]["changed"], true);
    assert_eq!(
        lifecycle_json["data"]["status"]["core"]["lifecycle"],
        "unconfigured"
    );

    let profile = profile_summary();
    let profile_list = JsonEnvelope::success(ApplicationOutputViewV1::from(
        ApplicationOutput::Profiles(ProfileListOutcome {
            profiles: vec![profile.clone()],
        }),
    ));
    let profile_json = serde_json::to_value(profile_list).expect("Profile output should serialize");
    assert_eq!(
        profile_json["data"]["profiles"][0]["refresh_state"],
        "error"
    );
    assert_eq!(
        profile_json["data"]["profiles"][0]["last_error"]["stage"],
        "download"
    );
    let serialized = serde_json::to_string(&profile_json).expect("JSON value should serialize");
    assert!(!serialized.contains("subscription-secret"));
    assert!(serialized.contains("[redacted]"));

    let mutation = JsonEnvelope::success(ApplicationOutputViewV1::from(
        ApplicationOutput::ProfileMutation(ProfileMutationOutcome {
            action: ProfileMutationAction::Activated,
            profile,
            runtime_apply: Some(applied_runtime()),
        }),
    ));
    let mutation_json = serde_json::to_value(mutation).expect("mutation should serialize");
    assert_eq!(mutation_json["data"]["action"], "activated");
    assert_eq!(mutation_json["data"]["runtime_apply"]["status"], "applied");
    assert_eq!(
        mutation_json["data"]["runtime_apply"]["candidate_generation"],
        "8"
    );
}

#[test]
fn proxy_and_latency_outputs_preserve_resolution_and_probe_state() {
    let candidate_a = NodeRecordId::for_provider("provider-a", "Shared");
    let candidate_b = NodeRecordId::for_provider("provider-b", "Shared");
    let proxies = JsonEnvelope::success(ApplicationOutputViewV1::from(ApplicationOutput::Proxies(
        ProxyListOutcome {
            group: ProxyGroupSummary {
                name: "Main".to_owned(),
                proxy_type: "Selector".to_owned(),
                selectable: true,
                selected_node: Some(SelectorIdentity {
                    id: candidate_a.as_str().to_owned(),
                    name: "Shared".to_owned(),
                }),
            },
            nodes: vec![ProxyNodeRow {
                id: None,
                name: "Shared".to_owned(),
                member_kind: ProxyMemberKind::Ambiguous,
                source: None,
                candidate_ids: vec![candidate_a.clone(), candidate_b],
                proxy_type: None,
                availability: ProxyAvailability::Unavailable,
                selected: true,
                delay_ms: None,
                sampled_at_unix_ms: None,
                freshness: LatencyFreshness::Unavailable,
                probe_status: LatencyProbeStatus::NotSampled,
            }],
        },
    )));
    let proxy_json = serde_json::to_value(proxies).expect("Proxy output should serialize");
    assert_eq!(proxy_json["data"]["nodes"][0]["member_kind"], "ambiguous");
    assert_eq!(
        proxy_json["data"]["nodes"][0]["candidate_ids"][0],
        candidate_a.as_str()
    );
    assert_eq!(
        proxy_json["data"]["nodes"][0]["probe_status"],
        "not_sampled"
    );

    let selection = JsonEnvelope::success(ApplicationOutputViewV1::from(
        ApplicationOutput::ProxySelection(ProxySelectionOutcome {
            group: "Main".to_owned(),
            previous_node: None,
            selected_node: SelectorIdentity {
                id: candidate_a.as_str().to_owned(),
                name: "Shared".to_owned(),
            },
            persisted: true,
            recovery: RecoveryOutcome {
                status: RecoveryStatus::NotRequired,
                restored_generation: None,
                message: None,
            },
        }),
    ));
    let selection_json = serde_json::to_value(selection).expect("selection should serialize");
    assert_eq!(selection_json["data"]["group"], "Main");
    assert_eq!(selection_json["data"]["persisted"], true);
    assert_eq!(selection_json["data"]["recovery"]["status"], "not_required");

    let sample = latency_summary();
    let latency_list = JsonEnvelope::success(ApplicationOutputViewV1::from(
        ApplicationOutput::Latencies(LatencyListOutcome {
            samples: vec![sample.clone()],
        }),
    ));
    let latency_json = serde_json::to_value(latency_list).expect("latency list should serialize");
    assert_eq!(latency_json["data"]["samples"][0]["delay_ms"], 42);
    assert_eq!(latency_json["data"]["samples"][0]["freshness"], "fresh");
    assert_eq!(latency_json["data"]["samples"][0]["probe_generation"], "3");

    let latency_show = JsonEnvelope::success(ApplicationOutputViewV1::from(
        ApplicationOutput::Latency(LatencyShowOutcome { sample }),
    ));
    let show_json = serde_json::to_value(latency_show).expect("latency show should serialize");
    assert_eq!(show_json["data"]["sample"]["node_name"], "Fast Node");
}

#[test]
fn rule_apply_recovery_and_log_metadata_have_explicit_machine_fields() {
    let rule = RuleSummary {
        index: 4,
        rule_string: "DOMAIN,example.com,Main".to_owned(),
        rule_type: "DOMAIN".to_owned(),
        payload: Some("example.com".to_owned()),
        policy_target: "Main".to_owned(),
        params: Vec::new(),
        policy_target_validation: PolicyTargetValidation::Valid,
    };
    let rules = JsonEnvelope::success(ApplicationOutputViewV1::from(ApplicationOutput::Rules(
        RuleListOutcome {
            initialized: true,
            revision: Some(LocalRuleSetRevision(11)),
            rules: vec![rule.clone()],
        },
    )));
    let rule_json = serde_json::to_value(rules).expect("rule list should serialize");
    assert_eq!(rule_json["data"]["revision"], "11");
    assert_eq!(rule_json["data"]["rules"][0]["index"], 4);
    assert_eq!(
        rule_json["data"]["rules"][0]["policy_target_validation"],
        "valid"
    );

    let recovered = RuntimeApplyOutcome {
        status: RuntimeApplyStatus::Recovered,
        candidate_generation: Some(RuntimeGeneration(12)),
        committed_generation: Some(RuntimeGeneration(11)),
        recovery: RecoveryOutcome {
            status: RecoveryStatus::Succeeded,
            restored_generation: Some(RuntimeGeneration(11)),
            message: Some("Previous generation restored".to_owned()),
        },
    };
    let mutation = JsonEnvelope::success(ApplicationOutputViewV1::from(
        ApplicationOutput::RuleMutation(RuleMutationOutcome {
            action: RuleMutationAction::Replaced,
            changed_rule: rule.rule_string,
            previous_rule: Some("DOMAIN,old.example,Main".to_owned()),
            resulting_position: Some(4),
            runtime_apply: recovered,
        }),
    ));
    let mutation_json = serde_json::to_value(mutation).expect("rule mutation should serialize");
    assert_eq!(
        mutation_json["data"]["runtime_apply"]["status"],
        "recovered"
    );
    assert_eq!(
        mutation_json["data"]["runtime_apply"]["recovery"]["restored_generation"],
        "11"
    );

    let metadata = JsonEnvelope::success(ApplicationOutputViewV1::from(
        ApplicationOutput::LogMetadata(LogMetadata {
            first_sequence: Some(101),
            last_sequence: Some(120),
            next_sequence: Some(121),
            dropped_total: 100,
            gap: Some(LogGap {
                requested_after_sequence: 80,
                first_available_sequence: 101,
                dropped_count: 20,
            }),
        }),
    ));
    let metadata_json = serde_json::to_value(metadata).expect("log metadata should serialize");
    assert_eq!(metadata_json["data"]["first_sequence"], "101");
    assert_eq!(metadata_json["data"]["dropped_total"], 100);
    assert_eq!(metadata_json["data"]["gap"]["dropped_count"], 20);
}

#[test]
fn selector_errors_include_kind_names_and_legacy_candidate_ids() {
    let error = ApplicationError::new(
        ErrorCode::ProfileAmbiguous,
        "Profile name is ambiguous",
        false,
    )
    .with_selector_candidates(
        SelectorKind::Profile,
        vec![
            SelectorCandidate::new("profile-a", "Shared"),
            SelectorCandidate::new("profile-b", "Shared"),
        ],
    );

    let api_error = ApiError::from(error);
    assert_eq!(api_error.details.as_ref().unwrap()["selector"], "profile");
    assert_eq!(
        api_error.details.as_ref().unwrap()["candidate_ids"],
        serde_json::json!(["profile-a", "profile-b"])
    );
    assert_eq!(
        api_error.details.as_ref().unwrap()["candidates"][0],
        serde_json::json!({"id": "profile-a", "name": "Shared"})
    );
}

fn profile_summary() -> ProfileSummary {
    ProfileSummary {
        id: ProfileId::parse("550e8400-e29b-41d4-a716-446655440000")
            .expect("fixture Profile ID should parse"),
        name: "Primary".to_owned(),
        subscription_url: SubscriptionUrl::parse(
            "https://user:password@example.com/subscription-secret.yaml?token=value",
        )
        .expect("fixture Subscription URL should parse"),
        active: true,
        refresh_state: ProfileRefreshState::Error,
        last_success_at_unix_ms: 1_700_000_000_000,
        next_refresh_at_unix_ms: 1_700_021_600_000,
        last_error: Some(ProfileRefreshFailure {
            stage: ProfileRefreshStage::Download,
            message: "Subscription source timed out".to_owned(),
        }),
    }
}

fn latency_summary() -> LatencySummary {
    LatencySummary {
        node_id: NodeRecordId::for_core("Fast Node"),
        node_name: "Fast Node".to_owned(),
        delay_ms: Some(42),
        sampled_at_unix_ms: Some(1_700_000_000_000),
        freshness: LatencyFreshness::Fresh,
        probe_status: LatencyProbeStatus::Succeeded,
        probe_generation: ProbeGeneration(3),
    }
}

fn applied_runtime() -> RuntimeApplyOutcome {
    RuntimeApplyOutcome {
        status: RuntimeApplyStatus::Applied,
        candidate_generation: Some(RuntimeGeneration(8)),
        committed_generation: Some(RuntimeGeneration(8)),
        recovery: RecoveryOutcome {
            status: RecoveryStatus::NotRequired,
            restored_generation: None,
            message: None,
        },
    }
}
