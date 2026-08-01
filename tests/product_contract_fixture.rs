use hopash::config::BUNDLED_CORE_VERSION;
use hopash::constants::*;
use hopash::contract::SCHEMA_VERSION;
use hopash::core::PROXY_VIEW_SCHEMA_VERSION;
use hopash::error::ProcessExitCode;
use hopash::ipc::IPC_PROTOCOL_VERSION;
use serde_json::Value;

fn fixture() -> Value {
    serde_json::from_str(include_str!("../fixtures/release/product-contract-v1.json"))
        .expect("the release product contract should be valid JSON")
}

fn duration_ms(value: std::time::Duration) -> u64 {
    value
        .as_millis()
        .try_into()
        .expect("product durations should fit in u64 milliseconds")
}

#[test]
fn release_fixture_freezes_versions_and_user_observable_intervals() {
    let contract = fixture();
    assert_eq!(contract["schema_version"], 1);
    assert_eq!(contract["mihomo_version"], BUNDLED_CORE_VERSION);
    assert_eq!(contract["protocol_versions"]["cli_json"], SCHEMA_VERSION);
    assert_eq!(contract["protocol_versions"]["ipc"], IPC_PROTOCOL_VERSION);
    assert_eq!(
        contract["protocol_versions"]["proxy_view"],
        PROXY_VIEW_SCHEMA_VERSION
    );

    let intervals = &contract["intervals_ms"];
    for (name, actual) in [
        ("profile_refresh", PROFILE_REFRESH_INTERVAL),
        ("profile_connect_timeout", PROFILE_CONNECT_TIMEOUT),
        ("profile_request_timeout", PROFILE_REQUEST_TIMEOUT),
        ("profile_total_timeout", PROFILE_TOTAL_TIMEOUT),
        ("core_readiness_timeout", CORE_READINESS_TIMEOUT),
        ("core_health_timeout", CORE_HEALTH_TIMEOUT),
        ("core_restart_initial_backoff", CORE_RESTART_INITIAL_BACKOFF),
        ("core_restart_max_backoff", CORE_RESTART_MAX_BACKOFF),
        (
            "core_service_liveness_interval",
            CORE_SERVICE_LIVENESS_INTERVAL,
        ),
        ("mihomo_validation_timeout", MIHOMO_VALIDATION_TIMEOUT),
        ("probe_interval", PROBE_INTERVAL),
        ("probe_timeout", PROBE_TIMEOUT),
        ("latency_freshness", LATENCY_FRESHNESS),
        ("status_sample_interval", STATUS_SAMPLE_INTERVAL),
        ("stream_stale_timeout", STREAM_STALE_TIMEOUT),
        ("reconnect_initial_backoff", RECONNECT_INITIAL_BACKOFF),
        ("reconnect_max_backoff", RECONNECT_MAX_BACKOFF),
        ("ipc_request_timeout", IPC_REQUEST_TIMEOUT),
        ("ipc_runtime_mutation_timeout", IPC_RUNTIME_MUTATION_TIMEOUT),
        ("ipc_profile_add_timeout", IPC_PROFILE_ADD_TIMEOUT),
    ] {
        assert_eq!(intervals[name], duration_ms(actual), "{name} drifted");
    }
}

#[test]
fn release_fixture_freezes_capacities_and_input_boundaries() {
    let contract = fixture();
    let capacities = &contract["capacities"];
    for (name, actual) in [
        ("profile_refresh_concurrency", PROFILE_REFRESH_CONCURRENCY),
        ("profile_redirect_limit", PROFILE_REDIRECT_LIMIT),
        ("profile_count", PROFILE_COUNT_MAX),
        ("core_restart_limit", CORE_RESTART_LIMIT),
        ("probe_workers", PROBE_WORKER_COUNT),
        (
            "selection_restore_attempts",
            SELECTION_RESTORE_ATTEMPT_LIMIT,
        ),
        ("active_nodes", MAX_ACTIVE_NODES),
        ("logs", LOG_CAPACITY),
        ("log_subscriber", LOG_SUBSCRIBER_CAPACITY),
        ("status_subscriber", STATUS_SUBSCRIBER_CAPACITY),
        ("traffic_series", TRAFFIC_SERIES_CAPACITY),
        ("local_rules", LOCAL_RULE_COUNT_MAX),
    ] {
        assert_eq!(capacities[name], actual, "{name} drifted");
    }

    let byte_limits = &contract["byte_limits"];
    for (name, actual) in [
        ("profile_metadata_name", PROFILE_METADATA_NAME_MAX_BYTES),
        ("subscription_url", SUBSCRIPTION_URL_MAX_BYTES),
        ("supervisor_state", SUPERVISOR_STATE_MAX_BYTES),
        ("effective_configuration", EFFECTIVE_CONFIGURATION_MAX_BYTES),
        ("mihomo_binary", MIHOMO_BINARY_MAX_BYTES),
        (
            "mihomo_validation_output",
            MIHOMO_VALIDATION_OUTPUT_MAX_BYTES,
        ),
        ("profile_response", PROFILE_RESPONSE_MAX_BYTES),
        ("rule_string", RULE_STRING_MAX_BYTES),
        ("local_rule_set", LOCAL_RULE_SET_MAX_BYTES),
        ("ipc_frame", IPC_FRAME_MAX_BYTES),
        ("ipc_request_frame", IPC_REQUEST_FRAME_MAX_BYTES),
        ("core_log_line", CORE_LOG_LINE_MAX_BYTES),
        ("json_output", JSON_OUTPUT_MAX_BYTES),
    ] {
        assert_eq!(byte_limits[name], actual, "{name} drifted");
    }

    assert_eq!(contract["other_limits"]["yaml_depth"], YAML_MAX_DEPTH);
    assert_eq!(
        contract["other_limits"]["ipc_list_page"],
        IPC_LIST_PAGE_SIZE
    );
    assert_eq!(
        contract["other_limits"]["minimum_terminal_width"],
        MINIMUM_TERMINAL_WIDTH
    );
    assert_eq!(
        contract["other_limits"]["minimum_terminal_height"],
        MINIMUM_TERMINAL_HEIGHT
    );
    assert_eq!(contract["probe_url"], PROBE_URL);
}

#[test]
fn release_fixture_freezes_process_exit_codes() {
    let exit_codes = &fixture()["exit_codes"];
    for (name, actual) in [
        ("success", ProcessExitCode::Success),
        ("usage", ProcessExitCode::Usage),
        (
            "supervisor_unavailable",
            ProcessExitCode::SupervisorUnavailable,
        ),
        ("domain_conflict", ProcessExitCode::DomainConflict),
        (
            "external_operation_failure",
            ProcessExitCode::ExternalOperationFailure,
        ),
        ("internal_failure", ProcessExitCode::InternalFailure),
        ("interrupted", ProcessExitCode::Interrupted),
    ] {
        assert_eq!(exit_codes[name], actual.as_u8(), "{name} drifted");
    }
}
