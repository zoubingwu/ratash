use hopash::constants::*;

#[test]
fn observable_product_limits_are_frozen_for_the_release_contract() {
    assert_eq!(PROFILE_REFRESH_INTERVAL.as_secs(), 21_600);
    assert_eq!(PROFILE_REFRESH_CONCURRENCY, 2);
    assert_eq!(PROBE_WORKER_COUNT, 16);
    assert_eq!(PROBE_INTERVAL.as_secs(), 300);
    assert_eq!(PROBE_TIMEOUT.as_secs(), 5);
    assert_eq!(LATENCY_FRESHNESS.as_secs(), 600);
    assert_eq!(STATUS_SAMPLE_INTERVAL.as_secs(), 1);
    assert_eq!(LOG_CAPACITY, 10_000);
    assert_eq!(TRAFFIC_SERIES_CAPACITY, 300);
    assert_eq!(MINIMUM_TERMINAL_WIDTH, 80);
    assert_eq!(MINIMUM_TERMINAL_HEIGHT, 24);
    assert_eq!(MAX_ACTIVE_NODES, 10_000);
    assert_eq!(LOCAL_RULE_COUNT_MAX, 20_000);
    assert_eq!(PROBE_URL, "https://www.gstatic.com/generate_204");
}

#[test]
fn every_input_and_transport_boundary_has_a_positive_limit() {
    for limit in [
        PROFILE_RESPONSE_MAX_BYTES,
        YAML_MAX_DEPTH,
        RULE_STRING_MAX_BYTES,
        LOCAL_RULE_SET_MAX_BYTES,
        IPC_FRAME_MAX_BYTES,
        CORE_LOG_LINE_MAX_BYTES,
        JSON_OUTPUT_MAX_BYTES,
    ] {
        assert!(limit > 0);
    }
}
