use ratash::constants::*;
use ratash::process_controller::NativeCoreProcessConfig;
use ratash::service::PrivilegedServiceConfig;
use std::path::PathBuf;

#[test]
fn observable_product_limits_are_frozen_for_the_release_contract() {
    assert_eq!(PROFILE_REFRESH_INTERVAL.as_secs(), 21_600);
    assert_eq!(PROFILE_REFRESH_CONCURRENCY, 2);
    assert_eq!(PROFILE_CONNECT_TIMEOUT.as_secs(), 15);
    assert_eq!(PROFILE_REQUEST_TIMEOUT.as_secs(), 30);
    assert_eq!(PROFILE_TOTAL_TIMEOUT.as_secs(), 120);
    assert_eq!(PROFILE_REDIRECT_LIMIT, 5);
    assert_eq!(PROFILE_METADATA_NAME_MAX_BYTES, 80);
    assert_eq!(PROFILE_COUNT_MAX, 1_000);
    assert_eq!(SUBSCRIPTION_URL_MAX_BYTES, 8_192);
    assert_eq!(SUPERVISOR_STATE_MAX_BYTES, 16 * 1_024 * 1_024);
    assert_eq!(EFFECTIVE_CONFIGURATION_MAX_BYTES, 64 * 1_024 * 1_024);
    assert_eq!(MIHOMO_VALIDATION_TIMEOUT.as_secs(), 10);
    assert_eq!(CORE_PROCESS_STOP_TIMEOUT.as_secs(), 5);
    assert_eq!(MIHOMO_BINARY_MAX_BYTES, 128 * 1_024 * 1_024);
    assert_eq!(MIHOMO_VALIDATION_OUTPUT_MAX_BYTES, 256 * 1_024);
    assert_eq!(PROBE_WORKER_COUNT, 16);
    assert_eq!(PROBE_INTERVAL.as_secs(), 300);
    assert_eq!(PROBE_TIMEOUT.as_secs(), 5);
    assert_eq!(LATENCY_FRESHNESS.as_secs(), 600);
    assert_eq!(STATUS_SAMPLE_INTERVAL.as_secs(), 1);
    assert_eq!(LOG_CAPACITY, 10_000);
    assert_eq!(LOG_RETENTION_MAX_BYTES, 32 * 1_024 * 1_024);
    assert_eq!(CORE_LOG_FORWARD_CAPACITY, 256);
    assert_eq!(CORE_LOG_FORWARD_MAX_BYTES, 4 * 1_024 * 1_024);
    assert_eq!(CORE_LOG_FORWARD_BATCH_MAX_BYTES, 512 * 1_024);
    assert_eq!(LOG_BROKER_RECOVERY_CAPACITY, 256);
    assert_eq!(LOG_BROKER_RECOVERY_MAX_BYTES, 4 * 1_024 * 1_024);
    assert_eq!(LOG_TAIL_MAX_RECORDS, 256);
    assert_eq!(LOG_TAIL_MAX_BYTES, 4 * 1_024 * 1_024);
    assert_eq!(LOG_SUBSCRIBER_MAX_BYTES, 4 * 1_024 * 1_024);
    assert_eq!(IPC_STREAM_CAPACITY, 3);
    assert_eq!(TRAFFIC_SERIES_CAPACITY, 300);
    assert_eq!(CONNECTION_RECORD_CAPACITY, 256);
    assert_eq!(CONNECTION_CHAIN_CAPACITY, 16);
    assert_eq!(CONNECTION_FIELD_MAX_BYTES, 512);
    assert_eq!(MINIMUM_TERMINAL_WIDTH, 80);
    assert_eq!(MINIMUM_TERMINAL_HEIGHT, 24);
    assert_eq!(TUI_SEARCH_MAX_BYTES, 256);
    assert_eq!(TUI_SEARCH_MAX_CHARACTERS, 128);
    assert_eq!(MAX_ACTIVE_NODES, 10_000);
    assert_eq!(SELECTION_RESTORE_ATTEMPT_LIMIT, 10);
    assert_eq!(LOCAL_RULE_COUNT_MAX, 20_000);
    assert_eq!(PROBE_URL, "https://www.gstatic.com/generate_204");
    assert_eq!(CORE_SERVICE_REQUEST_TIMEOUT.as_secs(), 10);
    assert_eq!(CORE_SERVICE_MUTATION_TIMEOUT.as_secs(), 40);
    assert_eq!(IPC_REQUEST_TIMEOUT.as_secs(), 15);
    assert_eq!(IPC_RUNTIME_MUTATION_TIMEOUT.as_secs(), 55);
    assert_eq!(IPC_PROFILE_ADD_TIMEOUT.as_secs(), 180);
    assert_eq!(DAEMON_STARTUP_TIMEOUT.as_secs(), 90);
    assert_eq!(DAEMON_SHUTDOWN_TIMEOUT.as_secs(), 10);
}

#[test]
fn every_input_and_transport_boundary_has_a_positive_limit() {
    for limit in [
        PROFILE_RESPONSE_MAX_BYTES,
        SUBSCRIPTION_URL_MAX_BYTES,
        SUPERVISOR_STATE_MAX_BYTES,
        EFFECTIVE_CONFIGURATION_MAX_BYTES,
        MIHOMO_BINARY_MAX_BYTES,
        MIHOMO_VALIDATION_OUTPUT_MAX_BYTES,
        YAML_MAX_DEPTH,
        RULE_STRING_MAX_BYTES,
        LOCAL_RULE_SET_MAX_BYTES,
        IPC_FRAME_MAX_BYTES,
        IPC_REQUEST_FRAME_MAX_BYTES,
        CORE_LOG_LINE_MAX_BYTES,
        LOG_RETENTION_MAX_BYTES,
        CORE_LOG_FORWARD_MAX_BYTES,
        CORE_LOG_FORWARD_BATCH_MAX_BYTES,
        LOG_BROKER_RECOVERY_MAX_BYTES,
        LOG_TAIL_MAX_BYTES,
        LOG_SUBSCRIBER_MAX_BYTES,
        CONNECTION_FIELD_MAX_BYTES,
        JSON_OUTPUT_MAX_BYTES,
        TUI_SEARCH_MAX_BYTES,
    ] {
        assert!(limit > 0);
    }
    const {
        assert!(JSON_OUTPUT_MAX_BYTES >= LOCAL_RULE_SET_MAX_BYTES * 4);
        assert!(IPC_FRAME_MAX_BYTES > JSON_OUTPUT_MAX_BYTES);
        assert!(IPC_REQUEST_FRAME_MAX_BYTES < IPC_FRAME_MAX_BYTES);
        assert!(LOG_BROKER_RECOVERY_CAPACITY < LOG_CAPACITY);
        assert!(CORE_LOG_FORWARD_CAPACITY < LOG_CAPACITY);
        assert!(LOG_TAIL_MAX_RECORDS <= LOG_BROKER_RECOVERY_CAPACITY);
        assert!(CORE_LOG_LINE_MAX_BYTES <= CORE_LOG_FORWARD_MAX_BYTES);
        assert!(CORE_LOG_LINE_MAX_BYTES <= CORE_LOG_FORWARD_BATCH_MAX_BYTES);
        assert!(CORE_LOG_FORWARD_BATCH_MAX_BYTES <= CORE_LOG_FORWARD_MAX_BYTES);
        assert!(CORE_LOG_FORWARD_MAX_BYTES <= LOG_RETENTION_MAX_BYTES);
        assert!(LOG_BROKER_RECOVERY_MAX_BYTES <= LOG_RETENTION_MAX_BYTES);
        assert!(LOG_TAIL_MAX_BYTES < IPC_FRAME_MAX_BYTES);
        assert!(LOG_SUBSCRIBER_MAX_BYTES <= LOG_RETENTION_MAX_BYTES);
        assert!(IPC_LIST_PAGE_SIZE < LOCAL_RULE_COUNT_MAX);
    }
}

#[test]
fn per_process_log_payload_envelopes_are_explicit() {
    const JSON_ESCAPE_EXPANSION: usize = 6;
    const RECORD_ENVELOPE_MAX_BYTES: usize = 256;
    const FORWARDED_BATCH_ENCODED_MAX_BYTES: usize = CORE_LOG_FORWARD_BATCH_MAX_BYTES
        * JSON_ESCAPE_EXPANSION
        + CORE_LOG_FORWARD_CAPACITY * RECORD_ENVELOPE_MAX_BYTES;
    const PRIVILEGED_SERVICE_MAX_BYTES: usize = CORE_LOG_FORWARD_MAX_BYTES * 2
        + CORE_LOG_FORWARD_BATCH_MAX_BYTES
        + FORWARDED_BATCH_ENCODED_MAX_BYTES;
    const SUPERVISOR_MAX_BYTES: usize = LOG_RETENTION_MAX_BYTES
        + LOG_BROKER_RECOVERY_MAX_BYTES
        + IPC_STREAM_CAPACITY * LOG_SUBSCRIBER_MAX_BYTES
        + 2 * LOG_TAIL_MAX_BYTES
        + CORE_LOG_FORWARD_BATCH_MAX_BYTES
        + FORWARDED_BATCH_ENCODED_MAX_BYTES;
    const STATUS_INTERFACE_MAX_BYTES: usize =
        LOG_RETENTION_MAX_BYTES + LOG_SUBSCRIBER_MAX_BYTES + 2 * LOG_TAIL_MAX_BYTES;

    const {
        assert!(FORWARDED_BATCH_ENCODED_MAX_BYTES < LOG_TAIL_MAX_BYTES);
    }
    assert_eq!(PRIVILEGED_SERVICE_MAX_BYTES, 12_124_160);
    assert_eq!(SUPERVISOR_MAX_BYTES, 62_455_808);
    assert_eq!(STATUS_INTERFACE_MAX_BYTES, 44 * 1_024 * 1_024);
}

#[test]
fn privileged_log_forwarders_use_the_release_capacity() {
    assert_eq!(
        NativeCoreProcessConfig::default().log_capacity,
        CORE_LOG_FORWARD_CAPACITY
    );
    let service = PrivilegedServiceConfig::product_defaults(
        PathBuf::from("/fixture/service-root"),
        "a".repeat(64),
        "b".repeat(64),
    );
    assert_eq!(service.log_capacity, CORE_LOG_FORWARD_CAPACITY);
    assert_eq!(service.max_log_line_bytes, CORE_LOG_LINE_MAX_BYTES);
}
