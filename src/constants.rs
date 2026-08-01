use std::time::Duration;

pub const PROFILE_REFRESH_INTERVAL: Duration = Duration::from_secs(6 * 60 * 60);
pub const PROFILE_REFRESH_CONCURRENCY: usize = 2;
pub const PROFILE_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
pub const PROFILE_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
pub const PROFILE_TOTAL_TIMEOUT: Duration = Duration::from_secs(30);
pub const PROFILE_REDIRECT_LIMIT: usize = 5;
pub const PROFILE_METADATA_NAME_MAX_BYTES: usize = 80;
pub const PROFILE_COUNT_MAX: usize = 1_000;
pub const SUBSCRIPTION_URL_MAX_BYTES: usize = 8 * 1_024;
pub const SUPERVISOR_STATE_MAX_BYTES: usize = 16 * 1024 * 1024;
pub const EFFECTIVE_CONFIGURATION_MAX_BYTES: usize = 64 * 1024 * 1024;

pub const CORE_READINESS_TIMEOUT: Duration = Duration::from_secs(10);
pub const CORE_HEALTH_TIMEOUT: Duration = Duration::from_secs(5);
pub const CORE_PROCESS_STOP_TIMEOUT: Duration = Duration::from_secs(5);
pub const CORE_RESTART_LIMIT: usize = 3;
pub const CORE_RESTART_INITIAL_BACKOFF: Duration = Duration::from_secs(1);
pub const CORE_RESTART_MAX_BACKOFF: Duration = Duration::from_secs(30);
pub const CORE_SERVICE_LIVENESS_INTERVAL: Duration = Duration::from_secs(5);
pub const MIHOMO_VALIDATION_TIMEOUT: Duration = Duration::from_secs(10);
pub const MIHOMO_BINARY_MAX_BYTES: usize = 128 * 1024 * 1024;
pub const MIHOMO_VALIDATION_OUTPUT_MAX_BYTES: usize = 256 * 1024;

pub const PROBE_WORKER_COUNT: usize = 16;
pub const PROBE_INTERVAL: Duration = Duration::from_secs(5 * 60);
pub const PROBE_TIMEOUT: Duration = Duration::from_secs(5);
pub const LATENCY_FRESHNESS: Duration = Duration::from_secs(10 * 60);
pub const PROBE_URL: &str = "https://www.gstatic.com/generate_204";
pub const MAX_ACTIVE_NODES: usize = 10_000;
pub const SELECTION_RESTORE_ATTEMPT_LIMIT: usize = 10;

pub const STATUS_SAMPLE_INTERVAL: Duration = Duration::from_secs(1);
pub const STREAM_STALE_TIMEOUT: Duration = Duration::from_secs(10);
pub const RECONNECT_INITIAL_BACKOFF: Duration = Duration::from_millis(250);
pub const RECONNECT_MAX_BACKOFF: Duration = Duration::from_secs(10);
pub const CORE_SERVICE_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
pub const CORE_SERVICE_MUTATION_TIMEOUT: Duration = Duration::from_secs(40);
pub const IPC_DEADLINE_LAYER_MARGIN: Duration = Duration::from_secs(5);
pub const IPC_REQUEST_TIMEOUT: Duration = Duration::from_secs(15);
pub const IPC_RUNTIME_MUTATION_TIMEOUT: Duration = Duration::from_secs(55);
pub const IPC_PROFILE_ADD_TIMEOUT: Duration = Duration::from_secs(95);
pub const DAEMON_STARTUP_TIMEOUT: Duration = Duration::from_secs(90);
pub const DAEMON_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);
pub const DAEMON_POLL_INTERVAL: Duration = Duration::from_millis(25);

pub const LOG_CAPACITY: usize = 10_000;
pub const LOG_SUBSCRIBER_CAPACITY: usize = 256;
pub const STATUS_SUBSCRIBER_CAPACITY: usize = 64;
pub const TRAFFIC_SERIES_CAPACITY: usize = 300;

pub const PROFILE_RESPONSE_MAX_BYTES: usize = 16 * 1024 * 1024;
pub const YAML_MAX_DEPTH: usize = 64;
pub const RULE_STRING_MAX_BYTES: usize = 16 * 1024;
pub const LOCAL_RULE_SET_MAX_BYTES: usize = 32 * 1024 * 1024;
pub const LOCAL_RULE_COUNT_MAX: usize = 20_000;
pub const CORE_LOG_LINE_MAX_BYTES: usize = 64 * 1024;
pub const JSON_OUTPUT_MAX_BYTES: usize = 128 * 1024 * 1024;
pub const IPC_FRAME_MAX_BYTES: usize = JSON_OUTPUT_MAX_BYTES + 4 * 1024 * 1024;
pub const IPC_REQUEST_FRAME_MAX_BYTES: usize = 256 * 1024;
pub const IPC_LIST_PAGE_SIZE: usize = 128;

pub const MINIMUM_TERMINAL_WIDTH: u16 = 80;
pub const MINIMUM_TERMINAL_HEIGHT: u16 = 24;

const _: () = {
    assert!(
        IPC_REQUEST_TIMEOUT.as_millis()
            >= CORE_SERVICE_REQUEST_TIMEOUT.as_millis()
                + IPC_DEADLINE_LAYER_MARGIN.as_millis()
    );
    assert!(
        IPC_RUNTIME_MUTATION_TIMEOUT.as_millis()
            >= CORE_SERVICE_MUTATION_TIMEOUT.as_millis()
                + IPC_DEADLINE_LAYER_MARGIN.as_millis()
    );
    assert!(
        IPC_PROFILE_ADD_TIMEOUT.as_millis()
            >= PROFILE_TOTAL_TIMEOUT.as_millis()
                + IPC_RUNTIME_MUTATION_TIMEOUT.as_millis()
                + IPC_DEADLINE_LAYER_MARGIN.as_millis()
    );
    assert!(
        DAEMON_STARTUP_TIMEOUT.as_millis()
            >= CORE_SERVICE_MUTATION_TIMEOUT.as_millis()
                + CORE_READINESS_TIMEOUT.as_millis()
                + IPC_DEADLINE_LAYER_MARGIN.as_millis()
    );
    assert!(
        DAEMON_SHUTDOWN_TIMEOUT.as_millis()
            >= CORE_PROCESS_STOP_TIMEOUT.as_millis()
                + IPC_DEADLINE_LAYER_MARGIN.as_millis()
    );
};
