use std::time::Duration;

pub const PROFILE_REFRESH_INTERVAL: Duration = Duration::from_secs(6 * 60 * 60);
pub const PROFILE_REFRESH_CONCURRENCY: usize = 2;

pub const CORE_READINESS_TIMEOUT: Duration = Duration::from_secs(10);
pub const CORE_HEALTH_TIMEOUT: Duration = Duration::from_secs(5);
pub const CORE_RESTART_LIMIT: usize = 3;

pub const PROBE_WORKER_COUNT: usize = 16;
pub const PROBE_INTERVAL: Duration = Duration::from_secs(5 * 60);
pub const PROBE_TIMEOUT: Duration = Duration::from_secs(5);
pub const LATENCY_FRESHNESS: Duration = Duration::from_secs(10 * 60);
pub const PROBE_URL: &str = "https://www.gstatic.com/generate_204";
pub const MAX_ACTIVE_NODES: usize = 10_000;

pub const STATUS_SAMPLE_INTERVAL: Duration = Duration::from_secs(1);
pub const STREAM_STALE_TIMEOUT: Duration = Duration::from_secs(10);
pub const RECONNECT_INITIAL_BACKOFF: Duration = Duration::from_millis(250);
pub const RECONNECT_MAX_BACKOFF: Duration = Duration::from_secs(10);
pub const IPC_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

pub const LOG_CAPACITY: usize = 10_000;
pub const LOG_SUBSCRIBER_CAPACITY: usize = 256;
pub const STATUS_SUBSCRIBER_CAPACITY: usize = 64;
pub const TRAFFIC_SERIES_CAPACITY: usize = 300;

pub const PROFILE_RESPONSE_MAX_BYTES: usize = 16 * 1024 * 1024;
pub const YAML_MAX_DEPTH: usize = 64;
pub const RULE_STRING_MAX_BYTES: usize = 16 * 1024;
pub const LOCAL_RULE_SET_MAX_BYTES: usize = 32 * 1024 * 1024;
pub const LOCAL_RULE_COUNT_MAX: usize = 20_000;
pub const IPC_FRAME_MAX_BYTES: usize = 4 * 1024 * 1024;
pub const CORE_LOG_LINE_MAX_BYTES: usize = 64 * 1024;
pub const JSON_OUTPUT_MAX_BYTES: usize = 64 * 1024 * 1024;

pub const MINIMUM_TERMINAL_WIDTH: u16 = 80;
pub const MINIMUM_TERMINAL_HEIGHT: u16 = 24;
