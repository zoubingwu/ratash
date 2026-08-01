use hopash::application::{ApplicationService, Clock};
use hopash::contract::{JsonEnvelope, StatusViewV1};
use hopash::domain::{CoreDiagnosticCategory, CoreLifecycle, CoreRestartStatus};
use serde_json::json;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug)]
struct TestClock {
    now_unix_ms: AtomicU64,
}

impl TestClock {
    fn new(now_unix_ms: u64) -> Self {
        Self {
            now_unix_ms: AtomicU64::new(now_unix_ms),
        }
    }

    fn set(&self, now_unix_ms: u64) {
        self.now_unix_ms.store(now_unix_ms, Ordering::SeqCst);
    }
}

impl Clock for TestClock {
    fn now_unix_ms(&self) -> u64 {
        self.now_unix_ms.load(Ordering::SeqCst)
    }
}

#[test]
fn zero_profile_status_serializes_the_complete_v1_contract() {
    let clock = Arc::new(TestClock::new(1_785_513_600_000));
    let application = ApplicationService::with_clock(clock.clone());
    clock.set(1_785_513_642_000);

    let envelope = JsonEnvelope::success(StatusViewV1::from(application.status()));
    let value = serde_json::to_value(envelope).expect("status envelope should serialize");

    assert_eq!(
        value,
        json!({
            "schema_version": 1,
            "data": {
                "supervisor": {
                    "lifecycle": "ready",
                    "started_at_unix_ms": "1785513600000",
                    "uptime_seconds": 42
                },
                "core": {
                    "lifecycle": "unconfigured",
                    "restart": {
                        "pending": false,
                        "attempts": 0
                    }
                },
                "tun": {
                    "requested": true,
                    "capable": false,
                    "effective": false,
                    "reason": "no_active_profile"
                },
                "traffic": {
                    "upload_bytes_per_second": 0,
                    "download_bytes_per_second": 0,
                    "state": "unavailable"
                },
                "connection_count": 0,
                "apply_state": "idle",
                "selection_restore_pending": false,
                "probe_queue": {
                    "active_node_count": 0,
                    "queue_depth": 0,
                    "in_flight_count": 0,
                    "overloaded": false,
                    "estimated_full_pass_duration_ms": 0,
                    "stale_ratio": 0.0
                },
                "stream_health": {
                    "traffic": "disconnected",
                    "connections": "disconnected",
                    "logs": "disconnected"
                }
            }
        })
    );
}

#[test]
fn core_restart_status_serializes_stable_public_diagnostics() {
    let mut status = ApplicationService::new().status();
    status.core.lifecycle = CoreLifecycle::Starting;
    status.core.restart = CoreRestartStatus {
        pending: true,
        attempts: 2,
        backoff_ms: Some(4_000),
        diagnostic: Some(CoreDiagnosticCategory::RestartLimitReached),
    };

    let value = serde_json::to_value(StatusViewV1::from(status))
        .expect("status should serialize through the public contract");

    assert_eq!(
        value["core"],
        json!({
            "lifecycle": "starting",
            "restart": {
                "pending": true,
                "attempts": 2,
                "backoff_ms": 4000,
                "diagnostic": "core_restart_limit_reached"
            }
        })
    );
}
