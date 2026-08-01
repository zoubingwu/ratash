use hopash::application::{ApplicationService, Clock};
use hopash::contract::{JsonEnvelope, StatusViewV1};
use hopash::domain::{
    ApplyState, CoreDiagnosticCategory, CoreLifecycle, CoreRestartStatus, RuntimeApplyPhase,
    RuntimeApplySnapshot, RuntimeGeneration, RuntimeRecoverySnapshot, RuntimeRecoveryStatus,
    SupervisorHealthReason, SupervisorLifecycle,
};
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
                    "uptime_seconds": 42,
                    "health_reasons": []
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
                "runtime_apply": {
                    "phase": "idle",
                    "recovery": {
                        "status": "not_required"
                    }
                },
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
fn supervisor_health_reasons_serialize_in_stable_cause_order() {
    let mut status = ApplicationService::new().status();
    status.supervisor.lifecycle = SupervisorLifecycle::Degraded;
    status.supervisor.health_reasons = vec![
        SupervisorHealthReason::RuntimeRecovery,
        SupervisorHealthReason::SelectionCompensation,
        SupervisorHealthReason::ConfigurationProjection,
        SupervisorHealthReason::ProbeScheduler,
        SupervisorHealthReason::SelectionRestoration,
    ];

    let value = serde_json::to_value(StatusViewV1::from(status))
        .expect("Supervisor health reasons should serialize");

    assert_eq!(
        value["supervisor"]["health_reasons"],
        json!([
            "runtime_recovery",
            "selection_compensation",
            "configuration_projection",
            "probe_scheduler",
            "selection_restoration"
        ])
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

#[test]
fn runtime_apply_status_serializes_generations_phase_and_recovery() {
    let mut status = ApplicationService::new().status();
    status.apply_state = ApplyState::Recovering;
    status.runtime_apply = RuntimeApplySnapshot {
        candidate_generation: Some(RuntimeGeneration(9)),
        committed_generation: Some(RuntimeGeneration(8)),
        phase: RuntimeApplyPhase::Recovering,
        recovery: RuntimeRecoverySnapshot {
            status: RuntimeRecoveryStatus::Pending,
            restored_generation: Some(RuntimeGeneration(8)),
            message: Some("Committed Runtime Generation cleanup is pending".to_owned()),
        },
    };

    let value = serde_json::to_value(StatusViewV1::from(status))
        .expect("Runtime Apply status should serialize");

    assert_eq!(value["apply_state"], json!("recovering"));
    assert_eq!(
        value["runtime_apply"],
        json!({
            "candidate_generation": "9",
            "committed_generation": "8",
            "phase": "recovering",
            "recovery": {
                "status": "pending",
                "restored_generation": "8",
                "message": "Committed Runtime Generation cleanup is pending"
            }
        })
    );
}

#[test]
fn stopped_supervisor_serializes_the_final_v1_lifecycle() {
    let mut status = ApplicationService::new().status();
    status.supervisor.lifecycle = SupervisorLifecycle::Stopped;
    status.core.lifecycle = CoreLifecycle::Stopped;

    let value = serde_json::to_value(StatusViewV1::from(status))
        .expect("stopped status should serialize through the public contract");

    assert_eq!(value["supervisor"]["lifecycle"], json!("stopped"));
    assert_eq!(value["core"]["lifecycle"], json!("stopped"));
}
