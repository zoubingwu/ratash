use ratash::diagnostics::{
    WrapperDiagnosticCategory, WrapperDiagnosticContext, WrapperDiagnosticError,
    WrapperDiagnosticRing, WrapperDiagnosticState,
};
use ratash::domain::{CoreInstanceGeneration, RuntimeGeneration, SupervisorHealthReason};

#[test]
fn supervisor_health_reasons_map_to_cause_scoped_categories() {
    let cases = [
        (
            SupervisorHealthReason::RuntimeRecovery,
            WrapperDiagnosticCategory::RuntimeRecovery,
        ),
        (
            SupervisorHealthReason::SelectionCompensation,
            WrapperDiagnosticCategory::SelectionCompensation,
        ),
        (
            SupervisorHealthReason::ConfigurationProjection,
            WrapperDiagnosticCategory::ConfigurationProjection,
        ),
        (
            SupervisorHealthReason::ProbeScheduler,
            WrapperDiagnosticCategory::ProbeScheduler,
        ),
        (
            SupervisorHealthReason::SelectionRestoration,
            WrapperDiagnosticCategory::SelectionRestoration,
        ),
    ];

    for (reason, category) in cases {
        assert_eq!(WrapperDiagnosticCategory::from(reason), category);
    }
}

#[test]
fn diagnostic_ring_evicts_oldest_records_and_reports_a_resync_gap() {
    let mut ring = WrapperDiagnosticRing::new(3).expect("fixture capacity should be valid");
    for revision in 1..=5 {
        ring.record(
            revision * 10,
            WrapperDiagnosticCategory::RuntimeApply,
            WrapperDiagnosticState::Raised,
            WrapperDiagnosticContext {
                runtime_generation: Some(RuntimeGeneration(revision)),
                core_generation: Some(CoreInstanceGeneration(revision)),
                revision: Some(revision),
            },
        )
        .expect("fixture sequence should have capacity");
    }

    let tail = ring
        .tail_after(Some(1), 3)
        .expect("fixture tail limit should be valid");

    assert_eq!(ring.len(), 3);
    assert_eq!(ring.capacity(), 3);
    assert_eq!(ring.evicted_total(), 2);
    assert_eq!(tail.evicted_total, 2);
    assert!(tail.gap);
    assert_eq!(tail.earliest_sequence, Some(3));
    assert_eq!(tail.latest_sequence, Some(5));
    assert_eq!(tail.records[0].context.revision, Some(3));
}

#[test]
fn diagnostic_tail_uses_the_latest_bounded_window_without_cloning_older_records() {
    let mut ring = WrapperDiagnosticRing::new(8).expect("fixture capacity should be valid");
    for revision in 1..=6 {
        ring.record(
            revision,
            WrapperDiagnosticCategory::TelemetryStream,
            WrapperDiagnosticState::Cleared,
            WrapperDiagnosticContext {
                revision: Some(revision),
                ..WrapperDiagnosticContext::default()
            },
        )
        .expect("fixture sequence should have capacity");
    }

    let tail = ring
        .tail_after(Some(1), 2)
        .expect("fixture tail limit should be valid");

    assert!(tail.gap);
    assert_eq!(
        tail.records
            .iter()
            .map(|record| record.sequence)
            .collect::<Vec<_>>(),
        [5, 6]
    );
}

#[test]
fn diagnostic_ring_rejects_zero_capacity_and_zero_tail_limits() {
    assert!(matches!(
        WrapperDiagnosticRing::new(0),
        Err(WrapperDiagnosticError::InvalidLimit)
    ));
    let ring = WrapperDiagnosticRing::new(1).expect("fixture capacity should be valid");
    assert!(ring.is_empty());
    assert_eq!(ring.len(), 0);
    assert!(matches!(
        ring.tail_after(None, 0),
        Err(WrapperDiagnosticError::InvalidLimit)
    ));
}
