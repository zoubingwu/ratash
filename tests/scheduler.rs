use std::cell::Cell;
use std::collections::BTreeSet;

use hopash::constants::{
    LATENCY_FRESHNESS, MAX_ACTIVE_NODES, PROBE_INTERVAL, PROBE_TIMEOUT, PROBE_WORKER_COUNT,
    PROFILE_REFRESH_CONCURRENCY, PROFILE_REFRESH_INTERVAL,
};
use hopash::domain::{NodeRecordId, ProbeGeneration, ProfileId, SampleState};
use hopash::profile::ProfileRevision;
use hopash::scheduler::{
    ProbeCompletion, ProbeCompletionStatus, ProbeOutcome, ProbeResetError, ProbeScheduler,
    ProbeStatus, ProfileRefreshScheduler, RefreshCompletion, RefreshCompletionStatus,
};

#[test]
fn overdue_profiles_dispatch_by_independent_deadline_with_bounded_concurrency() {
    let first = ProfileId::new();
    let second = ProfileId::new();
    let third = ProfileId::new();
    let mut scheduler = ProfileRefreshScheduler::new();
    scheduler.upsert(first, ProfileRevision(1), 300);
    scheduler.upsert(second, ProfileRevision(4), 100);
    scheduler.upsert(third, ProfileRevision(8), 200);

    let first_batch = scheduler.take_due(300);

    assert_eq!(first_batch.len(), PROFILE_REFRESH_CONCURRENCY);
    assert_eq!(first_batch[0].profile_id, second);
    assert_eq!(first_batch[0].profile_revision, ProfileRevision(4));
    assert_eq!(first_batch[1].profile_id, third);
    assert_eq!(scheduler.in_flight_count(), PROFILE_REFRESH_CONCURRENCY);
    assert!(scheduler.take_due(300).is_empty());
    assert_eq!(scheduler.next_deadline(), Some(300));
}

#[test]
fn refresh_completion_carries_revision_and_reschedules_from_completion_time() {
    let profile_id = ProfileId::new();
    let mut scheduler = ProfileRefreshScheduler::new();
    scheduler.upsert(profile_id, ProfileRevision(3), 10);
    let task = scheduler.take_due(10).remove(0);

    let status = scheduler.complete(RefreshCompletion {
        task,
        profile_revision: ProfileRevision(4),
        completed_at_unix_ms: 25,
    });
    let expected_deadline = 25 + duration_ms(PROFILE_REFRESH_INTERVAL);

    assert_eq!(
        status,
        RefreshCompletionStatus::Rescheduled {
            next_refresh_at_unix_ms: expected_deadline,
        }
    );
    assert_eq!(scheduler.in_flight_count(), 0);
    assert_eq!(scheduler.next_deadline(), Some(expected_deadline));
    assert!(scheduler.take_due(expected_deadline - 1).is_empty());
    assert_eq!(
        scheduler.take_due(expected_deadline)[0].profile_revision,
        ProfileRevision(4)
    );
}

#[test]
fn a_profile_has_at_most_one_refresh_in_flight() {
    let profile_id = ProfileId::new();
    let mut scheduler = ProfileRefreshScheduler::new();
    scheduler.upsert(profile_id, ProfileRevision(1), 0);

    let task = scheduler.take_due(0).remove(0);

    assert!(scheduler.take_due(u64::MAX).is_empty());
    assert_eq!(scheduler.in_flight_count(), 1);
    assert_eq!(task.profile_id, profile_id);
}

#[test]
fn refresh_completion_discards_a_changed_profile_revision() {
    let profile_id = ProfileId::new();
    let mut scheduler = ProfileRefreshScheduler::new();
    scheduler.upsert(profile_id, ProfileRevision(1), 0);
    let task = scheduler.take_due(0).remove(0);
    scheduler.upsert(profile_id, ProfileRevision(2), 5);

    let status = scheduler.complete(RefreshCompletion {
        task,
        profile_revision: ProfileRevision(2),
        completed_at_unix_ms: 10,
    });

    assert_eq!(status, RefreshCompletionStatus::StaleRevision);
    let replacement = scheduler.take_due(10).remove(0);
    assert_eq!(replacement.profile_revision, ProfileRevision(2));
    assert_eq!(replacement.due_at_unix_ms, 5);
}

#[test]
fn refresh_completion_releases_capacity_after_profile_removal() {
    let profile_id = ProfileId::new();
    let mut scheduler = ProfileRefreshScheduler::new();
    scheduler.upsert(profile_id, ProfileRevision(1), 0);
    let task = scheduler.take_due(0).remove(0);
    assert!(scheduler.remove(profile_id));

    let status = scheduler.complete(RefreshCompletion {
        task,
        profile_revision: ProfileRevision(1),
        completed_at_unix_ms: 10,
    });

    assert_eq!(status, RefreshCompletionStatus::ProfileRemoved);
    assert_eq!(scheduler.in_flight_count(), 0);
    assert_eq!(scheduler.scheduled_profile_count(), 0);
}

#[test]
fn one_hundred_due_profiles_keep_refresh_work_bounded_and_make_progress() {
    let mut scheduler = ProfileRefreshScheduler::new();
    for _ in 0..100 {
        scheduler.upsert(ProfileId::new(), ProfileRevision(1), 0);
    }

    let mut dispatched = BTreeSet::new();
    let mut peak_in_flight = 0;
    while dispatched.len() < 100 {
        let tasks = scheduler.take_due(0);
        assert!(!tasks.is_empty(), "each due Profile should make progress");
        assert!(tasks.len() <= PROFILE_REFRESH_CONCURRENCY);
        peak_in_flight = peak_in_flight.max(scheduler.in_flight_count());

        for task in tasks {
            assert!(dispatched.insert(task.profile_id));
            assert_eq!(
                scheduler.complete(RefreshCompletion {
                    task,
                    profile_revision: ProfileRevision(1),
                    completed_at_unix_ms: 0,
                }),
                RefreshCompletionStatus::Rescheduled {
                    next_refresh_at_unix_ms: duration_ms(PROFILE_REFRESH_INTERVAL),
                }
            );
        }
    }

    assert_eq!(scheduler.scheduled_profile_count(), 100);
    assert_eq!(scheduler.in_flight_count(), 0);
    assert_eq!(peak_in_flight, PROFILE_REFRESH_CONCURRENCY);
    assert_eq!(
        scheduler.next_deadline(),
        Some(duration_ms(PROFILE_REFRESH_INTERVAL))
    );
}

#[test]
fn probe_generation_deduplicates_nodes_and_enqueues_the_first_pass_immediately() {
    let first = node("first");
    let second = node("second");
    let mut scheduler = ProbeScheduler::new();

    scheduler
        .reset(
            ProbeGeneration(1),
            [first.clone(), second.clone(), first.clone()],
            100,
        )
        .expect("generation should be accepted");

    let metrics = scheduler.metrics(100);
    assert_eq!(metrics.active_node_count, 2);
    assert_eq!(metrics.queue_depth, 2);
    assert_eq!(metrics.in_flight_count, 0);
    assert_eq!(metrics.stale_ratio, 1.0);
    assert_eq!(scheduler.take_due(99), []);
    assert_eq!(scheduler.take_due(100).len(), 2);
}

#[test]
fn probe_queue_prioritizes_the_earliest_rescheduled_deadline() {
    let first = node("first");
    let second = node("second");
    let mut scheduler = ProbeScheduler::new();
    scheduler
        .reset(ProbeGeneration(1), [first.clone(), second.clone()], 0)
        .expect("generation should be accepted");
    let tasks = scheduler.take_due(0);
    let first_task = task_for(&tasks, &first);
    let second_task = task_for(&tasks, &second);
    complete_success(&mut scheduler, first_task, 20, 10);
    complete_success(&mut scheduler, second_task, 10, 20);

    let first_due = 10 + duration_ms(PROBE_INTERVAL);
    let due = scheduler.take_due(first_due);

    assert_eq!(due.len(), 1);
    assert_eq!(due[0].node_id, second);
    assert_eq!(due[0].due_at_unix_ms, first_due);
}

#[test]
fn probe_workers_and_node_membership_remain_bounded_at_ten_thousand_nodes() {
    let nodes = (0..MAX_ACTIVE_NODES)
        .map(|index| node(&format!("node-{index}")))
        .collect::<Vec<_>>();
    let mut scheduler = ProbeScheduler::new();
    scheduler
        .reset(ProbeGeneration(1), nodes, 0)
        .expect("product node limit should be accepted");

    let tasks = scheduler.take_due(0);
    let task_ids = tasks
        .iter()
        .map(|task| task.node_id.as_str())
        .collect::<BTreeSet<_>>();
    let metrics = scheduler.metrics(0);

    assert_eq!(tasks.len(), PROBE_WORKER_COUNT);
    assert_eq!(task_ids.len(), PROBE_WORKER_COUNT);
    assert!(scheduler.take_due(0).is_empty());
    assert_eq!(metrics.active_node_count, MAX_ACTIVE_NODES);
    assert_eq!(
        metrics.queue_depth + metrics.in_flight_count,
        MAX_ACTIVE_NODES
    );
    assert_eq!(metrics.in_flight_count, PROBE_WORKER_COUNT);
    assert!(metrics.overloaded);
    assert_eq!(
        metrics.estimated_full_pass_duration_ms,
        u64::try_from(MAX_ACTIVE_NODES.div_ceil(PROBE_WORKER_COUNT))
            .expect("fixture count should fit")
            * duration_ms(PROBE_TIMEOUT)
    );
}

#[test]
fn exceeding_the_active_node_limit_preserves_the_current_generation() {
    let current = node("current");
    let mut scheduler = ProbeScheduler::new();
    scheduler
        .reset(ProbeGeneration(4), [current], 0)
        .expect("initial generation should be accepted");
    let consumed = Cell::new(0);
    let oversized = (0..MAX_ACTIVE_NODES + 100_000).map(|index| {
        consumed.set(consumed.get() + 1);
        node(&format!("oversized-{index}"))
    });

    let result = scheduler.reset(ProbeGeneration(5), oversized, 10);

    assert_eq!(
        result,
        Err(ProbeResetError::NodeLimitExceeded {
            limit: MAX_ACTIVE_NODES,
            actual: MAX_ACTIVE_NODES + 1,
        })
    );
    assert_eq!(scheduler.generation(), Some(ProbeGeneration(4)));
    assert_eq!(scheduler.active_node_count(), 1);
    assert_eq!(consumed.get(), MAX_ACTIVE_NODES + 1);
}

#[test]
fn a_new_probe_generation_discards_pending_work_and_late_results() {
    let old_node = node("old");
    let current_node = node("current");
    let mut scheduler = ProbeScheduler::new();
    scheduler
        .reset(ProbeGeneration(7), [old_node.clone()], 0)
        .expect("initial generation should be accepted");
    let old_task = scheduler.take_due(0).remove(0);
    scheduler
        .reset(ProbeGeneration(8), [current_node.clone()], 10)
        .expect("new generation should be accepted");

    let status = scheduler.complete(ProbeCompletion {
        task: old_task,
        outcome: ProbeOutcome::Success { delay_ms: 5 },
        completed_at_unix_ms: 20,
    });

    assert_eq!(status, ProbeCompletionStatus::LateGeneration);
    assert!(scheduler.node_snapshot(&old_node, 20).is_none());
    assert_eq!(
        scheduler
            .node_snapshot(&current_node, 20)
            .expect("current node should remain scheduled")
            .status,
        ProbeStatus::Queued
    );
}

#[test]
fn probe_completion_reschedules_and_reports_sample_freshness() {
    let node_id = node("measured");
    let mut scheduler = ProbeScheduler::new();
    scheduler
        .reset(ProbeGeneration(2), [node_id.clone()], 0)
        .expect("generation should be accepted");
    let task = scheduler.take_due(0).remove(0);

    let status = scheduler.complete(ProbeCompletion {
        task,
        outcome: ProbeOutcome::Success { delay_ms: 42 },
        completed_at_unix_ms: 50,
    });
    let next_probe_at_unix_ms = 50 + duration_ms(PROBE_INTERVAL);

    assert_eq!(
        status,
        ProbeCompletionStatus::Rescheduled {
            next_probe_at_unix_ms,
        }
    );
    let fresh = scheduler
        .node_snapshot(&node_id, 50 + duration_ms(LATENCY_FRESHNESS))
        .expect("active node should have a sample");
    assert_eq!(fresh.status, ProbeStatus::Available);
    assert_eq!(
        fresh.sample.expect("sample should exist").state,
        SampleState::Fresh
    );
    let stale = scheduler
        .node_snapshot(&node_id, 51 + duration_ms(LATENCY_FRESHNESS))
        .expect("active node should have a sample");
    assert_eq!(
        stale.sample.expect("sample should exist").state,
        SampleState::Stale
    );
    assert!(scheduler.take_due(next_probe_at_unix_ms - 1).is_empty());
    assert_eq!(scheduler.take_due(next_probe_at_unix_ms).len(), 1);
}

#[test]
fn probe_failures_have_explicit_status_and_reschedule() {
    let node_id = node("timeout");
    let mut scheduler = ProbeScheduler::new();
    scheduler
        .reset(ProbeGeneration(1), [node_id.clone()], 0)
        .expect("generation should be accepted");
    let task = scheduler.take_due(0).remove(0);

    scheduler.complete(ProbeCompletion {
        task,
        outcome: ProbeOutcome::TimedOut,
        completed_at_unix_ms: 5,
    });
    let snapshot = scheduler
        .node_snapshot(&node_id, 5)
        .expect("active node should have a failure sample");

    assert_eq!(snapshot.status, ProbeStatus::TimedOut);
    let sample = snapshot.sample.expect("failure sample should exist");
    assert_eq!(sample.delay_ms, None);
    assert_eq!(sample.state, SampleState::Unavailable);
}

#[test]
fn inactive_state_has_zero_probe_work() {
    let node_id = node("active");
    let mut scheduler = ProbeScheduler::new();
    scheduler
        .reset(ProbeGeneration(1), [node_id], 0)
        .expect("generation should be accepted");
    scheduler.deactivate();

    let metrics = scheduler.metrics(u64::MAX);

    assert_eq!(scheduler.generation(), None);
    assert_eq!(metrics.active_node_count, 0);
    assert_eq!(metrics.queue_depth, 0);
    assert_eq!(metrics.in_flight_count, 0);
    assert_eq!(metrics.stale_ratio, 0.0);
    assert!(!metrics.overloaded);
    assert!(scheduler.take_due(u64::MAX).is_empty());
}

#[test]
fn probe_generation_must_increase_across_deactivation() {
    let mut scheduler = ProbeScheduler::new();
    scheduler
        .reset(ProbeGeneration(3), [node("first")], 0)
        .expect("generation should be accepted");
    scheduler.deactivate();

    assert_eq!(
        scheduler.reset(ProbeGeneration(3), [node("second")], 0),
        Err(ProbeResetError::NonIncreasingGeneration)
    );
}

fn complete_success(
    scheduler: &mut ProbeScheduler,
    task: hopash::scheduler::ProbeTask,
    completed_at_unix_ms: u64,
    delay_ms: u64,
) {
    let status = scheduler.complete(ProbeCompletion {
        task,
        outcome: ProbeOutcome::Success { delay_ms },
        completed_at_unix_ms,
    });
    assert!(matches!(status, ProbeCompletionStatus::Rescheduled { .. }));
}

fn task_for(
    tasks: &[hopash::scheduler::ProbeTask],
    node_id: &NodeRecordId,
) -> hopash::scheduler::ProbeTask {
    tasks
        .iter()
        .find(|task| &task.node_id == node_id)
        .expect("node task should exist")
        .clone()
}

fn node(name: &str) -> NodeRecordId {
    NodeRecordId::for_core(name)
}

fn duration_ms(duration: std::time::Duration) -> u64 {
    u64::try_from(duration.as_millis()).expect("product duration should fit in milliseconds")
}
