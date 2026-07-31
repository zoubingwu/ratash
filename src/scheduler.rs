use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use crate::constants::{
    LATENCY_FRESHNESS, MAX_ACTIVE_NODES, PROBE_INTERVAL, PROBE_TIMEOUT, PROBE_WORKER_COUNT,
    PROFILE_REFRESH_CONCURRENCY, PROFILE_REFRESH_INTERVAL,
};
use crate::domain::{LatencySample, NodeRecordId, ProbeGeneration, ProfileId, SampleState};
use crate::profile::ProfileRevision;

// -----------------------------------------------------------------------------
// Profile refresh scheduling
// -----------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RefreshTask {
    pub attempt_id: u64,
    pub profile_id: ProfileId,
    pub profile_revision: ProfileRevision,
    pub due_at_unix_ms: u64,
    registration_id: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RefreshCompletion {
    pub task: RefreshTask,
    pub profile_revision: ProfileRevision,
    pub completed_at_unix_ms: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RefreshCompletionStatus {
    Rescheduled { next_refresh_at_unix_ms: u64 },
    StaleRevision,
    ProfileRemoved,
    UnknownTask,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct RefreshEntry {
    profile_revision: ProfileRevision,
    due_at_unix_ms: u64,
    registration_id: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct InFlightRefresh {
    attempt_id: u64,
    registration_id: u64,
}

#[derive(Debug)]
pub struct ProfileRefreshScheduler {
    entries: BTreeMap<ProfileId, RefreshEntry>,
    in_flight: BTreeMap<ProfileId, InFlightRefresh>,
    next_attempt_id: u64,
    next_registration_id: u64,
}

impl Default for ProfileRefreshScheduler {
    fn default() -> Self {
        Self::new()
    }
}

impl ProfileRefreshScheduler {
    #[must_use]
    pub fn new() -> Self {
        Self {
            entries: BTreeMap::new(),
            in_flight: BTreeMap::new(),
            next_attempt_id: 1,
            next_registration_id: 1,
        }
    }

    pub fn upsert(
        &mut self,
        profile_id: ProfileId,
        profile_revision: ProfileRevision,
        next_refresh_at_unix_ms: u64,
    ) {
        let registration_id = match self.entries.get(&profile_id) {
            Some(entry) => entry.registration_id,
            None => self.take_registration_id(),
        };
        self.entries.insert(
            profile_id,
            RefreshEntry {
                profile_revision,
                due_at_unix_ms: next_refresh_at_unix_ms,
                registration_id,
            },
        );
    }

    pub fn remove(&mut self, profile_id: ProfileId) -> bool {
        self.entries.remove(&profile_id).is_some()
    }

    #[must_use]
    pub fn scheduled_profile_count(&self) -> usize {
        self.entries.len()
    }

    #[must_use]
    pub fn in_flight_count(&self) -> usize {
        self.in_flight.len()
    }

    #[must_use]
    pub fn next_deadline(&self) -> Option<u64> {
        self.entries
            .iter()
            .filter(|(profile_id, _)| !self.in_flight.contains_key(profile_id))
            .map(|(_, entry)| entry.due_at_unix_ms)
            .min()
    }

    pub fn take_due(&mut self, now_unix_ms: u64) -> Vec<RefreshTask> {
        let available = PROFILE_REFRESH_CONCURRENCY.saturating_sub(self.in_flight.len());
        let mut due = self
            .entries
            .iter()
            .filter(|(profile_id, entry)| {
                entry.due_at_unix_ms <= now_unix_ms && !self.in_flight.contains_key(profile_id)
            })
            .map(|(profile_id, entry)| {
                (
                    entry.due_at_unix_ms,
                    *profile_id,
                    entry.profile_revision,
                    entry.registration_id,
                )
            })
            .collect::<Vec<_>>();
        due.sort_unstable();
        due.truncate(available);

        due.into_iter()
            .map(
                |(due_at_unix_ms, profile_id, profile_revision, registration_id)| {
                    let attempt_id = self.take_attempt_id();
                    self.in_flight.insert(
                        profile_id,
                        InFlightRefresh {
                            attempt_id,
                            registration_id,
                        },
                    );
                    RefreshTask {
                        attempt_id,
                        profile_id,
                        profile_revision,
                        due_at_unix_ms,
                        registration_id,
                    }
                },
            )
            .collect()
    }

    pub fn complete(&mut self, completion: RefreshCompletion) -> RefreshCompletionStatus {
        let Some(in_flight) = self.in_flight.get(&completion.task.profile_id) else {
            return RefreshCompletionStatus::UnknownTask;
        };
        if in_flight.attempt_id != completion.task.attempt_id
            || in_flight.registration_id != completion.task.registration_id
        {
            return RefreshCompletionStatus::UnknownTask;
        }
        self.in_flight.remove(&completion.task.profile_id);

        let Some(entry) = self.entries.get_mut(&completion.task.profile_id) else {
            return RefreshCompletionStatus::ProfileRemoved;
        };
        if entry.registration_id != completion.task.registration_id
            || entry.profile_revision != completion.task.profile_revision
        {
            return RefreshCompletionStatus::StaleRevision;
        }

        let next_refresh_at_unix_ms = completion
            .completed_at_unix_ms
            .saturating_add(duration_ms(PROFILE_REFRESH_INTERVAL));
        entry.profile_revision = completion.profile_revision;
        entry.due_at_unix_ms = next_refresh_at_unix_ms;
        RefreshCompletionStatus::Rescheduled {
            next_refresh_at_unix_ms,
        }
    }

    fn take_attempt_id(&mut self) -> u64 {
        let attempt_id = self.next_attempt_id;
        self.next_attempt_id = next_nonzero(self.next_attempt_id);
        attempt_id
    }

    fn take_registration_id(&mut self) -> u64 {
        let registration_id = self.next_registration_id;
        self.next_registration_id = next_nonzero(self.next_registration_id);
        registration_id
    }
}

// -----------------------------------------------------------------------------
// Active Profile delay probe scheduling
// -----------------------------------------------------------------------------

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct DueProbe {
    due_at_unix_ms: u64,
    node_id: NodeRecordId,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct InFlightProbe {
    attempt_id: u64,
    generation: ProbeGeneration,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct StoredProbeSample {
    outcome: ProbeOutcome,
    sampled_at_unix_ms: u64,
    generation: ProbeGeneration,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProbeTask {
    pub attempt_id: u64,
    pub generation: ProbeGeneration,
    pub node_id: NodeRecordId,
    pub due_at_unix_ms: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProbeOutcome {
    Success { delay_ms: u64 },
    TimedOut,
    Unavailable,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProbeCompletion {
    pub task: ProbeTask,
    pub outcome: ProbeOutcome,
    pub completed_at_unix_ms: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProbeCompletionStatus {
    Rescheduled { next_probe_at_unix_ms: u64 },
    LateGeneration,
    UnknownTask,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProbeStatus {
    NotSampled,
    Queued,
    InFlight,
    Available,
    TimedOut,
    Unavailable,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProbeNodeSnapshot {
    pub node_id: NodeRecordId,
    pub sample: Option<LatencySample>,
    pub status: ProbeStatus,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ProbeMetrics {
    pub generation: Option<ProbeGeneration>,
    pub active_node_count: usize,
    pub queue_depth: usize,
    pub in_flight_count: usize,
    pub overloaded: bool,
    pub oldest_due_age_ms: Option<u64>,
    pub estimated_full_pass_duration_ms: u64,
    pub stale_node_count: usize,
    pub stale_ratio: f64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProbeResetError {
    NonIncreasingGeneration,
    NodeLimitExceeded { limit: usize, actual: usize },
}

#[derive(Debug)]
pub struct ProbeScheduler {
    generation: Option<ProbeGeneration>,
    last_generation: Option<ProbeGeneration>,
    active_nodes: BTreeSet<NodeRecordId>,
    queue: BTreeSet<DueProbe>,
    queued_deadlines: BTreeMap<NodeRecordId, u64>,
    in_flight: BTreeMap<NodeRecordId, InFlightProbe>,
    samples: BTreeMap<NodeRecordId, StoredProbeSample>,
    next_attempt_id: u64,
}

impl Default for ProbeScheduler {
    fn default() -> Self {
        Self::new()
    }
}

impl ProbeScheduler {
    #[must_use]
    pub fn new() -> Self {
        Self {
            generation: None,
            last_generation: None,
            active_nodes: BTreeSet::new(),
            queue: BTreeSet::new(),
            queued_deadlines: BTreeMap::new(),
            in_flight: BTreeMap::new(),
            samples: BTreeMap::new(),
            next_attempt_id: 1,
        }
    }

    pub fn reset<I>(
        &mut self,
        generation: ProbeGeneration,
        nodes: I,
        now_unix_ms: u64,
    ) -> Result<(), ProbeResetError>
    where
        I: IntoIterator<Item = NodeRecordId>,
    {
        if self
            .last_generation
            .is_some_and(|current| generation <= current)
        {
            return Err(ProbeResetError::NonIncreasingGeneration);
        }

        let mut active_nodes = BTreeSet::new();
        for node_id in nodes {
            active_nodes.insert(node_id);
            if active_nodes.len() > MAX_ACTIVE_NODES {
                return Err(ProbeResetError::NodeLimitExceeded {
                    limit: MAX_ACTIVE_NODES,
                    actual: active_nodes.len(),
                });
            }
        }

        self.generation = Some(generation);
        self.last_generation = Some(generation);
        self.active_nodes = active_nodes;
        self.queue.clear();
        self.queued_deadlines.clear();
        self.in_flight.clear();
        self.samples.clear();
        for node_id in &self.active_nodes {
            self.queue.insert(DueProbe {
                due_at_unix_ms: now_unix_ms,
                node_id: node_id.clone(),
            });
            self.queued_deadlines.insert(node_id.clone(), now_unix_ms);
        }
        Ok(())
    }

    pub fn deactivate(&mut self) {
        self.generation = None;
        self.active_nodes.clear();
        self.queue.clear();
        self.queued_deadlines.clear();
        self.in_flight.clear();
        self.samples.clear();
    }

    #[must_use]
    pub fn generation(&self) -> Option<ProbeGeneration> {
        self.generation
    }

    #[must_use]
    pub fn active_node_count(&self) -> usize {
        self.active_nodes.len()
    }

    pub fn take_due(&mut self, now_unix_ms: u64) -> Vec<ProbeTask> {
        let Some(generation) = self.generation else {
            return Vec::new();
        };
        let available = PROBE_WORKER_COUNT.saturating_sub(self.in_flight.len());
        let due = self
            .queue
            .iter()
            .take_while(|entry| entry.due_at_unix_ms <= now_unix_ms)
            .take(available)
            .cloned()
            .collect::<Vec<_>>();

        due.into_iter()
            .map(|entry| {
                self.queue.remove(&entry);
                self.queued_deadlines.remove(&entry.node_id);
                let attempt_id = self.take_attempt_id();
                self.in_flight.insert(
                    entry.node_id.clone(),
                    InFlightProbe {
                        attempt_id,
                        generation,
                    },
                );
                ProbeTask {
                    attempt_id,
                    generation,
                    node_id: entry.node_id,
                    due_at_unix_ms: entry.due_at_unix_ms,
                }
            })
            .collect()
    }

    pub fn complete(&mut self, completion: ProbeCompletion) -> ProbeCompletionStatus {
        if self.generation != Some(completion.task.generation) {
            return ProbeCompletionStatus::LateGeneration;
        }
        let Some(in_flight) = self.in_flight.get(&completion.task.node_id) else {
            return ProbeCompletionStatus::UnknownTask;
        };
        if in_flight.attempt_id != completion.task.attempt_id
            || in_flight.generation != completion.task.generation
        {
            return ProbeCompletionStatus::UnknownTask;
        }
        self.in_flight.remove(&completion.task.node_id);

        self.samples.insert(
            completion.task.node_id.clone(),
            StoredProbeSample {
                outcome: completion.outcome,
                sampled_at_unix_ms: completion.completed_at_unix_ms,
                generation: completion.task.generation,
            },
        );

        let next_probe_at_unix_ms = completion
            .completed_at_unix_ms
            .saturating_add(duration_ms(PROBE_INTERVAL));
        self.schedule(completion.task.node_id, next_probe_at_unix_ms);
        ProbeCompletionStatus::Rescheduled {
            next_probe_at_unix_ms,
        }
    }

    #[must_use]
    pub fn node_snapshot(
        &self,
        node_id: &NodeRecordId,
        now_unix_ms: u64,
    ) -> Option<ProbeNodeSnapshot> {
        self.active_nodes
            .contains(node_id)
            .then(|| ProbeNodeSnapshot {
                node_id: node_id.clone(),
                sample: self.sample(node_id, now_unix_ms),
                status: self.probe_status(node_id),
            })
    }

    #[must_use]
    pub fn metrics(&self, now_unix_ms: u64) -> ProbeMetrics {
        let active_node_count = self.active_nodes.len();
        let oldest_due_age_ms = self
            .queue
            .first()
            .map(|entry| now_unix_ms.saturating_sub(entry.due_at_unix_ms));
        let estimated_full_pass_duration_ms = batches(active_node_count, PROBE_WORKER_COUNT)
            .saturating_mul(duration_ms(PROBE_TIMEOUT));
        let stale_count = self
            .active_nodes
            .iter()
            .filter(|node_id| {
                self.samples
                    .get(node_id)
                    .is_none_or(|sample| sample_state(sample, now_unix_ms) != SampleState::Fresh)
            })
            .count();
        let stale_ratio = if active_node_count == 0 {
            0.0
        } else {
            stale_count as f64 / active_node_count as f64
        };
        let freshness_ms = duration_ms(LATENCY_FRESHNESS);
        let overloaded = active_node_count > 0
            && (estimated_full_pass_duration_ms > freshness_ms
                || oldest_due_age_ms.is_some_and(|age| age > freshness_ms));

        ProbeMetrics {
            generation: self.generation,
            active_node_count,
            queue_depth: self.queue.len(),
            in_flight_count: self.in_flight.len(),
            overloaded,
            oldest_due_age_ms,
            estimated_full_pass_duration_ms,
            stale_node_count: stale_count,
            stale_ratio,
        }
    }

    fn schedule(&mut self, node_id: NodeRecordId, due_at_unix_ms: u64) {
        debug_assert!(!self.queued_deadlines.contains_key(&node_id));
        debug_assert!(!self.in_flight.contains_key(&node_id));
        self.queue.insert(DueProbe {
            due_at_unix_ms,
            node_id: node_id.clone(),
        });
        self.queued_deadlines.insert(node_id, due_at_unix_ms);
    }

    fn sample(&self, node_id: &NodeRecordId, now_unix_ms: u64) -> Option<LatencySample> {
        let stored = self.samples.get(node_id)?;
        let delay_ms = match stored.outcome {
            ProbeOutcome::Success { delay_ms } => Some(delay_ms),
            ProbeOutcome::TimedOut | ProbeOutcome::Unavailable => None,
        };
        Some(LatencySample {
            node_id: node_id.clone(),
            delay_ms,
            sampled_at_unix_ms: Some(stored.sampled_at_unix_ms),
            state: sample_state(stored, now_unix_ms),
            probe_generation: stored.generation,
        })
    }

    fn probe_status(&self, node_id: &NodeRecordId) -> ProbeStatus {
        if self.in_flight.contains_key(node_id) {
            return ProbeStatus::InFlight;
        }
        match self.samples.get(node_id).map(|sample| sample.outcome) {
            Some(ProbeOutcome::Success { .. }) => ProbeStatus::Available,
            Some(ProbeOutcome::TimedOut) => ProbeStatus::TimedOut,
            Some(ProbeOutcome::Unavailable) => ProbeStatus::Unavailable,
            None if self.queued_deadlines.contains_key(node_id) => ProbeStatus::Queued,
            None => ProbeStatus::NotSampled,
        }
    }

    fn take_attempt_id(&mut self) -> u64 {
        let attempt_id = self.next_attempt_id;
        self.next_attempt_id = next_nonzero(self.next_attempt_id);
        attempt_id
    }
}

fn duration_ms(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn sample_state(sample: &StoredProbeSample, now_unix_ms: u64) -> SampleState {
    match sample.outcome {
        ProbeOutcome::Success { .. } => {
            let age = now_unix_ms.saturating_sub(sample.sampled_at_unix_ms);
            if age <= duration_ms(LATENCY_FRESHNESS) {
                SampleState::Fresh
            } else {
                SampleState::Stale
            }
        }
        ProbeOutcome::TimedOut | ProbeOutcome::Unavailable => SampleState::Unavailable,
    }
}

fn batches(item_count: usize, worker_count: usize) -> u64 {
    let batches = item_count.div_ceil(worker_count);
    u64::try_from(batches).unwrap_or(u64::MAX)
}

fn next_nonzero(value: u64) -> u64 {
    let next = value.wrapping_add(1);
    if next == 0 { 1 } else { next }
}
