//! Bounded, fair event scheduling for the Status Interface.

use std::collections::VecDeque;

use super::{EVENT_SOURCE_CAPACITY, UiEvent};

// -----------------------------------------------------------------------------
// Fair bounded event inbox
// -----------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EventSource {
    Terminal,
    CommandResult,
    Deadline,
    Telemetry,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EventBudgets {
    pub terminal: usize,
    pub command_result: usize,
    pub deadline: usize,
    pub telemetry: usize,
}

impl Default for EventBudgets {
    fn default() -> Self {
        Self {
            terminal: 8,
            command_result: 8,
            deadline: 2,
            telemetry: 64,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EventInboxError {
    InvalidCapacity,
    InvalidBudget,
}

#[derive(Debug)]
pub struct FairEventInbox {
    capacity_per_source: usize,
    budgets: EventBudgets,
    shutdown: Option<UiEvent>,
    terminal: VecDeque<UiEvent>,
    command_results: VecDeque<UiEvent>,
    deadlines: VecDeque<UiEvent>,
    telemetry: VecDeque<UiEvent>,
    dropped: [u64; 4],
}

impl FairEventInbox {
    pub fn new(capacity_per_source: usize, budgets: EventBudgets) -> Result<Self, EventInboxError> {
        if capacity_per_source == 0 {
            return Err(EventInboxError::InvalidCapacity);
        }
        if budgets.terminal == 0
            || budgets.command_result == 0
            || budgets.deadline == 0
            || budgets.telemetry == 0
        {
            return Err(EventInboxError::InvalidBudget);
        }
        Ok(Self::from_validated_parts(capacity_per_source, budgets))
    }

    pub fn product() -> Self {
        Self::from_validated_parts(EVENT_SOURCE_CAPACITY, EventBudgets::default())
    }

    fn from_validated_parts(capacity_per_source: usize, budgets: EventBudgets) -> Self {
        Self {
            capacity_per_source,
            budgets,
            shutdown: None,
            terminal: VecDeque::with_capacity(capacity_per_source),
            command_results: VecDeque::with_capacity(capacity_per_source),
            deadlines: VecDeque::with_capacity(capacity_per_source),
            telemetry: VecDeque::with_capacity(capacity_per_source),
            dropped: [0; 4],
        }
    }

    pub fn push(&mut self, source: EventSource, event: UiEvent) {
        if matches!(event, UiEvent::Shutdown) {
            self.shutdown = Some(event);
            return;
        }
        if source == EventSource::Telemetry
            && matches!(event, UiEvent::StatusSnapshot { .. })
            && let Some(position) = self
                .telemetry
                .iter()
                .rposition(|queued| matches!(queued, UiEvent::StatusSnapshot { .. }))
        {
            self.telemetry[position] = event;
            return;
        }
        let index = source_index(source);
        let queue = match source {
            EventSource::Terminal => &mut self.terminal,
            EventSource::CommandResult => &mut self.command_results,
            EventSource::Deadline => &mut self.deadlines,
            EventSource::Telemetry => &mut self.telemetry,
        };
        if queue.len() == self.capacity_per_source {
            queue.pop_front();
            self.dropped[index] = self.dropped[index].saturating_add(1);
        }
        queue.push_back(event);
    }

    pub fn drain_round(&mut self) -> Vec<UiEvent> {
        if let Some(shutdown) = self.shutdown.take() {
            return vec![shutdown];
        }
        let total_budget = self
            .budgets
            .terminal
            .saturating_add(self.budgets.command_result)
            .saturating_add(self.budgets.deadline)
            .saturating_add(self.budgets.telemetry);
        let mut events = Vec::with_capacity(total_budget);
        drain_source(&mut self.terminal, self.budgets.terminal, &mut events);
        drain_source(&mut self.telemetry, self.budgets.telemetry, &mut events);
        drain_source(
            &mut self.command_results,
            self.budgets.command_result,
            &mut events,
        );
        drain_source(&mut self.deadlines, self.budgets.deadline, &mut events);
        events
    }

    #[must_use]
    pub fn dropped(&self, source: EventSource) -> u64 {
        self.dropped[source_index(source)]
    }

    #[must_use]
    pub fn len(&self, source: EventSource) -> usize {
        match source {
            EventSource::Terminal => self.terminal.len(),
            EventSource::CommandResult => self.command_results.len(),
            EventSource::Deadline => self.deadlines.len(),
            EventSource::Telemetry => self.telemetry.len(),
        }
    }
}

fn source_index(source: EventSource) -> usize {
    match source {
        EventSource::Terminal => 0,
        EventSource::CommandResult => 1,
        EventSource::Deadline => 2,
        EventSource::Telemetry => 3,
    }
}

fn drain_source(queue: &mut VecDeque<UiEvent>, budget: usize, output: &mut Vec<UiEvent>) {
    for _ in 0..budget {
        let Some(event) = queue.pop_front() else {
            break;
        };
        output.push(event);
    }
}
