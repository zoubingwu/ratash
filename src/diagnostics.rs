use std::collections::VecDeque;
use std::fmt;

use crate::domain::{CoreInstanceGeneration, RuntimeGeneration, SupervisorHealthReason};

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum WrapperDiagnosticCategory {
    RuntimeRecovery,
    RuntimeApply,
    ProfileRefresh,
    SelectionCompensation,
    ConfigurationProjection,
    ProbeScheduler,
    SelectionRestoration,
    TelemetryStream,
    CoreLifecycle,
}

impl From<SupervisorHealthReason> for WrapperDiagnosticCategory {
    fn from(reason: SupervisorHealthReason) -> Self {
        match reason {
            SupervisorHealthReason::RuntimeRecovery => Self::RuntimeRecovery,
            SupervisorHealthReason::SelectionCompensation => Self::SelectionCompensation,
            SupervisorHealthReason::ConfigurationProjection => Self::ConfigurationProjection,
            SupervisorHealthReason::ProbeScheduler => Self::ProbeScheduler,
            SupervisorHealthReason::SelectionRestoration => Self::SelectionRestoration,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WrapperDiagnosticState {
    Raised,
    Cleared,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct WrapperDiagnosticContext {
    pub runtime_generation: Option<RuntimeGeneration>,
    pub core_generation: Option<CoreInstanceGeneration>,
    pub revision: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WrapperDiagnostic {
    pub sequence: u64,
    pub timestamp_unix_ms: u64,
    pub category: WrapperDiagnosticCategory,
    pub state: WrapperDiagnosticState,
    pub context: WrapperDiagnosticContext,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WrapperDiagnosticTail {
    pub records: Vec<WrapperDiagnostic>,
    pub evicted_total: u64,
    pub gap: bool,
    pub earliest_sequence: Option<u64>,
    pub latest_sequence: Option<u64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WrapperDiagnosticError {
    InvalidLimit,
    SequenceExhausted,
}

impl fmt::Display for WrapperDiagnosticError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidLimit => "Wrapper diagnostic limits must be greater than zero",
            Self::SequenceExhausted => "Wrapper diagnostic sequence is exhausted",
        })
    }
}

impl std::error::Error for WrapperDiagnosticError {}

#[derive(Clone, Debug)]
pub struct WrapperDiagnosticRing {
    capacity: usize,
    next_sequence: u64,
    evicted_total: u64,
    records: VecDeque<WrapperDiagnostic>,
}

impl WrapperDiagnosticRing {
    pub fn new(capacity: usize) -> Result<Self, WrapperDiagnosticError> {
        if capacity == 0 {
            return Err(WrapperDiagnosticError::InvalidLimit);
        }
        Ok(Self {
            capacity,
            next_sequence: 1,
            evicted_total: 0,
            records: VecDeque::with_capacity(capacity),
        })
    }

    pub fn record(
        &mut self,
        timestamp_unix_ms: u64,
        category: WrapperDiagnosticCategory,
        state: WrapperDiagnosticState,
        context: WrapperDiagnosticContext,
    ) -> Result<u64, WrapperDiagnosticError> {
        let following = self
            .next_sequence
            .checked_add(1)
            .ok_or(WrapperDiagnosticError::SequenceExhausted)?;
        let sequence = self.next_sequence;
        if self.records.len() == self.capacity {
            self.records.pop_front();
            self.evicted_total = self.evicted_total.saturating_add(1);
        }
        self.records.push_back(WrapperDiagnostic {
            sequence,
            timestamp_unix_ms,
            category,
            state,
            context,
        });
        emit_structured_event(sequence, timestamp_unix_ms, category, state, context);
        self.next_sequence = following;
        Ok(sequence)
    }

    pub fn tail_after(
        &self,
        after_sequence: Option<u64>,
        max_records: usize,
    ) -> Result<WrapperDiagnosticTail, WrapperDiagnosticError> {
        if max_records == 0 {
            return Err(WrapperDiagnosticError::InvalidLimit);
        }
        let matching = self
            .records
            .iter()
            .filter(|record| after_sequence.is_none_or(|after| record.sequence > after))
            .count();
        let skipped = matching.saturating_sub(max_records);
        let records = self
            .records
            .iter()
            .filter(|record| after_sequence.is_none_or(|after| record.sequence > after))
            .skip(skipped)
            .cloned()
            .collect::<Vec<_>>();
        let earliest_sequence = records.first().map(|record| record.sequence);
        let latest_sequence = records.last().map(|record| record.sequence);
        let gap = skipped > 0
            || after_sequence
                .zip(earliest_sequence)
                .is_some_and(|(after, earliest)| {
                    after.checked_add(1).is_some_and(|next| next < earliest)
                })
            || after_sequence.is_some_and(|after| {
                records.is_empty()
                    && after
                        .checked_add(1)
                        .is_some_and(|next| next < self.next_sequence)
            });
        Ok(WrapperDiagnosticTail {
            records,
            evicted_total: self.evicted_total,
            gap,
            earliest_sequence,
            latest_sequence,
        })
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.records.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    #[must_use]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    #[must_use]
    pub fn evicted_total(&self) -> u64 {
        self.evicted_total
    }
}

fn emit_structured_event(
    sequence: u64,
    timestamp_unix_ms: u64,
    category: WrapperDiagnosticCategory,
    state: WrapperDiagnosticState,
    context: WrapperDiagnosticContext,
) {
    let runtime_generation = context.runtime_generation.map(|generation| generation.0);
    let core_generation = context.core_generation.map(|generation| generation.0);
    match state {
        WrapperDiagnosticState::Raised => tracing::warn!(
            target: "hopash::wrapper",
            sequence,
            timestamp_unix_ms,
            category = ?category,
            state = ?state,
            runtime_generation,
            core_generation,
            revision = context.revision,
            "Wrapper diagnostic state changed"
        ),
        WrapperDiagnosticState::Cleared => tracing::info!(
            target: "hopash::wrapper",
            sequence,
            timestamp_unix_ms,
            category = ?category,
            state = ?state,
            runtime_generation,
            core_generation,
            revision = context.revision,
            "Wrapper diagnostic state changed"
        ),
    }
}
