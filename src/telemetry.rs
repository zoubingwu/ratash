use std::collections::VecDeque;
use std::fmt;

use crate::domain::{CoreInstanceGeneration, TrafficSample};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LogLevel {
    Debug,
    Info,
    Warn,
    Error,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LogSource {
    CoreApi,
    Stdout,
    Stderr,
}

#[derive(Clone, Eq, PartialEq)]
pub struct CoreLogRecord {
    sequence: u64,
    timestamp_unix_ms: u64,
    level: LogLevel,
    source: LogSource,
    message: String,
}

impl CoreLogRecord {
    #[must_use]
    pub fn new(
        sequence: u64,
        timestamp_unix_ms: u64,
        level: LogLevel,
        source: LogSource,
        message: impl Into<String>,
    ) -> Self {
        Self {
            sequence,
            timestamp_unix_ms,
            level,
            source,
            message: message.into(),
        }
    }

    #[must_use]
    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    #[must_use]
    pub fn timestamp_unix_ms(&self) -> u64 {
        self.timestamp_unix_ms
    }

    #[must_use]
    pub fn level(&self) -> LogLevel {
        self.level
    }

    #[must_use]
    pub fn source(&self) -> LogSource {
        self.source
    }

    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Debug for CoreLogRecord {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CoreLogRecord")
            .field("sequence", &self.sequence)
            .field("timestamp_unix_ms", &self.timestamp_unix_ms)
            .field("level", &self.level)
            .field("source", &self.source)
            .field("message_bytes", &self.message.len())
            .finish()
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct LogFilter {
    pub level: Option<LogLevel>,
    pub contains: Option<String>,
    pub since_unix_ms: Option<u64>,
    pub until_unix_ms: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogTail {
    pub records: Vec<CoreLogRecord>,
    pub dropped_total: u64,
    pub gap: bool,
    pub earliest_sequence: Option<u64>,
    pub latest_sequence: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TelemetryError {
    InvalidLimit,
    LogLineTooLarge { limit: usize },
    SequenceExhausted,
}

impl fmt::Display for TelemetryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidLimit => formatter.write_str("telemetry limits must be greater than zero"),
            Self::LogLineTooLarge { limit } => {
                write!(formatter, "Core Log line exceeds the {limit}-byte limit")
            }
            Self::SequenceExhausted => formatter.write_str("Core Log sequence is exhausted"),
        }
    }
}

impl std::error::Error for TelemetryError {}

#[derive(Clone, Debug)]
pub struct LogBuffer {
    capacity: usize,
    max_line_bytes: usize,
    next_sequence: u64,
    dropped_total: u64,
    records: VecDeque<CoreLogRecord>,
}

impl LogBuffer {
    pub fn new(capacity: usize, max_line_bytes: usize) -> Result<Self, TelemetryError> {
        if capacity == 0 || max_line_bytes == 0 {
            return Err(TelemetryError::InvalidLimit);
        }
        Ok(Self {
            capacity,
            max_line_bytes,
            next_sequence: 1,
            dropped_total: 0,
            records: VecDeque::with_capacity(capacity),
        })
    }

    pub fn push(
        &mut self,
        timestamp_unix_ms: u64,
        level: LogLevel,
        source: LogSource,
        message: impl Into<String>,
    ) -> Result<u64, TelemetryError> {
        let message = message.into();
        if message.len() > self.max_line_bytes {
            return Err(TelemetryError::LogLineTooLarge {
                limit: self.max_line_bytes,
            });
        }
        let sequence = self.next_sequence;
        self.next_sequence = self
            .next_sequence
            .checked_add(1)
            .ok_or(TelemetryError::SequenceExhausted)?;
        if self.records.len() == self.capacity {
            self.records.pop_front();
            self.dropped_total = self.dropped_total.saturating_add(1);
        }
        self.records.push_back(CoreLogRecord::new(
            sequence,
            timestamp_unix_ms,
            level,
            source,
            message,
        ));
        Ok(sequence)
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
    pub fn dropped_total(&self) -> u64 {
        self.dropped_total
    }

    #[must_use]
    pub fn records(&self) -> Vec<CoreLogRecord> {
        self.records.iter().cloned().collect()
    }

    #[must_use]
    pub fn tail_after(&self, after_sequence: Option<u64>) -> LogTail {
        let earliest_sequence = self.records.front().map(CoreLogRecord::sequence);
        let latest_sequence = self.records.back().map(CoreLogRecord::sequence);
        let gap = after_sequence
            .zip(earliest_sequence)
            .is_some_and(|(after, earliest)| {
                after.checked_add(1).is_some_and(|next| next < earliest)
            });
        let records = self
            .records
            .iter()
            .filter(|record| after_sequence.is_none_or(|after| record.sequence() > after))
            .cloned()
            .collect();
        LogTail {
            records,
            dropped_total: self.dropped_total,
            gap,
            earliest_sequence,
            latest_sequence,
        }
    }

    #[must_use]
    pub fn query(&self, filter: &LogFilter) -> Vec<CoreLogRecord> {
        let needle = filter.contains.as_ref().map(|value| value.to_lowercase());
        self.records
            .iter()
            .filter(|record| filter.level.is_none_or(|level| record.level == level))
            .filter(|record| {
                filter
                    .since_unix_ms
                    .is_none_or(|since| record.timestamp_unix_ms >= since)
            })
            .filter(|record| {
                filter
                    .until_unix_ms
                    .is_none_or(|until| record.timestamp_unix_ms <= until)
            })
            .filter(|record| {
                needle
                    .as_ref()
                    .is_none_or(|needle| record.message.to_lowercase().contains(needle))
            })
            .cloned()
            .collect()
    }
}

#[derive(Clone, Debug)]
pub struct TelemetryStore {
    core_generation: CoreInstanceGeneration,
    logs: LogBuffer,
    traffic_capacity: usize,
    traffic_history: VecDeque<TrafficSample>,
    latest_traffic: Option<TrafficSample>,
    connection_count: Option<u64>,
}

impl TelemetryStore {
    pub fn new(
        core_generation: CoreInstanceGeneration,
        log_capacity: usize,
        max_log_line_bytes: usize,
        traffic_capacity: usize,
    ) -> Result<Self, TelemetryError> {
        if traffic_capacity == 0 {
            return Err(TelemetryError::InvalidLimit);
        }
        Ok(Self {
            core_generation,
            logs: LogBuffer::new(log_capacity, max_log_line_bytes)?,
            traffic_capacity,
            traffic_history: VecDeque::with_capacity(traffic_capacity),
            latest_traffic: None,
            connection_count: None,
        })
    }

    pub fn replace_core(&mut self, generation: CoreInstanceGeneration) {
        self.core_generation = generation;
        self.latest_traffic = None;
        self.traffic_history.clear();
        self.connection_count = None;
    }

    pub fn publish_traffic(
        &mut self,
        generation: CoreInstanceGeneration,
        sample: TrafficSample,
    ) -> bool {
        if generation != self.core_generation {
            return false;
        }
        if self.traffic_history.len() == self.traffic_capacity {
            self.traffic_history.pop_front();
        }
        self.traffic_history.push_back(sample.clone());
        self.latest_traffic = Some(sample);
        true
    }

    pub fn publish_connections(&mut self, generation: CoreInstanceGeneration, count: u64) -> bool {
        if generation != self.core_generation {
            return false;
        }
        self.connection_count = Some(count);
        true
    }

    pub fn publish_log(
        &mut self,
        generation: CoreInstanceGeneration,
        timestamp_unix_ms: u64,
        level: LogLevel,
        source: LogSource,
        message: impl Into<String>,
    ) -> Result<bool, TelemetryError> {
        if generation != self.core_generation {
            return Ok(false);
        }
        self.logs.push(timestamp_unix_ms, level, source, message)?;
        Ok(true)
    }

    #[must_use]
    pub fn logs(&self) -> &LogBuffer {
        &self.logs
    }

    #[must_use]
    pub fn traffic_history(&self) -> &VecDeque<TrafficSample> {
        &self.traffic_history
    }

    #[must_use]
    pub fn latest_traffic(&self) -> Option<&TrafficSample> {
        self.latest_traffic.as_ref()
    }

    #[must_use]
    pub fn connection_count(&self) -> Option<u64> {
        self.connection_count
    }
}
