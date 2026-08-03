//! Bounded status and Core Log stream transport and fan-out.

use std::fmt;
use std::net::Shutdown;
use std::os::unix::net::UnixStream;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, Weak};
use std::time::Duration;

use serde::Serialize;

use crate::application::ApplicationError;
use crate::constants::{
    CORE_LOG_LINE_MAX_BYTES, IPC_FRAME_MAX_BYTES, LOG_BROKER_RECOVERY_CAPACITY,
    LOG_BROKER_RECOVERY_MAX_BYTES, LOG_SUBSCRIBER_CAPACITY, LOG_TAIL_MAX_BYTES,
    LOG_TAIL_MAX_RECORDS, STATUS_SUBSCRIBER_CAPACITY,
};
use crate::domain::StatusSnapshot;
use crate::ipc::{
    IpcResponse, IpcStreamFrame, IpcStreamPayload, LogStreamItem, LogSubscriber, LogTailV1,
    RequestId, StatusStreamItem, StatusSubscriber, read_frame,
};
use crate::telemetry::{CoreLogRecord, LogTail};
use crate::unix_io::DeadlineUnixStream;

use super::client_error::{application_error, connect_error, protocol_error, read_error};

const LOG_TAIL_ENVELOPE_MAX_BYTES: usize = 4_096;

#[derive(Clone)]
pub struct IpcStreamCancellation {
    inner: Arc<IpcStreamCancellationInner>,
}

struct IpcStreamCancellationInner {
    cancelled: AtomicBool,
    stream: UnixStream,
}

impl IpcStreamCancellation {
    pub(super) fn new(stream: UnixStream) -> Self {
        Self {
            inner: Arc::new(IpcStreamCancellationInner {
                cancelled: AtomicBool::new(false),
                stream,
            }),
        }
    }

    pub fn cancel(&self) {
        self.inner.cancelled.store(true, Ordering::Release);
        let _ = self.inner.stream.shutdown(Shutdown::Both);
    }

    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.inner.cancelled.load(Ordering::Acquire)
    }
}

impl fmt::Debug for IpcStreamCancellation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("IpcStreamCancellation")
            .field("cancelled", &self.is_cancelled())
            .finish()
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct GeneratedStreamItem<T> {
    pub connection_generation: u64,
    pub item: T,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StatusStreamUpdate {
    Snapshot {
        sequence: u64,
        timestamp_unix_ms: u64,
        snapshot: StatusSnapshot,
    },
    Delta {
        sequence: u64,
        timestamp_unix_ms: u64,
        patch: serde_json::Value,
        snapshot: StatusSnapshot,
    },
    ResyncRequired {
        expected_sequence: u64,
        observed_sequence: u64,
    },
}

pub(super) struct StreamTransport {
    stream: DeadlineUnixStream,
    request_id: RequestId,
    connection_generation: u64,
    cancellation: IpcStreamCancellation,
}

impl StreamTransport {
    pub(super) fn new(
        stream: DeadlineUnixStream,
        request_id: RequestId,
        connection_generation: u64,
        cancellation: IpcStreamCancellation,
    ) -> Self {
        Self {
            stream,
            request_id,
            connection_generation,
            cancellation,
        }
    }

    fn next_frame(&mut self) -> Result<Option<IpcStreamFrame>, ApplicationError> {
        if self.cancellation.is_cancelled() {
            return Ok(None);
        }
        self.stream.begin_read().map_err(connect_error)?;
        let value: serde_json::Value = match read_frame(&mut self.stream) {
            Ok(value) => value,
            Err(_) if self.cancellation.is_cancelled() => return Ok(None),
            Err(error) => return Err(read_error(error)),
        };
        if let Ok(frame) = serde_json::from_value::<IpcStreamFrame>(value.clone()) {
            frame
                .ensure_correlated(self.request_id)
                .map_err(|_| protocol_error("The IPC stream frame did not match the request"))?;
            return Ok(Some(frame));
        }
        if let Ok(response) = serde_json::from_value::<IpcResponse>(value) {
            response
                .ensure_correlated(self.request_id)
                .map_err(|_| protocol_error("The IPC response did not match the request"))?;
            if let Some(error) = response.error() {
                return Err(application_error(error));
            }
        }
        Err(protocol_error("The IPC stream frame is invalid"))
    }
}

impl Drop for StreamTransport {
    fn drop(&mut self) {
        self.cancellation.cancel();
    }
}

pub struct StatusStream {
    transport: StreamTransport,
    current_snapshot: Option<serde_json::Value>,
    last_sequence: Option<u64>,
    finished: bool,
}

impl StatusStream {
    pub(super) fn new(transport: StreamTransport) -> Self {
        Self {
            transport,
            current_snapshot: None,
            last_sequence: None,
            finished: false,
        }
    }

    #[must_use]
    pub fn cancellation(&self) -> IpcStreamCancellation {
        self.transport.cancellation.clone()
    }

    #[must_use]
    pub fn connection_generation(&self) -> u64 {
        self.transport.connection_generation
    }

    #[must_use]
    pub fn resume_after_sequence(&self) -> Option<u64> {
        self.last_sequence
    }

    pub fn next_item(
        &mut self,
    ) -> Result<Option<GeneratedStreamItem<StatusStreamUpdate>>, ApplicationError> {
        if self.finished {
            return Ok(None);
        }
        loop {
            let Some(frame) = self.transport.next_frame()? else {
                self.finished = true;
                return Ok(None);
            };
            let update = match frame.payload {
                IpcStreamPayload::Heartbeat => continue,
                IpcStreamPayload::Logs(_) => {
                    self.finished = true;
                    return Err(protocol_error(
                        "The status subscription received a log stream frame",
                    ));
                }
                IpcStreamPayload::Status(item) => self.decode_item(item)?,
            };
            return Ok(Some(GeneratedStreamItem {
                connection_generation: self.transport.connection_generation,
                item: update,
            }));
        }
    }

    fn decode_item(
        &mut self,
        item: StatusStreamItem,
    ) -> Result<StatusStreamUpdate, ApplicationError> {
        match item {
            StatusStreamItem::Snapshot {
                sequence,
                timestamp_unix_ms,
                snapshot,
            } => {
                if self.current_snapshot.is_some() {
                    self.finished = true;
                    return Err(protocol_error(
                        "The status subscription received an unexpected snapshot",
                    ));
                }
                let decoded = decode_status_snapshot(snapshot.clone())?;
                self.current_snapshot = Some(snapshot);
                self.last_sequence = Some(sequence);
                Ok(StatusStreamUpdate::Snapshot {
                    sequence,
                    timestamp_unix_ms,
                    snapshot: decoded,
                })
            }
            StatusStreamItem::Event {
                sequence,
                timestamp_unix_ms,
                event,
            } => {
                let Some(expected_sequence) =
                    self.last_sequence.and_then(|value| value.checked_add(1))
                else {
                    self.finished = true;
                    return Err(protocol_error(
                        "The status subscription requires a full snapshot",
                    ));
                };
                if sequence != expected_sequence {
                    self.finished = true;
                    return Err(protocol_error(
                        "The status subscription sequence is invalid",
                    ));
                }
                let Some(current) = self.current_snapshot.as_mut() else {
                    self.finished = true;
                    return Err(protocol_error(
                        "The status subscription started without a snapshot",
                    ));
                };
                apply_json_merge_patch(current, &event);
                let decoded = decode_status_snapshot(current.clone())?;
                self.last_sequence = Some(sequence);
                Ok(StatusStreamUpdate::Delta {
                    sequence,
                    timestamp_unix_ms,
                    patch: event,
                    snapshot: decoded,
                })
            }
            StatusStreamItem::ResyncRequired {
                expected_sequence,
                observed_sequence,
            } => {
                let valid = self.last_sequence.is_some_and(|last| {
                    last.checked_add(1)
                        .is_some_and(|next| expected_sequence >= next)
                }) && observed_sequence >= expected_sequence;
                if !valid {
                    self.finished = true;
                    return Err(protocol_error(
                        "The status resynchronization marker is invalid",
                    ));
                }
                self.finished = true;
                Ok(StatusStreamUpdate::ResyncRequired {
                    expected_sequence,
                    observed_sequence,
                })
            }
        }
    }
}

impl Iterator for StatusStream {
    type Item = Result<GeneratedStreamItem<StatusStreamUpdate>, ApplicationError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_item().transpose()
    }
}

pub struct LogStream {
    transport: StreamTransport,
    last_sequence: Option<u64>,
    finished: bool,
}

impl LogStream {
    pub(super) fn new(transport: StreamTransport, after_sequence: Option<u64>) -> Self {
        Self {
            transport,
            last_sequence: after_sequence,
            finished: false,
        }
    }

    #[must_use]
    pub fn cancellation(&self) -> IpcStreamCancellation {
        self.transport.cancellation.clone()
    }

    #[must_use]
    pub fn connection_generation(&self) -> u64 {
        self.transport.connection_generation
    }

    #[must_use]
    pub fn resume_after_sequence(&self) -> Option<u64> {
        self.last_sequence
    }

    pub fn next_item(
        &mut self,
    ) -> Result<Option<GeneratedStreamItem<LogStreamItem>>, ApplicationError> {
        if self.finished {
            return Ok(None);
        }
        loop {
            let Some(frame) = self.transport.next_frame()? else {
                self.finished = true;
                return Ok(None);
            };
            let item = match frame.payload {
                IpcStreamPayload::Heartbeat => continue,
                IpcStreamPayload::Status(_) => {
                    self.finished = true;
                    return Err(protocol_error(
                        "The log subscription received a status stream frame",
                    ));
                }
                IpcStreamPayload::Logs(item) => item,
            };
            self.validate_log_item(&item)?;
            if matches!(item, LogStreamItem::Gap { .. }) {
                self.finished = true;
            }
            return Ok(Some(GeneratedStreamItem {
                connection_generation: self.transport.connection_generation,
                item,
            }));
        }
    }

    fn validate_log_item(&mut self, item: &LogStreamItem) -> Result<(), ApplicationError> {
        match item {
            LogStreamItem::Record { record } => {
                if self.last_sequence.is_some_and(|sequence| {
                    sequence
                        .checked_add(1)
                        .is_none_or(|expected| expected != record.sequence)
                }) {
                    self.finished = true;
                    return Err(protocol_error("The log subscription sequence is invalid"));
                }
                self.last_sequence = Some(record.sequence);
            }
            LogStreamItem::Gap {
                after_sequence,
                latest_sequence,
            } => {
                if *after_sequence != self.last_sequence
                    || after_sequence.is_some_and(|after| *latest_sequence <= after)
                {
                    self.finished = true;
                    return Err(protocol_error("The log gap marker is invalid"));
                }
            }
        }
        Ok(())
    }
}

impl Iterator for LogStream {
    type Item = Result<GeneratedStreamItem<LogStreamItem>, ApplicationError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_item().transpose()
    }
}

fn encode_status_snapshot(status: StatusSnapshot) -> Result<serde_json::Value, StreamBrokerError> {
    super::wire::encode_status_snapshot(status).map_err(|_| StreamBrokerError::Encoding)
}

fn decode_status_snapshot(value: serde_json::Value) -> Result<StatusSnapshot, ApplicationError> {
    super::wire::decode_status_snapshot(value)
        .map_err(|_| protocol_error("The IPC status snapshot is invalid"))
}

fn apply_json_merge_patch(target: &mut serde_json::Value, patch: &serde_json::Value) {
    let serde_json::Value::Object(patch) = patch else {
        *target = patch.clone();
        return;
    };
    if !target.is_object() {
        *target = serde_json::Value::Object(serde_json::Map::new());
    }
    let Some(target) = target.as_object_mut() else {
        return;
    };
    for (key, value) in patch {
        if value.is_null() {
            target.remove(key);
        } else {
            apply_json_merge_patch(
                target.entry(key.clone()).or_insert(serde_json::Value::Null),
                value,
            );
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StreamBrokerError {
    Encoding,
    InvalidCapacity,
    ItemTooLarge,
    StatusSequence { expected: u64, actual: u64 },
    LogSequence { expected: u64, actual: u64 },
    SequenceExhausted,
}

impl fmt::Display for StreamBrokerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Encoding => formatter.write_str("IPC stream data could not be encoded"),
            Self::InvalidCapacity => formatter.write_str("IPC stream capacity must be positive"),
            Self::ItemTooLarge => formatter.write_str("IPC stream data exceeds the frame limit"),
            Self::StatusSequence { expected, actual } => write!(
                formatter,
                "status stream sequence {actual} does not match expected sequence {expected}"
            ),
            Self::LogSequence { expected, actual } => write!(
                formatter,
                "log stream sequence {actual} does not match expected sequence {expected}"
            ),
            Self::SequenceExhausted => formatter.write_str("IPC stream sequence is exhausted"),
        }
    }
}

impl std::error::Error for StreamBrokerError {}

#[derive(Clone)]
pub struct IpcStreamBroker {
    status: Arc<Mutex<StatusBrokerState>>,
    logs: Arc<Mutex<LogBrokerState>>,
}

struct StatusBrokerState {
    sequence: u64,
    timestamp_unix_ms: u64,
    snapshot: serde_json::Value,
    subscribers: Vec<Weak<StatusSubscription>>,
}

struct LogBrokerState {
    capacity: usize,
    dropped_total: u64,
    retained_bytes: usize,
    gap_before_earliest: bool,
    sequence_horizon: Option<u64>,
    records: std::collections::VecDeque<CoreLogRecord>,
    subscribers: Vec<Weak<LogSubscription>>,
}

pub(super) struct StatusSubscription {
    queue: Mutex<StatusSubscriber>,
    ready: Condvar,
}

pub(super) struct LogSubscription {
    queue: Mutex<LogSubscriber>,
    ready: Condvar,
}

impl IpcStreamBroker {
    pub fn new(
        sequence: u64,
        timestamp_unix_ms: u64,
        snapshot: StatusSnapshot,
    ) -> Result<Self, StreamBrokerError> {
        Self::with_log_capacity(
            sequence,
            timestamp_unix_ms,
            snapshot,
            LOG_BROKER_RECOVERY_CAPACITY,
        )
    }

    pub fn with_log_capacity(
        sequence: u64,
        timestamp_unix_ms: u64,
        snapshot: StatusSnapshot,
        log_capacity: usize,
    ) -> Result<Self, StreamBrokerError> {
        if log_capacity == 0 {
            return Err(StreamBrokerError::InvalidCapacity);
        }
        let snapshot = encode_status_snapshot(snapshot)?;
        ensure_stream_item_size(&snapshot)?;
        Ok(Self {
            status: Arc::new(Mutex::new(StatusBrokerState {
                sequence,
                timestamp_unix_ms,
                snapshot,
                subscribers: Vec::new(),
            })),
            logs: Arc::new(Mutex::new(LogBrokerState {
                capacity: log_capacity,
                dropped_total: 0,
                retained_bytes: 0,
                gap_before_earliest: false,
                sequence_horizon: None,
                records: std::collections::VecDeque::with_capacity(log_capacity),
                subscribers: Vec::new(),
            })),
        })
    }

    pub fn publish_status(
        &self,
        sequence: u64,
        timestamp_unix_ms: u64,
        snapshot: StatusSnapshot,
    ) -> Result<(), StreamBrokerError> {
        let snapshot = encode_status_snapshot(snapshot)?;
        ensure_stream_item_size(&snapshot)?;
        let mut state = self
            .status
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let expected = state
            .sequence
            .checked_add(1)
            .ok_or(StreamBrokerError::SequenceExhausted)?;
        if sequence != expected {
            return Err(StreamBrokerError::StatusSequence {
                expected,
                actual: sequence,
            });
        }
        let patch = json_merge_patch(&state.snapshot, &snapshot);
        ensure_stream_item_size(&patch)?;
        state.sequence = sequence;
        state.timestamp_unix_ms = timestamp_unix_ms;
        state.snapshot = snapshot;
        state.subscribers.retain(|subscriber| {
            let Some(subscriber) = subscriber.upgrade() else {
                return false;
            };
            subscriber.publish(sequence, timestamp_unix_ms, patch.clone());
            true
        });
        Ok(())
    }

    pub fn publish_log(&self, record: CoreLogRecord) -> Result<(), StreamBrokerError> {
        if record.message().len() > CORE_LOG_LINE_MAX_BYTES {
            return Err(StreamBrokerError::ItemTooLarge);
        }
        ensure_stream_item_size(&crate::ipc::LogRecordV1::from(&record))?;
        let mut state = self
            .logs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(horizon) = state.sequence_horizon {
            let expected = horizon
                .checked_add(1)
                .ok_or(StreamBrokerError::SequenceExhausted)?;
            if record.sequence() != expected {
                return Err(StreamBrokerError::LogSequence {
                    expected,
                    actual: record.sequence(),
                });
            }
        }
        state.sequence_horizon = Some(record.sequence());
        while state.records.len() == state.capacity
            || state.retained_bytes.saturating_add(record.message().len())
                > LOG_BROKER_RECOVERY_MAX_BYTES
        {
            let Some(dropped) = state.records.pop_front() else {
                break;
            };
            state.retained_bytes = state.retained_bytes.saturating_sub(dropped.message().len());
            state.dropped_total = state.dropped_total.saturating_add(1);
            state.gap_before_earliest = true;
        }
        state.retained_bytes = state.retained_bytes.saturating_add(record.message().len());
        state.records.push_back(record.clone());
        state.subscribers.retain(|subscriber| {
            let Some(subscriber) = subscriber.upgrade() else {
                return false;
            };
            subscriber.publish(&record);
            true
        });
        Ok(())
    }

    pub fn synchronize_log_tail(&self, tail: LogTail) -> Result<(), StreamBrokerError> {
        validate_log_tail(&tail)?;
        let mut state = self
            .logs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if tail.records.is_empty()
            && tail.latest_sequence == state.records.back().map(CoreLogRecord::sequence)
        {
            let previous_horizon = state.sequence_horizon;
            state.sequence_horizon = max_sequence(state.sequence_horizon, tail.sequence_horizon);
            if tail.gap
                && let Some(sequence_horizon) = state.sequence_horizon
                && previous_horizon.is_none_or(|previous| sequence_horizon > previous)
            {
                state.subscribers.retain(|subscriber| {
                    let Some(subscriber) = subscriber.upgrade() else {
                        return false;
                    };
                    subscriber.publish_gap(sequence_horizon);
                    true
                });
            }
            state.dropped_total = state.dropped_total.max(tail.dropped_total);
            state.gap_before_earliest |= tail.gap;
            return Ok(());
        }
        state.subscribers.retain(|subscriber| {
            let Some(subscriber) = subscriber.upgrade() else {
                return false;
            };
            for record in &tail.records {
                subscriber.publish(record);
            }
            true
        });
        let source_gap = tail.gap;
        let sequence_horizon = tail.sequence_horizon;
        let mut records = std::collections::VecDeque::with_capacity(state.capacity);
        let mut retained_bytes = 0_usize;
        let mut truncated = 0_usize;
        for record in tail.records {
            retained_bytes = retained_bytes.saturating_add(record.message().len());
            records.push_back(record);
            while records.len() > state.capacity || retained_bytes > LOG_BROKER_RECOVERY_MAX_BYTES {
                let Some(dropped) = records.pop_front() else {
                    break;
                };
                retained_bytes = retained_bytes.saturating_sub(dropped.message().len());
                truncated = truncated.saturating_add(1);
            }
        }
        state.records = records;
        state.retained_bytes = retained_bytes;
        state.sequence_horizon = sequence_horizon;
        let synchronized_dropped = tail
            .dropped_total
            .saturating_add(u64::try_from(truncated).unwrap_or(u64::MAX));
        state.dropped_total = state.dropped_total.max(synchronized_dropped);
        state.gap_before_earliest = source_gap || truncated > 0;
        Ok(())
    }

    pub(super) fn subscribe_status(
        &self,
    ) -> Result<(StatusStreamItem, Arc<StatusSubscription>), StreamBrokerError> {
        let mut state = self
            .status
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state
            .subscribers
            .retain(|subscriber| subscriber.strong_count() > 0);
        let mut queue = StatusSubscriber::new(
            STATUS_SUBSCRIBER_CAPACITY,
            state.sequence,
            state.timestamp_unix_ms,
            state.snapshot.clone(),
        )
        .map_err(|_| StreamBrokerError::InvalidCapacity)?;
        let initial = queue.pop_front().ok_or(StreamBrokerError::Encoding)?;
        let subscriber = Arc::new(StatusSubscription {
            queue: Mutex::new(queue),
            ready: Condvar::new(),
        });
        state.subscribers.push(Arc::downgrade(&subscriber));
        Ok((initial, subscriber))
    }

    pub(super) fn subscribe_logs(
        &self,
        after_sequence: Option<u64>,
    ) -> Result<Arc<LogSubscription>, StreamBrokerError> {
        let mut state = self
            .logs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state
            .subscribers
            .retain(|subscriber| subscriber.strong_count() > 0);
        let mut queue = LogSubscriber::new(LOG_SUBSCRIBER_CAPACITY, after_sequence)
            .map_err(|_| StreamBrokerError::InvalidCapacity)?;
        let requires_resync = after_sequence.is_some_and(|after| {
            sequence_has_gap(
                after,
                state
                    .records
                    .iter()
                    .filter(move |record| record.sequence() > after)
                    .map(CoreLogRecord::sequence),
                state.sequence_horizon,
            )
        });
        if requires_resync {
            let horizon = state.sequence_horizon.ok_or(StreamBrokerError::Encoding)?;
            queue.mark_gap(horizon);
        } else if after_sequence.is_some() {
            for record in state
                .records
                .iter()
                .filter(|record| after_sequence.is_none_or(|sequence| record.sequence() > sequence))
            {
                queue.publish(record);
            }
        }
        let subscriber = Arc::new(LogSubscription {
            queue: Mutex::new(queue),
            ready: Condvar::new(),
        });
        state.subscribers.push(Arc::downgrade(&subscriber));
        Ok(subscriber)
    }

    pub(super) fn log_tail(&self, after_sequence: Option<u64>) -> LogTailV1 {
        let state = self
            .logs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let earliest_sequence = state.records.front().map(CoreLogRecord::sequence);
        let latest_sequence = state.records.back().map(CoreLogRecord::sequence);
        let sequence_horizon = state.sequence_horizon;
        let mut records = Vec::with_capacity(LOG_TAIL_MAX_RECORDS);
        let mut encoded_bytes = LOG_TAIL_ENVELOPE_MAX_BYTES;
        let mut truncated = false;
        for record in state
            .records
            .iter()
            .rev()
            .filter(|record| after_sequence.is_none_or(|after| record.sequence() > after))
        {
            let projected = crate::ipc::LogRecordV1::from(record);
            let record_bytes = serde_json::to_vec(&projected)
                .map_or(LOG_TAIL_MAX_BYTES, |value| value.len().saturating_add(1));
            if records.len() == LOG_TAIL_MAX_RECORDS
                || encoded_bytes.saturating_add(record_bytes) > LOG_TAIL_MAX_BYTES
            {
                truncated = true;
                break;
            }
            encoded_bytes = encoded_bytes.saturating_add(record_bytes);
            records.push(projected);
        }
        records.reverse();
        let source_gap = after_sequence.map_or_else(
            || {
                state.gap_before_earliest
                    || wire_records_have_gap(&records)
                    || sequence_horizon > latest_sequence
            },
            |after| wire_sequence_has_gap(after, &records, sequence_horizon),
        );
        LogTailV1 {
            earliest_sequence: if truncated {
                records.first().map(|record| record.sequence)
            } else {
                earliest_sequence
            },
            latest_sequence,
            sequence_horizon,
            records,
            dropped_total: state.dropped_total,
            gap: source_gap || truncated,
        }
    }

    pub(super) fn notify_all(&self) {
        let status = self
            .status
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for subscriber in status.subscribers.iter().filter_map(Weak::upgrade) {
            subscriber.ready.notify_all();
        }
        drop(status);
        let logs = self
            .logs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for subscriber in logs.subscribers.iter().filter_map(Weak::upgrade) {
            subscriber.ready.notify_all();
        }
    }
}

fn validate_log_tail(tail: &LogTail) -> Result<(), StreamBrokerError> {
    let mut previous: Option<u64> = None;
    for record in &tail.records {
        if record.message().len() > CORE_LOG_LINE_MAX_BYTES {
            return Err(StreamBrokerError::ItemTooLarge);
        }
        ensure_stream_item_size(&crate::ipc::LogRecordV1::from(record))?;
        if let Some(previous) = previous {
            let expected = previous
                .checked_add(1)
                .ok_or(StreamBrokerError::SequenceExhausted)?;
            if record.sequence() < expected || (record.sequence() != expected && !tail.gap) {
                return Err(StreamBrokerError::LogSequence {
                    expected,
                    actual: record.sequence(),
                });
            }
        }
        previous = Some(record.sequence());
    }
    if previous.is_some_and(|last| tail.latest_sequence != Some(last))
        || tail.latest_sequence > tail.sequence_horizon
        || (tail.latest_sequence < tail.sequence_horizon && !tail.gap)
    {
        return Err(StreamBrokerError::Encoding);
    }
    Ok(())
}

fn wire_records_have_gap(records: &[crate::ipc::LogRecordV1]) -> bool {
    records.windows(2).any(|window| {
        window[0]
            .sequence
            .checked_add(1)
            .is_none_or(|expected| window[1].sequence != expected)
    })
}

fn wire_sequence_has_gap(
    after: u64,
    records: &[crate::ipc::LogRecordV1],
    sequence_horizon: Option<u64>,
) -> bool {
    sequence_has_gap(
        after,
        records.iter().map(|record| record.sequence),
        sequence_horizon,
    )
}

fn sequence_has_gap(
    after: u64,
    sequences: impl IntoIterator<Item = u64>,
    sequence_horizon: Option<u64>,
) -> bool {
    let Some(mut expected) = after.checked_add(1) else {
        return false;
    };
    for sequence in sequences {
        if sequence > expected {
            return true;
        }
        expected = sequence.saturating_add(1);
    }
    sequence_horizon.is_some_and(|horizon| horizon >= expected)
}

fn max_sequence(left: Option<u64>, right: Option<u64>) -> Option<u64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.max(right)),
        (left, right) => left.or(right),
    }
}

impl fmt::Debug for IpcStreamBroker {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("IpcStreamBroker")
            .finish_non_exhaustive()
    }
}

impl StatusSubscription {
    fn publish(&self, sequence: u64, timestamp_unix_ms: u64, patch: serde_json::Value) {
        self.queue
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .publish(sequence, timestamp_unix_ms, patch);
        self.ready.notify_one();
    }

    pub(super) fn wait_next(
        &self,
        shutdown: &AtomicBool,
        timeout: Duration,
    ) -> Option<StatusStreamItem> {
        wait_for_item(&self.queue, &self.ready, shutdown, timeout, |queue| {
            queue.pop_front()
        })
    }
}

impl LogSubscription {
    fn publish(&self, record: &CoreLogRecord) {
        self.queue
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .publish(record);
        self.ready.notify_one();
    }

    fn publish_gap(&self, latest_sequence: u64) {
        self.queue
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .mark_gap(latest_sequence);
        self.ready.notify_one();
    }

    pub(super) fn wait_next(
        &self,
        shutdown: &AtomicBool,
        timeout: Duration,
    ) -> Option<LogStreamItem> {
        wait_for_item(&self.queue, &self.ready, shutdown, timeout, |queue| {
            queue.pop_front()
        })
    }
}

fn wait_for_item<Q, T>(
    queue: &Mutex<Q>,
    ready: &Condvar,
    shutdown: &AtomicBool,
    timeout: Duration,
    mut pop: impl FnMut(&mut Q) -> Option<T>,
) -> Option<T> {
    let mut queue = queue
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if let Some(item) = pop(&mut queue) {
        return Some(item);
    }
    if shutdown.load(Ordering::Acquire) {
        return None;
    }
    let (mut queue, _) = ready
        .wait_timeout(queue, timeout)
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    pop(&mut queue)
}

fn ensure_stream_item_size(value: &impl Serialize) -> Result<(), StreamBrokerError> {
    let size = serde_json::to_vec(value)
        .map_err(|_| StreamBrokerError::Encoding)?
        .len();
    if size > IPC_FRAME_MAX_BYTES.saturating_sub(4_096) {
        Err(StreamBrokerError::ItemTooLarge)
    } else {
        Ok(())
    }
}

fn json_merge_patch(
    previous: &serde_json::Value,
    current: &serde_json::Value,
) -> serde_json::Value {
    match (previous, current) {
        (serde_json::Value::Object(previous), serde_json::Value::Object(current)) => {
            let mut patch = serde_json::Map::new();
            for key in previous.keys() {
                if !current.contains_key(key) {
                    patch.insert(key.clone(), serde_json::Value::Null);
                }
            }
            for (key, value) in current {
                match previous.get(key) {
                    Some(previous) if previous == value => {}
                    Some(previous) => {
                        patch.insert(key.clone(), json_merge_patch(previous, value));
                    }
                    None => {
                        patch.insert(key.clone(), value.clone());
                    }
                }
            }
            serde_json::Value::Object(patch)
        }
        _ => current.clone(),
    }
}
