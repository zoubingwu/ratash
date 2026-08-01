//! Live user-local IPC client and server adapters.

use std::fmt;
use std::fs;
use std::io;
use std::net::Shutdown;
use std::os::unix::fs::MetadataExt;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender, TrySendError};
use std::sync::{Arc, Condvar, Mutex, Weak};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use mio::net::UnixListener as MioUnixListener;
use mio::{Events, Interest, Poll, Token, Waker};
use serde::{Deserialize, Serialize};
use socket2::{Domain, SockAddr, Socket, Type};

use crate::application::{
    ApplicationClient, ApplicationError, ApplicationErrorDetails, ApplicationOperation,
    ApplicationOutput, LatencyFreshness, LatencyListOutcome, LatencyProbeStatus,
    LatencyShowOutcome, LatencySummary, LifecycleAction, LifecycleOutcome, LogGap, LogMetadata,
    PolicyTargetValidation, ProfileListOutcome, ProfileMutationAction, ProfileMutationOutcome,
    ProfileRefreshFailure, ProfileRefreshStage, ProfileRefreshState, ProfileSummary,
    ProxyAvailability, ProxyGroupSummary, ProxyListOutcome, ProxyMemberKind, ProxyNodeRow,
    ProxyNodeSource, ProxySelectionOutcome, RecoveryOutcome, RecoveryStatus, RuleListOutcome,
    RuleMutationAction, RuleMutationOutcome, RulePlacement as ApplicationRulePlacement,
    RuleSummary, RuntimeApplyFailureDetails, RuntimeApplyFailureStage, RuntimeApplyOutcome,
    RuntimeApplyStatus, SelectorCandidate, SelectorIdentity, SelectorKind,
};
use crate::constants::{
    CORE_LOG_LINE_MAX_BYTES, IPC_FRAME_MAX_BYTES, IPC_PROFILE_ADD_TIMEOUT, IPC_REQUEST_TIMEOUT,
    IPC_RUNTIME_MUTATION_TIMEOUT, LOG_CAPACITY, LOG_SUBSCRIBER_CAPACITY,
    STATUS_SUBSCRIBER_CAPACITY,
};
use crate::domain::{
    ActiveProfileSummary, ApplyState, CoreDiagnosticCategory, CoreInstanceGeneration,
    CoreLifecycle, CoreRestartStatus, CoreStatus, LatencySample, LocalRuleSetRevision,
    NodeRecordId, ProbeGeneration, ProbeQueueStatus, ProfileId, ProxyGroupId, RuntimeApplyPhase,
    RuntimeApplySnapshot, RuntimeGeneration, RuntimeRecoverySnapshot, RuntimeRecoveryStatus,
    SampleState, SelectedNodeSummary, StatusSnapshot, StreamHealthSet, StreamState,
    SubscriptionUrl, SupervisorLifecycle, SupervisorStatus, TrafficSample, TunReason, TunStatus,
};
use crate::error::ErrorCode;
use crate::ipc::{
    EmptyPayload, IpcError, IpcRequest, IpcResponse, IpcStreamFrame, IpcStreamPayload,
    LogStreamItem, LogSubscriber, LogSubscriptionPayload, LogTailPayload, LogTailV1,
    NodeSelectorPayload, OperationConversionError, PeerAuthorizationError, PeerAuthorizer,
    ProfileAddPayload, ProfileSelectorPayload, ProxyListPayload, ProxySelectPayload, RequestId,
    RequestOperation, RuleAddPayload, RulePlacement, RuleReplacePayload, RuleSelectorPayload,
    StatusStreamItem, StatusSubscriber, StatusSubscriptionPayload, bind_private_listener,
    read_frame, write_frame,
};
use crate::telemetry::{CoreLogRecord, LogTail};

use crate::unix_io::DeadlineUnixStream;

const DEFAULT_SERVER_WORKERS: usize = 4;
const DEFAULT_PENDING_CONNECTIONS: usize = 32;
const LISTENER_TOKEN: Token = Token(0);
const SHUTDOWN_TOKEN: Token = Token(1);
// -----------------------------------------------------------------------------
// Synchronous client
// -----------------------------------------------------------------------------

pub struct IpcClient {
    socket_path: PathBuf,
    connect_timeout: Duration,
    timeout_policy: IpcTimeoutPolicy,
    next_request_id: AtomicU64,
}

#[derive(Clone, Copy, Debug)]
enum IpcTimeoutPolicy {
    Product,
    Fixed(Duration),
}

impl IpcClient {
    #[must_use]
    pub fn new(socket_path: impl Into<PathBuf>) -> Self {
        Self {
            socket_path: socket_path.into(),
            connect_timeout: IPC_REQUEST_TIMEOUT,
            timeout_policy: IpcTimeoutPolicy::Product,
            next_request_id: AtomicU64::new(1),
        }
    }

    #[must_use]
    pub fn with_timeouts(
        socket_path: impl Into<PathBuf>,
        connect_timeout: Duration,
        io_timeout: Duration,
    ) -> Self {
        Self {
            socket_path: socket_path.into(),
            connect_timeout,
            timeout_policy: IpcTimeoutPolicy::Fixed(io_timeout),
            next_request_id: AtomicU64::new(1),
        }
    }

    fn request_id(&self) -> RequestId {
        let request_id = self.next_request_id.fetch_add(1, Ordering::Relaxed);
        if request_id == 0 {
            RequestId(self.next_request_id.fetch_add(1, Ordering::Relaxed))
        } else {
            RequestId(request_id)
        }
    }

    fn connect(&self) -> io::Result<UnixStream> {
        if self.connect_timeout.is_zero() || self.stream_timeout().is_zero() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "IPC deadlines must be positive",
            ));
        }
        let socket = Socket::new(Domain::UNIX, Type::STREAM, None)?;
        let address = SockAddr::unix(&self.socket_path)?;
        socket.connect_timeout(&address, self.connect_timeout)?;
        let stream = UnixStream::from(socket);
        Ok(stream)
    }

    fn stream_timeout(&self) -> Duration {
        match self.timeout_policy {
            IpcTimeoutPolicy::Product => IPC_REQUEST_TIMEOUT,
            IpcTimeoutPolicy::Fixed(timeout) => timeout,
        }
    }

    fn response_timeout(&self, operation: &ApplicationOperation) -> Duration {
        match self.timeout_policy {
            IpcTimeoutPolicy::Fixed(timeout) => timeout,
            IpcTimeoutPolicy::Product => match operation {
                ApplicationOperation::ProfileAdd { .. } => IPC_PROFILE_ADD_TIMEOUT,
                ApplicationOperation::ProfileUse { .. }
                | ApplicationOperation::RuleAdd { .. }
                | ApplicationOperation::RuleReplace { .. }
                | ApplicationOperation::RuleRemove { .. } => IPC_RUNTIME_MUTATION_TIMEOUT,
                _ => IPC_REQUEST_TIMEOUT,
            },
        }
    }

    pub fn subscribe_status(
        &self,
        after_sequence: Option<u64>,
        connection_generation: u64,
    ) -> Result<StatusStream, ApplicationError> {
        let transport = self.open_stream(
            RequestOperation::SubscribeStatus(StatusSubscriptionPayload { after_sequence }),
            connection_generation,
        )?;
        Ok(StatusStream {
            transport,
            current_snapshot: None,
            last_sequence: None,
            finished: false,
        })
    }

    pub fn follow_logs(
        &self,
        after_sequence: Option<u64>,
        connection_generation: u64,
    ) -> Result<LogStream, ApplicationError> {
        let transport = self.open_stream(
            RequestOperation::FollowLogs(LogSubscriptionPayload { after_sequence }),
            connection_generation,
        )?;
        Ok(LogStream {
            transport,
            last_sequence: after_sequence,
            finished: false,
        })
    }

    pub fn log_tail(&self, after_sequence: Option<u64>) -> Result<LogTailV1, ApplicationError> {
        let request_id = self.request_id();
        let request = IpcRequest::new(
            request_id,
            RequestOperation::LogTail(LogTailPayload { after_sequence }),
        );
        let stream = self.connect().map_err(connect_error)?;
        let mut stream =
            DeadlineUnixStream::new(stream, self.stream_timeout()).map_err(connect_error)?;
        stream.begin_write().map_err(connect_error)?;
        write_frame(&mut stream, &request).map_err(write_error)?;
        stream.begin_read().map_err(connect_error)?;
        let response: IpcResponse = read_frame(&mut stream).map_err(read_error)?;
        response
            .ensure_correlated(request_id)
            .map_err(|_| protocol_error("The IPC response did not match the request"))?;
        if let Some(error) = response.error() {
            return Err(application_error(error));
        }
        serde_json::from_value(
            response
                .data()
                .cloned()
                .ok_or_else(|| protocol_error("The IPC response outcome is incomplete"))?,
        )
        .map_err(|_| protocol_error("The IPC log tail response is invalid"))
    }

    fn open_stream(
        &self,
        operation: RequestOperation,
        connection_generation: u64,
    ) -> Result<StreamTransport, ApplicationError> {
        let request_id = self.request_id();
        let request = IpcRequest::new(request_id, operation);
        let stream = self.connect().map_err(connect_error)?;
        let cancellation = IpcStreamCancellation::new(
            stream
                .try_clone()
                .map_err(|_| connect_error(io::Error::other("IPC stream clone failed")))?,
        );
        let mut stream =
            DeadlineUnixStream::new(stream, self.stream_timeout()).map_err(connect_error)?;
        stream.begin_write().map_err(connect_error)?;
        write_frame(&mut stream, &request).map_err(write_error)?;
        Ok(StreamTransport {
            stream,
            request_id,
            connection_generation,
            cancellation,
        })
    }
}

#[derive(Clone)]
pub struct IpcStreamCancellation {
    inner: Arc<IpcStreamCancellationInner>,
}

struct IpcStreamCancellationInner {
    cancelled: AtomicBool,
    stream: UnixStream,
}

impl IpcStreamCancellation {
    fn new(stream: UnixStream) -> Self {
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

struct StreamTransport {
    stream: DeadlineUnixStream,
    request_id: RequestId,
    connection_generation: u64,
    cancellation: IpcStreamCancellation,
}

impl StreamTransport {
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
    serde_json::to_value(WireStatusSnapshot::from(status)).map_err(|_| StreamBrokerError::Encoding)
}

fn decode_status_snapshot(value: serde_json::Value) -> Result<StatusSnapshot, ApplicationError> {
    serde_json::from_value::<WireStatusSnapshot>(value)
        .map_err(|_| protocol_error("The IPC status snapshot is invalid"))?
        .try_into()
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
    let target = target
        .as_object_mut()
        .expect("the target was initialized as an object");
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
    records: std::collections::VecDeque<CoreLogRecord>,
    subscribers: Vec<Weak<LogSubscription>>,
}

struct StatusSubscription {
    queue: Mutex<StatusSubscriber>,
    ready: Condvar,
}

struct LogSubscription {
    queue: Mutex<LogSubscriber>,
    ready: Condvar,
}

impl IpcStreamBroker {
    pub fn new(
        sequence: u64,
        timestamp_unix_ms: u64,
        snapshot: StatusSnapshot,
    ) -> Result<Self, StreamBrokerError> {
        Self::with_log_capacity(sequence, timestamp_unix_ms, snapshot, LOG_CAPACITY)
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
        if let Some(latest) = state.records.back().map(CoreLogRecord::sequence) {
            let expected = latest
                .checked_add(1)
                .ok_or(StreamBrokerError::SequenceExhausted)?;
            if record.sequence() != expected {
                return Err(StreamBrokerError::LogSequence {
                    expected,
                    actual: record.sequence(),
                });
            }
        }
        if state.records.len() == state.capacity {
            state.records.pop_front();
            state.dropped_total = state.dropped_total.saturating_add(1);
        }
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
        state.subscribers.retain(|subscriber| {
            let Some(subscriber) = subscriber.upgrade() else {
                return false;
            };
            for record in &tail.records {
                subscriber.publish(record);
            }
            true
        });
        let truncated = tail.records.len().saturating_sub(state.capacity);
        state.records = tail
            .records
            .into_iter()
            .skip(truncated)
            .collect::<std::collections::VecDeque<_>>();
        state.dropped_total = tail
            .dropped_total
            .saturating_add(u64::try_from(truncated).unwrap_or(u64::MAX));
        Ok(())
    }

    fn subscribe_status(&self) -> (StatusStreamItem, Arc<StatusSubscription>) {
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
        .expect("the status subscriber capacity is positive");
        let initial = queue
            .pop_front()
            .expect("a new status subscriber contains its snapshot");
        let subscriber = Arc::new(StatusSubscription {
            queue: Mutex::new(queue),
            ready: Condvar::new(),
        });
        state.subscribers.push(Arc::downgrade(&subscriber));
        (initial, subscriber)
    }

    fn subscribe_logs(&self, after_sequence: Option<u64>) -> Arc<LogSubscription> {
        let mut state = self
            .logs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state
            .subscribers
            .retain(|subscriber| subscriber.strong_count() > 0);
        let anchor = after_sequence.or_else(|| state.records.back().map(CoreLogRecord::sequence));
        let mut queue = LogSubscriber::new(LOG_SUBSCRIBER_CAPACITY, anchor)
            .expect("the log subscriber capacity is positive");
        if after_sequence.is_some() {
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
        subscriber
    }

    fn log_tail(&self, after_sequence: Option<u64>) -> LogTailV1 {
        let state = self
            .logs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let earliest_sequence = state.records.front().map(CoreLogRecord::sequence);
        let latest_sequence = state.records.back().map(CoreLogRecord::sequence);
        let source_gap = after_sequence
            .zip(earliest_sequence)
            .is_some_and(|(after, earliest)| {
                after.checked_add(1).is_some_and(|next| next < earliest)
            });
        let candidates = state
            .records
            .iter()
            .filter(|record| after_sequence.is_none_or(|after| record.sequence() > after))
            .collect::<Vec<_>>();
        let mut records = Vec::new();
        let mut encoded_bytes = 512_usize;
        let payload_budget = IPC_FRAME_MAX_BYTES.saturating_sub(4_096);
        for record in candidates.iter().rev() {
            let projected = crate::ipc::LogRecordV1::from(*record);
            let record_bytes = serde_json::to_vec(&projected)
                .map_or(payload_budget, |value| value.len().saturating_add(1));
            if encoded_bytes.saturating_add(record_bytes) > payload_budget {
                break;
            }
            encoded_bytes = encoded_bytes.saturating_add(record_bytes);
            records.push(projected);
        }
        records.reverse();
        let truncated = records.len() < candidates.len();
        LogTailV1 {
            earliest_sequence: if truncated {
                records.first().map(|record| record.sequence)
            } else {
                earliest_sequence
            },
            latest_sequence,
            records,
            dropped_total: state.dropped_total,
            gap: source_gap || truncated,
        }
    }

    fn notify_all(&self) {
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
            if record.sequence() != expected {
                return Err(StreamBrokerError::LogSequence {
                    expected,
                    actual: record.sequence(),
                });
            }
        }
        previous = Some(record.sequence());
    }
    if let Some(last) = previous
        && tail.latest_sequence != Some(last)
    {
        return Err(StreamBrokerError::Encoding);
    }
    Ok(())
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

    fn wait_next(&self, shutdown: &AtomicBool, timeout: Duration) -> Option<StatusStreamItem> {
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

    fn wait_next(&self, shutdown: &AtomicBool, timeout: Duration) -> Option<LogStreamItem> {
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

impl fmt::Debug for IpcClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("IpcClient")
            .field("socket_path", &"[REDACTED]")
            .field("connect_timeout", &self.connect_timeout)
            .field("timeout_policy", &self.timeout_policy)
            .finish_non_exhaustive()
    }
}

impl ApplicationClient for IpcClient {
    fn execute(
        &self,
        operation: ApplicationOperation,
    ) -> Result<ApplicationOutput, ApplicationError> {
        let expected_output = ExpectedOutput::for_operation(&operation);
        let response_timeout = self.response_timeout(&operation);
        let request_id = self.request_id();
        let request = IpcRequest::new(request_id, request_operation(operation));
        let stream = self.connect().map_err(connect_error)?;
        let mut stream =
            DeadlineUnixStream::new(stream, response_timeout).map_err(connect_error)?;
        stream.begin_write().map_err(connect_error)?;
        write_frame(&mut stream, &request).map_err(write_error)?;
        stream.begin_read().map_err(connect_error)?;
        let response: IpcResponse = read_frame(&mut stream).map_err(read_error)?;
        response
            .ensure_correlated(request_id)
            .map_err(|_| protocol_error("The IPC response did not match the request"))?;

        if let Some(error) = response.error() {
            return Err(application_error(error));
        }
        let data = response
            .data()
            .cloned()
            .ok_or_else(|| protocol_error("The IPC response outcome is incomplete"))?;
        let output = serde_json::from_value::<WireApplicationOutput>(data)
            .map_err(|_| protocol_error("The IPC response data is invalid"))?
            .try_into()
            .map_err(|_| protocol_error("The IPC response data is invalid"))?;
        if expected_output.matches(&output) {
            Ok(output)
        } else {
            Err(protocol_error(
                "The IPC response output does not match the request",
            ))
        }
    }
}

fn connect_error(_error: io::Error) -> ApplicationError {
    ApplicationError::new(
        ErrorCode::SupervisorUnavailable,
        "The Hopash Supervisor IPC endpoint is unavailable",
        true,
    )
}

fn write_error(error: crate::ipc::FrameError) -> ApplicationError {
    match error {
        crate::ipc::FrameError::Io(error) if is_timeout(&error) => ApplicationError::new(
            ErrorCode::SupervisorUnavailable,
            "Timed out sending the Supervisor IPC request",
            true,
        ),
        crate::ipc::FrameError::Io(_) => ApplicationError::new(
            ErrorCode::SupervisorUnavailable,
            "The Supervisor IPC request could not be sent",
            true,
        ),
        crate::ipc::FrameError::Json(_) | crate::ipc::FrameError::FrameTooLarge { .. } => {
            ApplicationError::new(
                ErrorCode::Internal,
                "The application request could not be encoded",
                false,
            )
        }
    }
}

fn read_error(error: crate::ipc::FrameError) -> ApplicationError {
    match error {
        crate::ipc::FrameError::Io(error) if is_timeout(&error) => ApplicationError::new(
            ErrorCode::SupervisorUnavailable,
            "Timed out waiting for the Supervisor IPC response",
            true,
        ),
        crate::ipc::FrameError::Io(error)
            if matches!(
                error.kind(),
                io::ErrorKind::UnexpectedEof
                    | io::ErrorKind::ConnectionReset
                    | io::ErrorKind::BrokenPipe
            ) =>
        {
            ApplicationError::new(
                ErrorCode::SupervisorUnavailable,
                "The Supervisor IPC connection closed before responding",
                true,
            )
        }
        crate::ipc::FrameError::Io(_)
        | crate::ipc::FrameError::Json(_)
        | crate::ipc::FrameError::FrameTooLarge { .. } => {
            protocol_error("The Supervisor IPC response frame is invalid")
        }
    }
}

fn is_timeout(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock
    )
}

fn protocol_error(message: &'static str) -> ApplicationError {
    ApplicationError::new(ErrorCode::ProtocolMismatch, message, false)
}

fn application_error(error: &IpcError) -> ApplicationError {
    let Some(code) = parse_error_code(&error.code) else {
        return protocol_error("The IPC response error code is unknown");
    };
    let mut result = ApplicationError::new(code, error.message.clone(), error.retryable);
    if let Some(details) = decode_runtime_apply_failure(error.details.as_ref()) {
        result = result.with_details(details);
    }
    if let Some(application_candidate_ids) = error
        .details
        .as_ref()
        .and_then(|details| details.get("application_candidate_ids"))
        .and_then(decode_candidate_ids)
    {
        result = result.with_details(ApplicationErrorDetails::CandidateIds {
            candidate_ids: application_candidate_ids,
        });
    }
    let selector_candidates = decode_selector_candidates(error.details.as_ref());
    if result.details.is_none() && selector_candidates.is_none() {
        if let Some(candidate_ids) = error
            .details
            .as_ref()
            .and_then(|details| details.get("candidate_ids"))
            .and_then(decode_candidate_ids)
        {
            result = result.with_details(ApplicationErrorDetails::CandidateIds { candidate_ids });
        }
    }
    if let Some((selector, candidates)) = selector_candidates {
        result = result.with_selector_candidates(selector, candidates);
    }
    result
}

fn decode_candidate_ids(value: &serde_json::Value) -> Option<Vec<String>> {
    value
        .as_array()?
        .iter()
        .map(|value| value.as_str().map(str::to_owned))
        .collect()
}

fn decode_runtime_apply_failure(
    details: Option<&serde_json::Value>,
) -> Option<ApplicationErrorDetails> {
    let details = details?.as_object()?;
    let stage = RuntimeApplyFailureStage::parse(details.get("stage")?.as_str()?)?;
    let candidate_generation = decode_optional_generation(details.get("candidate_generation"))?;
    let committed_generation = decode_optional_generation(details.get("committed_generation"))?;
    let recovery = details.get("recovery")?.as_object()?;
    let status = match recovery.get("status")?.as_str()? {
        "not_required" => RecoveryStatus::NotRequired,
        "succeeded" => RecoveryStatus::Succeeded,
        "pending" => RecoveryStatus::Pending,
        "failed" => RecoveryStatus::Failed,
        _ => return None,
    };
    let restored_generation = decode_optional_generation(recovery.get("restored_generation"))?;
    let message = match recovery.get("message") {
        None | Some(serde_json::Value::Null) => None,
        Some(message) => Some(message.as_str()?.to_owned()),
    };
    Some(ApplicationErrorDetails::RuntimeApplyFailure(Box::new(
        RuntimeApplyFailureDetails {
            candidate_generation,
            committed_generation,
            stage,
            recovery: RecoveryOutcome {
                status,
                restored_generation,
                message,
            },
        },
    )))
}

fn decode_optional_generation(
    value: Option<&serde_json::Value>,
) -> Option<Option<RuntimeGeneration>> {
    match value {
        None | Some(serde_json::Value::Null) => Some(None),
        Some(value) => value
            .as_str()?
            .parse::<u64>()
            .ok()
            .map(RuntimeGeneration)
            .map(Some),
    }
}

fn decode_selector_candidates(
    details: Option<&serde_json::Value>,
) -> Option<(SelectorKind, Vec<SelectorCandidate>)> {
    let details = details?.as_object()?;
    let selector = match details.get("selector")?.as_str()? {
        "profile" => SelectorKind::Profile,
        "proxy_group" => SelectorKind::ProxyGroup,
        "node" => SelectorKind::Node,
        "rule" => SelectorKind::Rule,
        _ => return None,
    };
    let candidates = details
        .get("candidates")?
        .as_array()?
        .iter()
        .map(|candidate| {
            let candidate = candidate.as_object()?;
            Some(SelectorCandidate::new(
                candidate.get("id")?.as_str()?,
                candidate.get("name")?.as_str()?,
            ))
        })
        .collect::<Option<Vec<_>>>()?;
    Some((selector, candidates))
}

fn parse_error_code(code: &str) -> Option<ErrorCode> {
    Some(match code {
        "usage" => ErrorCode::Usage,
        "supervisor_unavailable" => ErrorCode::SupervisorUnavailable,
        "protocol_mismatch" => ErrorCode::ProtocolMismatch,
        "profile_ambiguous" => ErrorCode::ProfileAmbiguous,
        "profile_active" => ErrorCode::ProfileActive,
        "profile_not_found" => ErrorCode::ProfileNotFound,
        "proxy_group_not_found" => ErrorCode::ProxyGroupNotFound,
        "node_not_found" => ErrorCode::NodeNotFound,
        "node_ambiguous" => ErrorCode::NodeAmbiguous,
        "invalid_subscription_url" => ErrorCode::InvalidSubscriptionUrl,
        "rules_uninitialized" => ErrorCode::RulesUninitialized,
        "rule_busy" => ErrorCode::RuleBusy,
        "rule_not_found" => ErrorCode::RuleNotFound,
        "rule_ambiguous" => ErrorCode::RuleAmbiguous,
        "rule_already_exists" => ErrorCode::RuleAlreadyExists,
        "policy_target_not_found" => ErrorCode::PolicyTargetNotFound,
        "profile_field_unsupported" => ErrorCode::ProfileFieldUnsupported,
        "tun_permission_denied" => ErrorCode::TunPermissionDenied,
        "core_unavailable" => ErrorCode::CoreUnavailable,
        "external_operation_failed" => ErrorCode::ExternalOperationFailed,
        "internal" => ErrorCode::Internal,
        "operation_unavailable" => ErrorCode::OperationUnavailable,
        _ => return None,
    })
}

fn request_operation(operation: ApplicationOperation) -> RequestOperation {
    match operation {
        ApplicationOperation::Start => RequestOperation::Start(EmptyPayload {}),
        ApplicationOperation::Stop => RequestOperation::Stop(EmptyPayload {}),
        ApplicationOperation::Restart => RequestOperation::Restart(EmptyPayload {}),
        ApplicationOperation::GetStatus => RequestOperation::GetStatus(EmptyPayload {}),
        ApplicationOperation::ProfileAdd { subscription_url } => {
            RequestOperation::ProfileAdd(ProfileAddPayload::new(&subscription_url))
        }
        ApplicationOperation::ProfileList => RequestOperation::ProfileList(EmptyPayload {}),
        ApplicationOperation::ProfileUse { profile } => {
            RequestOperation::ProfileUse(ProfileSelectorPayload { profile })
        }
        ApplicationOperation::ProfileRemove { profile } => {
            RequestOperation::ProfileRemove(ProfileSelectorPayload { profile })
        }
        ApplicationOperation::ProxyList { group } => {
            RequestOperation::ProxyList(ProxyListPayload { group })
        }
        ApplicationOperation::ProxySelect { group, node } => {
            RequestOperation::ProxySelect(ProxySelectPayload { group, node })
        }
        ApplicationOperation::LatencyList => RequestOperation::LatencyList(EmptyPayload {}),
        ApplicationOperation::LatencyShow { node } => {
            RequestOperation::LatencyShow(NodeSelectorPayload { node })
        }
        ApplicationOperation::RuleList => RequestOperation::RuleList(EmptyPayload {}),
        ApplicationOperation::RuleAdd { rule, placement } => {
            RequestOperation::RuleAdd(RuleAddPayload {
                rule,
                placement: match placement {
                    ApplicationRulePlacement::Prepend => RulePlacement::Prepend,
                    ApplicationRulePlacement::Append => RulePlacement::Append,
                    ApplicationRulePlacement::Before(anchor) => RulePlacement::Before(anchor),
                    ApplicationRulePlacement::After(anchor) => RulePlacement::After(anchor),
                },
            })
        }
        ApplicationOperation::RuleReplace { old_rule, new_rule } => {
            RequestOperation::RuleReplace(RuleReplacePayload { old_rule, new_rule })
        }
        ApplicationOperation::RuleRemove { rule } => {
            RequestOperation::RuleRemove(RuleSelectorPayload { rule })
        }
    }
}

// -----------------------------------------------------------------------------
// Same-user authorization
// -----------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SameUserPeerAuthorizer {
    expected_uid: u32,
}

impl SameUserPeerAuthorizer {
    #[must_use]
    pub fn current() -> Self {
        Self {
            expected_uid: nix::unistd::geteuid().as_raw(),
        }
    }
}

impl Default for SameUserPeerAuthorizer {
    fn default() -> Self {
        Self::current()
    }
}

impl PeerAuthorizer for SameUserPeerAuthorizer {
    fn authorize(&self, peer: &UnixStream) -> Result<(), PeerAuthorizationError> {
        let actual_uid = peer_uid(peer).map_err(|_| {
            PeerAuthorizationError::new("The IPC peer identity could not be verified")
        })?;
        if actual_uid == self.expected_uid {
            Ok(())
        } else {
            Err(PeerAuthorizationError::new(
                "The IPC peer belongs to a different user",
            ))
        }
    }
}

#[cfg(any(
    target_os = "macos",
    target_os = "ios",
    target_os = "watchos",
    target_os = "tvos",
    target_os = "visionos"
))]
fn peer_uid(peer: &UnixStream) -> nix::Result<u32> {
    nix::sys::socket::getsockopt(peer, nix::sys::socket::sockopt::LocalPeerCred)
        .map(|credentials| credentials.uid())
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn peer_uid(peer: &UnixStream) -> nix::Result<u32> {
    nix::sys::socket::getsockopt(peer, nix::sys::socket::sockopt::PeerCredentials)
        .map(|credentials| credentials.uid())
}

#[cfg(not(any(
    target_os = "macos",
    target_os = "ios",
    target_os = "watchos",
    target_os = "tvos",
    target_os = "visionos",
    target_os = "linux",
    target_os = "android"
)))]
fn peer_uid(_peer: &UnixStream) -> nix::Result<u32> {
    Err(nix::errno::Errno::ENOTSUP)
}

// -----------------------------------------------------------------------------
// Bounded multi-client server
// -----------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub struct IpcServerConfig {
    pub io_timeout: Duration,
    pub worker_count: usize,
    pub pending_connection_capacity: usize,
}

impl Default for IpcServerConfig {
    fn default() -> Self {
        Self {
            io_timeout: IPC_REQUEST_TIMEOUT,
            worker_count: DEFAULT_SERVER_WORKERS,
            pending_connection_capacity: DEFAULT_PENDING_CONNECTIONS,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct SocketIdentity {
    device: u64,
    inode: u64,
}

#[derive(Clone, Default)]
struct AcceptLoopMetrics {
    #[cfg(test)]
    poll_returns: Arc<AtomicUsize>,
}

impl AcceptLoopMetrics {
    fn record_poll_return(&self) {
        #[cfg(test)]
        self.poll_returns.fetch_add(1, Ordering::Relaxed);
    }

    #[cfg(test)]
    fn poll_returns(&self) -> usize {
        self.poll_returns.load(Ordering::Relaxed)
    }
}

pub struct IpcServer {
    socket_path: PathBuf,
    socket_identity: SocketIdentity,
    shutdown: Arc<AtomicBool>,
    waker: Arc<Waker>,
    streams: Option<Arc<IpcStreamBroker>>,
    thread: Option<JoinHandle<io::Result<()>>>,
    #[cfg(test)]
    accept_metrics: AcceptLoopMetrics,
}

impl IpcServer {
    pub fn start<A, P>(
        socket_path: impl AsRef<Path>,
        application: Arc<A>,
        authorizer: Arc<P>,
        config: IpcServerConfig,
    ) -> io::Result<Self>
    where
        A: ApplicationClient + Send + Sync + 'static,
        P: PeerAuthorizer + 'static,
    {
        Self::start_inner(socket_path, application, authorizer, None, config)
    }

    pub fn start_with_streams<A, P>(
        socket_path: impl AsRef<Path>,
        application: Arc<A>,
        authorizer: Arc<P>,
        streams: Arc<IpcStreamBroker>,
        config: IpcServerConfig,
    ) -> io::Result<Self>
    where
        A: ApplicationClient + Send + Sync + 'static,
        P: PeerAuthorizer + 'static,
    {
        Self::start_inner(socket_path, application, authorizer, Some(streams), config)
    }

    fn start_inner<A, P>(
        socket_path: impl AsRef<Path>,
        application: Arc<A>,
        authorizer: Arc<P>,
        streams: Option<Arc<IpcStreamBroker>>,
        config: IpcServerConfig,
    ) -> io::Result<Self>
    where
        A: ApplicationClient + Send + Sync + 'static,
        P: PeerAuthorizer + 'static,
    {
        validate_server_config(&config)?;
        let socket_path = socket_path.as_ref().to_path_buf();
        let listener = bind_private_listener(&socket_path)?;
        let metadata = match fs::symlink_metadata(&socket_path) {
            Ok(metadata) => metadata,
            Err(error) => {
                drop(listener);
                let _ = fs::remove_file(&socket_path);
                return Err(error);
            }
        };
        let socket_identity = SocketIdentity {
            device: metadata.dev(),
            inode: metadata.ino(),
        };
        let (listener, poll, waker) = match prepare_accept_loop(listener) {
            Ok(parts) => parts,
            Err(error) => {
                let _ = cleanup_socket(&socket_path, socket_identity);
                return Err(error);
            }
        };
        let shutdown = Arc::new(AtomicBool::new(false));
        let thread_shutdown = Arc::clone(&shutdown);
        let application: Arc<dyn ApplicationClient + Send + Sync> = application;
        let authorizer: Arc<dyn PeerAuthorizer> = authorizer;
        let thread_streams = streams.clone();
        let accept_metrics = AcceptLoopMetrics::default();
        let thread_accept_metrics = accept_metrics.clone();
        let thread = match thread::Builder::new()
            .name("hopash-ipc-accept".to_owned())
            .spawn(move || {
                run_server(
                    listener,
                    poll,
                    ServerRunContext {
                        application,
                        authorizer,
                        streams: thread_streams,
                        config,
                        shutdown: thread_shutdown,
                        accept_metrics: thread_accept_metrics,
                    },
                )
            }) {
            Ok(thread) => thread,
            Err(error) => {
                let _ = cleanup_socket(&socket_path, socket_identity);
                return Err(error);
            }
        };
        Ok(Self {
            socket_path,
            socket_identity,
            shutdown,
            waker,
            streams,
            thread: Some(thread),
            #[cfg(test)]
            accept_metrics,
        })
    }

    pub fn shutdown(&mut self) -> io::Result<()> {
        self.shutdown.store(true, Ordering::Release);
        if let Some(streams) = &self.streams {
            streams.notify_all();
        }
        let wake_result = match self.thread.as_ref() {
            Some(thread) if !thread.is_finished() => self.waker.wake(),
            Some(_) | None => Ok(()),
        };
        let thread_result = self.thread.take().map_or(Ok(()), |thread| {
            thread
                .join()
                .map_err(|_| io::Error::other("IPC server thread panicked"))?
        });
        let cleanup_result = cleanup_socket(&self.socket_path, self.socket_identity);
        wake_result.and(thread_result).and(cleanup_result)
    }
}

impl fmt::Debug for IpcServer {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("IpcServer")
            .field("socket_path", &"[REDACTED]")
            .field("running", &self.thread.is_some())
            .finish()
    }
}

impl Drop for IpcServer {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}

fn prepare_accept_loop(listener: UnixListener) -> io::Result<(MioUnixListener, Poll, Arc<Waker>)> {
    listener.set_nonblocking(true)?;
    let mut listener = MioUnixListener::from_std(listener);
    let poll = Poll::new()?;
    poll.registry()
        .register(&mut listener, LISTENER_TOKEN, Interest::READABLE)?;
    let waker = Arc::new(Waker::new(poll.registry(), SHUTDOWN_TOKEN)?);
    Ok((listener, poll, waker))
}

fn validate_server_config(config: &IpcServerConfig) -> io::Result<()> {
    if config.io_timeout.is_zero()
        || config.worker_count == 0
        || config.pending_connection_capacity == 0
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "IPC server limits must be positive",
        ));
    }
    Ok(())
}

struct ServerRunContext {
    application: Arc<dyn ApplicationClient + Send + Sync>,
    authorizer: Arc<dyn PeerAuthorizer>,
    streams: Option<Arc<IpcStreamBroker>>,
    config: IpcServerConfig,
    shutdown: Arc<AtomicBool>,
    accept_metrics: AcceptLoopMetrics,
}

fn run_server(
    listener: MioUnixListener,
    mut poll: Poll,
    context: ServerRunContext,
) -> io::Result<()> {
    let ServerRunContext {
        application,
        authorizer,
        streams,
        config,
        shutdown,
        accept_metrics,
    } = context;
    let (sender, receiver) = mpsc::sync_channel(config.pending_connection_capacity);
    let receiver = Arc::new(Mutex::new(receiver));
    let active_streams = Arc::new(AtomicUsize::new(0));
    let stream_limit = config.worker_count.saturating_sub(1);
    let workers_context = WorkerContext {
        application,
        streams,
        active_streams,
        stream_limit,
        io_timeout: config.io_timeout,
        shutdown: Arc::clone(&shutdown),
    };
    let workers = spawn_workers(config.worker_count, Arc::clone(&receiver), workers_context)?;

    let accept_result = accept_loop(
        &listener,
        &mut poll,
        &authorizer,
        &sender,
        &shutdown,
        &accept_metrics,
    );
    drop(sender);
    let mut worker_panicked = false;
    for worker in workers {
        worker_panicked |= worker.join().is_err();
    }
    if worker_panicked && accept_result.is_ok() {
        return Err(io::Error::other("IPC worker thread panicked"));
    }
    accept_result
}

#[derive(Clone)]
struct WorkerContext {
    application: Arc<dyn ApplicationClient + Send + Sync>,
    streams: Option<Arc<IpcStreamBroker>>,
    active_streams: Arc<AtomicUsize>,
    stream_limit: usize,
    io_timeout: Duration,
    shutdown: Arc<AtomicBool>,
}

fn spawn_workers(
    count: usize,
    receiver: Arc<Mutex<Receiver<UnixStream>>>,
    context: WorkerContext,
) -> io::Result<Vec<JoinHandle<()>>> {
    (0..count)
        .map(|index| {
            let receiver = Arc::clone(&receiver);
            let context = context.clone();
            thread::Builder::new()
                .name(format!("hopash-ipc-worker-{index}"))
                .spawn(move || worker_loop(receiver, context))
        })
        .collect()
}

fn worker_loop(receiver: Arc<Mutex<Receiver<UnixStream>>>, context: WorkerContext) {
    loop {
        let received = receiver
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .recv();
        match received {
            Ok(stream) if !context.shutdown.load(Ordering::Acquire) => {
                handle_connection(
                    stream,
                    context.application.as_ref(),
                    context.streams.as_deref(),
                    &context.active_streams,
                    context.stream_limit,
                    context.io_timeout,
                    &context.shutdown,
                );
            }
            Ok(_) | Err(_) => break,
        }
    }
}

fn accept_loop(
    listener: &MioUnixListener,
    poll: &mut Poll,
    authorizer: &Arc<dyn PeerAuthorizer>,
    sender: &SyncSender<UnixStream>,
    shutdown: &AtomicBool,
    metrics: &AcceptLoopMetrics,
) -> io::Result<()> {
    let mut events = Events::with_capacity(4);
    loop {
        if shutdown.load(Ordering::Acquire) {
            return Ok(());
        }
        events.clear();
        match poll.poll(&mut events, None) {
            Ok(()) => metrics.record_poll_return(),
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error),
        }
        if shutdown.load(Ordering::Acquire)
            || events.iter().any(|event| event.token() == SHUTDOWN_TOKEN)
        {
            return Ok(());
        }
        if events.iter().any(|event| event.token() == LISTENER_TOKEN) {
            accept_ready_connections(listener, authorizer, sender, shutdown)?;
        }
    }
}

fn accept_ready_connections(
    listener: &MioUnixListener,
    authorizer: &Arc<dyn PeerAuthorizer>,
    sender: &SyncSender<UnixStream>,
    shutdown: &AtomicBool,
) -> io::Result<()> {
    loop {
        match listener.accept() {
            Ok((stream, _)) => {
                let stream = UnixStream::from(stream);
                if shutdown.load(Ordering::Acquire) {
                    let _ = stream.shutdown(Shutdown::Both);
                    return Ok(());
                }
                if authorizer.authorize(&stream).is_err() {
                    let _ = stream.shutdown(Shutdown::Both);
                    continue;
                }
                match sender.try_send(stream) {
                    Ok(()) => {}
                    Err(TrySendError::Full(stream)) | Err(TrySendError::Disconnected(stream)) => {
                        let _ = stream.shutdown(Shutdown::Both);
                    }
                }
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => return Ok(()),
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
}

fn handle_connection(
    stream: UnixStream,
    application: &(dyn ApplicationClient + Send + Sync),
    streams: Option<&IpcStreamBroker>,
    active_streams: &AtomicUsize,
    stream_limit: usize,
    io_timeout: Duration,
    shutdown: &AtomicBool,
) {
    let mut stream = match DeadlineUnixStream::new(stream, io_timeout) {
        Ok(stream) => stream,
        Err(_) => return,
    };
    if stream.begin_read().is_err() {
        return;
    }
    let request = match read_frame::<_, IpcRequest>(&mut stream) {
        Ok(request) => request,
        Err(_) => {
            let response = IpcResponse::failure(
                RequestId(0),
                IpcError::new(
                    ErrorCode::ProtocolMismatch,
                    "The IPC request frame is invalid",
                    false,
                ),
            );
            write_response(&mut stream, &response);
            return;
        }
    };
    if let Err(response) = request.validate_protocol() {
        write_response(&mut stream, &response);
        return;
    }
    let request_id = request.request_id;
    let operation = match request.operation {
        RequestOperation::SubscribeStatus(_) => {
            let Some(streams) = streams else {
                write_response(
                    &mut stream,
                    &IpcResponse::failure(
                        request_id,
                        conversion_error(OperationConversionError::StreamingOperation),
                    ),
                );
                return;
            };
            let Some(_slot) = StreamSlot::acquire(active_streams, stream_limit) else {
                write_response(&mut stream, &stream_capacity_response(request_id));
                return;
            };
            serve_status_stream(&mut stream, request_id, streams, io_timeout, shutdown);
            return;
        }
        RequestOperation::FollowLogs(LogSubscriptionPayload { after_sequence }) => {
            let Some(streams) = streams else {
                write_response(
                    &mut stream,
                    &IpcResponse::failure(
                        request_id,
                        conversion_error(OperationConversionError::StreamingOperation),
                    ),
                );
                return;
            };
            let Some(_slot) = StreamSlot::acquire(active_streams, stream_limit) else {
                write_response(&mut stream, &stream_capacity_response(request_id));
                return;
            };
            serve_log_stream(
                &mut stream,
                request_id,
                streams,
                after_sequence,
                io_timeout,
                shutdown,
            );
            return;
        }
        RequestOperation::LogTail(LogTailPayload { after_sequence }) => {
            let Some(streams) = streams else {
                write_response(
                    &mut stream,
                    &IpcResponse::failure(
                        request_id,
                        conversion_error(OperationConversionError::StreamingOperation),
                    ),
                );
                return;
            };
            let data = serde_json::to_value(streams.log_tail(after_sequence));
            let response = match data {
                Ok(data) => IpcResponse::success(request_id, data),
                Err(_) => IpcResponse::failure(
                    request_id,
                    IpcError::new(
                        ErrorCode::Internal,
                        "The log tail response could not be encoded",
                        false,
                    ),
                ),
            };
            write_response(&mut stream, &response);
            return;
        }
        operation => match operation.into_application_operation() {
            Ok(operation) => operation,
            Err(error) => {
                let response = IpcResponse::failure(request_id, conversion_error(error));
                write_response(&mut stream, &response);
                return;
            }
        },
    };
    let response = match application.execute(operation) {
        Ok(output) => match WireApplicationOutput::try_from(output)
            .and_then(|output| serde_json::to_value(output).map_err(|_| WireConversionError))
        {
            Ok(data) => IpcResponse::success(request_id, data),
            Err(_) => IpcResponse::failure(
                request_id,
                IpcError::new(
                    ErrorCode::Internal,
                    "The application response could not be encoded",
                    false,
                ),
            ),
        },
        Err(error) => IpcResponse::failure(request_id, wire_error(error)),
    };
    write_response(&mut stream, &response);
}

struct StreamSlot<'a> {
    active: &'a AtomicUsize,
}

impl<'a> StreamSlot<'a> {
    fn acquire(active: &'a AtomicUsize, limit: usize) -> Option<Self> {
        let mut current = active.load(Ordering::Acquire);
        loop {
            if current >= limit {
                return None;
            }
            match active.compare_exchange_weak(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Some(Self { active }),
                Err(actual) => current = actual,
            }
        }
    }
}

impl Drop for StreamSlot<'_> {
    fn drop(&mut self) {
        self.active.fetch_sub(1, Ordering::AcqRel);
    }
}

fn stream_capacity_response(request_id: RequestId) -> IpcResponse {
    IpcResponse::failure(
        request_id,
        IpcError::new(
            ErrorCode::OperationUnavailable,
            "The IPC stream subscriber capacity is reached",
            true,
        ),
    )
}

fn serve_status_stream(
    stream: &mut DeadlineUnixStream,
    request_id: RequestId,
    streams: &IpcStreamBroker,
    io_timeout: Duration,
    shutdown: &AtomicBool,
) {
    let (initial, subscription) = streams.subscribe_status();
    if !write_stream_frame(
        stream,
        &IpcStreamFrame::new(request_id, IpcStreamPayload::Status(initial)),
    ) {
        return;
    }
    let heartbeat_interval = heartbeat_interval(io_timeout);
    while !shutdown.load(Ordering::Acquire) {
        let Some(item) = subscription.wait_next(shutdown, heartbeat_interval) else {
            if shutdown.load(Ordering::Acquire)
                || !write_stream_frame(
                    stream,
                    &IpcStreamFrame::new(request_id, IpcStreamPayload::Heartbeat),
                )
            {
                return;
            }
            continue;
        };
        let terminal = matches!(item, StatusStreamItem::ResyncRequired { .. });
        if !write_stream_frame(
            stream,
            &IpcStreamFrame::new(request_id, IpcStreamPayload::Status(item)),
        ) || terminal
        {
            return;
        }
    }
}

fn serve_log_stream(
    stream: &mut DeadlineUnixStream,
    request_id: RequestId,
    streams: &IpcStreamBroker,
    after_sequence: Option<u64>,
    io_timeout: Duration,
    shutdown: &AtomicBool,
) {
    let subscription = streams.subscribe_logs(after_sequence);
    let heartbeat_interval = heartbeat_interval(io_timeout);
    while !shutdown.load(Ordering::Acquire) {
        let Some(item) = subscription.wait_next(shutdown, heartbeat_interval) else {
            if shutdown.load(Ordering::Acquire)
                || !write_stream_frame(
                    stream,
                    &IpcStreamFrame::new(request_id, IpcStreamPayload::Heartbeat),
                )
            {
                return;
            }
            continue;
        };
        let terminal = matches!(item, LogStreamItem::Gap { .. });
        if !write_stream_frame(
            stream,
            &IpcStreamFrame::new(request_id, IpcStreamPayload::Logs(item)),
        ) || terminal
        {
            return;
        }
    }
}

fn heartbeat_interval(io_timeout: Duration) -> Duration {
    io_timeout
        .checked_div(2)
        .unwrap_or(Duration::from_millis(1))
        .max(Duration::from_millis(1))
}

fn write_stream_frame(stream: &mut DeadlineUnixStream, frame: &IpcStreamFrame) -> bool {
    stream.begin_write().is_ok() && write_frame(stream, frame).is_ok()
}

fn write_response(stream: &mut DeadlineUnixStream, response: &IpcResponse) {
    if stream.begin_write().is_ok() {
        let _ = write_frame(stream, response);
    }
}

fn wire_error(error: ApplicationError) -> IpcError {
    error.into()
}

fn conversion_error(error: OperationConversionError) -> IpcError {
    match error {
        OperationConversionError::InvalidSubscriptionUrl => IpcError::new(
            ErrorCode::InvalidSubscriptionUrl,
            "The Subscription URL is invalid",
            false,
        ),
        OperationConversionError::StreamingOperation => IpcError::new(
            ErrorCode::OperationUnavailable,
            "This IPC endpoint handles one-shot operations only",
            false,
        ),
    }
}

fn cleanup_socket(path: &Path, expected: SocketIdentity) -> io::Result<()> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    };
    if metadata.dev() == expected.device && metadata.ino() == expected.inode {
        fs::remove_file(path)?;
    }
    Ok(())
}

// -----------------------------------------------------------------------------
// Transport output projection
// -----------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ExpectedOutput {
    Status,
    Lifecycle,
    Profiles,
    ProfileMutation,
    Proxies,
    ProxySelection,
    Latencies,
    Latency,
    Rules,
    RuleMutation,
}

impl ExpectedOutput {
    fn for_operation(operation: &ApplicationOperation) -> Self {
        match operation {
            ApplicationOperation::Start
            | ApplicationOperation::Stop
            | ApplicationOperation::Restart => Self::Lifecycle,
            ApplicationOperation::GetStatus => Self::Status,
            ApplicationOperation::ProfileAdd { .. }
            | ApplicationOperation::ProfileUse { .. }
            | ApplicationOperation::ProfileRemove { .. } => Self::ProfileMutation,
            ApplicationOperation::ProfileList => Self::Profiles,
            ApplicationOperation::ProxyList { .. } => Self::Proxies,
            ApplicationOperation::ProxySelect { .. } => Self::ProxySelection,
            ApplicationOperation::LatencyList => Self::Latencies,
            ApplicationOperation::LatencyShow { .. } => Self::Latency,
            ApplicationOperation::RuleList => Self::Rules,
            ApplicationOperation::RuleAdd { .. }
            | ApplicationOperation::RuleReplace { .. }
            | ApplicationOperation::RuleRemove { .. } => Self::RuleMutation,
        }
    }

    fn matches(self, output: &ApplicationOutput) -> bool {
        matches!(
            (self, output),
            (Self::Status, ApplicationOutput::Status(_))
                | (Self::Lifecycle, ApplicationOutput::Lifecycle(_))
                | (Self::Profiles, ApplicationOutput::Profiles(_))
                | (Self::ProfileMutation, ApplicationOutput::ProfileMutation(_))
                | (Self::Proxies, ApplicationOutput::Proxies(_))
                | (Self::ProxySelection, ApplicationOutput::ProxySelection(_))
                | (Self::Latencies, ApplicationOutput::Latencies(_))
                | (Self::Latency, ApplicationOutput::Latency(_))
                | (Self::Rules, ApplicationOutput::Rules(_))
                | (Self::RuleMutation, ApplicationOutput::RuleMutation(_))
        )
    }
}

#[derive(Debug)]
struct WireConversionError;

macro_rules! wire_enum {
    ($wire:ident, $domain:ident, [$($variant:ident),+ $(,)?]) => {
        #[derive(Debug, Deserialize, Serialize)]
        #[serde(rename_all = "snake_case")]
        enum $wire {
            $($variant),+
        }

        impl From<$domain> for $wire {
            fn from(value: $domain) -> Self {
                match value {
                    $($domain::$variant => Self::$variant),+
                }
            }
        }

        impl From<$wire> for $domain {
            fn from(value: $wire) -> Self {
                match value {
                    $($wire::$variant => Self::$variant),+
                }
            }
        }
    };
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "output", content = "data", rename_all = "snake_case")]
enum WireApplicationOutput {
    Status(WireStatusSnapshot),
    Lifecycle(WireLifecycleOutcome),
    Profiles(WireProfileListOutcome),
    ProfileMutation(WireProfileMutationOutcome),
    Proxies(WireProxyListOutcome),
    ProxySelection(WireProxySelectionOutcome),
    Latencies(WireLatencyListOutcome),
    Latency(WireLatencyShowOutcome),
    Rules(WireRuleListOutcome),
    RuleMutation(WireRuleMutationOutcome),
    LogMetadata(WireLogMetadata),
}

impl TryFrom<ApplicationOutput> for WireApplicationOutput {
    type Error = WireConversionError;

    fn try_from(output: ApplicationOutput) -> Result<Self, Self::Error> {
        match output {
            ApplicationOutput::Status(status) => Ok(Self::Status(status.into())),
            ApplicationOutput::Lifecycle(outcome) => Ok(Self::Lifecycle(outcome.into())),
            ApplicationOutput::Profiles(outcome) => Ok(Self::Profiles(outcome.into())),
            ApplicationOutput::ProfileMutation(outcome) => {
                Ok(Self::ProfileMutation(outcome.into()))
            }
            ApplicationOutput::Proxies(outcome) => Ok(Self::Proxies(outcome.into())),
            ApplicationOutput::ProxySelection(outcome) => Ok(Self::ProxySelection(outcome.into())),
            ApplicationOutput::Latencies(outcome) => Ok(Self::Latencies(outcome.into())),
            ApplicationOutput::Latency(outcome) => Ok(Self::Latency(outcome.into())),
            ApplicationOutput::Rules(outcome) => Ok(Self::Rules(outcome.into())),
            ApplicationOutput::RuleMutation(outcome) => Ok(Self::RuleMutation(outcome.into())),
            ApplicationOutput::LogMetadata(metadata) => Ok(Self::LogMetadata(metadata.into())),
        }
    }
}

impl TryFrom<WireApplicationOutput> for ApplicationOutput {
    type Error = WireConversionError;

    fn try_from(output: WireApplicationOutput) -> Result<Self, Self::Error> {
        match output {
            WireApplicationOutput::Status(status) => {
                Ok(Self::Status(StatusSnapshot::try_from(status)?))
            }
            WireApplicationOutput::Lifecycle(outcome) => {
                Ok(Self::Lifecycle(LifecycleOutcome::try_from(outcome)?))
            }
            WireApplicationOutput::Profiles(outcome) => {
                Ok(Self::Profiles(ProfileListOutcome::try_from(outcome)?))
            }
            WireApplicationOutput::ProfileMutation(outcome) => Ok(Self::ProfileMutation(
                ProfileMutationOutcome::try_from(outcome)?,
            )),
            WireApplicationOutput::Proxies(outcome) => {
                Ok(Self::Proxies(ProxyListOutcome::try_from(outcome)?))
            }
            WireApplicationOutput::ProxySelection(outcome) => {
                Ok(Self::ProxySelection(outcome.try_into()?))
            }
            WireApplicationOutput::Latencies(outcome) => {
                Ok(Self::Latencies(LatencyListOutcome::try_from(outcome)?))
            }
            WireApplicationOutput::Latency(outcome) => {
                Ok(Self::Latency(LatencyShowOutcome::try_from(outcome)?))
            }
            WireApplicationOutput::Rules(outcome) => Ok(Self::Rules(outcome.into())),
            WireApplicationOutput::RuleMutation(outcome) => Ok(Self::RuleMutation(outcome.into())),
            WireApplicationOutput::LogMetadata(metadata) => Ok(Self::LogMetadata(metadata.into())),
        }
    }
}

wire_enum!(WireLifecycleAction, LifecycleAction, [Start, Stop, Restart]);

#[derive(Debug, Deserialize, Serialize)]
struct WireLifecycleOutcome {
    action: WireLifecycleAction,
    changed: bool,
    status: WireStatusSnapshot,
}

impl From<LifecycleOutcome> for WireLifecycleOutcome {
    fn from(value: LifecycleOutcome) -> Self {
        Self {
            action: value.action.into(),
            changed: value.changed,
            status: value.status.into(),
        }
    }
}

impl TryFrom<WireLifecycleOutcome> for LifecycleOutcome {
    type Error = WireConversionError;

    fn try_from(value: WireLifecycleOutcome) -> Result<Self, Self::Error> {
        Ok(Self {
            action: value.action.into(),
            changed: value.changed,
            status: value.status.try_into()?,
        })
    }
}

wire_enum!(WireProfileRefreshState, ProfileRefreshState, [Fresh, Error]);
wire_enum!(
    WireProfileRefreshStage,
    ProfileRefreshStage,
    [Download, Parse, Validate, Apply]
);

#[derive(Debug, Deserialize, Serialize)]
struct WireProfileRefreshFailure {
    stage: WireProfileRefreshStage,
    message: String,
}

impl From<ProfileRefreshFailure> for WireProfileRefreshFailure {
    fn from(value: ProfileRefreshFailure) -> Self {
        Self {
            stage: value.stage.into(),
            message: value.message,
        }
    }
}

impl From<WireProfileRefreshFailure> for ProfileRefreshFailure {
    fn from(value: WireProfileRefreshFailure) -> Self {
        Self {
            stage: value.stage.into(),
            message: value.message,
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct WireProfileSummary {
    id: String,
    name: String,
    subscription_url: String,
    active: bool,
    refresh_state: WireProfileRefreshState,
    last_success_at_unix_ms: u64,
    next_refresh_at_unix_ms: u64,
    last_error: Option<WireProfileRefreshFailure>,
}

impl From<ProfileSummary> for WireProfileSummary {
    fn from(value: ProfileSummary) -> Self {
        Self {
            id: value.id.to_string(),
            name: value.name,
            subscription_url: value.subscription_url.redacted(),
            active: value.active,
            refresh_state: value.refresh_state.into(),
            last_success_at_unix_ms: value.last_success_at_unix_ms,
            next_refresh_at_unix_ms: value.next_refresh_at_unix_ms,
            last_error: value.last_error.map(Into::into),
        }
    }
}

impl TryFrom<WireProfileSummary> for ProfileSummary {
    type Error = WireConversionError;

    fn try_from(value: WireProfileSummary) -> Result<Self, Self::Error> {
        Ok(Self {
            id: ProfileId::parse(&value.id).map_err(|_| WireConversionError)?,
            name: value.name,
            subscription_url: SubscriptionUrl::parse(&value.subscription_url)
                .map_err(|_| WireConversionError)?,
            active: value.active,
            refresh_state: value.refresh_state.into(),
            last_success_at_unix_ms: value.last_success_at_unix_ms,
            next_refresh_at_unix_ms: value.next_refresh_at_unix_ms,
            last_error: value.last_error.map(Into::into),
        })
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct WireProfileListOutcome {
    profiles: Vec<WireProfileSummary>,
}

impl From<ProfileListOutcome> for WireProfileListOutcome {
    fn from(value: ProfileListOutcome) -> Self {
        Self {
            profiles: value.profiles.into_iter().map(Into::into).collect(),
        }
    }
}

impl TryFrom<WireProfileListOutcome> for ProfileListOutcome {
    type Error = WireConversionError;

    fn try_from(value: WireProfileListOutcome) -> Result<Self, Self::Error> {
        Ok(Self {
            profiles: value
                .profiles
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_, _>>()?,
        })
    }
}

wire_enum!(
    WireProfileMutationAction,
    ProfileMutationAction,
    [Added, Activated, Removed]
);

#[derive(Debug, Deserialize, Serialize)]
struct WireProfileMutationOutcome {
    action: WireProfileMutationAction,
    profile: WireProfileSummary,
    runtime_apply: Option<WireRuntimeApplyOutcome>,
}

impl From<ProfileMutationOutcome> for WireProfileMutationOutcome {
    fn from(value: ProfileMutationOutcome) -> Self {
        Self {
            action: value.action.into(),
            profile: value.profile.into(),
            runtime_apply: value.runtime_apply.map(Into::into),
        }
    }
}

impl TryFrom<WireProfileMutationOutcome> for ProfileMutationOutcome {
    type Error = WireConversionError;

    fn try_from(value: WireProfileMutationOutcome) -> Result<Self, Self::Error> {
        Ok(Self {
            action: value.action.into(),
            profile: value.profile.try_into()?,
            runtime_apply: value.runtime_apply.map(Into::into),
        })
    }
}

wire_enum!(
    WireProxyAvailability,
    ProxyAvailability,
    [Available, Unavailable]
);
wire_enum!(
    WireProxyMemberKind,
    ProxyMemberKind,
    [Node, Group, Missing, Ambiguous, ProviderUnavailable]
);

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum WireProxyNodeSource {
    Core,
    Provider { provider_name: String },
}

impl From<ProxyNodeSource> for WireProxyNodeSource {
    fn from(value: ProxyNodeSource) -> Self {
        match value {
            ProxyNodeSource::Core => Self::Core,
            ProxyNodeSource::Provider { provider_name } => Self::Provider { provider_name },
        }
    }
}

impl From<WireProxyNodeSource> for ProxyNodeSource {
    fn from(value: WireProxyNodeSource) -> Self {
        match value {
            WireProxyNodeSource::Core => Self::Core,
            WireProxyNodeSource::Provider { provider_name } => Self::Provider { provider_name },
        }
    }
}

wire_enum!(
    WireLatencyFreshness,
    LatencyFreshness,
    [NotSampled, Fresh, Stale, Unavailable]
);
wire_enum!(
    WireLatencyProbeStatus,
    LatencyProbeStatus,
    [NotSampled, Queued, InFlight, Succeeded, Failed]
);

#[derive(Debug, Deserialize, Serialize)]
struct WireProxyNodeRow {
    id: Option<String>,
    name: String,
    member_kind: WireProxyMemberKind,
    source: Option<WireProxyNodeSource>,
    candidate_ids: Vec<String>,
    proxy_type: Option<String>,
    availability: WireProxyAvailability,
    selected: bool,
    delay_ms: Option<u64>,
    sampled_at_unix_ms: Option<u64>,
    freshness: WireLatencyFreshness,
    probe_status: WireLatencyProbeStatus,
}

impl From<ProxyNodeRow> for WireProxyNodeRow {
    fn from(value: ProxyNodeRow) -> Self {
        Self {
            id: value.id.map(|id| id.as_str().to_owned()),
            name: value.name,
            member_kind: value.member_kind.into(),
            source: value.source.map(Into::into),
            candidate_ids: value
                .candidate_ids
                .into_iter()
                .map(|id| id.as_str().to_owned())
                .collect(),
            proxy_type: value.proxy_type,
            availability: value.availability.into(),
            selected: value.selected,
            delay_ms: value.delay_ms,
            sampled_at_unix_ms: value.sampled_at_unix_ms,
            freshness: value.freshness.into(),
            probe_status: value.probe_status.into(),
        }
    }
}

impl TryFrom<WireProxyNodeRow> for ProxyNodeRow {
    type Error = WireConversionError;

    fn try_from(value: WireProxyNodeRow) -> Result<Self, Self::Error> {
        Ok(Self {
            id: value
                .id
                .map(|id| NodeRecordId::parse(&id).map_err(|_| WireConversionError))
                .transpose()?,
            name: value.name,
            member_kind: value.member_kind.into(),
            source: value.source.map(Into::into),
            candidate_ids: value
                .candidate_ids
                .into_iter()
                .map(|id| NodeRecordId::parse(&id).map_err(|_| WireConversionError))
                .collect::<Result<_, _>>()?,
            proxy_type: value.proxy_type,
            availability: value.availability.into(),
            selected: value.selected,
            delay_ms: value.delay_ms,
            sampled_at_unix_ms: value.sampled_at_unix_ms,
            freshness: value.freshness.into(),
            probe_status: value.probe_status.into(),
        })
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct WireSelectorIdentity {
    id: String,
    name: String,
}

impl From<SelectorIdentity> for WireSelectorIdentity {
    fn from(value: SelectorIdentity) -> Self {
        Self {
            id: value.id,
            name: value.name,
        }
    }
}

impl From<WireSelectorIdentity> for SelectorIdentity {
    fn from(value: WireSelectorIdentity) -> Self {
        Self {
            id: value.id,
            name: value.name,
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct WireProxyGroupSummary {
    id: String,
    name: String,
    proxy_type: String,
    selectable: bool,
    selected_node: Option<WireSelectorIdentity>,
}

impl From<ProxyGroupSummary> for WireProxyGroupSummary {
    fn from(value: ProxyGroupSummary) -> Self {
        Self {
            id: value.id.as_str().to_owned(),
            name: value.name,
            proxy_type: value.proxy_type,
            selectable: value.selectable,
            selected_node: value.selected_node.map(Into::into),
        }
    }
}

impl TryFrom<WireProxyGroupSummary> for ProxyGroupSummary {
    type Error = WireConversionError;

    fn try_from(value: WireProxyGroupSummary) -> Result<Self, Self::Error> {
        Ok(Self {
            id: ProxyGroupId::parse(&value.id).map_err(|_| WireConversionError)?,
            name: value.name,
            proxy_type: value.proxy_type,
            selectable: value.selectable,
            selected_node: value.selected_node.map(Into::into),
        })
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct WireProxyListOutcome {
    group: WireProxyGroupSummary,
    #[serde(default)]
    groups: Vec<WireProxyGroupSummary>,
    nodes: Vec<WireProxyNodeRow>,
}

impl From<ProxyListOutcome> for WireProxyListOutcome {
    fn from(value: ProxyListOutcome) -> Self {
        Self {
            group: value.group.into(),
            groups: value.groups.into_iter().map(Into::into).collect(),
            nodes: value.nodes.into_iter().map(Into::into).collect(),
        }
    }
}

impl TryFrom<WireProxyListOutcome> for ProxyListOutcome {
    type Error = WireConversionError;

    fn try_from(value: WireProxyListOutcome) -> Result<Self, Self::Error> {
        Ok(Self {
            group: value.group.try_into()?,
            groups: value
                .groups
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_, _>>()?,
            nodes: value
                .nodes
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_, _>>()?,
        })
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct WireProxySelectionOutcome {
    group_id: String,
    group: String,
    previous_node: Option<WireSelectorIdentity>,
    selected_node: WireSelectorIdentity,
    persisted: bool,
    recovery: WireRecoveryOutcome,
}

impl From<ProxySelectionOutcome> for WireProxySelectionOutcome {
    fn from(value: ProxySelectionOutcome) -> Self {
        Self {
            group_id: value.group_id.as_str().to_owned(),
            group: value.group,
            previous_node: value.previous_node.map(Into::into),
            selected_node: value.selected_node.into(),
            persisted: value.persisted,
            recovery: value.recovery.into(),
        }
    }
}

impl TryFrom<WireProxySelectionOutcome> for ProxySelectionOutcome {
    type Error = WireConversionError;

    fn try_from(value: WireProxySelectionOutcome) -> Result<Self, Self::Error> {
        Ok(Self {
            group_id: ProxyGroupId::parse(&value.group_id).map_err(|_| WireConversionError)?,
            group: value.group,
            previous_node: value.previous_node.map(Into::into),
            selected_node: value.selected_node.into(),
            persisted: value.persisted,
            recovery: value.recovery.into(),
        })
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct WireLatencySummary {
    node_id: String,
    node_name: String,
    delay_ms: Option<u64>,
    sampled_at_unix_ms: Option<u64>,
    freshness: WireLatencyFreshness,
    probe_status: WireLatencyProbeStatus,
    probe_generation: u64,
}

impl From<LatencySummary> for WireLatencySummary {
    fn from(value: LatencySummary) -> Self {
        Self {
            node_id: value.node_id.as_str().to_owned(),
            node_name: value.node_name,
            delay_ms: value.delay_ms,
            sampled_at_unix_ms: value.sampled_at_unix_ms,
            freshness: value.freshness.into(),
            probe_status: value.probe_status.into(),
            probe_generation: value.probe_generation.0,
        }
    }
}

impl TryFrom<WireLatencySummary> for LatencySummary {
    type Error = WireConversionError;

    fn try_from(value: WireLatencySummary) -> Result<Self, Self::Error> {
        Ok(Self {
            node_id: NodeRecordId::parse(&value.node_id).map_err(|_| WireConversionError)?,
            node_name: value.node_name,
            delay_ms: value.delay_ms,
            sampled_at_unix_ms: value.sampled_at_unix_ms,
            freshness: value.freshness.into(),
            probe_status: value.probe_status.into(),
            probe_generation: ProbeGeneration(value.probe_generation),
        })
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct WireLatencyListOutcome {
    samples: Vec<WireLatencySummary>,
}

impl From<LatencyListOutcome> for WireLatencyListOutcome {
    fn from(value: LatencyListOutcome) -> Self {
        Self {
            samples: value.samples.into_iter().map(Into::into).collect(),
        }
    }
}

impl TryFrom<WireLatencyListOutcome> for LatencyListOutcome {
    type Error = WireConversionError;

    fn try_from(value: WireLatencyListOutcome) -> Result<Self, Self::Error> {
        Ok(Self {
            samples: value
                .samples
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_, _>>()?,
        })
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct WireLatencyShowOutcome {
    sample: WireLatencySummary,
}

impl From<LatencyShowOutcome> for WireLatencyShowOutcome {
    fn from(value: LatencyShowOutcome) -> Self {
        Self {
            sample: value.sample.into(),
        }
    }
}

impl TryFrom<WireLatencyShowOutcome> for LatencyShowOutcome {
    type Error = WireConversionError;

    fn try_from(value: WireLatencyShowOutcome) -> Result<Self, Self::Error> {
        Ok(Self {
            sample: value.sample.try_into()?,
        })
    }
}

wire_enum!(
    WirePolicyTargetValidation,
    PolicyTargetValidation,
    [Valid, Missing, Unavailable]
);

#[derive(Debug, Deserialize, Serialize)]
struct WireRuleSummary {
    index: usize,
    rule_string: String,
    rule_type: String,
    payload: Option<String>,
    policy_target: String,
    params: Vec<String>,
    policy_target_validation: WirePolicyTargetValidation,
}

impl From<RuleSummary> for WireRuleSummary {
    fn from(value: RuleSummary) -> Self {
        Self {
            index: value.index,
            rule_string: value.rule_string,
            rule_type: value.rule_type,
            payload: value.payload,
            policy_target: value.policy_target,
            params: value.params,
            policy_target_validation: value.policy_target_validation.into(),
        }
    }
}

impl From<WireRuleSummary> for RuleSummary {
    fn from(value: WireRuleSummary) -> Self {
        Self {
            index: value.index,
            rule_string: value.rule_string,
            rule_type: value.rule_type,
            payload: value.payload,
            policy_target: value.policy_target,
            params: value.params,
            policy_target_validation: value.policy_target_validation.into(),
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct WireRuleListOutcome {
    initialized: bool,
    revision: Option<u64>,
    rules: Vec<WireRuleSummary>,
}

impl From<RuleListOutcome> for WireRuleListOutcome {
    fn from(value: RuleListOutcome) -> Self {
        Self {
            initialized: value.initialized,
            revision: value.revision.map(|revision| revision.0),
            rules: value.rules.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<WireRuleListOutcome> for RuleListOutcome {
    fn from(value: WireRuleListOutcome) -> Self {
        Self {
            initialized: value.initialized,
            revision: value.revision.map(LocalRuleSetRevision),
            rules: value.rules.into_iter().map(Into::into).collect(),
        }
    }
}

wire_enum!(
    WireRuleMutationAction,
    RuleMutationAction,
    [Added, Replaced, Removed]
);

#[derive(Debug, Deserialize, Serialize)]
struct WireRuleMutationOutcome {
    action: WireRuleMutationAction,
    changed_rule: String,
    previous_rule: Option<String>,
    resulting_position: Option<usize>,
    runtime_apply: WireRuntimeApplyOutcome,
}

impl From<RuleMutationOutcome> for WireRuleMutationOutcome {
    fn from(value: RuleMutationOutcome) -> Self {
        Self {
            action: value.action.into(),
            changed_rule: value.changed_rule,
            previous_rule: value.previous_rule,
            resulting_position: value.resulting_position,
            runtime_apply: value.runtime_apply.into(),
        }
    }
}

impl From<WireRuleMutationOutcome> for RuleMutationOutcome {
    fn from(value: WireRuleMutationOutcome) -> Self {
        Self {
            action: value.action.into(),
            changed_rule: value.changed_rule,
            previous_rule: value.previous_rule,
            resulting_position: value.resulting_position,
            runtime_apply: value.runtime_apply.into(),
        }
    }
}

wire_enum!(
    WireRuntimeApplyStatus,
    RuntimeApplyStatus,
    [NotRequired, Applied, Recovered, Failed]
);
wire_enum!(
    WireRecoveryStatus,
    RecoveryStatus,
    [NotRequired, Succeeded, Pending, Failed]
);

#[derive(Debug, Deserialize, Serialize)]
struct WireRecoveryOutcome {
    status: WireRecoveryStatus,
    restored_generation: Option<u64>,
    message: Option<String>,
}

impl From<RecoveryOutcome> for WireRecoveryOutcome {
    fn from(value: RecoveryOutcome) -> Self {
        Self {
            status: value.status.into(),
            restored_generation: value.restored_generation.map(|generation| generation.0),
            message: value.message,
        }
    }
}

impl From<WireRecoveryOutcome> for RecoveryOutcome {
    fn from(value: WireRecoveryOutcome) -> Self {
        Self {
            status: value.status.into(),
            restored_generation: value.restored_generation.map(RuntimeGeneration),
            message: value.message,
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct WireRuntimeApplyOutcome {
    status: WireRuntimeApplyStatus,
    candidate_generation: Option<u64>,
    committed_generation: Option<u64>,
    recovery: WireRecoveryOutcome,
}

impl From<RuntimeApplyOutcome> for WireRuntimeApplyOutcome {
    fn from(value: RuntimeApplyOutcome) -> Self {
        Self {
            status: value.status.into(),
            candidate_generation: value.candidate_generation.map(|generation| generation.0),
            committed_generation: value.committed_generation.map(|generation| generation.0),
            recovery: value.recovery.into(),
        }
    }
}

impl From<WireRuntimeApplyOutcome> for RuntimeApplyOutcome {
    fn from(value: WireRuntimeApplyOutcome) -> Self {
        Self {
            status: value.status.into(),
            candidate_generation: value.candidate_generation.map(RuntimeGeneration),
            committed_generation: value.committed_generation.map(RuntimeGeneration),
            recovery: value.recovery.into(),
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct WireLogGap {
    requested_after_sequence: u64,
    first_available_sequence: u64,
    dropped_count: u64,
}

impl From<LogGap> for WireLogGap {
    fn from(value: LogGap) -> Self {
        Self {
            requested_after_sequence: value.requested_after_sequence,
            first_available_sequence: value.first_available_sequence,
            dropped_count: value.dropped_count,
        }
    }
}

impl From<WireLogGap> for LogGap {
    fn from(value: WireLogGap) -> Self {
        Self {
            requested_after_sequence: value.requested_after_sequence,
            first_available_sequence: value.first_available_sequence,
            dropped_count: value.dropped_count,
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct WireLogMetadata {
    first_sequence: Option<u64>,
    last_sequence: Option<u64>,
    next_sequence: Option<u64>,
    dropped_total: u64,
    gap: Option<WireLogGap>,
}

impl From<LogMetadata> for WireLogMetadata {
    fn from(value: LogMetadata) -> Self {
        Self {
            first_sequence: value.first_sequence,
            last_sequence: value.last_sequence,
            next_sequence: value.next_sequence,
            dropped_total: value.dropped_total,
            gap: value.gap.map(Into::into),
        }
    }
}

impl From<WireLogMetadata> for LogMetadata {
    fn from(value: WireLogMetadata) -> Self {
        Self {
            first_sequence: value.first_sequence,
            last_sequence: value.last_sequence,
            next_sequence: value.next_sequence,
            dropped_total: value.dropped_total,
            gap: value.gap.map(Into::into),
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct WireStatusSnapshot {
    supervisor: WireSupervisorStatus,
    core: WireCoreStatus,
    tun: WireTunStatus,
    active_profile: Option<WireActiveProfileSummary>,
    primary_proxy_group: Option<String>,
    selected_node: Option<WireSelectedNodeSummary>,
    latency: Option<WireLatencySample>,
    traffic: WireTrafficSample,
    connection_count: u64,
    runtime_generation: Option<u64>,
    apply_state: WireApplyState,
    #[serde(default)]
    runtime_apply: Option<WireRuntimeApplySnapshot>,
    #[serde(default)]
    selection_restore_pending: bool,
    #[serde(default)]
    probe_queue: WireProbeQueueStatus,
    stream_health: WireStreamHealthSet,
}

impl From<StatusSnapshot> for WireStatusSnapshot {
    fn from(status: StatusSnapshot) -> Self {
        Self {
            supervisor: status.supervisor.into(),
            core: status.core.into(),
            tun: status.tun.into(),
            active_profile: status.active_profile.map(Into::into),
            primary_proxy_group: status.primary_proxy_group,
            selected_node: status.selected_node.map(Into::into),
            latency: status.latency.map(Into::into),
            traffic: status.traffic.into(),
            connection_count: status.connection_count,
            runtime_generation: status.runtime_generation.map(|generation| generation.0),
            apply_state: status.apply_state.into(),
            runtime_apply: Some(status.runtime_apply.into()),
            selection_restore_pending: status.selection_restore_pending,
            probe_queue: status.probe_queue.into(),
            stream_health: status.stream_health.into(),
        }
    }
}

impl TryFrom<WireStatusSnapshot> for StatusSnapshot {
    type Error = WireConversionError;

    fn try_from(status: WireStatusSnapshot) -> Result<Self, Self::Error> {
        let runtime_generation = status.runtime_generation.map(RuntimeGeneration);
        let apply_state: ApplyState = status.apply_state.into();
        let runtime_apply = status.runtime_apply.map_or_else(
            || RuntimeApplySnapshot {
                candidate_generation: None,
                committed_generation: runtime_generation,
                phase: match apply_state {
                    ApplyState::Idle => RuntimeApplyPhase::Idle,
                    ApplyState::Applying => RuntimeApplyPhase::Applying,
                    ApplyState::Recovering => RuntimeApplyPhase::Recovering,
                    ApplyState::Failed => RuntimeApplyPhase::Failed,
                },
                recovery: RuntimeRecoverySnapshot::default(),
            },
            Into::into,
        );
        Ok(Self {
            supervisor: status.supervisor.into(),
            core: status.core.into(),
            tun: status.tun.into(),
            active_profile: status.active_profile.map(TryInto::try_into).transpose()?,
            primary_proxy_group: status.primary_proxy_group,
            selected_node: status.selected_node.map(TryInto::try_into).transpose()?,
            latency: status.latency.map(TryInto::try_into).transpose()?,
            traffic: status.traffic.into(),
            connection_count: status.connection_count,
            runtime_generation,
            apply_state,
            runtime_apply,
            selection_restore_pending: status.selection_restore_pending,
            probe_queue: status.probe_queue.try_into()?,
            stream_health: status.stream_health.into(),
        })
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct WireRuntimeApplySnapshot {
    candidate_generation: Option<u64>,
    committed_generation: Option<u64>,
    phase: WireRuntimeApplyPhase,
    recovery: WireRuntimeRecoverySnapshot,
}

impl From<RuntimeApplySnapshot> for WireRuntimeApplySnapshot {
    fn from(snapshot: RuntimeApplySnapshot) -> Self {
        Self {
            candidate_generation: snapshot.candidate_generation.map(|generation| generation.0),
            committed_generation: snapshot.committed_generation.map(|generation| generation.0),
            phase: snapshot.phase.into(),
            recovery: snapshot.recovery.into(),
        }
    }
}

impl From<WireRuntimeApplySnapshot> for RuntimeApplySnapshot {
    fn from(snapshot: WireRuntimeApplySnapshot) -> Self {
        Self {
            candidate_generation: snapshot.candidate_generation.map(RuntimeGeneration),
            committed_generation: snapshot.committed_generation.map(RuntimeGeneration),
            phase: snapshot.phase.into(),
            recovery: snapshot.recovery.into(),
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct WireRuntimeRecoverySnapshot {
    status: WireRuntimeRecoveryStatus,
    restored_generation: Option<u64>,
    message: Option<String>,
}

impl From<RuntimeRecoverySnapshot> for WireRuntimeRecoverySnapshot {
    fn from(snapshot: RuntimeRecoverySnapshot) -> Self {
        Self {
            status: snapshot.status.into(),
            restored_generation: snapshot.restored_generation.map(|generation| generation.0),
            message: snapshot.message,
        }
    }
}

impl From<WireRuntimeRecoverySnapshot> for RuntimeRecoverySnapshot {
    fn from(snapshot: WireRuntimeRecoverySnapshot) -> Self {
        Self {
            status: snapshot.status.into(),
            restored_generation: snapshot.restored_generation.map(RuntimeGeneration),
            message: snapshot.message,
        }
    }
}

#[derive(Debug, Default, Deserialize, Serialize)]
struct WireProbeQueueStatus {
    active_node_count: u64,
    queue_depth: u64,
    in_flight_count: u64,
    overloaded: bool,
    oldest_due_age_ms: Option<u64>,
    estimated_full_pass_duration_ms: u64,
    stale_node_count: u64,
}

impl From<ProbeQueueStatus> for WireProbeQueueStatus {
    fn from(status: ProbeQueueStatus) -> Self {
        Self {
            active_node_count: status.active_node_count,
            queue_depth: status.queue_depth,
            in_flight_count: status.in_flight_count,
            overloaded: status.overloaded,
            oldest_due_age_ms: status.oldest_due_age_ms,
            estimated_full_pass_duration_ms: status.estimated_full_pass_duration_ms,
            stale_node_count: status.stale_node_count,
        }
    }
}

impl TryFrom<WireProbeQueueStatus> for ProbeQueueStatus {
    type Error = WireConversionError;

    fn try_from(status: WireProbeQueueStatus) -> Result<Self, Self::Error> {
        let scheduled = status
            .queue_depth
            .checked_add(status.in_flight_count)
            .ok_or(WireConversionError)?;
        if status.stale_node_count > status.active_node_count
            || scheduled > status.active_node_count
            || status.oldest_due_age_ms.is_some() != (status.queue_depth > 0)
        {
            return Err(WireConversionError);
        }
        Ok(Self {
            active_node_count: status.active_node_count,
            queue_depth: status.queue_depth,
            in_flight_count: status.in_flight_count,
            overloaded: status.overloaded,
            oldest_due_age_ms: status.oldest_due_age_ms,
            estimated_full_pass_duration_ms: status.estimated_full_pass_duration_ms,
            stale_node_count: status.stale_node_count,
        })
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct WireSupervisorStatus {
    lifecycle: WireSupervisorLifecycle,
    started_at_unix_ms: u64,
    uptime_seconds: u64,
}

impl From<SupervisorStatus> for WireSupervisorStatus {
    fn from(status: SupervisorStatus) -> Self {
        Self {
            lifecycle: status.lifecycle.into(),
            started_at_unix_ms: status.started_at_unix_ms,
            uptime_seconds: status.uptime_seconds,
        }
    }
}

impl From<WireSupervisorStatus> for SupervisorStatus {
    fn from(status: WireSupervisorStatus) -> Self {
        Self {
            lifecycle: status.lifecycle.into(),
            started_at_unix_ms: status.started_at_unix_ms,
            uptime_seconds: status.uptime_seconds,
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum WireSupervisorLifecycle {
    Starting,
    Ready,
    Stopping,
    Degraded,
}

impl From<SupervisorLifecycle> for WireSupervisorLifecycle {
    fn from(value: SupervisorLifecycle) -> Self {
        match value {
            SupervisorLifecycle::Starting => Self::Starting,
            SupervisorLifecycle::Ready => Self::Ready,
            SupervisorLifecycle::Stopping => Self::Stopping,
            SupervisorLifecycle::Degraded => Self::Degraded,
        }
    }
}

impl From<WireSupervisorLifecycle> for SupervisorLifecycle {
    fn from(value: WireSupervisorLifecycle) -> Self {
        match value {
            WireSupervisorLifecycle::Starting => Self::Starting,
            WireSupervisorLifecycle::Ready => Self::Ready,
            WireSupervisorLifecycle::Stopping => Self::Stopping,
            WireSupervisorLifecycle::Degraded => Self::Degraded,
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct WireCoreStatus {
    lifecycle: WireCoreLifecycle,
    pid: Option<u32>,
    instance_generation: Option<u64>,
    #[serde(default)]
    restart: WireCoreRestartStatus,
}

impl From<CoreStatus> for WireCoreStatus {
    fn from(status: CoreStatus) -> Self {
        Self {
            lifecycle: status.lifecycle.into(),
            pid: status.pid,
            instance_generation: status.instance_generation.map(|generation| generation.0),
            restart: status.restart.into(),
        }
    }
}

impl From<WireCoreStatus> for CoreStatus {
    fn from(status: WireCoreStatus) -> Self {
        Self {
            lifecycle: status.lifecycle.into(),
            pid: status.pid,
            instance_generation: status.instance_generation.map(CoreInstanceGeneration),
            restart: status.restart.into(),
        }
    }
}

#[derive(Debug, Default, Deserialize, Serialize)]
struct WireCoreRestartStatus {
    pending: bool,
    attempts: u64,
    backoff_ms: Option<u64>,
    diagnostic: Option<WireCoreDiagnosticCategory>,
}

impl From<CoreRestartStatus> for WireCoreRestartStatus {
    fn from(status: CoreRestartStatus) -> Self {
        Self {
            pending: status.pending,
            attempts: status.attempts,
            backoff_ms: status.backoff_ms,
            diagnostic: status.diagnostic.map(Into::into),
        }
    }
}

impl From<WireCoreRestartStatus> for CoreRestartStatus {
    fn from(status: WireCoreRestartStatus) -> Self {
        Self {
            pending: status.pending,
            attempts: status.attempts,
            backoff_ms: status.backoff_ms,
            diagnostic: status.diagnostic.map(Into::into),
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum WireCoreDiagnosticCategory {
    RestartLimitReached,
}

impl From<CoreDiagnosticCategory> for WireCoreDiagnosticCategory {
    fn from(category: CoreDiagnosticCategory) -> Self {
        match category {
            CoreDiagnosticCategory::RestartLimitReached => Self::RestartLimitReached,
        }
    }
}

impl From<WireCoreDiagnosticCategory> for CoreDiagnosticCategory {
    fn from(category: WireCoreDiagnosticCategory) -> Self {
        match category {
            WireCoreDiagnosticCategory::RestartLimitReached => Self::RestartLimitReached,
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum WireCoreLifecycle {
    Unconfigured,
    Stopped,
    Starting,
    Ready,
    Reloading,
    Stopping,
    Degraded,
}

impl From<CoreLifecycle> for WireCoreLifecycle {
    fn from(value: CoreLifecycle) -> Self {
        match value {
            CoreLifecycle::Unconfigured => Self::Unconfigured,
            CoreLifecycle::Stopped => Self::Stopped,
            CoreLifecycle::Starting => Self::Starting,
            CoreLifecycle::Ready => Self::Ready,
            CoreLifecycle::Reloading => Self::Reloading,
            CoreLifecycle::Stopping => Self::Stopping,
            CoreLifecycle::Degraded => Self::Degraded,
        }
    }
}

impl From<WireCoreLifecycle> for CoreLifecycle {
    fn from(value: WireCoreLifecycle) -> Self {
        match value {
            WireCoreLifecycle::Unconfigured => Self::Unconfigured,
            WireCoreLifecycle::Stopped => Self::Stopped,
            WireCoreLifecycle::Starting => Self::Starting,
            WireCoreLifecycle::Ready => Self::Ready,
            WireCoreLifecycle::Reloading => Self::Reloading,
            WireCoreLifecycle::Stopping => Self::Stopping,
            WireCoreLifecycle::Degraded => Self::Degraded,
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct WireTunStatus {
    requested: bool,
    capable: bool,
    effective: bool,
    reason: Option<WireTunReason>,
}

impl From<TunStatus> for WireTunStatus {
    fn from(status: TunStatus) -> Self {
        Self {
            requested: status.requested,
            capable: status.capable,
            effective: status.effective,
            reason: status.reason.map(Into::into),
        }
    }
}

impl From<WireTunStatus> for TunStatus {
    fn from(status: WireTunStatus) -> Self {
        Self {
            requested: status.requested,
            capable: status.capable,
            effective: status.effective,
            reason: status.reason.map(Into::into),
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum WireTunReason {
    NoActiveProfile,
    PermissionDenied,
    Unsupported,
    CoreUnavailable,
}

impl From<TunReason> for WireTunReason {
    fn from(value: TunReason) -> Self {
        match value {
            TunReason::NoActiveProfile => Self::NoActiveProfile,
            TunReason::PermissionDenied => Self::PermissionDenied,
            TunReason::Unsupported => Self::Unsupported,
            TunReason::CoreUnavailable => Self::CoreUnavailable,
        }
    }
}

impl From<WireTunReason> for TunReason {
    fn from(value: WireTunReason) -> Self {
        match value {
            WireTunReason::NoActiveProfile => Self::NoActiveProfile,
            WireTunReason::PermissionDenied => Self::PermissionDenied,
            WireTunReason::Unsupported => Self::Unsupported,
            WireTunReason::CoreUnavailable => Self::CoreUnavailable,
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct WireActiveProfileSummary {
    id: String,
    name: String,
}

impl From<ActiveProfileSummary> for WireActiveProfileSummary {
    fn from(value: ActiveProfileSummary) -> Self {
        Self {
            id: value.id.to_string(),
            name: value.name,
        }
    }
}

impl TryFrom<WireActiveProfileSummary> for ActiveProfileSummary {
    type Error = WireConversionError;

    fn try_from(value: WireActiveProfileSummary) -> Result<Self, Self::Error> {
        Ok(Self {
            id: ProfileId::parse(&value.id).map_err(|_| WireConversionError)?,
            name: value.name,
        })
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct WireSelectedNodeSummary {
    id: String,
    name: String,
}

impl From<SelectedNodeSummary> for WireSelectedNodeSummary {
    fn from(value: SelectedNodeSummary) -> Self {
        Self {
            id: value.id.as_str().to_owned(),
            name: value.name,
        }
    }
}

impl TryFrom<WireSelectedNodeSummary> for SelectedNodeSummary {
    type Error = WireConversionError;

    fn try_from(value: WireSelectedNodeSummary) -> Result<Self, Self::Error> {
        Ok(Self {
            id: NodeRecordId::parse(&value.id).map_err(|_| WireConversionError)?,
            name: value.name,
        })
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct WireLatencySample {
    node_id: String,
    delay_ms: Option<u64>,
    sampled_at_unix_ms: Option<u64>,
    state: WireSampleState,
    probe_generation: u64,
}

impl From<LatencySample> for WireLatencySample {
    fn from(value: LatencySample) -> Self {
        Self {
            node_id: value.node_id.as_str().to_owned(),
            delay_ms: value.delay_ms,
            sampled_at_unix_ms: value.sampled_at_unix_ms,
            state: value.state.into(),
            probe_generation: value.probe_generation.0,
        }
    }
}

impl TryFrom<WireLatencySample> for LatencySample {
    type Error = WireConversionError;

    fn try_from(value: WireLatencySample) -> Result<Self, Self::Error> {
        Ok(Self {
            node_id: NodeRecordId::parse(&value.node_id).map_err(|_| WireConversionError)?,
            delay_ms: value.delay_ms,
            sampled_at_unix_ms: value.sampled_at_unix_ms,
            state: value.state.into(),
            probe_generation: ProbeGeneration(value.probe_generation),
        })
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct WireTrafficSample {
    upload_bytes_per_second: u64,
    download_bytes_per_second: u64,
    sampled_at_unix_ms: Option<u64>,
    state: WireSampleState,
}

impl From<TrafficSample> for WireTrafficSample {
    fn from(value: TrafficSample) -> Self {
        Self {
            upload_bytes_per_second: value.upload_bytes_per_second,
            download_bytes_per_second: value.download_bytes_per_second,
            sampled_at_unix_ms: value.sampled_at_unix_ms,
            state: value.state.into(),
        }
    }
}

impl From<WireTrafficSample> for TrafficSample {
    fn from(value: WireTrafficSample) -> Self {
        Self {
            upload_bytes_per_second: value.upload_bytes_per_second,
            download_bytes_per_second: value.download_bytes_per_second,
            sampled_at_unix_ms: value.sampled_at_unix_ms,
            state: value.state.into(),
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum WireSampleState {
    Fresh,
    Stale,
    Unavailable,
}

impl From<SampleState> for WireSampleState {
    fn from(value: SampleState) -> Self {
        match value {
            SampleState::Fresh => Self::Fresh,
            SampleState::Stale => Self::Stale,
            SampleState::Unavailable => Self::Unavailable,
        }
    }
}

impl From<WireSampleState> for SampleState {
    fn from(value: WireSampleState) -> Self {
        match value {
            WireSampleState::Fresh => Self::Fresh,
            WireSampleState::Stale => Self::Stale,
            WireSampleState::Unavailable => Self::Unavailable,
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum WireApplyState {
    Idle,
    Applying,
    Recovering,
    Failed,
}

impl From<ApplyState> for WireApplyState {
    fn from(value: ApplyState) -> Self {
        match value {
            ApplyState::Idle => Self::Idle,
            ApplyState::Applying => Self::Applying,
            ApplyState::Recovering => Self::Recovering,
            ApplyState::Failed => Self::Failed,
        }
    }
}

impl From<WireApplyState> for ApplyState {
    fn from(value: WireApplyState) -> Self {
        match value {
            WireApplyState::Idle => Self::Idle,
            WireApplyState::Applying => Self::Applying,
            WireApplyState::Recovering => Self::Recovering,
            WireApplyState::Failed => Self::Failed,
        }
    }
}

wire_enum!(
    WireRuntimeApplyPhase,
    RuntimeApplyPhase,
    [Idle, Applying, Succeeded, Recovering, Failed]
);
wire_enum!(
    WireRuntimeRecoveryStatus,
    RuntimeRecoveryStatus,
    [NotRequired, Succeeded, Pending, Failed]
);

#[derive(Debug, Deserialize, Serialize)]
struct WireStreamHealthSet {
    traffic: WireStreamState,
    connections: WireStreamState,
    logs: WireStreamState,
}

impl From<StreamHealthSet> for WireStreamHealthSet {
    fn from(value: StreamHealthSet) -> Self {
        Self {
            traffic: value.traffic.into(),
            connections: value.connections.into(),
            logs: value.logs.into(),
        }
    }
}

impl From<WireStreamHealthSet> for StreamHealthSet {
    fn from(value: WireStreamHealthSet) -> Self {
        Self {
            traffic: value.traffic.into(),
            connections: value.connections.into(),
            logs: value.logs.into(),
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum WireStreamState {
    Disconnected,
    Connecting,
    Healthy,
    Stale,
    Degraded,
}

impl From<StreamState> for WireStreamState {
    fn from(value: StreamState) -> Self {
        match value {
            StreamState::Disconnected => Self::Disconnected,
            StreamState::Connecting => Self::Connecting,
            StreamState::Healthy => Self::Healthy,
            StreamState::Stale => Self::Stale,
            StreamState::Degraded => Self::Degraded,
        }
    }
}

impl From<WireStreamState> for StreamState {
    fn from(value: WireStreamState) -> Self {
        match value {
            WireStreamState::Disconnected => Self::Disconnected,
            WireStreamState::Connecting => Self::Connecting,
            WireStreamState::Healthy => Self::Healthy,
            WireStreamState::Stale => Self::Stale,
            WireStreamState::Degraded => Self::Degraded,
        }
    }
}

#[cfg(test)]
mod timeout_tests {
    use super::*;
    use crate::constants::{
        CORE_HEALTH_TIMEOUT, CORE_READINESS_TIMEOUT, MIHOMO_VALIDATION_TIMEOUT,
        PROFILE_TOTAL_TIMEOUT,
    };

    struct IdleAuthorizer {
        calls: AtomicUsize,
    }

    impl PeerAuthorizer for IdleAuthorizer {
        fn authorize(&self, _peer: &UnixStream) -> Result<(), PeerAuthorizationError> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
    }

    struct IdleApplication {
        calls: AtomicUsize,
    }

    impl ApplicationClient for IdleApplication {
        fn execute(
            &self,
            _operation: ApplicationOperation,
        ) -> Result<ApplicationOutput, ApplicationError> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            Ok(ApplicationOutput::Status(
                crate::application::ApplicationService::new().status(),
            ))
        }
    }

    #[test]
    fn product_client_covers_the_complete_bounded_mutation_path() {
        let client = IpcClient::new("/tmp/hopash-timeout-contract.sock");
        let profile_add = ApplicationOperation::ProfileAdd {
            subscription_url: SubscriptionUrl::parse("https://example.test/profile.yaml")
                .expect("the fixture URL should be valid"),
        };
        let minimum_profile_add = PROFILE_TOTAL_TIMEOUT
            .saturating_add(MIHOMO_VALIDATION_TIMEOUT)
            .saturating_add(CORE_READINESS_TIMEOUT)
            .saturating_add(CORE_HEALTH_TIMEOUT);
        assert!(client.response_timeout(&profile_add) > minimum_profile_add);

        let rule_add = ApplicationOperation::RuleAdd {
            rule: "MATCH,DIRECT".to_owned(),
            placement: ApplicationRulePlacement::Append,
        };
        let minimum_runtime_mutation = MIHOMO_VALIDATION_TIMEOUT
            .saturating_add(CORE_READINESS_TIMEOUT)
            .saturating_add(CORE_HEALTH_TIMEOUT);
        assert!(client.response_timeout(&rule_add) > minimum_runtime_mutation);
        assert_eq!(
            client.response_timeout(&ApplicationOperation::GetStatus),
            IPC_REQUEST_TIMEOUT
        );
    }

    #[test]
    fn explicit_test_timeouts_remain_exact_for_every_operation() {
        let timeout = Duration::from_millis(7);
        let client = IpcClient::with_timeouts(
            "/tmp/hopash-fixed-timeout.sock",
            Duration::from_millis(5),
            timeout,
        );
        let operation = ApplicationOperation::ProfileAdd {
            subscription_url: SubscriptionUrl::parse("https://example.test/profile.yaml")
                .expect("the fixture URL should be valid"),
        };
        assert_eq!(client.response_timeout(&operation), timeout);
        assert_eq!(client.stream_timeout(), timeout);
    }

    #[test]
    fn idle_server_blocks_without_periodic_wakes_and_shutdown_bypasses_handlers() {
        let root = PathBuf::from("/tmp").join(format!(
            "hopash-idle-ipc-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4().simple()
        ));
        let socket = root.join("supervisor.sock");
        let application = Arc::new(IdleApplication {
            calls: AtomicUsize::new(0),
        });
        let authorizer = Arc::new(IdleAuthorizer {
            calls: AtomicUsize::new(0),
        });
        let mut server = IpcServer::start(
            &socket,
            Arc::clone(&application),
            Arc::clone(&authorizer),
            IpcServerConfig {
                io_timeout: Duration::from_millis(100),
                worker_count: 1,
                pending_connection_capacity: 1,
            },
        )
        .expect("the idle fixture server should start");

        thread::sleep(Duration::from_millis(75));
        assert_eq!(server.accept_metrics.poll_returns(), 0);
        assert_eq!(authorizer.calls.load(Ordering::Relaxed), 0);
        assert_eq!(application.calls.load(Ordering::Relaxed), 0);
        let started = std::time::Instant::now();
        server
            .shutdown()
            .expect("the idle fixture server should stop");

        assert!(started.elapsed() < Duration::from_millis(250));
        assert_eq!(server.accept_metrics.poll_returns(), 1);
        assert_eq!(authorizer.calls.load(Ordering::Relaxed), 0);
        assert_eq!(application.calls.load(Ordering::Relaxed), 0);
        assert!(!socket.exists());
        let _ = fs::remove_dir(&root);
    }
}
