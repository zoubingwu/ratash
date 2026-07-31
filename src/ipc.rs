use std::collections::VecDeque;
use std::fmt;
use std::fs;
use std::io::{self, Read, Write};
use std::os::unix::fs::{FileTypeExt, PermissionsExt};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::Path;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::application::{
    ApplicationError, ApplicationOperation, RulePlacement as ApplicationRulePlacement,
};
use crate::constants::IPC_FRAME_MAX_BYTES;
use crate::domain::{InvalidSubscriptionUrl, SubscriptionUrl};
use crate::error::ErrorCode;
use crate::telemetry::{CoreLogRecord, LogLevel, LogSource, LogTail};

pub const IPC_PROTOCOL_VERSION: u16 = 1;

// -----------------------------------------------------------------------------
// Request and response contract
// -----------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(transparent)]
pub struct RequestId(pub u64);

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct IpcRequest {
    pub protocol_version: u16,
    pub request_id: RequestId,
    #[serde(flatten)]
    pub operation: RequestOperation,
}

impl IpcRequest {
    #[must_use]
    pub fn new(request_id: RequestId, operation: RequestOperation) -> Self {
        Self {
            protocol_version: IPC_PROTOCOL_VERSION,
            request_id,
            operation,
        }
    }

    pub fn validate_protocol(&self) -> Result<(), IpcResponse> {
        if self.protocol_version == IPC_PROTOCOL_VERSION {
            return Ok(());
        }
        Err(IpcResponse::failure(
            self.request_id,
            IpcError::new(
                ErrorCode::ProtocolMismatch,
                "The IPC protocol version is incompatible",
                false,
            )
            .with_details(serde_json::json!({
                "expected": IPC_PROTOCOL_VERSION,
                "actual": self.protocol_version,
            })),
        ))
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "operation", content = "payload", rename_all = "snake_case")]
pub enum RequestOperation {
    Start(EmptyPayload),
    Stop(EmptyPayload),
    Restart(EmptyPayload),
    GetStatus(EmptyPayload),
    SubscribeStatus(StatusSubscriptionPayload),
    ProfileAdd(ProfileAddPayload),
    ProfileList(EmptyPayload),
    ProfileUse(ProfileSelectorPayload),
    ProfileRemove(ProfileSelectorPayload),
    ProxyList(ProxyListPayload),
    ProxySelect(ProxySelectPayload),
    LatencyList(EmptyPayload),
    LatencyShow(NodeSelectorPayload),
    RuleList(EmptyPayload),
    RuleAdd(RuleAddPayload),
    RuleReplace(RuleReplacePayload),
    RuleRemove(RuleSelectorPayload),
    FollowLogs(LogSubscriptionPayload),
    LogTail(LogTailPayload),
}

impl RequestOperation {
    pub fn into_application_operation(
        self,
    ) -> Result<ApplicationOperation, OperationConversionError> {
        let operation = match self {
            Self::Start(_) => ApplicationOperation::Start,
            Self::Stop(_) => ApplicationOperation::Stop,
            Self::Restart(_) => ApplicationOperation::Restart,
            Self::GetStatus(_) => ApplicationOperation::GetStatus,
            Self::ProfileAdd(payload) => ApplicationOperation::ProfileAdd {
                subscription_url: payload.subscription_url()?,
            },
            Self::ProfileList(_) => ApplicationOperation::ProfileList,
            Self::ProfileUse(payload) => ApplicationOperation::ProfileUse {
                profile: payload.profile,
            },
            Self::ProfileRemove(payload) => ApplicationOperation::ProfileRemove {
                profile: payload.profile,
            },
            Self::ProxyList(payload) => ApplicationOperation::ProxyList {
                group: payload.group,
            },
            Self::ProxySelect(payload) => ApplicationOperation::ProxySelect {
                group: payload.group,
                node: payload.node,
            },
            Self::LatencyList(_) => ApplicationOperation::LatencyList,
            Self::LatencyShow(payload) => ApplicationOperation::LatencyShow { node: payload.node },
            Self::RuleList(_) => ApplicationOperation::RuleList,
            Self::RuleAdd(payload) => ApplicationOperation::RuleAdd {
                rule: payload.rule,
                placement: payload.placement.into(),
            },
            Self::RuleReplace(payload) => ApplicationOperation::RuleReplace {
                old_rule: payload.old_rule,
                new_rule: payload.new_rule,
            },
            Self::RuleRemove(payload) => ApplicationOperation::RuleRemove { rule: payload.rule },
            Self::SubscribeStatus(_) | Self::FollowLogs(_) | Self::LogTail(_) => {
                return Err(OperationConversionError::StreamingOperation);
            }
        };
        Ok(operation)
    }
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EmptyPayload {}

#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProfileAddPayload {
    subscription_url: String,
}

impl ProfileAddPayload {
    #[must_use]
    pub fn new(subscription_url: &SubscriptionUrl) -> Self {
        Self {
            subscription_url: subscription_url.expose().as_str().to_owned(),
        }
    }

    pub fn subscription_url(&self) -> Result<SubscriptionUrl, InvalidSubscriptionUrl> {
        SubscriptionUrl::parse(&self.subscription_url)
    }
}

impl fmt::Debug for ProfileAddPayload {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProfileAddPayload")
            .field("subscription_url", &"[REDACTED]")
            .finish()
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProfileSelectorPayload {
    pub profile: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProxyListPayload {
    pub group: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProxySelectPayload {
    pub group: String,
    pub node: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct NodeSelectorPayload {
    pub node: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RuleAddPayload {
    pub rule: String,
    pub placement: RulePlacement,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "position", content = "anchor", rename_all = "snake_case")]
pub enum RulePlacement {
    Prepend,
    Append,
    Before(String),
    After(String),
}

impl From<RulePlacement> for ApplicationRulePlacement {
    fn from(placement: RulePlacement) -> Self {
        match placement {
            RulePlacement::Prepend => Self::Prepend,
            RulePlacement::Append => Self::Append,
            RulePlacement::Before(anchor) => Self::Before(anchor),
            RulePlacement::After(anchor) => Self::After(anchor),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RuleReplacePayload {
    pub old_rule: String,
    pub new_rule: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RuleSelectorPayload {
    pub rule: String,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct StatusSubscriptionPayload {
    pub after_sequence: Option<u64>,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LogSubscriptionPayload {
    pub after_sequence: Option<u64>,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LogTailPayload {
    pub after_sequence: Option<u64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OperationConversionError {
    InvalidSubscriptionUrl,
    StreamingOperation,
}

impl From<InvalidSubscriptionUrl> for OperationConversionError {
    fn from(_: InvalidSubscriptionUrl) -> Self {
        Self::InvalidSubscriptionUrl
    }
}

#[derive(Clone, Deserialize, Eq, PartialEq, Serialize)]
pub struct IpcResponse {
    pub protocol_version: u16,
    pub request_id: RequestId,
    #[serde(flatten)]
    outcome: ResponseOutcome,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct IpcStreamFrame {
    pub protocol_version: u16,
    pub request_id: RequestId,
    #[serde(flatten)]
    pub payload: IpcStreamPayload,
}

impl IpcStreamFrame {
    #[must_use]
    pub fn new(request_id: RequestId, payload: IpcStreamPayload) -> Self {
        Self {
            protocol_version: IPC_PROTOCOL_VERSION,
            request_id,
            payload,
        }
    }

    pub fn ensure_correlated(
        &self,
        expected_request_id: RequestId,
    ) -> Result<(), CorrelationError> {
        if self.protocol_version != IPC_PROTOCOL_VERSION {
            return Err(CorrelationError::ProtocolMismatch {
                expected: IPC_PROTOCOL_VERSION,
                actual: self.protocol_version,
            });
        }
        if self.request_id != expected_request_id {
            return Err(CorrelationError::RequestIdMismatch {
                expected: expected_request_id,
                actual: self.request_id,
            });
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(
    tag = "stream",
    content = "item",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum IpcStreamPayload {
    Status(StatusStreamItem),
    Logs(LogStreamItem),
    Heartbeat,
}

impl IpcResponse {
    #[must_use]
    pub fn success(request_id: RequestId, data: serde_json::Value) -> Self {
        Self {
            protocol_version: IPC_PROTOCOL_VERSION,
            request_id,
            outcome: ResponseOutcome::Success(SuccessBody { data }),
        }
    }

    #[must_use]
    pub fn failure(request_id: RequestId, error: IpcError) -> Self {
        Self {
            protocol_version: IPC_PROTOCOL_VERSION,
            request_id,
            outcome: ResponseOutcome::Failure(ErrorBody { error }),
        }
    }

    #[must_use]
    pub fn data(&self) -> Option<&serde_json::Value> {
        match &self.outcome {
            ResponseOutcome::Success(body) => Some(&body.data),
            ResponseOutcome::Failure(_) => None,
        }
    }

    #[must_use]
    pub fn error(&self) -> Option<&IpcError> {
        match &self.outcome {
            ResponseOutcome::Success(_) => None,
            ResponseOutcome::Failure(body) => Some(&body.error),
        }
    }

    pub fn ensure_correlated(
        &self,
        expected_request_id: RequestId,
    ) -> Result<(), CorrelationError> {
        if self.protocol_version != IPC_PROTOCOL_VERSION {
            return Err(CorrelationError::ProtocolMismatch {
                expected: IPC_PROTOCOL_VERSION,
                actual: self.protocol_version,
            });
        }
        if self.request_id != expected_request_id {
            return Err(CorrelationError::RequestIdMismatch {
                expected: expected_request_id,
                actual: self.request_id,
            });
        }
        Ok(())
    }
}

impl fmt::Debug for IpcResponse {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("IpcResponse")
            .field("protocol_version", &self.protocol_version)
            .field("request_id", &self.request_id)
            .field(
                "success",
                &matches!(self.outcome, ResponseOutcome::Success(_)),
            )
            .finish()
    }
}

#[derive(Clone, Deserialize, Eq, PartialEq, Serialize)]
#[serde(untagged)]
enum ResponseOutcome {
    Success(SuccessBody),
    Failure(ErrorBody),
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct SuccessBody {
    data: serde_json::Value,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct ErrorBody {
    error: IpcError,
}

#[derive(Clone, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct IpcError {
    pub code: String,
    pub message: String,
    pub retryable: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<serde_json::Value>,
}

impl fmt::Debug for IpcError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("IpcError")
            .field("code", &self.code)
            .field("retryable", &self.retryable)
            .field("has_details", &self.details.is_some())
            .finish()
    }
}

impl IpcError {
    #[must_use]
    pub fn new(code: ErrorCode, message: impl Into<String>, retryable: bool) -> Self {
        Self {
            code: error_code_name(code).to_owned(),
            message: message.into(),
            retryable,
            details: None,
        }
    }

    #[must_use]
    pub fn with_details(mut self, details: serde_json::Value) -> Self {
        self.details = Some(details);
        self
    }
}

impl From<ApplicationError> for IpcError {
    fn from(error: ApplicationError) -> Self {
        let error = crate::contract::ApiError::from(error);
        Self {
            code: error_code_name(error.code).to_owned(),
            message: error.message,
            retryable: error.retryable,
            details: error.details,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CorrelationError {
    ProtocolMismatch {
        expected: u16,
        actual: u16,
    },
    RequestIdMismatch {
        expected: RequestId,
        actual: RequestId,
    },
}

fn error_code_name(code: ErrorCode) -> &'static str {
    match code {
        ErrorCode::Usage => "usage",
        ErrorCode::SupervisorUnavailable => "supervisor_unavailable",
        ErrorCode::ProtocolMismatch => "protocol_mismatch",
        ErrorCode::ProfileAmbiguous => "profile_ambiguous",
        ErrorCode::ProfileActive => "profile_active",
        ErrorCode::ProfileNotFound => "profile_not_found",
        ErrorCode::ProxyGroupNotFound => "proxy_group_not_found",
        ErrorCode::NodeNotFound => "node_not_found",
        ErrorCode::NodeAmbiguous => "node_ambiguous",
        ErrorCode::InvalidSubscriptionUrl => "invalid_subscription_url",
        ErrorCode::RulesUninitialized => "rules_uninitialized",
        ErrorCode::RuleBusy => "rule_busy",
        ErrorCode::RuleNotFound => "rule_not_found",
        ErrorCode::RuleAmbiguous => "rule_ambiguous",
        ErrorCode::RuleAlreadyExists => "rule_already_exists",
        ErrorCode::PolicyTargetNotFound => "policy_target_not_found",
        ErrorCode::ProfileFieldUnsupported => "profile_field_unsupported",
        ErrorCode::TunPermissionDenied => "tun_permission_denied",
        ErrorCode::CoreUnavailable => "core_unavailable",
        ErrorCode::ExternalOperationFailed => "external_operation_failed",
        ErrorCode::Internal => "internal",
        ErrorCode::OperationUnavailable => "operation_unavailable",
    }
}

// -----------------------------------------------------------------------------
// Length-delimited JSON framing
// -----------------------------------------------------------------------------

pub fn write_frame<W, T>(writer: &mut W, value: &T) -> Result<(), FrameError>
where
    W: Write,
    T: Serialize,
{
    let payload = serde_json::to_vec(value).map_err(FrameError::Json)?;
    if payload.len() > IPC_FRAME_MAX_BYTES {
        return Err(FrameError::FrameTooLarge {
            limit: IPC_FRAME_MAX_BYTES,
            actual: payload.len(),
        });
    }
    let length = u32::try_from(payload.len()).map_err(|_| FrameError::FrameTooLarge {
        limit: IPC_FRAME_MAX_BYTES,
        actual: payload.len(),
    })?;
    writer
        .write_all(&length.to_be_bytes())
        .map_err(FrameError::Io)?;
    writer.write_all(&payload).map_err(FrameError::Io)
}

pub fn read_frame<R, T>(reader: &mut R) -> Result<T, FrameError>
where
    R: Read,
    T: DeserializeOwned,
{
    let mut header = [0_u8; 4];
    reader.read_exact(&mut header).map_err(FrameError::Io)?;
    let length = u32::from_be_bytes(header) as usize;
    if length > IPC_FRAME_MAX_BYTES {
        return Err(FrameError::FrameTooLarge {
            limit: IPC_FRAME_MAX_BYTES,
            actual: length,
        });
    }
    let mut payload = vec![0; length];
    reader.read_exact(&mut payload).map_err(FrameError::Io)?;
    serde_json::from_slice(&payload).map_err(FrameError::Json)
}

#[derive(Debug)]
pub enum FrameError {
    Io(io::Error),
    Json(serde_json::Error),
    FrameTooLarge { limit: usize, actual: usize },
}

impl fmt::Display for FrameError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "IPC frame I/O failed: {error}"),
            Self::Json(error) => write!(formatter, "IPC frame JSON is invalid: {error}"),
            Self::FrameTooLarge { limit, actual } => {
                write!(
                    formatter,
                    "IPC frame is {actual} bytes; limit is {limit} bytes"
                )
            }
        }
    }
}

impl std::error::Error for FrameError {}

// -----------------------------------------------------------------------------
// Private Unix socket boundary
// -----------------------------------------------------------------------------

pub fn bind_private_listener(socket_path: &Path) -> io::Result<UnixListener> {
    let parent = socket_path
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "IPC socket requires a parent directory",
            )
        })?;
    fs::create_dir_all(parent)?;
    let parent_metadata = fs::symlink_metadata(parent)?;
    if parent_metadata.file_type().is_symlink() || !parent_metadata.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "IPC socket parent must be a real directory",
        ));
    }
    fs::set_permissions(parent, fs::Permissions::from_mode(0o700))?;

    let listener = UnixListener::bind(socket_path)?;
    fs::set_permissions(socket_path, fs::Permissions::from_mode(0o600))?;
    let socket_metadata = fs::symlink_metadata(socket_path)?;
    if !socket_metadata.file_type().is_socket() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "IPC endpoint is not a Unix socket",
        ));
    }
    Ok(listener)
}

pub trait PeerAuthorizer: Send + Sync {
    fn authorize(&self, peer: &UnixStream) -> Result<(), PeerAuthorizationError>;
}

#[derive(Clone, Eq, PartialEq)]
pub struct PeerAuthorizationError {
    pub safe_message: String,
}

impl fmt::Debug for PeerAuthorizationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PeerAuthorizationError")
            .field("safe_message_bytes", &self.safe_message.len())
            .finish()
    }
}

impl PeerAuthorizationError {
    #[must_use]
    pub fn new(safe_message: impl Into<String>) -> Self {
        Self {
            safe_message: safe_message.into(),
        }
    }
}

impl fmt::Display for PeerAuthorizationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.safe_message)
    }
}

impl std::error::Error for PeerAuthorizationError {}

pub fn accept_authorized(
    listener: &UnixListener,
    authorizer: &dyn PeerAuthorizer,
) -> Result<UnixStream, AcceptError> {
    let (peer, _) = listener.accept().map_err(AcceptError::Io)?;
    authorizer
        .authorize(&peer)
        .map_err(AcceptError::Unauthorized)?;
    Ok(peer)
}

#[derive(Debug)]
pub enum AcceptError {
    Io(io::Error),
    Unauthorized(PeerAuthorizationError),
}

impl fmt::Display for AcceptError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "IPC accept failed: {error}"),
            Self::Unauthorized(error) => write!(formatter, "IPC peer rejected: {error}"),
        }
    }
}

impl std::error::Error for AcceptError {}

// -----------------------------------------------------------------------------
// Bounded status subscriber
// -----------------------------------------------------------------------------

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum StatusStreamItem {
    Snapshot {
        sequence: u64,
        timestamp_unix_ms: u64,
        snapshot: serde_json::Value,
    },
    Event {
        sequence: u64,
        timestamp_unix_ms: u64,
        event: serde_json::Value,
    },
    ResyncRequired {
        expected_sequence: u64,
        observed_sequence: u64,
    },
}

#[derive(Debug)]
pub struct StatusSubscriber {
    capacity: usize,
    last_sequence: u64,
    requires_resync: bool,
    queue: VecDeque<StatusStreamItem>,
}

impl StatusSubscriber {
    pub fn new(
        capacity: usize,
        sequence: u64,
        timestamp_unix_ms: u64,
        snapshot: serde_json::Value,
    ) -> Result<Self, SubscriberError> {
        if capacity == 0 {
            return Err(SubscriberError::InvalidCapacity);
        }
        let mut queue = VecDeque::with_capacity(capacity);
        queue.push_back(StatusStreamItem::Snapshot {
            sequence,
            timestamp_unix_ms,
            snapshot,
        });
        Ok(Self {
            capacity,
            last_sequence: sequence,
            requires_resync: false,
            queue,
        })
    }

    pub fn publish(
        &mut self,
        sequence: u64,
        timestamp_unix_ms: u64,
        event: serde_json::Value,
    ) -> SubscriberPublishStatus {
        if self.requires_resync {
            self.update_resync_marker(sequence);
            return SubscriberPublishStatus::AwaitingResync;
        }
        let Some(expected_sequence) = self.last_sequence.checked_add(1) else {
            self.require_resync(u64::MAX, sequence);
            return SubscriberPublishStatus::ResyncRequired;
        };
        if sequence != expected_sequence || self.queue.len() == self.capacity {
            self.require_resync(expected_sequence, sequence);
            return SubscriberPublishStatus::ResyncRequired;
        }
        self.last_sequence = sequence;
        self.queue.push_back(StatusStreamItem::Event {
            sequence,
            timestamp_unix_ms,
            event,
        });
        SubscriberPublishStatus::Queued
    }

    pub fn resync(&mut self, sequence: u64, timestamp_unix_ms: u64, snapshot: serde_json::Value) {
        self.queue.clear();
        self.last_sequence = sequence;
        self.requires_resync = false;
        self.queue.push_back(StatusStreamItem::Snapshot {
            sequence,
            timestamp_unix_ms,
            snapshot,
        });
    }

    pub fn pop_front(&mut self) -> Option<StatusStreamItem> {
        self.queue.pop_front()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.queue.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    #[must_use]
    pub fn requires_resync(&self) -> bool {
        self.requires_resync
    }

    fn require_resync(&mut self, expected_sequence: u64, observed_sequence: u64) {
        self.queue.clear();
        self.requires_resync = true;
        self.last_sequence = observed_sequence;
        self.queue.push_back(StatusStreamItem::ResyncRequired {
            expected_sequence,
            observed_sequence,
        });
    }

    fn update_resync_marker(&mut self, observed_sequence: u64) {
        self.last_sequence = self.last_sequence.max(observed_sequence);
        if let Some(StatusStreamItem::ResyncRequired {
            observed_sequence: marker_sequence,
            ..
        }) = self.queue.front_mut()
        {
            *marker_sequence = (*marker_sequence).max(observed_sequence);
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SubscriberPublishStatus {
    Queued,
    ResyncRequired,
    AwaitingResync,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SubscriberError {
    InvalidCapacity,
}

// -----------------------------------------------------------------------------
// Bounded log subscriber and tail projection
// -----------------------------------------------------------------------------

#[derive(Clone, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LogRecordV1 {
    pub sequence: u64,
    pub timestamp_unix_ms: u64,
    pub level: String,
    pub source: String,
    pub message: String,
}

impl From<&CoreLogRecord> for LogRecordV1 {
    fn from(record: &CoreLogRecord) -> Self {
        Self {
            sequence: record.sequence(),
            timestamp_unix_ms: record.timestamp_unix_ms(),
            level: log_level_name(record.level()).to_owned(),
            source: log_source_name(record.source()).to_owned(),
            message: record.message().to_owned(),
        }
    }
}

impl fmt::Debug for LogRecordV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LogRecordV1")
            .field("sequence", &self.sequence)
            .field("timestamp_unix_ms", &self.timestamp_unix_ms)
            .field("level", &self.level)
            .field("source", &self.source)
            .field("message_bytes", &self.message.len())
            .finish()
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum LogStreamItem {
    Record {
        record: LogRecordV1,
    },
    Gap {
        after_sequence: Option<u64>,
        latest_sequence: u64,
    },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LogTailV1 {
    pub records: Vec<LogRecordV1>,
    pub dropped_total: u64,
    pub gap: bool,
    pub earliest_sequence: Option<u64>,
    pub latest_sequence: Option<u64>,
}

impl From<LogTail> for LogTailV1 {
    fn from(tail: LogTail) -> Self {
        Self {
            records: tail.records.iter().map(Into::into).collect(),
            dropped_total: tail.dropped_total,
            gap: tail.gap,
            earliest_sequence: tail.earliest_sequence,
            latest_sequence: tail.latest_sequence,
        }
    }
}

#[derive(Debug)]
pub struct LogSubscriber {
    capacity: usize,
    last_observed_sequence: Option<u64>,
    last_delivered_sequence: Option<u64>,
    awaiting_tail: bool,
    gap_count: u64,
    queue: VecDeque<LogStreamItem>,
}

impl LogSubscriber {
    pub fn new(capacity: usize, after_sequence: Option<u64>) -> Result<Self, SubscriberError> {
        if capacity == 0 {
            return Err(SubscriberError::InvalidCapacity);
        }
        Ok(Self {
            capacity,
            last_observed_sequence: after_sequence,
            last_delivered_sequence: after_sequence,
            awaiting_tail: false,
            gap_count: 0,
            queue: VecDeque::with_capacity(capacity),
        })
    }

    pub fn publish(&mut self, record: &CoreLogRecord) -> SubscriberPublishStatus {
        let sequence = record.sequence();
        if self.awaiting_tail {
            self.last_observed_sequence = Some(
                self.last_observed_sequence
                    .map_or(sequence, |current| current.max(sequence)),
            );
            self.update_gap_marker(sequence);
            return SubscriberPublishStatus::AwaitingResync;
        }
        let sequence_gap = self.last_observed_sequence.is_some_and(|current| {
            current
                .checked_add(1)
                .is_none_or(|expected| expected != sequence)
        });
        if sequence_gap || self.queue.len() == self.capacity {
            self.require_tail(sequence);
            return SubscriberPublishStatus::ResyncRequired;
        }
        self.last_observed_sequence = Some(sequence);
        self.queue.push_back(LogStreamItem::Record {
            record: record.into(),
        });
        SubscriberPublishStatus::Queued
    }

    pub fn pop_front(&mut self) -> Option<LogStreamItem> {
        let item = self.queue.pop_front()?;
        if let LogStreamItem::Record { record } = &item {
            self.last_delivered_sequence = Some(record.sequence);
        }
        Some(item)
    }

    pub fn mark_tail_sent(&mut self, latest_sequence: Option<u64>) {
        self.queue.clear();
        self.last_observed_sequence = latest_sequence;
        self.last_delivered_sequence = latest_sequence;
        self.awaiting_tail = false;
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.queue.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    #[must_use]
    pub fn awaiting_tail(&self) -> bool {
        self.awaiting_tail
    }

    #[must_use]
    pub fn gap_count(&self) -> u64 {
        self.gap_count
    }

    fn require_tail(&mut self, latest_sequence: u64) {
        self.queue.clear();
        self.last_observed_sequence = Some(latest_sequence);
        self.awaiting_tail = true;
        self.gap_count = self.gap_count.saturating_add(1);
        self.queue.push_back(LogStreamItem::Gap {
            after_sequence: self.last_delivered_sequence,
            latest_sequence,
        });
    }

    fn update_gap_marker(&mut self, latest_sequence: u64) {
        if let Some(LogStreamItem::Gap {
            latest_sequence: marker_sequence,
            ..
        }) = self.queue.front_mut()
        {
            *marker_sequence = (*marker_sequence).max(latest_sequence);
        }
    }
}

fn log_level_name(level: LogLevel) -> &'static str {
    match level {
        LogLevel::Debug => "debug",
        LogLevel::Info => "info",
        LogLevel::Warn => "warn",
        LogLevel::Error => "error",
    }
}

fn log_source_name(source: LogSource) -> &'static str {
    match source {
        LogSource::CoreApi => "core_api",
        LogSource::Stdout => "stdout",
        LogSource::Stderr => "stderr",
    }
}
