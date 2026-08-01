//! Synchronous privileged CoreRuntime IPC client.

use std::collections::BTreeMap;
use std::fmt;
use std::io;
use std::net::Shutdown;
use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use socket2::{Domain, SockAddr, Socket, Type};

use crate::constants::{CORE_SERVICE_MUTATION_TIMEOUT, CORE_SERVICE_REQUEST_TIMEOUT};
use crate::core::{
    ApplyCandidateResult, CoreRuntime, CoreRuntimeError, CoreRuntimeStatus, ForwardedCoreLogBatch,
    OwnerSession, OwnerSessionProof, OwnerSessionRequest, RuntimeBundle, StopCoreResult,
};
use crate::ipc::{read_frame, write_frame};
use crate::unix_io::{DeadlineUnixStream, deadline_after, remaining_until};

use super::CORE_SERVICE_IPC_PROTOCOL_VERSION;
use super::authorization::peer_identity;
use super::error::{
    cancelled_apply_error, map_read_error, map_write_error, protocol_error, transport_unavailable,
    unexpected_response,
};
use super::wire::{
    WireApplyRequest, WireEmpty, WireLogsRequest, WireOperation, WireOutcome, WireProofRequest,
    WireRequest, WireResponse, WireSuccess,
};

pub struct CoreServiceClient {
    socket_path: PathBuf,
    expected_service_uid: u32,
    connect_timeout: Duration,
    timeout_policy: CoreServiceTimeoutPolicy,
    next_request_id: AtomicU64,
    apply_cancellation_requested: AtomicBool,
    active_request_streams: Mutex<BTreeMap<u64, UnixStream>>,
}

#[derive(Clone, Copy)]
enum CoreServiceTimeoutPolicy {
    Product,
    Fixed(Duration),
    OperationSpecific {
        request: Duration,
        mutation: Duration,
    },
}

impl CoreServiceClient {
    #[must_use]
    pub fn new(socket_path: impl Into<PathBuf>) -> Self {
        Self::with_service_uid(socket_path, 0)
    }

    #[must_use]
    pub fn with_timeouts(
        socket_path: impl Into<PathBuf>,
        connect_timeout: Duration,
        io_timeout: Duration,
    ) -> Self {
        Self::with_service_uid_and_timeouts(socket_path, 0, connect_timeout, io_timeout)
    }

    #[must_use]
    pub fn for_service_uid(socket_path: impl Into<PathBuf>, expected_service_uid: u32) -> Self {
        Self::with_service_uid(socket_path, expected_service_uid)
    }

    fn with_service_uid(socket_path: impl Into<PathBuf>, expected_service_uid: u32) -> Self {
        Self {
            socket_path: socket_path.into(),
            expected_service_uid,
            connect_timeout: CORE_SERVICE_REQUEST_TIMEOUT,
            timeout_policy: CoreServiceTimeoutPolicy::Product,
            next_request_id: AtomicU64::new(1),
            apply_cancellation_requested: AtomicBool::new(false),
            active_request_streams: Mutex::new(BTreeMap::new()),
        }
    }

    #[must_use]
    pub fn with_service_uid_and_timeouts(
        socket_path: impl Into<PathBuf>,
        expected_service_uid: u32,
        connect_timeout: Duration,
        io_timeout: Duration,
    ) -> Self {
        Self {
            socket_path: socket_path.into(),
            expected_service_uid,
            connect_timeout,
            timeout_policy: CoreServiceTimeoutPolicy::Fixed(io_timeout),
            next_request_id: AtomicU64::new(1),
            apply_cancellation_requested: AtomicBool::new(false),
            active_request_streams: Mutex::new(BTreeMap::new()),
        }
    }

    #[must_use]
    pub fn with_service_uid_and_operation_timeouts(
        socket_path: impl Into<PathBuf>,
        expected_service_uid: u32,
        connect_timeout: Duration,
        request_timeout: Duration,
        mutation_timeout: Duration,
    ) -> Self {
        Self {
            socket_path: socket_path.into(),
            expected_service_uid,
            connect_timeout,
            timeout_policy: CoreServiceTimeoutPolicy::OperationSpecific {
                request: request_timeout,
                mutation: mutation_timeout,
            },
            next_request_id: AtomicU64::new(1),
            apply_cancellation_requested: AtomicBool::new(false),
            active_request_streams: Mutex::new(BTreeMap::new()),
        }
    }

    fn request_id(&self) -> u64 {
        let request_id = self.next_request_id.fetch_add(1, Ordering::Relaxed);
        if request_id == 0 {
            self.next_request_id.fetch_add(1, Ordering::Relaxed)
        } else {
            request_id
        }
    }

    fn connect(&self, timeout: Duration) -> io::Result<UnixStream> {
        let connect_timeout = self.connect_timeout.min(timeout);
        if connect_timeout.is_zero() || !self.timeout_policy.is_valid() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Core service IPC deadlines must be positive",
            ));
        }
        let socket = Socket::new(Domain::UNIX, Type::STREAM, None)?;
        let address = SockAddr::unix(&self.socket_path)?;
        socket.connect_timeout(&address, connect_timeout)?;
        let stream = UnixStream::from(socket);
        let actual_uid = peer_identity(&stream).map_err(io::Error::other)?.uid();
        if actual_uid != self.expected_service_uid {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "Core service peer identity mismatch",
            ));
        }
        Ok(stream)
    }

    fn request(&self, operation: WireOperation) -> Result<WireSuccess, CoreRuntimeError> {
        let response_timeout = self.timeout_policy.response_timeout(&operation);
        self.request_with_timeout(operation, response_timeout)
    }

    fn request_with_timeout(
        &self,
        operation: WireOperation,
        response_timeout: Duration,
    ) -> Result<WireSuccess, CoreRuntimeError> {
        let is_runtime_apply = matches!(&operation, WireOperation::ApplyCandidate(_));
        if is_runtime_apply && self.apply_cancellation_requested.load(Ordering::Acquire) {
            return Err(cancelled_apply_error());
        }
        if response_timeout.is_zero() {
            return Err(transport_unavailable(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Core service IPC deadline must be positive",
            )));
        }
        let deadline = deadline_after(response_timeout).map_err(transport_unavailable)?;
        let track_request = !matches!(
            &operation,
            WireOperation::Stop(_)
                | WireOperation::CancelPendingApply(_)
                | WireOperation::CloseOwnerSession(_)
        );
        let request_id = self.request_id();
        let request = WireRequest {
            protocol_version: CORE_SERVICE_IPC_PROTOCOL_VERSION,
            request_id,
            operation,
        };
        let stream = self
            .connect(remaining_until(deadline).map_err(transport_unavailable)?)
            .map_err(transport_unavailable)?;
        let _active_request = if track_request {
            Some(ActiveRequest::register(self, request_id, &stream)?)
        } else {
            None
        };
        let mut stream = DeadlineUnixStream::new(
            stream,
            remaining_until(deadline).map_err(transport_unavailable)?,
        )
        .map_err(transport_unavailable)?;
        stream
            .begin_write_until(deadline)
            .map_err(transport_unavailable)?;
        if let Err(error) = write_frame(&mut stream, &request) {
            if is_runtime_apply && self.apply_cancellation_requested.load(Ordering::Acquire) {
                return Err(cancelled_apply_error());
            }
            return Err(map_write_error(error));
        }
        stream
            .begin_read_until(deadline)
            .map_err(transport_unavailable)?;
        let response: WireResponse = match read_frame(&mut stream) {
            Ok(response) => response,
            Err(_)
                if is_runtime_apply
                    && self.apply_cancellation_requested.load(Ordering::Acquire) =>
            {
                return Err(cancelled_apply_error());
            }
            Err(error) => return Err(map_read_error(error)),
        };
        if response.protocol_version != CORE_SERVICE_IPC_PROTOCOL_VERSION
            || response.request_id != request_id
        {
            return Err(protocol_error(
                "Core service IPC response correlation failed",
            ));
        }
        match response.outcome {
            WireOutcome::Success(success) => Ok(success),
            WireOutcome::Failure(error) => Err(error.into_core()),
        }
    }
}

struct ActiveRequest<'a> {
    client: &'a CoreServiceClient,
    request_id: u64,
}

impl<'a> ActiveRequest<'a> {
    fn register(
        client: &'a CoreServiceClient,
        request_id: u64,
        stream: &UnixStream,
    ) -> Result<Self, CoreRuntimeError> {
        let cancellation_stream = stream.try_clone().map_err(transport_unavailable)?;
        let mut active = client.active_request_streams.lock().map_err(|_| {
            transport_unavailable(io::Error::other(
                "Core service request cancellation state is unavailable",
            ))
        })?;
        active.insert(request_id, cancellation_stream);
        if client.apply_cancellation_requested.load(Ordering::Acquire)
            && let Some(stream) = active.remove(&request_id)
        {
            let _ = stream.shutdown(Shutdown::Both);
        }
        Ok(Self { client, request_id })
    }
}

impl Drop for ActiveRequest<'_> {
    fn drop(&mut self) {
        if let Ok(mut active) = self.client.active_request_streams.lock() {
            active.remove(&self.request_id);
        }
    }
}

impl fmt::Debug for CoreServiceClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CoreServiceClient")
            .field("socket_path", &"[REDACTED]")
            .field("expected_service_uid", &self.expected_service_uid)
            .field("connect_timeout", &self.connect_timeout)
            .field("request_timeout", &self.timeout_policy.request_timeout())
            .field("mutation_timeout", &self.timeout_policy.mutation_timeout())
            .finish_non_exhaustive()
    }
}

impl CoreServiceTimeoutPolicy {
    fn request_timeout(self) -> Duration {
        match self {
            Self::Product => CORE_SERVICE_REQUEST_TIMEOUT,
            Self::Fixed(timeout) => timeout,
            Self::OperationSpecific { request, .. } => request,
        }
    }

    fn mutation_timeout(self) -> Duration {
        match self {
            Self::Product => CORE_SERVICE_MUTATION_TIMEOUT,
            Self::Fixed(timeout) => timeout,
            Self::OperationSpecific { mutation, .. } => mutation,
        }
    }

    fn response_timeout(self, operation: &WireOperation) -> Duration {
        match operation {
            WireOperation::Status(_) | WireOperation::Logs(_) => self.request_timeout(),
            WireOperation::CancelPendingApply(_) => self.request_timeout(),
            WireOperation::OpenOwnerSession(_)
            | WireOperation::ApplyCandidate(_)
            | WireOperation::Stop(_)
            | WireOperation::CloseOwnerSession(_) => self.mutation_timeout(),
        }
    }

    fn is_valid(self) -> bool {
        !self.request_timeout().is_zero() && !self.mutation_timeout().is_zero()
    }
}

impl CoreRuntime for CoreServiceClient {
    fn open_owner_session(
        &self,
        request: &OwnerSessionRequest,
    ) -> Result<OwnerSession, CoreRuntimeError> {
        match self.request(WireOperation::OpenOwnerSession(request.into()))? {
            WireSuccess::OwnerSession(session) => Ok(session.into_core()),
            _ => Err(unexpected_response()),
        }
    }

    fn apply_candidate(
        &self,
        owner: &OwnerSessionProof,
        bundle: &RuntimeBundle,
    ) -> Result<ApplyCandidateResult, CoreRuntimeError> {
        match self.request(WireOperation::ApplyCandidate(WireApplyRequest {
            owner: owner.into(),
            bundle: bundle.into(),
        }))? {
            WireSuccess::ApplyCandidate(result) => Ok(result.into_core()),
            _ => Err(unexpected_response()),
        }
    }

    fn status(&self, owner: &OwnerSessionProof) -> Result<CoreRuntimeStatus, CoreRuntimeError> {
        match self.request(WireOperation::Status(WireProofRequest {
            owner: owner.into(),
        }))? {
            WireSuccess::Status(status) => Ok(status.into_core()),
            _ => Err(unexpected_response()),
        }
    }

    fn logs(
        &self,
        owner: &OwnerSessionProof,
        after_sequence: Option<u64>,
        limit: usize,
    ) -> Result<ForwardedCoreLogBatch, CoreRuntimeError> {
        match self.request(WireOperation::Logs(WireLogsRequest {
            owner: owner.into(),
            after_sequence,
            limit: u64::try_from(limit).unwrap_or(u64::MAX),
        }))? {
            WireSuccess::Logs(logs) => Ok(logs.into_core()),
            _ => Err(unexpected_response()),
        }
    }

    fn stop(&self, owner: &OwnerSessionProof) -> Result<StopCoreResult, CoreRuntimeError> {
        match self.request(WireOperation::Stop(WireProofRequest {
            owner: owner.into(),
        }))? {
            WireSuccess::Stop(result) => Ok(result.into_core()),
            _ => Err(unexpected_response()),
        }
    }

    fn stop_with_timeout(
        &self,
        owner: &OwnerSessionProof,
        timeout: Duration,
    ) -> Result<StopCoreResult, CoreRuntimeError> {
        match self.request_with_timeout(
            WireOperation::Stop(WireProofRequest {
                owner: owner.into(),
            }),
            timeout,
        )? {
            WireSuccess::Stop(result) => Ok(result.into_core()),
            _ => Err(unexpected_response()),
        }
    }

    fn close_owner_session(&self, owner: &OwnerSessionProof) -> Result<(), CoreRuntimeError> {
        match self.request(WireOperation::CloseOwnerSession(WireProofRequest {
            owner: owner.into(),
        }))? {
            WireSuccess::CloseOwnerSession(WireEmpty {}) => Ok(()),
            _ => Err(unexpected_response()),
        }
    }

    fn cancel_pending_requests(&self) {
        self.apply_cancellation_requested
            .store(true, Ordering::Release);
        let mut active = self
            .active_request_streams
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for stream in active.values() {
            let _ = stream.shutdown(Shutdown::Both);
        }
        active.clear();
    }

    fn cancel_pending_apply(&self, owner: &OwnerSessionProof) -> Result<(), CoreRuntimeError> {
        self.cancel_pending_apply_with_timeout(owner, self.timeout_policy.request_timeout())
    }

    fn cancel_pending_apply_with_timeout(
        &self,
        owner: &OwnerSessionProof,
        timeout: Duration,
    ) -> Result<(), CoreRuntimeError> {
        self.cancel_pending_requests();
        match self.request_with_timeout(
            WireOperation::CancelPendingApply(WireProofRequest {
                owner: owner.into(),
            }),
            timeout,
        )? {
            WireSuccess::CancelPendingApply(WireEmpty {}) => Ok(()),
            _ => Err(unexpected_response()),
        }
    }

    fn close_owner_session_with_timeout(
        &self,
        owner: &OwnerSessionProof,
        timeout: Duration,
    ) -> Result<(), CoreRuntimeError> {
        match self.request_with_timeout(
            WireOperation::CloseOwnerSession(WireProofRequest {
                owner: owner.into(),
            }),
            timeout,
        )? {
            WireSuccess::CloseOwnerSession(WireEmpty {}) => Ok(()),
            _ => Err(unexpected_response()),
        }
    }
}
