//! Privileged CoreRuntime IPC client and server adapters.

use std::collections::VecDeque;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::net::Shutdown;
use std::os::fd::OwnedFd;
use std::os::unix::fs::{
    DirBuilderExt, FileTypeExt, MetadataExt, OpenOptionsExt, PermissionsExt, chown,
};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender, TrySendError};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use mio::net::UnixListener as MioUnixListener;
use mio::{Events, Interest, Poll, Token, Waker};
use nix::fcntl::{OFlag, open, openat};
use nix::sys::stat::{Mode, SFlag, fstat};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use socket2::{Domain, SockAddr, Socket, Type};

use crate::constants::{
    CORE_LOG_LINE_MAX_BYTES, CORE_SERVICE_MUTATION_TIMEOUT, CORE_SERVICE_REQUEST_TIMEOUT,
    EFFECTIVE_CONFIGURATION_MAX_BYTES, IPC_FRAME_MAX_BYTES, MIHOMO_BINARY_MAX_BYTES,
    PROFILE_RESPONSE_MAX_BYTES,
};
use crate::core::{
    ApplyCandidateResult, ApplyDisposition, CoreControlEndpoint, CoreRuntime,
    CoreRuntimeDiagnosticCategory, CoreRuntimeError, CoreRuntimeErrorKind, CoreRuntimeLifecycle,
    CoreRuntimeRestartStatus, CoreRuntimeStatus, CoreRuntimeTunReason, CoreRuntimeTunStatus,
    ForwardedCoreLog, ForwardedCoreLogBatch, ManagedCoreHandle, OwnerSession, OwnerSessionProof,
    OwnerSessionRequest, ProcessOutputSource, RuntimeBundle, StopCoreResult,
};
use crate::domain::{CoreInstanceGeneration, RuntimeGeneration};
use crate::ipc::{FrameError, read_frame, write_frame};
use crate::runtime_bundle::{
    RuntimeGenerationRetention, inspect_runtime_generations_with_reserved,
    prune_runtime_generations_with_reserved,
};
use crate::unix_io::DeadlineUnixStream;

pub const CORE_SERVICE_IPC_PROTOCOL_VERSION: u16 = 1;

const RUNTIME_MANIFEST_SCHEMA_VERSION: u16 = 1;
const RUNTIME_MANIFEST_MAX_BYTES: usize = 64 * 1_024;
const RUNTIME_PROVIDER_FILE_MAX: usize = 1_024;
const DEFAULT_SERVER_WORKERS: usize = 4;
const DEFAULT_PENDING_CONNECTIONS: usize = 32;
const CORE_SERVICE_LOG_BATCH_MAX: usize = IPC_FRAME_MAX_BYTES / CORE_LOG_LINE_MAX_BYTES / 2;
const CORE_SERVICE_LISTENER_TOKEN: Token = Token(0);
const CORE_SERVICE_SHUTDOWN_TOKEN: Token = Token(1);

// -----------------------------------------------------------------------------
// Synchronous client
// -----------------------------------------------------------------------------

pub struct CoreServiceClient {
    socket_path: PathBuf,
    expected_service_uid: u32,
    connect_timeout: Duration,
    timeout_policy: CoreServiceTimeoutPolicy,
    next_request_id: AtomicU64,
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

    fn connect(&self) -> io::Result<UnixStream> {
        if self.connect_timeout.is_zero() || !self.timeout_policy.is_valid() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Core service IPC deadlines must be positive",
            ));
        }
        let socket = Socket::new(Domain::UNIX, Type::STREAM, None)?;
        let address = SockAddr::unix(&self.socket_path)?;
        socket.connect_timeout(&address, self.connect_timeout)?;
        let stream = UnixStream::from(socket);
        let actual_uid = peer_uid(&stream).map_err(io::Error::other)?;
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
        let request_id = self.request_id();
        let request = WireRequest {
            protocol_version: CORE_SERVICE_IPC_PROTOCOL_VERSION,
            request_id,
            operation,
        };
        let stream = self.connect().map_err(transport_unavailable)?;
        let mut stream =
            DeadlineUnixStream::new(stream, response_timeout).map_err(transport_unavailable)?;
        stream.begin_write().map_err(transport_unavailable)?;
        write_frame(&mut stream, &request).map_err(map_write_error)?;
        stream.begin_read().map_err(transport_unavailable)?;
        let response: WireResponse = read_frame(&mut stream).map_err(map_read_error)?;
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

    fn close_owner_session(&self, owner: &OwnerSessionProof) -> Result<(), CoreRuntimeError> {
        match self.request(WireOperation::CloseOwnerSession(WireProofRequest {
            owner: owner.into(),
        }))? {
            WireSuccess::CloseOwnerSession(WireEmpty {}) => Ok(()),
            _ => Err(unexpected_response()),
        }
    }
}

// -----------------------------------------------------------------------------
// Bounded service
// -----------------------------------------------------------------------------

pub struct CoreServiceServerConfig {
    pub runtime_staging_root: PathBuf,
    pub allowed_owner_uid: u32,
    pub io_timeout: Duration,
    pub worker_count: usize,
    pub pending_connection_capacity: usize,
}

impl CoreServiceServerConfig {
    #[must_use]
    pub fn new(runtime_staging_root: impl Into<PathBuf>, allowed_owner_uid: u32) -> Self {
        Self {
            runtime_staging_root: runtime_staging_root.into(),
            allowed_owner_uid,
            io_timeout: CORE_SERVICE_REQUEST_TIMEOUT,
            worker_count: DEFAULT_SERVER_WORKERS,
            pending_connection_capacity: DEFAULT_PENDING_CONNECTIONS,
        }
    }
}

impl fmt::Debug for CoreServiceServerConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CoreServiceServerConfig")
            .field("runtime_staging_root", &"[REDACTED]")
            .field("allowed_owner_uid", &self.allowed_owner_uid)
            .field("io_timeout", &self.io_timeout)
            .field("worker_count", &self.worker_count)
            .field(
                "pending_connection_capacity",
                &self.pending_connection_capacity,
            )
            .finish()
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
struct SocketIdentity {
    device: u64,
    inode: u64,
}

pub struct CoreServiceServer {
    socket_path: PathBuf,
    socket_identity: SocketIdentity,
    shutdown: Arc<AtomicBool>,
    waker: Arc<Waker>,
    thread: Option<JoinHandle<io::Result<()>>>,
}

impl CoreServiceServer {
    pub fn start<R>(
        socket_path: impl AsRef<Path>,
        runtime: Arc<R>,
        config: CoreServiceServerConfig,
    ) -> io::Result<Self>
    where
        R: CoreRuntime + 'static,
    {
        validate_server_config(&config)?;
        let runtime_staging_root = prepare_runtime_root(&config.runtime_staging_root)
            .map_err(|error| safe_io_error(error, "Core service runtime root setup failed"))?;
        let runtime_retention =
            ServiceRuntimeRetention::load(&runtime_staging_root).map_err(|_| {
                io::Error::other("Core service Runtime Generation retention state is unsafe")
            })?;
        let socket_path = socket_path.as_ref().to_path_buf();
        let listener = bind_service_listener(&socket_path, config.allowed_owner_uid)?;
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
        let runtime: Arc<dyn CoreRuntime> = runtime;
        let context = Arc::new(ServerContext {
            runtime,
            runtime_staging_root,
            runtime_retention: Mutex::new(runtime_retention),
            session: Mutex::new(None),
        });
        let thread_accept_metrics = CoreServiceAcceptMetrics::default();
        let thread = match thread::Builder::new()
            .name("hopash-core-service-accept".to_owned())
            .spawn(move || {
                run_server(
                    listener,
                    poll,
                    context,
                    config,
                    thread_shutdown,
                    thread_accept_metrics,
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
            thread: Some(thread),
        })
    }

    pub fn shutdown(&mut self) -> io::Result<()> {
        self.shutdown.store(true, Ordering::Release);
        let wake_result = match self.thread.as_ref() {
            Some(thread) if !thread.is_finished() => self.waker.wake(),
            Some(_) | None => Ok(()),
        };
        let thread_result = self.thread.take().map_or(Ok(()), |thread| {
            thread
                .join()
                .map_err(|_| io::Error::other("Core service IPC thread panicked"))?
        });
        let cleanup_result = cleanup_socket(&self.socket_path, self.socket_identity);
        wake_result.and(thread_result).and(cleanup_result)
    }
}

#[derive(Clone, Default)]
struct CoreServiceAcceptMetrics {
    #[cfg(test)]
    poll_returns: Arc<AtomicU64>,
}

impl CoreServiceAcceptMetrics {
    fn record_poll_return(&self) {
        #[cfg(test)]
        self.poll_returns.fetch_add(1, Ordering::Relaxed);
    }

    #[cfg(test)]
    fn poll_returns(&self) -> u64 {
        self.poll_returns.load(Ordering::Relaxed)
    }
}

impl fmt::Debug for CoreServiceServer {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CoreServiceServer")
            .field("socket_path", &"[REDACTED]")
            .field("running", &self.thread.is_some())
            .finish()
    }
}

impl Drop for CoreServiceServer {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}

struct ServerContext {
    runtime: Arc<dyn CoreRuntime>,
    runtime_staging_root: PathBuf,
    runtime_retention: Mutex<ServiceRuntimeRetention>,
    session: Mutex<Option<BoundSession>>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct ServiceRuntimeRetention {
    current: Option<RuntimeGeneration>,
    previous: Option<RuntimeGeneration>,
    prepared: Option<RuntimeGeneration>,
}

impl ServiceRuntimeRetention {
    fn load(root: &Path) -> Result<Self, ()> {
        let generations =
            inspect_runtime_generations_with_reserved(root, &["control"]).map_err(|_| ())?;
        let mut newest = generations.into_iter().rev();
        let retention = Self {
            prepared: newest.next(),
            current: newest.next(),
            previous: newest.next(),
        };
        prune_runtime_generations_with_reserved(root, retention.into(), &["control"])
            .map_err(|_| ())?;
        Ok(retention)
    }

    fn plan(self, generation: RuntimeGeneration) -> Result<Self, BundleIngressError> {
        if generation.0 == 0 {
            return Err(BundleIngressError::Invalid);
        }
        if self.prepared == Some(generation) {
            return Ok(self);
        }
        if self.current == Some(generation) {
            return Ok(Self {
                prepared: None,
                ..self
            });
        }
        if self.previous == Some(generation) {
            return Ok(Self {
                current: Some(generation),
                previous: None,
                prepared: None,
            });
        }
        let highest = [self.current, self.previous, self.prepared]
            .into_iter()
            .flatten()
            .max();
        if highest.is_some_and(|highest| generation <= highest) {
            return Err(BundleIngressError::Invalid);
        }
        let (current, previous) = self
            .prepared
            .map_or((self.current, self.previous), |prepared| {
                (Some(prepared), self.current)
            });
        Ok(Self {
            current,
            previous,
            prepared: Some(generation),
        })
    }

    fn discard_failed(self, generation: RuntimeGeneration) -> Self {
        if self.prepared == Some(generation) {
            Self {
                prepared: None,
                ..self
            }
        } else {
            self
        }
    }
}

impl From<ServiceRuntimeRetention> for RuntimeGenerationRetention {
    fn from(retention: ServiceRuntimeRetention) -> Self {
        Self::new(retention.current, retention.previous, retention.prepared)
    }
}

struct BoundSession {
    owner_uid: u32,
    session_id: String,
}

struct AcceptedConnection {
    stream: UnixStream,
    peer_uid: u32,
}

fn validate_server_config(config: &CoreServiceServerConfig) -> io::Result<()> {
    if !config.runtime_staging_root.is_absolute()
        || config.io_timeout.is_zero()
        || config.worker_count == 0
        || config.pending_connection_capacity == 0
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Core service IPC limits and paths are invalid",
        ));
    }
    Ok(())
}

fn prepare_accept_loop(listener: UnixListener) -> io::Result<(MioUnixListener, Poll, Arc<Waker>)> {
    listener.set_nonblocking(true)?;
    let mut listener = MioUnixListener::from_std(listener);
    let poll = Poll::new()?;
    poll.registry().register(
        &mut listener,
        CORE_SERVICE_LISTENER_TOKEN,
        Interest::READABLE,
    )?;
    let waker = Arc::new(Waker::new(poll.registry(), CORE_SERVICE_SHUTDOWN_TOKEN)?);
    Ok((listener, poll, waker))
}

fn run_server(
    listener: MioUnixListener,
    mut poll: Poll,
    context: Arc<ServerContext>,
    config: CoreServiceServerConfig,
    shutdown: Arc<AtomicBool>,
    accept_metrics: CoreServiceAcceptMetrics,
) -> io::Result<()> {
    let (sender, receiver) = mpsc::sync_channel(config.pending_connection_capacity);
    let receiver = Arc::new(Mutex::new(receiver));
    let workers = spawn_workers(
        config.worker_count,
        Arc::clone(&receiver),
        Arc::clone(&context),
        config.io_timeout,
        Arc::clone(&shutdown),
    )?;
    let accept_result = accept_loop(
        &listener,
        &mut poll,
        &sender,
        config.allowed_owner_uid,
        &shutdown,
        &accept_metrics,
    );
    drop(sender);
    let mut worker_panicked = false;
    for worker in workers {
        worker_panicked |= worker.join().is_err();
    }
    if worker_panicked && accept_result.is_ok() {
        Err(io::Error::other("Core service IPC worker panicked"))
    } else {
        accept_result
    }
}

fn spawn_workers(
    count: usize,
    receiver: Arc<Mutex<Receiver<AcceptedConnection>>>,
    context: Arc<ServerContext>,
    io_timeout: Duration,
    shutdown: Arc<AtomicBool>,
) -> io::Result<Vec<JoinHandle<()>>> {
    (0..count)
        .map(|index| {
            let receiver = Arc::clone(&receiver);
            let context = Arc::clone(&context);
            let shutdown = Arc::clone(&shutdown);
            thread::Builder::new()
                .name(format!("hopash-core-service-worker-{index}"))
                .spawn(move || worker_loop(receiver, context, io_timeout, shutdown))
        })
        .collect()
}

fn worker_loop(
    receiver: Arc<Mutex<Receiver<AcceptedConnection>>>,
    context: Arc<ServerContext>,
    io_timeout: Duration,
    shutdown: Arc<AtomicBool>,
) {
    loop {
        let received = receiver
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .recv();
        match received {
            Ok(connection) if !shutdown.load(Ordering::Acquire) => {
                handle_connection(connection, &context, io_timeout);
            }
            Ok(connection) => {
                let _ = connection.stream.shutdown(Shutdown::Both);
                break;
            }
            Err(_) => break,
        }
    }
}

fn accept_loop(
    listener: &MioUnixListener,
    poll: &mut Poll,
    sender: &SyncSender<AcceptedConnection>,
    allowed_owner_uid: u32,
    shutdown: &AtomicBool,
    metrics: &CoreServiceAcceptMetrics,
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
            || events
                .iter()
                .any(|event| event.token() == CORE_SERVICE_SHUTDOWN_TOKEN)
        {
            return Ok(());
        }
        if events
            .iter()
            .any(|event| event.token() == CORE_SERVICE_LISTENER_TOKEN)
        {
            accept_ready_connections(listener, sender, allowed_owner_uid, shutdown)?;
        }
    }
}

fn accept_ready_connections(
    listener: &MioUnixListener,
    sender: &SyncSender<AcceptedConnection>,
    allowed_owner_uid: u32,
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
                let peer_uid = match peer_uid(&stream) {
                    Ok(peer_uid) if peer_uid == allowed_owner_uid => peer_uid,
                    Err(_) => {
                        let _ = stream.shutdown(Shutdown::Both);
                        continue;
                    }
                    Ok(_) => {
                        let _ = stream.shutdown(Shutdown::Both);
                        continue;
                    }
                };
                let connection = AcceptedConnection { stream, peer_uid };
                match sender.try_send(connection) {
                    Ok(()) => {}
                    Err(TrySendError::Full(connection))
                    | Err(TrySendError::Disconnected(connection)) => {
                        let _ = connection.stream.shutdown(Shutdown::Both);
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
    connection: AcceptedConnection,
    context: &ServerContext,
    io_timeout: Duration,
) {
    let mut stream = match DeadlineUnixStream::new(connection.stream, io_timeout) {
        Ok(stream) => stream,
        Err(_) => return,
    };
    if stream.begin_read().is_err() {
        return;
    }
    let request = match read_frame::<_, WireRequest>(&mut stream) {
        Ok(request) => request,
        Err(_) => {
            write_response(
                &mut stream,
                WireResponse::failure(0, CoreRuntimeErrorKind::ProtocolMismatch),
            );
            return;
        }
    };
    if request.protocol_version != CORE_SERVICE_IPC_PROTOCOL_VERSION || request.request_id == 0 {
        write_response(
            &mut stream,
            WireResponse::failure(request.request_id, CoreRuntimeErrorKind::ProtocolMismatch),
        );
        return;
    }
    let response = match dispatch(context, connection.peer_uid, request.operation) {
        Ok(success) => WireResponse::success(request.request_id, success),
        Err(error) => WireResponse::failure(request.request_id, error.kind),
    };
    write_response(&mut stream, response);
}

fn dispatch(
    context: &ServerContext,
    peer_uid: u32,
    operation: WireOperation,
) -> Result<WireSuccess, CoreRuntimeError> {
    let mut binding = context
        .session
        .lock()
        .map_err(|_| unavailable_error("Core service session state is unavailable"))?;
    match operation {
        WireOperation::OpenOwnerSession(request) => {
            let request = request.into_core();
            if peer_uid != request.owner_uid {
                return Err(authentication_error("Core service peer UID mismatch"));
            }
            let session = context.runtime.open_owner_session(&request)?;
            if session.proof.session_id().is_empty()
                || session.proof.session_token().is_empty()
                || session.protocol_version != request.protocol_version
            {
                return Err(unavailable_error(
                    "Core service returned an invalid session",
                ));
            }
            *binding = Some(BoundSession {
                owner_uid: peer_uid,
                session_id: session.proof.session_id().to_owned(),
            });
            Ok(WireSuccess::OwnerSession((&session).into()))
        }
        WireOperation::ApplyCandidate(request) => {
            let owner = request.owner.into_core();
            authorize_bound_session(binding.as_ref(), peer_uid, &owner)?;
            let bundle = request.bundle.into_core();
            let mut retention = context.runtime_retention.lock().map_err(|_| {
                unavailable_error("Core service Runtime Generation state is unavailable")
            })?;
            let planned = retention
                .plan(bundle.generation)
                .map_err(|error| error.into_core())?;
            let staged = stage_runtime_bundle(&context.runtime_staging_root, peer_uid, &bundle)?;
            prune_runtime_generations_with_reserved(
                &context.runtime_staging_root,
                planned.into(),
                &["control"],
            )
            .map_err(|_| unavailable_error("Core service Runtime Generation cleanup failed"))?;
            *retention = planned;
            match context.runtime.apply_candidate(&owner, &staged) {
                Ok(result) => Ok(WireSuccess::ApplyCandidate((&result).into())),
                Err(error) if runtime_apply_is_indeterminate(error.kind) => Err(error),
                Err(error) => {
                    let retained = retention.discard_failed(bundle.generation);
                    prune_runtime_generations_with_reserved(
                        &context.runtime_staging_root,
                        retained.into(),
                        &["control"],
                    )
                    .map_err(|_| {
                        unavailable_error("Core service Runtime Generation cleanup failed")
                    })?;
                    *retention = retained;
                    Err(error)
                }
            }
        }
        WireOperation::Status(request) => {
            let owner = request.owner.into_core();
            authorize_bound_session(binding.as_ref(), peer_uid, &owner)?;
            context
                .runtime
                .status(&owner)
                .map(|status| WireSuccess::Status((&status).into()))
        }
        WireOperation::Logs(request) => {
            let owner = request.owner.into_core();
            authorize_bound_session(binding.as_ref(), peer_uid, &owner)?;
            let limit = usize::try_from(request.limit)
                .map_err(|_| protocol_error("Core service log limit is invalid"))?;
            let limit = limit.min(CORE_SERVICE_LOG_BATCH_MAX);
            let logs = context
                .runtime
                .logs(&owner, request.after_sequence, limit)?;
            if logs.records.len() > limit
                || logs
                    .records
                    .iter()
                    .any(|record| record.message.len() > CORE_LOG_LINE_MAX_BYTES)
            {
                return Err(unavailable_error(
                    "Core service returned an oversized log batch",
                ));
            }
            Ok(WireSuccess::Logs((&logs).into()))
        }
        WireOperation::Stop(request) => {
            let owner = request.owner.into_core();
            authorize_bound_session(binding.as_ref(), peer_uid, &owner)?;
            context
                .runtime
                .stop(&owner)
                .map(|result| WireSuccess::Stop((&result).into()))
        }
        WireOperation::CloseOwnerSession(request) => {
            let owner = request.owner.into_core();
            authorize_bound_session(binding.as_ref(), peer_uid, &owner)?;
            context.runtime.close_owner_session(&owner)?;
            *binding = None;
            Ok(WireSuccess::CloseOwnerSession(WireEmpty {}))
        }
    }
}

fn authorize_bound_session(
    binding: Option<&BoundSession>,
    peer_uid: u32,
    proof: &OwnerSessionProof,
) -> Result<(), CoreRuntimeError> {
    if binding.is_some_and(|binding| {
        binding.owner_uid == peer_uid && binding.session_id == proof.session_id()
    }) {
        Ok(())
    } else {
        Err(authentication_error(
            "Core service session is not bound to this peer",
        ))
    }
}

fn write_response(stream: &mut DeadlineUnixStream, response: WireResponse) {
    if stream.begin_write().is_ok() {
        let _ = write_frame(stream, &response);
    }
}

// -----------------------------------------------------------------------------
// Versioned wire contract
// -----------------------------------------------------------------------------

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct WireRequest {
    protocol_version: u16,
    request_id: u64,
    operation: WireOperation,
}

#[derive(Deserialize, Serialize)]
#[serde(
    tag = "operation",
    content = "payload",
    rename_all = "snake_case",
    deny_unknown_fields
)]
enum WireOperation {
    OpenOwnerSession(WireOwnerSessionRequest),
    ApplyCandidate(WireApplyRequest),
    Status(WireProofRequest),
    Logs(WireLogsRequest),
    Stop(WireProofRequest),
    CloseOwnerSession(WireProofRequest),
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct WireResponse {
    protocol_version: u16,
    request_id: u64,
    outcome: WireOutcome,
}

impl WireResponse {
    fn success(request_id: u64, success: WireSuccess) -> Self {
        let response = Self {
            protocol_version: CORE_SERVICE_IPC_PROTOCOL_VERSION,
            request_id,
            outcome: WireOutcome::Success(success),
        };
        if serde_json::to_vec(&response).is_ok_and(|encoded| encoded.len() <= IPC_FRAME_MAX_BYTES) {
            response
        } else {
            Self::failure(request_id, CoreRuntimeErrorKind::Unavailable)
        }
    }

    fn failure(request_id: u64, kind: CoreRuntimeErrorKind) -> Self {
        Self {
            protocol_version: CORE_SERVICE_IPC_PROTOCOL_VERSION,
            request_id,
            outcome: WireOutcome::Failure(WireCoreRuntimeError { kind: kind.into() }),
        }
    }
}

#[derive(Deserialize, Serialize)]
#[serde(
    tag = "outcome",
    content = "payload",
    rename_all = "snake_case",
    deny_unknown_fields
)]
enum WireOutcome {
    Success(WireSuccess),
    Failure(WireCoreRuntimeError),
}

#[derive(Deserialize, Serialize)]
#[serde(
    tag = "operation",
    content = "payload",
    rename_all = "snake_case",
    deny_unknown_fields
)]
enum WireSuccess {
    OwnerSession(WireOwnerSession),
    ApplyCandidate(WireApplyCandidateResult),
    Status(WireCoreRuntimeStatus),
    Logs(WireForwardedCoreLogBatch),
    Stop(WireStopCoreResult),
    CloseOwnerSession(WireEmpty),
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct WireEmpty {}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct WireOwnerSessionRequest {
    owner_uid: u32,
    supervisor_pid: u32,
    supervisor_start_identity: String,
    instance_token: String,
    protocol_version: u16,
}

impl From<&OwnerSessionRequest> for WireOwnerSessionRequest {
    fn from(request: &OwnerSessionRequest) -> Self {
        Self {
            owner_uid: request.owner_uid,
            supervisor_pid: request.supervisor_pid,
            supervisor_start_identity: request.supervisor_start_identity.clone(),
            instance_token: request.instance_token.clone(),
            protocol_version: request.protocol_version,
        }
    }
}

impl WireOwnerSessionRequest {
    fn into_core(self) -> OwnerSessionRequest {
        OwnerSessionRequest {
            owner_uid: self.owner_uid,
            supervisor_pid: self.supervisor_pid,
            supervisor_start_identity: self.supervisor_start_identity,
            instance_token: self.instance_token,
            protocol_version: self.protocol_version,
        }
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct WireOwnerSessionProof {
    session_id: String,
    session_token: String,
}

impl From<&OwnerSessionProof> for WireOwnerSessionProof {
    fn from(proof: &OwnerSessionProof) -> Self {
        Self {
            session_id: proof.session_id().to_owned(),
            session_token: proof.session_token().to_owned(),
        }
    }
}

impl WireOwnerSessionProof {
    fn into_core(self) -> OwnerSessionProof {
        OwnerSessionProof::new(self.session_id, self.session_token)
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct WireOwnerSession {
    proof: WireOwnerSessionProof,
    protocol_version: u16,
    owner_generation: u64,
    endpoint: WireCoreControlEndpoint,
}

impl From<&OwnerSession> for WireOwnerSession {
    fn from(session: &OwnerSession) -> Self {
        Self {
            proof: (&session.proof).into(),
            protocol_version: session.protocol_version,
            owner_generation: session.owner_generation,
            endpoint: (&session.endpoint).into(),
        }
    }
}

impl WireOwnerSession {
    fn into_core(self) -> OwnerSession {
        OwnerSession {
            proof: self.proof.into_core(),
            protocol_version: self.protocol_version,
            owner_generation: self.owner_generation,
            endpoint: self.endpoint.into_core(),
        }
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct WireProofRequest {
    owner: WireOwnerSessionProof,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct WireApplyRequest {
    owner: WireOwnerSessionProof,
    bundle: WireRuntimeBundle,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct WireLogsRequest {
    owner: WireOwnerSessionProof,
    after_sequence: Option<u64>,
    limit: u64,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct WireRuntimeBundle {
    generation: u64,
    generation_root: PathBuf,
    manifest_sha256: String,
    compiler_policy_sha256: String,
    mihomo_binary_sha256: String,
}

impl From<&RuntimeBundle> for WireRuntimeBundle {
    fn from(bundle: &RuntimeBundle) -> Self {
        Self {
            generation: bundle.generation.0,
            generation_root: bundle.generation_root.clone(),
            manifest_sha256: bundle.manifest_sha256.clone(),
            compiler_policy_sha256: bundle.compiler_policy_sha256.clone(),
            mihomo_binary_sha256: bundle.mihomo_binary_sha256.clone(),
        }
    }
}

impl WireRuntimeBundle {
    fn into_core(self) -> RuntimeBundle {
        RuntimeBundle {
            generation: RuntimeGeneration(self.generation),
            generation_root: self.generation_root,
            manifest_sha256: self.manifest_sha256,
            compiler_policy_sha256: self.compiler_policy_sha256,
            mihomo_binary_sha256: self.mihomo_binary_sha256,
        }
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct WireCoreControlEndpoint {
    socket_path: PathBuf,
    secret: String,
}

impl From<&CoreControlEndpoint> for WireCoreControlEndpoint {
    fn from(endpoint: &CoreControlEndpoint) -> Self {
        Self {
            socket_path: endpoint.socket_path.clone(),
            secret: endpoint.secret().to_owned(),
        }
    }
}

impl WireCoreControlEndpoint {
    fn into_core(self) -> CoreControlEndpoint {
        CoreControlEndpoint::new(self.socket_path, self.secret)
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct WireManagedCoreHandle {
    pid: u32,
    process_start_identity: String,
    endpoint: WireCoreControlEndpoint,
    instance_generation: u64,
    runtime_generation: u64,
}

impl From<&ManagedCoreHandle> for WireManagedCoreHandle {
    fn from(handle: &ManagedCoreHandle) -> Self {
        Self {
            pid: handle.pid,
            process_start_identity: handle.process_start_identity.clone(),
            endpoint: (&handle.endpoint).into(),
            instance_generation: handle.instance_generation.0,
            runtime_generation: handle.runtime_generation.0,
        }
    }
}

impl WireManagedCoreHandle {
    fn into_core(self) -> ManagedCoreHandle {
        ManagedCoreHandle {
            pid: self.pid,
            process_start_identity: self.process_start_identity,
            endpoint: self.endpoint.into_core(),
            instance_generation: CoreInstanceGeneration(self.instance_generation),
            runtime_generation: RuntimeGeneration(self.runtime_generation),
        }
    }
}

#[derive(Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum WireApplyDisposition {
    Spawned,
    Reloaded,
}

impl From<ApplyDisposition> for WireApplyDisposition {
    fn from(disposition: ApplyDisposition) -> Self {
        match disposition {
            ApplyDisposition::Spawned => Self::Spawned,
            ApplyDisposition::Reloaded => Self::Reloaded,
        }
    }
}

impl WireApplyDisposition {
    fn into_core(self) -> ApplyDisposition {
        match self {
            Self::Spawned => ApplyDisposition::Spawned,
            Self::Reloaded => ApplyDisposition::Reloaded,
        }
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct WireApplyCandidateResult {
    disposition: WireApplyDisposition,
    managed_core: WireManagedCoreHandle,
}

impl From<&ApplyCandidateResult> for WireApplyCandidateResult {
    fn from(result: &ApplyCandidateResult) -> Self {
        Self {
            disposition: result.disposition.into(),
            managed_core: (&result.managed_core).into(),
        }
    }
}

impl WireApplyCandidateResult {
    fn into_core(self) -> ApplyCandidateResult {
        ApplyCandidateResult {
            disposition: self.disposition.into_core(),
            managed_core: self.managed_core.into_core(),
        }
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct WireCoreRuntimeStatus {
    managed_core: Option<WireManagedCoreHandle>,
    lifecycle: WireCoreRuntimeLifecycle,
    restart: WireCoreRuntimeRestartStatus,
    tun: WireCoreRuntimeTunStatus,
}

impl From<&CoreRuntimeStatus> for WireCoreRuntimeStatus {
    fn from(status: &CoreRuntimeStatus) -> Self {
        Self {
            managed_core: status.managed_core.as_ref().map(Into::into),
            lifecycle: status.lifecycle.into(),
            restart: (&status.restart).into(),
            tun: status.tun.into(),
        }
    }
}

impl WireCoreRuntimeStatus {
    fn into_core(self) -> CoreRuntimeStatus {
        CoreRuntimeStatus {
            managed_core: self.managed_core.map(WireManagedCoreHandle::into_core),
            lifecycle: self.lifecycle.into_core(),
            restart: self.restart.into_core(),
            tun: self.tun.into_core(),
        }
    }
}

#[derive(Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum WireCoreRuntimeLifecycle {
    Owned,
    Running,
    RestartPending,
    Degraded,
}

impl From<CoreRuntimeLifecycle> for WireCoreRuntimeLifecycle {
    fn from(lifecycle: CoreRuntimeLifecycle) -> Self {
        match lifecycle {
            CoreRuntimeLifecycle::Owned => Self::Owned,
            CoreRuntimeLifecycle::Running => Self::Running,
            CoreRuntimeLifecycle::RestartPending => Self::RestartPending,
            CoreRuntimeLifecycle::Degraded => Self::Degraded,
        }
    }
}

impl WireCoreRuntimeLifecycle {
    fn into_core(self) -> CoreRuntimeLifecycle {
        match self {
            Self::Owned => CoreRuntimeLifecycle::Owned,
            Self::Running => CoreRuntimeLifecycle::Running,
            Self::RestartPending => CoreRuntimeLifecycle::RestartPending,
            Self::Degraded => CoreRuntimeLifecycle::Degraded,
        }
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct WireCoreRuntimeRestartStatus {
    pending: bool,
    attempts: u64,
    backoff_ms: Option<u64>,
    diagnostic: Option<WireCoreRuntimeDiagnosticCategory>,
}

impl From<&CoreRuntimeRestartStatus> for WireCoreRuntimeRestartStatus {
    fn from(status: &CoreRuntimeRestartStatus) -> Self {
        Self {
            pending: status.pending,
            attempts: u64::try_from(status.attempts).unwrap_or(u64::MAX),
            backoff_ms: status
                .backoff
                .map(|backoff| u64::try_from(backoff.as_millis()).unwrap_or(u64::MAX)),
            diagnostic: status.diagnostic.map(Into::into),
        }
    }
}

impl WireCoreRuntimeRestartStatus {
    fn into_core(self) -> CoreRuntimeRestartStatus {
        CoreRuntimeRestartStatus {
            pending: self.pending,
            attempts: usize::try_from(self.attempts).unwrap_or(usize::MAX),
            backoff: self.backoff_ms.map(Duration::from_millis),
            diagnostic: self
                .diagnostic
                .map(WireCoreRuntimeDiagnosticCategory::into_core),
        }
    }
}

#[derive(Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum WireCoreRuntimeDiagnosticCategory {
    CoreRestartLimitReached,
}

impl From<CoreRuntimeDiagnosticCategory> for WireCoreRuntimeDiagnosticCategory {
    fn from(category: CoreRuntimeDiagnosticCategory) -> Self {
        match category {
            CoreRuntimeDiagnosticCategory::CoreRestartLimitReached => Self::CoreRestartLimitReached,
        }
    }
}

impl WireCoreRuntimeDiagnosticCategory {
    fn into_core(self) -> CoreRuntimeDiagnosticCategory {
        match self {
            Self::CoreRestartLimitReached => CoreRuntimeDiagnosticCategory::CoreRestartLimitReached,
        }
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct WireCoreRuntimeTunStatus {
    capable: bool,
    reason: Option<WireCoreRuntimeTunReason>,
}

impl From<CoreRuntimeTunStatus> for WireCoreRuntimeTunStatus {
    fn from(status: CoreRuntimeTunStatus) -> Self {
        Self {
            capable: status.capable,
            reason: status.reason.map(Into::into),
        }
    }
}

impl WireCoreRuntimeTunStatus {
    fn into_core(self) -> CoreRuntimeTunStatus {
        CoreRuntimeTunStatus {
            capable: self.capable,
            reason: self.reason.map(WireCoreRuntimeTunReason::into_core),
        }
    }
}

#[derive(Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum WireCoreRuntimeTunReason {
    PermissionDenied,
    Unsupported,
}

impl From<CoreRuntimeTunReason> for WireCoreRuntimeTunReason {
    fn from(reason: CoreRuntimeTunReason) -> Self {
        match reason {
            CoreRuntimeTunReason::PermissionDenied => Self::PermissionDenied,
            CoreRuntimeTunReason::Unsupported => Self::Unsupported,
        }
    }
}

impl WireCoreRuntimeTunReason {
    fn into_core(self) -> CoreRuntimeTunReason {
        match self {
            Self::PermissionDenied => CoreRuntimeTunReason::PermissionDenied,
            Self::Unsupported => CoreRuntimeTunReason::Unsupported,
        }
    }
}

#[derive(Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum WireProcessOutputSource {
    Stdout,
    Stderr,
}

impl From<ProcessOutputSource> for WireProcessOutputSource {
    fn from(source: ProcessOutputSource) -> Self {
        match source {
            ProcessOutputSource::Stdout => Self::Stdout,
            ProcessOutputSource::Stderr => Self::Stderr,
        }
    }
}

impl WireProcessOutputSource {
    fn into_core(self) -> ProcessOutputSource {
        match self {
            Self::Stdout => ProcessOutputSource::Stdout,
            Self::Stderr => ProcessOutputSource::Stderr,
        }
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct WireForwardedCoreLog {
    sequence: u64,
    timestamp_unix_ms: u64,
    source: WireProcessOutputSource,
    message: String,
    instance_generation: u64,
}

impl From<&ForwardedCoreLog> for WireForwardedCoreLog {
    fn from(log: &ForwardedCoreLog) -> Self {
        Self {
            sequence: log.sequence,
            timestamp_unix_ms: log.timestamp_unix_ms,
            source: log.source.into(),
            message: log.message.clone(),
            instance_generation: log.instance_generation.0,
        }
    }
}

impl WireForwardedCoreLog {
    fn into_core(self) -> ForwardedCoreLog {
        ForwardedCoreLog {
            sequence: self.sequence,
            timestamp_unix_ms: self.timestamp_unix_ms,
            source: self.source.into_core(),
            message: self.message,
            instance_generation: CoreInstanceGeneration(self.instance_generation),
        }
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct WireForwardedCoreLogBatch {
    records: Vec<WireForwardedCoreLog>,
    next_sequence: Option<u64>,
    dropped_before: u64,
}

impl From<&ForwardedCoreLogBatch> for WireForwardedCoreLogBatch {
    fn from(batch: &ForwardedCoreLogBatch) -> Self {
        Self {
            records: batch.records.iter().map(Into::into).collect(),
            next_sequence: batch.next_sequence,
            dropped_before: batch.dropped_before,
        }
    }
}

impl WireForwardedCoreLogBatch {
    fn into_core(self) -> ForwardedCoreLogBatch {
        ForwardedCoreLogBatch {
            records: self
                .records
                .into_iter()
                .map(WireForwardedCoreLog::into_core)
                .collect(),
            next_sequence: self.next_sequence,
            dropped_before: self.dropped_before,
        }
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct WireStopCoreResult {
    stopped: bool,
    instance_generation: Option<u64>,
}

impl From<&StopCoreResult> for WireStopCoreResult {
    fn from(result: &StopCoreResult) -> Self {
        Self {
            stopped: result.stopped,
            instance_generation: result.instance_generation.map(|generation| generation.0),
        }
    }
}

impl WireStopCoreResult {
    fn into_core(self) -> StopCoreResult {
        StopCoreResult {
            stopped: self.stopped,
            instance_generation: self.instance_generation.map(CoreInstanceGeneration),
        }
    }
}

#[derive(Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum WireCoreRuntimeErrorKind {
    Authentication,
    ProtocolMismatch,
    TunPermissionDenied,
    InvalidBundle,
    ProcessIdentityMismatch,
    Apply,
    ReloadTimeout,
    Readiness,
    Unavailable,
}

impl From<CoreRuntimeErrorKind> for WireCoreRuntimeErrorKind {
    fn from(kind: CoreRuntimeErrorKind) -> Self {
        match kind {
            CoreRuntimeErrorKind::Authentication => Self::Authentication,
            CoreRuntimeErrorKind::ProtocolMismatch => Self::ProtocolMismatch,
            CoreRuntimeErrorKind::TunPermissionDenied => Self::TunPermissionDenied,
            CoreRuntimeErrorKind::InvalidBundle => Self::InvalidBundle,
            CoreRuntimeErrorKind::ProcessIdentityMismatch => Self::ProcessIdentityMismatch,
            CoreRuntimeErrorKind::Apply => Self::Apply,
            CoreRuntimeErrorKind::ReloadTimeout => Self::ReloadTimeout,
            CoreRuntimeErrorKind::Readiness => Self::Readiness,
            CoreRuntimeErrorKind::Unavailable => Self::Unavailable,
        }
    }
}

impl WireCoreRuntimeErrorKind {
    fn into_core(self) -> CoreRuntimeErrorKind {
        match self {
            Self::Authentication => CoreRuntimeErrorKind::Authentication,
            Self::ProtocolMismatch => CoreRuntimeErrorKind::ProtocolMismatch,
            Self::TunPermissionDenied => CoreRuntimeErrorKind::TunPermissionDenied,
            Self::InvalidBundle => CoreRuntimeErrorKind::InvalidBundle,
            Self::ProcessIdentityMismatch => CoreRuntimeErrorKind::ProcessIdentityMismatch,
            Self::Apply => CoreRuntimeErrorKind::Apply,
            Self::ReloadTimeout => CoreRuntimeErrorKind::ReloadTimeout,
            Self::Readiness => CoreRuntimeErrorKind::Readiness,
            Self::Unavailable => CoreRuntimeErrorKind::Unavailable,
        }
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct WireCoreRuntimeError {
    kind: WireCoreRuntimeErrorKind,
}

impl WireCoreRuntimeError {
    fn into_core(self) -> CoreRuntimeError {
        CoreRuntimeError::new(
            self.kind.into_core(),
            "remote Core runtime operation failed",
        )
    }
}

// -----------------------------------------------------------------------------
// Runtime Bundle ingress
// -----------------------------------------------------------------------------

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct IngressRuntimeManifest {
    schema_version: u16,
    runtime_generation: u64,
    compiler_policy_sha256: String,
    mihomo_binary_sha256: String,
    configuration_sha256: String,
    executable: String,
    configuration: String,
    provider_files: Vec<IngressManifestFile>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct IngressManifestFile {
    path: String,
    sha256: String,
    size: u64,
}

#[derive(Clone, Copy)]
enum BundleIngressError {
    Invalid,
    Unavailable,
}

impl BundleIngressError {
    fn into_core(self) -> CoreRuntimeError {
        match self {
            Self::Invalid => {
                CoreRuntimeError::new(CoreRuntimeErrorKind::InvalidBundle, "bundle ingress failed")
            }
            Self::Unavailable => CoreRuntimeError::new(
                CoreRuntimeErrorKind::Unavailable,
                "bundle staging is unavailable",
            ),
        }
    }
}

const fn runtime_apply_is_indeterminate(kind: CoreRuntimeErrorKind) -> bool {
    matches!(
        kind,
        CoreRuntimeErrorKind::ReloadTimeout | CoreRuntimeErrorKind::Unavailable
    )
}

fn stage_runtime_bundle(
    runtime_root: &Path,
    owner_uid: u32,
    bundle: &RuntimeBundle,
) -> Result<RuntimeBundle, CoreRuntimeError> {
    stage_runtime_bundle_inner(runtime_root, owner_uid, bundle)
        .map_err(BundleIngressError::into_core)
}

fn stage_runtime_bundle_inner(
    runtime_root: &Path,
    owner_uid: u32,
    bundle: &RuntimeBundle,
) -> Result<RuntimeBundle, BundleIngressError> {
    if !bundle.generation_root.is_absolute()
        || !valid_digest(&bundle.manifest_sha256)
        || !valid_digest(&bundle.compiler_policy_sha256)
        || !valid_digest(&bundle.mihomo_binary_sha256)
    {
        return Err(BundleIngressError::Invalid);
    }
    let final_root = runtime_root.join(format!("generation-{:020}", bundle.generation.0));
    if generation_directory_exists(&final_root)? {
        return Ok(staged_bundle(bundle, final_root));
    }

    let source_root = open_source_root(&bundle.generation_root, owner_uid)?;
    let manifest_bytes = read_source_bytes(
        &source_root,
        owner_uid,
        Path::new("manifest.json"),
        RUNTIME_MANIFEST_MAX_BYTES,
    )?;
    if sha256_hex(&manifest_bytes) != bundle.manifest_sha256 {
        return Err(BundleIngressError::Invalid);
    }
    let manifest: IngressRuntimeManifest =
        serde_json::from_slice(&manifest_bytes).map_err(|_| BundleIngressError::Invalid)?;
    validate_ingress_manifest(&manifest, bundle)?;

    let pending_root = create_pending_root(runtime_root, bundle.generation)?;
    let stage_result = stage_pending_bundle(
        &source_root,
        owner_uid,
        &pending_root,
        &manifest_bytes,
        &manifest,
    );
    if let Err(error) = stage_result {
        let _ = remove_pending_root(runtime_root, &pending_root);
        return Err(error);
    }

    match fs::rename(&pending_root, &final_root) {
        Ok(()) => sync_directory(runtime_root).map_err(|_| BundleIngressError::Unavailable)?,
        Err(error)
            if matches!(
                error.kind(),
                io::ErrorKind::AlreadyExists | io::ErrorKind::DirectoryNotEmpty
            ) =>
        {
            remove_pending_root(runtime_root, &pending_root)?;
            if !generation_directory_exists(&final_root)? {
                return Err(BundleIngressError::Unavailable);
            }
        }
        Err(_) => {
            let _ = remove_pending_root(runtime_root, &pending_root);
            return Err(BundleIngressError::Unavailable);
        }
    }
    Ok(staged_bundle(bundle, final_root))
}

fn validate_ingress_manifest(
    manifest: &IngressRuntimeManifest,
    bundle: &RuntimeBundle,
) -> Result<(), BundleIngressError> {
    if manifest.schema_version != RUNTIME_MANIFEST_SCHEMA_VERSION
        || manifest.runtime_generation != bundle.generation.0
        || manifest.compiler_policy_sha256 != bundle.compiler_policy_sha256
        || manifest.mihomo_binary_sha256 != bundle.mihomo_binary_sha256
        || !valid_digest(&manifest.configuration_sha256)
        || manifest.executable != "mihomo"
        || manifest.configuration != "config.yaml"
        || manifest.provider_files.len() > RUNTIME_PROVIDER_FILE_MAX
    {
        return Err(BundleIngressError::Invalid);
    }
    let mut previous: Option<&str> = None;
    for file in &manifest.provider_files {
        if previous.is_some_and(|previous| previous >= file.path.as_str())
            || !valid_provider_path(&file.path)
            || !valid_digest(&file.sha256)
            || file.size > PROFILE_RESPONSE_MAX_BYTES as u64
        {
            return Err(BundleIngressError::Invalid);
        }
        previous = Some(&file.path);
    }
    Ok(())
}

fn stage_pending_bundle(
    source_root: &OwnedFd,
    owner_uid: u32,
    pending_root: &Path,
    manifest_bytes: &[u8],
    manifest: &IngressRuntimeManifest,
) -> Result<(), BundleIngressError> {
    copy_verified_file(
        source_root,
        owner_uid,
        BundleFileCopy {
            relative_path: Path::new("mihomo"),
            destination: &pending_root.join("mihomo"),
            limit: MIHOMO_BINARY_MAX_BYTES,
            expected_size: None,
            expected_sha256: &manifest.mihomo_binary_sha256,
            mode: 0o500,
        },
    )?;
    copy_verified_file(
        source_root,
        owner_uid,
        BundleFileCopy {
            relative_path: Path::new("config.yaml"),
            destination: &pending_root.join("config.yaml"),
            limit: EFFECTIVE_CONFIGURATION_MAX_BYTES,
            expected_size: None,
            expected_sha256: &manifest.configuration_sha256,
            mode: 0o400,
        },
    )?;
    for provider in &manifest.provider_files {
        let relative = Path::new(&provider.path);
        let destination = pending_root.join(relative);
        create_destination_parents(pending_root, &destination)?;
        copy_verified_file(
            source_root,
            owner_uid,
            BundleFileCopy {
                relative_path: relative,
                destination: &destination,
                limit: PROFILE_RESPONSE_MAX_BYTES,
                expected_size: Some(provider.size),
                expected_sha256: &provider.sha256,
                mode: 0o400,
            },
        )?;
    }
    write_new_file(&pending_root.join("manifest.json"), manifest_bytes, 0o400)?;
    sync_tree_directories(pending_root)?;
    Ok(())
}

fn open_source_root(path: &Path, owner_uid: u32) -> Result<OwnedFd, BundleIngressError> {
    let descriptor = open(
        path,
        OFlag::O_RDONLY | OFlag::O_DIRECTORY | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC,
        Mode::empty(),
    )
    .map_err(|_| BundleIngressError::Invalid)?;
    let metadata = fstat(&descriptor).map_err(|_| BundleIngressError::Invalid)?;
    if !SFlag::from_bits_truncate(metadata.st_mode).contains(SFlag::S_IFDIR)
        || metadata.st_uid != owner_uid
    {
        return Err(BundleIngressError::Invalid);
    }
    Ok(descriptor)
}

fn open_source_file(
    root: &OwnedFd,
    owner_uid: u32,
    path: &Path,
) -> Result<(OwnedFd, u64), BundleIngressError> {
    let components = path
        .components()
        .map(|component| match component {
            Component::Normal(component) => Ok(component),
            _ => Err(BundleIngressError::Invalid),
        })
        .collect::<Result<Vec<_>, _>>()?;
    let (file_name, directories) = components.split_last().ok_or(BundleIngressError::Invalid)?;
    let mut directory = root.try_clone().map_err(|_| BundleIngressError::Invalid)?;
    for component in directories {
        directory = openat(
            &directory,
            Path::new(component),
            OFlag::O_RDONLY | OFlag::O_DIRECTORY | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC,
            Mode::empty(),
        )
        .map_err(|_| BundleIngressError::Invalid)?;
        let metadata = fstat(&directory).map_err(|_| BundleIngressError::Invalid)?;
        if !SFlag::from_bits_truncate(metadata.st_mode).contains(SFlag::S_IFDIR)
            || metadata.st_uid != owner_uid
        {
            return Err(BundleIngressError::Invalid);
        }
    }
    let descriptor = openat(
        &directory,
        Path::new(file_name),
        OFlag::O_RDONLY | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC | OFlag::O_NONBLOCK,
        Mode::empty(),
    )
    .map_err(|_| BundleIngressError::Invalid)?;
    let metadata = fstat(&descriptor).map_err(|_| BundleIngressError::Invalid)?;
    if !SFlag::from_bits_truncate(metadata.st_mode).contains(SFlag::S_IFREG)
        || metadata.st_size < 0
        || metadata.st_uid != owner_uid
    {
        return Err(BundleIngressError::Invalid);
    }
    Ok((descriptor, metadata.st_size as u64))
}

fn read_source_bytes(
    root: &OwnedFd,
    owner_uid: u32,
    path: &Path,
    limit: usize,
) -> Result<Vec<u8>, BundleIngressError> {
    let (descriptor, size) = open_source_file(root, owner_uid, path)?;
    if size > limit as u64 {
        return Err(BundleIngressError::Invalid);
    }
    let mut content = Vec::with_capacity(size as usize);
    File::from(descriptor)
        .take((limit as u64).saturating_add(1))
        .read_to_end(&mut content)
        .map_err(|_| BundleIngressError::Invalid)?;
    if content.len() > limit || content.len() as u64 != size {
        return Err(BundleIngressError::Invalid);
    }
    Ok(content)
}

struct BundleFileCopy<'a> {
    relative_path: &'a Path,
    destination: &'a Path,
    limit: usize,
    expected_size: Option<u64>,
    expected_sha256: &'a str,
    mode: u32,
}

fn copy_verified_file(
    source_root: &OwnedFd,
    owner_uid: u32,
    copy: BundleFileCopy<'_>,
) -> Result<(), BundleIngressError> {
    let (descriptor, initial_size) = open_source_file(source_root, owner_uid, copy.relative_path)?;
    if initial_size > copy.limit as u64
        || copy.expected_size.is_some_and(|size| size != initial_size)
    {
        return Err(BundleIngressError::Invalid);
    }
    let mut source = File::from(descriptor);
    let mut options = OpenOptions::new();
    options.write(true).create_new(true).mode(copy.mode);
    let mut target = options
        .open(copy.destination)
        .map_err(|_| BundleIngressError::Unavailable)?;
    let mut hasher = Sha256::new();
    let mut copied = 0_u64;
    let mut buffer = [0_u8; 64 * 1_024];
    loop {
        let read = source
            .read(&mut buffer)
            .map_err(|_| BundleIngressError::Invalid)?;
        if read == 0 {
            break;
        }
        copied = copied
            .checked_add(read as u64)
            .ok_or(BundleIngressError::Invalid)?;
        if copied > copy.limit as u64 {
            return Err(BundleIngressError::Invalid);
        }
        hasher.update(&buffer[..read]);
        target
            .write_all(&buffer[..read])
            .map_err(|_| BundleIngressError::Unavailable)?;
    }
    if copied != initial_size
        || copy.expected_size.is_some_and(|size| size != copied)
        || encode_digest(hasher.finalize().as_ref()) != copy.expected_sha256
    {
        return Err(BundleIngressError::Invalid);
    }
    target
        .set_permissions(fs::Permissions::from_mode(copy.mode))
        .and_then(|()| target.sync_all())
        .map_err(|_| BundleIngressError::Unavailable)
}

fn create_destination_parents(
    pending_root: &Path,
    destination: &Path,
) -> Result<(), BundleIngressError> {
    let parent = destination.parent().ok_or(BundleIngressError::Invalid)?;
    if parent == pending_root {
        return Ok(());
    }
    let relative = parent
        .strip_prefix(pending_root)
        .map_err(|_| BundleIngressError::Invalid)?;
    let mut current = pending_root.to_path_buf();
    for component in relative.components() {
        let Component::Normal(component) = component else {
            return Err(BundleIngressError::Invalid);
        };
        current.push(component);
        match fs::create_dir(&current) {
            Ok(()) => fs::set_permissions(&current, fs::Permissions::from_mode(0o700))
                .map_err(|_| BundleIngressError::Unavailable)?,
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                let metadata =
                    fs::symlink_metadata(&current).map_err(|_| BundleIngressError::Unavailable)?;
                if metadata.file_type().is_symlink() || !metadata.is_dir() {
                    return Err(BundleIngressError::Unavailable);
                }
            }
            Err(_) => return Err(BundleIngressError::Unavailable),
        }
    }
    Ok(())
}

fn write_new_file(path: &Path, content: &[u8], mode: u32) -> Result<(), BundleIngressError> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true).mode(mode);
    let mut file = options
        .open(path)
        .map_err(|_| BundleIngressError::Unavailable)?;
    file.write_all(content)
        .and_then(|()| file.set_permissions(fs::Permissions::from_mode(mode)))
        .and_then(|()| file.sync_all())
        .map_err(|_| BundleIngressError::Unavailable)
}

fn create_pending_root(
    runtime_root: &Path,
    generation: RuntimeGeneration,
) -> Result<PathBuf, BundleIngressError> {
    for _ in 0..4 {
        let pending = runtime_root.join(format!(
            ".generation-{:020}-{}.pending",
            generation.0,
            uuid::Uuid::new_v4()
        ));
        match fs::create_dir(&pending) {
            Ok(()) => {
                if fs::set_permissions(&pending, fs::Permissions::from_mode(0o700)).is_err() {
                    let _ = fs::remove_dir(&pending);
                    return Err(BundleIngressError::Unavailable);
                }
                return Ok(pending);
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(_) => return Err(BundleIngressError::Unavailable),
        }
    }
    Err(BundleIngressError::Unavailable)
}

fn remove_pending_root(runtime_root: &Path, pending: &Path) -> Result<(), BundleIngressError> {
    if pending.parent() != Some(runtime_root)
        || !pending
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.starts_with(".generation-") && name.ends_with(".pending"))
    {
        return Err(BundleIngressError::Unavailable);
    }
    fs::remove_dir_all(pending).map_err(|_| BundleIngressError::Unavailable)
}

fn generation_directory_exists(path: &Path) -> Result<bool, BundleIngressError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if !metadata.file_type().is_symlink() && metadata.is_dir() => Ok(true),
        Ok(_) => Err(BundleIngressError::Unavailable),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(_) => Err(BundleIngressError::Unavailable),
    }
}

fn staged_bundle(bundle: &RuntimeBundle, generation_root: PathBuf) -> RuntimeBundle {
    RuntimeBundle {
        generation: bundle.generation,
        generation_root,
        manifest_sha256: bundle.manifest_sha256.clone(),
        compiler_policy_sha256: bundle.compiler_policy_sha256.clone(),
        mihomo_binary_sha256: bundle.mihomo_binary_sha256.clone(),
    }
}

fn valid_provider_path(value: &str) -> bool {
    let path = Path::new(value);
    !value.is_empty()
        && !path.is_absolute()
        && !matches!(value, "manifest.json" | "config.yaml" | "mihomo")
        && path
            .components()
            .all(|component| matches!(component, Component::Normal(_)))
}

fn valid_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
}

fn sha256_hex(content: &[u8]) -> String {
    encode_digest(Sha256::digest(content).as_ref())
}

fn encode_digest(digest: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(digest.len() * 2);
    for byte in digest {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    encoded
}

fn sync_tree_directories(root: &Path) -> Result<(), BundleIngressError> {
    let mut directories = vec![root.to_path_buf()];
    let mut pending = VecDeque::from([root.to_path_buf()]);
    while let Some(directory) = pending.pop_front() {
        for entry in fs::read_dir(&directory).map_err(|_| BundleIngressError::Unavailable)? {
            let entry = entry.map_err(|_| BundleIngressError::Unavailable)?;
            let file_type = entry
                .file_type()
                .map_err(|_| BundleIngressError::Unavailable)?;
            if file_type.is_symlink() {
                return Err(BundleIngressError::Unavailable);
            }
            if file_type.is_dir() {
                let path = entry.path();
                directories.push(path.clone());
                pending.push_back(path);
            }
        }
    }
    directories.sort_by_key(|path| std::cmp::Reverse(path.components().count()));
    for directory in directories {
        sync_directory(&directory).map_err(|_| BundleIngressError::Unavailable)?;
    }
    Ok(())
}

// -----------------------------------------------------------------------------
// Platform and error helpers
// -----------------------------------------------------------------------------

fn prepare_runtime_root(path: &Path) -> io::Result<PathBuf> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "Core service runtime root requires a parent directory",
            )
        })?;
    let parent_metadata = fs::symlink_metadata(parent)?;
    if parent_metadata.file_type().is_symlink()
        || !parent_metadata.is_dir()
        || parent_metadata.uid() != nix::unistd::geteuid().as_raw()
    {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "Core service runtime parent must be a real directory",
        ));
    }
    match fs::create_dir(path) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
        Err(error) => return Err(error),
    }
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink()
        || !metadata.is_dir()
        || metadata.uid() != nix::unistd::geteuid().as_raw()
    {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "Core service runtime root must be a real directory",
        ));
    }
    fs::set_permissions(path, fs::Permissions::from_mode(0o711))?;
    fs::canonicalize(path)
}

fn bind_service_listener(socket_path: &Path, allowed_owner_uid: u32) -> io::Result<UnixListener> {
    let parent = socket_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "Core service IPC socket requires a parent directory",
            )
        })?;
    prepare_service_socket_parent(parent)
        .map_err(|error| safe_io_error(error, "Core service IPC parent setup failed"))?;
    recover_stale_service_socket(socket_path, parent, allowed_owner_uid)
        .map_err(|error| safe_io_error(error, "Core service IPC stale socket check failed"))?;
    let listener = UnixListener::bind(socket_path)
        .map_err(|error| safe_io_error(error, "Core service IPC bind failed"))?;
    let configure_result = configure_service_socket(socket_path, parent, allowed_owner_uid)
        .map_err(|error| safe_io_error(error, "Core service IPC access setup failed"));
    if let Err(error) = configure_result {
        drop(listener);
        let _ = fs::remove_file(socket_path);
        return Err(error);
    }
    Ok(listener)
}

fn recover_stale_service_socket(
    socket_path: &Path,
    parent: &Path,
    allowed_owner_uid: u32,
) -> io::Result<()> {
    let metadata = match fs::symlink_metadata(socket_path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    };
    if !metadata.file_type().is_socket() {
        return Err(io::Error::new(
            io::ErrorKind::AddrInUse,
            "Core service IPC path is occupied",
        ));
    }
    if metadata.uid() != allowed_owner_uid || metadata.mode() & 0o777 != 0o600 {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "Core service IPC stale socket identity is invalid",
        ));
    }
    let identity = SocketIdentity {
        device: metadata.dev(),
        inode: metadata.ino(),
    };
    match UnixStream::connect(socket_path) {
        Ok(stream) => {
            drop(stream);
            Err(io::Error::new(
                io::ErrorKind::AddrInUse,
                "Core service IPC socket is active",
            ))
        }
        Err(error) if error.kind() == io::ErrorKind::ConnectionRefused => {
            cleanup_socket(socket_path, identity)?;
            sync_directory(parent)
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

fn prepare_service_socket_parent(parent: &Path) -> io::Result<()> {
    match fs::symlink_metadata(parent) {
        Ok(metadata) => validate_service_socket_parent(&metadata),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            let ancestor = parent
                .parent()
                .filter(|ancestor| !ancestor.as_os_str().is_empty())
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "Core service IPC socket parent is invalid",
                    )
                })?;
            let ancestor_metadata = fs::symlink_metadata(ancestor)?;
            validate_service_socket_parent(&ancestor_metadata)?;
            let mut builder = fs::DirBuilder::new();
            builder.mode(0o700);
            builder.create(parent)?;
            let metadata = fs::symlink_metadata(parent)?;
            validate_service_socket_parent(&metadata)
        }
        Err(error) => Err(error),
    }?;
    fs::set_permissions(parent, fs::Permissions::from_mode(0o711))?;
    let metadata = fs::symlink_metadata(parent)?;
    validate_service_socket_parent(&metadata)?;
    if metadata.mode() & 0o777 != 0o711 {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "Core service IPC parent access policy failed",
        ));
    }
    Ok(())
}

fn validate_service_socket_parent(metadata: &fs::Metadata) -> io::Result<()> {
    if metadata.file_type().is_symlink()
        || !metadata.is_dir()
        || metadata.uid() != nix::unistd::geteuid().as_raw()
    {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "Core service IPC parent ownership is invalid",
        ));
    }
    Ok(())
}

fn configure_service_socket(
    socket_path: &Path,
    parent: &Path,
    allowed_owner_uid: u32,
) -> io::Result<()> {
    fs::set_permissions(socket_path, fs::Permissions::from_mode(0o600))?;
    let metadata = fs::symlink_metadata(socket_path)?;
    if metadata.uid() != allowed_owner_uid {
        chown(socket_path, Some(allowed_owner_uid), None)?;
    }
    fs::set_permissions(socket_path, fs::Permissions::from_mode(0o600))?;
    fs::set_permissions(parent, fs::Permissions::from_mode(0o711))?;
    let metadata = fs::symlink_metadata(socket_path)?;
    if !metadata.file_type().is_socket()
        || metadata.uid() != allowed_owner_uid
        || metadata.mode() & 0o777 != 0o600
    {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "Core service IPC socket access policy failed",
        ));
    }
    Ok(())
}

fn sync_directory(path: &Path) -> io::Result<()> {
    File::open(path)?.sync_all()
}

fn cleanup_socket(path: &Path, identity: SocketIdentity) -> io::Result<()> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    };
    if !metadata.file_type().is_socket()
        || metadata.dev() != identity.device
        || metadata.ino() != identity.inode
    {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "Core service IPC socket identity changed",
        ));
    }
    fs::remove_file(path)
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

fn transport_unavailable(_error: io::Error) -> CoreRuntimeError {
    unavailable_error("Core service IPC endpoint is unavailable")
}

fn map_write_error(error: FrameError) -> CoreRuntimeError {
    match error {
        FrameError::Io(error) if is_timeout(&error) => {
            unavailable_error("Core service IPC request timed out")
        }
        FrameError::Io(_) => unavailable_error("Core service IPC request failed"),
        FrameError::Json(_) | FrameError::FrameTooLarge { .. } => {
            protocol_error("Core service IPC request encoding failed")
        }
    }
}

fn map_read_error(error: FrameError) -> CoreRuntimeError {
    match error {
        FrameError::Io(error) if is_timeout(&error) => {
            unavailable_error("Core service IPC response timed out")
        }
        FrameError::Io(error)
            if matches!(
                error.kind(),
                io::ErrorKind::UnexpectedEof
                    | io::ErrorKind::ConnectionReset
                    | io::ErrorKind::BrokenPipe
            ) =>
        {
            unavailable_error("Core service IPC connection closed")
        }
        FrameError::Io(_) => unavailable_error("Core service IPC response failed"),
        FrameError::Json(_) | FrameError::FrameTooLarge { .. } => {
            protocol_error("Core service IPC response is invalid")
        }
    }
}

fn is_timeout(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock
    )
}

fn authentication_error(diagnostic: &'static str) -> CoreRuntimeError {
    CoreRuntimeError::new(CoreRuntimeErrorKind::Authentication, diagnostic)
}

fn protocol_error(diagnostic: &'static str) -> CoreRuntimeError {
    CoreRuntimeError::new(CoreRuntimeErrorKind::ProtocolMismatch, diagnostic)
}

fn unavailable_error(diagnostic: &'static str) -> CoreRuntimeError {
    CoreRuntimeError::new(CoreRuntimeErrorKind::Unavailable, diagnostic)
}

fn unexpected_response() -> CoreRuntimeError {
    protocol_error("Core service IPC response operation mismatch")
}

fn safe_io_error(error: io::Error, message: &'static str) -> io::Error {
    io::Error::new(error.kind(), message)
}

#[cfg(test)]
mod accept_loop_tests {
    use super::*;

    #[test]
    fn idle_poll_blocks_without_periodic_wakes_and_shutdown_bypasses_the_worker_queue() {
        let root = Path::new("/private/tmp").join(format!(
            "hcs-accept-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4()
        ));
        fs::create_dir(&root).expect("the accept fixture root should be created");
        let socket_path = root.join("core.sock");
        let listener =
            UnixListener::bind(&socket_path).expect("the idle accept fixture should bind");
        let (listener, mut poll, waker) =
            prepare_accept_loop(listener).expect("the idle accept loop should prepare");
        let (sender, receiver) = mpsc::sync_channel(1);
        let shutdown = Arc::new(AtomicBool::new(false));
        let thread_shutdown = Arc::clone(&shutdown);
        let metrics = CoreServiceAcceptMetrics::default();
        let thread_metrics = metrics.clone();
        let worker = thread::spawn(move || {
            accept_loop(
                &listener,
                &mut poll,
                &sender,
                nix::unistd::geteuid().as_raw(),
                &thread_shutdown,
                &thread_metrics,
            )
        });
        thread::sleep(Duration::from_millis(75));

        assert_eq!(metrics.poll_returns(), 0);
        shutdown.store(true, Ordering::Release);
        waker
            .wake()
            .expect("the private shutdown waker should fire");
        worker
            .join()
            .expect("the accept thread should finish")
            .expect("the accept loop should stop cleanly");
        assert_eq!(metrics.poll_returns(), 1);
        assert!(receiver.try_recv().is_err());

        fs::remove_dir_all(root).expect("the accept fixture root should be removed");
    }

    #[test]
    fn private_waker_stops_accept_after_path_replacement_and_preserves_the_replacement() {
        let root = Path::new("/private/tmp").join(format!(
            "hcs-wake-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4()
        ));
        fs::create_dir(&root).expect("the wake fixture root should be created");
        let socket_path = root.join("core.sock");
        let original =
            UnixListener::bind(&socket_path).expect("the original wake socket should bind");
        let metadata = fs::symlink_metadata(&socket_path)
            .expect("the original wake socket metadata should load");
        let identity = SocketIdentity {
            device: metadata.dev(),
            inode: metadata.ino(),
        };
        let (listener, mut poll, waker) =
            prepare_accept_loop(original).expect("the original accept loop should prepare");
        let (sender, receiver) = mpsc::sync_channel(1);
        let shutdown = Arc::new(AtomicBool::new(false));
        let thread_shutdown = Arc::clone(&shutdown);
        let metrics = CoreServiceAcceptMetrics::default();
        let thread_metrics = metrics.clone();
        let worker = thread::spawn(move || {
            accept_loop(
                &listener,
                &mut poll,
                &sender,
                nix::unistd::geteuid().as_raw(),
                &thread_shutdown,
                &thread_metrics,
            )
        });
        thread::sleep(Duration::from_millis(25));
        let mut server = CoreServiceServer {
            socket_path: socket_path.clone(),
            socket_identity: identity,
            shutdown,
            waker,
            thread: Some(worker),
        };
        fs::remove_file(&socket_path).expect("the original wake socket should be removed");
        let replacement =
            UnixListener::bind(&socket_path).expect("the replacement wake socket should bind");
        replacement
            .set_nonblocking(true)
            .expect("the replacement listener should be observable");

        let started = std::time::Instant::now();
        let error = server
            .shutdown()
            .expect_err("shutdown cleanup should preserve the replacement socket");

        assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
        assert!(started.elapsed() < Duration::from_millis(250));
        assert!(server.thread.is_none());
        assert_eq!(metrics.poll_returns(), 1);
        assert!(receiver.try_recv().is_err());
        assert!(matches!(
            replacement.accept(),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock
        ));
        drop(replacement);
        fs::remove_dir_all(root).expect("the wake fixture root should be removed");
    }
}
