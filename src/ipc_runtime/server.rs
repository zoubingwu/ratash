//! Same-user authorization and bounded Unix socket server runtime.

use std::collections::BTreeMap;
use std::fmt;
use std::fs;
use std::io;
use std::net::Shutdown;
use std::os::unix::fs::MetadataExt;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender, TrySendError};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use mio::net::UnixListener as MioUnixListener;
use mio::{Events, Interest, Poll, Token, Waker};

use crate::application::{ApplicationClient, ApplicationError};
use crate::constants::{IPC_REQUEST_FRAME_MAX_BYTES, IPC_REQUEST_TIMEOUT, IPC_STREAM_CAPACITY};
use crate::error::ErrorCode;
use crate::ipc::{
    IpcError, IpcRequest, IpcResponse, IpcStreamFrame, IpcStreamPayload, LogStreamItem,
    LogSubscriptionPayload, LogTailPayload, OperationConversionError, PeerAuthorizationError,
    PeerAuthorizer, RequestId, RequestOperation, StatusStreamItem, bind_private_listener,
    read_frame_with_limit, write_frame,
};
use crate::unix_io::DeadlineUnixStream;

use super::stream::IpcStreamBroker;
use super::wire::encode_application_output;

const DEFAULT_SERVER_WORKERS: usize = IPC_STREAM_CAPACITY + 1;
const DEFAULT_PENDING_CONNECTIONS: usize = 32;
const LISTENER_TOKEN: Token = Token(0);
const SHUTDOWN_TOKEN: Token = Token(1);

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
pub(super) struct AcceptLoopMetrics {
    #[cfg(test)]
    poll_returns: Arc<AtomicUsize>,
}

impl AcceptLoopMetrics {
    fn record_poll_return(&self) {
        #[cfg(test)]
        self.poll_returns.fetch_add(1, Ordering::Relaxed);
    }

    #[cfg(test)]
    pub(super) fn poll_returns(&self) -> usize {
        self.poll_returns.load(Ordering::Relaxed)
    }
}

#[derive(Default)]
struct ActiveConnections {
    cancelled: AtomicBool,
    next_id: AtomicU64,
    streams: Mutex<BTreeMap<u64, UnixStream>>,
}

impl ActiveConnections {
    fn request_id(&self) -> u64 {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        if id == 0 {
            self.next_id.fetch_add(1, Ordering::Relaxed)
        } else {
            id
        }
    }

    fn cancel_all(&self) {
        self.cancelled.store(true, Ordering::Release);
        let mut streams = self
            .streams
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for stream in streams.values() {
            let _ = stream.shutdown(Shutdown::Both);
        }
        streams.clear();
    }
}

struct ActiveConnection {
    connections: Arc<ActiveConnections>,
    id: u64,
}

impl ActiveConnection {
    fn register(connections: &Arc<ActiveConnections>, stream: &UnixStream) -> io::Result<Self> {
        let id = connections.request_id();
        let cancellation_stream = stream.try_clone()?;
        let mut streams = connections
            .streams
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        streams.insert(id, cancellation_stream);
        if connections.cancelled.load(Ordering::Acquire)
            && let Some(stream) = streams.remove(&id)
        {
            let _ = stream.shutdown(Shutdown::Both);
        }
        Ok(Self {
            connections: Arc::clone(connections),
            id,
        })
    }
}

impl Drop for ActiveConnection {
    fn drop(&mut self) {
        if let Ok(mut streams) = self.connections.streams.lock() {
            streams.remove(&self.id);
        }
    }
}

pub struct IpcServer {
    socket_path: PathBuf,
    socket_identity: SocketIdentity,
    shutdown: Arc<AtomicBool>,
    waker: Arc<Waker>,
    streams: Option<Arc<IpcStreamBroker>>,
    active_connections: Arc<ActiveConnections>,
    thread: Option<JoinHandle<io::Result<()>>>,
    #[cfg(test)]
    pub(super) accept_metrics: AcceptLoopMetrics,
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
        let active_connections = Arc::new(ActiveConnections::default());
        let thread_connections = Arc::clone(&active_connections);
        let application: Arc<dyn ApplicationClient + Send + Sync> = application;
        let authorizer: Arc<dyn PeerAuthorizer> = authorizer;
        let thread_streams = streams.clone();
        let accept_metrics = AcceptLoopMetrics::default();
        #[cfg(test)]
        let observed_accept_metrics = accept_metrics.clone();
        let thread_accept_metrics = accept_metrics;
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
                        active_connections: thread_connections,
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
            active_connections,
            thread: Some(thread),
            #[cfg(test)]
            accept_metrics: observed_accept_metrics,
        })
    }

    pub fn shutdown(&mut self) -> io::Result<()> {
        self.shutdown_inner(None)
    }

    pub fn shutdown_until(&mut self, deadline: Instant) -> io::Result<()> {
        self.shutdown_inner(Some(deadline))
    }

    fn shutdown_inner(&mut self, deadline: Option<Instant>) -> io::Result<()> {
        self.shutdown.store(true, Ordering::Release);
        self.active_connections.cancel_all();
        if let Some(streams) = &self.streams {
            streams.notify_all();
        }
        let wake_result = match self.thread.as_ref() {
            Some(thread) if !thread.is_finished() => self.waker.wake(),
            Some(_) | None => Ok(()),
        };
        let thread_result = self.thread.take().map_or(Ok(()), |thread| {
            if deadline.is_some_and(|deadline| !wait_until_finished(&thread, deadline)) {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "The IPC server exceeded the Supervisor shutdown deadline",
                ));
            }
            thread
                .join()
                .map_err(|_| io::Error::other("IPC server thread panicked"))?
        });
        let cleanup_result = cleanup_socket(&self.socket_path, self.socket_identity);
        wake_result.and(thread_result).and(cleanup_result)
    }
}

fn wait_until_finished<T>(thread: &JoinHandle<T>, deadline: Instant) -> bool {
    while !thread.is_finished() {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return false;
        }
        thread::sleep(remaining.min(Duration::from_millis(1)));
    }
    true
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
    active_connections: Arc<ActiveConnections>,
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
        active_connections,
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
        active_connections,
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
    active_connections: Arc<ActiveConnections>,
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
                let Ok(_active_connection) =
                    ActiveConnection::register(&context.active_connections, &stream)
                else {
                    let _ = stream.shutdown(Shutdown::Both);
                    continue;
                };
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
    let request =
        match read_frame_with_limit::<_, IpcRequest>(&mut stream, IPC_REQUEST_FRAME_MAX_BYTES) {
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
        Ok(output) => match encode_application_output(output) {
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

fn stream_initialization_response(request_id: RequestId) -> IpcResponse {
    IpcResponse::failure(
        request_id,
        IpcError::new(
            ErrorCode::Internal,
            "The IPC stream subscriber could not be initialized",
            false,
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
    let Ok((initial, subscription)) = streams.subscribe_status() else {
        write_response(stream, &stream_initialization_response(request_id));
        return;
    };
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
    let Ok(subscription) = streams.subscribe_logs(after_sequence) else {
        write_response(stream, &stream_initialization_response(request_id));
        return;
    };
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
        OperationConversionError::InvalidListPageOffset => IpcError::new(
            ErrorCode::ProtocolMismatch,
            "The list page offset is invalid",
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
