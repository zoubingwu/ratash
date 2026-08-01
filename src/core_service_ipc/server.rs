//! Bounded privileged CoreRuntime Unix socket server.

use std::fmt;
use std::fs;
use std::io;
use std::net::Shutdown;
use std::os::unix::fs::MetadataExt;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender, TrySendError};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use mio::net::UnixListener as MioUnixListener;
use mio::{Events, Interest, Poll, Token, Waker};

use crate::constants::{
    CORE_LOG_LINE_MAX_BYTES, CORE_SERVICE_REQUEST_TIMEOUT, IPC_FRAME_MAX_BYTES,
};
use crate::core::{CoreRuntime, CoreRuntimeError, CoreRuntimeErrorKind, OwnerSessionProof};
use crate::domain::RuntimeGeneration;
use crate::geodata::GeoDataCatalog;
use crate::ipc::{read_frame, write_frame};
use crate::runtime_bundle::{
    RuntimeGenerationRetention, inspect_runtime_generations_with_reserved,
    prune_runtime_generations_with_reserved,
};
use crate::unix_io::DeadlineUnixStream;

use super::CORE_SERVICE_IPC_PROTOCOL_VERSION;
use super::authorization::{
    AcceptPeerIdentity, CoreServicePeerAuthorizer, CoreServicePeerIdentity, peer_identity,
};
use super::error::{authentication_error, protocol_error, safe_io_error, unavailable_error};
use super::ingress::{BundleIngressError, GeoDataIngress, stage_runtime_bundle};
use super::socket::{SocketIdentity, bind_service_listener, cleanup_socket, prepare_runtime_root};
use super::wire::{WireEmpty, WireOperation, WireRequest, WireResponse, WireSuccess};

#[cfg(test)]
use super::client::CoreServiceClient;
#[cfg(test)]
use crate::core::{CoreControlEndpoint, OwnerSession, OwnerSessionRequest};
#[cfg(test)]
use std::sync::atomic::AtomicU64;

const DEFAULT_SERVER_WORKERS: usize = 4;
const DEFAULT_PENDING_CONNECTIONS: usize = 32;
const CORE_SERVICE_LOG_BATCH_MAX: usize = IPC_FRAME_MAX_BYTES / CORE_LOG_LINE_MAX_BYTES / 2;
const CORE_SERVICE_LISTENER_TOKEN: Token = Token(0);
const CORE_SERVICE_SHUTDOWN_TOKEN: Token = Token(1);

const fn runtime_apply_is_indeterminate(kind: CoreRuntimeErrorKind) -> bool {
    matches!(
        kind,
        CoreRuntimeErrorKind::ReloadTimeout | CoreRuntimeErrorKind::Unavailable
    )
}

pub struct CoreServiceServerConfig {
    pub runtime_staging_root: PathBuf,
    pub allowed_owner_uid: u32,
    pub io_timeout: Duration,
    pub worker_count: usize,
    pub pending_connection_capacity: usize,
    installed_geo_data: Option<GeoDataIngress>,
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
            installed_geo_data: None,
        }
    }

    #[must_use]
    pub fn with_installed_geo_data(
        mut self,
        root: impl Into<PathBuf>,
        expected_owner_uid: u32,
        catalog: GeoDataCatalog,
    ) -> Self {
        self.installed_geo_data = Some(GeoDataIngress::new(
            root.into(),
            expected_owner_uid,
            catalog,
        ));
        self
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
            .field("installed_geo_data", &self.installed_geo_data.is_some())
            .finish()
    }
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
        Self::start_with_peer_authorizer(socket_path, runtime, config, Arc::new(AcceptPeerIdentity))
    }

    pub fn start_with_peer_authorizer<R>(
        socket_path: impl AsRef<Path>,
        runtime: Arc<R>,
        config: CoreServiceServerConfig,
        peer_authorizer: Arc<dyn CoreServicePeerAuthorizer>,
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
            installed_geo_data: config.installed_geo_data.clone(),
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
                    peer_authorizer,
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
    installed_geo_data: Option<GeoDataIngress>,
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
    peer: CoreServicePeerIdentity,
    session_id: String,
}

struct AcceptedConnection {
    stream: UnixStream,
    peer: CoreServicePeerIdentity,
}

fn validate_server_config(config: &CoreServiceServerConfig) -> io::Result<()> {
    if !config.runtime_staging_root.is_absolute()
        || config
            .installed_geo_data
            .as_ref()
            .is_some_and(|geo_data| !geo_data.root().is_absolute())
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
    peer_authorizer: Arc<dyn CoreServicePeerAuthorizer>,
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
        peer_authorizer.as_ref(),
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
    peer_authorizer: &dyn CoreServicePeerAuthorizer,
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
            accept_ready_connections(
                listener,
                sender,
                allowed_owner_uid,
                peer_authorizer,
                shutdown,
            )?;
        }
    }
}

fn accept_ready_connections(
    listener: &MioUnixListener,
    sender: &SyncSender<AcceptedConnection>,
    allowed_owner_uid: u32,
    peer_authorizer: &dyn CoreServicePeerAuthorizer,
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
                let peer = match peer_identity(&stream) {
                    Ok(peer) if peer.uid() == allowed_owner_uid => peer,
                    Err(_) => {
                        let _ = stream.shutdown(Shutdown::Both);
                        continue;
                    }
                    Ok(_) => {
                        let _ = stream.shutdown(Shutdown::Both);
                        continue;
                    }
                };
                if peer_authorizer.authorize(&peer).is_err() {
                    let _ = stream.shutdown(Shutdown::Both);
                    continue;
                }
                let connection = AcceptedConnection { stream, peer };
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
    let response = match dispatch(context, &connection.peer, request.operation) {
        Ok(success) => WireResponse::success(request.request_id, success),
        Err(error) => WireResponse::failure(request.request_id, error.kind),
    };
    write_response(&mut stream, response);
}

fn dispatch(
    context: &ServerContext,
    peer: &CoreServicePeerIdentity,
    operation: WireOperation,
) -> Result<WireSuccess, CoreRuntimeError> {
    match operation {
        WireOperation::OpenOwnerSession(request) => {
            let request = request.into_core();
            if peer.uid() != request.owner_uid || peer.pid() != request.supervisor_pid {
                return Err(authentication_error(
                    "Core service peer process identity mismatch",
                ));
            }
            let mut binding = context
                .session
                .lock()
                .map_err(|_| unavailable_error("Core service session state is unavailable"))?;
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
                peer: peer.clone(),
                session_id: session.proof.session_id().to_owned(),
            });
            Ok(WireSuccess::OwnerSession((&session).into()))
        }
        WireOperation::ApplyCandidate(request) => {
            let owner = request.owner.into_core();
            authorize_context_session(context, peer, &owner)?;
            let bundle = request.bundle.into_core();
            let mut retention = context.runtime_retention.lock().map_err(|_| {
                unavailable_error("Core service Runtime Generation state is unavailable")
            })?;
            let planned = retention
                .plan(bundle.generation)
                .map_err(|error| error.into_core())?;
            let staged = stage_runtime_bundle(
                &context.runtime_staging_root,
                peer.uid(),
                &bundle,
                context.installed_geo_data.as_ref(),
            )?;
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
            authorize_context_session(context, peer, &owner)?;
            context
                .runtime
                .status(&owner)
                .map(|status| WireSuccess::Status((&status).into()))
        }
        WireOperation::Logs(request) => {
            let owner = request.owner.into_core();
            authorize_context_session(context, peer, &owner)?;
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
            authorize_context_session(context, peer, &owner)?;
            context
                .runtime
                .stop(&owner)
                .map(|result| WireSuccess::Stop((&result).into()))
        }
        WireOperation::CloseOwnerSession(request) => {
            let owner = request.owner.into_core();
            authorize_context_session(context, peer, &owner)?;
            context.runtime.close_owner_session(&owner)?;
            let mut binding = context
                .session
                .lock()
                .map_err(|_| unavailable_error("Core service session state is unavailable"))?;
            authorize_bound_session(binding.as_ref(), peer, &owner)?;
            *binding = None;
            Ok(WireSuccess::CloseOwnerSession(WireEmpty {}))
        }
        WireOperation::CancelPendingApply(request) => {
            let owner = request.owner.into_core();
            authorize_context_session(context, peer, &owner)?;
            context.runtime.cancel_pending_apply(&owner)?;
            Ok(WireSuccess::CancelPendingApply(WireEmpty {}))
        }
    }
}

fn authorize_context_session(
    context: &ServerContext,
    peer: &CoreServicePeerIdentity,
    proof: &OwnerSessionProof,
) -> Result<(), CoreRuntimeError> {
    let binding = context
        .session
        .lock()
        .map_err(|_| unavailable_error("Core service session state is unavailable"))?;
    authorize_bound_session(binding.as_ref(), peer, proof)
}

fn authorize_bound_session(
    binding: Option<&BoundSession>,
    peer: &CoreServicePeerIdentity,
    proof: &OwnerSessionProof,
) -> Result<(), CoreRuntimeError> {
    if binding
        .is_some_and(|binding| binding.peer == *peer && binding.session_id == proof.session_id())
    {
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

#[cfg(test)]
mod accept_loop_tests {
    use super::*;
    use crate::service::CORE_RUNTIME_PROTOCOL_VERSION;

    #[test]
    fn client_timeout_is_shared_across_request_write_and_response_read() {
        let root = Path::new("/private/tmp").join(format!(
            "hcs-deadline-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4()
        ));
        fs::create_dir(&root).expect("the deadline fixture root should be created");
        let socket_path = root.join("core.sock");
        let listener =
            UnixListener::bind(&socket_path).expect("the deadline fixture listener should bind");
        let server = thread::spawn(move || {
            let (mut stream, _) = listener
                .accept()
                .expect("the deadline fixture should accept the client");
            thread::sleep(Duration::from_millis(250));
            let request: WireRequest =
                read_frame(&mut stream).expect("the deadline fixture should read the request");
            thread::sleep(Duration::from_millis(750));
            let session = OwnerSession {
                proof: OwnerSessionProof::new("fixture-session", "fixture-token"),
                protocol_version: CORE_RUNTIME_PROTOCOL_VERSION,
                owner_generation: 1,
                endpoint: CoreControlEndpoint::new(
                    PathBuf::from("/private/tmp/hopash-deadline-core.sock"),
                    "fixture-secret",
                ),
            };
            let response = WireResponse::success(
                request.request_id,
                WireSuccess::OwnerSession((&session).into()),
            );
            let _ = write_frame(&mut stream, &response);
        });
        let timeout = Duration::from_millis(750);
        let client = CoreServiceClient::with_service_uid_and_timeouts(
            &socket_path,
            nix::unistd::geteuid().as_raw(),
            timeout,
            timeout,
        );
        let request = OwnerSessionRequest {
            owner_uid: nix::unistd::geteuid().as_raw(),
            supervisor_pid: std::process::id(),
            supervisor_start_identity: "fixture-process".to_owned(),
            instance_token: "x".repeat(512 * 1024),
            protocol_version: CORE_RUNTIME_PROTOCOL_VERSION,
        };

        let error = client
            .open_owner_session(&request)
            .expect_err("one absolute deadline should expire across write and read");

        assert_eq!(error.kind, CoreRuntimeErrorKind::Unavailable);
        server
            .join()
            .expect("the deadline fixture server should finish");
        fs::remove_dir_all(root).expect("the deadline fixture root should be removed");
    }

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
                &AcceptPeerIdentity,
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
                &AcceptPeerIdentity,
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
