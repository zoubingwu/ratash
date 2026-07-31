//! Dedicated authenticated shutdown channel for Supervisor lifecycle control.

use std::fmt;
use std::fs;
use std::io;
use std::net::Shutdown;
use std::os::unix::fs::{FileTypeExt, MetadataExt};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender, TrySendError};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use socket2::{Domain, SockAddr, Socket, Type};

use crate::daemon::{ShutdownAcknowledgement, ShutdownIntent};
use crate::ipc::{
    IPC_PROTOCOL_VERSION, PeerAuthorizer, RequestId, bind_private_listener, read_frame, write_frame,
};
use crate::unix_io::DeadlineUnixStream;

const CONTROL_WORKERS: usize = 2;
const PENDING_CONNECTIONS: usize = 8;
const WAKE_CONNECT_TIMEOUT: Duration = Duration::from_millis(250);

pub trait ShutdownControlHandler: Send + Sync {
    fn request_shutdown(
        &self,
        intent: &ShutdownIntent,
    ) -> Result<ShutdownAcknowledgement, ShutdownControlError>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShutdownControlError {
    Rejected,
    Internal,
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct ShutdownControlRequest<'a> {
    protocol_version: u16,
    request_id: RequestId,
    intent: &'a ShutdownIntent,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct OwnedShutdownControlRequest {
    protocol_version: u16,
    request_id: RequestId,
    intent: ShutdownIntent,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ShutdownControlResponse {
    protocol_version: u16,
    request_id: RequestId,
    #[serde(flatten)]
    outcome: ShutdownControlOutcome,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum ShutdownControlOutcome {
    Acknowledged {
        acknowledgement: ShutdownAcknowledgement,
    },
    Rejected {
        message: String,
    },
}

#[derive(Clone, Copy)]
struct SocketIdentity {
    device: u64,
    inode: u64,
}

pub struct ShutdownIpcServer {
    socket_path: PathBuf,
    socket_identity: SocketIdentity,
    shutdown: Arc<AtomicBool>,
    thread: Option<JoinHandle<io::Result<()>>>,
}

impl ShutdownIpcServer {
    pub fn start<H, P>(
        socket_path: impl AsRef<Path>,
        handler: Arc<H>,
        authorizer: Arc<P>,
        io_timeout: Duration,
    ) -> io::Result<Self>
    where
        H: ShutdownControlHandler + 'static,
        P: PeerAuthorizer + 'static,
    {
        if io_timeout.is_zero() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "shutdown IPC deadline must be positive",
            ));
        }
        let socket_path = socket_path.as_ref().to_path_buf();
        let listener = bind_private_listener(&socket_path)?;
        let metadata = fs::symlink_metadata(&socket_path)?;
        let socket_identity = SocketIdentity {
            device: metadata.dev(),
            inode: metadata.ino(),
        };
        let shutdown = Arc::new(AtomicBool::new(false));
        let thread_shutdown = Arc::clone(&shutdown);
        let handler: Arc<dyn ShutdownControlHandler> = handler;
        let authorizer: Arc<dyn PeerAuthorizer> = authorizer;
        let thread = match thread::Builder::new()
            .name("hopash-shutdown-ipc".to_owned())
            .spawn(move || run_server(listener, handler, authorizer, io_timeout, thread_shutdown))
        {
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
            thread: Some(thread),
        })
    }

    pub fn shutdown(&mut self) -> io::Result<()> {
        self.shutdown.store(true, Ordering::Release);
        let wake_result = match self.thread.as_ref() {
            Some(thread) if !thread.is_finished() => {
                wake_accept_loop(&self.socket_path, self.socket_identity)
            }
            Some(_) | None => Ok(()),
        };
        let thread_result = self.thread.take().map_or(Ok(()), |thread| {
            thread
                .join()
                .map_err(|_| io::Error::other("shutdown IPC server thread panicked"))?
        });
        wake_result
            .and(thread_result)
            .and(cleanup_socket(&self.socket_path, self.socket_identity))
    }
}

impl Drop for ShutdownIpcServer {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}

impl fmt::Debug for ShutdownIpcServer {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ShutdownIpcServer")
            .field("socket_path", &"[REDACTED]")
            .field("running", &self.thread.is_some())
            .finish()
    }
}

pub fn request_shutdown(
    socket_path: impl AsRef<Path>,
    intent: &ShutdownIntent,
    timeout: Duration,
) -> io::Result<ShutdownAcknowledgement> {
    if timeout.is_zero() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "shutdown IPC deadline must be positive",
        ));
    }
    let request_id = next_request_id();
    let request = ShutdownControlRequest {
        protocol_version: IPC_PROTOCOL_VERSION,
        request_id,
        intent,
    };
    let socket = Socket::new(Domain::UNIX, Type::STREAM, None)?;
    socket.connect_timeout(&SockAddr::unix(socket_path.as_ref())?, timeout)?;
    let mut stream = DeadlineUnixStream::new(UnixStream::from(socket), timeout)?;
    stream.begin_write()?;
    write_frame(&mut stream, &request).map_err(frame_error)?;
    stream.begin_read()?;
    let response: ShutdownControlResponse = read_frame(&mut stream).map_err(frame_error)?;
    if response.protocol_version != IPC_PROTOCOL_VERSION || response.request_id != request_id {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "shutdown IPC response correlation failed",
        ));
    }
    match response.outcome {
        ShutdownControlOutcome::Acknowledged { acknowledgement } => Ok(acknowledgement),
        ShutdownControlOutcome::Rejected { .. } => Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "the Supervisor rejected the shutdown identity",
        )),
    }
}

fn next_request_id() -> RequestId {
    static NEXT_REQUEST_ID: AtomicU64 = AtomicU64::new(1);
    let id = NEXT_REQUEST_ID.fetch_add(1, Ordering::Relaxed);
    RequestId(if id == 0 {
        NEXT_REQUEST_ID.fetch_add(1, Ordering::Relaxed)
    } else {
        id
    })
}

fn run_server(
    listener: UnixListener,
    handler: Arc<dyn ShutdownControlHandler>,
    authorizer: Arc<dyn PeerAuthorizer>,
    io_timeout: Duration,
    shutdown: Arc<AtomicBool>,
) -> io::Result<()> {
    let (sender, receiver) = mpsc::sync_channel(PENDING_CONNECTIONS);
    let receiver = Arc::new(Mutex::new(receiver));
    let workers = spawn_workers(
        Arc::clone(&receiver),
        handler,
        io_timeout,
        Arc::clone(&shutdown),
    )?;
    let accept_result = accept_loop(&listener, &authorizer, &sender, &shutdown);
    drop(sender);
    let mut panicked = false;
    for worker in workers {
        panicked |= worker.join().is_err();
    }
    if panicked && accept_result.is_ok() {
        Err(io::Error::other("shutdown IPC worker thread panicked"))
    } else {
        accept_result
    }
}

fn spawn_workers(
    receiver: Arc<Mutex<Receiver<UnixStream>>>,
    handler: Arc<dyn ShutdownControlHandler>,
    io_timeout: Duration,
    shutdown: Arc<AtomicBool>,
) -> io::Result<Vec<JoinHandle<()>>> {
    (0..CONTROL_WORKERS)
        .map(|index| {
            let receiver = Arc::clone(&receiver);
            let handler = Arc::clone(&handler);
            let shutdown = Arc::clone(&shutdown);
            thread::Builder::new()
                .name(format!("hopash-shutdown-worker-{index}"))
                .spawn(move || worker_loop(receiver, handler, io_timeout, shutdown))
        })
        .collect()
}

fn worker_loop(
    receiver: Arc<Mutex<Receiver<UnixStream>>>,
    handler: Arc<dyn ShutdownControlHandler>,
    io_timeout: Duration,
    shutdown: Arc<AtomicBool>,
) {
    loop {
        let received = receiver
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .recv();
        match received {
            Ok(stream) if !shutdown.load(Ordering::Acquire) => {
                handle_connection(stream, handler.as_ref(), io_timeout)
            }
            Ok(_) | Err(_) => return,
        }
    }
}

fn accept_loop(
    listener: &UnixListener,
    authorizer: &Arc<dyn PeerAuthorizer>,
    sender: &SyncSender<UnixStream>,
    shutdown: &AtomicBool,
) -> io::Result<()> {
    loop {
        match listener.accept() {
            Ok((stream, _)) => {
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
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
}

fn handle_connection(
    stream: UnixStream,
    handler: &dyn ShutdownControlHandler,
    io_timeout: Duration,
) {
    let mut stream = match DeadlineUnixStream::new(stream, io_timeout) {
        Ok(stream) => stream,
        Err(_) => return,
    };
    if stream.begin_read().is_err() {
        return;
    }
    let request: OwnedShutdownControlRequest = match read_frame(&mut stream) {
        Ok(request) => request,
        Err(_) => return,
    };
    let outcome = if request.protocol_version != IPC_PROTOCOL_VERSION
        || request.intent.protocol_version != IPC_PROTOCOL_VERSION
        || request.request_id.0 == 0
    {
        ShutdownControlOutcome::Rejected {
            message: "The shutdown request protocol is invalid".to_owned(),
        }
    } else {
        match handler.request_shutdown(&request.intent) {
            Ok(acknowledgement) => ShutdownControlOutcome::Acknowledged { acknowledgement },
            Err(ShutdownControlError::Rejected) => ShutdownControlOutcome::Rejected {
                message: "The shutdown identity is invalid".to_owned(),
            },
            Err(ShutdownControlError::Internal) => ShutdownControlOutcome::Rejected {
                message: "The shutdown request could not be accepted".to_owned(),
            },
        }
    };
    let response = ShutdownControlResponse {
        protocol_version: IPC_PROTOCOL_VERSION,
        request_id: request.request_id,
        outcome,
    };
    if stream.begin_write().is_ok() {
        let _ = write_frame(&mut stream, &response);
    }
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
            "shutdown IPC socket identity changed",
        ));
    }
    fs::remove_file(path)
}

fn wake_accept_loop(path: &Path, identity: SocketIdentity) -> io::Result<()> {
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.file_type().is_socket()
        || metadata.dev() != identity.device
        || metadata.ino() != identity.inode
    {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "shutdown IPC socket identity changed",
        ));
    }
    let socket = Socket::new(Domain::UNIX, Type::STREAM, None)?;
    socket.connect_timeout(&SockAddr::unix(path)?, WAKE_CONNECT_TIMEOUT)?;
    let stream = UnixStream::from(socket);
    stream.shutdown(Shutdown::Both)
}

fn frame_error(error: crate::ipc::FrameError) -> io::Error {
    match error {
        crate::ipc::FrameError::Io(error) => error,
        crate::ipc::FrameError::Json(_) | crate::ipc::FrameError::FrameTooLarge { .. } => {
            io::Error::new(io::ErrorKind::InvalidData, "shutdown IPC frame is invalid")
        }
    }
}
