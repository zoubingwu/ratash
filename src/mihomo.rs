use std::fmt;
use std::io::{self, Read, Write};
use std::net::Shutdown;
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use mio::net::UnixStream as MioUnixStream;
use mio::{Events, Interest, Poll, Token};
use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};
use serde::Serialize;
use tungstenite::handshake::{client::generate_key, derive_accept_key};
use tungstenite::protocol::{Message, Role, WebSocket, WebSocketConfig};
use url::form_urlencoded;

use crate::core::{
    ConnectionSummary, CoreControlEndpoint, CoreEvent, CoreEventStream, DelayProbeRequest,
    DelayProbeResult, DelayTarget, MihomoAdapter, MihomoError, MihomoErrorKind, MihomoJsonCodec,
    MihomoLogFrame, MihomoReadiness, MihomoVersion, NodeSelection, ProjectionError, ProxyView,
    TrafficFrame, project_proxy_view,
};
use crate::domain::{CoreInstanceGeneration, NodeRecordId};

const CONNECT_TOKEN: Token = Token(0);
const HTTP_HEADER_SLOTS: usize = 64;
const IO_BUFFER_BYTES: usize = 8 * 1024;
const MAX_CHUNK_LINE_BYTES: usize = 128;
const MAX_BEARER_SECRET_BYTES: usize = 8 * 1024;
const MAX_REQUEST_HEADER_BYTES: usize = 64 * 1024;
const MAX_REQUEST_BODY_BYTES: usize = 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MihomoAdapterConfig {
    pub connect_timeout: Duration,
    pub read_timeout: Duration,
    pub write_timeout: Duration,
    pub max_response_header_bytes: usize,
    pub max_response_body_bytes: usize,
    pub max_websocket_frame_bytes: usize,
}

impl Default for MihomoAdapterConfig {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(1),
            read_timeout: Duration::from_secs(5),
            write_timeout: Duration::from_secs(5),
            max_response_header_bytes: 64 * 1024,
            max_response_body_bytes: 32 * 1024 * 1024,
            max_websocket_frame_bytes: 32 * 1024 * 1024,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MihomoAdapterConfigError {
    field: &'static str,
}

impl MihomoAdapterConfigError {
    fn zero(field: &'static str) -> Self {
        Self { field }
    }
}

impl fmt::Display for MihomoAdapterConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Mihomo adapter setting '{}' must be positive",
            self.field
        )
    }
}

impl std::error::Error for MihomoAdapterConfigError {}

#[derive(Clone, Debug)]
pub struct UnixMihomoAdapter {
    config: MihomoAdapterConfig,
    active_operations: Arc<ActiveOperations>,
}

#[derive(Debug, Default)]
struct ActiveOperations {
    state: Mutex<ActiveOperationState>,
}

#[derive(Debug, Default)]
struct ActiveOperationState {
    owner_generation: Option<u64>,
    cancelled: bool,
    sockets: Vec<Weak<ActiveOperation>>,
}

impl ActiveOperations {
    fn register(&self, stream: &UnixStream) -> Result<Arc<ActiveOperation>, MihomoError> {
        let operation = Arc::new(ActiveOperation {
            stream: stream
                .try_clone()
                .map_err(|_| unavailable("Mihomo cancellation handle creation failed"))?,
            cancelled: AtomicBool::new(false),
        });
        let cancelled = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.sockets.retain(|socket| socket.strong_count() > 0);
            if state.cancelled {
                true
            } else {
                state.sockets.push(Arc::downgrade(&operation));
                false
            }
        };
        if cancelled {
            operation.cancel();
            return Err(unavailable("Mihomo operation was cancelled"));
        }
        Ok(operation)
    }

    fn cancel(&self) {
        self.cancel_matching(None);
    }

    fn cancel_for(&self, owner_generation: u64) {
        self.cancel_matching(Some(owner_generation));
    }

    fn cancel_matching(&self, owner_generation: Option<u64>) {
        let operations = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if owner_generation.is_some() && state.owner_generation != owner_generation {
                return;
            }
            state.cancelled = true;
            let operations = state
                .sockets
                .iter()
                .filter_map(Weak::upgrade)
                .collect::<Vec<_>>();
            state.sockets.clear();
            operations
        };
        for operation in operations {
            operation.cancel();
        }
    }

    fn reset(&self) {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .cancelled = false;
    }

    fn reset_for(&self, owner_generation: u64) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state
            .owner_generation
            .is_none_or(|current| owner_generation > current)
        {
            state.owner_generation = Some(owner_generation);
            state.cancelled = false;
        }
    }
}

#[derive(Debug)]
struct ActiveOperation {
    stream: UnixStream,
    cancelled: AtomicBool,
}

impl ActiveOperation {
    fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
        let _ = self.stream.shutdown(Shutdown::Both);
    }

    fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }
}

impl UnixMihomoAdapter {
    pub fn new(config: MihomoAdapterConfig) -> Result<Self, MihomoAdapterConfigError> {
        for (field, value) in [
            ("connect_timeout", config.connect_timeout),
            ("read_timeout", config.read_timeout),
            ("write_timeout", config.write_timeout),
        ] {
            if value.is_zero() {
                return Err(MihomoAdapterConfigError::zero(field));
            }
        }
        for (field, value) in [
            (
                "max_response_header_bytes",
                config.max_response_header_bytes,
            ),
            ("max_response_body_bytes", config.max_response_body_bytes),
            (
                "max_websocket_frame_bytes",
                config.max_websocket_frame_bytes,
            ),
        ] {
            if value == 0 {
                return Err(MihomoAdapterConfigError::zero(field));
            }
        }
        Ok(Self {
            config,
            active_operations: Arc::new(ActiveOperations::default()),
        })
    }

    pub(crate) fn cancel_pending_for(&self, owner_generation: u64) {
        self.active_operations.cancel_for(owner_generation);
    }

    pub(crate) fn reset_cancellation_for(&self, owner_generation: u64) {
        self.active_operations.reset_for(owner_generation);
    }

    pub fn reload_configuration(
        &self,
        endpoint: &CoreControlEndpoint,
        configuration_path: &Path,
    ) -> Result<(), MihomoError> {
        #[derive(Serialize)]
        struct ReloadBody<'a> {
            path: &'a str,
        }

        let path = configuration_path
            .to_str()
            .ok_or_else(|| invalid_response("Mihomo configuration path is not valid UTF-8"))?;
        if !configuration_path.is_absolute() {
            return Err(invalid_response(
                "Mihomo configuration path is not absolute",
            ));
        }
        let body = serde_json::to_vec(&ReloadBody { path })
            .map_err(|_| invalid_response("Mihomo reload request encoding failed"))?;
        let response = self.request(endpoint, HttpMethod::Put, "/configs?force=true", &body)?;
        match response.head.status {
            204 if response.body.is_empty() => Ok(()),
            401 | 403 => Err(unauthorized("Mihomo reload authorization failed")),
            500..=599 => Err(unavailable("Mihomo reload endpoint is unavailable")),
            _ => Err(invalid_response("Mihomo reload response is invalid")),
        }
    }

    fn request(
        &self,
        endpoint: &CoreControlEndpoint,
        method: HttpMethod,
        target: &str,
        body: &[u8],
    ) -> Result<HttpResponse, MihomoError> {
        validate_bearer_secret(endpoint.secret())?;
        validate_request_target(target)?;
        if body.len() > MAX_REQUEST_BODY_BYTES {
            return Err(invalid_response("Mihomo request body exceeded its limit"));
        }

        let mut stream = self.connect(endpoint)?;
        let _active_operation = self.active_operations.register(&stream)?;
        let request = encode_request(method, target, endpoint.secret(), body)?;
        let write_deadline = deadline_after(
            self.config.write_timeout,
            "Mihomo request write deadline is invalid",
        )?;
        write_all_deadlined(
            &mut stream,
            &request,
            write_deadline,
            "Mihomo request write failed",
        )?;
        flush_deadlined(&mut stream, write_deadline, "Mihomo request flush failed")?;
        let read_deadline = deadline_after(
            self.config.read_timeout,
            "Mihomo response read deadline is invalid",
        )?;
        read_http_response(&mut stream, self.config, read_deadline)
    }

    fn connect(&self, endpoint: &CoreControlEndpoint) -> Result<UnixStream, MihomoError> {
        let mut poll = Poll::new().map_err(|_| unavailable("Mihomo poll creation failed"))?;
        let mut stream = MioUnixStream::connect(&endpoint.socket_path)
            .map_err(|_| unavailable("Mihomo Unix socket connect failed"))?;
        poll.registry()
            .register(&mut stream, CONNECT_TOKEN, Interest::WRITABLE)
            .map_err(|_| unavailable("Mihomo Unix socket registration failed"))?;

        let deadline = Instant::now()
            .checked_add(self.config.connect_timeout)
            .ok_or_else(|| unavailable("Mihomo connect deadline is invalid"))?;
        let mut events = Events::with_capacity(4);
        loop {
            if connection_is_ready(&stream)? {
                break;
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(unavailable("Mihomo Unix socket connect timed out"));
            }
            poll.poll(&mut events, Some(remaining))
                .map_err(|_| unavailable("Mihomo Unix socket poll failed"))?;
            if events.is_empty() {
                return Err(unavailable("Mihomo Unix socket connect timed out"));
            }
            if events.iter().any(|event| event.token() == CONNECT_TOKEN)
                && let Some(_error) = stream
                    .take_error()
                    .map_err(|_| unavailable("Mihomo Unix socket status failed"))?
            {
                return Err(unavailable("Mihomo Unix socket connect failed"));
            }
        }

        poll.registry()
            .deregister(&mut stream)
            .map_err(|_| unavailable("Mihomo Unix socket deregistration failed"))?;
        let stream: UnixStream = stream.into();
        stream
            .set_nonblocking(false)
            .map_err(|_| unavailable("Mihomo Unix socket mode setup failed"))?;
        stream
            .set_read_timeout(Some(self.config.read_timeout))
            .map_err(|_| unavailable("Mihomo read deadline setup failed"))?;
        stream
            .set_write_timeout(Some(self.config.write_timeout))
            .map_err(|_| unavailable("Mihomo write deadline setup failed"))?;
        Ok(stream)
    }

    fn open_stream<T: Send + 'static>(
        &self,
        endpoint: &CoreControlEndpoint,
        target: &str,
        generation: CoreInstanceGeneration,
        decoder: fn(&[u8]) -> Result<T, ProjectionError>,
    ) -> Result<Box<dyn CoreEventStream<T>>, MihomoError> {
        validate_bearer_secret(endpoint.secret())?;
        validate_request_target(target)?;
        let mut stream = self.connect(endpoint)?;
        let active_operation = self.active_operations.register(&stream)?;
        let key = generate_key();
        let request = encode_websocket_request(target, endpoint.secret(), &key)?;
        let write_deadline = deadline_after(
            self.config.write_timeout,
            "Mihomo WebSocket handshake write deadline is invalid",
        )?;
        write_all_deadlined(
            &mut stream,
            &request,
            write_deadline,
            "Mihomo WebSocket handshake write failed",
        )?;
        flush_deadlined(
            &mut stream,
            write_deadline,
            "Mihomo WebSocket handshake flush failed",
        )?;

        let read_deadline = deadline_after(
            self.config.read_timeout,
            "Mihomo WebSocket handshake read deadline is invalid",
        )?;
        let head = read_http_head(
            &mut stream,
            self.config.max_response_header_bytes,
            read_deadline,
        )?;
        validate_websocket_handshake(&head, &key)?;
        let websocket_config = WebSocketConfig::default()
            .read_buffer_size(self.config.max_websocket_frame_bytes.min(IO_BUFFER_BYTES))
            .write_buffer_size(0)
            .max_write_buffer_size(
                self.config
                    .max_websocket_frame_bytes
                    .saturating_add(IO_BUFFER_BYTES),
            )
            .max_message_size(Some(self.config.max_websocket_frame_bytes))
            .max_frame_size(Some(self.config.max_websocket_frame_bytes));
        let stream =
            DeadlineUnixStream::new(stream, self.config.read_timeout, self.config.write_timeout);
        let websocket =
            WebSocket::from_partially_read(stream, head.tail, Role::Client, Some(websocket_config));
        Ok(Box::new(UnixWebSocketEventStream {
            websocket,
            generation,
            decoder,
            cancelled: false,
            active_operation,
        }))
    }
}

impl Default for UnixMihomoAdapter {
    fn default() -> Self {
        Self::new(MihomoAdapterConfig::default()).expect("default Mihomo adapter configuration")
    }
}

impl MihomoAdapter for UnixMihomoAdapter {
    fn cancel_pending(&self) {
        self.active_operations.cancel();
    }

    fn reset_cancellation(&self) {
        self.active_operations.reset();
    }

    fn version(&self, endpoint: &CoreControlEndpoint) -> Result<MihomoVersion, MihomoError> {
        let response = self.request(endpoint, HttpMethod::Get, "/version", &[])?;
        require_status(&response, 200, MihomoErrorKind::InvalidResponse)?;
        require_json_content_type(&response.head)?;
        MihomoJsonCodec::version(&response.body)
            .map_err(|_| invalid_response("Mihomo version projection failed"))
    }

    fn readiness(&self, endpoint: &CoreControlEndpoint) -> Result<MihomoReadiness, MihomoError> {
        let response = self.request(endpoint, HttpMethod::Get, "/version", &[])?;
        match response.head.status {
            200 => {
                require_json_content_type(&response.head)?;
                MihomoJsonCodec::version(&response.body)
                    .map_err(|_| invalid_response("Mihomo readiness projection failed"))?;
                Ok(MihomoReadiness::Ready)
            }
            401 | 403 => Err(unauthorized("Mihomo readiness authorization failed")),
            500..=599 => Ok(MihomoReadiness::Starting),
            _ => Err(invalid_response("Mihomo readiness status is invalid")),
        }
    }

    fn proxy_view(
        &self,
        endpoint: &CoreControlEndpoint,
        effective_group_order: &[String],
    ) -> Result<ProxyView, MihomoError> {
        let proxies = self.request(endpoint, HttpMethod::Get, "/proxies", &[])?;
        require_status(&proxies, 200, MihomoErrorKind::InvalidResponse)?;
        require_json_content_type(&proxies.head)?;

        let providers = match self.request(endpoint, HttpMethod::Get, "/providers/proxies", &[]) {
            Ok(response) if response.head.status == 200 => {
                require_json_content_type(&response.head)?;
                Some(response.body)
            }
            Ok(response) if matches!(response.head.status, 500..=599) => None,
            Ok(response) if matches!(response.head.status, 401 | 403) => {
                return Err(unauthorized("Mihomo provider authorization failed"));
            }
            Ok(_) => return Err(invalid_response("Mihomo provider status is invalid")),
            Err(error) if error.kind == MihomoErrorKind::Unavailable => None,
            Err(error) => return Err(error),
        };

        project_proxy_view(&proxies.body, providers.as_deref(), effective_group_order)
            .map_err(|_| invalid_response("Mihomo Proxy View projection failed"))
    }

    fn select_node(
        &self,
        endpoint: &CoreControlEndpoint,
        selection: &NodeSelection,
    ) -> Result<(), MihomoError> {
        #[derive(Serialize)]
        struct SelectionBody<'a> {
            name: &'a str,
        }

        if selection.group_name.is_empty() || selection.node_name.is_empty() {
            return Err(selection_rejected("Mihomo Node selection is empty"));
        }
        let group = encode_path_segment(&selection.group_name);
        let target = format!("/proxies/{group}/");
        let body = serde_json::to_vec(&SelectionBody {
            name: &selection.node_name,
        })
        .map_err(|_| selection_rejected("Mihomo Node selection encoding failed"))?;
        let response = self.request(endpoint, HttpMethod::Put, &target, &body)?;
        match response.head.status {
            204 if response.body.is_empty() => Ok(()),
            401 | 403 => Err(unauthorized("Mihomo Node selection authorization failed")),
            500..=599 => Err(unavailable("Mihomo Node selection endpoint is unavailable")),
            _ => Err(selection_rejected("Mihomo Node selection was rejected")),
        }
    }

    fn probe_delay(
        &self,
        endpoint: &CoreControlEndpoint,
        request: &DelayProbeRequest,
    ) -> Result<DelayProbeResult, MihomoError> {
        validate_delay_request(request)?;
        let mut query = form_urlencoded::Serializer::new(String::new());
        query.append_pair("url", &request.test_url);
        query.append_pair("timeout", &request.timeout_ms.to_string());
        let query = query.finish();
        let target = match &request.target {
            DelayTarget::CoreProxy { proxy_name } => {
                let proxy = encode_path_segment(proxy_name);
                format!("/proxies/{proxy}/delay?{query}")
            }
            DelayTarget::ProviderProxy {
                provider_name,
                proxy_name,
            } => {
                let provider = encode_path_segment(provider_name);
                let proxy = encode_path_segment(proxy_name);
                format!("/providers/proxies/{provider}/{proxy}/healthcheck?{query}")
            }
        };
        let response = self.request(endpoint, HttpMethod::Get, &target, &[])?;
        match response.head.status {
            200 => {
                require_json_content_type(&response.head)?;
                MihomoJsonCodec::delay(&response.body)
                    .map_err(|_| invalid_response("Mihomo Delay Probe projection failed"))
            }
            401 | 403 => Err(unauthorized("Mihomo Delay Probe authorization failed")),
            _ => Err(probe_failed("Mihomo Delay Probe was rejected")),
        }
    }

    fn connection_summary(
        &self,
        endpoint: &CoreControlEndpoint,
    ) -> Result<ConnectionSummary, MihomoError> {
        let response = self.request(endpoint, HttpMethod::Get, "/connections", &[])?;
        require_status(&response, 200, MihomoErrorKind::InvalidResponse)?;
        require_json_content_type(&response.head)?;
        MihomoJsonCodec::connections(&response.body)
            .map_err(|_| invalid_response("Mihomo connection projection failed"))
    }

    fn open_traffic_stream(
        &self,
        endpoint: &CoreControlEndpoint,
        generation: CoreInstanceGeneration,
    ) -> Result<Box<dyn CoreEventStream<TrafficFrame>>, MihomoError> {
        self.open_stream(endpoint, "/traffic", generation, MihomoJsonCodec::traffic)
    }

    fn open_connection_stream(
        &self,
        endpoint: &CoreControlEndpoint,
        generation: CoreInstanceGeneration,
    ) -> Result<Box<dyn CoreEventStream<ConnectionSummary>>, MihomoError> {
        self.open_stream(
            endpoint,
            "/connections",
            generation,
            MihomoJsonCodec::connections,
        )
    }

    fn open_log_stream(
        &self,
        endpoint: &CoreControlEndpoint,
        generation: CoreInstanceGeneration,
    ) -> Result<Box<dyn CoreEventStream<MihomoLogFrame>>, MihomoError> {
        self.open_stream(
            endpoint,
            "/logs?level=debug",
            generation,
            MihomoJsonCodec::log,
        )
    }
}

// -----------------------------------------------------------------------------
// Bounded HTTP over a private Unix socket
// -----------------------------------------------------------------------------

#[derive(Clone, Copy)]
enum HttpMethod {
    Get,
    Put,
}

impl HttpMethod {
    fn as_str(self) -> &'static str {
        match self {
            Self::Get => "GET",
            Self::Put => "PUT",
        }
    }
}

struct HttpHeader {
    name: String,
    value: Vec<u8>,
}

struct HttpHead {
    status: u16,
    headers: Vec<HttpHeader>,
    tail: Vec<u8>,
}

impl HttpHead {
    fn values<'a>(&'a self, name: &'static str) -> impl Iterator<Item = &'a [u8]> + 'a {
        self.headers
            .iter()
            .filter(move |header| header.name.eq_ignore_ascii_case(name))
            .map(|header| header.value.as_slice())
    }

    fn single_value(&self, name: &'static str) -> Result<Option<&[u8]>, MihomoError> {
        let mut values = self.values(name);
        let value = values.next();
        if values.next().is_some() {
            return Err(invalid_response(
                "Mihomo response repeated a singleton header",
            ));
        }
        Ok(value)
    }

    fn has_token(&self, name: &'static str, expected: &str) -> Result<bool, MihomoError> {
        for value in self.values(name) {
            let value = std::str::from_utf8(value)
                .map_err(|_| invalid_response("Mihomo response header is not ASCII"))?;
            if value
                .split(',')
                .any(|token| token.trim().eq_ignore_ascii_case(expected))
            {
                return Ok(true);
            }
        }
        Ok(false)
    }
}

struct HttpResponse {
    head: HttpHead,
    body: Vec<u8>,
}

fn connection_is_ready(stream: &MioUnixStream) -> Result<bool, MihomoError> {
    if let Some(_error) = stream
        .take_error()
        .map_err(|_| unavailable("Mihomo Unix socket status failed"))?
    {
        return Err(unavailable("Mihomo Unix socket connect failed"));
    }
    match stream.peer_addr() {
        Ok(_) => Ok(true),
        Err(error)
            if matches!(
                error.kind(),
                io::ErrorKind::NotConnected | io::ErrorKind::WouldBlock
            ) =>
        {
            Ok(false)
        }
        Err(_) => Err(unavailable("Mihomo Unix socket connect failed")),
    }
}

fn deadline_after(timeout: Duration, diagnostic: &'static str) -> Result<Instant, MihomoError> {
    Instant::now()
        .checked_add(timeout)
        .ok_or_else(|| unavailable(diagnostic))
}

fn remaining_deadline(
    deadline: Instant,
    diagnostic: &'static str,
) -> Result<Duration, MihomoError> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        Err(unavailable(diagnostic))
    } else {
        Ok(remaining)
    }
}

fn read_deadlined(
    stream: &mut UnixStream,
    buffer: &mut [u8],
    deadline: Instant,
    diagnostic: &'static str,
) -> Result<usize, MihomoError> {
    loop {
        let remaining = remaining_deadline(deadline, diagnostic)?;
        stream
            .set_read_timeout(Some(remaining))
            .map_err(|_| unavailable(diagnostic))?;
        match stream.read(buffer) {
            Ok(read) => return Ok(read),
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(_) => return Err(unavailable(diagnostic)),
        }
    }
}

fn write_all_deadlined(
    stream: &mut UnixStream,
    mut bytes: &[u8],
    deadline: Instant,
    diagnostic: &'static str,
) -> Result<(), MihomoError> {
    while !bytes.is_empty() {
        let remaining = remaining_deadline(deadline, diagnostic)?;
        stream
            .set_write_timeout(Some(remaining))
            .map_err(|_| unavailable(diagnostic))?;
        match stream.write(bytes) {
            Ok(0) => return Err(unavailable(diagnostic)),
            Ok(written) => bytes = &bytes[written..],
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(_) => return Err(unavailable(diagnostic)),
        }
    }
    Ok(())
}

fn flush_deadlined(
    stream: &mut UnixStream,
    deadline: Instant,
    diagnostic: &'static str,
) -> Result<(), MihomoError> {
    let remaining = remaining_deadline(deadline, diagnostic)?;
    stream
        .set_write_timeout(Some(remaining))
        .map_err(|_| unavailable(diagnostic))?;
    stream.flush().map_err(|_| unavailable(diagnostic))
}

fn encode_request(
    method: HttpMethod,
    target: &str,
    secret: &str,
    body: &[u8],
) -> Result<Vec<u8>, MihomoError> {
    let content_headers = if body.is_empty() {
        String::new()
    } else {
        format!(
            "Content-Type: application/json\r\nContent-Length: {}\r\n",
            body.len()
        )
    };
    let head = format!(
        "{} {target} HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer {secret}\r\nAccept: application/json\r\n{content_headers}Connection: close\r\n\r\n",
        method.as_str(),
    );
    if head.len() > MAX_REQUEST_HEADER_BYTES {
        return Err(invalid_response("Mihomo request header exceeded its limit"));
    }
    let mut request = Vec::with_capacity(head.len().saturating_add(body.len()));
    request.extend_from_slice(head.as_bytes());
    request.extend_from_slice(body);
    Ok(request)
}

fn encode_websocket_request(target: &str, secret: &str, key: &str) -> Result<Vec<u8>, MihomoError> {
    let request = format!(
        "GET {target} HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer {secret}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n\r\n"
    );
    if request.len() > MAX_REQUEST_HEADER_BYTES {
        return Err(invalid_response(
            "Mihomo WebSocket request header exceeded its limit",
        ));
    }
    Ok(request.into_bytes())
}

fn read_http_response(
    stream: &mut UnixStream,
    config: MihomoAdapterConfig,
    deadline: Instant,
) -> Result<HttpResponse, MihomoError> {
    let mut head = read_http_head(stream, config.max_response_header_bytes, deadline)?;
    let initial = std::mem::take(&mut head.tail);
    let body = read_http_body(
        stream,
        &head,
        initial,
        config.max_response_body_bytes,
        config.max_response_header_bytes,
        deadline,
    )?;
    Ok(HttpResponse { head, body })
}

fn read_http_head(
    stream: &mut UnixStream,
    limit: usize,
    deadline: Instant,
) -> Result<HttpHead, MihomoError> {
    let mut bytes = Vec::with_capacity(limit.min(IO_BUFFER_BYTES));
    let header_end = loop {
        if let Some(index) = find_bytes(&bytes, b"\r\n\r\n") {
            break index + 4;
        }
        if bytes.len() == limit {
            return Err(invalid_response(
                "Mihomo response header exceeded its limit",
            ));
        }
        let mut buffer = [0_u8; IO_BUFFER_BYTES];
        let capacity = (limit - bytes.len()).min(buffer.len());
        let read = read_deadlined(
            stream,
            &mut buffer[..capacity],
            deadline,
            "Mihomo response header read failed",
        )?;
        if read == 0 {
            return Err(unavailable("Mihomo response closed before its header"));
        }
        bytes.extend_from_slice(&buffer[..read]);
    };

    let mut headers = [httparse::EMPTY_HEADER; HTTP_HEADER_SLOTS];
    let mut parsed = httparse::Response::new(&mut headers);
    let parsed_bytes = parsed
        .parse(&bytes[..header_end])
        .map_err(|_| invalid_response("Mihomo response header is malformed"))?;
    if parsed_bytes != httparse::Status::Complete(header_end) || parsed.version != Some(1) {
        return Err(invalid_response("Mihomo response did not use HTTP/1.1"));
    }
    let status = parsed
        .code
        .ok_or_else(|| invalid_response("Mihomo response omitted its status"))?;
    let headers = parsed
        .headers
        .iter()
        .map(|header| HttpHeader {
            name: header.name.to_owned(),
            value: header.value.to_vec(),
        })
        .collect();
    Ok(HttpHead {
        status,
        headers,
        tail: bytes[header_end..].to_vec(),
    })
}

fn read_http_body(
    stream: &mut UnixStream,
    head: &HttpHead,
    initial: Vec<u8>,
    body_limit: usize,
    header_limit: usize,
    deadline: Instant,
) -> Result<Vec<u8>, MihomoError> {
    if let Some(encoding) = head.single_value("content-encoding")? {
        let encoding = ascii_header(encoding)?;
        if !encoding.trim().eq_ignore_ascii_case("identity") {
            return Err(invalid_response(
                "Mihomo response used unsupported content encoding",
            ));
        }
    }

    let content_length = head
        .single_value("content-length")?
        .map(parse_content_length)
        .transpose()?;
    let transfer_encoding = head.single_value("transfer-encoding")?;
    if content_length.is_some() && transfer_encoding.is_some() {
        return Err(invalid_response(
            "Mihomo response used conflicting body framing",
        ));
    }
    if matches!(head.status, 100..=199 | 204 | 304) {
        if content_length.unwrap_or(0) != 0 || transfer_encoding.is_some() || !initial.is_empty() {
            return Err(invalid_response("Mihomo bodyless response carried a body"));
        }
        return Ok(Vec::new());
    }

    if let Some(length) = content_length {
        return read_fixed_body(stream, initial, length, body_limit, deadline);
    }
    if let Some(encoding) = transfer_encoding {
        let encoding = ascii_header(encoding)?;
        if !encoding.trim().eq_ignore_ascii_case("chunked") {
            return Err(invalid_response(
                "Mihomo response used unsupported transfer encoding",
            ));
        }
        return read_chunked_body(stream, initial, body_limit, header_limit, deadline);
    }
    read_close_delimited_body(stream, initial, body_limit, deadline)
}

fn read_fixed_body(
    stream: &mut UnixStream,
    mut body: Vec<u8>,
    length: usize,
    limit: usize,
    deadline: Instant,
) -> Result<Vec<u8>, MihomoError> {
    if length > limit || body.len() > length {
        return Err(invalid_response("Mihomo response body exceeded its limit"));
    }
    while body.len() < length {
        let mut buffer = [0_u8; IO_BUFFER_BYTES];
        let capacity = (length - body.len()).min(buffer.len());
        let read = read_deadlined(
            stream,
            &mut buffer[..capacity],
            deadline,
            "Mihomo response body read failed",
        )?;
        if read == 0 {
            return Err(invalid_response("Mihomo response body was truncated"));
        }
        body.extend_from_slice(&buffer[..read]);
    }
    Ok(body)
}

fn read_close_delimited_body(
    stream: &mut UnixStream,
    mut body: Vec<u8>,
    limit: usize,
    deadline: Instant,
) -> Result<Vec<u8>, MihomoError> {
    if body.len() > limit {
        return Err(invalid_response("Mihomo response body exceeded its limit"));
    }
    loop {
        let mut buffer = [0_u8; IO_BUFFER_BYTES];
        let capacity = if body.len() == limit {
            1
        } else {
            (limit - body.len()).min(buffer.len())
        };
        let read = read_deadlined(
            stream,
            &mut buffer[..capacity],
            deadline,
            "Mihomo response body read failed",
        )?;
        if read == 0 {
            return Ok(body);
        }
        if body.len() == limit {
            return Err(invalid_response("Mihomo response body exceeded its limit"));
        }
        body.extend_from_slice(&buffer[..read]);
    }
}

fn read_chunked_body(
    stream: &mut UnixStream,
    mut encoded: Vec<u8>,
    limit: usize,
    header_limit: usize,
    deadline: Instant,
) -> Result<Vec<u8>, MihomoError> {
    let wire_limit = limit.saturating_add(header_limit.max(MAX_CHUNK_LINE_BYTES));
    if encoded.len() > wire_limit {
        return Err(invalid_response("Mihomo chunked body exceeded its limit"));
    }
    let mut cursor = 0;
    let mut body = Vec::new();
    loop {
        let line_end = loop {
            if let Some(relative) = find_bytes(&encoded[cursor..], b"\r\n") {
                break cursor + relative;
            }
            if encoded.len().saturating_sub(cursor) >= MAX_CHUNK_LINE_BYTES {
                return Err(invalid_response("Mihomo chunk header exceeded its limit"));
            }
            read_more_chunk_bytes(stream, &mut encoded, wire_limit, deadline)?;
        };
        let line = ascii_header(&encoded[cursor..line_end])?;
        let size_text = line.split(';').next().unwrap_or_default().trim();
        if size_text.is_empty() {
            return Err(invalid_response("Mihomo chunk size is missing"));
        }
        let chunk_size = usize::from_str_radix(size_text, 16)
            .map_err(|_| invalid_response("Mihomo chunk size is invalid"))?;
        cursor = line_end + 2;
        if chunk_size == 0 {
            ensure_chunk_bytes(
                stream,
                &mut encoded,
                cursor.saturating_add(2),
                wire_limit,
                deadline,
            )?;
            if encoded.get(cursor..cursor + 2) != Some(b"\r\n") {
                return Err(invalid_response("Mihomo chunk trailer is invalid"));
            }
            if encoded.len() != cursor + 2 {
                return Err(invalid_response(
                    "Mihomo chunked body carried trailing bytes",
                ));
            }
            return Ok(body);
        }
        if chunk_size > limit.saturating_sub(body.len()) {
            return Err(invalid_response("Mihomo response body exceeded its limit"));
        }
        let data_end = cursor
            .checked_add(chunk_size)
            .ok_or_else(|| invalid_response("Mihomo chunk size overflowed"))?;
        let framed_end = data_end
            .checked_add(2)
            .ok_or_else(|| invalid_response("Mihomo chunk size overflowed"))?;
        ensure_chunk_bytes(stream, &mut encoded, framed_end, wire_limit, deadline)?;
        if encoded.get(data_end..framed_end) != Some(b"\r\n") {
            return Err(invalid_response("Mihomo chunk framing is invalid"));
        }
        body.extend_from_slice(&encoded[cursor..data_end]);
        cursor = framed_end;
    }
}

fn ensure_chunk_bytes(
    stream: &mut UnixStream,
    encoded: &mut Vec<u8>,
    required: usize,
    wire_limit: usize,
    deadline: Instant,
) -> Result<(), MihomoError> {
    if required > wire_limit {
        return Err(invalid_response("Mihomo chunked body exceeded its limit"));
    }
    while encoded.len() < required {
        read_more_chunk_bytes(stream, encoded, wire_limit, deadline)?;
    }
    Ok(())
}

fn read_more_chunk_bytes(
    stream: &mut UnixStream,
    encoded: &mut Vec<u8>,
    wire_limit: usize,
    deadline: Instant,
) -> Result<(), MihomoError> {
    if encoded.len() == wire_limit {
        return Err(invalid_response("Mihomo chunked body exceeded its limit"));
    }
    let mut buffer = [0_u8; IO_BUFFER_BYTES];
    let capacity = (wire_limit - encoded.len()).min(buffer.len());
    let read = read_deadlined(
        stream,
        &mut buffer[..capacity],
        deadline,
        "Mihomo chunked body read failed",
    )?;
    if read == 0 {
        return Err(invalid_response("Mihomo chunked body was truncated"));
    }
    encoded.extend_from_slice(&buffer[..read]);
    Ok(())
}

fn parse_content_length(value: &[u8]) -> Result<usize, MihomoError> {
    ascii_header(value)?
        .trim()
        .parse::<usize>()
        .map_err(|_| invalid_response("Mihomo content length is invalid"))
}

fn ascii_header(value: &[u8]) -> Result<&str, MihomoError> {
    let value = std::str::from_utf8(value)
        .map_err(|_| invalid_response("Mihomo response header is not ASCII"))?;
    if value.bytes().all(|byte| byte == b'\t' || byte >= b' ') {
        Ok(value)
    } else {
        Err(invalid_response(
            "Mihomo response header contains control bytes",
        ))
    }
}

fn require_json_content_type(head: &HttpHead) -> Result<(), MihomoError> {
    let value = head
        .single_value("content-type")?
        .ok_or_else(|| invalid_response("Mihomo JSON response omitted content type"))?;
    let value = ascii_header(value)?;
    let media_type = value.split(';').next().unwrap_or_default().trim();
    if media_type.eq_ignore_ascii_case("application/json") {
        Ok(())
    } else {
        Err(invalid_response(
            "Mihomo JSON response used an invalid content type",
        ))
    }
}

fn require_status(
    response: &HttpResponse,
    expected: u16,
    mismatch_kind: MihomoErrorKind,
) -> Result<(), MihomoError> {
    match response.head.status {
        status if status == expected => Ok(()),
        401 | 403 => Err(unauthorized("Mihomo API authorization failed")),
        500..=599 => Err(unavailable("Mihomo API endpoint is unavailable")),
        _ => Err(MihomoError::new(
            mismatch_kind,
            "Mihomo API returned an unexpected status",
        )),
    }
}

fn validate_websocket_handshake(head: &HttpHead, key: &str) -> Result<(), MihomoError> {
    match head.status {
        101 => {}
        401 | 403 => return Err(unauthorized("Mihomo WebSocket authorization failed")),
        500..=599 => return Err(unavailable("Mihomo WebSocket endpoint is unavailable")),
        _ => return Err(invalid_response("Mihomo WebSocket status is invalid")),
    }
    if !head.has_token("connection", "upgrade")? || !head.has_token("upgrade", "websocket")? {
        return Err(invalid_response(
            "Mihomo WebSocket upgrade headers are invalid",
        ));
    }
    let accept = head
        .single_value("sec-websocket-accept")?
        .ok_or_else(|| invalid_response("Mihomo WebSocket accept header is missing"))?;
    let accept = ascii_header(accept)?.trim();
    if accept != derive_accept_key(key.as_bytes()) {
        return Err(invalid_response(
            "Mihomo WebSocket accept header is invalid",
        ));
    }
    if head.single_value("transfer-encoding")?.is_some() {
        return Err(invalid_response(
            "Mihomo WebSocket handshake carried transfer encoding",
        ));
    }
    if head
        .single_value("content-length")?
        .map(parse_content_length)
        .transpose()?
        .unwrap_or(0)
        != 0
    {
        return Err(invalid_response(
            "Mihomo WebSocket handshake carried an HTTP body",
        ));
    }
    Ok(())
}

fn validate_bearer_secret(secret: &str) -> Result<(), MihomoError> {
    if secret.is_empty()
        || secret.len() > MAX_BEARER_SECRET_BYTES
        || !secret.bytes().all(|byte| matches!(byte, b'!'..=b'~'))
    {
        return Err(unauthorized("Mihomo bearer secret is invalid"));
    }
    Ok(())
}

fn validate_request_target(target: &str) -> Result<(), MihomoError> {
    if target.is_empty()
        || target.len() > MAX_REQUEST_HEADER_BYTES
        || !target.starts_with('/')
        || !target.is_ascii()
        || target.bytes().any(|byte| byte.is_ascii_control())
    {
        return Err(invalid_response("Mihomo request target is invalid"));
    }
    Ok(())
}

fn validate_delay_request(request: &DelayProbeRequest) -> Result<(), MihomoError> {
    if request.timeout_ms == 0 || request.timeout_ms > i16::MAX as u64 {
        return Err(probe_failed(
            "Mihomo Delay Probe timeout is outside the supported range",
        ));
    }
    let url = url::Url::parse(&request.test_url)
        .map_err(|_| probe_failed("Mihomo Delay Probe URL is invalid"))?;
    if !matches!(url.scheme(), "http" | "https") || url.host().is_none() {
        return Err(probe_failed("Mihomo Delay Probe URL is invalid"));
    }
    let expected_record_id = match &request.target {
        DelayTarget::CoreProxy { proxy_name } => NodeRecordId::for_core(proxy_name),
        DelayTarget::ProviderProxy {
            provider_name,
            proxy_name,
        } => NodeRecordId::for_provider(provider_name, proxy_name),
    };
    if request.record_id != expected_record_id {
        return Err(probe_failed(
            "Mihomo Delay Probe target identity is inconsistent",
        ));
    }
    Ok(())
}

fn encode_path_segment(value: &str) -> String {
    utf8_percent_encode(value, NON_ALPHANUMERIC).to_string()
}

fn find_bytes(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

fn unavailable(diagnostic: &'static str) -> MihomoError {
    MihomoError::new(MihomoErrorKind::Unavailable, diagnostic)
}

fn unauthorized(diagnostic: &'static str) -> MihomoError {
    MihomoError::new(MihomoErrorKind::Unauthorized, diagnostic)
}

fn invalid_response(diagnostic: &'static str) -> MihomoError {
    MihomoError::new(MihomoErrorKind::InvalidResponse, diagnostic)
}

fn selection_rejected(diagnostic: &'static str) -> MihomoError {
    MihomoError::new(MihomoErrorKind::SelectionRejected, diagnostic)
}

fn probe_failed(diagnostic: &'static str) -> MihomoError {
    MihomoError::new(MihomoErrorKind::ProbeFailed, diagnostic)
}

// -----------------------------------------------------------------------------
// Bounded WebSocket event streams
// -----------------------------------------------------------------------------

struct DeadlineUnixStream {
    stream: UnixStream,
    read_timeout: Duration,
    write_timeout: Duration,
    read_deadline: Option<Instant>,
    write_deadline: Option<Instant>,
}

impl DeadlineUnixStream {
    fn new(stream: UnixStream, read_timeout: Duration, write_timeout: Duration) -> Self {
        Self {
            stream,
            read_timeout,
            write_timeout,
            read_deadline: None,
            write_deadline: None,
        }
    }

    fn begin_read(&mut self) {
        self.read_deadline = Instant::now().checked_add(self.read_timeout);
    }

    fn begin_write(&mut self) {
        self.write_deadline = Instant::now().checked_add(self.write_timeout);
    }

    fn shutdown(&self) -> io::Result<()> {
        self.stream.shutdown(Shutdown::Both)
    }

    fn remaining(deadline: Option<Instant>) -> io::Result<Duration> {
        let deadline = deadline.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::TimedOut,
                "Mihomo stream deadline is unavailable",
            )
        })?;
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "Mihomo stream deadline expired",
            ))
        } else {
            Ok(remaining)
        }
    }
}

impl Read for DeadlineUnixStream {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        loop {
            let remaining = Self::remaining(self.read_deadline)?;
            self.stream.set_read_timeout(Some(remaining))?;
            match self.stream.read(buffer) {
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                result => return result,
            }
        }
    }
}

impl Write for DeadlineUnixStream {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        loop {
            let remaining = Self::remaining(self.write_deadline)?;
            self.stream.set_write_timeout(Some(remaining))?;
            match self.stream.write(buffer) {
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                result => return result,
            }
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        let remaining = Self::remaining(self.write_deadline)?;
        self.stream.set_write_timeout(Some(remaining))?;
        self.stream.flush()
    }
}

struct UnixWebSocketEventStream<T> {
    websocket: WebSocket<DeadlineUnixStream>,
    generation: CoreInstanceGeneration,
    decoder: fn(&[u8]) -> Result<T, ProjectionError>,
    cancelled: bool,
    active_operation: Arc<ActiveOperation>,
}

impl<T: Send> CoreEventStream<T> for UnixWebSocketEventStream<T> {
    fn next_event(&mut self) -> Result<Option<CoreEvent<T>>, MihomoError> {
        if self.cancelled {
            return Ok(None);
        }
        self.websocket.get_mut().begin_read();
        self.websocket.get_mut().begin_write();
        loop {
            match self.websocket.read() {
                Ok(Message::Text(text)) => {
                    let payload = (self.decoder)(text.as_ref()).map_err(|_| {
                        invalid_response("Mihomo WebSocket event projection failed")
                    })?;
                    return Ok(Some(CoreEvent {
                        instance_generation: self.generation,
                        payload,
                    }));
                }
                Ok(Message::Close(_)) => {
                    self.cancelled = true;
                    let _ = self.websocket.get_mut().shutdown();
                    return Ok(None);
                }
                Ok(Message::Ping(_) | Message::Pong(_)) => {
                    self.websocket.get_mut().begin_write();
                    self.websocket
                        .flush()
                        .map_err(|_| stream_closed("Mihomo WebSocket control flush failed"))?;
                }
                Ok(Message::Binary(_) | Message::Frame(_)) => {
                    return Err(invalid_response(
                        "Mihomo WebSocket emitted a non-text event",
                    ));
                }
                Err(_) if self.active_operation.is_cancelled() => {
                    return Err(stream_closed("Mihomo WebSocket read was cancelled"));
                }
                Err(tungstenite::Error::ConnectionClosed | tungstenite::Error::AlreadyClosed) => {
                    self.cancelled = true;
                    return Ok(None);
                }
                Err(
                    tungstenite::Error::Capacity(_)
                    | tungstenite::Error::Protocol(_)
                    | tungstenite::Error::Utf8
                    | tungstenite::Error::AttackAttempt,
                ) => {
                    return Err(invalid_response("Mihomo WebSocket frame is invalid"));
                }
                Err(_) => return Err(stream_closed("Mihomo WebSocket read failed")),
            }
        }
    }

    fn cancel(&mut self) {
        self.cancelled = true;
        let _ = self.websocket.get_mut().shutdown();
    }
}

impl<T> Drop for UnixWebSocketEventStream<T> {
    fn drop(&mut self) {
        let _ = self.websocket.get_mut().shutdown();
    }
}

fn stream_closed(diagnostic: &'static str) -> MihomoError {
    MihomoError::new(MihomoErrorKind::StreamClosed, diagnostic)
}
