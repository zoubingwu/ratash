//! Synchronous request and streaming client transport.

use std::fmt;
use std::io;
use std::net::Shutdown;
use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use mio::net::UnixStream as MioUnixStream;
use mio::{Events, Interest, Poll, Token, Waker};

use crate::application::{
    ApplicationClient, ApplicationError, ApplicationOperation, ApplicationOutput,
    ProfileListOutcome, ProfileListPageOutcome, ProxyListOutcome, ProxyListPageOutcome,
    RuleListOutcome, RuleListPageOutcome, RulePlacement as ApplicationRulePlacement,
};
use crate::cancellation::CancellationToken;
use crate::constants::{
    IPC_LIST_PAGE_SIZE, IPC_PROFILE_ADD_TIMEOUT, IPC_REQUEST_TIMEOUT, IPC_RUNTIME_MUTATION_TIMEOUT,
    LOCAL_RULE_COUNT_MAX, MAX_ACTIVE_NODES, PROFILE_COUNT_MAX,
};
use crate::ipc::{
    EmptyPayload, IpcRequest, IpcResponse, LogSubscriptionPayload, LogTailPayload, LogTailV1,
    NodeSelectorPayload, ProfileAddPayload, ProfileListPagePayload, ProfileSelectorPayload,
    ProxyListPagePayload, ProxySelectPayload, RequestId, RequestOperation, RuleAddPayload,
    RuleListPagePayload, RulePlacement, RuleReplacePayload, RuleSelectorPayload,
    StatusSubscriptionPayload, read_frame, write_frame,
};
use crate::unix_io::DeadlineUnixStream;

use super::client_error::{
    application_error, cancelled_operation, connect_error, ipc_connection_is_ready,
    operation_may_commit, operation_read_error, operation_read_setup_error, operation_write_error,
    poll_for_ipc_connect, protocol_error, read_error, write_error,
};
use super::stream::{IpcStreamCancellation, LogStream, StatusStream, StreamTransport};
use super::wire::{ExpectedOutput, decode_application_output};

const CLIENT_CONNECT_TOKEN: Token = Token(0);
pub(super) const CLIENT_CANCEL_TOKEN: Token = Token(1);

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

    fn connect_cancellable(&self, cancellation: &CancellationToken) -> io::Result<UnixStream> {
        if self.connect_timeout.is_zero() || self.stream_timeout().is_zero() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "IPC deadlines must be positive",
            ));
        }
        if cancellation.is_cancelled() {
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "IPC connect was cancelled",
            ));
        }
        let mut poll = Poll::new()?;
        let cancellation_waker = Arc::new(Waker::new(poll.registry(), CLIENT_CANCEL_TOKEN)?);
        let interrupt_waker = Arc::clone(&cancellation_waker);
        let _cancellation_registration = cancellation.register_interrupt(move || {
            let _ = interrupt_waker.wake();
        });
        let mut stream = MioUnixStream::connect(&self.socket_path)?;
        poll.registry()
            .register(&mut stream, CLIENT_CONNECT_TOKEN, Interest::WRITABLE)?;
        let deadline = Instant::now()
            .checked_add(self.connect_timeout)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "IPC deadline overflow"))?;
        let mut events = Events::with_capacity(4);
        loop {
            if cancellation.is_cancelled() {
                return Err(io::Error::new(
                    io::ErrorKind::Interrupted,
                    "IPC connect was cancelled",
                ));
            }
            if ipc_connection_is_ready(&stream)? {
                break;
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "IPC connect timed out",
                ));
            }
            poll_for_ipc_connect(&mut poll, &mut events, remaining, cancellation)?;
        }
        poll.registry().deregister(&mut stream)?;
        let stream: UnixStream = stream.into();
        stream.set_nonblocking(false)?;
        Ok(stream)
    }

    pub(super) fn stream_timeout(&self) -> Duration {
        match self.timeout_policy {
            IpcTimeoutPolicy::Product => IPC_REQUEST_TIMEOUT,
            IpcTimeoutPolicy::Fixed(timeout) => timeout,
        }
    }

    pub(super) fn response_timeout(&self, operation: &ApplicationOperation) -> Duration {
        match self.timeout_policy {
            IpcTimeoutPolicy::Fixed(timeout) => timeout,
            IpcTimeoutPolicy::Product => match operation {
                ApplicationOperation::ProfileAdd { .. } => IPC_PROFILE_ADD_TIMEOUT,
                ApplicationOperation::ProfileUse { .. }
                | ApplicationOperation::ProfileRemove { .. }
                | ApplicationOperation::ProxySelect { .. }
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
        self.subscribe_status_cancellable(
            after_sequence,
            connection_generation,
            &CancellationToken::default(),
        )
    }

    pub fn subscribe_status_cancellable(
        &self,
        after_sequence: Option<u64>,
        connection_generation: u64,
        cancellation: &CancellationToken,
    ) -> Result<StatusStream, ApplicationError> {
        let transport = self.open_stream_cancellable(
            RequestOperation::SubscribeStatus(StatusSubscriptionPayload { after_sequence }),
            connection_generation,
            cancellation,
        )?;
        Ok(StatusStream::new(transport))
    }

    pub fn follow_logs(
        &self,
        after_sequence: Option<u64>,
        connection_generation: u64,
    ) -> Result<LogStream, ApplicationError> {
        self.follow_logs_cancellable(
            after_sequence,
            connection_generation,
            &CancellationToken::default(),
        )
    }

    pub fn follow_logs_cancellable(
        &self,
        after_sequence: Option<u64>,
        connection_generation: u64,
        cancellation: &CancellationToken,
    ) -> Result<LogStream, ApplicationError> {
        let transport = self.open_stream_cancellable(
            RequestOperation::FollowLogs(LogSubscriptionPayload { after_sequence }),
            connection_generation,
            cancellation,
        )?;
        Ok(LogStream::new(transport, after_sequence))
    }

    pub fn log_tail(&self, after_sequence: Option<u64>) -> Result<LogTailV1, ApplicationError> {
        self.log_tail_cancellable(after_sequence, &CancellationToken::default())
    }

    pub fn log_tail_cancellable(
        &self,
        after_sequence: Option<u64>,
        cancellation: &CancellationToken,
    ) -> Result<LogTailV1, ApplicationError> {
        if cancellation.is_cancelled() {
            return Err(cancelled_operation(false));
        }
        let request_id = self.request_id();
        let request = IpcRequest::new(
            request_id,
            RequestOperation::LogTail(LogTailPayload { after_sequence }),
        );
        let stream = self.connect_cancellable(cancellation).map_err(|error| {
            if cancellation.is_cancelled() {
                cancelled_operation(false)
            } else {
                connect_error(error)
            }
        })?;
        let interrupt_stream = stream
            .try_clone()
            .map_err(|_| connect_error(io::Error::other("IPC stream clone failed")))?;
        let _cancellation_registration = cancellation.register_interrupt(move || {
            let _ = interrupt_stream.shutdown(Shutdown::Both);
        });
        let mut stream =
            DeadlineUnixStream::new(stream, self.stream_timeout()).map_err(connect_error)?;
        stream.begin_write().map_err(connect_error)?;
        write_frame(&mut stream, &request).map_err(|error| {
            if cancellation.is_cancelled() {
                cancelled_operation(false)
            } else {
                write_error(error)
            }
        })?;
        stream.begin_read().map_err(|error| {
            if cancellation.is_cancelled() {
                cancelled_operation(false)
            } else {
                connect_error(error)
            }
        })?;
        let response: IpcResponse = read_frame(&mut stream).map_err(|error| {
            if cancellation.is_cancelled() {
                cancelled_operation(false)
            } else {
                read_error(error)
            }
        })?;
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

    fn open_stream_cancellable(
        &self,
        operation: RequestOperation,
        connection_generation: u64,
        cancellation: &CancellationToken,
    ) -> Result<StreamTransport, ApplicationError> {
        if cancellation.is_cancelled() {
            return Err(cancelled_operation(false));
        }
        let request_id = self.request_id();
        let request = IpcRequest::new(request_id, operation);
        let stream = self.connect_cancellable(cancellation).map_err(|error| {
            if cancellation.is_cancelled() {
                cancelled_operation(false)
            } else {
                connect_error(error)
            }
        })?;
        let interrupt_stream = stream
            .try_clone()
            .map_err(|_| connect_error(io::Error::other("IPC stream clone failed")))?;
        let _cancellation_registration = cancellation.register_interrupt(move || {
            let _ = interrupt_stream.shutdown(Shutdown::Both);
        });
        let stream_cancellation = IpcStreamCancellation::new(
            stream
                .try_clone()
                .map_err(|_| connect_error(io::Error::other("IPC stream clone failed")))?,
        );
        let mut stream =
            DeadlineUnixStream::new(stream, self.stream_timeout()).map_err(connect_error)?;
        stream.begin_write().map_err(connect_error)?;
        write_frame(&mut stream, &request).map_err(|error| {
            if cancellation.is_cancelled() {
                cancelled_operation(false)
            } else {
                write_error(error)
            }
        })?;
        Ok(StreamTransport::new(
            stream,
            request_id,
            connection_generation,
            stream_cancellation,
        ))
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
        self.execute_with_cancellation(operation, &CancellationToken::default())
    }

    fn execute_cancellable(
        &self,
        operation: ApplicationOperation,
        cancellation: &CancellationToken,
    ) -> Result<ApplicationOutput, ApplicationError> {
        self.execute_with_cancellation(operation, cancellation)
    }
}

impl IpcClient {
    fn execute_with_cancellation(
        &self,
        operation: ApplicationOperation,
        cancellation: &CancellationToken,
    ) -> Result<ApplicationOutput, ApplicationError> {
        match operation {
            ApplicationOperation::ProfileList => self.execute_profile_list(cancellation),
            ApplicationOperation::ProxyList { group } => {
                self.execute_proxy_list(group, cancellation)
            }
            ApplicationOperation::RuleList => self.execute_rule_list(cancellation),
            operation => self.execute_once(operation, cancellation),
        }
    }

    fn execute_once(
        &self,
        operation: ApplicationOperation,
        cancellation: &CancellationToken,
    ) -> Result<ApplicationOutput, ApplicationError> {
        if cancellation.is_cancelled() {
            return Err(cancelled_operation(false));
        }
        let expected_output = ExpectedOutput::for_operation(&operation);
        let response_timeout = self.response_timeout(&operation);
        let may_commit = operation_may_commit(&operation);
        let request_id = self.request_id();
        let request = IpcRequest::new(request_id, request_operation(operation));
        let stream = self.connect_cancellable(cancellation).map_err(|error| {
            if cancellation.is_cancelled() {
                cancelled_operation(false)
            } else {
                connect_error(error)
            }
        })?;
        let interrupt_stream = stream
            .try_clone()
            .map_err(|_| connect_error(io::Error::other("IPC stream clone failed")))?;
        let _cancellation_registration = cancellation.register_interrupt(move || {
            let _ = interrupt_stream.shutdown(Shutdown::Both);
        });
        let mut stream =
            DeadlineUnixStream::new(stream, response_timeout).map_err(connect_error)?;
        stream.begin_write().map_err(connect_error)?;
        write_frame(&mut stream, &request).map_err(|error| {
            if cancellation.is_cancelled() {
                cancelled_operation(may_commit)
            } else {
                operation_write_error(error, may_commit)
            }
        })?;
        stream.begin_read().map_err(|error| {
            if cancellation.is_cancelled() {
                cancelled_operation(may_commit)
            } else {
                operation_read_setup_error(error, may_commit)
            }
        })?;
        let response: IpcResponse = read_frame(&mut stream).map_err(|error| {
            if cancellation.is_cancelled() {
                cancelled_operation(may_commit)
            } else {
                operation_read_error(error, may_commit)
            }
        })?;
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
        let output = decode_application_output(data)
            .map_err(|_| protocol_error("The IPC response data is invalid"))?;
        if expected_output.matches(&output) {
            Ok(output)
        } else {
            Err(protocol_error(
                "The IPC response output does not match the request",
            ))
        }
    }

    fn execute_rule_list(
        &self,
        cancellation: &CancellationToken,
    ) -> Result<ApplicationOutput, ApplicationError> {
        let mut offset = 0;
        let mut metadata = None;
        let mut rules = Vec::new();
        loop {
            let output =
                self.execute_once(ApplicationOperation::RuleListPage { offset }, cancellation)?;
            let page = match output {
                ApplicationOutput::RulePage(page) => page,
                ApplicationOutput::Rules(outcome) if offset == 0 => {
                    validate_complete_rule_list(&outcome)?;
                    return Ok(ApplicationOutput::Rules(outcome));
                }
                _ => {
                    return Err(protocol_error("The IPC Rule List page response is invalid"));
                }
            };
            validate_rule_page(&page, offset)?;

            let page_metadata = (page.initialized, page.revision, page.total);
            match metadata {
                None => {
                    rules.reserve(page.total);
                    metadata = Some(page_metadata);
                }
                Some(expected) if expected != page_metadata => {
                    return Err(protocol_error(
                        "The IPC Rule List changed while pages were being read",
                    ));
                }
                Some(_) => {}
            }

            rules.extend(page.rules);
            offset = rules.len();
            if offset == page.total {
                return Ok(ApplicationOutput::Rules(RuleListOutcome {
                    initialized: page.initialized,
                    revision: page.revision,
                    rules,
                }));
            }
        }
    }

    fn execute_profile_list(
        &self,
        cancellation: &CancellationToken,
    ) -> Result<ApplicationOutput, ApplicationError> {
        let mut offset = 0;
        let mut metadata = None;
        let mut profiles = Vec::new();
        loop {
            let output = self.execute_once(
                ApplicationOperation::ProfileListPage { offset },
                cancellation,
            )?;
            let page = match output {
                ApplicationOutput::ProfilePage(page) => page,
                ApplicationOutput::Profiles(outcome) if offset == 0 => {
                    if outcome.profiles.len() > PROFILE_COUNT_MAX {
                        return Err(protocol_error("The IPC Profile List response is invalid"));
                    }
                    return Ok(ApplicationOutput::Profiles(outcome));
                }
                _ => {
                    return Err(protocol_error(
                        "The IPC Profile List page response is invalid",
                    ));
                }
            };
            validate_profile_page(&page, offset)?;
            let page_metadata = (page.snapshot_id, page.total);
            match metadata {
                None => {
                    profiles.reserve(page.total);
                    metadata = Some(page_metadata);
                }
                Some(expected) if expected != page_metadata => {
                    return Err(protocol_error(
                        "The IPC Profile List changed while pages were being read",
                    ));
                }
                Some(_) => {}
            }
            profiles.extend(page.profiles);
            offset = profiles.len();
            if offset == page.total {
                return Ok(ApplicationOutput::Profiles(ProfileListOutcome { profiles }));
            }
        }
    }

    fn execute_proxy_list(
        &self,
        group: String,
        cancellation: &CancellationToken,
    ) -> Result<ApplicationOutput, ApplicationError> {
        let mut groups_offset = 0;
        let mut nodes_offset = 0;
        let mut metadata = None;
        let mut groups = Vec::new();
        let mut nodes = Vec::new();
        loop {
            let output = self.execute_once(
                ApplicationOperation::ProxyListPage {
                    group: group.clone(),
                    groups_offset,
                    nodes_offset,
                },
                cancellation,
            )?;
            let page = match output {
                ApplicationOutput::ProxyPage(page) => page,
                ApplicationOutput::Proxies(outcome) if groups_offset == 0 && nodes_offset == 0 => {
                    validate_complete_proxy_list(&outcome)?;
                    return Ok(ApplicationOutput::Proxies(outcome));
                }
                _ => {
                    return Err(protocol_error(
                        "The IPC Proxy List page response is invalid",
                    ));
                }
            };
            validate_proxy_page(&page, groups_offset, nodes_offset)?;
            let page_metadata = (
                page.snapshot_id,
                page.group.clone(),
                page.groups_total,
                page.nodes_total,
            );
            match &metadata {
                None => {
                    groups.reserve(page.groups_total);
                    nodes.reserve(page.nodes_total);
                    metadata = Some(page_metadata);
                }
                Some(expected) if expected != &page_metadata => {
                    return Err(protocol_error(
                        "The IPC Proxy List changed while pages were being read",
                    ));
                }
                Some(_) => {}
            }
            groups.extend(page.groups);
            nodes.extend(page.nodes);
            groups_offset = groups.len();
            nodes_offset = nodes.len();
            if groups_offset == page.groups_total && nodes_offset == page.nodes_total {
                return Ok(ApplicationOutput::Proxies(ProxyListOutcome {
                    group: page.group,
                    groups,
                    nodes,
                }));
            }
        }
    }
}

fn validate_profile_page(
    page: &ProfileListPageOutcome,
    expected_offset: usize,
) -> Result<(), ApplicationError> {
    if page.total > PROFILE_COUNT_MAX
        || page.offset != expected_offset
        || page
            .total
            .checked_sub(page.offset)
            .map(|remaining| remaining.min(IPC_LIST_PAGE_SIZE))
            != Some(page.profiles.len())
    {
        return Err(protocol_error(
            "The IPC Profile List page response is invalid",
        ));
    }
    Ok(())
}

fn validate_complete_proxy_list(outcome: &ProxyListOutcome) -> Result<(), ApplicationError> {
    if outcome.groups.len() > MAX_ACTIVE_NODES || outcome.nodes.len() > MAX_ACTIVE_NODES {
        return Err(protocol_error("The IPC Proxy List response is invalid"));
    }
    Ok(())
}

fn validate_proxy_page(
    page: &ProxyListPageOutcome,
    expected_groups_offset: usize,
    expected_nodes_offset: usize,
) -> Result<(), ApplicationError> {
    let groups_len = page
        .groups_total
        .checked_sub(page.groups_offset)
        .map(|remaining| remaining.min(IPC_LIST_PAGE_SIZE));
    let nodes_len = page
        .nodes_total
        .checked_sub(page.nodes_offset)
        .map(|remaining| remaining.min(IPC_LIST_PAGE_SIZE));
    if page.groups_total > MAX_ACTIVE_NODES
        || page.nodes_total > MAX_ACTIVE_NODES
        || page.groups_offset != expected_groups_offset
        || page.nodes_offset != expected_nodes_offset
        || groups_len != Some(page.groups.len())
        || nodes_len != Some(page.nodes.len())
    {
        return Err(protocol_error(
            "The IPC Proxy List page response is invalid",
        ));
    }
    Ok(())
}

fn validate_complete_rule_list(outcome: &RuleListOutcome) -> Result<(), ApplicationError> {
    if outcome.rules.len() > LOCAL_RULE_COUNT_MAX
        || (!outcome.initialized && (!outcome.rules.is_empty() || outcome.revision.is_some()))
        || (outcome.initialized && outcome.revision.is_none())
        || outcome
            .rules
            .iter()
            .enumerate()
            .any(|(index, rule)| rule.index != index)
    {
        return Err(protocol_error("The IPC Rule List response is invalid"));
    }
    Ok(())
}

fn validate_rule_page(
    page: &RuleListPageOutcome,
    expected_offset: usize,
) -> Result<(), ApplicationError> {
    let expected_len = page
        .total
        .checked_sub(page.offset)
        .map(|remaining| remaining.min(IPC_LIST_PAGE_SIZE));
    if page.total > LOCAL_RULE_COUNT_MAX
        || page.offset != expected_offset
        || expected_len != Some(page.rules.len())
        || (!page.initialized && (page.total != 0 || page.revision.is_some()))
        || (page.initialized && page.revision.is_none())
        || page
            .rules
            .iter()
            .enumerate()
            .any(|(relative, rule)| rule.index != page.offset + relative)
    {
        return Err(protocol_error("The IPC Rule List page response is invalid"));
    }
    Ok(())
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
        ApplicationOperation::ProfileList => {
            RequestOperation::ProfileListPage(ProfileListPagePayload { offset: 0 })
        }
        ApplicationOperation::ProfileListPage { offset } => {
            RequestOperation::ProfileListPage(ProfileListPagePayload { offset })
        }
        ApplicationOperation::ProfileUse { profile } => {
            RequestOperation::ProfileUse(ProfileSelectorPayload { profile })
        }
        ApplicationOperation::ProfileRemove { profile } => {
            RequestOperation::ProfileRemove(ProfileSelectorPayload { profile })
        }
        ApplicationOperation::ProxyList { group } => {
            RequestOperation::ProxyListPage(ProxyListPagePayload {
                group,
                groups_offset: 0,
                nodes_offset: 0,
            })
        }
        ApplicationOperation::ProxyListPage {
            group,
            groups_offset,
            nodes_offset,
        } => RequestOperation::ProxyListPage(ProxyListPagePayload {
            group,
            groups_offset,
            nodes_offset,
        }),
        ApplicationOperation::ProxySelect { group, node } => {
            RequestOperation::ProxySelect(ProxySelectPayload { group, node })
        }
        ApplicationOperation::LatencyList => RequestOperation::LatencyList(EmptyPayload {}),
        ApplicationOperation::LatencyShow { node } => {
            RequestOperation::LatencyShow(NodeSelectorPayload { node })
        }
        ApplicationOperation::RuleList => {
            RequestOperation::RuleListPage(RuleListPagePayload { offset: 0 })
        }
        ApplicationOperation::RuleListPage { offset } => {
            RequestOperation::RuleListPage(RuleListPagePayload { offset })
        }
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
