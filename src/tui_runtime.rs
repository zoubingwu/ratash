use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::io;
use std::panic::{self, AssertUnwindSafe, PanicHookInfo};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender, TryRecvError, TrySendError};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crossterm::event::{Event as CrosstermEvent, EventStream, KeyCode, KeyEventKind, KeyModifiers};
use futures_util::StreamExt;
use ratatui::Terminal;
use ratatui::backend::{Backend, CrosstermBackend};

use crate::application::{
    ApplicationClient, ApplicationOperation, ApplicationOutput, ProfileMutationAction,
    ProfileRefreshState, ProxyAvailability, ProxyGroupSummary, ProxyListOutcome, ProxyMemberKind,
};
pub use crate::cancellation::CancellationToken;
use crate::constants::{
    LOG_CAPACITY, MAX_ACTIVE_NODES, RECONNECT_INITIAL_BACKOFF, RECONNECT_MAX_BACKOFF,
};
use crate::domain::{ProxyGroupId, StatusSnapshot};
use crate::ipc::RequestId;
use crate::tui::{
    AppState, Command, ConnectionStatus, CrosstermControl, EventSource, FairEventInbox,
    FullViewSnapshot, InteractionMap, MutationSuccess, ProfileRow, ProxyGroupRow,
    ProxyGroupSnapshot, ProxyRow, TerminalControl, TerminalSession, UiEvent, ViewLogRecord,
    from_crossterm_event, render, status_requires_snapshot_refresh, update,
};

const COMMAND_QUEUE_CAPACITY: usize = 32;
const COMMAND_WORKER_COUNT: usize = 2;
const COMMAND_RESULTS_PER_ROUND: usize = 8;
const STREAM_EVENTS_PER_ROUND: usize = 64;
const TERMINAL_EVENTS_PER_ROUND: usize = 8;
const TOAST_MAX_CHARACTERS: usize = 256;
const SNAPSHOT_REFRESH_COALESCE: Duration = Duration::from_millis(100);
const SNAPSHOT_REFRESH_MIN_INTERVAL: Duration = Duration::from_millis(500);
const PROFILE_REFRESH_SETTLE_DELAY: Duration = Duration::from_secs(1);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StatusInterfaceErrorKind {
    InvalidConfiguration,
    Snapshot,
    Stream,
    Command,
    CommandQueue,
    TerminalInput,
    TerminalSetup,
    Render,
    Signal,
}

#[derive(Clone, Eq, PartialEq)]
pub struct StatusInterfaceError {
    pub kind: StatusInterfaceErrorKind,
    message: String,
}

impl fmt::Debug for StatusInterfaceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StatusInterfaceError")
            .field("kind", &self.kind)
            .field("message_bytes", &self.message.len())
            .finish()
    }
}

impl StatusInterfaceError {
    pub fn new(kind: StatusInterfaceErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }
}

impl fmt::Display for StatusInterfaceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for StatusInterfaceError {}

// -----------------------------------------------------------------------------
// Injectable application and event boundaries
// -----------------------------------------------------------------------------

#[derive(Clone, Debug, Default)]
pub struct RuntimeWaker {
    inner: Arc<RuntimeWakeState>,
}

#[derive(Debug, Default)]
struct RuntimeWakeState {
    revision: Mutex<u64>,
    changed: Condvar,
}

impl RuntimeWaker {
    pub fn wake(&self) {
        let mut revision = self
            .inner
            .revision
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *revision = revision.wrapping_add(1);
        self.inner.changed.notify_one();
    }
}

pub trait RuntimeWaiter {
    fn checkpoint(&self) -> u64;
    fn wait(&self, checkpoint: u64, timeout: Option<Duration>);
}

impl RuntimeWaiter for RuntimeWaker {
    fn checkpoint(&self) -> u64 {
        *self
            .inner
            .revision
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn wait(&self, checkpoint: u64, timeout: Option<Duration>) {
        let revision = self
            .inner
            .revision
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if *revision != checkpoint {
            return;
        }
        match timeout {
            Some(timeout) => {
                let _ = self
                    .inner
                    .changed
                    .wait_timeout_while(revision, timeout, |revision| *revision == checkpoint)
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
            }
            None => {
                let _guard = self
                    .inner
                    .changed
                    .wait_while(revision, |revision| *revision == checkpoint)
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
            }
        }
    }
}

pub trait FullSnapshotSource: Send + Sync {
    fn fetch_full_snapshot(
        &self,
        connection_generation: u64,
        cancellation: &CancellationToken,
    ) -> Result<FullViewSnapshot, StatusInterfaceError>;

    fn refresh_view_snapshot(
        &self,
        connection_generation: u64,
        cancellation: &CancellationToken,
    ) -> Result<FullViewSnapshot, StatusInterfaceError> {
        self.fetch_full_snapshot(connection_generation, cancellation)
    }

    fn fetch_proxy_group(
        &self,
        _group: &str,
        _connection_generation: u64,
        _cancellation: &CancellationToken,
    ) -> Result<ProxyGroupSnapshot, StatusInterfaceError> {
        Err(StatusInterfaceError::new(
            StatusInterfaceErrorKind::InvalidConfiguration,
            "The snapshot source does not support Proxy Group loading",
        ))
    }
}

#[derive(Clone, Debug)]
pub struct LogTail {
    pub records: Vec<ViewLogRecord>,
    pub gap: bool,
    pub dropped_total: u64,
}

#[derive(Clone, Debug)]
pub enum StatusLogEvent {
    Status {
        connection_generation: u64,
        status: Box<StatusSnapshot>,
    },
    Logs {
        connection_generation: u64,
        records: Vec<ViewLogRecord>,
        gap: bool,
        dropped_total: u64,
    },
    Disconnected {
        connection_generation: u64,
    },
}

impl StatusLogEvent {
    fn into_ui_event(self) -> UiEvent {
        match self {
            Self::Status {
                connection_generation,
                status,
            } => UiEvent::StatusSnapshot {
                connection_generation,
                status: *status,
            },
            Self::Logs {
                connection_generation,
                records,
                gap,
                dropped_total,
            } => UiEvent::LogBatch {
                connection_generation,
                records: bounded_log_records(records),
                gap,
                dropped_total,
            },
            Self::Disconnected {
                connection_generation,
            } => UiEvent::Disconnected {
                connection_generation,
            },
        }
    }
}

pub trait StatusLogEventSource: Send + Sync {
    fn install_waker(&self, _waker: RuntimeWaker) {}

    fn connect(
        &self,
        connection_generation: u64,
        cancellation: &CancellationToken,
    ) -> Result<(), StatusInterfaceError>;

    /// Returns immediately when no event is ready.
    fn try_next(&self) -> Result<Option<StatusLogEvent>, StatusInterfaceError>;

    fn fetch_log_tail(
        &self,
        connection_generation: u64,
        after_sequence: Option<u64>,
        cancellation: &CancellationToken,
    ) -> Result<LogTail, StatusInterfaceError>;

    /// Closes the matching generation and is safe to call repeatedly.
    fn disconnect(&self, connection_generation: u64);
}

pub trait UiCommandExecutor: Send + Sync {
    fn execute(
        &self,
        operation: ApplicationOperation,
        cancellation: &CancellationToken,
    ) -> Result<String, StatusInterfaceError>;
}

pub struct ApplicationCommandExecutor<C: ?Sized> {
    client: Arc<C>,
}

impl<C: ?Sized> ApplicationCommandExecutor<C> {
    #[must_use]
    pub fn new(client: Arc<C>) -> Self {
        Self { client }
    }
}

impl<C> UiCommandExecutor for ApplicationCommandExecutor<C>
where
    C: ApplicationClient + Send + Sync + ?Sized,
{
    fn execute(
        &self,
        operation: ApplicationOperation,
        cancellation: &CancellationToken,
    ) -> Result<String, StatusInterfaceError> {
        if cancellation.is_cancelled() {
            return Err(cancelled_error());
        }
        let output = self
            .client
            .execute_cancellable(operation, cancellation)
            .map_err(|error| {
                StatusInterfaceError::new(StatusInterfaceErrorKind::Command, error.message)
            })?;
        if cancellation.is_cancelled() {
            return Err(cancelled_error());
        }
        match output {
            ApplicationOutput::ProfileMutation(outcome)
                if outcome.action == ProfileMutationAction::Activated =>
            {
                Ok(short_terminal_text(&format!(
                    "Profile {} is active",
                    outcome.profile.name
                )))
            }
            ApplicationOutput::ProxySelection(outcome) => Ok(short_terminal_text(&format!(
                "Selected {}",
                outcome.selected_node.name
            ))),
            _ => Err(StatusInterfaceError::new(
                StatusInterfaceErrorKind::Command,
                "The application returned an unexpected command result",
            )),
        }
    }
}

fn cancelled_error() -> StatusInterfaceError {
    StatusInterfaceError::new(
        StatusInterfaceErrorKind::Command,
        "The command was cancelled",
    )
}

pub struct ApplicationSnapshotSource<C: ?Sized, E: ?Sized> {
    client: Arc<C>,
    events: Arc<E>,
}

impl<C: ?Sized, E: ?Sized> ApplicationSnapshotSource<C, E> {
    #[must_use]
    pub fn new(client: Arc<C>, events: Arc<E>) -> Self {
        Self { client, events }
    }
}

impl<C, E> ApplicationSnapshotSource<C, E>
where
    C: ApplicationClient + Send + Sync + ?Sized,
    E: StatusLogEventSource + ?Sized,
{
    fn fetch_snapshot(
        &self,
        connection_generation: u64,
        cancellation: &CancellationToken,
        include_logs: bool,
    ) -> Result<FullViewSnapshot, StatusInterfaceError> {
        check_snapshot_cancellation(cancellation)?;
        let status = match self
            .client
            .execute_cancellable(ApplicationOperation::GetStatus, cancellation)
            .map_err(snapshot_application_error)?
        {
            ApplicationOutput::Status(status) => status,
            _ => return Err(unexpected_snapshot_output("status")),
        };

        check_snapshot_cancellation(cancellation)?;
        let profiles = match self
            .client
            .execute_cancellable(ApplicationOperation::ProfileList, cancellation)
            .map_err(snapshot_application_error)?
        {
            ApplicationOutput::Profiles(outcome) => outcome
                .profiles
                .into_iter()
                .map(|profile| ProfileRow {
                    id: profile.id,
                    name: profile.name,
                    active: profile.active,
                    fresh: profile.refresh_state == ProfileRefreshState::Fresh,
                    last_success_at_unix_ms: profile.last_success_at_unix_ms,
                    next_refresh_at_unix_ms: profile.next_refresh_at_unix_ms,
                    error: profile.last_error.map(|error| error.message),
                })
                .collect(),
            _ => return Err(unexpected_snapshot_output("Profile list")),
        };

        check_snapshot_cancellation(cancellation)?;
        let (proxy_groups, proxies) = if let Some(group) = status.primary_proxy_group.clone() {
            match self
                .client
                .execute_cancellable(
                    ApplicationOperation::ProxyList {
                        group,
                    },
                    cancellation,
                )
                .map_err(snapshot_application_error)?
            {
                ApplicationOutput::Proxies(outcome) => {
                    let snapshot = proxy_group_snapshot(outcome);
                    (snapshot.groups, snapshot.proxies)
                }
                _ => return Err(unexpected_snapshot_output("Proxy list")),
            }
        } else {
            (Vec::new(), Vec::new())
        };

        let tail = if include_logs {
            check_snapshot_cancellation(cancellation)?;
            self.events
                .fetch_log_tail(connection_generation, None, cancellation)?
        } else {
            LogTail {
                records: Vec::new(),
                gap: false,
                dropped_total: 0,
            }
        };
        Ok(FullViewSnapshot {
            status,
            proxy_groups,
            proxies,
            profiles,
            logs: bounded_log_records(tail.records),
            dropped_logs: tail.dropped_total,
        })
    }
}

impl<C, E> FullSnapshotSource for ApplicationSnapshotSource<C, E>
where
    C: ApplicationClient + Send + Sync + ?Sized,
    E: StatusLogEventSource + ?Sized,
{
    fn fetch_full_snapshot(
        &self,
        connection_generation: u64,
        cancellation: &CancellationToken,
    ) -> Result<FullViewSnapshot, StatusInterfaceError> {
        self.fetch_snapshot(connection_generation, cancellation, true)
    }

    fn refresh_view_snapshot(
        &self,
        connection_generation: u64,
        cancellation: &CancellationToken,
    ) -> Result<FullViewSnapshot, StatusInterfaceError> {
        self.fetch_snapshot(connection_generation, cancellation, false)
    }

    fn fetch_proxy_group(
        &self,
        group: &str,
        _connection_generation: u64,
        cancellation: &CancellationToken,
    ) -> Result<ProxyGroupSnapshot, StatusInterfaceError> {
        check_snapshot_cancellation(cancellation)?;
        match self
            .client
            .execute_cancellable(
                ApplicationOperation::ProxyList {
                    group: group.to_owned(),
                },
                cancellation,
            )
            .map_err(snapshot_application_error)?
        {
            ApplicationOutput::Proxies(outcome) => Ok(proxy_group_snapshot(outcome)),
            _ => Err(unexpected_snapshot_output("Proxy list")),
        }
    }
}

fn proxy_group_snapshot(outcome: ProxyListOutcome) -> ProxyGroupSnapshot {
    let ProxyListOutcome {
        group,
        groups,
        nodes,
    } = outcome;
    let group_id = group.id.clone();
    let group_name = group.name.clone();
    let current_group = proxy_group_row(group);
    let mut group_rows = groups
        .into_iter()
        .filter(|group| group.selectable)
        .take(MAX_ACTIVE_NODES)
        .map(proxy_group_row)
        .collect::<Vec<_>>();
    if group_rows.is_empty() {
        group_rows.push(current_group.clone());
    }
    let proxies = nodes
        .into_iter()
        .take(MAX_ACTIVE_NODES)
        .map(|node| ProxyRow {
            group_id: group_id.clone(),
            group: group_name.clone(),
            node_id: node.id,
            name: node.name,
            node_type: node
                .proxy_type
                .unwrap_or_else(|| proxy_member_kind_title(node.member_kind).to_owned()),
            available: node.availability == ProxyAvailability::Available,
            selected: node.selected,
            delay_ms: node.delay_ms,
            sampled_at_unix_ms: node.sampled_at_unix_ms,
            freshness: node.freshness,
            probe_status: node.probe_status,
        })
        .collect();
    ProxyGroupSnapshot {
        group: current_group,
        groups: group_rows,
        proxies,
    }
}

fn proxy_group_row(group: ProxyGroupSummary) -> ProxyGroupRow {
    ProxyGroupRow {
        id: group.id,
        name: group.name,
        proxy_type: group.proxy_type,
        selected_node: group.selected_node.map(|node| node.name),
    }
}

fn proxy_member_kind_title(kind: ProxyMemberKind) -> &'static str {
    match kind {
        ProxyMemberKind::Node => "node",
        ProxyMemberKind::Group => "group",
        ProxyMemberKind::Missing => "missing",
        ProxyMemberKind::Ambiguous => "ambiguous",
        ProxyMemberKind::ProviderUnavailable => "provider_unavailable",
    }
}

fn check_snapshot_cancellation(
    cancellation: &CancellationToken,
) -> Result<(), StatusInterfaceError> {
    if cancellation.is_cancelled() {
        Err(StatusInterfaceError::new(
            StatusInterfaceErrorKind::Snapshot,
            "The snapshot request was cancelled",
        ))
    } else {
        Ok(())
    }
}

fn snapshot_application_error(error: crate::application::ApplicationError) -> StatusInterfaceError {
    StatusInterfaceError::new(StatusInterfaceErrorKind::Snapshot, error.message)
}

fn unexpected_snapshot_output(resource: &str) -> StatusInterfaceError {
    StatusInterfaceError::new(
        StatusInterfaceErrorKind::Snapshot,
        format!("The application returned an unexpected {resource} result"),
    )
}

#[derive(Clone)]
pub struct StatusInterfaceSources {
    pub snapshots: Arc<dyn FullSnapshotSource>,
    pub events: Arc<dyn StatusLogEventSource>,
    pub commands: Arc<dyn UiCommandExecutor>,
}

impl StatusInterfaceSources {
    #[must_use]
    pub fn from_application<C, E>(client: Arc<C>, events: Arc<E>) -> Self
    where
        C: ApplicationClient + Send + Sync + 'static,
        E: StatusLogEventSource + 'static,
    {
        Self {
            snapshots: Arc::new(ApplicationSnapshotSource::new(
                Arc::clone(&client),
                Arc::clone(&events),
            )),
            events,
            commands: Arc::new(ApplicationCommandExecutor::new(client)),
        }
    }
}

// -----------------------------------------------------------------------------
// Typed command dispatch
// -----------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub struct DispatchedEvent {
    pub source: EventSource,
    pub event: UiEvent,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CommandDispatchError;

impl fmt::Display for CommandDispatchError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("The Status Interface command queue is unavailable")
    }
}

impl std::error::Error for CommandDispatchError {}

pub trait CommandDispatcher {
    fn install_waker(&mut self, _waker: RuntimeWaker) {}

    fn submit(&mut self, command: Command) -> Result<(), CommandDispatchError>;
    fn cancel(&mut self, request_id: RequestId);
    fn cancel_all(&mut self);
    fn try_next(&mut self) -> Result<Option<DispatchedEvent>, CommandDispatchError>;
    fn shutdown(&mut self);
}

struct CommandWork {
    command: Command,
    cancellation: CancellationToken,
    task_id: u64,
    request_id: Option<RequestId>,
}

enum WorkerMessage {
    Run(CommandWork),
    Stop,
}

pub struct BackgroundCommandDispatcher {
    sender: SyncSender<WorkerMessage>,
    receiver: Receiver<DispatchedEvent>,
    cancellations: Arc<Mutex<CancellationRegistry>>,
    mutations: Arc<Mutex<MutationDispatchState>>,
    stopping: Arc<AtomicBool>,
    wake: Arc<Mutex<Option<RuntimeWaker>>>,
    worker_wait_returns: Arc<AtomicU64>,
    workers: Vec<JoinHandle<()>>,
}

struct CancellationRegistry {
    next_task_id: u64,
    tasks: HashMap<u64, CancellationToken>,
    requests: HashMap<RequestId, u64>,
}

#[derive(Default)]
struct MutationDispatchState {
    active_task_id: Option<u64>,
    pending: Option<CommandWork>,
}

struct CommandWorkerContext {
    receiver: Arc<Mutex<Receiver<WorkerMessage>>>,
    result_sender: SyncSender<DispatchedEvent>,
    cancellations: Arc<Mutex<CancellationRegistry>>,
    mutations: Arc<Mutex<MutationDispatchState>>,
    stopping: Arc<AtomicBool>,
    wake: Arc<Mutex<Option<RuntimeWaker>>>,
    worker_wait_returns: Arc<AtomicU64>,
    sources: StatusInterfaceSources,
}

impl CancellationRegistry {
    fn new() -> Self {
        Self {
            next_task_id: 1,
            tasks: HashMap::new(),
            requests: HashMap::new(),
        }
    }

    fn register(&mut self, request_id: Option<RequestId>) -> (u64, CancellationToken) {
        let task_id = self.next_task_id;
        self.next_task_id = self.next_task_id.wrapping_add(1).max(1);
        let cancellation = CancellationToken::default();
        self.tasks.insert(task_id, cancellation.clone());
        if let Some(request_id) = request_id {
            self.requests.insert(request_id, task_id);
        }
        (task_id, cancellation)
    }

    fn remove(&mut self, task_id: u64, request_id: Option<RequestId>) {
        self.tasks.remove(&task_id);
        if let Some(request_id) = request_id
            && self.requests.get(&request_id) == Some(&task_id)
        {
            self.requests.remove(&request_id);
        }
    }

    fn cancel_request(&self, request_id: RequestId) {
        if let Some(cancellation) = self
            .requests
            .get(&request_id)
            .and_then(|task_id| self.tasks.get(task_id))
        {
            cancellation.cancel();
        }
    }

    fn cancel_all(&self) {
        for cancellation in self.tasks.values() {
            cancellation.cancel();
        }
    }
}

impl BackgroundCommandDispatcher {
    pub fn new(sources: StatusInterfaceSources) -> Result<Self, StatusInterfaceError> {
        Self::with_worker_count(sources, COMMAND_WORKER_COUNT)
    }

    fn with_worker_count(
        sources: StatusInterfaceSources,
        worker_count: usize,
    ) -> Result<Self, StatusInterfaceError> {
        if worker_count == 0 {
            return Err(StatusInterfaceError::new(
                StatusInterfaceErrorKind::InvalidConfiguration,
                "The Status Interface requires at least one command worker",
            ));
        }
        let (sender, work_receiver) = mpsc::sync_channel(COMMAND_QUEUE_CAPACITY);
        let work_receiver = Arc::new(Mutex::new(work_receiver));
        let (result_sender, receiver) = mpsc::sync_channel(crate::tui::EVENT_SOURCE_CAPACITY);
        let cancellations = Arc::new(Mutex::new(CancellationRegistry::new()));
        let mutations = Arc::new(Mutex::new(MutationDispatchState::default()));
        let stopping = Arc::new(AtomicBool::new(false));
        let wake = Arc::new(Mutex::new(None));
        let worker_wait_returns = Arc::new(AtomicU64::new(0));
        let mut workers: Vec<JoinHandle<()>> = Vec::with_capacity(worker_count);
        for index in 0..worker_count {
            let receiver = Arc::clone(&work_receiver);
            let result_sender = result_sender.clone();
            let cancellations = Arc::clone(&cancellations);
            let mutations = Arc::clone(&mutations);
            let worker_stopping = Arc::clone(&stopping);
            let worker_wake = Arc::clone(&wake);
            let worker_wait_returns = Arc::clone(&worker_wait_returns);
            let worker_sources = sources.clone();
            let worker = thread::Builder::new()
                .name(format!("hopash-tui-command-{index}"))
                .spawn(move || {
                    command_worker(CommandWorkerContext {
                        receiver,
                        result_sender,
                        cancellations,
                        mutations,
                        stopping: worker_stopping,
                        wake: worker_wake,
                        worker_wait_returns,
                        sources: worker_sources,
                    });
                })
                .map_err(|_| {
                    stopping.store(true, Ordering::Release);
                    for _ in 0..workers.len() {
                        let _ = sender.try_send(WorkerMessage::Stop);
                    }
                    for worker in workers.drain(..) {
                        let _ = worker.join();
                    }
                    StatusInterfaceError::new(
                        StatusInterfaceErrorKind::CommandQueue,
                        "The Status Interface command worker could not start",
                    )
                })?;
            workers.push(worker);
        }
        Ok(Self {
            sender,
            receiver,
            cancellations,
            mutations,
            stopping,
            wake,
            worker_wait_returns,
            workers,
        })
    }

    #[doc(hidden)]
    #[must_use]
    pub fn worker_wait_return_count(&self) -> u64 {
        self.worker_wait_returns.load(Ordering::Acquire)
    }
}

impl CommandDispatcher for BackgroundCommandDispatcher {
    fn install_waker(&mut self, waker: RuntimeWaker) {
        *self
            .wake
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(waker);
    }

    fn submit(&mut self, command: Command) -> Result<(), CommandDispatchError> {
        if self.stopping.load(Ordering::Acquire) {
            return Err(CommandDispatchError);
        }
        if let Command::Cancel { request_id } = command {
            self.cancel(request_id);
            return Ok(());
        }
        let request_id = command_request_id(&command);
        let (task_id, cancellation) = self
            .cancellations
            .lock()
            .map_err(|_| CommandDispatchError)?
            .register(request_id);
        let work = CommandWork {
            command,
            cancellation,
            task_id,
            request_id,
        };
        if is_mutation_command(&work.command) {
            return self.submit_mutation(work);
        }
        match self.sender.try_send(WorkerMessage::Run(work)) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(WorkerMessage::Run(work)))
            | Err(TrySendError::Disconnected(WorkerMessage::Run(work))) => {
                if let Ok(mut cancellations) = self.cancellations.lock() {
                    cancellations.remove(work.task_id, work.request_id);
                }
                Err(CommandDispatchError)
            }
            Err(TrySendError::Full(WorkerMessage::Stop))
            | Err(TrySendError::Disconnected(WorkerMessage::Stop)) => Err(CommandDispatchError),
        }
    }

    fn cancel(&mut self, request_id: RequestId) {
        if let Ok(cancellations) = self.cancellations.lock() {
            cancellations.cancel_request(request_id);
        }
    }

    fn cancel_all(&mut self) {
        if let Ok(cancellations) = self.cancellations.lock() {
            cancellations.cancel_all();
        }
    }

    fn try_next(&mut self) -> Result<Option<DispatchedEvent>, CommandDispatchError> {
        match self.receiver.try_recv() {
            Ok(event) => Ok(Some(event)),
            Err(TryRecvError::Empty) => Ok(None),
            Err(TryRecvError::Disconnected) => Err(CommandDispatchError),
        }
    }

    fn shutdown(&mut self) {
        if self.stopping.swap(true, Ordering::AcqRel) {
            return;
        }
        self.cancel_all();
        for _ in 0..self.workers.len() {
            let _ = self.sender.try_send(WorkerMessage::Stop);
        }
        for worker in self.workers.drain(..) {
            let _ = worker.join();
        }
    }
}

impl BackgroundCommandDispatcher {
    fn submit_mutation(&mut self, work: CommandWork) -> Result<(), CommandDispatchError> {
        let task_id = work.task_id;
        let mut work = Some(work);
        let (active_task_id, replaced) = {
            let mut mutations = self.mutations.lock().map_err(|_| CommandDispatchError)?;
            if let Some(active_task_id) = mutations.active_task_id {
                (
                    Some(active_task_id),
                    mutations.pending.replace(
                        work.take()
                            .expect("mutation work must be available before dispatch"),
                    ),
                )
            } else {
                mutations.active_task_id = Some(task_id);
                (None, None)
            }
        };
        if let Some(active_task_id) = active_task_id {
            if let Ok(mut cancellations) = self.cancellations.lock() {
                if let Some(cancellation) = cancellations.tasks.get(&active_task_id) {
                    cancellation.cancel();
                }
                if let Some(replaced) = replaced {
                    replaced.cancellation.cancel();
                    cancellations.remove(replaced.task_id, replaced.request_id);
                }
            }
            return Ok(());
        }
        let work = work.expect("active mutation work must be available before queueing");
        match self.sender.try_send(WorkerMessage::Run(work)) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(WorkerMessage::Run(work)))
            | Err(TrySendError::Disconnected(WorkerMessage::Run(work))) => {
                if let Ok(mut mutations) = self.mutations.lock()
                    && mutations.active_task_id == Some(work.task_id)
                {
                    mutations.active_task_id = None;
                }
                if let Ok(mut cancellations) = self.cancellations.lock() {
                    cancellations.remove(work.task_id, work.request_id);
                }
                Err(CommandDispatchError)
            }
            Err(TrySendError::Full(WorkerMessage::Stop))
            | Err(TrySendError::Disconnected(WorkerMessage::Stop)) => Err(CommandDispatchError),
        }
    }
}

impl Drop for BackgroundCommandDispatcher {
    fn drop(&mut self) {
        self.shutdown();
    }
}

fn command_worker(context: CommandWorkerContext) {
    let mut promoted = None;
    while !context.stopping.load(Ordering::Acquire) {
        let work = match promoted.take() {
            Some(work) => work,
            None => {
                let message = match context.receiver.lock() {
                    Ok(receiver) => receiver.recv(),
                    Err(_) => return,
                };
                context.worker_wait_returns.fetch_add(1, Ordering::Relaxed);
                match message {
                    Ok(WorkerMessage::Run(work)) => work,
                    Ok(WorkerMessage::Stop) | Err(_) => return,
                }
            }
        };
        if work.cancellation.is_cancelled() {
            remove_cancellation(&context.cancellations, work.task_id, work.request_id);
            promoted = complete_mutation(&context.mutations, work.task_id);
            continue;
        }
        let result = execute_work(
            &work,
            &context.sources.snapshots,
            &context.sources.events,
            &context.sources.commands,
        );
        remove_cancellation(&context.cancellations, work.task_id, work.request_id);
        if !work.cancellation.is_cancelled() && context.result_sender.try_send(result).is_ok() {
            wake_runtime(&context.wake);
        }
        promoted = complete_mutation(&context.mutations, work.task_id);
    }
}

fn complete_mutation(
    mutations: &Mutex<MutationDispatchState>,
    task_id: u64,
) -> Option<CommandWork> {
    let mut mutations = mutations
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if mutations.active_task_id != Some(task_id) {
        return None;
    }
    mutations.active_task_id = None;
    let next = mutations.pending.take();
    if let Some(next) = &next {
        mutations.active_task_id = Some(next.task_id);
    }
    next
}

fn is_mutation_command(command: &Command) -> bool {
    matches!(
        command,
        Command::ActivateProfile { .. } | Command::SelectNode { .. }
    )
}

fn execute_work(
    work: &CommandWork,
    snapshots: &Arc<dyn FullSnapshotSource>,
    events: &Arc<dyn StatusLogEventSource>,
    commands: &Arc<dyn UiCommandExecutor>,
) -> DispatchedEvent {
    match &work.command {
        Command::Connect {
            connection_generation,
        } => {
            let result = events
                .connect(*connection_generation, &work.cancellation)
                .and_then(|()| {
                    snapshots.fetch_full_snapshot(*connection_generation, &work.cancellation)
                });
            match result {
                Ok(snapshot) => DispatchedEvent {
                    source: EventSource::CommandResult,
                    event: UiEvent::Connected {
                        connection_generation: *connection_generation,
                        snapshot,
                    },
                },
                Err(_) => {
                    events.disconnect(*connection_generation);
                    DispatchedEvent {
                        source: EventSource::CommandResult,
                        event: UiEvent::Disconnected {
                            connection_generation: *connection_generation,
                        },
                    }
                }
            }
        }
        Command::ActivateProfile {
            request_id,
            connection_generation,
            profile_id,
        } => mutation_result(
            *request_id,
            *connection_generation,
            ApplicationOperation::ProfileUse {
                profile: profile_id.to_string(),
            },
            &work.cancellation,
            commands,
        ),
        Command::SelectNode {
            request_id,
            connection_generation,
            group_id,
            node_id,
        } => proxy_selection_result(
            *request_id,
            *connection_generation,
            group_id,
            node_id,
            &work.cancellation,
            commands,
        ),
        Command::FetchProxyGroup {
            request_id,
            connection_generation,
            group_id,
        } => {
            let result = snapshots
                .fetch_proxy_group(
                    group_id.as_str(),
                    *connection_generation,
                    &work.cancellation,
                )
                .map_err(|error| short_terminal_text(&error.to_string()));
            DispatchedEvent {
                source: EventSource::CommandResult,
                event: UiEvent::ProxyGroupLoaded {
                    request_id: *request_id,
                    connection_generation: *connection_generation,
                    result,
                },
            }
        }
        Command::FetchLogTail {
            connection_generation,
            after_sequence,
        } => {
            match events.fetch_log_tail(*connection_generation, *after_sequence, &work.cancellation)
            {
                Ok(tail) => DispatchedEvent {
                    source: EventSource::Telemetry,
                    event: UiEvent::LogBatch {
                        connection_generation: *connection_generation,
                        records: bounded_log_records(tail.records),
                        gap: tail.gap,
                        dropped_total: tail.dropped_total,
                    },
                },
                Err(_) => DispatchedEvent {
                    source: EventSource::CommandResult,
                    event: UiEvent::Disconnected {
                        connection_generation: *connection_generation,
                    },
                },
            }
        }
        Command::RefreshSnapshot {
            connection_generation,
            base_view_revision,
            base_status_revision,
        } => match snapshots.refresh_view_snapshot(*connection_generation, &work.cancellation) {
            Ok(snapshot) => DispatchedEvent {
                source: EventSource::CommandResult,
                event: UiEvent::SnapshotRefreshed {
                    connection_generation: *connection_generation,
                    base_view_revision: *base_view_revision,
                    base_status_revision: *base_status_revision,
                    snapshot,
                },
            },
            Err(_) => DispatchedEvent {
                source: EventSource::CommandResult,
                event: UiEvent::SnapshotRefreshFailed {
                    connection_generation: *connection_generation,
                    base_view_revision: *base_view_revision,
                },
            },
        },
        Command::ScheduleReconnect {
            connection_generation,
        } => DispatchedEvent {
            source: EventSource::Deadline,
            event: UiEvent::ReconnectDeadline {
                connection_generation: *connection_generation,
            },
        },
        Command::Cancel { request_id } => command_result(
            *request_id,
            0,
            Err(StatusInterfaceError::new(
                StatusInterfaceErrorKind::Command,
                "The command was cancelled",
            )),
        ),
    }
}

fn command_result(
    request_id: RequestId,
    connection_generation: u64,
    result: Result<MutationSuccess, StatusInterfaceError>,
) -> DispatchedEvent {
    DispatchedEvent {
        source: EventSource::CommandResult,
        event: UiEvent::CommandResult {
            request_id,
            connection_generation,
            result: result.map_err(|error| short_terminal_text(&error.to_string())),
        },
    }
}

fn mutation_result(
    request_id: RequestId,
    connection_generation: u64,
    operation: ApplicationOperation,
    cancellation: &CancellationToken,
    commands: &Arc<dyn UiCommandExecutor>,
) -> DispatchedEvent {
    let result = commands
        .execute(operation, cancellation)
        .map(|message| MutationSuccess {
            message: short_terminal_text(&message),
        });
    command_result(request_id, connection_generation, result)
}

fn proxy_selection_result(
    request_id: RequestId,
    connection_generation: u64,
    group_id: &ProxyGroupId,
    node_id: &crate::domain::NodeRecordId,
    cancellation: &CancellationToken,
    commands: &Arc<dyn UiCommandExecutor>,
) -> DispatchedEvent {
    let result = commands
        .execute(
            ApplicationOperation::ProxySelect {
                group: group_id.as_str().to_owned(),
                node: node_id.as_str().to_owned(),
            },
            cancellation,
        )
        .map(|message| MutationSuccess {
            message: short_terminal_text(&message),
        });
    command_result(request_id, connection_generation, result)
}

fn command_request_id(command: &Command) -> Option<RequestId> {
    match command {
        Command::ActivateProfile { request_id, .. }
        | Command::SelectNode { request_id, .. }
        | Command::FetchProxyGroup { request_id, .. }
        | Command::Cancel { request_id } => Some(*request_id),
        Command::Connect { .. }
        | Command::ScheduleReconnect { .. }
        | Command::FetchLogTail { .. }
        | Command::RefreshSnapshot { .. } => None,
    }
}

fn remove_cancellation(
    cancellations: &Arc<Mutex<CancellationRegistry>>,
    task_id: u64,
    request_id: Option<RequestId>,
) {
    if let Ok(mut cancellations) = cancellations.lock() {
        cancellations.remove(task_id, request_id);
    }
}

fn bounded_log_records(records: Vec<ViewLogRecord>) -> Vec<ViewLogRecord> {
    let skip = records.len().saturating_sub(LOG_CAPACITY);
    records.into_iter().skip(skip).collect()
}

fn short_terminal_text(value: &str) -> String {
    value
        .chars()
        .map(|character| {
            if character.is_control() {
                ' '
            } else {
                character
            }
        })
        .take(TOAST_MAX_CHARACTERS)
        .collect()
}

// -----------------------------------------------------------------------------
// Clock, signal, terminal input, and rendering seams
// -----------------------------------------------------------------------------

pub trait RuntimeClock {
    fn now(&self) -> Duration;
    fn now_unix_ms(&self) -> u64;
}

#[derive(Debug)]
pub struct MonotonicClock {
    started_at: Instant,
    started_at_unix_ms: u64,
}

impl Default for MonotonicClock {
    fn default() -> Self {
        Self {
            started_at: Instant::now(),
            started_at_unix_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis()
                .try_into()
                .unwrap_or(u64::MAX),
        }
    }
}

impl RuntimeClock for MonotonicClock {
    fn now(&self) -> Duration {
        self.started_at.elapsed()
    }

    fn now_unix_ms(&self) -> u64 {
        self.started_at_unix_ms.saturating_add(
            self.started_at
                .elapsed()
                .as_millis()
                .try_into()
                .unwrap_or(u64::MAX),
        )
    }
}

pub trait ShutdownSignal {
    fn shutdown_requested(&self) -> bool;

    fn install_waker(&self, _waker: RuntimeWaker) {}
}

#[derive(Debug, Default)]
pub struct NoShutdownSignal;

impl ShutdownSignal for NoShutdownSignal {
    fn shutdown_requested(&self) -> bool {
        false
    }
}

pub struct ProcessSignalSource {
    requested: Arc<AtomicBool>,
    wake: Arc<Mutex<Option<RuntimeWaker>>>,
    stop_sender: Option<tokio::sync::oneshot::Sender<()>>,
    worker: Option<JoinHandle<()>>,
}

impl ProcessSignalSource {
    pub fn new() -> Result<Self, StatusInterfaceError> {
        let requested = Arc::new(AtomicBool::new(false));
        let (stop_sender, stop_receiver) = tokio::sync::oneshot::channel();
        let (ready_sender, ready_receiver) = mpsc::sync_channel(1);
        let worker_requested = Arc::clone(&requested);
        let wake = Arc::new(Mutex::new(None));
        let worker_wake = Arc::clone(&wake);
        let worker = thread::Builder::new()
            .name("hopash-tui-signals".to_owned())
            .spawn(move || {
                run_signal_worker(worker_requested, worker_wake, stop_receiver, ready_sender);
            })
            .map_err(|_| {
                StatusInterfaceError::new(
                    StatusInterfaceErrorKind::Signal,
                    "The Status Interface signal listener could not start",
                )
            })?;
        match ready_receiver.recv() {
            Ok(Ok(())) => Ok(Self {
                requested,
                wake,
                stop_sender: Some(stop_sender),
                worker: Some(worker),
            }),
            Ok(Err(error)) => {
                let _ = worker.join();
                Err(error)
            }
            Err(_) => {
                let _ = worker.join();
                Err(StatusInterfaceError::new(
                    StatusInterfaceErrorKind::Signal,
                    "The Status Interface signal listener stopped during startup",
                ))
            }
        }
    }
}

impl ShutdownSignal for ProcessSignalSource {
    fn shutdown_requested(&self) -> bool {
        self.requested.load(Ordering::Acquire)
    }

    fn install_waker(&self, waker: RuntimeWaker) {
        *self
            .wake
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(waker);
    }
}

impl Drop for ProcessSignalSource {
    fn drop(&mut self) {
        if let Some(stop_sender) = self.stop_sender.take() {
            let _ = stop_sender.send(());
        }
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

#[cfg(unix)]
fn run_signal_worker(
    requested: Arc<AtomicBool>,
    wake: Arc<Mutex<Option<RuntimeWaker>>>,
    mut stop_receiver: tokio::sync::oneshot::Receiver<()>,
    ready_sender: SyncSender<Result<(), StatusInterfaceError>>,
) {
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .enable_time()
        .build()
    {
        Ok(runtime) => runtime,
        Err(_) => {
            let _ = ready_sender.send(Err(StatusInterfaceError::new(
                StatusInterfaceErrorKind::Signal,
                "The Status Interface signal runtime could not start",
            )));
            return;
        }
    };
    runtime.block_on(async move {
        let mut interrupt =
            match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt()) {
                Ok(signal) => signal,
                Err(_) => {
                    let _ = ready_sender.send(Err(StatusInterfaceError::new(
                        StatusInterfaceErrorKind::Signal,
                        "The interrupt signal listener could not start",
                    )));
                    return;
                }
            };
        let mut terminate =
            match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
                Ok(signal) => signal,
                Err(_) => {
                    let _ = ready_sender.send(Err(StatusInterfaceError::new(
                        StatusInterfaceErrorKind::Signal,
                        "The termination signal listener could not start",
                    )));
                    return;
                }
            };
        if ready_sender.send(Ok(())).is_err() {
            return;
        }
        tokio::select! {
            _ = interrupt.recv() => {
                requested.store(true, Ordering::Release);
                wake_runtime(&wake);
            }
            _ = terminate.recv() => {
                requested.store(true, Ordering::Release);
                wake_runtime(&wake);
            }
            _ = &mut stop_receiver => {}
        }
    });
}

#[cfg(not(unix))]
fn run_signal_worker(
    requested: Arc<AtomicBool>,
    wake: Arc<Mutex<Option<RuntimeWaker>>>,
    mut stop_receiver: tokio::sync::oneshot::Receiver<()>,
    ready_sender: SyncSender<Result<(), StatusInterfaceError>>,
) {
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .enable_time()
        .build()
    {
        Ok(runtime) => runtime,
        Err(_) => {
            let _ = ready_sender.send(Err(StatusInterfaceError::new(
                StatusInterfaceErrorKind::Signal,
                "The Status Interface signal runtime could not start",
            )));
            return;
        }
    };
    let _ = ready_sender.send(Ok(()));
    runtime.block_on(async move {
        tokio::select! {
            result = tokio::signal::ctrl_c() => {
                if result.is_ok() {
                    requested.store(true, Ordering::Release);
                    wake_runtime(&wake);
                }
            }
            _ = &mut stop_receiver => {}
        }
    });
}

fn wake_runtime(wake: &Mutex<Option<RuntimeWaker>>) {
    if let Some(waker) = wake
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .as_ref()
    {
        waker.wake();
    }
}

pub trait TerminalEventSource {
    fn install_waker(&mut self, _waker: RuntimeWaker) {}

    fn try_event(&mut self) -> Result<Option<UiEvent>, StatusInterfaceError>;

    fn shutdown(&mut self) {}
}

pub struct CrosstermEventSource {
    buffer: Arc<TerminalEventBuffer>,
    wake: Arc<Mutex<Option<RuntimeWaker>>>,
    stop_sender: Option<tokio::sync::oneshot::Sender<()>>,
    worker: Option<JoinHandle<()>>,
}

#[derive(Default)]
struct TerminalEventBuffer {
    events: Mutex<VecDeque<Result<UiEvent, StatusInterfaceError>>>,
}

impl CrosstermEventSource {
    pub fn new() -> Result<Self, StatusInterfaceError> {
        let buffer = Arc::new(TerminalEventBuffer::default());
        let wake = Arc::new(Mutex::new(None));
        let (stop_sender, stop_receiver) = tokio::sync::oneshot::channel();
        let worker_buffer = Arc::clone(&buffer);
        let worker_wake = Arc::clone(&wake);
        let worker = thread::Builder::new()
            .name("hopash-tui-terminal-events".to_owned())
            .spawn(move || run_terminal_event_worker(worker_buffer, worker_wake, stop_receiver))
            .map_err(|_| {
                StatusInterfaceError::new(
                    StatusInterfaceErrorKind::TerminalInput,
                    "The Status Interface terminal listener could not start",
                )
            })?;
        Ok(Self {
            buffer,
            wake,
            stop_sender: Some(stop_sender),
            worker: Some(worker),
        })
    }
}

impl TerminalEventSource for CrosstermEventSource {
    fn install_waker(&mut self, waker: RuntimeWaker) {
        *self
            .wake
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(waker);
    }

    fn try_event(&mut self) -> Result<Option<UiEvent>, StatusInterfaceError> {
        self.buffer
            .events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .pop_front()
            .transpose()
    }

    fn shutdown(&mut self) {
        if let Some(stop_sender) = self.stop_sender.take() {
            let _ = stop_sender.send(());
        }
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

impl Drop for CrosstermEventSource {
    fn drop(&mut self) {
        self.shutdown();
    }
}

fn run_terminal_event_worker(
    buffer: Arc<TerminalEventBuffer>,
    wake: Arc<Mutex<Option<RuntimeWaker>>>,
    mut stop_receiver: tokio::sync::oneshot::Receiver<()>,
) {
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .build()
    {
        Ok(runtime) => runtime,
        Err(_) => {
            publish_terminal_event(
                &buffer,
                &wake,
                Err(StatusInterfaceError::new(
                    StatusInterfaceErrorKind::TerminalInput,
                    "The Status Interface terminal listener could not initialize",
                )),
            );
            return;
        }
    };
    runtime.block_on(async move {
        let mut stream = EventStream::new();
        loop {
            tokio::select! {
                _ = &mut stop_receiver => return,
                event = stream.next() => {
                    let Some(event) = event else {
                        publish_terminal_event(
                            &buffer,
                            &wake,
                            Err(StatusInterfaceError::new(
                                StatusInterfaceErrorKind::TerminalInput,
                                "The Status Interface terminal listener stopped",
                            )),
                        );
                        return;
                    };
                    match event {
                        Ok(event) => {
                            if let Some(event) = terminal_ui_event(event) {
                                publish_terminal_event(&buffer, &wake, Ok(event));
                            }
                        }
                        Err(error) => {
                            publish_terminal_event(&buffer, &wake, Err(terminal_input_error(error)));
                            return;
                        }
                    }
                }
            }
        }
    });
}

fn terminal_ui_event(event: CrosstermEvent) -> Option<UiEvent> {
    if matches!(
        event,
        CrosstermEvent::Key(key)
            if key.kind == KeyEventKind::Press
                && key.code == KeyCode::Char('c')
                && key.modifiers.contains(KeyModifiers::CONTROL)
    ) {
        Some(UiEvent::Shutdown)
    } else {
        from_crossterm_event(event)
    }
}

fn publish_terminal_event(
    buffer: &TerminalEventBuffer,
    wake: &Mutex<Option<RuntimeWaker>>,
    event: Result<UiEvent, StatusInterfaceError>,
) {
    let mut events = buffer
        .events
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if events.len() == crate::tui::EVENT_SOURCE_CAPACITY {
        events.pop_front();
    }
    events.push_back(event);
    drop(events);
    wake_runtime(wake);
}

fn terminal_input_error(_error: io::Error) -> StatusInterfaceError {
    StatusInterfaceError::new(
        StatusInterfaceErrorKind::TerminalInput,
        "The Status Interface could not read terminal input",
    )
}

#[derive(Clone, Debug)]
pub struct RenderedFrame {
    pub interaction_map: InteractionMap,
    pub width: u16,
    pub height: u16,
}

pub trait StatusRenderer {
    fn draw(&mut self, state: &AppState) -> Result<RenderedFrame, StatusInterfaceError>;
}

pub struct RatatuiStatusRenderer<B: Backend> {
    terminal: Terminal<B>,
}

impl<B: Backend> RatatuiStatusRenderer<B> {
    pub fn new(backend: B) -> Result<Self, StatusInterfaceError> {
        let terminal = Terminal::new(backend).map_err(render_error)?;
        Ok(Self { terminal })
    }

    #[must_use]
    pub fn terminal(&self) -> &Terminal<B> {
        &self.terminal
    }
}

impl<B: Backend> StatusRenderer for RatatuiStatusRenderer<B> {
    fn draw(&mut self, state: &AppState) -> Result<RenderedFrame, StatusInterfaceError> {
        let mut interaction_map = None;
        let completed = self
            .terminal
            .draw(|frame| {
                interaction_map = Some(render(frame, state));
            })
            .map_err(render_error)?;
        Ok(RenderedFrame {
            interaction_map: interaction_map.expect("render always publishes an interaction map"),
            width: completed.area.width,
            height: completed.area.height,
        })
    }
}

fn render_error(_error: io::Error) -> StatusInterfaceError {
    StatusInterfaceError::new(
        StatusInterfaceErrorKind::Render,
        "The Status Interface frame could not be rendered",
    )
}

pub trait ReconnectTiming {
    fn schedule(&mut self, connection_generation: u64, now: Duration);
    fn take_due(&mut self, now: Duration) -> Option<u64>;
    fn reset(&mut self);

    fn deadline(&self) -> Option<Duration> {
        None
    }
}

#[derive(Clone, Debug)]
pub struct BoundedReconnectTimer {
    initial: Duration,
    maximum: Duration,
    attempts: u32,
    scheduled: Option<(u64, Duration)>,
}

impl BoundedReconnectTimer {
    pub fn new(initial: Duration, maximum: Duration) -> Result<Self, StatusInterfaceError> {
        if initial.is_zero() || maximum < initial {
            return Err(StatusInterfaceError::new(
                StatusInterfaceErrorKind::InvalidConfiguration,
                "Reconnect bounds must be positive and ordered",
            ));
        }
        Ok(Self {
            initial,
            maximum,
            attempts: 0,
            scheduled: None,
        })
    }

    #[must_use]
    pub fn deadline(&self) -> Option<Duration> {
        self.scheduled.map(|(_, deadline)| deadline)
    }

    fn next_delay(&self) -> Duration {
        let multiplier = 1_u32.checked_shl(self.attempts.min(31)).unwrap_or(u32::MAX);
        self.initial
            .checked_mul(multiplier)
            .unwrap_or(self.maximum)
            .min(self.maximum)
    }
}

impl ReconnectTiming for BoundedReconnectTimer {
    fn schedule(&mut self, connection_generation: u64, now: Duration) {
        let deadline = now.saturating_add(self.next_delay());
        self.attempts = self.attempts.saturating_add(1);
        self.scheduled = Some((connection_generation, deadline));
    }

    fn take_due(&mut self, now: Duration) -> Option<u64> {
        let (generation, deadline) = self.scheduled?;
        if now < deadline {
            return None;
        }
        self.scheduled = None;
        Some(generation)
    }

    fn reset(&mut self) {
        self.attempts = 0;
        self.scheduled = None;
    }

    fn deadline(&self) -> Option<Duration> {
        self.deadline()
    }
}

// -----------------------------------------------------------------------------
// Status Interface event loop
// -----------------------------------------------------------------------------

#[derive(Debug, Default)]
struct SnapshotFreshness {
    generation: u64,
    request_in_flight: bool,
    refresh_pending: bool,
    refresh_deadline: Option<Duration>,
    profile_deadline: Option<Duration>,
    next_allowed_at: Duration,
}

impl SnapshotFreshness {
    fn connected(
        generation: u64,
        next_profile_refresh_at_unix_ms: Option<u64>,
        now: Duration,
        now_unix_ms: u64,
    ) -> Self {
        let mut freshness = Self {
            generation,
            ..Self::default()
        };
        freshness.schedule_profile_deadline(next_profile_refresh_at_unix_ms, now, now_unix_ms);
        freshness
    }

    fn reset_connected(
        &mut self,
        generation: u64,
        next_profile_refresh_at_unix_ms: Option<u64>,
        now: Duration,
        now_unix_ms: u64,
    ) {
        *self = Self::connected(
            generation,
            next_profile_refresh_at_unix_ms,
            now,
            now_unix_ms,
        );
    }

    fn disconnect(&mut self) {
        self.request_in_flight = false;
        self.refresh_pending = false;
        self.refresh_deadline = None;
        self.profile_deadline = None;
    }

    fn request_refresh(&mut self, generation: u64, now: Duration) {
        if generation != self.generation {
            return;
        }
        self.refresh_pending = true;
        if !self.request_in_flight && self.refresh_deadline.is_none() {
            self.refresh_deadline = Some(
                now.saturating_add(SNAPSHOT_REFRESH_COALESCE)
                    .max(self.next_allowed_at),
            );
        }
    }

    fn take_due(&mut self, now: Duration) -> Option<u64> {
        if self
            .profile_deadline
            .is_some_and(|deadline| deadline <= now)
        {
            self.profile_deadline = None;
            self.refresh_pending = true;
            if !self.request_in_flight && self.refresh_deadline.is_none() {
                self.refresh_deadline = Some(now.max(self.next_allowed_at));
            }
        }
        if self.request_in_flight
            || !self.refresh_pending
            || self.refresh_deadline.is_none_or(|deadline| deadline > now)
        {
            return None;
        }
        self.request_in_flight = true;
        self.refresh_pending = false;
        self.refresh_deadline = None;
        self.next_allowed_at = now.saturating_add(SNAPSHOT_REFRESH_MIN_INTERVAL);
        Some(self.generation)
    }

    fn submit_failed(&mut self, generation: u64, now: Duration) {
        if generation != self.generation {
            return;
        }
        self.request_in_flight = false;
        self.request_refresh(generation, now);
    }

    fn complete(
        &mut self,
        generation: u64,
        accepted: bool,
        next_profile_refresh_at_unix_ms: Option<u64>,
        now: Duration,
        now_unix_ms: u64,
    ) {
        if generation != self.generation {
            return;
        }
        self.request_in_flight = false;
        self.schedule_profile_deadline(next_profile_refresh_at_unix_ms, now, now_unix_ms);
        if !accepted {
            self.refresh_pending = true;
        }
        if self.refresh_pending {
            self.refresh_deadline = Some(
                now.saturating_add(SNAPSHOT_REFRESH_COALESCE)
                    .max(self.next_allowed_at),
            );
        }
    }

    fn schedule_profile_deadline(
        &mut self,
        next_refresh_at_unix_ms: Option<u64>,
        now: Duration,
        now_unix_ms: u64,
    ) {
        self.profile_deadline = next_refresh_at_unix_ms.map(|deadline| {
            let delay_ms = deadline.saturating_sub(now_unix_ms);
            let delay =
                Duration::from_millis(delay_ms).saturating_add(PROFILE_REFRESH_SETTLE_DELAY);
            now.saturating_add(delay)
        });
    }

    fn deadline(&self) -> Option<Duration> {
        [self.refresh_deadline, self.profile_deadline]
            .into_iter()
            .flatten()
            .min()
    }
}

fn next_profile_refresh_at(snapshot: &FullViewSnapshot) -> Option<u64> {
    snapshot
        .profiles
        .iter()
        .map(|profile| profile.next_refresh_at_unix_ms)
        .min()
}

pub struct StatusInterfaceRuntime<'a> {
    state: AppState,
    inbox: FairEventInbox,
    events: Arc<dyn StatusLogEventSource>,
    dispatcher: &'a mut dyn CommandDispatcher,
    reconnect: &'a mut dyn ReconnectTiming,
    input: &'a mut dyn TerminalEventSource,
    waiter: &'a dyn RuntimeWaiter,
    clock: &'a dyn RuntimeClock,
    signal: &'a dyn ShutdownSignal,
    renderer: &'a mut dyn StatusRenderer,
    freshness: SnapshotFreshness,
    stopped: bool,
}

pub struct StatusInterfacePorts<'a> {
    pub events: Arc<dyn StatusLogEventSource>,
    pub dispatcher: &'a mut dyn CommandDispatcher,
    pub reconnect: &'a mut dyn ReconnectTiming,
    pub input: &'a mut dyn TerminalEventSource,
    pub waiter: &'a dyn RuntimeWaiter,
    pub waker: RuntimeWaker,
    pub clock: &'a dyn RuntimeClock,
    pub signal: &'a dyn ShutdownSignal,
    pub renderer: &'a mut dyn StatusRenderer,
}

impl<'a> StatusInterfaceRuntime<'a> {
    pub fn new(
        connection_generation: u64,
        snapshot: FullViewSnapshot,
        ports: StatusInterfacePorts<'a>,
    ) -> Self {
        let StatusInterfacePorts {
            events,
            dispatcher,
            reconnect,
            input,
            waiter,
            waker,
            clock,
            signal,
            renderer,
        } = ports;
        let freshness = SnapshotFreshness::connected(
            connection_generation,
            next_profile_refresh_at(&snapshot),
            clock.now(),
            clock.now_unix_ms(),
        );
        let mut state = AppState::new();
        let _ = update(
            &mut state,
            UiEvent::Connected {
                connection_generation,
                snapshot,
            },
        );
        events.install_waker(waker.clone());
        dispatcher.install_waker(waker.clone());
        input.install_waker(waker.clone());
        signal.install_waker(waker);
        Self {
            state,
            inbox: FairEventInbox::product(),
            events,
            dispatcher,
            reconnect,
            input,
            waiter,
            clock,
            signal,
            renderer,
            freshness,
            stopped: false,
        }
    }

    #[must_use]
    pub fn state(&self) -> &AppState {
        &self.state
    }

    pub fn run(&mut self) -> Result<(), StatusInterfaceError> {
        self.draw_if_dirty()?;
        while !self.state.should_quit {
            let checkpoint = self.waiter.checkpoint();
            let already_ready = self.collect_events()?;
            self.process_round();
            if !self.state.should_quit {
                self.draw_if_dirty()?;
            }
            if !already_ready && !self.state.should_quit {
                let now = self.clock.now();
                let timeout = [self.reconnect.deadline(), self.freshness.deadline()]
                    .into_iter()
                    .flatten()
                    .min()
                    .map(|deadline| deadline.saturating_sub(now));
                self.waiter.wait(checkpoint, timeout);
            }
        }
        self.stop();
        Ok(())
    }

    fn collect_events(&mut self) -> Result<bool, StatusInterfaceError> {
        let mut ready = false;
        for _ in 0..TERMINAL_EVENTS_PER_ROUND {
            let Some(event) = self.input.try_event()? else {
                break;
            };
            self.inbox.push(EventSource::Terminal, event);
            ready = true;
        }
        if self.signal.shutdown_requested() {
            self.inbox.push(EventSource::Deadline, UiEvent::Shutdown);
            ready = true;
        }
        for _ in 0..COMMAND_RESULTS_PER_ROUND {
            let Some(event) = self.dispatcher.try_next().map_err(|_| {
                StatusInterfaceError::new(
                    StatusInterfaceErrorKind::CommandQueue,
                    "The Status Interface command worker stopped",
                )
            })?
            else {
                break;
            };
            self.inbox.push(event.source, event.event);
            ready = true;
        }
        if self.state.connection.status == ConnectionStatus::Connected {
            for _ in 0..STREAM_EVENTS_PER_ROUND {
                match self.events.try_next() {
                    Ok(Some(event)) => {
                        self.inbox
                            .push(EventSource::Telemetry, event.into_ui_event());
                        ready = true;
                    }
                    Ok(None) => break,
                    Err(_) => {
                        self.inbox.push(
                            EventSource::Telemetry,
                            UiEvent::Disconnected {
                                connection_generation: self.state.connection.generation,
                            },
                        );
                        ready = true;
                        break;
                    }
                }
            }
        }
        if let Some(connection_generation) = self.reconnect.take_due(self.clock.now()) {
            self.inbox.push(
                EventSource::Deadline,
                UiEvent::ReconnectDeadline {
                    connection_generation,
                },
            );
            ready = true;
        }
        if let Some(connection_generation) = self.freshness.take_due(self.clock.now()) {
            self.dispatch(Command::RefreshSnapshot {
                connection_generation,
                base_view_revision: self.state.view_revision(),
                base_status_revision: self.state.status_revision(),
            });
            ready = true;
        }
        Ok(ready)
    }

    fn process_round(&mut self) {
        for event in self.inbox.drain_round() {
            let committed_mutation = match &event {
                UiEvent::CommandResult {
                    request_id,
                    connection_generation,
                    result: Ok(_),
                } => self.state.pending.as_ref().is_some_and(|pending| {
                    pending.request_id == *request_id
                        && pending.connection_generation == *connection_generation
                        && *connection_generation == self.state.connection.generation
                }),
                _ => false,
            };
            let refresh_from_status = match &event {
                UiEvent::StatusSnapshot {
                    connection_generation,
                    status,
                } if *connection_generation == self.state.connection.generation
                    && self.state.connection.status == ConnectionStatus::Connected =>
                {
                    self.state
                        .status
                        .as_ref()
                        .is_some_and(|previous| status_requires_snapshot_refresh(previous, status))
                }
                _ => false,
            };
            let completed_snapshot = match &event {
                UiEvent::SnapshotRefreshed {
                    connection_generation,
                    base_view_revision,
                    base_status_revision: _,
                    snapshot,
                } => Some((
                    *connection_generation,
                    next_profile_refresh_at(snapshot),
                    self.state
                        .accepts_snapshot_refresh(*connection_generation, *base_view_revision),
                )),
                _ => None,
            };
            let failed_snapshot = match &event {
                UiEvent::SnapshotRefreshFailed {
                    connection_generation,
                    ..
                } => Some(*connection_generation),
                _ => None,
            };
            let connected_snapshot = match &event {
                UiEvent::Connected {
                    connection_generation,
                    snapshot,
                } => Some((*connection_generation, next_profile_refresh_at(snapshot))),
                _ => None,
            };
            let connected_generation = match &event {
                UiEvent::Connected {
                    connection_generation,
                    ..
                } => Some(*connection_generation),
                _ => None,
            };
            let disconnected_generation = match &event {
                UiEvent::Disconnected {
                    connection_generation,
                } => Some(*connection_generation),
                _ => None,
            };
            let commands = update(&mut self.state, event);
            if refresh_from_status {
                self.freshness
                    .request_refresh(self.state.connection.generation, self.clock.now());
            }
            if committed_mutation {
                self.freshness
                    .request_refresh(self.state.connection.generation, self.clock.now());
            }
            if let Some((generation, next_profile_refresh, accepted)) = completed_snapshot {
                self.freshness.complete(
                    generation,
                    accepted,
                    next_profile_refresh,
                    self.clock.now(),
                    self.clock.now_unix_ms(),
                );
            }
            if let Some(generation) = failed_snapshot {
                self.freshness.submit_failed(generation, self.clock.now());
            }
            if let Some((generation, next_profile_refresh)) = connected_snapshot
                && generation == self.state.connection.generation
                && self.state.connection.status == ConnectionStatus::Connected
            {
                self.freshness.reset_connected(
                    generation,
                    next_profile_refresh,
                    self.clock.now(),
                    self.clock.now_unix_ms(),
                );
            }
            if connected_generation == Some(self.state.connection.generation)
                && self.state.connection.status == ConnectionStatus::Connected
            {
                self.reconnect.reset();
            }
            if disconnected_generation == Some(self.state.connection.generation)
                && self.state.connection.status == ConnectionStatus::Disconnected
            {
                self.events.disconnect(self.state.connection.generation);
                self.freshness.disconnect();
            }
            for command in commands {
                self.dispatch(command);
            }
            if self.state.should_quit {
                break;
            }
        }
    }

    fn dispatch(&mut self, command: Command) {
        match command {
            Command::ScheduleReconnect {
                connection_generation,
            } => {
                if connection_generation == self.state.connection.generation
                    && self.state.connection.status == ConnectionStatus::Disconnected
                {
                    self.reconnect
                        .schedule(connection_generation, self.clock.now());
                }
            }
            Command::Cancel { request_id } => self.dispatcher.cancel(request_id),
            command @ Command::Connect {
                connection_generation,
            } => {
                if connection_generation == self.state.connection.generation
                    && self.state.connection.status == ConnectionStatus::Connecting
                    && self.dispatcher.submit(command).is_err()
                {
                    self.inbox.push(
                        EventSource::CommandResult,
                        UiEvent::Disconnected {
                            connection_generation,
                        },
                    );
                }
            }
            command @ Command::ActivateProfile {
                request_id,
                connection_generation,
                ..
            }
            | command @ Command::SelectNode {
                request_id,
                connection_generation,
                ..
            } => {
                if connection_generation != self.state.connection.generation
                    || self.state.connection.status != ConnectionStatus::Connected
                {
                    self.inbox.push(
                        EventSource::CommandResult,
                        UiEvent::CommandResult {
                            request_id,
                            connection_generation,
                            result: Err("The Supervisor connection is unavailable".to_owned()),
                        },
                    );
                } else if self.dispatcher.submit(command).is_err() {
                    self.inbox.push(
                        EventSource::CommandResult,
                        UiEvent::CommandResult {
                            request_id,
                            connection_generation,
                            result: Err("The command queue is full".to_owned()),
                        },
                    );
                }
            }
            command @ Command::FetchProxyGroup {
                request_id,
                connection_generation,
                ..
            } => {
                let result = if connection_generation != self.state.connection.generation
                    || self.state.connection.status != ConnectionStatus::Connected
                {
                    Some("The Supervisor connection is unavailable".to_owned())
                } else if self.dispatcher.submit(command).is_err() {
                    Some("The command queue is full".to_owned())
                } else {
                    None
                };
                if let Some(message) = result {
                    self.inbox.push(
                        EventSource::CommandResult,
                        UiEvent::ProxyGroupLoaded {
                            request_id,
                            connection_generation,
                            result: Err(message),
                        },
                    );
                }
            }
            command @ Command::FetchLogTail {
                connection_generation,
                ..
            } => {
                if connection_generation == self.state.connection.generation
                    && self.state.connection.status == ConnectionStatus::Connected
                    && self.dispatcher.submit(command).is_err()
                {
                    self.inbox.push(
                        EventSource::CommandResult,
                        UiEvent::Disconnected {
                            connection_generation,
                        },
                    );
                }
            }
            command @ Command::RefreshSnapshot {
                connection_generation,
                ..
            } => {
                if connection_generation != self.state.connection.generation
                    || self.state.connection.status != ConnectionStatus::Connected
                {
                    self.freshness.disconnect();
                } else if self.dispatcher.submit(command).is_err() {
                    self.freshness
                        .submit_failed(connection_generation, self.clock.now());
                }
            }
        }
    }

    fn draw_if_dirty(&mut self) -> Result<(), StatusInterfaceError> {
        if !self.state.render_dirty {
            return Ok(());
        }
        let frame = self.renderer.draw(&self.state)?;
        self.state.terminal_width = frame.width;
        self.state.terminal_height = frame.height;
        self.state.publish_interaction_map(frame.interaction_map);
        Ok(())
    }

    fn stop(&mut self) {
        if self.stopped {
            return;
        }
        self.stopped = true;
        self.dispatcher.cancel_all();
        self.events.disconnect(self.state.connection.generation);
        self.input.shutdown();
    }
}

impl Drop for StatusInterfaceRuntime<'_> {
    fn drop(&mut self) {
        self.stop();
    }
}

pub fn bootstrap_status_interface(
    sources: &StatusInterfaceSources,
    connection_generation: u64,
) -> Result<FullViewSnapshot, StatusInterfaceError> {
    let cancellation = CancellationToken::default();
    sources
        .events
        .connect(connection_generation, &cancellation)?;
    match sources
        .snapshots
        .fetch_full_snapshot(connection_generation, &cancellation)
    {
        Ok(snapshot) => Ok(snapshot),
        Err(error) => {
            sources.events.disconnect(connection_generation);
            Err(error)
        }
    }
}

pub fn run_with_terminal_session<T>(
    control: &mut dyn TerminalControl,
    operation: impl FnOnce() -> Result<T, StatusInterfaceError>,
) -> Result<T, StatusInterfaceError> {
    let mut session = TerminalSession::enter(control).map_err(|_| {
        StatusInterfaceError::new(
            StatusInterfaceErrorKind::TerminalSetup,
            "The Status Interface could not initialize the terminal",
        )
    })?;
    let result = operation();
    let cleanup = session.cleanup().map_err(|_| {
        StatusInterfaceError::new(
            StatusInterfaceErrorKind::TerminalSetup,
            "The Status Interface could not fully restore the terminal",
        )
    });
    match result {
        Ok(value) => cleanup.map(|()| value),
        Err(error) => {
            let _ = cleanup;
            Err(error)
        }
    }
}

pub fn run_crossterm_status_interface(
    sources: StatusInterfaceSources,
) -> Result<(), StatusInterfaceError> {
    run_crossterm_status_interface_with_render_writer(sources, io::stdout())
}

#[doc(hidden)]
pub fn run_crossterm_status_interface_with_render_writer<W: io::Write>(
    sources: StatusInterfaceSources,
    render_writer: W,
) -> Result<(), StatusInterfaceError> {
    const INITIAL_CONNECTION_GENERATION: u64 = 1;

    let snapshot = bootstrap_status_interface(&sources, INITIAL_CONNECTION_GENERATION)?;
    let result = run_crossterm_after_bootstrap(
        sources.clone(),
        INITIAL_CONNECTION_GENERATION,
        snapshot,
        render_writer,
    );
    sources.events.disconnect(INITIAL_CONNECTION_GENERATION);
    result
}

fn run_crossterm_after_bootstrap<W: io::Write>(
    sources: StatusInterfaceSources,
    connection_generation: u64,
    snapshot: FullViewSnapshot,
    render_writer: W,
) -> Result<(), StatusInterfaceError> {
    let mut dispatcher = BackgroundCommandDispatcher::new(sources.clone())?;
    let signal = ProcessSignalSource::new()?;
    let clock = MonotonicClock::default();
    let mut reconnect =
        BoundedReconnectTimer::new(RECONNECT_INITIAL_BACKOFF, RECONNECT_MAX_BACKOFF)?;
    let mut input = CrosstermEventSource::new()?;
    let waker = RuntimeWaker::default();
    let backend = CrosstermBackend::new(render_writer);
    let mut renderer = RatatuiStatusRenderer::new(backend)?;
    let mut control = CrosstermControl::new(io::stdout());
    let panic_hook = TerminalPanicHook::install();
    let result = panic::catch_unwind(AssertUnwindSafe(|| {
        run_with_terminal_session(&mut control, || {
            let mut runtime = StatusInterfaceRuntime::new(
                connection_generation,
                snapshot,
                StatusInterfacePorts {
                    events: sources.events,
                    dispatcher: &mut dispatcher,
                    reconnect: &mut reconnect,
                    input: &mut input,
                    waiter: &waker,
                    waker: waker.clone(),
                    clock: &clock,
                    signal: &signal,
                    renderer: &mut renderer,
                },
            );
            runtime.run()
        })
    }));
    drop(panic_hook);
    dispatcher.shutdown();
    match result {
        Ok(result) => result,
        Err(payload) => panic::resume_unwind(payload),
    }
}

type PanicHook = dyn Fn(&PanicHookInfo<'_>) + Send + Sync + 'static;

struct TerminalPanicHook {
    previous: Arc<PanicHook>,
}

impl TerminalPanicHook {
    fn install() -> Self {
        let previous: Arc<PanicHook> = panic::take_hook().into();
        let hook_previous = Arc::clone(&previous);
        panic::set_hook(Box::new(move |information| {
            best_effort_terminal_cleanup();
            hook_previous(information);
        }));
        Self { previous }
    }
}

impl Drop for TerminalPanicHook {
    fn drop(&mut self) {
        let _ = panic::take_hook();
        let previous = Arc::clone(&self.previous);
        panic::set_hook(Box::new(move |information| previous(information)));
    }
}

fn best_effort_terminal_cleanup() {
    let mut control = CrosstermControl::new(io::stdout());
    for action in [
        crate::tui::TerminalAction::ShowCursor,
        crate::tui::TerminalAction::DisableBracketedPaste,
        crate::tui::TerminalAction::DisableFocusReporting,
        crate::tui::TerminalAction::DisableMouseCapture,
        crate::tui::TerminalAction::LeaveAlternateScreen,
        crate::tui::TerminalAction::DisableRawMode,
    ] {
        let _ = control.apply(action);
    }
}
