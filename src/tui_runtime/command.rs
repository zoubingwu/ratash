//! Bounded background command dispatch and application command execution.

use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender, TryRecvError, TrySendError};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};

use crate::application::{
    ApplicationClient, ApplicationOperation, ApplicationOutput, LifecycleAction,
    ProfileMutationAction, RuleMutationAction, RulePlacement,
};
use crate::cancellation::CancellationToken;
use crate::domain::{NodeRecordId, ProxyGroupId};
use crate::ipc::RequestId;
use crate::tui::{Command, EventSource, MutationSuccess, UiEvent};

use super::{
    FullSnapshotSource, RuntimeWaker, StatusInterfaceError, StatusInterfaceErrorKind,
    StatusInterfaceSources, StatusLogEventSource, bounded_log_records, wake_runtime,
};

const COMMAND_QUEUE_CAPACITY: usize = 32;
const COMMAND_WORKER_COUNT: usize = 2;
const TOAST_MAX_CHARACTERS: usize = 256;

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
            ApplicationOutput::RuleMutation(outcome) => Ok(match outcome.action {
                RuleMutationAction::Added => "Rule added",
                RuleMutationAction::Replaced => "Rule replaced",
                RuleMutationAction::Removed => "Rule removed",
            }
            .to_owned()),
            ApplicationOutput::Lifecycle(outcome) => Ok(match outcome.action {
                LifecycleAction::Start => "Supervisor started",
                LifecycleAction::Stop => "Supervisor stopped",
                LifecycleAction::Restart => "Supervisor restarted",
            }
            .to_owned()),
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
        let mut mutations = self.mutations.lock().map_err(|_| CommandDispatchError)?;
        if let Some(active_task_id) = mutations.active_task_id {
            let replaced = mutations.pending.replace(work);
            drop(mutations);
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
        mutations.active_task_id = Some(task_id);
        drop(mutations);

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
        Command::ActivateProfile { .. }
            | Command::SelectNode { .. }
            | Command::AddRule { .. }
            | Command::ReplaceRule { .. }
            | Command::RemoveRule { .. }
            | Command::RestartSupervisor { .. }
            | Command::StopSupervisor { .. }
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
        Command::FetchRules {
            request_id,
            connection_generation,
        } => {
            let result = snapshots
                .fetch_rules(*connection_generation, &work.cancellation)
                .map_err(|error| short_terminal_text(&error.to_string()));
            DispatchedEvent {
                source: EventSource::CommandResult,
                event: UiEvent::RulesLoaded {
                    request_id: *request_id,
                    connection_generation: *connection_generation,
                    result,
                },
            }
        }
        Command::AddRule {
            request_id,
            connection_generation,
            rule,
        } => mutation_result(
            *request_id,
            *connection_generation,
            ApplicationOperation::RuleAdd {
                rule: rule.clone(),
                placement: RulePlacement::Append,
            },
            &work.cancellation,
            commands,
        ),
        Command::ReplaceRule {
            request_id,
            connection_generation,
            old_rule,
            new_rule,
        } => mutation_result(
            *request_id,
            *connection_generation,
            ApplicationOperation::RuleReplace {
                old_rule: old_rule.clone(),
                new_rule: new_rule.clone(),
            },
            &work.cancellation,
            commands,
        ),
        Command::RemoveRule {
            request_id,
            connection_generation,
            rule,
        } => mutation_result(
            *request_id,
            *connection_generation,
            ApplicationOperation::RuleRemove { rule: rule.clone() },
            &work.cancellation,
            commands,
        ),
        Command::RestartSupervisor {
            request_id,
            connection_generation,
        } => mutation_result(
            *request_id,
            *connection_generation,
            ApplicationOperation::Restart,
            &work.cancellation,
            commands,
        ),
        Command::StopSupervisor {
            request_id,
            connection_generation,
        } => mutation_result(
            *request_id,
            *connection_generation,
            ApplicationOperation::Stop,
            &work.cancellation,
            commands,
        ),
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
    node_id: &NodeRecordId,
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
        | Command::FetchRules { request_id, .. }
        | Command::AddRule { request_id, .. }
        | Command::ReplaceRule { request_id, .. }
        | Command::RemoveRule { request_id, .. }
        | Command::RestartSupervisor { request_id, .. }
        | Command::StopSupervisor { request_id, .. }
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
