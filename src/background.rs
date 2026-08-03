use std::fmt;
use std::io;
use std::sync::mpsc::{Receiver, SyncSender, sync_channel};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crate::application::{ApplicationError, Clock};
use crate::constants::{
    CORE_LOG_LINE_MAX_BYTES, PROBE_WORKER_COUNT, PROFILE_REFRESH_CONCURRENCY,
    RECONNECT_INITIAL_BACKOFF, RECONNECT_MAX_BACKOFF, STATUS_SAMPLE_INTERVAL, STREAM_STALE_TIMEOUT,
};
use crate::core::{
    ConnectionSummary, CoreEventStream, DelayProbeRequest, ManagedCoreHandle, MihomoAdapter,
    MihomoError, MihomoErrorKind, MihomoLogFrame, MihomoLogLevel, TrafficFrame,
};
use crate::domain::{CoreInstanceGeneration, SampleState, StreamState, TrafficSample};
use crate::scheduler::{ProbeCompletion, ProbeOutcome, RefreshTask};
use crate::supervisor::{ProfileRefreshDisposition, ScheduledProbe, Supervisor, TelemetryStream};
use crate::telemetry::{LogLevel, LogSource};

// -----------------------------------------------------------------------------
// Injected application and Core ports
// -----------------------------------------------------------------------------

pub trait BackgroundApplication: Send + Sync {
    fn cancel_pending_refreshes(&self) {}

    fn reconcile_runtime_state(&self) -> Result<(), ApplicationError> {
        Ok(())
    }

    fn take_due_refreshes(&self) -> Result<Vec<RefreshTask>, ApplicationError>;

    fn execute_refresh_task(
        &self,
        task: RefreshTask,
    ) -> Result<ProfileRefreshDisposition, ApplicationError>;

    fn take_due_probes(&self) -> Result<Vec<ScheduledProbe>, ApplicationError>;

    fn complete_probe(
        &self,
        completion: ProbeCompletion,
    ) -> Result<crate::scheduler::ProbeCompletionStatus, ApplicationError>;

    fn managed_core(&self) -> Result<Option<ManagedCoreHandle>, ApplicationError>;

    fn set_stream_state(
        &self,
        generation: CoreInstanceGeneration,
        stream: TelemetryStream,
        state: StreamState,
    ) -> Result<bool, ApplicationError>;

    fn publish_traffic(
        &self,
        generation: CoreInstanceGeneration,
        sample: TrafficSample,
    ) -> Result<bool, ApplicationError>;

    fn publish_connections(
        &self,
        generation: CoreInstanceGeneration,
        summary: ConnectionSummary,
    ) -> Result<bool, ApplicationError>;

    fn publish_core_log(
        &self,
        generation: CoreInstanceGeneration,
        timestamp_unix_ms: u64,
        level: LogLevel,
        source: LogSource,
        message: String,
    ) -> Result<bool, ApplicationError>;

    fn record_core_log_drop(
        &self,
        _generation: CoreInstanceGeneration,
        _count: u64,
    ) -> Result<bool, ApplicationError> {
        Ok(false)
    }
}

impl BackgroundApplication for Supervisor {
    fn cancel_pending_refreshes(&self) {
        Supervisor::cancel_pending_profile_downloads(self);
    }

    fn reconcile_runtime_state(&self) -> Result<(), ApplicationError> {
        Supervisor::reconcile_runtime_state(self)
    }

    fn take_due_refreshes(&self) -> Result<Vec<RefreshTask>, ApplicationError> {
        Supervisor::take_due_refreshes(self)
    }

    fn execute_refresh_task(
        &self,
        task: RefreshTask,
    ) -> Result<ProfileRefreshDisposition, ApplicationError> {
        Supervisor::execute_refresh_task(self, task)
    }

    fn take_due_probes(&self) -> Result<Vec<ScheduledProbe>, ApplicationError> {
        Supervisor::take_due_probes(self)
    }

    fn complete_probe(
        &self,
        completion: ProbeCompletion,
    ) -> Result<crate::scheduler::ProbeCompletionStatus, ApplicationError> {
        Supervisor::complete_probe(self, completion)
    }

    fn managed_core(&self) -> Result<Option<ManagedCoreHandle>, ApplicationError> {
        Supervisor::managed_core(self)
    }

    fn set_stream_state(
        &self,
        generation: CoreInstanceGeneration,
        stream: TelemetryStream,
        state: StreamState,
    ) -> Result<bool, ApplicationError> {
        Supervisor::set_stream_state(self, generation, stream, state)
    }

    fn publish_traffic(
        &self,
        generation: CoreInstanceGeneration,
        sample: TrafficSample,
    ) -> Result<bool, ApplicationError> {
        Supervisor::publish_traffic(self, generation, sample)
    }

    fn publish_connections(
        &self,
        generation: CoreInstanceGeneration,
        summary: ConnectionSummary,
    ) -> Result<bool, ApplicationError> {
        Supervisor::publish_connections(self, generation, summary)
    }

    fn publish_core_log(
        &self,
        generation: CoreInstanceGeneration,
        timestamp_unix_ms: u64,
        level: LogLevel,
        source: LogSource,
        message: String,
    ) -> Result<bool, ApplicationError> {
        Supervisor::publish_core_log(self, generation, timestamp_unix_ms, level, source, message)
    }

    fn record_core_log_drop(
        &self,
        generation: CoreInstanceGeneration,
        count: u64,
    ) -> Result<bool, ApplicationError> {
        Supervisor::record_core_log_drop(self, generation, count)
    }
}

pub trait BackgroundCorePort: Send + Sync {
    fn cancel_pending(&self) {}

    fn probe_delay(
        &self,
        core: &ManagedCoreHandle,
        request: &DelayProbeRequest,
    ) -> Result<u64, MihomoError>;

    fn open_traffic_stream(
        &self,
        core: &ManagedCoreHandle,
    ) -> Result<Box<dyn CoreEventStream<TrafficFrame>>, MihomoError>;

    fn open_connection_stream(
        &self,
        core: &ManagedCoreHandle,
    ) -> Result<Box<dyn CoreEventStream<ConnectionSummary>>, MihomoError>;

    fn open_log_stream(
        &self,
        core: &ManagedCoreHandle,
    ) -> Result<Box<dyn CoreEventStream<MihomoLogFrame>>, MihomoError>;
}

pub struct MihomoBackgroundCorePort {
    mihomo: Arc<dyn MihomoAdapter>,
}

impl MihomoBackgroundCorePort {
    #[must_use]
    pub fn new(mihomo: Arc<dyn MihomoAdapter>) -> Self {
        Self { mihomo }
    }
}

impl BackgroundCorePort for MihomoBackgroundCorePort {
    fn cancel_pending(&self) {
        self.mihomo.cancel_pending();
    }

    fn probe_delay(
        &self,
        core: &ManagedCoreHandle,
        request: &DelayProbeRequest,
    ) -> Result<u64, MihomoError> {
        self.mihomo
            .probe_delay(&core.endpoint, request)
            .map(|result| result.delay_ms)
    }

    fn open_traffic_stream(
        &self,
        core: &ManagedCoreHandle,
    ) -> Result<Box<dyn CoreEventStream<TrafficFrame>>, MihomoError> {
        self.mihomo
            .open_traffic_stream(&core.endpoint, core.instance_generation)
    }

    fn open_connection_stream(
        &self,
        core: &ManagedCoreHandle,
    ) -> Result<Box<dyn CoreEventStream<ConnectionSummary>>, MihomoError> {
        self.mihomo
            .open_connection_stream(&core.endpoint, core.instance_generation)
    }

    fn open_log_stream(
        &self,
        core: &ManagedCoreHandle,
    ) -> Result<Box<dyn CoreEventStream<MihomoLogFrame>>, MihomoError> {
        self.mihomo
            .open_log_stream(&core.endpoint, core.instance_generation)
    }
}

// -----------------------------------------------------------------------------
// Background runtime owner
// -----------------------------------------------------------------------------

pub struct BackgroundRuntime {
    shutdown: Arc<ShutdownSignal>,
    application: Arc<dyn BackgroundApplication>,
    core: Arc<dyn BackgroundCorePort>,
    threads: Vec<JoinHandle<()>>,
}

impl BackgroundRuntime {
    pub fn start(
        application: Arc<dyn BackgroundApplication>,
        core: Arc<dyn BackgroundCorePort>,
        clock: Arc<dyn Clock>,
    ) -> io::Result<Self> {
        Self::start_with_timing(application, core, clock, BackgroundTiming::product())
    }

    fn start_with_timing(
        application: Arc<dyn BackgroundApplication>,
        core: Arc<dyn BackgroundCorePort>,
        clock: Arc<dyn Clock>,
        timing: BackgroundTiming,
    ) -> io::Result<Self> {
        let shutdown = Arc::new(ShutdownSignal::default());
        let mut start_guard = StartGuard::new(
            Arc::clone(&shutdown),
            Arc::clone(&application),
            Arc::clone(&core),
        );
        let (refresh_sender, refresh_receiver) = sync_channel(PROFILE_REFRESH_CONCURRENCY);
        let (probe_sender, probe_receiver) = sync_channel(PROBE_WORKER_COUNT);
        let refresh_receiver = Arc::new(Mutex::new(refresh_receiver));
        let probe_receiver = Arc::new(Mutex::new(probe_receiver));

        for worker in 0..PROFILE_REFRESH_CONCURRENCY {
            let application = Arc::clone(&application);
            let receiver = Arc::clone(&refresh_receiver);
            let shutdown = Arc::clone(&shutdown);
            spawn_owned(
                start_guard.threads_mut(),
                format!("ratash-refresh-{worker}"),
                move || refresh_worker(application, receiver, shutdown),
            )?;
        }
        for worker in 0..PROBE_WORKER_COUNT {
            let application = Arc::clone(&application);
            let core = Arc::clone(&core);
            let clock = Arc::clone(&clock);
            let receiver = Arc::clone(&probe_receiver);
            let shutdown = Arc::clone(&shutdown);
            spawn_owned(
                start_guard.threads_mut(),
                format!("ratash-probe-{worker}"),
                move || probe_worker(application, core, clock, receiver, shutdown),
            )?;
        }

        spawn_traffic_thread(
            start_guard.threads_mut(),
            Arc::clone(&application),
            Arc::clone(&core),
            Arc::clone(&clock),
            Arc::clone(&shutdown),
            timing,
        )?;
        spawn_connection_thread(
            start_guard.threads_mut(),
            Arc::clone(&application),
            Arc::clone(&core),
            Arc::clone(&clock),
            Arc::clone(&shutdown),
            timing,
        )?;
        spawn_log_thread(
            start_guard.threads_mut(),
            Arc::clone(&application),
            Arc::clone(&core),
            Arc::clone(&clock),
            Arc::clone(&shutdown),
            timing,
        )?;

        let owner_application = Arc::clone(&application);
        let owner_shutdown = Arc::clone(&shutdown);
        spawn_owned(
            start_guard.threads_mut(),
            "ratash-background".to_owned(),
            move || {
                scheduler_owner(
                    owner_application,
                    refresh_sender,
                    probe_sender,
                    owner_shutdown,
                    timing.scheduler_interval,
                );
            },
        )?;

        let threads = start_guard.finish();
        Ok(Self {
            shutdown,
            application,
            core,
            threads,
        })
    }

    pub fn shutdown(&mut self) -> Result<(), BackgroundShutdownError> {
        if self.threads.is_empty() {
            return Ok(());
        }
        self.request_shutdown();
        let mut panicked_threads = 0;
        for thread in self.threads.drain(..) {
            if thread.join().is_err() {
                panicked_threads += 1;
            }
        }
        if panicked_threads == 0 {
            Ok(())
        } else {
            Err(BackgroundShutdownError { panicked_threads })
        }
    }

    pub fn shutdown_until(&mut self, deadline: Instant) -> io::Result<()> {
        if self.threads.is_empty() {
            return Ok(());
        }
        self.request_shutdown();
        let mut panicked_threads = 0_usize;
        let mut timed_out_threads = 0_usize;
        for thread in self.threads.drain(..) {
            if !wait_until_finished(&thread, deadline) {
                timed_out_threads = timed_out_threads.saturating_add(1);
                continue;
            }
            if thread.join().is_err() {
                panicked_threads = panicked_threads.saturating_add(1);
            }
        }
        if timed_out_threads > 0 {
            Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "The background runtime exceeded the Supervisor shutdown deadline",
            ))
        } else if panicked_threads > 0 {
            Err(io::Error::other(
                "A background runtime thread terminated unexpectedly",
            ))
        } else {
            Ok(())
        }
    }

    fn request_shutdown(&self) {
        self.shutdown.request();
        self.application.cancel_pending_refreshes();
        self.core.cancel_pending();
    }
}

impl Drop for BackgroundRuntime {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BackgroundShutdownError {
    pub panicked_threads: usize,
}

impl fmt::Display for BackgroundShutdownError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{count} background thread(s) terminated unexpectedly",
            count = self.panicked_threads
        )
    }
}

impl std::error::Error for BackgroundShutdownError {}

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

fn spawn_owned(
    threads: &mut Vec<JoinHandle<()>>,
    name: String,
    task: impl FnOnce() + Send + 'static,
) -> io::Result<()> {
    threads.push(thread::Builder::new().name(name).spawn(task)?);
    Ok(())
}

fn scheduler_owner(
    application: Arc<dyn BackgroundApplication>,
    refresh_sender: SyncSender<RefreshTask>,
    probe_sender: SyncSender<ScheduledProbe>,
    shutdown: Arc<ShutdownSignal>,
    interval: Duration,
) {
    loop {
        if shutdown.is_requested() {
            return;
        }
        let _ = application.reconcile_runtime_state();
        if !dispatch_refreshes(application.as_ref(), &refresh_sender) {
            return;
        }
        if !dispatch_probes(application.as_ref(), &probe_sender) {
            return;
        }
        if shutdown.wait(interval) {
            return;
        }
    }
}

fn dispatch_refreshes(
    application: &dyn BackgroundApplication,
    sender: &SyncSender<RefreshTask>,
) -> bool {
    let Ok(tasks) = application.take_due_refreshes() else {
        return true;
    };
    for task in tasks {
        if sender.send(task).is_err() {
            return false;
        }
    }
    true
}

fn dispatch_probes(
    application: &dyn BackgroundApplication,
    sender: &SyncSender<ScheduledProbe>,
) -> bool {
    let Ok(tasks) = application.take_due_probes() else {
        return true;
    };
    for task in tasks {
        if sender.send(task).is_err() {
            return false;
        }
    }
    true
}

fn refresh_worker(
    application: Arc<dyn BackgroundApplication>,
    receiver: Arc<Mutex<Receiver<RefreshTask>>>,
    shutdown: Arc<ShutdownSignal>,
) {
    while let Some(task) = receive(&receiver) {
        if shutdown.is_requested() {
            return;
        }
        let _ = application.execute_refresh_task(task);
    }
}

fn probe_worker(
    application: Arc<dyn BackgroundApplication>,
    core: Arc<dyn BackgroundCorePort>,
    clock: Arc<dyn Clock>,
    receiver: Arc<Mutex<Receiver<ScheduledProbe>>>,
    shutdown: Arc<ShutdownSignal>,
) {
    while let Some(scheduled) = receive(&receiver) {
        if shutdown.is_requested() {
            return;
        }
        let outcome = execute_probe(application.as_ref(), core.as_ref(), &scheduled);
        if shutdown.is_requested() {
            return;
        }
        let _ = application.complete_probe(ProbeCompletion {
            task: scheduled.task,
            outcome,
            completed_at_unix_ms: clock.now_unix_ms(),
        });
    }
}

fn receive<T>(receiver: &Mutex<Receiver<T>>) -> Option<T> {
    receiver.lock().map_or_else(
        |poisoned| poisoned.into_inner().recv().ok(),
        |rx| rx.recv().ok(),
    )
}

fn execute_probe(
    application: &dyn BackgroundApplication,
    core: &dyn BackgroundCorePort,
    scheduled: &ScheduledProbe,
) -> ProbeOutcome {
    let Some(request) = scheduled.request.as_ref() else {
        return ProbeOutcome::Unavailable;
    };
    let managed_core = match application.managed_core() {
        Ok(Some(managed_core)) => managed_core,
        Ok(None) | Err(_) => return ProbeOutcome::Unavailable,
    };
    match core.probe_delay(&managed_core, request) {
        Ok(delay_ms) => ProbeOutcome::Success { delay_ms },
        Err(error) if error.kind == MihomoErrorKind::ProbeFailed => ProbeOutcome::TimedOut,
        Err(_) => ProbeOutcome::Unavailable,
    }
}

// -----------------------------------------------------------------------------
// Generation-scoped telemetry streams
// -----------------------------------------------------------------------------

#[derive(Clone, Copy)]
struct BackgroundTiming {
    scheduler_interval: Duration,
    stream_stale_timeout: Duration,
    reconnect_initial_backoff: Duration,
    reconnect_max_backoff: Duration,
}

impl BackgroundTiming {
    const fn product() -> Self {
        Self {
            scheduler_interval: STATUS_SAMPLE_INTERVAL,
            stream_stale_timeout: STREAM_STALE_TIMEOUT,
            reconnect_initial_backoff: RECONNECT_INITIAL_BACKOFF,
            reconnect_max_backoff: RECONNECT_MAX_BACKOFF,
        }
    }
}

fn spawn_traffic_thread(
    threads: &mut Vec<JoinHandle<()>>,
    application: Arc<dyn BackgroundApplication>,
    core: Arc<dyn BackgroundCorePort>,
    clock: Arc<dyn Clock>,
    shutdown: Arc<ShutdownSignal>,
    timing: BackgroundTiming,
) -> io::Result<()> {
    spawn_owned(threads, "ratash-traffic".to_owned(), move || {
        run_stream(
            application,
            core,
            clock,
            shutdown,
            timing,
            TelemetryStream::Traffic,
            |port, handle| port.open_traffic_stream(handle),
            |application, generation, frame, now| {
                application.publish_traffic(
                    generation,
                    TrafficSample {
                        upload_bytes_per_second: frame.upload_bytes_per_second,
                        download_bytes_per_second: frame.download_bytes_per_second,
                        sampled_at_unix_ms: Some(now),
                        state: SampleState::Fresh,
                    },
                )
            },
            Some(publish_stale_traffic),
        );
    })
}

fn spawn_connection_thread(
    threads: &mut Vec<JoinHandle<()>>,
    application: Arc<dyn BackgroundApplication>,
    core: Arc<dyn BackgroundCorePort>,
    clock: Arc<dyn Clock>,
    shutdown: Arc<ShutdownSignal>,
    timing: BackgroundTiming,
) -> io::Result<()> {
    spawn_owned(threads, "ratash-connections".to_owned(), move || {
        run_stream(
            application,
            core,
            clock,
            shutdown,
            timing,
            TelemetryStream::Connections,
            |port, handle| port.open_connection_stream(handle),
            |application, generation, frame, _now| {
                application.publish_connections(generation, frame)
            },
            None,
        );
    })
}

fn spawn_log_thread(
    threads: &mut Vec<JoinHandle<()>>,
    application: Arc<dyn BackgroundApplication>,
    core: Arc<dyn BackgroundCorePort>,
    clock: Arc<dyn Clock>,
    shutdown: Arc<ShutdownSignal>,
    timing: BackgroundTiming,
) -> io::Result<()> {
    spawn_owned(threads, "ratash-logs".to_owned(), move || {
        run_stream(
            application,
            core,
            clock,
            shutdown,
            timing,
            TelemetryStream::Logs,
            |port, handle| port.open_log_stream(handle),
            |application, generation, frame, now| {
                application.publish_core_log(
                    generation,
                    now,
                    map_log_level(frame.level),
                    LogSource::CoreApi,
                    truncate_log_message(frame.message),
                )
            },
            None,
        );
    })
}

#[expect(
    clippy::too_many_arguments,
    reason = "the stream worker keeps lifecycle dependencies and behavior callbacks explicit"
)]
fn run_stream<T: Send + 'static>(
    application: Arc<dyn BackgroundApplication>,
    core: Arc<dyn BackgroundCorePort>,
    clock: Arc<dyn Clock>,
    shutdown: Arc<ShutdownSignal>,
    timing: BackgroundTiming,
    kind: TelemetryStream,
    open: impl Fn(
        &dyn BackgroundCorePort,
        &ManagedCoreHandle,
    ) -> Result<Box<dyn CoreEventStream<T>>, MihomoError>,
    publish: impl Fn(
        &dyn BackgroundApplication,
        CoreInstanceGeneration,
        T,
        u64,
    ) -> Result<bool, ApplicationError>,
    publish_stale: Option<fn(&dyn BackgroundApplication, CoreInstanceGeneration, u64)>,
) {
    let mut backoff = ReconnectBackoff::new(
        timing.reconnect_initial_backoff,
        timing.reconnect_max_backoff,
    );
    let mut freshness = StreamFreshness::default();
    let mut published_state = PublishedStreamState::default();
    let mut log_gap_open = false;

    while !shutdown.is_requested() {
        let managed_core = match application.managed_core() {
            Ok(Some(core)) => core,
            Ok(None) | Err(_) => {
                if let Some(generation) = freshness.generation {
                    record_log_gap_once(application.as_ref(), kind, generation, &mut log_gap_open);
                    publish_failure_state(
                        application.as_ref(),
                        kind,
                        generation,
                        &mut freshness,
                        clock.now_unix_ms(),
                        timing.stream_stale_timeout,
                        None,
                        &mut published_state,
                        publish_stale,
                    );
                }
                if shutdown.wait(backoff.next_delay()) {
                    return;
                }
                continue;
            }
        };
        let generation = managed_core.instance_generation;
        if freshness.observe_generation(generation, clock.now_unix_ms()) {
            backoff.reset();
            log_gap_open = false;
        }
        published_state.publish(
            application.as_ref(),
            generation,
            kind,
            StreamState::Connecting,
        );

        let mut stream = match open(core.as_ref(), &managed_core) {
            Ok(stream) => stream,
            Err(error) => {
                record_log_gap_once(application.as_ref(), kind, generation, &mut log_gap_open);
                publish_failure_state(
                    application.as_ref(),
                    kind,
                    generation,
                    &mut freshness,
                    clock.now_unix_ms(),
                    timing.stream_stale_timeout,
                    Some(error.kind),
                    &mut published_state,
                    publish_stale,
                );
                if shutdown.wait(backoff.next_delay()) {
                    return;
                }
                continue;
            }
        };

        loop {
            if shutdown.is_requested() {
                stream.cancel();
                return;
            }
            let event = match stream.next_event() {
                Ok(Some(event)) => event,
                Ok(None) => {
                    record_log_gap_once(application.as_ref(), kind, generation, &mut log_gap_open);
                    publish_failure_state(
                        application.as_ref(),
                        kind,
                        generation,
                        &mut freshness,
                        clock.now_unix_ms(),
                        timing.stream_stale_timeout,
                        None,
                        &mut published_state,
                        publish_stale,
                    );
                    break;
                }
                Err(error) => {
                    record_log_gap_once(application.as_ref(), kind, generation, &mut log_gap_open);
                    publish_failure_state(
                        application.as_ref(),
                        kind,
                        generation,
                        &mut freshness,
                        clock.now_unix_ms(),
                        timing.stream_stale_timeout,
                        Some(error.kind),
                        &mut published_state,
                        publish_stale,
                    );
                    break;
                }
            };
            if shutdown.is_requested() {
                stream.cancel();
                return;
            }
            if event.instance_generation != generation
                || !generation_is_current(application.as_ref(), generation)
            {
                publish_failure_state(
                    application.as_ref(),
                    kind,
                    generation,
                    &mut freshness,
                    clock.now_unix_ms(),
                    timing.stream_stale_timeout,
                    None,
                    &mut published_state,
                    publish_stale,
                );
                break;
            }
            let now = clock.now_unix_ms();
            match publish(application.as_ref(), generation, event.payload, now) {
                Ok(true) => {
                    if kind == TelemetryStream::Logs {
                        log_gap_open = false;
                    }
                    freshness.observe_event(now);
                    backoff.reset();
                    published_state.publish(
                        application.as_ref(),
                        generation,
                        kind,
                        StreamState::Healthy,
                    );
                }
                Ok(false) | Err(_) => break,
            }
        }
        stream.cancel();
        if shutdown.wait(backoff.next_delay()) {
            return;
        }
    }
}

fn record_log_gap_once(
    application: &dyn BackgroundApplication,
    kind: TelemetryStream,
    generation: CoreInstanceGeneration,
    gap_open: &mut bool,
) {
    if kind == TelemetryStream::Logs
        && !*gap_open
        && application
            .record_core_log_drop(generation, 1)
            .is_ok_and(|accepted| accepted)
    {
        *gap_open = true;
    }
}

fn generation_is_current(
    application: &dyn BackgroundApplication,
    generation: CoreInstanceGeneration,
) -> bool {
    application
        .managed_core()
        .ok()
        .flatten()
        .is_some_and(|core| core.instance_generation == generation)
}

#[expect(
    clippy::too_many_arguments,
    reason = "failure publication keeps the stream transition inputs explicit"
)]
fn publish_failure_state(
    application: &dyn BackgroundApplication,
    kind: TelemetryStream,
    generation: CoreInstanceGeneration,
    freshness: &mut StreamFreshness,
    now_unix_ms: u64,
    stale_timeout: Duration,
    error: Option<MihomoErrorKind>,
    published_state: &mut PublishedStreamState,
    publish_stale: Option<fn(&dyn BackgroundApplication, CoreInstanceGeneration, u64)>,
) {
    let state = if freshness.is_stale(now_unix_ms, stale_timeout) {
        StreamState::Stale
    } else if matches!(
        error,
        Some(MihomoErrorKind::Unauthorized | MihomoErrorKind::InvalidResponse)
    ) {
        StreamState::Degraded
    } else {
        StreamState::Disconnected
    };
    published_state.publish(application, generation, kind, state);
    if state == StreamState::Stale && !freshness.stale_published {
        freshness.stale_published = true;
        if let Some(publish_stale) = publish_stale {
            publish_stale(application, generation, now_unix_ms);
        }
    }
}

fn publish_stale_traffic(
    application: &dyn BackgroundApplication,
    generation: CoreInstanceGeneration,
    now_unix_ms: u64,
) {
    let _ = application.publish_traffic(
        generation,
        TrafficSample {
            upload_bytes_per_second: 0,
            download_bytes_per_second: 0,
            sampled_at_unix_ms: Some(now_unix_ms),
            state: SampleState::Stale,
        },
    );
}

fn map_log_level(level: MihomoLogLevel) -> LogLevel {
    match level {
        MihomoLogLevel::Debug => LogLevel::Debug,
        MihomoLogLevel::Info => LogLevel::Info,
        MihomoLogLevel::Warn => LogLevel::Warn,
        MihomoLogLevel::Error => LogLevel::Error,
    }
}

const LOG_TRUNCATION_MARKER: &str = " [truncated]";

fn truncate_log_message(mut message: String) -> String {
    if message.len() <= CORE_LOG_LINE_MAX_BYTES {
        return message;
    }
    let mut end = CORE_LOG_LINE_MAX_BYTES - LOG_TRUNCATION_MARKER.len();
    while !message.is_char_boundary(end) {
        end -= 1;
    }
    message.truncate(end);
    message.push_str(LOG_TRUNCATION_MARKER);
    message
}

#[derive(Default)]
struct StreamFreshness {
    generation: Option<CoreInstanceGeneration>,
    observed_at_unix_ms: u64,
    last_event_at_unix_ms: Option<u64>,
    stale_published: bool,
}

impl StreamFreshness {
    fn observe_generation(&mut self, generation: CoreInstanceGeneration, now_unix_ms: u64) -> bool {
        if self.generation != Some(generation) {
            self.generation = Some(generation);
            self.observed_at_unix_ms = now_unix_ms;
            self.last_event_at_unix_ms = None;
            self.stale_published = false;
            true
        } else {
            false
        }
    }

    fn observe_event(&mut self, now_unix_ms: u64) {
        self.last_event_at_unix_ms = Some(now_unix_ms);
        self.stale_published = false;
    }

    fn is_stale(&self, now_unix_ms: u64, timeout: Duration) -> bool {
        let reference = self
            .last_event_at_unix_ms
            .unwrap_or(self.observed_at_unix_ms);
        now_unix_ms.saturating_sub(reference) >= duration_ms(timeout)
    }
}

#[derive(Default)]
struct PublishedStreamState {
    current: Option<(CoreInstanceGeneration, StreamState)>,
}

impl PublishedStreamState {
    fn publish(
        &mut self,
        application: &dyn BackgroundApplication,
        generation: CoreInstanceGeneration,
        kind: TelemetryStream,
        state: StreamState,
    ) {
        let next = (generation, state);
        if self.current == Some(next) {
            return;
        }
        self.current = Some(next);
        let _ = application.set_stream_state(generation, kind, state);
    }
}

struct ReconnectBackoff {
    initial: Duration,
    maximum: Duration,
    next: Duration,
}

impl ReconnectBackoff {
    fn new(initial: Duration, maximum: Duration) -> Self {
        debug_assert!(!initial.is_zero());
        debug_assert!(initial <= maximum);
        Self {
            initial,
            maximum,
            next: initial,
        }
    }

    fn next_delay(&mut self) -> Duration {
        let delay = self.next;
        self.next = self.next.saturating_mul(2).min(self.maximum);
        delay
    }

    fn reset(&mut self) {
        self.next = self.initial;
    }
}

#[derive(Default)]
struct ShutdownSignal {
    requested: Mutex<bool>,
    changed: Condvar,
}

struct StartGuard {
    shutdown: Arc<ShutdownSignal>,
    application: Arc<dyn BackgroundApplication>,
    core: Arc<dyn BackgroundCorePort>,
    threads: Vec<JoinHandle<()>>,
    armed: bool,
}

impl StartGuard {
    fn new(
        shutdown: Arc<ShutdownSignal>,
        application: Arc<dyn BackgroundApplication>,
        core: Arc<dyn BackgroundCorePort>,
    ) -> Self {
        Self {
            shutdown,
            application,
            core,
            threads: Vec::with_capacity(
                PROFILE_REFRESH_CONCURRENCY
                    .saturating_add(PROBE_WORKER_COUNT)
                    .saturating_add(4),
            ),
            armed: true,
        }
    }

    fn threads_mut(&mut self) -> &mut Vec<JoinHandle<()>> {
        &mut self.threads
    }

    fn finish(mut self) -> Vec<JoinHandle<()>> {
        self.armed = false;
        std::mem::take(&mut self.threads)
    }
}

impl Drop for StartGuard {
    fn drop(&mut self) {
        if self.armed {
            self.shutdown.request();
            self.application.cancel_pending_refreshes();
            self.core.cancel_pending();
            for thread in self.threads.drain(..) {
                let _ = thread.join();
            }
        }
    }
}

impl ShutdownSignal {
    fn request(&self) {
        let mut requested = self
            .requested
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *requested = true;
        self.changed.notify_all();
    }

    fn is_requested(&self) -> bool {
        *self
            .requested
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn wait(&self, duration: Duration) -> bool {
        let requested = self
            .requested
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if *requested {
            return true;
        }
        self.changed
            .wait_timeout_while(requested, duration, |requested| !*requested)
            .map_or_else(
                |poisoned| *poisoned.into_inner().0,
                |(requested, _)| *requested,
            )
    }
}

fn duration_ms(duration: Duration) -> u64 {
    duration.as_millis().try_into().unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deadline_wait_releases_a_stalled_background_thread_handle() {
        let (release, blocked) = std::sync::mpsc::channel::<()>();
        let worker = thread::spawn(move || {
            let _ = blocked.recv();
        });
        let started = Instant::now();

        assert!(!wait_until_finished(
            &worker,
            Instant::now() + Duration::from_millis(10)
        ));
        assert!(started.elapsed() < Duration::from_secs(1));

        release.send(()).expect("the fixture worker should release");
        worker.join().expect("the fixture worker should stop");
    }

    #[test]
    fn reconnect_backoff_doubles_and_stops_at_the_product_cap() {
        let mut backoff =
            ReconnectBackoff::new(Duration::from_millis(250), Duration::from_millis(1_000));

        assert_eq!(backoff.next_delay(), Duration::from_millis(250));
        assert_eq!(backoff.next_delay(), Duration::from_millis(500));
        assert_eq!(backoff.next_delay(), Duration::from_millis(1_000));
        assert_eq!(backoff.next_delay(), Duration::from_millis(1_000));
        backoff.reset();
        assert_eq!(backoff.next_delay(), Duration::from_millis(250));
    }

    #[test]
    fn freshness_uses_the_current_generation_and_latest_event() {
        let mut freshness = StreamFreshness::default();
        assert!(freshness.observe_generation(CoreInstanceGeneration(1), 1_000));
        assert!(!freshness.is_stale(10_999, Duration::from_secs(10)));
        assert!(freshness.is_stale(11_000, Duration::from_secs(10)));

        freshness.observe_event(12_000);
        assert!(!freshness.is_stale(21_999, Duration::from_secs(10)));
        assert!(freshness.observe_generation(CoreInstanceGeneration(2), 30_000));
        assert!(!freshness.is_stale(30_000, Duration::from_secs(10)));
    }

    #[test]
    fn ascii_log_truncation_appends_the_stable_marker_within_the_bound() {
        let message = "a".repeat(CORE_LOG_LINE_MAX_BYTES + 1);

        let truncated = truncate_log_message(message);

        assert_eq!(truncated.len(), CORE_LOG_LINE_MAX_BYTES);
        assert!(truncated.ends_with(LOG_TRUNCATION_MARKER));
    }

    #[test]
    fn log_truncation_preserves_a_utf8_boundary_before_the_marker() {
        let prefix_bytes = CORE_LOG_LINE_MAX_BYTES - LOG_TRUNCATION_MARKER.len();
        let mut message = "a".repeat(prefix_bytes - 1);
        message.push('\u{754c}');
        message.push_str(&"b".repeat(LOG_TRUNCATION_MARKER.len()));

        let truncated = truncate_log_message(message);

        assert!(truncated.len() <= CORE_LOG_LINE_MAX_BYTES);
        assert!(truncated.is_char_boundary(truncated.len()));
        assert!(truncated.ends_with(LOG_TRUNCATION_MARKER));
        assert_eq!(
            &truncated[..truncated.len() - LOG_TRUNCATION_MARKER.len()],
            "a".repeat(prefix_bytes - 1)
        );
    }
}
