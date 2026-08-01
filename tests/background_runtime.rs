use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use hopash::application::{ApplicationError, Clock};
use hopash::background::{BackgroundApplication, BackgroundCorePort, BackgroundRuntime};
use hopash::constants::{PROBE_WORKER_COUNT, PROFILE_REFRESH_CONCURRENCY};
use hopash::core::{
    ConnectionSummary, CoreControlEndpoint, CoreEvent, CoreEventStream, DelayProbeRequest,
    DelayTarget, ManagedCoreHandle, MihomoError, MihomoErrorKind, MihomoLogFrame, MihomoLogLevel,
    TrafficFrame,
};
use hopash::domain::{
    CoreInstanceGeneration, NodeRecordId, ProbeGeneration, RuntimeGeneration, StreamState,
    TrafficSample,
};
use hopash::profile::ProfileRevision;
use hopash::scheduler::{
    ProbeCompletion, ProbeCompletionStatus, ProbeOutcome, ProbeScheduler, ProfileRefreshScheduler,
    RefreshCompletion, RefreshTask,
};
use hopash::supervisor::{ProfileRefreshDisposition, ScheduledProbe, TelemetryStream};
use hopash::telemetry::{LogLevel, LogSource};

const WAIT_LIMIT: Duration = Duration::from_secs(3);

struct FixedClock {
    now: AtomicU64,
}

impl FixedClock {
    fn new(now: u64) -> Self {
        Self {
            now: AtomicU64::new(now),
        }
    }

    fn set(&self, now: u64) {
        self.now.store(now, Ordering::Release);
    }
}

impl Clock for FixedClock {
    fn now_unix_ms(&self) -> u64 {
        self.now.load(Ordering::Acquire)
    }
}

#[derive(Default)]
struct WorkGate {
    state: Mutex<GateState>,
    changed: Condvar,
}

#[derive(Default)]
struct GateState {
    active: usize,
    peak: usize,
    started: usize,
    released: bool,
}

impl WorkGate {
    fn enter(&self) {
        let mut state = self.state.lock().expect("work gate should lock");
        state.active += 1;
        state.started += 1;
        state.peak = state.peak.max(state.active);
        self.changed.notify_all();
        while !state.released {
            state = self.changed.wait(state).expect("work gate should wait");
        }
        state.active -= 1;
        self.changed.notify_all();
    }

    fn wait_for_started(&self, expected: usize) -> usize {
        let state = wait_for(&self.state, &self.changed, |state| {
            state.started >= expected
        });
        state.peak
    }

    fn release(&self) {
        let mut state = self.state.lock().expect("work gate should lock");
        state.released = true;
        self.changed.notify_all();
    }
}

#[derive(Default)]
struct Observed {
    refresh_completed: usize,
    probe_completions: Vec<ProbeCompletion>,
    traffic: Vec<(CoreInstanceGeneration, TrafficSample)>,
    connections: Vec<(CoreInstanceGeneration, u64)>,
    logs: Vec<(CoreInstanceGeneration, LogLevel, LogSource, String)>,
    log_drops: Vec<(CoreInstanceGeneration, u64)>,
    stream_states: Vec<(CoreInstanceGeneration, TelemetryStream, StreamState)>,
}

struct HarnessApplication {
    clock: Arc<FixedClock>,
    refreshes: Mutex<Option<ProfileRefreshScheduler>>,
    probes: Mutex<Option<ProbeScheduler>>,
    refresh_gate: Option<Arc<WorkGate>>,
    managed_generation: AtomicU64,
    managed_thread: Option<&'static str>,
    observed: Mutex<Observed>,
    changed: Condvar,
}

impl HarnessApplication {
    fn for_refresh(clock: Arc<FixedClock>, profile_count: usize, gate: Arc<WorkGate>) -> Self {
        let mut refreshes = ProfileRefreshScheduler::new();
        for revision in 1..=profile_count {
            refreshes.upsert(
                hopash::domain::ProfileId::new(),
                ProfileRevision(revision as u64),
                0,
            );
        }
        Self {
            clock,
            refreshes: Mutex::new(Some(refreshes)),
            probes: Mutex::new(None),
            refresh_gate: Some(gate),
            managed_generation: AtomicU64::new(0),
            managed_thread: None,
            observed: Mutex::new(Observed::default()),
            changed: Condvar::new(),
        }
    }

    fn for_probes(clock: Arc<FixedClock>, node_count: usize) -> Self {
        let mut probes = ProbeScheduler::new();
        probes
            .reset(
                ProbeGeneration(1),
                (0..node_count).map(|index| NodeRecordId::for_core(&format!("node-{index}"))),
                0,
            )
            .expect("probe generation should be accepted");
        Self {
            clock,
            refreshes: Mutex::new(None),
            probes: Mutex::new(Some(probes)),
            refresh_gate: None,
            managed_generation: AtomicU64::new(1),
            managed_thread: None,
            observed: Mutex::new(Observed::default()),
            changed: Condvar::new(),
        }
    }

    fn for_streams(clock: Arc<FixedClock>, generation: CoreInstanceGeneration) -> Self {
        Self {
            clock,
            refreshes: Mutex::new(None),
            probes: Mutex::new(None),
            refresh_gate: None,
            managed_generation: AtomicU64::new(generation.0),
            managed_thread: None,
            observed: Mutex::new(Observed::default()),
            changed: Condvar::new(),
        }
    }

    fn for_traffic_stream(clock: Arc<FixedClock>, generation: CoreInstanceGeneration) -> Self {
        Self {
            managed_thread: Some("hopash-traffic"),
            ..Self::for_streams(clock, generation)
        }
    }

    fn wait_for_refreshes(&self, expected: usize) {
        drop(wait_for(&self.observed, &self.changed, |observed| {
            observed.refresh_completed >= expected
        }));
    }

    fn wait_for_probes(&self, expected: usize) {
        drop(wait_for(&self.observed, &self.changed, |observed| {
            observed.probe_completions.len() >= expected
        }));
    }

    fn wait_for_telemetry(&self) {
        drop(wait_for(&self.observed, &self.changed, |observed| {
            !observed.traffic.is_empty()
                && !observed.connections.is_empty()
                && !observed.logs.is_empty()
        }));
    }

    fn wait_for_log_gap(&self) {
        drop(wait_for(&self.observed, &self.changed, |observed| {
            !observed.log_drops.is_empty()
        }));
    }

    fn wait_for_stale_traffic(&self) {
        drop(wait_for(&self.observed, &self.changed, |observed| {
            observed
                .traffic
                .iter()
                .any(|(_, sample)| sample.state == hopash::domain::SampleState::Stale)
        }));
    }
}

impl BackgroundApplication for HarnessApplication {
    fn cancel_pending_refreshes(&self) {
        if let Some(gate) = &self.refresh_gate {
            gate.release();
        }
    }

    fn take_due_refreshes(&self) -> Result<Vec<RefreshTask>, ApplicationError> {
        Ok(self
            .refreshes
            .lock()
            .expect("refresh scheduler should lock")
            .as_mut()
            .map_or_else(Vec::new, |scheduler| {
                scheduler.take_due(self.clock.now_unix_ms())
            }))
    }

    fn execute_refresh_task(
        &self,
        task: RefreshTask,
    ) -> Result<ProfileRefreshDisposition, ApplicationError> {
        if let Some(gate) = &self.refresh_gate {
            gate.enter();
        }
        if let Some(scheduler) = self
            .refreshes
            .lock()
            .expect("refresh scheduler should lock")
            .as_mut()
        {
            scheduler.complete(RefreshCompletion {
                task,
                profile_revision: task.profile_revision,
                completed_at_unix_ms: self.clock.now_unix_ms(),
            });
        }
        let mut observed = self.observed.lock().expect("observations should lock");
        observed.refresh_completed += 1;
        self.changed.notify_all();
        Ok(ProfileRefreshDisposition::InactiveStored)
    }

    fn take_due_probes(&self) -> Result<Vec<ScheduledProbe>, ApplicationError> {
        Ok(self
            .probes
            .lock()
            .expect("probe scheduler should lock")
            .as_mut()
            .map_or_else(Vec::new, |scheduler| {
                scheduler
                    .take_due(self.clock.now_unix_ms())
                    .into_iter()
                    .map(|task| ScheduledProbe {
                        request: Some(DelayProbeRequest {
                            record_id: task.node_id.clone(),
                            target: DelayTarget::CoreProxy {
                                proxy_name: "fixture-node".to_owned(),
                            },
                            test_url: "https://example.invalid/generate_204".to_owned(),
                            timeout_ms: 5_000,
                        }),
                        task,
                    })
                    .collect()
            }))
    }

    fn complete_probe(
        &self,
        completion: ProbeCompletion,
    ) -> Result<ProbeCompletionStatus, ApplicationError> {
        let status = self
            .probes
            .lock()
            .expect("probe scheduler should lock")
            .as_mut()
            .map_or(ProbeCompletionStatus::UnknownTask, |scheduler| {
                scheduler.complete(completion.clone())
            });
        let mut observed = self.observed.lock().expect("observations should lock");
        observed.probe_completions.push(completion);
        self.changed.notify_all();
        Ok(status)
    }

    fn managed_core(&self) -> Result<Option<ManagedCoreHandle>, ApplicationError> {
        if self.managed_thread.is_some_and(|expected| {
            std::thread::current()
                .name()
                .is_none_or(|name| name != expected)
        }) {
            return Ok(None);
        }
        let generation = self.managed_generation.load(Ordering::Acquire);
        Ok((generation > 0).then(|| managed_core(CoreInstanceGeneration(generation))))
    }

    fn set_stream_state(
        &self,
        generation: CoreInstanceGeneration,
        stream: TelemetryStream,
        state: StreamState,
    ) -> Result<bool, ApplicationError> {
        if generation.0 != self.managed_generation.load(Ordering::Acquire) {
            return Ok(false);
        }
        let mut observed = self.observed.lock().expect("observations should lock");
        observed.stream_states.push((generation, stream, state));
        self.changed.notify_all();
        Ok(true)
    }

    fn publish_traffic(
        &self,
        generation: CoreInstanceGeneration,
        sample: TrafficSample,
    ) -> Result<bool, ApplicationError> {
        if generation.0 != self.managed_generation.load(Ordering::Acquire) {
            return Ok(false);
        }
        let mut observed = self.observed.lock().expect("observations should lock");
        observed.traffic.push((generation, sample));
        self.changed.notify_all();
        Ok(true)
    }

    fn publish_connection_count(
        &self,
        generation: CoreInstanceGeneration,
        count: u64,
    ) -> Result<bool, ApplicationError> {
        if generation.0 != self.managed_generation.load(Ordering::Acquire) {
            return Ok(false);
        }
        let mut observed = self.observed.lock().expect("observations should lock");
        observed.connections.push((generation, count));
        self.changed.notify_all();
        Ok(true)
    }

    fn publish_core_log(
        &self,
        generation: CoreInstanceGeneration,
        _timestamp_unix_ms: u64,
        level: LogLevel,
        source: LogSource,
        message: String,
    ) -> Result<bool, ApplicationError> {
        if generation.0 != self.managed_generation.load(Ordering::Acquire) {
            return Ok(false);
        }
        let mut observed = self.observed.lock().expect("observations should lock");
        observed.logs.push((generation, level, source, message));
        self.changed.notify_all();
        Ok(true)
    }

    fn record_core_log_drop(
        &self,
        generation: CoreInstanceGeneration,
        count: u64,
    ) -> Result<bool, ApplicationError> {
        if generation.0 != self.managed_generation.load(Ordering::Acquire) {
            return Ok(false);
        }
        let mut observed = self.observed.lock().expect("observations should lock");
        observed.log_drops.push((generation, count));
        self.changed.notify_all();
        Ok(true)
    }
}

struct HarnessCore {
    probe_gate: Option<Arc<WorkGate>>,
    blocking_stream_gate: Option<Arc<WorkGate>>,
    event_generation: Option<CoreInstanceGeneration>,
    traffic_opened: AtomicBool,
    connections_opened: AtomicBool,
    logs_opened: AtomicBool,
    stale_traffic_clock: Option<Arc<FixedClock>>,
    events_read: Arc<(Mutex<usize>, Condvar)>,
}

impl HarnessCore {
    fn unavailable() -> Self {
        Self {
            probe_gate: None,
            blocking_stream_gate: None,
            event_generation: None,
            traffic_opened: AtomicBool::new(false),
            connections_opened: AtomicBool::new(false),
            logs_opened: AtomicBool::new(false),
            stale_traffic_clock: None,
            events_read: Arc::new((Mutex::new(0), Condvar::new())),
        }
    }

    fn for_probes(gate: Arc<WorkGate>) -> Self {
        Self {
            probe_gate: Some(gate),
            ..Self::unavailable()
        }
    }

    fn for_streams(event_generation: CoreInstanceGeneration) -> Self {
        Self {
            event_generation: Some(event_generation),
            ..Self::unavailable()
        }
    }

    fn for_blocking_traffic(event_generation: CoreInstanceGeneration, gate: Arc<WorkGate>) -> Self {
        Self {
            blocking_stream_gate: Some(gate),
            event_generation: Some(event_generation),
            ..Self::unavailable()
        }
    }

    fn for_stale_traffic(clock: Arc<FixedClock>) -> Self {
        Self {
            stale_traffic_clock: Some(clock),
            ..Self::unavailable()
        }
    }

    fn wait_for_events_read(&self, expected: usize) {
        let (count, changed) = &*self.events_read;
        drop(wait_for(count, changed, |count| *count >= expected));
    }
}

impl BackgroundCorePort for HarnessCore {
    fn cancel_pending(&self) {
        if let Some(gate) = &self.probe_gate {
            gate.release();
        }
        if let Some(gate) = &self.blocking_stream_gate {
            gate.release();
        }
    }

    fn probe_delay(
        &self,
        _core: &ManagedCoreHandle,
        _request: &DelayProbeRequest,
    ) -> Result<u64, MihomoError> {
        if let Some(gate) = &self.probe_gate {
            gate.enter();
            Ok(42)
        } else {
            Err(unavailable())
        }
    }

    fn open_traffic_stream(
        &self,
        _core: &ManagedCoreHandle,
    ) -> Result<Box<dyn CoreEventStream<TrafficFrame>>, MihomoError> {
        if let Some(clock) = &self.stale_traffic_clock {
            if !self.traffic_opened.swap(true, Ordering::AcqRel) {
                clock.set(10_000);
                return Err(MihomoError::new(
                    MihomoErrorKind::StreamClosed,
                    "fixture traffic stream closed",
                ));
            }
            return Err(unavailable());
        }
        let generation = self.event_generation.ok_or_else(unavailable)?;
        if self.traffic_opened.swap(true, Ordering::AcqRel) {
            return Err(unavailable());
        }
        if let Some(gate) = &self.blocking_stream_gate {
            return Ok(Box::new(BlockingEventStream {
                gate: Arc::clone(gate),
            }));
        }
        Ok(Box::new(SingleEventStream::new(
            generation,
            TrafficFrame {
                upload_bytes_per_second: 11,
                download_bytes_per_second: 29,
            },
            Arc::clone(&self.events_read),
        )))
    }

    fn open_connection_stream(
        &self,
        _core: &ManagedCoreHandle,
    ) -> Result<Box<dyn CoreEventStream<ConnectionSummary>>, MihomoError> {
        let generation = self.event_generation.ok_or_else(unavailable)?;
        if self.connections_opened.swap(true, Ordering::AcqRel) {
            return Err(unavailable());
        }
        Ok(Box::new(SingleEventStream::new(
            generation,
            ConnectionSummary {
                active_connections: 7,
                upload_total_bytes: 100,
                download_total_bytes: 200,
                memory_bytes: Some(300),
            },
            Arc::clone(&self.events_read),
        )))
    }

    fn open_log_stream(
        &self,
        _core: &ManagedCoreHandle,
    ) -> Result<Box<dyn CoreEventStream<MihomoLogFrame>>, MihomoError> {
        let generation = self.event_generation.ok_or_else(unavailable)?;
        if self.logs_opened.swap(true, Ordering::AcqRel) {
            return Err(unavailable());
        }
        Ok(Box::new(SingleEventStream::new(
            generation,
            MihomoLogFrame {
                level: MihomoLogLevel::Warn,
                message: "fixture warning".to_owned(),
            },
            Arc::clone(&self.events_read),
        )))
    }
}

struct SingleEventStream<T> {
    event: Option<CoreEvent<T>>,
    events_read: Arc<(Mutex<usize>, Condvar)>,
    cancelled: bool,
}

impl<T> SingleEventStream<T> {
    fn new(
        generation: CoreInstanceGeneration,
        payload: T,
        events_read: Arc<(Mutex<usize>, Condvar)>,
    ) -> Self {
        Self {
            event: Some(CoreEvent {
                instance_generation: generation,
                payload,
            }),
            events_read,
            cancelled: false,
        }
    }
}

impl<T: Send> CoreEventStream<T> for SingleEventStream<T> {
    fn next_event(&mut self) -> Result<Option<CoreEvent<T>>, MihomoError> {
        if self.cancelled {
            return Ok(None);
        }
        if self.event.is_some() {
            let (count, changed) = &*self.events_read;
            *count.lock().expect("event count should lock") += 1;
            changed.notify_all();
        }
        Ok(self.event.take())
    }

    fn cancel(&mut self) {
        self.cancelled = true;
    }
}

struct BlockingEventStream {
    gate: Arc<WorkGate>,
}

impl CoreEventStream<TrafficFrame> for BlockingEventStream {
    fn next_event(&mut self) -> Result<Option<CoreEvent<TrafficFrame>>, MihomoError> {
        self.gate.enter();
        Ok(None)
    }

    fn cancel(&mut self) {
        self.gate.release();
    }
}

#[test]
fn overdue_refresh_execution_uses_the_fixed_worker_limit() {
    let clock = Arc::new(FixedClock::new(0));
    let gate = Arc::new(WorkGate::default());
    let application = Arc::new(HarnessApplication::for_refresh(
        Arc::clone(&clock),
        PROFILE_REFRESH_CONCURRENCY + 2,
        Arc::clone(&gate),
    ));
    let core = Arc::new(HarnessCore::unavailable());
    let mut runtime = start_runtime(&application, &core, &clock);

    let peak = gate.wait_for_started(PROFILE_REFRESH_CONCURRENCY);
    assert_eq!(peak, PROFILE_REFRESH_CONCURRENCY);
    gate.release();
    application.wait_for_refreshes(PROFILE_REFRESH_CONCURRENCY);

    runtime.shutdown().expect("background runtime should stop");
}

#[test]
fn shutdown_cancels_an_in_flight_profile_refresh() {
    let clock = Arc::new(FixedClock::new(0));
    let gate = Arc::new(WorkGate::default());
    let application = Arc::new(HarnessApplication::for_refresh(
        Arc::clone(&clock),
        1,
        Arc::clone(&gate),
    ));
    let core = Arc::new(HarnessCore::unavailable());
    let mut runtime = start_runtime(&application, &core, &clock);

    gate.wait_for_started(1);
    let started = Instant::now();
    runtime.shutdown().expect("background runtime should stop");

    assert!(started.elapsed() < Duration::from_secs(1));
}

#[test]
fn delay_probe_execution_uses_fixed_workers_and_publishes_completions() {
    let clock = Arc::new(FixedClock::new(55));
    let gate = Arc::new(WorkGate::default());
    let application = Arc::new(HarnessApplication::for_probes(
        Arc::clone(&clock),
        PROBE_WORKER_COUNT + 4,
    ));
    let core = Arc::new(HarnessCore::for_probes(Arc::clone(&gate)));
    let mut runtime = start_runtime(&application, &core, &clock);

    let peak = gate.wait_for_started(PROBE_WORKER_COUNT);
    assert_eq!(peak, PROBE_WORKER_COUNT);
    gate.release();
    application.wait_for_probes(PROBE_WORKER_COUNT);

    let observed = application
        .observed
        .lock()
        .expect("observations should lock");
    assert!(observed.probe_completions.iter().all(|completion| {
        completion.outcome == ProbeOutcome::Success { delay_ms: 42 }
            && completion.task.generation == ProbeGeneration(1)
            && completion.completed_at_unix_ms == 55
    }));
    drop(observed);

    runtime.shutdown().expect("background runtime should stop");
}

#[test]
fn shutdown_cancels_an_in_flight_delay_probe() {
    let clock = Arc::new(FixedClock::new(55));
    let gate = Arc::new(WorkGate::default());
    let application = Arc::new(HarnessApplication::for_probes(Arc::clone(&clock), 1));
    let core = Arc::new(HarnessCore::for_probes(Arc::clone(&gate)));
    let mut runtime = start_runtime(&application, &core, &clock);

    gate.wait_for_started(1);
    let started = Instant::now();
    runtime.shutdown().expect("background runtime should stop");

    assert!(started.elapsed() < Duration::from_secs(1));
}

#[test]
fn shutdown_wakes_a_blocked_telemetry_stream() {
    let generation = CoreInstanceGeneration(10);
    let clock = Arc::new(FixedClock::new(0));
    let gate = Arc::new(WorkGate::default());
    let application = Arc::new(HarnessApplication::for_traffic_stream(
        Arc::clone(&clock),
        generation,
    ));
    let core = Arc::new(HarnessCore::for_blocking_traffic(
        generation,
        Arc::clone(&gate),
    ));
    let mut runtime = start_runtime(&application, &core, &clock);

    gate.wait_for_started(1);
    let started = Instant::now();
    runtime.shutdown().expect("background runtime should stop");

    assert!(started.elapsed() < Duration::from_secs(1));
}

#[test]
fn telemetry_streams_publish_fresh_generation_scoped_values() {
    let generation = CoreInstanceGeneration(5);
    let clock = Arc::new(FixedClock::new(9_000));
    let application = Arc::new(HarnessApplication::for_streams(
        Arc::clone(&clock),
        generation,
    ));
    let core = Arc::new(HarnessCore::for_streams(generation));
    let mut runtime = start_runtime(&application, &core, &clock);

    application.wait_for_telemetry();
    let observed = application
        .observed
        .lock()
        .expect("observations should lock");
    assert_eq!(
        observed.traffic,
        vec![(
            generation,
            TrafficSample {
                upload_bytes_per_second: 11,
                download_bytes_per_second: 29,
                sampled_at_unix_ms: Some(9_000),
                state: hopash::domain::SampleState::Fresh,
            }
        )]
    );
    assert_eq!(observed.connections, vec![(generation, 7)]);
    assert_eq!(
        observed.logs,
        vec![(
            generation,
            LogLevel::Warn,
            LogSource::CoreApi,
            "fixture warning".to_owned(),
        )]
    );
    for stream in [
        TelemetryStream::Traffic,
        TelemetryStream::Connections,
        TelemetryStream::Logs,
    ] {
        assert!(
            observed
                .stream_states
                .contains(&(generation, stream, StreamState::Healthy))
        );
    }
    drop(observed);

    runtime.shutdown().expect("background runtime should stop");
}

#[test]
fn a_log_stream_disconnect_records_one_gap_for_the_reconnect_episode() {
    let generation = CoreInstanceGeneration(6);
    let clock = Arc::new(FixedClock::new(10_000));
    let application = Arc::new(HarnessApplication::for_streams(
        Arc::clone(&clock),
        generation,
    ));
    let core = Arc::new(HarnessCore::for_streams(generation));
    let mut runtime = start_runtime(&application, &core, &clock);

    application.wait_for_telemetry();
    application.wait_for_log_gap();
    runtime.shutdown().expect("background runtime should stop");

    let observed = application
        .observed
        .lock()
        .expect("observations should lock");
    assert_eq!(observed.log_drops, vec![(generation, 1)]);
}

#[test]
fn stale_core_generation_events_are_discarded_before_publication() {
    let current_generation = CoreInstanceGeneration(8);
    let clock = Arc::new(FixedClock::new(1_000));
    let application = Arc::new(HarnessApplication::for_streams(
        Arc::clone(&clock),
        current_generation,
    ));
    let core = Arc::new(HarnessCore::for_streams(CoreInstanceGeneration(7)));
    let mut runtime = start_runtime(&application, &core, &clock);

    core.wait_for_events_read(3);
    runtime.shutdown().expect("background runtime should stop");

    let observed = application
        .observed
        .lock()
        .expect("observations should lock");
    assert!(observed.traffic.is_empty());
    assert!(observed.connections.is_empty());
    assert!(observed.logs.is_empty());
}

#[test]
fn stale_traffic_publishes_an_explicit_zero_sample_once() {
    let generation = CoreInstanceGeneration(9);
    let clock = Arc::new(FixedClock::new(0));
    let application = Arc::new(HarnessApplication::for_traffic_stream(
        Arc::clone(&clock),
        generation,
    ));
    let core = Arc::new(HarnessCore::for_stale_traffic(Arc::clone(&clock)));
    let mut runtime = start_runtime(&application, &core, &clock);

    application.wait_for_stale_traffic();
    runtime.shutdown().expect("background runtime should stop");

    let observed = application
        .observed
        .lock()
        .expect("observations should lock");
    assert_eq!(
        observed.traffic,
        vec![(
            generation,
            TrafficSample {
                upload_bytes_per_second: 0,
                download_bytes_per_second: 0,
                sampled_at_unix_ms: Some(10_000),
                state: hopash::domain::SampleState::Stale,
            }
        )]
    );
    assert!(observed.stream_states.contains(&(
        generation,
        TelemetryStream::Traffic,
        StreamState::Stale,
    )));
}

fn start_runtime(
    application: &Arc<HarnessApplication>,
    core: &Arc<HarnessCore>,
    clock: &Arc<FixedClock>,
) -> BackgroundRuntime {
    let application: Arc<dyn BackgroundApplication> = application.clone();
    let core: Arc<dyn BackgroundCorePort> = core.clone();
    let clock: Arc<dyn Clock> = clock.clone();
    BackgroundRuntime::start(application, core, clock).expect("background runtime should start")
}

fn managed_core(generation: CoreInstanceGeneration) -> ManagedCoreHandle {
    ManagedCoreHandle {
        pid: 123,
        process_start_identity: "fixture-start".to_owned(),
        endpoint: CoreControlEndpoint::new("/tmp/hopash-background-fixture.sock", "fixture-secret"),
        instance_generation: generation,
        runtime_generation: RuntimeGeneration(1),
    }
}

fn unavailable() -> MihomoError {
    MihomoError::new(MihomoErrorKind::Unavailable, "fixture stream unavailable")
}

fn wait_for<'a, T>(
    mutex: &'a Mutex<T>,
    changed: &Condvar,
    ready: impl Fn(&T) -> bool,
) -> std::sync::MutexGuard<'a, T> {
    let deadline = Instant::now() + WAIT_LIMIT;
    let mut value = mutex.lock().expect("test state should lock");
    while !ready(&value) {
        let remaining = deadline.saturating_duration_since(Instant::now());
        assert!(
            !remaining.is_zero(),
            "timed out waiting for background work"
        );
        let (next, timeout) = changed
            .wait_timeout(value, remaining)
            .expect("test state should wait");
        value = next;
        assert!(
            !timeout.timed_out() || ready(&value),
            "timed out waiting for background work"
        );
    }
    value
}
