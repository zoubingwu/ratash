use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::Condvar;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, TryLockError};

use crate::application::{
    ApplicationClient, ApplicationError, ApplicationOperation, ApplicationOutput, Clock,
    LatencyListOutcome, LatencyShowOutcome, ProfileListOutcome, ProfileListPageOutcome,
    ProfileMutationAction, ProfileMutationOutcome, ProxyListOutcome, ProxyListPageOutcome,
    ProxySelectionOutcome, RecoveryOutcome as ApplicationRecoveryOutcome, RecoveryStatus,
    RuleListOutcome, RuleListPageOutcome, RuleMutationAction, RuleMutationOutcome,
    SelectorIdentity,
};
use crate::config::{
    AuthoritativeConfig, ConfigCompiler, CoreConfigValidator, EffectiveConfiguration,
};
use crate::constants::{
    IPC_LIST_PAGE_SIZE, PROBE_TIMEOUT, PROBE_URL, PROFILE_COUNT_MAX, PROFILE_REFRESH_INTERVAL,
    RULE_STRING_MAX_BYTES, SELECTION_RESTORE_ATTEMPT_LIMIT, WRAPPER_DIAGNOSTIC_CAPACITY,
    YAML_MAX_DEPTH,
};
use crate::core::{
    ConnectionSummary, DelayProbeRequest, DelayTarget, ManagedCoreHandle, ProxyView,
};
use crate::diagnostics::{WrapperDiagnosticRing, WrapperDiagnosticState, WrapperDiagnosticTail};
use crate::domain::{
    ActiveProfileSummary, CoreInstanceGeneration, ProbeGeneration, ProfileId, RuntimeApplyPhase,
    RuntimeApplySnapshot, RuntimeGeneration, RuntimeRecoverySnapshot, StatusSnapshot,
    StreamHealthSet, StreamState, SubscriptionUrl, SupervisorHealthReason, SupervisorLifecycle,
    SupervisorStatus, TrafficSample,
};
use crate::error::ErrorCode;
use crate::profile::{
    Profile, ProfileCatalog, ProfileRevision, ProfileSnapshot, RefreshContext, RefreshFailure,
    RefreshStage, SnapshotLimits, derive_profile_name,
};
use crate::rule::{LocalRuleSet, RulePlacement, RuleString, RuleStringError};
use crate::scheduler::{
    ProbeCompletion, ProbeCompletionStatus, ProbeScheduler, ProbeTask, ProfileRefreshScheduler,
    RefreshCompletion, RefreshCompletionStatus, RefreshTask,
};
use crate::state::AuthoritativeStateStore;
use crate::telemetry::{LogLevel, LogSource, LogTail, TelemetryStore};
use crate::transaction::{CandidateRevisions, ConfigTransactionSuccess};

// -----------------------------------------------------------------------------
// External application ports
// -----------------------------------------------------------------------------

mod errors;
mod outcomes;
mod ports;
mod projections;
#[cfg(test)]
mod tests;
mod transactions;

pub use ports::{
    BlockingProfileFetchPort, DirectSupervisorCorePort, FetchedProfile, ProfileFetchError,
    ProfileFetchPort, SupervisorCorePort,
};
pub use transactions::{
    CoordinatedSupervisorTransactions, RuntimeBundleStagePort, SupervisorRevisionAuthority,
    SupervisorRuleTransactionReservation, SupervisorTransactionFailure,
    SupervisorTransactionFailureKind, SupervisorTransactionPort, SupervisorTransactionRequest,
};

use errors::{
    bounded_message, checked_rule, core_error, internal_error, invalid_rule_error,
    map_config_error, map_profile_error, map_rule_error, map_rule_placement, map_selection_error,
    map_transaction_error, no_active_profile, refresh_stage_for_transaction_failure,
    resolve_profile, rule_busy_error,
};
use outcomes::{
    failed_runtime_apply_snapshot, recovery_requires_degraded, runtime_apply_not_required,
    runtime_apply_success, successful_runtime_apply_snapshot,
};
use projections::{
    CoreHealthProjection, disconnected_stream_health, effective_group_order, empty_log_tail,
    ensure_telemetry, initial_status_snapshot, latency_summary, probe_eligible_node,
    probe_observations, probe_observations_page, probe_queue_status, profile_list_snapshot_id,
    profile_summary, proxy_group_summary, proxy_list_snapshot_id, proxy_row, refresh_is_stale,
    resolve_latency_node, resolve_proxy_group, rule_summary, selection_by_selector,
    status_proxy_fields, unavailable_traffic, wrapper_diagnostic_context,
};

pub struct SupervisorDependencies {
    pub clock: Arc<dyn Clock>,
    pub source: Arc<dyn ProfileFetchPort>,
    pub compiler: ConfigCompiler,
    pub validator: Arc<dyn CoreConfigValidator + Send + Sync>,
    pub transactions: Arc<dyn SupervisorTransactionPort>,
    pub state_store: Arc<AuthoritativeStateStore>,
    pub core: Arc<dyn SupervisorCorePort>,
    pub authoritative: AuthoritativeConfig,
    pub staging_root: PathBuf,
}

struct SupervisorState {
    profiles: ProfileCatalog,
    local_rules: LocalRuleSet,
    effective_configuration: Option<EffectiveConfiguration>,
    runtime_generation: Option<RuntimeGeneration>,
    refreshes: ProfileRefreshScheduler,
    probes: ProbeScheduler,
    next_probe_generation: u64,
    telemetry: Option<TelemetryStore>,
    telemetry_generation: Option<CoreInstanceGeneration>,
    stream_health: StreamHealthSet,
    cached_proxy_view: Option<ProxyView>,
    observed_core_generation: Option<CoreInstanceGeneration>,
    probe_core_generation: Option<CoreInstanceGeneration>,
    selection_restore_pending: bool,
    selection_restore_attempts_remaining: usize,
    health_reasons: BTreeSet<SupervisorHealthReason>,
    wrapper_diagnostics: WrapperDiagnosticRing,
}

#[derive(Default)]
struct ActivationQueue {
    running: bool,
    pending: Option<PendingActivation>,
}

impl ActivationQueue {
    fn enqueue(&mut self, selector: &str) -> Arc<ActivationCompletion> {
        let completion = Arc::new(ActivationCompletion::default());
        if let Some(superseded) = self.pending.replace(PendingActivation {
            selector: selector.to_owned(),
            completion: Arc::clone(&completion),
        }) {
            superseded.completion.complete(Err(ApplicationError::new(
                ErrorCode::OperationUnavailable,
                "Profile activation was superseded by a newer request",
                true,
            )));
        }
        completion
    }
}

struct PendingActivation {
    selector: String,
    completion: Arc<ActivationCompletion>,
}

#[derive(Default)]
struct ActivationCompletion {
    result: Mutex<Option<Result<ApplicationOutput, ApplicationError>>>,
    ready: Condvar,
}

struct ApplyActivity<'a> {
    in_progress: &'a AtomicBool,
}

impl Drop for ApplyActivity<'_> {
    fn drop(&mut self) {
        self.in_progress.store(false, Ordering::Release);
    }
}

impl ActivationCompletion {
    fn complete(&self, result: Result<ApplicationOutput, ApplicationError>) {
        let mut slot = self
            .result
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *slot = Some(result);
        self.ready.notify_one();
    }

    fn wait(&self) -> Result<ApplicationOutput, ApplicationError> {
        let mut slot = self.result.lock().map_err(|_| internal_error())?;
        while slot.is_none() {
            slot = self.ready.wait(slot).map_err(|_| internal_error())?;
        }
        slot.take().ok_or_else(internal_error)?
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProfileRefreshDisposition {
    ActiveApplied,
    InactiveStored,
    Discarded,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TelemetryStream {
    Traffic,
    Connections,
    Logs,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScheduledProbe {
    pub task: ProbeTask,
    pub request: Option<DelayProbeRequest>,
}

pub struct Supervisor {
    clock: Arc<dyn Clock>,
    started_at_unix_ms: u64,
    source: Arc<dyn ProfileFetchPort>,
    compiler: ConfigCompiler,
    validator: Arc<dyn CoreConfigValidator + Send + Sync>,
    transactions: Arc<dyn SupervisorTransactionPort>,
    core: Arc<dyn SupervisorCorePort>,
    authoritative: AuthoritativeConfig,
    staging_root: PathBuf,
    state: Mutex<SupervisorState>,
    activation: Mutex<ActivationQueue>,
    apply_in_progress: AtomicBool,
    last_runtime_apply: Mutex<RuntimeApplySnapshot>,
    last_status: Mutex<StatusSnapshot>,
}

impl Supervisor {
    pub fn open(dependencies: SupervisorDependencies) -> Result<Self, ApplicationError> {
        std::fs::create_dir_all(&dependencies.staging_root).map_err(|_| internal_error())?;
        let staging_root =
            std::fs::canonicalize(&dependencies.staging_root).map_err(|_| internal_error())?;
        let hydrated = dependencies
            .state_store
            .load_committed(
                SnapshotLimits::new(crate::constants::PROFILE_RESPONSE_MAX_BYTES, YAML_MAX_DEPTH),
                crate::rule::RuleSetLimits::product(),
            )
            .map_err(|_| internal_error())?;

        let (profiles, local_rules, effective_configuration, committed_generation) =
            if let Some(hydrated) = hydrated {
                let active_id = hydrated
                    .profiles
                    .active_profile_id()
                    .ok_or_else(internal_error)?;
                let active = hydrated
                    .profiles
                    .get(active_id)
                    .ok_or_else(internal_error)?;
                let rules = rule_strings(&hydrated.local_rules)?;
                let workspace = prepare_profile_workspace(&staging_root, active_id)?;
                dependencies
                    .compiler
                    .validate_persisted(
                        &active.snapshot,
                        &rules,
                        &hydrated.effective_configuration,
                        &workspace,
                    )
                    .map_err(map_config_error)?;
                let configuration = dependencies
                    .compiler
                    .compile(
                        &active.snapshot,
                        &rules,
                        &dependencies.authoritative,
                        &workspace,
                    )
                    .map_err(map_config_error)?;
                (
                    hydrated.profiles,
                    hydrated.local_rules,
                    Some(configuration),
                    Some(hydrated.runtime_generation),
                )
            } else {
                (
                    ProfileCatalog::new(),
                    LocalRuleSet::uninitialized(),
                    None,
                    None,
                )
            };

        let mut refreshes = ProfileRefreshScheduler::new();
        for profile in profiles.profiles() {
            refreshes.upsert(
                profile.id,
                profile.revision,
                profile.next_refresh_at_unix_ms,
            );
        }
        let revisions =
            current_revisions(&profiles, &local_rules, effective_configuration.as_ref());
        dependencies.transactions.set_current_revisions(revisions);
        let mut runtime_generation = committed_generation;
        let mut startup_runtime_apply = RuntimeApplySnapshot {
            committed_generation,
            ..RuntimeApplySnapshot::default()
        };
        let mut startup_health_reasons = BTreeSet::new();
        if let Some(configuration) = effective_configuration.as_ref() {
            let generation = dependencies
                .transactions
                .prepare_startup_reapply(committed_generation)
                .map_err(map_transaction_error)?;
            let result = dependencies
                .transactions
                .apply_startup(SupervisorTransactionRequest {
                    profiles: &profiles,
                    local_rules: &local_rules,
                    configuration,
                    generation,
                    revisions: current_revisions(&profiles, &local_rules, Some(configuration)),
                });
            let success = result.map_err(map_transaction_error)?;
            if recovery_requires_degraded(success.recovery) {
                startup_health_reasons.insert(SupervisorHealthReason::RuntimeRecovery);
            }
            runtime_generation = Some(success.committed_generation);
            startup_runtime_apply = successful_runtime_apply_snapshot(success);
        } else {
            let _ = dependencies
                .transactions
                .prepare_startup_reapply(None)
                .map_err(map_transaction_error)?;
        }
        let started_at_unix_ms = dependencies.clock.now_unix_ms();
        let initial_status = initial_status_snapshot(
            started_at_unix_ms,
            &profiles,
            runtime_generation,
            startup_runtime_apply.clone(),
            startup_health_reasons.iter().copied().collect(),
        );
        let supervisor = Self {
            clock: dependencies.clock,
            started_at_unix_ms,
            source: dependencies.source,
            compiler: dependencies.compiler,
            validator: dependencies.validator,
            transactions: dependencies.transactions,
            core: dependencies.core,
            authoritative: dependencies.authoritative,
            staging_root,
            state: Mutex::new(SupervisorState {
                profiles,
                local_rules,
                effective_configuration,
                runtime_generation,
                refreshes,
                probes: ProbeScheduler::new(),
                next_probe_generation: 0,
                telemetry: None,
                telemetry_generation: None,
                stream_health: disconnected_stream_health(),
                cached_proxy_view: None,
                observed_core_generation: None,
                probe_core_generation: None,
                selection_restore_pending: false,
                selection_restore_attempts_remaining: 0,
                health_reasons: BTreeSet::new(),
                wrapper_diagnostics: WrapperDiagnosticRing::new(WRAPPER_DIAGNOSTIC_CAPACITY)
                    .map_err(|_| internal_error())?,
            }),
            activation: Mutex::new(ActivationQueue::default()),
            apply_in_progress: AtomicBool::new(false),
            last_runtime_apply: Mutex::new(startup_runtime_apply),
            last_status: Mutex::new(initial_status),
        };
        {
            let mut state = supervisor.state.lock().map_err(|_| internal_error())?;
            for reason in startup_health_reasons {
                supervisor.set_health_reason(&mut state, reason, true);
            }
        }
        supervisor.reconcile_runtime_state()?;
        Ok(supervisor)
    }

    pub fn refresh_profile(
        &self,
        profile_id: ProfileId,
    ) -> Result<ProfileRefreshDisposition, ApplicationError> {
        let (context, url) = {
            let state = self.state.lock().map_err(|_| internal_error())?;
            let context = match state.profiles.refresh_context(profile_id) {
                Ok(context) => context,
                Err(_) => return Ok(ProfileRefreshDisposition::Discarded),
            };
            let url = state
                .profiles
                .get(profile_id)
                .ok_or_else(internal_error)?
                .subscription_url
                .clone();
            (context, url)
        };

        let fetched = match self.source.fetch(&url) {
            Ok(fetched) => fetched,
            Err(error) => {
                self.record_refresh_failure(
                    profile_id,
                    context,
                    RefreshStage::Download,
                    &error.message,
                )?;
                return Err(ApplicationError::new(
                    ErrorCode::ExternalOperationFailed,
                    error.message,
                    error.retryable,
                ));
            }
        };
        let snapshot = match ProfileSnapshot::parse(
            &fetched.body,
            SnapshotLimits::new(crate::constants::PROFILE_RESPONSE_MAX_BYTES, YAML_MAX_DEPTH),
        ) {
            Ok(snapshot) => snapshot,
            Err(_) => {
                self.record_refresh_failure(
                    profile_id,
                    context,
                    RefreshStage::Parse,
                    "The refreshed Profile Snapshot is invalid",
                )?;
                return Err(ApplicationError::new(
                    ErrorCode::ExternalOperationFailed,
                    "The refreshed Profile Snapshot is invalid",
                    false,
                ));
            }
        };

        let mut state = self.state.lock().map_err(|_| internal_error())?;
        if refresh_is_stale(&state.profiles, profile_id, context) {
            return Ok(ProfileRefreshDisposition::Discarded);
        }
        let now = self.clock.now_unix_ms();
        let next_refresh = next_refresh_at(now);
        let currently_active = state.profiles.active_profile_id() == Some(profile_id);
        if currently_active {
            let mut profiles = state.profiles.clone();
            profiles
                .commit_refresh(profile_id, context, snapshot, now, next_refresh)
                .map_err(|_| internal_error())?;
            let active = profiles.get(profile_id).ok_or_else(internal_error)?;
            let rules = rule_strings(&state.local_rules)?;
            let configuration = match self.compile(active, &rules) {
                Ok(configuration) => configuration,
                Err(error) => {
                    drop(state);
                    self.record_refresh_failure(
                        profile_id,
                        context,
                        RefreshStage::Validate,
                        &error.message,
                    )?;
                    return Err(error);
                }
            };
            let generation = next_runtime_generation(state.runtime_generation)?;
            let _apply = self.begin_apply(generation, state.runtime_generation);
            let result = self.apply_candidate(
                &profiles,
                &state.local_rules,
                &configuration,
                generation,
                false,
            );
            let failure_stage = result
                .as_ref()
                .err()
                .map_or(RefreshStage::Apply, refresh_stage_for_transaction_failure);
            let success = match self.settle_transaction(&mut state, result) {
                Ok(success) => success,
                Err(error) => {
                    drop(state);
                    self.record_refresh_failure(
                        profile_id,
                        context,
                        failure_stage,
                        &error.message,
                    )?;
                    return Err(error);
                }
            };
            state.profiles = profiles;
            state.effective_configuration = Some(configuration);
            state.runtime_generation = Some(success.committed_generation);
            let revision = state
                .profiles
                .get(profile_id)
                .ok_or_else(internal_error)?
                .revision;
            state.refreshes.upsert(profile_id, revision, next_refresh);
            self.reset_runtime_state(&mut state);
            Ok(ProfileRefreshDisposition::ActiveApplied)
        } else {
            let rules = snapshot.rule_strings().to_vec();
            let profile = state.profiles.get(profile_id).ok_or_else(internal_error)?;
            let validation_profile = Profile::new(
                profile_id,
                profile.name.clone(),
                profile.subscription_url.clone(),
                snapshot.clone(),
                now,
                next_refresh,
            );
            let configuration = match self.compile(&validation_profile, &rules) {
                Ok(configuration) => configuration,
                Err(error) => {
                    drop(state);
                    self.record_refresh_failure(
                        profile_id,
                        context,
                        RefreshStage::Validate,
                        &error.message,
                    )?;
                    return Err(error);
                }
            };
            if let Err(error) = self.validate_configuration(&configuration, profile_id) {
                drop(state);
                self.record_refresh_failure(
                    profile_id,
                    context,
                    RefreshStage::Validate,
                    &error.message,
                )?;
                return Err(error);
            }
            let mut profiles = state.profiles.clone();
            let revision = profiles
                .commit_refresh(profile_id, context, snapshot, now, next_refresh)
                .map_err(|_| internal_error())?;
            self.persist_metadata(&profiles, &state.local_rules, &state)?;
            state.profiles = profiles;
            state.refreshes.upsert(profile_id, revision, next_refresh);
            Ok(ProfileRefreshDisposition::InactiveStored)
        }
    }

    pub fn take_due_refreshes(&self) -> Result<Vec<RefreshTask>, ApplicationError> {
        let mut state = self.state.lock().map_err(|_| internal_error())?;
        Ok(state.refreshes.take_due(self.clock.now_unix_ms()))
    }

    pub fn managed_core(&self) -> Result<Option<ManagedCoreHandle>, ApplicationError> {
        let mut state = self.state.lock().map_err(|_| internal_error())?;
        if state.profiles.active_profile_id().is_none() {
            return Ok(None);
        }
        let managed_core = self
            .core
            .runtime_status()
            .map_err(|_| core_error("The Managed Core is unavailable"))?
            .managed_core;
        if let Some(core) = &managed_core {
            ensure_telemetry(&mut state, core.instance_generation)?;
        }
        Ok(managed_core)
    }

    pub fn set_stream_state(
        &self,
        generation: CoreInstanceGeneration,
        stream: TelemetryStream,
        stream_state: StreamState,
    ) -> Result<bool, ApplicationError> {
        let mut state = self.state.lock().map_err(|_| internal_error())?;
        if state.telemetry_generation != Some(generation) {
            return Ok(false);
        }
        match stream {
            TelemetryStream::Traffic => state.stream_health.traffic = stream_state,
            TelemetryStream::Connections => state.stream_health.connections = stream_state,
            TelemetryStream::Logs => state.stream_health.logs = stream_state,
        }
        Ok(true)
    }

    pub fn execute_refresh_task(
        &self,
        task: RefreshTask,
    ) -> Result<ProfileRefreshDisposition, ApplicationError> {
        {
            let mut state = self.state.lock().map_err(|_| internal_error())?;
            let completion = RefreshCompletion {
                task,
                profile_revision: task.profile_revision,
                completed_at_unix_ms: self.clock.now_unix_ms(),
            };
            match state.refreshes.complete(completion) {
                RefreshCompletionStatus::Rescheduled { .. } => {}
                RefreshCompletionStatus::StaleRevision
                | RefreshCompletionStatus::ProfileRemoved
                | RefreshCompletionStatus::UnknownTask => {
                    return Ok(ProfileRefreshDisposition::Discarded);
                }
            }
        }
        self.refresh_profile(task.profile_id)
    }

    pub fn cancel_pending_mutations(&self) {
        self.source.cancel_pending();
        self.validator.cancel_pending();
        self.transactions.cancel_pending();
        self.core.cancel_pending();
    }

    pub fn cancel_pending_profile_downloads(&self) {
        self.source.cancel_pending();
    }

    pub fn take_due_probes(&self) -> Result<Vec<ScheduledProbe>, ApplicationError> {
        let mut state = self.state.lock().map_err(|_| internal_error())?;
        let tasks = state.probes.take_due(self.clock.now_unix_ms());
        Ok(tasks
            .into_iter()
            .map(|task| {
                let request = state
                    .cached_proxy_view
                    .as_ref()
                    .and_then(|view| view.nodes.get(&task.node_id))
                    .map(|node| DelayProbeRequest {
                        record_id: node.record_id.clone(),
                        target: DelayTarget::from_node(node),
                        test_url: PROBE_URL.to_owned(),
                        timeout_ms: PROBE_TIMEOUT.as_millis().try_into().unwrap_or(u64::MAX),
                    });
                ScheduledProbe { task, request }
            })
            .collect())
    }

    pub fn complete_probe(
        &self,
        completion: ProbeCompletion,
    ) -> Result<ProbeCompletionStatus, ApplicationError> {
        let mut state = self.state.lock().map_err(|_| internal_error())?;
        Ok(state.probes.complete(completion))
    }

    pub fn publish_traffic(
        &self,
        generation: crate::domain::CoreInstanceGeneration,
        sample: TrafficSample,
    ) -> Result<bool, ApplicationError> {
        let mut state = self.state.lock().map_err(|_| internal_error())?;
        Ok(state
            .telemetry
            .as_mut()
            .is_some_and(|telemetry| telemetry.publish_traffic(generation, sample)))
    }

    pub fn publish_connections(
        &self,
        generation: crate::domain::CoreInstanceGeneration,
        summary: ConnectionSummary,
    ) -> Result<bool, ApplicationError> {
        let mut state = self.state.lock().map_err(|_| internal_error())?;
        Ok(state.telemetry.as_mut().is_some_and(|telemetry| {
            telemetry.publish_connections(
                generation,
                summary.active_connections,
                summary.upload_total_bytes,
                summary.download_total_bytes,
                summary.memory_bytes,
                summary.connections,
            )
        }))
    }

    pub fn publish_core_log(
        &self,
        generation: CoreInstanceGeneration,
        timestamp_unix_ms: u64,
        level: LogLevel,
        source: LogSource,
        message: impl Into<String>,
    ) -> Result<bool, ApplicationError> {
        let mut state = self.state.lock().map_err(|_| internal_error())?;
        match state.telemetry.as_mut() {
            Some(telemetry) => telemetry
                .publish_log(generation, timestamp_unix_ms, level, source, message)
                .map_err(|_| internal_error()),
            None => Ok(false),
        }
    }

    pub fn record_core_log_drop(
        &self,
        generation: CoreInstanceGeneration,
        count: u64,
    ) -> Result<bool, ApplicationError> {
        let mut state = self.state.lock().map_err(|_| internal_error())?;
        match state.telemetry.as_mut() {
            Some(telemetry) => telemetry
                .record_log_drop(generation, count)
                .map_err(|_| internal_error()),
            None => Ok(false),
        }
    }

    pub fn core_log_tail(&self, after_sequence: Option<u64>) -> Result<LogTail, ApplicationError> {
        let state = self.state.lock().map_err(|_| internal_error())?;
        Ok(state
            .telemetry
            .as_ref()
            .map_or_else(empty_log_tail, |telemetry| {
                telemetry.logs().tail_after(after_sequence)
            }))
    }

    pub fn wrapper_diagnostic_tail(
        &self,
        after_sequence: Option<u64>,
    ) -> Result<WrapperDiagnosticTail, ApplicationError> {
        let state = self.state.lock().map_err(|_| internal_error())?;
        state
            .wrapper_diagnostics
            .tail_after(after_sequence, WRAPPER_DIAGNOSTIC_CAPACITY)
            .map_err(|_| internal_error())
    }

    pub fn retry_selection_restore(&self) -> Result<bool, ApplicationError> {
        let mut state = self.state.lock().map_err(|_| internal_error())?;
        if !state.selection_restore_pending
            && state
                .health_reasons
                .contains(&SupervisorHealthReason::SelectionRestoration)
        {
            self.begin_selection_restore(&mut state);
        }
        self.reconcile_runtime_state_locked(&mut state);
        Ok(!state.selection_restore_pending
            && !state
                .health_reasons
                .contains(&SupervisorHealthReason::SelectionRestoration))
    }

    pub fn reconcile_runtime_state(&self) -> Result<(), ApplicationError> {
        let mut state = self.state.lock().map_err(|_| internal_error())?;
        self.reconcile_runtime_state_locked(&mut state);
        Ok(())
    }

    fn status(&self) -> Result<StatusSnapshot, ApplicationError> {
        let mut state = match self.state.try_lock() {
            Ok(state) => state,
            Err(TryLockError::WouldBlock) => {
                let mut status = self
                    .last_status
                    .lock()
                    .map_err(|_| internal_error())?
                    .clone();
                status.supervisor.uptime_seconds = self
                    .clock
                    .now_unix_ms()
                    .saturating_sub(self.started_at_unix_ms)
                    / 1_000;
                status.runtime_apply = self.current_runtime_apply();
                status.apply_state = status.runtime_apply.phase.compatibility_state();
                return Ok(status);
            }
            Err(TryLockError::Poisoned(_)) => return Err(internal_error()),
        };
        let active_profile_id = state.profiles.active_profile_id();
        let core_health = if active_profile_id.is_none() {
            CoreHealthProjection::unconfigured()
        } else {
            match self.core.runtime_status() {
                Ok(status) => CoreHealthProjection::from_runtime(status),
                Err(_) => CoreHealthProjection::unavailable(),
            }
        };
        let managed_core = core_health.managed_core.clone();
        if let Some(core) = &managed_core {
            ensure_telemetry(&mut state, core.instance_generation)?;
        }
        let uptime_seconds = self
            .clock
            .now_unix_ms()
            .saturating_sub(self.started_at_unix_ms)
            / 1_000;
        if let Some(core) = &managed_core
            && let Ok(order) = self.effective_group_order_with_health(&mut state)
            && let Ok(view) = self.core.proxy_view(core, &order)
        {
            state.cached_proxy_view = Some(view);
        }
        let active_profile =
            active_profile_id
                .and_then(|id| state.profiles.get(id))
                .map(|profile| ActiveProfileSummary {
                    id: profile.id,
                    name: profile.name.clone(),
                });
        let (primary_proxy_group, selected_node, latency) =
            status_proxy_fields(&state, self.clock.now_unix_ms());
        let traffic = state
            .telemetry
            .as_ref()
            .and_then(TelemetryStore::latest_traffic)
            .cloned()
            .unwrap_or_else(unavailable_traffic);
        let connection_count = state
            .telemetry
            .as_ref()
            .and_then(TelemetryStore::connection_count)
            .unwrap_or_default();
        let upload_total_bytes = state
            .telemetry
            .as_ref()
            .and_then(TelemetryStore::upload_total_bytes)
            .unwrap_or_default();
        let download_total_bytes = state
            .telemetry
            .as_ref()
            .and_then(TelemetryStore::download_total_bytes)
            .unwrap_or_default();
        let memory_bytes = state
            .telemetry
            .as_ref()
            .and_then(TelemetryStore::memory_bytes);
        let connections = state
            .telemetry
            .as_ref()
            .map_or_else(Vec::new, |telemetry| telemetry.connections().to_vec());
        let probe_queue = probe_queue_status(state.probes.metrics(self.clock.now_unix_ms()));
        let runtime_apply = self.current_runtime_apply();
        let status = StatusSnapshot {
            supervisor: SupervisorStatus {
                lifecycle: if !state.health_reasons.is_empty() || core_health.degraded {
                    SupervisorLifecycle::Degraded
                } else {
                    SupervisorLifecycle::Ready
                },
                started_at_unix_ms: self.started_at_unix_ms,
                uptime_seconds,
                health_reasons: state.health_reasons.iter().copied().collect(),
            },
            core: core_health.core,
            tun: core_health.tun,
            active_profile,
            primary_proxy_group,
            selected_node,
            latency,
            traffic,
            connection_count,
            upload_total_bytes,
            download_total_bytes,
            memory_bytes,
            connections,
            runtime_generation: state.runtime_generation,
            apply_state: runtime_apply.phase.compatibility_state(),
            runtime_apply,
            selection_restore_pending: state.selection_restore_pending,
            probe_queue,
            stream_health: state.stream_health.clone(),
        };
        *self.last_status.lock().map_err(|_| internal_error())? = status.clone();
        Ok(status)
    }

    fn begin_apply(
        &self,
        candidate_generation: RuntimeGeneration,
        committed_generation: Option<RuntimeGeneration>,
    ) -> ApplyActivity<'_> {
        *self
            .last_runtime_apply
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = RuntimeApplySnapshot {
            candidate_generation: Some(candidate_generation),
            committed_generation,
            phase: RuntimeApplyPhase::Applying,
            recovery: RuntimeRecoverySnapshot::default(),
        };
        self.apply_in_progress.store(true, Ordering::Release);
        ApplyActivity {
            in_progress: &self.apply_in_progress,
        }
    }

    fn current_runtime_apply(&self) -> RuntimeApplySnapshot {
        let mut status = self
            .last_runtime_apply
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        if self.apply_in_progress.load(Ordering::Acquire) {
            status.phase = RuntimeApplyPhase::Applying;
        }
        status
    }

    fn settle_transaction(
        &self,
        state: &mut SupervisorState,
        result: Result<ConfigTransactionSuccess, SupervisorTransactionFailure>,
    ) -> Result<ConfigTransactionSuccess, ApplicationError> {
        match result {
            Ok(success) => {
                self.set_health_reason(
                    state,
                    SupervisorHealthReason::RuntimeRecovery,
                    recovery_requires_degraded(success.recovery),
                );
                *self
                    .last_runtime_apply
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) =
                    successful_runtime_apply_snapshot(success);
                Ok(success)
            }
            Err(error) => {
                self.set_health_reason(
                    state,
                    SupervisorHealthReason::RuntimeRecovery,
                    recovery_requires_degraded(error.recovery),
                );
                *self
                    .last_runtime_apply
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) =
                    failed_runtime_apply_snapshot(error);
                Err(map_transaction_error(error))
            }
        }
    }

    fn set_health_reason(
        &self,
        state: &mut SupervisorState,
        reason: SupervisorHealthReason,
        active: bool,
    ) {
        let changed = if active {
            state.health_reasons.insert(reason)
        } else {
            state.health_reasons.remove(&reason)
        };
        if !changed {
            return;
        }
        let diagnostic_state = if active {
            WrapperDiagnosticState::Raised
        } else {
            WrapperDiagnosticState::Cleared
        };
        let context = wrapper_diagnostic_context(state, reason);
        let _ = state.wrapper_diagnostics.record(
            self.clock.now_unix_ms(),
            reason.into(),
            diagnostic_state,
            context,
        );
    }

    fn profile_add(
        &self,
        subscription_url: SubscriptionUrl,
    ) -> Result<ApplicationOutput, ApplicationError> {
        {
            let state = self.state.lock().map_err(|_| internal_error())?;
            if state.profiles.len() >= PROFILE_COUNT_MAX {
                return Err(ApplicationError::new(
                    ErrorCode::ExternalOperationFailed,
                    "The Profile limit has been reached",
                    false,
                ));
            }
        }
        let fetched = self.source.fetch(&subscription_url).map_err(|error| {
            ApplicationError::new(
                ErrorCode::ExternalOperationFailed,
                error.message,
                error.retryable,
            )
        })?;
        let snapshot = ProfileSnapshot::parse(
            &fetched.body,
            SnapshotLimits::new(crate::constants::PROFILE_RESPONSE_MAX_BYTES, YAML_MAX_DEPTH),
        )
        .map_err(|_| {
            ApplicationError::new(
                ErrorCode::ExternalOperationFailed,
                "The Profile Snapshot is invalid",
                false,
            )
        })?;
        let profile_id = ProfileId::new();
        let name = derive_profile_name(
            fetched.metadata_name.as_deref(),
            &subscription_url,
            profile_id,
        )
        .value;
        let now = self.clock.now_unix_ms();
        let profile = Profile::new(
            profile_id,
            name,
            subscription_url,
            snapshot,
            now,
            next_refresh_at(now),
        );
        let mut state = self.state.lock().map_err(|_| internal_error())?;
        let first_profile = state.profiles.is_empty();
        let mut profiles = state.profiles.clone();
        profiles.insert(profile).map_err(|_| internal_error())?;

        let runtime_apply = if first_profile {
            profiles
                .activate(&profile_id.to_string())
                .map_err(|_| internal_error())?;
            let profile = profiles.get(profile_id).ok_or_else(internal_error)?;
            let rules = profile
                .snapshot
                .rule_strings()
                .iter()
                .map(|rule| RuleString::new(rule.clone(), RULE_STRING_MAX_BYTES))
                .collect::<Result<Vec<_>, RuleStringError>>()
                .map_err(|_| invalid_rule_error())?;
            let local_rules = LocalRuleSet::initialized(rules);
            let strings = rule_strings(&local_rules)?;
            let configuration = self.compile(profile, &strings)?;
            let generation = RuntimeGeneration(1);
            let _apply = self.begin_apply(generation, state.runtime_generation);
            let result =
                self.apply_candidate(&profiles, &local_rules, &configuration, generation, false);
            let success = self.settle_transaction(&mut state, result)?;
            state.profiles = profiles;
            state.local_rules = local_rules;
            state.effective_configuration = Some(configuration);
            state.runtime_generation = Some(success.committed_generation);
            let (revision, next_refresh_at_unix_ms) = state
                .profiles
                .get(profile_id)
                .map(|profile| (profile.revision, profile.next_refresh_at_unix_ms))
                .ok_or_else(internal_error)?;
            state
                .refreshes
                .upsert(profile_id, revision, next_refresh_at_unix_ms);
            self.reset_runtime_state(&mut state);
            Some(runtime_apply_success(success))
        } else {
            let added = profiles.get(profile_id).ok_or_else(internal_error)?;
            let configuration = self.compile(added, added.snapshot.rule_strings())?;
            self.validate_configuration(&configuration, profile_id)?;
            self.persist_metadata(&profiles, &state.local_rules, &state)?;
            state.profiles = profiles;
            let (revision, next_refresh_at_unix_ms) = state
                .profiles
                .get(profile_id)
                .map(|profile| (profile.revision, profile.next_refresh_at_unix_ms))
                .ok_or_else(internal_error)?;
            state
                .refreshes
                .upsert(profile_id, revision, next_refresh_at_unix_ms);
            None
        };
        let profile = state.profiles.get(profile_id).ok_or_else(internal_error)?;
        Ok(ApplicationOutput::ProfileMutation(ProfileMutationOutcome {
            action: ProfileMutationAction::Added,
            profile: profile_summary(profile, state.profiles.active_profile_id()),
            runtime_apply,
        }))
    }

    fn profile_list(&self) -> Result<ApplicationOutput, ApplicationError> {
        let state = self.state.lock().map_err(|_| internal_error())?;
        Ok(ApplicationOutput::Profiles(ProfileListOutcome {
            profiles: state
                .profiles
                .profiles()
                .map(|profile| profile_summary(profile, state.profiles.active_profile_id()))
                .collect(),
        }))
    }

    fn profile_list_page(&self, offset: usize) -> Result<ApplicationOutput, ApplicationError> {
        let state = self.state.lock().map_err(|_| internal_error())?;
        let total = state.profiles.len();
        Ok(ApplicationOutput::ProfilePage(ProfileListPageOutcome {
            snapshot_id: profile_list_snapshot_id(&state.profiles),
            total,
            offset,
            profiles: state
                .profiles
                .profiles()
                .skip(offset)
                .take(IPC_LIST_PAGE_SIZE)
                .map(|profile| profile_summary(profile, state.profiles.active_profile_id()))
                .collect(),
        }))
    }

    fn profile_use(&self, selector: &str) -> Result<ApplicationOutput, ApplicationError> {
        let completion = {
            let mut queue = self
                .activation
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if queue.running {
                Some(queue.enqueue(selector))
            } else {
                queue.running = true;
                None
            }
        };
        if let Some(completion) = completion {
            return completion.wait();
        }

        let initial_result = self.activate_profile_once(selector);
        loop {
            let next = {
                let mut queue = self
                    .activation
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                match queue.pending.take() {
                    Some(pending) => Some(pending),
                    None => {
                        queue.running = false;
                        None
                    }
                }
            };
            let Some(next) = next else {
                break;
            };
            let result = self.activate_profile_once(&next.selector);
            next.completion.complete(result);
        }
        initial_result
    }

    fn activate_profile_once(&self, selector: &str) -> Result<ApplicationOutput, ApplicationError> {
        let mut state = self.state.lock().map_err(|_| internal_error())?;
        let profile_id = resolve_profile(&state.profiles, selector)?;
        if state.profiles.active_profile_id() == Some(profile_id) {
            let profile = state.profiles.get(profile_id).ok_or_else(internal_error)?;
            return Ok(ApplicationOutput::ProfileMutation(ProfileMutationOutcome {
                action: ProfileMutationAction::Activated,
                profile: profile_summary(profile, Some(profile_id)),
                runtime_apply: Some(runtime_apply_not_required(state.runtime_generation)),
            }));
        }
        let mut profiles = state.profiles.clone();
        profiles
            .activate(&profile_id.to_string())
            .map_err(|error| map_profile_error(&state.profiles, error))?;
        let profile = profiles.get(profile_id).ok_or_else(internal_error)?;
        let rules = rule_strings(&state.local_rules)?;
        let configuration = self.compile(profile, &rules)?;
        let generation = next_runtime_generation(state.runtime_generation)?;
        let _apply = self.begin_apply(generation, state.runtime_generation);
        let result = self.apply_candidate(
            &profiles,
            &state.local_rules,
            &configuration,
            generation,
            false,
        );
        let success = self.settle_transaction(&mut state, result)?;
        state.profiles = profiles;
        state.effective_configuration = Some(configuration);
        state.runtime_generation = Some(success.committed_generation);
        state.cached_proxy_view = None;
        self.reset_runtime_state(&mut state);
        let profile = state.profiles.get(profile_id).ok_or_else(internal_error)?;
        Ok(ApplicationOutput::ProfileMutation(ProfileMutationOutcome {
            action: ProfileMutationAction::Activated,
            profile: profile_summary(profile, Some(profile_id)),
            runtime_apply: Some(runtime_apply_success(success)),
        }))
    }

    fn profile_remove(&self, selector: &str) -> Result<ApplicationOutput, ApplicationError> {
        let mut state = self.state.lock().map_err(|_| internal_error())?;
        let mut profiles = state.profiles.clone();
        let removed = profiles
            .remove(selector)
            .map_err(|error| map_profile_error(&state.profiles, error))?;
        self.persist_metadata(&profiles, &state.local_rules, &state)?;
        state.profiles = profiles;
        state.refreshes.remove(removed.id);
        Ok(ApplicationOutput::ProfileMutation(ProfileMutationOutcome {
            action: ProfileMutationAction::Removed,
            profile: profile_summary(&removed, state.profiles.active_profile_id()),
            runtime_apply: None,
        }))
    }

    fn proxy_list(&self, group_selector: &str) -> Result<ApplicationOutput, ApplicationError> {
        let mut state = self.state.lock().map_err(|_| internal_error())?;
        let (core, view) = self.load_proxy_view(&mut state)?;
        ensure_telemetry(&mut state, core.instance_generation)?;
        let group = resolve_proxy_group(&view, group_selector)?;
        let observations = probe_observations(&state, &view, self.clock.now_unix_ms());
        let rows = view
            .node_rows(&group.name, &observations)
            .map_err(map_selection_error)?
            .into_iter()
            .map(proxy_row)
            .collect();
        let groups = view
            .groups
            .iter()
            .map(|group| proxy_group_summary(&view, group))
            .collect();
        Ok(ApplicationOutput::Proxies(ProxyListOutcome {
            group: proxy_group_summary(&view, group),
            groups,
            nodes: rows,
        }))
    }

    fn proxy_list_page(
        &self,
        group_selector: &str,
        groups_offset: usize,
        nodes_offset: usize,
    ) -> Result<ApplicationOutput, ApplicationError> {
        let mut state = self.state.lock().map_err(|_| internal_error())?;
        let (core, view) = self.load_proxy_view(&mut state)?;
        ensure_telemetry(&mut state, core.instance_generation)?;
        let group = resolve_proxy_group(&view, group_selector)?;
        let observations = probe_observations_page(
            &state,
            group,
            nodes_offset,
            IPC_LIST_PAGE_SIZE,
            self.clock.now_unix_ms(),
        );
        let (nodes_total, rows) = view
            .node_rows_page(&group.name, &observations, nodes_offset, IPC_LIST_PAGE_SIZE)
            .map_err(map_selection_error)?;
        let groups_total = view.groups.len();
        Ok(ApplicationOutput::ProxyPage(ProxyListPageOutcome {
            snapshot_id: proxy_list_snapshot_id(&view, core.instance_generation),
            group: proxy_group_summary(&view, group),
            groups_total,
            groups_offset,
            groups: view
                .groups
                .iter()
                .skip(groups_offset)
                .take(IPC_LIST_PAGE_SIZE)
                .map(|group| proxy_group_summary(&view, group))
                .collect(),
            nodes_total,
            nodes_offset,
            nodes: rows.into_iter().map(proxy_row).collect(),
        }))
    }

    fn proxy_select(
        &self,
        group_selector: &str,
        node_selector: &str,
    ) -> Result<ApplicationOutput, ApplicationError> {
        let mut state = self.state.lock().map_err(|_| internal_error())?;
        let (core, view) = self.load_proxy_view(&mut state)?;
        let group = resolve_proxy_group(&view, group_selector)?;
        let group_id = group.id.clone();
        let group_name = group.name.clone();
        let selection = selection_by_selector(&view, &group_name, node_selector)
            .map_err(map_selection_error)?;
        let previous = group
            .selected_name
            .as_deref()
            .and_then(|name| selection_by_selector(&view, &group_name, name).ok());
        self.core
            .select_node(&core, &selection)
            .map_err(|_| core_error("Mihomo rejected the Node selection"))?;

        let mut profiles = state.profiles.clone();
        let active_id = profiles.active_profile_id().ok_or_else(no_active_profile)?;
        profiles
            .get_mut(active_id)
            .ok_or_else(internal_error)?
            .selections
            .insert(group_name.clone(), selection.record_id.clone());
        if self
            .persist_metadata(&profiles, &state.local_rules, &state)
            .is_err()
        {
            let compensated = previous
                .as_ref()
                .is_some_and(|previous| self.core.select_node(&core, previous).is_ok());
            if !compensated {
                self.set_health_reason(
                    &mut state,
                    SupervisorHealthReason::SelectionCompensation,
                    true,
                );
            }
            return Err(ApplicationError::new(
                ErrorCode::ExternalOperationFailed,
                if compensated {
                    "The Node selection was restored after persistence failed"
                } else {
                    "The Node selection could not be restored after persistence failed"
                },
                false,
            ));
        }
        state.profiles = profiles;
        state.cached_proxy_view = None;
        state.selection_restore_pending = false;
        state.selection_restore_attempts_remaining = 0;
        self.set_health_reason(
            &mut state,
            SupervisorHealthReason::SelectionCompensation,
            false,
        );
        self.set_health_reason(
            &mut state,
            SupervisorHealthReason::SelectionRestoration,
            false,
        );
        Ok(ApplicationOutput::ProxySelection(ProxySelectionOutcome {
            group_id,
            group: group_name,
            previous_node: previous.map(|previous| SelectorIdentity {
                id: previous.record_id.as_str().to_owned(),
                name: previous.node_name,
            }),
            selected_node: SelectorIdentity {
                id: selection.record_id.as_str().to_owned(),
                name: selection.node_name,
            },
            persisted: true,
            recovery: ApplicationRecoveryOutcome {
                status: RecoveryStatus::NotRequired,
                restored_generation: None,
                message: None,
            },
        }))
    }

    fn latency_list(&self) -> Result<ApplicationOutput, ApplicationError> {
        let mut state = self.state.lock().map_err(|_| internal_error())?;
        let (_, view) = self.load_proxy_view(&mut state)?;
        let generation = state.probes.generation().ok_or_else(|| {
            ApplicationError::new(
                ErrorCode::CoreUnavailable,
                "The Active Profile Probe Generation is unavailable",
                true,
            )
        })?;
        let now = self.clock.now_unix_ms();
        let samples = view
            .nodes
            .values()
            .filter(|node| probe_eligible_node(node))
            .map(|node| {
                latency_summary(&state, node.record_id.clone(), &node.name, generation, now)
            })
            .collect();
        Ok(ApplicationOutput::Latencies(LatencyListOutcome { samples }))
    }

    fn latency_show(&self, selector: &str) -> Result<ApplicationOutput, ApplicationError> {
        let mut state = self.state.lock().map_err(|_| internal_error())?;
        let (_, view) = self.load_proxy_view(&mut state)?;
        let node = resolve_latency_node(&view, selector)?;
        let generation = state.probes.generation().ok_or_else(|| {
            ApplicationError::new(
                ErrorCode::CoreUnavailable,
                "The Active Profile Probe Generation is unavailable",
                true,
            )
        })?;
        Ok(ApplicationOutput::Latency(LatencyShowOutcome {
            sample: latency_summary(
                &state,
                node.record_id.clone(),
                &node.name,
                generation,
                self.clock.now_unix_ms(),
            ),
        }))
    }

    fn rule_list(&self) -> Result<ApplicationOutput, ApplicationError> {
        let state = self.state.lock().map_err(|_| internal_error())?;
        let list = state.local_rules.list().map_err(|_| invalid_rule_error())?;
        let initialized = list.initialized;
        let rules = list.entries.into_iter().map(rule_summary).collect();
        Ok(ApplicationOutput::Rules(RuleListOutcome {
            initialized,
            revision: initialized.then_some(state.local_rules.revision()),
            rules,
        }))
    }

    fn rule_list_page(&self, offset: usize) -> Result<ApplicationOutput, ApplicationError> {
        let state = self.state.lock().map_err(|_| internal_error())?;
        let page = state
            .local_rules
            .list_page(offset, IPC_LIST_PAGE_SIZE)
            .map_err(|_| invalid_rule_error())?;
        let initialized = page.initialized;
        Ok(ApplicationOutput::RulePage(RuleListPageOutcome {
            initialized,
            revision: initialized.then_some(state.local_rules.revision()),
            total: page.total,
            offset: page.offset,
            rules: page.entries.into_iter().map(rule_summary).collect(),
        }))
    }

    fn rule_mutation(&self, mutation: RuleMutation) -> Result<ApplicationOutput, ApplicationError> {
        let reservation = self
            .transactions
            .try_reserve_rule()
            .map_err(map_transaction_error)?;
        let mut state = match self.state.try_lock() {
            Ok(state) => state,
            Err(TryLockError::WouldBlock) => return Err(rule_busy_error()),
            Err(TryLockError::Poisoned(_)) => return Err(internal_error()),
        };
        let mut local_rules = clone_rule_set(&state.local_rules)?;
        let (action, changed_rule, previous_rule, resulting_position) = match mutation {
            RuleMutation::Add { rule, placement } => {
                let rule = checked_rule(rule)?;
                let position = local_rules
                    .add(rule.clone(), placement)
                    .map_err(map_rule_error)?;
                (
                    RuleMutationAction::Added,
                    rule.as_str().to_owned(),
                    None,
                    Some(position),
                )
            }
            RuleMutation::Replace { old_rule, new_rule } => {
                let old_rule = checked_rule(old_rule)?;
                let new_rule = checked_rule(new_rule)?;
                let position = local_rules
                    .replace(&old_rule, new_rule.clone())
                    .map_err(map_rule_error)?;
                (
                    RuleMutationAction::Replaced,
                    new_rule.as_str().to_owned(),
                    Some(old_rule.as_str().to_owned()),
                    Some(position),
                )
            }
            RuleMutation::Remove { rule } => {
                let rule = checked_rule(rule)?;
                let removed = local_rules.remove(&rule).map_err(map_rule_error)?;
                (
                    RuleMutationAction::Removed,
                    removed.as_str().to_owned(),
                    None,
                    None,
                )
            }
        };
        let active_id = state
            .profiles
            .active_profile_id()
            .ok_or_else(no_active_profile)?;
        let active = state.profiles.get(active_id).ok_or_else(internal_error)?;
        let rules = rule_strings(&local_rules)?;
        let configuration = self.compile(active, &rules)?;
        let generation = next_runtime_generation(state.runtime_generation)?;
        let _apply = self.begin_apply(generation, state.runtime_generation);
        let result = reservation.apply(SupervisorTransactionRequest {
            profiles: &state.profiles,
            local_rules: &local_rules,
            configuration: &configuration,
            generation,
            revisions: current_revisions(&state.profiles, &local_rules, Some(&configuration)),
        });
        let success = self.settle_transaction(&mut state, result)?;
        state.local_rules = local_rules;
        state.effective_configuration = Some(configuration);
        state.runtime_generation = Some(success.committed_generation);
        Ok(ApplicationOutput::RuleMutation(RuleMutationOutcome {
            action,
            changed_rule,
            previous_rule,
            resulting_position,
            runtime_apply: runtime_apply_success(success),
        }))
    }

    fn compile(
        &self,
        profile: &Profile,
        rules: &[String],
    ) -> Result<EffectiveConfiguration, ApplicationError> {
        let workspace = prepare_profile_workspace(&self.staging_root, profile.id)?;
        self.compiler
            .compile(&profile.snapshot, rules, &self.authoritative, &workspace)
            .map_err(map_config_error)
    }

    fn validate_configuration(
        &self,
        configuration: &EffectiveConfiguration,
        profile_id: ProfileId,
    ) -> Result<(), ApplicationError> {
        let workspace = prepare_profile_workspace(&self.staging_root, profile_id)?;
        self.validator
            .validate(configuration, &workspace)
            .map_err(|_| {
                ApplicationError::new(
                    ErrorCode::ExternalOperationFailed,
                    "Mihomo rejected the Profile Snapshot",
                    false,
                )
            })
    }

    fn apply_candidate(
        &self,
        profiles: &ProfileCatalog,
        local_rules: &LocalRuleSet,
        configuration: &EffectiveConfiguration,
        generation: RuntimeGeneration,
        fail_fast: bool,
    ) -> Result<ConfigTransactionSuccess, SupervisorTransactionFailure> {
        let revisions = current_revisions(profiles, local_rules, Some(configuration));
        self.transactions.apply(
            SupervisorTransactionRequest {
                profiles,
                local_rules,
                configuration,
                generation,
                revisions,
            },
            fail_fast,
        )
    }

    fn persist_metadata(
        &self,
        profiles: &ProfileCatalog,
        local_rules: &LocalRuleSet,
        state: &SupervisorState,
    ) -> Result<(), ApplicationError> {
        let configuration = state
            .effective_configuration
            .as_ref()
            .ok_or_else(internal_error)?;
        let generation = state.runtime_generation.ok_or_else(internal_error)?;
        self.transactions
            .persist_metadata(SupervisorTransactionRequest {
                profiles,
                local_rules,
                configuration,
                generation,
                revisions: current_revisions(profiles, local_rules, Some(configuration)),
            })
            .map_err(map_transaction_error)
    }

    fn load_proxy_view(
        &self,
        state: &mut SupervisorState,
    ) -> Result<(ManagedCoreHandle, ProxyView), ApplicationError> {
        if state.profiles.active_profile_id().is_none() {
            return Err(no_active_profile());
        }
        let core = self
            .core
            .runtime_status()
            .map_err(|_| core_error("The Managed Core is unavailable"))?
            .managed_core
            .ok_or_else(|| core_error("The Managed Core is unavailable"))?;
        let order = self.effective_group_order_with_health(state)?;
        let view = self
            .core
            .proxy_view(&core, &order)
            .map_err(|_| core_error("The Managed Core proxy view is unavailable"))?;
        state.cached_proxy_view = Some(view.clone());
        Ok((core, view))
    }

    fn reset_runtime_state(&self, state: &mut SupervisorState) {
        state.observed_core_generation = None;
        state.probe_core_generation = None;
        state.cached_proxy_view = None;
        self.begin_selection_restore(state);
        self.reconcile_runtime_state_locked(state);
    }

    fn reconcile_runtime_state_locked(&self, state: &mut SupervisorState) {
        if state.profiles.active_profile_id().is_none() {
            state.observed_core_generation = None;
            state.probe_core_generation = None;
            state.cached_proxy_view = None;
            state.probes.deactivate();
            state.selection_restore_pending = false;
            state.selection_restore_attempts_remaining = 0;
            self.set_health_reason(
                state,
                SupervisorHealthReason::ConfigurationProjection,
                false,
            );
            self.set_health_reason(state, SupervisorHealthReason::ProbeScheduler, false);
            self.set_health_reason(state, SupervisorHealthReason::SelectionRestoration, false);
            return;
        }
        let core = self
            .core
            .runtime_status()
            .ok()
            .and_then(|status| status.managed_core);
        let Some(core) = core else {
            if state.observed_core_generation.take().is_some() {
                state.probe_core_generation = None;
                state.cached_proxy_view = None;
                state.probes.deactivate();
                self.begin_selection_restore(state);
            }
            self.consume_selection_restore_attempt(state);
            return;
        };
        if state.observed_core_generation != Some(core.instance_generation) {
            state.observed_core_generation = Some(core.instance_generation);
            state.probe_core_generation = None;
            state.cached_proxy_view = None;
            state.probes.deactivate();
            self.begin_selection_restore(state);
        }
        if state.probe_core_generation == Some(core.instance_generation)
            && !state.selection_restore_pending
            && state.cached_proxy_view.is_some()
        {
            return;
        }
        let order = match self.effective_group_order_with_health(state) {
            Ok(order) => order,
            Err(_) => return,
        };
        let view = match self.core.proxy_view(&core, &order) {
            Ok(view) => view,
            Err(_) => {
                self.consume_selection_restore_attempt(state);
                return;
            }
        };
        state.cached_proxy_view = Some(view.clone());
        if state.probe_core_generation != Some(core.instance_generation) {
            self.seed_probes(state, core.instance_generation, &view);
        }
        if state.selection_restore_pending {
            self.restore_active_selections(state, &core, &view);
        }
    }

    fn seed_probes(
        &self,
        state: &mut SupervisorState,
        core_generation: CoreInstanceGeneration,
        view: &ProxyView,
    ) {
        state.next_probe_generation = state.next_probe_generation.saturating_add(1).max(1);
        let generation = ProbeGeneration(state.next_probe_generation);
        let nodes = view
            .nodes
            .values()
            .filter(|node| probe_eligible_node(node))
            .map(|node| node.record_id.clone())
            .collect::<Vec<_>>();
        if state
            .probes
            .reset(generation, nodes, self.clock.now_unix_ms())
            .is_err()
        {
            state.probes.deactivate();
            state.probe_core_generation = None;
            self.set_health_reason(state, SupervisorHealthReason::ProbeScheduler, true);
        } else {
            state.probe_core_generation = Some(core_generation);
            self.set_health_reason(state, SupervisorHealthReason::ProbeScheduler, false);
        }
    }

    fn begin_selection_restore(&self, state: &mut SupervisorState) {
        let Some(active_id) = state.profiles.active_profile_id() else {
            state.selection_restore_pending = false;
            state.selection_restore_attempts_remaining = 0;
            self.set_health_reason(state, SupervisorHealthReason::SelectionRestoration, false);
            return;
        };
        let has_selections = state
            .profiles
            .get(active_id)
            .is_some_and(|profile| !profile.selections.is_empty());
        state.selection_restore_pending = has_selections;
        state.selection_restore_attempts_remaining = if has_selections {
            SELECTION_RESTORE_ATTEMPT_LIMIT
        } else {
            0
        };
        if !has_selections {
            self.set_health_reason(state, SupervisorHealthReason::SelectionRestoration, false);
        }
    }

    fn restore_active_selections(
        &self,
        state: &mut SupervisorState,
        core: &ManagedCoreHandle,
        view: &ProxyView,
    ) {
        let Some(active_id) = state.profiles.active_profile_id() else {
            state.selection_restore_pending = false;
            state.selection_restore_attempts_remaining = 0;
            self.set_health_reason(state, SupervisorHealthReason::SelectionRestoration, false);
            return;
        };
        let selections = state
            .profiles
            .get(active_id)
            .map(|profile| profile.selections.clone())
            .unwrap_or_default();
        let mut pending = false;
        for (group, node_id) in selections {
            match selection_by_selector(view, &group, node_id.as_str()) {
                Ok(selection) => {
                    if self.core.select_node(core, &selection).is_err() {
                        pending = true;
                    }
                }
                Err(_) => pending = true,
            }
        }
        state.selection_restore_pending = pending;
        if pending {
            self.consume_selection_restore_attempt(state);
        } else {
            state.selection_restore_attempts_remaining = 0;
            self.set_health_reason(state, SupervisorHealthReason::SelectionRestoration, false);
        }
    }

    fn consume_selection_restore_attempt(&self, state: &mut SupervisorState) {
        if !state.selection_restore_pending {
            return;
        }
        state.selection_restore_attempts_remaining =
            state.selection_restore_attempts_remaining.saturating_sub(1);
        if state.selection_restore_attempts_remaining == 0 {
            state.selection_restore_pending = false;
            self.set_health_reason(state, SupervisorHealthReason::SelectionRestoration, true);
        }
    }

    fn effective_group_order_with_health(
        &self,
        state: &mut SupervisorState,
    ) -> Result<Vec<String>, ApplicationError> {
        match effective_group_order(&state.profiles) {
            Ok(order) => {
                self.set_health_reason(
                    state,
                    SupervisorHealthReason::ConfigurationProjection,
                    false,
                );
                Ok(order)
            }
            Err(error) => {
                self.set_health_reason(
                    state,
                    SupervisorHealthReason::ConfigurationProjection,
                    true,
                );
                Err(error)
            }
        }
    }

    fn record_refresh_failure(
        &self,
        profile_id: ProfileId,
        context: RefreshContext,
        stage: RefreshStage,
        message: &str,
    ) -> Result<(), ApplicationError> {
        let mut state = self.state.lock().map_err(|_| internal_error())?;
        if refresh_is_stale(&state.profiles, profile_id, context) {
            return Ok(());
        }
        let mut profiles = state.profiles.clone();
        let profile = profiles.get_mut(profile_id).ok_or_else(internal_error)?;
        profile.last_error = Some(RefreshFailure {
            stage,
            safe_message: bounded_message(message.to_owned()),
        });
        profile.next_refresh_at_unix_ms = next_refresh_at(self.clock.now_unix_ms());
        self.persist_metadata(&profiles, &state.local_rules, &state)?;
        state.profiles = profiles;
        let (revision, next_refresh_at_unix_ms) = state
            .profiles
            .get(profile_id)
            .map(|profile| (profile.revision, profile.next_refresh_at_unix_ms))
            .ok_or_else(internal_error)?;
        state
            .refreshes
            .upsert(profile_id, revision, next_refresh_at_unix_ms);
        Ok(())
    }
}

impl ApplicationClient for Supervisor {
    fn execute(
        &self,
        operation: ApplicationOperation,
    ) -> Result<ApplicationOutput, ApplicationError> {
        match operation {
            ApplicationOperation::GetStatus => self.status().map(ApplicationOutput::Status),
            ApplicationOperation::ProfileAdd { subscription_url } => {
                self.profile_add(subscription_url)
            }
            ApplicationOperation::ProfileList => self.profile_list(),
            ApplicationOperation::ProfileListPage { offset } => self.profile_list_page(offset),
            ApplicationOperation::ProfileUse { profile } => self.profile_use(&profile),
            ApplicationOperation::ProfileRemove { profile } => self.profile_remove(&profile),
            ApplicationOperation::ProxyList { group } => self.proxy_list(&group),
            ApplicationOperation::ProxyListPage {
                group,
                groups_offset,
                nodes_offset,
            } => self.proxy_list_page(&group, groups_offset, nodes_offset),
            ApplicationOperation::ProxySelect { group, node } => self.proxy_select(&group, &node),
            ApplicationOperation::LatencyList => self.latency_list(),
            ApplicationOperation::LatencyShow { node } => self.latency_show(&node),
            ApplicationOperation::RuleList => self.rule_list(),
            ApplicationOperation::RuleListPage { offset } => self.rule_list_page(offset),
            ApplicationOperation::RuleAdd { rule, placement } => {
                let placement = map_rule_placement(placement)?;
                self.rule_mutation(RuleMutation::Add { rule, placement })
            }
            ApplicationOperation::RuleReplace { old_rule, new_rule } => {
                self.rule_mutation(RuleMutation::Replace { old_rule, new_rule })
            }
            ApplicationOperation::RuleRemove { rule } => {
                self.rule_mutation(RuleMutation::Remove { rule })
            }
            ApplicationOperation::Start
            | ApplicationOperation::Stop
            | ApplicationOperation::Restart => Err(ApplicationError::new(
                ErrorCode::OperationUnavailable,
                "Lifecycle operations are handled by the foreground launcher",
                false,
            )),
        }
    }
}

enum RuleMutation {
    Add {
        rule: String,
        placement: RulePlacement,
    },
    Replace {
        old_rule: String,
        new_rule: String,
    },
    Remove {
        rule: String,
    },
}

// -----------------------------------------------------------------------------
// Projection and error helpers
// -----------------------------------------------------------------------------

fn current_revisions(
    profiles: &ProfileCatalog,
    local_rules: &LocalRuleSet,
    configuration: Option<&EffectiveConfiguration>,
) -> CandidateRevisions {
    let profile = profiles
        .active_profile_id()
        .and_then(|id| profiles.get(id))
        .map_or(ProfileRevision(0), |profile| profile.revision);
    CandidateRevisions {
        profile,
        active_profile: profiles.active_revision(),
        local_rule_set: local_rules.revision(),
        compiler_policy_sha256: configuration
            .map(EffectiveConfiguration::compiler_policy_sha256)
            .unwrap_or_default()
            .to_owned(),
        core_version: configuration
            .map(EffectiveConfiguration::core_version)
            .unwrap_or_default()
            .to_owned(),
    }
}

fn rule_strings(local_rules: &LocalRuleSet) -> Result<Vec<String>, ApplicationError> {
    let list = local_rules.list().map_err(|_| invalid_rule_error())?;
    if !list.initialized {
        return Err(ApplicationError::new(
            ErrorCode::RulesUninitialized,
            "The Local Rule Set is uninitialized",
            false,
        ));
    }
    Ok(list
        .entries
        .into_iter()
        .map(|entry| entry.rule.as_str().to_owned())
        .collect())
}

fn clone_rule_set(local_rules: &LocalRuleSet) -> Result<LocalRuleSet, ApplicationError> {
    let list = local_rules.list().map_err(|_| invalid_rule_error())?;
    if !list.initialized {
        return Ok(LocalRuleSet::uninitialized());
    }
    Ok(LocalRuleSet::initialized_at(
        list.entries
            .into_iter()
            .map(|entry| entry.rule.clone())
            .collect(),
        local_rules.revision(),
    ))
}

fn prepare_profile_workspace(
    staging_root: &Path,
    profile_id: ProfileId,
) -> Result<PathBuf, ApplicationError> {
    let workspace = staging_root.join(format!("profile-{profile_id}"));
    std::fs::create_dir_all(&workspace).map_err(|_| internal_error())?;
    std::fs::canonicalize(workspace).map_err(|_| internal_error())
}

fn next_runtime_generation(
    current: Option<RuntimeGeneration>,
) -> Result<RuntimeGeneration, ApplicationError> {
    current
        .map_or(Some(1), |generation| generation.0.checked_add(1))
        .map(RuntimeGeneration)
        .ok_or_else(internal_error)
}

fn next_refresh_at(now_unix_ms: u64) -> u64 {
    now_unix_ms.saturating_add(
        PROFILE_REFRESH_INTERVAL
            .as_millis()
            .try_into()
            .unwrap_or(u64::MAX),
    )
}
