use std::collections::BTreeMap;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Condvar;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, TryLockError};

use crate::application::{
    ApplicationClient, ApplicationError, ApplicationErrorDetails, ApplicationOperation,
    ApplicationOutput, Clock, LatencyFreshness as ApplicationLatencyFreshness, LatencyListOutcome,
    LatencyProbeStatus as ApplicationLatencyProbeStatus, LatencyShowOutcome, LatencySummary,
    PolicyTargetValidation, ProfileListOutcome, ProfileMutationAction, ProfileMutationOutcome,
    ProfileRefreshFailure, ProfileRefreshStage, ProfileRefreshState, ProfileSummary,
    ProxyAvailability, ProxyGroupSummary, ProxyListOutcome, ProxyMemberKind, ProxyNodeRow,
    ProxyNodeSource, ProxySelectionOutcome, RecoveryOutcome as ApplicationRecoveryOutcome,
    RecoveryStatus, RuleListOutcome, RuleMutationAction, RuleMutationOutcome,
    RulePlacement as ApplicationRulePlacement, RuleSummary, RuntimeApplyFailureDetails,
    RuntimeApplyFailureStage, RuntimeApplyOutcome, RuntimeApplyStatus, SelectorCandidate,
    SelectorIdentity, SelectorKind,
};
use crate::config::{
    AuthoritativeConfig, ConfigCompiler, ConfigError, CoreConfigValidator, EffectiveConfiguration,
};
use crate::constants::{
    CORE_LOG_LINE_MAX_BYTES, LOG_CAPACITY, PROBE_TIMEOUT, PROBE_URL, PROFILE_COUNT_MAX,
    PROFILE_REFRESH_INTERVAL, RULE_STRING_MAX_BYTES, TRAFFIC_SERIES_CAPACITY, YAML_MAX_DEPTH,
};
use crate::core::{
    Availability, CoreRuntime, CoreRuntimeStatus, DelayProbeRequest, DelayTarget,
    ManagedCoreHandle, MihomoAdapter, MihomoError, MihomoErrorKind, NodeRowMemberV1, NodeSelection,
    NodeSource, ProbeObservation, ProbeStatus as CoreProbeStatus, ProxyView, RuntimeBundle,
    SelectionError,
};
use crate::domain::{
    ActiveProfileSummary, ApplyState, CoreInstanceGeneration, CoreLifecycle, CoreStatus,
    NodeRecordId, ProbeGeneration, ProfileId, RuntimeGeneration, SampleState, SelectedNodeSummary,
    StatusSnapshot, StreamHealthSet, StreamState, SubscriptionUrl, SupervisorLifecycle,
    SupervisorStatus, TrafficSample, TunReason, TunStatus,
};
use crate::error::ErrorCode;
use crate::profile::{
    Profile, ProfileCatalog, ProfileRevision, ProfileSelectorError, ProfileSnapshot,
    RefreshContext, RefreshFailure, RefreshStage, SnapshotLimits, derive_profile_name,
};
use crate::profile_source::ProfileSource;
use crate::rule::{
    LocalRuleSet, RulePlacement, RuleSetError, RuleString, RuleStringError, parse_rule,
};
use crate::runtime_bundle::{RuntimeBundleStageErrorKind, RuntimeBundleStager};
use crate::scheduler::{
    ProbeCompletion, ProbeCompletionStatus, ProbeScheduler, ProbeStatus, ProbeTask,
    ProfileRefreshScheduler, RefreshCompletion, RefreshCompletionStatus, RefreshTask,
};
use crate::state::{AuthoritativeState, AuthoritativeStateStore, StateStoreErrorKind};
use crate::telemetry::{LogLevel, LogSource, LogTail, TelemetryStore};
use crate::transaction::{
    CandidateRevisionSource, CandidateRevisions, ConfigTransactionCandidate,
    ConfigTransactionCoordinator, ConfigTransactionError, ConfigTransactionErrorKind,
    ConfigTransactionSuccess, RecoveryOutcome as TransactionRecoveryOutcome,
};

// -----------------------------------------------------------------------------
// External application ports
// -----------------------------------------------------------------------------

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FetchedProfile {
    pub body: Vec<u8>,
    pub metadata_name: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProfileFetchError {
    pub message: String,
    pub retryable: bool,
}

impl ProfileFetchError {
    #[must_use]
    pub fn new(message: impl Into<String>, retryable: bool) -> Self {
        Self {
            message: bounded_message(message.into()),
            retryable,
        }
    }
}

pub trait ProfileFetchPort: Send + Sync {
    fn fetch(&self, url: &SubscriptionUrl) -> Result<FetchedProfile, ProfileFetchError>;
}

pub struct BlockingProfileFetchPort {
    runtime: tokio::runtime::Runtime,
    source: Arc<dyn ProfileSource>,
}

impl BlockingProfileFetchPort {
    pub fn new(source: Arc<dyn ProfileSource>) -> io::Result<Self> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(crate::constants::PROFILE_REFRESH_CONCURRENCY)
            .thread_name("hopash-profile-download")
            .enable_all()
            .build()?;
        Ok(Self { runtime, source })
    }
}

impl ProfileFetchPort for BlockingProfileFetchPort {
    fn fetch(&self, url: &SubscriptionUrl) -> Result<FetchedProfile, ProfileFetchError> {
        let download = self
            .runtime
            .block_on(self.source.download(url))
            .map_err(|error| ProfileFetchError::new(error.to_string(), error.retryable()))?;
        Ok(FetchedProfile {
            body: download.body().to_vec(),
            metadata_name: download.metadata_name().map(str::to_owned),
        })
    }
}

pub trait SupervisorCorePort: Send + Sync {
    fn runtime_status(&self) -> Result<CoreRuntimeStatus, MihomoError>;

    fn proxy_view(
        &self,
        core: &ManagedCoreHandle,
        effective_group_order: &[String],
    ) -> Result<ProxyView, MihomoError>;

    fn select_node(
        &self,
        core: &ManagedCoreHandle,
        selection: &NodeSelection,
    ) -> Result<(), MihomoError>;
}

pub struct DirectSupervisorCorePort {
    runtime: Arc<dyn CoreRuntime>,
    mihomo: Arc<dyn MihomoAdapter>,
    owner: crate::core::OwnerSessionProof,
}

impl DirectSupervisorCorePort {
    #[must_use]
    pub fn new(
        runtime: Arc<dyn CoreRuntime>,
        mihomo: Arc<dyn MihomoAdapter>,
        owner: crate::core::OwnerSessionProof,
    ) -> Self {
        Self {
            runtime,
            mihomo,
            owner,
        }
    }
}

impl SupervisorCorePort for DirectSupervisorCorePort {
    fn runtime_status(&self) -> Result<CoreRuntimeStatus, MihomoError> {
        self.runtime
            .status(&self.owner)
            .map_err(|error| MihomoError::new(MihomoErrorKind::Unavailable, error.to_string()))
    }

    fn proxy_view(
        &self,
        core: &ManagedCoreHandle,
        effective_group_order: &[String],
    ) -> Result<ProxyView, MihomoError> {
        self.mihomo
            .proxy_view(&core.endpoint, effective_group_order)
    }

    fn select_node(
        &self,
        core: &ManagedCoreHandle,
        selection: &NodeSelection,
    ) -> Result<(), MihomoError> {
        self.mihomo.select_node(&core.endpoint, selection)
    }
}

// -----------------------------------------------------------------------------
// Transaction adapter
// -----------------------------------------------------------------------------

pub struct SupervisorTransactionRequest<'a> {
    pub profiles: &'a ProfileCatalog,
    pub local_rules: &'a LocalRuleSet,
    pub configuration: &'a EffectiveConfiguration,
    pub generation: RuntimeGeneration,
    pub revisions: CandidateRevisions,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SupervisorTransactionFailureKind {
    Busy,
    State,
    Bundle,
    Coordinator(ConfigTransactionErrorKind),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SupervisorTransactionFailure {
    pub kind: SupervisorTransactionFailureKind,
    pub candidate_generation: Option<RuntimeGeneration>,
    pub committed_generation: Option<RuntimeGeneration>,
    pub recovery: TransactionRecoveryOutcome,
}

impl SupervisorTransactionFailure {
    #[must_use]
    pub const fn new(kind: SupervisorTransactionFailureKind) -> Self {
        Self {
            kind,
            candidate_generation: None,
            committed_generation: None,
            recovery: TransactionRecoveryOutcome::NotRequired,
        }
    }
}

impl From<ConfigTransactionError> for SupervisorTransactionFailure {
    fn from(error: ConfigTransactionError) -> Self {
        let kind = if error.kind == ConfigTransactionErrorKind::Busy {
            SupervisorTransactionFailureKind::Busy
        } else {
            SupervisorTransactionFailureKind::Coordinator(error.kind)
        };
        Self {
            kind,
            candidate_generation: error.candidate_generation,
            committed_generation: error.committed_generation,
            recovery: error.recovery,
        }
    }
}

pub trait SupervisorTransactionPort: Send + Sync {
    fn apply(
        &self,
        request: SupervisorTransactionRequest<'_>,
        fail_fast: bool,
    ) -> Result<ConfigTransactionSuccess, SupervisorTransactionFailure>;

    fn persist_metadata(
        &self,
        request: SupervisorTransactionRequest<'_>,
    ) -> Result<(), SupervisorTransactionFailure>;

    fn set_current_revisions(&self, revisions: CandidateRevisions);
}

pub trait RuntimeBundleStagePort: Send + Sync {
    fn stage(
        &self,
        generation: RuntimeGeneration,
        configuration: &EffectiveConfiguration,
    ) -> Result<RuntimeBundle, RuntimeBundleStageErrorKind>;
}

impl RuntimeBundleStagePort for RuntimeBundleStager {
    fn stage(
        &self,
        generation: RuntimeGeneration,
        configuration: &EffectiveConfiguration,
    ) -> Result<RuntimeBundle, RuntimeBundleStageErrorKind> {
        RuntimeBundleStager::stage(self, generation, configuration).map_err(|error| error.kind())
    }
}

pub struct SupervisorRevisionAuthority {
    current: Mutex<CandidateRevisions>,
}

impl SupervisorRevisionAuthority {
    #[must_use]
    pub fn new(initial: CandidateRevisions) -> Self {
        Self {
            current: Mutex::new(initial),
        }
    }

    pub fn set(&self, revisions: CandidateRevisions) {
        match self.current.lock() {
            Ok(mut current) => *current = revisions,
            Err(poisoned) => *poisoned.into_inner() = revisions,
        }
    }
}

impl CandidateRevisionSource for SupervisorRevisionAuthority {
    fn current(&self) -> CandidateRevisions {
        self.current.lock().map_or_else(
            |poisoned| poisoned.into_inner().clone(),
            |value| value.clone(),
        )
    }
}

pub struct CoordinatedSupervisorTransactions {
    state_store: Arc<AuthoritativeStateStore>,
    coordinator: Arc<ConfigTransactionCoordinator>,
    bundles: Arc<dyn RuntimeBundleStagePort>,
    revisions: Arc<SupervisorRevisionAuthority>,
    transaction_lock: Mutex<()>,
}

impl CoordinatedSupervisorTransactions {
    #[must_use]
    pub fn new(
        state_store: Arc<AuthoritativeStateStore>,
        coordinator: Arc<ConfigTransactionCoordinator>,
        bundles: Arc<dyn RuntimeBundleStagePort>,
        revisions: Arc<SupervisorRevisionAuthority>,
    ) -> Self {
        Self {
            state_store,
            coordinator,
            bundles,
            revisions,
            transaction_lock: Mutex::new(()),
        }
    }

    fn stage_state(
        &self,
        request: &SupervisorTransactionRequest<'_>,
    ) -> Result<crate::persistence::TransactionBundle, SupervisorTransactionFailure> {
        self.state_store
            .stage_candidate(AuthoritativeState {
                profiles: request.profiles,
                local_rules: request.local_rules,
                effective_configuration: request.configuration.yaml().as_bytes(),
                runtime_generation: request.generation,
            })
            .map_err(|error| {
                let _kind: StateStoreErrorKind = error.kind();
                SupervisorTransactionFailure::new(SupervisorTransactionFailureKind::State)
            })
    }
}

impl SupervisorTransactionPort for CoordinatedSupervisorTransactions {
    fn apply(
        &self,
        request: SupervisorTransactionRequest<'_>,
        fail_fast: bool,
    ) -> Result<ConfigTransactionSuccess, SupervisorTransactionFailure> {
        let _guard = if fail_fast {
            self.transaction_lock
                .try_lock()
                .map_err(|error| match error {
                    TryLockError::WouldBlock => {
                        SupervisorTransactionFailure::new(SupervisorTransactionFailureKind::Busy)
                    }
                    TryLockError::Poisoned(_) => {
                        SupervisorTransactionFailure::new(SupervisorTransactionFailureKind::State)
                    }
                })?
        } else {
            self.transaction_lock.lock().map_err(|_| {
                SupervisorTransactionFailure::new(SupervisorTransactionFailureKind::State)
            })?
        };
        let transaction = self.stage_state(&request)?;
        let runtime = self
            .bundles
            .stage(request.generation, request.configuration)
            .map_err(|_| {
                SupervisorTransactionFailure::new(SupervisorTransactionFailureKind::Bundle)
            })?;
        let previous = self.revisions.current();
        self.revisions.set(request.revisions.clone());
        let candidate = ConfigTransactionCandidate {
            transaction,
            runtime,
            configuration: request.configuration.clone(),
            revisions: request.revisions,
        };
        let result = if fail_fast {
            self.coordinator.try_execute_rule(&candidate)
        } else {
            self.coordinator.execute(&candidate)
        };
        match result {
            Ok(success) => Ok(success),
            Err(error) => {
                self.revisions.set(previous);
                Err(error.into())
            }
        }
    }

    fn persist_metadata(
        &self,
        request: SupervisorTransactionRequest<'_>,
    ) -> Result<(), SupervisorTransactionFailure> {
        let _guard = self.transaction_lock.lock().map_err(|_| {
            SupervisorTransactionFailure::new(SupervisorTransactionFailureKind::State)
        })?;
        let transaction = self.stage_state(&request)?;
        let persistence = self.state_store.persistence();
        let recovery = persistence.recover().map_err(|_| {
            SupervisorTransactionFailure::new(SupervisorTransactionFailureKind::State)
        })?;
        if recovery.prepared.is_some() {
            return Err(SupervisorTransactionFailure::new(
                SupervisorTransactionFailureKind::Coordinator(
                    ConfigTransactionErrorKind::RecoveryRequired,
                ),
            ));
        }
        let prepared = persistence.prepare(&transaction).map_err(|_| {
            SupervisorTransactionFailure::new(SupervisorTransactionFailureKind::State)
        })?;
        if persistence.commit_prepared(&prepared).is_err() {
            let recovered = persistence.recover().map_err(|_| {
                SupervisorTransactionFailure::new(SupervisorTransactionFailureKind::State)
            })?;
            let committed = recovered
                .committed
                .as_ref()
                .is_some_and(|manifest| manifest.current == prepared.candidate);
            if !committed {
                if persistence.clear_prepared(&prepared).is_ok() {
                    let _ = persistence.prune_unreachable();
                }
                return Err(SupervisorTransactionFailure::new(
                    SupervisorTransactionFailureKind::State,
                ));
            }
        }
        if persistence.clear_prepared(&prepared).is_ok() {
            let _ = persistence.prune_unreachable();
        }
        self.revisions.set(request.revisions);
        Ok(())
    }

    fn set_current_revisions(&self, revisions: CandidateRevisions) {
        self.revisions.set(revisions);
    }
}

// -----------------------------------------------------------------------------
// Supervisor application service
// -----------------------------------------------------------------------------

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
    selection_restore_pending: bool,
    degraded: bool,
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

        let (profiles, local_rules, effective_configuration, runtime_generation) =
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
                let configuration = dependencies
                    .compiler
                    .compile(
                        &active.snapshot,
                        &rules,
                        &dependencies.authoritative,
                        &workspace,
                    )
                    .map_err(map_config_error)?;
                if configuration.yaml().as_bytes() != hydrated.effective_configuration {
                    return Err(internal_error());
                }
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
        let started_at_unix_ms = dependencies.clock.now_unix_ms();
        let initial_status =
            initial_status_snapshot(started_at_unix_ms, &profiles, runtime_generation);
        Ok(Self {
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
                selection_restore_pending: false,
                degraded: false,
            }),
            activation: Mutex::new(ActivationQueue::default()),
            apply_in_progress: AtomicBool::new(false),
            last_status: Mutex::new(initial_status),
        })
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
            let _apply = self.begin_apply();
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
            if let Err(error) = settle_transaction(&mut state, result) {
                drop(state);
                self.record_refresh_failure(profile_id, context, failure_stage, &error.message)?;
                return Err(error);
            }
            state.profiles = profiles;
            state.effective_configuration = Some(configuration);
            state.runtime_generation = Some(generation);
            let revision = state
                .profiles
                .get(profile_id)
                .ok_or_else(internal_error)?
                .revision;
            state.refreshes.upsert(profile_id, revision, next_refresh);
            self.reset_probes(&mut state);
            self.restore_active_selections(&mut state);
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

    pub fn publish_connection_count(
        &self,
        generation: crate::domain::CoreInstanceGeneration,
        count: u64,
    ) -> Result<bool, ApplicationError> {
        let mut state = self.state.lock().map_err(|_| internal_error())?;
        Ok(state
            .telemetry
            .as_mut()
            .is_some_and(|telemetry| telemetry.publish_connections(generation, count)))
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

    pub fn core_log_tail(&self, after_sequence: Option<u64>) -> Result<LogTail, ApplicationError> {
        let state = self.state.lock().map_err(|_| internal_error())?;
        Ok(state
            .telemetry
            .as_ref()
            .map_or_else(empty_log_tail, |telemetry| {
                telemetry.logs().tail_after(after_sequence)
            }))
    }

    pub fn retry_selection_restore(&self) -> Result<bool, ApplicationError> {
        let mut state = self.state.lock().map_err(|_| internal_error())?;
        self.restore_active_selections(&mut state);
        Ok(!state.selection_restore_pending)
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
                status.apply_state = self.current_apply_state();
                return Ok(status);
            }
            Err(TryLockError::Poisoned(_)) => return Err(internal_error()),
        };
        let managed_core = if state.profiles.active_profile_id().is_none() {
            None
        } else {
            self.core
                .runtime_status()
                .ok()
                .and_then(|status| status.managed_core)
        };
        if let Some(core) = &managed_core {
            ensure_telemetry(&mut state, core.instance_generation)?;
        }
        let uptime_seconds = self
            .clock
            .now_unix_ms()
            .saturating_sub(self.started_at_unix_ms)
            / 1_000;
        let active_profile_id = state.profiles.active_profile_id();
        if let Some(core) = &managed_core {
            let order = effective_group_order(&state.profiles)?;
            if let Ok(view) = self.core.proxy_view(core, &order) {
                state.cached_proxy_view = Some(view);
            }
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
        let zero_profile = active_profile_id.is_none();
        let core_ready = managed_core.is_some();
        let status = StatusSnapshot {
            supervisor: SupervisorStatus {
                lifecycle: if state.degraded {
                    SupervisorLifecycle::Degraded
                } else {
                    SupervisorLifecycle::Ready
                },
                started_at_unix_ms: self.started_at_unix_ms,
                uptime_seconds,
            },
            core: CoreStatus {
                lifecycle: if zero_profile {
                    CoreLifecycle::Unconfigured
                } else if state.degraded {
                    CoreLifecycle::Degraded
                } else if core_ready {
                    CoreLifecycle::Ready
                } else {
                    CoreLifecycle::Stopped
                },
                pid: managed_core.as_ref().map(|core| core.pid),
                instance_generation: managed_core.as_ref().map(|core| core.instance_generation),
            },
            tun: TunStatus {
                requested: true,
                capable: core_ready,
                effective: core_ready,
                reason: if zero_profile {
                    Some(TunReason::NoActiveProfile)
                } else if core_ready {
                    None
                } else {
                    Some(TunReason::CoreUnavailable)
                },
            },
            active_profile,
            primary_proxy_group,
            selected_node,
            latency,
            traffic,
            connection_count,
            runtime_generation: state.runtime_generation,
            apply_state: self.current_apply_state(),
            stream_health: state.stream_health.clone(),
        };
        *self.last_status.lock().map_err(|_| internal_error())? = status.clone();
        Ok(status)
    }

    fn begin_apply(&self) -> ApplyActivity<'_> {
        self.apply_in_progress.store(true, Ordering::Release);
        ApplyActivity {
            in_progress: &self.apply_in_progress,
        }
    }

    fn current_apply_state(&self) -> ApplyState {
        if self.apply_in_progress.load(Ordering::Acquire) {
            ApplyState::Applying
        } else {
            ApplyState::Idle
        }
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
            let _apply = self.begin_apply();
            let result =
                self.apply_candidate(&profiles, &local_rules, &configuration, generation, false);
            let success = settle_transaction(&mut state, result)?;
            state.profiles = profiles;
            state.local_rules = local_rules;
            state.effective_configuration = Some(configuration);
            state.runtime_generation = Some(generation);
            let (revision, next_refresh_at_unix_ms) = state
                .profiles
                .get(profile_id)
                .map(|profile| (profile.revision, profile.next_refresh_at_unix_ms))
                .ok_or_else(internal_error)?;
            state
                .refreshes
                .upsert(profile_id, revision, next_refresh_at_unix_ms);
            self.reset_probes(&mut state);
            self.restore_active_selections(&mut state);
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
        let _apply = self.begin_apply();
        let result = self.apply_candidate(
            &profiles,
            &state.local_rules,
            &configuration,
            generation,
            false,
        );
        let success = settle_transaction(&mut state, result)?;
        state.profiles = profiles;
        state.effective_configuration = Some(configuration);
        state.runtime_generation = Some(generation);
        state.cached_proxy_view = None;
        self.reset_probes(&mut state);
        self.restore_active_selections(&mut state);
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

    fn proxy_list(&self, group_name: &str) -> Result<ApplicationOutput, ApplicationError> {
        let mut state = self.state.lock().map_err(|_| internal_error())?;
        let (core, view) = self.load_proxy_view(&mut state)?;
        ensure_telemetry(&mut state, core.instance_generation)?;
        let group = view
            .groups
            .iter()
            .find(|group| group.name == group_name)
            .cloned()
            .ok_or_else(|| selector_not_found(SelectorKind::ProxyGroup, "Proxy Group"))?;
        let observations = probe_observations(&state, &view, self.clock.now_unix_ms());
        let rows = view
            .node_rows(group_name, &observations)
            .map_err(map_selection_error)?
            .into_iter()
            .map(proxy_row)
            .collect();
        let selected_node = group
            .selected_name
            .as_deref()
            .and_then(|name| selection_by_selector(&view, group_name, name).ok())
            .map(|selection| SelectorIdentity {
                id: selection.record_id.as_str().to_owned(),
                name: selection.node_name,
            });
        Ok(ApplicationOutput::Proxies(ProxyListOutcome {
            group: ProxyGroupSummary {
                name: group.name,
                proxy_type: group.proxy_type,
                selectable: group.selectable,
                selected_node,
            },
            nodes: rows,
        }))
    }

    fn proxy_select(
        &self,
        group_name: &str,
        node_selector: &str,
    ) -> Result<ApplicationOutput, ApplicationError> {
        let mut state = self.state.lock().map_err(|_| internal_error())?;
        let (core, view) = self.load_proxy_view(&mut state)?;
        let selection =
            selection_by_selector(&view, group_name, node_selector).map_err(map_selection_error)?;
        let previous = view
            .groups
            .iter()
            .find(|group| group.name == group_name)
            .and_then(|group| group.selected_name.as_deref())
            .and_then(|name| selection_by_selector(&view, group_name, name).ok());
        self.core
            .select_node(&core, &selection)
            .map_err(|_| core_error("Mihomo rejected the Node selection"))?;

        let mut profiles = state.profiles.clone();
        let active_id = profiles.active_profile_id().ok_or_else(no_active_profile)?;
        profiles
            .get_mut(active_id)
            .ok_or_else(internal_error)?
            .selections
            .insert(group_name.to_owned(), selection.record_id.clone());
        if self
            .persist_metadata(&profiles, &state.local_rules, &state)
            .is_err()
        {
            let compensated = previous
                .as_ref()
                .is_some_and(|previous| self.core.select_node(&core, previous).is_ok());
            if !compensated {
                state.degraded = true;
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
        Ok(ApplicationOutput::ProxySelection(ProxySelectionOutcome {
            group: group_name.to_owned(),
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
        let rules = list
            .entries
            .into_iter()
            .map(|entry| RuleSummary {
                index: entry.index,
                rule_string: entry.rule.as_str().to_owned(),
                rule_type: entry.parsed.rule_type.as_str().to_owned(),
                payload: entry.parsed.payload.map(str::to_owned),
                policy_target: entry.parsed.policy_target.to_owned(),
                params: entry.parsed.params.into_iter().map(str::to_owned).collect(),
                policy_target_validation: PolicyTargetValidation::Valid,
            })
            .collect();
        Ok(ApplicationOutput::Rules(RuleListOutcome {
            initialized,
            revision: initialized.then_some(state.local_rules.revision()),
            rules,
        }))
    }

    fn rule_mutation(&self, mutation: RuleMutation) -> Result<ApplicationOutput, ApplicationError> {
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
        let _apply = self.begin_apply();
        let result = self.apply_candidate(
            &state.profiles,
            &local_rules,
            &configuration,
            generation,
            true,
        );
        let success = settle_transaction(&mut state, result)?;
        state.local_rules = local_rules;
        state.effective_configuration = Some(configuration);
        state.runtime_generation = Some(generation);
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
        let order = effective_group_order(&state.profiles)?;
        let view = self
            .core
            .proxy_view(&core, &order)
            .map_err(|_| core_error("The Managed Core proxy view is unavailable"))?;
        state.cached_proxy_view = Some(view.clone());
        Ok((core, view))
    }

    fn reset_probes(&self, state: &mut SupervisorState) {
        state.next_probe_generation = state.next_probe_generation.saturating_add(1).max(1);
        let generation = ProbeGeneration(state.next_probe_generation);
        let nodes = self
            .core
            .runtime_status()
            .ok()
            .and_then(|status| status.managed_core)
            .and_then(|core| {
                let order = effective_group_order(&state.profiles).ok()?;
                let view = self.core.proxy_view(&core, &order).ok()?;
                state.cached_proxy_view = Some(view.clone());
                Some(
                    view.nodes
                        .into_values()
                        .filter(|node| !node.core_internal)
                        .map(|node| node.record_id)
                        .collect::<Vec<_>>(),
                )
            })
            .unwrap_or_default();
        if state
            .probes
            .reset(generation, nodes, self.clock.now_unix_ms())
            .is_err()
        {
            state.probes.deactivate();
            state.degraded = true;
        }
    }

    fn restore_active_selections(&self, state: &mut SupervisorState) {
        let Some(active_id) = state.profiles.active_profile_id() else {
            state.selection_restore_pending = false;
            return;
        };
        let selections = state
            .profiles
            .get(active_id)
            .map(|profile| profile.selections.clone())
            .unwrap_or_default();
        if selections.is_empty() {
            state.selection_restore_pending = false;
            return;
        }
        let Some(core) = self
            .core
            .runtime_status()
            .ok()
            .and_then(|status| status.managed_core)
        else {
            state.selection_restore_pending = true;
            return;
        };
        let Some(view) = state.cached_proxy_view.clone() else {
            state.selection_restore_pending = true;
            return;
        };
        let mut pending = false;
        for (group, node_id) in selections {
            match selection_by_selector(&view, &group, node_id.as_str()) {
                Ok(selection) => {
                    if self.core.select_node(&core, &selection).is_err() {
                        pending = true;
                    }
                }
                Err(_) => pending = true,
            }
        }
        state.selection_restore_pending = pending;
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
            ApplicationOperation::ProfileUse { profile } => self.profile_use(&profile),
            ApplicationOperation::ProfileRemove { profile } => self.profile_remove(&profile),
            ApplicationOperation::ProxyList { group } => self.proxy_list(&group),
            ApplicationOperation::ProxySelect { group, node } => self.proxy_select(&group, &node),
            ApplicationOperation::LatencyList => self.latency_list(),
            ApplicationOperation::LatencyShow { node } => self.latency_show(&node),
            ApplicationOperation::RuleList => self.rule_list(),
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

fn profile_summary(profile: &Profile, active: Option<ProfileId>) -> ProfileSummary {
    ProfileSummary {
        id: profile.id,
        name: profile.name.clone(),
        subscription_url: profile.subscription_url.clone(),
        active: active == Some(profile.id),
        refresh_state: if profile.last_error.is_some() {
            ProfileRefreshState::Error
        } else {
            ProfileRefreshState::Fresh
        },
        last_success_at_unix_ms: profile.last_success_at_unix_ms,
        next_refresh_at_unix_ms: profile.next_refresh_at_unix_ms,
        last_error: profile
            .last_error
            .as_ref()
            .map(|failure| ProfileRefreshFailure {
                stage: match failure.stage {
                    RefreshStage::Download => ProfileRefreshStage::Download,
                    RefreshStage::Parse => ProfileRefreshStage::Parse,
                    RefreshStage::Validate => ProfileRefreshStage::Validate,
                    RefreshStage::Apply => ProfileRefreshStage::Apply,
                },
                message: failure.safe_message.clone(),
            }),
    }
}

fn effective_group_order(profiles: &ProfileCatalog) -> Result<Vec<String>, ApplicationError> {
    let active = profiles
        .active_profile_id()
        .and_then(|id| profiles.get(id))
        .ok_or_else(no_active_profile)?;
    let Some(serde_yaml_ng::Value::Sequence(groups)) =
        active.snapshot.document().get("proxy-groups")
    else {
        return Ok(Vec::new());
    };
    Ok(groups
        .iter()
        .filter_map(serde_yaml_ng::Value::as_mapping)
        .filter_map(|group| group.get("name"))
        .filter_map(serde_yaml_ng::Value::as_str)
        .map(str::to_owned)
        .collect())
}

fn selection_by_selector(
    view: &ProxyView,
    group_name: &str,
    selector: &str,
) -> Result<NodeSelection, SelectionError> {
    if let Ok(record_id) = NodeRecordId::parse(selector) {
        let group = view
            .groups
            .iter()
            .find(|group| group.name == group_name)
            .ok_or_else(|| SelectionError::GroupMissing(group_name.to_owned()))?;
        let node = group.members.iter().find_map(|member| match member {
            crate::core::ProxyMember::Node {
                name,
                record_id: candidate,
                availability,
            } if *candidate == record_id => Some((name, availability)),
            _ => None,
        });
        return match node {
            Some((name, Availability::Available)) => Ok(NodeSelection {
                group_name: group_name.to_owned(),
                node_name: name.clone(),
                record_id,
            }),
            Some((name, Availability::Unavailable)) => {
                Err(SelectionError::NodeUnavailable(name.clone()))
            }
            None => Err(SelectionError::NodeMissing(selector.to_owned())),
        };
    }
    view.resolve_exact_selection(group_name, selector)
}

fn probe_observations(
    state: &SupervisorState,
    view: &ProxyView,
    now_unix_ms: u64,
) -> BTreeMap<NodeRecordId, ProbeObservation> {
    view.nodes
        .keys()
        .filter_map(|node_id| {
            state
                .probes
                .node_snapshot(node_id, now_unix_ms)
                .map(|snapshot| {
                    (
                        node_id.clone(),
                        ProbeObservation {
                            sample: snapshot.sample,
                            status: match snapshot.status {
                                ProbeStatus::NotSampled => CoreProbeStatus::NotSampled,
                                ProbeStatus::Queued => CoreProbeStatus::Queued,
                                ProbeStatus::InFlight => CoreProbeStatus::InFlight,
                                ProbeStatus::Available => CoreProbeStatus::Succeeded,
                                ProbeStatus::TimedOut | ProbeStatus::Unavailable => {
                                    CoreProbeStatus::Failed
                                }
                            },
                        },
                    )
                })
        })
        .collect()
}

fn proxy_row(row: crate::core::NodeRowV1) -> ProxyNodeRow {
    let (id, member_kind, source, candidate_ids) = match row.member {
        NodeRowMemberV1::Group => (None, ProxyMemberKind::Group, None, Vec::new()),
        NodeRowMemberV1::Node { record_id, source } => {
            let source = match source {
                NodeSource::Core { .. } => ProxyNodeSource::Core,
                NodeSource::Provider { provider_name, .. } => {
                    ProxyNodeSource::Provider { provider_name }
                }
            };
            (
                Some(record_id),
                ProxyMemberKind::Node,
                Some(source),
                Vec::new(),
            )
        }
        NodeRowMemberV1::Unresolved {
            reason,
            candidate_ids,
        } => {
            let kind = match reason {
                crate::core::UnresolvedMemberReason::Missing => ProxyMemberKind::Missing,
                crate::core::UnresolvedMemberReason::Ambiguous => ProxyMemberKind::Ambiguous,
                crate::core::UnresolvedMemberReason::ProviderUnavailable => {
                    ProxyMemberKind::ProviderUnavailable
                }
            };
            (None, kind, None, candidate_ids)
        }
    };
    ProxyNodeRow {
        id,
        name: row.name,
        member_kind,
        source,
        candidate_ids,
        proxy_type: row.proxy_type,
        availability: match row.availability {
            Availability::Available => ProxyAvailability::Available,
            Availability::Unavailable => ProxyAvailability::Unavailable,
        },
        selected: row.selected,
        delay_ms: row.delay_ms,
        sampled_at_unix_ms: row.sampled_at_unix_ms,
        freshness: match row.freshness {
            crate::core::LatencyFreshness::NotSampled => ApplicationLatencyFreshness::NotSampled,
            crate::core::LatencyFreshness::Fresh => ApplicationLatencyFreshness::Fresh,
            crate::core::LatencyFreshness::Stale => ApplicationLatencyFreshness::Stale,
            crate::core::LatencyFreshness::Unavailable => ApplicationLatencyFreshness::Unavailable,
        },
        probe_status: match row.probe_status {
            CoreProbeStatus::NotSampled => ApplicationLatencyProbeStatus::NotSampled,
            CoreProbeStatus::Queued => ApplicationLatencyProbeStatus::Queued,
            CoreProbeStatus::InFlight => ApplicationLatencyProbeStatus::InFlight,
            CoreProbeStatus::Succeeded => ApplicationLatencyProbeStatus::Succeeded,
            CoreProbeStatus::Failed => ApplicationLatencyProbeStatus::Failed,
        },
    }
}

fn latency_summary(
    state: &SupervisorState,
    node_id: NodeRecordId,
    node_name: &str,
    generation: ProbeGeneration,
    now_unix_ms: u64,
) -> LatencySummary {
    let snapshot = state.probes.node_snapshot(&node_id, now_unix_ms);
    let sample = snapshot
        .as_ref()
        .and_then(|snapshot| snapshot.sample.as_ref());
    LatencySummary {
        node_id,
        node_name: node_name.to_owned(),
        delay_ms: sample.and_then(|sample| sample.delay_ms),
        sampled_at_unix_ms: sample.and_then(|sample| sample.sampled_at_unix_ms),
        freshness: match sample.map(|sample| sample.state) {
            None => ApplicationLatencyFreshness::NotSampled,
            Some(SampleState::Fresh) => ApplicationLatencyFreshness::Fresh,
            Some(SampleState::Stale) => ApplicationLatencyFreshness::Stale,
            Some(SampleState::Unavailable) => ApplicationLatencyFreshness::Unavailable,
        },
        probe_status: match snapshot.map(|snapshot| snapshot.status) {
            None | Some(ProbeStatus::NotSampled) => ApplicationLatencyProbeStatus::NotSampled,
            Some(ProbeStatus::Queued) => ApplicationLatencyProbeStatus::Queued,
            Some(ProbeStatus::InFlight) => ApplicationLatencyProbeStatus::InFlight,
            Some(ProbeStatus::Available) => ApplicationLatencyProbeStatus::Succeeded,
            Some(ProbeStatus::TimedOut | ProbeStatus::Unavailable) => {
                ApplicationLatencyProbeStatus::Failed
            }
        },
        probe_generation: generation,
    }
}

fn resolve_latency_node<'a>(
    view: &'a ProxyView,
    selector: &str,
) -> Result<&'a crate::core::ProxyNode, ApplicationError> {
    if let Ok(id) = NodeRecordId::parse(selector) {
        return view
            .nodes
            .get(&id)
            .ok_or_else(|| selector_not_found(SelectorKind::Node, "Node"));
    }
    let candidates = view
        .nodes
        .values()
        .filter(|node| node.name == selector)
        .collect::<Vec<_>>();
    match candidates.as_slice() {
        [] => Err(selector_not_found(SelectorKind::Node, "Node")),
        [node] => Ok(*node),
        _ => Err(ApplicationError::new(
            ErrorCode::NodeAmbiguous,
            "The Node selector is ambiguous",
            false,
        )
        .with_selector_candidates(
            SelectorKind::Node,
            candidates
                .into_iter()
                .map(|node| SelectorCandidate::new(node.record_id.as_str(), &node.name))
                .collect(),
        )),
    }
}

fn status_proxy_fields(
    state: &SupervisorState,
    now_unix_ms: u64,
) -> (
    Option<String>,
    Option<SelectedNodeSummary>,
    Option<crate::domain::LatencySample>,
) {
    let Some(view) = &state.cached_proxy_view else {
        return (None, None, None);
    };
    let Some(group) = view.primary_group() else {
        return (None, None, None);
    };
    let selection = group
        .selected_name
        .as_deref()
        .and_then(|name| selection_by_selector(view, &group.name, name).ok());
    let selected = selection.as_ref().map(|selection| SelectedNodeSummary {
        id: selection.record_id.clone(),
        name: selection.node_name.clone(),
    });
    let latency = selection.and_then(|selection| {
        state
            .probes
            .node_snapshot(&selection.record_id, now_unix_ms)
            .and_then(|snapshot| snapshot.sample)
    });
    (Some(group.name.clone()), selected, latency)
}

fn ensure_telemetry(
    state: &mut SupervisorState,
    generation: CoreInstanceGeneration,
) -> Result<(), ApplicationError> {
    let replaced = match (state.telemetry.as_mut(), state.telemetry_generation) {
        (Some(telemetry), Some(current)) if current != generation => {
            telemetry.replace_core(generation);
            state.telemetry_generation = Some(generation);
            true
        }
        (Some(_), Some(_)) => false,
        (Some(telemetry), None) => {
            telemetry.replace_core(generation);
            state.telemetry_generation = Some(generation);
            true
        }
        (None, _) => {
            state.telemetry = Some(
                TelemetryStore::new(
                    generation,
                    LOG_CAPACITY,
                    CORE_LOG_LINE_MAX_BYTES,
                    TRAFFIC_SERIES_CAPACITY,
                )
                .map_err(|_| internal_error())?,
            );
            state.telemetry_generation = Some(generation);
            true
        }
    };
    if replaced {
        state.stream_health = disconnected_stream_health();
    }
    Ok(())
}

fn disconnected_stream_health() -> StreamHealthSet {
    StreamHealthSet {
        traffic: StreamState::Disconnected,
        connections: StreamState::Disconnected,
        logs: StreamState::Disconnected,
    }
}

fn refresh_is_stale(
    profiles: &ProfileCatalog,
    profile_id: ProfileId,
    context: RefreshContext,
) -> bool {
    let Ok(current) = profiles.refresh_context(profile_id) else {
        return true;
    };
    current.profile_revision != context.profile_revision
        || (profiles.active_profile_id() == Some(profile_id)
            && current.active_revision != context.active_revision)
}

fn checked_rule(value: String) -> Result<RuleString, ApplicationError> {
    let rule = RuleString::new(value, RULE_STRING_MAX_BYTES).map_err(|_| invalid_rule_error())?;
    parse_rule(&rule).map_err(|_| invalid_rule_error())?;
    Ok(rule)
}

fn map_rule_placement(
    placement: ApplicationRulePlacement,
) -> Result<RulePlacement, ApplicationError> {
    Ok(match placement {
        ApplicationRulePlacement::Prepend => RulePlacement::Prepend,
        ApplicationRulePlacement::Append => RulePlacement::Append,
        ApplicationRulePlacement::Before(rule) => RulePlacement::Before(checked_rule(rule)?),
        ApplicationRulePlacement::After(rule) => RulePlacement::After(checked_rule(rule)?),
    })
}

fn resolve_profile(
    profiles: &ProfileCatalog,
    selector: &str,
) -> Result<ProfileId, ApplicationError> {
    profiles
        .resolve(selector)
        .map_err(|error| map_profile_error(profiles, error))
}

fn map_profile_error(profiles: &ProfileCatalog, error: ProfileSelectorError) -> ApplicationError {
    match error {
        ProfileSelectorError::NotFound => ApplicationError::new(
            ErrorCode::ProfileNotFound,
            "The Profile was not found",
            false,
        ),
        ProfileSelectorError::Active => ApplicationError::new(
            ErrorCode::ProfileActive,
            "The Active Profile cannot be removed",
            false,
        ),
        ProfileSelectorError::Ambiguous { candidate_ids } => ApplicationError::new(
            ErrorCode::ProfileAmbiguous,
            "The Profile selector is ambiguous",
            false,
        )
        .with_selector_candidates(
            SelectorKind::Profile,
            candidate_ids
                .into_iter()
                .map(|id| {
                    SelectorCandidate::new(
                        id.to_string(),
                        profiles
                            .get(id)
                            .map_or_else(|| id.to_string(), |profile| profile.name.clone()),
                    )
                })
                .collect(),
        ),
        ProfileSelectorError::ActiveRevisionExhausted => internal_error(),
    }
}

fn map_rule_error(error: RuleSetError) -> ApplicationError {
    match error {
        RuleSetError::RulesUninitialized => ApplicationError::new(
            ErrorCode::RulesUninitialized,
            "The Local Rule Set is uninitialized",
            false,
        ),
        RuleSetError::RuleNotFound => ApplicationError::new(
            ErrorCode::RuleNotFound,
            "The Rule String was not found",
            false,
        ),
        RuleSetError::RuleAmbiguous { matching_indexes } => ApplicationError::new(
            ErrorCode::RuleAmbiguous,
            "The Rule String is ambiguous",
            false,
        )
        .with_selector_candidates(
            SelectorKind::Rule,
            matching_indexes
                .into_iter()
                .map(|index| SelectorCandidate::new(index.to_string(), index.to_string()))
                .collect(),
        ),
        RuleSetError::RuleAlreadyExists { matching_indexes } => ApplicationError::new(
            ErrorCode::RuleAlreadyExists,
            "The Rule String already exists",
            false,
        )
        .with_selector_candidates(
            SelectorKind::Rule,
            matching_indexes
                .into_iter()
                .map(|index| SelectorCandidate::new(index.to_string(), index.to_string()))
                .collect(),
        ),
        RuleSetError::InvalidRule(_) => invalid_rule_error(),
        RuleSetError::RevisionExhausted => internal_error(),
    }
}

fn map_selection_error(error: SelectionError) -> ApplicationError {
    match error {
        SelectionError::NodeAmbiguous {
            name,
            candidate_ids,
        } => ApplicationError::new(
            ErrorCode::NodeAmbiguous,
            "The Node selector is ambiguous",
            false,
        )
        .with_selector_candidates(
            SelectorKind::Node,
            candidate_ids
                .into_iter()
                .map(|id| SelectorCandidate::new(id.as_str(), &name))
                .collect(),
        ),
        SelectionError::GroupMissing(_) => {
            selector_not_found(SelectorKind::ProxyGroup, "Proxy Group")
        }
        SelectionError::NodeMissing(_) => selector_not_found(SelectorKind::Node, "Node"),
        SelectionError::GroupNotSelectable(_)
        | SelectionError::NodeUnavailable(_)
        | SelectionError::ProviderUnavailable(_)
        | SelectionError::TargetIsGroup(_) => core_error("The selected Node is unavailable"),
    }
}

fn refresh_stage_for_transaction_failure(failure: &SupervisorTransactionFailure) -> RefreshStage {
    if matches!(
        failure.kind,
        SupervisorTransactionFailureKind::Coordinator(ConfigTransactionErrorKind::Validation)
    ) {
        RefreshStage::Validate
    } else {
        RefreshStage::Apply
    }
}

fn map_config_error(error: ConfigError) -> ApplicationError {
    match error {
        ConfigError::UnavailableReference { .. } => ApplicationError::new(
            ErrorCode::PolicyTargetNotFound,
            "The configuration contains an unavailable Policy Target",
            false,
        ),
        _ => ApplicationError::new(ErrorCode::ExternalOperationFailed, error.to_string(), false),
    }
}

fn map_transaction_error(error: SupervisorTransactionFailure) -> ApplicationError {
    match error.kind {
        SupervisorTransactionFailureKind::Busy => rule_busy_error(),
        kind => ApplicationError::new(
            ErrorCode::ExternalOperationFailed,
            "Runtime Apply failed and the committed configuration was retained",
            false,
        )
        .with_details(ApplicationErrorDetails::RuntimeApplyFailure(Box::new(
            RuntimeApplyFailureDetails {
                candidate_generation: error.candidate_generation,
                committed_generation: error.committed_generation,
                stage: transaction_failure_stage(kind),
                recovery: application_recovery(error.recovery),
            },
        ))),
    }
}

fn transaction_failure_stage(kind: SupervisorTransactionFailureKind) -> RuntimeApplyFailureStage {
    match kind {
        SupervisorTransactionFailureKind::Busy => RuntimeApplyFailureStage::Lock,
        SupervisorTransactionFailureKind::State => RuntimeApplyFailureStage::State,
        SupervisorTransactionFailureKind::Bundle => RuntimeApplyFailureStage::Bundle,
        SupervisorTransactionFailureKind::Coordinator(kind) => match kind {
            ConfigTransactionErrorKind::Busy | ConfigTransactionErrorKind::LockPoisoned => {
                RuntimeApplyFailureStage::Lock
            }
            ConfigTransactionErrorKind::RecoveryRequired => {
                RuntimeApplyFailureStage::RecoveryRequired
            }
            ConfigTransactionErrorKind::StaleCandidate => RuntimeApplyFailureStage::StaleCandidate,
            ConfigTransactionErrorKind::InvalidCandidate => {
                RuntimeApplyFailureStage::InvalidCandidate
            }
            ConfigTransactionErrorKind::Validation => RuntimeApplyFailureStage::Validation,
            ConfigTransactionErrorKind::Prepare => RuntimeApplyFailureStage::Prepare,
            ConfigTransactionErrorKind::Apply => RuntimeApplyFailureStage::Apply,
            ConfigTransactionErrorKind::IndeterminateApply => {
                RuntimeApplyFailureStage::IndeterminateApply
            }
            ConfigTransactionErrorKind::Health => RuntimeApplyFailureStage::Health,
            ConfigTransactionErrorKind::Commit => RuntimeApplyFailureStage::Commit,
            ConfigTransactionErrorKind::Cleanup => RuntimeApplyFailureStage::Cleanup,
            ConfigTransactionErrorKind::Recovery => RuntimeApplyFailureStage::Recovery,
        },
    }
}

fn settle_transaction(
    state: &mut SupervisorState,
    result: Result<ConfigTransactionSuccess, SupervisorTransactionFailure>,
) -> Result<ConfigTransactionSuccess, ApplicationError> {
    match result {
        Ok(success) => {
            if recovery_requires_degraded(success.recovery) {
                state.degraded = true;
            }
            Ok(success)
        }
        Err(error) => {
            if recovery_requires_degraded(error.recovery) {
                state.degraded = true;
            }
            Err(map_transaction_error(error))
        }
    }
}

fn recovery_requires_degraded(recovery: TransactionRecoveryOutcome) -> bool {
    matches!(
        recovery,
        TransactionRecoveryOutcome::Pending { .. } | TransactionRecoveryOutcome::Failed { .. }
    )
}

fn runtime_apply_success(success: ConfigTransactionSuccess) -> RuntimeApplyOutcome {
    let status = match success.recovery {
        TransactionRecoveryOutcome::NotRequired => RuntimeApplyStatus::Applied,
        TransactionRecoveryOutcome::Converged { .. } => RuntimeApplyStatus::Recovered,
        TransactionRecoveryOutcome::Pending { .. } | TransactionRecoveryOutcome::Failed { .. } => {
            RuntimeApplyStatus::Applied
        }
    };
    RuntimeApplyOutcome {
        status,
        candidate_generation: Some(success.candidate_generation),
        committed_generation: Some(success.committed_generation),
        recovery: application_recovery(success.recovery),
    }
}

fn application_recovery(recovery: TransactionRecoveryOutcome) -> ApplicationRecoveryOutcome {
    match recovery {
        TransactionRecoveryOutcome::NotRequired => ApplicationRecoveryOutcome {
            status: RecoveryStatus::NotRequired,
            restored_generation: None,
            message: None,
        },
        TransactionRecoveryOutcome::Converged { generation } => ApplicationRecoveryOutcome {
            status: RecoveryStatus::Succeeded,
            restored_generation: generation,
            message: Some("The committed Runtime Generation was confirmed".to_owned()),
        },
        TransactionRecoveryOutcome::Pending { target } => ApplicationRecoveryOutcome {
            status: RecoveryStatus::Failed,
            restored_generation: target,
            message: Some("Committed state cleanup is pending".to_owned()),
        },
        TransactionRecoveryOutcome::Failed { target } => ApplicationRecoveryOutcome {
            status: RecoveryStatus::Failed,
            restored_generation: target,
            message: Some("Committed state recovery failed".to_owned()),
        },
    }
}

fn runtime_apply_not_required(generation: Option<RuntimeGeneration>) -> RuntimeApplyOutcome {
    RuntimeApplyOutcome {
        status: RuntimeApplyStatus::NotRequired,
        candidate_generation: None,
        committed_generation: generation,
        recovery: ApplicationRecoveryOutcome {
            status: RecoveryStatus::NotRequired,
            restored_generation: None,
            message: None,
        },
    }
}

fn initial_status_snapshot(
    started_at_unix_ms: u64,
    profiles: &ProfileCatalog,
    runtime_generation: Option<RuntimeGeneration>,
) -> StatusSnapshot {
    let active_profile = profiles
        .active_profile_id()
        .and_then(|id| profiles.get(id))
        .map(|profile| ActiveProfileSummary {
            id: profile.id,
            name: profile.name.clone(),
        });
    let unconfigured = active_profile.is_none();
    StatusSnapshot {
        supervisor: SupervisorStatus {
            lifecycle: SupervisorLifecycle::Ready,
            started_at_unix_ms,
            uptime_seconds: 0,
        },
        core: CoreStatus {
            lifecycle: if unconfigured {
                CoreLifecycle::Unconfigured
            } else {
                CoreLifecycle::Stopped
            },
            pid: None,
            instance_generation: None,
        },
        tun: TunStatus {
            requested: true,
            capable: false,
            effective: false,
            reason: Some(if unconfigured {
                TunReason::NoActiveProfile
            } else {
                TunReason::CoreUnavailable
            }),
        },
        active_profile,
        primary_proxy_group: None,
        selected_node: None,
        latency: None,
        traffic: unavailable_traffic(),
        connection_count: 0,
        runtime_generation,
        apply_state: ApplyState::Idle,
        stream_health: disconnected_stream_health(),
    }
}

fn unavailable_traffic() -> TrafficSample {
    TrafficSample {
        upload_bytes_per_second: 0,
        download_bytes_per_second: 0,
        sampled_at_unix_ms: None,
        state: SampleState::Unavailable,
    }
}

fn empty_log_tail() -> LogTail {
    LogTail {
        records: Vec::new(),
        dropped_total: 0,
        gap: false,
        earliest_sequence: None,
        latest_sequence: None,
    }
}

fn bounded_message(message: String) -> String {
    message.chars().take(1_024).collect()
}

fn selector_not_found(kind: SelectorKind, label: &str) -> ApplicationError {
    ApplicationError::new(
        match kind {
            SelectorKind::Profile => ErrorCode::ProfileNotFound,
            SelectorKind::ProxyGroup => ErrorCode::ProxyGroupNotFound,
            SelectorKind::Node => ErrorCode::NodeNotFound,
            SelectorKind::Rule => ErrorCode::RuleNotFound,
        },
        format!("The {label} was not found"),
        false,
    )
}

fn invalid_rule_error() -> ApplicationError {
    ApplicationError::new(
        ErrorCode::ExternalOperationFailed,
        "The Rule String is invalid",
        false,
    )
}

fn rule_busy_error() -> ApplicationError {
    ApplicationError::new(
        ErrorCode::RuleBusy,
        "The configuration transaction is busy",
        true,
    )
}

fn core_error(message: &'static str) -> ApplicationError {
    ApplicationError::new(ErrorCode::CoreUnavailable, message, true)
}

fn no_active_profile() -> ApplicationError {
    ApplicationError::new(
        ErrorCode::ProfileNotFound,
        "No Active Profile is configured",
        false,
    )
}

fn internal_error() -> ApplicationError {
    ApplicationError::new(
        ErrorCode::Internal,
        "The Supervisor state is unavailable",
        false,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::profile_source::{ProfileDownload, ProfileSource, ProfileSourceError};
    use async_trait::async_trait;
    use std::sync::Barrier;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct ConcurrentSource {
        active: AtomicUsize,
        maximum: AtomicUsize,
        entered: Barrier,
    }

    #[async_trait]
    impl ProfileSource for ConcurrentSource {
        async fn download(
            &self,
            subscription_url: &SubscriptionUrl,
        ) -> Result<ProfileDownload, ProfileSourceError> {
            let active = self.active.fetch_add(1, Ordering::AcqRel) + 1;
            self.maximum.fetch_max(active, Ordering::AcqRel);
            self.entered.wait();
            self.active.fetch_sub(1, Ordering::AcqRel);
            Ok(ProfileDownload::from_parts(
                b"rules: [MATCH,DIRECT]".to_vec(),
                None,
                subscription_url.redacted(),
            ))
        }
    }

    #[test]
    fn blocking_adapter_runs_two_profile_downloads_concurrently() {
        let source = Arc::new(ConcurrentSource {
            active: AtomicUsize::new(0),
            maximum: AtomicUsize::new(0),
            entered: Barrier::new(crate::constants::PROFILE_REFRESH_CONCURRENCY),
        });
        let adapter = Arc::new(
            BlockingProfileFetchPort::new(source.clone())
                .expect("the Profile download runtime should start"),
        );
        let url = SubscriptionUrl::parse("https://example.test/profile.yaml")
            .expect("the fixture URL should be valid");
        let workers = (0..crate::constants::PROFILE_REFRESH_CONCURRENCY)
            .map(|_| {
                let adapter = Arc::clone(&adapter);
                let url = url.clone();
                std::thread::spawn(move || adapter.fetch(&url))
            })
            .collect::<Vec<_>>();

        for worker in workers {
            worker
                .join()
                .expect("the download worker should finish")
                .expect("the fixture download should succeed");
        }
        assert_eq!(
            source.maximum.load(Ordering::Acquire),
            crate::constants::PROFILE_REFRESH_CONCURRENCY
        );
    }

    #[test]
    fn activation_queue_keeps_only_the_latest_pending_target() {
        let mut queue = ActivationQueue {
            running: true,
            pending: None,
        };
        let superseded = queue.enqueue("secondary");
        let latest = queue.enqueue("tertiary");

        let error = superseded
            .wait()
            .expect_err("the older pending activation should be superseded");
        assert_eq!(error.code, ErrorCode::OperationUnavailable);
        assert!(error.retryable);
        assert_eq!(
            queue
                .pending
                .as_ref()
                .map(|pending| pending.selector.as_str()),
            Some("tertiary")
        );
        assert!(Arc::ptr_eq(
            &queue
                .pending
                .as_ref()
                .expect("the latest activation should remain pending")
                .completion,
            &latest
        ));
    }
}
