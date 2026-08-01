use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, BTreeSet};
use std::hash::{Hash, Hasher};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Condvar;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, TryLockError};

use crate::application::{
    ApplicationClient, ApplicationError, ApplicationErrorDetails, ApplicationOperation,
    ApplicationOutput, Clock, LatencyFreshness as ApplicationLatencyFreshness, LatencyListOutcome,
    LatencyProbeStatus as ApplicationLatencyProbeStatus, LatencyShowOutcome, LatencySummary,
    PolicyTargetValidation, ProfileListOutcome, ProfileListPageOutcome, ProfileMutationAction,
    ProfileMutationOutcome, ProfileRefreshFailure, ProfileRefreshStage, ProfileRefreshState,
    ProfileSummary, ProxyAvailability, ProxyGroupSummary, ProxyListOutcome, ProxyListPageOutcome,
    ProxyMemberKind, ProxyNodeRow, ProxyNodeSource, ProxySelectionOutcome,
    RecoveryOutcome as ApplicationRecoveryOutcome, RecoveryStatus, RuleListOutcome,
    RuleListPageOutcome, RuleMutationAction, RuleMutationOutcome,
    RulePlacement as ApplicationRulePlacement, RuleSummary, RuntimeApplyFailureDetails,
    RuntimeApplyFailureStage, RuntimeApplyOutcome, RuntimeApplyStatus, SelectorCandidate,
    SelectorIdentity, SelectorKind,
};
use crate::config::{
    AuthoritativeConfig, ConfigCompiler, ConfigError, CoreConfigValidator, EffectiveConfiguration,
};
use crate::constants::{
    CORE_LOG_LINE_MAX_BYTES, IPC_LIST_PAGE_SIZE, LOG_CAPACITY, PROBE_TIMEOUT, PROBE_URL,
    PROFILE_COUNT_MAX, PROFILE_REFRESH_INTERVAL, RULE_STRING_MAX_BYTES,
    SELECTION_RESTORE_ATTEMPT_LIMIT, TRAFFIC_SERIES_CAPACITY, YAML_MAX_DEPTH,
};
use crate::core::{
    Availability, CoreRuntime, CoreRuntimeDiagnosticCategory as RuntimeDiagnosticCategory,
    CoreRuntimeLifecycle, CoreRuntimeStatus, CoreRuntimeTunReason, DelayProbeRequest, DelayTarget,
    ManagedCoreHandle, MihomoAdapter, MihomoError, MihomoErrorKind, NodeRowMemberV1, NodeSelection,
    NodeSource, ProbeObservation, ProbeStatus as CoreProbeStatus, ProxyView, RuntimeBundle,
    SelectionError,
};
use crate::domain::{
    ActiveProfileSummary, CoreDiagnosticCategory, CoreInstanceGeneration, CoreLifecycle,
    CoreRestartStatus, CoreStatus, NodeRecordId, ProbeGeneration, ProbeQueueStatus, ProfileId,
    ProxyGroupId, RuntimeApplyPhase, RuntimeApplySnapshot, RuntimeGeneration,
    RuntimeRecoverySnapshot, RuntimeRecoveryStatus, SampleState, SelectedNodeSummary,
    StatusSnapshot, StreamHealthSet, StreamState, SubscriptionUrl, SupervisorHealthReason,
    SupervisorLifecycle, SupervisorStatus, TrafficSample, TunReason, TunStatus,
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
    RuleConfigTransactionReservation,
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

    fn cancel_pending(&self) {}
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

    fn cancel_pending(&self) {
        self.source.cancel_pending();
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
    fn try_reserve_rule(
        &self,
    ) -> Result<Box<dyn SupervisorRuleTransactionReservation + '_>, SupervisorTransactionFailure>;

    fn apply(
        &self,
        request: SupervisorTransactionRequest<'_>,
        fail_fast: bool,
    ) -> Result<ConfigTransactionSuccess, SupervisorTransactionFailure>;

    fn apply_startup(
        &self,
        request: SupervisorTransactionRequest<'_>,
    ) -> Result<ConfigTransactionSuccess, SupervisorTransactionFailure> {
        self.apply(request, false)
    }

    fn persist_metadata(
        &self,
        request: SupervisorTransactionRequest<'_>,
    ) -> Result<(), SupervisorTransactionFailure>;

    fn prepare_startup_reapply(
        &self,
        committed_generation: Option<RuntimeGeneration>,
    ) -> Result<RuntimeGeneration, SupervisorTransactionFailure> {
        committed_generation
            .map_or(Some(1), |generation| generation.0.checked_add(1))
            .filter(|generation| *generation > 0)
            .map(RuntimeGeneration)
            .ok_or_else(|| {
                SupervisorTransactionFailure::new(SupervisorTransactionFailureKind::State)
            })
    }

    fn set_current_revisions(&self, revisions: CandidateRevisions);
}

pub trait SupervisorRuleTransactionReservation {
    fn apply(
        &self,
        request: SupervisorTransactionRequest<'_>,
    ) -> Result<ConfigTransactionSuccess, SupervisorTransactionFailure>;
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

    fn apply_staged_transaction(
        &self,
        request: SupervisorTransactionRequest<'_>,
        execute: impl FnOnce(
            &ConfigTransactionCandidate,
        ) -> Result<ConfigTransactionSuccess, ConfigTransactionError>,
    ) -> Result<ConfigTransactionSuccess, SupervisorTransactionFailure> {
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
        let result = execute(&candidate);
        match result {
            Ok(success) => Ok(success),
            Err(error) => {
                self.revisions.set(previous);
                Err(error.into())
            }
        }
    }
}

struct CoordinatedRuleTransactionReservation<'a> {
    transactions: &'a CoordinatedSupervisorTransactions,
    coordinator: RuleConfigTransactionReservation<'a>,
    _transaction_guard: MutexGuard<'a, ()>,
}

impl SupervisorRuleTransactionReservation for CoordinatedRuleTransactionReservation<'_> {
    fn apply(
        &self,
        request: SupervisorTransactionRequest<'_>,
    ) -> Result<ConfigTransactionSuccess, SupervisorTransactionFailure> {
        self.transactions
            .apply_staged_transaction(request, |candidate| self.coordinator.execute(candidate))
    }
}

impl SupervisorTransactionPort for CoordinatedSupervisorTransactions {
    fn try_reserve_rule(
        &self,
    ) -> Result<Box<dyn SupervisorRuleTransactionReservation + '_>, SupervisorTransactionFailure>
    {
        let transaction_guard = self
            .transaction_lock
            .try_lock()
            .map_err(|error| match error {
                TryLockError::WouldBlock => {
                    SupervisorTransactionFailure::new(SupervisorTransactionFailureKind::Busy)
                }
                TryLockError::Poisoned(_) => {
                    SupervisorTransactionFailure::new(SupervisorTransactionFailureKind::State)
                }
            })?;
        let coordinator = self
            .coordinator
            .try_reserve_rule()
            .map_err(SupervisorTransactionFailure::from)?;
        Ok(Box::new(CoordinatedRuleTransactionReservation {
            transactions: self,
            coordinator,
            _transaction_guard: transaction_guard,
        }))
    }

    fn apply(
        &self,
        request: SupervisorTransactionRequest<'_>,
        fail_fast: bool,
    ) -> Result<ConfigTransactionSuccess, SupervisorTransactionFailure> {
        if fail_fast {
            return self.try_reserve_rule()?.apply(request);
        }
        let _guard = self.transaction_lock.lock().map_err(|_| {
            SupervisorTransactionFailure::new(SupervisorTransactionFailureKind::State)
        })?;
        self.apply_staged_transaction(request, |candidate| self.coordinator.execute(candidate))
    }

    fn apply_startup(
        &self,
        request: SupervisorTransactionRequest<'_>,
    ) -> Result<ConfigTransactionSuccess, SupervisorTransactionFailure> {
        let _guard = self.transaction_lock.lock().map_err(|_| {
            SupervisorTransactionFailure::new(SupervisorTransactionFailureKind::State)
        })?;
        self.apply_staged_transaction(request, |candidate| {
            self.coordinator.execute_startup_reapply(candidate)
        })
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

    fn prepare_startup_reapply(
        &self,
        committed_generation: Option<RuntimeGeneration>,
    ) -> Result<RuntimeGeneration, SupervisorTransactionFailure> {
        let _guard = self.transaction_lock.lock().map_err(|_| {
            SupervisorTransactionFailure::new(SupervisorTransactionFailureKind::State)
        })?;
        self.coordinator
            .prepare_startup_reapply(committed_generation)
            .map(|recovery| recovery.candidate_generation)
            .map_err(Into::into)
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
    observed_core_generation: Option<CoreInstanceGeneration>,
    probe_core_generation: Option<CoreInstanceGeneration>,
    selection_restore_pending: bool,
    selection_restore_attempts_remaining: usize,
    health_reasons: BTreeSet<SupervisorHealthReason>,
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
                health_reasons: startup_health_reasons,
            }),
            activation: Mutex::new(ActivationQueue::default()),
            apply_in_progress: AtomicBool::new(false),
            last_runtime_apply: Mutex::new(startup_runtime_apply),
            last_status: Mutex::new(initial_status),
        };
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
            && let Ok(order) = effective_group_order_with_health(&mut state)
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
                set_health_reason(
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
                set_health_reason(
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
                state
                    .health_reasons
                    .insert(SupervisorHealthReason::SelectionCompensation);
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
        state
            .health_reasons
            .remove(&SupervisorHealthReason::SelectionCompensation);
        state
            .health_reasons
            .remove(&SupervisorHealthReason::SelectionRestoration);
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
        let order = effective_group_order_with_health(state)?;
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
            state
                .health_reasons
                .remove(&SupervisorHealthReason::ConfigurationProjection);
            state
                .health_reasons
                .remove(&SupervisorHealthReason::ProbeScheduler);
            state
                .health_reasons
                .remove(&SupervisorHealthReason::SelectionRestoration);
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
        let order = match effective_group_order_with_health(state) {
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
            state
                .health_reasons
                .insert(SupervisorHealthReason::ProbeScheduler);
        } else {
            state.probe_core_generation = Some(core_generation);
            state
                .health_reasons
                .remove(&SupervisorHealthReason::ProbeScheduler);
        }
    }

    fn begin_selection_restore(&self, state: &mut SupervisorState) {
        let Some(active_id) = state.profiles.active_profile_id() else {
            state.selection_restore_pending = false;
            state.selection_restore_attempts_remaining = 0;
            state
                .health_reasons
                .remove(&SupervisorHealthReason::SelectionRestoration);
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
            state
                .health_reasons
                .remove(&SupervisorHealthReason::SelectionRestoration);
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
            state
                .health_reasons
                .remove(&SupervisorHealthReason::SelectionRestoration);
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
            state
                .health_reasons
                .remove(&SupervisorHealthReason::SelectionRestoration);
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
            state
                .health_reasons
                .insert(SupervisorHealthReason::SelectionRestoration);
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

fn profile_list_snapshot_id(profiles: &ProfileCatalog) -> u64 {
    let mut hasher = DefaultHasher::new();
    profiles.len().hash(&mut hasher);
    profiles.active_profile_id().hash(&mut hasher);
    for profile in profiles.profiles() {
        profile.id.hash(&mut hasher);
        profile.name.hash(&mut hasher);
        profile.subscription_url.expose().as_str().hash(&mut hasher);
        profile.revision.0.hash(&mut hasher);
        profile.last_success_at_unix_ms.hash(&mut hasher);
        profile.next_refresh_at_unix_ms.hash(&mut hasher);
        match &profile.last_error {
            Some(failure) => {
                1_u8.hash(&mut hasher);
                std::mem::discriminant(&failure.stage).hash(&mut hasher);
                failure.safe_message.hash(&mut hasher);
            }
            None => 0_u8.hash(&mut hasher),
        }
    }
    hasher.finish()
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

fn effective_group_order_with_health(
    state: &mut SupervisorState,
) -> Result<Vec<String>, ApplicationError> {
    match effective_group_order(&state.profiles) {
        Ok(order) => {
            state
                .health_reasons
                .remove(&SupervisorHealthReason::ConfigurationProjection);
            Ok(order)
        }
        Err(error) => {
            state
                .health_reasons
                .insert(SupervisorHealthReason::ConfigurationProjection);
            Err(error)
        }
    }
}

fn set_health_reason(state: &mut SupervisorState, reason: SupervisorHealthReason, active: bool) {
    if active {
        state.health_reasons.insert(reason);
    } else {
        state.health_reasons.remove(&reason);
    }
}

fn resolve_proxy_group<'a>(
    view: &'a ProxyView,
    selector: &str,
) -> Result<&'a crate::core::ProxyGroup, ApplicationError> {
    if let Ok(id) = ProxyGroupId::parse(selector) {
        return view
            .groups
            .iter()
            .find(|group| group.id == id)
            .ok_or_else(|| selector_not_found(SelectorKind::ProxyGroup, "Proxy Group"));
    }
    view.groups
        .iter()
        .find(|group| group.name == selector)
        .ok_or_else(|| selector_not_found(SelectorKind::ProxyGroup, "Proxy Group"))
}

fn selection_by_selector(
    view: &ProxyView,
    group_name: &str,
    selector: &str,
) -> Result<NodeSelection, SelectionError> {
    let group = view
        .groups
        .iter()
        .find(|group| group.name == group_name)
        .ok_or_else(|| SelectionError::GroupMissing(group_name.to_owned()))?;
    if !group.selectable {
        return Err(SelectionError::GroupNotSelectable(group_name.to_owned()));
    }
    if let Ok(record_id) = NodeRecordId::parse(selector) {
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

fn probe_observations_page(
    state: &SupervisorState,
    group: &crate::core::ProxyGroup,
    offset: usize,
    limit: usize,
    now_unix_ms: u64,
) -> BTreeMap<NodeRecordId, ProbeObservation> {
    let end = offset.saturating_add(limit).min(group.members.len());
    group
        .members
        .get(offset..end)
        .unwrap_or_default()
        .iter()
        .filter_map(|member| match member {
            crate::core::ProxyMember::Node { record_id, .. } => state
                .probes
                .node_snapshot(record_id, now_unix_ms)
                .map(|snapshot| {
                    (
                        record_id.clone(),
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
                }),
            _ => None,
        })
        .collect()
}

fn proxy_list_snapshot_id(view: &ProxyView, generation: CoreInstanceGeneration) -> u64 {
    let mut hasher = DefaultHasher::new();
    generation.hash(&mut hasher);
    view.schema_version.hash(&mut hasher);
    std::mem::discriminant(&view.order_source).hash(&mut hasher);
    std::mem::discriminant(&view.provider_state).hash(&mut hasher);
    for group in &view.groups {
        group.id.hash(&mut hasher);
        group.name.hash(&mut hasher);
        group.proxy_type.hash(&mut hasher);
        std::mem::discriminant(&group.availability).hash(&mut hasher);
        group.selectable.hash(&mut hasher);
        group.core_internal.hash(&mut hasher);
        group.selected_name.hash(&mut hasher);
        for member in &group.members {
            std::mem::discriminant(member).hash(&mut hasher);
            match member {
                crate::core::ProxyMember::Group { name } => name.hash(&mut hasher),
                crate::core::ProxyMember::Node {
                    name,
                    record_id,
                    availability,
                } => {
                    name.hash(&mut hasher);
                    record_id.hash(&mut hasher);
                    std::mem::discriminant(availability).hash(&mut hasher);
                }
                crate::core::ProxyMember::Unresolved {
                    name,
                    reason,
                    candidate_ids,
                } => {
                    name.hash(&mut hasher);
                    std::mem::discriminant(reason).hash(&mut hasher);
                    candidate_ids.hash(&mut hasher);
                }
            }
        }
    }
    for (record_id, node) in &view.nodes {
        record_id.hash(&mut hasher);
        node.name.hash(&mut hasher);
        node.proxy_type.hash(&mut hasher);
        std::mem::discriminant(&node.availability).hash(&mut hasher);
        node.core_internal.hash(&mut hasher);
        std::mem::discriminant(&node.source).hash(&mut hasher);
        match &node.source {
            crate::core::NodeSource::Core { proxy_name } => proxy_name.hash(&mut hasher),
            crate::core::NodeSource::Provider {
                provider_name,
                proxy_name,
            } => {
                provider_name.hash(&mut hasher);
                proxy_name.hash(&mut hasher);
            }
        }
    }
    hasher.finish()
}

fn proxy_group_summary(view: &ProxyView, group: &crate::core::ProxyGroup) -> ProxyGroupSummary {
    let selected_node = group
        .selected_name
        .as_deref()
        .and_then(|name| selection_by_selector(view, &group.name, name).ok())
        .map(|selection| SelectorIdentity {
            id: selection.record_id.as_str().to_owned(),
            name: selection.node_name,
        });
    ProxyGroupSummary {
        id: group.id.clone(),
        name: group.name.clone(),
        proxy_type: group.proxy_type.clone(),
        selectable: group.selectable,
        selected_node,
    }
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

fn rule_summary(entry: crate::rule::RuleListEntry<'_>) -> RuleSummary {
    RuleSummary {
        index: entry.index,
        rule_string: entry.rule.as_str().to_owned(),
        rule_type: entry.parsed.rule_type.as_str().to_owned(),
        payload: entry.parsed.payload.map(str::to_owned),
        policy_target: entry.parsed.policy_target.to_owned(),
        params: entry.parsed.params.into_iter().map(str::to_owned).collect(),
        policy_target_validation: PolicyTargetValidation::Valid,
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
            .filter(|node| probe_eligible_node(node))
            .ok_or_else(|| selector_not_found(SelectorKind::Node, "Node"));
    }
    let candidates = view
        .nodes
        .values()
        .filter(|node| probe_eligible_node(node) && node.name == selector)
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

fn probe_eligible_node(node: &crate::core::ProxyNode) -> bool {
    !node.core_internal
}

struct CoreHealthProjection {
    managed_core: Option<ManagedCoreHandle>,
    core: CoreStatus,
    tun: TunStatus,
    degraded: bool,
}

impl CoreHealthProjection {
    fn unconfigured() -> Self {
        Self {
            managed_core: None,
            core: CoreStatus {
                lifecycle: CoreLifecycle::Unconfigured,
                pid: None,
                instance_generation: None,
                restart: CoreRestartStatus::default(),
            },
            tun: TunStatus {
                requested: true,
                capable: false,
                effective: false,
                reason: Some(TunReason::NoActiveProfile),
            },
            degraded: false,
        }
    }

    fn unavailable() -> Self {
        Self {
            managed_core: None,
            core: CoreStatus {
                lifecycle: CoreLifecycle::Degraded,
                pid: None,
                instance_generation: None,
                restart: CoreRestartStatus::default(),
            },
            tun: TunStatus {
                requested: true,
                capable: false,
                effective: false,
                reason: Some(TunReason::CoreUnavailable),
            },
            degraded: true,
        }
    }

    fn from_runtime(status: CoreRuntimeStatus) -> Self {
        let (lifecycle, managed_core, degraded) = match (status.lifecycle, status.managed_core) {
            (CoreRuntimeLifecycle::Owned, None) => (CoreLifecycle::Stopped, None, false),
            (CoreRuntimeLifecycle::Running, Some(core)) => {
                (CoreLifecycle::Ready, Some(core), false)
            }
            (CoreRuntimeLifecycle::RestartPending, _) => (CoreLifecycle::Starting, None, false),
            (CoreRuntimeLifecycle::Degraded, _) => (CoreLifecycle::Degraded, None, true),
            (CoreRuntimeLifecycle::Owned | CoreRuntimeLifecycle::Running, _) => {
                (CoreLifecycle::Degraded, None, true)
            }
        };
        let capable = status.tun.capable;
        let effective = lifecycle == CoreLifecycle::Ready && capable;
        let runtime_tun_reason = status.tun.reason.map(|reason| match reason {
            CoreRuntimeTunReason::PermissionDenied => TunReason::PermissionDenied,
            CoreRuntimeTunReason::Unsupported => TunReason::Unsupported,
        });
        let reason = if effective {
            None
        } else {
            runtime_tun_reason.or(Some(TunReason::CoreUnavailable))
        };
        let restart = CoreRestartStatus {
            pending: status.restart.pending,
            attempts: u64::try_from(status.restart.attempts).unwrap_or(u64::MAX),
            backoff_ms: status
                .restart
                .backoff
                .map(|backoff| u64::try_from(backoff.as_millis()).unwrap_or(u64::MAX)),
            diagnostic: status.restart.diagnostic.map(|category| match category {
                RuntimeDiagnosticCategory::CoreRestartLimitReached => {
                    CoreDiagnosticCategory::RestartLimitReached
                }
            }),
        };
        Self {
            core: CoreStatus {
                lifecycle,
                pid: managed_core.as_ref().map(|core| core.pid),
                instance_generation: managed_core.as_ref().map(|core| core.instance_generation),
                restart,
            },
            managed_core,
            tun: TunStatus {
                requested: true,
                capable,
                effective,
                reason,
            },
            degraded,
        }
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
        ConfigError::UnsupportedField { .. } | ConfigError::UnsupportedVariant { .. } => {
            ApplicationError::new(
                ErrorCode::ProfileFieldUnsupported,
                "The Profile contains a field unsupported by the bundled Mihomo version",
                false,
            )
        }
        _ => ApplicationError::new(ErrorCode::ExternalOperationFailed, error.to_string(), false),
    }
}

fn map_transaction_error(error: SupervisorTransactionFailure) -> ApplicationError {
    match error.kind {
        SupervisorTransactionFailureKind::Busy => rule_busy_error(),
        SupervisorTransactionFailureKind::Coordinator(
            ConfigTransactionErrorKind::TunPermissionDenied,
        ) => ApplicationError::new(
            ErrorCode::TunPermissionDenied,
            "TUN capability is unavailable for the Managed Core",
            false,
        )
        .with_details(ApplicationErrorDetails::RuntimeApplyFailure(Box::new(
            RuntimeApplyFailureDetails {
                candidate_generation: error.candidate_generation,
                committed_generation: error.committed_generation,
                stage: RuntimeApplyFailureStage::Apply,
                recovery: application_recovery(error.recovery),
            },
        ))),
        SupervisorTransactionFailureKind::Coordinator(
            ConfigTransactionErrorKind::TunUnsupported,
        ) => ApplicationError::new(
            ErrorCode::TunUnsupported,
            "TUN is unsupported on this platform",
            false,
        )
        .with_details(ApplicationErrorDetails::RuntimeApplyFailure(Box::new(
            RuntimeApplyFailureDetails {
                candidate_generation: error.candidate_generation,
                committed_generation: error.committed_generation,
                stage: RuntimeApplyFailureStage::Apply,
                recovery: application_recovery(error.recovery),
            },
        ))),
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
            ConfigTransactionErrorKind::TunPermissionDenied
            | ConfigTransactionErrorKind::TunUnsupported => RuntimeApplyFailureStage::Apply,
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

fn successful_runtime_apply_snapshot(success: ConfigTransactionSuccess) -> RuntimeApplySnapshot {
    let phase = match success.recovery {
        TransactionRecoveryOutcome::NotRequired | TransactionRecoveryOutcome::Converged { .. } => {
            RuntimeApplyPhase::Succeeded
        }
        TransactionRecoveryOutcome::Pending { .. } => RuntimeApplyPhase::Recovering,
        TransactionRecoveryOutcome::Failed { .. } => RuntimeApplyPhase::Failed,
    };
    RuntimeApplySnapshot {
        candidate_generation: Some(success.candidate_generation),
        committed_generation: Some(success.committed_generation),
        phase,
        recovery: runtime_recovery_snapshot(success.recovery),
    }
}

fn failed_runtime_apply_snapshot(error: SupervisorTransactionFailure) -> RuntimeApplySnapshot {
    let phase = if matches!(error.recovery, TransactionRecoveryOutcome::Pending { .. }) {
        RuntimeApplyPhase::Recovering
    } else {
        RuntimeApplyPhase::Failed
    };
    RuntimeApplySnapshot {
        candidate_generation: error.candidate_generation,
        committed_generation: error.committed_generation,
        phase,
        recovery: runtime_recovery_snapshot(error.recovery),
    }
}

fn runtime_recovery_snapshot(recovery: TransactionRecoveryOutcome) -> RuntimeRecoverySnapshot {
    match recovery {
        TransactionRecoveryOutcome::NotRequired => RuntimeRecoverySnapshot::default(),
        TransactionRecoveryOutcome::Converged { generation } => RuntimeRecoverySnapshot {
            status: RuntimeRecoveryStatus::Succeeded,
            restored_generation: generation,
            message: Some("Committed Runtime Generation recovery succeeded".to_owned()),
        },
        TransactionRecoveryOutcome::Pending { target } => RuntimeRecoverySnapshot {
            status: RuntimeRecoveryStatus::Pending,
            restored_generation: target,
            message: Some("Committed Runtime Generation cleanup is pending".to_owned()),
        },
        TransactionRecoveryOutcome::Failed { target } => RuntimeRecoverySnapshot {
            status: RuntimeRecoveryStatus::Failed,
            restored_generation: target,
            message: Some("Committed Runtime Generation recovery failed".to_owned()),
        },
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
            status: RecoveryStatus::Pending,
            restored_generation: target,
            message: Some("Committed Runtime Generation cleanup is pending".to_owned()),
        },
        TransactionRecoveryOutcome::Failed { target } => ApplicationRecoveryOutcome {
            status: RecoveryStatus::Failed,
            restored_generation: target,
            message: Some("Committed Runtime Generation recovery failed".to_owned()),
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
    runtime_apply: RuntimeApplySnapshot,
    health_reasons: Vec<SupervisorHealthReason>,
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
            lifecycle: if health_reasons.is_empty() {
                SupervisorLifecycle::Ready
            } else {
                SupervisorLifecycle::Degraded
            },
            started_at_unix_ms,
            uptime_seconds: 0,
            health_reasons,
        },
        core: CoreStatus {
            lifecycle: if unconfigured {
                CoreLifecycle::Unconfigured
            } else {
                CoreLifecycle::Stopped
            },
            pid: None,
            instance_generation: None,
            restart: CoreRestartStatus::default(),
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
        apply_state: runtime_apply.phase.compatibility_state(),
        runtime_apply,
        selection_restore_pending: false,
        probe_queue: ProbeQueueStatus::default(),
        stream_health: disconnected_stream_health(),
    }
}

fn probe_queue_status(metrics: crate::scheduler::ProbeMetrics) -> ProbeQueueStatus {
    ProbeQueueStatus {
        active_node_count: metrics.active_node_count.try_into().unwrap_or(u64::MAX),
        queue_depth: metrics.queue_depth.try_into().unwrap_or(u64::MAX),
        in_flight_count: metrics.in_flight_count.try_into().unwrap_or(u64::MAX),
        overloaded: metrics.overloaded,
        oldest_due_age_ms: metrics.oldest_due_age_ms,
        estimated_full_pass_duration_ms: metrics.estimated_full_pass_duration_ms,
        stale_node_count: metrics.stale_node_count.try_into().unwrap_or(u64::MAX),
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
