use crate::config::{CoreConfigValidator, EffectiveConfiguration};
use crate::constants::EFFECTIVE_CONFIGURATION_MAX_BYTES;
use crate::core::{
    ApplyCandidateResult, CoreRuntime, CoreRuntimeError, CoreRuntimeStatus, ManagedCoreHandle,
    OwnerSessionProof, RuntimeBundle,
};
use crate::domain::{LocalRuleSetRevision, RuntimeGeneration};
use crate::persistence::{
    CommittedManifest, ObjectId, PersistencePruneResult, PersistenceStore, PreparedTransaction,
    RecoveryState, TransactionBundle, TransactionId,
};
use crate::profile::{ActiveProfileRevision, ProfileRevision};
use std::fmt;
use std::io;
use std::sync::{Arc, Mutex, MutexGuard, TryLockError};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CandidateRevisions {
    pub profile: ProfileRevision,
    pub active_profile: ActiveProfileRevision,
    pub local_rule_set: LocalRuleSetRevision,
    pub compiler_policy_sha256: String,
    pub core_version: String,
}

pub trait CandidateRevisionSource: Send + Sync {
    fn current(&self) -> CandidateRevisions;
}

#[derive(Clone, Eq, PartialEq)]
pub struct ConfigTransactionCandidate {
    pub transaction: TransactionBundle,
    pub runtime: RuntimeBundle,
    pub configuration: EffectiveConfiguration,
    pub revisions: CandidateRevisions,
}

impl fmt::Debug for ConfigTransactionCandidate {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ConfigTransactionCandidate")
            .field("runtime_generation", &self.runtime.generation)
            .field("revisions", &self.revisions)
            .field("configuration", &self.configuration)
            .finish()
    }
}

pub trait TransactionStore: Send + Sync {
    fn read_object_limited(&self, id: &ObjectId, limit: usize) -> io::Result<Vec<u8>>;
    fn prepare(&self, bundle: &TransactionBundle) -> io::Result<PreparedTransaction>;
    fn recover(&self) -> io::Result<RecoveryState>;
    fn commit_prepared(&self, prepared: &PreparedTransaction) -> io::Result<()>;
    fn clear_prepared(&self, prepared: &PreparedTransaction) -> io::Result<()>;
    fn load_transaction(&self, id: &TransactionId) -> io::Result<TransactionBundle>;

    fn prune_unreachable(&self) -> io::Result<PersistencePruneResult> {
        Ok(PersistencePruneResult::default())
    }
}

impl TransactionStore for PersistenceStore {
    fn read_object_limited(&self, id: &ObjectId, limit: usize) -> io::Result<Vec<u8>> {
        PersistenceStore::read_object_limited(self, id, limit)
    }

    fn prepare(&self, bundle: &TransactionBundle) -> io::Result<PreparedTransaction> {
        PersistenceStore::prepare(self, bundle)
    }

    fn recover(&self) -> io::Result<RecoveryState> {
        PersistenceStore::recover(self)
    }

    fn commit_prepared(&self, prepared: &PreparedTransaction) -> io::Result<()> {
        PersistenceStore::commit_prepared(self, prepared)
    }

    fn clear_prepared(&self, prepared: &PreparedTransaction) -> io::Result<()> {
        PersistenceStore::clear_prepared(self, prepared)
    }

    fn load_transaction(&self, id: &TransactionId) -> io::Result<TransactionBundle> {
        PersistenceStore::load_transaction(self, id)
    }

    fn prune_unreachable(&self) -> io::Result<PersistencePruneResult> {
        PersistenceStore::prune_unreachable(self)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeApplyFailure {
    Definite,
    Indeterminate,
    TunPermissionDenied,
}

pub trait RuntimeApplyPort: Send + Sync {
    fn apply_candidate(
        &self,
        owner: &OwnerSessionProof,
        bundle: &RuntimeBundle,
    ) -> Result<ApplyCandidateResult, RuntimeApplyFailure>;

    fn status(&self, owner: &OwnerSessionProof) -> Result<CoreRuntimeStatus, CoreRuntimeError>;

    fn stop(&self, owner: &OwnerSessionProof) -> Result<(), CoreRuntimeError>;
}

pub type ApplyFailureClassifier = fn(&CoreRuntimeError) -> RuntimeApplyFailure;

pub struct CoreRuntimeApplyAdapter {
    runtime: Arc<dyn CoreRuntime>,
    classify: ApplyFailureClassifier,
}

impl CoreRuntimeApplyAdapter {
    #[must_use]
    pub fn new(runtime: Arc<dyn CoreRuntime>, classify: ApplyFailureClassifier) -> Self {
        Self { runtime, classify }
    }
}

impl RuntimeApplyPort for CoreRuntimeApplyAdapter {
    fn apply_candidate(
        &self,
        owner: &OwnerSessionProof,
        bundle: &RuntimeBundle,
    ) -> Result<ApplyCandidateResult, RuntimeApplyFailure> {
        self.runtime
            .apply_candidate(owner, bundle)
            .map_err(|error| (self.classify)(&error))
    }

    fn status(&self, owner: &OwnerSessionProof) -> Result<CoreRuntimeStatus, CoreRuntimeError> {
        self.runtime.status(owner)
    }

    fn stop(&self, owner: &OwnerSessionProof) -> Result<(), CoreRuntimeError> {
        self.runtime.stop(owner).map(|_| ())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RuntimeHealthError;

pub trait RuntimeHealthProbe: Send + Sync {
    fn confirm_ready(&self, managed_core: &ManagedCoreHandle) -> Result<(), RuntimeHealthError>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RuntimeBundleResolveError;

pub trait RuntimeBundleResolver: Send + Sync {
    fn resolve(
        &self,
        transaction: &TransactionBundle,
    ) -> Result<RuntimeBundle, RuntimeBundleResolveError>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ApplyPath {
    Direct,
    CandidateRestart,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConfigTransactionSuccess {
    pub candidate_generation: RuntimeGeneration,
    pub committed_generation: RuntimeGeneration,
    pub apply_path: ApplyPath,
    pub recovery: RecoveryOutcome,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecoveryOutcome {
    NotRequired,
    Converged {
        generation: Option<RuntimeGeneration>,
    },
    Pending {
        target: Option<RuntimeGeneration>,
    },
    Failed {
        target: Option<RuntimeGeneration>,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConfigTransactionErrorKind {
    Busy,
    LockPoisoned,
    RecoveryRequired,
    StaleCandidate,
    InvalidCandidate,
    Validation,
    Prepare,
    TunPermissionDenied,
    Apply,
    IndeterminateApply,
    Health,
    Commit,
    Cleanup,
    Recovery,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConfigTransactionError {
    pub kind: ConfigTransactionErrorKind,
    pub candidate_generation: Option<RuntimeGeneration>,
    pub committed_generation: Option<RuntimeGeneration>,
    pub recovery: RecoveryOutcome,
}

impl ConfigTransactionError {
    fn new(
        kind: ConfigTransactionErrorKind,
        candidate_generation: Option<RuntimeGeneration>,
        committed_generation: Option<RuntimeGeneration>,
        recovery: RecoveryOutcome,
    ) -> Self {
        Self {
            kind,
            candidate_generation,
            committed_generation,
            recovery,
        }
    }

    fn simple(kind: ConfigTransactionErrorKind) -> Self {
        Self::new(kind, None, None, RecoveryOutcome::NotRequired)
    }
}

impl fmt::Display for ConfigTransactionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self.kind {
            ConfigTransactionErrorKind::Busy => "configuration transaction is busy",
            ConfigTransactionErrorKind::LockPoisoned => {
                "configuration transaction lock is unavailable"
            }
            ConfigTransactionErrorKind::RecoveryRequired => {
                "configuration transaction recovery is required"
            }
            ConfigTransactionErrorKind::StaleCandidate => {
                "configuration transaction candidate is stale"
            }
            ConfigTransactionErrorKind::InvalidCandidate => {
                "configuration transaction candidate is invalid"
            }
            ConfigTransactionErrorKind::Validation => "effective configuration validation failed",
            ConfigTransactionErrorKind::Prepare => {
                "configuration transaction journal preparation failed"
            }
            ConfigTransactionErrorKind::TunPermissionDenied => "TUN capability preflight failed",
            ConfigTransactionErrorKind::Apply => "Runtime Apply failed",
            ConfigTransactionErrorKind::IndeterminateApply => {
                "Runtime Apply result remained indeterminate after candidate restart"
            }
            ConfigTransactionErrorKind::Health => {
                "candidate Runtime Generation health confirmation failed"
            }
            ConfigTransactionErrorKind::Commit => {
                "committed Runtime Generation pointer update failed"
            }
            ConfigTransactionErrorKind::Cleanup => {
                "configuration transaction journal cleanup failed"
            }
            ConfigTransactionErrorKind::Recovery => "committed Runtime Generation recovery failed",
        })
    }
}

impl std::error::Error for ConfigTransactionError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StartupRecovery {
    pub committed_generation: Option<RuntimeGeneration>,
    pub cleared_prepared_journal: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StartupReapply {
    pub committed_generation: Option<RuntimeGeneration>,
    pub candidate_generation: RuntimeGeneration,
    pub cleared_prepared_journal: bool,
}

pub struct ConfigTransactionDependencies {
    pub store: Arc<dyn TransactionStore>,
    pub runtime: Arc<dyn RuntimeApplyPort>,
    pub validator: Arc<dyn CoreConfigValidator + Send + Sync>,
    pub health: Arc<dyn RuntimeHealthProbe>,
    pub revisions: Arc<dyn CandidateRevisionSource>,
    pub bundles: Arc<dyn RuntimeBundleResolver>,
    pub lifecycle_lock: Arc<Mutex<()>>,
}

pub struct ConfigTransactionCoordinator {
    coordinator_lock: Mutex<()>,
    lifecycle_lock: Arc<Mutex<()>>,
    store: Arc<dyn TransactionStore>,
    runtime: Arc<dyn RuntimeApplyPort>,
    validator: Arc<dyn CoreConfigValidator + Send + Sync>,
    health: Arc<dyn RuntimeHealthProbe>,
    revisions: Arc<dyn CandidateRevisionSource>,
    bundles: Arc<dyn RuntimeBundleResolver>,
    owner: OwnerSessionProof,
}

#[derive(Clone, Copy)]
enum FailureRecoveryMode {
    ConvergeCommitted,
    StopStartupCandidate,
}

impl ConfigTransactionCoordinator {
    #[must_use]
    pub fn new(dependencies: ConfigTransactionDependencies, owner: OwnerSessionProof) -> Self {
        Self {
            coordinator_lock: Mutex::new(()),
            lifecycle_lock: dependencies.lifecycle_lock,
            store: dependencies.store,
            runtime: dependencies.runtime,
            validator: dependencies.validator,
            health: dependencies.health,
            revisions: dependencies.revisions,
            bundles: dependencies.bundles,
            owner,
        }
    }

    pub fn execute(
        &self,
        candidate: &ConfigTransactionCandidate,
    ) -> Result<ConfigTransactionSuccess, ConfigTransactionError> {
        let guard = self.coordinator_lock.lock().map_err(|_| {
            ConfigTransactionError::simple(ConfigTransactionErrorKind::LockPoisoned)
        })?;
        self.execute_guarded(candidate, guard, FailureRecoveryMode::ConvergeCommitted)
    }

    pub fn execute_startup_reapply(
        &self,
        candidate: &ConfigTransactionCandidate,
    ) -> Result<ConfigTransactionSuccess, ConfigTransactionError> {
        let guard = self.coordinator_lock.lock().map_err(|_| {
            ConfigTransactionError::simple(ConfigTransactionErrorKind::LockPoisoned)
        })?;
        self.execute_guarded(candidate, guard, FailureRecoveryMode::StopStartupCandidate)
    }

    pub fn try_execute_rule(
        &self,
        candidate: &ConfigTransactionCandidate,
    ) -> Result<ConfigTransactionSuccess, ConfigTransactionError> {
        let guard = match self.coordinator_lock.try_lock() {
            Ok(guard) => guard,
            Err(TryLockError::WouldBlock) => {
                return Err(ConfigTransactionError::simple(
                    ConfigTransactionErrorKind::Busy,
                ));
            }
            Err(TryLockError::Poisoned(_)) => {
                return Err(ConfigTransactionError::simple(
                    ConfigTransactionErrorKind::LockPoisoned,
                ));
            }
        };
        self.execute_guarded(candidate, guard, FailureRecoveryMode::ConvergeCommitted)
    }

    pub fn recover_startup(&self) -> Result<StartupRecovery, ConfigTransactionError> {
        let _coordinator = self.coordinator_lock.lock().map_err(|_| {
            ConfigTransactionError::simple(ConfigTransactionErrorKind::LockPoisoned)
        })?;
        let _lifecycle = self.lifecycle_lock.lock().map_err(|_| {
            ConfigTransactionError::simple(ConfigTransactionErrorKind::LockPoisoned)
        })?;
        let state = self
            .store
            .recover()
            .map_err(|_| ConfigTransactionError::simple(ConfigTransactionErrorKind::Recovery))?;
        let committed_generation = self
            .generation_from_manifest(state.committed.as_ref())
            .map_err(|_| ConfigTransactionError::simple(ConfigTransactionErrorKind::Recovery))?;

        if self.converge_to_manifest(state.committed.as_ref()).is_err() {
            return Err(ConfigTransactionError::new(
                ConfigTransactionErrorKind::Recovery,
                None,
                committed_generation,
                RecoveryOutcome::Failed {
                    target: committed_generation,
                },
            ));
        }

        if let Some(prepared) = state.prepared.as_ref()
            && self.clear_prepared_and_prune(prepared).is_err()
        {
            return Err(ConfigTransactionError::new(
                ConfigTransactionErrorKind::Cleanup,
                None,
                committed_generation,
                RecoveryOutcome::Pending {
                    target: committed_generation,
                },
            ));
        }

        Ok(StartupRecovery {
            committed_generation,
            cleared_prepared_journal: state.prepared.is_some(),
        })
    }

    pub fn prepare_startup_reapply(
        &self,
        expected_committed_generation: Option<RuntimeGeneration>,
    ) -> Result<StartupReapply, ConfigTransactionError> {
        let _coordinator = self.coordinator_lock.lock().map_err(|_| {
            ConfigTransactionError::simple(ConfigTransactionErrorKind::LockPoisoned)
        })?;
        let state = self
            .store
            .recover()
            .map_err(|_| ConfigTransactionError::simple(ConfigTransactionErrorKind::Recovery))?;
        let committed_generation = self
            .generation_from_manifest(state.committed.as_ref())
            .map_err(|_| ConfigTransactionError::simple(ConfigTransactionErrorKind::Recovery))?;
        if committed_generation != expected_committed_generation {
            return Err(ConfigTransactionError::new(
                ConfigTransactionErrorKind::Recovery,
                None,
                committed_generation,
                RecoveryOutcome::Failed {
                    target: expected_committed_generation,
                },
            ));
        }

        let prepared_generation = state.prepared.as_ref().and_then(|prepared| {
            self.store
                .load_transaction(&prepared.candidate)
                .ok()
                .map(|transaction| transaction.runtime_generation)
        });
        let highest_generation = committed_generation
            .into_iter()
            .chain(prepared_generation)
            .map(|generation| generation.0)
            .max()
            .unwrap_or(0);
        let candidate_generation = highest_generation
            .checked_add(1)
            .filter(|generation| *generation > 0)
            .map(RuntimeGeneration)
            .ok_or_else(|| {
                ConfigTransactionError::new(
                    ConfigTransactionErrorKind::Recovery,
                    None,
                    committed_generation,
                    RecoveryOutcome::Failed {
                        target: committed_generation,
                    },
                )
            })?;

        if let Some(prepared) = state.prepared.as_ref()
            && self.clear_prepared_and_prune(prepared).is_err()
        {
            return Err(ConfigTransactionError::new(
                ConfigTransactionErrorKind::Cleanup,
                None,
                committed_generation,
                RecoveryOutcome::Pending {
                    target: committed_generation,
                },
            ));
        }

        Ok(StartupReapply {
            committed_generation,
            candidate_generation,
            cleared_prepared_journal: state.prepared.is_some(),
        })
    }

    fn execute_guarded(
        &self,
        candidate: &ConfigTransactionCandidate,
        _guard: MutexGuard<'_, ()>,
        failure_recovery_mode: FailureRecoveryMode,
    ) -> Result<ConfigTransactionSuccess, ConfigTransactionError> {
        let initial_state = self.store.recover().map_err(|_| {
            ConfigTransactionError::new(
                ConfigTransactionErrorKind::Recovery,
                Some(candidate.runtime.generation),
                None,
                RecoveryOutcome::Failed { target: None },
            )
        })?;
        let committed_generation = self
            .generation_from_manifest(initial_state.committed.as_ref())
            .map_err(|_| {
                ConfigTransactionError::new(
                    ConfigTransactionErrorKind::Recovery,
                    Some(candidate.runtime.generation),
                    None,
                    RecoveryOutcome::Failed { target: None },
                )
            })?;
        if initial_state.prepared.is_some() {
            return Err(ConfigTransactionError::new(
                ConfigTransactionErrorKind::RecoveryRequired,
                Some(candidate.runtime.generation),
                committed_generation,
                RecoveryOutcome::Pending {
                    target: committed_generation,
                },
            ));
        }

        self.validate_candidate(candidate, committed_generation)?;
        let prepared = match self.store.prepare(&candidate.transaction) {
            Ok(prepared) => prepared,
            Err(_) => {
                let recovery = self.clear_unapplied_journal(committed_generation);
                return Err(ConfigTransactionError::new(
                    ConfigTransactionErrorKind::Prepare,
                    Some(candidate.runtime.generation),
                    committed_generation,
                    recovery,
                ));
            }
        };

        if self
            .validator
            .validate(&candidate.configuration, &candidate.runtime.generation_root)
            .is_err()
        {
            let recovery = self.clear_prepared_unapplied(&prepared, committed_generation);
            return Err(ConfigTransactionError::new(
                ConfigTransactionErrorKind::Validation,
                Some(candidate.runtime.generation),
                committed_generation,
                recovery,
            ));
        }

        if self.revisions.current() != candidate.revisions {
            let recovery = self.clear_prepared_unapplied(&prepared, committed_generation);
            return Err(ConfigTransactionError::new(
                ConfigTransactionErrorKind::StaleCandidate,
                Some(candidate.runtime.generation),
                committed_generation,
                recovery,
            ));
        }

        let _lifecycle = self.lifecycle_lock.lock().map_err(|_| {
            ConfigTransactionError::new(
                ConfigTransactionErrorKind::LockPoisoned,
                Some(candidate.runtime.generation),
                committed_generation,
                RecoveryOutcome::Pending {
                    target: committed_generation,
                },
            )
        })?;

        let (applied, apply_path) = match self
            .runtime
            .apply_candidate(&self.owner, &candidate.runtime)
        {
            Ok(applied) => (applied, ApplyPath::Direct),
            Err(RuntimeApplyFailure::Definite) => {
                let recovery = self.recover_failed_candidate(
                    &prepared,
                    committed_generation,
                    failure_recovery_mode,
                );
                return Err(ConfigTransactionError::new(
                    ConfigTransactionErrorKind::Apply,
                    Some(candidate.runtime.generation),
                    committed_generation,
                    recovery,
                ));
            }
            Err(RuntimeApplyFailure::TunPermissionDenied) => {
                let recovery = self.recover_failed_candidate(
                    &prepared,
                    committed_generation,
                    failure_recovery_mode,
                );
                return Err(ConfigTransactionError::new(
                    ConfigTransactionErrorKind::TunPermissionDenied,
                    Some(candidate.runtime.generation),
                    committed_generation,
                    recovery,
                ));
            }
            Err(RuntimeApplyFailure::Indeterminate) => {
                match self.restart_candidate(&candidate.runtime) {
                    Ok(applied) => (applied, ApplyPath::CandidateRestart),
                    Err(()) => {
                        let recovery = self.recover_failed_candidate(
                            &prepared,
                            committed_generation,
                            failure_recovery_mode,
                        );
                        return Err(ConfigTransactionError::new(
                            ConfigTransactionErrorKind::IndeterminateApply,
                            Some(candidate.runtime.generation),
                            committed_generation,
                            recovery,
                        ));
                    }
                }
            }
        };

        if apply_path == ApplyPath::Direct
            && self
                .confirm(&applied, candidate.runtime.generation)
                .is_err()
        {
            let recovery = self.recover_failed_candidate(
                &prepared,
                committed_generation,
                failure_recovery_mode,
            );
            return Err(ConfigTransactionError::new(
                ConfigTransactionErrorKind::Health,
                Some(candidate.runtime.generation),
                committed_generation,
                recovery,
            ));
        }

        let recovery = if self.store.commit_prepared(&prepared).is_err() {
            self.recover_commit_failure(
                &prepared,
                candidate.runtime.generation,
                committed_generation,
                failure_recovery_mode,
            )?
        } else if self.clear_prepared_and_prune(&prepared).is_err() {
            RecoveryOutcome::Pending {
                target: Some(candidate.runtime.generation),
            }
        } else {
            RecoveryOutcome::NotRequired
        };

        Ok(ConfigTransactionSuccess {
            candidate_generation: candidate.runtime.generation,
            committed_generation: candidate.runtime.generation,
            apply_path,
            recovery,
        })
    }

    fn validate_candidate(
        &self,
        candidate: &ConfigTransactionCandidate,
        committed_generation: Option<RuntimeGeneration>,
    ) -> Result<(), ConfigTransactionError> {
        let candidate_generation = candidate.runtime.generation;
        if self.revisions.current() != candidate.revisions {
            return Err(ConfigTransactionError::new(
                ConfigTransactionErrorKind::StaleCandidate,
                Some(candidate_generation),
                committed_generation,
                RecoveryOutcome::NotRequired,
            ));
        }

        let generation_is_newer = committed_generation
            .map_or(candidate_generation.0 > 0, |current| {
                candidate_generation > current
            });
        let effective_object = self
            .store
            .read_object_limited(
                &candidate.transaction.effective_configuration,
                EFFECTIVE_CONFIGURATION_MAX_BYTES,
            )
            .ok();
        let structurally_valid = generation_is_newer
            && candidate.transaction.runtime_generation == candidate_generation
            && candidate.transaction.profile_revision == candidate.revisions.profile
            && candidate.transaction.local_rule_set_revision == candidate.revisions.local_rule_set
            && candidate.runtime.compiler_policy_sha256
                == candidate.revisions.compiler_policy_sha256
            && candidate.configuration.compiler_policy_sha256()
                == candidate.revisions.compiler_policy_sha256
            && candidate.configuration.core_version() == candidate.revisions.core_version
            && candidate.runtime.generation_root.is_absolute()
            && !candidate.runtime.manifest_sha256.is_empty()
            && !candidate.runtime.mihomo_binary_sha256.is_empty()
            && effective_object.as_deref() == Some(candidate.configuration.yaml().as_bytes());

        if structurally_valid {
            Ok(())
        } else {
            Err(ConfigTransactionError::new(
                ConfigTransactionErrorKind::InvalidCandidate,
                Some(candidate_generation),
                committed_generation,
                RecoveryOutcome::NotRequired,
            ))
        }
    }

    fn restart_candidate(&self, bundle: &RuntimeBundle) -> Result<ApplyCandidateResult, ()> {
        self.runtime.stop(&self.owner).map_err(|_| ())?;
        let applied = self
            .runtime
            .apply_candidate(&self.owner, bundle)
            .map_err(|_| ())?;
        self.confirm(&applied, bundle.generation)?;
        Ok(applied)
    }

    fn confirm(
        &self,
        applied: &ApplyCandidateResult,
        expected_generation: RuntimeGeneration,
    ) -> Result<(), ()> {
        let status = self.runtime.status(&self.owner).map_err(|_| ())?;
        let actual = status.managed_core.ok_or(())?;
        let expected = &applied.managed_core;
        if actual.pid != expected.pid
            || actual.process_start_identity != expected.process_start_identity
            || actual.endpoint != expected.endpoint
            || actual.instance_generation != expected.instance_generation
            || actual.runtime_generation != expected_generation
            || expected.runtime_generation != expected_generation
        {
            return Err(());
        }
        self.health.confirm_ready(&actual).map_err(|_| ())
    }

    fn rollback(
        &self,
        prepared: &PreparedTransaction,
        target: Option<RuntimeGeneration>,
    ) -> RecoveryOutcome {
        if self
            .converge_to_transaction(prepared.previous.as_ref())
            .is_err()
        {
            return RecoveryOutcome::Failed { target };
        }
        if self.clear_prepared_and_prune(prepared).is_err() {
            return RecoveryOutcome::Failed { target };
        }
        RecoveryOutcome::Converged { generation: target }
    }

    fn recover_failed_candidate(
        &self,
        prepared: &PreparedTransaction,
        target: Option<RuntimeGeneration>,
        mode: FailureRecoveryMode,
    ) -> RecoveryOutcome {
        match mode {
            FailureRecoveryMode::ConvergeCommitted => self.rollback(prepared, target),
            FailureRecoveryMode::StopStartupCandidate => {
                let _ = self.runtime.stop(&self.owner);
                let _ = self.clear_prepared_and_prune(prepared);
                RecoveryOutcome::Failed { target }
            }
        }
    }

    fn recover_commit_failure(
        &self,
        prepared: &PreparedTransaction,
        candidate_generation: RuntimeGeneration,
        previous_generation: Option<RuntimeGeneration>,
        failure_recovery_mode: FailureRecoveryMode,
    ) -> Result<RecoveryOutcome, ConfigTransactionError> {
        let state = match self.store.recover() {
            Ok(state) => state,
            Err(_) => {
                return Err(ConfigTransactionError::new(
                    ConfigTransactionErrorKind::Commit,
                    Some(candidate_generation),
                    previous_generation,
                    RecoveryOutcome::Failed {
                        target: previous_generation,
                    },
                ));
            }
        };
        if state.committed.as_ref().map(|manifest| &manifest.current) == Some(&prepared.candidate) {
            let recovery = if self.clear_prepared_and_prune(prepared).is_ok() {
                RecoveryOutcome::Converged {
                    generation: Some(candidate_generation),
                }
            } else {
                RecoveryOutcome::Pending {
                    target: Some(candidate_generation),
                }
            };
            return Ok(recovery);
        }

        let recovery =
            self.recover_failed_candidate(prepared, previous_generation, failure_recovery_mode);
        Err(ConfigTransactionError::new(
            ConfigTransactionErrorKind::Commit,
            Some(candidate_generation),
            previous_generation,
            recovery,
        ))
    }

    fn clear_unapplied_journal(
        &self,
        committed_generation: Option<RuntimeGeneration>,
    ) -> RecoveryOutcome {
        match self.store.recover() {
            Ok(RecoveryState {
                prepared: Some(prepared),
                ..
            }) => self.clear_prepared_unapplied(&prepared, committed_generation),
            Ok(_) => RecoveryOutcome::NotRequired,
            Err(_) => RecoveryOutcome::Failed {
                target: committed_generation,
            },
        }
    }

    fn clear_prepared_unapplied(
        &self,
        prepared: &PreparedTransaction,
        committed_generation: Option<RuntimeGeneration>,
    ) -> RecoveryOutcome {
        if self.clear_prepared_and_prune(prepared).is_ok() {
            RecoveryOutcome::Converged {
                generation: committed_generation,
            }
        } else {
            RecoveryOutcome::Failed {
                target: committed_generation,
            }
        }
    }

    fn converge_to_manifest(&self, manifest: Option<&CommittedManifest>) -> Result<(), ()> {
        self.converge_to_transaction(manifest.map(|manifest| &manifest.current))
    }

    fn converge_to_transaction(&self, transaction: Option<&TransactionId>) -> Result<(), ()> {
        let Some(transaction) = transaction else {
            return self.runtime.stop(&self.owner).map_err(|_| ());
        };
        let transaction = self.store.load_transaction(transaction).map_err(|_| ())?;
        let bundle = self.bundles.resolve(&transaction).map_err(|_| ())?;
        if bundle.generation != transaction.runtime_generation {
            return Err(());
        }

        if let Ok(status) = self.runtime.status(&self.owner)
            && let Some(managed_core) = status.managed_core
            && managed_core.runtime_generation == bundle.generation
            && self.health.confirm_ready(&managed_core).is_ok()
        {
            return Ok(());
        }

        self.runtime.stop(&self.owner).map_err(|_| ())?;
        let applied = self
            .runtime
            .apply_candidate(&self.owner, &bundle)
            .map_err(|_| ())?;
        self.confirm(&applied, bundle.generation)
    }

    fn generation_from_manifest(
        &self,
        manifest: Option<&CommittedManifest>,
    ) -> Result<Option<RuntimeGeneration>, ()> {
        manifest
            .map(|manifest| {
                self.store
                    .load_transaction(&manifest.current)
                    .map(|transaction| transaction.runtime_generation)
                    .map_err(|_| ())
            })
            .transpose()
    }

    fn clear_prepared_and_prune(&self, prepared: &PreparedTransaction) -> io::Result<()> {
        self.store.clear_prepared(prepared)?;
        let _ = self.store.prune_unreachable();
        Ok(())
    }
}
