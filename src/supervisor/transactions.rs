//! Coordinates persisted Supervisor transactions and Runtime Bundle staging.

use std::sync::{Arc, Mutex, MutexGuard, TryLockError};

use crate::config::EffectiveConfiguration;
use crate::core::RuntimeBundle;
use crate::domain::RuntimeGeneration;
use crate::profile::ProfileCatalog;
use crate::rule::LocalRuleSet;
use crate::runtime_bundle::{RuntimeBundleStageErrorKind, RuntimeBundleStager};
use crate::state::{AuthoritativeState, AuthoritativeStateStore, StateStoreErrorKind};
use crate::transaction::{
    CandidateRevisionSource, CandidateRevisions, ConfigTransactionCandidate,
    ConfigTransactionCoordinator, ConfigTransactionError, ConfigTransactionErrorKind,
    ConfigTransactionSuccess, RecoveryOutcome as TransactionRecoveryOutcome,
    RuleConfigTransactionReservation,
};

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

    fn cancel_pending(&self) {}
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

    fn cancel_pending(&self) {
        self.coordinator.request_shutdown();
    }
}
