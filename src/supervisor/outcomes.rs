//! Projects transaction outcomes into runtime apply and recovery results.

use super::{
    ApplicationRecoveryOutcome, ConfigTransactionSuccess, RecoveryStatus, RuntimeApplyOutcome,
    RuntimeApplyPhase, RuntimeApplySnapshot, RuntimeApplyStatus, RuntimeGeneration,
    RuntimeRecoverySnapshot, RuntimeRecoveryStatus, SupervisorTransactionFailure,
    TransactionRecoveryOutcome,
};

pub(super) fn successful_runtime_apply_snapshot(
    success: ConfigTransactionSuccess,
) -> RuntimeApplySnapshot {
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

pub(super) fn failed_runtime_apply_snapshot(
    error: SupervisorTransactionFailure,
) -> RuntimeApplySnapshot {
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

pub(super) fn runtime_recovery_snapshot(
    recovery: TransactionRecoveryOutcome,
) -> RuntimeRecoverySnapshot {
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

pub(super) fn recovery_requires_degraded(recovery: TransactionRecoveryOutcome) -> bool {
    matches!(
        recovery,
        TransactionRecoveryOutcome::Pending { .. } | TransactionRecoveryOutcome::Failed { .. }
    )
}

pub(super) fn runtime_apply_success(success: ConfigTransactionSuccess) -> RuntimeApplyOutcome {
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

pub(super) fn application_recovery(
    recovery: TransactionRecoveryOutcome,
) -> ApplicationRecoveryOutcome {
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

pub(super) fn runtime_apply_not_required(
    generation: Option<RuntimeGeneration>,
) -> RuntimeApplyOutcome {
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
