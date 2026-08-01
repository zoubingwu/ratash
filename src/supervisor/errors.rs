//! Maps domain and transaction failures into stable application errors.

use crate::application::{
    ApplicationError, ApplicationErrorDetails, RulePlacement as ApplicationRulePlacement,
    RuntimeApplyFailureDetails, RuntimeApplyFailureStage, SelectorCandidate, SelectorKind,
};
use crate::config::ConfigError;
use crate::constants::RULE_STRING_MAX_BYTES;
use crate::core::SelectionError;
use crate::domain::ProfileId;
use crate::error::ErrorCode;
use crate::profile::{ProfileCatalog, ProfileSelectorError, RefreshStage};
use crate::rule::{RulePlacement, RuleSetError, RuleString, parse_rule};
use crate::transaction::ConfigTransactionErrorKind;

use super::outcomes::application_recovery;
use super::transactions::{SupervisorTransactionFailure, SupervisorTransactionFailureKind};

pub(super) fn checked_rule(value: String) -> Result<RuleString, ApplicationError> {
    let rule = RuleString::new(value, RULE_STRING_MAX_BYTES).map_err(|_| invalid_rule_error())?;
    parse_rule(&rule).map_err(|_| invalid_rule_error())?;
    Ok(rule)
}

pub(super) fn map_rule_placement(
    placement: ApplicationRulePlacement,
) -> Result<RulePlacement, ApplicationError> {
    Ok(match placement {
        ApplicationRulePlacement::Prepend => RulePlacement::Prepend,
        ApplicationRulePlacement::Append => RulePlacement::Append,
        ApplicationRulePlacement::Before(rule) => RulePlacement::Before(checked_rule(rule)?),
        ApplicationRulePlacement::After(rule) => RulePlacement::After(checked_rule(rule)?),
    })
}

pub(super) fn resolve_profile(
    profiles: &ProfileCatalog,
    selector: &str,
) -> Result<ProfileId, ApplicationError> {
    profiles
        .resolve(selector)
        .map_err(|error| map_profile_error(profiles, error))
}

pub(super) fn map_profile_error(
    profiles: &ProfileCatalog,
    error: ProfileSelectorError,
) -> ApplicationError {
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

pub(super) fn map_rule_error(error: RuleSetError) -> ApplicationError {
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

pub(super) fn map_selection_error(error: SelectionError) -> ApplicationError {
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

pub(super) fn refresh_stage_for_transaction_failure(
    failure: &SupervisorTransactionFailure,
) -> RefreshStage {
    if matches!(
        failure.kind,
        SupervisorTransactionFailureKind::Coordinator(ConfigTransactionErrorKind::Validation)
    ) {
        RefreshStage::Validate
    } else {
        RefreshStage::Apply
    }
}

pub(super) fn map_config_error(error: ConfigError) -> ApplicationError {
    match error {
        ConfigError::UnavailableReference { .. } => ApplicationError::new(
            ErrorCode::PolicyTargetNotFound,
            "The configuration contains an unavailable Policy Target",
            false,
        ),
        _ => ApplicationError::new(ErrorCode::ExternalOperationFailed, error.to_string(), false),
    }
}

pub(super) fn map_transaction_error(error: SupervisorTransactionFailure) -> ApplicationError {
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
        SupervisorTransactionFailureKind::Coordinator(ConfigTransactionErrorKind::Shutdown) => {
            ApplicationError::new(
                ErrorCode::OperationUnavailable,
                "The Supervisor is shutting down",
                true,
            )
            .with_details(ApplicationErrorDetails::RuntimeApplyFailure(Box::new(
                RuntimeApplyFailureDetails {
                    candidate_generation: error.candidate_generation,
                    committed_generation: error.committed_generation,
                    stage: RuntimeApplyFailureStage::RecoveryRequired,
                    recovery: application_recovery(error.recovery),
                },
            )))
        }
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

pub(super) fn transaction_failure_stage(
    kind: SupervisorTransactionFailureKind,
) -> RuntimeApplyFailureStage {
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
            ConfigTransactionErrorKind::Shutdown => RuntimeApplyFailureStage::RecoveryRequired,
        },
    }
}

pub(super) fn bounded_message(message: String) -> String {
    message.chars().take(1_024).collect()
}

pub(super) fn selector_not_found(kind: SelectorKind, label: &str) -> ApplicationError {
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

pub(super) fn invalid_rule_error() -> ApplicationError {
    ApplicationError::new(
        ErrorCode::ExternalOperationFailed,
        "The Rule String is invalid",
        false,
    )
}

pub(super) fn rule_busy_error() -> ApplicationError {
    ApplicationError::new(
        ErrorCode::RuleBusy,
        "The configuration transaction is busy",
        true,
    )
}

pub(super) fn core_error(message: &'static str) -> ApplicationError {
    ApplicationError::new(ErrorCode::CoreUnavailable, message, true)
}

pub(super) fn no_active_profile() -> ApplicationError {
    ApplicationError::new(
        ErrorCode::ProfileNotFound,
        "No Active Profile is configured",
        false,
    )
}

pub(super) fn internal_error() -> ApplicationError {
    ApplicationError::new(
        ErrorCode::Internal,
        "The Supervisor state is unavailable",
        false,
    )
}
