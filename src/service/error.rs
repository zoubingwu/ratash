//! Service boundary errors and safe CoreRuntime error translation.

use crate::core::{CoreRuntimeError, CoreRuntimeErrorKind};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ServicePlatformErrorKind {
    Credential,
    ProcessInspection,
    Spawn,
    Reload,
    ReloadTimeout,
    ApplyCancelled,
    Stop,
    Readiness,
    ReadinessTimeout,
    Logs,
    TunUnavailable,
    TunUnsupported,
    ConfigurationPolicy,
    Randomness,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ServicePlatformError {
    pub kind: ServicePlatformErrorKind,
}

impl ServicePlatformError {
    #[must_use]
    pub const fn new(kind: ServicePlatformErrorKind) -> Self {
        Self { kind }
    }
}

pub(super) fn invalid_bundle(message: &'static str) -> CoreRuntimeError {
    service_error(CoreRuntimeErrorKind::InvalidBundle, message)
}

pub(super) fn map_spawn_error(error: ServicePlatformError) -> CoreRuntimeError {
    if error.kind == ServicePlatformErrorKind::ApplyCancelled {
        service_error(
            CoreRuntimeErrorKind::ReloadTimeout,
            "Core spawn was cancelled during Supervisor shutdown",
        )
    } else {
        service_error(CoreRuntimeErrorKind::Apply, "Core spawn failed")
    }
}

pub(super) fn map_reload_error(error: ServicePlatformError) -> CoreRuntimeError {
    match error.kind {
        ServicePlatformErrorKind::ReloadTimeout | ServicePlatformErrorKind::ApplyCancelled => {
            service_error(CoreRuntimeErrorKind::ReloadTimeout, "Core reload timed out")
        }
        _ => service_error(CoreRuntimeErrorKind::Apply, "Core reload failed"),
    }
}

pub(super) fn map_readiness_error(error: ServicePlatformError) -> CoreRuntimeError {
    if error.kind == ServicePlatformErrorKind::ApplyCancelled {
        service_error(
            CoreRuntimeErrorKind::ReloadTimeout,
            "Core readiness was cancelled during Supervisor shutdown",
        )
    } else {
        service_error(
            CoreRuntimeErrorKind::Readiness,
            "Core readiness confirmation failed",
        )
    }
}

pub(super) fn map_stop_error(_error: ServicePlatformError) -> CoreRuntimeError {
    service_error(CoreRuntimeErrorKind::Apply, "Core stop failed")
}

pub(super) fn map_tun_preflight_error(error: ServicePlatformError) -> CoreRuntimeError {
    match error.kind {
        ServicePlatformErrorKind::TunUnavailable => service_error(
            CoreRuntimeErrorKind::TunPermissionDenied,
            "TUN capability permission check failed",
        ),
        ServicePlatformErrorKind::TunUnsupported => service_error(
            CoreRuntimeErrorKind::TunUnsupported,
            "TUN control sockets are unsupported",
        ),
        _ => service_error(
            CoreRuntimeErrorKind::Unavailable,
            "TUN capability inspection failed",
        ),
    }
}

pub(super) fn service_error(kind: CoreRuntimeErrorKind, message: &'static str) -> CoreRuntimeError {
    CoreRuntimeError::new(kind, message)
}
